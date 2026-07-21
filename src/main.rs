//! Command line entry point for the rBTC node daemon.

use std::{
    env, fs,
    net::SocketAddr,
    path::PathBuf,
    process,
    str::FromStr,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use bitcoin::{
    Block, BlockHash, Network,
    consensus::{deserialize, serialize},
    hashes::Hash,
};
use rbtc::{
    api::explorer_router,
    block_execution::{connect_active_block, disconnect_execution_tip, recover_pending_transition},
    blockchain::{AppliedBlock, validate_block_structure},
    deployments::{DeploymentConfig, block_deployment_context, taproot_active},
    execution_store::RedbExecutionStore,
    explorer_store::RedbExplorerIndex,
    header_store::RedbHeaderStore,
    headers::HeaderDag,
    ibd::IbdPolicy,
    ledger::{LedgerRetention, PrunedBlockLedger},
    p2p::{MAX_BLOCKS_IN_FLIGHT, MAX_HEADERS_PER_RESPONSE, connect_outbound},
    undo_store::RedbUndoStore,
    utxo::{DEFAULT_HOT_WINDOW_SECS, RedbUtxoStore},
};
use tokio::time::timeout;

const PEER_TIMEOUT: Duration = Duration::from_secs(30);
const USER_AGENT: &str = "/rbtcd:0.1.0/";

struct Options {
    remote: SocketAddr,
    network: Network,
    fetch_block: Option<BlockHash>,
    headers_db: Option<PathBuf>,
    data_dir: Option<PathBuf>,
    once: bool,
    explorer_listen: Option<SocketAddr>,
    deployments: DeploymentConfig,
    ibd_policy: IbdPolicy,
}

struct ExplorerServer {
    #[cfg(test)]
    address: SocketAddr,
    task: Option<tokio::task::JoinHandle<Result<(), String>>>,
}

impl ExplorerServer {
    async fn bind(address: SocketAddr, index: Arc<RedbExplorerIndex>) -> Result<Self, String> {
        let listener = tokio::net::TcpListener::bind(address)
            .await
            .map_err(|error| format!("bind explorer at {address}: {error}"))?;
        let bound = listener
            .local_addr()
            .map_err(|error| format!("read explorer address: {error}"))?;
        println!("embedded explorer listening on http://{bound}");
        let task = tokio::spawn(async move {
            axum::serve(listener, explorer_router(index))
                .await
                .map_err(|error| format!("explorer server: {error}"))
        });
        Ok(Self {
            #[cfg(test)]
            address: bound,
            task: Some(task),
        })
    }

    #[cfg(test)]
    const fn address(&self) -> SocketAddr {
        self.address
    }

    async fn ensure_running(&mut self) -> Result<(), String> {
        let Some(task) = self.task.as_ref() else {
            return Err("explorer server task missing".to_owned());
        };
        if !task.is_finished() {
            return Ok(());
        }
        self.task
            .take()
            .expect("finished explorer task exists")
            .await
            .map_err(|error| format!("explorer server task: {error}"))?
    }
}

impl Drop for ExplorerServer {
    fn drop(&mut self) {
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    match parse_options(env::args().skip(1)) {
        Ok(Some(options)) => {
            if let Err(error) = run(options).await {
                eprintln!("rbtcd: {error}");
                process::exit(1);
            }
        }
        Ok(None) => print_usage(),
        Err(error) => {
            eprintln!("rbtcd: {error}\n");
            print_usage();
            process::exit(2);
        }
    }
}

async fn run(options: Options) -> Result<(), String> {
    let mut session = timeout(
        PEER_TIMEOUT,
        connect_outbound(
            options.remote,
            options.network.magic(),
            rand::random(),
            USER_AGENT.to_owned(),
            0,
        ),
    )
    .await
    .map_err(|_| {
        format!(
            "peer handshake timed out after {} seconds",
            PEER_TIMEOUT.as_secs()
        )
    })?
    .map_err(|error| error.to_string())?;

    let remote = session.remote_version();
    println!(
        "connected to {}: version={}, height={}, agent={}",
        options.remote, remote.version, remote.start_height, remote.user_agent
    );

    if let Some(hash) = options.fetch_block {
        session
            .ensure_full_witness_block_relay()
            .map_err(|error| error.to_string())?;
        timeout(PEER_TIMEOUT, session.request_witness_blocks(&[hash]))
            .await
            .map_err(|_| "getdata request timed out".to_owned())?
            .map_err(|error| error.to_string())?;
        let block = timeout(PEER_TIMEOUT, session.receive_requested_block(hash))
            .await
            .map_err(|_| {
                format!(
                    "block response timed out after {} seconds",
                    PEER_TIMEOUT.as_secs()
                )
            })?
            .map_err(|error| error.to_string())?;
        println!(
            "received block {} ({} transactions); not applied: IBD chainstate integration is pending",
            block.block_hash(),
            block.txdata.len()
        );
        return Ok(());
    }

    if let Some(path) = options.data_dir {
        session
            .ensure_full_witness_block_relay()
            .map_err(|error| error.to_string())?;
        return sync_regtest_node(
            &mut session,
            options.network,
            options.deployments,
            options.ibd_policy,
            path,
            options.once,
            options.explorer_listen,
        )
        .await;
    }

    if let Some(path) = options.headers_db {
        let headers = sync_headers(&mut session, options.network, path).await?;
        let status = options
            .ibd_policy
            .ensure_minimum_chainwork(&headers)
            .map_err(|error| error.to_string())?;
        println!(
            "minimum chainwork reached at height {}; full script validation remains required",
            status.height
        );
        return Ok(());
    }

    let genesis = bitcoin::blockdata::constants::genesis_block(options.network).block_hash();
    request_headers(&mut session, vec![genesis]).await?;
    let headers = receive_headers(&mut session).await?;
    println!(
        "received {} headers; pass --headers-db PATH to validate, persist, and continue IBD",
        headers.len()
    );
    Ok(())
}

async fn sync_headers(
    session: &mut rbtc::p2p::PeerSession<tokio::net::TcpStream>,
    network: Network,
    path: PathBuf,
) -> Result<HeaderDag, String> {
    let store = RedbHeaderStore::open(path).map_err(|error| error.to_string())?;
    let mut dag = store
        .load_dag(network, unix_time()?)
        .map_err(|error| error.to_string())?;
    println!(
        "resuming headers-first sync from height {} (local-clock fallback until network-time aggregation lands)",
        dag.active_tip().height
    );

    loop {
        request_headers(session, dag.block_locator()).await?;
        let headers = receive_headers(session).await?;
        let count = headers.len();
        if count == 0 {
            break;
        }
        let (candidate, _) = dag
            .validate_batch_contextual(&headers, unix_time()?)
            .map_err(|error| error.to_string())?;
        store
            .append_batch(&headers)
            .map_err(|error| error.to_string())?;
        dag = candidate;
        println!(
            "validated and persisted {count} headers; active height {}",
            dag.active_tip().height
        );
        if count < MAX_HEADERS_PER_RESPONSE {
            break;
        }
    }
    println!(
        "peer returned no more headers at height {}",
        dag.active_tip().height
    );
    Ok(dag)
}

#[allow(clippy::too_many_lines)]
async fn sync_regtest_node(
    session: &mut rbtc::p2p::PeerSession<tokio::net::TcpStream>,
    network: Network,
    deployment_config: DeploymentConfig,
    ibd_policy: IbdPolicy,
    data_dir: PathBuf,
    once: bool,
    explorer_listen: Option<SocketAddr>,
) -> Result<(), String> {
    if network != Network::Regtest {
        return Err(
            "block execution is currently safety-gated to regtest until deployment activation is complete"
                .to_owned(),
        );
    }
    fs::create_dir_all(&data_dir)
        .map_err(|error| format!("create data directory {}: {error}", data_dir.display()))?;
    let headers_path = data_dir.join("headers.redb");
    let chainstate =
        RedbUtxoStore::open(data_dir.join("chainstate.redb")).map_err(|error| error.to_string())?;
    let undo_store =
        RedbUndoStore::open(data_dir.join("undo.redb")).map_err(|error| error.to_string())?;
    let execution_store = RedbExecutionStore::open(data_dir.join("execution.redb"), network)
        .map_err(|error| error.to_string())?;
    execution_store
        .bind_consensus_config(
            &deployment_config.consensus_id(),
            &DeploymentConfig::for_network(network).consensus_id(),
        )
        .map_err(|error| error.to_string())?;
    let ledger = PrunedBlockLedger::open(data_dir.join("blocks"), LedgerRetention::default())
        .map_err(|error| error.to_string())?;
    let explorer = Arc::new(
        RedbExplorerIndex::open(data_dir.join("explorer.redb"), network)
            .map_err(|error| error.to_string())?,
    );
    let mut explorer_server = match explorer_listen {
        Some(address) => Some(ExplorerServer::bind(address, Arc::clone(&explorer)).await?),
        None => None,
    };
    let mut headers = sync_headers(session, network, headers_path.clone()).await?;
    if recover_pending_transition(
        &chainstate,
        &undo_store,
        &execution_store,
        u64::from(unix_time()?),
        DEFAULT_HOT_WINDOW_SECS,
    )
    .map_err(|error| error.to_string())?
    {
        println!("recovered an interrupted chainstate transition");
    }

    'resync: loop {
        if let Some(server) = &mut explorer_server {
            server.ensure_running().await?;
        }
        loop {
            if let Some(server) = &mut explorer_server {
                server.ensure_running().await?;
            }
            let tip = execution_store.tip().map_err(|error| error.to_string())?;
            if headers
                .active_header_at(tip.height)
                .is_some_and(|header| header.hash == tip.hash)
            {
                break;
            }
            let rewound = disconnect_execution_tip(
                &chainstate,
                &undo_store,
                &execution_store,
                &headers,
                u64::from(unix_time()?),
                DEFAULT_HOT_WINDOW_SECS,
            )
            .map_err(|error| error.to_string())?;
            println!(
                "disconnected stale execution tip; rewound to {}:{}",
                rewound.height, rewound.hash
            );
        }

        reconcile_ledger(
            session,
            network,
            deployment_config,
            &headers,
            &execution_store,
            &ledger,
        )
        .await?;
        reconcile_explorer(
            session,
            network,
            deployment_config,
            &headers,
            &execution_store,
            &undo_store,
            &ledger,
            &explorer,
        )
        .await?;

        loop {
            if let Some(server) = &mut explorer_server {
                server.ensure_running().await?;
            }
            let tip = execution_store.tip().map_err(|error| error.to_string())?;
            if tip.height >= headers.active_tip().height {
                println!("block execution caught up at height {}", tip.height);
                let ibd_status = ibd_policy
                    .status(&headers)
                    .map_err(|error| error.to_string())?;
                if ibd_status.minimum_chainwork_reached {
                    println!(
                        "minimum chainwork reached; full script validation remains enabled{}",
                        ibd_status
                            .active_assume_valid_height
                            .map_or_else(String::new, |height| format!(
                                " (assume-valid anchor active at {height})"
                            ))
                    );
                } else {
                    let error = ibd_policy
                        .ensure_minimum_chainwork(&headers)
                        .expect_err("status is below minimum chainwork");
                    if once {
                        return Err(error.to_string());
                    }
                    println!("remaining in IBD: {error}");
                }
                if once {
                    return Ok(());
                }
                tokio::time::sleep(Duration::from_secs(30)).await;
                headers = sync_headers(session, network, headers_path.clone()).await?;
                continue 'resync;
            }
            download_execute_batch(
                session,
                network,
                deployment_config,
                &headers,
                &chainstate,
                &undo_store,
                &execution_store,
                &ledger,
                &explorer,
            )
            .await?;
        }
    }
}

async fn reconcile_ledger(
    session: &mut rbtc::p2p::PeerSession<tokio::net::TcpStream>,
    network: Network,
    deployment_config: DeploymentConfig,
    headers: &HeaderDag,
    execution_store: &RedbExecutionStore,
    ledger: &PrunedBlockLedger,
) -> Result<(), String> {
    let tip = execution_store.tip().map_err(|error| error.to_string())?;
    if let Some(first_unexecuted) = tip.height.checked_add(1) {
        ledger
            .truncate_from(first_unexecuted)
            .map_err(|error| error.to_string())?;
    }

    if let Some(staged) = ledger.staged().map_err(|error| error.to_string())? {
        if staged.manifest.first_height > tip.height {
            ledger.discard_staged().map_err(|error| error.to_string())?;
        } else {
            let available = tip
                .height
                .checked_sub(staged.manifest.first_height)
                .and_then(|distance| distance.checked_add(1))
                .ok_or_else(|| "staged ledger height overflow".to_owned())?;
            let validated_count = available.min(staged.manifest.block_count);
            let mut on_active_chain = true;
            for (offset, bytes) in staged
                .blocks
                .iter()
                .take(usize::try_from(validated_count).expect("staged count fits usize"))
                .enumerate()
            {
                let height = staged
                    .manifest
                    .first_height
                    .checked_add(u32::try_from(offset).expect("staged offset fits u32"))
                    .ok_or_else(|| "staged ledger height overflow".to_owned())?;
                let block: Block = deserialize(bytes)
                    .map_err(|error| format!("decode staged block at height {height}: {error}"))?;
                let expected = headers
                    .active_header_at(height)
                    .ok_or_else(|| format!("missing active header at height {height}"))?;
                if block.block_hash() != expected.hash {
                    on_active_chain = false;
                    break;
                }
                validate_archive_block(
                    network,
                    deployment_config,
                    headers,
                    height,
                    expected.hash,
                    &block,
                )?;
            }
            if on_active_chain {
                let preceding_height = staged.manifest.first_height.saturating_sub(1);
                backfill_ledger(
                    session,
                    network,
                    deployment_config,
                    headers,
                    ledger,
                    preceding_height.min(tip.height),
                )
                .await?;
                ledger
                    .commit_staged(validated_count)
                    .map_err(|error| error.to_string())?;
                println!(
                    "recovered {validated_count} validated blocks from the staged ledger segment"
                );
            } else {
                ledger.discard_staged().map_err(|error| error.to_string())?;
            }
        }
    }

    backfill_ledger(
        session,
        network,
        deployment_config,
        headers,
        ledger,
        tip.height,
    )
    .await
}

async fn backfill_ledger(
    session: &mut rbtc::p2p::PeerSession<tokio::net::TcpStream>,
    network: Network,
    deployment_config: DeploymentConfig,
    headers: &HeaderDag,
    ledger: &PrunedBlockLedger,
    target_height: u32,
) -> Result<(), String> {
    if target_height == 0 {
        return Ok(());
    }
    let mut next_height = match ledger.retained_tip().map_err(|error| error.to_string())? {
        Some(height) if height >= target_height => return Ok(()),
        Some(height) => height
            .checked_add(1)
            .ok_or_else(|| "ledger height overflow".to_owned())?,
        None => target_height
            .saturating_sub(ledger.retention().max_blocks.saturating_sub(1))
            .max(1),
    };
    while next_height <= target_height {
        let remaining = target_height - next_height + 1;
        let batch_len = usize::try_from(remaining)
            .unwrap_or(usize::MAX)
            .min(MAX_BLOCKS_IN_FLIGHT);
        let expected = (0..batch_len)
            .map(|offset| {
                let height = next_height
                    .checked_add(u32::try_from(offset).expect("block batch fits u32"))
                    .ok_or_else(|| "ledger height overflow".to_owned())?;
                headers
                    .active_header_at(height)
                    .ok_or_else(|| format!("missing active header at height {height}"))
            })
            .collect::<Result<Vec<_>, String>>()?;
        let hashes = expected
            .iter()
            .map(|header| header.hash)
            .collect::<Vec<_>>();
        timeout(PEER_TIMEOUT, session.request_witness_blocks(&hashes))
            .await
            .map_err(|_| format!("ledger backfill getdata timed out for {batch_len} blocks"))?
            .map_err(|error| error.to_string())?;
        let blocks = timeout(PEER_TIMEOUT, session.receive_requested_blocks(&hashes))
            .await
            .map_err(|_| format!("ledger backfill response timed out for {batch_len} blocks"))?
            .map_err(|error| error.to_string())?;
        let mut serialized = Vec::with_capacity(blocks.len());
        for (expected, block) in expected.iter().zip(&blocks) {
            validate_archive_block(
                network,
                deployment_config,
                headers,
                expected.height,
                expected.hash,
                block,
            )?;
            serialized.push(serialize(block));
        }
        ledger
            .append(next_height, &serialized)
            .map_err(|error| error.to_string())?;
        if u32::try_from(batch_len).expect("block batch fits u32") == remaining {
            break;
        }
        next_height = next_height
            .checked_add(u32::try_from(batch_len).expect("block batch fits u32"))
            .ok_or_else(|| "ledger height overflow".to_owned())?;
    }
    Ok(())
}

fn validate_archive_block(
    network: Network,
    deployment_config: DeploymentConfig,
    headers: &HeaderDag,
    height: u32,
    expected_hash: BlockHash,
    block: &Block,
) -> Result<(), String> {
    let actual = block.block_hash();
    if actual != expected_hash {
        return Err(format!(
            "archive block {actual} does not match active block {expected_hash} at height {height}"
        ));
    }
    let deployments = block_deployment_context(
        network,
        height,
        expected_hash,
        block.header.time,
        taproot_active(headers, height, deployment_config).map_err(|error| error.to_string())?,
    );
    validate_block_structure(
        block,
        height,
        deployments.script_flags,
        deployments.bip34_active,
    )
    .map_err(|error| format!("archive block structure at height {height}: {error}"))
}

#[allow(clippy::too_many_arguments)]
async fn reconcile_explorer(
    session: &mut rbtc::p2p::PeerSession<tokio::net::TcpStream>,
    network: Network,
    deployment_config: DeploymentConfig,
    headers: &HeaderDag,
    execution_store: &RedbExecutionStore,
    undo_store: &RedbUndoStore,
    ledger: &PrunedBlockLedger,
    explorer: &RedbExplorerIndex,
) -> Result<(), String> {
    let execution_tip = execution_store.tip().map_err(|error| error.to_string())?;
    loop {
        let explorer_tip = explorer.tip().map_err(|error| error.to_string())?;
        if explorer_tip.height <= execution_tip.height
            && headers
                .active_header_at(explorer_tip.height)
                .is_some_and(|header| header.hash == explorer_tip.hash)
        {
            break;
        }
        let rewound = explorer
            .disconnect_tip()
            .map_err(|error| error.to_string())?;
        println!(
            "disconnected stale explorer tip; rewound to {}:{}",
            rewound.height, rewound.hash
        );
    }

    loop {
        let explorer_tip = explorer.tip().map_err(|error| error.to_string())?;
        if explorer_tip.height >= execution_tip.height {
            return Ok(());
        }
        let next_height = explorer_tip
            .height
            .checked_add(1)
            .ok_or_else(|| "explorer height overflow".to_owned())?;
        let remaining = execution_tip.height - explorer_tip.height;
        let batch_len = usize::try_from(remaining)
            .unwrap_or(usize::MAX)
            .min(MAX_BLOCKS_IN_FLIGHT);
        let expected = (0..batch_len)
            .map(|offset| {
                let height = next_height
                    .checked_add(u32::try_from(offset).expect("explorer batch fits u32"))
                    .ok_or_else(|| "explorer height overflow".to_owned())?;
                headers
                    .active_header_at(height)
                    .ok_or_else(|| format!("missing active header at height {height}"))
            })
            .collect::<Result<Vec<_>, String>>()?;
        let mut blocks = Vec::with_capacity(batch_len);
        let mut all_local = true;
        for header in &expected {
            let Some(bytes) = ledger
                .read_block(header.height)
                .map_err(|error| error.to_string())?
            else {
                all_local = false;
                break;
            };
            blocks.push(deserialize(&bytes).map_err(|error| {
                format!("decode retained block at height {}: {error}", header.height)
            })?);
        }
        if !all_local {
            let hashes = expected
                .iter()
                .map(|header| header.hash)
                .collect::<Vec<_>>();
            timeout(PEER_TIMEOUT, session.request_witness_blocks(&hashes))
                .await
                .map_err(|_| format!("explorer backfill getdata timed out for {batch_len} blocks"))?
                .map_err(|error| error.to_string())?;
            blocks = timeout(PEER_TIMEOUT, session.receive_requested_blocks(&hashes))
                .await
                .map_err(|_| {
                    format!("explorer backfill response timed out for {batch_len} blocks")
                })?
                .map_err(|error| error.to_string())?;
        }
        for (expected, block) in expected.iter().zip(&blocks) {
            validate_archive_block(
                network,
                deployment_config,
                headers,
                expected.height,
                expected.hash,
                block,
            )?;
            let transaction_undos = undo_store
                .get(expected.hash)
                .map_err(|error| error.to_string())?
                .ok_or_else(|| {
                    format!("missing active undo for explorer block {}", expected.hash)
                })?;
            explorer
                .connect(
                    expected.height,
                    block,
                    &AppliedBlock {
                        hash: expected.hash,
                        transaction_undos,
                    },
                )
                .map_err(|error| error.to_string())?;
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn download_execute_batch(
    session: &mut rbtc::p2p::PeerSession<tokio::net::TcpStream>,
    network: Network,
    deployment_config: DeploymentConfig,
    headers: &HeaderDag,
    chainstate: &RedbUtxoStore,
    undo_store: &RedbUndoStore,
    execution_store: &RedbExecutionStore,
    ledger: &PrunedBlockLedger,
    explorer: &RedbExplorerIndex,
) -> Result<(), String> {
    let tip = execution_store.tip().map_err(|error| error.to_string())?;
    let next_height = tip
        .height
        .checked_add(1)
        .ok_or_else(|| "execution height overflow".to_owned())?;
    let remaining = headers.active_tip().height - tip.height;
    let batch_len = usize::try_from(remaining)
        .unwrap_or(usize::MAX)
        .min(MAX_BLOCKS_IN_FLIGHT);
    let expected = (0..batch_len)
        .map(|offset| {
            let offset = u32::try_from(offset).expect("block batch fits u32");
            let height = next_height
                .checked_add(offset)
                .ok_or_else(|| "execution height overflow".to_owned())?;
            headers
                .active_header_at(height)
                .ok_or_else(|| format!("missing active header at height {height}"))
        })
        .collect::<Result<Vec<_>, String>>()?;
    let hashes = expected
        .iter()
        .map(|header| header.hash)
        .collect::<Vec<_>>();
    timeout(PEER_TIMEOUT, session.request_witness_blocks(&hashes))
        .await
        .map_err(|_| format!("getdata timed out for {batch_len} blocks"))?
        .map_err(|error| error.to_string())?;
    let blocks = timeout(PEER_TIMEOUT, session.receive_requested_blocks(&hashes))
        .await
        .map_err(|_| format!("block batch response timed out for {batch_len} blocks"))?
        .map_err(|error| error.to_string())?;
    for (expected, block) in expected.iter().zip(&blocks) {
        validate_archive_block(
            network,
            deployment_config,
            headers,
            expected.height,
            expected.hash,
            block,
        )?;
    }
    let serialized = blocks.iter().map(serialize).collect::<Vec<_>>();
    ledger
        .stage(next_height, &serialized)
        .map_err(|error| error.to_string())?;
    for (expected, block) in expected.into_iter().zip(&blocks) {
        let applied = connect_active_block(
            chainstate,
            undo_store,
            execution_store,
            headers,
            block,
            u64::from(unix_time()?),
            DEFAULT_HOT_WINDOW_SECS,
            block_deployment_context(
                network,
                expected.height,
                expected.hash,
                block.header.time,
                taproot_active(headers, expected.height, deployment_config)
                    .map_err(|error| error.to_string())?,
            ),
        )
        .map_err(|error| error.to_string())?;
        explorer
            .connect(expected.height, block, &applied)
            .map_err(|error| error.to_string())?;
        println!(
            "validated and executed block {}:{}",
            expected.height, expected.hash
        );
    }
    ledger
        .commit_staged(u32::try_from(blocks.len()).expect("block download batch count fits u32"))
        .map_err(|error| error.to_string())?;
    Ok(())
}

async fn request_headers(
    session: &mut rbtc::p2p::PeerSession<tokio::net::TcpStream>,
    locator: Vec<BlockHash>,
) -> Result<(), String> {
    timeout(
        PEER_TIMEOUT,
        session.request_headers(locator, BlockHash::all_zeros()),
    )
    .await
    .map_err(|_| "getheaders request timed out".to_owned())?
    .map_err(|error| error.to_string())
}

async fn receive_headers(
    session: &mut rbtc::p2p::PeerSession<tokio::net::TcpStream>,
) -> Result<Vec<bitcoin::block::Header>, String> {
    timeout(PEER_TIMEOUT, session.receive_headers())
        .await
        .map_err(|_| {
            format!(
                "headers response timed out after {} seconds",
                PEER_TIMEOUT.as_secs()
            )
        })?
        .map_err(|error| error.to_string())
}

fn unix_time() -> Result<u32, String> {
    let seconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| "system clock is before the Unix epoch".to_owned())?
        .as_secs();
    u32::try_from(seconds)
        .map_err(|_| "system clock does not fit Bitcoin timestamp range".to_owned())
}

fn parse_options(args: impl Iterator<Item = String>) -> Result<Option<Options>, String> {
    let mut args = args.peekable();
    if args.peek().is_none() {
        return Ok(None);
    }

    let mut remote = None;
    let mut network = Network::Bitcoin;
    let mut fetch_block = None;
    let mut headers_db = None;
    let mut data_dir = None;
    let mut once = false;
    let mut explorer_listen = None;
    let mut vbparams = Vec::new();
    let mut minimum_chainwork = None;
    let mut assume_valid = None;
    while let Some(argument) = args.next() {
        match argument.as_str() {
            "--help" | "-h" => return Ok(None),
            "--connect" => {
                let address = required_option_value(&mut args, "--connect")?;
                remote = Some(
                    address
                        .parse()
                        .map_err(|_| format!("invalid peer address: {address}"))?,
                );
            }
            "--network" => {
                let value = required_option_value(&mut args, "--network")?;
                network = Network::from_str(&value)
                    .map_err(|_| format!("unsupported network: {value}"))?;
            }
            "--fetch-block" => {
                let value = required_option_value(&mut args, "--fetch-block")?;
                fetch_block = Some(
                    BlockHash::from_str(&value)
                        .map_err(|_| format!("invalid block hash: {value}"))?,
                );
            }
            "--headers-db" => {
                headers_db = Some(PathBuf::from(required_option_value(
                    &mut args,
                    "--headers-db",
                )?));
            }
            "--data-dir" => {
                data_dir = Some(PathBuf::from(required_option_value(
                    &mut args,
                    "--data-dir",
                )?));
            }
            "--once" => once = true,
            "--explorer-listen" => {
                let value = required_option_value(&mut args, "--explorer-listen")?;
                let address: SocketAddr = value
                    .parse()
                    .map_err(|_| format!("invalid explorer listen address: {value}"))?;
                if !address.ip().is_loopback() {
                    return Err(
                        "--explorer-listen must use a loopback IP until authentication is enabled"
                            .to_owned(),
                    );
                }
                explorer_listen = Some(address);
            }
            "--vbparams" => {
                vbparams.push(required_option_value(&mut args, "--vbparams")?);
            }
            "--minimum-chainwork" => {
                minimum_chainwork = Some(required_option_value(&mut args, "--minimum-chainwork")?);
            }
            "--assumevalid" | "--assume-valid" => {
                assume_valid = Some(required_option_value(&mut args, "--assumevalid")?);
            }
            _ => return Err(format!("unknown option: {argument}")),
        }
    }

    let remote = remote.ok_or_else(|| "--connect is required".to_owned())?;
    if explorer_listen.is_some() && data_dir.is_none() {
        return Err("--explorer-listen requires --data-dir".to_owned());
    }
    let deployments = parse_deployment_config(network, data_dir.is_some(), vbparams)?;
    let ibd_policy = parse_ibd_policy(
        network,
        data_dir.is_some() || headers_db.is_some(),
        minimum_chainwork,
        assume_valid,
    )?;
    Ok(Some(Options {
        remote,
        network,
        fetch_block,
        headers_db,
        data_dir,
        once,
        explorer_listen,
        deployments,
        ibd_policy,
    }))
}

fn required_option_value(
    args: &mut impl Iterator<Item = String>,
    option: &str,
) -> Result<String, String> {
    args.next()
        .ok_or_else(|| format!("{option} requires a value"))
}

fn parse_deployment_config(
    network: Network,
    has_data_dir: bool,
    vbparams: Vec<String>,
) -> Result<DeploymentConfig, String> {
    if !vbparams.is_empty() && !has_data_dir {
        return Err("--vbparams requires --data-dir".to_owned());
    }
    let mut deployments = DeploymentConfig::for_network(network);
    for value in vbparams {
        deployments
            .apply_vbparams(&value)
            .map_err(|error| error.to_string())?;
    }
    Ok(deployments)
}

fn parse_ibd_policy(
    network: Network,
    sync_enabled: bool,
    minimum_chainwork: Option<String>,
    assume_valid: Option<String>,
) -> Result<IbdPolicy, String> {
    if !sync_enabled && (minimum_chainwork.is_some() || assume_valid.is_some()) {
        return Err("IBD policy options require --headers-db or --data-dir".to_owned());
    }
    let mut policy = IbdPolicy::for_network(network);
    if let Some(value) = minimum_chainwork {
        policy
            .set_minimum_chainwork(&value)
            .map_err(|error| error.to_string())?;
    }
    if let Some(value) = assume_valid {
        policy
            .set_assume_valid(&value)
            .map_err(|error| error.to_string())?;
    }
    Ok(policy)
}

fn print_usage() {
    println!(
        "rbtcd {}\n\nUSAGE:\n  rbtcd --connect HOST:PORT [--network bitcoin|testnet|testnet4|signet|regtest]\n  rbtcd --connect HOST:PORT --headers-db PATH [--network NETWORK] [--minimum-chainwork HEX] [--assumevalid HASH|0]\n  rbtcd --connect HOST:PORT --data-dir PATH --network regtest [--once] [--explorer-listen 127.0.0.1:3000] [--vbparams taproot:START:END[:MIN_HEIGHT]] [--minimum-chainwork HEX] [--assumevalid HASH|0]\n  rbtcd --connect HOST:PORT --fetch-block BLOCK_HASH [--network NETWORK]",
        env!("CARGO_PKG_VERSION")
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use bitcoin::{
        Amount, Block, OutPoint, ScriptBuf, Sequence, Transaction, TxIn, TxMerkleNode, TxOut,
        Witness,
        absolute::LockTime,
        block::{Header, Version},
        p2p::{Address, ServiceFlags, message::NetworkMessage, message_network::VersionMessage},
        pow::Target,
        transaction::Version as TransactionVersion,
    };
    use rbtc::{
        api::ExplorerIndex,
        blockchain::block_subsidy,
        execution_store::RedbExecutionStore,
        header_store::RedbHeaderStore,
        ledger::{LedgerRetention, PrunedBlockLedger},
        p2p::V1Transport,
        undo_store::RedbUndoStore,
        utxo::{OutPointKey, RedbUtxoStore, UtxoStore},
    };
    use tempfile::TempDir;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    fn peer_version(nonce: u64) -> VersionMessage {
        let receiver: SocketAddr = "127.0.0.1:18444".parse().unwrap();
        let sender: SocketAddr = "0.0.0.0:0".parse().unwrap();
        VersionMessage::new(
            ServiceFlags::NETWORK | ServiceFlags::WITNESS,
            0,
            Address::new(&receiver, ServiceFlags::NONE),
            Address::new(&sender, ServiceFlags::NONE),
            nonce,
            "/rbtcd:test-peer/".to_owned(),
            1,
        )
    }

    fn mine_regtest_child(parent: BlockHash, time: u32) -> Header {
        let target = Target::MAX_ATTAINABLE_REGTEST;
        let mut header = Header {
            version: Version::ONE,
            prev_blockhash: parent,
            merkle_root: TxMerkleNode::all_zeros(),
            time,
            bits: target.to_compact_lossy(),
            nonce: 0,
        };
        while header.validate_pow(target).is_err() {
            header.nonce = header.nonce.checked_add(1).unwrap();
        }
        header
    }

    fn regtest_block(parent: BlockHash, time: u32) -> Block {
        let coinbase = Transaction {
            version: TransactionVersion::ONE,
            lock_time: LockTime::ZERO,
            input: vec![TxIn {
                previous_output: OutPoint::null(),
                script_sig: ScriptBuf::from_bytes(vec![1, 1]),
                sequence: Sequence::MAX,
                witness: Witness::default(),
            }],
            output: vec![TxOut {
                value: Amount::from_sat(block_subsidy(1)),
                script_pubkey: ScriptBuf::new(),
            }],
        };
        let mut block = Block {
            header: Header {
                version: Version::ONE,
                prev_blockhash: parent,
                merkle_root: TxMerkleNode::all_zeros(),
                time,
                bits: Target::MAX_ATTAINABLE_REGTEST.to_compact_lossy(),
                nonce: 0,
            },
            txdata: vec![coinbase],
        };
        block.header.merkle_root = block.compute_merkle_root().unwrap();
        while block
            .header
            .validate_pow(Target::MAX_ATTAINABLE_REGTEST)
            .is_err()
        {
            block.header.nonce = block.header.nonce.checked_add(1).unwrap();
        }
        block
    }

    #[test]
    fn parses_a_header_probe() {
        let options = parse_options(
            ["--connect", "127.0.0.1:18444", "--network", "regtest"]
                .into_iter()
                .map(str::to_owned),
        )
        .unwrap()
        .unwrap();
        assert_eq!(options.remote, "127.0.0.1:18444".parse().unwrap());
        assert_eq!(options.network, Network::Regtest);
        assert!(options.fetch_block.is_none());
        assert!(options.headers_db.is_none());
        assert!(options.data_dir.is_none());
        assert!(!options.once);
        assert!(options.explorer_listen.is_none());
        assert_eq!(
            options.deployments,
            DeploymentConfig::for_network(Network::Regtest)
        );
        assert_eq!(options.ibd_policy, IbdPolicy::for_network(Network::Regtest));

        let served = parse_options(
            [
                "--connect",
                "127.0.0.1:18444",
                "--network",
                "regtest",
                "--data-dir",
                "/tmp/rbtc-test",
                "--explorer-listen",
                "127.0.0.1:0",
                "--vbparams",
                "taproot:-2:0:432",
            ]
            .into_iter()
            .map(str::to_owned),
        )
        .unwrap()
        .unwrap();
        assert_eq!(served.explorer_listen, Some("127.0.0.1:0".parse().unwrap()));
        assert_ne!(
            served.deployments,
            DeploymentConfig::for_network(Network::Regtest)
        );
        assert!(
            parse_options(
                [
                    "--connect",
                    "127.0.0.1:18444",
                    "--data-dir",
                    "/tmp/rbtc-test",
                    "--explorer-listen",
                    "0.0.0.0:3000",
                ]
                .into_iter()
                .map(str::to_owned),
            )
            .is_err()
        );
        assert!(
            parse_options(
                [
                    "--connect",
                    "127.0.0.1:18444",
                    "--network",
                    "bitcoin",
                    "--data-dir",
                    "/tmp/rbtc-test",
                    "--vbparams",
                    "taproot:0:1",
                ]
                .into_iter()
                .map(str::to_owned),
            )
            .is_err()
        );
        assert!(
            parse_options(
                [
                    "--connect",
                    "127.0.0.1:18444",
                    "--network",
                    "regtest",
                    "--vbparams",
                    "taproot:0:1",
                ]
                .into_iter()
                .map(str::to_owned),
            )
            .is_err()
        );
    }

    #[test]
    fn parses_and_rejects_ibd_policy_options() {
        let customized = parse_options(
            [
                "--connect",
                "127.0.0.1:18444",
                "--network",
                "regtest",
                "--headers-db",
                "/tmp/rbtc-headers",
                "--minimum-chainwork",
                "0000000000000000000000000000000000000000000000000000000000000000",
                "--assumevalid",
                "0000000000000000000000000000000000000000000000000000000000000000",
            ]
            .into_iter()
            .map(str::to_owned),
        )
        .unwrap()
        .unwrap();
        assert_ne!(
            customized.ibd_policy,
            IbdPolicy::for_network(Network::Regtest)
        );

        assert!(
            parse_options(
                ["--connect", "127.0.0.1:18444", "--minimum-chainwork", "01"]
                    .into_iter()
                    .map(str::to_owned),
            )
            .is_err()
        );
        assert!(
            parse_options(
                [
                    "--connect",
                    "127.0.0.1:18444",
                    "--headers-db",
                    "/tmp/rbtc-headers",
                    "--minimum-chainwork",
                    "invalid",
                ]
                .into_iter()
                .map(str::to_owned),
            )
            .is_err()
        );
        assert!(
            parse_options(
                [
                    "--connect",
                    "127.0.0.1:18444",
                    "--headers-db",
                    "/tmp/rbtc-headers",
                    "--assumevalid",
                    "invalid",
                ]
                .into_iter()
                .map(str::to_owned),
            )
            .is_err()
        );
    }

    #[tokio::test]
    async fn embedded_explorer_serves_the_static_page_on_loopback() {
        let directory = TempDir::new().unwrap();
        let index = Arc::new(
            RedbExplorerIndex::open(directory.path().join("explorer.redb"), Network::Regtest)
                .unwrap(),
        );
        let server = ExplorerServer::bind("127.0.0.1:0".parse().unwrap(), index)
            .await
            .unwrap();
        let mut stream = tokio::net::TcpStream::connect(server.address())
            .await
            .unwrap();
        stream
            .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        let mut response = Vec::new();
        stream.read_to_end(&mut response).await.unwrap();
        let response = String::from_utf8(response).unwrap();
        assert!(response.starts_with("HTTP/1.1 200 OK"));
        assert!(response.contains("content-security-policy:"));
        assert!(response.contains("<title>rBTC Explorer</title>"));
    }

    #[tokio::test]
    async fn daemon_validates_and_persists_a_header_sync() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let remote = listener.local_addr().unwrap();
        let genesis = bitcoin::blockdata::constants::genesis_block(Network::Regtest).header;
        let child = mine_regtest_child(genesis.block_hash(), genesis.time + 1);
        let child_hash = child.block_hash();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut peer = V1Transport::new(stream, Network::Regtest.magic());
            assert!(matches!(
                peer.read_message().await.unwrap().into_payload(),
                NetworkMessage::Version(_)
            ));
            peer.write_message(NetworkMessage::Version(peer_version(5)))
                .await
                .unwrap();
            assert!(matches!(
                peer.read_message().await.unwrap().into_payload(),
                NetworkMessage::Verack
            ));
            peer.write_message(NetworkMessage::Verack).await.unwrap();
            match peer.read_message().await.unwrap().into_payload() {
                NetworkMessage::GetHeaders(request) => {
                    assert_eq!(request.locator_hashes, vec![genesis.block_hash()]);
                    peer.write_message(NetworkMessage::Headers(vec![child]))
                        .await
                        .unwrap();
                }
                message => panic!("expected first getheaders, got {message:?}"),
            }
        });

        let directory = TempDir::new().unwrap();
        let header_path = directory.path().join("headers.redb");
        run(Options {
            remote,
            network: Network::Regtest,
            fetch_block: None,
            headers_db: Some(header_path.clone()),
            data_dir: None,
            once: true,
            explorer_listen: None,
            deployments: DeploymentConfig::for_network(Network::Regtest),
            ibd_policy: IbdPolicy::for_network(Network::Regtest),
        })
        .await
        .unwrap();
        server.await.unwrap();

        let store = RedbHeaderStore::open(header_path).unwrap();
        assert_eq!(
            store
                .load_dag(Network::Regtest, unix_time().unwrap())
                .unwrap()
                .active_tip()
                .hash,
            child_hash
        );
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn daemon_downloads_executes_and_recovers_a_regtest_block() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let remote = listener.local_addr().unwrap();
        let genesis = bitcoin::blockdata::constants::genesis_block(Network::Regtest).header;
        let block = regtest_block(genesis.block_hash(), genesis.time + 1);
        let block_hash = block.block_hash();
        let coinbase_outpoint = OutPointKey::from(OutPoint::new(block.txdata[0].compute_txid(), 0));
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut peer = V1Transport::new(stream, Network::Regtest.magic());
            assert!(matches!(
                peer.read_message().await.unwrap().into_payload(),
                NetworkMessage::Version(_)
            ));
            peer.write_message(NetworkMessage::Version(peer_version(6)))
                .await
                .unwrap();
            assert!(matches!(
                peer.read_message().await.unwrap().into_payload(),
                NetworkMessage::Verack
            ));
            peer.write_message(NetworkMessage::Verack).await.unwrap();
            match peer.read_message().await.unwrap().into_payload() {
                NetworkMessage::GetHeaders(_) => peer
                    .write_message(NetworkMessage::Headers(vec![block.header]))
                    .await
                    .unwrap(),
                message => panic!("expected getheaders, got {message:?}"),
            }
            match peer.read_message().await.unwrap().into_payload() {
                NetworkMessage::GetData(inventory) => {
                    assert_eq!(
                        inventory,
                        vec![bitcoin::p2p::message_blockdata::Inventory::WitnessBlock(
                            block_hash
                        )]
                    );
                    peer.write_message(NetworkMessage::Block(block))
                        .await
                        .unwrap();
                }
                message => panic!("expected getdata, got {message:?}"),
            }
        });

        let directory = TempDir::new().unwrap();
        run(Options {
            remote,
            network: Network::Regtest,
            fetch_block: None,
            headers_db: None,
            data_dir: Some(directory.path().to_path_buf()),
            once: true,
            explorer_listen: None,
            deployments: DeploymentConfig::for_network(Network::Regtest),
            ibd_policy: IbdPolicy::for_network(Network::Regtest),
        })
        .await
        .unwrap();
        server.await.unwrap();

        let execution =
            RedbExecutionStore::open(directory.path().join("execution.redb"), Network::Regtest)
                .unwrap();
        assert_eq!(execution.tip().unwrap().height, 1);
        assert_eq!(execution.tip().unwrap().hash, block_hash);
        let undo = RedbUndoStore::open(directory.path().join("undo.redb")).unwrap();
        assert!(undo.get(block_hash).unwrap().is_some());
        let chainstate = RedbUtxoStore::open(directory.path().join("chainstate.redb")).unwrap();
        assert!(chainstate.get(coinbase_outpoint).unwrap().is_some());
        let ledger =
            PrunedBlockLedger::open(directory.path().join("blocks"), LedgerRetention::default())
                .unwrap();
        assert_eq!(ledger.retained_ranges().unwrap(), vec![(1, 1)]);
        let archived: Block = deserialize(&ledger.read_block(1).unwrap().unwrap()).unwrap();
        assert_eq!(archived.block_hash(), block_hash);
        assert!(ledger.staged().unwrap().is_none());
        let explorer =
            RedbExplorerIndex::open(directory.path().join("explorer.redb"), Network::Regtest)
                .unwrap();
        assert_eq!(explorer.tip().unwrap().height, 1);
        assert_eq!(
            explorer.block(1).unwrap().unwrap().hash,
            block_hash.to_string()
        );
        drop(explorer);

        let archived_bytes = serialize(&archived);
        ledger.truncate_from(1).unwrap();
        ledger.stage(1, &[archived_bytes]).unwrap();
        drop(ledger);
        drop((execution, undo, chainstate));

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let recovery_remote = listener.local_addr().unwrap();
        let recovery_server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut peer = V1Transport::new(stream, Network::Regtest.magic());
            assert!(matches!(
                peer.read_message().await.unwrap().into_payload(),
                NetworkMessage::Version(_)
            ));
            peer.write_message(NetworkMessage::Version(peer_version(7)))
                .await
                .unwrap();
            assert!(matches!(
                peer.read_message().await.unwrap().into_payload(),
                NetworkMessage::Verack
            ));
            peer.write_message(NetworkMessage::Verack).await.unwrap();
            assert!(matches!(
                peer.read_message().await.unwrap().into_payload(),
                NetworkMessage::GetHeaders(_)
            ));
            peer.write_message(NetworkMessage::Headers(Vec::new()))
                .await
                .unwrap();
        });
        run(Options {
            remote: recovery_remote,
            network: Network::Regtest,
            fetch_block: None,
            headers_db: None,
            data_dir: Some(directory.path().to_path_buf()),
            once: true,
            explorer_listen: None,
            deployments: DeploymentConfig::for_network(Network::Regtest),
            ibd_policy: IbdPolicy::for_network(Network::Regtest),
        })
        .await
        .unwrap();
        recovery_server.await.unwrap();

        let recovered_ledger =
            PrunedBlockLedger::open(directory.path().join("blocks"), LedgerRetention::default())
                .unwrap();
        assert_eq!(recovered_ledger.retained_ranges().unwrap(), vec![(1, 1)]);
        assert!(recovered_ledger.staged().unwrap().is_none());
        recovered_ledger.truncate_from(1).unwrap();
        drop(recovered_ledger);

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let backfill_remote = listener.local_addr().unwrap();
        let backfill_server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut peer = V1Transport::new(stream, Network::Regtest.magic());
            assert!(matches!(
                peer.read_message().await.unwrap().into_payload(),
                NetworkMessage::Version(_)
            ));
            peer.write_message(NetworkMessage::Version(peer_version(8)))
                .await
                .unwrap();
            assert!(matches!(
                peer.read_message().await.unwrap().into_payload(),
                NetworkMessage::Verack
            ));
            peer.write_message(NetworkMessage::Verack).await.unwrap();
            assert!(matches!(
                peer.read_message().await.unwrap().into_payload(),
                NetworkMessage::GetHeaders(_)
            ));
            peer.write_message(NetworkMessage::Headers(Vec::new()))
                .await
                .unwrap();
            match peer.read_message().await.unwrap().into_payload() {
                NetworkMessage::GetData(inventory) => {
                    assert_eq!(
                        inventory,
                        vec![bitcoin::p2p::message_blockdata::Inventory::WitnessBlock(
                            block_hash
                        )]
                    );
                    peer.write_message(NetworkMessage::Block(archived))
                        .await
                        .unwrap();
                }
                message => panic!("expected ledger backfill getdata, got {message:?}"),
            }
        });
        run(Options {
            remote: backfill_remote,
            network: Network::Regtest,
            fetch_block: None,
            headers_db: None,
            data_dir: Some(directory.path().to_path_buf()),
            once: true,
            explorer_listen: None,
            deployments: DeploymentConfig::for_network(Network::Regtest),
            ibd_policy: IbdPolicy::for_network(Network::Regtest),
        })
        .await
        .unwrap();
        backfill_server.await.unwrap();

        let backfilled_ledger =
            PrunedBlockLedger::open(directory.path().join("blocks"), LedgerRetention::default())
                .unwrap();
        assert_eq!(backfilled_ledger.retained_ranges().unwrap(), vec![(1, 1)]);
    }
}
