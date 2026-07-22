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
    block_execution::{connect_active_block, connect_active_blocks, disconnect_execution_tip},
    blockchain::{AppliedBlock, validate_block_structure_with_deployments},
    chain_store::RedbChainStore,
    deployments::{DeploymentConfig, block_deployment_context_with_config, taproot_active},
    execution_store::RedbExecutionStore,
    explorer_store::RedbExplorerIndex,
    header_store::RedbHeaderStore,
    headers::HeaderDag,
    ibd::IbdPolicy,
    ledger::{LedgerRetention, PrunedBlockLedger},
    p2p::{MAX_BLOCKS_IN_FLIGHT, MAX_HEADERS_PER_RESPONSE, connect_outbound},
    peer_store::RedbPeerStore,
    undo_store::RedbUndoStore,
    utxo::DEFAULT_HOT_WINDOW_SECS,
};
use tokio::time::timeout;

const PEER_TIMEOUT: Duration = Duration::from_secs(30);
const PEER_DISCOVERY_TIMEOUT: Duration = Duration::from_secs(3);
const USER_AGENT: &str = "/rbtcd:0.1.0/";
const MAX_CONFIGURED_PEERS: usize = 16;

const fn supports_block_execution(network: Network) -> bool {
    matches!(network, Network::Regtest | Network::Signet)
}

struct Options {
    remotes: Vec<SocketAddr>,
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
    run_with_nonce(options, rand::random()).await
}

async fn run_with_nonce(options: Options, local_nonce: u64) -> Result<(), String> {
    let peer_store = if let Some(data_dir) = &options.data_dir {
        fs::create_dir_all(data_dir)
            .map_err(|error| format!("create data directory {}: {error}", data_dir.display()))?;
        reject_legacy_split_chainstate(data_dir)?;
        Some(
            RedbPeerStore::open(data_dir.join("peers.redb"), options.network)
                .map_err(|error| error.to_string())?,
        )
    } else {
        None
    };
    let mut remotes = options.remotes.clone();
    if let Some(store) = &peer_store {
        for learned in store
            .candidates(unix_time()?, MAX_CONFIGURED_PEERS)
            .map_err(|error| error.to_string())?
        {
            if remotes.len() == MAX_CONFIGURED_PEERS {
                break;
            }
            if !remotes.contains(&learned) {
                remotes.push(learned);
            }
        }
    }
    let mut failures = Vec::with_capacity(remotes.len());
    for remote in remotes {
        match run_with_peer(&options, remote, local_nonce, peer_store.as_ref()).await {
            Ok(()) => return Ok(()),
            Err(error) => {
                eprintln!("peer {remote} failed: {error}");
                failures.push(format!("{remote}: {error}"));
            }
        }
    }
    Err(format!(
        "all {} configured peers failed: {}",
        failures.len(),
        failures.join("; ")
    ))
}

async fn run_with_peer(
    options: &Options,
    remote: SocketAddr,
    local_nonce: u64,
    peer_store: Option<&RedbPeerStore>,
) -> Result<(), String> {
    record_peer_attempt(peer_store, remote);
    let mut session = timeout(
        PEER_TIMEOUT,
        connect_outbound(
            remote,
            options.network.magic(),
            local_nonce,
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

    let remote_version = session.remote_version();
    println!(
        "connected to {}: version={}, height={}, agent={}",
        remote, remote_version.version, remote_version.start_height, remote_version.user_agent
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

    if let Some(path) = &options.data_dir {
        session
            .ensure_full_witness_block_relay()
            .map_err(|error| error.to_string())?;
        if let Some(store) = peer_store {
            record_peer_success(store, remote);
            discover_peer_addresses(&mut session, store, remote).await;
        }
        return sync_validating_node(
            &mut session,
            options.network,
            options.deployments,
            options.ibd_policy,
            path.clone(),
            options.once,
            options.explorer_listen,
        )
        .await;
    }

    if let Some(path) = &options.headers_db {
        let headers = sync_headers(&mut session, options.deployments, path.clone()).await?;
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

fn record_peer_attempt(store: Option<&RedbPeerStore>, remote: SocketAddr) {
    let Some(store) = store else {
        return;
    };
    let now = match unix_time() {
        Ok(now) => now,
        Err(error) => {
            eprintln!("peer attempt history for {remote} skipped: {error}");
            return;
        }
    };
    if let Err(error) = store.record_attempt(remote, now) {
        eprintln!("peer attempt history for {remote} failed: {error}");
    }
}

fn record_peer_success(store: &RedbPeerStore, remote: SocketAddr) {
    let now = match unix_time() {
        Ok(now) => now,
        Err(error) => {
            eprintln!("peer success history for {remote} skipped: {error}");
            return;
        }
    };
    if let Err(error) = store.record_success(remote, now) {
        eprintln!("peer success history for {remote} failed: {error}");
    }
}

async fn discover_peer_addresses(
    session: &mut rbtc::p2p::PeerSession<tokio::net::TcpStream>,
    store: &RedbPeerStore,
    source: SocketAddr,
) {
    let discovery = timeout(PEER_DISCOVERY_TIMEOUT, async {
        session.request_addresses().await?;
        session.receive_addresses().await
    })
    .await;
    match discovery {
        Ok(Ok(addresses)) => {
            let now = match unix_time() {
                Ok(now) => now,
                Err(error) => {
                    eprintln!("peer address persistence from {source} skipped: {error}");
                    return;
                }
            };
            match store.insert_discovered(source, &addresses, now) {
                Ok(stats) if stats.accepted > 0 => println!(
                    "persisted {} learned peer addresses from {source}; rejected {}",
                    stats.accepted, stats.rejected
                ),
                Ok(_) => {}
                Err(error) => eprintln!("peer address persistence from {source} failed: {error}"),
            }
        }
        Ok(Err(error)) => eprintln!("peer address discovery from {source} failed: {error}"),
        Err(_) => eprintln!(
            "peer address discovery from {source} timed out after {} seconds",
            PEER_DISCOVERY_TIMEOUT.as_secs()
        ),
    }
}

async fn sync_headers(
    session: &mut rbtc::p2p::PeerSession<tokio::net::TcpStream>,
    deployments: DeploymentConfig,
    path: PathBuf,
) -> Result<HeaderDag, String> {
    let store = RedbHeaderStore::open(path).map_err(|error| error.to_string())?;
    let mut dag = store
        .load_dag_with_deployments(deployments, unix_time()?)
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

fn reject_legacy_split_chainstate(data_dir: &std::path::Path) -> Result<(), String> {
    for legacy in ["undo.redb", "execution.redb"] {
        let path = data_dir.join(legacy);
        if path.exists() {
            return Err(format!(
                "legacy split chain-state file {} is not migrated automatically; move the old data directory aside and rebuild or import a verified snapshot",
                path.display()
            ));
        }
    }
    Ok(())
}

#[allow(clippy::too_many_lines)]
async fn sync_validating_node(
    session: &mut rbtc::p2p::PeerSession<tokio::net::TcpStream>,
    network: Network,
    deployment_config: DeploymentConfig,
    ibd_policy: IbdPolicy,
    data_dir: PathBuf,
    once: bool,
    explorer_listen: Option<SocketAddr>,
) -> Result<(), String> {
    if !supports_block_execution(network) {
        return Err(
            "block execution is currently safety-gated to regtest and default signet until mainnet/testnet activation coverage is complete"
                .to_owned(),
        );
    }
    fs::create_dir_all(&data_dir)
        .map_err(|error| format!("create data directory {}: {error}", data_dir.display()))?;
    let headers_path = data_dir.join("headers.redb");
    reject_legacy_split_chainstate(&data_dir)?;
    let chainstate = RedbChainStore::open(data_dir.join("chainstate.redb"), network)
        .map_err(|error| error.to_string())?;
    let undo_store = chainstate.undos();
    let execution_store = chainstate.execution();
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
    let mut headers = sync_headers(session, deployment_config, headers_path.clone()).await?;
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
            deployment_config,
            &headers,
            execution_store,
            &ledger,
        )
        .await?;
        reconcile_explorer(
            session,
            deployment_config,
            &headers,
            execution_store,
            undo_store,
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
                headers = sync_headers(session, deployment_config, headers_path.clone()).await?;
                continue 'resync;
            }
            download_execute_batch(
                session,
                deployment_config,
                &headers,
                &chainstate,
                &ledger,
                &explorer,
            )
            .await?;
        }
    }
}

async fn reconcile_ledger(
    session: &mut rbtc::p2p::PeerSession<tokio::net::TcpStream>,
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
                validate_archive_block(deployment_config, headers, height, expected.hash, &block)?;
            }
            if on_active_chain {
                let preceding_height = staged.manifest.first_height.saturating_sub(1);
                backfill_ledger(
                    session,
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

    backfill_ledger(session, deployment_config, headers, ledger, tip.height).await
}

async fn backfill_ledger(
    session: &mut rbtc::p2p::PeerSession<tokio::net::TcpStream>,
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
    let deployments = block_deployment_context_with_config(
        deployment_config,
        height,
        expected_hash,
        block.header.time,
        taproot_active(headers, height, deployment_config).map_err(|error| error.to_string())?,
    );
    validate_block_structure_with_deployments(
        block,
        height,
        deployments.bip34_active,
        deployments.segwit_active,
        deployments.signet,
    )
    .map_err(|error| format!("archive block structure at height {height}: {error}"))
}

#[allow(clippy::too_many_arguments)]
async fn reconcile_explorer(
    session: &mut rbtc::p2p::PeerSession<tokio::net::TcpStream>,
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
    deployment_config: DeploymentConfig,
    headers: &HeaderDag,
    chainstate: &RedbChainStore,
    ledger: &PrunedBlockLedger,
    explorer: &RedbExplorerIndex,
) -> Result<(), String> {
    let execution_store = chainstate.execution();
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
    let deployment_contexts = expected
        .iter()
        .zip(&blocks)
        .map(|(expected, block)| {
            Ok(block_deployment_context_with_config(
                deployment_config,
                expected.height,
                expected.hash,
                block.header.time,
                taproot_active(headers, expected.height, deployment_config)
                    .map_err(|error| error.to_string())?,
            ))
        })
        .collect::<Result<Vec<_>, String>>()?;
    let now = u64::from(unix_time()?);
    let applied_blocks = if blocks.len() == 1 {
        vec![
            connect_active_block(
                chainstate,
                headers,
                &blocks[0],
                now,
                DEFAULT_HOT_WINDOW_SECS,
                deployment_contexts[0],
            )
            .map_err(|error| error.to_string())?,
        ]
    } else {
        connect_active_blocks(
            chainstate,
            headers,
            &blocks,
            now,
            DEFAULT_HOT_WINDOW_SECS,
            &deployment_contexts,
        )
        .map_err(|error| error.to_string())?
    };
    for ((expected, block), applied) in expected.into_iter().zip(&blocks).zip(&applied_blocks) {
        explorer
            .connect(expected.height, block, applied)
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

#[allow(clippy::too_many_lines)]
fn parse_options(args: impl Iterator<Item = String>) -> Result<Option<Options>, String> {
    let mut args = args.peekable();
    if args.peek().is_none() {
        return Ok(None);
    }

    let mut remotes: Vec<SocketAddr> = Vec::new();
    let mut network = Network::Bitcoin;
    let mut fetch_block = None;
    let mut headers_db = None;
    let mut data_dir = None;
    let mut once = false;
    let mut explorer_listen = None;
    let mut vbparams = Vec::new();
    let mut test_activation_heights = Vec::new();
    let mut minimum_chainwork = None;
    let mut assume_valid = None;
    while let Some(argument) = args.next() {
        match argument.as_str() {
            "--help" | "-h" => return Ok(None),
            "--connect" => {
                let address = required_option_value(&mut args, "--connect")?;
                let remote = address
                    .parse()
                    .map_err(|_| format!("invalid peer address: {address}"))?;
                if !remotes.contains(&remote) {
                    if remotes.len() == MAX_CONFIGURED_PEERS {
                        return Err(format!(
                            "too many unique --connect peers; limit is {MAX_CONFIGURED_PEERS}"
                        ));
                    }
                    remotes.push(remote);
                }
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
            "--testactivationheight" => {
                test_activation_heights
                    .push(required_option_value(&mut args, "--testactivationheight")?);
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

    if remotes.is_empty() {
        return Err("--connect is required".to_owned());
    }
    if explorer_listen.is_some() && data_dir.is_none() {
        return Err("--explorer-listen requires --data-dir".to_owned());
    }
    if data_dir.is_some() && !supports_block_execution(network) {
        return Err(
            "--data-dir block execution currently supports only regtest and default signet"
                .to_owned(),
        );
    }
    let deployments = parse_deployment_config(
        network,
        data_dir.is_some(),
        vbparams,
        test_activation_heights,
    )?;
    let ibd_policy = parse_ibd_policy(
        network,
        data_dir.is_some() || headers_db.is_some(),
        minimum_chainwork,
        assume_valid,
    )?;
    Ok(Some(Options {
        remotes,
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
    test_activation_heights: Vec<String>,
) -> Result<DeploymentConfig, String> {
    if (!vbparams.is_empty() || !test_activation_heights.is_empty()) && !has_data_dir {
        return Err("deployment overrides require --data-dir".to_owned());
    }
    let mut deployments = DeploymentConfig::for_network(network);
    for value in vbparams {
        deployments
            .apply_vbparams(&value)
            .map_err(|error| error.to_string())?;
    }
    for value in test_activation_heights {
        deployments
            .apply_test_activation_height(&value)
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
        "rbtcd {}\n\nUSAGE:\n  rbtcd --connect HOST:PORT [--connect HOST:PORT ...] [--network bitcoin|testnet|testnet4|signet|regtest]\n  rbtcd --connect HOST:PORT [--connect HOST:PORT ...] --headers-db PATH [--network NETWORK] [--minimum-chainwork HEX] [--assumevalid HASH|0]\n  rbtcd --connect HOST:PORT [--connect HOST:PORT ...] --data-dir PATH --network regtest|signet [--once] [--explorer-listen 127.0.0.1:3000] [--vbparams taproot:START:END[:MIN_HEIGHT]] [--testactivationheight NAME@HEIGHT] [--minimum-chainwork HEX] [--assumevalid HASH|0]\n  rbtcd --connect HOST:PORT [--connect HOST:PORT ...] --fetch-block BLOCK_HASH [--network NETWORK]",
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
        hex::FromHex,
        p2p::{Address, ServiceFlags, message::NetworkMessage, message_network::VersionMessage},
        pow::Target,
        transaction::Version as TransactionVersion,
    };
    use rbtc::{
        api::ExplorerIndex,
        blockchain::block_subsidy,
        chain_store::RedbChainStore,
        header_store::RedbHeaderStore,
        ledger::{LedgerRetention, PrunedBlockLedger},
        p2p::{PeerAddress, V1Transport},
        utxo::{OutPointKey, UtxoStore},
    };
    use tempfile::TempDir;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    fn peer_version(nonce: u64) -> VersionMessage {
        let receiver: SocketAddr = "127.0.0.1:18444".parse().unwrap();
        let sender: SocketAddr = "0.0.0.0:0".parse().unwrap();
        let mut version = VersionMessage::new(
            ServiceFlags::NETWORK | ServiceFlags::WITNESS,
            0,
            Address::new(&receiver, ServiceFlags::NONE),
            Address::new(&sender, ServiceFlags::NONE),
            nonce,
            "/rbtcd:test-peer/".to_owned(),
            1,
        );
        version.version = 70_016;
        version
    }

    async fn receive_client_negotiation(peer: &mut V1Transport<tokio::net::TcpStream>) {
        assert!(matches!(
            peer.read_message().await.unwrap().into_payload(),
            NetworkMessage::WtxidRelay
        ));
        assert!(matches!(
            peer.read_message().await.unwrap().into_payload(),
            NetworkMessage::SendAddrV2
        ));
        assert!(matches!(
            peer.read_message().await.unwrap().into_payload(),
            NetworkMessage::Verack
        ));
    }

    async fn respond_to_getaddr(peer: &mut V1Transport<tokio::net::TcpStream>) {
        respond_to_getaddr_with(peer, Vec::new()).await;
    }

    async fn respond_to_getaddr_with(
        peer: &mut V1Transport<tokio::net::TcpStream>,
        addresses: Vec<bitcoin::p2p::address::AddrV2Message>,
    ) {
        assert!(matches!(
            peer.read_message().await.unwrap().into_payload(),
            NetworkMessage::GetAddr
        ));
        peer.write_message(NetworkMessage::AddrV2(addresses))
            .await
            .unwrap();
    }

    async fn accept_peer(
        listener: TcpListener,
        version: VersionMessage,
    ) -> (V1Transport<tokio::net::TcpStream>, VersionMessage) {
        let (stream, _) = listener.accept().await.unwrap();
        let mut peer = V1Transport::new(stream, Network::Regtest.magic());
        let NetworkMessage::Version(local_version) =
            peer.read_message().await.unwrap().into_payload()
        else {
            panic!("expected version");
        };
        peer.write_message(NetworkMessage::Version(version))
            .await
            .unwrap();
        receive_client_negotiation(&mut peer).await;
        peer.write_message(NetworkMessage::Verack).await.unwrap();
        (peer, local_version)
    }

    fn mine_regtest_child(parent: BlockHash, time: u32) -> Header {
        let target = Target::MAX_ATTAINABLE_REGTEST;
        let mut header = Header {
            version: Version::from_consensus(4),
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
                script_sig: ScriptBuf::from_bytes(vec![0x51, 0x00]),
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
                version: Version::from_consensus(4),
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
    #[allow(clippy::too_many_lines)]
    fn parses_a_header_probe() {
        let options = parse_options(
            ["--connect", "127.0.0.1:18444", "--network", "regtest"]
                .into_iter()
                .map(str::to_owned),
        )
        .unwrap()
        .unwrap();
        assert_eq!(options.remotes, vec!["127.0.0.1:18444".parse().unwrap()]);
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
                "--testactivationheight",
                "bip34@10",
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
        assert_eq!(served.deployments.consensus_id().len(), 49);
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
                    "--testactivationheight",
                    "csv@10",
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

    #[test]
    fn parses_deduplicates_and_bounds_peer_candidates() {
        let options = parse_options(
            [
                "--connect",
                "127.0.0.1:18444",
                "--connect",
                "127.0.0.1:18445",
                "--connect",
                "127.0.0.1:18444",
                "--network",
                "regtest",
            ]
            .into_iter()
            .map(str::to_owned),
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            options.remotes,
            vec![
                "127.0.0.1:18444".parse().unwrap(),
                "127.0.0.1:18445".parse().unwrap()
            ]
        );

        let mut arguments = Vec::new();
        for port in 20_000..=(20_000 + MAX_CONFIGURED_PEERS) {
            arguments.push("--connect".to_owned());
            arguments.push(format!("127.0.0.1:{port}"));
        }
        let Err(error) = parse_options(arguments.into_iter()) else {
            panic!("the peer candidate bound must be enforced");
        };
        assert!(error.contains("too many unique --connect peers"));
    }

    #[test]
    fn data_dir_execution_network_gate_is_enforced_before_connect() {
        for network in ["regtest", "signet"] {
            let options = parse_options(
                [
                    "--connect",
                    "127.0.0.1:1",
                    "--network",
                    network,
                    "--data-dir",
                    "/tmp/rbtc-execution-network-gate",
                ]
                .into_iter()
                .map(str::to_owned),
            )
            .unwrap()
            .unwrap();
            assert_eq!(options.network.to_string(), network);
        }

        for network in ["bitcoin", "testnet", "testnet4"] {
            let result = parse_options(
                [
                    "--connect",
                    "127.0.0.1:1",
                    "--network",
                    network,
                    "--data-dir",
                    "/tmp/rbtc-execution-network-gate",
                ]
                .into_iter()
                .map(str::to_owned),
            );
            let Err(error) = result else {
                panic!("{network} execution must remain safety-gated");
            };
            assert!(error.contains("only regtest and default signet"));
        }
    }

    #[tokio::test]
    async fn legacy_chainstate_is_rejected_before_peer_store_creation_or_connect() {
        let directory = TempDir::new().unwrap();
        fs::write(directory.path().join("undo.redb"), b"legacy").unwrap();

        let error = run(Options {
            remotes: vec!["127.0.0.1:1".parse().unwrap()],
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
        .unwrap_err();

        assert!(error.contains("legacy split chain-state file"));
        assert!(!directory.path().join("peers.redb").exists());
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
            receive_client_negotiation(&mut peer).await;
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
            remotes: vec![remote],
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
    async fn daemon_uses_persisted_fallback_and_cools_failed_learned_peer() {
        let live_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let live_remote = live_listener.local_addr().unwrap();
        let explicit_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let explicit_remote = explicit_listener.local_addr().unwrap();
        let failed_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let failed_remote = failed_listener.local_addr().unwrap();
        drop(explicit_listener);
        drop(failed_listener);

        let server = tokio::spawn(async move {
            let (mut peer, _) = accept_peer(live_listener, peer_version(25)).await;
            respond_to_getaddr(&mut peer).await;
            match peer.read_message().await.unwrap().into_payload() {
                NetworkMessage::GetHeaders(_) => peer
                    .write_message(NetworkMessage::Headers(Vec::new()))
                    .await
                    .unwrap(),
                message => panic!("expected learned-peer getheaders, got {message:?}"),
            }
        });

        let directory = TempDir::new().unwrap();
        let now = unix_time().unwrap();
        let store =
            RedbPeerStore::open(directory.path().join("peers.redb"), Network::Regtest).unwrap();
        let services = ServiceFlags::NETWORK | ServiceFlags::WITNESS;
        store
            .insert_discovered(
                "127.0.0.10:18444".parse().unwrap(),
                &[
                    PeerAddress {
                        socket: failed_remote,
                        services,
                        last_seen: now,
                    },
                    PeerAddress {
                        socket: live_remote,
                        services,
                        last_seen: now.saturating_sub(1),
                    },
                ],
                now,
            )
            .unwrap();
        drop(store);

        run(Options {
            remotes: vec![explicit_remote],
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

        let reopened =
            RedbPeerStore::open(directory.path().join("peers.redb"), Network::Regtest).unwrap();
        assert_eq!(
            reopened.candidates(unix_time().unwrap(), 16).unwrap(),
            vec![live_remote]
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
            receive_client_negotiation(&mut peer).await;
            peer.write_message(NetworkMessage::Verack).await.unwrap();
            respond_to_getaddr_with(
                &mut peer,
                vec![bitcoin::p2p::address::AddrV2Message {
                    time: 0,
                    services: ServiceFlags::NETWORK | ServiceFlags::WITNESS,
                    addr: bitcoin::p2p::address::AddrV2::Ipv4("127.0.0.2".parse().unwrap()),
                    port: 18_445,
                }],
            )
            .await;
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
            remotes: vec![remote],
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

        let chainstate =
            RedbChainStore::open(directory.path().join("chainstate.redb"), Network::Regtest)
                .unwrap();
        assert_eq!(chainstate.execution().tip().unwrap().height, 1);
        assert_eq!(chainstate.execution().tip().unwrap().hash, block_hash);
        assert!(chainstate.undos().get(block_hash).unwrap().is_some());
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
        let peers =
            RedbPeerStore::open(directory.path().join("peers.redb"), Network::Regtest).unwrap();
        assert_eq!(peers.len().unwrap(), 1);
        assert_eq!(
            peers.candidates(unix_time().unwrap(), 16).unwrap(),
            vec!["127.0.0.2:18445".parse().unwrap()]
        );
        drop(peers);

        let archived_bytes = serialize(&archived);
        ledger.truncate_from(1).unwrap();
        ledger.stage(1, &[archived_bytes]).unwrap();
        drop(ledger);
        drop(chainstate);

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
            receive_client_negotiation(&mut peer).await;
            peer.write_message(NetworkMessage::Verack).await.unwrap();
            respond_to_getaddr(&mut peer).await;
            assert!(matches!(
                peer.read_message().await.unwrap().into_payload(),
                NetworkMessage::GetHeaders(_)
            ));
            peer.write_message(NetworkMessage::Headers(Vec::new()))
                .await
                .unwrap();
        });
        run(Options {
            remotes: vec![recovery_remote],
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
            receive_client_negotiation(&mut peer).await;
            peer.write_message(NetworkMessage::Verack).await.unwrap();
            respond_to_getaddr(&mut peer).await;
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
            remotes: vec![backfill_remote],
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

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn daemon_fails_over_and_resumes_persisted_ibd_with_the_next_peer() {
        let genesis = bitcoin::blockdata::constants::genesis_block(Network::Regtest).header;
        let block = regtest_block(genesis.block_hash(), genesis.time + 1);
        let block_hash = block.block_hash();
        let coinbase_outpoint = OutPointKey::from(OutPoint::new(block.txdata[0].compute_txid(), 0));

        let deficient_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let deficient_remote = deficient_listener.local_addr().unwrap();
        let deficient_server = tokio::spawn(async move {
            let mut version = peer_version(10);
            version.services = ServiceFlags::NETWORK;
            let (_peer, local_version) = accept_peer(deficient_listener, version).await;
            local_version.nonce
        });

        let interrupted_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let interrupted_remote = interrupted_listener.local_addr().unwrap();
        let interrupted_header = block.header;
        let interrupted_server = tokio::spawn(async move {
            let (mut peer, local_version) =
                accept_peer(interrupted_listener, peer_version(11)).await;
            respond_to_getaddr(&mut peer).await;
            match peer.read_message().await.unwrap().into_payload() {
                NetworkMessage::GetHeaders(request) => {
                    assert_eq!(request.locator_hashes, vec![genesis.block_hash()]);
                    peer.write_message(NetworkMessage::Headers(vec![interrupted_header]))
                        .await
                        .unwrap();
                }
                message => panic!("expected initial getheaders, got {message:?}"),
            }
            assert!(matches!(
                peer.read_message().await.unwrap().into_payload(),
                NetworkMessage::GetData(inventory)
                    if inventory == vec![bitcoin::p2p::message_blockdata::Inventory::WitnessBlock(block_hash)]
            ));
            local_version.nonce
        });

        let recovery_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let recovery_remote = recovery_listener.local_addr().unwrap();
        let recovery_server = tokio::spawn(async move {
            let (mut peer, local_version) = accept_peer(recovery_listener, peer_version(12)).await;
            respond_to_getaddr(&mut peer).await;
            match peer.read_message().await.unwrap().into_payload() {
                NetworkMessage::GetHeaders(request) => {
                    assert_eq!(request.locator_hashes.first(), Some(&block_hash));
                    peer.write_message(NetworkMessage::Headers(Vec::new()))
                        .await
                        .unwrap();
                }
                message => panic!("expected resumed getheaders, got {message:?}"),
            }
            assert!(matches!(
                peer.read_message().await.unwrap().into_payload(),
                NetworkMessage::GetData(inventory)
                    if inventory == vec![bitcoin::p2p::message_blockdata::Inventory::WitnessBlock(block_hash)]
            ));
            peer.write_message(NetworkMessage::Block(block))
                .await
                .unwrap();
            local_version.nonce
        });

        let directory = TempDir::new().unwrap();
        let local_nonce = 99;
        run_with_nonce(
            Options {
                remotes: vec![deficient_remote, interrupted_remote, recovery_remote],
                network: Network::Regtest,
                fetch_block: None,
                headers_db: None,
                data_dir: Some(directory.path().to_path_buf()),
                once: true,
                explorer_listen: None,
                deployments: DeploymentConfig::for_network(Network::Regtest),
                ibd_policy: IbdPolicy::for_network(Network::Regtest),
            },
            local_nonce,
        )
        .await
        .unwrap();
        let deficient_nonce = deficient_server.await.unwrap();
        let interrupted_nonce = interrupted_server.await.unwrap();
        let recovery_nonce = recovery_server.await.unwrap();
        assert_eq!(deficient_nonce, local_nonce);
        assert_eq!(interrupted_nonce, local_nonce);
        assert_eq!(recovery_nonce, local_nonce);

        let headers = RedbHeaderStore::open(directory.path().join("headers.redb")).unwrap();
        assert_eq!(
            headers
                .load_dag(Network::Regtest, unix_time().unwrap())
                .unwrap()
                .active_tip()
                .hash,
            block_hash
        );
        let chainstate =
            RedbChainStore::open(directory.path().join("chainstate.redb"), Network::Regtest)
                .unwrap();
        assert_eq!(chainstate.execution().tip().unwrap().height, 1);
        assert_eq!(chainstate.execution().tip().unwrap().hash, block_hash);
        assert!(chainstate.undos().get(block_hash).unwrap().is_some());
        assert!(chainstate.get(coinbase_outpoint).unwrap().is_some());
        let ledger =
            PrunedBlockLedger::open(directory.path().join("blocks"), LedgerRetention::default())
                .unwrap();
        assert_eq!(ledger.retained_ranges().unwrap(), vec![(1, 1)]);
        assert!(ledger.staged().unwrap().is_none());
        let explorer =
            RedbExplorerIndex::open(directory.path().join("explorer.redb"), Network::Regtest)
                .unwrap();
        assert_eq!(explorer.tip().unwrap().height, 1);
    }

    #[tokio::test]
    async fn daemon_downloads_and_executes_a_real_default_signet_block() {
        let encoded = include_str!("../tests/data/bitcoin-core-26/signet-block-1.hex");
        let block: Block = deserialize(&Vec::<u8>::from_hex(encoded.trim()).unwrap()).unwrap();
        let block_hash = block.block_hash();
        let coinbase_outpoint = OutPointKey::from(OutPoint::new(block.txdata[0].compute_txid(), 0));
        let genesis = bitcoin::blockdata::constants::genesis_block(Network::Signet).block_hash();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let remote = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut peer = V1Transport::new(stream, Network::Signet.magic());
            assert!(matches!(
                peer.read_message().await.unwrap().into_payload(),
                NetworkMessage::Version(_)
            ));
            peer.write_message(NetworkMessage::Version(peer_version(9)))
                .await
                .unwrap();
            receive_client_negotiation(&mut peer).await;
            peer.write_message(NetworkMessage::Verack).await.unwrap();
            respond_to_getaddr(&mut peer).await;
            match peer.read_message().await.unwrap().into_payload() {
                NetworkMessage::GetHeaders(request) => {
                    assert_eq!(request.locator_hashes, vec![genesis]);
                    peer.write_message(NetworkMessage::Headers(vec![block.header]))
                        .await
                        .unwrap();
                }
                message => panic!("expected Signet getheaders, got {message:?}"),
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
                message => panic!("expected Signet getdata, got {message:?}"),
            }
        });

        let directory = TempDir::new().unwrap();
        let mut ibd_policy = IbdPolicy::for_network(Network::Signet);
        ibd_policy
            .set_minimum_chainwork(
                "0000000000000000000000000000000000000000000000000000000000000000",
            )
            .unwrap();
        ibd_policy.set_assume_valid("0").unwrap();
        run(Options {
            remotes: vec![remote],
            network: Network::Signet,
            fetch_block: None,
            headers_db: None,
            data_dir: Some(directory.path().to_path_buf()),
            once: true,
            explorer_listen: None,
            deployments: DeploymentConfig::for_network(Network::Signet),
            ibd_policy,
        })
        .await
        .unwrap();
        server.await.unwrap();

        let chainstate =
            RedbChainStore::open(directory.path().join("chainstate.redb"), Network::Signet)
                .unwrap();
        assert_eq!(chainstate.execution().tip().unwrap().height, 1);
        assert_eq!(chainstate.execution().tip().unwrap().hash, block_hash);
        assert!(chainstate.undos().get(block_hash).unwrap().is_some());
        assert!(chainstate.get(coinbase_outpoint).unwrap().is_some());
        let ledger =
            PrunedBlockLedger::open(directory.path().join("blocks"), LedgerRetention::default())
                .unwrap();
        assert_eq!(ledger.retained_ranges().unwrap(), vec![(1, 1)]);
        let explorer =
            RedbExplorerIndex::open(directory.path().join("explorer.redb"), Network::Signet)
                .unwrap();
        assert_eq!(explorer.tip().unwrap().height, 1);
        assert_eq!(
            explorer.block(1).unwrap().unwrap().hash,
            block_hash.to_string()
        );
    }
}
