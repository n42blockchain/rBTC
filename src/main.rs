//! Command line entry point for the rBTC node daemon.

use std::{
    collections::HashSet,
    env, fs,
    io::Read,
    net::{IpAddr, SocketAddr},
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
    hex::FromHex,
};
use rbtc::{
    api::{WalletAuthToken, explorer_router, wallet_router},
    block_execution::{
        BlockExecutionError, connect_active_block, connect_active_blocks, disconnect_execution_tip,
    },
    blockchain::{AppliedBlock, validate_block_structure_with_deployments},
    chain_store::RedbChainStore,
    deployments::{DeploymentConfig, block_deployment_context_for_headers, taproot_active},
    execution_store::RedbExecutionStore,
    explorer_store::RedbExplorerIndex,
    header_store::RedbHeaderStore,
    headers::{HeaderDag, HeaderError},
    ibd::IbdPolicy,
    ledger::{LedgerRetention, PrunedBlockLedger},
    p2p::{MAX_BLOCKS_IN_FLIGHT, MAX_HEADERS_PER_RESPONSE, P2pError, connect_outbound},
    peer_store::{RedbPeerStore, is_acceptable_peer_address},
    snapshot::{SnapshotTrustAnchor, verify_snapshot_with_trust},
    undo_store::RedbUndoStore,
    utxo::DEFAULT_HOT_WINDOW_SECS,
    wallet::{DEFAULT_WALLET_GAP_LIMIT, EmbeddedWallet, MAX_WALLET_GAP_LIMIT, WalletTip},
};
use tokio::time::timeout;

const PEER_TIMEOUT: Duration = Duration::from_secs(30);
const PEER_DISCOVERY_TIMEOUT: Duration = Duration::from_secs(3);
const DNS_SEED_TIMEOUT: Duration = Duration::from_secs(5);
const USER_AGENT: &str = "/rbtcd:0.1.0/";
const MAX_CONFIGURED_PEERS: usize = 16;
const MAX_DNS_SEEDS: usize = 16;
const MAX_DNS_ADDRESSES_PER_SEED: usize = 64;
const MAX_WALLET_DESCRIPTOR_FILE_LEN: u64 = 64 * 1024;
const MAX_WALLET_TOKEN_FILE_LEN: u64 = 1024;
const MAX_WALLET_SCAN_PASSES: usize = 64;

const fn supports_block_execution(network: Network) -> bool {
    matches!(network, Network::Regtest | Network::Signet)
}

struct Options {
    remotes: Vec<SocketAddr>,
    /// `None` selects the pinned Core 26 defaults; `Some` is an explicit override.
    dns_seeds: Option<Vec<DnsSeed>>,
    network: Network,
    fetch_block: Option<BlockHash>,
    headers_db: Option<PathBuf>,
    data_dir: Option<PathBuf>,
    once: bool,
    explorer_listen: Option<SocketAddr>,
    wallet_api_files: Option<WalletApiFiles>,
    deployments: DeploymentConfig,
    ibd_policy: IbdPolicy,
    snapshot: Option<SnapshotActivationOptions>,
}

struct SnapshotActivationOptions {
    path: PathBuf,
    height: u32,
    block_hash: BlockHash,
    utxo_count: u64,
    records_bytes: u64,
    records_sha256: String,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct DnsSeed {
    host: String,
    port: u16,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PeerFailureKind {
    Transient,
    ProtocolViolation,
}

#[derive(Debug)]
struct PeerRunError {
    kind: PeerFailureKind,
    message: String,
}

impl PeerRunError {
    fn transient(message: impl Into<String>) -> Self {
        Self {
            kind: PeerFailureKind::Transient,
            message: message.into(),
        }
    }

    fn p2p(error: &P2pError) -> Self {
        let kind = if error.is_protocol_violation() {
            PeerFailureKind::ProtocolViolation
        } else {
            PeerFailureKind::Transient
        };
        Self {
            kind,
            message: error.to_string(),
        }
    }

    fn protocol(message: impl Into<String>) -> Self {
        Self {
            kind: PeerFailureKind::ProtocolViolation,
            message: message.into(),
        }
    }

    fn header(error: &HeaderError) -> Self {
        if error.is_peer_invalid() {
            Self::protocol(error.to_string())
        } else {
            Self::transient(error.to_string())
        }
    }

    fn block(error: &BlockExecutionError) -> Self {
        if error.is_peer_invalid() {
            Self::protocol(error.to_string())
        } else {
            Self::transient(error.to_string())
        }
    }
}

impl From<String> for PeerRunError {
    fn from(message: String) -> Self {
        Self::transient(message)
    }
}

impl std::fmt::Display for PeerRunError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

struct WalletApiFiles {
    descriptors: PathBuf,
    auth_token: PathBuf,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct WalletDescriptorFile {
    receive_descriptor: String,
    change_descriptor: String,
    #[serde(default = "default_wallet_gap_limit")]
    gap_limit: u32,
    #[serde(default)]
    birthday_height: u32,
}

struct WalletApiRuntime {
    wallet: Arc<EmbeddedWallet>,
    token: WalletAuthToken,
    scan: WalletScanConfig,
}

#[derive(Clone, Copy)]
struct WalletScanConfig {
    gap_limit: u32,
    birthday_height: u32,
}

struct ApiRuntime {
    listen: SocketAddr,
    wallet: Option<WalletApiRuntime>,
}

struct ApiServer {
    #[cfg(test)]
    address: SocketAddr,
    task: Option<tokio::task::JoinHandle<Result<(), String>>>,
}

impl ApiServer {
    async fn bind(
        address: SocketAddr,
        index: Arc<RedbExplorerIndex>,
        wallet: Option<&WalletApiRuntime>,
    ) -> Result<Self, String> {
        let listener = tokio::net::TcpListener::bind(address)
            .await
            .map_err(|error| format!("bind explorer at {address}: {error}"))?;
        let bound = listener
            .local_addr()
            .map_err(|error| format!("read explorer address: {error}"))?;
        let mut router = explorer_router(index);
        if let Some(wallet) = wallet {
            router = router.merge(wallet_router(
                Arc::clone(&wallet.wallet),
                wallet.token.clone(),
            ));
        }
        println!("embedded API listening on http://{bound}");
        let task = tokio::spawn(async move {
            axum::serve(listener, router)
                .await
                .map_err(|error| format!("API server: {error}"))
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
            return Err("API server task missing".to_owned());
        };
        if !task.is_finished() {
            return Ok(());
        }
        self.task
            .take()
            .expect("finished explorer task exists")
            .await
            .map_err(|error| format!("API server task: {error}"))?
    }
}

impl Drop for ApiServer {
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

fn prepare_api_runtime(options: &Options) -> Result<Option<ApiRuntime>, String> {
    let Some(listen) = options.explorer_listen else {
        return Ok(None);
    };
    let wallet = options
        .wallet_api_files
        .as_ref()
        .map(|files| {
            let descriptors = read_owner_only_text_file(
                &files.descriptors,
                "wallet descriptor",
                MAX_WALLET_DESCRIPTOR_FILE_LEN,
            )?;
            let descriptors: WalletDescriptorFile =
                serde_json::from_str(&descriptors).map_err(|_| {
                    "wallet descriptor file must contain the documented JSON object".to_owned()
                })?;
            if !(1..=MAX_WALLET_GAP_LIMIT).contains(&descriptors.gap_limit) {
                return Err(format!(
                    "wallet gap_limit must be between 1 and {MAX_WALLET_GAP_LIMIT}"
                ));
            }
            let token = read_owner_only_text_file(
                &files.auth_token,
                "wallet authentication token",
                MAX_WALLET_TOKEN_FILE_LEN,
            )?;
            let token = WalletAuthToken::new(token.trim_end_matches(['\r', '\n']))
                .map_err(str::to_owned)?;
            let data_dir = options
                .data_dir
                .as_ref()
                .expect("wallet API parser requires data directory");
            let wallet = EmbeddedWallet::open_or_create(
                data_dir.join("wallet.sqlite"),
                descriptors.receive_descriptor,
                descriptors.change_descriptor,
                options.network,
            )
            .map_err(|_| {
                "wallet initialization failed; verify descriptors, network, database, and file permissions"
                    .to_owned()
            })?;
            println!(
                "authenticated watch-only wallet API enabled; token file {}",
                files.auth_token.display()
            );
            Ok::<_, String>(WalletApiRuntime {
                wallet: Arc::new(wallet),
                token,
                scan: WalletScanConfig {
                    gap_limit: descriptors.gap_limit,
                    birthday_height: descriptors.birthday_height,
                },
            })
        })
        .transpose()?;
    Ok(Some(ApiRuntime { listen, wallet }))
}

const fn default_wallet_gap_limit() -> u32 {
    DEFAULT_WALLET_GAP_LIMIT
}

fn read_owner_only_text_file(
    path: &std::path::Path,
    label: &str,
    limit: u64,
) -> Result<String, String> {
    let metadata = fs::symlink_metadata(path)
        .map_err(|error| format!("read {label} file metadata {}: {error}", path.display()))?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(format!(
            "{label} file must be a regular file, not a symlink"
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o077 != 0 {
            return Err(format!(
                "{label} file permissions must not grant group or other access"
            ));
        }
    }
    if metadata.len() > limit {
        return Err(format!("{label} file exceeds {limit} bytes"));
    }
    let file = fs::File::open(path)
        .map_err(|error| format!("open {label} file {}: {error}", path.display()))?;
    let mut contents = String::new();
    file.take(limit.saturating_add(1))
        .read_to_string(&mut contents)
        .map_err(|error| format!("read {label} file {}: {error}", path.display()))?;
    if u64::try_from(contents.len()).unwrap_or(u64::MAX) > limit {
        return Err(format!("{label} file exceeds {limit} bytes"));
    }
    Ok(contents)
}

#[allow(clippy::too_many_lines)]
async fn run_with_nonce(options: Options, local_nonce: u64) -> Result<(), String> {
    if let Some(data_dir) = &options.data_dir {
        preflight_data_dir(data_dir, options.network, &options.deployments)?;
    }
    if options.snapshot.is_some() {
        return activate_assumed_snapshot(&options);
    }
    let api_runtime = prepare_api_runtime(&options)?;
    let peer_store = if let Some(data_dir) = &options.data_dir {
        Some(
            RedbPeerStore::open(data_dir.join("peers.redb"), options.network)
                .map_err(|error| error.to_string())?,
        )
    } else {
        None
    };
    let manual_remotes = options.remotes.iter().copied().collect::<HashSet<_>>();
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
    let mut attempted = remotes.iter().copied().collect::<HashSet<_>>();
    let mut failures = Vec::with_capacity(MAX_CONFIGURED_PEERS);
    for remote in remotes {
        match run_with_peer(
            &options,
            remote,
            local_nonce,
            peer_store.as_ref(),
            api_runtime.as_ref(),
        )
        .await
        {
            Ok(()) => {
                if completed_validating_session(&options) {
                    record_peer_session_success(peer_store.as_ref(), remote);
                }
                return Ok(());
            }
            Err(error) => {
                eprintln!("peer {remote} failed: {error}");
                record_peer_failure(
                    peer_store.as_ref(),
                    remote,
                    error.kind,
                    manual_remotes.contains(&remote),
                );
                failures.push(format!("{remote}: {error}"));
            }
        }
    }

    let seeds = selected_dns_seeds(&options);
    if !seeds.is_empty() && attempted.len() < MAX_CONFIGURED_PEERS {
        let remaining = MAX_CONFIGURED_PEERS - attempted.len();
        let mut dns_excluded = attempted.clone();
        if let Some(store) = &peer_store {
            for discouraged in store
                .discouraged_addresses(unix_time()?)
                .map_err(|error| error.to_string())?
            {
                dns_excluded.insert(discouraged);
            }
        }
        let (resolved, resolution_failures, rejected) =
            resolve_dns_candidates(options.network, &seeds, &dns_excluded, remaining).await;
        for failure in resolution_failures {
            eprintln!("DNS seed failed: {failure}");
        }
        if !resolved.is_empty() || rejected > 0 {
            println!(
                "DNS bootstrap selected {} peer candidates and rejected {rejected} ineligible or duplicate addresses",
                resolved.len()
            );
        }
        for remote in resolved {
            attempted.insert(remote);
            match run_with_peer(
                &options,
                remote,
                local_nonce,
                peer_store.as_ref(),
                api_runtime.as_ref(),
            )
            .await
            {
                Ok(()) => {
                    if completed_validating_session(&options) {
                        record_peer_session_success(peer_store.as_ref(), remote);
                    }
                    return Ok(());
                }
                Err(error) => {
                    eprintln!("peer {remote} failed: {error}");
                    record_peer_failure(peer_store.as_ref(), remote, error.kind, false);
                    failures.push(format!("{remote}: {error}"));
                }
            }
        }
    }
    if attempted.is_empty() {
        return Err(
            "no peer candidates available; provide --connect, enable/configure DNS seeds, or retain a verified peer database"
                .to_owned(),
        );
    }
    Err(format!(
        "all {} peer candidates failed: {}",
        failures.len(),
        failures.join("; ")
    ))
}

fn activate_assumed_snapshot(options: &Options) -> Result<(), String> {
    let snapshot = options
        .snapshot
        .as_ref()
        .expect("caller checked snapshot activation mode");
    let data_dir = options
        .data_dir
        .as_ref()
        .expect("snapshot parser requires data directory");
    let header_store =
        RedbHeaderStore::open(data_dir.join("headers.redb")).map_err(|error| error.to_string())?;
    let headers = header_store
        .load_dag_with_deployments(options.deployments.clone(), unix_time()?)
        .map_err(|error| error.to_string())?;
    let trusted = SnapshotTrustAnchor::new(
        options.network,
        snapshot.height,
        snapshot.block_hash,
        snapshot.utxo_count,
        snapshot.records_bytes,
        snapshot.records_sha256.clone(),
    )
    .map_err(|error| error.to_string())?;
    let chainstate = RedbChainStore::open(data_dir.join("chainstate.redb"), options.network)
        .map_err(|error| error.to_string())?;
    let now = u64::from(unix_time()?);
    let manifest = verify_snapshot_with_trust(&snapshot.path, &trusted)
        .and_then(|verified| {
            verified.assume_into(
                &chainstate,
                &headers,
                &trusted,
                now,
                DEFAULT_HOT_WINDOW_SECS,
            )
        })
        .map_err(|error| error.to_string())?;
    println!(
        "activated assumed UTXO snapshot at {}:{} with {} entries/{} canonical bytes; background genesis validation remains required",
        manifest.height, manifest.block_hash, manifest.utxo_count, manifest.records_bytes
    );
    Ok(())
}

fn preflight_data_dir(
    data_dir: &std::path::Path,
    network: Network,
    deployments: &DeploymentConfig,
) -> Result<(), String> {
    fs::create_dir_all(data_dir)
        .map_err(|error| format!("create data directory {}: {error}", data_dir.display()))?;
    reject_legacy_split_chainstate(data_dir)?;
    let chainstate = RedbChainStore::open(data_dir.join("chainstate.redb"), network)
        .map_err(|error| error.to_string())?;
    chainstate
        .execution()
        .bind_consensus_config(
            &deployments.consensus_id(),
            &DeploymentConfig::for_network(network).consensus_id(),
        )
        .map_err(|error| error.to_string())
}

fn completed_validating_session(options: &Options) -> bool {
    options.fetch_block.is_none() && (options.data_dir.is_some() || options.headers_db.is_some())
}

async fn run_with_peer(
    options: &Options,
    remote: SocketAddr,
    local_nonce: u64,
    peer_store: Option<&RedbPeerStore>,
    api_runtime: Option<&ApiRuntime>,
) -> Result<(), PeerRunError> {
    record_peer_attempt(peer_store, remote);
    let mut session = timeout(
        PEER_TIMEOUT,
        connect_outbound(
            remote,
            options.deployments.message_start(),
            local_nonce,
            USER_AGENT.to_owned(),
            0,
        ),
    )
    .await
    .map_err(|_| {
        PeerRunError::transient(format!(
            "peer handshake timed out after {} seconds",
            PEER_TIMEOUT.as_secs()
        ))
    })?
    .map_err(|error| PeerRunError::p2p(&error))?;

    let remote_version = session.remote_version();
    println!(
        "connected to {}: version={}, height={}, agent={}",
        remote, remote_version.version, remote_version.start_height, remote_version.user_agent
    );

    if let Some(hash) = options.fetch_block {
        session
            .ensure_full_witness_block_relay()
            .map_err(|error| PeerRunError::p2p(&error))?;
        timeout(PEER_TIMEOUT, session.request_witness_blocks(&[hash]))
            .await
            .map_err(|_| PeerRunError::transient("getdata request timed out"))?
            .map_err(|error| PeerRunError::p2p(&error))?;
        let block = timeout(PEER_TIMEOUT, session.receive_requested_block(hash))
            .await
            .map_err(|_| {
                PeerRunError::transient(format!(
                    "block response timed out after {} seconds",
                    PEER_TIMEOUT.as_secs()
                ))
            })?
            .map_err(|error| PeerRunError::p2p(&error))?;
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
            .map_err(|error| PeerRunError::p2p(&error))?;
        if let Some(store) = peer_store {
            record_verified_peer(store, remote, remote_version.services);
            discover_peer_addresses(&mut session, store, remote).await;
        }
        return sync_validating_node(
            &mut session,
            options.network,
            &options.deployments,
            options.ibd_policy,
            path.clone(),
            options.once,
            api_runtime,
        )
        .await;
    }

    if let Some(path) = &options.headers_db {
        let headers = sync_headers(&mut session, &options.deployments, path.clone()).await?;
        let status = options
            .ibd_policy
            .ensure_minimum_chainwork(&headers)
            .map_err(|error| PeerRunError::transient(error.to_string()))?;
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

fn record_peer_failure(
    store: Option<&RedbPeerStore>,
    remote: SocketAddr,
    kind: PeerFailureKind,
    manual: bool,
) {
    if kind != PeerFailureKind::ProtocolViolation || manual {
        return;
    }
    let Some(store) = store else {
        return;
    };
    let now = match unix_time() {
        Ok(now) => now,
        Err(error) => {
            eprintln!("protocol cooldown for {remote} skipped: {error}");
            return;
        }
    };
    match store.record_protocol_violation(remote, now) {
        Ok(until) => println!(
            "discouraged peer {remote} after an objective protocol violation until Unix time {until}"
        ),
        Err(error) => eprintln!("protocol cooldown for {remote} failed: {error}"),
    }
}

fn record_peer_session_success(store: Option<&RedbPeerStore>, remote: SocketAddr) {
    let Some(store) = store else {
        return;
    };
    let now = match unix_time() {
        Ok(now) => now,
        Err(error) => {
            eprintln!("peer session success for {remote} skipped: {error}");
            return;
        }
    };
    if let Err(error) = store.record_session_success(remote, now) {
        eprintln!("peer session success for {remote} failed: {error}");
    }
}

fn record_verified_peer(
    store: &RedbPeerStore,
    remote: SocketAddr,
    services: bitcoin::p2p::ServiceFlags,
) {
    let now = match unix_time() {
        Ok(now) => now,
        Err(error) => {
            eprintln!("peer success history for {remote} skipped: {error}");
            return;
        }
    };
    match store.insert_verified(remote, services, now) {
        Ok(true) => {}
        Ok(false) => eprintln!("verified peer {remote} was not eligible for persistence"),
        Err(error) => eprintln!("verified peer persistence for {remote} failed: {error}"),
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
    deployments: &DeploymentConfig,
    path: PathBuf,
) -> Result<HeaderDag, PeerRunError> {
    let store =
        RedbHeaderStore::open(path).map_err(|error| PeerRunError::transient(error.to_string()))?;
    let mut dag = store
        .load_dag_with_deployments(
            deployments.clone(),
            unix_time().map_err(PeerRunError::transient)?,
        )
        .map_err(|error| PeerRunError::transient(error.to_string()))?;
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
            .validate_batch_contextual(&headers, unix_time().map_err(PeerRunError::transient)?)
            .map_err(|error| PeerRunError::header(&error))?;
        store
            .append_batch(&headers)
            .map_err(|error| PeerRunError::transient(error.to_string()))?;
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
    deployment_config: &DeploymentConfig,
    ibd_policy: IbdPolicy,
    data_dir: PathBuf,
    once: bool,
    api_runtime: Option<&ApiRuntime>,
) -> Result<(), PeerRunError> {
    if !supports_block_execution(network) {
        return Err(PeerRunError::transient(
            "block execution is currently safety-gated to regtest and Signet until mainnet/testnet activation coverage is complete",
        ));
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
    let wallet_runtime = api_runtime.and_then(|api| api.wallet.as_ref());
    let wallet = wallet_runtime.map(|runtime| runtime.wallet.as_ref());
    let mut api_server: Option<ApiServer> = None;
    let mut headers = sync_headers(session, deployment_config, headers_path.clone()).await?;
    'resync: loop {
        if let Some(server) = &mut api_server {
            server.ensure_running().await?;
        }
        loop {
            if let Some(server) = &mut api_server {
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
        if let Some(wallet) = wallet_runtime {
            reconcile_wallet(
                session,
                deployment_config,
                &headers,
                execution_store,
                &ledger,
                &wallet.wallet,
                wallet.scan,
            )
            .await?;
        }
        if api_server.is_none() {
            if let Some(api) = api_runtime {
                api_server = Some(
                    ApiServer::bind(api.listen, Arc::clone(&explorer), api.wallet.as_ref()).await?,
                );
            }
        }

        loop {
            if let Some(server) = &mut api_server {
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
                        return Err(PeerRunError::transient(error.to_string()));
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
                wallet,
            )
            .await?;
            if let Some(wallet) = wallet_runtime {
                reconcile_wallet(
                    session,
                    deployment_config,
                    &headers,
                    execution_store,
                    &ledger,
                    &wallet.wallet,
                    wallet.scan,
                )
                .await?;
            }
        }
    }
}

#[allow(clippy::too_many_lines)]
async fn reconcile_wallet(
    session: &mut rbtc::p2p::PeerSession<tokio::net::TcpStream>,
    deployment_config: &DeploymentConfig,
    headers: &HeaderDag,
    execution_store: &RedbExecutionStore,
    ledger: &PrunedBlockLedger,
    wallet: &EmbeddedWallet,
    scan: WalletScanConfig,
) -> Result<(), String> {
    let execution_tip = execution_store.tip().map_err(|error| error.to_string())?;
    let initial_lookahead_changed = wallet
        .ensure_scan_lookahead(scan.gap_limit)
        .map_err(|error| error.to_string())?;
    let wallet_tip = wallet.chain_tip().map_err(|error| error.to_string())?;
    let common = wallet
        .chain_checkpoints()
        .map_err(|error| error.to_string())?
        .into_iter()
        .find(|checkpoint| {
            checkpoint.height <= execution_tip.height
                && headers
                    .active_header_at(checkpoint.height)
                    .is_some_and(|header| header.hash == checkpoint.hash)
        })
        .ok_or_else(|| "wallet has no checkpoint in common with the active chain".to_owned())?;
    if common != wallet_tip {
        wallet
            .rewind_to(common)
            .map_err(|error| error.to_string())?;
        println!(
            "disconnected stale wallet tip; rewound to {}:{}",
            common.height, common.hash
        );
    }

    let configured_scan_start = scan.birthday_height.max(1);
    let stored_scan_start = wallet
        .scan_start_height()
        .map_err(|error| error.to_string())?;
    let scan_start = stored_scan_start.map_or(configured_scan_start, |stored| {
        stored.min(configured_scan_start)
    });
    let skip_to = scan_start.saturating_sub(1).min(execution_tip.height);
    let wallet_tip = wallet.chain_tip().map_err(|error| error.to_string())?;
    if wallet_tip.height < skip_to {
        let header = headers
            .active_header_at(skip_to)
            .ok_or_else(|| format!("missing active header at wallet birthday {skip_to}"))?;
        wallet
            .advance_checkpoint(WalletTip {
                height: header.height,
                hash: header.hash,
            })
            .map_err(|error| error.to_string())?;
    }

    let normal_start = wallet
        .chain_tip()
        .map_err(|error| error.to_string())?
        .height
        .checked_add(1)
        .ok_or_else(|| "wallet height overflow".to_owned())?;
    replay_wallet_blocks(
        session,
        deployment_config,
        headers,
        ledger,
        wallet,
        normal_start,
        execution_tip.height,
    )
    .await?;

    let boundary_needs_rescan = stored_scan_start.map_or(normal_start > scan_start, |stored| {
        configured_scan_start < stored
    });
    let mut scan_needed =
        boundary_needs_rescan || (initial_lookahead_changed && normal_start > scan_start);
    if !scan_needed {
        scan_needed = wallet
            .ensure_scan_lookahead(scan.gap_limit)
            .map_err(|error| error.to_string())?;
    }
    for pass in 0..MAX_WALLET_SCAN_PASSES {
        if !scan_needed {
            wallet
                .record_scan_start_height(scan_start)
                .map_err(|error| error.to_string())?;
            return Ok(());
        }
        replay_wallet_blocks(
            session,
            deployment_config,
            headers,
            ledger,
            wallet,
            scan_start,
            execution_tip.height,
        )
        .await?;
        scan_needed = wallet
            .ensure_scan_lookahead(scan.gap_limit)
            .map_err(|error| error.to_string())?;
        if !scan_needed {
            wallet
                .record_scan_start_height(scan_start)
                .map_err(|error| error.to_string())?;
            println!(
                "wallet descriptor scan converged after {} pass(es)",
                pass + 1
            );
            return Ok(());
        }
    }
    Err(format!(
        "wallet descriptor scan exceeded {MAX_WALLET_SCAN_PASSES} lookahead passes"
    ))
}

#[allow(clippy::too_many_arguments)]
async fn replay_wallet_blocks(
    session: &mut rbtc::p2p::PeerSession<tokio::net::TcpStream>,
    deployment_config: &DeploymentConfig,
    headers: &HeaderDag,
    ledger: &PrunedBlockLedger,
    wallet: &EmbeddedWallet,
    mut next_height: u32,
    end_height: u32,
) -> Result<(), String> {
    while next_height <= end_height {
        let remaining = end_height - next_height + 1;
        let batch_len = usize::try_from(remaining)
            .unwrap_or(usize::MAX)
            .min(MAX_BLOCKS_IN_FLIGHT);
        let expected = (0..batch_len)
            .map(|offset| {
                let height = next_height
                    .checked_add(u32::try_from(offset).expect("wallet batch fits u32"))
                    .ok_or_else(|| "wallet height overflow".to_owned())?;
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
                .map_err(|_| format!("wallet backfill getdata timed out for {batch_len} blocks"))?
                .map_err(|error| error.to_string())?;
            blocks = timeout(PEER_TIMEOUT, session.receive_requested_blocks(&hashes))
                .await
                .map_err(|_| format!("wallet backfill response timed out for {batch_len} blocks"))?
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
            wallet
                .apply_validated_block(block, expected.height)
                .map_err(|error| error.to_string())?;
        }
        let last_height = expected
            .last()
            .expect("wallet replay batch is non-empty")
            .height;
        if last_height == end_height {
            return Ok(());
        }
        next_height = last_height
            .checked_add(1)
            .ok_or_else(|| "wallet height overflow".to_owned())?;
    }
    Ok(())
}

async fn reconcile_ledger(
    session: &mut rbtc::p2p::PeerSession<tokio::net::TcpStream>,
    deployment_config: &DeploymentConfig,
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
    deployment_config: &DeploymentConfig,
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
    deployment_config: &DeploymentConfig,
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
    let deployments = block_deployment_context_for_headers(
        deployment_config,
        headers,
        height,
        expected_hash,
        block.header.time,
        taproot_active(headers, height, deployment_config).map_err(|error| error.to_string())?,
    )
    .map_err(|error| error.to_string())?;
    validate_block_structure_with_deployments(
        block,
        height,
        deployments.bip34_active,
        deployments.segwit_active,
        deployments.signet_challenge.as_deref(),
    )
    .map_err(|error| format!("archive block structure at height {height}: {error}"))
}

#[allow(clippy::too_many_arguments)]
async fn reconcile_explorer(
    session: &mut rbtc::p2p::PeerSession<tokio::net::TcpStream>,
    deployment_config: &DeploymentConfig,
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

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
async fn download_execute_batch(
    session: &mut rbtc::p2p::PeerSession<tokio::net::TcpStream>,
    deployment_config: &DeploymentConfig,
    headers: &HeaderDag,
    chainstate: &RedbChainStore,
    ledger: &PrunedBlockLedger,
    explorer: &RedbExplorerIndex,
    wallet: Option<&EmbeddedWallet>,
) -> Result<(), PeerRunError> {
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
        .map_err(|_| PeerRunError::transient(format!("getdata timed out for {batch_len} blocks")))?
        .map_err(|error| PeerRunError::p2p(&error))?;
    let blocks = timeout(PEER_TIMEOUT, session.receive_requested_blocks(&hashes))
        .await
        .map_err(|_| {
            PeerRunError::transient(format!(
                "block batch response timed out for {batch_len} blocks"
            ))
        })?
        .map_err(|error| PeerRunError::p2p(&error))?;
    for (expected, block) in expected.iter().zip(&blocks) {
        validate_downloaded_block(
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
            block_deployment_context_for_headers(
                deployment_config,
                headers,
                expected.height,
                expected.hash,
                block.header.time,
                taproot_active(headers, expected.height, deployment_config)
                    .map_err(|error| error.to_string())?,
            )
            .map_err(|error| error.to_string())
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
                &deployment_contexts[0],
            )
            .map_err(|error| PeerRunError::block(&error))?,
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
        .map_err(|error| PeerRunError::block(&error))?
    };
    for ((expected, block), applied) in expected.into_iter().zip(&blocks).zip(&applied_blocks) {
        index_validated_block(explorer, wallet, expected.height, block, applied)?;
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

fn validate_downloaded_block(
    deployment_config: &DeploymentConfig,
    headers: &HeaderDag,
    height: u32,
    expected_hash: BlockHash,
    block: &Block,
) -> Result<(), PeerRunError> {
    let actual = block.block_hash();
    if actual != expected_hash {
        return Err(PeerRunError::protocol(format!(
            "downloaded block {actual} does not match active block {expected_hash} at height {height}"
        )));
    }
    let taproot = taproot_active(headers, height, deployment_config)
        .map_err(|error| PeerRunError::transient(error.to_string()))?;
    let deployments = block_deployment_context_for_headers(
        deployment_config,
        headers,
        height,
        expected_hash,
        block.header.time,
        taproot,
    )
    .map_err(|error| PeerRunError::transient(error.to_string()))?;
    validate_block_structure_with_deployments(
        block,
        height,
        deployments.bip34_active,
        deployments.segwit_active,
        deployments.signet_challenge.as_deref(),
    )
    .map_err(|error| {
        PeerRunError::protocol(format!(
            "downloaded block structure at height {height}: {error}"
        ))
    })
}

fn index_validated_block(
    explorer: &RedbExplorerIndex,
    wallet: Option<&EmbeddedWallet>,
    height: u32,
    block: &Block,
    applied: &AppliedBlock,
) -> Result<(), String> {
    explorer
        .connect(height, block, applied)
        .map_err(|error| error.to_string())?;
    if let Some(wallet) = wallet {
        wallet
            .apply_validated_block(block, height)
            .map_err(|error| error.to_string())?;
    }
    Ok(())
}

async fn request_headers(
    session: &mut rbtc::p2p::PeerSession<tokio::net::TcpStream>,
    locator: Vec<BlockHash>,
) -> Result<(), PeerRunError> {
    timeout(
        PEER_TIMEOUT,
        session.request_headers(locator, BlockHash::all_zeros()),
    )
    .await
    .map_err(|_| PeerRunError::transient("getheaders request timed out"))?
    .map_err(|error| PeerRunError::p2p(&error))
}

async fn receive_headers(
    session: &mut rbtc::p2p::PeerSession<tokio::net::TcpStream>,
) -> Result<Vec<bitcoin::block::Header>, PeerRunError> {
    timeout(PEER_TIMEOUT, session.receive_headers())
        .await
        .map_err(|_| {
            PeerRunError::transient(format!(
                "headers response timed out after {} seconds",
                PEER_TIMEOUT.as_secs()
            ))
        })?
        .map_err(|error| PeerRunError::p2p(&error))
}

fn unix_time() -> Result<u32, String> {
    let seconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| "system clock is before the Unix epoch".to_owned())?
        .as_secs();
    u32::try_from(seconds)
        .map_err(|_| "system clock does not fit Bitcoin timestamp range".to_owned())
}

fn selected_dns_seeds(options: &Options) -> Vec<DnsSeed> {
    options.dns_seeds.clone().unwrap_or_else(|| {
        core26_dns_seed_hosts(options.network)
            .iter()
            .map(|host| DnsSeed {
                host: (*host).to_owned(),
                port: default_p2p_port(options.network),
            })
            .collect()
    })
}

const fn core26_dns_seed_hosts(network: Network) -> &'static [&'static str] {
    match network {
        Network::Bitcoin => &[
            "seed.bitcoin.sipa.be.",
            "dnsseed.bluematt.me.",
            "dnsseed.bitcoin.dashjr.org.",
            "seed.bitcoinstats.com.",
            "seed.bitcoin.jonasschnelli.ch.",
            "seed.btc.petertodd.org.",
            "seed.bitcoin.sprovoost.nl.",
            "dnsseed.emzy.de.",
            "seed.bitcoin.wiz.biz.",
        ],
        Network::Testnet => &[
            "testnet-seed.bitcoin.jonasschnelli.ch.",
            "seed.tbtc.petertodd.org.",
            "seed.testnet.bitcoin.sprovoost.nl.",
            "testnet-seed.bluematt.me.",
        ],
        Network::Signet => &["seed.signet.bitcoin.sprovoost.nl.", "178.128.221.177"],
        // Core 26 predates testnet4. Regtest intentionally has no public bootstrap source.
        Network::Testnet4 | Network::Regtest => &[],
    }
}

const fn default_p2p_port(network: Network) -> u16 {
    match network {
        Network::Bitcoin => 8_333,
        Network::Testnet => 18_333,
        Network::Testnet4 => 48_333,
        Network::Signet => 38_333,
        Network::Regtest => 18_444,
    }
}

async fn resolve_dns_candidates(
    network: Network,
    seeds: &[DnsSeed],
    excluded: &HashSet<SocketAddr>,
    limit: usize,
) -> (Vec<SocketAddr>, Vec<String>, usize) {
    let mut lookups = tokio::task::JoinSet::new();
    let seed_count = seeds.len().min(MAX_DNS_SEEDS);
    for (index, seed) in seeds.iter().take(MAX_DNS_SEEDS).cloned().enumerate() {
        lookups.spawn(async move {
            let host = seed.host;
            let port = seed.port;
            let lookup = timeout(
                DNS_SEED_TIMEOUT,
                tokio::net::lookup_host((host.as_str(), port)),
            )
            .await;
            let result = match lookup {
                Ok(Ok(addresses)) => Ok(addresses
                    .take(MAX_DNS_ADDRESSES_PER_SEED)
                    .collect::<Vec<_>>()),
                Ok(Err(error)) => Err(format!("{host}:{port}: {error}")),
                Err(_) => Err(format!(
                    "{}:{} timed out after {} seconds",
                    host,
                    port,
                    DNS_SEED_TIMEOUT.as_secs()
                )),
            };
            (index, result)
        });
    }

    let mut batches = vec![None; seed_count];
    let mut failures = Vec::new();
    while let Some(result) = lookups.join_next().await {
        match result {
            Ok((index, Ok(addresses))) => batches[index] = Some(addresses),
            Ok((_index, Err(error))) => failures.push(error),
            Err(error) => failures.push(format!("resolver task failed: {error}")),
        }
    }
    let resolved = round_robin_addresses(&batches);
    let (candidates, rejected) = filter_resolved_peer_addresses(network, resolved, excluded, limit);
    (candidates, failures, rejected)
}

fn round_robin_addresses(batches: &[Option<Vec<SocketAddr>>]) -> Vec<SocketAddr> {
    let capacity = batches
        .iter()
        .flatten()
        .map(|batch| batch.len().min(MAX_DNS_ADDRESSES_PER_SEED))
        .sum::<usize>();
    let mut addresses = Vec::with_capacity(capacity);
    for offset in 0..MAX_DNS_ADDRESSES_PER_SEED {
        for batch in batches.iter().flatten() {
            if let Some(address) = batch.get(offset) {
                addresses.push(*address);
            }
        }
    }
    addresses
}

fn filter_resolved_peer_addresses(
    network: Network,
    resolved: impl IntoIterator<Item = SocketAddr>,
    excluded: &HashSet<SocketAddr>,
    limit: usize,
) -> (Vec<SocketAddr>, usize) {
    let limit = limit.min(MAX_CONFIGURED_PEERS);
    let mut selected = Vec::with_capacity(limit);
    let mut seen = excluded.clone();
    let mut rejected = 0;
    for address in resolved {
        if selected.len() == limit
            || !is_acceptable_peer_address(address, network)
            || !seen.insert(address)
        {
            rejected += 1;
        } else {
            selected.push(address);
        }
    }
    (selected, rejected)
}

fn parse_dns_seed(value: &str, network: Network) -> Result<DnsSeed, String> {
    if value.is_empty() || value.trim() != value {
        return Err(format!("invalid DNS seed: {value}"));
    }
    if let Ok(address) = value.parse::<SocketAddr>() {
        if address.port() == 0 {
            return Err(format!("invalid DNS seed port: {value}"));
        }
        return Ok(DnsSeed {
            host: address.ip().to_string(),
            port: address.port(),
        });
    }
    if let Ok(ip) = value.parse::<IpAddr>() {
        return Ok(DnsSeed {
            host: ip.to_string(),
            port: default_p2p_port(network),
        });
    }

    let (host, port) = match value.rsplit_once(':') {
        Some((host, port)) => {
            let port = port
                .parse::<u16>()
                .map_err(|_| format!("invalid DNS seed port: {value}"))?;
            (host, port)
        }
        None => (value, default_p2p_port(network)),
    };
    let dns_name = host.strip_suffix('.').unwrap_or(host);
    if port == 0
        || dns_name.is_empty()
        || dns_name.len() > 253
        || !dns_name.split('.').all(|label| {
            !label.is_empty()
                && label.len() <= 63
                && !label.starts_with('-')
                && !label.ends_with('-')
                && label
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        })
    {
        return Err(format!("invalid DNS seed: {value}"));
    }
    Ok(DnsSeed {
        host: host.to_ascii_lowercase(),
        port,
    })
}

#[allow(clippy::too_many_lines)]
fn parse_options(args: impl Iterator<Item = String>) -> Result<Option<Options>, String> {
    let mut args = args.peekable();
    if args.peek().is_none() {
        return Ok(None);
    }

    let mut remotes: Vec<SocketAddr> = Vec::new();
    let mut dns_seed_values = Vec::new();
    let mut no_dns_seeds = false;
    let mut network = Network::Bitcoin;
    let mut fetch_block = None;
    let mut headers_db = None;
    let mut data_dir = None;
    let mut once = false;
    let mut explorer_listen = None;
    let mut wallet_descriptors = None;
    let mut wallet_auth_token_file = None;
    let mut vbparams = Vec::new();
    let mut test_activation_heights = Vec::new();
    let mut minimum_chainwork = None;
    let mut assume_valid = None;
    let mut signet_challenges = Vec::new();
    let mut signet_seed_values = Vec::new();
    let mut snapshot_path = None;
    let mut snapshot_height = None;
    let mut snapshot_block_hash = None;
    let mut snapshot_utxo_count = None;
    let mut snapshot_records_bytes = None;
    let mut snapshot_records_sha256 = None;
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
            "--dns-seed" => {
                let seed = required_option_value(&mut args, "--dns-seed")?;
                if !dns_seed_values.contains(&seed) {
                    if dns_seed_values.len() == MAX_DNS_SEEDS {
                        return Err(format!(
                            "too many unique --dns-seed values; limit is {MAX_DNS_SEEDS}"
                        ));
                    }
                    dns_seed_values.push(seed);
                }
            }
            "--no-dns-seeds" => no_dns_seeds = true,
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
            "--wallet-descriptors" => {
                wallet_descriptors = Some(PathBuf::from(required_option_value(
                    &mut args,
                    "--wallet-descriptors",
                )?));
            }
            "--wallet-auth-token-file" => {
                wallet_auth_token_file = Some(PathBuf::from(required_option_value(
                    &mut args,
                    "--wallet-auth-token-file",
                )?));
            }
            "--vbparams" => {
                vbparams.push(required_option_value(&mut args, "--vbparams")?);
            }
            "--testactivationheight" => {
                test_activation_heights
                    .push(required_option_value(&mut args, "--testactivationheight")?);
            }
            "--signetchallenge" => {
                signet_challenges.push(required_option_value(&mut args, "--signetchallenge")?);
            }
            "--signetseednode" => {
                let seed = required_option_value(&mut args, "--signetseednode")?;
                if !signet_seed_values.contains(&seed) {
                    if signet_seed_values.len() == MAX_DNS_SEEDS {
                        return Err(format!(
                            "too many unique --signetseednode values; limit is {MAX_DNS_SEEDS}"
                        ));
                    }
                    signet_seed_values.push(seed);
                }
            }
            "--minimum-chainwork" => {
                minimum_chainwork = Some(required_option_value(&mut args, "--minimum-chainwork")?);
            }
            "--assumevalid" | "--assume-valid" => {
                assume_valid = Some(required_option_value(&mut args, "--assumevalid")?);
            }
            "--assumeutxo-snapshot" => {
                if snapshot_path.is_some() {
                    return Err(
                        "--assumeutxo-snapshot cannot be supplied more than once".to_owned()
                    );
                }
                snapshot_path = Some(PathBuf::from(required_option_value(
                    &mut args,
                    "--assumeutxo-snapshot",
                )?));
            }
            "--snapshot-height" => {
                if snapshot_height.is_some() {
                    return Err("--snapshot-height cannot be supplied more than once".to_owned());
                }
                let value = required_option_value(&mut args, "--snapshot-height")?;
                snapshot_height = Some(
                    value
                        .parse::<u32>()
                        .map_err(|_| format!("invalid snapshot height: {value}"))?,
                );
            }
            "--snapshot-blockhash" => {
                if snapshot_block_hash.is_some() {
                    return Err("--snapshot-blockhash cannot be supplied more than once".to_owned());
                }
                let value = required_option_value(&mut args, "--snapshot-blockhash")?;
                snapshot_block_hash = Some(
                    BlockHash::from_str(&value)
                        .map_err(|_| format!("invalid snapshot block hash: {value}"))?,
                );
            }
            "--snapshot-utxo-count" => {
                if snapshot_utxo_count.is_some() {
                    return Err(
                        "--snapshot-utxo-count cannot be supplied more than once".to_owned()
                    );
                }
                let value = required_option_value(&mut args, "--snapshot-utxo-count")?;
                snapshot_utxo_count = Some(
                    value
                        .parse::<u64>()
                        .map_err(|_| format!("invalid snapshot UTXO count: {value}"))?,
                );
            }
            "--snapshot-records-bytes" => {
                if snapshot_records_bytes.is_some() {
                    return Err(
                        "--snapshot-records-bytes cannot be supplied more than once".to_owned()
                    );
                }
                let value = required_option_value(&mut args, "--snapshot-records-bytes")?;
                snapshot_records_bytes = Some(
                    value
                        .parse::<u64>()
                        .map_err(|_| format!("invalid snapshot records length: {value}"))?,
                );
            }
            "--snapshot-records-sha256" => {
                if snapshot_records_sha256.is_some() {
                    return Err(
                        "--snapshot-records-sha256 cannot be supplied more than once".to_owned(),
                    );
                }
                snapshot_records_sha256 = Some(required_option_value(
                    &mut args,
                    "--snapshot-records-sha256",
                )?);
            }
            _ => return Err(format!("unknown option: {argument}")),
        }
    }

    if no_dns_seeds && !dns_seed_values.is_empty() {
        return Err("--dns-seed conflicts with --no-dns-seeds".to_owned());
    }
    if signet_challenges.len() > 1 {
        return Err("--signetchallenge cannot be supplied more than once".to_owned());
    }
    if (!signet_challenges.is_empty() || !signet_seed_values.is_empty())
        && network != Network::Signet
    {
        return Err("--signetchallenge and --signetseednode require --network signet".to_owned());
    }
    if !signet_seed_values.is_empty() && (!dns_seed_values.is_empty() || no_dns_seeds) {
        return Err("--signetseednode conflicts with --dns-seed and --no-dns-seeds".to_owned());
    }
    if !signet_seed_values.is_empty() {
        dns_seed_values = signet_seed_values;
    }
    if explorer_listen.is_some() && data_dir.is_none() {
        return Err("--explorer-listen requires --data-dir".to_owned());
    }
    let wallet_api_files = match (wallet_descriptors, wallet_auth_token_file) {
        (None, None) => None,
        (Some(descriptors), Some(auth_token)) => {
            if explorer_listen.is_none() || data_dir.is_none() {
                return Err(
                    "wallet API options require --data-dir and loopback --explorer-listen"
                        .to_owned(),
                );
            }
            Some(WalletApiFiles {
                descriptors,
                auth_token,
            })
        }
        _ => {
            return Err(
                "--wallet-descriptors and --wallet-auth-token-file must be supplied together"
                    .to_owned(),
            );
        }
    };
    let snapshot = match (
        snapshot_path,
        snapshot_height,
        snapshot_block_hash,
        snapshot_utxo_count,
        snapshot_records_bytes,
        snapshot_records_sha256,
    ) {
        (None, None, None, None, None, None) => None,
        (
            Some(path),
            Some(height),
            Some(block_hash),
            Some(utxo_count),
            Some(records_bytes),
            Some(records_sha256),
        ) => {
            SnapshotTrustAnchor::new(
                network,
                height,
                block_hash,
                utxo_count,
                records_bytes,
                records_sha256.clone(),
            )
            .map_err(|error| error.to_string())?;
            if data_dir.is_none() {
                return Err("--assumeutxo-snapshot requires --data-dir".to_owned());
            }
            if fetch_block.is_some()
                || headers_db.is_some()
                || once
                || explorer_listen.is_some()
                || !remotes.is_empty()
                || !dns_seed_values.is_empty()
                || no_dns_seeds
            {
                return Err(
                    "snapshot activation is offline and conflicts with peer, fetch, headers-db, once, explorer, and wallet modes"
                        .to_owned(),
                );
            }
            Some(SnapshotActivationOptions {
                path,
                height,
                block_hash,
                utxo_count,
                records_bytes,
                records_sha256,
            })
        }
        _ => {
            return Err(
                "--assumeutxo-snapshot requires --snapshot-height, --snapshot-blockhash, --snapshot-utxo-count, --snapshot-records-bytes, and --snapshot-records-sha256"
                    .to_owned(),
            );
        }
    };
    if data_dir.is_some() && !supports_block_execution(network) {
        return Err(
            "--data-dir block execution currently supports only regtest and Signet".to_owned(),
        );
    }
    let mut deployments = parse_deployment_config(
        network,
        data_dir.is_some(),
        vbparams,
        test_activation_heights,
    )?;
    if let Some(value) = signet_challenges.first() {
        let challenge = Vec::<u8>::from_hex(value)
            .map_err(|_| "invalid --signetchallenge; expected one hexadecimal script".to_owned())?;
        deployments
            .set_signet_challenge(challenge)
            .map_err(|error| error.to_string())?;
    }
    let ibd_policy = parse_ibd_policy(
        network,
        deployments.is_custom_signet(),
        data_dir.is_some() || headers_db.is_some(),
        minimum_chainwork,
        assume_valid,
    )?;
    let dns_seeds =
        if no_dns_seeds || (deployments.is_custom_signet() && dns_seed_values.is_empty()) {
            Some(Vec::new())
        } else if dns_seed_values.is_empty() {
            None
        } else {
            let mut seeds = Vec::with_capacity(dns_seed_values.len());
            for value in dns_seed_values {
                let seed = parse_dns_seed(&value, network)?;
                if !seeds.contains(&seed) {
                    seeds.push(seed);
                }
            }
            Some(seeds)
        };
    let has_dns_bootstrap = dns_seeds.as_ref().map_or_else(
        || !core26_dns_seed_hosts(network).is_empty(),
        |seeds| !seeds.is_empty(),
    );
    if remotes.is_empty() && !has_dns_bootstrap && data_dir.is_none() {
        return Err(
            "no peer bootstrap source; provide --connect or --dns-seed (a data directory may reuse verified peers)"
                .to_owned(),
        );
    }
    Ok(Some(Options {
        remotes,
        dns_seeds,
        network,
        fetch_block,
        headers_db,
        data_dir,
        once,
        explorer_listen,
        wallet_api_files,
        deployments,
        ibd_policy,
        snapshot,
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
    custom_signet: bool,
    sync_enabled: bool,
    minimum_chainwork: Option<String>,
    assume_valid: Option<String>,
) -> Result<IbdPolicy, String> {
    if !sync_enabled && (minimum_chainwork.is_some() || assume_valid.is_some()) {
        return Err("IBD policy options require --headers-db or --data-dir".to_owned());
    }
    let mut policy = if custom_signet {
        IbdPolicy::for_custom_signet()
    } else {
        IbdPolicy::for_network(network)
    };
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
        "rbtcd {}\n\nUSAGE:\n  rbtcd [--connect HOST:PORT ...] [--dns-seed HOST[:PORT] ... | --no-dns-seeds] [--network bitcoin|testnet|testnet4|signet|regtest]\n  rbtcd [PEER OPTIONS] --headers-db PATH [--network NETWORK] [--minimum-chainwork HEX] [--assumevalid HASH|0]\n  rbtcd [PEER OPTIONS] --data-dir PATH --network regtest|signet [--once] [--explorer-listen 127.0.0.1:3000 [--wallet-descriptors PATH --wallet-auth-token-file PATH]] [--vbparams taproot:START:END[:MIN_HEIGHT]] [--testactivationheight NAME@HEIGHT] [--signetchallenge HEX] [--signetseednode HOST[:PORT] ...] [--minimum-chainwork HEX] [--assumevalid HASH|0]\n  rbtcd --data-dir PATH --network regtest|signet --assumeutxo-snapshot FILE --snapshot-height HEIGHT --snapshot-blockhash HASH --snapshot-utxo-count COUNT --snapshot-records-bytes BYTES --snapshot-records-sha256 HEX\n  rbtcd [PEER OPTIONS] --fetch-block BLOCK_HASH [--network NETWORK]\n\nPEER OPTIONS:\n  Explicit --connect peers run first. If they and persisted verified peers fail, pinned Bitcoin Core 26 DNS seeds are used. Repeat --dns-seed to replace those defaults, or pass --no-dns-seeds. A custom Signet has no default seeds; repeat --signetseednode or --connect.",
        env!("CARGO_PKG_VERSION")
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use bitcoin::{
        Amount, Block, OutPoint, ScriptBuf, Sequence, Transaction, TxIn, TxMerkleNode, TxOut, Txid,
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
        snapshot::export_snapshot,
        utxo::{OutPointKey, RedbUtxoStore, Utxo, UtxoStore},
    };
    use tempfile::TempDir;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    const RECEIVE_DESCRIPTOR: &str = "wpkh([41f2aed0/84h/1h/0h]tpubDDFSdQWw75hk1ewbwnNpPp5DvXFRKt68ioPoyJDY752cNHKkFxPWqkqCyCf4hxrEfpuxh46QisehL3m8Bi6MsAv394QVLopwbtfvryFQNUH/0/*)#g0w0ymmw";
    const CHANGE_DESCRIPTOR: &str = "wpkh([41f2aed0/84h/1h/0h]tpubDDFSdQWw75hk1ewbwnNpPp5DvXFRKt68ioPoyJDY752cNHKkFxPWqkqCyCf4hxrEfpuxh46QisehL3m8Bi6MsAv394QVLopwbtfvryFQNUH/1/*)#emtwewtk";

    async fn http_request(address: SocketAddr, request: &[u8]) -> String {
        let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
        stream.write_all(request).await.unwrap();
        let mut response = Vec::new();
        stream.read_to_end(&mut response).await.unwrap();
        String::from_utf8(response).unwrap()
    }

    fn write_owner_only(path: &std::path::Path, contents: &str) {
        fs::write(path, contents).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(path, fs::Permissions::from_mode(0o600)).unwrap();
        }
    }

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
        assert!(options.wallet_api_files.is_none());
        assert_eq!(
            options.deployments,
            DeploymentConfig::for_network(Network::Regtest)
        );
        assert_eq!(options.ibd_policy, IbdPolicy::for_network(Network::Regtest));
        assert!(options.snapshot.is_none());
        assert!(!completed_validating_session(&options));

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
                "--wallet-descriptors",
                "/tmp/rbtc-wallet.json",
                "--wallet-auth-token-file",
                "/tmp/rbtc-wallet.token",
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
        let wallet_files = served.wallet_api_files.as_ref().unwrap();
        assert_eq!(
            wallet_files.descriptors,
            PathBuf::from("/tmp/rbtc-wallet.json")
        );
        assert_eq!(
            wallet_files.auth_token,
            PathBuf::from("/tmp/rbtc-wallet.token")
        );
        assert_ne!(
            served.deployments,
            DeploymentConfig::for_network(Network::Regtest)
        );
        assert_eq!(served.deployments.consensus_id().len(), 49);
        assert!(completed_validating_session(&served));
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
                    "regtest",
                    "--data-dir",
                    "/tmp/rbtc-test",
                    "--explorer-listen",
                    "127.0.0.1:3000",
                    "--wallet-descriptors",
                    "/tmp/rbtc-wallet.json",
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
                    "--data-dir",
                    "/tmp/rbtc-test",
                    "--wallet-descriptors",
                    "/tmp/rbtc-wallet.json",
                    "--wallet-auth-token-file",
                    "/tmp/rbtc-wallet.token",
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
    fn parses_only_complete_offline_snapshot_activation_identity() {
        let hash = "0f9188f13cb7b2c9e5f00a9d96e2d3f36b2f78438cbcd220c9f3b9a3e56e3426";
        let digest = "ab".repeat(32);
        let options = parse_options(
            [
                "--network".to_owned(),
                "regtest".to_owned(),
                "--data-dir".to_owned(),
                "/tmp/rbtc-snapshot-parser".to_owned(),
                "--assumeutxo-snapshot".to_owned(),
                "/tmp/base.rbtc".to_owned(),
                "--snapshot-height".to_owned(),
                "100".to_owned(),
                "--snapshot-blockhash".to_owned(),
                hash.to_owned(),
                "--snapshot-utxo-count".to_owned(),
                "1".to_owned(),
                "--snapshot-records-bytes".to_owned(),
                "66".to_owned(),
                "--snapshot-records-sha256".to_owned(),
                digest.clone(),
            ]
            .into_iter(),
        )
        .unwrap()
        .unwrap();
        let snapshot = options.snapshot.unwrap();
        assert_eq!(snapshot.path, PathBuf::from("/tmp/base.rbtc"));
        assert_eq!(snapshot.height, 100);
        assert_eq!(snapshot.block_hash, BlockHash::from_str(hash).unwrap());
        assert_eq!(snapshot.utxo_count, 1);
        assert_eq!(snapshot.records_bytes, 66);
        assert_eq!(snapshot.records_sha256, digest);

        for arguments in [
            vec![
                "--network",
                "regtest",
                "--data-dir",
                "/tmp/rbtc-snapshot-parser",
                "--assumeutxo-snapshot",
                "/tmp/base.rbtc",
            ],
            vec![
                "--network",
                "regtest",
                "--data-dir",
                "/tmp/rbtc-snapshot-parser",
                "--connect",
                "127.0.0.1:18444",
                "--assumeutxo-snapshot",
                "/tmp/base.rbtc",
                "--snapshot-height",
                "100",
                "--snapshot-blockhash",
                hash,
                "--snapshot-utxo-count",
                "1",
                "--snapshot-records-bytes",
                "66",
                "--snapshot-records-sha256",
                "abababababababababababababababababababababababababababababababab",
            ],
        ] {
            assert!(parse_options(arguments.into_iter().map(str::to_owned)).is_err());
        }
    }

    #[tokio::test]
    async fn offline_snapshot_cli_activates_and_reopens_the_assumed_base() {
        let directory = TempDir::new().unwrap();
        let mut headers = HeaderDag::new(Network::Regtest);
        let genesis = headers.active_tip();
        let header = mine_regtest_child(genesis.hash, genesis.header.time + 1);
        let info = headers.insert_contextual(header, header.time).unwrap();
        RedbHeaderStore::open(directory.path().join("headers.redb"))
            .unwrap()
            .append(header)
            .unwrap();

        let source = RedbUtxoStore::open(directory.path().join("source.redb")).unwrap();
        let outpoint = OutPointKey::from(OutPoint::new(Txid::from_byte_array([88; 32]), 0));
        source
            .apply(
                &[],
                &[(
                    outpoint,
                    Utxo {
                        value_sats: 123,
                        height: 1,
                        is_coinbase: false,
                        last_touched: 1,
                        creation_mtp: genesis.header.time,
                        script_pubkey: vec![0x51],
                    },
                )],
            )
            .unwrap();
        let path = directory.path().join("base.rbtc");
        let manifest =
            export_snapshot(&source, &path, "regtest", 1, info.hash.to_string()).unwrap();
        drop(source);

        run(Options {
            remotes: vec![],
            dns_seeds: None,
            network: Network::Regtest,
            fetch_block: None,
            headers_db: None,
            data_dir: Some(directory.path().to_path_buf()),
            once: false,
            explorer_listen: None,
            wallet_api_files: None,
            deployments: DeploymentConfig::for_network(Network::Regtest),
            ibd_policy: IbdPolicy::for_network(Network::Regtest),
            snapshot: Some(SnapshotActivationOptions {
                path,
                height: 1,
                block_hash: info.hash,
                utxo_count: manifest.utxo_count,
                records_bytes: manifest.records_bytes,
                records_sha256: manifest.records_sha256,
            }),
        })
        .await
        .unwrap();

        let reopened =
            RedbChainStore::open(directory.path().join("chainstate.redb"), Network::Regtest)
                .unwrap();
        assert_eq!(reopened.execution().tip().unwrap().height, 1);
        assert_eq!(
            reopened.execution().assumed_snapshot_base().unwrap(),
            Some(reopened.execution().tip().unwrap())
        );
        assert_eq!(reopened.get(outpoint).unwrap().unwrap().value_sats, 123);
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
    fn parses_core_compatible_custom_signet_identity_and_seeds() {
        let options = parse_options(
            [
                "--network",
                "signet",
                "--data-dir",
                "/tmp/rbtc-custom-signet",
                "--signetchallenge",
                "512103ad5e0edad18cb1f0fc0d28a3d4f1f3e445640337489abb10404f2d1e086be43051ae",
                "--signetseednode",
                "seed.example:39000",
            ]
            .into_iter()
            .map(str::to_owned),
        )
        .unwrap()
        .unwrap();
        assert!(options.deployments.is_custom_signet());
        assert_eq!(
            options.deployments.message_start().to_bytes(),
            [0x7e, 0xc6, 0x53, 0xa5]
        );
        assert_eq!(options.ibd_policy, IbdPolicy::for_custom_signet());
        assert_eq!(
            selected_dns_seeds(&options),
            vec![DnsSeed {
                host: "seed.example".to_owned(),
                port: 39_000,
            }]
        );

        let without_seeds = parse_options(
            [
                "--network",
                "signet",
                "--data-dir",
                "/tmp/rbtc-custom-signet",
                "--signetchallenge",
                "51",
            ]
            .into_iter()
            .map(str::to_owned),
        )
        .unwrap()
        .unwrap();
        assert!(selected_dns_seeds(&without_seeds).is_empty());
    }

    #[test]
    fn rejects_ambiguous_or_cross_network_signet_parameters() {
        for arguments in [
            vec!["--network", "bitcoin", "--signetchallenge", "51"],
            vec!["--network", "signet", "--signetchallenge", "0"],
            vec![
                "--network",
                "signet",
                "--signetchallenge",
                "51",
                "--signetchallenge",
                "52",
            ],
            vec![
                "--network",
                "signet",
                "--signetseednode",
                "seed.example",
                "--dns-seed",
                "other.example",
            ],
        ] {
            assert!(parse_options(arguments.into_iter().map(str::to_owned)).is_err());
        }
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
    fn parses_pinned_and_overridden_dns_bootstrap_sources() {
        let defaults = parse_options(["--network", "bitcoin"].into_iter().map(str::to_owned))
            .unwrap()
            .unwrap();
        let seeds = selected_dns_seeds(&defaults);
        assert_eq!(seeds.len(), 9);
        assert!(seeds.iter().all(|seed| seed.port == 8_333));
        assert_eq!(seeds[0].host, "seed.bitcoin.sipa.be.");

        let custom = parse_options(
            [
                "--network",
                "regtest",
                "--dns-seed",
                "LOCALHOST:19000",
                "--dns-seed",
                "localhost:19000",
            ]
            .into_iter()
            .map(str::to_owned),
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            selected_dns_seeds(&custom),
            vec![DnsSeed {
                host: "localhost".to_owned(),
                port: 19_000,
            }]
        );

        let persisted_only = parse_options(
            [
                "--network",
                "regtest",
                "--data-dir",
                "/tmp/rbtc-persisted-peer-bootstrap",
                "--no-dns-seeds",
            ]
            .into_iter()
            .map(str::to_owned),
        )
        .unwrap()
        .unwrap();
        assert!(persisted_only.remotes.is_empty());
        assert!(selected_dns_seeds(&persisted_only).is_empty());

        for arguments in [
            vec!["--network", "testnet4"],
            vec!["--network", "regtest", "--no-dns-seeds"],
            vec![
                "--connect",
                "127.0.0.1:18444",
                "--dns-seed",
                "localhost",
                "--no-dns-seeds",
            ],
            vec!["--dns-seed", "bad_name.invalid"],
            vec!["--dns-seed", "localhost:0"],
        ] {
            assert!(
                parse_options(arguments.into_iter().map(str::to_owned)).is_err(),
                "arguments must be rejected"
            );
        }

        let mut excessive = vec!["--network".to_owned(), "regtest".to_owned()];
        for index in 0..=MAX_DNS_SEEDS {
            excessive.push("--dns-seed".to_owned());
            excessive.push(format!("seed{index}.example"));
        }
        let Err(error) = parse_options(excessive.into_iter()) else {
            panic!("the DNS seed bound must be enforced");
        };
        assert!(error.contains("too many unique --dns-seed"));
    }

    #[test]
    fn filters_dns_results_by_network_deduplication_and_global_bound() {
        let excluded = HashSet::from(["1.1.1.1:8333".parse().unwrap()]);
        let (selected, rejected) = filter_resolved_peer_addresses(
            Network::Bitcoin,
            [
                "1.1.1.1:8333".parse().unwrap(),
                "10.0.0.1:8333".parse().unwrap(),
                "8.8.8.8:0".parse().unwrap(),
                "8.8.8.8:8333".parse().unwrap(),
                "8.8.8.8:8333".parse().unwrap(),
                "9.9.9.9:8333".parse().unwrap(),
            ],
            &excluded,
            1,
        );
        assert_eq!(selected, vec!["8.8.8.8:8333".parse().unwrap()]);
        assert_eq!(rejected, 5);

        let (regtest, rejected) = filter_resolved_peer_addresses(
            Network::Regtest,
            [
                "127.0.0.1:18444".parse().unwrap(),
                "0.0.0.0:18444".parse().unwrap(),
            ],
            &HashSet::new(),
            16,
        );
        assert_eq!(regtest, vec!["127.0.0.1:18444".parse().unwrap()]);
        assert_eq!(rejected, 1);

        let interleaved = round_robin_addresses(&[
            Some(vec![
                "1.1.1.1:8333".parse().unwrap(),
                "1.1.1.2:8333".parse().unwrap(),
            ]),
            None,
            Some(vec![
                "2.2.2.1:8333".parse().unwrap(),
                "2.2.2.2:8333".parse().unwrap(),
            ]),
        ]);
        assert_eq!(
            interleaved,
            [
                "1.1.1.1:8333",
                "2.2.2.1:8333",
                "1.1.1.2:8333",
                "2.2.2.2:8333",
            ]
            .map(|address| address.parse().unwrap())
        );

        let oversized = Some(
            (1_u16..=65)
                .map(|suffix| format!("1.1.1.{suffix}:8333").parse().unwrap())
                .collect(),
        );
        assert_eq!(round_robin_addresses(&[oversized]).len(), 64);

        let public = (1_u16..=32)
            .map(|suffix| format!("8.8.0.{suffix}:8333").parse().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            filter_resolved_peer_addresses(Network::Bitcoin, public, &HashSet::new(), usize::MAX,)
                .0
                .len(),
            MAX_CONFIGURED_PEERS
        );
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
            assert!(error.contains("only regtest and Signet"));
        }
    }

    #[test]
    fn custom_signet_identity_is_rejected_before_network_or_wallet_startup() {
        let directory = TempDir::new().unwrap();
        let default = DeploymentConfig::for_network(Network::Signet);
        preflight_data_dir(directory.path(), Network::Signet, &default).unwrap();
        assert!(!directory.path().join("peers.redb").exists());
        assert!(!directory.path().join("wallet.sqlite").exists());

        let mut custom = DeploymentConfig::for_network(Network::Signet);
        custom.set_signet_challenge(vec![0x51]).unwrap();
        let error = preflight_data_dir(directory.path(), Network::Signet, &custom).unwrap_err();
        assert!(error.contains("consensus configuration"));
        assert!(!directory.path().join("peers.redb").exists());
        assert!(!directory.path().join("wallet.sqlite").exists());
    }

    #[tokio::test]
    async fn legacy_chainstate_is_rejected_before_peer_store_creation_or_connect() {
        let directory = TempDir::new().unwrap();
        fs::write(directory.path().join("undo.redb"), b"legacy").unwrap();
        let descriptors = directory.path().join("wallet.json");
        let auth_token = directory.path().join("wallet.token");
        write_owner_only(
            &descriptors,
            &serde_json::json!({
                "receive_descriptor": RECEIVE_DESCRIPTOR,
                "change_descriptor": CHANGE_DESCRIPTOR
            })
            .to_string(),
        );
        write_owner_only(&auth_token, &"a".repeat(32));

        let error = run(Options {
            remotes: vec!["127.0.0.1:1".parse().unwrap()],
            dns_seeds: None,
            network: Network::Regtest,
            fetch_block: None,
            headers_db: None,
            data_dir: Some(directory.path().to_path_buf()),
            once: true,
            explorer_listen: Some("127.0.0.1:0".parse().unwrap()),
            wallet_api_files: Some(WalletApiFiles {
                descriptors,
                auth_token,
            }),
            deployments: DeploymentConfig::for_network(Network::Regtest),
            ibd_policy: IbdPolicy::for_network(Network::Regtest),
            snapshot: None,
        })
        .await
        .unwrap_err();

        assert!(error.contains("legacy split chain-state file"));
        assert!(!directory.path().join("peers.redb").exists());
        assert!(!directory.path().join("wallet.sqlite").exists());
    }

    #[test]
    fn wallet_runtime_loads_only_owner_only_bounded_configuration_files() {
        let directory = TempDir::new().unwrap();
        let descriptors = directory.path().join("wallet.json");
        let auth_token = directory.path().join("wallet.token");
        write_owner_only(
            &descriptors,
            &serde_json::json!({
                "receive_descriptor": RECEIVE_DESCRIPTOR,
                "change_descriptor": CHANGE_DESCRIPTOR,
                "gap_limit": 42,
                "birthday_height": 100
            })
            .to_string(),
        );
        write_owner_only(&auth_token, &format!("{}\n", "a".repeat(32)));
        let options = Options {
            remotes: vec!["127.0.0.1:1".parse().unwrap()],
            dns_seeds: None,
            network: Network::Regtest,
            fetch_block: None,
            headers_db: None,
            data_dir: Some(directory.path().to_path_buf()),
            once: true,
            explorer_listen: Some("127.0.0.1:0".parse().unwrap()),
            wallet_api_files: Some(WalletApiFiles {
                descriptors: descriptors.clone(),
                auth_token: auth_token.clone(),
            }),
            deployments: DeploymentConfig::for_network(Network::Regtest),
            ibd_policy: IbdPolicy::for_network(Network::Regtest),
            snapshot: None,
        };

        let runtime = prepare_api_runtime(&options).unwrap().unwrap();
        let wallet = runtime.wallet.as_ref().unwrap();
        assert_eq!(wallet.scan.gap_limit, 42);
        assert_eq!(wallet.scan.birthday_height, 100);
        assert!(directory.path().join("wallet.sqlite").exists());
        drop(runtime);

        write_owner_only(
            &descriptors,
            &serde_json::json!({
                "receive_descriptor": RECEIVE_DESCRIPTOR,
                "change_descriptor": CHANGE_DESCRIPTOR,
                "gap_limit": 0
            })
            .to_string(),
        );
        let error = prepare_api_runtime(&options).err().unwrap();
        assert!(error.contains("gap_limit"));
        write_owner_only(
            &descriptors,
            &serde_json::json!({
                "receive_descriptor": RECEIVE_DESCRIPTOR,
                "change_descriptor": CHANGE_DESCRIPTOR
            })
            .to_string(),
        );

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&auth_token, fs::Permissions::from_mode(0o644)).unwrap();
            let error = prepare_api_runtime(&options).err().unwrap();
            assert!(error.contains("permissions"));
            assert!(!error.contains(&"a".repeat(32)));
        }
    }

    #[tokio::test]
    async fn embedded_api_serves_explorer_and_authenticated_watch_only_wallet() {
        let directory = TempDir::new().unwrap();
        let index = Arc::new(
            RedbExplorerIndex::open(directory.path().join("explorer.redb"), Network::Regtest)
                .unwrap(),
        );
        let token_text = "a".repeat(32);
        let wallet = WalletApiRuntime {
            wallet: Arc::new(
                EmbeddedWallet::open_or_create(
                    directory.path().join("wallet.sqlite"),
                    RECEIVE_DESCRIPTOR,
                    CHANGE_DESCRIPTOR,
                    Network::Regtest,
                )
                .unwrap(),
            ),
            token: WalletAuthToken::new(&token_text).unwrap(),
            scan: WalletScanConfig {
                gap_limit: DEFAULT_WALLET_GAP_LIMIT,
                birthday_height: 0,
            },
        };
        let server = ApiServer::bind("127.0.0.1:0".parse().unwrap(), index, Some(&wallet))
            .await
            .unwrap();
        let response = http_request(
            server.address(),
            b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
        )
        .await;
        assert!(response.starts_with("HTTP/1.1 200 OK"));
        assert!(response.contains("content-security-policy:"));
        assert!(response.contains("<title>rBTC Explorer</title>"));
        assert!(response.contains("Watch-only wallet"));

        let unauthorized = http_request(
            server.address(),
            b"GET /api/v1/wallet/balance HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
        )
        .await;
        assert!(unauthorized.starts_with("HTTP/1.1 401 Unauthorized"));
        let request = format!(
            "POST /api/v1/wallet/address HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer {token_text}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
        );
        let authorized = http_request(server.address(), request.as_bytes()).await;
        assert!(authorized.starts_with("HTTP/1.1 200 OK"));
        let body = authorized.split("\r\n\r\n").nth(1).unwrap();
        let address: rbtc::wallet::WalletAddress = serde_json::from_str(body).unwrap();
        assert_eq!(address.index, 0);
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
            dns_seeds: None,
            network: Network::Regtest,
            fetch_block: None,
            headers_db: Some(header_path.clone()),
            data_dir: None,
            once: true,
            explorer_listen: None,
            wallet_api_files: None,
            deployments: DeploymentConfig::for_network(Network::Regtest),
            ibd_policy: IbdPolicy::for_network(Network::Regtest),
            snapshot: None,
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
            dns_seeds: None,
            network: Network::Regtest,
            fetch_block: None,
            headers_db: None,
            data_dir: Some(directory.path().to_path_buf()),
            once: true,
            explorer_listen: None,
            wallet_api_files: None,
            deployments: DeploymentConfig::for_network(Network::Regtest),
            ibd_policy: IbdPolicy::for_network(Network::Regtest),
            snapshot: None,
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
        assert!(
            !reopened
                .is_discouraged(failed_remote, unix_time().unwrap())
                .unwrap()
        );
    }

    #[tokio::test]
    async fn daemon_falls_back_to_dns_and_reuses_the_verified_peer_without_dns() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let dns_remote = listener.local_addr().unwrap();
        let failed_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let failed_explicit = failed_listener.local_addr().unwrap();
        drop(failed_listener);

        let server = tokio::spawn(async move {
            for nonce in [31, 32] {
                let (stream, _) = listener.accept().await.unwrap();
                let mut peer = V1Transport::new(stream, Network::Regtest.magic());
                assert!(matches!(
                    peer.read_message().await.unwrap().into_payload(),
                    NetworkMessage::Version(_)
                ));
                peer.write_message(NetworkMessage::Version(peer_version(nonce)))
                    .await
                    .unwrap();
                receive_client_negotiation(&mut peer).await;
                peer.write_message(NetworkMessage::Verack).await.unwrap();
                respond_to_getaddr(&mut peer).await;
                match peer.read_message().await.unwrap().into_payload() {
                    NetworkMessage::GetHeaders(_) => peer
                        .write_message(NetworkMessage::Headers(Vec::new()))
                        .await
                        .unwrap(),
                    message => panic!("expected DNS-bootstrap getheaders, got {message:?}"),
                }
            }
        });

        let directory = TempDir::new().unwrap();
        run(Options {
            remotes: vec![failed_explicit],
            dns_seeds: Some(vec![DnsSeed {
                host: dns_remote.ip().to_string(),
                port: dns_remote.port(),
            }]),
            network: Network::Regtest,
            fetch_block: None,
            headers_db: None,
            data_dir: Some(directory.path().to_path_buf()),
            once: true,
            explorer_listen: None,
            wallet_api_files: None,
            deployments: DeploymentConfig::for_network(Network::Regtest),
            ibd_policy: IbdPolicy::for_network(Network::Regtest),
            snapshot: None,
        })
        .await
        .unwrap();

        let peers =
            RedbPeerStore::open(directory.path().join("peers.redb"), Network::Regtest).unwrap();
        assert_eq!(
            peers.candidates(unix_time().unwrap(), 16).unwrap(),
            vec![dns_remote]
        );
        drop(peers);

        run(Options {
            remotes: Vec::new(),
            dns_seeds: Some(Vec::new()),
            network: Network::Regtest,
            fetch_block: None,
            headers_db: None,
            data_dir: Some(directory.path().to_path_buf()),
            once: true,
            explorer_listen: None,
            wallet_api_files: None,
            deployments: DeploymentConfig::for_network(Network::Regtest),
            ibd_policy: IbdPolicy::for_network(Network::Regtest),
            snapshot: None,
        })
        .await
        .unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn daemon_persists_protocol_discouragement_but_exempts_manual_peers() {
        let learned_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let learned_remote = learned_listener.local_addr().unwrap();
        let learned_server = tokio::spawn(async move {
            let (stream, _) = learned_listener.accept().await.unwrap();
            let mut peer = V1Transport::new(stream, Network::Regtest.magic());
            assert!(matches!(
                peer.read_message().await.unwrap().into_payload(),
                NetworkMessage::Version(_)
            ));
            peer.write_message(NetworkMessage::Version(peer_version(41)))
                .await
                .unwrap();
            peer.write_message(NetworkMessage::Version(peer_version(42)))
                .await
                .unwrap();
        });

        let directory = TempDir::new().unwrap();
        let now = unix_time().unwrap();
        let peers =
            RedbPeerStore::open(directory.path().join("peers.redb"), Network::Regtest).unwrap();
        peers
            .insert_discovered(
                "127.0.0.2:18444".parse().unwrap(),
                &[PeerAddress {
                    socket: learned_remote,
                    services: ServiceFlags::NETWORK | ServiceFlags::WITNESS,
                    last_seen: now,
                }],
                now,
            )
            .unwrap();
        drop(peers);

        let error = run(Options {
            remotes: Vec::new(),
            dns_seeds: Some(Vec::new()),
            network: Network::Regtest,
            fetch_block: None,
            headers_db: None,
            data_dir: Some(directory.path().to_path_buf()),
            once: true,
            explorer_listen: None,
            wallet_api_files: None,
            deployments: DeploymentConfig::for_network(Network::Regtest),
            ibd_policy: IbdPolicy::for_network(Network::Regtest),
            snapshot: None,
        })
        .await
        .unwrap_err();
        assert!(error.contains("duplicate version"));
        learned_server.await.unwrap();

        let peers =
            RedbPeerStore::open(directory.path().join("peers.redb"), Network::Regtest).unwrap();
        assert!(
            peers
                .is_discouraged(learned_remote, unix_time().unwrap())
                .unwrap()
        );
        assert!(
            peers
                .candidates(unix_time().unwrap(), 16)
                .unwrap()
                .is_empty()
        );
        drop(peers);

        let manual_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let manual_remote = manual_listener.local_addr().unwrap();
        let manual_server = tokio::spawn(async move {
            let (stream, _) = manual_listener.accept().await.unwrap();
            let mut peer = V1Transport::new(stream, Network::Regtest.magic());
            assert!(matches!(
                peer.read_message().await.unwrap().into_payload(),
                NetworkMessage::Version(_)
            ));
            peer.write_message(NetworkMessage::Version(peer_version(43)))
                .await
                .unwrap();
            peer.write_message(NetworkMessage::Version(peer_version(44)))
                .await
                .unwrap();
        });
        let error = run(Options {
            remotes: vec![manual_remote],
            dns_seeds: Some(Vec::new()),
            network: Network::Regtest,
            fetch_block: None,
            headers_db: None,
            data_dir: Some(directory.path().to_path_buf()),
            once: true,
            explorer_listen: None,
            wallet_api_files: None,
            deployments: DeploymentConfig::for_network(Network::Regtest),
            ibd_policy: IbdPolicy::for_network(Network::Regtest),
            snapshot: None,
        })
        .await
        .unwrap_err();
        assert!(error.contains("duplicate version"));
        manual_server.await.unwrap();

        let peers =
            RedbPeerStore::open(directory.path().join("peers.redb"), Network::Regtest).unwrap();
        assert!(
            !peers
                .is_discouraged(manual_remote, unix_time().unwrap())
                .unwrap()
        );
    }

    #[tokio::test]
    async fn daemon_discourages_a_learned_peer_serving_an_invalid_block() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let remote = listener.local_addr().unwrap();
        let genesis = bitcoin::blockdata::constants::genesis_block(Network::Regtest).header;
        let mut invalid_block = regtest_block(genesis.block_hash(), genesis.time + 1);
        let expected_hash = invalid_block.block_hash();
        let valid_header = invalid_block.header;
        invalid_block.txdata[0].output[0].value = Amount::from_sat(1);
        assert_ne!(
            invalid_block.compute_merkle_root().unwrap(),
            valid_header.merkle_root
        );
        let server = tokio::spawn(async move {
            let (mut peer, _) = accept_peer(listener, peer_version(51)).await;
            respond_to_getaddr(&mut peer).await;
            match peer.read_message().await.unwrap().into_payload() {
                NetworkMessage::GetHeaders(_) => peer
                    .write_message(NetworkMessage::Headers(vec![valid_header]))
                    .await
                    .unwrap(),
                message => panic!("expected invalid-block getheaders, got {message:?}"),
            }
            match peer.read_message().await.unwrap().into_payload() {
                NetworkMessage::GetData(inventory) => {
                    assert_eq!(
                        inventory,
                        vec![bitcoin::p2p::message_blockdata::Inventory::WitnessBlock(
                            expected_hash
                        )]
                    );
                    peer.write_message(NetworkMessage::Block(invalid_block))
                        .await
                        .unwrap();
                }
                message => panic!("expected invalid-block getdata, got {message:?}"),
            }
        });

        let directory = TempDir::new().unwrap();
        let now = unix_time().unwrap();
        let peers =
            RedbPeerStore::open(directory.path().join("peers.redb"), Network::Regtest).unwrap();
        peers
            .insert_discovered(
                "127.0.0.2:18444".parse().unwrap(),
                &[PeerAddress {
                    socket: remote,
                    services: ServiceFlags::NETWORK | ServiceFlags::WITNESS,
                    last_seen: now,
                }],
                now,
            )
            .unwrap();
        drop(peers);

        let error = run(Options {
            remotes: Vec::new(),
            dns_seeds: Some(Vec::new()),
            network: Network::Regtest,
            fetch_block: None,
            headers_db: None,
            data_dir: Some(directory.path().to_path_buf()),
            once: true,
            explorer_listen: None,
            wallet_api_files: None,
            deployments: DeploymentConfig::for_network(Network::Regtest),
            ibd_policy: IbdPolicy::for_network(Network::Regtest),
            snapshot: None,
        })
        .await
        .unwrap_err();
        assert!(error.contains("merkle root mismatch"));
        server.await.unwrap();

        let peers =
            RedbPeerStore::open(directory.path().join("peers.redb"), Network::Regtest).unwrap();
        assert!(peers.is_discouraged(remote, unix_time().unwrap()).unwrap());
        assert!(
            peers
                .candidates(unix_time().unwrap(), 16)
                .unwrap()
                .is_empty()
        );
        let chainstate =
            RedbChainStore::open(directory.path().join("chainstate.redb"), Network::Regtest)
                .unwrap();
        assert_eq!(chainstate.execution().tip().unwrap().height, 0);
        assert!(chainstate.undos().get(expected_hash).unwrap().is_none());
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn daemon_downloads_executes_and_recovers_a_regtest_block() {
        let directory = TempDir::new().unwrap();
        let descriptors = directory.path().join("wallet.json");
        let auth_token = directory.path().join("wallet.token");
        write_owner_only(
            &descriptors,
            &serde_json::json!({
                "receive_descriptor": RECEIVE_DESCRIPTOR,
                "change_descriptor": CHANGE_DESCRIPTOR
            })
            .to_string(),
        );
        write_owner_only(&auth_token, &"a".repeat(32));
        let receive_address = {
            let wallet = EmbeddedWallet::open_or_create(
                directory.path().join("wallet.sqlite"),
                RECEIVE_DESCRIPTOR,
                CHANGE_DESCRIPTOR,
                Network::Regtest,
            )
            .unwrap();
            wallet.reveal_receive_address().unwrap().address
        };
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let remote = listener.local_addr().unwrap();
        let genesis = bitcoin::blockdata::constants::genesis_block(Network::Regtest).header;
        let mut block = regtest_block(genesis.block_hash(), genesis.time + 1);
        block.txdata[0].output[0].script_pubkey = bitcoin::Address::from_str(&receive_address)
            .unwrap()
            .require_network(Network::Regtest)
            .unwrap()
            .script_pubkey();
        block.header.merkle_root = block.compute_merkle_root().unwrap();
        while block
            .header
            .validate_pow(Target::MAX_ATTAINABLE_REGTEST)
            .is_err()
        {
            block.header.nonce = block.header.nonce.checked_add(1).unwrap();
        }
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

        run(Options {
            remotes: vec![remote],
            dns_seeds: None,
            network: Network::Regtest,
            fetch_block: None,
            headers_db: None,
            data_dir: Some(directory.path().to_path_buf()),
            once: true,
            explorer_listen: Some("127.0.0.1:0".parse().unwrap()),
            wallet_api_files: Some(WalletApiFiles {
                descriptors: descriptors.clone(),
                auth_token: auth_token.clone(),
            }),
            deployments: DeploymentConfig::for_network(Network::Regtest),
            ibd_policy: IbdPolicy::for_network(Network::Regtest),
            snapshot: None,
        })
        .await
        .unwrap();
        // Let the aborted embedded API task release its Arc-backed databases.
        tokio::task::yield_now().await;
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
        let wallet = EmbeddedWallet::open_or_create(
            directory.path().join("wallet.sqlite"),
            RECEIVE_DESCRIPTOR,
            CHANGE_DESCRIPTOR,
            Network::Regtest,
        )
        .unwrap();
        assert_eq!(wallet.chain_tip().unwrap().height, 1);
        assert_eq!(wallet.chain_tip().unwrap().hash, block_hash);
        assert_eq!(wallet.balance().unwrap().immature, block_subsidy(1));
        drop(wallet);
        let peers =
            RedbPeerStore::open(directory.path().join("peers.redb"), Network::Regtest).unwrap();
        assert_eq!(peers.len().unwrap(), 2);
        let candidates = peers.candidates(unix_time().unwrap(), 16).unwrap();
        assert_eq!(candidates[0], remote);
        assert!(candidates.contains(&"127.0.0.2:18445".parse().unwrap()));
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
            dns_seeds: None,
            network: Network::Regtest,
            fetch_block: None,
            headers_db: None,
            data_dir: Some(directory.path().to_path_buf()),
            once: true,
            explorer_listen: None,
            wallet_api_files: None,
            deployments: DeploymentConfig::for_network(Network::Regtest),
            ibd_policy: IbdPolicy::for_network(Network::Regtest),
            snapshot: None,
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
            dns_seeds: None,
            network: Network::Regtest,
            fetch_block: None,
            headers_db: None,
            data_dir: Some(directory.path().to_path_buf()),
            once: true,
            explorer_listen: None,
            wallet_api_files: None,
            deployments: DeploymentConfig::for_network(Network::Regtest),
            ibd_policy: IbdPolicy::for_network(Network::Regtest),
            snapshot: None,
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
                dns_seeds: None,
                network: Network::Regtest,
                fetch_block: None,
                headers_db: None,
                data_dir: Some(directory.path().to_path_buf()),
                once: true,
                explorer_listen: None,
                wallet_api_files: None,
                deployments: DeploymentConfig::for_network(Network::Regtest),
                ibd_policy: IbdPolicy::for_network(Network::Regtest),
                snapshot: None,
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
            dns_seeds: None,
            network: Network::Signet,
            fetch_block: None,
            headers_db: None,
            data_dir: Some(directory.path().to_path_buf()),
            once: true,
            explorer_listen: None,
            wallet_api_files: None,
            deployments: DeploymentConfig::for_network(Network::Signet),
            ibd_policy,
            snapshot: None,
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
