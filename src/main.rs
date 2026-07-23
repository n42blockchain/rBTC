//! Command line entry point for the rBTC node daemon.

use std::{
    collections::HashSet,
    env, fs,
    io::{self, Read, Write},
    net::{IpAddr, SocketAddr},
    path::PathBuf,
    process,
    str::FromStr,
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use bitcoin::{
    Block, BlockHash, Network,
    consensus::{deserialize, serialize},
    hashes::Hash,
    hex::FromHex,
};
use rbtc::{
    api::{
        AuthorizationAuditLog, ExplorerEventHub, ExplorerEventKind, LocalAuthToken,
        explorer_events_router, explorer_router, rpc_router, wallet_router,
    },
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
    utxo::{DEFAULT_HOT_WINDOW_SECS, UtxoStore},
    validation_owner::{
        MAX_VALIDATION_OWNER_BYTES, ValidationDirectoryOwner, parse_validation_directory_owner,
    },
    wallet::{
        EmbeddedWallet, MAX_WALLET_DESCRIPTOR_CONFIG_BYTES, WalletTip,
        parse_wallet_descriptor_config,
    },
};
use tokio::time::timeout;

const PEER_TIMEOUT: Duration = Duration::from_secs(30);
const PEER_DISCOVERY_TIMEOUT: Duration = Duration::from_secs(3);
const DNS_SEED_TIMEOUT: Duration = Duration::from_secs(5);
const USER_AGENT: &str = "/rbtcd:0.1.0/";
const MAX_CONFIGURED_PEERS: usize = 16;
const MAX_DNS_SEEDS: usize = 16;
const MAX_DNS_ADDRESSES_PER_SEED: usize = 64;
const MAX_LOCAL_AUTH_TOKEN_FILE_LEN: u64 = 1024;
#[cfg(not(test))]
const LOCAL_AUTH_TOKEN_RELOAD_INTERVAL: Duration = Duration::from_secs(1);
#[cfg(test)]
const LOCAL_AUTH_TOKEN_RELOAD_INTERVAL: Duration = Duration::from_millis(20);
const MAX_WALLET_SCAN_PASSES: usize = 64;
const API_AUDIT_FILE: &str = "api-auth-audit.jsonl";
const MAX_VALIDATION_PAUSE_MS: u64 = 60_000;
const ADAPTIVE_VALIDATION_BUSY_PAUSE: Duration = Duration::from_millis(100);
const VALIDATION_OWNER_FILE: &str = ".rbtc-validation-owner.json";

const fn supports_block_execution(network: Network) -> bool {
    matches!(network, Network::Regtest | Network::Signet)
}

#[derive(Clone)]
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
    rpc_auth_token_file: Option<PathBuf>,
    deployments: DeploymentConfig,
    ibd_policy: IbdPolicy,
    snapshot: Option<SnapshotActivationOptions>,
    finalize_assumeutxo: Option<PathBuf>,
    validation_target: Option<ValidationTarget>,
    complete_assumeutxo: Option<PathBuf>,
    background_assumeutxo: Option<PathBuf>,
    cleanup_validation_dir: bool,
    validation_limits: ValidationLimits,
}

#[derive(Clone)]
struct SnapshotActivationOptions {
    path: PathBuf,
    height: u32,
    block_hash: BlockHash,
    utxo_count: u64,
    records_bytes: u64,
    records_sha256: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ValidationTarget {
    height: u32,
    block_hash: BlockHash,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ValidationLimits {
    max_blocks_per_batch: usize,
    pause_between_batches: Duration,
}

impl Default for ValidationLimits {
    fn default() -> Self {
        Self {
            max_blocks_per_batch: MAX_BLOCKS_IN_FLIGHT,
            pause_between_batches: Duration::ZERO,
        }
    }
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

#[derive(Clone)]
struct WalletApiFiles {
    descriptors: PathBuf,
    auth_token: PathBuf,
}

struct WalletApiRuntime {
    wallet: Arc<EmbeddedWallet>,
    token: LocalAuthToken,
    token_path: PathBuf,
    audit: AuthorizationAuditLog,
    scan: WalletScanConfig,
}

struct RpcApiRuntime {
    token: LocalAuthToken,
    token_path: PathBuf,
    audit: AuthorizationAuditLog,
}

#[derive(Clone, Copy)]
struct WalletScanConfig {
    gap_limit: u32,
    birthday_height: u32,
}

struct ApiRuntime {
    listen: SocketAddr,
    wallet: Option<WalletApiRuntime>,
    rpc: Option<RpcApiRuntime>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum BackgroundValidationState {
    Pending,
    Complete,
    Finalized,
    Failed(String),
}

#[derive(Clone)]
struct BackgroundValidationStatus {
    validation_dir: PathBuf,
    cleanup_after_finalize: bool,
    progress: Arc<Mutex<BackgroundValidationProgress>>,
}

#[derive(Clone, Debug)]
struct BackgroundValidationProgress {
    state: BackgroundValidationState,
    target: ValidationTarget,
    active_tip: Option<u32>,
    active_header_tip: Option<u32>,
    validation_tip: u32,
    adaptive_throttled: bool,
}

#[derive(Clone, Debug, serde::Serialize)]
struct ValidationStatusResponse {
    phase: &'static str,
    target_height: u32,
    target_block_hash: String,
    active_tip: Option<u32>,
    active_header_tip: Option<u32>,
    validation_tip: u32,
    validation_remaining: u32,
    adaptive_throttled: bool,
    error: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize)]
struct NodeTipResponse {
    height: u32,
    hash: String,
}

#[derive(Clone, Debug)]
struct NodeStatusProgress {
    network: String,
    header: NodeTipResponse,
    execution: NodeTipResponse,
    explorer: NodeTipResponse,
    wallet: Option<NodeTipResponse>,
    minimum_chainwork_reached: bool,
    active_assume_valid_height: Option<u32>,
    full_script_validation: bool,
    independently_validated: bool,
    utxo_hot: u64,
    utxo_cold: u64,
    ledger_segments: u32,
    ledger_blocks: u32,
    ledger_bytes: u64,
    ledger_first_height: Option<u32>,
    ledger_tip_height: Option<u32>,
}

#[derive(Clone)]
struct NodeStatus {
    started: Instant,
    progress: Arc<Mutex<NodeStatusProgress>>,
}

#[derive(Clone, Debug, serde::Serialize)]
struct NodeTrustResponse {
    minimum_chainwork_reached: bool,
    active_assume_valid_height: Option<u32>,
    full_script_validation: bool,
    independently_validated: bool,
}

#[derive(Clone, Debug, serde::Serialize)]
struct NodeStatusResponse {
    network: String,
    phase: &'static str,
    ready: bool,
    session_uptime_seconds: u64,
    header: NodeTipResponse,
    execution: NodeTipResponse,
    explorer: NodeTipResponse,
    wallet: Option<NodeTipResponse>,
    trust: NodeTrustResponse,
    utxo_hot: u64,
    utxo_cold: u64,
    ledger_segments: u32,
    ledger_blocks: u32,
    ledger_bytes: u64,
    ledger_first_height: Option<u32>,
    ledger_tip_height: Option<u32>,
}

impl BackgroundValidationStatus {
    fn new(
        validation_dir: PathBuf,
        target: ValidationTarget,
        cleanup_after_finalize: bool,
    ) -> Self {
        Self {
            validation_dir,
            cleanup_after_finalize,
            progress: Arc::new(Mutex::new(BackgroundValidationProgress {
                state: BackgroundValidationState::Pending,
                target,
                active_tip: None,
                active_header_tip: None,
                validation_tip: 0,
                adaptive_throttled: true,
            })),
        }
    }

    fn state(&self) -> BackgroundValidationState {
        self.progress
            .lock()
            .expect("background validation state lock not poisoned")
            .state
            .clone()
    }

    fn set(&self, state: BackgroundValidationState) {
        self.progress
            .lock()
            .expect("background validation state lock not poisoned")
            .state = state;
    }

    fn update_active(&self, tip: u32, header_tip: u32) {
        let mut progress = self
            .progress
            .lock()
            .expect("background validation state lock not poisoned");
        progress.active_tip = Some(tip);
        progress.active_header_tip = Some(header_tip);
    }

    fn update_validation(&self, tip: u32) {
        self.progress
            .lock()
            .expect("background validation state lock not poisoned")
            .validation_tip = tip;
    }

    fn target(&self) -> ValidationTarget {
        self.progress
            .lock()
            .expect("background validation state lock not poisoned")
            .target
    }

    fn adaptive_limits(&self, configured: ValidationLimits) -> ValidationLimits {
        let mut progress = self
            .progress
            .lock()
            .expect("background validation state lock not poisoned");
        let active_busy = progress
            .active_tip
            .zip(progress.active_header_tip)
            .is_none_or(|(tip, header_tip)| tip < header_tip);
        progress.adaptive_throttled = active_busy;
        if active_busy {
            ValidationLimits {
                max_blocks_per_batch: 1,
                pause_between_batches: configured
                    .pause_between_batches
                    .max(ADAPTIVE_VALIDATION_BUSY_PAUSE),
            }
        } else {
            configured
        }
    }

    fn response(&self) -> ValidationStatusResponse {
        let progress = self
            .progress
            .lock()
            .expect("background validation state lock not poisoned");
        let (phase, error) = match &progress.state {
            BackgroundValidationState::Pending => ("validating", None),
            BackgroundValidationState::Complete => ("finalizing", None),
            BackgroundValidationState::Finalized => ("finalized", None),
            BackgroundValidationState::Failed(error) => ("failed", Some(error.clone())),
        };
        ValidationStatusResponse {
            phase,
            target_height: progress.target.height,
            target_block_hash: progress.target.block_hash.to_string(),
            active_tip: progress.active_tip,
            active_header_tip: progress.active_header_tip,
            validation_tip: progress.validation_tip,
            validation_remaining: progress
                .target
                .height
                .saturating_sub(progress.validation_tip),
            adaptive_throttled: progress.adaptive_throttled,
            error,
        }
    }
}

impl NodeStatus {
    fn new(progress: NodeStatusProgress) -> Self {
        Self {
            started: Instant::now(),
            progress: Arc::new(Mutex::new(progress)),
        }
    }

    fn update(&self, progress: NodeStatusProgress) {
        *self.progress.lock().expect("node status lock not poisoned") = progress;
    }

    fn response(&self) -> NodeStatusResponse {
        let progress = self
            .progress
            .lock()
            .expect("node status lock not poisoned")
            .clone();
        let chain_caught_up = progress.header == progress.execution;
        let explorer_caught_up = progress.explorer == progress.execution;
        let wallet_caught_up = progress
            .wallet
            .as_ref()
            .is_none_or(|wallet| *wallet == progress.execution);
        let ready = progress.minimum_chainwork_reached
            && chain_caught_up
            && explorer_caught_up
            && wallet_caught_up;
        let phase = if !progress.minimum_chainwork_reached {
            "ibd"
        } else if !chain_caught_up {
            "syncing_blocks"
        } else if !explorer_caught_up || !wallet_caught_up {
            "reconciling"
        } else if progress.independently_validated {
            "ready"
        } else {
            "assumed_ready"
        };
        NodeStatusResponse {
            network: progress.network,
            phase,
            ready,
            session_uptime_seconds: self.started.elapsed().as_secs(),
            header: progress.header,
            execution: progress.execution,
            explorer: progress.explorer,
            wallet: progress.wallet,
            trust: NodeTrustResponse {
                minimum_chainwork_reached: progress.minimum_chainwork_reached,
                active_assume_valid_height: progress.active_assume_valid_height,
                full_script_validation: progress.full_script_validation,
                independently_validated: progress.independently_validated,
            },
            utxo_hot: progress.utxo_hot,
            utxo_cold: progress.utxo_cold,
            ledger_segments: progress.ledger_segments,
            ledger_blocks: progress.ledger_blocks,
            ledger_bytes: progress.ledger_bytes,
            ledger_first_height: progress.ledger_first_height,
            ledger_tip_height: progress.ledger_tip_height,
        }
    }
}

fn node_status_router(status: NodeStatus) -> axum::Router {
    axum::Router::new()
        .route("/api/v1/status", axum::routing::get(node_status))
        .route("/api/v1/ready", axum::routing::get(node_ready))
        .route("/metrics", axum::routing::get(node_metrics))
        .with_state(status)
}

async fn node_status(
    axum::extract::State(status): axum::extract::State<NodeStatus>,
) -> (
    [(axum::http::HeaderName, &'static str); 1],
    axum::Json<NodeStatusResponse>,
) {
    (
        [(axum::http::header::CACHE_CONTROL, "no-store")],
        axum::Json(status.response()),
    )
}

async fn node_ready(
    axum::extract::State(status): axum::extract::State<NodeStatus>,
) -> (
    axum::http::StatusCode,
    [(axum::http::HeaderName, &'static str); 1],
    axum::Json<NodeStatusResponse>,
) {
    let response = status.response();
    let code = if response.ready {
        axum::http::StatusCode::OK
    } else {
        axum::http::StatusCode::SERVICE_UNAVAILABLE
    };
    (
        code,
        [(axum::http::header::CACHE_CONTROL, "no-store")],
        axum::Json(response),
    )
}

async fn node_metrics(
    axum::extract::State(status): axum::extract::State<NodeStatus>,
) -> ([(axum::http::HeaderName, &'static str); 2], String) {
    let status = status.response();
    let ready = u8::from(status.ready);
    let minimum_chainwork = u8::from(status.trust.minimum_chainwork_reached);
    let independently_validated = u8::from(status.trust.independently_validated);
    let wallet_enabled = u8::from(status.wallet.is_some());
    let wallet_height = status.wallet.as_ref().map_or(0, |tip| tip.height);
    let ledger_first_height = status.ledger_first_height.unwrap_or(0);
    let ledger_tip_height = status.ledger_tip_height.unwrap_or(0);
    let execution_lag = status.header.height.saturating_sub(status.execution.height);
    let explorer_lag = status
        .execution
        .height
        .saturating_sub(status.explorer.height);
    let utxo_total = status.utxo_hot.saturating_add(status.utxo_cold);
    let body = format!(
        "# HELP rbtc_node_info Static network and current synchronization phase.\n# TYPE rbtc_node_info gauge\nrbtc_node_info{{network=\"{}\",phase=\"{}\"}} 1\n# HELP rbtc_ready Whether every serving projection is caught up and minimum chainwork is reached.\n# TYPE rbtc_ready gauge\nrbtc_ready {ready}\n# HELP rbtc_tip_height Durable heights by subsystem.\n# TYPE rbtc_tip_height gauge\nrbtc_tip_height{{kind=\"header\"}} {}\nrbtc_tip_height{{kind=\"execution\"}} {}\nrbtc_tip_height{{kind=\"explorer\"}} {}\nrbtc_tip_height{{kind=\"wallet\"}} {wallet_height}\nrbtc_tip_height{{kind=\"ledger\"}} {ledger_tip_height}\n# HELP rbtc_tip_lag_blocks Current block lag by subsystem.\n# TYPE rbtc_tip_lag_blocks gauge\nrbtc_tip_lag_blocks{{kind=\"execution\"}} {execution_lag}\nrbtc_tip_lag_blocks{{kind=\"explorer\"}} {explorer_lag}\n# HELP rbtc_utxos Current UTXO entries by tier.\n# TYPE rbtc_utxos gauge\nrbtc_utxos{{tier=\"hot\"}} {}\nrbtc_utxos{{tier=\"cold\"}} {}\nrbtc_utxos{{tier=\"total\"}} {utxo_total}\n# HELP rbtc_ledger_segments Retained immutable ledger segments.\n# TYPE rbtc_ledger_segments gauge\nrbtc_ledger_segments {}\n# HELP rbtc_ledger_blocks Retained pruned-ledger blocks.\n# TYPE rbtc_ledger_blocks gauge\nrbtc_ledger_blocks {}\n# HELP rbtc_ledger_bytes Retained compressed ledger bytes.\n# TYPE rbtc_ledger_bytes gauge\nrbtc_ledger_bytes {}\n# HELP rbtc_ledger_first_height Oldest retained block height, or zero for an empty ledger.\n# TYPE rbtc_ledger_first_height gauge\nrbtc_ledger_first_height {ledger_first_height}\n# HELP rbtc_minimum_chainwork_reached Whether the configured IBD work floor is reached.\n# TYPE rbtc_minimum_chainwork_reached gauge\nrbtc_minimum_chainwork_reached {minimum_chainwork}\n# HELP rbtc_independently_validated Whether chainstate no longer depends on an assumed snapshot.\n# TYPE rbtc_independently_validated gauge\nrbtc_independently_validated {independently_validated}\n# HELP rbtc_wallet_enabled Whether the serving node has an embedded wallet.\n# TYPE rbtc_wallet_enabled gauge\nrbtc_wallet_enabled {wallet_enabled}\n# HELP rbtc_session_uptime_seconds Current peer-serving session uptime.\n# TYPE rbtc_session_uptime_seconds gauge\nrbtc_session_uptime_seconds {}\n",
        status.network,
        status.phase,
        status.header.height,
        status.execution.height,
        status.explorer.height,
        status.utxo_hot,
        status.utxo_cold,
        status.ledger_segments,
        status.ledger_blocks,
        status.ledger_bytes,
        status.session_uptime_seconds,
    );
    (
        [
            (
                axum::http::header::CONTENT_TYPE,
                "text/plain; version=0.0.4; charset=utf-8",
            ),
            (axum::http::header::CACHE_CONTROL, "no-store"),
        ],
        body,
    )
}

fn validation_status_router(status: BackgroundValidationStatus) -> axum::Router {
    axum::Router::new()
        .route("/api/v1/validation", axum::routing::get(validation_status))
        .with_state(status)
}

async fn validation_status(
    axum::extract::State(status): axum::extract::State<BackgroundValidationStatus>,
) -> axum::Json<ValidationStatusResponse> {
    axum::Json(status.response())
}

struct ApiServer {
    #[cfg(test)]
    address: SocketAddr,
    task: Option<tokio::task::JoinHandle<Result<(), String>>>,
    token_tasks: Vec<tokio::task::JoinHandle<()>>,
}

impl ApiServer {
    async fn bind(
        address: SocketAddr,
        index: Arc<RedbExplorerIndex>,
        explorer_events: ExplorerEventHub,
        node_status: NodeStatus,
        wallet: Option<&WalletApiRuntime>,
        rpc: Option<&RpcApiRuntime>,
        background_validation: Option<&BackgroundValidationStatus>,
    ) -> Result<Self, String> {
        let listener = tokio::net::TcpListener::bind(address)
            .await
            .map_err(|error| format!("bind explorer at {address}: {error}"))?;
        let bound = listener
            .local_addr()
            .map_err(|error| format!("read explorer address: {error}"))?;
        let mut router = explorer_router(Arc::clone(&index))
            .merge(explorer_events_router(explorer_events))
            .merge(node_status_router(node_status));
        let mut token_tasks = Vec::new();
        if let Some(wallet) = wallet {
            let token = wallet.token.clone();
            let token_path = wallet.token_path.clone();
            token_tasks.push(tokio::spawn(watch_local_auth_token(
                token_path, token, "wallet",
            )));
        }
        if let Some(wallet) = wallet {
            router = router.merge(wallet_router(
                Arc::clone(&wallet.wallet),
                wallet.token.clone(),
                wallet.audit.clone(),
            ));
        }
        if let Some(rpc) = rpc {
            let token = rpc.token.clone();
            let token_path = rpc.token_path.clone();
            token_tasks.push(tokio::spawn(watch_local_auth_token(
                token_path, token, "RPC",
            )));
            router = router.merge(rpc_router(
                Arc::clone(&index),
                rpc.token.clone(),
                rpc.audit.clone(),
            ));
        }
        if let Some(status) = background_validation {
            router = router.merge(validation_status_router(status.clone()));
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
            token_tasks,
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
        for task in &self.token_tasks {
            task.abort();
        }
    }
}

fn collect_node_status(
    network: Network,
    ibd_policy: IbdPolicy,
    headers: &HeaderDag,
    chainstate: &RedbChainStore,
    explorer: &RedbExplorerIndex,
    ledger: &PrunedBlockLedger,
    wallet: Option<&EmbeddedWallet>,
) -> Result<NodeStatusProgress, String> {
    let header = headers.active_tip();
    let execution = chainstate
        .execution()
        .tip()
        .map_err(|error| error.to_string())?;
    let explorer = explorer.tip().map_err(|error| error.to_string())?;
    let wallet = wallet
        .map(|wallet| wallet.chain_tip().map_err(|error| error.to_string()))
        .transpose()?;
    let ibd = ibd_policy
        .status(headers)
        .map_err(|error| error.to_string())?;
    let utxos = chainstate.tier_stats().map_err(|error| error.to_string())?;
    let ledger = ledger.stats().map_err(|error| error.to_string())?;
    let independently_validated = chainstate
        .execution()
        .assumed_snapshot()
        .map_err(|error| error.to_string())?
        .is_none();
    Ok(NodeStatusProgress {
        network: network.to_string(),
        header: NodeTipResponse {
            height: header.height,
            hash: header.hash.to_string(),
        },
        execution: NodeTipResponse {
            height: execution.height,
            hash: execution.hash.to_string(),
        },
        explorer: NodeTipResponse {
            height: explorer.height,
            hash: explorer.hash.to_string(),
        },
        wallet: wallet.map(|tip| NodeTipResponse {
            height: tip.height,
            hash: tip.hash.to_string(),
        }),
        minimum_chainwork_reached: ibd.minimum_chainwork_reached,
        active_assume_valid_height: ibd.active_assume_valid_height,
        full_script_validation: ibd.full_script_validation,
        independently_validated,
        utxo_hot: utxos.hot,
        utxo_cold: utxos.cold,
        ledger_segments: ledger.segments,
        ledger_blocks: ledger.blocks,
        ledger_bytes: ledger.bytes,
        ledger_first_height: ledger.first_height,
        ledger_tip_height: ledger.tip_height,
    })
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

#[allow(clippy::too_many_lines)]
fn prepare_api_runtime(options: &Options) -> Result<Option<ApiRuntime>, String> {
    let Some(listen) = options.explorer_listen else {
        return Ok(None);
    };
    if let (Some(rpc_token), Some(wallet)) = (
        options.rpc_auth_token_file.as_ref(),
        options.wallet_api_files.as_ref(),
    ) {
        if same_file(rpc_token, &wallet.auth_token)? {
            return Err(
                "RPC and wallet authentication token files must be distinct regular files"
                    .to_owned(),
            );
        }
    }
    let rpc_token = options
        .rpc_auth_token_file
        .as_ref()
        .map(|path| {
            let token = read_owner_only_text_file(
                path,
                "RPC authentication token",
                MAX_LOCAL_AUTH_TOKEN_FILE_LEN,
            )?;
            let token =
                LocalAuthToken::new(token.trim_end_matches(['\r', '\n'])).map_err(str::to_owned)?;
            Ok::<_, String>((path.clone(), token))
        })
        .transpose()?;
    let wallet = options
        .wallet_api_files
        .as_ref()
        .map(|files| {
            let descriptors = read_owner_only_text_file(
                &files.descriptors,
                "wallet descriptor",
                u64::try_from(MAX_WALLET_DESCRIPTOR_CONFIG_BYTES)
                    .expect("wallet descriptor limit fits u64"),
            )?;
            let descriptors = parse_wallet_descriptor_config(descriptors.as_bytes())
                .map_err(|error| error.to_string())?;
            let token = read_owner_only_text_file(
                &files.auth_token,
                "wallet authentication token",
                MAX_LOCAL_AUTH_TOKEN_FILE_LEN,
            )?;
            let token = LocalAuthToken::new(token.trim_end_matches(['\r', '\n']))
                .map_err(str::to_owned)?;
            let data_dir = options
                .data_dir
                .as_ref()
                .expect("wallet API parser requires data directory");
            let audit = AuthorizationAuditLog::open(data_dir.join(API_AUDIT_FILE))?;
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
                token_path: files.auth_token.clone(),
                audit,
                scan: WalletScanConfig {
                    gap_limit: descriptors.gap_limit,
                    birthday_height: descriptors.birthday_height,
                },
            })
        })
        .transpose()?;
    let rpc = rpc_token
        .map(|(token_path, token)| {
            let audit = wallet.as_ref().map_or_else(
                || {
                    let data_dir = options
                        .data_dir
                        .as_ref()
                        .expect("RPC parser requires data directory");
                    AuthorizationAuditLog::open(data_dir.join(API_AUDIT_FILE))
                },
                |wallet| Ok(wallet.audit.clone()),
            )?;
            println!(
                "authenticated read-only JSON-RPC enabled; token file {}",
                token_path.display()
            );
            Ok::<_, String>(RpcApiRuntime {
                token,
                token_path,
                audit,
            })
        })
        .transpose()?;
    Ok(Some(ApiRuntime {
        listen,
        wallet,
        rpc,
    }))
}

async fn watch_local_auth_token(path: PathBuf, token: LocalAuthToken, surface: &'static str) {
    let mut interval = tokio::time::interval(LOCAL_AUTH_TOKEN_RELOAD_INTERVAL);
    let mut enabled = true;
    loop {
        interval.tick().await;
        let result = read_owner_only_text_file(
            &path,
            &format!("{surface} authentication token"),
            MAX_LOCAL_AUTH_TOKEN_FILE_LEN,
        )
        .and_then(|contents| {
            token
                .rotate(contents.trim_end_matches(['\r', '\n']))
                .map_err(str::to_owned)
        });
        match result {
            Ok(()) if !enabled => {
                eprintln!(
                    "{surface} authentication token restored from owner-only file {}",
                    path.display()
                );
                enabled = true;
            }
            Ok(()) => {}
            Err(error) if enabled => {
                token.invalidate();
                eprintln!("{surface} authentication disabled until token file is valid: {error}");
                enabled = false;
            }
            Err(_) => {
                token.invalidate();
            }
        }
    }
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
    if let Some(validation_dir) = &options.complete_assumeutxo {
        prepare_assumeutxo_validation(&options, validation_dir)?;
    }
    if let Some(validation_dir) = &options.background_assumeutxo {
        prepare_assumeutxo_validation(&options, validation_dir)?;
    }
    if options.snapshot.is_some() {
        return activate_assumed_snapshot(&options);
    }
    if options.finalize_assumeutxo.is_some() {
        return finalize_assumed_snapshot(&options);
    }
    if let Some(validation_dir) = options.background_assumeutxo.clone() {
        return run_background_assumeutxo(options, validation_dir, local_nonce).await;
    }
    run_peer_pool(&options, local_nonce, None, None).await
}

#[allow(clippy::too_many_lines)]
async fn run_peer_pool(
    options: &Options,
    local_nonce: u64,
    background_validation: Option<&BackgroundValidationStatus>,
    validation_scheduler: Option<&BackgroundValidationStatus>,
) -> Result<(), String> {
    let api_runtime = prepare_api_runtime(options)?;
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
            options,
            remote,
            local_nonce,
            peer_store.as_ref(),
            api_runtime.as_ref(),
            background_validation,
            validation_scheduler,
        )
        .await
        {
            Ok(()) => {
                if completed_validating_session(options) {
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

    let seeds = selected_dns_seeds(options);
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
                options,
                remote,
                local_nonce,
                peer_store.as_ref(),
                api_runtime.as_ref(),
                background_validation,
                validation_scheduler,
            )
            .await
            {
                Ok(()) => {
                    if completed_validating_session(options) {
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

async fn run_background_assumeutxo(
    options: Options,
    validation_dir: PathBuf,
    local_nonce: u64,
) -> Result<(), String> {
    let active_dir = options
        .data_dir
        .as_ref()
        .expect("background validation parser requires active data directory");
    let active = RedbChainStore::open(active_dir.join("chainstate.redb"), options.network)
        .map_err(|error| error.to_string())?;
    let assumed = active
        .execution()
        .assumed_snapshot()
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "active chainstate has no assumed UTXO snapshot to validate".to_owned())?;
    drop(active);

    let target = ValidationTarget {
        height: assumed.base.height,
        block_hash: assumed.base.hash,
    };
    let status = BackgroundValidationStatus::new(
        validation_dir.clone(),
        target,
        options.cleanup_validation_dir,
    );
    let mut active_options = options.clone();
    active_options.background_assumeutxo = None;
    active_options.validation_limits = ValidationLimits::default();

    let mut validation_options = options;
    validation_options.data_dir = Some(validation_dir.clone());
    validation_options.once = false;
    validation_options.explorer_listen = None;
    validation_options.wallet_api_files = None;
    validation_options.complete_assumeutxo = None;
    validation_options.background_assumeutxo = None;
    validation_options.validation_target = Some(target);

    println!(
        "starting active-chain service and independent genesis validation concurrently through {}:{}",
        assumed.base.height, assumed.base.hash
    );
    let validation_status = status.clone();
    let validation = async {
        let result = run_peer_pool(
            &validation_options,
            local_nonce,
            None,
            Some(&validation_status),
        )
        .await;
        match &result {
            Ok(()) => validation_status.set(BackgroundValidationState::Complete),
            Err(error) => {
                validation_status.set(BackgroundValidationState::Failed(error.clone()));
            }
        }
        result
    };
    let active = run_peer_pool(&active_options, local_nonce, Some(&status), None);
    tokio::pin!(active);
    tokio::pin!(validation);
    tokio::select! {
        active_result = &mut active => {
            active_result?;
            validation.await?;
        }
        validation_result = &mut validation => {
            validation_result?;
            active.await?;
        }
    }
    finalize_background_if_pending(&active_options, &validation_dir)
}

fn finalize_background_if_pending(
    options: &Options,
    validation_dir: &std::path::Path,
) -> Result<(), String> {
    let active_dir = options
        .data_dir
        .as_ref()
        .expect("background validation requires active data directory");
    let active = RedbChainStore::open(active_dir.join("chainstate.redb"), options.network)
        .map_err(|error| error.to_string())?;
    let pending = active
        .execution()
        .assumed_snapshot()
        .map_err(|error| error.to_string())?
        .is_some();
    let origin = active
        .execution()
        .snapshot_origin()
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "active chainstate is missing snapshot origin metadata".to_owned())?;
    drop(active);
    if pending {
        finalize_assumed_snapshot_from(options, validation_dir)?;
    }
    if options.cleanup_validation_dir && validation_dir.exists() {
        cleanup_completed_validation_dir(
            active_dir,
            validation_dir,
            options.network,
            ValidationTarget {
                height: origin.height,
                block_hash: origin.hash,
            },
        )?;
    }
    Ok(())
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

fn finalize_assumed_snapshot(options: &Options) -> Result<(), String> {
    let validation_dir = options
        .finalize_assumeutxo
        .as_ref()
        .expect("caller checked snapshot finalization mode");
    finalize_assumed_snapshot_from(options, validation_dir)
}

fn finalize_assumed_snapshot_from(
    options: &Options,
    validation_dir: &std::path::Path,
) -> Result<(), String> {
    let data_dir = options
        .data_dir
        .as_ref()
        .expect("finalization parser requires data directory");
    let active_path = data_dir.join("chainstate.redb");
    let validation_path = validation_dir.join("chainstate.redb");
    if !validation_path.is_file() {
        return Err(format!(
            "validation chainstate does not exist: {}",
            validation_path.display()
        ));
    }
    if same_file(&active_path, &validation_path)? {
        return Err("validation chainstate must be separate from active chainstate".to_owned());
    }
    reject_legacy_split_chainstate(validation_dir)?;
    let header_store =
        RedbHeaderStore::open(data_dir.join("headers.redb")).map_err(|error| error.to_string())?;
    let headers = header_store
        .load_dag_with_deployments(options.deployments.clone(), unix_time()?)
        .map_err(|error| error.to_string())?;
    let active =
        RedbChainStore::open(active_path, options.network).map_err(|error| error.to_string())?;
    finalize_assumed_snapshot_with(
        options.network,
        &options.deployments,
        &active,
        validation_dir,
        &headers,
    )
}

fn finalize_assumed_snapshot_with(
    network: Network,
    deployments: &DeploymentConfig,
    active: &RedbChainStore,
    validation_dir: &std::path::Path,
    headers: &HeaderDag,
) -> Result<(), String> {
    let validation_path = validation_dir.join("chainstate.redb");
    let validation =
        RedbChainStore::open(validation_path, network).map_err(|error| error.to_string())?;
    validation
        .execution()
        .bind_consensus_config(
            &deployments.consensus_id(),
            &DeploymentConfig::for_network(network).consensus_id(),
        )
        .map_err(|error| error.to_string())?;
    let finalized = active
        .finalize_assumed_snapshot(&validation, headers)
        .map_err(|error| error.to_string())?;
    println!(
        "independent genesis validation matched assumed UTXO snapshot at {}:{} ({} entries/{} canonical bytes); assumed-state marker cleared",
        finalized.base.height, finalized.base.hash, finalized.utxo_count, finalized.records_bytes
    );
    Ok(())
}

fn poll_background_validation(
    status: Option<&BackgroundValidationStatus>,
    network: Network,
    deployments: &DeploymentConfig,
    active: &RedbChainStore,
    active_dir: &std::path::Path,
    headers: &HeaderDag,
) -> Result<(), PeerRunError> {
    let Some(status) = status else {
        return Ok(());
    };
    match status.state() {
        BackgroundValidationState::Pending | BackgroundValidationState::Finalized => Ok(()),
        BackgroundValidationState::Failed(error) => Err(PeerRunError::transient(format!(
            "background genesis validation failed: {error}"
        ))),
        BackgroundValidationState::Complete => {
            if let Err(error) = finalize_assumed_snapshot_with(
                network,
                deployments,
                active,
                &status.validation_dir,
                headers,
            ) {
                status.set(BackgroundValidationState::Failed(error.clone()));
                return Err(PeerRunError::transient(format!(
                    "background genesis validation finalization failed: {error}"
                )));
            }
            if status.cleanup_after_finalize {
                if let Err(error) = cleanup_completed_validation_dir(
                    active_dir,
                    &status.validation_dir,
                    network,
                    status.target(),
                ) {
                    status.set(BackgroundValidationState::Failed(error.clone()));
                    return Err(PeerRunError::transient(format!(
                        "background validation directory cleanup failed: {error}"
                    )));
                }
            }
            status.set(BackgroundValidationState::Finalized);
            println!("background genesis validation finalized while the active node remained live");
            Ok(())
        }
    }
}

fn prepare_assumeutxo_validation(
    options: &Options,
    validation_dir: &std::path::Path,
) -> Result<(), String> {
    let active_dir = options
        .data_dir
        .as_ref()
        .expect("completion parser requires active data directory");
    reject_directory_symlink(validation_dir, "validation data directory")?;
    let claimable = match fs::read_dir(validation_dir) {
        Ok(mut entries) => entries.next().is_none(),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => true,
        Err(error) => {
            return Err(format!(
                "inspect validation data directory {}: {error}",
                validation_dir.display()
            ));
        }
    };
    fs::create_dir_all(validation_dir).map_err(|error| {
        format!(
            "create validation data directory {}: {error}",
            validation_dir.display()
        )
    })?;
    let active_dir = fs::canonicalize(active_dir)
        .map_err(|error| format!("resolve active data directory: {error}"))?;
    let validation_dir = fs::canonicalize(validation_dir)
        .map_err(|error| format!("resolve validation data directory: {error}"))?;
    if active_dir == validation_dir
        || active_dir.starts_with(&validation_dir)
        || validation_dir.starts_with(&active_dir)
    {
        return Err(
            "validation data directory must be separate from and not contain or be contained by the active data directory"
                .to_owned(),
        );
    }
    let active_chainstate = active_dir.join("chainstate.redb");
    let validation_chainstate = validation_dir.join("chainstate.redb");
    if validation_chainstate.exists() && same_file(&active_chainstate, &validation_chainstate)? {
        return Err("validation chainstate must be separate from active chainstate".to_owned());
    }
    preflight_data_dir(&validation_dir, options.network, &options.deployments)?;
    if same_file(&active_chainstate, &validation_chainstate)? {
        return Err("validation chainstate must be separate from active chainstate".to_owned());
    }
    let active = RedbChainStore::open(active_chainstate, options.network)
        .map_err(|error| error.to_string())?;
    let assumed = active
        .execution()
        .assumed_snapshot()
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "active chainstate has no assumed UTXO snapshot to validate".to_owned())?;
    let owner =
        ValidationDirectoryOwner::new(options.network, assumed.base.height, assumed.base.hash);
    ensure_validation_directory_owner(
        &validation_dir,
        &owner,
        claimable,
        options.cleanup_validation_dir,
    )?;
    Ok(())
}

fn ensure_validation_directory_owner(
    validation_dir: &std::path::Path,
    expected: &ValidationDirectoryOwner,
    claimable: bool,
    cleanup_requested: bool,
) -> Result<(), String> {
    ensure_validation_directory_owner_with_sync(
        validation_dir,
        expected,
        claimable,
        cleanup_requested,
        sync_directory,
    )
}

fn ensure_validation_directory_owner_with_sync(
    validation_dir: &std::path::Path,
    expected: &ValidationDirectoryOwner,
    claimable: bool,
    cleanup_requested: bool,
    mut sync_dir: impl FnMut(&std::path::Path) -> io::Result<()>,
) -> Result<(), String> {
    let marker = validation_dir.join(VALIDATION_OWNER_FILE);
    if marker.exists() {
        let contents = read_owner_only_text_file(
            &marker,
            "validation directory owner",
            MAX_VALIDATION_OWNER_BYTES as u64,
        )?;
        let actual = parse_validation_directory_owner(contents.as_bytes())
            .map_err(|_| "validation directory owner marker is malformed".to_owned())?;
        if actual != *expected {
            return Err(
                "validation directory owner marker does not match this snapshot".to_owned(),
            );
        }
        return Ok(());
    }
    if !claimable {
        if cleanup_requested {
            return Err(
                "validation directory cleanup requires an rBTC owner marker created in an initially empty directory"
                    .to_owned(),
            );
        }
        return Ok(());
    }
    let mut builder = fs::OpenOptions::new();
    builder.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        builder.mode(0o600);
    }
    let mut file = builder.open(&marker).map_err(|error| {
        format!(
            "create validation owner marker {}: {error}",
            marker.display()
        )
    })?;
    let mut contents = serde_json::to_vec(expected)
        .map_err(|error| format!("encode validation owner marker: {error}"))?;
    contents.push(b'\n');
    file.write_all(&contents)
        .and_then(|()| file.sync_all())
        .map_err(|error| {
            format!(
                "persist validation owner marker {}: {error}",
                marker.display()
            )
        })?;
    sync_dir(validation_dir)
        .map_err(|error| format!("sync validation owner marker directory: {error}"))?;
    Ok(())
}

fn cleanup_completed_validation_dir(
    active_dir: &std::path::Path,
    validation_dir: &std::path::Path,
    network: Network,
    expected: ValidationTarget,
) -> Result<(), String> {
    reject_directory_symlink(validation_dir, "validation data directory before cleanup")?;
    let active_dir = fs::canonicalize(active_dir)
        .map_err(|error| format!("resolve active data directory before cleanup: {error}"))?;
    let validation_dir = fs::canonicalize(validation_dir)
        .map_err(|error| format!("resolve validation data directory before cleanup: {error}"))?;
    if active_dir == validation_dir
        || active_dir.starts_with(&validation_dir)
        || validation_dir.starts_with(&active_dir)
    {
        return Err(
            "refusing to clean a validation directory that contains or is contained by the active data directory"
                .to_owned(),
        );
    }
    let owner = ValidationDirectoryOwner::new(network, expected.height, expected.block_hash);
    ensure_validation_directory_owner(&validation_dir, &owner, false, true)?;
    let validation = RedbChainStore::open(validation_dir.join("chainstate.redb"), network)
        .map_err(|error| error.to_string())?;
    let target = rbtc::execution_store::ExecutionTip {
        height: expected.height,
        hash: expected.block_hash,
    };
    if validation
        .execution()
        .assumed_snapshot()
        .map_err(|error| error.to_string())?
        .is_some()
        || validation
            .execution()
            .validation_target()
            .map_err(|error| error.to_string())?
            != Some(target)
        || validation
            .execution()
            .tip()
            .map_err(|error| error.to_string())?
            != target
    {
        return Err(
            "validation directory cleanup requires a completed non-assumed target chainstate"
                .to_owned(),
        );
    }
    drop(validation);
    for entry in fs::read_dir(&validation_dir)
        .map_err(|error| format!("inspect validation directory before cleanup: {error}"))?
    {
        let entry = entry.map_err(|error| format!("inspect validation entry: {error}"))?;
        let allowed = matches!(
            entry.file_name().to_str(),
            Some(
                VALIDATION_OWNER_FILE
                    | "chainstate.redb"
                    | "headers.redb"
                    | "peers.redb"
                    | "explorer.redb"
                    | "blocks"
            )
        );
        if !allowed {
            return Err(format!(
                "refusing validation cleanup because {} is not an rBTC validation artifact",
                entry.path().display()
            ));
        }
    }
    reject_cleanup_symlinks(&validation_dir)?;
    quarantine_and_remove_validation_dir(&validation_dir)
}

fn quarantine_and_remove_validation_dir(validation_dir: &std::path::Path) -> Result<(), String> {
    quarantine_and_remove_validation_dir_with_sync(validation_dir, sync_directory)
}

fn quarantine_and_remove_validation_dir_with_sync(
    validation_dir: &std::path::Path,
    mut sync_parent: impl FnMut(&std::path::Path) -> io::Result<()>,
) -> Result<(), String> {
    let parent = validation_dir
        .parent()
        .ok_or_else(|| "validation directory has no parent".to_owned())?;
    let name = validation_dir
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| "validation directory name is not UTF-8".to_owned())?;
    let tombstone = parent.join(format!(
        ".{name}.rbtc-cleanup-{:016x}",
        rand::random::<u64>()
    ));
    fs::rename(validation_dir, &tombstone).map_err(|error| {
        format!(
            "atomically quarantine validation directory {}: {error}",
            validation_dir.display()
        )
    })?;
    if let Err(sync_error) = sync_parent(parent) {
        return match fs::rename(&tombstone, validation_dir) {
            Ok(()) => match sync_parent(parent) {
                Ok(()) => Err(format!(
                    "sync validation directory parent: {sync_error}; quarantine was rolled back"
                )),
                Err(rollback_sync_error) => Err(format!(
                    "sync validation directory parent: {sync_error}; quarantine was rolled back but syncing the rollback failed: {rollback_sync_error}"
                )),
            },
            Err(rollback_error) => Err(format!(
                "sync validation directory parent: {sync_error}; validation directory remains quarantined at {} because rollback failed: {rollback_error}",
                tombstone.display()
            )),
        };
    }
    fs::remove_dir_all(&tombstone).map_err(|error| {
        format!(
            "validation directory was quarantined at {} but removal failed: {error}",
            tombstone.display()
        )
    })?;
    sync_parent(parent)
        .map_err(|error| format!("sync removed validation directory parent: {error}"))?;
    println!(
        "removed completed validation directory {} after owner and target revalidation; recovery is no longer possible",
        validation_dir.display()
    );
    Ok(())
}

fn sync_directory(path: &std::path::Path) -> io::Result<()> {
    #[cfg(unix)]
    {
        fs::File::open(path)?.sync_all()
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        Ok(())
    }
}

fn reject_directory_symlink(path: &std::path::Path, label: &str) -> Result<(), String> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            Err(format!("{label} must not be a symbolic link"))
        }
        Ok(_) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(format!("inspect {label} {}: {error}", path.display())),
    }
}

fn reject_cleanup_symlinks(path: &std::path::Path) -> Result<(), String> {
    for entry in fs::read_dir(path)
        .map_err(|error| format!("inspect cleanup tree {}: {error}", path.display()))?
    {
        let entry = entry.map_err(|error| format!("inspect cleanup tree entry: {error}"))?;
        let metadata = fs::symlink_metadata(entry.path()).map_err(|error| {
            format!("inspect cleanup entry {}: {error}", entry.path().display())
        })?;
        if metadata.file_type().is_symlink() {
            return Err(format!(
                "refusing validation cleanup because {} is a symlink",
                entry.path().display()
            ));
        }
        if metadata.is_dir() {
            reject_cleanup_symlinks(&entry.path())?;
        } else if !metadata.is_file() {
            return Err(format!(
                "refusing validation cleanup because {} is not a regular file",
                entry.path().display()
            ));
        }
    }
    Ok(())
}

fn same_file(first: &std::path::Path, second: &std::path::Path) -> Result<bool, String> {
    let first_canonical =
        fs::canonicalize(first).map_err(|error| format!("resolve {}: {error}", first.display()))?;
    let second_canonical = fs::canonicalize(second)
        .map_err(|error| format!("resolve {}: {error}", second.display()))?;
    if first_canonical == second_canonical {
        return Ok(true);
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let first = fs::metadata(first_canonical)
            .map_err(|error| format!("inspect {}: {error}", first.display()))?;
        let second = fs::metadata(second_canonical)
            .map_err(|error| format!("inspect {}: {error}", second.display()))?;
        Ok(first.dev() == second.dev() && first.ino() == second.ino())
    }
    #[cfg(not(unix))]
    Ok(false)
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
    background_validation: Option<&BackgroundValidationStatus>,
    validation_scheduler: Option<&BackgroundValidationStatus>,
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
        if let Some(validation_dir) = &options.complete_assumeutxo {
            return complete_assumeutxo_validation(&mut session, options, validation_dir.clone())
                .await;
        }
        return sync_validating_node(
            &mut session,
            options.network,
            &options.deployments,
            options.ibd_policy,
            path.clone(),
            options.once,
            options.validation_target,
            options.validation_limits,
            api_runtime,
            background_validation,
            validation_scheduler,
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

async fn complete_assumeutxo_validation(
    session: &mut rbtc::p2p::PeerSession<tokio::net::TcpStream>,
    options: &Options,
    validation_dir: PathBuf,
) -> Result<(), PeerRunError> {
    let active_dir = options
        .data_dir
        .as_ref()
        .expect("completion parser requires active data directory");
    let active = RedbChainStore::open(active_dir.join("chainstate.redb"), options.network)
        .map_err(|error| error.to_string())?;
    let assumed = active
        .execution()
        .assumed_snapshot()
        .map_err(|error| error.to_string())?
        .ok_or_else(|| {
            PeerRunError::transient("active chainstate has no assumed UTXO snapshot to validate")
        })?;
    drop(active);
    sync_validating_node(
        session,
        options.network,
        &options.deployments,
        options.ibd_policy,
        validation_dir.clone(),
        false,
        Some(ValidationTarget {
            height: assumed.base.height,
            block_hash: assumed.base.hash,
        }),
        options.validation_limits,
        None,
        None,
        None,
    )
    .await?;
    finalize_assumed_snapshot_from(options, &validation_dir).map_err(PeerRunError::transient)?;
    if options.cleanup_validation_dir {
        cleanup_completed_validation_dir(
            active_dir,
            &validation_dir,
            options.network,
            ValidationTarget {
                height: assumed.base.height,
                block_hash: assumed.base.hash,
            },
        )
        .map_err(PeerRunError::transient)?;
    }
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

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
async fn sync_validating_node(
    session: &mut rbtc::p2p::PeerSession<tokio::net::TcpStream>,
    network: Network,
    deployment_config: &DeploymentConfig,
    ibd_policy: IbdPolicy,
    data_dir: PathBuf,
    once: bool,
    validation_target: Option<ValidationTarget>,
    validation_limits: ValidationLimits,
    api_runtime: Option<&ApiRuntime>,
    background_validation: Option<&BackgroundValidationStatus>,
    validation_scheduler: Option<&BackgroundValidationStatus>,
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
    let execution_store = chainstate.execution();
    execution_store
        .bind_consensus_config(
            &deployment_config.consensus_id(),
            &DeploymentConfig::for_network(network).consensus_id(),
        )
        .map_err(|error| error.to_string())?;
    if let Some(target) = validation_target {
        execution_store
            .bind_validation_target(rbtc::execution_store::ExecutionTip {
                height: target.height,
                hash: target.block_hash,
            })
            .map_err(|error| error.to_string())?;
    }
    let validation_target = execution_store
        .validation_target()
        .map_err(|error| error.to_string())?
        .map(|target| ValidationTarget {
            height: target.height,
            block_hash: target.hash,
        })
        .or(validation_target);
    let ledger = PrunedBlockLedger::open(data_dir.join("blocks"), LedgerRetention::default())
        .map_err(|error| error.to_string())?;
    let explorer = Arc::new(
        RedbExplorerIndex::open(data_dir.join("explorer.redb"), network)
            .map_err(|error| error.to_string())?,
    );
    let explorer_events = api_runtime
        .map(|_| {
            explorer
                .tip()
                .map(|tip| ExplorerEventHub::new(tip.height, tip.hash.to_string()))
                .map_err(|error| error.to_string())
        })
        .transpose()?;
    let wallet_runtime = api_runtime.and_then(|api| api.wallet.as_ref());
    let wallet = wallet_runtime.map(|runtime| runtime.wallet.as_ref());
    let mut api_server: Option<ApiServer> = None;
    let mut headers = sync_headers(session, deployment_config, headers_path.clone()).await?;
    let node_status = api_runtime
        .map(|_| {
            collect_node_status(
                network,
                ibd_policy,
                &headers,
                &chainstate,
                &explorer,
                &ledger,
                wallet,
            )
            .map(NodeStatus::new)
        })
        .transpose()?;
    'resync: loop {
        let current_tip = execution_store.tip().map_err(|error| error.to_string())?;
        if let Some(status) = background_validation {
            status.update_active(current_tip.height, headers.active_tip().height);
        }
        if let Some(status) = validation_scheduler {
            status.update_validation(current_tip.height);
        }
        if let Some(target) = validation_target {
            let active = headers.active_header_at(target.height).ok_or_else(|| {
                PeerRunError::transient(format!(
                    "validation target height {} is above the available active header chain",
                    target.height
                ))
            })?;
            if active.hash != target.block_hash {
                return Err(PeerRunError::transient(format!(
                    "validation target {}:{} is not on the active header chain",
                    target.height, target.block_hash
                )));
            }
        }
        poll_background_validation(
            background_validation,
            network,
            deployment_config,
            &chainstate,
            &data_dir,
            &headers,
        )?;
        if let Some(server) = &mut api_server {
            server.ensure_running().await?;
        }
        loop {
            if let Some(server) = &mut api_server {
                server.ensure_running().await?;
            }
            poll_background_validation(
                background_validation,
                network,
                deployment_config,
                &chainstate,
                &data_dir,
                &headers,
            )?;
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
        if let Some(target) = validation_target {
            let tip = execution_store.tip().map_err(|error| error.to_string())?;
            if tip.height > target.height {
                return Err(PeerRunError::transient(format!(
                    "validation chainstate tip {}:{} is already above target {}:{}",
                    tip.height, tip.hash, target.height, target.block_hash
                )));
            }
            if tip.height == target.height && tip.hash != target.block_hash {
                return Err(PeerRunError::transient(format!(
                    "validation chainstate tip {}:{} does not match target {}:{}",
                    tip.height, tip.hash, target.height, target.block_hash
                )));
            }
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
            &chainstate,
            &ledger,
            &explorer,
            explorer_events.as_ref(),
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
        if let Some(status) = &node_status {
            status.update(collect_node_status(
                network,
                ibd_policy,
                &headers,
                &chainstate,
                &explorer,
                &ledger,
                wallet,
            )?);
        }
        if api_server.is_none() {
            if let Some(api) = api_runtime {
                api_server = Some(
                    ApiServer::bind(
                        api.listen,
                        Arc::clone(&explorer),
                        explorer_events
                            .as_ref()
                            .expect("API runtime has explorer events")
                            .clone(),
                        node_status
                            .as_ref()
                            .expect("API runtime has node status")
                            .clone(),
                        api.wallet.as_ref(),
                        api.rpc.as_ref(),
                        background_validation,
                    )
                    .await?,
                );
            }
        }

        loop {
            if let Some(server) = &mut api_server {
                server.ensure_running().await?;
            }
            let tip = execution_store.tip().map_err(|error| error.to_string())?;
            if let Some(status) = &node_status {
                status.update(collect_node_status(
                    network,
                    ibd_policy,
                    &headers,
                    &chainstate,
                    &explorer,
                    &ledger,
                    wallet,
                )?);
            }
            if let Some(status) = background_validation {
                status.update_active(tip.height, headers.active_tip().height);
            }
            if let Some(status) = validation_scheduler {
                status.update_validation(tip.height);
            }
            if let Some(target) = validation_target {
                if tip.height > target.height {
                    return Err(PeerRunError::transient(format!(
                        "validation chainstate tip {}:{} is already above target {}:{}",
                        tip.height, tip.hash, target.height, target.block_hash
                    )));
                }
                if tip.height == target.height {
                    if tip.hash != target.block_hash {
                        return Err(PeerRunError::transient(format!(
                            "validation chainstate tip {}:{} does not match target {}:{}",
                            tip.height, tip.hash, target.height, target.block_hash
                        )));
                    }
                    println!(
                        "independent genesis validation stopped exactly at {}:{}",
                        tip.height, tip.hash
                    );
                    return Ok(());
                }
            }
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
                let poll_seconds = if background_validation.is_some() {
                    1
                } else {
                    30
                };
                tokio::time::sleep(Duration::from_secs(poll_seconds)).await;
                headers = sync_headers(session, deployment_config, headers_path.clone()).await?;
                continue 'resync;
            }
            let effective_validation_limits = validation_scheduler
                .map_or(validation_limits, |status| {
                    status.adaptive_limits(validation_limits)
                });
            download_execute_batch(
                session,
                deployment_config,
                &headers,
                &chainstate,
                &ledger,
                &explorer,
                explorer_events.as_ref(),
                wallet,
                validation_target.map(|target| target.height),
                effective_validation_limits.max_blocks_per_batch,
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
            if let Some(target) = validation_target {
                let tip = execution_store.tip().map_err(|error| error.to_string())?;
                if let Some(status) = validation_scheduler {
                    status.update_validation(tip.height);
                }
                let remaining = target.height.saturating_sub(tip.height);
                println!(
                    "genesis validation progress: {} / {} blocks ({} remaining; batch cap {}; pause {} ms)",
                    tip.height,
                    target.height,
                    remaining,
                    effective_validation_limits.max_blocks_per_batch,
                    effective_validation_limits
                        .pause_between_batches
                        .as_millis()
                );
                if remaining > 0 && !effective_validation_limits.pause_between_batches.is_zero() {
                    tokio::time::sleep(effective_validation_limits.pause_between_batches).await;
                }
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

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
async fn reconcile_explorer(
    session: &mut rbtc::p2p::PeerSession<tokio::net::TcpStream>,
    deployment_config: &DeploymentConfig,
    headers: &HeaderDag,
    chainstate: &RedbChainStore,
    ledger: &PrunedBlockLedger,
    explorer: &RedbExplorerIndex,
    events: Option<&ExplorerEventHub>,
) -> Result<(), String> {
    let execution_store = chainstate.execution();
    let undo_store = chainstate.undos();
    let execution_tip = execution_store.tip().map_err(|error| error.to_string())?;
    if execution_store
        .snapshot_origin()
        .map_err(|error| error.to_string())?
        .is_some()
        && explorer
            .baseline()
            .map_err(|error| error.to_string())?
            .is_none()
    {
        let count = explorer
            .replace_with_chainstate_baseline(execution_tip, chainstate)
            .map_err(|error| error.to_string())?;
        println!(
            "initialized snapshot-aware explorer baseline at {}:{} with {count} current UTXOs",
            execution_tip.height, execution_tip.hash
        );
        publish_explorer_event(events, ExplorerEventKind::Rebased, execution_tip);
    }
    loop {
        let explorer_tip = explorer.tip().map_err(|error| error.to_string())?;
        if explorer_tip.height <= execution_tip.height
            && headers
                .active_header_at(explorer_tip.height)
                .is_some_and(|header| header.hash == explorer_tip.hash)
        {
            break;
        }
        if explorer.baseline().map_err(|error| error.to_string())? == Some(explorer_tip) {
            let count = explorer
                .replace_with_chainstate_baseline(execution_tip, chainstate)
                .map_err(|error| error.to_string())?;
            println!(
                "rebased snapshot-aware explorer at {}:{} with {count} current UTXOs after reorganization",
                execution_tip.height, execution_tip.hash
            );
            publish_explorer_event(events, ExplorerEventKind::Rebased, execution_tip);
            break;
        }
        let rewound = explorer
            .disconnect_tip()
            .map_err(|error| error.to_string())?;
        println!(
            "disconnected stale explorer tip; rewound to {}:{}",
            rewound.height, rewound.hash
        );
        publish_explorer_event(events, ExplorerEventKind::Disconnected, rewound);
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
            publish_explorer_event(
                events,
                ExplorerEventKind::Connected,
                rbtc::execution_store::ExecutionTip {
                    height: expected.height,
                    hash: expected.hash,
                },
            );
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
    explorer_events: Option<&ExplorerEventHub>,
    wallet: Option<&EmbeddedWallet>,
    maximum_height: Option<u32>,
    maximum_batch_size: usize,
) -> Result<(), PeerRunError> {
    let execution_store = chainstate.execution();
    let tip = execution_store.tip().map_err(|error| error.to_string())?;
    let next_height = tip
        .height
        .checked_add(1)
        .ok_or_else(|| "execution height overflow".to_owned())?;
    let execution_ceiling = maximum_height
        .unwrap_or_else(|| headers.active_tip().height)
        .min(headers.active_tip().height);
    let remaining = execution_ceiling.checked_sub(tip.height).ok_or_else(|| {
        PeerRunError::transient(format!(
            "execution tip {} is above requested ceiling {execution_ceiling}",
            tip.height
        ))
    })?;
    let batch_len = usize::try_from(remaining)
        .unwrap_or(usize::MAX)
        .min(maximum_batch_size);
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
        index_validated_block(
            explorer,
            explorer_events,
            wallet,
            expected.height,
            block,
            applied,
        )?;
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
    explorer_events: Option<&ExplorerEventHub>,
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
    publish_explorer_event(
        explorer_events,
        ExplorerEventKind::Connected,
        rbtc::execution_store::ExecutionTip {
            height,
            hash: applied.hash,
        },
    );
    Ok(())
}

fn publish_explorer_event(
    events: Option<&ExplorerEventHub>,
    kind: ExplorerEventKind,
    tip: rbtc::execution_store::ExecutionTip,
) {
    if let Some(events) = events {
        events.publish(kind, tip.height, tip.hash.to_string());
    }
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
    let mut rpc_auth_token_file = None;
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
    let mut finalize_assumeutxo = None;
    let mut validation_height = None;
    let mut validation_block_hash = None;
    let mut complete_assumeutxo = None;
    let mut background_assumeutxo = None;
    let mut cleanup_validation_dir = false;
    let mut validation_batch_size = None;
    let mut validation_pause_ms = None;
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
                        "--explorer-listen must use a loopback IP because explorer REST routes are unauthenticated"
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
            "--rpc-auth-token-file" => {
                rpc_auth_token_file = Some(PathBuf::from(required_option_value(
                    &mut args,
                    "--rpc-auth-token-file",
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
            "--finalize-assumeutxo" => {
                if finalize_assumeutxo.is_some() {
                    return Err(
                        "--finalize-assumeutxo cannot be supplied more than once".to_owned()
                    );
                }
                finalize_assumeutxo = Some(PathBuf::from(required_option_value(
                    &mut args,
                    "--finalize-assumeutxo",
                )?));
            }
            "--validate-until-height" => {
                if validation_height.is_some() {
                    return Err(
                        "--validate-until-height cannot be supplied more than once".to_owned()
                    );
                }
                let value = required_option_value(&mut args, "--validate-until-height")?;
                let height = value
                    .parse::<u32>()
                    .map_err(|_| format!("invalid validation target height: {value}"))?;
                if height == 0 {
                    return Err("validation target height must be above genesis".to_owned());
                }
                validation_height = Some(height);
            }
            "--validate-until-blockhash" => {
                if validation_block_hash.is_some() {
                    return Err(
                        "--validate-until-blockhash cannot be supplied more than once".to_owned(),
                    );
                }
                let value = required_option_value(&mut args, "--validate-until-blockhash")?;
                validation_block_hash = Some(
                    BlockHash::from_str(&value)
                        .map_err(|_| format!("invalid validation target block hash: {value}"))?,
                );
            }
            "--complete-assumeutxo" => {
                if complete_assumeutxo.is_some() {
                    return Err(
                        "--complete-assumeutxo cannot be supplied more than once".to_owned()
                    );
                }
                complete_assumeutxo = Some(PathBuf::from(required_option_value(
                    &mut args,
                    "--complete-assumeutxo",
                )?));
            }
            "--background-assumeutxo" => {
                if background_assumeutxo.is_some() {
                    return Err(
                        "--background-assumeutxo cannot be supplied more than once".to_owned()
                    );
                }
                background_assumeutxo = Some(PathBuf::from(required_option_value(
                    &mut args,
                    "--background-assumeutxo",
                )?));
            }
            "--cleanup-validation-dir" => cleanup_validation_dir = true,
            "--validation-batch-size" => {
                if validation_batch_size.is_some() {
                    return Err(
                        "--validation-batch-size cannot be supplied more than once".to_owned()
                    );
                }
                let value = required_option_value(&mut args, "--validation-batch-size")?;
                let size = value
                    .parse::<usize>()
                    .map_err(|_| format!("invalid validation batch size: {value}"))?;
                if !(1..=MAX_BLOCKS_IN_FLIGHT).contains(&size) {
                    return Err(format!(
                        "validation batch size must be between 1 and {MAX_BLOCKS_IN_FLIGHT}"
                    ));
                }
                validation_batch_size = Some(size);
            }
            "--validation-pause-ms" => {
                if validation_pause_ms.is_some() {
                    return Err(
                        "--validation-pause-ms cannot be supplied more than once".to_owned()
                    );
                }
                let value = required_option_value(&mut args, "--validation-pause-ms")?;
                let pause = value
                    .parse::<u64>()
                    .map_err(|_| format!("invalid validation pause: {value}"))?;
                if pause > MAX_VALIDATION_PAUSE_MS {
                    return Err(format!(
                        "validation pause cannot exceed {MAX_VALIDATION_PAUSE_MS} milliseconds"
                    ));
                }
                validation_pause_ms = Some(pause);
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
    if rpc_auth_token_file.is_some() && (explorer_listen.is_none() || data_dir.is_none()) {
        return Err(
            "--rpc-auth-token-file requires --data-dir and loopback --explorer-listen".to_owned(),
        );
    }
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
    if finalize_assumeutxo.is_some() {
        if data_dir.is_none() {
            return Err("--finalize-assumeutxo requires --data-dir".to_owned());
        }
        if snapshot.is_some()
            || fetch_block.is_some()
            || headers_db.is_some()
            || once
            || explorer_listen.is_some()
            || !remotes.is_empty()
            || !dns_seed_values.is_empty()
            || no_dns_seeds
        {
            return Err(
                "snapshot finalization is offline and conflicts with snapshot activation, peer, fetch, headers-db, once, explorer, and wallet modes"
                    .to_owned(),
            );
        }
    }
    let validation_target = match (validation_height, validation_block_hash) {
        (None, None) => None,
        (Some(height), Some(block_hash)) => {
            if data_dir.is_none() {
                return Err("bounded genesis validation requires --data-dir".to_owned());
            }
            if snapshot.is_some()
                || finalize_assumeutxo.is_some()
                || fetch_block.is_some()
                || headers_db.is_some()
                || explorer_listen.is_some()
            {
                return Err(
                    "bounded genesis validation conflicts with snapshot, finalization, fetch, headers-db, explorer, and wallet modes"
                        .to_owned(),
                );
            }
            Some(ValidationTarget { height, block_hash })
        }
        _ => {
            return Err(
                "--validate-until-height and --validate-until-blockhash must be supplied together"
                    .to_owned(),
            );
        }
    };
    if complete_assumeutxo.is_some() {
        if data_dir.is_none() {
            return Err("--complete-assumeutxo requires --data-dir".to_owned());
        }
        if snapshot.is_some()
            || finalize_assumeutxo.is_some()
            || validation_target.is_some()
            || fetch_block.is_some()
            || headers_db.is_some()
            || once
            || explorer_listen.is_some()
            || background_assumeutxo.is_some()
        {
            return Err(
                "automatic AssumeUTXO completion conflicts with snapshot activation, offline finalization, explicit validation targets, fetch, headers-db, once, explorer, and wallet modes"
                    .to_owned(),
            );
        }
    }
    if background_assumeutxo.is_some() {
        if data_dir.is_none() {
            return Err("--background-assumeutxo requires --data-dir".to_owned());
        }
        if snapshot.is_some()
            || finalize_assumeutxo.is_some()
            || validation_target.is_some()
            || complete_assumeutxo.is_some()
            || fetch_block.is_some()
            || headers_db.is_some()
        {
            return Err(
                "background AssumeUTXO validation conflicts with snapshot activation, offline finalization, explicit validation targets, sequential completion, fetch, and headers-db modes"
                    .to_owned(),
            );
        }
    }
    if (validation_batch_size.is_some() || validation_pause_ms.is_some())
        && validation_target.is_none()
        && complete_assumeutxo.is_none()
        && background_assumeutxo.is_none()
    {
        return Err(
            "validation resource limits require bounded or automatic AssumeUTXO validation"
                .to_owned(),
        );
    }
    if cleanup_validation_dir && complete_assumeutxo.is_none() && background_assumeutxo.is_none() {
        return Err(
            "--cleanup-validation-dir requires automatic or background AssumeUTXO validation"
                .to_owned(),
        );
    }
    let validation_limits = ValidationLimits {
        max_blocks_per_batch: validation_batch_size.unwrap_or(MAX_BLOCKS_IN_FLIGHT),
        pause_between_batches: Duration::from_millis(validation_pause_ms.unwrap_or(0)),
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
        rpc_auth_token_file,
        deployments,
        ibd_policy,
        snapshot,
        finalize_assumeutxo,
        validation_target,
        complete_assumeutxo,
        background_assumeutxo,
        cleanup_validation_dir,
        validation_limits,
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
        "rbtcd {}\n\nUSAGE:\n  rbtcd [--connect HOST:PORT ...] [--dns-seed HOST[:PORT] ... | --no-dns-seeds] [--network bitcoin|testnet|testnet4|signet|regtest]\n  rbtcd [PEER OPTIONS] --headers-db PATH [--network NETWORK] [--minimum-chainwork HEX] [--assumevalid HASH|0]\n  rbtcd [PEER OPTIONS] --data-dir PATH --network regtest|signet [--once] [--explorer-listen 127.0.0.1:3000 [--rpc-auth-token-file PATH] [--wallet-descriptors PATH --wallet-auth-token-file PATH]] [--vbparams taproot:START:END[:MIN_HEIGHT]] [--testactivationheight NAME@HEIGHT] [--signetchallenge HEX] [--signetseednode HOST[:PORT] ...] [--minimum-chainwork HEX] [--assumevalid HASH|0]\n  rbtcd [PEER OPTIONS] --data-dir ACTIVE --network regtest|signet --background-assumeutxo VALIDATION_DATA_DIR [--validation-batch-size N] [--validation-pause-ms MS] [--cleanup-validation-dir] [--once] [EXPLORER/RPC/WALLET OPTIONS]\n  rbtcd [PEER OPTIONS] --data-dir ACTIVE --network regtest|signet --complete-assumeutxo VALIDATION_DATA_DIR [--validation-batch-size N] [--validation-pause-ms MS] [--cleanup-validation-dir]\n  rbtcd [PEER OPTIONS] --data-dir PATH --network regtest|signet --validate-until-height HEIGHT --validate-until-blockhash HASH [--validation-batch-size N] [--validation-pause-ms MS]\n  rbtcd --data-dir PATH --network regtest|signet --assumeutxo-snapshot FILE --snapshot-height HEIGHT --snapshot-blockhash HASH --snapshot-utxo-count COUNT --snapshot-records-bytes BYTES --snapshot-records-sha256 HEX\n  rbtcd --data-dir PATH --network regtest|signet --finalize-assumeutxo VALIDATION_DATA_DIR\n  rbtcd [PEER OPTIONS] --fetch-block BLOCK_HASH [--network NETWORK]\n\nPEER OPTIONS:\n  Explicit --connect peers run first. If they and persisted verified peers fail, pinned Bitcoin Core 26 DNS seeds are used. Repeat --dns-seed to replace those defaults, or pass --no-dns-seeds. A custom Signet has no default seeds; repeat --signetseednode or --connect.",
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
    use proptest::prelude::*;
    use rbtc::{
        api::ExplorerIndex,
        blockchain::block_subsidy,
        chain_store::RedbChainStore,
        header_store::RedbHeaderStore,
        ledger::{LedgerRetention, PrunedBlockLedger},
        p2p::{PeerAddress, V1Transport},
        snapshot::export_snapshot,
        utxo::{OutPointKey, RedbUtxoStore, Utxo, UtxoStore},
        wallet::DEFAULT_WALLET_GAP_LIMIT,
    };
    use tempfile::TempDir;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tower::ServiceExt;

    const RECEIVE_DESCRIPTOR: &str = "wpkh([41f2aed0/84h/1h/0h]tpubDDFSdQWw75hk1ewbwnNpPp5DvXFRKt68ioPoyJDY752cNHKkFxPWqkqCyCf4hxrEfpuxh46QisehL3m8Bi6MsAv394QVLopwbtfvryFQNUH/0/*)#g0w0ymmw";
    const CHANGE_DESCRIPTOR: &str = "wpkh([41f2aed0/84h/1h/0h]tpubDDFSdQWw75hk1ewbwnNpPp5DvXFRKt68ioPoyJDY752cNHKkFxPWqkqCyCf4hxrEfpuxh46QisehL3m8Bi6MsAv394QVLopwbtfvryFQNUH/1/*)#emtwewtk";

    async fn http_request(address: SocketAddr, request: &[u8]) -> String {
        let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
        stream.write_all(request).await.unwrap();
        let mut response = Vec::new();
        stream.read_to_end(&mut response).await.unwrap();
        String::from_utf8(response).unwrap()
    }

    async fn http_stream_prefix(address: SocketAddr, request: &[u8], marker: &[u8]) -> String {
        let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
        stream.write_all(request).await.unwrap();
        let mut response = Vec::new();
        timeout(Duration::from_secs(2), async {
            while response.len() < 16 * 1024
                && !response
                    .windows(marker.len())
                    .any(|window| window == marker)
            {
                let mut buffer = [0_u8; 2048];
                let count = stream.read(&mut buffer).await.unwrap();
                if count == 0 {
                    break;
                }
                response.extend_from_slice(&buffer[..count]);
            }
        })
        .await
        .unwrap();
        String::from_utf8(response).unwrap()
    }

    fn ready_test_node_status(genesis: BlockHash) -> NodeStatus {
        let tip = NodeTipResponse {
            height: 0,
            hash: genesis.to_string(),
        };
        NodeStatus::new(NodeStatusProgress {
            network: Network::Regtest.to_string(),
            header: tip.clone(),
            execution: tip.clone(),
            explorer: tip.clone(),
            wallet: Some(tip),
            minimum_chainwork_reached: true,
            active_assume_valid_height: None,
            full_script_validation: true,
            independently_validated: true,
            utxo_hot: 1,
            utxo_cold: 2,
            ledger_segments: 0,
            ledger_blocks: 0,
            ledger_bytes: 0,
            ledger_first_height: None,
            ledger_tip_height: None,
        })
    }

    async fn assert_node_observability(address: SocketAddr) {
        let response = http_request(
            address,
            b"GET /api/v1/ready HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
        )
        .await;
        assert!(response.starts_with("HTTP/1.1 200 OK"));
        assert!(response.contains("cache-control: no-store"));
        let body = response.split("\r\n\r\n").nth(1).unwrap();
        let status: serde_json::Value = serde_json::from_str(body).unwrap();
        assert_eq!(status["phase"], "ready");
        assert_eq!(status["utxo_hot"], 1);
        assert_eq!(status["utxo_cold"], 2);
        assert_eq!(status["trust"]["independently_validated"], true);

        let response = http_request(
            address,
            b"GET /metrics HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
        )
        .await;
        assert!(response.starts_with("HTTP/1.1 200 OK"));
        assert!(response.contains("content-type: text/plain; version=0.0.4; charset=utf-8"));
        assert!(response.contains("cache-control: no-store"));
        assert!(response.contains("rbtc_ready 1"));
        assert!(response.contains("rbtc_utxos{tier=\"total\"} 3"));
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

    async fn serve_duplicate_version_peer(
        listener: TcpListener,
        first_nonce: u64,
        duplicate_nonce: u64,
    ) {
        let (stream, _) = listener.accept().await.unwrap();
        let mut peer = V1Transport::new(stream, Network::Regtest.magic());
        assert!(matches!(
            peer.read_message().await.unwrap().into_payload(),
            NetworkMessage::Version(_)
        ));
        peer.write_message(NetworkMessage::Version(peer_version(first_nonce)))
            .await
            .unwrap();
        receive_client_negotiation(&mut peer).await;
        peer.write_message(NetworkMessage::Version(peer_version(duplicate_nonce)))
            .await
            .unwrap();
    }

    async fn serve_concurrent_assumeutxo_peer(
        stream: tokio::net::TcpStream,
        version_nonce: u64,
        first: Block,
        second: Block,
    ) {
        let mut peer = V1Transport::new(stream, Network::Regtest.magic());
        assert!(matches!(
            peer.read_message().await.unwrap().into_payload(),
            NetworkMessage::Version(_)
        ));
        peer.write_message(NetworkMessage::Version(peer_version(version_nonce)))
            .await
            .unwrap();
        receive_client_negotiation(&mut peer).await;
        peer.write_message(NetworkMessage::Verack).await.unwrap();
        respond_to_getaddr(&mut peer).await;
        let NetworkMessage::GetHeaders(request) = peer.read_message().await.unwrap().into_payload()
        else {
            panic!("expected concurrent getheaders");
        };
        let active = request.locator_hashes.contains(&first.block_hash());
        let headers = if active {
            vec![second.header]
        } else {
            vec![first.header, second.header]
        };
        peer.write_message(NetworkMessage::Headers(headers))
            .await
            .unwrap();
        let expected_blocks = if active {
            vec![first, second]
        } else {
            vec![first]
        };
        for expected_block in expected_blocks {
            let NetworkMessage::GetData(inventory) =
                peer.read_message().await.unwrap().into_payload()
            else {
                panic!("expected concurrent block request");
            };
            assert_eq!(
                inventory,
                vec![bitcoin::p2p::message_blockdata::Inventory::WitnessBlock(
                    expected_block.block_hash()
                )]
            );
            peer.write_message(NetworkMessage::Block(expected_block))
                .await
                .unwrap();
        }
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
        regtest_block_at_height(parent, time, 1)
    }

    fn regtest_block_at_height(parent: BlockHash, time: u32, height: u32) -> Block {
        let coinbase = Transaction {
            version: TransactionVersion::ONE,
            lock_time: LockTime::ZERO,
            input: vec![TxIn {
                previous_output: OutPoint::null(),
                script_sig: bitcoin::script::Builder::new()
                    .push_int(i64::from(height))
                    .push_opcode(bitcoin::opcodes::all::OP_PUSHBYTES_0)
                    .into_script(),
                sequence: Sequence::MAX,
                witness: Witness::default(),
            }],
            output: vec![TxOut {
                value: Amount::from_sat(block_subsidy(height)),
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

    fn daemon_argument() -> impl Strategy<Value = String> {
        let known = prop::sample::select(
            [
                "--help",
                "--connect",
                "--dns-seed",
                "--no-dns-seeds",
                "--network",
                "--fetch-block",
                "--headers-db",
                "--data-dir",
                "--once",
                "--explorer-listen",
                "--wallet-descriptors",
                "--wallet-auth-token-file",
                "--rpc-auth-token-file",
                "--vbparams",
                "--testactivationheight",
                "--signetchallenge",
                "--signetseednode",
                "--minimum-chainwork",
                "--assumevalid",
                "--assume-valid",
                "--assumeutxo-snapshot",
                "--finalize-assumeutxo",
                "--validate-until-height",
                "--validate-until-blockhash",
                "--complete-assumeutxo",
                "--background-assumeutxo",
                "--cleanup-validation-dir",
                "--validation-batch-size",
                "--validation-pause-ms",
                "--snapshot-height",
                "--snapshot-blockhash",
                "--snapshot-utxo-count",
                "--snapshot-records-bytes",
                "--snapshot-records-sha256",
                "regtest",
                "signet",
                "bitcoin",
                "127.0.0.1:18444",
                "taproot:0:1:0",
                "segwit@0",
                "0",
                "1",
            ]
            .into_iter()
            .map(str::to_owned)
            .collect::<Vec<_>>(),
        );
        let arbitrary = prop::collection::vec(any::<u8>(), 0..96)
            .prop_map(|bytes| String::from_utf8_lossy(&bytes).into_owned());
        prop_oneof![4 => known, 1 => arbitrary]
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(512))]

        #[test]
        fn arbitrary_bounded_daemon_option_combinations_are_deterministic(
            arguments in prop::collection::vec(daemon_argument(), 0..80),
        ) {
            let first = parse_options(arguments.clone().into_iter())
                .map(|options| options.is_some());
            let second = parse_options(arguments.into_iter())
                .map(|options| options.is_some());
            prop_assert_eq!(first, second);
        }
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
                "--rpc-auth-token-file",
                "/tmp/rbtc-rpc.token",
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
        assert_eq!(
            served.rpc_auth_token_file,
            Some(PathBuf::from("/tmp/rbtc-rpc.token"))
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
                    "--rpc-auth-token-file",
                    "/tmp/rbtc-rpc.token",
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
    #[allow(clippy::too_many_lines)]
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
            rpc_auth_token_file: None,
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
            finalize_assumeutxo: None,
            validation_target: None,
            complete_assumeutxo: None,
            background_assumeutxo: None,
            cleanup_validation_dir: false,
            validation_limits: ValidationLimits::default(),
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
        let mut expected_utxo = reopened.get(outpoint).unwrap().unwrap();
        expected_utxo.last_touched = expected_utxo.last_touched.saturating_add(10_000);
        drop(reopened);

        let validation_dir = directory.path().join("validation");
        fs::create_dir(&validation_dir).unwrap();
        let validation =
            RedbChainStore::open(validation_dir.join("chainstate.redb"), Network::Regtest).unwrap();
        validation
            .execution()
            .bind_consensus_config(
                &DeploymentConfig::for_network(Network::Regtest).consensus_id(),
                &DeploymentConfig::for_network(Network::Regtest).consensus_id(),
            )
            .unwrap();
        validation
            .commit_connect(
                genesis.hash,
                rbtc::execution_store::ExecutionTip {
                    height: 1,
                    hash: info.hash,
                },
                &[],
                &[(outpoint, expected_utxo)],
                &[],
            )
            .unwrap();
        drop(validation);

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
            rpc_auth_token_file: None,
            deployments: DeploymentConfig::for_network(Network::Regtest),
            ibd_policy: IbdPolicy::for_network(Network::Regtest),
            snapshot: None,
            finalize_assumeutxo: Some(validation_dir),
            validation_target: None,
            complete_assumeutxo: None,
            background_assumeutxo: None,
            cleanup_validation_dir: false,
            validation_limits: ValidationLimits::default(),
        })
        .await
        .unwrap();

        let finalized =
            RedbChainStore::open(directory.path().join("chainstate.redb"), Network::Regtest)
                .unwrap();
        assert_eq!(finalized.execution().assumed_snapshot().unwrap(), None);
        assert_eq!(finalized.get(outpoint).unwrap().unwrap().value_sats, 123);
    }

    #[test]
    fn parses_only_offline_assumeutxo_finalization() {
        let options = parse_options(
            [
                "--network",
                "regtest",
                "--data-dir",
                "/tmp/rbtc-active",
                "--finalize-assumeutxo",
                "/tmp/rbtc-validation",
            ]
            .into_iter()
            .map(str::to_owned),
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            options.finalize_assumeutxo,
            Some(PathBuf::from("/tmp/rbtc-validation"))
        );
        for arguments in [
            vec!["--network", "regtest", "--finalize-assumeutxo", "/tmp/v"],
            vec![
                "--network",
                "regtest",
                "--data-dir",
                "/tmp/a",
                "--finalize-assumeutxo",
                "/tmp/v",
                "--connect",
                "127.0.0.1:18444",
            ],
            vec![
                "--network",
                "regtest",
                "--data-dir",
                "/tmp/a",
                "--finalize-assumeutxo",
                "/tmp/v",
                "--once",
            ],
        ] {
            assert!(parse_options(arguments.into_iter().map(str::to_owned)).is_err());
        }
    }

    #[test]
    fn parses_only_complete_bounded_genesis_validation_target() {
        let hash = BlockHash::from_byte_array([42; 32]);
        let options = parse_options(
            [
                "--network".to_owned(),
                "regtest".to_owned(),
                "--data-dir".to_owned(),
                "/tmp/rbtc-validation".to_owned(),
                "--connect".to_owned(),
                "127.0.0.1:18444".to_owned(),
                "--validate-until-height".to_owned(),
                "100".to_owned(),
                "--validate-until-blockhash".to_owned(),
                hash.to_string(),
            ]
            .into_iter(),
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            options.validation_target,
            Some(ValidationTarget {
                height: 100,
                block_hash: hash,
            })
        );
        let hash_text = hash.to_string();
        for arguments in [
            vec![
                "--network",
                "regtest",
                "--data-dir",
                "/tmp/v",
                "--validate-until-height",
                "1",
            ],
            vec![
                "--network",
                "regtest",
                "--data-dir",
                "/tmp/v",
                "--validate-until-height",
                "0",
                "--validate-until-blockhash",
                hash_text.as_str(),
            ],
            vec![
                "--network",
                "regtest",
                "--validate-until-height",
                "1",
                "--validate-until-blockhash",
                hash_text.as_str(),
            ],
        ] {
            assert!(parse_options(arguments.into_iter().map(str::to_owned)).is_err());
        }
    }

    #[test]
    fn parses_automatic_assumeutxo_completion_as_a_network_mode() {
        let options = parse_options(
            [
                "--network",
                "regtest",
                "--data-dir",
                "/tmp/rbtc-active",
                "--complete-assumeutxo",
                "/tmp/rbtc-validation",
                "--connect",
                "127.0.0.1:18444",
                "--validation-batch-size",
                "3",
                "--validation-pause-ms",
                "7",
            ]
            .into_iter()
            .map(str::to_owned),
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            options.complete_assumeutxo,
            Some(PathBuf::from("/tmp/rbtc-validation"))
        );
        assert_eq!(
            options.validation_limits,
            ValidationLimits {
                max_blocks_per_batch: 3,
                pause_between_batches: Duration::from_millis(7),
            }
        );
        for arguments in [
            vec!["--network", "regtest", "--complete-assumeutxo", "/tmp/v"],
            vec![
                "--network",
                "regtest",
                "--data-dir",
                "/tmp/a",
                "--complete-assumeutxo",
                "/tmp/v",
                "--once",
            ],
            vec![
                "--network",
                "regtest",
                "--data-dir",
                "/tmp/a",
                "--complete-assumeutxo",
                "/tmp/v",
                "--explorer-listen",
                "127.0.0.1:3000",
            ],
            vec![
                "--network",
                "regtest",
                "--data-dir",
                "/tmp/a",
                "--validation-batch-size",
                "1",
            ],
            vec![
                "--network",
                "regtest",
                "--data-dir",
                "/tmp/a",
                "--complete-assumeutxo",
                "/tmp/v",
                "--validation-pause-ms",
                "60001",
            ],
        ] {
            assert!(parse_options(arguments.into_iter().map(str::to_owned)).is_err());
        }
    }

    #[test]
    fn parses_concurrent_assumeutxo_service_with_active_node_options() {
        let options = parse_options(
            [
                "--network",
                "regtest",
                "--data-dir",
                "/tmp/rbtc-active",
                "--background-assumeutxo",
                "/tmp/rbtc-validation",
                "--connect",
                "127.0.0.1:18444",
                "--once",
                "--explorer-listen",
                "127.0.0.1:3000",
                "--validation-batch-size",
                "2",
                "--validation-pause-ms",
                "5",
                "--cleanup-validation-dir",
            ]
            .into_iter()
            .map(str::to_owned),
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            options.background_assumeutxo,
            Some(PathBuf::from("/tmp/rbtc-validation"))
        );
        assert!(options.once);
        assert!(options.cleanup_validation_dir);
        assert_eq!(
            options.explorer_listen,
            Some("127.0.0.1:3000".parse().unwrap())
        );
        assert_eq!(
            options.validation_limits,
            ValidationLimits {
                max_blocks_per_batch: 2,
                pause_between_batches: Duration::from_millis(5),
            }
        );
        for arguments in [
            vec!["--network", "regtest", "--background-assumeutxo", "/tmp/v"],
            vec![
                "--network",
                "regtest",
                "--data-dir",
                "/tmp/a",
                "--background-assumeutxo",
                "/tmp/v",
                "--complete-assumeutxo",
                "/tmp/v2",
            ],
            vec![
                "--network",
                "regtest",
                "--data-dir",
                "/tmp/a",
                "--cleanup-validation-dir",
            ],
        ] {
            assert!(parse_options(arguments.into_iter().map(str::to_owned)).is_err());
        }
    }

    #[test]
    fn background_validation_failure_is_propagated_to_active_service() {
        let directory = TempDir::new().unwrap();
        let chainstate =
            RedbChainStore::open(directory.path().join("chainstate.redb"), Network::Regtest)
                .unwrap();
        let headers = HeaderDag::new(Network::Regtest);
        let status = BackgroundValidationStatus::new(
            directory.path().join("validation"),
            ValidationTarget {
                height: 1,
                block_hash: headers.active_tip().hash,
            },
            false,
        );
        status.set(BackgroundValidationState::Failed(
            "validator stopped".to_owned(),
        ));
        let error = poll_background_validation(
            Some(&status),
            Network::Regtest,
            &DeploymentConfig::for_network(Network::Regtest),
            &chainstate,
            directory.path(),
            &headers,
        )
        .unwrap_err();
        assert_eq!(error.kind, PeerFailureKind::Transient);
        assert!(error.message.contains("validator stopped"));
    }

    #[test]
    fn adaptive_validation_yields_until_active_execution_catches_up() {
        let target = ValidationTarget {
            height: 100,
            block_hash: BlockHash::from_byte_array([5; 32]),
        };
        let status = BackgroundValidationStatus::new(PathBuf::from("validation"), target, false);
        let configured = ValidationLimits {
            max_blocks_per_batch: 8,
            pause_between_batches: Duration::from_millis(5),
        };
        assert_eq!(
            status.adaptive_limits(configured),
            ValidationLimits {
                max_blocks_per_batch: 1,
                pause_between_batches: ADAPTIVE_VALIDATION_BUSY_PAUSE,
            }
        );
        status.update_active(40, 50);
        assert_eq!(status.adaptive_limits(configured).max_blocks_per_batch, 1);
        status.update_active(50, 50);
        assert_eq!(status.adaptive_limits(configured), configured);
        status.update_validation(25);
        let response = status.response();
        assert_eq!(response.validation_tip, 25);
        assert_eq!(response.validation_remaining, 75);
        assert!(!response.adaptive_throttled);
    }

    #[test]
    fn node_readiness_requires_work_and_projection_consistency() {
        let hash = BlockHash::from_byte_array([9; 32]).to_string();
        let tip = NodeTipResponse {
            height: 10,
            hash: hash.clone(),
        };
        let mut progress = NodeStatusProgress {
            network: Network::Regtest.to_string(),
            header: tip.clone(),
            execution: tip.clone(),
            explorer: tip.clone(),
            wallet: Some(tip.clone()),
            minimum_chainwork_reached: true,
            active_assume_valid_height: None,
            full_script_validation: true,
            independently_validated: true,
            utxo_hot: 4,
            utxo_cold: 6,
            ledger_segments: 1,
            ledger_blocks: 10,
            ledger_bytes: 1_024,
            ledger_first_height: Some(1),
            ledger_tip_height: Some(10),
        };
        let status = NodeStatus::new(progress.clone());
        assert!(status.response().ready);
        assert_eq!(status.response().phase, "ready");

        progress.header.height = 11;
        status.update(progress.clone());
        assert!(!status.response().ready);
        assert_eq!(status.response().phase, "syncing_blocks");

        progress.execution = progress.header.clone();
        status.update(progress.clone());
        assert_eq!(status.response().phase, "reconciling");

        progress.explorer = progress.execution.clone();
        progress.wallet = Some(progress.execution.clone());
        progress.independently_validated = false;
        status.update(progress.clone());
        assert!(status.response().ready);
        assert_eq!(status.response().phase, "assumed_ready");

        progress.minimum_chainwork_reached = false;
        status.update(progress);
        assert!(!status.response().ready);
        assert_eq!(status.response().phase, "ibd");
    }

    #[tokio::test]
    async fn readiness_route_returns_service_unavailable_during_ibd() {
        let genesis = bitcoin::blockdata::constants::genesis_block(Network::Regtest).block_hash();
        let status = ready_test_node_status(genesis);
        let mut progress = status
            .progress
            .lock()
            .expect("node status lock not poisoned")
            .clone();
        progress.minimum_chainwork_reached = false;
        status.update(progress);
        let response = node_status_router(status)
            .oneshot(
                axum::http::Request::get("/api/v1/ready")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            axum::http::StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!(
            response.headers()[axum::http::header::CACHE_CONTROL],
            "no-store"
        );
        let body = axum::body::to_bytes(response.into_body(), 16 * 1024)
            .await
            .unwrap();
        let status: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(status["phase"], "ibd");
        assert_eq!(status["ready"], false);
    }

    #[test]
    fn validation_cleanup_refuses_unknown_files_without_renaming_directory() {
        let directory = TempDir::new().unwrap();
        let active_dir = directory.path().join("active");
        let validation_dir = directory.path().join("validation");
        fs::create_dir(&active_dir).unwrap();
        fs::create_dir(&validation_dir).unwrap();
        let target = ValidationTarget {
            height: 1,
            block_hash: BlockHash::from_byte_array([6; 32]),
        };
        ensure_validation_directory_owner(
            &validation_dir,
            &ValidationDirectoryOwner::new(Network::Regtest, target.height, target.block_hash),
            true,
            true,
        )
        .unwrap();
        let validation =
            RedbChainStore::open(validation_dir.join("chainstate.redb"), Network::Regtest).unwrap();
        let genesis = validation.execution().tip().unwrap();
        validation
            .execution()
            .bind_validation_target(rbtc::execution_store::ExecutionTip {
                height: target.height,
                hash: target.block_hash,
            })
            .unwrap();
        validation
            .execution()
            .advance(
                genesis.hash,
                rbtc::execution_store::ExecutionTip {
                    height: target.height,
                    hash: target.block_hash,
                },
            )
            .unwrap();
        drop(validation);
        let unknown = validation_dir.join("user-notes.txt");
        fs::write(&unknown, b"keep me").unwrap();
        let error = cleanup_completed_validation_dir(
            &active_dir,
            &validation_dir,
            Network::Regtest,
            target,
        )
        .unwrap_err();
        assert!(error.contains("not an rBTC validation artifact"));
        assert!(validation_dir.is_dir());
        assert_eq!(fs::read(unknown).unwrap(), b"keep me");
    }

    #[test]
    fn owner_marker_directory_sync_failure_preserves_a_reusable_marker() {
        let directory = TempDir::new().unwrap();
        let validation_dir = directory.path().join("validation");
        fs::create_dir(&validation_dir).unwrap();
        let owner = ValidationDirectoryOwner::new(
            Network::Regtest,
            42,
            BlockHash::from_byte_array([9; 32]),
        );
        let error = ensure_validation_directory_owner_with_sync(
            &validation_dir,
            &owner,
            true,
            false,
            |_| Err(io::Error::other("injected owner directory sync failure")),
        )
        .unwrap_err();
        assert!(error.contains("injected owner directory sync failure"));

        let marker = fs::read(validation_dir.join(VALIDATION_OWNER_FILE)).unwrap();
        assert_eq!(parse_validation_directory_owner(&marker).unwrap(), owner);
        ensure_validation_directory_owner_with_sync(&validation_dir, &owner, true, false, |_| {
            panic!("an existing owner marker must not rewrite or resync")
        })
        .unwrap();
    }

    #[test]
    fn cleanup_parent_sync_failure_rolls_back_quarantine_before_removal() {
        let directory = TempDir::new().unwrap();
        let validation_dir = directory.path().join("validation");
        fs::create_dir(&validation_dir).unwrap();
        fs::write(validation_dir.join("chainstate.redb"), b"preserve").unwrap();
        let calls = std::cell::Cell::new(0);
        let error = quarantine_and_remove_validation_dir_with_sync(&validation_dir, |_| {
            let call = calls.get();
            calls.set(call + 1);
            if call == 0 {
                Err(io::Error::other("injected quarantine sync failure"))
            } else {
                Ok(())
            }
        })
        .unwrap_err();
        assert!(error.contains("quarantine was rolled back"));
        assert_eq!(
            fs::read(validation_dir.join("chainstate.redb")).unwrap(),
            b"preserve"
        );
        assert_eq!(calls.get(), 2);
        assert!(fs::read_dir(directory.path()).unwrap().all(|entry| {
            !entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .contains("rbtc-cleanup")
        }));
    }

    #[test]
    fn cleanup_reports_final_parent_sync_failure_after_completed_removal() {
        let directory = TempDir::new().unwrap();
        let validation_dir = directory.path().join("validation");
        fs::create_dir(&validation_dir).unwrap();
        fs::write(validation_dir.join("chainstate.redb"), b"remove").unwrap();
        let calls = std::cell::Cell::new(0);
        let error = quarantine_and_remove_validation_dir_with_sync(&validation_dir, |_| {
            let call = calls.get();
            calls.set(call + 1);
            if call == 1 {
                Err(io::Error::other("injected removal sync failure"))
            } else {
                Ok(())
            }
        })
        .unwrap_err();
        assert!(error.contains("injected removal sync failure"));
        assert!(!validation_dir.exists());
        assert_eq!(calls.get(), 2);
        assert!(fs::read_dir(directory.path()).unwrap().next().is_none());
    }

    #[test]
    fn validation_cleanup_refuses_nested_data_directories() {
        let directory = TempDir::new().unwrap();
        let active_dir = directory.path().join("active");
        let validation_dir = active_dir.join("validation");
        fs::create_dir_all(&validation_dir).unwrap();
        let error = cleanup_completed_validation_dir(
            &active_dir,
            &validation_dir,
            Network::Regtest,
            ValidationTarget {
                height: 1,
                block_hash: BlockHash::from_byte_array([7; 32]),
            },
        )
        .unwrap_err();
        assert!(error.contains("contained by the active data directory"));
        assert!(validation_dir.is_dir());
    }

    #[cfg(unix)]
    #[test]
    fn validation_cleanup_refuses_directory_symlink() {
        use std::os::unix::fs::symlink;

        let directory = TempDir::new().unwrap();
        let active_dir = directory.path().join("active");
        let validation_target = directory.path().join("validation-target");
        let validation_link = directory.path().join("validation-link");
        fs::create_dir(&active_dir).unwrap();
        fs::create_dir(&validation_target).unwrap();
        symlink(&validation_target, &validation_link).unwrap();
        let error = cleanup_completed_validation_dir(
            &active_dir,
            &validation_link,
            Network::Regtest,
            ValidationTarget {
                height: 1,
                block_hash: BlockHash::from_byte_array([8; 32]),
            },
        )
        .unwrap_err();
        assert!(error.contains("must not be a symbolic link"));
        assert!(validation_target.is_dir());
        assert!(validation_link.exists());
    }

    #[cfg(unix)]
    #[test]
    fn same_file_rejects_symlink_and_hardlink_aliases() {
        let directory = TempDir::new().unwrap();
        let original = directory.path().join("original");
        let hardlink = directory.path().join("hardlink");
        let symlink = directory.path().join("symlink");
        fs::write(&original, b"chainstate").unwrap();
        fs::hard_link(&original, &hardlink).unwrap();
        std::os::unix::fs::symlink(&original, &symlink).unwrap();
        assert!(same_file(&original, &hardlink).unwrap());
        assert!(same_file(&original, &symlink).unwrap());
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
            rpc_auth_token_file: None,
            deployments: DeploymentConfig::for_network(Network::Regtest),
            ibd_policy: IbdPolicy::for_network(Network::Regtest),
            snapshot: None,
            finalize_assumeutxo: None,
            validation_target: None,
            complete_assumeutxo: None,
            background_assumeutxo: None,
            cleanup_validation_dir: false,
            validation_limits: ValidationLimits::default(),
        })
        .await
        .unwrap_err();

        assert!(error.contains("legacy split chain-state file"));
        assert!(!directory.path().join("peers.redb").exists());
        assert!(!directory.path().join("wallet.sqlite").exists());
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn wallet_runtime_loads_only_owner_only_bounded_configuration_files() {
        let directory = TempDir::new().unwrap();
        let descriptors = directory.path().join("wallet.json");
        let auth_token = directory.path().join("wallet.token");
        let rpc_token = directory.path().join("rpc.token");
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
        write_owner_only(&rpc_token, &format!("{}\n", "b".repeat(32)));
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
            rpc_auth_token_file: Some(rpc_token.clone()),
            deployments: DeploymentConfig::for_network(Network::Regtest),
            ibd_policy: IbdPolicy::for_network(Network::Regtest),
            snapshot: None,
            finalize_assumeutxo: None,
            validation_target: None,
            complete_assumeutxo: None,
            background_assumeutxo: None,
            cleanup_validation_dir: false,
            validation_limits: ValidationLimits::default(),
        };

        let runtime = prepare_api_runtime(&options).unwrap().unwrap();
        let wallet = runtime.wallet.as_ref().unwrap();
        assert_eq!(wallet.scan.gap_limit, 42);
        assert_eq!(wallet.scan.birthday_height, 100);
        assert!(
            runtime
                .rpc
                .as_ref()
                .unwrap()
                .token
                .authorizes(format!("Bearer {}", "b".repeat(32)).as_bytes())
        );
        assert!(directory.path().join("wallet.sqlite").exists());
        assert!(directory.path().join(API_AUDIT_FILE).exists());
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                fs::metadata(directory.path().join(API_AUDIT_FILE))
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o777,
                0o600
            );
        }
        drop(runtime);

        let mut aliased = options.clone();
        aliased.rpc_auth_token_file = Some(auth_token.clone());
        let error = prepare_api_runtime(&aliased).err().unwrap();
        assert!(error.contains("must be distinct"));

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

            fs::set_permissions(&auth_token, fs::Permissions::from_mode(0o600)).unwrap();
            fs::set_permissions(&rpc_token, fs::Permissions::from_mode(0o644)).unwrap();
            let error = prepare_api_runtime(&options).err().unwrap();
            assert!(error.contains("permissions"));
            assert!(!error.contains(&"b".repeat(32)));
        }
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn embedded_api_serves_explorer_and_authenticated_watch_only_wallet() {
        let directory = TempDir::new().unwrap();
        let index = Arc::new(
            RedbExplorerIndex::open(directory.path().join("explorer.redb"), Network::Regtest)
                .unwrap(),
        );
        let token_text = "a".repeat(32);
        let token_path = directory.path().join("wallet.token");
        write_owner_only(&token_path, &token_text);
        let rpc_token_text = "c".repeat(32);
        let rpc_token_path = directory.path().join("rpc.token");
        write_owner_only(&rpc_token_path, &rpc_token_text);
        let audit = AuthorizationAuditLog::open(directory.path().join(API_AUDIT_FILE)).unwrap();
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
            token: LocalAuthToken::new(&token_text).unwrap(),
            token_path: token_path.clone(),
            audit: audit.clone(),
            scan: WalletScanConfig {
                gap_limit: DEFAULT_WALLET_GAP_LIMIT,
                birthday_height: 0,
            },
        };
        let rpc = RpcApiRuntime {
            token: LocalAuthToken::new(&rpc_token_text).unwrap(),
            token_path: rpc_token_path.clone(),
            audit,
        };
        let genesis = bitcoin::blockdata::constants::genesis_block(Network::Regtest).block_hash();
        let validation = BackgroundValidationStatus::new(
            directory.path().join("validation"),
            ValidationTarget {
                height: 10,
                block_hash: genesis,
            },
            false,
        );
        validation.update_active(12, 12);
        validation.update_validation(4);
        let node_status = ready_test_node_status(genesis);
        let server = ApiServer::bind(
            "127.0.0.1:0".parse().unwrap(),
            index,
            ExplorerEventHub::new(0, genesis.to_string()),
            node_status,
            Some(&wallet),
            Some(&rpc),
            Some(&validation),
        )
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

        let event_marker = genesis.to_string();
        let response = http_stream_prefix(
            server.address(),
            b"GET /api/v1/events HTTP/1.1\r\nHost: localhost\r\n\r\n",
            event_marker.as_bytes(),
        )
        .await;
        assert!(response.starts_with("HTTP/1.1 200 OK"));
        assert!(response.contains("content-type: text/event-stream"));
        assert!(response.contains("event: tip"));
        assert!(response.contains(r#""sequence":0"#));
        assert!(response.contains(r#""kind":"snapshot""#));
        assert!(response.contains(&event_marker));

        assert_node_observability(server.address()).await;

        let response = http_request(
            server.address(),
            b"GET /api/v1/validation HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
        )
        .await;
        assert!(response.starts_with("HTTP/1.1 200 OK"));
        let body = response.split("\r\n\r\n").nth(1).unwrap();
        let status: serde_json::Value = serde_json::from_str(body).unwrap();
        assert_eq!(status["phase"], "validating");
        assert_eq!(status["target_height"], 10);
        assert_eq!(status["validation_tip"], 4);
        assert_eq!(status["validation_remaining"], 6);

        let rpc_body = r#"{"jsonrpc":"2.0","id":7,"method":"help","params":[]}"#;
        let wrong_rpc_scope = http_request(
            server.address(),
            format!(
                "POST /rpc HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer {token_text}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{rpc_body}",
                rpc_body.len()
            )
            .as_bytes(),
        )
        .await;
        assert!(wrong_rpc_scope.starts_with("HTTP/1.1 401 Unauthorized"));
        let rpc_response = http_request(
            server.address(),
            format!(
                "POST /rpc HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer {rpc_token_text}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{rpc_body}",
                rpc_body.len()
            )
            .as_bytes(),
        )
        .await;
        assert!(rpc_response.starts_with("HTTP/1.1 200 OK"));
        let rpc_result: serde_json::Value =
            serde_json::from_str(rpc_response.split("\r\n\r\n").nth(1).unwrap()).unwrap();
        assert_eq!(rpc_result["id"], 7);
        assert!(
            rpc_result["result"]
                .as_array()
                .unwrap()
                .contains(&serde_json::json!("getblockhash"))
        );

        let rotated_rpc_text = "d".repeat(32);
        let replacement = directory.path().join("rpc.token.next");
        write_owner_only(&replacement, &rotated_rpc_text);
        fs::rename(replacement, &rpc_token_path).unwrap();
        tokio::time::sleep(LOCAL_AUTH_TOKEN_RELOAD_INTERVAL * 2).await;
        let old_rpc = http_request(
            server.address(),
            format!(
                "POST /rpc HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer {rpc_token_text}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{rpc_body}",
                rpc_body.len()
            )
            .as_bytes(),
        )
        .await;
        assert!(old_rpc.starts_with("HTTP/1.1 401 Unauthorized"));
        let rotated_rpc = http_request(
            server.address(),
            format!(
                "POST /rpc HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer {rotated_rpc_text}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{rpc_body}",
                rpc_body.len()
            )
            .as_bytes(),
        )
        .await;
        assert!(rotated_rpc.starts_with("HTTP/1.1 200 OK"));

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

        let rotated_text = "b".repeat(32);
        let replacement = directory.path().join("wallet.token.next");
        write_owner_only(&replacement, &rotated_text);
        fs::rename(replacement, &token_path).unwrap();
        tokio::time::sleep(LOCAL_AUTH_TOKEN_RELOAD_INTERVAL * 2).await;
        let old = http_request(
            server.address(),
            format!(
                "GET /api/v1/wallet/balance HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer {token_text}\r\nConnection: close\r\n\r\n"
            )
            .as_bytes(),
        )
        .await;
        assert!(old.starts_with("HTTP/1.1 401 Unauthorized"));
        let rotated = http_request(
            server.address(),
            format!(
                "GET /api/v1/wallet/balance HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer {rotated_text}\r\nConnection: close\r\n\r\n"
            )
            .as_bytes(),
        )
        .await;
        assert!(rotated.starts_with("HTTP/1.1 200 OK"));

        write_owner_only(&token_path, "invalid");
        tokio::time::sleep(LOCAL_AUTH_TOKEN_RELOAD_INTERVAL * 2).await;
        let disabled = http_request(
            server.address(),
            format!(
                "GET /api/v1/wallet/balance HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer {rotated_text}\r\nConnection: close\r\n\r\n"
            )
            .as_bytes(),
        )
        .await;
        assert!(disabled.starts_with("HTTP/1.1 401 Unauthorized"));

        let audit = fs::read_to_string(directory.path().join(API_AUDIT_FILE)).unwrap();
        assert!(audit.contains(r#""authorization":"accepted""#));
        assert!(audit.contains(r#""authorization":"rejected""#));
        assert!(!audit.contains(&token_text));
        assert!(!audit.contains(&rotated_text));
        assert!(!audit.contains(&rpc_token_text));
        assert!(!audit.contains(&rotated_rpc_text));
        assert!(!audit.contains(r#""method":"help""#));
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
            rpc_auth_token_file: None,
            deployments: DeploymentConfig::for_network(Network::Regtest),
            ibd_policy: IbdPolicy::for_network(Network::Regtest),
            snapshot: None,
            finalize_assumeutxo: None,
            validation_target: None,
            complete_assumeutxo: None,
            background_assumeutxo: None,
            cleanup_validation_dir: false,
            validation_limits: ValidationLimits::default(),
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
    async fn bounded_genesis_validation_never_downloads_or_commits_above_target() {
        let genesis = bitcoin::blockdata::constants::genesis_block(Network::Regtest).header;
        let first = regtest_block(genesis.block_hash(), genesis.time + 1);
        let first_hash = first.block_hash();
        let mut second = regtest_block(first_hash, first.header.time + 1);
        second.txdata[0].input[0].script_sig = ScriptBuf::from_bytes(vec![0x52, 0x00]);
        second.header.merkle_root = second.compute_merkle_root().unwrap();
        second.header.nonce = 0;
        while second
            .header
            .validate_pow(Target::MAX_ATTAINABLE_REGTEST)
            .is_err()
        {
            second.header.nonce = second.header.nonce.checked_add(1).unwrap();
        }
        let second_hash = second.block_hash();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let remote = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut peer, _) = accept_peer(listener, peer_version(61)).await;
            respond_to_getaddr(&mut peer).await;
            match peer.read_message().await.unwrap().into_payload() {
                NetworkMessage::GetHeaders(_) => peer
                    .write_message(NetworkMessage::Headers(vec![first.header, second.header]))
                    .await
                    .unwrap(),
                message => panic!("expected bounded-validation getheaders, got {message:?}"),
            }
            match peer.read_message().await.unwrap().into_payload() {
                NetworkMessage::GetData(inventory) => {
                    assert_eq!(
                        inventory,
                        vec![bitcoin::p2p::message_blockdata::Inventory::WitnessBlock(
                            first_hash
                        )]
                    );
                    peer.write_message(NetworkMessage::Block(first))
                        .await
                        .unwrap();
                }
                message => panic!("expected one bounded-validation block, got {message:?}"),
            }
        });
        let directory = TempDir::new().unwrap();
        run(Options {
            remotes: vec![remote],
            dns_seeds: Some(Vec::new()),
            network: Network::Regtest,
            fetch_block: None,
            headers_db: None,
            data_dir: Some(directory.path().to_path_buf()),
            once: false,
            explorer_listen: None,
            wallet_api_files: None,
            rpc_auth_token_file: None,
            deployments: DeploymentConfig::for_network(Network::Regtest),
            ibd_policy: IbdPolicy::for_network(Network::Regtest),
            snapshot: None,
            finalize_assumeutxo: None,
            validation_target: Some(ValidationTarget {
                height: 1,
                block_hash: first_hash,
            }),
            complete_assumeutxo: None,
            background_assumeutxo: None,
            cleanup_validation_dir: false,
            validation_limits: ValidationLimits::default(),
        })
        .await
        .unwrap();
        server.await.unwrap();

        let chainstate =
            RedbChainStore::open(directory.path().join("chainstate.redb"), Network::Regtest)
                .unwrap();
        assert_eq!(chainstate.execution().tip().unwrap().height, 1);
        assert_eq!(chainstate.execution().tip().unwrap().hash, first_hash);
        assert_eq!(chainstate.execution().assumed_snapshot().unwrap(), None);
        assert_eq!(
            chainstate.execution().validation_target().unwrap(),
            Some(rbtc::execution_store::ExecutionTip {
                height: 1,
                hash: first_hash,
            })
        );
        drop(chainstate);
        let headers = RedbHeaderStore::open(directory.path().join("headers.redb"))
            .unwrap()
            .load_dag(Network::Regtest, unix_time().unwrap())
            .unwrap();
        assert_eq!(headers.active_tip().height, 2);
        assert_eq!(headers.active_tip().hash, second_hash);
        let ledger =
            PrunedBlockLedger::open(directory.path().join("blocks"), LedgerRetention::default())
                .unwrap();
        assert_eq!(ledger.retained_ranges().unwrap(), vec![(1, 1)]);
        drop(ledger);

        let restart_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let restart_remote = restart_listener.local_addr().unwrap();
        let restart_server = tokio::spawn(async move {
            let (mut peer, _) = accept_peer(restart_listener, peer_version(62)).await;
            respond_to_getaddr(&mut peer).await;
            match peer.read_message().await.unwrap().into_payload() {
                NetworkMessage::GetHeaders(_) => peer
                    .write_message(NetworkMessage::Headers(Vec::new()))
                    .await
                    .unwrap(),
                message => panic!("expected restart getheaders, got {message:?}"),
            }
        });
        run(Options {
            remotes: vec![restart_remote],
            dns_seeds: Some(Vec::new()),
            network: Network::Regtest,
            fetch_block: None,
            headers_db: None,
            data_dir: Some(directory.path().to_path_buf()),
            once: false,
            explorer_listen: None,
            wallet_api_files: None,
            rpc_auth_token_file: None,
            deployments: DeploymentConfig::for_network(Network::Regtest),
            ibd_policy: IbdPolicy::for_network(Network::Regtest),
            snapshot: None,
            finalize_assumeutxo: None,
            validation_target: None,
            complete_assumeutxo: None,
            background_assumeutxo: None,
            cleanup_validation_dir: false,
            validation_limits: ValidationLimits::default(),
        })
        .await
        .unwrap();
        restart_server.await.unwrap();
        let reopened =
            RedbChainStore::open(directory.path().join("chainstate.redb"), Network::Regtest)
                .unwrap();
        assert_eq!(reopened.execution().tip().unwrap().height, 1);
        assert_eq!(reopened.execution().tip().unwrap().hash, first_hash);
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn automatic_assumeutxo_completion_syncs_proves_and_clears_marker() {
        let directory = TempDir::new().unwrap();
        let active_dir = directory.path().join("active");
        fs::create_dir(&active_dir).unwrap();
        let validation_dir = directory.path().join("validation");
        let genesis = bitcoin::blockdata::constants::genesis_block(Network::Regtest).header;
        let block = regtest_block(genesis.block_hash(), genesis.time + 1);
        let block_hash = block.block_hash();
        RedbHeaderStore::open(active_dir.join("headers.redb"))
            .unwrap()
            .append(block.header)
            .unwrap();
        let outpoint = OutPointKey::from(OutPoint::new(block.txdata[0].compute_txid(), 0));
        let source = RedbUtxoStore::open(directory.path().join("source.redb")).unwrap();
        source
            .apply(
                &[],
                &[(
                    outpoint,
                    Utxo {
                        value_sats: block_subsidy(1),
                        height: 1,
                        is_coinbase: true,
                        last_touched: 0,
                        creation_mtp: genesis.time,
                        script_pubkey: Vec::new(),
                    },
                )],
            )
            .unwrap();
        let snapshot_path = directory.path().join("base.rbtc");
        let manifest = export_snapshot(
            &source,
            &snapshot_path,
            "regtest",
            1,
            block_hash.to_string(),
        )
        .unwrap();
        drop(source);
        run(Options {
            remotes: Vec::new(),
            dns_seeds: None,
            network: Network::Regtest,
            fetch_block: None,
            headers_db: None,
            data_dir: Some(active_dir.clone()),
            once: false,
            explorer_listen: None,
            wallet_api_files: None,
            rpc_auth_token_file: None,
            deployments: DeploymentConfig::for_network(Network::Regtest),
            ibd_policy: IbdPolicy::for_network(Network::Regtest),
            snapshot: Some(SnapshotActivationOptions {
                path: snapshot_path,
                height: 1,
                block_hash,
                utxo_count: manifest.utxo_count,
                records_bytes: manifest.records_bytes,
                records_sha256: manifest.records_sha256,
            }),
            finalize_assumeutxo: None,
            validation_target: None,
            complete_assumeutxo: None,
            background_assumeutxo: None,
            cleanup_validation_dir: false,
            validation_limits: ValidationLimits::default(),
        })
        .await
        .unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let remote = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut peer, _) = accept_peer(listener, peer_version(63)).await;
            respond_to_getaddr(&mut peer).await;
            match peer.read_message().await.unwrap().into_payload() {
                NetworkMessage::GetHeaders(_) => peer
                    .write_message(NetworkMessage::Headers(vec![block.header]))
                    .await
                    .unwrap(),
                message => panic!("expected automatic-validation getheaders, got {message:?}"),
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
                message => panic!("expected automatic-validation block, got {message:?}"),
            }
        });
        run(Options {
            remotes: vec![remote],
            dns_seeds: Some(Vec::new()),
            network: Network::Regtest,
            fetch_block: None,
            headers_db: None,
            data_dir: Some(active_dir.clone()),
            once: false,
            explorer_listen: None,
            wallet_api_files: None,
            rpc_auth_token_file: None,
            deployments: DeploymentConfig::for_network(Network::Regtest),
            ibd_policy: IbdPolicy::for_network(Network::Regtest),
            snapshot: None,
            finalize_assumeutxo: None,
            validation_target: None,
            complete_assumeutxo: Some(validation_dir.clone()),
            background_assumeutxo: None,
            cleanup_validation_dir: false,
            validation_limits: ValidationLimits::default(),
        })
        .await
        .unwrap();
        server.await.unwrap();

        let active =
            RedbChainStore::open(active_dir.join("chainstate.redb"), Network::Regtest).unwrap();
        assert_eq!(active.execution().assumed_snapshot().unwrap(), None);
        assert_eq!(active.execution().tip().unwrap().hash, block_hash);
        assert_eq!(
            active.get(outpoint).unwrap().unwrap().value_sats,
            block_subsidy(1)
        );
        let validation =
            RedbChainStore::open(validation_dir.join("chainstate.redb"), Network::Regtest).unwrap();
        assert_eq!(validation.execution().tip().unwrap().height, 1);
        assert_eq!(validation.execution().tip().unwrap().hash, block_hash);
        assert_eq!(validation.execution().assumed_snapshot().unwrap(), None);
        assert_eq!(
            validation.execution().validation_target().unwrap(),
            Some(rbtc::execution_store::ExecutionTip {
                height: 1,
                hash: block_hash,
            })
        );
        drop(validation);
        drop(active);
        let repeated = run(Options {
            remotes: Vec::new(),
            dns_seeds: Some(Vec::new()),
            network: Network::Regtest,
            fetch_block: None,
            headers_db: None,
            data_dir: Some(active_dir),
            once: false,
            explorer_listen: None,
            wallet_api_files: None,
            rpc_auth_token_file: None,
            deployments: DeploymentConfig::for_network(Network::Regtest),
            ibd_policy: IbdPolicy::for_network(Network::Regtest),
            snapshot: None,
            finalize_assumeutxo: None,
            validation_target: None,
            complete_assumeutxo: Some(validation_dir),
            background_assumeutxo: None,
            cleanup_validation_dir: false,
            validation_limits: ValidationLimits::default(),
        })
        .await
        .unwrap_err();
        assert!(repeated.contains("no assumed UTXO snapshot"));
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn active_node_and_genesis_validator_run_concurrently_and_finalize() {
        let directory = TempDir::new().unwrap();
        let active_dir = directory.path().join("active");
        fs::create_dir(&active_dir).unwrap();
        let validation_dir = directory.path().join("validation");
        let genesis = bitcoin::blockdata::constants::genesis_block(Network::Regtest).header;
        let first = regtest_block(genesis.block_hash(), genesis.time + 1);
        let first_hash = first.block_hash();
        let mut second = regtest_block(first_hash, first.header.time + 1);
        second.txdata[0].input[0].script_sig = ScriptBuf::from_bytes(vec![0x52, 0x00]);
        second.header.merkle_root = second.compute_merkle_root().unwrap();
        second.header.nonce = 0;
        while second
            .header
            .validate_pow(Target::MAX_ATTAINABLE_REGTEST)
            .is_err()
        {
            second.header.nonce = second.header.nonce.checked_add(1).unwrap();
        }
        let second_hash = second.block_hash();
        RedbHeaderStore::open(active_dir.join("headers.redb"))
            .unwrap()
            .append(first.header)
            .unwrap();
        let first_outpoint = OutPointKey::from(OutPoint::new(first.txdata[0].compute_txid(), 0));
        let source = RedbUtxoStore::open(directory.path().join("source.redb")).unwrap();
        source
            .apply(
                &[],
                &[(
                    first_outpoint,
                    Utxo {
                        value_sats: block_subsidy(1),
                        height: 1,
                        is_coinbase: true,
                        last_touched: 0,
                        creation_mtp: genesis.time,
                        script_pubkey: Vec::new(),
                    },
                )],
            )
            .unwrap();
        let snapshot_path = directory.path().join("concurrent-base.rbtc");
        let manifest = export_snapshot(
            &source,
            &snapshot_path,
            "regtest",
            1,
            first_hash.to_string(),
        )
        .unwrap();
        drop(source);
        run(Options {
            remotes: Vec::new(),
            dns_seeds: None,
            network: Network::Regtest,
            fetch_block: None,
            headers_db: None,
            data_dir: Some(active_dir.clone()),
            once: false,
            explorer_listen: None,
            wallet_api_files: None,
            rpc_auth_token_file: None,
            deployments: DeploymentConfig::for_network(Network::Regtest),
            ibd_policy: IbdPolicy::for_network(Network::Regtest),
            snapshot: Some(SnapshotActivationOptions {
                path: snapshot_path,
                height: 1,
                block_hash: first_hash,
                utxo_count: manifest.utxo_count,
                records_bytes: manifest.records_bytes,
                records_sha256: manifest.records_sha256,
            }),
            finalize_assumeutxo: None,
            validation_target: None,
            complete_assumeutxo: None,
            background_assumeutxo: None,
            cleanup_validation_dir: false,
            validation_limits: ValidationLimits::default(),
        })
        .await
        .unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let remote = listener.local_addr().unwrap();
        let first_for_server = first.clone();
        let second_for_server = second.clone();
        let server = tokio::spawn(async move {
            // Both outbound connections must be established before either
            // handshake is serviced, proving this mode is not sequential.
            let (first_stream, _) = listener.accept().await.unwrap();
            let (second_stream, _) = listener.accept().await.unwrap();
            let first_peer = serve_concurrent_assumeutxo_peer(
                first_stream,
                70,
                first_for_server.clone(),
                second_for_server.clone(),
            );
            let second_peer = serve_concurrent_assumeutxo_peer(
                second_stream,
                71,
                first_for_server,
                second_for_server,
            );
            tokio::join!(first_peer, second_peer);
        });
        timeout(
            Duration::from_secs(15),
            run(Options {
                remotes: vec![remote],
                dns_seeds: Some(Vec::new()),
                network: Network::Regtest,
                fetch_block: None,
                headers_db: None,
                data_dir: Some(active_dir.clone()),
                once: true,
                explorer_listen: None,
                wallet_api_files: None,
                rpc_auth_token_file: None,
                deployments: DeploymentConfig::for_network(Network::Regtest),
                ibd_policy: IbdPolicy::for_network(Network::Regtest),
                snapshot: None,
                finalize_assumeutxo: None,
                validation_target: None,
                complete_assumeutxo: None,
                background_assumeutxo: Some(validation_dir.clone()),
                cleanup_validation_dir: true,
                validation_limits: ValidationLimits {
                    max_blocks_per_batch: 1,
                    pause_between_batches: Duration::ZERO,
                },
            }),
        )
        .await
        .expect("concurrent validation must not wait for sequential peer startup")
        .unwrap();
        server.await.unwrap();

        let active =
            RedbChainStore::open(active_dir.join("chainstate.redb"), Network::Regtest).unwrap();
        assert_eq!(active.execution().assumed_snapshot().unwrap(), None);
        assert_eq!(active.execution().tip().unwrap().height, 2);
        assert_eq!(active.execution().tip().unwrap().hash, second_hash);
        assert!(active.get(first_outpoint).unwrap().is_some());
        drop(active);
        assert!(!validation_dir.exists());
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
            rpc_auth_token_file: None,
            deployments: DeploymentConfig::for_network(Network::Regtest),
            ibd_policy: IbdPolicy::for_network(Network::Regtest),
            snapshot: None,
            finalize_assumeutxo: None,
            validation_target: None,
            complete_assumeutxo: None,
            background_assumeutxo: None,
            cleanup_validation_dir: false,
            validation_limits: ValidationLimits::default(),
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
            rpc_auth_token_file: None,
            deployments: DeploymentConfig::for_network(Network::Regtest),
            ibd_policy: IbdPolicy::for_network(Network::Regtest),
            snapshot: None,
            finalize_assumeutxo: None,
            validation_target: None,
            complete_assumeutxo: None,
            background_assumeutxo: None,
            cleanup_validation_dir: false,
            validation_limits: ValidationLimits::default(),
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
            rpc_auth_token_file: None,
            deployments: DeploymentConfig::for_network(Network::Regtest),
            ibd_policy: IbdPolicy::for_network(Network::Regtest),
            snapshot: None,
            finalize_assumeutxo: None,
            validation_target: None,
            complete_assumeutxo: None,
            background_assumeutxo: None,
            cleanup_validation_dir: false,
            validation_limits: ValidationLimits::default(),
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
            serve_duplicate_version_peer(learned_listener, 41, 42).await;
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
            rpc_auth_token_file: None,
            deployments: DeploymentConfig::for_network(Network::Regtest),
            ibd_policy: IbdPolicy::for_network(Network::Regtest),
            snapshot: None,
            finalize_assumeutxo: None,
            validation_target: None,
            complete_assumeutxo: None,
            background_assumeutxo: None,
            cleanup_validation_dir: false,
            validation_limits: ValidationLimits::default(),
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
            serve_duplicate_version_peer(manual_listener, 43, 44).await;
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
            rpc_auth_token_file: None,
            deployments: DeploymentConfig::for_network(Network::Regtest),
            ibd_policy: IbdPolicy::for_network(Network::Regtest),
            snapshot: None,
            finalize_assumeutxo: None,
            validation_target: None,
            complete_assumeutxo: None,
            background_assumeutxo: None,
            cleanup_validation_dir: false,
            validation_limits: ValidationLimits::default(),
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
            rpc_auth_token_file: None,
            deployments: DeploymentConfig::for_network(Network::Regtest),
            ibd_policy: IbdPolicy::for_network(Network::Regtest),
            snapshot: None,
            finalize_assumeutxo: None,
            validation_target: None,
            complete_assumeutxo: None,
            background_assumeutxo: None,
            cleanup_validation_dir: false,
            validation_limits: ValidationLimits::default(),
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
            rpc_auth_token_file: None,
            deployments: DeploymentConfig::for_network(Network::Regtest),
            ibd_policy: IbdPolicy::for_network(Network::Regtest),
            snapshot: None,
            finalize_assumeutxo: None,
            validation_target: None,
            complete_assumeutxo: None,
            background_assumeutxo: None,
            cleanup_validation_dir: false,
            validation_limits: ValidationLimits::default(),
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
            rpc_auth_token_file: None,
            deployments: DeploymentConfig::for_network(Network::Regtest),
            ibd_policy: IbdPolicy::for_network(Network::Regtest),
            snapshot: None,
            finalize_assumeutxo: None,
            validation_target: None,
            complete_assumeutxo: None,
            background_assumeutxo: None,
            cleanup_validation_dir: false,
            validation_limits: ValidationLimits::default(),
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
            rpc_auth_token_file: None,
            deployments: DeploymentConfig::for_network(Network::Regtest),
            ibd_policy: IbdPolicy::for_network(Network::Regtest),
            snapshot: None,
            finalize_assumeutxo: None,
            validation_target: None,
            complete_assumeutxo: None,
            background_assumeutxo: None,
            cleanup_validation_dir: false,
            validation_limits: ValidationLimits::default(),
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
    #[ignore = "run explicitly with cargo test --release --bin rbtcd tests::reproducible_end_to_end_ibd_workload -- --ignored --exact --nocapture"]
    #[allow(clippy::too_many_lines)]
    async fn reproducible_end_to_end_ibd_workload() {
        const DEFAULT_BLOCKS: u32 = 100;
        const MAX_BLOCKS: u32 = 1_000;

        let block_count = std::env::var("RBTC_BENCH_IBD_BLOCKS").map_or(DEFAULT_BLOCKS, |value| {
            value
                .parse::<u32>()
                .expect("RBTC_BENCH_IBD_BLOCKS must be an unsigned 32-bit integer")
        });
        assert!(
            (1..=MAX_BLOCKS).contains(&block_count),
            "RBTC_BENCH_IBD_BLOCKS must be in 1..={MAX_BLOCKS}"
        );

        let genesis = bitcoin::blockdata::constants::genesis_block(Network::Regtest).header;
        let mut parent = genesis.block_hash();
        let mut blocks = Vec::with_capacity(block_count as usize);
        for height in 1..=block_count {
            let block =
                regtest_block_at_height(parent, genesis.time.saturating_add(height), height);
            parent = block.block_hash();
            blocks.push(block);
        }
        let expected_tip = parent;
        let serialized_block_bytes = blocks
            .iter()
            .map(|block| u64::try_from(serialize(block).len()).unwrap())
            .sum::<u64>();
        let headers = blocks.iter().map(|block| block.header).collect::<Vec<_>>();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let remote = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut peer, _) = accept_peer(listener, peer_version(80)).await;
            respond_to_getaddr(&mut peer).await;
            match peer.read_message().await.unwrap().into_payload() {
                NetworkMessage::GetHeaders(_) => peer
                    .write_message(NetworkMessage::Headers(headers))
                    .await
                    .unwrap(),
                message => panic!("expected benchmark getheaders, got {message:?}"),
            }

            let mut sent = 0_usize;
            while sent < blocks.len() {
                let NetworkMessage::GetData(inventory) =
                    peer.read_message().await.unwrap().into_payload()
                else {
                    panic!("expected benchmark getdata");
                };
                let end = sent.checked_add(inventory.len()).unwrap();
                assert!(end <= blocks.len());
                let expected = blocks[sent..end]
                    .iter()
                    .map(|block| {
                        bitcoin::p2p::message_blockdata::Inventory::WitnessBlock(block.block_hash())
                    })
                    .collect::<Vec<_>>();
                assert_eq!(inventory, expected);
                for block in &blocks[sent..end] {
                    peer.write_message(NetworkMessage::Block(block.clone()))
                        .await
                        .unwrap();
                }
                sent = end;
            }
        });

        let directory = TempDir::new().unwrap();
        let started = std::time::Instant::now();
        timeout(
            Duration::from_secs(120),
            run(Options {
                remotes: vec![remote],
                dns_seeds: Some(Vec::new()),
                network: Network::Regtest,
                fetch_block: None,
                headers_db: None,
                data_dir: Some(directory.path().to_path_buf()),
                once: true,
                explorer_listen: None,
                wallet_api_files: None,
                rpc_auth_token_file: None,
                deployments: DeploymentConfig::for_network(Network::Regtest),
                ibd_policy: IbdPolicy::for_network(Network::Regtest),
                snapshot: None,
                finalize_assumeutxo: None,
                validation_target: None,
                complete_assumeutxo: None,
                background_assumeutxo: None,
                cleanup_validation_dir: false,
                validation_limits: ValidationLimits::default(),
            }),
        )
        .await
        .expect("bounded IBD benchmark timed out")
        .unwrap();
        let elapsed = started.elapsed();
        server.await.unwrap();

        let chainstate =
            RedbChainStore::open(directory.path().join("chainstate.redb"), Network::Regtest)
                .unwrap();
        assert_eq!(chainstate.execution().tip().unwrap().height, block_count);
        assert_eq!(chainstate.execution().tip().unwrap().hash, expected_tip);
        let explorer =
            RedbExplorerIndex::open(directory.path().join("explorer.redb"), Network::Regtest)
                .unwrap();
        assert_eq!(explorer.tip().unwrap().height, block_count);
        assert_eq!(explorer.tip().unwrap().hash, expected_tip);
        let ledger =
            PrunedBlockLedger::open(directory.path().join("blocks"), LedgerRetention::default())
                .unwrap();
        assert_eq!(ledger.retained_ranges().unwrap(), vec![(1, block_count)]);

        let elapsed_ns = u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX);
        let report = serde_json::json!({
            "schema_version": 1,
            "generated_fixture": true,
            "blocks": block_count,
            "serialized_block_bytes": serialized_block_bytes,
            "elapsed_ns": elapsed_ns,
            "nanoseconds_per_block": elapsed_ns / u64::from(block_count),
            "tip_hash": expected_tip.to_string(),
        });
        let report = serde_json::to_string_pretty(&report).unwrap();
        println!("{report}");
        if let Ok(output) = std::env::var("RBTC_BENCH_IBD_REPORT") {
            let output = PathBuf::from(output);
            if let Some(parent) = output
                .parent()
                .filter(|parent| !parent.as_os_str().is_empty())
            {
                fs::create_dir_all(parent).unwrap();
            }
            fs::write(output, format!("{report}\n")).unwrap();
        }
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
                rpc_auth_token_file: None,
                deployments: DeploymentConfig::for_network(Network::Regtest),
                ibd_policy: IbdPolicy::for_network(Network::Regtest),
                snapshot: None,
                finalize_assumeutxo: None,
                validation_target: None,
                complete_assumeutxo: None,
                background_assumeutxo: None,
                cleanup_validation_dir: false,
                validation_limits: ValidationLimits::default(),
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
            rpc_auth_token_file: None,
            deployments: DeploymentConfig::for_network(Network::Signet),
            ibd_policy,
            snapshot: None,
            finalize_assumeutxo: None,
            validation_target: None,
            complete_assumeutxo: None,
            background_assumeutxo: None,
            cleanup_validation_dir: false,
            validation_limits: ValidationLimits::default(),
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
