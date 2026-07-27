//! Embeddable node runtime and command-line adapter.

mod config_file;

use fs2::FileExt;
use std::{
    collections::{BTreeSet, HashSet, VecDeque},
    fs,
    io::{self, Read, Seek, Write},
    net::{IpAddr, SocketAddr},
    path::PathBuf,
    str::FromStr,
    sync::{
        Arc, Mutex, RwLock,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use thiserror::Error;

#[derive(Default)]
struct RuntimeControl {
    shutdown_requested: AtomicBool,
    checkpoints_in_flight: AtomicUsize,
    shutdown_notify: tokio::sync::Notify,
}

impl RuntimeControl {
    fn request_shutdown(&self) {
        if !self.shutdown_requested.swap(true, Ordering::AcqRel) {
            self.shutdown_notify.notify_one();
        }
    }

    async fn shutdown_requested(&self) {
        if self.shutdown_requested.load(Ordering::Acquire) {
            return;
        }
        self.shutdown_notify.notified().await;
    }
}

struct InFlightCheckpoint<'a> {
    count: &'a AtomicUsize,
}

impl<'a> InFlightCheckpoint<'a> {
    fn try_enter(requested: &AtomicBool, count: &'a AtomicUsize) -> Option<Self> {
        if requested.load(Ordering::Acquire) {
            return None;
        }
        count.fetch_add(1, Ordering::AcqRel);
        if requested.load(Ordering::Acquire) {
            count.fetch_sub(1, Ordering::AcqRel);
            None
        } else {
            Some(Self { count })
        }
    }
}

impl Drop for InFlightCheckpoint<'_> {
    fn drop(&mut self) {
        self.count.fetch_sub(1, Ordering::AcqRel);
    }
}

use bitcoin::{
    Block, BlockHash, Network, OutPoint, Transaction, Txid,
    block::Header,
    consensus::{deserialize, serialize},
    hashes::Hash,
    hex::{DisplayHex, FromHex},
    p2p::{ServiceFlags, message_blockdata::Inventory},
};
use rbtc::{
    api::{
        AuthorizationAuditLog, ExplorerEventHub, ExplorerEventKind, LocalAuthToken,
        LocalRpcOperator, LocalRpcOperatorError, WALLET_BROADCAST_QUEUE_CAPACITY,
        WalletBroadcastRequest, WalletBroadcastSink, explorer_events_router, explorer_router,
        rpc_router_with_operator, wallet_router_with_sink,
    },
    archive::bounded_archive_prefix_len,
    auxiliary_index::{AuxiliaryIndexKind, RedbAuxiliaryIndex},
    block_execution::{
        BlockDeploymentContext, BlockExecutionError,
        connect_prevalidated_active_blocks_with_txids_and_utxos, disconnect_execution_tip,
        prefetch_prevalidated_active_block_utxos,
    },
    blockchain::{
        AppliedBlock, ValidatedBlockTransactionIds, validate_block_structure_with_deployments,
        validate_block_structure_with_deployments_and_txids,
    },
    chain_store::{
        ChainStoreError, ChainStoreOptions, RedbChainStore, ValidationDeltaShardMigration,
    },
    core_snapshot::verify_core31_snapshot,
    deployments::{DeploymentConfig, block_deployment_context_for_headers},
    diagnostics::{
        LogConfig, LogLevel, MAX_LOG_FILE_BYTES, MAX_LOG_FILES, MIN_LOG_FILE_BYTES, MIN_LOG_FILES,
    },
    execution_store::RedbExecutionStore,
    explorer_store::RedbExplorerIndex,
    fee_estimator::{FeeEstimatorError, FeeTrack, RedbFeeEstimator},
    header_store::RedbHeaderStore,
    headers::{HeaderDag, HeaderError, HeaderInfo},
    ibd::IbdPolicy,
    inbound::{
        InboundBasicFilter, InboundDataSource, InboundLimits, InboundStats, InboundStatsSnapshot,
        run_listener_with_stats,
    },
    index_policy::{
        IndexBuildState, IndexHistoryAvailability, IndexKind, validate_index_activation,
        validate_prune_boundary,
    },
    ledger::{
        DEFAULT_MAX_BYTES, DEFAULT_RETENTION_BLOCKS, LedgerBlockBatch, LedgerPrunePlan,
        LedgerRetention, PrunedBlockLedger,
    },
    p2p::{
        BlockTransferStats, MAX_BLOCKS_IN_FLIGHT, MAX_HEADERS_PER_RESPONSE,
        MAX_PENDING_TRANSACTION_INVENTORY, MAX_PENDING_TRANSACTIONS, MAX_PROTOCOL_MESSAGE_LEN,
        MempoolRelaySource, P2pError, PeerSession, TransactionRelay, connect_outbound,
    },
    peer_store::{RedbPeerStore, is_acceptable_peer_address},
    rbtc_info, rbtc_warn,
    rebroadcast_store::RedbRebroadcastStore,
    snapshot::{SnapshotTrustAnchor, verify_snapshot_with_trust},
    snapshot_download::{
        DEFAULT_SNAPSHOT_CHUNK_BYTES, DEFAULT_SNAPSHOT_DOWNLOAD_WORKERS,
        MAX_SNAPSHOT_DOWNLOAD_WORKERS, SnapshotDownloadConfig, download_snapshot,
    },
    transaction_admission::{
        AdmittedTransactionRelay, MAX_ADMITTED_TRANSACTION_BYTES, MAX_ADMITTED_TRANSACTIONS,
        MAX_CONFIGURED_MEMPOOL_BYTES, MAX_CONFIGURED_MEMPOOL_TRANSACTIONS,
        TransactionAdmissionContext, TransactionAdmissionPool, TransactionRequestId,
        dependency_packages, transaction_descendant_closure,
    },
    transaction_pool_store::{DEFAULT_MEMPOOL_EXPIRY_SECS, RedbTransactionPoolStore},
    utxo::{DEFAULT_HOT_WINDOW_SECS, UtxoStore},
    validation_owner::{
        MAX_VALIDATION_OWNER_BYTES, ValidationDirectoryOwner, parse_validation_directory_owner,
    },
    wallet::{
        EmbeddedWallet, MAX_WALLET_DESCRIPTOR_CONFIG_BYTES, WalletTip,
        parse_wallet_descriptor_config, read_persisted_wallet_sync_height,
    },
};
use tokio::sync::{broadcast, mpsc, watch};
use tokio::time::timeout;

const PEER_TIMEOUT: Duration = Duration::from_secs(30);
const AUXILIARY_BLOCK_RESPONSE_GRACE: Duration = Duration::from_secs(2);
const DEFAULT_VALIDATION_BATCH_SIZE: usize = 64;
const VALIDATION_BLOCK_WINDOW_SIZE: usize = MAX_BLOCKS_IN_FLIGHT * 4;
const MAX_VALIDATION_BATCH_SIZE: usize = 1_008;
const MAX_VALIDATION_PREFETCH_BATCH_SIZE: usize = MAX_VALIDATION_BATCH_SIZE;
const BULK_VALIDATION_CHAINSTATE_CACHE_BYTES: usize = 16 * 1024 * 1024 * 1024;
const BACKGROUND_PIPELINE_CHAINSTATE_CACHE_BYTES: usize = 8 * 1024 * 1024 * 1024;
const STANDBY_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(30);
#[cfg(not(test))]
const STANDBY_REAP_INTERVAL: Duration = Duration::from_secs(1);
#[cfg(test)]
const STANDBY_REAP_INTERVAL: Duration = Duration::from_millis(20);
const PEER_DISCOVERY_TIMEOUT: Duration = Duration::from_secs(3);
const DNS_SEED_TIMEOUT: Duration = Duration::from_secs(5);
const USER_AGENT: &str = "/rbtcd:0.1.0/";
const MAX_CONFIGURED_PEERS: usize = 16;
const DEFAULT_AUTOMATIC_HOT_STANDBYS: usize = 8;
const MAX_AUTOMATIC_HOT_STANDBYS: usize = 16;
const MAX_DNS_SEEDS: usize = 16;
const MAX_DNS_ADDRESSES_PER_SEED: usize = 64;
const UTXO_ACTIVITY_WINDOWS: [u32; 11] = [
    144, 1_008, 4_320, 8_640, 12_960, 25_920, 52_560, 105_120, 157_680, 262_800, 525_600,
];
const MIN_ACTIVITY_RECOMMENDATION_SAMPLES: u64 = 1_000_000;
const PERCENT_DECIMAL_SCALE: u64 = 100_000;
const ACTIVITY_RECOMMENDATION_PERCENT_UNITS: u64 = 99 * PERCENT_DECIMAL_SCALE;
const UTXO_RETIER_SCAN_BATCH_SIZE: usize = 65_536;
const MIN_PRUNE_RETENTION_BLOCKS: u32 = 288;
const MIN_PRUNE_MAX_BYTES: u64 = 550 * 1024 * 1024;
const DEFAULT_MINIMUM_FREE_BYTES: u64 = 5 * 1024 * 1024 * 1024;
const MINIMUM_FREE_BYTES_FLOOR: u64 = 512 * 1024 * 1024;
const MAXIMUM_FREE_BYTES_RESERVE: u64 = 1024 * 1024 * 1024 * 1024;
const DATABASE_COMMIT_HEADROOM_BYTES: u64 = 512 * 1024 * 1024;
const MAX_SERIALIZED_BLOCK_BYTES: u64 = 4_000_000;
const MIN_CHAINSTATE_CACHE_BYTES: usize = 64 * 1024 * 1024;
const MAX_CHAINSTATE_CACHE_BYTES: usize = 64 * 1024 * 1024 * 1024;

const fn validation_prefetch_limit(maximum_batch_size: usize) -> usize {
    if maximum_batch_size < MAX_VALIDATION_PREFETCH_BATCH_SIZE {
        maximum_batch_size
    } else {
        MAX_VALIDATION_PREFETCH_BATCH_SIZE
    }
}
const MAX_LOCAL_AUTH_TOKEN_FILE_LEN: u64 = 1024;
#[cfg(not(test))]
const LOCAL_AUTH_TOKEN_RELOAD_INTERVAL: Duration = Duration::from_secs(1);
#[cfg(test)]
const LOCAL_AUTH_TOKEN_RELOAD_INTERVAL: Duration = Duration::from_millis(20);
const MAX_WALLET_SCAN_PASSES: usize = 64;
const MAX_COMPACT_TRANSACTION_CANDIDATES: usize = 64;
const PEER_TRANSACTION_REBROADCAST_INTERVAL_SECS: u32 = 12 * 60 * 60;
const API_AUDIT_FILE: &str = "api-auth-audit.jsonl";
const MAX_VALIDATION_PAUSE_MS: u64 = 60_000;
const ADAPTIVE_VALIDATION_BUSY_BATCH_SIZE: usize = 252;
const MIN_PARALLEL_STRUCTURE_BLOCKS_PER_WORKER: usize = 64;
const VALIDATION_OWNER_FILE: &str = ".rbtc-validation-owner.json";
const DATA_DIRECTORY_LOCK_FILE: &str = ".rbtc.lock";
const MAX_DATA_DIRECTORY_LOCK_MARKER_BYTES: u64 = 512;
const DATA_FORMAT_MANIFEST_FILE: &str = ".rbtc-data-format.json";
const DATA_FORMAT_SCHEMA_VERSION: u32 = 3;
const MAX_DATA_FORMAT_MANIFEST_BYTES: u64 = 4 * 1024;
const NODE_EVENT_CAPACITY: usize = 32;
const DEFAULT_STORAGE_AUDIT_MAX_SEGMENTS: u32 = DEFAULT_RETENTION_BLOCKS;
const MAX_STORAGE_AUDIT_SEGMENTS: u32 = 4_096;
const DEFAULT_STORAGE_AUDIT_MAX_BYTES: u64 = 2 * 1024 * 1024 * 1024;
const MIN_STORAGE_AUDIT_BYTES: u64 = 1024 * 1024;
const MAX_STORAGE_AUDIT_BYTES: u64 = 16 * 1024 * 1024 * 1024;
const DEFAULT_VERIFY_CHAIN_DEPTH: u32 = 288;
const MAX_VERIFY_CHAIN_DEPTH: u32 = DEFAULT_RETENTION_BLOCKS;
const DEFAULT_VERIFY_CHAIN_BLOCK_BYTES: u64 = 1024 * 1024 * 1024;
const MIN_VERIFY_CHAIN_BLOCK_BYTES: u64 = 1024 * 1024;
const MAX_VERIFY_CHAIN_BLOCK_BYTES: u64 = 4 * 1024 * 1024 * 1024;

const fn supports_experimental_block_execution(network: Network) -> bool {
    matches!(network, Network::Bitcoin | Network::Testnet)
}

#[derive(Clone)]
struct Options {
    remotes: Vec<SocketAddr>,
    /// `None` selects the pinned Core 31 defaults; `Some` is an explicit override.
    dns_seeds: Option<Vec<DnsSeed>>,
    network: Network,
    fetch_block: Option<BlockHash>,
    headers_db: Option<PathBuf>,
    data_dir: Option<PathBuf>,
    once: bool,
    network_execution: NetworkExecutionMode,
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
    mempool_full_rbf: bool,
    cleanup_validation_dir: bool,
    offline_action: Option<OfflineAction>,
    validation_limits: ValidationLimits,
    ledger_retention: LedgerRetention,
    minimum_free_bytes: u64,
    cache: NodeCacheConfig,
    indexes: NodeIndexConfig,
    inbound_listen: Option<SocketAddr>,
    inbound_limits: InboundLimits,
    resources: NodeResourceConfig,
    logging: NodeLogConfig,
    runtime_control: Arc<RuntimeControl>,
    observer: Option<NodeObserver>,
}

struct AuxiliaryIndexes {
    transaction: Option<Arc<RedbAuxiliaryIndex>>,
    spent_output: Option<Arc<RedbAuxiliaryIndex>>,
    basic_filter: Option<Arc<RedbAuxiliaryIndex>>,
}

impl AuxiliaryIndexes {
    fn open(
        data_dir: &std::path::Path,
        network: Network,
        config: NodeIndexConfig,
    ) -> Result<Self, String> {
        let open = |enabled: bool, name: &str, kind| {
            enabled
                .then(|| RedbAuxiliaryIndex::open(data_dir.join(name), network, kind).map(Arc::new))
                .transpose()
                .map_err(|error| error.to_string())
        };
        Ok(Self {
            transaction: open(
                config.transaction,
                "txindex.redb",
                AuxiliaryIndexKind::Transaction,
            )?,
            spent_output: open(
                config.spent_output,
                "spent-output-index.redb",
                AuxiliaryIndexKind::SpentOutput,
            )?,
            basic_filter: open(
                config.basic_filter,
                "basic-filter-index.redb",
                AuxiliaryIndexKind::BasicFilter,
            )?,
        })
    }

    fn enabled(&self) -> impl Iterator<Item = (AuxiliaryIndexKind, &RedbAuxiliaryIndex)> {
        [
            (AuxiliaryIndexKind::Transaction, self.transaction.as_deref()),
            (
                AuxiliaryIndexKind::SpentOutput,
                self.spent_output.as_deref(),
            ),
            (
                AuxiliaryIndexKind::BasicFilter,
                self.basic_filter.as_deref(),
            ),
        ]
        .into_iter()
        .filter_map(|(kind, index)| index.map(|index| (kind, index)))
    }
}

const fn auxiliary_policy_kind(kind: AuxiliaryIndexKind) -> IndexKind {
    match kind {
        AuxiliaryIndexKind::Transaction => IndexKind::Transaction,
        AuxiliaryIndexKind::SpentOutput => IndexKind::SpentOutput,
        AuxiliaryIndexKind::BasicFilter => IndexKind::BasicFilter,
    }
}

const fn auxiliary_index_label(kind: AuxiliaryIndexKind) -> &'static str {
    match kind {
        AuxiliaryIndexKind::Transaction => "transaction",
        AuxiliaryIndexKind::SpentOutput => "spent-output",
        AuxiliaryIndexKind::BasicFilter => "basic-filter",
    }
}

/// Complete typed configuration for one ordinary persistent embedded node.
///
/// Offline snapshot activation/download and repair commands remain separate
/// tools. This type covers the long-running node surface, including background
/// AssumeUTXO validation and authenticated loopback APIs.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeConfig {
    /// Selected Bitcoin network.
    pub network: Network,
    /// Isolated durable data directory for this instance.
    pub data_dir: PathBuf,
    /// Ordered manually preferred outbound peers.
    pub preferred_peers: Vec<SocketAddr>,
    /// DNS bootstrap policy.
    pub dns_seeds: NodeDnsSeedPolicy,
    /// Return after reaching the current maximum-work peer tip.
    pub once: bool,
    /// Opt in to full-RBF mempool policy.
    pub mempool_full_rbf: bool,
    /// Bounded chainstate cache budgets.
    pub cache: NodeCacheConfig,
    /// Independently persisted optional indexes.
    pub indexes: NodeIndexConfig,
    /// Optional bounded inbound P2P listener.
    pub inbound: Option<NodeInboundConfig>,
    /// Peer and transaction-pool resource ceilings.
    pub resources: NodeResourceConfig,
    /// Standalone structured logging policy; embedded hosts may consume events.
    pub logging: NodeLogConfig,
    /// Bounded local freezer retention.
    pub storage: NodeStorageConfig,
    /// Optional loopback explorer/RPC/wallet services.
    pub api: Option<NodeApiConfig>,
    /// Consensus and IBD policy overrides.
    pub consensus: NodeConsensusConfig,
    /// Optional independent genesis validation of an assumed snapshot.
    pub assumeutxo_validation: Option<NodeAssumeUtxoValidation>,
}

impl NodeConfig {
    /// Creates one persistent node with Core 31 network defaults.
    #[must_use]
    pub fn new(network: Network, data_dir: impl Into<PathBuf>) -> Self {
        Self {
            network,
            data_dir: data_dir.into(),
            preferred_peers: Vec::new(),
            dns_seeds: NodeDnsSeedPolicy::Pinned,
            once: false,
            mempool_full_rbf: false,
            cache: NodeCacheConfig::default(),
            indexes: NodeIndexConfig::default(),
            inbound: None,
            resources: NodeResourceConfig::default(),
            logging: NodeLogConfig::default(),
            storage: NodeStorageConfig::default(),
            api: None,
            consensus: NodeConsensusConfig::default(),
            assumeutxo_validation: None,
        }
    }

    /// Validates every bound and cross-field dependency without opening files
    /// or sockets.
    pub fn validate(&self) -> Result<(), NodeConfigError> {
        self.clone().into_options().map(drop)
    }

    #[allow(clippy::too_many_lines)]
    fn into_options(self) -> Result<Options, NodeConfigError> {
        let deployments = parse_deployment_config(
            self.network,
            true,
            self.consensus.version_bits,
            self.consensus.activation_heights,
        )
        .map_err(NodeConfigError::new)?;
        let mut deployments = deployments;
        if let Some(challenge) = self.consensus.signet_challenge {
            deployments
                .set_signet_challenge(challenge)
                .map_err(|error| NodeConfigError::new(error.to_string()))?;
        }
        let ibd_policy = parse_ibd_policy(
            self.network,
            deployments.is_custom_signet(),
            true,
            self.consensus.minimum_chainwork,
            self.consensus.assume_valid,
        )
        .map_err(NodeConfigError::new)?;
        let dns_seeds = match self.dns_seeds {
            NodeDnsSeedPolicy::Pinned if deployments.is_custom_signet() => Some(Vec::new()),
            NodeDnsSeedPolicy::Pinned => None,
            NodeDnsSeedPolicy::Disabled => Some(Vec::new()),
            NodeDnsSeedPolicy::Custom(seeds) => {
                Some(seeds.into_iter().map(NodeDnsSeed::into_internal).collect())
            }
        };
        let (explorer_listen, wallet_api_files, rpc_auth_token_file) =
            self.api.map_or((None, None, None), |api| {
                (
                    Some(api.listen),
                    api.wallet.map(|wallet| WalletApiFiles {
                        descriptors: wallet.descriptors,
                        auth_token: wallet.auth_token,
                    }),
                    api.rpc_auth_token,
                )
            });
        let (complete_assumeutxo, background_assumeutxo, cleanup_validation_dir, validation_limits) =
            self.assumeutxo_validation.map_or(
                (None, None, false, ValidationLimits::default()),
                |validation| {
                    let (complete, background, config) = match validation {
                        NodeAssumeUtxoValidation::Complete(config) => (true, false, config),
                        NodeAssumeUtxoValidation::Background(config) => (false, true, config),
                    };
                    let limits = ValidationLimits {
                        max_blocks_per_batch: config.limits.max_blocks_per_batch,
                        pause_between_batches: config.limits.pause_between_batches,
                        quick_repair: config.limits.quick_repair,
                    };
                    let validation_dir = config.validation_dir;
                    (
                        complete.then(|| validation_dir.clone()),
                        background.then_some(validation_dir),
                        config.cleanup_on_success,
                        limits,
                    )
                },
            );
        validate_runtime_options(Options {
            remotes: self.preferred_peers,
            dns_seeds,
            network: self.network,
            fetch_block: None,
            headers_db: None,
            data_dir: Some(self.data_dir),
            once: self.once,
            network_execution: NetworkExecutionMode::Persistent,
            explorer_listen,
            wallet_api_files,
            rpc_auth_token_file,
            deployments,
            ibd_policy,
            snapshot: None,
            finalize_assumeutxo: None,
            validation_target: None,
            complete_assumeutxo,
            background_assumeutxo,
            mempool_full_rbf: self.mempool_full_rbf,
            cleanup_validation_dir,
            offline_action: None,
            validation_limits,
            ledger_retention: LedgerRetention {
                max_blocks: self.storage.prune_blocks,
                max_bytes: self.storage.prune_bytes,
                ..LedgerRetention::default()
            },
            minimum_free_bytes: self.storage.minimum_free_bytes,
            cache: self.cache,
            indexes: self.indexes,
            inbound_listen: self.inbound.as_ref().map(|inbound| inbound.listen),
            inbound_limits: self
                .inbound
                .map_or_else(InboundLimits::default, NodeInboundConfig::limits),
            resources: self.resources,
            logging: self.logging,
            runtime_control: Arc::new(RuntimeControl::default()),
            observer: None,
        })
        .map_err(NodeConfigError::new)
    }
}

/// Invalid embedded node configuration.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
#[error("invalid node configuration: {message}")]
pub struct NodeConfigError {
    message: String,
}

impl NodeConfigError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

/// DNS bootstrap selection for an embedded node.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub enum NodeDnsSeedPolicy {
    /// Use the release-pinned Core 31 seed set.
    #[default]
    Pinned,
    /// Disable DNS bootstrap and use only manual or persisted peers.
    Disabled,
    /// Replace pinned seeds with an ordered, bounded custom set.
    Custom(Vec<NodeDnsSeed>),
}

/// One validated DNS seed host and explicit port.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct NodeDnsSeed {
    host: String,
    port: u16,
}

impl NodeDnsSeed {
    /// Validates and normalizes one DNS name or IP address.
    pub fn new(host: impl Into<String>, port: u16) -> Result<Self, NodeConfigError> {
        let host = host.into();
        let rendered = if host.parse::<std::net::Ipv6Addr>().is_ok() {
            format!("[{host}]:{port}")
        } else {
            format!("{host}:{port}")
        };
        let seed = parse_dns_seed(&rendered, Network::Bitcoin).map_err(NodeConfigError::new)?;
        Ok(Self {
            host: seed.host,
            port: seed.port,
        })
    }

    /// Normalized host without brackets.
    #[must_use]
    pub fn host(&self) -> &str {
        &self.host
    }

    /// Explicit P2P port.
    #[must_use]
    pub const fn port(&self) -> u16 {
        self.port
    }

    fn into_internal(self) -> DnsSeed {
        DnsSeed {
            host: self.host,
            port: self.port,
        }
    }
}

/// Bounded cache budgets for one node instance.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NodeCacheConfig {
    /// Ordinary active-chain cache.
    pub active_chainstate_bytes: usize,
    /// Cache while active and genesis-validation pipelines coexist.
    pub background_chainstate_bytes: usize,
    /// Cache for explicit high-throughput bounded validation.
    pub bulk_validation_bytes: usize,
}

impl Default for NodeCacheConfig {
    fn default() -> Self {
        Self {
            active_chainstate_bytes: 1024 * 1024 * 1024,
            background_chainstate_bytes: BACKGROUND_PIPELINE_CHAINSTATE_CACHE_BYTES,
            bulk_validation_bytes: BULK_VALIDATION_CHAINSTATE_CACHE_BYTES,
        }
    }
}

/// Bounded freezer policy for one node instance.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NodeStorageConfig {
    /// Maximum locally retrievable block window.
    pub prune_blocks: u32,
    /// Maximum compressed freezer bytes.
    pub prune_bytes: u64,
    /// Free bytes preserved beyond one worst-case atomic checkpoint.
    pub minimum_free_bytes: u64,
}

/// Independently persisted optional active-chain indexes.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct NodeIndexConfig {
    /// Transaction ID to active block position (`-txindex` in Bitcoin Core).
    pub transaction: bool,
    /// Outpoint to active-chain spending transaction.
    pub spent_output: bool,
    /// BIP157/158 basic compact filters and filter headers.
    pub basic_filter: bool,
}

/// Optional inbound P2P listener and resource policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NodeInboundConfig {
    /// Explicit socket address to bind. No listener is opened when absent.
    pub listen: SocketAddr,
    /// Maximum concurrent inbound handshakes and established peers.
    pub max_connections: usize,
    /// Maximum concurrent inbound sockets from one IP or source network group.
    pub max_connections_per_ip: usize,
    /// Rolling 24-hour historical upload target; zero means unlimited.
    pub max_upload_bytes_per_day: u64,
    /// Maximum non-keepalive requests accepted per peer per minute.
    pub max_requests_per_minute: u32,
}

impl NodeInboundConfig {
    const fn limits(self) -> InboundLimits {
        InboundLimits {
            max_connections: self.max_connections,
            max_connections_per_ip: self.max_connections_per_ip,
            max_upload_bytes_per_day: self.max_upload_bytes_per_day,
            max_requests_per_minute: self.max_requests_per_minute,
            idle_timeout: Duration::from_secs(20 * 60),
        }
    }
}

impl Default for NodeStorageConfig {
    fn default() -> Self {
        Self {
            prune_blocks: DEFAULT_RETENTION_BLOCKS,
            prune_bytes: DEFAULT_MAX_BYTES,
            minimum_free_bytes: DEFAULT_MINIMUM_FREE_BYTES,
        }
    }
}

/// Bounded peer and transaction-pool resources for one node instance.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NodeResourceConfig {
    /// Maximum automatically discovered hot standby sessions.
    pub automatic_hot_standbys: usize,
    /// Maximum admitted mempool transactions.
    pub mempool_max_transactions: usize,
    /// Maximum witness-serialized mempool bytes.
    pub mempool_max_bytes: usize,
}

/// Standalone structured logging policy.
pub type NodeLogConfig = LogConfig;

impl Default for NodeResourceConfig {
    fn default() -> Self {
        Self {
            automatic_hot_standbys: DEFAULT_AUTOMATIC_HOT_STANDBYS,
            mempool_max_transactions: MAX_ADMITTED_TRANSACTIONS,
            mempool_max_bytes: MAX_ADMITTED_TRANSACTION_BYTES,
        }
    }
}

/// Optional loopback service configuration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeApiConfig {
    /// Loopback listen address for explorer and authenticated subrouters.
    pub listen: SocketAddr,
    /// Optional owner-only bearer-token file for read-only RPC.
    pub rpc_auth_token: Option<PathBuf>,
    /// Optional watch-only wallet configuration.
    pub wallet: Option<NodeWalletApiConfig>,
}

/// Watch-only wallet files for the embedded API.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeWalletApiConfig {
    /// Owner-only public descriptor configuration.
    pub descriptors: PathBuf,
    /// Owner-only bearer-token file.
    pub auth_token: PathBuf,
}

/// Core-compatible consensus and IBD policy overrides.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct NodeConsensusConfig {
    /// Ordered Core-compatible `vbparams` values.
    pub version_bits: Vec<String>,
    /// Ordered Core-compatible buried activation overrides.
    pub activation_heights: Vec<String>,
    /// Exact custom BIP325 Signet challenge script.
    pub signet_challenge: Option<Vec<u8>>,
    /// Optional 256-bit hexadecimal minimum chainwork override.
    pub minimum_chainwork: Option<String>,
    /// Optional assume-valid block hash or zero.
    pub assume_valid: Option<String>,
}

/// Bounded independent genesis-validation resources.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NodeValidationConfig {
    /// Blocks in one atomic validation checkpoint.
    pub max_blocks_per_batch: usize,
    /// Optional pause between background checkpoints.
    pub pause_between_batches: Duration,
    /// Allow the storage engine's bounded quick-repair path.
    pub quick_repair: bool,
}

impl Default for NodeValidationConfig {
    fn default() -> Self {
        Self {
            max_blocks_per_batch: DEFAULT_VALIDATION_BATCH_SIZE,
            pause_between_batches: Duration::ZERO,
            quick_repair: true,
        }
    }
}

/// Shared parameters for sequential or background AssumeUTXO verification.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeAssumeUtxoConfig {
    /// Isolated genesis-validation data directory.
    pub validation_dir: PathBuf,
    /// Bounded checkpoint resources.
    pub limits: NodeValidationConfig,
    /// Quarantine and remove the validation directory after finalization.
    pub cleanup_on_success: bool,
}

/// How an embedded node independently verifies an assumed UTXO base.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum NodeAssumeUtxoValidation {
    /// Validate to the assumed base before serving the active node.
    Complete(NodeAssumeUtxoConfig),
    /// Serve base-to-tip state while genesis validation progresses separately.
    Background(NodeAssumeUtxoConfig),
}

/// Builder for one outbound validating node owned by a host Tokio runtime.
///
/// The typed surface covers an ordinary persistent node, optional loopback
/// explorer/RPC/watch-only-wallet services, and independent AssumeUTXO
/// validation. Offline snapshot installation/download and repair actions remain
/// explicit tools rather than hidden launch side effects.
#[derive(Clone, Debug)]
pub struct NodeBuilder {
    config: NodeConfig,
}

impl NodeBuilder {
    /// Creates a persistent node using network-scoped Core 31 defaults.
    #[must_use]
    pub fn new(network: Network, data_dir: impl Into<PathBuf>) -> Self {
        Self {
            config: NodeConfig::new(network, data_dir),
        }
    }

    /// Creates a builder from the complete typed embedded configuration.
    #[must_use]
    pub const fn from_config(config: NodeConfig) -> Self {
        Self { config }
    }

    /// Returns the complete typed configuration before launch.
    #[must_use]
    pub const fn config(&self) -> &NodeConfig {
        &self.config
    }

    /// Adds one preferred outbound peer before persisted peers and DNS seeds.
    #[must_use]
    pub fn connect(mut self, remote: SocketAddr) -> Self {
        if !self.config.preferred_peers.contains(&remote) {
            self.config.preferred_peers.push(remote);
        }
        self
    }

    /// Selects pinned, disabled, or custom DNS bootstrap.
    #[must_use]
    pub fn dns_seeds(mut self, policy: NodeDnsSeedPolicy) -> Self {
        self.config.dns_seeds = policy;
        self
    }

    /// Returns after reaching the current peer-advertised maximum-work tip.
    #[must_use]
    pub const fn once(mut self, enabled: bool) -> Self {
        self.config.once = enabled;
        self
    }

    /// Enables opt-in full-RBF transaction admission policy.
    #[must_use]
    pub const fn mempool_full_rbf(mut self, enabled: bool) -> Self {
        self.config.mempool_full_rbf = enabled;
        self
    }

    /// Sets bounded active, background, and bulk chainstate cache budgets.
    #[must_use]
    pub const fn cache(mut self, cache: NodeCacheConfig) -> Self {
        self.config.cache = cache;
        self
    }

    /// Enables independently rebuildable transaction, spent-output, or basic-filter indexes.
    #[must_use]
    pub const fn indexes(mut self, indexes: NodeIndexConfig) -> Self {
        self.config.indexes = indexes;
        self
    }

    /// Enables a bounded inbound Bitcoin P2P listener.
    #[must_use]
    pub const fn inbound(mut self, inbound: NodeInboundConfig) -> Self {
        self.config.inbound = Some(inbound);
        self
    }

    /// Sets bounded hot-standby and transaction-pool resources.
    #[must_use]
    pub const fn resources(mut self, resources: NodeResourceConfig) -> Self {
        self.config.resources = resources;
        self
    }

    /// Sets the standalone structured logging policy.
    #[must_use]
    pub fn logging(mut self, logging: NodeLogConfig) -> Self {
        self.config.logging = logging;
        self
    }

    /// Mounts optional loopback explorer/RPC/watch-only-wallet services.
    #[must_use]
    pub fn api(mut self, api: NodeApiConfig) -> Self {
        self.config.api = Some(api);
        self
    }

    /// Configures sequential or concurrent independent snapshot verification.
    #[must_use]
    pub fn assumeutxo_validation(mut self, validation: NodeAssumeUtxoValidation) -> Self {
        self.config.assumeutxo_validation = Some(validation);
        self
    }

    /// Sets bounded circular block retention.
    ///
    /// Block retention is currently constrained to 288–1,008 blocks and the
    /// compressed-byte target must be at least 550 MiB.
    #[must_use]
    pub const fn ledger_retention(mut self, max_blocks: u32, max_bytes: u64) -> Self {
        self.config.storage.prune_blocks = max_blocks;
        self.config.storage.prune_bytes = max_bytes;
        self
    }

    /// Preserves free space beyond the forecast worst-case atomic checkpoint.
    #[must_use]
    pub const fn minimum_free_bytes(mut self, bytes: u64) -> Self {
        self.config.storage.minimum_free_bytes = bytes;
        self
    }

    /// Starts the node as a host-owned Tokio task.
    pub fn launch(self) -> Result<NodeHandle, NodeError> {
        let mut options = self.into_options()?;
        tokio::runtime::Handle::try_current().map_err(|_| NodeError::MissingRuntime)?;
        let runtime_control = Arc::clone(&options.runtime_control);
        let (shutdown, mut shutdown_rx) = watch::channel(false);
        let (lifecycle, lifecycle_rx) = watch::channel(NodeLifecycle::Starting);
        let (runtime_status, runtime_status_rx) =
            watch::channel(NodeRuntimeStatus::starting(options.network));
        let (events, _) = broadcast::channel(NODE_EVENT_CAPACITY);
        let observer = NodeObserver {
            status: runtime_status.clone(),
            events: events.clone(),
        };
        options.observer = Some(observer.clone());
        let task_lifecycle = lifecycle.clone();
        let task_events = events.clone();
        let task_observer = observer;
        let task = tokio::spawn(async move {
            task_lifecycle.send_replace(NodeLifecycle::Running);
            let _ = task_events.send(NodeEvent::Started);
            let run = run(options);
            tokio::pin!(run);
            let wait_for_runtime_shutdown = runtime_control.shutdown_requested();
            tokio::pin!(wait_for_runtime_shutdown);
            let result = tokio::select! {
                result = &mut run => result.map_err(NodeError::Runtime),
                changed = shutdown_rx.changed() => {
                    if changed.is_ok() && !*shutdown_rx.borrow() {
                        Err(NodeError::ControlChannelClosed)
                    } else {
                        task_lifecycle.send_replace(NodeLifecycle::Stopping);
                        runtime_control.request_shutdown();
                        wait_for_in_flight_checkpoints(
                            &runtime_control.checkpoints_in_flight,
                        )
                        .await;
                        Ok(())
                    }
                },
                () = &mut wait_for_runtime_shutdown => {
                    task_lifecycle.send_replace(NodeLifecycle::Stopping);
                    wait_for_in_flight_checkpoints(
                        &runtime_control.checkpoints_in_flight,
                    )
                    .await;
                    Ok(())
                }
            };
            match &result {
                Ok(()) => {
                    task_lifecycle.send_replace(NodeLifecycle::Stopped);
                    let _ = task_events.send(NodeEvent::Stopped);
                }
                Err(error) => {
                    let message = error.to_string();
                    task_lifecycle.send_replace(NodeLifecycle::Failed(message.clone()));
                    task_observer.failed(message.clone());
                    let _ = task_events.send(NodeEvent::Failed { message });
                }
            }
            result
        });
        Ok(NodeHandle {
            shutdown,
            task: Some(task),
            lifecycle_sender: lifecycle,
            lifecycle: lifecycle_rx,
            runtime_status_sender: runtime_status,
            runtime_status: runtime_status_rx,
            events,
        })
    }

    fn into_options(self) -> Result<Options, NodeError> {
        self.config
            .into_options()
            .map_err(|error| NodeError::InvalidConfig(error.message))
    }
}

fn validate_runtime_options(options: Options) -> Result<Options, String> {
    validate_peer_options(&options)?;
    validate_storage_options(&options)?;
    validate_validation_options(&options)?;
    validate_api_options(&options)?;
    validate_index_options(&options)?;
    validate_inbound_options(&options)?;
    Ok(options)
}

fn validate_inbound_options(options: &Options) -> Result<(), String> {
    if options.inbound_listen.is_none() {
        return Ok(());
    }
    if options.data_dir.is_none()
        || options.once
        || options.validation_target.is_some()
        || options.offline_action.is_some()
    {
        return Err(
            "inbound P2P requires an ordinary persistent data-directory node without --once or an offline/fixed validation target"
                .to_owned(),
        );
    }
    let limits = options.inbound_limits;
    if !(1..=256).contains(&limits.max_connections) {
        return Err("maximum inbound peers must be between 1 and 256".to_owned());
    }
    if limits.max_connections_per_ip == 0 || limits.max_connections_per_ip > limits.max_connections
    {
        return Err(
            "maximum inbound peers per IP must be between 1 and the total inbound-peer limit"
                .to_owned(),
        );
    }
    if limits.max_upload_bytes_per_day != 0
        && !(1024 * 1024..=1024_u64.pow(4)).contains(&limits.max_upload_bytes_per_day)
    {
        return Err(
            "inbound daily upload target must be zero or between 1 MiB and 1 TiB".to_owned(),
        );
    }
    if !(60..=100_000).contains(&limits.max_requests_per_minute) {
        return Err("inbound requests per minute must be between 60 and 100000".to_owned());
    }
    Ok(())
}

fn validate_index_options(options: &Options) -> Result<(), String> {
    let enabled =
        options.indexes.transaction || options.indexes.spent_output || options.indexes.basic_filter;
    if !enabled {
        return Ok(());
    }
    if options.data_dir.is_none() {
        return Err("optional indexes require a data directory".to_owned());
    }
    if options.snapshot.is_some() || options.finalize_assumeutxo.is_some() {
        return Err(
            "optional indexes cannot be created during snapshot installation or finalization"
                .to_owned(),
        );
    }
    if options.complete_assumeutxo.is_some() || options.background_assumeutxo.is_some() {
        return Err(
            "optional indexes require an independently executed chainstate; finish AssumeUTXO validation first or use --reindex-chainstate with the desired indexes"
                .to_owned(),
        );
    }
    if options.offline_action.as_ref().is_some_and(|action| {
        !matches!(
            action,
            OfflineAction::ReindexFromFreezer { .. } | OfflineAction::ReindexChainstate { .. }
        )
    }) {
        return Err(
            "optional indexes conflict with this read-only or storage-maintenance action"
                .to_owned(),
        );
    }
    Ok(())
}

fn validate_peer_options(options: &Options) -> Result<(), String> {
    if options
        .data_dir
        .as_ref()
        .is_some_and(|path| path.as_os_str().is_empty())
    {
        return Err("data directory must not be empty".to_owned());
    }
    if options.remotes.len() > MAX_CONFIGURED_PEERS {
        return Err(format!(
            "at most {MAX_CONFIGURED_PEERS} preferred peers are supported"
        ));
    }
    if options.remotes.iter().collect::<HashSet<_>>().len() != options.remotes.len() {
        return Err("preferred peers must be unique".to_owned());
    }
    if let Some(seeds) = &options.dns_seeds {
        if seeds.len() > MAX_DNS_SEEDS {
            return Err(format!("at most {MAX_DNS_SEEDS} DNS seeds are supported"));
        }
        if seeds.iter().collect::<HashSet<_>>().len() != seeds.len() {
            return Err("DNS seeds must be unique".to_owned());
        }
    }
    if options.resources.automatic_hot_standbys > MAX_AUTOMATIC_HOT_STANDBYS {
        return Err(format!(
            "automatic hot standbys cannot exceed {MAX_AUTOMATIC_HOT_STANDBYS}"
        ));
    }
    Ok(())
}

fn validate_storage_options(options: &Options) -> Result<(), String> {
    if !(MIN_PRUNE_RETENTION_BLOCKS..=DEFAULT_RETENTION_BLOCKS)
        .contains(&options.ledger_retention.max_blocks)
    {
        return Err(format!(
            "prune block target must be between {MIN_PRUNE_RETENTION_BLOCKS} and {DEFAULT_RETENTION_BLOCKS}"
        ));
    }
    if options.ledger_retention.max_bytes < MIN_PRUNE_MAX_BYTES {
        return Err(format!(
            "prune byte target must be at least {MIN_PRUNE_MAX_BYTES}"
        ));
    }
    if !(MINIMUM_FREE_BYTES_FLOOR..=MAXIMUM_FREE_BYTES_RESERVE)
        .contains(&options.minimum_free_bytes)
    {
        return Err(format!(
            "minimum free-space reserve must be between {MINIMUM_FREE_BYTES_FLOOR} and {MAXIMUM_FREE_BYTES_RESERVE} bytes"
        ));
    }
    for (name, bytes) in [
        ("active chainstate", options.cache.active_chainstate_bytes),
        (
            "background chainstate",
            options.cache.background_chainstate_bytes,
        ),
        ("bulk validation", options.cache.bulk_validation_bytes),
    ] {
        if !(MIN_CHAINSTATE_CACHE_BYTES..=MAX_CHAINSTATE_CACHE_BYTES).contains(&bytes) {
            return Err(format!(
                "{name} cache must be between {MIN_CHAINSTATE_CACHE_BYTES} and {MAX_CHAINSTATE_CACHE_BYTES} bytes"
            ));
        }
    }
    if !(1..=MAX_CONFIGURED_MEMPOOL_TRANSACTIONS)
        .contains(&options.resources.mempool_max_transactions)
    {
        return Err(format!(
            "mempool transaction target must be between 1 and {MAX_CONFIGURED_MEMPOOL_TRANSACTIONS}"
        ));
    }
    if !(MAX_ADMITTED_TRANSACTION_BYTES..=MAX_CONFIGURED_MEMPOOL_BYTES)
        .contains(&options.resources.mempool_max_bytes)
    {
        return Err(format!(
            "mempool byte target must be between {MAX_ADMITTED_TRANSACTION_BYTES} and {MAX_CONFIGURED_MEMPOOL_BYTES}"
        ));
    }
    if !(MIN_LOG_FILE_BYTES..=MAX_LOG_FILE_BYTES).contains(&options.logging.max_file_bytes) {
        return Err(format!(
            "log file size must be between {MIN_LOG_FILE_BYTES} and {MAX_LOG_FILE_BYTES} bytes"
        ));
    }
    if !(MIN_LOG_FILES..=MAX_LOG_FILES).contains(&options.logging.max_files) {
        return Err(format!(
            "log file count must be between {MIN_LOG_FILES} and {MAX_LOG_FILES}"
        ));
    }
    if options
        .logging
        .directory
        .as_ref()
        .is_some_and(|directory| directory.as_os_str().is_empty())
    {
        return Err("log directory must not be empty".to_owned());
    }
    Ok(())
}

fn validate_validation_options(options: &Options) -> Result<(), String> {
    if options.validation_limits.max_blocks_per_batch == 0
        || options.validation_limits.max_blocks_per_batch > MAX_VALIDATION_BATCH_SIZE
    {
        return Err(format!(
            "validation batch size must be between 1 and {MAX_VALIDATION_BATCH_SIZE}"
        ));
    }
    if options.validation_limits.pause_between_batches
        > Duration::from_millis(MAX_VALIDATION_PAUSE_MS)
    {
        return Err(format!(
            "validation pause must be at most {MAX_VALIDATION_PAUSE_MS} milliseconds"
        ));
    }
    if options.complete_assumeutxo.is_some() && options.background_assumeutxo.is_some() {
        return Err(
            "sequential and background AssumeUTXO validation are mutually exclusive".to_owned(),
        );
    }
    if options.cleanup_validation_dir
        && options.complete_assumeutxo.is_none()
        && options.background_assumeutxo.is_none()
    {
        return Err(
            "validation-directory cleanup requires an AssumeUTXO validation mode".to_owned(),
        );
    }
    if let Some(validation_dir) = options
        .complete_assumeutxo
        .as_ref()
        .or(options.background_assumeutxo.as_ref())
    {
        if validation_dir.as_os_str().is_empty() {
            return Err("validation data directory must not be empty".to_owned());
        }
        if options.data_dir.as_ref() == Some(validation_dir) {
            return Err(
                "validation data directory must differ from active data directory".to_owned(),
            );
        }
    }
    Ok(())
}

fn validate_api_options(options: &Options) -> Result<(), String> {
    if options.complete_assumeutxo.is_some() && (options.once || options.explorer_listen.is_some())
    {
        return Err(
            "sequential AssumeUTXO completion conflicts with once and API service modes".to_owned(),
        );
    }
    if let Some(listen) = options.explorer_listen {
        if !listen.ip().is_loopback() {
            return Err("embedded APIs require a loopback listen address".to_owned());
        }
        if options.data_dir.is_none() {
            return Err("embedded APIs require a data directory".to_owned());
        }
    } else if options.wallet_api_files.is_some() || options.rpc_auth_token_file.is_some() {
        return Err("wallet and RPC configuration require an API listener".to_owned());
    }
    if options.mempool_full_rbf && options.data_dir.is_none() {
        return Err("full-RBF policy requires a data directory".to_owned());
    }
    Ok(())
}

/// Host-owned handle for an embedded node task.
pub struct NodeHandle {
    shutdown: watch::Sender<bool>,
    task: Option<tokio::task::JoinHandle<Result<(), NodeError>>>,
    lifecycle_sender: watch::Sender<NodeLifecycle>,
    lifecycle: watch::Receiver<NodeLifecycle>,
    runtime_status_sender: watch::Sender<NodeRuntimeStatus>,
    runtime_status: watch::Receiver<NodeRuntimeStatus>,
    events: broadcast::Sender<NodeEvent>,
}

impl NodeHandle {
    /// Returns a cloneable shutdown controller that can be retained while the
    /// handle's `wait` future is owned by a host task executor.
    #[must_use]
    pub fn controller(&self) -> NodeController {
        NodeController {
            shutdown: self.shutdown.clone(),
            lifecycle: self.lifecycle.clone(),
            runtime_status: self.runtime_status.clone(),
            events: self.events.clone(),
        }
    }

    /// Returns the latest host-visible task lifecycle state.
    #[must_use]
    pub fn lifecycle(&self) -> NodeLifecycle {
        self.lifecycle.borrow().clone()
    }

    /// Subscribes to latest-value lifecycle changes without an unbounded queue.
    #[must_use]
    pub fn subscribe_lifecycle(&self) -> watch::Receiver<NodeLifecycle> {
        self.lifecycle.clone()
    }

    /// Returns the latest typed subsystem status.
    #[must_use]
    pub fn status(&self) -> NodeRuntimeStatus {
        self.runtime_status.borrow().clone()
    }

    /// Subscribes to latest-value subsystem status changes.
    #[must_use]
    pub fn subscribe_status(&self) -> watch::Receiver<NodeRuntimeStatus> {
        self.runtime_status.clone()
    }

    /// Subscribes to the bounded lifecycle event stream.
    ///
    /// A lagging receiver observes Tokio's `Lagged` error rather than retaining
    /// unbounded history in the node.
    #[must_use]
    pub fn subscribe_events(&self) -> broadcast::Receiver<NodeEvent> {
        self.events.subscribe()
    }

    /// Requests graceful shutdown without consuming the handle.
    pub fn request_shutdown(&self) {
        request_node_shutdown(&self.shutdown, &self.events);
    }

    /// Requests shutdown and waits for durable work to close.
    pub async fn shutdown(mut self) -> Result<(), NodeError> {
        self.request_shutdown();
        self.join().await
    }

    /// Waits until the node exits on its own or fails.
    pub async fn wait(mut self) -> Result<(), NodeError> {
        self.join().await
    }

    async fn join(&mut self) -> Result<(), NodeError> {
        let task = self.task.take().ok_or(NodeError::ControlChannelClosed)?;
        match task.await {
            Ok(result) => result,
            Err(error) => {
                let error = NodeError::Task(error.to_string());
                let message = error.to_string();
                self.lifecycle_sender
                    .send_replace(NodeLifecycle::Failed(message.clone()));
                let mut status = self.runtime_status.borrow().clone();
                status.last_error = Some(message.clone());
                self.runtime_status_sender.send_replace(status);
                let _ = self.events.send(NodeEvent::Failed { message });
                Err(error)
            }
        }
    }
}

/// Cloneable control plane for a node whose wait future is host-owned.
#[derive(Clone)]
pub struct NodeController {
    shutdown: watch::Sender<bool>,
    lifecycle: watch::Receiver<NodeLifecycle>,
    runtime_status: watch::Receiver<NodeRuntimeStatus>,
    events: broadcast::Sender<NodeEvent>,
}

impl NodeController {
    /// Returns the latest host-visible task lifecycle state.
    #[must_use]
    pub fn lifecycle(&self) -> NodeLifecycle {
        self.lifecycle.borrow().clone()
    }

    /// Subscribes to latest-value lifecycle changes without an unbounded queue.
    #[must_use]
    pub fn subscribe_lifecycle(&self) -> watch::Receiver<NodeLifecycle> {
        self.lifecycle.clone()
    }

    /// Returns the latest typed subsystem status.
    #[must_use]
    pub fn status(&self) -> NodeRuntimeStatus {
        self.runtime_status.borrow().clone()
    }

    /// Subscribes to latest-value subsystem status changes.
    #[must_use]
    pub fn subscribe_status(&self) -> watch::Receiver<NodeRuntimeStatus> {
        self.runtime_status.clone()
    }

    /// Subscribes to the bounded lifecycle event stream.
    #[must_use]
    pub fn subscribe_events(&self) -> broadcast::Receiver<NodeEvent> {
        self.events.subscribe()
    }

    /// Requests graceful shutdown at the next safe checkpoint boundary.
    pub fn request_shutdown(&self) {
        request_node_shutdown(&self.shutdown, &self.events);
    }
}

impl Drop for NodeHandle {
    fn drop(&mut self) {
        if !matches!(
            &*self.lifecycle.borrow(),
            NodeLifecycle::Stopped | NodeLifecycle::Failed(_)
        ) {
            request_node_shutdown(&self.shutdown, &self.events);
        }
    }
}

fn request_node_shutdown(shutdown: &watch::Sender<bool>, events: &broadcast::Sender<NodeEvent>) {
    if shutdown.send_if_modified(|requested| {
        if *requested {
            false
        } else {
            *requested = true;
            true
        }
    }) {
        let _ = events.send(NodeEvent::ShutdownRequested);
    }
}

/// Latest-value lifecycle state for a host-owned node task.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum NodeLifecycle {
    /// The task has been spawned but has not begun polling the node runtime.
    Starting,
    /// The node runtime is active; network readiness is reported separately.
    Running,
    /// Shutdown was accepted and in-flight durable work is draining.
    Stopping,
    /// The node exited successfully or completed graceful shutdown.
    Stopped,
    /// The node exited with a validation, storage, network, or task error.
    Failed(String),
}

/// Typed durable tip exposed to an embedding host.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NodeTip {
    /// Active height.
    pub height: u32,
    /// Active block hash.
    pub hash: BlockHash,
}

/// Current freezer footprint and retrievable range.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct NodeFreezerStatus {
    /// Immutable segments in the durable live index.
    pub segments: u32,
    /// Locally retrievable blocks.
    pub blocks: u32,
    /// Compressed bytes referenced by the live index.
    pub bytes: u64,
    /// Oldest retrievable height.
    pub first_height: Option<u32>,
    /// Newest retrievable height.
    pub tip_height: Option<u32>,
    /// Configured maximum retained block count.
    pub target_blocks: u32,
    /// Configured maximum compressed bytes.
    pub target_bytes: u64,
    /// Highest height physically pruned from the active retained prefix.
    pub pruned_through_height: Option<u32>,
}

/// Filesystem capacity and atomic-checkpoint safety forecast.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct NodeDiskStatus {
    /// Total filesystem bytes.
    pub total_bytes: u64,
    /// Currently available filesystem bytes.
    pub available_bytes: u64,
    /// Minimum bytes required before another atomic checkpoint may start.
    pub required_bytes: u64,
    /// Operator-selected reserve included in the required threshold.
    pub reserve_bytes: u64,
    /// Bounded freezer/mempool/log ceiling plus current optional-index bytes.
    pub projected_storage_ceiling_bytes: u64,
    /// Current bytes occupied by enabled optional index databases.
    pub optional_index_bytes: u64,
    /// Worst-case optional-index copy-on-write headroom for one configured batch.
    pub optional_index_checkpoint_headroom_bytes: u64,
}

/// Initial-download and snapshot trust state.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct NodeTrustStatus {
    /// Whether active work meets the configured floor.
    pub minimum_chainwork_reached: bool,
    /// Active assume-valid anchor height, when known.
    pub active_assume_valid_height: Option<u32>,
    /// Whether every connected block still receives full script validation.
    pub full_script_validation: bool,
    /// Whether independent genesis replay has cleared any assumed snapshot.
    pub independently_validated: bool,
}

/// Latest-value status for one isolated embedded node instance.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeRuntimeStatus {
    /// Selected network.
    pub network: Network,
    /// Current active outbound peer.
    pub active_peer: Option<SocketAddr>,
    /// Maximum-work header tip.
    pub header: Option<NodeTip>,
    /// Durable executed-chain tip.
    pub execution: Option<NodeTip>,
    /// Optional explorer projection tip.
    pub explorer: Option<NodeTip>,
    /// Optional wallet projection tip.
    pub wallet: Option<NodeTip>,
    /// Optional transaction index tip.
    pub transaction_index: Option<NodeTip>,
    /// Optional spent-output index tip.
    pub spent_output_index: Option<NodeTip>,
    /// Optional BIP157/158 basic-filter index tip.
    pub basic_filter_index: Option<NodeTip>,
    /// UTXOs in the recent-access tier.
    pub utxo_hot: u64,
    /// UTXOs in the inactive tier.
    pub utxo_cold: u64,
    /// Current bounded freezer footprint.
    pub freezer: NodeFreezerStatus,
    /// Current disk-capacity safety forecast.
    pub disk: NodeDiskStatus,
    /// Current trust/validation state.
    pub trust: NodeTrustStatus,
    /// Most recent terminal task error.
    pub last_error: Option<String>,
}

impl NodeRuntimeStatus {
    const fn starting(network: Network) -> Self {
        Self {
            network,
            active_peer: None,
            header: None,
            execution: None,
            explorer: None,
            wallet: None,
            transaction_index: None,
            spent_output_index: None,
            basic_filter_index: None,
            utxo_hot: 0,
            utxo_cold: 0,
            freezer: NodeFreezerStatus {
                segments: 0,
                blocks: 0,
                bytes: 0,
                first_height: None,
                tip_height: None,
                target_blocks: 0,
                target_bytes: 0,
                pruned_through_height: None,
            },
            disk: NodeDiskStatus {
                total_bytes: 0,
                available_bytes: 0,
                required_bytes: 0,
                reserve_bytes: 0,
                projected_storage_ceiling_bytes: 0,
                optional_index_bytes: 0,
                optional_index_checkpoint_headroom_bytes: 0,
            },
            trust: NodeTrustStatus {
                minimum_chainwork_reached: false,
                active_assume_valid_height: None,
                full_script_validation: true,
                independently_validated: false,
            },
            last_error: None,
        }
    }
}

/// Bounded edge-triggered runtime delta for host observability.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum NodeEvent {
    /// The host task began polling the node runtime.
    Started,
    /// A controller requested graceful shutdown.
    ShutdownRequested,
    /// An active outbound peer was selected.
    PeerConnected {
        /// Remote peer address.
        address: SocketAddr,
    },
    /// The active outbound peer session ended.
    PeerDisconnected {
        /// Remote peer address.
        address: SocketAddr,
    },
    /// Maximum-work header selection advanced or changed.
    HeaderTipChanged {
        /// New active header tip.
        tip: NodeTip,
    },
    /// Durable block execution advanced.
    ExecutionTipChanged {
        /// New durable execution tip.
        tip: NodeTip,
    },
    /// A stale execution branch was disconnected.
    Reorganized {
        /// Tip before the disconnect.
        previous: NodeTip,
        /// Tip immediately after the disconnect.
        current: NodeTip,
    },
    /// A rebuildable projection advanced.
    IndexTipChanged {
        /// Stable index name (`explorer` or `wallet`).
        index: &'static str,
        /// New durable projection tip.
        tip: NodeTip,
    },
    /// Freezer retention or physical footprint changed.
    FreezerChanged {
        /// New durable freezer state.
        status: NodeFreezerStatus,
    },
    /// Disk availability or checkpoint threshold changed.
    DiskSpaceChanged {
        /// New host-visible disk forecast.
        status: NodeDiskStatus,
    },
    /// The node exited successfully or completed graceful shutdown.
    Stopped,
    /// The node exited with an error.
    Failed {
        /// Stable human-readable error text; no credentials are included.
        message: String,
    },
}

#[derive(Clone)]
struct NodeObserver {
    status: watch::Sender<NodeRuntimeStatus>,
    events: broadcast::Sender<NodeEvent>,
}

impl NodeObserver {
    fn peer_connected(&self, address: SocketAddr) {
        let mut status = self.status.borrow().clone();
        if status.active_peer != Some(address) {
            status.active_peer = Some(address);
            self.status.send_replace(status);
            let _ = self.events.send(NodeEvent::PeerConnected { address });
        }
    }

    fn peer_disconnected(&self, address: SocketAddr) {
        let mut status = self.status.borrow().clone();
        if status.active_peer == Some(address) {
            status.active_peer = None;
            self.status.send_replace(status);
            let _ = self.events.send(NodeEvent::PeerDisconnected { address });
        }
    }

    fn publish(&self, mut next: NodeRuntimeStatus) {
        let previous = self.status.borrow().clone();
        next.active_peer = previous.active_peer;
        next.last_error = previous.last_error;
        if previous.header != next.header {
            if let Some(tip) = next.header {
                let _ = self.events.send(NodeEvent::HeaderTipChanged { tip });
            }
        }
        if previous.execution != next.execution {
            if let Some(tip) = next.execution {
                let _ = self.events.send(NodeEvent::ExecutionTipChanged { tip });
            }
        }
        if previous.explorer != next.explorer {
            if let Some(tip) = next.explorer {
                let _ = self.events.send(NodeEvent::IndexTipChanged {
                    index: "explorer",
                    tip,
                });
            }
        }
        if previous.wallet != next.wallet {
            if let Some(tip) = next.wallet {
                let _ = self.events.send(NodeEvent::IndexTipChanged {
                    index: "wallet",
                    tip,
                });
            }
        }
        for (changed, index, tip) in [
            (
                previous.transaction_index != next.transaction_index,
                "txindex",
                next.transaction_index,
            ),
            (
                previous.spent_output_index != next.spent_output_index,
                "spent-output",
                next.spent_output_index,
            ),
            (
                previous.basic_filter_index != next.basic_filter_index,
                "basic-filter",
                next.basic_filter_index,
            ),
        ] {
            if changed {
                if let Some(tip) = tip {
                    let _ = self.events.send(NodeEvent::IndexTipChanged { index, tip });
                }
            }
        }
        if previous.freezer != next.freezer {
            let _ = self.events.send(NodeEvent::FreezerChanged {
                status: next.freezer,
            });
        }
        if previous.disk != next.disk {
            let _ = self
                .events
                .send(NodeEvent::DiskSpaceChanged { status: next.disk });
        }
        self.status.send_replace(next);
    }

    fn reorganized(&self, previous: NodeTip, current: NodeTip) {
        let _ = self
            .events
            .send(NodeEvent::Reorganized { previous, current });
    }

    fn failed(&self, message: String) {
        let mut status = self.status.borrow().clone();
        status.last_error = Some(message);
        self.status.send_replace(status);
    }
}

/// Embedded node configuration, runtime, or task failure.
#[derive(Debug, Error)]
pub enum NodeError {
    /// The builder configuration violates a bounded runtime contract.
    #[error("invalid node configuration: {0}")]
    InvalidConfig(String),
    /// Launch must occur from the host application's Tokio runtime.
    #[error("embedded node launch requires an active Tokio runtime")]
    MissingRuntime,
    /// The node runtime returned a validation, storage, or network failure.
    #[error("node runtime: {0}")]
    Runtime(String),
    /// The host task could not be joined.
    #[error("node task: {0}")]
    Task(String),
    /// The host dropped or reused the control channel unexpectedly.
    #[error("node control channel is closed")]
    ControlChannelClosed,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum OfflineAction {
    UtxoActivityReport,
    RetierUtxos {
        hot_window_blocks: u32,
    },
    DownloadCoreSnapshot(SnapshotDownloadConfig),
    VerifyStorage {
        max_segments: u32,
        max_bytes: u64,
    },
    VerifyChain {
        depth: u32,
        max_block_bytes: u64,
    },
    ReindexFromFreezer {
        output: PathBuf,
    },
    ReindexChainstate {
        output: PathBuf,
    },
    PruneStorage {
        through_height: u32,
        plan_token: Option<String>,
    },
}

#[derive(Debug, serde::Serialize)]
struct OfflineVerifyChainReport {
    schema_version: u32,
    network: String,
    exclusive: bool,
    database_open_mode: &'static str,
    header_records: u64,
    header_height: u32,
    header_hash: String,
    chainwork: String,
    execution_height: u32,
    execution_hash: String,
    assumed_snapshot_height: Option<u32>,
    independently_validated: bool,
    utxo_hot: u64,
    utxo_cold: u64,
    freezer_first_height: Option<u32>,
    freezer_tip_height: Option<u32>,
    requested_depth: u32,
    checked_first_height: Option<u32>,
    checked_blocks: u32,
    checked_block_record_bytes: u64,
    checked_undos: u32,
    clean: bool,
    issues: Vec<String>,
    repair_plan: Vec<String>,
}

#[derive(Debug)]
struct FreezerCrossCheck {
    first_height: Option<u32>,
    tip_height: Option<u32>,
    checked_first_height: Option<u32>,
    checked_blocks: u32,
    checked_block_record_bytes: u64,
    checked_undos: u32,
    issues: Vec<String>,
    repair_plan: Vec<String>,
}

#[derive(Debug, serde::Serialize)]
struct FreezerReindexReport {
    schema_version: u32,
    network: String,
    source: String,
    output: String,
    target_height: u32,
    target_hash: String,
    executed_blocks: u32,
    validation_batch_size: usize,
    elapsed_seconds: u64,
    independently_validated: bool,
    source_freezer_complete: bool,
    promoted: bool,
}

#[derive(Debug, serde::Serialize)]
struct PeerReindexReport {
    schema_version: u32,
    network: String,
    source: String,
    output: String,
    target_height: u32,
    target_hash: String,
    elapsed_seconds: u64,
    headers_authenticated_by_work: bool,
    blocks_authenticated_by_consensus: bool,
    promoted: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum NetworkExecutionMode {
    Persistent,
    ExperimentalOnce,
    ExperimentalOnceExtend,
}

impl NetworkExecutionMode {
    const fn is_experimental(self) -> bool {
        matches!(self, Self::ExperimentalOnce | Self::ExperimentalOnceExtend)
    }

    const fn extends_validation_target(self) -> bool {
        matches!(self, Self::ExperimentalOnceExtend)
    }
}

#[derive(Clone)]
enum SnapshotActivationOptions {
    Rbtc {
        path: PathBuf,
        height: u32,
        block_hash: BlockHash,
        utxo_count: u64,
        records_bytes: u64,
        records_sha256: String,
    },
    Core(PathBuf),
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
    quick_repair: bool,
}

#[derive(Default)]
struct PrefetchedBlocks {
    serialized: Vec<Vec<u8>>,
}

impl Default for ValidationLimits {
    fn default() -> Self {
        Self {
            max_blocks_per_batch: DEFAULT_VALIDATION_BATCH_SIZE,
            pause_between_batches: Duration::ZERO,
            quick_repair: true,
        }
    }
}

fn active_assumeutxo_limits(configured: ValidationLimits) -> ValidationLimits {
    ValidationLimits {
        max_blocks_per_batch: configured.max_blocks_per_batch,
        quick_repair: configured.quick_repair,
        ..ValidationLimits::default()
    }
}

const fn chainstate_cache_bytes(
    network_execution: NetworkExecutionMode,
    background_pipeline: bool,
    cache: NodeCacheConfig,
) -> usize {
    if background_pipeline {
        cache.background_chainstate_bytes
    } else if network_execution.is_experimental() {
        cache.bulk_validation_bytes
    } else {
        cache.active_chainstate_bytes
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct DnsSeed {
    host: String,
    port: u16,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PeerFailureKind {
    LocalResource,
    Transient,
    ProtocolViolation,
}

#[derive(Debug)]
struct PeerRunError {
    kind: PeerFailureKind,
    message: String,
}

impl PeerRunError {
    fn local(message: impl Into<String>) -> Self {
        Self {
            kind: PeerFailureKind::LocalResource,
            message: message.into(),
        }
    }

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

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize)]
#[allow(clippy::struct_field_names)]
struct DiskSpaceSnapshot {
    total_bytes: u64,
    available_bytes: u64,
    required_bytes: u64,
    reserve_bytes: u64,
    projected_storage_ceiling_bytes: u64,
    optional_index_bytes: u64,
    optional_index_checkpoint_headroom_bytes: u64,
}

#[derive(Clone, Debug)]
struct DiskSpaceMonitor {
    data_dir: PathBuf,
    required_bytes: u64,
    reserve_bytes: u64,
    projected_storage_ceiling_bytes: u64,
    optional_index_checkpoint_headroom_bytes: u64,
}

impl DiskSpaceMonitor {
    fn for_options(options: &Options, data_dir: PathBuf) -> Self {
        let checkpoint_bytes = u64::try_from(options.validation_limits.max_blocks_per_batch)
            .unwrap_or(u64::MAX)
            .saturating_mul(MAX_SERIALIZED_BLOCK_BYTES)
            .saturating_mul(2);
        let mempool_bytes = u64::try_from(options.resources.mempool_max_bytes).unwrap_or(u64::MAX);
        let enabled_indexes = u64::from(options.indexes.transaction)
            + u64::from(options.indexes.spent_output)
            + u64::from(options.indexes.basic_filter);
        let optional_index_checkpoint_headroom_bytes =
            u64::try_from(options.validation_limits.max_blocks_per_batch)
                .unwrap_or(u64::MAX)
                .saturating_mul(MAX_SERIALIZED_BLOCK_BYTES)
                .saturating_mul(enabled_indexes);
        let required_bytes = options
            .minimum_free_bytes
            .saturating_add(checkpoint_bytes)
            .saturating_add(mempool_bytes)
            .saturating_add(options.logging.max_file_bytes)
            .saturating_add(optional_index_checkpoint_headroom_bytes)
            .saturating_add(DATABASE_COMMIT_HEADROOM_BYTES);
        let log_ceiling = options
            .logging
            .max_file_bytes
            .saturating_mul(u64::from(options.logging.max_files));
        let projected_storage_ceiling_bytes = options
            .ledger_retention
            .max_bytes
            .saturating_add(mempool_bytes)
            .saturating_add(log_ceiling);
        Self {
            data_dir,
            required_bytes,
            reserve_bytes: options.minimum_free_bytes,
            projected_storage_ceiling_bytes,
            optional_index_checkpoint_headroom_bytes,
        }
    }

    fn check(&self) -> Result<DiskSpaceSnapshot, PeerRunError> {
        let mut optional_index_bytes = 0_u64;
        for name in [
            "txindex.redb",
            "spent-output-index.redb",
            "basic-filter-index.redb",
        ] {
            match fs::symlink_metadata(self.data_dir.join(name)) {
                Ok(metadata) if metadata.is_file() && !metadata.file_type().is_symlink() => {
                    optional_index_bytes = optional_index_bytes.saturating_add(metadata.len());
                }
                Ok(_) => {
                    return Err(PeerRunError::local(format!(
                        "optional index path {} must be a regular non-symlink file",
                        self.data_dir.join(name).display()
                    )));
                }
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => {
                    return Err(PeerRunError::local(format!(
                        "inspect optional index path {}: {error}",
                        self.data_dir.join(name).display()
                    )));
                }
            }
        }
        let available_bytes = fs2::available_space(&self.data_dir).map_err(|error| {
            PeerRunError::local(format!(
                "read available disk space for {}: {error}",
                self.data_dir.display()
            ))
        })?;
        let total_bytes = fs2::total_space(&self.data_dir).map_err(|error| {
            PeerRunError::local(format!(
                "read total disk space for {}: {error}",
                self.data_dir.display()
            ))
        })?;
        let snapshot = DiskSpaceSnapshot {
            total_bytes,
            available_bytes,
            required_bytes: self.required_bytes,
            reserve_bytes: self.reserve_bytes,
            projected_storage_ceiling_bytes: self
                .projected_storage_ceiling_bytes
                .saturating_add(optional_index_bytes),
            optional_index_bytes,
            optional_index_checkpoint_headroom_bytes: self.optional_index_checkpoint_headroom_bytes,
        };
        if available_bytes < self.required_bytes {
            return Err(PeerRunError::local(format!(
                "insufficient disk space for atomic checkpoint in {}: available={} required={} reserve={}",
                self.data_dir.display(),
                available_bytes,
                self.required_bytes,
                self.reserve_bytes
            )));
        }
        Ok(snapshot)
    }
}

#[derive(Debug)]
struct DataDirectoryLock {
    file: fs::File,
}

impl DataDirectoryLock {
    fn acquire(data_dir: &std::path::Path, network: Network) -> Result<Self, String> {
        let path = data_dir.join(DATA_DIRECTORY_LOCK_FILE);
        match fs::symlink_metadata(&path) {
            Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_file() => {
                return Err(format!(
                    "data-directory lock {} must be a regular file, not a symlink",
                    path.display()
                ));
            }
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(format!(
                    "inspect data-directory lock {}: {error}",
                    path.display()
                ));
            }
        }
        let mut options = fs::OpenOptions::new();
        options.create(true).read(true).write(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options
            .open(&path)
            .map_err(|error| format!("open data-directory lock {}: {error}", path.display()))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::{MetadataExt, PermissionsExt};
            if file
                .metadata()
                .map_err(|error| format!("inspect data-directory lock: {error}"))?
                .nlink()
                != 1
            {
                return Err("data-directory lock must have exactly one hard link".to_owned());
            }
            fs::set_permissions(&path, fs::Permissions::from_mode(0o600))
                .map_err(|error| format!("restrict data-directory lock permissions: {error}"))?;
        }
        if let Err(error) = file.try_lock_exclusive() {
            if error.kind() == io::ErrorKind::WouldBlock {
                let marker = read_lock_marker(&mut file);
                return Err(format!(
                    "data directory {} is already locked{}",
                    data_dir.display(),
                    marker.map_or_else(String::new, |marker| format!(" ({marker})"))
                ));
            }
            return Err(format!(
                "lock data directory {}: {error}",
                data_dir.display()
            ));
        }
        let marker = format!(
            "pid={} network={} started_unix={}\n",
            std::process::id(),
            network,
            unix_time().unwrap_or(0)
        );
        file.set_len(0)
            .and_then(|()| file.rewind())
            .and_then(|()| file.write_all(marker.as_bytes()))
            .and_then(|()| file.sync_data())
            .map_err(|error| {
                format!(
                    "persist data-directory lock marker {}: {error}",
                    path.display()
                )
            })?;
        Ok(Self { file })
    }
}

impl Drop for DataDirectoryLock {
    fn drop(&mut self) {
        let _ = FileExt::unlock(&self.file);
    }
}

#[derive(Debug)]
struct DataDirectoryReadLock {
    file: fs::File,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
struct DataStoreSchemaVersions {
    headers: u32,
    chainstate: u32,
    freezer: u32,
    peers: u32,
    mempool: u32,
    fee_estimates: u32,
    explorer: u32,
    wallet: u32,
    rebroadcast: u32,
    #[serde(default = "data_store_schema_v1")]
    transaction_index: u32,
    #[serde(default = "data_store_schema_v1")]
    spent_output_index: u32,
    #[serde(default = "data_store_schema_v1")]
    basic_filter_index: u32,
}

const fn data_store_schema_v1() -> u32 {
    1
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
struct DataFormatManifest {
    schema_version: u32,
    minimum_reader_version: u32,
    network: String,
    stores: DataStoreSchemaVersions,
}

impl DataFormatManifest {
    fn current(network: Network) -> Self {
        Self {
            schema_version: DATA_FORMAT_SCHEMA_VERSION,
            minimum_reader_version: DATA_FORMAT_SCHEMA_VERSION,
            network: network.to_string(),
            stores: DataStoreSchemaVersions {
                headers: 1,
                chainstate: 1,
                freezer: 1,
                peers: 1,
                mempool: 1,
                fee_estimates: 1,
                explorer: 1,
                wallet: 1,
                rebroadcast: 1,
                transaction_index: 1,
                spent_output_index: 1,
                basic_filter_index: 2,
            },
        }
    }

    fn version2(network: Network) -> Self {
        let mut manifest = Self::current(network);
        manifest.schema_version = 2;
        manifest.minimum_reader_version = 2;
        manifest.stores.basic_filter_index = 1;
        manifest
    }
}

impl DataDirectoryReadLock {
    fn acquire(data_dir: &std::path::Path) -> Result<Self, String> {
        let path = data_dir.join(DATA_DIRECTORY_LOCK_FILE);
        let metadata = fs::symlink_metadata(&path).map_err(|error| {
            format!(
                "read-only storage audit requires the existing data-directory lock {}: {error}",
                path.display()
            )
        })?;
        if metadata.file_type().is_symlink() || !metadata.is_file() {
            return Err(format!(
                "data-directory lock {} must be a regular file, not a symlink",
                path.display()
            ));
        }
        #[cfg(unix)]
        if std::os::unix::fs::MetadataExt::nlink(&metadata) != 1 {
            return Err("data-directory lock must have exactly one hard link".to_owned());
        }
        let file = fs::File::open(&path).map_err(|error| {
            format!(
                "open data-directory lock {} read-only: {error}",
                path.display()
            )
        })?;
        if let Err(error) = FileExt::try_lock_shared(&file) {
            if error.kind() == io::ErrorKind::WouldBlock {
                return Err(format!(
                    "data directory {} is in use; stop the node before a storage audit",
                    data_dir.display()
                ));
            }
            return Err(format!(
                "lock data directory {} for read-only audit: {error}",
                data_dir.display()
            ));
        }
        Ok(Self { file })
    }
}

impl Drop for DataDirectoryReadLock {
    fn drop(&mut self) {
        let _ = FileExt::unlock(&self.file);
    }
}

fn validate_data_format_manifest(
    data_dir: &std::path::Path,
    network: Network,
) -> Result<bool, String> {
    let path = data_dir.join(DATA_FORMAT_MANIFEST_FILE);
    let metadata = match fs::symlink_metadata(&path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(true),
        Err(error) => {
            return Err(format!(
                "inspect data-format manifest {}: {error}",
                path.display()
            ));
        }
    };
    if metadata.file_type().is_symlink()
        || !metadata.is_file()
        || metadata.len() > MAX_DATA_FORMAT_MANIFEST_BYTES
    {
        return Err(format!(
            "data-format manifest {} must be a bounded regular file",
            path.display()
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        if metadata.nlink() != 1 || metadata.permissions().mode() & 0o077 != 0 {
            return Err(
                "data-format manifest must be owner-only with exactly one hard link".to_owned(),
            );
        }
    }
    let mut bytes = Vec::new();
    fs::File::open(&path)
        .and_then(|file| {
            file.take(MAX_DATA_FORMAT_MANIFEST_BYTES.saturating_add(1))
                .read_to_end(&mut bytes)
        })
        .map_err(|error| format!("read data-format manifest {}: {error}", path.display()))?;
    if u64::try_from(bytes.len()).unwrap_or(u64::MAX) > MAX_DATA_FORMAT_MANIFEST_BYTES {
        return Err("data-format manifest exceeds its byte bound".to_owned());
    }
    let actual: DataFormatManifest = serde_json::from_slice(&bytes)
        .map_err(|error| format!("decode data-format manifest: {error}"))?;
    if actual.schema_version == 1 && actual.minimum_reader_version == 1 {
        let mut legacy = DataFormatManifest::version2(network);
        legacy.schema_version = 1;
        legacy.minimum_reader_version = 1;
        if actual == legacy {
            return Ok(true);
        }
        return Err(format!(
            "data-format manifest does not match the {network} legacy schema inventory"
        ));
    }
    if actual.schema_version == 2 && actual.minimum_reader_version == 2 {
        if actual != DataFormatManifest::version2(network) {
            return Err(format!(
                "data-format manifest does not match the {network} version-2 schema inventory"
            ));
        }
        if data_dir.join("basic-filter-index.redb").exists() {
            return Err(
                "version-1 basic-filter index omitted the genesis filter; rebuild it with a complete freezer or --reindex-chainstate"
                    .to_owned(),
            );
        }
        return Ok(true);
    }
    if actual.schema_version != DATA_FORMAT_SCHEMA_VERSION
        || actual.minimum_reader_version != DATA_FORMAT_SCHEMA_VERSION
    {
        return Err(format!(
            "unsupported data-format schema version {} (minimum reader {})",
            actual.schema_version, actual.minimum_reader_version
        ));
    }
    let expected = DataFormatManifest::current(network);
    if actual != expected {
        return Err(format!(
            "data-format manifest does not match the {network} schema inventory"
        ));
    }
    Ok(false)
}

fn publish_data_format_manifest(
    data_dir: &std::path::Path,
    network: Network,
) -> Result<(), String> {
    let path = data_dir.join(DATA_FORMAT_MANIFEST_FILE);
    match fs::symlink_metadata(&path) {
        Ok(_) if !validate_data_format_manifest(data_dir, network)? => return Ok(()),
        Ok(_) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(format!(
                "inspect data-format manifest before publish: {error}"
            ));
        }
    }
    let temporary = data_dir.join(".rbtc-data-format.json.new");
    match fs::symlink_metadata(&temporary) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_file() => {
            return Err("temporary data-format manifest must be a regular file".to_owned());
        }
        #[cfg(unix)]
        Ok(metadata) if std::os::unix::fs::MetadataExt::nlink(&metadata) != 1 => {
            return Err("temporary data-format manifest must not be hard-linked".to_owned());
        }
        Ok(_) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(format!("inspect temporary data-format manifest: {error}")),
    }
    let mut options = fs::OpenOptions::new();
    options.create(true).write(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options
        .open(&temporary)
        .map_err(|error| format!("create temporary data-format manifest: {error}"))?;
    #[cfg(unix)]
    fs::set_permissions(
        &temporary,
        std::os::unix::fs::PermissionsExt::from_mode(0o600),
    )
    .map_err(|error| format!("restrict temporary data-format manifest: {error}"))?;
    file.write_all(
        &serde_json::to_vec(&DataFormatManifest::current(network))
            .expect("data-format manifest serialization is infallible"),
    )
    .and_then(|()| file.sync_all())
    .map_err(|error| format!("persist temporary data-format manifest: {error}"))?;
    drop(file);
    fs::rename(&temporary, &path)
        .map_err(|error| format!("publish data-format manifest: {error}"))?;
    fs::File::open(data_dir)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| format!("sync data-format manifest directory: {error}"))
}

fn read_lock_marker(file: &mut fs::File) -> Option<String> {
    file.rewind().ok()?;
    let mut marker = String::new();
    file.take(MAX_DATA_DIRECTORY_LOCK_MARKER_BYTES)
        .read_to_string(&mut marker)
        .ok()?;
    let marker = marker.trim();
    (!marker.is_empty()
        && marker
            .chars()
            .all(|character| character.is_ascii_graphic() || character == ' '))
    .then(|| marker.to_owned())
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
    broadcast_sender: mpsc::Sender<WalletBroadcastRequest>,
    broadcast_receiver: Arc<tokio::sync::Mutex<mpsc::Receiver<WalletBroadcastRequest>>>,
    pending_broadcast: Arc<tokio::sync::Mutex<Option<WalletBroadcastRequest>>>,
    compact_candidates: CompactTransactionCandidates,
    rebroadcast: Arc<RedbRebroadcastStore>,
}

#[derive(Default)]
struct CompactTransactionCandidates {
    transactions: Mutex<VecDeque<Transaction>>,
}

impl CompactTransactionCandidates {
    fn insert(&self, transaction: Transaction) {
        let wtxid = transaction.compute_wtxid();
        let mut transactions = self
            .transactions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        transactions.retain(|candidate| candidate.compute_wtxid() != wtxid);
        transactions.push_back(transaction);
        while transactions.len() > MAX_COMPACT_TRANSACTION_CANDIDATES {
            transactions.pop_front();
        }
    }

    fn snapshot(&self) -> Vec<Transaction> {
        self.transactions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .cloned()
            .collect()
    }

    fn replace(&self, transactions: impl IntoIterator<Item = Transaction>) {
        let mut current = self
            .transactions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        current.clear();
        for transaction in transactions {
            let wtxid = transaction.compute_wtxid();
            current.retain(|candidate| candidate.compute_wtxid() != wtxid);
            current.push_back(transaction);
            while current.len() > MAX_COMPACT_TRANSACTION_CANDIDATES {
                current.pop_front();
            }
        }
    }
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
    transaction_index: Option<NodeTipResponse>,
    spent_output_index: Option<NodeTipResponse>,
    basic_filter_index: Option<NodeTipResponse>,
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
    ledger_target_blocks: u32,
    ledger_target_bytes: u64,
    ledger_pruned_through_height: Option<u32>,
    disk_space: DiskSpaceSnapshot,
}

#[derive(Clone)]
struct NodeStatus {
    started: Instant,
    progress: Arc<Mutex<NodeStatusProgress>>,
    inbound: Option<Arc<InboundStats>>,
}

struct NodeRpcOperator {
    status: NodeStatus,
    transaction_pool: Arc<Mutex<TransactionAdmissionPool>>,
    runtime_control: Arc<RuntimeControl>,
    active_peer: Option<SocketAddr>,
    auxiliary_indexes: Arc<AuxiliaryIndexes>,
}

const NODE_OPERATOR_RPC_METHODS: &[&str] = &[
    "getblockchaininfo",
    "getnetworkinfo",
    "getpeerinfo",
    "getmempoolinfo",
    "getindexinfo",
    "gettxindexlocation",
    "gettxspendingprevout",
    "getblockfilter",
    "verifychain",
    "getloginfo",
    "getdiskinfo",
    "setloglevel",
    "stop",
];

impl LocalRpcOperator for NodeRpcOperator {
    fn methods(&self) -> &'static [&'static str] {
        NODE_OPERATOR_RPC_METHODS
    }

    #[allow(clippy::too_many_lines)]
    fn execute(
        &self,
        method: &str,
        params: &serde_json::Value,
    ) -> Option<Result<serde_json::Value, LocalRpcOperatorError>> {
        if !NODE_OPERATOR_RPC_METHODS.contains(&method) {
            return None;
        }
        if method == "setloglevel" {
            let level = rpc_one_log_level(params);
            return Some(level.and_then(|level| {
                if !rbtc::diagnostics::is_installed() {
                    return Err(LocalRpcOperatorError {
                        code: -32010,
                        message: "Runtime logging is host managed",
                    });
                }
                rbtc::diagnostics::set_level(level);
                Ok(serde_json::json!({"level": level.as_str()}))
            }));
        }
        if matches!(
            method,
            "gettxindexlocation" | "gettxspendingprevout" | "getblockfilter"
        ) {
            return Some(self.execute_index_query(method, params));
        }
        if !rpc_has_no_params(params) {
            return Some(Err(LocalRpcOperatorError {
                code: -32602,
                message: "Invalid params",
            }));
        }
        let status = self.status.response();
        let inbound_connections = status.inbound.as_ref().map_or(0, |inbound| inbound.active);
        Some(Ok(match method {
            "getblockchaininfo" => serde_json::json!({
                "chain": status.network,
                "blocks": status.execution.height,
                "headers": status.header.height,
                "bestblockhash": status.execution.hash,
                "initialblockdownload": !status.trust.minimum_chainwork_reached,
                "verificationprogress": verification_progress(
                    status.execution.height,
                    status.header.height,
                ),
                "pruned": true,
                "pruneheight": status.ledger_first_height,
                "rbtc_prune_target_blocks": status.ledger_target_blocks,
                "rbtc_prune_target_bytes": status.ledger_target_bytes,
                "rbtc_pruned_through_height": status.ledger_pruned_through_height,
                "rbtc_phase": status.phase,
                "rbtc_independently_validated": status.trust.independently_validated,
            }),
            "getnetworkinfo" => serde_json::json!({
                "version": env!("CARGO_PKG_VERSION"),
                "subversion": USER_AGENT,
                "networkactive": true,
                "connections": inbound_connections.saturating_add(
                    u64::try_from(usize::from(self.active_peer.is_some())).unwrap_or(u64::MAX)
                ),
                "connections_in": inbound_connections,
                "connections_out": usize::from(self.active_peer.is_some()),
                "rbtc_inbound": status.inbound,
                "networks": [{
                    "name": status.network,
                    "reachable": self.active_peer.is_some(),
                }],
            }),
            "getpeerinfo" => {
                let mut peers = Vec::new();
                if let Some(address) = self.active_peer {
                    peers.push(serde_json::json!({
                        "addr": address.to_string(),
                        "inbound": false,
                        "role": "active",
                    }));
                }
                if let Some(inbound) = status.inbound {
                    peers.extend(inbound.peers.into_iter().map(|peer| {
                        serde_json::json!({
                            "addr": peer.address.to_string(),
                            "inbound": true,
                            "role": "inbound",
                            "conntime": peer.connected_seconds,
                            "version": peer.protocol_version,
                            "subver": peer.user_agent,
                            "handshake_complete": peer.handshake_complete,
                            "requests": peer.requests,
                            "bytessent": peer.uploaded_bytes,
                            "historicalbytessent": peer.historical_upload_bytes,
                        })
                    }));
                }
                serde_json::Value::Array(peers)
            }
            "getmempoolinfo" => {
                let pool = self
                    .transaction_pool
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                serde_json::json!({
                    "loaded": true,
                    "size": pool.len(),
                    "bytes": pool.retained_bytes(),
                    "maxmempool": pool.max_bytes(),
                    "maxtransactions": pool.max_transactions(),
                    "orphan_count": pool.orphan_len(),
                    "orphan_bytes": pool.orphan_bytes(),
                })
            }
            "getindexinfo" => serde_json::json!({
                "explorer": {
                    "synced": status.explorer == status.execution,
                    "best_block_height": status.explorer.height,
                },
                "wallet": status.wallet.as_ref().map(|wallet| serde_json::json!({
                    "synced": *wallet == status.execution,
                    "best_block_height": wallet.height,
                })),
                "txindex": status.transaction_index.as_ref().map(|tip| serde_json::json!({
                    "synced": *tip == status.execution,
                    "best_block_height": tip.height,
                })),
                "spent_output": status.spent_output_index.as_ref().map(|tip| serde_json::json!({
                    "synced": *tip == status.execution,
                    "best_block_height": tip.height,
                })),
                "basic_filter": status.basic_filter_index.as_ref().map(|tip| serde_json::json!({
                    "synced": *tip == status.execution,
                    "best_block_height": tip.height,
                })),
            }),
            "verifychain" => serde_json::Value::Bool(status_consistent(&status)),
            "getloginfo" => serde_json::json!({
                "level": rbtc::diagnostics::level().as_str(),
                "dropped_records": rbtc::diagnostics::dropped_records(),
                "write_errors": rbtc::diagnostics::write_errors(),
                "host_managed": !rbtc::diagnostics::is_installed(),
            }),
            "getdiskinfo" => serde_json::to_value(status.disk_space)
                .expect("disk-space status serialization is infallible"),
            "stop" => {
                let runtime_control = Arc::clone(&self.runtime_control);
                tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    runtime_control.request_shutdown();
                });
                serde_json::Value::String("rBTC stopping".to_owned())
            }
            _ => unreachable!("method membership checked above"),
        }))
    }
}

impl NodeRpcOperator {
    #[allow(clippy::too_many_lines)]
    fn execute_index_query(
        &self,
        method: &str,
        params: &serde_json::Value,
    ) -> Result<serde_json::Value, LocalRpcOperatorError> {
        let invalid = || LocalRpcOperatorError {
            code: -32602,
            message: "Invalid params",
        };
        let unavailable = || LocalRpcOperatorError {
            code: -1,
            message: "Index is not enabled",
        };
        let storage = || LocalRpcOperatorError {
            code: -32020,
            message: "Index storage failure",
        };
        match method {
            "gettxindexlocation" => {
                let values = params.as_array().ok_or_else(invalid)?;
                let txid = values
                    .first()
                    .and_then(serde_json::Value::as_str)
                    .and_then(|value| Txid::from_str(value).ok())
                    .filter(|_| values.len() == 1)
                    .ok_or_else(invalid)?;
                let index = self
                    .auxiliary_indexes
                    .transaction
                    .as_deref()
                    .ok_or_else(unavailable)?;
                index
                    .transaction(txid)
                    .map_err(|_| storage())
                    .map(|location| {
                        location.map_or(serde_json::Value::Null, |location| {
                            serde_json::json!({
                                "txid": txid.to_string(),
                                "blockhash": location.block_hash.to_string(),
                                "blockheight": location.height,
                                "position": location.position,
                            })
                        })
                    })
            }
            "gettxspendingprevout" => {
                let values = params.as_array().ok_or_else(invalid)?;
                let requests = values
                    .first()
                    .and_then(serde_json::Value::as_array)
                    .filter(|_| values.len() == 1)
                    .ok_or_else(invalid)?;
                if requests.len() > 1_000 {
                    return Err(invalid());
                }
                let index = self
                    .auxiliary_indexes
                    .spent_output
                    .as_deref()
                    .ok_or_else(unavailable)?;
                requests
                    .iter()
                    .map(|request| {
                        let txid = request
                            .get("txid")
                            .and_then(serde_json::Value::as_str)
                            .and_then(|value| Txid::from_str(value).ok())
                            .ok_or_else(invalid)?;
                        let vout = request
                            .get("vout")
                            .and_then(serde_json::Value::as_u64)
                            .and_then(|value| u32::try_from(value).ok())
                            .ok_or_else(invalid)?;
                        let outpoint = OutPoint::new(txid, vout);
                        let location = index.spender(outpoint).map_err(|_| storage())?;
                        Ok(location.map_or_else(
                            || serde_json::json!({"txid": txid, "vout": vout}),
                            |location| {
                                serde_json::json!({
                                    "txid": txid,
                                    "vout": vout,
                                    "spendingtxid": location.spending_txid,
                                    "input_index": location.input_index,
                                    "blockhash": location.block_hash,
                                    "blockheight": location.height,
                                })
                            },
                        ))
                    })
                    .collect::<Result<Vec<_>, _>>()
                    .map(serde_json::Value::Array)
            }
            "getblockfilter" => {
                let values = params.as_array().ok_or_else(invalid)?;
                if !(values.len() == 1
                    || (values.len() == 2 && values[1].as_str() == Some("basic")))
                {
                    return Err(invalid());
                }
                let hash = values
                    .first()
                    .and_then(serde_json::Value::as_str)
                    .and_then(|value| BlockHash::from_str(value).ok())
                    .ok_or_else(invalid)?;
                let index = self
                    .auxiliary_indexes
                    .basic_filter
                    .as_deref()
                    .ok_or_else(unavailable)?;
                index
                    .basic_filter_by_hash(hash)
                    .map_err(|_| storage())?
                    .map_or_else(
                        || {
                            Err(LocalRpcOperatorError {
                                code: -5,
                                message: "Block filter not found",
                            })
                        },
                        |record| {
                            Ok(serde_json::json!({
                                "filter": record.filter.to_lower_hex_string(),
                                "header": record.filter_header.to_string(),
                            }))
                        },
                    )
            }
            _ => unreachable!("index method membership checked"),
        }
    }
}

fn rpc_has_no_params(params: &serde_json::Value) -> bool {
    params.is_null() || params.as_array().is_some_and(Vec::is_empty)
}

fn rpc_one_log_level(params: &serde_json::Value) -> Result<LogLevel, LocalRpcOperatorError> {
    let values = params.as_array().ok_or(LocalRpcOperatorError {
        code: -32602,
        message: "Invalid params",
    })?;
    if values.len() != 1 {
        return Err(LocalRpcOperatorError {
            code: -32602,
            message: "Invalid params",
        });
    }
    values[0]
        .as_str()
        .and_then(LogLevel::parse)
        .ok_or(LocalRpcOperatorError {
            code: -32602,
            message: "Invalid params",
        })
}

fn verification_progress(execution: u32, headers: u32) -> f64 {
    if headers == 0 {
        1.0
    } else {
        (f64::from(execution) / f64::from(headers)).min(1.0)
    }
}

fn status_consistent(status: &NodeStatusResponse) -> bool {
    status.execution.height <= status.header.height
        && (status.execution.height != status.header.height
            || status.execution.hash == status.header.hash)
        && status.explorer.height <= status.execution.height
        && (status.explorer.height != status.execution.height
            || status.explorer.hash == status.execution.hash)
        && status.wallet.as_ref().is_none_or(|wallet| {
            wallet.height <= status.execution.height
                && (wallet.height != status.execution.height
                    || wallet.hash == status.execution.hash)
        })
        && [
            status.transaction_index.as_ref(),
            status.spent_output_index.as_ref(),
            status.basic_filter_index.as_ref(),
        ]
        .into_iter()
        .flatten()
        .all(|tip| {
            tip.height <= status.execution.height
                && (tip.height != status.execution.height || tip.hash == status.execution.hash)
        })
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
    transaction_index: Option<NodeTipResponse>,
    spent_output_index: Option<NodeTipResponse>,
    basic_filter_index: Option<NodeTipResponse>,
    trust: NodeTrustResponse,
    utxo_hot: u64,
    utxo_cold: u64,
    ledger_segments: u32,
    ledger_blocks: u32,
    ledger_bytes: u64,
    ledger_first_height: Option<u32>,
    ledger_tip_height: Option<u32>,
    ledger_target_blocks: u32,
    ledger_target_bytes: u64,
    ledger_pruned_through_height: Option<u32>,
    inbound: Option<InboundStatsSnapshot>,
    disk_space: DiskSpaceSnapshot,
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
                // The active and background pipelines have independent peers
                // and stores. Keep a bounded background window running while
                // the serving chain downloads its much newer blocks: reducing
                // to small request/commit cycles adds network round trips,
                // freezer publication, and durable transactions without
                // freeing the active pipeline's bottleneck. Keep the busy
                // ceiling below one consensus-maximum GiB of block payload.
                max_blocks_per_batch: configured
                    .max_blocks_per_batch
                    .min(ADAPTIVE_VALIDATION_BUSY_BATCH_SIZE),
                pause_between_batches: configured.pause_between_batches,
                quick_repair: configured.quick_repair,
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
    #[cfg(test)]
    fn new(progress: NodeStatusProgress) -> Self {
        Self {
            started: Instant::now(),
            progress: Arc::new(Mutex::new(progress)),
            inbound: None,
        }
    }

    fn with_inbound(progress: NodeStatusProgress, inbound: Option<Arc<InboundStats>>) -> Self {
        Self {
            started: Instant::now(),
            progress: Arc::new(Mutex::new(progress)),
            inbound,
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
        let auxiliary_caught_up = [
            progress.transaction_index.as_ref(),
            progress.spent_output_index.as_ref(),
            progress.basic_filter_index.as_ref(),
        ]
        .into_iter()
        .flatten()
        .all(|tip| *tip == progress.execution);
        let disk_safe = progress.disk_space.available_bytes >= progress.disk_space.required_bytes;
        let ready = progress.minimum_chainwork_reached
            && chain_caught_up
            && explorer_caught_up
            && wallet_caught_up
            && auxiliary_caught_up
            && disk_safe;
        let phase = if !disk_safe {
            "low_disk"
        } else if !progress.minimum_chainwork_reached {
            "ibd"
        } else if !chain_caught_up {
            "syncing_blocks"
        } else if !explorer_caught_up || !wallet_caught_up || !auxiliary_caught_up {
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
            transaction_index: progress.transaction_index,
            spent_output_index: progress.spent_output_index,
            basic_filter_index: progress.basic_filter_index,
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
            ledger_target_blocks: progress.ledger_target_blocks,
            ledger_target_bytes: progress.ledger_target_bytes,
            ledger_pruned_through_height: progress.ledger_pruned_through_height,
            inbound: self.inbound.as_ref().map(|stats| stats.snapshot()),
            disk_space: progress.disk_space,
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

#[allow(clippy::too_many_lines)]
async fn node_metrics(
    axum::extract::State(status): axum::extract::State<NodeStatus>,
) -> ([(axum::http::HeaderName, &'static str); 2], String) {
    let status = status.response();
    let ready = u8::from(status.ready);
    let minimum_chainwork = u8::from(status.trust.minimum_chainwork_reached);
    let independently_validated = u8::from(status.trust.independently_validated);
    let wallet_enabled = u8::from(status.wallet.is_some());
    let wallet_height = status.wallet.as_ref().map_or(0, |tip| tip.height);
    let transaction_index_height = status
        .transaction_index
        .as_ref()
        .map_or(0, |tip| tip.height);
    let spent_output_index_height = status
        .spent_output_index
        .as_ref()
        .map_or(0, |tip| tip.height);
    let basic_filter_index_height = status
        .basic_filter_index
        .as_ref()
        .map_or(0, |tip| tip.height);
    let ledger_first_height = status.ledger_first_height.unwrap_or(0);
    let ledger_tip_height = status.ledger_tip_height.unwrap_or(0);
    let ledger_pruned_through = status.ledger_pruned_through_height.unwrap_or(0);
    let ledger_target_blocks = status.ledger_target_blocks;
    let ledger_target_bytes = status.ledger_target_bytes;
    let execution_lag = status.header.height.saturating_sub(status.execution.height);
    let explorer_lag = status
        .execution
        .height
        .saturating_sub(status.explorer.height);
    let utxo_total = status.utxo_hot.saturating_add(status.utxo_cold);
    let dropped_logs = rbtc::diagnostics::dropped_records();
    let log_write_errors = rbtc::diagnostics::write_errors();
    let disk_total = status.disk_space.total_bytes;
    let disk_available = status.disk_space.available_bytes;
    let disk_required = status.disk_space.required_bytes;
    let disk_reserve = status.disk_space.reserve_bytes;
    let storage_ceiling = status.disk_space.projected_storage_ceiling_bytes;
    let optional_index_bytes = status.disk_space.optional_index_bytes;
    let optional_index_headroom = status.disk_space.optional_index_checkpoint_headroom_bytes;
    let inbound_active = status.inbound.as_ref().map_or(0, |value| value.active);
    let inbound_accepted = status
        .inbound
        .as_ref()
        .map_or(0, |value| value.accepted_total);
    let inbound_rejected_capacity = status
        .inbound
        .as_ref()
        .map_or(0, |value| value.rejected_capacity_total);
    let inbound_rejected_source = status
        .inbound
        .as_ref()
        .map_or(0, |value| value.rejected_source_total);
    let inbound_completed = status
        .inbound
        .as_ref()
        .map_or(0, |value| value.completed_total);
    let inbound_request_budget_disconnects = status
        .inbound
        .as_ref()
        .map_or(0, |value| value.request_budget_disconnects);
    let inbound_upload_target_disconnects = status
        .inbound
        .as_ref()
        .map_or(0, |value| value.upload_target_disconnects);
    let inbound_protocol_disconnects = status
        .inbound
        .as_ref()
        .map_or(0, |value| value.protocol_disconnects);
    let inbound_request_bound_disconnects = status
        .inbound
        .as_ref()
        .map_or(0, |value| value.request_bound_disconnects);
    let inbound_io_disconnects = status
        .inbound
        .as_ref()
        .map_or(0, |value| value.io_disconnects);
    let inbound_data_disconnects = status
        .inbound
        .as_ref()
        .map_or(0, |value| value.data_disconnects);
    let inbound_historical_upload = status
        .inbound
        .as_ref()
        .map_or(0, |value| value.historical_upload_bytes_24h);
    let inbound_historical_target = status
        .inbound
        .as_ref()
        .map_or(0, |value| value.historical_upload_target_bytes);
    let body = format!(
        "# HELP rbtc_node_info Static network and current synchronization phase.\n# TYPE rbtc_node_info gauge\nrbtc_node_info{{network=\"{}\",phase=\"{}\"}} 1\n# HELP rbtc_ready Whether every serving projection is caught up and minimum chainwork is reached.\n# TYPE rbtc_ready gauge\nrbtc_ready {ready}\n# HELP rbtc_tip_height Durable heights by subsystem.\n# TYPE rbtc_tip_height gauge\nrbtc_tip_height{{kind=\"header\"}} {}\nrbtc_tip_height{{kind=\"execution\"}} {}\nrbtc_tip_height{{kind=\"explorer\"}} {}\nrbtc_tip_height{{kind=\"wallet\"}} {wallet_height}\nrbtc_tip_height{{kind=\"txindex\"}} {transaction_index_height}\nrbtc_tip_height{{kind=\"spent_output\"}} {spent_output_index_height}\nrbtc_tip_height{{kind=\"basic_filter\"}} {basic_filter_index_height}\nrbtc_tip_height{{kind=\"ledger\"}} {ledger_tip_height}\n# HELP rbtc_tip_lag_blocks Current block lag by subsystem.\n# TYPE rbtc_tip_lag_blocks gauge\nrbtc_tip_lag_blocks{{kind=\"execution\"}} {execution_lag}\nrbtc_tip_lag_blocks{{kind=\"explorer\"}} {explorer_lag}\n# HELP rbtc_utxos Current UTXO entries by tier.\n# TYPE rbtc_utxos gauge\nrbtc_utxos{{tier=\"hot\"}} {}\nrbtc_utxos{{tier=\"cold\"}} {}\nrbtc_utxos{{tier=\"total\"}} {utxo_total}\n# HELP rbtc_ledger_segments Retained immutable ledger segments.\n# TYPE rbtc_ledger_segments gauge\nrbtc_ledger_segments {}\n# HELP rbtc_ledger_blocks Retained pruned-ledger blocks.\n# TYPE rbtc_ledger_blocks gauge\nrbtc_ledger_blocks {}\n# HELP rbtc_ledger_bytes Retained compressed ledger bytes.\n# TYPE rbtc_ledger_bytes gauge\nrbtc_ledger_bytes {}\n# HELP rbtc_ledger_first_height Oldest retained block height, or zero for an empty ledger.\n# TYPE rbtc_ledger_first_height gauge\nrbtc_ledger_first_height {ledger_first_height}\n# HELP rbtc_ledger_pruned_through_height Highest physically pruned active-prefix height, or zero when none.\n# TYPE rbtc_ledger_pruned_through_height gauge\nrbtc_ledger_pruned_through_height {ledger_pruned_through}\n# HELP rbtc_ledger_retention_target Configured freezer retention ceilings.\n# TYPE rbtc_ledger_retention_target gauge\nrbtc_ledger_retention_target{{kind=\"blocks\"}} {ledger_target_blocks}\nrbtc_ledger_retention_target{{kind=\"bytes\"}} {ledger_target_bytes}\n# HELP rbtc_disk_bytes Filesystem capacity and checkpoint safety thresholds.\n# TYPE rbtc_disk_bytes gauge\nrbtc_disk_bytes{{kind=\"total\"}} {disk_total}\nrbtc_disk_bytes{{kind=\"available\"}} {disk_available}\nrbtc_disk_bytes{{kind=\"required\"}} {disk_required}\nrbtc_disk_bytes{{kind=\"reserve\"}} {disk_reserve}\nrbtc_disk_bytes{{kind=\"bounded_storage_ceiling\"}} {storage_ceiling}\nrbtc_disk_bytes{{kind=\"optional_indexes\"}} {optional_index_bytes}\nrbtc_disk_bytes{{kind=\"optional_index_checkpoint_headroom\"}} {optional_index_headroom}\n# HELP rbtc_minimum_chainwork_reached Whether the configured IBD work floor is reached.\n# TYPE rbtc_minimum_chainwork_reached gauge\nrbtc_minimum_chainwork_reached {minimum_chainwork}\n# HELP rbtc_independently_validated Whether chainstate no longer depends on an assumed snapshot.\n# TYPE rbtc_independently_validated gauge\nrbtc_independently_validated {independently_validated}\n# HELP rbtc_wallet_enabled Whether the serving node has an embedded wallet.\n# TYPE rbtc_wallet_enabled gauge\nrbtc_wallet_enabled {wallet_enabled}\n# HELP rbtc_log_dropped_records Total structured diagnostics dropped by rate or queue bounds.\n# TYPE rbtc_log_dropped_records counter\nrbtc_log_dropped_records {dropped_logs}\n# HELP rbtc_log_write_errors Total structured-log destination failures.\n# TYPE rbtc_log_write_errors counter\nrbtc_log_write_errors {log_write_errors}\n# HELP rbtc_inbound_connections Currently admitted inbound peer sessions.\n# TYPE rbtc_inbound_connections gauge\nrbtc_inbound_connections {inbound_active}\n# HELP rbtc_inbound_sessions_total Inbound session admission and completion counters.\n# TYPE rbtc_inbound_sessions_total counter\nrbtc_inbound_sessions_total{{result=\"accepted\"}} {inbound_accepted}\nrbtc_inbound_sessions_total{{result=\"rejected_capacity\"}} {inbound_rejected_capacity}\nrbtc_inbound_sessions_total{{result=\"rejected_source\"}} {inbound_rejected_source}\nrbtc_inbound_sessions_total{{result=\"completed\"}} {inbound_completed}\n# HELP rbtc_inbound_disconnects_total Classified inbound disconnect counters.\n# TYPE rbtc_inbound_disconnects_total counter\nrbtc_inbound_disconnects_total{{reason=\"request_budget\"}} {inbound_request_budget_disconnects}\nrbtc_inbound_disconnects_total{{reason=\"request_bound\"}} {inbound_request_bound_disconnects}\nrbtc_inbound_disconnects_total{{reason=\"upload_target\"}} {inbound_upload_target_disconnects}\nrbtc_inbound_disconnects_total{{reason=\"protocol\"}} {inbound_protocol_disconnects}\nrbtc_inbound_disconnects_total{{reason=\"io\"}} {inbound_io_disconnects}\nrbtc_inbound_disconnects_total{{reason=\"local_data\"}} {inbound_data_disconnects}\n# HELP rbtc_inbound_historical_upload_bytes Historical block upload in the current rolling window and its target.\n# TYPE rbtc_inbound_historical_upload_bytes gauge\nrbtc_inbound_historical_upload_bytes{{kind=\"used_24h\"}} {inbound_historical_upload}\nrbtc_inbound_historical_upload_bytes{{kind=\"target\"}} {inbound_historical_target}\n# HELP rbtc_session_uptime_seconds Current peer-serving session uptime.\n# TYPE rbtc_session_uptime_seconds gauge\nrbtc_session_uptime_seconds {}\n",
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

struct NodeInboundSource {
    headers: Arc<RwLock<HeaderDag>>,
    chainstate: Arc<RedbChainStore>,
    ledger: Arc<PrunedBlockLedger>,
    transaction_pool: Arc<Mutex<TransactionAdmissionPool>>,
    pending_transactions: Arc<Mutex<InboundTransactionQueue>>,
    basic_filter: Option<Arc<RedbAuxiliaryIndex>>,
}

struct SharedInboundSource {
    current: RwLock<Option<Arc<dyn InboundDataSource>>>,
    peer_store: RwLock<Option<Arc<RedbPeerStore>>>,
    stats: Arc<InboundStats>,
}

impl SharedInboundSource {
    fn new(max_upload_bytes_per_day: u64) -> Self {
        Self {
            current: RwLock::new(None),
            peer_store: RwLock::new(None),
            stats: Arc::new(InboundStats::new(max_upload_bytes_per_day)),
        }
    }

    fn stats(&self) -> Arc<InboundStats> {
        Arc::clone(&self.stats)
    }

    fn current(&self) -> Result<Arc<dyn InboundDataSource>, String> {
        self.current
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .map(Arc::clone)
            .ok_or_else(|| "active consensus data is not ready".to_owned())
    }

    fn install(self: &Arc<Self>, source: Arc<dyn InboundDataSource>) -> SharedInboundSourceLease {
        *self
            .current
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Arc::clone(&source));
        SharedInboundSourceLease {
            shared: Arc::clone(self),
            source,
        }
    }

    fn install_peer_store(&self, peer_store: Option<Arc<RedbPeerStore>>) {
        *self
            .peer_store
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = peer_store;
    }
}

struct SharedInboundSourceLease {
    shared: Arc<SharedInboundSource>,
    source: Arc<dyn InboundDataSource>,
}

impl Drop for SharedInboundSourceLease {
    fn drop(&mut self) {
        let mut current = self
            .shared
            .current
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if current
            .as_ref()
            .is_some_and(|source| Arc::ptr_eq(source, &self.source))
        {
            *current = None;
        }
    }
}

impl InboundDataSource for SharedInboundSource {
    fn start_height(&self) -> Result<u32, String> {
        self.current()?.start_height()
    }

    fn active_header(&self, height: u32) -> Result<Option<Header>, String> {
        self.current()?.active_header(height)
    }

    fn active_height(&self, hash: BlockHash) -> Result<Option<u32>, String> {
        self.current()?.active_height(hash)
    }

    fn block(&self, hash: BlockHash) -> Result<Option<Vec<u8>>, String> {
        self.current()?.block(hash)
    }

    fn mempool(&self) -> Result<Vec<Transaction>, String> {
        self.current()?.mempool()
    }

    fn transaction(&self, inventory: Inventory) -> Result<Option<Transaction>, String> {
        self.current()?.transaction(inventory)
    }

    fn submit_transaction(&self, transaction: Transaction) -> Result<bool, String> {
        self.current()?.submit_transaction(transaction)
    }

    fn basic_filter(&self, height: u32) -> Result<Option<InboundBasicFilter>, String> {
        self.current()?.basic_filter(height)
    }

    fn addresses(&self) -> Result<Vec<SocketAddr>, String> {
        let peer_store = self
            .peer_store
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .map(Arc::clone);
        let Some(peer_store) = peer_store else {
            return Ok(Vec::new());
        };
        peer_store
            .candidates(unix_time()?, rbtc::p2p::MAX_ADDRESSES_PER_MESSAGE)
            .map_err(|error| error.to_string())
    }

    fn fee_filter_sat_kvb(&self) -> Result<u64, String> {
        self.current()?.fee_filter_sat_kvb()
    }
}

#[derive(Default)]
struct InboundTransactionQueue {
    transactions: VecDeque<(Transaction, usize)>,
    bytes: usize,
}

impl InboundTransactionQueue {
    fn push(&mut self, transaction: Transaction) -> bool {
        if transaction.is_coinbase()
            || transaction.weight().to_wu() > u64::from(bitcoin::policy::MAX_STANDARD_TX_WEIGHT)
        {
            return false;
        }
        let bytes = serialize(&transaction).len();
        if bytes > MAX_PROTOCOL_MESSAGE_LEN as usize {
            return false;
        }
        let wtxid = transaction.compute_wtxid();
        if self
            .transactions
            .iter()
            .any(|(existing, _)| existing.compute_wtxid() == wtxid)
        {
            return false;
        }
        while self.transactions.len() >= MAX_PENDING_TRANSACTIONS
            || self.bytes.saturating_add(bytes) > MAX_PROTOCOL_MESSAGE_LEN as usize
        {
            let Some((_, removed)) = self.transactions.pop_front() else {
                break;
            };
            self.bytes = self.bytes.saturating_sub(removed);
        }
        self.transactions.push_back((transaction, bytes));
        self.bytes = self.bytes.saturating_add(bytes);
        true
    }

    fn drain(&mut self) -> Vec<Transaction> {
        self.bytes = 0;
        self.transactions
            .drain(..)
            .map(|(transaction, _)| transaction)
            .collect()
    }
}

impl NodeInboundSource {
    fn execution_height(&self) -> Result<u32, String> {
        self.chainstate
            .execution()
            .tip()
            .map(|tip| tip.height)
            .map_err(|error| error.to_string())
    }
}

impl InboundDataSource for NodeInboundSource {
    fn start_height(&self) -> Result<u32, String> {
        self.execution_height()
    }

    fn active_header(&self, height: u32) -> Result<Option<Header>, String> {
        if height > self.execution_height()? {
            return Ok(None);
        }
        Ok(self
            .headers
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .active_header_at(height)
            .map(|info| info.header))
    }

    fn active_height(&self, hash: BlockHash) -> Result<Option<u32>, String> {
        let executed = self.execution_height()?;
        Ok(self
            .headers
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .active_height_of(hash)
            .filter(|height| *height <= executed))
    }

    fn block(&self, hash: BlockHash) -> Result<Option<Vec<u8>>, String> {
        let Some(height) = self.active_height(hash)? else {
            return Ok(None);
        };
        let raw = self
            .ledger
            .read_block(height)
            .map_err(|error| error.to_string())?;
        if let Some(raw) = &raw {
            let block = deserialize::<Block>(raw)
                .map_err(|error| format!("decode retained active block {height}: {error}"))?;
            if block.block_hash() != hash {
                return Err(format!(
                    "retained active block at height {height} has hash {}, expected {hash}",
                    block.block_hash()
                ));
            }
        }
        Ok(raw)
    }

    fn mempool(&self) -> Result<Vec<Transaction>, String> {
        Ok(self
            .transaction_pool
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .snapshot())
    }

    fn transaction(&self, inventory: Inventory) -> Result<Option<Transaction>, String> {
        Ok(self
            .transaction_pool
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .snapshot()
            .into_iter()
            .find(|transaction| match inventory {
                Inventory::Transaction(txid) | Inventory::WitnessTransaction(txid) => {
                    transaction.compute_txid() == txid
                }
                Inventory::WTx(wtxid) => transaction.compute_wtxid() == wtxid,
                _ => false,
            }))
    }

    fn submit_transaction(&self, transaction: Transaction) -> Result<bool, String> {
        Ok(self
            .pending_transactions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(transaction))
    }

    fn basic_filter(&self, height: u32) -> Result<Option<InboundBasicFilter>, String> {
        if height > self.execution_height()? {
            return Ok(None);
        }
        let Some(index) = &self.basic_filter else {
            return Ok(None);
        };
        let Some(header) = self
            .headers
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .active_header_at(height)
        else {
            return Ok(None);
        };
        index
            .basic_filter(height)
            .map_err(|error| error.to_string())
            .map(|record| {
                record.map(|record| InboundBasicFilter {
                    block_hash: header.hash,
                    filter: record.filter,
                    filter_hash: record.filter_hash,
                    filter_header: record.filter_header,
                })
            })
    }

    fn fee_filter_sat_kvb(&self) -> Result<u64, String> {
        Ok(self
            .transaction_pool
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .rolling_minimum_fee_sat_kvb(unix_time()?))
    }
}

struct InboundServer {
    task: Option<tokio::task::JoinHandle<Result<(), String>>>,
}

impl InboundServer {
    async fn bind(
        address: SocketAddr,
        network: Network,
        local_nonce: u64,
        limits: InboundLimits,
        source: Arc<dyn InboundDataSource>,
        stats: Arc<InboundStats>,
        serve_compact_filters: bool,
    ) -> Result<Self, String> {
        let listener = tokio::net::TcpListener::bind(address)
            .await
            .map_err(|error| format!("bind inbound P2P at {address}: {error}"))?;
        let bound = listener
            .local_addr()
            .map_err(|error| format!("read inbound P2P address: {error}"))?;
        let mut services = ServiceFlags::NETWORK_LIMITED | ServiceFlags::WITNESS;
        if serve_compact_filters {
            services |= ServiceFlags::COMPACT_FILTERS;
        }
        rbtc_info!(
            "inbound P2P listening on {bound}; max_peers={} per_ip={} daily_upload_bytes={} requests_per_minute={}",
            limits.max_connections,
            limits.max_connections_per_ip,
            limits.max_upload_bytes_per_day,
            limits.max_requests_per_minute
        );
        let task = tokio::spawn(async move {
            run_listener_with_stats(
                listener,
                network.magic(),
                local_nonce,
                USER_AGENT.to_owned(),
                services,
                limits,
                source,
                stats,
            )
            .await
            .map_err(|error| error.to_string())
        });
        Ok(Self { task: Some(task) })
    }

    async fn wait_for_exit(&mut self) -> Result<(), String> {
        self.task
            .take()
            .ok_or_else(|| "inbound P2P listener task missing".to_owned())?
            .await
            .map_err(|error| format!("inbound P2P listener task: {error}"))?
    }
}

async fn wait_for_inbound_exit(server: &mut Option<InboundServer>) -> Result<(), String> {
    match server {
        Some(server) => server.wait_for_exit().await,
        None => std::future::pending().await,
    }
}

impl Drop for InboundServer {
    fn drop(&mut self) {
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

impl ApiServer {
    #[allow(clippy::too_many_arguments)]
    async fn bind(
        address: SocketAddr,
        index: Arc<RedbExplorerIndex>,
        explorer_events: ExplorerEventHub,
        node_status: NodeStatus,
        wallet: Option<&WalletApiRuntime>,
        rpc: Option<&RpcApiRuntime>,
        fee_estimator: Option<&Arc<RedbFeeEstimator>>,
        background_validation: Option<&BackgroundValidationStatus>,
        transaction_pool: Arc<Mutex<TransactionAdmissionPool>>,
        runtime_control: Arc<RuntimeControl>,
        active_peer: Option<SocketAddr>,
        auxiliary_indexes: Arc<AuxiliaryIndexes>,
    ) -> Result<Self, String> {
        let listener = tokio::net::TcpListener::bind(address)
            .await
            .map_err(|error| format!("bind explorer at {address}: {error}"))?;
        let bound = listener
            .local_addr()
            .map_err(|error| format!("read explorer address: {error}"))?;
        let mut router = explorer_router(Arc::clone(&index))
            .merge(explorer_events_router(explorer_events))
            .merge(node_status_router(node_status.clone()));
        let mut token_tasks = Vec::new();
        if let Some(wallet) = wallet {
            let token = wallet.token.clone();
            let token_path = wallet.token_path.clone();
            token_tasks.push(tokio::spawn(watch_local_auth_token(
                token_path, token, "wallet",
            )));
        }
        if let Some(wallet) = wallet {
            router = router.merge(wallet_router_with_sink(
                Arc::clone(&wallet.wallet),
                wallet.token.clone(),
                wallet.audit.clone(),
                Some(WalletBroadcastSink::durable(
                    wallet.broadcast_sender.clone(),
                    Arc::clone(&wallet.rebroadcast),
                )),
                fee_estimator.map(Arc::clone),
            ));
        }
        if let Some(rpc) = rpc {
            let token = rpc.token.clone();
            let token_path = rpc.token_path.clone();
            token_tasks.push(tokio::spawn(watch_local_auth_token(
                token_path, token, "RPC",
            )));
            let operator: Arc<dyn LocalRpcOperator> = Arc::new(NodeRpcOperator {
                status: node_status.clone(),
                transaction_pool: Arc::clone(&transaction_pool),
                runtime_control: Arc::clone(&runtime_control),
                active_peer,
                auxiliary_indexes: Arc::clone(&auxiliary_indexes),
            });
            router = router.merge(rpc_router_with_operator(
                Arc::clone(&index),
                fee_estimator.map(Arc::clone),
                rpc.token.clone(),
                rpc.audit.clone(),
                Some(operator),
            ));
        }
        if let Some(status) = background_validation {
            router = router.merge(validation_status_router(status.clone()));
        }
        rbtc_info!("embedded API listening on http://{bound}");
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

#[allow(clippy::too_many_arguments)]
fn collect_node_status(
    network: Network,
    ibd_policy: IbdPolicy,
    headers: &HeaderDag,
    chainstate: &RedbChainStore,
    explorer: &RedbExplorerIndex,
    ledger: &PrunedBlockLedger,
    wallet: Option<&EmbeddedWallet>,
    auxiliary_indexes: &AuxiliaryIndexes,
    disk_space: DiskSpaceSnapshot,
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
    let auxiliary_tip = |index: Option<&RedbAuxiliaryIndex>| {
        index
            .map(|index| index.tip().map_err(|error| error.to_string()))
            .transpose()
    };
    let transaction_index = auxiliary_tip(auxiliary_indexes.transaction.as_deref())?;
    let spent_output_index = auxiliary_tip(auxiliary_indexes.spent_output.as_deref())?;
    let basic_filter_index = auxiliary_tip(auxiliary_indexes.basic_filter.as_deref())?;
    let ibd = ibd_policy
        .status(headers)
        .map_err(|error| error.to_string())?;
    let utxos = chainstate.tier_stats().map_err(|error| error.to_string())?;
    let ledger_retention = ledger.retention();
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
        transaction_index: transaction_index.map(|tip| NodeTipResponse {
            height: tip.height,
            hash: tip.hash.to_string(),
        }),
        spent_output_index: spent_output_index.map(|tip| NodeTipResponse {
            height: tip.height,
            hash: tip.hash.to_string(),
        }),
        basic_filter_index: basic_filter_index.map(|tip| NodeTipResponse {
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
        ledger_target_blocks: ledger_retention.max_blocks,
        ledger_target_bytes: ledger_retention.max_bytes,
        ledger_pruned_through_height: ledger.first_height.and_then(|height| height.checked_sub(1)),
        disk_space,
    })
}

#[derive(Clone, Copy)]
struct RuntimeStatusSources<'a> {
    network: Network,
    ibd_policy: IbdPolicy,
    headers: &'a HeaderDag,
    chainstate: &'a RedbChainStore,
    explorer: Option<&'a RedbExplorerIndex>,
    ledger: &'a PrunedBlockLedger,
    wallet: Option<&'a EmbeddedWallet>,
    auxiliary_indexes: &'a AuxiliaryIndexes,
    disk_space: DiskSpaceSnapshot,
}

#[allow(clippy::too_many_lines)]
fn collect_runtime_status(sources: RuntimeStatusSources<'_>) -> Result<NodeRuntimeStatus, String> {
    let RuntimeStatusSources {
        network,
        ibd_policy,
        headers,
        chainstate,
        explorer,
        ledger,
        wallet,
        auxiliary_indexes,
        disk_space,
    } = sources;
    let header = headers.active_tip();
    let execution = chainstate
        .execution()
        .tip()
        .map_err(|error| error.to_string())?;
    let explorer = explorer
        .map(|index| index.tip().map_err(|error| error.to_string()))
        .transpose()?;
    let wallet = wallet
        .map(|wallet| wallet.chain_tip().map_err(|error| error.to_string()))
        .transpose()?;
    let auxiliary_tip = |index: Option<&RedbAuxiliaryIndex>| {
        index
            .map(|index| index.tip().map_err(|error| error.to_string()))
            .transpose()
    };
    let transaction_index = auxiliary_tip(auxiliary_indexes.transaction.as_deref())?;
    let spent_output_index = auxiliary_tip(auxiliary_indexes.spent_output.as_deref())?;
    let basic_filter_index = auxiliary_tip(auxiliary_indexes.basic_filter.as_deref())?;
    let ibd = ibd_policy
        .status(headers)
        .map_err(|error| error.to_string())?;
    let utxos = chainstate.tier_stats().map_err(|error| error.to_string())?;
    let freezer_retention = ledger.retention();
    let freezer = ledger.stats().map_err(|error| error.to_string())?;
    let independently_validated = chainstate
        .execution()
        .assumed_snapshot()
        .map_err(|error| error.to_string())?
        .is_none();
    Ok(NodeRuntimeStatus {
        network,
        active_peer: None,
        header: Some(NodeTip {
            height: header.height,
            hash: header.hash,
        }),
        execution: Some(NodeTip {
            height: execution.height,
            hash: execution.hash,
        }),
        explorer: explorer.map(|tip| NodeTip {
            height: tip.height,
            hash: tip.hash,
        }),
        wallet: wallet.map(|tip| NodeTip {
            height: tip.height,
            hash: tip.hash,
        }),
        transaction_index: transaction_index.map(|tip| NodeTip {
            height: tip.height,
            hash: tip.hash,
        }),
        spent_output_index: spent_output_index.map(|tip| NodeTip {
            height: tip.height,
            hash: tip.hash,
        }),
        basic_filter_index: basic_filter_index.map(|tip| NodeTip {
            height: tip.height,
            hash: tip.hash,
        }),
        utxo_hot: utxos.hot,
        utxo_cold: utxos.cold,
        freezer: NodeFreezerStatus {
            segments: freezer.segments,
            blocks: freezer.blocks,
            bytes: freezer.bytes,
            first_height: freezer.first_height,
            tip_height: freezer.tip_height,
            target_blocks: freezer_retention.max_blocks,
            target_bytes: freezer_retention.max_bytes,
            pruned_through_height: freezer
                .first_height
                .and_then(|height| height.checked_sub(1)),
        },
        disk: NodeDiskStatus {
            total_bytes: disk_space.total_bytes,
            available_bytes: disk_space.available_bytes,
            required_bytes: disk_space.required_bytes,
            reserve_bytes: disk_space.reserve_bytes,
            projected_storage_ceiling_bytes: disk_space.projected_storage_ceiling_bytes,
            optional_index_bytes: disk_space.optional_index_bytes,
            optional_index_checkpoint_headroom_bytes: disk_space
                .optional_index_checkpoint_headroom_bytes,
        },
        trust: NodeTrustStatus {
            minimum_chainwork_reached: ibd.minimum_chainwork_reached,
            active_assume_valid_height: ibd.active_assume_valid_height,
            full_script_validation: ibd.full_script_validation,
            independently_validated,
        },
        last_error: None,
    })
}

fn publish_runtime_status(
    observer: Option<&NodeObserver>,
    sources: RuntimeStatusSources<'_>,
) -> Result<(), String> {
    if let Some(observer) = observer {
        observer.publish(collect_runtime_status(sources)?);
    }
    Ok(())
}

/// Failure returned by the thin command-line adapter.
#[derive(Debug, Error)]
#[error("{message}")]
pub struct CliError {
    message: String,
    exit_code: i32,
}

impl CliError {
    /// Process exit code preserving the daemon's parse/runtime distinction.
    #[must_use]
    pub const fn exit_code(&self) -> i32 {
        self.exit_code
    }

    fn parse(message: String) -> Self {
        Self {
            message,
            exit_code: 2,
        }
    }

    fn runtime(message: String) -> Self {
        Self {
            message,
            exit_code: 1,
        }
    }
}

/// Runs the command-line adapter inside the caller's Tokio runtime.
///
/// The binary is intentionally a thin wrapper around this function. Embedders
/// should use [`NodeBuilder`] so shutdown and task ownership remain under the
/// host application's control.
pub async fn run_cli(arguments: impl Iterator<Item = String>) -> Result<(), CliError> {
    match parse_options(arguments) {
        Ok(Some(options)) => {
            let mut logging = options.logging.clone();
            if matches!(
                options.offline_action.as_ref(),
                Some(
                    OfflineAction::VerifyStorage { .. }
                        | OfflineAction::VerifyChain { .. }
                        | OfflineAction::ReindexFromFreezer { .. }
                        | OfflineAction::ReindexChainstate { .. }
                        | OfflineAction::PruneStorage { .. },
                )
            ) {
                // Offline storage maintenance must not create or rotate a
                // data-directory log merely because diagnostics initialized.
                logging.directory = None;
            }
            rbtc::diagnostics::install(&logging).map_err(CliError::runtime)?;
            // Keep signal collection on an independent runtime task. The
            // validating future performs bounded synchronous execution work;
            // if signal polling shares that same task, the OS notification can
            // otherwise remain unread until after another checkpoint starts.
            let runtime_control = Arc::clone(&options.runtime_control);
            let mut signal_task = tokio::spawn(shutdown_signal());
            let wait_for_runtime_shutdown = runtime_control.shutdown_requested();
            tokio::pin!(wait_for_runtime_shutdown);
            let result = tokio::select! {
                result = run(options) => {
                    signal_task.abort();
                    result
                },
                signal = &mut signal_task => {
                    let signal = signal
                        .map_err(|error| format!("shutdown signal task failed: {error}"))
                        .and_then(std::convert::identity);
                    if signal.is_ok() {
                        runtime_control.request_shutdown();
                        wait_for_in_flight_checkpoints(
                            &runtime_control.checkpoints_in_flight,
                        )
                        .await;
                        rbtc_warn!("rbtcd: shutdown signal received; closing durable stores");
                    }
                    signal
                },
                () = &mut wait_for_runtime_shutdown => {
                    signal_task.abort();
                    wait_for_in_flight_checkpoints(
                        &runtime_control.checkpoints_in_flight,
                    )
                    .await;
                    rbtc_warn!("rbtcd: authenticated shutdown requested; closing durable stores");
                    Ok(())
                },
            };
            rbtc::diagnostics::flush();
            result.map_err(CliError::runtime)
        }
        Ok(None) => {
            print_usage();
            Ok(())
        }
        Err(error) => {
            print_usage();
            Err(CliError::parse(error))
        }
    }
}

async fn shutdown_signal() -> Result<(), String> {
    let interrupt = tokio::signal::ctrl_c();
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .map_err(|error| format!("failed to install termination handler: {error}"))?;
        tokio::select! {
            result = interrupt => {
                result.map_err(|error| format!("failed to install interrupt handler: {error}"))
            }
            _ = terminate.recv() => Ok(()),
        }
    }
    #[cfg(not(unix))]
    {
        interrupt
            .await
            .map_err(|error| format!("failed to install interrupt handler: {error}"))
    }
}

async fn checkpoint_shutdown(requested: &AtomicBool) {
    if requested.load(Ordering::Acquire) {
        std::future::pending::<()>().await;
    } else {
        tokio::task::yield_now().await;
    }
}

async fn wait_for_in_flight_checkpoints(count: &AtomicUsize) {
    while count.load(Ordering::Acquire) != 0 {
        tokio::time::sleep(Duration::from_millis(10)).await;
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
            rbtc_info!(
                "authenticated watch-only wallet API enabled; token file {}",
                files.auth_token.display()
            );
            let (broadcast_sender, broadcast_receiver) =
                mpsc::channel(WALLET_BROADCAST_QUEUE_CAPACITY);
            let rebroadcast = Arc::new(
                RedbRebroadcastStore::open(
                    data_dir.join("rebroadcast.redb"),
                    options.network,
                )
                .map_err(|error| error.to_string())?,
            );
            let compact_candidates = CompactTransactionCandidates::default();
            for transaction in rebroadcast
                .unconfirmed_transactions()
                .map_err(|error| error.to_string())?
            {
                compact_candidates.insert(transaction);
            }
            Ok::<_, String>(WalletApiRuntime {
                wallet: Arc::new(wallet),
                token,
                token_path: files.auth_token.clone(),
                audit,
                scan: WalletScanConfig {
                    gap_limit: descriptors.gap_limit,
                    birthday_height: descriptors.birthday_height,
                },
                broadcast_sender,
                broadcast_receiver: Arc::new(tokio::sync::Mutex::new(broadcast_receiver)),
                pending_broadcast: Arc::new(tokio::sync::Mutex::new(None)),
                compact_candidates,
                rebroadcast,
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
            rbtc_info!(
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
                rbtc_warn!(
                    "{surface} authentication token restored from owner-only file {}",
                    path.display()
                );
                enabled = true;
            }
            Ok(()) => {}
            Err(error) if enabled => {
                token.invalidate();
                rbtc_warn!("{surface} authentication disabled until token file is valid: {error}");
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
    if let Some(OfflineAction::DownloadCoreSnapshot(config)) = &options.offline_action {
        let report = download_snapshot(config).map_err(|error| error.to_string())?;
        println!(
            "downloaded snapshot to {}: bytes={} chunks={} transport_sha256={}; Core UTXO authentication is still required",
            report.output.display(),
            report.bytes,
            report.chunks,
            report.transport_sha256.to_lower_hex_string(),
        );
        return Ok(());
    }
    if let Some(OfflineAction::VerifyStorage {
        max_segments,
        max_bytes,
    }) = &options.offline_action
    {
        let data_dir = options
            .data_dir
            .as_ref()
            .expect("storage audit parser requires data directory");
        let _read_lock = DataDirectoryReadLock::acquire(data_dir)?;
        let report = PrunedBlockLedger::audit(data_dir.join("blocks"), *max_segments, *max_bytes)
            .map_err(|error| error.to_string())?;
        println!(
            "{}",
            serde_json::to_string_pretty(&report)
                .expect("ledger audit report serialization is infallible")
        );
        if !report.complete || !report.issues.is_empty() {
            return Err(
                "read-only storage audit found issues; no repair action was executed".to_owned(),
            );
        }
        return Ok(());
    }
    if let Some(OfflineAction::PruneStorage {
        through_height,
        plan_token,
    }) = &options.offline_action
    {
        let data_dir = options
            .data_dir
            .as_ref()
            .expect("manual prune parser requires data directory");
        let blocks = data_dir.join("blocks");
        if let Some(plan_token) = plan_token {
            let _exclusive_lock = DataDirectoryLock::acquire(data_dir, options.network)?;
            let audit = PrunedBlockLedger::audit(
                &blocks,
                MAX_STORAGE_AUDIT_SEGMENTS,
                MAX_STORAGE_AUDIT_BYTES,
            )
            .map_err(|error| error.to_string())?;
            if !audit.complete || !audit.issues.is_empty() {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::json!({
                        "applied": false,
                        "audit": audit,
                    }))
                    .expect("manual prune audit serialization is infallible")
                );
                return Err(
                    "manual prune refused because the prerequisite freezer audit is not clean"
                        .to_owned(),
                );
            }
            let ledger =
                PrunedBlockLedger::open_persisted(&blocks).map_err(|error| error.to_string())?;
            let plan = ledger
                .plan_prune_through(*through_height, MIN_PRUNE_RETENTION_BLOCKS)
                .map_err(|error| error.to_string())?;
            validate_manual_prune_indexes(data_dir, options.network, &plan)?;
            let plan = ledger
                .apply_prune_plan(*through_height, MIN_PRUNE_RETENTION_BLOCKS, plan_token)
                .map_err(|error| error.to_string())?;
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({
                    "applied": true,
                    "plan": plan,
                }))
                .expect("manual prune result serialization is infallible")
            );
        } else {
            let _read_lock = DataDirectoryReadLock::acquire(data_dir)?;
            let plan = PrunedBlockLedger::plan_prune_read_only(
                &blocks,
                *through_height,
                MIN_PRUNE_RETENTION_BLOCKS,
            )
            .map_err(|error| error.to_string())?;
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({
                    "applied": false,
                    "plan": plan,
                }))
                .expect("manual prune plan serialization is infallible")
            );
        }
        return Ok(());
    }
    if let Some(OfflineAction::ReindexFromFreezer { output }) = &options.offline_action {
        return reindex_from_complete_freezer(&options, output);
    }
    if let Some(OfflineAction::ReindexChainstate { output }) = &options.offline_action {
        return reindex_chainstate_from_peers(&options, output, local_nonce).await;
    }
    if matches!(
        options.offline_action.as_ref(),
        Some(OfflineAction::VerifyChain { .. })
    ) {
        let data_dir = options
            .data_dir
            .as_ref()
            .expect("chain verification parser requires data directory");
        require_existing_verify_chain_data(data_dir, options.network)?;
    }
    if options.network_execution.is_experimental() {
        rbtc_warn!(
            "WARNING: experimental {} block execution is enabled for validation only; \
             this build is not a production full node and must not be trusted with funds",
            options.network
        );
    }
    if options.data_dir.is_some() {
        rbtc_info!("{}", startup_configuration_summary(&options));
    }
    if options.mempool_full_rbf {
        rbtc_info!("peer transaction admission full-RBF policy enabled");
    }
    let _data_directory_lock = options
        .data_dir
        .as_ref()
        .map(|data_dir| {
            fs::create_dir_all(data_dir).map_err(|error| {
                format!("create data directory {}: {error}", data_dir.display())
            })?;
            DataDirectoryLock::acquire(data_dir, options.network)
        })
        .transpose()?;
    if let Some(data_dir) = &options.data_dir {
        if data_dir.join(VALIDATION_OWNER_FILE).exists()
            && options.validation_target.is_none()
            && options.complete_assumeutxo.is_none()
            && options.background_assumeutxo.is_none()
        {
            return Err(
                "data directory contains an incomplete validation/reindex owner marker and cannot be promoted to ordinary service"
                    .to_owned(),
            );
        }
        if options.offline_action.is_none() {
            let disk_space = DiskSpaceMonitor::for_options(&options, data_dir.clone())
                .check()
                .map_err(|error| error.message)?;
            rbtc_info!(
                "startup disk preflight total={} available={} required={} reserve={}",
                disk_space.total_bytes,
                disk_space.available_bytes,
                disk_space.required_bytes,
                disk_space.reserve_bytes
            );
        }
        let data_format_manifest_missing =
            validate_data_format_manifest(data_dir, options.network)?;
        preflight_data_dir(
            data_dir,
            options.network,
            &options.deployments,
            options.network_execution.is_experimental(),
        )?;
        if data_format_manifest_missing {
            publish_data_format_manifest(data_dir, options.network)?;
        }
    }
    match &options.offline_action {
        Some(OfflineAction::UtxoActivityReport) => return report_utxo_activity(&options),
        Some(OfflineAction::RetierUtxos { hot_window_blocks }) => {
            return retier_utxos(&options, *hot_window_blocks);
        }
        Some(OfflineAction::VerifyChain {
            depth,
            max_block_bytes,
        }) => return verify_chain_offline(&options, *depth, *max_block_bytes),
        Some(
            OfflineAction::DownloadCoreSnapshot(_)
            | OfflineAction::VerifyStorage { .. }
            | OfflineAction::ReindexFromFreezer { .. }
            | OfflineAction::ReindexChainstate { .. }
            | OfflineAction::PruneStorage { .. },
        )
        | None => {}
    }
    if let Some(validation_dir) = &options.complete_assumeutxo {
        prepare_assumeutxo_validation(&options, validation_dir)?;
        DiskSpaceMonitor::for_options(&options, validation_dir.clone())
            .check()
            .map_err(|error| error.message)?;
    }
    if let Some(validation_dir) = &options.background_assumeutxo {
        prepare_assumeutxo_validation(&options, validation_dir)?;
        DiskSpaceMonitor::for_options(&options, validation_dir.clone())
            .check()
            .map_err(|error| error.message)?;
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

fn reindex_from_complete_freezer(
    options: &Options,
    output: &std::path::Path,
) -> Result<(), String> {
    let started = Instant::now();
    let source = options
        .data_dir
        .as_ref()
        .expect("freezer reindex parser requires source data directory");
    require_existing_reindex_source(source, options.network)?;
    let _source_lock = DataDirectoryLock::acquire(source, options.network)?;
    let header_store =
        RedbHeaderStore::open(source.join("headers.redb")).map_err(|error| error.to_string())?;
    let headers = header_store
        .load_dag_with_deployments(options.deployments.clone(), unix_time()?)
        .map_err(|error| error.to_string())?;
    options
        .ibd_policy
        .ensure_minimum_chainwork(&headers)
        .map_err(|error| error.to_string())?;
    let active_tip = headers.active_tip();
    let source_audit = PrunedBlockLedger::audit(
        source.join("blocks"),
        MAX_STORAGE_AUDIT_SEGMENTS,
        MAX_STORAGE_AUDIT_BYTES,
    )
    .map_err(|error| error.to_string())?;
    require_complete_reindex_history(&source_audit, active_tip.height)?;

    let target = rbtc::execution_store::ExecutionTip {
        height: active_tip.height,
        hash: active_tip.hash,
    };
    let (output, _output_lock) = prepare_reindex_output(source, output, options.network, target)?;
    let output_headers = prepare_reindex_headers(&output, &headers, &options.deployments)?;
    let chainstate = RedbChainStore::open_with_options(
        output.join("chainstate.redb"),
        options.network,
        ChainStoreOptions {
            quick_repair: options.validation_limits.quick_repair,
            cache_size_bytes: options.cache.bulk_validation_bytes,
            ..ChainStoreOptions::default()
        },
    )
    .map_err(|error| error.to_string())?;
    chainstate
        .execution()
        .bind_consensus_config(
            &options.deployments.consensus_id(),
            &options.deployments.legacy_consensus_id(),
            &DeploymentConfig::for_network(options.network).consensus_id(),
        )
        .map_err(|error| error.to_string())?;
    if target.height > 0 {
        chainstate
            .execution()
            .bind_validation_target(target)
            .map_err(|error| error.to_string())?;
    }
    let source_ledger = PrunedBlockLedger::open_persisted(source.join("blocks"))
        .map_err(|error| error.to_string())?;
    let output_ledger = PrunedBlockLedger::open(output.join("blocks"), options.ledger_retention)
        .map_err(|error| error.to_string())?;
    let auxiliary_indexes = AuxiliaryIndexes::open(&output, options.network, options.indexes)?;
    recover_local_reindex_staging(
        &options.deployments,
        &output_headers,
        &chainstate,
        &output_ledger,
    )?;
    recover_local_reindex_indexes(
        options,
        &output_headers,
        &chainstate,
        &source_ledger,
        &auxiliary_indexes,
    )?;
    if validate_data_format_manifest(&output, options.network)? {
        publish_data_format_manifest(&output, options.network)?;
    }

    let initial_height = run_local_reindex_batches(
        options,
        &output,
        target,
        &output_headers,
        &chainstate,
        &source_ledger,
        &output_ledger,
        &auxiliary_indexes,
    )?;
    finish_local_reindex(
        options,
        &output,
        target,
        &output_headers,
        &chainstate,
        Some(&auxiliary_indexes),
    )?;
    print_freezer_reindex_report(options, source, &output, target, initial_height, started);
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn run_local_reindex_batches(
    options: &Options,
    output: &std::path::Path,
    target: rbtc::execution_store::ExecutionTip,
    headers: &HeaderDag,
    chainstate: &RedbChainStore,
    source_ledger: &PrunedBlockLedger,
    output_ledger: &PrunedBlockLedger,
    auxiliary_indexes: &AuxiliaryIndexes,
) -> Result<u32, String> {
    let initial_height = chainstate
        .execution()
        .tip()
        .map_err(|error| error.to_string())?
        .height;
    loop {
        let tip = chainstate
            .execution()
            .tip()
            .map_err(|error| error.to_string())?;
        if tip.height >= target.height {
            return Ok(initial_height);
        }
        DiskSpaceMonitor::for_options(options, output.to_path_buf())
            .check()
            .map_err(|error| error.message)?;
        let first_height = tip
            .height
            .checked_add(1)
            .ok_or_else(|| "reindex height overflow".to_owned())?;
        let remaining = target.height - tip.height;
        let max_blocks = remaining.min(
            u32::try_from(options.validation_limits.max_blocks_per_batch)
                .expect("validation batch bound fits u32"),
        );
        let batch = source_ledger
            .read_block_batch(first_height, max_blocks, DEFAULT_VERIFY_CHAIN_BLOCK_BYTES)
            .map_err(|error| error.to_string())?;
        execute_local_reindex_batch(
            options,
            headers,
            chainstate,
            output_ledger,
            auxiliary_indexes,
            &batch,
        )?;
        let current = chainstate
            .execution()
            .tip()
            .map_err(|error| error.to_string())?;
        rbtc_info!(
            "freezer reindex progress {} / {} blocks at {}:{}",
            current.height,
            target.height,
            current.height,
            current.hash
        );
        if !options.validation_limits.pause_between_batches.is_zero() {
            std::thread::sleep(options.validation_limits.pause_between_batches);
        }
    }
}

fn recover_local_reindex_indexes(
    options: &Options,
    headers: &HeaderDag,
    chainstate: &RedbChainStore,
    source_ledger: &PrunedBlockLedger,
    indexes: &AuxiliaryIndexes,
) -> Result<(), String> {
    let execution_tip = chainstate
        .execution()
        .tip()
        .map_err(|error| error.to_string())?;
    for (kind, index) in indexes.enabled() {
        let label = auxiliary_index_label(kind);
        loop {
            let tip = index.tip().map_err(|error| error.to_string())?;
            if tip.height <= execution_tip.height
                && headers
                    .active_header_at(tip.height)
                    .is_some_and(|header| header.hash == tip.hash)
            {
                break;
            }
            index.disconnect_tip().map_err(|error| error.to_string())?;
        }
        loop {
            let tip = index.tip().map_err(|error| error.to_string())?;
            if tip.height >= execution_tip.height {
                break;
            }
            let first_height = tip
                .height
                .checked_add(1)
                .ok_or_else(|| format!("{label} index height overflow"))?;
            let count = (execution_tip.height - tip.height).min(
                u32::try_from(options.validation_limits.max_blocks_per_batch)
                    .expect("validation batch bound fits u32"),
            );
            let raw = source_ledger
                .read_block_batch(first_height, count, DEFAULT_VERIFY_CHAIN_BLOCK_BYTES)
                .map_err(|error| error.to_string())?;
            let blocks = raw
                .blocks
                .iter()
                .map(|bytes| deserialize::<Block>(bytes).map_err(|error| error.to_string()))
                .collect::<Result<Vec<_>, _>>()?;
            let expected = (0..blocks.len())
                .map(|offset| {
                    let height = first_height
                        .checked_add(u32::try_from(offset).expect("index batch fits u32"))
                        .ok_or_else(|| format!("{label} index height overflow"))?;
                    headers
                        .active_header_at(height)
                        .ok_or_else(|| format!("missing active header at height {height}"))
                })
                .collect::<Result<Vec<_>, String>>()?;
            let mut block_undos = Vec::with_capacity(blocks.len());
            for (expected, block) in expected.iter().zip(&blocks) {
                validate_archive_block(
                    &options.deployments,
                    headers,
                    expected.height,
                    expected.hash,
                    block,
                )?;
                block_undos.push(if kind == AuxiliaryIndexKind::Transaction {
                    Vec::new()
                } else {
                    chainstate
                        .undos()
                        .get(expected.hash)
                        .map_err(|error| error.to_string())?
                        .ok_or_else(|| {
                            format!(
                                "cannot resume {label} index at height {} because its execution undo was pruned; restart the explicit reindex with an empty output directory",
                                expected.height
                            )
                        })?
                });
            }
            let batch = expected
                .iter()
                .zip(&blocks)
                .zip(&block_undos)
                .map(|((expected, block), undos)| (expected.height, block, undos.as_slice()))
                .collect::<Vec<_>>();
            index
                .connect_batch(&batch)
                .map_err(|error| error.to_string())?;
        }
    }
    Ok(())
}

fn finish_local_reindex(
    options: &Options,
    output: &std::path::Path,
    target: rbtc::execution_store::ExecutionTip,
    headers: &HeaderDag,
    chainstate: &RedbChainStore,
    open_indexes: Option<&AuxiliaryIndexes>,
) -> Result<(), String> {
    let check = verify_freezer_cross_store(
        output,
        headers,
        chainstate,
        target.height,
        options.ledger_retention.max_blocks,
        DEFAULT_VERIFY_CHAIN_BLOCK_BYTES,
    )?;
    if !check.issues.is_empty() {
        return Err(format!(
            "rebuilt output failed final cross-store verification: {}",
            check.issues.join("; ")
        ));
    }
    let owned_indexes = open_indexes
        .is_none()
        .then(|| AuxiliaryIndexes::open(output, options.network, options.indexes))
        .transpose()?;
    let indexes = open_indexes
        .or(owned_indexes.as_ref())
        .expect("one index set");
    for (kind, index) in indexes.enabled() {
        let tip = index.tip().map_err(|error| error.to_string())?;
        if tip != target {
            return Err(format!(
                "rebuilt {} index stopped at {}:{}, expected {}:{}; output was not promoted",
                auxiliary_index_label(kind),
                tip.height,
                tip.hash,
                target.height,
                target.hash
            ));
        }
    }
    if target.height > 0 {
        chainstate
            .execution()
            .clear_completed_validation_target(target)
            .map_err(|error| error.to_string())?;
    }
    promote_reindex_output(output)?;
    Ok(())
}

fn print_freezer_reindex_report(
    options: &Options,
    source: &std::path::Path,
    output: &std::path::Path,
    target: rbtc::execution_store::ExecutionTip,
    initial_height: u32,
    started: Instant,
) {
    let report = FreezerReindexReport {
        schema_version: 1,
        network: options.network.to_string(),
        source: source.display().to_string(),
        output: output.display().to_string(),
        target_height: target.height,
        target_hash: target.hash.to_string(),
        executed_blocks: target.height.saturating_sub(initial_height),
        validation_batch_size: options.validation_limits.max_blocks_per_batch,
        elapsed_seconds: started.elapsed().as_secs(),
        independently_validated: true,
        source_freezer_complete: true,
        promoted: true,
    };
    println!(
        "{}",
        serde_json::to_string_pretty(&report)
            .expect("freezer reindex report serialization is infallible")
    );
}

async fn reindex_chainstate_from_peers(
    options: &Options,
    output: &std::path::Path,
    local_nonce: u64,
) -> Result<(), String> {
    let started = Instant::now();
    let source = options
        .data_dir
        .as_ref()
        .expect("peer reindex parser requires source data directory");
    require_existing_reindex_header_source(source, options.network)?;
    let _source_lock = DataDirectoryLock::acquire(source, options.network)?;
    let source_headers = RedbHeaderStore::open(source.join("headers.redb"))
        .map_err(|error| error.to_string())?
        .load_dag_with_deployments(options.deployments.clone(), unix_time()?)
        .map_err(|error| error.to_string())?;
    options
        .ibd_policy
        .ensure_minimum_chainwork(&source_headers)
        .map_err(|error| error.to_string())?;
    let active_tip = source_headers.active_tip();
    let target = rbtc::execution_store::ExecutionTip {
        height: active_tip.height,
        hash: active_tip.hash,
    };
    let (output, _output_lock) = prepare_reindex_output(source, output, options.network, target)?;
    prepare_reindex_headers(&output, &source_headers, &options.deployments)?;
    if validate_data_format_manifest(&output, options.network)? {
        publish_data_format_manifest(&output, options.network)?;
    }

    let mut validation_options = options.clone();
    validation_options.data_dir = Some(output.clone());
    validation_options.once = false;
    validation_options.network_execution = NetworkExecutionMode::Persistent;
    validation_options.explorer_listen = None;
    validation_options.wallet_api_files = None;
    validation_options.rpc_auth_token_file = None;
    validation_options.validation_target = (target.height > 0).then_some(ValidationTarget {
        height: target.height,
        block_hash: target.hash,
    });
    validation_options.offline_action = None;
    validation_options.cache.active_chainstate_bytes = options.cache.bulk_validation_bytes;
    validation_options.observer = None;
    if target.height > 0 {
        run_peer_pool(&validation_options, local_nonce, None, None).await?;
    } else {
        initialize_empty_reindex_output(&validation_options, &output)?;
    }

    let output_headers = RedbHeaderStore::open(output.join("headers.redb"))
        .map_err(|error| error.to_string())?
        .load_dag_with_deployments(options.deployments.clone(), unix_time()?)
        .map_err(|error| error.to_string())?;
    let chainstate = RedbChainStore::open_with_options(
        output.join("chainstate.redb"),
        options.network,
        ChainStoreOptions {
            quick_repair: options.validation_limits.quick_repair,
            cache_size_bytes: options.cache.bulk_validation_bytes,
            ..ChainStoreOptions::default()
        },
    )
    .map_err(|error| error.to_string())?;
    finish_local_reindex(options, &output, target, &output_headers, &chainstate, None)?;
    let report = PeerReindexReport {
        schema_version: 1,
        network: options.network.to_string(),
        source: source.display().to_string(),
        output: output.display().to_string(),
        target_height: target.height,
        target_hash: target.hash.to_string(),
        elapsed_seconds: started.elapsed().as_secs(),
        headers_authenticated_by_work: true,
        blocks_authenticated_by_consensus: true,
        promoted: true,
    };
    println!(
        "{}",
        serde_json::to_string_pretty(&report)
            .expect("peer reindex report serialization is infallible")
    );
    Ok(())
}

fn initialize_empty_reindex_output(
    options: &Options,
    output: &std::path::Path,
) -> Result<(), String> {
    let chainstate = RedbChainStore::open_with_options(
        output.join("chainstate.redb"),
        options.network,
        ChainStoreOptions {
            quick_repair: options.validation_limits.quick_repair,
            cache_size_bytes: options.cache.bulk_validation_bytes,
            ..ChainStoreOptions::default()
        },
    )
    .map_err(|error| error.to_string())?;
    chainstate
        .execution()
        .bind_consensus_config(
            &options.deployments.consensus_id(),
            &options.deployments.legacy_consensus_id(),
            &DeploymentConfig::for_network(options.network).consensus_id(),
        )
        .map_err(|error| error.to_string())?;
    PrunedBlockLedger::open(output.join("blocks"), options.ledger_retention)
        .map_err(|error| error.to_string())?;
    Ok(())
}

fn require_existing_reindex_header_source(
    source: &std::path::Path,
    network: Network,
) -> Result<(), String> {
    let metadata = fs::symlink_metadata(source).map_err(|error| {
        format!(
            "peer reindex requires an existing source {}: {error}",
            source.display()
        )
    })?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err("peer reindex source must be a directory, not a symlink".to_owned());
    }
    let headers = source.join("headers.redb");
    let metadata = fs::symlink_metadata(&headers)
        .map_err(|error| format!("peer reindex requires {}: {error}", headers.display()))?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err("peer reindex source headers must be a regular file".to_owned());
    }
    if validate_data_format_manifest(source, network)? {
        return Err("peer reindex requires an existing data-format manifest".to_owned());
    }
    Ok(())
}

fn require_existing_reindex_source(
    source: &std::path::Path,
    network: Network,
) -> Result<(), String> {
    require_existing_reindex_header_source(source, network)?;
    let blocks = source.join("blocks");
    let metadata = fs::symlink_metadata(&blocks)
        .map_err(|error| format!("freezer reindex requires {}: {error}", blocks.display()))?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(format!(
            "freezer reindex source {} has the wrong type",
            blocks.display()
        ));
    }
    Ok(())
}

fn require_complete_reindex_history(
    audit: &rbtc::ledger::LedgerAuditReport,
    target_height: u32,
) -> Result<(), String> {
    if !audit.complete || !audit.issues.is_empty() {
        return Err(
            "freezer reindex requires a complete clean source audit; no output was promoted"
                .to_owned(),
        );
    }
    let expected_range = if target_height == 0 {
        (None, None)
    } else {
        (Some(1), Some(target_height))
    };
    if (audit.first_height, audit.tip_height) != expected_range {
        return Err(format!(
            "local reindex is impossible: freezer range {:?}-{:?} does not cover required 1-{target_height}; use authenticated peers or AssumeUTXO in a fresh directory",
            audit.first_height, audit.tip_height
        ));
    }
    Ok(())
}

fn prepare_reindex_output(
    source: &std::path::Path,
    output: &std::path::Path,
    network: Network,
    target: rbtc::execution_store::ExecutionTip,
) -> Result<(PathBuf, DataDirectoryLock), String> {
    reject_directory_symlink(output, "reindex output directory")?;
    let claimable = match fs::read_dir(output) {
        Ok(mut entries) => entries.next().is_none(),
        Err(error) if error.kind() == io::ErrorKind::NotFound => true,
        Err(error) => {
            return Err(format!(
                "inspect reindex output {}: {error}",
                output.display()
            ));
        }
    };
    fs::create_dir_all(output)
        .map_err(|error| format!("create reindex output {}: {error}", output.display()))?;
    let source = fs::canonicalize(source)
        .map_err(|error| format!("resolve reindex source directory: {error}"))?;
    let output = fs::canonicalize(output)
        .map_err(|error| format!("resolve reindex output directory: {error}"))?;
    if source == output || source.starts_with(&output) || output.starts_with(&source) {
        return Err(
            "reindex output must be separate from and not contain or be contained by the source"
                .to_owned(),
        );
    }
    let lock = DataDirectoryLock::acquire(&output, network)?;
    let owner = ValidationDirectoryOwner::new(network, target.height, target.hash);
    let marker = output.join(VALIDATION_OWNER_FILE);
    if !claimable && !marker.exists() {
        return Err(
            "non-empty reindex output lacks the exact rBTC owner marker; choose an empty directory"
                .to_owned(),
        );
    }
    ensure_validation_directory_owner(&output, &owner, claimable, false)?;
    reject_unowned_reindex_entries(&output)?;
    for name in ["headers.redb", "chainstate.redb"] {
        let candidate = output.join(name);
        let source_file = source.join(name);
        if candidate.exists() && source_file.exists() && same_file(&candidate, &source_file)? {
            return Err(format!("reindex output {name} must not alias the source"));
        }
    }
    Ok((output, lock))
}

fn reject_unowned_reindex_entries(output: &std::path::Path) -> Result<(), String> {
    let allowed = [
        VALIDATION_OWNER_FILE,
        DATA_DIRECTORY_LOCK_FILE,
        DATA_FORMAT_MANIFEST_FILE,
        "headers.redb",
        "chainstate.redb",
        "peers.redb",
        "blocks",
    ];
    for entry in fs::read_dir(output)
        .map_err(|error| format!("inspect reindex output {}: {error}", output.display()))?
    {
        let entry = entry.map_err(|error| format!("inspect reindex output entry: {error}"))?;
        let name = entry.file_name();
        if !allowed.iter().any(|allowed| name == *allowed) {
            return Err(format!(
                "reindex output contains unowned entry {}; preserve it and choose an empty directory",
                entry.path().display()
            ));
        }
        let metadata = fs::symlink_metadata(entry.path())
            .map_err(|error| format!("inspect reindex output entry: {error}"))?;
        if metadata.file_type().is_symlink() {
            return Err("reindex output must not contain symlinks".to_owned());
        }
    }
    Ok(())
}

fn prepare_reindex_headers(
    output: &std::path::Path,
    source: &HeaderDag,
    deployments: &DeploymentConfig,
) -> Result<HeaderDag, String> {
    let store =
        RedbHeaderStore::open(output.join("headers.redb")).map_err(|error| error.to_string())?;
    let mut current = store
        .load_dag_with_deployments(deployments.clone(), unix_time()?)
        .map_err(|error| error.to_string())?;
    if store.len().map_err(|error| error.to_string())? != u64::from(current.active_tip().height) {
        return Err("reindex output headers contain an unexpected side branch".to_owned());
    }
    for height in 0..=current.active_tip().height {
        if current.active_header_at(height).map(|info| info.hash)
            != source.active_header_at(height).map(|info| info.hash)
        {
            return Err(format!(
                "reindex output header prefix diverges from source at height {height}"
            ));
        }
    }
    let mut next = current.active_tip().height.saturating_add(1);
    while next <= source.active_tip().height {
        let end = next
            .saturating_add(u32::try_from(MAX_HEADERS_PER_RESPONSE).expect("header bound fits u32"))
            .min(source.active_tip().height.saturating_add(1));
        let batch = (next..end)
            .map(|height| {
                source
                    .active_header_at(height)
                    .expect("source active chain contains every height")
                    .header
            })
            .collect::<Vec<_>>();
        let staged = current
            .stage_batch_contextual(&batch, unix_time()?)
            .map_err(|error| error.to_string())?;
        store
            .append_batch(&batch)
            .map_err(|error| error.to_string())?;
        let _ = staged.commit();
        next = end;
    }
    if current.active_tip().hash != source.active_tip().hash {
        return Err("reindex output headers did not reach the source maximum-work tip".to_owned());
    }
    Ok(current)
}

fn recover_local_reindex_staging(
    deployments: &DeploymentConfig,
    headers: &HeaderDag,
    chainstate: &RedbChainStore,
    ledger: &PrunedBlockLedger,
) -> Result<(), String> {
    let tip = chainstate
        .execution()
        .tip()
        .map_err(|error| error.to_string())?;
    if let Some(first_unexecuted) = tip.height.checked_add(1) {
        ledger
            .truncate_from(first_unexecuted)
            .map_err(|error| error.to_string())?;
    }
    let Some(staged) = ledger.staged().map_err(|error| error.to_string())? else {
        return Ok(());
    };
    if staged.manifest.first_height > tip.height {
        ledger.discard_staged().map_err(|error| error.to_string())?;
        return Ok(());
    }
    let validated_count = tip
        .height
        .checked_sub(staged.manifest.first_height)
        .and_then(|distance| distance.checked_add(1))
        .ok_or_else(|| "reindex staged height overflow".to_owned())?
        .min(staged.manifest.block_count);
    for (offset, raw) in staged
        .blocks
        .iter()
        .take(usize::try_from(validated_count).expect("staged block bound fits usize"))
        .enumerate()
    {
        let height = staged
            .manifest
            .first_height
            .checked_add(u32::try_from(offset).expect("staged offset fits u32"))
            .ok_or_else(|| "reindex staged height overflow".to_owned())?;
        let block: Block = deserialize(raw)
            .map_err(|error| format!("decode staged reindex block at {height}: {error}"))?;
        let expected = headers
            .active_header_at(height)
            .ok_or_else(|| format!("missing active header at reindex height {height}"))?;
        validate_archive_block(deployments, headers, height, expected.hash, &block)?;
    }
    ledger
        .commit_staged(validated_count)
        .map_err(|error| error.to_string())
}

fn execute_local_reindex_batch(
    options: &Options,
    headers: &HeaderDag,
    chainstate: &RedbChainStore,
    ledger: &PrunedBlockLedger,
    auxiliary_indexes: &AuxiliaryIndexes,
    batch: &LedgerBlockBatch,
) -> Result<(), String> {
    let current = chainstate
        .execution()
        .tip()
        .map_err(|error| error.to_string())?;
    if current.height.checked_add(1) != Some(batch.first_height) {
        return Err("freezer reindex batch does not extend the execution tip".to_owned());
    }
    let blocks = batch
        .blocks
        .iter()
        .map(|raw| {
            deserialize::<Block>(raw)
                .map_err(|error| format!("decode freezer reindex block: {error}"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let expected = (0..blocks.len())
        .map(|offset| {
            let height = batch
                .first_height
                .checked_add(u32::try_from(offset).expect("reindex batch offset fits u32"))
                .ok_or_else(|| "freezer reindex height overflow".to_owned())?;
            headers
                .active_header_at(height)
                .ok_or_else(|| format!("missing active header at reindex height {height}"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let validated = validate_downloaded_blocks(&options.deployments, headers, &expected, &blocks)
        .map_err(|error| error.to_string())?;
    let mut deployment_contexts = Vec::with_capacity(validated.len());
    let mut transaction_ids = Vec::with_capacity(validated.len());
    let mut serialized = Vec::with_capacity(validated.len());
    for (context, txids, raw) in validated {
        deployment_contexts.push(context);
        transaction_ids.push(txids);
        serialized.push(raw);
    }
    let (stage_result, prefetch_result) = std::thread::scope(|scope| {
        let prefetch = scope.spawn(|| {
            prefetch_prevalidated_active_block_utxos(chainstate, &blocks, &transaction_ids)
        });
        let stage = ledger.stage(batch.first_height, &serialized);
        (
            stage,
            prefetch
                .join()
                .expect("reindex UTXO prefetch worker must not panic"),
        )
    });
    stage_result.map_err(|error| error.to_string())?;
    let prefetched = prefetch_result.map_err(|error| error.to_string())?;
    let applied_blocks = connect_prevalidated_active_blocks_with_txids_and_utxos(
        chainstate,
        headers,
        &blocks,
        &transaction_ids,
        prefetched,
        u64::from(unix_time()?),
        DEFAULT_HOT_WINDOW_SECS,
        &deployment_contexts,
    )
    .map_err(|error| error.to_string())?;
    let index_batch = expected
        .iter()
        .zip(&blocks)
        .zip(&applied_blocks)
        .map(|((expected, block), applied)| {
            (expected.height, block, applied.transaction_undos.as_slice())
        })
        .collect::<Vec<_>>();
    std::thread::scope(|scope| {
        auxiliary_indexes
            .enabled()
            .map(|(_, index)| {
                scope.spawn(|| {
                    index
                        .connect_batch(&index_batch)
                        .map_err(|error| error.to_string())
                })
            })
            .collect::<Vec<_>>()
            .into_iter()
            .try_for_each(|job| {
                job.join()
                    .expect("local optional-index worker must not panic")
            })
    })?;
    ledger
        .commit_staged(u32::try_from(blocks.len()).expect("reindex batch bound fits u32"))
        .map_err(|error| error.to_string())?;
    prune_expired_block_undos(chainstate, headers, ledger)?;
    prune_expired_auxiliary_index_undos(auxiliary_indexes, ledger)?;
    Ok(())
}

fn promote_reindex_output(output: &std::path::Path) -> Result<(), String> {
    let marker = output.join(VALIDATION_OWNER_FILE);
    let metadata = fs::symlink_metadata(&marker)
        .map_err(|error| format!("inspect completed reindex owner marker: {error}"))?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err("completed reindex owner marker must be a regular file".to_owned());
    }
    fs::remove_file(&marker)
        .map_err(|error| format!("remove completed reindex owner marker: {error}"))?;
    sync_directory(output)
        .map_err(|error| format!("sync completed reindex output directory: {error}"))
}

fn validate_manual_prune_indexes(
    data_dir: &std::path::Path,
    network: Network,
    plan: &LedgerPrunePlan,
) -> Result<(), String> {
    let (Some(first_retained_height), Some(execution_height)) =
        (plan.first_retained_height, plan.tip_height)
    else {
        return Ok(());
    };
    let explorer_path = data_dir.join("explorer.redb");
    if explorer_path.exists() {
        let explorer =
            RedbExplorerIndex::open(explorer_path, network).map_err(|error| error.to_string())?;
        let tip = explorer.tip().map_err(|error| error.to_string())?;
        validate_prune_boundary(
            IndexBuildState {
                kind: IndexKind::Explorer,
                best_height: tip.height,
                required_from_height: 1,
                utxo_baseline_height: explorer
                    .baseline()
                    .map_err(|error| error.to_string())?
                    .map(|base| base.height),
            },
            first_retained_height,
            execution_height,
        )
        .map_err(|error| {
            format!(
                "manual prune would strand the enabled explorer index at height {}: {error:?}",
                tip.height
            )
        })?;
    }
    let wallet_path = data_dir.join("wallet.sqlite");
    if wallet_path.exists() {
        let best_height = read_persisted_wallet_sync_height(&wallet_path)
            .map_err(|error| error.to_string())?
            .ok_or_else(|| {
                "manual prune refuses a legacy wallet without a conservative sync cursor; start the node once with its descriptors to migrate it"
                    .to_owned()
            })?;
        validate_prune_boundary(
            IndexBuildState {
                kind: IndexKind::Wallet,
                best_height,
                required_from_height: 0,
                utxo_baseline_height: None,
            },
            first_retained_height,
            execution_height,
        )
        .map_err(|error| {
            format!(
                "manual prune would strand the enabled wallet index at height {best_height}: {error:?}"
            )
        })?;
    }
    for (file, kind) in [
        ("txindex.redb", AuxiliaryIndexKind::Transaction),
        ("spent-output-index.redb", AuxiliaryIndexKind::SpentOutput),
        ("basic-filter-index.redb", AuxiliaryIndexKind::BasicFilter),
    ] {
        let path = data_dir.join(file);
        if !path.exists() {
            continue;
        }
        let index =
            RedbAuxiliaryIndex::open(path, network, kind).map_err(|error| error.to_string())?;
        let tip = index.tip().map_err(|error| error.to_string())?;
        validate_prune_boundary(
            IndexBuildState {
                kind: auxiliary_policy_kind(kind),
                best_height: tip.height,
                required_from_height: 1,
                utxo_baseline_height: None,
            },
            first_retained_height,
            execution_height,
        )
        .map_err(|error| {
            format!(
                "manual prune would strand the {} index at height {}: {error:?}",
                auxiliary_index_label(kind),
                tip.height
            )
        })?;
    }
    Ok(())
}

fn require_existing_verify_chain_data(
    data_dir: &std::path::Path,
    network: Network,
) -> Result<(), String> {
    let metadata = fs::symlink_metadata(data_dir).map_err(|error| {
        format!(
            "offline chain verification requires an existing data directory {}: {error}",
            data_dir.display()
        )
    })?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err("offline chain verification data directory must not be a symlink".to_owned());
    }
    for required in ["headers.redb", "chainstate.redb"] {
        let path = data_dir.join(required);
        let metadata = fs::symlink_metadata(&path).map_err(|error| {
            format!(
                "offline chain verification requires existing {}: {error}",
                path.display()
            )
        })?;
        if metadata.file_type().is_symlink() || !metadata.is_file() {
            return Err(format!(
                "offline chain verification requires {} to be a regular file",
                path.display()
            ));
        }
    }
    let blocks = data_dir.join("blocks");
    let metadata = fs::symlink_metadata(&blocks).map_err(|error| {
        format!(
            "offline chain verification requires existing freezer {}: {error}",
            blocks.display()
        )
    })?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err("offline chain verification freezer must not be a symlink".to_owned());
    }
    if validate_data_format_manifest(data_dir, network)? {
        return Err(
            "offline chain verification requires an existing data-format manifest; start the matching release once to migrate a legacy directory"
                .to_owned(),
        );
    }
    Ok(())
}

fn verify_chain_offline(options: &Options, depth: u32, max_block_bytes: u64) -> Result<(), String> {
    let data_dir = options
        .data_dir
        .as_ref()
        .expect("chain verification parser requires data directory");
    let header_store =
        RedbHeaderStore::open(data_dir.join("headers.redb")).map_err(|error| error.to_string())?;
    let header_records = header_store.len().map_err(|error| error.to_string())?;
    let headers = header_store
        .load_dag_with_deployments(options.deployments.clone(), unix_time()?)
        .map_err(|error| error.to_string())?;
    let header = headers.active_tip();
    let chainstate = RedbChainStore::open(data_dir.join("chainstate.redb"), options.network)
        .map_err(|error| error.to_string())?;
    let execution = chainstate
        .execution()
        .tip()
        .map_err(|error| error.to_string())?;
    let assumed = chainstate
        .execution()
        .assumed_snapshot()
        .map_err(|error| error.to_string())?;
    let utxos = chainstate.tier_stats().map_err(|error| error.to_string())?;
    let mut issues = Vec::new();
    let mut repair_plan = Vec::new();
    if headers
        .active_header_at(execution.height)
        .map(|info| info.hash)
        != Some(execution.hash)
    {
        issues.push(
            "execution tip is not the maximum-work header chain at the same height".to_owned(),
        );
        repair_plan.push(
            "preserve the directory and rebuild chainstate separately from authenticated active-chain blocks"
                .to_owned(),
        );
    }
    if execution.height > header.height {
        issues.push("execution tip is above the validated header tip".to_owned());
    }
    if chainstate
        .undos()
        .pending_transition()
        .map_err(|error| error.to_string())?
        .is_some()
    {
        issues
            .push("an interrupted chainstate transition remains after exclusive reopen".to_owned());
        repair_plan.push(
            "do not serve the directory; preserve it and complete recovery with the same release"
                .to_owned(),
        );
    }
    let freezer = verify_freezer_cross_store(
        data_dir,
        &headers,
        &chainstate,
        execution.height,
        depth,
        max_block_bytes,
    )?;
    issues.extend(freezer.issues);
    repair_plan.extend(freezer.repair_plan);
    repair_plan.dedup();
    let mut report = OfflineVerifyChainReport {
        schema_version: 1,
        network: options.network.to_string(),
        exclusive: true,
        database_open_mode: "recovery-capable; may complete redb crash recovery",
        header_records,
        header_height: header.height,
        header_hash: header.hash.to_string(),
        chainwork: header.chainwork.to_string(),
        execution_height: execution.height,
        execution_hash: execution.hash.to_string(),
        assumed_snapshot_height: assumed.map(|snapshot| snapshot.base.height),
        independently_validated: assumed.is_none(),
        utxo_hot: utxos.hot,
        utxo_cold: utxos.cold,
        freezer_first_height: freezer.first_height,
        freezer_tip_height: freezer.tip_height,
        requested_depth: depth,
        checked_first_height: freezer.checked_first_height,
        checked_blocks: freezer.checked_blocks,
        checked_block_record_bytes: freezer.checked_block_record_bytes,
        checked_undos: freezer.checked_undos,
        clean: false,
        issues,
        repair_plan,
    };
    report.clean = report.issues.is_empty();
    print_offline_verify_chain_report(&report);
    if !report.clean {
        return Err(
            "offline chain verification found cross-store inconsistencies; no semantic repair was executed"
                .to_owned(),
        );
    }
    Ok(())
}

fn print_offline_verify_chain_report(report: &OfflineVerifyChainReport) {
    println!(
        "{}",
        serde_json::to_string_pretty(report)
            .expect("offline chain verification report serialization is infallible")
    );
}

fn verify_freezer_cross_store(
    data_dir: &std::path::Path,
    headers: &HeaderDag,
    chainstate: &RedbChainStore,
    execution_height: u32,
    depth: u32,
    max_block_bytes: u64,
) -> Result<FreezerCrossCheck, String> {
    let audit = PrunedBlockLedger::audit(
        data_dir.join("blocks"),
        MAX_STORAGE_AUDIT_SEGMENTS,
        MAX_STORAGE_AUDIT_BYTES,
    )
    .map_err(|error| error.to_string())?;
    let mut result = FreezerCrossCheck {
        first_height: audit.first_height,
        tip_height: audit.tip_height,
        checked_first_height: None,
        checked_blocks: 0,
        checked_block_record_bytes: 0,
        checked_undos: 0,
        issues: Vec::new(),
        repair_plan: Vec::new(),
    };
    if !audit.complete || !audit.issues.is_empty() {
        result
            .issues
            .push("the complete freezer integrity audit is not clean".to_owned());
        result.repair_plan.extend(audit.repair_plan);
        return Ok(result);
    }
    if audit.tip_height != (execution_height > 0).then_some(execution_height) {
        result
            .issues
            .push("freezer tip does not equal the durable execution tip".to_owned());
        result.repair_plan.push(
            "restore the missing active-chain freezer suffix from authenticated blocks before reopening service"
                .to_owned(),
        );
    }
    if let (Some(first), Some(tip)) = (audit.first_height, audit.tip_height) {
        verify_retained_suffix(
            data_dir,
            headers,
            chainstate,
            first,
            tip,
            depth,
            max_block_bytes,
            &mut result,
        )?;
    }
    Ok(result)
}

#[allow(clippy::too_many_arguments)]
fn verify_retained_suffix(
    data_dir: &std::path::Path,
    headers: &HeaderDag,
    chainstate: &RedbChainStore,
    first: u32,
    tip: u32,
    depth: u32,
    max_block_bytes: u64,
    result: &mut FreezerCrossCheck,
) -> Result<(), String> {
    let requested_first = tip.saturating_add(1).saturating_sub(depth).max(first);
    let ledger = PrunedBlockLedger::open_persisted(data_dir.join("blocks"))
        .map_err(|error| error.to_string())?;
    let blocks = ledger
        .verify_block_hashes(requested_first, tip, max_block_bytes)
        .map_err(|error| error.to_string())?;
    result.checked_first_height = Some(requested_first);
    result.checked_blocks =
        u32::try_from(blocks.hashes.len()).expect("verification depth bounds block count");
    result.checked_block_record_bytes = blocks.verified_record_bytes;
    let mut block_mismatch = false;
    let mut missing_undo = false;
    for (height, hash) in blocks.hashes {
        block_mismatch |= headers.active_header_at(height).map(|info| info.hash) != Some(hash);
        if chainstate
            .undos()
            .get(hash)
            .map_err(|error| error.to_string())?
            .is_some()
        {
            result.checked_undos = result.checked_undos.saturating_add(1);
        } else {
            missing_undo = true;
        }
    }
    if block_mismatch {
        result.issues.push(
            "a retained block payload does not match the maximum-work header at its height"
                .to_owned(),
        );
        result.repair_plan.push(
            "replace the mismatched freezer archive only from an authenticated active-chain source"
                .to_owned(),
        );
    }
    if missing_undo {
        result
            .issues
            .push("disconnect undo is missing for a checked retained block".to_owned());
        result.repair_plan.push(
            "do not advertise reorganization readiness; rebuild chainstate separately".to_owned(),
        );
    }
    Ok(())
}

fn startup_configuration_summary(options: &Options) -> String {
    let dns = match &options.dns_seeds {
        None => "pinned".to_owned(),
        Some(seeds) if seeds.is_empty() => "disabled".to_owned(),
        Some(seeds) => format!("custom:{}", seeds.len()),
    };
    let validation = if options.background_assumeutxo.is_some() {
        "background"
    } else if options.complete_assumeutxo.is_some() {
        "complete"
    } else if options.validation_target.is_some() {
        "bounded"
    } else {
        "none"
    };
    let inbound = options
        .inbound_listen
        .map_or_else(|| "disabled".to_owned(), |address| address.to_string());
    format!(
        "startup configuration network={} data_dir={} preferred_peers={} dns={} automatic_hot_standbys={} once={} full_rbf={} txindex={} spent_output_index={} block_filter_index={} inbound={} max_inbound_peers={} max_inbound_per_ip={} max_upload_bytes_per_day={} inbound_requests_per_minute={} mempool_max_transactions={} mempool_max_bytes={} cache_active_bytes={} cache_background_bytes={} cache_bulk_bytes={} prune_blocks={} prune_bytes={} minimum_free_bytes={} log_level={} log_max_bytes={} log_max_files={} validation={} validation_batch={} validation_pause_ms={} validation_quick_repair={} api={} rpc={} wallet={}",
        options.network,
        options
            .data_dir
            .as_ref()
            .map_or_else(|| "-".to_owned(), |path| path.display().to_string()),
        options.remotes.len(),
        dns,
        options.resources.automatic_hot_standbys,
        options.once,
        options.mempool_full_rbf,
        options.indexes.transaction,
        options.indexes.spent_output,
        options.indexes.basic_filter,
        inbound,
        options.inbound_limits.max_connections,
        options.inbound_limits.max_connections_per_ip,
        options.inbound_limits.max_upload_bytes_per_day,
        options.inbound_limits.max_requests_per_minute,
        options.resources.mempool_max_transactions,
        options.resources.mempool_max_bytes,
        options.cache.active_chainstate_bytes,
        options.cache.background_chainstate_bytes,
        options.cache.bulk_validation_bytes,
        options.ledger_retention.max_blocks,
        options.ledger_retention.max_bytes,
        options.minimum_free_bytes,
        options.logging.level.as_str(),
        options.logging.max_file_bytes,
        options.logging.max_files,
        validation,
        options.validation_limits.max_blocks_per_batch,
        options.validation_limits.pause_between_batches.as_millis(),
        options.validation_limits.quick_repair,
        options.explorer_listen.is_some(),
        options.rpc_auth_token_file.is_some(),
        options.wallet_api_files.is_some(),
    )
}

fn percentage_units(part: u64, total: u64) -> u64 {
    if total == 0 {
        0
    } else {
        let units = u128::from(part) * u128::from(100 * PERCENT_DECIMAL_SCALE) / u128::from(total);
        u64::try_from(units).expect("percentage is at most 100 percent")
    }
}

fn format_percentage(part: u64, total: u64) -> String {
    let units = percentage_units(part, total);
    format!(
        "{}.{:05}",
        units / PERCENT_DECIMAL_SCALE,
        units % PERCENT_DECIMAL_SCALE
    )
}

fn spent_age_quantile(rows: &[(u32, u64)], samples: u64, numerator: u64) -> Option<u32> {
    if samples == 0 || numerator == 0 || numerator > 1_000 {
        return None;
    }
    let rank = u128::from(samples)
        .checked_mul(u128::from(numerator))?
        .div_ceil(1_000);
    let mut cumulative = 0_u128;
    rows.iter().find_map(|(age, count)| {
        cumulative = cumulative.checked_add(u128::from(*count))?;
        (cumulative >= rank).then_some(*age)
    })
}

fn format_tier_probes_per_spend(hot_hits: u64, samples: u64) -> String {
    if samples == 0 {
        return "unavailable".to_owned();
    }
    let cold_misses = samples.saturating_sub(hot_hits);
    let fractional =
        u128::from(cold_misses) * u128::from(PERCENT_DECIMAL_SCALE) / u128::from(samples);
    format!(
        "{}.{:05}",
        1 + fractional / u128::from(PERCENT_DECIMAL_SCALE),
        fractional % u128::from(PERCENT_DECIMAL_SCALE)
    )
}

struct UtxoPopulation {
    window_counts: [u64; UTXO_ACTIVITY_WINDOWS.len()],
    window_bytes: [u64; UTXO_ACTIVITY_WINDOWS.len()],
    total_count: u64,
    total_bytes: u64,
}

fn scan_utxo_population(
    chainstate: &RedbChainStore,
    tip_height: u32,
) -> Result<UtxoPopulation, String> {
    let mut population = UtxoPopulation {
        window_counts: [0; UTXO_ACTIVITY_WINDOWS.len()],
        window_bytes: [0; UTXO_ACTIVITY_WINDOWS.len()],
        total_count: 0,
        total_bytes: 0,
    };
    chainstate
        .visit_utxo_snapshot(|_, utxo| {
            let age =
                tip_height
                    .checked_sub(utxo.height)
                    .ok_or(rbtc::utxo::UtxoError::Malformed(
                        "UTXO creation height exceeds execution tip",
                    ))?;
            let record_bytes = u64::try_from(36_usize + 29 + utxo.script_pubkey.len())
                .map_err(|_| rbtc::utxo::UtxoError::Malformed("UTXO record length exceeds u64"))?;
            population.total_count = population
                .total_count
                .checked_add(1)
                .ok_or(rbtc::utxo::UtxoError::Malformed("UTXO count overflow"))?;
            population.total_bytes = population
                .total_bytes
                .checked_add(record_bytes)
                .ok_or(rbtc::utxo::UtxoError::Malformed("UTXO byte count overflow"))?;
            for (index, window) in UTXO_ACTIVITY_WINDOWS.iter().enumerate() {
                if age <= *window {
                    population.window_counts[index] =
                        population.window_counts[index].checked_add(1).ok_or(
                            rbtc::utxo::UtxoError::Malformed("window UTXO count overflow"),
                        )?;
                    population.window_bytes[index] = population.window_bytes[index]
                        .checked_add(record_bytes)
                        .ok_or(rbtc::utxo::UtxoError::Malformed(
                            "window UTXO byte count overflow",
                        ))?;
                }
            }
            Ok(())
        })
        .map_err(|error| error.to_string())?;
    Ok(population)
}

/// Scans chainstate in fixed-size key order and combines the current UTXO
/// population with reorg-consistent historical spend ages. The scan is
/// intentionally offline so it neither competes with IBD nor perturbs cache
/// measurements.
fn report_utxo_activity(options: &Options) -> Result<(), String> {
    let data_dir = options
        .data_dir
        .as_ref()
        .expect("activity report parser requires data directory");
    let chainstate = RedbChainStore::open(data_dir.join("chainstate.redb"), options.network)
        .map_err(|error| error.to_string())?;
    let tip = chainstate
        .execution()
        .tip()
        .map_err(|error| error.to_string())?;
    let spent = chainstate
        .spent_age_histogram()
        .map_err(|error| error.to_string())?;
    let population = scan_utxo_population(&chainstate, tip.height)?;

    let complete_spend_coverage =
        spent.start_height == Some(1) && spent.end_height == Some(tip.height);
    println!(
        "UTXO activity report network={} tip={}:{} utxos={} estimated_record_bytes={}",
        options.network, tip.height, tip.hash, population.total_count, population.total_bytes
    );
    match (spent.start_height, spent.end_height) {
        (Some(start), Some(end)) => println!(
            "spent-age coverage={start}-{end} samples={} complete_genesis_to_tip={complete_spend_coverage}",
            spent.samples
        ),
        (None, None) => println!("spent-age coverage=none samples=0 complete_genesis_to_tip=false"),
        _ => unreachable!("chainstore rejects incomplete spent-age metadata"),
    }
    if spent.samples == 0 {
        println!("spent-age quantiles_blocks=unavailable");
    } else {
        println!(
            "spent-age quantiles_blocks p50={} p90={} p95={} p99={} p99.9={}",
            spent_age_quantile(&spent.rows, spent.samples, 500)
                .expect("non-empty validated histogram has a median"),
            spent_age_quantile(&spent.rows, spent.samples, 900)
                .expect("non-empty validated histogram has a p90"),
            spent_age_quantile(&spent.rows, spent.samples, 950)
                .expect("non-empty validated histogram has a p95"),
            spent_age_quantile(&spent.rows, spent.samples, 990)
                .expect("non-empty validated histogram has a p99"),
            spent_age_quantile(&spent.rows, spent.samples, 999)
                .expect("non-empty validated histogram has a p99.9"),
        );
    }
    println!(
        "window_blocks\tapprox_days\tspend_hits\tspend_hit_percent\testimated_tier_probes_per_spend\tcurrent_utxos\tcurrent_utxo_percent\testimated_record_bytes\testimated_byte_percent"
    );
    for (index, window) in UTXO_ACTIVITY_WINDOWS.iter().enumerate() {
        let spend_hits = spent.hits_within(*window);
        println!(
            "{window}\t{}\t{spend_hits}\t{}\t{}\t{}\t{}\t{}\t{}",
            window / 144,
            format_percentage(spend_hits, spent.samples),
            format_tier_probes_per_spend(spend_hits, spent.samples),
            population.window_counts[index],
            format_percentage(population.window_counts[index], population.total_count),
            population.window_bytes[index],
            format_percentage(population.window_bytes[index], population.total_bytes),
        );
    }

    if !complete_spend_coverage {
        println!(
            "recommendation=unavailable reason=incomplete_spend_history required_coverage=1-{}",
            tip.height
        );
    } else if spent.samples < MIN_ACTIVITY_RECOMMENDATION_SAMPLES {
        println!(
            "recommendation=unavailable reason=insufficient_samples samples={} required={MIN_ACTIVITY_RECOMMENDATION_SAMPLES}",
            spent.samples
        );
    } else if let Some(window) = UTXO_ACTIVITY_WINDOWS.iter().copied().find(|window| {
        percentage_units(spent.hits_within(*window), spent.samples)
            >= ACTIVITY_RECOMMENDATION_PERCENT_UNITS
    }) {
        println!(
            "recommendation_blocks={window} criterion=smallest_candidate_with_at_least_99.0_percent_historical_spend_hits"
        );
    } else {
        println!(
            "recommendation=unavailable reason=no_candidate_reaches_99.0_percent_historical_spend_hits"
        );
    }
    Ok(())
}

fn retier_utxos(options: &Options, hot_window_blocks: u32) -> Result<(), String> {
    let data_dir = options
        .data_dir
        .as_ref()
        .expect("UTXO re-tier parser requires data directory");
    let chainstate = RedbChainStore::open(data_dir.join("chainstate.redb"), options.network)
        .map_err(|error| error.to_string())?;
    if chainstate
        .execution()
        .assumed_snapshot()
        .map_err(|error| error.to_string())?
        .is_some()
    {
        return Err(
            "height-based UTXO re-tiering requires completed AssumeUTXO finalization".to_owned(),
        );
    }
    let tip = chainstate
        .execution()
        .tip()
        .map_err(|error| error.to_string())?;
    let started = Instant::now();
    let mut next_report = 10_000_000_u64;
    let progress = loop {
        let progress = chainstate
            .retier_utxos_by_height_batch(
                tip.height,
                hot_window_blocks,
                UTXO_RETIER_SCAN_BATCH_SIZE,
                false,
            )
            .map_err(|error| error.to_string())?;
        if progress.complete {
            break progress;
        }
        if progress.scanned >= next_report {
            println!(
                "UTXO re-tier progress scanned={} moved_to_hot={} moved_to_cold={} elapsed_seconds={}",
                progress.scanned,
                progress.moved_to_hot,
                progress.moved_to_cold,
                started.elapsed().as_secs()
            );
            next_report = progress.scanned.saturating_add(10_000_000);
        }
    };
    let tiers = chainstate.tier_stats().map_err(|error| error.to_string())?;
    println!(
        "UTXO re-tier complete network={} tip={}:{} hot_window_blocks={} scanned={} moved_to_hot={} moved_to_cold={} hot={} cold={} elapsed_seconds={}",
        options.network,
        tip.height,
        tip.hash,
        hot_window_blocks,
        progress.scanned,
        progress.moved_to_hot,
        progress.moved_to_cold,
        tiers.hot,
        tiers.cold,
        started.elapsed().as_secs()
    );
    Ok(())
}

#[allow(clippy::too_many_lines)]
async fn run_peer_pool(
    options: &Options,
    local_nonce: u64,
    background_validation: Option<&BackgroundValidationStatus>,
    validation_scheduler: Option<&BackgroundValidationStatus>,
) -> Result<(), String> {
    let api_runtime = prepare_api_runtime(options)?;
    let transaction_pool = Arc::new(Mutex::new(TransactionAdmissionPool::with_capacity(
        options.resources.mempool_max_transactions,
        options.resources.mempool_max_bytes,
    )));
    let mempool_pool = Arc::clone(&transaction_pool);
    let mempool_relay_source = MempoolRelaySource::new(move || {
        mempool_pool
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .relay_snapshot()
            .into_iter()
            .map(|entry| TransactionRelay {
                transaction: entry.transaction,
                fee_sats: Some(entry.fee_sats),
                policy_vsize: entry.policy_vsize,
            })
            .collect()
    });
    let (transaction_relay, _) = broadcast::channel(WALLET_BROADCAST_QUEUE_CAPACITY);
    let inbound_source = options.inbound_listen.map(|_| {
        Arc::new(SharedInboundSource::new(
            options.inbound_limits.max_upload_bytes_per_day,
        ))
    });
    let mut inbound_server = if let (Some(address), Some(source)) =
        (options.inbound_listen, inbound_source.as_ref())
    {
        let source: Arc<dyn InboundDataSource> = Arc::clone(source) as Arc<dyn InboundDataSource>;
        Some(
            InboundServer::bind(
                address,
                options.network,
                local_nonce,
                options.inbound_limits,
                source,
                inbound_source
                    .as_ref()
                    .expect("inbound source exists with listener")
                    .stats(),
                options.indexes.basic_filter,
            )
            .await?,
        )
    } else {
        None
    };
    let peer_store = if let Some(data_dir) = &options.data_dir {
        Some(Arc::new(
            RedbPeerStore::open(data_dir.join("peers.redb"), options.network)
                .map_err(|error| error.to_string())?,
        ))
    } else {
        None
    };
    if let Some(source) = &inbound_source {
        source.install_peer_store(peer_store.as_ref().map(Arc::clone));
    }
    let manual_remotes = options.remotes.iter().copied().collect::<HashSet<_>>();
    let mut remotes = options.remotes.clone();
    if let Some(store) = &peer_store {
        let now = unix_time()?;
        let pruned = store
            .prune_terrible(now)
            .map_err(|error| error.to_string())?;
        if pruned > 0 {
            rbtc_info!("pruned {pruned} terrible persisted peer entries before selection");
        }
        for collision in store
            .tried_collisions()
            .map_err(|error| error.to_string())?
        {
            if remotes.len() == MAX_CONFIGURED_PEERS {
                break;
            }
            if !remotes.contains(&collision.incumbent) {
                remotes.push(collision.incumbent);
            }
        }
        for learned in store
            .candidates(now, MAX_CONFIGURED_PEERS)
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
    if try_peer_candidates(
        options,
        &remotes,
        local_nonce,
        peer_store.as_ref(),
        api_runtime.as_ref(),
        background_validation,
        validation_scheduler,
        &transaction_pool,
        &transaction_relay,
        &mempool_relay_source,
        inbound_source.as_ref(),
        &mut inbound_server,
        &manual_remotes,
        &mut failures,
    )
    .await?
    {
        return Ok(());
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
            rbtc_warn!("DNS seed failed: {failure}");
        }
        if !resolved.is_empty() || rejected > 0 {
            rbtc_info!(
                "DNS bootstrap selected {} peer candidates and rejected {rejected} ineligible or duplicate addresses",
                resolved.len()
            );
        }
        attempted.extend(resolved.iter().copied());
        if try_peer_candidates(
            options,
            &resolved,
            local_nonce,
            peer_store.as_ref(),
            api_runtime.as_ref(),
            background_validation,
            validation_scheduler,
            &transaction_pool,
            &transaction_relay,
            &mempool_relay_source,
            inbound_source.as_ref(),
            &mut inbound_server,
            &manual_remotes,
            &mut failures,
        )
        .await?
        {
            return Ok(());
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
    // A caller-selected batch cap controls both independent pipelines so an
    // assumed chain can use the same bounded dual-peer windows while catching
    // the live tip. Pause throttling remains validator-only. The selected redb
    // repair policy applies to both bulk pipelines: deferred repair preserves
    // atomic durable commits and writes allocator state once at orderly close
    // instead of after every catch-up checkpoint.
    active_options.validation_limits = active_assumeutxo_limits(options.validation_limits);

    let mut validation_options = options;
    validation_options.data_dir = Some(validation_dir.clone());
    validation_options.once = false;
    validation_options.explorer_listen = None;
    validation_options.inbound_listen = None;
    validation_options.wallet_api_files = None;
    validation_options.complete_assumeutxo = None;
    validation_options.background_assumeutxo = None;
    validation_options.validation_target = Some(target);
    validation_options.observer = None;

    rbtc_info!(
        "starting active-chain service and independent genesis validation concurrently through {}:{}",
        assumed.base.height,
        assumed.base.hash
    );
    let validation_status = status.clone();
    let validation = tokio::spawn(async move {
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
    });
    let finalize_options = active_options.clone();
    let active_status = status;
    let active = tokio::spawn(async move {
        run_peer_pool(&active_options, local_nonce, Some(&active_status), None).await
    });
    tokio::pin!(active);
    tokio::pin!(validation);
    tokio::select! {
        active_result = &mut active => {
            let active_result = active_result
                .map_err(|error| format!("active-chain task failed: {error}"))?;
            if let Err(error) = active_result {
                validation.abort();
                let _ = validation.await;
                return Err(error);
            }
            validation
                .await
                .map_err(|error| format!("genesis-validation task failed: {error}"))??;
        }
        validation_result = &mut validation => {
            let validation_result = validation_result
                .map_err(|error| format!("genesis-validation task failed: {error}"))?;
            if let Err(error) = validation_result {
                active.abort();
                let _ = active.await;
                return Err(error);
            }
            active
                .await
                .map_err(|error| format!("active-chain task failed: {error}"))??;
        }
    }
    finalize_background_if_pending(&finalize_options, &validation_dir)
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
    match snapshot {
        SnapshotActivationOptions::Rbtc {
            path,
            height,
            block_hash,
            utxo_count,
            records_bytes,
            records_sha256,
        } => activate_rbtc_assumed_snapshot(
            options,
            path,
            *height,
            *block_hash,
            *utxo_count,
            *records_bytes,
            records_sha256,
        ),
        SnapshotActivationOptions::Core(path) => activate_core_assumed_snapshot(options, path),
    }
}

#[allow(clippy::too_many_arguments)]
fn activate_rbtc_assumed_snapshot(
    options: &Options,
    snapshot: &std::path::Path,
    height: u32,
    block_hash: BlockHash,
    utxo_count: u64,
    records_bytes: u64,
    records_sha256: &str,
) -> Result<(), String> {
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
        height,
        block_hash,
        utxo_count,
        records_bytes,
        records_sha256,
    )
    .map_err(|error| error.to_string())?;
    let chainstate = RedbChainStore::open(data_dir.join("chainstate.redb"), options.network)
        .map_err(|error| error.to_string())?;
    let now = u64::from(unix_time()?);
    let manifest = verify_snapshot_with_trust(snapshot, &trusted)
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
    rbtc_info!(
        "activated assumed UTXO snapshot at {}:{} with {} entries/{} canonical bytes; background genesis validation remains required",
        manifest.height,
        manifest.block_hash,
        manifest.utxo_count,
        manifest.records_bytes
    );
    Ok(())
}

fn activate_core_assumed_snapshot(
    options: &Options,
    snapshot: &std::path::Path,
) -> Result<(), String> {
    let data_dir = options
        .data_dir
        .as_ref()
        .expect("Core snapshot parser requires data directory");
    let header_store =
        RedbHeaderStore::open(data_dir.join("headers.redb")).map_err(|error| error.to_string())?;
    let headers = header_store
        .load_dag_with_deployments(options.deployments.clone(), unix_time()?)
        .map_err(|error| error.to_string())?;
    let chainstate = RedbChainStore::open(data_dir.join("chainstate.redb"), options.network)
        .map_err(|error| error.to_string())?;
    let now = u64::from(unix_time()?);
    let verified =
        verify_core31_snapshot(snapshot, &headers, now).map_err(|error| error.to_string())?;
    let anchor = verified.anchor();
    let metadata = verified
        .assume_into(&chainstate, &headers, DEFAULT_HOT_WINDOW_SECS)
        .map_err(|error| error.to_string())?;
    rbtc_info!(
        "activated Bitcoin Core 31 assumed UTXO snapshot at {}:{} with {} entries and chain transaction count {}; background genesis validation remains required",
        anchor.height,
        metadata.base_block_hash,
        metadata.coins_count,
        anchor.chain_tx_count
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
    let active = RedbChainStore::open_with_options(
        active_path,
        options.network,
        ChainStoreOptions {
            cache_size_bytes: BULK_VALIDATION_CHAINSTATE_CACHE_BYTES,
            validation_delta_journal: true,
            ..ChainStoreOptions::default()
        },
    )
    .map_err(|error| error.to_string())?;
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
    let validation = RedbChainStore::open_with_options(
        validation_path,
        network,
        ChainStoreOptions {
            cache_size_bytes: BULK_VALIDATION_CHAINSTATE_CACHE_BYTES,
            validation_delta_journal: true,
            ..ChainStoreOptions::default()
        },
    )
    .map_err(|error| error.to_string())?;
    validation
        .execution()
        .bind_consensus_config(
            &deployments.consensus_id(),
            &deployments.legacy_consensus_id(),
            &DeploymentConfig::for_network(network).consensus_id(),
        )
        .map_err(|error| error.to_string())?;
    let materialized = active
        .materialize_validation_deltas()
        .map_err(|error| error.to_string())?;
    if materialized > 0 {
        rbtc_info!(
            "materialized {materialized} net active-chain UTXO updates before AssumeUTXO finalization"
        );
    }
    let finalized = active
        .finalize_assumed_snapshot(&validation, headers)
        .map_err(|error| error.to_string())?;
    rbtc_info!(
        "independent genesis validation matched assumed UTXO snapshot at {}:{} ({} entries/{} canonical bytes); assumed-state marker cleared",
        finalized.base.height,
        finalized.base.hash,
        finalized.utxo_count,
        finalized.records_bytes
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
            rbtc_info!(
                "background genesis validation finalized while the active node remained live"
            );
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
    preflight_data_dir(
        &validation_dir,
        options.network,
        &options.deployments,
        false,
    )?;
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
                    | DATA_DIRECTORY_LOCK_FILE
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
    rbtc_info!(
        "removed completed validation directory {} after owner and target revalidation; recovery is no longer possible",
        validation_dir.display()
    );
    Ok(())
}

/// Makes a directory's entries durable after an atomic rename.
///
/// Windows has no portable directory fsync, so this is a documented no-op
/// there: a rename is ordered but its directory entry is not explicitly flushed.
#[cfg_attr(not(unix), allow(clippy::unnecessary_wraps))]
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
    validation_delta_journal: bool,
) -> Result<(), String> {
    fs::create_dir_all(data_dir)
        .map_err(|error| format!("create data directory {}: {error}", data_dir.display()))?;
    reject_legacy_split_chainstate(data_dir)?;
    if validation_delta_journal {
        // The validating-node open below binds and verifies the same consensus
        // identity. Avoid decoding every immutable delta index twice at startup.
        return Ok(());
    }
    let chainstate = RedbChainStore::open_with_options(
        data_dir.join("chainstate.redb"),
        network,
        ChainStoreOptions {
            retain_block_undo: !validation_delta_journal,
            validation_delta_journal,
            ..ChainStoreOptions::default()
        },
    )
    .map_err(|error| error.to_string())?;
    chainstate
        .execution()
        .bind_consensus_config(
            &deployments.consensus_id(),
            &deployments.legacy_consensus_id(),
            &DeploymentConfig::for_network(network).consensus_id(),
        )
        .map_err(|error| error.to_string())
}

fn completed_validating_session(options: &Options) -> bool {
    options.fetch_block.is_none() && (options.data_dir.is_some() || options.headers_db.is_some())
}

async fn negotiate_peer_preferences(
    session: &mut rbtc::p2p::PeerSession<tokio::net::TcpStream>,
) -> Result<(), PeerRunError> {
    timeout(PEER_TIMEOUT, session.prefer_headers_announcements())
        .await
        .map_err(|_| PeerRunError::transient("sendheaders negotiation timed out"))?
        .map_err(|error| PeerRunError::p2p(&error))?;
    timeout(PEER_TIMEOUT, session.negotiate_compact_block_relay())
        .await
        .map_err(|_| PeerRunError::transient("sendcmpct negotiation timed out"))?
        .map_err(|error| PeerRunError::p2p(&error))
}

struct ConnectedPeer {
    remote: SocketAddr,
    session: PeerSession<tokio::net::TcpStream>,
    validated_header_height: Option<u32>,
}

struct ActivePeerObservation {
    observer: Option<NodeObserver>,
    remote: SocketAddr,
}

impl ActivePeerObservation {
    fn new(observer: Option<NodeObserver>, remote: SocketAddr) -> Self {
        if let Some(observer) = &observer {
            observer.peer_connected(remote);
        }
        Self { observer, remote }
    }
}

impl Drop for ActivePeerObservation {
    fn drop(&mut self) {
        if let Some(observer) = &self.observer {
            observer.peer_disconnected(self.remote);
        }
    }
}

struct PendingPeer {
    ready: tokio::sync::oneshot::Receiver<()>,
    ready_observed: bool,
    activate: Option<tokio::sync::oneshot::Sender<()>>,
    task: tokio::task::JoinHandle<Result<ConnectedPeer, PeerRunError>>,
}

impl PendingPeer {
    fn observe_ready(&mut self) -> bool {
        if self.ready_observed {
            return true;
        }
        if self.ready.try_recv().is_ok() {
            self.ready_observed = true;
        }
        self.ready_observed
    }
}

async fn connect_peer(
    deployments: DeploymentConfig,
    remote: SocketAddr,
    local_nonce: u64,
    require_full_services: bool,
    peer_store: Option<Arc<RedbPeerStore>>,
) -> Result<ConnectedPeer, PeerRunError> {
    let handshake_started = Instant::now();
    let mut session = timeout(
        PEER_TIMEOUT,
        connect_outbound(
            remote,
            deployments.message_start(),
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
    let remote_services = remote_version.services;
    let handshake_millis = elapsed_ms(handshake_started);
    rbtc_info!(
        "connected to {}: version={}, height={}, agent={}",
        remote,
        remote_version.version,
        remote_version.start_height,
        remote_version.user_agent
    );
    negotiate_peer_preferences(&mut session).await?;

    if require_full_services {
        session
            .ensure_full_witness_block_relay()
            .map_err(|error| PeerRunError::p2p(&error))?;
        if let Some(store) = peer_store {
            record_verified_peer(store.as_ref(), remote, remote_services, handshake_millis);
        }
    }
    Ok(ConnectedPeer {
        remote,
        session,
        validated_header_height: None,
    })
}

async fn maintain_standby(
    mut connected: ConnectedPeer,
    mut activate: tokio::sync::oneshot::Receiver<()>,
    keepalive_interval: Duration,
    mut ping_nonce: u64,
    mut transaction_relay: Option<broadcast::Receiver<TransactionRelay>>,
    mut header_dag: Option<HeaderDag>,
    transaction_pool: Option<&Arc<Mutex<TransactionAdmissionPool>>>,
) -> Result<ConnectedPeer, PeerRunError> {
    enum StandbyAction {
        Activate,
        Keepalive,
        Relay(Result<TransactionRelay, broadcast::error::RecvError>),
    }
    let mut keepalive = tokio::time::interval(keepalive_interval);
    keepalive.tick().await;
    loop {
        if let Some(pool) = transaction_pool {
            register_pending_transaction_announcements(
                &mut connected.session,
                pool,
                u64::from(unix_time().map_err(PeerRunError::transient)?),
            );
        }
        let action = tokio::select! {
            biased;
            activated = &mut activate => {
                activated.map_err(|_| {
                    PeerRunError::transient("peer standby activation channel closed")
                })?;
                StandbyAction::Activate
            }
            _ = keepalive.tick() => StandbyAction::Keepalive,
            transaction = async {
                match transaction_relay.as_mut() {
                    Some(receiver) => receiver.recv().await,
                    None => std::future::pending().await,
                }
            } => StandbyAction::Relay(transaction),
        };
        match action {
            StandbyAction::Activate => return Ok(connected),
            StandbyAction::Keepalive => {
                timeout(PEER_TIMEOUT, connected.session.ping(ping_nonce))
                    .await
                    .map_err(|_| {
                        PeerRunError::transient(format!(
                            "peer standby keepalive timed out after {} seconds",
                            PEER_TIMEOUT.as_secs()
                        ))
                    })?
                    .map_err(|error| PeerRunError::p2p(&error))?;
                ping_nonce = ping_nonce.wrapping_add(1);
                if let Some(dag) = header_dag.as_mut() {
                    request_headers(&mut connected.session, dag.block_locator()).await?;
                    let headers = receive_headers(&mut connected.session).await?;
                    let unseen = unseen_header_suffix(dag, &headers)
                        .map_err(|error| PeerRunError::header(&error))?;
                    if !unseen.is_empty() {
                        let staged = dag
                            .stage_batch_contextual(
                                unseen,
                                unix_time().map_err(PeerRunError::transient)?,
                            )
                            .map_err(|error| PeerRunError::header(&error))?;
                        let _ = staged.commit();
                    }
                    connected.validated_header_height = Some(dag.active_tip().height);
                }
            }
            StandbyAction::Relay(Ok(relay)) => {
                timeout(
                    PEER_TIMEOUT,
                    connected.session.relay_transaction(&relay, ping_nonce),
                )
                .await
                .map_err(|_| {
                    PeerRunError::transient(format!(
                        "peer standby transaction relay timed out after {} seconds",
                        PEER_TIMEOUT.as_secs()
                    ))
                })?
                .map_err(|error| PeerRunError::p2p(&error))?;
                ping_nonce = ping_nonce.wrapping_add(1);
            }
            StandbyAction::Relay(Err(broadcast::error::RecvError::Lagged(skipped))) => {
                return Err(PeerRunError::transient(format!(
                    "peer standby transaction relay lagged by {skipped} messages"
                )));
            }
            StandbyAction::Relay(Err(broadcast::error::RecvError::Closed)) => {
                transaction_relay = None;
            }
        }
    }
}

struct TransactionRequestSourceGuard {
    source: u64,
    pool: Arc<Mutex<TransactionAdmissionPool>>,
    armed: bool,
}

impl TransactionRequestSourceGuard {
    fn new(source: u64, pool: Arc<Mutex<TransactionAdmissionPool>>) -> Self {
        Self {
            source,
            pool,
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for TransactionRequestSourceGuard {
    fn drop(&mut self) {
        if self.armed {
            self.pool
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .disconnect_transaction_request_source(self.source);
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn connect_and_maintain_standby(
    deployments: DeploymentConfig,
    remote: SocketAddr,
    local_nonce: u64,
    require_full_services: bool,
    peer_store: Option<Arc<RedbPeerStore>>,
    ready: tokio::sync::oneshot::Sender<()>,
    activate: tokio::sync::oneshot::Receiver<()>,
    transaction_relay: Option<broadcast::Receiver<TransactionRelay>>,
    header_dag: Option<HeaderDag>,
    mempool_relay_source: Option<MempoolRelaySource>,
    transaction_pool: Option<Arc<Mutex<TransactionAdmissionPool>>>,
) -> Result<ConnectedPeer, PeerRunError> {
    let mut connected = connect_peer(
        deployments,
        remote,
        local_nonce,
        require_full_services,
        peer_store,
    )
    .await?;
    if let Some(source) = mempool_relay_source {
        connected.session.set_mempool_relay_source(source);
    }
    ready
        .send(())
        .map_err(|()| PeerRunError::transient("peer standby readiness channel closed"))?;
    let orphan_source = connected.session.local_id();
    let mut source_guard = transaction_pool
        .as_ref()
        .map(|pool| TransactionRequestSourceGuard::new(orphan_source, Arc::clone(pool)));
    let result = maintain_standby(
        connected,
        activate,
        STANDBY_KEEPALIVE_INTERVAL,
        local_nonce,
        transaction_relay,
        header_dag,
        transaction_pool.as_ref(),
    )
    .await;
    if result.is_ok() {
        if let Some(guard) = source_guard.as_mut() {
            guard.disarm();
        }
    }
    result
}

fn standby_header_seed(options: &Options) -> Result<Option<HeaderDag>, String> {
    let path = options
        .data_dir
        .as_ref()
        .map(|directory| directory.join("headers.redb"))
        .or_else(|| options.headers_db.clone());
    let Some(path) = path else {
        return Ok(None);
    };
    if !path.exists() {
        return Ok(Some(HeaderDag::with_deployments(
            options.deployments.clone(),
        )));
    }
    let store = RedbHeaderStore::open(path).map_err(|error| error.to_string())?;
    store
        .load_dag_with_deployments(options.deployments.clone(), unix_time()?)
        .map(Some)
        .map_err(|error| error.to_string())
}

fn spawn_peer_connections(
    options: &Options,
    remotes: &[SocketAddr],
    local_nonce: u64,
    peer_store: Option<&Arc<RedbPeerStore>>,
    standby_relay: Option<&broadcast::Sender<TransactionRelay>>,
    mempool_relay_source: Option<&MempoolRelaySource>,
    transaction_pool: Option<&Arc<Mutex<TransactionAdmissionPool>>>,
) -> Result<VecDeque<(SocketAddr, PendingPeer)>, String> {
    let header_seed = standby_header_seed(options)?;
    Ok(remotes
        .iter()
        .copied()
        .map(|remote| {
            record_peer_attempt(peer_store.map(Arc::as_ref), remote);
            let deployments = options.deployments.clone();
            let peer_store = peer_store.cloned();
            let require_full_services = options.data_dir.is_some();
            let (ready_sender, ready) = tokio::sync::oneshot::channel();
            let (activate, activate_receiver) = tokio::sync::oneshot::channel();
            let transaction_relay = standby_relay.map(broadcast::Sender::subscribe);
            let header_dag = header_seed.clone();
            let mempool_relay_source = require_full_services
                .then(|| mempool_relay_source.cloned())
                .flatten();
            let transaction_pool = require_full_services
                .then(|| transaction_pool.cloned())
                .flatten();
            let task = tokio::spawn(connect_and_maintain_standby(
                deployments,
                remote,
                local_nonce,
                require_full_services,
                peer_store,
                ready_sender,
                activate_receiver,
                transaction_relay,
                header_dag,
                mempool_relay_source,
                transaction_pool,
            ));
            (
                remote,
                PendingPeer {
                    ready,
                    ready_observed: false,
                    activate: Some(activate),
                    task,
                },
            )
        })
        .collect())
}

async fn abort_pending_connections(pending: &mut VecDeque<(SocketAddr, PendingPeer)>) {
    for (_, connection) in pending.iter() {
        connection.task.abort();
    }
    while let Some((_, connection)) = pending.pop_front() {
        let _ = connection.task.await;
    }
}

async fn activate_pending_peer(mut pending: PendingPeer) -> Result<ConnectedPeer, PeerRunError> {
    if pending.ready_observed || pending.ready.await.is_ok() {
        let _ = pending
            .activate
            .take()
            .expect("pending peer has one activation sender")
            .send(());
    }
    match pending.task.await {
        Ok(result) => result,
        Err(error) => Err(PeerRunError::transient(format!(
            "peer connection task failed: {error}"
        ))),
    }
}

async fn activate_next_auxiliary_peer(
    candidates: &mut VecDeque<(SocketAddr, PendingPeer)>,
    session: &mut Option<rbtc::p2p::PeerSession<tokio::net::TcpStream>>,
) {
    while let Some((remote, pending)) = candidates.pop_front() {
        match activate_pending_peer(pending).await {
            Ok(connected) => {
                rbtc_info!("activated auxiliary block-download peer {remote}");
                *session = Some(connected.session);
                return;
            }
            Err(error) => {
                rbtc_warn!("auxiliary block-download peer {remote} failed activation: {error}");
            }
        }
    }
}

fn take_ready_auxiliary_peers(
    pending: &mut VecDeque<(SocketAddr, PendingPeer)>,
) -> VecDeque<(SocketAddr, PendingPeer)> {
    let mut selected = VecDeque::new();
    while selected.len() < 3 {
        let Some(index) = pending
            .iter_mut()
            .position(|(_, connection)| connection.observe_ready())
        else {
            break;
        };
        selected.push_back(
            pending
                .remove(index)
                .expect("observed auxiliary peer remains queued"),
        );
    }
    selected
}

async fn evict_excess_ready_standbys(
    pending: &mut VecDeque<(SocketAddr, PendingPeer)>,
    peer_store: Option<&RedbPeerStore>,
    manual_remotes: &HashSet<SocketAddr>,
    failures: &mut Vec<String>,
    maximum_automatic: usize,
) -> Vec<SocketAddr> {
    let ready_automatic = pending
        .iter_mut()
        .enumerate()
        .filter_map(|(index, (remote, connection))| {
            (!manual_remotes.contains(remote) && connection.observe_ready()).then_some(index)
        })
        .collect::<Vec<_>>();
    let excess = ready_automatic.len().saturating_sub(maximum_automatic);
    let mut victims = ready_automatic
        .into_iter()
        .rev()
        .take(excess)
        .collect::<Vec<_>>();
    let mut evicted = Vec::with_capacity(victims.len());
    for index in victims.drain(..) {
        let (remote, connection) = pending
            .remove(index)
            .expect("ready standby eviction index remains in the queue");
        connection.task.abort();
        match connection.task.await {
            Err(error) if error.is_cancelled() => {
                rbtc_info!(
                    "evicted standby peer {remote} under the {maximum_automatic}-peer automatic hot-standby capacity"
                );
                evicted.push(remote);
            }
            outcome => {
                let error = match outcome {
                    Ok(Err(error)) => error,
                    Err(error) => PeerRunError::transient(format!(
                        "peer connection task failed during standby eviction: {error}"
                    )),
                    Ok(Ok(_)) => {
                        PeerRunError::transient("peer standby exited before capacity eviction")
                    }
                };
                rbtc_warn!("standby peer {remote} failed during capacity eviction: {error}");
                record_peer_failure(
                    peer_store,
                    remote,
                    error.kind,
                    manual_remotes.contains(&remote),
                );
                failures.push(format!("{remote}: {error}"));
            }
        }
    }
    evicted.reverse();
    evicted
}

async fn reap_finished_standbys(
    pending: &mut VecDeque<(SocketAddr, PendingPeer)>,
    peer_store: Option<&RedbPeerStore>,
    manual_remotes: &HashSet<SocketAddr>,
    failures: &mut Vec<String>,
) {
    let finished = pending
        .iter()
        .enumerate()
        .filter_map(|(index, (_, connection))| connection.task.is_finished().then_some(index))
        .collect::<Vec<_>>();
    for (removed, index) in finished.into_iter().enumerate() {
        let (remote, connection) = pending
            .remove(index - removed)
            .expect("finished standby index remains in the queue");
        let error = match connection.task.await {
            Ok(Err(error)) => error,
            Err(error) => PeerRunError::transient(format!("peer connection task failed: {error}")),
            Ok(Ok(_)) => PeerRunError::transient("peer standby exited before activation"),
        };
        rbtc_warn!("standby peer {remote} failed: {error}");
        record_peer_failure(
            peer_store,
            remote,
            error.kind,
            manual_remotes.contains(&remote),
        );
        failures.push(format!("{remote}: {error}"));
    }
}

#[allow(clippy::too_many_arguments)]
async fn try_peer_candidates(
    options: &Options,
    remotes: &[SocketAddr],
    local_nonce: u64,
    peer_store: Option<&Arc<RedbPeerStore>>,
    api_runtime: Option<&ApiRuntime>,
    background_validation: Option<&BackgroundValidationStatus>,
    validation_scheduler: Option<&BackgroundValidationStatus>,
    transaction_pool: &Arc<Mutex<TransactionAdmissionPool>>,
    transaction_relay: &broadcast::Sender<TransactionRelay>,
    mempool_relay_source: &MempoolRelaySource,
    inbound_source: Option<&Arc<SharedInboundSource>>,
    inbound_server: &mut Option<InboundServer>,
    manual_remotes: &HashSet<SocketAddr>,
    failures: &mut Vec<String>,
) -> Result<bool, String> {
    let mut pending = match spawn_peer_connections(
        options,
        remotes,
        local_nonce,
        peer_store,
        Some(transaction_relay),
        Some(mempool_relay_source),
        Some(transaction_pool),
    ) {
        Ok(pending) => pending,
        Err(error) => {
            failures.push(format!("standby header seed: {error}"));
            return Ok(false);
        }
    };
    while let Some((remote, connection)) = pending.pop_front() {
        let connected = activate_pending_peer(connection).await;
        let result = match connected {
            Ok(connected) => {
                let auxiliary = if options.data_dir.is_some()
                    && options.validation_limits.max_blocks_per_batch > VALIDATION_BLOCK_WINDOW_SIZE
                {
                    take_ready_auxiliary_peers(&mut pending)
                } else {
                    VecDeque::new()
                };
                let mut active = Box::pin(run_connected_peer(
                    options,
                    connected,
                    auxiliary,
                    peer_store.map(Arc::as_ref),
                    api_runtime,
                    background_validation,
                    validation_scheduler,
                    transaction_pool,
                    transaction_relay,
                    inbound_source,
                ));
                let mut standby_reaper = tokio::time::interval(STANDBY_REAP_INTERVAL);
                loop {
                    tokio::select! {
                        result = &mut active => break result,
                        _ = standby_reaper.tick() => {
                            reap_finished_standbys(
                                &mut pending,
                                peer_store.map(Arc::as_ref),
                                manual_remotes,
                                failures,
                            ).await;
                            evict_excess_ready_standbys(
                                &mut pending,
                                peer_store.map(Arc::as_ref),
                                manual_remotes,
                                failures,
                                options.resources.automatic_hot_standbys,
                            ).await;
                        }
                        listener = wait_for_inbound_exit(inbound_server) => {
                            break Err(PeerRunError::local(match listener {
                                Ok(()) => "inbound P2P listener stopped unexpectedly".to_owned(),
                                Err(error) => error,
                            }));
                        }
                    }
                }
            }
            Err(error) => Err(error),
        };
        match result {
            Ok(()) => {
                if completed_validating_session(options) {
                    record_peer_session_success(peer_store.map(Arc::as_ref), remote);
                }
                abort_pending_connections(&mut pending).await;
                return Ok(true);
            }
            Err(error) => {
                if error.kind == PeerFailureKind::LocalResource {
                    abort_pending_connections(&mut pending).await;
                    return Err(error.message);
                }
                rbtc_warn!("peer {remote} failed: {error}");
                record_peer_failure(
                    peer_store.map(Arc::as_ref),
                    remote,
                    error.kind,
                    manual_remotes.contains(&remote),
                );
                failures.push(format!("{remote}: {error}"));
            }
        }
    }
    Ok(false)
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
async fn run_connected_peer(
    options: &Options,
    connected: ConnectedPeer,
    auxiliary: VecDeque<(SocketAddr, PendingPeer)>,
    peer_store: Option<&RedbPeerStore>,
    api_runtime: Option<&ApiRuntime>,
    background_validation: Option<&BackgroundValidationStatus>,
    validation_scheduler: Option<&BackgroundValidationStatus>,
    transaction_pool: &Arc<Mutex<TransactionAdmissionPool>>,
    transaction_relay: &broadcast::Sender<TransactionRelay>,
    inbound_source: Option<&Arc<SharedInboundSource>>,
) -> Result<(), PeerRunError> {
    let ConnectedPeer {
        remote,
        mut session,
        validated_header_height,
    } = connected;
    let _peer_observation = ActivePeerObservation::new(options.observer.clone(), remote);
    let orphan_source = session.local_id();
    if let Some(height) = validated_header_height {
        rbtc_info!("activated peer {remote} after standby validation through height {height}");
    }
    if let Some(hash) = options.fetch_block {
        fetch_requested_block(&mut session, hash).await?;
        return Ok(());
    }

    if let Some(path) = &options.data_dir {
        if let Some(store) = peer_store {
            discover_peer_addresses(&mut session, store, remote).await;
        }
        let transfer_before = session.block_transfer_stats();
        let result = if let Some(validation_dir) = &options.complete_assumeutxo {
            complete_assumeutxo_validation(
                &mut session,
                options,
                validation_dir.clone(),
                transaction_pool,
                transaction_relay,
            )
            .await
        } else {
            let disk_monitor = DiskSpaceMonitor::for_options(options, path.clone());
            sync_validating_node(
                &mut session,
                auxiliary,
                options.network,
                options.network_execution,
                &options.deployments,
                options.ibd_policy,
                path.clone(),
                &options.runtime_control,
                options.observer.as_ref(),
                Some(remote),
                options.once,
                options.validation_target,
                options.validation_limits,
                options.ledger_retention,
                disk_monitor,
                options.cache,
                options.indexes,
                inbound_source,
                options.mempool_full_rbf,
                api_runtime,
                background_validation,
                validation_scheduler,
                transaction_pool,
                transaction_relay,
            )
            .await
        };
        if result.is_ok() {
            record_peer_block_throughput(
                peer_store,
                remote,
                transfer_before,
                session.block_transfer_stats(),
            );
        }
        let removed_orphans = transaction_pool
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove_orphans_from(orphan_source);
        if removed_orphans > 0 {
            rbtc_info!(
                "removed {removed_orphans} orphan transaction{} after peer {remote} disconnected",
                if removed_orphans == 1 { "" } else { "s" }
            );
        }
        return result;
    }

    if let Some(path) = &options.headers_db {
        let headers = sync_headers(&mut session, &options.deployments, path.clone()).await?;
        let status = options
            .ibd_policy
            .ensure_minimum_chainwork(&headers)
            .map_err(|error| PeerRunError::transient(error.to_string()))?;
        rbtc_info!(
            "minimum chainwork reached at height {}; full script validation remains required",
            status.height
        );
        return Ok(());
    }

    let genesis = bitcoin::blockdata::constants::genesis_block(options.network).block_hash();
    request_headers(&mut session, vec![genesis]).await?;
    let headers = receive_headers(&mut session).await?;
    rbtc_info!(
        "received {} headers; pass --headers-db PATH to validate, persist, and continue IBD",
        headers.len()
    );
    Ok(())
}

async fn fetch_requested_block(
    session: &mut PeerSession<tokio::net::TcpStream>,
    hash: BlockHash,
) -> Result<(), PeerRunError> {
    session
        .ensure_full_witness_block_relay()
        .map_err(|error| PeerRunError::p2p(&error))?;
    timeout(PEER_TIMEOUT, session.request_blocks(&[hash]))
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
    rbtc_info!(
        "received block {} ({} transactions); not applied: IBD chainstate integration is pending",
        block.block_hash(),
        block.txdata.len()
    );
    Ok(())
}

async fn complete_assumeutxo_validation(
    session: &mut rbtc::p2p::PeerSession<tokio::net::TcpStream>,
    options: &Options,
    validation_dir: PathBuf,
    transaction_pool: &Arc<Mutex<TransactionAdmissionPool>>,
    transaction_relay: &broadcast::Sender<TransactionRelay>,
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
        VecDeque::new(),
        options.network,
        options.network_execution,
        &options.deployments,
        options.ibd_policy,
        validation_dir.clone(),
        &options.runtime_control,
        options.observer.as_ref(),
        None,
        false,
        Some(ValidationTarget {
            height: assumed.base.height,
            block_hash: assumed.base.hash,
        }),
        options.validation_limits,
        options.ledger_retention,
        DiskSpaceMonitor::for_options(options, validation_dir.clone()),
        options.cache,
        options.indexes,
        None,
        options.mempool_full_rbf,
        None,
        None,
        None,
        transaction_pool,
        transaction_relay,
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
            rbtc_warn!("peer attempt history for {remote} skipped: {error}");
            return;
        }
    };
    if let Err(error) = store.record_attempt(remote, now) {
        rbtc_warn!("peer attempt history for {remote} failed: {error}");
    }
}

fn record_peer_failure(
    store: Option<&RedbPeerStore>,
    remote: SocketAddr,
    kind: PeerFailureKind,
    manual: bool,
) {
    let Some(store) = store else {
        return;
    };
    let now = match unix_time() {
        Ok(now) => now,
        Err(error) => {
            rbtc_warn!("protocol cooldown for {remote} skipped: {error}");
            return;
        }
    };
    if let Err(error) = store.resolve_tried_collision_probe(remote, false, now) {
        rbtc_warn!("tried-collision failure resolution for {remote} failed: {error}");
    }
    if kind != PeerFailureKind::ProtocolViolation || manual {
        return;
    }
    match store.record_protocol_violation(remote, now) {
        Ok(until) => rbtc_info!(
            "discouraged peer {remote} after an objective protocol violation until Unix time {until}"
        ),
        Err(error) => rbtc_warn!("protocol cooldown for {remote} failed: {error}"),
    }
}

fn record_peer_session_success(store: Option<&RedbPeerStore>, remote: SocketAddr) {
    let Some(store) = store else {
        return;
    };
    let now = match unix_time() {
        Ok(now) => now,
        Err(error) => {
            rbtc_warn!("peer session success for {remote} skipped: {error}");
            return;
        }
    };
    if let Err(error) = store.record_session_success(remote, now) {
        rbtc_warn!("peer session success for {remote} failed: {error}");
    }
}

fn record_verified_peer(
    store: &RedbPeerStore,
    remote: SocketAddr,
    services: bitcoin::p2p::ServiceFlags,
    handshake_millis: u32,
) {
    let now = match unix_time() {
        Ok(now) => now,
        Err(error) => {
            rbtc_warn!("peer success history for {remote} skipped: {error}");
            return;
        }
    };
    match store.insert_verified(remote, services, now) {
        Ok(true) => {
            if let Err(error) = store.record_handshake_latency(remote, handshake_millis) {
                rbtc_warn!("peer handshake latency persistence for {remote} failed: {error}");
            }
            if let Err(error) = store.resolve_tried_collision_probe(remote, true, now) {
                rbtc_warn!("tried-collision success resolution for {remote} failed: {error}");
            }
        }
        Ok(false) => rbtc_warn!("verified peer {remote} was not eligible for persistence"),
        Err(error) => rbtc_warn!("verified peer persistence for {remote} failed: {error}"),
    }
}

fn record_peer_block_throughput(
    store: Option<&RedbPeerStore>,
    remote: SocketAddr,
    before: BlockTransferStats,
    after: BlockTransferStats,
) {
    let Some(store) = store else {
        return;
    };
    let Some(bytes_per_second) = block_throughput_bps(before, after) else {
        return;
    };
    if let Err(error) = store.record_block_throughput(remote, bytes_per_second) {
        rbtc_warn!("peer block throughput persistence for {remote} failed: {error}");
    }
}

fn block_throughput_bps(before: BlockTransferStats, after: BlockTransferStats) -> Option<u32> {
    let payload_bytes = after.payload_bytes.saturating_sub(before.payload_bytes);
    if payload_bytes == 0 {
        return None;
    }
    let response_nanos = after
        .response_time
        .saturating_sub(before.response_time)
        .as_nanos()
        .max(1);
    let bytes_per_second = u128::from(payload_bytes).saturating_mul(1_000_000_000) / response_nanos;
    Some(u32::try_from(bytes_per_second).unwrap_or(u32::MAX))
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
                    rbtc_warn!("peer address persistence from {source} skipped: {error}");
                    return;
                }
            };
            match store.insert_discovered(source, &addresses, now) {
                Ok(stats) if stats.accepted > 0 => rbtc_info!(
                    "persisted {} learned peer addresses from {source}; rejected {}",
                    stats.accepted,
                    stats.rejected
                ),
                Ok(_) => {}
                Err(error) => rbtc_warn!("peer address persistence from {source} failed: {error}"),
            }
        }
        Ok(Err(error)) => rbtc_warn!("peer address discovery from {source} failed: {error}"),
        Err(_) => rbtc_warn!(
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
    rbtc_info!(
        "resuming headers-first sync from {}:{} (local-clock fallback until network-time aggregation lands)",
        dag.active_tip().height,
        dag.active_tip().hash
    );

    loop {
        request_headers(session, dag.block_locator()).await?;
        let headers = receive_headers(session).await?;
        let response_count = headers.len();
        if response_count == 0 {
            break;
        }
        let unseen =
            unseen_header_suffix(&dag, &headers).map_err(|error| PeerRunError::header(&error))?;
        if unseen.is_empty() {
            break;
        }
        let staged = dag
            .stage_batch_contextual(unseen, unix_time().map_err(PeerRunError::transient)?)
            .map_err(|error| PeerRunError::header(&error))?;
        store
            .append_batch(unseen)
            .map_err(|error| PeerRunError::transient(error.to_string()))?;
        let _ = staged.commit();
        rbtc_info!(
            "validated and persisted {} headers; active tip {}:{}",
            unseen.len(),
            dag.active_tip().height,
            dag.active_tip().hash
        );
        if response_count < MAX_HEADERS_PER_RESPONSE {
            break;
        }
    }
    rbtc_info!(
        "peer returned no more headers at {}:{}",
        dag.active_tip().height,
        dag.active_tip().hash
    );
    Ok(dag)
}

fn unseen_header_suffix<'a>(
    dag: &HeaderDag,
    headers: &'a [Header],
) -> Result<&'a [Header], HeaderError> {
    let first_unseen = headers
        .iter()
        .position(|header| dag.get(&header.block_hash()).is_none())
        .unwrap_or(headers.len());
    if let Some(duplicate) = headers[first_unseen..]
        .iter()
        .find(|header| dag.get(&header.block_hash()).is_some())
    {
        return Err(HeaderError::Duplicate(duplicate.block_hash()));
    }
    Ok(&headers[first_unseen..])
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
async fn wait_for_peer_poll(
    session: &mut rbtc::p2p::PeerSession<tokio::net::TcpStream>,
    duration: Duration,
    wallet: Option<&WalletApiRuntime>,
    transaction_relay: &broadcast::Sender<TransactionRelay>,
) -> Result<(), PeerRunError> {
    struct BroadcastWork {
        transaction: Transaction,
        fee_sats: Option<u64>,
        result: Option<tokio::sync::oneshot::Sender<Result<(), ()>>>,
    }

    let sleep = tokio::time::sleep(duration);
    tokio::pin!(sleep);
    let Some(wallet) = wallet else {
        sleep.await;
        return Ok(());
    };
    loop {
        let work = if let Some(request) = wallet.pending_broadcast.lock().await.take() {
            Some(BroadcastWork {
                transaction: request.transaction,
                fee_sats: Some(request.fee_sats),
                result: Some(request.result),
            })
        } else {
            let mut receiver = wallet.broadcast_receiver.lock().await;
            match receiver.try_recv() {
                Ok(request) => Some(BroadcastWork {
                    transaction: request.transaction,
                    fee_sats: Some(request.fee_sats),
                    result: Some(request.result),
                }),
                Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {
                    sleep.await;
                    return Ok(());
                }
                Err(tokio::sync::mpsc::error::TryRecvError::Empty) => {
                    drop(receiver);
                    let due = wallet
                        .rebroadcast
                        .due(u64::from(unix_time()?), 1)
                        .map_err(|error| PeerRunError::transient(error.to_string()))?
                        .into_iter()
                        .next();
                    if let Some(transaction) = due {
                        Some(BroadcastWork {
                            transaction,
                            fee_sats: None,
                            result: None,
                        })
                    } else {
                        let mut receiver = wallet.broadcast_receiver.lock().await;
                        tokio::select! {
                            () = &mut sleep => return Ok(()),
                            request = receiver.recv() => request.map(|request| BroadcastWork {
                                transaction: request.transaction,
                                fee_sats: Some(request.fee_sats),
                                result: Some(request.result),
                            }),
                        }
                    }
                }
            }
        };
        let Some(work) = work else {
            sleep.await;
            return Ok(());
        };
        if work
            .result
            .as_ref()
            .is_some_and(tokio::sync::oneshot::Sender::is_closed)
        {
            continue;
        }
        match timeout(
            PEER_TIMEOUT,
            session.broadcast_transaction(&work.transaction),
        )
        .await
        {
            Ok(Ok(())) => {
                let relay = TransactionRelay {
                    policy_vsize: work.transaction.vsize(),
                    transaction: work.transaction.clone(),
                    fee_sats: work.fee_sats,
                };
                wallet
                    .rebroadcast
                    .record_broadcast(work.transaction.compute_wtxid(), u64::from(unix_time()?))
                    .map_err(|error| PeerRunError::transient(error.to_string()))?;
                wallet.compact_candidates.insert(work.transaction.clone());
                let _ = transaction_relay.send(relay);
                if let Some(result) = work.result {
                    let _ = result.send(Ok(()));
                }
            }
            Ok(Err(error)) => {
                let peer_error = PeerRunError::p2p(&error);
                if let Some(result) = work.result {
                    retain_wallet_broadcast(
                        wallet,
                        WalletBroadcastRequest {
                            transaction: work.transaction,
                            fee_sats: work.fee_sats.expect("live request retains validated fee"),
                            result,
                        },
                    )
                    .await;
                }
                return Err(peer_error);
            }
            Err(_) => {
                if let Some(result) = work.result {
                    retain_wallet_broadcast(
                        wallet,
                        WalletBroadcastRequest {
                            transaction: work.transaction,
                            fee_sats: work.fee_sats.expect("live request retains validated fee"),
                            result,
                        },
                    )
                    .await;
                }
                return Err(PeerRunError::transient(
                    "wallet transaction broadcast timed out",
                ));
            }
        }
    }
}

async fn retain_wallet_broadcast(wallet: &WalletApiRuntime, request: WalletBroadcastRequest) {
    if request.result.is_closed() {
        return;
    }
    let replaced = wallet.pending_broadcast.lock().await.replace(request);
    if let Some(replaced) = replaced {
        let _ = replaced.result.send(Err(()));
    }
}

fn compact_transaction_candidates(
    transaction_pool: &Arc<Mutex<TransactionAdmissionPool>>,
    wallet_runtime: Option<&WalletApiRuntime>,
) -> Vec<Transaction> {
    let mut candidates = transaction_pool
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .snapshot();
    if let Some(wallet) = wallet_runtime {
        for transaction in wallet.compact_candidates.snapshot() {
            let wtxid = transaction.compute_wtxid();
            candidates.retain(|candidate| candidate.compute_wtxid() != wtxid);
            candidates.push(transaction);
            while candidates.len() > MAX_COMPACT_TRANSACTION_CANDIDATES {
                candidates.remove(0);
            }
        }
    }
    candidates
}

fn retain_disconnected_block_transactions(
    pending: &mut VecDeque<Transaction>,
    block: &Block,
    max_transactions: usize,
    max_bytes: usize,
) {
    let mut pending_bytes = pending
        .iter()
        .map(|transaction| serialize(transaction).len())
        .sum::<usize>();
    for transaction in block
        .txdata
        .iter()
        .skip(1)
        .rev()
        .filter(|transaction| !transaction.is_coinbase())
    {
        let transaction_bytes = serialize(transaction).len();
        if transaction_bytes > MAX_ADMITTED_TRANSACTION_BYTES {
            continue;
        }
        pending.push_front(transaction.clone());
        pending_bytes = pending_bytes.saturating_add(transaction_bytes);
        while pending.len() > max_transactions || pending_bytes > max_bytes {
            if let Some(evicted) = pending.pop_back() {
                pending_bytes = pending_bytes.saturating_sub(serialize(&evicted).len());
            } else {
                break;
            }
        }
    }
}

fn relay_selected_transactions(
    relay: &broadcast::Sender<TransactionRelay>,
    transactions: &[AdmittedTransactionRelay],
    selected: &HashSet<bitcoin::Txid>,
) -> Vec<bitcoin::Txid> {
    let mut relayed = Vec::new();
    for entry in transactions {
        let txid = entry.transaction.compute_txid();
        let event = TransactionRelay {
            transaction: entry.transaction.clone(),
            fee_sats: Some(entry.fee_sats),
            policy_vsize: entry.policy_vsize,
        };
        if selected.contains(&txid) && relay.send(event).is_ok() {
            relayed.push(txid);
        }
    }
    relayed
}

fn relay_due_peer_transactions(
    store: &RedbTransactionPoolStore,
    active: &[AdmittedTransactionRelay],
    relay: &broadcast::Sender<TransactionRelay>,
    now: u32,
) -> Result<usize, String> {
    let due = store
        .due_relay(
            now,
            PEER_TRANSACTION_REBROADCAST_INTERVAL_SECS,
            WALLET_BROADCAST_QUEUE_CAPACITY,
        )
        .map_err(|error| error.to_string())?;
    let selected = due
        .iter()
        .map(Transaction::compute_txid)
        .collect::<HashSet<_>>();
    let relayed = relay_selected_transactions(relay, active, &selected);
    if !relayed.is_empty() {
        store
            .record_relay_attempts(&relayed, now)
            .map_err(|error| error.to_string())?;
    }
    Ok(relayed.len())
}

fn reconcile_fee_estimator(
    estimator: &RedbFeeEstimator,
    headers: &HeaderDag,
    execution_tip: rbtc::execution_store::ExecutionTip,
    ledger: &PrunedBlockLedger,
) -> Result<(), String> {
    loop {
        let (height, hash) = estimator.tip().map_err(|error| error.to_string())?;
        if height <= execution_tip.height
            && headers
                .active_header_at(height)
                .is_some_and(|header| header.hash == hash)
        {
            break;
        }
        match estimator.disconnect_tip(hash) {
            Ok(restored) => {
                if restored > 0 {
                    rbtc_info!(
                        "restored {restored} fee observation{} after estimator-tip disconnection",
                        if restored == 1 { "" } else { "s" }
                    );
                }
            }
            Err(FeeEstimatorError::Malformed("estimator history does not retain the tip")) => {
                estimator
                    .reset_tip(execution_tip.height, execution_tip.hash)
                    .map_err(|error| error.to_string())?;
                rbtc_info!(
                    "reset fee-estimator history at {}:{} after a reorganization deeper than its retained journal",
                    execution_tip.height,
                    execution_tip.hash
                );
                return Ok(());
            }
            Err(error) => return Err(error.to_string()),
        }
    }

    loop {
        let (height, hash) = estimator.tip().map_err(|error| error.to_string())?;
        if height >= execution_tip.height {
            return Ok(());
        }
        let next_height = height
            .checked_add(1)
            .ok_or_else(|| "fee-estimator height overflow".to_owned())?;
        let expected = headers
            .active_header_at(next_height)
            .ok_or_else(|| format!("missing active fee-estimator header at {next_height}"))?;
        let Some(raw) = ledger
            .read_block(next_height)
            .map_err(|error| error.to_string())?
        else {
            estimator
                .reset_tip(execution_tip.height, execution_tip.hash)
                .map_err(|error| error.to_string())?;
            rbtc_info!(
                "initialized fee-estimator tip at {}:{} because retained blocks do not reach its prior tip",
                execution_tip.height,
                execution_tip.hash
            );
            return Ok(());
        };
        let block = deserialize::<Block>(&raw)
            .map_err(|error| format!("decode fee-estimator block at {next_height}: {error}"))?;
        if block.block_hash() != expected.hash {
            return Err(format!(
                "retained fee-estimator block at {next_height} has hash {}, expected {}",
                block.block_hash(),
                expected.hash
            ));
        }
        let transaction_ids = block
            .txdata
            .iter()
            .skip(1)
            .map(Transaction::compute_txid)
            .collect::<BTreeSet<_>>();
        let confirmed = estimator
            .connect_block(next_height, expected.hash, hash, &transaction_ids)
            .map_err(|error| error.to_string())?;
        if confirmed > 0 {
            rbtc_info!(
                "recorded {confirmed} confirmed fee observation{} at block {next_height}",
                if confirmed == 1 { "" } else { "s" }
            );
        }
    }
}

fn transaction_admission_context(
    chainstate: &RedbChainStore,
    headers: &HeaderDag,
    deployment_config: &DeploymentConfig,
    full_rbf: bool,
) -> Result<TransactionAdmissionContext, String> {
    let tip = chainstate
        .execution()
        .tip()
        .map_err(|error| error.to_string())?;
    let active_tip = headers.active_tip();
    if tip.height != active_tip.height || tip.hash != active_tip.hash {
        return Err("transaction admission requires execution at the active header tip".to_owned());
    }
    let height = tip
        .height
        .checked_add(1)
        .ok_or_else(|| "transaction admission height overflow".to_owned())?;
    let parent_mtp = headers
        .median_time_past(tip.hash)
        .ok_or_else(|| "transaction admission parent MTP is unavailable".to_owned())?;
    let deployments = block_deployment_context_for_headers(
        deployment_config,
        headers,
        height,
        BlockHash::all_zeros(),
    )
    .map_err(|error| error.to_string())?;
    Ok(TransactionAdmissionContext {
        height,
        parent_mtp,
        script_flags: deployments.script_flags,
        csv_active: deployments.csv_active,
        full_rbf,
    })
}

fn transaction_inventory_is_known(
    inventory: Inventory,
    known: &[Transaction],
    pool: &TransactionAdmissionPool,
) -> bool {
    match inventory {
        Inventory::WTx(wtxid) => {
            pool.knows_wtxid(wtxid)
                || known
                    .iter()
                    .any(|transaction| transaction.compute_wtxid() == wtxid)
        }
        Inventory::Transaction(txid) | Inventory::WitnessTransaction(txid) => {
            pool.knows_transaction(txid)
                || known
                    .iter()
                    .any(|transaction| transaction.compute_txid() == txid)
        }
        _ => false,
    }
}

fn transaction_request_id(inventory: Inventory) -> Option<TransactionRequestId> {
    match inventory {
        Inventory::Transaction(txid) | Inventory::WitnessTransaction(txid) => {
            Some(TransactionRequestId::Txid(txid))
        }
        Inventory::WTx(wtxid) => Some(TransactionRequestId::Wtxid(wtxid)),
        _ => None,
    }
}

fn transaction_request_inventory(id: TransactionRequestId) -> Inventory {
    match id {
        TransactionRequestId::Txid(txid) => Inventory::Transaction(txid),
        TransactionRequestId::Wtxid(wtxid) => Inventory::WTx(wtxid),
    }
}

fn register_pending_transaction_announcements(
    session: &mut PeerSession<tokio::net::TcpStream>,
    transaction_pool: &Arc<Mutex<TransactionAdmissionPool>>,
    now: u64,
) -> usize {
    let ids = session
        .take_pending_transaction_inventory()
        .into_iter()
        .filter_map(transaction_request_id)
        .collect::<Vec<_>>();
    transaction_pool
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .announce_transaction_requests(session.local_id(), ids, now)
}

fn peer_orphan_candidates(
    package: Vec<Transaction>,
    peer_txids: &HashSet<bitcoin::Txid>,
) -> Vec<Transaction> {
    package
        .into_iter()
        .filter(|transaction| peer_txids.contains(&transaction.compute_txid()))
        .collect()
}

fn unavailable_parent_txids(
    pool: &TransactionAdmissionPool,
    chainstate: &RedbChainStore,
    submitted: &[Transaction],
    source_transactions: &[Transaction],
) -> Result<BTreeSet<bitcoin::Txid>, String> {
    let submitted_txids = submitted
        .iter()
        .map(Transaction::compute_txid)
        .collect::<BTreeSet<_>>();
    let mut unavailable = BTreeSet::new();
    for input in source_transactions
        .iter()
        .flat_map(|transaction| &transaction.input)
    {
        let parent = input.previous_output.txid;
        if submitted_txids.contains(&parent)
            || pool.knows_transaction(parent)
            || chainstate
                .get(input.previous_output.into())
                .map_err(|error| error.to_string())?
                .is_some()
        {
            continue;
        }
        unavailable.insert(parent);
    }
    Ok(unavailable)
}

struct PeerAdmissionProgress {
    more_orphan_work: bool,
    parent_requests: Vec<bitcoin::Txid>,
}

async fn fetch_announced_peer_transactions(
    session: &mut PeerSession<tokio::net::TcpStream>,
    transaction_pool: &Arc<Mutex<TransactionAdmissionPool>>,
    wallet: Option<&WalletApiRuntime>,
) -> Result<(), PeerRunError> {
    let now = u64::from(unix_time().map_err(PeerRunError::transient)?);
    register_pending_transaction_announcements(session, transaction_pool, now);
    let source = session.local_id();
    let requested = {
        let mut pool = transaction_pool
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let known = wallet
            .map(|wallet| wallet.compact_candidates.snapshot())
            .unwrap_or_default();
        let selected =
            pool.take_transaction_requests(source, now, MAX_PENDING_TRANSACTION_INVENTORY);
        selected
            .into_iter()
            .filter_map(|id| {
                let inventory = transaction_request_inventory(id);
                let rejected = matches!(
                    id,
                    TransactionRequestId::Txid(txid) if pool.is_recently_rejected(txid)
                );
                if rejected || transaction_inventory_is_known(inventory, &known, &pool) {
                    pool.forget_transaction_request(id);
                    None
                } else {
                    Some(inventory)
                }
            })
            .collect::<Vec<_>>()
    };
    if requested.is_empty() {
        return Ok(());
    }
    let outcome = timeout(
        PEER_TIMEOUT,
        session.request_announced_transactions_detailed(&requested),
    )
    .await
    .map_err(|_| {
        PeerRunError::transient(format!(
            "peer transaction download timed out after {} seconds",
            PEER_TIMEOUT.as_secs()
        ))
    })?
    .map_err(|error| PeerRunError::p2p(&error))?;
    {
        let mut pool = transaction_pool
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for inventory in outcome.received {
            if let Some(id) = transaction_request_id(inventory) {
                pool.forget_transaction_request(id);
            }
        }
        for inventory in outcome.not_found {
            if let Some(id) = transaction_request_id(inventory) {
                pool.complete_transaction_request(source, id);
            }
        }
    }
    let received = outcome.received_transactions;
    if received > 0 {
        rbtc_info!(
            "downloaded {received} announced peer transaction{} for bounded admission",
            if received == 1 { "" } else { "s" }
        );
    }
    Ok(())
}

async fn fetch_orphan_parent_transactions(
    session: &mut PeerSession<tokio::net::TcpStream>,
    parents: Vec<bitcoin::Txid>,
) -> Result<usize, PeerRunError> {
    let inventory = parents
        .into_iter()
        .map(Inventory::Transaction)
        .collect::<Vec<_>>();
    if inventory.is_empty() {
        return Ok(0);
    }
    timeout(
        PEER_TIMEOUT,
        session.request_announced_transactions(&inventory),
    )
    .await
    .map_err(|_| {
        PeerRunError::transient(format!(
            "orphan parent download timed out after {} seconds",
            PEER_TIMEOUT.as_secs()
        ))
    })?
    .map_err(|error| PeerRunError::p2p(&error))
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
fn admit_pending_peer_transactions(
    session: &mut PeerSession<tokio::net::TcpStream>,
    transaction_pool: &Arc<Mutex<TransactionAdmissionPool>>,
    transaction_store: Option<&RedbTransactionPoolStore>,
    fee_estimator: Option<&RedbFeeEstimator>,
    disconnected_transactions: &VecDeque<Transaction>,
    transaction_relay: &broadcast::Sender<TransactionRelay>,
    chainstate: &RedbChainStore,
    headers: &HeaderDag,
    deployment_config: &DeploymentConfig,
    full_rbf: bool,
) -> Result<PeerAdmissionProgress, String> {
    let context = transaction_admission_context(chainstate, headers, deployment_config, full_rbf)?;
    let now = unix_time()?;
    let expired = transaction_store
        .map(|store| store.expired_txids(now, DEFAULT_MEMPOOL_EXPIRY_SECS))
        .transpose()
        .map_err(|error| error.to_string())?
        .unwrap_or_default();
    let persisted = transaction_store
        .map(RedbTransactionPoolStore::transactions)
        .transpose()
        .map_err(|error| error.to_string())?
        .unwrap_or_default();
    let expired_with_descendants = transaction_descendant_closure(&persisted, &expired);
    let persisted = persisted
        .into_iter()
        .filter(|transaction| !expired_with_descendants.contains(&transaction.compute_txid()))
        .collect::<Vec<_>>();
    let persisted_txids = persisted
        .iter()
        .map(Transaction::compute_txid)
        .collect::<HashSet<_>>();
    let mut pending = persisted;
    if disconnected_transactions.is_empty() {
        pending.extend(
            transaction_store
                .map(RedbTransactionPoolStore::disconnected_transactions)
                .transpose()
                .map_err(|error| error.to_string())?
                .unwrap_or_default(),
        );
    } else {
        pending.extend(disconnected_transactions.iter().cloned());
    }
    let peer_pending = session.take_pending_transactions();
    let peer_txids = peer_pending
        .iter()
        .map(Transaction::compute_txid)
        .collect::<HashSet<_>>();
    pending.extend(peer_pending);
    let mut pool = transaction_pool
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let mut candidate = pool.clone();
    candidate.observe_chain_tip(
        chainstate
            .execution()
            .tip()
            .map_err(|error| error.to_string())?
            .hash,
        now,
    );
    let expired_removed = candidate.remove_with_descendants(&expired);
    let reconciled_removed = candidate.reconcile(chainstate, context);
    let expired_orphans = candidate.prune_orphans(now);
    let mut accepted_messages = Vec::new();
    let mut deferred_messages = Vec::new();
    let mut rejected_messages = Vec::new();
    let mut accepted_for_relay = HashSet::new();
    let mut parent_requests = BTreeSet::new();
    let mut packages = dependency_packages(pending)
        .into_iter()
        .map(|package| (package, false))
        .collect::<VecDeque<_>>();
    let mut accepted_parents = BTreeSet::new();
    let orphan_source = session.local_id();
    let mut processed_orphan_work = false;
    loop {
        while let Some((package, is_orphan_work)) = packages.pop_front() {
            let retained_package = package.clone();
            let source_package = if is_orphan_work {
                retained_package.clone()
            } else {
                peer_orphan_candidates(retained_package.clone(), &peer_txids)
            };
            for transaction in &retained_package {
                candidate.forget_transaction_requests_for(transaction);
            }
            let txid_set = package
                .iter()
                .map(Transaction::compute_txid)
                .collect::<BTreeSet<_>>();
            let txids = package
                .iter()
                .map(Transaction::compute_txid)
                .collect::<Vec<_>>();
            match candidate.admit_package_at(chainstate, package, context, now) {
                Ok(outcome) if !outcome.accepted.is_empty() => {
                    candidate.remove_orphans(&txid_set);
                    accepted_parents.extend(outcome.accepted.iter().copied());
                    accepted_for_relay.extend(
                        outcome
                            .accepted
                            .iter()
                            .copied()
                            .filter(|txid| !persisted_txids.contains(txid)),
                    );
                    let mut effects = Vec::new();
                    if outcome.replaced > 0 {
                        effects.push(format!(
                            "replacing {} BIP125 conflict{} or descendant{}",
                            outcome.replaced,
                            if outcome.replaced == 1 { "" } else { "s" },
                            if outcome.replaced == 1 { "" } else { "s" }
                        ));
                    }
                    if outcome.evicted > 0 {
                        effects.push(format!(
                            "evicting {} oldest entr{} or descendants",
                            outcome.evicted,
                            if outcome.evicted == 1 { "y" } else { "ies" }
                        ));
                    }
                    accepted_messages.push(format!(
                        "admitted peer transaction package [{}] into the bounded local pool{}",
                        outcome
                            .accepted
                            .iter()
                            .map(ToString::to_string)
                            .collect::<Vec<_>>()
                            .join(", "),
                        if effects.is_empty() {
                            String::new()
                        } else {
                            format!(" after {}", effects.join(" and "))
                        }
                    ));
                }
                Ok(_) => {
                    candidate.remove_orphans(&txid_set);
                }
                Err(error) if error.is_missing_input() => {
                    let rejected_parent = source_package
                        .iter()
                        .flat_map(|transaction| &transaction.input)
                        .map(|input| input.previous_output.txid)
                        .find(|txid| candidate.is_recently_rejected(*txid));
                    if let Some(parent) = rejected_parent {
                        let source_txids = source_package
                            .iter()
                            .map(Transaction::compute_txid)
                            .collect::<BTreeSet<_>>();
                        for transaction in &source_package {
                            candidate.remember_rejected_txid(transaction.compute_txid());
                        }
                        candidate.remove_orphans(&source_txids);
                        rejected_messages.push(format!(
                            "rejected missing-parent peer transaction package because parent {parent} was recently rejected"
                        ));
                        continue;
                    }
                    let unavailable = unavailable_parent_txids(
                        &candidate,
                        chainstate,
                        &retained_package,
                        &source_package,
                    )?;
                    parent_requests.extend(
                        candidate.select_orphan_parent_requests(orphan_source, &unavailable),
                    );
                    let retained = if is_orphan_work || source_package.is_empty() {
                        0
                    } else {
                        candidate.retain_orphans(&source_package, now, orphan_source)
                    };
                    if retained > 0 {
                        deferred_messages.push(format!(
                            "retained {retained} missing-parent peer transaction{} for bounded retry",
                            if retained == 1 { "" } else { "s" }
                        ));
                    }
                }
                Err(error) => {
                    candidate.remove_orphans(&txid_set);
                    for transaction in &source_package {
                        candidate.remember_terminal_rejection(transaction, &error);
                    }
                    rejected_messages.push(format!(
                        "rejected peer transaction package [{}]: {error}",
                        txids
                            .iter()
                            .map(ToString::to_string)
                            .collect::<Vec<_>>()
                            .join(", ")
                    ));
                }
            }
        }
        if !accepted_parents.is_empty() {
            let triggers = std::mem::take(&mut accepted_parents);
            candidate.schedule_orphan_children(&triggers);
        }
        if processed_orphan_work {
            break;
        }
        let Some(orphan) = candidate.take_orphan_work(orphan_source) else {
            break;
        };
        processed_orphan_work = true;
        packages.push_back((vec![orphan], true));
    }
    let relay_snapshot = candidate.relay_snapshot();
    if let Some(store) = transaction_store {
        store
            .replace_and_clear_disconnected_at(
                &candidate.snapshot(),
                now,
                &expired_with_descendants,
            )
            .map_err(|error| error.to_string())?;
    }
    if let Some(estimator) = fee_estimator {
        let tracks = relay_snapshot
            .iter()
            .map(|entry| {
                Ok(FeeTrack {
                    txid: entry.transaction.compute_txid(),
                    fee_sats: entry.fee_sats,
                    policy_vsize: u32::try_from(entry.policy_vsize)
                        .map_err(|_| "transaction policy vsize exceeds u32".to_owned())?,
                })
            })
            .collect::<Result<Vec<_>, String>>()?;
        estimator
            .track_pool(&tracks, context.height)
            .map_err(|error| error.to_string())?;
    }
    let more_orphan_work = candidate.has_orphan_work(orphan_source);
    *pool = candidate;
    drop(pool);
    let relayed =
        relay_selected_transactions(transaction_relay, &relay_snapshot, &accepted_for_relay);
    if !relayed.is_empty() {
        if let Some(store) = transaction_store {
            store
                .record_relay_attempts(&relayed, now)
                .map_err(|error| error.to_string())?;
        }
    }
    let expired_removed = expired_removed.max(expired_with_descendants.len());
    if expired_removed > 0 {
        rbtc_info!(
            "expired {expired_removed} local mempool transaction{} or descendant{} after {} hours",
            if expired_removed == 1 { "" } else { "s" },
            if expired_removed == 1 { "" } else { "s" },
            DEFAULT_MEMPOOL_EXPIRY_SECS / 60 / 60
        );
    }
    if reconciled_removed > 0 {
        rbtc_info!(
            "removed {reconciled_removed} local transactions after active-chain reconciliation"
        );
    }
    for message in accepted_messages {
        rbtc_info!("{message}");
    }
    if expired_orphans > 0 {
        rbtc_info!(
            "expired {expired_orphans} missing-parent transaction{} after 20 minutes",
            if expired_orphans == 1 { "" } else { "s" }
        );
    }
    for message in deferred_messages {
        rbtc_info!("{message}");
    }
    for message in rejected_messages {
        rbtc_warn!("{message}");
    }
    Ok(PeerAdmissionProgress {
        more_orphan_work,
        parent_requests: parent_requests.into_iter().collect(),
    })
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
async fn sync_validating_node(
    session: &mut rbtc::p2p::PeerSession<tokio::net::TcpStream>,
    mut auxiliary: VecDeque<(SocketAddr, PendingPeer)>,
    network: Network,
    network_execution: NetworkExecutionMode,
    deployment_config: &DeploymentConfig,
    ibd_policy: IbdPolicy,
    data_dir: PathBuf,
    runtime_control: &Arc<RuntimeControl>,
    observer: Option<&NodeObserver>,
    active_peer: Option<SocketAddr>,
    once: bool,
    validation_target: Option<ValidationTarget>,
    validation_limits: ValidationLimits,
    ledger_retention: LedgerRetention,
    disk_monitor: DiskSpaceMonitor,
    cache: NodeCacheConfig,
    index_config: NodeIndexConfig,
    inbound_source: Option<&Arc<SharedInboundSource>>,
    mempool_full_rbf: bool,
    api_runtime: Option<&ApiRuntime>,
    background_validation: Option<&BackgroundValidationStatus>,
    validation_scheduler: Option<&BackgroundValidationStatus>,
    transaction_pool: &Arc<Mutex<TransactionAdmissionPool>>,
    transaction_relay: &broadcast::Sender<TransactionRelay>,
) -> Result<(), PeerRunError> {
    if network_execution.is_experimental() {
        if !supports_experimental_block_execution(network) || !once {
            return Err(PeerRunError::transient(
                "experimental block execution requires bitcoin or legacy testnet together with bounded --once mode",
            ));
        }
        if validation_target.is_none() {
            return Err(PeerRunError::transient(
                "experimental block execution requires an authenticated validation height and block hash hard ceiling",
            ));
        }
    }
    fs::create_dir_all(&data_dir)
        .map_err(|error| format!("create data directory {}: {error}", data_dir.display()))?;
    let mut disk_space = disk_monitor.check()?;
    rbtc_info!(
        "disk forecast total={} available={} required={} reserve={} bounded_storage_ceiling={}",
        disk_space.total_bytes,
        disk_space.available_bytes,
        disk_space.required_bytes,
        disk_space.reserve_bytes,
        disk_space.projected_storage_ceiling_bytes
    );
    let headers_path = data_dir.join("headers.redb");
    reject_legacy_split_chainstate(&data_dir)?;
    let chainstate_path = data_dir.join("chainstate.redb");
    let chainstate_options = ChainStoreOptions {
        quick_repair: validation_limits.quick_repair,
        retain_block_undo: !network_execution.is_experimental(),
        cache_size_bytes: chainstate_cache_bytes(
            network_execution,
            background_validation.is_some() || validation_scheduler.is_some(),
            cache,
        ),
        validation_delta_journal: network_execution.is_experimental()
            || background_validation.is_some()
            || validation_scheduler.is_some(),
    };
    let chainstate_open_started = Instant::now();
    let mut open_chainstate = tokio::task::spawn_blocking(move || {
        RedbChainStore::open_with_options(chainstate_path, network, chainstate_options)
    });
    let mut open_keepalive = tokio::time::interval(STANDBY_KEEPALIVE_INTERVAL);
    open_keepalive.tick().await;
    let chainstate = loop {
        tokio::select! {
            result = &mut open_chainstate => {
                break result
                    .map_err(|error| format!("chainstate open worker failed: {error}"))?
                    .map_err(|error| error.to_string())?;
            }
            _ = open_keepalive.tick() => {
                let keepalive = timeout(PEER_TIMEOUT, session.ping(rand::random())).await;
                if let Err(error) = keepalive
                    .map_err(|_| {
                        PeerRunError::transient(format!(
                            "active peer keepalive timed out while opening chainstate after {} seconds",
                            PEER_TIMEOUT.as_secs()
                        ))
                    })
                    .and_then(|result| result.map_err(|error| PeerRunError::p2p(&error)))
                {
                    let _ = open_chainstate.await;
                    return Err(error);
                }
            }
        }
    };
    let chainstate = Arc::new(chainstate);
    rbtc_info!(
        "opened chainstate in {} ms",
        chainstate_open_started.elapsed().as_millis()
    );
    let mut auxiliary_session = None;
    activate_next_auxiliary_peer(&mut auxiliary, &mut auxiliary_session).await;
    let execution_store = chainstate.execution();
    execution_store
        .bind_consensus_config(
            &deployment_config.consensus_id(),
            &deployment_config.legacy_consensus_id(),
            &DeploymentConfig::for_network(network).consensus_id(),
        )
        .map_err(|error| error.to_string())?;
    if let Some(target) =
        validation_target.filter(|_| !network_execution.extends_validation_target())
    {
        execution_store
            .bind_validation_target(rbtc::execution_store::ExecutionTip {
                height: target.height,
                hash: target.block_hash,
            })
            .map_err(|error| error.to_string())?;
    }
    let persisted_validation_target = execution_store
        .validation_target()
        .map_err(|error| error.to_string())?
        .map(|target| ValidationTarget {
            height: target.height,
            block_hash: target.hash,
        });
    let validation_target = if network_execution.extends_validation_target() {
        validation_target.or(persisted_validation_target)
    } else {
        persisted_validation_target.or(validation_target)
    };
    let (mempool_max_transactions, mempool_max_bytes) = {
        let pool = transaction_pool
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        (pool.max_transactions(), pool.max_bytes())
    };
    let transaction_store = validation_target
        .is_none()
        .then(|| {
            RedbTransactionPoolStore::open_with_capacity(
                data_dir.join("mempool.redb"),
                network,
                mempool_max_transactions,
                mempool_max_bytes,
            )
        })
        .transpose()
        .map_err(|error| error.to_string())?;
    let fee_estimator = validation_target
        .is_none()
        .then(|| RedbFeeEstimator::open(data_dir.join("fee_estimates.redb"), network).map(Arc::new))
        .transpose()
        .map_err(|error| error.to_string())?;
    let ledger = Arc::new(
        PrunedBlockLedger::open(data_dir.join("blocks"), ledger_retention)
            .map_err(|error| error.to_string())?,
    );
    rbtc_info!(
        "opened pruned ledger with targets max_blocks={} max_bytes={}",
        ledger_retention.max_blocks,
        ledger_retention.max_bytes
    );
    let explorer = api_runtime
        .map(|_| {
            RedbExplorerIndex::open(data_dir.join("explorer.redb"), network)
                .map(Arc::new)
                .map_err(|error| error.to_string())
        })
        .transpose()?;
    let auxiliary_indexes = Arc::new(
        AuxiliaryIndexes::open(&data_dir, network, index_config).map_err(PeerRunError::local)?,
    );
    let explorer_events = explorer
        .as_ref()
        .map(|explorer| {
            explorer
                .tip()
                .map(|tip| ExplorerEventHub::new(tip.height, tip.hash.to_string()))
                .map_err(|error| error.to_string())
        })
        .transpose()?;
    let wallet_runtime = api_runtime.and_then(|api| api.wallet.as_ref());
    let wallet = wallet_runtime.map(|runtime| runtime.wallet.as_ref());
    let mut api_server: Option<ApiServer> = None;
    let mut inbound_source_lease: Option<SharedInboundSourceLease> = None;
    let inbound_transactions = Arc::new(Mutex::new(InboundTransactionQueue::default()));
    let mut headers = sync_headers(session, deployment_config, headers_path.clone()).await?;
    let inbound_headers = Arc::new(RwLock::new(headers.clone()));
    if network_execution.extends_validation_target() {
        let target = validation_target.expect("parser requires an explicit extension target");
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
        execution_store
            .extend_validation_target(rbtc::execution_store::ExecutionTip {
                height: target.height,
                hash: target.block_hash,
            })
            .map_err(|error| error.to_string())?;
        rbtc_info!(
            "extended completed validation target to {}:{}",
            target.height,
            target.block_hash
        );
    }
    let mut disconnected_transactions = VecDeque::new();
    let node_status = api_runtime
        .map(|_| {
            collect_node_status(
                network,
                ibd_policy,
                &headers,
                &chainstate,
                explorer
                    .as_deref()
                    .expect("API runtime has an explorer index"),
                &ledger,
                wallet,
                &auxiliary_indexes,
                disk_space,
            )
            .map(|progress| {
                NodeStatus::with_inbound(progress, inbound_source.map(|source| source.stats()))
            })
        })
        .transpose()?;
    publish_runtime_status(
        observer,
        RuntimeStatusSources {
            network,
            ibd_policy,
            headers: &headers,
            chainstate: &chainstate,
            explorer: explorer.as_deref(),
            ledger: &ledger,
            wallet,
            auxiliary_indexes: &auxiliary_indexes,
            disk_space,
        },
    )?;
    'resync: loop {
        disk_space = disk_monitor.check()?;
        let compact_candidates = compact_transaction_candidates(transaction_pool, wallet_runtime);
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
        let mut prefetched_blocks = PrefetchedBlocks::default();
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
            match ledger
                .read_block(tip.height)
                .map_err(|error| error.to_string())?
            {
                Some(raw) => {
                    let disconnected = deserialize::<Block>(&raw).map_err(|error| {
                        PeerRunError::transient(format!(
                            "decode retained stale block at height {}: {error}",
                            tip.height
                        ))
                    })?;
                    if disconnected.block_hash() != tip.hash {
                        return Err(PeerRunError::transient(format!(
                            "retained stale block at height {} has hash {}, expected {}",
                            tip.height,
                            disconnected.block_hash(),
                            tip.hash
                        )));
                    }
                    retain_disconnected_block_transactions(
                        &mut disconnected_transactions,
                        &disconnected,
                        mempool_max_transactions,
                        mempool_max_bytes,
                    );
                    if let Some(store) = &transaction_store {
                        store
                            .replace_disconnected(
                                &disconnected_transactions
                                    .iter()
                                    .cloned()
                                    .collect::<Vec<_>>(),
                            )
                            .map_err(|error| error.to_string())?;
                    }
                }
                None => {
                    rbtc_warn!(
                        "retained stale block at height {} is unavailable; its transactions cannot be reconsidered",
                        tip.height
                    );
                }
            }
            let rewound = disconnect_execution_tip(
                &chainstate,
                &headers,
                u64::from(unix_time()?),
                DEFAULT_HOT_WINDOW_SECS,
            )
            .map_err(|error| error.to_string())?;
            if let Some(observer) = observer {
                observer.reorganized(
                    NodeTip {
                        height: tip.height,
                        hash: tip.hash,
                    },
                    NodeTip {
                        height: rewound.height,
                        hash: rewound.hash,
                    },
                );
            }
            transaction_pool
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clear_recent_confirmed_transactions();
            rbtc_info!(
                "disconnected stale execution tip; rewound to {}:{}",
                rewound.height,
                rewound.hash
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
            &compact_candidates,
        )
        .await?;
        let startup_pruned = prune_expired_block_undos(&chainstate, &headers, &ledger)?;
        prune_expired_auxiliary_index_undos(&auxiliary_indexes, &ledger)?;
        if startup_pruned > 0 {
            rbtc_info!(
                "pruned {startup_pruned} expired block undo record{} after ledger reconciliation",
                if startup_pruned == 1 { "" } else { "s" }
            );
        }
        if let Some(explorer) = explorer.as_deref() {
            reconcile_explorer(
                session,
                deployment_config,
                &headers,
                &chainstate,
                &ledger,
                explorer,
                explorer_events.as_ref(),
                &compact_candidates,
            )
            .await?;
        }
        reconcile_auxiliary_indexes(
            session,
            deployment_config,
            &headers,
            &chainstate,
            &ledger,
            &auxiliary_indexes,
            &compact_candidates,
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
                &compact_candidates,
            )
            .await?;
            reconcile_rebroadcast_state(wallet)?;
        }
        if let Some(status) = &node_status {
            status.update(collect_node_status(
                network,
                ibd_policy,
                &headers,
                &chainstate,
                explorer
                    .as_deref()
                    .expect("node status has an explorer index"),
                &ledger,
                wallet,
                &auxiliary_indexes,
                disk_space,
            )?);
        }
        publish_runtime_status(
            observer,
            RuntimeStatusSources {
                network,
                ibd_policy,
                headers: &headers,
                chainstate: &chainstate,
                explorer: explorer.as_deref(),
                ledger: &ledger,
                wallet,
                auxiliary_indexes: &auxiliary_indexes,
                disk_space,
            },
        )?;
        if api_server.is_none() {
            if let Some(api) = api_runtime {
                api_server = Some(
                    ApiServer::bind(
                        api.listen,
                        Arc::clone(
                            explorer
                                .as_ref()
                                .expect("API runtime has an explorer index"),
                        ),
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
                        fee_estimator.as_ref(),
                        background_validation,
                        Arc::clone(transaction_pool),
                        Arc::clone(runtime_control),
                        active_peer,
                        Arc::clone(&auxiliary_indexes),
                    )
                    .await?,
                );
            }
        }
        if inbound_source_lease.is_none() {
            if let Some(shared) = inbound_source {
                let source = Arc::new(NodeInboundSource {
                    headers: Arc::clone(&inbound_headers),
                    chainstate: Arc::clone(&chainstate),
                    ledger: Arc::clone(&ledger),
                    transaction_pool: Arc::clone(transaction_pool),
                    pending_transactions: Arc::clone(&inbound_transactions),
                    basic_filter: auxiliary_indexes.basic_filter.as_ref().map(Arc::clone),
                });
                let source: Arc<dyn InboundDataSource> = source;
                inbound_source_lease = Some(shared.install(source));
            }
        }

        loop {
            disk_space = disk_monitor.check()?;
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
                    explorer
                        .as_deref()
                        .expect("node status has an explorer index"),
                    &ledger,
                    wallet,
                    &auxiliary_indexes,
                    disk_space,
                )?);
            }
            publish_runtime_status(
                observer,
                RuntimeStatusSources {
                    network,
                    ibd_policy,
                    headers: &headers,
                    chainstate: &chainstate,
                    explorer: explorer.as_deref(),
                    ledger: &ledger,
                    wallet,
                    auxiliary_indexes: &auxiliary_indexes,
                    disk_space,
                },
            )?;
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
                    rbtc_info!(
                        "independent genesis validation stopped exactly at {}:{}",
                        tip.height,
                        tip.hash
                    );
                    return Ok(());
                }
            }
            if tip.height >= headers.active_tip().height {
                rbtc_info!("block execution caught up at height {}", tip.height);
                if let Some(estimator) = fee_estimator.as_ref() {
                    reconcile_fee_estimator(estimator, &headers, tip, &ledger)?;
                }
                let ibd_status = ibd_policy
                    .status(&headers)
                    .map_err(|error| error.to_string())?;
                if ibd_status.minimum_chainwork_reached {
                    let queued_inbound = inbound_transactions
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .drain();
                    for transaction in queued_inbound {
                        session
                            .queue_pending_transaction(transaction)
                            .map_err(|error| PeerRunError::p2p(&error))?;
                    }
                    fetch_announced_peer_transactions(session, transaction_pool, wallet_runtime)
                        .await?;
                    loop {
                        let progress = admit_pending_peer_transactions(
                            session,
                            transaction_pool,
                            transaction_store.as_ref(),
                            fee_estimator.as_deref(),
                            &disconnected_transactions,
                            transaction_relay,
                            &chainstate,
                            &headers,
                            deployment_config,
                            mempool_full_rbf,
                        )?;
                        let requested_parents = !progress.parent_requests.is_empty();
                        if requested_parents {
                            let received =
                                fetch_orphan_parent_transactions(session, progress.parent_requests)
                                    .await?;
                            if received > 0 {
                                rbtc_info!(
                                    "downloaded {received} missing orphan parent transaction{}",
                                    if received == 1 { "" } else { "s" }
                                );
                            }
                        }
                        if !progress.more_orphan_work && !requested_parents {
                            break;
                        }
                        tokio::task::yield_now().await;
                    }
                    if let Some(store) = transaction_store.as_ref() {
                        let active = transaction_pool
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .relay_snapshot();
                        let relayed = relay_due_peer_transactions(
                            store,
                            &active,
                            transaction_relay,
                            unix_time()?,
                        )?;
                        if relayed > 0 {
                            rbtc_info!(
                                "republished {relayed} due peer transaction{} to hot standbys",
                                if relayed == 1 { "" } else { "s" }
                            );
                        }
                    }
                    disconnected_transactions.clear();
                    rbtc_info!(
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
                    rbtc_info!("remaining in IBD: {error}");
                }
                if once {
                    return Ok(());
                }
                let poll_seconds = if background_validation.is_some() {
                    1
                } else {
                    30
                };
                wait_for_peer_poll(
                    session,
                    Duration::from_secs(poll_seconds),
                    wallet_runtime,
                    transaction_relay,
                )
                .await?;
                if background_validation.is_none() {
                    timeout(PEER_TIMEOUT, session.ping(rand::random()))
                        .await
                        .map_err(|_| PeerRunError::transient("peer ping timed out"))?
                        .map_err(|error| PeerRunError::p2p(&error))?;
                }
                headers = sync_headers(session, deployment_config, headers_path.clone()).await?;
                *inbound_headers
                    .write()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) = headers.clone();
                continue 'resync;
            }
            let effective_validation_limits = validation_scheduler
                .map_or(validation_limits, |status| {
                    status.adaptive_limits(validation_limits)
                });
            let Some(checkpoint) = InFlightCheckpoint::try_enter(
                &runtime_control.shutdown_requested,
                &runtime_control.checkpoints_in_flight,
            ) else {
                std::future::pending::<()>().await;
                unreachable!("a rejected checkpoint remains parked until runtime shutdown");
            };
            // Ordinary IBD and fixed-target validation both consume an immutable
            // active-header snapshot here, so the already bounded next block
            // window can download while the current checkpoint executes.
            // A zero-pause background validator may prefetch exactly one
            // already-effective bounded checkpoint too. If the operator
            // configures a pause, leave the socket idle so that throttle still
            // controls both execution and speculative network work.
            let overlap_next_download = validation_scheduler.is_none()
                || effective_validation_limits.pause_between_batches.is_zero();
            let auxiliary_was_active = auxiliary_session.is_some();
            download_execute_batch(
                session,
                deployment_config,
                &headers,
                &chainstate,
                &ledger,
                explorer.as_deref(),
                explorer_events.as_ref(),
                wallet,
                &auxiliary_indexes,
                &compact_candidates,
                transaction_pool,
                validation_target.map(|target| target.height),
                effective_validation_limits.max_blocks_per_batch,
                &mut auxiliary_session,
                &mut prefetched_blocks,
                overlap_next_download,
            )
            .await?;
            if auxiliary_was_active && auxiliary_session.is_none() {
                rbtc_warn!(
                    "auxiliary block-download peer retired after its first active-window failure; trying the next bounded candidate"
                );
            }
            if auxiliary_session.is_none() {
                activate_next_auxiliary_peer(&mut auxiliary, &mut auxiliary_session).await;
            }
            if let Some(wallet) = wallet_runtime {
                reconcile_wallet(
                    session,
                    deployment_config,
                    &headers,
                    execution_store,
                    &ledger,
                    &wallet.wallet,
                    wallet.scan,
                    &compact_candidates,
                )
                .await?;
                reconcile_rebroadcast_state(wallet)?;
            }
            if let Some(target) = validation_target {
                let tip = execution_store.tip().map_err(|error| error.to_string())?;
                if let Some(status) = validation_scheduler {
                    status.update_validation(tip.height);
                }
                let remaining = target.height.saturating_sub(tip.height);
                rbtc_info!(
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
            drop(checkpoint);
            // A completely prefetched batch may otherwise run from decode
            // through commit without yielding, starving the outer shutdown
            // selector across successive checkpoints.
            checkpoint_shutdown(&runtime_control.shutdown_requested).await;
        }
    }
}

#[allow(clippy::too_many_lines)]
fn reconcile_rebroadcast_state(wallet: &WalletApiRuntime) -> Result<(), String> {
    let tracked = wallet
        .rebroadcast
        .tracked_transactions()
        .map_err(|error| error.to_string())?;
    let (canonical, confirmed) = wallet
        .wallet
        .transaction_chain_state(&tracked)
        .map_err(|error| error.to_string())?;
    wallet
        .rebroadcast
        .reconcile_chain_state(&canonical, &confirmed, u64::from(unix_time()?))
        .map_err(|error| error.to_string())?;
    wallet.compact_candidates.replace(
        wallet
            .rebroadcast
            .unconfirmed_transactions()
            .map_err(|error| error.to_string())?,
    );
    Ok(())
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
async fn reconcile_wallet(
    session: &mut rbtc::p2p::PeerSession<tokio::net::TcpStream>,
    deployment_config: &DeploymentConfig,
    headers: &HeaderDag,
    execution_store: &RedbExecutionStore,
    ledger: &PrunedBlockLedger,
    wallet: &EmbeddedWallet,
    scan: WalletScanConfig,
    compact_candidates: &[Transaction],
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
        rbtc_info!(
            "disconnected stale wallet tip; rewound to {}:{}",
            common.height,
            common.hash
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
        compact_candidates,
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
            compact_candidates,
        )
        .await?;
        scan_needed = wallet
            .ensure_scan_lookahead(scan.gap_limit)
            .map_err(|error| error.to_string())?;
        if !scan_needed {
            wallet
                .record_scan_start_height(scan_start)
                .map_err(|error| error.to_string())?;
            rbtc_info!(
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
    compact_candidates: &[Transaction],
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
            timeout(PEER_TIMEOUT, session.request_blocks(&hashes))
                .await
                .map_err(|_| format!("wallet backfill getdata timed out for {batch_len} blocks"))?
                .map_err(|error| error.to_string())?;
            blocks = timeout(
                PEER_TIMEOUT,
                session.receive_requested_blocks_with_candidates(&hashes, compact_candidates),
            )
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
    compact_candidates: &[Transaction],
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
                    compact_candidates,
                )
                .await?;
                ledger
                    .commit_staged(validated_count)
                    .map_err(|error| error.to_string())?;
                rbtc_info!(
                    "recovered {validated_count} validated blocks from the staged ledger segment"
                );
            } else {
                ledger.discard_staged().map_err(|error| error.to_string())?;
            }
        }
    }

    backfill_ledger(
        session,
        deployment_config,
        headers,
        ledger,
        tip.height,
        compact_candidates,
    )
    .await
}

fn prune_expired_block_undos(
    chainstate: &RedbChainStore,
    headers: &HeaderDag,
    ledger: &PrunedBlockLedger,
) -> Result<u64, String> {
    let Some(retain_from_height) = ledger
        .stats()
        .map_err(|error| error.to_string())?
        .first_height
    else {
        return Ok(0);
    };
    chainstate
        .prune_block_undos_before(headers, retain_from_height)
        .map_err(|error| error.to_string())
}

fn prune_expired_auxiliary_index_undos(
    indexes: &AuxiliaryIndexes,
    ledger: &PrunedBlockLedger,
) -> Result<u64, String> {
    let Some(retain_from_height) = ledger
        .stats()
        .map_err(|error| error.to_string())?
        .first_height
    else {
        return Ok(0);
    };
    let mut removed = 0_u64;
    for (_, index) in indexes.enabled() {
        removed = removed.saturating_add(
            index
                .prune_undos_before(retain_from_height)
                .map_err(|error| error.to_string())?,
        );
    }
    Ok(removed)
}

async fn backfill_ledger(
    session: &mut rbtc::p2p::PeerSession<tokio::net::TcpStream>,
    deployment_config: &DeploymentConfig,
    headers: &HeaderDag,
    ledger: &PrunedBlockLedger,
    target_height: u32,
    compact_candidates: &[Transaction],
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
        timeout(PEER_TIMEOUT, session.request_blocks(&hashes))
            .await
            .map_err(|_| format!("ledger backfill getdata timed out for {batch_len} blocks"))?
            .map_err(|error| error.to_string())?;
        let blocks = timeout(
            PEER_TIMEOUT,
            session.receive_requested_blocks_with_candidates(&hashes, compact_candidates),
        )
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
    let deployments =
        block_deployment_context_for_headers(deployment_config, headers, height, expected_hash)
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
    compact_candidates: &[Transaction],
) -> Result<(), String> {
    let execution_store = chainstate.execution();
    let undo_store = chainstate.undos();
    let execution_tip = execution_store.tip().map_err(|error| error.to_string())?;
    let initial_explorer_tip = explorer.tip().map_err(|error| error.to_string())?;
    let initial_baseline = explorer.baseline().map_err(|error| error.to_string())?;
    let retained = ledger.stats().map_err(|error| error.to_string())?;
    validate_index_activation(
        IndexBuildState {
            kind: IndexKind::Explorer,
            best_height: initial_explorer_tip.height,
            required_from_height: 1,
            utxo_baseline_height: initial_baseline.map(|tip| tip.height),
        },
        IndexHistoryAvailability {
            local_first_height: retained.first_height,
            // `reconcile_explorer` is entered only with an established
            // full-history+witness session. Missing local history is fetched
            // through that authenticated active-chain path below.
            authenticated_peer_history: true,
        },
        execution_tip.height,
    )
    .map_err(|error| format!("explorer prune compatibility refusal: {error:?}"))?;
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
        rbtc_info!(
            "initialized snapshot-aware explorer baseline at {}:{} with {count} current UTXOs",
            execution_tip.height,
            execution_tip.hash
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
            rbtc_info!(
                "rebased snapshot-aware explorer at {}:{} with {count} current UTXOs after reorganization",
                execution_tip.height,
                execution_tip.hash
            );
            publish_explorer_event(events, ExplorerEventKind::Rebased, execution_tip);
            break;
        }
        let rewound = explorer
            .disconnect_tip()
            .map_err(|error| error.to_string())?;
        rbtc_info!(
            "disconnected stale explorer tip; rewound to {}:{}",
            rewound.height,
            rewound.hash
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
            timeout(PEER_TIMEOUT, session.request_blocks(&hashes))
                .await
                .map_err(|_| format!("explorer backfill getdata timed out for {batch_len} blocks"))?
                .map_err(|error| error.to_string())?;
            blocks = timeout(
                PEER_TIMEOUT,
                session.receive_requested_blocks_with_candidates(&hashes, compact_candidates),
            )
            .await
            .map_err(|_| format!("explorer backfill response timed out for {batch_len} blocks"))?
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
async fn reconcile_auxiliary_indexes(
    session: &mut rbtc::p2p::PeerSession<tokio::net::TcpStream>,
    deployment_config: &DeploymentConfig,
    headers: &HeaderDag,
    chainstate: &RedbChainStore,
    ledger: &PrunedBlockLedger,
    indexes: &AuxiliaryIndexes,
    compact_candidates: &[Transaction],
) -> Result<(), String> {
    let execution_tip = chainstate
        .execution()
        .tip()
        .map_err(|error| error.to_string())?;
    let retained = ledger.stats().map_err(|error| error.to_string())?;
    for (kind, index) in indexes.enabled() {
        let label = auxiliary_index_label(kind);
        let initial_tip = index.tip().map_err(|error| error.to_string())?;
        validate_index_activation(
            IndexBuildState {
                kind: auxiliary_policy_kind(kind),
                best_height: initial_tip.height,
                required_from_height: 1,
                utxo_baseline_height: None,
            },
            IndexHistoryAvailability {
                local_first_height: retained.first_height,
                authenticated_peer_history: true,
            },
            execution_tip.height,
        )
        .map_err(|error| format!("{label} index activation refusal: {error:?}"))?;

        loop {
            let tip = index.tip().map_err(|error| error.to_string())?;
            if tip.height <= execution_tip.height
                && headers
                    .active_header_at(tip.height)
                    .is_some_and(|header| header.hash == tip.hash)
            {
                break;
            }
            let parent = index.disconnect_tip().map_err(|error| error.to_string())?;
            rbtc_info!(
                "disconnected stale {label} index tip; rewound to {}:{}",
                parent.height,
                parent.hash
            );
        }

        loop {
            let tip = index.tip().map_err(|error| error.to_string())?;
            if tip.height >= execution_tip.height {
                break;
            }
            let next_height = tip
                .height
                .checked_add(1)
                .ok_or_else(|| format!("{label} index height overflow"))?;
            let batch_len = usize::try_from(execution_tip.height - tip.height)
                .unwrap_or(usize::MAX)
                .min(MAX_BLOCKS_IN_FLIGHT);
            let expected = (0..batch_len)
                .map(|offset| {
                    let height = next_height
                        .checked_add(u32::try_from(offset).expect("index batch fits u32"))
                        .ok_or_else(|| format!("{label} index height overflow"))?;
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
                    format!(
                        "decode retained block for {label} index at height {}: {error}",
                        header.height
                    )
                })?);
            }
            if !all_local {
                let hashes = expected
                    .iter()
                    .map(|header| header.hash)
                    .collect::<Vec<_>>();
                timeout(PEER_TIMEOUT, session.request_blocks(&hashes))
                    .await
                    .map_err(|_| {
                        format!("{label} index backfill getdata timed out for {batch_len} blocks")
                    })?
                    .map_err(|error| error.to_string())?;
                blocks = timeout(
                    PEER_TIMEOUT,
                    session.receive_requested_blocks_with_candidates(&hashes, compact_candidates),
                )
                .await
                .map_err(|_| {
                    format!("{label} index backfill response timed out for {batch_len} blocks")
                })?
                .map_err(|error| error.to_string())?;
            }
            let mut block_undos = Vec::with_capacity(blocks.len());
            for (expected, block) in expected.iter().zip(&blocks) {
                validate_archive_block(
                    deployment_config,
                    headers,
                    expected.height,
                    expected.hash,
                    block,
                )?;
                let transaction_undos = if kind == AuxiliaryIndexKind::Transaction {
                    Vec::new()
                } else {
                    chainstate
                        .undos()
                        .get(expected.hash)
                        .map_err(|error| error.to_string())?
                        .ok_or_else(|| {
                            format!(
                                "{label} index requires spent-prevout undo at height {}; the pruned history cannot reconstruct it from blocks or a UTXO baseline—run --reindex-chainstate into a new directory with this index enabled",
                                expected.height
                            )
                        })?
                };
                block_undos.push(transaction_undos);
            }
            let batch = expected
                .iter()
                .zip(&blocks)
                .zip(&block_undos)
                .map(|((expected, block), undos)| (expected.height, block, undos.as_slice()))
                .collect::<Vec<_>>();
            index
                .connect_batch(&batch)
                .map_err(|error| error.to_string())?;
            tokio::task::yield_now().await;
        }
        let tip = index.tip().map_err(|error| error.to_string())?;
        if tip != initial_tip {
            rbtc_info!(
                "{label} index caught up from height {} to {}:{}",
                initial_tip.height,
                tip.height,
                tip.hash
            );
        }
    }
    Ok(())
}

async fn request_block_window(
    session: &mut rbtc::p2p::PeerSession<tokio::net::TcpStream>,
    hashes: &[BlockHash],
    context: &'static str,
) -> Result<(), PeerRunError> {
    for request_hashes in hashes.chunks(MAX_BLOCKS_IN_FLIGHT) {
        timeout(PEER_TIMEOUT, session.request_blocks(request_hashes))
            .await
            .map_err(|_| {
                PeerRunError::transient(format!(
                    "{context} getdata timed out for {} blocks",
                    request_hashes.len()
                ))
            })?
            .map_err(|error| PeerRunError::p2p(&error))?;
    }
    Ok(())
}

#[cfg(test)]
fn split_parallel_block_windows(
    hashes: &[BlockHash],
) -> (&[BlockHash], &[BlockHash], &[BlockHash]) {
    let primary_len = hashes.len().min(VALIDATION_BLOCK_WINDOW_SIZE);
    let (primary, remaining) = hashes.split_at(primary_len);
    let auxiliary_len = remaining.len().min(VALIDATION_BLOCK_WINDOW_SIZE);
    let (auxiliary, primary_remainder) = remaining.split_at(auxiliary_len);
    (primary, auxiliary, primary_remainder)
}

async fn receive_block_window(
    session: &mut rbtc::p2p::PeerSession<tokio::net::TcpStream>,
    hashes: &[BlockHash],
    compact_candidates: &[Transaction],
    context: &'static str,
) -> Result<Vec<Block>, PeerRunError> {
    timeout(
        PEER_TIMEOUT,
        session.receive_pipelined_blocks_with_candidates(hashes, compact_candidates),
    )
    .await
    .map_err(|_| {
        PeerRunError::transient(format!(
            "{context} block response timed out for {} blocks",
            hashes.len()
        ))
    })?
    .map_err(|error| PeerRunError::p2p(&error))
}

async fn receive_block_window_into(
    session: &mut rbtc::p2p::PeerSession<tokio::net::TcpStream>,
    hashes: &[BlockHash],
    compact_candidates: &[Transaction],
    blocks: &mut [Option<Block>],
    context: &'static str,
) -> Result<(), PeerRunError> {
    timeout(
        PEER_TIMEOUT,
        session.receive_pipelined_blocks_into_with_candidates(hashes, compact_candidates, blocks),
    )
    .await
    .map_err(|_| {
        PeerRunError::transient(format!(
            "{context} block response timed out for {} blocks",
            hashes.len()
        ))
    })?
    .map_err(|error| PeerRunError::p2p(&error))
}

async fn download_block_window(
    session: &mut rbtc::p2p::PeerSession<tokio::net::TcpStream>,
    hashes: &[BlockHash],
    compact_candidates: &[Transaction],
    context: &'static str,
) -> Result<Vec<Block>, PeerRunError> {
    request_block_window(session, hashes, context).await?;
    receive_block_window(session, hashes, compact_candidates, context).await
}

async fn download_execution_prefetch(
    session: &mut rbtc::p2p::PeerSession<tokio::net::TcpStream>,
    auxiliary_session: &mut Option<rbtc::p2p::PeerSession<tokio::net::TcpStream>>,
    hashes: &[BlockHash],
    compact_candidates: &[Transaction],
) -> Result<Vec<Vec<u8>>, PeerRunError> {
    let mut serialized = Vec::with_capacity(hashes.len());
    let mut offset = 0;
    while offset < hashes.len() {
        let remaining = &hashes[offset..];
        if auxiliary_session.is_some() && remaining.len() > VALIDATION_BLOCK_WINDOW_SIZE {
            let primary_len = remaining.len().min(VALIDATION_BLOCK_WINDOW_SIZE);
            let auxiliary_len = (remaining.len() - primary_len).min(VALIDATION_BLOCK_WINDOW_SIZE);
            let auxiliary = auxiliary_session
                .as_mut()
                .expect("auxiliary session was checked");
            let (primary_blocks, auxiliary_blocks, keep_auxiliary) = download_parallel_block_pair(
                session,
                &remaining[..primary_len],
                auxiliary,
                &remaining[primary_len..primary_len + auxiliary_len],
                compact_candidates,
            )
            .await?;
            serialized.extend(primary_blocks.into_iter().map(|block| serialize(&block)));
            serialized.extend(auxiliary_blocks.into_iter().map(|block| serialize(&block)));
            offset += primary_len + auxiliary_len;
            if !keep_auxiliary {
                *auxiliary_session = None;
            }
        } else {
            let window_len = remaining.len().min(VALIDATION_BLOCK_WINDOW_SIZE);
            let blocks = download_block_window(
                session,
                &remaining[..window_len],
                compact_candidates,
                "execution prefetch",
            )
            .await?;
            serialized.extend(blocks.into_iter().map(|block| serialize(&block)));
            offset += window_len;
        }
    }
    Ok(serialized)
}

async fn receive_parallel_block_windows(
    primary: &mut rbtc::p2p::PeerSession<tokio::net::TcpStream>,
    primary_hashes: &[BlockHash],
    auxiliary: &mut rbtc::p2p::PeerSession<tokio::net::TcpStream>,
    auxiliary_hashes: &[BlockHash],
    compact_candidates: &[Transaction],
    auxiliary_grace: Duration,
) -> Result<
    (
        Vec<Block>,
        Vec<Option<Block>>,
        Option<Result<(), PeerRunError>>,
    ),
    PeerRunError,
> {
    let mut auxiliary_blocks = (0..auxiliary_hashes.len())
        .map(|_| None)
        .collect::<Vec<_>>();
    let primary_receive =
        receive_block_window(primary, primary_hashes, compact_candidates, "primary");
    let mut auxiliary_result = None;
    let primary_result;
    {
        let auxiliary_receive = receive_block_window_into(
            auxiliary,
            auxiliary_hashes,
            compact_candidates,
            &mut auxiliary_blocks,
            "auxiliary",
        );
        tokio::pin!(primary_receive);
        tokio::pin!(auxiliary_receive);
        primary_result = loop {
            tokio::select! {
                result = &mut primary_receive => break result,
                result = &mut auxiliary_receive, if auxiliary_result.is_none() => {
                    auxiliary_result = Some(result);
                }
            }
        };
        if auxiliary_result.is_none() {
            if let Ok(result) = timeout(auxiliary_grace, &mut auxiliary_receive).await {
                auxiliary_result = Some(result);
            }
        }
    }
    primary_result.map(|blocks| (blocks, auxiliary_blocks, auxiliary_result))
}

async fn recover_lagging_auxiliary_window(
    primary: &mut rbtc::p2p::PeerSession<tokio::net::TcpStream>,
    auxiliary_hashes: &[BlockHash],
    compact_candidates: &[Transaction],
    mut auxiliary_blocks: Vec<Option<Block>>,
) -> Result<Vec<Block>, PeerRunError> {
    let received = auxiliary_blocks
        .iter()
        .filter(|block| block.is_some())
        .count();
    let missing_hashes = auxiliary_blocks
        .iter()
        .zip(auxiliary_hashes)
        .filter_map(|(block, hash)| block.is_none().then_some(*hash))
        .collect::<Vec<_>>();
    rbtc_warn!(
        "auxiliary block window lagged the primary response by more than {} ms after delivering {received}/{} blocks; requesting only {} missing blocks from primary",
        AUXILIARY_BLOCK_RESPONSE_GRACE.as_millis(),
        auxiliary_hashes.len(),
        missing_hashes.len(),
    );
    let missing_blocks = if missing_hashes.is_empty() {
        Vec::new()
    } else {
        download_block_window(
            primary,
            &missing_hashes,
            compact_candidates,
            "primary fallback",
        )
        .await?
    };
    let mut missing_blocks = missing_blocks.into_iter();
    for block in &mut auxiliary_blocks {
        if block.is_none() {
            *block = Some(
                missing_blocks
                    .next()
                    .expect("primary fallback returned every missing block"),
            );
        }
    }
    debug_assert!(missing_blocks.next().is_none());
    Ok(auxiliary_blocks
        .into_iter()
        .map(|block| block.expect("fallback filled every auxiliary response slot"))
        .collect())
}

async fn download_parallel_block_pair(
    primary: &mut rbtc::p2p::PeerSession<tokio::net::TcpStream>,
    primary_hashes: &[BlockHash],
    auxiliary: &mut rbtc::p2p::PeerSession<tokio::net::TcpStream>,
    auxiliary_hashes: &[BlockHash],
    compact_candidates: &[Transaction],
) -> Result<(Vec<Block>, Vec<Block>, bool), PeerRunError> {
    let (primary_request, auxiliary_request) = tokio::join!(
        request_block_window(primary, primary_hashes, "primary"),
        request_block_window(auxiliary, auxiliary_hashes, "auxiliary"),
    );
    primary_request?;
    if let Err(error) = auxiliary_request {
        rbtc_warn!("auxiliary block window disabled after request failure: {error}");
        let primary_blocks =
            receive_block_window(primary, primary_hashes, compact_candidates, "primary").await?;
        let auxiliary_blocks = download_block_window(
            primary,
            auxiliary_hashes,
            compact_candidates,
            "primary fallback",
        )
        .await?;
        return Ok((primary_blocks, auxiliary_blocks, false));
    }

    let (primary_blocks, auxiliary_blocks, auxiliary_result) = receive_parallel_block_windows(
        primary,
        primary_hashes,
        auxiliary,
        auxiliary_hashes,
        compact_candidates,
        AUXILIARY_BLOCK_RESPONSE_GRACE,
    )
    .await?;
    match auxiliary_result {
        Some(Ok(())) => Ok((
            primary_blocks,
            auxiliary_blocks
                .into_iter()
                .map(|block| block.expect("completed auxiliary response filled every slot"))
                .collect(),
            true,
        )),
        Some(Err(error)) => {
            rbtc_warn!(
                "auxiliary block window disabled after response failure; retrying on primary: {error}"
            );
            let auxiliary_blocks = download_block_window(
                primary,
                auxiliary_hashes,
                compact_candidates,
                "primary fallback",
            )
            .await?;
            Ok((primary_blocks, auxiliary_blocks, false))
        }
        None => Ok((
            primary_blocks,
            recover_lagging_auxiliary_window(
                primary,
                auxiliary_hashes,
                compact_candidates,
                auxiliary_blocks,
            )
            .await?,
            false,
        )),
    }
}

fn shard_validation_delta_candidates(
    chainstate: &RedbChainStore,
    heights: &[u32],
) -> Result<Vec<ValidationDeltaShardMigration>, ChainStoreError> {
    let mut migrations = Vec::with_capacity(heights.len());
    for height in heights {
        if let Some(migration) = chainstate.shard_legacy_validation_delta(*height)? {
            migrations.push(migration);
        }
    }
    Ok(migrations)
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
async fn download_execute_batch(
    session: &mut rbtc::p2p::PeerSession<tokio::net::TcpStream>,
    deployment_config: &DeploymentConfig,
    headers: &HeaderDag,
    chainstate: &RedbChainStore,
    ledger: &PrunedBlockLedger,
    explorer: Option<&RedbExplorerIndex>,
    explorer_events: Option<&ExplorerEventHub>,
    wallet: Option<&EmbeddedWallet>,
    auxiliary_indexes: &AuxiliaryIndexes,
    compact_candidates: &[Transaction],
    transaction_pool: &Arc<Mutex<TransactionAdmissionPool>>,
    maximum_height: Option<u32>,
    maximum_batch_size: usize,
    auxiliary_session: &mut Option<rbtc::p2p::PeerSession<tokio::net::TcpStream>>,
    prefetched_blocks: &mut PrefetchedBlocks,
    prefetch_next_batch: bool,
) -> Result<(), PeerRunError> {
    let batch_started = Instant::now();
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
    let mut expected = (0..batch_len)
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
    let mut blocks = Vec::with_capacity(batch_len);
    let prefetched = std::mem::take(prefetched_blocks);
    if prefetched.serialized.len() > hashes.len() {
        return Err(PeerRunError::transient(
            "prefetched blocks exceed the next active-chain batch",
        ));
    }
    let decoded_prefetch = prefetched
        .serialized
        .into_iter()
        .map(|bytes| {
            deserialize::<Block>(&bytes).map_err(|error| {
                PeerRunError::transient(format!("decode execution-prefetched block: {error}"))
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    if decoded_prefetch
        .iter()
        .zip(&hashes)
        .any(|(block, expected_hash)| block.block_hash() != *expected_hash)
    {
        return Err(PeerRunError::transient(
            "prefetched blocks do not match the next active-chain batch",
        ));
    }
    blocks.extend(decoded_prefetch);
    let mut offset = blocks.len();
    while offset < hashes.len() {
        let remaining = &hashes[offset..];
        if auxiliary_session.is_some() && remaining.len() > VALIDATION_BLOCK_WINDOW_SIZE {
            let primary_len = remaining.len().min(VALIDATION_BLOCK_WINDOW_SIZE);
            let auxiliary_len = (remaining.len() - primary_len).min(VALIDATION_BLOCK_WINDOW_SIZE);
            let primary_hashes = &remaining[..primary_len];
            let auxiliary_hashes = &remaining[primary_len..primary_len + auxiliary_len];
            let auxiliary = auxiliary_session
                .as_mut()
                .expect("auxiliary session was checked");
            let (primary_blocks, auxiliary_blocks, keep_auxiliary) = download_parallel_block_pair(
                session,
                primary_hashes,
                auxiliary,
                auxiliary_hashes,
                compact_candidates,
            )
            .await?;
            blocks.extend(primary_blocks);
            blocks.extend(auxiliary_blocks);
            offset += primary_len + auxiliary_len;
            if !keep_auxiliary {
                *auxiliary_session = None;
            }
        } else {
            let window_len = remaining.len().min(VALIDATION_BLOCK_WINDOW_SIZE);
            blocks.extend(
                download_block_window(
                    session,
                    &remaining[..window_len],
                    compact_candidates,
                    "primary remainder",
                )
                .await?,
            );
            offset += window_len;
        }
    }
    let downloaded_at = Instant::now();
    let validated_blocks =
        validate_downloaded_blocks(deployment_config, headers, &expected, &blocks)?;
    let mut deployment_contexts = Vec::with_capacity(validated_blocks.len());
    let mut transaction_ids = Vec::with_capacity(validated_blocks.len());
    let mut serialized = Vec::with_capacity(validated_blocks.len());
    for (deployments, txids, bytes) in validated_blocks {
        deployment_contexts.push(deployments);
        transaction_ids.push(txids);
        serialized.push(bytes);
    }
    let archive_batch_len =
        bounded_archive_prefix_len(&serialized).map_err(|error| error.to_string())?;
    let mut carried_prefetch = if archive_batch_len < serialized.len() {
        let downloaded_len = serialized.len();
        let carried = serialized.split_off(archive_batch_len);
        expected.truncate(archive_batch_len);
        blocks.truncate(archive_batch_len);
        deployment_contexts.truncate(archive_batch_len);
        transaction_ids.truncate(archive_batch_len);
        rbtc_warn!(
            "validation batch reduced from {downloaded_len} to {archive_batch_len} blocks at the archive record-byte ceiling; carrying {} verified blocks into the next batch",
            carried.len()
        );
        carried
    } else {
        Vec::new()
    };
    let structure_validated_at = Instant::now();
    let mut prefetch_error = None;
    let next_prefetch_hashes = if prefetch_next_batch {
        let last_height = expected
            .last()
            .expect("non-empty block batch has a last header")
            .height;
        let carried_len =
            u32::try_from(carried_prefetch.len()).expect("validation prefetch length fits u32");
        let carried_height = last_height
            .checked_add(carried_len)
            .expect("bounded carried prefetch height can advance");
        let remaining_after_carry = execution_ceiling.saturating_sub(carried_height);
        let prefetch_limit = validation_prefetch_limit(maximum_batch_size);
        let prefetch_capacity = prefetch_limit.saturating_sub(carried_prefetch.len());
        let prefetch_len = usize::try_from(remaining_after_carry)
            .unwrap_or(usize::MAX)
            .min(prefetch_capacity);
        if prefetch_len > 0 {
            let next_height = carried_height
                .checked_add(1)
                .expect("non-final validation batch height can advance");
            let next_hashes = (0..prefetch_len)
                .map(|offset| {
                    let height = next_height
                        .checked_add(u32::try_from(offset).expect("prefetch window fits u32"))
                        .ok_or_else(|| "prefetched block height overflow".to_owned())?;
                    headers
                        .active_header_at(height)
                        .map(|header| header.hash)
                        .ok_or_else(|| format!("missing active header at height {height}"))
                })
                .collect::<Result<Vec<_>, String>>();
            match next_hashes {
                Ok(next_hashes) => next_hashes,
                Err(error) => {
                    prefetch_error = Some(PeerRunError::transient(error));
                    Vec::new()
                }
            }
        } else {
            Vec::new()
        }
    } else {
        Vec::new()
    };
    let now = u64::from(unix_time()?);
    let delta_shard_candidates = (0..32)
        .filter_map(|_| chainstate.take_hottest_legacy_validation_delta())
        .collect::<Vec<_>>();
    let staged_at;
    let delta_shard_migrations;
    let delta_shard_elapsed;
    let (
        applied_blocks,
        downloaded_prefetch,
        execution_core_at,
        prefetch_elapsed,
        utxo_prefetch_elapsed,
    ) = if next_prefetch_hashes.is_empty() {
        let (
            stage_result,
            branch_staged_at,
            utxo_result,
            utxo_elapsed,
            shard_result,
            shard_elapsed,
        ) = {
            let work = || {
                std::thread::scope(|scope| {
                    let utxos = scope.spawn(|| {
                        let started = Instant::now();
                        let result = prefetch_prevalidated_active_block_utxos(
                            chainstate,
                            &blocks,
                            &transaction_ids,
                        );
                        (result, started.elapsed())
                    });
                    let shard = scope.spawn(|| {
                        let started = Instant::now();
                        let result =
                            shard_validation_delta_candidates(chainstate, &delta_shard_candidates);
                        (result, started.elapsed())
                    });
                    let stage_result = ledger.stage(next_height, &serialized);
                    let branch_staged_at = Instant::now();
                    let (utxo_result, utxo_elapsed) = utxos
                        .join()
                        .expect("scoped UTXO-prefetch thread must not panic");
                    let (shard_result, shard_elapsed) = shard
                        .join()
                        .expect("scoped validation-delta shard thread must not panic");
                    (
                        stage_result,
                        branch_staged_at,
                        utxo_result,
                        utxo_elapsed,
                        shard_result,
                        shard_elapsed,
                    )
                })
            };
            if tokio::runtime::Handle::current().runtime_flavor()
                == tokio::runtime::RuntimeFlavor::MultiThread
            {
                tokio::task::block_in_place(work)
            } else {
                work()
            }
        };
        stage_result.map_err(|error| error.to_string())?;
        delta_shard_migrations = shard_result.map_err(|error| error.to_string())?;
        delta_shard_elapsed = shard_elapsed;
        staged_at = branch_staged_at;
        let prefetched_utxos = utxo_result.map_err(|error| PeerRunError::block(&error))?;
        let applied_blocks = connect_prevalidated_active_blocks_with_txids_and_utxos(
            chainstate,
            headers,
            &blocks,
            &transaction_ids,
            prefetched_utxos,
            now,
            DEFAULT_HOT_WINDOW_SECS,
            &deployment_contexts,
        )
        .map_err(|error| PeerRunError::block(&error))?;
        (
            applied_blocks,
            Vec::new(),
            Instant::now(),
            Duration::ZERO,
            utxo_elapsed,
        )
    } else if tokio::runtime::Handle::current().runtime_flavor()
        == tokio::runtime::RuntimeFlavor::MultiThread
    {
        let runtime = tokio::runtime::Handle::current();
        let (
            stage_result,
            branch_staged_at,
            execution_result,
            execution_core_at,
            prefetch_result,
            prefetch_elapsed,
            utxo_prefetch_elapsed,
            shard_result,
            shard_elapsed,
        ) = tokio::task::block_in_place(|| {
            std::thread::scope(|scope| {
                let prefetch = scope.spawn(|| {
                    let started = Instant::now();
                    let result = runtime.block_on(download_execution_prefetch(
                        session,
                        auxiliary_session,
                        &next_prefetch_hashes,
                        compact_candidates,
                    ));
                    (result, started.elapsed())
                });
                let utxos = scope.spawn(|| {
                    let started = Instant::now();
                    let result = prefetch_prevalidated_active_block_utxos(
                        chainstate,
                        &blocks,
                        &transaction_ids,
                    );
                    (result, started.elapsed())
                });
                let shard = scope.spawn(|| {
                    let started = Instant::now();
                    let result =
                        shard_validation_delta_candidates(chainstate, &delta_shard_candidates);
                    (result, started.elapsed())
                });
                let stage_result = ledger.stage(next_height, &serialized);
                let branch_staged_at = Instant::now();
                let (utxo_result, utxo_prefetch_elapsed) = utxos
                    .join()
                    .expect("scoped UTXO-prefetch thread must not panic");
                let execution_result = match (stage_result.as_ref(), utxo_result) {
                    (Ok(()), Ok(prefetched_utxos)) => {
                        Some(connect_prevalidated_active_blocks_with_txids_and_utxos(
                            chainstate,
                            headers,
                            &blocks,
                            &transaction_ids,
                            prefetched_utxos,
                            now,
                            DEFAULT_HOT_WINDOW_SECS,
                            &deployment_contexts,
                        ))
                    }
                    (Ok(()), Err(error)) => Some(Err(error)),
                    (Err(_), _) => None,
                };
                let execution_core_at = Instant::now();
                let (prefetch_result, prefetch_elapsed) = prefetch
                    .join()
                    .expect("scoped execution-prefetch thread must not panic");
                let (shard_result, shard_elapsed) = shard
                    .join()
                    .expect("scoped validation-delta shard thread must not panic");
                (
                    stage_result,
                    branch_staged_at,
                    execution_result,
                    execution_core_at,
                    prefetch_result,
                    prefetch_elapsed,
                    utxo_prefetch_elapsed,
                    shard_result,
                    shard_elapsed,
                )
            })
        });
        stage_result.map_err(|error| error.to_string())?;
        delta_shard_migrations = shard_result.map_err(|error| error.to_string())?;
        delta_shard_elapsed = shard_elapsed;
        staged_at = branch_staged_at;
        let execution_result =
            execution_result.expect("successful staging starts chainstate execution");
        let applied_blocks = execution_result.map_err(|error| PeerRunError::block(&error))?;
        let downloaded_prefetch = match prefetch_result {
            Ok(blocks) => blocks,
            Err(error) => {
                prefetch_error = Some(error);
                Vec::new()
            }
        };
        (
            applied_blocks,
            downloaded_prefetch,
            execution_core_at,
            prefetch_elapsed,
            utxo_prefetch_elapsed,
        )
    } else {
        let (
            stage_result,
            branch_staged_at,
            utxo_result,
            utxo_elapsed,
            shard_result,
            shard_elapsed,
        ) = std::thread::scope(|scope| {
            let utxos = scope.spawn(|| {
                let started = Instant::now();
                let result =
                    prefetch_prevalidated_active_block_utxos(chainstate, &blocks, &transaction_ids);
                (result, started.elapsed())
            });
            let shard = scope.spawn(|| {
                let started = Instant::now();
                let result = shard_validation_delta_candidates(chainstate, &delta_shard_candidates);
                (result, started.elapsed())
            });
            let stage_result = ledger.stage(next_height, &serialized);
            let branch_staged_at = Instant::now();
            let (utxo_result, utxo_elapsed) = utxos
                .join()
                .expect("scoped UTXO-prefetch thread must not panic");
            let (shard_result, shard_elapsed) = shard
                .join()
                .expect("scoped validation-delta shard thread must not panic");
            (
                stage_result,
                branch_staged_at,
                utxo_result,
                utxo_elapsed,
                shard_result,
                shard_elapsed,
            )
        });
        stage_result.map_err(|error| error.to_string())?;
        delta_shard_migrations = shard_result.map_err(|error| error.to_string())?;
        delta_shard_elapsed = shard_elapsed;
        staged_at = branch_staged_at;
        let prefetched_utxos = utxo_result.map_err(|error| PeerRunError::block(&error))?;
        let applied_blocks = connect_prevalidated_active_blocks_with_txids_and_utxos(
            chainstate,
            headers,
            &blocks,
            &transaction_ids,
            prefetched_utxos,
            now,
            DEFAULT_HOT_WINDOW_SECS,
            &deployment_contexts,
        )
        .map_err(|error| PeerRunError::block(&error))?;
        let execution_core_at = Instant::now();
        let prefetch_started = Instant::now();
        let downloaded_prefetch = match download_execution_prefetch(
            session,
            auxiliary_session,
            &next_prefetch_hashes,
            compact_candidates,
        )
        .await
        {
            Ok(blocks) => blocks,
            Err(error) => {
                prefetch_error = Some(error);
                Vec::new()
            }
        };
        (
            applied_blocks,
            downloaded_prefetch,
            execution_core_at,
            prefetch_started.elapsed(),
            utxo_elapsed,
        )
    };
    let execution_prefetch_count = carried_prefetch.len() + downloaded_prefetch.len();
    carried_prefetch.extend(downloaded_prefetch);
    prefetched_blocks.serialized = carried_prefetch;
    let executed_at = Instant::now();
    if maximum_height.is_none() {
        let removed_orphans = {
            let mut pool = transaction_pool
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let removed = pool.remove_orphans_for_block_transactions(
                blocks.iter().flat_map(|block| &block.txdata),
            );
            pool.remember_confirmed_transactions(blocks.iter().flat_map(|block| &block.txdata));
            removed
        };
        if removed_orphans > 0 {
            rbtc_info!(
                "removed {removed_orphans} orphan transaction{} included or conflicted by connected blocks",
                if removed_orphans == 1 { "" } else { "s" }
            );
        }
    }
    index_validated_blocks(
        explorer,
        explorer_events,
        wallet,
        auxiliary_indexes,
        &expected,
        &blocks,
        &applied_blocks,
    )?;
    let indexed_at = Instant::now();
    let first = expected
        .first()
        .expect("non-empty block batch has a first header");
    let last = expected
        .last()
        .expect("non-empty block batch has a last header");
    validate_live_indexes_before_prune(explorer, wallet, auxiliary_indexes, last.height)?;
    ledger
        .commit_staged(u32::try_from(blocks.len()).expect("block download batch count fits u32"))
        .map_err(|error| error.to_string())?;
    let pruned_undos = prune_expired_block_undos(chainstate, headers, ledger)?;
    let pruned_index_undos = prune_expired_auxiliary_index_undos(auxiliary_indexes, ledger)?;
    let published_at = Instant::now();
    rbtc_info!(
        "validated and executed {} blocks {}-{}; active tip {}:{}; timings download={}ms structure={}ms stage={}ms execute={}ms execution-core={}ms utxo-prefetch={}ms prefetch={}ms index={}ms publish={}ms total={}ms",
        blocks.len(),
        first.height,
        last.height,
        last.height,
        last.hash,
        downloaded_at.duration_since(batch_started).as_millis(),
        structure_validated_at
            .duration_since(downloaded_at)
            .as_millis(),
        staged_at.duration_since(structure_validated_at).as_millis(),
        executed_at.duration_since(staged_at).as_millis(),
        execution_core_at.duration_since(staged_at).as_millis(),
        utxo_prefetch_elapsed.as_millis(),
        prefetch_elapsed.as_millis(),
        indexed_at.duration_since(executed_at).as_millis(),
        published_at.duration_since(indexed_at).as_millis(),
        published_at.duration_since(batch_started).as_millis(),
    );
    if pruned_undos > 0 {
        rbtc_info!(
            "pruned {pruned_undos} block undo record{} below the retained ledger window",
            if pruned_undos == 1 { "" } else { "s" }
        );
    }
    if pruned_index_undos > 0 {
        rbtc_info!(
            "pruned {pruned_index_undos} optional-index rollback record{} below the retained ledger window",
            if pruned_index_undos == 1 { "" } else { "s" }
        );
    }
    if execution_prefetch_count > 0 {
        rbtc_info!(
            "execution-overlapped prefetch retained {execution_prefetch_count} fully received blocks for the next batch"
        );
    }
    for migration in delta_shard_migrations {
        rbtc_info!(
            "migrated hot validation delta {} from {} bytes to {} sorted shards inside a {} ms migration window",
            migration.height,
            migration.legacy_bytes,
            migration.shard_count,
            delta_shard_elapsed.as_millis()
        );
    }
    if let Some(error) = prefetch_error {
        return Err(error);
    }
    Ok(())
}

fn validate_live_indexes_before_prune(
    explorer: Option<&RedbExplorerIndex>,
    wallet: Option<&EmbeddedWallet>,
    auxiliary_indexes: &AuxiliaryIndexes,
    execution_height: u32,
) -> Result<(), PeerRunError> {
    if let Some(explorer) = explorer {
        let tip = explorer.tip().map_err(|error| error.to_string())?;
        validate_prune_boundary(
            IndexBuildState {
                kind: IndexKind::Explorer,
                best_height: tip.height,
                required_from_height: 1,
                utxo_baseline_height: explorer
                    .baseline()
                    .map_err(|error| error.to_string())?
                    .map(|base| base.height),
            },
            execution_height,
            execution_height,
        )
        .map_err(|error| {
            PeerRunError::local(format!(
                "refusing freezer publication past explorer height {}: {error:?}",
                tip.height
            ))
        })?;
    }
    if let Some(wallet) = wallet {
        let tip = wallet.chain_tip().map_err(|error| error.to_string())?;
        validate_prune_boundary(
            IndexBuildState {
                kind: IndexKind::Wallet,
                best_height: tip.height,
                required_from_height: wallet
                    .scan_start_height()
                    .map_err(|error| error.to_string())?
                    .unwrap_or(0),
                utxo_baseline_height: None,
            },
            execution_height,
            execution_height,
        )
        .map_err(|error| {
            PeerRunError::local(format!(
                "refusing freezer publication past wallet height {}: {error:?}",
                tip.height
            ))
        })?;
    }
    for (kind, index) in auxiliary_indexes.enabled() {
        let tip = index.tip().map_err(|error| error.to_string())?;
        validate_prune_boundary(
            IndexBuildState {
                kind: auxiliary_policy_kind(kind),
                best_height: tip.height,
                required_from_height: 1,
                utxo_baseline_height: None,
            },
            execution_height,
            execution_height,
        )
        .map_err(|error| {
            PeerRunError::local(format!(
                "refusing freezer publication past {} index height {}: {error:?}",
                auxiliary_index_label(kind),
                tip.height
            ))
        })?;
    }
    Ok(())
}

fn validate_downloaded_block(
    deployment_config: &DeploymentConfig,
    headers: &HeaderDag,
    height: u32,
    expected_hash: BlockHash,
    block: &Block,
) -> Result<
    (
        BlockDeploymentContext,
        ValidatedBlockTransactionIds,
        Vec<u8>,
    ),
    PeerRunError,
> {
    let actual = block.block_hash();
    if actual != expected_hash {
        return Err(PeerRunError::protocol(format!(
            "downloaded block {actual} does not match active block {expected_hash} at height {height}"
        )));
    }
    let deployments =
        block_deployment_context_for_headers(deployment_config, headers, height, expected_hash)
            .map_err(|error| PeerRunError::transient(error.to_string()))?;
    let transaction_ids = validate_block_structure_with_deployments_and_txids(
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
    })?;
    Ok((deployments, transaction_ids, serialize(block)))
}

fn validate_downloaded_blocks(
    deployment_config: &DeploymentConfig,
    headers: &HeaderDag,
    expected: &[HeaderInfo],
    blocks: &[Block],
) -> Result<
    Vec<(
        BlockDeploymentContext,
        ValidatedBlockTransactionIds,
        Vec<u8>,
    )>,
    PeerRunError,
> {
    if expected.len() != blocks.len() {
        return Err(PeerRunError::transient(format!(
            "downloaded block count {} does not match expected count {}",
            blocks.len(),
            expected.len()
        )));
    }
    let available_workers = std::thread::available_parallelism().map_or(1, std::num::NonZero::get);
    let workers = available_workers.min(
        expected
            .len()
            .div_ceil(MIN_PARALLEL_STRUCTURE_BLOCKS_PER_WORKER),
    );
    if workers <= 1 {
        return expected
            .iter()
            .zip(blocks)
            .map(|(expected, block)| {
                validate_downloaded_block(
                    deployment_config,
                    headers,
                    expected.height,
                    expected.hash,
                    block,
                )
            })
            .collect();
    }

    let chunk_size = expected.len().div_ceil(workers);
    std::thread::scope(|scope| {
        let jobs = expected
            .chunks(chunk_size)
            .zip(blocks.chunks(chunk_size))
            .map(|(expected, blocks)| {
                scope.spawn(move || {
                    expected
                        .iter()
                        .zip(blocks)
                        .map(|(expected, block)| {
                            validate_downloaded_block(
                                deployment_config,
                                headers,
                                expected.height,
                                expected.hash,
                                block,
                            )
                        })
                        .collect::<Result<Vec<_>, PeerRunError>>()
                })
            })
            .collect::<Vec<_>>();
        let mut validated = Vec::with_capacity(expected.len());
        for job in jobs {
            validated.extend(
                job.join()
                    .expect("block-structure validation worker must not panic")?,
            );
        }
        Ok(validated)
    })
}

fn index_validated_blocks(
    explorer: Option<&RedbExplorerIndex>,
    explorer_events: Option<&ExplorerEventHub>,
    wallet: Option<&EmbeddedWallet>,
    auxiliary_indexes: &AuxiliaryIndexes,
    expected: &[HeaderInfo],
    blocks: &[Block],
    applied_blocks: &[AppliedBlock],
) -> Result<(), String> {
    if expected.len() != blocks.len() || blocks.len() != applied_blocks.len() {
        return Err("validated projection batch lengths differ".to_owned());
    }
    let explorer_batch = expected
        .iter()
        .zip(blocks)
        .zip(applied_blocks)
        .map(|((expected, block), applied)| (expected.height, block, applied))
        .collect::<Vec<_>>();
    let auxiliary_batch = expected
        .iter()
        .zip(blocks)
        .zip(applied_blocks)
        .map(|((expected, block), applied)| {
            (expected.height, block, applied.transaction_undos.as_slice())
        })
        .collect::<Vec<_>>();
    std::thread::scope(|scope| {
        let explorer_job = explorer.map(|explorer| {
            scope.spawn(|| {
                explorer
                    .connect_batch(&explorer_batch)
                    .map_err(|error| error.to_string())
            })
        });
        let auxiliary_jobs = auxiliary_indexes
            .enabled()
            .map(|(_, index)| {
                scope.spawn(|| {
                    index
                        .connect_batch(&auxiliary_batch)
                        .map_err(|error| error.to_string())
                })
            })
            .collect::<Vec<_>>();
        let wallet_result = if let Some(wallet) = wallet {
            expected
                .iter()
                .zip(blocks)
                .try_for_each(|(expected, block)| {
                    wallet
                        .apply_validated_block(block, expected.height)
                        .map_err(|error| error.to_string())
                })
        } else {
            Ok(())
        };
        if let Some(job) = explorer_job {
            job.join().expect("explorer index worker must not panic")?;
        }
        for job in auxiliary_jobs {
            job.join().expect("optional index worker must not panic")?;
        }
        wallet_result
    })?;
    for (expected, applied) in expected.iter().zip(applied_blocks) {
        publish_explorer_event(
            explorer_events,
            ExplorerEventKind::Connected,
            rbtc::execution_store::ExecutionTip {
                height: expected.height,
                hash: applied.hash,
            },
        );
    }
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

fn elapsed_ms(started: Instant) -> u32 {
    u32::try_from(started.elapsed().as_millis()).unwrap_or(u32::MAX)
}

fn selected_dns_seeds(options: &Options) -> Vec<DnsSeed> {
    options.dns_seeds.clone().unwrap_or_else(|| {
        pinned_dns_seed_hosts(options.network)
            .iter()
            .map(|host| DnsSeed {
                host: (*host).to_owned(),
                port: default_p2p_port(options.network),
            })
            .collect()
    })
}

const fn pinned_dns_seed_hosts(network: Network) -> &'static [&'static str] {
    match network {
        Network::Bitcoin => &[
            "seed.bitcoin.sipa.be.",
            "dnsseed.bluematt.me.",
            "seed.bitcoin.jonasschnelli.ch.",
            "seed.btc.petertodd.net.",
            "seed.bitcoin.sprovoost.nl.",
            "dnsseed.emzy.de.",
            "seed.bitcoin.wiz.biz.",
            "seed.mainnet.achownodes.xyz.",
        ],
        Network::Testnet => &[
            "testnet-seed.bitcoin.jonasschnelli.ch.",
            "seed.tbtc.petertodd.net.",
            "seed.testnet.bitcoin.sprovoost.nl.",
            "testnet-seed.bluematt.me.",
            "seed.testnet.achownodes.xyz.",
        ],
        Network::Testnet4 => &[
            "seed.testnet4.bitcoin.sprovoost.nl.",
            "seed.testnet4.wiz.biz.",
        ],
        Network::Signet => &[
            "seed.signet.bitcoin.sprovoost.nl.",
            "seed.signet.achownodes.xyz.",
        ],
        // Regtest intentionally has no public bootstrap source.
        Network::Regtest => &[],
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
    let arguments = config_file::merge_config_arguments(args.collect())?;
    parse_merged_options(arguments.into_iter())
}

#[allow(clippy::too_many_lines)]
fn parse_merged_options(args: impl Iterator<Item = String>) -> Result<Option<Options>, String> {
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
    let mut experimental_network_execution = false;
    let mut extend_validation_target = false;
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
    let mut core_snapshot_path = None;
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
    let mut mempool_full_rbf = false;
    let mut transaction_index = false;
    let mut spent_output_index = false;
    let mut basic_filter_index = false;
    let mut inbound_listen = None;
    let mut no_inbound_listen = false;
    let mut max_inbound_peers = None;
    let mut max_inbound_peers_per_ip = None;
    let mut max_upload_bytes_per_day = None;
    let mut inbound_requests_per_minute = None;
    let mut cleanup_validation_dir = false;
    let mut utxo_activity_report = false;
    let mut utxo_retier_window_blocks = None;
    let mut verify_storage = false;
    let mut storage_audit_max_segments = None;
    let mut storage_audit_max_bytes = None;
    let mut verify_chain = false;
    let mut verify_chain_depth = None;
    let mut verify_chain_block_bytes = None;
    let mut reindex_from_freezer = None;
    let mut reindex_chainstate = None;
    let mut manual_prune_through_height = None;
    let mut manual_prune_token = None;
    let mut snapshot_download_source = None;
    let mut snapshot_download_output = None;
    let mut snapshot_download_bytes = None;
    let mut snapshot_download_workers = None;
    let mut validation_batch_size = None;
    let mut validation_pause_ms = None;
    let mut validation_deferred_repair = false;
    let mut prune_blocks = None;
    let mut prune_max_bytes = None;
    let mut minimum_free_bytes = None;
    let mut active_chainstate_cache_bytes = None;
    let mut background_chainstate_cache_bytes = None;
    let mut bulk_validation_cache_bytes = None;
    let mut automatic_hot_standbys = None;
    let mut mempool_max_transactions = None;
    let mut mempool_max_bytes = None;
    let mut log_level = None;
    let mut log_dir = None;
    let mut log_max_bytes = None;
    let mut log_max_files = None;
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
            "--dns-seeds" => no_dns_seeds = false,
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
            "--no-once" => once = false,
            "--experimental-network-execution" => experimental_network_execution = true,
            "--extend-validation-target" => extend_validation_target = true,
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
            "--core-assumeutxo-snapshot" => {
                if core_snapshot_path.is_some() {
                    return Err(
                        "--core-assumeutxo-snapshot cannot be supplied more than once".to_owned(),
                    );
                }
                core_snapshot_path = Some(PathBuf::from(required_option_value(
                    &mut args,
                    "--core-assumeutxo-snapshot",
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
            "--mempool-full-rbf" => mempool_full_rbf = true,
            "--no-mempool-full-rbf" => mempool_full_rbf = false,
            "--txindex" => transaction_index = true,
            "--no-txindex" => transaction_index = false,
            "--spent-output-index" => spent_output_index = true,
            "--no-spent-output-index" => spent_output_index = false,
            "--block-filter-index" => basic_filter_index = true,
            "--no-block-filter-index" => basic_filter_index = false,
            "--listen" => {
                if inbound_listen.is_some() {
                    return Err("--listen cannot be supplied more than once".to_owned());
                }
                let value = required_option_value(&mut args, "--listen")?;
                inbound_listen = Some(
                    value
                        .parse::<SocketAddr>()
                        .map_err(|_| format!("invalid inbound listen address: {value}"))?,
                );
                no_inbound_listen = false;
            }
            "--no-listen" => {
                inbound_listen = None;
                no_inbound_listen = true;
            }
            "--max-inbound-peers" => {
                if max_inbound_peers.is_some() {
                    return Err("--max-inbound-peers cannot be supplied more than once".to_owned());
                }
                let value = required_option_value(&mut args, "--max-inbound-peers")?;
                max_inbound_peers = Some(
                    value
                        .parse::<usize>()
                        .map_err(|_| format!("invalid maximum inbound peer count: {value}"))?,
                );
            }
            "--max-inbound-peers-per-ip" => {
                if max_inbound_peers_per_ip.is_some() {
                    return Err(
                        "--max-inbound-peers-per-ip cannot be supplied more than once".to_owned(),
                    );
                }
                let value = required_option_value(&mut args, "--max-inbound-peers-per-ip")?;
                max_inbound_peers_per_ip = Some(
                    value
                        .parse::<usize>()
                        .map_err(|_| format!("invalid maximum inbound peers per IP: {value}"))?,
                );
            }
            "--max-upload-bytes-per-day" => {
                if max_upload_bytes_per_day.is_some() {
                    return Err(
                        "--max-upload-bytes-per-day cannot be supplied more than once".to_owned(),
                    );
                }
                let value = required_option_value(&mut args, "--max-upload-bytes-per-day")?;
                max_upload_bytes_per_day = Some(
                    value
                        .parse::<u64>()
                        .map_err(|_| format!("invalid daily upload target: {value}"))?,
                );
            }
            "--inbound-requests-per-minute" => {
                if inbound_requests_per_minute.is_some() {
                    return Err(
                        "--inbound-requests-per-minute cannot be supplied more than once"
                            .to_owned(),
                    );
                }
                let value = required_option_value(&mut args, "--inbound-requests-per-minute")?;
                inbound_requests_per_minute =
                    Some(value.parse::<u32>().map_err(|_| {
                        format!("invalid inbound requests-per-minute limit: {value}")
                    })?);
            }
            "--cleanup-validation-dir" => cleanup_validation_dir = true,
            "--no-cleanup-validation-dir" => cleanup_validation_dir = false,
            "--utxo-activity-report" => utxo_activity_report = true,
            "--verify-storage" => verify_storage = true,
            "--verify-chain" => verify_chain = true,
            "--verify-chain-depth" => {
                if verify_chain_depth.is_some() {
                    return Err("--verify-chain-depth cannot be supplied more than once".to_owned());
                }
                let value = required_option_value(&mut args, "--verify-chain-depth")?;
                let depth = value
                    .parse::<u32>()
                    .map_err(|_| format!("invalid chain verification depth: {value}"))?;
                if !(1..=MAX_VERIFY_CHAIN_DEPTH).contains(&depth) {
                    return Err(format!(
                        "chain verification depth must be between 1 and {MAX_VERIFY_CHAIN_DEPTH}"
                    ));
                }
                verify_chain_depth = Some(depth);
            }
            "--verify-chain-max-block-bytes" => {
                if verify_chain_block_bytes.is_some() {
                    return Err(
                        "--verify-chain-max-block-bytes cannot be supplied more than once"
                            .to_owned(),
                    );
                }
                let value = required_option_value(&mut args, "--verify-chain-max-block-bytes")?;
                let bytes = value.parse::<u64>().map_err(|_| {
                    format!("invalid chain verification block-byte budget: {value}")
                })?;
                if !(MIN_VERIFY_CHAIN_BLOCK_BYTES..=MAX_VERIFY_CHAIN_BLOCK_BYTES).contains(&bytes) {
                    return Err(format!(
                        "chain verification block-byte budget must be between {MIN_VERIFY_CHAIN_BLOCK_BYTES} and {MAX_VERIFY_CHAIN_BLOCK_BYTES}"
                    ));
                }
                verify_chain_block_bytes = Some(bytes);
            }
            "--reindex-from-freezer" => {
                if reindex_from_freezer.is_some() {
                    return Err(
                        "--reindex-from-freezer cannot be supplied more than once".to_owned()
                    );
                }
                reindex_from_freezer = Some(PathBuf::from(required_option_value(
                    &mut args,
                    "--reindex-from-freezer",
                )?));
            }
            "--reindex-chainstate" => {
                if reindex_chainstate.is_some() {
                    return Err("--reindex-chainstate cannot be supplied more than once".to_owned());
                }
                reindex_chainstate = Some(PathBuf::from(required_option_value(
                    &mut args,
                    "--reindex-chainstate",
                )?));
            }
            "--verify-storage-max-segments" => {
                if storage_audit_max_segments.is_some() {
                    return Err(
                        "--verify-storage-max-segments cannot be supplied more than once"
                            .to_owned(),
                    );
                }
                let value = required_option_value(&mut args, "--verify-storage-max-segments")?;
                let segments = value
                    .parse::<u32>()
                    .map_err(|_| format!("invalid storage audit segment budget: {value}"))?;
                if !(1..=MAX_STORAGE_AUDIT_SEGMENTS).contains(&segments) {
                    return Err(format!(
                        "storage audit segment budget must be between 1 and {MAX_STORAGE_AUDIT_SEGMENTS}"
                    ));
                }
                storage_audit_max_segments = Some(segments);
            }
            "--verify-storage-max-bytes" => {
                if storage_audit_max_bytes.is_some() {
                    return Err(
                        "--verify-storage-max-bytes cannot be supplied more than once".to_owned(),
                    );
                }
                let value = required_option_value(&mut args, "--verify-storage-max-bytes")?;
                let bytes = value
                    .parse::<u64>()
                    .map_err(|_| format!("invalid storage audit byte budget: {value}"))?;
                if !(MIN_STORAGE_AUDIT_BYTES..=MAX_STORAGE_AUDIT_BYTES).contains(&bytes) {
                    return Err(format!(
                        "storage audit byte budget must be between {MIN_STORAGE_AUDIT_BYTES} and {MAX_STORAGE_AUDIT_BYTES}"
                    ));
                }
                storage_audit_max_bytes = Some(bytes);
            }
            "--prune-through-height" => {
                if manual_prune_through_height.is_some() {
                    return Err(
                        "--prune-through-height cannot be supplied more than once".to_owned()
                    );
                }
                let value = required_option_value(&mut args, "--prune-through-height")?;
                manual_prune_through_height = Some(
                    value
                        .parse::<u32>()
                        .map_err(|_| format!("invalid manual prune height: {value}"))?,
                );
            }
            "--apply-prune-token" => {
                if manual_prune_token.is_some() {
                    return Err("--apply-prune-token cannot be supplied more than once".to_owned());
                }
                let value = required_option_value(&mut args, "--apply-prune-token")?;
                if value.len() != 64 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
                    return Err(
                        "manual prune plan token must be exactly 64 hexadecimal characters"
                            .to_owned(),
                    );
                }
                manual_prune_token = Some(value.to_ascii_lowercase());
            }
            "--retier-utxos-window-blocks" => {
                if utxo_retier_window_blocks.is_some() {
                    return Err(
                        "--retier-utxos-window-blocks cannot be supplied more than once".to_owned(),
                    );
                }
                let value = required_option_value(&mut args, "--retier-utxos-window-blocks")?;
                utxo_retier_window_blocks = Some(
                    value
                        .parse::<u32>()
                        .map_err(|_| format!("invalid UTXO hot window in blocks: {value}"))?,
                );
            }
            "--download-core-assumeutxo" => {
                if snapshot_download_source.is_some() {
                    return Err(
                        "--download-core-assumeutxo cannot be supplied more than once".to_owned(),
                    );
                }
                snapshot_download_source = Some(required_option_value(
                    &mut args,
                    "--download-core-assumeutxo",
                )?);
            }
            "--snapshot-download-output" => {
                if snapshot_download_output.is_some() {
                    return Err(
                        "--snapshot-download-output cannot be supplied more than once".to_owned(),
                    );
                }
                snapshot_download_output = Some(PathBuf::from(required_option_value(
                    &mut args,
                    "--snapshot-download-output",
                )?));
            }
            "--snapshot-download-bytes" => {
                if snapshot_download_bytes.is_some() {
                    return Err(
                        "--snapshot-download-bytes cannot be supplied more than once".to_owned(),
                    );
                }
                let value = required_option_value(&mut args, "--snapshot-download-bytes")?;
                snapshot_download_bytes = Some(
                    value
                        .parse::<u64>()
                        .map_err(|_| format!("invalid snapshot download length: {value}"))?,
                );
            }
            "--snapshot-download-workers" => {
                if snapshot_download_workers.is_some() {
                    return Err(
                        "--snapshot-download-workers cannot be supplied more than once".to_owned(),
                    );
                }
                let value = required_option_value(&mut args, "--snapshot-download-workers")?;
                snapshot_download_workers = Some(
                    value
                        .parse::<usize>()
                        .map_err(|_| format!("invalid snapshot download worker count: {value}"))?,
                );
                if !matches!(
                    snapshot_download_workers,
                    Some(1..=MAX_SNAPSHOT_DOWNLOAD_WORKERS)
                ) {
                    return Err(format!(
                        "snapshot download worker count must be between 1 and {MAX_SNAPSHOT_DOWNLOAD_WORKERS}"
                    ));
                }
            }
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
                if !(1..=MAX_VALIDATION_BATCH_SIZE).contains(&size) {
                    return Err(format!(
                        "validation batch size must be between 1 and {MAX_VALIDATION_BATCH_SIZE}"
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
            "--validation-deferred-repair" => validation_deferred_repair = true,
            "--validation-quick-repair" => validation_deferred_repair = false,
            "--automatic-hot-standbys" => {
                if automatic_hot_standbys.is_some() {
                    return Err(
                        "--automatic-hot-standbys cannot be supplied more than once".to_owned()
                    );
                }
                let value = required_option_value(&mut args, "--automatic-hot-standbys")?;
                automatic_hot_standbys = Some(
                    value
                        .parse::<usize>()
                        .map_err(|_| format!("invalid automatic hot-standby limit: {value}"))?,
                );
            }
            "--mempool-max-transactions" => {
                if mempool_max_transactions.is_some() {
                    return Err(
                        "--mempool-max-transactions cannot be supplied more than once".to_owned(),
                    );
                }
                let value = required_option_value(&mut args, "--mempool-max-transactions")?;
                mempool_max_transactions = Some(
                    value
                        .parse::<usize>()
                        .map_err(|_| format!("invalid mempool transaction target: {value}"))?,
                );
            }
            "--mempool-max-bytes" => {
                if mempool_max_bytes.is_some() {
                    return Err("--mempool-max-bytes cannot be supplied more than once".to_owned());
                }
                let value = required_option_value(&mut args, "--mempool-max-bytes")?;
                mempool_max_bytes = Some(
                    value
                        .parse::<usize>()
                        .map_err(|_| format!("invalid mempool byte target: {value}"))?,
                );
            }
            "--log-level" => {
                if log_level.is_some() {
                    return Err("--log-level cannot be supplied more than once".to_owned());
                }
                let value = required_option_value(&mut args, "--log-level")?;
                log_level = Some(
                    LogLevel::parse(&value).ok_or_else(|| format!("invalid log level: {value}"))?,
                );
            }
            "--log-dir" => {
                if log_dir.is_some() {
                    return Err("--log-dir cannot be supplied more than once".to_owned());
                }
                log_dir = Some(PathBuf::from(required_option_value(
                    &mut args,
                    "--log-dir",
                )?));
            }
            "--log-max-bytes" => {
                if log_max_bytes.is_some() {
                    return Err("--log-max-bytes cannot be supplied more than once".to_owned());
                }
                let value = required_option_value(&mut args, "--log-max-bytes")?;
                log_max_bytes = Some(
                    value
                        .parse::<u64>()
                        .map_err(|_| format!("invalid log file size: {value}"))?,
                );
            }
            "--log-max-files" => {
                if log_max_files.is_some() {
                    return Err("--log-max-files cannot be supplied more than once".to_owned());
                }
                let value = required_option_value(&mut args, "--log-max-files")?;
                log_max_files = Some(
                    value
                        .parse::<u8>()
                        .map_err(|_| format!("invalid log file count: {value}"))?,
                );
            }
            "--chainstate-cache-bytes" => {
                if active_chainstate_cache_bytes.is_some() {
                    return Err(
                        "--chainstate-cache-bytes cannot be supplied more than once".to_owned()
                    );
                }
                active_chainstate_cache_bytes = Some(parse_cache_bytes(
                    &required_option_value(&mut args, "--chainstate-cache-bytes")?,
                    "active chainstate",
                )?);
            }
            "--background-chainstate-cache-bytes" => {
                if background_chainstate_cache_bytes.is_some() {
                    return Err(
                        "--background-chainstate-cache-bytes cannot be supplied more than once"
                            .to_owned(),
                    );
                }
                background_chainstate_cache_bytes = Some(parse_cache_bytes(
                    &required_option_value(&mut args, "--background-chainstate-cache-bytes")?,
                    "background chainstate",
                )?);
            }
            "--bulk-validation-cache-bytes" => {
                if bulk_validation_cache_bytes.is_some() {
                    return Err(
                        "--bulk-validation-cache-bytes cannot be supplied more than once"
                            .to_owned(),
                    );
                }
                bulk_validation_cache_bytes = Some(parse_cache_bytes(
                    &required_option_value(&mut args, "--bulk-validation-cache-bytes")?,
                    "bulk validation",
                )?);
            }
            "--prune-blocks" => {
                if prune_blocks.is_some() {
                    return Err("--prune-blocks cannot be supplied more than once".to_owned());
                }
                let value = required_option_value(&mut args, "--prune-blocks")?;
                let blocks = value
                    .parse::<u32>()
                    .map_err(|_| format!("invalid prune block target: {value}"))?;
                if !(MIN_PRUNE_RETENTION_BLOCKS..=DEFAULT_RETENTION_BLOCKS).contains(&blocks) {
                    return Err(format!(
                        "prune block target must be between {MIN_PRUNE_RETENTION_BLOCKS} and {DEFAULT_RETENTION_BLOCKS}"
                    ));
                }
                prune_blocks = Some(blocks);
            }
            "--prune-max-bytes" => {
                if prune_max_bytes.is_some() {
                    return Err("--prune-max-bytes cannot be supplied more than once".to_owned());
                }
                let value = required_option_value(&mut args, "--prune-max-bytes")?;
                let bytes = value
                    .parse::<u64>()
                    .map_err(|_| format!("invalid prune byte target: {value}"))?;
                if bytes < MIN_PRUNE_MAX_BYTES {
                    return Err(format!(
                        "prune byte target must be at least {MIN_PRUNE_MAX_BYTES}"
                    ));
                }
                prune_max_bytes = Some(bytes);
            }
            "--minimum-free-bytes" => {
                if minimum_free_bytes.is_some() {
                    return Err("--minimum-free-bytes cannot be supplied more than once".to_owned());
                }
                let value = required_option_value(&mut args, "--minimum-free-bytes")?;
                minimum_free_bytes = Some(
                    value
                        .parse::<u64>()
                        .map_err(|_| format!("invalid minimum free-space reserve: {value}"))?,
                );
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
            Some(SnapshotActivationOptions::Rbtc {
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
    if core_snapshot_path.is_some() {
        if data_dir.is_none() {
            return Err("--core-assumeutxo-snapshot requires --data-dir".to_owned());
        }
        if snapshot.is_some() {
            return Err(
                "--core-assumeutxo-snapshot conflicts with --assumeutxo-snapshot".to_owned(),
            );
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
                "Core snapshot activation is offline and conflicts with peer, fetch, headers-db, once, explorer, and wallet modes"
                    .to_owned(),
            );
        }
    }
    let snapshot = match (snapshot, core_snapshot_path) {
        (snapshot @ Some(_), None) => snapshot,
        (None, Some(path)) => Some(SnapshotActivationOptions::Core(path)),
        (None, None) => None,
        (Some(_), Some(_)) => unreachable!("snapshot conflict was checked above"),
    };
    let snapshot_activation = snapshot.is_some();
    if finalize_assumeutxo.is_some() {
        if data_dir.is_none() {
            return Err("--finalize-assumeutxo requires --data-dir".to_owned());
        }
        if snapshot_activation
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
            if snapshot_activation
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
        if snapshot_activation
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
        if snapshot_activation
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
    if (validation_batch_size.is_some()
        || validation_pause_ms.is_some()
        || validation_deferred_repair)
        && validation_target.is_none()
        && complete_assumeutxo.is_none()
        && background_assumeutxo.is_none()
        && reindex_from_freezer.is_none()
        && reindex_chainstate.is_none()
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
    let snapshot_download = match (
        snapshot_download_source,
        snapshot_download_output,
        snapshot_download_bytes,
        snapshot_download_workers,
    ) {
        (None, None, None, None) => None,
        (Some(source), Some(output), Some(expected_bytes), workers) => {
            Some(SnapshotDownloadConfig {
                source,
                output,
                expected_bytes,
                workers: workers.unwrap_or(DEFAULT_SNAPSHOT_DOWNLOAD_WORKERS),
                chunk_bytes: DEFAULT_SNAPSHOT_CHUNK_BYTES,
            })
        }
        _ => {
            return Err(
                "--download-core-assumeutxo requires --snapshot-download-output and --snapshot-download-bytes; workers are optional"
                    .to_owned(),
            );
        }
    };
    if !verify_storage
        && (storage_audit_max_segments.is_some() || storage_audit_max_bytes.is_some())
    {
        return Err(
            "--verify-storage-max-segments and --verify-storage-max-bytes require --verify-storage"
                .to_owned(),
        );
    }
    if !verify_chain && (verify_chain_depth.is_some() || verify_chain_block_bytes.is_some()) {
        return Err(
            "--verify-chain-depth and --verify-chain-max-block-bytes require --verify-chain"
                .to_owned(),
        );
    }
    let offline_action_count = usize::from(utxo_activity_report)
        + usize::from(utxo_retier_window_blocks.is_some())
        + usize::from(snapshot_download.is_some())
        + usize::from(verify_storage)
        + usize::from(verify_chain)
        + usize::from(reindex_from_freezer.is_some())
        + usize::from(reindex_chainstate.is_some())
        + usize::from(manual_prune_through_height.is_some());
    if offline_action_count > 1 {
        return Err("offline maintenance actions cannot be combined".to_owned());
    }
    if verify_storage {
        if data_dir.is_none() {
            return Err("--verify-storage requires --data-dir".to_owned());
        }
        if manual_prune_through_height.is_some()
            || manual_prune_token.is_some()
            || utxo_activity_report
            || utxo_retier_window_blocks.is_some()
            || snapshot_download.is_some()
            || snapshot_activation
            || finalize_assumeutxo.is_some()
            || validation_target.is_some()
            || complete_assumeutxo.is_some()
            || background_assumeutxo.is_some()
            || fetch_block.is_some()
            || headers_db.is_some()
            || once
            || explorer_listen.is_some()
            || !remotes.is_empty()
            || !dns_seed_values.is_empty()
            || no_dns_seeds
            || mempool_full_rbf
            || cleanup_validation_dir
            || validation_batch_size.is_some()
            || validation_pause_ms.is_some()
            || validation_deferred_repair
            || experimental_network_execution
            || extend_validation_target
            || prune_blocks.is_some()
            || prune_max_bytes.is_some()
            || minimum_free_bytes.is_some()
            || active_chainstate_cache_bytes.is_some()
            || background_chainstate_cache_bytes.is_some()
            || bulk_validation_cache_bytes.is_some()
            || automatic_hot_standbys.is_some()
            || mempool_max_transactions.is_some()
            || mempool_max_bytes.is_some()
            || log_dir.is_some()
            || log_max_bytes.is_some()
            || log_max_files.is_some()
        {
            return Err(
                "storage verification is read-only and conflicts with peer, synchronization, snapshot, validation, serving, mutation, storage-target, and file-logging modes"
                    .to_owned(),
            );
        }
    }
    if verify_chain {
        if data_dir.is_none() {
            return Err("--verify-chain requires --data-dir".to_owned());
        }
        if snapshot_activation
            || finalize_assumeutxo.is_some()
            || validation_target.is_some()
            || complete_assumeutxo.is_some()
            || background_assumeutxo.is_some()
            || fetch_block.is_some()
            || headers_db.is_some()
            || once
            || explorer_listen.is_some()
            || !remotes.is_empty()
            || !dns_seed_values.is_empty()
            || no_dns_seeds
            || mempool_full_rbf
            || cleanup_validation_dir
            || validation_batch_size.is_some()
            || validation_pause_ms.is_some()
            || validation_deferred_repair
            || experimental_network_execution
            || extend_validation_target
            || prune_blocks.is_some()
            || prune_max_bytes.is_some()
            || minimum_free_bytes.is_some()
            || active_chainstate_cache_bytes.is_some()
            || background_chainstate_cache_bytes.is_some()
            || bulk_validation_cache_bytes.is_some()
            || automatic_hot_standbys.is_some()
            || mempool_max_transactions.is_some()
            || mempool_max_bytes.is_some()
            || log_dir.is_some()
            || log_max_bytes.is_some()
            || log_max_files.is_some()
        {
            return Err(
                "chain verification is exclusively offline and conflicts with peer, synchronization, snapshot, validation, serving, mutation, storage-target, and file-logging modes"
                    .to_owned(),
            );
        }
    }
    if reindex_from_freezer.is_some() {
        if data_dir.is_none() {
            return Err("--reindex-from-freezer requires --data-dir".to_owned());
        }
        if snapshot_activation
            || finalize_assumeutxo.is_some()
            || validation_target.is_some()
            || complete_assumeutxo.is_some()
            || background_assumeutxo.is_some()
            || fetch_block.is_some()
            || headers_db.is_some()
            || once
            || explorer_listen.is_some()
            || !remotes.is_empty()
            || !dns_seed_values.is_empty()
            || no_dns_seeds
            || mempool_full_rbf
            || cleanup_validation_dir
            || experimental_network_execution
            || extend_validation_target
            || active_chainstate_cache_bytes.is_some()
            || background_chainstate_cache_bytes.is_some()
            || automatic_hot_standbys.is_some()
            || mempool_max_transactions.is_some()
            || mempool_max_bytes.is_some()
            || log_dir.is_some()
            || log_max_bytes.is_some()
            || log_max_files.is_some()
        {
            return Err(
                "freezer reindex is exclusively offline and conflicts with peer, synchronization, snapshot, serving, unrelated resource, and file-logging modes"
                    .to_owned(),
            );
        }
    }
    if reindex_chainstate.is_some() {
        if data_dir.is_none() {
            return Err("--reindex-chainstate requires --data-dir".to_owned());
        }
        if snapshot_activation
            || finalize_assumeutxo.is_some()
            || validation_target.is_some()
            || complete_assumeutxo.is_some()
            || background_assumeutxo.is_some()
            || fetch_block.is_some()
            || headers_db.is_some()
            || once
            || explorer_listen.is_some()
            || mempool_full_rbf
            || cleanup_validation_dir
            || experimental_network_execution
            || extend_validation_target
            || active_chainstate_cache_bytes.is_some()
            || background_chainstate_cache_bytes.is_some()
            || automatic_hot_standbys.is_some()
            || mempool_max_transactions.is_some()
            || mempool_max_bytes.is_some()
        {
            return Err(
                "peer chainstate reindex conflicts with snapshot, explicit validation, serving, mempool, and unrelated resource modes"
                    .to_owned(),
            );
        }
        let has_peer_bootstrap = !remotes.is_empty()
            || (!no_dns_seeds
                && (!dns_seed_values.is_empty() || !pinned_dns_seed_hosts(network).is_empty()));
        if !has_peer_bootstrap {
            return Err(
                "--reindex-chainstate requires --connect or an enabled DNS bootstrap source"
                    .to_owned(),
            );
        }
    }
    if manual_prune_token.is_some() && manual_prune_through_height.is_none() {
        return Err("--apply-prune-token requires --prune-through-height".to_owned());
    }
    if manual_prune_through_height.is_some() {
        if data_dir.is_none() {
            return Err("--prune-through-height requires --data-dir".to_owned());
        }
        if verify_storage
            || utxo_activity_report
            || utxo_retier_window_blocks.is_some()
            || snapshot_download.is_some()
            || snapshot_activation
            || finalize_assumeutxo.is_some()
            || validation_target.is_some()
            || complete_assumeutxo.is_some()
            || background_assumeutxo.is_some()
            || fetch_block.is_some()
            || headers_db.is_some()
            || once
            || explorer_listen.is_some()
            || !remotes.is_empty()
            || !dns_seed_values.is_empty()
            || no_dns_seeds
            || mempool_full_rbf
            || cleanup_validation_dir
            || validation_batch_size.is_some()
            || validation_pause_ms.is_some()
            || validation_deferred_repair
            || experimental_network_execution
            || extend_validation_target
            || prune_blocks.is_some()
            || prune_max_bytes.is_some()
            || minimum_free_bytes.is_some()
            || active_chainstate_cache_bytes.is_some()
            || background_chainstate_cache_bytes.is_some()
            || bulk_validation_cache_bytes.is_some()
            || automatic_hot_standbys.is_some()
            || mempool_max_transactions.is_some()
            || mempool_max_bytes.is_some()
            || log_dir.is_some()
            || log_max_bytes.is_some()
            || log_max_files.is_some()
        {
            return Err(
                "manual pruning is offline and conflicts with peer, synchronization, snapshot, validation, serving, other storage, and file-logging modes"
                    .to_owned(),
            );
        }
    }
    if snapshot_download.is_some()
        && (utxo_activity_report
            || utxo_retier_window_blocks.is_some()
            || verify_storage
            || data_dir.is_some()
            || snapshot_activation
            || finalize_assumeutxo.is_some()
            || validation_target.is_some()
            || complete_assumeutxo.is_some()
            || background_assumeutxo.is_some()
            || fetch_block.is_some()
            || headers_db.is_some()
            || once
            || explorer_listen.is_some()
            || !remotes.is_empty()
            || !dns_seed_values.is_empty()
            || no_dns_seeds
            || mempool_full_rbf
            || cleanup_validation_dir
            || validation_batch_size.is_some()
            || validation_pause_ms.is_some()
            || validation_deferred_repair
            || experimental_network_execution
            || extend_validation_target)
    {
        return Err(
            "snapshot download is offline and conflicts with data-dir, peer, synchronization, activation, validation, serving, wallet, and policy modes"
                .to_owned(),
        );
    }
    if utxo_activity_report {
        if data_dir.is_none() {
            return Err("--utxo-activity-report requires --data-dir".to_owned());
        }
        if utxo_retier_window_blocks.is_some()
            || verify_storage
            || snapshot_activation
            || finalize_assumeutxo.is_some()
            || validation_target.is_some()
            || complete_assumeutxo.is_some()
            || background_assumeutxo.is_some()
            || fetch_block.is_some()
            || headers_db.is_some()
            || once
            || explorer_listen.is_some()
            || !remotes.is_empty()
            || !dns_seed_values.is_empty()
            || no_dns_seeds
            || mempool_full_rbf
            || cleanup_validation_dir
            || validation_batch_size.is_some()
            || validation_pause_ms.is_some()
            || validation_deferred_repair
            || experimental_network_execution
            || extend_validation_target
        {
            return Err(
                "UTXO activity reporting is offline and conflicts with peer, synchronization, snapshot, validation, serving, wallet, and policy modes"
                    .to_owned(),
            );
        }
    }
    if utxo_retier_window_blocks.is_some() {
        if data_dir.is_none() {
            return Err("--retier-utxos-window-blocks requires --data-dir".to_owned());
        }
        if verify_storage
            || snapshot_activation
            || finalize_assumeutxo.is_some()
            || validation_target.is_some()
            || complete_assumeutxo.is_some()
            || background_assumeutxo.is_some()
            || fetch_block.is_some()
            || headers_db.is_some()
            || once
            || explorer_listen.is_some()
            || !remotes.is_empty()
            || !dns_seed_values.is_empty()
            || no_dns_seeds
            || mempool_full_rbf
            || cleanup_validation_dir
            || validation_batch_size.is_some()
            || validation_pause_ms.is_some()
            || validation_deferred_repair
            || experimental_network_execution
            || extend_validation_target
        {
            return Err(
                "UTXO re-tiering is offline and conflicts with peer, synchronization, snapshot, validation, serving, wallet, and policy modes"
                    .to_owned(),
            );
        }
    }
    if (prune_blocks.is_some() || prune_max_bytes.is_some() || minimum_free_bytes.is_some())
        && data_dir.is_none()
    {
        return Err(
            "--prune-blocks, --prune-max-bytes, and --minimum-free-bytes require --data-dir"
                .to_owned(),
        );
    }
    if (prune_blocks.is_some() || prune_max_bytes.is_some() || minimum_free_bytes.is_some())
        && (snapshot_activation
            || finalize_assumeutxo.is_some()
            || utxo_activity_report
            || utxo_retier_window_blocks.is_some()
            || verify_storage
            || snapshot_download.is_some()
            || fetch_block.is_some()
            || headers_db.is_some())
    {
        return Err(
            "storage targets apply to validating-node execution and conflict with offline, snapshot, fetch, and headers-only modes"
                .to_owned(),
        );
    }
    if mempool_full_rbf && data_dir.is_none() {
        return Err("--mempool-full-rbf requires --data-dir".to_owned());
    }
    let indexes_enabled = transaction_index || spent_output_index || basic_filter_index;
    if indexes_enabled && data_dir.is_none() {
        return Err("optional indexes require --data-dir".to_owned());
    }
    if indexes_enabled
        && (snapshot_activation
            || finalize_assumeutxo.is_some()
            || utxo_activity_report
            || utxo_retier_window_blocks.is_some()
            || verify_storage
            || snapshot_download.is_some()
            || fetch_block.is_some()
            || headers_db.is_some())
    {
        return Err(
            "optional indexes apply to validating-node execution and conflict with offline, snapshot-install, fetch, and headers-only modes"
                .to_owned(),
        );
    }
    let resource_overridden = automatic_hot_standbys.is_some()
        || mempool_max_transactions.is_some()
        || mempool_max_bytes.is_some();
    if resource_overridden && data_dir.is_none() {
        return Err("peer and mempool resource options require --data-dir".to_owned());
    }
    if resource_overridden
        && (snapshot_activation
            || finalize_assumeutxo.is_some()
            || utxo_activity_report
            || utxo_retier_window_blocks.is_some()
            || verify_storage
            || snapshot_download.is_some()
            || fetch_block.is_some()
            || headers_db.is_some())
    {
        return Err(
            "peer and mempool resource options apply to validating-node execution and conflict with offline, snapshot, fetch, and headers-only modes"
                .to_owned(),
        );
    }
    let cache_overridden = active_chainstate_cache_bytes.is_some()
        || background_chainstate_cache_bytes.is_some()
        || bulk_validation_cache_bytes.is_some();
    if cache_overridden && data_dir.is_none() {
        return Err("chainstate cache options require --data-dir".to_owned());
    }
    if cache_overridden
        && (snapshot_activation
            || finalize_assumeutxo.is_some()
            || utxo_activity_report
            || utxo_retier_window_blocks.is_some()
            || verify_storage
            || snapshot_download.is_some()
            || fetch_block.is_some()
            || headers_db.is_some())
    {
        return Err(
            "chainstate cache options apply to validating-node execution and conflict with offline, snapshot, fetch, and headers-only modes"
                .to_owned(),
        );
    }
    let default_cache = NodeCacheConfig::default();
    let cache = NodeCacheConfig {
        active_chainstate_bytes: active_chainstate_cache_bytes
            .unwrap_or(default_cache.active_chainstate_bytes),
        background_chainstate_bytes: background_chainstate_cache_bytes
            .unwrap_or(default_cache.background_chainstate_bytes),
        bulk_validation_bytes: bulk_validation_cache_bytes
            .unwrap_or(default_cache.bulk_validation_bytes),
    };
    let validation_limits = ValidationLimits {
        max_blocks_per_batch: validation_batch_size.unwrap_or(DEFAULT_VALIDATION_BATCH_SIZE),
        pause_between_batches: Duration::from_millis(validation_pause_ms.unwrap_or(0)),
        quick_repair: !validation_deferred_repair,
    };
    let ledger_retention = LedgerRetention {
        max_blocks: prune_blocks.unwrap_or(DEFAULT_RETENTION_BLOCKS),
        max_bytes: prune_max_bytes.unwrap_or(DEFAULT_MAX_BYTES),
        ..LedgerRetention::default()
    };
    let minimum_free_bytes = minimum_free_bytes.unwrap_or(DEFAULT_MINIMUM_FREE_BYTES);
    if experimental_network_execution {
        if data_dir.is_none() {
            return Err("--experimental-network-execution requires --data-dir".to_owned());
        }
        if !supports_experimental_block_execution(network) {
            return Err(
                "--experimental-network-execution supports only bitcoin and legacy testnet"
                    .to_owned(),
            );
        }
        if !once {
            return Err(
                "--experimental-network-execution requires --once so it cannot start an indefinite serving node"
                    .to_owned(),
            );
        }
        if validation_target.is_none() {
            return Err(
                "--experimental-network-execution requires --validate-until-height and --validate-until-blockhash"
                    .to_owned(),
            );
        }
        if explorer_listen.is_some() {
            return Err(
                "--experimental-network-execution cannot expose explorer, RPC, or wallet services"
                    .to_owned(),
            );
        }
        if complete_assumeutxo.is_some()
            || background_assumeutxo.is_some()
            || cleanup_validation_dir
        {
            return Err(
                "--experimental-network-execution cannot run automatic AssumeUTXO validation or cleanup"
                    .to_owned(),
            );
        }
    }
    if extend_validation_target {
        if !experimental_network_execution {
            return Err(
                "--extend-validation-target requires --experimental-network-execution".to_owned(),
            );
        }
        if validation_target.is_none() {
            return Err(
                "--extend-validation-target requires --validate-until-height and --validate-until-blockhash"
                    .to_owned(),
            );
        }
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
        || !pinned_dns_seed_hosts(network).is_empty(),
        |seeds| !seeds.is_empty(),
    );
    if remotes.is_empty() && !has_dns_bootstrap && data_dir.is_none() && snapshot_download.is_none()
    {
        return Err(
            "no peer bootstrap source; provide --connect or --dns-seed (a data directory may reuse verified peers)"
                .to_owned(),
        );
    }
    if inbound_listen.is_none()
        && (max_inbound_peers.is_some()
            || max_inbound_peers_per_ip.is_some()
            || max_upload_bytes_per_day.is_some()
            || inbound_requests_per_minute.is_some())
    {
        return Err("inbound resource limits require --listen ADDRESS".to_owned());
    }
    let default_logging = NodeLogConfig::default();
    let default_inbound = InboundLimits::default();
    let logging_directory =
        log_dir.or_else(|| data_dir.as_ref().map(|directory| directory.join("logs")));
    validate_runtime_options(Options {
        remotes,
        dns_seeds,
        network,
        fetch_block,
        headers_db,
        data_dir,
        once,
        network_execution: if extend_validation_target {
            NetworkExecutionMode::ExperimentalOnceExtend
        } else if experimental_network_execution {
            NetworkExecutionMode::ExperimentalOnce
        } else {
            NetworkExecutionMode::Persistent
        },
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
        mempool_full_rbf,
        cleanup_validation_dir,
        offline_action: if utxo_activity_report {
            Some(OfflineAction::UtxoActivityReport)
        } else if let Some(hot_window_blocks) = utxo_retier_window_blocks {
            Some(OfflineAction::RetierUtxos { hot_window_blocks })
        } else if let Some(config) = snapshot_download {
            Some(OfflineAction::DownloadCoreSnapshot(config))
        } else if verify_storage {
            Some(OfflineAction::VerifyStorage {
                max_segments: storage_audit_max_segments
                    .unwrap_or(DEFAULT_STORAGE_AUDIT_MAX_SEGMENTS),
                max_bytes: storage_audit_max_bytes.unwrap_or(DEFAULT_STORAGE_AUDIT_MAX_BYTES),
            })
        } else if verify_chain {
            Some(OfflineAction::VerifyChain {
                depth: verify_chain_depth.unwrap_or(DEFAULT_VERIFY_CHAIN_DEPTH),
                max_block_bytes: verify_chain_block_bytes
                    .unwrap_or(DEFAULT_VERIFY_CHAIN_BLOCK_BYTES),
            })
        } else if let Some(output) = reindex_from_freezer {
            Some(OfflineAction::ReindexFromFreezer { output })
        } else if let Some(output) = reindex_chainstate {
            Some(OfflineAction::ReindexChainstate { output })
        } else {
            manual_prune_through_height.map(|through_height| OfflineAction::PruneStorage {
                through_height,
                plan_token: manual_prune_token,
            })
        },
        validation_limits,
        ledger_retention,
        minimum_free_bytes,
        cache,
        indexes: NodeIndexConfig {
            transaction: transaction_index,
            spent_output: spent_output_index,
            basic_filter: basic_filter_index,
        },
        inbound_listen: (!no_inbound_listen).then_some(inbound_listen).flatten(),
        inbound_limits: InboundLimits {
            max_connections: max_inbound_peers.unwrap_or(default_inbound.max_connections),
            max_connections_per_ip: max_inbound_peers_per_ip
                .unwrap_or(default_inbound.max_connections_per_ip),
            max_upload_bytes_per_day: max_upload_bytes_per_day
                .unwrap_or(default_inbound.max_upload_bytes_per_day),
            max_requests_per_minute: inbound_requests_per_minute
                .unwrap_or(default_inbound.max_requests_per_minute),
            idle_timeout: default_inbound.idle_timeout,
        },
        resources: NodeResourceConfig {
            automatic_hot_standbys: automatic_hot_standbys
                .unwrap_or(DEFAULT_AUTOMATIC_HOT_STANDBYS),
            mempool_max_transactions: mempool_max_transactions.unwrap_or(MAX_ADMITTED_TRANSACTIONS),
            mempool_max_bytes: mempool_max_bytes.unwrap_or(MAX_ADMITTED_TRANSACTION_BYTES),
        },
        logging: NodeLogConfig {
            level: log_level.unwrap_or(default_logging.level),
            directory: logging_directory,
            max_file_bytes: log_max_bytes.unwrap_or(default_logging.max_file_bytes),
            max_files: log_max_files.unwrap_or(default_logging.max_files),
        },
        runtime_control: Arc::new(RuntimeControl::default()),
        observer: None,
    })
    .map(Some)
}

fn required_option_value(
    args: &mut impl Iterator<Item = String>,
    option: &str,
) -> Result<String, String> {
    args.next()
        .ok_or_else(|| format!("{option} requires a value"))
}

fn parse_cache_bytes(value: &str, name: &str) -> Result<usize, String> {
    let bytes = value
        .parse::<usize>()
        .map_err(|_| format!("invalid {name} cache size: {value}"))?;
    if !(MIN_CHAINSTATE_CACHE_BYTES..=MAX_CHAINSTATE_CACHE_BYTES).contains(&bytes) {
        return Err(format!(
            "{name} cache must be between {MIN_CHAINSTATE_CACHE_BYTES} and {MAX_CHAINSTATE_CACHE_BYTES} bytes"
        ));
    }
    Ok(bytes)
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
        concat!(
            "rbtcd {}\n\nUSAGE:\n",
            "  rbtcd --config PATH [COMMAND-LINE OVERRIDES]\n",
            "  rbtcd [--connect HOST:PORT ...] [--dns-seed HOST[:PORT] ... | --no-dns-seeds] [--network bitcoin|testnet|testnet4|signet|regtest]\n",
            "  rbtcd [PEER OPTIONS] --headers-db PATH [--network NETWORK] [--minimum-chainwork HEX] [--assumevalid HASH|0]\n",
            "  rbtcd [PEER OPTIONS] --data-dir PATH --network bitcoin|testnet|testnet4|signet|regtest [--txindex] [--spent-output-index] [--block-filter-index] [--listen IP:PORT [--max-inbound-peers 1..256] [--max-inbound-peers-per-ip N] [--max-upload-bytes-per-day BYTES] [--inbound-requests-per-minute 60..100000]] [--automatic-hot-standbys 0..16] [--mempool-max-transactions 1..300000] [--mempool-max-bytes 4000000..1073741824] [--prune-blocks 288..1008] [--prune-max-bytes BYTES] [--minimum-free-bytes 536870912..1099511627776] [--chainstate-cache-bytes BYTES] [--background-chainstate-cache-bytes BYTES] [--bulk-validation-cache-bytes BYTES] [--log-level error|warn|info|debug] [--log-dir PATH] [--log-max-bytes 1048576..1073741824] [--log-max-files 2..20] [--mempool-full-rbf] [--once] [--explorer-listen 127.0.0.1:3000 [--rpc-auth-token-file PATH] [--wallet-descriptors PATH --wallet-auth-token-file PATH]] [--vbparams taproot:START:END[:MIN_HEIGHT]] [--testactivationheight NAME@HEIGHT] [--signetchallenge HEX] [--signetseednode HOST[:PORT] ...] [--minimum-chainwork HEX] [--assumevalid HASH|0]\n",
            "  rbtcd [PEER OPTIONS] --data-dir PATH --network bitcoin|testnet --experimental-network-execution --once [--extend-validation-target] --validate-until-height HEIGHT --validate-until-blockhash HASH [--validation-deferred-repair]\n",
            "  rbtcd [PEER OPTIONS] --data-dir ACTIVE --network bitcoin|testnet|testnet4|signet|regtest --background-assumeutxo VALIDATION_DATA_DIR [--validation-batch-size N] [--validation-pause-ms MS] [--cleanup-validation-dir] [--once] [EXPLORER/RPC/WALLET OPTIONS]\n",
            "  rbtcd [PEER OPTIONS] --data-dir ACTIVE --network bitcoin|testnet|testnet4|signet|regtest --complete-assumeutxo VALIDATION_DATA_DIR [--validation-batch-size N] [--validation-pause-ms MS] [--cleanup-validation-dir]\n",
            "  rbtcd [PEER OPTIONS] --data-dir PATH --network bitcoin|testnet|testnet4|signet|regtest --validate-until-height HEIGHT --validate-until-blockhash HASH [--validation-batch-size N] [--validation-pause-ms MS]\n",
            "  rbtcd --data-dir PATH --network bitcoin|testnet|testnet4|signet|regtest --assumeutxo-snapshot FILE --snapshot-height HEIGHT --snapshot-blockhash HASH --snapshot-utxo-count COUNT --snapshot-records-bytes BYTES --snapshot-records-sha256 HEX\n",
            "  rbtcd --data-dir PATH --network bitcoin|testnet|testnet4|signet --core-assumeutxo-snapshot CORE_DUMPTXOUTSET_FILE\n",
            "  rbtcd --data-dir PATH --network bitcoin|testnet|testnet4|signet|regtest --finalize-assumeutxo VALIDATION_DATA_DIR\n",
            "  rbtcd --data-dir PATH --network bitcoin|testnet|testnet4|signet|regtest --utxo-activity-report\n",
            "  rbtcd --data-dir PATH --network bitcoin|testnet|testnet4|signet|regtest --retier-utxos-window-blocks BLOCKS\n",
            "  rbtcd --data-dir PATH --network bitcoin|testnet|testnet4|signet|regtest --verify-storage [--verify-storage-max-segments 1..4096] [--verify-storage-max-bytes 1048576..17179869184]\n",
            "  rbtcd --data-dir PATH --network bitcoin|testnet|testnet4|signet|regtest --verify-chain [--verify-chain-depth 1..1008] [--verify-chain-max-block-bytes 1048576..4294967296]\n",
            "  rbtcd --data-dir SOURCE --network bitcoin|testnet|testnet4|signet|regtest --reindex-from-freezer OUTPUT [--txindex] [--spent-output-index] [--block-filter-index] [--validation-batch-size 1..1008] [--validation-pause-ms 0..60000] [--validation-deferred-repair] [--prune-blocks 288..1008] [--prune-max-bytes BYTES] [--minimum-free-bytes BYTES] [--bulk-validation-cache-bytes BYTES]\n",
            "  rbtcd [PEER OPTIONS] --data-dir SOURCE --network bitcoin|testnet|testnet4|signet|regtest --reindex-chainstate OUTPUT [--txindex] [--spent-output-index] [--block-filter-index] [--validation-batch-size 1..1008] [--validation-pause-ms 0..60000] [--validation-deferred-repair] [--prune-blocks 288..1008] [--prune-max-bytes BYTES] [--minimum-free-bytes BYTES] [--bulk-validation-cache-bytes BYTES]\n",
            "  rbtcd --data-dir PATH --network bitcoin|testnet|testnet4|signet|regtest --prune-through-height HEIGHT [--apply-prune-token PLAN_TOKEN]\n",
            "  rbtcd --download-core-assumeutxo HTTPS_URL --snapshot-download-output FILE --snapshot-download-bytes BYTES [--snapshot-download-workers 1..8]\n",
            "  rbtcd [PEER OPTIONS] --fetch-block BLOCK_HASH [--network NETWORK]\n\n",
            "CONFIG:\n",
            "  Strict key=value files are capped at 64 KiB. Global values apply first; [bitcoin], [testnet], [testnet4], [signet], or [regtest] values replace them. Unknown keys and duplicate scalars fail. Explicit CLI option groups replace file values.\n\n",
            "PEER OPTIONS:\n",
            "  Explicit --connect peers run first. If they and persisted verified peers fail, pinned Bitcoin Core 31 DNS seeds are used. Repeat --dns-seed to replace those defaults, or pass --no-dns-seeds. A custom Signet has no default seeds; repeat --signetseednode or --connect."
        ),
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
        bip152::HeaderAndShortIds,
        block::{Header, Version},
        hex::FromHex,
        p2p::{
            Address, ServiceFlags,
            message::NetworkMessage,
            message_compact_blocks::{CmpctBlock, SendCmpct},
            message_network::VersionMessage,
        },
        pow::Target,
        transaction::Version as TransactionVersion,
    };
    use proptest::prelude::*;
    use rbtc::{
        api::ExplorerIndex,
        blockchain::{block_subsidy, block_subsidy_with_interval},
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

    #[test]
    fn block_throughput_uses_completed_transfer_deltas_and_saturates() {
        let before = BlockTransferStats {
            payload_bytes: 1_000,
            response_time: Duration::from_secs(2),
        };
        assert_eq!(block_throughput_bps(before, before), None);
        assert_eq!(
            block_throughput_bps(
                before,
                BlockTransferStats {
                    payload_bytes: 3_000,
                    response_time: Duration::from_secs(3),
                }
            ),
            Some(2_000)
        );
        assert_eq!(
            block_throughput_bps(
                before,
                BlockTransferStats {
                    payload_bytes: u64::MAX,
                    response_time: before.response_time,
                }
            ),
            Some(u32::MAX)
        );
    }

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
            transaction_index: None,
            spent_output_index: None,
            basic_filter_index: None,
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
            ledger_target_blocks: 1_008,
            ledger_target_bytes: 1024 * 1024 * 1024,
            ledger_pruned_through_height: None,
            disk_space: DiskSpaceSnapshot {
                total_bytes: 10_000,
                available_bytes: 8_000,
                required_bytes: 2_000,
                reserve_bytes: 1_000,
                projected_storage_ceiling_bytes: 3_000,
                optional_index_bytes: 0,
                optional_index_checkpoint_headroom_bytes: 0,
            },
        })
    }

    #[tokio::test]
    async fn operator_rpc_reports_bounded_status_and_requests_graceful_shutdown() {
        let genesis = bitcoin::blockdata::constants::genesis_block(Network::Regtest).block_hash();
        let runtime_control = Arc::new(RuntimeControl::default());
        let transaction_pool = Arc::new(Mutex::new(TransactionAdmissionPool::default()));
        let operator = NodeRpcOperator {
            status: ready_test_node_status(genesis),
            transaction_pool,
            runtime_control: Arc::clone(&runtime_control),
            active_peer: Some("127.0.0.1:18444".parse().unwrap()),
            auxiliary_indexes: Arc::new(AuxiliaryIndexes {
                transaction: None,
                spent_output: None,
                basic_filter: None,
            }),
        };

        let blockchain = operator
            .execute("getblockchaininfo", &serde_json::json!([]))
            .unwrap()
            .unwrap();
        assert_eq!(blockchain["chain"], "regtest");
        assert_eq!(blockchain["blocks"], 0);
        assert_eq!(blockchain["initialblockdownload"], false);
        assert_eq!(
            operator
                .execute("getnetworkinfo", &serde_json::Value::Null)
                .unwrap()
                .unwrap()["connections_out"],
            1
        );
        assert_eq!(
            operator
                .execute("getpeerinfo", &serde_json::json!([]))
                .unwrap()
                .unwrap()[0]["addr"],
            "127.0.0.1:18444"
        );
        assert_eq!(
            operator
                .execute("getmempoolinfo", &serde_json::json!([]))
                .unwrap()
                .unwrap()["size"],
            0
        );
        assert_eq!(
            operator
                .execute("verifychain", &serde_json::json!([]))
                .unwrap()
                .unwrap(),
            serde_json::Value::Bool(true)
        );
        assert_eq!(
            operator
                .execute("getloginfo", &serde_json::json!([]))
                .unwrap()
                .unwrap()["host_managed"],
            true
        );
        assert_eq!(
            operator
                .execute("getdiskinfo", &serde_json::json!([]))
                .unwrap()
                .unwrap()["available_bytes"],
            8_000
        );
        assert_eq!(
            operator
                .execute("setloglevel", &serde_json::json!(["debug"]))
                .unwrap(),
            Err(LocalRpcOperatorError {
                code: -32010,
                message: "Runtime logging is host managed",
            })
        );
        assert_eq!(
            operator
                .execute("getindexinfo", &serde_json::json!([1]))
                .unwrap(),
            Err(LocalRpcOperatorError {
                code: -32602,
                message: "Invalid params",
            })
        );

        let shutdown = runtime_control.shutdown_requested();
        assert_eq!(
            operator
                .execute("stop", &serde_json::json!([]))
                .unwrap()
                .unwrap(),
            "rBTC stopping"
        );
        tokio::time::timeout(Duration::from_secs(1), shutdown)
            .await
            .expect("RPC stop notifies the outer runtime after returning a response");
    }

    #[test]
    fn operator_rpc_serves_enabled_optional_indexes_with_bounded_queries() {
        let directory = TempDir::new().unwrap();
        let txindex = Arc::new(
            RedbAuxiliaryIndex::open(
                directory.path().join("tx.redb"),
                Network::Regtest,
                AuxiliaryIndexKind::Transaction,
            )
            .unwrap(),
        );
        let spent = Arc::new(
            RedbAuxiliaryIndex::open(
                directory.path().join("spent.redb"),
                Network::Regtest,
                AuxiliaryIndexKind::SpentOutput,
            )
            .unwrap(),
        );
        let filters = Arc::new(
            RedbAuxiliaryIndex::open(
                directory.path().join("filters.redb"),
                Network::Regtest,
                AuxiliaryIndexKind::BasicFilter,
            )
            .unwrap(),
        );
        let genesis = bitcoin::blockdata::constants::genesis_block(Network::Regtest);
        let block = regtest_block_at_height(
            genesis.block_hash(),
            genesis.header.time.saturating_add(1),
            1,
        );
        txindex.connect_block(1, &block, &[]).unwrap();
        spent.connect_block(1, &block, &[]).unwrap();
        filters.connect_block(1, &block, &[]).unwrap();
        let operator = NodeRpcOperator {
            status: ready_test_node_status(genesis.block_hash()),
            transaction_pool: Arc::new(Mutex::new(TransactionAdmissionPool::default())),
            runtime_control: Arc::new(RuntimeControl::default()),
            active_peer: None,
            auxiliary_indexes: Arc::new(AuxiliaryIndexes {
                transaction: Some(txindex),
                spent_output: Some(spent),
                basic_filter: Some(filters),
            }),
        };
        let txid = block.txdata[0].compute_txid();
        let location = operator
            .execute("gettxindexlocation", &serde_json::json!([txid.to_string()]))
            .unwrap()
            .unwrap();
        assert_eq!(location["blockheight"], 1);
        let filter = operator
            .execute(
                "getblockfilter",
                &serde_json::json!([block.block_hash().to_string(), "basic"]),
            )
            .unwrap()
            .unwrap();
        assert!(
            filter["filter"]
                .as_str()
                .is_some_and(|value| !value.is_empty())
        );
        assert_eq!(filter["header"].as_str().unwrap().len(), 64);
        let unspent = operator
            .execute(
                "gettxspendingprevout",
                &serde_json::json!([[{"txid": txid.to_string(), "vout": 0}]]),
            )
            .unwrap()
            .unwrap();
        assert!(unspent[0].get("spendingtxid").is_none());
        assert!(
            operator
                .execute("gettxspendingprevout", &serde_json::json!([vec![0; 1_001]]))
                .unwrap()
                .is_err()
        );
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
        assert!(response.contains("rbtc_disk_bytes{kind=\"available\"} 8000"));
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
        receive_client_preferences(peer).await;
        assert!(matches!(
            peer.read_message().await.unwrap().into_payload(),
            NetworkMessage::GetAddr
        ));
        peer.write_message(NetworkMessage::AddrV2(addresses))
            .await
            .unwrap();
    }

    async fn receive_client_preferences(peer: &mut V1Transport<tokio::net::TcpStream>) {
        assert!(matches!(
            peer.read_message().await.unwrap().into_payload(),
            NetworkMessage::SendHeaders
        ));
        let NetworkMessage::SendCmpct(preference) =
            peer.read_message().await.unwrap().into_payload()
        else {
            panic!("expected sendcmpct");
        };
        assert!(!preference.send_compact);
        assert_eq!(preference.version, 2);
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
        peer.write_message(NetworkMessage::WtxidRelay)
            .await
            .unwrap();
        peer.write_message(NetworkMessage::Verack).await.unwrap();
        (peer, local_version)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn shutdown_checkpoint_returns_control_after_synchronous_work() {
        let requested = Arc::new(AtomicBool::new(false));
        let watcher_requested = Arc::clone(&requested);
        let watcher = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            watcher_requested.store(true, Ordering::Release);
        });
        let validating = async {
            loop {
                std::thread::sleep(Duration::from_millis(20));
                checkpoint_shutdown(&requested).await;
            }
        };

        timeout(Duration::from_secs(1), async {
            tokio::select! {
                () = validating => unreachable!("validation loop does not finish itself"),
                result = watcher => result.unwrap(),
            }
        })
        .await
        .expect("independent signal watcher must stop a synchronous validation loop");
    }

    #[tokio::test]
    async fn shutdown_waits_for_in_flight_checkpoint_and_rejects_the_next_one() {
        let requested = AtomicBool::new(false);
        let in_flight = AtomicUsize::new(0);
        let checkpoint = InFlightCheckpoint::try_enter(&requested, &in_flight)
            .expect("checkpoint starts before shutdown");
        assert_eq!(in_flight.load(Ordering::Acquire), 1);

        requested.store(true, Ordering::Release);
        assert!(
            timeout(
                Duration::from_millis(20),
                wait_for_in_flight_checkpoints(&in_flight)
            )
            .await
            .is_err(),
            "shutdown must retain the runtime while a checkpoint is active"
        );

        drop(checkpoint);
        timeout(
            Duration::from_millis(100),
            wait_for_in_flight_checkpoints(&in_flight),
        )
        .await
        .expect("shutdown barrier clears after the checkpoint drops");
        assert!(InFlightCheckpoint::try_enter(&requested, &in_flight).is_none());
        assert_eq!(in_flight.load(Ordering::Acquire), 0);
    }

    #[tokio::test]
    async fn candidate_handshakes_run_concurrently_without_reordering_the_queue() {
        let stalled_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let stalled_remote = stalled_listener.local_addr().unwrap();
        let (release_stalled, stalled_release) = tokio::sync::oneshot::channel();
        let stalled_server = tokio::spawn(async move {
            let (stream, _) = stalled_listener.accept().await.unwrap();
            let mut peer = V1Transport::new(stream, Network::Regtest.magic());
            assert!(matches!(
                peer.read_message().await.unwrap().into_payload(),
                NetworkMessage::Version(_)
            ));
            let _ = stalled_release.await;
        });

        let ready_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let ready_remote = ready_listener.local_addr().unwrap();
        let (ready, ready_received) = tokio::sync::oneshot::channel();
        let ready_server = tokio::spawn(async move {
            let (mut peer, _) = accept_peer(ready_listener, peer_version(101)).await;
            receive_client_preferences(&mut peer).await;
            ready.send(()).unwrap();
        });

        let options = Options {
            remotes: vec![stalled_remote, ready_remote],
            dns_seeds: Some(Vec::new()),
            network: Network::Regtest,
            fetch_block: None,
            headers_db: None,
            data_dir: None,
            once: true,
            network_execution: NetworkExecutionMode::Persistent,
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
            mempool_full_rbf: false,
            cleanup_validation_dir: false,
            offline_action: None,
            validation_limits: ValidationLimits::default(),
            ledger_retention: LedgerRetention::default(),
            minimum_free_bytes: DEFAULT_MINIMUM_FREE_BYTES,
            cache: NodeCacheConfig::default(),
            indexes: NodeIndexConfig::default(),
            inbound_listen: None,
            inbound_limits: InboundLimits::default(),
            resources: NodeResourceConfig::default(),
            logging: NodeLogConfig::default(),
            runtime_control: Arc::new(RuntimeControl::default()),
            observer: None,
        };
        let mut pending =
            spawn_peer_connections(&options, &options.remotes, 100, None, None, None, None)
                .unwrap();
        timeout(Duration::from_secs(2), ready_received)
            .await
            .expect("later candidate should handshake while first candidate is stalled")
            .unwrap();
        assert_eq!(pending.front().unwrap().0, stalled_remote);
        assert_eq!(pending.back().unwrap().0, ready_remote);

        abort_pending_connections(&mut pending).await;
        release_stalled.send(()).unwrap();
        stalled_server.await.unwrap();
        ready_server.await.unwrap();
    }

    #[tokio::test]
    async fn finished_standbys_are_reaped_and_classified_in_queue_order() {
        fn finished_pending(error: PeerRunError) -> PendingPeer {
            let (_ready_sender, ready) = tokio::sync::oneshot::channel();
            let (activate, _activation) = tokio::sync::oneshot::channel();
            let task = tokio::spawn(async move { Err::<ConnectedPeer, _>(error) });
            PendingPeer {
                ready,
                ready_observed: false,
                activate: Some(activate),
                task,
            }
        }

        let protocol_remote = "127.0.0.1:18441".parse().unwrap();
        let manual_remote = "127.0.0.1:18442".parse().unwrap();
        let transient_remote = "127.0.0.1:18443".parse().unwrap();
        let mut pending = VecDeque::from([
            (
                protocol_remote,
                finished_pending(PeerRunError::protocol("invalid standby header")),
            ),
            (
                manual_remote,
                finished_pending(PeerRunError::protocol("invalid manual standby header")),
            ),
            (
                transient_remote,
                finished_pending(PeerRunError::transient("standby timed out")),
            ),
        ]);
        timeout(Duration::from_secs(1), async {
            while pending
                .iter()
                .any(|(_, connection)| !connection.task.is_finished())
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        let directory = TempDir::new().unwrap();
        let store =
            RedbPeerStore::open(directory.path().join("peers.redb"), Network::Regtest).unwrap();
        let manual_remotes = HashSet::from([manual_remote]);
        let mut failures = Vec::new();
        reap_finished_standbys(&mut pending, Some(&store), &manual_remotes, &mut failures).await;

        assert!(pending.is_empty());
        assert_eq!(
            failures,
            vec![
                format!("{protocol_remote}: invalid standby header"),
                format!("{manual_remote}: invalid manual standby header"),
                format!("{transient_remote}: standby timed out"),
            ]
        );
        let now = unix_time().unwrap();
        assert!(store.is_discouraged(protocol_remote, now).unwrap());
        assert!(!store.is_discouraged(manual_remote, now).unwrap());
        assert!(!store.is_discouraged(transient_remote, now).unwrap());
    }

    #[tokio::test]
    async fn automatic_hot_standby_capacity_evicts_the_lowest_priority_ready_peers() {
        fn ready_pending() -> PendingPeer {
            let (ready_sender, ready) = tokio::sync::oneshot::channel();
            ready_sender.send(()).unwrap();
            let (activate, _activation) = tokio::sync::oneshot::channel();
            let task = tokio::spawn(std::future::pending::<Result<ConnectedPeer, PeerRunError>>());
            PendingPeer {
                ready,
                ready_observed: false,
                activate: Some(activate),
                task,
            }
        }

        let automatic = (0..DEFAULT_AUTOMATIC_HOT_STANDBYS + 2)
            .map(|offset| {
                (
                    format!("127.0.0.1:{}", 18_500 + offset).parse().unwrap(),
                    ready_pending(),
                )
            })
            .collect::<Vec<_>>();
        let manual = [
            ("127.0.0.1:18600".parse().unwrap(), ready_pending()),
            ("127.0.0.1:18601".parse().unwrap(), ready_pending()),
        ];
        let manual_remotes = manual.iter().map(|(remote, _)| *remote).collect();
        let mut pending = automatic.into_iter().chain(manual).collect::<VecDeque<_>>();
        let mut failures = Vec::new();

        let evicted = evict_excess_ready_standbys(
            &mut pending,
            None,
            &manual_remotes,
            &mut failures,
            DEFAULT_AUTOMATIC_HOT_STANDBYS,
        )
        .await;

        assert!(failures.is_empty());
        assert_eq!(
            evicted,
            vec![
                "127.0.0.1:18508".parse().unwrap(),
                "127.0.0.1:18509".parse().unwrap(),
            ]
        );
        assert_eq!(pending.len(), DEFAULT_AUTOMATIC_HOT_STANDBYS + 2);
        assert_eq!(
            pending
                .iter()
                .map(|(remote, _)| *remote)
                .collect::<Vec<_>>(),
            (18_500_usize..18_500 + DEFAULT_AUTOMATIC_HOT_STANDBYS)
                .map(|port| format!("127.0.0.1:{port}").parse().unwrap())
                .chain([
                    "127.0.0.1:18600".parse().unwrap(),
                    "127.0.0.1:18601".parse().unwrap(),
                ])
                .collect::<Vec<_>>()
        );

        abort_pending_connections(&mut pending).await;
    }

    #[tokio::test]
    async fn observed_standby_readiness_remains_activatable() {
        let (ready_sender, ready) = tokio::sync::oneshot::channel();
        ready_sender.send(()).unwrap();
        let (activate, activation) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            activation.await.unwrap();
            Err::<ConnectedPeer, _>(PeerRunError::transient("activated after readiness poll"))
        });
        let mut pending = PendingPeer {
            ready,
            ready_observed: false,
            activate: Some(activate),
            task,
        };

        assert!(pending.observe_ready());
        let result = timeout(Duration::from_secs(1), activate_pending_peer(pending))
            .await
            .expect("observed ready standby should still receive activation");
        let error = match result {
            Ok(_) => panic!("synthetic activated standby must return its test error"),
            Err(error) => error,
        };
        assert_eq!(error.to_string(), "activated after readiness poll");
    }

    #[tokio::test]
    async fn standby_session_is_kept_alive_and_can_be_activated() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let remote = listener.local_addr().unwrap();
        let (ping_seen, ping_received) = tokio::sync::oneshot::channel();
        let (release_server, server_release) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut peer, _) = accept_peer(listener, peer_version(202)).await;
            receive_client_preferences(&mut peer).await;
            let NetworkMessage::Ping(nonce) = peer.read_message().await.unwrap().into_payload()
            else {
                panic!("expected standby ping");
            };
            assert_eq!(nonce, 201);
            peer.write_message(NetworkMessage::Pong(nonce))
                .await
                .unwrap();
            ping_seen.send(()).unwrap();
            server_release.await.unwrap();
        });

        let connected = connect_peer(
            DeploymentConfig::for_network(Network::Regtest),
            remote,
            200,
            false,
            None,
        )
        .await
        .unwrap();
        let (activate, activation) = tokio::sync::oneshot::channel();
        let standby = tokio::spawn(maintain_standby(
            connected,
            activation,
            Duration::from_millis(10),
            201,
            None,
            None,
            None,
        ));
        timeout(Duration::from_secs(2), ping_received)
            .await
            .expect("standby should send a bounded keepalive")
            .unwrap();
        activate.send(()).unwrap();
        let activated = standby.await.unwrap().unwrap();
        assert_eq!(activated.remote, remote);

        drop(activated);
        release_server.send(()).unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn full_service_standby_retains_mempool_request_source_after_activation() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let remote = listener.local_addr().unwrap();
        let transaction = wallet_broadcast_transaction();
        let expected_inventory = Inventory::WTx(transaction.compute_wtxid());
        let expected_transaction = transaction.clone();
        let relay = transaction_relay(transaction);
        let source = MempoolRelaySource::new(move || vec![relay.clone()]);
        let server = tokio::spawn(async move {
            let (mut peer, _) = accept_peer(listener, peer_version(212)).await;
            receive_client_preferences(&mut peer).await;
            assert_eq!(
                peer.read_message().await.unwrap().into_payload(),
                NetworkMessage::Ping(211)
            );
            peer.write_message(NetworkMessage::MemPool).await.unwrap();
            assert_eq!(
                peer.read_message().await.unwrap().into_payload(),
                NetworkMessage::Inv(vec![expected_inventory])
            );
            peer.write_message(NetworkMessage::GetData(vec![expected_inventory]))
                .await
                .unwrap();
            assert_eq!(
                peer.read_message().await.unwrap().into_payload(),
                NetworkMessage::Tx(expected_transaction)
            );
            peer.write_message(NetworkMessage::Pong(211)).await.unwrap();
        });

        let (ready_sender, ready) = tokio::sync::oneshot::channel();
        let (activate, activation) = tokio::sync::oneshot::channel();
        let connection = tokio::spawn(connect_and_maintain_standby(
            DeploymentConfig::for_network(Network::Regtest),
            remote,
            210,
            true,
            None,
            ready_sender,
            activation,
            None,
            None,
            Some(source),
            None,
        ));
        timeout(Duration::from_secs(2), ready)
            .await
            .expect("full-service standby should become ready")
            .unwrap();
        activate.send(()).unwrap();
        let mut connected = connection.await.unwrap().unwrap();
        connected.session.ping(211).await.unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn standby_announcements_fail_over_after_notfound_without_duplicate_inflight() {
        async fn wait_for_announcements(
            pool: &Arc<Mutex<TransactionAdmissionPool>>,
            expected: usize,
        ) {
            timeout(Duration::from_secs(2), async {
                loop {
                    if pool
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .transaction_request_announcement_len()
                        == expected
                    {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            })
            .await
            .expect("standby announcement should enter the shared request tracker");
        }

        let transaction = wallet_broadcast_transaction();
        let inventory = Inventory::WTx(transaction.compute_wtxid());
        let first_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let first_remote = first_listener.local_addr().unwrap();
        let first_server = tokio::spawn(async move {
            let (mut peer, _) = accept_peer(first_listener, peer_version(232)).await;
            receive_client_preferences(&mut peer).await;
            let NetworkMessage::Ping(nonce) = peer.read_message().await.unwrap().into_payload()
            else {
                panic!("expected first standby ping");
            };
            peer.write_message(NetworkMessage::Inv(vec![inventory]))
                .await
                .unwrap();
            peer.write_message(NetworkMessage::Pong(nonce))
                .await
                .unwrap();
            loop {
                match peer.read_message().await.unwrap().into_payload() {
                    NetworkMessage::Ping(nonce) => {
                        peer.write_message(NetworkMessage::Pong(nonce))
                            .await
                            .unwrap();
                    }
                    NetworkMessage::GetData(entries) => {
                        assert_eq!(entries, vec![inventory]);
                        break;
                    }
                    message => panic!("unexpected first standby message: {message:?}"),
                }
            }
            peer.write_message(NetworkMessage::NotFound(vec![inventory]))
                .await
                .unwrap();
        });
        let pool = Arc::new(Mutex::new(TransactionAdmissionPool::default()));
        let (activate_first, first_activation) = tokio::sync::oneshot::channel();
        let first_connected = connect_peer(
            DeploymentConfig::for_network(Network::Regtest),
            first_remote,
            230,
            false,
            None,
        )
        .await
        .unwrap();
        let first_pool = Arc::clone(&pool);
        let first = tokio::spawn(async move {
            maintain_standby(
                first_connected,
                first_activation,
                Duration::from_millis(100),
                231,
                None,
                None,
                Some(&first_pool),
            )
            .await
        });
        wait_for_announcements(&pool, 1).await;

        let second_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let second_remote = second_listener.local_addr().unwrap();
        let expected = transaction.clone();
        let second_server = tokio::spawn(async move {
            let (mut peer, _) = accept_peer(second_listener, peer_version(235)).await;
            receive_client_preferences(&mut peer).await;
            let NetworkMessage::Ping(nonce) = peer.read_message().await.unwrap().into_payload()
            else {
                panic!("expected second standby ping");
            };
            peer.write_message(NetworkMessage::Inv(vec![inventory]))
                .await
                .unwrap();
            peer.write_message(NetworkMessage::Pong(nonce))
                .await
                .unwrap();
            loop {
                match peer.read_message().await.unwrap().into_payload() {
                    NetworkMessage::Ping(nonce) => {
                        peer.write_message(NetworkMessage::Pong(nonce))
                            .await
                            .unwrap();
                    }
                    NetworkMessage::GetData(entries) => {
                        assert_eq!(entries, vec![inventory]);
                        break;
                    }
                    message => panic!("unexpected second standby message: {message:?}"),
                }
            }
            peer.write_message(NetworkMessage::Tx(expected))
                .await
                .unwrap();
        });
        let (activate_second, second_activation) = tokio::sync::oneshot::channel();
        let second_connected = connect_peer(
            DeploymentConfig::for_network(Network::Regtest),
            second_remote,
            233,
            false,
            None,
        )
        .await
        .unwrap();
        let second_pool = Arc::clone(&pool);
        let second = tokio::spawn(async move {
            maintain_standby(
                second_connected,
                second_activation,
                Duration::from_millis(100),
                234,
                None,
                None,
                Some(&second_pool),
            )
            .await
        });
        wait_for_announcements(&pool, 2).await;

        activate_first.send(()).unwrap();
        let mut first = first.await.unwrap().unwrap();
        fetch_announced_peer_transactions(&mut first.session, &pool, None)
            .await
            .unwrap();
        assert!(first.session.take_pending_transactions().is_empty());
        assert_eq!(
            pool.lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .transaction_request_announcement_len(),
            2
        );

        activate_second.send(()).unwrap();
        let mut second = second.await.unwrap().unwrap();
        fetch_announced_peer_transactions(&mut second.session, &pool, None)
            .await
            .unwrap();
        assert_eq!(
            second.session.take_pending_transactions(),
            vec![transaction]
        );
        assert_eq!(
            pool.lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .transaction_request_announcement_len(),
            0
        );
        first_server.await.unwrap();
        second_server.await.unwrap();
    }

    #[tokio::test]
    async fn standby_validates_headers_continuously_before_activation() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let remote = listener.local_addr().unwrap();
        let genesis = bitcoin::blockdata::constants::genesis_block(Network::Regtest).header;
        let child = mine_regtest_child(genesis.block_hash(), genesis.time + 1);
        let (second_poll_seen, second_poll) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut peer, _) = accept_peer(listener, peer_version(212)).await;
            receive_client_preferences(&mut peer).await;
            for response in [vec![child], vec![child]] {
                let NetworkMessage::Ping(nonce) = peer.read_message().await.unwrap().into_payload()
                else {
                    panic!("expected standby ping");
                };
                peer.write_message(NetworkMessage::Pong(nonce))
                    .await
                    .unwrap();
                assert!(matches!(
                    peer.read_message().await.unwrap().into_payload(),
                    NetworkMessage::GetHeaders(_)
                ));
                peer.write_message(NetworkMessage::Headers(response))
                    .await
                    .unwrap();
            }
            second_poll_seen.send(()).unwrap();
        });

        let connected = connect_peer(
            DeploymentConfig::for_network(Network::Regtest),
            remote,
            210,
            false,
            None,
        )
        .await
        .unwrap();
        let (activate, activation) = tokio::sync::oneshot::channel();
        let standby = tokio::spawn(maintain_standby(
            connected,
            activation,
            Duration::from_millis(10),
            211,
            None,
            Some(HeaderDag::new(Network::Regtest)),
            None,
        ));
        timeout(Duration::from_secs(2), second_poll)
            .await
            .expect("standby should complete two independent header polls")
            .unwrap();
        activate.send(()).unwrap();
        let activated = standby.await.unwrap().unwrap();
        assert_eq!(activated.validated_header_height, Some(1));
        server.await.unwrap();
    }

    #[test]
    fn repeated_known_header_prefix_is_a_noop_but_not_an_infix() {
        let genesis = bitcoin::blockdata::constants::genesis_block(Network::Regtest).header;
        let child = mine_regtest_child(genesis.block_hash(), genesis.time + 1);
        let (dag, _) = HeaderDag::new(Network::Regtest)
            .validate_batch_contextual(&[child], child.time)
            .unwrap();
        let grandchild = mine_regtest_child(child.block_hash(), child.time + 1);

        assert!(unseen_header_suffix(&dag, &[child]).unwrap().is_empty());
        assert_eq!(
            unseen_header_suffix(&dag, &[child, grandchild]).unwrap(),
            &[grandchild]
        );
        assert!(matches!(
            unseen_header_suffix(&dag, &[grandchild, child]),
            Err(HeaderError::Duplicate(hash)) if hash == child.block_hash()
        ));
    }

    #[tokio::test]
    async fn standby_evicts_a_peer_that_announces_invalid_proof_of_work() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let remote = listener.local_addr().unwrap();
        let genesis = bitcoin::blockdata::constants::genesis_block(Network::Regtest).header;
        let mut invalid = mine_regtest_child(genesis.block_hash(), genesis.time + 1);
        while invalid.validate_pow(Target::MAX_ATTAINABLE_REGTEST).is_ok() {
            invalid.nonce = invalid.nonce.wrapping_add(1);
        }
        let server = tokio::spawn(async move {
            let (mut peer, _) = accept_peer(listener, peer_version(222)).await;
            receive_client_preferences(&mut peer).await;
            let NetworkMessage::Ping(nonce) = peer.read_message().await.unwrap().into_payload()
            else {
                panic!("expected standby ping");
            };
            peer.write_message(NetworkMessage::Pong(nonce))
                .await
                .unwrap();
            assert!(matches!(
                peer.read_message().await.unwrap().into_payload(),
                NetworkMessage::GetHeaders(_)
            ));
            peer.write_message(NetworkMessage::Headers(vec![invalid]))
                .await
                .unwrap();
        });
        let connected = connect_peer(
            DeploymentConfig::for_network(Network::Regtest),
            remote,
            220,
            false,
            None,
        )
        .await
        .unwrap();
        let (_activate, activation) = tokio::sync::oneshot::channel();
        let result = timeout(
            Duration::from_secs(2),
            maintain_standby(
                connected,
                activation,
                Duration::from_millis(10),
                221,
                None,
                Some(HeaderDag::new(Network::Regtest)),
                None,
            ),
        )
        .await
        .expect("invalid standby header should terminate the peer");
        let error = match result {
            Ok(_) => panic!("invalid standby header must evict the peer"),
            Err(error) => error,
        };
        assert_eq!(error.kind, PeerFailureKind::ProtocolViolation);
        server.await.unwrap();
    }

    async fn expect_standby_transaction(listener: TcpListener, expected: Transaction) {
        let (mut peer, _) = accept_peer(listener, peer_version(rand::random())).await;
        receive_client_preferences(&mut peer).await;
        let expected_inventory =
            bitcoin::p2p::message_blockdata::Inventory::WTx(expected.compute_wtxid());
        assert_eq!(
            peer.read_message().await.unwrap().into_payload(),
            NetworkMessage::Inv(vec![expected_inventory])
        );
        let NetworkMessage::Ping(nonce) = peer.read_message().await.unwrap().into_payload() else {
            panic!("expected transaction relay ping");
        };
        peer.write_message(NetworkMessage::GetData(vec![expected_inventory]))
            .await
            .unwrap();
        assert_eq!(
            peer.read_message().await.unwrap().into_payload(),
            NetworkMessage::Tx(expected)
        );
        peer.write_message(NetworkMessage::Pong(nonce))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn transaction_is_relayed_to_every_hot_standby() {
        let first_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let first_remote = first_listener.local_addr().unwrap();
        let second_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let second_remote = second_listener.local_addr().unwrap();
        let transaction = wallet_broadcast_transaction();
        let first_server = tokio::spawn(expect_standby_transaction(
            first_listener,
            transaction.clone(),
        ));
        let second_server = tokio::spawn(expect_standby_transaction(
            second_listener,
            transaction.clone(),
        ));
        let first = connect_peer(
            DeploymentConfig::for_network(Network::Regtest),
            first_remote,
            301,
            false,
            None,
        )
        .await
        .unwrap();
        let second = connect_peer(
            DeploymentConfig::for_network(Network::Regtest),
            second_remote,
            302,
            false,
            None,
        )
        .await
        .unwrap();
        let (relay, _) = broadcast::channel(WALLET_BROADCAST_QUEUE_CAPACITY);
        let (activate_first, first_activation) = tokio::sync::oneshot::channel();
        let (activate_second, second_activation) = tokio::sync::oneshot::channel();
        let first_standby = tokio::spawn(maintain_standby(
            first,
            first_activation,
            Duration::from_secs(60),
            303,
            Some(relay.subscribe()),
            None,
            None,
        ));
        let second_standby = tokio::spawn(maintain_standby(
            second,
            second_activation,
            Duration::from_secs(60),
            304,
            Some(relay.subscribe()),
            None,
            None,
        ));

        assert_eq!(relay.send(transaction_relay(transaction)).unwrap(), 2);
        timeout(Duration::from_secs(2), first_server)
            .await
            .expect("first standby transaction relay timed out")
            .unwrap();
        timeout(Duration::from_secs(2), second_server)
            .await
            .expect("second standby transaction relay timed out")
            .unwrap();

        activate_first.send(()).unwrap();
        activate_second.send(()).unwrap();
        assert_eq!(first_standby.await.unwrap().unwrap().remote, first_remote);
        assert_eq!(second_standby.await.unwrap().unwrap().remote, second_remote);
    }

    #[tokio::test]
    async fn lagging_transaction_relay_removes_hot_standby() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let remote = listener.local_addr().unwrap();
        let (release_server, server_release) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut peer, _) = accept_peer(listener, peer_version(401)).await;
            receive_client_preferences(&mut peer).await;
            server_release.await.unwrap();
        });
        let connected = connect_peer(
            DeploymentConfig::for_network(Network::Regtest),
            remote,
            400,
            false,
            None,
        )
        .await
        .unwrap();
        let (relay, receiver) = broadcast::channel(1);
        assert_eq!(
            relay
                .send(transaction_relay(wallet_broadcast_transaction()))
                .unwrap(),
            1
        );
        assert_eq!(
            relay
                .send(transaction_relay(wallet_broadcast_transaction()))
                .unwrap(),
            1
        );
        let (_activate, activation) = tokio::sync::oneshot::channel();
        let error = match maintain_standby(
            connected,
            activation,
            Duration::from_secs(60),
            402,
            Some(receiver),
            None,
            None,
        )
        .await
        {
            Ok(_) => panic!("lagging standby must be removed"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("lagged by 1 messages"));

        release_server.send(()).unwrap();
        server.await.unwrap();
    }

    fn wallet_broadcast_transaction() -> Transaction {
        Transaction {
            version: TransactionVersion::TWO,
            lock_time: LockTime::ZERO,
            input: vec![TxIn {
                previous_output: OutPoint {
                    txid: Txid::from_byte_array([1; 32]),
                    vout: 0,
                },
                script_sig: ScriptBuf::new(),
                sequence: Sequence::MAX,
                witness: Witness::new(),
            }],
            output: vec![TxOut {
                value: Amount::from_sat(1_000),
                script_pubkey: ScriptBuf::new(),
            }],
        }
    }

    fn transaction_relay(transaction: Transaction) -> TransactionRelay {
        TransactionRelay {
            policy_vsize: transaction.vsize(),
            transaction,
            fee_sats: Some(1_000),
        }
    }

    #[test]
    fn compact_transaction_candidates_are_deduplicated_and_bounded() {
        let candidates = CompactTransactionCandidates::default();
        let mut inserted = Vec::new();
        for marker in 1..=MAX_COMPACT_TRANSACTION_CANDIDATES + 1 {
            let mut transaction = wallet_broadcast_transaction();
            transaction.input[0].previous_output.txid =
                Txid::from_byte_array([u8::try_from(marker).unwrap(); 32]);
            inserted.push(transaction.compute_wtxid());
            candidates.insert(transaction);
        }
        let snapshot = candidates.snapshot();
        assert_eq!(snapshot.len(), MAX_COMPACT_TRANSACTION_CANDIDATES);
        assert!(!snapshot.iter().any(|tx| tx.compute_wtxid() == inserted[0]));
        assert_eq!(
            snapshot.last().unwrap().compute_wtxid(),
            *inserted.last().unwrap()
        );

        candidates.insert(snapshot[0].clone());
        let deduplicated = candidates.snapshot();
        assert_eq!(deduplicated.len(), MAX_COMPACT_TRANSACTION_CANDIDATES);
        assert_eq!(
            deduplicated.last().unwrap().compute_wtxid(),
            snapshot[0].compute_wtxid()
        );
    }

    #[test]
    fn disconnected_block_queue_excludes_coinbase_and_preserves_lower_height_parents() {
        let mut pending = VecDeque::new();
        let mut higher = regtest_block(BlockHash::all_zeros(), 1);
        for marker in 101..=110 {
            let mut transaction = wallet_broadcast_transaction();
            transaction.input[0].previous_output.txid = Txid::from_byte_array([marker; 32]);
            higher.txdata.push(transaction);
        }
        retain_disconnected_block_transactions(
            &mut pending,
            &higher,
            MAX_ADMITTED_TRANSACTIONS,
            MAX_ADMITTED_TRANSACTION_BYTES,
        );
        assert_eq!(pending.len(), 10);

        let mut lower = regtest_block(BlockHash::all_zeros(), 2);
        let mut expected = Vec::new();
        for marker in 1..=u8::try_from(MAX_ADMITTED_TRANSACTIONS).unwrap() {
            let mut transaction = wallet_broadcast_transaction();
            transaction.input[0].previous_output.txid = Txid::from_byte_array([marker; 32]);
            expected.push(transaction.compute_txid());
            lower.txdata.push(transaction);
        }
        retain_disconnected_block_transactions(
            &mut pending,
            &lower,
            MAX_ADMITTED_TRANSACTIONS,
            MAX_ADMITTED_TRANSACTION_BYTES,
        );

        assert_eq!(pending.len(), MAX_ADMITTED_TRANSACTIONS);
        assert_eq!(
            pending
                .iter()
                .map(Transaction::compute_txid)
                .collect::<Vec<_>>(),
            expected
        );
        assert!(pending.iter().all(|transaction| !transaction.is_coinbase()));
    }

    #[test]
    fn admitted_peer_relay_sends_only_selected_surviving_transactions() {
        let mut first = wallet_broadcast_transaction();
        first.input[0].previous_output.txid = Txid::from_byte_array([41; 32]);
        let mut second = wallet_broadcast_transaction();
        second.input[0].previous_output.txid = Txid::from_byte_array([42; 32]);
        let (relay, mut receiver) = broadcast::channel(WALLET_BROADCAST_QUEUE_CAPACITY);
        let first = AdmittedTransactionRelay {
            policy_vsize: first.vsize(),
            transaction: first,
            fee_sats: 500,
        };
        let second = AdmittedTransactionRelay {
            policy_vsize: second.vsize(),
            transaction: second,
            fee_sats: 1_000,
        };

        let relayed = relay_selected_transactions(
            &relay,
            &[first, second.clone()],
            &HashSet::from([second.transaction.compute_txid()]),
        );

        assert_eq!(relayed, vec![second.transaction.compute_txid()]);
        assert_eq!(
            receiver.try_recv().unwrap(),
            TransactionRelay {
                transaction: second.transaction,
                fee_sats: Some(second.fee_sats),
                policy_vsize: second.policy_vsize,
            }
        );
        assert!(matches!(
            receiver.try_recv(),
            Err(broadcast::error::TryRecvError::Empty)
        ));
    }

    #[test]
    fn peer_transaction_rebroadcast_is_due_persistent_and_requires_a_standby() {
        let directory = TempDir::new().unwrap();
        let store =
            RedbTransactionPoolStore::open(directory.path().join("mempool.redb"), Network::Regtest)
                .unwrap();
        let transaction = wallet_broadcast_transaction();
        store.replace(std::slice::from_ref(&transaction)).unwrap();
        let active = vec![AdmittedTransactionRelay {
            policy_vsize: transaction.vsize(),
            transaction: transaction.clone(),
            fee_sats: 1_000,
        }];
        let (relay, receiver) = broadcast::channel(WALLET_BROADCAST_QUEUE_CAPACITY);
        drop(receiver);

        assert_eq!(
            relay_due_peer_transactions(&store, &active, &relay, 100).unwrap(),
            0
        );
        let mut receiver = relay.subscribe();
        assert_eq!(
            relay_due_peer_transactions(&store, &active, &relay, 101).unwrap(),
            1
        );
        assert_eq!(receiver.try_recv().unwrap().transaction, transaction);
        assert_eq!(
            relay_due_peer_transactions(
                &store,
                &active,
                &relay,
                101 + PEER_TRANSACTION_REBROADCAST_INTERVAL_SECS - 1
            )
            .unwrap(),
            0
        );
        assert_eq!(
            relay_due_peer_transactions(
                &store,
                &active,
                &relay,
                101 + PEER_TRANSACTION_REBROADCAST_INTERVAL_SECS
            )
            .unwrap(),
            1
        );
    }

    #[tokio::test]
    async fn announced_peer_transaction_is_downloaded_for_existing_admission_path() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let remote = listener.local_addr().unwrap();
        let transaction = wallet_broadcast_transaction();
        let inventory = Inventory::WTx(transaction.compute_wtxid());
        let expected = transaction.clone();
        let server = tokio::spawn(async move {
            let (mut peer, _) = accept_peer(listener, peer_version(601)).await;
            assert_eq!(
                peer.read_message().await.unwrap().into_payload(),
                NetworkMessage::Ping(602)
            );
            peer.write_message(NetworkMessage::Inv(vec![inventory]))
                .await
                .unwrap();
            peer.write_message(NetworkMessage::Pong(602)).await.unwrap();
            assert_eq!(
                peer.read_message().await.unwrap().into_payload(),
                NetworkMessage::GetData(vec![inventory])
            );
            peer.write_message(NetworkMessage::Tx(expected))
                .await
                .unwrap();
        });
        let mut session = connect_outbound(
            remote,
            Network::Regtest.magic(),
            600,
            "/rbtcd:test/".to_owned(),
            0,
        )
        .await
        .unwrap();
        session.ping(602).await.unwrap();
        let pool = Arc::new(Mutex::new(TransactionAdmissionPool::default()));

        fetch_announced_peer_transactions(&mut session, &pool, None)
            .await
            .unwrap();

        assert_eq!(session.take_pending_transactions(), vec![transaction]);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn independent_block_windows_download_concurrently_and_preserve_order() {
        async fn serve_window(listener: TcpListener, blocks: Vec<Block>, nonce: u64) {
            let (mut peer, _) = accept_peer(listener, peer_version(nonce)).await;
            let expected = blocks
                .iter()
                .map(|block| Inventory::WitnessBlock(block.block_hash()))
                .collect::<Vec<_>>();
            assert_eq!(
                peer.read_message().await.unwrap().into_payload(),
                NetworkMessage::GetData(expected)
            );
            for block in blocks.into_iter().rev() {
                peer.write_message(NetworkMessage::Block(block))
                    .await
                    .unwrap();
            }
        }

        let primary_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let primary_remote = primary_listener.local_addr().unwrap();
        let auxiliary_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let auxiliary_remote = auxiliary_listener.local_addr().unwrap();
        let genesis = bitcoin::blockdata::constants::genesis_block(Network::Regtest);
        let primary_blocks = vec![
            regtest_block_at_height(genesis.block_hash(), genesis.header.time + 1, 1),
            regtest_block_at_height(genesis.block_hash(), genesis.header.time + 2, 2),
        ];
        let auxiliary_blocks = vec![
            regtest_block_at_height(genesis.block_hash(), genesis.header.time + 3, 3),
            regtest_block_at_height(genesis.block_hash(), genesis.header.time + 4, 4),
        ];
        let primary_expected = primary_blocks
            .iter()
            .map(Block::block_hash)
            .collect::<Vec<_>>();
        let auxiliary_expected = auxiliary_blocks
            .iter()
            .map(Block::block_hash)
            .collect::<Vec<_>>();
        let primary_server = tokio::spawn(serve_window(primary_listener, primary_blocks, 641));
        let auxiliary_server =
            tokio::spawn(serve_window(auxiliary_listener, auxiliary_blocks, 642));
        let (primary, auxiliary) = tokio::join!(
            connect_outbound(
                primary_remote,
                Network::Regtest.magic(),
                643,
                "/rbtcd:test/".to_owned(),
                0,
            ),
            connect_outbound(
                auxiliary_remote,
                Network::Regtest.magic(),
                644,
                "/rbtcd:test/".to_owned(),
                0,
            ),
        );
        let mut primary = primary.unwrap();
        let mut auxiliary = auxiliary.unwrap();

        let (primary_result, auxiliary_result) = tokio::join!(
            download_block_window(&mut primary, &primary_expected, &[], "primary test"),
            download_block_window(&mut auxiliary, &auxiliary_expected, &[], "auxiliary test"),
        );
        assert_eq!(
            primary_result
                .unwrap()
                .iter()
                .map(Block::block_hash)
                .collect::<Vec<_>>(),
            primary_expected
        );
        assert_eq!(
            auxiliary_result
                .unwrap()
                .iter()
                .map(Block::block_hash)
                .collect::<Vec<_>>(),
            auxiliary_expected
        );
        primary_server.await.unwrap();
        auxiliary_server.await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn scoped_execution_prefetch_drains_dual_peers_on_multithread_runtime() {
        let primary_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let primary_remote = primary_listener.local_addr().unwrap();
        let auxiliary_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let auxiliary_remote = auxiliary_listener.local_addr().unwrap();
        let genesis = bitcoin::blockdata::constants::genesis_block(Network::Regtest);
        let blocks = (1..=65)
            .map(|height| {
                regtest_block_at_height(genesis.block_hash(), genesis.header.time + height, height)
            })
            .collect::<Vec<_>>();
        let hashes = blocks.iter().map(Block::block_hash).collect::<Vec<_>>();
        let primary_inventory = hashes[..VALIDATION_BLOCK_WINDOW_SIZE]
            .iter()
            .copied()
            .map(Inventory::WitnessBlock)
            .collect::<Vec<_>>();
        let auxiliary_inventory = hashes[VALIDATION_BLOCK_WINDOW_SIZE..]
            .iter()
            .copied()
            .map(Inventory::WitnessBlock)
            .collect::<Vec<_>>();
        let primary_blocks = blocks[..VALIDATION_BLOCK_WINDOW_SIZE].to_vec();
        let auxiliary_blocks = blocks[VALIDATION_BLOCK_WINDOW_SIZE..].to_vec();
        let primary_server = tokio::spawn(async move {
            let (mut peer, _) = accept_peer(primary_listener, peer_version(643)).await;
            for inventory in primary_inventory.chunks(MAX_BLOCKS_IN_FLIGHT) {
                assert_eq!(
                    peer.read_message().await.unwrap().into_payload(),
                    NetworkMessage::GetData(inventory.to_vec())
                );
            }
            for block in primary_blocks {
                peer.write_message(NetworkMessage::Block(block))
                    .await
                    .unwrap();
            }
        });
        let auxiliary_server = tokio::spawn(async move {
            let (mut peer, _) = accept_peer(auxiliary_listener, peer_version(645)).await;
            assert_eq!(
                peer.read_message().await.unwrap().into_payload(),
                NetworkMessage::GetData(auxiliary_inventory)
            );
            for block in auxiliary_blocks {
                peer.write_message(NetworkMessage::Block(block))
                    .await
                    .unwrap();
            }
        });
        let mut session = connect_outbound(
            primary_remote,
            Network::Regtest.magic(),
            644,
            "/rbtcd:test/".to_owned(),
            0,
        )
        .await
        .unwrap();
        let mut auxiliary_session = Some(
            connect_outbound(
                auxiliary_remote,
                Network::Regtest.magic(),
                646,
                "/rbtcd:test/".to_owned(),
                0,
            )
            .await
            .unwrap(),
        );
        let runtime = tokio::runtime::Handle::current();

        let downloaded = std::thread::scope(|scope| {
            let prefetch = scope.spawn(|| {
                runtime.block_on(download_execution_prefetch(
                    &mut session,
                    &mut auxiliary_session,
                    &hashes,
                    &[],
                ))
            });
            std::thread::sleep(Duration::from_millis(20));
            prefetch.join().unwrap().unwrap()
        });

        assert_eq!(
            downloaded
                .iter()
                .map(|bytes| deserialize::<Block>(bytes).unwrap().block_hash())
                .collect::<Vec<_>>(),
            hashes
        );
        assert!(auxiliary_session.is_some());
        primary_server.await.unwrap();
        auxiliary_server.await.unwrap();
    }

    #[test]
    fn parallel_block_windows_keep_every_configured_batch_bounded() {
        for (batch_len, expected) in [
            (64, (64, 0, 0)),
            (129, (64, 64, 1)),
            (252, (64, 64, 124)),
            (504, (64, 64, 376)),
            (MAX_VALIDATION_BATCH_SIZE, (64, 64, 880)),
        ] {
            let hashes = vec![BlockHash::all_zeros(); batch_len];
            let (primary, auxiliary, remainder) = split_parallel_block_windows(&hashes);
            assert_eq!((primary.len(), auxiliary.len(), remainder.len()), expected);
            assert!(primary.len() <= VALIDATION_BLOCK_WINDOW_SIZE);
            assert!(auxiliary.len() <= VALIDATION_BLOCK_WINDOW_SIZE);
            assert_eq!(primary.len() + auxiliary.len() + remainder.len(), batch_len);
        }
    }

    #[test]
    fn execution_prefetch_covers_the_complete_validation_batch() {
        assert_eq!(validation_prefetch_limit(64), 64);
        assert_eq!(
            validation_prefetch_limit(MAX_VALIDATION_PREFETCH_BATCH_SIZE),
            MAX_VALIDATION_PREFETCH_BATCH_SIZE
        );
        assert_eq!(
            validation_prefetch_limit(MAX_VALIDATION_PREFETCH_BATCH_SIZE + 1),
            MAX_VALIDATION_PREFETCH_BATCH_SIZE
        );
        assert_eq!(
            validation_prefetch_limit(MAX_VALIDATION_BATCH_SIZE),
            MAX_VALIDATION_BATCH_SIZE
        );
    }

    #[test]
    fn parallel_structure_validation_preserves_order_and_earliest_error() {
        let headers = HeaderDag::new(Network::Regtest);
        let block = bitcoin::blockdata::constants::genesis_block(Network::Regtest);
        let expected = vec![headers.active_tip(); 192];
        let blocks = vec![block; expected.len()];
        let deployments = DeploymentConfig::for_network(Network::Regtest);

        let validated =
            validate_downloaded_blocks(&deployments, &headers, &expected, &blocks).unwrap();
        assert_eq!(validated.len(), expected.len());

        let mut invalid = expected;
        invalid[70].height = 70;
        invalid[70].hash = BlockHash::all_zeros();
        invalid[130].height = 130;
        invalid[130].hash = BlockHash::all_zeros();
        let error =
            validate_downloaded_blocks(&deployments, &headers, &invalid, &blocks).unwrap_err();
        assert!(error.to_string().contains("height 70"));
    }

    #[tokio::test]
    async fn lagging_auxiliary_window_is_abandoned_after_primary_grace() {
        let primary_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let primary_remote = primary_listener.local_addr().unwrap();
        let auxiliary_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let auxiliary_remote = auxiliary_listener.local_addr().unwrap();
        let genesis = bitcoin::blockdata::constants::genesis_block(Network::Regtest);
        let primary_block =
            regtest_block_at_height(genesis.block_hash(), genesis.header.time + 1, 1);
        let auxiliary_block =
            regtest_block_at_height(genesis.block_hash(), genesis.header.time + 2, 2);
        let second_auxiliary_block =
            regtest_block_at_height(auxiliary_block.block_hash(), genesis.header.time + 3, 3);
        let primary_hash = primary_block.block_hash();
        let auxiliary_hash = auxiliary_block.block_hash();
        let second_auxiliary_hash = second_auxiliary_block.block_hash();
        let primary_server = tokio::spawn(async move {
            let (mut peer, _) = accept_peer(primary_listener, peer_version(653)).await;
            assert_eq!(
                peer.read_message().await.unwrap().into_payload(),
                NetworkMessage::GetData(vec![Inventory::WitnessBlock(primary_hash)])
            );
            peer.write_message(NetworkMessage::Block(primary_block))
                .await
                .unwrap();
        });
        let (release_sender, release) = tokio::sync::oneshot::channel();
        let auxiliary_server = tokio::spawn(async move {
            let (mut peer, _) = accept_peer(auxiliary_listener, peer_version(654)).await;
            assert_eq!(
                peer.read_message().await.unwrap().into_payload(),
                NetworkMessage::GetData(vec![
                    Inventory::WitnessBlock(auxiliary_hash),
                    Inventory::WitnessBlock(second_auxiliary_hash),
                ])
            );
            peer.write_message(NetworkMessage::Block(auxiliary_block))
                .await
                .unwrap();
            let _ = release.await;
            let _ = peer
                .write_message(NetworkMessage::Block(second_auxiliary_block))
                .await;
        });
        let (primary, auxiliary) = tokio::join!(
            connect_outbound(
                primary_remote,
                Network::Regtest.magic(),
                655,
                "/rbtcd:test/".to_owned(),
                0,
            ),
            connect_outbound(
                auxiliary_remote,
                Network::Regtest.magic(),
                656,
                "/rbtcd:test/".to_owned(),
                0,
            ),
        );
        let mut primary = primary.unwrap();
        let mut auxiliary = auxiliary.unwrap();
        let primary_hashes = [primary_hash];
        let auxiliary_hashes = [auxiliary_hash, second_auxiliary_hash];
        let (primary_request, auxiliary_request) = tokio::join!(
            request_block_window(&mut primary, &primary_hashes, "primary lag test"),
            request_block_window(&mut auxiliary, &auxiliary_hashes, "auxiliary lag test"),
        );
        primary_request.unwrap();
        auxiliary_request.unwrap();

        let (blocks, auxiliary_blocks, auxiliary_result) = receive_parallel_block_windows(
            &mut primary,
            &primary_hashes,
            &mut auxiliary,
            &auxiliary_hashes,
            &[],
            Duration::from_millis(20),
        )
        .await
        .unwrap();

        assert_eq!(blocks[0].block_hash(), primary_hash);
        assert!(auxiliary_result.is_none());
        assert_eq!(
            auxiliary_blocks[0].as_ref().map(Block::block_hash),
            Some(auxiliary_hash)
        );
        assert!(auxiliary_blocks[1].is_none());
        drop(auxiliary);
        release_sender.send(()).unwrap();
        primary_server.await.unwrap();
        auxiliary_server.await.unwrap();
    }

    #[tokio::test]
    async fn orphan_parent_fetch_uses_txid_inventory_even_after_bip339() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let remote = listener.local_addr().unwrap();
        let parent = wallet_broadcast_transaction();
        let parent_txid = parent.compute_txid();
        let expected = parent.clone();
        let server = tokio::spawn(async move {
            let (mut peer, _) = accept_peer(listener, peer_version(611)).await;
            assert_eq!(
                peer.read_message().await.unwrap().into_payload(),
                NetworkMessage::GetData(vec![Inventory::Transaction(parent_txid)])
            );
            peer.write_message(NetworkMessage::Tx(expected))
                .await
                .unwrap();
        });
        let mut session = connect_outbound(
            remote,
            Network::Regtest.magic(),
            610,
            "/rbtcd:test/".to_owned(),
            0,
        )
        .await
        .unwrap();

        assert_eq!(
            fetch_orphan_parent_transactions(&mut session, vec![parent_txid])
                .await
                .unwrap(),
            1
        );
        assert_eq!(session.take_pending_transactions(), vec![parent]);
        server.await.unwrap();
    }

    #[test]
    fn known_transaction_inventory_is_not_downloaded_again() {
        let transaction = wallet_broadcast_transaction();
        let mut confirmed = wallet_broadcast_transaction();
        confirmed.input[0].previous_output.txid = Txid::from_byte_array([98; 32]);
        let mut pool = TransactionAdmissionPool::default();
        pool.remember_confirmed_transactions([&confirmed]);
        assert!(transaction_inventory_is_known(
            Inventory::Transaction(transaction.compute_txid()),
            std::slice::from_ref(&transaction),
            &pool,
        ));
        assert!(transaction_inventory_is_known(
            Inventory::WitnessTransaction(transaction.compute_txid()),
            std::slice::from_ref(&transaction),
            &pool,
        ));
        assert!(transaction_inventory_is_known(
            Inventory::WTx(transaction.compute_wtxid()),
            std::slice::from_ref(&transaction),
            &pool,
        ));
        assert!(transaction_inventory_is_known(
            Inventory::Transaction(confirmed.compute_txid()),
            &[],
            &pool,
        ));
        assert!(transaction_inventory_is_known(
            Inventory::WTx(confirmed.compute_wtxid()),
            &[],
            &pool,
        ));
        assert!(!transaction_inventory_is_known(
            Inventory::Transaction(Txid::from_byte_array([99; 32])),
            &[transaction],
            &pool,
        ));
    }

    #[test]
    fn transaction_request_source_guard_cleans_only_canceled_standby() {
        let pool = Arc::new(Mutex::new(TransactionAdmissionPool::default()));
        let id = TransactionRequestId::Txid(Txid::from_byte_array([89; 32]));
        {
            let mut tracker = pool
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            assert_eq!(tracker.announce_transaction_requests(51, [id], 0), 1);
            assert_eq!(tracker.announce_transaction_requests(52, [id], 0), 1);
        }
        drop(TransactionRequestSourceGuard::new(51, Arc::clone(&pool)));
        assert_eq!(
            pool.lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .transaction_request_announcement_len(),
            1
        );
        let mut activated = TransactionRequestSourceGuard::new(52, Arc::clone(&pool));
        activated.disarm();
        drop(activated);
        assert_eq!(
            pool.lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .transaction_request_announcement_len(),
            1
        );
    }

    #[test]
    fn peer_orphan_provenance_excludes_persisted_and_reorg_candidates() {
        let mut peer = wallet_broadcast_transaction();
        peer.input[0].previous_output.txid = Txid::from_byte_array([91; 32]);
        let mut persisted = wallet_broadcast_transaction();
        persisted.input[0].previous_output.txid = Txid::from_byte_array([92; 32]);
        let mut recovered = wallet_broadcast_transaction();
        recovered.input[0].previous_output.txid = Txid::from_byte_array([93; 32]);
        let selected = peer_orphan_candidates(
            vec![persisted, peer.clone(), recovered],
            &HashSet::from([peer.compute_txid()]),
        );

        assert_eq!(selected, vec![peer]);
    }

    #[test]
    fn orphan_parent_selection_skips_submitted_pool_and_confirmed_inputs() {
        let directory = TempDir::new().unwrap();
        let chainstate =
            RedbChainStore::open(directory.path().join("chainstate.redb"), Network::Regtest)
                .unwrap();
        let confirmed = OutPoint {
            txid: Txid::from_byte_array([94; 32]),
            vout: 0,
        };
        chainstate
            .apply(
                &[],
                &[(
                    confirmed.into(),
                    rbtc::utxo::Utxo {
                        value_sats: 10_000,
                        height: 0,
                        is_coinbase: false,
                        last_touched: 0,
                        creation_mtp: 0,
                        script_pubkey: Vec::new(),
                    },
                )],
            )
            .unwrap();
        let mut known_parent = wallet_broadcast_transaction();
        known_parent.input[0].previous_output.txid = Txid::from_byte_array([95; 32]);
        let mut submitted_parent = wallet_broadcast_transaction();
        submitted_parent.input[0].previous_output.txid = Txid::from_byte_array([96; 32]);
        let mut recently_confirmed_parent = wallet_broadcast_transaction();
        recently_confirmed_parent.input[0].previous_output.txid = Txid::from_byte_array([97; 32]);
        let missing = Txid::from_byte_array([97; 32]);
        let input = wallet_broadcast_transaction().input[0].clone();
        let mut child = wallet_broadcast_transaction();
        child.input = [
            confirmed,
            OutPoint::new(known_parent.compute_txid(), 0),
            OutPoint::new(submitted_parent.compute_txid(), 0),
            OutPoint::new(recently_confirmed_parent.compute_txid(), 0),
            OutPoint::new(missing, 0),
        ]
        .into_iter()
        .map(|previous_output| TxIn {
            previous_output,
            ..input.clone()
        })
        .collect();
        let mut pool = TransactionAdmissionPool::default();
        assert_eq!(pool.retain_orphans(&[known_parent], 100, 1), 1);
        pool.remember_confirmed_transactions([&recently_confirmed_parent]);

        assert_eq!(
            unavailable_parent_txids(
                &pool,
                &chainstate,
                &[submitted_parent, child.clone()],
                &[child],
            )
            .unwrap(),
            BTreeSet::from([missing])
        );
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn wallet_broadcast_queue_retries_before_writing_to_the_peer() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let remote = listener.local_addr().unwrap();
        let transaction = wallet_broadcast_transaction();
        let expected = transaction.clone();
        let server = tokio::spawn(async move {
            let (mut peer, _) = accept_peer(listener, peer_version(2)).await;
            assert_eq!(
                peer.read_message().await.unwrap().into_payload(),
                NetworkMessage::Tx(expected)
            );
        });
        let mut session = connect_outbound(
            remote,
            Network::Regtest.magic(),
            1,
            "/rbtcd:test/".to_owned(),
            0,
        )
        .await
        .unwrap();

        let directory = TempDir::new().unwrap();
        let (broadcast_sender, broadcast_receiver) = mpsc::channel(WALLET_BROADCAST_QUEUE_CAPACITY);
        let (standby_relay, mut standby_receiver) =
            broadcast::channel(WALLET_BROADCAST_QUEUE_CAPACITY);
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
            token: LocalAuthToken::new("a".repeat(32)).unwrap(),
            token_path: directory.path().join("wallet.token"),
            audit: AuthorizationAuditLog::open(directory.path().join(API_AUDIT_FILE)).unwrap(),
            scan: WalletScanConfig {
                gap_limit: DEFAULT_WALLET_GAP_LIMIT,
                birthday_height: 0,
            },
            broadcast_sender: broadcast_sender.clone(),
            broadcast_receiver: Arc::new(tokio::sync::Mutex::new(broadcast_receiver)),
            pending_broadcast: Arc::new(tokio::sync::Mutex::new(None)),
            compact_candidates: CompactTransactionCandidates::default(),
            rebroadcast: Arc::new(
                RedbRebroadcastStore::open(
                    directory.path().join("rebroadcast.redb"),
                    Network::Regtest,
                )
                .unwrap(),
            ),
        };
        let (result, mut completion) = tokio::sync::oneshot::channel();
        let (abandoned_result, abandoned_completion) = tokio::sync::oneshot::channel();
        drop(abandoned_completion);
        let mut abandoned = transaction.clone();
        abandoned.output[0].value = Amount::from_sat(999);
        let mut retry = transaction.clone();
        retry.input[0].previous_output = OutPoint::null();
        assert!(
            broadcast_sender
                .try_send(WalletBroadcastRequest {
                    transaction: abandoned,
                    fee_sats: 1_000,
                    result: abandoned_result,
                })
                .is_ok()
        );
        assert!(
            broadcast_sender
                .try_send(WalletBroadcastRequest {
                    transaction: retry,
                    fee_sats: 1_000,
                    result,
                })
                .is_ok()
        );

        assert!(
            wait_for_peer_poll(
                &mut session,
                Duration::from_millis(10),
                Some(&wallet),
                &standby_relay,
            )
            .await
            .is_err()
        );
        assert!(matches!(
            completion.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Empty)
        ));
        wallet
            .pending_broadcast
            .lock()
            .await
            .as_mut()
            .unwrap()
            .transaction = transaction.clone();
        wallet
            .rebroadcast
            .enqueue(&transaction, u64::from(unix_time().unwrap()))
            .unwrap();
        wait_for_peer_poll(
            &mut session,
            Duration::from_millis(10),
            Some(&wallet),
            &standby_relay,
        )
        .await
        .unwrap();
        assert_eq!(completion.await.unwrap(), Ok(()));
        let relayed = standby_receiver.recv().await.unwrap();
        assert_eq!(relayed.transaction, transaction);
        assert_eq!(relayed.fee_sats, Some(1_000));
        assert_eq!(wallet.compact_candidates.snapshot(), vec![transaction]);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn durable_wallet_transaction_is_rebroadcast_after_reopen() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let remote = listener.local_addr().unwrap();
        let transaction = wallet_broadcast_transaction();
        let expected = transaction.clone();
        let server = tokio::spawn(async move {
            let (mut peer, _) = accept_peer(listener, peer_version(3)).await;
            assert_eq!(
                peer.read_message().await.unwrap().into_payload(),
                NetworkMessage::Tx(expected)
            );
        });
        let mut session = connect_outbound(
            remote,
            Network::Regtest.magic(),
            2,
            "/rbtcd:test/".to_owned(),
            0,
        )
        .await
        .unwrap();
        let directory = TempDir::new().unwrap();
        let rebroadcast_path = directory.path().join("rebroadcast.redb");
        {
            let store = RedbRebroadcastStore::open(&rebroadcast_path, Network::Regtest).unwrap();
            store
                .enqueue(&transaction, u64::from(unix_time().unwrap()))
                .unwrap();
        }
        let rebroadcast =
            Arc::new(RedbRebroadcastStore::open(&rebroadcast_path, Network::Regtest).unwrap());
        let (broadcast_sender, broadcast_receiver) = mpsc::channel(WALLET_BROADCAST_QUEUE_CAPACITY);
        let (standby_relay, _) = broadcast::channel(WALLET_BROADCAST_QUEUE_CAPACITY);
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
            token: LocalAuthToken::new("a".repeat(32)).unwrap(),
            token_path: directory.path().join("wallet.token"),
            audit: AuthorizationAuditLog::open(directory.path().join(API_AUDIT_FILE)).unwrap(),
            scan: WalletScanConfig {
                gap_limit: DEFAULT_WALLET_GAP_LIMIT,
                birthday_height: 0,
            },
            broadcast_sender,
            broadcast_receiver: Arc::new(tokio::sync::Mutex::new(broadcast_receiver)),
            pending_broadcast: Arc::new(tokio::sync::Mutex::new(None)),
            compact_candidates: CompactTransactionCandidates::default(),
            rebroadcast: Arc::clone(&rebroadcast),
        };

        wait_for_peer_poll(
            &mut session,
            Duration::from_millis(10),
            Some(&wallet),
            &standby_relay,
        )
        .await
        .unwrap();
        assert!(
            rebroadcast
                .due(u64::from(unix_time().unwrap()), 8)
                .unwrap()
                .is_empty()
        );
        assert_eq!(wallet.compact_candidates.snapshot(), vec![transaction]);
        server.await.unwrap();
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
                value: Amount::from_sat(block_subsidy_with_interval(height, 150)),
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
                "--experimental-network-execution",
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
                "--mempool-full-rbf",
                "--txindex",
                "--spent-output-index",
                "--block-filter-index",
                "--automatic-hot-standbys",
                "--mempool-max-transactions",
                "--mempool-max-bytes",
                "--log-level",
                "--log-dir",
                "--log-max-bytes",
                "--log-max-files",
                "--cleanup-validation-dir",
                "--prune-blocks",
                "--prune-max-bytes",
                "--minimum-free-bytes",
                "--chainstate-cache-bytes",
                "--background-chainstate-cache-bytes",
                "--bulk-validation-cache-bytes",
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
    fn parses_bounded_pruned_ledger_targets() {
        let options = parse_options(
            [
                "--network",
                "regtest",
                "--data-dir",
                "/tmp/rbtc-prune-parser",
                "--prune-blocks",
                "576",
                "--prune-max-bytes",
                "1073741824",
                "--minimum-free-bytes",
                "2147483648",
            ]
            .into_iter()
            .map(str::to_owned),
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            options.ledger_retention,
            LedgerRetention {
                max_blocks: 576,
                max_bytes: 1_073_741_824,
                slots: LedgerRetention::default().slots,
            }
        );
        assert_eq!(options.minimum_free_bytes, 2 * 1024 * 1024 * 1024);

        for arguments in [
            vec!["--prune-blocks", "576"],
            vec![
                "--data-dir",
                "/tmp/rbtc-prune-parser",
                "--prune-blocks",
                "287",
            ],
            vec![
                "--data-dir",
                "/tmp/rbtc-prune-parser",
                "--prune-blocks",
                "1009",
            ],
            vec![
                "--data-dir",
                "/tmp/rbtc-prune-parser",
                "--prune-max-bytes",
                "576716799",
            ],
            vec![
                "--data-dir",
                "/tmp/rbtc-prune-parser",
                "--minimum-free-bytes",
                "536870911",
            ],
            vec![
                "--data-dir",
                "/tmp/rbtc-prune-parser",
                "--minimum-free-bytes",
                "1099511627777",
            ],
            vec![
                "--data-dir",
                "/tmp/rbtc-prune-parser",
                "--prune-blocks",
                "576",
                "--utxo-activity-report",
            ],
        ] {
            assert!(parse_options(arguments.into_iter().map(str::to_owned)).is_err());
        }
    }

    #[test]
    fn parses_independent_optional_indexes_and_explicit_disables() {
        let options = parse_options(
            [
                "--network",
                "regtest",
                "--data-dir",
                "/tmp/rbtc-index-parser",
                "--txindex",
                "--spent-output-index",
                "--block-filter-index",
            ]
            .into_iter()
            .map(str::to_owned),
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            options.indexes,
            NodeIndexConfig {
                transaction: true,
                spent_output: true,
                basic_filter: true,
            }
        );
        let disabled = parse_options(
            [
                "--data-dir",
                "/tmp/rbtc-index-parser",
                "--txindex",
                "--no-txindex",
                "--spent-output-index",
                "--no-spent-output-index",
                "--block-filter-index",
                "--no-block-filter-index",
            ]
            .into_iter()
            .map(str::to_owned),
        )
        .unwrap()
        .unwrap();
        assert_eq!(disabled.indexes, NodeIndexConfig::default());
        assert!(parse_options(["--txindex"].into_iter().map(str::to_owned)).is_err());
    }

    #[test]
    fn parses_and_bounds_inbound_listener_resources() {
        let options = parse_options(
            [
                "--network",
                "regtest",
                "--data-dir",
                "/tmp/rbtc-inbound-parser",
                "--listen",
                "127.0.0.1:18444",
                "--max-inbound-peers",
                "12",
                "--max-inbound-peers-per-ip",
                "3",
                "--max-upload-bytes-per-day",
                "1048576",
                "--inbound-requests-per-minute",
                "600",
            ]
            .into_iter()
            .map(str::to_owned),
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            options.inbound_listen,
            Some("127.0.0.1:18444".parse().unwrap())
        );
        assert_eq!(options.inbound_limits.max_connections, 12);
        assert_eq!(options.inbound_limits.max_connections_per_ip, 3);
        assert_eq!(options.inbound_limits.max_upload_bytes_per_day, 1_048_576);
        assert_eq!(options.inbound_limits.max_requests_per_minute, 600);

        for arguments in [
            vec![
                "--data-dir",
                "/tmp/rbtc-inbound-parser",
                "--listen",
                "127.0.0.1:18444",
                "--max-inbound-peers",
                "0",
            ],
            vec![
                "--data-dir",
                "/tmp/rbtc-inbound-parser",
                "--listen",
                "127.0.0.1:18444",
                "--max-inbound-peers",
                "2",
                "--max-inbound-peers-per-ip",
                "3",
            ],
            vec![
                "--data-dir",
                "/tmp/rbtc-inbound-parser",
                "--listen",
                "127.0.0.1:18444",
                "--once",
            ],
        ] {
            assert!(parse_options(arguments.into_iter().map(str::to_owned)).is_err());
        }
    }

    #[test]
    fn parses_and_bounds_explicit_chainstate_cache_budgets() {
        let options = parse_options(
            [
                "--network",
                "regtest",
                "--data-dir",
                "/tmp/rbtc-cache-parser",
                "--chainstate-cache-bytes",
                "268435456",
                "--background-chainstate-cache-bytes",
                "536870912",
                "--bulk-validation-cache-bytes",
                "1073741824",
            ]
            .into_iter()
            .map(str::to_owned),
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            options.cache,
            NodeCacheConfig {
                active_chainstate_bytes: 256 * 1024 * 1024,
                background_chainstate_bytes: 512 * 1024 * 1024,
                bulk_validation_bytes: 1024 * 1024 * 1024,
            }
        );

        for arguments in [
            vec!["--chainstate-cache-bytes", "268435456"],
            vec![
                "--data-dir",
                "/tmp/rbtc-cache-parser",
                "--chainstate-cache-bytes",
                "67108863",
            ],
            vec![
                "--data-dir",
                "/tmp/rbtc-cache-parser",
                "--bulk-validation-cache-bytes",
                "68719476737",
            ],
            vec![
                "--data-dir",
                "/tmp/rbtc-cache-parser",
                "--background-chainstate-cache-bytes",
                "invalid",
            ],
        ] {
            assert!(parse_options(arguments.into_iter().map(str::to_owned)).is_err());
        }
    }

    #[test]
    fn parses_and_bounds_peer_and_mempool_resource_budgets() {
        let options = parse_options(
            [
                "--network",
                "regtest",
                "--data-dir",
                "/tmp/rbtc-resource-parser",
                "--automatic-hot-standbys",
                "12",
                "--mempool-max-transactions",
                "4096",
                "--mempool-max-bytes",
                "314572800",
            ]
            .into_iter()
            .map(str::to_owned),
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            options.resources,
            NodeResourceConfig {
                automatic_hot_standbys: 12,
                mempool_max_transactions: 4_096,
                mempool_max_bytes: 300 * 1024 * 1024,
            }
        );

        for arguments in [
            vec!["--automatic-hot-standbys", "1"],
            vec![
                "--data-dir",
                "/tmp/rbtc-resource-parser",
                "--automatic-hot-standbys",
                "17",
            ],
            vec![
                "--data-dir",
                "/tmp/rbtc-resource-parser",
                "--mempool-max-transactions",
                "0",
            ],
            vec![
                "--data-dir",
                "/tmp/rbtc-resource-parser",
                "--mempool-max-transactions",
                "300001",
            ],
            vec![
                "--data-dir",
                "/tmp/rbtc-resource-parser",
                "--mempool-max-bytes",
                "3999999",
            ],
            vec![
                "--data-dir",
                "/tmp/rbtc-resource-parser",
                "--mempool-max-bytes",
                "1073741825",
            ],
        ] {
            assert!(parse_options(arguments.into_iter().map(str::to_owned)).is_err());
        }
    }

    #[test]
    fn parses_and_bounds_structured_log_rotation() {
        let options = parse_options(
            [
                "--network",
                "regtest",
                "--data-dir",
                "/tmp/rbtc-log-parser",
                "--log-level",
                "debug",
                "--log-dir",
                "/tmp/rbtc-explicit-logs",
                "--log-max-bytes",
                "2097152",
                "--log-max-files",
                "3",
            ]
            .into_iter()
            .map(str::to_owned),
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            options.logging,
            NodeLogConfig {
                level: LogLevel::Debug,
                directory: Some(PathBuf::from("/tmp/rbtc-explicit-logs")),
                max_file_bytes: 2 * 1024 * 1024,
                max_files: 3,
            }
        );

        let defaults = parse_options(
            ["--data-dir", "/tmp/rbtc-default-logs"]
                .into_iter()
                .map(str::to_owned),
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            defaults.logging.directory,
            Some(PathBuf::from("/tmp/rbtc-default-logs/logs"))
        );

        for arguments in [
            vec!["--log-level", "trace"],
            vec!["--log-max-bytes", "1048575"],
            vec!["--log-max-bytes", "1073741825"],
            vec!["--log-max-files", "1"],
            vec!["--log-max-files", "21"],
        ] {
            assert!(parse_options(arguments.into_iter().map(str::to_owned)).is_err());
        }
    }

    #[test]
    fn strict_network_config_is_bounded_and_cli_groups_replace_file_values() {
        let directory = TempDir::new().unwrap();
        let config_path = directory.path().join("rbtc.conf");
        fs::write(
            &config_path,
            "network=bitcoin\n\
             data_dir=/from-global\n\
             connect=127.0.0.1:8333\n\
             once=true\n\
             automatic_hot_standbys=10\n\
             mempool_max_transactions=2048\n\
             mempool_max_bytes=314572800\n\
             log_level=warn\n\
             log_max_bytes=2097152\n\
             log_max_files=3\n\
             prune_blocks=500\n\
             minimum_free_bytes=2147483648\n\
             explorer_listen=127.0.0.1:3000\n\
             rpc_auth_token_file=/secret/token\n\
             [testnet4]\n\
             data_dir=/from-testnet4\n\
             connect=127.0.0.2:48333\n\
             prune_blocks=600\n",
        )
        .unwrap();
        let options = parse_options(
            vec![
                "--config".to_owned(),
                config_path.display().to_string(),
                "--network".to_owned(),
                "testnet4".to_owned(),
                "--data-dir".to_owned(),
                "/from-cli".to_owned(),
                "--connect".to_owned(),
                "127.0.0.3:48333".to_owned(),
                "--no-once".to_owned(),
            ]
            .into_iter(),
        )
        .unwrap()
        .unwrap();
        assert_eq!(options.network, Network::Testnet4);
        assert_eq!(options.data_dir, Some(PathBuf::from("/from-cli")));
        assert_eq!(
            options.remotes,
            vec!["127.0.0.3:48333".parse::<SocketAddr>().unwrap()]
        );
        assert!(!options.once);
        assert_eq!(
            options.resources,
            NodeResourceConfig {
                automatic_hot_standbys: 10,
                mempool_max_transactions: 2_048,
                mempool_max_bytes: 300 * 1024 * 1024,
            }
        );
        assert_eq!(options.logging.level, LogLevel::Warn);
        assert_eq!(options.logging.max_file_bytes, 2 * 1024 * 1024);
        assert_eq!(options.logging.max_files, 3);
        assert_eq!(options.ledger_retention.max_blocks, 600);
        assert_eq!(options.minimum_free_bytes, 2 * 1024 * 1024 * 1024);
        let summary = startup_configuration_summary(&options);
        assert!(summary.contains("prune_blocks=600"));
        assert!(summary.contains("automatic_hot_standbys=10"));
        assert!(summary.contains("mempool_max_transactions=2048"));
        assert!(summary.contains("api=true rpc=true wallet=false"));
        assert!(!summary.contains("/secret/token"));

        fs::write(&config_path, "network=regtest\nunknown_limit=1\n").unwrap();
        let Err(error) =
            parse_options(["--config".to_owned(), config_path.display().to_string()].into_iter())
        else {
            panic!("unknown config keys must fail");
        };
        assert!(error.contains("unknown config key"));
    }

    #[test]
    fn typed_embedded_config_maps_all_persistent_subsystems() {
        let seed = NodeDnsSeed::new("Seed.Example.", 18_444).unwrap();
        assert_eq!(seed.host(), "seed.example.");
        assert_eq!(seed.port(), 18_444);
        let mut config = NodeConfig::new(Network::Regtest, "/tmp/rbtc-typed-config");
        config.preferred_peers = vec!["127.0.0.1:18444".parse().unwrap()];
        config.dns_seeds = NodeDnsSeedPolicy::Custom(vec![seed]);
        config.once = true;
        config.mempool_full_rbf = true;
        config.cache = NodeCacheConfig {
            active_chainstate_bytes: 256 * 1024 * 1024,
            background_chainstate_bytes: 512 * 1024 * 1024,
            bulk_validation_bytes: 1024 * 1024 * 1024,
        };
        config.resources = NodeResourceConfig {
            automatic_hot_standbys: 4,
            mempool_max_transactions: 4_096,
            mempool_max_bytes: 300 * 1024 * 1024,
        };
        config.logging = NodeLogConfig {
            level: LogLevel::Debug,
            directory: Some(PathBuf::from("/tmp/rbtc-typed-logs")),
            max_file_bytes: 2 * 1024 * 1024,
            max_files: 3,
        };
        config.storage = NodeStorageConfig {
            prune_blocks: 576,
            prune_bytes: 768 * 1024 * 1024,
            minimum_free_bytes: 2 * 1024 * 1024 * 1024,
        };
        config.api = Some(NodeApiConfig {
            listen: "127.0.0.1:0".parse().unwrap(),
            rpc_auth_token: Some(PathBuf::from("/tmp/rbtc-rpc-token")),
            wallet: Some(NodeWalletApiConfig {
                descriptors: PathBuf::from("/tmp/rbtc-wallet-descriptors"),
                auth_token: PathBuf::from("/tmp/rbtc-wallet-token"),
            }),
        });
        config.consensus.version_bits = vec!["taproot:0:1:0".to_owned()];
        config.consensus.activation_heights = vec!["segwit@0".to_owned()];
        config.assumeutxo_validation =
            Some(NodeAssumeUtxoValidation::Background(NodeAssumeUtxoConfig {
                validation_dir: PathBuf::from("/tmp/rbtc-typed-validation"),
                limits: NodeValidationConfig {
                    max_blocks_per_batch: 252,
                    pause_between_batches: Duration::from_millis(25),
                    quick_repair: false,
                },
                cleanup_on_success: true,
            }));

        config.validate().unwrap();
        let options = config.into_options().unwrap();
        assert_eq!(options.remotes, vec!["127.0.0.1:18444".parse().unwrap()]);
        assert_eq!(options.dns_seeds.unwrap()[0].host, "seed.example.");
        assert!(options.mempool_full_rbf);
        assert_eq!(options.cache.active_chainstate_bytes, 256 * 1024 * 1024);
        assert_eq!(
            options.resources,
            NodeResourceConfig {
                automatic_hot_standbys: 4,
                mempool_max_transactions: 4_096,
                mempool_max_bytes: 300 * 1024 * 1024,
            }
        );
        assert_eq!(options.logging.level, LogLevel::Debug);
        assert_eq!(options.ledger_retention.max_blocks, 576);
        assert_eq!(options.minimum_free_bytes, 2 * 1024 * 1024 * 1024);
        assert_eq!(
            options.background_assumeutxo,
            Some(PathBuf::from("/tmp/rbtc-typed-validation"))
        );
        assert!(options.cleanup_validation_dir);
        assert_eq!(options.validation_limits.max_blocks_per_batch, 252);
        assert_eq!(
            options.validation_limits.pause_between_batches,
            Duration::from_millis(25)
        );
        assert!(!options.validation_limits.quick_repair);
        assert!(options.explorer_listen.is_some());
        assert!(options.rpc_auth_token_file.is_some());
        assert!(options.wallet_api_files.is_some());
    }

    #[test]
    fn typed_embedded_config_rejects_cross_field_and_budget_errors() {
        let mut config = NodeConfig::new(Network::Regtest, "/tmp/rbtc-invalid-config");
        config.cache.active_chainstate_bytes = MIN_CHAINSTATE_CACHE_BYTES - 1;
        assert!(
            config
                .validate()
                .unwrap_err()
                .to_string()
                .contains("active chainstate cache")
        );

        config.cache = NodeCacheConfig::default();
        config.resources.mempool_max_transactions = 0;
        assert!(
            config
                .validate()
                .unwrap_err()
                .to_string()
                .contains("mempool transaction target")
        );

        config.resources = NodeResourceConfig::default();
        config.api = Some(NodeApiConfig {
            listen: "0.0.0.0:3000".parse().unwrap(),
            rpc_auth_token: None,
            wallet: None,
        });
        assert!(
            config
                .validate()
                .unwrap_err()
                .to_string()
                .contains("loopback")
        );

        config.api = None;
        config.once = true;
        config.assumeutxo_validation =
            Some(NodeAssumeUtxoValidation::Complete(NodeAssumeUtxoConfig {
                validation_dir: PathBuf::from("/tmp/rbtc-invalid-validation"),
                limits: NodeValidationConfig::default(),
                cleanup_on_success: false,
            }));
        assert!(
            config
                .validate()
                .unwrap_err()
                .to_string()
                .contains("sequential AssumeUTXO")
        );
    }

    #[test]
    fn embedded_builder_owns_bounded_persistent_node_configuration() {
        let first: SocketAddr = "127.0.0.1:18444".parse().unwrap();
        let second: SocketAddr = "127.0.0.2:18444".parse().unwrap();
        let options = NodeBuilder::new(Network::Regtest, "/tmp/rbtc-embedded")
            .connect(first)
            .connect(first)
            .connect(second)
            .once(true)
            .mempool_full_rbf(true)
            .resources(NodeResourceConfig {
                automatic_hot_standbys: 2,
                mempool_max_transactions: 1_024,
                mempool_max_bytes: 64 * 1024 * 1024,
            })
            .ledger_retention(576, DEFAULT_MAX_BYTES)
            .into_options()
            .unwrap();
        assert_eq!(options.network, Network::Regtest);
        assert_eq!(options.data_dir, Some(PathBuf::from("/tmp/rbtc-embedded")));
        assert_eq!(options.remotes, vec![first, second]);
        assert!(options.once);
        assert!(options.mempool_full_rbf);
        assert_eq!(options.resources.automatic_hot_standbys, 2);
        assert_eq!(options.resources.mempool_max_transactions, 1_024);
        assert_eq!(options.ledger_retention.max_blocks, 576);
        assert_eq!(options.ledger_retention.max_bytes, DEFAULT_MAX_BYTES);

        assert!(matches!(
            NodeBuilder::new(Network::Regtest, "")
                .into_options()
                .err()
                .unwrap(),
            NodeError::InvalidConfig(_)
        ));
        assert!(matches!(
            NodeBuilder::new(Network::Regtest, "/tmp/rbtc-embedded")
                .ledger_retention(287, DEFAULT_MAX_BYTES)
                .into_options()
                .err()
                .unwrap(),
            NodeError::InvalidConfig(_)
        ));
        assert!(matches!(
            NodeBuilder::new(Network::Regtest, "/tmp/rbtc-embedded")
                .ledger_retention(576, MIN_PRUNE_MAX_BYTES - 1)
                .into_options()
                .err()
                .unwrap(),
            NodeError::InvalidConfig(_)
        ));
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
        assert!(!options.mempool_full_rbf);
        assert!(!options.once);
        assert_eq!(options.network_execution, NetworkExecutionMode::Persistent);
        assert!(options.explorer_listen.is_none());
        assert!(options.wallet_api_files.is_none());
        assert_eq!(
            options.deployments,
            DeploymentConfig::for_network(Network::Regtest)
        );
        assert_eq!(options.ibd_policy, IbdPolicy::for_network(Network::Regtest));
        assert!(options.snapshot.is_none());
        assert!(!completed_validating_session(&options));

        assert_eq!(
            parse_options(
                [
                    "--connect",
                    "127.0.0.1:18444",
                    "--network",
                    "regtest",
                    "--mempool-full-rbf",
                ]
                .into_iter()
                .map(str::to_owned),
            )
            .err()
            .unwrap(),
            "--mempool-full-rbf requires --data-dir"
        );
        let full_rbf = parse_options(
            [
                "--connect",
                "127.0.0.1:18444",
                "--network",
                "regtest",
                "--data-dir",
                "/tmp/rbtc-full-rbf-parse-only",
                "--mempool-full-rbf",
            ]
            .into_iter()
            .map(str::to_owned),
        )
        .unwrap()
        .unwrap();
        assert!(full_rbf.mempool_full_rbf);

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
        assert_eq!(served.deployments.consensus_id().len(), 50);
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
        let SnapshotActivationOptions::Rbtc {
            path,
            height,
            block_hash,
            utxo_count,
            records_bytes,
            records_sha256,
        } = options.snapshot.unwrap()
        else {
            panic!("expected rBTC snapshot options");
        };
        assert_eq!(path, PathBuf::from("/tmp/base.rbtc"));
        assert_eq!(height, 100);
        assert_eq!(block_hash, BlockHash::from_str(hash).unwrap());
        assert_eq!(utxo_count, 1);
        assert_eq!(records_bytes, 66);
        assert_eq!(records_sha256, digest);

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

    #[test]
    fn parses_core31_snapshot_activation_without_manual_identity() {
        let options = parse_options(
            [
                "--network",
                "testnet4",
                "--data-dir",
                "/tmp/rbtc-core-snapshot-parser",
                "--core-assumeutxo-snapshot",
                "/tmp/utxo.dat",
            ]
            .into_iter()
            .map(str::to_owned),
        )
        .unwrap()
        .unwrap();
        assert!(matches!(
            options.snapshot,
            Some(SnapshotActivationOptions::Core(path))
                if path == PathBuf::from("/tmp/utxo.dat")
        ));
        assert!(
            parse_options(
                [
                    "--network",
                    "testnet4",
                    "--core-assumeutxo-snapshot",
                    "/tmp/utxo.dat",
                ]
                .into_iter()
                .map(str::to_owned),
            )
            .is_err()
        );
    }

    #[test]
    fn parses_only_offline_utxo_activity_reporting() {
        let options = parse_options(
            [
                "--network",
                "testnet4",
                "--data-dir",
                "/tmp/rbtc-activity-report",
                "--utxo-activity-report",
            ]
            .into_iter()
            .map(str::to_owned),
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            options.offline_action,
            Some(OfflineAction::UtxoActivityReport)
        );
        assert_eq!(
            options.data_dir,
            Some(PathBuf::from("/tmp/rbtc-activity-report"))
        );
        for arguments in [
            vec!["--network", "testnet4", "--utxo-activity-report"],
            vec![
                "--network",
                "testnet4",
                "--data-dir",
                "/tmp/rbtc-activity-report",
                "--utxo-activity-report",
                "--once",
            ],
            vec![
                "--network",
                "testnet4",
                "--data-dir",
                "/tmp/rbtc-activity-report",
                "--utxo-activity-report",
                "--connect",
                "127.0.0.1:48333",
            ],
        ] {
            assert!(parse_options(arguments.into_iter().map(str::to_owned)).is_err());
        }
        assert_eq!(format_percentage(1, 4), "25.00000");
        assert_eq!(format_percentage(0, 0), "0.00000");
        assert_eq!(
            spent_age_quantile(&[(0, 1), (2, 2), (10, 1)], 4, 500),
            Some(2)
        );
        assert_eq!(
            spent_age_quantile(&[(0, 1), (2, 2), (10, 1)], 4, 999),
            Some(10)
        );
        assert_eq!(spent_age_quantile(&[], 0, 990), None);
        assert_eq!(format_tier_probes_per_spend(99, 100), "1.01000");
        assert_eq!(format_tier_probes_per_spend(0, 0), "unavailable");
    }

    #[test]
    fn parses_only_bounded_read_only_storage_audits() {
        let options = parse_options(
            [
                "--network",
                "testnet4",
                "--data-dir",
                "/tmp/rbtc-storage-audit",
                "--verify-storage",
                "--verify-storage-max-segments",
                "512",
                "--verify-storage-max-bytes",
                "2147483648",
            ]
            .into_iter()
            .map(str::to_owned),
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            options.offline_action,
            Some(OfflineAction::VerifyStorage {
                max_segments: 512,
                max_bytes: 2_147_483_648,
            })
        );

        for arguments in [
            vec!["--network", "testnet4", "--verify-storage"],
            vec![
                "--network",
                "testnet4",
                "--data-dir",
                "/tmp/rbtc-storage-audit",
                "--verify-storage-max-segments",
                "10",
            ],
            vec![
                "--network",
                "testnet4",
                "--data-dir",
                "/tmp/rbtc-storage-audit",
                "--verify-storage",
                "--verify-storage-max-segments",
                "4097",
            ],
            vec![
                "--network",
                "testnet4",
                "--data-dir",
                "/tmp/rbtc-storage-audit",
                "--verify-storage",
                "--connect",
                "127.0.0.1:48333",
            ],
            vec![
                "--network",
                "testnet4",
                "--data-dir",
                "/tmp/rbtc-storage-audit",
                "--verify-storage",
                "--prune-blocks",
                "576",
            ],
        ] {
            assert!(parse_options(arguments.into_iter().map(str::to_owned)).is_err());
        }
    }

    #[test]
    fn parses_only_bounded_exclusive_chain_verification() {
        let options = parse_options(
            [
                "--network",
                "testnet4",
                "--data-dir",
                "/tmp/rbtc-chain-audit",
                "--verify-chain",
                "--verify-chain-depth",
                "576",
                "--verify-chain-max-block-bytes",
                "2147483648",
            ]
            .into_iter()
            .map(str::to_owned),
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            options.offline_action,
            Some(OfflineAction::VerifyChain {
                depth: 576,
                max_block_bytes: 2_147_483_648,
            })
        );
        for arguments in [
            vec!["--network", "testnet4", "--verify-chain"],
            vec![
                "--network",
                "testnet4",
                "--data-dir",
                "/tmp/rbtc-chain-audit",
                "--verify-chain-depth",
                "10",
            ],
            vec![
                "--network",
                "testnet4",
                "--data-dir",
                "/tmp/rbtc-chain-audit",
                "--verify-chain",
                "--verify-chain-depth",
                "1009",
            ],
            vec![
                "--network",
                "testnet4",
                "--data-dir",
                "/tmp/rbtc-chain-audit",
                "--verify-chain",
                "--connect",
                "127.0.0.1:48333",
            ],
            vec![
                "--network",
                "testnet4",
                "--data-dir",
                "/tmp/rbtc-chain-audit",
                "--verify-chain",
                "--verify-storage",
            ],
        ] {
            assert!(parse_options(arguments.into_iter().map(str::to_owned)).is_err());
        }
    }

    #[test]
    fn parses_only_separate_resumable_freezer_reindex() {
        let options = parse_options(
            [
                "--network",
                "regtest",
                "--data-dir",
                "/tmp/rbtc-reindex-source",
                "--reindex-from-freezer",
                "/tmp/rbtc-reindex-output",
                "--validation-batch-size",
                "64",
                "--validation-deferred-repair",
            ]
            .into_iter()
            .map(str::to_owned),
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            options.offline_action,
            Some(OfflineAction::ReindexFromFreezer {
                output: PathBuf::from("/tmp/rbtc-reindex-output"),
            })
        );
        assert_eq!(options.validation_limits.max_blocks_per_batch, 64);
        assert!(!options.validation_limits.quick_repair);
        for arguments in [
            vec![
                "--network",
                "regtest",
                "--reindex-from-freezer",
                "/tmp/output",
            ],
            vec![
                "--network",
                "regtest",
                "--data-dir",
                "/tmp/source",
                "--reindex-from-freezer",
                "/tmp/output",
                "--connect",
                "127.0.0.1:18444",
            ],
            vec![
                "--network",
                "regtest",
                "--data-dir",
                "/tmp/source",
                "--reindex-from-freezer",
                "/tmp/output",
                "--verify-chain",
            ],
        ] {
            assert!(parse_options(arguments.into_iter().map(str::to_owned)).is_err());
        }
    }

    #[test]
    fn parses_only_separate_authenticated_peer_reindex() {
        let options = parse_options(
            [
                "--network",
                "regtest",
                "--data-dir",
                "/tmp/rbtc-peer-reindex-source",
                "--reindex-chainstate",
                "/tmp/rbtc-peer-reindex-output",
                "--connect",
                "127.0.0.1:18444",
                "--validation-batch-size",
                "64",
                "--bulk-validation-cache-bytes",
                "8589934592",
            ]
            .into_iter()
            .map(str::to_owned),
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            options.offline_action,
            Some(OfflineAction::ReindexChainstate {
                output: PathBuf::from("/tmp/rbtc-peer-reindex-output"),
            })
        );
        assert_eq!(options.validation_limits.max_blocks_per_batch, 64);
        assert_eq!(options.cache.bulk_validation_bytes, 8_589_934_592);
        for arguments in [
            vec![
                "--network",
                "regtest",
                "--reindex-chainstate",
                "/tmp/output",
                "--connect",
                "127.0.0.1:18444",
            ],
            vec![
                "--network",
                "regtest",
                "--data-dir",
                "/tmp/source",
                "--reindex-chainstate",
                "/tmp/output",
                "--no-dns-seeds",
            ],
            vec![
                "--network",
                "regtest",
                "--data-dir",
                "/tmp/source",
                "--reindex-chainstate",
                "/tmp/output",
                "--connect",
                "127.0.0.1:18444",
                "--once",
            ],
        ] {
            assert!(parse_options(arguments.into_iter().map(str::to_owned)).is_err());
        }
    }

    #[test]
    fn parses_two_phase_offline_manual_pruning() {
        let dry_run = parse_options(
            [
                "--network",
                "bitcoin",
                "--data-dir",
                "/tmp/rbtc-manual-prune",
                "--prune-through-height",
                "900000",
            ]
            .into_iter()
            .map(str::to_owned),
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            dry_run.offline_action,
            Some(OfflineAction::PruneStorage {
                through_height: 900_000,
                plan_token: None,
            })
        );
        let token = "AB".repeat(32);
        let apply = parse_options(
            [
                "--network".to_owned(),
                "bitcoin".to_owned(),
                "--data-dir".to_owned(),
                "/tmp/rbtc-manual-prune".to_owned(),
                "--prune-through-height".to_owned(),
                "900000".to_owned(),
                "--apply-prune-token".to_owned(),
                token,
            ]
            .into_iter(),
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            apply.offline_action,
            Some(OfflineAction::PruneStorage {
                through_height: 900_000,
                plan_token: Some("ab".repeat(32)),
            })
        );

        for arguments in [
            vec!["--network", "bitcoin", "--prune-through-height", "900000"],
            vec![
                "--network",
                "bitcoin",
                "--data-dir",
                "/tmp/rbtc-manual-prune",
                "--apply-prune-token",
                "abababababababababababababababababababababababababababababababab",
            ],
            vec![
                "--network",
                "bitcoin",
                "--data-dir",
                "/tmp/rbtc-manual-prune",
                "--prune-through-height",
                "900000",
                "--apply-prune-token",
                "bad",
            ],
            vec![
                "--network",
                "bitcoin",
                "--data-dir",
                "/tmp/rbtc-manual-prune",
                "--prune-through-height",
                "900000",
                "--verify-storage",
            ],
        ] {
            assert!(parse_options(arguments.into_iter().map(str::to_owned)).is_err());
        }
    }

    #[tokio::test]
    async fn storage_audit_command_does_not_open_or_modify_node_databases() {
        let directory = TempDir::new().unwrap();
        drop(DataDirectoryLock::acquire(directory.path(), Network::Regtest).unwrap());
        let ledger =
            PrunedBlockLedger::open(directory.path().join("blocks"), LedgerRetention::default())
                .unwrap();
        ledger.append(1, &[vec![1, 2, 3]]).unwrap();
        drop(ledger);
        let marker_path = directory.path().join(DATA_DIRECTORY_LOCK_FILE);
        let index_path = directory.path().join("blocks").join("ledger-index.json");
        let marker = fs::read(&marker_path).unwrap();
        let index = fs::read(&index_path).unwrap();

        let options = parse_options(
            [
                "--network".to_owned(),
                "regtest".to_owned(),
                "--data-dir".to_owned(),
                directory.path().display().to_string(),
                "--verify-storage".to_owned(),
            ]
            .into_iter(),
        )
        .unwrap()
        .unwrap();
        run_with_nonce(options, 1).await.unwrap();

        assert_eq!(fs::read(marker_path).unwrap(), marker);
        assert_eq!(fs::read(index_path).unwrap(), index);
        assert!(!directory.path().join("chainstate.redb").exists());
        assert!(!directory.path().join("logs").exists());
    }

    #[tokio::test]
    async fn offline_chain_verification_requires_existing_state_and_accepts_a_clean_suffix() {
        let absent = TempDir::new().unwrap().path().join("absent");
        let absent_options = parse_options(
            [
                "--network".to_owned(),
                "regtest".to_owned(),
                "--data-dir".to_owned(),
                absent.display().to_string(),
                "--verify-chain".to_owned(),
            ]
            .into_iter(),
        )
        .unwrap()
        .unwrap();
        assert!(run_with_nonce(absent_options, 1).await.is_err());
        assert!(!absent.exists());

        let directory = TempDir::new().unwrap();
        drop(DataDirectoryLock::acquire(directory.path(), Network::Regtest).unwrap());
        let genesis = bitcoin::blockdata::constants::genesis_block(Network::Regtest);
        let block = regtest_block_at_height(
            genesis.block_hash(),
            genesis.header.time.saturating_add(1),
            1,
        );
        let block_hash = block.block_hash();
        let headers = RedbHeaderStore::open(directory.path().join("headers.redb")).unwrap();
        headers.append(block.header).unwrap();
        drop(headers);
        let chainstate =
            RedbChainStore::open(directory.path().join("chainstate.redb"), Network::Regtest)
                .unwrap();
        chainstate
            .commit_connect(
                genesis.block_hash(),
                rbtc::execution_store::ExecutionTip {
                    height: 1,
                    hash: block_hash,
                },
                &[],
                &[],
                &[],
            )
            .unwrap();
        drop(chainstate);
        let ledger =
            PrunedBlockLedger::open(directory.path().join("blocks"), LedgerRetention::default())
                .unwrap();
        ledger.append(1, &[serialize(&block)]).unwrap();
        drop(ledger);
        publish_data_format_manifest(directory.path(), Network::Regtest).unwrap();
        let options = parse_options(
            [
                "--network".to_owned(),
                "regtest".to_owned(),
                "--data-dir".to_owned(),
                directory.path().display().to_string(),
                "--verify-chain".to_owned(),
                "--verify-chain-depth".to_owned(),
                "1".to_owned(),
            ]
            .into_iter(),
        )
        .unwrap()
        .unwrap();
        run_with_nonce(options, 1).await.unwrap();
        assert!(!directory.path().join("peers.redb").exists());
        assert!(!directory.path().join("logs").exists());
    }

    #[tokio::test]
    async fn complete_freezer_reindex_builds_and_promotes_a_separate_chainstate() {
        let source = TempDir::new().unwrap();
        let output_parent = TempDir::new().unwrap();
        let output = output_parent.path().join("rebuilt");
        drop(DataDirectoryLock::acquire(source.path(), Network::Regtest).unwrap());
        let genesis = bitcoin::blockdata::constants::genesis_block(Network::Regtest);
        let first = regtest_block_at_height(
            genesis.block_hash(),
            genesis.header.time.saturating_add(1),
            1,
        );
        let second =
            regtest_block_at_height(first.block_hash(), first.header.time.saturating_add(1), 2);
        let headers = RedbHeaderStore::open(source.path().join("headers.redb")).unwrap();
        headers
            .append_batch(&[first.header, second.header])
            .unwrap();
        drop(headers);
        let ledger =
            PrunedBlockLedger::open(source.path().join("blocks"), LedgerRetention::default())
                .unwrap();
        ledger
            .append(1, &[serialize(&first), serialize(&second)])
            .unwrap();
        drop(ledger);
        publish_data_format_manifest(source.path(), Network::Regtest).unwrap();

        let options = parse_options(
            [
                "--network".to_owned(),
                "regtest".to_owned(),
                "--data-dir".to_owned(),
                source.path().display().to_string(),
                "--reindex-from-freezer".to_owned(),
                output.display().to_string(),
                "--validation-batch-size".to_owned(),
                "1".to_owned(),
                "--txindex".to_owned(),
                "--spent-output-index".to_owned(),
                "--block-filter-index".to_owned(),
            ]
            .into_iter(),
        )
        .unwrap()
        .unwrap();
        run_with_nonce(options, 1).await.unwrap();

        let rebuilt =
            RedbChainStore::open(output.join("chainstate.redb"), Network::Regtest).unwrap();
        assert_eq!(
            rebuilt.execution().tip().unwrap(),
            rbtc::execution_store::ExecutionTip {
                height: 2,
                hash: second.block_hash(),
            }
        );
        assert_eq!(rebuilt.execution().validation_target().unwrap(), None);
        assert_eq!(rebuilt.tier_stats().unwrap().hot, 2);
        assert!(rebuilt.undos().get(first.block_hash()).unwrap().is_some());
        assert!(rebuilt.undos().get(second.block_hash()).unwrap().is_some());
        drop(rebuilt);
        assert!(!output.join(VALIDATION_OWNER_FILE).exists());
        assert!(!validate_data_format_manifest(&output, Network::Regtest).unwrap());
        let rebuilt_ledger = PrunedBlockLedger::open_persisted(output.join("blocks")).unwrap();
        assert_eq!(
            rebuilt_ledger.retained_ranges().unwrap(),
            vec![(1, 1), (2, 2)]
        );
        for (file, kind) in [
            ("txindex.redb", AuxiliaryIndexKind::Transaction),
            ("spent-output-index.redb", AuxiliaryIndexKind::SpentOutput),
            ("basic-filter-index.redb", AuxiliaryIndexKind::BasicFilter),
        ] {
            let index =
                RedbAuxiliaryIndex::open(output.join(file), Network::Regtest, kind).unwrap();
            assert_eq!(index.tip().unwrap().height, 2);
            if kind == AuxiliaryIndexKind::Transaction {
                assert_eq!(
                    index
                        .transaction(first.txdata[0].compute_txid())
                        .unwrap()
                        .unwrap()
                        .height,
                    1
                );
            }
            if kind == AuxiliaryIndexKind::BasicFilter {
                assert!(
                    index
                        .basic_filter_by_hash(second.block_hash())
                        .unwrap()
                        .is_some()
                );
            }
        }
    }

    #[tokio::test]
    async fn authenticated_peer_reindex_builds_and_promotes_to_header_target() {
        let source = TempDir::new().unwrap();
        let output_parent = TempDir::new().unwrap();
        let output = output_parent.path().join("rebuilt");
        drop(DataDirectoryLock::acquire(source.path(), Network::Regtest).unwrap());
        let genesis = bitcoin::blockdata::constants::genesis_block(Network::Regtest);
        let first = regtest_block_at_height(
            genesis.block_hash(),
            genesis.header.time.saturating_add(1),
            1,
        );
        let second =
            regtest_block_at_height(first.block_hash(), first.header.time.saturating_add(1), 2);
        let headers = RedbHeaderStore::open(source.path().join("headers.redb")).unwrap();
        headers
            .append_batch(&[first.header, second.header])
            .unwrap();
        drop(headers);
        publish_data_format_manifest(source.path(), Network::Regtest).unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let remote = listener.local_addr().unwrap();
        let first_hash = first.block_hash();
        let second_hash = second.block_hash();
        let server = tokio::spawn(async move {
            let (mut peer, _) = accept_peer(listener, peer_version(81)).await;
            respond_to_getaddr(&mut peer).await;
            match peer.read_message().await.unwrap().into_payload() {
                NetworkMessage::GetHeaders(request) => {
                    assert_eq!(request.locator_hashes.first(), Some(&second_hash));
                    peer.write_message(NetworkMessage::Headers(Vec::new()))
                        .await
                        .unwrap();
                }
                message => panic!("expected peer-reindex getheaders, got {message:?}"),
            }
            match peer.read_message().await.unwrap().into_payload() {
                NetworkMessage::GetData(inventory) => {
                    assert_eq!(
                        inventory,
                        vec![
                            bitcoin::p2p::message_blockdata::Inventory::WitnessBlock(first_hash),
                            bitcoin::p2p::message_blockdata::Inventory::WitnessBlock(second_hash),
                        ]
                    );
                    peer.write_message(NetworkMessage::Block(first))
                        .await
                        .unwrap();
                    peer.write_message(NetworkMessage::Block(second))
                        .await
                        .unwrap();
                }
                message => panic!("expected peer-reindex block request, got {message:?}"),
            }
        });
        let options = parse_options(
            [
                "--network".to_owned(),
                "regtest".to_owned(),
                "--data-dir".to_owned(),
                source.path().display().to_string(),
                "--reindex-chainstate".to_owned(),
                output.display().to_string(),
                "--connect".to_owned(),
                remote.to_string(),
            ]
            .into_iter(),
        )
        .unwrap()
        .unwrap();
        run_with_nonce(options, 79).await.unwrap();
        server.await.unwrap();

        let rebuilt =
            RedbChainStore::open(output.join("chainstate.redb"), Network::Regtest).unwrap();
        assert_eq!(
            rebuilt.execution().tip().unwrap(),
            rbtc::execution_store::ExecutionTip {
                height: 2,
                hash: second_hash,
            }
        );
        assert_eq!(rebuilt.execution().validation_target().unwrap(), None);
        assert_eq!(rebuilt.tier_stats().unwrap().hot, 2);
        assert!(rebuilt.undos().get(first_hash).unwrap().is_some());
        assert!(rebuilt.undos().get(second_hash).unwrap().is_some());
        drop(rebuilt);
        assert!(!output.join(VALIDATION_OWNER_FILE).exists());
        assert!(!validate_data_format_manifest(&output, Network::Regtest).unwrap());
        assert_eq!(
            PrunedBlockLedger::open_persisted(output.join("blocks"))
                .unwrap()
                .retained_ranges()
                .unwrap(),
            vec![(1, 2)]
        );
    }

    #[tokio::test]
    async fn manual_prune_command_requires_dry_run_token_and_applies_exact_plan() {
        let directory = TempDir::new().unwrap();
        drop(DataDirectoryLock::acquire(directory.path(), Network::Regtest).unwrap());
        let blocks_path = directory.path().join("blocks");
        let retention = LedgerRetention {
            max_blocks: 500,
            max_bytes: 10_000_000,
            slots: 3,
        };
        let ledger = PrunedBlockLedger::open(&blocks_path, retention).unwrap();
        ledger.append(1, &vec![vec![1]; 150]).unwrap();
        ledger.append(151, &vec![vec![2]; 150]).unwrap();
        ledger.append(301, &vec![vec![3]; 150]).unwrap();
        drop(ledger);
        let index_path = blocks_path.join("ledger-index.json");
        let index_before = fs::read(&index_path).unwrap();

        let dry_run = parse_options(
            [
                "--network".to_owned(),
                "regtest".to_owned(),
                "--data-dir".to_owned(),
                directory.path().display().to_string(),
                "--prune-through-height".to_owned(),
                "150".to_owned(),
            ]
            .into_iter(),
        )
        .unwrap()
        .unwrap();
        run_with_nonce(dry_run, 1).await.unwrap();
        assert_eq!(fs::read(&index_path).unwrap(), index_before);
        let plan =
            PrunedBlockLedger::plan_prune_read_only(&blocks_path, 150, MIN_PRUNE_RETENTION_BLOCKS)
                .unwrap();
        assert_eq!(plan.effective_through_height, Some(150));
        assert_eq!(plan.first_retained_height, Some(151));

        let apply = parse_options(
            [
                "--network".to_owned(),
                "regtest".to_owned(),
                "--data-dir".to_owned(),
                directory.path().display().to_string(),
                "--prune-through-height".to_owned(),
                "150".to_owned(),
                "--apply-prune-token".to_owned(),
                plan.plan_token,
            ]
            .into_iter(),
        )
        .unwrap()
        .unwrap();
        run_with_nonce(apply, 1).await.unwrap();

        let reopened = PrunedBlockLedger::open_persisted(&blocks_path).unwrap();
        assert_eq!(
            reopened.retained_ranges().unwrap(),
            vec![(151, 300), (301, 450)]
        );
        assert!(!directory.path().join("chainstate.redb").exists());
        assert!(!directory.path().join("logs").exists());
    }

    #[test]
    fn manual_prune_refuses_to_strand_a_lagging_enabled_index() {
        let directory = TempDir::new().unwrap();
        let ledger =
            PrunedBlockLedger::open(directory.path().join("blocks"), LedgerRetention::default())
                .unwrap();
        ledger.append(1, &vec![vec![1]; 100]).unwrap();
        ledger.append(101, &vec![vec![2]; 300]).unwrap();
        let plan = ledger.plan_prune_through(100, 288).unwrap();
        RedbExplorerIndex::open(directory.path().join("explorer.redb"), Network::Regtest).unwrap();
        let error =
            validate_manual_prune_indexes(directory.path(), Network::Regtest, &plan).unwrap_err();
        assert!(error.contains("would strand the enabled explorer index"));
        assert_eq!(
            ledger.retained_ranges().unwrap(),
            vec![(1, 100), (101, 400)]
        );
    }

    #[test]
    fn parses_only_offline_height_based_utxo_retiering() {
        let options = parse_options(
            [
                "--network",
                "bitcoin",
                "--data-dir",
                "/tmp/rbtc-retier",
                "--retier-utxos-window-blocks",
                "52560",
            ]
            .into_iter()
            .map(str::to_owned),
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            options.offline_action,
            Some(OfflineAction::RetierUtxos {
                hot_window_blocks: 52_560
            })
        );
        for arguments in [
            vec![
                "--network",
                "bitcoin",
                "--retier-utxos-window-blocks",
                "52560",
            ],
            vec![
                "--network",
                "bitcoin",
                "--data-dir",
                "/tmp/rbtc-retier",
                "--retier-utxos-window-blocks",
                "invalid",
            ],
            vec![
                "--network",
                "bitcoin",
                "--data-dir",
                "/tmp/rbtc-retier",
                "--retier-utxos-window-blocks",
                "52560",
                "--once",
            ],
            vec![
                "--network",
                "bitcoin",
                "--data-dir",
                "/tmp/rbtc-retier",
                "--retier-utxos-window-blocks",
                "52560",
                "--utxo-activity-report",
            ],
        ] {
            assert!(parse_options(arguments.into_iter().map(str::to_owned)).is_err());
        }
    }

    #[test]
    fn parses_only_bounded_parallel_core_snapshot_downloads() {
        let options = parse_options(
            [
                "--download-core-assumeutxo",
                "https://example.com/utxo.dat",
                "--snapshot-download-output",
                "/tmp/utxo.dat",
                "--snapshot-download-bytes",
                "850142614",
                "--snapshot-download-workers",
                "4",
            ]
            .into_iter()
            .map(str::to_owned),
        )
        .unwrap()
        .unwrap();
        assert!(matches!(
            options.offline_action,
            Some(OfflineAction::DownloadCoreSnapshot(SnapshotDownloadConfig {
                source,
                output,
                expected_bytes: 850_142_614,
                workers: 4,
                chunk_bytes: DEFAULT_SNAPSHOT_CHUNK_BYTES,
            })) if source == "https://example.com/utxo.dat"
                && output == PathBuf::from("/tmp/utxo.dat")
        ));
        for arguments in [
            vec!["--download-core-assumeutxo", "https://example.com/utxo.dat"],
            vec![
                "--download-core-assumeutxo",
                "https://example.com/utxo.dat",
                "--snapshot-download-output",
                "/tmp/utxo.dat",
                "--snapshot-download-bytes",
                "850142614",
                "--snapshot-download-workers",
                "9",
            ],
            vec![
                "--download-core-assumeutxo",
                "https://example.com/utxo.dat",
                "--snapshot-download-output",
                "/tmp/utxo.dat",
                "--snapshot-download-bytes",
                "850142614",
                "--data-dir",
                "/tmp/rbtc",
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
            network_execution: NetworkExecutionMode::Persistent,
            explorer_listen: None,
            wallet_api_files: None,
            rpc_auth_token_file: None,
            deployments: DeploymentConfig::for_network(Network::Regtest),
            ibd_policy: IbdPolicy::for_network(Network::Regtest),
            snapshot: Some(SnapshotActivationOptions::Rbtc {
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
            mempool_full_rbf: false,
            cleanup_validation_dir: false,
            offline_action: None,
            validation_limits: ValidationLimits::default(),
            ledger_retention: LedgerRetention::default(),
            minimum_free_bytes: DEFAULT_MINIMUM_FREE_BYTES,
            cache: NodeCacheConfig::default(),
            indexes: NodeIndexConfig::default(),
            inbound_listen: None,
            inbound_limits: InboundLimits::default(),
            resources: NodeResourceConfig::default(),
            logging: NodeLogConfig::default(),
            runtime_control: Arc::new(RuntimeControl::default()),
            observer: None,
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
                &DeploymentConfig::for_network(Network::Regtest).legacy_consensus_id(),
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
            network_execution: NetworkExecutionMode::Persistent,
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
            mempool_full_rbf: false,
            cleanup_validation_dir: false,
            offline_action: None,
            validation_limits: ValidationLimits::default(),
            ledger_retention: LedgerRetention::default(),
            minimum_free_bytes: DEFAULT_MINIMUM_FREE_BYTES,
            cache: NodeCacheConfig::default(),
            indexes: NodeIndexConfig::default(),
            inbound_listen: None,
            inbound_limits: InboundLimits::default(),
            resources: NodeResourceConfig::default(),
            logging: NodeLogConfig::default(),
            runtime_control: Arc::new(RuntimeControl::default()),
            observer: None,
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
                quick_repair: true,
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
                quick_repair: true,
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
    fn adaptive_validation_uses_bounded_batches_until_active_execution_catches_up() {
        let target = ValidationTarget {
            height: 100,
            block_hash: BlockHash::from_byte_array([5; 32]),
        };
        let status = BackgroundValidationStatus::new(PathBuf::from("validation"), target, false);
        let configured = ValidationLimits {
            max_blocks_per_batch: 8,
            pause_between_batches: Duration::from_millis(5),
            quick_repair: false,
        };
        assert_eq!(
            status.adaptive_limits(configured),
            ValidationLimits {
                max_blocks_per_batch: 8,
                pause_between_batches: Duration::from_millis(5),
                quick_repair: false,
            }
        );
        status.update_active(40, 50);
        assert_eq!(status.adaptive_limits(configured), configured);
        let large = ValidationLimits {
            max_blocks_per_batch: 252,
            pause_between_batches: Duration::ZERO,
            quick_repair: false,
        };
        assert_eq!(
            status.adaptive_limits(large).max_blocks_per_batch,
            ADAPTIVE_VALIDATION_BUSY_BATCH_SIZE
        );
        status.update_active(50, 50);
        assert_eq!(status.adaptive_limits(large), large);
        status.update_validation(25);
        let response = status.response();
        assert_eq!(response.validation_tip, 25);
        assert_eq!(response.validation_remaining, 75);
        assert!(!response.adaptive_throttled);
    }

    #[test]
    fn background_active_chain_preserves_batch_and_repair_policy_without_pause() {
        assert_eq!(
            active_assumeutxo_limits(ValidationLimits {
                max_blocks_per_batch: 252,
                pause_between_batches: Duration::from_secs(3),
                quick_repair: false,
            }),
            ValidationLimits {
                max_blocks_per_batch: 252,
                pause_between_batches: Duration::ZERO,
                quick_repair: false,
            }
        );
    }

    #[test]
    fn concurrent_background_pipelines_share_a_bounded_bulk_cache_budget() {
        let cache = NodeCacheConfig::default();
        assert_eq!(
            chainstate_cache_bytes(NetworkExecutionMode::Persistent, true, cache),
            BACKGROUND_PIPELINE_CHAINSTATE_CACHE_BYTES
        );
        assert_eq!(
            chainstate_cache_bytes(NetworkExecutionMode::ExperimentalOnce, false, cache),
            BULK_VALIDATION_CHAINSTATE_CACHE_BYTES
        );
        assert_eq!(
            chainstate_cache_bytes(NetworkExecutionMode::Persistent, false, cache),
            ChainStoreOptions::default().cache_size_bytes
        );
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn embedded_observer_publishes_typed_deltas_and_bounds_slow_consumers() {
        let (status, _) = watch::channel(NodeRuntimeStatus::starting(Network::Regtest));
        let (events, mut receiver) = broadcast::channel(NODE_EVENT_CAPACITY);
        let observer = NodeObserver {
            status: status.clone(),
            events: events.clone(),
        };
        let header = NodeTip {
            height: 2,
            hash: BlockHash::from_byte_array([2; 32]),
        };
        let execution = NodeTip {
            height: 1,
            hash: BlockHash::from_byte_array([1; 32]),
        };
        let explorer = NodeTip {
            height: 1,
            hash: execution.hash,
        };
        let freezer = NodeFreezerStatus {
            segments: 1,
            blocks: 1,
            bytes: 512,
            first_height: Some(1),
            tip_height: Some(1),
            target_blocks: 1_008,
            target_bytes: 1024 * 1024 * 1024,
            pruned_through_height: Some(0),
        };
        let disk = NodeDiskStatus {
            total_bytes: 10_000,
            available_bytes: 8_000,
            required_bytes: 2_000,
            reserve_bytes: 1_000,
            projected_storage_ceiling_bytes: 3_000,
            optional_index_bytes: 0,
            optional_index_checkpoint_headroom_bytes: 0,
        };
        let next = NodeRuntimeStatus {
            network: Network::Regtest,
            active_peer: None,
            header: Some(header),
            execution: Some(execution),
            explorer: Some(explorer),
            wallet: None,
            transaction_index: None,
            spent_output_index: None,
            basic_filter_index: None,
            utxo_hot: 1,
            utxo_cold: 2,
            freezer,
            disk,
            trust: NodeTrustStatus {
                minimum_chainwork_reached: true,
                active_assume_valid_height: None,
                full_script_validation: true,
                independently_validated: true,
            },
            last_error: None,
        };
        observer.publish(next.clone());
        assert_eq!(
            receiver.try_recv().unwrap(),
            NodeEvent::HeaderTipChanged { tip: header }
        );
        assert_eq!(
            receiver.try_recv().unwrap(),
            NodeEvent::ExecutionTipChanged { tip: execution }
        );
        assert_eq!(
            receiver.try_recv().unwrap(),
            NodeEvent::IndexTipChanged {
                index: "explorer",
                tip: explorer,
            }
        );
        assert_eq!(
            receiver.try_recv().unwrap(),
            NodeEvent::FreezerChanged { status: freezer }
        );
        assert_eq!(
            receiver.try_recv().unwrap(),
            NodeEvent::DiskSpaceChanged { status: disk }
        );
        assert!(receiver.try_recv().is_err());
        observer.publish(next);
        assert!(receiver.try_recv().is_err());

        let rewound = NodeTip {
            height: 0,
            hash: bitcoin::blockdata::constants::genesis_block(Network::Regtest).block_hash(),
        };
        observer.reorganized(execution, rewound);
        assert_eq!(
            receiver.try_recv().unwrap(),
            NodeEvent::Reorganized {
                previous: execution,
                current: rewound,
            }
        );

        for _ in 0..=NODE_EVENT_CAPACITY {
            let _ = events.send(NodeEvent::Started);
        }
        assert!(matches!(
            receiver.try_recv(),
            Err(broadcast::error::TryRecvError::Lagged(_))
        ));
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
            transaction_index: None,
            spent_output_index: None,
            basic_filter_index: None,
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
            ledger_target_blocks: 1_008,
            ledger_target_bytes: 1024 * 1024 * 1024,
            ledger_pruned_through_height: Some(0),
            disk_space: DiskSpaceSnapshot {
                total_bytes: 10_000,
                available_bytes: 8_000,
                required_bytes: 2_000,
                reserve_bytes: 1_000,
                projected_storage_ceiling_bytes: 3_000,
                optional_index_bytes: 0,
                optional_index_checkpoint_headroom_bytes: 0,
            },
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
        progress.transaction_index = Some(tip);
        status.update(progress.clone());
        assert_eq!(status.response().phase, "reconciling");
        progress.transaction_index = Some(progress.execution.clone());
        progress.independently_validated = false;
        status.update(progress.clone());
        assert!(status.response().ready);
        assert_eq!(status.response().phase, "assumed_ready");

        progress.minimum_chainwork_reached = false;
        status.update(progress.clone());
        assert!(!status.response().ready);
        assert_eq!(status.response().phase, "ibd");

        progress.minimum_chainwork_reached = true;
        progress.disk_space.available_bytes = 1_999;
        status.update(progress);
        assert!(!status.response().ready);
        assert_eq!(status.response().phase, "low_disk");
    }

    #[test]
    fn disk_monitor_fails_locally_before_an_unsafe_checkpoint() {
        let directory = TempDir::new().unwrap();
        let safe = DiskSpaceMonitor {
            data_dir: directory.path().to_path_buf(),
            required_bytes: 1,
            reserve_bytes: 1,
            projected_storage_ceiling_bytes: 1,
            optional_index_checkpoint_headroom_bytes: 0,
        }
        .check()
        .unwrap();
        assert!(safe.available_bytes >= safe.required_bytes);

        let error = DiskSpaceMonitor {
            data_dir: directory.path().to_path_buf(),
            required_bytes: u64::MAX,
            reserve_bytes: u64::MAX,
            projected_storage_ceiling_bytes: 1,
            optional_index_checkpoint_headroom_bytes: 0,
        }
        .check()
        .unwrap_err();
        assert_eq!(error.kind, PeerFailureKind::LocalResource);
        assert!(error.message.contains("insufficient disk space"));
    }

    #[test]
    fn data_directory_lock_reports_owner_and_releases_cleanly() {
        let directory = TempDir::new().unwrap();
        let first = DataDirectoryLock::acquire(directory.path(), Network::Regtest).unwrap();
        let error = DataDirectoryLock::acquire(directory.path(), Network::Regtest).unwrap_err();
        assert!(error.contains("already locked"));
        assert!(error.contains("pid="));
        assert!(error.contains("network=regtest"));
        drop(first);
        DataDirectoryLock::acquire(directory.path(), Network::Regtest).unwrap();

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = fs::metadata(directory.path().join(DATA_DIRECTORY_LOCK_FILE))
                .unwrap()
                .permissions()
                .mode();
            assert_eq!(mode & 0o077, 0);
        }
    }

    #[test]
    fn read_only_audit_lock_never_rewrites_marker_and_excludes_the_node() {
        let directory = TempDir::new().unwrap();
        let exclusive = DataDirectoryLock::acquire(directory.path(), Network::Regtest).unwrap();
        let marker_path = directory.path().join(DATA_DIRECTORY_LOCK_FILE);
        let marker = fs::read(&marker_path).unwrap();
        assert!(
            DataDirectoryReadLock::acquire(directory.path())
                .unwrap_err()
                .contains("in use")
        );
        drop(exclusive);

        let first = DataDirectoryReadLock::acquire(directory.path()).unwrap();
        let second = DataDirectoryReadLock::acquire(directory.path()).unwrap();
        assert_eq!(fs::read(&marker_path).unwrap(), marker);
        assert!(DataDirectoryLock::acquire(directory.path(), Network::Regtest).is_err());
        drop((first, second));
        assert_eq!(fs::read(marker_path).unwrap(), marker);
    }

    #[test]
    fn data_format_manifest_migrates_absence_and_rejects_future_rollback() {
        let directory = TempDir::new().unwrap();
        assert!(validate_data_format_manifest(directory.path(), Network::Regtest).unwrap());
        publish_data_format_manifest(directory.path(), Network::Regtest).unwrap();
        assert!(!validate_data_format_manifest(directory.path(), Network::Regtest).unwrap());
        let path = directory.path().join(DATA_FORMAT_MANIFEST_FILE);
        let current = fs::read(&path).unwrap();
        publish_data_format_manifest(directory.path(), Network::Regtest).unwrap();
        assert_eq!(fs::read(&path).unwrap(), current);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                fs::metadata(&path).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }

        let legacy = serde_json::to_vec(&serde_json::json!({
            "schema_version": 1,
            "minimum_reader_version": 1,
            "network": "regtest",
            "stores": {
                "headers": 1,
                "chainstate": 1,
                "freezer": 1,
                "peers": 1,
                "mempool": 1,
                "fee_estimates": 1,
                "explorer": 1,
                "wallet": 1,
                "rebroadcast": 1,
            }
        }))
        .unwrap();
        fs::write(&path, legacy).unwrap();
        #[cfg(unix)]
        fs::set_permissions(&path, std::os::unix::fs::PermissionsExt::from_mode(0o600)).unwrap();
        assert!(validate_data_format_manifest(directory.path(), Network::Regtest).unwrap());
        publish_data_format_manifest(directory.path(), Network::Regtest).unwrap();
        assert!(!validate_data_format_manifest(directory.path(), Network::Regtest).unwrap());
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&fs::read(&path).unwrap()).unwrap()["schema_version"],
            3
        );

        let version2 = serde_json::to_vec(&DataFormatManifest::version2(Network::Regtest)).unwrap();
        fs::write(&path, &version2).unwrap();
        #[cfg(unix)]
        fs::set_permissions(&path, std::os::unix::fs::PermissionsExt::from_mode(0o600)).unwrap();
        assert!(validate_data_format_manifest(directory.path(), Network::Regtest).unwrap());
        let obsolete_filter = directory.path().join("basic-filter-index.redb");
        fs::write(&obsolete_filter, []).unwrap();
        assert!(
            validate_data_format_manifest(directory.path(), Network::Regtest)
                .unwrap_err()
                .contains("omitted the genesis filter")
        );
        fs::remove_file(obsolete_filter).unwrap();
        publish_data_format_manifest(directory.path(), Network::Regtest).unwrap();
        assert!(!validate_data_format_manifest(directory.path(), Network::Regtest).unwrap());

        let future = serde_json::to_vec(&serde_json::json!({
            "schema_version": 4,
            "minimum_reader_version": 4,
            "network": "regtest",
            "stores": {
                "headers": 1,
                "chainstate": 1,
                "freezer": 1,
                "peers": 1,
                "mempool": 1,
                "fee_estimates": 1,
                "explorer": 1,
                "wallet": 1,
                "rebroadcast": 1,
            }
        }))
        .unwrap();
        fs::write(&path, &future).unwrap();
        #[cfg(unix)]
        fs::set_permissions(&path, std::os::unix::fs::PermissionsExt::from_mode(0o600)).unwrap();
        assert!(
            validate_data_format_manifest(directory.path(), Network::Regtest)
                .unwrap_err()
                .contains("unsupported data-format schema version")
        );
        assert_eq!(fs::read(path).unwrap(), future);
    }

    #[cfg(unix)]
    #[test]
    fn data_format_manifest_rejects_symlink_and_hardlink_aliases() {
        use std::os::unix::fs::symlink;

        let directory = TempDir::new().unwrap();
        let outside = directory.path().join("outside");
        fs::write(&outside, "{}").unwrap();
        let manifest = directory.path().join(DATA_FORMAT_MANIFEST_FILE);
        symlink(&outside, &manifest).unwrap();
        assert!(
            validate_data_format_manifest(directory.path(), Network::Regtest)
                .unwrap_err()
                .contains("bounded regular file")
        );
        fs::remove_file(&manifest).unwrap();
        fs::hard_link(&outside, &manifest).unwrap();
        fs::set_permissions(
            &manifest,
            std::os::unix::fs::PermissionsExt::from_mode(0o600),
        )
        .unwrap();
        assert!(
            validate_data_format_manifest(directory.path(), Network::Regtest)
                .unwrap_err()
                .contains("exactly one hard link")
        );
    }

    #[cfg(unix)]
    #[test]
    fn data_directory_lock_rejects_symlinks_and_hardlinks() {
        use std::os::unix::fs::symlink;

        let directory = TempDir::new().unwrap();
        let outside = directory.path().join("outside");
        fs::write(&outside, "not a lock").unwrap();
        let lock = directory.path().join(DATA_DIRECTORY_LOCK_FILE);
        symlink(&outside, &lock).unwrap();
        assert!(
            DataDirectoryLock::acquire(directory.path(), Network::Regtest)
                .unwrap_err()
                .contains("regular file")
        );
        fs::remove_file(&lock).unwrap();
        fs::hard_link(&outside, &lock).unwrap();
        assert!(
            DataDirectoryLock::acquire(directory.path(), Network::Regtest)
                .unwrap_err()
                .contains("exactly one hard link")
        );
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
        assert_eq!(seeds.len(), 8);
        assert!(seeds.iter().all(|seed| seed.port == 8_333));
        assert_eq!(seeds[0].host, "seed.bitcoin.sipa.be.");

        let testnet4 = parse_options(["--network", "testnet4"].into_iter().map(str::to_owned))
            .unwrap()
            .unwrap();
        assert_eq!(
            selected_dns_seeds(&testnet4),
            vec![
                DnsSeed {
                    host: "seed.testnet4.bitcoin.sprovoost.nl.".to_owned(),
                    port: 48_333,
                },
                DnsSeed {
                    host: "seed.testnet4.wiz.biz.".to_owned(),
                    port: 48_333,
                },
            ]
        );

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
    fn persistent_execution_accepts_every_configured_network() {
        for network in ["bitcoin", "testnet", "testnet4", "regtest", "signet"] {
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
    }

    #[test]
    fn experimental_network_execution_requires_a_bounded_authenticated_target() {
        for network in ["bitcoin", "testnet"] {
            let options = parse_options(
                [
                    "--connect",
                    "127.0.0.1:1",
                    "--network",
                    network,
                    "--data-dir",
                    "/tmp/rbtc-experimental-execution-gate",
                    "--experimental-network-execution",
                    "--once",
                    "--validate-until-height",
                    "1",
                    "--validate-until-blockhash",
                    "00000000839a8e6886ab5951d76f411475428afc90947ee320161bbf18eb6048",
                ]
                .into_iter()
                .map(str::to_owned),
            )
            .unwrap()
            .unwrap();
            assert_eq!(
                options.network_execution,
                NetworkExecutionMode::ExperimentalOnce
            );
            assert!(options.once);
            assert!(options.validation_target.is_some());
        }

        for arguments in [
            vec![
                "--network",
                "bitcoin",
                "--data-dir",
                "/tmp/rbtc-experimental-execution-gate",
                "--experimental-network-execution",
            ],
            vec![
                "--network",
                "testnet4",
                "--data-dir",
                "/tmp/rbtc-experimental-execution-gate",
                "--experimental-network-execution",
                "--once",
            ],
            vec![
                "--network",
                "regtest",
                "--data-dir",
                "/tmp/rbtc-experimental-execution-gate",
                "--experimental-network-execution",
                "--once",
            ],
            vec![
                "--network",
                "bitcoin",
                "--data-dir",
                "/tmp/rbtc-experimental-execution-gate",
                "--experimental-network-execution",
                "--once",
            ],
            vec![
                "--network",
                "bitcoin",
                "--experimental-network-execution",
                "--once",
            ],
            vec![
                "--network",
                "bitcoin",
                "--data-dir",
                "/tmp/rbtc-experimental-execution-gate",
                "--extend-validation-target",
                "--once",
                "--validate-until-height",
                "2",
                "--validate-until-blockhash",
                "000000006a625f06636b8bb6ac7b960a8d03705dc0a87fe1b90a9e5da8d913cd",
            ],
        ] {
            assert!(
                parse_options(arguments.clone().into_iter().map(str::to_owned)).is_err(),
                "{arguments:?}"
            );
        }
    }

    #[test]
    fn experimental_target_extension_is_explicit_and_complete() {
        let extension = parse_options(
            [
                "--connect",
                "127.0.0.1:1",
                "--network",
                "bitcoin",
                "--data-dir",
                "/tmp/rbtc-experimental-execution-gate",
                "--experimental-network-execution",
                "--extend-validation-target",
                "--validation-deferred-repair",
                "--validation-batch-size",
                "1008",
                "--once",
                "--validate-until-height",
                "2",
                "--validate-until-blockhash",
                "000000006a625f06636b8bb6ac7b960a8d03705dc0a87fe1b90a9e5da8d913cd",
            ]
            .into_iter()
            .map(str::to_owned),
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            extension.network_execution,
            NetworkExecutionMode::ExperimentalOnceExtend
        );
        assert!(!extension.validation_limits.quick_repair);
        assert_eq!(extension.validation_limits.max_blocks_per_batch, 1_008);

        let too_large = [
            "--network",
            "bitcoin",
            "--data-dir",
            "/tmp/rbtc-experimental-execution-gate",
            "--experimental-network-execution",
            "--once",
            "--validation-batch-size",
            "1009",
            "--validate-until-height",
            "2",
            "--validate-until-blockhash",
            "000000006a625f06636b8bb6ac7b960a8d03705dc0a87fe1b90a9e5da8d913cd",
        ];
        assert!(parse_options(too_large.into_iter().map(str::to_owned)).is_err());
    }

    #[test]
    fn custom_signet_identity_is_rejected_before_network_or_wallet_startup() {
        let directory = TempDir::new().unwrap();
        let default = DeploymentConfig::for_network(Network::Signet);
        preflight_data_dir(directory.path(), Network::Signet, &default, false).unwrap();
        assert!(!directory.path().join("peers.redb").exists());
        assert!(!directory.path().join("wallet.sqlite").exists());

        let mut custom = DeploymentConfig::for_network(Network::Signet);
        custom.set_signet_challenge(vec![0x51]).unwrap();
        let error =
            preflight_data_dir(directory.path(), Network::Signet, &custom, false).unwrap_err();
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
            network_execution: NetworkExecutionMode::Persistent,
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
            mempool_full_rbf: false,
            cleanup_validation_dir: false,
            offline_action: None,
            validation_limits: ValidationLimits::default(),
            ledger_retention: LedgerRetention::default(),
            minimum_free_bytes: DEFAULT_MINIMUM_FREE_BYTES,
            cache: NodeCacheConfig::default(),
            indexes: NodeIndexConfig::default(),
            inbound_listen: None,
            inbound_limits: InboundLimits::default(),
            resources: NodeResourceConfig::default(),
            logging: NodeLogConfig::default(),
            runtime_control: Arc::new(RuntimeControl::default()),
            observer: None,
        })
        .await
        .unwrap_err();

        assert!(error.contains("legacy split chain-state file"));
        assert!(!directory.path().join("peers.redb").exists());
        assert!(!directory.path().join("wallet.sqlite").exists());
        assert!(!directory.path().join(DATA_FORMAT_MANIFEST_FILE).exists());
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
            network_execution: NetworkExecutionMode::Persistent,
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
            mempool_full_rbf: false,
            cleanup_validation_dir: false,
            offline_action: None,
            validation_limits: ValidationLimits::default(),
            ledger_retention: LedgerRetention::default(),
            minimum_free_bytes: DEFAULT_MINIMUM_FREE_BYTES,
            cache: NodeCacheConfig::default(),
            indexes: NodeIndexConfig::default(),
            inbound_listen: None,
            inbound_limits: InboundLimits::default(),
            resources: NodeResourceConfig::default(),
            logging: NodeLogConfig::default(),
            runtime_control: Arc::new(RuntimeControl::default()),
            observer: None,
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
        let (broadcast_sender, broadcast_receiver) = mpsc::channel(WALLET_BROADCAST_QUEUE_CAPACITY);
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
            broadcast_sender,
            broadcast_receiver: Arc::new(tokio::sync::Mutex::new(broadcast_receiver)),
            pending_broadcast: Arc::new(tokio::sync::Mutex::new(None)),
            compact_candidates: CompactTransactionCandidates::default(),
            rebroadcast: Arc::new(
                RedbRebroadcastStore::open(
                    directory.path().join("rebroadcast.redb"),
                    Network::Regtest,
                )
                .unwrap(),
            ),
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
            None,
            Some(&validation),
            Arc::new(Mutex::new(TransactionAdmissionPool::default())),
            Arc::new(RuntimeControl::default()),
            Some("127.0.0.1:18444".parse().unwrap()),
            Arc::new(AuxiliaryIndexes {
                transaction: None,
                spent_output: None,
                basic_filter: None,
            }),
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
        assert!(
            rpc_result["result"]
                .as_array()
                .unwrap()
                .contains(&serde_json::json!("getblockchaininfo"))
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

        let descriptors = http_request(
            server.address(),
            format!(
                "GET /api/v1/wallet/descriptors HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer {token_text}\r\nConnection: close\r\n\r\n"
            )
            .as_bytes(),
        )
        .await;
        assert!(descriptors.starts_with("HTTP/1.1 200 OK"));
        let descriptors: rbtc::wallet::WalletPublicDescriptors =
            serde_json::from_str(descriptors.split("\r\n\r\n").nth(1).unwrap()).unwrap();
        let imported =
            parse_wallet_descriptor_config(&serde_json::to_vec(&descriptors).unwrap()).unwrap();
        assert_eq!(imported.receive_descriptor, descriptors.receive_descriptor);
        assert_eq!(imported.change_descriptor, descriptors.change_descriptor);
        assert!(descriptors.receive_descriptor.contains("tpub"));

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
        assert!(!audit.contains("tpub"));
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
            receive_client_preferences(&mut peer).await;
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
            network_execution: NetworkExecutionMode::Persistent,
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
            mempool_full_rbf: false,
            cleanup_validation_dir: false,
            offline_action: None,
            validation_limits: ValidationLimits::default(),
            ledger_retention: LedgerRetention::default(),
            minimum_free_bytes: DEFAULT_MINIMUM_FREE_BYTES,
            cache: NodeCacheConfig::default(),
            indexes: NodeIndexConfig::default(),
            inbound_listen: None,
            inbound_limits: InboundLimits::default(),
            resources: NodeResourceConfig::default(),
            logging: NodeLogConfig::default(),
            runtime_control: Arc::new(RuntimeControl::default()),
            observer: None,
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
            network_execution: NetworkExecutionMode::Persistent,
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
            mempool_full_rbf: false,
            cleanup_validation_dir: false,
            offline_action: None,
            validation_limits: ValidationLimits::default(),
            ledger_retention: LedgerRetention::default(),
            minimum_free_bytes: DEFAULT_MINIMUM_FREE_BYTES,
            cache: NodeCacheConfig::default(),
            indexes: NodeIndexConfig::default(),
            inbound_listen: None,
            inbound_limits: InboundLimits::default(),
            resources: NodeResourceConfig::default(),
            logging: NodeLogConfig::default(),
            runtime_control: Arc::new(RuntimeControl::default()),
            observer: None,
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
            network_execution: NetworkExecutionMode::Persistent,
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
            mempool_full_rbf: false,
            cleanup_validation_dir: false,
            offline_action: None,
            validation_limits: ValidationLimits::default(),
            ledger_retention: LedgerRetention::default(),
            minimum_free_bytes: DEFAULT_MINIMUM_FREE_BYTES,
            cache: NodeCacheConfig::default(),
            indexes: NodeIndexConfig::default(),
            inbound_listen: None,
            inbound_limits: InboundLimits::default(),
            resources: NodeResourceConfig::default(),
            logging: NodeLogConfig::default(),
            runtime_control: Arc::new(RuntimeControl::default()),
            observer: None,
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
            network_execution: NetworkExecutionMode::Persistent,
            explorer_listen: None,
            wallet_api_files: None,
            rpc_auth_token_file: None,
            deployments: DeploymentConfig::for_network(Network::Regtest),
            ibd_policy: IbdPolicy::for_network(Network::Regtest),
            snapshot: Some(SnapshotActivationOptions::Rbtc {
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
            mempool_full_rbf: false,
            cleanup_validation_dir: false,
            offline_action: None,
            validation_limits: ValidationLimits::default(),
            ledger_retention: LedgerRetention::default(),
            minimum_free_bytes: DEFAULT_MINIMUM_FREE_BYTES,
            cache: NodeCacheConfig::default(),
            indexes: NodeIndexConfig::default(),
            inbound_listen: None,
            inbound_limits: InboundLimits::default(),
            resources: NodeResourceConfig::default(),
            logging: NodeLogConfig::default(),
            runtime_control: Arc::new(RuntimeControl::default()),
            observer: None,
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
            network_execution: NetworkExecutionMode::Persistent,
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
            mempool_full_rbf: false,
            cleanup_validation_dir: false,
            offline_action: None,
            validation_limits: ValidationLimits::default(),
            ledger_retention: LedgerRetention::default(),
            minimum_free_bytes: DEFAULT_MINIMUM_FREE_BYTES,
            cache: NodeCacheConfig::default(),
            indexes: NodeIndexConfig::default(),
            inbound_listen: None,
            inbound_limits: InboundLimits::default(),
            resources: NodeResourceConfig::default(),
            logging: NodeLogConfig::default(),
            runtime_control: Arc::new(RuntimeControl::default()),
            observer: None,
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
            network_execution: NetworkExecutionMode::Persistent,
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
            mempool_full_rbf: false,
            cleanup_validation_dir: false,
            offline_action: None,
            validation_limits: ValidationLimits::default(),
            ledger_retention: LedgerRetention::default(),
            minimum_free_bytes: DEFAULT_MINIMUM_FREE_BYTES,
            cache: NodeCacheConfig::default(),
            indexes: NodeIndexConfig::default(),
            inbound_listen: None,
            inbound_limits: InboundLimits::default(),
            resources: NodeResourceConfig::default(),
            logging: NodeLogConfig::default(),
            runtime_control: Arc::new(RuntimeControl::default()),
            observer: None,
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
            network_execution: NetworkExecutionMode::Persistent,
            explorer_listen: None,
            wallet_api_files: None,
            rpc_auth_token_file: None,
            deployments: DeploymentConfig::for_network(Network::Regtest),
            ibd_policy: IbdPolicy::for_network(Network::Regtest),
            snapshot: Some(SnapshotActivationOptions::Rbtc {
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
            mempool_full_rbf: false,
            cleanup_validation_dir: false,
            offline_action: None,
            validation_limits: ValidationLimits::default(),
            ledger_retention: LedgerRetention::default(),
            minimum_free_bytes: DEFAULT_MINIMUM_FREE_BYTES,
            cache: NodeCacheConfig::default(),
            indexes: NodeIndexConfig::default(),
            inbound_listen: None,
            inbound_limits: InboundLimits::default(),
            resources: NodeResourceConfig::default(),
            logging: NodeLogConfig::default(),
            runtime_control: Arc::new(RuntimeControl::default()),
            observer: None,
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
                network_execution: NetworkExecutionMode::Persistent,
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
                mempool_full_rbf: false,
                cleanup_validation_dir: true,
                offline_action: None,
                validation_limits: ValidationLimits {
                    max_blocks_per_batch: 1,
                    pause_between_batches: Duration::ZERO,
                    quick_repair: true,
                },
                ledger_retention: LedgerRetention::default(),
                minimum_free_bytes: DEFAULT_MINIMUM_FREE_BYTES,
                cache: NodeCacheConfig::default(),
                indexes: NodeIndexConfig::default(),
                inbound_listen: None,
                inbound_limits: InboundLimits::default(),
                resources: NodeResourceConfig::default(),
                logging: NodeLogConfig::default(),
                runtime_control: Arc::new(RuntimeControl::default()),
                observer: None,
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
            network_execution: NetworkExecutionMode::Persistent,
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
            mempool_full_rbf: false,
            cleanup_validation_dir: false,
            offline_action: None,
            validation_limits: ValidationLimits::default(),
            ledger_retention: LedgerRetention::default(),
            minimum_free_bytes: DEFAULT_MINIMUM_FREE_BYTES,
            cache: NodeCacheConfig::default(),
            indexes: NodeIndexConfig::default(),
            inbound_listen: None,
            inbound_limits: InboundLimits::default(),
            resources: NodeResourceConfig::default(),
            logging: NodeLogConfig::default(),
            runtime_control: Arc::new(RuntimeControl::default()),
            observer: None,
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
    #[allow(clippy::too_many_lines)]
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
            network_execution: NetworkExecutionMode::Persistent,
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
            mempool_full_rbf: false,
            cleanup_validation_dir: false,
            offline_action: None,
            validation_limits: ValidationLimits::default(),
            ledger_retention: LedgerRetention::default(),
            minimum_free_bytes: DEFAULT_MINIMUM_FREE_BYTES,
            cache: NodeCacheConfig::default(),
            indexes: NodeIndexConfig::default(),
            inbound_listen: None,
            inbound_limits: InboundLimits::default(),
            resources: NodeResourceConfig::default(),
            logging: NodeLogConfig::default(),
            runtime_control: Arc::new(RuntimeControl::default()),
            observer: None,
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
            network_execution: NetworkExecutionMode::Persistent,
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
            mempool_full_rbf: false,
            cleanup_validation_dir: false,
            offline_action: None,
            validation_limits: ValidationLimits::default(),
            ledger_retention: LedgerRetention::default(),
            minimum_free_bytes: DEFAULT_MINIMUM_FREE_BYTES,
            cache: NodeCacheConfig::default(),
            indexes: NodeIndexConfig::default(),
            inbound_listen: None,
            inbound_limits: InboundLimits::default(),
            resources: NodeResourceConfig::default(),
            logging: NodeLogConfig::default(),
            runtime_control: Arc::new(RuntimeControl::default()),
            observer: None,
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
            network_execution: NetworkExecutionMode::Persistent,
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
            mempool_full_rbf: false,
            cleanup_validation_dir: false,
            offline_action: None,
            validation_limits: ValidationLimits::default(),
            ledger_retention: LedgerRetention::default(),
            minimum_free_bytes: DEFAULT_MINIMUM_FREE_BYTES,
            cache: NodeCacheConfig::default(),
            indexes: NodeIndexConfig::default(),
            inbound_listen: None,
            inbound_limits: InboundLimits::default(),
            resources: NodeResourceConfig::default(),
            logging: NodeLogConfig::default(),
            runtime_control: Arc::new(RuntimeControl::default()),
            observer: None,
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
            network_execution: NetworkExecutionMode::Persistent,
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
            mempool_full_rbf: false,
            cleanup_validation_dir: false,
            offline_action: None,
            validation_limits: ValidationLimits::default(),
            ledger_retention: LedgerRetention::default(),
            minimum_free_bytes: DEFAULT_MINIMUM_FREE_BYTES,
            cache: NodeCacheConfig::default(),
            indexes: NodeIndexConfig::default(),
            inbound_listen: None,
            inbound_limits: InboundLimits::default(),
            resources: NodeResourceConfig::default(),
            logging: NodeLogConfig::default(),
            runtime_control: Arc::new(RuntimeControl::default()),
            observer: None,
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
    #[allow(clippy::too_many_lines)]
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
            network_execution: NetworkExecutionMode::Persistent,
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
            mempool_full_rbf: false,
            cleanup_validation_dir: false,
            offline_action: None,
            validation_limits: ValidationLimits::default(),
            ledger_retention: LedgerRetention::default(),
            minimum_free_bytes: DEFAULT_MINIMUM_FREE_BYTES,
            cache: NodeCacheConfig::default(),
            indexes: NodeIndexConfig::default(),
            inbound_listen: None,
            inbound_limits: InboundLimits::default(),
            resources: NodeResourceConfig::default(),
            logging: NodeLogConfig::default(),
            runtime_control: Arc::new(RuntimeControl::default()),
            observer: None,
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
            receive_client_preferences(&mut peer).await;
            peer.write_message(NetworkMessage::SendCmpct(SendCmpct {
                send_compact: false,
                version: 2,
            }))
            .await
            .unwrap();
            assert!(matches!(
                peer.read_message().await.unwrap().into_payload(),
                NetworkMessage::GetAddr
            ));
            peer.write_message(NetworkMessage::AddrV2(vec![
                bitcoin::p2p::address::AddrV2Message {
                    time: 0,
                    services: ServiceFlags::NETWORK | ServiceFlags::WITNESS,
                    addr: bitcoin::p2p::address::AddrV2::Ipv4("127.0.0.2".parse().unwrap()),
                    port: 18_445,
                },
            ]))
            .await
            .unwrap();
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
                        vec![bitcoin::p2p::message_blockdata::Inventory::CompactBlock(
                            block_hash
                        )]
                    );
                    peer.write_message(NetworkMessage::CmpctBlock(CmpctBlock {
                        compact_block: HeaderAndShortIds::from_block(&block, 6, 2, &[]).unwrap(),
                    }))
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
            network_execution: NetworkExecutionMode::Persistent,
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
            mempool_full_rbf: false,
            cleanup_validation_dir: false,
            offline_action: None,
            validation_limits: ValidationLimits::default(),
            ledger_retention: LedgerRetention::default(),
            minimum_free_bytes: DEFAULT_MINIMUM_FREE_BYTES,
            cache: NodeCacheConfig::default(),
            indexes: NodeIndexConfig::default(),
            inbound_listen: None,
            inbound_limits: InboundLimits::default(),
            resources: NodeResourceConfig::default(),
            logging: NodeLogConfig::default(),
            runtime_control: Arc::new(RuntimeControl::default()),
            observer: None,
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
            network_execution: NetworkExecutionMode::Persistent,
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
            mempool_full_rbf: false,
            cleanup_validation_dir: false,
            offline_action: None,
            validation_limits: ValidationLimits::default(),
            ledger_retention: LedgerRetention::default(),
            minimum_free_bytes: DEFAULT_MINIMUM_FREE_BYTES,
            cache: NodeCacheConfig::default(),
            indexes: NodeIndexConfig::default(),
            inbound_listen: None,
            inbound_limits: InboundLimits::default(),
            resources: NodeResourceConfig::default(),
            logging: NodeLogConfig::default(),
            runtime_control: Arc::new(RuntimeControl::default()),
            observer: None,
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
            network_execution: NetworkExecutionMode::Persistent,
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
            mempool_full_rbf: false,
            cleanup_validation_dir: false,
            offline_action: None,
            validation_limits: ValidationLimits::default(),
            ledger_retention: LedgerRetention::default(),
            minimum_free_bytes: DEFAULT_MINIMUM_FREE_BYTES,
            cache: NodeCacheConfig::default(),
            indexes: NodeIndexConfig::default(),
            inbound_listen: None,
            inbound_limits: InboundLimits::default(),
            resources: NodeResourceConfig::default(),
            logging: NodeLogConfig::default(),
            runtime_control: Arc::new(RuntimeControl::default()),
            observer: None,
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
                network_execution: NetworkExecutionMode::Persistent,
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
                mempool_full_rbf: false,
                cleanup_validation_dir: false,
                offline_action: None,
                validation_limits: ValidationLimits::default(),
                ledger_retention: LedgerRetention::default(),
                minimum_free_bytes: DEFAULT_MINIMUM_FREE_BYTES,
                cache: NodeCacheConfig::default(),
                indexes: NodeIndexConfig::default(),
                inbound_listen: None,
                inbound_limits: InboundLimits::default(),
                resources: NodeResourceConfig::default(),
                logging: NodeLogConfig::default(),
                runtime_control: Arc::new(RuntimeControl::default()),
                observer: None,
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
        assert!(!directory.path().join("explorer.redb").exists());
        let ledger =
            PrunedBlockLedger::open(directory.path().join("blocks"), LedgerRetention::default())
                .unwrap();
        let expected_ranges = (1..=block_count)
            .step_by(DEFAULT_VALIDATION_BATCH_SIZE)
            .map(|first| {
                let last = first
                    .saturating_add(u32::try_from(DEFAULT_VALIDATION_BATCH_SIZE - 1).unwrap())
                    .min(block_count);
                (first, last)
            })
            .collect::<Vec<_>>();
        assert_eq!(ledger.retained_ranges().unwrap(), expected_ranges);

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
                network_execution: NetworkExecutionMode::Persistent,
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
                mempool_full_rbf: false,
                cleanup_validation_dir: false,
                offline_action: None,
                validation_limits: ValidationLimits::default(),
                ledger_retention: LedgerRetention::default(),
                minimum_free_bytes: DEFAULT_MINIMUM_FREE_BYTES,
                cache: NodeCacheConfig::default(),
                indexes: NodeIndexConfig::default(),
                inbound_listen: None,
                inbound_limits: InboundLimits::default(),
                resources: NodeResourceConfig::default(),
                logging: NodeLogConfig::default(),
                runtime_control: Arc::new(RuntimeControl::default()),
                observer: None,
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
        assert!(!directory.path().join("explorer.redb").exists());
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
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
            network_execution: NetworkExecutionMode::Persistent,
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
            mempool_full_rbf: false,
            cleanup_validation_dir: false,
            offline_action: None,
            validation_limits: ValidationLimits::default(),
            ledger_retention: LedgerRetention::default(),
            minimum_free_bytes: DEFAULT_MINIMUM_FREE_BYTES,
            cache: NodeCacheConfig::default(),
            indexes: NodeIndexConfig::default(),
            inbound_listen: None,
            inbound_limits: InboundLimits::default(),
            resources: NodeResourceConfig::default(),
            logging: NodeLogConfig::default(),
            runtime_control: Arc::new(RuntimeControl::default()),
            observer: None,
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
        assert!(!directory.path().join("explorer.redb").exists());
    }
}
