//! Embedded REST routes for the explorer and descriptor wallet.
//!
//! Bind these routers only to loopback. The strict read-only JSON-RPC and
//! watch-only wallet routes enforce scoped bearer authentication, synchronized
//! audit records, and no-store responses; wallet address revelation also has a
//! bounded rate limit. TLS termination remains a deployment concern.

use std::{
    collections::HashMap,
    convert::Infallible,
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::Path as FsPath,
    sync::{Arc, Mutex, RwLock},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use axum::{
    Json, Router,
    body::Bytes,
    extract::{DefaultBodyLimit, Path, Query, Request, State},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    middleware::{self, Next},
    response::{Html, IntoResponse, Response, Sse, sse::Event},
    routing::{get, post},
};
use serde::{Deserialize, Serialize};
use tokio::sync::{Semaphore, broadcast, mpsc, oneshot};
use tokio_stream::{
    StreamExt,
    wrappers::{BroadcastStream, errors::BroadcastStreamRecvError},
};

use crate::{
    rebroadcast_store::{RebroadcastStoreError, RedbRebroadcastStore},
    transaction_policy::validate_standard_transaction,
    wallet::{
        EmbeddedWallet, MAX_WALLET_PSBT_FINALIZE_REQUEST_BYTES, MAX_WALLET_PSBT_REQUEST_BYTES,
        WalletAddress, WalletBalance, WalletError, WalletFinalizedTransaction, WalletPsbt,
        WalletPublicDescriptors, WalletStatus, WalletTransaction, WalletUtxo,
        parse_wallet_psbt_finalize_request, parse_wallet_psbt_request,
    },
};

/// Explorer block summary returned by the embedded API.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ExplorerBlock {
    /// Block height.
    pub height: u32,
    /// Display-order block hash.
    pub hash: String,
    /// Block timestamp.
    pub time: u64,
    /// Number of transactions in the block.
    pub transaction_count: u32,
}

/// Explorer transaction summary returned by the embedded API.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ExplorerTransaction {
    /// Display-order transaction ID.
    pub txid: String,
    /// Confirmed height, if known.
    pub confirmed_height: Option<u32>,
    /// Serialized transaction size.
    pub vbytes: u32,
}

/// UTXO response for an address/script search.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ExplorerUtxo {
    /// Txid containing the output.
    pub txid: String,
    /// Output index.
    pub vout: u32,
    /// Value in satoshis.
    pub value_sats: u64,
    /// Confirmed block height.
    pub height: u32,
}

/// A bounded page of current address UTXOs.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ExplorerUtxoPage {
    /// UTXOs in deterministic outpoint order.
    pub utxos: Vec<ExplorerUtxo>,
    /// Zero-based number of matching entries skipped.
    pub offset: u32,
    /// Requested maximum number of returned entries.
    pub limit: u32,
    /// Whether at least one additional matching entry exists.
    pub has_more: bool,
}

/// A bounded page of current wallet UTXOs.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct WalletUtxoPage {
    /// Current unspent wallet outputs.
    pub utxos: Vec<WalletUtxo>,
    /// Zero-based number of entries skipped.
    pub offset: u32,
    /// Requested maximum number of returned entries.
    pub limit: u32,
    /// Whether at least one additional entry exists.
    pub has_more: bool,
}

/// A bounded page of canonical wallet transactions.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct WalletTransactionPage {
    /// Canonical transactions in newest-first order.
    pub transactions: Vec<WalletTransaction>,
    /// Zero-based number of entries skipped.
    pub offset: u32,
    /// Requested maximum number of returned entries.
    pub limit: u32,
    /// Whether at least one additional entry exists.
    pub has_more: bool,
}

/// Default UTXO page size when the query omits `limit`.
pub const DEFAULT_UTXO_PAGE_SIZE: u32 = 50;
/// Maximum accepted UTXO page size.
pub const MAX_UTXO_PAGE_SIZE: u32 = 100;
/// Maximum accepted offset, bounding work for offset-based pagination.
pub const MAX_UTXO_PAGE_OFFSET: u32 = 10_000;
const MIN_WALLET_AUTH_TOKEN_LEN: usize = 32;
const MAX_WALLET_AUTH_TOKEN_LEN: usize = 256;
const WALLET_ADDRESS_BURST: u8 = 20;
const WALLET_ADDRESS_REFILL_INTERVAL: Duration = Duration::from_secs(60);
const MAX_EXPLORER_EVENT_CLIENTS: usize = 64;
/// Maximum on-disk local authorization audit log size.
pub const MAX_AUTHORIZATION_AUDIT_BYTES: u64 = 16 * 1024 * 1024;
/// Backwards-compatible wallet audit capacity name.
pub const MAX_WALLET_AUDIT_BYTES: u64 = MAX_AUTHORIZATION_AUDIT_BYTES;
/// Maximum accepted JSON-RPC request body size.
pub const MAX_RPC_BODY_BYTES: usize = 64 * 1024;
/// Maximum wallet broadcasts waiting for the active peer session.
pub const WALLET_BROADCAST_QUEUE_CAPACITY: usize = 8;
const WALLET_BROADCAST_TIMEOUT: Duration = Duration::from_secs(35);

/// Rotatable in-memory bearer credential protecting a local API surface.
///
/// The value is intentionally not printable through `Debug` or `Display`.
#[derive(Clone)]
pub struct LocalAuthToken(Arc<RwLock<Option<Arc<[u8]>>>>);

/// Backwards-compatible name for the wallet bearer credential.
pub type WalletAuthToken = LocalAuthToken;

/// Append-only, owner-only authorization audit log shared by local API routes.
///
/// Records contain only a timestamp, method, path without a query string, and
/// authorization result. Credentials, query values, bodies, and responses are
/// deliberately excluded.
#[derive(Clone)]
pub struct AuthorizationAuditLog(Arc<Mutex<WalletAuditWriter>>);

/// Backwards-compatible name for the wallet authorization audit log.
pub type WalletAuditLog = AuthorizationAuditLog;

struct WalletAuditWriter {
    file: File,
    bytes: u64,
}

#[derive(Serialize)]
struct WalletAuditRecord<'a> {
    version: u8,
    unix_time: u64,
    method: &'a str,
    path: &'a str,
    authorization: &'static str,
}

#[derive(Clone)]
struct LocalRouteSecurity {
    token: LocalAuthToken,
    audit: AuthorizationAuditLog,
}

struct WalletApiState {
    wallet: Arc<EmbeddedWallet>,
    address_limiter: Mutex<AddressRateLimiter>,
    psbt_limiter: Mutex<AddressRateLimiter>,
    broadcast: Option<WalletBroadcastSink>,
}

/// Bounded handoff that persists a verified transaction before publishing it.
#[derive(Clone)]
pub struct WalletBroadcastSink {
    sender: mpsc::Sender<WalletBroadcastRequest>,
    persistence: Option<Arc<RedbRebroadcastStore>>,
}

impl WalletBroadcastSink {
    /// Creates a durable sink backed by the network-bound rebroadcast store.
    pub fn durable(
        sender: mpsc::Sender<WalletBroadcastRequest>,
        persistence: Arc<RedbRebroadcastStore>,
    ) -> Self {
        Self {
            sender,
            persistence: Some(persistence),
        }
    }

    fn ephemeral(sender: mpsc::Sender<WalletBroadcastRequest>) -> Self {
        Self {
            sender,
            persistence: None,
        }
    }

    fn try_send(&self, request: WalletBroadcastRequest) -> Result<(), WalletBroadcastSinkError> {
        validate_standard_transaction(&request.transaction, request.fee_sats)
            .map_err(|_| WalletBroadcastSinkError::Admission)?;
        let permit = self
            .sender
            .try_reserve()
            .map_err(|_| WalletBroadcastSinkError::Unavailable)?;
        if let Some(persistence) = &self.persistence {
            persistence
                .enqueue(&request.transaction, current_unix_time()?)
                .map_err(|error| match error {
                    RebroadcastStoreError::Conflict(_) => WalletBroadcastSinkError::Admission,
                    _ => WalletBroadcastSinkError::Unavailable,
                })?;
        }
        permit.send(request);
        Ok(())
    }
}

#[derive(Debug)]
enum WalletBroadcastSinkError {
    Admission,
    Unavailable,
}

fn current_unix_time() -> Result<u64, WalletBroadcastSinkError> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .map_err(|_| WalletBroadcastSinkError::Unavailable)
}

/// One consensus-verified wallet transaction awaiting a peer socket write.
pub struct WalletBroadcastRequest {
    /// Exact transaction returned by wallet finalization.
    pub transaction: bitcoin::Transaction,
    /// Exact fee derived from the validated wallet prevouts.
    pub fee_sats: u64,
    /// Completion signal sent only after the active peer write finishes.
    pub result: oneshot::Sender<Result<(), ()>>,
}

struct AddressRateLimiter {
    available: u8,
    last_refill: Instant,
}

impl AddressRateLimiter {
    fn new() -> Self {
        Self {
            available: WALLET_ADDRESS_BURST,
            last_refill: Instant::now(),
        }
    }

    fn take(&mut self, now: Instant) -> bool {
        let intervals = now.saturating_duration_since(self.last_refill).as_secs()
            / WALLET_ADDRESS_REFILL_INTERVAL.as_secs();
        if intervals > 0 {
            let refill = u8::try_from(intervals.min(u64::from(WALLET_ADDRESS_BURST)))
                .expect("refill is capped to u8 burst");
            self.available = self
                .available
                .saturating_add(refill)
                .min(WALLET_ADDRESS_BURST);
            self.last_refill += Duration::from_secs(
                intervals.saturating_mul(WALLET_ADDRESS_REFILL_INTERVAL.as_secs()),
            );
        }
        if self.available == 0 {
            return false;
        }
        self.available -= 1;
        true
    }
}

impl LocalAuthToken {
    /// Validates an ASCII token read from an owner-only file.
    pub fn new(token: impl AsRef<str>) -> Result<Self, &'static str> {
        Ok(Self(Arc::new(RwLock::new(Some(Self::validate(token)?)))))
    }

    fn validate(token: impl AsRef<str>) -> Result<Arc<[u8]>, &'static str> {
        let token = token.as_ref().as_bytes();
        if !(MIN_WALLET_AUTH_TOKEN_LEN..=MAX_WALLET_AUTH_TOKEN_LEN).contains(&token.len())
            || !token.iter().all(u8::is_ascii_graphic)
        {
            return Err("local API token must be 32-256 printable ASCII bytes");
        }
        Ok(Arc::from(token))
    }

    /// Atomically replaces the credential shared by every cloned router state.
    pub fn rotate(&self, token: impl AsRef<str>) -> Result<(), &'static str> {
        let token = Self::validate(token)?;
        *self
            .0
            .write()
            .map_err(|_| "local API token lock poisoned")? = Some(token);
        Ok(())
    }

    /// Disables all authorization until a valid credential is rotated in.
    pub fn invalidate(&self) {
        if let Ok(mut token) = self.0.write() {
            *token = None;
        }
    }

    /// Checks one raw HTTP `Authorization` header using constant-time token comparison.
    ///
    /// The complete header grammar remains deliberately narrow: exactly one
    /// case-insensitive `Bearer` scheme, one space, and the configured token.
    #[must_use]
    pub fn authorizes(&self, header: &[u8]) -> bool {
        let Ok(header) = std::str::from_utf8(header) else {
            return false;
        };
        let Some((scheme, supplied)) = header.split_once(' ') else {
            return false;
        };
        let Ok(configured) = self.0.read() else {
            return false;
        };
        let Some(configured) = configured.as_ref() else {
            return false;
        };
        scheme.eq_ignore_ascii_case("Bearer")
            && supplied.len() <= MAX_WALLET_AUTH_TOKEN_LEN
            && constant_time_eq(supplied.as_bytes(), configured)
    }
}

impl AuthorizationAuditLog {
    /// Opens or creates a bounded append-only audit file.
    ///
    /// Existing files must be regular, owner-only, single-link files on Unix.
    /// New Unix files are created with mode `0600`.
    pub fn open(path: impl AsRef<FsPath>) -> Result<Self, String> {
        let path = path.as_ref();
        let (file, created) = match wallet_audit_open_options().create_new(true).open(path) {
            Ok(file) => (file, true),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                let before = std::fs::symlink_metadata(path).map_err(|error| {
                    format!(
                        "inspect local authorization audit log {}: {error}",
                        path.display()
                    )
                })?;
                validate_wallet_audit_metadata(path, &before)?;
                let file = wallet_audit_open_options().open(path).map_err(|error| {
                    format!(
                        "open local authorization audit log {}: {error}",
                        path.display()
                    )
                })?;
                let after = file.metadata().map_err(|error| {
                    format!(
                        "inspect local authorization audit log {}: {error}",
                        path.display()
                    )
                })?;
                validate_wallet_audit_metadata(path, &after)?;
                validate_wallet_audit_identity(path, &before, &after)?;
                (file, false)
            }
            Err(error) => {
                return Err(format!(
                    "create local authorization audit log {}: {error}",
                    path.display()
                ));
            }
        };
        let metadata = file.metadata().map_err(|error| {
            format!(
                "inspect local authorization audit log {}: {error}",
                path.display()
            )
        })?;
        validate_wallet_audit_metadata(path, &metadata)?;
        validate_wallet_audit_tail(path, &file, metadata.len())?;
        if created {
            sync_wallet_audit_parent(path)?;
        }
        Ok(Self(Arc::new(Mutex::new(WalletAuditWriter {
            file,
            bytes: metadata.len(),
        }))))
    }

    fn record(&self, method: &str, path: &str, authorized: bool) -> Result<(), String> {
        let unix_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| "system time precedes the Unix epoch".to_owned())?
            .as_secs();
        let mut line = serde_json::to_vec(&WalletAuditRecord {
            version: 1,
            unix_time,
            method,
            path,
            authorization: if authorized { "accepted" } else { "rejected" },
        })
        .map_err(|error| format!("serialize local authorization audit record: {error}"))?;
        line.push(b'\n');
        let line_len = u64::try_from(line.len())
            .map_err(|_| "local authorization audit record too large".to_owned())?;
        let mut writer = self
            .0
            .lock()
            .map_err(|_| "local authorization audit log lock poisoned".to_owned())?;
        if writer.bytes.saturating_add(line_len) > MAX_AUTHORIZATION_AUDIT_BYTES {
            return Err("local authorization audit log reached its size limit".to_owned());
        }
        let previous_bytes = writer.bytes;
        if let Err(error) = writer
            .file
            .write_all(&line)
            .and_then(|()| writer.file.sync_data())
        {
            let rollback = writer
                .file
                .set_len(previous_bytes)
                .and_then(|()| writer.file.sync_data());
            writer.bytes = MAX_AUTHORIZATION_AUDIT_BYTES;
            return match rollback {
                Ok(()) => Err(format!("write local authorization audit record: {error}")),
                Err(rollback) => Err(format!(
                    "write local authorization audit record: {error}; rollback failed: {rollback}"
                )),
            };
        }
        writer.bytes += line_len;
        Ok(())
    }
}

fn wallet_audit_open_options() -> OpenOptions {
    let mut options = OpenOptions::new();
    options.read(true).append(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options
}

fn validate_wallet_audit_tail(path: &FsPath, file: &File, bytes: u64) -> Result<(), String> {
    if bytes == 0 {
        return Ok(());
    }
    let mut reader = file.try_clone().map_err(|error| {
        format!(
            "inspect local authorization audit log {}: {error}",
            path.display()
        )
    })?;
    reader
        .seek(SeekFrom::Start(bytes - 1))
        .and_then(|_| {
            let mut tail = [0_u8; 1];
            reader.read_exact(&mut tail)?;
            if tail == [b'\n'] {
                Ok(())
            } else {
                Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "audit log ends with a partial record",
                ))
            }
        })
        .map_err(|error| {
            format!(
                "validate local authorization audit log {}: {error}",
                path.display()
            )
        })
}

#[cfg(unix)]
fn sync_wallet_audit_parent(path: &FsPath) -> Result<(), String> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| FsPath::new("."));
    File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| {
            format!(
                "sync local authorization audit log directory {}: {error}",
                parent.display()
            )
        })
}

#[cfg(not(unix))]
fn sync_wallet_audit_parent(_: &FsPath) -> Result<(), String> {
    Ok(())
}

fn validate_wallet_audit_metadata(
    path: &FsPath,
    metadata: &std::fs::Metadata,
) -> Result<(), String> {
    if !metadata.file_type().is_file() {
        return Err(format!(
            "local authorization audit log {} must be a regular file",
            path.display()
        ));
    }
    if metadata.len() > MAX_AUTHORIZATION_AUDIT_BYTES {
        return Err(format!(
            "local authorization audit log {} exceeds {MAX_AUTHORIZATION_AUDIT_BYTES} bytes",
            path.display()
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        if metadata.permissions().mode() & 0o077 != 0 {
            return Err(format!(
                "local authorization audit log {} permissions must not grant group or other access",
                path.display()
            ));
        }
        if metadata.nlink() != 1 {
            return Err(format!(
                "local authorization audit log {} must have exactly one hard link",
                path.display()
            ));
        }
    }
    Ok(())
}

#[cfg(unix)]
fn validate_wallet_audit_identity(
    path: &FsPath,
    before: &std::fs::Metadata,
    after: &std::fs::Metadata,
) -> Result<(), String> {
    use std::os::unix::fs::MetadataExt;
    if before.dev() != after.dev() || before.ino() != after.ino() {
        return Err(format!(
            "local authorization audit log {} changed while it was opened",
            path.display()
        ));
    }
    Ok(())
}

#[cfg(not(unix))]
fn validate_wallet_audit_identity(
    _: &FsPath,
    _: &std::fs::Metadata,
    _: &std::fs::Metadata,
) -> Result<(), String> {
    Ok(())
}

/// Read-only index required by explorer routes. Implement this against the node's block/tx indexes.
pub trait ExplorerIndex: Send + Sync + 'static {
    /// Returns a block summary by height.
    fn block(&self, height: u32) -> Result<Option<ExplorerBlock>, String>;
    /// Returns a transaction summary by txid.
    fn transaction(&self, txid: &str) -> Result<Option<ExplorerTransaction>, String>;
    /// Validates an address against the index network before querying storage.
    fn validate_address(&self, _address: &str) -> Result<(), String> {
        Ok(())
    }
    /// Returns a bounded slice of current UTXOs for a checked Bitcoin address.
    fn address_utxos(
        &self,
        address: &str,
        offset: u32,
        limit: u32,
    ) -> Result<Vec<ExplorerUtxo>, String>;
}

/// Thread-safe in-memory explorer index for embedded and regtest deployments.
///
/// A production daemon should replace this with a persistent projection that is
/// updated in the same lifecycle as validated block connect/disconnect events.
#[derive(Default)]
pub struct MemoryExplorerIndex {
    blocks: RwLock<HashMap<u32, ExplorerBlock>>,
    transactions: RwLock<HashMap<String, ExplorerTransaction>>,
    address_utxos: RwLock<HashMap<String, Vec<ExplorerUtxo>>>,
}

impl MemoryExplorerIndex {
    /// Records or replaces a block summary at its height.
    pub fn upsert_block(&self, block: ExplorerBlock) {
        self.blocks
            .write()
            .expect("explorer block lock not poisoned")
            .insert(block.height, block);
    }

    /// Records or replaces a transaction summary by txid.
    pub fn upsert_transaction(&self, transaction: ExplorerTransaction) {
        self.transactions
            .write()
            .expect("explorer transaction lock not poisoned")
            .insert(transaction.txid.clone(), transaction);
    }

    /// Replaces the current UTXO projection for an address.
    pub fn set_address_utxos(&self, address: impl Into<String>, mut utxos: Vec<ExplorerUtxo>) {
        utxos.sort_unstable_by(|left, right| {
            left.txid.cmp(&right.txid).then(left.vout.cmp(&right.vout))
        });
        self.address_utxos
            .write()
            .expect("explorer address lock not poisoned")
            .insert(address.into(), utxos);
    }
}

impl ExplorerIndex for MemoryExplorerIndex {
    fn block(&self, height: u32) -> Result<Option<ExplorerBlock>, String> {
        Ok(self
            .blocks
            .read()
            .map_err(|_| "explorer block lock poisoned".to_owned())?
            .get(&height)
            .cloned())
    }

    fn transaction(&self, txid: &str) -> Result<Option<ExplorerTransaction>, String> {
        Ok(self
            .transactions
            .read()
            .map_err(|_| "explorer transaction lock poisoned".to_owned())?
            .get(txid)
            .cloned())
    }

    fn address_utxos(
        &self,
        address: &str,
        offset: u32,
        limit: u32,
    ) -> Result<Vec<ExplorerUtxo>, String> {
        if offset > MAX_UTXO_PAGE_OFFSET || limit == 0 || limit > MAX_UTXO_PAGE_SIZE + 1 {
            return Err("explorer page window exceeds limits".to_owned());
        }
        let offset = usize::try_from(offset).map_err(|error| error.to_string())?;
        let limit = usize::try_from(limit).map_err(|error| error.to_string())?;
        Ok(self
            .address_utxos
            .read()
            .map_err(|_| "explorer address lock poisoned".to_owned())?
            .get(address)
            .into_iter()
            .flatten()
            .skip(offset)
            .take(limit)
            .cloned()
            .collect())
    }
}

/// Health response for load balancers and browser frontends.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct Health {
    /// Service label.
    pub service: &'static str,
    /// Service status.
    pub status: &'static str,
}

/// How the persistent explorer moved to the reported active-chain tip.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ExplorerEventKind {
    /// Initial state sent to a newly connected client.
    Snapshot,
    /// One validated active block was indexed.
    Connected,
    /// A stale explorer tip was removed during reorganization recovery.
    Disconnected,
    /// A snapshot-aware projection was rebuilt from current chainstate.
    Rebased,
}

/// Durable explorer-tip change delivered over the local SSE feed.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ExplorerEvent {
    /// Process-local monotonic ordering for subscription-race handling.
    pub sequence: u64,
    /// Transition responsible for this event.
    pub kind: ExplorerEventKind,
    /// New persistent explorer height after the transition.
    pub height: u32,
    /// New persistent explorer hash after the transition.
    pub hash: String,
}

/// Bounded fan-out for persistent explorer-tip notifications.
///
/// A newly subscribed client first receives the latest state. If it falls
/// behind the bounded broadcast ring, the stream emits a `resync` event rather
/// than pretending it observed every intermediate transition.
#[derive(Clone)]
pub struct ExplorerEventHub {
    sender: broadcast::Sender<ExplorerEvent>,
    latest: Arc<Mutex<ExplorerEvent>>,
    clients: Arc<Semaphore>,
}

impl ExplorerEventHub {
    /// Creates a feed whose initial snapshot matches the durable explorer tip.
    pub fn new(height: u32, hash: impl Into<String>) -> Self {
        let (sender, _) = broadcast::channel(128);
        Self {
            sender,
            latest: Arc::new(Mutex::new(ExplorerEvent {
                sequence: 0,
                kind: ExplorerEventKind::Snapshot,
                height,
                hash: hash.into(),
            })),
            clients: Arc::new(Semaphore::new(MAX_EXPLORER_EVENT_CLIENTS)),
        }
    }

    /// Publishes a transition after its explorer transaction has committed.
    pub fn publish(&self, kind: ExplorerEventKind, height: u32, hash: impl Into<String>) {
        let mut latest = self
            .latest
            .lock()
            .expect("explorer event lock not poisoned");
        let event = ExplorerEvent {
            sequence: latest
                .sequence
                .checked_add(1)
                .expect("explorer event sequence exhausted"),
            kind,
            height,
            hash: hash.into(),
        };
        latest.clone_from(&event);
        let _unused_when_no_clients = self.sender.send(event);
    }

    fn latest(&self) -> ExplorerEvent {
        self.latest
            .lock()
            .expect("explorer event lock not poisoned")
            .clone()
    }
}

/// Creates REST routes for the embedded read-only block explorer.
pub fn explorer_router<I: ExplorerIndex>(index: Arc<I>) -> Router {
    Router::new()
        .route("/", get(explorer_page))
        .route("/api/v1/health", get(health))
        .route("/api/v1/blocks/{height}", get(block::<I>))
        .route("/api/v1/tx/{txid}", get(transaction::<I>))
        .route("/api/v1/address/{address}/utxos", get(address_utxos::<I>))
        .with_state(index)
}

/// Creates the bounded SSE route for persistent explorer-tip transitions.
pub fn explorer_events_router(events: ExplorerEventHub) -> Router {
    Router::new()
        .route("/api/v1/events", get(explorer_events))
        .with_state(events)
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LocalRpcRequest {
    jsonrpc: String,
    id: serde_json::Value,
    method: String,
    #[serde(default)]
    params: serde_json::Value,
}

/// Creates an authenticated, audited JSON-RPC 2.0 route over the explorer index.
///
/// The initial read-only method set is `help`, `getblockhash`,
/// `rbtc.getblocksummary`, `rbtc.gettransaction`, and
/// `rbtc.getaddressutxos`. Request bodies, identifiers, parameters, and result
/// sizes are bounded.
pub fn rpc_router<I: ExplorerIndex>(
    index: Arc<I>,
    token: LocalAuthToken,
    audit: AuthorizationAuditLog,
) -> Router {
    Router::new()
        .route("/rpc", post(rpc_call::<I>))
        .with_state(index)
        .layer(DefaultBodyLimit::max(MAX_RPC_BODY_BYTES))
        .route_layer(middleware::from_fn_with_state(
            LocalRouteSecurity { token, audit },
            require_local_auth,
        ))
}

async fn rpc_call<I: ExplorerIndex>(
    State(index): State<Arc<I>>,
    body: Bytes,
) -> Json<serde_json::Value> {
    let Ok(request) = parse_local_rpc_request(&body) else {
        return rpc_error(&serde_json::Value::Null, -32600, "Invalid Request");
    };
    let id = request.id;
    match execute_rpc(index.as_ref(), &request.method, &request.params) {
        Ok(result) => Json(serde_json::json!({
            "jsonrpc": "2.0",
            "id": id,
            "result": result
        })),
        Err((code, message)) => rpc_error(&id, code, message),
    }
}

/// Checks the strict, size-bounded JSON-RPC 2.0 request envelope.
///
/// This parser does not execute a method or retain any supplied identifier or
/// parameter and is suitable for bounded parser regression and fuzz tests.
#[must_use]
pub fn is_well_formed_local_rpc_request(input: &[u8]) -> bool {
    parse_local_rpc_request(input).is_ok()
}

fn parse_local_rpc_request(input: &[u8]) -> Result<LocalRpcRequest, ()> {
    if input.len() > MAX_RPC_BODY_BYTES {
        return Err(());
    }
    let request = serde_json::from_slice::<LocalRpcRequest>(input).map_err(|_| ())?;
    if request.jsonrpc != "2.0"
        || !valid_rpc_id(&request.id)
        || request.method.is_empty()
        || request.method.len() > 64
        || request.method.bytes().any(|byte| byte.is_ascii_control())
    {
        return Err(());
    }
    Ok(request)
}

fn valid_rpc_id(id: &serde_json::Value) -> bool {
    match id {
        serde_json::Value::String(value) => value.len() <= 128,
        serde_json::Value::Number(_) | serde_json::Value::Null => true,
        _ => false,
    }
}

fn execute_rpc<I: ExplorerIndex>(
    index: &I,
    method: &str,
    params: &serde_json::Value,
) -> Result<serde_json::Value, (i64, &'static str)> {
    match method {
        "help" => {
            if !params.is_null() && params.as_array().is_none_or(|params| !params.is_empty()) {
                return Err((-32602, "Invalid params"));
            }
            Ok(serde_json::json!([
                "getblockhash",
                "rbtc.getblocksummary",
                "rbtc.gettransaction",
                "rbtc.getaddressutxos"
            ]))
        }
        "getblockhash" => {
            let (height,) = rpc_one_u32(params)?;
            let block = index
                .block(height)
                .map_err(|_| (-32603, "Internal error"))?
                .ok_or((-32001, "Block not found"))?;
            Ok(serde_json::Value::String(block.hash))
        }
        "rbtc.getblocksummary" => {
            let (height,) = rpc_one_u32(params)?;
            let block = index
                .block(height)
                .map_err(|_| (-32603, "Internal error"))?
                .ok_or((-32001, "Block not found"))?;
            serde_json::to_value(block).map_err(|_| (-32603, "Internal error"))
        }
        "rbtc.gettransaction" => {
            let txid = rpc_one_string(params, 64)?;
            if txid.len() != 64 || !txid.bytes().all(|byte| byte.is_ascii_hexdigit()) {
                return Err((-32602, "Invalid params"));
            }
            let transaction = index
                .transaction(txid)
                .map_err(|_| (-32603, "Internal error"))?
                .ok_or((-32002, "Transaction not found"))?;
            serde_json::to_value(transaction).map_err(|_| (-32603, "Internal error"))
        }
        "rbtc.getaddressutxos" => rpc_address_utxos(index, params),
        _ => Err((-32601, "Method not found")),
    }
}

fn rpc_one_u32(params: &serde_json::Value) -> Result<(u32,), (i64, &'static str)> {
    serde_json::from_value(params.clone()).map_err(|_| (-32602, "Invalid params"))
}

fn rpc_one_string(
    params: &serde_json::Value,
    maximum_len: usize,
) -> Result<&str, (i64, &'static str)> {
    let values = params.as_array().ok_or((-32602, "Invalid params"))?;
    if values.len() != 1 {
        return Err((-32602, "Invalid params"));
    }
    values[0]
        .as_str()
        .filter(|value| value.len() <= maximum_len)
        .ok_or((-32602, "Invalid params"))
}

fn rpc_address_utxos<I: ExplorerIndex>(
    index: &I,
    params: &serde_json::Value,
) -> Result<serde_json::Value, (i64, &'static str)> {
    let values = params.as_array().ok_or((-32602, "Invalid params"))?;
    if !(1..=3).contains(&values.len()) {
        return Err((-32602, "Invalid params"));
    }
    let address = values[0]
        .as_str()
        .filter(|address| address.len() <= 128)
        .ok_or((-32602, "Invalid params"))?;
    let offset = values
        .get(1)
        .map_or(Some(0), serde_json::Value::as_u64)
        .and_then(|value| u32::try_from(value).ok())
        .filter(|offset| *offset <= MAX_UTXO_PAGE_OFFSET)
        .ok_or((-32602, "Invalid params"))?;
    let limit = values
        .get(2)
        .map_or(
            Some(DEFAULT_UTXO_PAGE_SIZE.into()),
            serde_json::Value::as_u64,
        )
        .and_then(|value| u32::try_from(value).ok())
        .filter(|limit| (1..=MAX_UTXO_PAGE_SIZE).contains(limit))
        .ok_or((-32602, "Invalid params"))?;
    index
        .validate_address(address)
        .map_err(|_| (-32602, "Invalid params"))?;
    let mut utxos = index
        .address_utxos(address, offset, limit + 1)
        .map_err(|_| (-32603, "Internal error"))?;
    let has_more = utxos.len() > usize::try_from(limit).expect("bounded limit fits usize");
    utxos.truncate(usize::try_from(limit).expect("bounded limit fits usize"));
    serde_json::to_value(ExplorerUtxoPage {
        utxos,
        offset,
        limit,
        has_more,
    })
    .map_err(|_| (-32603, "Internal error"))
}

fn rpc_error(id: &serde_json::Value, code: i64, message: &'static str) -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": {
            "code": code,
            "message": message
        }
    }))
}

async fn explorer_events(
    State(events): State<ExplorerEventHub>,
) -> Result<Sse<impl tokio_stream::Stream<Item = Result<Event, Infallible>>>, StatusCode> {
    let permit = Arc::clone(&events.clients)
        .try_acquire_owned()
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
    let receiver = events.sender.subscribe();
    let mut snapshot = events.latest();
    snapshot.kind = ExplorerEventKind::Snapshot;
    let snapshot_sequence = snapshot.sequence;
    let initial = tokio_stream::once(Ok(tip_event(&snapshot)));
    let updates = BroadcastStream::new(receiver).filter_map(move |update| {
        let _keep_permit_for_stream_lifetime = &permit;
        match update {
            Ok(event) if event.sequence <= snapshot_sequence => None,
            Ok(event) => Some(Ok(tip_event(&event))),
            Err(BroadcastStreamRecvError::Lagged(missed)) => Some(Ok(Event::default()
                .event("resync")
                .data(format!(r#"{{"missed":{missed}}}"#)))),
        }
    });
    Ok(Sse::new(initial.chain(updates)).keep_alive(
        axum::response::sse::KeepAlive::new()
            .interval(Duration::from_secs(15))
            .text("keep-alive"),
    ))
}

fn tip_event(event: &ExplorerEvent) -> Event {
    let data = serde_json::to_string(&event).unwrap_or_else(|_| {
        r#"{"sequence":0,"kind":"snapshot","height":0,"hash":"serialization-error"}"#.to_owned()
    });
    Event::default().event("tip").data(data)
}

const EXPLORER_HTML: &str = r#"<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>rBTC Explorer</title><style>
:root{color-scheme:dark;background:#0b0f14;color:#d9e2ec;font:15px system-ui,sans-serif}body{max-width:900px;margin:0 auto;padding:32px 20px}h1{color:#f7931a}section{background:#131a22;border:1px solid #263241;border-radius:10px;padding:18px;margin:16px 0}form{display:flex;gap:8px}input{flex:1;background:#0b0f14;color:inherit;border:1px solid #405166;border-radius:6px;padding:10px}button{background:#f7931a;color:#111;border:0;border-radius:6px;padding:10px 16px;font-weight:700}pre{white-space:pre-wrap;word-break:break-word;min-height:24px}.muted{color:#91a4b7}</style></head>
<body><h1>rBTC Explorer</h1><p class="muted">Local, read-only active-chain explorer · <span id="live-tip">connecting to live tip…</span></p>
<section><h2>Block height</h2><form data-kind="blocks"><input inputmode="numeric" required placeholder="Height"><button>Search</button></form><pre></pre></section>
<section><h2>Transaction</h2><form data-kind="tx"><input required placeholder="txid"><button>Search</button></form><pre></pre></section>
<section><h2>Address UTXOs</h2><form data-kind="address"><input required placeholder="Checked Bitcoin address"><button>Search</button></form><pre></pre></section>
<section><h2>Watch-only wallet</h2><p class="muted">Optional authenticated wallet; token stays only in this page's memory.</p><form id="wallet"><input type="password" autocomplete="off" required placeholder="Bearer token"><button value="status">Status</button><button value="balance">Balance</button><button value="transactions">Transactions</button><button value="utxos">UTXOs</button><button value="address">New address</button></form><pre></pre></section>
<script>const live=document.querySelector('#live-tip'),events=new EventSource('/api/v1/events');events.addEventListener('tip',e=>{const t=JSON.parse(e.data);live.textContent=`${t.height}:${t.hash} (${t.kind})`});events.addEventListener('resync',()=>{live.textContent='event gap detected; reconnecting…';events.close();location.reload()});events.onerror=()=>{live.textContent='live tip disconnected'};for(const f of document.querySelectorAll('form[data-kind]'))f.addEventListener('submit',async e=>{e.preventDefault();const q=f.querySelector('input').value.trim(),k=f.dataset.kind,o=f.nextElementSibling;const u=k==='blocks'?`/api/v1/blocks/${encodeURIComponent(q)}`:k==='tx'?`/api/v1/tx/${encodeURIComponent(q)}`:`/api/v1/address/${encodeURIComponent(q)}/utxos`;o.textContent='Loading…';try{const r=await fetch(u);o.textContent=r.ok?JSON.stringify(await r.json(),null,2):`HTTP ${r.status}`}catch(x){o.textContent=String(x)}});const w=document.querySelector('#wallet');w.addEventListener('submit',async e=>{e.preventDefault();const t=w.querySelector('input').value,a=e.submitter.value,o=w.nextElementSibling;o.textContent='Loading…';try{const r=await fetch(`/api/v1/wallet/${a}`,{method:a==='address'?'POST':'GET',headers:{Authorization:`Bearer ${t}`}});o.textContent=r.ok?JSON.stringify(await r.json(),null,2):`HTTP ${r.status}`}catch(x){o.textContent=String(x)}});</script>
</body></html>"#;

async fn explorer_page() -> (HeaderMap, Html<&'static str>) {
    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_SECURITY_POLICY,
        HeaderValue::from_static(
            "default-src 'none'; style-src 'unsafe-inline'; script-src 'unsafe-inline'; connect-src 'self'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'",
        ),
    );
    headers.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    (headers, Html(EXPLORER_HTML))
}

/// Creates wallet routes whose every authorization attempt is durably audited.
pub fn wallet_router(
    wallet: Arc<EmbeddedWallet>,
    token: LocalAuthToken,
    audit: AuthorizationAuditLog,
    broadcast: Option<mpsc::Sender<WalletBroadcastRequest>>,
) -> Router {
    wallet_router_with_sink(
        wallet,
        token,
        audit,
        broadcast.map(WalletBroadcastSink::ephemeral),
    )
}

/// Creates wallet routes with durable pre-handoff transaction persistence.
pub fn wallet_router_with_sink(
    wallet: Arc<EmbeddedWallet>,
    token: LocalAuthToken,
    audit: AuthorizationAuditLog,
    broadcast: Option<WalletBroadcastSink>,
) -> Router {
    let state = Arc::new(WalletApiState {
        wallet,
        address_limiter: Mutex::new(AddressRateLimiter::new()),
        psbt_limiter: Mutex::new(AddressRateLimiter::new()),
        broadcast,
    });
    Router::new()
        .route("/api/v1/wallet/status", get(wallet_status))
        .route("/api/v1/wallet/descriptors", get(wallet_descriptors))
        .route("/api/v1/wallet/balance", get(wallet_balance))
        .route("/api/v1/wallet/transactions", get(wallet_transactions))
        .route("/api/v1/wallet/utxos", get(wallet_utxos))
        .route("/api/v1/wallet/address", post(next_address))
        .route(
            "/api/v1/wallet/psbt",
            post(create_wallet_psbt).layer(DefaultBodyLimit::max(MAX_WALLET_PSBT_REQUEST_BYTES)),
        )
        .route(
            "/api/v1/wallet/psbt/finalize",
            post(finalize_wallet_psbt).layer(DefaultBodyLimit::max(
                MAX_WALLET_PSBT_FINALIZE_REQUEST_BYTES,
            )),
        )
        .route(
            "/api/v1/wallet/psbt/broadcast",
            post(broadcast_wallet_psbt).layer(DefaultBodyLimit::max(
                MAX_WALLET_PSBT_FINALIZE_REQUEST_BYTES,
            )),
        )
        .with_state(state)
        .route_layer(middleware::from_fn_with_state(
            LocalRouteSecurity { token, audit },
            require_local_auth,
        ))
}

async fn require_local_auth(
    State(security): State<LocalRouteSecurity>,
    request: Request,
    next: Next,
) -> Response {
    let authorized = request
        .headers()
        .get(header::AUTHORIZATION)
        .is_some_and(|value| security.token.authorizes(value.as_bytes()));
    if security
        .audit
        .record(request.method().as_str(), request.uri().path(), authorized)
        .is_err()
    {
        let mut response = StatusCode::SERVICE_UNAVAILABLE.into_response();
        response
            .headers_mut()
            .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
        return response;
    }
    if authorized {
        let mut response = next.run(request).await;
        response
            .headers_mut()
            .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
        return response;
    }
    let mut response = StatusCode::UNAUTHORIZED.into_response();
    response.headers_mut().insert(
        header::WWW_AUTHENTICATE,
        HeaderValue::from_static("Bearer realm=\"rbtc-local\""),
    );
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    let mut difference = left.len() ^ right.len();
    for index in 0..left.len().max(right.len()) {
        let left = left.get(index).copied().unwrap_or(0);
        let right = right.get(index).copied().unwrap_or(0);
        difference |= usize::from(left ^ right);
    }
    difference == 0
}

async fn health() -> Json<Health> {
    Json(Health {
        service: "rbtc",
        status: "ok",
    })
}
async fn block<I: ExplorerIndex>(
    State(index): State<Arc<I>>,
    Path(height): Path<u32>,
) -> ApiResult<ExplorerBlock> {
    index
        .block(height)
        .map_err(internal)?
        .ok_or(StatusCode::NOT_FOUND)
        .map(Json)
}
async fn transaction<I: ExplorerIndex>(
    State(index): State<Arc<I>>,
    Path(txid): Path<String>,
) -> ApiResult<ExplorerTransaction> {
    if txid.len() != 64 || !txid.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(StatusCode::BAD_REQUEST);
    }
    index
        .transaction(&txid)
        .map_err(internal)?
        .ok_or(StatusCode::NOT_FOUND)
        .map(Json)
}
async fn address_utxos<I: ExplorerIndex>(
    State(index): State<Arc<I>>,
    Path(address): Path<String>,
    Query(query): Query<UtxoPageQuery>,
) -> ApiResult<ExplorerUtxoPage> {
    if address.is_empty() || address.len() > 128 {
        return Err(StatusCode::BAD_REQUEST);
    }
    index
        .validate_address(&address)
        .map_err(|_| StatusCode::BAD_REQUEST)?;
    let offset = query.offset.unwrap_or(0);
    let limit = query.limit.unwrap_or(DEFAULT_UTXO_PAGE_SIZE);
    if offset > MAX_UTXO_PAGE_OFFSET || limit == 0 || limit > MAX_UTXO_PAGE_SIZE {
        return Err(StatusCode::BAD_REQUEST);
    }
    let limit_usize = usize::try_from(limit).map_err(internal)?;
    let mut utxos = index
        .address_utxos(&address, offset, limit + 1)
        .map_err(internal)?;
    let has_more = utxos.len() > limit_usize;
    utxos.truncate(limit_usize);
    Ok(Json(ExplorerUtxoPage {
        utxos,
        offset,
        limit,
        has_more,
    }))
}
async fn wallet_balance(State(state): State<Arc<WalletApiState>>) -> ApiResult<WalletBalance> {
    state.wallet.balance().map_err(internal).map(Json)
}
async fn wallet_status(State(state): State<Arc<WalletApiState>>) -> ApiResult<WalletStatus> {
    state.wallet.status().map_err(internal).map(Json)
}
async fn wallet_descriptors(
    State(state): State<Arc<WalletApiState>>,
) -> Json<WalletPublicDescriptors> {
    Json(state.wallet.public_descriptors())
}
async fn create_wallet_psbt(
    State(state): State<Arc<WalletApiState>>,
    body: Bytes,
) -> ApiResult<WalletPsbt> {
    let request = parse_wallet_psbt_request(&body).map_err(|_| StatusCode::BAD_REQUEST)?;
    let allowed = state
        .psbt_limiter
        .lock()
        .map_err(internal)?
        .take(Instant::now());
    if !allowed {
        return Err(StatusCode::TOO_MANY_REQUESTS);
    }
    state
        .wallet
        .create_psbt(&request)
        .map_err(|error| match error {
            WalletError::Psbt(_) => StatusCode::BAD_REQUEST,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        })
        .map(Json)
}
async fn finalize_wallet_psbt(
    State(state): State<Arc<WalletApiState>>,
    body: Bytes,
) -> ApiResult<WalletFinalizedTransaction> {
    let request = parse_wallet_psbt_finalize_request(&body).map_err(|_| StatusCode::BAD_REQUEST)?;
    let allowed = state
        .psbt_limiter
        .lock()
        .map_err(internal)?
        .take(Instant::now());
    if !allowed {
        return Err(StatusCode::TOO_MANY_REQUESTS);
    }
    state
        .wallet
        .finalize_psbt(&request)
        .map_err(|error| match error {
            WalletError::Psbt(_) => StatusCode::BAD_REQUEST,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        })
        .map(Json)
}

async fn broadcast_wallet_psbt(
    State(state): State<Arc<WalletApiState>>,
    body: Bytes,
) -> ApiResult<WalletFinalizedTransaction> {
    let request = parse_wallet_psbt_finalize_request(&body).map_err(|_| StatusCode::BAD_REQUEST)?;
    let allowed = state
        .psbt_limiter
        .lock()
        .map_err(internal)?
        .take(Instant::now());
    if !allowed {
        return Err(StatusCode::TOO_MANY_REQUESTS);
    }
    let (response, transaction) = state
        .wallet
        .finalize_psbt_with_transaction(&request)
        .map_err(|error| match error {
            WalletError::Psbt(_) => StatusCode::BAD_REQUEST,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        })?;
    let broadcast = state
        .broadcast
        .as_ref()
        .ok_or(StatusCode::SERVICE_UNAVAILABLE)?;
    let (result, completion) = oneshot::channel();
    broadcast
        .try_send(WalletBroadcastRequest {
            transaction,
            fee_sats: response.fee_sats,
            result,
        })
        .map_err(|error| match error {
            WalletBroadcastSinkError::Admission => StatusCode::BAD_REQUEST,
            WalletBroadcastSinkError::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
        })?;
    match tokio::time::timeout(WALLET_BROADCAST_TIMEOUT, completion).await {
        Ok(Ok(Ok(()))) => Ok(Json(response)),
        Ok(Ok(Err(())) | Err(_)) | Err(_) => Err(StatusCode::SERVICE_UNAVAILABLE),
    }
}
async fn wallet_transactions(
    State(state): State<Arc<WalletApiState>>,
    Query(query): Query<UtxoPageQuery>,
) -> ApiResult<WalletTransactionPage> {
    let offset = query.offset.unwrap_or(0);
    let limit = query.limit.unwrap_or(DEFAULT_UTXO_PAGE_SIZE);
    if offset > MAX_UTXO_PAGE_OFFSET || limit == 0 || limit > MAX_UTXO_PAGE_SIZE {
        return Err(StatusCode::BAD_REQUEST);
    }
    let limit_usize = usize::try_from(limit).map_err(internal)?;
    let mut transactions = state
        .wallet
        .transactions(offset, limit + 1)
        .map_err(internal)?;
    let has_more = transactions.len() > limit_usize;
    transactions.truncate(limit_usize);
    Ok(Json(WalletTransactionPage {
        transactions,
        offset,
        limit,
        has_more,
    }))
}
async fn wallet_utxos(
    State(state): State<Arc<WalletApiState>>,
    Query(query): Query<UtxoPageQuery>,
) -> ApiResult<WalletUtxoPage> {
    let offset = query.offset.unwrap_or(0);
    let limit = query.limit.unwrap_or(DEFAULT_UTXO_PAGE_SIZE);
    if offset > MAX_UTXO_PAGE_OFFSET || limit == 0 || limit > MAX_UTXO_PAGE_SIZE {
        return Err(StatusCode::BAD_REQUEST);
    }
    let limit_usize = usize::try_from(limit).map_err(internal)?;
    let mut utxos = state.wallet.utxos(offset, limit + 1).map_err(internal)?;
    let has_more = utxos.len() > limit_usize;
    utxos.truncate(limit_usize);
    Ok(Json(WalletUtxoPage {
        utxos,
        offset,
        limit,
        has_more,
    }))
}
async fn next_address(State(state): State<Arc<WalletApiState>>) -> ApiResult<WalletAddress> {
    let allowed = state
        .address_limiter
        .lock()
        .map_err(internal)?
        .take(Instant::now());
    if !allowed {
        return Err(StatusCode::TOO_MANY_REQUESTS);
    }
    state
        .wallet
        .reveal_receive_address()
        .map_err(internal)
        .map(Json)
}

type ApiResult<T> = Result<Json<T>, StatusCode>;
fn internal<E>(_: E) -> StatusCode {
    StatusCode::INTERNAL_SERVER_ERROR
}

#[derive(Default, Deserialize)]
struct UtxoPageQuery {
    offset: Option<u32>,
    limit: Option<u32>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{body::Body, http::Request};
    use bitcoin::Network;
    use tower::ServiceExt;

    const RECEIVE_DESCRIPTOR: &str = "wpkh([41f2aed0/84h/1h/0h]tpubDDFSdQWw75hk1ewbwnNpPp5DvXFRKt68ioPoyJDY752cNHKkFxPWqkqCyCf4hxrEfpuxh46QisehL3m8Bi6MsAv394QVLopwbtfvryFQNUH/0/*)#g0w0ymmw";
    const CHANGE_DESCRIPTOR: &str = "wpkh([41f2aed0/84h/1h/0h]tpubDDFSdQWw75hk1ewbwnNpPp5DvXFRKt68ioPoyJDY752cNHKkFxPWqkqCyCf4hxrEfpuxh46QisehL3m8Bi6MsAv394QVLopwbtfvryFQNUH/1/*)#emtwewtk";

    fn broadcast_test_transaction(marker: u8) -> bitcoin::Transaction {
        use bitcoin::{
            Amount, OutPoint, ScriptBuf, Sequence, TxIn, TxOut, Txid, Witness, absolute::LockTime,
            hashes::Hash, transaction::Version,
        };
        bitcoin::Transaction {
            version: Version::TWO,
            lock_time: LockTime::ZERO,
            input: vec![TxIn {
                previous_output: OutPoint::new(Txid::from_byte_array([marker; 32]), 0),
                script_sig: ScriptBuf::new(),
                sequence: Sequence::MAX,
                witness: Witness::new(),
            }],
            output: vec![TxOut {
                value: Amount::from_sat(1_000),
                script_pubkey: ScriptBuf::new_p2wpkh(&bitcoin::WPubkeyHash::from_byte_array(
                    [marker; 20],
                )),
            }],
        }
    }

    #[test]
    fn durable_broadcast_sink_persists_before_channel_handoff() {
        let directory = tempfile::tempdir().unwrap();
        let store = Arc::new(
            RedbRebroadcastStore::open(directory.path().join("rebroadcast.redb"), Network::Regtest)
                .unwrap(),
        );
        let (sender, mut receiver) = mpsc::channel(1);
        let sink = WalletBroadcastSink::durable(sender, Arc::clone(&store));
        let transaction = broadcast_test_transaction(1);
        let (result, _completion) = oneshot::channel();
        sink.try_send(WalletBroadcastRequest {
            transaction: transaction.clone(),
            fee_sats: 1_000,
            result,
        })
        .unwrap();
        assert_eq!(store.unconfirmed_transactions().unwrap(), vec![transaction]);
        assert!(receiver.try_recv().is_ok());
    }

    #[test]
    fn full_broadcast_channel_does_not_persist_rejected_request() {
        let directory = tempfile::tempdir().unwrap();
        let store = Arc::new(
            RedbRebroadcastStore::open(directory.path().join("rebroadcast.redb"), Network::Regtest)
                .unwrap(),
        );
        let (sender, _receiver) = mpsc::channel(1);
        let (occupied_result, _occupied_completion) = oneshot::channel();
        sender
            .try_send(WalletBroadcastRequest {
                transaction: broadcast_test_transaction(1),
                fee_sats: 1_000,
                result: occupied_result,
            })
            .unwrap();
        let sink = WalletBroadcastSink::durable(sender, Arc::clone(&store));
        let (result, _completion) = oneshot::channel();
        assert!(
            sink.try_send(WalletBroadcastRequest {
                transaction: broadcast_test_transaction(2),
                fee_sats: 1_000,
                result,
            })
            .is_err()
        );
        assert_eq!(store.len().unwrap(), 0);
    }

    #[test]
    fn broadcast_sink_rejects_policy_and_persistent_input_conflicts() {
        let directory = tempfile::tempdir().unwrap();
        let store = Arc::new(
            RedbRebroadcastStore::open(directory.path().join("rebroadcast.redb"), Network::Regtest)
                .unwrap(),
        );
        let (sender, mut receiver) = mpsc::channel(1);
        let sink = WalletBroadcastSink::durable(sender, Arc::clone(&store));

        let mut nonstandard = broadcast_test_transaction(3);
        nonstandard.output[0].script_pubkey = bitcoin::ScriptBuf::new();
        let (result, _completion) = oneshot::channel();
        assert!(matches!(
            sink.try_send(WalletBroadcastRequest {
                transaction: nonstandard,
                fee_sats: 1_000,
                result,
            }),
            Err(WalletBroadcastSinkError::Admission)
        ));
        assert_eq!(store.len().unwrap(), 0);

        let first = broadcast_test_transaction(4);
        let (result, _completion) = oneshot::channel();
        sink.try_send(WalletBroadcastRequest {
            transaction: first.clone(),
            fee_sats: 1_000,
            result,
        })
        .unwrap();
        drop(receiver.try_recv().unwrap());
        let mut conflict = first;
        conflict.output[0].value = bitcoin::Amount::from_sat(999);
        let (result, _completion) = oneshot::channel();
        assert!(matches!(
            sink.try_send(WalletBroadcastRequest {
                transaction: conflict,
                fee_sats: 1_000,
                result,
            }),
            Err(WalletBroadcastSinkError::Admission)
        ));
        assert_eq!(store.len().unwrap(), 1);
        assert!(receiver.try_recv().is_err());
    }

    struct TestIndex;
    impl ExplorerIndex for TestIndex {
        fn block(&self, height: u32) -> Result<Option<ExplorerBlock>, String> {
            Ok((height == 1).then(|| ExplorerBlock {
                height,
                hash: "00".into(),
                time: 1,
                transaction_count: 1,
            }))
        }
        fn transaction(&self, _: &str) -> Result<Option<ExplorerTransaction>, String> {
            Ok(None)
        }
        fn validate_address(&self, address: &str) -> Result<(), String> {
            if address == "invalid" {
                Err("invalid address".to_owned())
            } else {
                Ok(())
            }
        }
        fn address_utxos(
            &self,
            _: &str,
            offset: u32,
            limit: u32,
        ) -> Result<Vec<ExplorerUtxo>, String> {
            Ok((0..3)
                .map(|vout| ExplorerUtxo {
                    txid: format!("{vout:064x}"),
                    vout,
                    value_sats: u64::from(vout),
                    height: 1,
                })
                .skip(usize::try_from(offset).unwrap())
                .take(usize::try_from(limit).unwrap())
                .collect())
        }
    }

    #[tokio::test]
    async fn explorer_returns_health_and_not_found() {
        let app = explorer_router(Arc::new(TestIndex));
        let page = app
            .clone()
            .oneshot(Request::get("/").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(page.status(), StatusCode::OK);
        assert!(page.headers().contains_key(header::CONTENT_SECURITY_POLICY));
        let health = app
            .clone()
            .oneshot(Request::get("/api/v1/health").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(health.status(), StatusCode::OK);
        let missing = app
            .oneshot(
                Request::get("/api/v1/blocks/2")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(missing.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn explorer_events_start_with_snapshot_and_bound_slow_clients() {
        let events = ExplorerEventHub::new(7, "initial-hash");
        let mut receiver = events.sender.subscribe();
        events.publish(ExplorerEventKind::Connected, 8, "connected-hash");
        let connected = receiver.recv().await.unwrap();
        assert_eq!(connected.sequence, 1);
        assert_eq!(connected.kind, ExplorerEventKind::Connected);
        assert_eq!(connected.height, 8);
        assert_eq!(connected.hash, "connected-hash");
        assert_eq!(events.latest(), connected);

        let response = explorer_events_router(events.clone())
            .oneshot(Request::get("/api/v1/events").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers()[header::CONTENT_TYPE],
            "text/event-stream"
        );
        assert_eq!(response.headers()[header::CACHE_CONTROL], "no-cache");
        let mut body = response.into_body().into_data_stream();
        let first = body.next().await.unwrap().unwrap();
        let first = std::str::from_utf8(&first).unwrap();
        assert!(first.contains("event: tip"));
        assert!(first.contains(r#""sequence":1"#));
        assert!(first.contains(r#""kind":"snapshot""#));
        assert!(first.contains(r#""height":8"#));

        let mut slow = events.sender.subscribe();
        for height in 9..=140 {
            events.publish(
                ExplorerEventKind::Connected,
                height,
                format!("hash-{height}"),
            );
        }
        assert!(matches!(
            slow.recv().await,
            Err(broadcast::error::RecvError::Lagged(_))
        ));
        assert_eq!(events.latest().height, 140);
    }

    #[test]
    fn concurrent_explorer_publishers_serialize_sequence_and_latest_state() {
        const PUBLISHERS: usize = 8;
        const EVENTS_PER_PUBLISHER: usize = 8;

        let events = ExplorerEventHub::new(0, "initial-hash");
        let mut receiver = events.sender.subscribe();
        let publishers = (0..PUBLISHERS)
            .map(|publisher| {
                let events = events.clone();
                std::thread::spawn(move || {
                    for event in 0..EVENTS_PER_PUBLISHER {
                        let height =
                            u32::try_from(publisher * EVENTS_PER_PUBLISHER + event + 1).unwrap();
                        events.publish(
                            ExplorerEventKind::Connected,
                            height,
                            format!("hash-{publisher}-{event}"),
                        );
                    }
                })
            })
            .collect::<Vec<_>>();

        for publisher in publishers {
            publisher.join().unwrap();
        }

        let expected = u64::try_from(PUBLISHERS * EVENTS_PER_PUBLISHER).unwrap();
        let mut last = None;
        for sequence in 1..=expected {
            let event = receiver.try_recv().unwrap();
            assert_eq!(event.sequence, sequence);
            last = Some(event);
        }
        assert_eq!(events.latest(), last.unwrap());
        assert!(matches!(
            receiver.try_recv(),
            Err(broadcast::error::TryRecvError::Empty)
        ));
    }

    #[tokio::test]
    async fn explorer_events_cap_concurrent_streams() {
        let app = explorer_events_router(ExplorerEventHub::new(0, "genesis"));
        let mut active = Vec::with_capacity(MAX_EXPLORER_EVENT_CLIENTS);
        for _ in 0..MAX_EXPLORER_EVENT_CLIENTS {
            let response = app
                .clone()
                .oneshot(Request::get("/api/v1/events").body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            active.push(response);
        }
        let exhausted = app
            .clone()
            .oneshot(Request::get("/api/v1/events").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(exhausted.status(), StatusCode::SERVICE_UNAVAILABLE);

        active.pop();
        let recovered = app
            .oneshot(Request::get("/api/v1/events").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(recovered.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn explorer_bounds_and_pages_untrusted_queries() {
        let app = explorer_router(Arc::new(TestIndex));
        let page = app
            .clone()
            .oneshot(
                Request::get("/api/v1/address/bcrt1test/utxos?offset=1&limit=1")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(page.status(), StatusCode::OK);
        let body = axum::body::to_bytes(page.into_body(), 4096).await.unwrap();
        let page: ExplorerUtxoPage = serde_json::from_slice(&body).unwrap();
        assert_eq!(page.utxos.len(), 1);
        assert_eq!(page.utxos[0].vout, 1);
        assert!(page.has_more);

        for uri in [
            "/api/v1/address/bcrt1test/utxos?limit=0",
            "/api/v1/address/bcrt1test/utxos?limit=101",
            "/api/v1/address/bcrt1test/utxos?offset=10001",
            "/api/v1/address/invalid/utxos",
            "/api/v1/address/bcrt1test/utxos?limit=invalid",
            "/api/v1/tx/not-a-txid",
        ] {
            let response = app
                .clone()
                .oneshot(Request::get(uri).body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{uri}");
        }
    }

    #[tokio::test]
    async fn rpc_requires_independent_auth_and_serves_bounded_read_methods() {
        let directory = tempfile::tempdir().unwrap();
        let audit_path = directory.path().join("api-auth-audit.jsonl");
        let token = "r".repeat(MIN_WALLET_AUTH_TOKEN_LEN);
        let app = rpc_router(
            Arc::new(TestIndex),
            LocalAuthToken::new(&token).unwrap(),
            AuthorizationAuditLog::open(&audit_path).unwrap(),
        );
        let unauthenticated_body = br#"{"jsonrpc":"2.0","id":1,"method":"help","params":["body-secret-must-not-be-audited"]}"#;
        let unauthenticated = app
            .clone()
            .oneshot(
                Request::post("/rpc")
                    .body(Body::from(unauthenticated_body.as_slice()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(unauthenticated.status(), StatusCode::UNAUTHORIZED);

        let block_hash = app
            .clone()
            .oneshot(
                Request::post("/rpc")
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::from(
                        r#"{"jsonrpc":"2.0","id":"block","method":"getblockhash","params":[1]}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(block_hash.status(), StatusCode::OK);
        assert_eq!(block_hash.headers()[header::CACHE_CONTROL], "no-store");
        let body = axum::body::to_bytes(block_hash.into_body(), 4096)
            .await
            .unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["jsonrpc"], "2.0");
        assert_eq!(body["id"], "block");
        assert_eq!(body["result"], "00");

        let utxos = app
            .oneshot(
                Request::post("/rpc")
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::from(
                        r#"{"jsonrpc":"2.0","id":2,"method":"rbtc.getaddressutxos","params":["bcrt1test",1,1]}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(utxos.status(), StatusCode::OK);
        let body = axum::body::to_bytes(utxos.into_body(), 4096).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["result"]["utxos"][0]["vout"], 1);
        assert_eq!(body["result"]["has_more"], true);

        let audit = std::fs::read_to_string(audit_path).unwrap();
        assert!(audit.contains(r#""path":"/rpc""#));
        assert!(audit.contains(r#""authorization":"accepted""#));
        assert!(audit.contains(r#""authorization":"rejected""#));
        assert!(!audit.contains(&token));
        assert!(!audit.contains("body-secret-must-not-be-audited"));
        assert!(!audit.contains("bcrt1test"));
    }

    #[tokio::test]
    async fn rpc_rejects_malformed_unbounded_and_unknown_requests() {
        let directory = tempfile::tempdir().unwrap();
        let token = "r".repeat(MIN_WALLET_AUTH_TOKEN_LEN);
        let app = rpc_router(
            Arc::new(TestIndex),
            LocalAuthToken::new(&token).unwrap(),
            AuthorizationAuditLog::open(directory.path().join("api-auth-audit.jsonl")).unwrap(),
        );
        for (body, expected_code) in [
            (r#"{"jsonrpc":"1.0","id":1,"method":"help"}"#, -32600),
            (
                r#"{"jsonrpc":"2.0","id":[],"method":"help","params":[]}"#,
                -32600,
            ),
            (
                r#"{"jsonrpc":"2.0","id":1,"method":"help","unexpected":true}"#,
                -32600,
            ),
            (
                r#"{"jsonrpc":"2.0","id":1,"method":"unknown","params":[]}"#,
                -32601,
            ),
            (
                r#"{"jsonrpc":"2.0","id":1,"method":"getblockhash","params":[1,2]}"#,
                -32602,
            ),
            (
                r#"{"jsonrpc":"2.0","id":1,"method":"rbtc.getaddressutxos","params":["invalid"]}"#,
                -32602,
            ),
        ] {
            let response = app
                .clone()
                .oneshot(
                    Request::post("/rpc")
                        .header(header::AUTHORIZATION, format!("Bearer {token}"))
                        .body(Body::from(body))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK, "{body}");
            let body = axum::body::to_bytes(response.into_body(), 4096)
                .await
                .unwrap();
            let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(body["error"]["code"], expected_code);
        }

        let oversized = app
            .oneshot(
                Request::post("/rpc")
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::from(vec![b' '; MAX_RPC_BODY_BYTES + 1]))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(oversized.status(), StatusCode::PAYLOAD_TOO_LARGE);
    }

    #[test]
    fn strict_rpc_envelope_parser_is_bounded_and_rejects_extensions() {
        assert!(is_well_formed_local_rpc_request(
            br#"{"jsonrpc":"2.0","id":1,"method":"help"}"#
        ));
        for input in [
            br#"{"jsonrpc":"1.0","id":1,"method":"help"}"#.as_slice(),
            br#"{"jsonrpc":"2.0","method":"help"}"#,
            br#"{"jsonrpc":"2.0","id":[],"method":"help"}"#,
            br#"{"jsonrpc":"2.0","id":1,"method":"help","extra":true}"#,
            br#"[{"jsonrpc":"2.0","id":1,"method":"help"}]"#,
        ] {
            assert!(!is_well_formed_local_rpc_request(input));
        }
        assert!(!is_well_formed_local_rpc_request(&vec![
            b' ';
            MAX_RPC_BODY_BYTES
                + 1
        ]));
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn wallet_routes_require_bearer_authentication() {
        let directory = tempfile::tempdir().unwrap();
        let wallet = Arc::new(
            EmbeddedWallet::open_or_create(
                directory.path().join("wallet.sqlite"),
                RECEIVE_DESCRIPTOR,
                CHANGE_DESCRIPTOR,
                Network::Testnet,
            )
            .unwrap(),
        );
        let token = "a".repeat(MIN_WALLET_AUTH_TOKEN_LEN);
        let app = wallet_router(
            wallet,
            WalletAuthToken::new(&token).unwrap(),
            WalletAuditLog::open(directory.path().join("wallet-api-audit.jsonl")).unwrap(),
            None,
        );

        for authorization in [None, Some("Bearer wrong-token")] {
            let mut request = Request::get("/api/v1/wallet/balance");
            if let Some(value) = authorization {
                request = request.header(header::AUTHORIZATION, value);
            }
            let response = app
                .clone()
                .oneshot(request.body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
            assert!(response.headers().contains_key(header::WWW_AUTHENTICATE));
            assert_eq!(response.headers()[header::CACHE_CONTROL], "no-store");
        }

        let balance = app
            .clone()
            .oneshot(
                Request::get("/api/v1/wallet/balance")
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(balance.status(), StatusCode::OK);
        assert_eq!(balance.headers()[header::CACHE_CONTROL], "no-store");
        let utxos = app
            .clone()
            .oneshot(
                Request::get("/api/v1/wallet/utxos?offset=0&limit=1")
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(utxos.status(), StatusCode::OK);
        let body = axum::body::to_bytes(utxos.into_body(), 4096).await.unwrap();
        let utxos: WalletUtxoPage = serde_json::from_slice(&body).unwrap();
        assert!(utxos.utxos.is_empty());
        assert!(!utxos.has_more);

        let unfunded = app
            .clone()
            .oneshot(
                Request::post("/api/v1/wallet/psbt")
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{"recipients":[],"fee_rate_sat_vb":1}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(unfunded.status(), StatusCode::BAD_REQUEST);
        let unknown_field = app
            .clone()
            .oneshot(
                Request::post("/api/v1/wallet/psbt")
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        r#"{"recipients":[],"fee_rate_sat_vb":1,"unknown":true}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(unknown_field.status(), StatusCode::BAD_REQUEST);
        let oversized = app
            .clone()
            .oneshot(
                Request::post("/api/v1/wallet/psbt")
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(vec![b' '; MAX_WALLET_PSBT_REQUEST_BYTES + 1]))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(oversized.status(), StatusCode::PAYLOAD_TOO_LARGE);

        let invalid_finalize = app
            .clone()
            .oneshot(
                Request::post("/api/v1/wallet/psbt/finalize")
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{"psbt":"not-base64"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(invalid_finalize.status(), StatusCode::BAD_REQUEST);
        let invalid_broadcast = app
            .clone()
            .oneshot(
                Request::post("/api/v1/wallet/psbt/broadcast")
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{"psbt":"not-base64"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(invalid_broadcast.status(), StatusCode::BAD_REQUEST);
        let unknown_finalize_field = app
            .clone()
            .oneshot(
                Request::post("/api/v1/wallet/psbt/finalize")
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{"psbt":"x","unknown":true}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(unknown_finalize_field.status(), StatusCode::BAD_REQUEST);
        let oversized_finalize = app
            .clone()
            .oneshot(
                Request::post("/api/v1/wallet/psbt/finalize")
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(vec![
                        b' ';
                        MAX_WALLET_PSBT_FINALIZE_REQUEST_BYTES + 1
                    ]))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(oversized_finalize.status(), StatusCode::PAYLOAD_TOO_LARGE);

        let address = app
            .clone()
            .oneshot(
                Request::post("/api/v1/wallet/address")
                    .header(header::AUTHORIZATION, format!("bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(address.status(), StatusCode::OK);
        let body = axum::body::to_bytes(address.into_body(), 4096)
            .await
            .unwrap();
        let address: WalletAddress = serde_json::from_slice(&body).unwrap();
        assert_eq!(address.index, 0);

        for _ in 1..WALLET_ADDRESS_BURST {
            let response = app
                .clone()
                .oneshot(
                    Request::post("/api/v1/wallet/address")
                        .header(header::AUTHORIZATION, format!("Bearer {token}"))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
        }
        let limited = app
            .oneshot(
                Request::post("/api/v1/wallet/address")
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(limited.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(limited.headers()[header::CACHE_CONTROL], "no-store");
    }

    #[tokio::test]
    async fn wallet_audit_records_only_bounded_non_secret_authorization_metadata() {
        let directory = tempfile::tempdir().unwrap();
        let wallet = Arc::new(
            EmbeddedWallet::open_or_create(
                directory.path().join("wallet.sqlite"),
                RECEIVE_DESCRIPTOR,
                CHANGE_DESCRIPTOR,
                Network::Testnet,
            )
            .unwrap(),
        );
        let audit_path = directory.path().join("wallet-api-audit.jsonl");
        let audit = WalletAuditLog::open(&audit_path).unwrap();
        let token = "audit-token-that-must-never-be-written".to_owned();
        let app = wallet_router(wallet, WalletAuthToken::new(&token).unwrap(), audit, None);

        let rejected = app
            .clone()
            .oneshot(
                Request::get("/api/v1/wallet/balance?private-query-value")
                    .header(
                        header::AUTHORIZATION,
                        "Bearer rejected-secret-that-must-not-be-written",
                    )
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(rejected.status(), StatusCode::UNAUTHORIZED);
        let accepted = app
            .oneshot(
                Request::get("/api/v1/wallet/balance?another-private-value")
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(accepted.status(), StatusCode::OK);

        let contents = std::fs::read_to_string(&audit_path).unwrap();
        let records = contents
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0]["method"], "GET");
        assert_eq!(records[0]["path"], "/api/v1/wallet/balance");
        assert_eq!(records[0]["authorization"], "rejected");
        assert_eq!(records[1]["authorization"], "accepted");
        for secret in [
            &token,
            "rejected-secret-that-must-not-be-written",
            "private-query-value",
            "another-private-value",
            "Authorization",
        ] {
            assert!(!contents.contains(secret), "{secret}");
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(audit_path).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
    }

    #[tokio::test]
    async fn full_wallet_audit_log_fails_closed_before_address_revelation() {
        let directory = tempfile::tempdir().unwrap();
        let wallet = Arc::new(
            EmbeddedWallet::open_or_create(
                directory.path().join("wallet.sqlite"),
                RECEIVE_DESCRIPTOR,
                CHANGE_DESCRIPTOR,
                Network::Testnet,
            )
            .unwrap(),
        );
        let audit_path = directory.path().join("wallet-api-audit.jsonl");
        let mut file = File::create(&audit_path).unwrap();
        file.set_len(MAX_WALLET_AUDIT_BYTES).unwrap();
        file.seek(SeekFrom::End(-1)).unwrap();
        file.write_all(b"\n").unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&audit_path, std::fs::Permissions::from_mode(0o600)).unwrap();
        }
        let audit = WalletAuditLog::open(audit_path).unwrap();
        let token = "a".repeat(MIN_WALLET_AUTH_TOKEN_LEN);
        let app = wallet_router(
            Arc::clone(&wallet),
            WalletAuthToken::new(&token).unwrap(),
            audit,
            None,
        );
        let response = app
            .oneshot(
                Request::post("/api/v1/wallet/address")
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(response.headers()[header::CACHE_CONTROL], "no-store");
        assert_eq!(wallet.status().unwrap().issued_receive_addresses, 0);
    }

    #[cfg(unix)]
    #[test]
    fn wallet_audit_rejects_over_permissive_symlink_and_hardlink_files() {
        use std::os::unix::fs::{PermissionsExt, symlink};

        let directory = tempfile::tempdir().unwrap();
        let target = directory.path().join("target.jsonl");
        File::create(&target).unwrap();
        std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o600)).unwrap();

        let over_permissive = directory.path().join("permissive.jsonl");
        File::create(&over_permissive).unwrap();
        std::fs::set_permissions(&over_permissive, std::fs::Permissions::from_mode(0o640)).unwrap();
        assert!(
            WalletAuditLog::open(over_permissive)
                .err()
                .unwrap()
                .contains("permissions")
        );

        let symlink_path = directory.path().join("symlink.jsonl");
        symlink(&target, &symlink_path).unwrap();
        assert!(
            WalletAuditLog::open(symlink_path)
                .err()
                .unwrap()
                .contains("regular file")
        );

        let hardlink_path = directory.path().join("hardlink.jsonl");
        std::fs::hard_link(&target, &hardlink_path).unwrap();
        assert!(
            WalletAuditLog::open(hardlink_path)
                .err()
                .unwrap()
                .contains("hard link")
        );
    }

    #[test]
    fn wallet_audit_rejects_a_partial_existing_record() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("wallet-api-audit.jsonl");
        std::fs::write(&path, br#"{"version":1"#).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        }
        let error = WalletAuditLog::open(path).err().unwrap();
        assert!(error.contains("partial record"));
    }

    #[tokio::test]
    async fn wallet_utxo_pages_reject_unbounded_queries() {
        let directory = tempfile::tempdir().unwrap();
        let wallet = Arc::new(
            EmbeddedWallet::open_or_create(
                directory.path().join("wallet.sqlite"),
                RECEIVE_DESCRIPTOR,
                CHANGE_DESCRIPTOR,
                Network::Testnet,
            )
            .unwrap(),
        );
        let token = "a".repeat(MIN_WALLET_AUTH_TOKEN_LEN);
        let app = wallet_router(
            wallet,
            WalletAuthToken::new(&token).unwrap(),
            WalletAuditLog::open(directory.path().join("wallet-api-audit.jsonl")).unwrap(),
            None,
        );
        for route in ["utxos", "transactions"] {
            for query in ["limit=0", "limit=101", "offset=10001"] {
                let response = app
                    .clone()
                    .oneshot(
                        Request::get(format!("/api/v1/wallet/{route}?{query}"))
                            .header(header::AUTHORIZATION, format!("Bearer {token}"))
                            .body(Body::empty())
                            .unwrap(),
                    )
                    .await
                    .unwrap();
                assert_eq!(response.status(), StatusCode::BAD_REQUEST);
            }
        }
    }

    #[tokio::test]
    async fn wallet_history_and_status_are_authenticated_and_typed() {
        let directory = tempfile::tempdir().unwrap();
        let wallet = Arc::new(
            EmbeddedWallet::open_or_create(
                directory.path().join("wallet.sqlite"),
                RECEIVE_DESCRIPTOR,
                CHANGE_DESCRIPTOR,
                Network::Testnet,
            )
            .unwrap(),
        );
        let token = "a".repeat(MIN_WALLET_AUTH_TOKEN_LEN);
        let app = wallet_router(
            wallet,
            WalletAuthToken::new(&token).unwrap(),
            WalletAuditLog::open(directory.path().join("wallet-api-audit.jsonl")).unwrap(),
            None,
        );
        let request = |path: &'static str| {
            Request::get(path)
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap()
        };
        let status = app
            .clone()
            .oneshot(request("/api/v1/wallet/status"))
            .await
            .unwrap();
        assert_eq!(status.status(), StatusCode::OK);
        let body = axum::body::to_bytes(status.into_body(), 4096)
            .await
            .unwrap();
        let status: WalletStatus = serde_json::from_slice(&body).unwrap();
        assert_eq!(status.tip_height, 0);
        assert_eq!(status.issued_receive_addresses, 0);

        let descriptors = app
            .clone()
            .oneshot(request("/api/v1/wallet/descriptors"))
            .await
            .unwrap();
        assert_eq!(descriptors.status(), StatusCode::OK);
        assert_eq!(descriptors.headers()[header::CACHE_CONTROL], "no-store");
        let body = axum::body::to_bytes(descriptors.into_body(), 4096)
            .await
            .unwrap();
        let descriptors: WalletPublicDescriptors = serde_json::from_slice(&body).unwrap();
        let imported = crate::wallet::parse_wallet_descriptor_config(
            &serde_json::to_vec(&descriptors).unwrap(),
        )
        .unwrap();
        assert_eq!(imported.receive_descriptor, descriptors.receive_descriptor);
        assert_eq!(imported.change_descriptor, descriptors.change_descriptor);
        assert!(descriptors.receive_descriptor.contains("tpub"));

        let history = app
            .oneshot(request("/api/v1/wallet/transactions?limit=1"))
            .await
            .unwrap();
        assert_eq!(history.status(), StatusCode::OK);
        let body = axum::body::to_bytes(history.into_body(), 4096)
            .await
            .unwrap();
        let history: WalletTransactionPage = serde_json::from_slice(&body).unwrap();
        assert!(history.transactions.is_empty());
        assert!(!history.has_more);
    }

    #[test]
    fn wallet_auth_token_rejects_short_or_non_graphic_values() {
        assert!(WalletAuthToken::new("short").is_err());
        assert!(WalletAuthToken::new(format!("{} ", "a".repeat(31))).is_err());
        assert!(WalletAuthToken::new("a".repeat(256)).is_ok());
        assert!(WalletAuthToken::new("a".repeat(257)).is_err());
        let token = WalletAuthToken::new("a".repeat(32)).unwrap();
        let oversized = HeaderValue::from_str(&format!("Bearer {}", "a".repeat(257))).unwrap();
        assert!(!token.authorizes(oversized.as_bytes()));
        assert!(token.authorizes(format!("bEaReR {}", "a".repeat(32)).as_bytes()));
        assert!(!token.authorizes(format!("Bearer  {}", "a".repeat(32)).as_bytes()));
        assert!(!token.authorizes(b"Bearer \xff"));
    }

    #[test]
    fn wallet_auth_token_rotation_is_atomic_shared_and_fail_closed() {
        let old = "a".repeat(32);
        let new = "b".repeat(32);
        let token = WalletAuthToken::new(&old).unwrap();
        let router_state = token.clone();
        assert!(router_state.authorizes(format!("Bearer {old}").as_bytes()));

        token.rotate(&new).unwrap();
        assert!(!router_state.authorizes(format!("Bearer {old}").as_bytes()));
        assert!(router_state.authorizes(format!("Bearer {new}").as_bytes()));
        assert!(token.rotate("invalid").is_err());
        assert!(router_state.authorizes(format!("Bearer {new}").as_bytes()));

        token.invalidate();
        assert!(!router_state.authorizes(format!("Bearer {new}").as_bytes()));
        router_state.rotate(&old).unwrap();
        assert!(token.authorizes(format!("Bearer {old}").as_bytes()));
    }

    #[test]
    fn concurrent_wallet_auth_checks_survive_repeated_rotation() {
        let first = "a".repeat(32);
        let second = "b".repeat(32);
        let token = WalletAuthToken::new(&first).unwrap();
        let readers = (0..8)
            .map(|_| {
                let token = token.clone();
                let first = first.clone();
                let second = second.clone();
                std::thread::spawn(move || {
                    for _ in 0..1_000 {
                        let _ = token.authorizes(format!("Bearer {first}").as_bytes());
                        let _ = token.authorizes(format!("Bearer {second}").as_bytes());
                    }
                })
            })
            .collect::<Vec<_>>();
        for index in 0..1_000 {
            token
                .rotate(if index % 2 == 0 { &second } else { &first })
                .unwrap();
        }
        for reader in readers {
            reader.join().unwrap();
        }
        token.rotate(&second).unwrap();
        assert!(!token.authorizes(format!("Bearer {first}").as_bytes()));
        assert!(token.authorizes(format!("Bearer {second}").as_bytes()));
    }

    #[test]
    fn concurrent_wallet_audit_writers_preserve_complete_json_lines() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("wallet-api-audit.jsonl");
        let audit = WalletAuditLog::open(&path).unwrap();
        let writers = (0..8)
            .map(|_| {
                let audit = audit.clone();
                std::thread::spawn(move || {
                    for _ in 0..20 {
                        audit.record("GET", "/api/v1/wallet/status", true).unwrap();
                    }
                })
            })
            .collect::<Vec<_>>();
        for writer in writers {
            writer.join().unwrap();
        }
        let contents = std::fs::read_to_string(path).unwrap();
        assert_eq!(contents.lines().count(), 160);
        for line in contents.lines() {
            let record: serde_json::Value = serde_json::from_str(line).unwrap();
            assert_eq!(record["authorization"], "accepted");
            assert_eq!(record["path"], "/api/v1/wallet/status");
        }
    }

    #[test]
    fn wallet_address_rate_limit_refills_one_token_per_minute() {
        let mut limiter = AddressRateLimiter::new();
        let start = limiter.last_refill;
        for _ in 0..WALLET_ADDRESS_BURST {
            assert!(limiter.take(start));
        }
        assert!(!limiter.take(start));
        assert!(limiter.take(start + WALLET_ADDRESS_REFILL_INTERVAL));
        assert!(!limiter.take(start + WALLET_ADDRESS_REFILL_INTERVAL));

        let much_later = start + Duration::from_secs(1_000 * 60);
        for _ in 0..WALLET_ADDRESS_BURST {
            assert!(limiter.take(much_later));
        }
        assert!(!limiter.take(much_later));
    }

    #[test]
    fn memory_index_returns_cloned_projections() {
        let index = MemoryExplorerIndex::default();
        index.upsert_block(ExplorerBlock {
            height: 5,
            hash: "block".into(),
            time: 10,
            transaction_count: 2,
        });
        index.set_address_utxos(
            "bcrt1test",
            vec![ExplorerUtxo {
                txid: "tx".into(),
                vout: 0,
                value_sats: 1,
                height: 5,
            }],
        );
        assert_eq!(index.block(5).unwrap().unwrap().hash, "block");
        assert_eq!(index.address_utxos("bcrt1test", 0, 100).unwrap().len(), 1);
        assert!(index.transaction("missing").unwrap().is_none());
    }
}
