//! Bounded inbound Bitcoin P2P service.
//!
//! Framing and handshake rules live in [`crate::p2p`]. This module owns the
//! accepting-side resource policy and serves only data authenticated and
//! published by the node-provided [`InboundDataSource`].

use std::{
    collections::{HashMap, HashSet, VecDeque},
    net::{IpAddr, SocketAddr},
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use bitcoin::{
    Block, BlockHash, Transaction,
    bip152::{BlockTransactions, HeaderAndShortIds},
    bip158::{FilterHash, FilterHeader},
    block::Header,
    consensus::{deserialize, serialize},
    hashes::Hash,
    p2p::{
        Address, ServiceFlags,
        address::{AddrV2, AddrV2Message},
        message::NetworkMessage,
        message_blockdata::Inventory,
        message_compact_blocks::{BlockTxn, CmpctBlock, SendCmpct},
        message_filter::{CFCheckpt, CFHeaders, CFilter},
    },
};
use thiserror::Error;
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{OwnedSemaphorePermit, Semaphore, broadcast},
    task::JoinSet,
    time::{Instant as TokioInstant, sleep_until, timeout},
};

use crate::{
    p2p::{
        InboundPeerSession, MAX_COMPACT_BLOCK_TRANSACTIONS, MAX_HEADERS_PER_RESPONSE,
        MAX_INVENTORY_ENTRIES, P2pError, TransactionRelay, accept_inbound,
    },
    utxo::{OutPointKey, Utxo},
};

const MAX_GETBLOCKS_RESULTS: usize = 500;
const BASIC_FILTER_TYPE: u8 = 0;
const FILTER_HEADER_INTERVAL: u32 = 1_000;
const RECENT_BLOCK_UPLOAD_WINDOW: u32 = 288;
const SENDHEADERS_VERSION: u32 = 70_012;
const FEEFILTER_VERSION: u32 = 70_013;
const SENDCMPCT_VERSION: u32 = 70_014;
const MAX_MONEY_SATS: i64 = 21_000_000 * 100_000_000;
const MAX_RELAY_ANNOUNCEMENTS_PER_PEER: usize = 5_000;

/// Resource ceilings for one optional inbound listener.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InboundLimits {
    /// Maximum concurrently established or handshaking inbound sockets.
    pub max_connections: usize,
    /// Maximum concurrent sockets from one source IP.
    pub max_connections_per_ip: usize,
    /// Rolling 24-hour upload target. Recent blocks remain serviceable.
    pub max_upload_bytes_per_day: u64,
    /// Maximum non-keepalive requests accepted from one peer per minute.
    pub max_requests_per_minute: u32,
    /// Idle/read timeout for one accepted session.
    pub idle_timeout: Duration,
    /// Explicit publicly reachable address; never inferred from a bind socket.
    pub advertised_address: Option<SocketAddr>,
    /// Exact source IPs granted a protected inbound role.
    ///
    /// Preferred peers still consume the global hard connection ceiling and
    /// every ordinary request/upload bound, but may exceed the per-source
    /// ceiling and cannot be displaced by an untrusted connection.
    pub preferred_peer_ips: Vec<IpAddr>,
}

impl Default for InboundLimits {
    fn default() -> Self {
        Self {
            max_connections: 32,
            max_connections_per_ip: 4,
            max_upload_bytes_per_day: 1024 * 1024 * 1024,
            max_requests_per_minute: 1_200,
            idle_timeout: Duration::from_secs(20 * 60),
            advertised_address: None,
            preferred_peer_ips: Vec::new(),
        }
    }
}

/// One active-chain BIP157/158 basic-filter record exposed to peers.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InboundBasicFilter {
    /// Active block hash.
    pub block_hash: BlockHash,
    /// Golomb-coded set bytes.
    pub filter: Vec<u8>,
    /// Hash of the filter bytes.
    pub filter_hash: FilterHash,
    /// Header chaining this filter to its predecessor.
    pub filter_header: FilterHeader,
}

/// One currently admitted inbound peer and its bounded resource accounting.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize)]
pub struct InboundPeerStats {
    /// Remote socket address.
    pub address: SocketAddr,
    /// Whether this source has the explicitly configured protected role.
    pub preferred: bool,
    /// Seconds since the TCP socket was admitted.
    pub connected_seconds: u64,
    /// Whether the version/verack handshake completed.
    pub handshake_complete: bool,
    /// Negotiated peer protocol version, after the handshake.
    pub protocol_version: Option<u32>,
    /// Bounded peer user agent, after the handshake.
    pub user_agent: Option<String>,
    /// Counted non-keepalive requests received from this peer.
    pub requests: u64,
    /// Consensus payload bytes admitted to this peer's socket write path.
    pub uploaded_bytes: u64,
    /// Historical block payload bytes charged to the rolling upload target.
    pub historical_upload_bytes: u64,
}

/// Process-lifetime inbound service counters plus the live peer set.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize)]
pub struct InboundStatsSnapshot {
    /// Configured listener has accepted this many sockets.
    pub accepted_total: u64,
    /// Sessions which completed the bounded version/verack handshake.
    pub handshakes_total: u64,
    /// Sockets rejected because the process-wide connection ceiling was full.
    pub rejected_capacity_total: u64,
    /// Sockets rejected by the per-IP or per-network-group ceiling.
    pub rejected_source_total: u64,
    /// Sessions which completed or failed after admission.
    pub completed_total: u64,
    /// Low-value sessions replaced to improve source-network diversity.
    pub evicted_total: u64,
    /// Sessions closed because their request-work budget was exhausted.
    pub request_budget_disconnects: u64,
    /// Sessions closed because the historical upload target was exhausted.
    pub upload_target_disconnects: u64,
    /// Sessions closed by protocol/framing failures.
    pub protocol_disconnects: u64,
    /// Sessions closed by structurally bounded but excessive requests.
    pub request_bound_disconnects: u64,
    /// Sessions closed by socket I/O or idle timeout.
    pub io_disconnects: u64,
    /// Sessions closed by local authenticated-data failures.
    pub data_disconnects: u64,
    /// Currently admitted sessions, including handshakes.
    pub active: u64,
    /// Historical block bytes charged in the current rolling 24-hour window.
    pub historical_upload_bytes_24h: u64,
    /// Configured rolling historical upload target; zero means unlimited.
    pub historical_upload_target_bytes: u64,
    /// Live per-peer accounting sorted by remote address.
    pub peers: Vec<InboundPeerStats>,
}

#[derive(Debug)]
struct LiveInboundPeer {
    address: SocketAddr,
    group: NetworkGroup,
    preferred: bool,
    connected: Instant,
    handshake_complete: bool,
    protocol_version: Option<u32>,
    user_agent: Option<String>,
    requests: u64,
    uploaded_bytes: u64,
    historical_upload_bytes: u64,
}

#[derive(Debug)]
struct InboundStatsState {
    next_id: u64,
    accepted_total: u64,
    handshakes_total: u64,
    rejected_capacity_total: u64,
    rejected_source_total: u64,
    completed_total: u64,
    evicted_total: u64,
    request_budget_disconnects: u64,
    upload_target_disconnects: u64,
    protocol_disconnects: u64,
    request_bound_disconnects: u64,
    io_disconnects: u64,
    data_disconnects: u64,
    historical_window_started: Instant,
    historical_upload_bytes_24h: u64,
    peers: HashMap<u64, LiveInboundPeer>,
}

impl Default for InboundStatsState {
    fn default() -> Self {
        Self {
            next_id: 0,
            accepted_total: 0,
            handshakes_total: 0,
            rejected_capacity_total: 0,
            rejected_source_total: 0,
            completed_total: 0,
            evicted_total: 0,
            request_budget_disconnects: 0,
            upload_target_disconnects: 0,
            protocol_disconnects: 0,
            request_bound_disconnects: 0,
            io_disconnects: 0,
            data_disconnects: 0,
            historical_window_started: Instant::now(),
            historical_upload_bytes_24h: 0,
            peers: HashMap::new(),
        }
    }
}

/// Shared, read-only-observable accounting for one inbound listener.
#[derive(Debug)]
pub struct InboundStats {
    historical_upload_target_bytes: u64,
    state: Mutex<InboundStatsState>,
}

impl InboundStats {
    /// Creates empty listener accounting for one configured upload target.
    #[must_use]
    pub fn new(historical_upload_target_bytes: u64) -> Self {
        Self {
            historical_upload_target_bytes,
            state: Mutex::new(InboundStatsState::default()),
        }
    }

    /// Returns a consistent point-in-time snapshot.
    #[must_use]
    pub fn snapshot(&self) -> InboundStatsSnapshot {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.historical_window_started.elapsed() >= Duration::from_secs(24 * 60 * 60) {
            state.historical_window_started = Instant::now();
            state.historical_upload_bytes_24h = 0;
        }
        let mut peers = state
            .peers
            .values()
            .map(|peer| InboundPeerStats {
                address: peer.address,
                preferred: peer.preferred,
                connected_seconds: peer.connected.elapsed().as_secs(),
                handshake_complete: peer.handshake_complete,
                protocol_version: peer.protocol_version,
                user_agent: peer.user_agent.clone(),
                requests: peer.requests,
                uploaded_bytes: peer.uploaded_bytes,
                historical_upload_bytes: peer.historical_upload_bytes,
            })
            .collect::<Vec<_>>();
        peers.sort_unstable_by_key(|peer| peer.address);
        InboundStatsSnapshot {
            accepted_total: state.accepted_total,
            handshakes_total: state.handshakes_total,
            rejected_capacity_total: state.rejected_capacity_total,
            rejected_source_total: state.rejected_source_total,
            completed_total: state.completed_total,
            evicted_total: state.evicted_total,
            request_budget_disconnects: state.request_budget_disconnects,
            upload_target_disconnects: state.upload_target_disconnects,
            protocol_disconnects: state.protocol_disconnects,
            request_bound_disconnects: state.request_bound_disconnects,
            io_disconnects: state.io_disconnects,
            data_disconnects: state.data_disconnects,
            active: u64::try_from(peers.len()).unwrap_or(u64::MAX),
            historical_upload_bytes_24h: state.historical_upload_bytes_24h,
            historical_upload_target_bytes: self.historical_upload_target_bytes,
            peers,
        }
    }

    fn reject_capacity(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.rejected_capacity_total = state.rejected_capacity_total.saturating_add(1);
    }

    fn reject_source(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.rejected_source_total = state.rejected_source_total.saturating_add(1);
    }

    fn accept(self: &Arc<Self>, address: SocketAddr, preferred: bool) -> InboundPeerAccount {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let id = state.next_id;
        state.next_id = state.next_id.wrapping_add(1);
        state.accepted_total = state.accepted_total.saturating_add(1);
        state.peers.insert(
            id,
            LiveInboundPeer {
                address,
                group: network_group(address.ip()),
                preferred,
                connected: Instant::now(),
                handshake_complete: false,
                protocol_version: None,
                user_agent: None,
                requests: 0,
                uploaded_bytes: 0,
                historical_upload_bytes: 0,
            },
        );
        InboundPeerAccount {
            id,
            stats: Arc::clone(self),
        }
    }

    fn eviction_candidate(&self, incoming: SocketAddr, incoming_preferred: bool) -> Option<u64> {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let incoming_group = network_group(incoming.ip());
        if !incoming_preferred
            && state
                .peers
                .values()
                .any(|peer| peer.group == incoming_group)
        {
            return None;
        }
        let mut group_counts = HashMap::new();
        for peer in state.peers.values() {
            *group_counts.entry(peer.group).or_insert(0_usize) += 1;
        }
        state
            .peers
            .iter()
            .filter(|(_, peer)| {
                !peer.preferred
                    && (incoming_preferred
                        || !peer.handshake_complete
                        || group_counts.get(&peer.group).copied().unwrap_or_default() > 1)
            })
            .min_by_key(|(_, peer)| {
                (
                    peer.handshake_complete,
                    peer.requests,
                    peer.uploaded_bytes,
                    peer.connected.elapsed(),
                )
            })
            .map(|(id, _)| *id)
    }
}

struct InboundPeerAccount {
    id: u64,
    stats: Arc<InboundStats>,
}

impl InboundPeerAccount {
    fn handshake(&self, version: u32, user_agent: String) {
        let mut state = self
            .stats
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state
            .peers
            .get(&self.id)
            .is_some_and(|peer| !peer.handshake_complete)
        {
            state.handshakes_total = state.handshakes_total.saturating_add(1);
        }
        if let Some(peer) = state.peers.get_mut(&self.id) {
            peer.handshake_complete = true;
            peer.protocol_version = Some(version);
            peer.user_agent = Some(user_agent);
        }
    }

    fn request(&self) {
        let mut state = self
            .stats
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(peer) = state.peers.get_mut(&self.id) {
            peer.requests = peer.requests.saturating_add(1);
        }
    }

    fn upload(&self, bytes: usize, historical_block: bool) {
        let bytes = u64::try_from(bytes).unwrap_or(u64::MAX);
        let mut state = self
            .stats
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(peer) = state.peers.get_mut(&self.id) {
            peer.uploaded_bytes = peer.uploaded_bytes.saturating_add(bytes);
            if historical_block {
                peer.historical_upload_bytes = peer.historical_upload_bytes.saturating_add(bytes);
            }
        }
        if historical_block {
            if state.historical_window_started.elapsed() >= Duration::from_secs(24 * 60 * 60) {
                state.historical_window_started = Instant::now();
                state.historical_upload_bytes_24h = 0;
            }
            state.historical_upload_bytes_24h =
                state.historical_upload_bytes_24h.saturating_add(bytes);
        }
    }

    fn finish(&self, error: Option<&InboundError>) {
        let mut state = self
            .stats
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.peers.remove(&self.id).is_none() {
            return;
        }
        state.completed_total = state.completed_total.saturating_add(1);
        match error {
            Some(InboundError::RequestBudget) => {
                state.request_budget_disconnects =
                    state.request_budget_disconnects.saturating_add(1);
            }
            Some(InboundError::UploadTarget) => {
                state.upload_target_disconnects = state.upload_target_disconnects.saturating_add(1);
            }
            Some(InboundError::Protocol(_)) => {
                state.protocol_disconnects = state.protocol_disconnects.saturating_add(1);
            }
            Some(InboundError::RequestBound(_)) => {
                state.request_bound_disconnects = state.request_bound_disconnects.saturating_add(1);
            }
            Some(InboundError::Io(_)) => {
                state.io_disconnects = state.io_disconnects.saturating_add(1);
            }
            Some(InboundError::Data(_)) => {
                state.data_disconnects = state.data_disconnects.saturating_add(1);
            }
            Some(InboundError::Evicted) => {
                state.evicted_total = state.evicted_total.saturating_add(1);
            }
            _ => {}
        }
    }
}

impl Drop for InboundPeerAccount {
    fn drop(&mut self) {
        self.finish(None);
    }
}

/// Read-only active-chain data exposed to inbound peers.
///
/// Implementations must return only maximum-work active-chain data already
/// committed by consensus execution. All returned collections are additionally
/// bounded by the service before they reach the wire.
pub trait InboundDataSource: Send + Sync + 'static {
    /// Current active executed height advertised in `version`.
    fn start_height(&self) -> Result<u32, String>;
    /// Active-chain header at one height.
    fn active_header(&self, height: u32) -> Result<Option<Header>, String>;
    /// Active-chain height of one block hash.
    fn active_height(&self, hash: BlockHash) -> Result<Option<u32>, String>;
    /// Consensus-serialized retained active block by hash.
    fn block(&self, hash: BlockHash) -> Result<Option<Vec<u8>>, String>;
    /// Bounded current mempool snapshot.
    fn mempool(&self) -> Result<Vec<Transaction>, String>;
    /// Current mempool transaction matching one supported inventory key.
    fn transaction(&self, inventory: Inventory) -> Result<Option<Transaction>, String>;
    /// Queues one untrusted peer transaction for the node's ordinary admission path.
    fn submit_transaction(&self, transaction: Transaction) -> Result<bool, String>;
    /// BIP158 basic filter data at one active height, when indexed.
    fn basic_filter(&self, height: u32) -> Result<Option<InboundBasicFilter>, String>;
    /// Diverse, already-vetted IPv4/IPv6 peers suitable for bounded address relay.
    fn addresses(&self) -> Result<Vec<SocketAddr>, String> {
        Ok(Vec::new())
    }
    /// Explicit local address and exact service bits suitable for relay.
    fn advertised_address(&self) -> Option<(SocketAddr, ServiceFlags)> {
        None
    }
    /// Current local minimum mempool fee in satoshis per 1,000 virtual bytes.
    fn fee_filter_sat_kvb(&self) -> Result<u64, String> {
        Ok(0)
    }
    /// Active-chain UTXO lookup for authenticated local control surfaces.
    fn utxo(&self, _outpoint: OutPointKey) -> Result<Option<Utxo>, String> {
        Ok(None)
    }
}

/// Inbound listener or peer-service failure.
#[derive(Debug, Error)]
pub enum InboundError {
    /// Listener or peer socket I/O failed.
    #[error("inbound io: {0}")]
    Io(#[from] std::io::Error),
    /// Shared v1 framing or handshake rejected the peer.
    #[error("inbound protocol: {0}")]
    Protocol(#[from] P2pError),
    /// The authenticated local data source failed closed.
    #[error("inbound data: {0}")]
    Data(String),
    /// A peer exceeded its bounded request-work rate.
    #[error("inbound peer request budget exhausted")]
    RequestBudget,
    /// The rolling historical upload target was exhausted.
    #[error("inbound historical upload target exhausted")]
    UploadTarget,
    /// A request was structurally valid but exceeded the service work bound.
    #[error("inbound request exceeds service bound: {0}")]
    RequestBound(&'static str),
    /// A new source network replaced this low-value session at hard capacity.
    #[error("inbound peer evicted for network-group diversity")]
    Evicted,
}

#[derive(Debug)]
struct UploadWindow {
    started: Instant,
    bytes: u64,
}

#[derive(Debug)]
struct UploadBudget {
    target: u64,
    window: Mutex<UploadWindow>,
}

impl UploadBudget {
    fn new(target: u64) -> Self {
        Self {
            target,
            window: Mutex::new(UploadWindow {
                started: Instant::now(),
                bytes: 0,
            }),
        }
    }

    fn charge(&self, bytes: usize, historical_block: bool) -> Result<(), InboundError> {
        if !historical_block || self.target == 0 {
            return Ok(());
        }
        let bytes = u64::try_from(bytes).unwrap_or(u64::MAX);
        let mut window = self
            .window
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if window.started.elapsed() >= Duration::from_secs(24 * 60 * 60) {
            window.started = Instant::now();
            window.bytes = 0;
        }
        let next = window
            .bytes
            .checked_add(bytes)
            .ok_or(InboundError::UploadTarget)?;
        if next > self.target {
            return Err(InboundError::UploadTarget);
        }
        window.bytes = next;
        Ok(())
    }
}

struct RequestBudget {
    limit: u32,
    started: Instant,
    requests: u32,
}

impl RequestBudget {
    fn new(limit: u32) -> Self {
        Self {
            limit,
            started: Instant::now(),
            requests: 0,
        }
    }

    fn charge(&mut self) -> Result<(), InboundError> {
        if self.started.elapsed() >= Duration::from_secs(60) {
            self.started = Instant::now();
            self.requests = 0;
        }
        self.requests = self.requests.saturating_add(1);
        if self.requests > self.limit {
            return Err(InboundError::RequestBudget);
        }
        Ok(())
    }
}

struct IpAdmission {
    ip: IpAddr,
    group: NetworkGroup,
    ip_counts: Arc<Mutex<HashMap<IpAddr, usize>>>,
    group_counts: Arc<Mutex<HashMap<NetworkGroup, usize>>>,
    _global: OwnedSemaphorePermit,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum NetworkGroup {
    Ipv4([u8; 2]),
    Ipv6([u8; 4]),
}

fn network_group(ip: IpAddr) -> NetworkGroup {
    match ip {
        IpAddr::V4(ip) => NetworkGroup::Ipv4([ip.octets()[0], ip.octets()[1]]),
        IpAddr::V6(ip) => {
            if let Some(ip) = ip.to_ipv4_mapped() {
                return NetworkGroup::Ipv4([ip.octets()[0], ip.octets()[1]]);
            }
            let octets = ip.octets();
            NetworkGroup::Ipv6([octets[0], octets[1], octets[2], octets[3]])
        }
    }
}

impl Drop for IpAdmission {
    fn drop(&mut self) {
        let mut ip_counts = self
            .ip_counts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(count) = ip_counts.get_mut(&self.ip) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                ip_counts.remove(&self.ip);
            }
        }
        drop(ip_counts);
        let mut group_counts = self
            .group_counts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(count) = group_counts.get_mut(&self.group) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                group_counts.remove(&self.group);
            }
        }
    }
}

fn admit_ip(
    remote: SocketAddr,
    limit: usize,
    preferred: bool,
    ip_counts: &Arc<Mutex<HashMap<IpAddr, usize>>>,
    group_counts: &Arc<Mutex<HashMap<NetworkGroup, usize>>>,
    global: OwnedSemaphorePermit,
) -> Option<IpAdmission> {
    let ip = remote.ip();
    let group = network_group(ip);
    let mut ips = ip_counts
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let mut groups = group_counts
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if !preferred
        && (ips.get(&ip).copied().unwrap_or_default() >= limit
            || groups.get(&group).copied().unwrap_or_default() >= limit)
    {
        return None;
    }
    *ips.entry(ip).or_default() += 1;
    *groups.entry(group).or_default() += 1;
    drop(groups);
    drop(ips);
    Some(IpAdmission {
        ip,
        group,
        ip_counts: Arc::clone(ip_counts),
        group_counts: Arc::clone(group_counts),
        _global: global,
    })
}

fn source_at_limit(
    remote: SocketAddr,
    limit: usize,
    ip_counts: &Arc<Mutex<HashMap<IpAddr, usize>>>,
    group_counts: &Arc<Mutex<HashMap<NetworkGroup, usize>>>,
) -> bool {
    let ips = ip_counts
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let groups = group_counts
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    ips.get(&remote.ip()).copied().unwrap_or_default() >= limit
        || groups
            .get(&network_group(remote.ip()))
            .copied()
            .unwrap_or_default()
            >= limit
}

async fn acquire_global_slot(
    global: &Arc<Semaphore>,
    peers: &mut JoinSet<u64>,
    cancellations: &mut HashMap<u64, tokio::sync::oneshot::Sender<()>>,
    stats: &InboundStats,
    remote: SocketAddr,
    preferred: bool,
) -> Option<OwnedSemaphorePermit> {
    if let Ok(permit) = Arc::clone(global).try_acquire_owned() {
        return Some(permit);
    }
    let candidate = stats.eviction_candidate(remote, preferred)?;
    let cancel = cancellations.remove(&candidate)?;
    let _ = cancel.send(());
    loop {
        if let Ok(permit) = Arc::clone(global).try_acquire_owned() {
            return Some(permit);
        }
        match peers.join_next().await {
            Some(Ok(id)) => {
                cancellations.remove(&id);
            }
            Some(Err(_)) => {}
            None => return None,
        }
    }
}

/// Runs one bounded listener until the task is cancelled or the listener fails.
///
/// Individual malformed, idle, over-budget, or disconnected peers are isolated
/// to their own tasks and do not terminate the listener.
pub async fn run_listener(
    listener: TcpListener,
    magic: bitcoin::p2p::Magic,
    local_nonce: u64,
    user_agent: String,
    services: ServiceFlags,
    limits: InboundLimits,
    source: Arc<dyn InboundDataSource>,
) -> Result<(), InboundError> {
    let upload_target = limits.max_upload_bytes_per_day;
    run_listener_with_stats(
        listener,
        magic,
        local_nonce,
        user_agent,
        services,
        limits,
        source,
        Arc::new(InboundStats::new(upload_target)),
    )
    .await
}

/// Runs one bounded listener while publishing live, process-lifetime accounting.
#[allow(clippy::too_many_arguments)]
pub async fn run_listener_with_stats(
    listener: TcpListener,
    magic: bitcoin::p2p::Magic,
    local_nonce: u64,
    user_agent: String,
    services: ServiceFlags,
    limits: InboundLimits,
    source: Arc<dyn InboundDataSource>,
    stats: Arc<InboundStats>,
) -> Result<(), InboundError> {
    run_listener_with_stats_and_relay(
        listener,
        magic,
        local_nonce,
        user_agent,
        services,
        limits,
        source,
        stats,
        None,
    )
    .await
}

/// Runs one bounded listener with live accounting and validated transaction
/// announcements shared across accepted peers.
#[allow(clippy::too_many_arguments)]
pub async fn run_listener_with_stats_and_relay(
    listener: TcpListener,
    magic: bitcoin::p2p::Magic,
    local_nonce: u64,
    user_agent: String,
    services: ServiceFlags,
    limits: InboundLimits,
    source: Arc<dyn InboundDataSource>,
    stats: Arc<InboundStats>,
    transaction_relay: Option<broadcast::Sender<TransactionRelay>>,
) -> Result<(), InboundError> {
    let global = Arc::new(Semaphore::new(limits.max_connections));
    let per_ip = Arc::new(Mutex::new(HashMap::new()));
    let per_group = Arc::new(Mutex::new(HashMap::new()));
    let upload = Arc::new(UploadBudget::new(limits.max_upload_bytes_per_day));
    let mut peers = JoinSet::new();
    let mut cancellations: HashMap<u64, tokio::sync::oneshot::Sender<()>> = HashMap::new();
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let (stream, remote) = accepted?;
                let preferred = limits.preferred_peer_ips.contains(&remote.ip());
                if !preferred && source_at_limit(
                    remote,
                    limits.max_connections_per_ip,
                    &per_ip,
                    &per_group,
                ) {
                    stats.reject_source();
                    drop(stream);
                    continue;
                }
                let Some(global_permit) = acquire_global_slot(
                    &global,
                    &mut peers,
                    &mut cancellations,
                    stats.as_ref(),
                    remote,
                    preferred,
                ).await else {
                    stats.reject_capacity();
                    drop(stream);
                    continue;
                };
                let Some(admission) =
                    admit_ip(
                        remote,
                        limits.max_connections_per_ip,
                        preferred,
                        &per_ip,
                        &per_group,
                        global_permit,
                    )
                else {
                    stats.reject_source();
                    drop(stream);
                    continue;
                };
                let account = stats.accept(remote, preferred);
                let source = Arc::clone(&source);
                let upload = Arc::clone(&upload);
                let user_agent = user_agent.clone();
                let relay = transaction_relay.as_ref().map(broadcast::Sender::subscribe);
                let peer_limits = limits.clone();
                let id = account.id;
                let (cancel, cancelled) = tokio::sync::oneshot::channel();
                peers.spawn(async move {
                    let _admission = admission;
                    tokio::select! {
                        result = serve_peer(
                            stream,
                            magic,
                            local_nonce,
                            user_agent,
                            services,
                            peer_limits,
                            source,
                            upload,
                            relay,
                            &account,
                        ) => account.finish(result.as_ref().err()),
                        _ = cancelled => account.finish(Some(&InboundError::Evicted)),
                    }
                    id
                });
                cancellations.insert(id, cancel);
            }
            completed = peers.join_next(), if !peers.is_empty() => {
                if let Some(Ok(id)) = completed {
                    cancellations.remove(&id);
                }
            }
        }
    }
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
async fn serve_peer(
    stream: TcpStream,
    magic: bitcoin::p2p::Magic,
    local_nonce: u64,
    user_agent: String,
    services: ServiceFlags,
    limits: InboundLimits,
    source: Arc<dyn InboundDataSource>,
    upload: Arc<UploadBudget>,
    mut transaction_relay: Option<broadcast::Receiver<TransactionRelay>>,
    account: &InboundPeerAccount,
) -> Result<(), InboundError> {
    let start_height = source.start_height().map_err(InboundError::Data)?;
    let start_height = i32::try_from(start_height).unwrap_or(i32::MAX);
    let mut peer = timeout(
        limits.idle_timeout,
        accept_inbound(
            stream,
            magic,
            local_nonce,
            user_agent,
            start_height,
            services,
        ),
    )
    .await
    .map_err(|_| {
        InboundError::Io(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "inbound handshake timed out",
        ))
    })??;
    account.handshake(
        peer.remote_version().version,
        peer.remote_version().user_agent.clone(),
    );
    if peer.remote_version().version >= SENDHEADERS_VERSION {
        send_accounted(
            &mut peer,
            NetworkMessage::SendHeaders,
            &upload,
            account,
            false,
        )
        .await?;
    }
    if peer.remote_version().version >= SENDCMPCT_VERSION {
        send_accounted(
            &mut peer,
            NetworkMessage::SendCmpct(SendCmpct {
                send_compact: false,
                version: 2,
            }),
            &upload,
            account,
            false,
        )
        .await?;
    }
    if peer.remote_version().version >= FEEFILTER_VERSION {
        let fee_filter = source.fee_filter_sat_kvb().map_err(InboundError::Data)?;
        let fee_filter = i64::try_from(fee_filter).unwrap_or(i64::MAX);
        send_accounted(
            &mut peer,
            NetworkMessage::FeeFilter(fee_filter),
            &upload,
            account,
            false,
        )
        .await?;
    }
    let mut request_budget = RequestBudget::new(limits.max_requests_per_minute);
    let mut requested_transactions = HashSet::new();
    let mut remote_fee_filter_sat_kvb = 0;
    let mut announced_transactions = HashSet::new();
    let mut announcement_order = VecDeque::new();
    let mut idle_deadline = TokioInstant::now() + limits.idle_timeout;
    loop {
        enum PeerWork {
            Message(Result<NetworkMessage, P2pError>),
            Relay(Result<TransactionRelay, broadcast::error::RecvError>),
            Idle,
        }
        let work = tokio::select! {
            message = peer.receive_message() => PeerWork::Message(message),
            relay = async {
                transaction_relay
                    .as_mut()
                    .expect("relay branch is guarded")
                    .recv()
                    .await
            }, if transaction_relay.is_some() => PeerWork::Relay(relay),
            () = sleep_until(idle_deadline) => PeerWork::Idle,
        };
        let message = match work {
            PeerWork::Message(message) => {
                idle_deadline = TokioInstant::now() + limits.idle_timeout;
                message?
            }
            PeerWork::Relay(Ok(relay)) => {
                if relay_meets_fee_filter(&relay, remote_fee_filter_sat_kvb) {
                    let inventory = if peer.wtxid_relay() {
                        Inventory::WTx(relay.transaction.compute_wtxid())
                    } else {
                        Inventory::Transaction(relay.transaction.compute_txid())
                    };
                    if remember_announcement(
                        inventory,
                        &mut announced_transactions,
                        &mut announcement_order,
                    ) {
                        send_accounted(
                            &mut peer,
                            NetworkMessage::Inv(vec![inventory]),
                            &upload,
                            account,
                            false,
                        )
                        .await?;
                    }
                }
                continue;
            }
            PeerWork::Relay(Err(broadcast::error::RecvError::Lagged(_))) => continue,
            PeerWork::Relay(Err(broadcast::error::RecvError::Closed)) => {
                transaction_relay = None;
                continue;
            }
            PeerWork::Idle => {
                return Err(InboundError::Io(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "inbound peer idle timeout",
                )));
            }
        };
        if let NetworkMessage::FeeFilter(rate) = &message {
            if peer.remote_version().version >= FEEFILTER_VERSION
                && (0..=MAX_MONEY_SATS).contains(rate)
            {
                remote_fee_filter_sat_kvb =
                    u64::try_from(*rate).expect("valid monetary fee filter fits u64");
            }
        }
        if !matches!(
            message,
            NetworkMessage::Ping(_)
                | NetworkMessage::Pong(_)
                | NetworkMessage::SendHeaders
                | NetworkMessage::SendCmpct(_)
                | NetworkMessage::WtxidRelay
                | NetworkMessage::SendAddrV2
                | NetworkMessage::FeeFilter(_)
        ) {
            request_budget.charge()?;
            account.request();
        }
        route_message(
            &mut peer,
            message,
            source.as_ref(),
            &upload,
            account,
            &mut requested_transactions,
        )
        .await?;
    }
}

fn relay_meets_fee_filter(relay: &TransactionRelay, fee_filter_sat_kvb: u64) -> bool {
    relay.fee_sats.is_none_or(|fee_sats| {
        let vbytes = relay.policy_vsize.max(relay.transaction.vsize());
        u128::from(fee_sats) * 1_000 >= u128::from(fee_filter_sat_kvb) * vbytes as u128
    })
}

fn remember_announcement(
    inventory: Inventory,
    announced: &mut HashSet<Inventory>,
    order: &mut VecDeque<Inventory>,
) -> bool {
    if !announced.insert(inventory) {
        return false;
    }
    order.push_back(inventory);
    while order.len() > MAX_RELAY_ANNOUNCEMENTS_PER_PEER {
        if let Some(expired) = order.pop_front() {
            announced.remove(&expired);
        }
    }
    true
}

async fn send_accounted(
    peer: &mut InboundPeerSession<TcpStream>,
    message: NetworkMessage,
    upload: &UploadBudget,
    account: &InboundPeerAccount,
    historical_block: bool,
) -> Result<(), InboundError> {
    let bytes = serialize(&message).len();
    upload.charge(bytes, historical_block)?;
    account.upload(bytes, historical_block);
    peer.send_message(message).await?;
    Ok(())
}

#[allow(clippy::too_many_lines)]
async fn route_message(
    peer: &mut InboundPeerSession<TcpStream>,
    message: NetworkMessage,
    source: &dyn InboundDataSource,
    upload: &UploadBudget,
    account: &InboundPeerAccount,
    requested_transactions: &mut HashSet<Inventory>,
) -> Result<(), InboundError> {
    match message {
        NetworkMessage::Ping(nonce) => {
            send_accounted(peer, NetworkMessage::Pong(nonce), upload, account, false).await
        }
        NetworkMessage::GetHeaders(request) => {
            let headers = headers_after_locator(
                source,
                &request.locator_hashes,
                request.stop_hash,
                MAX_HEADERS_PER_RESPONSE,
            )?;
            send_accounted(
                peer,
                NetworkMessage::Headers(headers),
                upload,
                account,
                false,
            )
            .await
        }
        NetworkMessage::GetBlocks(request) => {
            let headers = headers_after_locator(
                source,
                &request.locator_hashes,
                request.stop_hash,
                MAX_GETBLOCKS_RESULTS,
            )?;
            let inventory = headers
                .into_iter()
                .map(|header| Inventory::Block(header.block_hash()))
                .collect();
            send_accounted(peer, NetworkMessage::Inv(inventory), upload, account, false).await
        }
        NetworkMessage::GetAddr => serve_addresses(peer, source, upload, account).await,
        NetworkMessage::GetData(requests) => {
            serve_getdata(peer, requests, source, upload, account).await
        }
        NetworkMessage::Inv(inventory) => {
            let mut requests = Vec::new();
            for entry in inventory {
                let supported = matches!(entry, Inventory::Transaction(_))
                    || peer.wtxid_relay() && matches!(entry, Inventory::WTx(_));
                if !supported
                    || requested_transactions.contains(&entry)
                    || source
                        .transaction(entry)
                        .map_err(InboundError::Data)?
                        .is_some()
                {
                    continue;
                }
                if requested_transactions.len() == 64 {
                    break;
                }
                requested_transactions.insert(entry);
                requests.push(entry);
            }
            if requests.is_empty() {
                Ok(())
            } else {
                send_accounted(
                    peer,
                    NetworkMessage::GetData(requests),
                    upload,
                    account,
                    false,
                )
                .await
            }
        }
        NetworkMessage::Tx(transaction) => {
            let txid = transaction.compute_txid();
            let wtxid = transaction.compute_wtxid();
            let requested = requested_transactions.iter().copied().find(|entry| {
                matches!(entry, Inventory::Transaction(expected) if *expected == txid)
                    || matches!(entry, Inventory::WTx(expected) if *expected == wtxid)
            });
            let Some(requested) = requested else {
                return Err(InboundError::RequestBound("unsolicited transaction"));
            };
            requested_transactions.remove(&requested);
            let _ = source
                .submit_transaction(transaction)
                .map_err(InboundError::Data)?;
            Ok(())
        }
        NetworkMessage::NotFound(inventory) => {
            for entry in inventory {
                requested_transactions.remove(&entry);
            }
            Ok(())
        }
        NetworkMessage::MemPool => serve_mempool(peer, source, upload, account).await,
        NetworkMessage::GetBlockTxn(request) => {
            let hash = request.txs_request.block_hash;
            let Some(raw) = source.block(hash).map_err(InboundError::Data)? else {
                return send_accounted(
                    peer,
                    NetworkMessage::NotFound(vec![Inventory::CompactBlock(hash)]),
                    upload,
                    account,
                    false,
                )
                .await;
            };
            let block: Block = deserialize(&raw).map_err(|error| {
                InboundError::Data(format!("decode retained block {hash}: {error}"))
            })?;
            if request.txs_request.indexes.len() > MAX_COMPACT_BLOCK_TRANSACTIONS {
                return Err(InboundError::RequestBound("getblocktxn transaction count"));
            }
            let mut transactions = Vec::with_capacity(request.txs_request.indexes.len());
            for index in request.txs_request.indexes {
                let index =
                    usize::try_from(index).map_err(|_| InboundError::RequestBound("index"))?;
                let transaction = block
                    .txdata
                    .get(index)
                    .ok_or(InboundError::RequestBound("getblocktxn index"))?;
                transactions.push(transaction.clone());
            }
            send_accounted(
                peer,
                NetworkMessage::BlockTxn(BlockTxn {
                    transactions: BlockTransactions {
                        block_hash: hash,
                        transactions,
                    },
                }),
                upload,
                account,
                !is_recent_block(source, hash)?,
            )
            .await
        }
        NetworkMessage::GetCFilters(request) => {
            serve_filters(peer, request, source, upload, account).await
        }
        NetworkMessage::GetCFHeaders(request) => {
            serve_filter_headers(peer, request, source, upload, account).await
        }
        NetworkMessage::GetCFCheckpt(request) => {
            serve_filter_checkpoints(peer, request, source, upload, account).await
        }
        NetworkMessage::Version(_) => Err(InboundError::Protocol(P2pError::PostHandshakeVersion)),
        _ => Ok(()),
    }
}

async fn serve_addresses(
    peer: &mut InboundPeerSession<TcpStream>,
    source: &dyn InboundDataSource,
    upload: &UploadBudget,
    account: &InboundPeerAccount,
) -> Result<(), InboundError> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| InboundError::Data(format!("system clock: {error}")))?
        .as_secs();
    let now = u32::try_from(now).unwrap_or(u32::MAX);
    let services = ServiceFlags::NETWORK | ServiceFlags::WITNESS;
    let mut seen = HashSet::new();
    let mut addresses = Vec::new();
    if let Some((address, local_services)) = source.advertised_address() {
        seen.insert(address);
        addresses.push((now, Address::new(&address, local_services)));
    }
    addresses.extend(
        source
            .addresses()
            .map_err(InboundError::Data)?
            .into_iter()
            .filter(|address| address.port() != 0 && seen.insert(*address))
            .take(crate::p2p::MAX_ADDRESSES_PER_MESSAGE.saturating_sub(addresses.len()))
            .map(|address| (now, Address::new(&address, services))),
    );
    let message = if peer.addrv2_relay() {
        NetworkMessage::AddrV2(
            addresses
                .into_iter()
                .map(|(time, address)| {
                    let socket = address
                        .socket_addr()
                        .expect("locally constructed IP address is representable");
                    AddrV2Message {
                        time,
                        services: address.services,
                        addr: match socket.ip() {
                            IpAddr::V4(address) => AddrV2::Ipv4(address),
                            IpAddr::V6(address) => AddrV2::Ipv6(address),
                        },
                        port: socket.port(),
                    }
                })
                .collect(),
        )
    } else {
        NetworkMessage::Addr(addresses)
    };
    send_accounted(peer, message, upload, account, false).await
}

fn headers_after_locator(
    source: &dyn InboundDataSource,
    locator: &[BlockHash],
    stop_hash: BlockHash,
    limit: usize,
) -> Result<Vec<Header>, InboundError> {
    let start = locator
        .iter()
        .find_map(|hash| source.active_height(*hash).transpose())
        .transpose()
        .map_err(InboundError::Data)?
        .unwrap_or(0)
        .saturating_add(1);
    let tip = source.start_height().map_err(InboundError::Data)?;
    let mut headers = Vec::with_capacity(limit.min(64));
    for height in start..=tip {
        if headers.len() == limit {
            break;
        }
        let Some(header) = source.active_header(height).map_err(InboundError::Data)? else {
            break;
        };
        let hash = header.block_hash();
        headers.push(header);
        if stop_hash != BlockHash::all_zeros() && hash == stop_hash {
            break;
        }
    }
    Ok(headers)
}

async fn serve_getdata(
    peer: &mut InboundPeerSession<TcpStream>,
    requests: Vec<Inventory>,
    source: &dyn InboundDataSource,
    upload: &UploadBudget,
    account: &InboundPeerAccount,
) -> Result<(), InboundError> {
    let mut block_requests = 0usize;
    let mut transaction_requests = 0usize;
    let mut not_found = Vec::new();
    for request in requests {
        match request {
            Inventory::Block(hash) | Inventory::WitnessBlock(hash) => {
                block_requests += 1;
                if block_requests > 16 {
                    return Err(InboundError::RequestBound("block getdata count"));
                }
                if let Some(raw) = source.block(hash).map_err(InboundError::Data)? {
                    let block: Block = deserialize(&raw).map_err(|error| {
                        InboundError::Data(format!("decode retained block {hash}: {error}"))
                    })?;
                    send_accounted(
                        peer,
                        NetworkMessage::Block(block),
                        upload,
                        account,
                        !is_recent_block(source, hash)?,
                    )
                    .await?;
                } else {
                    not_found.push(request);
                }
            }
            Inventory::CompactBlock(hash) => {
                block_requests += 1;
                if block_requests > 16 {
                    return Err(InboundError::RequestBound("compact block getdata count"));
                }
                if let Some(raw) = source.block(hash).map_err(InboundError::Data)? {
                    let block: Block = deserialize(&raw).map_err(|error| {
                        InboundError::Data(format!("decode retained block {hash}: {error}"))
                    })?;
                    let compact = HeaderAndShortIds::from_block(&block, rand::random(), 2, &[])
                        .map_err(|error| {
                            InboundError::Data(format!("encode compact block {hash}: {error}"))
                        })?;
                    send_accounted(
                        peer,
                        NetworkMessage::CmpctBlock(CmpctBlock {
                            compact_block: compact,
                        }),
                        upload,
                        account,
                        !is_recent_block(source, hash)?,
                    )
                    .await?;
                } else {
                    not_found.push(request);
                }
            }
            Inventory::Transaction(_) | Inventory::WitnessTransaction(_) | Inventory::WTx(_) => {
                transaction_requests += 1;
                if transaction_requests > 64 {
                    return Err(InboundError::RequestBound("transaction getdata count"));
                }
                if let Some(transaction) =
                    source.transaction(request).map_err(InboundError::Data)?
                {
                    send_accounted(
                        peer,
                        NetworkMessage::Tx(transaction),
                        upload,
                        account,
                        false,
                    )
                    .await?;
                } else {
                    not_found.push(request);
                }
            }
            Inventory::Error | Inventory::Unknown { .. } => not_found.push(request),
        }
    }
    if !not_found.is_empty() {
        send_accounted(
            peer,
            NetworkMessage::NotFound(not_found),
            upload,
            account,
            false,
        )
        .await?;
    }
    Ok(())
}

async fn serve_mempool(
    peer: &mut InboundPeerSession<TcpStream>,
    source: &dyn InboundDataSource,
    upload: &UploadBudget,
    account: &InboundPeerAccount,
) -> Result<(), InboundError> {
    let mut transactions = source.mempool().map_err(InboundError::Data)?;
    transactions.truncate(MAX_INVENTORY_ENTRIES);
    let inventory = transactions
        .into_iter()
        .map(|transaction| {
            if peer.wtxid_relay() {
                Inventory::WTx(transaction.compute_wtxid())
            } else {
                Inventory::Transaction(transaction.compute_txid())
            }
        })
        .collect();
    send_accounted(peer, NetworkMessage::Inv(inventory), upload, account, false).await
}

fn requested_filter_range(
    source: &dyn InboundDataSource,
    filter_type: u8,
    start_height: u32,
    stop_hash: BlockHash,
    max: u32,
) -> Result<std::ops::RangeInclusive<u32>, InboundError> {
    if filter_type != BASIC_FILTER_TYPE {
        return Err(InboundError::RequestBound(
            "unsupported compact-filter type",
        ));
    }
    let stop_height = source
        .active_height(stop_hash)
        .map_err(InboundError::Data)?
        .ok_or(InboundError::RequestBound(
            "compact-filter stop hash is not active",
        ))?;
    if stop_height < start_height || stop_height - start_height >= max {
        return Err(InboundError::RequestBound("compact-filter range"));
    }
    Ok(start_height..=stop_height)
}

async fn serve_filters(
    peer: &mut InboundPeerSession<TcpStream>,
    request: bitcoin::p2p::message_filter::GetCFilters,
    source: &dyn InboundDataSource,
    upload: &UploadBudget,
    account: &InboundPeerAccount,
) -> Result<(), InboundError> {
    let range = requested_filter_range(
        source,
        request.filter_type,
        request.start_height,
        request.stop_hash,
        1_000,
    )?;
    for height in range {
        let Some(filter) = source.basic_filter(height).map_err(InboundError::Data)? else {
            return Err(InboundError::Data(format!(
                "basic filter index is unavailable at height {height}"
            )));
        };
        send_accounted(
            peer,
            NetworkMessage::CFilter(CFilter {
                filter_type: BASIC_FILTER_TYPE,
                block_hash: filter.block_hash,
                filter: filter.filter,
            }),
            upload,
            account,
            false,
        )
        .await?;
    }
    Ok(())
}

async fn serve_filter_headers(
    peer: &mut InboundPeerSession<TcpStream>,
    request: bitcoin::p2p::message_filter::GetCFHeaders,
    source: &dyn InboundDataSource,
    upload: &UploadBudget,
    account: &InboundPeerAccount,
) -> Result<(), InboundError> {
    let range = requested_filter_range(
        source,
        request.filter_type,
        request.start_height,
        request.stop_hash,
        2_000,
    )?;
    let previous_filter_header = if request.start_height == 0 {
        FilterHeader::all_zeros()
    } else {
        source
            .basic_filter(request.start_height - 1)
            .map_err(InboundError::Data)?
            .ok_or_else(|| {
                InboundError::Data("previous basic filter header is unavailable".to_owned())
            })?
            .filter_header
    };
    let mut filter_hashes = Vec::new();
    for height in range {
        let Some(filter) = source.basic_filter(height).map_err(InboundError::Data)? else {
            return Err(InboundError::Data(format!(
                "basic filter index is unavailable at height {height}"
            )));
        };
        filter_hashes.push(filter.filter_hash);
    }
    send_accounted(
        peer,
        NetworkMessage::CFHeaders(CFHeaders {
            filter_type: BASIC_FILTER_TYPE,
            stop_hash: request.stop_hash,
            previous_filter_header,
            filter_hashes,
        }),
        upload,
        account,
        false,
    )
    .await
}

async fn serve_filter_checkpoints(
    peer: &mut InboundPeerSession<TcpStream>,
    request: bitcoin::p2p::message_filter::GetCFCheckpt,
    source: &dyn InboundDataSource,
    upload: &UploadBudget,
    account: &InboundPeerAccount,
) -> Result<(), InboundError> {
    if request.filter_type != BASIC_FILTER_TYPE {
        return Err(InboundError::RequestBound(
            "unsupported compact-filter type",
        ));
    }
    let stop_height = source
        .active_height(request.stop_hash)
        .map_err(InboundError::Data)?
        .ok_or(InboundError::RequestBound(
            "compact-filter stop hash is not active",
        ))?;
    let mut filter_headers = Vec::new();
    let mut height = FILTER_HEADER_INTERVAL - 1;
    while height <= stop_height {
        let Some(filter) = source.basic_filter(height).map_err(InboundError::Data)? else {
            return Err(InboundError::Data(format!(
                "basic filter index is unavailable at height {height}"
            )));
        };
        filter_headers.push(filter.filter_header);
        let Some(next) = height.checked_add(FILTER_HEADER_INTERVAL) else {
            break;
        };
        height = next;
    }
    send_accounted(
        peer,
        NetworkMessage::CFCheckpt(CFCheckpt {
            filter_type: BASIC_FILTER_TYPE,
            stop_hash: request.stop_hash,
            filter_headers,
        }),
        upload,
        account,
        false,
    )
    .await
}

fn is_recent_block(source: &dyn InboundDataSource, hash: BlockHash) -> Result<bool, InboundError> {
    let tip = source.start_height().map_err(InboundError::Data)?;
    let Some(height) = source.active_height(hash).map_err(InboundError::Data)? else {
        return Ok(false);
    };
    Ok(height.saturating_add(RECENT_BLOCK_UPLOAD_WINDOW) >= tip)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::p2p::{TransactionRelay, connect_outbound};
    use bitcoin::{
        Network, OutPoint,
        bip152::BlockTransactionsRequest,
        blockdata::constants::genesis_block,
        p2p::{
            message_blockdata::{GetBlocksMessage, GetHeadersMessage},
            message_compact_blocks::GetBlockTxn,
            message_filter::{GetCFCheckpt, GetCFHeaders, GetCFilters},
        },
    };

    struct GenesisSource {
        block: Block,
        submitted: Mutex<Vec<Transaction>>,
        available: Mutex<Vec<Transaction>>,
    }

    impl GenesisSource {
        fn new() -> Self {
            Self {
                block: genesis_block(Network::Regtest),
                submitted: Mutex::new(Vec::new()),
                available: Mutex::new(Vec::new()),
            }
        }
    }

    impl InboundDataSource for GenesisSource {
        fn start_height(&self) -> Result<u32, String> {
            Ok(0)
        }

        fn active_header(&self, height: u32) -> Result<Option<Header>, String> {
            Ok((height == 0).then_some(self.block.header))
        }

        fn active_height(&self, hash: BlockHash) -> Result<Option<u32>, String> {
            Ok((hash == self.block.block_hash()).then_some(0))
        }

        fn block(&self, hash: BlockHash) -> Result<Option<Vec<u8>>, String> {
            Ok((hash == self.block.block_hash()).then(|| serialize(&self.block)))
        }

        fn mempool(&self) -> Result<Vec<Transaction>, String> {
            Ok(self
                .available
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone())
        }

        fn transaction(&self, inventory: Inventory) -> Result<Option<Transaction>, String> {
            Ok(self
                .available
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .iter()
                .find(|transaction| match inventory {
                    Inventory::Transaction(txid) | Inventory::WitnessTransaction(txid) => {
                        transaction.compute_txid() == txid
                    }
                    Inventory::WTx(wtxid) => transaction.compute_wtxid() == wtxid,
                    _ => false,
                })
                .cloned())
        }

        fn submit_transaction(&self, transaction: Transaction) -> Result<bool, String> {
            self.submitted
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(transaction);
            Ok(true)
        }

        fn basic_filter(&self, height: u32) -> Result<Option<InboundBasicFilter>, String> {
            Ok((height == 0).then_some(InboundBasicFilter {
                block_hash: self.block.block_hash(),
                filter: vec![0x00],
                filter_hash: FilterHash::from_byte_array([3; 32]),
                filter_header: FilterHeader::from_byte_array([4; 32]),
            }))
        }

        fn addresses(&self) -> Result<Vec<SocketAddr>, String> {
            Ok(vec!["1.2.3.4:8333".parse().unwrap()])
        }

        fn advertised_address(&self) -> Option<(SocketAddr, ServiceFlags)> {
            Some((
                "5.6.7.8:8333".parse().unwrap(),
                ServiceFlags::NETWORK_LIMITED | ServiceFlags::WITNESS,
            ))
        }

        fn fee_filter_sat_kvb(&self) -> Result<u64, String> {
            Ok(2_345)
        }
    }

    #[test]
    fn locator_and_upload_budgets_are_strict_and_recent_blocks_remain_available() {
        let source = GenesisSource::new();
        let hash = source.block.block_hash();
        assert!(
            headers_after_locator(&source, &[], hash, 2)
                .unwrap()
                .is_empty()
        );
        let upload = UploadBudget::new(1);
        assert!(upload.charge(2, true).is_err());
        assert!(upload.charge(usize::MAX, false).is_ok());
        assert!(is_recent_block(&source, hash).unwrap());
        assert_eq!(
            source.utxo(OutPointKey::from(OutPoint::null())).unwrap(),
            None
        );

        assert_eq!(
            requested_filter_range(&source, BASIC_FILTER_TYPE, 0, hash, 1).unwrap(),
            0..=0
        );
        assert!(matches!(
            requested_filter_range(&source, 1, 0, hash, 1),
            Err(InboundError::RequestBound(
                "unsupported compact-filter type"
            ))
        ));
        assert!(matches!(
            requested_filter_range(&source, BASIC_FILTER_TYPE, 1, hash, 1),
            Err(InboundError::RequestBound("compact-filter range"))
        ));
        assert!(matches!(
            requested_filter_range(
                &source,
                BASIC_FILTER_TYPE,
                0,
                BlockHash::from_byte_array([8; 32]),
                1,
            ),
            Err(InboundError::RequestBound(
                "compact-filter stop hash is not active"
            ))
        ));

        let stats = InboundStats::new(10);
        stats.reject_capacity();
        stats.reject_source();
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.rejected_capacity_total, 1);
        assert_eq!(snapshot.rejected_source_total, 1);
    }

    #[test]
    fn per_ip_admission_and_request_work_are_bounded() {
        let ip_counts = Arc::new(Mutex::new(HashMap::new()));
        let group_counts = Arc::new(Mutex::new(HashMap::new()));
        let semaphore = Arc::new(Semaphore::new(4));
        let remote = "127.0.0.1:18444".parse().unwrap();
        let first = admit_ip(
            remote,
            1,
            false,
            &ip_counts,
            &group_counts,
            Arc::clone(&semaphore).try_acquire_owned().unwrap(),
        )
        .unwrap();
        assert!(
            admit_ip(
                remote,
                1,
                false,
                &ip_counts,
                &group_counts,
                Arc::clone(&semaphore).try_acquire_owned().unwrap()
            )
            .is_none()
        );
        // A different address in the same IPv4 /16 cannot bypass the network
        // group ceiling; a distinct /16 remains independently admissible.
        assert!(
            admit_ip(
                "127.0.0.2:18444".parse().unwrap(),
                1,
                false,
                &ip_counts,
                &group_counts,
                Arc::clone(&semaphore).try_acquire_owned().unwrap()
            )
            .is_none()
        );
        let other_group = admit_ip(
            "126.1.0.2:18444".parse().unwrap(),
            1,
            false,
            &ip_counts,
            &group_counts,
            Arc::clone(&semaphore).try_acquire_owned().unwrap(),
        )
        .unwrap();
        drop(first);
        assert_eq!(
            ip_counts
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .get(&remote.ip()),
            None
        );
        drop(other_group);
        assert!(
            group_counts
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .is_empty()
        );
        assert_eq!(
            network_group("::ffff:1.2.3.4".parse().unwrap()),
            network_group("1.2.3.4".parse().unwrap())
        );

        let mut budget = RequestBudget::new(2);
        budget.charge().unwrap();
        budget.charge().unwrap();
        assert!(matches!(budget.charge(), Err(InboundError::RequestBudget)));
    }

    #[test]
    fn adaptive_eviction_improves_network_diversity_without_churn() {
        let stats = Arc::new(InboundStats::new(0));
        let useful = stats.accept("10.1.0.1:8333".parse().unwrap(), false);
        useful.handshake(70_016, "/useful/".to_owned());
        useful.request();
        useful.upload(1_000, false);
        let duplicate = stats.accept("10.1.1.1:8333".parse().unwrap(), false);
        duplicate.handshake(70_016, "/duplicate/".to_owned());
        let unique = stats.accept("11.1.0.1:8333".parse().unwrap(), false);
        unique.handshake(70_016, "/unique/".to_owned());

        assert_eq!(
            stats.eviction_candidate("12.1.0.1:8333".parse().unwrap(), false),
            Some(duplicate.id)
        );
        assert_eq!(
            stats.eviction_candidate("10.1.2.1:8333".parse().unwrap(), false),
            None
        );
        duplicate.finish(Some(&InboundError::Evicted));
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.evicted_total, 1);
        assert_eq!(snapshot.active, 2);
        assert_eq!(
            stats.eviction_candidate("12.1.0.1:8333".parse().unwrap(), false),
            None
        );
    }

    #[test]
    fn preferred_admission_bypasses_only_the_source_ceiling() {
        let ip_counts = Arc::new(Mutex::new(HashMap::new()));
        let group_counts = Arc::new(Mutex::new(HashMap::new()));
        let semaphore = Arc::new(Semaphore::new(2));
        let remote = "127.0.0.1:18444".parse().unwrap();
        let ordinary = admit_ip(
            remote,
            1,
            false,
            &ip_counts,
            &group_counts,
            Arc::clone(&semaphore).try_acquire_owned().unwrap(),
        )
        .unwrap();
        let preferred = admit_ip(
            remote,
            1,
            true,
            &ip_counts,
            &group_counts,
            Arc::clone(&semaphore).try_acquire_owned().unwrap(),
        )
        .unwrap();
        assert!(Arc::clone(&semaphore).try_acquire_owned().is_err());
        drop((ordinary, preferred));
        assert_eq!(semaphore.available_permits(), 2);
    }

    #[test]
    fn preferred_inbound_can_replace_but_cannot_be_replaced() {
        let stats = Arc::new(InboundStats::new(0));
        let ordinary = stats.accept("10.1.0.1:8333".parse().unwrap(), false);
        ordinary.handshake(70_016, "/ordinary/".to_owned());
        let protected = stats.accept("11.1.0.1:8333".parse().unwrap(), true);
        protected.handshake(70_016, "/protected/".to_owned());

        assert_eq!(
            stats.eviction_candidate("10.1.1.1:8333".parse().unwrap(), true),
            Some(ordinary.id)
        );
        ordinary.finish(Some(&InboundError::Evicted));
        assert_eq!(
            stats.eviction_candidate("12.1.0.1:8333".parse().unwrap(), false),
            None
        );
        let snapshot = stats.snapshot();
        assert!(snapshot.peers[0].preferred);
    }

    #[test]
    fn accounting_classifies_every_disconnect_reason() {
        let stats = Arc::new(InboundStats::new(100));
        let cases = [
            InboundError::RequestBudget,
            InboundError::UploadTarget,
            InboundError::Protocol(P2pError::WrongMagic),
            InboundError::RequestBound("test"),
            InboundError::Io(std::io::Error::other("test")),
            InboundError::Data("test".to_owned()),
            InboundError::Evicted,
        ];
        for (port, error) in (1_u16..).zip(cases) {
            stats
                .accept(SocketAddr::from(([127, 0, 0, 1], port)), false)
                .finish(Some(&error));
        }
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.completed_total, 7);
        assert_eq!(snapshot.request_budget_disconnects, 1);
        assert_eq!(snapshot.upload_target_disconnects, 1);
        assert_eq!(snapshot.protocol_disconnects, 1);
        assert_eq!(snapshot.request_bound_disconnects, 1);
        assert_eq!(snapshot.io_disconnects, 1);
        assert_eq!(snapshot.data_disconnects, 1);
        assert_eq!(snapshot.evicted_total, 1);
    }

    #[test]
    fn relay_filter_and_per_peer_deduplication_are_exact_and_bounded() {
        let transaction = GenesisSource::new().block.txdata[0].clone();
        let relay = TransactionRelay {
            transaction,
            fee_sats: Some(999),
            policy_vsize: 500,
        };
        assert!(!relay_meets_fee_filter(&relay, 2_000));
        assert!(relay_meets_fee_filter(
            &TransactionRelay {
                fee_sats: Some(1_000),
                ..relay.clone()
            },
            2_000
        ));
        assert!(relay_meets_fee_filter(
            &TransactionRelay {
                fee_sats: None,
                ..relay
            },
            u64::MAX
        ));

        let mut announced = HashSet::new();
        let mut order = VecDeque::new();
        for index in 0..=MAX_RELAY_ANNOUNCEMENTS_PER_PEER {
            let inventory = Inventory::Transaction(bitcoin::Txid::from_byte_array(
                u32::try_from(index)
                    .unwrap()
                    .to_le_bytes()
                    .repeat(8)
                    .try_into()
                    .unwrap(),
            ));
            assert!(remember_announcement(inventory, &mut announced, &mut order));
        }
        assert_eq!(announced.len(), MAX_RELAY_ANNOUNCEMENTS_PER_PEER);
        assert_eq!(order.len(), MAX_RELAY_ANNOUNCEMENTS_PER_PEER);
        assert!(!remember_announcement(
            *order.back().unwrap(),
            &mut announced,
            &mut order
        ));
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn listener_accepts_a_real_peer_and_serves_a_retained_witness_block() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let source = Arc::new(GenesisSource::new());
        let service_source: Arc<dyn InboundDataSource> = source.clone();
        let hash = source.block.block_hash();
        let stats = Arc::new(InboundStats::new(1_024));
        let (relay, _) = broadcast::channel(8);
        let task = tokio::spawn(run_listener_with_stats_and_relay(
            listener,
            Network::Regtest.magic(),
            2,
            "/rbtcd:inbound-test/".to_owned(),
            ServiceFlags::NETWORK_LIMITED | ServiceFlags::WITNESS,
            InboundLimits::default(),
            service_source,
            Arc::clone(&stats),
            Some(relay.clone()),
        ));
        let mut peer = connect_outbound(
            address,
            Network::Regtest.magic(),
            1,
            "/rbtcd:client-test/".to_owned(),
            0,
        )
        .await
        .unwrap();
        assert!(
            peer.remote_version()
                .services
                .has(ServiceFlags::NETWORK_LIMITED | ServiceFlags::WITNESS)
        );
        peer.request_addresses().await.unwrap();
        let addresses = peer.receive_addresses().await.unwrap();
        assert_eq!(addresses.len(), 2);
        assert_eq!(addresses[0].socket, "5.6.7.8:8333".parse().unwrap());
        assert!(
            addresses[0]
                .services
                .has(ServiceFlags::NETWORK_LIMITED | ServiceFlags::WITNESS)
        );
        assert_eq!(addresses[1].socket, "1.2.3.4:8333".parse().unwrap());
        assert_eq!(peer.fee_filter_sat_kvb(), 2_345);
        let mut relayed = source.block.txdata[0].clone();
        relayed.input[0].previous_output.vout = 1;
        source
            .available
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(relayed.clone());
        relay
            .send(TransactionRelay {
                transaction: relayed.clone(),
                fee_sats: Some(1_000),
                policy_vsize: relayed.vsize(),
            })
            .unwrap();
        peer.ping(11).await.unwrap();
        assert_eq!(
            peer.take_pending_transaction_inventory(),
            vec![Inventory::WTx(relayed.compute_wtxid())]
        );
        peer.request_blocks(&[hash]).await.unwrap();
        let block = peer.receive_requested_block(hash).await.unwrap();
        assert_eq!(block.block_hash(), hash);
        let observed = stats.snapshot();
        assert_eq!(observed.active, 1);
        assert_eq!(observed.accepted_total, 1);
        assert!(observed.peers[0].handshake_complete);
        assert!(observed.peers[0].requests >= 2);
        assert!(observed.peers[0].uploaded_bytes > 0);
        let mut transaction = source.block.txdata[0].clone();
        transaction.input[0].previous_output = OutPoint {
            txid: bitcoin::Txid::all_zeros(),
            vout: 0,
        };
        peer.relay_transaction(
            &TransactionRelay {
                transaction: transaction.clone(),
                fee_sats: Some(1_000),
                policy_vsize: transaction.vsize(),
            },
            9,
        )
        .await
        .unwrap();
        timeout(Duration::from_secs(1), async {
            loop {
                if !source
                    .submitted
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .is_empty()
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(
            source
                .submitted
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .as_slice(),
            &[transaction]
        );

        // Exercise the remaining serving routes over the same real,
        // checksum-validated connection.
        let mut transport = peer.into_test_transport();
        transport
            .write_message(NetworkMessage::GetHeaders(GetHeadersMessage {
                version: bitcoin::p2p::PROTOCOL_VERSION,
                locator_hashes: vec![hash],
                stop_hash: BlockHash::all_zeros(),
            }))
            .await
            .unwrap();
        assert!(matches!(
            transport.read_message().await.unwrap().into_payload(),
            NetworkMessage::Headers(headers) if headers.is_empty()
        ));

        transport
            .write_message(NetworkMessage::GetBlocks(GetBlocksMessage::new(
                vec![hash],
                BlockHash::all_zeros(),
            )))
            .await
            .unwrap();
        assert!(matches!(
            transport.read_message().await.unwrap().into_payload(),
            NetworkMessage::Inv(inventory) if inventory.is_empty()
        ));

        transport
            .write_message(NetworkMessage::MemPool)
            .await
            .unwrap();
        assert!(matches!(
            transport.read_message().await.unwrap().into_payload(),
            NetworkMessage::Inv(inventory)
                if inventory == vec![Inventory::WTx(relayed.compute_wtxid())]
        ));

        let missing = Inventory::Transaction(bitcoin::Txid::from_byte_array([9; 32]));
        transport
            .write_message(NetworkMessage::GetData(vec![
                Inventory::WTx(relayed.compute_wtxid()),
                missing,
            ]))
            .await
            .unwrap();
        assert!(matches!(
            transport.read_message().await.unwrap().into_payload(),
            NetworkMessage::Tx(actual) if actual == relayed
        ));
        assert!(matches!(
            transport.read_message().await.unwrap().into_payload(),
            NetworkMessage::NotFound(inventory) if inventory == vec![missing]
        ));

        transport
            .write_message(NetworkMessage::GetData(vec![Inventory::CompactBlock(hash)]))
            .await
            .unwrap();
        assert!(matches!(
            transport.read_message().await.unwrap().into_payload(),
            NetworkMessage::CmpctBlock(_)
        ));
        transport
            .write_message(NetworkMessage::GetBlockTxn(GetBlockTxn {
                txs_request: BlockTransactionsRequest {
                    block_hash: hash,
                    indexes: vec![0],
                },
            }))
            .await
            .unwrap();
        assert!(matches!(
            transport.read_message().await.unwrap().into_payload(),
            NetworkMessage::BlockTxn(response)
                if response.transactions.block_hash == hash
                    && response.transactions.transactions.len() == 1
        ));

        transport
            .write_message(NetworkMessage::GetCFilters(GetCFilters {
                filter_type: BASIC_FILTER_TYPE,
                start_height: 0,
                stop_hash: hash,
            }))
            .await
            .unwrap();
        assert!(matches!(
            transport.read_message().await.unwrap().into_payload(),
            NetworkMessage::CFilter(filter)
                if filter.block_hash == hash && filter.filter == vec![0]
        ));
        transport
            .write_message(NetworkMessage::GetCFHeaders(GetCFHeaders {
                filter_type: BASIC_FILTER_TYPE,
                start_height: 0,
                stop_hash: hash,
            }))
            .await
            .unwrap();
        assert!(matches!(
            transport.read_message().await.unwrap().into_payload(),
            NetworkMessage::CFHeaders(headers)
                if headers.stop_hash == hash
                    && headers.previous_filter_header == FilterHeader::all_zeros()
                    && headers.filter_hashes == vec![FilterHash::from_byte_array([3; 32])]
        ));
        transport
            .write_message(NetworkMessage::GetCFCheckpt(GetCFCheckpt {
                filter_type: BASIC_FILTER_TYPE,
                stop_hash: hash,
            }))
            .await
            .unwrap();
        assert!(matches!(
            transport.read_message().await.unwrap().into_payload(),
            NetworkMessage::CFCheckpt(checkpoint)
                if checkpoint.stop_hash == hash && checkpoint.filter_headers.is_empty()
        ));

        drop(transport);
        timeout(Duration::from_secs(1), async {
            loop {
                if stats.snapshot().active == 0 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(stats.snapshot().completed_total, 1);
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
    }

    #[tokio::test]
    async fn public_listener_wrapper_accepts_a_real_handshake() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let source: Arc<dyn InboundDataSource> = Arc::new(GenesisSource::new());
        let task = tokio::spawn(run_listener(
            listener,
            Network::Regtest.magic(),
            2,
            "/rbtcd:listener-wrapper/".to_owned(),
            ServiceFlags::NETWORK_LIMITED | ServiceFlags::WITNESS,
            InboundLimits::default(),
            source,
        ));
        let peer = connect_outbound(
            address,
            Network::Regtest.magic(),
            1,
            "/rbtcd:wrapper-client/".to_owned(),
            0,
        )
        .await
        .unwrap();
        assert_eq!(peer.remote_version().start_height, 0);
        drop(peer);
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
    }

    #[tokio::test]
    #[ignore = "requires RBTC_BITCOIND and RBTC_BTCD executable paths"]
    async fn core31_and_btcd_complete_real_inbound_handshakes() {
        let bitcoind = std::env::var_os("RBTC_BITCOIND").expect("RBTC_BITCOIND is required");
        let btcd = std::env::var_os("RBTC_BTCD").expect("RBTC_BTCD is required");
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let source: Arc<dyn InboundDataSource> = Arc::new(GenesisSource::new());
        let stats = Arc::new(InboundStats::new(1024 * 1024));
        let task = tokio::spawn(run_listener_with_stats(
            listener,
            Network::Regtest.magic(),
            2,
            "/rbtcd:interop/".to_owned(),
            ServiceFlags::NETWORK | ServiceFlags::WITNESS,
            InboundLimits {
                idle_timeout: Duration::from_secs(30),
                ..InboundLimits::default()
            },
            source,
            Arc::clone(&stats),
        ));

        let core_dir = tempfile::tempdir().unwrap();
        let mut core = std::process::Command::new(bitcoind)
            .args([
                "-regtest",
                "-server=0",
                "-listen=0",
                "-dnsseed=0",
                "-discover=0",
                "-v2transport=0",
                "-printtoconsole=0",
                &format!("-datadir={}", core_dir.path().display()),
                &format!("-connect={address}"),
            ])
            .spawn()
            .unwrap();
        let mut core_ready = false;
        for _ in 0..10_000 {
            if stats.snapshot().handshakes_total >= 1 {
                core_ready = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let _ = core.kill();
        let _ = core.wait();

        let btcd_dir = tempfile::tempdir().unwrap();
        let mut btcd = std::process::Command::new(btcd)
            .args([
                "--regtest",
                "--nolisten",
                "--norpc",
                "--nodnsseed",
                "--nostalldetect",
                &format!("--datadir={}", btcd_dir.path().display()),
                &format!("--logdir={}", btcd_dir.path().display()),
                &format!("--connect={address}"),
            ])
            .spawn()
            .unwrap();

        let mut btcd_ready = false;
        for _ in 0..10_000 {
            if stats.snapshot().handshakes_total >= 2 {
                btcd_ready = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        let _ = btcd.kill();
        let _ = btcd.wait();
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert!(
            core_ready && btcd_ready,
            "missing external handshake: core={core_ready}, btcd={btcd_ready}, stats={:?}",
            stats.snapshot()
        );
    }
}
