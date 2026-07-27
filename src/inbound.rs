//! Bounded inbound Bitcoin P2P service.
//!
//! Framing and handshake rules live in [`crate::p2p`]. This module owns the
//! accepting-side resource policy and serves only data authenticated and
//! published by the node-provided [`InboundDataSource`].

use std::{
    collections::{HashMap, HashSet},
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
        message::NetworkMessage,
        message_blockdata::Inventory,
        message_compact_blocks::{BlockTxn, CmpctBlock, SendCmpct},
        message_filter::{CFCheckpt, CFHeaders, CFilter},
    },
};
use thiserror::Error;
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{OwnedSemaphorePermit, Semaphore},
    task::JoinSet,
    time::timeout,
};

use crate::p2p::{
    InboundPeerSession, MAX_COMPACT_BLOCK_TRANSACTIONS, MAX_HEADERS_PER_RESPONSE,
    MAX_INVENTORY_ENTRIES, P2pError, accept_inbound,
};

const MAX_GETBLOCKS_RESULTS: usize = 500;
const BASIC_FILTER_TYPE: u8 = 0;
const FILTER_HEADER_INTERVAL: u32 = 1_000;
const RECENT_BLOCK_UPLOAD_WINDOW: u32 = 288;
const SENDHEADERS_VERSION: u32 = 70_012;
const FEEFILTER_VERSION: u32 = 70_013;
const SENDCMPCT_VERSION: u32 = 70_014;

/// Resource ceilings for one optional inbound listener.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
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
}

impl Default for InboundLimits {
    fn default() -> Self {
        Self {
            max_connections: 32,
            max_connections_per_ip: 4,
            max_upload_bytes_per_day: 1024 * 1024 * 1024,
            max_requests_per_minute: 1_200,
            idle_timeout: Duration::from_secs(20 * 60),
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
    /// Current local minimum mempool fee in satoshis per 1,000 virtual bytes.
    fn fee_filter_sat_kvb(&self) -> Result<u64, String> {
        Ok(0)
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
    if ips.get(&ip).copied().unwrap_or_default() >= limit
        || groups.get(&group).copied().unwrap_or_default() >= limit
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
    let global = Arc::new(Semaphore::new(limits.max_connections));
    let per_ip = Arc::new(Mutex::new(HashMap::new()));
    let per_group = Arc::new(Mutex::new(HashMap::new()));
    let upload = Arc::new(UploadBudget::new(limits.max_upload_bytes_per_day));
    let mut peers = JoinSet::new();
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let (stream, remote) = accepted?;
                let Ok(global_permit) = Arc::clone(&global).try_acquire_owned() else {
                    drop(stream);
                    continue;
                };
                let Some(admission) =
                    admit_ip(
                        remote,
                        limits.max_connections_per_ip,
                        &per_ip,
                        &per_group,
                        global_permit,
                    )
                else {
                    drop(stream);
                    continue;
                };
                let source = Arc::clone(&source);
                let upload = Arc::clone(&upload);
                let user_agent = user_agent.clone();
                peers.spawn(async move {
                    let _admission = admission;
                    let _ = serve_peer(
                        stream,
                        magic,
                        local_nonce,
                        user_agent,
                        services,
                        limits,
                        source,
                        upload,
                    )
                    .await;
                });
            }
            completed = peers.join_next(), if !peers.is_empty() => {
                let _ = completed;
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn serve_peer(
    stream: TcpStream,
    magic: bitcoin::p2p::Magic,
    local_nonce: u64,
    user_agent: String,
    services: ServiceFlags,
    limits: InboundLimits,
    source: Arc<dyn InboundDataSource>,
    upload: Arc<UploadBudget>,
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
    if peer.remote_version().version >= SENDHEADERS_VERSION {
        send_accounted(&mut peer, NetworkMessage::SendHeaders, &upload, false).await?;
    }
    if peer.remote_version().version >= SENDCMPCT_VERSION {
        send_accounted(
            &mut peer,
            NetworkMessage::SendCmpct(SendCmpct {
                send_compact: false,
                version: 2,
            }),
            &upload,
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
            false,
        )
        .await?;
    }
    let mut request_budget = RequestBudget::new(limits.max_requests_per_minute);
    let mut requested_transactions = HashSet::new();
    loop {
        let message = timeout(limits.idle_timeout, peer.receive_message())
            .await
            .map_err(|_| {
                InboundError::Io(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "inbound peer idle timeout",
                ))
            })??;
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
        }
        route_message(
            &mut peer,
            message,
            source.as_ref(),
            &upload,
            &mut requested_transactions,
        )
        .await?;
    }
}

async fn send_accounted(
    peer: &mut InboundPeerSession<TcpStream>,
    message: NetworkMessage,
    upload: &UploadBudget,
    historical_block: bool,
) -> Result<(), InboundError> {
    upload.charge(serialize(&message).len(), historical_block)?;
    peer.send_message(message).await?;
    Ok(())
}

#[allow(clippy::too_many_lines)]
async fn route_message(
    peer: &mut InboundPeerSession<TcpStream>,
    message: NetworkMessage,
    source: &dyn InboundDataSource,
    upload: &UploadBudget,
    requested_transactions: &mut HashSet<Inventory>,
) -> Result<(), InboundError> {
    match message {
        NetworkMessage::Ping(nonce) => {
            send_accounted(peer, NetworkMessage::Pong(nonce), upload, false).await
        }
        NetworkMessage::GetHeaders(request) => {
            let headers = headers_after_locator(
                source,
                &request.locator_hashes,
                request.stop_hash,
                MAX_HEADERS_PER_RESPONSE,
            )?;
            send_accounted(peer, NetworkMessage::Headers(headers), upload, false).await
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
            send_accounted(peer, NetworkMessage::Inv(inventory), upload, false).await
        }
        NetworkMessage::GetAddr => serve_addresses(peer, source, upload).await,
        NetworkMessage::GetData(requests) => serve_getdata(peer, requests, source, upload).await,
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
                send_accounted(peer, NetworkMessage::GetData(requests), upload, false).await
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
        NetworkMessage::MemPool => serve_mempool(peer, source, upload).await,
        NetworkMessage::GetBlockTxn(request) => {
            let hash = request.txs_request.block_hash;
            let Some(raw) = source.block(hash).map_err(InboundError::Data)? else {
                return send_accounted(
                    peer,
                    NetworkMessage::NotFound(vec![Inventory::CompactBlock(hash)]),
                    upload,
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
                !is_recent_block(source, hash)?,
            )
            .await
        }
        NetworkMessage::GetCFilters(request) => serve_filters(peer, request, source, upload).await,
        NetworkMessage::GetCFHeaders(request) => {
            serve_filter_headers(peer, request, source, upload).await
        }
        NetworkMessage::GetCFCheckpt(request) => {
            serve_filter_checkpoints(peer, request, source, upload).await
        }
        NetworkMessage::Version(_) => Err(InboundError::Protocol(P2pError::PostHandshakeVersion)),
        _ => Ok(()),
    }
}

async fn serve_addresses(
    peer: &mut InboundPeerSession<TcpStream>,
    source: &dyn InboundDataSource,
    upload: &UploadBudget,
) -> Result<(), InboundError> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| InboundError::Data(format!("system clock: {error}")))?
        .as_secs();
    let now = u32::try_from(now).unwrap_or(u32::MAX);
    let services = ServiceFlags::NETWORK | ServiceFlags::WITNESS;
    let mut seen = HashSet::new();
    let addresses = source
        .addresses()
        .map_err(InboundError::Data)?
        .into_iter()
        .filter(|address| address.port() != 0 && seen.insert(*address))
        .take(crate::p2p::MAX_ADDRESSES_PER_MESSAGE)
        .map(|address| (now, Address::new(&address, services)))
        .collect();
    send_accounted(peer, NetworkMessage::Addr(addresses), upload, false).await
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
                    send_accounted(peer, NetworkMessage::Tx(transaction), upload, false).await?;
                } else {
                    not_found.push(request);
                }
            }
            Inventory::Error | Inventory::Unknown { .. } => not_found.push(request),
        }
    }
    if !not_found.is_empty() {
        send_accounted(peer, NetworkMessage::NotFound(not_found), upload, false).await?;
    }
    Ok(())
}

async fn serve_mempool(
    peer: &mut InboundPeerSession<TcpStream>,
    source: &dyn InboundDataSource,
    upload: &UploadBudget,
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
    send_accounted(peer, NetworkMessage::Inv(inventory), upload, false).await
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
        false,
    )
    .await
}

async fn serve_filter_checkpoints(
    peer: &mut InboundPeerSession<TcpStream>,
    request: bitcoin::p2p::message_filter::GetCFCheckpt,
    source: &dyn InboundDataSource,
    upload: &UploadBudget,
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
    use bitcoin::{Network, OutPoint, blockdata::constants::genesis_block};

    struct GenesisSource {
        block: Block,
        submitted: Mutex<Vec<Transaction>>,
    }

    impl GenesisSource {
        fn new() -> Self {
            Self {
                block: genesis_block(Network::Regtest),
                submitted: Mutex::new(Vec::new()),
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
            Ok(Vec::new())
        }

        fn transaction(&self, _inventory: Inventory) -> Result<Option<Transaction>, String> {
            Ok(None)
        }

        fn submit_transaction(&self, transaction: Transaction) -> Result<bool, String> {
            self.submitted
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(transaction);
            Ok(true)
        }

        fn basic_filter(&self, _height: u32) -> Result<Option<InboundBasicFilter>, String> {
            Ok(None)
        }

        fn addresses(&self) -> Result<Vec<SocketAddr>, String> {
            Ok(vec!["1.2.3.4:8333".parse().unwrap()])
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
            &ip_counts,
            &group_counts,
            Arc::clone(&semaphore).try_acquire_owned().unwrap(),
        )
        .unwrap();
        assert!(
            admit_ip(
                remote,
                1,
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
                &ip_counts,
                &group_counts,
                Arc::clone(&semaphore).try_acquire_owned().unwrap()
            )
            .is_none()
        );
        let other_group = admit_ip(
            "126.1.0.2:18444".parse().unwrap(),
            1,
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

    #[tokio::test]
    async fn listener_accepts_a_real_peer_and_serves_a_retained_witness_block() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let source = Arc::new(GenesisSource::new());
        let service_source: Arc<dyn InboundDataSource> = source.clone();
        let hash = source.block.block_hash();
        let task = tokio::spawn(run_listener(
            listener,
            Network::Regtest.magic(),
            2,
            "/rbtcd:inbound-test/".to_owned(),
            ServiceFlags::NETWORK_LIMITED | ServiceFlags::WITNESS,
            InboundLimits::default(),
            service_source,
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
        assert_eq!(addresses.len(), 1);
        assert_eq!(addresses[0].socket, "1.2.3.4:8333".parse().unwrap());
        assert_eq!(peer.fee_filter_sat_kvb(), 2_345);
        peer.request_blocks(&[hash]).await.unwrap();
        let block = peer.receive_requested_block(hash).await.unwrap();
        assert_eq!(block.block_hash(), hash);
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
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
    }
}
