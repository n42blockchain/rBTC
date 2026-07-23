//! Bitcoin P2P v1 framing delegated to rust-bitcoin.
//!
//! This module deliberately does not implement an ad-hoc wire format. The
//! payload and envelope are exactly rust-bitcoin's consensus serialization of
//! Bitcoin's `RawNetworkMessage`. BIP324 v2 transport is tracked as a gated
//! follow-up because it changes the encrypted session handshake, not messages.

use bitcoin::{
    BlockHash, Transaction,
    consensus::{deserialize, encode::Error as EncodeError, serialize},
    p2p::{
        Address, Magic, ServiceFlags,
        address::AddrV2Message,
        message::{MAX_INV_SIZE, NetworkMessage, RawNetworkMessage},
        message_blockdata::{GetHeadersMessage, Inventory},
        message_network::VersionMessage,
    },
};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    net::SocketAddr,
    time::{SystemTime, UNIX_EPOCH},
};
use thiserror::Error;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::TcpStream,
};

/// Bitcoin Core 26's maximum accepted v1 P2P payload size (4,000,000 bytes).
pub const MAX_PROTOCOL_MESSAGE_LEN: u32 = 4_000_000;
const V1_HEADER_LEN: usize = 24;
const MAX_HANDSHAKE_MESSAGES: usize = 8;
const MAX_RESPONSE_MESSAGES: usize = 32;
const MAX_USER_AGENT_LEN: usize = 256;
const MAX_LOCATOR_HASHES: usize = 101;
const SENDHEADERS_VERSION: u32 = 70_012;
const ADDRESS_RELAY_VERSION: u32 = 70_016;
const MAX_ADDRESSES_PER_MESSAGE: usize = 1_000;
/// Maximum inventory entries accepted in `inv`, `getdata`, or `notfound`.
pub const MAX_INVENTORY_ENTRIES: usize = MAX_INV_SIZE;
/// Maximum headers permitted in one protocol `headers` response.
pub const MAX_HEADERS_PER_RESPONSE: usize = 2_000;
/// Maximum block bodies requested concurrently from one peer.
pub const MAX_BLOCKS_IN_FLIGHT: usize = 16;
const PROTOCOL_VERSION: u32 = 70_016;
const MIN_PEER_PROTOCOL_VERSION: u32 = 31_800;

fn validate_user_agent(user_agent: &str) -> Result<(), P2pError> {
    if user_agent.len() > MAX_USER_AGENT_LEN {
        return Err(P2pError::OversizeUserAgent {
            length: user_agent.len(),
            limit: MAX_USER_AGENT_LEN,
        });
    }
    Ok(())
}

/// Async v1 P2P framing error.
#[derive(Debug, Error)]
pub enum P2pError {
    /// Underlying stream I/O failed.
    #[error("p2p io: {0}")]
    Io(#[from] std::io::Error),
    /// rust-bitcoin rejected the protocol frame/checksum.
    #[error("p2p message: {0}")]
    Message(#[from] EncodeError),
    /// A peer sent a frame for a different Bitcoin network.
    #[error("unexpected network magic")]
    WrongMagic,
    /// A peer announced a payload larger than the configured hard limit.
    #[error("p2p payload length {length} exceeds limit {limit}")]
    Oversize {
        /// Announced payload length.
        length: u32,
        /// Configured maximum length.
        limit: u32,
    },
    /// A peer sent a second version message during the initial handshake.
    #[error("duplicate version message during handshake")]
    DuplicateVersion,
    /// A peer sent `version` after the handshake had completed.
    #[error("version message received after handshake")]
    PostHandshakeVersion,
    /// A peer acknowledged the connection before identifying itself.
    #[error("verack received before version during handshake")]
    VerackBeforeVersion,
    /// The peer did not complete the handshake within the bounded message budget.
    #[error("peer did not complete handshake within {MAX_HANDSHAKE_MESSAGES} messages")]
    HandshakeIncomplete,
    /// A peer did not provide the requested response within the bounded message budget.
    #[error("peer did not provide headers within {MAX_RESPONSE_MESSAGES} messages")]
    HeadersResponseIncomplete,
    /// A peer did not provide an address response within the bounded message budget.
    #[error("peer did not provide addresses within {MAX_RESPONSE_MESSAGES} messages")]
    AddressResponseIncomplete,
    /// A peer exceeded the protocol maximum for one `headers` response.
    #[error("peer sent {count} headers; limit is {MAX_HEADERS_PER_RESPONSE}")]
    TooManyHeaders {
        /// Number of headers received from the peer.
        count: usize,
    },
    /// A peer did not provide a requested block within the bounded message budget.
    #[error("peer did not provide block {requested} within {MAX_RESPONSE_MESSAGES} messages")]
    BlockResponseIncomplete {
        /// Block requested by the caller.
        requested: BlockHash,
    },
    /// A peer supplied a block that was not part of the outstanding request.
    #[error("peer sent unexpected block {actual}; requested {expected}")]
    UnexpectedBlock {
        /// Block requested by the caller.
        expected: BlockHash,
        /// Block received from the peer.
        actual: BlockHash,
    },
    /// A peer explicitly reported that it does not have the requested block.
    #[error("peer does not have requested block {0}")]
    BlockNotFound(BlockHash),
    /// The peer predates the minimum protocol version accepted by Core 26.
    #[error("peer protocol version {actual} is below minimum {minimum}")]
    ObsoleteVersion {
        /// Version announced by the peer.
        actual: u32,
        /// Minimum compatible version.
        minimum: u32,
    },
    /// A local or remote version message contains an oversized user agent.
    #[error("peer user agent length {length} exceeds limit {limit}")]
    OversizeUserAgent {
        /// UTF-8 byte length of the user-agent string.
        length: usize,
        /// Maximum accepted byte length.
        limit: usize,
    },
    /// The outbound connection reached another instance of this node.
    #[error("peer version nonce matches local nonce")]
    SelfConnection,
    /// A block source lacks full-history and/or witness relay capability.
    #[error("peer services {offered} do not include required {required}")]
    MissingServices {
        /// Services required for full witness block IBD.
        required: ServiceFlags,
        /// Services announced by the peer.
        offered: ServiceFlags,
    },
    /// A caller attempted to exceed the per-peer in-flight block bound.
    #[error("requested {count} blocks at once; limit is {MAX_BLOCKS_IN_FLIGHT}")]
    TooManyBlockRequests {
        /// Requested inventory count.
        count: usize,
    },
    /// A batch contains the same requested hash more than once.
    #[error("duplicate requested block {0}")]
    DuplicateBlockRequest(BlockHash),
    /// A peer sent a block outside the outstanding batch.
    #[error("peer sent unsolicited block {0}")]
    UnsolicitedBlock(BlockHash),
    /// A caller supplied more locator hashes than Bitcoin Core accepts.
    #[error("getheaders locator contains {count} hashes; limit is {MAX_LOCATOR_HASHES}")]
    TooManyLocatorHashes {
        /// Number of supplied locator hashes.
        count: usize,
    },
    /// A peer exceeded Bitcoin Core's address-message entry limit.
    #[error("peer sent {count} addresses; limit is {MAX_ADDRESSES_PER_MESSAGE}")]
    TooManyAddresses {
        /// Number of addresses received from the peer.
        count: usize,
    },
    /// A peer exceeded Bitcoin Core's inventory-vector entry limit.
    #[error("peer sent {count} entries in {command}; limit is {MAX_INVENTORY_ENTRIES}")]
    TooManyInventoryEntries {
        /// Wire command containing the oversized vector.
        command: &'static str,
        /// Number of inventory entries received.
        count: usize,
    },
    /// A peer sent an oversized locator in an unsolicited request.
    #[error("peer sent {count} locator hashes in {command}; limit is {MAX_LOCATOR_HASHES}")]
    TooManyRemoteLocatorHashes {
        /// Wire command containing the oversized locator.
        command: &'static str,
        /// Number of locator hashes received.
        count: usize,
    },
    /// A matching pong did not arrive within the shared response-frame budget.
    #[error("peer did not provide the requested pong within {MAX_RESPONSE_MESSAGES} messages")]
    PongResponseIncomplete,
    /// A local caller attempted to relay a coinbase transaction.
    #[error("coinbase transactions cannot be relayed")]
    OutboundCoinbaseTransaction,
    /// A local caller attempted to relay a transaction above the standardness limit.
    #[error("transaction weight {weight} exceeds relay limit {limit}")]
    OutboundTransactionTooHeavy {
        /// Transaction weight in weight units.
        weight: u64,
        /// Maximum standard transaction weight in weight units.
        limit: u64,
    },
}

impl P2pError {
    /// Returns whether the error proves that the remote peer violated a bounded wire rule.
    ///
    /// Timeouts, I/O failures, obsolete capabilities, unavailable blocks, and caller-side
    /// request mistakes are deliberately not classified as peer misbehavior.
    #[must_use]
    pub const fn is_protocol_violation(&self) -> bool {
        match self {
            Self::Io(_)
            | Self::Message(EncodeError::Io(_))
            | Self::HandshakeIncomplete
            | Self::HeadersResponseIncomplete
            | Self::AddressResponseIncomplete
            | Self::BlockResponseIncomplete { .. }
            | Self::BlockNotFound(_)
            | Self::ObsoleteVersion { .. }
            | Self::SelfConnection
            | Self::MissingServices { .. }
            | Self::TooManyBlockRequests { .. }
            | Self::DuplicateBlockRequest(_)
            | Self::TooManyLocatorHashes { .. }
            | Self::PongResponseIncomplete
            | Self::OutboundCoinbaseTransaction
            | Self::OutboundTransactionTooHeavy { .. } => false,
            Self::Message(_)
            | Self::WrongMagic
            | Self::Oversize { .. }
            | Self::DuplicateVersion
            | Self::PostHandshakeVersion
            | Self::VerackBeforeVersion
            | Self::TooManyHeaders { .. }
            | Self::UnexpectedBlock { .. }
            | Self::OversizeUserAgent { .. }
            | Self::UnsolicitedBlock(_)
            | Self::TooManyAddresses { .. }
            | Self::TooManyInventoryEntries { .. }
            | Self::TooManyRemoteLocatorHashes { .. } => true,
        }
    }
}

/// A directly connectable IPv4 or IPv6 address learned from `addr`/`addrv2`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PeerAddress {
    /// Socket address advertised by the peer.
    pub socket: SocketAddr,
    /// Service flags associated with the advertised address.
    pub services: ServiceFlags,
    /// Peer-supplied last-seen Unix timestamp.
    pub last_seen: u32,
}

/// An established Bitcoin peer session.
///
/// The session owns its transport, so messages left after handshake remain in
/// order for header and block synchronisation.
pub struct PeerSession<S> {
    transport: V1Transport<S>,
    remote_version: VersionMessage,
    pending_messages: VecDeque<NetworkMessage>,
}

impl<S> PeerSession<S> {
    fn new(transport: V1Transport<S>, remote_version: VersionMessage) -> Self {
        Self {
            transport,
            remote_version,
            pending_messages: VecDeque::new(),
        }
    }

    /// Returns the peer's negotiated `version` message.
    #[must_use]
    pub const fn remote_version(&self) -> &VersionMessage {
        &self.remote_version
    }

    /// Returns the underlying framed transport.
    #[must_use]
    pub fn into_transport(self) -> V1Transport<S> {
        self.transport
    }

    /// Requires a full-history peer capable of serving witness block payloads.
    pub fn ensure_full_witness_block_relay(&self) -> Result<(), P2pError> {
        let required = ServiceFlags::NETWORK | ServiceFlags::WITNESS;
        if !self.remote_version.services.has(required) {
            return Err(P2pError::MissingServices {
                required,
                offered: self.remote_version.services,
            });
        }
        Ok(())
    }
}

/// Async, bounded Bitcoin P2P v1 message transport.
pub struct V1Transport<S> {
    stream: S,
    magic: Magic,
    max_payload_len: u32,
}

impl<S> V1Transport<S> {
    /// Wraps an established stream with Bitcoin network and size constraints.
    #[must_use]
    pub const fn new(stream: S, magic: Magic) -> Self {
        Self {
            stream,
            magic,
            max_payload_len: MAX_PROTOCOL_MESSAGE_LEN,
        }
    }

    /// Changes the maximum accepted payload length for this connection.
    #[must_use]
    pub const fn with_max_payload_len(mut self, max_payload_len: u32) -> Self {
        self.max_payload_len = max_payload_len;
        self
    }

    /// Returns the wrapped stream after the peer session ends.
    #[must_use]
    pub fn into_inner(self) -> S {
        self.stream
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> V1Transport<S> {
    /// Reads one complete, checksum-validated Bitcoin v1 message.
    ///
    /// # Errors
    ///
    /// Returns I/O, framing/checksum, network-magic, or message-size errors.
    pub async fn read_message(&mut self) -> Result<RawNetworkMessage, P2pError> {
        let mut header = [0_u8; V1_HEADER_LEN];
        self.stream.read_exact(&mut header).await?;
        if header[..4] != self.magic.to_bytes() {
            return Err(P2pError::WrongMagic);
        }
        let payload_len = u32::from_le_bytes(header[16..20].try_into().expect("fixed header"));
        if payload_len > self.max_payload_len {
            return Err(P2pError::Oversize {
                length: payload_len,
                limit: self.max_payload_len,
            });
        }
        let payload_len = usize::try_from(payload_len).expect("u32 fits usize");
        let mut message = Vec::with_capacity(V1_HEADER_LEN + payload_len);
        message.extend_from_slice(&header);
        message.resize(V1_HEADER_LEN + payload_len, 0);
        self.stream
            .read_exact(&mut message[V1_HEADER_LEN..])
            .await?;
        Ok(decode_v1(&message)?)
    }

    /// Writes one complete protocol-compatible Bitcoin v1 envelope.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if the peer stream cannot accept the frame.
    pub async fn write_message(&mut self, payload: NetworkMessage) -> Result<(), P2pError> {
        let encoded = encode_v1(self.magic, payload);
        self.stream.write_all(&encoded).await?;
        self.stream.flush().await?;
        Ok(())
    }

    /// Performs the outbound `version`/`verack` handshake defined by the v1
    /// Bitcoin P2P protocol.
    ///
    /// The caller owns connection timeouts and the local `version` fields. The
    /// handshake is deliberately bounded so a peer cannot keep a session in
    /// its pre-authentication state by streaming unrelated messages. Pings
    /// received during negotiation are answered as required by the protocol.
    ///
    /// # Errors
    ///
    /// Returns an error for malformed transport frames, an invalid handshake
    /// ordering, or a peer that does not finish negotiation promptly.
    pub async fn handshake(&mut self, local: VersionMessage) -> Result<VersionMessage, P2pError> {
        let local_protocol_version = local.version;
        self.write_message(NetworkMessage::Version(local)).await?;

        let mut remote_version = None;
        let mut received_verack = false;
        for _ in 0..MAX_HANDSHAKE_MESSAGES {
            match self.read_message().await?.into_payload() {
                NetworkMessage::Version(version) => {
                    if version.version < MIN_PEER_PROTOCOL_VERSION {
                        return Err(P2pError::ObsoleteVersion {
                            actual: version.version,
                            minimum: MIN_PEER_PROTOCOL_VERSION,
                        });
                    }
                    validate_user_agent(&version.user_agent)?;
                    let common_version = local_protocol_version.min(version.version);
                    if remote_version.replace(version).is_some() {
                        return Err(P2pError::DuplicateVersion);
                    }
                    if common_version >= ADDRESS_RELAY_VERSION {
                        self.write_message(NetworkMessage::WtxidRelay).await?;
                        self.write_message(NetworkMessage::SendAddrV2).await?;
                    }
                    self.write_message(NetworkMessage::Verack).await?;
                }
                NetworkMessage::Verack => {
                    if remote_version.is_none() {
                        return Err(P2pError::VerackBeforeVersion);
                    }
                    received_verack = true;
                }
                NetworkMessage::Ping(nonce) => {
                    self.write_message(NetworkMessage::Pong(nonce)).await?;
                }
                _ => {}
            }

            if received_verack {
                if let Some(version) = remote_version {
                    return Ok(version);
                }
            }
        }
        Err(P2pError::HandshakeIncomplete)
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> PeerSession<S> {
    async fn read_wire_response_message(&mut self) -> Result<Option<NetworkMessage>, P2pError> {
        let message = self.transport.read_message().await?.into_payload();
        validate_post_handshake_message(&message)?;
        match message {
            NetworkMessage::Ping(nonce) => {
                self.transport
                    .write_message(NetworkMessage::Pong(nonce))
                    .await?;
                Ok(None)
            }
            NetworkMessage::Version(_) => Err(P2pError::PostHandshakeVersion),
            message => Ok(Some(message)),
        }
    }

    async fn read_bounded_response_message(&mut self) -> Result<Option<NetworkMessage>, P2pError> {
        if let Some(message) = self.pending_messages.pop_front() {
            return Ok(Some(message));
        }
        self.read_wire_response_message().await
    }

    /// Sends an application-level keepalive and waits for its matching pong.
    ///
    /// Stale pongs and unrelated messages consume the same fixed response
    /// budget used by header and block requests. Non-pong application messages
    /// are retained in wire order for the next response consumer. Concurrent
    /// inbound pings are answered without resetting that budget.
    pub async fn ping(&mut self, nonce: u64) -> Result<(), P2pError> {
        self.transport
            .write_message(NetworkMessage::Ping(nonce))
            .await?;
        for _ in 0..MAX_RESPONSE_MESSAGES {
            match self.read_wire_response_message().await? {
                Some(NetworkMessage::Pong(received)) if received == nonce => return Ok(()),
                Some(NetworkMessage::Pong(_)) | None => {}
                Some(message) => self.pending_messages.push_back(message),
            }
        }
        Err(P2pError::PongResponseIncomplete)
    }

    /// Requests header announcements instead of unsolicited block inventory.
    ///
    /// The node still validates and explicitly downloads every announced
    /// header/block; this preference only selects the lower-overhead
    /// headers-first announcement mode defined by BIP130.
    pub async fn prefer_headers_announcements(&mut self) -> Result<(), P2pError> {
        if self.remote_version.version < SENDHEADERS_VERSION {
            return Ok(());
        }
        self.transport
            .write_message(NetworkMessage::SendHeaders)
            .await
    }

    /// Sends one standard-weight non-coinbase transaction to the connected peer.
    ///
    /// A successful write only proves delivery to this peer's socket. It does
    /// not imply mempool acceptance or wider network propagation.
    pub async fn broadcast_transaction(
        &mut self,
        transaction: &Transaction,
    ) -> Result<(), P2pError> {
        if transaction.is_coinbase() {
            return Err(P2pError::OutboundCoinbaseTransaction);
        }
        let weight = transaction.weight().to_wu();
        let limit = u64::from(bitcoin::policy::MAX_STANDARD_TX_WEIGHT);
        if weight > limit {
            return Err(P2pError::OutboundTransactionTooHeavy { weight, limit });
        }
        self.transport
            .write_message(NetworkMessage::Tx(transaction.clone()))
            .await
    }

    /// Reads the next application message, answering P2P keepalive pings.
    ///
    /// Non-keepalive messages are returned in wire order. A caller that needs
    /// a specific response should use the corresponding request/response
    /// method so messages unrelated to its request are handled deliberately.
    pub async fn read_message(&mut self) -> Result<NetworkMessage, P2pError> {
        loop {
            if let Some(message) = self.read_bounded_response_message().await? {
                return Ok(message);
            }
        }
    }

    /// Sends a standard `getheaders` request using a newest-to-oldest locator.
    ///
    /// A zero `stop_hash` requests the protocol maximum of 2,000 headers.
    /// Callers must read the subsequent `headers` response from the session's
    /// transport and validate every returned header before issuing another
    /// request.
    pub async fn request_headers(
        &mut self,
        locator_hashes: Vec<BlockHash>,
        stop_hash: BlockHash,
    ) -> Result<(), P2pError> {
        if locator_hashes.len() > MAX_LOCATOR_HASHES {
            return Err(P2pError::TooManyLocatorHashes {
                count: locator_hashes.len(),
            });
        }
        let request = GetHeadersMessage {
            version: PROTOCOL_VERSION,
            locator_hashes,
            stop_hash,
        };
        self.transport
            .write_message(NetworkMessage::GetHeaders(request))
            .await
    }

    /// Requests a one-shot address sample from the connected peer.
    pub async fn request_addresses(&mut self) -> Result<(), P2pError> {
        self.transport.write_message(NetworkMessage::GetAddr).await
    }

    /// Receives one bounded legacy `addr` or BIP155 `addrv2` response.
    ///
    /// Unsupported address families and zero ports are ignored. Repeated IPv4
    /// or IPv6 socket addresses are returned once in their original order.
    pub async fn receive_addresses(&mut self) -> Result<Vec<PeerAddress>, P2pError> {
        for _ in 0..MAX_RESPONSE_MESSAGES {
            match self.read_bounded_response_message().await? {
                Some(NetworkMessage::Addr(addresses)) => {
                    if addresses.len() > MAX_ADDRESSES_PER_MESSAGE {
                        return Err(P2pError::TooManyAddresses {
                            count: addresses.len(),
                        });
                    }
                    return Ok(deduplicate_addresses(addresses.into_iter().filter_map(
                        |(last_seen, address)| {
                            let socket = address.socket_addr().ok()?;
                            (socket.port() != 0).then_some(PeerAddress {
                                socket,
                                services: address.services,
                                last_seen,
                            })
                        },
                    )));
                }
                Some(NetworkMessage::AddrV2(addresses)) => {
                    if addresses.len() > MAX_ADDRESSES_PER_MESSAGE {
                        return Err(P2pError::TooManyAddresses {
                            count: addresses.len(),
                        });
                    }
                    return Ok(deduplicate_addresses(addresses.into_iter().filter_map(
                        |address: AddrV2Message| {
                            let socket = address.socket_addr().ok()?;
                            (socket.port() != 0).then_some(PeerAddress {
                                socket,
                                services: address.services,
                                last_seen: address.time,
                            })
                        },
                    )));
                }
                _ => {}
            }
        }
        Err(P2pError::AddressResponseIncomplete)
    }

    /// Waits for a `headers` response, transparently answering keepalives.
    ///
    /// Peers commonly announce capabilities immediately after `verack`; those
    /// messages are safely skipped here because this method is used only by
    /// the sequential headers-first synchronizer. The result still requires
    /// contextual consensus validation before storage.
    pub async fn receive_headers(&mut self) -> Result<Vec<bitcoin::block::Header>, P2pError> {
        for _ in 0..MAX_RESPONSE_MESSAGES {
            if let Some(NetworkMessage::Headers(headers)) =
                self.read_bounded_response_message().await?
            {
                if headers.len() > MAX_HEADERS_PER_RESPONSE {
                    return Err(P2pError::TooManyHeaders {
                        count: headers.len(),
                    });
                }
                return Ok(headers);
            }
        }
        Err(P2pError::HeadersResponseIncomplete)
    }

    /// Requests witness-serialized blocks through standard `getdata` entries.
    pub async fn request_witness_blocks(&mut self, hashes: &[BlockHash]) -> Result<(), P2pError> {
        if hashes.len() > MAX_BLOCKS_IN_FLIGHT {
            return Err(P2pError::TooManyBlockRequests {
                count: hashes.len(),
            });
        }
        let mut unique = HashSet::with_capacity(hashes.len());
        for hash in hashes {
            if !unique.insert(*hash) {
                return Err(P2pError::DuplicateBlockRequest(*hash));
            }
        }
        let inventory = hashes
            .iter()
            .copied()
            .map(Inventory::WitnessBlock)
            .collect();
        self.transport
            .write_message(NetworkMessage::GetData(inventory))
            .await
    }

    /// Waits for a particular block requested with [`Self::request_witness_blocks`].
    ///
    /// The returned block has only passed wire checksum validation. Its header,
    /// merkle root, witness commitment, and every transaction still require
    /// contextual chainstate validation before it can be committed.
    pub async fn receive_requested_block(
        &mut self,
        expected: BlockHash,
    ) -> Result<bitcoin::Block, P2pError> {
        self.receive_requested_blocks(&[expected])
            .await?
            .pop()
            .ok_or(P2pError::BlockResponseIncomplete {
                requested: expected,
            })
    }

    /// Receives a bounded block batch and restores the caller's requested order.
    ///
    /// Peers may respond out of order, but may not inject or duplicate block
    /// payloads. The returned vector aligns exactly with `expected`.
    pub async fn receive_requested_blocks(
        &mut self,
        expected: &[BlockHash],
    ) -> Result<Vec<bitcoin::Block>, P2pError> {
        if expected.len() > MAX_BLOCKS_IN_FLIGHT {
            return Err(P2pError::TooManyBlockRequests {
                count: expected.len(),
            });
        }
        if expected.is_empty() {
            return Ok(Vec::new());
        }
        let mut positions = HashMap::with_capacity(expected.len());
        for (position, hash) in expected.iter().copied().enumerate() {
            if positions.insert(hash, position).is_some() {
                return Err(P2pError::DuplicateBlockRequest(hash));
            }
        }
        let mut blocks = (0..expected.len()).map(|_| None).collect::<Vec<_>>();
        for _ in 0..MAX_RESPONSE_MESSAGES {
            match self.read_bounded_response_message().await? {
                Some(NetworkMessage::Block(block)) => {
                    let actual = block.block_hash();
                    let Some(position) = positions.remove(&actual) else {
                        return Err(P2pError::UnsolicitedBlock(actual));
                    };
                    blocks[position] = Some(block);
                    if positions.is_empty() {
                        return Ok(blocks
                            .into_iter()
                            .map(|block| block.expect("every requested position was filled"))
                            .collect());
                    }
                }
                Some(NetworkMessage::NotFound(inventory)) => {
                    if let Some(hash) = inventory.iter().find_map(|entry| match entry {
                        Inventory::Block(hash) | Inventory::WitnessBlock(hash)
                            if positions.contains_key(hash) =>
                        {
                            Some(*hash)
                        }
                        _ => None,
                    }) {
                        return Err(P2pError::BlockNotFound(hash));
                    }
                }
                _ => {}
            }
        }
        Err(P2pError::BlockResponseIncomplete {
            requested: *positions
                .keys()
                .next()
                .expect("non-empty batch remains incomplete"),
        })
    }
}

/// Enforces per-command vector bounds after a v1 handshake.
///
/// This check applies before routing any application message, including
/// unrelated frames received while waiting for a header, block, address, or
/// pong response.
pub fn validate_post_handshake_message(message: &NetworkMessage) -> Result<(), P2pError> {
    match message {
        NetworkMessage::Inv(entries) => validate_inventory_len("inv", entries.len()),
        NetworkMessage::GetData(entries) => validate_inventory_len("getdata", entries.len()),
        NetworkMessage::NotFound(entries) => validate_inventory_len("notfound", entries.len()),
        NetworkMessage::GetBlocks(request) if request.locator_hashes.len() > MAX_LOCATOR_HASHES => {
            Err(P2pError::TooManyRemoteLocatorHashes {
                command: "getblocks",
                count: request.locator_hashes.len(),
            })
        }
        NetworkMessage::GetHeaders(request)
            if request.locator_hashes.len() > MAX_LOCATOR_HASHES =>
        {
            Err(P2pError::TooManyRemoteLocatorHashes {
                command: "getheaders",
                count: request.locator_hashes.len(),
            })
        }
        NetworkMessage::Headers(headers) if headers.len() > MAX_HEADERS_PER_RESPONSE => {
            Err(P2pError::TooManyHeaders {
                count: headers.len(),
            })
        }
        NetworkMessage::Addr(addresses) if addresses.len() > MAX_ADDRESSES_PER_MESSAGE => {
            Err(P2pError::TooManyAddresses {
                count: addresses.len(),
            })
        }
        NetworkMessage::AddrV2(addresses) if addresses.len() > MAX_ADDRESSES_PER_MESSAGE => {
            Err(P2pError::TooManyAddresses {
                count: addresses.len(),
            })
        }
        _ => Ok(()),
    }
}

fn validate_inventory_len(command: &'static str, count: usize) -> Result<(), P2pError> {
    if count > MAX_INVENTORY_ENTRIES {
        return Err(P2pError::TooManyInventoryEntries { command, count });
    }
    Ok(())
}

fn deduplicate_addresses(addresses: impl Iterator<Item = PeerAddress>) -> Vec<PeerAddress> {
    let mut sockets = HashSet::new();
    addresses
        .filter(|address| sockets.insert(address.socket))
        .collect()
}

/// Opens a TCP connection and completes an outbound Bitcoin v1 handshake.
///
/// The caller supplies a process-unique nonce (normally from a CSPRNG) to
/// detect accidental self-connections. rBTC advertises no serving capability
/// until block serving and persistent chainstate are available.
///
/// # Errors
///
/// Returns an error when TCP setup, framing, or the peer handshake fails.
pub async fn connect_outbound(
    remote: SocketAddr,
    magic: Magic,
    nonce: u64,
    user_agent: String,
    start_height: i32,
) -> Result<PeerSession<TcpStream>, P2pError> {
    validate_user_agent(&user_agent)?;
    let stream = TcpStream::connect(remote).await?;
    let local_address = stream.local_addr()?;
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let timestamp = i64::try_from(timestamp).unwrap_or(i64::MAX);
    let mut local_version = VersionMessage::new(
        ServiceFlags::NONE,
        timestamp,
        Address::new(&remote, ServiceFlags::NONE),
        Address::new(&local_address, ServiceFlags::NONE),
        nonce,
        user_agent,
        start_height,
    );
    local_version.version = PROTOCOL_VERSION;

    let mut transport = V1Transport::new(stream, magic);
    let remote_version = transport.handshake(local_version).await?;
    if remote_version.nonce == nonce {
        return Err(P2pError::SelfConnection);
    }
    Ok(PeerSession::new(transport, remote_version))
}

/// Builds a protocol-compatible v1 P2P envelope.
#[must_use]
pub fn encode_v1(magic: Magic, payload: NetworkMessage) -> Vec<u8> {
    serialize(&RawNetworkMessage::new(magic, payload))
}

/// Parses and checks the magic, command, length, and checksum of a v1 P2P envelope.
pub fn decode_v1(bytes: &[u8]) -> Result<RawNetworkMessage, EncodeError> {
    deserialize(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bitcoin::{
        Network,
        hashes::Hash,
        p2p::{
            Address, Magic, ServiceFlags,
            address::{AddrV2, AddrV2Message},
            message::NetworkMessage,
            message_network::VersionMessage,
        },
    };
    use proptest::prelude::*;
    use std::net::{Ipv4Addr, SocketAddr};
    use tokio::{
        io::{AsyncWriteExt, duplex},
        net::TcpListener,
    };

    fn version(nonce: u64) -> VersionMessage {
        let receiver: SocketAddr = "127.0.0.1:18444".parse().unwrap();
        let sender: SocketAddr = "0.0.0.0:0".parse().unwrap();
        VersionMessage::new(
            ServiceFlags::NONE,
            0,
            Address::new(&receiver, ServiceFlags::NONE),
            Address::new(&sender, ServiceFlags::NONE),
            nonce,
            "/rbtcd:test/".to_owned(),
            0,
        )
    }

    #[test]
    fn v1_verack_roundtrip() {
        let message = encode_v1(Magic::BITCOIN, NetworkMessage::Verack);
        let decoded = decode_v1(&message).unwrap();
        assert_eq!(decoded.magic(), &Magic::BITCOIN);
        assert!(matches!(decoded.into_payload(), NetworkMessage::Verack));
    }

    #[test]
    fn only_objective_remote_wire_failures_are_protocol_violations() {
        let hash = BlockHash::all_zeros();
        for error in [
            P2pError::WrongMagic,
            P2pError::Oversize {
                length: MAX_PROTOCOL_MESSAGE_LEN + 1,
                limit: MAX_PROTOCOL_MESSAGE_LEN,
            },
            P2pError::DuplicateVersion,
            P2pError::PostHandshakeVersion,
            P2pError::VerackBeforeVersion,
            P2pError::TooManyHeaders {
                count: MAX_HEADERS_PER_RESPONSE + 1,
            },
            P2pError::UnsolicitedBlock(hash),
            P2pError::TooManyAddresses {
                count: MAX_ADDRESSES_PER_MESSAGE + 1,
            },
            P2pError::TooManyInventoryEntries {
                command: "inv",
                count: MAX_INVENTORY_ENTRIES + 1,
            },
            P2pError::TooManyRemoteLocatorHashes {
                command: "getheaders",
                count: MAX_LOCATOR_HASHES + 1,
            },
        ] {
            assert!(error.is_protocol_violation(), "{error}");
        }

        for error in [
            P2pError::HandshakeIncomplete,
            P2pError::HeadersResponseIncomplete,
            P2pError::BlockNotFound(hash),
            P2pError::ObsoleteVersion {
                actual: MIN_PEER_PROTOCOL_VERSION - 1,
                minimum: MIN_PEER_PROTOCOL_VERSION,
            },
            P2pError::SelfConnection,
            P2pError::MissingServices {
                required: ServiceFlags::NETWORK | ServiceFlags::WITNESS,
                offered: ServiceFlags::NETWORK,
            },
            P2pError::TooManyBlockRequests {
                count: MAX_BLOCKS_IN_FLIGHT + 1,
            },
            P2pError::PongResponseIncomplete,
            P2pError::OutboundCoinbaseTransaction,
            P2pError::OutboundTransactionTooHeavy {
                weight: u64::from(bitcoin::policy::MAX_STANDARD_TX_WEIGHT) + 1,
                limit: u64::from(bitcoin::policy::MAX_STANDARD_TX_WEIGHT),
            },
        ] {
            assert!(!error.is_protocol_violation(), "{error}");
        }
    }

    proptest! {
        #[test]
        fn v1_ping_envelope_roundtrips_every_nonce(nonce in any::<u64>()) {
            let message = encode_v1(Magic::BITCOIN, NetworkMessage::Ping(nonce));
            let decoded = decode_v1(&message).unwrap();
            prop_assert_eq!(decoded.magic(), &Magic::BITCOIN);
            prop_assert!(matches!(decoded.into_payload(), NetworkMessage::Ping(value) if value == nonce));
        }

        #[test]
        fn arbitrary_bounded_v1_input_never_panics(
            bytes in proptest::collection::vec(any::<u8>(), 0..=4096)
        ) {
            let _ = decode_v1(&bytes);
        }

        #[test]
        fn any_ping_payload_corruption_fails_checksum(
            nonce in any::<u64>(), payload_byte in 0_usize..8
        ) {
            let mut message = encode_v1(Magic::BITCOIN, NetworkMessage::Ping(nonce));
            message[V1_HEADER_LEN + payload_byte] ^= 1;
            prop_assert!(decode_v1(&message).is_err());
        }
    }

    #[tokio::test]
    async fn transport_rejects_announced_oversize_before_reading_payload() {
        let (mut writer, reader) = duplex(128);
        let mut header = encode_v1(Magic::BITCOIN, NetworkMessage::Verack);
        header[16..20].copy_from_slice(&(1025_u32).to_le_bytes());
        writer.write_all(&header[..V1_HEADER_LEN]).await.unwrap();
        drop(writer);

        let mut transport = V1Transport::new(reader, Magic::BITCOIN).with_max_payload_len(1024);
        assert!(matches!(
            transport.read_message().await,
            Err(P2pError::Oversize {
                length: 1025,
                limit: 1024
            })
        ));
    }

    #[tokio::test]
    async fn default_transport_matches_core_26_message_limit() {
        let (mut writer, reader) = duplex(128);
        let mut header = encode_v1(Magic::BITCOIN, NetworkMessage::Verack);
        header[16..20].copy_from_slice(&(MAX_PROTOCOL_MESSAGE_LEN + 1).to_le_bytes());
        writer.write_all(&header[..V1_HEADER_LEN]).await.unwrap();
        drop(writer);

        let mut transport = V1Transport::new(reader, Magic::BITCOIN);
        assert!(matches!(
            transport.read_message().await,
            Err(P2pError::Oversize {
                length,
                limit: MAX_PROTOCOL_MESSAGE_LEN
            }) if length == MAX_PROTOCOL_MESSAGE_LEN + 1
        ));
    }

    #[tokio::test]
    async fn transport_rejects_truncated_payload() {
        let (mut writer, reader) = duplex(128);
        let message = encode_v1(Magic::BITCOIN, NetworkMessage::Ping(42));
        writer
            .write_all(&message[..V1_HEADER_LEN + 3])
            .await
            .unwrap();
        drop(writer);

        let mut transport = V1Transport::new(reader, Magic::BITCOIN);
        assert!(matches!(
            transport.read_message().await,
            Err(P2pError::Io(_))
        ));
    }

    #[tokio::test]
    async fn transport_roundtrips_and_rejects_wrong_network() {
        let (left, right) = duplex(1024);
        let mut writer = V1Transport::new(left, Magic::BITCOIN);
        let mut reader = V1Transport::new(right, Magic::BITCOIN);
        writer.write_message(NetworkMessage::Verack).await.unwrap();
        assert!(matches!(
            reader.read_message().await.unwrap().into_payload(),
            NetworkMessage::Verack
        ));

        let (left, right) = duplex(1024);
        let mut writer = V1Transport::new(left, Magic::TESTNET3);
        let mut reader = V1Transport::new(right, Magic::BITCOIN);
        writer.write_message(NetworkMessage::Verack).await.unwrap();
        assert!(matches!(
            reader.read_message().await,
            Err(P2pError::WrongMagic)
        ));
    }

    #[tokio::test]
    async fn outbound_handshake_is_protocol_compatible() {
        let (client_stream, server_stream) = duplex(4096);
        let client = tokio::spawn(async move {
            let mut client = V1Transport::new(client_stream, Network::Regtest.magic());
            client.handshake(version(1)).await.unwrap()
        });
        let server = tokio::spawn(async move {
            let mut server = V1Transport::new(server_stream, Network::Regtest.magic());
            assert!(matches!(
                server.read_message().await.unwrap().into_payload(),
                NetworkMessage::Version(_)
            ));
            server
                .write_message(NetworkMessage::Version(version(2)))
                .await
                .unwrap();
            assert!(matches!(
                server.read_message().await.unwrap().into_payload(),
                NetworkMessage::Verack
            ));
            server.write_message(NetworkMessage::Verack).await.unwrap();
        });

        assert_eq!(client.await.unwrap().nonce, 2);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn modern_handshake_negotiates_wtxid_and_addrv2_before_verack() {
        let (client_stream, server_stream) = duplex(4096);
        let client = tokio::spawn(async move {
            let mut local = version(1);
            local.version = ADDRESS_RELAY_VERSION;
            let mut client = V1Transport::new(client_stream, Network::Regtest.magic());
            client.handshake(local).await.unwrap()
        });
        let server = tokio::spawn(async move {
            let mut server = V1Transport::new(server_stream, Network::Regtest.magic());
            server.read_message().await.unwrap();
            let mut remote = version(2);
            remote.version = ADDRESS_RELAY_VERSION;
            server
                .write_message(NetworkMessage::Version(remote))
                .await
                .unwrap();
            assert!(matches!(
                server.read_message().await.unwrap().into_payload(),
                NetworkMessage::WtxidRelay
            ));
            assert!(matches!(
                server.read_message().await.unwrap().into_payload(),
                NetworkMessage::SendAddrV2
            ));
            assert!(matches!(
                server.read_message().await.unwrap().into_payload(),
                NetworkMessage::Verack
            ));
            server.write_message(NetworkMessage::Verack).await.unwrap();
        });

        assert_eq!(client.await.unwrap().nonce, 2);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn addrv2_request_filters_unsupported_zero_port_and_duplicates() {
        let (client_stream, server_stream) = duplex(16 * 1024);
        let expected: SocketAddr = "1.2.3.4:8333".parse().unwrap();
        let client = tokio::spawn(async move {
            let mut session = PeerSession::new(
                V1Transport::new(client_stream, Network::Regtest.magic()),
                version(1),
            );
            session.request_addresses().await.unwrap();
            session.receive_addresses().await.unwrap()
        });
        let server = tokio::spawn(async move {
            let mut transport = V1Transport::new(server_stream, Network::Regtest.magic());
            assert!(matches!(
                transport.read_message().await.unwrap().into_payload(),
                NetworkMessage::GetAddr
            ));
            let full_services = ServiceFlags::NETWORK | ServiceFlags::WITNESS;
            transport
                .write_message(NetworkMessage::AddrV2(vec![
                    AddrV2Message {
                        time: 100,
                        services: full_services,
                        addr: AddrV2::Ipv4(Ipv4Addr::new(1, 2, 3, 4)),
                        port: 8333,
                    },
                    AddrV2Message {
                        time: 101,
                        services: full_services,
                        addr: AddrV2::Ipv4(Ipv4Addr::new(1, 2, 3, 4)),
                        port: 8333,
                    },
                    AddrV2Message {
                        time: 102,
                        services: full_services,
                        addr: AddrV2::Ipv4(Ipv4Addr::LOCALHOST),
                        port: 0,
                    },
                    AddrV2Message {
                        time: 103,
                        services: full_services,
                        addr: AddrV2::TorV3([7; 32]),
                        port: 8333,
                    },
                ]))
                .await
                .unwrap();
        });
        assert_eq!(
            client.await.unwrap(),
            vec![PeerAddress {
                socket: expected,
                services: ServiceFlags::NETWORK | ServiceFlags::WITNESS,
                last_seen: 100,
            }]
        );
        server.await.unwrap();
    }

    #[tokio::test]
    async fn legacy_address_response_converts_ipv4_and_ipv6() {
        let (client_stream, server_stream) = duplex(16 * 1024);
        let ipv4: SocketAddr = "8.8.8.8:8333".parse().unwrap();
        let ipv6: SocketAddr = "[2001:4860:4860::8888]:8333".parse().unwrap();
        let client = tokio::spawn(async move {
            let mut session = PeerSession::new(
                V1Transport::new(client_stream, Network::Regtest.magic()),
                version(1),
            );
            session.receive_addresses().await.unwrap()
        });
        let server = tokio::spawn(async move {
            let mut transport = V1Transport::new(server_stream, Network::Regtest.magic());
            transport
                .write_message(NetworkMessage::Addr(vec![
                    (200, Address::new(&ipv4, ServiceFlags::NETWORK)),
                    (201, Address::new(&ipv6, ServiceFlags::NETWORK_LIMITED)),
                ]))
                .await
                .unwrap();
        });
        assert_eq!(
            client.await.unwrap(),
            vec![
                PeerAddress {
                    socket: ipv4,
                    services: ServiceFlags::NETWORK,
                    last_seen: 200,
                },
                PeerAddress {
                    socket: ipv6,
                    services: ServiceFlags::NETWORK_LIMITED,
                    last_seen: 201,
                },
            ]
        );
        server.await.unwrap();
    }

    #[tokio::test]
    async fn address_response_rejects_more_than_core_limit() {
        let (client_stream, server_stream) = duplex(128 * 1024);
        let client = tokio::spawn(async move {
            let mut session = PeerSession::new(
                V1Transport::new(client_stream, Network::Regtest.magic()),
                version(1),
            );
            session.receive_addresses().await
        });
        let server = tokio::spawn(async move {
            let mut transport = V1Transport::new(server_stream, Network::Regtest.magic());
            let address: SocketAddr = "1.2.3.4:8333".parse().unwrap();
            transport
                .write_message(NetworkMessage::Addr(vec![
                    (
                        100,
                        Address::new(&address, ServiceFlags::NETWORK)
                    );
                    MAX_ADDRESSES_PER_MESSAGE + 1
                ]))
                .await
                .unwrap();
        });
        assert!(matches!(
            client.await.unwrap(),
            Err(P2pError::TooManyAddresses { count })
                if count == MAX_ADDRESSES_PER_MESSAGE + 1
        ));
        server.await.unwrap();
    }

    #[test]
    fn full_block_ibd_requires_network_and_witness_services() {
        let (stream, _) = duplex(64);
        let session = PeerSession::new(
            V1Transport::new(stream, Network::Regtest.magic()),
            version(1),
        );
        assert!(matches!(
            session.ensure_full_witness_block_relay(),
            Err(P2pError::MissingServices { .. })
        ));

        let (stream, _) = duplex(64);
        let mut remote = version(2);
        remote.services = ServiceFlags::NETWORK | ServiceFlags::WITNESS;
        let session = PeerSession::new(V1Transport::new(stream, Network::Regtest.magic()), remote);
        session.ensure_full_witness_block_relay().unwrap();
    }

    #[tokio::test]
    async fn block_batch_restores_request_order_from_out_of_order_peer() {
        let (client_stream, server_stream) = duplex(16 * 1024);
        let first = bitcoin::blockdata::constants::genesis_block(Network::Regtest);
        let second = bitcoin::blockdata::constants::genesis_block(Network::Bitcoin);
        let expected = [first.block_hash(), second.block_hash()];
        let client = tokio::spawn(async move {
            let mut session = PeerSession::new(
                V1Transport::new(client_stream, Network::Regtest.magic()),
                version(1),
            );
            session.request_witness_blocks(&expected).await.unwrap();
            session.receive_requested_blocks(&expected).await.unwrap()
        });
        let server = tokio::spawn(async move {
            let mut transport = V1Transport::new(server_stream, Network::Regtest.magic());
            assert!(matches!(
                transport.read_message().await.unwrap().into_payload(),
                NetworkMessage::GetData(inventory) if inventory.len() == 2
            ));
            transport
                .write_message(NetworkMessage::Block(second))
                .await
                .unwrap();
            transport
                .write_message(NetworkMessage::Block(first))
                .await
                .unwrap();
        });
        let blocks = client.await.unwrap();
        assert_eq!(blocks[0].block_hash(), expected[0]);
        assert_eq!(blocks[1].block_hash(), expected[1]);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn block_request_rejects_oversize_and_duplicates_before_writing() {
        let (client_stream, _) = duplex(64);
        let mut session = PeerSession::new(
            V1Transport::new(client_stream, Network::Regtest.magic()),
            version(1),
        );
        let hash = bitcoin::blockdata::constants::genesis_block(Network::Regtest).block_hash();
        assert!(matches!(
            session
                .request_witness_blocks(&[hash; MAX_BLOCKS_IN_FLIGHT + 1])
                .await,
            Err(P2pError::TooManyBlockRequests { .. })
        ));
        assert!(matches!(
            session.request_witness_blocks(&[hash, hash]).await,
            Err(P2pError::DuplicateBlockRequest(duplicate)) if duplicate == hash
        ));
    }

    #[tokio::test]
    async fn block_batch_rejects_unsolicited_and_notfound_responses() {
        let expected = bitcoin::blockdata::constants::genesis_block(Network::Regtest);
        let unsolicited = bitcoin::blockdata::constants::genesis_block(Network::Bitcoin);
        let expected_hash = expected.block_hash();
        let unsolicited_hash = unsolicited.block_hash();
        let (client_stream, server_stream) = duplex(16 * 1024);
        let client = tokio::spawn(async move {
            let mut session = PeerSession::new(
                V1Transport::new(client_stream, Network::Regtest.magic()),
                version(1),
            );
            session.receive_requested_blocks(&[expected_hash]).await
        });
        let server = tokio::spawn(async move {
            let mut transport = V1Transport::new(server_stream, Network::Regtest.magic());
            transport
                .write_message(NetworkMessage::Block(unsolicited))
                .await
                .unwrap();
        });
        assert!(matches!(
            client.await.unwrap(),
            Err(P2pError::UnsolicitedBlock(actual)) if actual == unsolicited_hash
        ));
        server.await.unwrap();

        let (client_stream, server_stream) = duplex(4096);
        let client = tokio::spawn(async move {
            let mut session = PeerSession::new(
                V1Transport::new(client_stream, Network::Regtest.magic()),
                version(1),
            );
            session.receive_requested_blocks(&[expected_hash]).await
        });
        let server = tokio::spawn(async move {
            let mut transport = V1Transport::new(server_stream, Network::Regtest.magic());
            transport
                .write_message(NetworkMessage::NotFound(vec![Inventory::WitnessBlock(
                    expected_hash,
                )]))
                .await
                .unwrap();
        });
        assert!(matches!(
            client.await.unwrap(),
            Err(P2pError::BlockNotFound(actual)) if actual == expected_hash
        ));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn header_response_rejects_more_than_protocol_maximum() {
        let (client_stream, server_stream) = duplex(512 * 1024);
        let client = tokio::spawn(async move {
            let mut session = PeerSession::new(
                V1Transport::new(client_stream, Network::Regtest.magic()),
                version(1),
            );
            session.receive_headers().await
        });
        let server = tokio::spawn(async move {
            let mut transport = V1Transport::new(server_stream, Network::Regtest.magic());
            let header = bitcoin::blockdata::constants::genesis_block(Network::Regtest).header;
            transport
                .write_message(NetworkMessage::Headers(vec![
                    header;
                    MAX_HEADERS_PER_RESPONSE + 1
                ]))
                .await
                .unwrap();
        });
        assert!(matches!(
            client.await.unwrap(),
            Err(P2pError::TooManyHeaders { count }) if count == MAX_HEADERS_PER_RESPONSE + 1
        ));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn keepalive_pings_consume_the_bounded_response_budget() {
        let (client_stream, server_stream) = duplex(16 * 1024);
        let client = tokio::spawn(async move {
            let mut session = PeerSession::new(
                V1Transport::new(client_stream, Network::Regtest.magic()),
                version(1),
            );
            session.receive_headers().await
        });
        let server = tokio::spawn(async move {
            let mut transport = V1Transport::new(server_stream, Network::Regtest.magic());
            for nonce in 0..u64::try_from(MAX_RESPONSE_MESSAGES).unwrap() {
                transport
                    .write_message(NetworkMessage::Ping(nonce))
                    .await
                    .unwrap();
                assert!(matches!(
                    transport.read_message().await.unwrap().into_payload(),
                    NetworkMessage::Pong(received) if received == nonce
                ));
            }
        });

        assert!(matches!(
            client.await.unwrap(),
            Err(P2pError::HeadersResponseIncomplete)
        ));
        server.await.unwrap();
    }

    #[test]
    fn all_post_handshake_vector_commands_enforce_protocol_limits() {
        let hash = BlockHash::all_zeros();
        for (message, command) in [
            (
                NetworkMessage::Inv(vec![Inventory::Block(hash); MAX_INVENTORY_ENTRIES + 1]),
                "inv",
            ),
            (
                NetworkMessage::GetData(vec![Inventory::Block(hash); MAX_INVENTORY_ENTRIES + 1]),
                "getdata",
            ),
            (
                NetworkMessage::NotFound(vec![Inventory::Block(hash); MAX_INVENTORY_ENTRIES + 1]),
                "notfound",
            ),
        ] {
            assert!(matches!(
                validate_post_handshake_message(&message),
                Err(P2pError::TooManyInventoryEntries {
                    command: actual,
                    count
                }) if actual == command && count == MAX_INVENTORY_ENTRIES + 1
            ));
        }

        for message in [
            NetworkMessage::GetHeaders(GetHeadersMessage {
                version: PROTOCOL_VERSION,
                locator_hashes: vec![hash; MAX_LOCATOR_HASHES + 1],
                stop_hash: hash,
            }),
            NetworkMessage::GetBlocks(bitcoin::p2p::message_blockdata::GetBlocksMessage::new(
                vec![hash; MAX_LOCATOR_HASHES + 1],
                hash,
            )),
        ] {
            assert!(matches!(
                validate_post_handshake_message(&message),
                Err(P2pError::TooManyRemoteLocatorHashes { count, .. })
                    if count == MAX_LOCATOR_HASHES + 1
            ));
        }
    }

    #[tokio::test]
    async fn active_ping_matches_nonce_and_answers_crossed_keepalive() {
        let (client_stream, server_stream) = duplex(16 * 1024);
        let client = tokio::spawn(async move {
            let mut session = PeerSession::new(
                V1Transport::new(client_stream, Network::Regtest.magic()),
                version(1),
            );
            session.ping(42).await
        });
        let server = tokio::spawn(async move {
            let mut transport = V1Transport::new(server_stream, Network::Regtest.magic());
            assert!(matches!(
                transport.read_message().await.unwrap().into_payload(),
                NetworkMessage::Ping(42)
            ));
            transport
                .write_message(NetworkMessage::Pong(41))
                .await
                .unwrap();
            transport
                .write_message(NetworkMessage::Ping(7))
                .await
                .unwrap();
            assert!(matches!(
                transport.read_message().await.unwrap().into_payload(),
                NetworkMessage::Pong(7)
            ));
            transport
                .write_message(NetworkMessage::Pong(42))
                .await
                .unwrap();
        });

        client.await.unwrap().unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn active_ping_preserves_a_headers_announcement() {
        let expected = bitcoin::blockdata::constants::genesis_block(Network::Regtest).header;
        let (client_stream, server_stream) = duplex(16 * 1024);
        let client = tokio::spawn(async move {
            let mut session = PeerSession::new(
                V1Transport::new(client_stream, Network::Regtest.magic()),
                version(1),
            );
            session.ping(42).await.unwrap();
            session.receive_headers().await.unwrap()
        });
        let server = tokio::spawn(async move {
            let mut transport = V1Transport::new(server_stream, Network::Regtest.magic());
            assert!(matches!(
                transport.read_message().await.unwrap().into_payload(),
                NetworkMessage::Ping(42)
            ));
            transport
                .write_message(NetworkMessage::Headers(vec![expected]))
                .await
                .unwrap();
            transport
                .write_message(NetworkMessage::Pong(42))
                .await
                .unwrap();
        });

        assert_eq!(client.await.unwrap(), vec![expected]);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn active_ping_has_a_total_frame_budget() {
        let (client_stream, server_stream) = duplex(16 * 1024);
        let client = tokio::spawn(async move {
            let mut session = PeerSession::new(
                V1Transport::new(client_stream, Network::Regtest.magic()),
                version(1),
            );
            session.ping(42).await
        });
        let server = tokio::spawn(async move {
            let mut transport = V1Transport::new(server_stream, Network::Regtest.magic());
            assert!(matches!(
                transport.read_message().await.unwrap().into_payload(),
                NetworkMessage::Ping(42)
            ));
            for nonce in 0..u64::try_from(MAX_RESPONSE_MESSAGES).unwrap() {
                transport
                    .write_message(NetworkMessage::Pong(nonce))
                    .await
                    .unwrap();
            }
        });

        assert!(matches!(
            client.await.unwrap(),
            Err(P2pError::PongResponseIncomplete)
        ));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn headers_first_announcement_preference_uses_bip130_message() {
        let (client_stream, server_stream) = duplex(1024);
        let client = tokio::spawn(async move {
            let mut remote = version(1);
            remote.version = SENDHEADERS_VERSION;
            let mut session = PeerSession::new(
                V1Transport::new(client_stream, Network::Regtest.magic()),
                remote,
            );
            session.prefer_headers_announcements().await
        });
        let server = tokio::spawn(async move {
            let mut transport = V1Transport::new(server_stream, Network::Regtest.magic());
            assert!(matches!(
                transport.read_message().await.unwrap().into_payload(),
                NetworkMessage::SendHeaders
            ));
        });

        client.await.unwrap().unwrap();
        server.await.unwrap();

        let (client_stream, mut server_stream) = duplex(1024);
        let mut remote = version(1);
        remote.version = SENDHEADERS_VERSION - 1;
        let mut session = PeerSession::new(
            V1Transport::new(client_stream, Network::Regtest.magic()),
            remote,
        );
        session.prefer_headers_announcements().await.unwrap();
        assert!(
            tokio::time::timeout(
                std::time::Duration::from_millis(10),
                server_stream.read_u8()
            )
            .await
            .is_err()
        );
    }

    #[tokio::test]
    async fn transaction_broadcast_is_bounded_and_uses_tx_message() {
        let coinbase =
            bitcoin::blockdata::constants::genesis_block(Network::Regtest).txdata[0].clone();
        let mut transaction = coinbase.clone();
        transaction.input[0].previous_output.vout = 0;

        let (client_stream, server_stream) = duplex(1024 * 1024);
        let expected = transaction.clone();
        let client = tokio::spawn(async move {
            let mut session = PeerSession::new(
                V1Transport::new(client_stream, Network::Regtest.magic()),
                version(1),
            );
            session.broadcast_transaction(&transaction).await
        });
        let server = tokio::spawn(async move {
            let mut transport = V1Transport::new(server_stream, Network::Regtest.magic());
            assert_eq!(
                transport.read_message().await.unwrap().into_payload(),
                NetworkMessage::Tx(expected)
            );
        });
        client.await.unwrap().unwrap();
        server.await.unwrap();

        let (client_stream, mut server_stream) = duplex(64);
        let mut session = PeerSession::new(
            V1Transport::new(client_stream, Network::Regtest.magic()),
            version(1),
        );
        assert!(matches!(
            session.broadcast_transaction(&coinbase).await,
            Err(P2pError::OutboundCoinbaseTransaction)
        ));

        let mut oversized = coinbase;
        oversized.input[0].previous_output.vout = 0;
        oversized.input[0].script_sig = bitcoin::ScriptBuf::from_bytes(vec![0; 100_001]);
        assert!(matches!(
            session.broadcast_transaction(&oversized).await,
            Err(P2pError::OutboundTransactionTooHeavy { .. })
        ));
        assert!(
            tokio::time::timeout(
                std::time::Duration::from_millis(10),
                server_stream.read_u8()
            )
            .await
            .is_err()
        );
    }

    #[tokio::test]
    async fn block_wait_uses_the_same_total_frame_budget() {
        let expected = bitcoin::blockdata::constants::genesis_block(Network::Regtest).block_hash();
        let (client_stream, server_stream) = duplex(16 * 1024);
        let client = tokio::spawn(async move {
            let mut session = PeerSession::new(
                V1Transport::new(client_stream, Network::Regtest.magic()),
                version(1),
            );
            session.receive_requested_block(expected).await
        });
        let server = tokio::spawn(async move {
            let mut transport = V1Transport::new(server_stream, Network::Regtest.magic());
            for nonce in 0..u64::try_from(MAX_RESPONSE_MESSAGES).unwrap() {
                transport
                    .write_message(NetworkMessage::Ping(nonce))
                    .await
                    .unwrap();
                assert!(matches!(
                    transport.read_message().await.unwrap().into_payload(),
                    NetworkMessage::Pong(received) if received == nonce
                ));
            }
        });

        assert!(matches!(
            client.await.unwrap(),
            Err(P2pError::BlockResponseIncomplete { requested }) if requested == expected
        ));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn response_rejects_version_after_handshake() {
        let (client_stream, server_stream) = duplex(4096);
        let client = tokio::spawn(async move {
            let mut session = PeerSession::new(
                V1Transport::new(client_stream, Network::Regtest.magic()),
                version(1),
            );
            session.receive_headers().await
        });
        let server = tokio::spawn(async move {
            let mut transport = V1Transport::new(server_stream, Network::Regtest.magic());
            transport
                .write_message(NetworkMessage::Version(version(2)))
                .await
                .unwrap();
        });

        assert!(matches!(
            client.await.unwrap(),
            Err(P2pError::PostHandshakeVersion)
        ));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn handshake_rejects_obsolete_protocol_versions() {
        let (client_stream, server_stream) = duplex(4096);
        let client = tokio::spawn(async move {
            let mut client = V1Transport::new(client_stream, Network::Regtest.magic());
            client.handshake(version(1)).await
        });
        let server = tokio::spawn(async move {
            let mut server = V1Transport::new(server_stream, Network::Regtest.magic());
            server.read_message().await.unwrap();
            let mut obsolete = version(2);
            obsolete.version = MIN_PEER_PROTOCOL_VERSION - 1;
            server
                .write_message(NetworkMessage::Version(obsolete))
                .await
                .unwrap();
        });
        assert!(matches!(
            client.await.unwrap(),
            Err(P2pError::ObsoleteVersion { .. })
        ));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn handshake_rejects_oversized_remote_and_local_user_agents() {
        validate_user_agent(&"x".repeat(MAX_USER_AGENT_LEN)).unwrap();

        let (client_stream, server_stream) = duplex(4096);
        let client = tokio::spawn(async move {
            let mut client = V1Transport::new(client_stream, Network::Regtest.magic());
            client.handshake(version(1)).await
        });
        let server = tokio::spawn(async move {
            let mut server = V1Transport::new(server_stream, Network::Regtest.magic());
            server.read_message().await.unwrap();
            let mut oversized = version(2);
            oversized.user_agent = "x".repeat(MAX_USER_AGENT_LEN + 1);
            server
                .write_message(NetworkMessage::Version(oversized))
                .await
                .unwrap();
        });
        assert!(matches!(
            client.await.unwrap(),
            Err(P2pError::OversizeUserAgent {
                length,
                limit: MAX_USER_AGENT_LEN
            }) if length == MAX_USER_AGENT_LEN + 1
        ));
        server.await.unwrap();

        let result = connect_outbound(
            "127.0.0.1:1".parse().unwrap(),
            Network::Regtest.magic(),
            3,
            "x".repeat(MAX_USER_AGENT_LEN + 1),
            0,
        )
        .await;
        assert!(matches!(
            result,
            Err(P2pError::OversizeUserAgent {
                length,
                limit: MAX_USER_AGENT_LEN
            }) if length == MAX_USER_AGENT_LEN + 1
        ));
    }

    #[tokio::test]
    async fn getheaders_rejects_oversized_locator_before_writing() {
        let (client_stream, _) = duplex(64);
        let mut session = PeerSession::new(
            V1Transport::new(client_stream, Network::Regtest.magic()),
            version(1),
        );
        let result = session
            .request_headers(
                vec![BlockHash::all_zeros(); MAX_LOCATOR_HASHES + 1],
                BlockHash::all_zeros(),
            )
            .await;
        assert!(matches!(
            result,
            Err(P2pError::TooManyLocatorHashes { count }) if count == MAX_LOCATOR_HASHES + 1
        ));
    }

    #[tokio::test]
    async fn tcp_session_handshakes_then_requests_headers() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let remote = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let block = bitcoin::blockdata::constants::genesis_block(Network::Regtest);
            let block_hash = block.block_hash();
            let (stream, _) = listener.accept().await.unwrap();
            let mut server = V1Transport::new(stream, Network::Regtest.magic());
            assert!(matches!(
                server.read_message().await.unwrap().into_payload(),
                NetworkMessage::Version(_)
            ));
            server
                .write_message(NetworkMessage::Version(version(4)))
                .await
                .unwrap();
            assert!(matches!(
                server.read_message().await.unwrap().into_payload(),
                NetworkMessage::Verack
            ));
            server.write_message(NetworkMessage::Verack).await.unwrap();
            match server.read_message().await.unwrap().into_payload() {
                NetworkMessage::GetHeaders(request) => {
                    assert_eq!(request.version, PROTOCOL_VERSION);
                    assert_eq!(request.locator_hashes, vec![BlockHash::all_zeros()]);
                    assert_eq!(request.stop_hash, BlockHash::all_zeros());
                    server
                        .write_message(NetworkMessage::SendHeaders)
                        .await
                        .unwrap();
                    server
                        .write_message(NetworkMessage::Ping(42))
                        .await
                        .unwrap();
                    assert!(matches!(
                        server.read_message().await.unwrap().into_payload(),
                        NetworkMessage::Pong(42)
                    ));
                    server
                        .write_message(NetworkMessage::Headers(Vec::new()))
                        .await
                        .unwrap();
                }
                message => panic!("expected getheaders, got {message:?}"),
            }
            match server.read_message().await.unwrap().into_payload() {
                NetworkMessage::GetData(inventory) => {
                    assert_eq!(inventory, vec![Inventory::WitnessBlock(block_hash)]);
                }
                message => panic!("expected getdata, got {message:?}"),
            }
            server
                .write_message(NetworkMessage::Block(block))
                .await
                .unwrap();
        });

        let mut client = connect_outbound(
            remote,
            Network::Regtest.magic(),
            3,
            "/rbtcd:test/".to_owned(),
            0,
        )
        .await
        .unwrap();
        assert_eq!(client.remote_version().nonce, 4);
        client
            .request_headers(vec![BlockHash::all_zeros()], BlockHash::all_zeros())
            .await
            .unwrap();
        assert!(client.receive_headers().await.unwrap().is_empty());
        let block_hash =
            bitcoin::blockdata::constants::genesis_block(Network::Regtest).block_hash();
        client.request_witness_blocks(&[block_hash]).await.unwrap();
        assert_eq!(
            client
                .receive_requested_block(block_hash)
                .await
                .unwrap()
                .block_hash(),
            block_hash
        );
        server.await.unwrap();
    }
}
