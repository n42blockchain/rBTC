//! Bitcoin P2P v1 framing delegated to rust-bitcoin.
//!
//! This module deliberately does not implement an ad-hoc wire format. The
//! payload and envelope are exactly rust-bitcoin's consensus serialization of
//! Bitcoin's `RawNetworkMessage`. BIP324 v2 transport is tracked as a gated
//! follow-up because it changes the encrypted session handshake, not messages.

use bitcoin::{
    BlockHash,
    consensus::{deserialize, encode::Error as EncodeError, serialize},
    p2p::{
        Address, Magic, ServiceFlags,
        message::{NetworkMessage, RawNetworkMessage},
        message_blockdata::{GetHeadersMessage, Inventory},
        message_network::VersionMessage,
    },
};
use std::{
    collections::{HashMap, HashSet},
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
    /// A peer acknowledged the connection before identifying itself.
    #[error("verack received before version during handshake")]
    VerackBeforeVersion,
    /// The peer did not complete the handshake within the bounded message budget.
    #[error("peer did not complete handshake within {MAX_HANDSHAKE_MESSAGES} messages")]
    HandshakeIncomplete,
    /// A peer did not provide the requested response within the bounded message budget.
    #[error("peer did not provide headers within {MAX_RESPONSE_MESSAGES} messages")]
    HeadersResponseIncomplete,
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
}

/// An established Bitcoin peer session.
///
/// The session owns its transport, so messages left after handshake remain in
/// order for header and block synchronisation.
pub struct PeerSession<S> {
    transport: V1Transport<S>,
    remote_version: VersionMessage,
}

impl<S> PeerSession<S> {
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
                    if remote_version.replace(version).is_some() {
                        return Err(P2pError::DuplicateVersion);
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
    /// Reads the next application message, answering P2P keepalive pings.
    ///
    /// Non-keepalive messages are returned in wire order. A caller that needs
    /// a specific response should use the corresponding request/response
    /// method so messages unrelated to its request are handled deliberately.
    pub async fn read_message(&mut self) -> Result<NetworkMessage, P2pError> {
        loop {
            match self.transport.read_message().await?.into_payload() {
                NetworkMessage::Ping(nonce) => {
                    self.transport
                        .write_message(NetworkMessage::Pong(nonce))
                        .await?;
                }
                message => return Ok(message),
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

    /// Waits for a `headers` response, transparently answering keepalives.
    ///
    /// Peers commonly announce capabilities immediately after `verack`; those
    /// messages are safely skipped here because this method is used only by
    /// the sequential headers-first synchronizer. The result still requires
    /// contextual consensus validation before storage.
    pub async fn receive_headers(&mut self) -> Result<Vec<bitcoin::block::Header>, P2pError> {
        for _ in 0..MAX_RESPONSE_MESSAGES {
            if let NetworkMessage::Headers(headers) = self.read_message().await? {
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
        for _ in 0..MAX_RESPONSE_MESSAGES.saturating_add(expected.len()) {
            match self.read_message().await? {
                NetworkMessage::Block(block) => {
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
                NetworkMessage::NotFound(inventory) => {
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
    Ok(PeerSession {
        transport,
        remote_version,
    })
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
            Address, Magic, ServiceFlags, message::NetworkMessage, message_network::VersionMessage,
        },
    };
    use proptest::prelude::*;
    use std::net::SocketAddr;
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

    #[test]
    fn full_block_ibd_requires_network_and_witness_services() {
        let (stream, _) = duplex(64);
        let session = PeerSession {
            transport: V1Transport::new(stream, Network::Regtest.magic()),
            remote_version: version(1),
        };
        assert!(matches!(
            session.ensure_full_witness_block_relay(),
            Err(P2pError::MissingServices { .. })
        ));

        let (stream, _) = duplex(64);
        let mut remote = version(2);
        remote.services = ServiceFlags::NETWORK | ServiceFlags::WITNESS;
        let session = PeerSession {
            transport: V1Transport::new(stream, Network::Regtest.magic()),
            remote_version: remote,
        };
        session.ensure_full_witness_block_relay().unwrap();
    }

    #[tokio::test]
    async fn block_batch_restores_request_order_from_out_of_order_peer() {
        let (client_stream, server_stream) = duplex(16 * 1024);
        let first = bitcoin::blockdata::constants::genesis_block(Network::Regtest);
        let second = bitcoin::blockdata::constants::genesis_block(Network::Bitcoin);
        let expected = [first.block_hash(), second.block_hash()];
        let client = tokio::spawn(async move {
            let mut session = PeerSession {
                transport: V1Transport::new(client_stream, Network::Regtest.magic()),
                remote_version: version(1),
            };
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
        let mut session = PeerSession {
            transport: V1Transport::new(client_stream, Network::Regtest.magic()),
            remote_version: version(1),
        };
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
            let mut session = PeerSession {
                transport: V1Transport::new(client_stream, Network::Regtest.magic()),
                remote_version: version(1),
            };
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
            let mut session = PeerSession {
                transport: V1Transport::new(client_stream, Network::Regtest.magic()),
                remote_version: version(1),
            };
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
            let mut session = PeerSession {
                transport: V1Transport::new(client_stream, Network::Regtest.magic()),
                remote_version: version(1),
            };
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
        let mut session = PeerSession {
            transport: V1Transport::new(client_stream, Network::Regtest.magic()),
            remote_version: version(1),
        };
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
