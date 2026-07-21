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
    net::SocketAddr,
    time::{SystemTime, UNIX_EPOCH},
};
use thiserror::Error;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::TcpStream,
};

/// Maximum v1 P2P message size accepted before allocation (32 MiB).
pub const MAX_PROTOCOL_MESSAGE_LEN: u32 = 32 * 1024 * 1024;
const V1_HEADER_LEN: usize = 24;
const MAX_HANDSHAKE_MESSAGES: usize = 8;
const PROTOCOL_VERSION: u32 = 70_016;

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
        let request = GetHeadersMessage {
            version: PROTOCOL_VERSION,
            locator_hashes,
            stop_hash,
        };
        self.transport
            .write_message(NetworkMessage::GetHeaders(request))
            .await
    }

    /// Requests witness-serialized blocks through standard `getdata` entries.
    pub async fn request_witness_blocks(&mut self, hashes: &[BlockHash]) -> Result<(), P2pError> {
        let inventory = hashes
            .iter()
            .copied()
            .map(Inventory::WitnessBlock)
            .collect();
        self.transport
            .write_message(NetworkMessage::GetData(inventory))
            .await
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
    use std::net::SocketAddr;
    use tokio::{io::duplex, net::TcpListener};

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
    async fn tcp_session_handshakes_then_requests_headers() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let remote = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
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
                }
                message => panic!("expected getheaders, got {message:?}"),
            }
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
        server.await.unwrap();
    }
}
