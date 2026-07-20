//! Bitcoin P2P v1 framing delegated to rust-bitcoin.
//!
//! This module deliberately does not implement an ad-hoc wire format. The
//! payload and envelope are exactly rust-bitcoin's consensus serialization of
//! Bitcoin's `RawNetworkMessage`. BIP324 v2 transport is tracked as a gated
//! follow-up because it changes the encrypted session handshake, not messages.

use bitcoin::{
    consensus::{deserialize, encode::Error as EncodeError, serialize},
    p2p::{
        Magic,
        message::{NetworkMessage, RawNetworkMessage},
    },
};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// Maximum v1 P2P message size accepted before allocation (32 MiB).
pub const MAX_PROTOCOL_MESSAGE_LEN: u32 = 32 * 1024 * 1024;
const V1_HEADER_LEN: usize = 24;

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
    use bitcoin::p2p::{Magic, message::NetworkMessage};
    use tokio::io::duplex;

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
}
