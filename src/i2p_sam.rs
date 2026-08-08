//! I2P SAM v3 client for reaching and publishing Bitcoin peers.
//!
//! This module speaks the subset of the SAM v3 bridge protocol a node needs:
//! `HELLO` version negotiation, `SESSION CREATE` for one long-lived STREAM
//! session whose destination key can be persisted and replayed, and
//! `STREAM CONNECT` on a separate socket per outbound peer. Replies are read
//! line-bounded and count-bounded, so a hostile or malfunctioning bridge
//! cannot make the node allocate without limit.
//!
//! Addresses follow Bitcoin's BIP155 I2P form: the 32-byte SHA-256 of the
//! peer's destination, rendered as a 52-character base32 `.b32.i2p` name.
//! I2P has no port concept for these peers, so the advertised port is fixed
//! at zero and the name alone identifies the destination.
//!
//! The SAM bridge is a fully privileged local interface — it can open
//! arbitrary streams on this node's behalf — so the connection is refused
//! unless it is loopback.

use std::io;
use std::net::SocketAddr;
use std::time::Duration;

use thiserror::Error;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

use crate::p2p::{decode_base32, encode_base32};

/// Bound on one SAM reply line.
const MAX_REPLY_LINE_BYTES: u64 = 8 * 1024;
/// Length of the base32 body of a `.b32.i2p` name.
const B32_BODY_LEN: usize = 52;
/// Length of the decoded destination hash.
const DESTINATION_HASH_LEN: usize = 32;
/// Bound on a session identifier supplied by this node.
const MAX_SESSION_ID_LEN: usize = 32;
/// Bound on a destination key retained from the bridge.
const MAX_DESTINATION_KEY_LEN: usize = 8 * 1024;

/// I2P SAM failures.
#[derive(Debug, Error)]
pub enum I2pSamError {
    /// The bridge connection failed.
    #[error("i2p sam io: {0}")]
    Io(#[from] std::io::Error),
    /// A non-loopback bridge address was requested.
    #[error("the I2P SAM bridge must be reached over loopback")]
    NonLoopbackBridge,
    /// The bridge does not support a SAM version this client speaks.
    #[error("the I2P SAM bridge does not support SAM 3.1 or later")]
    UnsupportedVersion,
    /// A reply exceeded the accepted bounds.
    #[error("an I2P SAM reply exceeded the accepted bounds")]
    OversizedReply,
    /// The bridge answered with a failure result.
    #[error("i2p sam command failed: {result} {detail}")]
    CommandFailed {
        /// The bridge's `RESULT` value.
        result: String,
        /// Bounded reply detail.
        detail: String,
    },
    /// A reply was missing a field this client requires.
    #[error("an I2P SAM reply was malformed")]
    MalformedReply,
    /// A destination name failed structural validation.
    #[error("the I2P address is not a valid .b32.i2p destination")]
    InvalidAddress,
    /// A session identifier or destination key exceeded its bound.
    #[error("an I2P SAM parameter exceeded its accepted bound")]
    OversizedParameter,
}

/// A validated BIP155 I2P destination.
///
/// The address is the 32-byte SHA-256 of the peer's I2P destination in the
/// 52-character base32 `.b32.i2p` form. Validation is structural: the name's
/// length, alphabet, and decoded size must all match, so a truncated or
/// mistyped name can never reach a bridge or a store.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct I2pAddress {
    name: String,
}

impl I2pAddress {
    /// Validates a `<52-char-base32>.b32.i2p` destination name.
    ///
    /// # Errors
    ///
    /// Returns an error when the suffix, length, alphabet, or decoded size
    /// does not match the BIP155 I2P form.
    pub fn new(name: &str) -> Result<Self, I2pSamError> {
        let name = name.to_ascii_lowercase();
        let Some(body) = name.strip_suffix(".b32.i2p") else {
            return Err(I2pSamError::InvalidAddress);
        };
        if body.len() != B32_BODY_LEN {
            return Err(I2pSamError::InvalidAddress);
        }
        let decoded = decode_base32(body).ok_or(I2pSamError::InvalidAddress)?;
        if decoded.len() != DESTINATION_HASH_LEN {
            return Err(I2pSamError::InvalidAddress);
        }
        Ok(Self { name })
    }

    /// Builds an address from a BIP155 destination hash.
    #[must_use]
    pub fn from_destination_hash(hash: [u8; DESTINATION_HASH_LEN]) -> Self {
        Self {
            name: format!("{}.b32.i2p", encode_base32(&hash)),
        }
    }

    /// Returns the validated `.b32.i2p` name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the 32-byte destination hash carried in BIP155 `addrv2`.
    #[must_use]
    pub fn destination_hash(&self) -> [u8; DESTINATION_HASH_LEN] {
        decode_base32(
            self.name
                .strip_suffix(".b32.i2p")
                .expect("a validated name keeps its suffix"),
        )
        .expect("a validated name decodes")
        .try_into()
        .expect("a validated name carries 32 hash bytes")
    }
}

impl std::fmt::Display for I2pAddress {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.name)
    }
}

/// Bounds for one SAM session.
#[derive(Clone, Copy, Debug)]
pub struct I2pSamConfig {
    /// Deadline for connecting and for each command exchange.
    pub timeout: Duration,
}

impl Default for I2pSamConfig {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(30),
        }
    }
}

/// One established SAM STREAM session.
///
/// The session lives as long as its control connection: dropping it releases
/// the destination at the bridge. Outbound peers are dialled on separate
/// sockets that reference the session by identifier.
pub struct I2pSamSession {
    bridge: SocketAddr,
    session_id: String,
    destination_key: String,
    address: I2pAddress,
    config: I2pSamConfig,
    _control: TcpStream,
}

impl I2pSamSession {
    /// Creates a STREAM session on a loopback SAM bridge.
    ///
    /// `destination_key` replays a previously stored key so the node keeps
    /// its published address across restarts; `None` asks the bridge for a
    /// transient destination. The key is secret material: callers must store
    /// it owner-only and never log it.
    ///
    /// # Errors
    ///
    /// Returns an error for a non-loopback bridge, an unsupported SAM
    /// version, a refused session, or any I/O or bound violation.
    pub async fn create(
        bridge: SocketAddr,
        session_id: &str,
        destination_key: Option<&str>,
        config: I2pSamConfig,
    ) -> Result<Self, I2pSamError> {
        if !bridge.ip().is_loopback() {
            return Err(I2pSamError::NonLoopbackBridge);
        }
        if session_id.is_empty()
            || session_id.len() > MAX_SESSION_ID_LEN
            || !session_id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
        {
            return Err(I2pSamError::OversizedParameter);
        }
        if destination_key.is_some_and(|key| key.len() > MAX_DESTINATION_KEY_LEN) {
            return Err(I2pSamError::OversizedParameter);
        }
        let mut control = connect_and_greet(bridge, config).await?;
        let destination = destination_key.unwrap_or("TRANSIENT");
        let reply = command(
            &mut control,
            &format!(
                "SESSION CREATE STYLE=STREAM ID={session_id} DESTINATION={destination} SIGNATURE_TYPE=7"
            ),
            config,
        )
        .await?;
        let destination_key = field(&reply, "DESTINATION=")
            .ok_or(I2pSamError::MalformedReply)?
            .to_owned();
        if destination_key.len() > MAX_DESTINATION_KEY_LEN {
            return Err(I2pSamError::OversizedParameter);
        }
        let address = I2pAddress::from_destination_hash(destination_hash(&destination_key)?);
        Ok(Self {
            bridge,
            session_id: session_id.to_owned(),
            destination_key,
            address,
            config,
            _control: control,
        })
    }

    /// Returns this node's published I2P address.
    #[must_use]
    pub fn address(&self) -> &I2pAddress {
        &self.address
    }

    /// Returns the destination key to persist for address reuse.
    ///
    /// This is secret material: store it owner-only and never log it.
    #[must_use]
    pub fn destination_key(&self) -> &str {
        &self.destination_key
    }

    /// Opens one outbound stream to an I2P peer.
    ///
    /// The returned socket carries ordinary Bitcoin P2P bytes, so the caller
    /// drives the same v1 or BIP324 handshake it would on a TCP peer.
    ///
    /// # Errors
    ///
    /// Returns an error when the bridge refuses the stream or any bound is
    /// violated.
    pub async fn connect_stream(&self, destination: &I2pAddress) -> Result<TcpStream, I2pSamError> {
        let mut stream = connect_and_greet(self.bridge, self.config).await?;
        command(
            &mut stream,
            &format!(
                "STREAM CONNECT ID={} DESTINATION={} SILENT=false",
                self.session_id,
                destination.name()
            ),
            self.config,
        )
        .await?;
        Ok(stream)
    }
}

/// Connects to the bridge and completes SAM version negotiation.
async fn connect_and_greet(
    bridge: SocketAddr,
    config: I2pSamConfig,
) -> Result<TcpStream, I2pSamError> {
    let mut stream = tokio::time::timeout(config.timeout, TcpStream::connect(bridge))
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "sam connect timed out"))??;
    let reply = command(&mut stream, "HELLO VERSION MIN=3.1 MAX=3.3", config).await?;
    let version = field(&reply, "VERSION=").ok_or(I2pSamError::MalformedReply)?;
    if !version.starts_with("3.") {
        return Err(I2pSamError::UnsupportedVersion);
    }
    Ok(stream)
}

/// Sends one command and returns its single bounded reply line.
async fn command(
    stream: &mut TcpStream,
    request: &str,
    config: I2pSamConfig,
) -> Result<String, I2pSamError> {
    tokio::time::timeout(config.timeout, async {
        stream.write_all(request.as_bytes()).await?;
        stream.write_all(b"\n").await?;
        stream.flush().await?;
        let mut reader = BufReader::new(&mut *stream);
        let mut line = String::new();
        let read = tokio::io::AsyncReadExt::take(&mut reader, MAX_REPLY_LINE_BYTES)
            .read_line(&mut line)
            .await?;
        if read == 0 {
            return Err(I2pSamError::MalformedReply);
        }
        if u64::try_from(read).unwrap_or(u64::MAX) >= MAX_REPLY_LINE_BYTES {
            return Err(I2pSamError::OversizedReply);
        }
        let line = line.trim_end_matches(['\r', '\n']).to_owned();
        let result = field(&line, "RESULT=").ok_or(I2pSamError::MalformedReply)?;
        if result != "OK" {
            return Err(I2pSamError::CommandFailed {
                result: result.to_owned(),
                detail: field(&line, "MESSAGE=").unwrap_or_default().to_owned(),
            });
        }
        Ok(line)
    })
    .await
    .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "sam command timed out"))?
}

/// Computes the BIP155 destination hash of a SAM destination key.
fn destination_hash(destination_key: &str) -> Result<[u8; DESTINATION_HASH_LEN], I2pSamError> {
    use sha2::{Digest, Sha256};
    // SAM renders destinations in I2P's base64 alphabet, which substitutes
    // `-` and `~` for `+` and `/`.
    let decoded = decode_i2p_base64(destination_key).ok_or(I2pSamError::MalformedReply)?;
    Ok(Sha256::digest(&decoded).into())
}

/// Decodes I2P's base64 alphabet without padding requirements.
fn decode_i2p_base64(input: &str) -> Option<Vec<u8>> {
    let mut output = Vec::with_capacity(input.len() * 3 / 4);
    let mut accumulator: u32 = 0;
    let mut bits = 0_u32;
    for byte in input.bytes() {
        let value = match byte {
            b'A'..=b'Z' => byte - b'A',
            b'a'..=b'z' => byte - b'a' + 26,
            b'0'..=b'9' => byte - b'0' + 52,
            b'-' => 62,
            b'~' => 63,
            b'=' => continue,
            _ => return None,
        };
        accumulator = (accumulator << 6) | u32::from(value);
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            output.push(u8::try_from((accumulator >> bits) & 0xff).expect("masked to one byte"));
        }
    }
    Some(output)
}

/// Returns the value following `name`.
///
/// A quoted value ends at its closing quote and may contain spaces,
/// which SAM's `MESSAGE=` field routinely does; an unquoted value ends
/// at the next space.
fn field<'line>(line: &'line str, name: &str) -> Option<&'line str> {
    let start = line.find(name)? + name.len();
    let rest = &line[start..];
    if let Some(quoted) = rest.strip_prefix('"') {
        let end = quoted.find('"').unwrap_or(quoted.len());
        return Some(&quoted[..end]);
    }
    let end = rest.find([' ', '\r']).unwrap_or(rest.len());
    Some(&rest[..end])
}

#[cfg(test)]
mod tests {
    use super::*;
    use sha2::Digest;
    use std::sync::{Arc, Mutex};
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::net::TcpListener;

    /// A destination key in I2P's base64 alphabet.
    const TEST_DESTINATION: &str = "abcdEFGH1234-~abcdEFGH1234-~abcdEFGH1234-~";

    /// A SAM bridge answering the subset this client speaks.
    ///
    /// Connections are served concurrently because a session holds its
    /// control socket open while every outbound peer opens another one.
    fn serve_sam_bridge(listener: TcpListener, version: &'static str) -> Arc<Mutex<Vec<String>>> {
        let commands = Arc::new(Mutex::new(Vec::new()));
        let recorded = Arc::clone(&commands);
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    return;
                };
                let recorded = Arc::clone(&recorded);
                tokio::spawn(async move {
                    let (reader, mut writer) = stream.into_split();
                    let mut reader = BufReader::new(reader);
                    loop {
                        let mut line = String::new();
                        if reader.read_line(&mut line).await.unwrap_or(0) == 0 {
                            return;
                        }
                        let line = line.trim_end().to_owned();
                        recorded
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .push(line.clone());
                        let reply = if line.starts_with("HELLO") {
                            format!("HELLO REPLY RESULT=OK VERSION={version}\n")
                        } else if line.starts_with("SESSION CREATE") {
                            format!("SESSION STATUS RESULT=OK DESTINATION={TEST_DESTINATION}\n")
                        } else if line.starts_with("STREAM CONNECT") {
                            "STREAM STATUS RESULT=OK\n".to_owned()
                        } else {
                            "RESULT=I2P_ERROR MESSAGE=\"unsupported command\"\n".to_owned()
                        };
                        if writer.write_all(reply.as_bytes()).await.is_err() {
                            return;
                        }
                    }
                });
            }
        });
        commands
    }

    #[test]
    fn i2p_addresses_round_trip_and_reject_malformed_names() {
        let hash = [0x5a_u8; DESTINATION_HASH_LEN];
        let address = I2pAddress::from_destination_hash(hash);
        assert_eq!(address.destination_hash(), hash);
        assert!(address.name().ends_with(".b32.i2p"));
        assert_eq!(address.name().len(), B32_BODY_LEN + ".b32.i2p".len());
        assert_eq!(I2pAddress::new(address.name()).unwrap(), address);
        assert_eq!(
            I2pAddress::new(&address.name().to_ascii_uppercase()).unwrap(),
            address,
            "names are case-normalized"
        );
        assert_eq!(address.to_string(), address.name());

        for invalid in [
            "short.b32.i2p",
            "abc.onion",
            &format!("{}.b32.i2p", "1".repeat(B32_BODY_LEN)),
            &format!("{}.b32.i2p", "a".repeat(B32_BODY_LEN + 1)),
        ] {
            assert!(
                matches!(I2pAddress::new(invalid), Err(I2pSamError::InvalidAddress)),
                "{invalid} must be refused"
            );
        }
    }

    #[tokio::test]
    async fn creates_a_stream_session_and_dials_a_peer() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let bridge = listener.local_addr().unwrap();
        let commands = serve_sam_bridge(listener, "3.3");

        let session = I2pSamSession::create(bridge, "rbtc-test", None, I2pSamConfig::default())
            .await
            .expect("the session is created");
        assert_eq!(session.destination_key(), TEST_DESTINATION);
        let expected = I2pAddress::from_destination_hash(<[u8; 32]>::from(sha2::Sha256::digest(
            decode_i2p_base64(TEST_DESTINATION).unwrap(),
        )));
        assert_eq!(session.address(), &expected);

        let peer = I2pAddress::from_destination_hash([0x11; DESTINATION_HASH_LEN]);
        let stream = session
            .connect_stream(&peer)
            .await
            .expect("the bridge opens the stream");
        drop(stream);

        let commands = commands
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        assert!(commands[0].starts_with("HELLO VERSION MIN=3.1"));
        assert!(
            commands[1].contains("SESSION CREATE STYLE=STREAM ID=rbtc-test DESTINATION=TRANSIENT"),
            "{:?}",
            commands[1]
        );
        assert!(
            commands.iter().any(|command| command
                == &format!(
                    "STREAM CONNECT ID=rbtc-test DESTINATION={} SILENT=false",
                    peer.name()
                )),
            "{commands:?}"
        );
    }

    #[tokio::test]
    async fn refuses_non_loopback_bridges_and_malformed_parameters() {
        assert!(matches!(
            I2pSamSession::create(
                "203.0.113.7:7656".parse().unwrap(),
                "rbtc",
                None,
                I2pSamConfig::default()
            )
            .await,
            Err(I2pSamError::NonLoopbackBridge)
        ));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let bridge = listener.local_addr().unwrap();
        for invalid in ["", "has space", &"x".repeat(MAX_SESSION_ID_LEN + 1)] {
            assert!(
                matches!(
                    I2pSamSession::create(bridge, invalid, None, I2pSamConfig::default()).await,
                    Err(I2pSamError::OversizedParameter)
                ),
                "session id {invalid:?} must be refused before dialling"
            );
        }
        assert!(matches!(
            I2pSamSession::create(
                bridge,
                "rbtc",
                Some(&"k".repeat(MAX_DESTINATION_KEY_LEN + 1)),
                I2pSamConfig::default()
            )
            .await,
            Err(I2pSamError::OversizedParameter)
        ));
    }

    #[tokio::test]
    async fn refuses_a_bridge_speaking_an_unsupported_version() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let bridge = listener.local_addr().unwrap();
        let _commands = serve_sam_bridge(listener, "2.0");
        assert!(matches!(
            I2pSamSession::create(bridge, "rbtc", None, I2pSamConfig::default()).await,
            Err(I2pSamError::UnsupportedVersion)
        ));
    }

    #[test]
    fn parses_reply_fields_and_i2p_base64() {
        assert_eq!(field("RESULT=OK VERSION=3.3", "VERSION="), Some("3.3"));
        assert_eq!(field("RESULT=OK MESSAGE=\"a b\"", "MESSAGE="), Some("a b"));
        assert_eq!(field("RESULT=OK", "VERSION="), None);
        assert_eq!(decode_i2p_base64("-~"), Some(vec![0xfb]));
        assert_eq!(decode_i2p_base64("!!"), None);
        assert!(decode_i2p_base64("AAAA").is_some());
    }
}
