//! Tor control-port client for ephemeral onion services.
//!
//! This module speaks the narrow subset of Tor's control protocol needed to
//! publish and withdraw one ephemeral v3 onion service: `PROTOCOLINFO`
//! capability discovery, `SAFECOOKIE` challenge-response authentication
//! (never plaintext cookie disclosure), `ADD_ONION` with a fresh ED25519-v3
//! key, and `DEL_ONION` on shutdown. Every reply is read line-bounded and
//! count-bounded, so a hostile or malfunctioning control port cannot make the
//! node allocate without limit. The control port is a fully privileged local
//! interface: reaching it is equivalent to controlling this host's Tor
//! instance, so the connection is refused unless it is loopback and the
//! cookie file is read only for the challenge computation.

use std::io;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};

use crate::p2p::OnionAddress;

/// Bound on one control-protocol reply line.
const MAX_REPLY_LINE_BYTES: u64 = 4 * 1024;
/// Bound on continuation lines accepted for one reply.
const MAX_REPLY_LINES: usize = 64;
/// Exact length of Tor's control authentication cookie.
const COOKIE_LEN: usize = 32;
/// Length of the client and server SAFECOOKIE nonces.
const NONCE_LEN: usize = 32;
/// Length of a SAFECOOKIE HMAC.
const HMAC_LEN: usize = 32;
/// HMAC key for the server's authentication proof.
const SERVER_HMAC_KEY: &[u8] = b"Tor safe cookie authentication server-to-controller hash";
/// HMAC key for the controller's authentication proof.
const CONTROLLER_HMAC_KEY: &[u8] = b"Tor safe cookie authentication controller-to-server hash";

/// Tor control-port failures.
#[derive(Debug, Error)]
pub enum TorControlError {
    /// The control connection failed.
    #[error("tor control io: {0}")]
    Io(#[from] std::io::Error),
    /// A non-loopback control address was requested.
    #[error("the Tor control port must be reached over loopback")]
    NonLoopbackControlPort,
    /// The control port did not offer SAFECOOKIE authentication.
    #[error("the Tor control port does not offer SAFECOOKIE authentication")]
    UnsupportedAuthentication,
    /// The authentication cookie file was missing or the wrong size.
    #[error("the Tor authentication cookie file is unreadable or not 32 bytes")]
    UnreadableCookie,
    /// The control port failed to prove it holds the same cookie.
    #[error("the Tor control port failed its SAFECOOKIE proof")]
    ServerProofMismatch,
    /// A reply exceeded the accepted line or continuation bounds.
    #[error("a Tor control reply exceeded the accepted bounds")]
    OversizedReply,
    /// The control port answered with a failure status.
    #[error("tor control command failed: {status} {detail}")]
    CommandFailed {
        /// Three-digit reply status.
        status: String,
        /// Bounded first reply line.
        detail: String,
    },
    /// A reply was missing a field this client requires.
    #[error("a Tor control reply was malformed")]
    MalformedReply,
    /// The published service identifier was not a valid v3 onion address.
    #[error("the published onion service identifier is not a valid v3 address")]
    InvalidServiceId,
}

/// Bounds for one Tor control session.
#[derive(Clone, Copy, Debug)]
pub struct TorControlConfig {
    /// Deadline for connecting, authenticating, and each command.
    pub timeout: Duration,
}

impl Default for TorControlConfig {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(10),
        }
    }
}

/// One authenticated Tor control connection.
pub struct TorController {
    reader: BufReader<OwnedReadHalf>,
    writer: OwnedWriteHalf,
    timeout: Duration,
}

/// A published ephemeral onion service.
///
/// The service exists only while Tor keeps this control connection, and
/// [`TorController::remove_onion_service`] withdraws it explicitly.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PublishedOnionService {
    /// The published v3 address, including the advertised port.
    pub address: OnionAddress,
    /// Tor's service identifier without the `.onion` suffix.
    pub service_id: String,
    /// The private key Tor generated, in Tor's `TYPE:BLOB` form.
    ///
    /// Retaining it lets a later launch republish the same address. It is
    /// secret material: it must never be logged or written outside an
    /// owner-only file.
    pub private_key: String,
}

impl TorController {
    /// Connects to a loopback control port and authenticates with SAFECOOKIE.
    ///
    /// # Errors
    ///
    /// Returns an error for a non-loopback address, an unreadable cookie
    /// file, a control port that does not offer SAFECOOKIE, a failed server
    /// proof, or any I/O or bound violation.
    pub async fn connect(
        control: SocketAddr,
        cookie_path: &Path,
        config: TorControlConfig,
    ) -> Result<Self, TorControlError> {
        if !control.ip().is_loopback() {
            return Err(TorControlError::NonLoopbackControlPort);
        }
        let stream = tokio::time::timeout(config.timeout, TcpStream::connect(control))
            .await
            .map_err(|_| {
                io::Error::new(io::ErrorKind::TimedOut, "tor control connect timed out")
            })??;
        let (reader, writer) = stream.into_split();
        let mut controller = Self {
            reader: BufReader::new(reader),
            writer,
            timeout: config.timeout,
        };
        controller.authenticate(cookie_path).await?;
        Ok(controller)
    }

    async fn authenticate(&mut self, cookie_path: &Path) -> Result<(), TorControlError> {
        let protocol_info = self.command("PROTOCOLINFO 1").await?;
        if !protocol_info
            .iter()
            .any(|line| line.contains("AUTH") && line.contains("SAFECOOKIE"))
        {
            return Err(TorControlError::UnsupportedAuthentication);
        }
        let cookie = read_cookie(cookie_path)?;
        let client_nonce: [u8; NONCE_LEN] = rand::random();
        let challenge = self
            .command(&format!(
                "AUTHCHALLENGE SAFECOOKIE {}",
                hex_upper(&client_nonce)
            ))
            .await?;
        let reply = challenge.first().ok_or(TorControlError::MalformedReply)?;
        let server_hash =
            decode_hex(field(reply, "SERVERHASH=").ok_or(TorControlError::MalformedReply)?)
                .ok_or(TorControlError::MalformedReply)?;
        let server_nonce =
            decode_hex(field(reply, "SERVERNONCE=").ok_or(TorControlError::MalformedReply)?)
                .ok_or(TorControlError::MalformedReply)?;
        if server_hash.len() != HMAC_LEN || server_nonce.len() != NONCE_LEN {
            return Err(TorControlError::MalformedReply);
        }
        let mut transcript = Vec::with_capacity(COOKIE_LEN + NONCE_LEN * 2);
        transcript.extend_from_slice(&cookie);
        transcript.extend_from_slice(&client_nonce);
        transcript.extend_from_slice(&server_nonce);
        // The server proof is checked before disclosing the controller proof,
        // so a control port that does not already hold the cookie learns
        // nothing usable from this exchange.
        if hmac_sha256(SERVER_HMAC_KEY, &transcript) != server_hash.as_slice() {
            return Err(TorControlError::ServerProofMismatch);
        }
        let controller_proof = hmac_sha256(CONTROLLER_HMAC_KEY, &transcript);
        self.command(&format!("AUTHENTICATE {}", hex_upper(&controller_proof)))
            .await?;
        Ok(())
    }

    /// Publishes one ephemeral v3 onion service forwarding to a local port.
    ///
    /// `private_key` republishes a previously generated service; `None`
    /// requests a fresh ED25519-v3 key. The service is discarded when the
    /// control connection closes.
    ///
    /// # Errors
    ///
    /// Returns an error when Tor refuses the command or answers without a
    /// usable service identifier.
    pub async fn add_onion_service(
        &mut self,
        virtual_port: u16,
        forward_to: SocketAddr,
        private_key: Option<&str>,
    ) -> Result<PublishedOnionService, TorControlError> {
        // One command, and never `Flags=DiscardPK`: Tor rejects that flag
        // with a non-`NEW` key type, and with a new key it would discard the
        // very key this node must persist to keep its address across
        // restarts.
        let key = private_key.unwrap_or("NEW:ED25519-V3");
        let reply = self
            .command(&format!("ADD_ONION {key} Port={virtual_port},{forward_to}"))
            .await?;
        let service_id = reply
            .iter()
            .find_map(|line| field(line, "ServiceID="))
            .ok_or(TorControlError::MalformedReply)?
            .to_owned();
        // Tor returns `PrivateKey=` only when it generated one; a replayed
        // key is carried through so the published service always reports the
        // key that reproduces it.
        let private_key = reply
            .iter()
            .find_map(|line| field(line, "PrivateKey="))
            .map(ToOwned::to_owned)
            .or_else(|| private_key.map(ToOwned::to_owned))
            .unwrap_or_default();
        let address = OnionAddress::new(&format!("{service_id}.onion"), virtual_port)
            .map_err(|_| TorControlError::InvalidServiceId)?;
        Ok(PublishedOnionService {
            address,
            service_id,
            private_key,
        })
    }

    /// Withdraws one published onion service.
    ///
    /// # Errors
    ///
    /// Returns an error when Tor refuses the command.
    pub async fn remove_onion_service(
        &mut self,
        service: &PublishedOnionService,
    ) -> Result<(), TorControlError> {
        self.command(&format!("DEL_ONION {}", service.service_id))
            .await
            .map(|_| ())
    }

    /// Sends one command and returns its bounded reply lines without status
    /// codes.
    async fn command(&mut self, command: &str) -> Result<Vec<String>, TorControlError> {
        tokio::time::timeout(self.timeout, async {
            self.writer.write_all(command.as_bytes()).await?;
            self.writer.write_all(b"\r\n").await?;
            self.writer.flush().await?;
            self.read_reply().await
        })
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "tor control command timed out"))?
    }

    async fn read_reply(&mut self) -> Result<Vec<String>, TorControlError> {
        let mut lines = Vec::new();
        for _ in 0..MAX_REPLY_LINES {
            let mut line = String::new();
            let read = tokio::io::AsyncReadExt::take(&mut self.reader, MAX_REPLY_LINE_BYTES)
                .read_line(&mut line)
                .await?;
            if read == 0 {
                return Err(TorControlError::MalformedReply);
            }
            if u64::try_from(read).unwrap_or(u64::MAX) >= MAX_REPLY_LINE_BYTES {
                return Err(TorControlError::OversizedReply);
            }
            let line = line.trim_end_matches(['\r', '\n']).to_owned();
            if line.len() < 4 {
                return Err(TorControlError::MalformedReply);
            }
            let (status, rest) = line.split_at(3);
            let separator = rest.as_bytes()[0];
            let body = rest[1..].to_owned();
            if !status.starts_with('2') {
                return Err(TorControlError::CommandFailed {
                    status: status.to_owned(),
                    detail: body,
                });
            }
            lines.push(body);
            // `-` and `+` mark continuation lines; a space ends the reply.
            if separator == b' ' {
                return Ok(lines);
            }
        }
        Err(TorControlError::OversizedReply)
    }
}

/// Reads Tor's 32-byte authentication cookie.
fn read_cookie(path: &Path) -> Result<[u8; COOKIE_LEN], TorControlError> {
    let cookie = std::fs::read(path).map_err(|_| TorControlError::UnreadableCookie)?;
    cookie
        .try_into()
        .map_err(|_| TorControlError::UnreadableCookie)
}

/// Returns the value following `name`.
///
/// A quoted value ends at its closing quote and may contain spaces,
/// which Tor uses for descriptive fields; an unquoted value ends at the
/// next space.
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

fn hex_upper(bytes: &[u8]) -> String {
    bytes.iter().fold(String::new(), |mut output, byte| {
        use std::fmt::Write;
        let _ = write!(output, "{byte:02X}");
        output
    })
}

fn decode_hex(input: &str) -> Option<Vec<u8>> {
    if input.len() % 2 != 0 {
        return None;
    }
    (0..input.len() / 2)
        .map(|index| u8::from_str_radix(&input[index * 2..index * 2 + 2], 16).ok())
        .collect()
}

/// HMAC-SHA256 over one message, sized for this module's fixed keys.
fn hmac_sha256(key: &[u8], message: &[u8]) -> [u8; HMAC_LEN] {
    const BLOCK_LEN: usize = 64;
    let mut key_block = [0u8; BLOCK_LEN];
    if key.len() > BLOCK_LEN {
        let digest: [u8; 32] = Sha256::digest(key).into();
        key_block[..digest.len()].copy_from_slice(&digest);
    } else {
        key_block[..key.len()].copy_from_slice(key);
    }
    let mut inner_pad = key_block;
    let mut outer_pad = key_block;
    for byte in &mut inner_pad {
        *byte ^= 0x36;
    }
    for byte in &mut outer_pad {
        *byte ^= 0x5c;
    }
    let mut inner = Sha256::new();
    inner.update(inner_pad);
    inner.update(message);
    let inner_digest = inner.finalize();
    let mut outer = Sha256::new();
    outer.update(outer_pad);
    outer.update(inner_digest);
    outer.finalize().into()
}

/// Returns Tor's default control cookie path inside a data directory.
#[must_use]
pub fn default_cookie_path(tor_data_dir: &Path) -> PathBuf {
    tor_data_dir.join("control_auth_cookie")
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::net::TcpListener;

    /// A control port that follows the protocol with a known cookie.
    async fn serve_control_port(
        listener: TcpListener,
        cookie: [u8; COOKIE_LEN],
        service_id: String,
        offer_safecookie: bool,
    ) -> Vec<String> {
        let (stream, _) = listener.accept().await.unwrap();
        let (reader, mut writer) = stream.into_split();
        let mut reader = BufReader::new(reader);
        let mut commands = Vec::new();
        let mut client_nonce = Vec::new();
        loop {
            let mut line = String::new();
            if reader.read_line(&mut line).await.unwrap() == 0 {
                return commands;
            }
            let line = line.trim_end().to_owned();
            commands.push(line.clone());
            if line.starts_with("PROTOCOLINFO") {
                let methods = if offer_safecookie {
                    "COOKIE,SAFECOOKIE"
                } else {
                    "HASHEDPASSWORD"
                };
                writer
                    .write_all(format!("250-AUTH METHODS={methods}\r\n250 OK\r\n").as_bytes())
                    .await
                    .unwrap();
            } else if let Some(nonce) = line.strip_prefix("AUTHCHALLENGE SAFECOOKIE ") {
                client_nonce = decode_hex(nonce).unwrap();
                let server_nonce = [0x5a_u8; NONCE_LEN];
                let mut transcript = cookie.to_vec();
                transcript.extend_from_slice(&client_nonce);
                transcript.extend_from_slice(&server_nonce);
                let server_hash = hmac_sha256(SERVER_HMAC_KEY, &transcript);
                writer
                    .write_all(
                        format!(
                            "250 AUTHCHALLENGE SERVERHASH={} SERVERNONCE={}\r\n",
                            hex_upper(&server_hash),
                            hex_upper(&server_nonce)
                        )
                        .as_bytes(),
                    )
                    .await
                    .unwrap();
            } else if let Some(proof) = line.strip_prefix("AUTHENTICATE ") {
                let server_nonce = [0x5a_u8; NONCE_LEN];
                let mut transcript = cookie.to_vec();
                transcript.extend_from_slice(&client_nonce);
                transcript.extend_from_slice(&server_nonce);
                let expected = hex_upper(&hmac_sha256(CONTROLLER_HMAC_KEY, &transcript));
                if proof == expected {
                    writer.write_all(b"250 OK\r\n").await.unwrap();
                } else {
                    writer
                        .write_all(b"515 Bad authentication\r\n")
                        .await
                        .unwrap();
                }
            } else if line.starts_with("ADD_ONION") {
                writer
                    .write_all(
                        format!(
                            "250-ServiceID={service_id}\r\n250-PrivateKey=ED25519-V3:secret\r\n250 OK\r\n"
                        )
                        .as_bytes(),
                    )
                    .await
                    .unwrap();
            } else if line.starts_with("DEL_ONION") {
                writer.write_all(b"250 OK\r\n").await.unwrap();
            } else {
                writer
                    .write_all(b"510 Unrecognized command\r\n")
                    .await
                    .unwrap();
            }
        }
    }

    fn cookie_file(directory: &tempfile::TempDir, cookie: &[u8]) -> PathBuf {
        let path = directory.path().join("control_auth_cookie");
        std::fs::write(&path, cookie).unwrap();
        path
    }

    #[tokio::test]
    async fn publishes_and_withdraws_an_ephemeral_onion_service() {
        let cookie = [0x11_u8; COOKIE_LEN];
        let directory = tempfile::TempDir::new().unwrap();
        let path = cookie_file(&directory, &cookie);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let control = listener.local_addr().unwrap();
        let expected = OnionAddress::from_public_key([0x33; 32], 8333);
        let service_id = expected.name().strip_suffix(".onion").unwrap().to_owned();
        let server = tokio::spawn(serve_control_port(
            listener,
            cookie,
            service_id.clone(),
            true,
        ));

        let mut controller = TorController::connect(control, &path, TorControlConfig::default())
            .await
            .expect("safecookie authentication succeeds");
        let published = controller
            .add_onion_service(8333, "127.0.0.1:8333".parse().unwrap(), None)
            .await
            .expect("the service publishes");
        assert_eq!(published.address, expected);
        assert_eq!(published.service_id, service_id);
        assert_eq!(published.private_key, "ED25519-V3:secret");
        controller
            .remove_onion_service(&published)
            .await
            .expect("the service withdraws");
        drop(controller);

        let commands = server.await.unwrap();
        assert!(commands[0].starts_with("PROTOCOLINFO"));
        assert!(commands[1].starts_with("AUTHCHALLENGE SAFECOOKIE "));
        assert!(commands[2].starts_with("AUTHENTICATE "));
        assert!(
            commands
                .iter()
                .all(|command| !command.contains(&hex_upper(&cookie))),
            "the cookie itself is never sent to the control port"
        );
        assert!(
            commands
                .iter()
                .any(|command| command.contains("ADD_ONION NEW:ED25519-V3")),
            "a fresh key is requested when none is supplied"
        );
        assert!(
            commands
                .iter()
                .any(|command| command == &format!("DEL_ONION {service_id}"))
        );
    }

    #[tokio::test]
    async fn refuses_a_control_port_without_safecookie() {
        let cookie = [0x11_u8; COOKIE_LEN];
        let directory = tempfile::TempDir::new().unwrap();
        let path = cookie_file(&directory, &cookie);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let control = listener.local_addr().unwrap();
        let server = tokio::spawn(serve_control_port(
            listener,
            cookie,
            "unused".to_owned(),
            false,
        ));
        assert!(matches!(
            TorController::connect(control, &path, TorControlConfig::default()).await,
            Err(TorControlError::UnsupportedAuthentication)
        ));
        server.abort();
    }

    #[tokio::test]
    async fn refuses_a_control_port_that_cannot_prove_the_cookie() {
        let directory = tempfile::TempDir::new().unwrap();
        // The control port answers using a different cookie than ours.
        let path = cookie_file(&directory, &[0x22_u8; COOKIE_LEN]);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let control = listener.local_addr().unwrap();
        let server = tokio::spawn(serve_control_port(
            listener,
            [0x11_u8; COOKIE_LEN],
            "unused".to_owned(),
            true,
        ));
        assert!(matches!(
            TorController::connect(control, &path, TorControlConfig::default()).await,
            Err(TorControlError::ServerProofMismatch)
        ));
        server.abort();
    }

    #[tokio::test]
    async fn refuses_non_loopback_control_ports_and_bad_cookies() {
        let directory = tempfile::TempDir::new().unwrap();
        let path = cookie_file(&directory, &[0x11_u8; COOKIE_LEN]);
        assert!(matches!(
            TorController::connect(
                "203.0.113.7:9051".parse().unwrap(),
                &path,
                TorControlConfig::default()
            )
            .await,
            Err(TorControlError::NonLoopbackControlPort)
        ));
        let short = cookie_file(&directory, b"too short");
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let control = listener.local_addr().unwrap();
        let server = tokio::spawn(serve_control_port(
            listener,
            [0x11_u8; COOKIE_LEN],
            "unused".to_owned(),
            true,
        ));
        assert!(matches!(
            TorController::connect(control, &short, TorControlConfig::default()).await,
            Err(TorControlError::UnreadableCookie)
        ));
        server.abort();
    }

    #[test]
    fn parses_reply_fields_and_default_cookie_path() {
        assert_eq!(field("250-ServiceID=abc def", "ServiceID="), Some("abc"));
        assert_eq!(field("250 X=\"quoted\" Y=1", "X="), Some("quoted"));
        assert_eq!(
            field("250 X=\"two words\" Y=1", "X="),
            Some("two words"),
            "a quoted value keeps its spaces"
        );
        assert_eq!(field("250 OK", "ServiceID="), None);
        assert_eq!(decode_hex("00FF"), Some(vec![0, 255]));
        assert_eq!(decode_hex("0"), None);
        assert_eq!(decode_hex("zz"), None);
        assert_eq!(hex_upper(&[0, 255]), "00FF");
        assert_eq!(
            default_cookie_path(Path::new("/var/lib/tor")),
            Path::new("/var/lib/tor/control_auth_cookie")
        );
    }
}
