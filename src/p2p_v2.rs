//! BIP324 version-2 transport cryptographic core.
//!
//! This module owns the key agreement and record encryption used by the v2
//! encrypted transport: ElligatorSwift X-only ECDH through `secp256k1`,
//! HKDF-SHA256 session-key derivation bound to the network magic, and the
//! rekeying FSChaCha20 / FSChaCha20Poly1305 ciphers for the 3-byte packet
//! length field and the authenticated payload. It performs no I/O and holds
//! no connection state; the garbage/version handshake state machine and the
//! transport integration build on this boundary. Every construction is
//! checked against the official BIP324 packet-encoding test vectors,
//! including the 224-packet rekey boundaries.

use std::sync::OnceLock;

use bitcoin::p2p::Magic;
use bitcoin::secp256k1::ellswift::{ElligatorSwift, ElligatorSwiftParty};
use bitcoin::secp256k1::{All, Secp256k1, SecretKey};
use chacha20::ChaCha20;
use chacha20::cipher::{KeyIvInit, StreamCipher};
use chacha20poly1305::aead::{Aead, Payload};
use chacha20poly1305::{ChaCha20Poly1305, KeyInit};
use sha2::{Digest, Sha256};
use thiserror::Error;

/// Number of messages encrypted under one key before the cipher rekeys.
const REKEY_INTERVAL: u64 = 224;
/// Length in bytes of each side's garbage terminator.
pub const GARBAGE_TERMINATOR_LEN: usize = 16;
/// Length in bytes of the AEAD authentication tag.
pub const TAG_LEN: usize = 16;
/// Length in bytes of the encrypted packet length field.
pub const LENGTH_FIELD_LEN: usize = 3;
/// Length in bytes of the encrypted header preceding the contents.
const HEADER_LEN: usize = 1;
/// Header bit marking a decoy packet the receiver must ignore.
const IGNORE_BIT: u8 = 0x80;
/// Maximum contents length representable in the 24-bit length field.
pub const MAX_CONTENTS_LEN: usize = (1 << 24) - 1;
/// HKDF salt prefix; the 4-byte network magic is appended.
const KDF_SALT_PREFIX: &[u8] = b"bitcoin_v2_shared_secret";
/// Length in bytes of an ElligatorSwift-encoded public key.
pub const ELLSWIFT_LEN: usize = 64;
/// Maximum garbage bytes each side may send before its garbage terminator.
pub const MAX_GARBAGE_LEN: usize = 4095;
/// Length of the v1 prefix (network magic plus the `version` command name)
/// that distinguishes a v1 peer from a v2 public key.
const V1_PREFIX_LEN: usize = 16;
/// Bound on decoy packets accepted before the peer's version packet.
const MAX_HANDSHAKE_PACKETS: usize = 64;
/// Bound on one handshake packet's contents; matches the enforced v1
/// 4,000,000-byte message ceiling rather than the 24-bit field maximum.
const MAX_HANDSHAKE_PACKET_CONTENTS: usize = 4_000_000;
/// Bound on secret-key generation attempts before failing closed.
const MAX_KEY_GENERATION_ATTEMPTS: usize = 64;

/// Errors from the v2 record layer.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum V2CryptoError {
    /// A packet failed AEAD authentication.
    #[error("v2 packet failed authentication")]
    Authentication,
    /// Contents exceed the 24-bit length field.
    #[error("v2 packet contents length {length} exceeds the 16,777,215-byte bound")]
    OversizedContents {
        /// The rejected contents length.
        length: usize,
    },
    /// A ciphertext was shorter than one header byte plus the tag.
    #[error("v2 packet ciphertext length {length} is below the 17-byte minimum")]
    TruncatedPacket {
        /// The rejected ciphertext length.
        length: usize,
    },
}

/// Our role in the v2 handshake.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    /// We opened the connection and speak first.
    Initiator,
    /// We accepted the connection.
    Responder,
}

/// Session secrets derived from the handshake ECDH secret.
///
/// Field names follow the BIP324 derivation labels. Both sides derive the
/// identical structure; [`SessionKeys::into_packet_ciphers`] selects the
/// send/receive halves for a concrete role.
pub struct SessionKeys {
    /// Length-field key for packets sent by the initiator (`initiator_L`).
    pub initiator_length_key: [u8; 32],
    /// Payload key for packets sent by the initiator (`initiator_P`).
    pub initiator_packet_key: [u8; 32],
    /// Length-field key for packets sent by the responder (`responder_L`).
    pub responder_length_key: [u8; 32],
    /// Payload key for packets sent by the responder (`responder_P`).
    pub responder_packet_key: [u8; 32],
    /// Terminator ending the initiator's handshake garbage.
    pub initiator_garbage_terminator: [u8; GARBAGE_TERMINATOR_LEN],
    /// Terminator ending the responder's handshake garbage.
    pub responder_garbage_terminator: [u8; GARBAGE_TERMINATOR_LEN],
    /// Session identifier for out-of-band comparison.
    pub session_id: [u8; 32],
}

/// Computes the BIP324 tagged ECDH secret for one side of the handshake.
///
/// `our_ellswift` and `their_ellswift` are the 64-byte ElligatorSwift-encoded
/// public keys exchanged in the clear; the initiator's key is ECDH party `A`.
#[must_use]
pub fn v2_shared_secret(
    our_secret: &SecretKey,
    our_ellswift: ElligatorSwift,
    their_ellswift: ElligatorSwift,
    role: Role,
) -> [u8; 32] {
    let (initiator, responder, party) = match role {
        Role::Initiator => (our_ellswift, their_ellswift, ElligatorSwiftParty::A),
        Role::Responder => (their_ellswift, our_ellswift, ElligatorSwiftParty::B),
    };
    *ElligatorSwift::shared_secret(initiator, responder, *our_secret, party, None).as_secret_bytes()
}

/// Derives the complete session-key material from the ECDH secret.
///
/// The derivation is bound to the network through the message-start magic in
/// the HKDF salt, so a session keyed for one network cannot authenticate
/// packets on another.
#[must_use]
pub fn derive_session_keys(shared_secret: &[u8; 32], magic: Magic) -> SessionKeys {
    let mut salt = [0u8; KDF_SALT_PREFIX.len() + 4];
    salt[..KDF_SALT_PREFIX.len()].copy_from_slice(KDF_SALT_PREFIX);
    salt[KDF_SALT_PREFIX.len()..].copy_from_slice(&magic.to_bytes());
    let pseudorandom_key = hmac_sha256(&salt, &[shared_secret]);
    let garbage = hkdf_expand32(&pseudorandom_key, b"garbage_terminators");
    let mut initiator_garbage_terminator = [0u8; GARBAGE_TERMINATOR_LEN];
    let mut responder_garbage_terminator = [0u8; GARBAGE_TERMINATOR_LEN];
    initiator_garbage_terminator.copy_from_slice(&garbage[..GARBAGE_TERMINATOR_LEN]);
    responder_garbage_terminator.copy_from_slice(&garbage[GARBAGE_TERMINATOR_LEN..]);
    SessionKeys {
        initiator_length_key: hkdf_expand32(&pseudorandom_key, b"initiator_L"),
        initiator_packet_key: hkdf_expand32(&pseudorandom_key, b"initiator_P"),
        responder_length_key: hkdf_expand32(&pseudorandom_key, b"responder_L"),
        responder_packet_key: hkdf_expand32(&pseudorandom_key, b"responder_P"),
        initiator_garbage_terminator,
        responder_garbage_terminator,
        session_id: hkdf_expand32(&pseudorandom_key, b"session_id"),
    }
}

impl SessionKeys {
    /// Splits the key material into the packet ciphers for `role`.
    #[must_use]
    pub fn into_packet_ciphers(self, role: Role) -> (PacketSender, PacketReceiver) {
        match role {
            Role::Initiator => (
                PacketSender::new(self.initiator_length_key, self.initiator_packet_key),
                PacketReceiver::new(self.responder_length_key, self.responder_packet_key),
            ),
            Role::Responder => (
                PacketSender::new(self.responder_length_key, self.responder_packet_key),
                PacketReceiver::new(self.initiator_length_key, self.initiator_packet_key),
            ),
        }
    }
}

/// Encrypts the outbound half of one v2 session.
pub struct PacketSender {
    length_cipher: FsChaCha20,
    packet_cipher: FsChaCha20Poly1305,
}

impl PacketSender {
    fn new(length_key: [u8; 32], packet_key: [u8; 32]) -> Self {
        Self {
            length_cipher: FsChaCha20::new(length_key),
            packet_cipher: FsChaCha20Poly1305::new(packet_key),
        }
    }

    /// Encrypts one packet: encrypted length field, then header+contents+tag.
    ///
    /// `aad` is empty for every packet except each side's version packet,
    /// which authenticates that side's handshake garbage. `ignore` marks a
    /// decoy packet the receiver discards after authentication.
    pub fn encrypt_packet(
        &mut self,
        contents: &[u8],
        aad: &[u8],
        ignore: bool,
    ) -> Result<Vec<u8>, V2CryptoError> {
        if contents.len() > MAX_CONTENTS_LEN {
            return Err(V2CryptoError::OversizedContents {
                length: contents.len(),
            });
        }
        let length_le = u32::try_from(contents.len())
            .expect("contents length is bounded by MAX_CONTENTS_LEN")
            .to_le_bytes();
        let mut length_bytes = [length_le[0], length_le[1], length_le[2]];
        self.length_cipher.crypt_in_place(&mut length_bytes);
        let mut plaintext = Vec::with_capacity(HEADER_LEN + contents.len());
        plaintext.push(if ignore { IGNORE_BIT } else { 0 });
        plaintext.extend_from_slice(contents);
        let ciphertext = self.packet_cipher.encrypt(aad, &plaintext);
        let mut packet = Vec::with_capacity(LENGTH_FIELD_LEN + ciphertext.len());
        packet.extend_from_slice(&length_bytes);
        packet.extend_from_slice(&ciphertext);
        Ok(packet)
    }
}

/// Decrypts the inbound half of one v2 session.
pub struct PacketReceiver {
    length_cipher: FsChaCha20,
    packet_cipher: FsChaCha20Poly1305,
}

impl PacketReceiver {
    fn new(length_key: [u8; 32], packet_key: [u8; 32]) -> Self {
        Self {
            length_cipher: FsChaCha20::new(length_key),
            packet_cipher: FsChaCha20Poly1305::new(packet_key),
        }
    }

    /// Decrypts a 3-byte length field into the following contents length.
    ///
    /// The length field is not authenticated on its own; the caller must
    /// bound the returned length before allocating and must treat any
    /// subsequent [`PacketReceiver::decrypt_packet`] failure as fatal for
    /// the connection.
    pub fn decrypt_length(&mut self, mut encrypted: [u8; LENGTH_FIELD_LEN]) -> usize {
        self.length_cipher.crypt_in_place(&mut encrypted);
        let length = u32::from_le_bytes([encrypted[0], encrypted[1], encrypted[2], 0]);
        usize::try_from(length).expect("a 24-bit length fits every supported usize")
    }

    /// Authenticates and decrypts header+contents+tag ciphertext.
    ///
    /// Returns the decoy flag and the contents. A failure means the cipher
    /// states are no longer synchronized and the connection must close.
    pub fn decrypt_packet(
        &mut self,
        aad: &[u8],
        ciphertext: &[u8],
    ) -> Result<(bool, Vec<u8>), V2CryptoError> {
        if ciphertext.len() < HEADER_LEN + TAG_LEN {
            return Err(V2CryptoError::TruncatedPacket {
                length: ciphertext.len(),
            });
        }
        let mut plaintext = self.packet_cipher.decrypt(aad, ciphertext)?;
        let header = plaintext[0];
        plaintext.drain(..HEADER_LEN);
        Ok((header & IGNORE_BIT != 0, plaintext))
    }
}

/// Errors from the v2 handshake state machine.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum V2HandshakeError {
    /// The record layer rejected a handshake packet.
    #[error(transparent)]
    Crypto(#[from] V2CryptoError),
    /// Requested garbage exceeds the protocol bound.
    #[error("v2 handshake garbage length {length} exceeds the 4,095-byte bound")]
    OversizedGarbage {
        /// The rejected garbage length.
        length: usize,
    },
    /// The peer's garbage terminator did not appear within the bound.
    #[error("v2 handshake garbage terminator not found within {limit} bytes")]
    GarbageTerminatorMissing {
        /// Maximum bytes searched.
        limit: usize,
    },
    /// A pre-version packet announced oversized contents.
    #[error("v2 handshake packet contents length {length} exceeds the {limit}-byte bound")]
    OversizedHandshakePacket {
        /// The announced contents length.
        length: usize,
        /// The enforced ceiling.
        limit: usize,
    },
    /// The peer sent more decoy packets than the accepted bound.
    #[error("v2 handshake exceeded {limit} packets before the version packet")]
    TooManyHandshakePackets {
        /// The enforced ceiling.
        limit: usize,
    },
    /// Secret-key generation failed repeatedly; entropy is unusable.
    #[error("v2 handshake key generation failed after {limit} attempts")]
    KeyGeneration {
        /// Attempts made before failing closed.
        limit: usize,
    },
    /// The state machine was driven after completion or failure.
    #[error("v2 handshake driven after completion or failure")]
    Finished,
}

/// Completed v2 session state handed to the transport.
pub struct V2Session {
    /// Cipher for packets we send.
    pub sender: PacketSender,
    /// Cipher for packets we receive.
    pub receiver: PacketReceiver,
    /// Session identifier for out-of-band comparison.
    pub session_id: [u8; 32],
    /// Application bytes received beyond the handshake, in order.
    pub leftover: Vec<u8>,
}

/// Observable outcome of feeding handshake bytes.
pub enum HandshakeEvent {
    /// More inbound bytes are required.
    NeedMoreData,
    /// The peer opened with the v1 prefix (responder role only); the caller
    /// must continue the same connection as v1 using the returned bytes.
    PeerSpeaksV1 {
        /// Every byte received so far, unconsumed.
        received: Vec<u8>,
    },
    /// The handshake finished; the session owns any leftover bytes.
    Complete(Box<V2Session>),
}

/// One step's outputs: bytes to write, then the observable event.
pub struct HandshakeStep {
    /// Bytes to write to the peer before waiting for more input.
    pub send: Vec<u8>,
    /// The state observable after this step.
    pub event: HandshakeEvent,
}

/// State while scanning for the peer's garbage terminator.
struct GarbagePhase {
    sender: PacketSender,
    receiver: PacketReceiver,
    recv_terminator: [u8; GARBAGE_TERMINATOR_LEN],
    session_id: [u8; 32],
}

/// State while decrypting decoys until the peer's version packet.
struct VersionPhase {
    sender: PacketSender,
    receiver: PacketReceiver,
    first_packet_aad: Option<Vec<u8>>,
    session_id: [u8; 32],
    packets_seen: usize,
    pending_length: Option<usize>,
}

enum HandshakeState {
    /// Waiting for the peer's 64-byte public key.
    AwaitingKey,
    /// Keys derived; scanning for the peer's garbage terminator.
    AwaitingGarbageTerminator(GarbagePhase),
    /// Terminator found; decrypting decoys until the version packet.
    AwaitingVersionPacket(VersionPhase),
    /// The handshake failed or completed; no further input is accepted.
    Finished,
}

/// Sans-I/O BIP324 handshake driver for one connection.
///
/// The caller owns the socket: it writes the byte strings this machine
/// returns and feeds every received chunk to [`V2Handshake::push_bytes`].
/// Reads must stay bounded by the caller's ordinary receive budget; the
/// machine itself bounds garbage, decoy count, and per-packet contents, and
/// every protocol violation fails closed into a terminal state.
pub struct V2Handshake {
    role: Role,
    magic: Magic,
    secret: SecretKey,
    our_ellswift: ElligatorSwift,
    our_garbage: Vec<u8>,
    buffer: Vec<u8>,
    state: HandshakeState,
}

impl V2Handshake {
    /// Creates the outbound-side handshake and the first bytes to send:
    /// our ElligatorSwift public key followed by our garbage.
    pub fn initiator(magic: Magic, garbage: Vec<u8>) -> Result<(Self, Vec<u8>), V2HandshakeError> {
        Self::validate_garbage(&garbage)?;
        let (secret, our_ellswift) = generate_handshake_keypair(magic)?;
        let mut first = Vec::with_capacity(ELLSWIFT_LEN + garbage.len());
        first.extend_from_slice(&our_ellswift.to_array());
        first.extend_from_slice(&garbage);
        Ok((
            Self {
                role: Role::Initiator,
                magic,
                secret,
                our_ellswift,
                our_garbage: garbage,
                buffer: Vec::new(),
                state: HandshakeState::AwaitingKey,
            },
            first,
        ))
    }

    /// Creates the inbound-side handshake; it sends nothing until the
    /// initiator's public key arrives or the v1 prefix is recognized.
    pub fn responder(magic: Magic, garbage: Vec<u8>) -> Result<Self, V2HandshakeError> {
        Self::validate_garbage(&garbage)?;
        let (secret, our_ellswift) = generate_handshake_keypair(magic)?;
        Ok(Self {
            role: Role::Responder,
            magic,
            secret,
            our_ellswift,
            our_garbage: garbage,
            buffer: Vec::new(),
            state: HandshakeState::AwaitingKey,
        })
    }

    fn validate_garbage(garbage: &[u8]) -> Result<(), V2HandshakeError> {
        if garbage.len() > MAX_GARBAGE_LEN {
            return Err(V2HandshakeError::OversizedGarbage {
                length: garbage.len(),
            });
        }
        Ok(())
    }

    /// Feeds received bytes and advances as far as possible.
    ///
    /// Any error is terminal: the connection must close (initiator) or fall
    /// back is impossible (responder past the v1 prefix check).
    pub fn push_bytes(&mut self, bytes: &[u8]) -> Result<HandshakeStep, V2HandshakeError> {
        if matches!(self.state, HandshakeState::Finished) {
            return Err(V2HandshakeError::Finished);
        }
        self.buffer.extend_from_slice(bytes);
        let mut send = Vec::new();
        loop {
            let outcome = match std::mem::replace(&mut self.state, HandshakeState::Finished) {
                HandshakeState::AwaitingKey => self.consume_peer_key(&mut send)?,
                HandshakeState::AwaitingGarbageTerminator(phase) => {
                    self.locate_garbage_terminator(phase)?
                }
                HandshakeState::AwaitingVersionPacket(phase) => {
                    self.consume_handshake_packet(phase)?
                }
                HandshakeState::Finished => return Err(V2HandshakeError::Finished),
            };
            if let Some(event) = outcome {
                return Ok(HandshakeStep { send, event });
            }
        }
    }

    /// Consumes the peer's public key, derives the session, and queues our
    /// garbage terminator and version packet (plus, for the responder, our
    /// key and garbage). Also recognizes a v1 peer in the responder role.
    fn consume_peer_key(
        &mut self,
        send: &mut Vec<u8>,
    ) -> Result<Option<HandshakeEvent>, V2HandshakeError> {
        if self.role == Role::Responder {
            let prefix = v1_prefix(self.magic);
            let compared = self.buffer.len().min(V1_PREFIX_LEN);
            if self.buffer[..compared] == prefix[..compared] {
                if compared == V1_PREFIX_LEN {
                    return Ok(Some(HandshakeEvent::PeerSpeaksV1 {
                        received: std::mem::take(&mut self.buffer),
                    }));
                }
                self.state = HandshakeState::AwaitingKey;
                return Ok(Some(HandshakeEvent::NeedMoreData));
            }
        }
        if self.buffer.len() < ELLSWIFT_LEN {
            self.state = HandshakeState::AwaitingKey;
            return Ok(Some(HandshakeEvent::NeedMoreData));
        }
        let mut their_key = [0u8; ELLSWIFT_LEN];
        their_key.copy_from_slice(&self.buffer[..ELLSWIFT_LEN]);
        self.buffer.drain(..ELLSWIFT_LEN);
        let their_ellswift = ElligatorSwift::from_array(their_key);
        let shared = v2_shared_secret(&self.secret, self.our_ellswift, their_ellswift, self.role);
        let keys = derive_session_keys(&shared, self.magic);
        let session_id = keys.session_id;
        let (send_terminator, recv_terminator) = match self.role {
            Role::Initiator => (
                keys.initiator_garbage_terminator,
                keys.responder_garbage_terminator,
            ),
            Role::Responder => (
                keys.responder_garbage_terminator,
                keys.initiator_garbage_terminator,
            ),
        };
        let (mut sender, receiver) = keys.into_packet_ciphers(self.role);
        if self.role == Role::Responder {
            send.extend_from_slice(&self.our_ellswift.to_array());
            send.extend_from_slice(&self.our_garbage);
        }
        send.extend_from_slice(&send_terminator);
        let version_packet = sender.encrypt_packet(&[], &self.our_garbage, false)?;
        send.extend_from_slice(&version_packet);
        self.state = HandshakeState::AwaitingGarbageTerminator(GarbagePhase {
            sender,
            receiver,
            recv_terminator,
            session_id,
        });
        Ok(None)
    }

    /// Scans for the peer's garbage terminator within the protocol bound and
    /// records the preceding garbage as the first packet's associated data.
    fn locate_garbage_terminator(
        &mut self,
        phase: GarbagePhase,
    ) -> Result<Option<HandshakeEvent>, V2HandshakeError> {
        let Some(position) = self
            .buffer
            .windows(GARBAGE_TERMINATOR_LEN)
            .take(MAX_GARBAGE_LEN + 1)
            .position(|window| window == phase.recv_terminator)
        else {
            if self.buffer.len() > MAX_GARBAGE_LEN + GARBAGE_TERMINATOR_LEN {
                return Err(V2HandshakeError::GarbageTerminatorMissing {
                    limit: MAX_GARBAGE_LEN + GARBAGE_TERMINATOR_LEN,
                });
            }
            self.state = HandshakeState::AwaitingGarbageTerminator(phase);
            return Ok(Some(HandshakeEvent::NeedMoreData));
        };
        let peer_garbage = self.buffer[..position].to_vec();
        self.buffer.drain(..position + GARBAGE_TERMINATOR_LEN);
        self.state = HandshakeState::AwaitingVersionPacket(VersionPhase {
            sender: phase.sender,
            receiver: phase.receiver,
            first_packet_aad: Some(peer_garbage),
            session_id: phase.session_id,
            packets_seen: 0,
            pending_length: None,
        });
        Ok(None)
    }

    /// Decrypts one bounded handshake packet: a decoy keeps waiting for the
    /// version packet, and the version packet completes the handshake.
    fn consume_handshake_packet(
        &mut self,
        mut phase: VersionPhase,
    ) -> Result<Option<HandshakeEvent>, V2HandshakeError> {
        let length = if let Some(length) = phase.pending_length {
            length
        } else {
            if self.buffer.len() < LENGTH_FIELD_LEN {
                self.state = HandshakeState::AwaitingVersionPacket(phase);
                return Ok(Some(HandshakeEvent::NeedMoreData));
            }
            let mut encrypted = [0u8; LENGTH_FIELD_LEN];
            encrypted.copy_from_slice(&self.buffer[..LENGTH_FIELD_LEN]);
            self.buffer.drain(..LENGTH_FIELD_LEN);
            let length = phase.receiver.decrypt_length(encrypted);
            if length > MAX_HANDSHAKE_PACKET_CONTENTS {
                return Err(V2HandshakeError::OversizedHandshakePacket {
                    length,
                    limit: MAX_HANDSHAKE_PACKET_CONTENTS,
                });
            }
            length
        };
        let ciphertext_len = HEADER_LEN + length + TAG_LEN;
        if self.buffer.len() < ciphertext_len {
            phase.pending_length = Some(length);
            self.state = HandshakeState::AwaitingVersionPacket(phase);
            return Ok(Some(HandshakeEvent::NeedMoreData));
        }
        let aad = phase.first_packet_aad.take().unwrap_or_default();
        let (ignore, _contents) = phase
            .receiver
            .decrypt_packet(&aad, &self.buffer[..ciphertext_len])?;
        self.buffer.drain(..ciphertext_len);
        if ignore {
            phase.packets_seen += 1;
            if phase.packets_seen > MAX_HANDSHAKE_PACKETS {
                return Err(V2HandshakeError::TooManyHandshakePackets {
                    limit: MAX_HANDSHAKE_PACKETS,
                });
            }
            phase.pending_length = None;
            self.state = HandshakeState::AwaitingVersionPacket(phase);
            return Ok(None);
        }
        Ok(Some(HandshakeEvent::Complete(Box::new(V2Session {
            sender: phase.sender,
            receiver: phase.receiver,
            session_id: phase.session_id,
            leftover: std::mem::take(&mut self.buffer),
        }))))
    }
}

fn secp_context() -> &'static Secp256k1<All> {
    static CONTEXT: OnceLock<Secp256k1<All>> = OnceLock::new();
    CONTEXT.get_or_init(Secp256k1::new)
}

fn v1_prefix(magic: Magic) -> [u8; V1_PREFIX_LEN] {
    let mut prefix = [0u8; V1_PREFIX_LEN];
    prefix[..4].copy_from_slice(&magic.to_bytes());
    prefix[4..11].copy_from_slice(b"version");
    prefix
}

/// Generates a fresh handshake keypair whose encoding cannot be mistaken
/// for a v1 `version` message start.
fn generate_handshake_keypair(
    magic: Magic,
) -> Result<(SecretKey, ElligatorSwift), V2HandshakeError> {
    let prefix = v1_prefix(magic);
    for _ in 0..MAX_KEY_GENERATION_ATTEMPTS {
        let Ok(secret) = SecretKey::from_slice(&rand::random::<[u8; 32]>()) else {
            continue;
        };
        let ellswift = ElligatorSwift::from_seckey(secp_context(), secret, Some(rand::random()));
        if ellswift.to_array()[..V1_PREFIX_LEN] != prefix {
            return Ok((secret, ellswift));
        }
    }
    Err(V2HandshakeError::KeyGeneration {
        limit: MAX_KEY_GENERATION_ATTEMPTS,
    })
}

/// BIP324's rekeying ChaCha20 stream cipher for packet length fields.
///
/// The keystream position runs continuously within one rekey epoch; after
/// [`REKEY_INTERVAL`] messages the next 32 keystream bytes become the new
/// key and the nonce's rekey counter increments.
struct FsChaCha20 {
    cipher: ChaCha20,
    chunk_counter: u64,
}

impl FsChaCha20 {
    fn new(key: [u8; 32]) -> Self {
        Self {
            cipher: length_cipher_instance(&key, 0),
            chunk_counter: 0,
        }
    }

    fn crypt_in_place(&mut self, chunk: &mut [u8]) {
        self.cipher.apply_keystream(chunk);
        self.chunk_counter += 1;
        if self.chunk_counter % REKEY_INTERVAL == 0 {
            let mut new_key = [0u8; 32];
            self.cipher.apply_keystream(&mut new_key);
            self.cipher = length_cipher_instance(&new_key, self.chunk_counter / REKEY_INTERVAL);
        }
    }
}

fn length_cipher_instance(key: &[u8; 32], rekey_counter: u64) -> ChaCha20 {
    let mut nonce = [0u8; 12];
    nonce[4..].copy_from_slice(&rekey_counter.to_le_bytes());
    ChaCha20::new(key.into(), (&nonce).into())
}

/// BIP324's rekeying ChaCha20-Poly1305 AEAD for packet payloads.
///
/// The 96-bit nonce is the little-endian message counter within the current
/// rekey epoch followed by the little-endian epoch counter. After
/// [`REKEY_INTERVAL`] messages the key is replaced by encrypting 32 zero
/// bytes under the all-ones message counter.
struct FsChaCha20Poly1305 {
    key: [u8; 32],
    packet_counter: u64,
}

impl FsChaCha20Poly1305 {
    fn new(key: [u8; 32]) -> Self {
        Self {
            key,
            packet_counter: 0,
        }
    }

    fn nonce(&self) -> [u8; 12] {
        let mut nonce = [0u8; 12];
        let message = u32::try_from(self.packet_counter % REKEY_INTERVAL)
            .expect("message counter is below the 224-message rekey interval");
        nonce[..4].copy_from_slice(&message.to_le_bytes());
        nonce[4..].copy_from_slice(&(self.packet_counter / REKEY_INTERVAL).to_le_bytes());
        nonce
    }

    fn advance(&mut self) {
        if (self.packet_counter + 1) % REKEY_INTERVAL == 0 {
            let mut rekey_nonce = [0xff_u8; 12];
            rekey_nonce[4..].copy_from_slice(&(self.packet_counter / REKEY_INTERVAL).to_le_bytes());
            let cipher = ChaCha20Poly1305::new(&self.key.into());
            let ciphertext = cipher
                .encrypt(
                    (&rekey_nonce).into(),
                    Payload {
                        msg: &[0u8; 32],
                        aad: &[],
                    },
                )
                .expect("in-memory AEAD encryption cannot fail");
            self.key.copy_from_slice(&ciphertext[..32]);
        }
        self.packet_counter += 1;
    }

    fn encrypt(&mut self, aad: &[u8], plaintext: &[u8]) -> Vec<u8> {
        let cipher = ChaCha20Poly1305::new(&self.key.into());
        let ciphertext = cipher
            .encrypt(
                (&self.nonce()).into(),
                Payload {
                    msg: plaintext,
                    aad,
                },
            )
            .expect("in-memory AEAD encryption cannot fail");
        self.advance();
        ciphertext
    }

    fn decrypt(&mut self, aad: &[u8], ciphertext: &[u8]) -> Result<Vec<u8>, V2CryptoError> {
        let cipher = ChaCha20Poly1305::new(&self.key.into());
        let plaintext = cipher
            .decrypt(
                (&self.nonce()).into(),
                Payload {
                    msg: ciphertext,
                    aad,
                },
            )
            .map_err(|_| V2CryptoError::Authentication)?;
        self.advance();
        Ok(plaintext)
    }
}

fn hmac_sha256(key: &[u8], message_parts: &[&[u8]]) -> [u8; 32] {
    const BLOCK_LEN: usize = 64;
    let mut key_block = [0u8; BLOCK_LEN];
    if key.len() > BLOCK_LEN {
        let digest: [u8; 32] = Sha256::digest(key).into();
        key_block[..digest.len()].copy_from_slice(&digest);
    } else {
        key_block[..key.len()].copy_from_slice(key);
    }
    let mut inner_pad = key_block;
    for byte in &mut inner_pad {
        *byte ^= 0x36;
    }
    let mut inner = Sha256::new();
    inner.update(inner_pad);
    for part in message_parts {
        inner.update(part);
    }
    let inner_digest = inner.finalize();
    let mut outer_pad = key_block;
    for byte in &mut outer_pad {
        *byte ^= 0x5c;
    }
    let mut outer = Sha256::new();
    outer.update(outer_pad);
    outer.update(inner_digest);
    outer.finalize().into()
}

/// One-block HKDF-Expand: the derived keys are exactly one SHA-256 output.
fn hkdf_expand32(pseudorandom_key: &[u8; 32], info: &[u8]) -> [u8; 32] {
    hmac_sha256(pseudorandom_key, &[info, &[1u8]])
}

#[cfg(test)]
mod tests {
    use super::*;
    use bitcoin::hex::FromHex;

    const VECTORS: &str = include_str!("../tests/data/bip324_packet_encoding_test_vectors.csv");

    struct Vector {
        index: u64,
        secret: SecretKey,
        ours: ElligatorSwift,
        theirs: ElligatorSwift,
        role: Role,
        contents: Vec<u8>,
        aad: Vec<u8>,
        ignore: bool,
        shared_secret: [u8; 32],
        initiator_length_key: [u8; 32],
        initiator_packet_key: [u8; 32],
        responder_length_key: [u8; 32],
        responder_packet_key: [u8; 32],
        send_garbage_terminator: [u8; 16],
        recv_garbage_terminator: [u8; 16],
        session_id: [u8; 32],
        ciphertext: Vec<u8>,
        ciphertext_endswith: Vec<u8>,
    }

    fn array<const LEN: usize>(hex: &str) -> [u8; LEN] {
        Vec::<u8>::from_hex(hex)
            .expect("vector field is valid hex")
            .try_into()
            .expect("vector field has the expected length")
    }

    fn vectors() -> Vec<Vector> {
        VECTORS
            .lines()
            .skip(1)
            .filter(|line| !line.trim().is_empty())
            .map(|line| {
                let fields: Vec<&str> = line.split(',').collect();
                assert_eq!(fields.len(), 22, "unexpected vector column count");
                let unit = Vec::<u8>::from_hex(fields[5]).expect("contents hex");
                let multiply: usize = fields[6].parse().expect("multiplier");
                let mut contents = Vec::with_capacity(unit.len() * multiply);
                for _ in 0..multiply {
                    contents.extend_from_slice(&unit);
                }
                Vector {
                    index: fields[0].parse().expect("packet index"),
                    secret: SecretKey::from_slice(&array::<32>(fields[1]))
                        .expect("vector secret key"),
                    ours: ElligatorSwift::from_array(array::<64>(fields[2])),
                    theirs: ElligatorSwift::from_array(array::<64>(fields[3])),
                    role: if fields[4] == "1" {
                        Role::Initiator
                    } else {
                        assert_eq!(fields[4], "0", "unexpected in_initiating value");
                        Role::Responder
                    },
                    contents,
                    aad: Vec::<u8>::from_hex(fields[7]).expect("aad hex"),
                    ignore: fields[8] == "1",
                    shared_secret: array::<32>(fields[12]),
                    initiator_length_key: array::<32>(fields[13]),
                    initiator_packet_key: array::<32>(fields[14]),
                    responder_length_key: array::<32>(fields[15]),
                    responder_packet_key: array::<32>(fields[16]),
                    send_garbage_terminator: array::<16>(fields[17]),
                    recv_garbage_terminator: array::<16>(fields[18]),
                    session_id: array::<32>(fields[19]),
                    ciphertext: Vec::<u8>::from_hex(fields[20]).expect("ciphertext hex"),
                    ciphertext_endswith: Vec::<u8>::from_hex(fields[21])
                        .expect("ciphertext suffix hex"),
                }
            })
            .collect()
    }

    #[test]
    fn derives_official_vector_key_material() {
        let cases = vectors();
        assert_eq!(cases.len(), 7, "official vector count");
        for case in &cases {
            let shared = v2_shared_secret(&case.secret, case.ours, case.theirs, case.role);
            assert_eq!(shared, case.shared_secret);
            let keys = derive_session_keys(&shared, Magic::BITCOIN);
            assert_eq!(keys.initiator_length_key, case.initiator_length_key);
            assert_eq!(keys.initiator_packet_key, case.initiator_packet_key);
            assert_eq!(keys.responder_length_key, case.responder_length_key);
            assert_eq!(keys.responder_packet_key, case.responder_packet_key);
            assert_eq!(keys.session_id, case.session_id);
            let (send_terminator, recv_terminator) = match case.role {
                Role::Initiator => (
                    keys.initiator_garbage_terminator,
                    keys.responder_garbage_terminator,
                ),
                Role::Responder => (
                    keys.responder_garbage_terminator,
                    keys.initiator_garbage_terminator,
                ),
            };
            assert_eq!(send_terminator, case.send_garbage_terminator);
            assert_eq!(recv_terminator, case.recv_garbage_terminator);
        }
    }

    #[test]
    fn encrypts_and_round_trips_official_vector_packets() {
        for case in vectors() {
            let shared = v2_shared_secret(&case.secret, case.ours, case.theirs, case.role);
            let (mut sender, _) =
                derive_session_keys(&shared, Magic::BITCOIN).into_packet_ciphers(case.role);
            let peer_role = match case.role {
                Role::Initiator => Role::Responder,
                Role::Responder => Role::Initiator,
            };
            let (_, mut receiver) =
                derive_session_keys(&shared, Magic::BITCOIN).into_packet_ciphers(peer_role);
            let mut dummies = Vec::new();
            for _ in 0..case.index {
                dummies.push(
                    sender
                        .encrypt_packet(&[], &[], false)
                        .expect("dummy packet encrypts"),
                );
            }
            let packet = sender
                .encrypt_packet(&case.contents, &case.aad, case.ignore)
                .expect("vector packet encrypts");
            if case.ciphertext.is_empty() {
                assert!(packet.ends_with(&case.ciphertext_endswith));
            } else {
                assert_eq!(packet, case.ciphertext);
            }
            for dummy in &dummies {
                let length = receiver.decrypt_length(dummy[..LENGTH_FIELD_LEN].try_into().unwrap());
                assert_eq!(length, 0);
                let (ignore, contents) = receiver
                    .decrypt_packet(&[], &dummy[LENGTH_FIELD_LEN..])
                    .expect("dummy packet authenticates");
                assert!(!ignore);
                assert!(contents.is_empty());
            }
            let length = receiver.decrypt_length(packet[..LENGTH_FIELD_LEN].try_into().unwrap());
            assert_eq!(length, case.contents.len());
            let (ignore, contents) = receiver
                .decrypt_packet(&case.aad, &packet[LENGTH_FIELD_LEN..])
                .expect("vector packet authenticates");
            assert_eq!(ignore, case.ignore);
            assert_eq!(contents, case.contents);
        }
    }

    fn fixed_session() -> (PacketSender, PacketReceiver, PacketSender, PacketReceiver) {
        let secp = bitcoin::secp256k1::Secp256k1::new();
        let initiator_secret = SecretKey::from_slice(&[0x11; 32]).expect("static key");
        let responder_secret = SecretKey::from_slice(&[0x22; 32]).expect("static key");
        let initiator_ellswift =
            ElligatorSwift::from_seckey(&secp, initiator_secret, Some([0x33; 32]));
        let responder_ellswift =
            ElligatorSwift::from_seckey(&secp, responder_secret, Some([0x44; 32]));
        let initiator_shared = v2_shared_secret(
            &initiator_secret,
            initiator_ellswift,
            responder_ellswift,
            Role::Initiator,
        );
        let responder_shared = v2_shared_secret(
            &responder_secret,
            responder_ellswift,
            initiator_ellswift,
            Role::Responder,
        );
        assert_eq!(initiator_shared, responder_shared);
        let (initiator_send, initiator_recv) =
            derive_session_keys(&initiator_shared, Magic::SIGNET)
                .into_packet_ciphers(Role::Initiator);
        let (responder_send, responder_recv) =
            derive_session_keys(&responder_shared, Magic::SIGNET)
                .into_packet_ciphers(Role::Responder);
        (
            initiator_send,
            initiator_recv,
            responder_send,
            responder_recv,
        )
    }

    #[test]
    fn both_roles_stay_synchronized_across_rekey_boundaries() {
        let (mut initiator_send, mut initiator_recv, mut responder_send, mut responder_recv) =
            fixed_session();
        for round in 0..(REKEY_INTERVAL + 76) {
            let contents = round.to_le_bytes();
            let to_responder = initiator_send
                .encrypt_packet(&contents, &[], false)
                .expect("initiator packet encrypts");
            let length =
                responder_recv.decrypt_length(to_responder[..LENGTH_FIELD_LEN].try_into().unwrap());
            assert_eq!(length, contents.len());
            let (ignore, plaintext) = responder_recv
                .decrypt_packet(&[], &to_responder[LENGTH_FIELD_LEN..])
                .expect("initiator packet authenticates");
            assert!(!ignore);
            assert_eq!(plaintext, contents);
            let to_initiator = responder_send
                .encrypt_packet(&contents, &[], true)
                .expect("responder packet encrypts");
            let length =
                initiator_recv.decrypt_length(to_initiator[..LENGTH_FIELD_LEN].try_into().unwrap());
            assert_eq!(length, contents.len());
            let (ignore, plaintext) = initiator_recv
                .decrypt_packet(&[], &to_initiator[LENGTH_FIELD_LEN..])
                .expect("responder packet authenticates");
            assert!(ignore);
            assert_eq!(plaintext, contents);
        }
    }

    #[test]
    fn rejects_tampered_ciphertext_wrong_aad_and_truncation() {
        let (mut initiator_send, _, _, mut responder_recv) = fixed_session();
        let packet = initiator_send
            .encrypt_packet(b"payload", b"aad", false)
            .expect("packet encrypts");
        assert_eq!(
            responder_recv.decrypt_packet(b"aad", &packet[LENGTH_FIELD_LEN..LENGTH_FIELD_LEN + 4]),
            Err(V2CryptoError::TruncatedPacket { length: 4 })
        );
        let mut tampered = packet[LENGTH_FIELD_LEN..].to_vec();
        tampered[0] ^= 0x01;
        assert_eq!(
            responder_recv.decrypt_packet(b"aad", &tampered),
            Err(V2CryptoError::Authentication)
        );
        assert_eq!(
            responder_recv.decrypt_packet(b"wrong aad", &packet[LENGTH_FIELD_LEN..]),
            Err(V2CryptoError::Authentication)
        );
        let (ignore, contents) = responder_recv
            .decrypt_packet(b"aad", &packet[LENGTH_FIELD_LEN..])
            .expect("untampered packet still authenticates");
        assert!(!ignore);
        assert_eq!(contents, b"payload");
    }

    #[test]
    fn rejects_contents_beyond_the_length_field_bound() {
        let (mut initiator_send, ..) = fixed_session();
        let oversized = vec![0u8; MAX_CONTENTS_LEN + 1];
        assert_eq!(
            initiator_send.encrypt_packet(&oversized, &[], false),
            Err(V2CryptoError::OversizedContents {
                length: MAX_CONTENTS_LEN + 1,
            })
        );
    }

    #[test]
    fn network_magic_binds_the_key_derivation() {
        let shared = [0x5a_u8; 32];
        let mainnet = derive_session_keys(&shared, Magic::BITCOIN);
        let testnet = derive_session_keys(&shared, Magic::TESTNET4);
        assert_ne!(mainnet.session_id, testnet.session_id);
        assert_ne!(mainnet.initiator_length_key, testnet.initiator_length_key);
    }

    fn decrypt_one_packet(
        receiver: &mut PacketReceiver,
        bytes: &[u8],
        aad: &[u8],
    ) -> (bool, Vec<u8>, usize) {
        let length = receiver.decrypt_length(bytes[..LENGTH_FIELD_LEN].try_into().unwrap());
        let end = LENGTH_FIELD_LEN + HEADER_LEN + length + TAG_LEN;
        let (ignore, contents) = receiver
            .decrypt_packet(aad, &bytes[LENGTH_FIELD_LEN..end])
            .expect("packet authenticates");
        (ignore, contents, end)
    }

    #[test]
    fn initiator_and_responder_machines_complete_against_each_other() {
        let magic = Magic::REGTEST;
        let (mut initiator, first) =
            V2Handshake::initiator(magic, vec![0xaa; 100]).expect("initiator constructs");
        let mut responder =
            V2Handshake::responder(magic, vec![0xbb; 4095]).expect("responder constructs");
        let reply = responder
            .push_bytes(&first)
            .expect("responder consumes key");
        assert!(matches!(reply.event, HandshakeEvent::NeedMoreData));
        assert!(!reply.send.is_empty());
        let mut initiator_session = None;
        let mut initiator_send = Vec::new();
        for (offset, byte) in reply.send.iter().enumerate() {
            let step = initiator
                .push_bytes(std::slice::from_ref(byte))
                .expect("initiator consumes dribbled reply");
            initiator_send.extend_from_slice(&step.send);
            match step.event {
                HandshakeEvent::NeedMoreData => {
                    assert!(offset + 1 < reply.send.len(), "must complete on final byte");
                }
                HandshakeEvent::Complete(session) => {
                    assert_eq!(offset + 1, reply.send.len());
                    initiator_session = Some(session);
                }
                HandshakeEvent::PeerSpeaksV1 { .. } => panic!("initiator cannot see v1"),
            }
        }
        let mut initiator_session = initiator_session.expect("initiator completed");
        let responder_step = responder
            .push_bytes(&initiator_send)
            .expect("responder consumes terminator and version packet");
        let HandshakeEvent::Complete(mut responder_session) = responder_step.event else {
            panic!("responder must complete");
        };
        assert!(responder_step.send.is_empty());
        assert_eq!(initiator_session.session_id, responder_session.session_id);
        assert!(initiator_session.leftover.is_empty());
        assert!(responder_session.leftover.is_empty());
        let to_responder = initiator_session
            .sender
            .encrypt_packet(b"ping", &[], false)
            .expect("application packet encrypts");
        let (ignore, contents, consumed) =
            decrypt_one_packet(&mut responder_session.receiver, &to_responder, &[]);
        assert!(!ignore);
        assert_eq!(contents, b"ping");
        assert_eq!(consumed, to_responder.len());
        let to_initiator = responder_session
            .sender
            .encrypt_packet(b"pong", &[], false)
            .expect("application packet encrypts");
        let (ignore, contents, _) =
            decrypt_one_packet(&mut initiator_session.receiver, &to_initiator, &[]);
        assert!(!ignore);
        assert_eq!(contents, b"pong");
        assert!(
            matches!(initiator.push_bytes(&[]), Err(V2HandshakeError::Finished)),
            "a completed machine refuses further input"
        );
    }

    /// Builds a hand-driven responder flight so decoys, leftover bytes, and
    /// garbage tampering can be exercised against the initiator machine.
    fn manual_responder_flight(
        initiator_first: &[u8],
        peer_garbage: &[u8],
    ) -> (Vec<u8>, Vec<u8>, [u8; 32]) {
        let secp = bitcoin::secp256k1::Secp256k1::new();
        let peer_secret = SecretKey::from_slice(&[0x77; 32]).expect("static key");
        let peer_ellswift = ElligatorSwift::from_seckey(&secp, peer_secret, Some([0x88; 32]));
        let initiator_ellswift = ElligatorSwift::from_array(
            initiator_first[..ELLSWIFT_LEN]
                .try_into()
                .expect("initiator key present"),
        );
        let shared = v2_shared_secret(
            &peer_secret,
            peer_ellswift,
            initiator_ellswift,
            Role::Responder,
        );
        let keys = derive_session_keys(&shared, Magic::SIGNET);
        let responder_terminator = keys.responder_garbage_terminator;
        let session_id = keys.session_id;
        let (mut peer_sender, _) = keys.into_packet_ciphers(Role::Responder);
        let mut flight = peer_ellswift.to_array().to_vec();
        flight.extend_from_slice(peer_garbage);
        flight.extend_from_slice(&responder_terminator);
        flight.extend_from_slice(
            &peer_sender
                .encrypt_packet(b"decoy contents", peer_garbage, true)
                .expect("decoy encrypts"),
        );
        flight.extend_from_slice(
            &peer_sender
                .encrypt_packet(&[], &[], false)
                .expect("version packet encrypts"),
        );
        let leftover_packet = peer_sender
            .encrypt_packet(b"first application packet", &[], false)
            .expect("application packet encrypts");
        (flight, leftover_packet, session_id)
    }

    #[test]
    fn initiator_skips_decoys_and_returns_leftover_application_bytes() {
        let (mut initiator, first) =
            V2Handshake::initiator(Magic::SIGNET, Vec::new()).expect("initiator constructs");
        let peer_garbage = vec![0xcc; 333];
        let (flight, leftover_packet, session_id) = manual_responder_flight(&first, &peer_garbage);
        let mut input = flight;
        input.extend_from_slice(&leftover_packet);
        let step = initiator.push_bytes(&input).expect("handshake completes");
        let HandshakeEvent::Complete(mut session) = step.event else {
            panic!("initiator must complete");
        };
        assert_eq!(session.session_id, session_id);
        assert_eq!(session.leftover, leftover_packet);
        let leftover = std::mem::take(&mut session.leftover);
        let (ignore, contents, consumed) =
            decrypt_one_packet(&mut session.receiver, &leftover, &[]);
        assert!(!ignore);
        assert_eq!(contents, b"first application packet");
        assert_eq!(consumed, leftover.len());
    }

    #[test]
    fn tampered_peer_garbage_fails_the_version_packet() {
        let (mut initiator, first) =
            V2Handshake::initiator(Magic::SIGNET, Vec::new()).expect("initiator constructs");
        let peer_garbage = vec![0xcc; 333];
        let (mut flight, _, _) = manual_responder_flight(&first, &peer_garbage);
        flight[ELLSWIFT_LEN + 5] ^= 0x01;
        assert!(matches!(
            initiator.push_bytes(&flight),
            Err(V2HandshakeError::Crypto(V2CryptoError::Authentication))
        ));
        assert!(matches!(
            initiator.push_bytes(&[]),
            Err(V2HandshakeError::Finished)
        ));
    }

    #[test]
    fn responder_detects_a_v1_peer_and_preserves_its_bytes() {
        let mut responder =
            V2Handshake::responder(Magic::BITCOIN, Vec::new()).expect("responder constructs");
        let mut v1_bytes = Magic::BITCOIN.to_bytes().to_vec();
        v1_bytes.extend_from_slice(b"version\0\0\0\0\0");
        let step = responder
            .push_bytes(&v1_bytes[..10])
            .expect("partial prefix stays pending");
        assert!(matches!(step.event, HandshakeEvent::NeedMoreData));
        v1_bytes.extend_from_slice(b"remaining v1 payload");
        let step = responder
            .push_bytes(&v1_bytes[10..])
            .expect("full prefix is recognized");
        let HandshakeEvent::PeerSpeaksV1 { received } = step.event else {
            panic!("responder must report a v1 peer");
        };
        assert_eq!(received, v1_bytes);
        assert!(step.send.is_empty(), "nothing may be sent to a v1 peer");
        assert!(matches!(
            responder.push_bytes(&[]),
            Err(V2HandshakeError::Finished)
        ));
    }

    #[test]
    fn missing_garbage_terminator_fails_closed() {
        let (mut initiator, _) =
            V2Handshake::initiator(Magic::BITCOIN, Vec::new()).expect("initiator constructs");
        let step = initiator
            .push_bytes(&[0x5c; ELLSWIFT_LEN])
            .expect("peer key is consumed");
        assert!(matches!(step.event, HandshakeEvent::NeedMoreData));
        assert!(matches!(
            initiator.push_bytes(&[0x5d; MAX_GARBAGE_LEN + GARBAGE_TERMINATOR_LEN + 1]),
            Err(V2HandshakeError::GarbageTerminatorMissing { limit })
                if limit == MAX_GARBAGE_LEN + GARBAGE_TERMINATOR_LEN
        ));
    }

    #[test]
    fn oversized_garbage_is_rejected_at_construction() {
        assert!(matches!(
            V2Handshake::initiator(Magic::BITCOIN, vec![0; MAX_GARBAGE_LEN + 1]),
            Err(V2HandshakeError::OversizedGarbage { length }) if length == MAX_GARBAGE_LEN + 1
        ));
        assert!(matches!(
            V2Handshake::responder(Magic::BITCOIN, vec![0; MAX_GARBAGE_LEN + 1]),
            Err(V2HandshakeError::OversizedGarbage { length }) if length == MAX_GARBAGE_LEN + 1
        ));
    }
}
