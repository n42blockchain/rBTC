//! Bitcoin Core-compatible ZMQ notification endpoint.
//!
//! This module implements the narrow ZMTP 3.x subset a `PUB` notification
//! socket needs — `NULL` security, `READY` negotiation against `SUB`/`XSUB`
//! peers, prefix subscriptions in both the message and command encodings —
//! in bounded safe Rust rather than binding `libzmq`. Notifications follow
//! Core's wire contract: three frames per message (topic, body, 4-byte
//! little-endian per-topic counter), `hashblock`/`hashtx` bodies in display
//! byte order, `rawblock`/`rawtx` consensus bytes, and the `sequence` topic
//! with its `C`/`D`/`A`/`R` labels and mempool sequence numbers. Slow
//! subscribers lose messages instead of growing node memory: distribution
//! runs over one bounded broadcast queue, every drop is counted, and each
//! peer's subscription table is capped. The endpoint is deliberately
//! publish-only and unauthenticated, so it must only be bound to operator
//! controlled interfaces, exactly like Core's ZMQ ports.

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::Duration;

use bitcoin::consensus::serialize;
use bitcoin::{Block, BlockHash, Transaction, Txid, hashes::Hash};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast;
use tokio::task::{JoinHandle, JoinSet};

/// ZMTP frame flag marking a continuation frame.
const FLAG_MORE: u8 = 0x01;
/// ZMTP frame flag marking an eight-byte length.
const FLAG_LONG: u8 = 0x02;
/// ZMTP frame flag marking a command frame.
const FLAG_COMMAND: u8 = 0x04;
/// Bound on any accepted command or subscription frame.
const MAX_PEER_FRAME_LEN: usize = 1024;
/// Bound on one subscription prefix.
const MAX_SUBSCRIPTION_LEN: usize = 64;
/// Bound on retained subscription prefixes per peer.
const MAX_SUBSCRIPTIONS_PER_PEER: usize = 64;
/// Topics published by the endpoint, in sequence-counter order.
const TOPICS: [&[u8]; 5] = [b"hashblock", b"hashtx", b"rawblock", b"rawtx", b"sequence"];

/// One published notification topic.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NotificationTopic {
    /// 32-byte block hash in display byte order.
    HashBlock,
    /// 32-byte transaction id in display byte order.
    HashTx,
    /// Consensus-serialized block bytes.
    RawBlock,
    /// Consensus-serialized transaction bytes.
    RawTx,
    /// Core's mempool/block sequence stream.
    Sequence,
}

impl NotificationTopic {
    const fn index(self) -> usize {
        match self {
            Self::HashBlock => 0,
            Self::HashTx => 1,
            Self::RawBlock => 2,
            Self::RawTx => 3,
            Self::Sequence => 4,
        }
    }

    const fn as_bytes(self) -> &'static [u8] {
        TOPICS[self.index()]
    }
}

/// Errors from the ZMQ endpoint lifecycle.
#[derive(Debug, Error)]
pub enum ZmqPublisherError {
    /// Binding or accepting on the configured listener failed.
    #[error("zmq endpoint io: {0}")]
    Io(#[from] io::Error),
}

/// Bounds for one ZMQ notification endpoint.
#[derive(Clone, Copy, Debug)]
pub struct ZmqPublisherConfig {
    /// Notifications retained for distribution before the slowest peers
    /// start losing messages. Memory is bounded by this count times the
    /// largest published notification, shared across peers.
    pub queue_capacity: usize,
    /// Deadline for a peer to complete the ZMTP handshake.
    pub handshake_timeout: Duration,
}

impl Default for ZmqPublisherConfig {
    fn default() -> Self {
        Self {
            queue_capacity: 64,
            handshake_timeout: Duration::from_secs(5),
        }
    }
}

struct PublisherShared {
    sender: broadcast::Sender<Arc<Notification>>,
    sequences: [AtomicU32; 5],
    dropped_messages: AtomicU64,
}

struct Notification {
    topic: NotificationTopic,
    body: Vec<u8>,
    sequence: u32,
}

/// Cloneable publication handle used by the node's emission points.
#[derive(Clone)]
pub struct ZmqPublisherHandle {
    shared: Arc<PublisherShared>,
}

impl ZmqPublisherHandle {
    fn publish(&self, topic: NotificationTopic, body: Vec<u8>) {
        let sequence = self.shared.sequences[topic.index()].fetch_add(1, Ordering::Relaxed);
        // A send error only means no subscriber is connected; the per-topic
        // counter still advances, exactly like Core's unconnected PUB socket.
        let _ = self.shared.sender.send(Arc::new(Notification {
            topic,
            body,
            sequence,
        }));
    }

    /// Publishes `hashblock`, `rawblock`, and the `C` sequence label for one
    /// connected block.
    pub fn publish_block_connected(&self, hash: BlockHash, raw_block: Vec<u8>) {
        self.publish(
            NotificationTopic::HashBlock,
            display_order(hash.as_byte_array()),
        );
        self.publish(NotificationTopic::RawBlock, raw_block);
        let mut body = Vec::with_capacity(33);
        body.extend_from_slice(hash.as_byte_array());
        body.push(b'C');
        self.publish(NotificationTopic::Sequence, body);
    }

    /// Publishes the `D` sequence label for one disconnected block.
    pub fn publish_block_disconnected(&self, hash: BlockHash) {
        let mut body = Vec::with_capacity(33);
        body.extend_from_slice(hash.as_byte_array());
        body.push(b'D');
        self.publish(NotificationTopic::Sequence, body);
    }

    /// Publishes `hashtx`, `rawtx`, and the `A` sequence label for one
    /// transaction accepted into the mempool.
    pub fn publish_mempool_transaction(
        &self,
        txid: Txid,
        raw_transaction: Vec<u8>,
        mempool_sequence: u64,
    ) {
        self.publish(
            NotificationTopic::HashTx,
            display_order(txid.as_byte_array()),
        );
        self.publish(NotificationTopic::RawTx, raw_transaction);
        let mut body = Vec::with_capacity(41);
        body.extend_from_slice(txid.as_byte_array());
        body.push(b'A');
        body.extend_from_slice(&mempool_sequence.to_le_bytes());
        self.publish(NotificationTopic::Sequence, body);
    }

    /// Publishes the `R` sequence label for one transaction removed from the
    /// mempool for a reason other than block inclusion.
    pub fn publish_mempool_removal(&self, txid: Txid, mempool_sequence: u64) {
        let mut body = Vec::with_capacity(41);
        body.extend_from_slice(txid.as_byte_array());
        body.push(b'R');
        body.extend_from_slice(&mempool_sequence.to_le_bytes());
        self.publish(NotificationTopic::Sequence, body);
    }

    /// Returns notifications lost to slow or absent subscribers so far.
    #[must_use]
    pub fn dropped_messages(&self) -> u64 {
        self.shared.dropped_messages.load(Ordering::Relaxed)
    }
}

/// Node-side notification facade pairing the endpoint handle with the
/// global mempool sequence counter Core's `sequence` topic requires.
///
/// `R` labels cover expiry, BIP125 replacement, and capacity eviction;
/// transactions removed because a block confirmed them are deliberately not
/// published, matching Core's `sequence` contract.
#[derive(Clone)]
pub struct ZmqNotifier {
    handle: ZmqPublisherHandle,
    mempool_sequence: Arc<AtomicU64>,
}

impl ZmqNotifier {
    /// Wraps a publication handle with a fresh mempool sequence counter.
    #[must_use]
    pub fn new(handle: ZmqPublisherHandle) -> Self {
        Self {
            handle,
            mempool_sequence: Arc::new(AtomicU64::new(1)),
        }
    }

    /// Publishes one durably connected block.
    pub fn block_connected(&self, block: &Block) {
        self.handle
            .publish_block_connected(block.block_hash(), serialize(block));
    }

    /// Publishes one disconnected stale block.
    pub fn block_disconnected(&self, hash: BlockHash) {
        self.handle.publish_block_disconnected(hash);
    }

    /// Publishes one transaction newly accepted into the mempool.
    pub fn transaction_accepted(&self, transaction: &Transaction) {
        let sequence = self.mempool_sequence.fetch_add(1, Ordering::Relaxed);
        self.handle.publish_mempool_transaction(
            transaction.compute_txid(),
            serialize(transaction),
            sequence,
        );
    }

    /// Publishes one transaction removed from the mempool for a reason other
    /// than block inclusion.
    pub fn transaction_removed(&self, txid: Txid) {
        let sequence = self.mempool_sequence.fetch_add(1, Ordering::Relaxed);
        self.handle.publish_mempool_removal(txid, sequence);
    }
}

/// One bound ZMQ notification endpoint.
pub struct ZmqPublisher {
    handle: ZmqPublisherHandle,
    local_addr: SocketAddr,
    acceptor: JoinHandle<()>,
}

impl ZmqPublisher {
    /// Binds the endpoint and starts its bounded acceptor.
    ///
    /// # Errors
    ///
    /// Returns an error when the listener cannot bind.
    pub async fn bind(
        listen: SocketAddr,
        config: ZmqPublisherConfig,
    ) -> Result<Self, ZmqPublisherError> {
        let listener = TcpListener::bind(listen).await?;
        let local_addr = listener.local_addr()?;
        let shared = Arc::new(PublisherShared {
            sender: broadcast::Sender::new(config.queue_capacity.max(1)),
            sequences: [const { AtomicU32::new(0) }; 5],
            dropped_messages: AtomicU64::new(0),
        });
        let handle = ZmqPublisherHandle {
            shared: Arc::clone(&shared),
        };
        let acceptor = tokio::spawn(accept_loop(listener, shared, config));
        Ok(Self {
            handle,
            local_addr,
            acceptor,
        })
    }

    /// Returns the cloneable publication handle.
    #[must_use]
    pub fn handle(&self) -> ZmqPublisherHandle {
        self.handle.clone()
    }

    /// Returns the bound listener address.
    #[must_use]
    pub const fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Stops accepting and drops every subscriber connection.
    pub fn shutdown(self) {
        self.acceptor.abort();
    }
}

impl Drop for ZmqPublisher {
    fn drop(&mut self) {
        self.acceptor.abort();
    }
}

async fn accept_loop(
    listener: TcpListener,
    shared: Arc<PublisherShared>,
    config: ZmqPublisherConfig,
) {
    let mut connections: JoinSet<()> = JoinSet::new();
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let Ok((stream, _)) = accepted else { continue };
                let receiver = shared.sender.subscribe();
                let shared = Arc::clone(&shared);
                connections.spawn(async move {
                    let _ = serve_subscriber(stream, receiver, shared, config).await;
                });
            }
            Some(_) = connections.join_next(), if !connections.is_empty() => {}
        }
    }
}

async fn serve_subscriber(
    mut stream: TcpStream,
    mut receiver: broadcast::Receiver<Arc<Notification>>,
    shared: Arc<PublisherShared>,
    config: ZmqPublisherConfig,
) -> Result<(), io::Error> {
    tokio::time::timeout(config.handshake_timeout, zmtp_handshake(&mut stream))
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "zmtp handshake timed out"))??;
    let (mut read_half, mut write_half) = stream.into_split();
    // Peer frames are parsed on their own task because `read_frame` is not
    // cancel-safe inside `select!`; the bounded channel keeps the peer's
    // subscription traffic from outrunning frame application.
    let (frame_sender, mut frames) = tokio::sync::mpsc::channel::<(u8, Vec<u8>)>(8);
    let reader = tokio::spawn(async move {
        loop {
            match read_frame(&mut read_half, MAX_PEER_FRAME_LEN).await {
                Ok(frame) => {
                    if frame_sender.send(frame).await.is_err() {
                        return;
                    }
                }
                Err(_) => return,
            }
        }
    });
    let mut subscriptions: Vec<Vec<u8>> = Vec::new();
    let result = loop {
        tokio::select! {
            frame = frames.recv() => {
                match frame {
                    Some((flags, body)) => {
                        if let Err(error) = apply_peer_frame(flags, &body, &mut subscriptions) {
                            break Err(error);
                        }
                    }
                    None => break Ok(()),
                }
            }
            notification = receiver.recv() => {
                match notification {
                    Ok(notification) => {
                        let topic = notification.topic.as_bytes();
                        if subscriptions
                            .iter()
                            .any(|prefix| topic.starts_with(prefix))
                        {
                            if let Err(error) =
                                write_notification(&mut write_half, &notification).await
                            {
                                break Err(error);
                            }
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(missed)) => {
                        shared
                            .dropped_messages
                            .fetch_add(missed, Ordering::Relaxed);
                    }
                    Err(broadcast::error::RecvError::Closed) => break Ok(()),
                }
            }
        }
    };
    reader.abort();
    result
}

/// Applies one post-handshake peer frame: a subscription message
/// (`0x01`/`0x00` prefix), a `SUBSCRIBE`/`CANCEL` command, or an ignored
/// `PING`-class command.
fn apply_peer_frame(
    flags: u8,
    body: &[u8],
    subscriptions: &mut Vec<Vec<u8>>,
) -> Result<(), io::Error> {
    let (subscribe, prefix) = if flags & FLAG_COMMAND == 0 {
        match body.split_first() {
            Some((1, prefix)) => (true, prefix),
            Some((0, prefix)) => (false, prefix),
            _ => return Ok(()),
        }
    } else if let Some(prefix) = command_body(body, b"SUBSCRIBE") {
        (true, prefix)
    } else if let Some(prefix) = command_body(body, b"CANCEL") {
        (false, prefix)
    } else {
        return Ok(());
    };
    if prefix.len() > MAX_SUBSCRIPTION_LEN {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "subscription prefix exceeds the accepted bound",
        ));
    }
    if subscribe {
        if !subscriptions.iter().any(|existing| existing == prefix) {
            if subscriptions.len() >= MAX_SUBSCRIPTIONS_PER_PEER {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "subscription count exceeds the accepted bound",
                ));
            }
            subscriptions.push(prefix.to_vec());
        }
    } else {
        subscriptions.retain(|existing| existing != prefix);
    }
    Ok(())
}

fn command_body<'body>(body: &'body [u8], name: &[u8]) -> Option<&'body [u8]> {
    let (length, rest) = body.split_first()?;
    let (command, payload) = rest.split_at_checked(usize::from(*length))?;
    (command == name).then_some(payload)
}

async fn zmtp_handshake(stream: &mut TcpStream) -> Result<(), io::Error> {
    let mut greeting = [0u8; 64];
    greeting[0] = 0xff;
    greeting[9] = 0x7f;
    greeting[10] = 3;
    greeting[11] = 1;
    greeting[12..16].copy_from_slice(b"NULL");
    stream.write_all(&greeting).await?;
    stream.flush().await?;
    let mut peer_greeting = [0u8; 64];
    stream.read_exact(&mut peer_greeting).await?;
    if peer_greeting[0] != 0xff || peer_greeting[9] & 0x01 != 0x01 || peer_greeting[10] < 3 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "peer is not a ZMTP 3.x endpoint",
        ));
    }
    if &peer_greeting[12..16] != b"NULL" || peer_greeting[16..32].iter().any(|byte| *byte != 0) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "peer requested an unsupported security mechanism",
        ));
    }
    let mut ready = Vec::new();
    ready.push(5u8);
    ready.extend_from_slice(b"READY");
    append_metadata(&mut ready, b"Socket-Type", b"PUB");
    write_frame(stream, FLAG_COMMAND, &ready).await?;
    let (flags, body) = read_frame(stream, MAX_PEER_FRAME_LEN).await?;
    if flags & FLAG_COMMAND == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "peer sent data before completing the handshake",
        ));
    }
    let Some(metadata) = command_body(&body, b"READY") else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "peer did not answer with READY",
        ));
    };
    let socket_type = metadata_value(metadata, b"Socket-Type").ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidData, "peer omitted its socket type")
    })?;
    if socket_type != b"SUB" && socket_type != b"XSUB" {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "only SUB and XSUB peers may connect to a PUB endpoint",
        ));
    }
    Ok(())
}

fn append_metadata(buffer: &mut Vec<u8>, name: &[u8], value: &[u8]) {
    buffer.push(u8::try_from(name.len()).expect("metadata names are short"));
    buffer.extend_from_slice(name);
    buffer.extend_from_slice(
        &u32::try_from(value.len())
            .expect("metadata values are short")
            .to_be_bytes(),
    );
    buffer.extend_from_slice(value);
}

fn metadata_value<'body>(mut metadata: &'body [u8], name: &[u8]) -> Option<&'body [u8]> {
    while let Some((name_length, rest)) = metadata.split_first() {
        let (property, rest) = rest.split_at_checked(usize::from(*name_length))?;
        let (value_length, rest) = rest.split_at_checked(4)?;
        let value_length =
            usize::try_from(u32::from_be_bytes(value_length.try_into().ok()?)).ok()?;
        let (value, rest) = rest.split_at_checked(value_length)?;
        if property.eq_ignore_ascii_case(name) {
            return Some(value);
        }
        metadata = rest;
    }
    None
}

async fn write_notification<S: AsyncWrite + Unpin>(
    stream: &mut S,
    notification: &Notification,
) -> Result<(), io::Error> {
    write_frame(stream, FLAG_MORE, notification.topic.as_bytes()).await?;
    write_frame(stream, FLAG_MORE, &notification.body).await?;
    write_frame(stream, 0, &notification.sequence.to_le_bytes()).await?;
    stream.flush().await
}

async fn write_frame<S: AsyncWrite + Unpin>(
    stream: &mut S,
    flags: u8,
    body: &[u8],
) -> Result<(), io::Error> {
    if let Ok(short) = u8::try_from(body.len()) {
        stream.write_all(&[flags, short]).await?;
    } else {
        stream.write_all(&[flags | FLAG_LONG]).await?;
        let length = u64::try_from(body.len()).expect("frame bodies fit u64");
        stream.write_all(&length.to_be_bytes()).await?;
    }
    stream.write_all(body).await
}

async fn read_frame<S: AsyncRead + Unpin>(
    stream: &mut S,
    max_len: usize,
) -> Result<(u8, Vec<u8>), io::Error> {
    let mut flags = [0u8; 1];
    stream.read_exact(&mut flags).await?;
    let flags = flags[0];
    let length = if flags & FLAG_LONG == 0 {
        let mut short = [0u8; 1];
        stream.read_exact(&mut short).await?;
        usize::from(short[0])
    } else {
        let mut long = [0u8; 8];
        stream.read_exact(&mut long).await?;
        usize::try_from(u64::from_be_bytes(long))
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "oversized frame"))?
    };
    if length > max_len {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "frame exceeds the accepted bound",
        ));
    }
    let mut body = vec![0u8; length];
    stream.read_exact(&mut body).await?;
    Ok((flags & !FLAG_LONG, body))
}

/// Reverses internal hash bytes into RPC display order, matching Core's
/// `hashblock`/`hashtx` payloads.
fn display_order(hash: &[u8]) -> Vec<u8> {
    hash.iter().rev().copied().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn connect_sub(address: SocketAddr, subscriptions: &[&[u8]]) -> TcpStream {
        let mut stream = TcpStream::connect(address).await.expect("connects");
        let mut greeting = [0u8; 64];
        greeting[0] = 0xff;
        greeting[9] = 0x7f;
        greeting[10] = 3;
        greeting[11] = 0;
        greeting[12..16].copy_from_slice(b"NULL");
        stream.write_all(&greeting).await.expect("greeting sends");
        let mut peer_greeting = [0u8; 64];
        stream
            .read_exact(&mut peer_greeting)
            .await
            .expect("server greeting arrives");
        assert_eq!(peer_greeting[10], 3, "server speaks ZMTP 3.x");
        let mut ready = Vec::new();
        ready.push(5u8);
        ready.extend_from_slice(b"READY");
        append_metadata(&mut ready, b"Socket-Type", b"SUB");
        write_frame(&mut stream, FLAG_COMMAND, &ready)
            .await
            .expect("ready sends");
        let (flags, body) = read_frame(&mut stream, MAX_PEER_FRAME_LEN)
            .await
            .expect("server ready arrives");
        assert_ne!(flags & FLAG_COMMAND, 0);
        let metadata = command_body(&body, b"READY").expect("server answers READY");
        assert_eq!(
            metadata_value(metadata, b"Socket-Type").expect("socket type present"),
            b"PUB"
        );
        for prefix in subscriptions {
            let mut subscribe = vec![1u8];
            subscribe.extend_from_slice(prefix);
            write_frame(&mut stream, 0, &subscribe)
                .await
                .expect("subscription sends");
        }
        stream.flush().await.expect("flushes");
        stream
    }

    async fn read_notification(stream: &mut TcpStream) -> (Vec<u8>, Vec<u8>, u32) {
        let (flags, topic) = read_frame(stream, 1 << 22).await.expect("topic frame");
        assert_ne!(flags & FLAG_MORE, 0);
        let (flags, body) = read_frame(stream, 1 << 22).await.expect("body frame");
        assert_ne!(flags & FLAG_MORE, 0);
        let (flags, sequence) = read_frame(stream, 1 << 22).await.expect("sequence frame");
        assert_eq!(flags & FLAG_MORE, 0);
        let sequence = u32::from_le_bytes(sequence.try_into().expect("four-byte sequence"));
        (topic, body, sequence)
    }

    async fn bound_publisher() -> ZmqPublisher {
        ZmqPublisher::bind(
            "127.0.0.1:0".parse().expect("loopback"),
            ZmqPublisherConfig::default(),
        )
        .await
        .expect("endpoint binds")
    }

    #[tokio::test]
    async fn publishes_core_compatible_block_notifications() {
        let publisher = bound_publisher().await;
        let handle = publisher.handle();
        let mut subscriber = connect_sub(publisher.local_addr(), &[b""]).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        let hash = BlockHash::from_byte_array([0xab; 32]);
        handle.publish_block_connected(hash, vec![0x01, 0x02, 0x03]);
        let (topic, body, sequence) = read_notification(&mut subscriber).await;
        assert_eq!(topic, b"hashblock");
        assert_eq!(body, display_order(hash.as_byte_array()));
        assert_eq!(sequence, 0);
        let (topic, body, sequence) = read_notification(&mut subscriber).await;
        assert_eq!(topic, b"rawblock");
        assert_eq!(body, vec![0x01, 0x02, 0x03]);
        assert_eq!(sequence, 0);
        let (topic, body, sequence) = read_notification(&mut subscriber).await;
        assert_eq!(topic, b"sequence");
        assert_eq!(&body[..32], hash.as_byte_array());
        assert_eq!(body[32], b'C');
        assert_eq!(sequence, 0);
        handle.publish_block_disconnected(hash);
        let (topic, body, sequence) = read_notification(&mut subscriber).await;
        assert_eq!(topic, b"sequence");
        assert_eq!(body[32], b'D');
        assert_eq!(sequence, 1, "sequence counters advance per topic");
    }

    #[tokio::test]
    async fn prefix_subscriptions_filter_topics() {
        let publisher = bound_publisher().await;
        let handle = publisher.handle();
        let mut subscriber = connect_sub(publisher.local_addr(), &[b"rawtx"]).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        let txid = Txid::from_byte_array([0x77; 32]);
        handle.publish_mempool_transaction(txid, vec![0xaa, 0xbb], 9);
        let (topic, body, sequence) = read_notification(&mut subscriber).await;
        assert_eq!(topic, b"rawtx", "hashtx and sequence are filtered out");
        assert_eq!(body, vec![0xaa, 0xbb]);
        assert_eq!(sequence, 0);
        handle.publish_mempool_removal(txid, 10);
        handle.publish_mempool_transaction(txid, vec![0xcc], 11);
        let (topic, body, _) = read_notification(&mut subscriber).await;
        assert_eq!(topic, b"rawtx");
        assert_eq!(body, vec![0xcc]);
    }

    #[tokio::test]
    async fn mempool_sequence_labels_match_core() {
        let publisher = bound_publisher().await;
        let handle = publisher.handle();
        let mut subscriber = connect_sub(publisher.local_addr(), &[b"sequence"]).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        let txid = Txid::from_byte_array([0x11; 32]);
        handle.publish_mempool_transaction(txid, vec![0x00], 41);
        let (_, body, _) = read_notification(&mut subscriber).await;
        assert_eq!(&body[..32], txid.as_byte_array());
        assert_eq!(body[32], b'A');
        assert_eq!(body[33..], 41u64.to_le_bytes());
        handle.publish_mempool_removal(txid, 42);
        let (_, body, _) = read_notification(&mut subscriber).await;
        assert_eq!(body[32], b'R');
        assert_eq!(body[33..], 42u64.to_le_bytes());
    }

    #[tokio::test]
    async fn slow_subscribers_lose_messages_instead_of_growing_memory() {
        let publisher = ZmqPublisher::bind(
            "127.0.0.1:0".parse().expect("loopback"),
            ZmqPublisherConfig {
                queue_capacity: 4,
                ..ZmqPublisherConfig::default()
            },
        )
        .await
        .expect("endpoint binds");
        let handle = publisher.handle();
        let mut subscriber = connect_sub(publisher.local_addr(), &[b"hashtx"]).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        for round in 0..64u8 {
            handle.publish_mempool_transaction(
                Txid::from_byte_array([round; 32]),
                vec![round],
                u64::from(round),
            );
        }
        let mut received = Vec::new();
        loop {
            let read = tokio::time::timeout(
                Duration::from_millis(500),
                read_notification(&mut subscriber),
            )
            .await;
            match read {
                Ok((topic, _, sequence)) => {
                    assert_eq!(topic, b"hashtx");
                    received.push(sequence);
                }
                Err(_) => break,
            }
        }
        assert!(
            received.len() < 64,
            "a four-entry queue cannot deliver a 64-message burst"
        );
        assert!(handle.dropped_messages() > 0, "drops are counted");
        let last = *received.last().expect("some notifications arrive");
        assert_eq!(last, 63, "the newest notifications survive");
    }

    #[tokio::test]
    async fn rejects_non_subscriber_socket_types() {
        let publisher = bound_publisher().await;
        let mut stream = TcpStream::connect(publisher.local_addr())
            .await
            .expect("connects");
        let mut greeting = [0u8; 64];
        greeting[0] = 0xff;
        greeting[9] = 0x7f;
        greeting[10] = 3;
        greeting[12..16].copy_from_slice(b"NULL");
        stream.write_all(&greeting).await.expect("greeting sends");
        let mut peer_greeting = [0u8; 64];
        stream
            .read_exact(&mut peer_greeting)
            .await
            .expect("server greeting arrives");
        let mut ready = Vec::new();
        ready.push(5u8);
        ready.extend_from_slice(b"READY");
        append_metadata(&mut ready, b"Socket-Type", b"REQ");
        write_frame(&mut stream, FLAG_COMMAND, &ready)
            .await
            .expect("ready sends");
        let mut probe = [0u8; 1];
        // The server must drop the connection after the READY exchange.
        loop {
            match read_frame(&mut stream, MAX_PEER_FRAME_LEN).await {
                Ok((flags, _)) if flags & FLAG_COMMAND != 0 => {}
                Ok(_) => panic!("a REQ peer must not receive data"),
                Err(_) => break,
            }
        }
        assert!(
            stream.read_exact(&mut probe).await.is_err(),
            "connection is closed"
        );
    }
}
