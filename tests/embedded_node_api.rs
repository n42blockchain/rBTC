//! External-crate acceptance tests for the host-owned node runtime API.

use bitcoin::{
    Network,
    p2p::{Address, ServiceFlags, message::NetworkMessage, message_network::VersionMessage},
};
use rbtc::node::{
    NodeApiConfig, NodeBuilder, NodeCacheConfig, NodeConfig, NodeDnsSeed, NodeDnsSeedPolicy,
    NodeError, NodeEvent, NodeLifecycle, NodeResourceConfig, NodeStorageConfig,
};
use rbtc::p2p::V1Transport;
use std::{
    future::Future,
    net::SocketAddr,
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};
use tempfile::TempDir;
use tokio::{net::TcpListener, sync::oneshot, task::JoinHandle, time::timeout};

async fn hold_one_connection() -> (SocketAddr, oneshot::Receiver<()>, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let remote = listener.local_addr().unwrap();
    let (accepted, accepted_rx) = oneshot::channel();
    let peer = tokio::spawn(async move {
        let _connection = listener.accept().await.unwrap();
        let _ = accepted.send(());
        std::future::pending::<()>().await;
    });
    (remote, accepted_rx, peer)
}

async fn serve_idle_regtest_node(listener: TcpListener) {
    let (stream, _) = listener.accept().await.unwrap();
    let mut peer = V1Transport::new(stream, Network::Regtest.magic());
    assert!(matches!(
        peer.read_message().await.unwrap().into_payload(),
        NetworkMessage::Version(_)
    ));
    let receiver: SocketAddr = "127.0.0.1:18444".parse().unwrap();
    let sender: SocketAddr = "0.0.0.0:0".parse().unwrap();
    let mut version = VersionMessage::new(
        ServiceFlags::NETWORK | ServiceFlags::WITNESS,
        0,
        Address::new(&receiver, ServiceFlags::NONE),
        Address::new(&sender, ServiceFlags::NONE),
        9001,
        "/rbtc:embedded-test/".to_owned(),
        0,
    );
    version.version = 70_016;
    peer.write_message(NetworkMessage::Version(version))
        .await
        .unwrap();
    assert!(matches!(
        peer.read_message().await.unwrap().into_payload(),
        NetworkMessage::WtxidRelay
    ));
    assert!(matches!(
        peer.read_message().await.unwrap().into_payload(),
        NetworkMessage::SendAddrV2
    ));
    assert!(matches!(
        peer.read_message().await.unwrap().into_payload(),
        NetworkMessage::Verack
    ));
    peer.write_message(NetworkMessage::WtxidRelay)
        .await
        .unwrap();
    peer.write_message(NetworkMessage::Verack).await.unwrap();
    assert!(matches!(
        peer.read_message().await.unwrap().into_payload(),
        NetworkMessage::SendHeaders
    ));
    assert!(matches!(
        peer.read_message().await.unwrap().into_payload(),
        NetworkMessage::SendCmpct(_)
    ));
    assert!(matches!(
        peer.read_message().await.unwrap().into_payload(),
        NetworkMessage::GetAddr
    ));
    peer.write_message(NetworkMessage::AddrV2(Vec::new()))
        .await
        .unwrap();
    assert!(matches!(
        peer.read_message().await.unwrap().into_payload(),
        NetworkMessage::GetHeaders(_)
    ));
    peer.write_message(NetworkMessage::Headers(Vec::new()))
        .await
        .unwrap();
    std::future::pending::<()>().await;
}

#[derive(Default)]
struct CriticalTaskExecutor {
    spawned: AtomicUsize,
}

impl CriticalTaskExecutor {
    fn spawn_critical_task(
        &self,
        future: impl Future<Output = Result<(), NodeError>> + Send + 'static,
    ) -> JoinHandle<Result<(), NodeError>> {
        self.spawned.fetch_add(1, Ordering::Relaxed);
        tokio::spawn(future)
    }
}

#[test]
fn external_host_can_configure_the_public_node_builder() {
    let error = NodeBuilder::new(Network::Regtest, "/tmp/rbtc-embedded-api")
        .ledger_retention(287, 1024 * 1024 * 1024)
        .launch()
        .err()
        .expect("invalid retention must fail before spawning a task");
    assert!(matches!(error, NodeError::InvalidConfig(_)));

    let error = NodeBuilder::new(Network::Regtest, "/tmp/rbtc-embedded-api")
        .launch()
        .err()
        .expect("launch outside a host runtime must fail without panicking");
    assert!(matches!(error, NodeError::MissingRuntime));
}

#[test]
fn external_host_can_validate_and_retain_the_complete_typed_config() {
    let mut config = NodeConfig::new(Network::Regtest, "/tmp/rbtc-typed-external");
    config.dns_seeds =
        NodeDnsSeedPolicy::Custom(vec![NodeDnsSeed::new("seed.example", 18_444).unwrap()]);
    config.cache = NodeCacheConfig {
        active_chainstate_bytes: 256 * 1024 * 1024,
        background_chainstate_bytes: 512 * 1024 * 1024,
        bulk_validation_bytes: 1024 * 1024 * 1024,
    };
    config.resources = NodeResourceConfig {
        automatic_hot_standbys: 4,
        mempool_max_transactions: 4_096,
        mempool_max_bytes: 300 * 1024 * 1024,
        only_net: rbtc::node::NodeOnlyNet::Any,
        proxy: None,
        v2_transport: false,
    };
    config.storage = NodeStorageConfig {
        prune_blocks: 576,
        prune_bytes: 768 * 1024 * 1024,
        minimum_free_bytes: 2 * 1024 * 1024 * 1024,
    };
    config.api = Some(NodeApiConfig {
        listen: "127.0.0.1:0".parse().unwrap(),
        rpc_auth_token: None,
        wallet: None,
    });
    config.validate().unwrap();

    let builder = NodeBuilder::from_config(config.clone());
    assert_eq!(builder.config(), &config);
    assert!(matches!(
        builder
            .launch()
            .err()
            .expect("launch must require a runtime"),
        NodeError::MissingRuntime
    ));
    assert!(NodeDnsSeed::new("bad host", 18_444).is_err());
    assert!(NodeDnsSeed::new("seed.example", 0).is_err());
}

#[tokio::test]
async fn host_controller_stops_the_embedded_task() {
    let (remote, _accepted, peer) = hold_one_connection().await;
    let directory = TempDir::new().unwrap();
    let handle = NodeBuilder::new(Network::Regtest, directory.path())
        .connect(remote)
        .launch()
        .unwrap();
    let controller = handle.controller();
    let mut events = controller.subscribe_events();
    let mut lifecycle = controller.subscribe_lifecycle();
    timeout(Duration::from_secs(2), async {
        while *lifecycle.borrow_and_update() != NodeLifecycle::Running {
            lifecycle.changed().await.unwrap();
        }
    })
    .await
    .expect("host must observe the running lifecycle state");
    let executor = CriticalTaskExecutor::default();
    let critical_task = executor.spawn_critical_task(handle.wait());
    assert_eq!(executor.spawned.load(Ordering::Relaxed), 1);
    controller.request_shutdown();
    assert_eq!(
        timeout(Duration::from_secs(2), async {
            loop {
                let event = events.recv().await.unwrap();
                if event == NodeEvent::ShutdownRequested {
                    break event;
                }
            }
        })
        .await
        .expect("shutdown event must be bounded"),
        NodeEvent::ShutdownRequested
    );
    timeout(Duration::from_secs(2), critical_task)
        .await
        .expect("embedded shutdown must not depend on process signals")
        .unwrap()
        .unwrap();
    assert_eq!(controller.lifecycle(), NodeLifecycle::Stopped);
    assert_eq!(events.recv().await.unwrap(), NodeEvent::Stopped);
    peer.abort();
}

#[tokio::test]
async fn host_can_run_two_isolated_nodes_in_one_runtime() {
    let (first_remote, first_accepted, first_peer) = hold_one_connection().await;
    let (second_remote, second_accepted, second_peer) = hold_one_connection().await;
    let first_directory = TempDir::new().unwrap();
    let second_directory = TempDir::new().unwrap();

    let first = NodeBuilder::new(Network::Regtest, first_directory.path())
        .connect(first_remote)
        .launch()
        .unwrap();
    let second = NodeBuilder::new(Network::Regtest, second_directory.path())
        .connect(second_remote)
        .launch()
        .unwrap();

    timeout(Duration::from_secs(2), async {
        first_accepted.await.unwrap();
        second_accepted.await.unwrap();
    })
    .await
    .expect("both isolated nodes must establish their own peer session");

    first.controller().request_shutdown();
    second.controller().request_shutdown();
    let (first_result, second_result) = tokio::join!(
        timeout(Duration::from_secs(2), first.wait()),
        timeout(Duration::from_secs(2), second.wait()),
    );
    first_result
        .expect("first node must stop independently")
        .unwrap();
    second_result
        .expect("second node must stop independently")
        .unwrap();
    first_peer.abort();
    second_peer.abort();
}

#[tokio::test]
async fn host_observes_typed_peer_header_execution_and_freezer_state() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let remote = listener.local_addr().unwrap();
    let peer = tokio::spawn(serve_idle_regtest_node(listener));
    let directory = TempDir::new().unwrap();
    let handle = NodeBuilder::new(Network::Regtest, directory.path())
        .connect(remote)
        .launch()
        .unwrap();
    let controller = handle.controller();
    let mut status = controller.subscribe_status();
    let mut events = controller.subscribe_events();

    timeout(Duration::from_secs(3), async {
        loop {
            let current = status.borrow_and_update().clone();
            if current.active_peer == Some(remote)
                && current.header.is_some()
                && current.execution.is_some()
            {
                break;
            }
            status.changed().await.unwrap();
        }
    })
    .await
    .expect("typed node status must advance without enabling the HTTP API");

    let current = controller.status();
    assert_eq!(current.network, Network::Regtest);
    assert_eq!(current.active_peer, Some(remote));
    assert_eq!(current.header.unwrap().height, 0);
    assert_eq!(current.execution.unwrap().height, 0);
    assert_eq!(current.freezer.blocks, 0);
    assert!(current.trust.minimum_chainwork_reached);

    let mut saw_peer = false;
    let mut saw_header = false;
    let mut saw_execution = false;
    while !(saw_peer && saw_header && saw_execution) {
        match timeout(Duration::from_secs(2), events.recv())
            .await
            .expect("typed events must be bounded")
            .unwrap()
        {
            NodeEvent::PeerConnected { address } => {
                assert_eq!(address, remote);
                saw_peer = true;
            }
            NodeEvent::HeaderTipChanged { tip } => {
                assert_eq!(tip.height, 0);
                saw_header = true;
            }
            NodeEvent::ExecutionTipChanged { tip } => {
                assert_eq!(tip.height, 0);
                saw_execution = true;
            }
            _ => {}
        }
    }

    controller.request_shutdown();
    timeout(Duration::from_secs(2), handle.wait())
        .await
        .expect("observed node must stop cleanly")
        .unwrap();
    peer.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn host_configured_zmq_endpoint_accepts_a_subscriber() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let remote = listener.local_addr().unwrap();
    let peer = tokio::spawn(serve_idle_regtest_node(listener));
    let zmq_address: SocketAddr = {
        let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        probe.local_addr().unwrap()
    };
    let directory = TempDir::new().unwrap();
    let handle = NodeBuilder::new(Network::Regtest, directory.path())
        .connect(remote)
        .zmq_listen(zmq_address)
        .launch()
        .unwrap();
    let controller = handle.controller();

    let mut stream = timeout(Duration::from_secs(5), async {
        loop {
            match tokio::net::TcpStream::connect(zmq_address).await {
                Ok(stream) => break stream,
                Err(_) => tokio::time::sleep(Duration::from_millis(50)).await,
            }
        }
    })
    .await
    .expect("the configured ZMQ endpoint must start listening");

    let mut greeting = [0u8; 64];
    greeting[0] = 0xff;
    greeting[9] = 0x7f;
    greeting[10] = 3;
    greeting[12..16].copy_from_slice(b"NULL");
    stream.write_all(&greeting).await.unwrap();
    let mut server_greeting = [0u8; 64];
    timeout(
        Duration::from_secs(2),
        stream.read_exact(&mut server_greeting),
    )
    .await
    .expect("server greeting must arrive")
    .unwrap();
    assert_eq!(server_greeting[0], 0xff);
    assert_eq!(server_greeting[10], 3, "the endpoint speaks ZMTP 3.x");
    assert_eq!(&server_greeting[12..16], b"NULL");

    let mut ready = vec![0x04u8, 0u8, 5u8];
    ready.extend_from_slice(b"READY");
    ready.extend_from_slice(&[11u8]);
    ready.extend_from_slice(b"Socket-Type");
    ready.extend_from_slice(&3u32.to_be_bytes());
    ready.extend_from_slice(b"SUB");
    ready[1] = u8::try_from(ready.len() - 2).unwrap();
    stream.write_all(&ready).await.unwrap();

    let mut flags = [0u8; 1];
    timeout(Duration::from_secs(2), stream.read_exact(&mut flags))
        .await
        .expect("server READY must arrive")
        .unwrap();
    assert_eq!(flags[0] & 0x04, 0x04, "the endpoint answers with a command");
    let mut length = [0u8; 1];
    stream.read_exact(&mut length).await.unwrap();
    let mut body = vec![0u8; usize::from(length[0])];
    stream.read_exact(&mut body).await.unwrap();
    assert_eq!(&body[1..6], b"READY");
    let metadata = String::from_utf8_lossy(&body[6..]).into_owned();
    assert!(
        metadata.contains("PUB"),
        "the endpoint must identify as a PUB socket: {metadata:?}"
    );

    controller.request_shutdown();
    timeout(Duration::from_secs(3), handle.wait())
        .await
        .expect("node with a ZMQ endpoint must stop cleanly")
        .unwrap();
    peer.abort();
}
