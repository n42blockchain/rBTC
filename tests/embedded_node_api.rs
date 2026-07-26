//! External-crate acceptance tests for the host-owned node runtime API.

use bitcoin::Network;
use rbtc::node::{NodeBuilder, NodeError, NodeEvent, NodeLifecycle};
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
