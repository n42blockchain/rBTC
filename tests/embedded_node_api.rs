//! External-crate acceptance tests for the host-owned node runtime API.

use bitcoin::Network;
use rbtc::node::{NodeBuilder, NodeError};
use std::time::Duration;
use tempfile::TempDir;
use tokio::{net::TcpListener, time::timeout};

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
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let remote = listener.local_addr().unwrap();
    let peer = tokio::spawn(async move {
        let _connection = listener.accept().await.unwrap();
        std::future::pending::<()>().await;
    });
    let directory = TempDir::new().unwrap();
    let handle = NodeBuilder::new(Network::Regtest, directory.path())
        .connect(remote)
        .launch()
        .unwrap();
    let controller = handle.controller();
    controller.request_shutdown();
    timeout(Duration::from_secs(2), handle.wait())
        .await
        .expect("embedded shutdown must not depend on process signals")
        .unwrap();
    peer.abort();
}
