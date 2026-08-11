//! End-to-end mining round trip against a running regtest node.
//!
//! `getblocktemplate` and `rbtc.submitblock` each have unit coverage, but the
//! loop between them does not: a template is only useful if a block built from
//! exactly its fields is accepted, and a submission is only useful if the node
//! then reports the tip it claims. This drives both over the authenticated
//! JSON-RPC route of a real node, so the header staging, the prefetch handoff,
//! the ordinary execution path, and the verdict channel are all exercised
//! together rather than in isolation.

use std::net::SocketAddr;
use std::time::Duration;

use bitcoin::Network;
use bitcoin::hex::{DisplayHex, FromHex};
use rbtc::node::{NodeApiConfig, NodeBuilder};
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::time::timeout;

use bitcoin::p2p::{
    Address, ServiceFlags, message::NetworkMessage, message_network::VersionMessage,
};
use rbtc::p2p::V1Transport;

/// Completes the handshake, then keeps answering for the whole test.
///
/// A peer that stops responding after the first exchange is not enough here:
/// a caught-up node re-requests headers and pings on every poll, so a silent
/// peer would end the run before the round trip finished, and the failure
/// would look like a mining defect rather than a harness one.
async fn serve_responsive_regtest_peer(listener: TcpListener) {
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
        "/rbtc:mining-round-trip/".to_owned(),
        0,
    );
    version.version = 70_016;
    peer.write_message(NetworkMessage::Version(version))
        .await
        .unwrap();
    loop {
        let Ok(message) = peer.read_message().await else {
            return;
        };
        let reply = match message.into_payload() {
            NetworkMessage::Verack => Some(NetworkMessage::Verack),
            NetworkMessage::WtxidRelay => Some(NetworkMessage::WtxidRelay),
            NetworkMessage::GetAddr => Some(NetworkMessage::AddrV2(Vec::new())),
            // Always empty: this node produces its own blocks in this test, so
            // the peer must never advance the chain behind the template.
            NetworkMessage::GetHeaders(_) => Some(NetworkMessage::Headers(Vec::new())),
            NetworkMessage::Ping(nonce) => Some(NetworkMessage::Pong(nonce)),
            _ => None,
        };
        if let Some(reply) = reply {
            if peer.write_message(reply).await.is_err() {
                return;
            }
        }
    }
}

/// Owner-only token the node reads and this test presents.
const RPC_TOKEN: &str = "mining-round-trip-token-0123456789abcdef";

/// Posts one JSON-RPC call to the node's authenticated route.
async fn rpc(address: SocketAddr, method: &str, params: serde_json::Value) -> serde_json::Value {
    let body = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": method,
        "params": params,
    })
    .to_string();
    let request = format!(
        "POST /rpc HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer {RPC_TOKEN}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
    stream.write_all(request.as_bytes()).await.unwrap();
    let mut response = Vec::new();
    timeout(Duration::from_secs(10), stream.read_to_end(&mut response))
        .await
        .expect("the RPC route answers")
        .unwrap();
    let text = String::from_utf8(response).expect("the response is text");
    let (_, payload) = text
        .split_once("\r\n\r\n")
        .expect("the response has a body");
    serde_json::from_str(payload.trim()).expect("the body is JSON")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[allow(clippy::too_many_lines)]
async fn a_block_built_from_a_template_is_accepted_and_reported_as_connected() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let remote = listener.local_addr().unwrap();
    let peer = tokio::spawn(serve_responsive_regtest_peer(listener));

    let api_address: SocketAddr = {
        let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        probe.local_addr().unwrap()
    };
    let directory = TempDir::new().unwrap();
    let token_path = directory.path().join("rpc.token");
    std::fs::write(&token_path, RPC_TOKEN).unwrap();
    let handle = NodeBuilder::new(Network::Regtest, directory.path())
        .connect(remote)
        .api(NodeApiConfig {
            listen: api_address,
            rpc_auth_token: Some(token_path),
            wallet: None,
        })
        .launch()
        .unwrap();
    let controller = handle.controller();

    // The node serves templates only once it is caught up on a chainstate it
    // validated itself, so wait for that rather than racing it.
    let template = timeout(Duration::from_secs(30), async {
        loop {
            let response = rpc(api_address, "getblocktemplate", serde_json::json!([])).await;
            if let Some(result) = response.get("result") {
                if !result.is_null() {
                    return result.clone();
                }
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    })
    .await
    .expect("a caught-up regtest node serves a template");

    let genesis = bitcoin::constants::genesis_block(Network::Regtest);
    assert_eq!(template["height"], 1);
    assert_eq!(
        template["previousblockhash"],
        genesis.block_hash().to_string()
    );
    assert_eq!(template["transactions"].as_array().unwrap().len(), 0);

    // Build strictly from the template's own fields: a template that describes
    // a block the node then rejects is worse than no template at all.
    let height = u32::try_from(template["height"].as_u64().unwrap()).unwrap();
    let bits = u32::from_be_bytes(
        <[u8; 4]>::try_from(
            Vec::<u8>::from_hex(template["bits"].as_str().unwrap())
                .unwrap()
                .as_slice(),
        )
        .unwrap(),
    );
    let mut assembly = rbtc::block_assembly::BlockTemplate::regtest(
        genesis.block_hash(),
        height,
        u32::try_from(template["curtime"].as_u64().unwrap()).unwrap(),
    );
    assembly.target = bitcoin::CompactTarget::from_consensus(bits).into();
    assembly.version = i32::try_from(template["version"].as_i64().unwrap()).unwrap();
    let block = rbtc::block_assembly::assemble_block(&assembly).expect("the template block solves");
    assert_eq!(
        block.header.bits.to_consensus(),
        bits,
        "the mined block must carry the difficulty the template asked for"
    );

    let submitted = rpc(
        api_address,
        "rbtc.submitblock",
        serde_json::json!([bitcoin::consensus::serialize(&block).to_lower_hex_string()]),
    )
    .await;
    let result = submitted
        .get("result")
        .unwrap_or_else(|| panic!("submission returned an error: {submitted}"));
    assert_eq!(
        result["connected"], true,
        "the node must report the block it actually connected: {result}"
    );
    assert_eq!(result["hash"], block.block_hash().to_string());

    // The verdict is only trustworthy if the node's own tip agrees with it.
    let mut status = controller.subscribe_status();
    timeout(Duration::from_secs(10), async {
        loop {
            if status
                .borrow_and_update()
                .execution
                .is_some_and(|tip| tip.height == 1)
            {
                break;
            }
            status.changed().await.unwrap();
        }
    })
    .await
    .expect("execution reaches the submitted block");

    let next = rpc(api_address, "getblocktemplate", serde_json::json!([])).await;
    let next = &next["result"];
    assert_eq!(
        next["height"], 2,
        "the following template must build on the block just submitted"
    );
    assert_eq!(next["previousblockhash"], block.block_hash().to_string());

    // A resubmission must not be reported as a fresh connection.
    let again = rpc(
        api_address,
        "rbtc.submitblock",
        serde_json::json!([bitcoin::consensus::serialize(&block).to_lower_hex_string()]),
    )
    .await;
    assert_eq!(
        again["result"]["connected"], false,
        "a block already on the chain is not connected again: {again}"
    );

    controller.request_shutdown();
    timeout(Duration::from_secs(5), handle.wait())
        .await
        .expect("the node stops cleanly")
        .unwrap();
    peer.abort();
}
