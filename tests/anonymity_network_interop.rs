//! Optional interoperability tests against real Tor and I2P daemons.
//!
//! These tests are ignored by default because they need external daemons and
//! live anonymity-network circuits. Everything else covering these code paths
//! runs against in-process mocks, which cannot catch a divergence between the
//! protocol as this repository models it and as the daemons actually speak it
//! — reply framing, field order, descriptor propagation delay, and stream
//! semantics after a successful connect are exactly where that divergence
//! would appear.
//!
//! Tor, publishing an onion service and dialling it back through SOCKS5:
//!
//! ```bash
//! # torrc: ControlPort 9051 / CookieAuthentication 1 / SocksPort 9050
//! RBTC_TOR_CONTROL=127.0.0.1:9051 \
//! RBTC_TOR_COOKIE=/opt/homebrew/var/lib/tor/control_auth_cookie \
//! RBTC_TOR_SOCKS=127.0.0.1:9050 \
//!   cargo test --release --all-features --test anonymity_network_interop \
//!   -- --ignored --nocapture
//! ```
//!
//! I2P, creating a SAM session and proving the destination is stable:
//!
//! ```bash
//! # i2pd: sam.enabled = true (default bridge 127.0.0.1:7656)
//! RBTC_I2P_SAM=127.0.0.1:7656 \
//!   cargo test --release --all-features --test anonymity_network_interop \
//!   -- --ignored --nocapture
//! ```
//!
//! Each test reports its required variables and fails with that message when
//! they are absent, so a partially configured run states what is missing
//! instead of silently passing.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use bitcoin::{
    Network,
    p2p::{Address, ServiceFlags, message::NetworkMessage, message_network::VersionMessage},
};
use rbtc::i2p_sam::{I2pSamConfig, I2pSamSession};
use rbtc::p2p::{OnionAddress, ProxyTarget, V1Transport, connect_proxied_target};
use rbtc::tor_control::{TorControlConfig, TorController};
use tokio::net::TcpListener;

/// Deadline for an onion service descriptor to become reachable.
///
/// Tor publishes the descriptor to the hash ring and the client then fetches
/// it; a cold daemon regularly needs tens of seconds before the first
/// successful connect.
const DESCRIPTOR_TIMEOUT: Duration = Duration::from_secs(180);
/// Delay between dial attempts while the descriptor propagates.
const DIAL_RETRY_DELAY: Duration = Duration::from_secs(5);
/// Deadline for one SAM session creation, which builds tunnels first.
const SAM_SESSION_TIMEOUT: Duration = Duration::from_secs(180);

fn required_socket(name: &str) -> SocketAddr {
    let value = std::env::var(name)
        .unwrap_or_else(|_| panic!("{name} must be set to run this interoperability test"));
    value
        .parse()
        .unwrap_or_else(|_| panic!("{name} must be an IP:PORT socket address, got {value:?}"))
}

fn required_path(name: &str) -> PathBuf {
    PathBuf::from(
        std::env::var(name)
            .unwrap_or_else(|_| panic!("{name} must be set to run this interoperability test")),
    )
}

/// Builds the `version` message the accepting side of the test sends.
fn responder_version(nonce: u64) -> VersionMessage {
    let receiver: SocketAddr = "127.0.0.1:8333".parse().expect("static address");
    let sender: SocketAddr = "0.0.0.0:0".parse().expect("static address");
    let mut version = VersionMessage::new(
        ServiceFlags::NETWORK | ServiceFlags::WITNESS,
        0,
        Address::new(&receiver, ServiceFlags::NONE),
        Address::new(&sender, ServiceFlags::NONE),
        nonce,
        "/rbtc:onion-interop/".to_owned(),
        0,
    );
    version.version = 70_016;
    version
}

/// Accepts one connection, completes the inbound v1 handshake, and answers
/// one address request.
///
/// The exchange is deliberately a request/response pair rather than a ping.
/// `PeerSession::read_message` answers keepalive pings internally and only
/// surfaces application messages, so a caller can never observe a ping and a
/// test built on one would block until the peer disconnects.
async fn serve_one_onion_peer(listener: TcpListener) -> Result<String, String> {
    const RESPONDER_NONCE: u64 = 0x5253_504f_4e44_4552;
    let (stream, _) = listener.accept().await.map_err(|error| error.to_string())?;
    let mut peer = V1Transport::new(stream, Network::Bitcoin.magic());
    let remote = peer
        .handshake_inbound(responder_version(RESPONDER_NONCE))
        .await
        .map_err(|error| format!("inbound handshake over onion failed: {error}"))?;
    loop {
        match peer
            .read_message()
            .await
            .map_err(|error| format!("read over onion failed: {error}"))?
            .into_payload()
        {
            NetworkMessage::GetAddr => {
                peer.write_message(NetworkMessage::AddrV2(Vec::new()))
                    .await
                    .map_err(|error| format!("address reply over onion failed: {error}"))?;
                return Ok(remote.user_agent);
            }
            NetworkMessage::Ping(nonce) => {
                peer.write_message(NetworkMessage::Pong(nonce))
                    .await
                    .map_err(|error| format!("pong over onion failed: {error}"))?;
            }
            _ => continue,
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "set RBTC_TOR_CONTROL, RBTC_TOR_COOKIE, and RBTC_TOR_SOCKS to run against a real Tor daemon"]
async fn published_onion_service_is_reachable_through_the_real_tor_network() {
    let control = required_socket("RBTC_TOR_CONTROL");
    let cookie = required_path("RBTC_TOR_COOKIE");
    let socks = required_socket("RBTC_TOR_SOCKS");

    // The service forwards to a listener this test owns, so a successful dial
    // proves a complete circuit rather than only a control-port exchange.
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind local");
    let forward_to = listener.local_addr().expect("local address");
    let responder = tokio::spawn(serve_one_onion_peer(listener));

    let mut controller = TorController::connect(control, &cookie, TorControlConfig::default())
        .await
        .expect("authenticate against the Tor control port");
    let published = controller
        .add_onion_service(8333, forward_to, None)
        .await
        .expect("publish an ephemeral onion service");
    println!("published {} forwarding to {forward_to}", published.address);
    assert_eq!(
        OnionAddress::new(published.address.name(), 8333).expect("the published name validates"),
        published.address,
        "a real Tor service identifier must satisfy the same v3 rules as a learned address"
    );

    // The descriptor needs time to reach the hash ring; retry until the
    // deadline instead of failing on the first attempt.
    let deadline = Instant::now() + DESCRIPTOR_TIMEOUT;
    let mut attempts = 0;
    let session = loop {
        attempts += 1;
        let attempt = connect_proxied_target(
            socks,
            &ProxyTarget::Onion(published.address.clone()),
            Network::Bitcoin.magic(),
            0x000d_d1ce,
            "/rbtc:onion-interop/".to_owned(),
            0,
            false,
        )
        .await;
        match attempt {
            Ok(session) => break session,
            Err(error) if Instant::now() < deadline => {
                println!("attempt {attempts} not yet reachable ({error}); retrying");
                tokio::time::sleep(DIAL_RETRY_DELAY).await;
            }
            Err(error) => panic!(
                "the published service never became reachable within {}s: {error}",
                DESCRIPTOR_TIMEOUT.as_secs()
            ),
        }
    };
    println!("connected through Tor after {attempts} attempt(s)");
    assert_eq!(
        session.remote_version().user_agent,
        "/rbtc:onion-interop/",
        "the circuit must reach this test's own listener"
    );

    // A request/response round trip proves the circuit carries traffic in
    // both directions after negotiation, using an API that surfaces its
    // result to the caller.
    let mut session = session;
    session
        .request_addresses()
        .await
        .expect("address request over onion");
    let learned = session
        .receive_addresses()
        .await
        .expect("address response over onion");
    assert!(
        learned.is_empty(),
        "the responder answers with an empty address set, got {learned:?}"
    );
    let dialler_agent = responder
        .await
        .expect("responder task")
        .expect("the responder completed its exchange");
    assert_eq!(
        dialler_agent, "/rbtc:onion-interop/",
        "the responder must observe this test's own dialler across the circuit"
    );

    controller
        .remove_onion_service(&published)
        .await
        .expect("withdraw the service");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "set RBTC_I2P_SAM to run against a real I2P router"]
async fn sam_sessions_produce_stable_reusable_destinations() {
    let bridge = required_socket("RBTC_I2P_SAM");
    let config = I2pSamConfig {
        timeout: SAM_SESSION_TIMEOUT,
    };

    let first = I2pSamSession::create(bridge, "rbtc-interop-a", None, config)
        .await
        .expect("create a transient SAM session");
    println!("session destination {}", first.address());
    assert!(
        first.address().name().ends_with(".b32.i2p"),
        "a real router must return a BIP155-shaped destination"
    );
    assert!(
        !first.destination_key().is_empty(),
        "a session must expose the key needed to republish its address"
    );

    // Replaying the key must reproduce the same published address, which is
    // what lets a restarted node keep the destination its peers learned.
    let key = first.destination_key().to_owned();
    let address = first.address().clone();
    drop(first);
    let reused = I2pSamSession::create(bridge, "rbtc-interop-b", Some(&key), config)
        .await
        .expect("recreate the session from its stored key");
    assert_eq!(
        reused.address(),
        &address,
        "replaying a destination key must republish the same address"
    );

    // An unreachable destination must fail within the configured deadline
    // rather than hanging, because the scheduler treats a stalled dial as an
    // ordinary peer failure only if it actually returns.
    let unreachable = rbtc::i2p_sam::I2pAddress::from_destination_hash([0x42; 32]);
    let started = Instant::now();
    let outcome = reused.connect_stream(&unreachable).await;
    assert!(
        outcome.is_err(),
        "a destination with no router entry must not appear connectable"
    );
    assert!(
        started.elapsed() <= SAM_SESSION_TIMEOUT + Duration::from_secs(5),
        "a refused dial must respect the configured deadline, took {:?}",
        started.elapsed()
    );
}

/// Proves the SAM bridge is refused on a non-loopback address even when a
/// real router is present, so the guard cannot be bypassed by configuration.
#[tokio::test]
#[ignore = "set RBTC_I2P_SAM to run against a real I2P router"]
async fn sam_bridge_is_refused_on_a_non_loopback_address() {
    let bridge = required_socket("RBTC_I2P_SAM");
    assert!(
        bridge.ip().is_loopback(),
        "RBTC_I2P_SAM must itself be loopback for this check to be meaningful"
    );
    let routable: SocketAddr = "203.0.113.7:7656".parse().expect("static address");
    assert!(
        I2pSamSession::create(routable, "rbtc-interop-c", None, I2pSamConfig::default())
            .await
            .is_err(),
        "a non-loopback bridge must be refused before any connection attempt"
    );
}
