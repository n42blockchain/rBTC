//! Explicit live gates for the wallet queue's production private-broadcast path.
//! Synthetic regtest transactions reach only receivers owned by these tests.

use super::*;

const LIVE_DEADLINE: Duration = Duration::from_secs(240);
static ROUTER: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn endpoint(name: &str) -> SocketAddr {
    std::env::var(name)
        .unwrap_or_else(|_| panic!("set {name} to run this live gate"))
        .parse()
        .unwrap_or_else(|_| panic!("{name} must be an IP:PORT address"))
}

async fn receive_private_transaction(
    stream: tokio::net::TcpStream,
) -> (Transaction, V1Transport<tokio::net::TcpStream>) {
    let mut peer = V1Transport::new(stream, Network::Regtest.magic());
    peer.handshake_inbound(peer_version(0x7072_6976_6174_6501))
        .await
        .unwrap();
    let mut delivered = None;
    loop {
        match peer.read_message().await.unwrap().into_payload() {
            NetworkMessage::Tx(transaction) => delivered = Some(transaction),
            NetworkMessage::Ping(nonce) => {
                peer.write_message(NetworkMessage::Pong(nonce))
                    .await
                    .unwrap();
                if let Some(transaction) = delivered {
                    // The JoinHandle result owns the stream until the wallet
                    // observes its pong. Closing this mock peer immediately
                    // could cancel the router's still-buffered response.
                    return (transaction, peer);
                }
            }
            _ => {}
        }
    }
}

#[allow(clippy::too_many_lines)]
async fn exercise_wallet_queue(
    directory: &TempDir,
    context: PrivateBroadcastContext,
    waves: usize,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let observer = tokio::spawn(async move {
        let (mut peer, _) = accept_peer(listener, peer_version(0x636c_6561_726e_6574)).await;
        // Keep observing for the entire live wave, including descriptor/tunnel
        // delays and retries. A short quiet interval must never count as EOF.
        loop {
            match peer.read_message().await {
                Ok(message) => match message.into_payload() {
                    NetworkMessage::Tx(_) => panic!("private transaction leaked to clearnet"),
                    NetworkMessage::Ping(nonce) => {
                        peer.write_message(NetworkMessage::Pong(nonce))
                            .await
                            .unwrap();
                    }
                    _ => {}
                },
                Err(rbtc::p2p::P2pError::Io(error))
                    if error.kind() == std::io::ErrorKind::UnexpectedEof =>
                {
                    break;
                }
                Err(error) => panic!("clearnet observation ended before clean EOF: {error}"),
            }
        }
    });
    let mut session = connect_outbound(
        address,
        Network::Regtest.magic(),
        1,
        "/rbtcd:live-private-test/".to_owned(),
        0,
    )
    .await
    .unwrap();
    let (broadcast_sender, broadcast_receiver) = mpsc::channel(WALLET_BROADCAST_QUEUE_CAPACITY);
    let (standby_relay, mut standby_receiver) = broadcast::channel(WALLET_BROADCAST_QUEUE_CAPACITY);
    let private_broadcast = std::sync::OnceLock::new();
    private_broadcast
        .set(context)
        .unwrap_or_else(|_| unreachable!());
    let wallet = WalletApiRuntime {
        wallet: Arc::new(
            EmbeddedWallet::open_or_create(
                directory.path().join("wallet.sqlite"),
                RECEIVE_DESCRIPTOR,
                CHANGE_DESCRIPTOR,
                Network::Regtest,
            )
            .unwrap(),
        ),
        token: LocalAuthToken::new("a".repeat(32)).unwrap(),
        token_path: directory.path().join("wallet.token"),
        audit: AuthorizationAuditLog::open(directory.path().join(API_AUDIT_FILE)).unwrap(),
        scan: WalletScanConfig {
            gap_limit: DEFAULT_WALLET_GAP_LIMIT,
            birthday_height: 0,
        },
        broadcast_sender,
        broadcast_receiver: Arc::new(tokio::sync::Mutex::new(broadcast_receiver)),
        pending_broadcast: Arc::new(tokio::sync::Mutex::new(None)),
        compact_candidates: CompactTransactionCandidates::default(),
        rebroadcast: Arc::new(
            RedbRebroadcastStore::open(directory.path().join("rebroadcast.redb"), Network::Regtest)
                .unwrap(),
        ),
        private_broadcast,
    };
    let transaction = wallet_broadcast_transaction();
    wallet
        .rebroadcast
        .enqueue(&transaction, u64::from(unix_time().unwrap()))
        .unwrap();
    for _ in 0..waves {
        let (result, mut completion) = tokio::sync::oneshot::channel();
        wallet
            .broadcast_sender
            .send(WalletBroadcastRequest {
                transaction: transaction.clone(),
                fee_sats: 1_000,
                result,
            })
            .await
            .unwrap();
        timeout(LIVE_DEADLINE, async {
            loop {
                wait_for_peer_poll(
                    &mut session,
                    Duration::from_millis(10),
                    Some(&wallet),
                    &standby_relay,
                    &Notify::new(),
                )
                .await
                .unwrap();
                match completion.try_recv() {
                    Ok(result) => {
                        assert_eq!(result, Ok(()));
                        break;
                    }
                    Err(tokio::sync::oneshot::error::TryRecvError::Empty) => {
                        assert!(wallet.pending_broadcast.lock().await.is_some());
                        assert!(matches!(
                            standby_receiver.try_recv(),
                            Err(broadcast::error::TryRecvError::Empty)
                        ));
                        tokio::time::sleep(Duration::from_secs(5)).await;
                    }
                    Err(error) => panic!("private broadcast completion lost: {error}"),
                }
            }
        })
        .await
        .expect("a live private wave must deliver before the deadline");
    }
    assert!(wallet.pending_broadcast.lock().await.is_none());
    assert_eq!(wallet.compact_candidates.snapshot(), vec![transaction]);
    assert!(
        wallet
            .rebroadcast
            .due(u64::from(unix_time().unwrap()), 1)
            .unwrap()
            .is_empty()
    );
    assert!(matches!(
        standby_receiver.try_recv(),
        Err(broadcast::error::TryRecvError::Empty)
    ));
    drop(session);
    timeout(Duration::from_secs(10), observer)
        .await
        .unwrap()
        .unwrap();
    println!(
        "wallet queue completed; clearnet observer reached EOF without Tx; standby relay empty"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires RBTC_TOR_CONTROL, RBTC_TOR_COOKIE and RBTC_TOR_SOCKS for a real Tor daemon"]
async fn live_private_broadcast_over_tor_preserves_clearnet_isolation() {
    let _router = ROUTER.lock().await;
    let directory = TempDir::new().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut controller = TorController::connect(
        endpoint("RBTC_TOR_CONTROL"),
        &PathBuf::from(std::env::var_os("RBTC_TOR_COOKIE").expect("set RBTC_TOR_COOKIE")),
        crate::tor_control::TorControlConfig::default(),
    )
    .await
    .unwrap();
    let published = controller
        .add_onion_service(8333, listener.local_addr().unwrap(), None)
        .await
        .unwrap();
    let receiver = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        receive_private_transaction(stream).await
    });
    let peers = Arc::new(
        RedbPeerStore::open(directory.path().join("peers.redb"), Network::Regtest).unwrap(),
    );
    let now = unix_time().unwrap();
    peers
        .insert_discovered_onion(
            &[rbtc::p2p::OnionPeerAddress {
                onion: published.address.clone(),
                services: ServiceFlags::NETWORK | ServiceFlags::WITNESS,
                last_seen: now - 1,
            }],
            now,
        )
        .unwrap();
    exercise_wallet_queue(
        &directory,
        PrivateBroadcastContext {
            proxy: Some(endpoint("RBTC_TOR_SOCKS")),
            i2p_sam: None,
            peer_store: Some(peers),
            message_start: Network::Regtest.magic(),
        },
        1,
    )
    .await;
    let (payload, _peer) = timeout(Duration::from_secs(30), receiver)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(payload, wallet_broadcast_transaction());
    controller.remove_onion_service(&published).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires RBTC_I2P_SAM for a real I2P router"]
async fn live_private_broadcast_over_i2p_preserves_clearnet_isolation() {
    let _router = ROUTER.lock().await;
    let directory = TempDir::new().unwrap();
    let bridge = endpoint("RBTC_I2P_SAM");
    let serving = Arc::new(
        I2pSamSession::create(
            bridge,
            &format!("rbtc-pvt-rx-{:016x}", rand::random::<u64>()),
            None,
            crate::i2p_sam::I2pSamConfig {
                timeout: LIVE_DEADLINE,
            },
        )
        .await
        .unwrap(),
    );
    let destination = serving.address().clone();
    let acceptor = Arc::clone(&serving);
    let receiver = tokio::spawn(async move {
        let mut waves = Vec::new();
        for _ in 0..2 {
            let accepted = acceptor.accept_stream().await.unwrap();
            waves.push((
                accepted.peer,
                receive_private_transaction(accepted.stream).await,
            ));
        }
        waves
    });
    let peers = Arc::new(
        RedbPeerStore::open(directory.path().join("peers.redb"), Network::Regtest).unwrap(),
    );
    let now = unix_time().unwrap();
    peers
        .insert_discovered_i2p(
            &[rbtc::p2p::I2pPeerAddress {
                i2p: destination.clone(),
                services: ServiceFlags::NETWORK | ServiceFlags::WITNESS,
                last_seen: now - 1,
            }],
            now,
        )
        .unwrap();
    exercise_wallet_queue(
        &directory,
        PrivateBroadcastContext {
            proxy: None,
            i2p_sam: Some(bridge),
            peer_store: Some(peers),
            message_start: Network::Regtest.magic(),
        },
        2,
    )
    .await;
    let waves = timeout(Duration::from_secs(30), receiver)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(waves.len(), 2);
    assert_ne!(
        waves[0].0, waves[1].0,
        "each private wave uses a fresh destination"
    );
    for (sender, (transaction, _peer)) in waves {
        assert_ne!(sender, destination);
        assert_eq!(transaction, wallet_broadcast_transaction());
    }
}
