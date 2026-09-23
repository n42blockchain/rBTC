use super::*;

#[derive(Debug, Default)]
struct HeaderPeerProgress {
    requests: std::sync::atomic::AtomicUsize,
    pings: std::sync::atomic::AtomicUsize,
}

async fn next_header_request(
    peer: &mut V1Transport<tokio::net::TcpStream>,
    progress: &HeaderPeerProgress,
) -> bitcoin::p2p::message_blockdata::GetHeadersMessage {
    use std::sync::atomic::Ordering;
    loop {
        match peer.read_message().await.unwrap().into_payload() {
            NetworkMessage::GetHeaders(request) => {
                progress.requests.fetch_add(1, Ordering::Relaxed);
                return request;
            }
            NetworkMessage::Ping(nonce) => {
                progress.pings.fetch_add(1, Ordering::Relaxed);
                peer.write_message(NetworkMessage::Pong(nonce))
                    .await
                    .unwrap();
            }
            other => panic!("expected GetHeaders or replay Ping, received {other:?}"),
        }
    }
}

async fn serve_replay_pings_until(
    peer: &mut V1Transport<tokio::net::TcpStream>,
    mut finished: tokio::sync::oneshot::Receiver<()>,
    progress: &HeaderPeerProgress,
) {
    loop {
        tokio::select! {
            biased;
            _ = &mut finished => return,
            message = peer.read_message() => {
                match message.unwrap().into_payload() {
                    NetworkMessage::Ping(nonce) => {
                        progress.pings.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        peer.write_message(NetworkMessage::Pong(nonce)).await.unwrap();
                    }
                    other => panic!("expected replay Ping after final headers, received {other:?}"),
                }
            }
        }
    }
}

#[tokio::test]
async fn header_replay_keepalive_precedes_locator_without_breaking_peer_protocol() {
    use std::sync::atomic::Ordering;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let remote = listener.local_addr().unwrap();
    let progress = Arc::new(HeaderPeerProgress::default());
    let peer_progress = Arc::clone(&progress);
    let genesis = bitcoin::blockdata::constants::genesis_block(Network::Regtest).block_hash();
    let (finish_peer, finished) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut peer, _) = accept_peer(listener, peer_version(9733)).await;
        let request = next_header_request(&mut peer, &peer_progress).await;
        assert_eq!(request.locator_hashes, vec![genesis]);
        peer.write_message(NetworkMessage::Headers(Vec::new()))
            .await
            .unwrap();
        serve_replay_pings_until(&mut peer, finished, &peer_progress).await;
    });
    let mut peer = connect_outbound(
        remote,
        Network::Regtest.magic(),
        9732,
        "/rbtc:keepalive-test/".to_owned(),
        0,
    )
    .await
    .unwrap();
    let mut keepalive = crate::node::header_sync::ReplayKeepalive::new(
        std::time::Instant::now()
            .checked_sub(Duration::from_secs(20))
            .unwrap(),
    );
    timeout(Duration::from_secs(5), keepalive.tick(Some(&mut peer)))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(progress.pings.load(Ordering::Relaxed), 1);
    assert_eq!(progress.requests.load(Ordering::Relaxed), 0);
    keepalive.tick(Some(&mut peer)).await.unwrap();
    assert_eq!(progress.pings.load(Ordering::Relaxed), 1);
    request_headers(&mut peer, vec![genesis]).await.unwrap();
    assert!(receive_headers(&mut peer).await.unwrap().is_empty());
    let mut keepalive = crate::node::header_sync::ReplayKeepalive::new(
        std::time::Instant::now()
            .checked_sub(Duration::from_secs(20))
            .unwrap(),
    );
    timeout(Duration::from_secs(5), keepalive.tick(Some(&mut peer)))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(progress.pings.load(Ordering::Relaxed), 2);
    finish_peer.send(()).unwrap();
    timeout(Duration::from_secs(5), server)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(progress.requests.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn header_replay_polls_admission_while_finishing_keepalive() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let remote = listener.local_addr().unwrap();
    let (grant, granted) = tokio::sync::oneshot::channel();
    let (polled, admission_polled) = tokio::sync::oneshot::channel();
    let genesis = bitcoin::blockdata::constants::genesis_block(Network::Regtest).block_hash();
    let server = tokio::spawn(async move {
        let (mut peer, _) = accept_peer(listener, peer_version(9735)).await;
        let message = peer.read_message().await.unwrap().into_payload();
        let NetworkMessage::Ping(nonce) = message else {
            panic!("expected replay ping, got {message:?}");
        };
        grant.send(42).unwrap();
        // Withhold Pong until admission advances. Polling only ping here
        // would stall the FIFO head and this bounded handshake would fail.
        timeout(Duration::from_secs(5), admission_polled)
            .await
            .unwrap()
            .unwrap();
        peer.write_message(NetworkMessage::Pong(nonce))
            .await
            .unwrap();
        let request = next_header_request(&mut peer, &HeaderPeerProgress::default()).await;
        assert_eq!(request.locator_hashes, vec![genesis]);
    });
    let mut peer = connect_outbound(
        remote,
        Network::Regtest.magic(),
        9734,
        "/rbtc:admission-keepalive/".to_owned(),
        0,
    )
    .await
    .unwrap();
    let mut keepalive = crate::node::header_sync::ReplayKeepalive::new(
        std::time::Instant::now()
            .checked_sub(Duration::from_secs(20))
            .unwrap(),
    );
    let admission = async move {
        let value = granted.await.unwrap();
        polled.send(()).unwrap();
        value
    };
    assert_eq!(
        timeout(
            Duration::from_secs(5),
            keepalive.wait(Some(&mut peer), admission)
        )
        .await
        .unwrap()
        .unwrap(),
        42
    );
    // The granted work did not cancel the ping exchange or leave its Pong
    // for a later request to consume as an unexpected protocol message.
    request_headers(&mut peer, vec![genesis]).await.unwrap();
    timeout(Duration::from_secs(5), server)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn header_replay_disconnect_drops_pending_admission() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let remote = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut peer, _) = accept_peer(listener, peer_version(9737)).await;
        assert!(matches!(
            peer.read_message().await.unwrap().into_payload(),
            NetworkMessage::Ping(_)
        ));
        // Disconnect without Pong, while admission is still waiting.
    });
    let mut peer = connect_outbound(
        remote,
        Network::Regtest.magic(),
        9736,
        "/rbtc:admission-disconnect/".to_owned(),
        0,
    )
    .await
    .unwrap();
    let (owned, released) = tokio::sync::oneshot::channel::<()>();
    let admission = async move {
        let _owned = owned;
        std::future::pending::<()>().await;
    };
    let mut keepalive = crate::node::header_sync::ReplayKeepalive::new(
        std::time::Instant::now()
            .checked_sub(Duration::from_secs(20))
            .unwrap(),
    );
    let error = timeout(
        Duration::from_secs(5),
        keepalive.wait(Some(&mut peer), admission),
    )
    .await
    .unwrap()
    .unwrap_err();
    assert_eq!(error.kind, PeerFailureKind::Transient);
    assert!(
        released.await.is_err(),
        "pending admission retained its resources"
    );
    server.await.unwrap();
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn header_recovery_continues_past_known_or_evicted_losing_prefix() {
    for (evict, interrupted) in [(false, false), (true, false), (false, true), (true, true)] {
        let peer_progress = Arc::new(HeaderPeerProgress::default());
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("headers.redb");
        let deployments = DeploymentConfig::for_network(Network::Regtest);
        let mut dag = HeaderDag::with_deployments(deployments.clone());
        let genesis = dag.active_tip();
        let mut parent = genesis.header;
        let mut active = Vec::new();
        for _ in 0..2001 {
            parent = mine_regtest_child(parent.block_hash(), parent.time + 1);
            active.push(parent);
        }
        let mut fork = Vec::new();
        parent = genesis.header;
        for _ in 0..2002 {
            parent = mine_regtest_child(parent.block_hash(), parent.time + 10);
            fork.push(parent);
        }
        let now = unix_time().unwrap();
        let _ = dag.stage_batch_contextual(&active, now).unwrap().commit();
        let _ = dag
            .stage_batch_contextual(&fork[..2000], now)
            .unwrap()
            .commit();
        let store = RedbHeaderStore::open(&path).unwrap();
        store.append_batch(&active).unwrap();
        store.append_batch(&fork[..2000]).unwrap();
        drop(store);
        let mut dag = NodeHeaderState::test_seed(dag, &path);
        if evict {
            assert_eq!(dag.retain_idle(genesis.hash, 0, 2000).unwrap(), 0);
            let executed = dag.active_tip().hash;
            assert_eq!(dag.retain_idle(executed, 0, 1000).unwrap(), 1000);
            assert!(dag.header(&fork[999].block_hash()).unwrap().is_some());
            assert!(dag.header(&fork[1000].block_hash()).unwrap().is_none());
            assert_eq!(dag.retain_idle(executed, 0, 1000).unwrap(), 1000);
            assert_eq!(dag.retain_idle(executed, 0, 1000).unwrap(), 0);
        }
        let expected = fork[2001].block_hash();
        let cursor = fork[1999].block_hash();
        if interrupted {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let remote = listener.local_addr().unwrap();
            let prefix = fork[..2000].to_vec();
            let server_progress = Arc::clone(&peer_progress);
            let server = tokio::spawn(async move {
                let (mut peer, _) = accept_peer(listener, peer_version(9561)).await;
                let _ = next_header_request(&mut peer, &server_progress).await;
                peer.write_message(NetworkMessage::Headers(prefix))
                    .await
                    .unwrap();
                let request = next_header_request(&mut peer, &server_progress).await;
                assert_eq!(request.locator_hashes[0], cursor);
                // Disconnect after the committed prefix, before the winning suffix.
            });
            let mut session = connect_outbound(
                remote,
                Network::Regtest.magic(),
                9560,
                "/rbtc:interrupted-recovery/".to_owned(),
                0,
            )
            .await
            .unwrap();
            assert!(
                timeout(
                    Duration::from_secs(20),
                    sync_headers(
                        &mut session,
                        &deployments,
                        path.clone(),
                        &NetworkTime::default(),
                        None,
                    )
                )
                .await
                .unwrap()
                .is_err()
            );
            timeout(Duration::from_secs(20), server)
                .await
                .unwrap()
                .unwrap();
            let store = RedbHeaderStore::open(&path).unwrap();
            assert_eq!(store.recovery_tip().unwrap(), Some(cursor));
            assert_eq!(store.len().unwrap(), 4001);
            assert_eq!(
                store.load_dag(Network::Regtest, now).unwrap().active_tip(),
                dag.active_tip()
            );
        }
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let remote = listener.local_addr().unwrap();
        let (finish_peer, finished) = tokio::sync::oneshot::channel();
        let server_progress = Arc::clone(&peer_progress);
        let server = tokio::spawn(async move {
            let (mut peer, _) = accept_peer(listener, peer_version(9551)).await;
            let mut request = next_header_request(&mut peer, &server_progress).await;
            if !interrupted {
                peer.write_message(NetworkMessage::Headers(fork[..2000].to_vec()))
                    .await
                    .unwrap();
                let next = next_header_request(&mut peer, &server_progress).await;
                request = next;
            }
            assert_eq!(request.locator_hashes[0], cursor);
            assert_eq!(request.locator_hashes.last(), Some(&genesis.hash));
            peer.write_message(NetworkMessage::Headers(fork[2000..].to_vec()))
                .await
                .unwrap();
            serve_replay_pings_until(&mut peer, finished, &server_progress).await;
        });
        let mut session = connect_outbound(
            remote,
            Network::Regtest.magic(),
            9550,
            "/rbtc:recovery-test/".to_owned(),
            0,
        )
        .await
        .unwrap();
        let recovered = timeout(
            Duration::from_secs(20),
            sync_headers(
                &mut session,
                &deployments,
                path.clone(),
                &NetworkTime::default(),
                None,
            ),
        )
        .await
        .unwrap_or_else(|error| panic!("header recovery timeout: evict={evict} interrupted={interrupted} peer={peer_progress:?}: {error}"))
        .unwrap();
        finish_peer.send(()).unwrap();
        assert_eq!(recovered.active_tip().hash, expected);
        assert_eq!(
            recovered.branch_locator(expected).unwrap(),
            Some(recovered.block_locator().unwrap())
        );
        let store = RedbHeaderStore::open(&path).unwrap();
        assert_eq!(store.recovery_tip().unwrap(), None);
        let reopened = store.load_dag(Network::Regtest, now).unwrap();
        assert_eq!(reopened.active_tip(), recovered.active_tip());
        timeout(Duration::from_secs(20), server)
            .await
            .unwrap()
            .unwrap();
    }
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn block_window_fallback_preserves_order_and_only_refetches_missing_slots() {
    let genesis = bitcoin::blockdata::constants::genesis_block(Network::Regtest);
    let blocks = (1..=3)
        .map(|height| {
            regtest_block_at_height(genesis.block_hash(), genesis.header.time + height, height)
        })
        .collect::<Vec<_>>();
    let primary_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let primary_address = primary_listener.local_addr().unwrap();
    let auxiliary_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let auxiliary_address = auxiliary_listener.local_addr().unwrap();
    let responses = vec![
        vec![blocks[0].clone()],
        blocks[1..].to_vec(),
        vec![blocks[2].clone()],
    ];
    let primary_server = tokio::spawn(async move {
        let (mut peer, _) = accept_peer(primary_listener, peer_version(9_031)).await;
        for response in responses {
            let NetworkMessage::GetData(request) =
                peer.read_message().await.unwrap().into_payload()
            else {
                panic!("expected primary block request");
            };
            assert_eq!(
                request,
                response
                    .iter()
                    .map(|block| Inventory::WitnessBlock(block.block_hash()))
                    .collect::<Vec<_>>()
            );
            for block in response.into_iter().rev() {
                peer.write_message(NetworkMessage::Block(block))
                    .await
                    .unwrap();
            }
        }
    });
    let auxiliary_server = tokio::spawn(async move {
        let (mut peer, _) = accept_peer(auxiliary_listener, peer_version(9_032)).await;
        let NetworkMessage::GetData(request) = peer.read_message().await.unwrap().into_payload()
        else {
            panic!("expected auxiliary block request");
        };
        peer.write_message(NetworkMessage::NotFound(request))
            .await
            .unwrap();
    });
    let (primary, auxiliary) = tokio::join!(
        connect_outbound(
            primary_address,
            Network::Regtest.magic(),
            9_033,
            "/rbtc:fallback-test/".to_owned(),
            0
        ),
        connect_outbound(
            auxiliary_address,
            Network::Regtest.magic(),
            9_034,
            "/rbtc:fallback-test/".to_owned(),
            0
        ),
    );
    let mut primary = primary.unwrap();
    let mut auxiliary = auxiliary.unwrap();
    let hashes = blocks[1..]
        .iter()
        .map(Block::block_hash)
        .collect::<Vec<_>>();
    let (first, recovered, keep_auxiliary) = timeout(
        Duration::from_secs(10),
        download_parallel_block_pair(
            &mut primary,
            &[blocks[0].block_hash()],
            &mut auxiliary,
            &hashes,
            &[],
        ),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(first, blocks[..1]);
    assert_eq!(recovered, blocks[1..]);
    assert!(!keep_auxiliary);
    let recovered = timeout(
        Duration::from_secs(10),
        recover_lagging_auxiliary_window(
            &mut primary,
            &hashes,
            &[],
            vec![Some(blocks[1].clone()), None],
        ),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(recovered, blocks[1..]);
    let already_complete = recover_lagging_auxiliary_window(
        &mut primary,
        &hashes,
        &[],
        recovered.into_iter().map(Some).collect(),
    )
    .await
    .unwrap();
    assert_eq!(already_complete, blocks[1..]);
    primary_server.await.unwrap();
    auxiliary_server.await.unwrap();
}

#[tokio::test]
async fn header_resync_reuses_retained_forks_on_empty_and_duplicate_polls() {
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("headers.redb");
    let deployments = DeploymentConfig::for_network(Network::Regtest);
    let clock = NetworkTime::default();
    let now = unix_time().unwrap();
    let mut reference = HeaderDag::with_deployments(deployments.clone());
    let genesis = reference.active_tip();
    let first = mine_regtest_child(genesis.hash, genesis.header.time + 1);
    let second = mine_regtest_child(first.block_hash(), first.time + 1);
    let mut batch = vec![first, second];
    for offset in 10..1010 {
        batch.push(mine_regtest_child(
            genesis.hash,
            genesis.header.time + offset,
        ));
    }
    let _ = reference
        .stage_batch_contextual(&batch, now)
        .unwrap()
        .commit();
    let store = RedbHeaderStore::open(&path).unwrap();
    store.append_batch(&batch).unwrap();
    drop(store);
    let locator = reference.block_locator();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let remote = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut peer, _) = accept_peer(listener, peer_version(551)).await;
        for poll in 0..9 {
            let NetworkMessage::GetHeaders(request) =
                peer.read_message().await.unwrap().into_payload()
            else {
                panic!("expected header poll");
            };
            assert_eq!(request.locator_hashes, locator);
            let response = if poll % 2 == 0 {
                Vec::new()
            } else {
                vec![first, second]
            };
            peer.write_message(NetworkMessage::Headers(response))
                .await
                .unwrap();
        }
    });
    let mut session = connect_outbound(
        remote,
        Network::Regtest.magic(),
        550,
        "/rbtcd:test/".to_owned(),
        0,
    )
    .await
    .unwrap();
    rbtc::header_store::REPLAYED_HEADERS.with(|count| count.set(0));
    let mut dag = sync_headers_bounded(
        &mut session,
        &deployments,
        path.clone(),
        &clock,
        None,
        DEFAULT_MAX_SIDE_CHAIN_HEADERS,
    )
    .await
    .unwrap();
    assert_eq!(
        rbtc::header_store::REPLAYED_HEADERS.with(std::cell::Cell::get),
        batch.len()
    );
    rbtc::header_store::REPLAYED_HEADERS.with(|count| count.set(0));
    for _ in 0..8 {
        dag = sync_headers_bounded(
            &mut session,
            &deployments,
            path.clone(),
            &clock,
            Some(dag),
            DEFAULT_MAX_SIDE_CHAIN_HEADERS,
        )
        .await
        .unwrap();
        assert_eq!(dag.active_tip(), reference.active_tip());
        assert_eq!(
            dag.retained_header_count(),
            reference.retained_header_count()
        );
        for header in &batch {
            assert_eq!(
                dag.header(&header.block_hash()).unwrap(),
                reference.get(&header.block_hash())
            );
        }
    }
    assert_eq!(
        rbtc::header_store::REPLAYED_HEADERS.with(std::cell::Cell::get),
        0,
        "polling must not replay retained headers"
    );
    server.await.unwrap();
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn header_resync_preserves_local_submissions_and_promotes_a_retained_fork() {
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("headers.redb");
    let deployments = DeploymentConfig::for_network(Network::Regtest);
    let clock = NetworkTime::default();
    let now = unix_time().unwrap();
    let mut dag = HeaderDag::with_deployments(deployments.clone());
    let genesis = dag.active_tip();
    let first = mine_regtest_child(genesis.hash, genesis.header.time + 1);
    let second = mine_regtest_child(first.block_hash(), first.time + 1);
    let fork = mine_regtest_child(genesis.hash, genesis.header.time + 10);
    let _ = dag
        .stage_batch_contextual(&[first, second, fork], now)
        .unwrap()
        .commit();
    let store = RedbHeaderStore::open(&path).unwrap();
    store.append_batch(&[first, second, fork]).unwrap();
    drop(store);
    let local = submitted_regtest_block(second.block_hash(), 3, now);
    let pending = Mutex::new(PendingBlockQueue::default());
    assert!(matches!(
        pending.lock().unwrap().push(local.clone()),
        BlockSubmission::Queued(_)
    ));
    let mut reference = dag.clone();
    reference.insert_contextual(local.header, now).unwrap();
    let mut dag = NodeHeaderState::test_seed(dag, &path);
    let projection = RwLock::new(dag.published());
    stage_submitted_blocks(
        &pending,
        &mut dag,
        &path,
        &projection,
        &clock,
        &mut PrefetchedBlocks::default(),
        &mut Vec::new(),
        DEFAULT_MAX_SIDE_CHAIN_HEADERS,
    )
    .unwrap();
    assert_eq!(dag.active_tip().hash, local.block_hash());
    let locator = HeaderView::block_locator(&dag).unwrap();
    let mut extension = Vec::new();
    let mut parent = fork;
    for _ in 0..3 {
        parent = mine_regtest_child(parent.block_hash(), parent.time + 1);
        extension.push(parent);
    }
    let _ = reference
        .stage_batch_contextual(&extension, now)
        .unwrap()
        .commit();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let remote = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut peer, _) = accept_peer(listener, peer_version(561)).await;
        let NetworkMessage::GetHeaders(request) = peer.read_message().await.unwrap().into_payload()
        else {
            panic!("expected retained-tip locator");
        };
        assert_eq!(request.locator_hashes, locator);
        // Include the already retained fork root before its unseen suffix.
        let mut response = vec![fork];
        response.extend(extension);
        peer.write_message(NetworkMessage::Headers(response))
            .await
            .unwrap();
    });
    let mut session = connect_outbound(
        remote,
        Network::Regtest.magic(),
        560,
        "/rbtcd:test/".to_owned(),
        3,
    )
    .await
    .unwrap();
    rbtc::header_store::REPLAYED_HEADERS.with(|count| count.set(0));
    let dag = sync_headers_bounded(
        &mut session,
        &deployments,
        path.clone(),
        &clock,
        Some(dag),
        DEFAULT_MAX_SIDE_CHAIN_HEADERS,
    )
    .await
    .unwrap();
    assert_eq!(
        rbtc::header_store::REPLAYED_HEADERS.with(std::cell::Cell::get),
        0
    );
    assert_eq!(dag.active_tip(), reference.active_tip());
    assert_eq!(dag.active_tip().height, 4);
    assert_eq!(
        dag.retained_header_count(),
        reference.retained_header_count()
    );
    assert!(dag.header(&local.block_hash()).unwrap().is_some());
    *projection.write().unwrap() = dag.published();
    assert_eq!(projection.read().unwrap().active_tip(), dag.active_tip());
    assert_eq!(projection.read().unwrap().active_tip().height, 4);
    let restored = RedbHeaderStore::open(&path)
        .unwrap()
        .load_dag(Network::Regtest, now)
        .unwrap();
    assert_eq!(restored.active_tip(), dag.active_tip());
    assert_eq!(
        restored.retained_header_count(),
        dag.retained_header_count()
    );
    assert_eq!(
        restored.header(&local.block_hash()).unwrap(),
        dag.header(&local.block_hash()).unwrap()
    );
    server.await.unwrap();
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn header_resync_bounds_competing_side_chains_then_reacquires_an_evicted_fork() {
    const CAP: usize = 4;
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("headers.redb");
    let deployments = DeploymentConfig::for_network(Network::Regtest);
    let clock = NetworkTime::default();
    let dag = HeaderDag::with_deployments(deployments.clone());
    let genesis = dag.active_tip();

    // The chain a normal peer extension keeps active throughout round one.
    let mut active = Vec::new();
    let mut parent = genesis.header;
    for _ in 0..3 {
        parent = mine_regtest_child(parent.block_hash(), parent.time + 1);
        active.push(parent);
    }

    // Ten distinct single-header competing side forks off genesis, all tied
    // on chainwork (regtest mining sits at the fixed minimum difficulty) and
    // each individually weaker than the three-header active chain above.
    let mut side_forks: Vec<Header> = (0..10u32)
        .map(|offset| mine_regtest_child(genesis.hash, genesis.header.time + 100 + offset))
        .collect();
    let mut first_response = active.clone();
    first_response.extend(side_forks.iter().copied());

    // Ties break on ascending hash, so the selector keeps the four
    // greatest-hash forks and evicts the other six.
    side_forks.sort_by_key(|header| header.block_hash().to_byte_array());
    let (evicted_forks, retained_forks) = side_forks.split_at(side_forks.len() - CAP);
    let reacquired = evicted_forks[0];

    // Extend the evicted fork past the active chain's chainwork (height 4
    // beats height 3), so the node must reacquire it to win the tip back.
    let mut extension = Vec::new();
    let mut tip = reacquired;
    for _ in 0..3 {
        tip = mine_regtest_child(tip.block_hash(), tip.time + 1);
        extension.push(tip);
    }
    let mut second_response = vec![reacquired];
    second_response.extend(extension.iter().copied());

    let locator_round_one = dag.block_locator();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let remote = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut peer, _) = accept_peer(listener, peer_version(701)).await;
        // Round one: several competing low-work forks alongside the winning chain.
        let NetworkMessage::GetHeaders(request) = peer.read_message().await.unwrap().into_payload()
        else {
            panic!("expected first header poll");
        };
        assert_eq!(request.locator_hashes, locator_round_one);
        peer.write_message(NetworkMessage::Headers(first_response))
            .await
            .unwrap();
        // Round two: the node's locator still only reflects the active
        // chain, so the peer must resend the evicted fork from the shared
        // ancestor (genesis) rather than from anything the node retained.
        let NetworkMessage::GetHeaders(_) = peer.read_message().await.unwrap().into_payload()
        else {
            panic!("expected second header poll");
        };
        peer.write_message(NetworkMessage::Headers(second_response))
            .await
            .unwrap();
    });

    let mut session = connect_outbound(
        remote,
        Network::Regtest.magic(),
        700,
        "/rbtcd:test/".to_owned(),
        0,
    )
    .await
    .unwrap();

    // (a) Several competing low-work forks exceed the cap: retained
    // side-chain headers are bounded and the active chain is untouched. A
    // rejected/evicted competing fork is ordinary local resource handling,
    // never a peer fault, so this call must still return `Ok`.
    let dag = sync_headers_bounded(
        &mut session,
        &deployments,
        path.clone(),
        &clock,
        Some(NodeHeaderState::test_seed(dag, &path)),
        CAP,
    )
    .await
    .unwrap();
    assert_eq!(dag.active_tip().hash, active.last().unwrap().block_hash());
    assert_eq!(dag.active_tip().height, 3);
    let side_after_round_one = dag.retained_header_count() - 4;
    assert_eq!(
        side_after_round_one, CAP,
        "ten competing forks over a cap of {CAP} must be bounded down to exactly the cap"
    );
    for header in retained_forks {
        assert!(dag.header(&header.block_hash()).unwrap().is_some());
    }
    for header in evicted_forks {
        assert!(dag.header(&header.block_hash()).unwrap().is_none());
    }

    // (b) The peer then extends an evicted fork past the active chain's
    // chainwork; ordinary `getheaders` resync (the active-chain locator
    // above) reacquires and revalidates it from the common ancestor, making
    // it the new active header tip.
    let dag = sync_headers_bounded(
        &mut session,
        &deployments,
        path.clone(),
        &clock,
        Some(dag),
        CAP,
    )
    .await
    .unwrap();
    assert_eq!(
        dag.active_tip().hash,
        extension.last().unwrap().block_hash()
    );
    assert_eq!(dag.active_tip().height, 4);
    let side_after_round_two = dag.retained_header_count() - 5;
    assert!(
        side_after_round_two <= CAP,
        "retained {side_after_round_two} side-chain headers above the cap"
    );
    assert!(dag.header(&reacquired.block_hash()).unwrap().is_some());
    for header in &evicted_forks[1..] {
        assert!(
            dag.header(&header.block_hash()).unwrap().is_none(),
            "an evicted fork the peer never resent must not reappear on its own"
        );
    }

    server.await.unwrap();
    drop(session);

    // (c) Reopening the durable store never resurrects evicted headers, and
    // the reacquired active chain survives the reload.
    let restored = RedbHeaderStore::open(&path)
        .unwrap()
        .load_dag(Network::Regtest, unix_time().unwrap())
        .unwrap();
    assert_eq!(restored.active_tip(), dag.active_tip());
    for header in &evicted_forks[1..] {
        assert!(restored.get(&header.block_hash()).is_none());
    }
    assert!(restored.get(&reacquired.block_hash()).is_some());
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn stronger_peer_fork_wins_while_a_losing_disk_candidate_remains() {
    use crate::node::header_sync::candidate_path;

    const SIDE_CAP: usize = 2;
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("headers.redb");
    let deployments = DeploymentConfig::for_network(Network::Regtest);
    let clock = NetworkTime::default();
    let mut dag = HeaderDag::with_deployments(deployments.clone());
    let genesis = dag.active_tip();

    let mut active = Vec::new();
    let mut parent = genesis.header;
    for _ in 0..2002 {
        parent = mine_regtest_child(parent.block_hash(), parent.time + 1);
        active.push(parent);
    }
    let _ = dag
        .stage_batch_contextual(&active, u32::MAX)
        .unwrap()
        .commit();

    // Fill the in-memory side-chain allowance so the next fork is journaled.
    let side_roots: Vec<_> = (0..u32::try_from(SIDE_CAP).unwrap())
        .map(|offset| mine_regtest_child(genesis.hash, genesis.header.time + 100 + offset))
        .collect();
    for root in &side_roots {
        let _ = dag
            .stage_batch_contextual(std::slice::from_ref(root), u32::MAX)
            .unwrap()
            .commit();
    }
    let store = RedbHeaderStore::open(&path).unwrap();
    store.append_batch(&active).unwrap();
    store.append_batch(&side_roots).unwrap();
    drop(store);

    // Peer A's chain has less work than the active chain, so it remains as a
    // durable candidate instead of being promoted into the header DAG.
    let mut candidate_a = Vec::new();
    parent = side_roots[0];
    for _ in 0..2000 {
        parent = mine_regtest_child(parent.block_hash(), parent.time + 10);
        candidate_a.push(parent);
    }
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let remote = listener.local_addr().unwrap();
    let candidate_tip = candidate_a.last().unwrap().block_hash();
    let server = tokio::spawn(async move {
        let (mut peer, _) = accept_peer(listener, peer_version(741)).await;
        let progress = HeaderPeerProgress::default();
        let _ = next_header_request(&mut peer, &progress).await;
        peer.write_message(NetworkMessage::Headers(candidate_a))
            .await
            .unwrap();
        let request = next_header_request(&mut peer, &progress).await;
        assert_eq!(request.locator_hashes[0], candidate_tip);
        peer.write_message(NetworkMessage::Headers(Vec::new()))
            .await
            .unwrap();
    });
    let mut peer = connect_outbound(
        remote,
        Network::Regtest.magic(),
        740,
        "/rbtcd:candidate-a/".to_owned(),
        0,
    )
    .await
    .unwrap();
    let state = sync_headers_bounded(
        &mut peer,
        &deployments,
        path.clone(),
        &clock,
        Some(NodeHeaderState::test_seed(dag, &path)),
        SIDE_CAP,
    )
    .await
    .unwrap();
    server.await.unwrap();
    assert_eq!(state.active_tip().hash, active.last().unwrap().block_hash());
    assert!(candidate_path(&path).exists());
    assert_eq!(
        crate::header_candidate::DiskHeaderCandidate::stored_anchor(candidate_path(&path)).unwrap(),
        Some(side_roots[0].block_hash())
    );
    drop(peer);

    // Peer B does not know candidate A and answers from their common genesis.
    // Its 2003-header branch spans two protocol responses and overtakes the
    // active chain while candidate A is still on disk.
    let mut candidate_b = Vec::new();
    parent = genesis.header;
    for _ in 0..2003 {
        parent = mine_regtest_child(parent.block_hash(), parent.time + 20);
        candidate_b.push(parent);
    }
    let expected_tip = candidate_b.last().unwrap().block_hash();
    let extension_cursor = candidate_b[1999].block_hash();
    let suffix = candidate_b[2000..].to_vec();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let remote = listener.local_addr().unwrap();
    let (batch_processed, batch_processed_rx) = tokio::sync::oneshot::channel();
    let (disconnect_peer, disconnect_peer_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut peer, _) = accept_peer(listener, peer_version(743)).await;
        let progress = HeaderPeerProgress::default();
        let request = next_header_request(&mut peer, &progress).await;
        assert!(request.locator_hashes.contains(&genesis.hash));
        peer.write_message(NetworkMessage::Headers(candidate_b[..2000].to_vec()))
            .await
            .unwrap();
        let request = next_header_request(&mut peer, &progress).await;
        assert_eq!(request.locator_hashes[0], extension_cursor);
        batch_processed.send(()).unwrap();
        disconnect_peer_rx.await.unwrap();
    });
    let mut peer = connect_outbound(
        remote,
        Network::Regtest.magic(),
        742,
        "/rbtcd:candidate-b/".to_owned(),
        0,
    )
    .await
    .unwrap();
    let mut sync = Box::pin(sync_headers_bounded(
        &mut peer,
        &deployments,
        path.clone(),
        &clock,
        Some(state),
        SIDE_CAP,
    ));
    tokio::select! {
        result = &mut sync => {
            let client_result = match result {
                Ok(_) => "completed successfully".to_owned(),
                Err(error) => error.to_string(),
            };
            let peer_result = server.await;
            panic!("sync ended before the second headers batch: {client_result}; peer={peer_result:?}");
        },
        _ = batch_processed_rx => {}
    }
    assert_eq!(
        crate::header_candidate::DiskHeaderCandidate::stored_anchor(candidate_path(&path)).unwrap(),
        Some(genesis.hash),
        "the new peer's competing branch must replace the stale candidate journal"
    );
    disconnect_peer.send(()).unwrap();
    let error = match sync.await {
        Ok(_) => panic!("the peer disconnected before sending the candidate extension"),
        Err(error) => error,
    };
    assert_eq!(error.kind, PeerFailureKind::Transient);
    server.await.unwrap();
    drop(peer);

    // Rebuild the in-memory state and resume the replacement journal after
    // disconnect, then finish the fork through its new peer.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let remote = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut peer, _) = accept_peer(listener, peer_version(745)).await;
        let request = next_header_request(&mut peer, &HeaderPeerProgress::default()).await;
        assert_eq!(request.locator_hashes[0], extension_cursor);
        peer.write_message(NetworkMessage::Headers(suffix))
            .await
            .unwrap();
    });
    let mut peer = connect_outbound(
        remote,
        Network::Regtest.magic(),
        744,
        "/rbtcd:candidate-b-resume/".to_owned(),
        0,
    )
    .await
    .unwrap();
    let state = sync_headers_bounded(
        &mut peer,
        &deployments,
        path.clone(),
        &clock,
        None,
        SIDE_CAP,
    )
    .await
    .unwrap();
    server.await.unwrap();

    assert_eq!(state.active_tip().hash, expected_tip);
    assert_eq!(state.active_tip().height, 2003);
    assert!(!candidate_path(&path).exists());
    let restored = RedbHeaderStore::open(&path)
        .unwrap()
        .load_dag(Network::Regtest, u32::MAX)
        .unwrap();
    assert_eq!(restored.active_tip().hash, expected_tip);
}

#[tokio::test]
async fn header_recovery_cursor_survives_ingress_retention_until_spill() {
    const SIDE_CAP: usize = 2;
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("headers.redb");
    let deployments = DeploymentConfig::for_network(Network::Regtest);
    let clock = NetworkTime::default();
    let mut dag = HeaderDag::with_deployments(deployments.clone());
    let genesis = dag.active_tip();

    let mut active = Vec::new();
    let mut parent = genesis.header;
    for _ in 0..2001 {
        parent = mine_regtest_child(parent.block_hash(), parent.time + 1);
        active.push(parent);
    }
    let _ = dag
        .stage_batch_contextual(&active, u32::MAX)
        .unwrap()
        .commit();
    let side_root = mine_regtest_child(genesis.hash, genesis.header.time + 100);
    let _ = dag
        .stage_batch_contextual(std::slice::from_ref(&side_root), u32::MAX)
        .unwrap()
        .commit();
    let store = RedbHeaderStore::open(&path).unwrap();
    store.append_batch(&active).unwrap();
    store.append(side_root).unwrap();
    drop(store);

    let mut fork = Vec::new();
    parent = genesis.header;
    for _ in 0..2000 {
        parent = mine_regtest_child(parent.block_hash(), parent.time + 10);
        fork.push(parent);
    }
    let cursor = fork.last().unwrap().block_hash();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let remote = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut peer, _) = accept_peer(listener, peer_version(747)).await;
        let progress = HeaderPeerProgress::default();
        let _ = next_header_request(&mut peer, &progress).await;
        peer.write_message(NetworkMessage::Headers(fork))
            .await
            .unwrap();
        let request = next_header_request(&mut peer, &progress).await;
        assert_eq!(request.locator_hashes[0], cursor);
        peer.write_message(NetworkMessage::Headers(Vec::new()))
            .await
            .unwrap();
    });
    let mut peer = connect_outbound(
        remote,
        Network::Regtest.magic(),
        746,
        "/rbtcd:recovery-cursor-retention/".to_owned(),
        0,
    )
    .await
    .unwrap();
    let state = sync_headers_bounded(
        &mut peer,
        &deployments,
        path.clone(),
        &clock,
        Some(NodeHeaderState::test_seed(dag, &path)),
        SIDE_CAP,
    )
    .await
    .unwrap();
    server.await.unwrap();

    assert_eq!(state.active_tip().hash, active.last().unwrap().block_hash());
    assert_eq!(
        state.retained_header_count() - state.active_tip().height as usize - 1,
        SIDE_CAP,
        "the completed losing recovery branch must return to the configured side-header cap"
    );
    assert!(!crate::node::header_sync::candidate_path(&path).exists());
    assert_eq!(
        RedbHeaderStore::open(&path)
            .unwrap()
            .recovery_tip()
            .unwrap(),
        None
    );
}

#[tokio::test]
async fn header_resync_rejects_an_invalid_batch_without_persisting_its_prefix() {
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("headers.redb");
    let deployments = DeploymentConfig::for_network(Network::Regtest);
    let dag = HeaderDag::with_deployments(deployments.clone());
    let genesis = dag.active_tip();
    let first = mine_regtest_child(genesis.hash, genesis.header.time + 1);
    let second = mine_regtest_child(first.block_hash(), genesis.header.time);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let remote = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut peer, _) = accept_peer(listener, peer_version(571)).await;
        assert!(matches!(
            peer.read_message().await.unwrap().into_payload(),
            NetworkMessage::GetHeaders(_)
        ));
        peer.write_message(NetworkMessage::Headers(vec![first, second]))
            .await
            .unwrap();
    });
    let mut session = connect_outbound(
        remote,
        Network::Regtest.magic(),
        570,
        "/rbtcd:test/".to_owned(),
        0,
    )
    .await
    .unwrap();
    let error = sync_headers(
        &mut session,
        &deployments,
        path.clone(),
        &NetworkTime::default(),
        Some(NodeHeaderState::test_seed(dag, &path)),
    )
    .await
    .err()
    .unwrap();
    assert_eq!(error.kind, PeerFailureKind::ProtocolViolation);
    assert!(error.message.contains("median time past"));
    let store = RedbHeaderStore::open(&path).unwrap();
    assert_eq!(store.len().unwrap(), 0);
    assert_eq!(store.recovery_tip().unwrap(), None);
    assert_eq!(
        store
            .load_dag(Network::Regtest, unix_time().unwrap())
            .unwrap()
            .active_tip(),
        genesis
    );
    server.await.unwrap();
}

#[tokio::test]
async fn header_resync_cancellation_keeps_committed_batches_for_restart() {
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("headers.redb");
    let deployments = DeploymentConfig::for_network(Network::Regtest);
    let dag = HeaderDag::with_deployments(deployments.clone());
    let mut parent = dag.active_tip().header;
    let mut batch = Vec::new();
    for _ in 0..MAX_HEADERS_PER_RESPONSE {
        parent = mine_regtest_child(parent.block_hash(), parent.time + 1);
        batch.push(parent);
    }
    let last = parent.block_hash();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let remote = listener.local_addr().unwrap();
    let (reached, at_next_request) = oneshot::channel();
    let (finish, finished) = oneshot::channel::<()>();
    let server = tokio::spawn(async move {
        let (mut peer, _) = accept_peer(listener, peer_version(581)).await;
        assert!(matches!(
            peer.read_message().await.unwrap().into_payload(),
            NetworkMessage::GetHeaders(_)
        ));
        peer.write_message(NetworkMessage::Headers(batch))
            .await
            .unwrap();
        let NetworkMessage::GetHeaders(request) = peer.read_message().await.unwrap().into_payload()
        else {
            panic!("expected next batch request");
        };
        assert_eq!(request.locator_hashes[0], last);
        reached.send(()).unwrap();
        let _ = finished.await;
    });
    let mut session = connect_outbound(
        remote,
        Network::Regtest.magic(),
        580,
        "/rbtcd:test/".to_owned(),
        0,
    )
    .await
    .unwrap();
    let clock = NetworkTime::default();
    let mut syncing = Box::pin(sync_headers(
        &mut session,
        &deployments,
        path.clone(),
        &clock,
        Some(NodeHeaderState::test_seed(dag, &path)),
    ));
    tokio::select! {
        result = &mut syncing => panic!("sync ended before cancellation: {:?}", result.err()),
        ready = at_next_request => ready.unwrap(),
    }
    drop(syncing);
    drop(finish);
    server.await.unwrap();
    let store = RedbHeaderStore::open(&path).unwrap();
    assert_eq!(store.recovery_tip().unwrap(), Some(last));
    assert_eq!(
        store.len().unwrap(),
        u64::try_from(MAX_HEADERS_PER_RESPONSE).unwrap()
    );
    let restored = store
        .load_dag(Network::Regtest, unix_time().unwrap())
        .unwrap();
    assert_eq!(restored.active_tip().hash, last);
    assert_eq!(
        restored.retained_header_count(),
        MAX_HEADERS_PER_RESPONSE + 1
    );
}

#[tokio::test]
async fn header_resync_rejects_local_count_or_configuration_mismatch() {
    for mismatch in ["count", "deployments"] {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("headers.redb");
        let dag = HeaderDag::new(Network::Regtest);
        let mut deployments = DeploymentConfig::for_network(Network::Regtest);
        let store = RedbHeaderStore::open(&path).unwrap();
        if mismatch == "count" {
            let tip = dag.active_tip();
            store
                .append(mine_regtest_child(tip.hash, tip.header.time + 1))
                .unwrap();
        } else {
            deployments
                .apply_test_activation_height("bip34@10")
                .unwrap();
        }
        drop(store);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let remote = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut peer, _) = accept_peer(listener, peer_version(591)).await;
            assert!(
                peer.read_message().await.is_err(),
                "no header request before the local guard"
            );
        });
        let mut session = connect_outbound(
            remote,
            Network::Regtest.magic(),
            590,
            "/rbtcd:test/".to_owned(),
            0,
        )
        .await
        .unwrap();
        rbtc::header_store::REPLAYED_HEADERS.with(|count| count.set(0));
        let error = sync_headers(
            &mut session,
            &deployments,
            path.clone(),
            &NetworkTime::default(),
            Some(NodeHeaderState::test_seed(dag, &path)),
        )
        .await
        .err()
        .unwrap();
        assert_eq!(error.kind, PeerFailureKind::LocalResource);
        assert_eq!(
            rbtc::header_store::REPLAYED_HEADERS.with(std::cell::Cell::get),
            0
        );
        drop(session);
        server.await.unwrap();
    }
}

#[tokio::test]
#[ignore = "explicit retained-header resync resource probe"]
#[allow(clippy::too_many_lines)]
async fn header_resync_resource_probe() {
    let mode = std::env::var("RBTC_HEADER_RESYNC_MODE").unwrap_or_else(|_| "reuse".to_owned());
    assert!(mode == "reuse" || mode == "reload");
    let siblings = std::env::var("RBTC_HEADER_RESYNC_SIBLINGS")
        .map_or(50_000, |value| value.parse::<u32>().unwrap());
    assert!((1..=200_000).contains(&siblings));
    let rounds = 8;
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("headers.redb");
    let deployments = DeploymentConfig::for_network(Network::Regtest);
    let now = unix_time().unwrap();
    let mut dag = HeaderDag::with_deployments(deployments.clone());
    let genesis = dag.active_tip();
    let store = RedbHeaderStore::open(&path).unwrap();
    let mut batch = Vec::with_capacity(MAX_HEADERS_PER_RESPONSE);
    let mut parent = genesis.header;
    for index in 0..2500 + siblings {
        let header = if index < 2500 {
            parent = mine_regtest_child(parent.block_hash(), parent.time + 1);
            parent
        } else {
            mine_regtest_child(genesis.hash, genesis.header.time + index + 10)
        };
        batch.push(header);
        if batch.len() == MAX_HEADERS_PER_RESPONSE || index + 1 == 2500 + siblings {
            let staged = dag.stage_batch_contextual(&batch, now).unwrap();
            store.append_batch(&batch).unwrap();
            let _ = staged.commit();
            batch.clear();
        }
    }
    let expected = dag.active_tip();
    let count = dag.retained_header_count();
    let locator = HeaderView::block_locator(&dag).unwrap();
    drop(dag);
    drop(store);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let remote = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut peer, _) = accept_peer(listener, peer_version(601)).await;
        for _ in 0..=rounds {
            let NetworkMessage::GetHeaders(request) =
                peer.read_message().await.unwrap().into_payload()
            else {
                panic!("expected probe header poll");
            };
            assert_eq!(request.locator_hashes, locator);
            peer.write_message(NetworkMessage::Headers(Vec::new()))
                .await
                .unwrap();
        }
    });
    let mut session = connect_outbound(
        remote,
        Network::Regtest.magic(),
        600,
        "/rbtcd:probe/".to_owned(),
        2500,
    )
    .await
    .unwrap();
    let clock = NetworkTime::default();
    let mut dag = sync_headers_bounded(
        &mut session,
        &deployments,
        path.clone(),
        &clock,
        None,
        DEFAULT_MAX_SIDE_CHAIN_HEADERS,
    )
    .await
    .unwrap();
    let memory = || {
        fs::read_to_string("/proc/self/status").ok().map(|status| {
            ["VmRSS:", "VmHWM:"].map(|field| {
                status.lines().find_map(|line| {
                    line.strip_prefix(field)?
                        .split_whitespace()
                        .next()?
                        .parse::<u64>()
                        .ok()
                })
            })
        })
    };
    let before = memory();
    rbtc::header_store::REPLAYED_HEADERS.with(|count| count.set(0));
    let started = Instant::now();
    for _ in 0..rounds {
        dag = if mode == "reuse" {
            sync_headers_bounded(
                &mut session,
                &deployments,
                path.clone(),
                &clock,
                Some(dag),
                DEFAULT_MAX_SIDE_CHAIN_HEADERS,
            )
            .await
            .unwrap()
        } else {
            // This is the pre-change serving-loop assignment: the old DAG
            // stays alive until full replay returns its replacement.
            sync_headers_bounded(
                &mut session,
                &deployments,
                path.clone(),
                &clock,
                None,
                DEFAULT_MAX_SIDE_CHAIN_HEADERS,
            )
            .await
            .unwrap()
        };
        assert_eq!(dag.active_tip(), expected);
        assert_eq!(dag.retained_header_count(), count);
    }
    let elapsed_micros = started.elapsed().as_micros();
    let replayed = rbtc::header_store::REPLAYED_HEADERS.with(std::cell::Cell::get);
    assert_eq!(
        replayed,
        if mode == "reuse" {
            0
        } else {
            (count - 1) * rounds
        }
    );
    let after = memory();
    server.await.unwrap();
    println!(
        "HEADER_RESYNC_PROBE={}",
        serde_json::json!({
            "mode": mode, "siblings": siblings, "active_entries": 2501,
            "retained_entries": count, "rounds": rounds, "elapsed_micros": elapsed_micros,
            "replayed_headers": replayed, "rss_and_hwm_before_kib": before,
            "rss_and_hwm_after_kib": after, "database_bytes": fs::metadata(&path).unwrap().len(),
            "allocator": "system", "workload": "warm Redb/tmpfs and loopback V1 empty-header polls",
        })
    );
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn disk_candidate_resumes_after_disconnect_and_defers_atomic_promotion() {
    use crate::node::header_sync::{HeaderSyncPolicy, candidate_path, sync_headers_with_policy};
    let peer_progress = Arc::new(HeaderPeerProgress::default());
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("headers.redb");
    let deployments = DeploymentConfig::for_network(Network::Regtest);
    let mut dag = HeaderDag::new(Network::Regtest);
    let genesis = dag.active_tip();
    let mut parent = genesis.header;
    let mut active = Vec::new();
    for _ in 0..2001 {
        parent = mine_regtest_child(parent.block_hash(), parent.time + 1);
        active.push(parent);
    }
    parent = genesis.header;
    let mut fork = Vec::new();
    for _ in 0..2002 {
        parent = mine_regtest_child(parent.block_hash(), parent.time + 10);
        fork.push(parent);
    }
    let _ = dag
        .stage_batch_contextual(&active, u32::MAX)
        .unwrap()
        .commit();
    let _ = dag
        .stage_batch_contextual(&fork[..1], u32::MAX)
        .unwrap()
        .commit();
    let store = RedbHeaderStore::open(&path).unwrap();
    store.append_batch(&active).unwrap();
    store.append_batch(&fork[..1]).unwrap();
    drop(store);
    let original = dag.active_tip();
    let cursor = fork[1999].block_hash();
    let winner = fork[2001].block_hash();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let remote = listener.local_addr().unwrap();
    let prefix = fork[..2000].to_vec();
    let server_progress = Arc::clone(&peer_progress);
    let server = tokio::spawn(async move {
        let (mut peer, _) = accept_peer(listener, peer_version(9711)).await;
        let _ = next_header_request(&mut peer, &server_progress).await;
        peer.write_message(NetworkMessage::Headers(prefix))
            .await
            .unwrap();
        let request = next_header_request(&mut peer, &server_progress).await;
        assert_eq!(request.locator_hashes[0], cursor);
        // Disconnect with a complete, losing prefix persisted only on disk.
    });
    let mut peer = connect_outbound(
        remote,
        Network::Regtest.magic(),
        9710,
        "/rbtc:disk/".to_owned(),
        0,
    )
    .await
    .unwrap();
    let policy = HeaderSyncPolicy {
        spill_side_headers: 0,
        promotion_bytes: 1,
        ..HeaderSyncPolicy::default()
    };
    assert!(
        sync_headers_with_policy(
            &mut peer,
            &deployments,
            path.clone(),
            &NetworkTime::default(),
            Some(NodeHeaderState::test_seed(dag, &path)),
            policy
        )
        .await
        .is_err()
    );
    server.await.unwrap();
    let store = RedbHeaderStore::open(&path).unwrap();
    assert_eq!(store.len().unwrap(), 2002);
    let mut restored =
        NodeHeaderState::test_seed(store.load_dag(Network::Regtest, u32::MAX).unwrap(), &path);
    drop(store);
    assert_eq!(restored.active_tip(), original);
    // The candidate is anchored on a retained side header. Idle eviction must
    // not remove it even when the execution tip matches the active tip.
    assert_eq!(restored.retain_idle(original.hash, 0, 1).unwrap(), 0);
    assert!(restored.header(&fork[0].block_hash()).unwrap().is_some());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let remote = listener.local_addr().unwrap();
    let suffix = fork[2000..].to_vec();
    let (finish_peer, finished) = tokio::sync::oneshot::channel();
    let server_progress = Arc::clone(&peer_progress);
    let server = tokio::spawn(async move {
        let (mut peer, _) = accept_peer(listener, peer_version(9721)).await;
        let request = next_header_request(&mut peer, &server_progress).await;
        assert_eq!(request.locator_hashes[0], cursor);
        peer.write_message(NetworkMessage::Headers(suffix))
            .await
            .unwrap();
        serve_replay_pings_until(&mut peer, finished, &server_progress).await;
    });
    let mut peer = connect_outbound(
        remote,
        Network::Regtest.magic(),
        9720,
        "/rbtc:disk/".to_owned(),
        0,
    )
    .await
    .unwrap();
    let error = match sync_headers_with_policy(
        &mut peer,
        &deployments,
        path.clone(),
        &NetworkTime::default(),
        Some(restored),
        policy,
    )
    .await
    {
        Ok(_) => panic!("promotion byte allowance must defer"),
        Err(error) => error,
    };
    finish_peer.send(()).unwrap();
    assert_eq!(error.kind, PeerFailureKind::LocalResource);
    server.await.unwrap();
    let store = RedbHeaderStore::open(&path).unwrap();
    assert_eq!(store.len().unwrap(), 2002);
    assert_eq!(
        store
            .load_dag(Network::Regtest, u32::MAX)
            .unwrap()
            .active_tip(),
        original
    );
    drop(store);
    assert!(candidate_path(&path).exists());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let remote = listener.local_addr().unwrap();
    let server_progress = Arc::clone(&peer_progress);
    let server = tokio::spawn(async move {
        let (mut peer, _) = accept_peer(listener, peer_version(9731)).await;
        let request = next_header_request(&mut peer, &server_progress).await;
        assert_eq!(request.locator_hashes[0], winner);
        peer.write_message(NetworkMessage::Headers(Vec::new()))
            .await
            .unwrap();
    });
    let mut peer = connect_outbound(
        remote,
        Network::Regtest.magic(),
        9730,
        "/rbtc:disk/".to_owned(),
        0,
    )
    .await
    .unwrap();
    let result = sync_headers_with_policy(
        &mut peer,
        &deployments,
        path.clone(),
        &NetworkTime::default(),
        None,
        HeaderSyncPolicy {
            spill_side_headers: 0,
            promotion_bytes: 8 * 1024 * 1024,
            ..HeaderSyncPolicy::default()
        },
    )
    .await
    .unwrap();
    server.await.unwrap();
    assert_eq!(result.active_tip().hash, winner);
    assert!(!candidate_path(&path).exists());
    let store = RedbHeaderStore::open(&path).unwrap();
    assert_eq!(store.len().unwrap(), 4003);
    assert_eq!(
        store
            .load_dag(Network::Regtest, u32::MAX)
            .unwrap()
            .active_tip(),
        result.active_tip()
    );
}

/// Exercise every durable boundary around the full-candidate completion marker.
/// These are restart-state fixtures, not a process-kill acceptance measurement.
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn candidate_import_restart_never_publishes_a_partial_winner() {
    use crate::{
        header_candidate::{DiskHeaderCandidate, HeaderCandidateLimits},
        header_store::HeaderStoreError,
        headers::HeaderWorkBudget,
        node::header_sync::{candidate_path, sync_headers},
    };
    for (imported, finished, startup) in [
        (0, false, false),
        (3, false, false),
        (4, false, false),
        (4, true, false),
        (3, false, true),
    ] {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("headers.redb");
        let deployments = DeploymentConfig::for_network(Network::Regtest);
        let mut dag = HeaderDag::new(Network::Regtest);
        let genesis = dag.active_tip();
        let mut active = Vec::new();
        let mut parent = genesis.header;
        for _ in 0..2 {
            parent = mine_regtest_child(parent.block_hash(), parent.time + 1);
            active.push(parent);
        }
        let mut fork = Vec::new();
        parent = genesis.header;
        for _ in 0..4 {
            parent = mine_regtest_child(parent.block_hash(), parent.time + 10);
            fork.push(parent);
        }
        let _ = dag
            .stage_batch_contextual(&active, u32::MAX)
            .unwrap()
            .commit();
        let original = dag.active_tip();
        let mut budget = HeaderWorkBudget::default();
        let journal = candidate_path(&path);
        let mut candidate = DiskHeaderCandidate::open(
            &journal,
            &dag,
            genesis.hash,
            u32::MAX,
            HeaderCandidateLimits::default(),
            &mut budget,
        )
        .unwrap();
        candidate.append(&fork, u32::MAX, &mut budget).unwrap();
        let winner = candidate.tip();
        drop(candidate);
        let store = RedbHeaderStore::open(&path).unwrap();
        store.append_batch(&active).unwrap();
        store.begin_candidate_promotion(winner.hash).unwrap();
        store.append_batch(&fork[..imported]).unwrap();
        if finished {
            store.finish_candidate_promotion(winner.hash).unwrap();
        } else {
            assert!(matches!(
                store.load_dag(Network::Regtest, u32::MAX),
                Err(HeaderStoreError::PendingCandidate)
            ));
        }
        if startup {
            // This runs before peer creation in the real standby seed path.
            crate::node::header_sync::recover_pending_promotion(
                &store,
                &path,
                &deployments,
                u32::MAX,
            )
            .await
            .unwrap();
            assert_eq!(
                store
                    .load_dag(Network::Regtest, u32::MAX)
                    .unwrap()
                    .active_tip(),
                winner
            );
        }
        drop(store);
        assert_eq!(dag.active_tip(), original);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let remote = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut peer, _) = accept_peer(listener, peer_version(9811)).await;
            let NetworkMessage::GetHeaders(request) =
                peer.read_message().await.unwrap().into_payload()
            else {
                panic!("expected fully recovered winner");
            };
            assert_eq!(request.locator_hashes[0], winner.hash);
            peer.write_message(NetworkMessage::Headers(Vec::new()))
                .await
                .unwrap();
        });
        let mut peer = connect_outbound(
            remote,
            Network::Regtest.magic(),
            9810,
            "/rbtc:restart/".to_owned(),
            0,
        )
        .await
        .unwrap();
        let state = sync_headers(
            &mut peer,
            &deployments,
            path.clone(),
            &NetworkTime::default(),
            None,
        )
        .await
        .unwrap();
        server.await.unwrap();
        assert_eq!(state.active_tip(), winner);
        assert!(!journal.exists());
        let store = RedbHeaderStore::open(&path).unwrap();
        assert_eq!(store.pending_candidate_tip().unwrap(), None);
        assert_eq!(store.len().unwrap(), 6);
        assert_eq!(
            store
                .load_dag(Network::Regtest, u32::MAX)
                .unwrap()
                .active_tip(),
            winner
        );
    }
}

#[tokio::test]
async fn missing_pending_candidate_fails_locally_before_network_request() {
    use crate::node::header_sync::sync_headers;
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("headers.redb");
    let deployments = DeploymentConfig::for_network(Network::Regtest);
    let genesis = HeaderDag::new(Network::Regtest).active_tip();
    let store = RedbHeaderStore::open(&path).unwrap();
    store.begin_candidate_promotion(genesis.hash).unwrap();
    drop(store);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let remote = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut peer, _) = accept_peer(listener, peer_version(9821)).await;
        // The client closes without sending any getheaders request.
        assert!(peer.read_message().await.is_err());
    });
    let mut peer = connect_outbound(
        remote,
        Network::Regtest.magic(),
        9820,
        "/rbtc:missing/".to_owned(),
        0,
    )
    .await
    .unwrap();
    let result = sync_headers(
        &mut peer,
        &deployments,
        path.clone(),
        &NetworkTime::default(),
        None,
    )
    .await;
    let Err(error) = result else {
        panic!("missing journal must fail");
    };
    assert_eq!(error.kind, PeerFailureKind::LocalResource);
    drop(peer);
    server.await.unwrap();
    assert_eq!(
        RedbHeaderStore::open(&path)
            .unwrap()
            .pending_candidate_tip()
            .unwrap(),
        Some(genesis.hash)
    );
}

#[tokio::test]
async fn disk_reindex_resumes_a_multi_frame_prefix_and_rejects_divergence() {
    let directory = TempDir::new().unwrap();
    let deployments = DeploymentConfig::for_network(Network::Regtest);
    let mut source = HeaderDag::with_deployments(deployments.clone());
    let genesis = source.active_tip().header;
    let mut parent = genesis;
    let mut headers = Vec::new();
    for _ in 0..2001 {
        parent = mine_regtest_child(parent.block_hash(), parent.time + 1);
        headers.push(parent);
    }
    let _ = source
        .stage_batch_contextual(&headers, unix_time().unwrap())
        .unwrap()
        .commit();
    let path = directory.path().join("headers.redb");
    let store = RedbHeaderStore::open(&path).unwrap();
    store.append_batch(&headers[..2000]).unwrap();
    drop(store);
    let state = prepare_reindex_headers(directory.path(), &source, &deployments)
        .await
        .unwrap();
    assert_eq!(state.active_tip(), source.active_tip());
    drop(state);
    let state = prepare_reindex_headers(directory.path(), &source, &deployments)
        .await
        .unwrap();
    assert_eq!(state.active_tip(), source.active_tip());
    drop(state);
    let mut other = HeaderDag::with_deployments(deployments.clone());
    other
        .insert_contextual(
            mine_regtest_child(genesis.block_hash(), genesis.time + 2),
            unix_time().unwrap(),
        )
        .unwrap();
    let error = prepare_reindex_headers(directory.path(), &other, &deployments)
        .await
        .err()
        .unwrap();
    assert!(error.contains("prefix diverges"), "{error}");
    let store = RedbHeaderStore::open(&path).unwrap();
    assert_eq!(store.len().unwrap(), 2001);
}
