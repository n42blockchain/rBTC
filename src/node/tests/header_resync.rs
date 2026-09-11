use super::*;

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
    let mut dag = sync_headers(&mut session, &deployments, path.clone(), &clock, None)
        .await
        .unwrap();
    assert_eq!(
        rbtc::header_store::REPLAYED_HEADERS.with(std::cell::Cell::get),
        batch.len()
    );
    rbtc::header_store::REPLAYED_HEADERS.with(|count| count.set(0));
    for _ in 0..8 {
        dag = sync_headers(&mut session, &deployments, path.clone(), &clock, Some(dag))
            .await
            .unwrap();
        assert_eq!(dag.active_tip(), reference.active_tip());
        assert_eq!(
            dag.retained_header_count(),
            reference.retained_header_count()
        );
        for header in &batch {
            assert_eq!(
                dag.get(&header.block_hash()),
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
    let projection = RwLock::new(dag.active_chain_snapshot());
    stage_submitted_blocks(
        &pending,
        &mut dag,
        &path,
        &projection,
        &clock,
        &mut PrefetchedBlocks::default(),
        &mut Vec::new(),
    )
    .unwrap();
    assert_eq!(dag.active_tip().hash, local.block_hash());
    let locator = dag.block_locator();
    let mut extension = Vec::new();
    let mut parent = fork;
    for _ in 0..3 {
        parent = mine_regtest_child(parent.block_hash(), parent.time + 1);
        extension.push(parent);
    }
    let mut reference = dag.clone();
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
    let dag = sync_headers(&mut session, &deployments, path.clone(), &clock, Some(dag))
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
    assert!(dag.get(&local.block_hash()).is_some());
    dag.refresh_active_chain_snapshot(&mut projection.write().unwrap());
    assert_eq!(projection.read().unwrap().active_tip(), dag.active_tip());
    assert_eq!(projection.read().unwrap().retained_header_count(), 5);
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
        restored.get(&local.block_hash()),
        dag.get(&local.block_hash())
    );
    server.await.unwrap();
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
        Some(dag),
    )
    .await
    .err()
    .unwrap();
    assert_eq!(error.kind, PeerFailureKind::ProtocolViolation);
    assert!(error.message.contains("median time past"));
    let store = RedbHeaderStore::open(&path).unwrap();
    assert_eq!(store.len().unwrap(), 0);
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
        Some(dag),
    ));
    tokio::select! {
        result = &mut syncing => panic!("sync ended before cancellation: {:?}", result.err()),
        ready = at_next_request => ready.unwrap(),
    }
    drop(syncing);
    drop(finish);
    server.await.unwrap();
    let store = RedbHeaderStore::open(&path).unwrap();
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
            path,
            &NetworkTime::default(),
            Some(dag),
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
    let locator = dag.block_locator();
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
    let mut dag = sync_headers(&mut session, &deployments, path.clone(), &clock, None)
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
            sync_headers(&mut session, &deployments, path.clone(), &clock, Some(dag))
                .await
                .unwrap()
        } else {
            // This is the pre-change serving-loop assignment: the old DAG
            // stays alive until full replay returns its replacement.
            sync_headers(&mut session, &deployments, path.clone(), &clock, None)
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
