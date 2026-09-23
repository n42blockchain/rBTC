use super::*;

#[test]
fn memory_plan_rejects_overcommit_before_creating_data_directory() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("not-created");
    let mut config = NodeConfig::new(Network::Regtest, &path);
    assert_eq!(
        config.resources.memory_budget_bytes,
        32 * 1024 * 1024 * 1024
    );
    let defaults = parse_options(
        ["--network", "regtest", "--connect", "127.0.0.1:18444"]
            .into_iter()
            .map(str::to_owned),
    )
    .unwrap()
    .unwrap();
    assert_eq!(
        defaults.resources.memory_budget_bytes,
        config.resources.memory_budget_bytes
    );
    config.resources.memory_budget_bytes = 1024 * 1024 * 1024;
    assert!(
        config
            .validate()
            .unwrap_err()
            .to_string()
            .contains("startup cache plan")
    );
    assert!(!path.exists());
    config.resources.memory_budget_bytes = crate::node_memory::DEFAULT_MEMORY_BUDGET_BYTES;
    assert!(config.validate().is_ok());
}

#[test]
fn cli_and_config_bind_the_same_memory_limit_and_validate_duplicates() {
    let root = tempfile::tempdir().unwrap();
    let config = root.path().join("node.conf");
    fs::write(
        &config,
        "network=regtest\ndata_dir=unused\nmemory_budget_bytes=8589934592\n",
    )
    .unwrap();
    let parsed = parse_options(["--config".to_owned(), config.display().to_string()].into_iter())
        .unwrap()
        .unwrap();
    assert_eq!(parsed.resources.memory_budget_bytes, 8 * 1024 * 1024 * 1024);
    let overridden = parse_options(
        [
            "--config".to_owned(),
            config.display().to_string(),
            "--memory-budget-bytes".to_owned(),
            "17179869184".to_owned(),
        ]
        .into_iter(),
    )
    .unwrap()
    .unwrap();
    assert_eq!(
        overridden.resources.memory_budget_bytes,
        16 * 1024 * 1024 * 1024
    );
    assert!(
        parse_options(
            [
                "--memory-budget-bytes",
                "8589934592",
                "--memory-budget-bytes",
                "8589934592"
            ]
            .into_iter()
            .map(str::to_owned)
        )
        .is_err()
    );
}

#[test]
fn runtime_candidate_leases_compete_with_database_caches() {
    let root = tempfile::tempdir().unwrap();
    let mut options = peer_retry_test_options(true, Arc::new(RuntimeControl::default()));
    options.data_dir = Some(root.path().to_owned());
    bind_runtime_memory(&options).unwrap();
    let memory = runtime_memory(&options);
    // Leave less than a candidate reservation after simulated engine ownership.
    let caches = memory.reserve(memory.snapshot().limit - 100).unwrap();
    let budget = runtime_admission_budget(&options);
    let candidate = budget.reserve_candidate(60).unwrap();
    assert!(
        runtime_admission_budget(&options)
            .reserve_candidate(41)
            .is_err()
    );
    assert_eq!(budget.snapshot().candidate_bytes, 60);
    assert_eq!(memory.snapshot().used, memory.snapshot().limit - 40);
    drop(candidate);
    drop(caches);
    assert_eq!(memory.snapshot().used, 0);
}

#[test]
fn background_startup_counts_both_caches_and_status_exposes_live_usage() {
    let mut options = peer_retry_test_options(true, Arc::new(RuntimeControl::default()));
    options.background_assumeutxo = Some(PathBuf::from("validation"));
    options.resources.memory_budget_bytes = 8 * 1024 * 1024 * 1024;
    assert!(validate_memory_plan(&options).is_err());
    options.resources.memory_budget_bytes = 13 * 1024 * 1024 * 1024;
    validate_memory_plan(&options).unwrap();
    let memory = runtime_memory(&options);
    let mut status = ready_test_node_status(
        bitcoin::blockdata::constants::genesis_block(Network::Regtest).block_hash(),
    );
    status.memory = Some(memory.clone());
    let lease = memory.reserve(1024).unwrap();
    let value = serde_json::to_value(status.response()).unwrap();
    assert_eq!(value["memory_reservations"]["used"], 1024);
    drop(lease);
    assert_eq!(status.response().memory_reservations.unwrap().used, 0);
    assert_eq!(status.response().memory_reservations.unwrap().peak, 1024);
    options.resources.memory_budget_bytes += 1;
    assert!(
        bind_runtime_memory(&options)
            .unwrap_err()
            .contains("live runtime")
    );
}

#[test]
fn staged_prefix_validation_is_ordered_bounded_and_never_publishes() {
    let directory = tempfile::tempdir().unwrap();
    let ledger = PrunedBlockLedger::open(directory.path(), LedgerRetention::default()).unwrap();
    let blocks: Vec<_> = (0_u8..35).map(|n| vec![n]).collect();
    ledger.stage(10, &blocks).unwrap();
    let identity = ledger.staged_manifest().unwrap().unwrap();
    let mut seen = Vec::new();
    assert!(
        visit_staged_prefix(&ledger, &identity, 35, |height, raw| {
            seen.push((height, raw[0]));
            Ok(true)
        })
        .unwrap()
    );
    assert_eq!(
        seen,
        (0_u8..35)
            .map(|n| (10 + u32::from(n), n))
            .collect::<Vec<_>>()
    );
    assert!(ledger.retained_tip().unwrap().is_none());
    seen.clear();
    assert!(
        !visit_staged_prefix(&ledger, &identity, 35, |height, raw| {
            seen.push((height, raw[0]));
            Ok(height < 27)
        })
        .unwrap()
    );
    assert_eq!(seen.len(), 18);
    assert_eq!(seen.last(), Some(&(27, 17)));
    assert_eq!(
        visit_staged_prefix(&ledger, &identity, 35, |_, _| Err(
            "injected validation failure".to_owned()
        ))
        .unwrap_err(),
        "injected validation failure"
    );
    assert_eq!(ledger.staged_manifest().unwrap().unwrap(), identity);
    assert!(ledger.retained_tip().unwrap().is_none());
}

#[test]
fn archive_admission_failure_reaches_the_node_as_a_local_resource_error() {
    let directory = tempfile::tempdir().unwrap();
    let memory = crate::node_memory::MemoryBudget::new(16 * 1024 * 1024);
    memory.bind(&[directory.path().to_path_buf()]).unwrap();
    let ledger = PrunedBlockLedger::open(directory.path(), LedgerRetention::default()).unwrap();
    let pressure = memory.reserve_spool(memory.spool_snapshot().limit).unwrap();
    let error = ledger.stage(1, &[vec![1]]).unwrap_err();
    assert!(matches!(
        &error,
        crate::ledger::LedgerError::Archive(crate::archive::ArchiveError::ResourceBudget(_))
    ));
    assert_eq!(
        PeerRunError::ledger(&error).kind,
        PeerFailureKind::LocalBudget(crate::node_memory::ReservationKind::ExecutionSpool)
    );
    assert!(ledger.staged_manifest().unwrap().is_none());
    drop(pressure);
    let pressure = memory.reserve(memory.snapshot().limit).unwrap();
    let error = ledger.stage(1, &[vec![1]]).unwrap_err();
    assert_eq!(
        PeerRunError::ledger(&error).kind,
        PeerFailureKind::LocalBudget(crate::node_memory::ReservationKind::Memory)
    );
    assert!(ledger.staged_manifest().unwrap().is_none());
    assert_eq!(memory.spool_snapshot().used, 0);
    drop(pressure);
    ledger.stage(1, &[vec![1]]).unwrap();
    assert_eq!(memory.spool_snapshot().used, 0);
    assert!(ledger.staged_manifest().unwrap().is_some());
}

#[test]
fn replay_payload_admission_follows_prevalidation_staging_and_aliases() {
    let directory = tempfile::tempdir().unwrap();
    let ledger_directory = directory.path().join("ledger");
    let config = DeploymentConfig::for_network(Network::Regtest);
    let genesis = bitcoin::constants::genesis_block(Network::Regtest);
    let block = submitted_regtest_block(genesis.block_hash(), 1, unix_time().unwrap());
    let mut dag = HeaderDag::with_deployments(config.clone());
    dag.insert(block.header).unwrap();
    let headers = NodeHeaderState::test_seed(dag, &directory.path().join("headers.redb"));
    let ledger = PrunedBlockLedger::open(&ledger_directory, LedgerRetention::default()).unwrap();
    ledger.append(1, &[serialize(&block)]).unwrap();
    let memory = crate::node_memory::MemoryBudget::new(16 * 1024 * 1024);
    memory.bind(&[ledger_directory]).unwrap();
    let batch = ledger.read_block_batch(1, 1, 1_000_000).unwrap();
    let used = memory.snapshot().used;
    assert!(used > serialize(&block).len() as u64);
    let cloned_batch = batch.clone();
    assert_eq!(memory.snapshot().used, used);
    let validated = prevalidate_replay_blocks(&config, &headers, 1, batch.blocks).unwrap();
    assert_eq!(memory.snapshot().used, used);
    assert_eq!(validated[0].bytes.as_ptr(), cloned_batch.blocks[0].as_ptr());
    let mut prefetch = PrefetchedBlocks {
        validated,
        ..PrefetchedBlocks::default()
    };
    drop(cloned_batch);
    let payload_used = memory.snapshot().used;
    assert!(payload_used > 0 && payload_used < used);
    let bytes = prefetch.validated.pop().unwrap().bytes;
    let alias = bytes.clone();
    let serialized = vec![bytes];
    ledger.stage(2, &serialized).unwrap();
    drop(serialized);
    drop(prefetch);
    assert_eq!(memory.snapshot().used, payload_used);
    assert_eq!(alias.as_ref(), serialize(&block));
    drop(alias);
    assert_eq!(memory.snapshot().used, 0);
    let single = ledger.read_owned_block(1).unwrap().unwrap();
    assert_eq!(memory.snapshot().used, payload_used);
    drop(single);
    assert_eq!(memory.snapshot().used, 0);
}

#[test]
fn memory_retry_adopts_new_attempt_stage_and_refuses_replacements() {
    use crate::execution_store::ExecutionTip;
    use crate::node_memory::ReservationKind;
    let directory = tempfile::tempdir().unwrap();
    let ledger = PrunedBlockLedger::open(directory.path(), LedgerRetention::default()).unwrap();
    let before = ExecutionTip {
        height: 0,
        hash: bitcoin::blockdata::constants::genesis_block(Network::Regtest).block_hash(),
    };
    let memory = PeerFailureKind::LocalBudget(ReservationKind::Memory);
    let mut expected_stage = None;
    let mut next = |kind, size, after, scripts| {
        next_memory_retry(
            kind,
            size,
            before,
            after,
            &ledger,
            scripts,
            &mut expected_stage,
        )
    };
    let mut size = 9;
    let mut sizes = Vec::new();
    while let Some(smaller) = next(memory, size, Some(before), false) {
        sizes.push(smaller);
        size = smaller;
    }
    assert_eq!(sizes, [4, 2, 1]);
    for kind in [
        PeerFailureKind::LocalResource,
        PeerFailureKind::Transient,
        PeerFailureKind::ProtocolViolation,
        PeerFailureKind::LocalBudget(ReservationKind::ExecutionSpool),
    ] {
        assert_eq!(next(kind, 9, Some(before), false), None);
    }
    assert_eq!(next(memory, 0, Some(before), false), None);
    assert_eq!(next(memory, 9, None, false), None);
    assert_eq!(next(memory, 9, Some(before), true), None);
    assert_eq!(
        next(
            memory,
            9,
            Some(ExecutionTip {
                height: 1,
                ..before
            }),
            false
        ),
        None
    );
    assert_eq!(
        next(
            memory,
            9,
            Some(ExecutionTip {
                hash: BlockHash::all_zeros(),
                ..before
            }),
            false
        ),
        None
    );
    ledger
        .stage(1, &[vec![7], vec![8], vec![9], vec![10]])
        .unwrap();
    let identity = ledger.staged_manifest().unwrap().unwrap();
    assert_eq!(next(memory, 9, Some(before), false), Some(2));
    assert_eq!(ledger.staged_manifest().unwrap().unwrap(), identity);
    assert!(ledger.retained_tip().unwrap().is_none());
    let path = directory.path().join("ledger-staged.rblk");
    fs::write(&path, b"corrupt stage must remain intact").unwrap();
    assert_eq!(next(memory, 9, Some(before), false), None);
    assert_eq!(fs::read(path).unwrap(), b"corrupt stage must remain intact");
}

#[test]
fn staged_memory_retry_requires_the_original_identity_and_uncommitted_tip() {
    use crate::execution_store::ExecutionTip;
    use crate::node_memory::ReservationKind;
    let directory = tempfile::tempdir().unwrap();
    let ledger = PrunedBlockLedger::open(directory.path(), LedgerRetention::default()).unwrap();
    ledger
        .stage(1, &[vec![1], vec![2], vec![3], vec![4]])
        .unwrap();
    let identity = ledger.staged_manifest().unwrap().unwrap();
    let path = directory.path().join("ledger-staged.rblk");
    let original = fs::read(&path).unwrap();
    let before = ExecutionTip {
        height: 0,
        hash: bitcoin::blockdata::constants::genesis_block(Network::Regtest).block_hash(),
    };
    let memory = PeerFailureKind::LocalBudget(ReservationKind::Memory);
    let mut expected_stage = Some(identity.clone());
    let mut next = |kind, size, after, scripts| {
        next_memory_retry(
            kind,
            size,
            before,
            after,
            &ledger,
            scripts,
            &mut expected_stage,
        )
    };
    assert_eq!(next(memory, 100, Some(before), false), Some(2));
    assert_eq!(next(memory, 2, Some(before), false), Some(1));
    assert_eq!(next(memory, 1, Some(before), false), None);
    assert_eq!(next(memory, 0, Some(before), false), None);
    assert_eq!(next(memory, 4, None, false), None);
    assert_eq!(next(memory, 4, Some(before), true), None);
    for after in [
        ExecutionTip {
            height: 1,
            ..before
        },
        ExecutionTip {
            hash: BlockHash::all_zeros(),
            ..before
        },
    ] {
        assert_eq!(next(memory, 4, Some(after), false), None);
    }
    for kind in [
        PeerFailureKind::LocalResource,
        PeerFailureKind::Transient,
        PeerFailureKind::ProtocolViolation,
        PeerFailureKind::LocalBudget(ReservationKind::ExecutionSpool),
    ] {
        assert_eq!(next(kind, 4, Some(before), false), None);
    }
    assert_eq!(fs::read(&path).unwrap(), original);
    ledger.discard_staged().unwrap();
    assert_eq!(next(memory, 4, Some(before), false), None);
    ledger
        .stage(1, &[vec![9], vec![8], vec![7], vec![6]])
        .unwrap();
    assert_eq!(next(memory, 4, Some(before), false), None);
    let replacement = fs::read(&path).unwrap();
    assert_ne!(replacement, original);
    assert_eq!(fs::read(&path).unwrap(), replacement);
    fs::write(&path, b"corrupt identity is not retryable").unwrap();
    assert_eq!(next(memory, 4, Some(before), false), None);
    assert_eq!(
        fs::read(&path).unwrap(),
        b"corrupt identity is not retryable"
    );
    assert!(ledger.retained_tip().unwrap().is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn memory_retry_replays_smaller_windows_without_duplicate_commits() {
    let directory = tempfile::tempdir().unwrap();
    let genesis = bitcoin::blockdata::constants::genesis_block(Network::Regtest);
    let mut headers = HeaderDag::new(Network::Regtest);
    let mut blocks = Vec::new();
    for height in 1..=4 {
        let template = crate::block_assembly::BlockTemplate::regtest(
            headers.active_tip().hash,
            height,
            genesis.header.time + height,
        );
        let mut block = crate::block_assembly::build_block(&template).unwrap();
        let mut script = vec![0; 8192];
        script[0] = 0x6a; // Provably unspendable outputs: payload, not chainstate growth.
        block.txdata[0]
            .output
            .extend((0..32).map(|_| bitcoin::TxOut {
                value: bitcoin::Amount::ZERO,
                script_pubkey: ScriptBuf::from_bytes(script.clone()),
            }));
        block.header.merkle_root = block.compute_merkle_root().unwrap();
        let block = crate::block_assembly::grind_block(block, template.target).unwrap();
        headers.insert(block.header).unwrap();
        blocks.push(serialize(&block));
    }
    let corpus_path = directory.path().join("corpus");
    let corpus = PrunedBlockLedger::open(&corpus_path, LedgerRetention::default()).unwrap();
    corpus.append(1, &blocks).unwrap();
    let memory = crate::node_memory::MemoryBudget::new(64 * 1024 * 1024);
    memory.bind(&[corpus_path]).unwrap();
    drop(
        corpus
            .read_block_batch(1, 1, REPLAY_BATCH_MAX_BYTES)
            .unwrap(),
    );
    let one_peak = memory.snapshot().peak;
    drop(
        corpus
            .read_block_batch(1, 4, REPLAY_BATCH_MAX_BYTES)
            .unwrap(),
    );
    assert!(memory.snapshot().peak > one_peak);
    assert_eq!(memory.snapshot().used, 0);
    let pressure = memory.reserve(memory.snapshot().limit - one_peak).unwrap();
    assert!(
        corpus
            .read_block_batch(1, 4, REPLAY_BATCH_MAX_BYTES)
            .is_err()
    );
    drop(
        corpus
            .read_block_batch(1, 1, REPLAY_BATCH_MAX_BYTES)
            .unwrap(),
    );
    let chainstate =
        RedbChainStore::open(directory.path().join("chainstate.redb"), Network::Regtest).unwrap();
    let ledger = PrunedBlockLedger::open(
        directory.path().join("destination"),
        LedgerRetention::default(),
    )
    .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let remote = listener.local_addr().unwrap();
    let server = tokio::spawn(accept_peer(listener, peer_version(9_101)));
    let mut session = connect_outbound(
        remote,
        Network::Regtest.magic(),
        9_102,
        "/rbtc:memory-retry/".to_owned(),
        0,
    )
    .await
    .unwrap();
    let (peer, _) = server.await.unwrap();
    let indexes = AuxiliaryIndexes {
        transaction: None,
        spent_output: None,
        basic_filter: None,
    };
    let pool = Arc::new(Mutex::new(TransactionAdmissionPool::default()));
    let mut prefetched = PrefetchedBlocks::default();
    let mut scripts = Vec::new();
    let mut auxiliary = None;
    // Even a single block cannot fit with one extra byte held: retries must
    // terminate with the original typed pressure and leave durable state alone.
    let last_byte = memory.reserve(1).unwrap();
    let error = timeout(
        Duration::from_secs(20),
        download_execute_batch(
            &mut session,
            &DeploymentConfig::for_network(Network::Regtest),
            &headers,
            &chainstate,
            &ledger,
            None,
            None,
            None,
            &indexes,
            &[],
            &pool,
            Some(4),
            4,
            &mut auxiliary,
            &mut prefetched,
            &mut scripts,
            false,
            Some(&corpus),
            None,
        ),
    )
    .await
    .expect("single-block pressure terminates")
    .unwrap_err();
    assert_eq!(
        error.kind,
        PeerFailureKind::LocalBudget(crate::node_memory::ReservationKind::Memory)
    );
    assert_eq!(chainstate.execution_tip().unwrap().height, 0);
    assert!(ledger.staged_manifest().unwrap().is_none());
    assert!(ledger.retained_tip().unwrap().is_none());
    assert_eq!(
        memory.snapshot().used,
        memory.snapshot().limit - one_peak + 1
    );
    drop(last_byte);
    for height in 1..=4 {
        timeout(
            Duration::from_secs(20),
            download_execute_batch(
                &mut session,
                &DeploymentConfig::for_network(Network::Regtest),
                &headers,
                &chainstate,
                &ledger,
                None,
                None,
                None,
                &indexes,
                &[],
                &pool,
                Some(4),
                4,
                &mut auxiliary,
                &mut prefetched,
                &mut scripts,
                false,
                Some(&corpus),
                None,
            ),
        )
        .await
        .expect("finite retry finishes")
        .unwrap();
        assert_eq!(chainstate.execution_tip().unwrap().height, height);
        assert!(ledger.staged_manifest().unwrap().is_none());
        assert!(scripts.is_empty());
    }
    for (offset, expected) in blocks.iter().enumerate() {
        assert_eq!(
            ledger
                .read_block(u32::try_from(offset + 1).unwrap())
                .unwrap()
                .unwrap(),
            *expected
        );
    }
    drop(pressure);
    assert_eq!(memory.snapshot().used, 0);
    drop(peer);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn staged_memory_retry_calibrates_publication_and_stops_after_commit() {
    let directory = tempfile::tempdir().unwrap();
    let genesis = bitcoin::blockdata::constants::genesis_block(Network::Regtest);
    let mut headers = HeaderDag::new(Network::Regtest);
    let mut blocks = Vec::new();
    for height in 1..=16 {
        let template = crate::block_assembly::BlockTemplate::regtest(
            headers.active_tip().hash,
            height,
            genesis.header.time + height,
        );
        let mut block = crate::block_assembly::build_block(&template).unwrap();
        let mut script = vec![0; 8192];
        script[0] = 0x6a;
        block.txdata[0]
            .output
            .extend((0..96).map(|_| bitcoin::TxOut {
                value: bitcoin::Amount::ZERO,
                script_pubkey: ScriptBuf::from_bytes(script.clone()),
            }));
        block.header.merkle_root = block.compute_merkle_root().unwrap();
        let block = crate::block_assembly::grind_block(block, template.target).unwrap();
        headers.insert(block.header).unwrap();
        blocks.push(serialize(&block));
    }
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let remote = listener.local_addr().unwrap();
    let server = tokio::spawn(accept_peer(listener, peer_version(9_301)));
    let mut session = connect_outbound(
        remote,
        Network::Regtest.magic(),
        9_302,
        "/rbtc:staged-pressure/".to_owned(),
        0,
    )
    .await
    .unwrap();
    let (peer, _) = server.await.unwrap();
    let mut read_peak = 0;
    let mut staging_peak = 0;
    let mut checkpoint_peak = 0;
    // Separate fresh fixtures calibrate and exercise ownership, without copying
    // databases or weakening the actual production reservations.
    for case in ["calibrate", "retry", "postcommit"] {
        let root = directory.path().join(case);
        fs::create_dir_all(&root).unwrap();
        let path = root.join("blocks");
        let ledger = PrunedBlockLedger::open(&path, LedgerRetention::default()).unwrap();
        ledger.stage(1, &blocks).unwrap();
        let original = fs::read(path.join("ledger-staged.rblk")).unwrap();
        let identity = ledger.staged_manifest().unwrap().unwrap();
        let chainstate =
            RedbChainStore::open(root.join("chainstate.redb"), Network::Regtest).unwrap();
        let memory = crate::node_memory::MemoryBudget::new(64 * 1024 * 1024);
        memory.bind(std::slice::from_ref(&path)).unwrap();
        if case == "calibrate" {
            // A fresh attempt can create its immutable stage before execution
            // reaches a memory denial. Adopt that exact stage as the retry
            // identity while the durable execution tip remains unchanged.
            let before = chainstate.execution_tip().unwrap();
            let mut expected_stage = None;
            assert_eq!(
                next_memory_retry(
                    PeerFailureKind::LocalBudget(crate::node_memory::ReservationKind::Memory),
                    16,
                    before,
                    Some(before),
                    &ledger,
                    false,
                    &mut expected_stage,
                ),
                Some(8)
            );
            assert_eq!(expected_stage, Some(identity.clone()));

            // Wrapper and attempt each hold a returned admitted identity.
            let outer = ledger.staged_manifest().unwrap().unwrap();
            let inner = ledger.staged_manifest().unwrap().unwrap();
            drop(
                ledger
                    .read_staged_batch(&inner, 1, 1, crate::archive::MAX_RECORDS_BYTES)
                    .unwrap(),
            );
            read_peak = memory.snapshot().peak;
            // Validation now retains an admitted serialization while staging
            // verifies the immutable source, before durable execution begins.
            let block = deserialize::<Block>(&blocks[0]).unwrap();
            let encoded = ledger.serialize_block(&block).unwrap();
            ledger.stage_or_verify(1, &[encoded], Some(&inner)).unwrap();
            staging_peak = memory.snapshot().peak;
            assert!(staging_peak > read_peak);
            drop((outer, inner));
            staged_pressure_batch(&mut session, &headers, &chainstate, &ledger, 1)
                .await
                .unwrap();
            checkpoint_peak = memory.snapshot().peak;
            assert!(
                checkpoint_peak > staging_peak,
                "publication needs more than read and staging admission"
            );
            assert_eq!(chainstate.execution_tip().unwrap().height, 1);
            drop(expected_stage);
            assert_eq!(memory.snapshot().used, 0);
            continue;
        }
        let allowance = if case == "retry" {
            checkpoint_peak
        } else {
            staging_peak
        };
        let pressure = memory.reserve(memory.snapshot().limit - allowance).unwrap();
        {
            let outer = ledger.staged_manifest().unwrap().unwrap();
            let inner = ledger.staged_manifest().unwrap().unwrap();
            assert!(
                ledger
                    .read_staged_batch(&inner, 1, 16, crate::archive::MAX_RECORDS_BYTES)
                    .is_err()
            );
            drop((outer, inner));
        }
        if case == "retry" {
            // A one-byte deficit in the calibrated one-block read must stop
            // the finite retry sequence before any durable execution changes.
            let extra = memory.reserve(checkpoint_peak - read_peak + 1).unwrap();
            let error = staged_pressure_batch(&mut session, &headers, &chainstate, &ledger, 16)
                .await
                .unwrap_err();
            assert_eq!(
                error.kind,
                PeerFailureKind::LocalBudget(crate::node_memory::ReservationKind::Memory)
            );
            assert_eq!(chainstate.execution_tip().unwrap().height, 0);
            assert!(ledger.retained_tip().unwrap().is_none());
            assert_eq!(fs::read(path.join("ledger-staged.rblk")).unwrap(), original);
            assert_eq!(
                memory.snapshot().used,
                memory.snapshot().limit - read_peak + 1
            );
            drop(extra);
        }
        let result = staged_pressure_batch(&mut session, &headers, &chainstate, &ledger, 16).await;
        let advanced = chainstate.execution_tip().unwrap().height;
        assert!(
            advanced > 0 && advanced < 16,
            "{case}: batch must shrink before it can execute; height={advanced}, result={result:?}"
        );
        if case == "postcommit" {
            assert_eq!(
                result.unwrap_err().kind,
                PeerFailureKind::LocalBudget(crate::node_memory::ReservationKind::Memory)
            );
            assert_eq!(
                advanced, 1,
                "publication failure must not replay an executed batch"
            );
            assert!(ledger.retained_tip().unwrap().is_none());
            assert_eq!(fs::read(path.join("ledger-staged.rblk")).unwrap(), original);
            assert_eq!(memory.snapshot().used, memory.snapshot().limit - allowance);
            drop(pressure);
            reconcile_overlay_stage(
                &DeploymentConfig::for_network(Network::Regtest),
                &headers,
                chainstate.execution_tip().unwrap(),
                &ledger,
            )
            .unwrap();
            assert_eq!(chainstate.execution_tip().unwrap().height, 1);
            assert_eq!(ledger.read_block(1).unwrap().unwrap(), blocks[0]);
        } else {
            result.unwrap();
            assert_eq!(ledger.retained_tip().unwrap(), Some(advanced));
            assert_eq!(fs::read(path.join("ledger-staged.rblk")).unwrap(), original);
            while chainstate.execution_tip().unwrap().height < 16 {
                let before = chainstate.execution_tip().unwrap().height;
                staged_pressure_batch(&mut session, &headers, &chainstate, &ledger, 16)
                    .await
                    .unwrap();
                let after = chainstate.execution_tip().unwrap().height;
                assert!(after > before && after <= 16);
                assert_eq!(ledger.retained_tip().unwrap(), Some(after));
                if after < 16 {
                    assert_eq!(ledger.staged_manifest().unwrap().unwrap(), identity);
                }
            }
            assert!(ledger.staged_manifest().unwrap().is_none());
            for (offset, expected) in blocks.iter().enumerate() {
                assert_eq!(
                    ledger
                        .read_block(u32::try_from(offset + 1).unwrap())
                        .unwrap()
                        .unwrap(),
                    *expected
                );
            }
            drop(pressure);
        }
        assert_eq!(memory.snapshot().used, 0);
        assert_eq!(memory.spool_snapshot().used, 0);
    }
    drop(peer);
}

async fn staged_pressure_batch(
    session: &mut rbtc::p2p::PeerSession<tokio::net::TcpStream>,
    headers: &HeaderDag,
    chainstate: &RedbChainStore,
    ledger: &PrunedBlockLedger,
    limit: usize,
) -> Result<(), PeerRunError> {
    let indexes = AuxiliaryIndexes {
        transaction: None,
        spent_output: None,
        basic_filter: None,
    };
    let pool = Arc::new(Mutex::new(TransactionAdmissionPool::default()));
    let mut scripts = Vec::new();
    let result = timeout(
        Duration::from_secs(30),
        download_execute_batch(
            session,
            &DeploymentConfig::for_network(Network::Regtest),
            headers,
            chainstate,
            ledger,
            None,
            None,
            None,
            &indexes,
            &[],
            &pool,
            Some(16),
            limit,
            &mut None,
            &mut PrefetchedBlocks::default(),
            &mut scripts,
            false,
            None,
            None,
        ),
    )
    .await
    .expect("finite staged retry terminates");
    assert!(scripts.is_empty());
    result
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn staged_reuse_survives_reopen_and_preserves_a_larger_suffix() {
    exercise_staged_checkpoint_recovery(false, false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn overlay_staged_reuse_recovers_an_executed_prefix_and_keeps_its_suffix() {
    exercise_staged_checkpoint_recovery(true, false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn staged_reuse_recovers_committed_prefix_before_discarding_stale_suffix() {
    exercise_staged_checkpoint_recovery(false, true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn overlay_staged_reuse_recovers_committed_prefix_before_discarding_stale_suffix() {
    exercise_staged_checkpoint_recovery(true, true).await;
}

#[allow(clippy::too_many_lines)]
async fn exercise_staged_checkpoint_recovery(overlay: bool, stale_suffix: bool) {
    let directory = tempfile::tempdir().unwrap();
    let genesis = bitcoin::blockdata::constants::genesis_block(Network::Regtest);
    let deployments = DeploymentConfig::for_network(Network::Regtest);
    let mut headers = HeaderDag::new(Network::Regtest);
    let mut blocks = Vec::new();
    for height in 1..=2 {
        let block =
            crate::block_assembly::assemble_block(&crate::block_assembly::BlockTemplate::regtest(
                headers.active_tip().hash,
                height,
                genesis.header.time + height,
            ))
            .unwrap();
        headers.insert(block.header).unwrap();
        blocks.push(serialize(&block));
    }
    let path = directory.path().join("blocks");
    let ledger = PrunedBlockLedger::open(&path, LedgerRetention::default()).unwrap();
    ledger.stage(1, &blocks).unwrap();
    let identity = ledger.staged_manifest().unwrap().unwrap();
    let bytes_before = fs::read(path.join("ledger-staged.rblk")).unwrap();
    drop(ledger);
    let ledger = PrunedBlockLedger::open_persisted(&path).unwrap();
    let chainstate =
        RedbChainStore::open(directory.path().join("chainstate.redb"), Network::Regtest).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let remote = listener.local_addr().unwrap();
    let server = tokio::spawn(accept_peer(listener, peer_version(9_201)));
    let mut session = connect_outbound(
        remote,
        Network::Regtest.magic(),
        9_202,
        "/rbtc:staged-reuse/".to_owned(),
        0,
    )
    .await
    .unwrap();
    let (peer, _) = server.await.unwrap();
    timeout(
        Duration::from_secs(10),
        reconcile_ledger(
            &mut session,
            &deployments,
            &headers,
            chainstate.execution(),
            &ledger,
            &[],
        ),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(ledger.staged_manifest().unwrap().unwrap(), identity);
    let indexes = AuxiliaryIndexes {
        transaction: None,
        spent_output: None,
        basic_filter: None,
    };
    let pool = Arc::new(Mutex::new(TransactionAdmissionPool::default()));
    let mut prefetched = PrefetchedBlocks::default();
    let mut scripts = Vec::new();
    let mut auxiliary = None;
    timeout(
        Duration::from_secs(10),
        download_execute_batch(
            &mut session,
            &deployments,
            &headers,
            &chainstate,
            &ledger,
            None,
            None,
            None,
            &indexes,
            &[],
            &pool,
            Some(1),
            1,
            &mut auxiliary,
            &mut prefetched,
            &mut scripts,
            false,
            None,
            None,
        ),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(chainstate.execution_tip().unwrap().height, 1);
    assert_eq!(ledger.read_block(1).unwrap().unwrap(), blocks[0]);
    assert!(ledger.read_block(2).unwrap().is_none());
    // Model execution commit surviving while ledger publication is absent.
    // Recovery must publish from the retained stage without executing block 1 again.
    ledger.truncate_from(1).unwrap();
    drop(ledger);
    drop(chainstate);
    let chainstate =
        RedbChainStore::open(directory.path().join("chainstate.redb"), Network::Regtest).unwrap();
    let ledger = PrunedBlockLedger::open_persisted(&path).unwrap();
    if stale_suffix {
        let first: Block = deserialize(&blocks[0]).unwrap();
        headers = HeaderDag::new(Network::Regtest);
        headers.insert(first.header).unwrap();
        let replacement =
            crate::block_assembly::assemble_block(&crate::block_assembly::BlockTemplate::regtest(
                first.block_hash(),
                2,
                genesis.header.time + 3,
            ))
            .unwrap();
        headers.insert(replacement.header).unwrap();
        assert_ne!(
            replacement.block_hash(),
            deserialize::<Block>(&blocks[1]).unwrap().block_hash()
        );
    }
    if overlay {
        reconcile_overlay_stage(
            &deployments,
            &headers,
            chainstate.execution_tip().unwrap(),
            &ledger,
        )
        .unwrap();
    } else {
        timeout(
            Duration::from_secs(10),
            reconcile_ledger(
                &mut session,
                &deployments,
                &headers,
                chainstate.execution(),
                &ledger,
                &[],
            ),
        )
        .await
        .unwrap()
        .unwrap();
    }
    assert_eq!(chainstate.execution_tip().unwrap().height, 1);
    assert_eq!(ledger.read_block(1).unwrap().unwrap(), blocks[0]);
    if stale_suffix {
        assert!(ledger.staged_manifest().unwrap().is_none());
        assert!(ledger.read_block(2).unwrap().is_none());
        drop(peer);
        return;
    }
    assert_eq!(ledger.staged_manifest().unwrap().unwrap(), identity);
    assert_eq!(
        fs::read(path.join("ledger-staged.rblk")).unwrap(),
        bytes_before
    );
    let memory = crate::node_memory::MemoryBudget::new(64 * 1024 * 1024);
    memory.bind(std::slice::from_ref(&path)).unwrap();
    let pressure = memory.reserve(memory.snapshot().limit).unwrap();
    let denied = timeout(
        Duration::from_secs(10),
        download_execute_batch(
            &mut session,
            &deployments,
            &headers,
            &chainstate,
            &ledger,
            None,
            None,
            None,
            &indexes,
            &[],
            &pool,
            Some(2),
            2,
            &mut auxiliary,
            &mut prefetched,
            &mut scripts,
            false,
            None,
            None,
        ),
    )
    .await
    .unwrap()
    .unwrap_err();
    assert_eq!(
        denied.kind,
        PeerFailureKind::LocalBudget(crate::node_memory::ReservationKind::Memory)
    );
    assert_eq!(chainstate.execution_tip().unwrap().height, 1);
    assert_eq!(
        fs::read(path.join("ledger-staged.rblk")).unwrap(),
        bytes_before
    );
    drop(pressure);
    assert_eq!(memory.snapshot().used, 0);
    // Staged bytes supersede unrelated speculative input, without any block
    // request: the connected peer only completed the handshake and is idle.
    prefetched.serialized.push(vec![99].into());
    timeout(
        Duration::from_secs(10),
        download_execute_batch(
            &mut session,
            &deployments,
            &headers,
            &chainstate,
            &ledger,
            None,
            None,
            None,
            &indexes,
            &[],
            &pool,
            Some(2),
            2,
            &mut auxiliary,
            &mut prefetched,
            &mut scripts,
            false,
            None,
            None,
        ),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(chainstate.execution_tip().unwrap().height, 2);
    assert!(ledger.staged_manifest().unwrap().is_none());
    for (offset, expected) in blocks.iter().enumerate() {
        assert_eq!(
            ledger
                .read_block(u32::try_from(offset + 1).unwrap())
                .unwrap()
                .unwrap(),
            *expected
        );
    }
    drop(peer);
}

#[test]
fn staged_reuse_requires_identity_and_exact_selected_bytes() {
    let directory = tempfile::tempdir().unwrap();
    let ledger = PrunedBlockLedger::open(directory.path(), LedgerRetention::default()).unwrap();
    let blocks = [vec![1], vec![2]];
    ledger.stage(1, &blocks).unwrap();
    let identity = ledger.staged_manifest().unwrap().unwrap();
    let before = fs::read(directory.path().join("ledger-staged.rblk")).unwrap();
    ledger.stage_or_verify(1, &blocks, Some(&identity)).unwrap();
    ledger
        .stage_or_verify(1, &blocks[..1], Some(&identity))
        .unwrap();
    ledger
        .stage_or_verify(2, &blocks[1..], Some(&identity))
        .unwrap();
    for (first, candidate) in [
        (2, blocks.to_vec()),
        (1, vec![]),
        (2, vec![vec![1]]),
        (1, vec![vec![1], vec![3]]),
    ] {
        assert!(
            ledger
                .stage_or_verify(first, &candidate, Some(&identity))
                .is_err()
        );
        assert_eq!(
            fs::read(directory.path().join("ledger-staged.rblk")).unwrap(),
            before
        );
    }
    ledger.discard_staged().unwrap();
    ledger.stage(1, &[vec![3], vec![4]]).unwrap();
    let replacement = ledger.staged_manifest().unwrap().unwrap();
    assert!(ledger.stage_or_verify(1, &blocks, Some(&identity)).is_err());
    assert_eq!(ledger.staged_manifest().unwrap().unwrap(), replacement);
    assert!(ledger.retained_tip().unwrap().is_none());
}

#[test]
fn validated_serialization_keeps_shared_admission_and_refunds_parallel_failures() {
    let directory = tempfile::tempdir().unwrap();
    let ledger = PrunedBlockLedger::open(directory.path(), LedgerRetention::default()).unwrap();
    let memory = crate::node_memory::MemoryBudget::new(64 * 1024 * 1024);
    memory.bind(&[directory.path().to_path_buf()]).unwrap();
    let headers = HeaderDag::new(Network::Regtest);
    let block = bitcoin::blockdata::constants::genesis_block(Network::Regtest);
    let deployments = DeploymentConfig::for_network(Network::Regtest);
    let encoded = ledger.serialize_block(&block).unwrap();
    assert_eq!(encoded.as_ref(), serialize(&block));
    let per_block = memory.snapshot().used;
    assert!(per_block > block.total_size() as u64);
    let alias = encoded.clone();
    drop(encoded);
    assert_eq!(memory.snapshot().used, per_block);
    drop(alias);
    assert_eq!(memory.snapshot().used, 0);

    let blocks = vec![block; 192];
    let expected = vec![headers.active_tip(); blocks.len()];
    // Allow all but the last encoded byte. Partially completed parallel workers
    // must join and refund their payloads without staging or blaming the peer.
    let allowance = per_block * blocks.len() as u64 - 1;
    let pressure = memory.reserve(memory.snapshot().limit - allowance).unwrap();
    let error = validate_downloaded_blocks(&deployments, &headers, &expected, &blocks, &ledger)
        .unwrap_err();
    assert_eq!(
        error.kind,
        PeerFailureKind::LocalBudget(crate::node_memory::ReservationKind::Memory)
    );
    assert_eq!(memory.snapshot().used, memory.snapshot().limit - allowance);
    assert!(ledger.staged_manifest().unwrap().is_none());
    assert!(ledger.retained_tip().unwrap().is_none());
    drop(pressure);
    let validated =
        validate_downloaded_blocks(&deployments, &headers, &expected, &blocks, &ledger).unwrap();
    assert_eq!(memory.snapshot().used, per_block * blocks.len() as u64);
    for (result, block) in validated.iter().zip(&blocks) {
        assert_eq!(result.2.as_ref(), serialize(block));
    }
    let carried = validated[0].2.clone();
    drop(validated);
    assert_eq!(memory.snapshot().used, per_block);
    drop(carried);
    assert_eq!(memory.snapshot().used, 0);

    // The exact preallocation must also include witness marker/flag and stacks.
    let mut witness_block = blocks[0].clone();
    witness_block.txdata[0].input[0].witness.push([42; 253]);
    let encoded = ledger.serialize_block(&witness_block).unwrap();
    assert_eq!(encoded.as_ref(), serialize(&witness_block));
    drop(encoded);
    assert_eq!(memory.snapshot().used, 0);
}
