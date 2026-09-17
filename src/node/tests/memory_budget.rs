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
