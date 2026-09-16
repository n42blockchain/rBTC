use super::*;
use crate::admission_resources::{AdmissionBudget, AdmissionResourceLimits};

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn peer_admission_recovers_orphans_persists_replacements_and_rejects_bad_scripts() {
    let directory = TempDir::new().unwrap();
    let (source, funding, funded) = dry_run_test_source(directory.path());
    let spend = |input, value, script: Vec<u8>| Transaction {
        version: TransactionVersion::TWO,
        lock_time: LockTime::ZERO,
        input: vec![TxIn {
            previous_output: input,
            script_sig: ScriptBuf::from_bytes(script),
            sequence: Sequence::ENABLE_RBF_NO_LOCKTIME,
            witness: Witness::new(),
        }],
        output: vec![TxOut {
            value: Amount::from_sat(value),
            script_pubkey: ScriptBuf::from_bytes(funded.script_pubkey.clone()),
        }],
    };
    let parent = spend(funding, funded.value_sats - 10_000, vec![1, 0x51]);
    let child = spend(
        OutPoint::new(parent.compute_txid(), 0),
        funded.value_sats - 20_000,
        vec![1, 0x51],
    );
    let store =
        RedbTransactionPoolStore::open(directory.path().join("pool.redb"), Network::Regtest)
            .unwrap();
    let estimator =
        RedbFeeEstimator::open(directory.path().join("fees.redb"), Network::Regtest).unwrap();
    let (relay, mut receiver) = broadcast::channel(16);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let remote = listener.local_addr().unwrap();
    let server = tokio::spawn(accept_peer(listener, peer_version(9_021)));
    let mut session = connect_outbound(
        remote,
        Network::Regtest.magic(),
        9_022,
        "/rbtc:admission-test/".to_owned(),
        0,
    )
    .await
    .unwrap();
    let (peer, _) = server.await.unwrap();
    let headers = source.headers.read().unwrap();
    let admit = |session: &mut PeerSession<tokio::net::TcpStream>| {
        admit_pending_peer_transactions(
            session,
            &source.transaction_pool,
            Some(&store),
            Some(&estimator),
            &VecDeque::new(),
            &relay,
            &source.chainstate,
            &headers,
            &source.deployments,
            true,
            None,
        )
        .unwrap()
    };
    session.queue_pending_transaction(child.clone()).unwrap();
    let progress = admit(&mut session);
    assert_eq!(progress.parent_requests, vec![parent.compute_txid()]);
    assert!(!progress.resource_deferred);
    assert_eq!(source.transaction_pool.lock().unwrap().orphan_len(), 1);
    assert!(store.transactions().unwrap().is_empty());
    session.queue_pending_transaction(parent.clone()).unwrap();
    admit(&mut session);
    let mut accepted = store
        .transactions()
        .unwrap()
        .iter()
        .map(Transaction::compute_txid)
        .collect::<Vec<_>>();
    accepted.sort_unstable();
    let mut expected = vec![parent.compute_txid(), child.compute_txid()];
    expected.sort_unstable();
    assert_eq!(accepted, expected);
    assert_eq!(source.transaction_pool.lock().unwrap().orphan_len(), 0);
    let mut relayed = vec![
        receiver.try_recv().unwrap().transaction.compute_txid(),
        receiver.try_recv().unwrap().transaction.compute_txid(),
    ];
    relayed.sort_unstable();
    assert_eq!(relayed, expected);
    let replacement = spend(funding, funded.value_sats - 100_000, vec![1, 0x51]);
    session
        .queue_pending_transaction(replacement.clone())
        .unwrap();
    admit(&mut session);
    assert_eq!(store.transactions().unwrap(), vec![replacement.clone()]);
    assert_eq!(
        source.transaction_pool.lock().unwrap().snapshot(),
        vec![replacement.clone()]
    );
    assert_eq!(
        receiver.try_recv().unwrap().transaction.compute_txid(),
        replacement.compute_txid()
    );
    let invalid = spend(
        OutPoint::new(replacement.compute_txid(), 0),
        funded.value_sats - 110_000,
        vec![1, 0x52],
    );
    session.queue_pending_transaction(invalid.clone()).unwrap();
    admit(&mut session);
    assert!(
        source
            .transaction_pool
            .lock()
            .unwrap()
            .is_recently_rejected(invalid.compute_txid())
    );
    assert_eq!(store.transactions().unwrap(), vec![replacement.clone()]);
    // A child of a known rejected transaction must not consume orphan capacity or request it again.
    let rejected_child = spend(
        OutPoint::new(invalid.compute_txid(), 0),
        funded.value_sats - 120_000,
        vec![1, 0x51],
    );
    session
        .queue_pending_transaction(rejected_child.clone())
        .unwrap();
    assert!(admit(&mut session).parent_requests.is_empty());
    assert!(
        source
            .transaction_pool
            .lock()
            .unwrap()
            .is_recently_rejected(rejected_child.compute_txid())
    );
    assert_eq!(source.transaction_pool.lock().unwrap().orphan_len(), 0);
    assert!(receiver.try_recv().is_err());
    drop(store);
    let reopened =
        RedbTransactionPoolStore::open(directory.path().join("pool.redb"), Network::Regtest)
            .unwrap();
    assert_eq!(reopened.transactions().unwrap(), vec![replacement]);
    drop(peer);
}

#[test]
fn dry_run_resource_deferral_is_an_error_not_an_invalid_transaction_verdict() {
    let directory = TempDir::new().unwrap();
    let (source, funding, _) = dry_run_test_source(directory.path());
    let mut transaction = bitcoin::blockdata::constants::genesis_block(Network::Regtest)
        .txdata
        .remove(0);
    transaction.input[0].previous_output = funding;
    let budget = AdmissionBudget::new(AdmissionResourceLimits {
        // Reach the inner admission call, then exhaust its first payload charge.
        work_burst: u64::try_from(transaction.total_size()).unwrap() * 2 + 4 * 1024 * 1024,
        work_per_second: 0,
        candidate_bytes: 16 * 1024 * 1024,
    });
    *source.transaction_pool.lock().unwrap() =
        TransactionAdmissionPool::default().with_admission_budget(budget.clone());
    let error = source.dry_run_admission(vec![transaction]).unwrap_err();
    assert!(error.contains("resource deferred"));
    assert!(source.transaction_pool.lock().unwrap().is_empty());
    assert_eq!(budget.snapshot().candidate_bytes, 0);
}

#[tokio::test]
async fn peer_resource_deferral_preserves_queue_before_and_after_draining() {
    let directory = TempDir::new().unwrap();
    let chainstate =
        RedbChainStore::open(directory.path().join("chain.redb"), Network::Regtest).unwrap();
    let headers = HeaderDag::new(Network::Regtest);
    let deployments = DeploymentConfig::for_network(Network::Regtest);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let remote = listener.local_addr().unwrap();
    let (done, wait_done) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(async move {
        let (_peer, _) = accept_peer(listener, peer_version(811)).await;
        let _ = wait_done.await;
    });
    let mut session = connect_outbound(
        remote,
        Network::Regtest.magic(),
        810,
        "/rbtcd:test/".to_owned(),
        0,
    )
    .await
    .unwrap();
    let mut transaction = bitcoin::blockdata::constants::genesis_block(Network::Regtest)
        .txdata
        .remove(0);
    transaction.input[0].previous_output = OutPoint::new(Txid::from_byte_array([1; 32]), 0);
    let (relay, _) = broadcast::channel(8);
    for (allowance, memory_limit) in [
        (0, 512 * 1024 * 1024),
        (24_000_000, 512 * 1024 * 1024),
        // Work succeeds but pipeline memory cannot be reserved. The pool
        // guard must be released and the undrained queue must remain intact.
        (24_000_000, 0),
    ] {
        let budget = AdmissionBudget::new(AdmissionResourceLimits {
            work_burst: allowance,
            work_per_second: 0,
            candidate_bytes: memory_limit,
        });
        // A large configured ceiling must not reserve a full empty pool. The
        // second allowance covers actual snapshot estimates, then runs out
        // after the peer queue has been drained and before candidate cloning.
        let pool = Arc::new(Mutex::new(
            TransactionAdmissionPool::with_capacity(300_000, 1024 * 1024 * 1024)
                .with_admission_budget(budget.clone()),
        ));
        session
            .queue_pending_transaction(transaction.clone())
            .unwrap();
        let progress = admit_pending_peer_transactions(
            &mut session,
            &pool,
            None,
            None,
            &VecDeque::new(),
            &relay,
            &chainstate,
            &headers,
            &deployments,
            false,
            None,
        )
        .unwrap();
        assert!(progress.resource_deferred);
        assert!(!progress.more_orphan_work);
        assert!(progress.parent_requests.is_empty());
        assert_eq!(
            session.take_pending_transactions(),
            vec![transaction.clone()]
        );
        let pool = pool
            .try_lock()
            .expect("deferral must release the pool guard");
        assert!(pool.is_empty());
        assert!(!pool.is_recently_rejected(transaction.compute_txid()));
        assert_eq!(budget.snapshot().candidate_bytes, 0);
        assert_eq!(budget.snapshot().charged.iter().sum::<u64>(), allowance);
    }
    done.send(()).unwrap();
    server.await.unwrap();
}
