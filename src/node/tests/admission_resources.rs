use super::*;
use crate::admission_resources::{AdmissionBudget, AdmissionResourceLimits};

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
    for allowance in [0, 24_000_000] {
        let budget = AdmissionBudget::new(AdmissionResourceLimits {
            work_burst: allowance,
            work_per_second: 0,
            candidate_bytes: 512 * 1024 * 1024,
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
        let pool = pool.lock().unwrap();
        assert!(pool.is_empty());
        assert!(!pool.is_recently_rejected(transaction.compute_txid()));
        assert_eq!(budget.snapshot().candidate_bytes, 0);
        assert_eq!(budget.snapshot().charged.iter().sum::<u64>(), allowance);
    }
    done.send(()).unwrap();
    server.await.unwrap();
}
