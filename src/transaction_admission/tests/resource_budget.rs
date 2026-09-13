use super::*;
use crate::admission_resources::AdmissionResourceLimits;

#[test]
fn rejection_and_pool_clones_cannot_reset_the_shared_allowance() {
    let (_directory, store) = store();
    let (_, _, tx) = spend(1);
    let budget = AdmissionBudget::new(AdmissionResourceLimits {
        work_burst: 1,
        work_per_second: 0,
        candidate_bytes: 16 * 1024 * 1024,
    });
    let mut pool = TransactionAdmissionPool::default().with_admission_budget(budget.clone());
    let error = pool.admit(&store, tx.clone(), context()).unwrap_err();
    assert!(matches!(
        error,
        TransactionAdmissionError::ResourceDeferred(_)
    ));
    let mut legacy = tx.clone();
    legacy.input[0].witness.clear();
    assert!(!pool.remember_terminal_rejection(&legacy, &error));
    assert!(!pool.is_recently_rejected(legacy.compute_txid()));
    assert!(matches!(
        pool.clone().admit(&store, tx.clone(), context()),
        Err(TransactionAdmissionError::ResourceDeferred(_))
    ));
    assert!(pool.is_empty());
    assert!(!pool.is_recently_rejected(tx.compute_txid()));
    assert_eq!(budget.snapshot().candidate_bytes, 0);
    assert_eq!(
        budget.snapshot().charged[AdmissionStage::Payload as usize],
        1
    );
}

#[test]
fn deferred_reconciliation_keeps_candidates_and_withholds_stale_payload_views() {
    let (_directory, store) = store();
    let (first_key, first_utxo, first) = spend(1);
    let (second_key, second_utxo, second) = spend(2);
    store
        .apply(
            &[],
            &[
                (first_key.into(), first_utxo),
                (second_key.into(), second_utxo),
            ],
        )
        .unwrap();
    let mut pool = TransactionAdmissionPool::default();
    pool.admit(&store, first.clone(), context()).unwrap();
    pool.admit(&store, second.clone(), context()).unwrap();
    // The first entry is invalid in the new view. Exhaust work before the
    // second finishes, after the private candidate has already removed the first.
    store.apply(&[first_key.into()], &[]).unwrap();
    let metadata = 2 * 512 + 2 * 128 + 4 * 1024 * 1024;
    let first_preparation = 1024 + u64::try_from(first.total_size()).unwrap() * 3;
    let budget = AdmissionBudget::new(AdmissionResourceLimits {
        work_burst: metadata + 2 * 4096 + first_preparation,
        work_per_second: 0,
        candidate_bytes: 16 * 1024 * 1024,
    });
    pool = pool.with_admission_budget(budget.clone());
    pool.require_revalidation(BlockHash::from_byte_array([42; 32]));
    assert!(pool.snapshot().is_empty());
    assert!(pool.relay_snapshot().is_empty());
    assert!(matches!(
        pool.reconcile(&store, context()),
        Err(TransactionAdmissionError::ResourceDeferred(_))
    ));
    assert_eq!(
        pool.len(),
        2,
        "the failed candidate cannot publish its first removal"
    );
    assert!(pool.requires_revalidation());
    assert!(!pool.is_recently_rejected(first.compute_txid()));
    assert_eq!(budget.snapshot().candidate_bytes, 0);
    // Restoring scheduler capacity still requires the actual fresh validation.
    pool = pool.with_admission_budget(AdmissionBudget::default());
    assert_eq!(pool.reconcile(&store, context()).unwrap(), 1);
    assert!(!pool.requires_revalidation());
    assert_eq!(pool.snapshot(), vec![second]);
}
