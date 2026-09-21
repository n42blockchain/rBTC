use std::sync::atomic::{AtomicUsize, Ordering};

use super::*;
use crate::admission_resources::AdmissionResourceLimits;

/// Wraps a base store to count `get` calls, proving prevout lookups never
/// run once the overlay's precharge has already refused or deferred a
/// candidate.
struct CountingStore<'a, S> {
    inner: &'a S,
    gets: AtomicUsize,
}

impl<'a, S: UtxoStore> CountingStore<'a, S> {
    fn new(inner: &'a S) -> Self {
        Self {
            inner,
            gets: AtomicUsize::new(0),
        }
    }

    fn get_calls(&self) -> usize {
        self.gets.load(Ordering::SeqCst)
    }
}

impl<S: UtxoStore> UtxoStore for CountingStore<'_, S> {
    fn get(&self, outpoint: OutPointKey) -> Result<Option<Utxo>, UtxoError> {
        self.gets.fetch_add(1, Ordering::SeqCst);
        self.inner.get(outpoint)
    }

    fn apply(
        &self,
        spent: &[OutPointKey],
        created: &[(OutPointKey, Utxo)],
    ) -> Result<(), UtxoError> {
        self.inner.apply(spent, created)
    }

    fn apply_with_undo(
        &self,
        spent: &[OutPointKey],
        created: &[(OutPointKey, Utxo)],
    ) -> Result<UtxoUndo, UtxoError> {
        self.inner.apply_with_undo(spent, created)
    }

    fn undo(&self, undo: &UtxoUndo, now: u64, hot_window_secs: u64) -> Result<(), UtxoError> {
        self.inner.undo(undo, now, hot_window_secs)
    }

    fn age_to_cold(&self, now: u64, hot_window_secs: u64) -> Result<u64, UtxoError> {
        self.inner.age_to_cold(now, hot_window_secs)
    }

    fn snapshot_entries(&self) -> Result<BTreeMap<OutPointKey, Utxo>, UtxoError> {
        self.inner.snapshot_entries()
    }

    fn replace_all(
        &self,
        entries: &BTreeMap<OutPointKey, Utxo>,
        now: u64,
        hot_window_secs: u64,
    ) -> Result<(), UtxoError> {
        self.inner.replace_all(entries, now, hot_window_secs)
    }

    fn tier_stats(&self) -> Result<TierStats, UtxoError> {
        self.inner.tier_stats()
    }
}

/// Builds a transaction with `inputs` distinct, unresolved prevouts. Only
/// its shape matters for resource-capacity tests; the referenced outpoints
/// need not exist in any store.
fn many_inputs_transaction(inputs: u16) -> Transaction {
    let witness_script = Builder::new().push_opcode(opcodes::OP_TRUE).into_script();
    let input = (0..inputs)
        .map(|index| {
            let mut txid_bytes = [0_u8; 32];
            txid_bytes[..2].copy_from_slice(&index.to_le_bytes());
            TxIn {
                previous_output: OutPoint::new(Txid::from_byte_array(txid_bytes), 0),
                script_sig: ScriptBuf::new(),
                sequence: Sequence::MAX,
                witness: Witness::from_slice(&[witness_script.as_bytes()]),
            }
        })
        .collect();
    Transaction {
        version: Version::TWO,
        lock_time: LockTime::ZERO,
        input,
        output: vec![TxOut {
            value: Amount::from_sat(1),
            script_pubkey: ScriptBuf::new_p2wsh(&witness_script.wscript_hash()),
        }],
    }
}

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

#[test]
fn prevout_precharge_defers_before_any_base_store_lookup() {
    let (_directory, store) = store();
    let (outpoint, utxo, tx) = spend(1);
    store.apply(&[], &[(outpoint.into(), utxo)]).unwrap();
    let counting = CountingStore::new(&store);
    // Exactly enough for every charge before the new prevout precharge:
    // package/traversal/byte payload, candidate metadata, the graph charge,
    // and the coarse `charge_preparation` estimate. Nothing is left for the
    // precharge itself, so it must defer without ever calling `get`.
    let payload = 1 + 2 + u64::try_from(tx.total_size()).unwrap() * 3;
    let metadata = 4 * 1024 * 1024;
    let graph = 4096;
    let charge_preparation = 1024 + u64::try_from(tx.total_size()).unwrap() * 3;
    let budget = AdmissionBudget::new(AdmissionResourceLimits {
        work_burst: payload + metadata + graph + charge_preparation,
        work_per_second: 0,
        candidate_bytes: 16 * 1024 * 1024,
    });
    let mut pool = TransactionAdmissionPool::default().with_admission_budget(budget);
    let error = pool.admit(&counting, tx, context()).unwrap_err();
    assert!(matches!(
        error,
        TransactionAdmissionError::ResourceDeferred(_)
    ));
    assert_eq!(
        counting.get_calls(),
        0,
        "the base store must not be touched once the precharge itself defers"
    );
    assert!(pool.is_empty(), "a deferred candidate leaves the pool unchanged");
}

#[test]
fn prevout_script_clone_is_charged_under_script_stage() {
    let (_directory, store) = store();
    let (outpoint, utxo, tx) = spend(1);
    let script_len = u64::try_from(utxo.script_pubkey.len()).unwrap();
    store.apply(&[], &[(outpoint.into(), utxo)]).unwrap();
    let budget = AdmissionBudget::default();
    let mut pool = TransactionAdmissionPool::default().with_admission_budget(budget.clone());
    pool.admit(&store, tx, context()).unwrap();
    // The OP_TRUE witness script carries no sigops, so the Script stage
    // charge is exactly the `prevout_scripts` clone (script bytes), proving
    // the clone allocation is charged and not left free.
    assert_eq!(
        budget.snapshot().charged[AdmissionStage::Script as usize],
        script_len
    );
}

#[test]
fn oversized_candidate_is_permanently_refused_while_a_normal_one_still_admits() {
    let (_directory, store) = store();
    let (outpoint, utxo, normal_tx) = spend(1);
    store.apply(&[], &[(outpoint.into(), utxo)]).unwrap();
    let limits = AdmissionResourceLimits {
        // Large enough for the fixed ~4 MiB candidate-metadata charge plus a
        // single normal input, but smaller than 200 inputs' worst-case
        // prevout precharge (200 * 30_087 = 6_017_400), which can never fit
        // regardless of refill.
        work_burst: 6_000_000,
        work_per_second: 0,
        candidate_bytes: 16 * 1024 * 1024,
    };

    let oversized_budget = AdmissionBudget::new(limits);
    let mut oversized_pool =
        TransactionAdmissionPool::default().with_admission_budget(oversized_budget);
    let oversized_tx = many_inputs_transaction(200);
    let error = oversized_pool
        .admit(&store, oversized_tx, context())
        .unwrap_err();
    assert!(
        matches!(error, TransactionAdmissionError::CandidateUnfittable(_)),
        "an unfittable candidate must be a permanent refusal, not {error:?}"
    );
    assert!(oversized_pool.is_empty());

    let normal_budget = AdmissionBudget::new(limits);
    let mut normal_pool = TransactionAdmissionPool::default().with_admission_budget(normal_budget);
    assert!(matches!(
        normal_pool.admit(&store, normal_tx, context()),
        Ok(TransactionAdmissionOutcome::Accepted { .. })
    ));
}
