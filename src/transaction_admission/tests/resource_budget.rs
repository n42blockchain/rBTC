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
fn indexed_measures_match_relay_and_withhold_stale_chain_values() {
    let (_directory, store) = store();
    let (outpoint, utxo, transaction) = spend(1);
    store.apply(&[], &[(outpoint.into(), utxo)]).unwrap();
    let txid = transaction.compute_txid();
    let mut pool = TransactionAdmissionPool::default();
    pool.admit(&store, transaction, context()).unwrap();
    let relay = pool.relay_snapshot();
    let before = pool.admission_budget().snapshot().charged;
    assert_eq!(
        pool.validated_measures(txid),
        Some((relay[0].policy_vsize, relay[0].fee_sats))
    );
    assert_eq!(
        pool.validated_measures(Txid::from_byte_array([99; 32])),
        None
    );
    assert_eq!(pool.admission_budget().snapshot().charged, before);
    pool.require_revalidation(BlockHash::from_byte_array([42; 32]));
    assert_eq!(pool.validated_measures(txid), None);
    pool.reconcile(&store, context()).unwrap();
    assert_eq!(
        pool.validated_measures(txid),
        Some((relay[0].policy_vsize, relay[0].fee_sats))
    );
}

#[test]
fn rejection_and_pool_clones_cannot_reset_the_shared_allowance() {
    let (_directory, store) = store();
    let (_, _, tx) = spend(1);
    let limits = AdmissionResourceLimits {
        // Comfortably above the candidate's own conservative worst-case
        // estimate (dominated by the flat
        // `MAX_STANDARD_TRANSACTION_SIGOP_COST` sigop bound charged for a
        // not-yet-validated transaction), so the up-front gate's total
        // capacity check passes and only the drained allowance below defers.
        work_burst: 200_000_000,
        work_per_second: 0,
        candidate_bytes: 16 * 1024 * 1024,
    };
    let budget = AdmissionBudget::new(limits);
    // Leave an allowance above zero but far below this candidate's
    // conservative worst-case estimate, so the up-front gate defers
    // (retryable) instead of refusing the candidate permanently.
    budget
        .charge(AdmissionStage::Graph, limits.work_burst - 1)
        .unwrap();
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
    // Cloning the pool and retrying cannot manufacture free work either:
    // the up-front gate never charges anything for a deferred candidate,
    // so only the earlier real Graph charge is ever recorded.
    assert_eq!(
        budget.snapshot().charged[AdmissionStage::Graph as usize],
        limits.work_burst - 1
    );
    assert_eq!(
        budget.snapshot().charged[AdmissionStage::Payload as usize],
        0
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
    // precharge itself: this ledger's own capacity can never cover this
    // candidate's conservative estimate, so the up-front gate refuses it
    // permanently before any stage charge and before `get` is ever called.
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
        TransactionAdmissionError::CandidateUnfittable(_)
    ));
    assert_eq!(
        counting.get_calls(),
        0,
        "the base store must not be touched once the up-front gate refuses"
    );
    assert!(
        pool.is_empty(),
        "a refused candidate leaves the pool unchanged"
    );
}

#[test]
fn sigop_worst_case_bound_defers_up_front_without_store_lookups() {
    let (_directory, store) = store();
    let (outpoint, utxo, tx) = spend(1);
    store.apply(&[], &[(outpoint.into(), utxo)]).unwrap();
    let counting = CountingStore::new(&store);
    // The candidate's own conservative worst-case estimate, computed the
    // same way `candidate_work_estimate` does for a not-yet-validated
    // transaction: it now includes `MAX_STANDARD_TRANSACTION_SIGOP_COST`'s
    // Script-stage charge, since the real sigop cost is unknown before
    // prevout resolution.
    let sigop_worst_case = MAX_STANDARD_TRANSACTION_SIGOP_COST.saturating_mul(10_000);
    let estimate = 1
        + payload_traversal_charge(&tx)
        + payload_bytes_charge(&tx)
        + apply_to_overlay_estimate(&tx, None)
        + 4 * 1024 * 1024
        + graph_charge_estimate(0, 1);
    let budget = AdmissionBudget::new(AdmissionResourceLimits {
        // Exactly the candidate's own worst-case estimate, so the up-front
        // total-capacity check passes and only the drained allowance below
        // decides whether it defers.
        work_burst: estimate,
        work_per_second: 0,
        candidate_bytes: 16 * 1024 * 1024,
    });
    // Drain exactly the sigop worst-case term: the remaining allowance
    // covers every other up-front charge (payload, metadata, graph, prevout
    // preparation and precharge, script bytes) but not the Script stage's
    // `MAX_STANDARD_TRANSACTION_SIGOP_COST` bound this not-yet-validated
    // candidate has not proven it stays under. Before this bound was added
    // to the estimate, this exact allowance wrongly admitted the candidate.
    budget
        .charge(AdmissionStage::Graph, sigop_worst_case)
        .unwrap();
    let before = budget.snapshot();
    let mut pool = TransactionAdmissionPool::default().with_admission_budget(budget.clone());
    let error = pool.admit(&counting, tx, context()).unwrap_err();
    assert!(matches!(
        error,
        TransactionAdmissionError::ResourceDeferred(_)
    ));
    assert_eq!(
        counting.get_calls(),
        0,
        "the up-front gate defers before any base-store lookup"
    );
    let after = budget.snapshot();
    assert_eq!(
        after.charged, before.charged,
        "no stage charge runs before the up-front gate defers"
    );
    assert!(pool.is_empty());
}

#[test]
fn replacement_diagram_work_is_included_in_the_up_front_candidate_estimate() {
    const CLUSTERS: usize = 3;
    const PARENTS_PER_CLUSTER: usize = MAX_MEMPOOL_CLUSTER_TRANSACTIONS - 1;

    let (_directory, store) = store();
    let mut rbf_context = context();
    rbf_context.full_rbf = true;
    let mut source = Vec::new();
    for index in 1..=(CLUSTERS * PARENTS_PER_CLUSTER) {
        source.push(spend(u8::try_from(index).unwrap()));
    }
    store
        .apply(
            &[],
            &source
                .iter()
                .map(|(outpoint, utxo, _)| ((*outpoint).into(), utxo.clone()))
                .collect::<Vec<_>>(),
        )
        .unwrap();

    let mut pool = TransactionAdmissionPool::with_capacity(512, 256 * 1024 * 1024);
    let mut parents = Vec::with_capacity(source.len());
    for (_, _, parent) in source {
        pool.admit(&store, parent.clone(), rbf_context).unwrap();
        parents.push(parent);
    }

    let witness_script = Builder::new().push_opcode(opcodes::OP_TRUE).into_script();
    let mut conflict_inputs = Vec::with_capacity(CLUSTERS);
    for group in parents.chunks(PARENTS_PER_CLUSTER) {
        let inputs = group
            .iter()
            .map(|parent| TxIn {
                previous_output: OutPoint::new(parent.compute_txid(), 0),
                script_sig: ScriptBuf::new(),
                sequence: Sequence::MAX,
                witness: Witness::from_slice(&[witness_script.as_bytes()]),
            })
            .collect::<Vec<_>>();
        let transaction = Transaction {
            version: Version::TWO,
            lock_time: LockTime::ZERO,
            input: inputs,
            output: vec![TxOut {
                value: Amount::from_sat(u64::try_from(group.len()).unwrap() * 90_000 - 10_000),
                script_pubkey: ScriptBuf::new_p2wsh(&witness_script.wscript_hash()),
            }],
        };
        pool.admit(&store, transaction.clone(), rbf_context)
            .unwrap();
        conflict_inputs.push(transaction.input[0].previous_output);
    }

    let replacement = Transaction {
        version: Version::TWO,
        lock_time: LockTime::ZERO,
        input: conflict_inputs
            .into_iter()
            .map(|previous_output| TxIn {
                previous_output,
                script_sig: ScriptBuf::new(),
                sequence: Sequence::MAX,
                witness: Witness::from_slice(&[witness_script.as_bytes()]),
            })
            .collect(),
        output: vec![TxOut {
            value: Amount::from_sat(70_000),
            script_pubkey: ScriptBuf::new_p2wsh(&witness_script.wscript_hash()),
        }],
    };

    // This is the former estimate: it accounts for transaction graph shape,
    // but not the three old clusters and resulting clusters whose diagrams
    // are independently linearized while evaluating this replacement.
    let former_estimate = 1_u64
        + payload_traversal_charge(&replacement)
        + payload_bytes_charge(&replacement)
        + apply_to_overlay_estimate(&replacement, None)
        + u64::try_from(pool.metadata_reservation_bytes()).unwrap()
        + graph_charge_estimate(pool.entries.len(), 1)
        + pool
            .entries
            .iter()
            .map(|entry| apply_to_overlay_estimate(&entry.transaction, Some(entry.sigop_cost)))
            .sum::<u64>();
    let budget = AdmissionBudget::new(AdmissionResourceLimits {
        work_burst: former_estimate,
        work_per_second: 0,
        candidate_bytes: 16 * 1024 * 1024,
    });
    pool = pool.with_admission_budget(budget.clone());
    let counting = CountingStore::new(&store);

    let error = pool.admit(&counting, replacement, rbf_context).unwrap_err();
    assert!(
        matches!(error, TransactionAdmissionError::CandidateUnfittable(_)),
        "the total-capacity preflight must include replacement diagram work: {error:?}"
    );
    assert_eq!(
        budget.snapshot().charged,
        [0; 6],
        "an unfittable replacement must be refused before stage work"
    );
    assert_eq!(
        counting.get_calls(),
        0,
        "the base store must not be touched when preflight refuses the replacement"
    );
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
        // Exactly the normal one-input candidate's own conservative
        // worst-case estimate (dominated by the flat
        // `MAX_STANDARD_TRANSACTION_SIGOP_COST` sigop bound charged for a
        // not-yet-validated transaction, plus the fixed ~4 MiB
        // candidate-metadata charge). 200 inputs' extra prevout precharge and
        // script-byte bound push the oversized candidate's own estimate past
        // this same ceiling, so it can never fit regardless of refill.
        work_burst: 1
            + payload_traversal_charge(&normal_tx)
            + payload_bytes_charge(&normal_tx)
            + apply_to_overlay_estimate(&normal_tx, None)
            + 4 * 1024 * 1024
            + graph_charge_estimate(0, 1),
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

#[test]
fn a_candidate_below_its_estimate_defers_atomically_then_admits_after_refill() {
    let (_directory, store) = store();
    let (outpoint, utxo, tx) = spend(1);
    store.apply(&[], &[(outpoint.into(), utxo)]).unwrap();
    let counting = CountingStore::new(&store);
    let limits = AdmissionResourceLimits {
        // Comfortably above this one-input candidate's conservative
        // worst-case estimate (dominated by the flat
        // `MAX_STANDARD_TRANSACTION_SIGOP_COST` sigop bound charged for a
        // not-yet-validated transaction), so the candidate fits the ledger's
        // total capacity.
        work_burst: 200_000_000,
        work_per_second: 0,
        candidate_bytes: 16 * 1024 * 1024,
    };
    let budget = AdmissionBudget::new(limits);
    // Simulate prior ledger activity that leaves an allowance above zero
    // but far below the candidate's estimate.
    budget
        .charge(AdmissionStage::Graph, limits.work_burst - 100)
        .unwrap();
    let before = budget.snapshot();
    let mut pool = TransactionAdmissionPool::default().with_admission_budget(budget.clone());
    let error = pool.admit(&counting, tx.clone(), context()).unwrap_err();
    assert!(matches!(
        error,
        TransactionAdmissionError::ResourceDeferred(_)
    ));
    assert_eq!(
        counting.get_calls(),
        0,
        "the up-front gate defers before any base-store lookup"
    );
    let after = budget.snapshot();
    assert_eq!(
        after.charged, before.charged,
        "no stage charge runs before the up-front gate defers"
    );
    assert!(pool.is_empty());

    // A fresh ledger under the same limits models the allowance recovering
    // through refill; the same candidate now admits.
    let refilled = AdmissionBudget::new(limits);
    let mut pool = TransactionAdmissionPool::default().with_admission_budget(refilled);
    assert!(matches!(
        pool.admit(&counting, tx, context()),
        Ok(TransactionAdmissionOutcome::Accepted { .. })
    ));
}

#[test]
fn repeated_deferrals_never_drain_the_ledger() {
    let (_directory, store) = store();
    let (_, _, tx) = spend(1);
    let limits = AdmissionResourceLimits {
        // Comfortably above the candidate's own conservative worst-case
        // estimate (dominated by the flat
        // `MAX_STANDARD_TRANSACTION_SIGOP_COST` sigop bound charged for a
        // not-yet-validated transaction), so the up-front gate's total
        // capacity check passes and only the drained allowance below defers.
        work_burst: 200_000_000,
        work_per_second: 0,
        candidate_bytes: 16 * 1024 * 1024,
    };
    let budget = AdmissionBudget::new(limits);
    budget
        .charge(AdmissionStage::Graph, limits.work_burst - 100)
        .unwrap();
    let pool = TransactionAdmissionPool::default().with_admission_budget(budget.clone());
    for _ in 0..5 {
        assert!(matches!(
            pool.clone().admit(&store, tx.clone(), context()),
            Err(TransactionAdmissionError::ResourceDeferred(_))
        ));
    }
    assert_eq!(
        budget.snapshot().charged[AdmissionStage::Graph as usize],
        limits.work_burst - 100,
        "repeated deferrals leave the one real prior charge untouched"
    );
    for stage in [
        AdmissionStage::Payload,
        AdmissionStage::Metadata,
        AdmissionStage::Prevout,
        AdmissionStage::Script,
        AdmissionStage::Snapshot,
    ] {
        assert_eq!(
            budget.snapshot().charged[stage as usize],
            0,
            "no other stage is ever charged by a deferred candidate"
        );
    }
}

#[test]
fn single_transaction_lookups_do_not_clone_the_pool() {
    let (_directory, store) = store();
    let (outpoint, utxo, tx) = spend(1);
    store.apply(&[], &[(outpoint.into(), utxo)]).unwrap();
    let mut pool = TransactionAdmissionPool::default();
    pool.admit(&store, tx.clone(), context()).unwrap();
    assert_eq!(pool.transaction(tx.compute_txid()), Some(tx.clone()));
    assert_eq!(
        pool.transaction_by_wtxid(tx.compute_wtxid()),
        Some(tx.clone())
    );
    let (_, _, absent) = spend(2);
    assert_eq!(pool.transaction(absent.compute_txid()), None);
    assert_eq!(pool.transaction_by_wtxid(absent.compute_wtxid()), None);
}

#[test]
fn prevout_materialization_is_reserved_before_lookup_and_overlay_delta_is_retained() {
    let (_directory, store) = store();
    let (outpoint, utxo, transaction) = spend(1);
    store.apply(&[], &[(outpoint.into(), utxo)]).unwrap();

    let required = transaction_materialization_reservation_bytes(&transaction);
    let budget = AdmissionBudget::new(AdmissionResourceLimits {
        work_burst: 1_000_000,
        work_per_second: 0,
        candidate_bytes: required.saturating_sub(1),
    });
    let overlay = AdmissionUtxoOverlay::new(&store, &budget);
    let error = match apply_to_overlay(&overlay, &transaction, context(), 0, None) {
        Ok(_) => panic!("materialization reservation must defer before lookup"),
        Err(error) => error,
    };
    assert!(matches!(
        error,
        TransactionAdmissionError::ResourceDeferred(_)
    ));
    assert_eq!(budget.snapshot().candidate_bytes, 0);
    assert!(overlay.get(outpoint.into()).unwrap().is_some());

    let budget = AdmissionBudget::new(AdmissionResourceLimits {
        work_burst: 1_000_000,
        work_per_second: 0,
        candidate_bytes: required + 1_024,
    });
    let overlay = AdmissionUtxoOverlay::new(&store, &budget);
    apply_to_overlay(&overlay, &transaction, context(), 0, None).unwrap();
    assert!(budget.snapshot().candidate_bytes > 0);
    drop(overlay);
    assert_eq!(budget.snapshot().candidate_bytes, 0);
}
