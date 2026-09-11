use super::*;

fn seeded_pool() -> (
    TempDir,
    RedbUtxoStore,
    TransactionAdmissionPool,
    Transaction,
) {
    let (directory, store) = store();
    let (outpoint, utxo, transaction) = spend(1);
    store.apply(&[], &[(outpoint.into(), utxo)]).unwrap();
    let mut pool = TransactionAdmissionPool::default();
    pool.admit(&store, transaction.clone(), context()).unwrap();
    let (_, _, mut orphan) = spend(2);
    orphan.output.push(TxOut {
        value: Amount::ZERO,
        script_pubkey: Builder::new()
            .push_opcode(opcodes::all::OP_RETURN)
            .push_slice(PushBytesBuf::try_from(vec![0; 60_000]).unwrap())
            .into_script(),
    });
    assert_eq!(pool.retain_orphans(&[orphan], 1, 7), 1);
    (directory, store, pool, transaction)
}

#[test]
fn known_package_preserves_pool_allocations_and_still_advances_fee_decay() {
    for alternate_witness in [false, true] {
        let (_directory, store, mut pool, mut transaction) = seeded_pool();
        if alternate_witness {
            transaction.input[0].witness = Witness::new();
        }
        let entries = pool.entries.as_slices().0.as_ptr();
        let orphan_script = pool.orphans[0].transaction.output[1]
            .script_pubkey
            .as_bytes()
            .as_ptr();
        let snapshot = pool.snapshot();
        let positions = pool.positions.clone();
        let spent = pool.spent.clone();
        let orphan_bytes = pool.orphan_bytes;
        pool.rolling_minimum_fee_sat_kvb = 600.25;
        pool.rolling_fee_decay_enabled = true;
        let expected_rate = 600.25 / 2_f64.powf(11.0 / 10_800.0);
        CANDIDATE_POOL_CLONES.with(|clones| clones.set(0));
        ADMISSION_VALIDATION_RUNS.with(|runs| runs.set(0));
        let outcome = pool
            .admit_package_at(&store, vec![transaction], context(), 11)
            .unwrap();
        assert_eq!(outcome.already_present, 1);
        assert!(outcome.accepted.is_empty());
        assert!(outcome.evicted.is_empty());
        assert!(outcome.replaced.is_empty());
        assert_eq!(CANDIDATE_POOL_CLONES.with(std::cell::Cell::get), 0);
        assert_eq!(ADMISSION_VALIDATION_RUNS.with(std::cell::Cell::get), 0);
        assert_eq!(pool.entries.as_slices().0.as_ptr(), entries);
        assert_eq!(
            pool.orphans[0].transaction.output[1]
                .script_pubkey
                .as_bytes()
                .as_ptr(),
            orphan_script
        );
        assert_eq!(pool.snapshot(), snapshot);
        assert_eq!(pool.positions, positions);
        assert_eq!(pool.spent, spent);
        assert_eq!(pool.orphan_bytes, orphan_bytes);
        assert_eq!(
            pool.rolling_minimum_fee_sat_kvb.to_bits(),
            expected_rate.to_bits()
        );
        assert_eq!(pool.rolling_fee_last_update, 11);
    }
}

#[test]
fn duplicate_package_is_rejected_before_cloning_pool_or_orphan_payloads() {
    for already_retained in [false, true] {
        let (_directory, store, mut pool, transaction) = seeded_pool();
        let candidate = if already_retained {
            transaction
        } else {
            spend(3).2
        };
        let mut variant = candidate.clone();
        variant.input[0].witness = Witness::new();
        let before = pool.snapshot();
        CANDIDATE_POOL_CLONES.with(|clones| clones.set(0));
        ADMISSION_VALIDATION_RUNS.with(|runs| runs.set(0));
        assert!(matches!(
            pool.admit_package(&store, vec![candidate.clone(), variant], context()),
            Err(TransactionAdmissionError::DuplicatePackageTransaction(txid))
                if txid == candidate.compute_txid()
        ));
        assert_eq!(CANDIDATE_POOL_CLONES.with(std::cell::Cell::get), 0);
        assert_eq!(ADMISSION_VALIDATION_RUNS.with(std::cell::Cell::get), 0);
        assert_eq!(pool.snapshot(), before);
        assert_eq!(pool.orphan_len(), 1);
    }
}

#[test]
fn mixed_package_keeps_incumbent_witness_and_validates_new_child_atomically() {
    for valid_child in [false, true] {
        let (_directory, store, mut pool, mut incumbent) = seeded_pool();
        let retained = Arc::clone(&pool.entries[0].transaction);
        let before = pool.snapshot();
        let mut child = child(&incumbent, 80_000);
        if !valid_child {
            child.input[0].witness = Witness::new();
        }
        let child_txid = child.compute_txid();
        // Package substitution uses the original witness even when this
        // supplied incumbent variant cannot pass SCRIPT verification.
        incumbent.input[0].witness = Witness::new();
        CANDIDATE_POOL_CLONES.with(|clones| clones.set(0));
        ADMISSION_VALIDATION_RUNS.with(|runs| runs.set(0));
        let result = pool.admit_package(&store, vec![child, incumbent], context());
        assert_eq!(CANDIDATE_POOL_CLONES.with(std::cell::Cell::get), 1);
        assert_eq!(ADMISSION_VALIDATION_RUNS.with(std::cell::Cell::get), 2);
        if valid_child {
            let outcome = result.unwrap();
            assert_eq!(outcome.already_present, 1);
            assert_eq!(outcome.accepted, vec![child_txid]);
            assert_eq!(pool.len(), 2);
        } else {
            assert!(result.is_err());
            assert_eq!(pool.snapshot(), before);
        }
        assert!(Arc::ptr_eq(&pool.entries[0].transaction, &retained));
        assert_eq!(pool.orphan_len(), 1);
    }
}
