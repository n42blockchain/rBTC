use super::*;

/// The pre-change growth pass, kept independent of the component projection.
fn reference_growth_pruning(pool: &mut TransactionAdmissionPool, larger: Vec<Txid>) {
    for txid in larger.into_iter().rev() {
        if pool.entry(txid).is_none() {
            continue;
        }
        if pool.validate_cluster_limits(&[txid]).is_err()
            || pool.validate_truc_policy(&[txid]).is_err()
        {
            pool.remove_with_descendants(&BTreeSet::from([txid]));
        }
    }
}

fn activation_fixture(
    count: usize,
    truc: bool,
) -> (TempDir, RedbUtxoStore, TransactionAdmissionPool) {
    let (directory, store) = store();
    let mut builder = Builder::new().push_int(0).push_opcode(opcodes::all::OP_IF);
    for _ in 0..190 {
        builder = builder.push_opcode(opcodes::all::OP_CHECKMULTISIG);
    }
    let witness_script = builder
        .push_opcode(opcodes::all::OP_ENDIF)
        .push_opcode(opcodes::OP_TRUE)
        .into_script();
    let script = ScriptBuf::new_p2wsh(&witness_script.wscript_hash());
    let mut transactions = Vec::with_capacity(count * 2);
    let mut funding = Vec::with_capacity(count);
    for index in 0..count {
        let (_, utxo, mut parent) = spend(1);
        let mut bytes = [0_u8; 32];
        bytes[..8].copy_from_slice(&u64::try_from(index + 1).unwrap().to_le_bytes());
        let outpoint = OutPoint::new(Txid::from_byte_array(bytes), 0);
        parent.input[0].previous_output = outpoint;
        parent.output[0].script_pubkey = script.clone();
        if truc {
            parent.version = Version(TRUC_VERSION);
        }
        let mut child = child(&parent, 80_000);
        child.version = parent.version;
        child.input[0].witness = Witness::from_slice(&[witness_script.as_bytes()]);
        transactions.extend([parent, child]);
        funding.push((outpoint.into(), utxo));
    }
    store.apply(&[], &funding).unwrap();
    let mut pool = unchecked_pool(transactions);
    pool.max_transactions = count * 2;
    let earlier = TransactionAdmissionContext {
        script_flags: bitcoinconsensus::VERIFY_P2SH,
        ..context()
    };
    // Populate fresh validation metadata without quadratic fixture admission.
    assert_eq!(pool.reconcile(&store, earlier), 0);
    let ids = txids(&pool);
    pool.validate_cluster_limits(&ids).unwrap();
    pool.validate_truc_policy(&ids).unwrap();
    (directory, store, pool)
}

fn assert_activation_work_is_local(truc: bool) {
    for count in [8, 32, 64] {
        let (_directory, store, mut pool) = activation_fixture(count, truc);
        let original = pool.clone();
        let base = store.snapshot_entries().unwrap();
        POLICY_INDEX_ENTRY_VISITS.with(|visits| visits.set(0));
        ADMISSION_VALIDATION_RUNS.with(|runs| runs.set(0));
        assert_eq!(
            pool.reconcile(&store, context()),
            if truc { count } else { 0 }
        );
        let visits = POLICY_INDEX_ENTRY_VISITS.with(std::cell::Cell::get);
        assert!(
            visits <= 4 * original.len(),
            "{count} independent pairs visited {visits} policy/index entries; budget {}",
            4 * original.len()
        );
        assert_eq!(
            ADMISSION_VALIDATION_RUNS.with(std::cell::Cell::get),
            count * 2
        );
        for entry in &pool.entries {
            let before = original.entry(entry.txid).unwrap();
            assert!(Arc::ptr_eq(&entry.transaction, &before.transaction));
        }
        let expected = original
            .entries
            .iter()
            .enumerate()
            .filter(|(index, _)| !truc || index % 2 == 0)
            .map(|(_, entry)| entry.txid)
            .collect::<Vec<_>>();
        assert_eq!(txids(&pool), expected);
        assert_eq!(store.snapshot_entries().unwrap(), base);
        assert_eq!(pool.reconcile(&store, context()), 0);
    }
}

#[test]
fn reconciliation_growth_keeps_valid_pairs_without_whole_pool_policy_scans() {
    assert_activation_work_is_local(false);
}

#[test]
fn reconciliation_growth_removes_truc_children_without_repeated_whole_pool_rebuilds() {
    assert_activation_work_is_local(true);
}

fn next_random(state: &mut u64) -> usize {
    *state = state
        .wrapping_mul(6_364_136_223_846_793_005)
        .wrapping_add(1);
    usize::try_from(*state >> 32).unwrap()
}

#[test]
fn reconciliation_growth_matches_global_reverse_pruning_on_interleaved_graphs() {
    for seed in 0..96_u64 {
        let mut random = seed + 1;
        let mut components = Vec::new();
        for (component, count) in [1, 2, 7, 16, 64].into_iter().enumerate() {
            let mut transactions = Vec::<Transaction>::new();
            for index in 0..count {
                let (_, _, mut transaction) = spend(u8::try_from(component + 1).unwrap());
                transaction.output = vec![transaction.output[0].clone(); count * 2];
                if component == 1 {
                    transaction.version = Version(TRUC_VERSION);
                }
                if index > 0 {
                    let parents = 1 + next_random(&mut random) % 2;
                    transaction.input = (0..parents)
                        .map(|edge| {
                            let parent = next_random(&mut random) % index;
                            let mut input = transaction.input[0].clone();
                            input.previous_output = OutPoint::new(
                                transactions[parent].compute_txid(),
                                u32::try_from(index * 2 + edge).unwrap(),
                            );
                            input
                        })
                        .collect();
                }
                transactions.push(transaction);
            }
            components.push(transactions);
        }
        // Interleave independent components while retaining dependency order.
        let mut transactions = Vec::new();
        for index in 0..64 {
            for component in &components {
                if let Some(transaction) = component.get(index) {
                    transactions.push(transaction.clone());
                }
            }
        }
        let mut pool = unchecked_pool(transactions);
        for entry in &mut pool.entries {
            entry.policy_vsize = 100 + next_random(&mut random) % 400;
        }
        pool.validate_cluster_limits(&txids(&pool)).unwrap();
        pool.validate_truc_policy(&txids(&pool)).unwrap();
        let mut larger = Vec::new();
        for entry in &mut pool.entries {
            if next_random(&mut random) % 3 != 0 {
                entry.policy_vsize += 1_000 + next_random(&mut random) % 45_000;
                larger.push(entry.txid);
            }
        }
        let original = pool.clone();
        let mut reference = pool.clone();
        reference_growth_pruning(&mut reference, larger.clone());
        pool.enforce_reconciled_growth(larger);
        assert_eq!(pool.snapshot(), reference.snapshot(), "seed {seed}");
        assert_eq!(pool.spent, reference.spent, "seed {seed}");
        assert_eq!(pool.positions, reference.positions, "seed {seed}");
        assert_eq!(pool.retained_bytes, reference.retained_bytes, "seed {seed}");
        for entry in &pool.entries {
            let expected = reference.entry(entry.txid).unwrap();
            assert_eq!(entry.policy_vsize, expected.policy_vsize);
            assert_eq!(entry.fee_sats, expected.fee_sats);
            assert_eq!(entry.script_verification, expected.script_verification);
            assert!(Arc::ptr_eq(
                &entry.transaction,
                &original.entry(entry.txid).unwrap().transaction
            ));
        }
    }
}

#[test]
#[ignore = "generated exceptional-growth policy work benchmark"]
fn reconciliation_growth_resource_probe() {
    use std::{hint::black_box, time::Instant};

    let count = std::env::var("RBTC_GROWTH_CLUSTERS")
        .unwrap_or_else(|_| "1024".to_owned())
        .parse::<usize>()
        .unwrap();
    assert!((1..=16_384).contains(&count));
    let mode = std::env::var("RBTC_GROWTH_MODE").unwrap_or_else(|_| "local".to_owned());
    assert!(mode == "local" || mode == "reference");
    let (_directory, store, mut pool) = activation_fixture(count, true);
    // Perform the real contextual validation before timing the growth phase.
    let overlay = AdmissionUtxoOverlay::new(&store);
    let mut larger = Vec::new();
    for entry in &mut pool.entries {
        let applied = apply_to_overlay(
            &overlay,
            &entry.transaction,
            context(),
            0,
            entry.script_verification,
        )
        .unwrap();
        if applied.policy_vsize > entry.policy_vsize {
            larger.push(entry.txid);
        }
        entry.policy_vsize = applied.policy_vsize;
        entry.fee_sats = applied.fee_sats;
        entry.sigop_cost = applied.sigop_cost;
        entry.script_verification = Some(applied.script_verification);
    }
    drop(overlay);
    let before = pool.len();
    POLICY_INDEX_ENTRY_VISITS.with(|visits| visits.set(0));
    let started = Instant::now();
    if mode == "reference" {
        reference_growth_pruning(&mut pool, larger);
    } else {
        pool.enforce_reconciled_growth(larger);
    }
    let micros = started.elapsed().as_micros();
    let visits = POLICY_INDEX_ENTRY_VISITS.with(std::cell::Cell::get);
    assert_eq!(before - pool.len(), count);
    println!(
        "{}",
        serde_json::json!({
            "mode": mode, "clusters": count, "entries_before": before,
            "entries_after": pool.len(), "growth_policy_micros": micros,
            "policy_index_entry_visits": visits,
            "allocator": "system (library test executable)",
            "scope": "growth pruning only; contextual validation and fixture setup excluded"
        })
    );
    black_box(pool);
}
