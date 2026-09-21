use super::*;
use crate::headers::{HeaderInfo, HeaderRetentionError};

fn branches() -> (HeaderDag, Vec<Header>, Vec<Header>) {
    let mut dag = HeaderDag::new(Network::Regtest);
    let genesis = dag.active_tip();
    let mut active = Vec::new();
    let mut parent = genesis;
    for _ in 0..3 {
        let header = mine_child(parent.hash, parent.header.time + 1);
        parent = dag.insert_contextual(header, header.time).unwrap();
        active.push(header);
    }
    let mut side = Vec::new();
    parent = genesis;
    for _ in 0..2 {
        let header = mine_child(parent.hash, parent.header.time + 10);
        parent = dag.insert_contextual(header, header.time).unwrap();
        side.push(header);
    }
    (dag, active, side)
}

fn leaf_first(side: &[Header]) -> Vec<BlockHash> {
    side.iter().rev().map(Header::block_hash).collect()
}

#[test]
fn retention_protects_context_and_rolls_back_partial_plans() {
    let (mut dag, active, side) = branches();
    let tip = dag.active_tip();
    let leaves = leaf_first(&side);
    assert!(matches!(
        dag.stage_leaf_evictions(&leaves, &[], 1),
        Err(HeaderRetentionError::ResourceDeferred { limit: 1 })
    ));
    assert_eq!(dag.retained_header_count(), 6);
    assert!(matches!(
        dag.stage_leaf_evictions(&[side[0].block_hash()], &[], 2),
        Err(HeaderRetentionError::HasChildren(_))
    ));
    // Remove the child privately, then encounter a pinned execution tip.
    assert!(matches!(
        dag.stage_leaf_evictions(&leaves, &[side[0].block_hash()], 2),
        Err(HeaderRetentionError::Protected(_))
    ));
    assert_eq!(dag.retained_header_count(), 6);
    assert!(matches!(
        dag.stage_leaf_evictions(&[active[2].block_hash()], &[], 2),
        Err(HeaderRetentionError::Protected(_))
    ));
    drop(dag.stage_leaf_evictions(&leaves, &[], 2).unwrap());
    assert_eq!(dag.active_tip(), tip);
    assert_eq!(dag.retained_header_count(), 6);
    assert!(matches!(
        dag.stage_leaf_evictions(&[side[0].block_hash()], &[], 2),
        Err(HeaderRetentionError::HasChildren(_))
    ));
    // A dropped header insertion must update the already-built child index too.
    let child = mine_child(side[1].block_hash(), side[1].time + 1);
    drop(dag.stage_batch_contextual(&[child], child.time).unwrap());
    dag.stage_leaf_evictions(&leaves, &[], 2).unwrap().commit();
    assert_eq!(dag.retained_header_count(), 4);
    assert_eq!(dag.active_tip(), tip);
}

#[test]
fn durable_eviction_reopens_and_explicitly_refetched_fork_can_win() {
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("headers.redb");
    let store = RedbHeaderStore::open(&path).unwrap();
    let (mut dag, active, mut side) = branches();
    store.append_batch(&active).unwrap();
    store.append_batch(&side).unwrap();
    let expected_tip = dag.active_tip();
    let mut reference = dag.clone();
    let stage = dag
        .stage_leaf_evictions(&leaf_first(&side), &[], 2)
        .unwrap();
    store.persist_eviction(&stage).unwrap();
    stage.commit();
    assert_eq!(store.len().unwrap(), 3);
    drop(store);
    let store = RedbHeaderStore::open(&path).unwrap();
    let mut reopened = store
        .load_dag_with_limit(
            DeploymentConfig::for_network(Network::Regtest),
            side[1].time,
            3,
        )
        .unwrap();
    assert_eq!(reopened.active_tip(), expected_tip);
    assert!(reopened.get(&side[0].block_hash()).is_none());
    assert!(reopened.get(&side[1].block_hash()).is_none());
    // This is explicit reacquisition from a retained ancestor. It tests the
    // storage primitive, not the unfinished automatic peer recovery scheduler.
    let mut parent = *side.last().unwrap();
    for _ in 0..2 {
        let next = mine_child(parent.block_hash(), parent.time + 1);
        reference.insert_contextual(next, next.time).unwrap();
        side.push(next);
        parent = next;
    }
    let stage = reopened.stage_batch_contextual(&side, parent.time).unwrap();
    store.append_batch(&side).unwrap();
    let _ = stage.commit();
    assert_eq!(reopened.active_tip(), reference.active_tip());
    assert_eq!(
        reopened.median_time_past(parent.block_hash()),
        reference.median_time_past(parent.block_hash())
    );
    assert_eq!(
        store.len().unwrap(),
        7,
        "sequence gaps are not retained rows"
    );
    drop(store);
    let restored = RedbHeaderStore::open(path)
        .unwrap()
        .load_dag(Network::Regtest, parent.time)
        .unwrap();
    assert_eq!(restored.active_tip(), reference.active_tip());
}

#[test]
fn bounded_reopen_defers_before_historical_validation() {
    let directory = TempDir::new().unwrap();
    let store = RedbHeaderStore::open(directory.path().join("headers.redb")).unwrap();
    let (_, active, side) = branches();
    store.append_batch(&active).unwrap();
    store.append_batch(&side).unwrap();
    REPLAYED_HEADERS.with(|count| count.set(0));
    assert!(matches!(
        store.load_dag_with_limit(
            DeploymentConfig::for_network(Network::Regtest),
            side[1].time,
            4
        ),
        Err(HeaderStoreError::ResourceDeferred {
            retained: 5,
            limit: 4
        })
    ));
    REPLAYED_HEADERS.with(|count| assert_eq!(count.get(), 0));
    assert_eq!(store.len().unwrap(), 5);
}

#[test]
fn failed_durable_eviction_aborts_every_row_and_restores_the_dag() {
    let directory = TempDir::new().unwrap();
    let store = RedbHeaderStore::open(directory.path().join("headers.redb")).unwrap();
    let (mut dag, active, side) = branches();
    store.append_batch(&active).unwrap();
    store.append_batch(&side).unwrap();
    // Inject a stale reverse index for the second removal. The first removal
    // has already happened inside the uncommitted redb transaction when it fails.
    let transaction = store.db.begin_write().unwrap();
    transaction
        .open_table(HASH_SEQUENCE)
        .unwrap()
        .insert(side[0].block_hash().to_byte_array().as_slice(), 999)
        .unwrap();
    transaction.commit().unwrap();
    let before = dag.active_tip();
    let stage = dag
        .stage_leaf_evictions(&leaf_first(&side), &[], 2)
        .unwrap();
    assert!(store.persist_eviction(&stage).is_err());
    drop(stage);
    assert_eq!(dag.active_tip(), before);
    assert_eq!(dag.retained_header_count(), 6);
    assert_eq!(store.len().unwrap(), 5);
    let transaction = store.db.begin_read().unwrap();
    let headers = transaction.open_table(HEADERS).unwrap();
    for header in &side {
        assert!(
            headers
                .get(header.block_hash().to_byte_array().as_slice())
                .unwrap()
                .is_some()
        );
    }
}

#[test]
fn legacy_reverse_index_is_migrated_atomically_on_first_eviction() {
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("headers.redb");
    let store = RedbHeaderStore::open(&path).unwrap();
    let (mut dag, active, side) = branches();
    store.append_batch(&active).unwrap();
    store.append_batch(&side).unwrap();
    let transaction = store.db.begin_write().unwrap();
    transaction.delete_table(HASH_SEQUENCE).unwrap();
    transaction.commit().unwrap();
    drop(store);
    let store = RedbHeaderStore::open(path).unwrap();
    let stage = dag
        .stage_leaf_evictions(&leaf_first(&side), &[], 2)
        .unwrap();
    let removed: Vec<HeaderInfo> = stage.evicted().to_vec();
    store.persist_eviction(&stage).unwrap();
    stage.commit();
    assert_eq!(removed.len(), 2);
    let transaction = store.db.begin_read().unwrap();
    assert_eq!(
        transaction
            .open_table(HASH_SEQUENCE)
            .unwrap()
            .len()
            .unwrap(),
        3
    );
    assert_eq!(
        transaction
            .open_table(INSERTION_ORDER)
            .unwrap()
            .len()
            .unwrap(),
        3
    );
}

#[test]
fn eviction_candidates_are_leaf_first_lowest_chainwork_and_respect_caps_and_pins() {
    let (mut dag, active, side) = branches();
    // Already within the cap: nothing is selected.
    assert!(dag.select_side_chain_eviction_candidates(&[], 2).is_empty());
    assert!(dag.select_side_chain_eviction_candidates(&[], 5).is_empty());

    // Only the present leaf (side[1], the child of side[0]) is eligible while
    // side[0] still has a retained child.
    assert_eq!(
        dag.select_side_chain_eviction_candidates(&[], 1),
        vec![side[1].block_hash()]
    );

    // Pinning the only current leaf must not expose its non-leaf parent.
    assert!(
        dag.select_side_chain_eviction_candidates(&[side[1].block_hash()], 0)
            .is_empty()
    );

    // Leaf-first walk: evicting side[1] exposes side[0] in the same pass.
    let selected = dag.select_side_chain_eviction_candidates(&[], 0);
    assert_eq!(selected, vec![side[1].block_hash(), side[0].block_hash()]);
    for hash in selected {
        assert!(active.iter().all(|header| header.block_hash() != hash));
        assert_ne!(hash, dag.active_tip().hash);
    }

    // Pinning a side-chain hash never active-height-protected is still honored.
    let selected = dag.select_side_chain_eviction_candidates(&[side[0].block_hash()], 0);
    assert_eq!(selected, vec![side[1].block_hash()]);
}

/// Replicates `node.rs`'s `evict_excess_side_chain_headers` four steps
/// (select, stage, persist, commit) so this test exercises the exact
/// sequence node ingress runs after every accepted header batch, without
/// depending on that private helper across module boundaries.
fn run_eviction_step(dag: &mut HeaderDag, store: &RedbHeaderStore, cap: usize) -> usize {
    let candidates = dag.select_side_chain_eviction_candidates(&[], cap);
    if candidates.is_empty() {
        return 0;
    }
    let count = candidates.len();
    let stage = dag
        .stage_leaf_evictions(&candidates, &[], count)
        .expect("selected eviction candidates must stage cleanly");
    store
        .persist_eviction(&stage)
        .expect("staged eviction must persist durably");
    stage.commit();
    count
}

/// Feeds `total_headers` hostile, low-work competing side-chain headers at
/// a fixed retention cap and asserts the cap holds throughout, that the
/// active tip never moves, and that the durable file stops growing once the
/// steady state is reached. Finishes by reviving one evicted fork past the
/// active chain's chainwork and reopening the durable store under a bound.
#[allow(clippy::too_many_lines)]
fn run_hostile_side_chain_feeder(total_headers: usize) {
    const CAP: usize = 1_024;
    const ACTIVE_LEN: u32 = 10;
    const BATCH_SIZE: usize = 500;
    const LONG_FORK_PERIOD: usize = 37;
    const LONG_FORK_LEN: usize = 3;

    let start = std::time::Instant::now();
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("headers.redb");
    let store = RedbHeaderStore::open(&path).unwrap();

    let mut dag = HeaderDag::new(Network::Regtest);
    let genesis = dag.active_tip();
    let mut active = Vec::with_capacity(ACTIVE_LEN as usize);
    let mut parent = genesis;
    for _ in 0..ACTIVE_LEN {
        let header = mine_child(parent.hash, parent.header.time + 1);
        parent = dag.insert_contextual(header, header.time).unwrap();
        active.push(header);
    }
    store.append_batch(&active).unwrap();
    let active_tip = dag.active_tip();

    // Hostile forks branch off genesis and every active header short of the
    // tip and short of a height that, combined with a long fork's extra
    // depth, could ever tie or beat the active tip's chainwork.
    let mut ancestors = vec![genesis.header];
    ancestors.extend(active.iter().take(5).copied());

    // One fork branching off a mid-height active ancestor, fed before the
    // hostile flood and expected to be evicted well before the run ends,
    // since every later tier it competes against carries more chainwork.
    let mut time_cursor = active_tip.header.time + 10;
    let revival_root = mine_child(ancestors[2].block_hash(), time_cursor);
    let staged = dag
        .stage_batch_contextual(std::slice::from_ref(&revival_root), time_cursor + 1)
        .unwrap();
    store
        .append_batch(std::slice::from_ref(&revival_root))
        .unwrap();
    let _ = staged.commit();

    let mut fed = 1usize;
    let mut evicted_total = 0usize;
    let mut peak_side = 0usize;
    let mut size_samples: Vec<u64> = Vec::new();
    let mut total_index = 0usize;

    while fed < total_headers {
        let batch_target = BATCH_SIZE.min(total_headers - fed);
        let mut batch = Vec::with_capacity(batch_target + LONG_FORK_LEN);
        while batch.len() < batch_target {
            let ancestor = ancestors[total_index % ancestors.len()];
            let long_fork = total_index % LONG_FORK_PERIOD == 0;
            let length = if long_fork { LONG_FORK_LEN } else { 1 };
            let mut parent = ancestor;
            for _ in 0..length {
                time_cursor += 1;
                let header = mine_child(parent.block_hash(), time_cursor);
                batch.push(header);
                parent = header;
            }
            total_index += 1;
        }
        let staged = dag.stage_batch_contextual(&batch, time_cursor + 1).unwrap();
        store.append_batch(&batch).unwrap();
        let _ = staged.commit();
        fed += batch.len();

        evicted_total += run_eviction_step(&mut dag, &store, CAP);

        assert_eq!(
            dag.active_tip(),
            active_tip,
            "a hostile fork moved the active tip"
        );
        let active_chain_len = usize::try_from(dag.active_tip().height).unwrap() + 1;
        let side_count = dag.retained_header_count() - active_chain_len;
        assert!(
            side_count <= CAP,
            "retained {side_count} side-chain headers above the cap of {CAP}"
        );
        peak_side = peak_side.max(side_count);
        size_samples.push(std::fs::metadata(&path).unwrap().len());
    }

    // The revival root lost to every later, higher-chainwork tier.
    assert!(
        dag.get(&revival_root.block_hash()).is_none(),
        "the revival root should have been evicted under sustained pressure"
    );

    // Extend the evicted fork past the active chain's chainwork from its
    // still-retained common ancestor (an active-chain header can never be
    // evicted), exactly as an ordinary `getheaders` reacquisition would.
    // Timestamps keep following the same shared counter used throughout the
    // flood, so the extension's times stay newer than every other header
    // still retained (needed for a single-`adjusted_time` replay below).
    let mut extension = vec![revival_root];
    let mut parent = revival_root;
    for _ in 0..(usize::try_from(active_tip.height).unwrap() + LONG_FORK_LEN) {
        time_cursor += 1;
        parent = mine_child(parent.block_hash(), time_cursor);
        extension.push(parent);
    }
    let staged = dag
        .stage_batch_contextual(&extension, time_cursor + 1)
        .unwrap();
    store.append_batch(&extension).unwrap();
    let _ = staged.commit();
    assert_eq!(
        dag.active_tip().hash,
        extension.last().unwrap().block_hash()
    );
    assert_ne!(dag.active_tip().hash, active_tip.hash);

    evicted_total += run_eviction_step(&mut dag, &store, CAP);
    let active_chain_len = usize::try_from(dag.active_tip().height).unwrap() + 1;
    let side_count = dag.retained_header_count() - active_chain_len;
    assert!(
        side_count <= CAP,
        "retained {side_count} side-chain headers above the cap of {CAP} after revival"
    );

    let final_size = std::fs::metadata(&path).unwrap().len();
    let tenth = size_samples.len() / 10;
    let size_after_first_tenth = size_samples[tenth.min(size_samples.len() - 1)];
    assert!(
        final_size <= size_after_first_tenth.saturating_mul(2),
        "durable file grew from {size_after_first_tenth} to {final_size} bytes; \
         the retention cap should keep it near-flat after the initial ramp-up"
    );

    drop(store);
    let reopened_store = RedbHeaderStore::open(&path).unwrap();
    let margin = 64;
    let max_headers = CAP + active_chain_len + margin;
    let reopened = reopened_store
        .load_dag_with_limit(
            DeploymentConfig::for_network(Network::Regtest),
            time_cursor + 1,
            max_headers,
        )
        .unwrap();
    assert_eq!(reopened.active_tip().hash, dag.active_tip().hash);
    let reopened_active_len = usize::try_from(reopened.active_tip().height).unwrap() + 1;
    let reopened_side = reopened.retained_header_count() - reopened_active_len;
    assert!(
        reopened_side <= CAP,
        "reopened store retained {reopened_side} side-chain headers above the cap of {CAP}"
    );

    eprintln!(
        "hostile side-chain feeder: total_headers={total_headers} fed={fed} \
         evicted={evicted_total} peak_side={peak_side} final_redb_bytes={final_size} \
         size_after_first_tenth={size_after_first_tenth} elapsed={:?}",
        start.elapsed()
    );
}

#[test]
fn sustained_hostile_side_chain_feeder_stays_within_the_cap() {
    run_hostile_side_chain_feeder(10_000);
}

#[test]
#[ignore = "sustained resource stress: run explicitly with \
            `cargo test --release -- --ignored sustained_hostile_side_chain_feeder_at_full_scale`"]
fn sustained_hostile_side_chain_feeder_at_full_scale_stays_within_the_cap() {
    run_hostile_side_chain_feeder(100_000);
}

#[test]
fn eviction_candidates_break_chainwork_ties_by_ascending_hash() {
    let mut dag = HeaderDag::new(Network::Regtest);
    let genesis = dag.active_tip();
    // Two active-chain headers keep every sibling below strictly below the
    // active tip's chainwork, so none of them can ever be promoted.
    let mut parent = genesis;
    for _ in 0..2 {
        let header = mine_child(parent.hash, parent.header.time + 1);
        parent = dag.insert_contextual(header, header.time).unwrap();
    }
    // `mine_child` always mines at the fixed regtest minimum difficulty, so
    // three siblings off genesis carry identical chainwork; only their hash
    // breaks the tie.
    let mut siblings: Vec<Header> = (0..3)
        .map(|offset| mine_child(genesis.hash, genesis.header.time + 100 + offset))
        .collect();
    for header in &siblings {
        dag.insert_contextual(*header, header.time).unwrap();
    }
    siblings.sort_by_key(|header| header.block_hash().to_byte_array());
    let expected: Vec<BlockHash> = siblings.iter().map(Header::block_hash).collect();
    assert_eq!(dag.select_side_chain_eviction_candidates(&[], 0), expected);
}
