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
fn recovery_cursor_is_atomic_durable_and_cleared_by_eviction() {
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("headers.redb");
    let store = RedbHeaderStore::open(&path).unwrap();
    let (mut dag, active, side) = branches();
    assert_eq!(store.recovery_tip().unwrap(), None);
    store
        .append_recovery_batch(&active, active[2].block_hash())
        .unwrap();
    // Failure after inserting every row must abort both the rows and cursor.
    assert!(
        store
            .append_recovery_batch(&side, BlockHash::all_zeros())
            .is_err()
    );
    assert_eq!(store.len().unwrap(), 3);
    assert_eq!(store.recovery_tip().unwrap(), Some(active[2].block_hash()));
    store
        .append_recovery_batch(&side, side[1].block_hash())
        .unwrap();
    drop(store);
    let store = RedbHeaderStore::open(&path).unwrap();
    assert_eq!(store.recovery_tip().unwrap(), Some(side[1].block_hash()));
    assert!(
        store
            .append_recovery_batch(&active, active[2].block_hash())
            .is_err()
    );
    assert_eq!(store.recovery_tip().unwrap(), Some(side[1].block_hash()));
    let stage = dag
        .stage_leaf_evictions(&leaf_first(&side), &[], 2)
        .unwrap();
    store.persist_eviction(&stage).unwrap();
    stage.commit();
    assert_eq!(store.recovery_tip().unwrap(), None);
    store
        .append_recovery_batch(&[], active[2].block_hash())
        .unwrap();
    assert_eq!(store.recovery_tip().unwrap(), Some(active[2].block_hash()));
    store.clear_recovery_tip().unwrap();
    assert_eq!(store.recovery_tip().unwrap(), None);
    assert_eq!(store.len().unwrap(), 3);
}

#[test]
fn ingress_capacity_defers_atomically_and_rollback_restores_capacity() {
    let (mut dag, _, side) = branches();
    let before = dag.active_tip();
    let count = dag.retained_header_count();
    let child = mine_child(side[1].block_hash(), side[1].time + 1);
    let error = match dag.stage_batch_contextual_with_limit(&[child], child.time, count - 1) {
        Ok(_) => panic!("full DAG must defer before mutation"),
        Err(error) => error,
    };
    assert!(matches!(
        error,
        crate::headers::HeaderError::ResourceDeferred { .. }
    ));
    assert!(!error.is_peer_invalid());
    assert_eq!(dag.retained_header_count(), count);
    assert_eq!(dag.active_tip(), before);
    drop(
        dag.stage_batch_contextual_with_limit(&[child], child.time, count)
            .unwrap(),
    );
    assert_eq!(dag.retained_header_count(), count);
    assert!(dag.get(&child.block_hash()).is_none());
    let _ = dag
        .stage_batch_contextual_with_limit(&[child], child.time, count)
        .unwrap()
        .commit();
    assert_eq!(dag.retained_header_count(), count + 1);
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
    store
        .append_recovery_batch(&side, side[1].block_hash())
        .unwrap();
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
    assert_eq!(store.recovery_tip().unwrap(), Some(side[1].block_hash()));
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
#[allow(clippy::too_many_lines)]
fn disk_candidate_promotion_is_atomic_bounded_and_idempotent() {
    use crate::{
        header_candidate::{DiskHeaderCandidate, HeaderCandidateLimits},
        headers::HeaderWorkBudget,
    };
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("headers.redb");
    let store = RedbHeaderStore::open(&path).unwrap();
    let (_, active, side) = branches();
    let mut dag = HeaderDag::new(Network::Regtest);
    let anchor = dag.active_tip().hash;
    let _ = dag
        .stage_batch_contextual(&active, u32::MAX)
        .unwrap()
        .commit();
    store.append_batch(&active).unwrap();
    let original = dag.active_tip();
    let mut candidate = DiskHeaderCandidate::open(
        directory.path().join("candidate"),
        &dag,
        anchor,
        u32::MAX,
        HeaderCandidateLimits::default(),
        &mut HeaderWorkBudget::default(),
    )
    .unwrap();
    candidate
        .append(&side, u32::MAX, &mut HeaderWorkBudget::default())
        .unwrap();
    assert!(
        !store
            .promote_candidate(
                &mut dag,
                &mut candidate,
                u32::MAX,
                1_000_000,
                &mut HeaderWorkBudget::default()
            )
            .unwrap()
    );
    let third = mine_child(side[1].block_hash(), side[1].time + 1);
    let fourth = mine_child(third.block_hash(), third.time + 1);
    candidate
        .append(&[third, fourth], u32::MAX, &mut HeaderWorkBudget::default())
        .unwrap();
    assert!(
        store
            .promote_candidate(
                &mut dag,
                &mut candidate,
                u32::MAX,
                1,
                &mut HeaderWorkBudget::default()
            )
            .is_err()
    );
    assert_eq!(dag.active_tip(), original);
    assert_eq!(store.len().unwrap(), 3);
    // Force the durable transaction to fail after inserting its first row.
    let transaction = store.db.begin_write().unwrap();
    transaction
        .open_table(META)
        .unwrap()
        .insert(NEXT_SEQUENCE_KEY, (u64::MAX - 1).to_le_bytes().as_slice())
        .unwrap();
    transaction.commit().unwrap();
    assert!(
        store
            .promote_candidate(
                &mut dag,
                &mut candidate,
                u32::MAX,
                1_000_000,
                &mut HeaderWorkBudget::default()
            )
            .is_err()
    );
    assert_eq!(dag.active_tip(), original);
    assert_eq!(dag.retained_header_count(), 4);
    assert_eq!(store.len().unwrap(), 3);
    assert_eq!(candidate.len(), 4);
    let transaction = store.db.begin_write().unwrap();
    transaction
        .open_table(META)
        .unwrap()
        .insert(NEXT_SEQUENCE_KEY, 3_u64.to_le_bytes().as_slice())
        .unwrap();
    transaction.commit().unwrap();
    assert!(
        store
            .promote_candidate(
                &mut dag,
                &mut candidate,
                u32::MAX,
                1_000_000,
                &mut HeaderWorkBudget::default()
            )
            .unwrap()
    );
    assert_eq!(dag.active_tip(), candidate.tip());
    assert_eq!(store.len().unwrap(), 7);
    drop(store);
    let store = RedbHeaderStore::open(path).unwrap();
    let mut restored = store.load_dag(Network::Regtest, u32::MAX).unwrap();
    assert_eq!(restored.active_tip(), candidate.tip());
    assert!(
        store
            .promote_candidate(
                &mut restored,
                &mut candidate,
                u32::MAX,
                0,
                &mut HeaderWorkBudget::new(0)
            )
            .unwrap()
    );
    assert_eq!(store.len().unwrap(), 7);
}
