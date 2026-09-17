use super::*;
use crate::{header_store::RedbHeaderStore, headers::HeaderReadError};
use bitcoin::{Network, TxMerkleNode, block::Version, pow::Target};
use redb::ReadableTableMetadata;
use tempfile::TempDir;

fn child(parent: HeaderInfo, spacing: u32) -> Header {
    let target = Target::MAX_ATTAINABLE_REGTEST;
    let mut header = Header {
        version: Version::from_consensus(4),
        prev_blockhash: parent.hash,
        merkle_root: TxMerkleNode::all_zeros(),
        time: parent.header.time + spacing,
        bits: target.to_compact_lossy(),
        nonce: 0,
    };
    while header.validate_pow(target).is_err() {
        header.nonce += 1;
    }
    header
}

#[test]
fn disk_views_preserve_old_branch_and_match_memory_queries() {
    let dir = TempDir::new().unwrap();
    let mut memory = HeaderDag::new(Network::Regtest);
    let mut disk =
        DiskHeaderIndex::create(dir.path().join("index"), memory.deployments().clone()).unwrap();
    let genesis = memory.active_tip();
    let mut active = Vec::new();
    for _ in 0..257 {
        let header = child(memory.active_tip(), 1);
        memory.insert_contextual(header, u32::MAX).unwrap();
        active.push(header);
    }
    disk.append(&active, u32::MAX, &mut HeaderWorkBudget::default())
        .unwrap();
    let old = disk.snapshot().unwrap();
    assert_eq!(old.active_tip(), memory.active_tip());
    for height in 0..=257 {
        assert_eq!(
            old.active_header(height).unwrap(),
            memory.active_header_at(height)
        );
    }
    assert_eq!(old.block_locator().unwrap(), memory.block_locator());
    assert_eq!(
        old.median_time_past(old.tip.hash).unwrap(),
        memory.median_time_past(old.tip.hash)
    );
    let mut parent = genesis;
    let mut fork = Vec::new();
    for _ in 0..258 {
        let header = child(parent, 2);
        parent = memory.insert_contextual(header, u32::MAX).unwrap();
        fork.push(header);
    }
    disk.append(&fork, u32::MAX, &mut HeaderWorkBudget::default())
        .unwrap();
    let new = disk.snapshot().unwrap();
    assert_eq!(new.active_tip(), memory.active_tip());
    assert_eq!(new.block_locator().unwrap(), memory.block_locator());
    for height in 0..=258 {
        assert_eq!(
            new.active_header(height).unwrap(),
            memory.active_header_at(height)
        );
    }
    assert_eq!(old.active_tip().hash, active.last().unwrap().block_hash());
    assert!(old.header(&new.tip.hash).unwrap().is_none());
    assert_eq!(new.active_height(old.tip.hash).unwrap(), None);
    assert_eq!(
        new.branch_locator(old.tip.hash).unwrap(),
        memory.block_locator_from(old.tip.hash)
    );
    // The read transaction remains usable even after the writer is dropped.
    drop(disk);
    assert_eq!(
        new.active_header(258).unwrap().unwrap(),
        memory.active_tip()
    );
}

#[test]
fn failed_index_batch_preserves_context_and_read_versions() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("index");
    let config = DeploymentConfig::for_network(Network::Regtest);
    let mut disk = DiskHeaderIndex::create(&path, config.clone()).unwrap();
    assert!(DiskHeaderIndex::create(&path, config).is_err());
    let old = disk.snapshot().unwrap();
    let first = child(old.active_tip(), 1);
    let mut broken = first;
    broken.prev_blockhash = first.block_hash();
    // The second timestamp equals its parent, so the first must roll back too.
    assert!(
        disk.append(&[first, broken], u32::MAX, &mut HeaderWorkBudget::default())
            .is_err()
    );
    assert!(disk.is_empty());
    assert_eq!(disk.snapshot().unwrap().active_tip(), old.active_tip());
    assert!(
        disk.snapshot()
            .unwrap()
            .header(&first.block_hash())
            .unwrap()
            .is_none()
    );
    assert!(
        disk.append(&[first], u32::MAX, &mut HeaderWorkBudget::new(1))
            .is_err()
    );
    assert!(disk.is_empty());
    disk.append(&[first], u32::MAX, &mut HeaderWorkBudget::default())
        .unwrap();
    assert_eq!(disk.len(), 1);
    assert_eq!(
        disk.snapshot().unwrap().active_tip().hash,
        first.block_hash()
    );
    assert_eq!(old.active_tip().height, 0);
}

#[test]
fn indexed_record_corruption_is_a_local_read_error() {
    let dir = TempDir::new().unwrap();
    let disk = DiskHeaderIndex::create(
        dir.path().join("index"),
        DeploymentConfig::for_network(Network::Regtest),
    )
    .unwrap();
    let old = disk.snapshot().unwrap();
    let transaction = disk.db.begin_write().unwrap();
    let mut encoded = Record {
        info: disk.tip,
        skip: disk.tip.hash,
    }
    .encode();
    encoded[90] ^= 1;
    transaction
        .open_table(RECORDS)
        .unwrap()
        .insert(disk.tip.hash.as_byte_array().as_slice(), encoded.as_slice())
        .unwrap();
    transaction.commit().unwrap();
    assert!(matches!(
        disk.snapshot().unwrap().active_header(0),
        Err(HeaderReadError::Inconsistent(_))
    ));
    assert_eq!(old.active_header(0).unwrap().unwrap(), disk.tip);
}

#[test]
fn pinned_raw_replay_builds_disk_index_without_a_historical_dag() {
    let dir = TempDir::new().unwrap();
    let source = RedbHeaderStore::open(dir.path().join("headers")).unwrap();
    let mut memory = HeaderDag::new(Network::Regtest);
    let mut batch = Vec::new();
    for _ in 0..2003 {
        let header = child(memory.active_tip(), 1);
        memory.insert_contextual(header, u32::MAX).unwrap();
        batch.push(header);
    }
    source.append_batch(&batch).unwrap();
    let expected = memory.active_tip();
    drop(memory);
    let mut replay = source.replay_reader().unwrap();
    // Later source growth cannot change the pinned replay's input set.
    source.append(child(expected, 1)).unwrap();
    let mut disk = DiskHeaderIndex::create(
        dir.path().join("index"),
        DeploymentConfig::for_network(Network::Regtest),
    )
    .unwrap();
    assert!(replay.next_batch(&mut HeaderWorkBudget::new(1)).is_err());
    assert_eq!(replay.remaining(), 2003);
    let mut rounds = 0;
    while let Some(batch) = replay.next_batch(&mut HeaderWorkBudget::default()).unwrap() {
        assert!(batch.len() <= 2000);
        disk.append(&batch, u32::MAX, &mut HeaderWorkBudget::default())
            .unwrap();
        rounds += 1;
    }
    assert_eq!(rounds, 2);
    assert_eq!(disk.len(), 2003);
    assert_eq!(disk.snapshot().unwrap().active_tip(), expected);
}

#[test]
fn staged_index_abort_and_last_reader_cleanup_are_complete() {
    let directory = TempDir::new().unwrap();
    let mut index = DiskHeaderIndex::create_scratch(
        directory.path(),
        DeploymentConfig::for_network(Network::Regtest),
    )
    .unwrap();
    let old = index.snapshot().unwrap();
    let first = child(old.active_tip(), 1);
    let stage = index
        .stage(&[first], u32::MAX, &mut HeaderWorkBudget::default())
        .unwrap();
    assert_eq!(stage.active_tip().hash, first.block_hash());
    assert!(old.header(&first.block_hash()).unwrap().is_none());
    drop(stage);
    assert!(index.is_empty());
    assert!(
        index
            .snapshot()
            .unwrap()
            .header(&first.block_hash())
            .unwrap()
            .is_none()
    );
    index
        .append(&[first], u32::MAX, &mut HeaderWorkBudget::default())
        .unwrap();
    let current = index.snapshot().unwrap();
    let scratch = index.scratch.as_ref().unwrap().path().to_path_buf();
    drop(index);
    assert!(scratch.exists());
    drop(old);
    assert_eq!(current.active_tip().hash, first.block_hash());
    drop(current);
    assert!(!scratch.exists());
}

#[test]
fn disk_leaf_eviction_preserves_pins_and_old_versions() {
    let directory = TempDir::new().unwrap();
    let mut index = DiskHeaderIndex::create(
        directory.path().join("index"),
        DeploymentConfig::for_network(Network::Regtest),
    )
    .unwrap();
    let genesis = index.snapshot().unwrap().active_tip();
    let active = child(genesis, 1);
    let side = child(genesis, 2);
    index
        .append(&[active, side], u32::MAX, &mut HeaderWorkBudget::default())
        .unwrap();
    let old = index.snapshot().unwrap();
    let pinned = index
        .stage_eviction(
            0,
            2000,
            &[side.block_hash()],
            &mut HeaderWorkBudget::default(),
        )
        .unwrap();
    assert!(pinned.evicted().is_empty());
    drop(pinned);
    let rollback = index
        .stage_eviction(0, 2000, &[], &mut HeaderWorkBudget::default())
        .unwrap();
    assert_eq!(rollback.evicted().len(), 1);
    drop(rollback);
    assert!(
        index
            .snapshot()
            .unwrap()
            .header(&side.block_hash())
            .unwrap()
            .is_some()
    );
    index
        .stage_eviction(0, 2000, &[], &mut HeaderWorkBudget::default())
        .unwrap()
        .commit()
        .unwrap();
    assert_eq!(index.len(), 1);
    assert!(
        index
            .snapshot()
            .unwrap()
            .header(&side.block_hash())
            .unwrap()
            .is_none()
    );
    assert!(old.header(&side.block_hash()).unwrap().is_some());
    assert_eq!(
        index.snapshot().unwrap().active_tip().hash,
        active.block_hash()
    );
}

#[test]
fn shared_seed_overlays_validate_independent_forks_without_copying_history() {
    let dir = TempDir::new().unwrap();
    let mut memory = HeaderDag::new(Network::Regtest);
    let mut seed_index =
        DiskHeaderIndex::create_scratch(dir.path(), memory.deployments().clone()).unwrap();
    let genesis = memory.active_tip();
    let mut batch = Vec::new();
    for _ in 0..257 {
        let header = child(memory.active_tip(), 1);
        memory.insert_contextual(header, u32::MAX).unwrap();
        batch.push(header);
    }
    seed_index
        .append(&batch, u32::MAX, &mut HeaderWorkBudget::default())
        .unwrap();
    let seed = Arc::new(seed_index.snapshot().unwrap());
    let seed_directory = seed_index.scratch.as_ref().unwrap().path().to_path_buf();
    let mut left = DiskHeaderIndex::overlay(Arc::clone(&seed)).unwrap();
    let mut right = DiskHeaderIndex::overlay(Arc::clone(&seed)).unwrap();
    assert!(Arc::ptr_eq(
        left.base.as_ref().unwrap(),
        right.base.as_ref().unwrap()
    ));
    assert_eq!(
        left.db
            .begin_read()
            .unwrap()
            .open_table(RECORDS)
            .unwrap()
            .len()
            .unwrap(),
        1
    );
    let left_old = left.snapshot().unwrap();
    let mut parent = genesis;
    let mut fork = Vec::new();
    for _ in 0..258 {
        let header = child(parent, 2);
        parent = memory.insert_contextual(header, u32::MAX).unwrap();
        fork.push(header);
    }
    left.append(&fork, u32::MAX, &mut HeaderWorkBudget::default())
        .unwrap();
    let left_view = left.snapshot().unwrap();
    assert_eq!(left_view.active_tip(), memory.active_tip());
    for height in 0..=258 {
        assert_eq!(
            left_view.active_header(height).unwrap(),
            memory.active_header_at(height)
        );
    }
    let right_header = child(seed.active_tip(), 3);
    right
        .append(&[right_header], u32::MAX, &mut HeaderWorkBudget::default())
        .unwrap();
    let right_view = right.snapshot().unwrap();
    assert_eq!(right_view.active_tip().hash, right_header.block_hash());
    assert_eq!(
        right_view.header(&left_view.active_tip().hash).unwrap(),
        None
    );
    assert_eq!(left_view.header(&right_header.block_hash()).unwrap(), None);
    assert_eq!(left_old.active_tip(), seed.active_tip());
    assert_eq!(
        left_view.branch_locator(seed.active_tip().hash).unwrap(),
        Some(seed.block_locator().unwrap())
    );
    assert!(DiskHeaderIndex::overlay(Arc::new(left_view.clone())).is_err());
    assert!(
        left.stage_eviction(0, 1, &[], &mut HeaderWorkBudget::default())
            .is_err()
    );
    drop(seed_index);
    drop(seed);
    drop(left);
    drop(right);
    drop(left_old);
    drop(left_view);
    assert!(seed_directory.exists());
    assert_eq!(
        right_view.active_header(257).unwrap().unwrap().hash,
        batch[256].block_hash()
    );
    drop(right_view);
    assert!(!seed_directory.exists());
}
