use super::*;
use crate::{header_store::RedbHeaderStore, headers::HeaderReadError};
use bitcoin::{Network, TxMerkleNode, block::Version, pow::Target};
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
