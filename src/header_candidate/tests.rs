use super::*;
use bitcoin::{Network, TxMerkleNode, block::Version, pow::Target};
use tempfile::TempDir;

fn mine(parent: HeaderInfo, offset: u32) -> Header {
    let mut header = Header {
        version: Version::from_consensus(4),
        prev_blockhash: parent.hash,
        merkle_root: TxMerkleNode::all_zeros(),
        time: parent.header.time + offset,
        bits: Target::MAX_ATTAINABLE_REGTEST.to_compact_lossy(),
        nonce: 0,
    };
    while header.validate_pow(header.target()).is_err() {
        header.nonce += 1;
    }
    header
}

fn open(path: &Path, source: &HeaderDag) -> DiskHeaderCandidate {
    DiskHeaderCandidate::open(
        path,
        source,
        source.active_tip().hash,
        u32::MAX,
        HeaderCandidateLimits::default(),
        &mut HeaderWorkBudget::new(u64::MAX),
    )
    .unwrap()
}

// Windows byte-range locks also exclude reads through another descriptor.
fn disk_bytes(candidate: &mut DiskHeaderCandidate) -> Vec<u8> {
    candidate.file.seek(SeekFrom::Start(0)).unwrap();
    let mut bytes = Vec::new();
    candidate.file.read_to_end(&mut bytes).unwrap();
    bytes
}

#[test]
fn long_fork_streams_reopens_and_matches_full_dag_with_bounded_context() {
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("fork");
    let source = HeaderDag::new(Network::Regtest);
    let mut reference = source.clone();
    let mut candidate = open(&path, &source);
    let mut work = HeaderWorkBudget::new(100_000_000);
    for _ in 0..25 {
        let mut batch = Vec::new();
        for _ in 0..200 {
            let header = mine(reference.active_tip(), 1);
            reference.insert_contextual(header, u32::MAX).unwrap();
            batch.push(header);
        }
        candidate.append(&batch, u32::MAX, &mut work).unwrap();
        assert_eq!(candidate.tip(), reference.active_tip());
        assert!(candidate.resident_context_entries() <= 145);
    }
    assert_eq!(candidate.len(), 5000);
    assert_eq!(source.retained_header_count(), 1);
    let locator = candidate.block_locator(&source, &mut work).unwrap();
    assert_eq!(locator.first(), Some(&reference.active_tip().hash));
    assert_eq!(locator.last(), Some(&source.active_tip().hash));
    drop(candidate);
    let mut reopened = open(&path, &source);
    assert_eq!(reopened.tip(), reference.active_tip());
    assert_eq!(reopened.len(), 5000);
    assert!(reopened.resident_context_entries() <= 145);
    let mut exported = source.clone();
    reopened
        .visit_batches(&mut work, |batch, work| {
            let _ = exported
                .stage_batch_contextual_with_budget(
                    batch,
                    u32::MAX,
                    crate::headers::HeaderBatchLimits::default(),
                    work,
                )?
                .commit();
            Ok(())
        })
        .unwrap();
    assert_eq!(exported.active_tip(), reference.active_tip());
    let before = reopened.tip();
    assert!(
        reopened
            .visit_batches(&mut work, |_, _| Err(HeaderCandidateError::Deferred(
                "destination"
            )))
            .is_err()
    );
    assert_eq!(reopened.tip(), before);
}

#[test]
fn invalid_or_over_budget_batches_never_publish_a_prefix() {
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("fork");
    let source = HeaderDag::new(Network::Regtest);
    let mut candidate = open(&path, &source);
    let first = mine(source.active_tip(), 1);
    let before = disk_bytes(&mut candidate);
    let mut work = HeaderWorkBudget::new(100_000);
    assert!(
        candidate
            .append(&[first, first], u32::MAX, &mut work)
            .is_err()
    );
    assert!(work.remaining() < 100_000);
    assert_eq!(candidate.tip(), source.active_tip());
    assert_eq!(disk_bytes(&mut candidate), before);
    assert!(
        candidate
            .append(&[first], u32::MAX, &mut HeaderWorkBudget::new(0))
            .is_err()
    );
    assert_eq!(disk_bytes(&mut candidate), before);
    candidate.limits.max_file_bytes = PREFIX_BYTES + 115;
    assert!(matches!(
        candidate.append(&[first], u32::MAX, &mut work),
        Err(HeaderCandidateError::Deferred(_))
    ));
    assert_eq!(disk_bytes(&mut candidate), before);
    candidate.limits.max_file_bytes += 1;
    candidate.append(&[first], u32::MAX, &mut work).unwrap();
    assert_eq!(candidate.file_bytes(), PREFIX_BYTES + 116);
    assert_eq!(candidate.len(), 1);
}

#[test]
fn partial_tail_recovery_waits_for_successful_budgeted_revalidation() {
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("fork");
    let source = HeaderDag::new(Network::Regtest);
    let mut candidate = open(&path, &source);
    let first = mine(source.active_tip(), 1);
    candidate
        .append(&[first], u32::MAX, &mut HeaderWorkBudget::default())
        .unwrap();
    let committed = candidate.file_bytes();
    drop(candidate);
    let mut file = OpenOptions::new().append(true).open(&path).unwrap();
    file.write_all(&1_u32.to_le_bytes()).unwrap();
    file.write_all(&[0; 13]).unwrap();
    file.sync_all().unwrap();
    drop(file);
    let original = std::fs::read(&path).unwrap();
    assert!(
        DiskHeaderCandidate::open(
            &path,
            &source,
            source.active_tip().hash,
            u32::MAX,
            HeaderCandidateLimits::default(),
            &mut HeaderWorkBudget::new(16_384)
        )
        .is_err()
    );
    assert_eq!(std::fs::read(&path).unwrap(), original);
    let reopened = open(&path, &source);
    assert_eq!(reopened.len(), 1);
    assert_eq!(reopened.tip().hash, first.block_hash());
    assert_eq!(std::fs::metadata(&path).unwrap().len(), committed);
}

#[test]
fn complete_corruption_is_preserved_and_configuration_is_bound() {
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("fork");
    let source = HeaderDag::new(Network::Regtest);
    let mut candidate = open(&path, &source);
    candidate
        .append(
            &[mine(source.active_tip(), 1)],
            u32::MAX,
            &mut HeaderWorkBudget::default(),
        )
        .unwrap();
    drop(candidate);
    let mut corrupt = std::fs::read(&path).unwrap();
    *corrupt.last_mut().unwrap() ^= 1;
    std::fs::write(&path, &corrupt).unwrap();
    assert!(matches!(
        DiskHeaderCandidate::open(
            &path,
            &source,
            source.active_tip().hash,
            u32::MAX,
            HeaderCandidateLimits::default(),
            &mut HeaderWorkBudget::default()
        ),
        Err(HeaderCandidateError::Malformed("frame checksum"))
    ));
    assert_eq!(std::fs::read(&path).unwrap(), corrupt);
    let mut config = crate::deployments::DeploymentConfig::for_network(Network::Regtest);
    config.apply_test_activation_height("bip34@1234").unwrap();
    let other = HeaderDag::with_deployments(config);
    assert!(matches!(
        DiskHeaderCandidate::open(
            &path,
            &other,
            other.active_tip().hash,
            u32::MAX,
            HeaderCandidateLimits::default(),
            &mut HeaderWorkBudget::default()
        ),
        Err(HeaderCandidateError::Malformed(
            "anchor/configuration identity"
        ))
    ));
    assert_eq!(std::fs::read(&path).unwrap(), corrupt);
}

#[test]
fn checksum_does_not_replace_contextual_consensus_validation() {
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("fork");
    let source = HeaderDag::new(Network::Regtest);
    drop(open(&path, &source));
    let invalid = mine(source.active_tip(), 0); // Valid PoW, invalid MTP.
    let mut frame = 1_u32.to_le_bytes().to_vec();
    frame.extend(serialize(&invalid));
    frame.extend_from_slice(&Sha256::digest(&frame));
    let mut file = OpenOptions::new().append(true).open(&path).unwrap();
    file.write_all(&frame).unwrap();
    drop(file);
    let before = std::fs::read(&path).unwrap();
    assert!(matches!(
        DiskHeaderCandidate::open(
            &path,
            &source,
            source.active_tip().hash,
            u32::MAX,
            HeaderCandidateLimits::default(),
            &mut HeaderWorkBudget::default()
        ),
        Err(HeaderCandidateError::Header(HeaderError::TimeTooOld { .. }))
    ));
    assert_eq!(std::fs::read(&path).unwrap(), before);
}

#[test]
fn exclusive_lock_and_write_failure_preserve_the_committed_tip() {
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("fork");
    let source = HeaderDag::new(Network::Regtest);
    let mut candidate = open(&path, &source);
    assert!(
        DiskHeaderCandidate::open(
            &path,
            &source,
            source.active_tip().hash,
            u32::MAX,
            HeaderCandidateLimits::default(),
            &mut HeaderWorkBudget::default()
        )
        .is_err()
    );
    // A read-only descriptor causes a real write/rollback failure on every OS.
    candidate.file = File::open(&path).unwrap();
    assert!(
        candidate
            .append(
                &[mine(source.active_tip(), 1)],
                u32::MAX,
                &mut HeaderWorkBudget::default()
            )
            .is_err()
    );
    assert!(candidate.poisoned);
    assert_eq!(candidate.tip(), source.active_tip());
    assert_eq!(std::fs::metadata(&path).unwrap().len(), PREFIX_BYTES);
    drop(candidate);
    assert!(open(&path, &source).is_empty());
}

#[test]
fn every_incomplete_frame_boundary_recovers_only_the_previous_commit() {
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("fork");
    let source = HeaderDag::new(Network::Regtest);
    let mut candidate = open(&path, &source);
    candidate
        .append(
            &[mine(source.active_tip(), 1)],
            u32::MAX,
            &mut HeaderWorkBudget::default(),
        )
        .unwrap();
    let previous_tip = candidate.tip();
    let previous = disk_bytes(&mut candidate);
    candidate
        .append(
            &[mine(previous_tip, 1)],
            u32::MAX,
            &mut HeaderWorkBudget::default(),
        )
        .unwrap();
    let next_tip = candidate.tip();
    let complete = disk_bytes(&mut candidate);
    drop(candidate);
    for cut in previous.len()..=complete.len() {
        std::fs::write(&path, &complete[..cut]).unwrap();
        let mut recovered = open(&path, &source);
        if cut == complete.len() {
            assert_eq!(recovered.tip(), next_tip);
        } else {
            assert_eq!(recovered.tip(), previous_tip);
            assert_eq!(disk_bytes(&mut recovered), previous);
        }
    }
}

#[test]
fn incremental_replay_retries_whole_frames_after_work_exhaustion() {
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("fork");
    let source = HeaderDag::new(Network::Regtest);
    let mut candidate = open(&path, &source);
    let mut reference = source.clone();
    for size in [1, 2, 1] {
        let mut batch = Vec::new();
        for _ in 0..size {
            let header = mine(reference.active_tip(), 1);
            reference.insert_contextual(header, u32::MAX).unwrap();
            batch.push(header);
        }
        candidate
            .append(&batch, u32::MAX, &mut HeaderWorkBudget::default())
            .unwrap();
    }
    let expected = candidate.tip();
    drop(candidate);
    let bytes = std::fs::read(&path).unwrap();
    let mut recovery = DiskHeaderCandidate::start_recovery(
        &path,
        &source,
        source.active_tip().hash,
        HeaderCandidateLimits::default(),
        &mut HeaderWorkBudget::default(),
    )
    .unwrap();
    assert_eq!(recovery.validated_headers(), 0);
    assert!(
        !recovery
            .advance(1, u32::MAX, &mut HeaderWorkBudget::default())
            .unwrap()
    );
    assert_eq!(recovery.validated_headers(), 1);
    // Enough to validate one header of the next two-header frame.
    let mut short = HeaderWorkBudget::new(730);
    assert!(recovery.advance(1, u32::MAX, &mut short).is_err());
    assert!(short.remaining() < 100);
    assert_eq!(recovery.validated_headers(), 1);
    assert_eq!(disk_bytes(&mut recovery.candidate), bytes);
    assert!(
        recovery
            .advance(2, u32::MAX, &mut HeaderWorkBudget::default())
            .unwrap()
    );
    assert_eq!(recovery.finish().unwrap().tip(), expected);
    let recovery = DiskHeaderCandidate::start_recovery(
        &path,
        &source,
        source.active_tip().hash,
        HeaderCandidateLimits::default(),
        &mut HeaderWorkBudget::default(),
    )
    .unwrap();
    assert!(matches!(
        recovery.finish(),
        Err(HeaderCandidateError::Deferred(_))
    ));
}

#[test]
fn opening_without_work_does_not_create_a_file_and_oversized_reopen_preserves_it() {
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("fork");
    let source = HeaderDag::new(Network::Regtest);
    assert!(
        DiskHeaderCandidate::open(
            &path,
            &source,
            source.active_tip().hash,
            u32::MAX,
            HeaderCandidateLimits::default(),
            &mut HeaderWorkBudget::new(0)
        )
        .is_err()
    );
    assert!(!path.exists());
    let mut candidate = open(&path, &source);
    candidate
        .append(
            &[mine(source.active_tip(), 1)],
            u32::MAX,
            &mut HeaderWorkBudget::default(),
        )
        .unwrap();
    drop(candidate);
    let before = std::fs::read(&path).unwrap();
    let limits = HeaderCandidateLimits {
        max_file_bytes: PREFIX_BYTES,
        ..HeaderCandidateLimits::default()
    };
    assert!(matches!(
        DiskHeaderCandidate::open(
            &path,
            &source,
            source.active_tip().hash,
            u32::MAX,
            limits,
            &mut HeaderWorkBudget::default()
        ),
        Err(HeaderCandidateError::Deferred(_))
    ));
    assert_eq!(std::fs::read(&path).unwrap(), before);
}
