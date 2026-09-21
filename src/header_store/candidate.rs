//! Atomic publication of a disk candidate that fits the reviewed materialization allowance.
use super::{HeaderStoreError, RedbHeaderStore};
use crate::{
    header_candidate::{DiskHeaderCandidate, HeaderCandidateError},
    headers::{HeaderBatchLimits, HeaderDag, HeaderInfo, HeaderWorkBudget},
};
use bitcoin::{BlockHash, block::Header};

impl RedbHeaderStore {
    /// Promotes a stronger candidate in one durable transaction. A false result
    /// means it still loses work; true also covers an already-published tip after
    /// a crash. The caller may remove the journal only after true is returned.
    /// The caller must supply the complete DAG corresponding to this store.
    /// `max_bytes` bounds input plus staging vectors, not total DAG/engine RSS.
    /// Large candidates defer before materialization and keep their disk state.
    pub fn promote_candidate(
        &self,
        dag: &mut HeaderDag,
        candidate: &mut DiskHeaderCandidate,
        adjusted_time: u32,
        max_bytes: usize,
        work: &mut HeaderWorkBudget,
    ) -> Result<bool, HeaderStoreError> {
        let tip = candidate.tip();
        if let Some(known) = dag.get(&tip.hash) {
            if known != tip {
                return Err(HeaderStoreError::Malformed("candidate tip context differs"));
            }
            return Ok(true);
        }
        if tip.chainwork <= dag.active_tip().chainwork {
            return Ok(false);
        }
        let count = usize::try_from(candidate.len())
            .map_err(|_| HeaderCandidateError::Deferred("promotion count"))?;
        let bytes_per_entry =
            size_of::<Header>() + size_of::<HeaderInfo>() + size_of::<BlockHash>();
        if count
            .checked_mul(bytes_per_entry)
            .is_none_or(|bytes| bytes > max_bytes)
        {
            return Err(HeaderCandidateError::Deferred("promotion materialization bytes").into());
        }
        work.consume(count as u64)?;
        let mut batch = Vec::with_capacity(count);
        candidate.visit_batches(work, |headers, work| {
            work.consume(2 * headers.len() as u64)?;
            for header in headers {
                if dag.get(&header.block_hash()).is_none() {
                    batch.push(*header);
                }
            }
            Ok(())
        })?;
        let limits = HeaderBatchLimits {
            max_staging_bytes: max_bytes - count * size_of::<Header>(),
            ..HeaderBatchLimits::default()
        };
        let stage = dag.stage_batch_contextual_with_budget(&batch, adjusted_time, limits, work)?;
        if stage.active_tip() != tip {
            return Err(HeaderStoreError::Malformed(
                "candidate promotion tip mismatch",
            ));
        }
        self.append_recovery_batch(&batch, tip.hash)?;
        let _ = stage.commit();
        Ok(true)
    }
}
