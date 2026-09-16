//! Explicit staging metadata and validation traversal allowances.

use super::{BlockHash, Header, HeaderDag, HeaderError, HeaderInfo};

/// Limits checked before allocating a batch's rollback and accepted-info vectors.
#[derive(Clone, Copy, Debug)]
pub struct HeaderBatchLimits {
    /// Maximum retained non-genesis entries after the batch.
    pub max_headers: usize,
    /// Combined requested bytes of the two staging vectors (excluding caller input).
    pub max_staging_bytes: usize,
}

impl Default for HeaderBatchLimits {
    fn default() -> Self {
        Self {
            max_headers: super::DEFAULT_MAX_RETAINED_HEADERS,
            max_staging_bytes: 1024 * 1024,
        }
    }
}

impl HeaderBatchLimits {
    pub(super) fn check_staging_bytes(self, count: usize) -> Result<(), HeaderError> {
        let required = count.checked_mul(size_of::<HeaderInfo>() + size_of::<BlockHash>());
        if required.is_none_or(|bytes| bytes > self.max_staging_bytes) {
            return Err(HeaderError::BudgetDeferred {
                resource: "staging metadata bytes",
                required: required.map_or(u64::MAX, |bytes| bytes as u64),
                remaining: self.max_staging_bytes as u64,
            });
        }
        Ok(())
    }
}

/// Consumable validation allowance. Failed consensus validation and dropped
/// stages do not refund work. Callers can share one allowance over many batches.
/// Units conservatively account for ancestry visits and fixed per-header work;
/// they are not CPU cycles or wall-clock time. The default is per staging call,
/// not a node-wide rate limiter.
#[derive(Debug)]
pub struct HeaderWorkBudget {
    remaining: u64,
}

impl Default for HeaderWorkBudget {
    fn default() -> Self {
        Self::new(32_000_000)
    }
}

impl HeaderWorkBudget {
    /// Creates an allowance without automatic refills.
    pub const fn new(units: u64) -> Self {
        Self { remaining: units }
    }

    /// Unspent work; unused rollback reservations remain consumed.
    pub const fn remaining(&self) -> u64 {
        self.remaining
    }

    fn consume(&mut self, units: u64) -> Result<(), HeaderError> {
        if units > self.remaining {
            return Err(HeaderError::BudgetDeferred {
                resource: "validation work",
                required: units,
                remaining: self.remaining,
            });
        }
        self.remaining -= units;
        Ok(())
    }
}

impl HeaderDag {
    pub(super) fn reserve_validation_work(
        &self,
        header: &Header,
        work: &mut HeaderWorkBudget,
    ) -> Result<(), HeaderError> {
        // Both difficulty walks are bounded by the network's retarget interval.
        // Fixed work includes MTP, hashes, PoW, checkpoints, index changes and
        // rollback removal. Reserve before even duplicate/parent validation.
        work.consume(2 * self.params.difficulty_adjustment_interval() + 128)?;
        // Hash-table growth can move the complete retained index. Reserve
        // those visits too, even when the candidate later proves invalid.
        if self.headers.len() == self.headers.capacity() {
            work.consume(self.headers.len() as u64)?;
        }
        if let Some(counts) = &self.child_counts {
            if counts.len() == counts.capacity() {
                work.consume(counts.len() as u64)?;
            }
        }
        if self.active_chain.len() == self.active_chain.capacity() {
            work.consume(self.active_chain.len() as u64)?;
        }
        if !self.insertion_rebuilds_active_chain(header) {
            return Ok(());
        }
        let mut current = self.headers[&header.prev_blockhash];
        let mut new_suffix = 1_u64;
        loop {
            // Charge the preflight walk itself before looking at the next ancestor.
            work.consume(1)?;
            if self.active_chain.get(current.height as usize) == Some(&current.hash) {
                let old_suffix = self.active_chain.len() as u64 - u64::from(current.height) - 1;
                // Reserve the promotion's walk/copy plus the old suffix's
                // walk/copy on rollback. Across repeated promotions these
                // reservations bound the eventual return to the original tip.
                let new_len = u64::from(current.height) + 1 + new_suffix;
                if new_len > self.active_chain.capacity() as u64
                    && self.active_chain.len() != self.active_chain.capacity()
                {
                    work.consume(self.active_chain.len() as u64)?;
                }
                return work.consume(8 * (new_suffix + old_suffix));
            }
            new_suffix += 1;
            current = self.headers[&current.header.prev_blockhash];
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::headers::tests::mine_child;
    use bitcoin::Network;

    #[test]
    fn staging_bytes_defer_before_validation_without_mutation() {
        let mut dag = HeaderDag::new(Network::Regtest);
        let tip = dag.active_tip();
        let header = mine_child(tip.hash, tip.header.time + 1);
        let mut work = HeaderWorkBudget::default();
        let before = work.remaining();
        let limits = HeaderBatchLimits {
            max_staging_bytes: 0,
            ..HeaderBatchLimits::default()
        };
        let error = dag
            .stage_batch_contextual_with_budget(&[header], header.time, limits, &mut work)
            .err()
            .unwrap();
        assert!(matches!(
            error,
            HeaderError::BudgetDeferred {
                resource: "staging metadata bytes",
                ..
            }
        ));
        assert!(!error.is_peer_invalid());
        assert_eq!(work.remaining(), before);
        assert_eq!(dag.active_tip(), tip);
        assert_eq!(dag.retained_header_count(), 1);
    }

    #[test]
    fn staging_byte_boundary_accepts_exact_fit_and_rejects_one_byte_less() {
        let mut dag = HeaderDag::new(Network::Regtest);
        let tip = dag.active_tip();
        let first = mine_child(tip.hash, tip.header.time + 1);
        let second = mine_child(first.block_hash(), first.time + 1);
        let bytes = 2 * (size_of::<HeaderInfo>() + size_of::<BlockHash>());
        let mut limits = HeaderBatchLimits {
            max_staging_bytes: bytes - 1,
            ..HeaderBatchLimits::default()
        };
        let mut work = HeaderWorkBudget::default();
        assert!(matches!(
            dag.stage_batch_contextual_with_budget(
                &[first, second],
                second.time,
                limits,
                &mut work
            ),
            Err(HeaderError::BudgetDeferred { .. })
        ));
        assert_eq!(dag.active_tip(), tip);
        limits.max_staging_bytes = bytes;
        let _ = dag
            .stage_batch_contextual_with_budget(&[first, second], second.time, limits, &mut work)
            .unwrap()
            .commit();
        assert_eq!(dag.active_tip().hash, second.block_hash());
    }

    #[test]
    fn shared_work_survives_failed_validation_and_batch_rollback() {
        let mut dag = HeaderDag::new(Network::Regtest);
        let tip = dag.active_tip();
        let first = mine_child(tip.hash, tip.header.time + 1);
        let second = mine_child(first.block_hash(), first.time + 1);
        let per_header = 2 * dag.params.difficulty_adjustment_interval() + 128;
        let mut work = HeaderWorkBudget::new(per_header + 1);
        let error = dag
            .stage_batch_contextual_with_budget(
                &[first, second],
                second.time,
                HeaderBatchLimits::default(),
                &mut work,
            )
            .err()
            .unwrap();
        assert!(matches!(
            error,
            HeaderError::BudgetDeferred {
                resource: "validation work",
                ..
            }
        ));
        assert_eq!(work.remaining(), 0);
        assert_eq!(dag.active_tip(), tip);
        assert!(dag.get(&first.block_hash()).is_none());
        assert!(
            dag.stage_batch_contextual_with_budget(
                &[first],
                first.time,
                HeaderBatchLimits::default(),
                &mut work
            )
            .is_err()
        );
        let mut invalid = first;
        invalid.time = tip.header.time;
        let mut work = HeaderWorkBudget::new(per_header);
        assert!(matches!(
            dag.stage_batch_contextual_with_budget(
                &[invalid],
                first.time,
                HeaderBatchLimits::default(),
                &mut work
            ),
            Err(HeaderError::TimeTooOld { .. })
        ));
        assert_eq!(work.remaining(), 0);
    }

    #[test]
    fn reorg_budget_failure_preserves_both_branches_and_original_tip() {
        let mut dag = HeaderDag::new(Network::Regtest);
        let genesis = dag.active_tip();
        let mut active = genesis;
        for _ in 0..30 {
            let header = mine_child(active.hash, active.header.time + 1);
            active = dag.insert_contextual(header, header.time).unwrap();
        }
        let mut side = genesis;
        for _ in 0..30 {
            let header = mine_child(side.hash, side.header.time + 10);
            side = dag.insert_contextual(header, header.time).unwrap();
        }
        let winner = mine_child(side.hash, side.header.time + 1);
        let mut work = HeaderWorkBudget::new(2 * dag.params.difficulty_adjustment_interval() + 140);
        assert!(matches!(
            dag.stage_batch_contextual_with_budget(
                &[winner],
                winner.time,
                HeaderBatchLimits::default(),
                &mut work
            ),
            Err(HeaderError::BudgetDeferred { .. })
        ));
        assert_eq!(dag.active_tip(), active);
        assert_eq!(dag.retained_header_count(), 61);
        let mut work = HeaderWorkBudget::default();
        drop(
            dag.stage_batch_contextual_with_budget(
                &[winner],
                winner.time,
                HeaderBatchLimits::default(),
                &mut work,
            )
            .unwrap(),
        );
        assert_eq!(dag.active_tip(), active);
        assert_eq!(dag.retained_header_count(), 61);
        let _ = dag
            .stage_batch_contextual_with_budget(
                &[winner],
                winner.time,
                HeaderBatchLimits::default(),
                &mut work,
            )
            .unwrap()
            .commit();
        assert_eq!(dag.active_tip().hash, winner.block_hash());
    }
}
