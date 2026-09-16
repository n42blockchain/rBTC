//! Retention transactions and bounded selection for caught-up idle maintenance.
//! Node ingress itself does not impose a hard retained-header resource limit.

use super::{BlockHash, HashMap, HeaderDag, HeaderInfo};
use thiserror::Error;

/// A local retention outcome, separate from consensus header validity.
#[derive(Debug, Eq, PartialEq, Error)]
pub enum HeaderRetentionError {
    /// The requested maintenance batch exceeds its caller-supplied allowance.
    #[error("header retention resource deferred: batch exceeds {limit} entries")]
    ResourceDeferred {
        /// Maximum simultaneously staged removals.
        limit: usize,
    },
    /// Active-chain or explicitly pinned execution/recovery context is protected.
    #[error("header retention protects {0}")]
    Protected(BlockHash),
    /// A retained child still needs this header's consensus context.
    #[error("header retention requires leaf-first removal of {0}")]
    HasChildren(BlockHash),
    /// The maintenance plan is stale or refers to an unknown header.
    #[error("header retention does not know {0}")]
    Unknown(BlockHash),
}

/// Leaf removals staged in memory until their durable transaction succeeds.
///
/// Dropping the guard restores removed ancestors before their children. The
/// active tip cannot change, and neither a failed plan nor failed persistence
/// can publish a partial eviction. Explicit pins must include execution and
/// in-flight recovery tips; protecting their retained children protects ancestry.
pub struct StagedHeaderEviction<'a> {
    dag: &'a mut HeaderDag,
    removed: Vec<HeaderInfo>,
    committed: bool,
}

impl StagedHeaderEviction<'_> {
    /// Headers removed in leaf-first order, for the atomic durable operation.
    #[must_use]
    pub fn evicted(&self) -> &[HeaderInfo] {
        &self.removed
    }

    /// Publishes the removals after persistence succeeds.
    pub fn commit(mut self) {
        self.committed = true;
    }
}

impl Drop for StagedHeaderEviction<'_> {
    fn drop(&mut self) {
        if !self.committed {
            for info in self.removed.drain(..).rev() {
                self.dag.restore_retained_header(info);
            }
        }
    }
}

impl HeaderDag {
    /// Selects a bounded leaf-first batch toward a side-header retention target.
    /// Must only be used when execution matches the active tip and no recovery
    /// is in flight. Active ancestors are never selected. Selection scans the
    /// retained index once and uses at most max_removals temporary entries.
    #[must_use]
    pub fn side_chain_eviction_plan(&self, target: usize, max_removals: usize) -> Vec<BlockHash> {
        use std::{cmp::Reverse, collections::BinaryHeap};
        let active = self.active_chain.len();
        let remove = self
            .headers
            .len()
            .saturating_sub(active)
            .saturating_sub(target)
            .min(max_removals);
        if remove == 0 {
            return Vec::new();
        }
        let mut selected = BinaryHeap::with_capacity(remove);
        for info in self.headers.values() {
            if self.active_height_of(info.hash).is_some() {
                continue;
            }
            let candidate = Reverse((info.height, info.hash));
            if selected.len() < remove {
                selected.push(candidate);
            } else if selected.peek().is_some_and(|lowest| candidate < *lowest) {
                selected.pop();
                selected.push(candidate);
            }
        }
        // Children always have greater heights. Retaining the highest entries
        // guarantees no selected parent has a child outside this batch.
        let mut ordered = selected
            .into_iter()
            .map(|Reverse(item)| item)
            .collect::<Vec<_>>();
        ordered.sort_unstable_by(|a, b| b.cmp(a));
        ordered.into_iter().map(|(_, hash)| hash).collect()
    }

    /// Stages explicitly selected side-chain leaves within a removal allowance.
    ///
    /// This is a maintenance primitive, not an automatic fork-selection policy.
    /// The first call builds an `O(retained headers)` child index; subsequent
    /// insertions, rollbacks and removals maintain it incrementally. Active
    /// history, retained descendants and caller-pinned tips cannot be removed.
    /// A recovery scheduler must reacquire evicted ancestry before retrying a
    /// stronger fork; callers must not classify any error as consensus invalid.
    pub fn stage_leaf_evictions(
        &mut self,
        hashes: &[BlockHash],
        pinned_tips: &[BlockHash],
        max_entries: usize,
    ) -> Result<StagedHeaderEviction<'_>, HeaderRetentionError> {
        if hashes.len() > max_entries {
            return Err(HeaderRetentionError::ResourceDeferred { limit: max_entries });
        }
        for hash in pinned_tips {
            if self.get(hash).is_none() {
                return Err(HeaderRetentionError::Unknown(*hash));
            }
        }
        if self.child_counts.is_none() && !hashes.is_empty() {
            let mut counts = HashMap::new();
            for info in self.headers.values().filter(|info| info.height > 0) {
                *counts.entry(info.header.prev_blockhash).or_insert(0) += 1;
            }
            self.child_counts = Some(counts);
        }
        let mut stage = StagedHeaderEviction {
            dag: self,
            removed: Vec::with_capacity(hashes.len()),
            committed: false,
        };
        for hash in hashes {
            let info = stage
                .dag
                .get(hash)
                .ok_or(HeaderRetentionError::Unknown(*hash))?;
            if stage.dag.active_height_of(*hash).is_some() || pinned_tips.contains(hash) {
                return Err(HeaderRetentionError::Protected(*hash));
            }
            if stage
                .dag
                .child_counts
                .as_ref()
                .is_some_and(|counts| counts.contains_key(hash))
            {
                return Err(HeaderRetentionError::HasChildren(*hash));
            }
            stage.dag.remove_retained_header(*hash);
            stage.removed.push(info);
        }
        Ok(stage)
    }

    pub(super) fn remove_retained_header(&mut self, hash: BlockHash) {
        if let Some(info) = self.headers.remove(&hash) {
            if let Some(counts) = self.child_counts.as_mut() {
                if let Some(count) = counts.get_mut(&info.header.prev_blockhash) {
                    *count -= 1;
                    if *count == 0 {
                        counts.remove(&info.header.prev_blockhash);
                    }
                }
                debug_assert!(!counts.contains_key(&hash));
            }
        }
    }

    pub(super) fn restore_retained_header(&mut self, info: HeaderInfo) {
        self.headers.insert(info.hash, info);
        if let Some(counts) = self.child_counts.as_mut() {
            *counts.entry(info.header.prev_blockhash).or_insert(0) += 1;
        }
    }
}
