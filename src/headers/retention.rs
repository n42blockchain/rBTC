//! Explicit retention transactions and the leaf-first side-chain candidate
//! selector used by node ingress and idle maintenance to bound retained
//! competing headers.

use std::{cmp::Reverse, collections::BinaryHeap};

use bitcoin::{hashes::Hash, pow::Work};

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

/// Orders eviction candidates by ascending chainwork, then by hash so ties
/// resolve identically wherever this selector runs.
#[derive(Clone, Copy, PartialEq, Eq)]
struct EvictionCandidate {
    chainwork: Work,
    hash: BlockHash,
}

impl PartialOrd for EvictionCandidate {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for EvictionCandidate {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.chainwork
            .cmp(&other.chainwork)
            .then_with(|| self.hash.to_byte_array().cmp(&other.hash.to_byte_array()))
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
    /// This is a maintenance primitive; it does not choose which headers to
    /// remove itself (see [`Self::select_side_chain_eviction_candidates`] for
    /// node ingress's bounded policy). The first call builds an
    /// `O(retained headers)` child index; subsequent insertions, rollbacks and
    /// removals maintain it incrementally. Active history, retained
    /// descendants and caller-pinned tips cannot be removed. Ordinary
    /// `getheaders` sync must reacquire evicted ancestry before a stronger
    /// fork can win again; callers must not classify any error here as
    /// consensus invalid.
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
        if !hashes.is_empty() {
            self.ensure_child_counts();
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

    /// Selects side-chain leaves to keep retained side-chain headers within
    /// `max_side_chain_headers`, for a subsequent [`Self::stage_leaf_evictions`].
    ///
    /// A candidate must be off the active chain (a header with more chainwork
    /// than the active tip would already be the active tip, so no side-chain
    /// header ever exceeds it), not in `pinned_tips`, and currently childless.
    /// Candidates are ordered lowest chainwork first; ties break on hash so
    /// independent runs of this selector agree without sharing arrival order.
    /// Evicting a leaf can expose its parent as a new leaf, which this walk
    /// then considers in the same pass, so a run of low-work side-chain
    /// headers is fully reclaimed leaf-first without a second call. The
    /// active chain and its ancestors are never candidates: every entry on
    /// it fails the off-active-chain check at every step of the walk.
    ///
    /// Returns an empty vector when retained side-chain headers are already
    /// within the cap, or once no further eligible header remains.
    pub fn select_side_chain_eviction_candidates(
        &mut self,
        pinned_tips: &[BlockHash],
        max_side_chain_headers: usize,
    ) -> Vec<BlockHash> {
        let side_chain_count = self.headers.len().saturating_sub(self.active_chain.len());
        if side_chain_count <= max_side_chain_headers {
            return Vec::new();
        }
        self.ensure_child_counts();
        let mut remaining_children = self.child_counts.clone().unwrap_or_default();
        let active_work = self.active_tip().chainwork;
        let mut heap: BinaryHeap<Reverse<EvictionCandidate>> = self
            .headers
            .values()
            .filter(|info| {
                info.chainwork <= active_work
                    && !pinned_tips.contains(&info.hash)
                    && !remaining_children.contains_key(&info.hash)
                    && self.active_height_of(info.hash).is_none()
            })
            .map(|info| {
                Reverse(EvictionCandidate {
                    chainwork: info.chainwork,
                    hash: info.hash,
                })
            })
            .collect();
        let mut to_evict = side_chain_count - max_side_chain_headers;
        let mut evicted = Vec::with_capacity(to_evict);
        while to_evict > 0 {
            let Some(Reverse(candidate)) = heap.pop() else {
                break;
            };
            let Some(parent) = self
                .headers
                .get(&candidate.hash)
                .map(|info| info.header.prev_blockhash)
            else {
                continue;
            };
            evicted.push(candidate.hash);
            to_evict -= 1;
            let Some(count) = remaining_children.get_mut(&parent) else {
                continue;
            };
            *count -= 1;
            if *count > 0 {
                continue;
            }
            remaining_children.remove(&parent);
            if pinned_tips.contains(&parent) || self.active_height_of(parent).is_some() {
                continue;
            }
            if let Some(chainwork) = self.headers.get(&parent).map(|info| info.chainwork) {
                heap.push(Reverse(EvictionCandidate {
                    chainwork,
                    hash: parent,
                }));
            }
        }
        evicted
    }

    fn ensure_child_counts(&mut self) {
        if self.child_counts.is_none() {
            let mut counts = HashMap::new();
            for info in self.headers.values().filter(|info| info.height > 0) {
                *counts.entry(info.header.prev_blockhash).or_insert(0) += 1;
            }
            self.child_counts = Some(counts);
        }
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
