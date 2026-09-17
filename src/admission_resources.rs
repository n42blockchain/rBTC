//! Node-shared admission work accounting and candidate-memory reservations.
//!
//! Work already charged is never rolled back when a candidate fails. Clones
//! share this ledger, so competing peers and repeated chain views cannot each
//! obtain a fresh burst. Work units are explicit accounting units, not cycles.

use std::{
    sync::{Arc, Mutex},
    time::Instant,
};
use thiserror::Error;

/// Independently observable admission stages sharing one work allowance.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(usize)]
pub enum AdmissionStage {
    /// Payload traversal, identity hashing and package shape checks.
    Payload,
    /// Candidate metadata and index construction.
    Metadata,
    /// Prevout resolution, context checks and content commitments.
    Prevout,
    /// Consensus and standardness script verification.
    Script,
    /// Dependency processing and budgeted cluster optimization.
    Graph,
    /// Owned relay and persistence views.
    Snapshot,
}

/// Explicit limits for a shared node admission ledger.
#[derive(Clone, Copy, Debug)]
pub struct AdmissionResourceLimits {
    /// Maximum work units available after an idle interval.
    pub work_burst: u64,
    /// Work units replenished per monotonic second; zero disables replenishment.
    pub work_per_second: u64,
    /// Simultaneously reserved candidate-construction bytes.
    pub candidate_bytes: usize,
}

impl Default for AdmissionResourceLimits {
    fn default() -> Self {
        Self {
            work_burst: 8_000_000_000,
            work_per_second: 1_000_000_000,
            candidate_bytes: 512 * 1024 * 1024,
        }
    }
}

/// A local scheduling/resource outcome, not transaction invalidity.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Error)]
#[error("admission resource deferred at {stage:?}: {reason}")]
pub struct AdmissionDeferred {
    /// Stage that could not reserve its allowance.
    pub stage: AdmissionStage,
    /// Stable local reason; never a peer-invalid classification.
    pub reason: &'static str,
}

/// Cumulative ledger counters, including failed and rejected attempts.
#[derive(Clone, Debug, Default)]
pub struct AdmissionResourceSnapshot {
    /// Work charged by `AdmissionStage` discriminant.
    pub charged: [u64; 6],
    /// Resource deferrals by stage.
    pub deferred: [u64; 6],
    /// Currently reserved candidate bytes.
    pub candidate_bytes: usize,
    /// Peak simultaneously reserved candidate bytes.
    pub peak_candidate_bytes: usize,
}

struct State {
    remaining: u64,
    updated: Instant,
    fractional: u128,
    counters: AdmissionResourceSnapshot,
}

struct Shared {
    memory: Option<crate::node_memory::MemoryBudget>,
    limits: AdmissionResourceLimits,
    state: Mutex<State>,
}

/// Shared token ledger; cloning preserves the same allowance and counters.
#[derive(Clone)]
pub struct AdmissionBudget(Arc<Shared>);

impl Default for AdmissionBudget {
    fn default() -> Self {
        Self::new(AdmissionResourceLimits::default())
    }
}

impl AdmissionBudget {
    /// Creates an independent node ledger with explicit limits.
    #[must_use]
    pub fn new(limits: AdmissionResourceLimits) -> Self {
        Self::with_memory(limits, None)
    }

    /// Creates a stage ledger whose candidate leases also debit the node ledger.
    #[must_use]
    pub fn with_memory(
        limits: AdmissionResourceLimits,
        memory: Option<crate::node_memory::MemoryBudget>,
    ) -> Self {
        Self(Arc::new(Shared {
            memory,
            limits,
            state: Mutex::new(State {
                remaining: limits.work_burst,
                updated: Instant::now(),
                fractional: 0,
                counters: AdmissionResourceSnapshot::default(),
            }),
        }))
    }

    /// Charges work before performing it. Failed reservations consume remaining
    /// work, and candidate rollback never refunds prior successful charges.
    pub fn charge(&self, stage: AdmissionStage, amount: u64) -> Result<(), AdmissionDeferred> {
        let mut ledger = self
            .0
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let now = Instant::now();
        let accrued = now
            .duration_since(ledger.updated)
            .as_nanos()
            .saturating_mul(u128::from(self.0.limits.work_per_second))
            .saturating_add(ledger.fractional);
        ledger.updated = now;
        ledger.fractional = accrued % 1_000_000_000;
        let refill = u64::try_from(accrued / 1_000_000_000).unwrap_or(u64::MAX);
        ledger.remaining = ledger
            .remaining
            .saturating_add(refill)
            .min(self.0.limits.work_burst);
        if ledger.remaining == self.0.limits.work_burst {
            ledger.fractional = 0;
        }
        let charged = amount.min(ledger.remaining);
        ledger.remaining -= charged;
        let index = stage as usize;
        ledger.counters.charged[index] = ledger.counters.charged[index].saturating_add(charged);
        if charged != amount {
            ledger.counters.deferred[index] = ledger.counters.deferred[index].saturating_add(1);
            return Err(AdmissionDeferred {
                stage,
                reason: "shared work allowance exhausted",
            });
        }
        Ok(())
    }

    /// Reserves candidate-construction memory until the returned guard is dropped.
    /// This accounts for the caller's explicit estimate, not allocator RSS or
    /// values that have escaped into independently bounded network queues.
    pub fn reserve_candidate(
        &self,
        bytes: usize,
    ) -> Result<CandidateReservation, AdmissionDeferred> {
        let mut state = self
            .0
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(total) = state
            .counters
            .candidate_bytes
            .checked_add(bytes)
            .filter(|total| *total <= self.0.limits.candidate_bytes)
        else {
            let index = AdmissionStage::Metadata as usize;
            state.counters.deferred[index] = state.counters.deferred[index].saturating_add(1);
            return Err(AdmissionDeferred {
                stage: AdmissionStage::Metadata,
                reason: "shared candidate memory allowance exhausted",
            });
        };
        let memory = self
            .0
            .memory
            .as_ref()
            .map(|budget| budget.reserve(bytes as u64))
            .transpose()
            .map_err(|_| {
                let index = AdmissionStage::Metadata as usize;
                state.counters.deferred[index] = state.counters.deferred[index].saturating_add(1);
                AdmissionDeferred {
                    stage: AdmissionStage::Metadata,
                    reason: "node memory reservation allowance exhausted",
                }
            })?;
        state.counters.candidate_bytes = total;
        state.counters.peak_candidate_bytes = state.counters.peak_candidate_bytes.max(total);
        Ok(CandidateReservation {
            shared: Arc::clone(&self.0),
            bytes,
            _memory: memory,
        })
    }

    /// Reads cumulative counters without resetting work or refunding a failure.
    #[must_use]
    pub fn snapshot(&self) -> AdmissionResourceSnapshot {
        self.0
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .counters
            .clone()
    }
}

/// A non-cloneable lease released on success, error, unwind or cancellation.
pub struct CandidateReservation {
    _memory: Option<crate::node_memory::MemoryLease>,
    shared: Arc<Shared>,
    bytes: usize,
}

impl Drop for CandidateReservation {
    fn drop(&mut self) {
        let mut state = self
            .shared
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.counters.candidate_bytes -= self.bytes;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clones_and_failed_attempts_share_work_without_refunds() {
        let budget = AdmissionBudget::new(AdmissionResourceLimits {
            work_burst: 100,
            work_per_second: 0,
            candidate_bytes: 64,
        });
        let peer = budget.clone();
        budget.charge(AdmissionStage::Payload, 60).unwrap();
        assert!(peer.charge(AdmissionStage::Graph, 50).is_err());
        assert!(budget.charge(AdmissionStage::Prevout, 1).is_err());
        let counters = budget.snapshot();
        assert_eq!(counters.charged[AdmissionStage::Payload as usize], 60);
        assert_eq!(counters.charged[AdmissionStage::Graph as usize], 40);
        assert_eq!(counters.deferred[AdmissionStage::Graph as usize], 1);
    }

    #[test]
    fn candidate_leases_are_shared_and_released_on_unwind() {
        let budget = AdmissionBudget::new(AdmissionResourceLimits {
            work_burst: 100,
            work_per_second: 0,
            candidate_bytes: 64,
        });
        let first = budget.reserve_candidate(40).unwrap();
        assert!(budget.clone().reserve_candidate(25).is_err());
        let _ = std::panic::catch_unwind(|| {
            let _second = budget.reserve_candidate(24).unwrap();
            panic!("cancel candidate");
        });
        assert_eq!(budget.snapshot().candidate_bytes, 40);
        drop(first);
        assert_eq!(budget.snapshot().candidate_bytes, 0);
        assert_eq!(budget.snapshot().peak_candidate_bytes, 64);
    }
}
