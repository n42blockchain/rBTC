//! Thread-local timers for the serial block-validation path.
//!
//! `core-validate` is the largest remaining stage of a snapshot catch-up and
//! runs on one thread per batch, so the cheapest honest profile is a set of
//! per-thread accumulators that the per-transaction code adds to and the
//! batch executor drains after each block. Nothing here changes behaviour;
//! an `Instant` pair per transaction is noise next to the work it brackets.

use std::{cell::Cell, time::Duration};

/// Input loading and consensus checks (`prepare_transaction_with_context`).
pub(crate) const PREPARE: usize = 0;
/// Writing the transaction's effect into the block overlay.
pub(crate) const APPLY_UTXO: usize = 1;
/// Folding the block overlay into its net change.
pub(crate) const NET: usize = 2;
/// Header, tip and BIP30 checks before the transaction loop.
pub(crate) const CHECKS: usize = 3;

const SLOTS: usize = 4;

thread_local! {
    static NANOS: Cell<[u64; SLOTS]> = const { Cell::new([0; SLOTS]) };
}

/// Adds `elapsed` to one slot of this thread's profile.
pub(crate) fn add(slot: usize, elapsed: Duration) {
    NANOS.with(|cell| {
        let mut nanos = cell.get();
        nanos[slot] =
            nanos[slot].saturating_add(u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX));
        cell.set(nanos);
    });
}

/// Returns and clears this thread's profile.
pub(crate) fn take() -> [Duration; SLOTS] {
    NANOS.with(|cell| cell.replace([0; SLOTS]).map(Duration::from_nanos))
}
