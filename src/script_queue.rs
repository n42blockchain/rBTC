//! Byte-bounded pending script work with batch cancellation.
//!
//! A full queue returns ownership to the producer for inline execution. A
//! worker never waits for another worker to free capacity, and discarded
//! batches can release their queued allocations without waiting to be run.

use std::{
    collections::VecDeque,
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

struct QueuedWork<T> {
    value: T,
    charge: usize,
    cancelled: Arc<AtomicBool>,
}

struct Pending<T> {
    work: VecDeque<QueuedWork<T>>,
    bytes: usize,
}

pub(crate) struct ScriptQueue<T> {
    pending: Mutex<Pending<T>>,
    available: Condvar,
    limit: usize,
}

impl<T> ScriptQueue<T> {
    pub(crate) fn new(limit: usize) -> Self {
        Self {
            pending: Mutex::new(Pending {
                work: VecDeque::new(),
                bytes: 0,
            }),
            available: Condvar::new(),
            limit,
        }
    }

    /// Returns the work when full, oversized or already cancelled.
    ///
    /// `retained_bytes` includes the value's owned allocations. Queue-entry
    /// overhead is charged here, so even zero-byte jobs have a count bound.
    pub(crate) fn try_push(
        &self,
        value: T,
        retained_bytes: usize,
        cancelled: Arc<AtomicBool>,
    ) -> Option<T> {
        let charge = retained_bytes.saturating_add(size_of::<QueuedWork<T>>());
        let mut pending = self
            .pending
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if cancelled.load(Ordering::Acquire) || charge > self.limit.saturating_sub(pending.bytes) {
            return Some(value);
        }
        pending.bytes += charge;
        pending.work.push_back(QueuedWork {
            value,
            charge,
            cancelled,
        });
        drop(pending);
        self.available.notify_one();
        None
    }

    /// Waits for live work. Cancellation is rechecked by the execution loop
    /// because a batch can be dropped after this handoff.
    pub(crate) fn pop(&self) -> T {
        let mut pending = self
            .pending
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        loop {
            while let Some(work) = pending.work.pop_front() {
                pending.bytes -= work.charge;
                if !work.cancelled.load(Ordering::Acquire) {
                    return work.value;
                }
            }
            pending = self
                .available
                .wait(pending)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
    }

    pub(crate) fn discard_cancelled(&self) {
        let mut pending = self
            .pending
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        pending
            .work
            .retain(|work| !work.cancelled.load(Ordering::Acquire));
        pending.bytes = pending.work.iter().map(|work| work.charge).sum();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn budget_returns_ownership_and_pop_releases_the_exact_charge() {
        let charge = 8 + size_of::<QueuedWork<Vec<u8>>>();
        let queue = ScriptQueue::new(2 * charge);
        let token = Arc::new(AtomicBool::new(false));
        assert!(queue.try_push(vec![1; 8], 8, token.clone()).is_none());
        assert!(queue.try_push(vec![2; 8], 8, token.clone()).is_none());
        assert_eq!(
            queue.try_push(vec![3; 8], 8, token.clone()),
            Some(vec![3; 8])
        );
        assert_eq!(queue.pending.lock().unwrap().bytes, 2 * charge);
        assert_eq!(queue.pop(), vec![1; 8]);
        assert_eq!(queue.pending.lock().unwrap().bytes, charge);
        assert!(queue.try_push(vec![3; 8], 8, token).is_none());
        assert_eq!(queue.pop(), vec![2; 8]);
        assert_eq!(queue.pop(), vec![3; 8]);
        assert_eq!(queue.pending.lock().unwrap().bytes, 0);
    }

    #[test]
    fn cancellation_releases_only_abandoned_batches() {
        let queue = ScriptQueue::new(1_024);
        let abandoned = Arc::new(AtomicBool::new(false));
        let live = Arc::new(AtomicBool::new(false));
        assert!(queue.try_push(1, 64, abandoned.clone()).is_none());
        assert!(queue.try_push(2, 64, live).is_none());
        abandoned.store(true, Ordering::Release);
        queue.discard_cancelled();
        assert_eq!(queue.pending.lock().unwrap().work.len(), 1);
        assert_eq!(queue.try_push(3, 64, abandoned), Some(3));
        assert_eq!(queue.pop(), 2);
        assert_eq!(queue.pending.lock().unwrap().bytes, 0);
    }

    #[test]
    fn cancellation_is_honored_even_before_explicit_cleanup() {
        let queue = ScriptQueue::new(1_024);
        let abandoned = Arc::new(AtomicBool::new(false));
        assert!(queue.try_push(1, 64, abandoned.clone()).is_none());
        assert!(
            queue
                .try_push(2, 64, Arc::new(AtomicBool::new(false)))
                .is_none()
        );
        abandoned.store(true, Ordering::Release);
        assert_eq!(queue.pop(), 2);
        assert_eq!(queue.pending.lock().unwrap().bytes, 0);
    }

    #[test]
    fn metadata_and_oversized_work_cannot_bypass_the_budget() {
        let queue = ScriptQueue::new(size_of::<QueuedWork<()>>());
        let live = Arc::new(AtomicBool::new(false));
        assert!(queue.try_push((), 0, live.clone()).is_none());
        assert_eq!(queue.try_push((), 0, live.clone()), Some(()));
        assert_eq!(queue.try_push((), usize::MAX, live), Some(()));
    }

    #[test]
    fn waiting_consumer_is_notified_of_new_work() {
        let queue = Arc::new(ScriptQueue::new(1_024));
        let consumer = queue.clone();
        let (sender, receiver) = std::sync::mpsc::channel();
        let worker = std::thread::spawn(move || sender.send(consumer.pop()).unwrap());
        assert!(
            queue
                .try_push(42, 0, Arc::new(AtomicBool::new(false)))
                .is_none()
        );
        assert_eq!(
            receiver
                .recv_timeout(std::time::Duration::from_secs(5))
                .unwrap(),
            42
        );
        worker.join().unwrap();
    }
}
