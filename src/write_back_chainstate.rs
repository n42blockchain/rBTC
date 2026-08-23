//! Cross-batch write-back layer in front of an [`ExecutionChainStore`].
//!
//! Measured over mainnet 935,001–963,350, 33.5% of inputs spend an output
//! created in the same block and 80.7% spend one at most 256 blocks old
//! (`docs/REAL_BLOCK_OVERLAY_REPLAY_2026-08-21.md`). A store that commits
//! every 256-block batch durably still writes most of those short-lived coins
//! and deletes them a batch or two later. This layer keeps the net effect of
//! the last few batches in memory, serves reads from it first, and hands the
//! inner store one larger batch whose intra-batch fold cancels the pairs that
//! never needed to exist on disk.
//!
//! The engine commit of a full buffer takes tens of seconds, so it runs on a
//! background thread: the buffer being committed stays readable behind the
//! fresh buffer that keeps accepting batches, reads consult pending → in
//! flight → engine, and the next flush waits for the previous commit before
//! starting its own. Commits therefore stay sequential and the engine tip
//! still only ever lags the reported tip.
//!
//! Durability is deliberately coarser: the inner store's tip lags the tip this
//! layer reports by at most two buffers. A crash loses only work that the
//! catch-up loop re-executes from its retained ledger — the overlay start-up
//! path already truncates the ledger to the durable tip. Every operation that
//! must observe the durable state (disconnect, rebase, compaction, direct
//! UTXO mutation, snapshot export) flushes and waits first.

use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex, RwLock},
    thread::JoinHandle,
    time::{Duration, Instant},
};

use bitcoin::BlockHash;

use crate::{
    OutPointKey, Utxo,
    chain_store::{ChainStoreError, ConnectTransition, ExecutionChainStore},
    execution_store::{ExecutionStoreError, ExecutionTip},
    headers::HeaderDag,
    utxo::{TierStats, UtxoError, UtxoStore, UtxoUndo},
};

/// When the buffered batches are handed to the inner store.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WriteBackLimits {
    /// Flush once this many batches are buffered. `1` flushes every batch,
    /// which is the inner store's own behaviour plus one map lookup per read.
    pub max_batches: u32,
    /// Flush once the buffered net-created coins reach this count, whatever
    /// the batch count; bounds memory on coin-heavy stretches.
    pub max_created: u64,
}

impl WriteBackLimits {
    /// Flush every batch: semantics identical to the bare inner store.
    pub const PASS_THROUGH: Self = Self {
        max_batches: 1,
        max_created: u64::MAX,
    };
}

/// What one flush wrote to the inner store.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct WriteBackFlush {
    /// Batches folded into this flush.
    pub batches: u32,
    /// Blocks whose transitions were committed.
    pub blocks: u32,
    /// Coins still unspent at flush time and therefore written.
    pub created: u64,
    /// Spends that reached the inner store (the coin pre-dated the buffer).
    pub spent: u64,
    /// Coins created and spent inside the buffer that never reached disk.
    pub cancelled: u64,
    /// Wall time of the inner commit, on whichever thread ran it.
    pub elapsed: Duration,
    /// Time the caller blocked on this commit: zero when it finished in the
    /// background before anyone needed it, the whole commit when it did not.
    pub waited: Duration,
}

#[derive(Default)]
struct Pending {
    transitions: Vec<ConnectTransition>,
    /// Net-created coins: created in the buffer and not yet spent.
    created: HashMap<OutPointKey, Utxo>,
    /// Spends of coins the inner store (or the in-flight buffer) holds.
    spent: HashSet<OutPointKey>,
    /// Tip after the last buffered transition; `None` when empty.
    tip: Option<ExecutionTip>,
    batches: u32,
    cancelled: u64,
}

impl Pending {
    fn is_empty(&self) -> bool {
        self.transitions.is_empty()
    }
}

/// A buffer handed to the engine: still answers reads until the commit lands.
struct Committed {
    transitions: Vec<ConnectTransition>,
    created: HashMap<OutPointKey, Utxo>,
    spent: HashSet<OutPointKey>,
    tip: ExecutionTip,
}

struct InFlight {
    state: Arc<Committed>,
    handle: JoinHandle<Result<WriteBackFlush, ChainStoreError>>,
}

/// Write-back buffer over any execution chain store.
pub struct WriteBackChainstate<C> {
    inner: Arc<C>,
    limits: WriteBackLimits,
    pending: RwLock<Pending>,
    in_flight: Mutex<Option<InFlight>>,
    finished: Mutex<Vec<WriteBackFlush>>,
    /// Set once a background commit has failed; every later mutation
    /// refuses, since the in-memory view no longer has a durable ancestor.
    failed: Mutex<Option<String>>,
}

impl<C: ExecutionChainStore + 'static> WriteBackChainstate<C> {
    /// Wraps `inner`; nothing is buffered until the first commit.
    #[must_use]
    pub fn new(inner: C, limits: WriteBackLimits) -> Self {
        let limits = WriteBackLimits {
            max_batches: limits.max_batches.max(1),
            max_created: limits.max_created.max(1),
        };
        Self {
            inner: Arc::new(inner),
            limits,
            pending: RwLock::new(Pending::default()),
            in_flight: Mutex::new(None),
            finished: Mutex::new(Vec::new()),
            failed: Mutex::new(None),
        }
    }

    /// The wrapped store. Reads through it bypass the buffer.
    pub fn inner(&self) -> &C {
        &self.inner
    }

    /// Mutable access to the wrapped store, flushing and waiting first so the
    /// store's durable state is current for whatever the caller does with it.
    ///
    /// # Errors
    ///
    /// Fails if the flush fails; the buffer is kept.
    pub fn inner_mut_flushed(&mut self) -> Result<&mut C, ChainStoreError> {
        self.flush()?;
        Arc::get_mut(&mut self.inner).ok_or(ChainStoreError::Utxo(UtxoError::Malformed(
            "write-back store is still shared with a background commit",
        )))
    }

    /// Mutable access to the wrapped store for work that only reshapes the
    /// engine's storage — compaction — and needs none of the buffered state.
    ///
    /// Waits for a background commit so the engine is quiescent, but keeps
    /// the accepting buffer in memory: the engine's logical content is the
    /// same before and after such work, so the buffer stays valid over it.
    ///
    /// # Errors
    ///
    /// Fails if the background commit failed.
    pub fn inner_mut_settled(&mut self) -> Result<&mut C, ChainStoreError> {
        self.check_failed()?;
        // A settled commit is recorded for the caller's log by `settle`.
        self.wait_in_flight(Instant::now())?;
        Arc::get_mut(&mut self.inner).ok_or(ChainStoreError::Utxo(UtxoError::Malformed(
            "write-back store is still shared with a background commit",
        )))
    }

    /// Whether a background commit is still running.
    #[must_use]
    pub fn commit_in_progress(&self) -> bool {
        self.lock_in_flight()
            .as_ref()
            .is_some_and(|in_flight| !in_flight.handle.is_finished())
    }

    /// Blocks buffered but not yet handed to the inner store.
    #[must_use]
    pub fn pending_blocks(&self) -> u32 {
        u32::try_from(self.read().transitions.len()).unwrap_or(u32::MAX)
    }

    /// Net-created coins currently held in the accepting buffer.
    #[must_use]
    pub fn pending_created(&self) -> u64 {
        u64::try_from(self.read().created.len()).unwrap_or(u64::MAX)
    }

    /// Blocks whose commit is running in the background right now.
    #[must_use]
    pub fn in_flight_blocks(&self) -> u32 {
        self.in_flight_state().map_or(0, |state| {
            u32::try_from(state.transitions.len()).unwrap_or(u32::MAX)
        })
    }

    /// Approximate bytes the buffers hold, for capacity accounting.
    #[must_use]
    pub fn pending_estimated_bytes(&self) -> u64 {
        let pending = self.read();
        let mut coins = u64::try_from(pending.created.len()).unwrap_or(u64::MAX);
        let mut spends = u64::try_from(pending.spent.len()).unwrap_or(u64::MAX);
        drop(pending);
        if let Some(state) = self.in_flight_state() {
            coins = coins.saturating_add(u64::try_from(state.created.len()).unwrap_or(u64::MAX));
            spends = spends.saturating_add(u64::try_from(state.spent.len()).unwrap_or(u64::MAX));
        }
        coins
            .saturating_mul(ESTIMATED_COIN_BYTES)
            .saturating_add(spends.saturating_mul(ESTIMATED_SPEND_BYTES))
    }

    /// Takes the record of the oldest finished flush not yet reported.
    ///
    /// A commit still running is not reported; call again later.
    pub fn take_last_flush(&self) -> Option<WriteBackFlush> {
        // Harvest a background commit that finished on its own, without
        // blocking on one that is still running.
        let finished_now = {
            let mut slot = self.lock_in_flight();
            match slot.as_ref() {
                Some(in_flight) if in_flight.handle.is_finished() => slot.take(),
                _ => None,
            }
        };
        if let Some(in_flight) = finished_now {
            // A failure is recorded and surfaces on the next mutation.
            let _ = self.settle(in_flight, Instant::now());
        }
        let mut finished = self
            .finished
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if finished.is_empty() {
            None
        } else {
            Some(finished.remove(0))
        }
    }

    /// Commits everything buffered to the inner store and waits for it.
    ///
    /// Returns the record of the commit this call made, or `None` when
    /// nothing was buffered (a commit that was already in flight is waited
    /// for either way and reported through [`Self::take_last_flush`]).
    ///
    /// # Errors
    ///
    /// Propagates a commit error, this call's or an earlier background one;
    /// the buffers are left intact so the in-memory view stays consistent
    /// for a caller that abandons the run.
    pub fn flush(&self) -> Result<Option<WriteBackFlush>, ChainStoreError> {
        let started_one = self.start_flush()?;
        let waited_started = Instant::now();
        let settled = self.wait_in_flight(waited_started)?;
        // The record stays in the finished list as well: callers that do not
        // report it themselves harvest it with `take_last_flush`.
        Ok(if started_one { settled } else { None })
    }

    /// Hands the accepting buffer to a background commit without waiting.
    ///
    /// Waits for a previous commit first so engine commits stay sequential.
    /// Returns whether a new commit was started.
    ///
    /// # Errors
    ///
    /// Propagates a failed earlier commit.
    pub fn start_flush(&self) -> Result<bool, ChainStoreError> {
        self.check_failed()?;
        // Only one commit at a time: the tips must advance in order.
        self.wait_in_flight(Instant::now())?;
        let (state, batches, cancelled) = {
            let mut pending = self.write();
            if pending.is_empty() {
                return Ok(false);
            }
            let taken = std::mem::take(&mut *pending);
            let tip = taken.tip.expect("non-empty buffer has a tip");
            (
                Arc::new(Committed {
                    transitions: taken.transitions,
                    created: taken.created,
                    spent: taken.spent,
                    tip,
                }),
                taken.batches,
                taken.cancelled,
            )
        };
        let inner = Arc::clone(&self.inner);
        let thread_state = Arc::clone(&state);
        let handle = std::thread::Builder::new()
            .name("write-back-flush".to_owned())
            .spawn(move || {
                let started = Instant::now();
                inner.commit_connect_batch(&thread_state.transitions)?;
                Ok(WriteBackFlush {
                    batches,
                    blocks: u32::try_from(thread_state.transitions.len()).unwrap_or(u32::MAX),
                    created: u64::try_from(thread_state.created.len()).unwrap_or(u64::MAX),
                    spent: u64::try_from(thread_state.spent.len()).unwrap_or(u64::MAX),
                    cancelled,
                    elapsed: started.elapsed(),
                    waited: Duration::ZERO,
                })
            })
            .map_err(|_| {
                ChainStoreError::Utxo(UtxoError::Malformed(
                    "could not spawn the write-back commit thread",
                ))
            })?;
        *self.lock_in_flight() = Some(InFlight { state, handle });
        Ok(true)
    }

    /// Waits for the background commit, if any, and records its outcome.
    fn wait_in_flight(
        &self,
        waited_started: Instant,
    ) -> Result<Option<WriteBackFlush>, ChainStoreError> {
        let in_flight = self.lock_in_flight().take();
        match in_flight {
            Some(in_flight) => {
                let already_done = in_flight.handle.is_finished();
                let flush = self.settle(in_flight, waited_started)?;
                let _ = already_done;
                Ok(Some(flush))
            }
            None => Ok(None),
        }
    }

    /// Joins a commit thread and records success or failure.
    fn settle(
        &self,
        in_flight: InFlight,
        waited_started: Instant,
    ) -> Result<WriteBackFlush, ChainStoreError> {
        let was_finished = in_flight.handle.is_finished();
        let outcome = in_flight.handle.join().unwrap_or_else(|_| {
            Err(ChainStoreError::Utxo(UtxoError::Malformed(
                "write-back commit thread panicked",
            )))
        });
        match outcome {
            Ok(mut flush) => {
                flush.waited = if was_finished {
                    Duration::ZERO
                } else {
                    waited_started.elapsed()
                };
                self.finished
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .push(flush);
                Ok(flush)
            }
            Err(error) => {
                // Keep the committed buffer readable so the in-memory view
                // stays consistent, and refuse further mutation.
                *self
                    .failed
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(error.to_string());
                *self.lock_in_flight() = Some(InFlight {
                    state: in_flight.state,
                    handle: std::thread::spawn(|| {
                        Err(ChainStoreError::Utxo(UtxoError::Malformed(
                            "write-back commit already failed",
                        )))
                    }),
                });
                Err(error)
            }
        }
    }

    fn check_failed(&self) -> Result<(), ChainStoreError> {
        if self
            .failed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_some()
        {
            return Err(ChainStoreError::Utxo(UtxoError::Malformed(
                "write-back commit failed earlier; the store must be reopened",
            )));
        }
        Ok(())
    }

    fn read(&self) -> std::sync::RwLockReadGuard<'_, Pending> {
        self.pending
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn write(&self) -> std::sync::RwLockWriteGuard<'_, Pending> {
        self.pending
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn lock_in_flight(&self) -> std::sync::MutexGuard<'_, Option<InFlight>> {
        self.in_flight
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn in_flight_state(&self) -> Option<Arc<Committed>> {
        self.lock_in_flight()
            .as_ref()
            .map(|in_flight| Arc::clone(&in_flight.state))
    }

    /// The newest tip this layer knows: accepting buffer, in-flight buffer,
    /// then the engine.
    fn current_tip(
        &self,
        pending: &Pending,
        in_flight: Option<&Committed>,
    ) -> Result<ExecutionTip, ChainStoreError> {
        if let Some(tip) = pending.tip {
            return Ok(tip);
        }
        if let Some(state) = in_flight {
            return Ok(state.tip);
        }
        self.inner.execution_tip()
    }

    /// Buffers one contiguous batch, starting a background flush afterwards
    /// if a limit is hit.
    fn buffer(&self, transitions: Vec<ConnectTransition>) -> Result<(), ChainStoreError> {
        if transitions.is_empty() {
            return Ok(());
        }
        self.check_failed()?;
        let in_flight = self.in_flight_state();
        {
            let mut pending = self.write();
            let mut tip = self.current_tip(&pending, in_flight.as_deref())?;
            // Validate the whole batch against the in-memory view before
            // touching it, so a rejected batch leaves the buffer unchanged.
            let mut staged_created: Vec<(OutPointKey, Utxo)> = Vec::new();
            let mut staged_spent: Vec<OutPointKey> = Vec::new();
            let mut staged_cancelled: Vec<OutPointKey> = Vec::new();
            let mut batch_created: HashSet<OutPointKey> = HashSet::new();
            let mut batch_spent: HashSet<OutPointKey> = HashSet::new();
            let known_created = |key: &OutPointKey| {
                pending.created.contains_key(key)
                    || in_flight
                        .as_ref()
                        .is_some_and(|state| state.created.contains_key(key))
            };
            let known_spent = |key: &OutPointKey| {
                pending.spent.contains(key)
                    || in_flight
                        .as_ref()
                        .is_some_and(|state| state.spent.contains(key))
            };
            for transition in &transitions {
                if transition.expected_parent != tip.hash
                    || tip.height.checked_add(1) != Some(transition.next.height)
                {
                    return Err(ChainStoreError::Execution(
                        ExecutionStoreError::NonSequential {
                            current_height: tip.height,
                            current_hash: tip.hash,
                        },
                    ));
                }
                for key in &transition.spent {
                    if !batch_spent.insert(*key) || known_spent(key) {
                        return Err(ChainStoreError::Utxo(UtxoError::DuplicateSpend(*key)));
                    }
                    if batch_created.remove(key) || pending.created.contains_key(key) {
                        staged_cancelled.push(*key);
                    } else {
                        // Spends of coins the in-flight buffer created go to
                        // the engine as ordinary spends: by the time this
                        // buffer is committed those coins are durable.
                        staged_spent.push(*key);
                    }
                }
                for (key, utxo) in &transition.created {
                    if batch_created.contains(key) || known_created(key) {
                        return Err(ChainStoreError::Utxo(UtxoError::Duplicate(*key)));
                    }
                    batch_created.insert(*key);
                    staged_created.push((*key, utxo.clone()));
                }
                tip = transition.next;
            }
            // Creations cancelled within this same batch were removed from
            // `batch_created` above and must not be inserted.
            let cancelled_in_batch: HashSet<OutPointKey> =
                staged_cancelled.iter().copied().collect();
            for (key, utxo) in staged_created {
                if cancelled_in_batch.contains(&key) {
                    continue;
                }
                pending.created.insert(key, utxo);
            }
            for key in &staged_cancelled {
                pending.created.remove(key);
            }
            pending.cancelled = pending
                .cancelled
                .saturating_add(u64::try_from(staged_cancelled.len()).unwrap_or(u64::MAX));
            pending.spent.extend(staged_spent);
            pending.transitions.extend(transitions);
            pending.tip = Some(tip);
            pending.batches = pending.batches.saturating_add(1);
            let created = u64::try_from(pending.created.len()).unwrap_or(u64::MAX);
            let limit_hit =
                pending.batches >= self.limits.max_batches || created >= self.limits.max_created;
            if !limit_hit {
                return Ok(());
            }
            // The batch limit is a target, not a wall: while the previous
            // commit is still running the buffer keeps accepting, up to twice
            // the batch limit, so the loop never waits for the engine unless
            // the coin limit or that ceiling is reached. The next batch
            // retries the start, by which time the commit has usually landed.
            let must_flush = pending.batches >= self.limits.max_batches.saturating_mul(2)
                || created >= self.limits.max_created;
            if !must_flush && self.commit_in_progress() {
                return Ok(());
            }
        }
        self.start_flush().map(|_| ())
    }

    /// Looks a key up in the accepting buffer, then the in-flight one.
    fn lookup_buffers(
        pending: &Pending,
        in_flight: Option<&Committed>,
        outpoint: &OutPointKey,
    ) -> BufferAnswer {
        if let Some(utxo) = pending.created.get(outpoint) {
            return BufferAnswer::Coin(utxo.clone());
        }
        if pending.spent.contains(outpoint) {
            return BufferAnswer::Spent;
        }
        if let Some(state) = in_flight {
            if let Some(utxo) = state.created.get(outpoint) {
                return BufferAnswer::Coin(utxo.clone());
            }
            if state.spent.contains(outpoint) {
                return BufferAnswer::Spent;
            }
        }
        BufferAnswer::Unknown
    }
}

impl<C> Drop for WriteBackChainstate<C> {
    fn drop(&mut self) {
        // Never leave a commit thread running past the store it writes to.
        let in_flight = self
            .in_flight
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(in_flight) = in_flight {
            let _ = in_flight.handle.join();
        }
    }
}

/// What the in-memory buffers know about a key.
enum BufferAnswer {
    /// Created in a buffer and still unspent.
    Coin(Utxo),
    /// Spent by a buffer; the engine's copy, if any, is stale.
    Spent,
    /// Not touched by any buffer: ask the engine.
    Unknown,
}

/// Rough in-memory footprint of one buffered coin (key, value, map slot).
const ESTIMATED_COIN_BYTES: u64 = 128;
/// Rough in-memory footprint of one buffered spend.
const ESTIMATED_SPEND_BYTES: u64 = 64;

impl<C: ExecutionChainStore + 'static> UtxoStore for WriteBackChainstate<C> {
    fn get(&self, outpoint: OutPointKey) -> Result<Option<Utxo>, UtxoError> {
        let in_flight = self.in_flight_state();
        {
            let pending = self.read();
            match Self::lookup_buffers(&pending, in_flight.as_deref(), &outpoint) {
                BufferAnswer::Coin(utxo) => return Ok(Some(utxo)),
                BufferAnswer::Spent => return Ok(None),
                BufferAnswer::Unknown => {}
            }
        }
        self.inner.get(outpoint)
    }

    fn get_many(
        &self,
        outpoints: &[OutPointKey],
    ) -> Result<Vec<(OutPointKey, Option<Utxo>)>, UtxoError> {
        let mut results: Vec<(OutPointKey, Option<Utxo>)> = Vec::with_capacity(outpoints.len());
        let mut inner_wanted: Vec<OutPointKey> = Vec::new();
        let mut inner_positions: Vec<usize> = Vec::new();
        let in_flight = self.in_flight_state();
        {
            let pending = self.read();
            for outpoint in outpoints {
                match Self::lookup_buffers(&pending, in_flight.as_deref(), outpoint) {
                    BufferAnswer::Coin(utxo) => results.push((*outpoint, Some(utxo))),
                    BufferAnswer::Spent => results.push((*outpoint, None)),
                    BufferAnswer::Unknown => {
                        inner_positions.push(results.len());
                        inner_wanted.push(*outpoint);
                        results.push((*outpoint, None));
                    }
                }
            }
        }
        if !inner_wanted.is_empty() {
            let resolved = self.inner.get_many(&inner_wanted)?;
            if resolved.len() != inner_wanted.len() {
                return Err(UtxoError::Malformed(
                    "inner store returned a misaligned get_many result",
                ));
            }
            for (position, (_, utxo)) in inner_positions.into_iter().zip(resolved) {
                results[position].1 = utxo;
            }
        }
        Ok(results)
    }

    fn apply(
        &self,
        spent: &[OutPointKey],
        created: &[(OutPointKey, Utxo)],
    ) -> Result<(), UtxoError> {
        self.flush().map_err(chain_store_to_utxo)?;
        self.inner.apply(spent, created)
    }

    fn apply_with_undo(
        &self,
        spent: &[OutPointKey],
        created: &[(OutPointKey, Utxo)],
    ) -> Result<UtxoUndo, UtxoError> {
        self.flush().map_err(chain_store_to_utxo)?;
        self.inner.apply_with_undo(spent, created)
    }

    fn apply_with_undo_fresh_outputs(
        &self,
        spent: &[OutPointKey],
        created: &[(OutPointKey, Utxo)],
    ) -> Result<UtxoUndo, UtxoError> {
        self.flush().map_err(chain_store_to_utxo)?;
        self.inner.apply_with_undo_fresh_outputs(spent, created)
    }

    fn undo(&self, undo: &UtxoUndo, now: u64, hot_window_secs: u64) -> Result<(), UtxoError> {
        self.flush().map_err(chain_store_to_utxo)?;
        self.inner.undo(undo, now, hot_window_secs)
    }

    fn age_to_cold(&self, now: u64, hot_window_secs: u64) -> Result<u64, UtxoError> {
        self.inner.age_to_cold(now, hot_window_secs)
    }

    fn snapshot_entries(&self) -> Result<std::collections::BTreeMap<OutPointKey, Utxo>, UtxoError> {
        self.flush().map_err(chain_store_to_utxo)?;
        self.inner.snapshot_entries()
    }

    fn replace_all(
        &self,
        entries: &std::collections::BTreeMap<OutPointKey, Utxo>,
        now: u64,
        hot_window_secs: u64,
    ) -> Result<(), UtxoError> {
        self.flush().map_err(chain_store_to_utxo)?;
        self.inner.replace_all(entries, now, hot_window_secs)
    }

    fn tier_stats(&self) -> Result<TierStats, UtxoError> {
        let mut stats = self.inner.tier_stats()?;
        stats.hot = stats
            .hot
            .saturating_add(u64::try_from(self.read().created.len()).unwrap_or(u64::MAX));
        if let Some(state) = self.in_flight_state() {
            stats.hot = stats
                .hot
                .saturating_add(u64::try_from(state.created.len()).unwrap_or(u64::MAX));
        }
        Ok(stats)
    }
}

impl<C: ExecutionChainStore + 'static> ExecutionChainStore for WriteBackChainstate<C> {
    fn execution_tip(&self) -> Result<ExecutionTip, ChainStoreError> {
        let in_flight = self.in_flight_state();
        let pending = self.read();
        self.current_tip(&pending, in_flight.as_deref())
    }

    fn assumed_snapshot_base(&self) -> Result<Option<ExecutionTip>, ChainStoreError> {
        self.inner.assumed_snapshot_base()
    }

    fn block_undo(&self, hash: BlockHash) -> Result<Option<Vec<UtxoUndo>>, ChainStoreError> {
        {
            let pending = self.read();
            if let Some(transition) = pending
                .transitions
                .iter()
                .rev()
                .find(|transition| transition.next.hash == hash)
            {
                return Ok(Some(transition.transaction_undos.clone()));
            }
        }
        if let Some(state) = self.in_flight_state() {
            if let Some(transition) = state
                .transitions
                .iter()
                .rev()
                .find(|transition| transition.next.hash == hash)
            {
                return Ok(Some(transition.transaction_undos.clone()));
            }
        }
        self.inner.block_undo(hash)
    }

    fn retains_block_undo(&self) -> bool {
        self.inner.retains_block_undo()
    }

    fn take_commit_profile(&self) -> Option<[u64; 5]> {
        self.inner.take_commit_profile()
    }

    fn commit_connect(
        &self,
        expected_parent: BlockHash,
        next: ExecutionTip,
        spent: &[OutPointKey],
        created: &[(OutPointKey, Utxo)],
        transaction_undos: &[UtxoUndo],
    ) -> Result<UtxoUndo, ChainStoreError> {
        // The logical undo of this block is its spends (with pre-images the
        // caller already carries in `transaction_undos`) and its creations;
        // the buffered transition keeps the full per-transaction list for
        // `block_undo`, so return the flattened form the trait promises.
        let mut undo_spent = Vec::new();
        for undo in transaction_undos {
            undo_spent.extend(undo.spent().iter().cloned());
        }
        self.buffer(vec![ConnectTransition {
            expected_parent,
            next,
            spent: spent.to_vec(),
            created: created.to_vec(),
            transaction_undos: transaction_undos.to_vec(),
        }])?;
        Ok(UtxoUndo::from_parts(
            undo_spent,
            created.iter().map(|(key, _)| *key).collect(),
        ))
    }

    fn commit_connect_batch(
        &self,
        transitions: &[ConnectTransition],
    ) -> Result<(), ChainStoreError> {
        self.buffer(transitions.to_vec())
    }

    fn commit_connect_batch_owned(
        &self,
        transitions: Vec<ConnectTransition>,
    ) -> Result<(), ChainStoreError> {
        self.buffer(transitions)
    }

    fn commit_disconnect(
        &self,
        expected_current: ExecutionTip,
        parent: ExecutionTip,
        spent: &[OutPointKey],
        created: &[(OutPointKey, Utxo)],
        transaction_undos: &[UtxoUndo],
    ) -> Result<UtxoUndo, ChainStoreError> {
        self.flush()?;
        self.inner
            .commit_disconnect(expected_current, parent, spent, created, transaction_undos)
    }

    fn prune_block_undos_before(
        &self,
        headers: &HeaderDag,
        retain_from_height: u32,
    ) -> Result<u64, ChainStoreError> {
        // Buffered undo belongs to the newest blocks, which sit above any
        // retention floor the ledger window can produce. Pruning needs the
        // engine's writer, which a background commit holds for its whole
        // duration; housekeeping is not worth stalling the loop for, so it
        // waits for a batch on which no commit is running.
        if self.commit_in_progress() {
            return Ok(0);
        }
        self.inner
            .prune_block_undos_before(headers, retain_from_height)
    }

    fn take_hottest_legacy_validation_delta(&self) -> Option<u32> {
        self.inner.take_hottest_legacy_validation_delta()
    }

    fn shard_legacy_validation_delta(
        &self,
        height: u32,
    ) -> Result<Option<crate::chain_store::ValidationDeltaShardMigration>, ChainStoreError> {
        self.inner.shard_legacy_validation_delta(height)
    }
}

fn chain_store_to_utxo(error: ChainStoreError) -> UtxoError {
    match error {
        ChainStoreError::Utxo(error) => error,
        _ => UtxoError::Malformed("write-back flush failed before a direct UTXO mutation"),
    }
}

#[cfg(test)]
mod tests {
    use bitcoin::{Network, OutPoint, Txid, hashes::Hash};
    use tempfile::TempDir;

    use super::*;
    use crate::chain_store::RedbChainStore;

    fn key(byte: u8) -> OutPointKey {
        OutPoint::new(Txid::from_byte_array([byte; 32]), 0).into()
    }

    fn coin(value_sats: u64) -> Utxo {
        Utxo {
            value_sats,
            height: 1,
            is_coinbase: false,
            last_touched: 0,
            creation_mtp: 0,
            script_pubkey: vec![0x51],
        }
    }

    fn tip(height: u32) -> ExecutionTip {
        ExecutionTip {
            height,
            hash: BlockHash::from_byte_array([u8::try_from(height).unwrap() + 0x40; 32]),
        }
    }

    fn transition(
        parent: ExecutionTip,
        next: ExecutionTip,
        spent: Vec<OutPointKey>,
        created: Vec<(OutPointKey, Utxo)>,
    ) -> ConnectTransition {
        ConnectTransition {
            expected_parent: parent.hash,
            next,
            spent,
            created,
            transaction_undos: Vec::new(),
        }
    }

    fn open(directory: &TempDir, name: &str) -> RedbChainStore {
        RedbChainStore::open(directory.path().join(name), Network::Regtest).unwrap()
    }

    #[test]
    fn settled_access_keeps_the_buffer_while_flushed_access_drains_it() {
        let directory = TempDir::new().unwrap();
        let inner = open(&directory, "settled.redb");
        let genesis = inner.execution_tip().unwrap();
        let mut store = WriteBackChainstate::new(
            inner,
            WriteBackLimits {
                max_batches: 2,
                max_created: u64::MAX,
            },
        );
        store
            .commit_connect_batch(&[transition(genesis, tip(1), vec![], vec![(key(1), coin(5))])])
            .unwrap();
        store
            .commit_connect_batch(&[transition(tip(1), tip(2), vec![], vec![(key(2), coin(6))])])
            .unwrap();
        store
            .commit_connect_batch(&[transition(tip(2), tip(3), vec![], vec![(key(3), coin(7))])])
            .unwrap();
        // Batches 1-2 hit the limit and are committing in the background;
        // batch 3 waits in the accepting buffer.

        // Compaction-style access waits for the running commit only.
        let engine = store.inner_mut_settled().unwrap();
        assert_eq!(engine.execution_tip().unwrap(), tip(2));
        assert_eq!(engine.get(key(2)).unwrap(), Some(coin(6)));
        assert_eq!(engine.get(key(3)).unwrap(), None);
        assert!(!store.commit_in_progress());
        assert_eq!(store.pending_blocks(), 1);
        assert_eq!(store.execution_tip().unwrap(), tip(3));
        assert_eq!(store.get(key(3)).unwrap(), Some(coin(7)));
        assert_eq!(store.take_last_flush().map(|flush| flush.blocks), Some(2));

        // Flushed access drains the buffer as before.
        let engine = store.inner_mut_flushed().unwrap();
        assert_eq!(engine.execution_tip().unwrap(), tip(3));
        assert_eq!(engine.get(key(3)).unwrap(), Some(coin(7)));
        assert_eq!(store.pending_blocks(), 0);
        assert_eq!(store.take_last_flush().map(|flush| flush.blocks), Some(1));
    }

    #[test]
    fn buffers_until_the_batch_limit_and_serves_reads_from_memory() {
        let directory = TempDir::new().unwrap();
        let inner = open(&directory, "buffered.redb");
        let genesis = inner.execution_tip().unwrap();
        let store = WriteBackChainstate::new(
            inner,
            WriteBackLimits {
                max_batches: 2,
                max_created: u64::MAX,
            },
        );

        store
            .commit_connect_batch(&[transition(
                genesis,
                tip(1),
                vec![],
                vec![(key(1), coin(5)), (key(2), coin(6))],
            )])
            .unwrap();
        // Buffered: visible through the layer, absent from the engine.
        assert_eq!(store.execution_tip().unwrap(), tip(1));
        assert_eq!(store.inner().execution_tip().unwrap(), genesis);
        assert_eq!(store.get(key(1)).unwrap(), Some(coin(5)));
        assert_eq!(store.inner().get(key(1)).unwrap(), None);
        assert_eq!(store.pending_blocks(), 1);
        assert!(store.take_last_flush().is_none());

        store
            .commit_connect_batch(&[transition(
                tip(1),
                tip(2),
                vec![key(1)],
                vec![(key(3), coin(7))],
            )])
            .unwrap();
        // Second batch hit the limit: a background commit is running or
        // done; the layer answers from the in-flight buffer meanwhile.
        assert_eq!(store.pending_blocks(), 0);
        assert_eq!(store.execution_tip().unwrap(), tip(2));
        assert_eq!(store.get(key(2)).unwrap(), Some(coin(6)));
        assert_eq!(store.get(key(1)).unwrap(), None);
        // Waiting for it yields the record with the pair key(1) cancelled.
        assert!(store.flush().unwrap().is_none(), "nothing new to commit");
        let flush = store.take_last_flush().expect("limit reached");
        assert_eq!(flush.batches, 2);
        assert_eq!(flush.blocks, 2);
        assert_eq!(flush.created, 2);
        assert_eq!(flush.spent, 0);
        assert_eq!(flush.cancelled, 1);
        assert!(store.take_last_flush().is_none());
        assert_eq!(store.inner().execution_tip().unwrap(), tip(2));
        assert_eq!(store.inner().get(key(1)).unwrap(), None);
        assert_eq!(store.inner().get(key(2)).unwrap(), Some(coin(6)));
        assert_eq!(store.inner().get(key(3)).unwrap(), Some(coin(7)));
        assert_eq!(
            store
                .get_many(&[key(1), key(2), key(3), key(9)])
                .unwrap()
                .into_iter()
                .map(|(_, utxo)| utxo.map(|utxo| utxo.value_sats))
                .collect::<Vec<_>>(),
            vec![None, Some(6), Some(7), None]
        );
    }

    #[test]
    fn flushed_state_matches_a_store_that_committed_every_batch() {
        let directory = TempDir::new().unwrap();
        let direct = open(&directory, "direct.redb");
        let buffered = WriteBackChainstate::new(
            open(&directory, "buffered.redb"),
            WriteBackLimits {
                max_batches: 64,
                max_created: u64::MAX,
            },
        );
        let genesis = direct.execution_tip().unwrap();
        let batches = vec![
            vec![transition(
                genesis,
                tip(1),
                vec![],
                vec![(key(1), coin(1)), (key(2), coin(2))],
            )],
            vec![
                transition(tip(1), tip(2), vec![key(1)], vec![(key(3), coin(3))]),
                transition(tip(2), tip(3), vec![key(3)], vec![(key(4), coin(4))]),
            ],
            vec![transition(
                tip(3),
                tip(4),
                vec![key(2)],
                vec![(key(5), coin(5))],
            )],
        ];
        for batch in &batches {
            direct.commit_connect_batch(batch).unwrap();
            buffered.commit_connect_batch(batch).unwrap();
        }
        // Before the flush the engine behind the buffer is still at genesis,
        // but every read through the buffer already agrees with `direct`.
        assert_eq!(buffered.inner().execution_tip().unwrap(), genesis);
        for k in 1..=5 {
            assert_eq!(buffered.get(key(k)).unwrap(), direct.get(key(k)).unwrap());
        }
        let flush = buffered.flush().unwrap().expect("four buffered blocks");
        assert_eq!(flush.blocks, 4);
        assert_eq!(flush.batches, 3);
        assert_eq!(flush.created, 2);
        assert_eq!(flush.cancelled, 3);
        assert_eq!(flush.spent, 0);
        assert_eq!(buffered.inner().execution_tip().unwrap(), tip(4));
        assert_eq!(
            buffered.inner().snapshot_entries().unwrap(),
            direct.snapshot_entries().unwrap()
        );
        assert!(buffered.flush().unwrap().is_none());
    }

    #[test]
    fn batches_accepted_while_a_commit_is_in_flight_land_after_it() {
        let directory = TempDir::new().unwrap();
        let direct = open(&directory, "direct.redb");
        let buffered = WriteBackChainstate::new(
            open(&directory, "buffered.redb"),
            WriteBackLimits {
                max_batches: 1,
                max_created: u64::MAX,
            },
        );
        let genesis = direct.execution_tip().unwrap();
        // Every batch starts a background commit; the next batch is accepted
        // against the in-flight buffer and spends a coin it created.
        let batches = vec![
            vec![transition(
                genesis,
                tip(1),
                vec![],
                vec![(key(1), coin(1)), (key(2), coin(2))],
            )],
            vec![transition(
                tip(1),
                tip(2),
                vec![key(1)],
                vec![(key(3), coin(3))],
            )],
            vec![transition(
                tip(2),
                tip(3),
                vec![key(3)],
                vec![(key(4), coin(4))],
            )],
        ];
        for batch in &batches {
            direct.commit_connect_batch(batch).unwrap();
            buffered.commit_connect_batch(batch).unwrap();
            assert_eq!(
                buffered.execution_tip().unwrap(),
                batch.last().unwrap().next
            );
            for k in 1..=4 {
                assert_eq!(buffered.get(key(k)).unwrap(), direct.get(key(k)).unwrap());
            }
        }
        // The last batches are in flight or still accepting: re-creating a
        // coin they created, or spending a coin they spent, is rejected from
        // the buffers without waiting for the engine. (key(3) was created and
        // spent inside the window, so it cancelled and is unknown here;
        // key(1) is a durable coin the window spent.)
        assert!(matches!(
            buffered.commit_connect_batch(&[transition(
                tip(3),
                tip(4),
                vec![],
                vec![(key(4), coin(9))]
            )]),
            Err(ChainStoreError::Utxo(UtxoError::Duplicate(_)))
        ));
        assert!(matches!(
            buffered.commit_connect_batch(&[transition(tip(3), tip(4), vec![key(1)], vec![])]),
            Err(ChainStoreError::Utxo(UtxoError::DuplicateSpend(_)))
        ));
        buffered.flush().unwrap();
        let mut records = Vec::new();
        while let Some(record) = buffered.take_last_flush() {
            records.push(record);
        }
        // A batch that arrives while a commit is running stays in the buffer
        // and lands with the next one, so two or three commits cover the
        // three blocks.
        assert!(
            (2..=3).contains(&records.len()),
            "{} commits",
            records.len()
        );
        assert_eq!(records.iter().map(|record| record.blocks).sum::<u32>(), 3);
        assert_eq!(buffered.inner().execution_tip().unwrap(), tip(3));
        assert_eq!(
            buffered.inner().snapshot_entries().unwrap(),
            direct.snapshot_entries().unwrap()
        );
    }

    #[test]
    fn rejects_out_of_order_and_duplicate_transitions_without_touching_the_buffer() {
        let directory = TempDir::new().unwrap();
        let store = WriteBackChainstate::new(
            open(&directory, "buffered.redb"),
            WriteBackLimits {
                max_batches: 8,
                max_created: u64::MAX,
            },
        );
        let genesis = store.execution_tip().unwrap();
        store
            .commit_connect_batch(&[transition(genesis, tip(1), vec![], vec![(key(1), coin(1))])])
            .unwrap();

        // Wrong parent: the buffered tip is tip(1), not genesis.
        assert!(matches!(
            store.commit_connect_batch(&[transition(genesis, tip(2), vec![], vec![])]),
            Err(ChainStoreError::Execution(
                ExecutionStoreError::NonSequential { .. }
            ))
        ));
        // Re-creating a coin the buffer already holds.
        assert!(matches!(
            store.commit_connect_batch(&[transition(
                tip(1),
                tip(2),
                vec![],
                vec![(key(1), coin(9))]
            )]),
            Err(ChainStoreError::Utxo(UtxoError::Duplicate(_)))
        ));
        // Spending the same buffered coin twice inside one batch.
        assert!(matches!(
            store.commit_connect_batch(&[transition(tip(1), tip(2), vec![key(1), key(1)], vec![])]),
            Err(ChainStoreError::Utxo(UtxoError::DuplicateSpend(_)))
        ));
        assert_eq!(store.pending_blocks(), 1);
        assert_eq!(store.execution_tip().unwrap(), tip(1));
        assert_eq!(store.get(key(1)).unwrap(), Some(coin(1)));
    }

    #[test]
    fn coin_limit_forces_a_flush_before_the_batch_limit() {
        let directory = TempDir::new().unwrap();
        let store = WriteBackChainstate::new(
            open(&directory, "buffered.redb"),
            WriteBackLimits {
                max_batches: 64,
                max_created: 2,
            },
        );
        let genesis = store.execution_tip().unwrap();
        store
            .commit_connect_batch(&[transition(genesis, tip(1), vec![], vec![(key(1), coin(1))])])
            .unwrap();
        assert_eq!(store.pending_blocks(), 1);
        store
            .commit_connect_batch(&[transition(tip(1), tip(2), vec![], vec![(key(2), coin(2))])])
            .unwrap();
        assert_eq!(store.pending_blocks(), 0);
        store.flush().unwrap();
        assert_eq!(store.inner().execution_tip().unwrap(), tip(2));
        assert_eq!(store.take_last_flush().unwrap().created, 2);
    }
}
