//! Node-owned single-slot disk candidate scheduling and shared validation work.
use super::{
    DeploymentConfig, HeaderView, IDLE_SIDE_HEADER_TARGET, MAX_HEADERS_PER_RESPONSE, NetworkTime,
    NodeHeaderState, PeerRunError, RedbHeaderStore, receive_headers, request_headers, unix_time,
    unseen_header_suffix,
};
use crate::{
    header_candidate::{DiskHeaderCandidate, HeaderCandidateError, HeaderCandidateLimits},
    headers::HeaderWorkBudget,
    rbtc_info,
};
use std::{
    fs,
    path::PathBuf,
    sync::{Arc, Mutex, OnceLock},
    time::{Duration, Instant},
};

const WORK_CAPACITY: u64 = 1_000_000_000;
const WORK_PER_SECOND: u64 = 64_000_000;
pub(super) const BATCH_WORK: u64 = 32_000_000;
#[cfg(test)]
const PROMOTION_WORK: u64 = 256_000_000;

struct WorkPool {
    available: u64,
    updated: Instant,
}
impl WorkPool {
    fn refill(&mut self, now: Instant) {
        let earned = now
            .saturating_duration_since(self.updated)
            .as_nanos()
            .saturating_mul(u128::from(WORK_PER_SECOND))
            / 1_000_000_000;
        self.available = self
            .available
            .saturating_add(u64::try_from(earned).unwrap_or(u64::MAX))
            .min(WORK_CAPACITY);
        self.updated = now;
    }
    fn release(&mut self, unused: u64, reserved: u64, now: Instant) {
        self.refill(now);
        self.available = self
            .available
            .saturating_add(unused.min(reserved))
            .min(WORK_CAPACITY);
    }
    fn reserve(&mut self, units: u64, now: Instant) -> Result<(), Duration> {
        self.refill(now);
        if self.available < units {
            let nanos = (units - self.available) * 1_000_000_000 / WORK_PER_SECOND + 1;
            return Err(Duration::from_nanos(nanos));
        }
        self.available -= units;
        Ok(())
    }
}
// Queue only asynchronous admission, never a live work lease. Tokio's mutex
// hands the head position to waiters in FIFO order; cancelling a future removes
// its queue entry and drops any head guard it acquired.
struct WorkScheduler {
    pool: Mutex<WorkPool>,
    admission: tokio::sync::Mutex<()>,
    returned: tokio::sync::Notify,
}
impl WorkScheduler {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            pool: Mutex::new(WorkPool {
                available: WORK_CAPACITY,
                updated: Instant::now(),
            }),
            admission: tokio::sync::Mutex::new(()),
            returned: tokio::sync::Notify::new(),
        })
    }
    fn reserve(self: &Arc<Self>, units: u64) -> Result<HeaderWorkLease, Duration> {
        assert!(
            units <= WORK_CAPACITY,
            "header work request exceeds pool capacity"
        );
        self.pool
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .reserve(units, Instant::now())?;
        Ok(HeaderWorkLease {
            budget: HeaderWorkBudget::new(units),
            reserved: units,
            scheduler: Arc::clone(self),
        })
    }
    fn try_work(self: &Arc<Self>, units: u64) -> Result<HeaderWorkLease, Duration> {
        // Synchronous callers defer rather than jumping ahead of queued work.
        let _head = self
            .admission
            .try_lock()
            .map_err(|_| Duration::from_millis(10))?;
        self.reserve(units)
    }
    async fn work(self: &Arc<Self>, units: u64) -> HeaderWorkLease {
        assert!(
            units <= WORK_CAPACITY,
            "header work request exceeds pool capacity"
        );
        let _head = self.admission.lock().await;
        loop {
            let returned = self.returned.notified();
            match self.reserve(units) {
                Ok(lease) => return lease,
                Err(delay) => {
                    // One queue head waits on this notification. notify_one
                    // retains a permit if a lease returns before we poll it.
                    tokio::select! {
                        () = returned => {},
                        () = tokio::time::sleep(delay) => {},
                    }
                }
            }
        }
    }
}
fn work_scheduler() -> &'static Arc<WorkScheduler> {
    static SCHEDULER: OnceLock<Arc<WorkScheduler>> = OnceLock::new();
    SCHEDULER.get_or_init(WorkScheduler::new)
}

pub(super) struct HeaderWorkLease {
    pub(super) budget: HeaderWorkBudget,
    reserved: u64,
    scheduler: Arc<WorkScheduler>,
}
impl Drop for HeaderWorkLease {
    fn drop(&mut self) {
        {
            let mut pool = self
                .scheduler
                .pool
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            // Only unspent reservations return. Failed validation consumes work.
            pool.release(self.budget.remaining(), self.reserved, Instant::now());
        }
        self.scheduler.returned.notify_one();
    }
}
pub(super) fn try_header_work() -> Result<HeaderWorkLease, Duration> {
    work_scheduler().try_work(BATCH_WORK)
}
pub(super) async fn work(units: u64) -> HeaderWorkLease {
    work_scheduler().work(units).await
}

#[derive(Clone, Copy)]
pub(super) struct HeaderSyncPolicy {
    pub(super) spill_side_headers: usize,
    pub(super) promotion_bytes: usize,
    pub(super) retained_side_headers: Option<usize>,
}
impl Default for HeaderSyncPolicy {
    fn default() -> Self {
        Self {
            spill_side_headers: IDLE_SIDE_HEADER_TARGET,
            promotion_bytes: 8 * 1024 * 1024,
            retained_side_headers: None,
        }
    }
}
pub(super) fn candidate_path(path: &std::path::Path) -> PathBuf {
    path.with_extension("candidate")
}
fn local(error: impl std::fmt::Display) -> PeerRunError {
    PeerRunError::local(error.to_string())
}
fn received_candidate(error: HeaderCandidateError) -> PeerRunError {
    match error {
        HeaderCandidateError::Header(header) => PeerRunError::header(&header),
        other => local(other),
    }
}

// Recovery and import can outlast a peer's idle timeout on large journals.
// Ping between bounded frames so unsolicited messages use the session's normal
// bounded queue and network failures leave the durable cursor available.
pub(super) struct ReplayKeepalive(Instant);
impl ReplayKeepalive {
    pub(super) fn new(last_ping: Instant) -> Self {
        Self(last_ping)
    }
    /// Keep the same queued admission future alive while servicing the peer.
    /// A granted lease does not cancel an in-flight ping: finish its exchange
    /// before returning so the session never inherits a half-read response.
    pub(super) async fn wait<T>(
        &mut self,
        session: Option<&mut rbtc::p2p::PeerSession<tokio::net::TcpStream>>,
        admission: impl std::future::Future<Output = T>,
    ) -> Result<T, PeerRunError> {
        let Some(session) = session else {
            return Ok(admission.await);
        };
        tokio::pin!(admission);
        loop {
            let remaining = Duration::from_secs(20).saturating_sub(self.0.elapsed());
            if !remaining.is_zero() {
                tokio::select! {
                    granted = &mut admission => return Ok(granted),
                    () = tokio::time::sleep(remaining) => {}
                }
            }
            let ping = self.tick(Some(session));
            tokio::pin!(ping);
            tokio::select! {
                // Continue polling admission during ping I/O so a FIFO head
                // cannot stall all following candidates while awaiting Pong.
                granted = &mut admission => {
                    ping.await?;
                    return Ok(granted);
                }
                result = &mut ping => result?,
            }
        }
    }

    pub(super) async fn tick(
        &mut self,
        session: Option<&mut rbtc::p2p::PeerSession<tokio::net::TcpStream>>,
    ) -> Result<(), PeerRunError> {
        let Some(session) = session else {
            return Ok(());
        };
        if self.0.elapsed() >= Duration::from_secs(20) {
            tokio::time::timeout(super::PEER_TIMEOUT, session.ping(rand::random()))
                .await
                .map_err(|_| {
                    PeerRunError::transient("peer keepalive timed out during header replay")
                })?
                .map_err(|error| PeerRunError::p2p(&error))?;
            self.0 = Instant::now();
        }
        Ok(())
    }
}

async fn resume_candidate(
    mut session: Option<&mut rbtc::p2p::PeerSession<tokio::net::TcpStream>>,
    path: &std::path::Path,
    dag: &dyn HeaderView,
    now: u32,
) -> Result<Option<DiskHeaderCandidate>, PeerRunError> {
    let Some(anchor) = DiskHeaderCandidate::stored_anchor(path).map_err(local)? else {
        return Ok(None);
    };
    let mut keepalive = ReplayKeepalive::new(Instant::now());
    let mut recovery = {
        let mut lease = keepalive
            .wait(session.as_deref_mut(), work(BATCH_WORK))
            .await?;
        DiskHeaderCandidate::start_recovery(
            path,
            dag,
            anchor,
            HeaderCandidateLimits::default(),
            &mut lease.budget,
        )
        .map_err(local)?
    };
    loop {
        let done = {
            let mut lease = keepalive
                .wait(session.as_deref_mut(), work(BATCH_WORK))
                .await?;
            recovery.advance(1, now, &mut lease.budget).map_err(local)?
        };
        if done {
            return recovery.finish().map(Some).map_err(local);
        }
        tokio::task::yield_now().await;
    }
}

async fn promote(
    mut session: Option<&mut rbtc::p2p::PeerSession<tokio::net::TcpStream>>,
    candidate: &mut Option<DiskHeaderCandidate>,
    path: &std::path::Path,
    state: &mut NodeHeaderState,
    store: &RedbHeaderStore,
    now: u32,
    policy: HeaderSyncPolicy,
) -> Result<(), PeerRunError> {
    let pending = store.pending_candidate_tip().map_err(local)?;
    let Some(disk) = candidate.as_mut() else {
        if pending.is_some() {
            return Err(local("pending header promotion has no recoverable journal"));
        }
        return Ok(());
    };
    let tip = disk.tip();
    if pending.is_some_and(|hash| hash != tip.hash) {
        return Err(local("pending promotion and candidate journal disagree"));
    }
    if state.header(&tip.hash)? == Some(tip) {
        if pending.is_some() {
            store.finish_candidate_promotion(tip.hash).map_err(local)?;
        }
    } else {
        if pending.is_none() && tip.chainwork <= state.active_tip().chainwork {
            return Ok(());
        }
        // This allowance now covers one bounded streaming frame, never the
        // complete candidate. Engine cache and transaction memory are separate.
        let frame_bytes = usize::try_from(disk.len().min(2_000)).expect("bounded count")
            * (size_of::<bitcoin::block::Header>()
                + size_of::<crate::headers::HeaderInfo>()
                + size_of::<bitcoin::BlockHash>())
            * 2;
        if frame_bytes > policy.promotion_bytes {
            return Err(local("candidate promotion frame allowance exhausted"));
        }
        store.begin_candidate_promotion(tip.hash).map_err(local)?;
        {
            let mut reader = disk.reader().map_err(local)?;
            let mut keepalive = ReplayKeepalive::new(Instant::now());
            loop {
                {
                    let mut lease = keepalive
                        .wait(session.as_deref_mut(), work(BATCH_WORK))
                        .await?;
                    let Some(batch) = reader.next_batch(&mut lease.budget).map_err(local)? else {
                        break;
                    };
                    let unseen = unseen_header_suffix(state, &batch).map_err(local)?;
                    state.append(store, unseen, now, &mut lease.budget, true)?;
                }
                tokio::task::yield_now().await;
            }
        }
        if state.header(&tip.hash)? != Some(tip) || state.active_tip().chainwork < tip.chainwork {
            return Err(local("candidate promotion did not reach its validated tip"));
        }
        store.finish_candidate_promotion(tip.hash).map_err(local)?;
    }
    // Complete raw history and promotion completion are durable before unlink.
    drop(candidate.take());
    fs::remove_file(path).map_err(local)?;
    Ok(())
}

/// Startup must complete durable import intent before legacy seed readers run.
/// No network is needed: the journal already contains the validated winner.
pub(super) async fn recover_pending_promotion(
    store: &RedbHeaderStore,
    path: &std::path::Path,
    deployments: &DeploymentConfig,
    now: u32,
) -> Result<(), PeerRunError> {
    if store.pending_candidate_tip().map_err(local)?.is_none() {
        return Ok(());
    }
    let mut state = NodeHeaderState::resume(store, path, deployments, now, None, None).await?;
    let journal = candidate_path(path);
    let mut candidate = resume_candidate(None, &journal, &state, now).await?;
    promote(
        None,
        &mut candidate,
        &journal,
        &mut state,
        store,
        now,
        HeaderSyncPolicy::default(),
    )
    .await
}

#[cfg(test)]
pub(super) async fn sync_headers(
    session: &mut rbtc::p2p::PeerSession<tokio::net::TcpStream>,
    deployments: &DeploymentConfig,
    path: PathBuf,
    network_time: &NetworkTime,
    existing: Option<NodeHeaderState>,
) -> Result<NodeHeaderState, PeerRunError> {
    sync_headers_with_policy(
        session,
        deployments,
        path,
        network_time,
        existing,
        HeaderSyncPolicy::default(),
    )
    .await
}

pub(super) async fn sync_headers_bounded(
    session: &mut rbtc::p2p::PeerSession<tokio::net::TcpStream>,
    deployments: &DeploymentConfig,
    path: PathBuf,
    network_time: &NetworkTime,
    existing: Option<NodeHeaderState>,
    max_side_chain_headers: usize,
) -> Result<NodeHeaderState, PeerRunError> {
    sync_headers_with_policy(
        session,
        deployments,
        path,
        network_time,
        existing,
        HeaderSyncPolicy {
            spill_side_headers: max_side_chain_headers,
            retained_side_headers: Some(max_side_chain_headers),
            ..HeaderSyncPolicy::default()
        },
    )
    .await
}

#[allow(clippy::too_many_lines)]
pub(super) async fn sync_headers_with_policy(
    session: &mut rbtc::p2p::PeerSession<tokio::net::TcpStream>,
    deployments: &DeploymentConfig,
    path: PathBuf,
    network_time: &NetworkTime,
    existing: Option<NodeHeaderState>,
    policy: HeaderSyncPolicy,
) -> Result<NodeHeaderState, PeerRunError> {
    let pending_path = candidate_path(&path);
    let store = RedbHeaderStore::open(&path).map_err(local)?;
    let adjusted = network_time.adjusted_time(unix_time().map_err(PeerRunError::transient)?);
    let mut dag = NodeHeaderState::resume(
        &store,
        &path,
        deployments,
        adjusted,
        existing,
        Some(session),
    )
    .await?;
    let time = network_time.snapshot();
    rbtc_info!(
        "resuming headers-first sync from {}:{} (network_time_samples={} offset_seconds={} usable={})",
        dag.active_tip().height,
        dag.active_tip().hash,
        time.samples,
        time.offset_seconds,
        time.usable
    );

    let now = || {
        unix_time()
            .map(|time| network_time.adjusted_time(time))
            .map_err(PeerRunError::transient)
    };
    let mut candidate = resume_candidate(Some(session), &pending_path, &dag, now()?).await?;
    promote(
        Some(session),
        &mut candidate,
        &pending_path,
        &mut dag,
        &store,
        now()?,
        policy,
    )
    .await?;
    let mut recovery_tip = match store.recovery_tip().map_err(local)? {
        Some(hash) if dag.header(&hash)?.is_some() => hash,
        _ => dag.active_tip().hash,
    };
    let mut following_candidate = candidate.is_some();
    let mut recovering = false;
    loop {
        let locator = if following_candidate {
            let mut lease = work(BATCH_WORK).await;
            candidate
                .as_ref()
                .expect("candidate continuation")
                .block_locator(&dag, &mut lease.budget)
                .map_err(local)?
        } else {
            let mut lease = work(BATCH_WORK).await;
            lease.budget.consume(90_000).map_err(local)?;
            dag.branch_locator(recovery_tip)?
                .ok_or_else(|| local("header recovery ancestry missing"))?
        };
        request_headers(session, locator).await?;
        let headers = receive_headers(session).await?;
        let response_count = headers.len();
        if response_count == 0 {
            break;
        }
        if candidate
            .as_ref()
            .is_some_and(|disk| headers[0].prev_blockhash == disk.tip().hash)
        {
            {
                let mut lease = work(BATCH_WORK).await;
                candidate
                    .as_mut()
                    .expect("candidate checked")
                    .append(&headers, now()?, &mut lease.budget)
                    .map_err(received_candidate)?;
            }
            recovery_tip = headers.last().expect("nonempty response").block_hash();
            promote(
                Some(session),
                &mut candidate,
                &pending_path,
                &mut dag,
                &store,
                now()?,
                policy,
            )
            .await?;
            following_candidate = candidate.is_some();
            recovering = true;
            if response_count < MAX_HEADERS_PER_RESPONSE {
                break;
            }
            continue;
        }
        // A new peer can be on another branch. Keep the durable candidate, but
        // use that peer's retained ancestry for the rest of this conversation.
        following_candidate = false;
        let mut input_work = work(BATCH_WORK).await;
        input_work
            .budget
            .consume(4 * headers.len() as u64)
            .map_err(local)?;
        let unseen = unseen_header_suffix(&dag, &headers).map_err(|e| PeerRunError::header(&e))?;
        drop(input_work);
        let response_tip = headers.last().expect("nonempty response").block_hash();
        if unseen.is_empty() {
            if response_count < MAX_HEADERS_PER_RESPONSE
                || response_tip == recovery_tip
                || (recovering
                    && dag.header(&response_tip)?.map(|info| info.height)
                        <= dag.header(&recovery_tip)?.map(|info| info.height))
            {
                break;
            }
            store
                .append_recovery_batch(&[], response_tip)
                .map_err(local)?;
            recovery_tip = response_tip;
            recovering = true;
            continue;
        }
        let side = dag
            .retained_header_count()
            .saturating_sub(dag.active_tip().height as usize + 1);
        if unseen[0].prev_blockhash != dag.active_tip().hash && side >= policy.spill_side_headers {
            // A different peer can reveal a competing branch while the
            // single bounded journal holds a losing candidate. Reassign that
            // journal to the branch this session is following; discarded
            // losing headers remain reacquirable through the common locator.
            if candidate.is_some() {
                drop(candidate.take());
                fs::remove_file(&pending_path).map_err(local)?;
            }
            let mut lease = work(BATCH_WORK).await;
            let mut disk = DiskHeaderCandidate::open(
                &pending_path,
                &dag,
                unseen[0].prev_blockhash,
                now()?,
                HeaderCandidateLimits::default(),
                &mut lease.budget,
            )
            .map_err(local)?;
            disk.append(unseen, now()?, &mut lease.budget)
                .map_err(received_candidate)?;
            candidate = Some(disk);
            drop(lease);
            promote(
                Some(session),
                &mut candidate,
                &pending_path,
                &mut dag,
                &store,
                now()?,
                policy,
            )
            .await?;
            following_candidate = candidate.is_some();
        } else {
            let mut lease = work(BATCH_WORK).await;
            dag.append(&store, unseen, now()?, &mut lease.budget, true)?;
            if let Some(side_target) = policy.retained_side_headers {
                let candidate_anchor = candidate
                    .as_ref()
                    .map(DiskHeaderCandidate::anchor)
                    .into_iter()
                    .collect::<Vec<_>>();
                dag.retain_ingress(&store, side_target, &candidate_anchor, &mut lease.budget)?;
            }
        }
        recovery_tip = response_tip;
        recovering = true;
        if response_count < MAX_HEADERS_PER_RESPONSE {
            break;
        }
    }
    store.clear_recovery_tip().map_err(local)?;
    if let Some(side_target) = policy.retained_side_headers {
        let mut lease = work(BATCH_WORK).await;
        let candidate_anchor = candidate
            .as_ref()
            .map(DiskHeaderCandidate::anchor)
            .into_iter()
            .collect::<Vec<_>>();
        dag.retain_ingress(&store, side_target, &candidate_anchor, &mut lease.budget)?;
    }
    rbtc_info!(
        "peer returned no more headers at {}:{} (pending_disk_candidate={})",
        dag.active_tip().height,
        dag.active_tip().hash,
        candidate.is_some()
    );
    Ok(dag)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn work_pool_never_overbooks_and_refills_from_monotonic_time() {
        let now = Instant::now();
        let mut pool = WorkPool {
            available: BATCH_WORK,
            updated: now,
        };
        pool.reserve(BATCH_WORK, now).unwrap();
        assert_eq!(pool.available, 0);
        assert!(pool.reserve(1, now).is_err());
        pool.reserve(BATCH_WORK, now + Duration::from_millis(500))
            .unwrap();
        assert_eq!(pool.available, 0);
        pool.refill(now + Duration::from_secs(100));
        assert_eq!(pool.available, WORK_CAPACITY);
        pool.reserve(PROMOTION_WORK, now + Duration::from_secs(100))
            .unwrap();
        assert_eq!(pool.available, WORK_CAPACITY - PROMOTION_WORK);
        let later = now + Duration::from_secs(100);
        let mut budget = HeaderWorkBudget::new(PROMOTION_WORK);
        budget.consume(1234).unwrap();
        assert!(budget.consume(PROMOTION_WORK).is_err());
        pool.release(budget.remaining(), PROMOTION_WORK, later);
        assert_eq!(pool.available, WORK_CAPACITY - 1234);
        // Distinct consumers cannot each claim the same remaining allowance.
        pool.reserve(WORK_CAPACITY - 1234, later).unwrap();
        assert!(pool.reserve(1, later).is_err());
    }
    async fn assert_waiting<F: std::future::Future>(future: std::pin::Pin<&mut F>) {
        let mut future = future;
        std::future::poll_fn(|cx| {
            assert!(future.as_mut().poll(cx).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
    }

    #[tokio::test]
    async fn queued_work_cannot_be_bypassed_and_returned_work_wakes_the_head() {
        let scheduler = WorkScheduler::new();
        let held = scheduler.try_work(WORK_CAPACITY).unwrap();
        let mut first = Box::pin(scheduler.work(WORK_CAPACITY));
        assert_waiting(first.as_mut()).await;
        let mut second = Box::pin(scheduler.work(1));
        assert_waiting(second.as_mut()).await;
        assert!(scheduler.try_work(1).is_err());
        drop(held);
        // Even with the full balance returned, neither the second waiter nor
        // synchronous admission can steal the first waiter's queue position.
        assert_waiting(second.as_mut()).await;
        assert!(scheduler.try_work(1).is_err());
        let mut first = tokio::time::timeout(Duration::from_secs(5), first)
            .await
            .unwrap();
        first.budget.consume(1234).unwrap();
        drop(first);
        let second = tokio::time::timeout(Duration::from_secs(5), second)
            .await
            .unwrap();
        drop(second);
        assert!(scheduler.try_work(BATCH_WORK).is_ok());
    }

    #[tokio::test]
    async fn cancelling_head_and_queued_waiters_preserves_progress_and_credit() {
        let scheduler = WorkScheduler::new();
        let held = scheduler.try_work(WORK_CAPACITY).unwrap();
        let mut first = Box::pin(scheduler.work(WORK_CAPACITY));
        assert_waiting(first.as_mut()).await;
        let mut cancelled = Box::pin(scheduler.work(WORK_CAPACITY));
        assert_waiting(cancelled.as_mut()).await;
        let mut survivor = Box::pin(scheduler.work(WORK_CAPACITY));
        assert_waiting(survivor.as_mut()).await;
        drop(cancelled);
        drop(first);
        // No work was reserved by any cancelled future.
        drop(held);
        let survivor = tokio::time::timeout(Duration::from_secs(5), survivor)
            .await
            .unwrap();
        assert_eq!(survivor.budget.remaining(), WORK_CAPACITY);
        drop(survivor);
        assert!(scheduler.try_work(WORK_CAPACITY).is_ok());
    }
    #[tokio::test]
    async fn activation_cancels_a_standby_work_wait_without_leaking_queue_position() {
        let scheduler = WorkScheduler::new();
        let held = scheduler.try_work(WORK_CAPACITY).unwrap();
        let (activate, mut activation) = tokio::sync::oneshot::channel();
        let mut waiting = Box::pin(super::super::standby_header_work(
            &mut activation,
            scheduler.work(WORK_CAPACITY),
        ));
        assert_waiting(waiting.as_mut()).await;
        activate.send(()).unwrap();
        assert!(waiting.await.unwrap().is_none());
        drop(held);
        assert!(scheduler.try_work(WORK_CAPACITY).is_ok());

        let (activate, mut activation) = tokio::sync::oneshot::channel();
        let granted =
            super::super::standby_header_work(&mut activation, scheduler.work(BATCH_WORK))
                .await
                .unwrap();
        assert!(granted.is_some());
        drop(granted);
        drop(activate);
        assert!(
            super::super::standby_header_work(&mut activation, scheduler.work(BATCH_WORK))
                .await
                .is_err()
        );
        assert!(scheduler.try_work(WORK_CAPACITY).is_ok());
    }
}
