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
    sync::{Mutex, OnceLock},
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
fn work_pool() -> &'static Mutex<WorkPool> {
    static POOL: OnceLock<Mutex<WorkPool>> = OnceLock::new();
    POOL.get_or_init(|| {
        Mutex::new(WorkPool {
            available: WORK_CAPACITY,
            updated: Instant::now(),
        })
    })
}

pub(super) struct HeaderWorkLease {
    pub(super) budget: HeaderWorkBudget,
    reserved: u64,
}
impl Drop for HeaderWorkLease {
    fn drop(&mut self) {
        let mut pool = work_pool()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // Only unspent reservations return. Failed validation consumes work.
        pool.release(self.budget.remaining(), self.reserved, Instant::now());
    }
}
fn try_work(units: u64) -> Result<HeaderWorkLease, Duration> {
    work_pool()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .reserve(units, Instant::now())?;
    Ok(HeaderWorkLease {
        budget: HeaderWorkBudget::new(units),
        reserved: units,
    })
}
pub(super) fn try_header_work() -> Result<HeaderWorkLease, Duration> {
    try_work(BATCH_WORK)
}
pub(super) async fn work(units: u64) -> HeaderWorkLease {
    loop {
        match try_work(units) {
            Ok(lease) => return lease,
            Err(delay) => tokio::time::sleep(delay).await,
        }
    }
}

#[derive(Clone, Copy)]
pub(super) struct HeaderSyncPolicy {
    pub(super) spill_side_headers: usize,
    pub(super) promotion_bytes: usize,
}
impl Default for HeaderSyncPolicy {
    fn default() -> Self {
        Self {
            spill_side_headers: IDLE_SIDE_HEADER_TARGET,
            promotion_bytes: 8 * 1024 * 1024,
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
struct ReplayKeepalive(Instant);
impl ReplayKeepalive {
    fn new() -> Self {
        Self(Instant::now())
    }
    async fn tick(
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
                    PeerRunError::transient("peer keepalive timed out during candidate replay")
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
    let mut recovery = {
        let mut lease = work(BATCH_WORK).await;
        DiskHeaderCandidate::start_recovery(
            path,
            dag,
            anchor,
            HeaderCandidateLimits::default(),
            &mut lease.budget,
        )
        .map_err(local)?
    };
    let mut keepalive = ReplayKeepalive::new();
    loop {
        keepalive.tick(session.as_deref_mut()).await?;
        let done = {
            let mut lease = work(BATCH_WORK).await;
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
            let mut keepalive = ReplayKeepalive::new();
            loop {
                keepalive.tick(session.as_deref_mut()).await?;
                {
                    let mut lease = work(BATCH_WORK).await;
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
        if candidate.is_none()
            && unseen[0].prev_blockhash != dag.active_tip().hash
            && side >= policy.spill_side_headers
        {
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
        }
        recovery_tip = response_tip;
        recovering = true;
        if response_count < MAX_HEADERS_PER_RESPONSE {
            break;
        }
    }
    store.clear_recovery_tip().map_err(local)?;
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
}
