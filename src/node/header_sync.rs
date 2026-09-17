//! Node-owned single-slot disk candidate scheduling and shared validation work.
use super::{
    DeploymentConfig, HeaderDag, IDLE_SIDE_HEADER_TARGET, MAX_HEADERS_PER_RESPONSE, NetworkTime,
    PeerRunError, RedbHeaderStore, receive_headers, request_headers, resume_header_dag, unix_time,
    unseen_header_suffix,
};
use crate::{
    header_candidate::{DiskHeaderCandidate, HeaderCandidateError, HeaderCandidateLimits},
    headers::{HeaderBatchLimits, HeaderWorkBudget},
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

async fn resume_candidate(
    path: &std::path::Path,
    dag: &HeaderDag,
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
    loop {
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
    candidate: &mut Option<DiskHeaderCandidate>,
    path: &std::path::Path,
    dag: &mut HeaderDag,
    store: &RedbHeaderStore,
    now: u32,
    policy: HeaderSyncPolicy,
) -> Result<(), PeerRunError> {
    let Some(disk) = candidate.as_mut() else {
        return Ok(());
    };
    if disk.tip().chainwork <= dag.active_tip().chainwork && dag.get(&disk.tip().hash).is_none() {
        return Ok(());
    }
    let promoted = {
        let mut lease = work(PROMOTION_WORK).await;
        store
            .promote_candidate(dag, disk, now, policy.promotion_bytes, &mut lease.budget)
            .map_err(local)?
    };
    if promoted {
        // The redb commit precedes cleanup. On a crash here, replay sees the
        // already-retained candidate tip and can idempotently finish cleanup.
        drop(candidate.take());
        fs::remove_file(path).map_err(local)?;
    }
    Ok(())
}

pub(super) async fn sync_headers(
    session: &mut rbtc::p2p::PeerSession<tokio::net::TcpStream>,
    deployments: &DeploymentConfig,
    path: PathBuf,
    network_time: &NetworkTime,
    existing: Option<HeaderDag>,
) -> Result<HeaderDag, PeerRunError> {
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
    existing: Option<HeaderDag>,
    policy: HeaderSyncPolicy,
) -> Result<HeaderDag, PeerRunError> {
    let pending_path = candidate_path(&path);
    let store = RedbHeaderStore::open(path).map_err(local)?;
    let mut dag = resume_header_dag(&store, deployments, existing)?;
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
    let mut candidate = resume_candidate(&pending_path, &dag, now()?).await?;
    promote(
        &mut candidate,
        &pending_path,
        &mut dag,
        &store,
        now()?,
        policy,
    )
    .await?;
    let mut recovery_tip = store
        .recovery_tip()
        .map_err(local)?
        .filter(|hash| dag.get(hash).is_some())
        .unwrap_or(dag.active_tip().hash);
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
            lease
                .budget
                .consume(2 * dag.retained_header_count() as u64 + 128)
                .map_err(local)?;
            dag.block_locator_from(recovery_tip)
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
                    && dag.get(&response_tip).map(|info| info.height)
                        <= dag.get(&recovery_tip).map(|info| info.height))
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
            let stage = dag
                .stage_batch_contextual_with_budget(
                    unseen,
                    now()?,
                    HeaderBatchLimits::default(),
                    &mut lease.budget,
                )
                .map_err(|e| PeerRunError::header(&e))?;
            store
                .append_recovery_batch(unseen, response_tip)
                .map_err(local)?;
            let _ = stage.commit();
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
