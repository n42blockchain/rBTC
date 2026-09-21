//! Node ownership of a bounded derived index and its last committed read view.
use super::{
    DeploymentConfig, HeaderInfo, HeaderReadError, HeaderSnapshot, HeaderView, PeerRunError,
    RedbHeaderStore, header_sync,
};
use crate::{
    header_index::{DiskHeaderIndex, DiskHeaderView, HeaderIndexError},
    headers::HeaderWorkBudget,
};
use bitcoin::{BlockHash, block::Header};
use std::{
    path::{Path, PathBuf},
    time::Instant,
};

type Session = crate::p2p::PeerSession<tokio::net::TcpStream>;
fn local(error: impl std::fmt::Display) -> PeerRunError {
    PeerRunError::local(error.to_string())
}
fn validation(error: HeaderIndexError) -> PeerRunError {
    match error {
        HeaderIndexError::Header(error) => PeerRunError::header(&error),
        other => local(other),
    }
}

pub(super) struct NodeHeaderState {
    index: DiskHeaderIndex,
    view: DiskHeaderView,
    path: PathBuf,
}
impl NodeHeaderState {
    #[cfg(test)]
    pub(super) fn test_seed(dag: crate::headers::HeaderDag, path: &Path) -> Self {
        let mut index =
            DiskHeaderIndex::create_scratch(path.parent().unwrap(), dag.deployments().clone())
                .unwrap();
        for batch in dag.test_replay_headers().chunks(2_000) {
            index
                .append(batch, u32::MAX, &mut HeaderWorkBudget::default())
                .unwrap();
        }
        let view = index.snapshot().unwrap();
        assert_eq!(view.active_tip(), dag.active_tip());
        drop(dag);
        Self {
            index,
            view,
            path: path.to_path_buf(),
        }
    }

    pub(super) async fn resume(
        store: &RedbHeaderStore,
        path: &Path,
        deployments: &DeploymentConfig,
        now: u32,
        existing: Option<Self>,
        mut session: Option<&mut Session>,
    ) -> Result<Self, PeerRunError> {
        if let Some(state) = existing {
            if state.path != path
                || state.view.deployments() != deployments
                || state.index.len() != store.len().map_err(local)?
            {
                return Err(local(
                    "retained header index does not match path, count or deployment configuration",
                ));
            }
            return Ok(state);
        }
        let mut index = DiskHeaderIndex::create_scratch(
            path.parent()
                .filter(|p| !p.as_os_str().is_empty())
                .unwrap_or(Path::new(".")),
            deployments.clone(),
        )
        .map_err(local)?;
        let mut reader = store.replay_reader().map_err(local)?;
        let mut keepalive = header_sync::ReplayKeepalive::new(Instant::now());
        loop {
            {
                let mut lease = keepalive
                    .wait(
                        session.as_deref_mut(),
                        header_sync::work(header_sync::BATCH_WORK),
                    )
                    .await?;
                let Some(batch) = reader.next_batch(&mut lease.budget).map_err(local)? else {
                    break;
                };
                index
                    .append(&batch, now, &mut lease.budget)
                    .map_err(local)?;
            }
            tokio::task::yield_now().await;
        }
        let view = index.snapshot().map_err(local)?;
        Ok(Self {
            index,
            view,
            path: path.to_path_buf(),
        })
    }

    pub(super) fn shared_disk_view(&self) -> std::sync::Arc<DiskHeaderView> {
        std::sync::Arc::new(self.view.clone())
    }

    pub(super) fn retained_header_count(&self) -> usize {
        usize::try_from(self.index.len())
            .unwrap_or(usize::MAX)
            .saturating_add(1)
    }
    pub(super) fn published(&self) -> HeaderSnapshot {
        self.view.clone().into()
    }

    pub(super) fn append(
        &mut self,
        store: &RedbHeaderStore,
        batch: &[Header],
        now: u32,
        work: &mut HeaderWorkBudget,
        recovery: bool,
    ) -> Result<(), PeerRunError> {
        if batch.is_empty() {
            return Ok(());
        }
        let stage = self.index.stage(batch, now, work).map_err(validation)?;
        if recovery {
            store
                .append_recovery_batch(batch, batch.last().expect("nonempty").block_hash())
                .map_err(local)?;
        } else {
            store.append_batch(batch).map_err(local)?;
        }
        stage.commit().map_err(local)?;
        self.view = self.index.snapshot().map_err(local)?;
        Ok(())
    }

    pub(super) fn retain_idle(
        &mut self,
        execution_tip: BlockHash,
        side_target: usize,
        max_removals: usize,
    ) -> Result<usize, PeerRunError> {
        if self.active_tip().hash != execution_tip
            || header_sync::candidate_path(&self.path)
                .try_exists()
                .map_err(local)?
        {
            return Ok(0);
        }
        if self
            .index
            .len()
            .saturating_sub(u64::from(self.active_tip().height))
            <= side_target as u64
        {
            return Ok(0);
        }
        let store = RedbHeaderStore::open(&self.path).map_err(local)?;
        if store.pending_candidate_tip().map_err(local)?.is_some() {
            return Err(local("unfinished candidate promotion"));
        }
        let mut pins = vec![execution_tip];
        if let Some(cursor) = store.recovery_tip().map_err(local)? {
            pins.push(cursor);
        }
        let Ok(mut lease) = header_sync::try_header_work() else {
            return Ok(0);
        };
        let stage = self
            .index
            .stage_eviction(side_target, max_removals, &pins, &mut lease.budget)
            .map_err(local)?;
        let removed = stage.evicted().len();
        store
            .persist_eviction_records(stage.evicted())
            .map_err(local)?;
        stage.commit().map_err(local)?;
        self.view = self.index.snapshot().map_err(local)?;
        Ok(removed)
    }

    pub(super) fn retain_ingress(
        &mut self,
        store: &RedbHeaderStore,
        side_target: usize,
        pins: &[BlockHash],
        work: &mut HeaderWorkBudget,
    ) -> Result<usize, PeerRunError> {
        let stage = self
            .index
            .stage_eviction(side_target, usize::MAX, pins, work)
            .map_err(local)?;
        let removed = stage.evicted().len();
        store
            .persist_eviction_records(stage.evicted())
            .map_err(local)?;
        stage.commit().map_err(local)?;
        self.view = self.index.snapshot().map_err(local)?;
        Ok(removed)
    }
}
impl HeaderView for NodeHeaderState {
    fn deployments(&self) -> &DeploymentConfig {
        self.view.deployments()
    }
    fn active_tip(&self) -> HeaderInfo {
        self.view.active_tip()
    }
    fn header(&self, hash: &BlockHash) -> Result<Option<HeaderInfo>, HeaderReadError> {
        self.view.header(hash)
    }
    fn active_header(&self, height: u32) -> Result<Option<HeaderInfo>, HeaderReadError> {
        self.view.active_header(height)
    }
    fn ancestor(&self, tip: BlockHash, height: u32) -> Result<Option<HeaderInfo>, HeaderReadError> {
        self.view.ancestor(tip, height)
    }
    fn branch_locator(&self, tip: BlockHash) -> Result<Option<Vec<BlockHash>>, HeaderReadError> {
        self.view.branch_locator(tip)
    }
}
