//! MDBX dirty-page policy and runtime reservation ownership.
//!
//! The engine limit triggers spilling; it is not a process RSS ceiling. Mapped
//! pages, engine metadata, large overflow values and copy buffers remain outside
//! this explicitly accounted allowance.
use crate::{
    node_memory::{MemoryBudget, MemoryLease},
    utxo::UtxoError,
};
use libmdbx::{Database, DatabaseOptions, Mode, NoWriteMap, ReadWriteOptions, SyncMode};
use std::{fs, ops::Deref, path::Path};

// Fixed across hosts: avoid MDBX's default fraction of system/available RAM.
const DIRTY_PAGES: u64 = 4096;
const RESERVE_PAGES: u64 = 64;
// MDBX supports pages up to 64 KiB; existing environments retain their page size.
const MAX_PAGE_BYTES: u64 = 64 * 1024;
pub(crate) const RESERVATION_BYTES: u64 = (DIRTY_PAGES + RESERVE_PAGES) * MAX_PAGE_BYTES;

#[derive(Clone)]
pub(crate) struct Allowance {
    _reservation: Option<std::sync::Arc<MemoryLease>>,
    memory: Option<MemoryBudget>,
}
impl Allowance {
    pub(crate) fn reserve(memory: Option<MemoryBudget>) -> Result<Self, UtxoError> {
        let reservation = memory
            .as_ref()
            .map(|budget| budget.reserve(RESERVATION_BYTES))
            .transpose()?;
        Ok(Self {
            _reservation: reservation.map(std::sync::Arc::new),
            memory,
        })
    }
}

pub(crate) struct Environment {
    // Close the engine before returning its reservation.
    db: Database<NoWriteMap>,
    allowance: Allowance,
}
impl Deref for Environment {
    type Target = Database<NoWriteMap>;
    fn deref(&self) -> &Self::Target {
        &self.db
    }
}
impl Environment {
    pub(crate) fn memory(&self) -> Option<MemoryBudget> {
        self.allowance.memory.clone()
    }

    pub(crate) fn allowance(&self) -> Allowance {
        self.allowance.clone()
    }

    pub(crate) fn open(
        path: &Path,
        capacity_bytes: u64,
        memory: Option<MemoryBudget>,
    ) -> Result<Self, UtxoError> {
        Self::open_reserved(path, capacity_bytes, Allowance::reserve(memory)?)
    }

    pub(crate) fn open_reserved(
        path: &Path,
        capacity_bytes: u64,
        allowance: Allowance,
    ) -> Result<Self, UtxoError> {
        let capacity = isize::try_from(capacity_bytes)
            .map_err(|_| UtxoError::Malformed("MDBX capacity exceeds platform limit"))?;
        // Admission precedes directory creation and engine allocation.
        fs::create_dir_all(path)?;
        let db = Database::open_with_options(
            path,
            DatabaseOptions {
                max_tables: Some(4),
                txn_dp_limit: Some(DIRTY_PAGES),
                dp_reserve_limit: Some(RESERVE_PAGES),
                // Do not let host-RAM heuristics populate a speculative mapped
                // working set outside the node's explicit resource planning.
                // Demand-faulted pages still require RSS acceptance.
                no_rdahead: true,
                mode: Mode::ReadWrite(ReadWriteOptions {
                    sync_mode: SyncMode::Durable,
                    max_size: Some(capacity),
                    ..ReadWriteOptions::default()
                }),
                ..DatabaseOptions::default()
            },
        )?;
        if u64::from(db.stat()?.page_size()) > MAX_PAGE_BYTES {
            return Err(UtxoError::Malformed(
                "MDBX page size exceeds reserved policy",
            ));
        }
        Ok(Self { db, allowance })
    }
}

pub(crate) fn open(path: &Path, capacity_bytes: u64) -> Result<Environment, UtxoError> {
    Environment::open(path, capacity_bytes, crate::node_memory::for_path(path)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sibling_environment_cannot_reset_the_node_allowance() {
        let root = tempfile::tempdir().unwrap();
        let budget = MemoryBudget::new(RESERVATION_BYTES);
        let first_path = root.path().join("active");
        budget.bind(std::slice::from_ref(&first_path)).unwrap();
        let first = open(&first_path, 64 * 1024 * 1024).unwrap();
        assert_eq!(budget.snapshot().used, RESERVATION_BYTES);
        assert!(first.info().unwrap().read_ahead_disabled());
        let sibling = root.path().join("active.compact");
        let owner = first.memory();
        assert!(Environment::open(&sibling, 64 * 1024 * 1024, owner.clone()).is_err());
        assert!(!sibling.exists());
        drop(first);
        let second = Environment::open(&sibling, 64 * 1024 * 1024, owner.clone()).unwrap();
        assert!(second.info().unwrap().read_ahead_disabled());
        assert_eq!(budget.snapshot().used, RESERVATION_BYTES);
        drop(second);
        assert_eq!(budget.snapshot().used, 0);
        let reopened = Environment::open(&first_path, 64 * 1024 * 1024, owner).unwrap();
        assert!(reopened.info().unwrap().read_ahead_disabled());
        drop(reopened);
        assert_eq!(budget.snapshot().used, 0);
    }
    #[test]
    fn compact_copy_admission_precedes_artifacts_and_reopen_keeps_ownership() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("active");
        let budget = MemoryBudget::new(2 * RESERVATION_BYTES);
        budget.bind(std::slice::from_ref(&path)).unwrap();
        let mut store =
            crate::mdbx_utxo::MdbxUtxoStore::open_with_capacity(&path, 64 * 1024 * 1024).unwrap();
        let before = store.audit().unwrap();
        let competing = budget.reserve(RESERVATION_BYTES).unwrap();
        assert!(store.compact_with_reserve(0).is_err());
        assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 1);
        assert_eq!(store.audit().unwrap().content_sha256, before.content_sha256);
        drop(competing);
        let mut concurrent = None;
        store
            .compact_with_phase_hook(|phase| {
                if phase == crate::mdbx_utxo::MdbxCompactionPhase::SourceRenamed {
                    // The source engine is closed here. Its reservation must stay
                    // pinned so another candidate cannot steal its reopen capacity.
                    assert_eq!(budget.snapshot().used, RESERVATION_BYTES);
                    concurrent = Some(budget.reserve(RESERVATION_BYTES).unwrap());
                }
            })
            .unwrap();
        assert_eq!(budget.snapshot().used, 2 * RESERVATION_BYTES);
        drop(concurrent);
        assert_eq!(budget.snapshot().used, RESERVATION_BYTES);
        assert_eq!(store.audit().unwrap().content_sha256, before.content_sha256);
        drop(store);
        assert_eq!(budget.snapshot().used, 0);
    }
}
