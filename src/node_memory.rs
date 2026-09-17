//! Shared reservations for explicitly accounted node allocations.
//!
//! This bounds reservations, not RSS: engine dirty pages, allocator overhead and
//! allocations without a lease still need separate integration and measurement.
use redb::{Database, DatabaseError, StorageBackend, backends::FileBackend};
use std::{
    collections::HashMap,
    fs::OpenOptions,
    io,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, OnceLock, Weak},
};

/// Default aggregate reservation allowance (32 GiB).
pub const DEFAULT_MEMORY_BUDGET_BYTES: u64 = 32 * 1024 * 1024 * 1024;

/// Aggregate logical byte allowance for ephemeral execution results (16 GiB).
pub const DEFAULT_EXECUTION_SPOOL_BYTES: u64 = 16 * 1024 * 1024 * 1024;

/// Cache configured by redb 2.6 for stores without a caller-selected cache.
pub(crate) const DEFAULT_REDB_CACHE_BYTES: usize = 1024 * 1024 * 1024;

pub(crate) fn create_redb(path: impl AsRef<Path>) -> Result<Database, DatabaseError> {
    open_redb(path.as_ref(), DEFAULT_REDB_CACHE_BYTES)
}

#[derive(Debug, Default)]
struct Usage {
    used: u64,
    peak: u64,
}
#[derive(Debug)]
struct Shared {
    limit: u64,
    usage: Mutex<Usage>,
    spool_usage: Mutex<Usage>,
}

/// One node's memory ledger. Clones share the same allowance.
#[derive(Clone, Debug)]
pub struct MemoryBudget(Arc<Shared>);

/// Observable aggregate reservations; these values are not process RSS.
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize)]
pub struct MemorySnapshot {
    /// Configured reservation ceiling.
    pub limit: u64,
    /// Bytes held by live owners.
    pub used: u64,
    /// Highest simultaneous reservation.
    pub peak: u64,
}
impl MemoryBudget {
    /// Creates an independent allowance, including zero for fail-closed callers.
    #[must_use]
    pub fn new(limit: u64) -> Self {
        Self(Arc::new(Shared {
            limit,
            usage: Mutex::default(),
            spool_usage: Mutex::default(),
        }))
    }
    /// Reserves before allocation; the returned lease follows its actual owner.
    pub fn reserve(&self, bytes: u64) -> io::Result<MemoryLease> {
        let mut usage = self
            .0
            .usage
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if bytes > self.0.limit.saturating_sub(usage.used) {
            return Err(io::Error::other(
                "node memory reservation allowance exhausted",
            ));
        }
        usage.used += bytes;
        usage.peak = usage.peak.max(usage.used);
        Ok(MemoryLease {
            budget: self.clone(),
            bytes,
        })
    }
    pub(crate) fn reserve_spool(&self, bytes: u64) -> io::Result<SpoolLease> {
        let mut usage = self
            .0
            .spool_usage
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // Shared by active/background pipelines. This is an ephemeral logical
        // byte limit, not a physical quota for all node files.
        if bytes > DEFAULT_EXECUTION_SPOOL_BYTES.saturating_sub(usage.used) {
            return Err(io::Error::other("execution spool disk allowance exhausted"));
        }
        usage.used += bytes;
        usage.peak = usage.peak.max(usage.used);
        Ok(SpoolLease {
            budget: self.clone(),
            bytes,
        })
    }

    /// Logical temporary execution bytes; independent of the memory ledger.
    #[must_use]
    pub fn spool_snapshot(&self) -> MemorySnapshot {
        let usage = self
            .0
            .spool_usage
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        MemorySnapshot {
            limit: DEFAULT_EXECUTION_SPOOL_BYTES,
            used: usage.used,
            peak: usage.peak,
        }
    }

    /// Reads usage without resetting counters.
    #[must_use]
    pub fn snapshot(&self) -> MemorySnapshot {
        let usage = self
            .0
            .usage
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        MemorySnapshot {
            limit: self.0.limit,
            used: usage.used,
            peak: usage.peak,
        }
    }

    /// Binds all runtime directories atomically. Escaped database views keep the
    /// ledger alive and prevent a later runtime from resetting its allowance.
    pub(crate) fn bind(&self, roots: &[PathBuf]) -> io::Result<()> {
        let roots = roots
            .iter()
            .map(|root| canonical(root))
            .collect::<io::Result<Vec<_>>>()?;
        let mut registry = registry()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        registry.retain(|_, owner| owner.strong_count() != 0);
        for root in &roots {
            for (existing, owner) in registry.iter() {
                if (root.starts_with(existing) || existing.starts_with(root))
                    && owner
                        .upgrade()
                        .is_some_and(|owner| !Arc::ptr_eq(&owner, &self.0))
                {
                    return Err(io::Error::other(
                        "node memory directory already belongs to a live runtime",
                    ));
                }
            }
        }
        for root in roots {
            registry.insert(root, Arc::downgrade(&self.0));
        }
        Ok(())
    }
}

/// Non-cloneable reservation released on error, cancellation or final owner drop.
#[derive(Debug)]
pub struct MemoryLease {
    budget: MemoryBudget,
    bytes: u64,
}
impl MemoryLease {
    pub(crate) fn shares_owner(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.budget.0, &other.budget.0)
    }

    pub(crate) fn shrink_to(&mut self, bytes: u64) -> io::Result<()> {
        if bytes > self.bytes {
            return Err(io::Error::other("memory lease cannot grow by shrinking"));
        }
        let mut usage = self
            .budget
            .0
            .usage
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        usage.used -= self.bytes - bytes;
        self.bytes = bytes;
        Ok(())
    }

    pub(crate) fn duplicate(&self) -> io::Result<Self> {
        self.budget.reserve(self.bytes)
    }

    pub(crate) fn reserve_additional(&self, bytes: u64) -> io::Result<Self> {
        self.budget.reserve(bytes)
    }
}
impl Drop for MemoryLease {
    fn drop(&mut self) {
        self.budget
            .0
            .usage
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .used -= self.bytes;
    }
}

pub(crate) struct SpoolLease {
    budget: MemoryBudget,
    bytes: u64,
}
impl Drop for SpoolLease {
    fn drop(&mut self) {
        self.budget
            .0
            .spool_usage
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .used -= self.bytes;
    }
}

type Registry = HashMap<PathBuf, Weak<Shared>>;
fn registry() -> &'static Mutex<Registry> {
    static REGISTRY: OnceLock<Mutex<Registry>> = OnceLock::new();
    REGISTRY.get_or_init(Mutex::default)
}
// Resolve existing symlinks while allowing not-yet-created output directories.
fn canonical(path: &Path) -> io::Result<PathBuf> {
    match path.canonicalize() {
        Ok(path) => Ok(path),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            let name = path.file_name().ok_or(error)?;
            let parent = path
                .parent()
                .filter(|p| !p.as_os_str().is_empty())
                .unwrap_or(Path::new("."));
            Ok(canonical(parent)?.join(name))
        }
        Err(error) => Err(error),
    }
}
pub(crate) fn for_path(path: &Path) -> io::Result<Option<MemoryBudget>> {
    let path = canonical(path)?;
    let registry = registry()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    Ok(registry.iter().find_map(|(root, owner)| {
        path.starts_with(root)
            .then(|| owner.upgrade().map(MemoryBudget))
            .flatten()
    }))
}

#[derive(Debug)]
struct Backend {
    file: FileBackend,
    cache: MemoryLease,
}
impl StorageBackend for Backend {
    fn len(&self) -> io::Result<u64> {
        self.file.len()
    }
    fn read(&self, offset: u64, len: usize) -> io::Result<Vec<u8>> {
        let _read = self.cache.reserve_additional(len as u64)?;
        self.file.read(offset, len)
    }
    fn set_len(&self, len: u64) -> io::Result<()> {
        self.file.set_len(len)
    }
    fn sync_data(&self, eventual: bool) -> io::Result<()> {
        self.file.sync_data(eventual)
    }
    fn write(&self, offset: u64, data: &[u8]) -> io::Result<()> {
        self.file.write(offset, data)
    }
}

pub(crate) fn open_existing_redb(path: impl AsRef<Path>) -> Result<Database, DatabaseError> {
    redb_with_mode(path.as_ref(), DEFAULT_REDB_CACHE_BYTES, false)
}

pub(crate) fn open_redb(path: &Path, cache_bytes: usize) -> Result<Database, DatabaseError> {
    redb_with_mode(path, cache_bytes, true)
}

fn redb_with_mode(
    path: &Path,
    cache_bytes: usize,
    create: bool,
) -> Result<Database, DatabaseError> {
    let mut builder = Database::builder();
    builder.set_cache_size(cache_bytes);
    let Some(budget) = for_path(path)? else {
        return if create {
            builder.create(path)
        } else {
            builder.open(path)
        };
    };
    // Reserve before creating the file or engine. FileBackend preserves redb's
    // platform-specific locking and I/O, and owns the lease until engine drop.
    let cache = budget.reserve(cache_bytes as u64)?;
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(create)
        .truncate(false)
        .open(path)?;
    let file = FileBackend::new(file)?;
    // redb 2.6 only distinguishes open from create when the locked file is
    // empty (page_manager.rs). Check after acquiring the same engine lock.
    if !create && file.len()? == 0 {
        return Err(io::Error::from(io::ErrorKind::InvalidData).into());
    }
    builder.create_with_backend(Backend { file, cache })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shrinking_lease_refunds_only_released_capacity() {
        let budget = MemoryBudget::new(100);
        let mut lease = budget.reserve(80).unwrap();
        assert!(lease.shrink_to(81).is_err());
        assert_eq!(budget.snapshot().used, 80);
        lease.shrink_to(30).unwrap();
        let copy = lease.duplicate().unwrap();
        assert_eq!(budget.snapshot().used, 60);
        lease.shrink_to(0).unwrap();
        assert_eq!(budget.snapshot().used, 30);
        drop(copy);
        drop(lease);
        assert_eq!(budget.snapshot().used, 0);
        assert_eq!(budget.snapshot().peak, 80);
    }

    #[test]
    fn shared_reservations_do_not_overflow_and_release_on_unwind() {
        let budget = MemoryBudget::new(u64::MAX);
        let first = budget.reserve(u64::MAX - 1).unwrap();
        assert!(budget.clone().reserve(2).is_err());
        let _ = std::panic::catch_unwind(|| {
            let _last = budget.reserve(1).unwrap();
            panic!("cancel");
        });
        assert_eq!(budget.snapshot().used, u64::MAX - 1);
        drop(first);
        assert_eq!(budget.snapshot().used, 0);
        assert_eq!(budget.snapshot().peak, u64::MAX);
    }

    #[test]
    fn directory_binding_is_atomic_and_survives_escaped_leases() {
        let dir = tempfile::tempdir().unwrap();
        let one = dir.path().join("one");
        let two = dir.path().join("two");
        let owner = MemoryBudget::new(100);
        owner.bind(std::slice::from_ref(&one)).unwrap();
        let other = MemoryBudget::new(100);
        assert!(other.bind(&[two.clone(), one.clone()]).is_err());
        assert!(for_path(&two.join("db")).unwrap().is_none());
        let lease = owner.reserve(10).unwrap();
        drop(owner);
        assert!(other.bind(std::slice::from_ref(&one)).is_err());
        drop(lease);
        other.bind(&[one.clone(), two]).unwrap();
        assert_eq!(
            for_path(&one.join("db")).unwrap().unwrap().snapshot().limit,
            100
        );
    }

    #[test]
    fn engine_cache_stays_reserved_until_read_transaction_drops() {
        let dir = tempfile::tempdir().unwrap();
        let budget = MemoryBudget::new(3 * 1024 * 1024);
        budget.bind(&[dir.path().to_owned()]).unwrap();
        let first = open_redb(&dir.path().join("first.redb"), 2 * 1024 * 1024).unwrap();
        let read = first.begin_read().unwrap();
        drop(first);
        assert!(open_redb(&dir.path().join("second.redb"), 2 * 1024 * 1024).is_err());
        assert!(!dir.path().join("second.redb").exists());
        drop(read);
        assert_eq!(budget.snapshot().used, 0);
        let second = open_redb(&dir.path().join("second.redb"), 2 * 1024 * 1024).unwrap();
        drop(second);
        assert_eq!(budget.snapshot().used, 0);
    }
    #[test]
    fn headers_chainstate_and_candidates_share_one_allowance() {
        use crate::admission_resources::{AdmissionBudget, AdmissionResourceLimits};
        let dir = tempfile::tempdir().unwrap();
        let budget = MemoryBudget::new(8 * 1024 * 1024);
        budget.bind(&[dir.path().to_owned()]).unwrap();
        let chain = open_redb(&dir.path().join("chain.redb"), 2 * 1024 * 1024).unwrap();
        let header = crate::header_storage_budget::open(
            &dir.path().join("headers.redb"),
            dir.path(),
            2 * 1024 * 1024,
            false,
        )
        .unwrap();
        let admission =
            AdmissionBudget::with_memory(AdmissionResourceLimits::default(), Some(budget.clone()));
        assert_eq!(budget.snapshot().used, 4 * 1024 * 1024);
        let candidate = admission.reserve_candidate(4 * 1024 * 1024).unwrap();
        assert!(admission.reserve_candidate(1).is_err());
        assert_eq!(admission.snapshot().candidate_bytes, 4 * 1024 * 1024);
        drop(candidate);
        drop(chain);
        assert_eq!(budget.snapshot().used, 2 * 1024 * 1024);
        drop(header);
        assert_eq!(budget.snapshot().used, 0);
    }

    #[test]
    fn failed_database_open_refunds_aggregate_reservation() {
        let dir = tempfile::tempdir().unwrap();
        let budget = MemoryBudget::new(4 * 1024 * 1024);
        budget.bind(&[dir.path().to_owned()]).unwrap();
        assert!(open_redb(&dir.path().join("missing/chain.redb"), 2 * 1024 * 1024).is_err());
        assert_eq!(budget.snapshot().used, 0);
        let path = dir.path().join("locked.redb");
        let first = open_redb(&path, 1024 * 1024).unwrap();
        assert!(open_redb(&path, 1024 * 1024).is_err());
        assert_eq!(budget.snapshot().used, 1024 * 1024);
        drop(first);
        assert_eq!(budget.snapshot().used, 0);
    }
    #[test]
    fn existing_open_never_creates_or_initializes_files_and_refunds_failures() {
        let dir = tempfile::tempdir().unwrap();
        let budget = MemoryBudget::new(3 * DEFAULT_REDB_CACHE_BYTES as u64);
        budget.bind(&[dir.path().to_owned()]).unwrap();
        let path = dir.path().join("existing.redb");
        assert!(open_existing_redb(&path).is_err());
        assert!(!path.exists());
        std::fs::write(&path, []).unwrap();
        assert!(open_existing_redb(&path).is_err());
        assert_eq!(std::fs::metadata(&path).unwrap().len(), 0);
        assert_eq!(budget.snapshot().used, 0);
        drop(create_redb(&path).unwrap());
        let db = open_existing_redb(&path).unwrap();
        assert_eq!(budget.snapshot().used, DEFAULT_REDB_CACHE_BYTES as u64);
        drop(db);
        assert_eq!(budget.snapshot().used, 0);
    }

    #[test]
    fn supporting_stores_compete_and_fail_before_file_creation() {
        let dir = tempfile::tempdir().unwrap();
        let budget = MemoryBudget::new(DEFAULT_REDB_CACHE_BYTES as u64 + 1024 * 1024);
        budget.bind(&[dir.path().to_owned()]).unwrap();
        let peers = crate::peer_store::RedbPeerStore::open(
            dir.path().join("peers.redb"),
            bitcoin::Network::Regtest,
        )
        .unwrap();
        let path = dir.path().join("fees.redb");
        assert!(
            crate::fee_estimator::RedbFeeEstimator::open(&path, bitcoin::Network::Regtest).is_err()
        );
        assert!(!path.exists());
        drop(peers);
        let fees =
            crate::fee_estimator::RedbFeeEstimator::open(&path, bitcoin::Network::Regtest).unwrap();
        assert_eq!(budget.snapshot().used, DEFAULT_REDB_CACHE_BYTES as u64);
        drop(fees);
        assert_eq!(budget.snapshot().used, 0);
    }
}
