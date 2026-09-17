//! Shared admission for header caches and registered database file lengths.
//! This excludes filesystem metadata and other node subsystems;
//! it is not a claim about aggregate physical disk usage or process RSS.
use fs2::FileExt;
use redb::{Database, DatabaseError, StorageBackend};
use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{self, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex, OnceLock, Weak},
};

mod inventory;

const CACHE_LIMIT: u64 = 512 * 1024 * 1024;
const FILE_LIMIT: u64 = 16 * 1024 * 1024 * 1024;

#[derive(Debug, Default)]
struct Usage {
    cache: u64,
    files: u64,
    paths: HashMap<PathBuf, u64>,
}
#[derive(Debug)]
struct Budget {
    usage: Mutex<Usage>,
    cache_limit: u64,
    file_limit: u64,
    directory: Option<PathBuf>,
    _owner: Option<File>,
}
impl Budget {
    fn reserve(&self, cache: u64, files: u64) -> io::Result<()> {
        let mut used = self
            .usage
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if cache > self.cache_limit.saturating_sub(used.cache)
            || files > self.file_limit.saturating_sub(used.files)
        {
            return Err(io::Error::other(
                "shared header storage byte allowance exhausted",
            ));
        }
        used.cache += cache;
        used.files += files;
        Ok(())
    }
    fn register(&self, path: &Path) -> io::Result<PathBuf> {
        let key = path
            .parent()
            .unwrap_or(Path::new("."))
            .canonicalize()?
            .join(
                path.file_name()
                    .ok_or_else(|| io::Error::other("missing header filename"))?,
            );
        let mut used = self
            .usage
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // A closed file remains charged. Only observed deletion retires it.
        let mut missing = Vec::new();
        for old in used.paths.keys() {
            if !old.try_exists()? {
                missing.push(old.clone());
            }
        }
        for old in missing {
            used.files -= used.paths.remove(&old).expect("registered path");
        }
        if !used.paths.contains_key(&key) {
            if used.paths.len() >= inventory::MAX_FILES {
                return Err(io::Error::other("header file inventory is full"));
            }
            let existing = inventory::file_len(&key)?.unwrap_or(0);
            let total = used
                .files
                .checked_add(existing)
                .ok_or_else(|| io::Error::other("header inventory length overflow"))?;
            used.paths.insert(key.clone(), existing);
            used.files = total;
            if let Some(directory) = &self.directory {
                if let Err(error) = inventory::save(directory, used.paths.keys()) {
                    used.paths.remove(&key);
                    used.files -= existing;
                    return Err(error);
                }
            }
        }
        Ok(key)
    }
    fn observe(&self, path: &Path, len: u64) -> io::Result<()> {
        let mut used = self
            .usage
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let old = *used
            .paths
            .get(path)
            .ok_or_else(|| io::Error::other("unregistered header file"))?;
        let total = (used.files - old)
            .checked_add(len)
            .ok_or_else(|| io::Error::other("header inventory length overflow"))?;
        // Existing oversized data must remain charged even when opening it is
        // refused. Otherwise a failed open would hide its disk occupancy.
        used.files = total;
        used.paths.insert(path.to_path_buf(), len);
        if total > self.file_limit {
            return Err(io::Error::other(
                "existing header inventory exceeds file allowance",
            ));
        }
        Ok(())
    }
    fn resize(&self, path: &Path, len: u64, shrink: bool) -> io::Result<()> {
        let mut used = self
            .usage
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let old = *used
            .paths
            .get(path)
            .ok_or_else(|| io::Error::other("unregistered header file"))?;
        if len > old {
            let growth = len - old;
            if growth > self.file_limit.saturating_sub(used.files) {
                return Err(io::Error::other(
                    "shared header file byte allowance exhausted",
                ));
            }
            used.files += growth;
            used.paths.insert(path.to_path_buf(), len);
        } else if shrink {
            used.files -= old - len;
            used.paths.insert(path.to_path_buf(), len);
        }
        Ok(())
    }
    fn release(&self, cache: u64, files: u64) {
        let mut used = self
            .usage
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        used.cache -= cache;
        used.files -= files;
    }
}
fn shared(parent: &Path) -> io::Result<Arc<Budget>> {
    static POOLS: OnceLock<Mutex<HashMap<PathBuf, Weak<Budget>>>> = OnceLock::new();
    let key = parent.canonicalize()?;
    let mut pools = POOLS
        .get_or_init(Mutex::default)
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    pools.retain(|_, pool| pool.strong_count() != 0);
    if let Some(pool) = pools.get(&key).and_then(Weak::upgrade) {
        return Ok(pool);
    }
    let owner = inventory::lock(&key)?;
    let paths = inventory::load(&key)?;
    let files = paths
        .values()
        .try_fold(0_u64, |sum, len| sum.checked_add(*len))
        .ok_or_else(|| io::Error::other("header inventory length overflow"))?;
    let pool = Arc::new(Budget {
        usage: Mutex::new(Usage {
            cache: 0,
            files,
            paths,
        }),
        cache_limit: CACHE_LIMIT,
        file_limit: FILE_LIMIT,
        directory: Some(key.clone()),
        _owner: Some(owner),
    });
    pools.insert(key, Arc::downgrade(&pool));
    Ok(pool)
}

#[derive(Debug)]
struct Backend {
    file: Mutex<File>,
    path: PathBuf,
    pool: Arc<Budget>,
    cache: u64,
}
impl Backend {
    fn open(path: &Path, fresh: bool, cache: u64, pool: Arc<Budget>) -> io::Result<Self> {
        // Reserve before opening a file or allocating an engine cache.
        pool.reserve(cache, 0)?;
        let opened = (|| {
            let _ = inventory::file_len(path)?;
            let key = pool.register(path)?;
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(!fresh)
                .create_new(fresh)
                .truncate(false)
                .open(path)?;
            file.try_lock_exclusive()?;
            let len = file.metadata()?.len();
            pool.observe(&key, len)?;
            Ok::<_, io::Error>((file, key))
        })();
        match opened {
            Ok((file, path)) => Ok(Self {
                file: Mutex::new(file),
                path,
                pool,
                cache,
            }),
            Err(error) => {
                pool.release(cache, 0);
                Err(error)
            }
        }
    }
}
impl Drop for Backend {
    fn drop(&mut self) {
        self.pool.release(self.cache, 0);
    }
}
struct ReadReservation<'a> {
    pool: &'a Budget,
    bytes: u64,
}
impl Drop for ReadReservation<'_> {
    fn drop(&mut self) {
        self.pool.release(self.bytes, 0);
    }
}
impl StorageBackend for Backend {
    fn len(&self) -> io::Result<u64> {
        Ok(self
            .file
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .metadata()?
            .len())
    }
    fn read(&self, offset: u64, len: usize) -> io::Result<Vec<u8>> {
        let mut file = self
            .file
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let file_len = file.metadata()?.len();
        if offset
            .checked_add(len as u64)
            .is_none_or(|end| end > file_len)
        {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "header database read outside file",
            ));
        }
        // Reserve the in-flight allocation before reading; once returned it is
        // owned by redb, whose configured cache is reserved separately.
        self.pool.reserve(len as u64, 0)?;
        let _read = ReadReservation {
            pool: &self.pool,
            bytes: len as u64,
        };
        let mut bytes = Vec::new();
        bytes.try_reserve_exact(len).map_err(io::Error::other)?;
        bytes.resize(len, 0);
        file.seek(SeekFrom::Start(offset))?;
        file.read_exact(&mut bytes)?;
        Ok(bytes)
    }
    fn set_len(&self, len: u64) -> io::Result<()> {
        let file = self
            .file
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.pool.resize(&self.path, len, false)?;
        // A failed resize remains charged until an observed successful shrink,
        // reopen under the file lock, or actual deletion.
        file.set_len(len)?;
        self.pool.resize(&self.path, len, true)?;
        Ok(())
    }
    fn sync_data(&self, _eventual: bool) -> io::Result<()> {
        self.file
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .sync_all()
    }
    fn write(&self, offset: u64, bytes: &[u8]) -> io::Result<()> {
        let mut file = self
            .file
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let len = file.metadata()?.len();
        if offset
            .checked_add(bytes.len() as u64)
            .is_none_or(|end| end > len)
        {
            return Err(io::Error::other(
                "header database write requires reserved file growth",
            ));
        }
        file.seek(SeekFrom::Start(offset))?;
        file.write_all(bytes)
    }
}

/// Reservation for a journal whose owner already holds its exclusive file lock.
pub(crate) struct RegisteredFile {
    pool: Arc<Budget>,
    path: PathBuf,
}
impl RegisteredFile {
    pub(crate) fn open(path: &Path, len: u64) -> io::Result<Self> {
        let _ = inventory::file_len(path)?;
        let directory = path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        let pool = shared(directory)?;
        let path = pool.register(path)?;
        pool.observe(&path, len)?;
        Ok(Self { pool, path })
    }
    pub(crate) fn reserve(&self, len: u64) -> io::Result<()> {
        self.pool.resize(&self.path, len, false)
    }
    pub(crate) fn truncated(&self, len: u64) -> io::Result<()> {
        self.pool.resize(&self.path, len, true)
    }
}

pub(crate) fn open(
    path: &Path,
    budget_directory: &Path,
    cache_bytes: usize,
    fresh: bool,
) -> Result<Database, DatabaseError> {
    let pool = shared(budget_directory)?;
    let backend = Backend::open(path, fresh, cache_bytes as u64, pool)?;
    Database::builder()
        .set_cache_size(cache_bytes)
        .create_with_backend(backend)
}

#[cfg(test)]
mod tests {
    use super::*;
    use redb::ReadableTableMetadata;
    fn budget(cache: u64, files: u64) -> Arc<Budget> {
        Arc::new(Budget {
            usage: Mutex::default(),
            cache_limit: cache,
            file_limit: files,
            directory: None,
            _owner: None,
        })
    }
    #[test]
    fn growth_and_writes_reserve_shared_bytes_before_mutation() {
        let root = tempfile::tempdir().unwrap();
        let pool = budget(16, 100);
        let left = Backend::open(&root.path().join("left"), true, 8, Arc::clone(&pool)).unwrap();
        let right = Backend::open(&root.path().join("right"), true, 8, Arc::clone(&pool)).unwrap();
        assert!(Backend::open(&root.path().join("denied"), true, 1, Arc::clone(&pool)).is_err());
        assert!(!root.path().join("denied").exists());
        left.set_len(60).unwrap();
        right.set_len(40).unwrap();
        assert!(right.set_len(41).is_err());
        assert_eq!(right.len().unwrap(), 40);
        assert!(
            right.read(0, 1).is_err(),
            "cache reservations leave no transient allowance"
        );
        assert_eq!(pool.usage.lock().unwrap().cache, 16);
        assert!(right.write(40, &[1]).is_err());
        left.set_len(59).unwrap();
        right.set_len(41).unwrap();
        drop(left);
        assert_eq!(pool.usage.lock().unwrap().files, 100);
        drop(right);
        assert_eq!(pool.usage.lock().unwrap().cache, 0);
        assert!(pool.usage.lock().unwrap().files > 0);
    }
    #[test]
    fn read_versions_keep_the_engine_reservation_after_writer_exit() {
        let root = tempfile::tempdir().unwrap();
        let pool = budget(2 * 1024 * 1024, 64 * 1024 * 1024);
        let backend = Backend::open(
            &root.path().join("db"),
            true,
            1024 * 1024,
            Arc::clone(&pool),
        )
        .unwrap();
        let db = Database::builder()
            .set_cache_size(1024 * 1024)
            .create_with_backend(backend)
            .unwrap();
        let read = db.begin_read().unwrap();
        drop(db);
        assert_eq!(pool.usage.lock().unwrap().cache, 1024 * 1024);
        assert!(pool.usage.lock().unwrap().files > 0);
        drop(read);
        assert_eq!(pool.usage.lock().unwrap().cache, 0);
        assert!(pool.usage.lock().unwrap().files > 0);
    }
    #[test]
    fn engine_growth_failure_reopens_at_the_last_committed_version() {
        const TABLE: redb::TableDefinition<u64, &[u8]> = redb::TableDefinition::new("budget_test");
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("db");
        let pool = budget(8 * 1024 * 1024, 4 * 1024 * 1024);
        let backend = Backend::open(&path, true, 1024 * 1024, Arc::clone(&pool)).unwrap();
        let db = Database::builder()
            .set_cache_size(1024 * 1024)
            .create_with_backend(backend)
            .unwrap();
        let transaction = db.begin_write().unwrap();
        transaction.open_table(TABLE).unwrap();
        transaction.commit().unwrap();
        let mut committed = 0_u64;
        let payload = vec![7; 64 * 1024];
        let mut failed = false;
        for key in 0..128 {
            let result = (|| -> Result<(), redb::Error> {
                let transaction = db.begin_write()?;
                transaction
                    .open_table(TABLE)?
                    .insert(key, payload.as_slice())?;
                transaction.commit()?;
                Ok(())
            })();
            if result.is_err() {
                failed = true;
                break;
            }
            committed += 1;
        }
        assert!(failed, "the engine must encounter the shared growth limit");
        assert!(path.metadata().unwrap().len() <= pool.file_limit);
        drop(db);
        let reopened = Backend::open(
            &path,
            false,
            1024 * 1024,
            budget(8 * 1024 * 1024, 16 * 1024 * 1024),
        )
        .unwrap();
        let reopened = Database::builder()
            .set_cache_size(1024 * 1024)
            .create_with_backend(reopened)
            .unwrap();
        let read = reopened.begin_read().unwrap();
        let table = read.open_table(TABLE).unwrap();
        assert_eq!(table.len().unwrap(), committed);
        for key in 0..committed {
            assert_eq!(table.get(key).unwrap().unwrap().value(), payload);
        }
    }
    #[test]
    fn closed_files_remain_charged_across_pool_recreation_until_deleted() {
        let root = tempfile::tempdir().unwrap();
        let root = root.path().canonicalize().unwrap();
        let path = root.join("custom-headers.redb");
        let pool = shared(&root).unwrap();
        let file = Backend::open(&path, true, 8, Arc::clone(&pool)).unwrap();
        file.set_len(60).unwrap();
        drop(file);
        assert_eq!(pool.usage.lock().unwrap().cache, 0);
        assert_eq!(pool.usage.lock().unwrap().files, 60);
        drop(pool);
        let reopened = shared(&root).unwrap();
        assert_eq!(reopened.usage.lock().unwrap().files, 60);
        let file = Backend::open(&path, false, 8, Arc::clone(&reopened)).unwrap();
        assert_eq!(
            reopened.usage.lock().unwrap().files,
            60,
            "reopen must not double charge"
        );
        drop(file);
        std::fs::remove_file(&path).unwrap();
        let next = Backend::open(&root.join("next"), true, 8, Arc::clone(&reopened)).unwrap();
        assert_eq!(reopened.usage.lock().unwrap().files, 0);
        drop(next);
    }
    #[test]
    fn candidate_journal_and_database_share_persistent_file_accounting() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().canonicalize().unwrap();
        let pool = shared(&root).unwrap();
        let database =
            Backend::open(&root.join("headers.redb"), true, 8, Arc::clone(&pool)).unwrap();
        database.set_len(60).unwrap();
        let source = crate::headers::HeaderDag::new(bitcoin::Network::Regtest);
        let journal_path = root.join("headers.candidate");
        let journal = crate::header_candidate::DiskHeaderCandidate::open(
            &journal_path,
            &source,
            source.active_tip().hash,
            u32::MAX,
            crate::header_candidate::HeaderCandidateLimits::default(),
            &mut crate::headers::HeaderWorkBudget::default(),
        )
        .unwrap();
        let length = journal_path.metadata().unwrap().len();
        assert_eq!(pool.usage.lock().unwrap().files, 60 + length);
        drop(journal);
        drop(database);
        drop(pool);
        let reopened = shared(&root).unwrap();
        assert_eq!(reopened.usage.lock().unwrap().files, 60 + length);
        std::fs::remove_file(journal_path).unwrap();
        let file =
            Backend::open(&root.join("headers.redb"), false, 8, Arc::clone(&reopened)).unwrap();
        assert_eq!(reopened.usage.lock().unwrap().files, 60);
        drop(file);
    }
    #[test]
    fn refused_oversized_existing_file_stays_in_the_inventory() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("existing");
        std::fs::write(&path, [0; 101]).unwrap();
        let pool = budget(16, 100);
        assert!(Backend::open(&path, false, 8, Arc::clone(&pool)).is_err());
        assert_eq!(pool.usage.lock().unwrap().files, 101);
        assert_eq!(pool.usage.lock().unwrap().cache, 0);
        assert!(Backend::open(&root.path().join("next"), true, 8, Arc::clone(&pool)).is_err());
        std::fs::remove_file(path).unwrap();
        let recovered =
            Backend::open(&root.path().join("next"), false, 8, Arc::clone(&pool)).unwrap();
        assert_eq!(pool.usage.lock().unwrap().files, 0);
        drop(recovered);
    }
}
