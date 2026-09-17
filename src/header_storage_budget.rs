//! Shared admission for live header database caches and logical file lengths.
//! This excludes closed files, filesystem metadata and other node subsystems;
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

const CACHE_LIMIT: u64 = 512 * 1024 * 1024;
const FILE_LIMIT: u64 = 16 * 1024 * 1024 * 1024;

#[derive(Debug, Default)]
struct Usage {
    cache: u64,
    files: u64,
}
#[derive(Debug)]
struct Budget {
    usage: Mutex<Usage>,
    cache_limit: u64,
    file_limit: u64,
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
    let pool = Arc::new(Budget {
        usage: Mutex::default(),
        cache_limit: CACHE_LIMIT,
        file_limit: FILE_LIMIT,
    });
    pools.insert(key, Arc::downgrade(&pool));
    Ok(pool)
}

#[derive(Debug)]
struct Backend {
    file: Mutex<File>,
    reserved_len: Mutex<u64>,
    pool: Arc<Budget>,
    cache: u64,
}
impl Backend {
    fn open(path: &Path, fresh: bool, cache: u64, pool: Arc<Budget>) -> io::Result<Self> {
        // Reserve before opening a file or allocating an engine cache.
        pool.reserve(cache, 0)?;
        let opened = (|| {
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(!fresh)
                .create_new(fresh)
                .truncate(false)
                .open(path)?;
            file.try_lock_exclusive()?;
            let len = file.metadata()?.len();
            pool.reserve(0, len)?;
            Ok::<_, io::Error>((file, len))
        })();
        match opened {
            Ok((file, len)) => Ok(Self {
                file: Mutex::new(file),
                reserved_len: Mutex::new(len),
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
        let len = *self
            .reserved_len
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.pool.release(self.cache, len);
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
        let mut reserved = self
            .reserved_len
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if len > *reserved {
            self.pool.reserve(0, len - *reserved)?;
            *reserved = len;
        }
        // A failed resize conservatively keeps its reservation until success
        // or backend close; it can never make an ambiguous write free.
        file.set_len(len)?;
        self.pool.release(0, *reserved - len);
        *reserved = len;
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
        assert_eq!(pool.usage.lock().unwrap().files, 41);
        drop(right);
        assert_eq!(pool.usage.lock().unwrap().cache, 0);
        assert_eq!(pool.usage.lock().unwrap().files, 0);
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
        assert_eq!(pool.usage.lock().unwrap().files, 0);
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
}
