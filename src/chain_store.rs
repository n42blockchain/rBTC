//! Unified durable storage for active-chain state.

use std::{
    collections::BTreeMap,
    panic::{AssertUnwindSafe, catch_unwind},
    path::Path,
    sync::{Arc, Mutex, MutexGuard},
};

use bitcoin::{BlockHash, Network};
use redb::{Database, Durability};
use thiserror::Error;

use crate::{
    execution_store::{
        ExecutionStoreError, ExecutionTip, RedbExecutionStore, advance_transaction,
        assume_snapshot_transaction, metadata_exists as execution_metadata_exists,
        rewind_transaction,
    },
    undo_store::{
        RedbUndoStore, UndoStoreError, insert_transaction as insert_undo_transaction,
        remove_transaction as remove_undo_transaction,
        tables_empty_transaction as undo_tables_empty_transaction,
    },
    utxo::{
        OutPointKey, RedbUtxoStore, TierStats, Utxo, UtxoError, UtxoStore, UtxoUndo,
        apply_with_undo_transaction, replace_all_transaction, tables_empty_transaction,
    },
};

/// Persistence behavior for unified chain-state commits.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ChainStoreOptions {
    /// Persist allocator state and use redb's two-phase commit protocol.
    pub quick_repair: bool,
}

impl Default for ChainStoreOptions {
    fn default() -> Self {
        Self { quick_repair: true }
    }
}

/// Errors from the unified chain-state database.
#[derive(Debug, Error)]
pub enum ChainStoreError {
    /// The database is structurally damaged and could not be opened safely.
    #[error("chainstate database is truncated or structurally damaged")]
    Damaged,
    /// A pre-unification UTXO file cannot be safely upgraded in place.
    #[error("legacy chainstate has UTXOs but no co-located execution metadata")]
    LegacyLayout,
    /// Database open/create failed.
    #[error("redb database: {0}")]
    Database(#[from] redb::DatabaseError),
    /// Transaction creation failed.
    #[error("redb transaction: {0}")]
    Transaction(#[from] redb::TransactionError),
    /// Transaction commit failed.
    #[error("redb commit: {0}")]
    Commit(#[from] redb::CommitError),
    /// UTXO operation failed.
    #[error("UTXO store: {0}")]
    Utxo(#[from] UtxoError),
    /// Undo operation failed.
    #[error("undo store: {0}")]
    Undo(#[from] UndoStoreError),
    /// Execution metadata operation failed.
    #[error("execution store: {0}")]
    Execution(#[from] ExecutionStoreError),
    /// Snapshot activation would overwrite previously initialized chain state.
    #[error("assumed snapshot activation requires empty UTXO and undo tables")]
    SnapshotNotFresh,
}

/// One physical redb database containing UTXOs, block undo, and execution metadata.
///
/// Logical views remain separate so snapshot and query code can retain narrow APIs,
/// while active-chain connect/disconnect writes share one atomic transaction.
pub struct RedbChainStore {
    db: Arc<Database>,
    utxos: RedbUtxoStore,
    undos: RedbUndoStore,
    execution: RedbExecutionStore,
    options: ChainStoreOptions,
    write_guard: Mutex<()>,
}

/// One already-validated active-chain transition in an atomic IBD checkpoint.
pub(crate) struct ConnectTransition {
    pub(crate) expected_parent: BlockHash,
    pub(crate) next: ExecutionTip,
    pub(crate) spent: Vec<OutPointKey>,
    pub(crate) created: Vec<(OutPointKey, Utxo)>,
    pub(crate) transaction_undos: Vec<UtxoUndo>,
}

impl RedbChainStore {
    /// Opens a unified store with crash-fast recovery enabled.
    pub fn open(path: impl AsRef<Path>, network: Network) -> Result<Self, ChainStoreError> {
        Self::open_with_options(path, network, ChainStoreOptions::default())
    }

    /// Opens a unified store with explicit persistence options.
    pub fn open_with_options(
        path: impl AsRef<Path>,
        network: Network,
        options: ChainStoreOptions,
    ) -> Result<Self, ChainStoreError> {
        // redb 2.6 contains an internal assertion for certain truncated files.
        // Convert that boundary panic into an explicit startup rejection so a
        // damaged chainstate cannot take the daemon down without diagnosis.
        let database = catch_unwind(AssertUnwindSafe(|| Database::create(path)))
            .map_err(|_| ChainStoreError::Damaged)??;
        let db = Arc::new(database);
        Self::from_database(db, network, options)
    }

    fn from_database(
        db: Arc<Database>,
        network: Network,
        options: ChainStoreOptions,
    ) -> Result<Self, ChainStoreError> {
        let utxos = RedbUtxoStore::from_database(Arc::clone(&db))?;
        if !execution_metadata_exists(&db)? {
            let stats = utxos.tier_stats()?;
            if stats.hot != 0 || stats.cold != 0 {
                return Err(ChainStoreError::LegacyLayout);
            }
        }
        let undos = RedbUndoStore::from_database(Arc::clone(&db))?;
        let execution = RedbExecutionStore::from_database(Arc::clone(&db), network)?;
        Ok(Self {
            db,
            utxos,
            undos,
            execution,
            options,
            write_guard: Mutex::new(()),
        })
    }

    /// Read-only access to retained block undo records.
    pub fn undos(&self) -> &RedbUndoStore {
        &self.undos
    }

    /// Read/write access to execution metadata outside block transitions.
    pub fn execution(&self) -> &RedbExecutionStore {
        &self.execution
    }

    /// Atomically initializes an empty chainstate from an externally trusted UTXO snapshot.
    ///
    /// The execution tip and persistent assumed-state marker become visible in the
    /// same commit as every UTXO. Existing UTXOs, undo, pending transitions, an
    /// advanced tip, or an earlier snapshot marker make the operation fail closed.
    pub fn assume_snapshot(
        &self,
        anchor: ExecutionTip,
        records_sha256: &[u8; 32],
        entries: &BTreeMap<OutPointKey, Utxo>,
        now: u64,
        hot_window_secs: u64,
    ) -> Result<(), ChainStoreError> {
        let _guard = self.lock();
        let mut transaction = self.db.begin_write()?;
        self.configure(&mut transaction);
        if !tables_empty_transaction(&transaction)? || !undo_tables_empty_transaction(&transaction)?
        {
            return Err(ChainStoreError::SnapshotNotFresh);
        }
        assume_snapshot_transaction(&transaction, anchor, records_sha256)?;
        replace_all_transaction(&transaction, entries, now, hot_window_secs)?;
        transaction.commit()?;
        Ok(())
    }

    /// Atomically applies UTXOs, records block undo, and advances the execution tip.
    pub fn commit_connect(
        &self,
        expected_parent: BlockHash,
        next: ExecutionTip,
        spent: &[OutPointKey],
        created: &[(OutPointKey, Utxo)],
        transaction_undos: &[UtxoUndo],
    ) -> Result<UtxoUndo, ChainStoreError> {
        let _guard = self.lock();
        let mut transaction = self.db.begin_write()?;
        self.configure(&mut transaction);
        let undo = apply_with_undo_transaction(&transaction, spent, created)?;
        insert_undo_transaction(&transaction, next.hash, transaction_undos)?;
        advance_transaction(&transaction, expected_parent, next)?;
        transaction.commit()?;
        Ok(undo)
    }

    /// Atomically persists a contiguous group of validated IBD blocks.
    ///
    /// Undo remains block-addressable and the stored execution tip advances
    /// through every block inside the transaction, but no prefix becomes
    /// visible unless the complete checkpoint commits.
    pub(crate) fn commit_connect_batch(
        &self,
        transitions: &[ConnectTransition],
    ) -> Result<Vec<UtxoUndo>, ChainStoreError> {
        if transitions.is_empty() {
            return Ok(Vec::new());
        }
        let _guard = self.lock();
        let mut transaction = self.db.begin_write()?;
        self.configure(&mut transaction);
        let mut undos = Vec::with_capacity(transitions.len());
        for transition in transitions {
            undos.push(apply_with_undo_transaction(
                &transaction,
                &transition.spent,
                &transition.created,
            )?);
            insert_undo_transaction(
                &transaction,
                transition.next.hash,
                &transition.transaction_undos,
            )?;
            advance_transaction(&transaction, transition.expected_parent, transition.next)?;
        }
        transaction.commit()?;
        Ok(undos)
    }

    /// Atomically applies a reverse UTXO transition, removes undo, and rewinds the tip.
    pub fn commit_disconnect(
        &self,
        expected_current: ExecutionTip,
        parent: ExecutionTip,
        spent: &[OutPointKey],
        created: &[(OutPointKey, Utxo)],
    ) -> Result<UtxoUndo, ChainStoreError> {
        let _guard = self.lock();
        let mut transaction = self.db.begin_write()?;
        self.configure(&mut transaction);
        let undo = apply_with_undo_transaction(&transaction, spent, created)?;
        rewind_transaction(&transaction, expected_current, parent)?;
        if !remove_undo_transaction(&transaction, expected_current.hash)? {
            return Err(UndoStoreError::Malformed("missing atomic disconnect undo").into());
        }
        transaction.commit()?;
        Ok(undo)
    }

    fn configure(&self, transaction: &mut redb::WriteTransaction) {
        transaction.set_durability(Durability::Immediate);
        transaction.set_quick_repair(self.options.quick_repair);
    }

    fn lock(&self) -> MutexGuard<'_, ()> {
        self.write_guard.lock().expect("write lock not poisoned")
    }
}

impl UtxoStore for RedbChainStore {
    fn get(&self, outpoint: OutPointKey) -> Result<Option<Utxo>, UtxoError> {
        self.utxos.get(outpoint)
    }

    fn apply(
        &self,
        spent: &[OutPointKey],
        created: &[(OutPointKey, Utxo)],
    ) -> Result<(), UtxoError> {
        self.utxos.apply(spent, created)
    }

    fn apply_with_undo(
        &self,
        spent: &[OutPointKey],
        created: &[(OutPointKey, Utxo)],
    ) -> Result<UtxoUndo, UtxoError> {
        self.utxos.apply_with_undo(spent, created)
    }

    fn undo(&self, undo: &UtxoUndo, now: u64, hot_window_secs: u64) -> Result<(), UtxoError> {
        self.utxos.undo(undo, now, hot_window_secs)
    }

    fn age_to_cold(&self, now: u64, hot_window_secs: u64) -> Result<u64, UtxoError> {
        self.utxos.age_to_cold(now, hot_window_secs)
    }

    fn snapshot_entries(&self) -> Result<BTreeMap<OutPointKey, Utxo>, UtxoError> {
        self.utxos.snapshot_entries()
    }

    fn replace_all(
        &self,
        _entries: &BTreeMap<OutPointKey, Utxo>,
        _now: u64,
        _hot_window_secs: u64,
    ) -> Result<(), UtxoError> {
        Err(UtxoError::Malformed(
            "unified chainstate requires trusted assumed snapshot activation",
        ))
    }

    fn tier_stats(&self) -> Result<TierStats, UtxoError> {
        self.utxos.tier_stats()
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fmt, io,
        sync::{
            RwLock,
            atomic::{AtomicBool, Ordering},
        },
    };

    use bitcoin::{OutPoint, Txid, hashes::Hash};
    use redb::StorageBackend;
    use tempfile::TempDir;

    use super::*;

    fn key(byte: u8) -> OutPointKey {
        OutPoint::new(Txid::from_byte_array([byte; 32]), 0).into()
    }

    fn coin(value_sats: u64) -> Utxo {
        Utxo {
            value_sats,
            height: 0,
            is_coinbase: false,
            last_touched: 0,
            creation_mtp: 0,
            script_pubkey: vec![0x51],
        }
    }

    #[derive(Clone)]
    struct QuotaBackend {
        bytes: Arc<RwLock<Vec<u8>>>,
        full: Arc<AtomicBool>,
    }

    impl fmt::Debug for QuotaBackend {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter
                .debug_struct("QuotaBackend")
                .finish_non_exhaustive()
        }
    }

    impl StorageBackend for QuotaBackend {
        fn len(&self) -> io::Result<u64> {
            u64::try_from(self.bytes.read().expect("backend lock").len()).map_err(io::Error::other)
        }

        fn read(&self, offset: u64, len: usize) -> io::Result<Vec<u8>> {
            let offset = usize::try_from(offset).map_err(io::Error::other)?;
            let end = offset
                .checked_add(len)
                .ok_or_else(|| io::Error::other("read overflow"))?;
            self.bytes
                .read()
                .expect("backend lock")
                .get(offset..end)
                .map(<[u8]>::to_vec)
                .ok_or_else(|| io::Error::new(io::ErrorKind::UnexpectedEof, "backend read"))
        }

        fn set_len(&self, len: u64) -> io::Result<()> {
            if self.full.load(Ordering::SeqCst) {
                return Err(io::Error::other("simulated disk full"));
            }
            let len = usize::try_from(len).map_err(io::Error::other)?;
            self.bytes.write().expect("backend lock").resize(len, 0);
            Ok(())
        }

        fn sync_data(&self, _eventual: bool) -> io::Result<()> {
            if self.full.load(Ordering::SeqCst) {
                return Err(io::Error::other("simulated disk full"));
            }
            Ok(())
        }

        fn write(&self, offset: u64, data: &[u8]) -> io::Result<()> {
            if self.full.load(Ordering::SeqCst) {
                return Err(io::Error::other("simulated disk full"));
            }
            let offset = usize::try_from(offset).map_err(io::Error::other)?;
            let end = offset
                .checked_add(data.len())
                .ok_or_else(|| io::Error::other("write overflow"))?;
            let mut bytes = self.bytes.write().expect("backend lock");
            if end > bytes.len() {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "write past backend length",
                ));
            }
            bytes[offset..end].copy_from_slice(data);
            Ok(())
        }
    }

    #[test]
    fn failed_transition_exposes_only_the_pre_transaction_state_after_reopen() {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("chainstate.redb");
        let store = RedbChainStore::open_with_options(
            &path,
            Network::Regtest,
            ChainStoreOptions { quick_repair: true },
        )
        .unwrap();
        let genesis = store.execution().tip().unwrap();
        let first = ExecutionTip {
            height: 1,
            hash: BlockHash::from_byte_array([11; 32]),
        };
        store.apply(&[], &[(key(1), coin(10))]).unwrap();
        store
            .commit_connect(genesis.hash, first, &[key(1)], &[(key(2), coin(9))], &[])
            .unwrap();

        let duplicate_undo = ExecutionTip {
            height: 2,
            hash: first.hash,
        };
        assert!(matches!(
            store.commit_connect(
                first.hash,
                duplicate_undo,
                &[key(2)],
                &[(key(3), coin(8))],
                &[],
            ),
            Err(ChainStoreError::Undo(UndoStoreError::Duplicate(hash))) if hash == first.hash
        ));
        assert_eq!(store.execution().tip().unwrap(), first);
        assert_eq!(store.get(key(2)).unwrap(), Some(coin(9)));
        assert!(store.get(key(3)).unwrap().is_none());
        drop(store);

        let reopened = RedbChainStore::open(&path, Network::Regtest).unwrap();
        assert_eq!(reopened.execution().tip().unwrap(), first);
        assert_eq!(reopened.get(key(2)).unwrap(), Some(coin(9)));
        assert!(reopened.get(key(3)).unwrap().is_none());
        assert!(reopened.undos().get(first.hash).unwrap().is_some());
    }

    #[test]
    fn disk_full_commit_never_exposes_mixed_chain_state() {
        let backend = QuotaBackend {
            bytes: Arc::new(RwLock::new(Vec::new())),
            full: Arc::new(AtomicBool::new(false)),
        };
        let database = Arc::new(
            Database::builder()
                .create_with_backend(backend.clone())
                .unwrap(),
        );
        let store =
            RedbChainStore::from_database(database, Network::Regtest, ChainStoreOptions::default())
                .unwrap();
        let genesis = store.execution().tip().unwrap();
        store.apply(&[], &[(key(1), coin(10))]).unwrap();
        backend.full.store(true, Ordering::SeqCst);
        let next = ExecutionTip {
            height: 1,
            hash: BlockHash::from_byte_array([22; 32]),
        };
        assert!(
            store
                .commit_connect(genesis.hash, next, &[key(1)], &[(key(2), coin(9))], &[],)
                .is_err()
        );
        backend.full.store(false, Ordering::SeqCst);
        drop(store);

        let reopened = RedbChainStore::from_database(
            Arc::new(Database::builder().create_with_backend(backend).unwrap()),
            Network::Regtest,
            ChainStoreOptions::default(),
        )
        .unwrap();
        assert_eq!(reopened.execution().tip().unwrap(), genesis);
        assert_eq!(reopened.get(key(1)).unwrap(), Some(coin(10)));
        assert!(reopened.get(key(2)).unwrap().is_none());
        assert!(reopened.undos().get(next.hash).unwrap().is_none());
    }

    #[test]
    fn assumed_snapshot_utxos_tip_and_marker_survive_reopen_together() {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("chainstate.redb");
        let store = RedbChainStore::open(&path, Network::Regtest).unwrap();
        let anchor = ExecutionTip {
            height: 144,
            hash: BlockHash::from_byte_array([44; 32]),
        };
        let entries = BTreeMap::from([(key(1), coin(10)), (key(2), coin(20))]);
        store
            .assume_snapshot(anchor, &[1; 32], &entries, 100, 60)
            .unwrap();
        assert_eq!(store.execution().tip().unwrap(), anchor);
        assert_eq!(
            store.execution().assumed_snapshot_base().unwrap(),
            Some(anchor)
        );
        assert_eq!(
            store.execution().assumed_snapshot_records_sha256().unwrap(),
            Some([1; 32])
        );
        assert_eq!(store.snapshot_entries().unwrap(), entries);
        drop(store);

        let reopened = RedbChainStore::open(path, Network::Regtest).unwrap();
        assert_eq!(reopened.execution().tip().unwrap(), anchor);
        assert_eq!(
            reopened.execution().assumed_snapshot_base().unwrap(),
            Some(anchor)
        );
        assert_eq!(
            reopened
                .execution()
                .assumed_snapshot_records_sha256()
                .unwrap(),
            Some([1; 32])
        );
        assert_eq!(reopened.snapshot_entries().unwrap(), entries);
    }

    #[test]
    fn assumed_snapshot_refuses_to_overwrite_any_existing_utxo() {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("chainstate.redb");
        let store = RedbChainStore::open(&path, Network::Regtest).unwrap();
        let genesis = store.execution().tip().unwrap();
        store.apply(&[], &[(key(1), coin(10))]).unwrap();
        let anchor = ExecutionTip {
            height: 1,
            hash: BlockHash::from_byte_array([45; 32]),
        };
        assert!(matches!(
            store.assume_snapshot(
                anchor,
                &[2; 32],
                &BTreeMap::from([(key(2), coin(20))]),
                100,
                60
            ),
            Err(ChainStoreError::SnapshotNotFresh)
        ));
        assert_eq!(store.execution().tip().unwrap(), genesis);
        assert_eq!(store.execution().assumed_snapshot_base().unwrap(), None);
        assert_eq!(
            store.execution().assumed_snapshot_records_sha256().unwrap(),
            None
        );
        assert_eq!(
            store.snapshot_entries().unwrap(),
            BTreeMap::from([(key(1), coin(10))])
        );
        assert!(matches!(
            store.replace_all(&BTreeMap::from([(key(3), coin(30))]), 100, 60),
            Err(UtxoError::Malformed(
                "unified chainstate requires trusted assumed snapshot activation"
            ))
        ));
        assert_eq!(store.execution().tip().unwrap(), genesis);
        assert_eq!(
            store.snapshot_entries().unwrap(),
            BTreeMap::from([(key(1), coin(10))])
        );
    }

    #[test]
    fn disk_full_snapshot_activation_never_exposes_utxos_without_its_tip() {
        let backend = QuotaBackend {
            bytes: Arc::new(RwLock::new(Vec::new())),
            full: Arc::new(AtomicBool::new(false)),
        };
        let database = Arc::new(
            Database::builder()
                .create_with_backend(backend.clone())
                .unwrap(),
        );
        let store =
            RedbChainStore::from_database(database, Network::Regtest, ChainStoreOptions::default())
                .unwrap();
        let genesis = store.execution().tip().unwrap();
        let anchor = ExecutionTip {
            height: 10,
            hash: BlockHash::from_byte_array([46; 32]),
        };
        backend.full.store(true, Ordering::SeqCst);
        assert!(
            store
                .assume_snapshot(
                    anchor,
                    &[3; 32],
                    &BTreeMap::from([(key(1), coin(10))]),
                    100,
                    60
                )
                .is_err()
        );
        backend.full.store(false, Ordering::SeqCst);
        drop(store);

        let reopened = RedbChainStore::from_database(
            Arc::new(Database::builder().create_with_backend(backend).unwrap()),
            Network::Regtest,
            ChainStoreOptions::default(),
        )
        .unwrap();
        assert_eq!(reopened.execution().tip().unwrap(), genesis);
        assert_eq!(reopened.execution().assumed_snapshot_base().unwrap(), None);
        assert_eq!(
            reopened
                .execution()
                .assumed_snapshot_records_sha256()
                .unwrap(),
            None
        );
        assert!(reopened.snapshot_entries().unwrap().is_empty());
    }

    #[test]
    fn refuses_to_initialize_metadata_over_a_legacy_utxo_file() {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("chainstate.redb");
        let legacy = RedbUtxoStore::open(&path).unwrap();
        legacy.apply(&[], &[(key(1), coin(10))]).unwrap();
        drop(legacy);

        assert!(matches!(
            RedbChainStore::open(&path, Network::Regtest),
            Err(ChainStoreError::LegacyLayout)
        ));
        let reopened = RedbUtxoStore::open(path).unwrap();
        assert_eq!(reopened.get(key(1)).unwrap(), Some(coin(10)));
    }

    #[test]
    fn failed_second_block_aborts_the_entire_durable_checkpoint() {
        let directory = TempDir::new().unwrap();
        let store =
            RedbChainStore::open(directory.path().join("chainstate.redb"), Network::Regtest)
                .unwrap();
        let genesis = store.execution().tip().unwrap();
        store.apply(&[], &[(key(1), coin(10))]).unwrap();
        let repeated_hash = BlockHash::from_byte_array([33; 32]);
        let transitions = [
            ConnectTransition {
                expected_parent: genesis.hash,
                next: ExecutionTip {
                    height: 1,
                    hash: repeated_hash,
                },
                spent: vec![key(1)],
                created: vec![(key(2), coin(9))],
                transaction_undos: vec![],
            },
            ConnectTransition {
                expected_parent: repeated_hash,
                next: ExecutionTip {
                    height: 2,
                    hash: repeated_hash,
                },
                spent: vec![key(2)],
                created: vec![(key(3), coin(8))],
                transaction_undos: vec![],
            },
        ];

        assert!(matches!(
            store.commit_connect_batch(&transitions),
            Err(ChainStoreError::Undo(UndoStoreError::Duplicate(hash))) if hash == repeated_hash
        ));
        assert_eq!(store.execution().tip().unwrap(), genesis);
        assert_eq!(store.get(key(1)).unwrap(), Some(coin(10)));
        assert!(store.get(key(2)).unwrap().is_none());
        assert!(store.get(key(3)).unwrap().is_none());
        assert!(store.undos().get(repeated_hash).unwrap().is_none());
    }
}
