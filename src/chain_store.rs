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
        AssumedSnapshot, ExecutionStoreError, ExecutionTip, RedbExecutionStore,
        advance_transaction, assume_snapshot_transaction, clear_assumed_snapshot_transaction,
        metadata_exists as execution_metadata_exists, rewind_transaction,
    },
    headers::HeaderDag,
    undo_store::{
        RedbUndoStore, UndoStoreError, insert_transaction as insert_undo_transaction,
        remove_transaction as remove_undo_transaction,
        tables_empty_transaction as undo_tables_empty_transaction,
    },
    utxo::{
        OutPointKey, RedbUtxoStore, TierStats, Utxo, UtxoError, UtxoStore, UtxoUndo,
        apply_with_undo_transaction, insert_snapshot_entries_transaction, tables_empty_transaction,
    },
};

/// Persistence behavior for unified chain-state commits.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ChainStoreOptions {
    /// Persist allocator state and use redb's two-phase commit protocol.
    pub quick_repair: bool,
}

/// Authenticated identity of one canonical snapshot entry stream.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SnapshotContentIdentity {
    /// SHA-256 of every canonical uncompressed record.
    pub records_sha256: [u8; 32],
    /// Exact number of UTXO records.
    pub utxo_count: u64,
    /// Exact canonical uncompressed byte length.
    pub records_bytes: u64,
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
    /// Offline database compaction failed.
    #[error("redb compaction: {0}")]
    Compaction(#[from] redb::CompactionError),
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
    /// The imported canonical entry stream did not match its trusted digest.
    #[error("assumed snapshot records SHA-256 mismatch")]
    SnapshotDigestMismatch,
    /// The imported entry count differed from the inspected manifest.
    #[error("assumed snapshot expected {expected} UTXOs but decoded {actual}")]
    SnapshotCountMismatch {
        /// Count declared by the verified manifest.
        expected: u64,
        /// Count decoded again inside the activation transaction.
        actual: u64,
    },
    /// The imported canonical stream length differed from authenticated metadata.
    #[error("assumed snapshot expected {expected} record bytes but decoded {actual}")]
    SnapshotSizeMismatch {
        /// Canonical byte length in authenticated release metadata.
        expected: u64,
        /// Canonical byte length decoded inside the transaction.
        actual: u64,
    },
    /// The active chainstate has no assumed-state marker to finalize.
    #[error("chainstate has no assumed UTXO snapshot awaiting validation")]
    NoAssumedSnapshot,
    /// The independent validation chainstate is itself snapshot-based.
    #[error("validation chainstate must be independently executed from genesis")]
    ValidationChainstateIsAssumed,
    /// The validation chainstate did not stop exactly at the snapshot base.
    #[error(
        "validation chainstate tip {actual_height}:{actual_hash} does not match snapshot base {expected_height}:{expected_hash}"
    )]
    ValidationTipMismatch {
        /// Required snapshot-base height.
        expected_height: u32,
        /// Required snapshot-base hash.
        expected_hash: BlockHash,
        /// Independently validated height.
        actual_height: u32,
        /// Independently validated hash.
        actual_hash: BlockHash,
    },
    /// The two chainstates were not executed under identical consensus rules.
    #[error("active and validation chainstates have different consensus configurations")]
    ValidationConsensusMismatch,
    /// The validation directory was assigned to another snapshot base.
    #[error("validation directory target does not match the assumed snapshot base")]
    ValidationTargetMismatch,
    /// The snapshot base is not on the selected active header chain.
    #[error("snapshot base {height}:{hash} is not on the active header chain")]
    SnapshotBaseNotActive {
        /// Snapshot-base height.
        height: u32,
        /// Snapshot-base hash.
        hash: BlockHash,
    },
    /// The current execution tip is not on the selected active header chain.
    #[error("execution tip {height}:{hash} is not on the active header chain")]
    ExecutionTipNotActive {
        /// Current execution height.
        height: u32,
        /// Current execution hash.
        hash: BlockHash,
    },
    /// The independently computed UTXO identity differs from trusted metadata.
    #[error("validation chainstate UTXO identity does not match the assumed snapshot")]
    ValidationContentMismatch,
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

    /// Compacts a closed chainstate database and reports whether maintenance work ran.
    ///
    /// The daemon and every other [`RedbChainStore`] handle for this path must be
    /// closed first. redb performs the maintenance commits with its two-phase
    /// protocol; a structurally damaged file is rejected rather than rewritten.
    pub fn compact_file(path: impl AsRef<Path>) -> Result<bool, ChainStoreError> {
        catch_unwind(AssertUnwindSafe(|| {
            let mut database = Database::open(path)?;
            Ok(database.compact()?)
        }))
        .map_err(|_| ChainStoreError::Damaged)?
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

    /// Returns one sorted, cursor-based page from the complete hot/cold UTXO set.
    pub fn utxo_snapshot_page(
        &self,
        after: Option<OutPointKey>,
        limit: usize,
    ) -> Result<Vec<(OutPointKey, Utxo)>, UtxoError> {
        self.utxos.snapshot_page(after, limit)
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
        let records_bytes = entries.values().try_fold(0_u64, |total, utxo| {
            let encoded = utxo.encode()?;
            total
                .checked_add(u64::try_from(36 + encoded.len()).expect("record length fits u64"))
                .ok_or(UtxoError::Malformed("snapshot records length overflow"))
        })?;
        self.assume_snapshot_entries(
            anchor,
            SnapshotContentIdentity {
                records_sha256: *records_sha256,
                utxo_count: u64::try_from(entries.len()).expect("usize fits u64"),
                records_bytes,
            },
            entries.iter().map(|(key, utxo)| Ok((*key, utxo.clone()))),
            now,
            hot_window_secs,
        )
    }

    /// Streams a canonical snapshot directly into one atomic chainstate transaction.
    ///
    /// The digest and count are recomputed while records enter redb. Any decoder,
    /// ordering, count, digest, or commit error aborts all inserted records and
    /// leaves the genesis execution metadata unchanged.
    pub fn assume_snapshot_entries<I>(
        &self,
        anchor: ExecutionTip,
        content: SnapshotContentIdentity,
        entries: I,
        now: u64,
        hot_window_secs: u64,
    ) -> Result<(), ChainStoreError>
    where
        I: IntoIterator<Item = Result<(OutPointKey, Utxo), UtxoError>>,
    {
        let _guard = self.lock();
        let mut transaction = self.db.begin_write()?;
        self.configure(&mut transaction);
        if !tables_empty_transaction(&transaction)? || !undo_tables_empty_transaction(&transaction)?
        {
            return Err(ChainStoreError::SnapshotNotFresh);
        }
        let (actual_count, actual_records_bytes, actual_digest, utxo_set_sha256) =
            insert_snapshot_entries_transaction(
                &transaction,
                entries,
                content.utxo_count,
                content.records_bytes,
                now,
                hot_window_secs,
            )?;
        if actual_count != content.utxo_count {
            return Err(ChainStoreError::SnapshotCountMismatch {
                expected: content.utxo_count,
                actual: actual_count,
            });
        }
        if actual_records_bytes != content.records_bytes {
            return Err(ChainStoreError::SnapshotSizeMismatch {
                expected: content.records_bytes,
                actual: actual_records_bytes,
            });
        }
        if actual_digest != content.records_sha256 {
            return Err(ChainStoreError::SnapshotDigestMismatch);
        }
        assume_snapshot_transaction(
            &transaction,
            AssumedSnapshot {
                base: anchor,
                utxo_count: content.utxo_count,
                records_bytes: content.records_bytes,
                records_sha256: content.records_sha256,
                utxo_set_sha256,
            },
        )?;
        transaction.commit()?;
        Ok(())
    }

    /// Clears an assumed-state marker after an independent genesis validation matches it.
    ///
    /// `validation` must be a separate, non-assumed chainstate stopped exactly at
    /// the snapshot base. Its UTXOs are streamed in canonical order, so this check
    /// has bounded memory use. The marker is rechecked and removed in one durable
    /// transaction; active UTXOs and the (possibly newer) active execution tip are
    /// left untouched.
    pub fn finalize_assumed_snapshot(
        &self,
        validation: &Self,
        headers: &HeaderDag,
    ) -> Result<AssumedSnapshot, ChainStoreError> {
        let _guard = self.lock();
        let assumed = self
            .execution
            .assumed_snapshot()?
            .ok_or(ChainStoreError::NoAssumedSnapshot)?;
        if validation.execution.assumed_snapshot()?.is_some() {
            return Err(ChainStoreError::ValidationChainstateIsAssumed);
        }
        let validation_tip = validation.execution.tip()?;
        if validation_tip != assumed.base {
            return Err(ChainStoreError::ValidationTipMismatch {
                expected_height: assumed.base.height,
                expected_hash: assumed.base.hash,
                actual_height: validation_tip.height,
                actual_hash: validation_tip.hash,
            });
        }
        if validation
            .execution
            .validation_target()?
            .is_some_and(|target| target != assumed.base)
        {
            return Err(ChainStoreError::ValidationTargetMismatch);
        }
        let active_config = self.execution.consensus_config()?;
        let validation_config = validation.execution.consensus_config()?;
        if active_config.is_none() || active_config != validation_config {
            return Err(ChainStoreError::ValidationConsensusMismatch);
        }
        if headers
            .active_header_at(assumed.base.height)
            .is_none_or(|header| header.hash != assumed.base.hash)
        {
            return Err(ChainStoreError::SnapshotBaseNotActive {
                height: assumed.base.height,
                hash: assumed.base.hash,
            });
        }
        let active_tip = self.execution.tip()?;
        if headers
            .active_header_at(active_tip.height)
            .is_none_or(|header| header.hash != active_tip.hash)
        {
            return Err(ChainStoreError::ExecutionTipNotActive {
                height: active_tip.height,
                hash: active_tip.hash,
            });
        }
        let (utxo_count, records_bytes, utxo_set_sha256) =
            validation.utxos.snapshot_content_identity()?;
        if utxo_count != assumed.utxo_count
            || records_bytes != assumed.records_bytes
            || utxo_set_sha256 != assumed.utxo_set_sha256
        {
            return Err(ChainStoreError::ValidationContentMismatch);
        }
        let mut transaction = self.db.begin_write()?;
        self.configure(&mut transaction);
        clear_assumed_snapshot_transaction(&transaction, assumed)?;
        transaction.commit()?;
        Ok(assumed)
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

    use bitcoin::{
        OutPoint, TxMerkleNode, Txid,
        block::{Header, Version},
        consensus::Params,
        hashes::Hash,
    };
    use redb::StorageBackend;
    use sha2::{Digest, Sha256};
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

    fn snapshot_digest(entries: &BTreeMap<OutPointKey, Utxo>) -> [u8; 32] {
        let mut digest = Sha256::new();
        for (key, utxo) in entries {
            digest.update(key.as_bytes());
            digest.update(utxo.encode().unwrap());
        }
        digest.finalize().into()
    }

    fn snapshot_bytes(entries: &BTreeMap<OutPointKey, Utxo>) -> u64 {
        entries
            .values()
            .map(|utxo| u64::try_from(36 + utxo.encode().unwrap().len()).unwrap())
            .sum()
    }

    fn mine_child(parent: BlockHash, time: u32) -> Header {
        let target = Params::new(Network::Regtest).max_attainable_target;
        let mut header = Header {
            version: Version::from_consensus(4),
            prev_blockhash: parent,
            merkle_root: TxMerkleNode::all_zeros(),
            time,
            bits: target.to_compact_lossy(),
            nonce: 0,
        };
        while header.validate_pow(target).is_err() {
            header.nonce += 1;
        }
        header
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
    fn offline_compaction_preserves_chainstate_tip_utxos_and_undo() {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("chainstate.redb");
        let store = RedbChainStore::open(&path, Network::Regtest).unwrap();
        let genesis = store.execution().tip().unwrap();
        store.apply(&[], &[(key(1), coin(10))]).unwrap();
        let first = ExecutionTip {
            height: 1,
            hash: BlockHash::from_byte_array([11; 32]),
        };
        store
            .commit_connect(genesis.hash, first, &[key(1)], &[(key(2), coin(9))], &[])
            .unwrap();
        drop(store);

        let _compacted = RedbChainStore::compact_file(&path).unwrap();
        let reopened = RedbChainStore::open(&path, Network::Regtest).unwrap();
        assert_eq!(reopened.execution().tip().unwrap(), first);
        assert_eq!(reopened.get(key(2)).unwrap(), Some(coin(9)));
        assert!(reopened.get(key(1)).unwrap().is_none());
        assert!(reopened.undos().get(first.hash).unwrap().is_some());
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
        let records_sha256 = snapshot_digest(&entries);
        store
            .assume_snapshot(anchor, &records_sha256, &entries, 100, 60)
            .unwrap();
        assert_eq!(store.execution().tip().unwrap(), anchor);
        assert_eq!(
            store.execution().assumed_snapshot_base().unwrap(),
            Some(anchor)
        );
        assert_eq!(
            store.execution().assumed_snapshot_records_sha256().unwrap(),
            Some(records_sha256)
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
            Some(records_sha256)
        );
        assert_eq!(reopened.snapshot_entries().unwrap(), entries);
    }

    #[test]
    fn independent_genesis_validation_atomically_finalizes_assumed_snapshot() {
        let directory = TempDir::new().unwrap();
        let active =
            RedbChainStore::open(directory.path().join("active.redb"), Network::Regtest).unwrap();
        let validation =
            RedbChainStore::open(directory.path().join("validation.redb"), Network::Regtest)
                .unwrap();
        active
            .execution()
            .bind_consensus_config(b"rules", b"rules")
            .unwrap();
        validation
            .execution()
            .bind_consensus_config(b"rules", b"rules")
            .unwrap();
        let mut headers = HeaderDag::new(Network::Regtest);
        let genesis = headers.active_tip();
        let header = mine_child(genesis.hash, genesis.header.time + 1);
        let anchor = ExecutionTip {
            height: 1,
            hash: header.block_hash(),
        };
        headers.insert(header).unwrap();
        let mut recent = coin(20);
        recent.last_touched = 100;
        let entries = BTreeMap::from([(key(1), coin(10)), (key(2), recent)]);
        let digest = snapshot_digest(&entries);
        active
            .assume_snapshot(anchor, &digest, &entries, 100, 60)
            .unwrap();
        let independently_replayed = entries
            .iter()
            .map(|(key, utxo)| {
                let mut utxo = utxo.clone();
                utxo.last_touched = utxo.last_touched.saturating_add(10_000);
                (*key, utxo)
            })
            .collect::<Vec<_>>();
        validation
            .commit_connect(genesis.hash, anchor, &[], &independently_replayed, &[])
            .unwrap();
        let next_header = mine_child(anchor.hash, header.time + 1);
        let active_tip = ExecutionTip {
            height: 2,
            hash: next_header.block_hash(),
        };
        headers.insert(next_header).unwrap();
        active
            .commit_connect(anchor.hash, active_tip, &[], &[], &[])
            .unwrap();

        let finalized = active
            .finalize_assumed_snapshot(&validation, &headers)
            .unwrap();
        assert_eq!(finalized.base, anchor);
        assert_eq!(finalized.utxo_count, 2);
        assert_eq!(finalized.records_bytes, snapshot_bytes(&entries));
        assert_eq!(active.execution().assumed_snapshot().unwrap(), None);
        assert_eq!(active.execution().snapshot_origin().unwrap(), Some(anchor));
        assert_eq!(active.execution().tip().unwrap(), active_tip);
        assert_eq!(active.snapshot_entries().unwrap(), entries);
        assert!(matches!(
            active.finalize_assumed_snapshot(&validation, &headers),
            Err(ChainStoreError::NoAssumedSnapshot)
        ));
        drop(active);
        let reopened =
            RedbChainStore::open(directory.path().join("active.redb"), Network::Regtest).unwrap();
        assert_eq!(reopened.execution().assumed_snapshot().unwrap(), None);
        assert_eq!(
            reopened.execution().snapshot_origin().unwrap(),
            Some(anchor)
        );
        assert_eq!(reopened.execution().tip().unwrap(), active_tip);
        assert_eq!(reopened.snapshot_entries().unwrap(), entries);
    }

    #[test]
    fn finalize_rejects_wrong_validation_identity_and_preserves_marker() {
        let directory = TempDir::new().unwrap();
        let active =
            RedbChainStore::open(directory.path().join("active.redb"), Network::Regtest).unwrap();
        let validation =
            RedbChainStore::open(directory.path().join("validation.redb"), Network::Regtest)
                .unwrap();
        active
            .execution()
            .bind_consensus_config(b"rules", b"rules")
            .unwrap();
        validation
            .execution()
            .bind_consensus_config(b"rules", b"rules")
            .unwrap();
        let mut headers = HeaderDag::new(Network::Regtest);
        let genesis = headers.active_tip();
        let header = mine_child(genesis.hash, genesis.header.time + 1);
        let anchor = ExecutionTip {
            height: 1,
            hash: header.block_hash(),
        };
        headers.insert(header).unwrap();
        let entries = BTreeMap::from([(key(1), coin(10))]);
        active
            .assume_snapshot(anchor, &snapshot_digest(&entries), &entries, 100, 60)
            .unwrap();
        validation
            .commit_connect(genesis.hash, anchor, &[], &[(key(1), coin(11))], &[])
            .unwrap();

        assert!(matches!(
            active.finalize_assumed_snapshot(&validation, &headers),
            Err(ChainStoreError::ValidationContentMismatch)
        ));
        assert_eq!(
            active.execution().assumed_snapshot().unwrap().unwrap().base,
            anchor
        );
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
    fn streaming_snapshot_rejects_count_digest_order_and_late_input_errors_atomically() {
        let directory = TempDir::new().unwrap();
        let store =
            RedbChainStore::open(directory.path().join("chainstate.redb"), Network::Regtest)
                .unwrap();
        let genesis = store.execution().tip().unwrap();
        let anchor = ExecutionTip {
            height: 10,
            hash: BlockHash::from_byte_array([47; 32]),
        };
        let entries = BTreeMap::from([(key(1), coin(10)), (key(2), coin(20))]);
        let digest = snapshot_digest(&entries);
        let records_bytes = snapshot_bytes(&entries);
        let stream = || entries.iter().map(|(key, utxo)| Ok((*key, utxo.clone())));

        assert!(matches!(
            store.assume_snapshot_entries(
                anchor,
                SnapshotContentIdentity {
                    records_sha256: digest,
                    utxo_count: 3,
                    records_bytes,
                },
                stream(),
                100,
                60
            ),
            Err(ChainStoreError::SnapshotCountMismatch {
                expected: 3,
                actual: 2
            })
        ));
        assert!(matches!(
            store.assume_snapshot_entries(
                anchor,
                SnapshotContentIdentity {
                    records_sha256: [0; 32],
                    utxo_count: 2,
                    records_bytes,
                },
                stream(),
                100,
                60
            ),
            Err(ChainStoreError::SnapshotDigestMismatch)
        ));
        assert!(matches!(
            store.assume_snapshot_entries(
                anchor,
                SnapshotContentIdentity {
                    records_sha256: digest,
                    utxo_count: 2,
                    records_bytes: records_bytes + 1,
                },
                stream(),
                100,
                60
            ),
            Err(ChainStoreError::SnapshotSizeMismatch {
                expected,
                actual
            }) if expected == records_bytes + 1 && actual == records_bytes
        ));
        assert!(matches!(
            store.assume_snapshot_entries(
                anchor,
                SnapshotContentIdentity {
                    records_sha256: digest,
                    utxo_count: 2,
                    records_bytes,
                },
                vec![Ok((key(2), coin(20))), Ok((key(1), coin(10)))],
                100,
                60
            ),
            Err(ChainStoreError::Utxo(UtxoError::Malformed(
                "snapshot outpoints are not strictly ordered"
            )))
        ));
        assert!(matches!(
            store.assume_snapshot_entries(
                anchor,
                SnapshotContentIdentity {
                    records_sha256: digest,
                    utxo_count: 2,
                    records_bytes,
                },
                vec![
                    Ok((key(1), coin(10))),
                    Err(UtxoError::Malformed("late decoder failure"))
                ],
                100,
                60
            ),
            Err(ChainStoreError::Utxo(UtxoError::Malformed(
                "late decoder failure"
            )))
        ));
        assert_eq!(store.execution().tip().unwrap(), genesis);
        assert_eq!(store.execution().assumed_snapshot_base().unwrap(), None);
        assert!(store.snapshot_entries().unwrap().is_empty());
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
