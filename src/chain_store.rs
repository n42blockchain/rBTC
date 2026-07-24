//! Unified durable storage for active-chain state.

use std::{
    collections::BTreeMap,
    panic::{AssertUnwindSafe, catch_unwind},
    path::Path,
    sync::{Arc, Mutex, MutexGuard},
};

use ahash::{AHashMap, AHashSet};
use bitcoin::{BlockHash, Network};
use redb::{Database, Durability, ReadableTable, ReadableTableMetadata, TableDefinition};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::{
    execution_store::{
        AssumedSnapshot, ExecutionStoreError, ExecutionTip, RedbExecutionStore,
        advance_transaction, assume_snapshot_transaction, clear_assumed_snapshot_transaction,
        metadata_exists as execution_metadata_exists, rewind_transaction,
    },
    headers::HeaderDag,
    undo_store::{
        RedbUndoStore, UndoStoreError, clear_block_undos_database,
        insert_transaction as insert_undo_transaction,
        remove_transaction as remove_undo_transaction,
        tables_empty_transaction as undo_tables_empty_transaction,
    },
    utxo::{
        OutPointKey, RedbUtxoStore, TierStats, Utxo, UtxoError, UtxoStore, UtxoUndo,
        apply_validated_changes_transaction, apply_with_undo_transaction,
        insert_snapshot_entries_transaction, tables_empty_transaction, update_utxo_set_digest,
    },
};

/// Persistence behavior for unified chain-state commits.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ChainStoreOptions {
    /// Persist allocator state and use redb's two-phase commit protocol.
    pub quick_repair: bool,
    /// Retain per-block records needed to disconnect the active tip.
    pub retain_block_undo: bool,
    /// Total redb read/write cache budget in bytes.
    pub cache_size_bytes: usize,
    /// Persist validation-only UTXO changes in a sequential delta journal.
    ///
    /// This is valid only for fixed-target stores that retain no block undo.
    pub validation_delta_journal: bool,
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
        Self {
            quick_repair: true,
            retain_block_undo: true,
            cache_size_bytes: 1024 * 1024 * 1024,
            validation_delta_journal: false,
        }
    }
}

/// Errors from the unified chain-state database.
#[derive(Debug, Error)]
pub enum ChainStoreError {
    /// The database is structurally damaged and could not be opened safely.
    #[error("chainstate database is truncated or structurally damaged")]
    Damaged,
    /// A journal-backed validation store was opened without its required mode.
    #[error("chainstate contains validation delta records but journal mode is disabled")]
    ValidationDeltaModeRequired,
    /// The sequential validation journal cannot back a reorganizing chainstate.
    #[error("validation delta journal requires block undo retention to be disabled")]
    ValidationDeltaRetainsUndo,
    /// A pre-unification UTXO file cannot be safely upgraded in place.
    #[error("legacy chainstate has UTXOs but no co-located execution metadata")]
    LegacyLayout,
    /// Database open/create failed.
    #[error("redb database: {0}")]
    Database(#[from] redb::DatabaseError),
    /// Transaction creation failed.
    #[error("redb transaction: {0}")]
    Transaction(#[from] redb::TransactionError),
    /// Table access failed.
    #[error("redb table: {0}")]
    Table(#[from] redb::TableError),
    /// Key/value access failed.
    #[error("redb storage: {0}")]
    Storage(#[from] redb::StorageError),
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
    validation_journal: Option<Mutex<ValidationJournal>>,
    write_guard: Mutex<()>,
}

const VALIDATION_DELTA_TABLE: TableDefinition<u32, &[u8]> =
    TableDefinition::new("validation_utxo_deltas");
const MAX_VALIDATION_DELTA_RECORD_BYTES: usize = 512 * 1024 * 1024;
const VALIDATION_DELTA_MAGIC: [u8; 4] = *b"RVD3";
const VALIDATION_DELTA_HEADER_BYTES: usize = 16;
const VALIDATION_DELTA_INDEX_BYTES: usize = 45;
const VALIDATION_BLOOM_BITS_PER_UPDATE: usize = 10;
const VALIDATION_ROWS_PER_BLOOM_GROUP: usize = 16;
const VALIDATION_GROUP_BLOOM_UPDATES: usize = 16_000_000;

struct ValidationJournal {
    rows: Vec<ValidationJournalRow>,
    groups: Vec<ValidationBloom>,
    utxo_count: u64,
}

struct ValidationJournalRow {
    height: u32,
    bloom: ValidationBloom,
}

struct ValidationBloom {
    bits: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ValidationUpdate {
    spent_in_batch: bool,
    utxo: Option<Utxo>,
}

type ValidationDeltaUpdates = Vec<(OutPointKey, ValidationUpdate)>;

impl ValidationBloom {
    fn with_update_count(update_count: usize) -> Result<Self, UtxoError> {
        let bit_count = update_count
            .checked_mul(VALIDATION_BLOOM_BITS_PER_UPDATE)
            .ok_or(UtxoError::Malformed("validation bloom size overflow"))?;
        let byte_count = bit_count.div_ceil(8).max(1);
        Ok(Self {
            bits: vec![0; byte_count],
        })
    }

    fn insert(&mut self, outpoint: OutPointKey) {
        let bit_count = u64::try_from(self.bits.len() * 8).expect("bloom bit length fits u64");
        let (first, step) = validation_bloom_hashes(outpoint);
        for probe in 0..3_u64 {
            let bit = first.wrapping_add(probe.wrapping_mul(step)) % bit_count;
            let byte = usize::try_from(bit / 8).expect("bloom byte index fits usize");
            self.bits[byte] |= 1 << (bit % 8);
        }
    }

    fn might_contain(&self, outpoint: OutPointKey) -> bool {
        let bit_count = u64::try_from(self.bits.len() * 8).expect("bloom bit length fits u64");
        let (first, step) = validation_bloom_hashes(outpoint);
        (0..3_u64).all(|probe| {
            let bit = first.wrapping_add(probe.wrapping_mul(step)) % bit_count;
            let byte = usize::try_from(bit / 8).expect("bloom byte index fits usize");
            self.bits[byte] & (1 << (bit % 8)) != 0
        })
    }
}

fn validation_bloom_hashes(outpoint: OutPointKey) -> (u64, u64) {
    let bytes = outpoint.as_bytes();
    let word = |offset| {
        u64::from_le_bytes(
            bytes[offset..offset + 8]
                .try_into()
                .expect("fixed-width outpoint word"),
        )
    };
    let first = word(0)
        ^ word(16).rotate_left(23)
        ^ u64::from_le_bytes(
            bytes[28..36]
                .try_into()
                .expect("fixed-width outpoint suffix"),
        );
    let step = (word(8) ^ word(24).rotate_left(41)).rotate_left(17) | 1;
    (first, step)
}

/// One already-validated active-chain transition in an atomic IBD checkpoint.
pub(crate) struct ConnectTransition {
    pub(crate) expected_parent: BlockHash,
    pub(crate) next: ExecutionTip,
    pub(crate) spent: Vec<OutPointKey>,
    pub(crate) created: Vec<(OutPointKey, Utxo)>,
    pub(crate) transaction_undos: Vec<UtxoUndo>,
}

fn encode_validation_delta<'a>(
    updates: impl ExactSizeIterator<Item = (&'a OutPointKey, &'a ValidationUpdate)>,
    utxo_count: u64,
) -> Result<Vec<u8>, UtxoError> {
    let update_count = updates.len();
    let count = u32::try_from(update_count)
        .map_err(|_| UtxoError::Malformed("validation delta update count"))?;
    let data_start = VALIDATION_DELTA_HEADER_BYTES
        .checked_add(
            update_count
                .checked_mul(VALIDATION_DELTA_INDEX_BYTES)
                .ok_or(UtxoError::Malformed("validation delta index overflow"))?,
        )
        .ok_or(UtxoError::Malformed("validation delta index overflow"))?;
    let mut encoded = Vec::with_capacity(data_start);
    let mut data = Vec::new();
    encoded.extend_from_slice(&VALIDATION_DELTA_MAGIC);
    encoded.extend_from_slice(&utxo_count.to_le_bytes());
    encoded.extend_from_slice(&count.to_le_bytes());
    for (outpoint, update) in updates {
        encoded.extend_from_slice(outpoint.as_bytes());
        let state = u8::from(update.utxo.is_some()) | (u8::from(update.spent_in_batch) << 1);
        encoded.push(state);
        let offset = data_start
            .checked_add(data.len())
            .ok_or(UtxoError::Malformed(
                "validation delta data offset overflow",
            ))?;
        let offset = u32::try_from(offset)
            .map_err(|_| UtxoError::Malformed("validation delta data offset"))?;
        let data_before = data.len();
        if let Some(utxo) = &update.utxo {
            utxo.encode_into(&mut data)?;
        }
        let length = u32::try_from(data.len() - data_before)
            .map_err(|_| UtxoError::Malformed("validation delta UTXO length"))?;
        encoded.extend_from_slice(&offset.to_le_bytes());
        encoded.extend_from_slice(&length.to_le_bytes());
    }
    encoded.extend_from_slice(&data);
    if encoded.len() > MAX_VALIDATION_DELTA_RECORD_BYTES {
        return Err(UtxoError::Malformed("validation delta record too large"));
    }
    Ok(encoded)
}

fn fold_validation_updates(
    spent: &[OutPointKey],
    created: Vec<(OutPointKey, Utxo)>,
) -> ValidationDeltaUpdates {
    let mut updates = Vec::with_capacity(spent.len().saturating_add(created.len()));
    let mut spent_rows = spent.iter().copied().peekable();
    let mut created_rows = created.into_iter().peekable();
    while spent_rows.peek().is_some() || created_rows.peek().is_some() {
        match (spent_rows.peek(), created_rows.peek()) {
            (Some(spent), Some((created, _))) if spent < created => {
                updates.push((
                    spent_rows.next().expect("peeked spent row"),
                    ValidationUpdate {
                        spent_in_batch: true,
                        utxo: None,
                    },
                ));
            }
            (Some(spent), Some((created, _))) if spent == created => {
                let outpoint = spent_rows.next().expect("peeked spent row");
                let (_, utxo) = created_rows.next().expect("peeked created row");
                updates.push((
                    outpoint,
                    ValidationUpdate {
                        spent_in_batch: true,
                        utxo: Some(utxo),
                    },
                ));
            }
            (_, Some(_)) => {
                let (outpoint, utxo) = created_rows.next().expect("peeked created row");
                updates.push((
                    outpoint,
                    ValidationUpdate {
                        spent_in_batch: false,
                        utxo: Some(utxo),
                    },
                ));
            }
            (Some(_), None) => {
                updates.push((
                    spent_rows.next().expect("peeked spent row"),
                    ValidationUpdate {
                        spent_in_batch: true,
                        utxo: None,
                    },
                ));
            }
            (None, None) => break,
        }
    }
    updates
}

fn validation_delta_header(encoded: &[u8]) -> Result<(u64, usize, usize), UtxoError> {
    if encoded.len() < VALIDATION_DELTA_HEADER_BYTES
        || encoded.len() > MAX_VALIDATION_DELTA_RECORD_BYTES
    {
        return Err(UtxoError::Malformed("validation delta record length"));
    }
    if encoded[..4] != VALIDATION_DELTA_MAGIC {
        return Err(UtxoError::Malformed("validation delta format"));
    }
    let utxo_count = u64::from_le_bytes(
        encoded[4..12]
            .try_into()
            .expect("eight-byte validation UTXO count"),
    );
    let count = u32::from_le_bytes(
        encoded[12..VALIDATION_DELTA_HEADER_BYTES]
            .try_into()
            .expect("four-byte validation delta prefix"),
    );
    let count = usize::try_from(count).expect("u32 fits usize");
    let data_start = VALIDATION_DELTA_HEADER_BYTES
        .checked_add(
            count
                .checked_mul(VALIDATION_DELTA_INDEX_BYTES)
                .ok_or(UtxoError::Malformed("validation delta index overflow"))?,
        )
        .filter(|data_start| *data_start <= encoded.len())
        .ok_or(UtxoError::Malformed(
            "validation delta update count exceeds record",
        ))?;
    Ok((utxo_count, count, data_start))
}

fn validation_delta_index_entry(
    encoded: &[u8],
    index: usize,
) -> Result<(OutPointKey, u8, usize, usize), UtxoError> {
    let start = VALIDATION_DELTA_HEADER_BYTES
        .checked_add(
            index
                .checked_mul(VALIDATION_DELTA_INDEX_BYTES)
                .ok_or(UtxoError::Malformed("validation delta index overflow"))?,
        )
        .ok_or(UtxoError::Malformed("validation delta index overflow"))?;
    let end = start
        .checked_add(VALIDATION_DELTA_INDEX_BYTES)
        .filter(|end| *end <= encoded.len())
        .ok_or(UtxoError::Malformed("truncated validation delta index"))?;
    let entry = &encoded[start..end];
    let outpoint = OutPointKey::from_bytes(&entry[..36])?;
    let state = entry[36];
    let offset = u32::from_le_bytes(entry[37..41].try_into().expect("four-byte delta offset"));
    let length = u32::from_le_bytes(entry[41..45].try_into().expect("four-byte delta length"));
    Ok((
        outpoint,
        state,
        usize::try_from(offset).expect("u32 fits usize"),
        usize::try_from(length).expect("u32 fits usize"),
    ))
}

fn inspect_validation_delta(
    encoded: &[u8],
    mut aggregate_bloom: Option<&mut ValidationBloom>,
) -> Result<(u64, usize, ValidationBloom), UtxoError> {
    let (utxo_count, count, data_start) = validation_delta_header(encoded)?;
    let mut bloom = ValidationBloom::with_update_count(count)?;
    let mut previous = None;
    let mut expected_offset = data_start;
    for index in 0..count {
        let (outpoint, state, offset, length) = validation_delta_index_entry(encoded, index)?;
        if previous.is_some_and(|previous| previous >= outpoint) {
            return Err(UtxoError::Malformed(
                "validation delta outpoints are not strictly ordered",
            ));
        }
        previous = Some(outpoint);
        bloom.insert(outpoint);
        if let Some(aggregate_bloom) = &mut aggregate_bloom {
            aggregate_bloom.insert(outpoint);
        }
        if state > 3 {
            return Err(UtxoError::Malformed("unknown validation delta state"));
        }
        if offset != expected_offset || (state & 1 == 0 && length != 0) {
            return Err(UtxoError::Malformed(
                "non-canonical validation delta data index",
            ));
        }
        let end = offset
            .checked_add(length)
            .filter(|end| *end <= encoded.len())
            .ok_or(UtxoError::Malformed("truncated validation delta UTXO"))?;
        if state & 1 != 0 {
            Utxo::validate_encoded(&encoded[offset..end])?;
        }
        expected_offset = end;
    }
    if expected_offset != encoded.len() {
        return Err(UtxoError::Malformed("trailing validation delta bytes"));
    }
    Ok((utxo_count, count, bloom))
}

fn validation_delta_lookup(
    encoded: &[u8],
    outpoint: OutPointKey,
) -> Result<Option<ValidationUpdate>, UtxoError> {
    let (_, count, _) = validation_delta_header(encoded)?;
    let mut left = 0_usize;
    let mut right = count;
    while left < right {
        let middle = left + (right - left) / 2;
        let (candidate, state, offset, length) = validation_delta_index_entry(encoded, middle)?;
        match candidate.cmp(&outpoint) {
            std::cmp::Ordering::Less => left = middle + 1,
            std::cmp::Ordering::Greater => right = middle,
            std::cmp::Ordering::Equal => {
                let end = offset
                    .checked_add(length)
                    .filter(|end| *end <= encoded.len())
                    .ok_or(UtxoError::Malformed("truncated validation delta UTXO"))?;
                let utxo = (state & 1 != 0)
                    .then(|| Utxo::decode(&encoded[offset..end]))
                    .transpose()?;
                return Ok(Some(ValidationUpdate {
                    spent_in_batch: state & 2 != 0,
                    utxo,
                }));
            }
        }
    }
    Ok(None)
}

fn decode_validation_delta(encoded: &[u8]) -> Result<(u64, ValidationDeltaUpdates), UtxoError> {
    let (utxo_count, count, _) = inspect_validation_delta(encoded, None)?;
    let mut updates = Vec::with_capacity(count);
    for index in 0..count {
        let (outpoint, state, offset, length) = validation_delta_index_entry(encoded, index)?;
        let end = offset + length;
        let utxo = (state & 1 != 0)
            .then(|| Utxo::decode(&encoded[offset..end]))
            .transpose()?;
        updates.push((
            outpoint,
            ValidationUpdate {
                spent_in_batch: state & 2 != 0,
                utxo,
            },
        ));
    }
    Ok((utxo_count, updates))
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
        let mut database = catch_unwind(AssertUnwindSafe(|| {
            Database::builder()
                .set_cache_size(options.cache_size_bytes)
                .create(path)
        }))
        .map_err(|_| ChainStoreError::Damaged)??;
        if !options.retain_block_undo && clear_block_undos_database(&database)? {
            database.compact()?;
        }
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
        if options.validation_delta_journal && options.retain_block_undo {
            return Err(ChainStoreError::ValidationDeltaRetainsUndo);
        }
        let utxos = RedbUtxoStore::from_database(Arc::clone(&db))?;
        if !execution_metadata_exists(&db)? {
            let stats = utxos.tier_stats()?;
            if stats.hot != 0 || stats.cold != 0 {
                return Err(ChainStoreError::LegacyLayout);
            }
        }
        let undos = RedbUndoStore::from_database(Arc::clone(&db))?;
        let execution = RedbExecutionStore::from_database(Arc::clone(&db), network)?;
        let validation_journal = {
            let transaction = db.begin_write()?;
            {
                let _deltas = transaction.open_table(VALIDATION_DELTA_TABLE)?;
            }
            transaction.commit()?;
            let transaction = db.begin_read()?;
            let deltas = transaction.open_table(VALIDATION_DELTA_TABLE)?;
            if !options.validation_delta_journal && !deltas.is_empty()? {
                return Err(ChainStoreError::ValidationDeltaModeRequired);
            }
            if options.validation_delta_journal {
                let base_stats = utxos.tier_stats()?;
                let mut journal = ValidationJournal {
                    rows: Vec::new(),
                    groups: Vec::new(),
                    utxo_count: base_stats
                        .hot
                        .checked_add(base_stats.cold)
                        .ok_or(UtxoError::Malformed("validation UTXO count overflow"))?,
                };
                let mut previous_height = None;
                for row in deltas.iter()? {
                    let (height, encoded) = row?;
                    let height = height.value();
                    if previous_height.is_some_and(|previous| previous >= height) {
                        return Err(UtxoError::Malformed(
                            "validation delta heights are not ordered",
                        )
                        .into());
                    }
                    previous_height = Some(height);
                    if journal.rows.len() % VALIDATION_ROWS_PER_BLOOM_GROUP == 0 {
                        journal.groups.push(ValidationBloom::with_update_count(
                            VALIDATION_GROUP_BLOOM_UPDATES,
                        )?);
                    }
                    let aggregate = journal
                        .groups
                        .last_mut()
                        .expect("validation row has a bloom group");
                    let (utxo_count, _, bloom) =
                        inspect_validation_delta(encoded.value(), Some(aggregate))?;
                    journal.utxo_count = utxo_count;
                    journal.rows.push(ValidationJournalRow { height, bloom });
                }
                if let Some(last_delta_height) = previous_height {
                    let execution_height = execution.tip()?.height;
                    if last_delta_height != execution_height {
                        return Err(UtxoError::Malformed(
                            "validation delta tip does not match execution tip",
                        )
                        .into());
                    }
                }
                Some(Mutex::new(journal))
            } else {
                None
            }
        };
        Ok(Self {
            db,
            utxos,
            undos,
            execution,
            options,
            validation_journal,
            write_guard: Mutex::new(()),
        })
    }

    /// Whether this chainstate retains the records required for reorganization.
    #[must_use]
    pub(crate) const fn retains_block_undo(&self) -> bool {
        self.options.retain_block_undo
    }

    /// Read-only access to retained block undo records.
    pub fn undos(&self) -> &RedbUndoStore {
        &self.undos
    }

    /// Read/write access to execution metadata outside block transitions.
    pub fn execution(&self) -> &RedbExecutionStore {
        &self.execution
    }

    /// Folds every validation-journal update into the base UTXO tables.
    ///
    /// The journal, base tables, and execution metadata share this database,
    /// so either the complete materialized state or the complete journal-backed
    /// state survives a crash. The store remains in journal mode afterward.
    pub fn materialize_validation_deltas(&self) -> Result<u64, ChainStoreError> {
        let Some(validation_journal) = &self.validation_journal else {
            return Ok(0);
        };
        let _guard = self.lock();
        let mut journal = validation_journal
            .lock()
            .expect("validation journal lock not poisoned");
        if journal.rows.is_empty() {
            return Ok(0);
        }
        let updates = {
            let transaction = self.db.begin_read()?;
            let deltas = transaction.open_table(VALIDATION_DELTA_TABLE)?;
            let mut updates: AHashMap<OutPointKey, ValidationUpdate> = AHashMap::new();
            for row in &journal.rows {
                let encoded = deltas
                    .get(row.height)?
                    .ok_or(UtxoError::Malformed("missing validation delta row"))?;
                for (outpoint, update) in decode_validation_delta(encoded.value())?.1 {
                    if let Some(current) = updates.get_mut(&outpoint) {
                        current.utxo = update.utxo;
                    } else {
                        updates.insert(outpoint, update);
                    }
                }
            }
            updates
        };
        let mut spent = Vec::new();
        let mut created = Vec::new();
        let mut ordered = updates.iter().collect::<Vec<_>>();
        ordered.sort_unstable_by_key(|(outpoint, _)| **outpoint);
        for (outpoint, update) in ordered {
            if update.spent_in_batch {
                spent.push(*outpoint);
            }
            if let Some(utxo) = &update.utxo {
                created.push((*outpoint, utxo.clone()));
            }
        }
        let mut transaction = self.db.begin_write()?;
        self.configure(&mut transaction);
        apply_validated_changes_transaction(&transaction, &spent, &created)?;
        {
            let mut deltas = transaction.open_table(VALIDATION_DELTA_TABLE)?;
            deltas.retain(|_, _| false)?;
        }
        transaction.commit()?;
        let count = u64::try_from(updates.len()).expect("usize fits u64");
        journal.rows.clear();
        journal.groups.clear();
        Ok(count)
    }

    /// Returns one sorted, cursor-based page from the complete hot/cold UTXO set.
    pub fn utxo_snapshot_page(
        &self,
        after: Option<OutPointKey>,
        limit: usize,
    ) -> Result<Vec<(OutPointKey, Utxo)>, UtxoError> {
        if self.validation_journal.is_some() {
            return Ok(self
                .snapshot_entries()?
                .into_iter()
                .filter(|(outpoint, _)| after.is_none_or(|after| *outpoint > after))
                .take(limit)
                .collect());
        }
        self.utxos.snapshot_page(after, limit)
    }

    fn snapshot_content_identity(&self) -> Result<(u64, u64, [u8; 32]), UtxoError> {
        if self.validation_journal.is_none() {
            return self.utxos.snapshot_content_identity();
        }
        let entries = self.snapshot_entries()?;
        let mut records_bytes = 0_u64;
        let mut digest = Sha256::new();
        for (outpoint, utxo) in &entries {
            let encoded = utxo.encode()?;
            records_bytes = records_bytes
                .checked_add(
                    u64::try_from(outpoint.as_bytes().len() + encoded.len())
                        .expect("UTXO record length fits u64"),
                )
                .ok_or(UtxoError::Malformed("snapshot records length overflow"))?;
            update_utxo_set_digest(&mut digest, outpoint.as_bytes(), utxo);
        }
        Ok((
            u64::try_from(entries.len()).expect("usize fits u64"),
            records_bytes,
            digest.finalize().into(),
        ))
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
            validation.snapshot_content_identity()?;
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
        if self.validation_journal.is_some() {
            return Err(UtxoError::Malformed(
                "validation journal requires atomic checkpoint commits",
            )
            .into());
        }
        let _guard = self.lock();
        let mut transaction = self.db.begin_write()?;
        self.configure(&mut transaction);
        let undo = apply_with_undo_transaction(&transaction, spent, created)?;
        if self.options.retain_block_undo {
            insert_undo_transaction(&transaction, next.hash, transaction_undos)?;
        }
        advance_transaction(&transaction, expected_parent, next)?;
        transaction.commit()?;
        Ok(undo)
    }

    fn commit_validation_batch(
        &self,
        validation_journal: &Mutex<ValidationJournal>,
        transitions: &[ConnectTransition],
        spent: &[OutPointKey],
        created: Vec<(OutPointKey, Utxo)>,
    ) -> Result<(), ChainStoreError> {
        let created_count = u64::try_from(created.len()).expect("usize fits u64");
        let updates = fold_validation_updates(spent, created);
        let final_height = transitions
            .last()
            .expect("non-empty transitions have a final height")
            .next
            .height;
        let _guard = self.lock();
        let mut journal = validation_journal
            .lock()
            .expect("validation journal lock not poisoned");
        let spent_count = u64::try_from(spent.len()).expect("usize fits u64");
        let next_utxo_count = journal
            .utxo_count
            .checked_sub(spent_count)
            .and_then(|count| count.checked_add(created_count))
            .ok_or(UtxoError::Malformed("validation UTXO count overflow"))?;
        let encoded = encode_validation_delta(
            updates.iter().map(|(outpoint, update)| (outpoint, update)),
            next_utxo_count,
        )?;
        let mut bloom = ValidationBloom::with_update_count(updates.len())?;
        let starts_group = journal.rows.len() % VALIDATION_ROWS_PER_BLOOM_GROUP == 0;
        let mut new_group = starts_group
            .then(|| ValidationBloom::with_update_count(VALIDATION_GROUP_BLOOM_UPDATES))
            .transpose()?;
        for outpoint in updates.iter().map(|(outpoint, _)| *outpoint) {
            bloom.insert(outpoint);
            if let Some(new_group) = &mut new_group {
                new_group.insert(outpoint);
            }
        }
        let mut transaction = self.db.begin_write()?;
        self.configure(&mut transaction);
        {
            let mut deltas = transaction.open_table(VALIDATION_DELTA_TABLE)?;
            if deltas.get(final_height)?.is_some() {
                return Err(UtxoError::Malformed("duplicate validation delta checkpoint").into());
            }
            deltas.insert(final_height, encoded.as_slice())?;
        }
        for transition in transitions {
            advance_transaction(&transaction, transition.expected_parent, transition.next)?;
        }
        transaction.commit()?;
        if let Some(new_group) = new_group {
            journal.groups.push(new_group);
        } else {
            let group = journal
                .groups
                .last_mut()
                .expect("validation row has a bloom group");
            for outpoint in updates.iter().map(|(outpoint, _)| *outpoint) {
                group.insert(outpoint);
            }
        }
        journal.rows.push(ValidationJournalRow {
            height: final_height,
            bloom,
        });
        journal.utxo_count = next_utxo_count;
        Ok(())
    }

    /// Atomically persists a contiguous group of validated IBD blocks.
    ///
    /// Undo remains block-addressable and the stored execution tip advances
    /// through every block inside the transaction, but no prefix becomes
    /// visible unless the complete checkpoint commits.
    pub(crate) fn commit_connect_batch(
        &self,
        transitions: &[ConnectTransition],
    ) -> Result<(), ChainStoreError> {
        if transitions.is_empty() {
            return Ok(());
        }
        let mut spent = AHashSet::new();
        let mut created = AHashMap::new();
        for transition in transitions {
            for key in &transition.spent {
                if created.remove(key).is_none() && !spent.insert(*key) {
                    return Err(UtxoError::DuplicateSpend(*key).into());
                }
            }
            for (key, utxo) in &transition.created {
                if created.insert(*key, utxo.clone()).is_some() {
                    return Err(UtxoError::Duplicate(*key).into());
                }
            }
        }
        let mut spent = spent.into_iter().collect::<Vec<_>>();
        spent.sort_unstable();
        let mut created = created.into_iter().collect::<Vec<_>>();
        created.sort_unstable_by_key(|(outpoint, _)| *outpoint);
        if let Some(validation_journal) = &self.validation_journal {
            return self.commit_validation_batch(validation_journal, transitions, &spent, created);
        }
        let _guard = self.lock();
        let mut transaction = self.db.begin_write()?;
        self.configure(&mut transaction);
        apply_validated_changes_transaction(&transaction, &spent, &created)?;
        if self.options.retain_block_undo {
            for transition in transitions {
                insert_undo_transaction(
                    &transaction,
                    transition.next.hash,
                    &transition.transaction_undos,
                )?;
            }
        }
        for transition in transitions {
            advance_transaction(&transaction, transition.expected_parent, transition.next)?;
        }
        transaction.commit()?;
        Ok(())
    }

    /// Atomically applies a reverse UTXO transition, removes undo, and rewinds the tip.
    pub fn commit_disconnect(
        &self,
        expected_current: ExecutionTip,
        parent: ExecutionTip,
        spent: &[OutPointKey],
        created: &[(OutPointKey, Utxo)],
    ) -> Result<UtxoUndo, ChainStoreError> {
        if self.validation_journal.is_some() {
            return Err(UtxoError::Malformed("validation journal cannot disconnect blocks").into());
        }
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
        if let Some(journal) = &self.validation_journal {
            let journal = journal
                .lock()
                .expect("validation journal lock not poisoned");
            let transaction = self.db.begin_read()?;
            let deltas = transaction.open_table(VALIDATION_DELTA_TABLE)?;
            for (group_index, group) in journal.groups.iter().enumerate().rev() {
                if !group.might_contain(outpoint) {
                    continue;
                }
                let start = group_index * VALIDATION_ROWS_PER_BLOOM_GROUP;
                let end = (start + VALIDATION_ROWS_PER_BLOOM_GROUP).min(journal.rows.len());
                for row in journal.rows[start..end].iter().rev() {
                    if !row.bloom.might_contain(outpoint) {
                        continue;
                    }
                    let encoded = deltas
                        .get(row.height)?
                        .ok_or(UtxoError::Malformed("missing validation delta row"))?;
                    if let Some(update) = validation_delta_lookup(encoded.value(), outpoint)? {
                        return Ok(update.utxo);
                    }
                }
            }
        }
        self.utxos.get(outpoint)
    }

    fn get_many(
        &self,
        outpoints: &[OutPointKey],
    ) -> Result<Vec<(OutPointKey, Option<Utxo>)>, UtxoError> {
        let Some(journal) = &self.validation_journal else {
            return self.utxos.get_many(outpoints);
        };
        let journal = journal
            .lock()
            .expect("validation journal lock not poisoned");
        let transaction = self.db.begin_read()?;
        let deltas = transaction.open_table(VALIDATION_DELTA_TABLE)?;
        let mut results = vec![None; outpoints.len()];
        let mut unresolved = (0..outpoints.len()).collect::<Vec<_>>();
        for (group_index, group) in journal.groups.iter().enumerate().rev() {
            if unresolved.is_empty() {
                break;
            }
            let mut group_unresolved = Vec::new();
            let mut next_unresolved = Vec::with_capacity(unresolved.len());
            for index in unresolved {
                let outpoint = outpoints[index];
                if group.might_contain(outpoint) {
                    group_unresolved.push(index);
                } else {
                    next_unresolved.push(index);
                }
            }
            let start = group_index * VALIDATION_ROWS_PER_BLOOM_GROUP;
            let end = (start + VALIDATION_ROWS_PER_BLOOM_GROUP).min(journal.rows.len());
            for row in journal.rows[start..end].iter().rev() {
                if group_unresolved.is_empty() {
                    break;
                }
                let encoded = deltas
                    .get(row.height)?
                    .ok_or(UtxoError::Malformed("missing validation delta row"))?;
                let mut row_unresolved = Vec::with_capacity(group_unresolved.len());
                for index in group_unresolved {
                    let outpoint = outpoints[index];
                    if row.bloom.might_contain(outpoint) {
                        if let Some(update) = validation_delta_lookup(encoded.value(), outpoint)? {
                            results[index] = Some((outpoint, update.utxo));
                            continue;
                        }
                    }
                    row_unresolved.push(index);
                }
                group_unresolved = row_unresolved;
            }
            next_unresolved.extend(group_unresolved);
            unresolved = next_unresolved;
        }
        drop(deltas);
        drop(transaction);
        drop(journal);
        let unresolved_outpoints = unresolved
            .iter()
            .map(|index| outpoints[*index])
            .collect::<Vec<_>>();
        for (index, result) in unresolved
            .into_iter()
            .zip(self.utxos.get_many(&unresolved_outpoints)?)
        {
            results[index] = Some(result);
        }
        Ok(results
            .into_iter()
            .map(|result| result.expect("every UTXO prefetch result is populated"))
            .collect())
    }

    fn apply(
        &self,
        spent: &[OutPointKey],
        created: &[(OutPointKey, Utxo)],
    ) -> Result<(), UtxoError> {
        if self.validation_journal.is_some() {
            return Err(UtxoError::Malformed(
                "validation journal requires atomic checkpoint commits",
            ));
        }
        self.utxos.apply(spent, created)
    }

    fn apply_with_undo(
        &self,
        spent: &[OutPointKey],
        created: &[(OutPointKey, Utxo)],
    ) -> Result<UtxoUndo, UtxoError> {
        if self.validation_journal.is_some() {
            return Err(UtxoError::Malformed(
                "validation journal requires atomic checkpoint commits",
            ));
        }
        self.utxos.apply_with_undo(spent, created)
    }

    fn undo(&self, undo: &UtxoUndo, now: u64, hot_window_secs: u64) -> Result<(), UtxoError> {
        if self.validation_journal.is_some() {
            return Err(UtxoError::Malformed(
                "validation journal cannot disconnect blocks",
            ));
        }
        self.utxos.undo(undo, now, hot_window_secs)
    }

    fn age_to_cold(&self, now: u64, hot_window_secs: u64) -> Result<u64, UtxoError> {
        if self.validation_journal.is_some() {
            return Ok(0);
        }
        self.utxos.age_to_cold(now, hot_window_secs)
    }

    fn snapshot_entries(&self) -> Result<BTreeMap<OutPointKey, Utxo>, UtxoError> {
        let mut entries = self.utxos.snapshot_entries()?;
        if let Some(journal) = &self.validation_journal {
            let journal = journal
                .lock()
                .expect("validation journal lock not poisoned");
            let transaction = self.db.begin_read()?;
            let deltas = transaction.open_table(VALIDATION_DELTA_TABLE)?;
            for row in &journal.rows {
                let encoded = deltas
                    .get(row.height)?
                    .ok_or(UtxoError::Malformed("missing validation delta row"))?;
                for (outpoint, update) in decode_validation_delta(encoded.value())?.1 {
                    match update.utxo {
                        Some(utxo) => {
                            entries.insert(outpoint, utxo);
                        }
                        None => {
                            entries.remove(&outpoint);
                        }
                    }
                }
            }
        }
        Ok(entries)
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
        if self.validation_journal.is_none() {
            return self.utxos.tier_stats();
        }
        let count = self
            .validation_journal
            .as_ref()
            .expect("checked validation journal")
            .lock()
            .expect("validation journal lock not poisoned")
            .utxo_count;
        Ok(TierStats {
            hot: count,
            cold: 0,
        })
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
            ChainStoreOptions {
                quick_repair: true,
                ..ChainStoreOptions::default()
            },
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
    fn batch_folds_intermediate_outputs_into_one_atomic_net_change() {
        let directory = TempDir::new().unwrap();
        let store =
            RedbChainStore::open(directory.path().join("chainstate.redb"), Network::Regtest)
                .unwrap();
        let genesis = store.execution().tip().unwrap();
        store.apply(&[], &[(key(1), coin(10))]).unwrap();
        let first_hash = BlockHash::from_byte_array([31; 32]);
        let second_hash = BlockHash::from_byte_array([32; 32]);
        let transitions = [
            ConnectTransition {
                expected_parent: genesis.hash,
                next: ExecutionTip {
                    height: 1,
                    hash: first_hash,
                },
                spent: vec![key(1)],
                created: vec![(key(2), coin(9))],
                transaction_undos: vec![],
            },
            ConnectTransition {
                expected_parent: first_hash,
                next: ExecutionTip {
                    height: 2,
                    hash: second_hash,
                },
                spent: vec![key(2)],
                created: vec![(key(3), coin(8))],
                transaction_undos: vec![],
            },
        ];

        store.commit_connect_batch(&transitions).unwrap();
        assert_eq!(
            store.execution().tip().unwrap(),
            ExecutionTip {
                height: 2,
                hash: second_hash,
            }
        );
        assert!(store.get(key(1)).unwrap().is_none());
        assert!(store.get(key(2)).unwrap().is_none());
        assert_eq!(store.get(key(3)).unwrap(), Some(coin(8)));
        assert!(store.undos().get(first_hash).unwrap().is_some());
        assert!(store.undos().get(second_hash).unwrap().is_some());
    }

    #[test]
    fn batch_atomically_replaces_a_spent_outpoint() {
        let directory = TempDir::new().unwrap();
        let store =
            RedbChainStore::open(directory.path().join("chainstate.redb"), Network::Regtest)
                .unwrap();
        let genesis = store.execution().tip().unwrap();
        store.apply(&[], &[(key(1), coin(10))]).unwrap();
        let replacement = Utxo {
            value_sats: 99,
            ..coin(20)
        };
        store
            .commit_connect_batch(&[ConnectTransition {
                expected_parent: genesis.hash,
                next: ExecutionTip {
                    height: 1,
                    hash: BlockHash::from_byte_array([40; 32]),
                },
                spent: vec![key(1)],
                created: vec![(key(1), replacement.clone())],
                transaction_undos: vec![],
            }])
            .unwrap();
        assert_eq!(store.get(key(1)).unwrap(), Some(replacement));
    }

    #[test]
    fn validation_delta_encoding_is_canonical_and_strict() {
        let updates = BTreeMap::from([
            (
                key(1),
                ValidationUpdate {
                    spent_in_batch: true,
                    utxo: None,
                },
            ),
            (
                key(2),
                ValidationUpdate {
                    spent_in_batch: false,
                    utxo: Some(coin(20)),
                },
            ),
            (
                key(3),
                ValidationUpdate {
                    spent_in_batch: true,
                    utxo: Some(coin(30)),
                },
            ),
            (
                key(4),
                ValidationUpdate {
                    spent_in_batch: false,
                    utxo: None,
                },
            ),
        ]);
        let encoded = encode_validation_delta(updates.iter(), 77).unwrap();
        let (utxo_count, decoded) = decode_validation_delta(&encoded).unwrap();
        assert_eq!(utxo_count, 77);
        assert_eq!(decoded, updates.into_iter().collect::<Vec<_>>());

        let mut trailing = encoded.clone();
        trailing.push(0);
        assert!(matches!(
            decode_validation_delta(&trailing),
            Err(UtxoError::Malformed("trailing validation delta bytes"))
        ));

        let mut unknown_tag = encoded.clone();
        unknown_tag[16 + 36] = 4;
        assert!(matches!(
            decode_validation_delta(&unknown_tag),
            Err(UtxoError::Malformed("unknown validation delta state"))
        ));

        let tombstone = ValidationUpdate {
            spent_in_batch: true,
            utxo: None,
        };
        let unordered_updates = BTreeMap::from([(key(1), tombstone.clone()), (key(2), tombstone)]);
        let mut unordered = encode_validation_delta(unordered_updates.iter(), 9).unwrap();
        let first = unordered[16..16 + VALIDATION_DELTA_INDEX_BYTES].to_vec();
        let second = unordered
            [16 + VALIDATION_DELTA_INDEX_BYTES..16 + 2 * VALIDATION_DELTA_INDEX_BYTES]
            .to_vec();
        unordered[16..16 + VALIDATION_DELTA_INDEX_BYTES].copy_from_slice(&second);
        unordered[16 + VALIDATION_DELTA_INDEX_BYTES..16 + 2 * VALIDATION_DELTA_INDEX_BYTES]
            .copy_from_slice(&first);
        assert!(matches!(
            decode_validation_delta(&unordered),
            Err(UtxoError::Malformed(
                "validation delta outpoints are not strictly ordered"
            ))
        ));
    }

    #[test]
    fn validation_delta_journal_survives_restart_and_materializes_atomically() {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("chainstate.redb");
        let base_options = ChainStoreOptions {
            retain_block_undo: false,
            ..ChainStoreOptions::default()
        };
        let store =
            RedbChainStore::open_with_options(&path, Network::Regtest, base_options).unwrap();
        let genesis = store.execution().tip().unwrap();
        store.apply(&[], &[(key(1), coin(10))]).unwrap();
        drop(store);

        let journal_options = ChainStoreOptions {
            validation_delta_journal: true,
            ..base_options
        };
        let store =
            RedbChainStore::open_with_options(&path, Network::Regtest, journal_options).unwrap();
        let first_hash = BlockHash::from_byte_array([51; 32]);
        let second_hash = BlockHash::from_byte_array([52; 32]);
        store
            .commit_connect_batch(&[
                ConnectTransition {
                    expected_parent: genesis.hash,
                    next: ExecutionTip {
                        height: 1,
                        hash: first_hash,
                    },
                    spent: vec![key(1)],
                    created: vec![(key(2), coin(9))],
                    transaction_undos: vec![],
                },
                ConnectTransition {
                    expected_parent: first_hash,
                    next: ExecutionTip {
                        height: 2,
                        hash: second_hash,
                    },
                    spent: vec![key(2)],
                    created: vec![(key(3), coin(8))],
                    transaction_undos: vec![],
                },
            ])
            .unwrap();
        let third = ExecutionTip {
            height: 3,
            hash: BlockHash::from_byte_array([53; 32]),
        };
        store
            .commit_connect_batch(&[ConnectTransition {
                expected_parent: second_hash,
                next: third,
                spent: vec![key(3)],
                created: vec![(key(4), coin(7))],
                transaction_undos: vec![],
            }])
            .unwrap();
        assert_eq!(
            store.get_many(&[key(1), key(2), key(3), key(4)]).unwrap(),
            vec![
                (key(1), None),
                (key(2), None),
                (key(3), None),
                (key(4), Some(coin(7))),
            ]
        );
        assert_eq!(store.tier_stats().unwrap(), TierStats { hot: 1, cold: 0 });
        assert_eq!(store.execution().tip().unwrap(), third);
        drop(store);

        assert!(matches!(
            RedbChainStore::open_with_options(&path, Network::Regtest, base_options),
            Err(ChainStoreError::ValidationDeltaModeRequired)
        ));

        let store =
            RedbChainStore::open_with_options(&path, Network::Regtest, journal_options).unwrap();
        assert_eq!(store.execution().tip().unwrap(), third);
        assert_eq!(
            store.snapshot_entries().unwrap(),
            BTreeMap::from([(key(4), coin(7))])
        );
        assert_eq!(store.tier_stats().unwrap(), TierStats { hot: 1, cold: 0 });
        assert_eq!(store.materialize_validation_deltas().unwrap(), 3);
        assert_eq!(store.materialize_validation_deltas().unwrap(), 0);
        assert_eq!(
            store.snapshot_entries().unwrap(),
            BTreeMap::from([(key(4), coin(7))])
        );
        drop(store);

        let reopened =
            RedbChainStore::open_with_options(&path, Network::Regtest, base_options).unwrap();
        assert_eq!(reopened.execution().tip().unwrap(), third);
        assert_eq!(
            reopened.snapshot_entries().unwrap(),
            BTreeMap::from([(key(4), coin(7))])
        );
        assert_eq!(
            reopened.tier_stats().unwrap(),
            TierStats { hot: 1, cold: 0 }
        );
    }

    #[test]
    fn validation_delta_materialization_preserves_base_replacement() {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("chainstate.redb");
        let base_options = ChainStoreOptions {
            retain_block_undo: false,
            ..ChainStoreOptions::default()
        };
        let store =
            RedbChainStore::open_with_options(&path, Network::Regtest, base_options).unwrap();
        let genesis = store.execution().tip().unwrap();
        store.apply(&[], &[(key(1), coin(10))]).unwrap();
        drop(store);

        let store = RedbChainStore::open_with_options(
            &path,
            Network::Regtest,
            ChainStoreOptions {
                validation_delta_journal: true,
                ..base_options
            },
        )
        .unwrap();
        let replacement = coin(9);
        store
            .commit_connect_batch(&[ConnectTransition {
                expected_parent: genesis.hash,
                next: ExecutionTip {
                    height: 1,
                    hash: BlockHash::from_byte_array([55; 32]),
                },
                spent: vec![key(1)],
                created: vec![(key(1), replacement.clone())],
                transaction_undos: vec![],
            }])
            .unwrap();
        assert_eq!(store.materialize_validation_deltas().unwrap(), 1);
        drop(store);

        let reopened =
            RedbChainStore::open_with_options(&path, Network::Regtest, base_options).unwrap();
        assert_eq!(reopened.get(key(1)).unwrap(), Some(replacement));
    }

    #[test]
    fn validation_delta_groups_preserve_newest_and_older_row_lookups() {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("chainstate.redb");
        let base_options = ChainStoreOptions {
            retain_block_undo: false,
            ..ChainStoreOptions::default()
        };
        let store =
            RedbChainStore::open_with_options(&path, Network::Regtest, base_options).unwrap();
        let genesis = store.execution().tip().unwrap();
        store
            .apply(&[], &[(key(1), coin(10)), (key(3), coin(30))])
            .unwrap();
        drop(store);

        let journal_options = ChainStoreOptions {
            validation_delta_journal: true,
            ..base_options
        };
        let store =
            RedbChainStore::open_with_options(&path, Network::Regtest, journal_options).unwrap();
        let mut parent = genesis.hash;
        for height in 1..=18_u32 {
            let next = ExecutionTip {
                height,
                hash: BlockHash::from_byte_array([u8::try_from(height).unwrap(); 32]),
            };
            let mut spent = vec![key(1)];
            let mut created = vec![(key(1), coin(100 + u64::from(height)))];
            if height == 1 {
                spent.push(key(3));
                created.push((key(2), coin(777)));
            }
            store
                .commit_connect_batch(&[ConnectTransition {
                    expected_parent: parent,
                    next,
                    spent,
                    created,
                    transaction_undos: vec![],
                }])
                .unwrap();
            parent = next.hash;
        }
        assert_eq!(
            store.get_many(&[key(1), key(2), key(3), key(4)]).unwrap(),
            vec![
                (key(1), Some(coin(118))),
                (key(2), Some(coin(777))),
                (key(3), None),
                (key(4), None),
            ]
        );
        {
            let journal = store
                .validation_journal
                .as_ref()
                .expect("journal enabled")
                .lock()
                .expect("journal lock");
            assert_eq!(journal.rows.len(), 18);
            assert_eq!(journal.groups.len(), 2);
        }
        drop(store);

        let store =
            RedbChainStore::open_with_options(&path, Network::Regtest, journal_options).unwrap();
        assert_eq!(store.get(key(1)).unwrap(), Some(coin(118)));
        assert_eq!(store.get(key(2)).unwrap(), Some(coin(777)));
        assert!(store.get(key(3)).unwrap().is_none());
        assert_eq!(store.materialize_validation_deltas().unwrap(), 3);
        drop(store);

        let store =
            RedbChainStore::open_with_options(&path, Network::Regtest, base_options).unwrap();
        assert_eq!(
            store.snapshot_entries().unwrap(),
            BTreeMap::from([(key(1), coin(118)), (key(2), coin(777))])
        );
    }

    #[test]
    fn validation_delta_journal_rejects_reorganizing_store() {
        let directory = TempDir::new().unwrap();
        assert!(matches!(
            RedbChainStore::open_with_options(
                directory.path().join("chainstate.redb"),
                Network::Regtest,
                ChainStoreOptions {
                    validation_delta_journal: true,
                    ..ChainStoreOptions::default()
                },
            ),
            Err(ChainStoreError::ValidationDeltaRetainsUndo)
        ));
    }

    #[test]
    fn failed_validation_delta_commit_exposes_neither_tip_nor_utxo() {
        let backend = QuotaBackend {
            bytes: Arc::new(RwLock::new(Vec::new())),
            full: Arc::new(AtomicBool::new(false)),
        };
        let options = ChainStoreOptions {
            retain_block_undo: false,
            validation_delta_journal: true,
            ..ChainStoreOptions::default()
        };
        let database = Arc::new(
            Database::builder()
                .create_with_backend(backend.clone())
                .unwrap(),
        );
        let store = RedbChainStore::from_database(database, Network::Regtest, options).unwrap();
        let genesis = store.execution().tip().unwrap();
        let next = ExecutionTip {
            height: 1,
            hash: BlockHash::from_byte_array([54; 32]),
        };
        backend.full.store(true, Ordering::SeqCst);
        assert!(
            store
                .commit_connect_batch(&[ConnectTransition {
                    expected_parent: genesis.hash,
                    next,
                    spent: vec![],
                    created: vec![(key(1), coin(10))],
                    transaction_undos: vec![],
                }])
                .is_err()
        );
        backend.full.store(false, Ordering::SeqCst);
        drop(store);

        let reopened = RedbChainStore::from_database(
            Arc::new(Database::builder().create_with_backend(backend).unwrap()),
            Network::Regtest,
            options,
        )
        .unwrap();
        assert_eq!(reopened.execution().tip().unwrap(), genesis);
        assert!(reopened.get(key(1)).unwrap().is_none());
        assert!(reopened.snapshot_entries().unwrap().is_empty());
    }

    #[test]
    fn validation_delta_tip_mismatch_is_rejected_on_reopen() {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("chainstate.redb");
        let base_options = ChainStoreOptions {
            retain_block_undo: false,
            ..ChainStoreOptions::default()
        };
        drop(RedbChainStore::open_with_options(&path, Network::Regtest, base_options).unwrap());
        let database = Database::open(&path).unwrap();
        let transaction = database.begin_write().unwrap();
        {
            let mut deltas = transaction.open_table(VALIDATION_DELTA_TABLE).unwrap();
            let delta = BTreeMap::from([(
                key(1),
                ValidationUpdate {
                    spent_in_batch: false,
                    utxo: Some(coin(10)),
                },
            )]);
            let encoded = encode_validation_delta(delta.iter(), 1).unwrap();
            deltas.insert(1, encoded.as_slice()).unwrap();
        }
        transaction.commit().unwrap();
        drop(database);

        assert!(matches!(
            RedbChainStore::open_with_options(
                &path,
                Network::Regtest,
                ChainStoreOptions {
                    validation_delta_journal: true,
                    ..base_options
                },
            ),
            Err(ChainStoreError::Utxo(UtxoError::Malformed(
                "validation delta tip does not match execution tip"
            )))
        ));
    }

    #[test]
    fn validation_only_store_discards_historical_and_new_block_undo() {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("chainstate.redb");
        let store = RedbChainStore::open(&path, Network::Regtest).unwrap();
        let genesis = store.execution().tip().unwrap();
        store.apply(&[], &[(key(1), coin(10))]).unwrap();
        let first = ExecutionTip {
            height: 1,
            hash: BlockHash::from_byte_array([41; 32]),
        };
        store
            .commit_connect(genesis.hash, first, &[key(1)], &[(key(2), coin(9))], &[])
            .unwrap();
        assert!(store.undos().get(first.hash).unwrap().is_some());
        drop(store);

        let store = RedbChainStore::open_with_options(
            &path,
            Network::Regtest,
            ChainStoreOptions {
                retain_block_undo: false,
                ..ChainStoreOptions::default()
            },
        )
        .unwrap();
        assert!(!store.retains_block_undo());
        assert!(store.undos().get(first.hash).unwrap().is_none());
        let second = ExecutionTip {
            height: 2,
            hash: BlockHash::from_byte_array([42; 32]),
        };
        store
            .commit_connect(first.hash, second, &[key(2)], &[(key(3), coin(8))], &[])
            .unwrap();
        assert!(store.undos().get(second.hash).unwrap().is_none());
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
