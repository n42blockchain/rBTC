//! Unified durable storage for active-chain state.

use std::{
    collections::{BTreeMap, BTreeSet},
    panic::{AssertUnwindSafe, catch_unwind},
    path::Path,
    sync::{Arc, Mutex, MutexGuard, OnceLock},
};

use ahash::{AHashMap, AHashSet};
use bitcoin::{BlockHash, Network, hashes::Hash};
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
        HeightRetierProgress, OutPointKey, RedbUtxoStore, TierStats, Utxo, UtxoError, UtxoStore,
        UtxoUndo, apply_validated_changes_transaction, apply_with_undo_transaction,
        insert_snapshot_entries_transaction, tables_empty_transaction, update_utxo_set_digest,
    },
};

fn bulk_commit_guard() -> MutexGuard<'static, ()> {
    static BULK_COMMIT: OnceLock<Mutex<()>> = OnceLock::new();
    BULK_COMMIT
        .get_or_init(|| Mutex::new(()))
        .lock()
        .expect("bulk chainstate commit lock not poisoned")
}

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
const VALIDATION_DELTA_SHARD_TABLE: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("validation_utxo_delta_shards");
const VALIDATION_DELTA_BLOOM_TABLE: TableDefinition<u32, &[u8]> =
    TableDefinition::new("validation_utxo_delta_blooms");
const VALIDATION_GROUP_BLOOM_TABLE: TableDefinition<u32, &[u8]> =
    TableDefinition::new("validation_utxo_group_blooms");
const SPENT_AGE_TABLE: TableDefinition<u32, u64> = TableDefinition::new("spent_output_age_blocks");
const SPENT_AGE_META_TABLE: TableDefinition<u8, u32> =
    TableDefinition::new("spent_output_age_meta");
const SPENT_AGE_START_HEIGHT: u8 = 0;
const SPENT_AGE_END_HEIGHT: u8 = 1;
const MAX_VALIDATION_DELTA_RECORD_BYTES: usize = 512 * 1024 * 1024;
const VALIDATION_DELTA_MAGIC: [u8; 4] = *b"RVD3";
const VALIDATION_SHARDED_DELTA_MAGIC: [u8; 4] = *b"RVD4";
const VALIDATION_COMPACT_SHARDED_DELTA_MAGIC: [u8; 4] = *b"RVD5";
const VALIDATION_DELTA_HEADER_BYTES: usize = 16;
const VALIDATION_DELTA_INDEX_BYTES: usize = 45;
const VALIDATION_RVD4_SHARD_COUNT: usize = 256;
const VALIDATION_DELTA_SHARD_COUNT: usize = 16;
const VALIDATION_DELTA_SHARD_BITMAP_BYTES: usize = VALIDATION_RVD4_SHARD_COUNT / 8;
const VALIDATION_SHARDED_DELTA_HEADER_BYTES: usize = 16 + VALIDATION_DELTA_SHARD_BITMAP_BYTES;
const VALIDATION_COMPACT_SHARDED_DELTA_HEADER_BYTES: usize = 16 + VALIDATION_DELTA_SHARD_COUNT / 8;
const VALIDATION_BLOOM_MAGIC: [u8; 4] = *b"RVB1";
const VALIDATION_BLOOM_HEADER_BYTES: usize = 48;
const VALIDATION_BLOOM_BITS_PER_UPDATE: usize = 10;
const VALIDATION_ROWS_PER_BLOOM_GROUP: usize = 16;
const VALIDATION_GROUP_BLOOM_UPDATES: usize = 16_000_000;
const MIN_PARALLEL_VALIDATION_BLOOM_KEYS: usize = 16_384;

struct ValidationJournal {
    rows: Vec<ValidationJournalRow>,
    groups: Vec<ValidationBloom>,
    legacy_hits: BTreeMap<u32, u64>,
    utxo_count: u64,
}

struct ValidationJournalRow {
    height: u32,
    bloom: ValidationBloom,
}

#[derive(Clone)]
struct ValidationBloom {
    bits: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ValidationUpdate {
    spent_in_batch: bool,
    utxo: Option<Utxo>,
}

type ValidationDeltaUpdates = Vec<(OutPointKey, ValidationUpdate)>;
type EncodedValidationDeltaShards = Vec<(u8, Vec<u8>)>;
struct ValidationShardReadJob {
    height: u32,
    row_index: usize,
    shard: u8,
    candidates: Vec<usize>,
}

type ValidationShardMatch = (usize, usize, OutPointKey, Option<Utxo>);

enum ValidationRowReadPlan {
    Empty,
    Legacy(Vec<usize>),
    Sharded {
        header: ShardedValidationDeltaHeader,
        candidates: Vec<usize>,
    },
}

impl ValidationBloom {
    fn with_update_count(update_count: usize) -> Result<Self, UtxoError> {
        let byte_count = validation_bloom_byte_count(update_count)?;
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

fn validation_bloom_byte_count(update_count: usize) -> Result<usize, UtxoError> {
    update_count
        .checked_mul(VALIDATION_BLOOM_BITS_PER_UPDATE)
        .map(|bits| bits.div_ceil(8).max(1))
        .ok_or(UtxoError::Malformed("validation bloom size overflow"))
}

fn encode_validation_bloom(bloom: &ValidationBloom, utxo_count: u64) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(VALIDATION_BLOOM_HEADER_BYTES + bloom.bits.len());
    encoded.extend_from_slice(&VALIDATION_BLOOM_MAGIC);
    encoded.extend_from_slice(&utxo_count.to_le_bytes());
    encoded.extend_from_slice(
        &u32::try_from(bloom.bits.len())
            .expect("bounded validation bloom byte length fits u32")
            .to_le_bytes(),
    );
    encoded.extend_from_slice(Sha256::digest(&bloom.bits).as_slice());
    encoded.extend_from_slice(&bloom.bits);
    encoded
}

fn decode_validation_bloom(
    encoded: &[u8],
    expected_bytes: usize,
) -> Result<(u64, ValidationBloom), UtxoError> {
    if encoded.len() != VALIDATION_BLOOM_HEADER_BYTES.saturating_add(expected_bytes) {
        return Err(UtxoError::Malformed("validation bloom record length"));
    }
    if encoded[..4] != VALIDATION_BLOOM_MAGIC {
        return Err(UtxoError::Malformed("validation bloom format"));
    }
    let utxo_count = u64::from_le_bytes(
        encoded[4..12]
            .try_into()
            .expect("eight-byte validation bloom UTXO count"),
    );
    let byte_count = u32::from_le_bytes(
        encoded[12..16]
            .try_into()
            .expect("four-byte validation bloom length"),
    );
    if usize::try_from(byte_count).expect("u32 fits usize") != expected_bytes {
        return Err(UtxoError::Malformed("validation bloom bit length"));
    }
    let bits = &encoded[VALIDATION_BLOOM_HEADER_BYTES..];
    if encoded[16..VALIDATION_BLOOM_HEADER_BYTES] != Sha256::digest(bits).as_slice()[..] {
        return Err(UtxoError::Malformed("validation bloom checksum"));
    }
    Ok((
        utxo_count,
        ValidationBloom {
            bits: bits.to_vec(),
        },
    ))
}

fn partition_validation_bloom_matches(
    bloom: &ValidationBloom,
    outpoints: &[OutPointKey],
    indices: Vec<usize>,
) -> (Vec<usize>, Vec<usize>) {
    let available_workers = std::thread::available_parallelism().map_or(1, std::num::NonZero::get);
    let workers = available_workers.min(indices.len().div_ceil(MIN_PARALLEL_VALIDATION_BLOOM_KEYS));
    if workers <= 1 {
        return indices
            .into_iter()
            .partition(|index| bloom.might_contain(outpoints[*index]));
    }
    let chunk_size = indices.len().div_ceil(workers);
    std::thread::scope(|scope| {
        let jobs = indices
            .chunks(chunk_size)
            .map(|chunk| {
                scope.spawn(|| {
                    chunk
                        .iter()
                        .copied()
                        .partition::<Vec<_>, _>(|index| bloom.might_contain(outpoints[*index]))
                })
            })
            .collect::<Vec<_>>();
        let mut matching = Vec::new();
        let mut rejected = Vec::new();
        for job in jobs {
            let (mut job_matching, mut job_rejected) =
                job.join().expect("validation bloom worker must not panic");
            matching.append(&mut job_matching);
            rejected.append(&mut job_rejected);
        }
        (matching, rejected)
    })
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
pub struct ConnectTransition {
    /// Hash the durable tip must currently have.
    pub expected_parent: BlockHash,
    /// Tip after this transition; exactly one block above the parent.
    pub next: ExecutionTip,
    /// Outpoints removed by the block, excluding same-batch creations.
    pub spent: Vec<OutPointKey>,
    /// Coins created by the block and still unspent at the batch end.
    pub created: Vec<(OutPointKey, Utxo)>,
    /// Per-transaction undo data, empty when undo retention is disabled.
    pub transaction_undos: Vec<UtxoUndo>,
}

/// Atomic chainstate surface block execution needs to connect and disconnect
/// active blocks.
///
/// Every commit method must apply its complete effect — UTXO mutation, block
/// undo, and execution tip — in one storage transaction, so a crash exposes
/// either the whole transition or none of it. Implementations must also keep
/// [`UtxoStore::get_many`] results in caller order; the executor's prefetch
/// verification depends on positional alignment.
pub trait ExecutionChainStore: UtxoStore {
    /// Returns the durable execution tip.
    fn execution_tip(&self) -> Result<ExecutionTip, ChainStoreError>;
    /// Returns the assumed snapshot base below which no undo data exists.
    fn assumed_snapshot_base(&self) -> Result<Option<ExecutionTip>, ChainStoreError>;
    /// Returns the stored per-transaction undo list for an executed block.
    fn block_undo(&self, hash: BlockHash) -> Result<Option<Vec<UtxoUndo>>, ChainStoreError>;
    /// Reports whether connect transitions must carry block undo data.
    fn retains_block_undo(&self) -> bool;
    /// Commits one block's net UTXO effect, undo, and tip advance atomically.
    ///
    /// The stored tip must equal `expected_parent` and `next` must extend it
    /// by exactly one block, checked inside the same transaction.
    fn commit_connect(
        &self,
        expected_parent: BlockHash,
        next: ExecutionTip,
        spent: &[OutPointKey],
        created: &[(OutPointKey, Utxo)],
        transaction_undos: &[UtxoUndo],
    ) -> Result<UtxoUndo, ChainStoreError>;
    /// Commits a contiguous batch of validated transitions atomically.
    fn commit_connect_batch(
        &self,
        transitions: &[ConnectTransition],
    ) -> Result<(), ChainStoreError>;
    /// Reverses the tip block and removes its undo in one transaction.
    fn commit_disconnect(
        &self,
        expected_current: ExecutionTip,
        parent: ExecutionTip,
        spent: &[OutPointKey],
        created: &[(OutPointKey, Utxo)],
        transaction_undos: &[UtxoUndo],
    ) -> Result<UtxoUndo, ChainStoreError>;
    /// Removes block undo below `retain_from_height`, resolving every stored
    /// hash through the authenticated header DAG first.
    ///
    /// The default keeps all undo; stores with their own retention windows
    /// override this so the retained-ledger floor also bounds undo growth.
    fn prune_block_undos_before(
        &self,
        headers: &HeaderDag,
        retain_from_height: u32,
    ) -> Result<u64, ChainStoreError> {
        let (_, _) = (headers, retain_from_height);
        Ok(0)
    }
    /// Takes an advisory candidate for legacy validation-journal migration.
    ///
    /// Only journal-backed stores return candidates; the default is `None`.
    fn take_hottest_legacy_validation_delta(&self) -> Option<u32> {
        None
    }
    /// Rewrites one legacy validation-journal row as sorted shards.
    ///
    /// The default reports that no migration was necessary.
    fn shard_legacy_validation_delta(
        &self,
        height: u32,
    ) -> Result<Option<ValidationDeltaShardMigration>, ChainStoreError> {
        let _ = height;
        Ok(None)
    }
}

impl ExecutionChainStore for RedbChainStore {
    fn execution_tip(&self) -> Result<ExecutionTip, ChainStoreError> {
        Ok(self.execution().tip()?)
    }

    fn assumed_snapshot_base(&self) -> Result<Option<ExecutionTip>, ChainStoreError> {
        Ok(self.execution().assumed_snapshot_base()?)
    }

    fn block_undo(&self, hash: BlockHash) -> Result<Option<Vec<UtxoUndo>>, ChainStoreError> {
        Ok(self.undos().get(hash)?)
    }

    fn retains_block_undo(&self) -> bool {
        RedbChainStore::retains_block_undo(self)
    }

    fn commit_connect(
        &self,
        expected_parent: BlockHash,
        next: ExecutionTip,
        spent: &[OutPointKey],
        created: &[(OutPointKey, Utxo)],
        transaction_undos: &[UtxoUndo],
    ) -> Result<UtxoUndo, ChainStoreError> {
        RedbChainStore::commit_connect(
            self,
            expected_parent,
            next,
            spent,
            created,
            transaction_undos,
        )
    }

    fn commit_connect_batch(
        &self,
        transitions: &[ConnectTransition],
    ) -> Result<(), ChainStoreError> {
        RedbChainStore::commit_connect_batch(self, transitions)
    }

    fn commit_disconnect(
        &self,
        expected_current: ExecutionTip,
        parent: ExecutionTip,
        spent: &[OutPointKey],
        created: &[(OutPointKey, Utxo)],
        transaction_undos: &[UtxoUndo],
    ) -> Result<UtxoUndo, ChainStoreError> {
        RedbChainStore::commit_disconnect(
            self,
            expected_current,
            parent,
            spent,
            created,
            transaction_undos,
        )
    }

    fn prune_block_undos_before(
        &self,
        headers: &HeaderDag,
        retain_from_height: u32,
    ) -> Result<u64, ChainStoreError> {
        RedbChainStore::prune_block_undos_before(self, headers, retain_from_height)
    }

    fn take_hottest_legacy_validation_delta(&self) -> Option<u32> {
        RedbChainStore::take_hottest_legacy_validation_delta(self)
    }

    fn shard_legacy_validation_delta(
        &self,
        height: u32,
    ) -> Result<Option<ValidationDeltaShardMigration>, ChainStoreError> {
        RedbChainStore::shard_legacy_validation_delta(self, height)
    }
}

/// Reorg-consistent spent-output coin-age observations for one chainstate.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SpentAgeHistogram {
    /// First connected block represented by this sample.
    pub start_height: Option<u32>,
    /// Last connected block represented by this sample.
    pub end_height: Option<u32>,
    /// Total observed spent outputs.
    pub samples: u64,
    /// Exact `(age_in_blocks, spend_count)` rows in ascending age order.
    pub rows: Vec<(u32, u64)>,
}

impl SpentAgeHistogram {
    /// Returns the number of observed spends whose coin age is at most `blocks`.
    #[must_use]
    pub fn hits_within(&self, blocks: u32) -> u64 {
        self.rows
            .iter()
            .take_while(|(age, _)| *age <= blocks)
            .map(|(_, count)| *count)
            .sum()
    }
}

/// Result of atomically replacing one legacy giant validation row with shards.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ValidationDeltaShardMigration {
    /// Checkpoint height whose row was rewritten.
    pub height: u32,
    /// Bytes in the legacy RVD3 row.
    pub legacy_bytes: u64,
    /// Number of non-empty sorted RVD4 shards.
    pub shard_count: usize,
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

#[derive(Clone, Copy)]
struct ShardedValidationDeltaHeader {
    utxo_count: u64,
    update_count: usize,
    shard_count: usize,
    populated_shards: [u8; VALIDATION_DELTA_SHARD_BITMAP_BYTES],
}

fn validation_delta_shard(outpoint: OutPointKey, shard_count: usize) -> u8 {
    match shard_count {
        VALIDATION_RVD4_SHARD_COUNT => outpoint.as_bytes()[0],
        VALIDATION_DELTA_SHARD_COUNT => outpoint.as_bytes()[0] >> 4,
        _ => unreachable!("validated shard count"),
    }
}

fn validation_delta_shard_key(height: u32, shard: u8) -> [u8; 5] {
    let mut key = [0_u8; 5];
    key[..4].copy_from_slice(&height.to_be_bytes());
    key[4] = shard;
    key
}

fn encode_sharded_validation_delta(
    updates: &ValidationDeltaUpdates,
    utxo_count: u64,
) -> Result<(Vec<u8>, EncodedValidationDeltaShards), UtxoError> {
    let update_count = u32::try_from(updates.len())
        .map_err(|_| UtxoError::Malformed("validation delta update count"))?;
    let mut populated_shards = [0_u8; VALIDATION_DELTA_SHARD_BITMAP_BYTES];
    let mut shards = Vec::new();
    let mut start = 0;
    while start < updates.len() {
        let shard = validation_delta_shard(updates[start].0, VALIDATION_DELTA_SHARD_COUNT);
        let mut end = start + 1;
        while end < updates.len()
            && validation_delta_shard(updates[end].0, VALIDATION_DELTA_SHARD_COUNT) == shard
        {
            end += 1;
        }
        populated_shards[usize::from(shard) / 8] |= 1 << (shard % 8);
        let encoded = encode_validation_delta(
            updates[start..end]
                .iter()
                .map(|(outpoint, update)| (outpoint, update)),
            utxo_count,
        )?;
        shards.push((shard, encoded));
        start = end;
    }
    let mut manifest = Vec::with_capacity(VALIDATION_COMPACT_SHARDED_DELTA_HEADER_BYTES);
    manifest.extend_from_slice(&VALIDATION_COMPACT_SHARDED_DELTA_MAGIC);
    manifest.extend_from_slice(&utxo_count.to_le_bytes());
    manifest.extend_from_slice(&update_count.to_le_bytes());
    manifest.extend_from_slice(&populated_shards[..VALIDATION_DELTA_SHARD_COUNT / 8]);
    Ok((manifest, shards))
}

fn decode_sharded_validation_delta_header(
    encoded: &[u8],
) -> Result<ShardedValidationDeltaHeader, UtxoError> {
    let shard_count = if encoded.starts_with(&VALIDATION_SHARDED_DELTA_MAGIC)
        && encoded.len() == VALIDATION_SHARDED_DELTA_HEADER_BYTES
    {
        VALIDATION_RVD4_SHARD_COUNT
    } else if encoded.starts_with(&VALIDATION_COMPACT_SHARDED_DELTA_MAGIC)
        && encoded.len() == VALIDATION_COMPACT_SHARDED_DELTA_HEADER_BYTES
    {
        VALIDATION_DELTA_SHARD_COUNT
    } else {
        return Err(UtxoError::Malformed("sharded validation delta manifest"));
    };
    let utxo_count = u64::from_le_bytes(
        encoded[4..12]
            .try_into()
            .expect("eight-byte validation UTXO count"),
    );
    let update_count = usize::try_from(u32::from_le_bytes(
        encoded[12..16]
            .try_into()
            .expect("four-byte validation delta count"),
    ))
    .expect("u32 fits usize");
    let mut populated_shards = [0_u8; VALIDATION_DELTA_SHARD_BITMAP_BYTES];
    populated_shards[..encoded.len() - 16].copy_from_slice(&encoded[16..]);
    if update_count == 0 && populated_shards.iter().any(|byte| *byte != 0) {
        return Err(UtxoError::Malformed(
            "empty sharded validation delta has populated shards",
        ));
    }
    Ok(ShardedValidationDeltaHeader {
        utxo_count,
        update_count,
        shard_count,
        populated_shards,
    })
}

fn validation_shard_is_populated(header: ShardedValidationDeltaHeader, shard: u8) -> bool {
    header.populated_shards[usize::from(shard) / 8] & (1 << (shard % 8)) != 0
}

enum ValidationDeltaRecordHeader {
    Legacy {
        utxo_count: u64,
        update_count: usize,
    },
    Sharded(ShardedValidationDeltaHeader),
}

fn validation_delta_record_header(
    encoded: &[u8],
) -> Result<ValidationDeltaRecordHeader, UtxoError> {
    if encoded.starts_with(&VALIDATION_DELTA_MAGIC) {
        let (utxo_count, update_count, _) = validation_delta_header(encoded)?;
        Ok(ValidationDeltaRecordHeader::Legacy {
            utxo_count,
            update_count,
        })
    } else if encoded.starts_with(&VALIDATION_SHARDED_DELTA_MAGIC)
        || encoded.starts_with(&VALIDATION_COMPACT_SHARDED_DELTA_MAGIC)
    {
        decode_sharded_validation_delta_header(encoded).map(ValidationDeltaRecordHeader::Sharded)
    } else {
        Err(UtxoError::Malformed("validation delta format"))
    }
}

fn decode_validation_delta_record<F>(
    encoded: &[u8],
    mut load_shard: F,
) -> Result<(u64, ValidationDeltaUpdates), UtxoError>
where
    F: FnMut(u8) -> Result<Vec<u8>, UtxoError>,
{
    match validation_delta_record_header(encoded)? {
        ValidationDeltaRecordHeader::Legacy { .. } => decode_validation_delta(encoded),
        ValidationDeltaRecordHeader::Sharded(header) => {
            let mut updates = Vec::with_capacity(header.update_count);
            for shard in 0..header.shard_count {
                let shard = u8::try_from(shard).expect("validation shard fits u8");
                if !validation_shard_is_populated(header, shard) {
                    continue;
                }
                let encoded_shard = load_shard(shard)?;
                let (shard_utxo_count, shard_updates) = decode_validation_delta(&encoded_shard)?;
                if shard_utxo_count != header.utxo_count
                    || shard_updates.is_empty()
                    || shard_updates.iter().any(|(outpoint, _)| {
                        validation_delta_shard(*outpoint, header.shard_count) != shard
                    })
                {
                    return Err(UtxoError::Malformed(
                        "validation delta shard content mismatch",
                    ));
                }
                updates.extend(shard_updates);
            }
            if updates.len() != header.update_count {
                return Err(UtxoError::Malformed(
                    "validation delta shard count mismatch",
                ));
            }
            Ok((header.utxo_count, updates))
        }
    }
}

fn inspect_validation_delta_record<F>(
    encoded: &[u8],
    load_shard: F,
    aggregate_bloom: Option<&mut ValidationBloom>,
) -> Result<(u64, usize, ValidationBloom), UtxoError>
where
    F: FnMut(u8) -> Result<Vec<u8>, UtxoError>,
{
    if encoded.starts_with(&VALIDATION_DELTA_MAGIC) {
        return inspect_validation_delta(encoded, aggregate_bloom);
    }
    let (utxo_count, updates) = decode_validation_delta_record(encoded, load_shard)?;
    let mut bloom = ValidationBloom::with_update_count(updates.len())?;
    if let Some(aggregate_bloom) = aggregate_bloom {
        for outpoint in updates.iter().map(|(outpoint, _)| *outpoint) {
            bloom.insert(outpoint);
            aggregate_bloom.insert(outpoint);
        }
    } else {
        for outpoint in updates.iter().map(|(outpoint, _)| *outpoint) {
            bloom.insert(outpoint);
        }
    }
    Ok((utxo_count, updates.len(), bloom))
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

/// Returns the next sorted delta keys after an exclusive cursor without
/// decoding or allocating their UTXO values.
fn validation_delta_key_page(
    encoded: &[u8],
    after: Option<OutPointKey>,
    limit: usize,
) -> Result<(u64, Vec<OutPointKey>), UtxoError> {
    let (utxo_count, count, _) = validation_delta_header(encoded)?;
    if limit == 0 {
        return Ok((utxo_count, Vec::new()));
    }
    let mut left = 0_usize;
    let mut right = count;
    if let Some(after) = after {
        while left < right {
            let middle = left + (right - left) / 2;
            let (candidate, ..) = validation_delta_index_entry(encoded, middle)?;
            if candidate <= after {
                left = middle + 1;
            } else {
                right = middle;
            }
        }
    }
    let end = left.saturating_add(limit).min(count);
    let mut keys = Vec::with_capacity(end.saturating_sub(left));
    for index in left..end {
        keys.push(validation_delta_index_entry(encoded, index)?.0);
    }
    Ok((utxo_count, keys))
}

fn decode_validation_delta(encoded: &[u8]) -> Result<(u64, ValidationDeltaUpdates), UtxoError> {
    let (utxo_count, count, _) = inspect_validation_delta(encoded, None)?;
    let mut updates = Vec::with_capacity(count);
    for index in 0..count {
        let (outpoint, state, offset, length) = validation_delta_index_entry(encoded, index)?;
        // `inspect_validation_delta` above already proved every offset/length
        // pair in range, but re-derive the bound here so this pass is sound on
        // its own rather than only in combination with the first one.
        let end = offset
            .checked_add(length)
            .filter(|end| *end <= encoded.len())
            .ok_or(UtxoError::Malformed("truncated validation delta UTXO"))?;
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

fn spent_age_counts<'a>(
    blocks: impl IntoIterator<Item = (u32, &'a [UtxoUndo])>,
) -> Result<BTreeMap<u32, u64>, UtxoError> {
    let mut counts = BTreeMap::new();
    for (spend_height, transaction_undos) in blocks {
        for (_, spent) in transaction_undos
            .iter()
            .flat_map(|undo| undo.spent().iter())
        {
            let age = spend_height
                .checked_sub(spent.height)
                .ok_or(UtxoError::Malformed("spent coin height exceeds block"))?;
            let count = counts.entry(age).or_insert(0_u64);
            *count = count
                .checked_add(1)
                .ok_or(UtxoError::Malformed("spent-age sample overflow"))?;
        }
    }
    Ok(counts)
}

#[cfg(test)]
fn identity_from_sorted_entries(
    entries: impl IntoIterator<Item = (OutPointKey, Utxo)>,
) -> Result<(u64, u64, [u8; 32]), UtxoError> {
    let mut count = 0_u64;
    let mut records_bytes = 0_u64;
    let mut digest = Sha256::new();
    for (outpoint, utxo) in entries {
        let encoded = utxo.encode()?;
        count = count
            .checked_add(1)
            .ok_or(UtxoError::Malformed("snapshot UTXO count overflow"))?;
        records_bytes = records_bytes
            .checked_add(
                u64::try_from(outpoint.as_bytes().len() + encoded.len())
                    .expect("UTXO record length fits u64"),
            )
            .ok_or(UtxoError::Malformed("snapshot records length overflow"))?;
        update_utxo_set_digest(&mut digest, outpoint.as_bytes(), &utxo);
    }
    Ok((count, records_bytes, digest.finalize().into()))
}

fn connect_spent_ages_transaction(
    transaction: &redb::WriteTransaction,
    counts: &BTreeMap<u32, u64>,
    start_height: u32,
    end_height: u32,
) -> Result<(), UtxoError> {
    let mut metadata = transaction.open_table(SPENT_AGE_META_TABLE)?;
    let existing_start = metadata
        .get(SPENT_AGE_START_HEIGHT)?
        .map(|value| value.value());
    let existing_end = metadata
        .get(SPENT_AGE_END_HEIGHT)?
        .map(|value| value.value());
    match (existing_start, existing_end) {
        (None, None) => {
            metadata.insert(SPENT_AGE_START_HEIGHT, start_height)?;
        }
        (Some(_), Some(previous_end)) if previous_end.checked_add(1) == Some(start_height) => {}
        _ => {
            return Err(UtxoError::Malformed(
                "spent-age histogram coverage is not contiguous",
            ));
        }
    }
    metadata.insert(SPENT_AGE_END_HEIGHT, end_height)?;
    drop(metadata);

    let mut histogram = transaction.open_table(SPENT_AGE_TABLE)?;
    for (age, increment) in counts {
        let current = histogram.get(*age)?.map_or(0, |value| value.value());
        let updated = current
            .checked_add(*increment)
            .ok_or(UtxoError::Malformed("spent-age histogram overflow"))?;
        histogram.insert(*age, updated)?;
    }
    Ok(())
}

fn disconnect_spent_ages_transaction(
    transaction: &redb::WriteTransaction,
    counts: &BTreeMap<u32, u64>,
    current_height: u32,
    parent_height: u32,
) -> Result<(), UtxoError> {
    let metadata = transaction.open_table(SPENT_AGE_META_TABLE)?;
    let Some(start_height) = metadata
        .get(SPENT_AGE_START_HEIGHT)?
        .map(|value| value.value())
    else {
        return Ok(());
    };
    let end_height = metadata
        .get(SPENT_AGE_END_HEIGHT)?
        .map(|value| value.value())
        .ok_or(UtxoError::Malformed("incomplete spent-age metadata"))?;
    if end_height != current_height {
        return Err(UtxoError::Malformed(
            "spent-age histogram tip does not match disconnect",
        ));
    }
    drop(metadata);

    let mut histogram = transaction.open_table(SPENT_AGE_TABLE)?;
    for (age, decrement) in counts {
        let current = histogram
            .get(*age)?
            .map(|value| value.value())
            .ok_or(UtxoError::Malformed("missing spent-age histogram row"))?;
        let updated = current
            .checked_sub(*decrement)
            .ok_or(UtxoError::Malformed("spent-age histogram underflow"))?;
        if updated == 0 {
            histogram.remove(*age)?;
        } else {
            histogram.insert(*age, updated)?;
        }
    }
    if current_height == start_height {
        histogram.retain(|_, _| false)?;
        drop(histogram);
        let mut metadata = transaction.open_table(SPENT_AGE_META_TABLE)?;
        metadata.retain(|_, _| false)?;
    } else {
        drop(histogram);
        let mut metadata = transaction.open_table(SPENT_AGE_META_TABLE)?;
        metadata.insert(SPENT_AGE_END_HEIGHT, parent_height)?;
    }
    Ok(())
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

    #[allow(clippy::too_many_lines)]
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
        let validation_journal = {
            let transaction = db.begin_write()?;
            {
                let _deltas = transaction.open_table(VALIDATION_DELTA_TABLE)?;
                let _delta_shards = transaction.open_table(VALIDATION_DELTA_SHARD_TABLE)?;
                let _row_blooms = transaction.open_table(VALIDATION_DELTA_BLOOM_TABLE)?;
                let _group_blooms = transaction.open_table(VALIDATION_GROUP_BLOOM_TABLE)?;
            }
            transaction.commit()?;
            let transaction = db.begin_read()?;
            let deltas = transaction.open_table(VALIDATION_DELTA_TABLE)?;
            let delta_shards = transaction.open_table(VALIDATION_DELTA_SHARD_TABLE)?;
            let row_blooms = transaction.open_table(VALIDATION_DELTA_BLOOM_TABLE)?;
            let group_blooms = transaction.open_table(VALIDATION_GROUP_BLOOM_TABLE)?;
            // A durable journal is self-describing state, not a caller hint.
            // Automatically resume it after restart so a crash between a bulk
            // catch-up checkpoint and final materialization cannot make the
            // database require an otherwise unrelated CLI flag.
            let validation_delta_journal = options.validation_delta_journal
                || !deltas.is_empty()?
                || !delta_shards.is_empty()?;
            if validation_delta_journal {
                let base_stats = utxos.tier_stats()?;
                let mut journal = ValidationJournal {
                    rows: Vec::new(),
                    groups: Vec::new(),
                    legacy_hits: BTreeMap::new(),
                    utxo_count: base_stats
                        .hot
                        .checked_add(base_stats.cold)
                        .ok_or(UtxoError::Malformed("validation UTXO count overflow"))?,
                };
                let mut migrated_rows = Vec::new();
                let mut migrated_groups = Vec::new();
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
                        let group_index = u32::try_from(journal.groups.len()).map_err(|_| {
                            UtxoError::Malformed("validation bloom group index overflow")
                        })?;
                        if let Some(encoded) = group_blooms.get(group_index)? {
                            let expected =
                                validation_bloom_byte_count(VALIDATION_GROUP_BLOOM_UPDATES)?;
                            let (utxo_count, bloom) =
                                decode_validation_bloom(encoded.value(), expected)?;
                            if utxo_count != 0 {
                                return Err(UtxoError::Malformed(
                                    "validation group bloom carries a UTXO count",
                                )
                                .into());
                            }
                            journal.groups.push(bloom);
                        } else {
                            journal.groups.push(ValidationBloom::with_update_count(
                                VALIDATION_GROUP_BLOOM_UPDATES,
                            )?);
                        }
                    }
                    let group_index = journal.groups.len() - 1;
                    let group_is_persisted = group_blooms
                        .get(u32::try_from(group_index).map_err(|_| {
                            UtxoError::Malformed("validation bloom group index overflow")
                        })?)?
                        .is_some();
                    let aggregate = journal
                        .groups
                        .last_mut()
                        .expect("validation row has a bloom group");
                    let record_header = validation_delta_record_header(encoded.value())?;
                    let (utxo_count, update_count) = match record_header {
                        ValidationDeltaRecordHeader::Legacy {
                            utxo_count,
                            update_count,
                        } => (utxo_count, update_count),
                        ValidationDeltaRecordHeader::Sharded(header) => {
                            (header.utxo_count, header.update_count)
                        }
                    };
                    let expected_bloom_bytes = validation_bloom_byte_count(update_count)?;
                    let bloom = if let Some(encoded_bloom) = row_blooms.get(height)? {
                        let (bloom_utxo_count, bloom) =
                            decode_validation_bloom(encoded_bloom.value(), expected_bloom_bytes)?;
                        if bloom_utxo_count != utxo_count {
                            return Err(UtxoError::Malformed(
                                "validation delta bloom UTXO count mismatch",
                            )
                            .into());
                        }
                        if !group_is_persisted {
                            let (_, _, inspected) = inspect_validation_delta_record(
                                encoded.value(),
                                |shard| {
                                    let key = validation_delta_shard_key(height, shard);
                                    delta_shards
                                        .get(key.as_slice())?
                                        .map(|encoded| encoded.value().to_vec())
                                        .ok_or(UtxoError::Malformed(
                                            "missing validation delta shard",
                                        ))
                                },
                                Some(aggregate),
                            )?;
                            if inspected.bits != bloom.bits {
                                return Err(UtxoError::Malformed(
                                    "validation delta bloom content mismatch",
                                )
                                .into());
                            }
                        }
                        bloom
                    } else {
                        let (_, _, bloom) = inspect_validation_delta_record(
                            encoded.value(),
                            |shard| {
                                let key = validation_delta_shard_key(height, shard);
                                delta_shards
                                    .get(key.as_slice())?
                                    .map(|encoded| encoded.value().to_vec())
                                    .ok_or(UtxoError::Malformed("missing validation delta shard"))
                            },
                            (!group_is_persisted).then_some(aggregate),
                        )?;
                        migrated_rows.push((height, encode_validation_bloom(&bloom, utxo_count)));
                        bloom
                    };
                    let completed_group =
                        ((journal.rows.len() + 1) % VALIDATION_ROWS_PER_BLOOM_GROUP == 0
                            && !group_is_persisted)
                            .then(|| encode_validation_bloom(aggregate, 0));
                    journal.utxo_count = utxo_count;
                    journal.rows.push(ValidationJournalRow { height, bloom });
                    if let Some(encoded) = completed_group {
                        let group_index = u32::try_from(group_index).map_err(|_| {
                            UtxoError::Malformed("validation bloom group index overflow")
                        })?;
                        migrated_groups.push((group_index, encoded));
                    }
                }
                if !journal.rows.is_empty()
                    && journal.rows.len() % VALIDATION_ROWS_PER_BLOOM_GROUP != 0
                {
                    let group_index = u32::try_from(journal.groups.len() - 1).map_err(|_| {
                        UtxoError::Malformed("validation bloom group index overflow")
                    })?;
                    if group_blooms.get(group_index)?.is_none() {
                        let aggregate = journal
                            .groups
                            .last()
                            .expect("partial validation group has an aggregate bloom");
                        migrated_groups.push((group_index, encode_validation_bloom(aggregate, 0)));
                    }
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
                drop(group_blooms);
                drop(row_blooms);
                drop(delta_shards);
                drop(deltas);
                drop(transaction);
                if !migrated_rows.is_empty() || !migrated_groups.is_empty() {
                    let mut migration = db.begin_write()?;
                    migration.set_durability(Durability::Immediate);
                    migration.set_quick_repair(options.quick_repair);
                    {
                        let mut blooms = migration.open_table(VALIDATION_DELTA_BLOOM_TABLE)?;
                        for (height, encoded) in &migrated_rows {
                            blooms.insert(*height, encoded.as_slice())?;
                        }
                        let mut groups = migration.open_table(VALIDATION_GROUP_BLOOM_TABLE)?;
                        for (index, encoded) in &migrated_groups {
                            groups.insert(*index, encoded.as_slice())?;
                        }
                    }
                    migration.commit()?;
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

    /// Removes disconnect records older than the locally retained block window.
    ///
    /// Every hash is first resolved through the authenticated header DAG. Any
    /// unknown record fails closed before a write transaction begins.
    pub fn prune_block_undos_before(
        &self,
        headers: &HeaderDag,
        retain_from_height: u32,
    ) -> Result<u64, ChainStoreError> {
        if !self.options.retain_block_undo {
            return Ok(0);
        }
        let mut expired = self
            .undos
            .hashes()?
            .into_iter()
            .map(|hash| {
                headers
                    .get(&hash)
                    .map(|header| (header.height, hash))
                    .ok_or(UndoStoreError::Malformed(
                        "block undo references an unknown header",
                    ))
            })
            .collect::<Result<Vec<_>, _>>()?;
        expired.retain(|(height, _)| *height < retain_from_height);
        expired.sort_unstable_by_key(|(height, hash)| (*height, hash.to_byte_array()));
        if expired.is_empty() {
            return Ok(0);
        }

        let _guard = self.lock();
        let mut transaction = self.db.begin_write()?;
        self.configure(&mut transaction);
        let mut removed = 0_u64;
        for (_, hash) in expired {
            removed += u64::from(remove_undo_transaction(&transaction, hash)?);
        }
        transaction.commit()?;
        Ok(removed)
    }

    /// Read-only access to retained block undo records.
    pub fn undos(&self) -> &RedbUndoStore {
        &self.undos
    }

    /// Read/write access to execution metadata outside block transitions.
    pub fn execution(&self) -> &RedbExecutionStore {
        &self.execution
    }

    /// Takes the hottest recently read legacy row as an advisory migration candidate.
    pub fn take_hottest_legacy_validation_delta(&self) -> Option<u32> {
        let Some(validation_journal) = &self.validation_journal else {
            return None;
        };
        let mut journal = validation_journal
            .lock()
            .expect("validation journal lock not poisoned");
        let (&height, _) = journal
            .legacy_hits
            .iter()
            .max_by_key(|(height, hits)| (**hits, **height))?;
        journal.legacy_hits.remove(&height);
        Some(height)
    }

    /// Atomically rewrites one legacy giant journal row as sorted shards.
    ///
    /// The RVD3-to-RVD4 replacement and every shard insertion share one
    /// immediate-durability transaction, so concurrent readers observe exactly
    /// one complete representation.
    pub fn shard_legacy_validation_delta(
        &self,
        height: u32,
    ) -> Result<Option<ValidationDeltaShardMigration>, ChainStoreError> {
        if self.validation_journal.is_none() {
            return Ok(None);
        };
        let _guard = self.lock();
        let mut transaction = self.db.begin_write()?;
        self.configure(&mut transaction);
        let (legacy_bytes, utxo_count, updates) = {
            let deltas = transaction.open_table(VALIDATION_DELTA_TABLE)?;
            let encoded = deltas
                .get(height)?
                .ok_or(UtxoError::Malformed("missing validation delta row"))?;
            if !encoded.value().starts_with(&VALIDATION_DELTA_MAGIC) {
                return Ok(None);
            }
            let legacy_bytes = u64::try_from(encoded.value().len())
                .map_err(|_| UtxoError::Malformed("validation delta record length"))?;
            let (utxo_count, updates) = decode_validation_delta(encoded.value())?;
            (legacy_bytes, utxo_count, updates)
        };
        let (manifest, shards) = encode_sharded_validation_delta(&updates, utxo_count)?;
        {
            let mut deltas = transaction.open_table(VALIDATION_DELTA_TABLE)?;
            deltas.insert(height, manifest.as_slice())?;
            let mut delta_shards = transaction.open_table(VALIDATION_DELTA_SHARD_TABLE)?;
            for (shard, encoded) in &shards {
                let key = validation_delta_shard_key(height, *shard);
                delta_shards.insert(key.as_slice(), encoded.as_slice())?;
            }
        }
        transaction.commit()?;
        Ok(Some(ValidationDeltaShardMigration {
            height,
            legacy_bytes,
            shard_count: shards.len(),
        }))
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
        let _bulk_guard = bulk_commit_guard();
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
            let delta_shards = transaction.open_table(VALIDATION_DELTA_SHARD_TABLE)?;
            let mut updates: AHashMap<OutPointKey, ValidationUpdate> = AHashMap::new();
            for row in &journal.rows {
                let encoded = deltas
                    .get(row.height)?
                    .ok_or(UtxoError::Malformed("missing validation delta row"))?;
                let decoded = decode_validation_delta_record(encoded.value(), |shard| {
                    let key = validation_delta_shard_key(row.height, shard);
                    delta_shards
                        .get(key.as_slice())?
                        .map(|encoded| encoded.value().to_vec())
                        .ok_or(UtxoError::Malformed("missing validation delta shard"))
                })?;
                for (outpoint, update) in decoded.1 {
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
            let mut delta_shards = transaction.open_table(VALIDATION_DELTA_SHARD_TABLE)?;
            delta_shards.retain(|_, _| false)?;
            let mut blooms = transaction.open_table(VALIDATION_DELTA_BLOOM_TABLE)?;
            blooms.retain(|_, _| false)?;
            let mut groups = transaction.open_table(VALIDATION_GROUP_BLOOM_TABLE)?;
            groups.retain(|_, _| false)?;
        }
        transaction.commit()?;
        let count = u64::try_from(updates.len()).expect("usize fits u64");
        journal.rows.clear();
        journal.groups.clear();
        Ok(count)
    }

    fn validation_journal_candidate_page(
        &self,
        after: Option<OutPointKey>,
        limit: usize,
    ) -> Result<Vec<OutPointKey>, UtxoError> {
        let journal = self
            .validation_journal
            .as_ref()
            .expect("caller checked validation journal")
            .lock()
            .expect("validation journal lock not poisoned");
        let transaction = self.db.begin_read()?;
        let deltas = transaction.open_table(VALIDATION_DELTA_TABLE)?;
        let delta_shards = transaction.open_table(VALIDATION_DELTA_SHARD_TABLE)?;
        let mut candidates = BTreeSet::new();
        let mut retain_smallest = |outpoint| {
            candidates.insert(outpoint);
            if candidates.len() > limit {
                candidates.pop_last();
            }
        };
        for (outpoint, _) in self.utxos.snapshot_page(after, limit)? {
            retain_smallest(outpoint);
        }
        for row in &journal.rows {
            let encoded = deltas
                .get(row.height)?
                .ok_or(UtxoError::Malformed("missing validation delta row"))?;
            match validation_delta_record_header(encoded.value())? {
                ValidationDeltaRecordHeader::Legacy { .. } => {
                    let (_, keys) = validation_delta_key_page(encoded.value(), after, limit)?;
                    for outpoint in keys {
                        retain_smallest(outpoint);
                    }
                }
                ValidationDeltaRecordHeader::Sharded(header) => {
                    let first_shard = after.map_or(0, |cursor| {
                        usize::from(validation_delta_shard(cursor, header.shard_count))
                    });
                    let mut retained = 0_usize;
                    for shard in first_shard..header.shard_count {
                        if retained == limit {
                            break;
                        }
                        let shard = u8::try_from(shard).expect("validation shard count fits u8");
                        if !validation_shard_is_populated(header, shard) {
                            continue;
                        }
                        let key = validation_delta_shard_key(row.height, shard);
                        let encoded_shard = delta_shards
                            .get(key.as_slice())?
                            .ok_or(UtxoError::Malformed("missing validation delta shard"))?;
                        let (utxo_count, keys) = validation_delta_key_page(
                            encoded_shard.value(),
                            after,
                            limit - retained,
                        )?;
                        if utxo_count != header.utxo_count
                            || keys.iter().any(|outpoint| {
                                validation_delta_shard(*outpoint, header.shard_count) != shard
                            })
                        {
                            return Err(UtxoError::Malformed(
                                "validation delta shard content mismatch",
                            ));
                        }
                        retained += keys.len();
                        for outpoint in keys {
                            retain_smallest(outpoint);
                        }
                    }
                }
            }
        }
        Ok(candidates.into_iter().collect())
    }

    /// Returns one sorted, cursor-based page from the complete hot/cold UTXO set.
    ///
    /// A validation-journal page merges the durable base and sorted delta
    /// indexes in bounded windows. Removed candidates are skipped and replaced
    /// until the requested logical page is full; `after` remains an exclusive
    /// cursor over the folded set.
    pub fn utxo_snapshot_page(
        &self,
        after: Option<OutPointKey>,
        limit: usize,
    ) -> Result<Vec<(OutPointKey, Utxo)>, UtxoError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        if self.validation_journal.is_none() {
            return self.utxos.snapshot_page(after, limit);
        }

        // Validation-journal writers take this guard before changing either
        // durable rows or the in-memory row inventory. Holding it across the
        // bounded merge gives every candidate and overlay lookup one logical
        // snapshot even though each uses its own redb read transaction.
        let _guard = self.lock();
        let mut page = Vec::with_capacity(limit);
        let mut cursor = after;
        while page.len() < limit {
            // A floor amortizes pages containing many deleted candidates while
            // keeping memory independent of the UTXO-set and journal sizes.
            let scan_limit = limit.saturating_sub(page.len()).max(256);
            let candidates = self.validation_journal_candidate_page(cursor, scan_limit)?;
            if candidates.is_empty() {
                break;
            }
            for (outpoint, utxo) in self.get_many(&candidates)? {
                cursor = Some(outpoint);
                if let Some(utxo) = utxo {
                    page.push((outpoint, utxo));
                    if page.len() == limit {
                        break;
                    }
                }
            }
        }
        Ok(page)
    }

    /// Reclassifies one bounded, durable UTXO page by block-age.
    pub fn retier_utxos_by_height_batch(
        &self,
        tip_height: u32,
        hot_window_blocks: u32,
        scan_limit: usize,
        quick_repair: bool,
    ) -> Result<HeightRetierProgress, UtxoError> {
        if self.validation_journal.is_some() {
            return Err(UtxoError::Malformed(
                "height re-tier requires materialized chainstate",
            ));
        }
        self.utxos
            .retier_by_height_batch(tip_height, hot_window_blocks, scan_limit, quick_repair)
    }

    fn snapshot_content_identity(&self) -> Result<(u64, u64, [u8; 32]), UtxoError> {
        let mut records_bytes = 0_u64;
        let mut digest = Sha256::new();
        let count = self.visit_utxo_snapshot(|outpoint, utxo| {
            let encoded = utxo.encode()?;
            records_bytes = records_bytes
                .checked_add(
                    u64::try_from(outpoint.as_bytes().len() + encoded.len())
                        .expect("UTXO record length fits u64"),
                )
                .ok_or(UtxoError::Malformed("snapshot records length overflow"))?;
            update_utxo_set_digest(&mut digest, outpoint.as_bytes(), utxo);
            Ok(())
        })?;
        Ok((count, records_bytes, digest.finalize().into()))
    }

    /// Visits the complete logical UTXO set in lexical key order with bounded
    /// memory, including any durable validation overlay.
    ///
    /// Journal-backed callers load and fold one key prefix at a time. This is
    /// the production path for finalization, activity reporting, and explorer
    /// snapshot baselines; none materializes the complete set in memory.
    #[allow(clippy::too_many_lines)]
    pub fn visit_utxo_snapshot<F>(&self, mut visitor: F) -> Result<u64, UtxoError>
    where
        F: FnMut(OutPointKey, &Utxo) -> Result<(), UtxoError>,
    {
        if self.validation_journal.is_none() {
            let mut cursor = None;
            let mut count = 0_u64;
            loop {
                let page = self.utxos.snapshot_page(cursor, 4_096)?;
                if page.is_empty() {
                    break;
                }
                for (outpoint, utxo) in &page {
                    visitor(*outpoint, utxo)?;
                    count = count
                        .checked_add(1)
                        .ok_or(UtxoError::Malformed("snapshot UTXO count overflow"))?;
                }
                cursor = page.last().map(|(outpoint, _)| *outpoint);
                if page.len() < 4_096 {
                    break;
                }
            }
            return Ok(count);
        }
        let journal = self
            .validation_journal
            .as_ref()
            .expect("caller checked validation journal")
            .lock()
            .expect("validation journal lock not poisoned");
        let transaction = self.db.begin_read()?;
        let deltas = transaction.open_table(VALIDATION_DELTA_TABLE)?;
        let delta_shards = transaction.open_table(VALIDATION_DELTA_SHARD_TABLE)?;
        if journal.rows.is_empty() {
            drop(delta_shards);
            drop(deltas);
            drop(transaction);
            drop(journal);
            let mut count = 0_u64;
            let mut cursor = None;
            loop {
                let page = self.utxos.snapshot_page(cursor, 4_096)?;
                if page.is_empty() {
                    break;
                }
                for (outpoint, utxo) in &page {
                    visitor(*outpoint, utxo)?;
                    count = count
                        .checked_add(1)
                        .ok_or(UtxoError::Malformed("snapshot UTXO count overflow"))?;
                }
                cursor = page.last().map(|(outpoint, _)| *outpoint);
                if page.len() < 4_096 {
                    break;
                }
            }
            return Ok(count);
        }
        let mut shard_count = VALIDATION_DELTA_SHARD_COUNT;
        for row in &journal.rows {
            let encoded = deltas
                .get(row.height)?
                .ok_or(UtxoError::Malformed("missing validation delta row"))?;
            match validation_delta_record_header(encoded.value())? {
                ValidationDeltaRecordHeader::Legacy { .. } => {
                    // Legacy journals are bounded compatibility state. Keep
                    // their existing path; all newly written rows are sharded.
                    drop(delta_shards);
                    drop(deltas);
                    drop(transaction);
                    drop(journal);
                    let entries = self.snapshot_entries()?;
                    let count = u64::try_from(entries.len()).expect("usize fits u64");
                    for (outpoint, utxo) in &entries {
                        visitor(*outpoint, utxo)?;
                    }
                    return Ok(count);
                }
                ValidationDeltaRecordHeader::Sharded(header) => {
                    shard_count = shard_count.max(header.shard_count);
                }
            }
        }

        let mut count = 0_u64;
        for lexical_shard in 0..shard_count {
            let lexical_shard =
                u8::try_from(lexical_shard).expect("validation shard count fits u8");
            let mut entries = self
                .utxos
                .snapshot_shard_entries(lexical_shard, shard_count)?
                .into_iter()
                .collect::<AHashMap<_, _>>();
            for row in &journal.rows {
                let encoded = deltas
                    .get(row.height)?
                    .ok_or(UtxoError::Malformed("missing validation delta row"))?;
                let ValidationDeltaRecordHeader::Sharded(header) =
                    validation_delta_record_header(encoded.value())?
                else {
                    unreachable!("legacy rows returned through compatibility path");
                };
                let scale = shard_count / header.shard_count;
                let stored_shard = usize::from(lexical_shard) / scale;
                let stored_shard =
                    u8::try_from(stored_shard).expect("validation shard count fits u8");
                if !validation_shard_is_populated(header, stored_shard) {
                    continue;
                }
                let key = validation_delta_shard_key(row.height, stored_shard);
                let encoded = delta_shards
                    .get(key.as_slice())?
                    .ok_or(UtxoError::Malformed("missing validation delta shard"))?;
                let (utxo_count, updates) = decode_validation_delta(encoded.value())?;
                if utxo_count != header.utxo_count {
                    return Err(UtxoError::Malformed(
                        "validation delta shard UTXO count mismatch",
                    ));
                }
                for (outpoint, update) in updates {
                    if validation_delta_shard(outpoint, shard_count) != lexical_shard {
                        continue;
                    }
                    if update.spent_in_batch && entries.remove(&outpoint).is_none() {
                        return Err(UtxoError::Malformed(
                            "validation delta spends an absent output",
                        ));
                    }
                    if let Some(utxo) = update.utxo {
                        if entries.insert(outpoint, utxo).is_some() && !update.spent_in_batch {
                            return Err(UtxoError::Malformed(
                                "validation delta recreates an unspent output",
                            ));
                        }
                    }
                }
            }
            let mut entries = entries.into_iter().collect::<Vec<_>>();
            entries.sort_unstable_by_key(|(outpoint, _)| *outpoint);
            for (outpoint, utxo) in entries {
                visitor(outpoint, &utxo)?;
                count = count
                    .checked_add(1)
                    .ok_or(UtxoError::Malformed("snapshot UTXO count overflow"))?;
            }
        }
        if count != journal.utxo_count {
            return Err(UtxoError::Malformed(
                "validation journal final UTXO count mismatch",
            ));
        }
        Ok(count)
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
        let age_counts = spent_age_counts([(next.height, transaction_undos)])?;
        let _guard = self.lock();
        let mut transaction = self.db.begin_write()?;
        self.configure(&mut transaction);
        let undo = apply_with_undo_transaction(&transaction, spent, created)?;
        if self.options.retain_block_undo {
            insert_undo_transaction(&transaction, next.hash, transaction_undos)?;
        }
        connect_spent_ages_transaction(&transaction, &age_counts, next.height, next.height)?;
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
        let first_height = transitions
            .first()
            .expect("non-empty transitions have a first height")
            .next
            .height;
        let age_counts = spent_age_counts(transitions.iter().map(|transition| {
            (
                transition.next.height,
                transition.transaction_undos.as_slice(),
            )
        }))?;
        let _bulk_guard = bulk_commit_guard();
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
        let (encoded, encoded_shards) = encode_sharded_validation_delta(&updates, next_utxo_count)?;
        let mut bloom = ValidationBloom::with_update_count(updates.len())?;
        let starts_group = journal.rows.len() % VALIDATION_ROWS_PER_BLOOM_GROUP == 0;
        let mut updated_group = if starts_group {
            ValidationBloom::with_update_count(VALIDATION_GROUP_BLOOM_UPDATES)?
        } else {
            journal
                .groups
                .last()
                .expect("continuing validation row has an existing bloom group")
                .clone()
        };
        for outpoint in updates.iter().map(|(outpoint, _)| *outpoint) {
            bloom.insert(outpoint);
            updated_group.insert(outpoint);
        }
        let encoded_bloom = encode_validation_bloom(&bloom, next_utxo_count);
        let encoded_group = encode_validation_bloom(&updated_group, 0);
        let group_index = u32::try_from(journal.rows.len() / VALIDATION_ROWS_PER_BLOOM_GROUP)
            .map_err(|_| UtxoError::Malformed("validation bloom group index overflow"))?;
        let mut transaction = self.db.begin_write()?;
        self.configure(&mut transaction);
        {
            let mut deltas = transaction.open_table(VALIDATION_DELTA_TABLE)?;
            if deltas.get(final_height)?.is_some() {
                return Err(UtxoError::Malformed("duplicate validation delta checkpoint").into());
            }
            deltas.insert(final_height, encoded.as_slice())?;
            let mut delta_shards = transaction.open_table(VALIDATION_DELTA_SHARD_TABLE)?;
            for (shard, encoded_shard) in &encoded_shards {
                let key = validation_delta_shard_key(final_height, *shard);
                delta_shards.insert(key.as_slice(), encoded_shard.as_slice())?;
            }
            let mut blooms = transaction.open_table(VALIDATION_DELTA_BLOOM_TABLE)?;
            blooms.insert(final_height, encoded_bloom.as_slice())?;
            let mut groups = transaction.open_table(VALIDATION_GROUP_BLOOM_TABLE)?;
            groups.insert(group_index, encoded_group.as_slice())?;
        }
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
        connect_spent_ages_transaction(&transaction, &age_counts, first_height, final_height)?;
        transaction.commit()?;
        if starts_group {
            journal.groups.push(updated_group);
        } else {
            *journal
                .groups
                .last_mut()
                .expect("validation row has a bloom group") = updated_group;
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
        let first_height = transitions
            .first()
            .expect("non-empty transitions have a first height")
            .next
            .height;
        let final_height = transitions
            .last()
            .expect("non-empty transitions have a final height")
            .next
            .height;
        let age_counts = spent_age_counts(transitions.iter().map(|transition| {
            (
                transition.next.height,
                transition.transaction_undos.as_slice(),
            )
        }))?;
        let _bulk_guard = bulk_commit_guard();
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
        connect_spent_ages_transaction(&transaction, &age_counts, first_height, final_height)?;
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
        transaction_undos: &[UtxoUndo],
    ) -> Result<UtxoUndo, ChainStoreError> {
        if self.validation_journal.as_ref().is_some_and(|journal| {
            !journal
                .lock()
                .expect("validation journal lock not poisoned")
                .rows
                .is_empty()
        }) {
            // Reorganizations are rare during bulk catch-up. Materialize the
            // complete overlay first so the ordinary block-addressable undo
            // path remains the single rollback implementation.
            self.materialize_validation_deltas()?;
        }
        let age_counts = spent_age_counts([(expected_current.height, transaction_undos)])?;
        let _bulk_guard = bulk_commit_guard();
        let _guard = self.lock();
        let mut transaction = self.db.begin_write()?;
        self.configure(&mut transaction);
        let undo = apply_with_undo_transaction(&transaction, spent, created)?;
        disconnect_spent_ages_transaction(
            &transaction,
            &age_counts,
            expected_current.height,
            parent.height,
        )?;
        rewind_transaction(&transaction, expected_current, parent)?;
        if !remove_undo_transaction(&transaction, expected_current.hash)? {
            return Err(UndoStoreError::Malformed("missing atomic disconnect undo").into());
        }
        transaction.commit()?;
        Ok(undo)
    }

    /// Reads the exact, network-scoped spent-output age histogram.
    ///
    /// Coverage begins with the first block connected after this telemetry
    /// schema is available. Connects and disconnects update it in the same
    /// transaction as chainstate, so reorgs cannot leave stale samples.
    pub fn spent_age_histogram(&self) -> Result<SpentAgeHistogram, ChainStoreError> {
        let transaction = self.db.begin_read()?;
        let metadata = match transaction.open_table(SPENT_AGE_META_TABLE) {
            Ok(metadata) => metadata,
            Err(redb::TableError::TableDoesNotExist(_)) => {
                return match transaction.open_table(SPENT_AGE_TABLE) {
                    Err(redb::TableError::TableDoesNotExist(_)) => Ok(SpentAgeHistogram::default()),
                    Err(error) => Err(error.into()),
                    Ok(_) => Err(UtxoError::Malformed(
                        "spent-age rows exist without coverage metadata",
                    )
                    .into()),
                };
            }
            Err(error) => return Err(error.into()),
        };
        let start_height = metadata
            .get(SPENT_AGE_START_HEIGHT)?
            .map(|value| value.value());
        let end_height = metadata
            .get(SPENT_AGE_END_HEIGHT)?
            .map(|value| value.value());
        if start_height.is_some() != end_height.is_some() {
            return Err(UtxoError::Malformed("incomplete spent-age metadata").into());
        }
        let histogram = transaction.open_table(SPENT_AGE_TABLE)?;
        let mut rows = Vec::new();
        let mut samples = 0_u64;
        for row in histogram.range(0..=u32::MAX)? {
            let (age, count) = row?;
            let count = count.value();
            if count == 0 {
                return Err(UtxoError::Malformed("empty spent-age histogram row").into());
            }
            samples = samples
                .checked_add(count)
                .ok_or(UtxoError::Malformed("spent-age sample overflow"))?;
            rows.push((age.value(), count));
        }
        if start_height.is_none() && !rows.is_empty() {
            return Err(
                UtxoError::Malformed("spent-age rows exist without coverage metadata").into(),
            );
        }
        Ok(SpentAgeHistogram {
            start_height,
            end_height,
            samples,
            rows,
        })
    }

    fn configure(&self, transaction: &mut redb::WriteTransaction) {
        transaction.set_durability(Durability::Immediate);
        transaction.set_quick_repair(self.options.quick_repair);
    }

    fn lookup_validation_shards_parallel(
        &self,
        jobs: &[ValidationShardReadJob],
        outpoints: &[OutPointKey],
    ) -> Result<Vec<ValidationShardMatch>, UtxoError> {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let available_workers =
            std::thread::available_parallelism().map_or(1, std::num::NonZero::get);
        let workers = available_workers.min(jobs.len());
        let next_job = AtomicUsize::new(0);
        std::thread::scope(|scope| {
            let workers = (0..workers)
                .map(|_| {
                    scope.spawn(|| {
                        let transaction = self.db.begin_read()?;
                        let shards = transaction.open_table(VALIDATION_DELTA_SHARD_TABLE)?;
                        let mut matches = Vec::new();
                        loop {
                            let job_index = next_job.fetch_add(1, Ordering::Relaxed);
                            let Some(job) = jobs.get(job_index) else {
                                break;
                            };
                            let key = validation_delta_shard_key(job.height, job.shard);
                            let encoded = shards
                                .get(key.as_slice())?
                                .ok_or(UtxoError::Malformed("missing validation delta shard"))?;
                            for &index in &job.candidates {
                                let outpoint = outpoints[index];
                                if let Some(update) =
                                    validation_delta_lookup(encoded.value(), outpoint)?
                                {
                                    matches.push((job.row_index, index, outpoint, update.utxo));
                                }
                            }
                        }
                        Ok::<_, UtxoError>(matches)
                    })
                })
                .collect::<Vec<_>>();
            let mut loaded = Vec::new();
            for worker in workers {
                loaded.extend(
                    worker
                        .join()
                        .expect("validation shard read worker must not panic")?,
                );
            }
            loaded.sort_unstable_by_key(|(row_index, ..)| std::cmp::Reverse(*row_index));
            Ok(loaded)
        })
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
            let delta_shards = transaction.open_table(VALIDATION_DELTA_SHARD_TABLE)?;
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
                    let update = match validation_delta_record_header(encoded.value())? {
                        ValidationDeltaRecordHeader::Legacy { .. } => {
                            validation_delta_lookup(encoded.value(), outpoint)?
                        }
                        ValidationDeltaRecordHeader::Sharded(header) => {
                            let shard = validation_delta_shard(outpoint, header.shard_count);
                            if validation_shard_is_populated(header, shard) {
                                let key = validation_delta_shard_key(row.height, shard);
                                let encoded_shard = delta_shards.get(key.as_slice())?.ok_or(
                                    UtxoError::Malformed("missing validation delta shard"),
                                )?;
                                validation_delta_lookup(encoded_shard.value(), outpoint)?
                            } else {
                                None
                            }
                        }
                    };
                    if let Some(update) = update {
                        return Ok(update.utxo);
                    }
                }
            }
        }
        self.utxos.get(outpoint)
    }

    #[allow(clippy::too_many_lines)]
    fn get_many(
        &self,
        outpoints: &[OutPointKey],
    ) -> Result<Vec<(OutPointKey, Option<Utxo>)>, UtxoError> {
        let Some(journal) = &self.validation_journal else {
            return self.utxos.get_many(outpoints);
        };
        let mut journal = journal
            .lock()
            .expect("validation journal lock not poisoned");
        let transaction = self.db.begin_read()?;
        let deltas = transaction.open_table(VALIDATION_DELTA_TABLE)?;
        let delta_shards = transaction.open_table(VALIDATION_DELTA_SHARD_TABLE)?;
        let mut results = vec![None; outpoints.len()];
        let mut unresolved = (0..outpoints.len()).collect::<Vec<_>>();
        let mut legacy_hits = Vec::new();
        for (group_index, group) in journal.groups.iter().enumerate().rev() {
            if unresolved.is_empty() {
                break;
            }
            let (mut group_unresolved, mut next_unresolved) =
                partition_validation_bloom_matches(group, outpoints, unresolved);
            let start = group_index * VALIDATION_ROWS_PER_BLOOM_GROUP;
            let end = (start + VALIDATION_ROWS_PER_BLOOM_GROUP).min(journal.rows.len());
            let rows = &journal.rows[start..end];
            let mut row_candidates = vec![Vec::new(); rows.len()];
            for index in &group_unresolved {
                for (row_index, row) in rows.iter().enumerate() {
                    if row.bloom.might_contain(outpoints[*index]) {
                        row_candidates[row_index].push(*index);
                    }
                }
            }
            let mut row_plans = Vec::with_capacity(rows.len());
            let mut all_populated_rows_are_sharded = true;
            for (row, candidates) in rows.iter().zip(row_candidates) {
                if candidates.is_empty() {
                    row_plans.push(ValidationRowReadPlan::Empty);
                    continue;
                }
                let encoded = deltas
                    .get(row.height)?
                    .ok_or(UtxoError::Malformed("missing validation delta row"))?;
                match validation_delta_record_header(encoded.value())? {
                    ValidationDeltaRecordHeader::Legacy { .. } => {
                        all_populated_rows_are_sharded = false;
                        row_plans.push(ValidationRowReadPlan::Legacy(candidates));
                    }
                    ValidationDeltaRecordHeader::Sharded(header) => {
                        row_plans.push(ValidationRowReadPlan::Sharded { header, candidates });
                    }
                }
            }
            if all_populated_rows_are_sharded {
                let mut jobs = Vec::new();
                for (row_index, (row, plan)) in rows.iter().zip(&row_plans).enumerate().rev() {
                    let ValidationRowReadPlan::Sharded { header, candidates } = plan else {
                        continue;
                    };
                    let mut shard_candidates = vec![Vec::new(); header.shard_count];
                    for &index in candidates {
                        let shard = validation_delta_shard(outpoints[index], header.shard_count);
                        if validation_shard_is_populated(*header, shard) {
                            shard_candidates[usize::from(shard)].push(index);
                        }
                    }
                    jobs.extend(
                        shard_candidates
                            .into_iter()
                            .enumerate()
                            .filter(|(_, candidates)| !candidates.is_empty())
                            .map(|(shard, candidates)| ValidationShardReadJob {
                                height: row.height,
                                row_index,
                                shard: u8::try_from(shard).expect("validation shard fits u8"),
                                candidates,
                            }),
                    );
                }
                for (_row_index, index, outpoint, utxo) in
                    self.lookup_validation_shards_parallel(&jobs, outpoints)?
                {
                    if results[index].is_none() {
                        results[index] = Some((outpoint, utxo));
                    }
                }
            } else {
                for (row_index, (row, plan)) in rows.iter().zip(row_plans).enumerate().rev() {
                    match plan {
                        ValidationRowReadPlan::Empty => {}
                        ValidationRowReadPlan::Legacy(candidates) => {
                            let encoded = deltas
                                .get(row.height)?
                                .ok_or(UtxoError::Malformed("missing validation delta row"))?;
                            legacy_hits.push((
                                row.height,
                                u64::try_from(candidates.len()).unwrap_or(u64::MAX),
                            ));
                            for index in candidates {
                                if results[index].is_some() {
                                    continue;
                                }
                                let outpoint = outpoints[index];
                                if let Some(update) =
                                    validation_delta_lookup(encoded.value(), outpoint)?
                                {
                                    results[index] = Some((outpoint, update.utxo));
                                }
                            }
                        }
                        ValidationRowReadPlan::Sharded { header, candidates } => {
                            let mut shard_candidates = vec![Vec::new(); header.shard_count];
                            for index in candidates {
                                if results[index].is_some() {
                                    continue;
                                }
                                let shard =
                                    validation_delta_shard(outpoints[index], header.shard_count);
                                if validation_shard_is_populated(header, shard) {
                                    shard_candidates[usize::from(shard)].push(index);
                                }
                            }
                            let jobs = shard_candidates
                                .into_iter()
                                .enumerate()
                                .filter(|(_, candidates)| !candidates.is_empty())
                                .map(|(shard, candidates)| ValidationShardReadJob {
                                    height: row.height,
                                    row_index,
                                    shard: u8::try_from(shard).expect("validation shard fits u8"),
                                    candidates,
                                })
                                .collect::<Vec<_>>();
                            for (_, index, outpoint, utxo) in
                                self.lookup_validation_shards_parallel(&jobs, outpoints)?
                            {
                                results[index] = Some((outpoint, utxo));
                            }
                        }
                    }
                }
            }
            group_unresolved.retain(|index| results[*index].is_none());
            next_unresolved.extend(group_unresolved);
            unresolved = next_unresolved;
        }
        for (height, hits) in legacy_hits {
            let entry = journal.legacy_hits.entry(height).or_default();
            *entry = entry.saturating_add(hits);
        }
        drop(deltas);
        drop(delta_shards);
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
            let delta_shards = transaction.open_table(VALIDATION_DELTA_SHARD_TABLE)?;
            for row in &journal.rows {
                let encoded = deltas
                    .get(row.height)?
                    .ok_or(UtxoError::Malformed("missing validation delta row"))?;
                let decoded = decode_validation_delta_record(encoded.value(), |shard| {
                    let key = validation_delta_shard_key(row.height, shard);
                    delta_shards
                        .get(key.as_slice())?
                        .map(|encoded| encoded.value().to_vec())
                        .ok_or(UtxoError::Malformed("missing validation delta shard"))
                })?;
                for (outpoint, update) in decoded.1 {
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
    fn spent_age_histogram_is_atomic_sorted_and_reorg_reversible() {
        let directory = TempDir::new().unwrap();
        let store =
            RedbChainStore::open(directory.path().join("chainstate.redb"), Network::Regtest)
                .unwrap();
        assert_eq!(
            store.spent_age_histogram().unwrap(),
            SpentAgeHistogram::default()
        );
        let genesis = store.execution().tip().unwrap();
        let old = coin(10);
        store.apply(&[], &[(key(1), old.clone())]).unwrap();
        let next = ExecutionTip {
            height: 1,
            hash: BlockHash::from_byte_array([21; 32]),
        };
        let transaction_undos = vec![UtxoUndo::from_parts(
            vec![(key(1), old.clone())],
            Vec::new(),
        )];
        store
            .commit_connect(genesis.hash, next, &[key(1)], &[], &transaction_undos)
            .unwrap();
        assert_eq!(
            store.spent_age_histogram().unwrap(),
            SpentAgeHistogram {
                start_height: Some(1),
                end_height: Some(1),
                samples: 1,
                rows: vec![(1, 1)],
            }
        );
        assert_eq!(store.spent_age_histogram().unwrap().hits_within(0), 0);
        assert_eq!(store.spent_age_histogram().unwrap().hits_within(1), 1);

        store
            .commit_disconnect(next, genesis, &[], &[(key(1), old)], &transaction_undos)
            .unwrap();
        assert_eq!(
            store.spent_age_histogram().unwrap(),
            SpentAgeHistogram::default()
        );
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
        let validation = RedbChainStore::open_with_options(
            directory.path().join("validation.redb"),
            Network::Regtest,
            ChainStoreOptions {
                retain_block_undo: false,
                validation_delta_journal: true,
                ..ChainStoreOptions::default()
            },
        )
        .unwrap();
        active
            .execution()
            .bind_consensus_config(b"rules", b"rules", b"rules")
            .unwrap();
        validation
            .execution()
            .bind_consensus_config(b"rules", b"rules", b"rules")
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
            .commit_connect_batch(&[ConnectTransition {
                expected_parent: genesis.hash,
                next: anchor,
                spent: Vec::new(),
                created: independently_replayed,
                transaction_undos: Vec::new(),
            }])
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
            .bind_consensus_config(b"rules", b"rules", b"rules")
            .unwrap();
        validation
            .execution()
            .bind_consensus_config(b"rules", b"rules", b"rules")
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
        let expected = updates.clone().into_iter().collect::<Vec<_>>();
        assert_eq!(decoded, expected);

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

        let mut bloom = ValidationBloom::with_update_count(4).unwrap();
        for value in 1..=4 {
            bloom.insert(key(value));
        }
        let encoded_bloom = encode_validation_bloom(&bloom, 77);
        assert_eq!(
            decode_validation_bloom(&encoded_bloom, bloom.bits.len())
                .unwrap()
                .0,
            77
        );
        let mut damaged_bloom = encoded_bloom.clone();
        *damaged_bloom.last_mut().unwrap() ^= 1;
        assert!(matches!(
            decode_validation_bloom(&damaged_bloom, bloom.bits.len()),
            Err(UtxoError::Malformed("validation bloom checksum"))
        ));
    }

    #[test]
    fn sharded_validation_delta_is_sorted_and_requires_every_manifest_shard() {
        let updates = BTreeMap::from([
            (
                key(1),
                ValidationUpdate {
                    spent_in_batch: true,
                    utxo: None,
                },
            ),
            (
                key(32),
                ValidationUpdate {
                    spent_in_batch: false,
                    utxo: Some(coin(20)),
                },
            ),
            (
                key(64),
                ValidationUpdate {
                    spent_in_batch: true,
                    utxo: Some(coin(30)),
                },
            ),
        ])
        .into_iter()
        .collect::<Vec<_>>();
        let (manifest, shards) = encode_sharded_validation_delta(&updates, 77).unwrap();
        assert_eq!(&manifest[..4], &VALIDATION_COMPACT_SHARDED_DELTA_MAGIC);
        assert_eq!(
            shards.iter().map(|(shard, _)| *shard).collect::<Vec<_>>(),
            vec![0, 2, 4]
        );
        let (utxo_count, decoded) = decode_validation_delta_record(&manifest, |requested| {
            shards
                .iter()
                .find(|(shard, _)| *shard == requested)
                .map(|(_, encoded)| encoded.clone())
                .ok_or(UtxoError::Malformed("missing validation delta shard"))
        })
        .unwrap();
        assert_eq!(utxo_count, 77);
        assert_eq!(decoded, updates);
        assert!(matches!(
            decode_validation_delta_record(&manifest, |_| {
                Err(UtxoError::Malformed("missing validation delta shard"))
            }),
            Err(UtxoError::Malformed("missing validation delta shard"))
        ));

        let mut rvd4_bitmap = [0_u8; VALIDATION_DELTA_SHARD_BITMAP_BYTES];
        let mut rvd4_shards = Vec::new();
        for (outpoint, update) in &updates {
            let shard = validation_delta_shard(*outpoint, VALIDATION_RVD4_SHARD_COUNT);
            rvd4_bitmap[usize::from(shard) / 8] |= 1 << (shard % 8);
            rvd4_shards.push((
                shard,
                encode_validation_delta(std::iter::once((outpoint, update)), 77).unwrap(),
            ));
        }
        let mut rvd4_manifest = Vec::with_capacity(VALIDATION_SHARDED_DELTA_HEADER_BYTES);
        rvd4_manifest.extend_from_slice(&VALIDATION_SHARDED_DELTA_MAGIC);
        rvd4_manifest.extend_from_slice(&77_u64.to_le_bytes());
        rvd4_manifest.extend_from_slice(&3_u32.to_le_bytes());
        rvd4_manifest.extend_from_slice(&rvd4_bitmap);
        let (_, decoded_rvd4) = decode_validation_delta_record(&rvd4_manifest, |requested| {
            rvd4_shards
                .iter()
                .find(|(shard, _)| *shard == requested)
                .map(|(_, encoded)| encoded.clone())
                .ok_or(UtxoError::Malformed("missing validation delta shard"))
        })
        .unwrap();
        assert_eq!(decoded_rvd4, updates);
    }

    #[test]
    fn parallel_validation_bloom_partition_preserves_serial_order() {
        let outpoints = (0..(MIN_PARALLEL_VALIDATION_BLOOM_KEYS * 3))
            .map(|index| {
                OutPoint::new(
                    Txid::from_byte_array([u8::try_from(index % 251).unwrap(); 32]),
                    u32::try_from(index).unwrap(),
                )
                .into()
            })
            .collect::<Vec<_>>();
        let mut bloom = ValidationBloom::with_update_count(outpoints.len()).unwrap();
        for outpoint in outpoints.iter().step_by(3).copied() {
            bloom.insert(outpoint);
        }
        let indices = (0..outpoints.len()).rev().collect::<Vec<_>>();
        let expected: (Vec<_>, Vec<_>) = indices
            .iter()
            .copied()
            .partition(|index| bloom.might_contain(outpoints[*index]));

        assert_eq!(
            partition_validation_bloom_matches(&bloom, &outpoints, indices),
            expected
        );
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

        let store =
            RedbChainStore::open_with_options(&path, Network::Regtest, base_options).unwrap();
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
    fn parallel_validation_shard_reads_preserve_caller_order() {
        let directory = TempDir::new().unwrap();
        let store = RedbChainStore::open_with_options(
            directory.path().join("chainstate.redb"),
            Network::Regtest,
            ChainStoreOptions {
                retain_block_undo: false,
                validation_delta_journal: true,
                ..ChainStoreOptions::default()
            },
        )
        .unwrap();
        let genesis = store.execution().tip().unwrap();
        let keys = (0_u8..16).map(|shard| key(shard << 4)).collect::<Vec<_>>();
        let created = keys
            .iter()
            .enumerate()
            .map(|(index, outpoint)| {
                (
                    *outpoint,
                    coin(u64::try_from(index).expect("small index") + 1),
                )
            })
            .collect::<Vec<_>>();
        let first = ExecutionTip {
            height: 1,
            hash: BlockHash::from_byte_array([59; 32]),
        };
        store
            .commit_connect_batch(&[ConnectTransition {
                expected_parent: genesis.hash,
                next: first,
                spent: Vec::new(),
                created: created.clone(),
                transaction_undos: Vec::new(),
            }])
            .unwrap();
        store
            .commit_connect_batch(&[ConnectTransition {
                expected_parent: first.hash,
                next: ExecutionTip {
                    height: 2,
                    hash: BlockHash::from_byte_array([60; 32]),
                },
                spent: vec![keys[0]],
                created: Vec::new(),
                transaction_undos: Vec::new(),
            }])
            .unwrap();

        let requested = keys.iter().copied().rev().collect::<Vec<_>>();
        let expected = created
            .iter()
            .rev()
            .map(|(outpoint, coin)| (*outpoint, (*outpoint != keys[0]).then_some(coin.clone())))
            .collect::<Vec<_>>();
        assert_eq!(store.get_many(&requested).unwrap(), expected);
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
    fn journal_identity_streams_a_nonempty_base_by_prefix() {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("chainstate.redb");
        let base_options = ChainStoreOptions {
            retain_block_undo: false,
            ..ChainStoreOptions::default()
        };
        let store =
            RedbChainStore::open_with_options(&path, Network::Regtest, base_options).unwrap();
        store
            .apply(&[], &[(key(1), coin(1)), (key(240), coin(2))])
            .unwrap();
        let genesis = store.execution().tip().unwrap();
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
        store
            .commit_connect_batch(&[ConnectTransition {
                expected_parent: genesis.hash,
                next: ExecutionTip {
                    height: 1,
                    hash: BlockHash::from_byte_array([91; 32]),
                },
                spent: vec![key(1)],
                created: vec![(key(16), coin(3)), (key(255), coin(4))],
                transaction_undos: Vec::new(),
            }])
            .unwrap();
        let expected =
            BTreeMap::from([(key(16), coin(3)), (key(240), coin(2)), (key(255), coin(4))]);
        let (count, bytes, digest) = store.snapshot_content_identity().unwrap();
        assert_eq!(count, 3);
        assert_eq!(bytes, snapshot_bytes(&expected));
        assert_eq!(
            digest,
            identity_from_sorted_entries(expected.into_iter())
                .unwrap()
                .2
        );
    }

    #[test]
    fn validation_journal_pages_merge_removals_replacements_and_new_keys() {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("chainstate.redb");
        let base_options = ChainStoreOptions {
            retain_block_undo: false,
            ..ChainStoreOptions::default()
        };
        let store =
            RedbChainStore::open_with_options(&path, Network::Regtest, base_options).unwrap();
        store
            .apply(
                &[],
                &[
                    (key(1), coin(1)),
                    (key(4), coin(4)),
                    (key(7), coin(7)),
                    (key(10), coin(10)),
                ],
            )
            .unwrap();
        let genesis = store.execution().tip().unwrap();
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
        let first = ExecutionTip {
            height: 1,
            hash: BlockHash::from_byte_array([92; 32]),
        };
        store
            .commit_connect_batch(&[ConnectTransition {
                expected_parent: genesis.hash,
                next: first,
                spent: vec![key(1), key(4)],
                created: vec![
                    (key(2), coin(2)),
                    (key(4), coin(40)),
                    (key(6), coin(6)),
                    (key(9), coin(9)),
                ],
                transaction_undos: Vec::new(),
            }])
            .unwrap();
        store
            .commit_connect_batch(&[ConnectTransition {
                expected_parent: first.hash,
                next: ExecutionTip {
                    height: 2,
                    hash: BlockHash::from_byte_array([93; 32]),
                },
                spent: vec![key(6)],
                created: vec![(key(5), coin(5))],
                transaction_undos: Vec::new(),
            }])
            .unwrap();

        let expected = vec![
            (key(2), coin(2)),
            (key(4), coin(40)),
            (key(5), coin(5)),
            (key(7), coin(7)),
            (key(9), coin(9)),
            (key(10), coin(10)),
        ];
        assert!(store.utxo_snapshot_page(None, 0).unwrap().is_empty());
        let first_page = store.utxo_snapshot_page(None, 2).unwrap();
        let second_page = store
            .utxo_snapshot_page(first_page.last().map(|(outpoint, _)| *outpoint), 2)
            .unwrap();
        let third_page = store
            .utxo_snapshot_page(second_page.last().map(|(outpoint, _)| *outpoint), 2)
            .unwrap();
        let end = store
            .utxo_snapshot_page(third_page.last().map(|(outpoint, _)| *outpoint), 2)
            .unwrap();
        assert_eq!([first_page, second_page, third_page].concat(), expected);
        assert!(end.is_empty());
        assert_eq!(
            store.utxo_snapshot_page(Some(key(1)), 3).unwrap(),
            expected[..3]
        );
    }

    #[test]
    fn hot_legacy_validation_row_migrates_atomically_to_sorted_shards() {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("chainstate.redb");
        let options = ChainStoreOptions {
            retain_block_undo: false,
            validation_delta_journal: true,
            ..ChainStoreOptions::default()
        };
        let store = RedbChainStore::open_with_options(&path, Network::Regtest, options).unwrap();
        let genesis = store.execution().tip().unwrap();
        store
            .commit_connect_batch(&[ConnectTransition {
                expected_parent: genesis.hash,
                next: ExecutionTip {
                    height: 1,
                    hash: BlockHash::from_byte_array([56; 32]),
                },
                spent: vec![],
                created: vec![(key(1), coin(10)), (key(2), coin(20))],
                transaction_undos: vec![],
            }])
            .unwrap();
        let legacy_updates =
            fold_validation_updates(&[], vec![(key(1), coin(10)), (key(2), coin(20))]);
        let legacy = encode_validation_delta(
            legacy_updates
                .iter()
                .map(|(outpoint, update)| (outpoint, update)),
            2,
        )
        .unwrap();
        let transaction = store.db.begin_write().unwrap();
        {
            let mut deltas = transaction.open_table(VALIDATION_DELTA_TABLE).unwrap();
            deltas.insert(1, legacy.as_slice()).unwrap();
            let mut shards = transaction
                .open_table(VALIDATION_DELTA_SHARD_TABLE)
                .unwrap();
            shards.retain(|_, _| false).unwrap();
        }
        transaction.commit().unwrap();

        assert_eq!(
            store.get_many(&[key(2), key(3)]).unwrap(),
            vec![(key(2), Some(coin(20))), (key(3), None)]
        );
        assert_eq!(store.take_hottest_legacy_validation_delta(), Some(1));
        let migrated = store.shard_legacy_validation_delta(1).unwrap().unwrap();
        assert_eq!(migrated.height, 1);
        assert_eq!(migrated.legacy_bytes, u64::try_from(legacy.len()).unwrap());
        assert_eq!(migrated.shard_count, 1);
        assert!(store.take_hottest_legacy_validation_delta().is_none());
        drop(store);

        let reopened = RedbChainStore::open_with_options(&path, Network::Regtest, options).unwrap();
        assert_eq!(reopened.get(key(1)).unwrap(), Some(coin(10)));
        assert_eq!(reopened.get(key(2)).unwrap(), Some(coin(20)));
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

        let database = Database::open(&path).unwrap();
        let transaction = database.begin_write().unwrap();
        {
            let mut rows = transaction
                .open_table(VALIDATION_DELTA_BLOOM_TABLE)
                .unwrap();
            rows.retain(|_, _| false).unwrap();
            let mut groups = transaction
                .open_table(VALIDATION_GROUP_BLOOM_TABLE)
                .unwrap();
            groups.retain(|_, _| false).unwrap();
        }
        transaction.commit().unwrap();
        drop(database);

        let store =
            RedbChainStore::open_with_options(&path, Network::Regtest, journal_options).unwrap();
        {
            let transaction = store.db.begin_read().unwrap();
            let rows = transaction
                .open_table(VALIDATION_DELTA_BLOOM_TABLE)
                .unwrap();
            let groups = transaction
                .open_table(VALIDATION_GROUP_BLOOM_TABLE)
                .unwrap();
            assert_eq!(rows.len().unwrap(), 18);
            assert_eq!(groups.len().unwrap(), 2);
        }
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
    fn retained_undo_journal_materializes_before_disconnect() {
        let directory = TempDir::new().unwrap();
        let store = RedbChainStore::open_with_options(
            directory.path().join("chainstate.redb"),
            Network::Regtest,
            ChainStoreOptions {
                validation_delta_journal: true,
                ..ChainStoreOptions::default()
            },
        )
        .unwrap();
        let genesis = store.execution().tip().unwrap();
        let next = ExecutionTip {
            height: 1,
            hash: BlockHash::from_byte_array([49; 32]),
        };
        let block_undo = UtxoUndo::new(Vec::new(), vec![key(1)]);
        store
            .commit_connect_batch(&[ConnectTransition {
                expected_parent: genesis.hash,
                next,
                spent: Vec::new(),
                created: vec![(key(1), coin(1))],
                transaction_undos: vec![block_undo.clone()],
            }])
            .unwrap();
        assert_eq!(
            store.undos().get(next.hash).unwrap(),
            Some(vec![block_undo.clone()])
        );
        assert_eq!(store.get(key(1)).unwrap(), Some(coin(1)));

        store
            .commit_disconnect(next, genesis, &[key(1)], &[], &[block_undo])
            .unwrap();
        assert_eq!(store.execution().tip().unwrap(), genesis);
        assert_eq!(store.get(key(1)).unwrap(), None);
        assert_eq!(store.undos().get(next.hash).unwrap(), None);
        assert_eq!(store.materialize_validation_deltas().unwrap(), 0);
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
    fn block_undo_pruning_tracks_the_retained_block_floor() {
        let directory = TempDir::new().unwrap();
        let store =
            RedbChainStore::open(directory.path().join("chainstate.redb"), Network::Regtest)
                .unwrap();
        let mut headers = HeaderDag::new(Network::Regtest);
        let mut parent = headers.active_tip();
        let mut tips = Vec::new();
        for height in 1..=4 {
            let header = mine_child(parent.hash, parent.header.time + 1);
            let next = ExecutionTip {
                height,
                hash: header.block_hash(),
            };
            headers.insert(header).unwrap();
            store
                .commit_connect(parent.hash, next, &[], &[], &[])
                .unwrap();
            tips.push(next);
            parent = headers.active_tip();
        }

        assert_eq!(store.prune_block_undos_before(&headers, 3).unwrap(), 2);
        assert!(store.undos().get(tips[0].hash).unwrap().is_none());
        assert!(store.undos().get(tips[1].hash).unwrap().is_none());
        assert!(store.undos().get(tips[2].hash).unwrap().is_some());
        assert!(store.undos().get(tips[3].hash).unwrap().is_some());
        assert_eq!(store.prune_block_undos_before(&headers, 3).unwrap(), 0);
        assert_eq!(store.execution().tip().unwrap(), tips[3]);
    }

    #[test]
    fn block_undo_pruning_fails_closed_for_an_unknown_header() {
        let directory = TempDir::new().unwrap();
        let store =
            RedbChainStore::open(directory.path().join("chainstate.redb"), Network::Regtest)
                .unwrap();
        let genesis = store.execution().tip().unwrap();
        let unknown = ExecutionTip {
            height: 1,
            hash: BlockHash::from_byte_array([91; 32]),
        };
        store
            .commit_connect(genesis.hash, unknown, &[], &[], &[])
            .unwrap();

        assert!(matches!(
            store.prune_block_undos_before(&HeaderDag::new(Network::Regtest), 2),
            Err(ChainStoreError::Undo(UndoStoreError::Malformed(
                "block undo references an unknown header"
            )))
        ));
        assert!(store.undos().get(unknown.hash).unwrap().is_some());
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
