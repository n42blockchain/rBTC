//! Optional durable MDBX implementation of the hot/cold UTXO interface.
//!
//! This backend is experimental and is not selected by the node yet. It does,
//! however, own compact coins, compact block undo, creation-MTP metadata, and
//! the execution tip in one environment so a chain transition is one durable
//! MDBX transaction rather than split storage.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    sync::{Mutex, MutexGuard},
};

use bitcoin::{BlockHash, OutPoint, Txid, hashes::Hash};
use libmdbx::{
    Database, DatabaseKind, DatabaseOptions, Mode, NoWriteMap, RO, ReadWriteOptions, SyncMode,
    Table, TableFlags, Transaction, TransactionKind, WriteFlags,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{
    chain_store::{ChainStoreError, ConnectTransition, ExecutionChainStore},
    execution_store::ExecutionTip,
    headers::HeaderDag,
    utxo::{OutPointKey, TierStats, Utxo, UtxoError, UtxoStore, UtxoUndo},
};

const HOT: &str = "utxo_hot";
const COLD: &str = "utxo_cold";
const UNDO: &str = "undo";
const META: &str = "meta";
const FORMAT_KEY: &[u8] = b"format";
const TIP_KEY: &[u8] = b"tip";
const COMPACTION_MANIFEST_FILE: &str = ".rbtc-mdbx-compaction.json";
const COMPACTION_MANIFEST_SCHEMA: u32 = 1;
const MAINTENANCE_STATE_FILE: &str = ".rbtc-mdbx-maintenance.json";
const MAINTENANCE_STATE_SCHEMA: u32 = 1;
const FORMAT_VERSION: u32 = 2;
const UNDO_FORMAT_VERSION: u32 = 1;
/// The IBD checkpoint size whose net UTXO effect is folded into one write.
pub const MAX_ATOMIC_IBD_BATCH_BLOCKS: usize = 256;
type FoldedBatchChanges = (Vec<OutPointKey>, Vec<(OutPointKey, Utxo)>);
/// Three years at Bitcoin's target ten-minute spacing.
pub const DEFAULT_HOT_WINDOW_BLOCKS: u32 = 3 * 365 * 24 * 6;
/// Default hard geometry ceiling for a complete MDBX chainstate.
pub const DEFAULT_CHAINSTATE_CAPACITY_BYTES: u64 = 128 * 1024 * 1024 * 1024;
/// Start considering compact-copy at 55% of the 128 GiB geometry ceiling.
///
/// The supplied 169M-entry chainstate occupied 76.01 GB before compaction,
/// about 55% of this ceiling, while its live pages occupied 50.26 GB. This
/// leaves ample headroom for one checkpoint and the verified copy/swap.
pub const DEFAULT_COMPACTION_TRIGGER_PERCENT: u8 = 55;
/// Require 50% growth over the last post-compaction high-water mark.
///
/// A percentage trigger alone would immediately recompact a live set whose
/// irreducible size exceeds the trigger. The growth guard turns compaction
/// frequency into a function of newly accumulated garbage instead.
pub const DEFAULT_RECOMPACT_GROWTH_PERCENT: u8 = 50;
/// Free space preserved after writing a verified compact copy.
pub const DEFAULT_COMPACTION_FREE_SPACE_RESERVE_BYTES: u64 = 16 * 1024 * 1024 * 1024;

/// MDBX high-water allocation against its hard geometry ceiling.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MdbxChainstateCapacity {
    /// Bytes covered by the highest allocated page.
    pub used_bytes: u64,
    /// Configured hard geometry ceiling.
    pub capacity_bytes: u64,
}

impl MdbxChainstateCapacity {
    /// Returns high-water use as a whole percentage of the geometry ceiling.
    #[must_use]
    pub fn used_percent(self) -> u8 {
        if self.capacity_bytes == 0 {
            return 100;
        }
        u8::try_from((u128::from(self.used_bytes) * 100 / u128::from(self.capacity_bytes)).min(100))
            .expect("bounded percentage fits u8")
    }
}

/// Deep storage accounting for one complete four-table MDBX chainstate.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MdbxChainstateAudit {
    /// Highest allocated page boundary; this is what approaches `MDBX_MAP_FULL`.
    pub high_water_bytes: u64,
    /// Pages reachable from the four named tables.
    pub live_page_bytes: u64,
    /// Pages currently recorded on MDBX's freelist.
    pub free_page_bytes: u64,
    /// Sum of encoded keys and values in the four named tables.
    pub record_bytes: u64,
    /// Logical length of `mdbx.dat`.
    pub file_bytes: u64,
    /// Filesystem blocks allocated to `mdbx.dat`.
    pub allocated_bytes: u64,
    /// Configured hard geometry ceiling.
    pub capacity_bytes: u64,
    /// Entries in `utxo_hot`.
    pub hot_entries: u64,
    /// Entries in `utxo_cold`.
    pub cold_entries: u64,
    /// Entries in `undo`.
    pub undo_entries: u64,
    /// Entries in `meta`.
    pub meta_entries: u64,
    /// Execution tip included in the audited snapshot.
    pub tip: Option<ExecutionTip>,
    /// SHA-256 over table names and every ordered key/value pair.
    pub content_sha256: [u8; 32],
}

/// Cheap physical accounting that does not hash every chainstate record.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MdbxChainstateMetrics {
    /// Highest allocated page boundary.
    pub high_water_bytes: u64,
    /// Pages reachable from the four named tables.
    pub live_page_bytes: u64,
    /// Pages recorded on the freelist.
    pub free_page_bytes: u64,
    /// Logical length of `mdbx.dat`.
    pub file_bytes: u64,
    /// Filesystem blocks allocated to `mdbx.dat`.
    pub allocated_bytes: u64,
    /// Configured geometry ceiling.
    pub capacity_bytes: u64,
    /// Entries in `utxo_hot`.
    pub hot_entries: u64,
    /// Entries in `utxo_cold`.
    pub cold_entries: u64,
    /// Entries in `undo`.
    pub undo_entries: u64,
    /// Entries in `meta`.
    pub meta_entries: u64,
}

/// Result of rewriting only live MDBX pages into a fresh environment.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MdbxCompactionReport {
    /// High-water bytes before compaction.
    pub before_bytes: u64,
    /// High-water bytes after compaction and reopen.
    pub after_bytes: u64,
    /// Reachable table pages before the copy.
    pub before_live_page_bytes: u64,
    /// Reachable table pages after the copy.
    pub after_live_page_bytes: u64,
    /// Freelist bytes before the copy.
    pub before_free_page_bytes: u64,
    /// Freelist bytes after the copy.
    pub after_free_page_bytes: u64,
    /// Filesystem allocation of the source environment before the copy.
    pub before_allocated_bytes: u64,
    /// Filesystem allocation of the active compacted environment.
    pub after_allocated_bytes: u64,
    /// Canonical encoded key/value bytes copied without change.
    pub record_bytes: u64,
    /// Verified identity shared by the source and compacted environments.
    pub content_sha256: [u8; 32],
}

/// Durable boundary reached by a compact-copy directory swap.
///
/// Exposed so the crash gate can terminate a child process at every boundary;
/// ordinary callers should use [`MdbxUtxoStore::compact`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MdbxCompactionPhase {
    /// Verified copy and manifest are durable; the source is still active.
    CopySynced,
    /// Source was renamed aside, before syncing the parent directory.
    SourceRenamed,
    /// The first rename is durable, before promoting the verified copy.
    SourceRenameSynced,
    /// Verified copy was promoted, before syncing the parent directory.
    CopyPromoted,
    /// Promotion is durable; the old source remains available for rollback.
    CopyPromotionSynced,
}

impl MdbxCompactionReport {
    /// Bytes of high-water allocation reclaimed by the compact copy.
    #[must_use]
    pub const fn released_bytes(self) -> u64 {
        self.before_bytes.saturating_sub(self.after_bytes)
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct CompactionManifest {
    schema: u32,
    content_sha256: [u8; 32],
    record_bytes: u64,
    hot_entries: u64,
    cold_entries: u64,
    undo_entries: u64,
    meta_entries: u64,
    tip_height: Option<u32>,
    tip_hash: Option<[u8; 32]>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct MaintenanceState {
    schema: u32,
    last_compacted_bytes: u64,
}

impl CompactionManifest {
    fn from_audit(audit: MdbxChainstateAudit) -> Self {
        Self {
            schema: COMPACTION_MANIFEST_SCHEMA,
            content_sha256: audit.content_sha256,
            record_bytes: audit.record_bytes,
            hot_entries: audit.hot_entries,
            cold_entries: audit.cold_entries,
            undo_entries: audit.undo_entries,
            meta_entries: audit.meta_entries,
            tip_height: audit.tip.map(|tip| tip.height),
            tip_hash: audit.tip.map(|tip| tip.hash.to_byte_array()),
        }
    }

    fn matches(&self, audit: MdbxChainstateAudit) -> bool {
        self == &Self::from_audit(audit)
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct EncodedMdbxKey {
    bytes: [u8; 37],
    len: u8,
}

impl EncodedMdbxKey {
    fn as_slice(&self) -> &[u8] {
        &self.bytes[..usize::from(self.len)]
    }

    #[cfg(test)]
    fn len(self) -> usize {
        usize::from(self.len)
    }
}

/// Encodes wire-order txid plus a length-tagged minimal big-endian vout.
///
/// The 34–37-byte format is globally bytewise ordered by numeric vout, unlike
/// btcd's slightly smaller MSB-VLQ at its length boundaries. It also saves two
/// bytes for the overwhelmingly common 0–255 vouts versus a fixed u32.
fn encode_mdbx_key(key: OutPointKey) -> EncodedMdbxKey {
    let outpoint = key.to_outpoint();
    let width = usize::try_from(
        (u32::BITS - outpoint.vout.leading_zeros())
            .div_ceil(8)
            .max(1),
    )
    .expect("vout byte width fits usize");
    let len = 33 + width;
    let mut bytes = [0_u8; 37];
    bytes[..32].copy_from_slice(&outpoint.txid.to_byte_array());
    bytes[32] = u8::try_from(width - 1).expect("vout width tag fits u8");
    bytes[33..len].copy_from_slice(&outpoint.vout.to_be_bytes()[4 - width..]);
    EncodedMdbxKey {
        bytes,
        len: u8::try_from(len).expect("MDBX key length fits u8"),
    }
}

fn decode_mdbx_key(bytes: &[u8]) -> Result<OutPointKey, UtxoError> {
    let Some(tag) = bytes.get(32).copied() else {
        return Err(UtxoError::Malformed("MDBX outpoint key"));
    };
    let width = usize::from(tag) + 1;
    if width > 4 || bytes.len() != 33 + width || (width > 1 && bytes[33] == 0) {
        return Err(UtxoError::Malformed("MDBX outpoint key"));
    }
    let txid = Txid::from_byte_array(bytes[..32].try_into().expect("fixed key length"));
    let mut vout = [0_u8; 4];
    vout[4 - width..].copy_from_slice(&bytes[33..]);
    let vout = u32::from_be_bytes(vout);
    Ok(OutPointKey::from(OutPoint::new(txid, vout)))
}

fn encode_mdbx_block_undo(undos: &[UtxoUndo]) -> Result<Vec<u8>, UtxoError> {
    let count = u32::try_from(undos.len())
        .map_err(|_| UtxoError::Malformed("MDBX undo transaction count"))?;
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&UNDO_FORMAT_VERSION.to_be_bytes());
    bytes.extend_from_slice(&count.to_be_bytes());
    for undo in undos {
        let spent_count = u32::try_from(undo.spent().len())
            .map_err(|_| UtxoError::Malformed("MDBX undo spent count"))?;
        bytes.extend_from_slice(&spent_count.to_be_bytes());
        for (key, coin) in undo.spent() {
            let coin = coin.encode_compact()?;
            let coin_len = u32::try_from(coin.len())
                .map_err(|_| UtxoError::Malformed("MDBX undo coin length"))?;
            bytes.extend_from_slice(encode_mdbx_key(*key).as_slice());
            bytes.extend_from_slice(&coin_len.to_be_bytes());
            bytes.extend_from_slice(&coin);
        }
        let created_count = u32::try_from(undo.created().len())
            .map_err(|_| UtxoError::Malformed("MDBX undo created count"))?;
        bytes.extend_from_slice(&created_count.to_be_bytes());
        for key in undo.created() {
            bytes.extend_from_slice(encode_mdbx_key(*key).as_slice());
        }
    }
    Ok(bytes)
}

fn decode_mdbx_block_undo<K: TransactionKind, E: DatabaseKind>(
    transaction: &Transaction<'_, K, E>,
    meta: &Table<'_>,
    bytes: &[u8],
) -> Result<Vec<UtxoUndo>, UtxoError> {
    let mut cursor = 0;
    if take_mdbx_u32(bytes, &mut cursor, "MDBX undo version")? != UNDO_FORMAT_VERSION {
        return Err(UtxoError::Malformed("unsupported MDBX undo version"));
    }
    let count = usize::try_from(take_mdbx_u32(
        bytes,
        &mut cursor,
        "MDBX undo transaction count",
    )?)
    .expect("u32 fits usize");
    if count > bytes.len().saturating_sub(cursor) / 8 {
        return Err(UtxoError::Malformed(
            "MDBX undo transaction count exceeds record",
        ));
    }
    let mut undos = Vec::with_capacity(count);
    for _ in 0..count {
        let spent_count =
            usize::try_from(take_mdbx_u32(bytes, &mut cursor, "MDBX undo spent count")?)
                .expect("u32 fits usize");
        if spent_count > bytes.len().saturating_sub(cursor) / 40 {
            return Err(UtxoError::Malformed("MDBX undo spent count exceeds record"));
        }
        let mut spent = Vec::with_capacity(spent_count);
        for _ in 0..spent_count {
            let key = decode_mdbx_key(take_mdbx_key(
                bytes,
                &mut cursor,
                "MDBX undo spent outpoint",
            )?)?;
            let coin_len =
                usize::try_from(take_mdbx_u32(bytes, &mut cursor, "MDBX undo coin length")?)
                    .expect("u32 fits usize");
            let coin = MdbxUtxoStore::decode_coin(
                transaction,
                meta,
                take_mdbx(bytes, &mut cursor, coin_len, "MDBX undo coin")?,
            )?;
            spent.push((key, coin));
        }
        let created_count = usize::try_from(take_mdbx_u32(
            bytes,
            &mut cursor,
            "MDBX undo created count",
        )?)
        .expect("u32 fits usize");
        if created_count > bytes.len().saturating_sub(cursor) / 34 {
            return Err(UtxoError::Malformed(
                "MDBX undo created count exceeds record",
            ));
        }
        let mut created = Vec::with_capacity(created_count);
        for _ in 0..created_count {
            created.push(decode_mdbx_key(take_mdbx_key(
                bytes,
                &mut cursor,
                "MDBX undo created outpoint",
            )?)?);
        }
        undos.push(UtxoUndo::from_parts(spent, created));
    }
    if cursor != bytes.len() {
        return Err(UtxoError::Malformed("trailing MDBX undo bytes"));
    }
    Ok(undos)
}

fn take_mdbx<'a>(
    bytes: &'a [u8],
    cursor: &mut usize,
    length: usize,
    field: &'static str,
) -> Result<&'a [u8], UtxoError> {
    let end = cursor
        .checked_add(length)
        .ok_or(UtxoError::Malformed(field))?;
    let value = bytes.get(*cursor..end).ok_or(UtxoError::Malformed(field))?;
    *cursor = end;
    Ok(value)
}

fn take_mdbx_key<'a>(
    bytes: &'a [u8],
    cursor: &mut usize,
    field: &'static str,
) -> Result<&'a [u8], UtxoError> {
    let tag_at = cursor.checked_add(32).ok_or(UtxoError::Malformed(field))?;
    let width = usize::from(*bytes.get(tag_at).ok_or(UtxoError::Malformed(field))?) + 1;
    if width > 4 {
        return Err(UtxoError::Malformed(field));
    }
    take_mdbx(bytes, cursor, 33 + width, field)
}

fn take_mdbx_u32(bytes: &[u8], cursor: &mut usize, field: &'static str) -> Result<u32, UtxoError> {
    Ok(u32::from_be_bytes(
        take_mdbx(bytes, cursor, 4, field)?
            .try_into()
            .expect("fixed length"),
    ))
}

/// Durable MDBX chainstate foundation using exactly four named tables.
///
/// `utxo_hot` and `utxo_cold` hold compact coins, `undo` holds compact
/// per-block disconnect data, and `meta` holds the format, height-indexed
/// creation MTP, and execution tip. Keeping these in one environment lets each
/// block (or 256-block IBD checkpoint) commit UTXOs, undo, and tip atomically.
pub struct MdbxUtxoStore {
    db: Option<Database<NoWriteMap>>,
    database_dir: PathBuf,
    capacity_bytes: u64,
    write_guard: Mutex<()>,
}

impl MdbxUtxoStore {
    /// Opens or creates a durable MDBX environment directory.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, UtxoError> {
        Self::open_with_capacity(path, DEFAULT_CHAINSTATE_CAPACITY_BYTES)
    }

    /// Opens with an engine-enforced maximum allocation high-water mark.
    pub fn open_with_capacity(
        path: impl AsRef<Path>,
        capacity_bytes: u64,
    ) -> Result<Self, UtxoError> {
        let database_dir = path.as_ref().to_path_buf();
        if database_dir.file_name().is_none() {
            return Err(UtxoError::Malformed("MDBX chainstate directory name"));
        }
        recover_compaction_swap(&database_dir)?;
        let db = open_environment(&database_dir, capacity_bytes)?;
        if compaction_manifest_path(&database_dir).exists() {
            validate_compacted_environment(&db)?;
            validate_compaction_manifest(&db, &database_dir, capacity_bytes)?;
        }
        let transaction = db.begin_rw_txn()?;
        let hot = transaction.create_table(Some(HOT), TableFlags::empty())?;
        let cold = transaction.create_table(Some(COLD), TableFlags::empty())?;
        transaction.create_table(Some(UNDO), TableFlags::empty())?;
        let meta = transaction.create_table(Some(META), TableFlags::empty())?;
        match transaction.get::<Vec<u8>>(&meta, FORMAT_KEY)? {
            Some(version) if version.as_slice() == FORMAT_VERSION.to_be_bytes() => {}
            Some(_) => return Err(UtxoError::Malformed("unsupported MDBX chainstate format")),
            None => {
                if transaction.table_stat(&hot)?.entries() != 0
                    || transaction.table_stat(&cold)?.entries() != 0
                {
                    return Err(UtxoError::Malformed(
                        "legacy MDBX UTXO encoding requires rebuild",
                    ));
                }
                transaction.put(
                    &meta,
                    FORMAT_KEY,
                    FORMAT_VERSION.to_be_bytes(),
                    WriteFlags::empty(),
                )?;
            }
        }
        transaction.commit()?;
        remove_compaction_manifest(&database_dir)?;
        remove_stale_compaction_paths(&database_dir)?;
        Ok(Self {
            db: Some(db),
            database_dir,
            capacity_bytes,
            write_guard: Mutex::new(()),
        })
    }

    fn db(&self) -> &Database<NoWriteMap> {
        self.db
            .as_ref()
            .expect("MDBX handle is absent only during an exclusive compact swap")
    }

    fn lock(&self) -> MutexGuard<'_, ()> {
        self.write_guard
            .lock()
            .expect("MDBX write lock not poisoned")
    }

    /// Reports allocation high-water usage against the hard geometry ceiling.
    pub fn capacity(&self) -> Result<MdbxChainstateCapacity, UtxoError> {
        let info = self.db().info()?;
        let page_size = u64::from(self.db().stat()?.page_size());
        let last_page = u64::try_from(info.last_pgno())
            .map_err(|_| UtxoError::Malformed("MDBX page number exceeds u64"))?;
        Ok(MdbxChainstateCapacity {
            used_bytes: last_page.saturating_add(1).saturating_mul(page_size),
            capacity_bytes: self.capacity_bytes,
        })
    }

    /// Scans all four tables and reports physical amplification plus a stable
    /// content identity. This is intentionally more expensive than
    /// [`Self::capacity`] and is intended for replacement gates, maintenance,
    /// and compact-copy verification rather than the per-block hot path.
    pub fn audit(&self) -> Result<MdbxChainstateAudit, UtxoError> {
        audit_environment(self.db(), &self.database_dir, self.capacity_bytes)
    }

    /// Reports page/file amplification and table counts without reading every
    /// record. Suitable for periodic checkpoints in a long churn run.
    pub fn metrics(&self) -> Result<MdbxChainstateMetrics, UtxoError> {
        metrics_environment(self.db(), &self.database_dir, self.capacity_bytes)
    }

    /// Returns whether a compact copy is justified by both geometry pressure
    /// and growth since the previous successful copy.
    ///
    /// `last_compacted_bytes` is the high-water mark reported immediately
    /// after the prior compaction. Supplying it prevents a large irreducible
    /// live set from being recopied after every checkpoint.
    pub fn compaction_is_worthwhile(
        &self,
        trigger_percent: u8,
        recompact_growth_percent: u8,
        last_compacted_bytes: Option<u64>,
    ) -> Result<bool, UtxoError> {
        if trigger_percent == 0 || trigger_percent > 100 || recompact_growth_percent > 100 {
            return Err(UtxoError::Malformed("MDBX compaction policy percentage"));
        }
        let capacity = self.capacity()?;
        if capacity.used_percent() < trigger_percent {
            return Ok(false);
        }
        let last_compacted_bytes = last_compacted_bytes.or(self.last_compacted_bytes()?);
        Ok(last_compacted_bytes.is_none_or(|last| {
            capacity.used_bytes.saturating_sub(last)
                >= last.saturating_mul(u64::from(recompact_growth_percent)) / 100
        }))
    }

    /// Returns the durable post-copy high-water baseline, when one exists.
    pub fn last_compacted_bytes(&self) -> Result<Option<u64>, UtxoError> {
        read_maintenance_state(&self.database_dir)
            .map(|state| state.map(|state| state.last_compacted_bytes))
    }

    /// Reclaims unreachable pages through `MDBX_CP_COMPACT` and a recoverable
    /// directory swap. This is a space operation; it does not promise faster
    /// lookups or mutation of the unchanged live tree.
    pub fn compact(&mut self) -> Result<MdbxCompactionReport, UtxoError> {
        self.compact_with_reserve(DEFAULT_COMPACTION_FREE_SPACE_RESERVE_BYTES)
    }

    /// Runs compact-copy only when the filesystem can hold the estimated live
    /// copy plus `reserve_bytes` without consuming the operator reserve.
    pub fn compact_with_reserve(
        &mut self,
        reserve_bytes: u64,
    ) -> Result<MdbxCompactionReport, UtxoError> {
        self.compact_inner(reserve_bytes, |_| {})
    }

    /// Runs verified compact-copy while reporting each crash-test boundary.
    ///
    /// The hook must not mutate the environment. The repository's ignored
    /// subprocess gate uses it only to terminate the child without unwinding.
    pub fn compact_with_phase_hook(
        &mut self,
        phase_hook: impl FnMut(MdbxCompactionPhase),
    ) -> Result<MdbxCompactionReport, UtxoError> {
        self.compact_inner(0, phase_hook)
    }

    fn compact_inner(
        &mut self,
        reserve_bytes: u64,
        mut phase_hook: impl FnMut(MdbxCompactionPhase),
    ) -> Result<MdbxCompactionReport, UtxoError> {
        let metrics = self.metrics()?;
        let copy_margin = (metrics.live_page_bytes / 10).max(64 * 1024 * 1024);
        let required_free = metrics
            .live_page_bytes
            .saturating_add(copy_margin)
            .saturating_add(reserve_bytes);
        let available = fs2::available_space(
            self.database_dir
                .parent()
                .filter(|parent| !parent.as_os_str().is_empty())
                .unwrap_or_else(|| Path::new(".")),
        )?;
        if available < required_free {
            return Err(std::io::Error::other(
                "insufficient free space for verified MDBX compact copy",
            )
            .into());
        }
        let before = self.audit()?;
        let before_bytes = before.high_water_bytes;
        let fresh_dir = compaction_path(&self.database_dir);
        let old_dir = compacted_out_path(&self.database_dir);
        remove_path_if_exists(&fresh_dir)?;
        remove_path_if_exists(&old_dir)?;
        fs::create_dir_all(&fresh_dir)?;
        self.db().copy_compact(&fresh_dir.join("mdbx.dat"))?;
        let copied = open_environment(&fresh_dir, self.capacity_bytes)?;
        validate_compacted_environment(&copied)?;
        let copied_audit = audit_environment(&copied, &fresh_dir, self.capacity_bytes)?;
        if CompactionManifest::from_audit(before) != CompactionManifest::from_audit(copied_audit) {
            return Err(UtxoError::Malformed("compacted MDBX content identity"));
        }
        drop(copied);
        write_maintenance_state(&fresh_dir, copied_audit.high_water_bytes)?;
        write_compaction_manifest(&fresh_dir, before)?;
        sync_directory(&fresh_dir)?;
        phase_hook(MdbxCompactionPhase::CopySynced);

        drop(self.db.take());
        if let Err(error) = fs::rename(&self.database_dir, &old_dir) {
            self.db = Some(open_environment(&self.database_dir, self.capacity_bytes)?);
            return Err(error.into());
        }
        phase_hook(MdbxCompactionPhase::SourceRenamed);
        if let Err(error) = sync_database_parent(&self.database_dir) {
            fs::rename(&old_dir, &self.database_dir)?;
            sync_database_parent(&self.database_dir)?;
            self.db = Some(open_environment(&self.database_dir, self.capacity_bytes)?);
            return Err(error);
        }
        phase_hook(MdbxCompactionPhase::SourceRenameSynced);
        if let Err(error) = fs::rename(&fresh_dir, &self.database_dir) {
            fs::rename(&old_dir, &self.database_dir)?;
            sync_database_parent(&self.database_dir)?;
            self.db = Some(open_environment(&self.database_dir, self.capacity_bytes)?);
            return Err(error.into());
        }
        phase_hook(MdbxCompactionPhase::CopyPromoted);
        if let Err(error) = sync_database_parent(&self.database_dir) {
            restore_compaction_old(&self.database_dir, &fresh_dir, &old_dir)?;
            self.db = Some(open_environment(&self.database_dir, self.capacity_bytes)?);
            return Err(error);
        }
        phase_hook(MdbxCompactionPhase::CopyPromotionSynced);
        match open_environment(&self.database_dir, self.capacity_bytes).and_then(|db| {
            validate_compacted_environment(&db)?;
            validate_compaction_manifest(&db, &self.database_dir, self.capacity_bytes)?;
            Ok(db)
        }) {
            Ok(db) => self.db = Some(db),
            Err(open_error) => {
                restore_compaction_old(&self.database_dir, &fresh_dir, &old_dir)?;
                self.db = Some(open_environment(&self.database_dir, self.capacity_bytes)?);
                return Err(open_error);
            }
        }
        remove_compaction_manifest(&self.database_dir)?;
        remove_path_if_exists(&old_dir)?;
        sync_database_parent(&self.database_dir)?;
        let after = self.audit()?;
        if CompactionManifest::from_audit(before) != CompactionManifest::from_audit(after) {
            return Err(UtxoError::Malformed(
                "active compacted MDBX content identity",
            ));
        }
        Ok(MdbxCompactionReport {
            before_bytes,
            after_bytes: after.high_water_bytes,
            before_live_page_bytes: before.live_page_bytes,
            after_live_page_bytes: after.live_page_bytes,
            before_free_page_bytes: before.free_page_bytes,
            after_free_page_bytes: after.free_page_bytes,
            before_allocated_bytes: before.allocated_bytes,
            after_allocated_bytes: after.allocated_bytes,
            record_bytes: before.record_bytes,
            content_sha256: before.content_sha256,
        })
    }

    fn decode_coin<K: TransactionKind, E: DatabaseKind>(
        transaction: &Transaction<'_, K, E>,
        meta: &Table<'_>,
        bytes: &[u8],
    ) -> Result<Utxo, UtxoError> {
        let mut coin = Utxo::decode_compact_with_creation_mtp(bytes, 0)?;
        coin.creation_mtp = Self::read_creation_mtp(transaction, meta, coin.height)?;
        Ok(coin)
    }

    fn read_creation_mtp<K: TransactionKind, E: DatabaseKind>(
        transaction: &Transaction<'_, K, E>,
        meta: &Table<'_>,
        height: u32,
    ) -> Result<u32, UtxoError> {
        let key = creation_mtp_key(height);
        let mtp = transaction
            .get::<Vec<u8>>(meta, &key)?
            .ok_or(UtxoError::Malformed(
                "missing creation MTP for compact coin",
            ))?;
        let mtp: [u8; 4] = mtp
            .as_slice()
            .try_into()
            .map_err(|_| UtxoError::Malformed("creation MTP metadata"))?;
        Ok(u32::from_be_bytes(mtp))
    }

    fn decode_coin_cached<K: TransactionKind, E: DatabaseKind>(
        transaction: &Transaction<'_, K, E>,
        meta: &Table<'_>,
        mtp_by_height: &mut BTreeMap<u32, u32>,
        bytes: &[u8],
    ) -> Result<Utxo, UtxoError> {
        let mut coin = Utxo::decode_compact_with_creation_mtp(bytes, 0)?;
        coin.creation_mtp = if let Some(mtp) = mtp_by_height.get(&coin.height) {
            *mtp
        } else {
            let mtp = Self::read_creation_mtp(transaction, meta, coin.height)?;
            mtp_by_height.insert(coin.height, mtp);
            mtp
        };
        Ok(coin)
    }

    fn register_creation_mtp(
        transaction: &Transaction<'_, libmdbx::RW, NoWriteMap>,
        meta: &Table<'_>,
        coin: &Utxo,
    ) -> Result<(), UtxoError> {
        let key = creation_mtp_key(coin.height);
        transaction.put(
            meta,
            key,
            coin.creation_mtp.to_be_bytes(),
            WriteFlags::empty(),
        )?;
        Ok(())
    }

    fn register_undo_creation_mtps(
        transaction: &Transaction<'_, libmdbx::RW, NoWriteMap>,
        meta: &Table<'_>,
        undos: &[UtxoUndo],
    ) -> Result<(), UtxoError> {
        for undo in undos {
            for (_, coin) in undo.spent() {
                Self::register_creation_mtp(transaction, meta, coin)?;
            }
        }
        Ok(())
    }

    fn read_tip<K: TransactionKind, E: DatabaseKind>(
        transaction: &Transaction<'_, K, E>,
        meta: &Table<'_>,
    ) -> Result<Option<ExecutionTip>, UtxoError> {
        transaction
            .get::<Vec<u8>>(meta, TIP_KEY)?
            .map(|encoded| {
                if encoded.len() != 36 {
                    return Err(UtxoError::Malformed("MDBX execution tip"));
                }
                let height = u32::from_be_bytes(
                    encoded[..4]
                        .try_into()
                        .expect("checked execution-tip height"),
                );
                let hash = BlockHash::from_byte_array(
                    encoded[4..].try_into().expect("checked execution-tip hash"),
                );
                Ok(ExecutionTip { height, hash })
            })
            .transpose()
    }

    fn write_tip(
        transaction: &Transaction<'_, libmdbx::RW, NoWriteMap>,
        meta: &Table<'_>,
        tip: ExecutionTip,
    ) -> Result<(), UtxoError> {
        let mut encoded = [0_u8; 36];
        encoded[..4].copy_from_slice(&tip.height.to_be_bytes());
        encoded[4..].copy_from_slice(&tip.hash.to_byte_array());
        transaction.put(meta, TIP_KEY, encoded, WriteFlags::empty())?;
        Ok(())
    }

    /// Initializes an empty MDBX chainstate at a trusted execution tip.
    pub fn initialize_execution_tip(&self, tip: ExecutionTip) -> Result<(), ChainStoreError> {
        let _guard = self.lock();
        let transaction = self.db().begin_rw_txn().map_err(UtxoError::from)?;
        let hot = transaction.open_table(Some(HOT)).map_err(UtxoError::from)?;
        let cold = transaction
            .open_table(Some(COLD))
            .map_err(UtxoError::from)?;
        let undo = transaction
            .open_table(Some(UNDO))
            .map_err(UtxoError::from)?;
        let meta = transaction
            .open_table(Some(META))
            .map_err(UtxoError::from)?;
        if transaction
            .table_stat(&hot)
            .map_err(UtxoError::from)?
            .entries()
            != 0
            || transaction
                .table_stat(&cold)
                .map_err(UtxoError::from)?
                .entries()
                != 0
            || transaction
                .table_stat(&undo)
                .map_err(UtxoError::from)?
                .entries()
                != 0
            || Self::read_tip(&transaction, &meta)?.is_some()
        {
            return Err(UtxoError::Malformed(
                "MDBX execution tip initialization requires empty chainstate",
            )
            .into());
        }
        Self::write_tip(&transaction, &meta, tip)?;
        transaction.commit().map_err(UtxoError::from)?;
        Ok(())
    }

    /// Removes disconnect records below an authenticated active-chain height.
    ///
    /// Every stored undo hash is resolved before the write starts. An unknown
    /// or malformed hash fails closed, so pruning cannot silently discard a
    /// record whose chain position was never established.
    pub fn prune_block_undos_before(
        &self,
        headers: &HeaderDag,
        retain_from_height: u32,
    ) -> Result<u64, ChainStoreError> {
        let transaction = self.db().begin_ro_txn().map_err(UtxoError::from)?;
        let undo = transaction
            .open_table(Some(UNDO))
            .map_err(UtxoError::from)?;
        let mut cursor = transaction.cursor(&undo).map_err(UtxoError::from)?;
        let mut expired = Vec::new();
        for row in cursor.iter_start::<Vec<u8>, Vec<u8>>() {
            let (key, _) = row.map_err(UtxoError::from)?;
            let hash = BlockHash::from_byte_array(
                key.as_slice()
                    .try_into()
                    .map_err(|_| UtxoError::Malformed("MDBX undo block hash"))?,
            );
            let height =
                headers
                    .get(&hash)
                    .map(|header| header.height)
                    .ok_or(UtxoError::Malformed(
                        "MDBX block undo references an unknown header",
                    ))?;
            if height < retain_from_height {
                expired.push((height, hash));
            }
        }
        drop(cursor);
        drop(transaction);
        expired.sort_unstable_by_key(|(height, hash)| (*height, hash.to_byte_array()));
        self.remove_block_undos(
            &expired
                .into_iter()
                .map(|(_, hash)| hash)
                .collect::<Vec<_>>(),
        )
    }

    /// Removes exact block-undo hashes in one durable transaction.
    ///
    /// Production callers should normally use [`Self::prune_block_undos_before`],
    /// which authenticates heights through the header DAG. This lower-level
    /// surface exists for deterministic replay tools that derive every hash
    /// from their own persisted execution tip.
    pub fn remove_block_undos(&self, hashes: &[BlockHash]) -> Result<u64, ChainStoreError> {
        if hashes.is_empty() {
            return Ok(0);
        }
        let mut hashes = hashes.to_vec();
        hashes.sort_unstable_by_key(|hash| hash.to_byte_array());
        hashes.dedup();
        let _guard = self.lock();
        let transaction = self.db().begin_rw_txn().map_err(UtxoError::from)?;
        let undo = transaction
            .open_table(Some(UNDO))
            .map_err(UtxoError::from)?;
        let mut removed = 0_u64;
        for hash in hashes {
            removed += u64::from(
                transaction
                    .del(&undo, hash.to_byte_array(), None)
                    .map_err(UtxoError::from)?,
            );
        }
        transaction.commit().map_err(UtxoError::from)?;
        Ok(removed)
    }

    fn validate_tip_advance(
        current: ExecutionTip,
        expected_parent: BlockHash,
        next: ExecutionTip,
    ) -> Result<(), UtxoError> {
        if current.hash != expected_parent {
            return Err(UtxoError::Malformed("MDBX execution parent mismatch"));
        }
        if current.height.checked_add(1) != Some(next.height) {
            return Err(UtxoError::Malformed(
                "MDBX execution height is not contiguous",
            ));
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn apply_net_changes(
        transaction: &Transaction<'_, libmdbx::RW, NoWriteMap>,
        hot: &Table<'_>,
        cold: &Table<'_>,
        meta: &Table<'_>,
        spent: &[OutPointKey],
        created: &[(OutPointKey, Utxo)],
        tip_height: u32,
        hot_window_blocks: u32,
    ) -> Result<UtxoUndo, UtxoError> {
        let mut seen_spent = BTreeSet::new();
        let mut undo_spent = Vec::with_capacity(spent.len());
        for key in spent {
            if !seen_spent.insert(*key) {
                return Err(UtxoError::DuplicateSpend(*key));
            }
            let storage_key = encode_mdbx_key(*key);
            let value = transaction
                .get::<Vec<u8>>(hot, storage_key.as_slice())?
                .or(transaction.get::<Vec<u8>>(cold, storage_key.as_slice())?)
                .ok_or(UtxoError::Missing(*key))?;
            undo_spent.push((*key, Self::decode_coin(transaction, meta, &value)?));
        }
        let mut seen_created = BTreeSet::new();
        let mut mtp_by_height = BTreeMap::new();
        for (key, _) in created {
            if !seen_created.insert(*key) {
                return Err(UtxoError::Duplicate(*key));
            }
            let storage_key = encode_mdbx_key(*key);
            if !seen_spent.contains(key)
                && (transaction
                    .get::<()>(hot, storage_key.as_slice())?
                    .is_some()
                    || transaction
                        .get::<()>(cold, storage_key.as_slice())?
                        .is_some())
            {
                return Err(UtxoError::Duplicate(*key));
            }
        }
        for (_, coin) in created {
            if mtp_by_height
                .insert(coin.height, coin.creation_mtp)
                .is_some_and(|mtp| mtp != coin.creation_mtp)
            {
                return Err(UtxoError::Malformed(
                    "conflicting creation MTP in one block batch",
                ));
            }
        }
        for key in spent {
            let storage_key = encode_mdbx_key(*key);
            transaction.del(hot, storage_key.as_slice(), None)?;
            transaction.del(cold, storage_key.as_slice(), None)?;
        }
        for (key, coin) in created {
            Self::register_creation_mtp(transaction, meta, coin)?;
            let age = tip_height
                .checked_sub(coin.height)
                .ok_or(UtxoError::Malformed(
                    "UTXO creation height exceeds execution tip",
                ))?;
            let target = if age <= hot_window_blocks { hot } else { cold };
            let storage_key = encode_mdbx_key(*key);
            transaction.put(
                target,
                storage_key.as_slice(),
                coin.encode_compact()?,
                WriteFlags::empty(),
            )?;
        }
        Ok(UtxoUndo::new(
            undo_spent,
            created.iter().map(|(key, _)| *key).collect(),
        ))
    }

    fn fold_batch_changes(
        transitions: &[ConnectTransition],
    ) -> Result<FoldedBatchChanges, UtxoError> {
        let mut spent = BTreeSet::new();
        let mut created = BTreeMap::new();
        for transition in transitions {
            for key in &transition.spent {
                if created.remove(key).is_none() && !spent.insert(*key) {
                    return Err(UtxoError::DuplicateSpend(*key));
                }
            }
            for (key, coin) in &transition.created {
                if created.insert(*key, coin.clone()).is_some() {
                    return Err(UtxoError::Duplicate(*key));
                }
            }
        }
        Ok((spent.into_iter().collect(), created.into_iter().collect()))
    }

    /// Returns one sorted page across both physical tiers without materializing
    /// the complete UTXO set.
    pub fn snapshot_page(
        &self,
        after: Option<OutPointKey>,
        limit: usize,
    ) -> Result<Vec<(OutPointKey, Utxo)>, UtxoError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let _guard = self.lock();
        let transaction = self.db().begin_ro_txn()?;
        let hot = transaction.open_table(Some(HOT))?;
        let cold = transaction.open_table(Some(COLD))?;
        let meta = transaction.open_table(Some(META))?;
        let after_bytes = after.map(encode_mdbx_key);

        let mut hot_cursor = transaction.cursor(&hot)?;
        let mut cold_cursor = transaction.cursor(&cold)?;
        let mut hot_rows = match after_bytes.as_ref() {
            Some(after) => hot_cursor.iter_from::<Vec<u8>, Vec<u8>>(after.as_slice()),
            None => hot_cursor.iter_start::<Vec<u8>, Vec<u8>>(),
        };
        let mut cold_rows = match after_bytes.as_ref() {
            Some(after) => cold_cursor.iter_from::<Vec<u8>, Vec<u8>>(after.as_slice()),
            None => cold_cursor.iter_start::<Vec<u8>, Vec<u8>>(),
        };

        #[allow(clippy::items_after_statements)]
        fn next_row(
            rows: &mut impl Iterator<Item = std::result::Result<(Vec<u8>, Vec<u8>), libmdbx::Error>>,
            after_bytes: Option<&[u8]>,
            transaction: &Transaction<'_, RO, NoWriteMap>,
            meta: &Table<'_>,
        ) -> Result<Option<(OutPointKey, Utxo)>, UtxoError> {
            loop {
                let row = rows.next().transpose()?;
                let Some((key, value)) = row else {
                    return Ok(None);
                };
                if after_bytes.is_some_and(|after| key.as_slice() <= after) {
                    continue;
                }
                let key = decode_mdbx_key(&key)?;
                return Ok(Some((
                    key,
                    MdbxUtxoStore::decode_coin(transaction, meta, &value)?,
                )));
            }
        }

        let after_bytes = after_bytes.as_ref().map(EncodedMdbxKey::as_slice);
        let mut hot_next = next_row(&mut hot_rows, after_bytes, &transaction, &meta)?;
        let mut cold_next = next_row(&mut cold_rows, after_bytes, &transaction, &meta)?;
        let mut page = Vec::with_capacity(limit);
        while page.len() < limit && (hot_next.is_some() || cold_next.is_some()) {
            let take_hot = match (&hot_next, &cold_next) {
                (Some((hot_key, _)), Some((cold_key, _))) => match hot_key.cmp(cold_key) {
                    std::cmp::Ordering::Less => true,
                    std::cmp::Ordering::Greater => false,
                    std::cmp::Ordering::Equal => {
                        return Err(UtxoError::Malformed("outpoint in both tiers"));
                    }
                },
                (Some(_), None) => true,
                (None, Some(_)) => false,
                (None, None) => break,
            };
            let (key, utxo) = if take_hot {
                let row = hot_next.take().expect("selected populated row");
                hot_next = next_row(&mut hot_rows, None, &transaction, &meta)?;
                row
            } else {
                let row = cold_next.take().expect("selected populated row");
                cold_next = next_row(&mut cold_rows, None, &transaction, &meta)?;
                row
            };
            page.push((key, utxo));
        }
        Ok(page)
    }

    /// Reclassifies all compact coins using consensus coin age in blocks.
    ///
    /// This evaluation API intentionally has no wall-clock input. The
    /// production chainstate uses the same predicate at bounded batch commit
    /// boundaries so tier placement is deterministic across machines.
    pub fn retier_by_height(
        &self,
        tip_height: u32,
        hot_window_blocks: u32,
    ) -> Result<u64, UtxoError> {
        let _guard = self.lock();
        let transaction = self.db().begin_rw_txn()?;
        let hot = transaction.open_table(Some(HOT))?;
        let cold = transaction.open_table(Some(COLD))?;
        let mut moves = Vec::new();
        for (name, source_is_hot) in [(HOT, true), (COLD, false)] {
            let source = transaction.open_table(Some(name))?;
            let mut cursor = transaction.cursor(&source)?;
            for row in cursor.iter_start::<Vec<u8>, Vec<u8>>() {
                let (key, value) = row?;
                let coin = Utxo::decode_compact_with_creation_mtp(&value, 0)?;
                let age = tip_height
                    .checked_sub(coin.height)
                    .ok_or(UtxoError::Malformed(
                        "UTXO creation height exceeds re-tier tip",
                    ))?;
                let should_be_hot = age <= hot_window_blocks;
                if source_is_hot != should_be_hot {
                    moves.push((key, value, should_be_hot));
                }
            }
        }
        for (key, value, to_hot) in &moves {
            let (source, target) = if *to_hot {
                (&cold, &hot)
            } else {
                (&hot, &cold)
            };
            transaction.del(source, key, None)?;
            transaction.put(target, key, value, WriteFlags::empty())?;
        }
        transaction.commit()?;
        Ok(u64::try_from(moves.len()).expect("usize fits u64"))
    }
}

fn creation_mtp_key(height: u32) -> [u8; 5] {
    let mut key = [0_u8; 5];
    key[0] = b'm';
    key[1..].copy_from_slice(&height.to_be_bytes());
    key
}

fn open_environment(
    database_dir: &Path,
    capacity_bytes: u64,
) -> Result<Database<NoWriteMap>, UtxoError> {
    fs::create_dir_all(database_dir)?;
    let capacity = isize::try_from(capacity_bytes)
        .map_err(|_| UtxoError::Malformed("MDBX capacity exceeds platform limit"))?;
    Ok(Database::open_with_options(
        database_dir,
        DatabaseOptions {
            max_tables: Some(4),
            mode: Mode::ReadWrite(ReadWriteOptions {
                sync_mode: SyncMode::Durable,
                max_size: Some(capacity),
                ..ReadWriteOptions::default()
            }),
            ..DatabaseOptions::default()
        },
    )?)
}

fn metrics_environment(
    db: &Database<NoWriteMap>,
    database_dir: &Path,
    capacity_bytes: u64,
) -> Result<MdbxChainstateMetrics, UtxoError> {
    let info = db.info()?;
    let page_size = u64::from(db.stat()?.page_size());
    let high_water_pages = u64::try_from(info.last_pgno())
        .map_err(|_| UtxoError::Malformed("MDBX page number exceeds u64"))?
        .saturating_add(1);
    let free_pages = u64::try_from(db.freelist()?)
        .map_err(|_| UtxoError::Malformed("MDBX freelist exceeds u64"))?;
    let transaction = db.begin_ro_txn()?;
    let mut live_page_bytes = 0_u64;
    let mut counts = [0_u64; 4];
    for (index, name) in [HOT, COLD, UNDO, META].into_iter().enumerate() {
        let table = transaction.open_table(Some(name))?;
        let stats = transaction.table_stat(&table)?;
        live_page_bytes = live_page_bytes.saturating_add(stats.total_size());
        counts[index] = u64::try_from(stats.entries())
            .map_err(|_| UtxoError::Malformed("MDBX table entries exceed u64"))?;
    }
    let (file_bytes, allocated_bytes) = database_file_sizes(database_dir)?;
    Ok(MdbxChainstateMetrics {
        high_water_bytes: high_water_pages.saturating_mul(page_size),
        live_page_bytes,
        free_page_bytes: free_pages.saturating_mul(page_size),
        file_bytes,
        allocated_bytes,
        capacity_bytes,
        hot_entries: counts[0],
        cold_entries: counts[1],
        undo_entries: counts[2],
        meta_entries: counts[3],
    })
}

fn audit_environment(
    db: &Database<NoWriteMap>,
    database_dir: &Path,
    capacity_bytes: u64,
) -> Result<MdbxChainstateAudit, UtxoError> {
    let metrics = metrics_environment(db, database_dir, capacity_bytes)?;
    let transaction = db.begin_ro_txn()?;
    let mut hasher = Sha256::new();
    hasher.update(b"rbtc-mdbx-four-table-audit-v1");
    let mut record_bytes = 0_u64;
    for name in [HOT, COLD, UNDO, META] {
        let table = transaction.open_table(Some(name))?;
        hasher.update(
            u64::try_from(name.len())
                .expect("table name fits u64")
                .to_be_bytes(),
        );
        hasher.update(name.as_bytes());
        let mut cursor = transaction.cursor(&table)?;
        for row in cursor.iter_start::<Vec<u8>, Vec<u8>>() {
            let (key, value) = row?;
            let key_len = u64::try_from(key.len()).expect("key length fits u64");
            let value_len = u64::try_from(value.len()).expect("value length fits u64");
            record_bytes = record_bytes
                .saturating_add(key_len)
                .saturating_add(value_len);
            hasher.update(key_len.to_be_bytes());
            hasher.update(&key);
            hasher.update(value_len.to_be_bytes());
            hasher.update(&value);
        }
    }
    let meta = transaction.open_table(Some(META))?;
    let tip = MdbxUtxoStore::read_tip(&transaction, &meta)?;
    Ok(MdbxChainstateAudit {
        high_water_bytes: metrics.high_water_bytes,
        live_page_bytes: metrics.live_page_bytes,
        free_page_bytes: metrics.free_page_bytes,
        record_bytes,
        file_bytes: metrics.file_bytes,
        allocated_bytes: metrics.allocated_bytes,
        capacity_bytes,
        hot_entries: metrics.hot_entries,
        cold_entries: metrics.cold_entries,
        undo_entries: metrics.undo_entries,
        meta_entries: metrics.meta_entries,
        tip,
        content_sha256: hasher.finalize().into(),
    })
}

fn database_file_sizes(database_dir: &Path) -> Result<(u64, u64), UtxoError> {
    let metadata = fs::metadata(database_dir.join("mdbx.dat"))?;
    let logical = metadata.len();
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        Ok((logical, metadata.blocks().saturating_mul(512)))
    }
    #[cfg(not(unix))]
    {
        Ok((logical, logical))
    }
}

fn compaction_manifest_path(database_dir: &Path) -> PathBuf {
    database_dir.join(COMPACTION_MANIFEST_FILE)
}

fn maintenance_state_path(database_dir: &Path) -> PathBuf {
    database_dir.join(MAINTENANCE_STATE_FILE)
}

fn write_maintenance_state(
    database_dir: &Path,
    last_compacted_bytes: u64,
) -> Result<(), UtxoError> {
    let state = MaintenanceState {
        schema: MAINTENANCE_STATE_SCHEMA,
        last_compacted_bytes,
    };
    let encoded =
        serde_json::to_vec(&state).map_err(|_| UtxoError::Malformed("MDBX maintenance state"))?;
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(maintenance_state_path(database_dir))?;
    file.write_all(&encoded)?;
    file.sync_all()?;
    Ok(())
}

fn read_maintenance_state(database_dir: &Path) -> Result<Option<MaintenanceState>, UtxoError> {
    let encoded = match fs::read(maintenance_state_path(database_dir)) {
        Ok(encoded) => encoded,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    if encoded.len() > 1024 {
        return Err(UtxoError::Malformed("MDBX maintenance state"));
    }
    let state: MaintenanceState = serde_json::from_slice(&encoded)
        .map_err(|_| UtxoError::Malformed("MDBX maintenance state"))?;
    if state.schema != MAINTENANCE_STATE_SCHEMA || state.last_compacted_bytes == 0 {
        return Err(UtxoError::Malformed("MDBX maintenance state"));
    }
    Ok(Some(state))
}

fn write_compaction_manifest(
    database_dir: &Path,
    audit: MdbxChainstateAudit,
) -> Result<(), UtxoError> {
    let encoded = serde_json::to_vec(&CompactionManifest::from_audit(audit))
        .map_err(|_| UtxoError::Malformed("MDBX compaction manifest"))?;
    let path = compaction_manifest_path(database_dir);
    let mut file = OpenOptions::new().write(true).create_new(true).open(path)?;
    file.write_all(&encoded)?;
    file.sync_all()?;
    Ok(())
}

fn read_compaction_manifest(database_dir: &Path) -> Result<CompactionManifest, UtxoError> {
    let encoded = fs::read(compaction_manifest_path(database_dir))?;
    if encoded.len() > 4 * 1024 {
        return Err(UtxoError::Malformed("MDBX compaction manifest"));
    }
    let manifest: CompactionManifest = serde_json::from_slice(&encoded)
        .map_err(|_| UtxoError::Malformed("MDBX compaction manifest"))?;
    if manifest.schema != COMPACTION_MANIFEST_SCHEMA {
        return Err(UtxoError::Malformed("MDBX compaction manifest schema"));
    }
    Ok(manifest)
}

fn validate_compaction_manifest(
    db: &Database<NoWriteMap>,
    database_dir: &Path,
    capacity_bytes: u64,
) -> Result<(), UtxoError> {
    let manifest = read_compaction_manifest(database_dir)?;
    let audit = audit_environment(db, database_dir, capacity_bytes)?;
    if !manifest.matches(audit) {
        return Err(UtxoError::Malformed("MDBX compaction manifest identity"));
    }
    Ok(())
}

fn remove_compaction_manifest(database_dir: &Path) -> Result<(), UtxoError> {
    let path = compaction_manifest_path(database_dir);
    match fs::remove_file(path) {
        Ok(()) => {
            sync_directory(database_dir)?;
            Ok(())
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn validate_compacted_environment(db: &Database<NoWriteMap>) -> Result<(), UtxoError> {
    let transaction = db.begin_ro_txn()?;
    transaction.open_table(Some(HOT))?;
    transaction.open_table(Some(COLD))?;
    transaction.open_table(Some(UNDO))?;
    let meta = transaction.open_table(Some(META))?;
    match transaction.get::<Vec<u8>>(&meta, FORMAT_KEY)? {
        Some(version) if version.as_slice() == FORMAT_VERSION.to_be_bytes() => Ok(()),
        _ => Err(UtxoError::Malformed("compacted MDBX format marker")),
    }
}

fn compaction_path(database_dir: &Path) -> PathBuf {
    database_dir.with_file_name({
        let mut name = database_dir.file_name().unwrap_or_default().to_owned();
        name.push(".compacting");
        name
    })
}

fn compacted_out_path(database_dir: &Path) -> PathBuf {
    database_dir.with_file_name({
        let mut name = database_dir.file_name().unwrap_or_default().to_owned();
        name.push(".compacted-out");
        name
    })
}

fn remove_path_if_exists(path: &Path) -> Result<(), UtxoError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => {
            fs::remove_dir_all(path)?;
            Ok(())
        }
        Ok(_) => {
            fs::remove_file(path)?;
            Ok(())
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn recover_compaction_swap(database_dir: &Path) -> Result<(), UtxoError> {
    let old_dir = compacted_out_path(database_dir);
    let fresh_dir = compaction_path(database_dir);
    if !database_dir.exists() {
        if old_dir.exists() {
            fs::rename(&old_dir, database_dir)?;
            remove_path_if_exists(&fresh_dir)?;
            sync_database_parent(database_dir)?;
        } else if fresh_dir.exists() {
            if !compaction_manifest_path(&fresh_dir).is_file() {
                return Err(UtxoError::Malformed("unverified orphan MDBX compact copy"));
            }
            fs::rename(&fresh_dir, database_dir)?;
            sync_database_parent(database_dir)?;
        }
    }
    Ok(())
}

fn remove_stale_compaction_paths(database_dir: &Path) -> Result<(), UtxoError> {
    remove_path_if_exists(&compaction_path(database_dir))?;
    remove_path_if_exists(&compacted_out_path(database_dir))?;
    sync_database_parent(database_dir)
}

fn restore_compaction_old(
    database_dir: &Path,
    fresh_dir: &Path,
    old_dir: &Path,
) -> Result<(), UtxoError> {
    if database_dir.exists() {
        fs::rename(database_dir, fresh_dir)?;
    }
    fs::rename(old_dir, database_dir)?;
    sync_database_parent(database_dir)
}

fn sync_database_parent(database_dir: &Path) -> Result<(), UtxoError> {
    let parent = database_dir
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    sync_directory(parent)
}

#[cfg_attr(not(unix), allow(clippy::unnecessary_wraps))]
fn sync_directory(path: &Path) -> Result<(), UtxoError> {
    #[cfg(unix)]
    {
        fs::File::open(path)?.sync_all()?;
    }
    #[cfg(not(unix))]
    {
        let _ = path;
    }
    Ok(())
}

impl UtxoStore for MdbxUtxoStore {
    fn get(&self, outpoint: OutPointKey) -> Result<Option<Utxo>, UtxoError> {
        let transaction = self.db().begin_ro_txn()?;
        let hot = transaction.open_table(Some(HOT))?;
        let meta = transaction.open_table(Some(META))?;
        let storage_key = encode_mdbx_key(outpoint);
        if let Some(value) = transaction.get::<Vec<u8>>(&hot, storage_key.as_slice())? {
            return Self::decode_coin(&transaction, &meta, &value).map(Some);
        }
        let cold = transaction.open_table(Some(COLD))?;
        transaction
            .get::<Vec<u8>>(&cold, storage_key.as_slice())?
            .map(|value| Self::decode_coin(&transaction, &meta, &value))
            .transpose()
    }

    fn get_many(
        &self,
        outpoints: &[OutPointKey],
    ) -> Result<Vec<(OutPointKey, Option<Utxo>)>, UtxoError> {
        let transaction = self.db().begin_ro_txn()?;
        let hot = transaction.open_table(Some(HOT))?;
        let cold = transaction.open_table(Some(COLD))?;
        let meta = transaction.open_table(Some(META))?;
        let mut mtp_by_height = BTreeMap::new();
        outpoints
            .iter()
            .map(|outpoint| {
                let storage_key = encode_mdbx_key(*outpoint);
                let coin = transaction
                    .get::<Vec<u8>>(&hot, storage_key.as_slice())?
                    .or(transaction.get::<Vec<u8>>(&cold, storage_key.as_slice())?)
                    .map(|value| {
                        Self::decode_coin_cached(&transaction, &meta, &mut mtp_by_height, &value)
                    })
                    .transpose()?;
                Ok((*outpoint, coin))
            })
            .collect()
    }

    fn apply(
        &self,
        spent: &[OutPointKey],
        created: &[(OutPointKey, Utxo)],
    ) -> Result<(), UtxoError> {
        self.apply_with_undo(spent, created).map(|_| ())
    }

    fn apply_with_undo(
        &self,
        spent: &[OutPointKey],
        created: &[(OutPointKey, Utxo)],
    ) -> Result<UtxoUndo, UtxoError> {
        let _guard = self.lock();
        let transaction = self.db().begin_rw_txn()?;
        let hot = transaction.open_table(Some(HOT))?;
        let cold = transaction.open_table(Some(COLD))?;
        let meta = transaction.open_table(Some(META))?;
        let mut seen_spent = BTreeSet::new();
        let mut undo_spent = Vec::with_capacity(spent.len());
        for key in spent {
            if !seen_spent.insert(*key) {
                return Err(UtxoError::DuplicateSpend(*key));
            }
            let storage_key = encode_mdbx_key(*key);
            let value = transaction
                .get::<Vec<u8>>(&hot, storage_key.as_slice())?
                .or(transaction.get::<Vec<u8>>(&cold, storage_key.as_slice())?)
                .ok_or(UtxoError::Missing(*key))?;
            undo_spent.push((*key, Self::decode_coin(&transaction, &meta, &value)?));
        }
        let mut seen_created = BTreeSet::new();
        for (key, _) in created {
            if !seen_created.insert(*key) {
                return Err(UtxoError::Duplicate(*key));
            }
            let storage_key = encode_mdbx_key(*key);
            if !seen_spent.contains(key)
                && (transaction
                    .get::<()>(&hot, storage_key.as_slice())?
                    .is_some()
                    || transaction
                        .get::<()>(&cold, storage_key.as_slice())?
                        .is_some())
            {
                return Err(UtxoError::Duplicate(*key));
            }
        }
        for key in spent {
            let storage_key = encode_mdbx_key(*key);
            transaction.del(&hot, storage_key.as_slice(), None)?;
            transaction.del(&cold, storage_key.as_slice(), None)?;
        }
        for (key, utxo) in created {
            Self::register_creation_mtp(&transaction, &meta, utxo)?;
            let storage_key = encode_mdbx_key(*key);
            transaction.put(
                &hot,
                storage_key.as_slice(),
                utxo.encode_compact()?,
                WriteFlags::empty(),
            )?;
        }
        transaction.commit()?;
        Ok(UtxoUndo::new(
            undo_spent,
            created.iter().map(|(key, _)| *key).collect(),
        ))
    }

    fn undo(&self, undo: &UtxoUndo, _now: u64, _hot_window_secs: u64) -> Result<(), UtxoError> {
        let _guard = self.lock();
        let transaction = self.db().begin_rw_txn()?;
        let hot = transaction.open_table(Some(HOT))?;
        let cold = transaction.open_table(Some(COLD))?;
        let meta = transaction.open_table(Some(META))?;
        let recreated = undo.created().iter().copied().collect::<BTreeSet<_>>();
        for (key, _) in undo.spent() {
            let storage_key = encode_mdbx_key(*key);
            if !recreated.contains(key)
                && (transaction
                    .get::<()>(&hot, storage_key.as_slice())?
                    .is_some()
                    || transaction
                        .get::<()>(&cold, storage_key.as_slice())?
                        .is_some())
            {
                return Err(UtxoError::Duplicate(*key));
            }
        }
        for key in undo.created() {
            let storage_key = encode_mdbx_key(*key);
            transaction.del(&hot, storage_key.as_slice(), None)?;
            transaction.del(&cold, storage_key.as_slice(), None)?;
        }
        for (key, utxo) in undo.spent() {
            Self::register_creation_mtp(&transaction, &meta, utxo)?;
            // A restored coin is hot until the height-based re-tier pass places
            // it. Wall-clock timestamps are deliberately not persisted.
            let storage_key = encode_mdbx_key(*key);
            transaction.put(
                &hot,
                storage_key.as_slice(),
                utxo.encode_compact()?,
                WriteFlags::empty(),
            )?;
        }
        transaction.commit()?;
        Ok(())
    }

    fn age_to_cold(&self, _now: u64, _hot_window_secs: u64) -> Result<u64, UtxoError> {
        Err(UtxoError::Malformed(
            "MDBX wall-clock tiering is unsupported; use block-height re-tiering",
        ))
    }

    fn snapshot_entries(&self) -> Result<BTreeMap<OutPointKey, Utxo>, UtxoError> {
        let transaction = self.db().begin_ro_txn()?;
        let meta = transaction.open_table(Some(META))?;
        let mut entries = BTreeMap::new();
        for name in [HOT, COLD] {
            let table = transaction.open_table(Some(name))?;
            let mut cursor = transaction.cursor(&table)?;
            for row in cursor.iter_start::<Vec<u8>, Vec<u8>>() {
                let (key, value) = row?;
                let key = decode_mdbx_key(&key)?;
                if entries
                    .insert(key, Self::decode_coin(&transaction, &meta, &value)?)
                    .is_some()
                {
                    return Err(UtxoError::Malformed("outpoint in both MDBX tiers"));
                }
            }
        }
        Ok(entries)
    }

    fn replace_all(
        &self,
        entries: &BTreeMap<OutPointKey, Utxo>,
        _now: u64,
        _hot_window_secs: u64,
    ) -> Result<(), UtxoError> {
        let _guard = self.lock();
        let transaction = self.db().begin_rw_txn()?;
        let hot = transaction.open_table(Some(HOT))?;
        let cold = transaction.open_table(Some(COLD))?;
        let meta = transaction.open_table(Some(META))?;
        transaction.clear_table(&hot)?;
        transaction.clear_table(&cold)?;
        for (key, utxo) in entries {
            Self::register_creation_mtp(&transaction, &meta, utxo)?;
            let storage_key = encode_mdbx_key(*key);
            transaction.put(
                &hot,
                storage_key.as_slice(),
                utxo.encode_compact()?,
                WriteFlags::empty(),
            )?;
        }
        transaction.commit()?;
        Ok(())
    }

    fn tier_stats(&self) -> Result<TierStats, UtxoError> {
        let transaction = self.db().begin_ro_txn()?;
        let hot = transaction.open_table(Some(HOT))?;
        let cold = transaction.open_table(Some(COLD))?;
        Ok(TierStats {
            hot: u64::try_from(transaction.table_stat(&hot)?.entries())
                .expect("MDBX entry count fits u64"),
            cold: u64::try_from(transaction.table_stat(&cold)?.entries())
                .expect("MDBX entry count fits u64"),
        })
    }
}

impl ExecutionChainStore for MdbxUtxoStore {
    fn execution_tip(&self) -> Result<ExecutionTip, ChainStoreError> {
        let transaction = self.db().begin_ro_txn().map_err(UtxoError::from)?;
        let meta = transaction
            .open_table(Some(META))
            .map_err(UtxoError::from)?;
        Self::read_tip(&transaction, &meta)?
            .ok_or_else(|| UtxoError::Malformed("MDBX execution tip is uninitialized").into())
    }

    fn assumed_snapshot_base(&self) -> Result<Option<ExecutionTip>, ChainStoreError> {
        Ok(None)
    }

    fn block_undo(&self, hash: BlockHash) -> Result<Option<Vec<UtxoUndo>>, ChainStoreError> {
        let transaction = self.db().begin_ro_txn().map_err(UtxoError::from)?;
        let undo = transaction
            .open_table(Some(UNDO))
            .map_err(UtxoError::from)?;
        let meta = transaction
            .open_table(Some(META))
            .map_err(UtxoError::from)?;
        transaction
            .get::<Vec<u8>>(&undo, &hash.to_byte_array())
            .map_err(UtxoError::from)?
            .map(|encoded| decode_mdbx_block_undo(&transaction, &meta, &encoded))
            .transpose()
            .map_err(Into::into)
    }

    fn retains_block_undo(&self) -> bool {
        true
    }

    fn prune_block_undos_before(
        &self,
        headers: &HeaderDag,
        retain_from_height: u32,
    ) -> Result<u64, ChainStoreError> {
        MdbxUtxoStore::prune_block_undos_before(self, headers, retain_from_height)
    }

    fn commit_connect(
        &self,
        expected_parent: BlockHash,
        next: ExecutionTip,
        spent: &[OutPointKey],
        created: &[(OutPointKey, Utxo)],
        transaction_undos: &[UtxoUndo],
    ) -> Result<UtxoUndo, ChainStoreError> {
        let _guard = self.lock();
        let transaction = self.db().begin_rw_txn().map_err(UtxoError::from)?;
        let hot = transaction.open_table(Some(HOT)).map_err(UtxoError::from)?;
        let cold = transaction
            .open_table(Some(COLD))
            .map_err(UtxoError::from)?;
        let undo = transaction
            .open_table(Some(UNDO))
            .map_err(UtxoError::from)?;
        let meta = transaction
            .open_table(Some(META))
            .map_err(UtxoError::from)?;
        let current = Self::read_tip(&transaction, &meta)?
            .ok_or(UtxoError::Malformed("MDBX execution tip is uninitialized"))?;
        Self::validate_tip_advance(current, expected_parent, next)?;
        let aggregate = Self::apply_net_changes(
            &transaction,
            &hot,
            &cold,
            &meta,
            spent,
            created,
            next.height,
            DEFAULT_HOT_WINDOW_BLOCKS,
        )?;
        let hash = next.hash.to_byte_array();
        if transaction
            .get::<()>(&undo, &hash)
            .map_err(UtxoError::from)?
            .is_some()
        {
            return Err(UtxoError::Malformed("duplicate MDBX block undo").into());
        }
        Self::register_undo_creation_mtps(&transaction, &meta, transaction_undos)?;
        let encoded = encode_mdbx_block_undo(transaction_undos)?;
        transaction
            .put(&undo, hash, encoded, WriteFlags::empty())
            .map_err(UtxoError::from)?;
        Self::write_tip(&transaction, &meta, next)?;
        transaction.commit().map_err(UtxoError::from)?;
        Ok(aggregate)
    }

    fn commit_connect_batch(
        &self,
        transitions: &[ConnectTransition],
    ) -> Result<(), ChainStoreError> {
        if transitions.is_empty() {
            return Ok(());
        }
        if transitions.len() > MAX_ATOMIC_IBD_BATCH_BLOCKS {
            return Err(UtxoError::Malformed("MDBX IBD batch exceeds 256 blocks").into());
        }
        let (spent, created) = Self::fold_batch_changes(transitions)?;
        let _guard = self.lock();
        let transaction = self.db().begin_rw_txn().map_err(UtxoError::from)?;
        let hot = transaction.open_table(Some(HOT)).map_err(UtxoError::from)?;
        let cold = transaction
            .open_table(Some(COLD))
            .map_err(UtxoError::from)?;
        let undo = transaction
            .open_table(Some(UNDO))
            .map_err(UtxoError::from)?;
        let meta = transaction
            .open_table(Some(META))
            .map_err(UtxoError::from)?;
        let mut current = Self::read_tip(&transaction, &meta)?
            .ok_or(UtxoError::Malformed("MDBX execution tip is uninitialized"))?;
        for transition in transitions {
            Self::validate_tip_advance(current, transition.expected_parent, transition.next)?;
            current = transition.next;
        }
        Self::apply_net_changes(
            &transaction,
            &hot,
            &cold,
            &meta,
            &spent,
            &created,
            current.height,
            DEFAULT_HOT_WINDOW_BLOCKS,
        )?;
        for transition in transitions {
            let hash = transition.next.hash.to_byte_array();
            if transaction
                .get::<()>(&undo, &hash)
                .map_err(UtxoError::from)?
                .is_some()
            {
                return Err(UtxoError::Malformed("duplicate MDBX block undo").into());
            }
            Self::register_undo_creation_mtps(&transaction, &meta, &transition.transaction_undos)?;
            let encoded = encode_mdbx_block_undo(&transition.transaction_undos)?;
            transaction
                .put(&undo, hash, encoded, WriteFlags::empty())
                .map_err(UtxoError::from)?;
        }
        Self::write_tip(&transaction, &meta, current)?;
        transaction.commit().map_err(UtxoError::from)?;
        Ok(())
    }

    fn commit_disconnect(
        &self,
        expected_current: ExecutionTip,
        parent: ExecutionTip,
        spent: &[OutPointKey],
        created: &[(OutPointKey, Utxo)],
        _transaction_undos: &[UtxoUndo],
    ) -> Result<UtxoUndo, ChainStoreError> {
        if parent.height.checked_add(1) != Some(expected_current.height) {
            return Err(UtxoError::Malformed("MDBX disconnect height is not contiguous").into());
        }
        let _guard = self.lock();
        let transaction = self.db().begin_rw_txn().map_err(UtxoError::from)?;
        let hot = transaction.open_table(Some(HOT)).map_err(UtxoError::from)?;
        let cold = transaction
            .open_table(Some(COLD))
            .map_err(UtxoError::from)?;
        let undo = transaction
            .open_table(Some(UNDO))
            .map_err(UtxoError::from)?;
        let meta = transaction
            .open_table(Some(META))
            .map_err(UtxoError::from)?;
        if Self::read_tip(&transaction, &meta)? != Some(expected_current) {
            return Err(UtxoError::Malformed("MDBX disconnect tip mismatch").into());
        }
        let aggregate = Self::apply_net_changes(
            &transaction,
            &hot,
            &cold,
            &meta,
            spent,
            created,
            parent.height,
            DEFAULT_HOT_WINDOW_BLOCKS,
        )?;
        let hash = expected_current.hash.to_byte_array();
        if !transaction
            .del(&undo, hash, None)
            .map_err(UtxoError::from)?
        {
            return Err(UtxoError::Malformed("missing MDBX block undo").into());
        }
        Self::write_tip(&transaction, &meta, parent)?;
        transaction.commit().map_err(UtxoError::from)?;
        Ok(aggregate)
    }
}

#[cfg(test)]
mod tests {
    use bitcoin::{BlockHash, OutPoint, Txid, hashes::Hash, hex::FromHex};
    use tempfile::TempDir;

    use super::*;

    fn key(byte: u8) -> OutPointKey {
        OutPoint::new(Txid::from_byte_array([byte; 32]), 0).into()
    }

    fn coin(height: u32) -> Utxo {
        Utxo {
            value_sats: 42,
            height,
            is_coinbase: false,
            last_touched: 0,
            creation_mtp: height.saturating_mul(10),
            script_pubkey: vec![0x51],
        }
    }

    fn block_hash(byte: u8) -> BlockHash {
        BlockHash::from_byte_array([byte; 32])
    }

    fn block_hash_at(height: u32) -> BlockHash {
        let mut bytes = [0_u8; 32];
        bytes[..4].copy_from_slice(&height.to_be_bytes());
        BlockHash::from_byte_array(bytes)
    }

    fn btcd_p2pkh_coin() -> Utxo {
        let mut script = vec![0x76, 0xa9, 0x14];
        script.extend_from_slice(
            &Vec::<u8>::from_hex("b8025be1b3efc63b0ad48e7f9f10e87544528d58").unwrap(),
        );
        script.extend_from_slice(&[0x88, 0xac]);
        Utxo {
            value_sats: 15_000_000,
            height: 113_931,
            is_coinbase: false,
            last_touched: 999,
            creation_mtp: 123_456,
            script_pubkey: script,
        }
    }

    #[test]
    fn ordered_variable_keys_roundtrip_numeric_vout_boundaries() {
        let txid = Txid::from_byte_array([0x42; 32]);
        let vouts = [0, 1, 127, 128, 255, 256, 16_384, 65_535, 65_536, u32::MAX];
        let encoded = vouts
            .into_iter()
            .map(|vout| encode_mdbx_key(OutPointKey::from(OutPoint::new(txid, vout))))
            .collect::<Vec<_>>();
        assert!(encoded.windows(2).all(|pair| pair[0] < pair[1]));
        assert!(encoded.iter().all(|key| (34..=37).contains(&key.len())));
        assert_eq!(
            encoded
                .iter()
                .map(|key| decode_mdbx_key(key.as_slice()).unwrap().to_outpoint().vout)
                .collect::<Vec<_>>(),
            vouts
        );

        let mut noncanonical = encode_mdbx_key(OutPointKey::from(OutPoint::new(txid, 255)))
            .as_slice()
            .to_vec();
        noncanonical[32] = 1;
        noncanonical.insert(33, 0);
        assert!(matches!(
            decode_mdbx_key(&noncanonical),
            Err(UtxoError::Malformed("MDBX outpoint key"))
        ));
    }

    #[test]
    fn physical_schema_uses_four_tables_ordered_keys_and_btcd_coin_values() {
        let directory = TempDir::new().unwrap();
        let store = MdbxUtxoStore::open(directory.path().join("mdbx")).unwrap();
        let txid = Txid::from_byte_array([7; 32]);
        let coin = btcd_p2pkh_coin();
        let keys = [256_u32, 1, 2].map(|vout| OutPointKey::from(OutPoint::new(txid, vout)));
        store
            .apply(&[], &keys.map(|key| (key, coin.clone())))
            .unwrap();

        let transaction = store.db().begin_ro_txn().unwrap();
        let hot = transaction.open_table(Some(HOT)).unwrap();
        transaction.open_table(Some(COLD)).unwrap();
        transaction.open_table(Some(UNDO)).unwrap();
        transaction.open_table(Some(META)).unwrap();
        let mut cursor = transaction.cursor(&hot).unwrap();
        let rows = cursor
            .iter_start::<Vec<u8>, Vec<u8>>()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(
            rows.iter()
                .map(|(key, _)| decode_mdbx_key(key).unwrap().to_outpoint().vout)
                .collect::<Vec<_>>(),
            vec![1, 2, 256]
        );
        assert_eq!(
            rows.iter().map(|(key, _)| key.len()).collect::<Vec<_>>(),
            vec![34, 34, 35]
        );
        assert!(rows.iter().all(|(key, value)| {
            key[..32] == txid.to_byte_array()
                && value
                    == &Vec::<u8>::from_hex("8cf316800900b8025be1b3efc63b0ad48e7f9f10e87544528d58")
                        .unwrap()
        }));
        drop(cursor);
        drop(transaction);

        assert_eq!(
            store
                .snapshot_page(None, 10)
                .unwrap()
                .into_iter()
                .map(|(key, _)| key.to_outpoint().vout)
                .collect::<Vec<_>>(),
            vec![1, 2, 256]
        );
    }

    #[test]
    fn durable_backend_roundtrips_atomic_updates_and_tiers() {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("mdbx");
        let store = MdbxUtxoStore::open(&path).unwrap();
        store
            .apply(&[], &[(key(1), coin(1)), (key(2), coin(100))])
            .unwrap();
        let undo = store
            .apply_with_undo(&[key(2)], &[(key(3), coin(101))])
            .unwrap();
        assert_eq!(store.retier_by_height(101, 60).unwrap(), 1);
        assert_eq!(store.tier_stats().unwrap(), TierStats { hot: 1, cold: 1 });
        store.undo(&undo, 100, 60).unwrap();
        drop(store);

        let reopened = MdbxUtxoStore::open(path).unwrap();
        assert_eq!(reopened.get(key(1)).unwrap(), Some(coin(1)));
        assert_eq!(reopened.get(key(2)).unwrap(), Some(coin(100)));
        assert!(reopened.get(key(3)).unwrap().is_none());
    }

    #[test]
    fn batch_lookup_reuses_one_view_and_preserves_duplicates_and_misses() {
        let directory = TempDir::new().unwrap();
        let store = MdbxUtxoStore::open(directory.path().join("mdbx")).unwrap();
        store
            .apply(&[], &[(key(1), coin(1)), (key(2), coin(2))])
            .unwrap();
        store.retier_by_height(2, 0).unwrap();

        assert_eq!(
            store.get_many(&[key(2), key(9), key(1), key(2)]).unwrap(),
            vec![
                (key(2), Some(coin(2))),
                (key(9), None),
                (key(1), Some(coin(1))),
                (key(2), Some(coin(2))),
            ]
        );
    }

    #[test]
    fn atomic_batch_folds_ephemeral_outputs_and_commits_undo_with_tip() {
        let directory = TempDir::new().unwrap();
        let store = MdbxUtxoStore::open(directory.path().join("mdbx")).unwrap();
        let genesis = ExecutionTip {
            height: 0,
            hash: block_hash(0),
        };
        store.initialize_execution_tip(genesis).unwrap();

        let ephemeral = key(10);
        let survivor = key(11);
        let one = ExecutionTip {
            height: 1,
            hash: block_hash(1),
        };
        let two = ExecutionTip {
            height: 2,
            hash: block_hash(2),
        };
        store
            .commit_connect_batch(&[
                ConnectTransition {
                    expected_parent: genesis.hash,
                    next: one,
                    spent: Vec::new(),
                    created: vec![(ephemeral, coin(1))],
                    transaction_undos: Vec::new(),
                },
                ConnectTransition {
                    expected_parent: one.hash,
                    next: two,
                    spent: vec![ephemeral],
                    created: vec![(survivor, coin(2))],
                    transaction_undos: Vec::new(),
                },
            ])
            .unwrap();

        assert_eq!(store.execution_tip().unwrap(), two);
        assert!(store.get(ephemeral).unwrap().is_none());
        assert_eq!(store.get(survivor).unwrap(), Some(coin(2)));
        assert_eq!(store.block_undo(one.hash).unwrap(), Some(Vec::new()));
        assert_eq!(store.block_undo(two.hash).unwrap(), Some(Vec::new()));
        assert_eq!(store.tier_stats().unwrap(), TierStats { hot: 1, cold: 0 });
    }

    #[test]
    fn failed_late_batch_leaves_utxos_undo_and_tip_unchanged() {
        let directory = TempDir::new().unwrap();
        let store = MdbxUtxoStore::open(directory.path().join("mdbx")).unwrap();
        let genesis = ExecutionTip {
            height: 0,
            hash: block_hash(0),
        };
        store.initialize_execution_tip(genesis).unwrap();
        let first_output = key(20);
        let missing = key(21);
        let one = ExecutionTip {
            height: 1,
            hash: block_hash(1),
        };
        let two = ExecutionTip {
            height: 2,
            hash: block_hash(2),
        };
        let result = store.commit_connect_batch(&[
            ConnectTransition {
                expected_parent: genesis.hash,
                next: one,
                spent: Vec::new(),
                created: vec![(first_output, coin(1))],
                transaction_undos: Vec::new(),
            },
            ConnectTransition {
                expected_parent: one.hash,
                next: two,
                spent: vec![missing],
                created: Vec::new(),
                transaction_undos: Vec::new(),
            },
        ]);
        assert!(matches!(
            result,
            Err(ChainStoreError::Utxo(UtxoError::Missing(key))) if key == missing
        ));
        assert_eq!(store.execution_tip().unwrap(), genesis);
        assert!(store.get(first_output).unwrap().is_none());
        assert!(store.block_undo(one.hash).unwrap().is_none());
        assert!(store.block_undo(two.hash).unwrap().is_none());
    }

    #[test]
    fn compact_undo_roundtrips_without_legacy_coin_fields() {
        let directory = TempDir::new().unwrap();
        let store = MdbxUtxoStore::open(directory.path().join("mdbx")).unwrap();
        let genesis = ExecutionTip {
            height: 0,
            hash: block_hash_at(0),
        };
        store.initialize_execution_tip(genesis).unwrap();
        let next = ExecutionTip {
            height: 1,
            hash: block_hash_at(1),
        };
        let logical = UtxoUndo::from_parts(vec![(key(42), btcd_p2pkh_coin())], Vec::new());
        store
            .commit_connect(genesis.hash, next, &[], &[], std::slice::from_ref(&logical))
            .unwrap();

        let transaction = store.db().begin_ro_txn().unwrap();
        let undo = transaction.open_table(Some(UNDO)).unwrap();
        let raw = transaction
            .get::<Vec<u8>>(&undo, &next.hash.to_byte_array())
            .unwrap()
            .unwrap();
        // version + tx count + spent count + 34-byte key + coin length +
        // 26-byte compact coin + created count.
        assert_eq!(raw.len(), 80);
        let expected = UtxoUndo::from_parts(
            vec![(
                key(42),
                Utxo {
                    last_touched: 0,
                    ..btcd_p2pkh_coin()
                },
            )],
            Vec::new(),
        );
        assert_eq!(store.block_undo(next.hash).unwrap(), Some(vec![expected]));
    }

    #[test]
    fn ibd_batch_accepts_256_blocks_and_rejects_257() {
        let directory = TempDir::new().unwrap();
        let store = MdbxUtxoStore::open(directory.path().join("mdbx")).unwrap();
        let genesis = ExecutionTip {
            height: 0,
            hash: block_hash_at(0),
        };
        store.initialize_execution_tip(genesis).unwrap();
        let transitions = (1..=257_u32)
            .map(|height| ConnectTransition {
                expected_parent: block_hash_at(height - 1),
                next: ExecutionTip {
                    height,
                    hash: block_hash_at(height),
                },
                spent: Vec::new(),
                created: Vec::new(),
                transaction_undos: Vec::new(),
            })
            .collect::<Vec<_>>();
        assert!(matches!(
            store.commit_connect_batch(&transitions),
            Err(ChainStoreError::Utxo(UtxoError::Malformed(
                "MDBX IBD batch exceeds 256 blocks"
            )))
        ));
        store
            .commit_connect_batch(&transitions[..MAX_ATOMIC_IBD_BATCH_BLOCKS])
            .unwrap();
        assert_eq!(store.execution_tip().unwrap().height, 256);
        assert!(store.block_undo(block_hash_at(256)).unwrap().is_some());
    }

    #[test]
    fn compact_copy_reclaims_churn_and_preserves_all_four_tables() {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("mdbx");
        let mut store = MdbxUtxoStore::open_with_capacity(&path, 64 * 1024 * 1024).unwrap();
        let genesis = ExecutionTip {
            height: 0,
            hash: block_hash_at(0),
        };
        store.initialize_execution_tip(genesis).unwrap();
        let mut live = (0..512_u32)
            .map(|index| {
                let outpoint =
                    OutPoint::new(Txid::from_byte_array([index.to_le_bytes()[0]; 32]), index);
                (OutPointKey::from(outpoint), coin(0))
            })
            .collect::<Vec<_>>();
        store.apply(&[], &live).unwrap();
        for generation in 1..=32_u32 {
            let spent = live.iter().map(|(key, _)| *key).collect::<Vec<_>>();
            live = (0..512_u32)
                .map(|index| {
                    let mut txid = [0_u8; 32];
                    txid[..4].copy_from_slice(&generation.to_be_bytes());
                    txid[4..8].copy_from_slice(&index.to_be_bytes());
                    (
                        OutPointKey::from(OutPoint::new(Txid::from_byte_array(txid), index)),
                        coin(generation),
                    )
                })
                .collect();
            store.apply(&spent, &live).unwrap();
        }
        let replacement = (key(250), coin(1));
        let logical_undo =
            UtxoUndo::from_parts(vec![(live[0].0, live[0].1.clone())], vec![replacement.0]);
        let next = ExecutionTip {
            height: 1,
            hash: block_hash_at(1),
        };
        store
            .commit_connect(
                genesis.hash,
                next,
                &[live[0].0],
                std::slice::from_ref(&replacement),
                std::slice::from_ref(&logical_undo),
            )
            .unwrap();
        live[0] = replacement;
        let before = store.capacity().unwrap();
        let before_audit = store.audit().unwrap();
        assert_eq!(before_audit.hot_entries, 512);
        assert_eq!(before_audit.undo_entries, 1);
        assert!(before_audit.meta_entries >= 3);
        assert_eq!(before.capacity_bytes, 64 * 1024 * 1024);
        let report = store.compact_with_reserve(0).unwrap();
        assert_eq!(report.before_bytes, before.used_bytes);
        assert!(report.after_bytes <= report.before_bytes);
        assert_eq!(report.content_sha256, before_audit.content_sha256);
        assert_eq!(report.record_bytes, before_audit.record_bytes);
        assert_eq!(
            store.last_compacted_bytes().unwrap(),
            Some(report.after_bytes)
        );
        assert!(store.get(live[0].0).unwrap().is_some());
        assert_eq!(store.execution_tip().unwrap(), next);
        assert_eq!(
            store.block_undo(next.hash).unwrap(),
            Some(vec![logical_undo])
        );
        drop(store);

        let reopened = MdbxUtxoStore::open_with_capacity(&path, 64 * 1024 * 1024).unwrap();
        assert_eq!(reopened.tier_stats().unwrap().hot, 512);
        assert!(reopened.get(live[511].0).unwrap().is_some());
        assert_eq!(reopened.execution_tip().unwrap(), next);
        assert_eq!(
            reopened.audit().unwrap().content_sha256,
            report.content_sha256
        );
    }

    #[test]
    fn open_recovers_old_environment_left_mid_compaction_swap() {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("mdbx");
        let store = MdbxUtxoStore::open(&path).unwrap();
        store.apply(&[], &[(key(1), coin(1))]).unwrap();
        drop(store);

        let old = compacted_out_path(&path);
        let fresh = compaction_path(&path);
        fs::rename(&path, &old).unwrap();
        fs::create_dir_all(&fresh).unwrap();
        let recovered = MdbxUtxoStore::open(&path).unwrap();
        assert_eq!(recovered.get(key(1)).unwrap(), Some(coin(1)));
        assert!(!old.exists());
        assert!(!fresh.exists());
    }

    fn prepare_verified_compaction_candidate(path: &Path) -> [u8; 32] {
        let store = MdbxUtxoStore::open(path).unwrap();
        store
            .initialize_execution_tip(ExecutionTip {
                height: 0,
                hash: block_hash_at(0),
            })
            .unwrap();
        store.apply(&[], &[(key(7), coin(0))]).unwrap();
        let next = ExecutionTip {
            height: 1,
            hash: block_hash_at(1),
        };
        store
            .commit_connect(
                block_hash_at(0),
                next,
                &[key(7)],
                &[(key(8), coin(1))],
                &[UtxoUndo::from_parts(vec![(key(7), coin(0))], vec![key(8)])],
            )
            .unwrap();
        let audit = store.audit().unwrap();
        let fresh = compaction_path(path);
        fs::create_dir_all(&fresh).unwrap();
        store.db().copy_compact(&fresh.join("mdbx.dat")).unwrap();
        let copied = open_environment(&fresh, DEFAULT_CHAINSTATE_CAPACITY_BYTES).unwrap();
        let copied_audit =
            audit_environment(&copied, &fresh, DEFAULT_CHAINSTATE_CAPACITY_BYTES).unwrap();
        assert_eq!(audit.content_sha256, copied_audit.content_sha256);
        drop(copied);
        write_compaction_manifest(&fresh, audit).unwrap();
        drop(store);
        audit.content_sha256
    }

    fn assert_recovered_candidate(path: &Path, expected: [u8; 32]) {
        let recovered = MdbxUtxoStore::open(path).unwrap();
        let audit = recovered.audit().unwrap();
        assert_eq!(audit.content_sha256, expected);
        assert_eq!(audit.hot_entries, 1);
        assert_eq!(audit.undo_entries, 1);
        assert_eq!(audit.tip.unwrap().height, 1);
        assert!(!compaction_path(path).exists());
        assert!(!compacted_out_path(path).exists());
        assert!(!compaction_manifest_path(path).exists());
    }

    #[test]
    fn compaction_recovery_covers_every_durable_swap_topology() {
        // Copy complete, before the first rename (and its preceding fsync).
        let before_first = TempDir::new().unwrap();
        let path = before_first.path().join("mdbx");
        let expected = prepare_verified_compaction_candidate(&path);
        assert_recovered_candidate(&path, expected);

        // After source -> compacted-out, on either side of the parent fsync.
        let between_renames = TempDir::new().unwrap();
        let path = between_renames.path().join("mdbx");
        let expected = prepare_verified_compaction_candidate(&path);
        fs::rename(&path, compacted_out_path(&path)).unwrap();
        assert_recovered_candidate(&path, expected);

        // After compacting -> source, on either side of the parent fsync.
        let after_second = TempDir::new().unwrap();
        let path = after_second.path().join("mdbx");
        let expected = prepare_verified_compaction_candidate(&path);
        fs::rename(&path, compacted_out_path(&path)).unwrap();
        fs::rename(compaction_path(&path), &path).unwrap();
        assert_recovered_candidate(&path, expected);

        // A verified copy can be salvaged if it is the only durable survivor.
        let orphan = TempDir::new().unwrap();
        let path = orphan.path().join("mdbx");
        let expected = prepare_verified_compaction_candidate(&path);
        fs::remove_dir_all(&path).unwrap();
        assert_recovered_candidate(&path, expected);
    }

    #[test]
    fn compaction_recovery_rejects_a_tampered_verified_copy_and_keeps_old() {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("mdbx");
        prepare_verified_compaction_candidate(&path);
        fs::rename(&path, compacted_out_path(&path)).unwrap();
        fs::rename(compaction_path(&path), &path).unwrap();
        let manifest_path = compaction_manifest_path(&path);
        let mut manifest = read_compaction_manifest(&path).unwrap();
        manifest.content_sha256[0] ^= 0xff;
        fs::write(&manifest_path, serde_json::to_vec(&manifest).unwrap()).unwrap();

        assert!(matches!(
            MdbxUtxoStore::open(&path),
            Err(UtxoError::Malformed("MDBX compaction manifest identity"))
        ));
        assert!(compacted_out_path(&path).exists());
        assert!(manifest_path.exists());
    }

    #[test]
    fn compaction_policy_requires_pressure_and_growth() {
        let directory = TempDir::new().unwrap();
        let mut store =
            MdbxUtxoStore::open_with_capacity(directory.path().join("mdbx"), 1024 * 1024).unwrap();
        let used = store.capacity().unwrap().used_bytes;
        assert!(!store.compaction_is_worthwhile(100, 50, None).unwrap());
        assert!(store.compaction_is_worthwhile(1, 50, None).unwrap());
        assert!(!store.compaction_is_worthwhile(1, 50, Some(used)).unwrap());
        assert!(matches!(
            store.compaction_is_worthwhile(0, 50, None),
            Err(UtxoError::Malformed("MDBX compaction policy percentage"))
        ));
        let identity = store.audit().unwrap().content_sha256;
        assert!(matches!(
            store.compact_with_reserve(u64::MAX),
            Err(UtxoError::Io(_))
        ));
        assert_eq!(store.audit().unwrap().content_sha256, identity);
        assert!(!compaction_path(&store.database_dir).exists());
        assert!(!compacted_out_path(&store.database_dir).exists());
    }
}
