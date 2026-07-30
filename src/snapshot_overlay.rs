//! Snapshot-backed chainstate: an immutable compressed base plus one MDBX
//! overlay environment.
//!
//! The base UTXO set stays in a retained Core `dumptxoutset` file served
//! through its [`crate::core_snapshot_index`] minimal-perfect-hash sidecar.
//! Everything that changes after the base — coins created above it, base
//! coins spent above it, per-block undo data, and the execution tip — lives
//! in one MDBX environment with named tables, so a single read-write MDBX
//! transaction commits the complete block transition atomically:
//!
//! - `utxo_overlay` holds coins created above the base.
//! - `utxo_spent_base` holds tombstones for base coins spent above it.
//! - `block_undos` holds per-block undo records keyed by block hash.
//! - `meta` binds the base snapshot identity and the execution tip.
//!
//! Reads resolve `overlay → tombstone → base`; an overlay entry shadows the
//! base even when a tombstone also exists, and restores pick their target
//! from the coin's creation height, so the full mutation matrix — spend a
//! base coin, spend an overlay coin, create, and reorg-restore either kind —
//! stays exact.
//!
//! The environment is opened with a hard MDBX geometry ceiling. A commit
//! that would grow past it fails closed with `MDBX_MAP_FULL` instead of
//! growing without bound; [`SnapshotOverlayChainstate::rebase_into`] then
//! folds base and overlay into a fresh compressed snapshot at the current
//! tip, rebuilds the access index, and resets the overlay in one atomic
//! metadata switch, after which the ceiling is available again.
//!
//! The snapshot base is an assumed base in the AssumeUTXO sense: no undo
//! exists at or below it, so block disconnection refuses to cross it, and a
//! rebase moves that floor up to the tip it materialized.

use std::{
    fs::{self, File},
    io::{BufReader, Read, Write as _},
    path::{Path, PathBuf},
    sync::{Mutex, MutexGuard},
};

use bitcoin::{
    BlockHash, OutPoint,
    hashes::{Hash as _, sha256d},
};
use libmdbx::{
    Database, DatabaseOptions, Mode, NoWriteMap, RW, ReadWriteOptions, SyncMode, Table, TableFlags,
    Transaction, TransactionKind, WriteFlags,
};
use thiserror::Error;

use crate::{
    chain_store::{ChainStoreError, ConnectTransition, ExecutionChainStore},
    core_snapshot::{
        MAX_COINS_PER_TXID, compress_amount, compress_script, read_compact_size, read_core_varint,
        read_metadata, update_core_utxo_hash, write_compact_size, write_core_varint,
    },
    core_snapshot_index::{
        CoreSnapshotIndexError, CoreSnapshotUtxoIndex, SnapshotBaseIdentity,
        build_core_snapshot_index_with_identity,
    },
    execution_store::{ExecutionStoreError, ExecutionTip},
    headers::HeaderDag,
    undo_store::{decode_block_undo, encode_block_undo},
    utxo::{OutPointKey, TierStats, Utxo, UtxoError, UtxoStore, UtxoUndo},
};

const OVERLAY: &str = "utxo_overlay";
const TOMBSTONE: &str = "utxo_spent_base";
const UNDO: &str = "block_undos";
const META: &str = "meta";
const META_IDENTITY: &[u8] = b"identity";
const META_TIP: &[u8] = b"tip";
const IDENTITY_BYTES: usize = 4 + 32 + 4 + 8 + 8 + 32 + 64;
const TIP_BYTES: usize = 4 + 32;

/// Failures while opening, rebasing, or inspecting the overlay chainstate.
#[derive(Debug, Error)]
pub enum SnapshotOverlayError {
    /// Filesystem access failed.
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    /// MDBX environment operation failed.
    #[error("mdbx: {0}")]
    Mdbx(#[from] libmdbx::Error),
    /// Snapshot access-index operation failed.
    #[error("snapshot index: {0}")]
    Index(#[from] CoreSnapshotIndexError),
    /// UTXO codec or store operation failed.
    #[error("utxo: {0}")]
    Utxo(#[from] UtxoError),
    /// Chainstate commit surface failed.
    #[error("chainstate: {0}")]
    ChainStore(#[from] ChainStoreError),
    /// Persistent state is inconsistent with the supplied configuration.
    #[error("invalid snapshot overlay state: {0}")]
    Invalid(&'static str),
}

/// Disk-usage view of the bounded overlay environment.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OverlayCapacity {
    /// High-water bytes the environment has ever allocated.
    pub used_bytes: u64,
    /// Hard geometry ceiling configured at open.
    pub capacity_bytes: u64,
}

impl OverlayCapacity {
    /// Returns used capacity in whole percent, saturating at 100.
    #[must_use]
    pub fn used_percent(&self) -> u8 {
        if self.capacity_bytes == 0 {
            return 100;
        }
        u8::try_from((self.used_bytes.saturating_mul(100) / self.capacity_bytes).min(100))
            .expect("bounded percentage fits u8")
    }
}

/// Result of folding the overlay into a fresh compressed snapshot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RebaseReport {
    /// Identity of the new base, derived from this node's own chainstate.
    pub identity: SnapshotBaseIdentity,
    /// Coins in the new snapshot.
    pub coins: u64,
    /// New snapshot file length in bytes.
    pub snapshot_bytes: u64,
    /// New access-index file length in bytes.
    pub index_bytes: u64,
    /// Overlay coins folded into the base.
    pub folded_overlay: u64,
    /// Tombstoned base coins dropped from the base.
    pub dropped_tombstones: u64,
}

/// Configuration for opening a snapshot-backed overlay chainstate.
pub struct SnapshotOverlayConfig {
    /// Directory for the MDBX environment.
    pub database_dir: PathBuf,
    /// Retained compressed snapshot file.
    pub snapshot_path: PathBuf,
    /// Published access index beside the snapshot.
    pub index_path: PathBuf,
    /// Hard MDBX geometry ceiling in bytes (for example 3 GiB).
    pub capacity_bytes: u64,
    /// `last_touched` value synthesized for coins served from the base.
    pub import_time: u64,
    /// Creation median-time-past per height, `0..=base_height`, from the
    /// validated header chain; required to evaluate BIP68 time locks on
    /// base coins.
    pub mtp_by_height: Vec<u32>,
}

/// Snapshot-backed chainstate with one bounded MDBX overlay environment.
pub struct SnapshotOverlayChainstate {
    db: Database<NoWriteMap>,
    base: CoreSnapshotUtxoIndex,
    identity: SnapshotBaseIdentity,
    mtp_by_height: Vec<u32>,
    import_time: u64,
    capacity_bytes: u64,
    snapshot_path: PathBuf,
    index_path: PathBuf,
    write_guard: Mutex<()>,
}

impl SnapshotOverlayChainstate {
    /// Opens the environment and binds it to the snapshot base.
    ///
    /// A fresh environment records the supplied identity and initializes the
    /// tip at the base; reopening verifies that the stored identity, the
    /// access index, and the supplied identity all agree before serving.
    ///
    /// # Errors
    ///
    /// Fails closed on I/O or MDBX errors, an index/snapshot mismatch, an
    /// identity mismatch, or an MTP table that does not cover the base.
    pub fn open(
        config: SnapshotOverlayConfig,
        identity: Option<&SnapshotBaseIdentity>,
    ) -> Result<Self, SnapshotOverlayError> {
        let base = CoreSnapshotUtxoIndex::open(&config.index_path, &config.snapshot_path)?;
        fs::create_dir_all(&config.database_dir)?;
        let capacity = isize::try_from(config.capacity_bytes)
            .map_err(|_| SnapshotOverlayError::Invalid("capacity exceeds platform limits"))?;
        let db = Database::open_with_options(
            &config.database_dir,
            DatabaseOptions {
                max_tables: Some(4),
                mode: Mode::ReadWrite(ReadWriteOptions {
                    sync_mode: SyncMode::Durable,
                    max_size: Some(capacity),
                    ..ReadWriteOptions::default()
                }),
                ..DatabaseOptions::default()
            },
        )?;
        let identity = {
            let transaction = db.begin_rw_txn()?;
            for name in [OVERLAY, TOMBSTONE, UNDO, META] {
                transaction.create_table(Some(name), TableFlags::empty())?;
            }
            let meta = transaction.open_table(Some(META))?;
            let effective = match (transaction.get::<Vec<u8>>(&meta, META_IDENTITY)?, identity) {
                (None, None) => {
                    return Err(SnapshotOverlayError::Invalid(
                        "a fresh environment requires an explicit base identity",
                    ));
                }
                (None, Some(identity)) => {
                    transaction.put(
                        &meta,
                        META_IDENTITY,
                        encode_identity(identity, &base)?,
                        WriteFlags::empty(),
                    )?;
                    transaction.put(
                        &meta,
                        META_TIP,
                        encode_tip(ExecutionTip {
                            height: identity.height,
                            hash: identity.block_hash,
                        }),
                        WriteFlags::empty(),
                    )?;
                    identity.clone()
                }
                (Some(stored), supplied) => {
                    let decoded = decode_identity(&stored)?;
                    if let Some(supplied) = supplied {
                        if *supplied != decoded {
                            return Err(SnapshotOverlayError::Invalid(
                                "environment is bound to a different snapshot base",
                            ));
                        }
                    }
                    // Re-encoding against the opened index verifies that the
                    // supplied files still carry the bound network, coin
                    // count, byte length, and content digest.
                    if encode_identity(&decoded, &base)? != stored {
                        return Err(SnapshotOverlayError::Invalid(
                            "snapshot files do not match the bound base identity",
                        ));
                    }
                    decoded
                }
            };
            transaction.commit()?;
            effective
        };
        if base.base_height() != identity.height || base.base_block_hash() != identity.block_hash {
            return Err(SnapshotOverlayError::Invalid(
                "access index does not match the supplied base identity",
            ));
        }
        let expected_mtp_len = usize::try_from(identity.height)
            .ok()
            .and_then(|height| height.checked_add(1))
            .ok_or(SnapshotOverlayError::Invalid("base height overflows"))?;
        if config.mtp_by_height.len() != expected_mtp_len {
            return Err(SnapshotOverlayError::Invalid(
                "creation-MTP table must cover exactly heights 0..=base",
            ));
        }
        Ok(Self {
            db,
            base,
            identity,
            mtp_by_height: config.mtp_by_height,
            import_time: config.import_time,
            capacity_bytes: config.capacity_bytes,
            snapshot_path: config.snapshot_path,
            index_path: config.index_path,
            write_guard: Mutex::new(()),
        })
    }

    /// Returns the base identity a previously bound environment stores, or
    /// `None` when `database_dir` has no bound environment yet.
    ///
    /// The driver uses this to resume after a rebase, when the current base
    /// is no longer any compiled release anchor.
    ///
    /// # Errors
    ///
    /// Fails on I/O or MDBX errors, or a malformed identity record.
    pub fn stored_identity(
        database_dir: &Path,
        capacity_bytes: u64,
    ) -> Result<Option<SnapshotBaseIdentity>, SnapshotOverlayError> {
        if !database_dir.join("mdbx.dat").exists() {
            return Ok(None);
        }
        let capacity = isize::try_from(capacity_bytes)
            .map_err(|_| SnapshotOverlayError::Invalid("capacity exceeds platform limits"))?;
        let db: Database<NoWriteMap> = Database::open_with_options(
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
        )?;
        let transaction = db.begin_rw_txn()?;
        transaction.create_table(Some(META), TableFlags::empty())?;
        let meta = transaction.open_table(Some(META))?;
        transaction
            .get::<Vec<u8>>(&meta, META_IDENTITY)?
            .map(|stored| decode_identity(&stored))
            .transpose()
    }

    fn lock(&self) -> MutexGuard<'_, ()> {
        self.write_guard
            .lock()
            .expect("overlay write lock not poisoned")
    }

    /// Returns the immutable base identity currently backing this store.
    #[must_use]
    pub fn base_identity(&self) -> &SnapshotBaseIdentity {
        &self.identity
    }

    /// Reports high-water disk usage against the hard geometry ceiling.
    ///
    /// # Errors
    ///
    /// Fails on MDBX environment errors.
    pub fn capacity(&self) -> Result<OverlayCapacity, SnapshotOverlayError> {
        let info = self.db.info()?;
        let page_size = u64::from(self.db.stat()?.page_size());
        let last_page = u64::try_from(info.last_pgno())
            .map_err(|_| SnapshotOverlayError::Invalid("page number overflows"))?;
        Ok(OverlayCapacity {
            used_bytes: (last_page + 1).saturating_mul(page_size),
            capacity_bytes: self.capacity_bytes,
        })
    }

    /// Returns whether high-water usage has reached `threshold_percent`.
    ///
    /// # Errors
    ///
    /// Fails on MDBX environment errors.
    pub fn needs_rebase(&self, threshold_percent: u8) -> Result<bool, SnapshotOverlayError> {
        Ok(self.capacity()?.used_percent() >= threshold_percent)
    }

    /// Reports whether an error is the hard capacity ceiling failing closed.
    #[must_use]
    pub fn is_capacity_exhausted(error: &ChainStoreError) -> bool {
        matches!(
            error,
            ChainStoreError::Utxo(UtxoError::Mdbx(libmdbx::Error::MapFull))
        )
    }

    fn base_utxo(&self, outpoint: OutPoint) -> Result<Option<Utxo>, UtxoError> {
        let Some(coin) = self.base.get(&outpoint).map_err(index_read_error)? else {
            return Ok(None);
        };
        let mtp = usize::try_from(coin.height)
            .ok()
            .and_then(|height| self.mtp_by_height.get(height))
            .copied()
            .ok_or(UtxoError::Malformed("base coin height above MTP table"))?;
        Ok(Some(Utxo {
            value_sats: coin.value_sats,
            height: coin.height,
            is_coinbase: coin.is_coinbase,
            last_touched: self.import_time,
            creation_mtp: mtp,
            script_pubkey: coin.script_pubkey,
        }))
    }

    fn read_tip<K: TransactionKind>(
        transaction: &Transaction<'_, K, NoWriteMap>,
        meta: &Table<'_>,
    ) -> Result<ExecutionTip, ChainStoreError> {
        let bytes = transaction
            .get::<Vec<u8>>(meta, META_TIP)
            .map_err(utxo_mdbx)?
            .ok_or(ChainStoreError::Utxo(UtxoError::Malformed(
                "missing overlay tip",
            )))?;
        decode_tip(&bytes)
    }

    /// Applies one connect mutation inside the caller's transaction.
    fn connect_mutation(
        &self,
        transaction: &Transaction<'_, RW, NoWriteMap>,
        overlay: &Table<'_>,
        tombstone: &Table<'_>,
        spent: &[OutPointKey],
        created: &[(OutPointKey, Utxo)],
    ) -> Result<UtxoUndo, ChainStoreError> {
        let mut seen_spent = std::collections::BTreeSet::new();
        let mut undo_spent = Vec::with_capacity(spent.len());
        for key in spent {
            if !seen_spent.insert(*key) {
                return Err(ChainStoreError::Utxo(UtxoError::DuplicateSpend(*key)));
            }
            if let Some(value) = transaction
                .get::<Vec<u8>>(overlay, key.as_bytes())
                .map_err(utxo_mdbx)?
            {
                transaction
                    .del(overlay, key.as_bytes(), None)
                    .map_err(utxo_mdbx)?;
                undo_spent.push((*key, Utxo::decode(&value)?));
                continue;
            }
            if transaction
                .get::<()>(tombstone, key.as_bytes())
                .map_err(utxo_mdbx)?
                .is_some()
            {
                return Err(ChainStoreError::Utxo(UtxoError::Missing(*key)));
            }
            let coin = self
                .base_utxo(key.to_outpoint())?
                .ok_or(ChainStoreError::Utxo(UtxoError::Missing(*key)))?;
            transaction
                .put(tombstone, key.as_bytes(), [], WriteFlags::empty())
                .map_err(utxo_mdbx)?;
            undo_spent.push((*key, coin));
        }
        let mut seen_created = std::collections::BTreeSet::new();
        for (key, utxo) in created {
            if !seen_created.insert(*key) {
                return Err(ChainStoreError::Utxo(UtxoError::Duplicate(*key)));
            }
            if !seen_spent.contains(key) {
                if transaction
                    .get::<()>(overlay, key.as_bytes())
                    .map_err(utxo_mdbx)?
                    .is_some()
                {
                    return Err(ChainStoreError::Utxo(UtxoError::Duplicate(*key)));
                }
                let base_hidden = transaction
                    .get::<()>(tombstone, key.as_bytes())
                    .map_err(utxo_mdbx)?
                    .is_some();
                if !base_hidden && self.base_utxo(key.to_outpoint())?.is_some() {
                    return Err(ChainStoreError::Utxo(UtxoError::Duplicate(*key)));
                }
            }
            transaction
                .put(overlay, key.as_bytes(), utxo.encode()?, WriteFlags::empty())
                .map_err(utxo_mdbx)?;
        }
        Ok(UtxoUndo::from_parts(
            undo_spent,
            created.iter().map(|(key, _)| *key).collect(),
        ))
    }

    /// Applies one disconnect mutation: removes coins the block created and
    /// restores coins it spent, picking the restore target from the coin's
    /// creation height relative to the base.
    fn disconnect_mutation(
        &self,
        transaction: &Transaction<'_, RW, NoWriteMap>,
        overlay: &Table<'_>,
        tombstone: &Table<'_>,
        removed: &[OutPointKey],
        restored: &[(OutPointKey, Utxo)],
    ) -> Result<(), ChainStoreError> {
        for key in removed {
            if transaction
                .get::<()>(overlay, key.as_bytes())
                .map_err(utxo_mdbx)?
                .is_none()
            {
                return Err(ChainStoreError::Utxo(UtxoError::Missing(*key)));
            }
            transaction
                .del(overlay, key.as_bytes(), None)
                .map_err(utxo_mdbx)?;
        }
        for (key, utxo) in restored {
            if utxo.height > self.identity.height {
                if transaction
                    .get::<()>(overlay, key.as_bytes())
                    .map_err(utxo_mdbx)?
                    .is_some()
                {
                    return Err(ChainStoreError::Utxo(UtxoError::Duplicate(*key)));
                }
                transaction
                    .put(overlay, key.as_bytes(), utxo.encode()?, WriteFlags::empty())
                    .map_err(utxo_mdbx)?;
            } else {
                if transaction
                    .get::<()>(tombstone, key.as_bytes())
                    .map_err(utxo_mdbx)?
                    .is_none()
                {
                    return Err(ChainStoreError::Utxo(UtxoError::Duplicate(*key)));
                }
                transaction
                    .del(tombstone, key.as_bytes(), None)
                    .map_err(utxo_mdbx)?;
            }
        }
        Ok(())
    }

    fn advance_tip(
        transaction: &Transaction<'_, RW, NoWriteMap>,
        meta: &Table<'_>,
        expected_parent: BlockHash,
        next: ExecutionTip,
    ) -> Result<(), ChainStoreError> {
        let current = Self::read_tip(transaction, meta)?;
        if current.hash != expected_parent || current.height.checked_add(1) != Some(next.height) {
            return Err(ChainStoreError::Execution(
                ExecutionStoreError::NonSequential {
                    current_height: current.height,
                    current_hash: current.hash,
                },
            ));
        }
        transaction
            .put(meta, META_TIP, encode_tip(next), WriteFlags::empty())
            .map_err(utxo_mdbx)?;
        Ok(())
    }

    fn commit_transitions(&self, transitions: &[ConnectTransition]) -> Result<(), ChainStoreError> {
        let _guard = self.lock();
        let transaction = self.db.begin_rw_txn().map_err(utxo_mdbx)?;
        let overlay = transaction.open_table(Some(OVERLAY)).map_err(utxo_mdbx)?;
        let tombstone = transaction.open_table(Some(TOMBSTONE)).map_err(utxo_mdbx)?;
        let undo = transaction.open_table(Some(UNDO)).map_err(utxo_mdbx)?;
        let meta = transaction.open_table(Some(META)).map_err(utxo_mdbx)?;
        for transition in transitions {
            Self::advance_tip(
                &transaction,
                &meta,
                transition.expected_parent,
                transition.next,
            )?;
            self.connect_mutation(
                &transaction,
                &overlay,
                &tombstone,
                &transition.spent,
                &transition.created,
            )?;
            let block_undo = encode_block_undo(&transition.transaction_undos)?;
            transaction
                .put(
                    &undo,
                    transition.next.hash.to_byte_array(),
                    block_undo,
                    WriteFlags::empty(),
                )
                .map_err(utxo_mdbx)?;
        }
        transaction.commit().map_err(utxo_mdbx)?;
        Ok(())
    }

    /// Folds the base snapshot and the overlay into a fresh compressed
    /// snapshot at the current tip, rebuilds the access index, and resets
    /// the overlay, tombstone, and undo tables in one atomic switch.
    ///
    /// `mtp_extension` supplies creation median-time-past values for heights
    /// `old_base + 1 ..= tip`, in order; they extend the store's MTP table so
    /// folded overlay coins keep evaluating BIP68 identically. Every folded
    /// coin's stored `creation_mtp` is cross-checked against the extension.
    ///
    /// The previous snapshot and index files are left in place as operator
    /// evidence; only the environment's identity binding switches. Because
    /// undo data is cleared, block disconnection can no longer cross the new
    /// base — the same contract an AssumeUTXO activation establishes.
    ///
    /// # Errors
    ///
    /// Fails closed without switching identity on I/O errors, existing
    /// output paths, an MTP extension of the wrong length, or any
    /// merge/re-authentication mismatch.
    #[allow(clippy::too_many_lines)]
    pub fn rebase_into(
        &mut self,
        new_snapshot_path: impl AsRef<Path>,
        new_index_path: impl AsRef<Path>,
        mtp_extension: &[u32],
    ) -> Result<RebaseReport, SnapshotOverlayError> {
        let new_snapshot_path = new_snapshot_path.as_ref();
        let new_index_path = new_index_path.as_ref();
        if new_snapshot_path.exists() || new_index_path.exists() {
            return Err(SnapshotOverlayError::Invalid(
                "rebase output paths already exist",
            ));
        }
        // `&mut self` already excludes every other reader and writer, and the
        // read transaction below pins one MVCC view for the whole merge.
        let transaction = self.db.begin_ro_txn()?;
        let meta = transaction.open_table(Some(META))?;
        let tip = Self::read_tip(&transaction, &meta)?;
        let expected_extension = tip
            .height
            .checked_sub(self.identity.height)
            .ok_or(SnapshotOverlayError::Invalid("tip below base"))?;
        if mtp_extension.len() != usize::try_from(expected_extension).expect("height fits usize") {
            return Err(SnapshotOverlayError::Invalid(
                "MTP extension must cover exactly heights base+1..=tip",
            ));
        }
        let overlay = transaction.open_table(Some(OVERLAY))?;
        let tombstone = transaction.open_table(Some(TOMBSTONE))?;
        let folded_overlay = u64::try_from(transaction.table_stat(&overlay)?.entries())
            .expect("entry count fits u64");
        let dropped_tombstones = u64::try_from(transaction.table_stat(&tombstone)?.entries())
            .expect("entry count fits u64");
        let coins = self
            .base
            .coin_count()
            .checked_sub(dropped_tombstones)
            .and_then(|kept| kept.checked_add(folded_overlay))
            .ok_or(SnapshotOverlayError::Invalid("coin count underflow"))?;
        if coins == 0 {
            return Err(SnapshotOverlayError::Invalid(
                "rebase would materialize an empty UTXO set",
            ));
        }

        // Stream-merge the old snapshot (minus tombstones) with the overlay
        // into a temporary file, hashing Core's commitment as we go.
        let temporary = new_snapshot_path.with_file_name({
            let mut name = new_snapshot_path
                .file_name()
                .ok_or(SnapshotOverlayError::Invalid("rebase snapshot file name"))?
                .to_owned();
            name.push(".tmp");
            name
        });
        let mut writer = std::io::BufWriter::new(File::create(&temporary)?);
        let mut header = Vec::with_capacity(51);
        header.extend_from_slice(b"utxo\xff");
        header.extend_from_slice(&2_u16.to_le_bytes());
        header.extend_from_slice(&self.base.network().magic().to_bytes());
        header.extend_from_slice(&tip.hash.to_byte_array());
        header.extend_from_slice(&coins.to_le_bytes());
        writer.write_all(&header)?;

        let mut core_hash = sha256d::Hash::engine();
        let mut written = 0_u64;
        let mut base_groups = BaseGroupReader::new(&self.snapshot_path)?;
        let mut overlay_groups = OverlayGroupReader::new(&transaction, &overlay)?;
        let mut base_group = base_groups.next_group()?;
        let mut overlay_group = overlay_groups.next_group();
        loop {
            let take_base = match (&base_group, &overlay_group) {
                (None, None) => break,
                (Some(_), None) => (true, false),
                (None, Some(_)) => (false, true),
                (Some(base), Some(new)) => match base.0.cmp(&new.0) {
                    std::cmp::Ordering::Less => (true, false),
                    std::cmp::Ordering::Greater => (false, true),
                    std::cmp::Ordering::Equal => (true, true),
                },
            };
            let mut txid = [0_u8; 32];
            let mut coins_in_group = Vec::new();
            if take_base.0 {
                let base = base_group.take().expect("selected above");
                txid = base.0;
                coins_in_group = Self::filter_tombstoned(&transaction, &tombstone, &base)?;
                base_group = base_groups.next_group()?;
            }
            if take_base.1 {
                let (new_txid, mut new_coins) = overlay_group.take().expect("selected above");
                txid = new_txid;
                coins_in_group.append(&mut new_coins);
                overlay_group = overlay_groups.next_group();
            }
            if coins_in_group.is_empty() {
                continue;
            }
            coins_in_group.sort_unstable_by_key(|(vout, _)| *vout);
            for (vout, utxo) in &coins_in_group {
                if utxo.height > self.identity.height {
                    let offset = usize::try_from(utxo.height - self.identity.height - 1)
                        .expect("height fits usize");
                    if mtp_extension[offset] != utxo.creation_mtp {
                        return Err(SnapshotOverlayError::Invalid(
                            "MTP extension disagrees with a folded overlay coin",
                        ));
                    }
                }
                let mut key = [0_u8; 36];
                key[..32].copy_from_slice(&txid);
                key[32..].copy_from_slice(&vout.to_le_bytes());
                update_core_utxo_hash(
                    &mut core_hash,
                    OutPointKey::from_bytes(&key).expect("fixed key length"),
                    utxo,
                );
            }
            let mut group_bytes = Vec::new();
            group_bytes.extend_from_slice(&txid);
            write_compact_size(
                &mut group_bytes,
                u64::try_from(coins_in_group.len()).expect("group size fits u64"),
            );
            for (vout, utxo) in &coins_in_group {
                write_compact_size(&mut group_bytes, u64::from(*vout));
                write_core_varint(
                    &mut group_bytes,
                    u64::from((utxo.height << 1) | u32::from(utxo.is_coinbase)),
                );
                write_core_varint(&mut group_bytes, compress_amount(utxo.value_sats));
                compress_script(&mut group_bytes, &utxo.script_pubkey);
            }
            writer.write_all(&group_bytes)?;
            written += u64::try_from(coins_in_group.len()).expect("group size fits u64");
        }
        if written != coins {
            return Err(SnapshotOverlayError::Invalid(
                "materialized coin count mismatch",
            ));
        }
        writer.flush()?;
        writer.get_ref().sync_all()?;
        drop(writer);
        drop(transaction);
        fs::rename(&temporary, new_snapshot_path)?;
        // Windows has no portable directory fsync; see `diagnostics::sync_directory`.
        #[cfg(unix)]
        if let Some(parent) = new_snapshot_path.parent() {
            File::open(parent)?.sync_all()?;
        }

        let identity = SnapshotBaseIdentity {
            height: tip.height,
            block_hash: tip.hash,
            hash_serialized: sha256d::Hash::from_engine(core_hash).to_string(),
        };
        // The index build re-decodes the complete file and re-derives the
        // commitment, so a compression asymmetry cannot survive publication.
        let report =
            build_core_snapshot_index_with_identity(new_snapshot_path, new_index_path, &identity)?;
        let new_base = CoreSnapshotUtxoIndex::open(new_index_path, new_snapshot_path)?;

        let transaction = self.db.begin_rw_txn()?;
        for name in [OVERLAY, TOMBSTONE, UNDO] {
            let table = transaction.open_table(Some(name))?;
            transaction.clear_table(&table)?;
        }
        let meta = transaction.open_table(Some(META))?;
        transaction.put(
            &meta,
            META_IDENTITY,
            encode_identity(&identity, &new_base)?,
            WriteFlags::empty(),
        )?;
        transaction.commit()?;

        self.mtp_by_height.extend_from_slice(mtp_extension);
        self.base = new_base;
        self.identity = identity.clone();
        new_snapshot_path.clone_into(&mut self.snapshot_path);
        new_index_path.clone_into(&mut self.index_path);
        Ok(RebaseReport {
            identity,
            coins,
            snapshot_bytes: report.snapshot_bytes,
            index_bytes: report.index_bytes,
            folded_overlay,
            dropped_tombstones,
        })
    }

    fn filter_tombstoned<K: TransactionKind>(
        transaction: &Transaction<'_, K, NoWriteMap>,
        tombstone: &Table<'_>,
        group: &([u8; 32], Vec<(u32, Utxo)>),
    ) -> Result<Vec<(u32, Utxo)>, SnapshotOverlayError> {
        let (txid, coins) = group;
        let mut kept = Vec::with_capacity(coins.len());
        for (vout, utxo) in coins {
            let mut key = [0_u8; 36];
            key[..32].copy_from_slice(txid);
            key[32..].copy_from_slice(&vout.to_le_bytes());
            if transaction.get::<()>(tombstone, &key)?.is_none() {
                kept.push((*vout, utxo.clone()));
            }
        }
        Ok(kept)
    }
}

/// Sequentially decodes txid groups from the retained snapshot file.
struct BaseGroupReader {
    reader: BufReader<File>,
    remaining: u64,
}

impl BaseGroupReader {
    fn new(snapshot_path: &Path) -> Result<Self, SnapshotOverlayError> {
        let mut reader = BufReader::new(File::open(snapshot_path)?);
        let metadata = read_metadata(&mut reader).map_err(index_read_error)?;
        Ok(Self {
            reader,
            remaining: metadata.coins_count,
        })
    }

    #[allow(clippy::type_complexity)]
    fn next_group(&mut self) -> Result<Option<([u8; 32], Vec<(u32, Utxo)>)>, SnapshotOverlayError> {
        if self.remaining == 0 {
            return Ok(None);
        }
        let mut txid = [0_u8; 32];
        self.reader.read_exact(&mut txid)?;
        let group_count = read_compact_size(&mut self.reader).map_err(index_read_error)?;
        if group_count == 0 || group_count > self.remaining || group_count > MAX_COINS_PER_TXID {
            return Err(SnapshotOverlayError::Invalid("snapshot group count"));
        }
        let mut coins = Vec::with_capacity(
            usize::try_from(group_count).expect("bounded group count fits usize"),
        );
        for _ in 0..group_count {
            let vout = read_compact_size(&mut self.reader).map_err(index_read_error)?;
            let vout =
                u32::try_from(vout).map_err(|_| SnapshotOverlayError::Invalid("vout overflow"))?;
            let code = read_core_varint(&mut self.reader).map_err(index_read_error)?;
            let code =
                u32::try_from(code).map_err(|_| SnapshotOverlayError::Invalid("coin code"))?;
            let amount = read_core_varint(&mut self.reader).map_err(index_read_error)?;
            let value_sats = crate::core_snapshot::decompress_amount(amount)
                .ok_or(SnapshotOverlayError::Invalid("amount overflow"))?;
            let script_pubkey = crate::core_snapshot::decompress_script(&mut self.reader)
                .map_err(index_read_error)?;
            coins.push((
                vout,
                Utxo {
                    value_sats,
                    height: code >> 1,
                    is_coinbase: code & 1 == 1,
                    last_touched: 0,
                    creation_mtp: 0,
                    script_pubkey,
                },
            ));
            self.remaining -= 1;
        }
        Ok(Some((txid, coins)))
    }
}

/// Groups the overlay cursor's key-ordered rows by txid.
struct OverlayGroupReader {
    rows: std::vec::IntoIter<([u8; 36], Utxo)>,
    pending: Option<([u8; 36], Utxo)>,
}

impl OverlayGroupReader {
    fn new<K: TransactionKind>(
        transaction: &Transaction<'_, K, NoWriteMap>,
        overlay: &Table<'_>,
    ) -> Result<Self, SnapshotOverlayError> {
        let mut cursor = transaction.cursor(overlay)?;
        let mut rows = Vec::new();
        for row in cursor.iter_start::<Vec<u8>, Vec<u8>>() {
            let (key, value) = row?;
            let key: [u8; 36] = key
                .as_slice()
                .try_into()
                .map_err(|_| SnapshotOverlayError::Invalid("overlay key width"))?;
            rows.push((key, Utxo::decode(&value)?));
        }
        Ok(Self {
            rows: rows.into_iter(),
            pending: None,
        })
    }

    #[allow(clippy::type_complexity)]
    fn next_group(&mut self) -> Option<([u8; 32], Vec<(u32, Utxo)>)> {
        let first = self.pending.take().or_else(|| self.rows.next())?;
        let txid: [u8; 32] = first.0[..32].try_into().expect("fixed key length");
        let mut coins = vec![(
            u32::from_le_bytes(first.0[32..].try_into().expect("fixed key length")),
            first.1,
        )];
        for row in self.rows.by_ref() {
            if row.0[..32] == txid {
                coins.push((
                    u32::from_le_bytes(row.0[32..].try_into().expect("fixed key length")),
                    row.1,
                ));
            } else {
                self.pending = Some(row);
                break;
            }
        }
        Some((txid, coins))
    }
}

impl UtxoStore for SnapshotOverlayChainstate {
    fn get(&self, outpoint: OutPointKey) -> Result<Option<Utxo>, UtxoError> {
        let transaction = self.db.begin_ro_txn()?;
        let overlay = transaction.open_table(Some(OVERLAY))?;
        if let Some(value) = transaction.get::<Vec<u8>>(&overlay, outpoint.as_bytes())? {
            return Utxo::decode(&value).map(Some);
        }
        let tombstone = transaction.open_table(Some(TOMBSTONE))?;
        if transaction
            .get::<()>(&tombstone, outpoint.as_bytes())?
            .is_some()
        {
            return Ok(None);
        }
        drop(transaction);
        self.base_utxo(outpoint.to_outpoint())
    }

    fn get_many(
        &self,
        outpoints: &[OutPointKey],
    ) -> Result<Vec<(OutPointKey, Option<Utxo>)>, UtxoError> {
        let transaction = self.db.begin_ro_txn()?;
        let overlay = transaction.open_table(Some(OVERLAY))?;
        let tombstone = transaction.open_table(Some(TOMBSTONE))?;
        let mut results = Vec::with_capacity(outpoints.len());
        for outpoint in outpoints {
            if let Some(value) = transaction.get::<Vec<u8>>(&overlay, outpoint.as_bytes())? {
                results.push((*outpoint, Some(Utxo::decode(&value)?)));
            } else if transaction
                .get::<()>(&tombstone, outpoint.as_bytes())?
                .is_some()
            {
                results.push((*outpoint, None));
            } else {
                results.push((*outpoint, self.base_utxo(outpoint.to_outpoint())?));
            }
        }
        Ok(results)
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
        let transaction = self.db.begin_rw_txn()?;
        let overlay = transaction.open_table(Some(OVERLAY))?;
        let tombstone = transaction.open_table(Some(TOMBSTONE))?;
        let undo = self
            .connect_mutation(&transaction, &overlay, &tombstone, spent, created)
            .map_err(chain_store_to_utxo)?;
        transaction.commit()?;
        Ok(undo)
    }

    fn undo(&self, undo: &UtxoUndo, _now: u64, _hot_window_secs: u64) -> Result<(), UtxoError> {
        let _guard = self.lock();
        let transaction = self.db.begin_rw_txn()?;
        let overlay = transaction.open_table(Some(OVERLAY))?;
        let tombstone = transaction.open_table(Some(TOMBSTONE))?;
        self.disconnect_mutation(
            &transaction,
            &overlay,
            &tombstone,
            undo.created(),
            undo.spent(),
        )
        .map_err(chain_store_to_utxo)?;
        transaction.commit()?;
        Ok(())
    }

    fn age_to_cold(&self, _now: u64, _hot_window_secs: u64) -> Result<u64, UtxoError> {
        // The base file is the cold tier by construction; the overlay stays hot.
        Ok(0)
    }

    fn snapshot_entries(&self) -> Result<std::collections::BTreeMap<OutPointKey, Utxo>, UtxoError> {
        Err(UtxoError::Malformed(
            "snapshot-backed chainstate exports through rebase materialization",
        ))
    }

    fn replace_all(
        &self,
        _entries: &std::collections::BTreeMap<OutPointKey, Utxo>,
        _now: u64,
        _hot_window_secs: u64,
    ) -> Result<(), UtxoError> {
        Err(UtxoError::Malformed(
            "snapshot-backed chainstate cannot replace its immutable base",
        ))
    }

    fn tier_stats(&self) -> Result<TierStats, UtxoError> {
        let transaction = self.db.begin_ro_txn()?;
        let overlay = transaction.open_table(Some(OVERLAY))?;
        let tombstone = transaction.open_table(Some(TOMBSTONE))?;
        let hot = u64::try_from(transaction.table_stat(&overlay)?.entries())
            .expect("entry count fits u64");
        let tombstones = u64::try_from(transaction.table_stat(&tombstone)?.entries())
            .expect("entry count fits u64");
        Ok(TierStats {
            hot,
            cold: self.base.coin_count().saturating_sub(tombstones),
        })
    }
}

impl ExecutionChainStore for SnapshotOverlayChainstate {
    fn execution_tip(&self) -> Result<ExecutionTip, ChainStoreError> {
        let transaction = self.db.begin_ro_txn().map_err(utxo_mdbx)?;
        let meta = transaction.open_table(Some(META)).map_err(utxo_mdbx)?;
        Self::read_tip(&transaction, &meta)
    }

    fn assumed_snapshot_base(&self) -> Result<Option<ExecutionTip>, ChainStoreError> {
        Ok(Some(ExecutionTip {
            height: self.identity.height,
            hash: self.identity.block_hash,
        }))
    }

    fn block_undo(&self, hash: BlockHash) -> Result<Option<Vec<UtxoUndo>>, ChainStoreError> {
        let transaction = self.db.begin_ro_txn().map_err(utxo_mdbx)?;
        let undo = transaction.open_table(Some(UNDO)).map_err(utxo_mdbx)?;
        transaction
            .get::<Vec<u8>>(&undo, &hash.to_byte_array())
            .map_err(utxo_mdbx)?
            .map(|bytes| decode_block_undo(&bytes).map_err(ChainStoreError::Undo))
            .transpose()
    }

    fn retains_block_undo(&self) -> bool {
        true
    }

    fn prune_block_undos_before(
        &self,
        headers: &HeaderDag,
        retain_from_height: u32,
    ) -> Result<u64, ChainStoreError> {
        let _guard = self.lock();
        let transaction = self.db.begin_rw_txn().map_err(utxo_mdbx)?;
        let undo = transaction.open_table(Some(UNDO)).map_err(utxo_mdbx)?;
        let mut expired = Vec::new();
        {
            let mut cursor = transaction.cursor(&undo).map_err(utxo_mdbx)?;
            for row in cursor.iter_start::<Vec<u8>, ()>() {
                let (key, ()) = row.map_err(utxo_mdbx)?;
                let key: [u8; 32] = key
                    .as_slice()
                    .try_into()
                    .map_err(|_| ChainStoreError::Utxo(UtxoError::Malformed("undo key width")))?;
                let hash = BlockHash::from_byte_array(key);
                let header =
                    headers
                        .get(&hash)
                        .ok_or(ChainStoreError::Utxo(UtxoError::Malformed(
                            "block undo references an unknown header",
                        )))?;
                if header.height < retain_from_height {
                    expired.push(hash);
                }
            }
        }
        let mut removed = 0_u64;
        for hash in expired {
            removed += u64::from(
                transaction
                    .del(&undo, hash.to_byte_array(), None)
                    .map_err(utxo_mdbx)?,
            );
        }
        transaction.commit().map_err(utxo_mdbx)?;
        Ok(removed)
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
        let transaction = self.db.begin_rw_txn().map_err(utxo_mdbx)?;
        let overlay = transaction.open_table(Some(OVERLAY)).map_err(utxo_mdbx)?;
        let tombstone = transaction.open_table(Some(TOMBSTONE)).map_err(utxo_mdbx)?;
        let undo_table = transaction.open_table(Some(UNDO)).map_err(utxo_mdbx)?;
        let meta = transaction.open_table(Some(META)).map_err(utxo_mdbx)?;
        Self::advance_tip(&transaction, &meta, expected_parent, next)?;
        let undo = self.connect_mutation(&transaction, &overlay, &tombstone, spent, created)?;
        transaction
            .put(
                &undo_table,
                next.hash.to_byte_array(),
                encode_block_undo(transaction_undos)?,
                WriteFlags::empty(),
            )
            .map_err(utxo_mdbx)?;
        transaction.commit().map_err(utxo_mdbx)?;
        Ok(undo)
    }

    fn commit_connect_batch(
        &self,
        transitions: &[ConnectTransition],
    ) -> Result<(), ChainStoreError> {
        self.commit_transitions(transitions)
    }

    fn commit_disconnect(
        &self,
        expected_current: ExecutionTip,
        parent: ExecutionTip,
        spent: &[OutPointKey],
        created: &[(OutPointKey, Utxo)],
        _transaction_undos: &[UtxoUndo],
    ) -> Result<UtxoUndo, ChainStoreError> {
        let _guard = self.lock();
        let transaction = self.db.begin_rw_txn().map_err(utxo_mdbx)?;
        let meta = transaction.open_table(Some(META)).map_err(utxo_mdbx)?;
        let current = Self::read_tip(&transaction, &meta)?;
        if current != expected_current || parent.height.checked_add(1) != Some(current.height) {
            return Err(ChainStoreError::Execution(
                ExecutionStoreError::NonSequential {
                    current_height: current.height,
                    current_hash: current.hash,
                },
            ));
        }
        let base_tip = ExecutionTip {
            height: self.identity.height,
            hash: self.identity.block_hash,
        };
        if base_tip == current {
            return Err(ChainStoreError::Utxo(UtxoError::Malformed(
                "cannot disconnect the assumed snapshot base",
            )));
        }
        let overlay = transaction.open_table(Some(OVERLAY)).map_err(utxo_mdbx)?;
        let tombstone = transaction.open_table(Some(TOMBSTONE)).map_err(utxo_mdbx)?;
        self.disconnect_mutation(&transaction, &overlay, &tombstone, spent, created)?;
        let undo_table = transaction.open_table(Some(UNDO)).map_err(utxo_mdbx)?;
        if !transaction
            .del(&undo_table, current.hash.to_byte_array(), None)
            .map_err(utxo_mdbx)?
        {
            return Err(ChainStoreError::Utxo(UtxoError::Malformed(
                "missing atomic disconnect undo",
            )));
        }
        transaction
            .put(&meta, META_TIP, encode_tip(parent), WriteFlags::empty())
            .map_err(utxo_mdbx)?;
        transaction.commit().map_err(utxo_mdbx)?;
        Ok(UtxoUndo::from_parts(created.to_vec(), spent.to_vec()))
    }
}

fn utxo_mdbx(error: libmdbx::Error) -> ChainStoreError {
    ChainStoreError::Utxo(UtxoError::Mdbx(error))
}

fn chain_store_to_utxo(error: ChainStoreError) -> UtxoError {
    match error {
        ChainStoreError::Utxo(error) => error,
        _ => UtxoError::Malformed("overlay chainstate mutation failed"),
    }
}

fn index_read_error(error: impl Into<CoreSnapshotIndexError>) -> UtxoError {
    match error.into() {
        CoreSnapshotIndexError::Io(error) => UtxoError::Io(error),
        _ => UtxoError::Malformed("snapshot base read failed"),
    }
}

fn encode_identity(
    identity: &SnapshotBaseIdentity,
    base: &CoreSnapshotUtxoIndex,
) -> Result<Vec<u8>, SnapshotOverlayError> {
    let hash: &[u8; 64] = identity
        .hash_serialized
        .as_bytes()
        .try_into()
        .map_err(|_| SnapshotOverlayError::Invalid("serialized UTXO-set hash width"))?;
    let mut bytes = Vec::with_capacity(IDENTITY_BYTES);
    bytes.extend_from_slice(&base.network().magic().to_bytes());
    bytes.extend_from_slice(&identity.block_hash.to_byte_array());
    bytes.extend_from_slice(&identity.height.to_le_bytes());
    bytes.extend_from_slice(&base.coin_count().to_le_bytes());
    bytes.extend_from_slice(&base.snapshot_bytes().to_le_bytes());
    bytes.extend_from_slice(&base.snapshot_sha256());
    bytes.extend_from_slice(hash);
    debug_assert_eq!(bytes.len(), IDENTITY_BYTES);
    Ok(bytes)
}

fn decode_identity(bytes: &[u8]) -> Result<SnapshotBaseIdentity, SnapshotOverlayError> {
    let bytes: &[u8; IDENTITY_BYTES] = bytes
        .try_into()
        .map_err(|_| SnapshotOverlayError::Invalid("identity record width"))?;
    let block_hash = BlockHash::from_byte_array(bytes[4..36].try_into().expect("fixed record"));
    let height = u32::from_le_bytes(bytes[36..40].try_into().expect("fixed record"));
    let hash_serialized = std::str::from_utf8(&bytes[88..152])
        .map_err(|_| SnapshotOverlayError::Invalid("identity hash encoding"))?;
    if !hash_serialized
        .bytes()
        .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        return Err(SnapshotOverlayError::Invalid("identity hash encoding"));
    }
    Ok(SnapshotBaseIdentity {
        height,
        block_hash,
        hash_serialized: hash_serialized.to_owned(),
    })
}

fn encode_tip(tip: ExecutionTip) -> [u8; TIP_BYTES] {
    let mut bytes = [0_u8; TIP_BYTES];
    bytes[..4].copy_from_slice(&tip.height.to_le_bytes());
    bytes[4..].copy_from_slice(&tip.hash.to_byte_array());
    bytes
}

fn decode_tip(bytes: &[u8]) -> Result<ExecutionTip, ChainStoreError> {
    let bytes: &[u8; TIP_BYTES] = bytes
        .try_into()
        .map_err(|_| ChainStoreError::Utxo(UtxoError::Malformed("overlay tip record")))?;
    Ok(ExecutionTip {
        height: u32::from_le_bytes(bytes[..4].try_into().expect("fixed record")),
        hash: BlockHash::from_byte_array(bytes[4..].try_into().expect("fixed record")),
    })
}

#[cfg(test)]
mod tests {
    use bitcoin::{Network, Txid};
    use tempfile::TempDir;

    use super::*;

    const BASE_HEIGHT: u32 = 100;
    const IMPORT_TIME: u64 = 5_000;

    fn key(txid: u8, vout: u32) -> OutPointKey {
        OutPointKey::from(OutPoint::new(Txid::from_byte_array([txid; 32]), vout))
    }

    fn mtp_for(height: u32) -> u32 {
        1_000 + height
    }

    fn base_coin(height: u32, value_sats: u64, script: Vec<u8>) -> Utxo {
        Utxo {
            value_sats,
            height,
            is_coinbase: height == BASE_HEIGHT,
            last_touched: IMPORT_TIME,
            creation_mtp: mtp_for(height),
            script_pubkey: script,
        }
    }

    fn overlay_coin(height: u32, value_sats: u64) -> Utxo {
        Utxo {
            value_sats,
            height,
            is_coinbase: false,
            last_touched: 77,
            creation_mtp: mtp_for(height),
            script_pubkey: vec![0x51, 0xac],
        }
    }

    fn block_hash(height: u32) -> BlockHash {
        let mut bytes = [0_u8; 32];
        bytes[..4].copy_from_slice(&height.to_le_bytes());
        bytes[31] = 0xbb;
        BlockHash::from_byte_array(bytes)
    }

    fn tip(height: u32, hash: BlockHash) -> ExecutionTip {
        ExecutionTip { height, hash }
    }

    /// The four base coins every test starts from, keyed by ascending txid.
    fn base_coins() -> Vec<(u8, u32, Utxo)> {
        vec![
            (
                1,
                0,
                base_coin(BASE_HEIGHT, 5_000_000_000, {
                    [&[0x76, 0xa9, 20][..], &[3_u8; 20], &[0x88, 0xac]].concat()
                }),
            ),
            (1, 256, base_coin(0, 0, vec![0x51, 0xac])),
            (
                2,
                1,
                base_coin(55, 12_345, [&[0xa9, 20][..], &[4_u8; 20], &[0x87]].concat()),
            ),
            (4, 0, base_coin(0, 0, Vec::new())),
        ]
    }

    /// Encodes a canonical snapshot from `coins` and derives its identity.
    fn write_base_snapshot(
        directory: &Path,
        base_height: u32,
        base_hash: BlockHash,
        coins: &[(u8, u32, Utxo)],
    ) -> (PathBuf, PathBuf, SnapshotBaseIdentity) {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(b"utxo\xff");
        bytes.extend_from_slice(&2_u16.to_le_bytes());
        bytes.extend_from_slice(&Network::Regtest.magic().to_bytes());
        bytes.extend_from_slice(&base_hash.to_byte_array());
        bytes.extend_from_slice(&u64::try_from(coins.len()).unwrap().to_le_bytes());

        let mut core_hash = sha256d::Hash::engine();
        let mut txids: Vec<u8> = coins.iter().map(|(txid, ..)| *txid).collect();
        txids.dedup();
        for txid in txids {
            let mut group: Vec<(u32, &Utxo)> = coins
                .iter()
                .filter(|(candidate, ..)| *candidate == txid)
                .map(|(_, vout, utxo)| (*vout, utxo))
                .collect();
            group.sort_unstable_by_key(|(vout, _)| *vout);
            bytes.extend_from_slice(&[txid; 32]);
            write_compact_size(&mut bytes, u64::try_from(group.len()).unwrap());
            for (vout, utxo) in group {
                update_core_utxo_hash(&mut core_hash, key(txid, vout), utxo);
                write_compact_size(&mut bytes, u64::from(vout));
                write_core_varint(
                    &mut bytes,
                    u64::from((utxo.height << 1) | u32::from(utxo.is_coinbase)),
                );
                write_core_varint(&mut bytes, compress_amount(utxo.value_sats));
                compress_script(&mut bytes, &utxo.script_pubkey);
            }
        }
        let identity = SnapshotBaseIdentity {
            height: base_height,
            block_hash: base_hash,
            hash_serialized: sha256d::Hash::from_engine(core_hash).to_string(),
        };
        let snapshot_path = directory.join(format!("utxo-{base_height}.dat"));
        let index_path = directory.join(format!("utxo-{base_height}.rbtcidx"));
        fs::write(&snapshot_path, &bytes).unwrap();
        build_core_snapshot_index_with_identity(&snapshot_path, &index_path, &identity).unwrap();
        (snapshot_path, index_path, identity)
    }

    fn open_store(
        directory: &Path,
        snapshot_path: &Path,
        index_path: &Path,
        identity: &SnapshotBaseIdentity,
        capacity_bytes: u64,
    ) -> Result<SnapshotOverlayChainstate, SnapshotOverlayError> {
        SnapshotOverlayChainstate::open(
            SnapshotOverlayConfig {
                database_dir: directory.join("overlay-mdbx"),
                snapshot_path: snapshot_path.to_owned(),
                index_path: index_path.to_owned(),
                capacity_bytes,
                import_time: IMPORT_TIME,
                mtp_by_height: (0..=identity.height).map(mtp_for).collect(),
            },
            Some(identity),
        )
    }

    fn setup(capacity_bytes: u64) -> (TempDir, SnapshotOverlayChainstate, SnapshotBaseIdentity) {
        let directory = TempDir::new().unwrap();
        let (snapshot_path, index_path, identity) = write_base_snapshot(
            directory.path(),
            BASE_HEIGHT,
            block_hash(BASE_HEIGHT),
            &base_coins(),
        );
        let store = open_store(
            directory.path(),
            &snapshot_path,
            &index_path,
            &identity,
            capacity_bytes,
        )
        .unwrap();
        (directory, store, identity)
    }

    #[test]
    fn resolves_base_coins_with_creation_mtp_and_import_time() {
        let (_directory, store, identity) = setup(32 << 20);
        assert_eq!(
            store.execution_tip().unwrap(),
            tip(BASE_HEIGHT, identity.block_hash)
        );
        assert_eq!(
            store.assumed_snapshot_base().unwrap(),
            Some(tip(BASE_HEIGHT, identity.block_hash))
        );
        for (txid, vout, coin) in base_coins() {
            assert_eq!(store.get(key(txid, vout)).unwrap().as_ref(), Some(&coin));
        }
        assert_eq!(store.get(key(1, 7)).unwrap(), None);
        assert_eq!(store.tier_stats().unwrap(), TierStats { hot: 0, cold: 4 });
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn connects_disconnects_the_full_mutation_matrix() {
        let (_directory, store, identity) = setup(32 << 20);
        let base_a = store.get(key(1, 0)).unwrap().unwrap();
        let base_b = store.get(key(1, 256)).unwrap().unwrap();

        // Block 101: spend base coin A, create overlay coins D and E.
        let d = overlay_coin(101, 111);
        let e = overlay_coin(101, 222);
        store
            .commit_connect(
                identity.block_hash,
                tip(101, block_hash(101)),
                &[key(1, 0)],
                &[(key(9, 0), d.clone()), (key(9, 1), e.clone())],
                &[UtxoUndo::from_parts(
                    vec![(key(1, 0), base_a.clone())],
                    vec![key(9, 0), key(9, 1)],
                )],
            )
            .unwrap();
        assert_eq!(store.get(key(1, 0)).unwrap(), None);
        assert_eq!(store.get(key(9, 0)).unwrap().as_ref(), Some(&d));
        assert_eq!(store.tier_stats().unwrap(), TierStats { hot: 2, cold: 3 });

        // A stale parent is rejected by the same-transaction tip CAS.
        let stale = store.commit_connect(
            identity.block_hash,
            tip(101, block_hash(101)),
            &[],
            &[],
            &[],
        );
        assert!(matches!(
            stale.unwrap_err(),
            ChainStoreError::Execution(ExecutionStoreError::NonSequential { .. })
        ));

        // Block 102: spend overlay coin D and base coin B, create F.
        let f = overlay_coin(102, 333);
        store
            .commit_connect(
                block_hash(101),
                tip(102, block_hash(102)),
                &[key(9, 0), key(1, 256)],
                &[(key(11, 0), f.clone())],
                &[UtxoUndo::from_parts(
                    vec![(key(9, 0), d.clone()), (key(1, 256), base_b.clone())],
                    vec![key(11, 0)],
                )],
            )
            .unwrap();
        assert_eq!(store.get(key(9, 0)).unwrap(), None);
        assert_eq!(store.get(key(1, 256)).unwrap(), None);
        assert_eq!(store.get(key(11, 0)).unwrap().as_ref(), Some(&f));

        // Caller-ordered batch reads across all three layers.
        let many = store
            .get_many(&[key(11, 0), key(1, 0), key(2, 1), key(9, 1), key(8, 8)])
            .unwrap();
        assert_eq!(many[0].1.as_ref(), Some(&f));
        assert_eq!(many[1].1, None);
        assert!(many[2].1.is_some());
        assert_eq!(many[3].1.as_ref(), Some(&e));
        assert_eq!(many[4].1, None);

        // Spending a coin twice or recreating a visible coin fails closed.
        assert!(matches!(
            store
                .commit_connect(
                    block_hash(102),
                    tip(103, block_hash(103)),
                    &[key(9, 0)],
                    &[],
                    &[],
                )
                .unwrap_err(),
            ChainStoreError::Utxo(UtxoError::Missing(_))
        ));
        assert!(matches!(
            store
                .commit_connect(
                    block_hash(102),
                    tip(103, block_hash(103)),
                    &[],
                    &[(key(2, 1), overlay_coin(103, 1))],
                    &[],
                )
                .unwrap_err(),
            ChainStoreError::Utxo(UtxoError::Duplicate(_))
        ));

        // Disconnect 102: restore overlay coin D and base coin B exactly.
        assert!(store.block_undo(block_hash(102)).unwrap().is_some());
        store
            .commit_disconnect(
                tip(102, block_hash(102)),
                tip(101, block_hash(101)),
                &[key(11, 0)],
                &[(key(9, 0), d.clone()), (key(1, 256), base_b.clone())],
                &[],
            )
            .unwrap();
        assert_eq!(store.get(key(9, 0)).unwrap().as_ref(), Some(&d));
        assert_eq!(store.get(key(1, 256)).unwrap().as_ref(), Some(&base_b));
        assert_eq!(store.get(key(11, 0)).unwrap(), None);
        assert!(store.block_undo(block_hash(102)).unwrap().is_none());

        // Disconnect 101, returning exactly to the base state.
        store
            .commit_disconnect(
                tip(101, block_hash(101)),
                tip(BASE_HEIGHT, identity.block_hash),
                &[key(9, 0), key(9, 1)],
                &[(key(1, 0), base_a.clone())],
                &[],
            )
            .unwrap();
        assert_eq!(store.get(key(1, 0)).unwrap().as_ref(), Some(&base_a));
        assert_eq!(store.tier_stats().unwrap(), TierStats { hot: 0, cold: 4 });

        // The assumed snapshot base cannot be disconnected.
        assert!(
            store
                .commit_disconnect(
                    tip(BASE_HEIGHT, identity.block_hash),
                    tip(BASE_HEIGHT - 1, block_hash(BASE_HEIGHT - 1)),
                    &[],
                    &[],
                    &[],
                )
                .is_err()
        );
    }

    #[test]
    fn batch_commits_are_atomic_and_survive_reopen() {
        let directory = TempDir::new().unwrap();
        let (snapshot_path, index_path, identity) = write_base_snapshot(
            directory.path(),
            BASE_HEIGHT,
            block_hash(BASE_HEIGHT),
            &base_coins(),
        );
        let store = open_store(
            directory.path(),
            &snapshot_path,
            &index_path,
            &identity,
            32 << 20,
        )
        .unwrap();

        let x = overlay_coin(101, 1);
        let y = overlay_coin(102, 2);
        store
            .commit_connect_batch(&[
                ConnectTransition {
                    expected_parent: identity.block_hash,
                    next: tip(101, block_hash(101)),
                    spent: Vec::new(),
                    created: vec![(key(20, 0), x.clone())],
                    transaction_undos: Vec::new(),
                },
                ConnectTransition {
                    expected_parent: block_hash(101),
                    next: tip(102, block_hash(102)),
                    spent: vec![key(20, 0)],
                    created: vec![(key(21, 0), y.clone())],
                    transaction_undos: Vec::new(),
                },
            ])
            .unwrap();
        assert_eq!(store.get(key(20, 0)).unwrap(), None);
        assert_eq!(store.get(key(21, 0)).unwrap().as_ref(), Some(&y));

        // A broken linkage in the middle rejects the whole batch atomically.
        let error = store
            .commit_connect_batch(&[
                ConnectTransition {
                    expected_parent: block_hash(102),
                    next: tip(103, block_hash(103)),
                    spent: Vec::new(),
                    created: vec![(key(22, 0), overlay_coin(103, 3))],
                    transaction_undos: Vec::new(),
                },
                ConnectTransition {
                    expected_parent: block_hash(999),
                    next: tip(104, block_hash(104)),
                    spent: Vec::new(),
                    created: vec![(key(23, 0), overlay_coin(104, 4))],
                    transaction_undos: Vec::new(),
                },
            ])
            .unwrap_err();
        assert!(matches!(
            error,
            ChainStoreError::Execution(ExecutionStoreError::NonSequential { .. })
        ));
        assert_eq!(store.execution_tip().unwrap(), tip(102, block_hash(102)));
        assert_eq!(store.get(key(22, 0)).unwrap(), None);
        drop(store);

        let reopened = open_store(
            directory.path(),
            &snapshot_path,
            &index_path,
            &identity,
            32 << 20,
        )
        .unwrap();
        assert_eq!(reopened.execution_tip().unwrap(), tip(102, block_hash(102)));
        assert_eq!(reopened.get(key(21, 0)).unwrap().as_ref(), Some(&y));

        // The environment refuses a different claimed base identity.
        drop(reopened);
        let mut other = identity.clone();
        other.hash_serialized = "0".repeat(64);
        let Err(error) = open_store(
            directory.path(),
            &snapshot_path,
            &index_path,
            &other,
            32 << 20,
        ) else {
            panic!("a different claimed base identity must be rejected");
        };
        assert!(matches!(error, SnapshotOverlayError::Invalid(_)));
    }

    #[test]
    fn hard_capacity_ceiling_fails_closed() {
        let (_directory, store, identity) = setup(1 << 20);
        let mut parent = identity.block_hash;
        let mut height = BASE_HEIGHT;
        let mut capacity_error = None;
        for round in 0..64_u32 {
            let next_height = height + 1;
            let created: Vec<(OutPointKey, Utxo)> = (0..8_u32)
                .map(|index| {
                    (
                        key(u8::try_from(30 + round).unwrap(), index),
                        Utxo {
                            script_pubkey: vec![0x6a; 9_500],
                            ..overlay_coin(next_height, 1)
                        },
                    )
                })
                .collect();
            match store.commit_connect(
                parent,
                tip(next_height, block_hash(next_height)),
                &[],
                &created,
                &[],
            ) {
                Ok(_) => {
                    parent = block_hash(next_height);
                    height = next_height;
                }
                Err(error) => {
                    capacity_error = Some(error);
                    break;
                }
            }
        }
        let error = capacity_error.expect("1 MiB geometry must fill within 64 blocks");
        assert!(SnapshotOverlayChainstate::is_capacity_exhausted(&error));
        assert!(store.needs_rebase(50).unwrap());
        // The failed transaction aborted cleanly: the tip is still consistent.
        assert_eq!(store.execution_tip().unwrap(), tip(height, parent));
        let usage = store.capacity().unwrap();
        assert!(usage.used_bytes <= usage.capacity_bytes);
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn rebase_folds_overlay_and_tombstones_into_a_new_base() {
        let directory = TempDir::new().unwrap();
        let (snapshot_path, index_path, identity) = write_base_snapshot(
            directory.path(),
            BASE_HEIGHT,
            block_hash(BASE_HEIGHT),
            &base_coins(),
        );
        let mut store = open_store(
            directory.path(),
            &snapshot_path,
            &index_path,
            &identity,
            32 << 20,
        )
        .unwrap();
        let base_a = store.get(key(1, 0)).unwrap().unwrap();

        // Block 101 spends base coin A and creates D + E; block 102 spends D
        // and creates F. Rebase folds {E, F} and drops A.
        let d = overlay_coin(101, 111);
        let e = overlay_coin(101, 222);
        let f = overlay_coin(102, 333);
        store
            .commit_connect(
                identity.block_hash,
                tip(101, block_hash(101)),
                &[key(1, 0)],
                &[(key(9, 0), d.clone()), (key(9, 1), e.clone())],
                &[UtxoUndo::from_parts(
                    vec![(key(1, 0), base_a)],
                    vec![key(9, 0), key(9, 1)],
                )],
            )
            .unwrap();
        store
            .commit_connect(
                block_hash(101),
                tip(102, block_hash(102)),
                &[key(9, 0)],
                &[(key(11, 0), f.clone())],
                &[],
            )
            .unwrap();

        // A wrong-length or wrong-valued MTP extension is rejected up front.
        let new_snapshot = directory.path().join("utxo-102.dat");
        let new_index = directory.path().join("utxo-102.rbtcidx");
        assert!(matches!(
            store
                .rebase_into(&new_snapshot, &new_index, &[mtp_for(101)])
                .unwrap_err(),
            SnapshotOverlayError::Invalid(_)
        ));
        assert!(matches!(
            store
                .rebase_into(&new_snapshot, &new_index, &[mtp_for(101), 9])
                .unwrap_err(),
            SnapshotOverlayError::Invalid("MTP extension disagrees with a folded overlay coin")
        ));
        assert!(!new_snapshot.exists());

        let report = store
            .rebase_into(&new_snapshot, &new_index, &[mtp_for(101), mtp_for(102)])
            .unwrap();
        assert_eq!(report.coins, 5);
        assert_eq!(report.folded_overlay, 2);
        assert_eq!(report.dropped_tombstones, 1);
        assert_eq!(report.identity.height, 102);
        assert_eq!(report.identity.block_hash, block_hash(102));

        // The folded coins now come from the new immutable base, with their
        // consensus fields intact and tier metadata reset to import policy.
        assert_eq!(store.get(key(1, 0)).unwrap(), None);
        let folded_e = store.get(key(9, 1)).unwrap().unwrap();
        assert_eq!(folded_e.value_sats, e.value_sats);
        assert_eq!(folded_e.height, e.height);
        assert_eq!(folded_e.creation_mtp, e.creation_mtp);
        assert_eq!(folded_e.script_pubkey, e.script_pubkey);
        assert_eq!(folded_e.last_touched, IMPORT_TIME);
        assert!(store.get(key(11, 0)).unwrap().is_some());
        assert_eq!(store.tier_stats().unwrap(), TierStats { hot: 0, cold: 5 });
        assert!(store.block_undo(block_hash(101)).unwrap().is_none());
        assert_eq!(
            store.assumed_snapshot_base().unwrap(),
            Some(tip(102, block_hash(102)))
        );
        assert!(!d.script_pubkey.is_empty());

        // Execution continues above the new base: spending a folded coin now
        // writes a tombstone against the new snapshot.
        store
            .commit_connect(
                block_hash(102),
                tip(103, block_hash(103)),
                &[key(9, 1)],
                &[],
                &[],
            )
            .unwrap();
        assert_eq!(store.get(key(9, 1)).unwrap(), None);
        drop(store);

        // Reopen against the new base files and identity.
        let reopened = SnapshotOverlayChainstate::open(
            SnapshotOverlayConfig {
                database_dir: directory.path().join("overlay-mdbx"),
                snapshot_path: new_snapshot.clone(),
                index_path: new_index.clone(),
                capacity_bytes: 32 << 20,
                import_time: IMPORT_TIME,
                mtp_by_height: (0..=102).map(mtp_for).collect(),
            },
            None,
        )
        .unwrap();
        assert_eq!(reopened.base_identity(), &report.identity);
        assert_eq!(reopened.execution_tip().unwrap(), tip(103, block_hash(103)));
        assert_eq!(reopened.get(key(9, 1)).unwrap(), None);
        assert!(reopened.get(key(11, 0)).unwrap().is_some());
        drop(reopened);

        // The old base files no longer match the environment's binding.
        assert!(
            open_store(
                directory.path(),
                &snapshot_path,
                &index_path,
                &identity,
                32 << 20,
            )
            .is_err()
        );
    }
}
