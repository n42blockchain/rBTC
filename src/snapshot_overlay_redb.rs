//! Snapshot-backed chainstate on redb: an immutable compressed base plus one
//! size-bounded, actively compacted redb overlay.
//!
//! This is the redb counterpart of [`crate::snapshot_overlay`], built to the
//! same contract — the same immutable snapshot base served through its
//! minimal-perfect-hash index, the same four logical tables (`utxo_overlay`,
//! `utxo_spent_base`, `block_undos`, `meta`), the same one-transaction-per-block
//! atomicity, and the same `overlay → tombstone → base` read order — so the
//! two engines can be compared on identical work.
//!
//! Two differences are inherent to the engines and are the point of the
//! comparison:
//!
//! - **The budget is policy-enforced, not engine-enforced.** redb has no
//!   geometry ceiling, so nothing aborts a commit that would exceed the
//!   budget; this store measures the file after each commit instead. A single
//!   batch can therefore overshoot before the next check sees it, whereas the
//!   MDBX store's `MDBX_MAP_FULL` refuses the offending commit outright.
//! - **Space is reclaimed in place.** redb's `compact()` genuinely shrinks the
//!   file, which `libmdbx-rs` 0.6.6 exposes no equivalent for — there, a
//!   rebase must recreate the environment file to reset `last_pgno`. Periodic
//!   compaction here is expected to postpone rebases rather than replace them:
//!   compaction reclaims space freed by spends and tombstoned base coins, but
//!   cannot shrink the overlay's live working set, which only folding into a
//!   new base removes.

use std::{
    fs,
    path::{Path, PathBuf},
    sync::{Mutex, MutexGuard},
};

use bitcoin::{
    BlockHash, OutPoint,
    hashes::{Hash as _, sha256d},
};
use redb::{Database, Durability, ReadableTable as _, ReadableTableMetadata as _, TableDefinition};

use crate::{
    chain_store::{ChainStoreError, ConnectTransition, ExecutionChainStore},
    core_snapshot::{
        compress_amount, compress_script, update_core_utxo_hash, write_compact_size,
        write_core_varint,
    },
    core_snapshot_index::{
        CoreSnapshotUtxoIndex, SnapshotBaseIdentity, build_core_snapshot_index_with_identity,
    },
    execution_store::{ExecutionStoreError, ExecutionTip},
    headers::HeaderDag,
    snapshot_overlay::{
        BaseGroupReader, META_IDENTITY, META_TIP, OverlayCapacity, RebaseReport, RemoveFilesOnDrop,
        SnapshotOverlayConfig, SnapshotOverlayError, chain_store_to_utxo, decode_identity,
        decode_tip, encode_identity, encode_tip, index_read_error,
    },
    snapshot_overlay::{compress_block_undo, decompress_block_undo},
    utxo::{OutPointKey, TierStats, Utxo, UtxoError, UtxoStore, UtxoUndo},
};

const OVERLAY: TableDefinition<&[u8], &[u8]> = TableDefinition::new("utxo_overlay");
const TOMBSTONE: TableDefinition<&[u8], ()> = TableDefinition::new("utxo_spent_base");
const UNDO: TableDefinition<&[u8], &[u8]> = TableDefinition::new("block_undos");
const META: TableDefinition<&[u8], &[u8]> = TableDefinition::new("meta");

/// Outcome of one active-compaction pass.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CompactionReport {
    /// Whether redb reported that it moved any pages.
    pub reclaimed: bool,
    /// File size before compaction.
    pub before_bytes: u64,
    /// File size after compaction.
    pub after_bytes: u64,
}

impl CompactionReport {
    /// Returns bytes released, saturating at zero.
    #[must_use]
    pub const fn released_bytes(&self) -> u64 {
        self.before_bytes.saturating_sub(self.after_bytes)
    }
}

/// Snapshot-backed chainstate with one size-bounded redb overlay database.
pub struct SnapshotOverlayRedbChainstate {
    db: Database,
    database_path: PathBuf,
    base: CoreSnapshotUtxoIndex,
    identity: SnapshotBaseIdentity,
    mtp_by_height: Vec<u32>,
    import_time: u64,
    capacity_bytes: u64,
    snapshot_path: PathBuf,
    index_path: PathBuf,
    write_guard: Mutex<()>,
}

impl SnapshotOverlayRedbChainstate {
    /// Opens the overlay database and binds it to the snapshot base.
    ///
    /// `config.database_dir` names the redb file itself rather than a
    /// directory, matching redb's single-file layout.
    ///
    /// # Errors
    ///
    /// Fails closed on I/O or redb errors, an index/snapshot mismatch, an
    /// identity mismatch, or an MTP table that does not cover the base.
    pub fn open(
        config: SnapshotOverlayConfig,
        identity: Option<&SnapshotBaseIdentity>,
    ) -> Result<Self, SnapshotOverlayError> {
        let base = CoreSnapshotUtxoIndex::open(&config.index_path, &config.snapshot_path)?;
        if let Some(parent) = config.database_dir.parent() {
            fs::create_dir_all(parent)?;
        }
        let db = Database::create(&config.database_dir).map_err(overlay_redb)?;
        let identity = {
            let transaction = db.begin_write().map_err(overlay_redb)?;
            {
                // Materialize every table so later read transactions never
                // see a missing-table error.
                let _ = transaction.open_table(OVERLAY).map_err(overlay_redb)?;
                let _ = transaction.open_table(TOMBSTONE).map_err(overlay_redb)?;
                let _ = transaction.open_table(UNDO).map_err(overlay_redb)?;
            }
            let effective = {
                let mut meta = transaction.open_table(META).map_err(overlay_redb)?;
                let stored = meta
                    .get(META_IDENTITY)
                    .map_err(overlay_redb)?
                    .map(|value| value.value().to_vec());
                match (stored, identity) {
                    (None, None) => {
                        return Err(SnapshotOverlayError::Invalid(
                            "a fresh overlay requires an explicit base identity",
                        ));
                    }
                    (None, Some(identity)) => {
                        meta.insert(META_IDENTITY, encode_identity(identity, &base)?.as_slice())
                            .map_err(overlay_redb)?;
                        meta.insert(
                            META_TIP,
                            encode_tip(ExecutionTip {
                                height: identity.height,
                                hash: identity.block_hash,
                            })
                            .as_slice(),
                        )
                        .map_err(overlay_redb)?;
                        identity.clone()
                    }
                    (Some(stored), supplied) => {
                        let decoded = decode_identity(&stored)?;
                        if let Some(supplied) = supplied {
                            if *supplied != decoded {
                                return Err(SnapshotOverlayError::Invalid(
                                    "overlay is bound to a different snapshot base",
                                ));
                            }
                        }
                        // Re-encoding against the opened index verifies the
                        // supplied files still carry the bound network, coin
                        // count, byte length, and content digest.
                        if encode_identity(&decoded, &base)? != stored {
                            return Err(SnapshotOverlayError::Invalid(
                                "snapshot files do not match the bound base identity",
                            ));
                        }
                        decoded
                    }
                }
            };
            transaction.commit().map_err(overlay_redb)?;
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
            database_path: config.database_dir,
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

    /// Returns the base identity a previously bound overlay stores, or `None`
    /// when `database_path` has no overlay database yet.
    ///
    /// # Errors
    ///
    /// Fails on I/O or redb errors, or a malformed identity record.
    pub fn stored_identity(
        database_path: &Path,
    ) -> Result<Option<SnapshotBaseIdentity>, SnapshotOverlayError> {
        if !database_path.exists() {
            return Ok(None);
        }
        let db = Database::open(database_path).map_err(overlay_redb)?;
        let transaction = db.begin_read().map_err(overlay_redb)?;
        let meta = match transaction.open_table(META) {
            Ok(meta) => meta,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
            Err(error) => return Err(overlay_redb(error)),
        };
        meta.get(META_IDENTITY)
            .map_err(overlay_redb)?
            .map(|stored| decode_identity(stored.value()))
            .transpose()
    }

    fn lock(&self) -> MutexGuard<'_, ()> {
        self.write_guard
            .lock()
            .expect("overlay write lock not poisoned")
    }

    /// Returns the immutable base identity currently backing this store.
    #[must_use]
    pub const fn base_identity(&self) -> &SnapshotBaseIdentity {
        &self.identity
    }

    /// Reports on-disk usage against the configured budget.
    ///
    /// Unlike the MDBX store, this is a plain file-size measurement taken
    /// after the fact: redb has no geometry ceiling to refuse an oversized
    /// commit, so this is what the budget is enforced against by policy.
    ///
    /// # Errors
    ///
    /// Fails when the database file cannot be measured.
    pub fn capacity(&self) -> Result<OverlayCapacity, SnapshotOverlayError> {
        Ok(OverlayCapacity {
            used_bytes: fs::metadata(&self.database_path)?.len(),
            capacity_bytes: self.capacity_bytes,
        })
    }

    /// Returns whether usage has reached `threshold_percent`.
    ///
    /// # Errors
    ///
    /// Fails when the database file cannot be measured.
    pub fn needs_rebase(&self, threshold_percent: u8) -> Result<bool, SnapshotOverlayError> {
        Ok(self.capacity()?.used_percent() >= threshold_percent)
    }

    /// Compacts the overlay database in place, reclaiming space freed by
    /// spends and by base coins whose tombstones were folded away.
    ///
    /// This is redb's native compaction; it rewrites the file and genuinely
    /// shrinks it, which is what this engine offers over the MDBX store's
    /// need to recreate its environment file. It requires exclusive access
    /// and no live read transaction, which `&mut self` and the write lock
    /// together provide.
    ///
    /// # Errors
    ///
    /// Fails on redb compaction errors or when the file cannot be measured.
    pub fn compact(&mut self) -> Result<CompactionReport, SnapshotOverlayError> {
        let before_bytes = fs::metadata(&self.database_path)?.len();
        let reclaimed = {
            let _guard = self.write_guard.lock().expect("write lock not poisoned");
            self.db.compact().map_err(overlay_redb)?
        };
        Ok(CompactionReport {
            reclaimed,
            before_bytes,
            after_bytes: fs::metadata(&self.database_path)?.len(),
        })
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

    /// Applies one connect mutation inside the caller's transaction.
    fn connect_mutation(
        &self,
        transaction: &redb::WriteTransaction,
        spent: &[OutPointKey],
        created: &[(OutPointKey, Utxo)],
    ) -> Result<UtxoUndo, ChainStoreError> {
        let mut overlay = transaction.open_table(OVERLAY)?;
        let mut tombstone = transaction.open_table(TOMBSTONE)?;
        let mut seen_spent = std::collections::BTreeSet::new();
        let mut undo_spent = Vec::with_capacity(spent.len());
        for key in spent {
            if !seen_spent.insert(*key) {
                return Err(ChainStoreError::Utxo(UtxoError::DuplicateSpend(*key)));
            }
            if let Some(value) = overlay.remove(key.as_bytes().as_slice())? {
                undo_spent.push((*key, Utxo::decode(value.value())?));
                continue;
            }
            if tombstone.get(key.as_bytes().as_slice())?.is_some() {
                return Err(ChainStoreError::Utxo(UtxoError::Missing(*key)));
            }
            let coin = self
                .base_utxo(key.to_outpoint())?
                .ok_or(ChainStoreError::Utxo(UtxoError::Missing(*key)))?;
            tombstone.insert(key.as_bytes().as_slice(), ())?;
            undo_spent.push((*key, coin));
        }
        let mut seen_created = std::collections::BTreeSet::new();
        for (key, utxo) in created {
            if !seen_created.insert(*key) {
                return Err(ChainStoreError::Utxo(UtxoError::Duplicate(*key)));
            }
            if !seen_spent.contains(key) {
                if overlay.get(key.as_bytes().as_slice())?.is_some() {
                    return Err(ChainStoreError::Utxo(UtxoError::Duplicate(*key)));
                }
                let base_hidden = tombstone.get(key.as_bytes().as_slice())?.is_some();
                if !base_hidden && self.base_utxo(key.to_outpoint())?.is_some() {
                    return Err(ChainStoreError::Utxo(UtxoError::Duplicate(*key)));
                }
            }
            overlay.insert(key.as_bytes().as_slice(), utxo.encode()?.as_slice())?;
        }
        Ok(UtxoUndo::from_parts(
            undo_spent,
            created.iter().map(|(key, _)| *key).collect(),
        ))
    }

    /// Applies one disconnect mutation, returning the exact pre-image of
    /// every removed key so a caller can build a correct redo `UtxoUndo`.
    ///
    /// Mirrors [`crate::snapshot_overlay`]'s handling exactly: a removed key
    /// is not necessarily overlay-resident, since an earlier disconnect may
    /// have un-tombstoned a base coin, and removing that again means
    /// re-tombstoning it.
    fn disconnect_mutation(
        &self,
        transaction: &redb::WriteTransaction,
        removed: &[OutPointKey],
        restored: &[(OutPointKey, Utxo)],
    ) -> Result<Vec<(OutPointKey, Utxo)>, ChainStoreError> {
        let mut overlay = transaction.open_table(OVERLAY)?;
        let mut tombstone = transaction.open_table(TOMBSTONE)?;
        let mut removed_values = Vec::with_capacity(removed.len());
        for key in removed {
            if let Some(value) = overlay.remove(key.as_bytes().as_slice())? {
                removed_values.push((*key, Utxo::decode(value.value())?));
                continue;
            }
            if tombstone.get(key.as_bytes().as_slice())?.is_some() {
                return Err(ChainStoreError::Utxo(UtxoError::Missing(*key)));
            }
            let coin = self
                .base_utxo(key.to_outpoint())?
                .ok_or(ChainStoreError::Utxo(UtxoError::Missing(*key)))?;
            tombstone.insert(key.as_bytes().as_slice(), ())?;
            removed_values.push((*key, coin));
        }
        for (key, utxo) in restored {
            if utxo.height > self.identity.height {
                if overlay.get(key.as_bytes().as_slice())?.is_some() {
                    return Err(ChainStoreError::Utxo(UtxoError::Duplicate(*key)));
                }
                overlay.insert(key.as_bytes().as_slice(), utxo.encode()?.as_slice())?;
            } else if tombstone.remove(key.as_bytes().as_slice())?.is_none() {
                return Err(ChainStoreError::Utxo(UtxoError::Duplicate(*key)));
            }
        }
        Ok(removed_values)
    }

    fn read_tip(transaction: &redb::WriteTransaction) -> Result<ExecutionTip, ChainStoreError> {
        let meta = transaction.open_table(META)?;
        let stored = meta
            .get(META_TIP)?
            .ok_or(ChainStoreError::Utxo(UtxoError::Malformed(
                "missing overlay tip",
            )))?;
        decode_tip(stored.value())
    }

    fn advance_tip(
        transaction: &redb::WriteTransaction,
        expected_parent: BlockHash,
        next: ExecutionTip,
    ) -> Result<(), ChainStoreError> {
        let current = Self::read_tip(transaction)?;
        if current.hash != expected_parent || current.height.checked_add(1) != Some(next.height) {
            return Err(ChainStoreError::Execution(
                ExecutionStoreError::NonSequential {
                    current_height: current.height,
                    current_hash: current.hash,
                },
            ));
        }
        let mut meta = transaction.open_table(META)?;
        meta.insert(META_TIP, encode_tip(next).as_slice())?;
        Ok(())
    }

    fn begin_durable_write(&self) -> Result<redb::WriteTransaction, ChainStoreError> {
        let mut transaction = self.db.begin_write()?;
        transaction.set_durability(Durability::Immediate);
        Ok(transaction)
    }

    /// Folds the base snapshot and the overlay into a fresh compressed
    /// snapshot at the current tip, rebuilds the access index, then clears
    /// and compacts the overlay.
    ///
    /// Unlike the MDBX store, no environment file has to be recreated:
    /// clearing the tables and compacting genuinely returns the space, which
    /// is the property this engine is being evaluated for.
    ///
    /// # Errors
    ///
    /// Fails closed without switching identity on I/O errors, existing output
    /// paths, an MTP extension of the wrong length, or a merge mismatch.
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
        let transaction = self.db.begin_read().map_err(overlay_redb)?;
        let meta = transaction.open_table(META).map_err(overlay_redb)?;
        let tip = decode_tip(
            meta.get(META_TIP)
                .map_err(overlay_redb)?
                .ok_or(SnapshotOverlayError::Invalid("missing overlay tip"))?
                .value(),
        )?;
        let expected_extension = tip
            .height
            .checked_sub(self.identity.height)
            .ok_or(SnapshotOverlayError::Invalid("tip below base"))?;
        if mtp_extension.len() != usize::try_from(expected_extension).expect("height fits usize") {
            return Err(SnapshotOverlayError::Invalid(
                "MTP extension must cover exactly heights base+1..=tip",
            ));
        }
        let overlay = transaction.open_table(OVERLAY).map_err(overlay_redb)?;
        let tombstone = transaction.open_table(TOMBSTONE).map_err(overlay_redb)?;
        let folded_overlay = overlay.len().map_err(overlay_redb)?;
        let dropped_tombstones = tombstone.len().map_err(overlay_redb)?;
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

        let mut cleanup = RemoveFilesOnDrop::default();
        let temporary = new_snapshot_path.with_file_name({
            let mut name = new_snapshot_path
                .file_name()
                .ok_or(SnapshotOverlayError::Invalid("rebase snapshot file name"))?
                .to_owned();
            name.push(".tmp");
            name
        });
        cleanup.track(temporary.clone());
        let mut writer = std::io::BufWriter::new(fs::File::create(&temporary)?);
        let mut header = Vec::with_capacity(51);
        header.extend_from_slice(b"utxo\xff");
        header.extend_from_slice(&2_u16.to_le_bytes());
        header.extend_from_slice(&self.base.network().magic().to_bytes());
        header.extend_from_slice(&tip.hash.to_byte_array());
        header.extend_from_slice(&coins.to_le_bytes());
        std::io::Write::write_all(&mut writer, &header)?;

        let mut core_hash = sha256d::Hash::engine();
        let mut written = 0_u64;
        let mut base_groups = BaseGroupReader::new(&self.snapshot_path)?;
        let mut overlay_rows = OverlayGroupReader::new(&overlay)?;
        let mut base_group = base_groups.next_group()?;
        let mut overlay_group = overlay_rows.next_group()?;
        loop {
            let take = match (&base_group, &overlay_group) {
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
            if take.0 {
                let base = base_group.take().expect("selected above");
                txid = base.0;
                for (vout, utxo) in base.1 {
                    let mut key = [0_u8; 36];
                    key[..32].copy_from_slice(&txid);
                    key[32..].copy_from_slice(&vout.to_le_bytes());
                    if tombstone
                        .get(key.as_slice())
                        .map_err(overlay_redb)?
                        .is_none()
                    {
                        coins_in_group.push((vout, utxo));
                    }
                }
                base_group = base_groups.next_group()?;
            }
            if take.1 {
                let (new_txid, mut new_coins) = overlay_group.take().expect("selected above");
                txid = new_txid;
                coins_in_group.append(&mut new_coins);
                overlay_group = overlay_rows.next_group()?;
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
            std::io::Write::write_all(&mut writer, &group_bytes)?;
            written += u64::try_from(coins_in_group.len()).expect("group size fits u64");
        }
        if written != coins {
            return Err(SnapshotOverlayError::Invalid(
                "materialized coin count mismatch",
            ));
        }
        std::io::Write::flush(&mut writer)?;
        writer.get_ref().sync_all()?;
        drop(writer);
        drop(overlay);
        drop(tombstone);
        drop(meta);
        drop(transaction);
        fs::rename(&temporary, new_snapshot_path)?;
        cleanup.replace(new_snapshot_path.to_owned());
        // Windows has no portable directory fsync; see `diagnostics::sync_directory`.
        #[cfg(unix)]
        if let Some(parent) = new_snapshot_path.parent() {
            fs::File::open(parent)?.sync_all()?;
        }

        let identity = SnapshotBaseIdentity {
            height: tip.height,
            block_hash: tip.hash,
            hash_serialized: sha256d::Hash::from_engine(core_hash).to_string(),
        };
        let report =
            build_core_snapshot_index_with_identity(new_snapshot_path, new_index_path, &identity)?;
        cleanup.track(new_index_path.to_owned());
        let new_base = CoreSnapshotUtxoIndex::open(new_index_path, new_snapshot_path)?;

        let identity_bytes = encode_identity(&identity, &new_base)?;
        {
            let write = self.db.begin_write().map_err(overlay_redb)?;
            // Dropping each table outright is far cheaper than walking every
            // row out of it: `retain` with an always-false predicate removes
            // entries one at a time, rewriting B-tree pages as it goes, which
            // for a multi-million-row overlay is exactly the write
            // amplification a rebase is trying to escape. Reopening in the
            // same transaction recreates them empty, matching what the
            // fresh-open path materializes.
            write.delete_table(OVERLAY).map_err(overlay_redb)?;
            write.delete_table(TOMBSTONE).map_err(overlay_redb)?;
            write.delete_table(UNDO).map_err(overlay_redb)?;
            let _ = write.open_table(OVERLAY).map_err(overlay_redb)?;
            let _ = write.open_table(TOMBSTONE).map_err(overlay_redb)?;
            let _ = write.open_table(UNDO).map_err(overlay_redb)?;
            let mut meta = write.open_table(META).map_err(overlay_redb)?;
            meta.insert(META_IDENTITY, identity_bytes.as_slice())
                .map_err(overlay_redb)?;
            meta.insert(
                META_TIP,
                encode_tip(ExecutionTip {
                    height: identity.height,
                    hash: identity.block_hash,
                })
                .as_slice(),
            )
            .map_err(overlay_redb)?;
            drop(meta);
            write.commit().map_err(overlay_redb)?;
        }
        // Reclaim the space the cleared tables just freed, so the budget is
        // genuinely available again rather than merely marked reusable.
        self.db.compact().map_err(overlay_redb)?;

        self.mtp_by_height.extend_from_slice(mtp_extension);
        self.base = new_base;
        self.identity = identity.clone();
        new_snapshot_path.clone_into(&mut self.snapshot_path);
        new_index_path.clone_into(&mut self.index_path);
        cleanup.disarm();
        Ok(RebaseReport {
            identity,
            coins,
            snapshot_bytes: report.snapshot_bytes,
            index_bytes: report.index_bytes,
            folded_overlay,
            dropped_tombstones,
        })
    }
}

/// Number of overlay rows read per page while materializing a rebase.
///
/// The merge only ever needs the next key in order, so the whole table never
/// has to be resident. Paging keeps peak memory flat in the overlay's size —
/// at a 10 GiB budget it can hold several million coins, which materialized
/// in full would cost hundreds of megabytes on top of the engine's own cache.
const OVERLAY_PAGE_ROWS: usize = 16_384;

/// Groups the overlay table's key-ordered rows by txid, reading a bounded
/// page at a time rather than materializing the whole table.
struct OverlayGroupReader<'txn> {
    table: &'txn redb::ReadOnlyTable<&'static [u8], &'static [u8]>,
    page: std::vec::IntoIter<([u8; 36], Utxo)>,
    /// Last key returned, so the next page resumes strictly after it.
    resume_after: Option<[u8; 36]>,
    exhausted: bool,
    pending: Option<([u8; 36], Utxo)>,
}

impl<'txn> OverlayGroupReader<'txn> {
    fn new(
        table: &'txn redb::ReadOnlyTable<&'static [u8], &'static [u8]>,
    ) -> Result<Self, SnapshotOverlayError> {
        let mut reader = Self {
            table,
            page: Vec::new().into_iter(),
            resume_after: None,
            exhausted: false,
            pending: None,
        };
        reader.fill_page()?;
        Ok(reader)
    }

    /// Reads the next bounded run of rows, resuming strictly after the last
    /// key already returned.
    fn fill_page(&mut self) -> Result<(), SnapshotOverlayError> {
        if self.exhausted {
            return Ok(());
        }
        let mut rows = Vec::with_capacity(OVERLAY_PAGE_ROWS);
        let range = match &self.resume_after {
            None => self.table.range::<&[u8]>(..).map_err(overlay_redb)?,
            // Exclusive lower bound: the previous page ended on this key.
            Some(previous) => self
                .table
                .range::<&[u8]>((
                    std::ops::Bound::Excluded(previous.as_slice()),
                    std::ops::Bound::Unbounded,
                ))
                .map_err(overlay_redb)?,
        };
        for row in range {
            let (key, value) = row.map_err(overlay_redb)?;
            let key: [u8; 36] = key
                .value()
                .try_into()
                .map_err(|_| SnapshotOverlayError::Invalid("overlay key width"))?;
            rows.push((key, Utxo::decode(value.value())?));
            if rows.len() == OVERLAY_PAGE_ROWS {
                break;
            }
        }
        if let Some((last, _)) = rows.last() {
            self.resume_after = Some(*last);
        }
        self.exhausted = rows.len() < OVERLAY_PAGE_ROWS;
        self.page = rows.into_iter();
        Ok(())
    }

    fn next_row(&mut self) -> Result<Option<([u8; 36], Utxo)>, SnapshotOverlayError> {
        if let Some(row) = self.pending.take() {
            return Ok(Some(row));
        }
        if let Some(row) = self.page.next() {
            return Ok(Some(row));
        }
        if self.exhausted {
            return Ok(None);
        }
        self.fill_page()?;
        Ok(self.page.next())
    }

    #[allow(clippy::type_complexity)]
    fn next_group(&mut self) -> Result<Option<([u8; 32], Vec<(u32, Utxo)>)>, SnapshotOverlayError> {
        let Some(first) = self.next_row()? else {
            return Ok(None);
        };
        let txid: [u8; 32] = first.0[..32].try_into().expect("fixed key length");
        let mut coins = vec![(
            u32::from_le_bytes(first.0[32..].try_into().expect("fixed key length")),
            first.1,
        )];
        // A txid group can straddle a page boundary, so keep pulling rows
        // until the txid changes rather than until the page runs out.
        while let Some(row) = self.next_row()? {
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
        Ok(Some((txid, coins)))
    }
}

impl UtxoStore for SnapshotOverlayRedbChainstate {
    fn get(&self, outpoint: OutPointKey) -> Result<Option<Utxo>, UtxoError> {
        let transaction = self.db.begin_read()?;
        let overlay = transaction.open_table(OVERLAY)?;
        if let Some(value) = overlay.get(outpoint.as_bytes().as_slice())? {
            return Utxo::decode(value.value()).map(Some);
        }
        let tombstone = transaction.open_table(TOMBSTONE)?;
        if tombstone.get(outpoint.as_bytes().as_slice())?.is_some() {
            return Ok(None);
        }
        drop(tombstone);
        drop(overlay);
        drop(transaction);
        self.base_utxo(outpoint.to_outpoint())
    }

    fn get_many(
        &self,
        outpoints: &[OutPointKey],
    ) -> Result<Vec<(OutPointKey, Option<Utxo>)>, UtxoError> {
        let transaction = self.db.begin_read()?;
        let overlay = transaction.open_table(OVERLAY)?;
        let tombstone = transaction.open_table(TOMBSTONE)?;
        let mut results = Vec::with_capacity(outpoints.len());
        for outpoint in outpoints {
            if let Some(value) = overlay.get(outpoint.as_bytes().as_slice())? {
                results.push((*outpoint, Some(Utxo::decode(value.value())?)));
            } else if tombstone.get(outpoint.as_bytes().as_slice())?.is_some() {
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
        let transaction = self.begin_durable_write().map_err(chain_store_to_utxo)?;
        let undo = self
            .connect_mutation(&transaction, spent, created)
            .map_err(chain_store_to_utxo)?;
        transaction.commit()?;
        Ok(undo)
    }

    fn undo(&self, undo: &UtxoUndo, _now: u64, _hot_window_secs: u64) -> Result<(), UtxoError> {
        let _guard = self.lock();
        let transaction = self.begin_durable_write().map_err(chain_store_to_utxo)?;
        self.disconnect_mutation(&transaction, undo.created(), undo.spent())
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
        let transaction = self.db.begin_read()?;
        let overlay = transaction.open_table(OVERLAY)?;
        let tombstone = transaction.open_table(TOMBSTONE)?;
        Ok(TierStats {
            hot: overlay.len()?,
            cold: self.base.coin_count().saturating_sub(tombstone.len()?),
        })
    }
}

impl ExecutionChainStore for SnapshotOverlayRedbChainstate {
    fn execution_tip(&self) -> Result<ExecutionTip, ChainStoreError> {
        let transaction = self.db.begin_read()?;
        let meta = transaction.open_table(META)?;
        let stored = meta
            .get(META_TIP)?
            .ok_or(ChainStoreError::Utxo(UtxoError::Malformed(
                "missing overlay tip",
            )))?;
        decode_tip(stored.value())
    }

    fn assumed_snapshot_base(&self) -> Result<Option<ExecutionTip>, ChainStoreError> {
        Ok(Some(ExecutionTip {
            height: self.identity.height,
            hash: self.identity.block_hash,
        }))
    }

    fn block_undo(&self, hash: BlockHash) -> Result<Option<Vec<UtxoUndo>>, ChainStoreError> {
        let transaction = self.db.begin_read()?;
        let undo = transaction.open_table(UNDO)?;
        undo.get(hash.to_byte_array().as_slice())?
            .map(|bytes| decompress_block_undo(bytes.value()))
            .transpose()
    }

    fn retains_block_undo(&self) -> bool {
        true
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
        let transaction = self.begin_durable_write()?;
        Self::advance_tip(&transaction, expected_parent, next)?;
        let undo = self.connect_mutation(&transaction, spent, created)?;
        {
            let mut undo_table = transaction.open_table(UNDO)?;
            undo_table.insert(
                next.hash.to_byte_array().as_slice(),
                compress_block_undo(transaction_undos)?.as_slice(),
            )?;
        }
        transaction.commit()?;
        Ok(undo)
    }

    fn commit_connect_batch(
        &self,
        transitions: &[ConnectTransition],
    ) -> Result<(), ChainStoreError> {
        if transitions.is_empty() {
            return Ok(());
        }
        let (spent, created) = crate::snapshot_overlay::fold_connect_batch(transitions)?;
        let _guard = self.lock();
        let transaction = self.begin_durable_write()?;
        for transition in transitions {
            Self::advance_tip(&transaction, transition.expected_parent, transition.next)?;
            let mut undo_table = transaction.open_table(UNDO)?;
            undo_table.insert(
                transition.next.hash.to_byte_array().as_slice(),
                compress_block_undo(&transition.transaction_undos)?.as_slice(),
            )?;
        }
        self.connect_mutation(&transaction, &spent, &created)?;
        transaction.commit()?;
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
        let _guard = self.lock();
        let transaction = self.begin_durable_write()?;
        let current = Self::read_tip(&transaction)?;
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
        // See `snapshot_overlay`'s equivalent: the returned undo must
        // describe *this* mutation, so the removed keys' real pre-images go
        // in the `spent` field and the inserted keys in `created`.
        let removed_with_values = self.disconnect_mutation(&transaction, spent, created)?;
        {
            let mut undo_table = transaction.open_table(UNDO)?;
            if undo_table
                .remove(current.hash.to_byte_array().as_slice())?
                .is_none()
            {
                return Err(ChainStoreError::Utxo(UtxoError::Malformed(
                    "missing atomic disconnect undo",
                )));
            }
        }
        {
            let mut meta = transaction.open_table(META)?;
            meta.insert(META_TIP, encode_tip(parent).as_slice())?;
        }
        transaction.commit()?;
        Ok(UtxoUndo::from_parts(
            removed_with_values,
            created.iter().map(|(key, _)| *key).collect(),
        ))
    }

    fn prune_block_undos_before(
        &self,
        headers: &HeaderDag,
        retain_from_height: u32,
    ) -> Result<u64, ChainStoreError> {
        let _guard = self.lock();
        let transaction = self.begin_durable_write()?;
        let mut expired = Vec::new();
        {
            let undo = transaction.open_table(UNDO)?;
            for row in undo.iter()? {
                let (key, _) = row?;
                let key: [u8; 32] = key
                    .value()
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
        {
            let mut undo = transaction.open_table(UNDO)?;
            for hash in expired {
                if undo.remove(hash.to_byte_array().as_slice())?.is_some() {
                    removed += 1;
                }
            }
        }
        transaction.commit()?;
        Ok(removed)
    }
}

fn overlay_redb<E: Into<ChainStoreError>>(error: E) -> SnapshotOverlayError {
    SnapshotOverlayError::ChainStore(error.into())
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;
    use crate::snapshot_overlay::tests::{
        BASE_HEIGHT, IMPORT_TIME, base_coins, block_hash, key, mtp_for, overlay_coin, tip,
        write_base_snapshot,
    };

    fn open_store(
        directory: &Path,
        snapshot_path: &Path,
        index_path: &Path,
        identity: &SnapshotBaseIdentity,
        capacity_bytes: u64,
    ) -> Result<SnapshotOverlayRedbChainstate, SnapshotOverlayError> {
        SnapshotOverlayRedbChainstate::open(
            SnapshotOverlayConfig {
                database_dir: directory.join("overlay.redb"),
                snapshot_path: snapshot_path.to_owned(),
                index_path: index_path.to_owned(),
                capacity_bytes,
                import_time: IMPORT_TIME,
                mtp_by_height: (0..=identity.height).map(mtp_for).collect(),
            },
            Some(identity),
        )
    }

    fn setup(
        capacity_bytes: u64,
    ) -> (
        TempDir,
        SnapshotOverlayRedbChainstate,
        SnapshotBaseIdentity,
        PathBuf,
        PathBuf,
    ) {
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
        (directory, store, identity, snapshot_path, index_path)
    }

    #[test]
    fn resolves_base_coins_and_reports_policy_capacity() {
        let (_directory, store, identity, ..) = setup(32 << 20);
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
        // Unlike MDBX's geometry ceiling, this is a measured file size.
        let usage = store.capacity().unwrap();
        assert!(usage.used_bytes > 0);
        assert_eq!(usage.capacity_bytes, 32 << 20);
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn connects_disconnects_the_full_mutation_matrix() {
        let (_directory, store, identity, ..) = setup(32 << 20);
        let base_a = store.get(key(1, 0)).unwrap().unwrap();
        let base_b = store.get(key(1, 256)).unwrap().unwrap();

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
        assert!(matches!(
            store
                .commit_connect(
                    identity.block_hash,
                    tip(101, block_hash(101)),
                    &[],
                    &[],
                    &[],
                )
                .unwrap_err(),
            ChainStoreError::Execution(ExecutionStoreError::NonSequential { .. })
        ));

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

        // Double spend and duplicate creation both fail closed.
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

        // Disconnect 102 restores both an overlay coin and a base coin.
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

    /// The redb counterpart of the MDBX store's undo-contract regression
    /// test: a disconnect's returned undo must redo exactly that block.
    #[test]
    fn commit_disconnect_returns_an_undo_that_correctly_redoes_the_block() {
        let (_directory, store, identity, ..) = setup(32 << 20);
        let base_a = store.get(key(1, 0)).unwrap().unwrap();
        let d = overlay_coin(101, 111);
        store
            .commit_connect(
                identity.block_hash,
                tip(101, block_hash(101)),
                &[key(1, 0)],
                &[(key(9, 0), d.clone())],
                &[],
            )
            .unwrap();
        let pre_d = store.get(key(9, 0)).unwrap();
        let pre_a = store.get(key(1, 0)).unwrap();

        let redo = store
            .commit_disconnect(
                tip(101, block_hash(101)),
                tip(BASE_HEIGHT, identity.block_hash),
                &[key(9, 0)],
                &[(key(1, 0), base_a)],
                &[],
            )
            .unwrap();
        assert_eq!(store.get(key(9, 0)).unwrap(), None);
        assert!(store.get(key(1, 0)).unwrap().is_some());

        store.undo(&redo, IMPORT_TIME, 0).unwrap();
        assert_eq!(store.get(key(9, 0)).unwrap(), pre_d);
        assert_eq!(store.get(key(1, 0)).unwrap(), pre_a);
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

        // A broken linkage mid-batch rejects the whole batch atomically.
        assert!(matches!(
            store
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
                .unwrap_err(),
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
        drop(reopened);
        assert_eq!(
            SnapshotOverlayRedbChainstate::stored_identity(&directory.path().join("overlay.redb"))
                .unwrap(),
            Some(identity)
        );
    }

    /// Active compaction is this engine's advantage over the MDBX store: it
    /// reclaims space in place, without recreating the database file. This
    /// grows the overlay, spends most of it back, and checks compaction
    /// genuinely shrinks the file.
    #[test]
    fn active_compaction_reclaims_space_freed_by_spends() {
        let (_directory, mut store, identity, ..) = setup(64 << 20);
        let mut parent = identity.block_hash;
        let mut height = BASE_HEIGHT;
        let mut created_keys = Vec::new();
        for round in 0..24_u32 {
            let next_height = height + 1;
            let created: Vec<(OutPointKey, Utxo)> = (0..8_u32)
                .map(|index| {
                    let coin_key = key(u8::try_from(40 + round).unwrap(), index);
                    created_keys.push(coin_key);
                    (
                        coin_key,
                        Utxo {
                            script_pubkey: vec![0x6a; 9_500],
                            ..overlay_coin(next_height, 1)
                        },
                    )
                })
                .collect();
            store
                .commit_connect(
                    parent,
                    tip(next_height, block_hash(next_height)),
                    &[],
                    &created,
                    &[],
                )
                .unwrap();
            parent = block_hash(next_height);
            height = next_height;
        }
        let grown = store.capacity().unwrap().used_bytes;

        // Spend everything just created; the pages become free but redb does
        // not return them to the filesystem until compaction runs.
        for chunk in created_keys.chunks(8) {
            let next_height = height + 1;
            store
                .commit_connect(
                    parent,
                    tip(next_height, block_hash(next_height)),
                    chunk,
                    &[],
                    &[],
                )
                .unwrap();
            parent = block_hash(next_height);
            height = next_height;
        }
        let before_compaction = store.capacity().unwrap().used_bytes;

        let report = store.compact().unwrap();
        assert_eq!(report.before_bytes, before_compaction);
        assert_eq!(report.after_bytes, store.capacity().unwrap().used_bytes);
        assert!(
            report.after_bytes < grown,
            "compaction must release space the spends freed: grown={grown} after={}",
            report.after_bytes
        );
        assert!(report.released_bytes() > 0);

        // The store is still fully usable and consistent afterwards.
        assert_eq!(store.execution_tip().unwrap(), tip(height, parent));
        assert_eq!(store.tier_stats().unwrap(), TierStats { hot: 0, cold: 4 });
        for (txid, vout, coin) in base_coins() {
            assert_eq!(store.get(key(txid, vout)).unwrap().as_ref(), Some(&coin));
        }
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn rebase_folds_the_overlay_and_compacts_the_reclaimed_space() {
        let (directory, mut store, identity, snapshot_path, index_path) = setup(64 << 20);
        let d = overlay_coin(101, 111);
        let e = overlay_coin(101, 222);
        let f = overlay_coin(102, 333);
        store
            .commit_connect(
                identity.block_hash,
                tip(101, block_hash(101)),
                &[key(1, 0)],
                &[(key(9, 0), d.clone()), (key(9, 1), e.clone())],
                &[],
            )
            .unwrap();
        store
            .commit_connect(
                block_hash(101),
                tip(102, block_hash(102)),
                &[key(9, 0)],
                &[(key(11, 0), f)],
                &[],
            )
            .unwrap();

        let new_snapshot = directory.path().join("utxo-102.dat");
        let new_index = directory.path().join("utxo-102.rbtcidx");
        // A wrong MTP extension is rejected before anything is published.
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

        // Folded coins now come from the new base with consensus fields intact.
        assert_eq!(store.get(key(1, 0)).unwrap(), None);
        let folded = store.get(key(9, 1)).unwrap().unwrap();
        assert_eq!(folded.value_sats, e.value_sats);
        assert_eq!(folded.height, e.height);
        assert_eq!(folded.creation_mtp, e.creation_mtp);
        assert_eq!(folded.last_touched, IMPORT_TIME);
        assert_eq!(store.tier_stats().unwrap(), TierStats { hot: 0, cold: 5 });
        assert!(store.block_undo(block_hash(101)).unwrap().is_none());
        assert_eq!(
            store.assumed_snapshot_base().unwrap(),
            Some(tip(102, block_hash(102)))
        );

        // Execution continues above the new base.
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

        // The stored identity now names the rebased base, and the old base
        // files no longer satisfy it.
        assert_eq!(
            SnapshotOverlayRedbChainstate::stored_identity(&directory.path().join("overlay.redb"))
                .unwrap()
                .unwrap(),
            report.identity
        );
        assert!(
            open_store(
                directory.path(),
                &snapshot_path,
                &index_path,
                &identity,
                64 << 20,
            )
            .is_err()
        );
    }

    /// Repeated fill-and-rebase cycles must keep working: the MDBX store
    /// needed its environment file recreated for this, and redb should get
    /// there through compaction alone.
    #[test]
    fn repeated_fill_and_rebase_cycles_stay_within_budget() {
        let (directory, mut store, identity, ..) = setup(64 << 20);
        let mut parent = identity.block_hash;
        let mut height = BASE_HEIGHT;
        let mut base_height = BASE_HEIGHT;
        for cycle in 0..3_u32 {
            for round in 0..16_u32 {
                let next_height = height + 1;
                let created: Vec<(OutPointKey, Utxo)> = (0..8_u32)
                    .map(|index| {
                        (
                            key(u8::try_from(60 + cycle * 16 + round).unwrap(), index),
                            Utxo {
                                script_pubkey: vec![0x6a; 9_500],
                                ..overlay_coin(next_height, 1)
                            },
                        )
                    })
                    .collect();
                store
                    .commit_connect(
                        parent,
                        tip(next_height, block_hash(next_height)),
                        &[],
                        &created,
                        &[],
                    )
                    .unwrap();
                parent = block_hash(next_height);
                height = next_height;
            }
            let mtp_extension: Vec<u32> = (base_height + 1..=height).map(mtp_for).collect();
            let new_snapshot = directory.path().join(format!("utxo-{height}.dat"));
            let new_index = directory.path().join(format!("utxo-{height}.rbtcidx"));
            store
                .rebase_into(&new_snapshot, &new_index, &mtp_extension)
                .unwrap();
            base_height = height;
            let after = store.capacity().unwrap();
            assert!(
                after.used_percent() < 25,
                "cycle {cycle}: compaction after rebase must return the budget, got {}%",
                after.used_percent()
            );
        }
        // Still accepting writes after three cycles.
        store
            .commit_connect(
                parent,
                tip(height + 1, block_hash(height + 1)),
                &[],
                &[(key(200, 0), overlay_coin(height + 1, 1))],
                &[],
            )
            .unwrap();
    }
}
