//! Atomic UTXO storage with a hot/cold physical layout.

use std::{
    collections::BTreeMap,
    path::Path,
    sync::{Arc, Mutex, MutexGuard},
};

use bitcoin::{OutPoint, Txid, hashes::Hash};
use redb::{Database, ReadableTable, ReadableTableMetadata, TableDefinition, WriteTransaction};
use sha2::{Digest, Sha256};
use thiserror::Error;

/// The number of seconds in the default hot window (60 days).
pub const DEFAULT_HOT_WINDOW_SECS: u64 = 60 * 24 * 60 * 60;
const HOT_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("utxo_hot");
const COLD_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("utxo_cold");

/// Errors emitted by UTXO storage and encoding.
#[derive(Debug, Error)]
pub enum UtxoError {
    /// Filesystem preparation failed.
    #[error("UTXO filesystem: {0}")]
    Io(#[from] std::io::Error),
    /// Optional MDBX backend operation failed.
    #[cfg(feature = "mdbx")]
    #[error("MDBX UTXO store: {0}")]
    Mdbx(#[from] libmdbx::Error),
    /// Database open/create failed.
    #[error("redb database: {0}")]
    Database(#[from] redb::DatabaseError),
    /// Transaction creation failed.
    #[error("redb transaction: {0}")]
    Transaction(#[from] redb::TransactionError),
    /// Table access failed.
    #[error("redb table: {0}")]
    Table(#[from] redb::TableError),
    /// Key/value read or write failed.
    #[error("redb storage: {0}")]
    Storage(#[from] redb::StorageError),
    /// Transaction commit failed.
    #[error("redb commit: {0}")]
    Commit(#[from] redb::CommitError),
    /// A persisted record does not have the expected canonical form.
    #[error("malformed UTXO record: {0}")]
    Malformed(&'static str),
    /// A mutation attempted to recreate an already unspent output.
    #[error("duplicate unspent output {0}")]
    Duplicate(OutPointKey),
    /// A transaction attempted to spend an output that is not in chainstate.
    #[error("missing unspent output {0}")]
    Missing(OutPointKey),
    /// A single atomic mutation contains the same input more than once.
    #[error("duplicate spend {0}")]
    DuplicateSpend(OutPointKey),
}

/// A fixed-width Bitcoin outpoint key: wire-order txid followed by vout (LE).
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct OutPointKey([u8; 36]);

impl OutPointKey {
    /// Returns the 36-byte lexicographically sortable database key.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 36] {
        &self.0
    }

    /// Reconstructs an outpoint key from a database key.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, UtxoError> {
        let bytes: [u8; 36] = bytes
            .try_into()
            .map_err(|_| UtxoError::Malformed("outpoint key"))?;
        Ok(Self(bytes))
    }

    /// Converts this key into the rust-bitcoin outpoint type.
    #[must_use]
    pub fn to_outpoint(self) -> OutPoint {
        let txid = Txid::from_byte_array(self.0[..32].try_into().expect("fixed key length"));
        let vout = u32::from_le_bytes(self.0[32..].try_into().expect("fixed key length"));
        OutPoint::new(txid, vout)
    }
}

impl From<OutPoint> for OutPointKey {
    fn from(outpoint: OutPoint) -> Self {
        let mut key = [0_u8; 36];
        key[..32].copy_from_slice(&outpoint.txid.to_byte_array());
        key[32..].copy_from_slice(&outpoint.vout.to_le_bytes());
        Self(key)
    }
}

impl std::fmt::Display for OutPointKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_outpoint())
    }
}

/// The consensus-relevant data of an unspent transaction output plus storage metadata.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Utxo {
    /// Value in satoshis.
    pub value_sats: u64,
    /// Block height that created the coin.
    pub height: u32,
    /// Whether the coin came from a coinbase transaction.
    pub is_coinbase: bool,
    /// Unix seconds when this record was last created or observed unspent.
    pub last_touched: u64,
    /// Median-time-past of the block immediately preceding this output's creation block.
    ///
    /// This is consensus metadata used to evaluate BIP68 time-based relative locks.
    pub creation_mtp: u32,
    /// ScriptPubKey serialized exactly as it appears on the wire.
    pub script_pubkey: Vec<u8>,
}

impl Utxo {
    pub(crate) fn encode(&self) -> Result<Vec<u8>, UtxoError> {
        let script_len = u32::try_from(self.script_pubkey.len())
            .map_err(|_| UtxoError::Malformed("script exceeds u32"))?;
        let mut bytes = Vec::with_capacity(29 + self.script_pubkey.len());
        bytes.extend_from_slice(&self.value_sats.to_le_bytes());
        bytes.extend_from_slice(&self.height.to_le_bytes());
        bytes.push(u8::from(self.is_coinbase));
        bytes.extend_from_slice(&self.last_touched.to_le_bytes());
        bytes.extend_from_slice(&self.creation_mtp.to_le_bytes());
        bytes.extend_from_slice(&script_len.to_le_bytes());
        bytes.extend_from_slice(&self.script_pubkey);
        Ok(bytes)
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, UtxoError> {
        if bytes.len() < 29 {
            return Err(UtxoError::Malformed("record header"));
        }
        let value_sats = u64::from_le_bytes(bytes[..8].try_into().expect("checked length"));
        let height = u32::from_le_bytes(bytes[8..12].try_into().expect("checked length"));
        let is_coinbase = match bytes[12] {
            0 => false,
            1 => true,
            _ => return Err(UtxoError::Malformed("coinbase flag")),
        };
        let last_touched = u64::from_le_bytes(bytes[13..21].try_into().expect("checked length"));
        let creation_mtp = u32::from_le_bytes(bytes[21..25].try_into().expect("checked length"));
        let script_len = u32::from_le_bytes(bytes[25..29].try_into().expect("checked length"));
        let script_len = usize::try_from(script_len).expect("u32 fits usize");
        if bytes.len() != 29 + script_len {
            return Err(UtxoError::Malformed("script length"));
        }
        Ok(Self {
            value_sats,
            height,
            is_coinbase,
            last_touched,
            creation_mtp,
            script_pubkey: bytes[29..].to_vec(),
        })
    }
}

/// Aggregate population counts of the two physical UTXO tiers.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct TierStats {
    /// Number of recently touched UTXOs.
    pub hot: u64,
    /// Number of inactive UTXOs.
    pub cold: u64,
}

/// The information required to reverse one successful atomic UTXO mutation.
///
/// Undo records must be applied in reverse transaction/block order during a
/// chain reorganization.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UtxoUndo {
    spent: Vec<(OutPointKey, Utxo)>,
    created: Vec<OutPointKey>,
}

impl UtxoUndo {
    /// Constructs logical undo data for one atomic mutation.
    #[must_use]
    pub(crate) fn new(spent: Vec<(OutPointKey, Utxo)>, created: Vec<OutPointKey>) -> Self {
        Self { spent, created }
    }

    /// Constructs logical undo data for an external [`UtxoStore`] implementation.
    ///
    /// `spent` contains the exact records removed by the mutation and `created`
    /// contains every outpoint inserted by it. Implementations must reject
    /// missing/duplicate mutations before constructing this value.
    #[must_use]
    pub fn from_parts(spent: Vec<(OutPointKey, Utxo)>, created: Vec<OutPointKey>) -> Self {
        Self { spent, created }
    }

    /// Returns the spent outputs that must be restored on disconnect.
    #[must_use]
    pub fn spent(&self) -> &[(OutPointKey, Utxo)] {
        &self.spent
    }

    /// Returns outputs that must be removed on disconnect.
    #[must_use]
    pub fn created(&self) -> &[OutPointKey] {
        &self.created
    }

    /// Encodes undo data for durable block-disconnect storage.
    pub(crate) fn encode(&self) -> Result<Vec<u8>, UtxoError> {
        let spent_count = u32::try_from(self.spent.len())
            .map_err(|_| UtxoError::Malformed("undo spent count"))?;
        let created_count = u32::try_from(self.created.len())
            .map_err(|_| UtxoError::Malformed("undo created count"))?;
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&spent_count.to_le_bytes());
        for (outpoint, utxo) in &self.spent {
            let utxo = utxo.encode()?;
            let utxo_len =
                u32::try_from(utxo.len()).map_err(|_| UtxoError::Malformed("undo UTXO length"))?;
            bytes.extend_from_slice(outpoint.as_bytes());
            bytes.extend_from_slice(&utxo_len.to_le_bytes());
            bytes.extend_from_slice(&utxo);
        }
        bytes.extend_from_slice(&created_count.to_le_bytes());
        for outpoint in &self.created {
            bytes.extend_from_slice(outpoint.as_bytes());
        }
        Ok(bytes)
    }

    /// Decodes undo data previously produced by [`Self::encode`].
    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, UtxoError> {
        let (spent_count, mut cursor) = take_u32(bytes, 0, "undo spent count")?;
        let spent_count = usize::try_from(spent_count).expect("u32 fits usize");
        if spent_count > bytes.len().saturating_sub(cursor) / 40 {
            return Err(UtxoError::Malformed("undo spent count exceeds record"));
        }
        let mut spent = Vec::with_capacity(spent_count);
        for _ in 0..spent_count {
            let outpoint =
                OutPointKey::from_bytes(take(bytes, &mut cursor, 36, "undo spent outpoint")?)?;
            let utxo_len = take_u32_at(bytes, &mut cursor, "undo UTXO length")?;
            let utxo_len = usize::try_from(utxo_len).expect("u32 fits usize");
            let utxo = Utxo::decode(take(bytes, &mut cursor, utxo_len, "undo UTXO")?)?;
            spent.push((outpoint, utxo));
        }
        let created_count = take_u32_at(bytes, &mut cursor, "undo created count")?;
        let created_count = usize::try_from(created_count).expect("u32 fits usize");
        if created_count > bytes.len().saturating_sub(cursor) / 36 {
            return Err(UtxoError::Malformed("undo created count exceeds record"));
        }
        let mut created = Vec::with_capacity(created_count);
        for _ in 0..created_count {
            created.push(OutPointKey::from_bytes(take(
                bytes,
                &mut cursor,
                36,
                "undo created outpoint",
            )?)?);
        }
        if cursor != bytes.len() {
            return Err(UtxoError::Malformed("trailing undo bytes"));
        }
        Ok(Self { spent, created })
    }
}

fn take<'a>(
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

fn take_u32(bytes: &[u8], cursor: usize, field: &'static str) -> Result<(u32, usize), UtxoError> {
    let mut cursor = cursor;
    let value = take(bytes, &mut cursor, 4, field)?;
    Ok((
        u32::from_le_bytes(value.try_into().expect("fixed length")),
        cursor,
    ))
}

fn take_u32_at(bytes: &[u8], cursor: &mut usize, field: &'static str) -> Result<u32, UtxoError> {
    let value = take(bytes, cursor, 4, field)?;
    Ok(u32::from_le_bytes(value.try_into().expect("fixed length")))
}

/// Atomic UTXO operations used by chainstate and snapshots.
pub trait UtxoStore: Send + Sync {
    /// Fetches a coin from either physical tier.
    fn get(&self, outpoint: OutPointKey) -> Result<Option<Utxo>, UtxoError>;
    /// Atomically deletes inputs and inserts fresh outputs.
    fn apply(
        &self,
        spent: &[OutPointKey],
        created: &[(OutPointKey, Utxo)],
    ) -> Result<(), UtxoError>;
    /// Applies a mutation and returns durable logical undo data for a later reorg.
    fn apply_with_undo(
        &self,
        spent: &[OutPointKey],
        created: &[(OutPointKey, Utxo)],
    ) -> Result<UtxoUndo, UtxoError>;
    /// Reverses one prior mutation using its undo data.
    fn undo(&self, undo: &UtxoUndo, now: u64, hot_window_secs: u64) -> Result<(), UtxoError>;
    /// Moves aged records from hot to cold without changing their consensus content.
    fn age_to_cold(&self, now: u64, hot_window_secs: u64) -> Result<u64, UtxoError>;
    /// Returns stable, sorted records for a consistent snapshot.
    fn snapshot_entries(&self) -> Result<BTreeMap<OutPointKey, Utxo>, UtxoError>;
    /// Replaces all records atomically from already-validated snapshot entries.
    fn replace_all(
        &self,
        entries: &BTreeMap<OutPointKey, Utxo>,
        now: u64,
        hot_window_secs: u64,
    ) -> Result<(), UtxoError>;
    /// Counts entries in each tier.
    fn tier_stats(&self) -> Result<TierStats, UtxoError>;
}

/// redb-backed UTXO store. Its copy-on-write B-trees offer crash-safe ACID transactions without a C/C++ toolchain.
pub struct RedbUtxoStore {
    db: Arc<Database>,
    /// Coordinates logically related operations spanning both physical tables.
    write_guard: Mutex<()>,
}

impl RedbUtxoStore {
    /// Opens or creates a chainstate file at `path`.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, UtxoError> {
        Self::from_database(Arc::new(Database::create(path)?))
    }

    pub(crate) fn from_database(db: Arc<Database>) -> Result<Self, UtxoError> {
        let transaction = db.begin_write()?;
        {
            let _hot = transaction.open_table(HOT_TABLE)?;
            let _cold = transaction.open_table(COLD_TABLE)?;
        }
        transaction.commit()?;
        Ok(Self {
            db,
            write_guard: Mutex::new(()),
        })
    }

    fn lock(&self) -> MutexGuard<'_, ()> {
        self.write_guard.lock().expect("write lock not poisoned")
    }

    /// Computes count, encoded length, and logical UTXO-set identity without
    /// materializing all UTXOs. The digest excludes local `last_touched` time.
    pub(crate) fn snapshot_content_identity(&self) -> Result<(u64, u64, [u8; 32]), UtxoError> {
        let _guard = self.lock();
        let transaction = self.db.begin_read()?;
        let hot = transaction.open_table(HOT_TABLE)?;
        let cold = transaction.open_table(COLD_TABLE)?;
        let mut hot_rows = hot.iter()?;
        let mut cold_rows = cold.iter()?;
        let mut hot_next = hot_rows
            .next()
            .transpose()?
            .map(|(key, value)| (key.value().to_vec(), value.value().to_vec()));
        let mut cold_next = cold_rows
            .next()
            .transpose()?
            .map(|(key, value)| (key.value().to_vec(), value.value().to_vec()));
        let mut count = 0_u64;
        let mut records_bytes = 0_u64;
        let mut digest = Sha256::new();
        while hot_next.is_some() || cold_next.is_some() {
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
            let (key, value) = if take_hot {
                let row = hot_next.take().expect("selected populated hot iterator");
                hot_next = hot_rows
                    .next()
                    .transpose()?
                    .map(|(key, value)| (key.value().to_vec(), value.value().to_vec()));
                row
            } else {
                let row = cold_next.take().expect("selected populated cold iterator");
                cold_next = cold_rows
                    .next()
                    .transpose()?
                    .map(|(key, value)| (key.value().to_vec(), value.value().to_vec()));
                row
            };
            OutPointKey::from_bytes(&key)?;
            let utxo = Utxo::decode(&value)?;
            update_utxo_set_digest(&mut digest, &key, &utxo);
            count = count
                .checked_add(1)
                .ok_or(UtxoError::Malformed("snapshot UTXO count overflow"))?;
            records_bytes = records_bytes
                .checked_add(u64::try_from(key.len() + value.len()).expect("record fits u64"))
                .ok_or(UtxoError::Malformed("snapshot records length overflow"))?;
        }
        Ok((count, records_bytes, digest.finalize().into()))
    }
}

impl UtxoStore for RedbUtxoStore {
    fn get(&self, outpoint: OutPointKey) -> Result<Option<Utxo>, UtxoError> {
        let transaction = self.db.begin_read()?;
        let hot = transaction.open_table(HOT_TABLE)?;
        if let Some(value) = hot.get(outpoint.as_bytes().as_slice())? {
            return Utxo::decode(value.value()).map(Some);
        }
        let cold = transaction.open_table(COLD_TABLE)?;
        cold.get(outpoint.as_bytes().as_slice())?
            .map(|value| Utxo::decode(value.value()))
            .transpose()
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
        let transaction = self.db.begin_write()?;
        let undo = apply_with_undo_transaction(&transaction, spent, created)?;
        transaction.commit()?;
        Ok(undo)
    }

    fn undo(&self, undo: &UtxoUndo, now: u64, hot_window_secs: u64) -> Result<(), UtxoError> {
        let _guard = self.lock();
        let cutoff = now.saturating_sub(hot_window_secs);
        let transaction = self.db.begin_write()?;
        {
            let mut hot = transaction.open_table(HOT_TABLE)?;
            let mut cold = transaction.open_table(COLD_TABLE)?;
            let recreated = undo
                .created
                .iter()
                .copied()
                .collect::<std::collections::BTreeSet<_>>();
            for (key, _) in &undo.spent {
                if !recreated.contains(key)
                    && (hot.get(key.as_bytes().as_slice())?.is_some()
                        || cold.get(key.as_bytes().as_slice())?.is_some())
                {
                    return Err(UtxoError::Duplicate(*key));
                }
            }
            for key in &undo.created {
                hot.remove(key.as_bytes().as_slice())?;
                cold.remove(key.as_bytes().as_slice())?;
            }
            for (key, utxo) in &undo.spent {
                let encoded = utxo.encode()?;
                if utxo.last_touched < cutoff {
                    cold.insert(key.as_bytes().as_slice(), encoded.as_slice())?;
                } else {
                    hot.insert(key.as_bytes().as_slice(), encoded.as_slice())?;
                }
            }
        }
        transaction.commit()?;
        Ok(())
    }

    fn age_to_cold(&self, now: u64, hot_window_secs: u64) -> Result<u64, UtxoError> {
        let _guard = self.lock();
        let cutoff = now.saturating_sub(hot_window_secs);
        let transaction = self.db.begin_write()?;
        let moved = {
            let hot = transaction.open_table(HOT_TABLE)?;
            let rows = hot
                .iter()?
                .filter_map(|row| match row {
                    Ok((key, value)) => match Utxo::decode(value.value()) {
                        Ok(utxo) if utxo.last_touched < cutoff => {
                            Some(Ok((key.value().to_vec(), value.value().to_vec())))
                        }
                        Ok(_) => None,
                        Err(error) => Some(Err(error)),
                    },
                    Err(error) => Some(Err(error.into())),
                })
                .collect::<Result<Vec<_>, UtxoError>>()?;
            drop(hot);
            let mut hot = transaction.open_table(HOT_TABLE)?;
            let mut cold = transaction.open_table(COLD_TABLE)?;
            for (key, value) in &rows {
                hot.remove(key.as_slice())?;
                cold.insert(key.as_slice(), value.as_slice())?;
            }
            u64::try_from(rows.len()).expect("usize fits u64")
        };
        transaction.commit()?;
        Ok(moved)
    }

    fn snapshot_entries(&self) -> Result<BTreeMap<OutPointKey, Utxo>, UtxoError> {
        let _guard = self.lock();
        let transaction = self.db.begin_read()?;
        let mut entries = BTreeMap::new();
        for definition in [HOT_TABLE, COLD_TABLE] {
            let table = transaction.open_table(definition)?;
            for row in table.iter()? {
                let (key, value) = row?;
                let key = OutPointKey::from_bytes(key.value())?;
                if entries.insert(key, Utxo::decode(value.value())?).is_some() {
                    return Err(UtxoError::Malformed("outpoint in both tiers"));
                }
            }
        }
        Ok(entries)
    }

    fn replace_all(
        &self,
        entries: &BTreeMap<OutPointKey, Utxo>,
        now: u64,
        hot_window_secs: u64,
    ) -> Result<(), UtxoError> {
        let _guard = self.lock();
        let transaction = self.db.begin_write()?;
        replace_all_transaction(&transaction, entries, now, hot_window_secs)?;
        transaction.commit()?;
        Ok(())
    }

    fn tier_stats(&self) -> Result<TierStats, UtxoError> {
        let _guard = self.lock();
        let transaction = self.db.begin_read()?;
        let count = |definition| -> Result<u64, UtxoError> {
            let table = transaction.open_table(definition)?;
            let count = table
                .iter()?
                .try_fold(0_u64, |count, row| row.map(|_| count + 1))?;
            Ok(count)
        };
        Ok(TierStats {
            hot: count(HOT_TABLE)?,
            cold: count(COLD_TABLE)?,
        })
    }
}

pub(crate) fn tables_empty_transaction(transaction: &WriteTransaction) -> Result<bool, UtxoError> {
    let hot = transaction.open_table(HOT_TABLE)?;
    let cold = transaction.open_table(COLD_TABLE)?;
    Ok(hot.is_empty()? && cold.is_empty()?)
}

pub(crate) fn replace_all_transaction(
    transaction: &WriteTransaction,
    entries: &BTreeMap<OutPointKey, Utxo>,
    now: u64,
    hot_window_secs: u64,
) -> Result<(), UtxoError> {
    let cutoff = now.saturating_sub(hot_window_secs);
    {
        let mut hot = transaction.open_table(HOT_TABLE)?;
        hot.retain(|_, _| false)?;
    }
    {
        let mut cold = transaction.open_table(COLD_TABLE)?;
        cold.retain(|_, _| false)?;
    }
    let mut hot = transaction.open_table(HOT_TABLE)?;
    let mut cold = transaction.open_table(COLD_TABLE)?;
    for (key, value) in entries {
        let encoded = value.encode()?;
        if value.last_touched < cutoff {
            cold.insert(key.as_bytes().as_slice(), encoded.as_slice())?;
        } else {
            hot.insert(key.as_bytes().as_slice(), encoded.as_slice())?;
        }
    }
    Ok(())
}

pub(crate) fn insert_snapshot_entries_transaction<I>(
    transaction: &WriteTransaction,
    entries: I,
    expected_count: u64,
    expected_records_bytes: u64,
    now: u64,
    hot_window_secs: u64,
) -> Result<(u64, u64, [u8; 32], [u8; 32]), UtxoError>
where
    I: IntoIterator<Item = Result<(OutPointKey, Utxo), UtxoError>>,
{
    let cutoff = now.saturating_sub(hot_window_secs);
    let mut hot = transaction.open_table(HOT_TABLE)?;
    let mut cold = transaction.open_table(COLD_TABLE)?;
    let mut previous = None;
    let mut count = 0_u64;
    let mut records_bytes = 0_u64;
    let mut records = Sha256::new();
    let mut utxo_set = Sha256::new();
    for entry in entries {
        let (key, utxo) = entry?;
        if previous.is_some_and(|previous| key <= previous) {
            return Err(UtxoError::Malformed(
                "snapshot outpoints are not strictly ordered",
            ));
        }
        previous = Some(key);
        let encoded = utxo.encode()?;
        // Snapshot v2 records are exactly the 36-byte key followed by the
        // canonical UTXO encoding. Hash inside the redb transaction so a file
        // replacement between inspection and activation cannot become durable.
        records.update(key.as_bytes());
        records.update(&encoded);
        update_utxo_set_digest(&mut utxo_set, key.as_bytes(), &utxo);
        if utxo.last_touched < cutoff {
            cold.insert(key.as_bytes().as_slice(), encoded.as_slice())?;
        } else {
            hot.insert(key.as_bytes().as_slice(), encoded.as_slice())?;
        }
        count = count
            .checked_add(1)
            .ok_or(UtxoError::Malformed("snapshot UTXO count overflow"))?;
        records_bytes = records_bytes
            .checked_add(u64::try_from(36 + encoded.len()).expect("record length fits u64"))
            .ok_or(UtxoError::Malformed("snapshot records length overflow"))?;
        if count > expected_count {
            return Err(UtxoError::Malformed(
                "snapshot exceeds authenticated UTXO count",
            ));
        }
        if records_bytes > expected_records_bytes {
            return Err(UtxoError::Malformed(
                "snapshot exceeds authenticated records length",
            ));
        }
    }
    Ok((
        count,
        records_bytes,
        records.finalize().into(),
        utxo_set.finalize().into(),
    ))
}

fn update_utxo_set_digest(digest: &mut Sha256, key: &[u8], utxo: &Utxo) {
    digest.update(key);
    digest.update(utxo.value_sats.to_le_bytes());
    digest.update(utxo.height.to_le_bytes());
    digest.update([u8::from(utxo.is_coinbase)]);
    digest.update(utxo.creation_mtp.to_le_bytes());
    digest.update(
        u32::try_from(utxo.script_pubkey.len())
            .expect("persisted script length was validated")
            .to_le_bytes(),
    );
    digest.update(&utxo.script_pubkey);
}

pub(crate) fn apply_with_undo_transaction(
    transaction: &WriteTransaction,
    spent: &[OutPointKey],
    created: &[(OutPointKey, Utxo)],
) -> Result<UtxoUndo, UtxoError> {
    let mut hot = transaction.open_table(HOT_TABLE)?;
    let mut cold = transaction.open_table(COLD_TABLE)?;
    let mut seen_spent = std::collections::BTreeSet::new();
    let mut undo_spent = Vec::with_capacity(spent.len());
    for key in spent {
        if !seen_spent.insert(*key) {
            return Err(UtxoError::DuplicateSpend(*key));
        }
        let previous = match hot.get(key.as_bytes().as_slice())? {
            Some(value) => Utxo::decode(value.value())?,
            None => match cold.get(key.as_bytes().as_slice())? {
                Some(value) => Utxo::decode(value.value())?,
                None => return Err(UtxoError::Missing(*key)),
            },
        };
        undo_spent.push((*key, previous));
    }
    let mut seen_created = std::collections::BTreeSet::new();
    for (key, _) in created {
        if !seen_created.insert(*key) {
            return Err(UtxoError::Duplicate(*key));
        }
        if !seen_spent.contains(key)
            && (hot.get(key.as_bytes().as_slice())?.is_some()
                || cold.get(key.as_bytes().as_slice())?.is_some())
        {
            return Err(UtxoError::Duplicate(*key));
        }
    }
    for key in spent {
        hot.remove(key.as_bytes().as_slice())?;
        cold.remove(key.as_bytes().as_slice())?;
    }
    for (key, utxo) in created {
        let value = utxo.encode()?;
        hot.insert(key.as_bytes().as_slice(), value.as_slice())?;
    }
    Ok(UtxoUndo {
        spent: undo_spent,
        created: created.iter().map(|(key, _)| *key).collect(),
    })
}

#[cfg(test)]
mod tests {
    use bitcoin::{OutPoint, Txid, hashes::Hash};
    use tempfile::TempDir;

    use super::*;

    fn store() -> (TempDir, RedbUtxoStore) {
        let dir = TempDir::new().unwrap();
        let store = RedbUtxoStore::open(dir.path().join("chainstate.redb")).unwrap();
        (dir, store)
    }
    fn key(n: u8) -> OutPointKey {
        OutPoint::new(Txid::from_byte_array([n; 32]), 0).into()
    }
    fn coin(touched: u64) -> Utxo {
        Utxo {
            value_sats: 42,
            height: 100,
            is_coinbase: false,
            last_touched: touched,
            creation_mtp: 0,
            script_pubkey: vec![0x51],
        }
    }

    #[test]
    fn apply_is_atomic_and_ages_to_cold() {
        let (_dir, store) = store();
        store
            .apply(&[], &[(key(1), coin(10)), (key(2), coin(100))])
            .unwrap();
        assert_eq!(store.age_to_cold(100, 60).unwrap(), 1);
        assert_eq!(store.tier_stats().unwrap(), TierStats { hot: 1, cold: 1 });
        store.apply(&[key(1)], &[]).unwrap();
        assert!(store.get(key(1)).unwrap().is_none());
    }

    #[test]
    fn rejects_duplicate_and_roundtrips_outpoint() {
        let (_dir, store) = store();
        let outpoint = OutPoint::new(Txid::from_byte_array([7; 32]), 12);
        let key = OutPointKey::from(outpoint);
        assert_eq!(key.to_outpoint(), outpoint);
        store.apply(&[], &[(key, coin(1))]).unwrap();
        assert!(matches!(
            store.apply(&[], &[(key, coin(1))]),
            Err(UtxoError::Duplicate(_))
        ));
    }

    #[test]
    fn undo_restores_the_pre_mutation_chainstate() {
        let (_dir, store) = store();
        store.apply(&[], &[(key(1), coin(10))]).unwrap();
        let before = store.snapshot_entries().unwrap();
        let undo = store
            .apply_with_undo(&[key(1)], &[(key(2), coin(20))])
            .unwrap();
        assert!(store.get(key(1)).unwrap().is_none());
        assert!(store.get(key(2)).unwrap().is_some());
        store.undo(&undo, 100, 60).unwrap();
        assert_eq!(store.snapshot_entries().unwrap(), before);
    }

    #[test]
    fn rejects_missing_and_duplicate_spends() {
        let (_dir, store) = store();
        assert!(matches!(
            store.apply(&[key(1)], &[]),
            Err(UtxoError::Missing(_))
        ));
        store.apply(&[], &[(key(1), coin(1))]).unwrap();
        assert!(matches!(
            store.apply(&[key(1), key(1)], &[]),
            Err(UtxoError::DuplicateSpend(_))
        ));
    }

    #[test]
    fn atomic_replacement_undo_restores_original_coin() {
        let (_dir, store) = store();
        store.apply(&[], &[(key(1), coin(10))]).unwrap();
        let replacement = Utxo {
            value_sats: 99,
            ..coin(20)
        };
        let undo = store
            .apply_with_undo(&[key(1)], &[(key(1), replacement.clone())])
            .unwrap();
        assert_eq!(store.get(key(1)).unwrap(), Some(replacement));
        store.undo(&undo, 100, 60).unwrap();
        assert_eq!(store.get(key(1)).unwrap(), Some(coin(10)));
    }
}
