//! Atomic UTXO storage with a hot/cold physical layout.

use std::{
    collections::BTreeMap,
    path::Path,
    sync::{Mutex, MutexGuard},
};

use bitcoin::{OutPoint, Txid, hashes::Hash};
use redb::{Database, ReadableTable, TableDefinition};
use thiserror::Error;

/// The number of seconds in the default hot window (60 days).
pub const DEFAULT_HOT_WINDOW_SECS: u64 = 60 * 24 * 60 * 60;
const HOT_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("utxo_hot");
const COLD_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("utxo_cold");

/// Errors emitted by UTXO storage and encoding.
#[derive(Debug, Error)]
pub enum UtxoError {
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
    /// ScriptPubKey serialized exactly as it appears on the wire.
    pub script_pubkey: Vec<u8>,
}

impl Utxo {
    fn encode(&self) -> Result<Vec<u8>, UtxoError> {
        let script_len = u32::try_from(self.script_pubkey.len())
            .map_err(|_| UtxoError::Malformed("script exceeds u32"))?;
        let mut bytes = Vec::with_capacity(25 + self.script_pubkey.len());
        bytes.extend_from_slice(&self.value_sats.to_le_bytes());
        bytes.extend_from_slice(&self.height.to_le_bytes());
        bytes.push(u8::from(self.is_coinbase));
        bytes.extend_from_slice(&self.last_touched.to_le_bytes());
        bytes.extend_from_slice(&script_len.to_le_bytes());
        bytes.extend_from_slice(&self.script_pubkey);
        Ok(bytes)
    }

    fn decode(bytes: &[u8]) -> Result<Self, UtxoError> {
        if bytes.len() < 25 {
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
        let script_len = u32::from_le_bytes(bytes[21..25].try_into().expect("checked length"));
        let script_len = usize::try_from(script_len).expect("u32 fits usize");
        if bytes.len() != 25 + script_len {
            return Err(UtxoError::Malformed("script length"));
        }
        Ok(Self {
            value_sats,
            height,
            is_coinbase,
            last_touched,
            script_pubkey: bytes[25..].to_vec(),
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
    db: Database,
    /// Coordinates logically related operations spanning both physical tables.
    write_guard: Mutex<()>,
}

impl RedbUtxoStore {
    /// Opens or creates a chainstate file at `path`.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, UtxoError> {
        let db = Database::create(path)?;
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
        let undo = {
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
                if hot.get(key.as_bytes().as_slice())?.is_some()
                    || cold.get(key.as_bytes().as_slice())?.is_some()
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
            UtxoUndo {
                spent: undo_spent,
                created: created.iter().map(|(key, _)| *key).collect(),
            }
        };
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
            for (key, _) in &undo.spent {
                if hot.get(key.as_bytes().as_slice())?.is_some()
                    || cold.get(key.as_bytes().as_slice())?.is_some()
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
        let cutoff = now.saturating_sub(hot_window_secs);
        let transaction = self.db.begin_write()?;
        {
            let mut hot = transaction.open_table(HOT_TABLE)?;
            hot.retain(|_, _| false)?;
        }
        {
            let mut cold = transaction.open_table(COLD_TABLE)?;
            cold.retain(|_, _| false)?;
        }
        {
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
        }
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
}
