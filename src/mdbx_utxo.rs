//! Optional durable MDBX implementation of the hot/cold UTXO interface.
//!
//! This backend is experimental and intended for storage evaluation. Production
//! active-chain execution remains on the unified redb chain store until MDBX
//! also owns undo and execution-tip metadata in the same transaction.

use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
    sync::{Mutex, MutexGuard},
};

use libmdbx::{
    Database, DatabaseOptions, Mode, NoWriteMap, ReadWriteOptions, SyncMode, TableFlags, WriteFlags,
};

use crate::utxo::{OutPointKey, TierStats, Utxo, UtxoError, UtxoStore, UtxoUndo};

const HOT: &str = "utxo_hot";
const COLD: &str = "utxo_cold";

/// Durable, copy-on-write MDBX UTXO store using two named tables.
pub struct MdbxUtxoStore {
    db: Database<NoWriteMap>,
    write_guard: Mutex<()>,
}

impl MdbxUtxoStore {
    /// Opens or creates a durable MDBX environment directory.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, UtxoError> {
        std::fs::create_dir_all(path.as_ref())?;
        let db = Database::open_with_options(
            path,
            DatabaseOptions {
                max_tables: Some(2),
                mode: Mode::ReadWrite(ReadWriteOptions {
                    sync_mode: SyncMode::Durable,
                    ..ReadWriteOptions::default()
                }),
                ..DatabaseOptions::default()
            },
        )?;
        let transaction = db.begin_rw_txn()?;
        transaction.create_table(Some(HOT), TableFlags::empty())?;
        transaction.create_table(Some(COLD), TableFlags::empty())?;
        transaction.commit()?;
        Ok(Self {
            db,
            write_guard: Mutex::new(()),
        })
    }

    fn lock(&self) -> MutexGuard<'_, ()> {
        self.write_guard
            .lock()
            .expect("MDBX write lock not poisoned")
    }
}

impl UtxoStore for MdbxUtxoStore {
    fn get(&self, outpoint: OutPointKey) -> Result<Option<Utxo>, UtxoError> {
        let transaction = self.db.begin_ro_txn()?;
        let hot = transaction.open_table(Some(HOT))?;
        if let Some(value) = transaction.get::<Vec<u8>>(&hot, outpoint.as_bytes())? {
            return Utxo::decode(&value).map(Some);
        }
        let cold = transaction.open_table(Some(COLD))?;
        transaction
            .get::<Vec<u8>>(&cold, outpoint.as_bytes())?
            .map(|value| Utxo::decode(&value))
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
        let transaction = self.db.begin_rw_txn()?;
        let hot = transaction.open_table(Some(HOT))?;
        let cold = transaction.open_table(Some(COLD))?;
        let mut seen_spent = BTreeSet::new();
        let mut undo_spent = Vec::with_capacity(spent.len());
        for key in spent {
            if !seen_spent.insert(*key) {
                return Err(UtxoError::DuplicateSpend(*key));
            }
            let value = transaction
                .get::<Vec<u8>>(&hot, key.as_bytes())?
                .or(transaction.get::<Vec<u8>>(&cold, key.as_bytes())?)
                .ok_or(UtxoError::Missing(*key))?;
            undo_spent.push((*key, Utxo::decode(&value)?));
        }
        let mut seen_created = BTreeSet::new();
        for (key, _) in created {
            if !seen_created.insert(*key) {
                return Err(UtxoError::Duplicate(*key));
            }
            if !seen_spent.contains(key)
                && (transaction.get::<()>(&hot, key.as_bytes())?.is_some()
                    || transaction.get::<()>(&cold, key.as_bytes())?.is_some())
            {
                return Err(UtxoError::Duplicate(*key));
            }
        }
        for key in spent {
            transaction.del(&hot, key.as_bytes(), None)?;
            transaction.del(&cold, key.as_bytes(), None)?;
        }
        for (key, utxo) in created {
            transaction.put(&hot, key.as_bytes(), utxo.encode()?, WriteFlags::empty())?;
        }
        transaction.commit()?;
        Ok(UtxoUndo::new(
            undo_spent,
            created.iter().map(|(key, _)| *key).collect(),
        ))
    }

    fn undo(&self, undo: &UtxoUndo, now: u64, hot_window_secs: u64) -> Result<(), UtxoError> {
        let _guard = self.lock();
        let cutoff = now.saturating_sub(hot_window_secs);
        let transaction = self.db.begin_rw_txn()?;
        let hot = transaction.open_table(Some(HOT))?;
        let cold = transaction.open_table(Some(COLD))?;
        let recreated = undo.created().iter().copied().collect::<BTreeSet<_>>();
        for (key, _) in undo.spent() {
            if !recreated.contains(key)
                && (transaction.get::<()>(&hot, key.as_bytes())?.is_some()
                    || transaction.get::<()>(&cold, key.as_bytes())?.is_some())
            {
                return Err(UtxoError::Duplicate(*key));
            }
        }
        for key in undo.created() {
            transaction.del(&hot, key.as_bytes(), None)?;
            transaction.del(&cold, key.as_bytes(), None)?;
        }
        for (key, utxo) in undo.spent() {
            let target = if utxo.last_touched < cutoff {
                &cold
            } else {
                &hot
            };
            transaction.put(target, key.as_bytes(), utxo.encode()?, WriteFlags::empty())?;
        }
        transaction.commit()?;
        Ok(())
    }

    fn age_to_cold(&self, now: u64, hot_window_secs: u64) -> Result<u64, UtxoError> {
        let _guard = self.lock();
        let cutoff = now.saturating_sub(hot_window_secs);
        let transaction = self.db.begin_rw_txn()?;
        let hot = transaction.open_table(Some(HOT))?;
        let cold = transaction.open_table(Some(COLD))?;
        let rows = {
            let mut cursor = transaction.cursor(&hot)?;
            cursor
                .iter_start::<Vec<u8>, Vec<u8>>()
                .filter_map(|row| match row {
                    Ok((key, value)) => match Utxo::decode(&value) {
                        Ok(utxo) if utxo.last_touched < cutoff => Some(Ok((key, value))),
                        Ok(_) => None,
                        Err(error) => Some(Err(error)),
                    },
                    Err(error) => Some(Err(error.into())),
                })
                .collect::<Result<Vec<_>, UtxoError>>()?
        };
        for (key, value) in &rows {
            transaction.del(&hot, key, None)?;
            transaction.put(&cold, key, value, WriteFlags::empty())?;
        }
        transaction.commit()?;
        Ok(u64::try_from(rows.len()).expect("usize fits u64"))
    }

    fn snapshot_entries(&self) -> Result<BTreeMap<OutPointKey, Utxo>, UtxoError> {
        let transaction = self.db.begin_ro_txn()?;
        let mut entries = BTreeMap::new();
        for name in [HOT, COLD] {
            let table = transaction.open_table(Some(name))?;
            let mut cursor = transaction.cursor(&table)?;
            for row in cursor.iter_start::<Vec<u8>, Vec<u8>>() {
                let (key, value) = row?;
                let key = OutPointKey::from_bytes(&key)?;
                if entries.insert(key, Utxo::decode(&value)?).is_some() {
                    return Err(UtxoError::Malformed("outpoint in both MDBX tiers"));
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
        let transaction = self.db.begin_rw_txn()?;
        let hot = transaction.open_table(Some(HOT))?;
        let cold = transaction.open_table(Some(COLD))?;
        transaction.clear_table(&hot)?;
        transaction.clear_table(&cold)?;
        for (key, utxo) in entries {
            let target = if utxo.last_touched < cutoff {
                &cold
            } else {
                &hot
            };
            transaction.put(target, key.as_bytes(), utxo.encode()?, WriteFlags::empty())?;
        }
        transaction.commit()?;
        Ok(())
    }

    fn tier_stats(&self) -> Result<TierStats, UtxoError> {
        let transaction = self.db.begin_ro_txn()?;
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

#[cfg(test)]
mod tests {
    use bitcoin::{OutPoint, Txid, hashes::Hash};
    use tempfile::TempDir;

    use super::*;

    fn key(byte: u8) -> OutPointKey {
        OutPoint::new(Txid::from_byte_array([byte; 32]), 0).into()
    }

    fn coin(touched: u64) -> Utxo {
        Utxo {
            value_sats: 42,
            height: 1,
            is_coinbase: false,
            last_touched: touched,
            creation_mtp: 0,
            script_pubkey: vec![0x51],
        }
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
        assert_eq!(store.age_to_cold(100, 60).unwrap(), 1);
        assert_eq!(store.tier_stats().unwrap(), TierStats { hot: 1, cold: 1 });
        store.undo(&undo, 100, 60).unwrap();
        drop(store);

        let reopened = MdbxUtxoStore::open(path).unwrap();
        assert_eq!(reopened.get(key(1)).unwrap(), Some(coin(1)));
        assert_eq!(reopened.get(key(2)).unwrap(), Some(coin(100)));
        assert!(reopened.get(key(3)).unwrap().is_none());
    }
}
