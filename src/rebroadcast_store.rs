//! Bounded, network-bound persistence for locally originated wallet transactions.

use std::{collections::HashSet, fs, path::Path, sync::Mutex};

use bitcoin::{
    Network, Transaction, Txid, Wtxid,
    consensus::{deserialize, serialize},
    hashes::Hash,
    hex::{DisplayHex, FromHex},
};
use redb::{Database, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};
use thiserror::Error;

const META: TableDefinition<&str, &[u8]> = TableDefinition::new("rebroadcast_metadata");
const TRANSACTIONS: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("rebroadcast_transactions");
const GENESIS_KEY: &str = "genesis";
const MAX_STORED_TRANSACTIONS: usize = 64;
const MAX_STORED_TRANSACTION_BYTES: usize = 128 * 1024;
const MAX_STORED_VALUE_BYTES: usize = MAX_STORED_TRANSACTION_BYTES * 2 + 256;
const REBROADCAST_INTERVAL_SECS: u64 = 12 * 60 * 60;
const REBROADCAST_EXPIRY_SECS: u64 = 14 * 24 * 60 * 60;

/// Durable rebroadcast queue failures.
#[derive(Debug, Error)]
pub enum RebroadcastStoreError {
    /// Database open/create failed.
    #[error("rebroadcast database: {0}")]
    Database(#[from] redb::DatabaseError),
    /// Transaction creation failed.
    #[error("rebroadcast transaction: {0}")]
    Transaction(#[from] redb::TransactionError),
    /// Table access failed.
    #[error("rebroadcast table: {0}")]
    Table(#[from] redb::TableError),
    /// Key/value access failed.
    #[error("rebroadcast storage: {0}")]
    Storage(#[from] redb::StorageError),
    /// Transaction commit failed.
    #[error("rebroadcast commit: {0}")]
    Commit(#[from] redb::CommitError),
    /// Metadata serialization failed.
    #[error("rebroadcast encoding: {0}")]
    Encoding(#[from] serde_json::Error),
    /// Filesystem safety validation failed.
    #[error("rebroadcast file: {0}")]
    File(String),
    /// The database belongs to another network.
    #[error("rebroadcast database belongs to another Bitcoin network")]
    NetworkMismatch,
    /// Stored or submitted data violates queue invariants.
    #[error("malformed rebroadcast store: {0}")]
    Malformed(&'static str),
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct StoredTransaction {
    first_seen: u64,
    last_broadcast: u64,
    attempts: u32,
    confirmed: bool,
    transaction: String,
}

/// Network-bound durable queue for transactions awaiting periodic rebroadcast.
pub struct RedbRebroadcastStore {
    db: Database,
    write_guard: Mutex<()>,
}

/// Validates one raw persisted key/value pair without opening a database.
///
/// This is the production parser entry point used by bounded fuzz regression.
pub fn validate_persisted_rebroadcast_entry(
    key: &[u8],
    value: &[u8],
) -> Result<(), RebroadcastStoreError> {
    validate_stored(key, value).map(|_| ())
}

impl RedbRebroadcastStore {
    /// Opens or creates an owner-only queue and validates every persisted row.
    pub fn open(path: impl AsRef<Path>, network: Network) -> Result<Self, RebroadcastStoreError> {
        let path = path.as_ref();
        validate_file_before_open(path)?;
        let db = Database::create(path)?;
        restrict_file_permissions(path)?;
        let genesis = genesis_hash(network);
        let transaction = db.begin_write()?;
        {
            let mut meta = transaction.open_table(META)?;
            if let Some(stored) = meta.get(GENESIS_KEY)? {
                if stored.value() != genesis.as_slice() {
                    return Err(RebroadcastStoreError::NetworkMismatch);
                }
            } else {
                meta.insert(GENESIS_KEY, genesis.as_slice())?;
            }
            let entries = transaction.open_table(TRANSACTIONS)?;
            let mut count = 0_usize;
            for row in entries.iter()? {
                let (key, value) = row?;
                validate_stored(key.value(), value.value())?;
                count += 1;
                if count > MAX_STORED_TRANSACTIONS {
                    return Err(RebroadcastStoreError::Malformed(
                        "too many persisted transactions",
                    ));
                }
            }
        }
        transaction.commit()?;
        Ok(Self {
            db,
            write_guard: Mutex::new(()),
        })
    }

    /// Atomically inserts a standard non-coinbase transaction if it is new.
    pub fn enqueue(
        &self,
        transaction: &Transaction,
        now: u64,
    ) -> Result<(), RebroadcastStoreError> {
        validate_transaction(transaction, now)?;
        let _guard = self
            .write_guard
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let write = self.db.begin_write()?;
        {
            let mut entries = write.open_table(TRANSACTIONS)?;
            let key = transaction.compute_wtxid().to_byte_array();
            if entries.get(key.as_slice())?.is_none() {
                let encoded = encode_stored(&StoredTransaction {
                    first_seen: now,
                    last_broadcast: 0,
                    attempts: 0,
                    confirmed: false,
                    transaction: serialize(transaction).to_lower_hex_string(),
                })?;
                entries.insert(key.as_slice(), encoded.as_slice())?;
            }
            prune_expired_and_capacity(&mut entries, now)?;
        }
        write.commit()?;
        Ok(())
    }

    /// Returns up to `limit` due transactions without mutating their schedule.
    pub fn due(&self, now: u64, limit: usize) -> Result<Vec<Transaction>, RebroadcastStoreError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let read = self.db.begin_read()?;
        let entries = read.open_table(TRANSACTIONS)?;
        let mut due = Vec::new();
        for row in entries.iter()? {
            let (key, value) = row?;
            let stored = validate_stored(key.value(), value.value())?;
            if !stored.confirmed
                && now.saturating_sub(stored.first_seen) <= REBROADCAST_EXPIRY_SECS
                && (stored.last_broadcast == 0
                    || now.saturating_sub(stored.last_broadcast) >= REBROADCAST_INTERVAL_SECS)
            {
                due.push((
                    stored.last_broadcast != 0,
                    stored.last_broadcast,
                    stored.first_seen,
                    decode_transaction(&stored.transaction)?,
                ));
            }
        }
        due.sort_by_key(|entry| (entry.0, entry.1, entry.2));
        Ok(due
            .into_iter()
            .take(limit.min(MAX_STORED_TRANSACTIONS))
            .map(|entry| entry.3)
            .collect())
    }

    /// Records a completed active-peer socket write.
    pub fn record_broadcast(&self, wtxid: Wtxid, now: u64) -> Result<(), RebroadcastStoreError> {
        let _guard = self
            .write_guard
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let write = self.db.begin_write()?;
        {
            let mut entries = write.open_table(TRANSACTIONS)?;
            let key = wtxid.to_byte_array();
            let value = entries
                .get(key.as_slice())?
                .ok_or(RebroadcastStoreError::Malformed(
                    "broadcast transaction is not persisted",
                ))?;
            let mut stored = validate_stored(key.as_slice(), value.value())?;
            stored.last_broadcast = now.max(stored.first_seen);
            stored.attempts = stored.attempts.saturating_add(1);
            let encoded = encode_stored(&stored)?;
            drop(value);
            entries.insert(key.as_slice(), encoded.as_slice())?;
            prune_expired_and_capacity(&mut entries, now)?;
        }
        write.commit()?;
        Ok(())
    }

    /// Reconciles confirmation flags against the wallet's active-chain view.
    pub fn reconcile_confirmed(
        &self,
        confirmed_txids: &HashSet<Txid>,
        now: u64,
    ) -> Result<(), RebroadcastStoreError> {
        let _guard = self
            .write_guard
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let write = self.db.begin_write()?;
        {
            let mut entries = write.open_table(TRANSACTIONS)?;
            let rows = entries
                .iter()?
                .map(|row| {
                    let (key, value) = row?;
                    Ok::<_, RebroadcastStoreError>((
                        key.value().to_vec(),
                        validate_stored(key.value(), value.value())?,
                    ))
                })
                .collect::<Result<Vec<_>, _>>()?;
            for (key, mut stored) in rows {
                let transaction = decode_transaction(&stored.transaction)?;
                let confirmed = confirmed_txids.contains(&transaction.compute_txid());
                if stored.confirmed != confirmed {
                    stored.confirmed = confirmed;
                    let encoded = encode_stored(&stored)?;
                    entries.insert(key.as_slice(), encoded.as_slice())?;
                }
            }
            prune_expired_and_capacity(&mut entries, now)?;
        }
        write.commit()?;
        Ok(())
    }

    /// Returns all unconfirmed transactions for compact-block reconstruction.
    pub fn unconfirmed_transactions(&self) -> Result<Vec<Transaction>, RebroadcastStoreError> {
        let read = self.db.begin_read()?;
        let entries = read.open_table(TRANSACTIONS)?;
        let mut transactions = Vec::new();
        for row in entries.iter()? {
            let (key, value) = row?;
            let stored = validate_stored(key.value(), value.value())?;
            if !stored.confirmed {
                transactions.push((stored.first_seen, decode_transaction(&stored.transaction)?));
            }
        }
        transactions.sort_by_key(|entry| entry.0);
        Ok(transactions.into_iter().map(|entry| entry.1).collect())
    }

    /// Returns the txids whose confirmation state must be reconciled.
    pub fn tracked_txids(&self) -> Result<HashSet<Txid>, RebroadcastStoreError> {
        let read = self.db.begin_read()?;
        let entries = read.open_table(TRANSACTIONS)?;
        let mut txids = HashSet::new();
        for row in entries.iter()? {
            let (key, value) = row?;
            let stored = validate_stored(key.value(), value.value())?;
            txids.insert(decode_transaction(&stored.transaction)?.compute_txid());
        }
        Ok(txids)
    }

    /// Returns the number of durable rows, including confirmed retention rows.
    pub fn len(&self) -> Result<usize, RebroadcastStoreError> {
        let read = self.db.begin_read()?;
        let entries = read.open_table(TRANSACTIONS)?;
        Ok(entries.iter()?.count())
    }

    /// Returns whether the durable queue has no retained rows.
    pub fn is_empty(&self) -> Result<bool, RebroadcastStoreError> {
        self.len().map(|len| len == 0)
    }
}

fn validate_transaction(transaction: &Transaction, now: u64) -> Result<(), RebroadcastStoreError> {
    if now == 0 {
        return Err(RebroadcastStoreError::Malformed("zero first-seen time"));
    }
    if transaction.is_coinbase() {
        return Err(RebroadcastStoreError::Malformed("coinbase transaction"));
    }
    if transaction.weight().to_wu() > u64::from(bitcoin::policy::MAX_STANDARD_TX_WEIGHT) {
        return Err(RebroadcastStoreError::Malformed(
            "transaction exceeds standard weight",
        ));
    }
    let bytes = serialize(transaction);
    if bytes.is_empty() || bytes.len() > MAX_STORED_TRANSACTION_BYTES {
        return Err(RebroadcastStoreError::Malformed(
            "transaction encoding is oversized",
        ));
    }
    Ok(())
}

fn validate_stored(key: &[u8], bytes: &[u8]) -> Result<StoredTransaction, RebroadcastStoreError> {
    if key.len() != 32 || bytes.len() > MAX_STORED_VALUE_BYTES {
        return Err(RebroadcastStoreError::Malformed(
            "invalid persisted key or value length",
        ));
    }
    let stored: StoredTransaction = serde_json::from_slice(bytes)?;
    if stored.first_seen == 0
        || (stored.last_broadcast == 0) != (stored.attempts == 0)
        || (stored.last_broadcast != 0 && stored.last_broadcast < stored.first_seen)
        || stored.transaction.is_empty()
        || stored.transaction.len() > MAX_STORED_TRANSACTION_BYTES * 2
        || stored.transaction.len() % 2 != 0
    {
        return Err(RebroadcastStoreError::Malformed(
            "invalid persisted transaction metadata",
        ));
    }
    let transaction = decode_transaction(&stored.transaction)?;
    validate_transaction(&transaction, stored.first_seen)?;
    if transaction.compute_wtxid().to_byte_array().as_slice() != key {
        return Err(RebroadcastStoreError::Malformed(
            "persisted transaction key mismatch",
        ));
    }
    Ok(stored)
}

fn decode_transaction(hex: &str) -> Result<Transaction, RebroadcastStoreError> {
    let bytes = Vec::<u8>::from_hex(hex)
        .map_err(|_| RebroadcastStoreError::Malformed("invalid transaction encoding"))?;
    deserialize(&bytes)
        .map_err(|_| RebroadcastStoreError::Malformed("invalid transaction encoding"))
}

fn encode_stored(stored: &StoredTransaction) -> Result<Vec<u8>, RebroadcastStoreError> {
    let encoded = serde_json::to_vec(stored)?;
    if encoded.len() > MAX_STORED_VALUE_BYTES {
        return Err(RebroadcastStoreError::Malformed(
            "persisted transaction value is oversized",
        ));
    }
    Ok(encoded)
}

fn prune_expired_and_capacity(
    entries: &mut redb::Table<&[u8], &[u8]>,
    now: u64,
) -> Result<(), RebroadcastStoreError> {
    let mut rows = entries
        .iter()?
        .map(|row| {
            let (key, value) = row?;
            let stored = validate_stored(key.value(), value.value())?;
            Ok::<_, RebroadcastStoreError>((
                key.value().to_vec(),
                stored.confirmed,
                stored.first_seen,
            ))
        })
        .collect::<Result<Vec<_>, _>>()?;
    for (key, _, first_seen) in &rows {
        if now.saturating_sub(*first_seen) > REBROADCAST_EXPIRY_SECS {
            entries.remove(key.as_slice())?;
        }
    }
    rows.retain(|(_, _, first_seen)| now.saturating_sub(*first_seen) <= REBROADCAST_EXPIRY_SECS);
    rows.sort_by_key(|(_, confirmed, first_seen)| (!*confirmed, *first_seen));
    let excess = rows.len().saturating_sub(MAX_STORED_TRANSACTIONS);
    for (key, _, _) in rows.into_iter().take(excess) {
        entries.remove(key.as_slice())?;
    }
    Ok(())
}

fn genesis_hash(network: Network) -> [u8; 32] {
    bitcoin::blockdata::constants::genesis_block(network)
        .block_hash()
        .to_byte_array()
}

fn validate_file_before_open(path: &Path) -> Result<(), RebroadcastStoreError> {
    let Ok(metadata) = fs::symlink_metadata(path) else {
        return Ok(());
    };
    if metadata.file_type().is_symlink() || !metadata.file_type().is_file() {
        return Err(RebroadcastStoreError::File(
            "path must be a regular non-symlink file".to_owned(),
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o077 != 0 {
            return Err(RebroadcastStoreError::File(
                "permissions must deny group and other access".to_owned(),
            ));
        }
    }
    Ok(())
}

fn restrict_file_permissions(path: &Path) -> Result<(), RebroadcastStoreError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))
            .map_err(|error| RebroadcastStoreError::File(error.to_string()))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use bitcoin::{
        Amount, OutPoint, ScriptBuf, Sequence, TxIn, TxOut, Txid, Witness, absolute::LockTime,
        transaction::Version,
    };
    use tempfile::TempDir;

    use super::*;

    fn transaction(marker: u8) -> Transaction {
        Transaction {
            version: Version::TWO,
            lock_time: LockTime::ZERO,
            input: vec![TxIn {
                previous_output: OutPoint::new(Txid::from_byte_array([marker; 32]), 0),
                script_sig: ScriptBuf::new(),
                sequence: Sequence::MAX,
                witness: Witness::new(),
            }],
            output: vec![TxOut {
                value: Amount::from_sat(1_000),
                script_pubkey: ScriptBuf::new(),
            }],
        }
    }

    #[test]
    fn queue_survives_reopen_and_obeys_schedule() {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("rebroadcast.redb");
        let first = transaction(1);
        {
            let store = RedbRebroadcastStore::open(&path, Network::Regtest).unwrap();
            store.enqueue(&first, 100).unwrap();
            assert_eq!(store.due(100, 8).unwrap(), vec![first.clone()]);
            store.record_broadcast(first.compute_wtxid(), 101).unwrap();
            assert!(store.due(102, 8).unwrap().is_empty());
        }
        let reopened = RedbRebroadcastStore::open(&path, Network::Regtest).unwrap();
        assert_eq!(
            reopened.due(101 + REBROADCAST_INTERVAL_SECS, 8).unwrap(),
            vec![first]
        );
        drop(reopened);
        assert!(matches!(
            RedbRebroadcastStore::open(&path, Network::Signet),
            Err(RebroadcastStoreError::NetworkMismatch)
        ));
    }

    #[test]
    fn confirmation_suppresses_and_reorg_restores_due_transaction() {
        let directory = TempDir::new().unwrap();
        let store =
            RedbRebroadcastStore::open(directory.path().join("rebroadcast.redb"), Network::Regtest)
                .unwrap();
        let transaction = transaction(2);
        store.enqueue(&transaction, 200).unwrap();
        store
            .reconcile_confirmed(&HashSet::from([transaction.compute_txid()]), 201)
            .unwrap();
        assert!(store.due(201, 8).unwrap().is_empty());
        store.reconcile_confirmed(&HashSet::new(), 202).unwrap();
        assert_eq!(store.due(202, 8).unwrap(), vec![transaction]);
    }

    #[test]
    fn capacity_prefers_eviction_of_confirmed_then_oldest() {
        let directory = TempDir::new().unwrap();
        let store =
            RedbRebroadcastStore::open(directory.path().join("rebroadcast.redb"), Network::Regtest)
                .unwrap();
        let confirmed = transaction(1);
        store.enqueue(&confirmed, 1).unwrap();
        store
            .reconcile_confirmed(&HashSet::from([confirmed.compute_txid()]), 2)
            .unwrap();
        for marker in 2..=u8::try_from(MAX_STORED_TRANSACTIONS + 1).unwrap() {
            store
                .enqueue(&transaction(marker), u64::from(marker))
                .unwrap();
        }
        assert_eq!(store.len().unwrap(), MAX_STORED_TRANSACTIONS);
        assert!(
            !store
                .unconfirmed_transactions()
                .unwrap()
                .iter()
                .any(|candidate| candidate.compute_wtxid() == confirmed.compute_wtxid())
        );
    }

    #[test]
    fn expiry_and_duplicate_enqueue_are_bounded() {
        let directory = TempDir::new().unwrap();
        let store =
            RedbRebroadcastStore::open(directory.path().join("rebroadcast.redb"), Network::Regtest)
                .unwrap();
        let transaction = transaction(3);
        store.enqueue(&transaction, 10).unwrap();
        store.enqueue(&transaction, 11).unwrap();
        assert_eq!(store.len().unwrap(), 1);
        assert!(
            store
                .due(10 + REBROADCAST_EXPIRY_SECS + 1, 8)
                .unwrap()
                .is_empty()
        );
        store
            .reconcile_confirmed(&HashSet::new(), 10 + REBROADCAST_EXPIRY_SECS + 1)
            .unwrap();
        assert_eq!(store.len().unwrap(), 0);
    }

    #[test]
    fn malformed_persisted_row_fails_closed_without_rewrite() {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("rebroadcast.redb");
        drop(RedbRebroadcastStore::open(&path, Network::Regtest).unwrap());
        {
            let db = Database::create(&path).unwrap();
            let write = db.begin_write().unwrap();
            {
                let mut entries = write.open_table(TRANSACTIONS).unwrap();
                entries
                    .insert([0_u8; 32].as_slice(), b"{}".as_slice())
                    .unwrap();
            }
            write.commit().unwrap();
        }
        let before = fs::read(&path).unwrap();
        assert!(matches!(
            RedbRebroadcastStore::open(&path, Network::Regtest),
            Err(RebroadcastStoreError::Encoding(_) | RebroadcastStoreError::Malformed(_))
        ));
        assert_eq!(fs::read(path).unwrap(), before);
    }

    #[cfg(unix)]
    #[test]
    fn queue_file_is_owner_only_and_rejects_permissive_reopen() {
        use std::os::unix::fs::PermissionsExt;

        let directory = TempDir::new().unwrap();
        let path = directory.path().join("rebroadcast.redb");
        drop(RedbRebroadcastStore::open(&path, Network::Regtest).unwrap());
        assert_eq!(
            fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );
        fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();
        assert!(matches!(
            RedbRebroadcastStore::open(&path, Network::Regtest),
            Err(RebroadcastStoreError::File(_))
        ));
    }
}
