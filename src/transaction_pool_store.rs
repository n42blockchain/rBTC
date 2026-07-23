//! Bounded, network-bound persistence for the peer transaction admission pool.

use std::{
    collections::{BTreeMap, BTreeSet, HashSet},
    fs,
    path::Path,
    sync::Mutex,
    time::{SystemTime, UNIX_EPOCH},
};

use bitcoin::{
    Network, OutPoint, Transaction, Txid,
    consensus::{deserialize, serialize},
    hashes::Hash,
};
use redb::{Database, ReadableTable, TableDefinition};
use thiserror::Error;

use crate::transaction_admission::{MAX_ADMITTED_TRANSACTION_BYTES, MAX_ADMITTED_TRANSACTIONS};

const META: TableDefinition<&str, &[u8]> = TableDefinition::new("transaction_pool_metadata");
const SNAPSHOTS: TableDefinition<&str, &[u8]> = TableDefinition::new("transaction_pool_snapshots");
const GENESIS_KEY: &str = "genesis";
const SNAPSHOT_KEY: &str = "active";
const DISCONNECTED_KEY: &str = "disconnected";
const RELAY_ATTEMPTS_KEY: &str = "relay_attempts";
const ADMISSION_TIMES_KEY: &str = "admission_times";
const SNAPSHOT_VERSION: u8 = 1;
const SNAPSHOT_HEADER_BYTES: usize = 5;
const MAX_SNAPSHOT_BYTES: usize =
    SNAPSHOT_HEADER_BYTES + MAX_ADMITTED_TRANSACTIONS * 4 + MAX_ADMITTED_TRANSACTION_BYTES;
const RELAY_ATTEMPTS_VERSION: u8 = 1;
const RELAY_ATTEMPTS_HEADER_BYTES: usize = 5;
const RELAY_ATTEMPT_BYTES: usize = 36;
const MAX_RELAY_ATTEMPTS_BYTES: usize =
    RELAY_ATTEMPTS_HEADER_BYTES + MAX_ADMITTED_TRANSACTIONS * RELAY_ATTEMPT_BYTES;
const ADMISSION_TIMES_VERSION: u8 = 1;
const ADMISSION_TIMES_HEADER_BYTES: usize = 5;
const ADMISSION_TIME_BYTES: usize = 36;
const MAX_ADMISSION_TIMES_BYTES: usize =
    ADMISSION_TIMES_HEADER_BYTES + MAX_ADMITTED_TRANSACTIONS * ADMISSION_TIME_BYTES;

/// Bitcoin Core's default 336-hour mempool expiry.
pub const DEFAULT_MEMPOOL_EXPIRY_SECS: u32 = 14 * 24 * 60 * 60;

/// Durable transaction-pool failures.
#[derive(Debug, Error)]
pub enum TransactionPoolStoreError {
    /// Database open/create failed.
    #[error("transaction-pool database: {0}")]
    Database(#[from] redb::DatabaseError),
    /// Transaction creation failed.
    #[error("transaction-pool transaction: {0}")]
    Transaction(#[from] redb::TransactionError),
    /// Table access failed.
    #[error("transaction-pool table: {0}")]
    Table(#[from] redb::TableError),
    /// Key/value access failed.
    #[error("transaction-pool storage: {0}")]
    Storage(#[from] redb::StorageError),
    /// Transaction commit failed.
    #[error("transaction-pool commit: {0}")]
    Commit(#[from] redb::CommitError),
    /// Filesystem safety validation failed.
    #[error("transaction-pool file: {0}")]
    File(String),
    /// The system wall clock cannot be represented by the persisted format.
    #[error("transaction-pool clock: {0}")]
    Time(String),
    /// The database belongs to another network.
    #[error("transaction-pool database belongs to another Bitcoin network")]
    NetworkMismatch,
    /// Stored or submitted data violates pool invariants.
    #[error("malformed transaction-pool store: {0}")]
    Malformed(&'static str),
}

/// Network-bound durable snapshot of the bounded peer transaction pool.
pub struct RedbTransactionPoolStore {
    db: Database,
    write_guard: Mutex<()>,
}

/// Validates one raw persisted transaction-pool snapshot without opening a database.
///
/// This is the production parser entry point used by bounded fuzz regression.
pub fn validate_persisted_transaction_pool_snapshot(
    bytes: &[u8],
) -> Result<(), TransactionPoolStoreError> {
    decode_snapshot(bytes).map(|_| ())
}

/// Validates raw persisted peer-transaction relay metadata without opening a database.
pub fn validate_persisted_transaction_relay_attempts(
    bytes: &[u8],
) -> Result<(), TransactionPoolStoreError> {
    decode_relay_attempts(bytes).map(|_| ())
}

/// Validates raw persisted transaction admission times without opening a database.
pub fn validate_persisted_transaction_admission_times(
    bytes: &[u8],
) -> Result<(), TransactionPoolStoreError> {
    decode_admission_times(bytes).map(|_| ())
}

impl RedbTransactionPoolStore {
    /// Opens or creates an owner-only store and validates the persisted snapshot.
    pub fn open(
        path: impl AsRef<Path>,
        network: Network,
    ) -> Result<Self, TransactionPoolStoreError> {
        Self::open_at(path, network, current_unix_time()?)
    }

    /// Opens a store using an explicit wall-clock time for deterministic migration.
    pub fn open_at(
        path: impl AsRef<Path>,
        network: Network,
        now: u32,
    ) -> Result<Self, TransactionPoolStoreError> {
        let path = path.as_ref();
        validate_file_before_open(path)?;
        let db = Database::create(path)?;
        restrict_file_permissions(path)?;
        let genesis = genesis_hash(network);
        let write = db.begin_write()?;
        {
            let mut meta = write.open_table(META)?;
            if let Some(stored) = meta.get(GENESIS_KEY)? {
                if stored.value() != genesis.as_slice() {
                    return Err(TransactionPoolStoreError::NetworkMismatch);
                }
            } else {
                meta.insert(GENESIS_KEY, genesis.as_slice())?;
            }
            let mut snapshots = write.open_table(SNAPSHOTS)?;
            for key in [SNAPSHOT_KEY, DISCONNECTED_KEY] {
                if let Some(snapshot) = snapshots.get(key)? {
                    decode_snapshot(snapshot.value())?;
                } else {
                    let empty = encode_snapshot(&[])?;
                    snapshots.insert(key, empty.as_slice())?;
                }
            }
            let active = decode_snapshot(
                snapshots
                    .get(SNAPSHOT_KEY)?
                    .expect("active snapshot was initialized above")
                    .value(),
            )?
            .into_iter()
            .map(|transaction| transaction.compute_txid())
            .collect::<HashSet<_>>();
            if let Some(attempts) = snapshots.get(RELAY_ATTEMPTS_KEY)? {
                if !decode_relay_attempts(attempts.value())?
                    .keys()
                    .all(|txid| active.contains(txid))
                {
                    return Err(TransactionPoolStoreError::Malformed(
                        "relay attempt references a non-pool transaction",
                    ));
                }
            } else {
                let empty = encode_relay_attempts(&BTreeMap::new())?;
                snapshots.insert(RELAY_ATTEMPTS_KEY, empty.as_slice())?;
            }
            if let Some(times) = snapshots.get(ADMISSION_TIMES_KEY)? {
                let times = decode_admission_times(times.value())?;
                if times.keys().copied().collect::<HashSet<_>>() != active {
                    return Err(TransactionPoolStoreError::Malformed(
                        "admission times do not match active transactions",
                    ));
                }
            } else {
                let migrated = active
                    .iter()
                    .copied()
                    .map(|txid| (txid, now))
                    .collect::<BTreeMap<_, _>>();
                let migrated = encode_admission_times(&migrated)?;
                snapshots.insert(ADMISSION_TIMES_KEY, migrated.as_slice())?;
            }
        }
        write.commit()?;
        Ok(Self {
            db,
            write_guard: Mutex::new(()),
        })
    }

    /// Loads the complete bounded snapshot in parent-before-child insertion order.
    pub fn transactions(&self) -> Result<Vec<Transaction>, TransactionPoolStoreError> {
        self.read_snapshot(SNAPSHOT_KEY)
    }

    /// Loads bounded transactions recovered from disconnected active-chain blocks.
    pub fn disconnected_transactions(&self) -> Result<Vec<Transaction>, TransactionPoolStoreError> {
        self.read_snapshot(DISCONNECTED_KEY)
    }

    fn read_snapshot(
        &self,
        key: &'static str,
    ) -> Result<Vec<Transaction>, TransactionPoolStoreError> {
        let read = self.db.begin_read()?;
        let snapshots = read.open_table(SNAPSHOTS)?;
        let snapshot = snapshots
            .get(key)?
            .ok_or(TransactionPoolStoreError::Malformed(
                "missing transaction-pool snapshot",
            ))?;
        decode_snapshot(snapshot.value())
    }

    /// Atomically replaces the durable snapshot after validating every invariant.
    pub fn replace(&self, transactions: &[Transaction]) -> Result<(), TransactionPoolStoreError> {
        self.replace_at(transactions, current_unix_time()?)
    }

    /// Atomically replaces the durable snapshot while preserving first-admission times.
    pub fn replace_at(
        &self,
        transactions: &[Transaction],
        now: u32,
    ) -> Result<(), TransactionPoolStoreError> {
        let encoded = encode_snapshot(transactions)?;
        self.replace_active_encoded(&encoded, transactions, now, &BTreeSet::new())
    }

    /// Atomically replaces the bounded disconnected-block candidate snapshot.
    pub fn replace_disconnected(
        &self,
        transactions: &[Transaction],
    ) -> Result<(), TransactionPoolStoreError> {
        let encoded = encode_snapshot(transactions)?;
        self.replace_encoded(DISCONNECTED_KEY, &encoded)
    }

    fn replace_encoded(
        &self,
        key: &'static str,
        encoded: &[u8],
    ) -> Result<(), TransactionPoolStoreError> {
        let _guard = self
            .write_guard
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let write = self.db.begin_write()?;
        {
            let mut snapshots = write.open_table(SNAPSHOTS)?;
            snapshots.insert(key, encoded)?;
        }
        write.commit()?;
        Ok(())
    }

    fn replace_active_encoded(
        &self,
        encoded: &[u8],
        transactions: &[Transaction],
        now: u32,
        reset_admission_times: &BTreeSet<Txid>,
    ) -> Result<(), TransactionPoolStoreError> {
        let _guard = self
            .write_guard
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let write = self.db.begin_write()?;
        {
            let mut snapshots = write.open_table(SNAPSHOTS)?;
            let mut attempts = {
                let stored = snapshots.get(RELAY_ATTEMPTS_KEY)?.ok_or(
                    TransactionPoolStoreError::Malformed("missing transaction relay attempts"),
                )?;
                decode_relay_attempts(stored.value())?
            };
            let active = transactions
                .iter()
                .map(Transaction::compute_txid)
                .collect::<HashSet<_>>();
            attempts.retain(|txid, _| active.contains(txid));
            let attempts = encode_relay_attempts(&attempts)?;
            let mut admission_times = {
                let stored = snapshots.get(ADMISSION_TIMES_KEY)?.ok_or(
                    TransactionPoolStoreError::Malformed("missing transaction admission times"),
                )?;
                decode_admission_times(stored.value())?
            };
            admission_times.retain(|txid, _| active.contains(txid));
            for txid in &active {
                if reset_admission_times.contains(txid) {
                    admission_times.insert(*txid, now);
                } else {
                    admission_times.entry(*txid).or_insert(now);
                }
            }
            let admission_times = encode_admission_times(&admission_times)?;
            snapshots.insert(SNAPSHOT_KEY, encoded)?;
            snapshots.insert(RELAY_ATTEMPTS_KEY, attempts.as_slice())?;
            snapshots.insert(ADMISSION_TIMES_KEY, admission_times.as_slice())?;
        }
        write.commit()?;
        Ok(())
    }

    /// Atomically publishes the admitted pool and clears recovered reorg candidates.
    pub fn replace_and_clear_disconnected(
        &self,
        transactions: &[Transaction],
    ) -> Result<(), TransactionPoolStoreError> {
        self.replace_and_clear_disconnected_at(transactions, current_unix_time()?, &BTreeSet::new())
    }

    /// Atomically publishes the pool, clears recovery rows, and resets selected admission times.
    pub fn replace_and_clear_disconnected_at(
        &self,
        transactions: &[Transaction],
        now: u32,
        reset_admission_times: &BTreeSet<Txid>,
    ) -> Result<(), TransactionPoolStoreError> {
        let encoded = encode_snapshot(transactions)?;
        let empty = encode_snapshot(&[])?;
        self.replace_active_and_disconnected_encoded(
            &encoded,
            &empty,
            transactions,
            now,
            reset_admission_times,
        )
    }

    fn replace_active_and_disconnected_encoded(
        &self,
        active_encoded: &[u8],
        disconnected_encoded: &[u8],
        transactions: &[Transaction],
        now: u32,
        reset_admission_times: &BTreeSet<Txid>,
    ) -> Result<(), TransactionPoolStoreError> {
        let _guard = self
            .write_guard
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let write = self.db.begin_write()?;
        {
            let mut snapshots = write.open_table(SNAPSHOTS)?;
            let mut attempts = {
                let stored = snapshots.get(RELAY_ATTEMPTS_KEY)?.ok_or(
                    TransactionPoolStoreError::Malformed("missing transaction relay attempts"),
                )?;
                decode_relay_attempts(stored.value())?
            };
            let active = transactions
                .iter()
                .map(Transaction::compute_txid)
                .collect::<HashSet<_>>();
            attempts.retain(|txid, _| active.contains(txid));
            let attempts = encode_relay_attempts(&attempts)?;
            let mut admission_times = {
                let stored = snapshots.get(ADMISSION_TIMES_KEY)?.ok_or(
                    TransactionPoolStoreError::Malformed("missing transaction admission times"),
                )?;
                decode_admission_times(stored.value())?
            };
            admission_times.retain(|txid, _| active.contains(txid));
            for txid in &active {
                if reset_admission_times.contains(txid) {
                    admission_times.insert(*txid, now);
                } else {
                    admission_times.entry(*txid).or_insert(now);
                }
            }
            let admission_times = encode_admission_times(&admission_times)?;
            snapshots.insert(SNAPSHOT_KEY, active_encoded)?;
            snapshots.insert(DISCONNECTED_KEY, disconnected_encoded)?;
            snapshots.insert(RELAY_ATTEMPTS_KEY, attempts.as_slice())?;
            snapshots.insert(ADMISSION_TIMES_KEY, admission_times.as_slice())?;
        }
        write.commit()?;
        Ok(())
    }

    /// Returns parent-ordered transactions whose last successful standby publication is due.
    pub fn due_relay(
        &self,
        now: u32,
        interval_secs: u32,
        limit: usize,
    ) -> Result<Vec<Transaction>, TransactionPoolStoreError> {
        let read = self.db.begin_read()?;
        let snapshots = read.open_table(SNAPSHOTS)?;
        let transactions = decode_snapshot(
            snapshots
                .get(SNAPSHOT_KEY)?
                .ok_or(TransactionPoolStoreError::Malformed(
                    "missing transaction-pool snapshot",
                ))?
                .value(),
        )?;
        let attempts = decode_relay_attempts(
            snapshots
                .get(RELAY_ATTEMPTS_KEY)?
                .ok_or(TransactionPoolStoreError::Malformed(
                    "missing transaction relay attempts",
                ))?
                .value(),
        )?;
        Ok(transactions
            .into_iter()
            .filter(|transaction| {
                attempts
                    .get(&transaction.compute_txid())
                    .is_none_or(|attempt| now.saturating_sub(*attempt) >= interval_secs)
            })
            .take(limit.min(MAX_ADMITTED_TRANSACTIONS))
            .collect())
    }

    /// Returns active transaction IDs older than the configured persisted lifetime.
    pub fn expired_txids(
        &self,
        now: u32,
        expiry_secs: u32,
    ) -> Result<BTreeSet<Txid>, TransactionPoolStoreError> {
        let read = self.db.begin_read()?;
        let snapshots = read.open_table(SNAPSHOTS)?;
        let active = decode_snapshot(
            snapshots
                .get(SNAPSHOT_KEY)?
                .ok_or(TransactionPoolStoreError::Malformed(
                    "missing transaction-pool snapshot",
                ))?
                .value(),
        )?
        .into_iter()
        .map(|transaction| transaction.compute_txid())
        .collect::<BTreeSet<_>>();
        let admission_times = decode_admission_times(
            snapshots
                .get(ADMISSION_TIMES_KEY)?
                .ok_or(TransactionPoolStoreError::Malformed(
                    "missing transaction admission times",
                ))?
                .value(),
        )?;
        if admission_times.keys().copied().collect::<BTreeSet<_>>() != active {
            return Err(TransactionPoolStoreError::Malformed(
                "admission times do not match active transactions",
            ));
        }
        Ok(admission_times
            .into_iter()
            .filter_map(|(txid, admitted_at)| {
                (now.saturating_sub(admitted_at) > expiry_secs).then_some(txid)
            })
            .collect())
    }

    /// Atomically records successful publication attempts for active pool transactions.
    pub fn record_relay_attempts(
        &self,
        txids: &[Txid],
        now: u32,
    ) -> Result<usize, TransactionPoolStoreError> {
        if txids.len() > MAX_ADMITTED_TRANSACTIONS {
            return Err(TransactionPoolStoreError::Malformed(
                "too many transaction relay attempts",
            ));
        }
        let requested = txids.iter().copied().collect::<HashSet<_>>();
        if requested.len() != txids.len() {
            return Err(TransactionPoolStoreError::Malformed(
                "duplicate transaction relay attempt",
            ));
        }
        let _guard = self
            .write_guard
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let write = self.db.begin_write()?;
        let recorded = {
            let mut snapshots = write.open_table(SNAPSHOTS)?;
            let active =
                {
                    let stored = snapshots.get(SNAPSHOT_KEY)?.ok_or(
                        TransactionPoolStoreError::Malformed("missing transaction-pool snapshot"),
                    )?;
                    decode_snapshot(stored.value())?
                        .into_iter()
                        .map(|transaction| transaction.compute_txid())
                        .collect::<HashSet<_>>()
                };
            if !requested.is_subset(&active) {
                return Err(TransactionPoolStoreError::Malformed(
                    "relay attempt references a non-pool transaction",
                ));
            }
            let mut attempts = {
                let stored = snapshots.get(RELAY_ATTEMPTS_KEY)?.ok_or(
                    TransactionPoolStoreError::Malformed("missing transaction relay attempts"),
                )?;
                decode_relay_attempts(stored.value())?
            };
            attempts.retain(|txid, _| active.contains(txid));
            for txid in requested {
                attempts.insert(txid, now);
            }
            let recorded = attempts.len();
            let encoded = encode_relay_attempts(&attempts)?;
            snapshots.insert(RELAY_ATTEMPTS_KEY, encoded.as_slice())?;
            recorded
        };
        write.commit()?;
        Ok(recorded)
    }
}

fn encode_snapshot(transactions: &[Transaction]) -> Result<Vec<u8>, TransactionPoolStoreError> {
    validate_transactions(transactions)?;
    let count = u32::try_from(transactions.len())
        .map_err(|_| TransactionPoolStoreError::Malformed("transaction count overflow"))?;
    let mut encoded = Vec::with_capacity(
        SNAPSHOT_HEADER_BYTES
            + transactions.len() * 4
            + transactions
                .iter()
                .map(|transaction| serialize(transaction).len())
                .sum::<usize>(),
    );
    encoded.push(SNAPSHOT_VERSION);
    encoded.extend_from_slice(&count.to_le_bytes());
    for transaction in transactions {
        let raw = serialize(transaction);
        let len = u32::try_from(raw.len())
            .map_err(|_| TransactionPoolStoreError::Malformed("transaction length overflow"))?;
        encoded.extend_from_slice(&len.to_le_bytes());
        encoded.extend_from_slice(&raw);
    }
    if encoded.len() > MAX_SNAPSHOT_BYTES {
        return Err(TransactionPoolStoreError::Malformed(
            "transaction-pool snapshot is oversized",
        ));
    }
    Ok(encoded)
}

fn decode_snapshot(bytes: &[u8]) -> Result<Vec<Transaction>, TransactionPoolStoreError> {
    if bytes.len() < SNAPSHOT_HEADER_BYTES || bytes.len() > MAX_SNAPSHOT_BYTES {
        return Err(TransactionPoolStoreError::Malformed(
            "invalid transaction-pool snapshot length",
        ));
    }
    if bytes[0] != SNAPSHOT_VERSION {
        return Err(TransactionPoolStoreError::Malformed(
            "unsupported transaction-pool snapshot version",
        ));
    }
    let count = usize::try_from(u32::from_le_bytes(
        bytes[1..SNAPSHOT_HEADER_BYTES]
            .try_into()
            .expect("snapshot header has exact length"),
    ))
    .map_err(|_| TransactionPoolStoreError::Malformed("transaction count overflow"))?;
    if count > MAX_ADMITTED_TRANSACTIONS {
        return Err(TransactionPoolStoreError::Malformed(
            "too many persisted transactions",
        ));
    }
    let mut cursor = SNAPSHOT_HEADER_BYTES;
    let mut transactions = Vec::with_capacity(count);
    let mut serialized_bytes = 0_usize;
    for _ in 0..count {
        let length_end = cursor
            .checked_add(4)
            .filter(|end| *end <= bytes.len())
            .ok_or(TransactionPoolStoreError::Malformed(
                "truncated persisted transaction length",
            ))?;
        let transaction_len = usize::try_from(u32::from_le_bytes(
            bytes[cursor..length_end]
                .try_into()
                .expect("transaction length has exact width"),
        ))
        .map_err(|_| TransactionPoolStoreError::Malformed("transaction length overflow"))?;
        cursor = length_end;
        serialized_bytes = serialized_bytes
            .checked_add(transaction_len)
            .filter(|total| *total <= MAX_ADMITTED_TRANSACTION_BYTES)
            .ok_or(TransactionPoolStoreError::Malformed(
                "persisted transactions exceed the byte limit",
            ))?;
        let transaction_end = cursor
            .checked_add(transaction_len)
            .filter(|end| *end <= bytes.len())
            .ok_or(TransactionPoolStoreError::Malformed(
                "truncated persisted transaction",
            ))?;
        if transaction_len == 0 {
            return Err(TransactionPoolStoreError::Malformed(
                "empty persisted transaction",
            ));
        }
        let transaction = deserialize::<Transaction>(&bytes[cursor..transaction_end])
            .map_err(|_| TransactionPoolStoreError::Malformed("invalid transaction encoding"))?;
        if serialize(&transaction).as_slice() != &bytes[cursor..transaction_end] {
            return Err(TransactionPoolStoreError::Malformed(
                "noncanonical transaction encoding",
            ));
        }
        transactions.push(transaction);
        cursor = transaction_end;
    }
    if cursor != bytes.len() {
        return Err(TransactionPoolStoreError::Malformed(
            "trailing transaction-pool snapshot bytes",
        ));
    }
    validate_transactions(&transactions)?;
    Ok(transactions)
}

fn validate_transactions(transactions: &[Transaction]) -> Result<(), TransactionPoolStoreError> {
    if transactions.len() > MAX_ADMITTED_TRANSACTIONS {
        return Err(TransactionPoolStoreError::Malformed(
            "too many persisted transactions",
        ));
    }
    let mut retained_bytes = 0_usize;
    let mut all_txids = HashSet::with_capacity(transactions.len());
    for transaction in transactions {
        if transaction.is_coinbase() {
            return Err(TransactionPoolStoreError::Malformed(
                "persisted coinbase transaction",
            ));
        }
        if transaction.weight().to_wu() > u64::from(bitcoin::policy::MAX_STANDARD_TX_WEIGHT) {
            return Err(TransactionPoolStoreError::Malformed(
                "persisted transaction exceeds standard weight",
            ));
        }
        retained_bytes = retained_bytes
            .checked_add(serialize(transaction).len())
            .filter(|total| *total <= MAX_ADMITTED_TRANSACTION_BYTES)
            .ok_or(TransactionPoolStoreError::Malformed(
                "persisted transactions exceed the byte limit",
            ))?;
        if !all_txids.insert(transaction.compute_txid()) {
            return Err(TransactionPoolStoreError::Malformed(
                "duplicate persisted transaction ID",
            ));
        }
    }

    let mut seen_txids = HashSet::<Txid>::with_capacity(transactions.len());
    let mut spent = BTreeSet::<OutPoint>::new();
    for transaction in transactions {
        for input in &transaction.input {
            if all_txids.contains(&input.previous_output.txid)
                && !seen_txids.contains(&input.previous_output.txid)
            {
                return Err(TransactionPoolStoreError::Malformed(
                    "persisted child precedes its parent",
                ));
            }
            if !spent.insert(input.previous_output) {
                return Err(TransactionPoolStoreError::Malformed(
                    "persisted transactions contain an input conflict",
                ));
            }
        }
        seen_txids.insert(transaction.compute_txid());
    }
    Ok(())
}

fn encode_relay_attempts(
    attempts: &BTreeMap<Txid, u32>,
) -> Result<Vec<u8>, TransactionPoolStoreError> {
    if attempts.len() > MAX_ADMITTED_TRANSACTIONS {
        return Err(TransactionPoolStoreError::Malformed(
            "too many transaction relay attempts",
        ));
    }
    let count = u32::try_from(attempts.len())
        .map_err(|_| TransactionPoolStoreError::Malformed("relay attempt count overflow"))?;
    let mut encoded =
        Vec::with_capacity(RELAY_ATTEMPTS_HEADER_BYTES + attempts.len() * RELAY_ATTEMPT_BYTES);
    encoded.push(RELAY_ATTEMPTS_VERSION);
    encoded.extend_from_slice(&count.to_le_bytes());
    for (txid, attempted_at) in attempts {
        encoded.extend_from_slice(&txid.to_byte_array());
        encoded.extend_from_slice(&attempted_at.to_le_bytes());
    }
    Ok(encoded)
}

fn decode_relay_attempts(bytes: &[u8]) -> Result<BTreeMap<Txid, u32>, TransactionPoolStoreError> {
    if bytes.len() < RELAY_ATTEMPTS_HEADER_BYTES || bytes.len() > MAX_RELAY_ATTEMPTS_BYTES {
        return Err(TransactionPoolStoreError::Malformed(
            "invalid transaction relay attempts length",
        ));
    }
    if bytes[0] != RELAY_ATTEMPTS_VERSION {
        return Err(TransactionPoolStoreError::Malformed(
            "unsupported transaction relay attempts version",
        ));
    }
    let count = usize::try_from(u32::from_le_bytes(
        bytes[1..RELAY_ATTEMPTS_HEADER_BYTES]
            .try_into()
            .expect("relay attempts header has exact length"),
    ))
    .map_err(|_| TransactionPoolStoreError::Malformed("relay attempt count overflow"))?;
    if count > MAX_ADMITTED_TRANSACTIONS
        || bytes.len() != RELAY_ATTEMPTS_HEADER_BYTES + count * RELAY_ATTEMPT_BYTES
    {
        return Err(TransactionPoolStoreError::Malformed(
            "invalid transaction relay attempt count",
        ));
    }
    let mut attempts = BTreeMap::new();
    let mut cursor = RELAY_ATTEMPTS_HEADER_BYTES;
    for _ in 0..count {
        let txid = Txid::from_byte_array(
            bytes[cursor..cursor + 32]
                .try_into()
                .expect("relay txid has exact length"),
        );
        cursor += 32;
        let attempted_at = u32::from_le_bytes(
            bytes[cursor..cursor + 4]
                .try_into()
                .expect("relay timestamp has exact length"),
        );
        cursor += 4;
        if attempts.insert(txid, attempted_at).is_some() {
            return Err(TransactionPoolStoreError::Malformed(
                "duplicate transaction relay attempt",
            ));
        }
    }
    Ok(attempts)
}

fn encode_admission_times(
    admission_times: &BTreeMap<Txid, u32>,
) -> Result<Vec<u8>, TransactionPoolStoreError> {
    if admission_times.len() > MAX_ADMITTED_TRANSACTIONS {
        return Err(TransactionPoolStoreError::Malformed(
            "too many transaction admission times",
        ));
    }
    let count = u32::try_from(admission_times.len())
        .map_err(|_| TransactionPoolStoreError::Malformed("admission time count overflow"))?;
    let mut encoded = Vec::with_capacity(
        ADMISSION_TIMES_HEADER_BYTES + admission_times.len() * ADMISSION_TIME_BYTES,
    );
    encoded.push(ADMISSION_TIMES_VERSION);
    encoded.extend_from_slice(&count.to_le_bytes());
    for (txid, admitted_at) in admission_times {
        encoded.extend_from_slice(&txid.to_byte_array());
        encoded.extend_from_slice(&admitted_at.to_le_bytes());
    }
    Ok(encoded)
}

fn decode_admission_times(bytes: &[u8]) -> Result<BTreeMap<Txid, u32>, TransactionPoolStoreError> {
    if bytes.len() < ADMISSION_TIMES_HEADER_BYTES || bytes.len() > MAX_ADMISSION_TIMES_BYTES {
        return Err(TransactionPoolStoreError::Malformed(
            "invalid transaction admission times length",
        ));
    }
    if bytes[0] != ADMISSION_TIMES_VERSION {
        return Err(TransactionPoolStoreError::Malformed(
            "unsupported transaction admission times version",
        ));
    }
    let count = usize::try_from(u32::from_le_bytes(
        bytes[1..ADMISSION_TIMES_HEADER_BYTES]
            .try_into()
            .expect("admission times header has exact length"),
    ))
    .map_err(|_| TransactionPoolStoreError::Malformed("admission time count overflow"))?;
    if count > MAX_ADMITTED_TRANSACTIONS
        || bytes.len() != ADMISSION_TIMES_HEADER_BYTES + count * ADMISSION_TIME_BYTES
    {
        return Err(TransactionPoolStoreError::Malformed(
            "invalid transaction admission time count",
        ));
    }
    let mut admission_times = BTreeMap::new();
    let mut cursor = ADMISSION_TIMES_HEADER_BYTES;
    for _ in 0..count {
        let txid = Txid::from_byte_array(
            bytes[cursor..cursor + 32]
                .try_into()
                .expect("admission txid has exact length"),
        );
        cursor += 32;
        let admitted_at = u32::from_le_bytes(
            bytes[cursor..cursor + 4]
                .try_into()
                .expect("admission timestamp has exact length"),
        );
        cursor += 4;
        if admission_times.insert(txid, admitted_at).is_some() {
            return Err(TransactionPoolStoreError::Malformed(
                "duplicate transaction admission time",
            ));
        }
    }
    Ok(admission_times)
}

fn current_unix_time() -> Result<u32, TransactionPoolStoreError> {
    let seconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| TransactionPoolStoreError::Time(error.to_string()))?
        .as_secs();
    u32::try_from(seconds)
        .map_err(|_| TransactionPoolStoreError::Time("Unix time exceeds u32".to_owned()))
}

fn genesis_hash(network: Network) -> [u8; 32] {
    bitcoin::blockdata::constants::genesis_block(network)
        .block_hash()
        .to_byte_array()
}

fn validate_file_before_open(path: &Path) -> Result<(), TransactionPoolStoreError> {
    let Ok(metadata) = fs::symlink_metadata(path) else {
        return Ok(());
    };
    if metadata.file_type().is_symlink() || !metadata.file_type().is_file() {
        return Err(TransactionPoolStoreError::File(
            "path must be a regular non-symlink file".to_owned(),
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o077 != 0 {
            return Err(TransactionPoolStoreError::File(
                "permissions must deny group and other access".to_owned(),
            ));
        }
    }
    Ok(())
}

fn restrict_file_permissions(path: &Path) -> Result<(), TransactionPoolStoreError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))
            .map_err(|error| TransactionPoolStoreError::File(error.to_string()))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use bitcoin::{
        Amount, ScriptBuf, Sequence, TxIn, TxOut, Txid, Witness, absolute::LockTime,
        blockdata::script::Builder, opcodes, transaction::Version,
    };
    use tempfile::TempDir;

    use crate::{
        transaction_admission::{TransactionAdmissionContext, TransactionAdmissionPool},
        utxo::{RedbUtxoStore, Utxo, UtxoStore},
    };

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

    fn child(parent: &Transaction) -> Transaction {
        let mut child = transaction(200);
        child.input[0].previous_output = OutPoint::new(parent.compute_txid(), 0);
        child
    }

    fn admissible_transaction(marker: u8) -> (OutPoint, Utxo, Transaction) {
        let witness_script = Builder::new().push_opcode(opcodes::OP_TRUE).into_script();
        let outpoint = OutPoint::new(Txid::from_byte_array([marker; 32]), 0);
        let utxo = Utxo {
            value_sats: 100_000,
            height: 1,
            is_coinbase: false,
            last_touched: 0,
            creation_mtp: 1,
            script_pubkey: ScriptBuf::new_p2wsh(&witness_script.wscript_hash()).into_bytes(),
        };
        let transaction = Transaction {
            version: Version::TWO,
            lock_time: LockTime::ZERO,
            input: vec![TxIn {
                previous_output: outpoint,
                script_sig: ScriptBuf::new(),
                sequence: Sequence::MAX,
                witness: Witness::from_slice(&[witness_script.as_bytes()]),
            }],
            output: vec![TxOut {
                value: Amount::from_sat(90_000),
                script_pubkey: ScriptBuf::new_p2wsh(&witness_script.wscript_hash()),
            }],
        };
        (outpoint, utxo, transaction)
    }

    fn admission_context() -> TransactionAdmissionContext {
        TransactionAdmissionContext {
            height: 200,
            parent_mtp: 1_700_000_000,
            script_flags: bitcoinconsensus::VERIFY_P2SH | bitcoinconsensus::VERIFY_WITNESS,
            csv_active: true,
            full_rbf: false,
        }
    }

    #[test]
    fn snapshot_survives_reopen_and_is_network_bound() {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("mempool.redb");
        let parent = transaction(1);
        let child = child(&parent);
        {
            let store = RedbTransactionPoolStore::open(&path, Network::Regtest).unwrap();
            store.replace(&[parent.clone(), child.clone()]).unwrap();
            store
                .replace_disconnected(std::slice::from_ref(&parent))
                .unwrap();
            assert_eq!(
                store.transactions().unwrap(),
                vec![parent.clone(), child.clone()]
            );
        }
        let reopened = RedbTransactionPoolStore::open(&path, Network::Regtest).unwrap();
        assert_eq!(reopened.transactions().unwrap(), vec![parent, child]);
        assert_eq!(reopened.disconnected_transactions().unwrap().len(), 1);
        let admitted = reopened.transactions().unwrap();
        reopened.replace_and_clear_disconnected(&admitted).unwrap();
        assert!(reopened.disconnected_transactions().unwrap().is_empty());
        drop(reopened);
        assert!(matches!(
            RedbTransactionPoolStore::open(&path, Network::Signet),
            Err(TransactionPoolStoreError::NetworkMismatch)
        ));
    }

    #[test]
    fn peer_relay_schedule_is_due_persistent_and_pruned_with_the_pool() {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("mempool.redb");
        let first = transaction(31);
        let second = transaction(32);
        let first_txid = first.compute_txid();
        let second_txid = second.compute_txid();
        let interval = 12 * 60 * 60;
        {
            let store = RedbTransactionPoolStore::open(&path, Network::Regtest).unwrap();
            store.replace(&[first.clone(), second.clone()]).unwrap();
            assert_eq!(
                store.due_relay(100, interval, 64).unwrap(),
                vec![first.clone(), second.clone()]
            );
            assert_eq!(store.record_relay_attempts(&[first_txid], 100).unwrap(), 1);
            assert_eq!(
                store.due_relay(100 + interval - 1, interval, 64).unwrap(),
                vec![second.clone()]
            );
            assert_eq!(store.record_relay_attempts(&[second_txid], 101).unwrap(), 2);
            assert!(store.due_relay(102, interval, 64).unwrap().is_empty());
        }

        let reopened = RedbTransactionPoolStore::open(&path, Network::Regtest).unwrap();
        assert_eq!(
            reopened.due_relay(100 + interval, interval, 1).unwrap(),
            vec![first]
        );
        reopened.replace(std::slice::from_ref(&second)).unwrap();
        assert!(matches!(
            reopened.record_relay_attempts(&[first_txid], 200),
            Err(TransactionPoolStoreError::Malformed(
                "relay attempt references a non-pool transaction"
            ))
        ));
        assert!(reopened.due_relay(102, interval, 64).unwrap().is_empty());
    }

    #[test]
    fn admission_times_persist_expire_reset_and_prune_with_the_pool() {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("mempool.redb");
        let first = transaction(41);
        let second = transaction(42);
        let third = transaction(43);
        let first_txid = first.compute_txid();
        let second_txid = second.compute_txid();
        let third_txid = third.compute_txid();
        {
            let store = RedbTransactionPoolStore::open_at(&path, Network::Regtest, 100).unwrap();
            store.replace_at(&[first, second.clone()], 100).unwrap();
            assert!(
                store
                    .expired_txids(
                        100 + DEFAULT_MEMPOOL_EXPIRY_SECS - 1,
                        DEFAULT_MEMPOOL_EXPIRY_SECS
                    )
                    .unwrap()
                    .is_empty()
            );
            assert_eq!(
                store
                    .expired_txids(
                        100 + DEFAULT_MEMPOOL_EXPIRY_SECS,
                        DEFAULT_MEMPOOL_EXPIRY_SECS
                    )
                    .unwrap(),
                BTreeSet::new()
            );
            assert_eq!(
                store
                    .expired_txids(
                        100 + DEFAULT_MEMPOOL_EXPIRY_SECS + 1,
                        DEFAULT_MEMPOOL_EXPIRY_SECS
                    )
                    .unwrap(),
                BTreeSet::from([first_txid, second_txid])
            );
            store
                .replace_at(&[second.clone(), third.clone()], 200)
                .unwrap();
            assert_eq!(
                store
                    .expired_txids(
                        100 + DEFAULT_MEMPOOL_EXPIRY_SECS,
                        DEFAULT_MEMPOOL_EXPIRY_SECS
                    )
                    .unwrap(),
                BTreeSet::new()
            );
            assert_eq!(
                store
                    .expired_txids(
                        100 + DEFAULT_MEMPOOL_EXPIRY_SECS + 1,
                        DEFAULT_MEMPOOL_EXPIRY_SECS
                    )
                    .unwrap(),
                BTreeSet::from([second_txid])
            );
        }

        let now = 100 + DEFAULT_MEMPOOL_EXPIRY_SECS;
        let reopened = RedbTransactionPoolStore::open_at(&path, Network::Regtest, now).unwrap();
        reopened
            .replace_and_clear_disconnected_at(
                &[second, third],
                now,
                &BTreeSet::from([second_txid]),
            )
            .unwrap();
        assert!(
            reopened
                .expired_txids(now, DEFAULT_MEMPOOL_EXPIRY_SECS)
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            reopened
                .expired_txids(
                    200 + DEFAULT_MEMPOOL_EXPIRY_SECS,
                    DEFAULT_MEMPOOL_EXPIRY_SECS
                )
                .unwrap(),
            BTreeSet::new()
        );
        assert_eq!(
            reopened
                .expired_txids(
                    200 + DEFAULT_MEMPOOL_EXPIRY_SECS + 1,
                    DEFAULT_MEMPOOL_EXPIRY_SECS
                )
                .unwrap(),
            BTreeSet::from([third_txid])
        );
    }

    #[test]
    fn legacy_store_without_admission_times_migrates_active_entries_as_new() {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("mempool.redb");
        let transaction = transaction(44);
        let txid = transaction.compute_txid();
        {
            let store = RedbTransactionPoolStore::open_at(&path, Network::Regtest, 100).unwrap();
            store.replace_at(&[transaction], 100).unwrap();
        }
        {
            let db = Database::create(&path).unwrap();
            let write = db.begin_write().unwrap();
            {
                let mut snapshots = write.open_table(SNAPSHOTS).unwrap();
                snapshots.remove(ADMISSION_TIMES_KEY).unwrap();
            }
            write.commit().unwrap();
        }

        let migrated = RedbTransactionPoolStore::open_at(&path, Network::Regtest, 500).unwrap();
        assert!(
            migrated
                .expired_txids(
                    500 + DEFAULT_MEMPOOL_EXPIRY_SECS - 1,
                    DEFAULT_MEMPOOL_EXPIRY_SECS
                )
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            migrated
                .expired_txids(
                    500 + DEFAULT_MEMPOOL_EXPIRY_SECS,
                    DEFAULT_MEMPOOL_EXPIRY_SECS
                )
                .unwrap(),
            BTreeSet::new()
        );
        assert_eq!(
            migrated
                .expired_txids(
                    500 + DEFAULT_MEMPOOL_EXPIRY_SECS + 1,
                    DEFAULT_MEMPOOL_EXPIRY_SECS
                )
                .unwrap(),
            BTreeSet::from([txid])
        );
    }

    #[test]
    fn admission_times_parser_rejects_duplicates_and_bad_lengths() {
        let txid = transaction(45).compute_txid();
        let valid = encode_admission_times(&BTreeMap::from([(txid, 1)])).unwrap();
        assert_eq!(decode_admission_times(&valid).unwrap()[&txid], 1);
        assert!(decode_admission_times(&valid[..valid.len() - 1]).is_err());
        let mut duplicate = valid.clone();
        duplicate[1..5].copy_from_slice(&2_u32.to_le_bytes());
        duplicate.extend_from_slice(&txid.to_byte_array());
        duplicate.extend_from_slice(&2_u32.to_le_bytes());
        assert!(matches!(
            decode_admission_times(&duplicate),
            Err(TransactionPoolStoreError::Malformed(
                "duplicate transaction admission time"
            ))
        ));
    }

    #[test]
    fn admission_times_must_exactly_match_active_pool_without_rewrite() {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("mempool.redb");
        {
            let store = RedbTransactionPoolStore::open_at(&path, Network::Regtest, 100).unwrap();
            store.replace_at(&[transaction(46)], 100).unwrap();
        }
        {
            let db = Database::create(&path).unwrap();
            let write = db.begin_write().unwrap();
            {
                let mut snapshots = write.open_table(SNAPSHOTS).unwrap();
                let unknown = BTreeMap::from([(transaction(47).compute_txid(), 100)]);
                let encoded = encode_admission_times(&unknown).unwrap();
                snapshots
                    .insert(ADMISSION_TIMES_KEY, encoded.as_slice())
                    .unwrap();
            }
            write.commit().unwrap();
        }
        let before = fs::read(&path).unwrap();
        assert!(matches!(
            RedbTransactionPoolStore::open_at(&path, Network::Regtest, 200),
            Err(TransactionPoolStoreError::Malformed(
                "admission times do not match active transactions"
            ))
        ));
        assert_eq!(fs::read(path).unwrap(), before);
    }

    #[test]
    fn legacy_store_without_relay_schedule_migrates_to_immediately_due() {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("mempool.redb");
        let transaction = transaction(33);
        {
            let store = RedbTransactionPoolStore::open(&path, Network::Regtest).unwrap();
            store.replace(std::slice::from_ref(&transaction)).unwrap();
        }
        {
            let db = Database::create(&path).unwrap();
            let write = db.begin_write().unwrap();
            {
                let mut snapshots = write.open_table(SNAPSHOTS).unwrap();
                snapshots.remove(RELAY_ATTEMPTS_KEY).unwrap();
            }
            write.commit().unwrap();
        }

        let migrated = RedbTransactionPoolStore::open(&path, Network::Regtest).unwrap();
        assert_eq!(migrated.due_relay(1, 1, 64).unwrap(), vec![transaction]);
    }

    #[test]
    fn relay_schedule_parser_rejects_duplicates_and_bad_lengths() {
        let txid = transaction(34).compute_txid();
        let valid = encode_relay_attempts(&BTreeMap::from([(txid, 1)])).unwrap();
        assert_eq!(decode_relay_attempts(&valid).unwrap()[&txid], 1);
        assert!(decode_relay_attempts(&valid[..valid.len() - 1]).is_err());
        let mut duplicate = valid.clone();
        duplicate[1..5].copy_from_slice(&2_u32.to_le_bytes());
        duplicate.extend_from_slice(&txid.to_byte_array());
        duplicate.extend_from_slice(&2_u32.to_le_bytes());
        assert!(matches!(
            decode_relay_attempts(&duplicate),
            Err(TransactionPoolStoreError::Malformed(
                "duplicate transaction relay attempt"
            ))
        ));
    }

    #[test]
    fn relay_schedule_rejects_non_pool_txids_without_rewriting() {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("mempool.redb");
        {
            let store = RedbTransactionPoolStore::open(&path, Network::Regtest).unwrap();
            store.replace(&[transaction(35)]).unwrap();
        }
        {
            let db = Database::create(&path).unwrap();
            let write = db.begin_write().unwrap();
            {
                let mut snapshots = write.open_table(SNAPSHOTS).unwrap();
                let unknown = BTreeMap::from([(transaction(36).compute_txid(), 1_800_000_000_u32)]);
                let encoded = encode_relay_attempts(&unknown).unwrap();
                snapshots
                    .insert(RELAY_ATTEMPTS_KEY, encoded.as_slice())
                    .unwrap();
            }
            write.commit().unwrap();
        }
        let before = fs::read(&path).unwrap();
        assert!(matches!(
            RedbTransactionPoolStore::open(&path, Network::Regtest),
            Err(TransactionPoolStoreError::Malformed(
                "relay attempt references a non-pool transaction"
            ))
        ));
        assert_eq!(fs::read(path).unwrap(), before);
    }

    #[test]
    fn restart_revalidates_against_chainstate_and_durably_removes_stale_entries() {
        let directory = TempDir::new().unwrap();
        let chainstate = RedbUtxoStore::open(directory.path().join("chainstate.redb")).unwrap();
        let pool_path = directory.path().join("mempool.redb");
        let (outpoint, utxo, transaction) = admissible_transaction(9);
        chainstate.apply(&[], &[(outpoint.into(), utxo)]).unwrap();

        let mut original = TransactionAdmissionPool::default();
        original
            .admit(&chainstate, transaction.clone(), admission_context())
            .unwrap();
        {
            let persistent = RedbTransactionPoolStore::open(&pool_path, Network::Regtest).unwrap();
            persistent.replace(&original.snapshot()).unwrap();
        }

        let persistent = RedbTransactionPoolStore::open(&pool_path, Network::Regtest).unwrap();
        let mut restored = TransactionAdmissionPool::default();
        for transaction in persistent.transactions().unwrap() {
            restored
                .admit(&chainstate, transaction, admission_context())
                .unwrap();
        }
        assert_eq!(restored.len(), 1);

        chainstate.apply(&[outpoint.into()], &[]).unwrap();
        let mut after_chain_change = TransactionAdmissionPool::default();
        for transaction in persistent.transactions().unwrap() {
            assert!(
                after_chain_change
                    .admit(&chainstate, transaction, admission_context())
                    .is_err()
            );
        }
        persistent.replace(&after_chain_change.snapshot()).unwrap();
        drop(persistent);
        assert!(
            RedbTransactionPoolStore::open(&pool_path, Network::Regtest)
                .unwrap()
                .transactions()
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn replacement_rejects_child_first_and_input_conflicts() {
        let directory = TempDir::new().unwrap();
        let store =
            RedbTransactionPoolStore::open(directory.path().join("mempool.redb"), Network::Regtest)
                .unwrap();
        let parent = transaction(2);
        let child = child(&parent);
        assert!(matches!(
            store.replace(&[child, parent.clone()]),
            Err(TransactionPoolStoreError::Malformed(
                "persisted child precedes its parent"
            ))
        ));
        let mut conflict = parent.clone();
        conflict.output[0].value = Amount::from_sat(999);
        assert!(matches!(
            store.replace(&[parent, conflict]),
            Err(TransactionPoolStoreError::Malformed(
                "persisted transactions contain an input conflict"
            ))
        ));
        assert!(store.transactions().unwrap().is_empty());
    }

    #[test]
    fn malformed_snapshot_fails_closed_without_rewrite() {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("mempool.redb");
        drop(RedbTransactionPoolStore::open(&path, Network::Regtest).unwrap());
        {
            let db = Database::create(&path).unwrap();
            let write = db.begin_write().unwrap();
            {
                let mut snapshots = write.open_table(SNAPSHOTS).unwrap();
                snapshots
                    .insert(SNAPSHOT_KEY, b"\x01\x01".as_slice())
                    .unwrap();
            }
            write.commit().unwrap();
        }
        let before = fs::read(&path).unwrap();
        assert!(matches!(
            RedbTransactionPoolStore::open(&path, Network::Regtest),
            Err(TransactionPoolStoreError::Malformed(_))
        ));
        assert_eq!(fs::read(path).unwrap(), before);
    }

    #[test]
    fn parser_rejects_truncation_trailing_bytes_and_oversized_counts() {
        let transaction = transaction(3);
        let valid = encode_snapshot(&[transaction]).unwrap();
        assert!(decode_snapshot(&valid).is_ok());
        assert!(decode_snapshot(&valid[..valid.len() - 1]).is_err());
        let mut trailing = valid.clone();
        trailing.push(0);
        assert!(decode_snapshot(&trailing).is_err());
        let mut excessive = vec![SNAPSHOT_VERSION];
        excessive.extend_from_slice(
            &u32::try_from(MAX_ADMITTED_TRANSACTIONS + 1)
                .unwrap()
                .to_le_bytes(),
        );
        assert!(decode_snapshot(&excessive).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn store_file_is_owner_only_and_rejects_permissive_reopen() {
        use std::os::unix::fs::PermissionsExt;

        let directory = TempDir::new().unwrap();
        let path = directory.path().join("mempool.redb");
        drop(RedbTransactionPoolStore::open(&path, Network::Regtest).unwrap());
        assert_eq!(
            fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );
        fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();
        assert!(matches!(
            RedbTransactionPoolStore::open(&path, Network::Regtest),
            Err(TransactionPoolStoreError::File(_))
        ));
    }
}
