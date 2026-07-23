//! Bounded, network-bound persistence for the peer transaction admission pool.

use std::{
    collections::{BTreeSet, HashSet},
    fs,
    path::Path,
    sync::Mutex,
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
const SNAPSHOT_VERSION: u8 = 1;
const SNAPSHOT_HEADER_BYTES: usize = 5;
const MAX_SNAPSHOT_BYTES: usize =
    SNAPSHOT_HEADER_BYTES + MAX_ADMITTED_TRANSACTIONS * 4 + MAX_ADMITTED_TRANSACTION_BYTES;

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

impl RedbTransactionPoolStore {
    /// Opens or creates an owner-only store and validates the persisted snapshot.
    pub fn open(
        path: impl AsRef<Path>,
        network: Network,
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
        let encoded = encode_snapshot(transactions)?;
        self.replace_encoded(SNAPSHOT_KEY, &encoded)
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

    /// Atomically publishes the admitted pool and clears recovered reorg candidates.
    pub fn replace_and_clear_disconnected(
        &self,
        transactions: &[Transaction],
    ) -> Result<(), TransactionPoolStoreError> {
        let encoded = encode_snapshot(transactions)?;
        let empty = encode_snapshot(&[])?;
        let _guard = self
            .write_guard
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let write = self.db.begin_write()?;
        {
            let mut snapshots = write.open_table(SNAPSHOTS)?;
            snapshots.insert(SNAPSHOT_KEY, encoded.as_slice())?;
            snapshots.insert(DISCONNECTED_KEY, empty.as_slice())?;
        }
        write.commit()?;
        Ok(())
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
