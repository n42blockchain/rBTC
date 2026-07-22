//! Durable active-chain execution tip metadata.

use std::{
    path::Path,
    sync::{Arc, Mutex, MutexGuard},
};

use bitcoin::{BlockHash, Network, hashes::Hash};
use redb::{Database, ReadableTable, TableDefinition, WriteTransaction};
use thiserror::Error;

const META: TableDefinition<&str, &[u8]> = TableDefinition::new("execution_metadata");
const GENESIS_KEY: &str = "genesis";
const TIP_KEY: &str = "tip";
const CONSENSUS_CONFIG_KEY: &str = "consensus_config";
const ASSUMED_SNAPSHOT_BASE_KEY: &str = "assumed_snapshot_base";
const ASSUMED_SNAPSHOT_RECORDS_KEY: &str = "assumed_snapshot_records_sha256";

/// Last block whose UTXO transition is recorded as complete.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExecutionTip {
    /// Active-chain height, with genesis at zero.
    pub height: u32,
    /// Hash at `height`.
    pub hash: BlockHash,
}

/// Execution-tip persistence failures.
#[derive(Debug, Error)]
pub enum ExecutionStoreError {
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
    /// Persisted metadata is malformed.
    #[error("malformed execution metadata: {0}")]
    Malformed(&'static str),
    /// The database belongs to another Bitcoin network.
    #[error(
        "execution database genesis {stored} does not match selected network genesis {expected}"
    )]
    NetworkMismatch {
        /// Persisted genesis hash.
        stored: BlockHash,
        /// Selected network genesis hash.
        expected: BlockHash,
    },
    /// Persisted blocks were validated with different consensus configuration.
    #[error("execution consensus configuration cannot change above genesis height {height}")]
    ConsensusConfigMismatch {
        /// Current executed height.
        height: u32,
    },
    /// A transition did not extend the stored tip by exactly one block.
    #[error("execution transition does not extend tip {current_height}:{current_hash}")]
    NonSequential {
        /// Persisted current height.
        current_height: u32,
        /// Persisted current hash.
        current_hash: BlockHash,
    },
    /// An assumed UTXO snapshot can only initialize an untouched genesis state.
    #[error("assumed snapshot requires a fresh genesis chainstate")]
    SnapshotRequiresFreshChainstate,
}

/// redb-backed execution tip initialized to the selected network's genesis.
pub struct RedbExecutionStore {
    db: Arc<Database>,
    write_guard: Mutex<()>,
    new_database: bool,
}

impl RedbExecutionStore {
    /// Opens or initializes execution metadata at `path`.
    pub fn open(path: impl AsRef<Path>, network: Network) -> Result<Self, ExecutionStoreError> {
        Self::from_database(Arc::new(Database::create(path)?), network)
    }

    pub(crate) fn from_database(
        db: Arc<Database>,
        network: Network,
    ) -> Result<Self, ExecutionStoreError> {
        let expected = bitcoin::blockdata::constants::genesis_block(network).block_hash();
        let transaction = db.begin_write()?;
        let new_database;
        {
            let mut meta = transaction.open_table(META)?;
            let stored_genesis = meta.get(GENESIS_KEY)?.map(|value| value.value().to_vec());
            new_database = stored_genesis.is_none();
            if let Some(value) = stored_genesis {
                let stored = decode_hash(&value, "genesis hash")?;
                if stored != expected {
                    return Err(ExecutionStoreError::NetworkMismatch { stored, expected });
                }
                if meta.get(TIP_KEY)?.is_none() {
                    return Err(ExecutionStoreError::Malformed("missing execution tip"));
                }
            } else {
                meta.insert(GENESIS_KEY, expected.to_byte_array().as_slice())?;
                meta.insert(
                    TIP_KEY,
                    encode_tip(ExecutionTip {
                        height: 0,
                        hash: expected,
                    })
                    .as_slice(),
                )?;
            }
        }
        transaction.commit()?;
        Ok(Self {
            db,
            write_guard: Mutex::new(()),
            new_database,
        })
    }

    /// Reads the last completely recorded execution tip.
    pub fn tip(&self) -> Result<ExecutionTip, ExecutionStoreError> {
        let transaction = self.db.begin_read()?;
        let meta = transaction.open_table(META)?;
        let value = meta
            .get(TIP_KEY)?
            .ok_or(ExecutionStoreError::Malformed("missing execution tip"))?;
        decode_tip(value.value())
    }

    /// Returns the UTXO snapshot base that still requires background validation.
    pub fn assumed_snapshot_base(&self) -> Result<Option<ExecutionTip>, ExecutionStoreError> {
        let transaction = self.db.begin_read()?;
        let meta = transaction.open_table(META)?;
        meta.get(ASSUMED_SNAPSHOT_BASE_KEY)?
            .map(|value| decode_tip(value.value()))
            .transpose()
    }

    /// Returns the authenticated canonical-record digest of the assumed snapshot.
    pub fn assumed_snapshot_records_sha256(&self) -> Result<Option<[u8; 32]>, ExecutionStoreError> {
        let transaction = self.db.begin_read()?;
        let meta = transaction.open_table(META)?;
        meta.get(ASSUMED_SNAPSHOT_RECORDS_KEY)?
            .map(|value| {
                value
                    .value()
                    .try_into()
                    .map_err(|_| ExecutionStoreError::Malformed("assumed snapshot digest length"))
            })
            .transpose()
    }

    /// Binds this execution database to a canonical consensus configuration.
    ///
    /// A fresh database accepts the first selected configuration. Once bound it
    /// cannot be changed in place, even at genesis, because an interrupted block
    /// transition may already exist in the other stores. Databases created
    /// before this metadata existed can migrate only to `legacy_default`.
    pub fn bind_consensus_config(
        &self,
        selected: &[u8],
        legacy_default: &[u8],
    ) -> Result<(), ExecutionStoreError> {
        if selected.is_empty() || legacy_default.is_empty() {
            return Err(ExecutionStoreError::Malformed(
                "empty consensus configuration",
            ));
        }
        let _guard = self.lock();
        let transaction = self.db.begin_write()?;
        {
            let mut meta = transaction.open_table(META)?;
            let tip_value = meta
                .get(TIP_KEY)?
                .ok_or(ExecutionStoreError::Malformed("missing execution tip"))?;
            let tip = decode_tip(tip_value.value())?;
            drop(tip_value);
            let stored = meta
                .get(CONSENSUS_CONFIG_KEY)?
                .map(|value| value.value().to_vec());
            let compatible = stored.as_deref().map_or_else(
                || (self.new_database && tip.height == 0) || selected == legacy_default,
                |stored| stored == selected,
            );
            if !compatible {
                return Err(ExecutionStoreError::ConsensusConfigMismatch { height: tip.height });
            }
            if stored.as_deref() != Some(selected) {
                meta.insert(CONSENSUS_CONFIG_KEY, selected)?;
            }
        }
        transaction.commit()?;
        Ok(())
    }

    /// Advances the execution tip by exactly one child block.
    pub fn advance(
        &self,
        expected_parent: BlockHash,
        next: ExecutionTip,
    ) -> Result<(), ExecutionStoreError> {
        let _guard = self.lock();
        let transaction = self.db.begin_write()?;
        advance_transaction(&transaction, expected_parent, next)?;
        transaction.commit()?;
        Ok(())
    }

    /// Rewinds the execution tip to an explicitly supplied parent.
    pub fn rewind(
        &self,
        expected_current: ExecutionTip,
        parent: ExecutionTip,
    ) -> Result<(), ExecutionStoreError> {
        let _guard = self.lock();
        let transaction = self.db.begin_write()?;
        rewind_transaction(&transaction, expected_current, parent)?;
        transaction.commit()?;
        Ok(())
    }

    fn lock(&self) -> MutexGuard<'_, ()> {
        self.write_guard.lock().expect("write lock not poisoned")
    }
}

pub(crate) fn metadata_exists(db: &Database) -> Result<bool, ExecutionStoreError> {
    let transaction = db.begin_read()?;
    match transaction.open_table(META) {
        Ok(meta) => Ok(meta.get(GENESIS_KEY)?.is_some()),
        Err(redb::TableError::TableDoesNotExist(_)) => Ok(false),
        Err(error) => Err(error.into()),
    }
}

pub(crate) fn advance_transaction(
    transaction: &WriteTransaction,
    expected_parent: BlockHash,
    next: ExecutionTip,
) -> Result<(), ExecutionStoreError> {
    let mut meta = transaction.open_table(META)?;
    let current_value = meta
        .get(TIP_KEY)?
        .ok_or(ExecutionStoreError::Malformed("missing execution tip"))?;
    let current = decode_tip(current_value.value())?;
    if current.hash != expected_parent || current.height.checked_add(1) != Some(next.height) {
        return Err(ExecutionStoreError::NonSequential {
            current_height: current.height,
            current_hash: current.hash,
        });
    }
    drop(current_value);
    meta.insert(TIP_KEY, encode_tip(next).as_slice())?;
    Ok(())
}

pub(crate) fn assume_snapshot_transaction(
    transaction: &WriteTransaction,
    anchor: ExecutionTip,
    records_sha256: &[u8; 32],
) -> Result<(), ExecutionStoreError> {
    if anchor.height == 0 {
        return Err(ExecutionStoreError::SnapshotRequiresFreshChainstate);
    }
    let mut meta = transaction.open_table(META)?;
    let current_value = meta
        .get(TIP_KEY)?
        .ok_or(ExecutionStoreError::Malformed("missing execution tip"))?;
    let current = decode_tip(current_value.value())?;
    let genesis_value = meta
        .get(GENESIS_KEY)?
        .ok_or(ExecutionStoreError::Malformed("missing genesis hash"))?;
    let genesis = decode_hash(genesis_value.value(), "genesis hash")?;
    let has_snapshot = meta.get(ASSUMED_SNAPSHOT_BASE_KEY)?.is_some();
    let has_snapshot_digest = meta.get(ASSUMED_SNAPSHOT_RECORDS_KEY)?.is_some();
    if current.height != 0 || current.hash != genesis || has_snapshot || has_snapshot_digest {
        return Err(ExecutionStoreError::SnapshotRequiresFreshChainstate);
    }
    drop(current_value);
    drop(genesis_value);
    let encoded = encode_tip(anchor);
    meta.insert(TIP_KEY, encoded.as_slice())?;
    meta.insert(ASSUMED_SNAPSHOT_BASE_KEY, encoded.as_slice())?;
    meta.insert(ASSUMED_SNAPSHOT_RECORDS_KEY, records_sha256.as_slice())?;
    Ok(())
}

pub(crate) fn rewind_transaction(
    transaction: &WriteTransaction,
    expected_current: ExecutionTip,
    parent: ExecutionTip,
) -> Result<(), ExecutionStoreError> {
    let mut meta = transaction.open_table(META)?;
    let current_value = meta
        .get(TIP_KEY)?
        .ok_or(ExecutionStoreError::Malformed("missing execution tip"))?;
    let current = decode_tip(current_value.value())?;
    if current != expected_current || parent.height.checked_add(1) != Some(current.height) {
        return Err(ExecutionStoreError::NonSequential {
            current_height: current.height,
            current_hash: current.hash,
        });
    }
    drop(current_value);
    meta.insert(TIP_KEY, encode_tip(parent).as_slice())?;
    Ok(())
}

fn encode_tip(tip: ExecutionTip) -> [u8; 36] {
    let mut bytes = [0_u8; 36];
    bytes[..4].copy_from_slice(&tip.height.to_le_bytes());
    bytes[4..].copy_from_slice(&tip.hash.to_byte_array());
    bytes
}

fn decode_tip(bytes: &[u8]) -> Result<ExecutionTip, ExecutionStoreError> {
    if bytes.len() != 36 {
        return Err(ExecutionStoreError::Malformed("execution tip length"));
    }
    Ok(ExecutionTip {
        height: u32::from_le_bytes(bytes[..4].try_into().expect("checked length")),
        hash: decode_hash(&bytes[4..], "execution tip hash")?,
    })
}

fn decode_hash(bytes: &[u8], field: &'static str) -> Result<BlockHash, ExecutionStoreError> {
    let bytes: [u8; 32] = bytes
        .try_into()
        .map_err(|_| ExecutionStoreError::Malformed(field))?;
    Ok(BlockHash::from_byte_array(bytes))
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;
    use crate::deployments::DeploymentConfig;

    #[test]
    fn advances_and_recovers_tip_while_rejecting_gaps() {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("execution.redb");
        let store = RedbExecutionStore::open(&path, Network::Regtest).unwrap();
        store.bind_consensus_config(b"default", b"default").unwrap();
        let genesis = store.tip().unwrap();
        let child = ExecutionTip {
            height: 1,
            hash: BlockHash::from_byte_array([1; 32]),
        };
        store.advance(genesis.hash, child).unwrap();
        assert!(matches!(
            store.advance(
                child.hash,
                ExecutionTip {
                    height: 3,
                    hash: BlockHash::from_byte_array([3; 32])
                }
            ),
            Err(ExecutionStoreError::NonSequential { .. })
        ));
        drop(store);

        let reopened = RedbExecutionStore::open(path, Network::Regtest).unwrap();
        reopened
            .bind_consensus_config(b"default", b"default")
            .unwrap();
        assert!(matches!(
            reopened.bind_consensus_config(b"custom", b"default"),
            Err(ExecutionStoreError::ConsensusConfigMismatch { height: 1 })
        ));
        assert_eq!(reopened.tip().unwrap(), child);
        reopened.rewind(child, genesis).unwrap();
        assert_eq!(reopened.tip().unwrap(), genesis);
        assert!(matches!(
            reopened.bind_consensus_config(b"custom", b"default"),
            Err(ExecutionStoreError::ConsensusConfigMismatch { height: 0 })
        ));
    }

    #[test]
    fn legacy_non_genesis_store_only_accepts_default_consensus() {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("execution.redb");
        let store = RedbExecutionStore::open(&path, Network::Regtest).unwrap();
        let genesis = store.tip().unwrap();
        store
            .advance(
                genesis.hash,
                ExecutionTip {
                    height: 1,
                    hash: BlockHash::from_byte_array([1; 32]),
                },
            )
            .unwrap();
        assert!(matches!(
            store.bind_consensus_config(b"custom", b"default"),
            Err(ExecutionStoreError::ConsensusConfigMismatch { height: 1 })
        ));
        store.bind_consensus_config(b"default", b"default").unwrap();
    }

    #[test]
    fn fresh_store_accepts_one_custom_consensus_binding() {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("execution.redb");
        let store = RedbExecutionStore::open(&path, Network::Regtest).unwrap();
        store.bind_consensus_config(b"custom", b"default").unwrap();
        drop(store);

        let store = RedbExecutionStore::open(path, Network::Regtest).unwrap();
        store.bind_consensus_config(b"custom", b"default").unwrap();
        assert!(matches!(
            store.bind_consensus_config(b"other", b"default"),
            Err(ExecutionStoreError::ConsensusConfigMismatch { height: 0 })
        ));
    }

    #[test]
    fn buried_activation_binding_survives_restart_and_rejects_changes() {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("execution.redb");
        let defaults = DeploymentConfig::for_network(Network::Regtest);
        let mut selected = defaults.clone();
        selected.apply_test_activation_height("bip34@10").unwrap();
        let store = RedbExecutionStore::open(&path, Network::Regtest).unwrap();
        store
            .bind_consensus_config(&selected.consensus_id(), &defaults.consensus_id())
            .unwrap();
        drop(store);

        let store = RedbExecutionStore::open(path, Network::Regtest).unwrap();
        store
            .bind_consensus_config(&selected.consensus_id(), &defaults.consensus_id())
            .unwrap();
        let mut changed = defaults.clone();
        changed.apply_test_activation_height("bip34@11").unwrap();
        assert!(matches!(
            store.bind_consensus_config(&changed.consensus_id(), &defaults.consensus_id()),
            Err(ExecutionStoreError::ConsensusConfigMismatch { height: 0 })
        ));
    }
}
