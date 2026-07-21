//! Durable active-chain execution tip metadata.

use std::{
    path::Path,
    sync::{Mutex, MutexGuard},
};

use bitcoin::{BlockHash, Network, hashes::Hash};
use redb::{Database, ReadableTable, TableDefinition};
use thiserror::Error;

const META: TableDefinition<&str, &[u8]> = TableDefinition::new("execution_metadata");
const GENESIS_KEY: &str = "genesis";
const TIP_KEY: &str = "tip";

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
    /// A transition did not extend the stored tip by exactly one block.
    #[error("execution transition does not extend tip {current_height}:{current_hash}")]
    NonSequential {
        /// Persisted current height.
        current_height: u32,
        /// Persisted current hash.
        current_hash: BlockHash,
    },
}

/// redb-backed execution tip initialized to the selected network's genesis.
pub struct RedbExecutionStore {
    db: Database,
    write_guard: Mutex<()>,
}

impl RedbExecutionStore {
    /// Opens or initializes execution metadata at `path`.
    pub fn open(path: impl AsRef<Path>, network: Network) -> Result<Self, ExecutionStoreError> {
        let expected = bitcoin::blockdata::constants::genesis_block(network).block_hash();
        let db = Database::create(path)?;
        let transaction = db.begin_write()?;
        {
            let mut meta = transaction.open_table(META)?;
            let stored_genesis = meta.get(GENESIS_KEY)?.map(|value| value.value().to_vec());
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

    /// Advances the execution tip by exactly one child block.
    pub fn advance(
        &self,
        expected_parent: BlockHash,
        next: ExecutionTip,
    ) -> Result<(), ExecutionStoreError> {
        let _guard = self.lock();
        let transaction = self.db.begin_write()?;
        {
            let mut meta = transaction.open_table(META)?;
            let current_value = meta
                .get(TIP_KEY)?
                .ok_or(ExecutionStoreError::Malformed("missing execution tip"))?;
            let current = decode_tip(current_value.value())?;
            if current.hash != expected_parent || current.height.checked_add(1) != Some(next.height)
            {
                return Err(ExecutionStoreError::NonSequential {
                    current_height: current.height,
                    current_hash: current.hash,
                });
            }
            drop(current_value);
            meta.insert(TIP_KEY, encode_tip(next).as_slice())?;
        }
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
        {
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
        }
        transaction.commit()?;
        Ok(())
    }

    fn lock(&self) -> MutexGuard<'_, ()> {
        self.write_guard.lock().expect("write lock not poisoned")
    }
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

    #[test]
    fn advances_and_recovers_tip_while_rejecting_gaps() {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("execution.redb");
        let store = RedbExecutionStore::open(&path, Network::Regtest).unwrap();
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
        assert_eq!(reopened.tip().unwrap(), child);
        reopened.rewind(child, genesis).unwrap();
        assert_eq!(reopened.tip().unwrap(), genesis);
    }
}
