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
const ASSUMED_SNAPSHOT_COUNT_KEY: &str = "assumed_snapshot_utxo_count";
const ASSUMED_SNAPSHOT_BYTES_KEY: &str = "assumed_snapshot_records_bytes";
const ASSUMED_SNAPSHOT_UTXO_SET_KEY: &str = "assumed_snapshot_utxo_set_sha256";
const VALIDATION_TARGET_KEY: &str = "validation_target";

/// Last block whose UTXO transition is recorded as complete.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExecutionTip {
    /// Active-chain height, with genesis at zero.
    pub height: u32,
    /// Hash at `height`.
    pub hash: BlockHash,
}

/// Durable identity of an assumed UTXO snapshot awaiting genesis validation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AssumedSnapshot {
    /// Active-chain block represented by the snapshot.
    pub base: ExecutionTip,
    /// Exact number of canonical UTXO records.
    pub utxo_count: u64,
    /// Exact byte length of the canonical uncompressed record stream.
    pub records_bytes: u64,
    /// SHA-256 of the canonical uncompressed record stream.
    pub records_sha256: [u8; 32],
    /// SHA-256 of consensus UTXO fields, excluding local tier-aging time.
    pub utxo_set_sha256: [u8; 32],
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
    /// The assumed-state marker changed while validation was being finalized.
    #[error("assumed snapshot identity changed during finalization")]
    AssumedSnapshotChanged,
    /// A validation directory is permanently bound to another snapshot base.
    #[error(
        "validation target {stored_height}:{stored_hash} does not match requested {requested_height}:{requested_hash}"
    )]
    ValidationTargetMismatch {
        /// Persisted validation height.
        stored_height: u32,
        /// Persisted validation hash.
        stored_hash: BlockHash,
        /// Requested validation height.
        requested_height: u32,
        /// Requested validation hash.
        requested_hash: BlockHash,
    },
    /// The requested validation target is below the already executed tip.
    #[error("validation target height {target_height} is below execution height {tip_height}")]
    ValidationTargetBehindTip {
        /// Requested validation height.
        target_height: u32,
        /// Existing execution height.
        tip_height: u32,
    },
    /// A transition tried to execute beyond the immutable validation ceiling.
    #[error("execution height {next_height} exceeds immutable validation target {target_height}")]
    ValidationTargetExceeded {
        /// Persisted validation height.
        target_height: u32,
        /// Proposed execution height.
        next_height: u32,
    },
    /// An assumed chainstate cannot serve as a genesis validator.
    #[error("validation target cannot be bound to an assumed chainstate")]
    ValidationTargetOnAssumedChainstate,
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
        self.assumed_snapshot()
            .map(|snapshot| snapshot.map(|value| value.base))
    }

    /// Returns the authenticated canonical-record digest of the assumed snapshot.
    pub fn assumed_snapshot_records_sha256(&self) -> Result<Option<[u8; 32]>, ExecutionStoreError> {
        self.assumed_snapshot()
            .map(|snapshot| snapshot.map(|value| value.records_sha256))
    }

    /// Returns the complete authenticated identity of an assumed snapshot.
    pub fn assumed_snapshot(&self) -> Result<Option<AssumedSnapshot>, ExecutionStoreError> {
        let transaction = self.db.begin_read()?;
        let meta = transaction.open_table(META)?;
        decode_assumed_snapshot(&meta)
    }

    /// Returns the consensus configuration bound to this execution database.
    pub fn consensus_config(&self) -> Result<Option<Vec<u8>>, ExecutionStoreError> {
        let transaction = self.db.begin_read()?;
        let meta = transaction.open_table(META)?;
        Ok(meta
            .get(CONSENSUS_CONFIG_KEY)?
            .map(|value| value.value().to_vec()))
    }

    /// Returns the immutable snapshot base assigned to this validation directory.
    pub fn validation_target(&self) -> Result<Option<ExecutionTip>, ExecutionStoreError> {
        let transaction = self.db.begin_read()?;
        let meta = transaction.open_table(META)?;
        meta.get(VALIDATION_TARGET_KEY)?
            .map(|value| decode_tip(value.value()))
            .transpose()
    }

    /// Permanently binds a non-assumed chainstate to one validation target.
    pub fn bind_validation_target(
        &self,
        requested: ExecutionTip,
    ) -> Result<(), ExecutionStoreError> {
        if requested.height == 0 {
            return Err(ExecutionStoreError::Malformed(
                "validation target cannot be genesis",
            ));
        }
        let _guard = self.lock();
        let transaction = self.db.begin_write()?;
        {
            let mut meta = transaction.open_table(META)?;
            if decode_assumed_snapshot(&meta)?.is_some() {
                return Err(ExecutionStoreError::ValidationTargetOnAssumedChainstate);
            }
            let tip_value = meta
                .get(TIP_KEY)?
                .ok_or(ExecutionStoreError::Malformed("missing execution tip"))?;
            let tip = decode_tip(tip_value.value())?;
            drop(tip_value);
            if tip.height > requested.height {
                return Err(ExecutionStoreError::ValidationTargetBehindTip {
                    target_height: requested.height,
                    tip_height: tip.height,
                });
            }
            let stored = meta
                .get(VALIDATION_TARGET_KEY)?
                .map(|value| decode_tip(value.value()))
                .transpose()?;
            if let Some(stored) = stored {
                if stored != requested {
                    return Err(ExecutionStoreError::ValidationTargetMismatch {
                        stored_height: stored.height,
                        stored_hash: stored.hash,
                        requested_height: requested.height,
                        requested_hash: requested.hash,
                    });
                }
            } else {
                meta.insert(VALIDATION_TARGET_KEY, encode_tip(requested).as_slice())?;
            }
            if tip.height == requested.height && tip.hash != requested.hash {
                return Err(ExecutionStoreError::ValidationTargetMismatch {
                    stored_height: tip.height,
                    stored_hash: tip.hash,
                    requested_height: requested.height,
                    requested_hash: requested.hash,
                });
            }
        }
        transaction.commit()?;
        Ok(())
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
    let validation_target = meta
        .get(VALIDATION_TARGET_KEY)?
        .map(|value| decode_tip(value.value()))
        .transpose()?;
    if let Some(target) = validation_target {
        if next.height > target.height {
            return Err(ExecutionStoreError::ValidationTargetExceeded {
                target_height: target.height,
                next_height: next.height,
            });
        }
        if next.height == target.height && next.hash != target.hash {
            return Err(ExecutionStoreError::ValidationTargetMismatch {
                stored_height: target.height,
                stored_hash: target.hash,
                requested_height: next.height,
                requested_hash: next.hash,
            });
        }
    }
    drop(current_value);
    meta.insert(TIP_KEY, encode_tip(next).as_slice())?;
    Ok(())
}

pub(crate) fn assume_snapshot_transaction(
    transaction: &WriteTransaction,
    snapshot: AssumedSnapshot,
) -> Result<(), ExecutionStoreError> {
    let anchor = snapshot.base;
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
    let has_snapshot = [
        ASSUMED_SNAPSHOT_BASE_KEY,
        ASSUMED_SNAPSHOT_RECORDS_KEY,
        ASSUMED_SNAPSHOT_COUNT_KEY,
        ASSUMED_SNAPSHOT_BYTES_KEY,
        ASSUMED_SNAPSHOT_UTXO_SET_KEY,
        VALIDATION_TARGET_KEY,
    ]
    .into_iter()
    .try_fold(false, |present, key| {
        meta.get(key).map(|value| present || value.is_some())
    })?;
    if current.height != 0 || current.hash != genesis || has_snapshot {
        return Err(ExecutionStoreError::SnapshotRequiresFreshChainstate);
    }
    drop(current_value);
    drop(genesis_value);
    let encoded = encode_tip(anchor);
    meta.insert(TIP_KEY, encoded.as_slice())?;
    meta.insert(ASSUMED_SNAPSHOT_BASE_KEY, encoded.as_slice())?;
    meta.insert(
        ASSUMED_SNAPSHOT_RECORDS_KEY,
        snapshot.records_sha256.as_slice(),
    )?;
    meta.insert(
        ASSUMED_SNAPSHOT_COUNT_KEY,
        snapshot.utxo_count.to_le_bytes().as_slice(),
    )?;
    meta.insert(
        ASSUMED_SNAPSHOT_BYTES_KEY,
        snapshot.records_bytes.to_le_bytes().as_slice(),
    )?;
    meta.insert(
        ASSUMED_SNAPSHOT_UTXO_SET_KEY,
        snapshot.utxo_set_sha256.as_slice(),
    )?;
    Ok(())
}

pub(crate) fn clear_assumed_snapshot_transaction(
    transaction: &WriteTransaction,
    expected: AssumedSnapshot,
) -> Result<(), ExecutionStoreError> {
    let mut meta = transaction.open_table(META)?;
    if decode_assumed_snapshot(&meta)? != Some(expected) {
        return Err(ExecutionStoreError::AssumedSnapshotChanged);
    }
    for key in [
        ASSUMED_SNAPSHOT_BASE_KEY,
        ASSUMED_SNAPSHOT_RECORDS_KEY,
        ASSUMED_SNAPSHOT_COUNT_KEY,
        ASSUMED_SNAPSHOT_BYTES_KEY,
        ASSUMED_SNAPSHOT_UTXO_SET_KEY,
    ] {
        meta.remove(key)?;
    }
    Ok(())
}

fn decode_assumed_snapshot(
    meta: &impl ReadableTable<&'static str, &'static [u8]>,
) -> Result<Option<AssumedSnapshot>, ExecutionStoreError> {
    let base = meta
        .get(ASSUMED_SNAPSHOT_BASE_KEY)?
        .map(|value| value.value().to_vec());
    let digest = meta
        .get(ASSUMED_SNAPSHOT_RECORDS_KEY)?
        .map(|value| value.value().to_vec());
    let count = meta
        .get(ASSUMED_SNAPSHOT_COUNT_KEY)?
        .map(|value| value.value().to_vec());
    let bytes = meta
        .get(ASSUMED_SNAPSHOT_BYTES_KEY)?
        .map(|value| value.value().to_vec());
    let utxo_set = meta
        .get(ASSUMED_SNAPSHOT_UTXO_SET_KEY)?
        .map(|value| value.value().to_vec());
    if [&base, &digest, &count, &bytes, &utxo_set]
        .into_iter()
        .all(Option::is_none)
    {
        return Ok(None);
    }
    let base = base.ok_or(ExecutionStoreError::Malformed(
        "incomplete assumed snapshot identity",
    ))?;
    let digest = digest.ok_or(ExecutionStoreError::Malformed(
        "incomplete assumed snapshot identity",
    ))?;
    let count = count.ok_or(ExecutionStoreError::Malformed(
        "incomplete assumed snapshot identity",
    ))?;
    let bytes = bytes.ok_or(ExecutionStoreError::Malformed(
        "incomplete assumed snapshot identity",
    ))?;
    let utxo_set = utxo_set.ok_or(ExecutionStoreError::Malformed(
        "incomplete assumed snapshot identity",
    ))?;
    Ok(Some(AssumedSnapshot {
        base: decode_tip(&base)?,
        records_sha256: digest
            .try_into()
            .map_err(|_| ExecutionStoreError::Malformed("assumed snapshot digest length"))?,
        utxo_count: decode_u64(&count, "assumed snapshot UTXO count")?,
        records_bytes: decode_u64(&bytes, "assumed snapshot records length")?,
        utxo_set_sha256: utxo_set.try_into().map_err(|_| {
            ExecutionStoreError::Malformed("assumed snapshot UTXO-set digest length")
        })?,
    }))
}

fn decode_u64(bytes: &[u8], field: &'static str) -> Result<u64, ExecutionStoreError> {
    Ok(u64::from_le_bytes(
        bytes
            .try_into()
            .map_err(|_| ExecutionStoreError::Malformed(field))?,
    ))
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
    fn incomplete_assumed_snapshot_identity_fails_closed() {
        let directory = TempDir::new().unwrap();
        let store =
            RedbExecutionStore::open(directory.path().join("execution.redb"), Network::Regtest)
                .unwrap();
        let transaction = store.db.begin_write().unwrap();
        {
            let mut meta = transaction.open_table(META).unwrap();
            meta.insert(
                ASSUMED_SNAPSHOT_BASE_KEY,
                encode_tip(ExecutionTip {
                    height: 1,
                    hash: BlockHash::from_byte_array([1; 32]),
                })
                .as_slice(),
            )
            .unwrap();
        }
        transaction.commit().unwrap();
        assert!(matches!(
            store.assumed_snapshot(),
            Err(ExecutionStoreError::Malformed(
                "incomplete assumed snapshot identity"
            ))
        ));
    }

    #[test]
    fn validation_target_is_durable_and_cannot_change_or_move_behind_tip() {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("execution.redb");
        let store = RedbExecutionStore::open(&path, Network::Regtest).unwrap();
        let genesis = store.tip().unwrap();
        let target = ExecutionTip {
            height: 2,
            hash: BlockHash::from_byte_array([2; 32]),
        };
        store.bind_validation_target(target).unwrap();
        store.bind_validation_target(target).unwrap();
        assert_eq!(store.validation_target().unwrap(), Some(target));
        assert!(matches!(
            store.bind_validation_target(ExecutionTip {
                height: 2,
                hash: BlockHash::from_byte_array([3; 32]),
            }),
            Err(ExecutionStoreError::ValidationTargetMismatch { .. })
        ));
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
            store.advance(
                BlockHash::from_byte_array([1; 32]),
                ExecutionTip {
                    height: 2,
                    hash: BlockHash::from_byte_array([3; 32]),
                },
            ),
            Err(ExecutionStoreError::ValidationTargetMismatch { .. })
        ));
        store
            .advance(BlockHash::from_byte_array([1; 32]), target)
            .unwrap();
        assert!(matches!(
            store.advance(
                target.hash,
                ExecutionTip {
                    height: 3,
                    hash: BlockHash::from_byte_array([4; 32]),
                },
            ),
            Err(ExecutionStoreError::ValidationTargetExceeded {
                target_height: 2,
                next_height: 3,
            })
        ));
        assert!(matches!(
            store.bind_validation_target(ExecutionTip {
                height: 1,
                hash: BlockHash::from_byte_array([1; 32]),
            }),
            Err(ExecutionStoreError::ValidationTargetBehindTip { .. })
        ));
        drop(store);
        let reopened = RedbExecutionStore::open(path, Network::Regtest).unwrap();
        assert_eq!(reopened.validation_target().unwrap(), Some(target));
    }

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
