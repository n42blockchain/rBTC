//! Crash-safe persistence and replay for validated proof-of-work headers.

use std::{
    path::Path,
    sync::{Mutex, MutexGuard},
};

use bitcoin::{
    Network,
    block::{BlockHash, Header},
    consensus::{deserialize, encode::Error as EncodeError, serialize},
    hashes::Hash,
};
use redb::{Database, ReadableTable, ReadableTableMetadata, TableDefinition};
use thiserror::Error;

use crate::{
    deployments::DeploymentConfig,
    headers::{HeaderDag, HeaderError, StagedHeaderEviction},
};

/// Header-store page-cache allowance, independent of retained DAG entries.
/// Redb transaction/repair buffers and the OS page cache are separate resources.
pub const HEADER_STORE_CACHE_BYTES: usize = 64 * 1024 * 1024;

const HEADERS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("headers_by_hash");
const INSERTION_ORDER: TableDefinition<u64, &[u8]> = TableDefinition::new("header_insertion_order");
const HASH_SEQUENCE: TableDefinition<&[u8], u64> = TableDefinition::new("header_hash_sequence");
const META: TableDefinition<&str, &[u8]> = TableDefinition::new("header_metadata");
const NEXT_SEQUENCE_KEY: &str = "next_sequence";
const RECOVERY_TIP_KEY: &str = "recovery_tip";

/// Default ceiling on persisted non-genesis entries before DAG materialization.
/// This is an entry-count safety limit, not a measured process-RSS guarantee.
/// Offline callers can supply a reviewed allowance through load_dag_with_limit.
pub const DEFAULT_MAX_REPLAY_HEADERS: usize = crate::headers::DEFAULT_MAX_RETAINED_HEADERS;

#[cfg(test)]
thread_local! {
    pub(crate) static REPLAYED_HEADERS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// Failures from header persistence and replay.
#[derive(Debug, Error)]
pub enum HeaderStoreError {
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
    /// A header's consensus encoding could not be decoded.
    #[error("header encoding: {0}")]
    Encoding(#[from] EncodeError),
    /// Persisted data violates the store's canonical format.
    #[error("malformed header store: {0}")]
    Malformed(&'static str),
    /// A header has already been committed to this store.
    #[error("duplicate persisted header {0}")]
    Duplicate(BlockHash),
    /// A header failed contextual proof-of-work chain validation during replay.
    #[error("header replay validation: {0}")]
    Header(#[from] HeaderError),
    /// A local reopen allowance was exceeded before any DAG replay or allocation.
    #[error("header replay resource deferred: {retained} headers exceed allowance {limit}")]
    ResourceDeferred {
        /// Persisted non-genesis header count.
        retained: u64,
        /// Caller-supplied retained-header allowance, excluding genesis.
        limit: usize,
    },
}

/// Transactional redb storage for headers already accepted by [`HeaderDag`].
///
/// Header insertion order is persisted separately from the hash lookup table,
/// so all known branches can be rebuilt parent-first after a restart. The
/// active tip is therefore recomputed from cumulative work rather than trusted
/// as mutable metadata.
pub struct RedbHeaderStore {
    db: Database,
    write_guard: Mutex<()>,
}

impl RedbHeaderStore {
    /// Opens or creates a header database at `path`.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, HeaderStoreError> {
        let db = Database::builder()
            .set_cache_size(HEADER_STORE_CACHE_BYTES)
            .create(path)?;
        let transaction = db.begin_write()?;
        {
            let _headers = transaction.open_table(HEADERS)?;
            let _order = transaction.open_table(INSERTION_ORDER)?;
            let _sequence = transaction.open_table(HASH_SEQUENCE)?;
            let mut meta = transaction.open_table(META)?;
            if meta.get(NEXT_SEQUENCE_KEY)?.is_none() {
                meta.insert(NEXT_SEQUENCE_KEY, 0_u64.to_le_bytes().as_slice())?;
            }
        }
        transaction.commit()?;
        Ok(Self {
            db,
            write_guard: Mutex::new(()),
        })
    }

    /// Persists a header after the caller has accepted it into a header DAG.
    pub fn append(&self, header: Header) -> Result<(), HeaderStoreError> {
        self.append_batch(&[header])
    }

    /// Persists a validated header batch in one redb write transaction.
    ///
    /// The caller should first use [`HeaderDag::stage_batch_contextual`] and
    /// commit the returned guard only after this durable append succeeds.
    /// A duplicate or malformed header aborts the complete batch, leaving the
    /// durable prefix unchanged.
    pub fn append_batch(&self, batch: &[Header]) -> Result<(), HeaderStoreError> {
        self.append_batch_with_cursor(batch, None)
    }

    /// Atomically persists validated headers and the next recovery locator tip.
    /// The tip must be a persisted non-genesis header, including one in `batch`.
    /// An empty batch checkpoints an already validated, retained prefix.
    pub fn append_recovery_batch(
        &self,
        batch: &[Header],
        tip: BlockHash,
    ) -> Result<(), HeaderStoreError> {
        self.append_batch_with_cursor(batch, Some(tip))
    }

    /// Returns the durable recovery hint, never a trusted consensus checkpoint.
    /// Callers must replay validation and resolve it in their DAG before use.
    pub fn recovery_tip(&self) -> Result<Option<BlockHash>, HeaderStoreError> {
        let transaction = self.db.begin_read()?;
        let meta = transaction.open_table(META)?;
        let value = meta.get(RECOVERY_TIP_KEY)?;
        value
            .map(|value| {
                let bytes = value
                    .value()
                    .try_into()
                    .map_err(|_| HeaderStoreError::Malformed("header recovery tip encoding"))?;
                Ok(BlockHash::from_byte_array(bytes))
            })
            .transpose()
    }

    /// Clears a completed recovery hint without changing any retained headers.
    pub fn clear_recovery_tip(&self) -> Result<(), HeaderStoreError> {
        let _guard = self.lock();
        let transaction = self.db.begin_write()?;
        transaction.open_table(META)?.remove(RECOVERY_TIP_KEY)?;
        transaction.commit()?;
        Ok(())
    }

    fn append_batch_with_cursor(
        &self,
        batch: &[Header],
        tip: Option<BlockHash>,
    ) -> Result<(), HeaderStoreError> {
        if batch.is_empty() && tip.is_none() {
            return Ok(());
        }
        let _guard = self.lock();
        let transaction = self.db.begin_write()?;
        {
            let mut headers = transaction.open_table(HEADERS)?;
            let mut meta = transaction.open_table(META)?;
            let mut sequence = read_sequence(
                meta.get(NEXT_SEQUENCE_KEY)?
                    .as_ref()
                    .map(redb::AccessGuard::value),
            )?;
            let mut order = transaction.open_table(INSERTION_ORDER)?;
            let mut reverse = transaction.open_table(HASH_SEQUENCE)?;
            for header in batch {
                let hash = header.block_hash();
                let hash_bytes = hash.to_byte_array();
                let encoded = serialize(header);
                if encoded.len() != 80 {
                    return Err(HeaderStoreError::Malformed("header encoding length"));
                }
                if headers.get(hash_bytes.as_slice())?.is_some() {
                    return Err(HeaderStoreError::Duplicate(hash));
                }
                headers.insert(hash_bytes.as_slice(), encoded.as_slice())?;
                order.insert(sequence, hash_bytes.as_slice())?;
                reverse.insert(hash_bytes.as_slice(), sequence)?;
                sequence = sequence
                    .checked_add(1)
                    .ok_or(HeaderStoreError::Malformed("header sequence overflow"))?;
            }
            meta.insert(NEXT_SEQUENCE_KEY, sequence.to_le_bytes().as_slice())?;
            if let Some(tip) = tip {
                let bytes = tip.to_byte_array();
                if headers.get(bytes.as_slice())?.is_none() {
                    return Err(HeaderStoreError::Malformed("unretained recovery tip"));
                }
                meta.insert(RECOVERY_TIP_KEY, bytes.as_slice())?;
            }
        }
        transaction.commit()?;
        Ok(())
    }

    /// Returns the number of non-genesis headers persisted in this store.
    pub fn len(&self) -> Result<u64, HeaderStoreError> {
        let transaction = self.db.begin_read()?;
        Ok(transaction.open_table(HEADERS)?.len()?)
    }

    /// Atomically persists an explicitly staged in-memory retention operation.
    ///
    /// The stage must come from the complete DAG corresponding to this store,
    /// with execution/recovery tips pinned. The caller commits its guard only
    /// after this method succeeds. A legacy reverse index is filled by streaming
    /// insertion rows inside this same transaction; no historical DAG is copied.
    /// Redb reuses freed pages, but this method does not shrink the file or claim
    /// a physical disk budget. Caught-up idle maintenance uses this operation;
    /// ingress does not yet impose a hard resource budget.
    pub fn persist_eviction(
        &self,
        stage: &StagedHeaderEviction<'_>,
    ) -> Result<(), HeaderStoreError> {
        if stage.evicted().is_empty() {
            return Ok(());
        }
        let _guard = self.lock();
        let transaction = self.db.begin_write()?;
        {
            let mut headers = transaction.open_table(HEADERS)?;
            let mut order = transaction.open_table(INSERTION_ORDER)?;
            let mut reverse = transaction.open_table(HASH_SEQUENCE)?;
            let mut meta = transaction.open_table(META)?;
            if reverse.len()? != order.len()? {
                for row in order.iter()? {
                    let (sequence, hash) = row?;
                    reverse.insert(hash.value(), sequence.value())?;
                }
            }
            for info in stage.evicted() {
                let hash = info.hash.to_byte_array();
                let sequence = reverse
                    .get(hash.as_slice())?
                    .ok_or(HeaderStoreError::Malformed("evicted header lacks sequence"))?
                    .value();
                let encoded = headers
                    .get(hash.as_slice())?
                    .ok_or(HeaderStoreError::Malformed("evicted header missing"))?;
                if encoded.value() != serialize(&info.header) {
                    return Err(HeaderStoreError::Malformed(
                        "evicted header differs from DAG",
                    ));
                }
                drop(encoded);
                let ordered = order.get(sequence)?.ok_or(HeaderStoreError::Malformed(
                    "evicted header lacks ordered row",
                ))?;
                if ordered.value() != hash {
                    return Err(HeaderStoreError::Malformed("evicted header order mismatch"));
                }
                drop(ordered);
                headers.remove(hash.as_slice())?;
                order.remove(sequence)?;
                reverse.remove(hash.as_slice())?;
                let removes_cursor = meta
                    .get(RECOVERY_TIP_KEY)?
                    .is_some_and(|cursor| cursor.value() == hash);
                if removes_cursor {
                    meta.remove(RECOVERY_TIP_KEY)?;
                }
            }
        }
        transaction.commit()?;
        Ok(())
    }

    /// Returns whether no non-genesis headers have been persisted.
    pub fn is_empty(&self) -> Result<bool, HeaderStoreError> {
        Ok(self.len()? == 0)
    }

    /// Rebuilds a fully validated header DAG from the append-only record.
    ///
    /// `adjusted_time` must originate from the node's network-time subsystem.
    /// Replaying the contextual difficulty and timestamp checks detects corrupt
    /// or cross-network database content before it can influence active-chain
    /// selection.
    pub fn load_dag(
        &self,
        network: Network,
        adjusted_time: u32,
    ) -> Result<HeaderDag, HeaderStoreError> {
        self.load_dag_with_deployments(DeploymentConfig::for_network(network), adjusted_time)
    }

    /// Rebuilds the header DAG under an explicitly selected consensus configuration.
    pub fn load_dag_with_deployments(
        &self,
        deployments: DeploymentConfig,
        adjusted_time: u32,
    ) -> Result<HeaderDag, HeaderStoreError> {
        self.load_dag_with_limit(deployments, adjusted_time, DEFAULT_MAX_REPLAY_HEADERS)
    }

    /// Checks retained row count before materializing any historical DAG entries.
    ///
    /// The allowance excludes genesis. Exceeding it is a local resource deferral,
    /// not corrupt or invalid chain data. This primitive does not select entries
    /// to discard, and automatic bounded node startup awaits the recovery policy.
    pub fn load_dag_with_limit(
        &self,
        deployments: DeploymentConfig,
        adjusted_time: u32,
        max_headers: usize,
    ) -> Result<HeaderDag, HeaderStoreError> {
        let transaction = self.db.begin_read()?;
        let order = transaction.open_table(INSERTION_ORDER)?;
        let headers = transaction.open_table(HEADERS)?;
        let retained = headers.len()?;
        if order.len()? != retained {
            return Err(HeaderStoreError::Malformed(
                "header and insertion counts differ",
            ));
        }
        if retained > u64::try_from(max_headers).unwrap_or(u64::MAX) {
            return Err(HeaderStoreError::ResourceDeferred {
                retained,
                limit: max_headers,
            });
        }
        let mut dag = HeaderDag::with_deployments(deployments);
        for row in order.iter()? {
            let (_sequence, hash) = row?;
            let hash = BlockHash::from_byte_array(
                hash.value()
                    .try_into()
                    .map_err(|_| HeaderStoreError::Malformed("header order hash"))?,
            );
            let encoded = headers
                .get(hash.to_byte_array().as_slice())?
                .ok_or(HeaderStoreError::Malformed("ordered header missing"))?;
            let header: Header = deserialize(encoded.value())?;
            if header.block_hash() != hash {
                return Err(HeaderStoreError::Malformed("header hash mismatch"));
            }
            #[cfg(test)]
            REPLAYED_HEADERS.with(|count| count.set(count.get() + 1));
            dag.insert_contextual(header, adjusted_time)?;
        }
        Ok(dag)
    }

    fn lock(&self) -> MutexGuard<'_, ()> {
        self.write_guard.lock().expect("write lock not poisoned")
    }
}

fn read_sequence(value: Option<&[u8]>) -> Result<u64, HeaderStoreError> {
    let value = value.ok_or(HeaderStoreError::Malformed("missing next sequence"))?;
    let value: [u8; 8] = value
        .try_into()
        .map_err(|_| HeaderStoreError::Malformed("next sequence length"))?;
    Ok(u64::from_le_bytes(value))
}

#[cfg(test)]
mod tests {
    mod retention;
    use bitcoin::{
        TxMerkleNode,
        block::{Header, Version},
        pow::Target,
    };
    use tempfile::TempDir;

    use super::*;

    fn mine_child(parent: BlockHash, time: u32) -> Header {
        let target = Target::MAX_ATTAINABLE_REGTEST;
        let mut header = Header {
            version: Version::from_consensus(4),
            prev_blockhash: parent,
            merkle_root: TxMerkleNode::all_zeros(),
            time,
            bits: target.to_compact_lossy(),
            nonce: 0,
        };
        while header.validate_pow(target).is_err() {
            header.nonce = header.nonce.checked_add(1).unwrap();
        }
        header
    }

    #[test]
    fn persists_and_replays_a_valid_header_chain() {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("headers.redb");
        let store = RedbHeaderStore::open(&path).unwrap();
        let mut dag = HeaderDag::new(Network::Regtest);
        let genesis = dag.active_tip();
        let first = mine_child(genesis.hash, genesis.header.time + 1);
        let first_info = dag.insert_contextual(first, first.time).unwrap();
        let second = mine_child(first_info.hash, first.time + 1);
        let second_info = dag.insert_contextual(second, second.time).unwrap();
        store.append_batch(&[first, second]).unwrap();

        assert_eq!(store.len().unwrap(), 2);
        drop(store);
        let restored_store = RedbHeaderStore::open(path).unwrap();
        let restored = restored_store
            .load_dag(Network::Regtest, second.time)
            .unwrap();
        assert_eq!(restored.active_tip().hash, second_info.hash);
        assert_eq!(restored.active_tip().height, 2);
    }

    #[test]
    fn replay_uses_selected_buried_activation_heights() {
        let directory = TempDir::new().unwrap();
        let store = RedbHeaderStore::open(directory.path().join("headers.redb")).unwrap();
        let genesis = HeaderDag::new(Network::Regtest).active_tip();
        let mut header = mine_child(genesis.hash, genesis.header.time + 1);
        header.version = Version::from_consensus(1);
        header.nonce = 0;
        while header.validate_pow(Target::MAX_ATTAINABLE_REGTEST).is_err() {
            header.nonce = header.nonce.checked_add(1).unwrap();
        }
        store.append(header).unwrap();
        assert!(matches!(
            store.load_dag(Network::Regtest, header.time),
            Err(HeaderStoreError::Header(
                HeaderError::ObsoleteVersion { .. }
            ))
        ));

        let mut deployments = DeploymentConfig::for_network(Network::Regtest);
        for value in ["bip34@10", "dersig@10", "cltv@10"] {
            deployments.apply_test_activation_height(value).unwrap();
        }
        let restored = store
            .load_dag_with_deployments(deployments, header.time)
            .unwrap();
        assert_eq!(restored.active_tip().hash, header.block_hash());
    }
}
