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
use redb::{Database, ReadableTable, TableDefinition};
use thiserror::Error;

use crate::headers::{HeaderDag, HeaderError};

const HEADERS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("headers_by_hash");
const INSERTION_ORDER: TableDefinition<u64, &[u8]> = TableDefinition::new("header_insertion_order");
const META: TableDefinition<&str, &[u8]> = TableDefinition::new("header_metadata");
const NEXT_SEQUENCE_KEY: &str = "next_sequence";

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
}

/// Append-only redb storage for headers already accepted by [`HeaderDag`].
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
        let db = Database::create(path)?;
        let transaction = db.begin_write()?;
        {
            let _headers = transaction.open_table(HEADERS)?;
            let _order = transaction.open_table(INSERTION_ORDER)?;
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
    /// The caller should first use [`HeaderDag::validate_batch_contextual`].
    /// A duplicate or malformed header aborts the complete batch, leaving the
    /// durable prefix unchanged.
    pub fn append_batch(&self, batch: &[Header]) -> Result<(), HeaderStoreError> {
        if batch.is_empty() {
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
                sequence = sequence
                    .checked_add(1)
                    .ok_or(HeaderStoreError::Malformed("header sequence overflow"))?;
            }
            meta.insert(NEXT_SEQUENCE_KEY, sequence.to_le_bytes().as_slice())?;
        }
        transaction.commit()?;
        Ok(())
    }

    /// Returns the number of non-genesis headers persisted in this store.
    pub fn len(&self) -> Result<u64, HeaderStoreError> {
        let transaction = self.db.begin_read()?;
        let meta = transaction.open_table(META)?;
        read_sequence(
            meta.get(NEXT_SEQUENCE_KEY)?
                .as_ref()
                .map(redb::AccessGuard::value),
        )
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
        let transaction = self.db.begin_read()?;
        let order = transaction.open_table(INSERTION_ORDER)?;
        let headers = transaction.open_table(HEADERS)?;
        let mut dag = HeaderDag::new(network);
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
}
