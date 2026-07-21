//! Durable block-disconnect records for restart-safe chain reorganizations.

use std::{
    path::Path,
    sync::{Mutex, MutexGuard},
};

use bitcoin::{BlockHash, hashes::Hash};
use redb::{Database, ReadableTable, TableDefinition};
use thiserror::Error;

use crate::utxo::{UtxoError, UtxoUndo};

const BLOCK_UNDOS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("block_undos");
const FORMAT_VERSION: u32 = 1;

/// Failures from durable block undo storage.
#[derive(Debug, Error)]
pub enum UndoStoreError {
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
    /// A nested transaction undo record is malformed.
    #[error("UTXO undo: {0}")]
    Utxo(#[from] UtxoError),
    /// A persisted record violates this module's canonical binary format.
    #[error("malformed block undo: {0}")]
    Malformed(&'static str),
    /// An undo record already exists for the block hash.
    #[error("undo already exists for block {0}")]
    Duplicate(BlockHash),
}

/// Append/remove storage for one block's transaction undos.
///
/// The value preserves transaction order. Callers must apply entries in
/// reverse order when disconnecting a block, exactly as
/// [`crate::blockchain::disconnect_block`] does for in-memory data.
pub struct RedbUndoStore {
    db: Database,
    write_guard: Mutex<()>,
}

impl RedbUndoStore {
    /// Opens or creates an undo database at `path`.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, UndoStoreError> {
        let db = Database::create(path)?;
        let transaction = db.begin_write()?;
        {
            let _undos = transaction.open_table(BLOCK_UNDOS)?;
        }
        transaction.commit()?;
        Ok(Self {
            db,
            write_guard: Mutex::new(()),
        })
    }

    /// Stores the complete undo vector for one connected block.
    pub fn insert(&self, block: BlockHash, undos: &[UtxoUndo]) -> Result<(), UndoStoreError> {
        let _guard = self.lock();
        let block_bytes = block.to_byte_array();
        let encoded = encode_block_undo(undos)?;
        let transaction = self.db.begin_write()?;
        {
            let mut table = transaction.open_table(BLOCK_UNDOS)?;
            if table.get(block_bytes.as_slice())?.is_some() {
                return Err(UndoStoreError::Duplicate(block));
            }
            table.insert(block_bytes.as_slice(), encoded.as_slice())?;
        }
        transaction.commit()?;
        Ok(())
    }

    /// Loads a block's transaction undos, if retained.
    pub fn get(&self, block: BlockHash) -> Result<Option<Vec<UtxoUndo>>, UndoStoreError> {
        let transaction = self.db.begin_read()?;
        let table = transaction.open_table(BLOCK_UNDOS)?;
        table
            .get(block.to_byte_array().as_slice())?
            .map(|value| decode_block_undo(value.value()))
            .transpose()
    }

    /// Removes an undo record after its block has been permanently pruned.
    pub fn remove(&self, block: BlockHash) -> Result<bool, UndoStoreError> {
        let _guard = self.lock();
        let transaction = self.db.begin_write()?;
        let removed = {
            let mut table = transaction.open_table(BLOCK_UNDOS)?;
            table.remove(block.to_byte_array().as_slice())?.is_some()
        };
        transaction.commit()?;
        Ok(removed)
    }

    fn lock(&self) -> MutexGuard<'_, ()> {
        self.write_guard.lock().expect("write lock not poisoned")
    }
}

fn encode_block_undo(undos: &[UtxoUndo]) -> Result<Vec<u8>, UndoStoreError> {
    let count =
        u32::try_from(undos.len()).map_err(|_| UndoStoreError::Malformed("transaction count"))?;
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
    bytes.extend_from_slice(&count.to_le_bytes());
    for undo in undos {
        let undo = undo.encode()?;
        let length = u32::try_from(undo.len())
            .map_err(|_| UndoStoreError::Malformed("transaction undo length"))?;
        bytes.extend_from_slice(&length.to_le_bytes());
        bytes.extend_from_slice(&undo);
    }
    Ok(bytes)
}

fn decode_block_undo(bytes: &[u8]) -> Result<Vec<UtxoUndo>, UndoStoreError> {
    let mut cursor = 0;
    let version = take_u32(bytes, &mut cursor, "format version")?;
    if version != FORMAT_VERSION {
        return Err(UndoStoreError::Malformed("unsupported format version"));
    }
    let count = take_u32(bytes, &mut cursor, "transaction count")?;
    let count = usize::try_from(count).expect("u32 fits usize");
    if count > bytes.len().saturating_sub(cursor) / 4 {
        return Err(UndoStoreError::Malformed(
            "transaction count exceeds record",
        ));
    }
    let mut undos = Vec::with_capacity(count);
    for _ in 0..count {
        let length = take_u32(bytes, &mut cursor, "transaction undo length")?;
        let length = usize::try_from(length).expect("u32 fits usize");
        let undo = UtxoUndo::decode(take(bytes, &mut cursor, length, "transaction undo")?)?;
        undos.push(undo);
    }
    if cursor != bytes.len() {
        return Err(UndoStoreError::Malformed("trailing bytes"));
    }
    Ok(undos)
}

fn take<'a>(
    bytes: &'a [u8],
    cursor: &mut usize,
    length: usize,
    field: &'static str,
) -> Result<&'a [u8], UndoStoreError> {
    let end = cursor
        .checked_add(length)
        .ok_or(UndoStoreError::Malformed(field))?;
    let value = bytes
        .get(*cursor..end)
        .ok_or(UndoStoreError::Malformed(field))?;
    *cursor = end;
    Ok(value)
}

fn take_u32(bytes: &[u8], cursor: &mut usize, field: &'static str) -> Result<u32, UndoStoreError> {
    let value = take(bytes, cursor, 4, field)?;
    Ok(u32::from_le_bytes(value.try_into().expect("fixed length")))
}

#[cfg(test)]
mod tests {
    use bitcoin::{OutPoint, Txid, hashes::Hash};
    use tempfile::TempDir;

    use super::*;
    use crate::utxo::{RedbUtxoStore, Utxo, UtxoStore};

    #[test]
    fn survives_reopen_and_removes_a_block_undo() {
        let directory = TempDir::new().unwrap();
        let chainstate = RedbUtxoStore::open(directory.path().join("chainstate.redb")).unwrap();
        let outpoint = OutPoint::new(Txid::all_zeros(), 0);
        let undo = chainstate
            .apply_with_undo(
                &[],
                &[(
                    (outpoint).into(),
                    Utxo {
                        value_sats: 1,
                        height: 1,
                        is_coinbase: false,
                        last_touched: 0,
                        creation_mtp: 0,
                        script_pubkey: vec![0x51],
                    },
                )],
            )
            .unwrap();
        let block = BlockHash::all_zeros();
        let path = directory.path().join("undo.redb");
        let store = RedbUndoStore::open(&path).unwrap();
        store.insert(block, std::slice::from_ref(&undo)).unwrap();
        drop(store);

        let reopened = RedbUndoStore::open(path).unwrap();
        assert_eq!(reopened.get(block).unwrap(), Some(vec![undo]));
        assert!(reopened.remove(block).unwrap());
        assert!(reopened.get(block).unwrap().is_none());
    }
}
