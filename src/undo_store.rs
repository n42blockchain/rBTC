//! Durable block-disconnect records for restart-safe chain reorganizations.

use std::{
    path::Path,
    sync::{Arc, Mutex, MutexGuard},
};

use bitcoin::{BlockHash, hashes::Hash};
use redb::{Database, ReadableTable, TableDefinition, WriteTransaction};
use thiserror::Error;

use crate::{
    execution_store::ExecutionTip,
    utxo::{OutPointKey, Utxo, UtxoError, UtxoUndo},
};

const BLOCK_UNDOS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("block_undos");
const PENDING_TRANSITION: TableDefinition<&str, &[u8]> = TableDefinition::new("pending_transition");
const PENDING_KEY: &str = "active";
const FORMAT_VERSION: u32 = 1;
const PENDING_FORMAT_VERSION: u32 = 1;

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
    /// A previous block transition must be recovered before another begins.
    #[error("pending block transition already exists")]
    PendingExists,
    /// The caller attempted to clear a different transition.
    #[error("pending transition does not target block {0}")]
    WrongPending(BlockHash),
}

/// Write-ahead information sufficient to classify and recover a block commit.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PendingTransition {
    /// Whether the intended operation connects or disconnects `next`.
    pub kind: TransitionKind,
    /// Execution tip before the UTXO mutation.
    pub parent: ExecutionTip,
    /// Execution tip after a completed mutation.
    pub next: ExecutionTip,
    /// Aggregate mutation undo, describing every pre-transition value.
    pub undo: UtxoUndo,
    /// Every output present after the transition among the changed keys.
    pub created: Vec<(OutPointKey, Utxo)>,
}

/// Direction of a write-ahead active-chain transition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransitionKind {
    /// Move from `parent` to `next`.
    Connect,
    /// Move from `next` back to `parent`.
    Disconnect,
}

/// Append/remove storage for one block's transaction undos.
///
/// The value preserves transaction order. Callers must apply entries in
/// reverse order when disconnecting a block, exactly as
/// [`crate::blockchain::disconnect_block`] does for in-memory data.
pub struct RedbUndoStore {
    db: Arc<Database>,
    write_guard: Mutex<()>,
}

impl RedbUndoStore {
    /// Opens or creates an undo database at `path`.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, UndoStoreError> {
        Self::from_database(Arc::new(Database::create(path)?))
    }

    pub(crate) fn from_database(db: Arc<Database>) -> Result<Self, UndoStoreError> {
        let transaction = db.begin_write()?;
        {
            let _undos = transaction.open_table(BLOCK_UNDOS)?;
            let _pending = transaction.open_table(PENDING_TRANSITION)?;
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
        let transaction = self.db.begin_write()?;
        insert_transaction(&transaction, block, undos)?;
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
        let removed = remove_transaction(&transaction, block)?;
        transaction.commit()?;
        Ok(removed)
    }

    /// Durably records intent before the corresponding UTXO transaction starts.
    pub fn prepare_transition(&self, pending: &PendingTransition) -> Result<(), UndoStoreError> {
        let _guard = self.lock();
        let encoded = encode_pending_transition(pending)?;
        let transaction = self.db.begin_write()?;
        {
            let mut table = transaction.open_table(PENDING_TRANSITION)?;
            if table.get(PENDING_KEY)?.is_some() {
                return Err(UndoStoreError::PendingExists);
            }
            table.insert(PENDING_KEY, encoded.as_slice())?;
        }
        transaction.commit()?;
        Ok(())
    }

    /// Loads an interrupted block transition, if one exists.
    pub fn pending_transition(&self) -> Result<Option<PendingTransition>, UndoStoreError> {
        let transaction = self.db.begin_read()?;
        let table = transaction.open_table(PENDING_TRANSITION)?;
        table
            .get(PENDING_KEY)?
            .map(|value| decode_pending_transition(value.value()))
            .transpose()
    }

    /// Clears a recovered or fully completed transition.
    pub fn clear_transition(&self, expected: BlockHash) -> Result<(), UndoStoreError> {
        let _guard = self.lock();
        let transaction = self.db.begin_write()?;
        {
            let mut table = transaction.open_table(PENDING_TRANSITION)?;
            let target = table
                .get(PENDING_KEY)?
                .map(|value| decode_pending_transition(value.value()))
                .transpose()?
                .map(|pending| pending.next.hash);
            if let Some(target) = target {
                if target != expected {
                    return Err(UndoStoreError::WrongPending(expected));
                }
                table.remove(PENDING_KEY)?;
            }
        }
        transaction.commit()?;
        Ok(())
    }

    fn lock(&self) -> MutexGuard<'_, ()> {
        self.write_guard.lock().expect("write lock not poisoned")
    }
}

pub(crate) fn insert_transaction(
    transaction: &WriteTransaction,
    block: BlockHash,
    undos: &[UtxoUndo],
) -> Result<(), UndoStoreError> {
    let block_bytes = block.to_byte_array();
    let encoded = encode_block_undo(undos)?;
    let mut table = transaction.open_table(BLOCK_UNDOS)?;
    if table.get(block_bytes.as_slice())?.is_some() {
        return Err(UndoStoreError::Duplicate(block));
    }
    table.insert(block_bytes.as_slice(), encoded.as_slice())?;
    Ok(())
}

pub(crate) fn remove_transaction(
    transaction: &WriteTransaction,
    block: BlockHash,
) -> Result<bool, UndoStoreError> {
    let mut table = transaction.open_table(BLOCK_UNDOS)?;
    Ok(table.remove(block.to_byte_array().as_slice())?.is_some())
}

fn encode_tip(tip: ExecutionTip, bytes: &mut Vec<u8>) {
    bytes.extend_from_slice(&tip.height.to_le_bytes());
    bytes.extend_from_slice(&tip.hash.to_byte_array());
}

fn decode_tip(
    bytes: &[u8],
    cursor: &mut usize,
    field: &'static str,
) -> Result<ExecutionTip, UndoStoreError> {
    let height = take_u32(bytes, cursor, field)?;
    let hash = BlockHash::from_byte_array(
        take(bytes, cursor, 32, field)?
            .try_into()
            .expect("checked hash length"),
    );
    Ok(ExecutionTip { height, hash })
}

fn encode_pending_transition(pending: &PendingTransition) -> Result<Vec<u8>, UndoStoreError> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&PENDING_FORMAT_VERSION.to_le_bytes());
    bytes.push(match pending.kind {
        TransitionKind::Connect => 0,
        TransitionKind::Disconnect => 1,
    });
    encode_tip(pending.parent, &mut bytes);
    encode_tip(pending.next, &mut bytes);
    let undo = pending.undo.encode()?;
    let undo_len =
        u32::try_from(undo.len()).map_err(|_| UndoStoreError::Malformed("pending undo length"))?;
    bytes.extend_from_slice(&undo_len.to_le_bytes());
    bytes.extend_from_slice(&undo);
    let created_count = u32::try_from(pending.created.len())
        .map_err(|_| UndoStoreError::Malformed("pending created count"))?;
    bytes.extend_from_slice(&created_count.to_le_bytes());
    for (outpoint, utxo) in &pending.created {
        let utxo = utxo.encode()?;
        let length = u32::try_from(utxo.len())
            .map_err(|_| UndoStoreError::Malformed("pending UTXO length"))?;
        bytes.extend_from_slice(outpoint.as_bytes());
        bytes.extend_from_slice(&length.to_le_bytes());
        bytes.extend_from_slice(&utxo);
    }
    Ok(bytes)
}

fn decode_pending_transition(bytes: &[u8]) -> Result<PendingTransition, UndoStoreError> {
    let mut cursor = 0;
    if take_u32(bytes, &mut cursor, "pending format version")? != PENDING_FORMAT_VERSION {
        return Err(UndoStoreError::Malformed(
            "unsupported pending format version",
        ));
    }
    let kind = match take(bytes, &mut cursor, 1, "pending transition kind")?[0] {
        0 => TransitionKind::Connect,
        1 => TransitionKind::Disconnect,
        _ => return Err(UndoStoreError::Malformed("pending transition kind")),
    };
    let parent = decode_tip(bytes, &mut cursor, "pending parent tip")?;
    let next = decode_tip(bytes, &mut cursor, "pending next tip")?;
    let undo_len = usize::try_from(take_u32(bytes, &mut cursor, "pending undo length")?)
        .expect("u32 fits usize");
    let undo = UtxoUndo::decode(take(bytes, &mut cursor, undo_len, "pending undo")?)?;
    let created_count = usize::try_from(take_u32(bytes, &mut cursor, "pending created count")?)
        .expect("u32 fits usize");
    if created_count > bytes.len().saturating_sub(cursor) / 40 {
        return Err(UndoStoreError::Malformed(
            "pending created count exceeds record",
        ));
    }
    let mut created = Vec::with_capacity(created_count);
    for _ in 0..created_count {
        let outpoint =
            OutPointKey::from_bytes(take(bytes, &mut cursor, 36, "pending created outpoint")?)?;
        let length = usize::try_from(take_u32(bytes, &mut cursor, "pending UTXO length")?)
            .expect("u32 fits usize");
        let utxo = Utxo::decode(take(bytes, &mut cursor, length, "pending UTXO")?)?;
        created.push((outpoint, utxo));
    }
    if cursor != bytes.len() {
        return Err(UndoStoreError::Malformed("trailing pending bytes"));
    }
    Ok(PendingTransition {
        kind,
        parent,
        next,
        undo,
        created,
    })
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
        let pending = PendingTransition {
            kind: TransitionKind::Connect,
            parent: ExecutionTip {
                height: 0,
                hash: BlockHash::from_byte_array([1; 32]),
            },
            next: ExecutionTip {
                height: 1,
                hash: block,
            },
            undo: undo.clone(),
            created: vec![(
                outpoint.into(),
                Utxo {
                    value_sats: 2,
                    height: 2,
                    is_coinbase: false,
                    last_touched: 1,
                    creation_mtp: 1,
                    script_pubkey: vec![0x51],
                },
            )],
        };
        store.prepare_transition(&pending).unwrap();
        drop(store);

        let reopened = RedbUndoStore::open(path).unwrap();
        assert_eq!(reopened.get(block).unwrap(), Some(vec![undo]));
        assert_eq!(reopened.pending_transition().unwrap(), Some(pending));
        reopened.clear_transition(block).unwrap();
        assert!(reopened.remove(block).unwrap());
        assert!(reopened.get(block).unwrap().is_none());
    }
}
