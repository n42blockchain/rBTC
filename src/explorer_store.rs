//! Persistent active-chain projections for the embedded block explorer.

use std::{collections::BTreeSet, path::Path, str::FromStr, sync::Mutex};

use bitcoin::{
    Address, Block, BlockHash, Network, Txid,
    hashes::{Hash, sha256},
};
use redb::{Database, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    api::{
        ExplorerBlock, ExplorerIndex, ExplorerTransaction, ExplorerUtxo, MAX_UTXO_PAGE_OFFSET,
        MAX_UTXO_PAGE_SIZE,
    },
    blockchain::AppliedBlock,
    chain_store::RedbChainStore,
    chainstate::is_unspendable,
    execution_store::ExecutionTip,
    utxo::{OutPointKey, UtxoError},
};

const META: TableDefinition<&str, &[u8]> = TableDefinition::new("explorer_metadata");
const BLOCKS: TableDefinition<u32, &[u8]> = TableDefinition::new("explorer_blocks");
const TRANSACTIONS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("explorer_transactions");
const ADDRESS_UTXOS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("explorer_address_utxos");
const BLOCK_UNDOS: TableDefinition<u32, &[u8]> = TableDefinition::new("explorer_block_undos");
const GENESIS_KEY: &str = "genesis";
const TIP_KEY: &str = "tip";
const BASELINE_KEY: &str = "utxo_baseline";
const BASELINE_PAGE_SIZE: usize = 4_096;

/// Persistent explorer projection errors.
#[derive(Debug, Error)]
pub enum ExplorerStoreError {
    /// Database open/create failed.
    #[error("redb database: {0}")]
    Database(#[from] redb::DatabaseError),
    /// Transaction creation failed.
    #[error("redb transaction: {0}")]
    Transaction(#[from] redb::TransactionError),
    /// Table access failed.
    #[error("redb table: {0}")]
    Table(#[from] redb::TableError),
    /// Key/value access failed.
    #[error("redb storage: {0}")]
    Storage(#[from] redb::StorageError),
    /// Transaction commit failed.
    #[error("redb commit: {0}")]
    Commit(#[from] redb::CommitError),
    /// Projection serialization failed.
    #[error("explorer encoding: {0}")]
    Encoding(#[from] serde_json::Error),
    /// Reading the chainstate UTXO baseline failed.
    #[error("explorer UTXO baseline: {0}")]
    Utxo(#[from] UtxoError),
    /// The selected network does not match the database.
    #[error("explorer database belongs to another Bitcoin network")]
    NetworkMismatch,
    /// The requested address is invalid for this database's network.
    #[error("invalid address: {0}")]
    Address(String),
    /// Persisted data or a requested transition violates index invariants.
    #[error("invalid explorer transition: {0}")]
    Invalid(&'static str),
}

#[derive(Debug, Deserialize, Serialize)]
struct ExplorerUndo {
    parent_height: u32,
    parent_hash: String,
    created_keys: Vec<Vec<u8>>,
    spent: Vec<(Vec<u8>, Vec<u8>)>,
    previous_transactions: Vec<(Vec<u8>, Option<Vec<u8>>)>,
}

/// redb-backed block, transaction, and current address-UTXO projections.
pub struct RedbExplorerIndex {
    db: Database,
    network: Network,
    write_guard: Mutex<()>,
}

impl RedbExplorerIndex {
    /// Opens or creates an explorer index for `network`.
    pub fn open(path: impl AsRef<Path>, network: Network) -> Result<Self, ExplorerStoreError> {
        let genesis = bitcoin::blockdata::constants::genesis_block(network).block_hash();
        let db = Database::create(path)?;
        let transaction = db.begin_write()?;
        {
            let mut meta = transaction.open_table(META)?;
            let stored = meta.get(GENESIS_KEY)?.map(|value| value.value().to_vec());
            if let Some(stored) = stored {
                if stored != genesis.to_byte_array() {
                    return Err(ExplorerStoreError::NetworkMismatch);
                }
                if meta.get(TIP_KEY)?.is_none() {
                    return Err(ExplorerStoreError::Invalid("missing explorer tip"));
                }
            } else {
                meta.insert(GENESIS_KEY, genesis.to_byte_array().as_slice())?;
                meta.insert(
                    TIP_KEY,
                    encode_tip(ExecutionTip {
                        height: 0,
                        hash: genesis,
                    })
                    .as_slice(),
                )?;
            }
            let _blocks = transaction.open_table(BLOCKS)?;
            let _transactions = transaction.open_table(TRANSACTIONS)?;
            let _utxos = transaction.open_table(ADDRESS_UTXOS)?;
            let _undos = transaction.open_table(BLOCK_UNDOS)?;
        }
        transaction.commit()?;
        Ok(Self {
            db,
            network,
            write_guard: Mutex::new(()),
        })
    }

    /// Returns the highest active-chain block reflected by this index.
    pub fn tip(&self) -> Result<ExecutionTip, ExplorerStoreError> {
        let transaction = self.db.begin_read()?;
        let meta = transaction.open_table(META)?;
        let value = meta
            .get(TIP_KEY)?
            .ok_or(ExplorerStoreError::Invalid("missing explorer tip"))?;
        decode_tip(value.value())
    }

    /// Returns the lowest tip whose current UTXO projection was imported
    /// without historical block and transaction rows.
    pub fn baseline(&self) -> Result<Option<ExecutionTip>, ExplorerStoreError> {
        let transaction = self.db.begin_read()?;
        let meta = transaction.open_table(META)?;
        meta.get(BASELINE_KEY)?
            .map(|value| decode_tip(value.value()))
            .transpose()
    }

    /// Atomically replaces all projections with a cursor-streamed UTXO baseline.
    ///
    /// This is used when execution began from a trusted snapshot: historical
    /// blocks before `tip` remain unavailable, while current address UTXOs and
    /// every subsequently validated block remain fully queryable and reversible.
    pub fn replace_with_chainstate_baseline(
        &self,
        tip: ExecutionTip,
        chainstate: &RedbChainStore,
    ) -> Result<u64, ExplorerStoreError> {
        let _guard = self.write_guard.lock().expect("explorer lock not poisoned");
        let transaction = self.db.begin_write()?;
        let mut count = 0_u64;
        {
            let mut blocks = transaction.open_table(BLOCKS)?;
            let mut transactions = transaction.open_table(TRANSACTIONS)?;
            let mut utxos = transaction.open_table(ADDRESS_UTXOS)?;
            let mut undos = transaction.open_table(BLOCK_UNDOS)?;
            blocks.retain(|_, _| false)?;
            transactions.retain(|_, _| false)?;
            utxos.retain(|_, _| false)?;
            undos.retain(|_, _| false)?;
            let mut cursor = None;
            loop {
                let page = chainstate.utxo_snapshot_page(cursor, BASELINE_PAGE_SIZE)?;
                if page.is_empty() {
                    break;
                }
                for (outpoint, utxo) in &page {
                    if is_unspendable(bitcoin::Script::from_bytes(&utxo.script_pubkey)) {
                        continue;
                    }
                    let bitcoin_outpoint = outpoint.to_outpoint();
                    let key = address_utxo_key(&utxo.script_pubkey, *outpoint);
                    if utxos
                        .insert(
                            key.as_slice(),
                            serde_json::to_vec(&ExplorerUtxo {
                                txid: bitcoin_outpoint.txid.to_string(),
                                vout: bitcoin_outpoint.vout,
                                value_sats: utxo.value_sats,
                                height: utxo.height,
                            })?
                            .as_slice(),
                        )?
                        .is_some()
                    {
                        return Err(ExplorerStoreError::Invalid(
                            "duplicate baseline explorer UTXO",
                        ));
                    }
                    count = count
                        .checked_add(1)
                        .ok_or(ExplorerStoreError::Invalid("baseline UTXO count"))?;
                }
                cursor = page.last().map(|(outpoint, _)| *outpoint);
                if page.len() < BASELINE_PAGE_SIZE {
                    break;
                }
            }
            let mut meta = transaction.open_table(META)?;
            let encoded = encode_tip(tip);
            meta.insert(TIP_KEY, encoded.as_slice())?;
            meta.insert(BASELINE_KEY, encoded.as_slice())?;
        }
        transaction.commit()?;
        Ok(count)
    }

    /// Atomically indexes one fully validated child block.
    #[allow(clippy::too_many_lines)]
    pub fn connect(
        &self,
        height: u32,
        block: &Block,
        applied: &AppliedBlock,
    ) -> Result<(), ExplorerStoreError> {
        let undo_offset = applied
            .transaction_undos
            .len()
            .checked_sub(block.txdata.len())
            .ok_or(ExplorerStoreError::Invalid(
                "block undo does not match block",
            ))?;
        if block.block_hash() != applied.hash || undo_offset > 1 {
            return Err(ExplorerStoreError::Invalid(
                "block undo does not match block",
            ));
        }
        let _guard = self.write_guard.lock().expect("explorer lock not poisoned");
        let transaction = self.db.begin_write()?;
        {
            let mut meta = transaction.open_table(META)?;
            let current_value = meta
                .get(TIP_KEY)?
                .ok_or(ExplorerStoreError::Invalid("missing explorer tip"))?;
            let current = decode_tip(current_value.value())?;
            drop(current_value);
            if current.height.checked_add(1) != Some(height)
                || block.header.prev_blockhash != current.hash
            {
                return Err(ExplorerStoreError::Invalid(
                    "block does not extend explorer tip",
                ));
            }
            let mut blocks = transaction.open_table(BLOCKS)?;
            let mut transactions = transaction.open_table(TRANSACTIONS)?;
            let mut utxos = transaction.open_table(ADDRESS_UTXOS)?;
            let mut undos = transaction.open_table(BLOCK_UNDOS)?;
            if blocks.get(height)?.is_some() || undos.get(height)?.is_some() {
                return Err(ExplorerStoreError::Invalid(
                    "explorer height already indexed",
                ));
            }

            let mut undo = ExplorerUndo {
                parent_height: current.height,
                parent_hash: current.hash.to_string(),
                created_keys: Vec::new(),
                spent: Vec::new(),
                previous_transactions: Vec::new(),
            };
            let mut created_keys = BTreeSet::new();
            for exception_undo in &applied.transaction_undos[..undo_offset] {
                if !exception_undo.created().is_empty() {
                    return Err(ExplorerStoreError::Invalid("BIP30 explorer undo shape"));
                }
                for (outpoint, previous) in exception_undo.spent() {
                    let key = address_utxo_key(&previous.script_pubkey, *outpoint);
                    let removed = utxos
                        .remove(key.as_slice())?
                        .ok_or(ExplorerStoreError::Invalid("BIP30 explorer UTXO missing"))?;
                    undo.spent.push((key, removed.value().to_vec()));
                }
            }
            for (bitcoin_tx, transaction_undo) in block
                .txdata
                .iter()
                .zip(&applied.transaction_undos[undo_offset..])
            {
                for (outpoint, previous) in transaction_undo.spent() {
                    let key = address_utxo_key(&previous.script_pubkey, *outpoint);
                    let removed = utxos
                        .remove(key.as_slice())?
                        .ok_or(ExplorerStoreError::Invalid("spent explorer UTXO missing"))?;
                    if !created_keys.remove(&key) {
                        undo.spent.push((key, removed.value().to_vec()));
                    }
                }

                let txid = bitcoin_tx.compute_txid();
                let txid_key = txid.to_byte_array();
                let previous = transactions
                    .get(txid_key.as_slice())?
                    .map(|value| value.value().to_vec());
                undo.previous_transactions
                    .push((txid_key.to_vec(), previous));
                let vbytes = u32::try_from(bitcoin_tx.vsize())
                    .map_err(|_| ExplorerStoreError::Invalid("transaction vsize"))?;
                transactions.insert(
                    txid_key.as_slice(),
                    serde_json::to_vec(&ExplorerTransaction {
                        txid: txid.to_string(),
                        confirmed_height: Some(height),
                        vbytes,
                    })?
                    .as_slice(),
                )?;

                for (vout, output) in bitcoin_tx.output.iter().enumerate() {
                    if is_unspendable(&output.script_pubkey) {
                        continue;
                    }
                    let vout = u32::try_from(vout)
                        .map_err(|_| ExplorerStoreError::Invalid("output index"))?;
                    let outpoint = OutPointKey::from(bitcoin::OutPoint::new(txid, vout));
                    let key = address_utxo_key(output.script_pubkey.as_bytes(), outpoint);
                    if utxos
                        .insert(
                            key.as_slice(),
                            serde_json::to_vec(&ExplorerUtxo {
                                txid: txid.to_string(),
                                vout,
                                value_sats: output.value.to_sat(),
                                height,
                            })?
                            .as_slice(),
                        )?
                        .is_some()
                    {
                        return Err(ExplorerStoreError::Invalid("duplicate explorer UTXO"));
                    }
                    if !created_keys.insert(key) {
                        return Err(ExplorerStoreError::Invalid(
                            "duplicate created explorer UTXO",
                        ));
                    }
                }
            }
            undo.created_keys = created_keys.into_iter().collect();

            blocks.insert(
                height,
                serde_json::to_vec(&ExplorerBlock {
                    height,
                    hash: applied.hash.to_string(),
                    time: u64::from(block.header.time),
                    transaction_count: u32::try_from(block.txdata.len())
                        .map_err(|_| ExplorerStoreError::Invalid("transaction count"))?,
                })?
                .as_slice(),
            )?;
            undos.insert(height, serde_json::to_vec(&undo)?.as_slice())?;
            meta.insert(
                TIP_KEY,
                encode_tip(ExecutionTip {
                    height,
                    hash: applied.hash,
                })
                .as_slice(),
            )?;
        }
        transaction.commit()?;
        Ok(())
    }

    /// Atomically removes the current active-chain projection using its own undo record.
    pub fn disconnect_tip(&self) -> Result<ExecutionTip, ExplorerStoreError> {
        let _guard = self.write_guard.lock().expect("explorer lock not poisoned");
        let transaction = self.db.begin_write()?;
        let parent;
        {
            let mut meta = transaction.open_table(META)?;
            let current_value = meta
                .get(TIP_KEY)?
                .ok_or(ExplorerStoreError::Invalid("missing explorer tip"))?;
            let current = decode_tip(current_value.value())?;
            drop(current_value);
            if current.height == 0 {
                return Err(ExplorerStoreError::Invalid("cannot disconnect genesis"));
            }
            if meta
                .get(BASELINE_KEY)?
                .map(|value| decode_tip(value.value()))
                .transpose()?
                == Some(current)
            {
                return Err(ExplorerStoreError::Invalid(
                    "cannot disconnect explorer UTXO baseline",
                ));
            }
            let mut blocks = transaction.open_table(BLOCKS)?;
            let mut transactions = transaction.open_table(TRANSACTIONS)?;
            let mut utxos = transaction.open_table(ADDRESS_UTXOS)?;
            let mut undos = transaction.open_table(BLOCK_UNDOS)?;
            let block_value = blocks
                .get(current.height)?
                .ok_or(ExplorerStoreError::Invalid("missing explorer block"))?;
            let block_summary: ExplorerBlock = serde_json::from_slice(block_value.value())?;
            if block_summary.hash != current.hash.to_string() {
                return Err(ExplorerStoreError::Invalid("explorer block tip mismatch"));
            }
            drop(block_value);
            let encoded = undos
                .get(current.height)?
                .ok_or(ExplorerStoreError::Invalid("missing explorer undo"))?;
            let undo: ExplorerUndo = serde_json::from_slice(encoded.value())?;
            let parent_hash = BlockHash::from_str(&undo.parent_hash)
                .map_err(|_| ExplorerStoreError::Invalid("explorer parent hash"))?;
            parent = ExecutionTip {
                height: undo.parent_height,
                hash: parent_hash,
            };
            if parent.height.checked_add(1) != Some(current.height) {
                return Err(ExplorerStoreError::Invalid("explorer undo height"));
            }
            drop(encoded);

            for key in &undo.created_keys {
                if utxos.remove(key.as_slice())?.is_none() {
                    return Err(ExplorerStoreError::Invalid("created explorer UTXO missing"));
                }
            }
            for (key, value) in &undo.spent {
                if utxos.insert(key.as_slice(), value.as_slice())?.is_some() {
                    return Err(ExplorerStoreError::Invalid("restored explorer UTXO exists"));
                }
            }
            for (txid, previous) in undo.previous_transactions.iter().rev() {
                if transactions.remove(txid.as_slice())?.is_none() {
                    return Err(ExplorerStoreError::Invalid("explorer transaction missing"));
                }
                if let Some(previous) = previous {
                    transactions.insert(txid.as_slice(), previous.as_slice())?;
                }
            }
            if blocks.remove(current.height)?.is_none() {
                return Err(ExplorerStoreError::Invalid("missing explorer block"));
            }
            undos.remove(current.height)?;
            meta.insert(TIP_KEY, encode_tip(parent).as_slice())?;
        }
        transaction.commit()?;
        Ok(parent)
    }
}

impl ExplorerIndex for RedbExplorerIndex {
    fn block(&self, height: u32) -> Result<Option<ExplorerBlock>, String> {
        let transaction = self.db.begin_read().map_err(|error| error.to_string())?;
        let blocks = transaction
            .open_table(BLOCKS)
            .map_err(|error| error.to_string())?;
        blocks
            .get(height)
            .map_err(|error| error.to_string())?
            .map(|value| serde_json::from_slice(value.value()).map_err(|error| error.to_string()))
            .transpose()
    }

    fn transaction(&self, txid: &str) -> Result<Option<ExplorerTransaction>, String> {
        let txid = Txid::from_str(txid).map_err(|error| error.to_string())?;
        let transaction = self.db.begin_read().map_err(|error| error.to_string())?;
        let transactions = transaction
            .open_table(TRANSACTIONS)
            .map_err(|error| error.to_string())?;
        transactions
            .get(txid.to_byte_array().as_slice())
            .map_err(|error| error.to_string())?
            .map(|value| serde_json::from_slice(value.value()).map_err(|error| error.to_string()))
            .transpose()
    }

    fn validate_address(&self, address: &str) -> Result<(), String> {
        Address::from_str(address)
            .map_err(|error| error.to_string())?
            .require_network(self.network)
            .map(drop)
            .map_err(|error| error.to_string())
    }

    fn address_utxos(
        &self,
        address: &str,
        offset: u32,
        limit: u32,
    ) -> Result<Vec<ExplorerUtxo>, String> {
        self.address_utxos_checked(address, offset, limit)
            .map_err(|error| error.to_string())
    }
}

impl RedbExplorerIndex {
    fn address_utxos_checked(
        &self,
        address: &str,
        offset: u32,
        limit: u32,
    ) -> Result<Vec<ExplorerUtxo>, ExplorerStoreError> {
        if offset > MAX_UTXO_PAGE_OFFSET || limit == 0 || limit > MAX_UTXO_PAGE_SIZE + 1 {
            return Err(ExplorerStoreError::Invalid(
                "address UTXO page window exceeds limits",
            ));
        }
        let address = Address::from_str(address)
            .map_err(|error| ExplorerStoreError::Address(error.to_string()))?
            .require_network(self.network)
            .map_err(|error| ExplorerStoreError::Address(error.to_string()))?;
        let prefix = sha256::Hash::hash(address.script_pubkey().as_bytes()).to_byte_array();
        let mut start = [0_u8; 68];
        start[..32].copy_from_slice(&prefix);
        let mut end = [0xff_u8; 68];
        end[..32].copy_from_slice(&prefix);
        let transaction = self.db.begin_read()?;
        let utxos = transaction.open_table(ADDRESS_UTXOS)?;
        let offset = usize::try_from(offset)
            .map_err(|_| ExplorerStoreError::Invalid("address UTXO offset"))?;
        let limit = usize::try_from(limit)
            .map_err(|_| ExplorerStoreError::Invalid("address UTXO limit"))?;
        utxos
            .range(start.as_slice()..=end.as_slice())?
            .skip(offset)
            .take(limit)
            .map(|entry| {
                let (_, value) = entry?;
                Ok(serde_json::from_slice(value.value())?)
            })
            .collect()
    }
}

fn address_utxo_key(script_pubkey: &[u8], outpoint: OutPointKey) -> Vec<u8> {
    let mut key = Vec::with_capacity(68);
    key.extend_from_slice(&sha256::Hash::hash(script_pubkey).to_byte_array());
    key.extend_from_slice(outpoint.as_bytes());
    key
}

fn encode_tip(tip: ExecutionTip) -> [u8; 36] {
    let mut bytes = [0_u8; 36];
    bytes[..4].copy_from_slice(&tip.height.to_le_bytes());
    bytes[4..].copy_from_slice(&tip.hash.to_byte_array());
    bytes
}

fn decode_tip(bytes: &[u8]) -> Result<ExecutionTip, ExplorerStoreError> {
    if bytes.len() != 36 {
        return Err(ExplorerStoreError::Invalid("explorer tip length"));
    }
    let hash: [u8; 32] = bytes[4..]
        .try_into()
        .map_err(|_| ExplorerStoreError::Invalid("explorer tip hash"))?;
    Ok(ExecutionTip {
        height: u32::from_le_bytes(
            bytes[..4]
                .try_into()
                .map_err(|_| ExplorerStoreError::Invalid("explorer tip height"))?,
        ),
        hash: BlockHash::from_byte_array(hash),
    })
}

#[cfg(test)]
mod tests {
    use bitcoin::{
        Amount, OutPoint, PubkeyHash, ScriptBuf, Sequence, Transaction, TxIn, TxMerkleNode, TxOut,
        Witness,
        absolute::LockTime,
        block::{Header, Version as HeaderVersion},
        pow::Target,
        transaction::Version,
    };
    use tempfile::TempDir;

    use super::*;
    use crate::utxo::{Utxo, UtxoStore, UtxoUndo};

    fn transaction(previous: Option<OutPoint>, script: ScriptBuf) -> Transaction {
        Transaction {
            version: Version::ONE,
            lock_time: LockTime::ZERO,
            input: vec![TxIn {
                previous_output: previous.unwrap_or_else(OutPoint::null),
                script_sig: if previous.is_none() {
                    ScriptBuf::from_bytes(vec![1, 1])
                } else {
                    ScriptBuf::new()
                },
                sequence: Sequence::MAX,
                witness: Witness::new(),
            }],
            output: vec![TxOut {
                value: Amount::from_sat(50),
                script_pubkey: script,
            }],
        }
    }

    fn block(parent: BlockHash, time: u32, txdata: Vec<Transaction>) -> Block {
        let mut block = Block {
            header: Header {
                version: HeaderVersion::ONE,
                prev_blockhash: parent,
                merkle_root: TxMerkleNode::all_zeros(),
                time,
                bits: Target::MAX_ATTAINABLE_REGTEST.to_compact_lossy(),
                nonce: 0,
            },
            txdata,
        };
        block.header.merkle_root = block.compute_merkle_root().unwrap();
        block
    }

    fn created_undo(transaction: &Transaction) -> UtxoUndo {
        UtxoUndo::new(
            Vec::new(),
            vec![OutPointKey::from(OutPoint::new(
                transaction.compute_txid(),
                0,
            ))],
        )
    }

    #[test]
    fn snapshot_baseline_streams_current_utxos_and_survives_reopen() {
        let directory = TempDir::new().unwrap();
        let chainstate =
            RedbChainStore::open(directory.path().join("chainstate.redb"), Network::Regtest)
                .unwrap();
        let script = ScriptBuf::new_p2pkh(&PubkeyHash::from_byte_array([7; 20]));
        let address = Address::from_script(&script, Network::Regtest).unwrap();
        let outpoint = OutPointKey::from(OutPoint::new(Txid::from_byte_array([8; 32]), 3));
        chainstate
            .apply(
                &[],
                &[(
                    outpoint,
                    Utxo {
                        value_sats: 99,
                        height: 50,
                        is_coinbase: false,
                        last_touched: 0,
                        creation_mtp: 0,
                        script_pubkey: script.into_bytes(),
                    },
                )],
            )
            .unwrap();
        let baseline = ExecutionTip {
            height: 50,
            hash: BlockHash::from_byte_array([9; 32]),
        };
        let path = directory.path().join("explorer.redb");
        let index = RedbExplorerIndex::open(&path, Network::Regtest).unwrap();
        assert_eq!(
            index
                .replace_with_chainstate_baseline(baseline, &chainstate)
                .unwrap(),
            1
        );
        assert_eq!(index.tip().unwrap(), baseline);
        assert_eq!(index.baseline().unwrap(), Some(baseline));
        assert_eq!(
            index.address_utxos(&address.to_string(), 0, 10).unwrap()[0].value_sats,
            99
        );
        assert!(matches!(
            index.disconnect_tip(),
            Err(ExplorerStoreError::Invalid(
                "cannot disconnect explorer UTXO baseline"
            ))
        ));
        drop(index);
        let reopened = RedbExplorerIndex::open(path, Network::Regtest).unwrap();
        assert_eq!(reopened.tip().unwrap(), baseline);
        assert_eq!(reopened.baseline().unwrap(), Some(baseline));
    }

    #[test]
    fn explorer_does_not_publish_provably_unspendable_outputs_as_utxos() {
        let directory = TempDir::new().unwrap();
        let index =
            RedbExplorerIndex::open(directory.path().join("explorer.redb"), Network::Regtest)
                .unwrap();
        let genesis = index.tip().unwrap();
        let script = ScriptBuf::new_p2pkh(&PubkeyHash::from_byte_array([9; 20]));
        let mut coinbase = transaction(None, script.clone());
        coinbase.output.insert(
            0,
            TxOut {
                value: Amount::from_sat(1),
                script_pubkey: ScriptBuf::from_bytes(vec![0x6a, 0x01, 0x01]),
            },
        );
        let spendable = OutPointKey::from(OutPoint::new(coinbase.compute_txid(), 1));
        let block = block(genesis.hash, 1, vec![coinbase]);
        index
            .connect(
                1,
                &block,
                &AppliedBlock {
                    hash: block.block_hash(),
                    transaction_undos: vec![UtxoUndo::new(Vec::new(), vec![spendable])],
                },
            )
            .unwrap();

        let read = index.db.begin_read().unwrap();
        let utxos = read.open_table(ADDRESS_UTXOS).unwrap();
        assert_eq!(utxos.iter().unwrap().count(), 1);
        drop(utxos);
        drop(read);
        assert_eq!(index.disconnect_tip().unwrap(), genesis);
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn persists_queries_and_reverses_active_chain_projections() {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("explorer.redb");
        let index = RedbExplorerIndex::open(&path, Network::Regtest).unwrap();
        let genesis = index.tip().unwrap();
        let script_a = ScriptBuf::new_p2pkh(&PubkeyHash::from_byte_array([1; 20]));
        let script_b = ScriptBuf::new_p2pkh(&PubkeyHash::from_byte_array([2; 20]));
        let address_a = Address::from_script(&script_a, Network::Regtest).unwrap();
        let address_b = Address::from_script(&script_b, Network::Regtest).unwrap();
        let coinbase_one = transaction(None, script_a.clone());
        let coinbase_one_outpoint = OutPoint::new(coinbase_one.compute_txid(), 0);
        let block_one = block(genesis.hash, 1, vec![coinbase_one.clone()]);
        let applied_one = AppliedBlock {
            hash: block_one.block_hash(),
            transaction_undos: vec![created_undo(&coinbase_one)],
        };
        index.connect(1, &block_one, &applied_one).unwrap();

        assert_eq!(index.block(1).unwrap().unwrap().transaction_count, 1);
        assert_eq!(
            index
                .transaction(&coinbase_one.compute_txid().to_string())
                .unwrap()
                .unwrap()
                .confirmed_height,
            Some(1)
        );
        assert_eq!(
            index
                .address_utxos(&address_a.to_string(), 0, 100)
                .unwrap()
                .len(),
            1
        );
        drop(index);

        let index = RedbExplorerIndex::open(&path, Network::Regtest).unwrap();
        assert_eq!(index.tip().unwrap().height, 1);
        let duplicate_block = block(block_one.block_hash(), 2, vec![coinbase_one.clone()]);
        index
            .connect(
                2,
                &duplicate_block,
                &AppliedBlock {
                    hash: duplicate_block.block_hash(),
                    transaction_undos: vec![
                        UtxoUndo::new(
                            vec![(
                                coinbase_one_outpoint.into(),
                                Utxo {
                                    value_sats: 50,
                                    height: 1,
                                    is_coinbase: true,
                                    last_touched: 1,
                                    creation_mtp: 0,
                                    script_pubkey: script_a.as_bytes().to_vec(),
                                },
                            )],
                            Vec::new(),
                        ),
                        created_undo(&coinbase_one),
                    ],
                },
            )
            .unwrap();
        assert_eq!(index.disconnect_tip().unwrap().height, 1);
        assert_eq!(
            index
                .address_utxos(&address_a.to_string(), 0, 100)
                .unwrap()
                .len(),
            1
        );

        let coinbase_two = transaction(None, script_b.clone());
        let spend = transaction(Some(coinbase_one_outpoint), script_b.clone());
        let spend_outpoint = OutPointKey::from(OutPoint::new(spend.compute_txid(), 0));
        let spend_again = transaction(Some(spend_outpoint.to_outpoint()), script_b.clone());
        let spend_again_outpoint = OutPointKey::from(OutPoint::new(spend_again.compute_txid(), 0));
        let block_two = block(
            block_one.block_hash(),
            2,
            vec![coinbase_two.clone(), spend.clone(), spend_again.clone()],
        );
        let applied_two = AppliedBlock {
            hash: block_two.block_hash(),
            transaction_undos: vec![
                created_undo(&coinbase_two),
                UtxoUndo::new(
                    vec![(
                        coinbase_one_outpoint.into(),
                        Utxo {
                            value_sats: 50,
                            height: 1,
                            is_coinbase: true,
                            last_touched: 1,
                            creation_mtp: 0,
                            script_pubkey: script_a.as_bytes().to_vec(),
                        },
                    )],
                    vec![spend_outpoint],
                ),
                UtxoUndo::new(
                    vec![(
                        spend_outpoint,
                        Utxo {
                            value_sats: 50,
                            height: 2,
                            is_coinbase: false,
                            last_touched: 2,
                            creation_mtp: 1,
                            script_pubkey: script_b.as_bytes().to_vec(),
                        },
                    )],
                    vec![spend_again_outpoint],
                ),
            ],
        };
        index.connect(2, &block_two, &applied_two).unwrap();
        assert!(
            index
                .address_utxos(&address_a.to_string(), 0, 100)
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            index
                .address_utxos(&address_b.to_string(), 0, 100)
                .unwrap()
                .len(),
            2
        );
        let first_page = index.address_utxos(&address_b.to_string(), 0, 1).unwrap();
        let second_page = index.address_utxos(&address_b.to_string(), 1, 1).unwrap();
        assert_eq!(first_page.len(), 1);
        assert_eq!(second_page.len(), 1);
        assert_ne!(first_page[0], second_page[0]);
        assert!(
            index
                .address_utxos(&address_b.to_string(), 0, MAX_UTXO_PAGE_SIZE + 2)
                .is_err()
        );

        assert_eq!(index.disconnect_tip().unwrap().height, 1);
        assert_eq!(
            index
                .address_utxos(&address_a.to_string(), 0, 100)
                .unwrap()
                .len(),
            1
        );
        assert!(
            index
                .address_utxos(&address_b.to_string(), 0, 100)
                .unwrap()
                .is_empty()
        );
        assert!(index.block(2).unwrap().is_none());
        assert!(
            index
                .transaction(&spend.compute_txid().to_string())
                .unwrap()
                .is_none()
        );
    }
}
