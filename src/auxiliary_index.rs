//! Independently rebuildable transaction, spent-output, and BIP158 indexes.

use std::{
    collections::{HashMap, HashSet},
    fs,
    path::Path,
    sync::Mutex,
};

use bitcoin::{
    Block, BlockHash, Network, OutPoint, ScriptBuf, Txid,
    bip158::{BlockFilter, FilterHash, FilterHeader},
    hashes::Hash,
};
use redb::{Database, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    execution_store::ExecutionTip,
    utxo::{OutPointKey, UtxoUndo},
};

const META: TableDefinition<&str, &[u8]> = TableDefinition::new("aux_index_metadata");
const RECORDS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("aux_index_records");
const UNDOS: TableDefinition<u32, &[u8]> = TableDefinition::new("aux_index_undos");
const GENESIS_KEY: &str = "genesis";
const KIND_KEY: &str = "kind";
const TIP_KEY: &str = "tip";
const SCHEMA_KEY: &str = "schema";
const LEGACY_SCHEMA_VERSION: u32 = 1;
const BASIC_FILTER_SCHEMA_VERSION: u32 = 2;

/// One independently persisted optional index family.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AuxiliaryIndexKind {
    /// Transaction ID to active block location.
    Transaction,
    /// Spent outpoint to active-chain spender location.
    SpentOutput,
    /// BIP158 basic filter plus BIP157 filter-header chain.
    BasicFilter,
}

impl AuxiliaryIndexKind {
    const fn tag(self) -> u8 {
        match self {
            Self::Transaction => 1,
            Self::SpentOutput => 2,
            Self::BasicFilter => 3,
        }
    }

    const fn schema_version(self) -> u32 {
        match self {
            Self::Transaction | Self::SpentOutput => LEGACY_SCHEMA_VERSION,
            Self::BasicFilter => BASIC_FILTER_SCHEMA_VERSION,
        }
    }
}

/// Active-chain location of one transaction.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TransactionLocation {
    /// Containing block height.
    pub height: u32,
    /// Containing block hash.
    pub block_hash: BlockHash,
    /// Zero-based transaction position.
    pub position: u32,
}

/// Active-chain location that spent one outpoint.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SpentOutputLocation {
    /// Spending transaction ID.
    pub spending_txid: Txid,
    /// Zero-based input position.
    pub input_index: u32,
    /// Containing block height.
    pub height: u32,
    /// Containing block hash.
    pub block_hash: BlockHash,
}

/// Persisted BIP158 basic filter and its chained BIP157 header.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BasicFilterRecord {
    /// Golomb-coded set bytes.
    pub filter: Vec<u8>,
    /// Double-SHA256 of `filter`.
    pub filter_hash: FilterHash,
    /// Header chaining this filter hash to the preceding filter header.
    pub filter_header: FilterHeader,
}

/// Optional-index storage failure.
#[derive(Debug, Error)]
pub enum AuxiliaryIndexError {
    /// Filesystem path or permission failure.
    #[error("optional index filesystem: {0}")]
    Io(#[from] std::io::Error),
    /// Database open/create failure.
    #[error("optional index database: {0}")]
    Database(#[from] redb::DatabaseError),
    /// Database transaction failure.
    #[error("optional index transaction: {0}")]
    Transaction(#[from] redb::TransactionError),
    /// Database table failure.
    #[error("optional index table: {0}")]
    Table(#[from] redb::TableError),
    /// Database read/write failure.
    #[error("optional index storage: {0}")]
    Storage(#[from] redb::StorageError),
    /// Database commit failure.
    #[error("optional index commit: {0}")]
    Commit(#[from] redb::CommitError),
    /// Undo encoding failure.
    #[error("optional index encoding: {0}")]
    Encoding(#[from] serde_json::Error),
    /// BIP158 filter construction failure.
    #[error("basic filter: {0}")]
    Filter(#[from] bitcoin::bip158::Error),
    /// Store belongs to another network.
    #[error("optional index belongs to another Bitcoin network")]
    NetworkMismatch,
    /// Store belongs to another index family.
    #[error("optional index kind does not match its data file")]
    KindMismatch,
    /// Persisted data or transition is inconsistent.
    #[error("invalid optional index transition: {0}")]
    Invalid(&'static str),
}

#[derive(Debug, Deserialize, Serialize)]
struct IndexUndo {
    parent_height: u32,
    parent_hash: [u8; 32],
    previous: Vec<(Vec<u8>, Option<Vec<u8>>)>,
}

/// One network-bound optional index stored in its own redb file.
pub struct RedbAuxiliaryIndex {
    db: Database,
    kind: AuxiliaryIndexKind,
    write_guard: Mutex<()>,
}

impl RedbAuxiliaryIndex {
    /// Opens or creates one independently rebuildable index.
    pub fn open(
        path: impl AsRef<Path>,
        network: Network,
        kind: AuxiliaryIndexKind,
    ) -> Result<Self, AuxiliaryIndexError> {
        let path = path.as_ref();
        validate_database_path(path)?;
        let genesis_block = bitcoin::blockdata::constants::genesis_block(network);
        let genesis = genesis_block.block_hash();
        let schema_version = kind.schema_version();
        let db = Database::create(path)?;
        restrict_database_permissions(path)?;
        let transaction = db.begin_write()?;
        {
            let mut meta = transaction.open_table(META)?;
            if let Some(stored) = meta.get(GENESIS_KEY)? {
                if stored.value() != genesis.to_byte_array() {
                    return Err(AuxiliaryIndexError::NetworkMismatch);
                }
                if meta
                    .get(KIND_KEY)?
                    .is_none_or(|value| value.value() != [kind.tag()])
                {
                    return Err(AuxiliaryIndexError::KindMismatch);
                }
                if meta
                    .get(SCHEMA_KEY)?
                    .is_none_or(|value| value.value() != schema_version.to_le_bytes())
                {
                    return Err(AuxiliaryIndexError::Invalid("unsupported schema"));
                }
                if meta.get(TIP_KEY)?.is_none() {
                    return Err(AuxiliaryIndexError::Invalid("missing index tip"));
                }
            } else {
                meta.insert(GENESIS_KEY, genesis.to_byte_array().as_slice())?;
                meta.insert(KIND_KEY, [kind.tag()].as_slice())?;
                meta.insert(SCHEMA_KEY, schema_version.to_le_bytes().as_slice())?;
                meta.insert(
                    TIP_KEY,
                    encode_tip(ExecutionTip {
                        height: 0,
                        hash: genesis,
                    })
                    .as_slice(),
                )?;
            }
            let mut records = transaction.open_table(RECORDS)?;
            if kind == AuxiliaryIndexKind::BasicFilter
                && records.get(0_u32.to_be_bytes().as_slice())?.is_none()
            {
                let filter = BlockFilter::new_script_filter(&genesis_block, |outpoint| {
                    Err::<ScriptBuf, _>(bitcoin::bip158::Error::UtxoMissing(*outpoint))
                })?;
                let filter_hash = FilterHash::hash(&filter.content);
                let record = BasicFilterRecord {
                    filter: filter.content,
                    filter_hash,
                    filter_header: filter_hash.filter_header(&FilterHeader::all_zeros()),
                };
                records.insert(
                    0_u32.to_be_bytes().as_slice(),
                    encode_filter_record(&record).as_slice(),
                )?;
                let mut hash_key = Vec::with_capacity(33);
                hash_key.push(b'h');
                hash_key.extend_from_slice(&genesis.to_byte_array());
                records.insert(hash_key.as_slice(), 0_u32.to_be_bytes().as_slice())?;
            }
            transaction.open_table(UNDOS)?;
        }
        transaction.commit()?;
        Ok(Self {
            db,
            kind,
            write_guard: Mutex::new(()),
        })
    }

    /// Returns this store's index family.
    #[must_use]
    pub const fn kind(&self) -> AuxiliaryIndexKind {
        self.kind
    }

    /// Returns the highest active block atomically represented by the index.
    pub fn tip(&self) -> Result<ExecutionTip, AuxiliaryIndexError> {
        let transaction = self.db.begin_read()?;
        let meta = transaction.open_table(META)?;
        decode_tip(
            meta.get(TIP_KEY)?
                .ok_or(AuxiliaryIndexError::Invalid("missing index tip"))?
                .value(),
        )
    }

    /// Atomically indexes one already validated and executed active block.
    pub fn connect_block(
        &self,
        height: u32,
        block: &Block,
        transaction_undos: &[UtxoUndo],
    ) -> Result<(), AuxiliaryIndexError> {
        self.connect_batch(&[(height, block, transaction_undos)])
    }

    /// Atomically indexes a consecutive block batch in one sorted write transaction.
    pub fn connect_batch(
        &self,
        blocks: &[(u32, &Block, &[UtxoUndo])],
    ) -> Result<(), AuxiliaryIndexError> {
        if blocks.is_empty() {
            return Ok(());
        }
        let _guard = self
            .write_guard
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let transaction = self.db.begin_write()?;
        let mut tip;
        {
            let meta = transaction.open_table(META)?;
            tip = decode_tip(
                meta.get(TIP_KEY)?
                    .ok_or(AuxiliaryIndexError::Invalid("missing index tip"))?
                    .value(),
            )?;
        }
        {
            let mut records = transaction.open_table(RECORDS)?;
            let mut undos = transaction.open_table(UNDOS)?;
            for (height, block, transaction_undos) in blocks {
                if tip.height.checked_add(1) != Some(*height)
                    || block.header.prev_blockhash != tip.hash
                {
                    return Err(AuxiliaryIndexError::Invalid(
                        "block does not extend optional index tip",
                    ));
                }
                let mut previous =
                    build_records(self.kind, *height, block, transaction_undos, &records)?;
                // redb's copy-on-write pages are materially cheaper when one
                // block's independent keys arrive in database order.
                previous.sort_unstable_by(|left, right| left.0.cmp(&right.0));
                for (key, value, _) in &previous {
                    records.insert(key.as_slice(), value.as_slice())?;
                }
                let undo = IndexUndo {
                    parent_height: tip.height,
                    parent_hash: tip.hash.to_byte_array(),
                    previous: previous
                        .into_iter()
                        .map(|(key, _, old)| (key, old))
                        .collect(),
                };
                undos.insert(*height, serde_json::to_vec(&undo)?.as_slice())?;
                tip = ExecutionTip {
                    height: *height,
                    hash: block.block_hash(),
                };
            }
        }
        {
            let mut meta = transaction.open_table(META)?;
            meta.insert(TIP_KEY, encode_tip(tip).as_slice())?;
        }
        transaction.commit()?;
        Ok(())
    }

    /// Atomically removes the current tip and restores overwritten BIP30 rows.
    pub fn disconnect_tip(&self) -> Result<ExecutionTip, AuxiliaryIndexError> {
        let _guard = self
            .write_guard
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let transaction = self.db.begin_write()?;
        let parent;
        {
            let mut meta = transaction.open_table(META)?;
            let tip = decode_tip(
                meta.get(TIP_KEY)?
                    .ok_or(AuxiliaryIndexError::Invalid("missing index tip"))?
                    .value(),
            )?;
            if tip.height == 0 {
                return Err(AuxiliaryIndexError::Invalid("cannot disconnect genesis"));
            }
            let mut undos = transaction.open_table(UNDOS)?;
            let undo: IndexUndo = serde_json::from_slice(
                undos
                    .get(tip.height)?
                    .ok_or(AuxiliaryIndexError::Invalid("missing index undo"))?
                    .value(),
            )?;
            let mut records = transaction.open_table(RECORDS)?;
            for (key, previous) in undo.previous.iter().rev() {
                if let Some(previous) = previous {
                    records.insert(key.as_slice(), previous.as_slice())?;
                } else {
                    records.remove(key.as_slice())?;
                }
            }
            undos.remove(tip.height)?;
            parent = ExecutionTip {
                height: undo.parent_height,
                hash: BlockHash::from_byte_array(undo.parent_hash),
            };
            meta.insert(TIP_KEY, encode_tip(parent).as_slice())?;
        }
        transaction.commit()?;
        Ok(parent)
    }

    /// Drops rollback rows older than the first locally disconnectable height.
    pub fn prune_undos_before(&self, retain_from_height: u32) -> Result<u64, AuxiliaryIndexError> {
        let _guard = self
            .write_guard
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let transaction = self.db.begin_write()?;
        let removed;
        {
            let mut undos = transaction.open_table(UNDOS)?;
            let mut count = 0_u64;
            for entry in undos.iter()? {
                let (height, _) = entry?;
                if height.value() < retain_from_height {
                    count = count.saturating_add(1);
                }
            }
            removed = count;
            undos.retain(|height, _| height >= retain_from_height)?;
        }
        transaction.commit()?;
        Ok(removed)
    }

    /// Looks up a transaction in a transaction index.
    pub fn transaction(
        &self,
        txid: Txid,
    ) -> Result<Option<TransactionLocation>, AuxiliaryIndexError> {
        self.require_kind(AuxiliaryIndexKind::Transaction)?;
        self.record(&txid.to_byte_array())?
            .map(|bytes| decode_transaction_location(&bytes))
            .transpose()
    }

    /// Looks up the active-chain spender of an outpoint.
    pub fn spender(
        &self,
        outpoint: OutPoint,
    ) -> Result<Option<SpentOutputLocation>, AuxiliaryIndexError> {
        self.require_kind(AuxiliaryIndexKind::SpentOutput)?;
        self.record(OutPointKey::from(outpoint).as_bytes())?
            .map(|bytes| decode_spent_location(&bytes))
            .transpose()
    }

    /// Returns one basic compact filter record by block height.
    pub fn basic_filter(
        &self,
        height: u32,
    ) -> Result<Option<BasicFilterRecord>, AuxiliaryIndexError> {
        self.require_kind(AuxiliaryIndexKind::BasicFilter)?;
        self.record(&height.to_be_bytes())?
            .map(|bytes| decode_filter_record(&bytes))
            .transpose()
    }

    /// Returns one basic compact filter record by active block hash.
    pub fn basic_filter_by_hash(
        &self,
        block_hash: BlockHash,
    ) -> Result<Option<BasicFilterRecord>, AuxiliaryIndexError> {
        self.require_kind(AuxiliaryIndexKind::BasicFilter)?;
        let mut key = Vec::with_capacity(33);
        key.push(b'h');
        key.extend_from_slice(&block_hash.to_byte_array());
        let Some(height) = self.record(&key)? else {
            return Ok(None);
        };
        if height.len() != 4 {
            return Err(AuxiliaryIndexError::Invalid("basic filter hash lookup"));
        }
        self.basic_filter(u32::from_be_bytes(
            height.try_into().expect("checked filter height"),
        ))
    }

    fn record(&self, key: &[u8]) -> Result<Option<Vec<u8>>, AuxiliaryIndexError> {
        let transaction = self.db.begin_read()?;
        let records = transaction.open_table(RECORDS)?;
        Ok(records.get(key)?.map(|value| value.value().to_vec()))
    }

    fn require_kind(&self, expected: AuxiliaryIndexKind) -> Result<(), AuxiliaryIndexError> {
        if self.kind == expected {
            Ok(())
        } else {
            Err(AuxiliaryIndexError::KindMismatch)
        }
    }
}

fn validate_database_path(path: &Path) -> Result<(), AuxiliaryIndexError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => {
            if metadata.file_type().is_symlink() || !metadata.is_file() {
                return Err(AuxiliaryIndexError::Invalid(
                    "database path must be a regular non-symlink file",
                ));
            }
            #[cfg(unix)]
            if std::os::unix::fs::MetadataExt::nlink(&metadata) != 1 {
                return Err(AuxiliaryIndexError::Invalid(
                    "database must have exactly one hard link",
                ));
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    Ok(())
}

fn restrict_database_permissions(path: &Path) -> Result<(), AuxiliaryIndexError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    }
    #[cfg(not(unix))]
    let _ = path;
    Ok(())
}

type PendingRecord = (Vec<u8>, Vec<u8>, Option<Vec<u8>>);

fn build_records(
    kind: AuxiliaryIndexKind,
    height: u32,
    block: &Block,
    transaction_undos: &[UtxoUndo],
    records: &impl ReadableTable<&'static [u8], &'static [u8]>,
) -> Result<Vec<PendingRecord>, AuxiliaryIndexError> {
    match kind {
        AuxiliaryIndexKind::Transaction => block
            .txdata
            .iter()
            .enumerate()
            .map(|(position, transaction)| {
                let key = transaction.compute_txid().to_byte_array().to_vec();
                let value = encode_transaction_location(TransactionLocation {
                    height,
                    block_hash: block.block_hash(),
                    position: u32::try_from(position)
                        .map_err(|_| AuxiliaryIndexError::Invalid("transaction position"))?,
                });
                let old = records
                    .get(key.as_slice())?
                    .map(|value| value.value().to_vec());
                Ok((key, value, old))
            })
            .collect(),
        AuxiliaryIndexKind::SpentOutput => {
            let mut pending = Vec::new();
            let mut seen = HashSet::new();
            for transaction in block.txdata.iter().skip(1) {
                let spending_txid = transaction.compute_txid();
                for (input_index, input) in transaction.input.iter().enumerate() {
                    let key = OutPointKey::from(input.previous_output).as_bytes().to_vec();
                    if !seen.insert(key.clone()) {
                        return Err(AuxiliaryIndexError::Invalid(
                            "outpoint has multiple spenders in one block",
                        ));
                    }
                    let value = encode_spent_location(SpentOutputLocation {
                        spending_txid,
                        input_index: u32::try_from(input_index)
                            .map_err(|_| AuxiliaryIndexError::Invalid("input position"))?,
                        height,
                        block_hash: block.block_hash(),
                    });
                    let old = records
                        .get(key.as_slice())?
                        .map(|value| value.value().to_vec());
                    if old.is_some() {
                        return Err(AuxiliaryIndexError::Invalid(
                            "outpoint already has an active spender",
                        ));
                    }
                    pending.push((key, value, None));
                }
            }
            Ok(pending)
        }
        AuxiliaryIndexKind::BasicFilter => {
            let spent = transaction_undos
                .iter()
                .flat_map(UtxoUndo::spent)
                .map(|(outpoint, utxo)| {
                    (
                        outpoint.to_outpoint(),
                        ScriptBuf::from_bytes(utxo.script_pubkey.clone()),
                    )
                })
                .collect::<HashMap<_, _>>();
            let filter = BlockFilter::new_script_filter(block, |outpoint| {
                spent
                    .get(outpoint)
                    .cloned()
                    .ok_or(bitcoin::bip158::Error::UtxoMissing(*outpoint))
            })?;
            let previous = records
                .get((height - 1).to_be_bytes().as_slice())?
                .ok_or(AuxiliaryIndexError::Invalid("missing previous filter"))?;
            let previous_header = decode_filter_record(previous.value())?.filter_header;
            let filter_hash = FilterHash::hash(&filter.content);
            let record = BasicFilterRecord {
                filter_header: filter_hash.filter_header(&previous_header),
                filter_hash,
                filter: filter.content,
            };
            let key = height.to_be_bytes().to_vec();
            let old = records
                .get(key.as_slice())?
                .map(|value| value.value().to_vec());
            let mut hash_key = Vec::with_capacity(33);
            hash_key.push(b'h');
            hash_key.extend_from_slice(&block.block_hash().to_byte_array());
            let hash_old = records
                .get(hash_key.as_slice())?
                .map(|value| value.value().to_vec());
            Ok(vec![
                (key, encode_filter_record(&record), old),
                (hash_key, height.to_be_bytes().to_vec(), hash_old),
            ])
        }
    }
}

fn encode_tip(tip: ExecutionTip) -> [u8; 36] {
    let mut bytes = [0_u8; 36];
    bytes[..4].copy_from_slice(&tip.height.to_le_bytes());
    bytes[4..].copy_from_slice(&tip.hash.to_byte_array());
    bytes
}

fn decode_tip(bytes: &[u8]) -> Result<ExecutionTip, AuxiliaryIndexError> {
    if bytes.len() != 36 {
        return Err(AuxiliaryIndexError::Invalid("tip encoding"));
    }
    Ok(ExecutionTip {
        height: u32::from_le_bytes(bytes[..4].try_into().expect("checked length")),
        hash: BlockHash::from_byte_array(bytes[4..].try_into().expect("checked length")),
    })
}

fn encode_transaction_location(location: TransactionLocation) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(40);
    bytes.extend_from_slice(&location.height.to_le_bytes());
    bytes.extend_from_slice(&location.block_hash.to_byte_array());
    bytes.extend_from_slice(&location.position.to_le_bytes());
    bytes
}

fn decode_transaction_location(bytes: &[u8]) -> Result<TransactionLocation, AuxiliaryIndexError> {
    if bytes.len() != 40 {
        return Err(AuxiliaryIndexError::Invalid("transaction location"));
    }
    Ok(TransactionLocation {
        height: u32::from_le_bytes(bytes[..4].try_into().expect("checked length")),
        block_hash: BlockHash::from_byte_array(bytes[4..36].try_into().expect("checked length")),
        position: u32::from_le_bytes(bytes[36..].try_into().expect("checked length")),
    })
}

fn encode_spent_location(location: SpentOutputLocation) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(72);
    bytes.extend_from_slice(&location.spending_txid.to_byte_array());
    bytes.extend_from_slice(&location.input_index.to_le_bytes());
    bytes.extend_from_slice(&location.height.to_le_bytes());
    bytes.extend_from_slice(&location.block_hash.to_byte_array());
    bytes
}

fn decode_spent_location(bytes: &[u8]) -> Result<SpentOutputLocation, AuxiliaryIndexError> {
    if bytes.len() != 72 {
        return Err(AuxiliaryIndexError::Invalid("spent-output location"));
    }
    Ok(SpentOutputLocation {
        spending_txid: Txid::from_byte_array(bytes[..32].try_into().expect("checked length")),
        input_index: u32::from_le_bytes(bytes[32..36].try_into().expect("checked length")),
        height: u32::from_le_bytes(bytes[36..40].try_into().expect("checked length")),
        block_hash: BlockHash::from_byte_array(bytes[40..].try_into().expect("checked length")),
    })
}

fn encode_filter_record(record: &BasicFilterRecord) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(68 + record.filter.len());
    bytes.extend_from_slice(
        &u32::try_from(record.filter.len())
            .expect("consensus block filter length fits u32")
            .to_le_bytes(),
    );
    bytes.extend_from_slice(&record.filter);
    bytes.extend_from_slice(record.filter_hash.as_byte_array());
    bytes.extend_from_slice(record.filter_header.as_byte_array());
    bytes
}

fn decode_filter_record(bytes: &[u8]) -> Result<BasicFilterRecord, AuxiliaryIndexError> {
    if bytes.len() < 68 {
        return Err(AuxiliaryIndexError::Invalid("basic filter record"));
    }
    let length = usize::try_from(u32::from_le_bytes(
        bytes[..4].try_into().expect("checked length"),
    ))
    .expect("u32 fits usize");
    if bytes.len() != 68 + length {
        return Err(AuxiliaryIndexError::Invalid("basic filter length"));
    }
    Ok(BasicFilterRecord {
        filter: bytes[4..4 + length].to_vec(),
        filter_hash: FilterHash::from_byte_array(
            bytes[4 + length..36 + length]
                .try_into()
                .expect("checked length"),
        ),
        filter_header: FilterHeader::from_byte_array(
            bytes[36 + length..].try_into().expect("checked length"),
        ),
    })
}

#[cfg(test)]
mod tests {
    use bitcoin::{
        Amount, ScriptBuf, Sequence, Transaction, TxIn, TxMerkleNode, TxOut, Witness,
        absolute::LockTime,
        block::{Header, Version as HeaderVersion},
        pow::Target,
        transaction::Version,
    };
    use tempfile::TempDir;

    use super::*;
    use crate::utxo::Utxo;

    fn transaction(previous: Option<OutPoint>, tag: u8) -> Transaction {
        Transaction {
            version: Version::ONE,
            lock_time: LockTime::ZERO,
            input: vec![TxIn {
                previous_output: previous.unwrap_or_else(OutPoint::null),
                script_sig: previous
                    .map_or_else(|| ScriptBuf::from_bytes(vec![1, tag]), |_| ScriptBuf::new()),
                sequence: Sequence::MAX,
                witness: Witness::new(),
            }],
            output: vec![TxOut {
                value: Amount::from_sat(50),
                script_pubkey: ScriptBuf::from_bytes(vec![0x51, tag]),
            }],
        }
    }

    fn block(parent: BlockHash, time: u32, transactions: Vec<Transaction>) -> Block {
        let mut block = Block {
            header: Header {
                version: HeaderVersion::ONE,
                prev_blockhash: parent,
                merkle_root: TxMerkleNode::all_zeros(),
                time,
                bits: Target::MAX_ATTAINABLE_REGTEST.to_compact_lossy(),
                nonce: time,
            },
            txdata: transactions,
        };
        block.header.merkle_root = block.compute_merkle_root().unwrap();
        block
    }

    fn spent_undo(outpoint: OutPoint, script: &ScriptBuf) -> UtxoUndo {
        UtxoUndo::from_parts(
            vec![(
                outpoint.into(),
                Utxo {
                    value_sats: 50,
                    height: 1,
                    is_coinbase: true,
                    last_touched: 1,
                    creation_mtp: 0,
                    script_pubkey: script.as_bytes().to_vec(),
                },
            )],
            Vec::new(),
        )
    }

    #[test]
    fn transaction_index_restores_bip30_overwrite_after_reopen() {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("tx.redb");
        let index =
            RedbAuxiliaryIndex::open(&path, Network::Regtest, AuxiliaryIndexKind::Transaction)
                .unwrap();
        let genesis = index.tip().unwrap();
        let repeated = transaction(None, 1);
        let txid = repeated.compute_txid();
        let first = block(genesis.hash, 1, vec![repeated.clone()]);
        index.connect_block(1, &first, &[]).unwrap();
        let second = block(first.block_hash(), 2, vec![repeated]);
        index.connect_block(2, &second, &[]).unwrap();
        assert_eq!(
            index.transaction(txid).unwrap().unwrap(),
            TransactionLocation {
                height: 2,
                block_hash: second.block_hash(),
                position: 0,
            }
        );
        drop(index);

        let reopened =
            RedbAuxiliaryIndex::open(path, Network::Regtest, AuxiliaryIndexKind::Transaction)
                .unwrap();
        assert_eq!(reopened.disconnect_tip().unwrap().height, 1);
        assert_eq!(
            reopened.transaction(txid).unwrap().unwrap().block_hash,
            first.block_hash()
        );
    }

    #[test]
    fn spent_output_index_locates_and_reverses_spender() {
        let directory = TempDir::new().unwrap();
        let index = RedbAuxiliaryIndex::open(
            directory.path().join("spent.redb"),
            Network::Regtest,
            AuxiliaryIndexKind::SpentOutput,
        )
        .unwrap();
        let genesis = index.tip().unwrap();
        let coinbase = transaction(None, 2);
        let created = OutPoint::new(coinbase.compute_txid(), 0);
        let first = block(genesis.hash, 1, vec![coinbase]);
        index.connect_block(1, &first, &[]).unwrap();
        let second_coinbase = transaction(None, 3);
        let spend = transaction(Some(created), 4);
        let spend_txid = spend.compute_txid();
        let second = block(first.block_hash(), 2, vec![second_coinbase, spend]);
        index.connect_block(2, &second, &[]).unwrap();
        assert_eq!(
            index.spender(created).unwrap().unwrap(),
            SpentOutputLocation {
                spending_txid: spend_txid,
                input_index: 0,
                height: 2,
                block_hash: second.block_hash(),
            }
        );
        index.disconnect_tip().unwrap();
        assert!(index.spender(created).unwrap().is_none());
    }

    #[test]
    fn basic_filter_persists_bip157_header_chain_and_requires_spent_scripts() {
        let directory = TempDir::new().unwrap();
        let index = RedbAuxiliaryIndex::open(
            directory.path().join("filters.redb"),
            Network::Regtest,
            AuxiliaryIndexKind::BasicFilter,
        )
        .unwrap();
        let genesis = index.tip().unwrap();
        let coinbase = transaction(None, 5);
        let spent_script = coinbase.output[0].script_pubkey.clone();
        let created = OutPoint::new(coinbase.compute_txid(), 0);
        let first = block(genesis.hash, 1, vec![coinbase]);
        index.connect_block(1, &first, &[]).unwrap();
        let genesis_filter = index.basic_filter(0).unwrap().unwrap();
        let first_filter = index.basic_filter(1).unwrap().unwrap();
        assert_eq!(
            first_filter.filter_header,
            first_filter
                .filter_hash
                .filter_header(&genesis_filter.filter_header)
        );

        let second = block(
            first.block_hash(),
            2,
            vec![transaction(None, 6), transaction(Some(created), 7)],
        );
        let undo = spent_undo(created, &spent_script);
        index.connect_block(2, &second, &[undo]).unwrap();
        let second_filter = index.basic_filter(2).unwrap().unwrap();
        assert_eq!(
            second_filter.filter_header,
            second_filter
                .filter_hash
                .filter_header(&first_filter.filter_header)
        );
        assert!(!second_filter.filter.is_empty());
        index.disconnect_tip().unwrap();
        assert!(index.basic_filter(2).unwrap().is_none());
    }

    #[test]
    fn one_file_is_bound_to_network_and_index_family() {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("index.redb");
        RedbAuxiliaryIndex::open(&path, Network::Regtest, AuxiliaryIndexKind::Transaction).unwrap();
        assert!(matches!(
            RedbAuxiliaryIndex::open(&path, Network::Bitcoin, AuxiliaryIndexKind::Transaction),
            Err(AuxiliaryIndexError::NetworkMismatch)
        ));
        assert!(matches!(
            RedbAuxiliaryIndex::open(&path, Network::Regtest, AuxiliaryIndexKind::BasicFilter),
            Err(AuxiliaryIndexError::KindMismatch)
        ));
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                fs::metadata(&path).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
    }

    #[test]
    fn batch_failure_is_atomic_and_rollback_retention_is_bounded() {
        let directory = TempDir::new().unwrap();
        let index = RedbAuxiliaryIndex::open(
            directory.path().join("tx.redb"),
            Network::Regtest,
            AuxiliaryIndexKind::Transaction,
        )
        .unwrap();
        let genesis = index.tip().unwrap();
        let first_transaction = transaction(None, 8);
        let first_txid = first_transaction.compute_txid();
        let first = block(genesis.hash, 1, vec![first_transaction]);
        let disconnected = block(genesis.hash, 2, vec![transaction(None, 9)]);
        assert!(
            index
                .connect_batch(&[(1, &first, &[]), (2, &disconnected, &[])])
                .is_err()
        );
        assert_eq!(index.tip().unwrap(), genesis);
        assert!(index.transaction(first_txid).unwrap().is_none());

        let second = block(first.block_hash(), 2, vec![transaction(None, 10)]);
        index
            .connect_batch(&[(1, &first, &[]), (2, &second, &[])])
            .unwrap();
        assert_eq!(index.prune_undos_before(2).unwrap(), 1);
        assert_eq!(index.disconnect_tip().unwrap().height, 1);
        assert!(matches!(
            index.disconnect_tip(),
            Err(AuxiliaryIndexError::Invalid("missing index undo"))
        ));
    }

    #[cfg(unix)]
    #[test]
    fn database_path_rejects_symlink_and_hardlink_aliases() {
        use std::{fs::hard_link, os::unix::fs::symlink};

        let directory = TempDir::new().unwrap();
        let target = directory.path().join("target.redb");
        RedbAuxiliaryIndex::open(&target, Network::Regtest, AuxiliaryIndexKind::Transaction)
            .unwrap();
        let link = directory.path().join("link.redb");
        symlink(&target, &link).unwrap();
        assert!(
            RedbAuxiliaryIndex::open(link, Network::Regtest, AuxiliaryIndexKind::Transaction)
                .is_err()
        );
        let alias = directory.path().join("alias.redb");
        hard_link(&target, &alias).unwrap();
        assert!(
            RedbAuxiliaryIndex::open(alias, Network::Regtest, AuxiliaryIndexKind::Transaction)
                .is_err()
        );
    }
}
