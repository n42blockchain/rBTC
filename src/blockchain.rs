//! Atomic block-to-chainstate transition checks.

use bitcoin::Block;
use thiserror::Error;

use crate::{
    chainstate::{AppliedTransaction, ChainstateError, apply_transaction},
    utxo::{UtxoError, UtxoStore, UtxoUndo},
};

/// Bitcoin's maximum serialized block weight in weight units.
pub const MAX_BLOCK_WEIGHT: u64 = 4_000_000;
const HALVING_INTERVAL: u32 = 210_000;
const INITIAL_SUBSIDY_SATS: u64 = 50 * 100_000_000;

/// Reorg data produced by a successfully applied block.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AppliedBlock {
    /// Hash of the connected block.
    pub hash: bitcoin::BlockHash,
    /// Transaction undo data in block order; disconnect in reverse order.
    pub transaction_undos: Vec<UtxoUndo>,
}

/// Block-level validation or atomic-application error.
#[derive(Debug, Error)]
pub enum BlockError {
    /// The block has no transactions.
    #[error("block has no transactions")]
    Empty,
    /// The first transaction is not structurally a coinbase.
    #[error("first transaction is not coinbase")]
    MissingCoinbase,
    /// A transaction after index zero is structurally a coinbase.
    #[error("multiple coinbase transactions")]
    MultipleCoinbase,
    /// Header's Merkle root does not commit to the supplied transactions.
    #[error("block merkle root mismatch")]
    MerkleRoot,
    /// A SegWit witness commitment is missing or does not match the transaction data.
    #[error("block witness commitment mismatch")]
    WitnessCommitment,
    /// Coinbase does not begin with the minimally encoded BIP34 block height.
    #[error("coinbase does not encode BIP34 height {height}")]
    Bip34Height {
        /// Expected block height.
        height: u32,
    },
    /// Block weight is over the consensus limit.
    #[error("block weight {weight} exceeds limit {MAX_BLOCK_WEIGHT}")]
    Weight {
        /// Actual block weight in weight units.
        weight: u64,
    },
    /// Coinbase claims more than subsidy plus validated transaction fees.
    #[error("coinbase value {claimed} exceeds allowed value {allowed}")]
    ExcessCoinbase {
        /// Sum of coinbase outputs in satoshis.
        claimed: u64,
        /// Subsidy plus fees in satoshis.
        allowed: u64,
    },
    /// Sum of individually valid transaction fees overflowed.
    #[error("block fee sum overflow")]
    FeeOverflow,
    /// One transaction failed validation or chainstate application.
    #[error("transaction {index}: {source}")]
    Transaction {
        /// Transaction position in the block.
        index: usize,
        /// Underlying error.
        #[source]
        source: ChainstateError,
    },
    /// A failed block application could not restore previous UTXO state.
    #[error("rollback: {0}")]
    Rollback(#[source] UtxoError),
}

/// Validates and atomically applies a block's transaction effects.
///
/// Header DAG, difficulty, and timestamp validation must be completed before
/// this function is called. `script_flags` is the caller's deployment-aware
/// flag set for this candidate height.
///
/// # Errors
///
/// Returns a block structural, consensus accounting, transaction, or rollback error.
pub fn apply_block<S: UtxoStore>(
    store: &S,
    block: &Block,
    height: u32,
    now: u64,
    hot_window_secs: u64,
    script_flags: u32,
) -> Result<AppliedBlock, BlockError> {
    apply_block_with_bip34(
        store,
        block,
        height,
        now,
        hot_window_secs,
        script_flags,
        false,
    )
}

/// Validates and applies a block, optionally enforcing the BIP34 height commitment.
///
/// The chain deployment manager must set `bip34_active` only at and after the
/// selected network's activation height.
pub fn apply_block_with_bip34<S: UtxoStore>(
    store: &S,
    block: &Block,
    height: u32,
    now: u64,
    hot_window_secs: u64,
    script_flags: u32,
    bip34_active: bool,
) -> Result<AppliedBlock, BlockError> {
    if block.txdata.is_empty() {
        return Err(BlockError::Empty);
    }
    if !block.txdata[0].is_coinbase() {
        return Err(BlockError::MissingCoinbase);
    }
    if bip34_active && !coinbase_has_height(&block.txdata[0], height) {
        return Err(BlockError::Bip34Height { height });
    }
    if block.txdata[1..]
        .iter()
        .any(bitcoin::Transaction::is_coinbase)
    {
        return Err(BlockError::MultipleCoinbase);
    }
    if !block.check_merkle_root() {
        return Err(BlockError::MerkleRoot);
    }
    if !block.check_witness_commitment() {
        return Err(BlockError::WitnessCommitment);
    }
    let weight = block.weight().to_wu();
    if weight > MAX_BLOCK_WEIGHT {
        return Err(BlockError::Weight { weight });
    }

    let mut applied = Vec::with_capacity(block.txdata.len());
    for (index, transaction) in block.txdata.iter().enumerate() {
        match apply_transaction(store, transaction, height, now, script_flags) {
            Ok(transaction) => applied.push(transaction),
            Err(source) => {
                rollback(store, &applied, now, hot_window_secs)?;
                return Err(BlockError::Transaction { index, source });
            }
        }
    }
    let fees = applied[1..].iter().try_fold(0_u64, |fees, transaction| {
        fees.checked_add(transaction.input_value_sats - transaction.output_value_sats)
            .ok_or(BlockError::FeeOverflow)
    })?;
    let allowed = block_subsidy(height)
        .checked_add(fees)
        .ok_or(BlockError::FeeOverflow)?;
    if applied[0].output_value_sats > allowed {
        rollback(store, &applied, now, hot_window_secs)?;
        return Err(BlockError::ExcessCoinbase {
            claimed: applied[0].output_value_sats,
            allowed,
        });
    }
    Ok(AppliedBlock {
        hash: block.block_hash(),
        transaction_undos: applied
            .into_iter()
            .map(|transaction| transaction.undo)
            .collect(),
    })
}

fn coinbase_has_height(transaction: &bitcoin::Transaction, height: u32) -> bool {
    let encoded = encode_script_num(height);
    let script = transaction.input[0].script_sig.as_bytes();
    encoded.len() <= 75
        && script.len() > encoded.len()
        && script[0] == u8::try_from(encoded.len()).expect("BIP34 height fits direct push")
        && script[1..=encoded.len()] == encoded
}

fn encode_script_num(mut value: u32) -> Vec<u8> {
    if value == 0 {
        return Vec::new();
    }
    let mut encoded = Vec::new();
    while value > 0 {
        encoded.push((value & 0xff) as u8);
        value >>= 8;
    }
    if encoded.last().is_some_and(|byte| byte & 0x80 != 0) {
        encoded.push(0);
    }
    encoded
}

/// Disconnects a previously applied block in reverse transaction order.
pub fn disconnect_block<S: UtxoStore>(
    store: &S,
    applied: &AppliedBlock,
    now: u64,
    hot_window_secs: u64,
) -> Result<(), UtxoError> {
    for undo in applied.transaction_undos.iter().rev() {
        store.undo(undo, now, hot_window_secs)?;
    }
    Ok(())
}

/// Returns the fixed block subsidy at `height`, excluding transaction fees.
#[must_use]
pub const fn block_subsidy(height: u32) -> u64 {
    let halvings = height / HALVING_INTERVAL;
    if halvings >= 64 {
        0
    } else {
        INITIAL_SUBSIDY_SATS >> halvings
    }
}

fn rollback<S: UtxoStore>(
    store: &S,
    applied: &[AppliedTransaction],
    now: u64,
    hot_window_secs: u64,
) -> Result<(), BlockError> {
    for transaction in applied.iter().rev() {
        store
            .undo(&transaction.undo, now, hot_window_secs)
            .map_err(BlockError::Rollback)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use bitcoin::{
        Amount, Block, OutPoint, ScriptBuf, Sequence, Transaction, TxIn, TxOut, Witness,
        absolute::LockTime, blockdata::constants::genesis_block, transaction::Version,
    };
    use tempfile::TempDir;

    use super::*;
    use crate::utxo::{OutPointKey, RedbUtxoStore};

    fn store() -> (TempDir, RedbUtxoStore) {
        let dir = TempDir::new().unwrap();
        let store = RedbUtxoStore::open(dir.path().join("chainstate.redb")).unwrap();
        (dir, store)
    }

    fn coinbase() -> Transaction {
        Transaction {
            version: Version::ONE,
            lock_time: LockTime::ZERO,
            input: vec![TxIn {
                previous_output: OutPoint::null(),
                script_sig: ScriptBuf::from_bytes(vec![1, 2]),
                sequence: Sequence::MAX,
                witness: Witness::default(),
            }],
            output: vec![TxOut {
                value: Amount::from_sat(block_subsidy(0)),
                script_pubkey: ScriptBuf::new(),
            }],
        }
    }

    fn block(transactions: Vec<Transaction>) -> Block {
        let mut block = Block {
            header: genesis_block(bitcoin::Network::Regtest).header,
            txdata: transactions,
        };
        block.header.merkle_root = block.compute_merkle_root().unwrap();
        block
    }

    #[test]
    fn applies_and_disconnects_a_valid_coinbase_block() {
        let (_dir, store) = store();
        let transaction = coinbase();
        let block = block(vec![transaction.clone()]);
        let applied = apply_block(&store, &block, 0, 100, 60, 0).unwrap();
        let output = OutPointKey::from(OutPoint::new(transaction.compute_txid(), 0));
        assert!(store.get(output).unwrap().is_some());
        disconnect_block(&store, &applied, 100, 60).unwrap();
        assert!(store.get(output).unwrap().is_none());
    }

    #[test]
    fn rolls_back_coinbase_when_later_transaction_fails() {
        let (_dir, store) = store();
        let coinbase = coinbase();
        let invalid = Transaction {
            version: Version::ONE,
            lock_time: LockTime::ZERO,
            input: vec![],
            output: vec![TxOut {
                value: Amount::from_sat(1),
                script_pubkey: ScriptBuf::new(),
            }],
        };
        let block = block(vec![coinbase.clone(), invalid]);
        assert!(matches!(
            apply_block(&store, &block, 0, 100, 60, 0),
            Err(BlockError::Transaction { index: 1, .. })
        ));
        let output = OutPointKey::from(OutPoint::new(coinbase.compute_txid(), 0));
        assert!(store.get(output).unwrap().is_none());
    }

    #[test]
    fn rejects_witness_data_without_a_witness_commitment() {
        let (_dir, store) = store();
        let mut transaction = coinbase();
        transaction.input[0].witness = Witness::from_slice(&[b"reserved".as_slice()]);
        let block = block(vec![transaction]);
        assert!(matches!(
            apply_block(&store, &block, 0, 100, 60, 0),
            Err(BlockError::WitnessCommitment)
        ));
    }

    #[test]
    fn bip34_requires_minimally_encoded_coinbase_height() {
        let (_dir, store) = store();
        let mut transaction = coinbase();
        transaction.input[0].script_sig = ScriptBuf::from_bytes(vec![1, 10]);
        let valid_block = block(vec![transaction.clone()]);
        assert!(apply_block_with_bip34(&store, &valid_block, 10, 100, 60, 0, true).is_ok());
        let bad = block(vec![coinbase()]);
        assert!(matches!(
            apply_block_with_bip34(&store, &bad, 10, 100, 60, 0, true),
            Err(BlockError::Bip34Height { height: 10 })
        ));
    }
}
