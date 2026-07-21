//! Sequential active-chain block execution and durable progress coordination.

use bitcoin::{Block, BlockHash};
use thiserror::Error;

use crate::{
    blockchain::{AppliedBlock, BlockError, apply_block_with_deployments, disconnect_block},
    execution_store::{ExecutionStoreError, ExecutionTip, RedbExecutionStore},
    headers::HeaderDag,
    undo_store::{RedbUndoStore, UndoStoreError},
    utxo::{UtxoError, UtxoStore},
};

/// Consensus deployments selected for a candidate block.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct BlockDeploymentContext {
    /// Explicit libbitcoinconsensus script verification flags.
    pub script_flags: u32,
    /// Whether BIP34 coinbase-height commitment is active.
    pub bip34_active: bool,
    /// Whether the CSV deployment (BIP68/BIP112/BIP113) is active.
    pub csv_active: bool,
}

/// Failures while connecting one downloaded active-chain block.
#[derive(Debug, Error)]
pub enum BlockExecutionError {
    /// The persisted execution tip is no longer on the selected active header chain.
    #[error("execution tip {height}:{hash} is not on the active header chain")]
    TipNotActive {
        /// Persisted execution height.
        height: u32,
        /// Persisted execution hash.
        hash: BlockHash,
    },
    /// There is no active header after the persisted execution tip.
    #[error("no next active header after execution height {0}")]
    NoNextHeader(u32),
    /// The peer returned a block other than the next active-chain block.
    #[error("downloaded block {actual} does not match next active block {expected}")]
    UnexpectedBlock {
        /// Required active-chain block hash.
        expected: BlockHash,
        /// Downloaded block hash.
        actual: BlockHash,
    },
    /// Parent MTP could not be derived from the validated header DAG.
    #[error("missing median-time-past for parent {0}")]
    MissingParentMtp(BlockHash),
    /// Block consensus validation or UTXO application failed.
    #[error("block validation: {0}")]
    Block(#[from] BlockError),
    /// Durable block undo insertion failed.
    #[error("undo persistence: {0}")]
    Undo(#[from] UndoStoreError),
    /// Durable execution-tip update failed.
    #[error("execution tip persistence: {0}")]
    Execution(#[from] ExecutionStoreError),
    /// A persistence failure could not be cleanly rolled back from UTXO state.
    #[error("UTXO rollback after persistence failure: {0}")]
    Rollback(#[from] UtxoError),
}

/// Validates and connects exactly the next active-chain block.
///
/// UTXO application occurs first, then undo storage, then execution-tip
/// advancement. Failures in the latter two stages trigger an immediate UTXO
/// rollback. These stores are not yet one physical redb transaction, so
/// power-loss atomicity across the three files remains a release gate.
#[allow(clippy::too_many_arguments)]
pub fn connect_active_block<S: UtxoStore>(
    chainstate: &S,
    undo_store: &RedbUndoStore,
    execution_store: &RedbExecutionStore,
    headers: &HeaderDag,
    block: &Block,
    now: u64,
    hot_window_secs: u64,
    deployments: BlockDeploymentContext,
) -> Result<AppliedBlock, BlockExecutionError> {
    let current = execution_store.tip()?;
    let active_current = headers.active_header_at(current.height);
    if active_current.is_none_or(|header| header.hash != current.hash) {
        return Err(BlockExecutionError::TipNotActive {
            height: current.height,
            hash: current.hash,
        });
    }
    let next_height = current
        .height
        .checked_add(1)
        .ok_or(BlockExecutionError::NoNextHeader(current.height))?;
    let expected = headers
        .active_header_at(next_height)
        .ok_or(BlockExecutionError::NoNextHeader(current.height))?;
    let actual = block.block_hash();
    if actual != expected.hash {
        return Err(BlockExecutionError::UnexpectedBlock {
            expected: expected.hash,
            actual,
        });
    }
    let parent_mtp = headers
        .median_time_past(current.hash)
        .ok_or(BlockExecutionError::MissingParentMtp(current.hash))?;
    let applied = apply_block_with_deployments(
        chainstate,
        block,
        next_height,
        now,
        parent_mtp,
        hot_window_secs,
        deployments.script_flags,
        deployments.bip34_active,
        deployments.csv_active,
    )?;

    if let Err(error) = undo_store.insert(applied.hash, &applied.transaction_undos) {
        disconnect_block(chainstate, &applied, now, hot_window_secs)?;
        return Err(BlockExecutionError::Undo(error));
    }
    if let Err(error) = execution_store.advance(
        current.hash,
        ExecutionTip {
            height: next_height,
            hash: applied.hash,
        },
    ) {
        undo_store.remove(applied.hash)?;
        disconnect_block(chainstate, &applied, now, hot_window_secs)?;
        return Err(BlockExecutionError::Execution(error));
    }
    Ok(applied)
}

#[cfg(test)]
mod tests {
    use bitcoin::{
        Amount, Block, Network, OutPoint, ScriptBuf, Sequence, Transaction, TxIn, TxMerkleNode,
        TxOut, Witness,
        absolute::LockTime,
        block::{Header, Version as HeaderVersion},
        hashes::Hash,
        pow::Target,
        transaction::Version,
    };
    use tempfile::TempDir;

    use super::*;
    use crate::{
        blockchain::block_subsidy,
        headers::HeaderDag,
        utxo::{OutPointKey, RedbUtxoStore},
    };

    fn coinbase(height: u32) -> Transaction {
        Transaction {
            version: Version::ONE,
            lock_time: LockTime::ZERO,
            input: vec![TxIn {
                previous_output: OutPoint::null(),
                script_sig: ScriptBuf::from_bytes(vec![1, u8::try_from(height).unwrap()]),
                sequence: Sequence::MAX,
                witness: Witness::default(),
            }],
            output: vec![TxOut {
                value: Amount::from_sat(block_subsidy(height)),
                script_pubkey: ScriptBuf::new(),
            }],
        }
    }

    fn block(parent: BlockHash, time: u32) -> Block {
        let transaction = coinbase(1);
        let mut block = Block {
            header: Header {
                version: HeaderVersion::ONE,
                prev_blockhash: parent,
                merkle_root: TxMerkleNode::all_zeros(),
                time,
                bits: Target::MAX_ATTAINABLE_REGTEST.to_compact_lossy(),
                nonce: 0,
            },
            txdata: vec![transaction],
        };
        block.header.merkle_root = block.compute_merkle_root().unwrap();
        while block
            .header
            .validate_pow(Target::MAX_ATTAINABLE_REGTEST)
            .is_err()
        {
            block.header.nonce = block.header.nonce.checked_add(1).unwrap();
        }
        block
    }

    #[test]
    fn connects_active_block_and_recovers_execution_tip() {
        let directory = TempDir::new().unwrap();
        let chainstate = RedbUtxoStore::open(directory.path().join("chainstate.redb")).unwrap();
        let undo_store = RedbUndoStore::open(directory.path().join("undo.redb")).unwrap();
        let execution_store =
            RedbExecutionStore::open(directory.path().join("execution.redb"), Network::Regtest)
                .unwrap();
        let mut headers = HeaderDag::new(Network::Regtest);
        let genesis = headers.active_tip();
        let block = block(genesis.hash, genesis.header.time + 1);
        let info = headers
            .insert_contextual(block.header, block.header.time)
            .unwrap();

        let applied = connect_active_block(
            &chainstate,
            &undo_store,
            &execution_store,
            &headers,
            &block,
            1,
            60,
            BlockDeploymentContext::default(),
        )
        .unwrap();
        assert_eq!(execution_store.tip().unwrap().hash, info.hash);
        assert!(undo_store.get(applied.hash).unwrap().is_some());
        let coinbase_outpoint = OutPointKey::from(OutPoint::new(block.txdata[0].compute_txid(), 0));
        assert!(
            crate::utxo::UtxoStore::get(&chainstate, coinbase_outpoint)
                .unwrap()
                .is_some()
        );
    }
}
