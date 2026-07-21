//! Sequential active-chain block execution and durable progress coordination.

use std::collections::BTreeSet;

use bitcoin::{Block, BlockHash, OutPoint};
use thiserror::Error;

use crate::{
    blockchain::{AppliedBlock, BlockError, apply_block_with_deployments, disconnect_block},
    execution_store::{ExecutionStoreError, ExecutionTip, RedbExecutionStore},
    headers::HeaderDag,
    undo_store::{RedbUndoStore, UndoStoreError},
    utxo::{OutPointKey, UtxoError, UtxoStore},
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
    /// Whether this is one of the two historical mainnet BIP30 exceptions.
    pub bip30_exception: bool,
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
    /// The validated header journal lacks the executed tip or its parent.
    #[error("missing executed header {0} during disconnect")]
    MissingExecutedHeader(BlockHash),
    /// The genesis execution tip cannot be disconnected.
    #[error("cannot disconnect genesis execution tip")]
    DisconnectGenesis,
    /// Durable undo data is missing for an executed block.
    #[error("missing durable undo for executed block {0}")]
    MissingUndo(BlockHash),
    /// BIP30 forbids overwriting an existing unspent transaction output.
    #[error("BIP30 duplicate unspent output {0}")]
    Bip30Collision(OutPointKey),
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
    let collisions = block_output_collisions(chainstate, block)?;
    let exception_undo = if collisions.is_empty() {
        None
    } else if deployments.bip30_exception {
        Some(chainstate.apply_with_undo(&collisions, &[])?)
    } else {
        return Err(BlockExecutionError::Bip30Collision(collisions[0]));
    };
    let mut applied = match apply_block_with_deployments(
        chainstate,
        block,
        next_height,
        now,
        parent_mtp,
        hot_window_secs,
        deployments.script_flags,
        deployments.bip34_active,
        deployments.csv_active,
    ) {
        Ok(applied) => applied,
        Err(error) => {
            if let Some(undo) = &exception_undo {
                chainstate.undo(undo, now, hot_window_secs)?;
            }
            return Err(BlockExecutionError::Block(error));
        }
    };
    if let Some(undo) = exception_undo {
        applied.transaction_undos.insert(0, undo);
    }

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

fn block_output_collisions<S: UtxoStore>(
    chainstate: &S,
    block: &Block,
) -> Result<Vec<OutPointKey>, UtxoError> {
    let mut collisions = BTreeSet::new();
    for transaction in &block.txdata {
        let txid = transaction.compute_txid();
        for vout in 0..transaction.output.len() {
            let vout = u32::try_from(vout).map_err(|_| UtxoError::Malformed("output index"))?;
            let outpoint = OutPointKey::from(OutPoint::new(txid, vout));
            if chainstate.get(outpoint)?.is_some() {
                collisions.insert(outpoint);
            }
        }
    }
    Ok(collisions.into_iter().collect())
}

/// Disconnects the current execution tip using its durable undo record.
///
/// Unlike [`connect_active_block`], the executed header need not remain on the
/// newly selected active chain; this is the primitive used to walk back to a
/// common ancestor before connecting a stronger branch.
pub fn disconnect_execution_tip<S: UtxoStore>(
    chainstate: &S,
    undo_store: &RedbUndoStore,
    execution_store: &RedbExecutionStore,
    headers: &HeaderDag,
    now: u64,
    hot_window_secs: u64,
) -> Result<ExecutionTip, BlockExecutionError> {
    let current = execution_store.tip()?;
    if current.height == 0 {
        return Err(BlockExecutionError::DisconnectGenesis);
    }
    let current_header = headers
        .get(&current.hash)
        .ok_or(BlockExecutionError::MissingExecutedHeader(current.hash))?;
    let parent_hash = current_header.header.prev_blockhash;
    let parent = headers
        .get(&parent_hash)
        .ok_or(BlockExecutionError::MissingExecutedHeader(parent_hash))?;
    if parent.height.checked_add(1) != Some(current.height) {
        return Err(BlockExecutionError::MissingExecutedHeader(parent_hash));
    }
    let transaction_undos = undo_store
        .get(current.hash)?
        .ok_or(BlockExecutionError::MissingUndo(current.hash))?;
    let applied = AppliedBlock {
        hash: current.hash,
        transaction_undos,
    };
    disconnect_block(chainstate, &applied, now, hot_window_secs)?;
    let parent_tip = ExecutionTip {
        height: parent.height,
        hash: parent.hash,
    };
    execution_store.rewind(current, parent_tip)?;
    undo_store.remove(current.hash)?;
    Ok(parent_tip)
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
        utxo::{OutPointKey, RedbUtxoStore, Utxo, UtxoStore},
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
        let active_block = block(genesis.hash, genesis.header.time + 1);
        let info = headers
            .insert_contextual(active_block.header, active_block.header.time)
            .unwrap();

        let applied = connect_active_block(
            &chainstate,
            &undo_store,
            &execution_store,
            &headers,
            &active_block,
            1,
            60,
            BlockDeploymentContext::default(),
        )
        .unwrap();
        assert_eq!(execution_store.tip().unwrap().hash, info.hash);
        assert!(undo_store.get(applied.hash).unwrap().is_some());
        let coinbase_outpoint =
            OutPointKey::from(OutPoint::new(active_block.txdata[0].compute_txid(), 0));
        assert!(
            crate::utxo::UtxoStore::get(&chainstate, coinbase_outpoint)
                .unwrap()
                .is_some()
        );

        let side_one = block(genesis.hash, genesis.header.time + 2);
        let side_one_info = headers
            .insert_contextual(side_one.header, side_one.header.time)
            .unwrap();
        let side_two = block(side_one_info.hash, side_one.header.time + 1);
        headers
            .insert_contextual(side_two.header, side_two.header.time)
            .unwrap();
        assert_ne!(headers.active_header_at(1).unwrap().hash, info.hash);

        let rewound =
            disconnect_execution_tip(&chainstate, &undo_store, &execution_store, &headers, 2, 60)
                .unwrap();
        assert_eq!(rewound.height, 0);
        assert!(undo_store.get(applied.hash).unwrap().is_none());
        assert!(
            crate::utxo::UtxoStore::get(&chainstate, coinbase_outpoint)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn bip30_rejects_collisions_and_exception_undo_restores_overwritten_coin() {
        let directory = TempDir::new().unwrap();
        let chainstate = RedbUtxoStore::open(directory.path().join("chainstate.redb")).unwrap();
        let undo_store = RedbUndoStore::open(directory.path().join("undo.redb")).unwrap();
        let execution_store =
            RedbExecutionStore::open(directory.path().join("execution.redb"), Network::Regtest)
                .unwrap();
        let mut headers = HeaderDag::new(Network::Regtest);
        let genesis = headers.active_tip();
        let block = block(genesis.hash, genesis.header.time + 1);
        headers
            .insert_contextual(block.header, block.header.time)
            .unwrap();
        let collision = OutPointKey::from(OutPoint::new(block.txdata[0].compute_txid(), 0));
        chainstate
            .apply(
                &[],
                &[(
                    collision,
                    Utxo {
                        value_sats: 42,
                        height: 0,
                        is_coinbase: false,
                        last_touched: 0,
                        creation_mtp: 0,
                        script_pubkey: Vec::new(),
                    },
                )],
            )
            .unwrap();

        assert!(matches!(
            connect_active_block(
                &chainstate,
                &undo_store,
                &execution_store,
                &headers,
                &block,
                1,
                60,
                BlockDeploymentContext::default(),
            ),
            Err(BlockExecutionError::Bip30Collision(key)) if key == collision
        ));
        assert_eq!(chainstate.get(collision).unwrap().unwrap().value_sats, 42);

        connect_active_block(
            &chainstate,
            &undo_store,
            &execution_store,
            &headers,
            &block,
            1,
            60,
            BlockDeploymentContext {
                bip30_exception: true,
                ..BlockDeploymentContext::default()
            },
        )
        .unwrap();
        assert_eq!(
            chainstate.get(collision).unwrap().unwrap().value_sats,
            block_subsidy(1)
        );
        disconnect_execution_tip(&chainstate, &undo_store, &execution_store, &headers, 2, 60)
            .unwrap();
        assert_eq!(chainstate.get(collision).unwrap().unwrap().value_sats, 42);
    }
}
