//! Sequential active-chain block execution and durable progress coordination.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex},
};

use ahash::AHashMap;
use bitcoin::{Block, BlockHash, OutPoint, Txid};
use thiserror::Error;

use crate::{
    blockchain::{
        AppliedBlock, BlockError, DeferredScriptBatch, DeferredScriptCheck,
        ValidatedBlockTransactionIds, apply_block_with_deployments,
        apply_prevalidated_block_with_deferred_scripts,
        apply_prevalidated_block_with_deferred_scripts_and_txids,
        apply_prevalidated_block_with_deployments, disconnect_block,
        validate_block_structure_with_deployments, verify_deferred_scripts,
    },
    chain_store::{ChainStoreError, ConnectTransition, RedbChainStore},
    chainstate::is_unspendable,
    execution_store::{ExecutionStoreError, ExecutionTip, RedbExecutionStore},
    headers::HeaderDag,
    undo_store::{PendingTransition, RedbUndoStore, TransitionKind, UndoStoreError},
    utxo::{OutPointKey, TierStats, Utxo, UtxoError, UtxoStore, UtxoUndo},
};

/// Consensus deployments selected for a candidate block.
#[derive(Clone, Debug, Eq, PartialEq)]
#[allow(clippy::struct_excessive_bools)]
pub struct BlockDeploymentContext {
    /// Explicit libbitcoinconsensus script verification flags.
    pub script_flags: u32,
    /// Whether BIP34 coinbase-height commitment is active.
    pub bip34_active: bool,
    /// Whether the CSV deployment (BIP68/BIP112/BIP113) is active.
    pub csv_active: bool,
    /// Whether BIP141 witness commitments and BIP147 NULLDUMMY are active.
    pub segwit_active: bool,
    /// BIP325 challenge script for the selected default or custom Signet.
    pub signet_challenge: Option<Arc<[u8]>>,
    /// Whether Core requires collision checks for this block's transaction outputs.
    pub bip30_enforced: bool,
    /// Whether a historical BIP30 exception must preserve overwritten coins for undo.
    pub bip30_overwrite: bool,
    /// Maximum proof-of-work subsidy for this candidate height.
    pub subsidy_sats: u64,
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
    /// Unified chain-state transaction failed.
    #[error("atomic chain-state persistence: {0}")]
    ChainStore(#[from] ChainStoreError),
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
    /// Assumed snapshot UTXOs have no block undo below their trusted base.
    #[error("cannot disconnect assumed snapshot base {height}:{hash}")]
    DisconnectAssumedSnapshotBase {
        /// Snapshot base height.
        height: u32,
        /// Snapshot base block hash.
        hash: BlockHash,
    },
    /// Durable undo data is missing for an executed block.
    #[error("missing durable undo for executed block {0}")]
    MissingUndo(BlockHash),
    /// BIP30 forbids overwriting an existing unspent transaction output.
    #[error("BIP30 duplicate unspent output {0}")]
    Bip30Collision(OutPointKey),
    /// A batch did not provide exactly one deployment context per block.
    #[error("block batch has {blocks} blocks but {deployments} deployment contexts")]
    DeploymentCount {
        /// Number of candidate blocks.
        blocks: usize,
        /// Number of deployment contexts.
        deployments: usize,
    },
    /// Precomputed transaction identifiers do not align with their blocks.
    #[error("precomputed transaction identifiers do not match block batch")]
    TransactionIdCount,
    /// A write-ahead transition does not match either its pre- or post-state.
    #[error("pending transition UTXO state is internally inconsistent")]
    InconsistentTransition,
    /// A pending transition is unrelated to the durable execution tip.
    #[error("execution tip {actual_height}:{actual_hash} matches neither pending parent nor child")]
    TransitionTipMismatch {
        /// Durable execution height.
        actual_height: u32,
        /// Durable execution hash.
        actual_hash: BlockHash,
    },
}

impl BlockExecutionError {
    /// Returns whether a freshly downloaded block is objectively invalid peer data.
    #[must_use]
    pub const fn is_peer_invalid(&self) -> bool {
        match self {
            Self::UnexpectedBlock { .. } => true,
            Self::Block(error) => error.is_peer_invalid(),
            _ => false,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PendingUtxoState {
    Before,
    After,
    Mixed,
}

fn pending_utxo_state<S: UtxoStore>(
    chainstate: &S,
    pending: &PendingTransition,
) -> Result<PendingUtxoState, UtxoError> {
    let before = pending
        .undo
        .spent()
        .iter()
        .cloned()
        .collect::<BTreeMap<_, _>>();
    let after = pending.created.iter().cloned().collect::<BTreeMap<_, _>>();
    let keys = pending
        .undo
        .spent()
        .iter()
        .map(|(outpoint, _)| *outpoint)
        .chain(pending.undo.created().iter().copied())
        .collect::<BTreeSet<_>>();
    let mut matches_before = true;
    let mut matches_after = true;
    for outpoint in keys {
        let current = chainstate.get(outpoint)?;
        matches_before &= current.as_ref() == before.get(&outpoint);
        matches_after &= current.as_ref() == after.get(&outpoint);
    }
    Ok(match (matches_before, matches_after) {
        (true, _) => PendingUtxoState::Before,
        (false, true) => PendingUtxoState::After,
        (false, false) => PendingUtxoState::Mixed,
    })
}

/// Recovers an interrupted write-ahead block transition idempotently.
///
/// Connect intents are rolled back unless the execution tip reached the child.
/// Disconnect intents are completed unless the execution tip already reached
/// the parent. Every recovery step is safe to retry after another interruption.
pub fn recover_pending_transition<S: UtxoStore>(
    chainstate: &S,
    undo_store: &RedbUndoStore,
    execution_store: &RedbExecutionStore,
    now: u64,
    hot_window_secs: u64,
) -> Result<bool, BlockExecutionError> {
    let Some(pending) = undo_store.pending_transition()? else {
        return Ok(false);
    };
    let execution_tip = execution_store.tip()?;
    let state = pending_utxo_state(chainstate, &pending)?;
    match pending.kind {
        TransitionKind::Connect if execution_tip == pending.parent => {
            match state {
                PendingUtxoState::Before => {}
                PendingUtxoState::After => {
                    chainstate.undo(&pending.undo, now, hot_window_secs)?;
                }
                PendingUtxoState::Mixed => {
                    return Err(BlockExecutionError::InconsistentTransition);
                }
            }
            undo_store.remove(pending.next.hash)?;
            undo_store.clear_transition(pending.next.hash)?;
            Ok(true)
        }
        TransitionKind::Connect if execution_tip == pending.next => {
            if state != PendingUtxoState::After {
                return Err(BlockExecutionError::InconsistentTransition);
            }
            if undo_store.get(pending.next.hash)?.is_none() {
                return Err(BlockExecutionError::MissingUndo(pending.next.hash));
            }
            undo_store.clear_transition(pending.next.hash)?;
            Ok(true)
        }
        TransitionKind::Disconnect if execution_tip == pending.next => {
            match state {
                PendingUtxoState::After => {
                    chainstate.undo(&pending.undo, now, hot_window_secs)?;
                }
                PendingUtxoState::Before => {}
                PendingUtxoState::Mixed => {
                    return Err(BlockExecutionError::InconsistentTransition);
                }
            }
            execution_store.rewind(pending.next, pending.parent)?;
            undo_store.remove(pending.next.hash)?;
            undo_store.clear_transition(pending.next.hash)?;
            Ok(true)
        }
        TransitionKind::Disconnect if execution_tip == pending.parent => {
            if state != PendingUtxoState::Before {
                return Err(BlockExecutionError::InconsistentTransition);
            }
            undo_store.remove(pending.next.hash)?;
            undo_store.clear_transition(pending.next.hash)?;
            Ok(true)
        }
        _ => Err(BlockExecutionError::TransitionTipMismatch {
            actual_height: execution_tip.height,
            actual_hash: execution_tip.hash,
        }),
    }
}

/// Validates and connects exactly the next active-chain block.
///
/// Validation runs against a lazy in-memory overlay. The complete block's net
/// UTXO effect, block undo, and execution tip are committed in one storage
/// transaction, so a process crash cannot expose a partially applied block.
#[allow(clippy::too_many_arguments)]
pub fn connect_active_block(
    chainstate: &RedbChainStore,
    headers: &HeaderDag,
    block: &Block,
    now: u64,
    hot_window_secs: u64,
    deployments: &BlockDeploymentContext,
) -> Result<AppliedBlock, BlockExecutionError> {
    let current = chainstate.execution().tip()?;
    let (applied, transition) = validate_active_block(
        chainstate,
        headers,
        block,
        current,
        now,
        hot_window_secs,
        deployments,
        false,
    )?;
    let next_tip = ExecutionTip {
        height: current.height + 1,
        hash: applied.hash,
    };
    let committed_undo = chainstate.commit_connect(
        current.hash,
        next_tip,
        &transition.spent,
        &transition.created,
        &applied.transaction_undos,
    )?;
    debug_assert_eq!(committed_undo, transition.undo);
    Ok(applied)
}

/// Validates a contiguous IBD block group and commits it as one checkpoint.
///
/// Every block is evaluated against the prior block's in-memory UTXO result.
/// A validation or persistence failure exposes neither a UTXO prefix nor a
/// corresponding undo/tip prefix.
#[allow(clippy::too_many_arguments)]
pub fn connect_active_blocks(
    chainstate: &RedbChainStore,
    headers: &HeaderDag,
    blocks: &[Block],
    now: u64,
    hot_window_secs: u64,
    deployments: &[BlockDeploymentContext],
) -> Result<Vec<AppliedBlock>, BlockExecutionError> {
    connect_active_blocks_inner(
        chainstate,
        headers,
        blocks,
        now,
        hot_window_secs,
        deployments,
        false,
        None,
    )
}

/// Connects a downloaded batch whose block structures were already checked
/// against the supplied deployment contexts.
///
/// Script, UTXO, lock-time, subsidy, fee, and sigop validation remain enabled.
#[allow(clippy::too_many_arguments)]
pub fn connect_prevalidated_active_blocks(
    chainstate: &RedbChainStore,
    headers: &HeaderDag,
    blocks: &[Block],
    now: u64,
    hot_window_secs: u64,
    deployments: &[BlockDeploymentContext],
) -> Result<Vec<AppliedBlock>, BlockExecutionError> {
    connect_active_blocks_inner(
        chainstate,
        headers,
        blocks,
        now,
        hot_window_secs,
        deployments,
        true,
        None,
    )
}

/// Connects structurally authenticated blocks while reusing the transaction
/// identifiers already computed for their Merkle roots.
#[allow(clippy::too_many_arguments)]
pub fn connect_prevalidated_active_blocks_with_txids(
    chainstate: &RedbChainStore,
    headers: &HeaderDag,
    blocks: &[Block],
    transaction_ids: &[ValidatedBlockTransactionIds],
    now: u64,
    hot_window_secs: u64,
    deployments: &[BlockDeploymentContext],
) -> Result<Vec<AppliedBlock>, BlockExecutionError> {
    connect_active_blocks_inner(
        chainstate,
        headers,
        blocks,
        now,
        hot_window_secs,
        deployments,
        true,
        Some(transaction_ids),
    )
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
fn connect_active_blocks_inner(
    chainstate: &RedbChainStore,
    headers: &HeaderDag,
    blocks: &[Block],
    now: u64,
    hot_window_secs: u64,
    deployments: &[BlockDeploymentContext],
    structure_prevalidated: bool,
    transaction_ids: Option<&[ValidatedBlockTransactionIds]>,
) -> Result<Vec<AppliedBlock>, BlockExecutionError> {
    if blocks.len() != deployments.len() {
        return Err(BlockExecutionError::DeploymentCount {
            blocks: blocks.len(),
            deployments: deployments.len(),
        });
    }
    if blocks.is_empty() {
        return Ok(Vec::new());
    }
    if transaction_ids.is_some_and(|ids| {
        ids.len() != blocks.len()
            || ids
                .iter()
                .zip(blocks)
                .any(|(ids, block)| ids.as_slice().len() != block.txdata.len())
    }) {
        return Err(BlockExecutionError::TransactionIdCount);
    }

    let mut current = chainstate.execution().tip()?;
    let mut input_outpoints = blocks
        .iter()
        .flat_map(|block| block.txdata.iter().skip(1))
        .flat_map(|transaction| transaction.input.iter())
        .map(|input| OutPointKey::from(input.previous_output))
        .collect::<Vec<_>>();
    input_outpoints.sort_unstable();
    input_outpoints.dedup();
    let output_count = blocks
        .iter()
        .flat_map(|block| &block.txdata)
        .map(|transaction| transaction.output.len())
        .sum::<usize>();
    let cumulative = UtxoOverlay::with_capacity(
        chainstate,
        input_outpoints.len().saturating_add(output_count),
    );
    cumulative.prefetch(&input_outpoints)?;
    let mut applied_blocks = Vec::with_capacity(blocks.len());
    let mut transitions = Vec::with_capacity(blocks.len());
    let mut deferred_scripts = Vec::new();
    let mut script_batch = (blocks.len() > 1).then(DeferredScriptBatch::new);
    for (block_order, (block, deployment)) in blocks.iter().zip(deployments).enumerate() {
        let block_capacity = block
            .txdata
            .iter()
            .map(|transaction| transaction.input.len() + transaction.output.len())
            .sum();
        let block_overlay = UtxoOverlay::with_capacity(&cumulative, block_capacity);
        let validated = validate_active_block_inner(
            &block_overlay,
            headers,
            block,
            current,
            now,
            hot_window_secs,
            deployment,
            structure_prevalidated,
            true,
            transaction_ids.map(|ids| ids[block_order].as_slice()),
        );
        let (applied, changes, mut block_scripts) = match validated {
            Ok(validated) => validated,
            Err(error) => {
                let script_failure = if let Some(batch) = script_batch.take() {
                    batch.finish()
                } else {
                    verify_deferred_scripts(std::mem::take(&mut deferred_scripts))
                };
                if let Some((index, source)) = script_failure {
                    return Err(BlockExecutionError::Block(BlockError::Transaction {
                        index,
                        source: source.into(),
                    }));
                }
                return Err(error);
            }
        };
        for script in &mut block_scripts {
            script.set_block_order(block_order);
        }
        let next = ExecutionTip {
            height: current
                .height
                .checked_add(1)
                .ok_or(BlockExecutionError::NoNextHeader(current.height))?,
            hash: applied.hash,
        };
        cumulative.apply_validated_changes(&changes);
        transitions.push(ConnectTransition {
            expected_parent: current.hash,
            next,
            spent: changes.spent,
            created: changes.created,
            transaction_undos: if chainstate.retains_block_undo() {
                applied.transaction_undos.clone()
            } else {
                Vec::new()
            },
        });
        applied_blocks.push(applied);
        if let Some(batch) = &mut script_batch {
            batch.submit(block_scripts);
        } else {
            deferred_scripts.extend(block_scripts);
        }
        current = next;
    }
    let script_failure = if let Some(batch) = script_batch {
        batch.finish()
    } else {
        verify_deferred_scripts(deferred_scripts)
    };
    if let Some((index, source)) = script_failure {
        return Err(BlockExecutionError::Block(BlockError::Transaction {
            index,
            source: source.into(),
        }));
    }
    chainstate.commit_connect_batch(&transitions)?;
    Ok(applied_blocks)
}

#[allow(clippy::too_many_arguments)]
fn validate_active_block<S: UtxoStore>(
    chainstate: &S,
    headers: &HeaderDag,
    block: &Block,
    current: ExecutionTip,
    now: u64,
    hot_window_secs: u64,
    deployments: &BlockDeploymentContext,
    structure_prevalidated: bool,
) -> Result<(AppliedBlock, UtxoChanges), BlockExecutionError> {
    validate_active_block_inner(
        chainstate,
        headers,
        block,
        current,
        now,
        hot_window_secs,
        deployments,
        structure_prevalidated,
        false,
        None,
    )
    .map(|(applied, changes, _)| (applied, changes))
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
fn validate_active_block_inner<'a, S: UtxoStore>(
    chainstate: &S,
    headers: &HeaderDag,
    block: &'a Block,
    current: ExecutionTip,
    now: u64,
    hot_window_secs: u64,
    deployments: &BlockDeploymentContext,
    structure_prevalidated: bool,
    defer_scripts: bool,
    transaction_ids: Option<&[Txid]>,
) -> Result<(AppliedBlock, UtxoChanges, Vec<DeferredScriptCheck<'a>>), BlockExecutionError> {
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
    let overlay = UtxoOverlay::new(chainstate);
    let exception_undo = apply_bip30_rules(&overlay, block, deployments)?;
    let (mut applied, scripts) = if defer_scripts {
        if !structure_prevalidated {
            validate_block_structure_with_deployments(
                block,
                next_height,
                deployments.bip34_active,
                deployments.segwit_active,
                deployments.signet_challenge.as_deref(),
            )
            .map_err(BlockExecutionError::Block)?;
        }
        match transaction_ids {
            Some(transaction_ids) => apply_prevalidated_block_with_deferred_scripts_and_txids(
                &overlay,
                block,
                transaction_ids,
                next_height,
                now,
                parent_mtp,
                hot_window_secs,
                deployments.script_flags,
                deployments.csv_active,
                deployments.subsidy_sats,
            ),
            None => apply_prevalidated_block_with_deferred_scripts(
                &overlay,
                block,
                next_height,
                now,
                parent_mtp,
                hot_window_secs,
                deployments.script_flags,
                deployments.csv_active,
                deployments.subsidy_sats,
            ),
        }
        .map_err(BlockExecutionError::Block)?
    } else if structure_prevalidated {
        (
            apply_prevalidated_block_with_deployments(
                &overlay,
                block,
                next_height,
                now,
                parent_mtp,
                hot_window_secs,
                deployments.script_flags,
                deployments.csv_active,
                deployments.subsidy_sats,
            )
            .map_err(BlockExecutionError::Block)?,
            Vec::new(),
        )
    } else {
        (
            apply_block_with_deployments(
                &overlay,
                block,
                next_height,
                now,
                parent_mtp,
                hot_window_secs,
                deployments.script_flags,
                deployments.bip34_active,
                deployments.csv_active,
                deployments.segwit_active,
                deployments.signet_challenge.as_deref(),
                deployments.subsidy_sats,
            )
            .map_err(BlockExecutionError::Block)?,
            Vec::new(),
        )
    };
    if let Some(undo) = exception_undo {
        applied.transaction_undos.insert(0, undo);
    }
    let transition = overlay.net_changes()?;
    Ok((applied, transition, scripts))
}

#[derive(Default)]
struct OverlayState {
    original: AHashMap<OutPointKey, Option<Utxo>>,
    current: AHashMap<OutPointKey, Option<Utxo>>,
}

struct UtxoChanges {
    spent: Vec<OutPointKey>,
    created: Vec<(OutPointKey, Utxo)>,
    undo: UtxoUndo,
}

/// Block-scoped UTXO mutations retained in memory until validation succeeds.
struct UtxoOverlay<'a, S> {
    base: &'a S,
    state: Mutex<OverlayState>,
}

impl<'a, S: UtxoStore> UtxoOverlay<'a, S> {
    fn new(base: &'a S) -> Self {
        Self::with_capacity(base, 0)
    }

    fn with_capacity(base: &'a S, capacity: usize) -> Self {
        Self {
            base,
            state: Mutex::new(OverlayState {
                original: AHashMap::with_capacity(capacity),
                current: AHashMap::with_capacity(capacity),
            }),
        }
    }

    fn load(
        &self,
        state: &mut OverlayState,
        outpoint: OutPointKey,
    ) -> Result<Option<Utxo>, UtxoError> {
        if let Some(value) = state.current.get(&outpoint) {
            return Ok(value.clone());
        }
        let value = self.base.get(outpoint)?;
        state.original.insert(outpoint, value.clone());
        state.current.insert(outpoint, value.clone());
        Ok(value)
    }

    fn net_changes(&self) -> Result<UtxoChanges, UtxoError> {
        let state = self.state.lock().expect("overlay lock not poisoned");
        let mut spent = Vec::new();
        let mut created = Vec::new();
        let mut undo_spent = Vec::new();
        for (outpoint, original) in &state.original {
            let current = state
                .current
                .get(outpoint)
                .ok_or(UtxoError::Malformed("overlay current value"))?;
            if original == current {
                continue;
            }
            if original.is_some() {
                spent.push(*outpoint);
                undo_spent.push((
                    *outpoint,
                    original.clone().expect("original was checked as present"),
                ));
            }
            if let Some(utxo) = current {
                created.push((*outpoint, utxo.clone()));
            }
        }
        spent.sort_unstable();
        created.sort_unstable_by_key(|(outpoint, _)| *outpoint);
        undo_spent.sort_unstable_by_key(|(outpoint, _)| *outpoint);
        let undo_created = created.iter().map(|(outpoint, _)| *outpoint).collect();
        Ok(UtxoChanges {
            spent,
            created,
            undo: UtxoUndo::new(undo_spent, undo_created),
        })
    }

    fn apply_validated_changes(&self, changes: &UtxoChanges) {
        let mut state = self.state.lock().expect("overlay lock not poisoned");
        for outpoint in &changes.spent {
            state.current.insert(*outpoint, None);
        }
        for (outpoint, utxo) in &changes.created {
            state.current.insert(*outpoint, Some(utxo.clone()));
        }
    }

    fn prefetch(&self, outpoints: &[OutPointKey]) -> Result<(), UtxoError> {
        let prefetched = self.base.get_many(outpoints)?;
        let mut state = self.state.lock().expect("overlay lock not poisoned");
        for (outpoint, value) in prefetched {
            state.original.insert(outpoint, value.clone());
            state.current.insert(outpoint, value);
        }
        Ok(())
    }
}

impl<S: UtxoStore> UtxoStore for UtxoOverlay<'_, S> {
    fn get(&self, outpoint: OutPointKey) -> Result<Option<Utxo>, UtxoError> {
        let mut state = self.state.lock().expect("overlay lock not poisoned");
        self.load(&mut state, outpoint)
    }

    fn apply(
        &self,
        spent: &[OutPointKey],
        created: &[(OutPointKey, Utxo)],
    ) -> Result<(), UtxoError> {
        self.apply_with_undo(spent, created).map(|_| ())
    }

    fn apply_with_undo(
        &self,
        spent: &[OutPointKey],
        created: &[(OutPointKey, Utxo)],
    ) -> Result<UtxoUndo, UtxoError> {
        let mut state = self.state.lock().expect("overlay lock not poisoned");
        let mut seen_spent = BTreeSet::new();
        let mut undo_spent = Vec::with_capacity(spent.len());
        for outpoint in spent {
            if !seen_spent.insert(*outpoint) {
                return Err(UtxoError::DuplicateSpend(*outpoint));
            }
            let previous = self
                .load(&mut state, *outpoint)?
                .ok_or(UtxoError::Missing(*outpoint))?;
            undo_spent.push((*outpoint, previous));
        }
        let mut seen_created = BTreeSet::new();
        for (outpoint, _) in created {
            if !seen_created.insert(*outpoint)
                || (!seen_spent.contains(outpoint) && self.load(&mut state, *outpoint)?.is_some())
            {
                return Err(UtxoError::Duplicate(*outpoint));
            }
        }
        for outpoint in spent {
            state.current.insert(*outpoint, None);
        }
        for (outpoint, utxo) in created {
            state.current.insert(*outpoint, Some(utxo.clone()));
        }
        Ok(UtxoUndo::new(
            undo_spent,
            created.iter().map(|(outpoint, _)| *outpoint).collect(),
        ))
    }

    fn apply_with_undo_fresh_outputs(
        &self,
        spent: &[OutPointKey],
        created: &[(OutPointKey, Utxo)],
    ) -> Result<UtxoUndo, UtxoError> {
        let mut state = self.state.lock().expect("overlay lock not poisoned");
        let mut seen_spent = BTreeSet::new();
        let mut undo_spent = Vec::with_capacity(spent.len());
        for outpoint in spent {
            if !seen_spent.insert(*outpoint) {
                return Err(UtxoError::DuplicateSpend(*outpoint));
            }
            let previous = self
                .load(&mut state, *outpoint)?
                .ok_or(UtxoError::Missing(*outpoint))?;
            undo_spent.push((*outpoint, previous));
        }
        let mut seen_created = BTreeSet::new();
        for (outpoint, _) in created {
            if !seen_created.insert(*outpoint)
                || (!seen_spent.contains(outpoint)
                    && state.current.get(outpoint).is_some_and(Option::is_some))
            {
                return Err(UtxoError::Duplicate(*outpoint));
            }
        }
        for outpoint in spent {
            state.current.insert(*outpoint, None);
        }
        for (outpoint, utxo) in created {
            state.original.entry(*outpoint).or_insert(None);
            state.current.insert(*outpoint, Some(utxo.clone()));
        }
        Ok(UtxoUndo::new(
            undo_spent,
            created.iter().map(|(outpoint, _)| *outpoint).collect(),
        ))
    }

    fn undo(&self, undo: &UtxoUndo, _now: u64, _hot_window_secs: u64) -> Result<(), UtxoError> {
        let mut state = self.state.lock().expect("overlay lock not poisoned");
        for (outpoint, _) in undo.spent() {
            if self.load(&mut state, *outpoint)?.is_some() {
                return Err(UtxoError::Duplicate(*outpoint));
            }
        }
        for outpoint in undo.created() {
            self.load(&mut state, *outpoint)?;
            state.current.insert(*outpoint, None);
        }
        for (outpoint, utxo) in undo.spent() {
            state.current.insert(*outpoint, Some(utxo.clone()));
        }
        Ok(())
    }

    fn age_to_cold(&self, _now: u64, _hot_window_secs: u64) -> Result<u64, UtxoError> {
        Ok(0)
    }

    fn snapshot_entries(&self) -> Result<BTreeMap<OutPointKey, Utxo>, UtxoError> {
        let mut entries = self.base.snapshot_entries()?;
        let state = self.state.lock().expect("overlay lock not poisoned");
        for (outpoint, current) in &state.current {
            if let Some(utxo) = current {
                entries.insert(*outpoint, utxo.clone());
            } else {
                entries.remove(outpoint);
            }
        }
        Ok(entries)
    }

    fn replace_all(
        &self,
        entries: &BTreeMap<OutPointKey, Utxo>,
        _now: u64,
        _hot_window_secs: u64,
    ) -> Result<(), UtxoError> {
        let base = self.base.snapshot_entries()?;
        let mut state = self.state.lock().expect("overlay lock not poisoned");
        state.original.clear();
        state.current.clear();
        for outpoint in base.keys().chain(entries.keys()) {
            state
                .original
                .insert(*outpoint, base.get(outpoint).cloned());
            state
                .current
                .insert(*outpoint, entries.get(outpoint).cloned());
        }
        Ok(())
    }

    fn tier_stats(&self) -> Result<TierStats, UtxoError> {
        Ok(TierStats {
            hot: u64::try_from(self.snapshot_entries()?.len())
                .map_err(|_| UtxoError::Malformed("overlay entry count"))?,
            cold: 0,
        })
    }
}

fn block_output_collisions<S: UtxoStore>(
    chainstate: &S,
    block: &Block,
) -> Result<Vec<OutPointKey>, UtxoError> {
    let mut collisions = BTreeSet::new();
    for transaction in &block.txdata {
        let txid = transaction.compute_txid();
        for (vout, output) in transaction.output.iter().enumerate() {
            if is_unspendable(&output.script_pubkey) {
                continue;
            }
            let vout = u32::try_from(vout).map_err(|_| UtxoError::Malformed("output index"))?;
            let outpoint = OutPointKey::from(OutPoint::new(txid, vout));
            if chainstate.get(outpoint)?.is_some() {
                collisions.insert(outpoint);
            }
        }
    }
    Ok(collisions.into_iter().collect())
}

fn apply_bip30_rules<S: UtxoStore>(
    chainstate: &S,
    block: &Block,
    deployments: &BlockDeploymentContext,
) -> Result<Option<UtxoUndo>, BlockExecutionError> {
    if !deployments.bip30_enforced && !deployments.bip30_overwrite {
        return Ok(None);
    }
    let collisions = block_output_collisions(chainstate, block)?;
    if collisions.is_empty() {
        return Ok(None);
    }
    if deployments.bip30_overwrite {
        return chainstate
            .apply_with_undo(&collisions, &[])
            .map(Some)
            .map_err(Into::into);
    }
    Err(BlockExecutionError::Bip30Collision(collisions[0]))
}

/// Disconnects the current execution tip using its durable undo record.
///
/// Unlike [`connect_active_block`], the executed header need not remain on the
/// newly selected active chain; this is the primitive used to walk back to a
/// common ancestor before connecting a stronger branch.
pub fn disconnect_execution_tip(
    chainstate: &RedbChainStore,
    headers: &HeaderDag,
    now: u64,
    hot_window_secs: u64,
) -> Result<ExecutionTip, BlockExecutionError> {
    let current = chainstate.execution().tip()?;
    if current.height == 0 {
        return Err(BlockExecutionError::DisconnectGenesis);
    }
    if chainstate.execution().assumed_snapshot_base()? == Some(current) {
        return Err(BlockExecutionError::DisconnectAssumedSnapshotBase {
            height: current.height,
            hash: current.hash,
        });
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
    let transaction_undos = chainstate
        .undos()
        .get(current.hash)?
        .ok_or(BlockExecutionError::MissingUndo(current.hash))?;
    let applied = AppliedBlock {
        hash: current.hash,
        transaction_undos,
    };
    let parent_tip = ExecutionTip {
        height: parent.height,
        hash: parent.hash,
    };
    let overlay = UtxoOverlay::new(chainstate);
    disconnect_block(&overlay, &applied, now, hot_window_secs)?;
    let transition = overlay.net_changes()?;
    chainstate.commit_disconnect(current, parent_tip, &transition.spent, &transition.created)?;
    Ok(parent_tip)
}

#[cfg(test)]
mod tests {
    use bitcoin::{
        Amount, Block, Network, OutPoint, ScriptBuf, Sequence, Transaction, TxIn, TxMerkleNode,
        TxOut, Witness,
        absolute::LockTime,
        block::{Header, Version as HeaderVersion},
        consensus::deserialize,
        hashes::Hash,
        hex::FromHex,
        pow::Target,
        transaction::Version,
    };
    use tempfile::TempDir;

    use super::*;
    use crate::{
        blockchain::{BlockError, block_subsidy},
        chain_store::RedbChainStore,
        deployments::block_deployment_context,
        headers::HeaderDag,
        utxo::{OutPointKey, RedbUtxoStore, Utxo, UtxoStore},
    };

    #[test]
    fn only_downloaded_consensus_failures_are_peer_invalid() {
        assert!(BlockExecutionError::Block(BlockError::Empty).is_peer_invalid());
        assert!(
            BlockExecutionError::UnexpectedBlock {
                expected: BlockHash::all_zeros(),
                actual: BlockHash::from_byte_array([1; 32]),
            }
            .is_peer_invalid()
        );
        assert!(
            !BlockExecutionError::Block(BlockError::Rollback(UtxoError::Malformed("test")))
                .is_peer_invalid()
        );
        assert!(!BlockExecutionError::NoNextHeader(0).is_peer_invalid());
    }

    fn coinbase(height: u32) -> Transaction {
        let mut height_prefix = match height {
            0 => vec![0x00],
            1..=16 => vec![0x50 + u8::try_from(height).unwrap()],
            _ => vec![1, u8::try_from(height).unwrap()],
        };
        height_prefix.push(0);
        Transaction {
            version: Version::ONE,
            lock_time: LockTime::ZERO,
            input: vec![TxIn {
                previous_output: OutPoint::null(),
                script_sig: ScriptBuf::from_bytes(height_prefix),
                sequence: Sequence::MAX,
                witness: Witness::default(),
            }],
            output: vec![TxOut {
                value: Amount::from_sat(block_subsidy(height)),
                script_pubkey: ScriptBuf::new(),
            }],
        }
    }

    fn block_with_transactions(
        parent: BlockHash,
        time: u32,
        transactions: Vec<Transaction>,
    ) -> Block {
        let mut block = Block {
            header: Header {
                version: HeaderVersion::from_consensus(4),
                prev_blockhash: parent,
                merkle_root: TxMerkleNode::all_zeros(),
                time,
                bits: Target::MAX_ATTAINABLE_REGTEST.to_compact_lossy(),
                nonce: 0,
            },
            txdata: transactions,
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

    fn block(parent: BlockHash, time: u32) -> Block {
        block_with_transactions(parent, time, vec![coinbase(1)])
    }

    fn height_block(parent: BlockHash, time: u32, height: u32) -> Block {
        block_with_transactions(parent, time, vec![coinbase(height)])
    }

    fn deployments(height: u32) -> BlockDeploymentContext {
        block_deployment_context(
            Network::Regtest,
            height,
            BlockHash::all_zeros(),
            u32::MAX,
            true,
        )
    }

    #[test]
    fn connects_active_block_and_recovers_execution_tip() {
        let directory = TempDir::new().unwrap();
        let chainstate =
            RedbChainStore::open(directory.path().join("chainstate.redb"), Network::Regtest)
                .unwrap();
        let mut headers = HeaderDag::new(Network::Regtest);
        let genesis = headers.active_tip();
        let active_block = block(genesis.hash, genesis.header.time + 1);
        let info = headers
            .insert_contextual(active_block.header, active_block.header.time)
            .unwrap();

        let applied =
            connect_active_block(&chainstate, &headers, &active_block, 1, 60, &deployments(1))
                .unwrap();
        assert_eq!(chainstate.execution().tip().unwrap().hash, info.hash);
        assert!(chainstate.undos().get(applied.hash).unwrap().is_some());
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

        let rewound = disconnect_execution_tip(&chainstate, &headers, 2, 60).unwrap();
        assert_eq!(rewound.height, 0);
        assert!(chainstate.undos().get(applied.hash).unwrap().is_none());
        assert!(
            crate::utxo::UtxoStore::get(&chainstate, coinbase_outpoint)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn commits_a_multi_transaction_block_atomically() {
        let directory = TempDir::new().unwrap();
        let chainstate =
            RedbChainStore::open(directory.path().join("chainstate.redb"), Network::Regtest)
                .unwrap();
        let previous = OutPoint::new(bitcoin::Txid::from_byte_array([7; 32]), 0);
        chainstate
            .apply(
                &[],
                &[(
                    previous.into(),
                    Utxo {
                        value_sats: 1_000,
                        height: 0,
                        is_coinbase: false,
                        last_touched: 0,
                        creation_mtp: 0,
                        script_pubkey: vec![0x51],
                    },
                )],
            )
            .unwrap();
        let spend = Transaction {
            version: Version::ONE,
            lock_time: LockTime::ZERO,
            input: vec![TxIn {
                previous_output: previous,
                script_sig: ScriptBuf::new(),
                sequence: Sequence::MAX,
                witness: Witness::default(),
            }],
            output: vec![TxOut {
                value: Amount::from_sat(900),
                script_pubkey: ScriptBuf::new(),
            }],
        };
        let mut headers = HeaderDag::new(Network::Regtest);
        let genesis = headers.active_tip();
        let active_block = block_with_transactions(
            genesis.hash,
            genesis.header.time + 1,
            vec![coinbase(1), spend],
        );
        headers
            .insert_contextual(active_block.header, active_block.header.time)
            .unwrap();

        connect_active_block(&chainstate, &headers, &active_block, 1, 60, &deployments(1)).unwrap();

        assert!(chainstate.get(previous.into()).unwrap().is_none());
        let created = OutPoint::new(active_block.txdata[1].compute_txid(), 0).into();
        assert_eq!(chainstate.get(created).unwrap().unwrap().value_sats, 900);
        assert!(
            chainstate
                .undos()
                .get(active_block.block_hash())
                .unwrap()
                .is_some()
        );
        assert_eq!(chainstate.execution().tip().unwrap().height, 1);
    }

    #[test]
    fn ibd_checkpoint_commits_all_blocks_or_no_blocks() {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("chainstate.redb");
        let chainstate = RedbChainStore::open(&path, Network::Regtest).unwrap();
        let mut headers = HeaderDag::new(Network::Regtest);
        let genesis = headers.active_tip();
        let first = height_block(genesis.hash, genesis.header.time + 1, 1);
        headers
            .insert_contextual(first.header, first.header.time)
            .unwrap();
        let second = height_block(first.block_hash(), first.header.time + 1, 2);
        headers
            .insert_contextual(second.header, second.header.time)
            .unwrap();
        let contexts = [deployments(1), deployments(2)];

        let applied = connect_active_blocks(
            &chainstate,
            &headers,
            &[first.clone(), second.clone()],
            1,
            60,
            &contexts,
        )
        .unwrap();
        assert_eq!(applied.len(), 2);
        assert_eq!(chainstate.execution().tip().unwrap().height, 2);
        assert!(
            chainstate
                .undos()
                .get(first.block_hash())
                .unwrap()
                .is_some()
        );
        assert!(
            chainstate
                .undos()
                .get(second.block_hash())
                .unwrap()
                .is_some()
        );
        drop(chainstate);

        let reopened = RedbChainStore::open(&path, Network::Regtest).unwrap();
        assert_eq!(reopened.execution().tip().unwrap().height, 2);
        for block in [&first, &second] {
            let outpoint = OutPoint::new(block.txdata[0].compute_txid(), 0).into();
            assert!(reopened.get(outpoint).unwrap().is_some());
            assert!(reopened.undos().get(block.block_hash()).unwrap().is_some());
        }

        let failed_directory = TempDir::new().unwrap();
        let failed = RedbChainStore::open(
            failed_directory.path().join("chainstate.redb"),
            Network::Regtest,
        )
        .unwrap();
        let mut invalid_second = second.clone();
        invalid_second.txdata[0].output[0].value =
            Amount::from_sat(block_subsidy(2).checked_add(1).unwrap());
        invalid_second.header.merkle_root = invalid_second.compute_merkle_root().unwrap();
        assert!(
            connect_active_blocks(
                &failed,
                &headers,
                &[first.clone(), invalid_second],
                1,
                60,
                &contexts,
            )
            .is_err()
        );
        assert_eq!(failed.execution().tip().unwrap().height, 0);
        let first_outpoint = OutPoint::new(first.txdata[0].compute_txid(), 0).into();
        assert!(failed.get(first_outpoint).unwrap().is_none());
        assert!(failed.undos().get(first.block_hash()).unwrap().is_none());
    }

    #[test]
    fn ibd_checkpoint_defers_all_scripts_but_reports_the_earliest_block() {
        let directory = TempDir::new().unwrap();
        let chainstate =
            RedbChainStore::open(directory.path().join("chainstate.redb"), Network::Regtest)
                .unwrap();
        let outpoints = (0_u8..8)
            .map(|byte| OutPoint::new(bitcoin::Txid::from_byte_array([byte; 32]), 0))
            .collect::<Vec<_>>();
        let coins = outpoints
            .iter()
            .enumerate()
            .map(|(index, outpoint)| {
                (
                    OutPointKey::from(*outpoint),
                    Utxo {
                        value_sats: 2,
                        height: 0,
                        is_coinbase: false,
                        last_touched: 0,
                        creation_mtp: 0,
                        script_pubkey: vec![if matches!(index, 3 | 4) { 0x00 } else { 0x51 }],
                    },
                )
            })
            .collect::<Vec<_>>();
        chainstate.apply(&[], &coins).unwrap();
        let spend = |outpoint: OutPoint| Transaction {
            version: Version::ONE,
            lock_time: LockTime::ZERO,
            input: vec![TxIn {
                previous_output: outpoint,
                script_sig: ScriptBuf::new(),
                sequence: Sequence::MAX,
                witness: Witness::default(),
            }],
            output: vec![TxOut {
                value: Amount::from_sat(1),
                script_pubkey: ScriptBuf::new(),
            }],
        };
        let mut headers = HeaderDag::new(Network::Regtest);
        let genesis = headers.active_tip();
        let first = block_with_transactions(
            genesis.hash,
            genesis.header.time + 1,
            std::iter::once(coinbase(1))
                .chain(outpoints[..4].iter().copied().map(spend))
                .collect(),
        );
        headers
            .insert_contextual(first.header, first.header.time)
            .unwrap();
        let second = block_with_transactions(
            first.block_hash(),
            first.header.time + 1,
            std::iter::once(coinbase(2))
                .chain(outpoints[4..].iter().copied().map(spend))
                .collect(),
        );
        headers
            .insert_contextual(second.header, second.header.time)
            .unwrap();

        assert!(matches!(
            connect_prevalidated_active_blocks(
                &chainstate,
                &headers,
                &[first, second],
                1,
                60,
                &[deployments(1), deployments(2)],
            ),
            Err(BlockExecutionError::Block(BlockError::Transaction {
                index: 4,
                ..
            }))
        ));
        assert_eq!(chainstate.execution().tip().unwrap().height, 0);
        for outpoint in outpoints {
            assert!(chainstate.get(outpoint.into()).unwrap().is_some());
        }
    }

    #[test]
    fn write_ahead_recovery_rolls_back_or_finishes_from_execution_tip() {
        let directory = TempDir::new().unwrap();
        let chainstate = RedbUtxoStore::open(directory.path().join("chainstate.redb")).unwrap();
        let undo_store = RedbUndoStore::open(directory.path().join("undo.redb")).unwrap();
        let execution_store =
            RedbExecutionStore::open(directory.path().join("execution.redb"), Network::Regtest)
                .unwrap();
        let parent = execution_store.tip().unwrap();
        let next = ExecutionTip {
            height: 1,
            hash: BlockHash::from_byte_array([9; 32]),
        };
        let old_key = OutPointKey::from(OutPoint::new(bitcoin::Txid::from_byte_array([1; 32]), 0));
        let new_key = OutPointKey::from(OutPoint::new(bitcoin::Txid::from_byte_array([2; 32]), 0));
        let old_coin = Utxo {
            value_sats: 10,
            height: 0,
            is_coinbase: false,
            last_touched: 0,
            creation_mtp: 0,
            script_pubkey: vec![0x51],
        };
        let new_coin = Utxo {
            value_sats: 9,
            ..old_coin.clone()
        };
        chainstate
            .apply(&[], &[(old_key, old_coin.clone())])
            .unwrap();
        let aggregate = UtxoUndo::new(vec![(old_key, old_coin.clone())], vec![new_key]);
        let pending = PendingTransition {
            kind: TransitionKind::Connect,
            parent,
            next,
            undo: aggregate.clone(),
            created: vec![(new_key, new_coin.clone())],
        };

        undo_store.prepare_transition(&pending).unwrap();
        chainstate
            .apply_with_undo(&[old_key], &[(new_key, new_coin.clone())])
            .unwrap();
        undo_store
            .insert(next.hash, std::slice::from_ref(&aggregate))
            .unwrap();
        assert!(
            recover_pending_transition(&chainstate, &undo_store, &execution_store, 1, 60).unwrap()
        );
        assert_eq!(chainstate.get(old_key).unwrap(), Some(old_coin.clone()));
        assert!(chainstate.get(new_key).unwrap().is_none());
        assert!(undo_store.get(next.hash).unwrap().is_none());
        assert!(undo_store.pending_transition().unwrap().is_none());

        undo_store.prepare_transition(&pending).unwrap();
        chainstate
            .apply_with_undo(&[old_key], &[(new_key, new_coin.clone())])
            .unwrap();
        undo_store
            .insert(next.hash, std::slice::from_ref(&aggregate))
            .unwrap();
        execution_store.advance(parent.hash, next).unwrap();
        assert!(
            recover_pending_transition(&chainstate, &undo_store, &execution_store, 1, 60).unwrap()
        );
        assert!(chainstate.get(old_key).unwrap().is_none());
        assert_eq!(chainstate.get(new_key).unwrap(), Some(new_coin));
        assert!(undo_store.get(next.hash).unwrap().is_some());
        assert!(undo_store.pending_transition().unwrap().is_none());

        let mut disconnect_pending = pending;
        disconnect_pending.kind = TransitionKind::Disconnect;
        undo_store.prepare_transition(&disconnect_pending).unwrap();
        assert!(
            recover_pending_transition(&chainstate, &undo_store, &execution_store, 1, 60).unwrap()
        );
        assert_eq!(execution_store.tip().unwrap(), parent);
        assert_eq!(chainstate.get(old_key).unwrap(), Some(old_coin));
        assert!(chainstate.get(new_key).unwrap().is_none());
        assert!(undo_store.get(next.hash).unwrap().is_none());
        assert!(undo_store.pending_transition().unwrap().is_none());
    }

    #[test]
    fn bip30_rejects_collisions_and_exception_undo_restores_overwritten_coin() {
        let directory = TempDir::new().unwrap();
        let chainstate =
            RedbChainStore::open(directory.path().join("chainstate.redb"), Network::Regtest)
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
                &headers,
                &block,
                1,
                60,
                &deployments(1),
            ),
            Err(BlockExecutionError::Bip30Collision(key)) if key == collision
        ));
        assert_eq!(chainstate.get(collision).unwrap().unwrap().value_sats, 42);

        connect_active_block(
            &chainstate,
            &headers,
            &block,
            1,
            60,
            &BlockDeploymentContext {
                bip30_enforced: false,
                bip30_overwrite: true,
                ..deployments(1)
            },
        )
        .unwrap();
        assert_eq!(
            chainstate.get(collision).unwrap().unwrap().value_sats,
            block_subsidy(1)
        );
        disconnect_execution_tip(&chainstate, &headers, 2, 60).unwrap();
        assert_eq!(chainstate.get(collision).unwrap().unwrap().value_sats, 42);

        connect_active_blocks(
            &chainstate,
            &headers,
            &[block],
            1,
            60,
            &[BlockDeploymentContext {
                bip30_enforced: false,
                bip30_overwrite: true,
                ..deployments(1)
            }],
        )
        .unwrap();
        assert_eq!(
            chainstate.get(collision).unwrap().unwrap().value_sats,
            block_subsidy(1)
        );
        disconnect_execution_tip(&chainstate, &headers, 2, 60).unwrap();
        assert_eq!(chainstate.get(collision).unwrap().unwrap().value_sats, 42);
    }

    #[test]
    fn default_signet_solution_is_enforced_before_chainstate_commit() {
        let encoded = include_str!("../tests/data/bitcoin-core-26/signet-block-1.hex");
        let block: Block = deserialize(&Vec::<u8>::from_hex(encoded.trim()).unwrap()).unwrap();
        let directory = TempDir::new().unwrap();
        let chainstate =
            RedbChainStore::open(directory.path().join("chainstate.redb"), Network::Signet)
                .unwrap();
        let mut headers = HeaderDag::new(Network::Signet);
        headers.insert_contextual(block.header, u32::MAX).unwrap();
        let context = block_deployment_context(
            Network::Signet,
            1,
            block.block_hash(),
            block.header.time,
            false,
        );
        assert!(context.signet_challenge.is_some());

        let mut damaged = block.clone();
        let script = damaged.txdata[0].output[1].script_pubkey.as_mut_bytes();
        let header = script
            .windows(4)
            .position(|window| window == [0xec, 0xc7, 0xda, 0xa2])
            .unwrap();
        script[header + 12] ^= 1;
        let damaged_outpoint =
            OutPointKey::from(OutPoint::new(damaged.txdata[0].compute_txid(), 0));
        assert!(matches!(
            connect_active_block(&chainstate, &headers, &damaged, 1, 60, &context),
            Err(BlockExecutionError::Block(BlockError::Signet(_)))
        ));
        assert_eq!(chainstate.execution().tip().unwrap().height, 0);
        assert!(
            chainstate
                .undos()
                .get(block.block_hash())
                .unwrap()
                .is_none()
        );
        assert!(chainstate.get(damaged_outpoint).unwrap().is_none());

        connect_active_block(&chainstate, &headers, &block, 1, 60, &context).unwrap();
        assert_eq!(chainstate.execution().tip().unwrap().height, 1);
        assert!(
            chainstate
                .undos()
                .get(block.block_hash())
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn custom_signet_challenge_reaches_atomic_block_execution() {
        let encoded = include_str!("../tests/data/bitcoin-core-26/signet-block-1.hex");
        let block: Block = deserialize(&Vec::<u8>::from_hex(encoded.trim()).unwrap()).unwrap();
        let directory = TempDir::new().unwrap();
        let chainstate =
            RedbChainStore::open(directory.path().join("chainstate.redb"), Network::Signet)
                .unwrap();
        let mut headers = HeaderDag::new(Network::Signet);
        headers.insert_contextual(block.header, u32::MAX).unwrap();
        let mut context = block_deployment_context(
            Network::Signet,
            1,
            block.block_hash(),
            block.header.time,
            true,
        );
        context.signet_challenge = Some(Arc::from([0x00]));

        assert!(matches!(
            connect_active_block(&chainstate, &headers, &block, 1, 60, &context),
            Err(BlockExecutionError::Block(BlockError::Signet(_)))
        ));
        assert_eq!(chainstate.execution().tip().unwrap().height, 0);
        assert!(
            chainstate
                .undos()
                .get(block.block_hash())
                .unwrap()
                .is_none()
        );
        assert_eq!(
            chainstate.tier_stats().unwrap(),
            TierStats { hot: 0, cold: 0 }
        );
    }
}
