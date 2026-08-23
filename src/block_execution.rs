//! Sequential active-chain block execution and durable progress coordination.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use ahash::{AHashMap, AHashSet};
use bitcoin::{Block, BlockHash, OutPoint, Txid};
use thiserror::Error;

use crate::{
    blockchain::{
        AppliedBlock, BlockError, DeferredScriptBatch, DeferredScriptCheck, PreparedBlock,
        ValidatedBlockTransactionIds, apply_block_with_deployments,
        apply_prevalidated_block_with_deferred_scripts,
        apply_prevalidated_block_with_deferred_scripts_and_txids,
        apply_prevalidated_block_with_deployments, disconnect_block,
        prepare_prevalidated_block_with_deferred_scripts,
        validate_block_structure_with_deployments, verify_deferred_scripts,
    },
    chain_store::{ChainStoreError, ConnectTransition, ExecutionChainStore},
    chainstate::{PreparedTransaction, is_unspendable},
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

/// Read-only durable UTXOs prepared for one structurally authenticated batch.
///
/// Keeping this value separate from execution lets archive staging and the
/// chainstate read phase overlap without permitting any chainstate mutation
/// before the staged archive is durable.
#[derive(Debug)]
pub struct ActiveBlockUtxoPrefetch {
    entries: Vec<(OutPointKey, Option<Utxo>)>,
}

impl ActiveBlockUtxoPrefetch {
    /// The prefetched coins, for a store that must overlay what changed
    /// since they were read.
    pub fn entries_mut(&mut self) -> &mut [(OutPointKey, Option<Utxo>)] {
        &mut self.entries
    }
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
    /// Prepared UTXOs do not belong to the batch being connected.
    #[error("prepared UTXOs do not match block batch inputs")]
    UtxoPrefetchMismatch,
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
    /// The transition has no observable UTXO effect, so the current chainstate
    /// satisfies both its pre- and post-state.
    ///
    /// A block whose only outputs are provably unspendable and which spends
    /// nothing produces an empty transition, and recovery must accept it from
    /// either side instead of declaring the chainstate inconsistent.
    Either,
    Mixed,
}

impl PendingUtxoState {
    const fn matches_before(self) -> bool {
        matches!(self, Self::Before | Self::Either)
    }

    const fn matches_after(self) -> bool {
        matches!(self, Self::After | Self::Either)
    }
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
        (true, true) => PendingUtxoState::Either,
        (true, false) => PendingUtxoState::Before,
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
                PendingUtxoState::Before | PendingUtxoState::Either => {}
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
            if !state.matches_after() {
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
                PendingUtxoState::Before | PendingUtxoState::Either => {}
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
            if !state.matches_before() {
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
pub fn connect_active_block<C: ExecutionChainStore>(
    chainstate: &C,
    headers: &HeaderDag,
    block: &Block,
    now: u64,
    hot_window_secs: u64,
    deployments: &BlockDeploymentContext,
) -> Result<AppliedBlock, BlockExecutionError> {
    let current = chainstate.execution_tip()?;
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
pub fn connect_active_blocks<C: ExecutionChainStore>(
    chainstate: &C,
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
        None,
        AppliedUndos::Keep,
    )
    .map(|(applied, _)| applied)
}

/// Connects a downloaded batch whose block structures were already checked
/// against the supplied deployment contexts.
///
/// Script, UTXO, lock-time, subsidy, fee, and sigop validation remain enabled.
#[allow(clippy::too_many_arguments)]
pub fn connect_prevalidated_active_blocks<C: ExecutionChainStore>(
    chainstate: &C,
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
        None,
        AppliedUndos::Keep,
    )
    .map(|(applied, _)| applied)
}

/// Connects structurally authenticated blocks while reusing the transaction
/// identifiers already computed for their Merkle roots.
#[allow(clippy::too_many_arguments)]
pub fn connect_prevalidated_active_blocks_with_txids<C: ExecutionChainStore>(
    chainstate: &C,
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
        None,
        AppliedUndos::Keep,
    )
    .map(|(applied, _)| applied)
}

/// Reads the external durable UTXOs required by one prevalidated block batch.
///
/// Outputs created earlier in the same batch are deliberately excluded because
/// execution resolves them through its cumulative in-memory overlay.
pub fn prefetch_prevalidated_active_block_utxos<C: ExecutionChainStore>(
    chainstate: &C,
    blocks: &[Block],
    transaction_ids: &[ValidatedBlockTransactionIds],
) -> Result<ActiveBlockUtxoPrefetch, BlockExecutionError> {
    if transaction_ids.len() != blocks.len()
        || transaction_ids
            .iter()
            .zip(blocks)
            .any(|(ids, block)| ids.as_slice().len() != block.txdata.len())
    {
        return Err(BlockExecutionError::TransactionIdCount);
    }
    let output_count = blocks
        .iter()
        .flat_map(|block| &block.txdata)
        .map(|transaction| transaction.output.len())
        .sum::<usize>();
    let input_outpoints =
        external_batch_input_outpoints(blocks, Some(transaction_ids), output_count);
    let entries = chainstate.get_many(&input_outpoints)?;
    Ok(ActiveBlockUtxoPrefetch { entries })
}

/// [`prefetch_prevalidated_active_block_utxos`] over blocks that are not held
/// in one slice, as a read-ahead holds them beside their transaction ids.
///
/// # Errors
///
/// Fails when a block's transaction-id list does not match it, or on a store
/// read error.
pub fn prefetch_active_block_utxos_from<'a, C: ExecutionChainStore>(
    chainstate: &C,
    blocks: impl Iterator<Item = (&'a Block, &'a ValidatedBlockTransactionIds)> + Clone,
) -> Result<ActiveBlockUtxoPrefetch, BlockExecutionError> {
    let mut output_count = 0_usize;
    for (block, transaction_ids) in blocks.clone() {
        if transaction_ids.as_slice().len() != block.txdata.len() {
            return Err(BlockExecutionError::TransactionIdCount);
        }
        output_count += block
            .txdata
            .iter()
            .map(|transaction| transaction.output.len())
            .sum::<usize>();
    }
    let input_outpoints = external_input_outpoints_with_ids(blocks, output_count);
    let entries = chainstate.get_many(&input_outpoints)?;
    Ok(ActiveBlockUtxoPrefetch { entries })
}

/// Connects a prevalidated batch using UTXOs read before archive staging ended.
#[allow(clippy::too_many_arguments)]
pub fn connect_prevalidated_active_blocks_with_txids_and_utxos<C: ExecutionChainStore>(
    chainstate: &C,
    headers: &HeaderDag,
    blocks: &[Block],
    transaction_ids: &[ValidatedBlockTransactionIds],
    prefetched_utxos: ActiveBlockUtxoPrefetch,
    now: u64,
    hot_window_secs: u64,
    deployments: &[BlockDeploymentContext],
) -> Result<Vec<AppliedBlock>, BlockExecutionError> {
    connect_prevalidated_active_blocks_with_breakdown(
        chainstate,
        headers,
        blocks,
        transaction_ids,
        prefetched_utxos,
        now,
        hot_window_secs,
        deployments,
        AppliedUndos::Keep,
    )
    .map(|(applied, _)| applied)
}

/// As [`connect_prevalidated_active_blocks_with_txids_and_utxos`], also
/// reporting where the time went.
///
/// The catch-up driver uses this so `execution-core` can be broken down in the
/// batch log without a profiler, which matters because it is by far the
/// largest term and the one every optimization question lands on.
///
/// # Errors
///
/// Identical to [`connect_prevalidated_active_blocks_with_txids_and_utxos`].
#[allow(clippy::too_many_arguments)]
pub fn connect_prevalidated_active_blocks_with_breakdown<C: ExecutionChainStore>(
    chainstate: &C,
    headers: &HeaderDag,
    blocks: &[Block],
    transaction_ids: &[ValidatedBlockTransactionIds],
    prefetched_utxos: ActiveBlockUtxoPrefetch,
    now: u64,
    hot_window_secs: u64,
    deployments: &[BlockDeploymentContext],
    applied_undos: AppliedUndos,
) -> Result<(Vec<AppliedBlock>, ExecutionBreakdown), BlockExecutionError> {
    connect_active_blocks_inner(
        chainstate,
        headers,
        blocks,
        now,
        hot_window_secs,
        deployments,
        true,
        Some(transaction_ids),
        Some(prefetched_utxos),
        applied_undos,
    )
}

/// Where a batch's execution time went inside `execution-core`.
///
/// `execution-core` is a single number in the batch log, and it is the largest
/// one, so any question about it — is the sequential loop the bottleneck, or
/// are the script workers? — needs it split. The four parts are disjoint and
/// cover the whole span:
///
/// - `validate` is the sequential per-block work: resolving every input from
///   the UTXO overlay, accounting, maturity, and lock checks. Script
///   verification is not here; it is deferred.
/// - `submit` is handing those deferred scripts to the worker pool, which
///   serializes each transaction and copies its prevouts. It runs on the
///   sequential thread, so it competes with `validate`.
/// - `script_wait` is time blocked waiting for the workers to finish. Near
///   zero means the sequential thread is the bottleneck and the workers were
///   starved; large means the reverse.
/// - `commit` is the single storage transaction that publishes the batch.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ExecutionBreakdown {
    /// Sequential per-block validation, including UTXO resolution.
    pub validate: Duration,
    /// Serializing and enqueuing deferred script work.
    pub submit: Duration,
    /// Blocked waiting for script workers to drain.
    pub script_wait: Duration,
    /// The batch's storage commit.
    pub commit: Duration,
    /// Folding each validated block into the batch overlay and building its
    /// transition.
    pub apply: Duration,
    /// Inside `validate`: input loading and consensus checks per transaction.
    pub validate_prepare: Duration,
    /// Inside `validate`: writing each transaction into the block overlay.
    pub validate_utxo: Duration,
    /// Inside `validate`: folding the block overlay into its net change.
    pub validate_net: Duration,
    /// Inside `validate`: header, tip and BIP30 checks before the loop.
    pub validate_checks: Duration,
    /// Inside `apply`: deriving the block's net change from its prepared
    /// transactions.
    pub apply_net: Duration,
    /// Inside `apply`: folding the net change into the batch overlay.
    pub apply_fold: Duration,
}

/// Whether the executor must leave each block's undo records in its
/// [`AppliedBlock`] as well as in the store transition.
///
/// Explorer and auxiliary indexes read them from the applied block; a bare
/// catch-up does not, and cloning ~7,000 spent coins per block for nobody is
/// a measurable share of `core-apply`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AppliedUndos {
    /// Keep a copy on the applied block (clone into the transition).
    Keep,
    /// Move the records into the transition; the applied block keeps none.
    Drop,
}

/// Inputs of a batch that spend coins created before it, given each block's
/// precomputed transaction ids.
fn external_input_outpoints_with_ids<'a>(
    blocks: impl Iterator<Item = (&'a Block, &'a ValidatedBlockTransactionIds)>,
    output_count: usize,
) -> Vec<OutPointKey> {
    let mut input_outpoints = Vec::new();
    let mut created_in_batch = AHashSet::with_capacity(output_count);
    for (block, transaction_ids) in blocks {
        for (index, (transaction, txid)) in block
            .txdata
            .iter()
            .zip(transaction_ids.as_slice())
            .enumerate()
        {
            if index != 0 {
                input_outpoints.extend(
                    transaction
                        .input
                        .iter()
                        .map(|input| OutPointKey::from(input.previous_output))
                        .filter(|outpoint| !created_in_batch.contains(outpoint)),
                );
            }
            for (vout, output) in transaction.output.iter().enumerate() {
                if is_unspendable(&output.script_pubkey) {
                    continue;
                }
                let vout = u32::try_from(vout).expect("transaction output count fits u32");
                created_in_batch.insert(OutPointKey::from(OutPoint::new(*txid, vout)));
            }
        }
    }
    input_outpoints
}

fn external_batch_input_outpoints(
    blocks: &[Block],
    transaction_ids: Option<&[ValidatedBlockTransactionIds]>,
    output_count: usize,
) -> Vec<OutPointKey> {
    let mut input_outpoints = Vec::new();
    if let Some(transaction_ids) = transaction_ids {
        return external_input_outpoints_with_ids(blocks.iter().zip(transaction_ids), output_count);
    }
    {
        input_outpoints.extend(
            blocks
                .iter()
                .flat_map(|block| block.txdata.iter().skip(1))
                .flat_map(|transaction| transaction.input.iter())
                .map(|input| OutPointKey::from(input.previous_output)),
        );
    }
    input_outpoints.sort_unstable();
    input_outpoints.dedup();
    input_outpoints
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
fn connect_active_blocks_inner<C: ExecutionChainStore>(
    chainstate: &C,
    headers: &HeaderDag,
    blocks: &[Block],
    now: u64,
    _hot_window_secs: u64,
    deployments: &[BlockDeploymentContext],
    structure_prevalidated: bool,
    transaction_ids: Option<&[ValidatedBlockTransactionIds]>,
    prefetched_utxos: Option<ActiveBlockUtxoPrefetch>,
    applied_undos: AppliedUndos,
) -> Result<(Vec<AppliedBlock>, ExecutionBreakdown), BlockExecutionError> {
    let mut breakdown = ExecutionBreakdown::default();
    // Stale accumulators from an earlier failed batch on this thread must not
    // leak into this batch's figures.
    let _ = crate::validation_profile::take();
    if blocks.len() != deployments.len() {
        return Err(BlockExecutionError::DeploymentCount {
            blocks: blocks.len(),
            deployments: deployments.len(),
        });
    }
    if blocks.is_empty() {
        return Ok((Vec::new(), breakdown));
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

    let mut current = chainstate.execution_tip()?;
    let output_count = blocks
        .iter()
        .flat_map(|block| &block.txdata)
        .map(|transaction| transaction.output.len())
        .sum::<usize>();
    let input_outpoints = external_batch_input_outpoints(blocks, transaction_ids, output_count);
    let cumulative = UtxoOverlay::with_capacity(
        chainstate,
        input_outpoints.len().saturating_add(output_count),
    );
    if let Some(prefetched_utxos) = prefetched_utxos {
        if prefetched_utxos.entries.len() != input_outpoints.len()
            || prefetched_utxos
                .entries
                .iter()
                .zip(&input_outpoints)
                .any(|((actual, _), expected)| actual != expected)
        {
            return Err(BlockExecutionError::UtxoPrefetchMismatch);
        }
        cumulative.seed_prefetched(prefetched_utxos.entries);
    } else {
        cumulative.prefetch(&input_outpoints)?;
    }
    let mut applied_blocks = Vec::with_capacity(blocks.len());
    let mut transitions = Vec::with_capacity(blocks.len());
    let mut deferred_scripts = Vec::new();
    let mut script_batch = (blocks.len() > 1).then(DeferredScriptBatch::new);
    let retains_undo = chainstate.retains_block_undo();
    // Two-stage pipeline: block N+1 is validated against block N's delta laid
    // over the batch overlay while a helper thread folds N's delta into that
    // overlay and builds N's transition. Each block still sees exactly the
    // state its predecessor left, and the transitions are collected in order.
    std::thread::scope(|scope| -> Result<(), BlockExecutionError> {
        let mut previous_delta: Option<Arc<PreparedDelta>> = None;
        let mut pending_tail: Option<std::thread::ScopedJoinHandle<'_, PipelineTail>> = None;
        for (block_order, (block, deployment)) in blocks.iter().zip(deployments).enumerate() {
            let validate_started = Instant::now();
            let validated = {
                let view = DeltaView::new(previous_delta.as_deref(), &cumulative);
                prepare_active_block_inner(
                    &view,
                    headers,
                    block,
                    current,
                    now,
                    deployment,
                    structure_prevalidated,
                    transaction_ids.map(|ids| ids[block_order].as_slice()),
                )
            };
            breakdown.validate += validate_started.elapsed();
            let [prepare, utxo, net_change, checks] = crate::validation_profile::take();
            breakdown.validate_prepare += prepare;
            breakdown.validate_utxo += utxo;
            breakdown.validate_net += net_change;
            breakdown.validate_checks += checks;
            // The previous block's tail must land before its delta is
            // replaced, and its outputs go in first so the order holds.
            if let Some(tail) = pending_tail.take() {
                let (transition, applied, [apply_elapsed, net, fold]) =
                    tail.join().expect("pipeline tail thread must not panic")?;
                transitions.push(transition);
                applied_blocks.push(applied);
                breakdown.apply += apply_elapsed;
                breakdown.apply_net += net;
                breakdown.apply_fold += fold;
            }
            let (prepared, delta, mut block_scripts) = match validated {
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
                hash: prepared.block.hash,
            };
            let delta = Arc::new(delta);
            previous_delta = Some(Arc::clone(&delta));
            let cumulative_ref = &cumulative;
            let parent_hash = current.hash;
            pending_tail = Some(scope.spawn(move || {
                let started = Instant::now();
                let changes = delta.net_changes();
                let net_elapsed = started.elapsed();
                let fold_started = Instant::now();
                cumulative_ref.apply_validated_changes(&changes);
                let fold_elapsed = fold_started.elapsed();
                let mut applied = prepared.into_applied(retains_undo);
                let transition = ConnectTransition {
                    expected_parent: parent_hash,
                    next,
                    spent: changes.spent,
                    created: changes.created,
                    transaction_undos: match (retains_undo, applied_undos) {
                        (false, _) => Vec::new(),
                        (true, AppliedUndos::Keep) => applied.transaction_undos.clone(),
                        (true, AppliedUndos::Drop) => {
                            std::mem::take(&mut applied.transaction_undos)
                        }
                    },
                };
                Ok((
                    transition,
                    applied,
                    [started.elapsed(), net_elapsed, fold_elapsed],
                ))
            }));
            let submit_started = Instant::now();
            if let Some(batch) = &mut script_batch {
                batch.submit(block_scripts);
            } else {
                deferred_scripts.extend(block_scripts);
            }
            breakdown.submit += submit_started.elapsed();
            current = next;
        }
        if let Some(tail) = pending_tail.take() {
            let (transition, applied, [apply_elapsed, net, fold]) =
                tail.join().expect("pipeline tail thread must not panic")?;
            transitions.push(transition);
            applied_blocks.push(applied);
            breakdown.apply += apply_elapsed;
            breakdown.apply_net += net;
            breakdown.apply_fold += fold;
        }
        Ok(())
    })?;
    let wait_started = Instant::now();
    let script_failure = if let Some(batch) = script_batch {
        batch.finish()
    } else {
        verify_deferred_scripts(deferred_scripts)
    };
    breakdown.script_wait = wait_started.elapsed();
    if let Some((index, source)) = script_failure {
        return Err(BlockExecutionError::Block(BlockError::Transaction {
            index,
            source: source.into(),
        }));
    }
    let commit_started = Instant::now();
    chainstate.commit_connect_batch_owned(transitions)?;
    breakdown.commit = commit_started.elapsed();
    Ok((applied_blocks, breakdown))
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
    .and_then(|(applied, delta, _)| {
        let net_started = Instant::now();
        let changes = delta.net_changes()?;
        crate::validation_profile::add(crate::validation_profile::NET, net_started.elapsed());
        Ok((applied, changes))
    })
}

/// The pipeline's counterpart of [`validate_active_block_inner`]: the same
/// header, BIP30 and structure checks, then every transaction resolved and
/// checked against a [`BlockPrepareView`] without writing anything.
#[allow(clippy::too_many_arguments)]
fn prepare_active_block_inner<'a, S: UtxoStore>(
    chainstate: &S,
    headers: &HeaderDag,
    block: &'a Block,
    current: ExecutionTip,
    now: u64,
    deployments: &BlockDeploymentContext,
    structure_prevalidated: bool,
    transaction_ids: Option<&[Txid]>,
) -> Result<
    (
        PreparedActiveBlock,
        PreparedDelta,
        Vec<DeferredScriptCheck<'a>>,
    ),
    BlockExecutionError,
> {
    let checks_started = Instant::now();
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
    let capacity = block
        .txdata
        .iter()
        .map(|transaction| transaction.input.len() + transaction.output.len())
        .sum();
    let view = BlockPrepareView::new(chainstate, capacity);
    let exception_undo = prepare_bip30_rules(&view, block, deployments)?;
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
    crate::validation_profile::add(crate::validation_profile::CHECKS, checks_started.elapsed());
    let computed_ids;
    let transaction_ids = if let Some(ids) = transaction_ids {
        ids
    } else {
        computed_ids = block
            .txdata
            .iter()
            .map(bitcoin::Transaction::compute_txid)
            .collect::<Vec<_>>();
        computed_ids.as_slice()
    };
    let (prepared, scripts) = prepare_prevalidated_block_with_deferred_scripts(
        &view,
        block,
        transaction_ids,
        next_height,
        now,
        parent_mtp,
        deployments.script_flags,
        deployments.csv_active,
        deployments.subsidy_sats,
        |transaction| view.record(transaction),
    )
    .map_err(BlockExecutionError::Block)?;
    Ok((
        PreparedActiveBlock {
            block: prepared,
            exception_undo,
        },
        view.into_delta(),
        scripts,
    ))
}

/// [`apply_bip30_rules`] for a prepare view: the overwritten coins are
/// recorded as spent in the view and returned as the exception undo.
fn prepare_bip30_rules<S: UtxoStore>(
    view: &BlockPrepareView<'_, S>,
    block: &Block,
    deployments: &BlockDeploymentContext,
) -> Result<Option<UtxoUndo>, BlockExecutionError> {
    if !deployments.bip30_enforced && !deployments.bip30_overwrite {
        return Ok(None);
    }
    let collisions = block_output_collisions(view, block)?;
    if collisions.is_empty() {
        return Ok(None);
    }
    if !deployments.bip30_overwrite {
        return Err(BlockExecutionError::Bip30Collision(collisions[0]));
    }
    let mut overwritten = Vec::with_capacity(collisions.len());
    for outpoint in &collisions {
        let utxo = view.get(*outpoint)?.ok_or(UtxoError::Missing(*outpoint))?;
        overwritten.push((*outpoint, utxo));
    }
    view.mark_spent(&collisions);
    Ok(Some(UtxoUndo::from_parts(overwritten, Vec::new())))
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
) -> Result<(AppliedBlock, BlockDelta, Vec<DeferredScriptCheck<'a>>), BlockExecutionError> {
    let checks_started = Instant::now();
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
    crate::validation_profile::add(crate::validation_profile::CHECKS, checks_started.elapsed());
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
    Ok((applied, overlay.into_delta(), scripts))
}

/// Per-overlay view of the coins a block or batch touched.
///
/// `original` caches every coin read from the base exactly as it was first
/// seen; `current` holds only keys this overlay modified. A read therefore
/// costs one clone and one insert on first touch instead of two, and the net
/// change of the overlay is the set of `current` keys whose value differs
/// from `original`.
#[derive(Default)]
struct OverlayState {
    original: AHashMap<OutPointKey, Option<Utxo>>,
    current: AHashMap<OutPointKey, Option<Utxo>>,
}

impl OverlayState {
    /// The overlay's view of a key, without touching the base.
    fn cached(&self, outpoint: &OutPointKey) -> Option<&Option<Utxo>> {
        self.current
            .get(outpoint)
            .or_else(|| self.original.get(outpoint))
    }
}

struct UtxoChanges {
    spent: Vec<OutPointKey>,
    created: Vec<(OutPointKey, Utxo)>,
    undo: UtxoUndo,
}

/// Independently locked shards per overlay.
///
/// A batch overlay is read by the block being validated while the block
/// before it is folded in and while prefetched coins are seeded; one mutex
/// around a multi-million-entry map would serialize all of that. Keys route
/// by their first txid bytes, which are uniformly random.
const OVERLAY_SHARDS: usize = 64;

/// Below this many prefetched coins, seeding on the calling thread is cheaper
/// than fanning out.
const PARALLEL_SEED_THRESHOLD: usize = 65_536;

/// Keys folded into a batch-overlay shard per lock acquisition.
const FOLD_BURST: usize = 64;
/// Threads used to seed a large prefetch; each owns a contiguous shard range.
const SEED_WORKERS: usize = 8;

fn overlay_shard(outpoint: &OutPointKey) -> usize {
    let bytes = outpoint.as_bytes();
    let mut word = [0_u8; 8];
    word.copy_from_slice(&bytes[..8]);
    usize::try_from(u64::from_le_bytes(word) % OVERLAY_SHARDS as u64).expect("shard index fits")
}

/// The previous block's net delta laid over the batch overlay.
///
/// While the helper thread folds that delta into the batch overlay, the next
/// block reads it from here instead, so it never waits for the fold and never
/// sees a half-applied state: a key in the delta answers from the delta, any
/// other key is untouched by the fold and answers from the batch overlay.
struct DeltaView<'a, S> {
    delta: Option<&'a PreparedDelta>,
    base: &'a S,
}

/// What the delta knows about one outpoint.
enum DeltaAnswer {
    Coin(Utxo),
    Spent,
    Unknown,
}

impl<'a, S: UtxoStore> DeltaView<'a, S> {
    fn new(delta: Option<&'a PreparedDelta>, base: &'a S) -> Self {
        Self { delta, base }
    }

    fn lookup(&self, outpoint: &OutPointKey) -> DeltaAnswer {
        self.delta
            .map_or(DeltaAnswer::Unknown, |delta| delta.lookup(outpoint))
    }
}

/// What one block did to the coins it touched, recorded while it was
/// prepared: the coins it created and the keys it spent (a coin created and
/// spent inside the block is in both).
///
/// Built on the validation thread as the block's transactions resolve, read
/// by the next block through a [`DeltaView`] and turned into the block's net
/// change on the pipeline tail — so neither the next block nor the tail
/// waits for a write.
pub(crate) struct PreparedDelta {
    /// The block's final word on every key it touched, updated in transaction
    /// order: `Some` is a coin the block leaves behind, `None` a key it spent.
    state: AHashMap<OutPointKey, Option<Utxo>>,
    /// Keys the block spent (or overwrote under BIP30) that existed before it,
    /// as opposed to coins it created and spent itself.
    spent_from_base: AHashSet<OutPointKey>,
}

impl PreparedDelta {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            state: AHashMap::with_capacity(capacity),
            spent_from_base: AHashSet::with_capacity(capacity),
        }
    }

    fn lookup(&self, outpoint: &OutPointKey) -> DeltaAnswer {
        match self.state.get(outpoint) {
            Some(Some(utxo)) => DeltaAnswer::Coin(utxo.clone()),
            Some(None) => DeltaAnswer::Spent,
            None => DeltaAnswer::Unknown,
        }
    }

    fn spend(&mut self, outpoint: OutPointKey) {
        if self.state.insert(outpoint, None).is_none() {
            self.spent_from_base.insert(outpoint);
        }
    }

    fn record(&mut self, prepared: &PreparedTransaction) {
        for outpoint in &prepared.spent {
            self.spend(*outpoint);
        }
        for (outpoint, utxo) in &prepared.created {
            self.state.insert(*outpoint, Some(utxo.clone()));
        }
    }

    /// The block's net effect: coins it leaves behind, keys it removed from
    /// the state it started from. A BIP30 overwrite puts a key in both, the
    /// removal first. Sorted like the overlay's `net_changes`.
    fn net_changes(&self) -> UtxoChanges {
        let mut spent: Vec<OutPointKey> = self.spent_from_base.iter().copied().collect();
        let mut created: Vec<(OutPointKey, Utxo)> = self
            .state
            .iter()
            .filter_map(|(outpoint, value)| value.as_ref().map(|utxo| (*outpoint, utxo.clone())))
            .collect();
        spent.sort_unstable();
        created.sort_unstable_by_key(|(outpoint, _)| *outpoint);
        // The pipeline carries undo per transaction in `AppliedBlock`; the
        // block-level record is only built on the unpipelined path.
        UtxoChanges {
            spent,
            created,
            undo: UtxoUndo::from_parts(Vec::new(), Vec::new()),
        }
    }
}

/// The read view a block is prepared against: what the block itself has
/// resolved so far, over the state it starts from.
struct BlockPrepareView<'a, S> {
    base: &'a S,
    delta: Mutex<PreparedDelta>,
}

impl<'a, S: UtxoStore> BlockPrepareView<'a, S> {
    fn new(base: &'a S, capacity: usize) -> Self {
        Self {
            base,
            delta: Mutex::new(PreparedDelta::with_capacity(capacity)),
        }
    }

    fn delta(&self) -> std::sync::MutexGuard<'_, PreparedDelta> {
        self.delta.lock().expect("prepare view lock not poisoned")
    }

    fn record(&self, prepared: &PreparedTransaction) {
        self.delta().record(prepared);
    }

    /// Removes coins the block overwrites under the BIP30 exception.
    fn mark_spent(&self, outpoints: &[OutPointKey]) {
        let mut delta = self.delta();
        for outpoint in outpoints {
            delta.spend(*outpoint);
        }
    }

    fn into_delta(self) -> PreparedDelta {
        self.delta
            .into_inner()
            .expect("prepare view lock not poisoned")
    }
}

impl<S: UtxoStore> UtxoStore for BlockPrepareView<'_, S> {
    fn get(&self, outpoint: OutPointKey) -> Result<Option<Utxo>, UtxoError> {
        match self.delta().lookup(&outpoint) {
            DeltaAnswer::Coin(utxo) => Ok(Some(utxo)),
            DeltaAnswer::Spent => Ok(None),
            DeltaAnswer::Unknown => self.base.get(outpoint),
        }
    }

    fn get_many(
        &self,
        outpoints: &[OutPointKey],
    ) -> Result<Vec<(OutPointKey, Option<Utxo>)>, UtxoError> {
        let mut results = Vec::with_capacity(outpoints.len());
        let mut wanted = Vec::new();
        let mut positions = Vec::new();
        {
            let delta = self.delta();
            for outpoint in outpoints {
                match delta.lookup(outpoint) {
                    DeltaAnswer::Coin(utxo) => results.push((*outpoint, Some(utxo))),
                    DeltaAnswer::Spent => results.push((*outpoint, None)),
                    DeltaAnswer::Unknown => {
                        positions.push(results.len());
                        wanted.push(*outpoint);
                        results.push((*outpoint, None));
                    }
                }
            }
        }
        if !wanted.is_empty() {
            let resolved = self.base.get_many(&wanted)?;
            for (position, (_, utxo)) in positions.into_iter().zip(resolved) {
                results[position].1 = utxo;
            }
        }
        Ok(results)
    }

    fn apply(&self, _: &[OutPointKey], _: &[(OutPointKey, Utxo)]) -> Result<(), UtxoError> {
        Err(UtxoError::Malformed("prepare view is read-only"))
    }

    fn apply_with_undo(
        &self,
        _: &[OutPointKey],
        _: &[(OutPointKey, Utxo)],
    ) -> Result<UtxoUndo, UtxoError> {
        Err(UtxoError::Malformed("prepare view is read-only"))
    }

    fn undo(&self, _: &UtxoUndo, _: u64, _: u64) -> Result<(), UtxoError> {
        Err(UtxoError::Malformed("prepare view is read-only"))
    }

    fn age_to_cold(&self, _: u64, _: u64) -> Result<u64, UtxoError> {
        Ok(0)
    }

    fn snapshot_entries(&self) -> Result<BTreeMap<OutPointKey, Utxo>, UtxoError> {
        Err(UtxoError::Malformed("prepare view is read-only"))
    }

    fn replace_all(
        &self,
        _: &BTreeMap<OutPointKey, Utxo>,
        _: u64,
        _: u64,
    ) -> Result<(), UtxoError> {
        Err(UtxoError::Malformed("prepare view is read-only"))
    }

    fn tier_stats(&self) -> Result<TierStats, UtxoError> {
        self.base.tier_stats()
    }
}

/// A prepared block plus the BIP30 exception undo, ready for the tail.
struct PreparedActiveBlock {
    block: PreparedBlock,
    exception_undo: Option<UtxoUndo>,
}

impl PreparedActiveBlock {
    /// The `AppliedBlock` applying this block would have produced; undo
    /// records are built from the prepared coins only when they are kept.
    fn into_applied(self, retains_undo: bool) -> AppliedBlock {
        let hash = self.block.hash;
        let mut transaction_undos = Vec::new();
        if retains_undo {
            transaction_undos.reserve(self.block.transactions.len() + 1);
            if let Some(undo) = self.exception_undo {
                transaction_undos.push(undo);
            }
            for prepared in self.block.transactions {
                let spent = prepared.spent.into_iter().zip(prepared.prevouts).collect();
                let created = prepared
                    .created
                    .into_iter()
                    .map(|(outpoint, _)| outpoint)
                    .collect();
                transaction_undos.push(UtxoUndo::from_parts(spent, created));
            }
        }
        AppliedBlock {
            hash,
            transaction_undos,
        }
    }
}

impl<S: UtxoStore> UtxoStore for DeltaView<'_, S> {
    fn get(&self, outpoint: OutPointKey) -> Result<Option<Utxo>, UtxoError> {
        match self.lookup(&outpoint) {
            DeltaAnswer::Coin(utxo) => Ok(Some(utxo)),
            DeltaAnswer::Spent => Ok(None),
            DeltaAnswer::Unknown => self.base.get(outpoint),
        }
    }

    fn get_many(
        &self,
        outpoints: &[OutPointKey],
    ) -> Result<Vec<(OutPointKey, Option<Utxo>)>, UtxoError> {
        let mut results = Vec::with_capacity(outpoints.len());
        let mut wanted = Vec::new();
        let mut positions = Vec::new();
        for outpoint in outpoints {
            match self.lookup(outpoint) {
                DeltaAnswer::Coin(utxo) => results.push((*outpoint, Some(utxo))),
                DeltaAnswer::Spent => results.push((*outpoint, None)),
                DeltaAnswer::Unknown => {
                    positions.push(results.len());
                    wanted.push(*outpoint);
                    results.push((*outpoint, None));
                }
            }
        }
        if !wanted.is_empty() {
            let resolved = self.base.get_many(&wanted)?;
            for (position, (_, utxo)) in positions.into_iter().zip(resolved) {
                results[position].1 = utxo;
            }
        }
        Ok(results)
    }

    fn apply(&self, _: &[OutPointKey], _: &[(OutPointKey, Utxo)]) -> Result<(), UtxoError> {
        Err(UtxoError::Malformed("delta view is read-only"))
    }

    fn apply_with_undo(
        &self,
        _: &[OutPointKey],
        _: &[(OutPointKey, Utxo)],
    ) -> Result<UtxoUndo, UtxoError> {
        Err(UtxoError::Malformed("delta view is read-only"))
    }

    fn undo(&self, _: &UtxoUndo, _: u64, _: u64) -> Result<(), UtxoError> {
        Err(UtxoError::Malformed("delta view is read-only"))
    }

    fn age_to_cold(&self, _: u64, _: u64) -> Result<u64, UtxoError> {
        Ok(0)
    }

    fn snapshot_entries(&self) -> Result<BTreeMap<OutPointKey, Utxo>, UtxoError> {
        Err(UtxoError::Malformed("delta view is read-only"))
    }

    fn replace_all(
        &self,
        _: &BTreeMap<OutPointKey, Utxo>,
        _: u64,
        _: u64,
    ) -> Result<(), UtxoError> {
        Err(UtxoError::Malformed("delta view is read-only"))
    }

    fn tier_stats(&self) -> Result<TierStats, UtxoError> {
        self.base.tier_stats()
    }
}

/// What the pipeline tail hands back for one block.
type PipelineTail = Result<(ConnectTransition, AppliedBlock, [Duration; 3]), UtxoError>;

/// A validated block's overlay shards, detached from their base.
///
/// Keeps everything the block read or wrote, so it serves two purposes at
/// once: the next block reads through it while the batch overlay is still
/// being updated, and the pipeline tail derives the block's net changes from
/// it off the validation thread.
struct BlockDelta {
    shards: Vec<Mutex<OverlayState>>,
}

impl BlockDelta {
    fn net_changes(&self) -> Result<UtxoChanges, UtxoError> {
        net_changes_of(&self.shards)
    }
}

fn net_changes_of(shards: &[Mutex<OverlayState>]) -> Result<UtxoChanges, UtxoError> {
    let mut spent = Vec::new();
    let mut created = Vec::new();
    let mut undo_spent = Vec::new();
    for shard in shards {
        let state = shard.lock().expect("overlay lock not poisoned");
        for (outpoint, current) in &state.current {
            let original = state
                .original
                .get(outpoint)
                .ok_or(UtxoError::Malformed("overlay original value"))?;
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

/// Block-scoped UTXO mutations retained in memory until validation succeeds.
struct UtxoOverlay<'a, S> {
    base: &'a S,
    shards: Vec<Mutex<OverlayState>>,
}

impl<'a, S: UtxoStore> UtxoOverlay<'a, S> {
    fn new(base: &'a S) -> Self {
        Self::with_capacity(base, 0)
    }

    fn with_capacity(base: &'a S, capacity: usize) -> Self {
        let per_shard = capacity.div_ceil(OVERLAY_SHARDS);
        Self {
            base,
            shards: (0..OVERLAY_SHARDS)
                .map(|_| {
                    Mutex::new(OverlayState {
                        original: AHashMap::with_capacity(per_shard),
                        current: AHashMap::with_capacity(per_shard),
                    })
                })
                .collect(),
        }
    }

    fn shard(&self, outpoint: &OutPointKey) -> std::sync::MutexGuard<'_, OverlayState> {
        self.shards[overlay_shard(outpoint)]
            .lock()
            .expect("overlay lock not poisoned")
    }

    /// Reads a key through the shard the caller already holds.
    fn load_in(
        &self,
        state: &mut OverlayState,
        outpoint: OutPointKey,
    ) -> Result<Option<Utxo>, UtxoError> {
        if let Some(value) = state.cached(&outpoint) {
            return Ok(value.clone());
        }
        let value = self.base.get(outpoint)?;
        state.original.insert(outpoint, value.clone());
        Ok(value)
    }

    fn load(&self, outpoint: OutPointKey) -> Result<Option<Utxo>, UtxoError> {
        let mut state = self.shard(&outpoint);
        self.load_in(&mut state, outpoint)
    }

    /// The overlay's cached view of a key, without touching the base:
    /// `None` when the key was never touched, `Some(None)` when it is known
    /// absent, `Some(Some(_))` when it is known present.
    #[allow(clippy::option_option)]
    fn cached_value(&self, outpoint: &OutPointKey) -> Option<Option<Utxo>> {
        self.shard(outpoint).cached(outpoint).cloned()
    }

    fn net_changes(&self) -> Result<UtxoChanges, UtxoError> {
        net_changes_of(&self.shards)
    }

    /// Hand over the shards so the block's delta outlives the overlay.
    fn into_delta(self) -> BlockDelta {
        BlockDelta {
            shards: self.shards,
        }
    }

    /// Folds a block's net change in. The batch overlay's own net change is
    /// never read on the pipelined path — the blocks carry their transitions
    /// — so only `current` is maintained: reads consult it first, and a key
    /// the block wrote needs no `original` entry to be answered correctly.
    fn apply_validated_changes(&self, changes: &UtxoChanges) {
        // Group the keys by shard first so each shard is locked once per
        // block instead of once per key; the tail thread and the validation
        // thread share these locks, so fewer acquisitions help both.
        let mut spent_by_shard: Vec<Vec<OutPointKey>> = vec![Vec::new(); OVERLAY_SHARDS];
        for outpoint in &changes.spent {
            spent_by_shard[overlay_shard(outpoint)].push(*outpoint);
        }
        let mut created_by_shard: Vec<Vec<&(OutPointKey, Utxo)>> = vec![Vec::new(); OVERLAY_SHARDS];
        for entry in &changes.created {
            created_by_shard[overlay_shard(&entry.0)].push(entry);
        }
        // Each shard is taken in short bursts: a reader on the validation
        // thread that needs the same shard waits for a burst, not for the
        // whole block's worth of inserts.
        for (shard, (spent, created)) in self
            .shards
            .iter()
            .zip(spent_by_shard.into_iter().zip(created_by_shard))
        {
            for burst in spent.chunks(FOLD_BURST) {
                let mut state = shard.lock().expect("overlay lock not poisoned");
                for outpoint in burst {
                    state.current.insert(*outpoint, None);
                }
            }
            for burst in created.chunks(FOLD_BURST) {
                let mut state = shard.lock().expect("overlay lock not poisoned");
                for (outpoint, utxo) in burst {
                    state.current.insert(*outpoint, Some(utxo.clone()));
                }
            }
        }
    }

    fn prefetch(&self, outpoints: &[OutPointKey]) -> Result<(), UtxoError> {
        let prefetched = self.base.get_many(outpoints)?;
        self.seed_prefetched(prefetched);
        Ok(())
    }

    /// Seeds the read cache with coins fetched ahead of validation.
    ///
    /// A batch's inputs run to millions of coins; they are partitioned by
    /// shard once and inserted by a few threads that each own a shard range,
    /// so no two threads ever contend for a lock.
    fn seed_prefetched(&self, prefetched: Vec<(OutPointKey, Option<Utxo>)>) {
        if prefetched.len() < PARALLEL_SEED_THRESHOLD {
            for (outpoint, value) in prefetched {
                self.shard(&outpoint).original.insert(outpoint, value);
            }
            return;
        }
        let mut partitioned: Vec<Vec<(OutPointKey, Option<Utxo>)>> =
            (0..OVERLAY_SHARDS).map(|_| Vec::new()).collect();
        for (outpoint, value) in prefetched {
            partitioned[overlay_shard(&outpoint)].push((outpoint, value));
        }
        let shards_per_worker = OVERLAY_SHARDS.div_ceil(SEED_WORKERS);
        std::thread::scope(|scope| {
            for (worker, chunk) in partitioned.chunks_mut(shards_per_worker).enumerate() {
                let first_shard = worker * shards_per_worker;
                let shards = &self.shards;
                scope.spawn(move || {
                    for (offset, entries) in chunk.iter_mut().enumerate() {
                        if entries.is_empty() {
                            continue;
                        }
                        let mut state = shards[first_shard + offset]
                            .lock()
                            .expect("overlay lock not poisoned");
                        state.original.reserve(entries.len());
                        for (outpoint, value) in entries.drain(..) {
                            state.original.insert(outpoint, value);
                        }
                    }
                });
            }
        });
    }

    /// Records a spend in its shard.
    fn write_spent(&self, outpoint: &OutPointKey) {
        let mut state = self.shard(outpoint);
        state.original.entry(*outpoint).or_insert(None);
        state.current.insert(*outpoint, None);
    }

    /// Records a creation in its shard.
    fn write_created(&self, outpoint: &OutPointKey, utxo: &Utxo) {
        let mut state = self.shard(outpoint);
        state.original.entry(*outpoint).or_insert(None);
        state.current.insert(*outpoint, Some(utxo.clone()));
    }
}

impl<S: UtxoStore> UtxoStore for UtxoOverlay<'_, S> {
    fn get(&self, outpoint: OutPointKey) -> Result<Option<Utxo>, UtxoError> {
        self.load(outpoint)
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
        let mut seen_spent = BTreeSet::new();
        let mut undo_spent = Vec::with_capacity(spent.len());
        for outpoint in spent {
            if !seen_spent.insert(*outpoint) {
                return Err(UtxoError::DuplicateSpend(*outpoint));
            }
            let previous = self.load(*outpoint)?.ok_or(UtxoError::Missing(*outpoint))?;
            undo_spent.push((*outpoint, previous));
        }
        let mut seen_created = BTreeSet::new();
        for (outpoint, _) in created {
            if !seen_created.insert(*outpoint)
                || (!seen_spent.contains(outpoint) && self.load(*outpoint)?.is_some())
            {
                return Err(UtxoError::Duplicate(*outpoint));
            }
        }
        for outpoint in spent {
            self.write_spent(outpoint);
        }
        for (outpoint, utxo) in created {
            self.write_created(outpoint, utxo);
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
        let mut seen_spent = BTreeSet::new();
        let mut undo_spent = Vec::with_capacity(spent.len());
        for outpoint in spent {
            if !seen_spent.insert(*outpoint) {
                return Err(UtxoError::DuplicateSpend(*outpoint));
            }
            let previous = self.load(*outpoint)?.ok_or(UtxoError::Missing(*outpoint))?;
            undo_spent.push((*outpoint, previous));
        }
        let mut seen_created = BTreeSet::new();
        for (outpoint, _) in created {
            if !seen_created.insert(*outpoint)
                || (!seen_spent.contains(outpoint)
                    && self
                        .cached_value(outpoint)
                        .is_some_and(|value| value.is_some()))
            {
                return Err(UtxoError::Duplicate(*outpoint));
            }
            // The caller's contract is that every created outpoint was already
            // proved absent under its enclosing consensus rules, which is what
            // lets this path skip the durable probe that `apply_with_undo`
            // performs. `apply_bip30_rules` only performs that proof when BIP30
            // is enforced or an exception applies, so above the BIP34 anchor the
            // guarantee rests on txid uniqueness alone. Re-check it in debug
            // builds so the invariant cannot silently weaken; a release build
            // still fails closed at commit, where
            // `apply_validated_changes_transaction` rejects the collision.
            if cfg!(debug_assertions) && !seen_spent.contains(outpoint) {
                assert!(
                    self.load(*outpoint)?.is_none(),
                    "fresh-output fast path was given an outpoint that already exists: {outpoint}"
                );
            }
        }
        for outpoint in spent {
            self.write_spent(outpoint);
        }
        for (outpoint, utxo) in created {
            self.write_created(outpoint, utxo);
        }
        Ok(UtxoUndo::new(
            undo_spent,
            created.iter().map(|(outpoint, _)| *outpoint).collect(),
        ))
    }

    fn apply_with_undo_fresh_outputs_from_prevouts(
        &self,
        spent: &[OutPointKey],
        prevouts: &[Utxo],
        created: &[(OutPointKey, Utxo)],
    ) -> Result<UtxoUndo, UtxoError> {
        if prevouts.len() != spent.len() {
            return Err(UtxoError::Malformed(
                "prevout count does not match spent count",
            ));
        }
        let mut seen_spent = BTreeSet::new();
        let mut undo_spent = Vec::with_capacity(spent.len());
        for (outpoint, prevout) in spent.iter().zip(prevouts) {
            if !seen_spent.insert(*outpoint) {
                return Err(UtxoError::DuplicateSpend(*outpoint));
            }
            // The caller read this coin through `get` moments ago, so the
            // overlay already caches its original; in a debug build make
            // sure the handed-in value is that coin.
            let cached = self.cached_value(outpoint);
            debug_assert_eq!(
                cached.clone().flatten().as_ref(),
                Some(prevout),
                "prevout handed to the overlay differs from its cached coin: {outpoint}"
            );
            if cached.is_none_or(|value| value.is_none()) {
                return Err(UtxoError::Missing(*outpoint));
            }
            undo_spent.push((*outpoint, prevout.clone()));
        }
        let mut seen_created = BTreeSet::new();
        for (outpoint, _) in created {
            if !seen_created.insert(*outpoint)
                || (!seen_spent.contains(outpoint)
                    && self
                        .cached_value(outpoint)
                        .is_some_and(|value| value.is_some()))
            {
                return Err(UtxoError::Duplicate(*outpoint));
            }
        }
        for outpoint in spent {
            self.write_spent(outpoint);
        }
        for (outpoint, utxo) in created {
            self.write_created(outpoint, utxo);
        }
        Ok(UtxoUndo::new(
            undo_spent,
            created.iter().map(|(outpoint, _)| *outpoint).collect(),
        ))
    }

    fn undo(&self, undo: &UtxoUndo, _now: u64, _hot_window_secs: u64) -> Result<(), UtxoError> {
        for (outpoint, _) in undo.spent() {
            if self.load(*outpoint)?.is_some() {
                return Err(UtxoError::Duplicate(*outpoint));
            }
        }
        for outpoint in undo.created() {
            self.load(*outpoint)?;
            self.write_spent(outpoint);
        }
        for (outpoint, utxo) in undo.spent() {
            self.write_created(outpoint, utxo);
        }
        Ok(())
    }

    fn age_to_cold(&self, _now: u64, _hot_window_secs: u64) -> Result<u64, UtxoError> {
        Ok(0)
    }

    fn snapshot_entries(&self) -> Result<BTreeMap<OutPointKey, Utxo>, UtxoError> {
        let mut entries = self.base.snapshot_entries()?;
        for shard in &self.shards {
            let state = shard.lock().expect("overlay lock not poisoned");
            for (outpoint, current) in &state.current {
                if let Some(utxo) = current {
                    entries.insert(*outpoint, utxo.clone());
                } else {
                    entries.remove(outpoint);
                }
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
        for shard in &self.shards {
            let mut state = shard.lock().expect("overlay lock not poisoned");
            state.original.clear();
            state.current.clear();
        }
        for outpoint in base.keys().chain(entries.keys()) {
            let mut state = self.shard(outpoint);
            state
                .original
                .insert(*outpoint, base.get(outpoint).cloned());
            state
                .current
                .insert(*outpoint, entries.get(outpoint).cloned());
        }
        Ok(())
    }

    /// Counts the overlay's population without materializing the UTXO set.
    ///
    /// Only outpoints this overlay has touched can differ from the base, so the
    /// base count plus the overlay's net delta is exact. Materializing instead
    /// would pull the whole chainstate into memory for a single number.
    fn tier_stats(&self) -> Result<TierStats, UtxoError> {
        let base = self.base.tier_stats()?;
        let total = base
            .hot
            .checked_add(base.cold)
            .ok_or(UtxoError::Malformed("base entry count"))?;
        let mut delta = 0_i64;
        for shard in &self.shards {
            let state = shard.lock().expect("overlay lock not poisoned");
            for (outpoint, current) in &state.current {
                let present_before = match state.original.get(outpoint) {
                    Some(original) => original.is_some(),
                    // Reached only for an outpoint written straight into
                    // `current` without a recorded pre-image; bounded by the
                    // overlay size.
                    None => self.base.get(*outpoint)?.is_some(),
                };
                delta += i64::from(current.is_some()) - i64::from(present_before);
            }
        }
        let hot = i64::try_from(total)
            .ok()
            .and_then(|total| total.checked_add(delta))
            .and_then(|total| u64::try_from(total).ok())
            .ok_or(UtxoError::Malformed("overlay entry count"))?;
        Ok(TierStats { hot, cold: 0 })
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
pub fn disconnect_execution_tip<C: ExecutionChainStore>(
    chainstate: &C,
    headers: &HeaderDag,
    now: u64,
    hot_window_secs: u64,
) -> Result<ExecutionTip, BlockExecutionError> {
    let current = chainstate.execution_tip()?;
    if current.height == 0 {
        return Err(BlockExecutionError::DisconnectGenesis);
    }
    if chainstate.assumed_snapshot_base()? == Some(current) {
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
        .block_undo(current.hash)?
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
    chainstate.commit_disconnect(
        current,
        parent_tip,
        &transition.spent,
        &transition.created,
        &applied.transaction_undos,
    )?;
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
        blockchain::{
            BlockError, block_subsidy, validate_block_structure_with_deployments_and_txids,
        },
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

    #[test]
    fn prevalidated_batch_prefetch_omits_outputs_created_earlier_in_batch() {
        let external = OutPoint::new(Txid::from_byte_array([71; 32]), 0);
        let parent = Transaction {
            version: Version::ONE,
            lock_time: LockTime::ZERO,
            input: vec![TxIn {
                previous_output: external,
                script_sig: ScriptBuf::new(),
                sequence: Sequence::MAX,
                witness: Witness::default(),
            }],
            output: vec![TxOut {
                value: Amount::from_sat(900),
                script_pubkey: ScriptBuf::new(),
            }],
        };
        let child = Transaction {
            version: Version::ONE,
            lock_time: LockTime::ZERO,
            input: vec![TxIn {
                previous_output: OutPoint::new(parent.compute_txid(), 0),
                script_sig: ScriptBuf::new(),
                sequence: Sequence::MAX,
                witness: Witness::default(),
            }],
            output: vec![TxOut {
                value: Amount::from_sat(800),
                script_pubkey: ScriptBuf::new(),
            }],
        };
        let block =
            block_with_transactions(BlockHash::all_zeros(), 1, vec![coinbase(1), parent, child]);
        let transaction_ids =
            validate_block_structure_with_deployments_and_txids(&block, 1, false, false, None)
                .unwrap();
        assert_eq!(
            external_batch_input_outpoints(
                std::slice::from_ref(&block),
                Some(std::slice::from_ref(&transaction_ids)),
                3,
            ),
            vec![external.into()]
        );
    }

    #[test]
    fn prepared_utxos_connect_exact_batch_and_reject_mismatch() {
        let directory = TempDir::new().unwrap();
        let chainstate =
            RedbChainStore::open(directory.path().join("chainstate.redb"), Network::Regtest)
                .unwrap();
        let previous = OutPoint::new(Txid::from_byte_array([72; 32]), 0);
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
        let transaction_ids = validate_block_structure_with_deployments_and_txids(
            &active_block,
            1,
            false,
            false,
            None,
        )
        .unwrap();
        let blocks = [active_block.clone()];
        let transaction_ids = [transaction_ids];
        let contexts = [deployments(1)];

        let mut mismatched =
            prefetch_prevalidated_active_block_utxos(&chainstate, &blocks, &transaction_ids)
                .unwrap();
        assert_eq!(mismatched.entries.len(), 1);
        mismatched.entries.clear();
        assert!(matches!(
            connect_prevalidated_active_blocks_with_txids_and_utxos(
                &chainstate,
                &headers,
                &blocks,
                &transaction_ids,
                mismatched,
                1,
                60,
                &contexts,
            ),
            Err(BlockExecutionError::UtxoPrefetchMismatch)
        ));
        assert_eq!(chainstate.execution().tip().unwrap().height, 0);

        let prefetched =
            prefetch_prevalidated_active_block_utxos(&chainstate, &blocks, &transaction_ids)
                .unwrap();
        connect_prevalidated_active_blocks_with_txids_and_utxos(
            &chainstate,
            &headers,
            &blocks,
            &transaction_ids,
            prefetched,
            1,
            60,
            &contexts,
        )
        .unwrap();
        assert_eq!(chainstate.execution().tip().unwrap().height, 1);
        assert!(chainstate.get(previous.into()).unwrap().is_none());
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
        block_deployment_context(Network::Regtest, height, BlockHash::all_zeros())
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
    fn write_ahead_recovery_accepts_a_transition_without_a_net_utxo_effect() {
        let directory = TempDir::new().unwrap();
        let chainstate = RedbUtxoStore::open(directory.path().join("chainstate.redb")).unwrap();
        let undo_store = RedbUndoStore::open(directory.path().join("undo.redb")).unwrap();
        let execution_store =
            RedbExecutionStore::open(directory.path().join("execution.redb"), Network::Regtest)
                .unwrap();
        let parent = execution_store.tip().unwrap();
        let next = ExecutionTip {
            height: 1,
            hash: BlockHash::from_byte_array([8; 32]),
        };
        let empty_undo = UtxoUndo::new(Vec::new(), Vec::new());
        let mut pending = PendingTransition {
            kind: TransitionKind::Connect,
            parent,
            next,
            undo: empty_undo.clone(),
            created: Vec::new(),
        };

        // A crash before execution-tip publication observes both the pre- and
        // post-UTXO state because the transition has no coin effect.
        undo_store.prepare_transition(&pending).unwrap();
        assert!(
            recover_pending_transition(&chainstate, &undo_store, &execution_store, 1, 60).unwrap()
        );
        assert_eq!(execution_store.tip().unwrap(), parent);
        assert!(undo_store.pending_transition().unwrap().is_none());

        // The same ambiguous UTXO observation must also be accepted after the
        // execution tip and empty undo have become durable.
        undo_store.prepare_transition(&pending).unwrap();
        undo_store
            .insert(next.hash, std::slice::from_ref(&empty_undo))
            .unwrap();
        execution_store.advance(parent.hash, next).unwrap();
        assert!(
            recover_pending_transition(&chainstate, &undo_store, &execution_store, 1, 60).unwrap()
        );
        assert_eq!(execution_store.tip().unwrap(), next);
        assert!(undo_store.get(next.hash).unwrap().is_some());
        assert!(undo_store.pending_transition().unwrap().is_none());

        pending.kind = TransitionKind::Disconnect;
        undo_store.prepare_transition(&pending).unwrap();
        assert!(
            recover_pending_transition(&chainstate, &undo_store, &execution_store, 1, 60).unwrap()
        );
        assert_eq!(execution_store.tip().unwrap(), parent);
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
        let context = block_deployment_context(Network::Signet, 1, block.block_hash());
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
        let mut context = block_deployment_context(Network::Signet, 1, block.block_hash());
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
