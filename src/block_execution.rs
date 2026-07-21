//! Sequential active-chain block execution and durable progress coordination.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Mutex,
};

use bitcoin::{Block, BlockHash, OutPoint};
use thiserror::Error;

use crate::{
    blockchain::{AppliedBlock, BlockError, apply_block_with_deployments, disconnect_block},
    execution_store::{ExecutionStoreError, ExecutionTip, RedbExecutionStore},
    headers::HeaderDag,
    undo_store::{PendingTransition, RedbUndoStore, TransitionKind, UndoStoreError},
    utxo::{OutPointKey, TierStats, Utxo, UtxoError, UtxoStore, UtxoUndo},
};

/// Consensus deployments selected for a candidate block.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BlockDeploymentContext {
    /// Explicit libbitcoinconsensus script verification flags.
    pub script_flags: u32,
    /// Whether BIP34 coinbase-height commitment is active.
    pub bip34_active: bool,
    /// Whether the CSV deployment (BIP68/BIP112/BIP113) is active.
    pub csv_active: bool,
    /// Whether this is one of the two historical mainnet BIP30 exceptions.
    pub bip30_exception: bool,
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
/// UTXO effect is committed in one storage transaction, so a process crash
/// cannot expose a partially applied block. Undo storage and execution-tip
/// advancement remain separate durable transactions; failures observed by the
/// process trigger an immediate UTXO rollback.
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
    recover_pending_transition(
        chainstate,
        undo_store,
        execution_store,
        now,
        hot_window_secs,
    )?;
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
    let overlay = UtxoOverlay::new(chainstate);
    let collisions = block_output_collisions(&overlay, block)?;
    let exception_undo = if collisions.is_empty() {
        None
    } else if deployments.bip30_exception {
        Some(overlay.apply_with_undo(&collisions, &[])?)
    } else {
        return Err(BlockExecutionError::Bip30Collision(collisions[0]));
    };
    let mut applied = match apply_block_with_deployments(
        &overlay,
        block,
        next_height,
        now,
        parent_mtp,
        hot_window_secs,
        deployments.script_flags,
        deployments.bip34_active,
        deployments.csv_active,
        deployments.subsidy_sats,
    ) {
        Ok(applied) => applied,
        Err(error) => return Err(BlockExecutionError::Block(error)),
    };
    if let Some(undo) = exception_undo {
        applied.transaction_undos.insert(0, undo);
    }
    let transition = overlay.net_changes()?;
    let next_tip = ExecutionTip {
        height: next_height,
        hash: applied.hash,
    };
    undo_store.prepare_transition(&PendingTransition {
        kind: TransitionKind::Connect,
        parent: current,
        next: next_tip,
        undo: transition.undo.clone(),
        created: transition.created.clone(),
    })?;
    let committed_undo = match chainstate.apply_with_undo(&transition.spent, &transition.created) {
        Ok(undo) => undo,
        Err(error) => {
            undo_store.clear_transition(applied.hash)?;
            return Err(BlockExecutionError::Rollback(error));
        }
    };
    debug_assert_eq!(committed_undo, transition.undo);

    if let Err(error) = undo_store.insert(applied.hash, &applied.transaction_undos) {
        chainstate.undo(&committed_undo, now, hot_window_secs)?;
        undo_store.clear_transition(applied.hash)?;
        return Err(BlockExecutionError::Undo(error));
    }
    if let Err(error) = execution_store.advance(current.hash, next_tip) {
        undo_store.remove(applied.hash)?;
        chainstate.undo(&committed_undo, now, hot_window_secs)?;
        undo_store.clear_transition(applied.hash)?;
        return Err(BlockExecutionError::Execution(error));
    }
    undo_store.clear_transition(applied.hash)?;
    Ok(applied)
}

#[derive(Default)]
struct OverlayState {
    original: BTreeMap<OutPointKey, Option<Utxo>>,
    current: BTreeMap<OutPointKey, Option<Utxo>>,
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
        Self {
            base,
            state: Mutex::new(OverlayState::default()),
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
        let undo_created = created.iter().map(|(outpoint, _)| *outpoint).collect();
        Ok(UtxoChanges {
            spent,
            created,
            undo: UtxoUndo::new(undo_spent, undo_created),
        })
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
            if !seen_created.insert(*outpoint) || self.load(&mut state, *outpoint)?.is_some() {
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
    recover_pending_transition(
        chainstate,
        undo_store,
        execution_store,
        now,
        hot_window_secs,
    )?;
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
    let parent_tip = ExecutionTip {
        height: parent.height,
        hash: parent.hash,
    };
    let overlay = UtxoOverlay::new(chainstate);
    disconnect_block(&overlay, &applied, now, hot_window_secs)?;
    let transition = overlay.net_changes()?;
    let forward_undo = UtxoUndo::new(transition.created.clone(), transition.spent.clone());
    undo_store.prepare_transition(&PendingTransition {
        kind: TransitionKind::Disconnect,
        parent: parent_tip,
        next: current,
        undo: forward_undo,
        created: transition.undo.spent().to_vec(),
    })?;
    let committed_undo = match chainstate.apply_with_undo(&transition.spent, &transition.created) {
        Ok(undo) => undo,
        Err(error) => {
            undo_store.clear_transition(current.hash)?;
            return Err(BlockExecutionError::Rollback(error));
        }
    };
    if let Err(error) = execution_store.rewind(current, parent_tip) {
        chainstate.undo(&committed_undo, now, hot_window_secs)?;
        undo_store.clear_transition(current.hash)?;
        return Err(BlockExecutionError::Execution(error));
    }
    undo_store.remove(current.hash)?;
    undo_store.clear_transition(current.hash)?;
    Ok(parent_tip)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

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
        deployments::block_deployment_context,
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

    fn block_with_transactions(
        parent: BlockHash,
        time: u32,
        transactions: Vec<Transaction>,
    ) -> Block {
        let mut block = Block {
            header: Header {
                version: HeaderVersion::ONE,
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

    fn deployments(height: u32) -> BlockDeploymentContext {
        block_deployment_context(
            Network::Regtest,
            height,
            BlockHash::all_zeros(),
            u32::MAX,
            true,
        )
    }

    struct CountingStore<'a> {
        inner: &'a RedbUtxoStore,
        writes: AtomicUsize,
    }

    impl UtxoStore for CountingStore<'_> {
        fn get(&self, outpoint: OutPointKey) -> Result<Option<Utxo>, UtxoError> {
            self.inner.get(outpoint)
        }

        fn apply(
            &self,
            spent: &[OutPointKey],
            created: &[(OutPointKey, Utxo)],
        ) -> Result<(), UtxoError> {
            self.writes.fetch_add(1, Ordering::Relaxed);
            self.inner.apply(spent, created)
        }

        fn apply_with_undo(
            &self,
            spent: &[OutPointKey],
            created: &[(OutPointKey, Utxo)],
        ) -> Result<UtxoUndo, UtxoError> {
            self.writes.fetch_add(1, Ordering::Relaxed);
            self.inner.apply_with_undo(spent, created)
        }

        fn undo(&self, undo: &UtxoUndo, now: u64, hot_window_secs: u64) -> Result<(), UtxoError> {
            self.writes.fetch_add(1, Ordering::Relaxed);
            self.inner.undo(undo, now, hot_window_secs)
        }

        fn age_to_cold(&self, now: u64, hot_window_secs: u64) -> Result<u64, UtxoError> {
            self.inner.age_to_cold(now, hot_window_secs)
        }

        fn snapshot_entries(&self) -> Result<BTreeMap<OutPointKey, Utxo>, UtxoError> {
            self.inner.snapshot_entries()
        }

        fn replace_all(
            &self,
            entries: &BTreeMap<OutPointKey, Utxo>,
            now: u64,
            hot_window_secs: u64,
        ) -> Result<(), UtxoError> {
            self.inner.replace_all(entries, now, hot_window_secs)
        }

        fn tier_stats(&self) -> Result<TierStats, UtxoError> {
            self.inner.tier_stats()
        }
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
            deployments(1),
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
    fn commits_a_multi_transaction_block_with_one_utxo_write() {
        let directory = TempDir::new().unwrap();
        let inner = RedbUtxoStore::open(directory.path().join("chainstate.redb")).unwrap();
        let previous = OutPoint::new(bitcoin::Txid::from_byte_array([7; 32]), 0);
        inner
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
        let chainstate = CountingStore {
            inner: &inner,
            writes: AtomicUsize::new(0),
        };
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
        let undo_store = RedbUndoStore::open(directory.path().join("undo.redb")).unwrap();
        let execution_store =
            RedbExecutionStore::open(directory.path().join("execution.redb"), Network::Regtest)
                .unwrap();
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

        connect_active_block(
            &chainstate,
            &undo_store,
            &execution_store,
            &headers,
            &active_block,
            1,
            60,
            deployments(1),
        )
        .unwrap();

        assert_eq!(chainstate.writes.load(Ordering::Relaxed), 1);
        assert!(inner.get(previous.into()).unwrap().is_none());
        let created = OutPoint::new(active_block.txdata[1].compute_txid(), 0).into();
        assert_eq!(inner.get(created).unwrap().unwrap().value_sats, 900);
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
                deployments(1),
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
                ..deployments(1)
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
