//! Atomic block-to-chainstate transition checks.

use std::{
    collections::VecDeque,
    sync::{Arc, Condvar, Mutex, OnceLock, mpsc},
};

use bitcoin::{
    Block, TxMerkleNode,
    consensus::{Encodable, serialize},
    hashes::{Hash, sha256d},
};
use thiserror::Error;

use crate::{
    chainstate::{AppliedTransaction, ChainstateError, apply_transaction_with_deferred_scripts},
    consensus::{
        ConsensusError, verify_serialized_transaction_scripts_with_flags,
        verify_transaction_scripts_with_flags,
    },
    signet::{SignetError, validate_signet_block_solution},
    utxo::{Utxo, UtxoError, UtxoStore, UtxoUndo},
};

/// Bitcoin's maximum serialized block weight in weight units.
pub const MAX_BLOCK_WEIGHT: u64 = 4_000_000;
/// Maximum consensus signature-operation cost per block.
pub const MAX_BLOCK_SIGOPS_COST: u64 = 80_000;
const BITCOIN_HALVING_INTERVAL: u32 = 210_000;
const INITIAL_SUBSIDY_SATS: u64 = 50 * 100_000_000;

/// Reorg data produced by a successfully applied block.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AppliedBlock {
    /// Hash of the connected block.
    pub hash: bitcoin::BlockHash,
    /// Transaction undo data in block order; disconnect in reverse order.
    pub transaction_undos: Vec<UtxoUndo>,
}

pub(crate) struct DeferredScriptCheck<'a> {
    index: usize,
    block_order: usize,
    transaction: &'a bitcoin::Transaction,
    prevouts: Vec<Utxo>,
    script_flags: u32,
}

impl DeferredScriptCheck<'_> {
    pub(crate) fn set_block_order(&mut self, block_order: usize) {
        self.block_order = block_order;
    }
}

type ScriptValidationResult = (usize, usize, Result<(), ConsensusError>);

struct ScriptValidationJob {
    index: usize,
    block_order: usize,
    raw_transaction: Vec<u8>,
    input_count: usize,
    prevouts: Vec<Utxo>,
    script_flags: u32,
    result: mpsc::Sender<ScriptValidationResult>,
}

#[derive(Default)]
struct ScriptValidationQueue {
    jobs: Mutex<VecDeque<ScriptValidationJob>>,
    available: Condvar,
}

struct ScriptValidationPool {
    queue: Arc<ScriptValidationQueue>,
    workers: usize,
}

impl ScriptValidationPool {
    fn new() -> Self {
        let workers = std::thread::available_parallelism()
            .map_or(1, std::num::NonZero::get)
            .max(1);
        let queue = Arc::new(ScriptValidationQueue::default());
        for worker in 0..workers {
            let queue = Arc::clone(&queue);
            std::thread::Builder::new()
                .name(format!("rbtc-script-{worker}"))
                .spawn(move || script_validation_worker(&queue))
                .expect("script-validation worker must start");
        }
        Self { queue, workers }
    }

    fn enqueue(&self, job: ScriptValidationJob) {
        self.queue
            .jobs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push_back(job);
        self.queue.available.notify_one();
    }
}

fn script_validation_worker(queue: &ScriptValidationQueue) {
    loop {
        let job = {
            let mut jobs = queue
                .jobs
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            while jobs.is_empty() {
                jobs = queue
                    .available
                    .wait(jobs)
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
            }
            jobs.pop_front().expect("non-empty script queue")
        };
        let validation = verify_serialized_transaction_scripts_with_flags(
            &job.raw_transaction,
            job.input_count,
            &job.prevouts,
            job.script_flags,
        );
        let _ = job.result.send((job.block_order, job.index, validation));
    }
}

fn script_validation_pool() -> &'static ScriptValidationPool {
    static POOL: OnceLock<ScriptValidationPool> = OnceLock::new();
    POOL.get_or_init(ScriptValidationPool::new)
}

/// A checkpoint-wide producer/consumer session that overlaps sequential UTXO
/// construction with immutable script validation.
pub(crate) struct DeferredScriptBatch {
    result: mpsc::Sender<ScriptValidationResult>,
    results: mpsc::Receiver<ScriptValidationResult>,
    jobs: usize,
}

impl DeferredScriptBatch {
    pub(crate) fn new() -> Self {
        let (result, results) = mpsc::channel();
        Self {
            result,
            results,
            jobs: 0,
        }
    }

    pub(crate) fn submit(&mut self, checks: Vec<DeferredScriptCheck<'_>>) {
        let pool = script_validation_pool();
        for check in checks {
            pool.enqueue(ScriptValidationJob {
                index: check.index,
                block_order: check.block_order,
                raw_transaction: serialize(check.transaction),
                input_count: check.transaction.input.len(),
                prevouts: check.prevouts,
                script_flags: check.script_flags,
                result: self.result.clone(),
            });
            self.jobs = self.jobs.saturating_add(1);
        }
    }

    pub(crate) fn finish(self) -> Option<(usize, ConsensusError)> {
        (0..self.jobs)
            .filter_map(|_| {
                let (block_order, index, validation) = self
                    .results
                    .recv()
                    .expect("script-validation worker terminated without a result");
                validation.err().map(|error| (block_order, index, error))
            })
            .min_by_key(|(block_order, index, _)| (*block_order, *index))
            .map(|(_, index, error)| (index, error))
    }
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
    /// The transaction Merkle tree contains an ambiguous duplicated branch.
    #[error("block transaction merkle tree is mutated")]
    MutatedMerkleTree,
    /// A SegWit witness commitment is missing or does not match the transaction data.
    #[error("block witness commitment mismatch")]
    WitnessCommitment,
    /// Witness data appears without an active, valid coinbase commitment.
    #[error("block contains unexpected witness data")]
    UnexpectedWitness,
    /// A Signet block did not satisfy the network's BIP325 challenge.
    #[error("Signet block solution: {0}")]
    Signet(#[from] SignetError),
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
    /// Sum of individually valid transaction fees exceeded Bitcoin's money range.
    #[error("block fee sum {fees} exceeds MAX_MONEY")]
    FeeOutOfRange {
        /// Accumulated transaction fees in satoshis.
        fees: u64,
    },
    /// Fee or subsidy accounting overflowed its integer representation.
    #[error("block fee accounting overflow")]
    FeeOverflow,
    /// Aggregate signature-operation cost exceeds the consensus block limit.
    #[error("block sigop cost {cost} exceeds limit {MAX_BLOCK_SIGOPS_COST}")]
    SigopCost {
        /// Accumulated cost at the rejecting transaction.
        cost: u64,
    },
    /// Aggregate signature-operation accounting overflowed.
    #[error("block sigop cost overflow")]
    SigopOverflow,
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

impl BlockError {
    /// Returns whether the failure proves a freshly downloaded block is invalid.
    #[must_use]
    pub const fn is_peer_invalid(&self) -> bool {
        match self {
            Self::Transaction { source, .. } => source.is_peer_invalid(),
            Self::Rollback(_) => false,
            Self::Empty
            | Self::MissingCoinbase
            | Self::MultipleCoinbase
            | Self::MerkleRoot
            | Self::MutatedMerkleTree
            | Self::WitnessCommitment
            | Self::UnexpectedWitness
            | Self::Signet(_)
            | Self::Bip34Height { .. }
            | Self::Weight { .. }
            | Self::ExcessCoinbase { .. }
            | Self::FeeOutOfRange { .. }
            | Self::FeeOverflow
            | Self::SigopCost { .. }
            | Self::SigopOverflow => true,
        }
    }
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
    creation_mtp: u32,
    hot_window_secs: u64,
    script_flags: u32,
) -> Result<AppliedBlock, BlockError> {
    apply_block_with_bip34(
        store,
        block,
        height,
        now,
        creation_mtp,
        hot_window_secs,
        script_flags,
        false,
    )
}

/// Validates and applies a block, optionally enforcing the BIP34 height commitment.
///
/// The chain deployment manager must set `bip34_active` only at and after the
/// selected network's activation height.
#[allow(clippy::too_many_arguments)]
pub fn apply_block_with_bip34<S: UtxoStore>(
    store: &S,
    block: &Block,
    height: u32,
    now: u64,
    creation_mtp: u32,
    hot_window_secs: u64,
    script_flags: u32,
    bip34_active: bool,
) -> Result<AppliedBlock, BlockError> {
    apply_block_with_deployments(
        store,
        block,
        height,
        now,
        creation_mtp,
        hot_window_secs,
        script_flags,
        bip34_active,
        false,
        script_flags & bitcoinconsensus::VERIFY_WITNESS != 0,
        None,
        block_subsidy(height),
    )
}

/// Validates and applies a block with its consensus-derived previous MTP.
///
/// `creation_mtp` is the median time past of the candidate block's parent and
/// is persisted on every created output for BIP68 evaluation.
#[allow(clippy::too_many_arguments)]
pub fn apply_block_with_context<S: UtxoStore>(
    store: &S,
    block: &Block,
    height: u32,
    now: u64,
    creation_mtp: u32,
    hot_window_secs: u64,
    script_flags: u32,
    bip34_active: bool,
) -> Result<AppliedBlock, BlockError> {
    apply_block_with_deployments(
        store,
        block,
        height,
        now,
        creation_mtp,
        hot_window_secs,
        script_flags,
        bip34_active,
        false,
        script_flags & bitcoinconsensus::VERIFY_WITNESS != 0,
        None,
        block_subsidy(height),
    )
}

/// Validates and applies a block with deployment-aware BIP34, CSV, SegWit, and Signet rules.
///
/// `csv_active` enables the BIP68 relative sequence locks and changes absolute
/// lock-time evaluation to BIP113 parent-MTP semantics. Before activation,
/// absolute lock-time is compared with the candidate header timestamp.
/// `signet_challenge` selects the exact default or custom BIP325 script. `subsidy_sats`
/// must come from the selected network's consensus parameters at `height`.
#[allow(clippy::too_many_arguments)]
#[allow(clippy::fn_params_excessive_bools)]
pub fn apply_block_with_deployments<S: UtxoStore>(
    store: &S,
    block: &Block,
    height: u32,
    now: u64,
    creation_mtp: u32,
    hot_window_secs: u64,
    script_flags: u32,
    bip34_active: bool,
    csv_active: bool,
    segwit_active: bool,
    signet_challenge: Option<&[u8]>,
    subsidy_sats: u64,
) -> Result<AppliedBlock, BlockError> {
    validate_block_structure_with_deployments(
        block,
        height,
        bip34_active,
        segwit_active,
        signet_challenge,
    )?;
    apply_prevalidated_block_with_deployments(
        store,
        block,
        height,
        now,
        creation_mtp,
        hot_window_secs,
        script_flags,
        csv_active,
        subsidy_sats,
    )
}

/// Applies a block whose context-free and deployment-aware structure was
/// already checked against the same candidate context.
///
/// This avoids recomputing transaction/witness Merkle commitments on the IBD
/// path after the downloaded payload has already passed structural validation.
#[allow(clippy::too_many_arguments)]
pub(crate) fn apply_prevalidated_block_with_deployments<S: UtxoStore>(
    store: &S,
    block: &Block,
    height: u32,
    now: u64,
    creation_mtp: u32,
    hot_window_secs: u64,
    script_flags: u32,
    csv_active: bool,
    subsidy_sats: u64,
) -> Result<AppliedBlock, BlockError> {
    apply_prevalidated_block_with_deployments_inner(
        store,
        block,
        height,
        now,
        creation_mtp,
        hot_window_secs,
        script_flags,
        csv_active,
        subsidy_sats,
        false,
    )
    .map(|(applied, _)| applied)
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn apply_prevalidated_block_with_deferred_scripts<'a, S: UtxoStore>(
    store: &S,
    block: &'a Block,
    height: u32,
    now: u64,
    creation_mtp: u32,
    hot_window_secs: u64,
    script_flags: u32,
    csv_active: bool,
    subsidy_sats: u64,
) -> Result<(AppliedBlock, Vec<DeferredScriptCheck<'a>>), BlockError> {
    apply_prevalidated_block_with_deployments_inner(
        store,
        block,
        height,
        now,
        creation_mtp,
        hot_window_secs,
        script_flags,
        csv_active,
        subsidy_sats,
        true,
    )
}

#[allow(clippy::too_many_arguments)]
#[allow(clippy::too_many_lines)]
fn apply_prevalidated_block_with_deployments_inner<'a, S: UtxoStore>(
    store: &S,
    block: &'a Block,
    height: u32,
    now: u64,
    creation_mtp: u32,
    hot_window_secs: u64,
    script_flags: u32,
    csv_active: bool,
    subsidy_sats: u64,
    defer_scripts: bool,
) -> Result<(AppliedBlock, Vec<DeferredScriptCheck<'a>>), BlockError> {
    let mut applied = Vec::with_capacity(block.txdata.len());
    let mut script_checks = Vec::with_capacity(block.txdata.len().saturating_sub(1));
    let mut sigop_cost = 0_u64;
    let mut fees = 0_u64;
    let lock_time_context = if csv_active {
        creation_mtp
    } else {
        block.header.time
    };
    for (index, transaction) in block.txdata.iter().enumerate() {
        match apply_transaction_with_deferred_scripts(
            store,
            transaction,
            height,
            now,
            creation_mtp,
            lock_time_context,
            script_flags,
            csv_active,
        ) {
            Ok((transaction, prevouts)) => {
                applied.push(transaction);
                if index != 0 {
                    script_checks.push(DeferredScriptCheck {
                        index,
                        block_order: 0,
                        transaction: &block.txdata[index],
                        prevouts,
                        script_flags,
                    });
                    let transaction = applied.last().expect("just pushed");
                    let transaction_fee = transaction
                        .input_value_sats
                        .checked_sub(transaction.output_value_sats)
                        .expect("validated non-coinbase transaction cannot inflate");
                    let Some(next_fees) = fees.checked_add(transaction_fee) else {
                        rollback(store, &applied, now, hot_window_secs)?;
                        return Err(BlockError::FeeOverflow);
                    };
                    if next_fees > crate::chainstate::MAX_MONEY_SATS {
                        rollback(store, &applied, now, hot_window_secs)?;
                        return Err(BlockError::FeeOutOfRange { fees: next_fees });
                    }
                    fees = next_fees;
                }
                let Some(next_sigop_cost) =
                    sigop_cost.checked_add(applied.last().expect("just pushed").sigop_cost)
                else {
                    rollback(store, &applied, now, hot_window_secs)?;
                    return Err(BlockError::SigopOverflow);
                };
                sigop_cost = next_sigop_cost;
                if sigop_cost > MAX_BLOCK_SIGOPS_COST {
                    rollback(store, &applied, now, hot_window_secs)?;
                    return Err(BlockError::SigopCost { cost: sigop_cost });
                }
            }
            Err(source) => {
                let script_failure = verify_deferred_scripts(script_checks);
                rollback(store, &applied, now, hot_window_secs)?;
                if let Some((index, source)) = script_failure {
                    return Err(BlockError::Transaction {
                        index,
                        source: source.into(),
                    });
                }
                return Err(BlockError::Transaction { index, source });
            }
        }
    }
    if !defer_scripts {
        if let Some((index, source)) = verify_deferred_scripts(std::mem::take(&mut script_checks)) {
            rollback(store, &applied, now, hot_window_secs)?;
            return Err(BlockError::Transaction {
                index,
                source: source.into(),
            });
        }
    }
    let Some(allowed) = subsidy_sats.checked_add(fees) else {
        let script_failure = defer_scripts
            .then(|| verify_deferred_scripts(std::mem::take(&mut script_checks)))
            .flatten();
        rollback(store, &applied, now, hot_window_secs)?;
        if let Some((index, source)) = script_failure {
            return Err(BlockError::Transaction {
                index,
                source: source.into(),
            });
        }
        return Err(BlockError::FeeOverflow);
    };
    if applied[0].output_value_sats > allowed {
        let script_failure = defer_scripts
            .then(|| verify_deferred_scripts(std::mem::take(&mut script_checks)))
            .flatten();
        rollback(store, &applied, now, hot_window_secs)?;
        if let Some((index, source)) = script_failure {
            return Err(BlockError::Transaction {
                index,
                source: source.into(),
            });
        }
        return Err(BlockError::ExcessCoinbase {
            claimed: applied[0].output_value_sats,
            allowed,
        });
    }
    Ok((
        AppliedBlock {
            hash: block.block_hash(),
            transaction_undos: applied
                .into_iter()
                .map(|transaction| transaction.undo)
                .collect(),
        },
        script_checks,
    ))
}

pub(crate) fn verify_deferred_scripts(
    checks: Vec<DeferredScriptCheck<'_>>,
) -> Option<(usize, ConsensusError)> {
    if checks.is_empty() {
        return None;
    }
    let input_count = checks
        .iter()
        .map(|check| check.transaction.input.len())
        .sum::<usize>();
    let pool = script_validation_pool();
    if pool.workers == 1 || checks.len() == 1 || input_count < 8 {
        return checks.into_iter().find_map(|check| {
            verify_transaction_scripts_with_flags(
                check.transaction,
                &check.prevouts,
                check.script_flags,
            )
            .err()
            .map(|error| (check.index, error))
        });
    }
    let mut batch = DeferredScriptBatch::new();
    batch.submit(checks);
    batch.finish()
}

/// Validates block commitments and context-free structure without mutating UTXO state.
///
/// This is used when re-reading bytes for a block that the durable execution
/// tip proves was previously fully validated. It rejects ambiguous mutated
/// Merkle trees and enforces deployment-aware witness commitments.
pub fn validate_block_structure(
    block: &Block,
    height: u32,
    script_flags: u32,
    bip34_active: bool,
) -> Result<(), BlockError> {
    validate_block_structure_with_deployments(
        block,
        height,
        bip34_active,
        script_flags & bitcoinconsensus::VERIFY_WITNESS != 0,
        None,
    )
}

/// Validates block structure with script, SegWit, and Signet gates separated.
pub fn validate_block_structure_with_deployments(
    block: &Block,
    height: u32,
    bip34_active: bool,
    segwit_active: bool,
    signet_challenge: Option<&[u8]>,
) -> Result<(), BlockError> {
    if let Some(challenge) = signet_challenge {
        if block.block_hash()
            != bitcoin::blockdata::constants::genesis_block(bitcoin::Network::Signet).block_hash()
        {
            validate_signet_block_solution(block, challenge)?;
        }
    }
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
    let (merkle_root, mutated) = transaction_merkle_root(block);
    if merkle_root != Some(block.header.merkle_root) {
        return Err(BlockError::MerkleRoot);
    }
    if mutated {
        return Err(BlockError::MutatedMerkleTree);
    }
    check_witness_commitment(block, segwit_active)?;
    let weight = block.weight().to_wu();
    if weight > MAX_BLOCK_WEIGHT {
        return Err(BlockError::Weight { weight });
    }
    Ok(())
}

fn transaction_merkle_root(block: &Block) -> (Option<TxMerkleNode>, bool) {
    let mut hashes = block
        .txdata
        .iter()
        .map(|transaction| transaction.compute_txid().to_raw_hash())
        .collect::<Vec<_>>();
    if hashes.is_empty() {
        return (None, false);
    }
    let mut mutated = false;
    while hashes.len() > 1 {
        let mut parents = Vec::with_capacity(hashes.len().div_ceil(2));
        for pair in hashes.chunks(2) {
            let left = pair[0];
            let right = pair.get(1).copied().unwrap_or(left);
            if pair.len() == 2 && left == right {
                mutated = true;
            }
            let mut engine = sha256d::Hash::engine();
            left.consensus_encode(&mut engine)
                .expect("hash engines do not fail");
            right
                .consensus_encode(&mut engine)
                .expect("hash engines do not fail");
            parents.push(sha256d::Hash::from_engine(engine));
        }
        hashes = parents;
    }
    (Some(hashes[0].into()), mutated)
}

fn check_witness_commitment(block: &Block, segwit_active: bool) -> Result<(), BlockError> {
    const MAGIC: [u8; 6] = [0x6a, 0x24, 0xaa, 0x21, 0xa9, 0xed];
    let has_commitment = block.txdata[0].output.iter().any(|output| {
        output.script_pubkey.len() >= 38 && output.script_pubkey.as_bytes()[..6] == MAGIC
    });
    let has_witness = block
        .txdata
        .iter()
        .flat_map(|transaction| &transaction.input)
        .any(|input| !input.witness.is_empty());
    if segwit_active && has_commitment {
        let reserved = &block.txdata[0].input[0].witness;
        if reserved.len() != 1 || reserved[0].len() != 32 || !block.check_witness_commitment() {
            return Err(BlockError::WitnessCommitment);
        }
        return Ok(());
    }
    if has_witness {
        return Err(BlockError::UnexpectedWitness);
    }
    Ok(())
}

fn coinbase_has_height(transaction: &bitcoin::Transaction, height: u32) -> bool {
    let expected = encode_script_num_push(height);
    let script = transaction.input[0].script_sig.as_bytes();
    script.len() >= expected.len() && script[..expected.len()] == expected
}

fn encode_script_num_push(height: u32) -> Vec<u8> {
    match height {
        0 => vec![0x00],
        1..=16 => vec![0x50 + u8::try_from(height).expect("small script number")],
        _ => {
            let encoded = encode_script_num(height);
            let mut pushed = Vec::with_capacity(encoded.len() + 1);
            pushed.push(u8::try_from(encoded.len()).expect("u32 script number fits direct push"));
            pushed.extend_from_slice(&encoded);
            pushed
        }
    }
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

/// Returns the Bitcoin 210,000-block-interval subsidy, excluding fees.
///
/// Network-aware execution should use its candidate deployment context; this
/// helper preserves the public Bitcoin/mainnet calculation used by callers.
#[must_use]
pub const fn block_subsidy(height: u32) -> u64 {
    block_subsidy_with_interval(height, BITCOIN_HALVING_INTERVAL)
}

/// Returns the proof-of-work subsidy for a network-specific halving interval.
///
/// The interval is a consensus parameter: Bitcoin, testnet, and signet use
/// 210,000 blocks, while Bitcoin Core regtest uses 150 blocks.
#[must_use]
pub const fn block_subsidy_with_interval(height: u32, halving_interval: u32) -> u64 {
    assert!(halving_interval != 0, "halving interval must be non-zero");
    let halvings = height / halving_interval;
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
        absolute::LockTime, blockdata::constants::genesis_block, hashes::Hash,
        transaction::Version,
    };
    use tempfile::TempDir;

    use super::*;
    use crate::utxo::{OutPointKey, RedbUtxoStore, Utxo};

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

    fn insert_spendable_output(store: &RedbUtxoStore, outpoint: OutPoint) {
        store
            .apply(
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
    }

    #[test]
    fn applies_and_disconnects_a_valid_coinbase_block() {
        let (_dir, store) = store();
        let transaction = coinbase();
        let block = block(vec![transaction.clone()]);
        let applied = apply_block(&store, &block, 0, 100, 0, 60, 0).unwrap();
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
            apply_block(&store, &block, 0, 100, 0, 60, 0),
            Err(BlockError::Transaction { index: 1, .. })
        ));
        let output = OutPointKey::from(OutPoint::new(coinbase.compute_txid(), 0));
        assert!(store.get(output).unwrap().is_none());
    }

    #[test]
    fn parallel_script_pool_reports_the_earliest_failure_and_rolls_back() {
        let (_dir, store) = store();
        let outpoints = (0_u8..8)
            .map(|byte| OutPoint::new(bitcoin::Txid::from_byte_array([byte; 32]), 0))
            .collect::<Vec<_>>();
        let spendable = outpoints
            .iter()
            .enumerate()
            .map(|(index, outpoint)| {
                (
                    OutPointKey::from(*outpoint),
                    Utxo {
                        value_sats: 1,
                        height: 1,
                        is_coinbase: false,
                        last_touched: 0,
                        creation_mtp: 0,
                        script_pubkey: vec![if matches!(index, 2 | 6) { 0x00 } else { 0x51 }],
                    },
                )
            })
            .collect::<Vec<_>>();
        store.apply(&[], &spendable).unwrap();
        let spends = outpoints
            .chunks_exact(2)
            .map(|pair| Transaction {
                version: Version::ONE,
                lock_time: LockTime::ZERO,
                input: pair
                    .iter()
                    .map(|outpoint| TxIn {
                        previous_output: *outpoint,
                        script_sig: ScriptBuf::new(),
                        sequence: Sequence::MAX,
                        witness: Witness::default(),
                    })
                    .collect(),
                output: vec![TxOut {
                    value: Amount::from_sat(1),
                    script_pubkey: ScriptBuf::new(),
                }],
            })
            .collect::<Vec<_>>();
        let coinbase = coinbase();
        let coinbase_output = OutPointKey::from(OutPoint::new(coinbase.compute_txid(), 0));
        let candidate = block(std::iter::once(coinbase).chain(spends).collect());

        assert!(matches!(
            apply_block(&store, &candidate, 2, 100, 0, 60, 0),
            Err(BlockError::Transaction { index: 2, .. })
        ));
        assert!(store.get(coinbase_output).unwrap().is_none());
        for outpoint in outpoints {
            assert!(store.get(outpoint.into()).unwrap().is_some());
        }
    }

    #[test]
    fn rejects_accumulated_fees_above_money_range_and_rolls_back() {
        let (_dir, store) = store();
        let previous = [
            OutPoint::new(bitcoin::Txid::from_byte_array([7; 32]), 0),
            OutPoint::new(bitcoin::Txid::from_byte_array([8; 32]), 0),
        ];
        let spendable = |outpoint: OutPoint| {
            (
                OutPointKey::from(outpoint),
                Utxo {
                    value_sats: crate::chainstate::MAX_MONEY_SATS,
                    height: 0,
                    is_coinbase: false,
                    last_touched: 0,
                    creation_mtp: 0,
                    script_pubkey: vec![0x51],
                },
            )
        };
        store
            .apply(&[], &[spendable(previous[0]), spendable(previous[1])])
            .unwrap();
        let spend = |outpoint: OutPoint| Transaction {
            version: Version::ONE,
            lock_time: LockTime::ZERO,
            input: vec![TxIn {
                previous_output: outpoint,
                script_sig: ScriptBuf::new(),
                sequence: Sequence::MAX,
                witness: Witness::new(),
            }],
            output: vec![TxOut {
                value: Amount::ZERO,
                script_pubkey: ScriptBuf::new(),
            }],
        };
        let candidate = block(vec![coinbase(), spend(previous[0]), spend(previous[1])]);

        assert!(matches!(
            apply_block(&store, &candidate, 1, 100, 0, 60, 0),
            Err(BlockError::FeeOutOfRange { fees })
                if fees == crate::chainstate::MAX_MONEY_SATS * 2
        ));
        assert!(store.get(previous[0].into()).unwrap().is_some());
        assert!(store.get(previous[1].into()).unwrap().is_some());
        assert_eq!(store.snapshot_entries().unwrap().len(), 2);
    }

    #[test]
    fn fee_accounting_overflow_rolls_back_every_transaction() {
        let (_dir, store) = store();
        let previous = OutPoint::new(bitcoin::Txid::from_byte_array([9; 32]), 0);
        insert_spendable_output(&store, previous);
        let spend = Transaction {
            version: Version::ONE,
            lock_time: LockTime::ZERO,
            input: vec![TxIn {
                previous_output: previous,
                script_sig: ScriptBuf::new(),
                sequence: Sequence::MAX,
                witness: Witness::new(),
            }],
            output: vec![TxOut {
                value: Amount::ZERO,
                script_pubkey: ScriptBuf::new(),
            }],
        };
        let candidate = block(vec![coinbase(), spend]);

        assert!(matches!(
            apply_block_with_deployments(
                &store,
                &candidate,
                1,
                100,
                0,
                60,
                0,
                false,
                false,
                false,
                None,
                u64::MAX,
            ),
            Err(BlockError::FeeOverflow)
        ));
        assert!(store.get(previous.into()).unwrap().is_some());
        assert_eq!(store.snapshot_entries().unwrap().len(), 1);
    }

    #[test]
    fn rejects_witness_data_without_a_witness_commitment() {
        let (_dir, store) = store();
        let mut transaction = coinbase();
        transaction.input[0].witness = Witness::from_slice(&[b"reserved".as_slice()]);
        let block = block(vec![transaction]);
        assert!(matches!(
            apply_block(&store, &block, 0, 100, 0, 60, 0),
            Err(BlockError::UnexpectedWitness)
        ));
    }

    #[test]
    fn active_segwit_commitment_requires_a_reserved_nonce() {
        let (_dir, store) = store();
        let mut transaction = coinbase();
        transaction.output.push(TxOut {
            value: Amount::ZERO,
            script_pubkey: ScriptBuf::from_bytes(
                [vec![0x6a, 0x24, 0xaa, 0x21, 0xa9, 0xed], vec![0; 32]].concat(),
            ),
        });
        let block = block(vec![transaction]);
        assert!(matches!(
            apply_block(
                &store,
                &block,
                0,
                100,
                0,
                60,
                bitcoinconsensus::VERIFY_P2SH | bitcoinconsensus::VERIFY_WITNESS,
            ),
            Err(BlockError::WitnessCommitment)
        ));
        assert!(store.snapshot_entries().unwrap().is_empty());
    }

    #[test]
    fn rejects_a_mutated_transaction_merkle_tree() {
        let (_dir, store) = store();
        let transaction = |value| Transaction {
            version: Version::ONE,
            lock_time: LockTime::ZERO,
            input: Vec::new(),
            output: vec![TxOut {
                value: Amount::from_sat(value),
                script_pubkey: ScriptBuf::new(),
            }],
        };
        let duplicate = transaction(2);
        let block = block(vec![
            coinbase(),
            transaction(1),
            duplicate.clone(),
            duplicate,
        ]);
        assert!(matches!(
            apply_block(&store, &block, 0, 100, 0, 60, 0),
            Err(BlockError::MutatedMerkleTree)
        ));
        assert!(store.snapshot_entries().unwrap().is_empty());
    }

    #[test]
    fn rejects_excessive_sigop_cost_and_rolls_back_coinbase() {
        let (_dir, store) = store();
        let mut coinbase = coinbase();
        coinbase.output[0].script_pubkey = ScriptBuf::from_bytes(vec![0xac; 20_001]);
        let block = block(vec![coinbase]);
        assert!(matches!(
            apply_block(&store, &block, 0, 100, 0, 60, 0),
            Err(BlockError::SigopCost { cost: 80_004 })
        ));
        assert!(store.snapshot_entries().unwrap().is_empty());
    }

    #[test]
    fn bip113_switches_absolute_time_locks_to_parent_mtp() {
        let previous_output = OutPoint::new(bitcoin::Txid::from_byte_array([9; 32]), 0);
        let spend = Transaction {
            version: Version::TWO,
            lock_time: LockTime::from_consensus(500_000_000),
            input: vec![TxIn {
                previous_output,
                script_sig: ScriptBuf::new(),
                sequence: Sequence::ZERO,
                witness: Witness::default(),
            }],
            output: vec![TxOut {
                value: Amount::from_sat(1),
                script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
            }],
        };
        let mut candidate = block(vec![coinbase(), spend]);
        candidate.header.time = 500_000_001;

        let (_dir, pre_csv_store) = store();
        insert_spendable_output(&pre_csv_store, previous_output);
        apply_block_with_deployments(
            &pre_csv_store,
            &candidate,
            101,
            0,
            500_000_000,
            60,
            0,
            false,
            false,
            false,
            None,
            block_subsidy(101),
        )
        .unwrap();

        let (_dir, csv_store) = store();
        insert_spendable_output(&csv_store, previous_output);
        assert!(matches!(
            apply_block_with_deployments(
                &csv_store,
                &candidate,
                101,
                0,
                500_000_000,
                60,
                0,
                false,
                true,
                false,
                None,
                block_subsidy(101),
            ),
            Err(BlockError::Transaction {
                index: 1,
                source: ChainstateError::NonFinalLockTime { .. },
            })
        ));
    }

    #[test]
    fn bip34_requires_minimally_encoded_coinbase_height() {
        let (_dir, store) = store();
        let mut transaction = coinbase();
        transaction.input[0].script_sig = ScriptBuf::from_bytes(vec![0x5a, 0x00]);
        let valid_block = block(vec![transaction.clone()]);
        assert!(apply_block_with_bip34(&store, &valid_block, 10, 100, 0, 60, 0, true).is_ok());
        transaction.input[0].script_sig = ScriptBuf::from_bytes(vec![1, 17]);
        let height_seventeen = block(vec![transaction]);
        assert!(apply_block_with_bip34(&store, &height_seventeen, 17, 100, 0, 60, 0, true).is_ok());
        let bad = block(vec![coinbase()]);
        assert!(matches!(
            apply_block_with_bip34(&store, &bad, 10, 100, 0, 60, 0, true),
            Err(BlockError::Bip34Height { height: 10 })
        ));
    }

    #[test]
    fn candidate_subsidy_is_a_consensus_parameter() {
        let (_dir, store) = store();
        let candidate = block(vec![coinbase()]);
        let regtest_subsidy = block_subsidy_with_interval(150, 150);
        assert_eq!(regtest_subsidy, 25 * 100_000_000);
        assert!(matches!(
            apply_block_with_deployments(
                &store,
                &candidate,
                150,
                0,
                0,
                60,
                0,
                false,
                false,
                false,
                None,
                regtest_subsidy,
            ),
            Err(BlockError::ExcessCoinbase {
                claimed: 5_000_000_000,
                allowed: 2_500_000_000,
            })
        ));
        assert!(store.snapshot_entries().unwrap().is_empty());
    }
}
