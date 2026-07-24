//! Transaction-to-UTXO application with consensus-critical accounting checks.

use std::collections::BTreeSet;

use bitcoin::{Script, Sequence, Transaction, Witness, WitnessVersion, script::Instruction};
use thiserror::Error;

use crate::{
    consensus::{ConsensusError, verify_transaction_scripts_with_flags},
    utxo::{OutPointKey, Utxo, UtxoError, UtxoStore, UtxoUndo},
};

/// Coinbase outputs require this many confirmations before they are spendable.
pub const COINBASE_MATURITY: u32 = 100;
/// Total bitcoin supply cap in satoshis.
pub const MAX_MONEY_SATS: u64 = 21_000_000 * 100_000_000;
const MAX_SCRIPT_SIZE: usize = 10_000;
const SEQUENCE_LOCKTIME_MASK: u32 = 0x0000_FFFF;
const SEQUENCE_LOCKTIME_GRANULARITY: u32 = 9;

/// A successful transaction application and its reorg data.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AppliedTransaction {
    /// Transaction ID whose outputs were created.
    pub txid: bitcoin::Txid,
    /// Sum of consumed inputs, in satoshis (zero for coinbase).
    pub input_value_sats: u64,
    /// Sum of created outputs, in satoshis.
    pub output_value_sats: u64,
    /// Consensus sigop cost including the witness scale factor where applicable.
    pub sigop_cost: u64,
    /// Undo data to apply if this transaction's containing block disconnects.
    pub undo: UtxoUndo,
}

/// Read-only consensus result for one transaction at a candidate-chain context.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ValidatedTransaction {
    /// Transaction ID whose inputs and outputs were checked.
    pub txid: bitcoin::Txid,
    /// Sum of consumed inputs, in satoshis (zero for coinbase).
    pub input_value_sats: u64,
    /// Sum of created outputs, in satoshis.
    pub output_value_sats: u64,
    /// Consensus sigop cost including the witness scale factor where applicable.
    pub sigop_cost: u64,
}

struct PreparedTransaction {
    validated: ValidatedTransaction,
    spent: Vec<OutPointKey>,
    created: Vec<(OutPointKey, Utxo)>,
    prevouts: Vec<Utxo>,
}

/// Transaction application failure.
#[derive(Debug, Error)]
pub enum ChainstateError {
    /// Chainstate lookup or mutation failed.
    #[error("utxo: {0}")]
    Utxo(#[from] UtxoError),
    /// Bitcoin Core's script interpreter rejected an input.
    #[error("consensus script: {0}")]
    Script(#[from] ConsensusError),
    /// An input sum or output sum exceeded the supported monetary integer range.
    #[error("transaction value sum overflow")]
    ValueOverflow,
    /// The transaction creates more value than it spends.
    #[error("transaction output value exceeds input value")]
    Inflation,
    /// A coinbase output is not mature at the candidate height.
    #[error("coinbase output {outpoint} is immature until height {matures_at}")]
    ImmatureCoinbase {
        /// Coinbase outpoint being spent.
        outpoint: OutPointKey,
        /// First height at which it can be spent.
        matures_at: u32,
    },
    /// Coinbase input script length is outside the consensus range 2..=100.
    #[error("coinbase script length must be between 2 and 100 bytes")]
    CoinbaseScriptSize,
    /// A non-coinbase transaction contains no inputs.
    #[error("non-coinbase transaction has no inputs")]
    NoInputs,
    /// A transaction contains no outputs.
    #[error("transaction has no outputs")]
    NoOutputs,
    /// An output or aggregate transaction value exceeds Bitcoin's money range.
    #[error("transaction value exceeds MAX_MONEY")]
    MoneyRange,
    /// A transaction's absolute lock time is not yet final for this block.
    #[error("transaction lock time {lock_time} is not final at {context}")]
    NonFinalLockTime {
        /// Transaction lock-time value in consensus units.
        lock_time: u32,
        /// Candidate block height or MTP used for the comparison.
        context: u32,
    },
    /// An input's BIP68 height-based relative lock is not yet satisfied.
    #[error("input {outpoint} requires block height above {minimum_height}")]
    RelativeHeightLock {
        /// Spent output subject to the lock.
        outpoint: OutPointKey,
        /// The candidate height must be strictly above this value.
        minimum_height: u32,
    },
    /// An input's BIP68 time-based relative lock is not yet satisfied.
    #[error("input {outpoint} requires median time past above {minimum_mtp}")]
    RelativeTimeLock {
        /// Spent output subject to the lock.
        outpoint: OutPointKey,
        /// The candidate parent MTP must be strictly above this value.
        minimum_mtp: u32,
    },
    /// A transaction contains the same previous outpoint more than once.
    #[error("duplicate transaction input {0}")]
    DuplicateInput(OutPointKey),
    /// A non-coinbase transaction contains the reserved null previous outpoint.
    #[error("non-coinbase transaction contains null previous outpoint")]
    NullPrevout,
    /// A transaction's non-witness serialization alone exceeds block weight.
    #[error("transaction base weight exceeds maximum block weight")]
    Oversize,
}

impl ChainstateError {
    /// Returns whether the failure proves a freshly downloaded transaction is invalid.
    #[must_use]
    pub const fn is_peer_invalid(&self) -> bool {
        match self {
            Self::Utxo(error) => matches!(
                error,
                UtxoError::Duplicate(_) | UtxoError::Missing(_) | UtxoError::DuplicateSpend(_)
            ),
            Self::Script(_)
            | Self::ValueOverflow
            | Self::Inflation
            | Self::ImmatureCoinbase { .. }
            | Self::CoinbaseScriptSize
            | Self::NoInputs
            | Self::NoOutputs
            | Self::MoneyRange
            | Self::NonFinalLockTime { .. }
            | Self::RelativeHeightLock { .. }
            | Self::RelativeTimeLock { .. }
            | Self::DuplicateInput(_)
            | Self::NullPrevout
            | Self::Oversize => true,
        }
    }
}

/// Applies one transaction to a UTXO store after accounting and script checks.
///
/// `script_flags` must reflect the candidate block's active BIP deployments.
/// Coinbase subsidy and the one-coinbase-per-block rule are enforced by the
/// block-level validator, which also supplies the correct height and flags.
/// `lock_time_context` is the candidate header time before BIP113 activation,
/// or its parent MTP after activation.
///
/// # Errors
///
/// Returns an error for missing/immature inputs, invalid scripts, monetary
/// overflow/inflation, invalid coinbase script size, or storage failures.
pub fn apply_transaction<S: UtxoStore>(
    store: &S,
    transaction: &Transaction,
    height: u32,
    now: u64,
    creation_mtp: u32,
    lock_time_context: u32,
    script_flags: u32,
) -> Result<AppliedTransaction, ChainstateError> {
    apply_transaction_with_context(
        store,
        transaction,
        height,
        now,
        creation_mtp,
        lock_time_context,
        script_flags,
        false,
    )
}

/// Applies a transaction with deployment-aware absolute and relative lock-time context.
///
/// When `csv_active` is set, BIP68 relative locks are enforced for version-2+
/// transactions and `lock_time_context` must be the candidate block parent's
/// MTP, as required by BIP113. Before activation it must be the candidate
/// block header time. `creation_mtp` is saved on newly created outputs.
///
/// # Errors
///
/// Returns the same errors as [`apply_transaction`], plus non-final absolute
/// and relative lock-time violations when the deployment is active.
#[allow(clippy::too_many_arguments)]
pub fn apply_transaction_with_context<S: UtxoStore>(
    store: &S,
    transaction: &Transaction,
    height: u32,
    now: u64,
    creation_mtp: u32,
    lock_time_context: u32,
    script_flags: u32,
    csv_active: bool,
) -> Result<AppliedTransaction, ChainstateError> {
    let prepared = prepare_transaction_with_context(
        store,
        transaction,
        None,
        height,
        now,
        creation_mtp,
        lock_time_context,
        script_flags,
        csv_active,
        true,
    )?;
    let undo = store.apply_with_undo(&prepared.spent, &prepared.created)?;
    Ok(AppliedTransaction {
        txid: prepared.validated.txid,
        input_value_sats: prepared.validated.input_value_sats,
        output_value_sats: prepared.validated.output_value_sats,
        sigop_cost: prepared.validated.sigop_cost,
        undo,
    })
}

/// Applies accounting and UTXO changes while returning the resolved prevouts
/// for a block-level parallel script-validation pass.
///
/// All non-script consensus checks remain ordered, so transactions may spend
/// outputs created earlier in the same candidate block. The caller must roll
/// back every returned application if any deferred script check fails.
#[allow(clippy::too_many_arguments)]
pub(crate) fn apply_transaction_with_deferred_scripts<S: UtxoStore>(
    store: &S,
    transaction: &Transaction,
    txid: bitcoin::Txid,
    height: u32,
    now: u64,
    creation_mtp: u32,
    lock_time_context: u32,
    script_flags: u32,
    csv_active: bool,
) -> Result<(AppliedTransaction, Vec<Utxo>), ChainstateError> {
    let prepared = prepare_transaction_with_context(
        store,
        transaction,
        Some(txid),
        height,
        now,
        creation_mtp,
        lock_time_context,
        script_flags,
        csv_active,
        false,
    )?;
    let undo = store.apply_with_undo_fresh_outputs(&prepared.spent, &prepared.created)?;
    Ok((
        AppliedTransaction {
            txid: prepared.validated.txid,
            input_value_sats: prepared.validated.input_value_sats,
            output_value_sats: prepared.validated.output_value_sats,
            sigop_cost: prepared.validated.sigop_cost,
            undo,
        },
        prepared.prevouts,
    ))
}

/// Checks one transaction against the current UTXO set without mutating it.
///
/// This applies the same accounting, maturity, finality, relative-lock, and
/// script checks as [`apply_transaction_with_context`]. Block-only rules and
/// local relay policy remain the caller's responsibility.
///
/// # Errors
///
/// Returns the same validation failures as [`apply_transaction_with_context`],
/// but never writes to the supplied UTXO store.
#[allow(clippy::too_many_arguments)]
pub fn validate_transaction_with_context<S: UtxoStore>(
    store: &S,
    transaction: &Transaction,
    height: u32,
    creation_mtp: u32,
    lock_time_context: u32,
    script_flags: u32,
    csv_active: bool,
) -> Result<ValidatedTransaction, ChainstateError> {
    prepare_transaction_with_context(
        store,
        transaction,
        None,
        height,
        0,
        creation_mtp,
        lock_time_context,
        script_flags,
        csv_active,
        true,
    )
    .map(|prepared| prepared.validated)
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
fn prepare_transaction_with_context<S: UtxoStore>(
    store: &S,
    transaction: &Transaction,
    precomputed_txid: Option<bitcoin::Txid>,
    height: u32,
    now: u64,
    creation_mtp: u32,
    lock_time_context: u32,
    script_flags: u32,
    csv_active: bool,
    verify_scripts: bool,
) -> Result<PreparedTransaction, ChainstateError> {
    if transaction.base_size().saturating_mul(4) > 4_000_000 {
        return Err(ChainstateError::Oversize);
    }
    if !transaction_is_final(transaction, height, lock_time_context) {
        return Err(ChainstateError::NonFinalLockTime {
            lock_time: transaction.lock_time.to_consensus_u32(),
            context: if transaction.lock_time.is_block_height() {
                height
            } else {
                lock_time_context
            },
        });
    }
    let txid = precomputed_txid.unwrap_or_else(|| transaction.compute_txid());
    let output_value = transaction.output.iter().try_fold(0_u64, |sum, output| {
        if output.value.to_sat() > MAX_MONEY_SATS {
            return Err(ChainstateError::MoneyRange);
        }
        sum.checked_add(output.value.to_sat())
            .ok_or(ChainstateError::ValueOverflow)
    })?;
    if output_value > MAX_MONEY_SATS {
        return Err(ChainstateError::MoneyRange);
    }
    if transaction.output.is_empty() {
        return Err(ChainstateError::NoOutputs);
    }
    if transaction.is_coinbase() {
        let script_len = transaction.input[0].script_sig.len();
        if !(2..=100).contains(&script_len) {
            return Err(ChainstateError::CoinbaseScriptSize);
        }
        let created = created_outputs(transaction, txid, height, now, creation_mtp, true);
        return Ok(PreparedTransaction {
            validated: ValidatedTransaction {
                txid,
                input_value_sats: 0,
                output_value_sats: output_value,
                sigop_cost: legacy_sigop_cost(transaction),
            },
            spent: Vec::new(),
            created,
            prevouts: Vec::new(),
        });
    }

    if transaction.input.is_empty() {
        return Err(ChainstateError::NoInputs);
    }

    let mut unique_inputs = BTreeSet::new();
    for input in &transaction.input {
        if input.previous_output.is_null() {
            return Err(ChainstateError::NullPrevout);
        }
        let outpoint = OutPointKey::from(input.previous_output);
        if !unique_inputs.insert(outpoint) {
            return Err(ChainstateError::DuplicateInput(outpoint));
        }
    }

    let mut spent = Vec::with_capacity(transaction.input.len());
    let mut prevouts = Vec::with_capacity(transaction.input.len());
    let mut input_value = 0_u64;
    for input in &transaction.input {
        let outpoint = OutPointKey::from(input.previous_output);
        let utxo = store.get(outpoint)?.ok_or(UtxoError::Missing(outpoint))?;
        if utxo.value_sats > MAX_MONEY_SATS {
            return Err(ChainstateError::MoneyRange);
        }
        if utxo.is_coinbase {
            let matures_at = utxo.height.saturating_add(COINBASE_MATURITY);
            if height < matures_at {
                return Err(ChainstateError::ImmatureCoinbase {
                    outpoint,
                    matures_at,
                });
            }
        }
        if csv_active && transaction.version.0 >= 2 {
            check_sequence_lock(input.sequence, outpoint, &utxo, height, creation_mtp)?;
        }
        input_value = input_value
            .checked_add(utxo.value_sats)
            .ok_or(ChainstateError::ValueOverflow)?;
        if input_value > MAX_MONEY_SATS {
            return Err(ChainstateError::MoneyRange);
        }
        spent.push(outpoint);
        prevouts.push(utxo);
    }
    if output_value > input_value {
        return Err(ChainstateError::Inflation);
    }
    if verify_scripts {
        verify_transaction_scripts_with_flags(transaction, &prevouts, script_flags)?;
    }
    let created = created_outputs(transaction, txid, height, now, creation_mtp, false);
    Ok(PreparedTransaction {
        validated: ValidatedTransaction {
            txid,
            input_value_sats: input_value,
            output_value_sats: output_value,
            sigop_cost: transaction_sigop_cost(transaction, &prevouts, script_flags),
        },
        spent,
        created,
        prevouts,
    })
}

fn legacy_sigop_cost(transaction: &Transaction) -> u64 {
    let sigops = transaction
        .input
        .iter()
        .map(|input| input.script_sig.count_sigops_legacy())
        .chain(
            transaction
                .output
                .iter()
                .map(|output| output.script_pubkey.count_sigops_legacy()),
        )
        .sum::<usize>();
    u64::try_from(sigops).expect("transaction sigops fit u64") * 4
}

fn transaction_sigop_cost(transaction: &Transaction, prevouts: &[Utxo], flags: u32) -> u64 {
    let mut cost = legacy_sigop_cost(transaction);
    for (input, prevout) in transaction.input.iter().zip(prevouts) {
        let script_pubkey = Script::from_bytes(&prevout.script_pubkey);
        if flags & bitcoinconsensus::VERIFY_P2SH != 0 && script_pubkey.is_p2sh() {
            cost += u64::try_from(p2sh_sigops(&input.script_sig)).expect("P2SH sigops fit u64") * 4;
        }
        if flags & bitcoinconsensus::VERIFY_WITNESS != 0 {
            cost += u64::try_from(witness_sigops(
                &input.script_sig,
                script_pubkey,
                &input.witness,
            ))
            .expect("witness sigops fit u64");
        }
    }
    cost
}

fn p2sh_sigops(script_sig: &Script) -> usize {
    last_push(script_sig).map_or(0, |redeem| Script::from_bytes(&redeem).count_sigops())
}

fn last_push(script: &Script) -> Option<Vec<u8>> {
    let mut last = Vec::new();
    for instruction in script.instructions() {
        match instruction.ok()? {
            Instruction::PushBytes(bytes) => last = bytes.as_bytes().to_vec(),
            Instruction::Op(opcode) if opcode.to_u8() <= 0x60 => last.clear(),
            Instruction::Op(_) => return None,
        }
    }
    Some(last)
}

fn witness_sigops(script_sig: &Script, script_pubkey: &Script, witness: &Witness) -> usize {
    if script_pubkey.is_witness_program() {
        return witness_program_sigops(script_pubkey, witness);
    }
    if script_pubkey.is_p2sh() && script_sig.is_push_only() {
        if let Some(redeem) = last_push(script_sig) {
            let redeem = Script::from_bytes(&redeem);
            if redeem.is_witness_program() {
                return witness_program_sigops(redeem, witness);
            }
        }
    }
    0
}

fn witness_program_sigops(program: &Script, witness: &Witness) -> usize {
    if program.witness_version() != Some(WitnessVersion::V0) {
        return 0;
    }
    match program.as_bytes().get(1).copied() {
        Some(20) => 1,
        Some(32) => witness
            .last()
            .map_or(0, |script| Script::from_bytes(script).count_sigops()),
        _ => 0,
    }
}

fn transaction_is_final(transaction: &Transaction, height: u32, lock_time_context: u32) -> bool {
    let lock_time = transaction.lock_time.to_consensus_u32();
    let comparison = if transaction.lock_time.is_block_height() {
        height
    } else {
        lock_time_context
    };
    lock_time < comparison
        || transaction
            .input
            .iter()
            .all(|input| input.sequence == Sequence::MAX)
}

pub(crate) fn check_sequence_lock(
    sequence: Sequence,
    outpoint: OutPointKey,
    utxo: &Utxo,
    height: u32,
    parent_mtp: u32,
) -> Result<(), ChainstateError> {
    if !sequence.is_relative_lock_time() {
        return Ok(());
    }
    let relative = sequence.to_consensus_u32() & SEQUENCE_LOCKTIME_MASK;
    if sequence.is_height_locked() {
        let minimum_height = utxo.height.saturating_add(relative).saturating_sub(1);
        if height <= minimum_height {
            return Err(ChainstateError::RelativeHeightLock {
                outpoint,
                minimum_height,
            });
        }
    } else {
        let relative_seconds = relative << SEQUENCE_LOCKTIME_GRANULARITY;
        let minimum_mtp = utxo
            .creation_mtp
            .saturating_add(relative_seconds)
            .saturating_sub(1);
        if parent_mtp <= minimum_mtp {
            return Err(ChainstateError::RelativeTimeLock {
                outpoint,
                minimum_mtp,
            });
        }
    }
    Ok(())
}

fn created_outputs(
    transaction: &Transaction,
    txid: bitcoin::Txid,
    height: u32,
    now: u64,
    creation_mtp: u32,
    is_coinbase: bool,
) -> Vec<(OutPointKey, Utxo)> {
    transaction
        .output
        .iter()
        .enumerate()
        .filter(|(_, output)| !is_unspendable(&output.script_pubkey))
        .map(|(vout, output)| {
            let vout = u32::try_from(vout).expect("transaction output count fits u32");
            (
                bitcoin::OutPoint::new(txid, vout).into(),
                Utxo {
                    value_sats: output.value.to_sat(),
                    height,
                    is_coinbase,
                    last_touched: now,
                    creation_mtp,
                    script_pubkey: output.script_pubkey.as_bytes().to_vec(),
                },
            )
        })
        .collect()
}

/// Matches Bitcoin Core's `CScript::IsUnspendable` UTXO-pruning predicate.
pub(crate) fn is_unspendable(script: &Script) -> bool {
    script.is_op_return() || script.len() > MAX_SCRIPT_SIZE
}

#[cfg(test)]
mod tests {
    use bitcoin::{
        Amount, OutPoint, ScriptBuf, Sequence, Transaction, TxIn, TxOut, Txid, Witness,
        absolute::LockTime,
        hashes::Hash,
        opcodes::all::{OP_CHECKMULTISIG, OP_CHECKSIG},
        script::Builder,
        transaction::Version,
    };
    use tempfile::TempDir;

    use super::*;
    use crate::utxo::{RedbUtxoStore, Utxo};

    fn store() -> (TempDir, RedbUtxoStore) {
        let dir = TempDir::new().unwrap();
        let store = RedbUtxoStore::open(dir.path().join("chainstate.redb")).unwrap();
        (dir, store)
    }

    fn coinbase(script_sig: Vec<u8>) -> Transaction {
        Transaction {
            version: Version::ONE,
            lock_time: LockTime::ZERO,
            input: vec![TxIn {
                previous_output: OutPoint::null(),
                script_sig: ScriptBuf::from_bytes(script_sig),
                sequence: Sequence::MAX,
                witness: Witness::default(),
            }],
            output: vec![TxOut {
                value: Amount::from_sat(50 * 100_000_000),
                script_pubkey: ScriptBuf::new(),
            }],
        }
    }

    fn spend(outpoint: OutPoint, sequence: Sequence, lock_time: LockTime) -> Transaction {
        Transaction {
            version: Version::TWO,
            lock_time,
            input: vec![TxIn {
                previous_output: outpoint,
                script_sig: ScriptBuf::new(),
                sequence,
                witness: Witness::default(),
            }],
            output: vec![TxOut {
                value: Amount::from_sat(1),
                script_pubkey: ScriptBuf::new(),
            }],
        }
    }

    fn insert_unspent(store: &RedbUtxoStore, outpoint: OutPoint, height: u32, mtp: u32) {
        store
            .apply(
                &[],
                &[(
                    (outpoint).into(),
                    Utxo {
                        value_sats: 1,
                        height,
                        is_coinbase: false,
                        last_touched: 0,
                        creation_mtp: mtp,
                        script_pubkey: Vec::new(),
                    },
                )],
            )
            .unwrap();
    }

    #[test]
    fn coinbase_creates_immature_utxo_and_returns_undo() {
        let (_dir, store) = store();
        let transaction = coinbase(vec![1, 2]);
        let applied = apply_transaction(&store, &transaction, 10, 100, 99, 100, 0).unwrap();
        let key = OutPointKey::from(OutPoint::new(transaction.compute_txid(), 0));
        assert!(store.get(key).unwrap().unwrap().is_coinbase);
        assert_eq!(applied.undo.created(), &[key]);
        store.undo(&applied.undo, 100, 60).unwrap();
        assert!(store.get(key).unwrap().is_none());
    }

    #[test]
    fn provably_unspendable_outputs_affect_value_but_not_the_utxo_set() {
        let (_dir, store) = store();
        let mut transaction = coinbase(vec![1, 2]);
        transaction.output = vec![
            TxOut {
                value: Amount::from_sat(7),
                script_pubkey: ScriptBuf::from_bytes(vec![0x6a, 0x01, 0x01]),
            },
            TxOut {
                value: Amount::from_sat(11),
                script_pubkey: ScriptBuf::new(),
            },
        ];
        let applied = apply_transaction(&store, &transaction, 10, 100, 99, 100, 0).unwrap();
        let unspendable = OutPointKey::from(OutPoint::new(transaction.compute_txid(), 0));
        let spendable = OutPointKey::from(OutPoint::new(transaction.compute_txid(), 1));

        assert_eq!(applied.output_value_sats, 18);
        assert!(store.get(unspendable).unwrap().is_none());
        assert!(store.get(spendable).unwrap().is_some());
        assert_eq!(applied.undo.created(), &[spendable]);

        assert!(is_unspendable(Script::from_bytes(&[0x6a])));
        assert!(is_unspendable(Script::from_bytes(&[0x51; 10_001])));
        assert!(!is_unspendable(Script::from_bytes(&[0x65])));
    }

    #[test]
    fn coinbase_script_size_is_checked() {
        let (_dir, store) = store();
        assert!(matches!(
            apply_transaction(&store, &coinbase(vec![1]), 10, 100, 99, 100, 0),
            Err(ChainstateError::CoinbaseScriptSize)
        ));
    }

    #[test]
    fn rejects_empty_noncoinbase_inputs_and_all_empty_outputs() {
        let (_dir, store) = store();
        let no_inputs = Transaction {
            version: Version::ONE,
            lock_time: LockTime::ZERO,
            input: vec![],
            output: vec![TxOut {
                value: Amount::from_sat(0),
                script_pubkey: ScriptBuf::new(),
            }],
        };
        assert!(matches!(
            apply_transaction(&store, &no_inputs, 10, 100, 99, 100, 0),
            Err(ChainstateError::NoInputs)
        ));
        let mut no_outputs = coinbase(vec![1, 2]);
        no_outputs.output.clear();
        assert!(matches!(
            apply_transaction(&store, &no_outputs, 10, 100, 99, 100, 0),
            Err(ChainstateError::NoOutputs)
        ));
    }

    #[test]
    fn rejects_duplicate_null_and_oversize_transaction_inputs() {
        let (_dir, store) = store();
        let previous = OutPoint::new(Txid::from_byte_array([8; 32]), 0);
        insert_unspent(&store, previous, 0, 0);
        let mut duplicate = spend(previous, Sequence::MAX, LockTime::ZERO);
        duplicate.input.push(duplicate.input[0].clone());
        assert!(matches!(
            apply_transaction(&store, &duplicate, 1, 0, 0, 0, 0),
            Err(ChainstateError::DuplicateInput(_))
        ));

        let mut null = spend(OutPoint::null(), Sequence::MAX, LockTime::ZERO);
        null.input.push(TxIn {
            previous_output: previous,
            script_sig: ScriptBuf::new(),
            sequence: Sequence::MAX,
            witness: Witness::default(),
        });
        assert!(matches!(
            apply_transaction(&store, &null, 1, 0, 0, 0, 0),
            Err(ChainstateError::NullPrevout)
        ));

        let mut oversize = spend(previous, Sequence::MAX, LockTime::ZERO);
        oversize.input[0].script_sig = ScriptBuf::from_bytes(vec![0; 1_000_001]);
        assert!(matches!(
            apply_transaction(&store, &oversize, 1, 0, 0, 0, 0),
            Err(ChainstateError::Oversize)
        ));
    }

    #[test]
    fn bip68_rejects_unsatisfied_height_and_time_locks() {
        let (_dir, store) = store();
        let height_outpoint = OutPoint::new(Txid::from_byte_array([1; 32]), 0);
        insert_unspent(&store, height_outpoint, 100, 1_000);
        let height_locked = spend(height_outpoint, Sequence::from_height(6), LockTime::ZERO);
        assert!(matches!(
            apply_transaction_with_context(&store, &height_locked, 105, 0, 2_000, 2_000, 0, true),
            Err(ChainstateError::RelativeHeightLock {
                minimum_height: 105,
                ..
            })
        ));

        let time_outpoint = OutPoint::new(Txid::from_byte_array([2; 32]), 0);
        insert_unspent(&store, time_outpoint, 100, 1_000);
        let time_locked = spend(
            time_outpoint,
            Sequence::from_512_second_intervals(2),
            LockTime::ZERO,
        );
        assert!(matches!(
            apply_transaction_with_context(&store, &time_locked, 200, 0, 2_023, 2_023, 0, true),
            Err(ChainstateError::RelativeTimeLock {
                minimum_mtp: 2_023,
                ..
            })
        ));
    }

    #[test]
    fn bip113_uses_strict_parent_mtp_for_absolute_time_locks() {
        let transaction = spend(
            OutPoint::new(Txid::from_byte_array([3; 32]), 0),
            Sequence::ZERO,
            LockTime::from_consensus(500_000_000),
        );
        assert!(!transaction_is_final(&transaction, 1, 500_000_000));
        assert!(transaction_is_final(&transaction, 1, 500_000_001));
    }

    #[test]
    fn counts_legacy_p2sh_and_witness_sigop_cost_like_core() {
        let mut legacy = coinbase(vec![1, 2]);
        legacy.output[0].script_pubkey = Builder::new()
            .push_opcode(OP_CHECKSIG)
            .push_opcode(OP_CHECKMULTISIG)
            .into_script();
        assert_eq!(legacy_sigop_cost(&legacy), 84);

        let redeem = Builder::new()
            .push_int(2)
            .push_opcode(OP_CHECKMULTISIG)
            .into_script();
        let mut p2sh_spend = spend(OutPoint::null(), Sequence::MAX, LockTime::ZERO);
        p2sh_spend.input[0].script_sig = ScriptBuf::from_bytes(vec![2, 0x52, 0xae]);
        let p2sh_prevout = Utxo {
            value_sats: 1,
            height: 0,
            is_coinbase: false,
            last_touched: 0,
            creation_mtp: 0,
            script_pubkey: redeem.to_p2sh().into_bytes(),
        };
        assert_eq!(
            transaction_sigop_cost(
                &p2sh_spend,
                std::slice::from_ref(&p2sh_prevout),
                bitcoinconsensus::VERIFY_P2SH,
            ),
            8
        );

        let mut witness_spend = spend(OutPoint::null(), Sequence::MAX, LockTime::ZERO);
        witness_spend.input[0].witness = Witness::from_slice(&[&[0x53, 0xae]]);
        let witness_prevout = Utxo {
            script_pubkey: [vec![0x00, 0x20], vec![0; 32]].concat(),
            ..p2sh_prevout
        };
        assert_eq!(
            transaction_sigop_cost(
                &witness_spend,
                &[witness_prevout],
                bitcoinconsensus::VERIFY_P2SH | bitcoinconsensus::VERIFY_WITNESS,
            ),
            3
        );
    }
}
