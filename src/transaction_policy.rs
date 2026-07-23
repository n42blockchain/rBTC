//! Local transaction relay policy kept separate from consensus validation.

use bitcoin::{OutPoint, Script, ScriptBuf, Transaction};
use thiserror::Error;

/// Core 26's minimum non-witness transaction size used by standardness policy.
pub const MIN_STANDARD_TX_NONWITNESS_SIZE: usize = 65;
/// Core's maximum standard scriptSig size.
pub const MAX_STANDARD_SCRIPTSIG_SIZE: usize = 1_650;
/// Maximum standard OP_RETURN script size, including the opcode.
pub const MAX_STANDARD_OP_RETURN_SIZE: usize = 83;
/// Default minimum relay fee used by the local wallet admission path.
pub const DEFAULT_MIN_RELAY_FEE_SAT_VB: u64 = 1;
/// Maximum accurate sigops in a standard P2SH redeem script.
pub const MAX_STANDARD_P2SH_SIGOPS: usize = 15;
/// Maximum witness script bytes in a standard P2WSH spend.
pub const MAX_STANDARD_P2WSH_SCRIPT_SIZE: usize = 3_600;
/// Maximum argument items before the witness script in a standard P2WSH spend.
pub const MAX_STANDARD_P2WSH_STACK_ITEMS: usize = 100;
/// Maximum bytes in each P2WSH argument stack item.
pub const MAX_STANDARD_P2WSH_STACK_ITEM_SIZE: usize = 80;
/// Maximum bytes in each tapscript argument stack item.
pub const MAX_STANDARD_TAPSCRIPT_STACK_ITEM_SIZE: usize = 80;
/// Maximum public keys in a standard bare multisig output.
pub const MAX_STANDARD_BARE_MULTISIG_KEYS: usize = 3;

/// A transaction failed local relay policy after consensus verification.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum TransactionPolicyError {
    /// Coinbase transactions never enter a mempool.
    #[error("coinbase transaction is not relayable")]
    Coinbase,
    /// Core 26 relays only versions 1 and 2 by default.
    #[error("transaction version is non-standard")]
    Version,
    /// The non-witness encoding is too small to relay.
    #[error("transaction non-witness size is below the standard minimum")]
    TooSmall,
    /// The transaction exceeds Core's standard weight ceiling.
    #[error("transaction exceeds the standard weight limit")]
    TooHeavy,
    /// An input scriptSig exceeds the standard byte limit.
    #[error("input {0} scriptSig exceeds the standard size limit")]
    ScriptSigSize(usize),
    /// A scriptSig contains a non-push opcode.
    #[error("input {0} scriptSig is not push-only")]
    ScriptSigNotPushOnly(usize),
    /// An output script is not one of Core's standard templates.
    #[error("output {0} script is non-standard")]
    OutputScript(usize),
    /// More than one data-carrier output is present.
    #[error("transaction contains more than one OP_RETURN output")]
    MultipleOpReturn,
    /// A data-carrier script exceeds Core's default standard size.
    #[error("output {0} OP_RETURN script exceeds the standard size")]
    OpReturnSize(usize),
    /// A spendable output is below the default dust threshold.
    #[error("output {index} value {value_sats} is below dust threshold {minimum_sats}")]
    Dust {
        /// Zero-based transaction output index.
        index: usize,
        /// Submitted value.
        value_sats: u64,
        /// Minimum relayable value for this script.
        minimum_sats: u64,
    },
    /// Fee is below the configured minimum relay rate.
    #[error("fee {fee_sats} is below minimum {minimum_sats}")]
    FeeRate {
        /// Exact fee derived from validated prevouts.
        fee_sats: u64,
        /// Required fee rounded up per virtual byte.
        minimum_sats: u64,
    },
    /// A transaction input conflicts with a locally retained transaction.
    #[error("input {0} conflicts with a locally retained transaction")]
    Conflict(OutPoint),
    /// A spent output is not a standard input template.
    #[error("input {0} spends a non-standard output script")]
    InputScript(usize),
    /// A P2SH input has no parseable final redeem-script push.
    #[error("input {0} has no parseable P2SH redeem script")]
    P2shRedeemScript(usize),
    /// A P2SH redeem script exceeds the accurate sigop ceiling.
    #[error("input {index} P2SH redeem script has {sigops} sigops; limit is {limit}")]
    P2shSigops {
        /// Zero-based input index.
        index: usize,
        /// Accurate redeem-script sigop count.
        sigops: usize,
        /// Standard policy ceiling.
        limit: usize,
    },
    /// Witness data is attached to a non-witness input.
    #[error("input {0} has witness data for a non-witness program")]
    UnexpectedWitness(usize),
    /// A P2WSH witness script exceeds the standard byte ceiling.
    #[error("input {0} P2WSH script exceeds the standard size limit")]
    WitnessScriptSize(usize),
    /// A P2WSH witness has too many argument items.
    #[error("input {0} P2WSH stack has too many items")]
    WitnessStackItems(usize),
    /// A P2WSH argument item exceeds the standard byte ceiling.
    #[error("input {input} P2WSH stack item {item} exceeds the standard size limit")]
    WitnessStackItemSize {
        /// Zero-based input index.
        input: usize,
        /// Zero-based witness argument index.
        item: usize,
    },
    /// Taproot annexes have no standard relay semantics.
    #[error("input {0} contains a non-standard Taproot annex")]
    TaprootAnnex(usize),
    /// A Taproot script-path spend has an empty control block.
    #[error("input {0} has an empty Taproot control block")]
    TaprootControlBlock(usize),
    /// A tapscript argument item exceeds the standard byte ceiling.
    #[error("input {input} tapscript stack item {item} exceeds the standard size limit")]
    TapscriptStackItemSize {
        /// Zero-based input index.
        input: usize,
        /// Zero-based tapscript argument index.
        item: usize,
    },
}

/// Applies bounded Core-like relay policy to a consensus-verified transaction.
///
/// Prevout-dependent script and witness policy remains the responsibility of
/// the caller's consensus/finalization path; this function deliberately does
/// not reinterpret signatures or mutate chainstate.
pub fn validate_standard_transaction(
    transaction: &Transaction,
    fee_sats: u64,
) -> Result<(), TransactionPolicyError> {
    if transaction.is_coinbase() {
        return Err(TransactionPolicyError::Coinbase);
    }
    if !transaction.version.is_standard() {
        return Err(TransactionPolicyError::Version);
    }
    if transaction.base_size() < MIN_STANDARD_TX_NONWITNESS_SIZE {
        return Err(TransactionPolicyError::TooSmall);
    }
    if transaction.weight() > Transaction::MAX_STANDARD_WEIGHT {
        return Err(TransactionPolicyError::TooHeavy);
    }
    for (index, input) in transaction.input.iter().enumerate() {
        if input.script_sig.len() > MAX_STANDARD_SCRIPTSIG_SIZE {
            return Err(TransactionPolicyError::ScriptSigSize(index));
        }
        if !input.script_sig.is_push_only() {
            return Err(TransactionPolicyError::ScriptSigNotPushOnly(index));
        }
    }
    let mut op_return = false;
    for (index, output) in transaction.output.iter().enumerate() {
        let script = &output.script_pubkey;
        if script.is_op_return() {
            if op_return {
                return Err(TransactionPolicyError::MultipleOpReturn);
            }
            if script.len() > MAX_STANDARD_OP_RETURN_SIZE {
                return Err(TransactionPolicyError::OpReturnSize(index));
            }
            op_return = true;
            continue;
        }
        if !(script.is_p2pk()
            || script.is_p2pkh()
            || script.is_p2sh()
            || script.is_p2wpkh()
            || script.is_p2wsh()
            || script.is_p2tr()
            || is_standard_bare_multisig(script))
        {
            return Err(TransactionPolicyError::OutputScript(index));
        }
        let minimum = script.minimal_non_dust().to_sat();
        if output.value.to_sat() < minimum {
            return Err(TransactionPolicyError::Dust {
                index,
                value_sats: output.value.to_sat(),
                minimum_sats: minimum,
            });
        }
    }
    let minimum_fee = u64::try_from(transaction.vsize())
        .unwrap_or(u64::MAX)
        .saturating_mul(DEFAULT_MIN_RELAY_FEE_SAT_VB);
    if fee_sats < minimum_fee {
        return Err(TransactionPolicyError::FeeRate {
            fee_sats,
            minimum_sats: minimum_fee,
        });
    }
    Ok(())
}

/// Applies Core 26 prevout-dependent input and witness standardness policy.
pub fn validate_standard_inputs(
    transaction: &Transaction,
    prevout_scripts: &[ScriptBuf],
) -> Result<(), TransactionPolicyError> {
    for (index, input) in transaction.input.iter().enumerate() {
        let prevout = prevout_scripts
            .get(index)
            .ok_or(TransactionPolicyError::InputScript(index))?;
        if !(prevout.is_p2pk()
            || prevout.is_p2pkh()
            || prevout.is_p2sh()
            || prevout.is_p2wpkh()
            || prevout.is_p2wsh()
            || prevout.is_p2tr()
            || is_standard_bare_multisig(prevout))
        {
            return Err(TransactionPolicyError::InputScript(index));
        }
        let redeem_script = prevout
            .is_p2sh()
            .then(|| pushed_redeem_script(&input.script_sig, index))
            .transpose()?;
        if let Some(redeem_script) = &redeem_script {
            let sigops = redeem_script.count_sigops();
            if sigops > MAX_STANDARD_P2SH_SIGOPS {
                return Err(TransactionPolicyError::P2shSigops {
                    index,
                    sigops,
                    limit: MAX_STANDARD_P2SH_SIGOPS,
                });
            }
        }
        if input.witness.is_empty() {
            continue;
        }
        let witness_program = redeem_script.as_deref().unwrap_or(prevout.as_script());
        if !witness_program.is_witness_program() {
            return Err(TransactionPolicyError::UnexpectedWitness(index));
        }
        if witness_program.is_p2wsh() {
            let witness_script = input
                .witness
                .last()
                .expect("non-empty witness has a final script");
            if witness_script.len() > MAX_STANDARD_P2WSH_SCRIPT_SIZE {
                return Err(TransactionPolicyError::WitnessScriptSize(index));
            }
            let arguments = input.witness.len().saturating_sub(1);
            if arguments > MAX_STANDARD_P2WSH_STACK_ITEMS {
                return Err(TransactionPolicyError::WitnessStackItems(index));
            }
            for (item, value) in input.witness.iter().take(arguments).enumerate() {
                if value.len() > MAX_STANDARD_P2WSH_STACK_ITEM_SIZE {
                    return Err(TransactionPolicyError::WitnessStackItemSize {
                        input: index,
                        item,
                    });
                }
            }
        }
        if witness_program.is_p2tr() && !prevout.is_p2sh() {
            let stack = input.witness.iter().collect::<Vec<_>>();
            if stack.len() >= 2
                && stack.last().is_some_and(|item| {
                    item.first() == Some(&bitcoin::taproot::TAPROOT_ANNEX_PREFIX)
                })
            {
                return Err(TransactionPolicyError::TaprootAnnex(index));
            }
            if stack.len() >= 2 {
                let control = stack.last().expect("script path has a control block");
                if control.is_empty() {
                    return Err(TransactionPolicyError::TaprootControlBlock(index));
                }
                if control[0] & 0xfe == bitcoin::taproot::TAPROOT_LEAF_TAPSCRIPT {
                    for (item, value) in stack[..stack.len() - 2].iter().enumerate() {
                        if value.len() > MAX_STANDARD_TAPSCRIPT_STACK_ITEM_SIZE {
                            return Err(TransactionPolicyError::TapscriptStackItemSize {
                                input: index,
                                item,
                            });
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

fn is_standard_bare_multisig(script: &Script) -> bool {
    let Ok(instructions) = script.instructions().collect::<Result<Vec<_>, _>>() else {
        return false;
    };
    let [
        bitcoin::script::Instruction::Op(required),
        public_keys @ ..,
        bitcoin::script::Instruction::Op(total),
        bitcoin::script::Instruction::Op(checkmultisig),
    ] = instructions.as_slice()
    else {
        return false;
    };
    let Some(required) = decode_positive_pushnum(*required) else {
        return false;
    };
    let Some(total) = decode_positive_pushnum(*total) else {
        return false;
    };
    *checkmultisig == bitcoin::opcodes::all::OP_CHECKMULTISIG
        && total == public_keys.len()
        && (1..=MAX_STANDARD_BARE_MULTISIG_KEYS).contains(&total)
        && required <= total
        && public_keys.iter().all(|instruction| {
            matches!(
                instruction,
                bitcoin::script::Instruction::PushBytes(bytes)
                    if matches!(bytes.len(), 33 | 65)
            )
        })
}

fn decode_positive_pushnum(opcode: bitcoin::opcodes::Opcode) -> Option<usize> {
    let value = opcode.to_u8();
    (0x51..=0x60)
        .contains(&value)
        .then(|| usize::from(value - 0x50))
}

fn pushed_redeem_script(
    script_sig: &Script,
    input: usize,
) -> Result<ScriptBuf, TransactionPolicyError> {
    script_sig
        .instructions()
        .last()
        .and_then(Result::ok)
        .and_then(|instruction| match instruction {
            bitcoin::script::Instruction::PushBytes(bytes) => {
                Some(ScriptBuf::from_bytes(bytes.as_bytes().to_vec()))
            }
            bitcoin::script::Instruction::Op(_) => None,
        })
        .ok_or(TransactionPolicyError::P2shRedeemScript(input))
}

#[cfg(test)]
mod tests {
    use bitcoin::{
        Amount, OutPoint, ScriptBuf, Sequence, TxIn, TxOut, Txid, Witness,
        absolute::LockTime,
        blockdata::{opcodes, script::Builder},
        hashes::Hash,
        script::PushBytesBuf,
        transaction::Version,
    };

    use super::*;

    fn standard_transaction() -> Transaction {
        Transaction {
            version: Version::TWO,
            lock_time: LockTime::ZERO,
            input: vec![TxIn {
                previous_output: OutPoint::new(Txid::from_byte_array([1; 32]), 0),
                script_sig: ScriptBuf::new(),
                sequence: Sequence::MAX,
                witness: Witness::from_slice(&[vec![1; 72], vec![2; 33]]),
            }],
            output: vec![TxOut {
                value: Amount::from_sat(1_000),
                script_pubkey: ScriptBuf::new_p2wpkh(&bitcoin::WPubkeyHash::from_byte_array(
                    [3; 20],
                )),
            }],
        }
    }

    fn bare_multisig(public_keys: usize) -> ScriptBuf {
        let mut builder = Builder::new().push_int(1);
        for _ in 0..public_keys {
            builder = builder.push_slice([2; 33]);
        }
        builder
            .push_int(i64::try_from(public_keys).unwrap())
            .push_opcode(opcodes::all::OP_CHECKMULTISIG)
            .into_script()
    }

    #[test]
    fn accepts_standard_segwit_transaction_at_minimum_fee() {
        let transaction = standard_transaction();
        validate_standard_transaction(&transaction, u64::try_from(transaction.vsize()).unwrap())
            .unwrap();
    }

    #[test]
    fn rejects_version_size_weight_and_scriptsig_policy() {
        let mut transaction = standard_transaction();
        transaction.version = Version(3);
        assert_eq!(
            validate_standard_transaction(&transaction, 1_000),
            Err(TransactionPolicyError::Version)
        );

        let mut transaction = standard_transaction();
        transaction.output[0].script_pubkey = ScriptBuf::new();
        assert_eq!(
            validate_standard_transaction(&transaction, 1_000),
            Err(TransactionPolicyError::TooSmall)
        );

        let mut transaction = standard_transaction();
        transaction.input[0].witness = Witness::from_slice(&[vec![0; 400_000]]);
        assert_eq!(
            validate_standard_transaction(&transaction, 1_000_000),
            Err(TransactionPolicyError::TooHeavy)
        );

        let mut transaction = standard_transaction();
        transaction.input[0].script_sig = ScriptBuf::from_bytes(vec![0x61; 1_651]);
        assert_eq!(
            validate_standard_transaction(&transaction, 10_000),
            Err(TransactionPolicyError::ScriptSigSize(0))
        );

        let mut transaction = standard_transaction();
        transaction.input[0].script_sig = Builder::new()
            .push_opcode(opcodes::all::OP_DUP)
            .into_script();
        assert_eq!(
            validate_standard_transaction(&transaction, 1_000),
            Err(TransactionPolicyError::ScriptSigNotPushOnly(0))
        );
    }

    #[test]
    fn rejects_nonstandard_dust_and_multiple_data_outputs() {
        let mut transaction = standard_transaction();
        transaction.output[0].script_pubkey = ScriptBuf::from_bytes(vec![0x61; 24]);
        assert_eq!(
            validate_standard_transaction(&transaction, 1_000),
            Err(TransactionPolicyError::OutputScript(0))
        );

        let mut transaction = standard_transaction();
        transaction.output[0].value = Amount::from_sat(1);
        assert!(matches!(
            validate_standard_transaction(&transaction, 1_000),
            Err(TransactionPolicyError::Dust { index: 0, .. })
        ));

        let mut transaction = standard_transaction();
        let data = Builder::new()
            .push_opcode(opcodes::all::OP_RETURN)
            .into_script();
        transaction.output = vec![
            TxOut {
                value: Amount::ZERO,
                script_pubkey: data.clone(),
            },
            TxOut {
                value: Amount::ZERO,
                script_pubkey: data,
            },
        ];
        assert_eq!(
            validate_standard_transaction(&transaction, 1_000),
            Err(TransactionPolicyError::MultipleOpReturn)
        );
    }

    #[test]
    fn accepts_bare_multisig_only_through_three_public_keys() {
        let mut transaction = standard_transaction();
        transaction.output[0].script_pubkey = bare_multisig(MAX_STANDARD_BARE_MULTISIG_KEYS);
        assert_eq!(validate_standard_transaction(&transaction, 1_000), Ok(()));

        transaction.output[0].script_pubkey = bare_multisig(MAX_STANDARD_BARE_MULTISIG_KEYS + 1);
        assert_eq!(
            validate_standard_transaction(&transaction, 1_000),
            Err(TransactionPolicyError::OutputScript(0))
        );

        let malformed_key = Builder::new()
            .push_int(1)
            .push_slice([2; 32])
            .push_int(1)
            .push_opcode(opcodes::all::OP_CHECKMULTISIG)
            .into_script();
        transaction.output[0].script_pubkey = malformed_key;
        assert_eq!(
            validate_standard_transaction(&transaction, 1_000),
            Err(TransactionPolicyError::OutputScript(0))
        );
    }

    #[test]
    fn rejects_fee_below_rounded_vsize_floor() {
        let transaction = standard_transaction();
        let minimum = u64::try_from(transaction.vsize()).unwrap();
        assert_eq!(
            validate_standard_transaction(&transaction, minimum - 1),
            Err(TransactionPolicyError::FeeRate {
                fee_sats: minimum - 1,
                minimum_sats: minimum,
            })
        );
    }

    #[test]
    fn enforces_accurate_p2sh_sigop_limit() {
        let mut transaction = standard_transaction();
        transaction.input[0].witness = Witness::new();
        for (sigops, expected) in [
            (15, Ok(())),
            (
                16,
                Err(TransactionPolicyError::P2shSigops {
                    index: 0,
                    sigops: 16,
                    limit: MAX_STANDARD_P2SH_SIGOPS,
                }),
            ),
        ] {
            let redeem_script = Builder::new()
                .push_int(sigops)
                .push_opcode(opcodes::all::OP_CHECKMULTISIG)
                .into_script();
            let redeem_push = PushBytesBuf::try_from(redeem_script.as_bytes().to_vec()).unwrap();
            transaction.input[0].script_sig = Builder::new().push_slice(redeem_push).into_script();
            let prevout = ScriptBuf::new_p2sh(&redeem_script.script_hash());
            assert_eq!(validate_standard_inputs(&transaction, &[prevout]), expected);
        }
    }

    #[test]
    fn enforces_p2wsh_script_stack_and_item_limits() {
        let mut transaction = standard_transaction();
        transaction.input[0].script_sig = ScriptBuf::new();
        let witness_script = ScriptBuf::from_bytes(vec![0; MAX_STANDARD_P2WSH_SCRIPT_SIZE]);
        let prevout = ScriptBuf::new_p2wsh(&witness_script.wscript_hash());
        let boundary_items =
            vec![vec![0; MAX_STANDARD_P2WSH_STACK_ITEM_SIZE]; MAX_STANDARD_P2WSH_STACK_ITEMS];
        let mut witness = boundary_items.clone();
        witness.push(witness_script.as_bytes().to_vec());
        transaction.input[0].witness = Witness::from_slice(&witness);
        assert_eq!(
            validate_standard_inputs(&transaction, std::slice::from_ref(&prevout)),
            Ok(())
        );

        let mut too_many = boundary_items.clone();
        too_many.push(Vec::new());
        too_many.push(witness_script.as_bytes().to_vec());
        transaction.input[0].witness = Witness::from_slice(&too_many);
        assert_eq!(
            validate_standard_inputs(&transaction, std::slice::from_ref(&prevout)),
            Err(TransactionPolicyError::WitnessStackItems(0))
        );

        transaction.input[0].witness = Witness::from_slice(&[
            vec![0; MAX_STANDARD_P2WSH_STACK_ITEM_SIZE + 1],
            witness_script.as_bytes().to_vec(),
        ]);
        assert_eq!(
            validate_standard_inputs(&transaction, std::slice::from_ref(&prevout)),
            Err(TransactionPolicyError::WitnessStackItemSize { input: 0, item: 0 })
        );

        let oversized_script = ScriptBuf::from_bytes(vec![0; MAX_STANDARD_P2WSH_SCRIPT_SIZE + 1]);
        transaction.input[0].witness = Witness::from_slice(&[oversized_script.as_bytes().to_vec()]);
        assert_eq!(
            validate_standard_inputs(&transaction, &[prevout]),
            Err(TransactionPolicyError::WitnessScriptSize(0))
        );
    }

    #[test]
    fn rejects_taproot_annex_and_oversized_tapscript_arguments() {
        let mut transaction = standard_transaction();
        transaction.input[0].script_sig = ScriptBuf::new();
        let mut program = vec![0x51, 0x20];
        program.extend([2; 32]);
        let prevout = ScriptBuf::from_bytes(program);
        assert!(prevout.is_p2tr());

        transaction.input[0].witness = Witness::from_slice(&[vec![1], vec![0x50]]);
        assert_eq!(
            validate_standard_inputs(&transaction, std::slice::from_ref(&prevout)),
            Err(TransactionPolicyError::TaprootAnnex(0))
        );

        transaction.input[0].witness = Witness::from_slice(&[vec![0x51], Vec::new()]);
        assert_eq!(
            validate_standard_inputs(&transaction, std::slice::from_ref(&prevout)),
            Err(TransactionPolicyError::TaprootControlBlock(0))
        );

        transaction.input[0].witness = Witness::from_slice(&[
            vec![0; MAX_STANDARD_TAPSCRIPT_STACK_ITEM_SIZE + 1],
            vec![0x51],
            vec![bitcoin::taproot::TAPROOT_LEAF_TAPSCRIPT],
        ]);
        assert_eq!(
            validate_standard_inputs(&transaction, &[prevout]),
            Err(TransactionPolicyError::TapscriptStackItemSize { input: 0, item: 0 })
        );
    }

    #[test]
    fn rejects_nonstandard_prevouts_and_unexpected_witness_data() {
        let transaction = standard_transaction();
        let nonstandard = Builder::new()
            .push_opcode(opcodes::all::OP_DUP)
            .into_script();
        assert_eq!(
            validate_standard_inputs(&transaction, &[nonstandard]),
            Err(TransactionPolicyError::InputScript(0))
        );

        let bare_key = Builder::new()
            .push_slice([2; 33])
            .push_opcode(opcodes::all::OP_CHECKSIG)
            .into_script();
        assert!(bare_key.is_p2pk());
        assert_eq!(
            validate_standard_inputs(&transaction, &[bare_key]),
            Err(TransactionPolicyError::UnexpectedWitness(0))
        );

        assert_eq!(
            validate_standard_inputs(
                &transaction,
                &[bare_multisig(MAX_STANDARD_BARE_MULTISIG_KEYS + 1)],
            ),
            Err(TransactionPolicyError::InputScript(0))
        );
    }
}
