//! Local transaction relay policy kept separate from consensus validation.

use bitcoin::{OutPoint, Transaction};
use thiserror::Error;

/// Core 26's minimum non-witness transaction size used by standardness policy.
pub const MIN_STANDARD_TX_NONWITNESS_SIZE: usize = 65;
/// Core's maximum standard scriptSig size.
pub const MAX_STANDARD_SCRIPTSIG_SIZE: usize = 1_650;
/// Maximum standard OP_RETURN script size, including the opcode.
pub const MAX_STANDARD_OP_RETURN_SIZE: usize = 83;
/// Default minimum relay fee used by the local wallet admission path.
pub const DEFAULT_MIN_RELAY_FEE_SAT_VB: u64 = 1;

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
            || script.is_p2tr())
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

#[cfg(test)]
mod tests {
    use bitcoin::{
        Amount, OutPoint, ScriptBuf, Sequence, TxIn, TxOut, Txid, Witness,
        absolute::LockTime,
        blockdata::{opcodes, script::Builder},
        hashes::Hash,
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
}
