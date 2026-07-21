//! Thin adapters around Bitcoin Core's consensus library.

use bitcoin::{Transaction, consensus::encode::serialize};
use thiserror::Error;

use crate::utxo::Utxo;

/// Script-validation failure returned by libbitcoinconsensus.
#[derive(Debug, Error)]
pub enum ConsensusError {
    /// A UTXO cannot be represented by libbitcoinconsensus's signed amount ABI.
    #[error("UTXO amount is outside the consensus ABI range")]
    AmountOutOfRange,
    /// A script is too large for the consensus ABI's length field.
    #[error("UTXO script is outside the consensus ABI range")]
    ScriptTooLarge,
    /// The supplied prevout vector is not aligned with transaction inputs.
    #[error("prevout count ({prevouts}) does not match input count ({inputs})")]
    PrevoutCount {
        /// Number of prevouts supplied.
        prevouts: usize,
        /// Number of transaction inputs.
        inputs: usize,
    },
    /// Bitcoin Core's consensus script engine rejected an input.
    #[error("script validation failed for input {input}: {source}")]
    Script {
        /// Input index that failed validation.
        input: usize,
        /// Concrete library error.
        #[source]
        source: bitcoinconsensus::Error,
    },
}

/// Validates every transaction input against its UTXO set using Bitcoin Core's script engine.
///
/// `height` is the candidate block height and controls activation flags. Callers
/// must separately enforce the full contextual rules (BIP34, BIP68, BIP113,
/// subsidy, sigops, weight, etc.) before committing a UTXO mutation.
pub fn verify_transaction_scripts(
    transaction: &Transaction,
    prevouts: &[Utxo],
    height: u32,
) -> Result<(), ConsensusError> {
    verify_transaction_scripts_with_flags(
        transaction,
        prevouts,
        bitcoinconsensus::height_to_flags(height),
    )
}

/// Validates every transaction input using explicitly selected deployment flags.
///
/// This network-neutral entry point lets the header/chain service derive flags
/// from the selected network's active BIP deployments.
pub fn verify_transaction_scripts_with_flags(
    transaction: &Transaction,
    prevouts: &[Utxo],
    flags: u32,
) -> Result<(), ConsensusError> {
    if transaction.input.len() != prevouts.len() {
        return Err(ConsensusError::PrevoutCount {
            prevouts: prevouts.len(),
            inputs: transaction.input.len(),
        });
    }
    let spent_outputs = prevouts
        .iter()
        .map(|utxo| -> Result<bitcoinconsensus::Utxo, ConsensusError> {
            Ok(bitcoinconsensus::Utxo {
                script_pubkey: utxo.script_pubkey.as_ptr(),
                script_pubkey_len: u32::try_from(utxo.script_pubkey.len())
                    .map_err(|_| ConsensusError::ScriptTooLarge)?,
                value: i64::try_from(utxo.value_sats)
                    .map_err(|_| ConsensusError::AmountOutOfRange)?,
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let raw_transaction = serialize(transaction);
    for (input, utxo) in prevouts.iter().enumerate() {
        bitcoinconsensus::verify_with_flags(
            &utxo.script_pubkey,
            utxo.value_sats,
            &raw_transaction,
            Some(&spent_outputs),
            input,
            flags,
        )
        .map_err(|source| ConsensusError::Script { input, source })?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use bitcoin::{
        Amount, OutPoint, ScriptBuf, Sequence, Transaction, TxIn, TxOut, Txid, Witness,
        absolute::LockTime,
        consensus::deserialize,
        hashes::Hash,
        hex::FromHex,
        script::Builder,
        secp256k1::{Keypair, Secp256k1, SecretKey, XOnlyPublicKey},
        taproot::{LeafVersion, TaprootBuilder},
        transaction::Version,
    };

    use super::*;

    fn transaction(encoded: &str) -> Transaction {
        deserialize(&Vec::<u8>::from_hex(encoded).unwrap()).unwrap()
    }

    fn prevout(script_pubkey: &str, value_sats: u64) -> Utxo {
        Utxo {
            value_sats,
            height: 0,
            is_coinbase: false,
            last_touched: 0,
            creation_mtp: 0,
            script_pubkey: Vec::<u8>::from_hex(script_pubkey).unwrap(),
        }
    }

    fn tapscript_spend(script: &ScriptBuf) -> (Transaction, Utxo, Vec<u8>) {
        let secp = Secp256k1::new();
        let secret = SecretKey::from_slice(&[1; 32]).unwrap();
        let keypair = Keypair::from_secret_key(&secp, &secret);
        let (internal_key, _) = XOnlyPublicKey::from_keypair(&keypair);
        let spend_info = TaprootBuilder::new()
            .add_leaf(0, script.clone())
            .unwrap()
            .finalize(&secp, internal_key)
            .unwrap();
        let control = spend_info
            .control_block(&(script.clone(), LeafVersion::TapScript))
            .unwrap()
            .serialize();
        let transaction = Transaction {
            version: Version::TWO,
            lock_time: LockTime::ZERO,
            input: vec![TxIn {
                previous_output: OutPoint::new(Txid::from_byte_array([2; 32]), 0),
                script_sig: ScriptBuf::new(),
                sequence: Sequence::MAX,
                witness: Witness::from_slice(&[script.as_bytes(), &control]),
            }],
            output: vec![TxOut {
                value: Amount::from_sat(900),
                script_pubkey: ScriptBuf::new(),
            }],
        };
        let spent = Utxo {
            value_sats: 1_000,
            height: 0,
            is_coinbase: false,
            last_touched: 0,
            creation_mtp: 0,
            script_pubkey: ScriptBuf::new_p2tr_tweaked(spend_info.output_key()).into_bytes(),
        };
        (transaction, spent, control)
    }

    #[test]
    fn core_26_bip141_bip143_valid_and_invalid_sighash_vectors() {
        // https://github.com/bitcoin/bitcoin/tree/v26.0/src/test/data
        // tx_{valid,invalid}.json:
        // "BIP143: correct/wrong sighash (without FindAndDelete)".
        let script_pubkey = "00209e1be07558ea5cc8e02ed1d80c0911048afad949affa36d5c3951e3159dbea19";
        let valid = transaction(concat!(
            "0100000000010169c12106097dc2e0526493ef67f21269fe888ef05c7a3a5daca",
            "b38e1ac8387f14c1d000000ffffffff01010000000000000000034830450220487f",
            "b382c4974de3f7d834c1b617fe15860828c7f96454490edd6d891556dcc9022100",
            "baf95feb48f845d5bfc9882eb6aeefa1bc3790e39f59eaa46ff7f15ae626c53e",
            "012102a9781d66b61fb5a7ef00ac5ad5bc6ffc78be7b44a566e3c87870e107936",
            "8df4c4aad4830450220487fb382c4974de3f7d834c1b617fe15860828c7f964544",
            "90edd6d891556dcc9022100baf95feb48f845d5bfc9882eb6aeefa1bc3790e39f",
            "59eaa46ff7f15ae626c53e0100000000",
        ));
        let invalid = transaction(concat!(
            "0100000000010169c12106097dc2e0526493ef67f21269fe888ef05c7a3a5daca",
            "b38e1ac8387f14c1d000000ffffffff01010000000000000000034830450220487f",
            "b382c4974de3f7d834c1b617fe15860828c7f96454490edd6d891556dcc9022100",
            "baf95feb48f845d5bfc9882eb6aeefa1bc3790e39f59eaa46ff7f15ae626c53e",
            "012102a9d7ed6e161f0e255c10bbfcca0128a9e2035c2c8da58899c54d22d3a31",
            "afdef4aad4830450220487fb382c4974de3f7d834c1b617fe15860828c7f964544",
            "90edd6d891556dcc9022100baf95feb48f845d5bfc9882eb6aeefa1bc3790e39f",
            "59eaa46ff7f15ae626c53e0100000000",
        ));
        let prevouts = [prevout(script_pubkey, 200_000)];
        let flags = bitcoinconsensus::VERIFY_P2SH | bitcoinconsensus::VERIFY_WITNESS;

        verify_transaction_scripts_with_flags(&valid, &prevouts, flags).unwrap();
        assert!(verify_transaction_scripts_with_flags(&invalid, &prevouts, flags).is_err());
        let wrong_amount = [prevout(script_pubkey, 199_999)];
        assert!(verify_transaction_scripts_with_flags(&valid, &wrong_amount, flags).is_err());
    }

    #[test]
    fn core_26_bip147_nulldummy_activation_vector() {
        // https://github.com/bitcoin/bitcoin/blob/v26.0/src/test/data/tx_valid.json
        // "dummy value set to something other than an empty string".
        let script_pubkey = concat!(
            "514104cc71eb30d653c0c3163990c47b976f3fb3f37cccdcbedb169a1dfef58bb",
            "fbfaff7d8a473e7e2e6d317b87bafe8bde97e3cf8f065dec022b51d11fcdd0d3",
            "48ac4410461cbdcc5409fb4b4d42b51d33381354d80e550078cb532a34bfa2fcf",
            "deb7d76519aecc62770f5b0e4ef8551946d8a540911abe3e7854a26f39f58b25",
            "c15342af52ae",
        );
        let spending = transaction(concat!(
            "0100000001b14bdcbc3e01bdaad36cc08e81e69c82e1060bc14e518db2b49aa43a",
            "d90ba260000000004a01ff47304402203f16c6f40162ab686621ef3000b04e75418",
            "a0c0cb2d8aebeac894ae360ac1e780220ddc15ecdfc3507ac48e1681a33eb6099",
            "6631bf6bf5bc0a0682c4db743ce7ca2b01ffffffff0140420f00000000001976a9",
            "14660d4ef3a743e3e696ad990364e555c271ad504b88ac00000000",
        ));
        let prevouts = [prevout(script_pubkey, 0)];

        verify_transaction_scripts_with_flags(&spending, &prevouts, bitcoinconsensus::VERIFY_NONE)
            .unwrap();
        assert!(
            verify_transaction_scripts_with_flags(
                &spending,
                &prevouts,
                bitcoinconsensus::VERIFY_NULLDUMMY,
            )
            .is_err()
        );
    }

    #[test]
    fn core_26_bip341_bip342_tapscript_commitment_and_execution() {
        let true_script = Builder::new().push_int(1).into_script();
        let false_script = Builder::new().push_int(0).into_script();
        let (valid, spent, true_control) = tapscript_spend(&true_script);
        let flags = bitcoinconsensus::VERIFY_ALL_PRE_TAPROOT | bitcoinconsensus::VERIFY_TAPROOT;

        verify_transaction_scripts_with_flags(&valid, &[spent.clone()], flags).unwrap();

        let mut uncommitted_script = valid;
        uncommitted_script.input[0].witness =
            Witness::from_slice(&[false_script.as_bytes(), &true_control]);
        assert!(
            verify_transaction_scripts_with_flags(&uncommitted_script, &[spent], flags).is_err()
        );

        let (false_transaction, false_prevout, _) = tapscript_spend(&false_script);
        assert!(
            verify_transaction_scripts_with_flags(&false_transaction, &[false_prevout], flags)
                .is_err()
        );
    }
}
