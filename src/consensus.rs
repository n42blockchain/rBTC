//! Thin adapters around Bitcoin Core's consensus library.

use bitcoin::{Transaction, consensus::encode::serialize};
use thiserror::Error;

use crate::utxo::Utxo;

/// Script-validation failure returned by libbitcoinconsensus.
#[derive(Debug, Error)]
pub enum ConsensusError {
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
        .map(|utxo| bitcoinconsensus::Utxo {
            script_pubkey: utxo.script_pubkey.as_ptr(),
            script_pubkey_len: u32::try_from(utxo.script_pubkey.len())
                .expect("script length fits u32"),
            value: i64::try_from(utxo.value_sats).expect("Bitcoin amount fits i64"),
        })
        .collect::<Vec<_>>();
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
