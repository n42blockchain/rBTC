//! Transaction-to-UTXO application with consensus-critical accounting checks.

use bitcoin::Transaction;
use thiserror::Error;

use crate::{
    consensus::{ConsensusError, verify_transaction_scripts_with_flags},
    utxo::{OutPointKey, Utxo, UtxoError, UtxoStore, UtxoUndo},
};

/// Coinbase outputs require this many confirmations before they are spendable.
pub const COINBASE_MATURITY: u32 = 100;

/// A successful transaction application and its reorg data.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AppliedTransaction {
    /// Transaction ID whose outputs were created.
    pub txid: bitcoin::Txid,
    /// Sum of consumed inputs, in satoshis (zero for coinbase).
    pub input_value_sats: u64,
    /// Sum of created outputs, in satoshis.
    pub output_value_sats: u64,
    /// Undo data to apply if this transaction's containing block disconnects.
    pub undo: UtxoUndo,
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
}

/// Applies one transaction to a UTXO store after accounting and script checks.
///
/// `script_flags` must reflect the candidate block's active BIP deployments.
/// Coinbase subsidy and the one-coinbase-per-block rule are enforced by the
/// block-level validator, which also supplies the correct height and flags.
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
    script_flags: u32,
) -> Result<AppliedTransaction, ChainstateError> {
    let txid = transaction.compute_txid();
    let output_value = transaction.output.iter().try_fold(0_u64, |sum, output| {
        sum.checked_add(output.value.to_sat())
            .ok_or(ChainstateError::ValueOverflow)
    })?;
    if transaction.is_coinbase() {
        let script_len = transaction.input[0].script_sig.len();
        if !(2..=100).contains(&script_len) {
            return Err(ChainstateError::CoinbaseScriptSize);
        }
        let created = created_outputs(transaction, height, now, true);
        return Ok(AppliedTransaction {
            txid,
            input_value_sats: 0,
            output_value_sats: output_value,
            undo: store.apply_with_undo(&[], &created)?,
        });
    }

    let mut spent = Vec::with_capacity(transaction.input.len());
    let mut prevouts = Vec::with_capacity(transaction.input.len());
    let mut input_value = 0_u64;
    for input in &transaction.input {
        let outpoint = OutPointKey::from(input.previous_output);
        let utxo = store.get(outpoint)?.ok_or(UtxoError::Missing(outpoint))?;
        if utxo.is_coinbase {
            let matures_at = utxo.height.saturating_add(COINBASE_MATURITY);
            if height < matures_at {
                return Err(ChainstateError::ImmatureCoinbase {
                    outpoint,
                    matures_at,
                });
            }
        }
        input_value = input_value
            .checked_add(utxo.value_sats)
            .ok_or(ChainstateError::ValueOverflow)?;
        spent.push(outpoint);
        prevouts.push(utxo);
    }
    if output_value > input_value {
        return Err(ChainstateError::Inflation);
    }
    verify_transaction_scripts_with_flags(transaction, &prevouts, script_flags)?;
    let created = created_outputs(transaction, height, now, false);
    Ok(AppliedTransaction {
        txid,
        input_value_sats: input_value,
        output_value_sats: output_value,
        undo: store.apply_with_undo(&spent, &created)?,
    })
}

fn created_outputs(
    transaction: &Transaction,
    height: u32,
    now: u64,
    is_coinbase: bool,
) -> Vec<(OutPointKey, Utxo)> {
    let txid = transaction.compute_txid();
    transaction
        .output
        .iter()
        .enumerate()
        .map(|(vout, output)| {
            let vout = u32::try_from(vout).expect("transaction output count fits u32");
            (
                bitcoin::OutPoint::new(txid, vout).into(),
                Utxo {
                    value_sats: output.value.to_sat(),
                    height,
                    is_coinbase,
                    last_touched: now,
                    script_pubkey: output.script_pubkey.as_bytes().to_vec(),
                },
            )
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use bitcoin::{
        Amount, OutPoint, ScriptBuf, Sequence, Transaction, TxIn, TxOut, Witness,
        absolute::LockTime, transaction::Version,
    };
    use tempfile::TempDir;

    use super::*;
    use crate::utxo::RedbUtxoStore;

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

    #[test]
    fn coinbase_creates_immature_utxo_and_returns_undo() {
        let (_dir, store) = store();
        let transaction = coinbase(vec![1, 2]);
        let applied = apply_transaction(&store, &transaction, 10, 100, 0).unwrap();
        let key = OutPointKey::from(OutPoint::new(transaction.compute_txid(), 0));
        assert!(store.get(key).unwrap().unwrap().is_coinbase);
        assert_eq!(applied.undo.created(), &[key]);
        store.undo(&applied.undo, 100, 60).unwrap();
        assert!(store.get(key).unwrap().is_none());
    }

    #[test]
    fn coinbase_script_size_is_checked() {
        let (_dir, store) = store();
        assert!(matches!(
            apply_transaction(&store, &coinbase(vec![1]), 10, 100, 0),
            Err(ChainstateError::CoinbaseScriptSize)
        ));
    }
}
