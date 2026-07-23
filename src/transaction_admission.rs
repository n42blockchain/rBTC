//! Bounded local admission for independently received transactions.
//!
//! This is deliberately a single-transaction pool: every input must already
//! exist in the active chainstate. Package admission and replacement are
//! separate policy layers and are not implied here.

use std::collections::{BTreeMap, VecDeque};

use bitcoin::{OutPoint, Transaction, Txid, Wtxid, consensus::encode::serialize};
use thiserror::Error;

use crate::{
    chainstate::{ChainstateError, ValidatedTransaction, validate_transaction_with_context},
    transaction_policy::{TransactionPolicyError, validate_standard_transaction},
    utxo::UtxoStore,
};

/// Maximum number of independently admitted peer transactions.
pub const MAX_ADMITTED_TRANSACTIONS: usize = 64;
/// Maximum witness-serialized bytes retained by the local admission pool.
pub const MAX_ADMITTED_TRANSACTION_BYTES: usize = 4_000_000;

/// Chain context used for read-only transaction validation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TransactionAdmissionContext {
    /// Height of the next candidate block.
    pub height: u32,
    /// Median time past of the active parent.
    pub parent_mtp: u32,
    /// Explicit consensus script flags active for the next block.
    pub script_flags: u32,
    /// Whether BIP68/BIP113 relative and absolute lock semantics are active.
    pub csv_active: bool,
}

/// Result of one local admission attempt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransactionAdmissionOutcome {
    /// A new transaction passed consensus and standardness checks.
    Accepted {
        /// Accepted transaction ID.
        txid: Txid,
        /// Number of oldest entries evicted to preserve hard resource bounds.
        evicted: usize,
    },
    /// The same witness transaction was already retained.
    AlreadyPresent(Wtxid),
}

/// A transaction failed local admission.
#[derive(Debug, Error)]
pub enum TransactionAdmissionError {
    /// Consensus, finality, maturity, or chainstate lookup failed.
    #[error("chainstate validation: {0}")]
    Chainstate(#[from] ChainstateError),
    /// Local standard relay policy rejected the transaction.
    #[error("relay policy: {0}")]
    Policy(#[from] TransactionPolicyError),
}

struct AdmittedTransaction {
    transaction: Transaction,
    serialized_len: usize,
}

/// Wire-ordered, conflict-indexed, hard-bounded local transaction pool.
#[derive(Default)]
pub struct TransactionAdmissionPool {
    entries: VecDeque<AdmittedTransaction>,
    spent: BTreeMap<OutPoint, Txid>,
    retained_bytes: usize,
}

impl TransactionAdmissionPool {
    /// Returns the number of retained transactions.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns whether no transactions are retained.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Returns witness-serialized bytes currently charged to the pool.
    #[must_use]
    pub const fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }

    /// Clones admitted transactions in oldest-to-newest order.
    #[must_use]
    pub fn snapshot(&self) -> Vec<Transaction> {
        self.entries
            .iter()
            .map(|entry| entry.transaction.clone())
            .collect()
    }

    /// Validates and retains one transaction without mutating chainstate.
    ///
    /// Inputs must be confirmed UTXOs. An input already reserved by another
    /// retained transaction is rejected; replacement is intentionally deferred
    /// to the RBF layer. Oldest entries are evicted only after the new
    /// transaction has passed every validation gate.
    pub fn admit<S: UtxoStore>(
        &mut self,
        store: &S,
        transaction: Transaction,
        context: TransactionAdmissionContext,
    ) -> Result<TransactionAdmissionOutcome, TransactionAdmissionError> {
        let wtxid = transaction.compute_wtxid();
        if self
            .entries
            .iter()
            .any(|entry| entry.transaction.compute_wtxid() == wtxid)
        {
            return Ok(TransactionAdmissionOutcome::AlreadyPresent(wtxid));
        }

        let ValidatedTransaction {
            txid,
            input_value_sats,
            output_value_sats,
            ..
        } = validate_transaction_with_context(
            store,
            &transaction,
            context.height,
            context.parent_mtp,
            context.parent_mtp,
            context.script_flags,
            context.csv_active,
        )?;
        let fee_sats = input_value_sats
            .checked_sub(output_value_sats)
            .expect("consensus validation rejects transaction inflation");
        validate_standard_transaction(&transaction, fee_sats)?;
        if let Some(conflict) = transaction
            .input
            .iter()
            .map(|input| input.previous_output)
            .find(|outpoint| self.spent.contains_key(outpoint))
        {
            return Err(TransactionPolicyError::Conflict(conflict).into());
        }

        let serialized_len = serialize(&transaction).len();
        let mut evicted = 0;
        while self.entries.len() >= MAX_ADMITTED_TRANSACTIONS
            || self.retained_bytes.saturating_add(serialized_len) > MAX_ADMITTED_TRANSACTION_BYTES
        {
            let Some(oldest) = self.entries.pop_front() else {
                break;
            };
            self.remove_indexes(&oldest);
            evicted += 1;
        }
        for input in &transaction.input {
            self.spent.insert(input.previous_output, txid);
        }
        self.retained_bytes = self
            .retained_bytes
            .checked_add(serialized_len)
            .expect("bounded admitted transaction bytes fit usize");
        self.entries.push_back(AdmittedTransaction {
            transaction,
            serialized_len,
        });
        Ok(TransactionAdmissionOutcome::Accepted { txid, evicted })
    }

    /// Revalidates every entry after an active-chain change.
    ///
    /// Transactions mined, conflicted, made immature, or otherwise invalid in
    /// the new context are removed. Surviving insertion order is preserved.
    pub fn reconcile<S: UtxoStore>(
        &mut self,
        store: &S,
        context: TransactionAdmissionContext,
    ) -> usize {
        let previous = self
            .entries
            .drain(..)
            .map(|entry| entry.transaction)
            .collect::<Vec<_>>();
        self.spent.clear();
        self.retained_bytes = 0;
        let before = previous.len();
        for transaction in previous {
            let _ = self.admit(store, transaction, context);
        }
        before.saturating_sub(self.entries.len())
    }

    fn remove_indexes(&mut self, entry: &AdmittedTransaction) {
        let txid = entry.transaction.compute_txid();
        for input in &entry.transaction.input {
            if self.spent.get(&input.previous_output) == Some(&txid) {
                self.spent.remove(&input.previous_output);
            }
        }
        self.retained_bytes = self
            .retained_bytes
            .checked_sub(entry.serialized_len)
            .expect("admitted transaction charge matches retained byte total");
    }
}

#[cfg(test)]
mod tests {
    use bitcoin::{
        Amount, ScriptBuf, Sequence, Transaction, TxIn, TxOut, Txid, Witness, absolute::LockTime,
        blockdata::script::Builder, hashes::Hash, opcodes, transaction::Version,
    };
    use tempfile::TempDir;

    use crate::utxo::{OutPointKey, RedbUtxoStore, Utxo};

    use super::*;

    fn context() -> TransactionAdmissionContext {
        TransactionAdmissionContext {
            height: 200,
            parent_mtp: 1_700_000_000,
            script_flags: bitcoinconsensus::VERIFY_P2SH | bitcoinconsensus::VERIFY_WITNESS,
            csv_active: true,
        }
    }

    fn store() -> (TempDir, RedbUtxoStore) {
        let directory = TempDir::new().unwrap();
        let store = RedbUtxoStore::open(directory.path().join("chainstate.redb")).unwrap();
        (directory, store)
    }

    fn spend(index: u8) -> (OutPoint, Utxo, Transaction) {
        let witness_script = Builder::new().push_opcode(opcodes::OP_TRUE).into_script();
        let outpoint = OutPoint::new(Txid::from_byte_array([index; 32]), 0);
        let utxo = Utxo {
            value_sats: 100_000,
            height: 1,
            is_coinbase: false,
            last_touched: 0,
            creation_mtp: 1,
            script_pubkey: ScriptBuf::new_p2wsh(&witness_script.wscript_hash()).into_bytes(),
        };
        let transaction = Transaction {
            version: Version::TWO,
            lock_time: LockTime::ZERO,
            input: vec![TxIn {
                previous_output: outpoint,
                script_sig: ScriptBuf::new(),
                sequence: Sequence::MAX,
                witness: Witness::from_slice(&[witness_script.as_bytes()]),
            }],
            output: vec![TxOut {
                value: Amount::from_sat(90_000),
                script_pubkey: ScriptBuf::new_p2wsh(&witness_script.wscript_hash()),
            }],
        };
        (outpoint, utxo, transaction)
    }

    #[test]
    fn admission_checks_scripts_and_policy_without_mutating_chainstate() {
        let (_directory, store) = store();
        let (outpoint, utxo, transaction) = spend(1);
        store.apply(&[], &[(outpoint.into(), utxo)]).unwrap();
        let txid = transaction.compute_txid();
        let mut pool = TransactionAdmissionPool::default();

        let mut invalid = transaction.clone();
        invalid.input[0].witness = Witness::new();
        assert!(matches!(
            pool.admit(&store, invalid, context()),
            Err(TransactionAdmissionError::Chainstate(
                ChainstateError::Script(_)
            ))
        ));
        assert!(store.get(OutPointKey::from(outpoint)).unwrap().is_some());
        assert_eq!(
            pool.admit(&store, transaction.clone(), context()).unwrap(),
            TransactionAdmissionOutcome::Accepted { txid, evicted: 0 }
        );
        assert_eq!(
            pool.admit(&store, transaction, context()).unwrap(),
            TransactionAdmissionOutcome::AlreadyPresent(pool.snapshot()[0].compute_wtxid())
        );
        assert!(store.get(OutPointKey::from(outpoint)).unwrap().is_some());
        assert_eq!(pool.len(), 1);
        assert!(pool.retained_bytes() > 0);
    }

    #[test]
    fn conflicting_spend_is_rejected_until_replacement_policy_exists() {
        let (_directory, store) = store();
        let (outpoint, utxo, first) = spend(2);
        store.apply(&[], &[(outpoint.into(), utxo)]).unwrap();
        let mut second = first.clone();
        second.output[0].value = Amount::from_sat(89_000);
        let mut pool = TransactionAdmissionPool::default();
        pool.admit(&store, first, context()).unwrap();

        assert!(matches!(
            pool.admit(&store, second, context()),
            Err(TransactionAdmissionError::Policy(
                TransactionPolicyError::Conflict(conflict)
            )) if conflict == outpoint
        ));
        assert_eq!(pool.len(), 1);
    }

    #[test]
    fn capacity_evicts_oldest_and_reconcile_removes_mined_transactions() {
        let (_directory, store) = store();
        let mut transactions = Vec::new();
        for index in 1..=u8::try_from(MAX_ADMITTED_TRANSACTIONS + 1).unwrap() {
            let (outpoint, utxo, transaction) = spend(index);
            store.apply(&[], &[(outpoint.into(), utxo)]).unwrap();
            transactions.push((outpoint, transaction));
        }
        let first_txid = transactions[0].1.compute_txid();
        let last_txid = transactions.last().unwrap().1.compute_txid();
        let mut pool = TransactionAdmissionPool::default();
        for (_, transaction) in &transactions {
            pool.admit(&store, transaction.clone(), context()).unwrap();
        }

        assert_eq!(pool.len(), MAX_ADMITTED_TRANSACTIONS);
        assert!(
            !pool
                .snapshot()
                .iter()
                .any(|transaction| transaction.compute_txid() == first_txid)
        );
        assert_eq!(pool.snapshot().last().unwrap().compute_txid(), last_txid);

        let mined = transactions.last().unwrap().0;
        store.apply(&[mined.into()], &[]).unwrap();
        assert_eq!(pool.reconcile(&store, context()), 1);
        assert!(
            !pool
                .snapshot()
                .iter()
                .any(|transaction| transaction.compute_txid() == last_txid)
        );
    }
}
