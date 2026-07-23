//! Bounded local admission for independently received transactions.
//!
//! Confirmed inputs and outputs of already-admitted parents are available to a
//! bounded package overlay. Replacement remains a separate policy layer.

use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    sync::Mutex,
};

use bitcoin::{OutPoint, Transaction, Txid, Wtxid, consensus::encode::serialize};
use thiserror::Error;

use crate::{
    chainstate::{ChainstateError, apply_transaction_with_context},
    transaction_policy::{TransactionPolicyError, validate_standard_transaction},
    utxo::{OutPointKey, TierStats, Utxo, UtxoError, UtxoStore, UtxoUndo},
};

/// Maximum number of locally admitted peer transactions.
pub const MAX_ADMITTED_TRANSACTIONS: usize = 64;
/// Maximum witness-serialized bytes retained by the local admission pool.
pub const MAX_ADMITTED_TRANSACTION_BYTES: usize = 4_000_000;
/// Maximum transactions accepted as one dependency-connected package.
pub const MAX_PACKAGE_TRANSACTIONS: usize = 25;
/// Core-like maximum aggregate virtual size accepted for one package.
pub const MAX_PACKAGE_VBYTES: usize = 101_000;

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
    /// An empty package has no admission meaning.
    #[error("transaction package is empty")]
    EmptyPackage,
    /// The package exceeds the bounded transaction count.
    #[error("transaction package contains {count} entries; limit is {limit}")]
    TooManyPackageTransactions {
        /// Submitted entry count.
        count: usize,
        /// Hard package entry limit.
        limit: usize,
    },
    /// The package exceeds the bounded aggregate virtual size.
    #[error("transaction package virtual size {vbytes} exceeds limit {limit}")]
    PackageTooLarge {
        /// Submitted aggregate virtual size.
        vbytes: usize,
        /// Hard virtual-size limit.
        limit: usize,
    },
    /// Two package entries have the same non-witness transaction ID.
    #[error("transaction package contains duplicate txid {0}")]
    DuplicatePackageTransaction(Txid),
    /// Package dependencies cannot be ordered parent before child.
    #[error("transaction package dependencies are cyclic")]
    CyclicPackage,
    /// Capacity cannot be recovered without evicting the new package or one of its ancestors.
    #[error("transaction package cannot fit without evicting a required ancestor")]
    PackageCapacity,
}

/// Result of an atomic package admission attempt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PackageAdmissionOutcome {
    /// Newly accepted transaction IDs in parent-before-child order.
    pub accepted: Vec<Txid>,
    /// Exact witness transactions already present in the pool.
    pub already_present: usize,
    /// Existing oldest transactions and descendants evicted for capacity.
    pub evicted: usize,
}

#[derive(Clone)]
struct AdmittedTransaction {
    transaction: Transaction,
    serialized_len: usize,
}

/// Wire-ordered, conflict-indexed, hard-bounded local transaction pool.
#[derive(Clone, Default)]
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
    /// Inputs may be confirmed UTXOs or outputs of already-admitted parents.
    /// An input already reserved by another retained transaction is rejected;
    /// replacement is intentionally deferred to the RBF layer.
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
        let txid = transaction.compute_txid();
        let outcome = self.admit_package(store, vec![transaction], context)?;
        Ok(TransactionAdmissionOutcome::Accepted {
            txid,
            evicted: outcome.evicted,
        })
    }

    /// Atomically validates a dependency-connected transaction package.
    ///
    /// Package order is untrusted: entries are deduplicated and topologically
    /// ordered before a private UTXO overlay applies parents ahead of children.
    /// Any consensus, policy, missing-parent, conflict, or resource failure
    /// leaves the pool byte-for-byte unchanged.
    pub fn admit_package<S: UtxoStore>(
        &mut self,
        store: &S,
        transactions: Vec<Transaction>,
        context: TransactionAdmissionContext,
    ) -> Result<PackageAdmissionOutcome, TransactionAdmissionError> {
        validate_package_bounds(&transactions)?;
        let mut candidate = self.clone();
        let outcome = candidate.admit_package_inner(store, transactions, context)?;
        *self = candidate;
        Ok(outcome)
    }

    fn admit_package_inner<S: UtxoStore>(
        &mut self,
        store: &S,
        transactions: Vec<Transaction>,
        context: TransactionAdmissionContext,
    ) -> Result<PackageAdmissionOutcome, TransactionAdmissionError> {
        let overlay = AdmissionUtxoOverlay::new(store);
        for entry in &self.entries {
            apply_to_overlay(&overlay, &entry.transaction, context)?;
        }

        let mut already_present = 0;
        let mut package = BTreeMap::new();
        for transaction in transactions {
            let wtxid = transaction.compute_wtxid();
            if self
                .entries
                .iter()
                .any(|entry| entry.transaction.compute_wtxid() == wtxid)
            {
                already_present += 1;
                continue;
            }
            let txid = transaction.compute_txid();
            if self
                .entries
                .iter()
                .any(|entry| entry.transaction.compute_txid() == txid)
                || package.insert(txid, transaction).is_some()
            {
                return Err(TransactionAdmissionError::DuplicatePackageTransaction(txid));
            }
        }
        if package.is_empty() {
            return Ok(PackageAdmissionOutcome {
                accepted: Vec::new(),
                already_present,
                evicted: 0,
            });
        }

        let ordered = topological_package_order(package)?;
        let mut package_spent = BTreeSet::new();
        let mut accepted = Vec::with_capacity(ordered.len());
        for transaction in ordered {
            if let Some(conflict) = transaction
                .input
                .iter()
                .map(|input| input.previous_output)
                .find(|outpoint| {
                    self.spent.contains_key(outpoint) || !package_spent.insert(*outpoint)
                })
            {
                return Err(TransactionPolicyError::Conflict(conflict).into());
            }
            let txid = transaction.compute_txid();
            apply_to_overlay(&overlay, &transaction, context)?;
            let serialized_len = serialize(&transaction).len();
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
            accepted.push(txid);
        }
        let protected = accepted.iter().copied().collect::<BTreeSet<_>>();
        let evicted = self.evict_to_capacity(&protected)?;
        Ok(PackageAdmissionOutcome {
            accepted,
            already_present,
            evicted,
        })
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

    fn evict_to_capacity(
        &mut self,
        protected: &BTreeSet<Txid>,
    ) -> Result<usize, TransactionAdmissionError> {
        let mut evicted = 0;
        while self.entries.len() > MAX_ADMITTED_TRANSACTIONS
            || self.retained_bytes > MAX_ADMITTED_TRANSACTION_BYTES
        {
            let removed = self
                .entries
                .iter()
                .map(|entry| self.descendant_closure(entry.transaction.compute_txid()))
                .find(|closure| closure.is_disjoint(protected))
                .ok_or(TransactionAdmissionError::PackageCapacity)?;
            let before = self.entries.len();
            self.entries
                .retain(|entry| !removed.contains(&entry.transaction.compute_txid()));
            evicted += before.saturating_sub(self.entries.len());
            self.rebuild_indexes();
        }
        Ok(evicted)
    }

    fn descendant_closure(&self, txid: Txid) -> BTreeSet<Txid> {
        let mut removed = BTreeSet::from([txid]);
        loop {
            let descendants = self
                .entries
                .iter()
                .filter(|entry| {
                    entry
                        .transaction
                        .input
                        .iter()
                        .any(|input| removed.contains(&input.previous_output.txid))
                })
                .map(|entry| entry.transaction.compute_txid())
                .filter(|txid| !removed.contains(txid))
                .collect::<Vec<_>>();
            if descendants.is_empty() {
                break;
            }
            removed.extend(descendants);
        }
        removed
    }

    fn rebuild_indexes(&mut self) {
        self.spent.clear();
        self.retained_bytes = 0;
        for entry in &self.entries {
            let txid = entry.transaction.compute_txid();
            for input in &entry.transaction.input {
                self.spent.insert(input.previous_output, txid);
            }
            self.retained_bytes = self
                .retained_bytes
                .checked_add(entry.serialized_len)
                .expect("bounded admitted transaction bytes fit usize");
        }
    }
}

fn validate_package_bounds(transactions: &[Transaction]) -> Result<(), TransactionAdmissionError> {
    if transactions.is_empty() {
        return Err(TransactionAdmissionError::EmptyPackage);
    }
    if transactions.len() > MAX_PACKAGE_TRANSACTIONS {
        return Err(TransactionAdmissionError::TooManyPackageTransactions {
            count: transactions.len(),
            limit: MAX_PACKAGE_TRANSACTIONS,
        });
    }
    let vbytes = transactions
        .iter()
        .try_fold(0_usize, |total, transaction| {
            total.checked_add(transaction.vsize())
        })
        .unwrap_or(usize::MAX);
    if vbytes > MAX_PACKAGE_VBYTES {
        return Err(TransactionAdmissionError::PackageTooLarge {
            vbytes,
            limit: MAX_PACKAGE_VBYTES,
        });
    }
    Ok(())
}

fn topological_package_order(
    mut package: BTreeMap<Txid, Transaction>,
) -> Result<Vec<Transaction>, TransactionAdmissionError> {
    let mut ordered = Vec::with_capacity(package.len());
    while !package.is_empty() {
        let ready = package
            .iter()
            .find_map(|(txid, transaction)| {
                transaction
                    .input
                    .iter()
                    .all(|input| !package.contains_key(&input.previous_output.txid))
                    .then_some(*txid)
            })
            .ok_or(TransactionAdmissionError::CyclicPackage)?;
        ordered.push(
            package
                .remove(&ready)
                .expect("topological package entry remains available"),
        );
    }
    Ok(ordered)
}

fn apply_to_overlay<S: UtxoStore>(
    overlay: &AdmissionUtxoOverlay<'_, S>,
    transaction: &Transaction,
    context: TransactionAdmissionContext,
) -> Result<(), TransactionAdmissionError> {
    let applied = apply_transaction_with_context(
        overlay,
        transaction,
        context.height,
        0,
        context.parent_mtp,
        context.parent_mtp,
        context.script_flags,
        context.csv_active,
    )?;
    let fee_sats = applied
        .input_value_sats
        .checked_sub(applied.output_value_sats)
        .expect("consensus validation rejects transaction inflation");
    validate_standard_transaction(transaction, fee_sats)?;
    Ok(())
}

struct AdmissionUtxoOverlay<'a, S> {
    base: &'a S,
    current: Mutex<BTreeMap<OutPointKey, Option<Utxo>>>,
}

impl<'a, S> AdmissionUtxoOverlay<'a, S> {
    fn new(base: &'a S) -> Self {
        Self {
            base,
            current: Mutex::new(BTreeMap::new()),
        }
    }
}

impl<S: UtxoStore> UtxoStore for AdmissionUtxoOverlay<'_, S> {
    fn get(&self, outpoint: OutPointKey) -> Result<Option<Utxo>, UtxoError> {
        if let Some(value) = self
            .current
            .lock()
            .expect("admission overlay lock not poisoned")
            .get(&outpoint)
        {
            return Ok(value.clone());
        }
        self.base.get(outpoint)
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
        let mut current = self
            .current
            .lock()
            .expect("admission overlay lock not poisoned");
        let mut seen = BTreeSet::new();
        let mut undo_spent = Vec::with_capacity(spent.len());
        for outpoint in spent {
            if !seen.insert(*outpoint) {
                return Err(UtxoError::DuplicateSpend(*outpoint));
            }
            let previous = match current.get(outpoint) {
                Some(previous) => previous.clone(),
                None => self.base.get(*outpoint)?,
            }
            .ok_or(UtxoError::Missing(*outpoint))?;
            undo_spent.push((*outpoint, previous));
        }
        seen.clear();
        for (outpoint, _) in created {
            let previous = match current.get(outpoint) {
                Some(previous) => previous.clone(),
                None => self.base.get(*outpoint)?,
            };
            if !seen.insert(*outpoint) || previous.is_some() {
                return Err(UtxoError::Duplicate(*outpoint));
            }
        }
        for outpoint in spent {
            current.insert(*outpoint, None);
        }
        for (outpoint, utxo) in created {
            current.insert(*outpoint, Some(utxo.clone()));
        }
        Ok(UtxoUndo::from_parts(
            undo_spent,
            created.iter().map(|(outpoint, _)| *outpoint).collect(),
        ))
    }

    fn undo(&self, undo: &UtxoUndo, _now: u64, _hot_window_secs: u64) -> Result<(), UtxoError> {
        let mut current = self
            .current
            .lock()
            .expect("admission overlay lock not poisoned");
        for outpoint in undo.created() {
            current.insert(*outpoint, None);
        }
        for (outpoint, utxo) in undo.spent() {
            current.insert(*outpoint, Some(utxo.clone()));
        }
        Ok(())
    }

    fn age_to_cold(&self, _now: u64, _hot_window_secs: u64) -> Result<u64, UtxoError> {
        Ok(0)
    }

    fn snapshot_entries(&self) -> Result<BTreeMap<OutPointKey, Utxo>, UtxoError> {
        let mut entries = self.base.snapshot_entries()?;
        for (outpoint, utxo) in self
            .current
            .lock()
            .expect("admission overlay lock not poisoned")
            .iter()
        {
            if let Some(utxo) = utxo {
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
        let mut current = self
            .current
            .lock()
            .expect("admission overlay lock not poisoned");
        current.clear();
        for outpoint in base.keys().chain(entries.keys()) {
            if base.get(outpoint) != entries.get(outpoint) {
                current.insert(*outpoint, entries.get(outpoint).cloned());
            }
        }
        Ok(())
    }

    fn tier_stats(&self) -> Result<TierStats, UtxoError> {
        Ok(TierStats {
            hot: u64::try_from(self.snapshot_entries()?.len())
                .map_err(|_| UtxoError::Malformed("admission overlay entry count"))?,
            cold: 0,
        })
    }
}

/// Splits an unordered wire batch into dependency-connected packages.
#[must_use]
pub fn dependency_packages(mut transactions: Vec<Transaction>) -> Vec<Vec<Transaction>> {
    let mut packages = Vec::new();
    while let Some(seed) = transactions.pop() {
        let mut package = vec![seed];
        loop {
            let package_txids = package
                .iter()
                .map(Transaction::compute_txid)
                .collect::<BTreeSet<_>>();
            let package_parents = package
                .iter()
                .flat_map(|transaction| {
                    transaction
                        .input
                        .iter()
                        .map(|input| input.previous_output.txid)
                })
                .collect::<BTreeSet<_>>();
            let Some(index) = transactions.iter().position(|transaction| {
                let txid = transaction.compute_txid();
                package_parents.contains(&txid)
                    || transaction
                        .input
                        .iter()
                        .any(|input| package_txids.contains(&input.previous_output.txid))
            }) else {
                break;
            };
            package.push(transactions.swap_remove(index));
        }
        packages.push(package);
    }
    packages
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

    fn child(parent: &Transaction, value_sats: u64) -> Transaction {
        let witness_script = Builder::new().push_opcode(opcodes::OP_TRUE).into_script();
        Transaction {
            version: Version::TWO,
            lock_time: LockTime::ZERO,
            input: vec![TxIn {
                previous_output: OutPoint::new(parent.compute_txid(), 0),
                script_sig: ScriptBuf::new(),
                sequence: Sequence::MAX,
                witness: Witness::from_slice(&[witness_script.as_bytes()]),
            }],
            output: vec![TxOut {
                value: Amount::from_sat(value_sats),
                script_pubkey: ScriptBuf::new_p2wsh(&witness_script.wscript_hash()),
            }],
        }
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
    fn unordered_parent_child_package_is_atomic_and_uses_the_overlay() {
        let (_directory, store) = store();
        let (outpoint, utxo, parent) = spend(3);
        store.apply(&[], &[(outpoint.into(), utxo)]).unwrap();
        let child = child(&parent, 80_000);
        let parent_txid = parent.compute_txid();
        let child_txid = child.compute_txid();
        let mut pool = TransactionAdmissionPool::default();

        let mut invalid_child = child.clone();
        invalid_child.input[0].witness = Witness::new();
        assert!(matches!(
            pool.admit_package(&store, vec![invalid_child, parent.clone()], context()),
            Err(TransactionAdmissionError::Chainstate(
                ChainstateError::Script(_)
            ))
        ));
        assert!(pool.is_empty());
        assert!(store.get(outpoint.into()).unwrap().is_some());

        let outcome = pool
            .admit_package(&store, vec![child, parent], context())
            .unwrap();
        assert_eq!(outcome.accepted, vec![parent_txid, child_txid]);
        assert_eq!(
            pool.snapshot()
                .iter()
                .map(Transaction::compute_txid)
                .collect::<Vec<_>>(),
            vec![parent_txid, child_txid]
        );
        assert!(store.get(outpoint.into()).unwrap().is_some());
    }

    #[test]
    fn existing_parent_accepts_a_later_child_and_wire_groups_are_independent() {
        let (_directory, store) = store();
        let (first_outpoint, first_utxo, parent) = spend(4);
        let (second_outpoint, second_utxo, unrelated) = spend(5);
        store
            .apply(
                &[],
                &[
                    (first_outpoint.into(), first_utxo),
                    (second_outpoint.into(), second_utxo),
                ],
            )
            .unwrap();
        let child = child(&parent, 80_000);
        let packages = dependency_packages(vec![child.clone(), unrelated.clone(), parent.clone()]);
        assert_eq!(packages.len(), 2);
        assert!(packages.iter().any(|package| package.len() == 2));
        assert!(packages.iter().any(|package| package.len() == 1));

        let mut pool = TransactionAdmissionPool::default();
        assert!(matches!(
            pool.admit(&store, child.clone(), context()),
            Err(TransactionAdmissionError::Chainstate(
                ChainstateError::Utxo(UtxoError::Missing(_))
            ))
        ));
        pool.admit(&store, parent, context()).unwrap();
        pool.admit(&store, child, context()).unwrap();
        assert_eq!(pool.len(), 2);
    }

    #[test]
    fn package_count_bound_and_duplicate_txid_leave_pool_unchanged() {
        let (_directory, store) = store();
        let (outpoint, utxo, transaction) = spend(6);
        store.apply(&[], &[(outpoint.into(), utxo)]).unwrap();
        let mut pool = TransactionAdmissionPool::default();

        assert!(matches!(
            pool.admit_package(
                &store,
                vec![transaction.clone(); MAX_PACKAGE_TRANSACTIONS + 1],
                context()
            ),
            Err(TransactionAdmissionError::TooManyPackageTransactions { .. })
        ));
        assert!(matches!(
            pool.admit_package(&store, vec![transaction.clone(), transaction], context()),
            Err(TransactionAdmissionError::DuplicatePackageTransaction(_))
        ));
        assert!(pool.is_empty());
    }

    #[test]
    fn capacity_evicts_an_oldest_parent_with_all_descendants() {
        let (_directory, store) = store();
        let (root_outpoint, root_utxo, parent) = spend(7);
        store
            .apply(&[], &[(root_outpoint.into(), root_utxo)])
            .unwrap();
        let child = child(&parent, 80_000);
        let parent_txid = parent.compute_txid();
        let child_txid = child.compute_txid();
        let mut pool = TransactionAdmissionPool::default();
        pool.admit_package(&store, vec![child, parent], context())
            .unwrap();

        for index in 8..=70 {
            let (outpoint, utxo, transaction) = spend(index);
            store.apply(&[], &[(outpoint.into(), utxo)]).unwrap();
            pool.admit(&store, transaction, context()).unwrap();
        }
        let txids = pool
            .snapshot()
            .iter()
            .map(Transaction::compute_txid)
            .collect::<BTreeSet<_>>();
        assert_eq!(pool.len(), MAX_ADMITTED_TRANSACTIONS - 1);
        assert!(!txids.contains(&parent_txid));
        assert!(!txids.contains(&child_txid));
    }

    #[test]
    fn capacity_rejects_a_child_when_every_eviction_would_remove_its_ancestor() {
        let (_directory, store) = store();
        let (outpoint, mut utxo, mut transaction) = spend(71);
        utxo.value_sats = 10_000_000;
        transaction.output[0].value = Amount::from_sat(9_999_000);
        store.apply(&[], &[(outpoint.into(), utxo)]).unwrap();
        let mut pool = TransactionAdmissionPool::default();
        pool.admit(&store, transaction.clone(), context()).unwrap();

        let mut value = 9_998_000;
        for _ in 1..MAX_ADMITTED_TRANSACTIONS {
            transaction = child(&transaction, value);
            pool.admit(&store, transaction.clone(), context()).unwrap();
            value -= 1_000;
        }
        assert_eq!(pool.len(), MAX_ADMITTED_TRANSACTIONS);
        let before = pool
            .snapshot()
            .iter()
            .map(Transaction::compute_wtxid)
            .collect::<Vec<_>>();
        let candidate = child(&transaction, value);
        assert!(matches!(
            pool.admit(&store, candidate, context()),
            Err(TransactionAdmissionError::PackageCapacity)
        ));
        assert_eq!(
            pool.snapshot()
                .iter()
                .map(Transaction::compute_wtxid)
                .collect::<Vec<_>>(),
            before
        );
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
