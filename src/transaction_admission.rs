//! Bounded local admission for independently received transactions.
//!
//! Confirmed inputs and outputs of already-admitted parents are available to a
//! bounded package overlay. Replacement follows BIP125 policy by default;
//! explicit full-RBF mode bypasses only the signaling requirement.

use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    sync::Mutex,
};

use bitcoin::{BlockHash, OutPoint, Transaction, Txid, Wtxid, consensus::encode::serialize};
use rand::Rng;
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
/// Maximum number of missing-parent transactions retained across peer failover.
pub const MAX_ORPHAN_TRANSACTIONS: usize = 64;
/// Maximum witness-serialized bytes retained by the orphan pool.
pub const MAX_ORPHAN_TRANSACTION_BYTES: usize = 4_000_000;
/// Core-compatible orphan lifetime of twenty minutes.
pub const ORPHAN_EXPIRY_SECS: u32 = 20 * 60;
/// Core's twelve-hour rolling mempool minimum-fee half-life.
pub const ROLLING_FEE_HALFLIFE_SECS: u32 = 12 * 60 * 60;
const SATOSHIS_PER_KVB: u64 = 1_000;
/// Maximum transactions accepted as one dependency-connected package.
pub const MAX_PACKAGE_TRANSACTIONS: usize = 25;
/// Core-like maximum aggregate virtual size accepted for one package.
pub const MAX_PACKAGE_VBYTES: usize = 101_000;
/// Core default maximum ancestors, including the transaction itself.
pub const MAX_ANCESTOR_TRANSACTIONS: usize = 25;
/// Core default maximum aggregate ancestor virtual size.
pub const MAX_ANCESTOR_VBYTES: usize = 101_000;
/// Core default maximum descendants, including the transaction itself.
pub const MAX_DESCENDANT_TRANSACTIONS: usize = 25;
/// Core default maximum aggregate descendant virtual size.
pub const MAX_DESCENDANT_VBYTES: usize = 101_000;
/// Maximum original transactions and descendants replaced in one admission.
pub const MAX_REPLACEMENT_EVICTIONS: usize = 100;
/// Additional relay fee charged for every replacement virtual byte.
pub const INCREMENTAL_RELAY_FEE_SAT_VB: u64 = 1;

const MAX_NON_RBF_SEQUENCE: u32 = 0xffff_fffe;

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
    /// Whether conflicts may be replaced without BIP125 signaling.
    pub full_rbf: bool,
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
        /// Original conflicts and descendants removed by BIP125 replacement.
        replaced: usize,
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
    /// A transaction would exceed the bounded ancestor count.
    #[error("transaction {txid} has {count} ancestors; limit is {limit}")]
    TooManyAncestors {
        /// Transaction whose ancestor set is over the limit.
        txid: Txid,
        /// Ancestors including the transaction itself.
        count: usize,
        /// Hard ancestor-count limit.
        limit: usize,
    },
    /// A transaction would exceed the bounded aggregate ancestor size.
    #[error("transaction {txid} ancestor virtual size {vbytes} exceeds limit {limit}")]
    AncestorsTooLarge {
        /// Transaction whose ancestor set is over the limit.
        txid: Txid,
        /// Aggregate ancestor virtual size.
        vbytes: usize,
        /// Hard aggregate ancestor virtual-size limit.
        limit: usize,
    },
    /// An ancestor would exceed the bounded descendant count.
    #[error("transaction {txid} has {count} descendants; limit is {limit}")]
    TooManyDescendants {
        /// Transaction whose descendant set is over the limit.
        txid: Txid,
        /// Descendants including the transaction itself.
        count: usize,
        /// Hard descendant-count limit.
        limit: usize,
    },
    /// An ancestor would exceed the bounded aggregate descendant size.
    #[error("transaction {txid} descendant virtual size {vbytes} exceeds limit {limit}")]
    DescendantsTooLarge {
        /// Transaction whose descendant set is over the limit.
        txid: Txid,
        /// Aggregate descendant virtual size.
        vbytes: usize,
        /// Hard aggregate descendant virtual-size limit.
        limit: usize,
    },
    /// A directly conflicting transaction did not opt in, including through an ancestor.
    #[error("directly conflicting transaction {0} does not signal BIP125 replaceability")]
    NonReplaceable(Txid),
    /// A replacement package added an input from an unrelated unconfirmed transaction.
    #[error("replacement adds unconfirmed input {0}")]
    ReplacementAddsUnconfirmedInput(OutPoint),
    /// A replacement did not pay more than all transactions it would remove.
    #[error(
        "replacement fee {replacement_fee_sats} sat does not exceed conflicting fee {conflicts_fee_sats} sat"
    )]
    InsufficientReplacementFee {
        /// Aggregate replacement-package fee.
        replacement_fee_sats: u64,
        /// Aggregate fee of all conflicts and descendants.
        conflicts_fee_sats: u64,
    },
    /// A replacement did not pay the incremental relay fee for its own bandwidth.
    #[error(
        "replacement additional fee {additional_fee_sats} sat is below required incremental fee {required_fee_sats} sat"
    )]
    InsufficientReplacementRelayFee {
        /// Fee above the transactions being removed.
        additional_fee_sats: u64,
        /// Incremental relay fee required by replacement virtual size.
        required_fee_sats: u64,
    },
    /// A replacement would remove more transactions than BIP125 permits.
    #[error("replacement would evict {count} transactions; limit is {limit}")]
    TooManyReplacementEvictions {
        /// Original conflicts plus their descendants.
        count: usize,
        /// BIP125 eviction limit.
        limit: usize,
    },
    /// Capacity pressure raised the rolling mempool minimum above this fee.
    #[error("fee {fee_sats} is below rolling mempool minimum {minimum_sats}")]
    RollingMinimumFee {
        /// Transaction fee.
        fee_sats: u64,
        /// Fee required at the current rolling rate.
        minimum_sats: u64,
    },
}

impl TransactionAdmissionError {
    /// Returns whether admission failed only because an input is not yet available.
    #[must_use]
    pub const fn is_missing_input(&self) -> bool {
        matches!(
            self,
            Self::Chainstate(ChainstateError::Utxo(UtxoError::Missing(_)))
        )
    }
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
    /// Original conflicts and descendants removed by BIP125 replacement.
    pub replaced: usize,
}

#[derive(Clone)]
struct AdmittedTransaction {
    transaction: Transaction,
    serialized_len: usize,
    fee_sats: u64,
}

#[derive(Clone)]
struct OrphanTransaction {
    transaction: Transaction,
    serialized_len: usize,
    expires_at: u32,
    source: u64,
}

#[derive(Default)]
struct ReplacementPlan {
    txids: BTreeSet<Txid>,
    conflicts_fee_sats: u64,
}

/// Wire-ordered, conflict-indexed, hard-bounded local transaction pool.
#[derive(Clone, Default)]
pub struct TransactionAdmissionPool {
    entries: VecDeque<AdmittedTransaction>,
    spent: BTreeMap<OutPoint, Txid>,
    retained_bytes: usize,
    orphans: VecDeque<OrphanTransaction>,
    orphan_bytes: usize,
    orphan_children_by_parent: BTreeMap<Txid, BTreeSet<Txid>>,
    orphans_by_outpoint: BTreeMap<OutPoint, BTreeSet<Txid>>,
    rolling_minimum_fee_sat_kvb: u64,
    rolling_fee_last_update: u32,
    rolling_fee_decay_enabled: bool,
    observed_chain_tip: Option<BlockHash>,
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

    /// Returns the number of missing-parent transactions retained in memory.
    #[must_use]
    pub fn orphan_len(&self) -> usize {
        self.orphans.len()
    }

    /// Returns witness-serialized bytes charged to the orphan pool.
    #[must_use]
    pub const fn orphan_bytes(&self) -> usize {
        self.orphan_bytes
    }

    /// Observes a caught-up chain tip and enables fee decay after a later block.
    pub fn observe_chain_tip(&mut self, tip: BlockHash, now: u32) {
        if self
            .observed_chain_tip
            .is_some_and(|previous| previous != tip)
        {
            self.rolling_fee_decay_enabled = true;
            self.rolling_fee_last_update = now;
        }
        self.observed_chain_tip = Some(tip);
    }

    /// Returns the current rolling minimum in satoshis per 1,000 virtual bytes.
    pub fn rolling_minimum_fee_sat_kvb(&mut self, now: u32) -> u64 {
        self.decay_rolling_minimum_fee(now);
        self.effective_rolling_minimum_fee_sat_kvb()
    }

    fn effective_rolling_minimum_fee_sat_kvb(&self) -> u64 {
        if self.rolling_minimum_fee_sat_kvb == 0 || !self.rolling_fee_decay_enabled {
            self.rolling_minimum_fee_sat_kvb
        } else {
            self.rolling_minimum_fee_sat_kvb.max(SATOSHIS_PER_KVB)
        }
    }

    /// Removes orphan transactions whose twenty-minute lifetime has elapsed.
    pub fn prune_orphans(&mut self, now: u32) -> usize {
        let before = self.orphans.len();
        self.orphans.retain(|orphan| orphan.expires_at > now);
        self.rebuild_orphan_indexes();
        before.saturating_sub(self.orphans.len())
    }

    /// Retains missing-parent transactions under independent count and byte bounds.
    pub fn retain_orphans(&mut self, transactions: &[Transaction], now: u32, source: u64) -> usize {
        self.prune_orphans(now);
        let mut inserted = BTreeSet::new();
        let mut rng = rand::rng();
        for transaction in transactions {
            let txid = transaction.compute_txid();
            let wtxid = transaction.compute_wtxid();
            if transaction.is_coinbase()
                || transaction.weight().to_wu() > u64::from(bitcoin::policy::MAX_STANDARD_TX_WEIGHT)
                || self
                    .entries
                    .iter()
                    .any(|entry| entry.transaction.compute_txid() == txid)
                || self.orphans.iter().any(|orphan| {
                    orphan.transaction.compute_txid() == txid
                        || orphan.transaction.compute_wtxid() == wtxid
                })
            {
                continue;
            }
            let serialized_len = serialize(transaction).len();
            if serialized_len > MAX_ORPHAN_TRANSACTION_BYTES {
                continue;
            }
            self.orphan_bytes = self.orphan_bytes.saturating_add(serialized_len);
            self.orphans.push_back(OrphanTransaction {
                transaction: transaction.clone(),
                serialized_len,
                expires_at: now.saturating_add(ORPHAN_EXPIRY_SECS),
                source,
            });
            inserted.insert(txid);
            while self.orphans.len() > MAX_ORPHAN_TRANSACTIONS
                || self.orphan_bytes > MAX_ORPHAN_TRANSACTION_BYTES
            {
                let index = rng.random_range(0..self.orphans.len());
                let removed = self
                    .orphans
                    .remove(index)
                    .expect("random orphan index is in bounds");
                self.orphan_bytes = self.orphan_bytes.saturating_sub(removed.serialized_len);
            }
        }
        self.rebuild_orphan_indexes();
        let retained = self
            .orphans
            .iter()
            .map(|orphan| orphan.transaction.compute_txid())
            .collect::<BTreeSet<_>>();
        inserted.intersection(&retained).count()
    }

    /// Returns untried orphans directly spending any newly accepted parent.
    #[must_use]
    pub fn orphan_children(
        &self,
        parents: &BTreeSet<Txid>,
        already_tried: &BTreeSet<Txid>,
    ) -> Vec<Transaction> {
        let child_txids = parents
            .iter()
            .filter_map(|parent| self.orphan_children_by_parent.get(parent))
            .flatten()
            .filter(|txid| !already_tried.contains(*txid))
            .copied()
            .collect::<BTreeSet<_>>();
        self.orphans
            .iter()
            .filter(|orphan| child_txids.contains(&orphan.transaction.compute_txid()))
            .map(|orphan| orphan.transaction.clone())
            .collect()
    }

    /// Removes exact orphan IDs after successful admission or terminal rejection.
    pub fn remove_orphans(&mut self, txids: &BTreeSet<Txid>) -> usize {
        let before = self.orphans.len();
        self.orphans
            .retain(|orphan| !txids.contains(&orphan.transaction.compute_txid()));
        self.rebuild_orphan_indexes();
        before.saturating_sub(self.orphans.len())
    }

    /// Removes every orphan attributed to one disconnected peer session.
    pub fn remove_orphans_from(&mut self, source: u64) -> usize {
        let before = self.orphans.len();
        self.orphans.retain(|orphan| orphan.source != source);
        self.rebuild_orphan_indexes();
        before.saturating_sub(self.orphans.len())
    }

    /// Removes orphans included or made impossible by newly connected block transactions.
    pub fn remove_orphans_for_block_transactions<'a>(
        &mut self,
        transactions: impl IntoIterator<Item = &'a Transaction>,
    ) -> usize {
        let removed = transactions
            .into_iter()
            .flat_map(|transaction| transaction.input.iter())
            .filter_map(|input| self.orphans_by_outpoint.get(&input.previous_output))
            .flatten()
            .copied()
            .collect::<BTreeSet<_>>();
        self.remove_orphans(&removed)
    }

    fn rebuild_orphan_indexes(&mut self) {
        self.orphan_bytes = self
            .orphans
            .iter()
            .map(|orphan| orphan.serialized_len)
            .sum();
        self.orphan_children_by_parent.clear();
        self.orphans_by_outpoint.clear();
        for orphan in &self.orphans {
            let txid = orphan.transaction.compute_txid();
            for input in &orphan.transaction.input {
                self.orphan_children_by_parent
                    .entry(input.previous_output.txid)
                    .or_default()
                    .insert(txid);
                self.orphans_by_outpoint
                    .entry(input.previous_output)
                    .or_default()
                    .insert(txid);
            }
        }
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
    /// Conflicts may replace BIP125-signaling entries when every replacement
    /// rule is satisfied.
    pub fn admit<S: UtxoStore>(
        &mut self,
        store: &S,
        transaction: Transaction,
        context: TransactionAdmissionContext,
    ) -> Result<TransactionAdmissionOutcome, TransactionAdmissionError> {
        self.admit_at(store, transaction, context, 0)
    }

    /// Validates and retains one transaction at an explicit wall-clock time.
    pub fn admit_at<S: UtxoStore>(
        &mut self,
        store: &S,
        transaction: Transaction,
        context: TransactionAdmissionContext,
        now: u32,
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
        let outcome = self.admit_package_at(store, vec![transaction], context, now)?;
        Ok(TransactionAdmissionOutcome::Accepted {
            txid,
            evicted: outcome.evicted,
            replaced: outcome.replaced,
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
        self.admit_package_at(store, transactions, context, 0)
    }

    /// Atomically validates a package at an explicit wall-clock time.
    pub fn admit_package_at<S: UtxoStore>(
        &mut self,
        store: &S,
        transactions: Vec<Transaction>,
        context: TransactionAdmissionContext,
        now: u32,
    ) -> Result<PackageAdmissionOutcome, TransactionAdmissionError> {
        validate_package_bounds(&transactions)?;
        self.decay_rolling_minimum_fee(now);
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
        let (already_present, package) = self.collect_new_package(transactions)?;
        if package.is_empty() {
            return Ok(PackageAdmissionOutcome {
                accepted: Vec::new(),
                already_present,
                evicted: 0,
                replaced: 0,
            });
        }

        let ordered = topological_package_order(package)?;
        let replacement_vbytes = ordered.iter().fold(0_usize, |total, transaction| {
            total.saturating_add(transaction.vsize())
        });
        let replacement = self.prepare_replacement(&ordered, context.full_rbf)?;

        let overlay = AdmissionUtxoOverlay::new(store);
        for entry in &self.entries {
            let _ = apply_to_overlay(&overlay, &entry.transaction, context)?;
        }
        let (accepted, replacement_fee_sats) =
            self.append_ordered_package(&overlay, ordered, context)?;
        self.validate_topology_limits(&accepted)?;
        validate_replacement_fees(
            replacement_fee_sats,
            replacement.conflicts_fee_sats,
            replacement_vbytes,
            !replacement.txids.is_empty(),
        )?;
        let protected = accepted.iter().copied().collect::<BTreeSet<_>>();
        let evicted = self.evict_to_capacity(&protected)?;
        Ok(PackageAdmissionOutcome {
            accepted,
            already_present,
            evicted,
            replaced: replacement.txids.len(),
        })
    }

    fn collect_new_package(
        &self,
        transactions: Vec<Transaction>,
    ) -> Result<(usize, BTreeMap<Txid, Transaction>), TransactionAdmissionError> {
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
        Ok((already_present, package))
    }

    fn prepare_replacement(
        &mut self,
        ordered: &[Transaction],
        full_rbf: bool,
    ) -> Result<ReplacementPlan, TransactionAdmissionError> {
        let direct_conflicts = ordered
            .iter()
            .flat_map(|transaction| transaction.input.iter())
            .filter_map(|input| self.spent.get(&input.previous_output).copied())
            .collect::<BTreeSet<_>>();
        if direct_conflicts.is_empty() {
            return Ok(ReplacementPlan::default());
        }
        let mut txids = BTreeSet::new();
        for txid in &direct_conflicts {
            if !full_rbf && !self.transaction_is_replaceable(*txid) {
                return Err(TransactionAdmissionError::NonReplaceable(*txid));
            }
            txids.extend(self.descendant_closure(*txid));
        }
        if txids.len() > MAX_REPLACEMENT_EVICTIONS {
            return Err(TransactionAdmissionError::TooManyReplacementEvictions {
                count: txids.len(),
                limit: MAX_REPLACEMENT_EVICTIONS,
            });
        }
        self.validate_replacement_inputs(ordered, &direct_conflicts)?;
        let conflicts_fee_sats = self
            .entries
            .iter()
            .filter(|entry| txids.contains(&entry.transaction.compute_txid()))
            .fold(0_u64, |total, entry| total.saturating_add(entry.fee_sats));
        self.entries
            .retain(|entry| !txids.contains(&entry.transaction.compute_txid()));
        self.rebuild_indexes();
        Ok(ReplacementPlan {
            txids,
            conflicts_fee_sats,
        })
    }

    fn validate_replacement_inputs(
        &self,
        ordered: &[Transaction],
        direct_conflicts: &BTreeSet<Txid>,
    ) -> Result<(), TransactionAdmissionError> {
        let allowed = self
            .entries
            .iter()
            .filter(|entry| direct_conflicts.contains(&entry.transaction.compute_txid()))
            .flat_map(|entry| {
                entry
                    .transaction
                    .input
                    .iter()
                    .map(|input| input.previous_output)
            })
            .collect::<BTreeSet<_>>();
        let pool_txids = self
            .entries
            .iter()
            .map(|entry| entry.transaction.compute_txid())
            .collect::<BTreeSet<_>>();
        if let Some(outpoint) = ordered
            .iter()
            .flat_map(|transaction| transaction.input.iter())
            .map(|input| input.previous_output)
            .find(|outpoint| pool_txids.contains(&outpoint.txid) && !allowed.contains(outpoint))
        {
            return Err(TransactionAdmissionError::ReplacementAddsUnconfirmedInput(
                outpoint,
            ));
        }
        Ok(())
    }

    fn append_ordered_package<S: UtxoStore>(
        &mut self,
        overlay: &AdmissionUtxoOverlay<'_, S>,
        ordered: Vec<Transaction>,
        context: TransactionAdmissionContext,
    ) -> Result<(Vec<Txid>, u64), TransactionAdmissionError> {
        let mut package_spent = BTreeSet::new();
        let mut accepted = Vec::with_capacity(ordered.len());
        let mut replacement_fee_sats = 0_u64;
        for transaction in ordered {
            if let Some(conflict) = transaction
                .input
                .iter()
                .map(|input| input.previous_output)
                .find(|outpoint| !package_spent.insert(*outpoint))
            {
                return Err(TransactionPolicyError::Conflict(conflict).into());
            }
            let txid = transaction.compute_txid();
            let fee_sats = apply_to_overlay(overlay, &transaction, context)?;
            let minimum_sats = fee_for_rate(
                self.effective_rolling_minimum_fee_sat_kvb(),
                transaction.vsize(),
            );
            if fee_sats < minimum_sats {
                return Err(TransactionAdmissionError::RollingMinimumFee {
                    fee_sats,
                    minimum_sats,
                });
            }
            replacement_fee_sats = replacement_fee_sats.saturating_add(fee_sats);
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
                fee_sats,
            });
            accepted.push(txid);
        }
        Ok((accepted, replacement_fee_sats))
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
        let rolling_minimum_fee_sat_kvb = self.rolling_minimum_fee_sat_kvb;
        self.rolling_minimum_fee_sat_kvb = 0;
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
        self.rolling_minimum_fee_sat_kvb = rolling_minimum_fee_sat_kvb;
        before.saturating_sub(self.entries.len())
    }

    /// Removes selected transactions and every retained descendant atomically.
    pub fn remove_with_descendants(&mut self, roots: &BTreeSet<Txid>) -> usize {
        let removed = roots
            .iter()
            .flat_map(|txid| self.descendant_closure(*txid))
            .collect::<BTreeSet<_>>();
        let before = self.entries.len();
        self.entries
            .retain(|entry| !removed.contains(&entry.transaction.compute_txid()));
        let removed = before.saturating_sub(self.entries.len());
        if removed > 0 {
            self.rebuild_indexes();
        }
        removed
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
            let (removed_fee_sats, removed_vbytes) = self
                .entries
                .iter()
                .filter(|entry| removed.contains(&entry.transaction.compute_txid()))
                .fold((0_u64, 0_usize), |(fees, vbytes), entry| {
                    (
                        fees.saturating_add(entry.fee_sats),
                        vbytes.saturating_add(entry.transaction.vsize()),
                    )
                });
            let removed_rate = u64::try_from(
                u128::from(removed_fee_sats).saturating_mul(u128::from(SATOSHIS_PER_KVB))
                    / u128::try_from(removed_vbytes).unwrap_or(u128::MAX),
            )
            .unwrap_or(u64::MAX)
            .saturating_add(SATOSHIS_PER_KVB);
            if removed_rate > self.rolling_minimum_fee_sat_kvb {
                self.rolling_minimum_fee_sat_kvb = removed_rate;
                self.rolling_fee_decay_enabled = false;
            }
            let before = self.entries.len();
            self.entries
                .retain(|entry| !removed.contains(&entry.transaction.compute_txid()));
            evicted += before.saturating_sub(self.entries.len());
            self.rebuild_indexes();
        }
        Ok(evicted)
    }

    #[allow(
        clippy::cast_possible_truncation,
        clippy::cast_precision_loss,
        clippy::cast_sign_loss
    )]
    fn decay_rolling_minimum_fee(&mut self, now: u32) {
        if !self.rolling_fee_decay_enabled
            || self.rolling_minimum_fee_sat_kvb == 0
            || now <= self.rolling_fee_last_update.saturating_add(10)
        {
            return;
        }
        let mut half_life = f64::from(ROLLING_FEE_HALFLIFE_SECS);
        if self.retained_bytes < MAX_ADMITTED_TRANSACTION_BYTES / 4 {
            half_life /= 4.0;
        } else if self.retained_bytes < MAX_ADMITTED_TRANSACTION_BYTES / 2 {
            half_life /= 2.0;
        }
        let elapsed = f64::from(now.saturating_sub(self.rolling_fee_last_update));
        // Core performs this exponential decay in double precision. Both operands
        // are non-negative and the quotient cannot exceed the original u64 rate.
        self.rolling_minimum_fee_sat_kvb = ((self.rolling_minimum_fee_sat_kvb as f64)
            / 2_f64.powf(elapsed / half_life))
        .round() as u64;
        self.rolling_fee_last_update = now;
        if self.rolling_minimum_fee_sat_kvb < SATOSHIS_PER_KVB / 2 {
            self.rolling_minimum_fee_sat_kvb = 0;
        }
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

    fn ancestor_closure(&self, txid: Txid) -> BTreeSet<Txid> {
        let pool_txids = self
            .entries
            .iter()
            .map(|entry| entry.transaction.compute_txid())
            .collect::<BTreeSet<_>>();
        let mut ancestors = BTreeSet::from([txid]);
        let mut pending = vec![txid];
        while let Some(candidate) = pending.pop() {
            let Some(transaction) = self
                .entries
                .iter()
                .find(|entry| entry.transaction.compute_txid() == candidate)
                .map(|entry| &entry.transaction)
            else {
                continue;
            };
            for parent in transaction
                .input
                .iter()
                .map(|input| input.previous_output.txid)
                .filter(|parent| pool_txids.contains(parent))
            {
                if ancestors.insert(parent) {
                    pending.push(parent);
                }
            }
        }
        ancestors
    }

    fn closure_vbytes(&self, txids: &BTreeSet<Txid>) -> usize {
        self.entries
            .iter()
            .filter(|entry| txids.contains(&entry.transaction.compute_txid()))
            .fold(0_usize, |total, entry| {
                total.saturating_add(entry.transaction.vsize())
            })
    }

    fn validate_topology_limits(&self, accepted: &[Txid]) -> Result<(), TransactionAdmissionError> {
        let mut affected_ancestors = BTreeSet::new();
        for txid in accepted {
            let ancestors = self.ancestor_closure(*txid);
            if ancestors.len() > MAX_ANCESTOR_TRANSACTIONS {
                return Err(TransactionAdmissionError::TooManyAncestors {
                    txid: *txid,
                    count: ancestors.len(),
                    limit: MAX_ANCESTOR_TRANSACTIONS,
                });
            }
            let vbytes = self.closure_vbytes(&ancestors);
            if vbytes > MAX_ANCESTOR_VBYTES {
                return Err(TransactionAdmissionError::AncestorsTooLarge {
                    txid: *txid,
                    vbytes,
                    limit: MAX_ANCESTOR_VBYTES,
                });
            }
            affected_ancestors.extend(ancestors);
        }
        for txid in affected_ancestors {
            let descendants = self.descendant_closure(txid);
            if descendants.len() > MAX_DESCENDANT_TRANSACTIONS {
                return Err(TransactionAdmissionError::TooManyDescendants {
                    txid,
                    count: descendants.len(),
                    limit: MAX_DESCENDANT_TRANSACTIONS,
                });
            }
            let vbytes = self.closure_vbytes(&descendants);
            if vbytes > MAX_DESCENDANT_VBYTES {
                return Err(TransactionAdmissionError::DescendantsTooLarge {
                    txid,
                    vbytes,
                    limit: MAX_DESCENDANT_VBYTES,
                });
            }
        }
        Ok(())
    }

    fn transaction_is_replaceable(&self, txid: Txid) -> bool {
        let mut pending = vec![txid];
        let mut visited = BTreeSet::new();
        while let Some(candidate) = pending.pop() {
            if !visited.insert(candidate) {
                continue;
            }
            let Some(transaction) = self
                .entries
                .iter()
                .find(|entry| entry.transaction.compute_txid() == candidate)
                .map(|entry| &entry.transaction)
            else {
                continue;
            };
            if transaction
                .input
                .iter()
                .any(|input| input.sequence.to_consensus_u32() < MAX_NON_RBF_SEQUENCE)
            {
                return true;
            }
            pending.extend(
                transaction
                    .input
                    .iter()
                    .map(|input| input.previous_output.txid),
            );
        }
        false
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

fn validate_replacement_fees(
    replacement_fee_sats: u64,
    conflicts_fee_sats: u64,
    replacement_vbytes: usize,
    is_replacement: bool,
) -> Result<(), TransactionAdmissionError> {
    if !is_replacement {
        return Ok(());
    }
    if replacement_fee_sats <= conflicts_fee_sats {
        return Err(TransactionAdmissionError::InsufficientReplacementFee {
            replacement_fee_sats,
            conflicts_fee_sats,
        });
    }
    let additional_fee_sats = replacement_fee_sats - conflicts_fee_sats;
    let required_fee_sats = u64::try_from(replacement_vbytes)
        .unwrap_or(u64::MAX)
        .saturating_mul(INCREMENTAL_RELAY_FEE_SAT_VB);
    if additional_fee_sats < required_fee_sats {
        return Err(TransactionAdmissionError::InsufficientReplacementRelayFee {
            additional_fee_sats,
            required_fee_sats,
        });
    }
    Ok(())
}

fn fee_for_rate(rate_sat_kvb: u64, vbytes: usize) -> u64 {
    let product =
        u128::from(rate_sat_kvb).saturating_mul(u128::try_from(vbytes).unwrap_or(u128::MAX));
    let fee = u64::try_from(
        product.saturating_add(u128::from(SATOSHIS_PER_KVB - 1)) / u128::from(SATOSHIS_PER_KVB),
    )
    .unwrap_or(u64::MAX);
    if fee == 0 && rate_sat_kvb > 0 { 1 } else { fee }
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
) -> Result<u64, TransactionAdmissionError> {
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
    Ok(fee_sats)
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

/// Returns selected transaction IDs and every descendant in an unordered snapshot.
#[must_use]
pub fn transaction_descendant_closure(
    transactions: &[Transaction],
    roots: &BTreeSet<Txid>,
) -> BTreeSet<Txid> {
    let mut closure = roots.clone();
    loop {
        let before = closure.len();
        for transaction in transactions {
            if transaction
                .input
                .iter()
                .any(|input| closure.contains(&input.previous_output.txid))
            {
                closure.insert(transaction.compute_txid());
            }
        }
        if closure.len() == before {
            return closure;
        }
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
            full_rbf: false,
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

    fn child_at(parent: &Transaction, vout: u32, value_sats: u64) -> Transaction {
        let mut transaction = child(parent, value_sats);
        transaction.input[0].previous_output.vout = vout;
        transaction
    }

    fn unchecked_pool(transactions: Vec<Transaction>) -> TransactionAdmissionPool {
        let mut pool = TransactionAdmissionPool::default();
        for transaction in transactions {
            let serialized_len = serialize(&transaction).len();
            pool.entries.push_back(AdmittedTransaction {
                transaction,
                serialized_len,
                fee_sats: 0,
            });
        }
        pool.rebuild_indexes();
        pool
    }

    fn signal_rbf(transaction: &mut Transaction) {
        transaction.input[0].sequence = Sequence(0xffff_fffd);
    }

    fn txids(pool: &TransactionAdmissionPool) -> Vec<Txid> {
        pool.snapshot()
            .iter()
            .map(Transaction::compute_txid)
            .collect()
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
            TransactionAdmissionOutcome::Accepted {
                txid,
                evicted: 0,
                replaced: 0,
            }
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
    fn non_signaling_conflict_is_rejected_without_mutating_the_pool() {
        let (_directory, store) = store();
        let (outpoint, utxo, first) = spend(2);
        store.apply(&[], &[(outpoint.into(), utxo)]).unwrap();
        let mut second = first.clone();
        second.output[0].value = Amount::from_sat(89_000);
        let first_txid = first.compute_txid();
        let mut pool = TransactionAdmissionPool::default();
        pool.admit(&store, first, context()).unwrap();

        assert!(matches!(
            pool.admit(&store, second, context()),
            Err(TransactionAdmissionError::NonReplaceable(conflict))
                if conflict == first_txid
        ));
        assert_eq!(txids(&pool), vec![first_txid]);
    }

    #[test]
    fn full_rbf_replaces_non_signaling_conflicts_without_weakening_fee_rules() {
        let (_directory, store) = store();
        let (outpoint, utxo, original) = spend(71);
        store.apply(&[], &[(outpoint.into(), utxo)]).unwrap();
        let original_txid = original.compute_txid();
        let mut insufficient_fee = original.clone();
        insufficient_fee.output[0].value = Amount::from_sat(90_001);
        let mut replacement = original.clone();
        replacement.output[0].value = Amount::from_sat(89_000);
        let replacement_txid = replacement.compute_txid();
        let mut full_rbf = context();
        full_rbf.full_rbf = true;
        let mut pool = TransactionAdmissionPool::default();
        pool.admit(&store, original, context()).unwrap();

        assert!(matches!(
            pool.admit(&store, insufficient_fee, full_rbf),
            Err(TransactionAdmissionError::InsufficientReplacementFee {
                replacement_fee_sats: 9_999,
                conflicts_fee_sats: 10_000,
            })
        ));
        assert_eq!(txids(&pool), vec![original_txid]);
        let outcome = pool
            .admit_package(&store, vec![replacement], full_rbf)
            .unwrap();
        assert_eq!(outcome.replaced, 1);
        assert_eq!(txids(&pool), vec![replacement_txid]);
    }

    #[test]
    fn ancestor_and_descendant_count_limits_reject_atomically_at_twenty_six() {
        let (_directory, store) = store();
        let (chain_outpoint, chain_utxo, chain_parent) = spend(69);
        store
            .apply(&[], &[(chain_outpoint.into(), chain_utxo)])
            .unwrap();
        let mut chain = vec![chain_parent.clone()];
        let mut previous = chain_parent;
        for index in 1..MAX_ANCESTOR_TRANSACTIONS {
            let next = child(&previous, 90_000 - u64::try_from(index).unwrap() * 1_000);
            chain.push(next.clone());
            previous = next;
        }
        let mut chain_pool = TransactionAdmissionPool::default();
        chain_pool.admit_package(&store, chain, context()).unwrap();
        let before = chain_pool.snapshot();
        let twenty_sixth = child(&previous, 65_000);
        let twenty_sixth_txid = twenty_sixth.compute_txid();
        assert!(matches!(
            chain_pool.admit(&store, twenty_sixth, context()),
            Err(TransactionAdmissionError::TooManyAncestors {
                txid,
                count: 26,
                limit: MAX_ANCESTOR_TRANSACTIONS,
            }) if txid == twenty_sixth_txid
        ));
        assert_eq!(chain_pool.snapshot(), before);

        let (fanout_outpoint, fanout_utxo, mut fanout_parent) = spend(70);
        store
            .apply(&[], &[(fanout_outpoint.into(), fanout_utxo)])
            .unwrap();
        let output = fanout_parent.output[0].clone();
        fanout_parent.output = (0..MAX_DESCENDANT_TRANSACTIONS)
            .map(|_| TxOut {
                value: Amount::from_sat(3_000),
                script_pubkey: output.script_pubkey.clone(),
            })
            .collect();
        let parent_txid = fanout_parent.compute_txid();
        let mut fanout = vec![fanout_parent.clone()];
        for vout in 0..u32::try_from(MAX_DESCENDANT_TRANSACTIONS - 1).unwrap() {
            fanout.push(child_at(&fanout_parent, vout, 2_000));
        }
        let mut fanout_pool = TransactionAdmissionPool::default();
        fanout_pool
            .admit_package(&store, fanout, context())
            .unwrap();
        let before = fanout_pool.snapshot();
        let last = child_at(
            &fanout_parent,
            u32::try_from(MAX_DESCENDANT_TRANSACTIONS - 1).unwrap(),
            2_000,
        );
        assert!(matches!(
            fanout_pool.admit(&store, last, context()),
            Err(TransactionAdmissionError::TooManyDescendants {
                txid,
                count: 26,
                limit: MAX_DESCENDANT_TRANSACTIONS,
            }) if txid == parent_txid
        ));
        assert_eq!(fanout_pool.snapshot(), before);
    }

    #[test]
    fn ancestor_and_descendant_virtual_size_limits_are_distinct() {
        let (_directory, _store) = store();
        let (_, _, mut large_parent) = spend(67);
        large_parent.output[0].script_pubkey = ScriptBuf::from_bytes(vec![0; 60_000]);
        let mut large_child = child(&large_parent, 80_000);
        large_child.output[0].script_pubkey = ScriptBuf::from_bytes(vec![0; 60_000]);
        assert!(large_parent.vsize() < MAX_ANCESTOR_VBYTES);
        assert!(large_child.vsize() < MAX_ANCESTOR_VBYTES);
        let child_txid = large_child.compute_txid();
        let ancestor_pool = unchecked_pool(vec![large_parent.clone(), large_child.clone()]);
        assert!(matches!(
            ancestor_pool.validate_topology_limits(&[child_txid]),
            Err(TransactionAdmissionError::AncestorsTooLarge {
                txid,
                vbytes,
                limit: MAX_ANCESTOR_VBYTES,
            }) if txid == child_txid && vbytes > MAX_ANCESTOR_VBYTES
        ));

        let (_, _, parent) = spend(68);
        let mut first = child_at(&parent, 0, 80_000);
        first.output[0].script_pubkey = ScriptBuf::from_bytes(vec![0; 60_000]);
        let mut second = child_at(&parent, 1, 79_000);
        second.output[0].script_pubkey = ScriptBuf::from_bytes(vec![0; 60_000]);
        let parent_txid = parent.compute_txid();
        let second_txid = second.compute_txid();
        let descendant_pool = unchecked_pool(vec![parent, first, second]);
        assert!(matches!(
            descendant_pool.validate_topology_limits(&[second_txid]),
            Err(TransactionAdmissionError::DescendantsTooLarge {
                txid,
                vbytes,
                limit: MAX_DESCENDANT_VBYTES,
            }) if txid == parent_txid && vbytes > MAX_DESCENDANT_VBYTES
        ));
    }

    #[test]
    fn signaling_conflict_replaces_the_original_atomically() {
        let (_directory, store) = store();
        let (outpoint, utxo, mut original) = spend(72);
        store.apply(&[], &[(outpoint.into(), utxo)]).unwrap();
        signal_rbf(&mut original);
        let original_txid = original.compute_txid();
        let mut replacement = original.clone();
        replacement.output[0].value = Amount::from_sat(80_000);
        let replacement_txid = replacement.compute_txid();
        let mut pool = TransactionAdmissionPool::default();
        pool.admit(&store, original, context()).unwrap();

        let outcome = pool
            .admit_package(&store, vec![replacement], context())
            .unwrap();
        assert_eq!(outcome.accepted, vec![replacement_txid]);
        assert_eq!(outcome.replaced, 1);
        assert_eq!(outcome.evicted, 0);
        assert_eq!(txids(&pool), vec![replacement_txid]);
        assert!(!txids(&pool).contains(&original_txid));
    }

    #[test]
    fn inherited_rbf_signal_allows_replacing_a_non_signaling_child() {
        let (_directory, store) = store();
        let (outpoint, utxo, mut parent) = spend(73);
        store.apply(&[], &[(outpoint.into(), utxo)]).unwrap();
        signal_rbf(&mut parent);
        let original_child = child(&parent, 80_000);
        let mut replacement = child(&parent, 70_000);
        replacement.version = Version::ONE;
        let parent_txid = parent.compute_txid();
        let replacement_txid = replacement.compute_txid();
        let mut pool = TransactionAdmissionPool::default();
        pool.admit_package(&store, vec![original_child, parent], context())
            .unwrap();

        let outcome = pool
            .admit_package(&store, vec![replacement], context())
            .unwrap();
        assert_eq!(outcome.replaced, 1);
        assert_eq!(txids(&pool), vec![parent_txid, replacement_txid]);
    }

    #[test]
    fn replacement_removes_conflict_descendants_and_pays_their_fees() {
        let (_directory, store) = store();
        let (outpoint, utxo, mut original) = spend(74);
        store.apply(&[], &[(outpoint.into(), utxo)]).unwrap();
        signal_rbf(&mut original);
        let descendant = child(&original, 80_000);
        let mut replacement = original.clone();
        replacement.output[0].value = Amount::from_sat(60_000);
        let replacement_txid = replacement.compute_txid();
        let mut pool = TransactionAdmissionPool::default();
        pool.admit_package(&store, vec![descendant, original], context())
            .unwrap();

        let outcome = pool
            .admit_package(&store, vec![replacement], context())
            .unwrap();
        assert_eq!(outcome.replaced, 2);
        assert_eq!(txids(&pool), vec![replacement_txid]);
    }

    #[test]
    fn replacement_cannot_add_an_unrelated_unconfirmed_input() {
        let (_directory, store) = store();
        let (first_outpoint, first_utxo, mut original) = spend(75);
        let (second_outpoint, second_utxo, unrelated) = spend(76);
        store
            .apply(
                &[],
                &[
                    (first_outpoint.into(), first_utxo),
                    (second_outpoint.into(), second_utxo),
                ],
            )
            .unwrap();
        signal_rbf(&mut original);
        let original_txid = original.compute_txid();
        let unrelated_txid = unrelated.compute_txid();
        let mut replacement = original.clone();
        replacement.input.push(TxIn {
            previous_output: OutPoint::new(unrelated_txid, 0),
            script_sig: ScriptBuf::new(),
            sequence: Sequence::MAX,
            witness: replacement.input[0].witness.clone(),
        });
        replacement.output[0].value = Amount::from_sat(160_000);
        let added = replacement.input[1].previous_output;
        let mut pool = TransactionAdmissionPool::default();
        pool.admit(&store, original, context()).unwrap();
        pool.admit(&store, unrelated, context()).unwrap();
        let before = txids(&pool);

        assert!(matches!(
            pool.admit(&store, replacement, context()),
            Err(TransactionAdmissionError::ReplacementAddsUnconfirmedInput(outpoint))
                if outpoint == added
        ));
        assert_eq!(txids(&pool), before);
        assert_eq!(before, vec![original_txid, unrelated_txid]);
    }

    #[test]
    fn replacement_fee_rules_fail_atomically() {
        let (_directory, store) = store();
        let (outpoint, utxo, mut original) = spend(77);
        store.apply(&[], &[(outpoint.into(), utxo)]).unwrap();
        signal_rbf(&mut original);
        let original_txid = original.compute_txid();
        let mut lower_fee = original.clone();
        lower_fee.output[0].value = Amount::from_sat(90_001);
        let mut insufficient_increment = original.clone();
        insufficient_increment.output[0].value = Amount::from_sat(89_999);
        let mut pool = TransactionAdmissionPool::default();
        pool.admit(&store, original, context()).unwrap();

        assert!(matches!(
            pool.admit(&store, lower_fee, context()),
            Err(TransactionAdmissionError::InsufficientReplacementFee {
                replacement_fee_sats: 9_999,
                conflicts_fee_sats: 10_000,
            })
        ));
        assert_eq!(txids(&pool), vec![original_txid]);
        assert!(matches!(
            pool.admit(&store, insufficient_increment, context()),
            Err(TransactionAdmissionError::InsufficientReplacementRelayFee {
                additional_fee_sats: 1,
                required_fee_sats,
            }) if required_fee_sats > 1
        ));
        assert_eq!(txids(&pool), vec![original_txid]);
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
    fn orphan_pool_is_deduplicated_bounded_and_expires_at_twenty_minutes() {
        let mut pool = TransactionAdmissionPool::default();
        let transactions = (1_u8..=65).map(|index| spend(index).2).collect::<Vec<_>>();
        assert_eq!(
            pool.retain_orphans(&transactions, 100, 1),
            MAX_ORPHAN_TRANSACTIONS
        );
        assert_eq!(pool.orphan_len(), MAX_ORPHAN_TRANSACTIONS);
        assert!(pool.orphan_bytes() <= MAX_ORPHAN_TRANSACTION_BYTES);
        assert_eq!(pool.retain_orphans(&transactions[1..2], 200, 2), 0);
        assert_eq!(pool.prune_orphans(100 + ORPHAN_EXPIRY_SECS - 1), 0);
        assert_eq!(
            pool.prune_orphans(100 + ORPHAN_EXPIRY_SECS),
            MAX_ORPHAN_TRANSACTIONS
        );
        assert_eq!(pool.orphan_bytes(), 0);
    }

    #[test]
    fn newly_admitted_parents_unlock_orphans_one_generation_at_a_time() {
        let (_directory, store) = store();
        let (outpoint, utxo, parent) = spend(66);
        store.apply(&[], &[(outpoint.into(), utxo)]).unwrap();
        let child_transaction = child(&parent, 80_000);
        let grandchild = child(&child_transaction, 70_000);
        let parent_txid = parent.compute_txid();
        let child_txid = child_transaction.compute_txid();
        let grandchild_txid = grandchild.compute_txid();
        let mut pool = TransactionAdmissionPool::default();
        assert_eq!(
            pool.retain_orphans(&[grandchild.clone(), child_transaction.clone()], 100, 1,),
            2
        );

        pool.admit(&store, parent, context()).unwrap();
        let children = pool.orphan_children(&BTreeSet::from([parent_txid]), &BTreeSet::new());
        assert_eq!(
            children
                .iter()
                .map(Transaction::compute_txid)
                .collect::<Vec<_>>(),
            vec![child_txid]
        );
        pool.admit_package(&store, children, context()).unwrap();
        pool.remove_orphans(&BTreeSet::from([child_txid]));

        let grandchildren = pool.orphan_children(&BTreeSet::from([child_txid]), &BTreeSet::new());
        assert_eq!(
            grandchildren
                .iter()
                .map(Transaction::compute_txid)
                .collect::<Vec<_>>(),
            vec![grandchild_txid]
        );
        pool.admit_package(&store, grandchildren, context())
            .unwrap();
        pool.remove_orphans(&BTreeSet::from([grandchild_txid]));
        assert_eq!(pool.len(), 3);
        assert_eq!(pool.orphan_len(), 0);
    }

    #[test]
    fn orphan_disconnect_cleanup_is_isolated_by_source() {
        let mut pool = TransactionAdmissionPool::default();
        let first = spend(82).2;
        let second = spend(83).2;
        assert_eq!(pool.retain_orphans(&[first], 100, 11), 1);
        assert_eq!(pool.retain_orphans(&[second], 100, 12), 1);
        let before_bytes = pool.orphan_bytes();
        assert_eq!(pool.orphan_children_by_parent.len(), 2);

        assert_eq!(pool.remove_orphans_from(11), 1);
        assert_eq!(pool.orphan_len(), 1);
        assert!(pool.orphan_bytes() < before_bytes);
        assert_eq!(pool.orphan_children_by_parent.len(), 1);
        assert_eq!(pool.remove_orphans_from(11), 0);
        assert_eq!(pool.remove_orphans_from(12), 1);
        assert_eq!(pool.orphan_bytes(), 0);
        assert!(pool.orphan_children_by_parent.is_empty());
    }

    #[test]
    fn connected_block_removes_exact_outpoint_conflicts_only() {
        let mut pool = TransactionAdmissionPool::default();
        let first = spend(84).2;
        let reannounced_after_reorg = first.clone();
        let first_txid = first.compute_txid();
        let mut other_output = first.clone();
        other_output.input[0].previous_output.vout = 1;
        let other_output_txid = other_output.compute_txid();
        let unrelated = spend(85).2;
        let unrelated_txid = unrelated.compute_txid();
        assert_eq!(
            pool.retain_orphans(&[first.clone(), other_output.clone(), unrelated], 100, 1,),
            3
        );
        let mut block_transaction = first;
        block_transaction.output[0].value = Amount::from_sat(89_999);

        assert_eq!(
            pool.remove_orphans_for_block_transactions([&block_transaction]),
            1
        );
        let retained = pool
            .orphans
            .iter()
            .map(|orphan| orphan.transaction.compute_txid())
            .collect::<BTreeSet<_>>();
        assert!(!retained.contains(&first_txid));
        assert!(retained.contains(&other_output_txid));
        assert!(retained.contains(&unrelated_txid));
        assert_eq!(pool.orphans_by_outpoint.len(), 2);
        assert_eq!(pool.retain_orphans(&[reannounced_after_reorg], 200, 2), 1);
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
    fn capacity_bump_rejects_low_fees_until_block_enabled_decay_clears_it() {
        let (_directory, store) = store();
        let mut pool = TransactionAdmissionPool::default();
        for index in 1..=u8::try_from(MAX_ADMITTED_TRANSACTIONS + 1).unwrap() {
            let (outpoint, utxo, transaction) = spend(index);
            store.apply(&[], &[(outpoint.into(), utxo)]).unwrap();
            pool.admit_at(&store, transaction, context(), 100).unwrap();
        }
        let bumped = pool.rolling_minimum_fee_sat_kvb(1_000_000);
        assert!(bumped > SATOSHIS_PER_KVB);

        let (outpoint, utxo, mut low_fee) = spend(200);
        low_fee.output[0].value = Amount::from_sat(99_800);
        store.apply(&[], &[(outpoint.into(), utxo)]).unwrap();
        let before = txids(&pool);
        assert!(matches!(
            pool.admit_at(&store, low_fee.clone(), context(), 200),
            Err(TransactionAdmissionError::RollingMinimumFee { .. })
        ));
        assert_eq!(txids(&pool), before);

        let first_tip = BlockHash::from_byte_array([1; 32]);
        let second_tip = BlockHash::from_byte_array([2; 32]);
        pool.observe_chain_tip(first_tip, 200);
        assert_eq!(pool.rolling_minimum_fee_sat_kvb(200 + 48 * 60 * 60), bumped);
        pool.observe_chain_tip(second_tip, 300);
        assert_eq!(pool.rolling_minimum_fee_sat_kvb(300 + 48 * 60 * 60), 0);
        pool.admit_at(&store, low_fee, context(), 300 + 48 * 60 * 60)
            .unwrap();
    }

    #[test]
    fn rolling_fee_uses_accelerated_half_life_below_quarter_capacity() {
        assert_eq!(fee_for_rate(1_001, 100), 101);
        assert_eq!(fee_for_rate(1, 1), 1);
        let mut pool = TransactionAdmissionPool {
            rolling_minimum_fee_sat_kvb: 8_000,
            ..TransactionAdmissionPool::default()
        };
        let first_tip = BlockHash::from_byte_array([3; 32]);
        let second_tip = BlockHash::from_byte_array([4; 32]);
        pool.observe_chain_tip(first_tip, 100);
        pool.observe_chain_tip(second_tip, 200);
        assert_eq!(
            pool.rolling_minimum_fee_sat_kvb(200 + ROLLING_FEE_HALFLIFE_SECS / 4),
            4_000
        );
        pool.rolling_minimum_fee_sat_kvb = 600;
        assert_eq!(pool.rolling_minimum_fee_sat_kvb(11_001), SATOSHIS_PER_KVB);
    }

    #[test]
    fn active_chain_reconciliation_does_not_reprice_retained_transactions() {
        let (_directory, store) = store();
        let (outpoint, utxo, transaction) = spend(201);
        store.apply(&[], &[(outpoint.into(), utxo)]).unwrap();
        let txid = transaction.compute_txid();
        let mut pool = TransactionAdmissionPool::default();
        pool.admit(&store, transaction, context()).unwrap();
        pool.rolling_minimum_fee_sat_kvb = 1_000_000;

        assert_eq!(pool.reconcile(&store, context()), 0);
        assert_eq!(txids(&pool), vec![txid]);
        assert_eq!(pool.rolling_minimum_fee_sat_kvb(0), 1_000_000);
    }

    #[test]
    fn explicit_removal_drops_descendants_and_preserves_unrelated_entries() {
        let (_directory, store) = store();
        let (parent_outpoint, parent_utxo, parent) = spend(78);
        let (unrelated_outpoint, unrelated_utxo, unrelated) = spend(79);
        store
            .apply(
                &[],
                &[
                    (parent_outpoint.into(), parent_utxo),
                    (unrelated_outpoint.into(), unrelated_utxo),
                ],
            )
            .unwrap();
        let child = child(&parent, 80_000);
        let parent_txid = parent.compute_txid();
        let unrelated_txid = unrelated.compute_txid();
        let mut pool = TransactionAdmissionPool::default();
        pool.admit_package(&store, vec![child, parent], context())
            .unwrap();
        pool.admit(&store, unrelated, context()).unwrap();

        assert_eq!(
            pool.remove_with_descendants(&BTreeSet::from([parent_txid])),
            2
        );
        assert_eq!(txids(&pool), vec![unrelated_txid]);
    }

    #[test]
    fn snapshot_descendant_closure_handles_unordered_multigeneration_entries() {
        let (_, _, parent) = spend(80);
        let child_transaction = child(&parent, 80_000);
        let grandchild = child(&child_transaction, 70_000);
        let (_, _, unrelated) = spend(81);
        let parent_txid = parent.compute_txid();
        assert_eq!(
            transaction_descendant_closure(
                &[
                    grandchild.clone(),
                    unrelated,
                    child_transaction.clone(),
                    parent,
                ],
                &BTreeSet::from([parent_txid]),
            ),
            BTreeSet::from([
                parent_txid,
                child_transaction.compute_txid(),
                grandchild.compute_txid()
            ])
        );
    }

    #[test]
    fn topology_rejection_precedes_capacity_eviction_without_mutation() {
        let (_directory, store) = store();
        let (outpoint, mut utxo, mut transaction) = spend(71);
        utxo.value_sats = 10_000_000;
        transaction.output[0].value = Amount::from_sat(9_999_000);
        store.apply(&[], &[(outpoint.into(), utxo)]).unwrap();
        let mut pool = TransactionAdmissionPool::default();
        pool.admit(&store, transaction.clone(), context()).unwrap();

        let mut value = 9_998_000;
        for _ in 1..MAX_ANCESTOR_TRANSACTIONS {
            transaction = child(&transaction, value);
            pool.admit(&store, transaction.clone(), context()).unwrap();
            value -= 1_000;
        }
        for index in
            100..100 + u8::try_from(MAX_ADMITTED_TRANSACTIONS - MAX_ANCESTOR_TRANSACTIONS).unwrap()
        {
            let (outpoint, utxo, unrelated) = spend(index);
            store.apply(&[], &[(outpoint.into(), utxo)]).unwrap();
            pool.admit(&store, unrelated, context()).unwrap();
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
            Err(TransactionAdmissionError::TooManyAncestors {
                count: 26,
                limit: MAX_ANCESTOR_TRANSACTIONS,
                ..
            })
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
