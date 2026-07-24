//! Bounded local admission for independently received transactions.
//!
//! Confirmed inputs and outputs of already-admitted parents are available to a
//! bounded package overlay. Replacement follows BIP125 policy by default;
//! explicit full-RBF mode bypasses only the signaling requirement.

use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    sync::Mutex,
};

use bitcoin::{
    BlockHash, OutPoint, ScriptBuf, Transaction, Txid, Wtxid, consensus::encode::serialize,
    hashes::sha256d,
};
use rand::Rng;
use thiserror::Error;

use crate::{
    chainstate::{ChainstateError, apply_transaction_with_context},
    consensus::{ConsensusError, verify_transaction_scripts_with_flags},
    transaction_policy::{
        TransactionPolicyError, validate_standard_inputs, validate_standard_transaction,
    },
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
/// Maximum distinct blind parent requests retained across the orphanage.
pub const MAX_ORPHAN_PARENT_REQUESTS: usize = 64;
/// Maximum exact witness-independent transaction rejects remembered per chain tip.
pub const MAX_RECENT_REJECTED_TXIDS: usize = 1_024;
/// Core 26's rolling-filter capacity for recently confirmed txid/wtxid identifiers.
pub const MAX_RECENT_CONFIRMED_TRANSACTION_IDS: usize = 48_000;
/// Maximum announcements tracked across sixteen bounded peer sessions.
pub const MAX_TRANSACTION_REQUEST_ANNOUNCEMENTS: usize = 1_024;
/// Maximum announcements tracked from one peer session.
pub const MAX_TRANSACTION_REQUEST_ANNOUNCEMENTS_PER_SOURCE: usize = 64;
/// Core 26's interval before retrying one transaction through another peer.
pub const TRANSACTION_REQUEST_TIMEOUT_SECS: u64 = 60;
/// Core-compatible orphan lifetime of twenty minutes.
pub const ORPHAN_EXPIRY_SECS: u32 = 20 * 60;
/// Core's twelve-hour rolling mempool minimum-fee half-life.
pub const ROLLING_FEE_HALFLIFE_SECS: u32 = 12 * 60 * 60;
const SATOSHIS_PER_KVB: u64 = 1_000;
const DEFAULT_BYTES_PER_SIGOP: usize = 20;
/// Core 26's per-transaction standard sigop cost ceiling.
pub const MAX_STANDARD_TRANSACTION_SIGOP_COST: u64 = 16_000;
/// Core 26 standard-script flags expressible through the public consensus ABI.
pub const PUBLIC_STANDARD_SCRIPT_VERIFY_FLAGS: u32 =
    bitcoinconsensus::VERIFY_ALL_PRE_TAPROOT | bitcoinconsensus::VERIFY_TAPROOT;
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
/// Core's single-transaction CPFP descendant-size carve-out.
pub const EXTRA_DESCENDANT_TRANSACTION_VBYTES: usize = 10_000;
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
    /// A public standard-script flag rejected an otherwise active-consensus-valid input.
    #[error("standard script policy: {0}")]
    StandardScript(#[source] ConsensusError),
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
    /// A child-with-parents package is below the rolling mempool minimum.
    #[error("package fee {fee_sats} is below rolling mempool minimum {minimum_sats}")]
    PackageRollingMinimumFee {
        /// Aggregate package fee.
        fee_sats: u64,
        /// Aggregate fee required at the current rolling rate.
        minimum_sats: u64,
    },
    /// A transaction exceeds Core's standard per-transaction sigop budget.
    #[error("transaction sigop cost {cost} exceeds standard limit {limit}")]
    TooManySigops {
        /// Consensus-derived transaction sigop cost.
        cost: u64,
        /// Standard policy ceiling.
        limit: u64,
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

/// Fee metadata required to apply per-peer transaction announcement filters.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdmittedTransactionRelay {
    /// Full witness transaction.
    pub transaction: Transaction,
    /// Exact fee derived from validated prevouts.
    pub fee_sats: u64,
    /// Sigop-adjusted mempool virtual size.
    pub policy_vsize: usize,
}

#[derive(Clone)]
struct AdmittedTransaction {
    transaction: Transaction,
    serialized_len: usize,
    fee_sats: u64,
    policy_vsize: usize,
}

#[derive(Clone)]
struct OrphanTransaction {
    transaction: Transaction,
    serialized_len: usize,
    expires_at: u32,
    source: u64,
}

/// Transaction identifier retained by the shared multi-peer request scheduler.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransactionRequestId {
    /// Legacy txid announcement or parent request.
    Txid(Txid),
    /// BIP339 witness-transaction identifier.
    Wtxid(Wtxid),
}

impl TransactionRequestId {
    fn raw_hash(self) -> sha256d::Hash {
        match self {
            Self::Txid(txid) => txid.to_raw_hash(),
            Self::Wtxid(wtxid) => wtxid.to_raw_hash(),
        }
    }
}

#[derive(Clone)]
enum TransactionRequestState {
    Candidate { not_before: u64 },
    InFlight { expires_at: u64 },
    Completed,
}

#[derive(Clone)]
struct TransactionRequestAnnouncement {
    id: TransactionRequestId,
    source: u64,
    sequence: u64,
    state: TransactionRequestState,
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
    orphans_by_outpoint: BTreeMap<OutPoint, BTreeSet<Txid>>,
    orphan_work_by_source: BTreeMap<u64, BTreeSet<Txid>>,
    orphan_parent_requests_by_source: BTreeMap<u64, BTreeSet<Txid>>,
    recent_rejected_txids: VecDeque<Txid>,
    recent_rejected_txid_set: BTreeSet<Txid>,
    recent_confirmed_ids: VecDeque<sha256d::Hash>,
    recent_confirmed_id_set: BTreeSet<sha256d::Hash>,
    transaction_request_announcements: VecDeque<TransactionRequestAnnouncement>,
    next_transaction_request_sequence: u64,
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
            self.recent_rejected_txids.clear();
            self.recent_rejected_txid_set.clear();
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

    /// Schedules live children of newly accepted parents for their source peer.
    pub fn schedule_orphan_children(&mut self, parents: &BTreeSet<Txid>) -> usize {
        self.orphan_parent_requests_by_source.retain(|_, requests| {
            requests.retain(|txid| !parents.contains(txid));
            !requests.is_empty()
        });
        let mut child_txids = BTreeSet::<Txid>::new();
        for parent in parents {
            let Some(transaction) = self
                .entries
                .iter()
                .find(|entry| entry.transaction.compute_txid() == *parent)
                .map(|entry| &entry.transaction)
            else {
                continue;
            };
            for vout in 0..transaction.output.len() {
                let Ok(vout) = u32::try_from(vout) else {
                    break;
                };
                if let Some(children) = self.orphans_by_outpoint.get(&OutPoint {
                    txid: *parent,
                    vout,
                }) {
                    child_txids.extend(children.iter().copied());
                }
            }
        }
        let mut scheduled = 0;
        for orphan in &self.orphans {
            let txid = orphan.transaction.compute_txid();
            if child_txids.contains(&txid)
                && self
                    .orphan_work_by_source
                    .entry(orphan.source)
                    .or_default()
                    .insert(txid)
            {
                scheduled += 1;
            }
        }
        scheduled
    }

    /// Pops one scheduled orphan for a peer, leaving other work for later passes.
    pub fn take_orphan_work(&mut self, source: u64) -> Option<Transaction> {
        loop {
            let txid = self
                .orphan_work_by_source
                .get(&source)
                .and_then(|work| work.first().copied())?;
            let work = self
                .orphan_work_by_source
                .get_mut(&source)
                .expect("selected orphan work source remains present");
            work.remove(&txid);
            if work.is_empty() {
                self.orphan_work_by_source.remove(&source);
            }
            if let Some(orphan) = self
                .orphans
                .iter()
                .find(|orphan| orphan.source == source && orphan.transaction.compute_txid() == txid)
            {
                return Some(orphan.transaction.clone());
            }
        }
    }

    /// Returns whether a peer has another scheduled orphan validation unit.
    #[must_use]
    pub fn has_orphan_work(&self, source: u64) -> bool {
        self.orphan_work_by_source
            .get(&source)
            .is_some_and(|work| !work.is_empty())
    }

    /// Returns whether the active pool, orphanage, or recent chain knows a txid.
    #[must_use]
    pub fn knows_transaction(&self, txid: Txid) -> bool {
        self.entries
            .iter()
            .any(|entry| entry.transaction.compute_txid() == txid)
            || self
                .orphans
                .iter()
                .any(|orphan| orphan.transaction.compute_txid() == txid)
            || self.is_recently_confirmed_txid(txid)
    }

    /// Returns whether the active pool, orphanage, or recent chain knows a wtxid.
    #[must_use]
    pub fn knows_wtxid(&self, wtxid: Wtxid) -> bool {
        self.entries
            .iter()
            .any(|entry| entry.transaction.compute_wtxid() == wtxid)
            || self
                .orphans
                .iter()
                .any(|orphan| orphan.transaction.compute_wtxid() == wtxid)
            || self.is_recently_confirmed_wtxid(wtxid)
    }

    /// Returns whether a recently connected block contained this txid.
    #[must_use]
    pub fn is_recently_confirmed_txid(&self, txid: Txid) -> bool {
        self.recent_confirmed_id_set.contains(&txid.to_raw_hash())
    }

    /// Returns whether a recently connected block contained this wtxid.
    #[must_use]
    pub fn is_recently_confirmed_wtxid(&self, wtxid: Wtxid) -> bool {
        self.recent_confirmed_id_set.contains(&wtxid.to_raw_hash())
    }

    /// Adds connected-block identifiers under Core's 48,000-entry filter capacity.
    pub fn remember_confirmed_transactions<'a>(
        &mut self,
        transactions: impl IntoIterator<Item = &'a Transaction>,
    ) -> usize {
        let mut inserted = 0;
        let mut confirmed_txids = BTreeSet::new();
        for transaction in transactions {
            self.forget_transaction_requests_for(transaction);
            let txid = transaction.compute_txid();
            confirmed_txids.insert(txid);
            let txid_hash = txid.to_raw_hash();
            inserted += usize::from(self.remember_recent_confirmed_id(txid_hash));
            let wtxid_hash = transaction.compute_wtxid().to_raw_hash();
            if wtxid_hash != txid_hash {
                inserted += usize::from(self.remember_recent_confirmed_id(wtxid_hash));
            }
        }
        self.orphan_parent_requests_by_source.retain(|_, requests| {
            requests.retain(|txid| !confirmed_txids.contains(txid));
            !requests.is_empty()
        });
        inserted
    }

    fn remember_recent_confirmed_id(&mut self, id: sha256d::Hash) -> bool {
        if !self.recent_confirmed_id_set.insert(id) {
            return false;
        }
        self.recent_confirmed_ids.push_back(id);
        while self.recent_confirmed_ids.len() > MAX_RECENT_CONFIRMED_TRANSACTION_IDS {
            let removed = self
                .recent_confirmed_ids
                .pop_front()
                .expect("over-capacity recent confirmed cache is non-empty");
            self.recent_confirmed_id_set.remove(&removed);
        }
        true
    }

    /// Clears the recent-confirmed cache after any active-chain disconnection.
    pub fn clear_recent_confirmed_transactions(&mut self) {
        self.recent_confirmed_ids.clear();
        self.recent_confirmed_id_set.clear();
    }

    /// Records source announcements under global and per-session hard limits.
    pub fn announce_transaction_requests(
        &mut self,
        source: u64,
        ids: impl IntoIterator<Item = TransactionRequestId>,
        not_before: u64,
    ) -> usize {
        let mut inserted = 0;
        for id in ids {
            let raw_hash = id.raw_hash();
            if self.transaction_request_announcements.len() >= MAX_TRANSACTION_REQUEST_ANNOUNCEMENTS
                || self
                    .transaction_request_announcements
                    .iter()
                    .filter(|announcement| announcement.source == source)
                    .count()
                    >= MAX_TRANSACTION_REQUEST_ANNOUNCEMENTS_PER_SOURCE
                || self
                    .transaction_request_announcements
                    .iter()
                    .any(|announcement| {
                        announcement.source == source && announcement.id.raw_hash() == raw_hash
                    })
            {
                continue;
            }
            let sequence = self.next_transaction_request_sequence;
            self.next_transaction_request_sequence = self
                .next_transaction_request_sequence
                .checked_add(1)
                .expect("transaction announcement sequence exhausted");
            self.transaction_request_announcements
                .push_back(TransactionRequestAnnouncement {
                    id,
                    source,
                    sequence,
                    state: TransactionRequestState::Candidate { not_before },
                });
            inserted += 1;
        }
        inserted
    }

    /// Returns the number of bounded peer announcements currently tracked.
    #[must_use]
    pub fn transaction_request_announcement_len(&self) -> usize {
        self.transaction_request_announcements.len()
    }

    /// Selects source requests in announcement order while allowing one in flight per hash.
    pub fn take_transaction_requests(
        &mut self,
        source: u64,
        now: u64,
        limit: usize,
    ) -> Vec<TransactionRequestId> {
        for announcement in &mut self.transaction_request_announcements {
            if matches!(
                announcement.state,
                TransactionRequestState::InFlight { expires_at } if expires_at <= now
            ) {
                announcement.state = TransactionRequestState::Completed;
            }
        }
        self.prune_completed_transaction_requests();

        let in_flight = self
            .transaction_request_announcements
            .iter()
            .filter(|announcement| {
                matches!(announcement.state, TransactionRequestState::InFlight { .. })
            })
            .map(|announcement| announcement.id.raw_hash())
            .collect::<BTreeSet<_>>();
        let mut selected = self
            .transaction_request_announcements
            .iter()
            .enumerate()
            .filter_map(|(index, announcement)| {
                let TransactionRequestState::Candidate { not_before } = announcement.state else {
                    return None;
                };
                let raw_hash = announcement.id.raw_hash();
                (announcement.source == source
                    && not_before <= now
                    && !in_flight.contains(&raw_hash))
                .then_some((announcement.sequence, index))
            })
            .collect::<Vec<_>>();
        selected.sort_unstable();
        selected.truncate(limit);
        let expiry = now.saturating_add(TRANSACTION_REQUEST_TIMEOUT_SECS);
        selected
            .into_iter()
            .map(|(_, index)| {
                let announcement = self
                    .transaction_request_announcements
                    .get_mut(index)
                    .expect("selected transaction announcement remains in bounds");
                announcement.state = TransactionRequestState::InFlight { expires_at: expiry };
                announcement.id
            })
            .collect()
    }

    /// Marks one source response complete so another source may become requestable.
    pub fn complete_transaction_request(&mut self, source: u64, id: TransactionRequestId) -> bool {
        let raw_hash = id.raw_hash();
        let Some(announcement) =
            self.transaction_request_announcements
                .iter_mut()
                .find(|announcement| {
                    announcement.source == source && announcement.id.raw_hash() == raw_hash
                })
        else {
            return false;
        };
        announcement.state = TransactionRequestState::Completed;
        self.prune_completed_transaction_requests();
        true
    }

    /// Forgets every source announcement for a no-longer-needed identifier.
    pub fn forget_transaction_request(&mut self, id: TransactionRequestId) -> usize {
        let raw_hash = id.raw_hash();
        let before = self.transaction_request_announcements.len();
        self.transaction_request_announcements
            .retain(|announcement| announcement.id.raw_hash() != raw_hash);
        before.saturating_sub(self.transaction_request_announcements.len())
    }

    /// Forgets both relay identifiers of a received or confirmed transaction.
    pub fn forget_transaction_requests_for(&mut self, transaction: &Transaction) -> usize {
        let mut removed =
            self.forget_transaction_request(TransactionRequestId::Txid(transaction.compute_txid()));
        removed += self
            .forget_transaction_request(TransactionRequestId::Wtxid(transaction.compute_wtxid()));
        removed
    }

    /// Removes all announcements owned by a disconnected session.
    pub fn disconnect_transaction_request_source(&mut self, source: u64) -> usize {
        let before = self.transaction_request_announcements.len();
        self.transaction_request_announcements
            .retain(|announcement| announcement.source != source);
        self.prune_completed_transaction_requests();
        before.saturating_sub(self.transaction_request_announcements.len())
    }

    fn prune_completed_transaction_requests(&mut self) {
        let live_hashes = self
            .transaction_request_announcements
            .iter()
            .filter(|announcement| {
                !matches!(announcement.state, TransactionRequestState::Completed)
            })
            .map(|announcement| announcement.id.raw_hash())
            .collect::<BTreeSet<_>>();
        self.transaction_request_announcements
            .retain(|announcement| {
                !matches!(announcement.state, TransactionRequestState::Completed)
                    || live_hashes.contains(&announcement.id.raw_hash())
            });
    }

    /// Returns whether a witness-independent rejection is remembered at this tip.
    #[must_use]
    pub fn is_recently_rejected(&self, txid: Txid) -> bool {
        self.recent_rejected_txid_set.contains(&txid)
    }

    /// Remembers one exact txid rejection under a bounded oldest-first cache.
    pub fn remember_rejected_txid(&mut self, txid: Txid) -> bool {
        if !self.recent_rejected_txid_set.insert(txid) {
            return false;
        }
        self.recent_rejected_txids.push_back(txid);
        while self.recent_rejected_txids.len() > MAX_RECENT_REJECTED_TXIDS {
            let removed = self
                .recent_rejected_txids
                .pop_front()
                .expect("over-capacity recent reject cache is non-empty");
            self.recent_rejected_txid_set.remove(&removed);
        }
        true
    }

    /// Caches terminal failures only when every witness variant has the same result.
    pub fn remember_terminal_rejection(
        &mut self,
        transaction: &Transaction,
        error: &TransactionAdmissionError,
    ) -> bool {
        let txid = transaction.compute_txid();
        let witness_independent = txid.to_raw_hash() == transaction.compute_wtxid().to_raw_hash()
            || matches!(
                error,
                TransactionAdmissionError::Policy(
                    TransactionPolicyError::InputScript(_)
                        | TransactionPolicyError::P2shRedeemScript(_)
                        | TransactionPolicyError::P2shSigops { .. }
                        | TransactionPolicyError::UpgradableWitnessProgram(_)
                )
            );
        witness_independent && self.remember_rejected_txid(txid)
    }

    /// Selects one bounded, deduplicated batch of not-yet-requested parent txids.
    pub fn select_orphan_parent_requests(
        &mut self,
        source: u64,
        candidates: &BTreeSet<Txid>,
    ) -> Vec<Txid> {
        let mut remaining = MAX_ORPHAN_PARENT_REQUESTS.saturating_sub(
            self.orphan_parent_requests_by_source
                .values()
                .map(BTreeSet::len)
                .sum(),
        );
        let mut selected = Vec::new();
        for txid in candidates {
            if remaining == 0
                || self.is_recently_rejected(*txid)
                || self
                    .orphan_parent_requests_by_source
                    .values()
                    .any(|requests| requests.contains(txid))
            {
                continue;
            }
            self.orphan_parent_requests_by_source
                .entry(source)
                .or_default()
                .insert(*txid);
            selected.push(*txid);
            remaining -= 1;
        }
        selected
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
        self.disconnect_transaction_request_source(source);
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
        self.orphans_by_outpoint.clear();
        let mut orphan_sources = BTreeMap::new();
        let mut live_parents_by_source = BTreeMap::<u64, BTreeSet<Txid>>::new();
        for orphan in &self.orphans {
            let txid = orphan.transaction.compute_txid();
            orphan_sources.insert(txid, orphan.source);
            for input in &orphan.transaction.input {
                live_parents_by_source
                    .entry(orphan.source)
                    .or_default()
                    .insert(input.previous_output.txid);
                self.orphans_by_outpoint
                    .entry(input.previous_output)
                    .or_default()
                    .insert(txid);
            }
        }
        self.orphan_work_by_source.retain(|source, work| {
            work.retain(|txid| orphan_sources.get(txid) == Some(source));
            !work.is_empty()
        });
        self.orphan_parent_requests_by_source
            .retain(|source, requests| {
                requests.retain(|txid| {
                    live_parents_by_source
                        .get(source)
                        .is_some_and(|parents| parents.contains(txid))
                });
                !requests.is_empty()
            });
    }

    /// Clones admitted transactions in oldest-to-newest order.
    #[must_use]
    pub fn snapshot(&self) -> Vec<Transaction> {
        self.entries
            .iter()
            .map(|entry| entry.transaction.clone())
            .collect()
    }

    /// Clones the active pool with exact fee and policy-vsize relay metadata.
    #[must_use]
    pub fn relay_snapshot(&self) -> Vec<AdmittedTransactionRelay> {
        self.entries
            .iter()
            .map(|entry| AdmittedTransactionRelay {
                transaction: entry.transaction.clone(),
                fee_sats: entry.fee_sats,
                policy_vsize: entry.policy_vsize,
            })
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
        let allow_cpfp_carveout = transactions.len() == 1;
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
        let child_with_parents = is_child_with_parents(&ordered);
        let replacement = self.prepare_replacement(&ordered, context.full_rbf)?;
        let use_package_feerate = child_with_parents && replacement.txids.is_empty();

        let overlay = AdmissionUtxoOverlay::new(store);
        for entry in &self.entries {
            let _ = apply_to_overlay(&overlay, &entry.transaction, context)?;
        }
        let (accepted, replacement_fee_sats, replacement_vbytes) =
            self.append_ordered_package(&overlay, ordered, context, use_package_feerate)?;
        if replacement_vbytes > MAX_PACKAGE_VBYTES {
            return Err(TransactionAdmissionError::PackageTooLarge {
                vbytes: replacement_vbytes,
                limit: MAX_PACKAGE_VBYTES,
            });
        }
        self.validate_topology_limits(&accepted, allow_cpfp_carveout)?;
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
        use_package_feerate: bool,
    ) -> Result<(Vec<Txid>, u64, usize), TransactionAdmissionError> {
        let mut package_spent = BTreeSet::new();
        let mut accepted = Vec::with_capacity(ordered.len());
        let mut replacement_fee_sats = 0_u64;
        let mut replacement_vbytes = 0_usize;
        let rolling_rate = self
            .effective_rolling_minimum_fee_sat_kvb()
            .max(SATOSHIS_PER_KVB);
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
            let applied = apply_to_overlay(overlay, &transaction, context)?;
            let individual_rate = if use_package_feerate {
                SATOSHIS_PER_KVB
            } else {
                rolling_rate
            };
            let minimum_sats = fee_for_rate(individual_rate, applied.policy_vsize);
            if applied.fee_sats < minimum_sats {
                return Err(TransactionAdmissionError::RollingMinimumFee {
                    fee_sats: applied.fee_sats,
                    minimum_sats,
                });
            }
            replacement_fee_sats = replacement_fee_sats.saturating_add(applied.fee_sats);
            replacement_vbytes = replacement_vbytes.saturating_add(applied.policy_vsize);
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
                fee_sats: applied.fee_sats,
                policy_vsize: applied.policy_vsize,
            });
            accepted.push(txid);
        }
        if use_package_feerate {
            let minimum_sats = fee_for_rate(rolling_rate, replacement_vbytes);
            if replacement_fee_sats < minimum_sats {
                return Err(TransactionAdmissionError::PackageRollingMinimumFee {
                    fee_sats: replacement_fee_sats,
                    minimum_sats,
                });
            }
        }
        Ok((accepted, replacement_fee_sats, replacement_vbytes))
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
                        vbytes.saturating_add(entry.policy_vsize),
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
                total.saturating_add(entry.policy_vsize)
            })
    }

    fn validate_topology_limits(
        &self,
        accepted: &[Txid],
        allow_cpfp_carveout: bool,
    ) -> Result<(), TransactionAdmissionError> {
        let mut affected_ancestors = BTreeSet::new();
        let mut cpfp_carveout_parent = None;
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
            if allow_cpfp_carveout && accepted.len() == 1 && ancestors.len() == 2 {
                let policy_vsize = self
                    .entries
                    .iter()
                    .find(|entry| entry.transaction.compute_txid() == *txid)
                    .map_or(usize::MAX, |entry| entry.policy_vsize);
                if policy_vsize <= EXTRA_DESCENDANT_TRANSACTION_VBYTES {
                    cpfp_carveout_parent =
                        ancestors.iter().copied().find(|ancestor| ancestor != txid);
                }
            }
            affected_ancestors.extend(ancestors);
        }
        for txid in affected_ancestors {
            let descendants = self.descendant_closure(txid);
            let uses_carveout = cpfp_carveout_parent == Some(txid);
            let count_limit = MAX_DESCENDANT_TRANSACTIONS + usize::from(uses_carveout);
            if descendants.len() > count_limit {
                return Err(TransactionAdmissionError::TooManyDescendants {
                    txid,
                    count: descendants.len(),
                    limit: count_limit,
                });
            }
            let vbytes = self.closure_vbytes(&descendants);
            let vbytes_limit = MAX_DESCENDANT_VBYTES
                + usize::from(uses_carveout) * EXTRA_DESCENDANT_TRANSACTION_VBYTES;
            if vbytes > vbytes_limit {
                return Err(TransactionAdmissionError::DescendantsTooLarge {
                    txid,
                    vbytes,
                    limit: vbytes_limit,
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

fn is_child_with_parents(transactions: &[Transaction]) -> bool {
    let Some((child, parents)) = transactions.split_last() else {
        return false;
    };
    if parents.is_empty() {
        return false;
    }
    let child_inputs = child
        .input
        .iter()
        .map(|input| input.previous_output.txid)
        .collect::<BTreeSet<_>>();
    parents
        .iter()
        .all(|parent| child_inputs.contains(&parent.compute_txid()))
}

struct AppliedAdmission {
    fee_sats: u64,
    policy_vsize: usize,
}

fn apply_to_overlay<S: UtxoStore>(
    overlay: &AdmissionUtxoOverlay<'_, S>,
    transaction: &Transaction,
    context: TransactionAdmissionContext,
) -> Result<AppliedAdmission, TransactionAdmissionError> {
    let prevouts = transaction
        .input
        .iter()
        .map(|input| {
            let outpoint = OutPointKey::from(input.previous_output);
            overlay
                .get(outpoint)
                .map_err(ChainstateError::from)?
                .ok_or(ChainstateError::Utxo(UtxoError::Missing(outpoint)))
        })
        .collect::<Result<Vec<Utxo>, ChainstateError>>()?;
    let prevout_scripts = prevouts
        .iter()
        .map(|utxo| ScriptBuf::from_bytes(utxo.script_pubkey.clone()))
        .collect::<Vec<_>>();
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
    validate_standard_inputs(transaction, &prevout_scripts)?;
    if context.script_flags & PUBLIC_STANDARD_SCRIPT_VERIFY_FLAGS
        != PUBLIC_STANDARD_SCRIPT_VERIFY_FLAGS
    {
        verify_transaction_scripts_with_flags(
            transaction,
            &prevouts,
            PUBLIC_STANDARD_SCRIPT_VERIFY_FLAGS,
        )
        .map_err(TransactionAdmissionError::StandardScript)?;
    }
    if applied.sigop_cost > MAX_STANDARD_TRANSACTION_SIGOP_COST {
        return Err(TransactionAdmissionError::TooManySigops {
            cost: applied.sigop_cost,
            limit: MAX_STANDARD_TRANSACTION_SIGOP_COST,
        });
    }
    Ok(AppliedAdmission {
        fee_sats,
        policy_vsize: transaction_policy_vsize(transaction, applied.sigop_cost),
    })
}

fn transaction_policy_vsize(transaction: &Transaction, sigop_cost: u64) -> usize {
    let weight = usize::try_from(transaction.weight().to_wu()).unwrap_or(usize::MAX);
    let sigop_weight = usize::try_from(sigop_cost)
        .unwrap_or(usize::MAX)
        .saturating_mul(DEFAULT_BYTES_PER_SIGOP);
    weight.max(sigop_weight).saturating_add(3) / 4
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
        Amount, ScriptBuf, Sequence, Transaction, TxIn, TxOut, Txid, Witness,
        absolute::LockTime,
        blockdata::script::Builder,
        consensus::deserialize,
        hashes::Hash,
        hex::FromHex,
        opcodes,
        script::PushBytesBuf,
        secp256k1::{Keypair, Secp256k1, SecretKey, XOnlyPublicKey},
        taproot::{LeafVersion, TaprootBuilder},
        transaction::Version,
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

    fn transaction_from_hex(encoded: &str) -> Transaction {
        deserialize(&Vec::<u8>::from_hex(encoded).unwrap()).unwrap()
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

    fn taproot_script_path_spend(
        index: u8,
        script: &ScriptBuf,
        leaf_version: LeafVersion,
    ) -> (OutPoint, Utxo, Transaction) {
        let secp = Secp256k1::new();
        let secret = SecretKey::from_slice(&[index; 32]).unwrap();
        let keypair = Keypair::from_secret_key(&secp, &secret);
        let (internal_key, _) = XOnlyPublicKey::from_keypair(&keypair);
        let spend_info = TaprootBuilder::new()
            .add_leaf_with_ver(0, script.clone(), leaf_version)
            .unwrap()
            .finalize(&secp, internal_key)
            .unwrap();
        let control = spend_info
            .control_block(&(script.clone(), leaf_version))
            .unwrap()
            .serialize();
        let outpoint = OutPoint::new(Txid::from_byte_array([index; 32]), 0);
        let output_script = Builder::new().push_opcode(opcodes::OP_TRUE).into_script();
        (
            outpoint,
            Utxo {
                value_sats: 100_000,
                height: 1,
                is_coinbase: false,
                last_touched: 0,
                creation_mtp: 1,
                script_pubkey: ScriptBuf::new_p2tr_tweaked(spend_info.output_key()).into_bytes(),
            },
            Transaction {
                version: Version::TWO,
                lock_time: LockTime::ZERO,
                input: vec![TxIn {
                    previous_output: outpoint,
                    script_sig: ScriptBuf::new(),
                    sequence: Sequence::MAX,
                    witness: Witness::from_slice(&[script.as_bytes(), &control]),
                }],
                output: vec![TxOut {
                    value: Amount::from_sat(90_000),
                    script_pubkey: ScriptBuf::new_p2wsh(&output_script.wscript_hash()),
                }],
            },
        )
    }

    fn unchecked_pool(transactions: Vec<Transaction>) -> TransactionAdmissionPool {
        let mut pool = TransactionAdmissionPool::default();
        for transaction in transactions {
            let serialized_len = serialize(&transaction).len();
            pool.entries.push_back(AdmittedTransaction {
                policy_vsize: transaction.vsize(),
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
    fn input_policy_rejection_does_not_mutate_chainstate_or_pool() {
        let (_directory, store) = store();
        let (outpoint, mut utxo, mut transaction) = spend(86);
        let witness_script = Builder::new()
            .push_opcode(opcodes::all::OP_DROP)
            .push_opcode(opcodes::OP_TRUE)
            .into_script();
        utxo.script_pubkey = ScriptBuf::new_p2wsh(&witness_script.wscript_hash()).into_bytes();
        transaction.input[0].witness = Witness::from_slice(&[
            vec![0; crate::transaction_policy::MAX_STANDARD_P2WSH_STACK_ITEM_SIZE + 1],
            witness_script.as_bytes().to_vec(),
        ]);
        store.apply(&[], &[(outpoint.into(), utxo)]).unwrap();
        let mut pool = TransactionAdmissionPool::default();

        assert!(matches!(
            pool.admit(&store, transaction, context()),
            Err(TransactionAdmissionError::Policy(
                TransactionPolicyError::WitnessStackItemSize { input: 0, item: 0 }
            ))
        ));
        assert!(store.get(OutPointKey::from(outpoint)).unwrap().is_some());
        assert!(pool.is_empty());
        assert_eq!(pool.retained_bytes(), 0);
    }

    #[test]
    fn upgradable_script_policy_rejections_are_consensus_valid_and_atomic() {
        let (_directory, store) = store();
        let future_leaf = LeafVersion::from_consensus(0xc2).unwrap();
        let (future_outpoint, future_utxo, future_transaction) =
            taproot_script_path_spend(91, &ScriptBuf::new(), future_leaf);
        let (success_outpoint, success_utxo, success_transaction) = taproot_script_path_spend(
            92,
            &ScriptBuf::from_bytes(vec![opcodes::all::OP_RESERVED.to_u8()]),
            LeafVersion::TapScript,
        );

        let mut future_program = vec![opcodes::all::OP_PUSHNUM_2.to_u8(), 32];
        future_program.extend([3; 32]);
        let future_program = ScriptBuf::from_bytes(future_program);
        let redeem_push = PushBytesBuf::try_from(future_program.as_bytes().to_vec()).unwrap();
        let wrapped_outpoint = OutPoint::new(Txid::from_byte_array([93; 32]), 0);
        let wrapped_utxo = Utxo {
            value_sats: 100_000,
            height: 1,
            is_coinbase: false,
            last_touched: 0,
            creation_mtp: 1,
            script_pubkey: ScriptBuf::new_p2sh(&future_program.script_hash()).into_bytes(),
        };
        let output_script = Builder::new().push_opcode(opcodes::OP_TRUE).into_script();
        let wrapped_transaction = Transaction {
            version: Version::TWO,
            lock_time: LockTime::ZERO,
            input: vec![TxIn {
                previous_output: wrapped_outpoint,
                script_sig: Builder::new().push_slice(redeem_push).into_script(),
                sequence: Sequence::MAX,
                witness: Witness::new(),
            }],
            output: vec![TxOut {
                value: Amount::from_sat(90_000),
                script_pubkey: ScriptBuf::new_p2wsh(&output_script.wscript_hash()),
            }],
        };
        store
            .apply(
                &[],
                &[
                    (future_outpoint.into(), future_utxo),
                    (success_outpoint.into(), success_utxo),
                    (wrapped_outpoint.into(), wrapped_utxo),
                ],
            )
            .unwrap();

        let mut active_context = context();
        active_context.script_flags |= bitcoinconsensus::VERIFY_TAPROOT;
        let cases = [
            (
                future_outpoint,
                future_transaction,
                TransactionPolicyError::UpgradableTaprootLeafVersion(0),
            ),
            (
                success_outpoint,
                success_transaction,
                TransactionPolicyError::TapscriptOpSuccess(0),
            ),
            (
                wrapped_outpoint,
                wrapped_transaction,
                TransactionPolicyError::UpgradableWitnessProgram(0),
            ),
        ];
        let mut pool = TransactionAdmissionPool::default();
        for (outpoint, transaction, expected) in cases {
            let error = pool.admit(&store, transaction, active_context).unwrap_err();
            let TransactionAdmissionError::Policy(actual) = error else {
                panic!("expected policy rejection, got {error}");
            };
            assert_eq!(actual, expected);
            assert!(store.get(OutPointKey::from(outpoint)).unwrap().is_some());
            assert!(pool.is_empty());
            assert_eq!(pool.retained_bytes(), 0);
        }
    }

    #[test]
    fn public_standard_script_flags_apply_before_custom_activation_and_are_atomic() {
        // Bitcoin Core 26 tx_valid.json: valid without the standard DERSIG and
        // NULLDUMMY flags, rejected when their public standard subset is used.
        let transaction = transaction_from_hex(concat!(
            "0100000001b14bdcbc3e01bdaad36cc08e81e69c82e1060bc14e518db2b49aa43a",
            "d90ba260000000004a01ff47304402203f16c6f40162ab686621ef3000b04e75418",
            "a0c0cb2d8aebeac894ae360ac1e780220ddc15ecdfc3507ac48e1681a33eb6099",
            "6631bf6bf5bc0a0682c4db743ce7ca2b01ffffffff0140420f00000000001976a9",
            "14660d4ef3a743e3e696ad990364e555c271ad504b88ac00000000",
        ));
        let prevout_script = Vec::<u8>::from_hex(concat!(
            "514104cc71eb30d653c0c3163990c47b976f3fb3f37cccdcbedb169a1dfef58bb",
            "fbfaff7d8a473e7e2e6d317b87bafe8bde97e3cf8f065dec022b51d11fcdd0d3",
            "48ac4410461cbdcc5409fb4b4d42b51d33381354d80e550078cb532a34bfa2fcf",
            "deb7d76519aecc62770f5b0e4ef8551946d8a540911abe3e7854a26f39f58b25",
            "c15342af52ae",
        ))
        .unwrap();
        let outpoint = transaction.input[0].previous_output;
        let (_directory, store) = store();
        store
            .apply(
                &[],
                &[(
                    outpoint.into(),
                    Utxo {
                        value_sats: 2_000_000,
                        height: 1,
                        is_coinbase: false,
                        last_touched: 0,
                        creation_mtp: 1,
                        script_pubkey: prevout_script,
                    },
                )],
            )
            .unwrap();
        let mut pre_activation = context();
        pre_activation.script_flags = bitcoinconsensus::VERIFY_NONE;
        pre_activation.csv_active = false;
        let mut pool = TransactionAdmissionPool::default();

        assert!(matches!(
            pool.admit(&store, transaction.clone(), pre_activation),
            Err(TransactionAdmissionError::StandardScript(
                ConsensusError::Script { input: 0, .. }
            ))
        ));
        assert!(store.get(OutPointKey::from(outpoint)).unwrap().is_some());
        assert!(pool.is_empty());
    }

    #[test]
    fn output_policy_rejection_does_not_mutate_chainstate_or_pool() {
        let (_directory, store) = store();
        let (outpoint, utxo, mut transaction) = spend(87);
        let mut builder = Builder::new().push_int(1);
        for _ in 0..=crate::transaction_policy::MAX_STANDARD_BARE_MULTISIG_KEYS {
            builder = builder.push_slice([2; 33]);
        }
        transaction.output[0].script_pubkey = builder
            .push_int(
                i64::try_from(crate::transaction_policy::MAX_STANDARD_BARE_MULTISIG_KEYS + 1)
                    .unwrap(),
            )
            .push_opcode(opcodes::all::OP_CHECKMULTISIG)
            .into_script();
        store.apply(&[], &[(outpoint.into(), utxo)]).unwrap();
        let mut pool = TransactionAdmissionPool::default();

        assert!(matches!(
            pool.admit(&store, transaction, context()),
            Err(TransactionAdmissionError::Policy(
                TransactionPolicyError::OutputScript(0)
            ))
        ));
        assert!(store.get(OutPointKey::from(outpoint)).unwrap().is_some());
        assert!(pool.is_empty());
        assert_eq!(pool.retained_bytes(), 0);
    }

    #[test]
    fn sigop_adjusted_vsize_enforces_the_minimum_relay_fee() {
        let (_directory, store) = store();
        let (outpoint, mut utxo, mut transaction) = spend(88);
        let mut builder = Builder::new().push_int(0).push_opcode(opcodes::all::OP_IF);
        for _ in 0..100 {
            builder = builder.push_opcode(opcodes::all::OP_CHECKSIG);
        }
        let witness_script = builder
            .push_opcode(opcodes::all::OP_ENDIF)
            .push_opcode(opcodes::OP_TRUE)
            .into_script();
        utxo.script_pubkey = ScriptBuf::new_p2wsh(&witness_script.wscript_hash()).into_bytes();
        transaction.input[0].witness = Witness::from_slice(&[witness_script.as_bytes()]);
        transaction.output[0].value = Amount::from_sat(99_800);
        store.apply(&[], &[(outpoint.into(), utxo)]).unwrap();
        let mut pool = TransactionAdmissionPool::default();

        assert!(transaction.vsize() < 200);
        assert!(matches!(
            pool.admit(&store, transaction, context()),
            Err(TransactionAdmissionError::RollingMinimumFee {
                fee_sats: 200,
                minimum_sats: 500,
            })
        ));
        assert!(store.get(OutPointKey::from(outpoint)).unwrap().is_some());
        assert!(pool.is_empty());
    }

    #[test]
    fn standard_transaction_sigop_cost_is_capped_atomically() {
        let (_directory, store) = store();
        let mut builder = Builder::new().push_int(0).push_opcode(opcodes::all::OP_IF);
        for _ in 0..198 {
            builder = builder.push_opcode(opcodes::all::OP_CHECKMULTISIG);
        }
        let witness_script = builder
            .push_opcode(opcodes::all::OP_ENDIF)
            .push_opcode(opcodes::OP_TRUE)
            .into_script();
        let script_pubkey = ScriptBuf::new_p2wsh(&witness_script.wscript_hash());
        let mut created = Vec::new();
        let mut inputs = Vec::new();
        for index in 90..95 {
            let outpoint = OutPoint::new(Txid::from_byte_array([index; 32]), 0);
            created.push((
                outpoint.into(),
                Utxo {
                    value_sats: 100_000,
                    height: 1,
                    is_coinbase: false,
                    last_touched: 0,
                    creation_mtp: 1,
                    script_pubkey: script_pubkey.clone().into_bytes(),
                },
            ));
            inputs.push(TxIn {
                previous_output: outpoint,
                script_sig: ScriptBuf::new(),
                sequence: Sequence::MAX,
                witness: Witness::from_slice(&[witness_script.as_bytes()]),
            });
        }
        store.apply(&[], &created).unwrap();
        let transaction = Transaction {
            version: Version::TWO,
            lock_time: LockTime::ZERO,
            input: inputs,
            output: vec![TxOut {
                value: Amount::from_sat(350_000),
                script_pubkey,
            }],
        };
        let mut pool = TransactionAdmissionPool::default();

        assert_eq!(transaction_policy_vsize(&transaction, 19_800), 99_000);
        assert!(matches!(
            pool.admit(&store, transaction, context()),
            Err(TransactionAdmissionError::TooManySigops {
                cost: 19_800,
                limit: MAX_STANDARD_TRANSACTION_SIGOP_COST,
            })
        ));
        assert!(pool.is_empty());
        for (outpoint, _) in created {
            assert!(store.get(outpoint).unwrap().is_some());
        }
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
        fanout_parent.output = (0..=MAX_DESCENDANT_TRANSACTIONS)
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
        let mut multi_package_pool = fanout_pool.clone();
        assert!(matches!(
            multi_package_pool.admit_package(
                &store,
                vec![last.clone(), fanout_parent.clone()],
                context(),
            ),
            Err(TransactionAdmissionError::TooManyDescendants {
                txid,
                count: 26,
                limit: MAX_DESCENDANT_TRANSACTIONS,
            }) if txid == parent_txid
        ));
        assert_eq!(multi_package_pool.snapshot(), before);

        fanout_pool.admit(&store, last, context()).unwrap();
        assert_eq!(fanout_pool.len(), MAX_DESCENDANT_TRANSACTIONS + 1);
        let before = fanout_pool.snapshot();
        let twenty_seventh = child_at(
            &fanout_parent,
            u32::try_from(MAX_DESCENDANT_TRANSACTIONS).unwrap(),
            2_000,
        );
        assert!(matches!(
            fanout_pool.admit(&store, twenty_seventh, context()),
            Err(TransactionAdmissionError::TooManyDescendants {
                txid,
                count: 27,
                limit: 26,
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
            ancestor_pool.validate_topology_limits(&[child_txid], false),
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
            descendant_pool.validate_topology_limits(&[second_txid], false),
            Err(TransactionAdmissionError::DescendantsTooLarge {
                txid,
                vbytes,
                limit: MAX_DESCENDANT_VBYTES,
            }) if txid == parent_txid && vbytes > MAX_DESCENDANT_VBYTES
        ));
    }

    #[test]
    fn cpfp_carveout_extends_descendant_size_by_ten_thousand_vbytes() {
        let (_, _, mut parent) = spend(96);
        let output = parent.output[0].clone();
        parent.output = vec![output.clone(), output];
        let mut existing_child = child_at(&parent, 0, 80_000);
        existing_child.output[0].script_pubkey = ScriptBuf::from_bytes(vec![0; 100_600]);
        let mut carveout_child = child_at(&parent, 1, 70_000);
        carveout_child.output[0].script_pubkey = ScriptBuf::from_bytes(vec![0; 5_000]);
        let parent_txid = parent.compute_txid();
        let child_txid = carveout_child.compute_txid();
        let pool = unchecked_pool(vec![parent, existing_child, carveout_child]);
        let descendants = pool.descendant_closure(parent_txid);
        let descendant_vbytes = pool.closure_vbytes(&descendants);
        assert!(descendant_vbytes > MAX_DESCENDANT_VBYTES);
        assert!(descendant_vbytes <= MAX_DESCENDANT_VBYTES + EXTRA_DESCENDANT_TRANSACTION_VBYTES);

        assert!(matches!(
            pool.validate_topology_limits(&[child_txid], false),
            Err(TransactionAdmissionError::DescendantsTooLarge {
                txid,
                limit: MAX_DESCENDANT_VBYTES,
                ..
            }) if txid == parent_txid
        ));
        assert!(pool.validate_topology_limits(&[child_txid], true).is_ok());
    }

    #[test]
    fn cpfp_carveout_rejects_a_child_above_ten_thousand_vbytes() {
        let (_, _, mut parent) = spend(97);
        let output = parent.output[0].clone();
        parent.output = (0..MAX_DESCENDANT_TRANSACTIONS)
            .map(|_| output.clone())
            .collect();
        let mut entries = vec![parent.clone()];
        for vout in 0..u32::try_from(MAX_DESCENDANT_TRANSACTIONS - 1).unwrap() {
            entries.push(child_at(&parent, vout, 80_000));
        }
        let mut oversized = child_at(
            &parent,
            u32::try_from(MAX_DESCENDANT_TRANSACTIONS - 1).unwrap(),
            79_000,
        );
        oversized.output[0].script_pubkey =
            ScriptBuf::from_bytes(vec![0; EXTRA_DESCENDANT_TRANSACTION_VBYTES + 1_000]);
        let parent_txid = parent.compute_txid();
        let oversized_txid = oversized.compute_txid();
        assert!(oversized.vsize() > EXTRA_DESCENDANT_TRANSACTION_VBYTES);
        entries.push(oversized);
        let pool = unchecked_pool(entries);

        assert!(matches!(
            pool.validate_topology_limits(&[oversized_txid], true),
            Err(TransactionAdmissionError::TooManyDescendants {
                txid,
                count: 26,
                limit: MAX_DESCENDANT_TRANSACTIONS,
            }) if txid == parent_txid
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
    fn child_with_parents_uses_aggregate_rolling_minimum_fee() {
        let (_directory, store) = store();
        let (outpoint, utxo, mut parent) = spend(98);
        store.apply(&[], &[(outpoint.into(), utxo)]).unwrap();
        let parent_vbytes = parent.vsize();
        let parent_fee = u64::try_from(parent_vbytes).unwrap();
        parent.output[0].value = Amount::from_sat(100_000 - parent_fee);
        let mut child = child(&parent, 80_000);
        let package_vbytes = parent_vbytes + child.vsize();
        let package_minimum = fee_for_rate(5_000, package_vbytes);
        let child_fee = package_minimum - parent_fee;
        child.output[0].value =
            Amount::from_sat(parent.output[0].value.to_sat().saturating_sub(child_fee));
        assert!(parent_fee < fee_for_rate(5_000, parent_vbytes));

        let mut pool = TransactionAdmissionPool {
            rolling_minimum_fee_sat_kvb: 5_000,
            ..TransactionAdmissionPool::default()
        };
        let outcome = pool
            .admit_package(&store, vec![child.clone(), parent.clone()], context())
            .unwrap();
        assert_eq!(outcome.accepted.len(), 2);

        let mut insufficient_child = child;
        insufficient_child.output[0].value =
            Amount::from_sat(insufficient_child.output[0].value.to_sat() + 1);
        let mut insufficient_pool = TransactionAdmissionPool {
            rolling_minimum_fee_sat_kvb: 5_000,
            ..TransactionAdmissionPool::default()
        };
        assert!(matches!(
            insufficient_pool.admit_package(
                &store,
                vec![insufficient_child, parent],
                context(),
            ),
            Err(TransactionAdmissionError::PackageRollingMinimumFee {
                fee_sats,
                minimum_sats,
            }) if fee_sats + 1 == minimum_sats && minimum_sats == package_minimum
        ));
        assert!(insufficient_pool.is_empty());
        assert!(store.get(outpoint.into()).unwrap().is_some());
    }

    #[test]
    fn package_feerate_never_bypasses_individual_minimum_relay() {
        let (_directory, store) = store();
        let (outpoint, utxo, mut parent) = spend(99);
        store.apply(&[], &[(outpoint.into(), utxo)]).unwrap();
        let parent_minimum = u64::try_from(parent.vsize()).unwrap();
        parent.output[0].value = Amount::from_sat(100_000 - (parent_minimum - 1));
        let child = child(
            &parent,
            parent.output[0].value.to_sat().saturating_sub(10_000),
        );
        let mut pool = TransactionAdmissionPool {
            rolling_minimum_fee_sat_kvb: 5_000,
            ..TransactionAdmissionPool::default()
        };

        assert!(matches!(
            pool.admit_package(&store, vec![child, parent], context()),
            Err(TransactionAdmissionError::Policy(
                TransactionPolicyError::FeeRate {
                    fee_sats,
                    minimum_sats,
                }
            )) if fee_sats + 1 == minimum_sats && minimum_sats == parent_minimum
        ));
        assert!(pool.is_empty());
        assert!(store.get(outpoint.into()).unwrap().is_some());
    }

    #[test]
    fn aggregate_rolling_fee_does_not_apply_to_deeper_packages() {
        let (_directory, store) = store();
        let (outpoint, utxo, mut parent) = spend(100);
        store.apply(&[], &[(outpoint.into(), utxo)]).unwrap();
        let parent_fee = u64::try_from(parent.vsize()).unwrap();
        parent.output[0].value = Amount::from_sat(100_000 - parent_fee);
        let middle = child(
            &parent,
            parent.output[0].value.to_sat().saturating_sub(5_000),
        );
        let last = child(
            &middle,
            middle.output[0].value.to_sat().saturating_sub(5_000),
        );
        let mut pool = TransactionAdmissionPool {
            rolling_minimum_fee_sat_kvb: 5_000,
            ..TransactionAdmissionPool::default()
        };

        assert!(!is_child_with_parents(&[
            parent.clone(),
            middle.clone(),
            last.clone(),
        ]));
        assert!(matches!(
            pool.admit_package(&store, vec![last, parent, middle], context()),
            Err(TransactionAdmissionError::RollingMinimumFee {
                fee_sats,
                minimum_sats,
            }) if fee_sats == parent_fee && minimum_sats > parent_fee
        ));
        assert!(pool.is_empty());
        assert!(store.get(outpoint.into()).unwrap().is_some());
    }

    #[test]
    fn aggregate_rolling_fee_does_not_apply_to_partial_or_replacement_packages() {
        let (_directory, store) = store();
        let (partial_outpoint, partial_utxo, mut existing_parent) = spend(101);
        existing_parent.output[0].value = Amount::from_sat(99_000);
        store
            .apply(&[], &[(partial_outpoint.into(), partial_utxo)])
            .unwrap();
        let existing_parent_txid = existing_parent.compute_txid();
        let mut partial_pool = TransactionAdmissionPool::default();
        partial_pool
            .admit(&store, existing_parent.clone(), context())
            .unwrap();
        let mut later_child = child(&existing_parent, 80_000);
        let child_fee = u64::try_from(later_child.vsize()).unwrap();
        later_child.output[0].value =
            Amount::from_sat(existing_parent.output[0].value.to_sat() - child_fee);
        partial_pool.rolling_minimum_fee_sat_kvb = 5_000;

        assert!(matches!(
            partial_pool.admit(&store, later_child, context()),
            Err(TransactionAdmissionError::RollingMinimumFee {
                fee_sats,
                minimum_sats,
            }) if fee_sats == child_fee && minimum_sats > child_fee
        ));
        assert_eq!(txids(&partial_pool), vec![existing_parent_txid]);

        let (replacement_outpoint, replacement_utxo, mut original) = spend(102);
        signal_rbf(&mut original);
        store
            .apply(&[], &[(replacement_outpoint.into(), replacement_utxo)])
            .unwrap();
        let original_txid = original.compute_txid();
        let mut replacement_pool = TransactionAdmissionPool::default();
        replacement_pool
            .admit(&store, original.clone(), context())
            .unwrap();
        replacement_pool.rolling_minimum_fee_sat_kvb = 200_000;
        let mut replacement = original;
        replacement.output[0].value = Amount::from_sat(89_000);
        let replacement_child = child(&replacement, 40_000);
        assert!(is_child_with_parents(&[
            replacement.clone(),
            replacement_child.clone(),
        ]));

        assert!(matches!(
            replacement_pool.admit_package(
                &store,
                vec![replacement_child, replacement],
                context(),
            ),
            Err(TransactionAdmissionError::RollingMinimumFee {
                fee_sats: 11_000,
                minimum_sats,
            }) if minimum_sats > 11_000
        ));
        assert_eq!(txids(&replacement_pool), vec![original_txid]);
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
        let retained = pool.orphans.front().unwrap().transaction.clone();
        assert_eq!(pool.retain_orphans(&[retained], 200, 2), 0);
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
        assert_eq!(
            pool.schedule_orphan_children(&BTreeSet::from([parent_txid])),
            1
        );
        let child_work = pool.take_orphan_work(1).unwrap();
        assert_eq!(child_work.compute_txid(), child_txid);
        assert!(pool.take_orphan_work(1).is_none());
        pool.admit_package(&store, vec![child_work], context())
            .unwrap();
        pool.remove_orphans(&BTreeSet::from([child_txid]));

        assert_eq!(
            pool.schedule_orphan_children(&BTreeSet::from([child_txid])),
            1
        );
        let grandchild_work = pool.take_orphan_work(1).unwrap();
        assert_eq!(grandchild_work.compute_txid(), grandchild_txid);
        pool.admit_package(&store, vec![grandchild_work], context())
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
        assert_eq!(pool.orphans_by_outpoint.len(), 2);

        assert_eq!(pool.remove_orphans_from(11), 1);
        assert_eq!(pool.orphan_len(), 1);
        assert!(pool.orphan_bytes() < before_bytes);
        assert_eq!(pool.orphans_by_outpoint.len(), 1);
        assert_eq!(pool.remove_orphans_from(11), 0);
        assert_eq!(pool.remove_orphans_from(12), 1);
        assert_eq!(pool.orphan_bytes(), 0);
        assert!(pool.orphans_by_outpoint.is_empty());
    }

    #[test]
    fn orphan_work_is_deduplicated_ordered_and_isolated_by_source() {
        let (_directory, store) = store();
        let (outpoint, utxo, parent) = spend(87);
        store.apply(&[], &[(outpoint.into(), utxo)]).unwrap();
        let parent_txid = parent.compute_txid();
        let mut first = child(&parent, 80_000);
        let mut second = first.clone();
        let third = first.clone();
        let mut invalid_vout = first.clone();
        first.output[0].value = Amount::from_sat(79_999);
        second.output[0].value = Amount::from_sat(79_998);
        invalid_vout.input[0].previous_output.vout = 1;
        let first_txid = first.compute_txid();
        let second_txid = second.compute_txid();
        let third_txid = third.compute_txid();
        let mut pool = TransactionAdmissionPool::default();
        assert_eq!(pool.retain_orphans(&[second, first], 100, 11), 2);
        assert_eq!(pool.retain_orphans(&[third], 100, 12), 1);
        assert_eq!(pool.retain_orphans(&[invalid_vout], 100, 13), 1);
        assert_eq!(
            pool.select_orphan_parent_requests(11, &BTreeSet::from([parent_txid])),
            vec![parent_txid]
        );
        pool.admit(&store, parent, context()).unwrap();

        let parents = BTreeSet::from([parent_txid]);
        assert_eq!(pool.schedule_orphan_children(&parents), 3);
        assert!(pool.orphan_parent_requests_by_source.is_empty());
        assert_eq!(pool.schedule_orphan_children(&parents), 0);
        assert!(!pool.has_orphan_work(13));
        let first_work = pool.take_orphan_work(11).unwrap().compute_txid();
        assert!(pool.has_orphan_work(11));
        let second_work = pool.take_orphan_work(11).unwrap().compute_txid();
        assert!(pool.take_orphan_work(11).is_none());
        assert!(!pool.has_orphan_work(11));
        let mut source_eleven = vec![first_work, second_work];
        source_eleven.sort_unstable();
        let mut expected = vec![first_txid, second_txid];
        expected.sort_unstable();
        assert_eq!(source_eleven, expected);

        assert!(pool.has_orphan_work(12));
        assert_eq!(pool.remove_orphans_from(12), 1);
        assert!(pool.take_orphan_work(12).is_none());
        assert!(!pool.has_orphan_work(12));
        assert!(
            !pool
                .orphans
                .iter()
                .any(|orphan| orphan.transaction.compute_txid() == third_txid)
        );
    }

    #[test]
    fn orphan_parent_requests_are_bounded_deduplicated_and_pruned() {
        fn marker_txid(marker: u32) -> Txid {
            let mut bytes = [0_u8; 32];
            bytes[..4].copy_from_slice(&marker.to_le_bytes());
            Txid::from_byte_array(bytes)
        }

        let mut orphan = spend(90).2;
        let input = orphan.input[0].clone();
        let parents = (0..=u32::try_from(MAX_ORPHAN_PARENT_REQUESTS).unwrap())
            .map(marker_txid)
            .collect::<BTreeSet<_>>();
        orphan.input = parents
            .iter()
            .map(|txid| TxIn {
                previous_output: OutPoint::new(*txid, 0),
                ..input.clone()
            })
            .collect();
        let mut pool = TransactionAdmissionPool::default();
        assert_eq!(pool.retain_orphans(&[orphan], 100, 21), 1);
        let rejected = *parents.first().unwrap();
        assert!(pool.remember_rejected_txid(rejected));

        let selected = pool.select_orphan_parent_requests(21, &parents);
        assert_eq!(selected.len(), MAX_ORPHAN_PARENT_REQUESTS);
        assert!(!selected.contains(&rejected));
        assert!(pool.select_orphan_parent_requests(21, &parents).is_empty());
        assert_eq!(
            pool.orphan_parent_requests_by_source
                .values()
                .map(BTreeSet::len)
                .sum::<usize>(),
            MAX_ORPHAN_PARENT_REQUESTS
        );

        assert_eq!(pool.remove_orphans_from(21), 1);
        assert!(pool.orphan_parent_requests_by_source.is_empty());
    }

    #[test]
    fn recent_rejects_are_witness_safe_bounded_and_reset_on_tip_change() {
        fn marker_txid(marker: u32) -> Txid {
            let mut bytes = [0_u8; 32];
            bytes[..4].copy_from_slice(&marker.to_le_bytes());
            Txid::from_byte_array(bytes)
        }

        let mut pool = TransactionAdmissionPool::default();
        let witness_transaction = spend(91).2;
        let witness_dependent = TransactionAdmissionError::Policy(TransactionPolicyError::Dust {
            index: 0,
            value_sats: 1,
            minimum_sats: 2,
        });
        assert!(!pool.remember_terminal_rejection(&witness_transaction, &witness_dependent));
        assert!(!pool.is_recently_rejected(witness_transaction.compute_txid()));
        let input_standardness =
            TransactionAdmissionError::Policy(TransactionPolicyError::InputScript(0));
        assert!(pool.remember_terminal_rejection(&witness_transaction, &input_standardness));
        assert!(pool.is_recently_rejected(witness_transaction.compute_txid()));

        let upgradable_program = spend(93).2;
        let upgradable_standardness =
            TransactionAdmissionError::Policy(TransactionPolicyError::UpgradableWitnessProgram(0));
        assert!(pool.remember_terminal_rejection(&upgradable_program, &upgradable_standardness));
        assert!(pool.is_recently_rejected(upgradable_program.compute_txid()));

        let mut legacy_transaction = spend(92).2;
        legacy_transaction.input[0].witness = Witness::new();
        assert!(pool.remember_terminal_rejection(&legacy_transaction, &witness_dependent));
        assert!(pool.is_recently_rejected(legacy_transaction.compute_txid()));

        let mut bounded = TransactionAdmissionPool::default();
        let tip = BlockHash::from_byte_array([1; 32]);
        bounded.observe_chain_tip(tip, 100);
        for marker in 0..=u32::try_from(MAX_RECENT_REJECTED_TXIDS).unwrap() {
            assert!(bounded.remember_rejected_txid(marker_txid(marker)));
        }
        assert_eq!(
            bounded.recent_rejected_txids.len(),
            MAX_RECENT_REJECTED_TXIDS
        );
        assert!(!bounded.is_recently_rejected(marker_txid(0)));
        assert!(bounded.is_recently_rejected(marker_txid(
            u32::try_from(MAX_RECENT_REJECTED_TXIDS).unwrap()
        )));
        bounded.observe_chain_tip(tip, 101);
        assert!(!bounded.recent_rejected_txids.is_empty());
        bounded.observe_chain_tip(BlockHash::from_byte_array([2; 32]), 102);
        assert!(bounded.recent_rejected_txids.is_empty());
        assert!(bounded.recent_rejected_txid_set.is_empty());
    }

    #[test]
    fn recent_confirmed_ids_are_bounded_disconnect_scoped_and_prune_requests() {
        fn marker_hash(marker: u32) -> sha256d::Hash {
            let mut bytes = [0_u8; 32];
            bytes[..4].copy_from_slice(&marker.to_le_bytes());
            sha256d::Hash::from_byte_array(bytes)
        }

        let confirmed = spend(93).2;
        assert_ne!(
            confirmed.compute_txid().to_raw_hash(),
            confirmed.compute_wtxid().to_raw_hash()
        );
        let mut orphan = spend(94).2;
        orphan.input[0].previous_output = OutPoint::new(confirmed.compute_txid(), 0);
        let mut pool = TransactionAdmissionPool::default();
        assert_eq!(pool.retain_orphans(&[orphan], 100, 31), 1);
        assert_eq!(
            pool.select_orphan_parent_requests(31, &BTreeSet::from([confirmed.compute_txid()])),
            vec![confirmed.compute_txid()]
        );

        assert_eq!(pool.remember_confirmed_transactions([&confirmed]), 2);
        assert!(pool.knows_transaction(confirmed.compute_txid()));
        assert!(pool.is_recently_confirmed_wtxid(confirmed.compute_wtxid()));
        assert!(pool.orphan_parent_requests_by_source.is_empty());
        pool.observe_chain_tip(BlockHash::from_byte_array([1; 32]), 100);
        pool.observe_chain_tip(BlockHash::from_byte_array([2; 32]), 101);
        assert!(pool.is_recently_confirmed_txid(confirmed.compute_txid()));
        pool.clear_recent_confirmed_transactions();
        assert!(!pool.is_recently_confirmed_txid(confirmed.compute_txid()));
        assert!(!pool.is_recently_confirmed_wtxid(confirmed.compute_wtxid()));

        for marker in 0..=u32::try_from(MAX_RECENT_CONFIRMED_TRANSACTION_IDS).unwrap() {
            assert!(pool.remember_recent_confirmed_id(marker_hash(marker)));
        }
        assert_eq!(
            pool.recent_confirmed_ids.len(),
            MAX_RECENT_CONFIRMED_TRANSACTION_IDS
        );
        assert!(!pool.recent_confirmed_id_set.contains(&marker_hash(0)));
        assert!(pool.recent_confirmed_id_set.contains(&marker_hash(
            u32::try_from(MAX_RECENT_CONFIRMED_TRANSACTION_IDS).unwrap()
        )));
    }

    #[test]
    fn transaction_requests_are_bounded_single_flight_and_fail_over_by_source() {
        fn marker_id(marker: u32) -> TransactionRequestId {
            let mut bytes = [0_u8; 32];
            bytes[..4].copy_from_slice(&marker.to_le_bytes());
            TransactionRequestId::Txid(Txid::from_byte_array(bytes))
        }

        let shared = marker_id(1);
        let mut tracker = TransactionAdmissionPool::default();
        assert_eq!(tracker.announce_transaction_requests(41, [shared], 100), 1);
        assert_eq!(tracker.announce_transaction_requests(42, [shared], 100), 1);
        assert_eq!(tracker.take_transaction_requests(42, 100, 64), vec![shared]);
        assert!(tracker.take_transaction_requests(41, 159, 64).is_empty());
        assert_eq!(tracker.take_transaction_requests(41, 160, 64), vec![shared]);
        assert!(tracker.complete_transaction_request(41, shared));
        assert!(tracker.transaction_request_announcements.is_empty());

        let delayed = marker_id(2);
        assert_eq!(tracker.announce_transaction_requests(43, [delayed], 200), 1);
        assert!(tracker.take_transaction_requests(43, 199, 64).is_empty());
        assert_eq!(
            tracker.take_transaction_requests(43, 200, 64),
            vec![delayed]
        );
        assert_eq!(tracker.announce_transaction_requests(44, [delayed], 200), 1);
        assert_eq!(tracker.disconnect_transaction_request_source(43), 1);
        assert_eq!(
            tracker.take_transaction_requests(44, 200, 64),
            vec![delayed]
        );
        assert_eq!(tracker.forget_transaction_request(delayed), 1);
        assert!(tracker.transaction_request_announcements.is_empty());

        let mut bounded = TransactionAdmissionPool::default();
        for source in 0..16_u64 {
            let ids = (0..MAX_TRANSACTION_REQUEST_ANNOUNCEMENTS_PER_SOURCE)
                .map(|index| {
                    let marker = u32::try_from(
                        source
                            * u64::try_from(MAX_TRANSACTION_REQUEST_ANNOUNCEMENTS_PER_SOURCE)
                                .unwrap()
                            + u64::try_from(index).unwrap(),
                    )
                    .unwrap();
                    marker_id(marker)
                })
                .collect::<Vec<_>>();
            assert_eq!(
                bounded.announce_transaction_requests(source, ids, 0),
                MAX_TRANSACTION_REQUEST_ANNOUNCEMENTS_PER_SOURCE
            );
        }
        assert_eq!(
            bounded.transaction_request_announcements.len(),
            MAX_TRANSACTION_REQUEST_ANNOUNCEMENTS
        );
        assert_eq!(
            bounded.announce_transaction_requests(16, [marker_id(2_000)], 0),
            0
        );
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
    fn sigop_adjusted_package_size_is_enforced_atomically() {
        let (_directory, store) = store();
        let mut builder = Builder::new().push_int(0).push_opcode(opcodes::all::OP_IF);
        for _ in 0..198 {
            builder = builder.push_opcode(opcodes::all::OP_CHECKMULTISIG);
        }
        let witness_script = builder
            .push_opcode(opcodes::all::OP_ENDIF)
            .push_opcode(opcodes::OP_TRUE)
            .into_script();
        let script_pubkey = ScriptBuf::new_p2wsh(&witness_script.wscript_hash());
        let confirmed = OutPoint::new(Txid::from_byte_array([89; 32]), 0);
        store
            .apply(
                &[],
                &[(
                    confirmed.into(),
                    Utxo {
                        value_sats: 1_000_000,
                        height: 1,
                        is_coinbase: false,
                        last_touched: 0,
                        creation_mtp: 1,
                        script_pubkey: script_pubkey.clone().into_bytes(),
                    },
                )],
            )
            .unwrap();
        let mut package = Vec::new();
        let mut previous_output = confirmed;
        let mut value_sats = 1_000_000;
        for _ in 0..6 {
            value_sats -= 25_000;
            let transaction = Transaction {
                version: Version::TWO,
                lock_time: LockTime::ZERO,
                input: vec![TxIn {
                    previous_output,
                    script_sig: ScriptBuf::new(),
                    sequence: Sequence::MAX,
                    witness: Witness::from_slice(&[witness_script.as_bytes()]),
                }],
                output: vec![TxOut {
                    value: Amount::from_sat(value_sats),
                    script_pubkey: script_pubkey.clone(),
                }],
            };
            previous_output = OutPoint::new(transaction.compute_txid(), 0);
            package.push(transaction);
        }
        assert!(package.iter().map(Transaction::vsize).sum::<usize>() < MAX_PACKAGE_VBYTES);
        let mut pool = TransactionAdmissionPool::default();

        assert!(matches!(
            pool.admit_package(&store, package, context()),
            Err(TransactionAdmissionError::PackageTooLarge { vbytes, limit })
                if vbytes > limit && limit == MAX_PACKAGE_VBYTES
        ));
        assert!(pool.is_empty());
        assert!(store.get(confirmed.into()).unwrap().is_some());
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
