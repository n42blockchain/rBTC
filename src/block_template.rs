//! Fee-optimal transaction selection for a block template.
//!
//! The mempool hands out a dependency-ordered snapshot, which is enough to
//! build *a* valid block but not a well-paying one: a child paying for its
//! parent (CPFP) looks unattractive on its own fee rate, and nothing in the
//! snapshot bounds a block's weight or sigop cost. This module scores whole
//! ancestor packages, so a high-fee child pulls its parents in with it, and
//! it fills the block against both consensus ceilings.
//!
//! Selection is deliberately separate from assembly. It reads no chain state
//! and performs no validation: every candidate is already mempool-validated,
//! and the block that results is validated again on connection.

use std::collections::{HashMap, HashSet};

use bitcoin::{Transaction, Txid};

use crate::blockchain::{MAX_BLOCK_SIGOPS_COST, MAX_BLOCK_WEIGHT};

/// Weight held back for the coinbase transaction.
///
/// Matches Bitcoin Core's reserve so a template built here leaves the same
/// room a miner's coinbase is expected to need.
pub const DEFAULT_RESERVED_WEIGHT: u64 = 4_000;

/// Sigop cost held back for the coinbase transaction.
pub const DEFAULT_RESERVED_SIGOP_COST: u64 = 400;

/// How many candidates one selection will consider.
///
/// Ancestor scoring is quadratic in the worst case, so an unbounded mempool
/// could stall template construction. Anything dropped by this ceiling is
/// reported in [`TemplateSelection::skipped_over_ceiling`] rather than
/// silently discarded.
pub const MAX_TEMPLATE_CANDIDATES: usize = 8_000;

/// One mempool transaction offered to the selector.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TemplateCandidate {
    /// Full witness transaction.
    pub transaction: Transaction,
    /// Exact fee derived from validated prevouts.
    pub fee_sats: u64,
    /// Exact sigop cost measured against validated prevouts.
    pub sigop_cost: u64,
}

/// Consensus ceilings a selection must respect.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TemplateLimits {
    /// Weight available to non-coinbase transactions.
    pub max_weight: u64,
    /// Sigop cost available to non-coinbase transactions.
    pub max_sigop_cost: u64,
}

impl Default for TemplateLimits {
    fn default() -> Self {
        Self {
            max_weight: MAX_BLOCK_WEIGHT.saturating_sub(DEFAULT_RESERVED_WEIGHT),
            max_sigop_cost: MAX_BLOCK_SIGOPS_COST.saturating_sub(DEFAULT_RESERVED_SIGOP_COST),
        }
    }
}

/// The chosen transactions and what they consume.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct TemplateSelection {
    /// Chosen transactions, parents always before their children.
    pub transactions: Vec<Transaction>,
    /// Total fees the chosen transactions pay.
    pub fee_sats: u64,
    /// Total weight the chosen transactions consume.
    pub weight: u64,
    /// Total sigop cost the chosen transactions consume.
    pub sigop_cost: u64,
    /// Candidates never considered because the candidate ceiling was reached.
    pub skipped_over_ceiling: usize,
}

/// One candidate's package: itself plus every ancestor also in the candidate set.
struct Package {
    /// Indices into the candidate list, ascending, so dependencies come first.
    members: Vec<usize>,
    fee_sats: u64,
    weight: u64,
}

impl Package {
    /// Fee rate in sats per 1,000 weight units, saturating rather than dividing by zero.
    fn score(&self) -> u128 {
        if self.weight == 0 {
            return u128::MAX;
        }
        u128::from(self.fee_sats)
            .saturating_mul(1_000)
            .saturating_div(u128::from(self.weight))
    }
}

/// Selects the best-paying transactions that fit inside `limits`.
///
/// Candidates must arrive in a valid dependency order, which is what the
/// mempool's relay snapshot provides; the result preserves that order.
/// Scoring is by ancestor package, so a child that pays its parent's way is
/// evaluated together with the parents it needs.
///
/// A package that does not fit is skipped and selection continues, because a
/// single oversized package must not shut out every smaller one behind it.
#[must_use]
pub fn select_template_transactions(
    candidates: &[TemplateCandidate],
    limits: TemplateLimits,
) -> TemplateSelection {
    let skipped_over_ceiling = candidates.len().saturating_sub(MAX_TEMPLATE_CANDIDATES);
    let candidates = &candidates[..candidates.len().min(MAX_TEMPLATE_CANDIDATES)];
    let position: HashMap<Txid, usize> = candidates
        .iter()
        .enumerate()
        .map(|(index, candidate)| (candidate.transaction.compute_txid(), index))
        .collect();

    let mut packages: Vec<(usize, Package)> = Vec::with_capacity(candidates.len());
    for index in 0..candidates.len() {
        packages.push((index, ancestor_package(candidates, &position, index)));
    }
    // Best-paying package first; ties broken by position so the order is
    // deterministic for a given snapshot rather than dependent on sort
    // implementation details.
    packages.sort_by(|left, right| {
        right
            .1
            .score()
            .cmp(&left.1.score())
            .then_with(|| left.0.cmp(&right.0))
    });

    let mut chosen: HashSet<usize> = HashSet::new();
    let mut selection = TemplateSelection {
        skipped_over_ceiling,
        ..TemplateSelection::default()
    };
    for (_, package) in packages {
        let pending: Vec<usize> = package
            .members
            .iter()
            .copied()
            .filter(|member| !chosen.contains(member))
            .collect();
        if pending.is_empty() {
            continue;
        }
        let (weight, sigop_cost, fee_sats) = pending.iter().fold((0, 0, 0), |totals, member| {
            let candidate = &candidates[*member];
            (
                totals.0 + candidate.transaction.weight().to_wu(),
                totals.1 + candidate.sigop_cost,
                totals.2 + candidate.fee_sats,
            )
        });
        if selection.weight + weight > limits.max_weight
            || selection.sigop_cost + sigop_cost > limits.max_sigop_cost
        {
            continue;
        }
        chosen.extend(pending);
        selection.weight += weight;
        selection.sigop_cost += sigop_cost;
        selection.fee_sats += fee_sats;
    }

    // Emitting by candidate position restores the snapshot's dependency order
    // across every package at once, so no child precedes a parent chosen for
    // a different package.
    let mut ordered: Vec<usize> = chosen.into_iter().collect();
    ordered.sort_unstable();
    selection.transactions = ordered
        .into_iter()
        .map(|index| candidates[index].transaction.clone())
        .collect();
    selection
}

/// Collects `index` and every candidate ancestor it depends on.
fn ancestor_package(
    candidates: &[TemplateCandidate],
    position: &HashMap<Txid, usize>,
    index: usize,
) -> Package {
    let mut members = HashSet::from([index]);
    let mut frontier = vec![index];
    while let Some(current) = frontier.pop() {
        for input in &candidates[current].transaction.input {
            let Some(parent) = position.get(&input.previous_output.txid) else {
                // A confirmed prevout: outside the candidate set and already
                // paid for by the block that contains it.
                continue;
            };
            if members.insert(*parent) {
                frontier.push(*parent);
            }
        }
    }
    let mut members: Vec<usize> = members.into_iter().collect();
    members.sort_unstable();
    let (fee_sats, weight) = members.iter().fold((0, 0), |totals, member| {
        let candidate = &candidates[*member];
        (
            totals.0 + candidate.fee_sats,
            totals.1 + candidate.transaction.weight().to_wu(),
        )
    });
    Package {
        members,
        fee_sats,
        weight,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bitcoin::{
        Amount, OutPoint, ScriptBuf, Sequence, TxIn, TxOut, Witness, absolute::LockTime,
        transaction::Version,
    };

    /// Builds a transaction spending `inputs` and paying `outputs` one-satoshi outputs.
    fn transaction(inputs: &[OutPoint], outputs: usize, padding: usize) -> Transaction {
        Transaction {
            version: Version::TWO,
            lock_time: LockTime::ZERO,
            input: inputs
                .iter()
                .map(|previous_output| TxIn {
                    previous_output: *previous_output,
                    script_sig: ScriptBuf::from_bytes(vec![0x51; padding]),
                    sequence: Sequence::ENABLE_RBF_NO_LOCKTIME,
                    witness: Witness::new(),
                })
                .collect(),
            output: (0..outputs)
                .map(|_| TxOut {
                    value: Amount::from_sat(1_000),
                    script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
                })
                .collect(),
        }
    }

    fn candidate(transaction: Transaction, fee_sats: u64, sigop_cost: u64) -> TemplateCandidate {
        TemplateCandidate {
            transaction,
            fee_sats,
            sigop_cost,
        }
    }

    fn outpoint(seed: u8) -> OutPoint {
        OutPoint {
            txid: Txid::from_raw_hash(bitcoin::hashes::Hash::from_byte_array([seed; 32])),
            vout: 0,
        }
    }

    #[test]
    fn higher_paying_transactions_are_selected_first() {
        let cheap = candidate(transaction(&[outpoint(1)], 1, 0), 1_000, 0);
        let rich = candidate(transaction(&[outpoint(2)], 1, 0), 100_000, 0);
        let cheap_txid = cheap.transaction.compute_txid();
        let rich_txid = rich.transaction.compute_txid();

        // A budget that admits exactly one of the two.
        let limits = TemplateLimits {
            max_weight: rich.transaction.weight().to_wu(),
            max_sigop_cost: MAX_BLOCK_SIGOPS_COST,
        };
        let selection = select_template_transactions(&[cheap, rich], limits);

        assert_eq!(selection.transactions.len(), 1);
        assert_eq!(selection.transactions[0].compute_txid(), rich_txid);
        assert_ne!(selection.transactions[0].compute_txid(), cheap_txid);
        assert_eq!(selection.fee_sats, 100_000);
    }

    #[test]
    fn a_paying_child_pulls_its_unattractive_parent_into_the_block() {
        // The parent pays nothing on its own and would lose to the unrelated
        // transaction on individual fee rate. Scored as a package with its
        // child, both belong in the block instead.
        let parent = transaction(&[outpoint(1)], 1, 0);
        let parent_txid = parent.compute_txid();
        let child = transaction(
            &[OutPoint {
                txid: parent_txid,
                vout: 0,
            }],
            1,
            0,
        );
        let child_txid = child.compute_txid();
        let unrelated = transaction(&[outpoint(9)], 1, 0);
        let unrelated_txid = unrelated.compute_txid();

        let candidates = vec![
            candidate(parent.clone(), 0, 0),
            candidate(child, 200_000, 0),
            candidate(unrelated, 5_000, 0),
        ];
        // Room for the package but not for the unrelated transaction as well.
        let limits = TemplateLimits {
            max_weight: parent.weight().to_wu() * 2,
            max_sigop_cost: MAX_BLOCK_SIGOPS_COST,
        };
        let selection = select_template_transactions(&candidates, limits);

        let selected: Vec<Txid> = selection
            .transactions
            .iter()
            .map(Transaction::compute_txid)
            .collect();
        assert_eq!(selected, vec![parent_txid, child_txid]);
        assert!(!selected.contains(&unrelated_txid));
        assert_eq!(selection.fee_sats, 200_000);
    }

    #[test]
    fn a_child_never_precedes_the_parent_it_spends() {
        // The child scores far above the parent, so a selector that emitted in
        // score order would produce a block that fails validation outright.
        let parent = transaction(&[outpoint(1)], 1, 0);
        let parent_txid = parent.compute_txid();
        let child = transaction(
            &[OutPoint {
                txid: parent_txid,
                vout: 0,
            }],
            1,
            0,
        );
        let child_txid = child.compute_txid();

        let selection = select_template_transactions(
            &[candidate(parent, 1, 0), candidate(child, 500_000, 0)],
            TemplateLimits::default(),
        );

        let selected: Vec<Txid> = selection
            .transactions
            .iter()
            .map(Transaction::compute_txid)
            .collect();
        assert_eq!(selected, vec![parent_txid, child_txid]);
    }

    #[test]
    fn the_weight_ceiling_is_never_exceeded() {
        let candidates: Vec<TemplateCandidate> = (0..40)
            .map(|seed| candidate(transaction(&[outpoint(seed)], 1, 400), 10_000, 0))
            .collect();
        let one = candidates[0].transaction.weight().to_wu();
        let limits = TemplateLimits {
            max_weight: one * 10 + one / 2,
            max_sigop_cost: MAX_BLOCK_SIGOPS_COST,
        };

        let selection = select_template_transactions(&candidates, limits);

        assert_eq!(selection.transactions.len(), 10);
        assert!(selection.weight <= limits.max_weight);
        assert_eq!(
            selection.weight,
            selection
                .transactions
                .iter()
                .map(|transaction| transaction.weight().to_wu())
                .sum::<u64>()
        );
    }

    #[test]
    fn the_sigop_ceiling_is_never_exceeded() {
        // Cheap in weight, expensive in sigops: only the sigop budget can stop
        // these, so a selector counting weight alone would overfill the block.
        let candidates: Vec<TemplateCandidate> = (0..40)
            .map(|seed| candidate(transaction(&[outpoint(seed)], 1, 0), 10_000, 1_000))
            .collect();
        let limits = TemplateLimits {
            max_weight: MAX_BLOCK_WEIGHT,
            max_sigop_cost: 4_500,
        };

        let selection = select_template_transactions(&candidates, limits);

        assert_eq!(selection.transactions.len(), 4);
        assert!(selection.sigop_cost <= limits.max_sigop_cost);
    }

    #[test]
    fn an_oversized_package_does_not_shut_out_the_ones_behind_it() {
        // The fat package scores highest but cannot fit. Selection must carry
        // on rather than stopping at the first thing that does not fit.
        let fat = transaction(&[outpoint(1)], 1, 2_000);
        let slim = transaction(&[outpoint(2)], 1, 0);
        let slim_txid = slim.compute_txid();
        let limits = TemplateLimits {
            max_weight: slim.weight().to_wu(),
            max_sigop_cost: MAX_BLOCK_SIGOPS_COST,
        };

        let selection = select_template_transactions(
            &[candidate(fat, 10_000_000, 0), candidate(slim, 1, 0)],
            limits,
        );

        assert_eq!(selection.transactions.len(), 1);
        assert_eq!(selection.transactions[0].compute_txid(), slim_txid);
    }

    #[test]
    fn candidates_past_the_ceiling_are_reported_rather_than_dropped_silently() {
        let candidates: Vec<TemplateCandidate> = (0..MAX_TEMPLATE_CANDIDATES + 5)
            .map(|seed| {
                candidate(
                    transaction(
                        &[OutPoint {
                            txid: Txid::from_raw_hash(bitcoin::hashes::Hash::from_byte_array({
                                let mut bytes = [0u8; 32];
                                bytes[..8].copy_from_slice(&(seed as u64).to_le_bytes());
                                bytes
                            })),
                            vout: 0,
                        }],
                        1,
                        0,
                    ),
                    1_000,
                    0,
                )
            })
            .collect();

        let selection = select_template_transactions(&candidates, TemplateLimits::default());

        assert_eq!(selection.skipped_over_ceiling, 5);
        assert_eq!(selection.transactions.len(), MAX_TEMPLATE_CANDIDATES);
    }

    #[test]
    fn the_default_limits_leave_room_for_a_coinbase() {
        let limits = TemplateLimits::default();
        assert_eq!(
            limits.max_weight,
            MAX_BLOCK_WEIGHT - DEFAULT_RESERVED_WEIGHT
        );
        assert_eq!(
            limits.max_sigop_cost,
            MAX_BLOCK_SIGOPS_COST - DEFAULT_RESERVED_SIGOP_COST
        );
    }
}
