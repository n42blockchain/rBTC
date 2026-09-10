//! Chunk-feerate transaction selection for a block template.
//!
//! The mempool hands out a dependency-ordered snapshot, which is enough to
//! build *a* valid block but not a well-paying one: a child paying for its
//! parent (CPFP) looks unattractive on its own fee rate, and nothing in the
//! snapshot bounds a block's weight or sigop cost. This module uses the same
//! bounded cluster linearization and chunks as the admission pool, and
//! it fills the block against both consensus ceilings.
//!
//! Selection is deliberately separate from assembly. It reads no chain state
//! and performs no validation: every candidate is already mempool-validated,
//! and the block that results is validated again on connection.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use bitcoin::{Transaction, Txid};

use crate::{
    blockchain::{MAX_BLOCK_SIGOPS_COST, MAX_BLOCK_WEIGHT},
    feerate_diagram::{FeeFrac, linearize_components},
    transaction_admission::MAX_MEMPOOL_CLUSTER_TRANSACTIONS,
};

/// Weight held back for the coinbase transaction.
///
/// Matches Bitcoin Core's reserve so a template built here leaves the same
/// room a miner's coinbase is expected to need.
pub const DEFAULT_RESERVED_WEIGHT: u64 = 4_000;

/// Sigop cost held back for the coinbase transaction.
pub const DEFAULT_RESERVED_SIGOP_COST: u64 = 400;

/// How many candidates one selection will consider.
///
/// Linearization work is bounded per cluster; this additionally caps the
/// number of components considered. Anything dropped by this ceiling is
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

/// Selects fee-ordered chunks that fit inside `limits`.
///
/// Candidates must arrive in a valid dependency order, which is what the
/// mempool's relay snapshot provides, so truncating at the candidate ceiling
/// cannot drop a retained child's parent. Within that prefix, canonical txid
/// order feeds the same linearization/chunking primitives as mempool RBF.
/// Fees use sigop-adjusted policy vsize, not a rounded weight-only score.
///
/// A chunk that does not fit is skipped, as are its dependent chunks; unrelated
/// work remains eligible. Invalid graphs or unrepresentable fees fail closed
/// with an empty selection. Candidates are already consensus-validated.
#[must_use]
pub fn select_template_transactions(
    candidates: &[TemplateCandidate],
    limits: TemplateLimits,
) -> TemplateSelection {
    let skipped_over_ceiling = candidates.len().saturating_sub(MAX_TEMPLATE_CANDIDATES);
    let candidates = &candidates[..candidates.len().min(MAX_TEMPLATE_CANDIDATES)];
    let mut selection = TemplateSelection {
        skipped_over_ceiling,
        ..TemplateSelection::default()
    };
    let by_txid = candidates
        .iter()
        .map(|candidate| (candidate.transaction.compute_txid(), candidate))
        .collect::<BTreeMap<_, _>>();
    if by_txid.len() != candidates.len() {
        return selection;
    }
    let candidates = by_txid.values().copied().collect::<Vec<_>>();
    let position: HashMap<Txid, usize> = candidates
        .iter()
        .enumerate()
        .map(|(index, candidate)| (candidate.transaction.compute_txid(), index))
        .collect();

    let parents = candidates
        .iter()
        .map(|candidate| {
            let mut parents = candidate
                .transaction
                .input
                .iter()
                .filter_map(|input| position.get(&input.previous_output.txid).copied())
                .collect::<Vec<_>>();
            parents.sort_unstable();
            parents.dedup();
            parents
        })
        .collect::<Vec<_>>();
    let fractions = candidates
        .iter()
        .map(|candidate| {
            let size = candidate
                .transaction
                .weight()
                .to_wu()
                .max(candidate.sigop_cost.saturating_mul(20))
                .div_ceil(4);
            Ok(FeeFrac::new(
                i64::try_from(candidate.fee_sats)?,
                i32::try_from(size)?,
            ))
        })
        .collect::<Result<Vec<_>, std::num::TryFromIntError>>();
    let Ok(fractions) = fractions else {
        return selection;
    };
    let Ok(chunks) = linearize_components(&fractions, &parents, MAX_MEMPOOL_CLUSTER_TRANSACTIONS)
    else {
        return selection;
    };
    let mut chosen = vec![false; candidates.len()];
    for chunk in chunks {
        let members = chunk.members.iter().copied().collect::<BTreeSet<_>>();
        if members.iter().any(|&member| {
            parents[member]
                .iter()
                .any(|parent| !chosen[*parent] && !members.contains(parent))
        }) {
            continue;
        }
        let totals = chunk
            .members
            .iter()
            .try_fold((0_u64, 0_u64, 0_u64), |totals, member| {
                let candidate = &candidates[*member];
                Some((
                    totals
                        .0
                        .checked_add(candidate.transaction.weight().to_wu())?,
                    totals.1.checked_add(candidate.sigop_cost)?,
                    totals.2.checked_add(candidate.fee_sats)?,
                ))
            });
        let Some((weight, sigop_cost, fee_sats)) = totals else {
            continue;
        };
        if weight > limits.max_weight.saturating_sub(selection.weight)
            || sigop_cost > limits.max_sigop_cost.saturating_sub(selection.sigop_cost)
        {
            continue;
        }
        let Some(total_fees) = selection.fee_sats.checked_add(fee_sats) else {
            continue;
        };
        for member in chunk.members {
            chosen[member] = true;
            selection
                .transactions
                .push(candidates[member].transaction.clone());
        }
        selection.weight += weight;
        selection.sigop_cost += sigop_cost;
        selection.fee_sats = total_fees;
    }

    selection
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
    fn upstream_selection_scores_sigop_adjusted_vsize() {
        let expensive = candidate(transaction(&[outpoint(1)], 1, 0), 1_000, 1_000);
        let efficient = candidate(transaction(&[outpoint(2)], 1, 0), 100, 0);
        let wanted = efficient.transaction.compute_txid();
        let limits = TemplateLimits {
            max_weight: efficient.transaction.weight().to_wu(),
            max_sigop_cost: MAX_BLOCK_SIGOPS_COST,
        };
        let selected = select_template_transactions(&[expensive, efficient], limits);
        assert_eq!(selected.transactions[0].compute_txid(), wanted);
    }

    #[test]
    fn upstream_skipped_parent_chunk_cannot_leave_a_child_in_the_template() {
        let parent = transaction(&[outpoint(1)], 1, 2_000);
        let child = transaction(&[OutPoint::new(parent.compute_txid(), 0)], 1, 0);
        let other = transaction(&[outpoint(2)], 1, 0);
        let wanted = other.compute_txid();
        let limits = TemplateLimits {
            max_weight: other.weight().to_wu() * 2,
            max_sigop_cost: MAX_BLOCK_SIGOPS_COST,
        };
        let selected = select_template_transactions(
            &[
                candidate(parent, 10_000_000, 0),
                candidate(child, 1, 0),
                candidate(other, 2, 0),
            ],
            limits,
        );
        assert_eq!(
            selected
                .transactions
                .iter()
                .map(Transaction::compute_txid)
                .collect::<Vec<_>>(),
            vec![wanted]
        );
    }

    #[test]
    fn upstream_shared_parent_is_charged_once_and_low_fee_child_waits() {
        let parent = transaction(&[outpoint(1)], 2, 0);
        let rich = transaction(&[OutPoint::new(parent.compute_txid(), 0)], 1, 0);
        let low = transaction(&[OutPoint::new(parent.compute_txid(), 1)], 1, 0);
        let other = transaction(&[outpoint(2)], 1, 0);
        let expected = [&parent, &rich, &other, &low].map(Transaction::compute_txid);
        let selected = select_template_transactions(
            &[
                candidate(parent, 0, 0),
                candidate(low, 100, 0),
                candidate(other, 150, 0),
                candidate(rich, 1_000, 0),
            ],
            TemplateLimits::default(),
        );
        assert_eq!(
            selected
                .transactions
                .iter()
                .map(Transaction::compute_txid)
                .collect::<Vec<_>>(),
            expected
        );
        assert_eq!(selected.fee_sats, 1_250);
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
