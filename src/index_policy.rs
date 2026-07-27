//! Pruning compatibility rules shared by rebuildable projections.
//!
//! Consensus stores never depend on these decisions. The policy exists so an
//! index cannot silently discover after pruning that its required build range
//! is unavailable.

/// Rebuildable projection families supported or planned by rBTC.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IndexKind {
    /// REST/RPC block, transaction, and current address-UTXO projection.
    Explorer,
    /// Watch-only descriptor projection, starting at its configured birthday.
    Wallet,
    /// Full transaction-id to block-position index.
    Transaction,
    /// Spent-output lookup/history index.
    SpentOutput,
    /// BIP157/158 basic compact-filter index.
    BasicFilter,
}

impl IndexKind {
    /// Whether a trusted current-UTXO baseline can replace unavailable block
    /// history for this projection.
    #[must_use]
    pub const fn accepts_utxo_baseline(self) -> bool {
        matches!(self, Self::Explorer | Self::Wallet)
    }

    /// Whether a fully caught-up index remains complete after old block files
    /// are physically pruned.
    #[must_use]
    pub const fn survives_pruning_after_sync(self) -> bool {
        true
    }
}

/// Materialized progress and required history for one optional index.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IndexBuildState {
    /// Index family.
    pub kind: IndexKind,
    /// Highest active-chain block durably represented by the index.
    pub best_height: u32,
    /// First block required by a clean rebuild (genesis is height zero).
    pub required_from_height: u32,
    /// A UTXO baseline at the execution tip is available and authenticated by
    /// the chainstate's existing AssumeUTXO lifecycle.
    pub utxo_baseline_height: Option<u32>,
}

/// Sources available to fill a missing index range.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IndexHistoryAvailability {
    /// First locally retained freezer height, or `None` at genesis.
    pub local_first_height: Option<u32>,
    /// A connected full-history+witness peer can serve older active blocks.
    pub authenticated_peer_history: bool,
}

/// Refusal from a prune/index compatibility gate.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IndexPolicyError {
    /// Persisted index progress is ahead of consensus execution.
    AheadOfExecution,
    /// The required catch-up prefix is neither local nor peer-available.
    MissingBuildHistory,
    /// Pruning would delete a block not yet represented by this index.
    PruneWouldStrandIndex,
    /// This index cannot be reconstructed from a current-UTXO baseline.
    UnsupportedBaseline,
    /// A claimed baseline is ahead of the durable execution tip.
    BaselineAheadOfExecution,
}

/// Checks whether one index can reach `execution_height` without weakening its
/// advertised completeness.
pub fn validate_index_activation(
    state: IndexBuildState,
    history: IndexHistoryAvailability,
    execution_height: u32,
) -> Result<(), IndexPolicyError> {
    if state.best_height > execution_height {
        return Err(IndexPolicyError::AheadOfExecution);
    }
    if let Some(base) = state.utxo_baseline_height {
        if !state.kind.accepts_utxo_baseline() {
            return Err(IndexPolicyError::UnsupportedBaseline);
        }
        if base > execution_height {
            return Err(IndexPolicyError::BaselineAheadOfExecution);
        }
        if base >= state.required_from_height.saturating_sub(1) {
            return Ok(());
        }
    }
    let next = state
        .best_height
        .saturating_add(1)
        .max(state.required_from_height);
    if next > execution_height
        || history.local_first_height.is_none_or(|first| next >= first)
        || history.authenticated_peer_history
    {
        return Ok(());
    }
    Err(IndexPolicyError::MissingBuildHistory)
}

/// Refuses a physical prune boundary that would overtake a lagging enabled
/// index. Operators must first catch it up, disable/remove it explicitly, or
/// rebuild it through an explicit authenticated-history workflow.
pub fn validate_prune_boundary(
    state: IndexBuildState,
    first_retained_height: u32,
    execution_height: u32,
) -> Result<(), IndexPolicyError> {
    if state.best_height > execution_height {
        return Err(IndexPolicyError::AheadOfExecution);
    }
    let next_required = state
        .best_height
        .saturating_add(1)
        .max(state.required_from_height);
    if next_required < first_retained_height && next_required <= execution_height {
        return Err(IndexPolicyError::PruneWouldStrandIndex);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pruned_activation_requires_local_suffix_peer_history_or_supported_baseline() {
        let history = IndexHistoryAvailability {
            local_first_height: Some(900),
            authenticated_peer_history: false,
        };
        let transaction = IndexBuildState {
            kind: IndexKind::Transaction,
            best_height: 0,
            required_from_height: 1,
            utxo_baseline_height: None,
        };
        assert_eq!(
            validate_index_activation(transaction, history, 1_000),
            Err(IndexPolicyError::MissingBuildHistory)
        );
        assert!(
            validate_index_activation(
                transaction,
                IndexHistoryAvailability {
                    authenticated_peer_history: true,
                    ..history
                },
                1_000
            )
            .is_ok()
        );
        let explorer = IndexBuildState {
            kind: IndexKind::Explorer,
            utxo_baseline_height: Some(1_000),
            ..transaction
        };
        assert!(validate_index_activation(explorer, history, 1_000).is_ok());
        assert_eq!(
            validate_index_activation(
                IndexBuildState {
                    kind: IndexKind::BasicFilter,
                    utxo_baseline_height: Some(1_000),
                    ..transaction
                },
                history,
                1_000
            ),
            Err(IndexPolicyError::UnsupportedBaseline)
        );
    }

    #[test]
    fn prune_never_overtakes_lagging_enabled_index() {
        let lagging = IndexBuildState {
            kind: IndexKind::Transaction,
            best_height: 700,
            required_from_height: 1,
            utxo_baseline_height: None,
        };
        assert_eq!(
            validate_prune_boundary(lagging, 900, 1_000),
            Err(IndexPolicyError::PruneWouldStrandIndex)
        );
        assert!(
            validate_prune_boundary(
                IndexBuildState {
                    best_height: 1_000,
                    ..lagging
                },
                900,
                1_000
            )
            .is_ok()
        );
        assert_eq!(
            validate_prune_boundary(
                IndexBuildState {
                    best_height: 1_001,
                    ..lagging
                },
                900,
                1_000
            ),
            Err(IndexPolicyError::AheadOfExecution)
        );
    }

    #[test]
    fn every_index_is_prune_safe_only_after_materializing_its_history() {
        for kind in [
            IndexKind::Explorer,
            IndexKind::Wallet,
            IndexKind::Transaction,
            IndexKind::SpentOutput,
            IndexKind::BasicFilter,
        ] {
            assert!(kind.survives_pruning_after_sync());
            assert!(
                validate_prune_boundary(
                    IndexBuildState {
                        kind,
                        best_height: 100,
                        required_from_height: 1,
                        utxo_baseline_height: None,
                    },
                    90,
                    100
                )
                .is_ok()
            );
        }
    }
}
