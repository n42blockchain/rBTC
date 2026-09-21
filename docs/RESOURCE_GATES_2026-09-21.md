# Resource gate follow-up, 2026-09-21

This round narrows the three code-closable production resource gates in
[RELEASE_READINESS_2026-09-14.md](RELEASE_READINESS_2026-09-14.md). None of
them is marked accepted: each keeps a stated remainder, and the public soak,
native signing and frozen-source acceptance gates are unchanged.

## Optimizer budget

`adversarial_shapes_respect_budget_and_never_worsen_the_baseline`
(`src/feerate_diagram/optimizer.rs`) runs five deterministic 64-entry shapes
(long chain, complete DAG, wide fan-in, wide fan-out, layered bipartite) with
three fee patterns (sawtooth, equal feerate, feerate descending with topology)
at budgets 0, 1, 100, 2,000, 10,000, 100,000 and the production
`DEFAULT_OPTIMIZER_WORK` (debug builds run 0, 100, 10,000 and the default).
Every run asserts `work_used <= budget`, a complete topological order, a
non-increasing chunk feerate sequence and a diagram that is `Better` or `Equal`
against the supplied identity order. Work accounting is deterministic and
identical in debug and release builds.

| Shape | Sawtooth | Equal feerate | Descending |
| --- | --- | --- | --- |
| Long chain | 695,047 / converged | 513,823 / converged | 468,127 / converged |
| Complete DAG | 1,000,000 / **not converged** | 599,104 / converged | 553,408 / converged |
| Wide fan-in | 471,568 / converged | 474,112 / converged | 470,080 / converged |
| Wide fan-out | 469,150 / converged | 470,206 / converged | 468,127 / converged |
| Layered bipartite | 300,630 / converged | 534,624 / converged | 515,712 / converged |

Values are work used at the default budget. The complete DAG has exactly one
topological order, so the sawtooth case exhausts the whole default budget on a
cluster with no ordering choice. The result is still non-worsening, so this is
an efficiency gap rather than a correctness defect: a follow-up can
short-circuit clusters whose dependency closure admits a single order.

The `feerate_diagram` fuzz target now also calls `linearize_with_budget` with a
fuzz-derived budget (0 to about 1.05 million) and no previous order, a valid
previous order, or a reversed and possibly invalid one. It asserts the same
budget, topology and chunk-order invariants, plus non-worsening whenever the
previous order was reused. It builds and passes Clippy on
`nightly-2026-07-13`; a campaign has not yet been run.

Remainder: a fuzz campaign over the new entry, low-budget Core differential
comparison, and the single-order short-circuit.

## Admission resources

Prevout materialization is now charged before any base-store lookup.
`apply_to_overlay` precharges `inputs x PREVOUT_LOOKUP_BOUND_BYTES x 3`, where
the bound is `chainstate::MAX_SCRIPT_SIZE` (10,000; larger outputs are never
stored) plus the 29-byte fixed `Utxo` encoding prefix. The later precise charge
only bills any positive excess over that precharge; charges remain
non-refundable. Cloning the prevout scripts for verification is charged to the
`Script` stage before the clone.

A candidate whose precharge exceeds `work_burst` can never fit, even after a
full refill. It is refused with the permanent
`TransactionAdmissionError::CandidateUnfittable` instead of a retryable
deferral. The node already caches it as an ordinary terminal rejection without
peer scoring. With the default limits (8e9 burst, 1e9 per second), a maximal
standard transaction (about 2,400 inputs) precharges about 7.3e7. This is far
from unfittable and allows about 33,000 prevout lookups per second of sustained
refill. Bulk reconciliation or pool reload can now defer earlier than before;
deferral keeps candidates, so this slows the operation rather than losing work.

Tests in `src/transaction_admission/tests/resource_budget.rs`:
`prevout_precharge_defers_before_any_base_store_lookup` (a counting store sees
zero lookups), `prevout_script_clone_is_charged_under_script_stage`, and
`oversized_candidate_is_permanently_refused_while_a_normal_one_still_admits`.

Remainder: `snapshot()`/`relay_snapshot()` clones remain uncharged. Admission
remains serialized behind one pool mutex. Resumable scheduling of a large
candidate that fits the burst but not the current allowance is not
implemented; it is retried from the start.

## Competing header retention

Peer header sync (`sync_headers`) and local block submission
(`stage_submitted_blocks`) now bound retained side-chain headers after every
committed batch. The cap is `NodeResourceConfig::max_side_chain_headers`
(default `DEFAULT_MAX_SIDE_CHAIN_HEADERS = 16,384`, about 1.6 MB of header
metadata). `HeaderDag::select_side_chain_eviction_candidates` returns early
without work while under the cap. Over the cap, it walks childless headers off
the active chain, lowest chainwork first with a hash tie-break. Evicting a leaf
can expose its parent in the same pass. The active chain is never a candidate,
and submission-awaited hashes are pinned. Eviction is staged, persisted in one
redb transaction and only then committed in memory. A staging or persistence
failure is logged and rolled back; it never scores or disconnects a peer.

`header_resync_bounds_competing_side_chains_then_reacquires_an_evicted_fork`
(`src/node/tests/header_resync.rs`, cap 4, loopback peer) shows:

1. Ten competing low-work forks are bounded to the cap. The active chain and
   peer session stay intact.
2. The peer extends an evicted fork past the active chainwork. Ordinary
   `getheaders` with the active-chain locator reacquires it from the common
   ancestor, revalidates it and promotes it to the active header tip.
3. After reopen, evicted headers that were not resent stay absent, and the
   reacquired active chain is preserved.

Two selector unit tests cover cap, pinning, leaf-first order and deterministic
tie-breaking.

Declared boundary: a competing fork that stays at or below active chainwork
for more than `max_side_chain_headers` headers has its own leaf evicted after
each batch. It therefore cannot overtake through ordinary sync. At the default
cap, this means a reorganization deeper than 16,384 blocks, far beyond any
observed mainnet reorg. Handling it would need a separate bounded candidate
stage, which is not implemented.

Remainder: a sustained resource run under a hostile fork feeder, and the
candidate stage above if the boundary is not accepted.

## Verification

Merged tree `beb6739`: `cargo clippy --lib --tests -- -D warnings` clean;
`cargo test --lib` on Windows passed 877 with 9 ignored; all integration
test targets compile. The Mac all-feature suite and readiness script tests are
pending (the readiness script tests fail on Windows before and after this round
because of CRLF checkout and symlink privilege, not because of these changes).

## Second pass, same day

**Optimizer single-order short-circuit** (`1894f8e`). If the baseline order is
the only topological order, `linearize_with_budget` returns it as optimal after
an `O(n + edges)` check charged to the budget. A pair of consecutive entries
without a direct edge could be swapped, so an order is unique exactly when
every consecutive pair is a direct edge. When the budget cannot cover the
check, the existing search runs unchanged. The adversarial sweep now converges
for all 15 cases. Complete DAGs use 2,080 work, down from up to 1,000,000 with
no convergence in the sawtooth case. Long chains use 127, down from up to
695,047. The other shapes are unchanged within the check cost.
`unique_order_clusters_short_circuit_the_search` and
`independent_roots_are_not_short_circuited` cover the rule.

**Up-front candidate work gate** (`43908ca`). `admit_package_at` estimates the
candidate's conservative total work from the formulas its stages use. The
estimate covers payload traversal and bytes, metadata, prevout preparation and
precharge, and graph. It is checked before any stage runs:
`CandidateUnfittable` if it exceeds `work_burst`, otherwise a retryable
`ResourceDeferred` without touching any counter if the current allowance is
short. The existing per-stage charges are unchanged, so nothing is charged
twice. A deferred candidate no longer spends non-refundable allowance on a
partial attempt, and because the estimate never exceeds the burst, a retry
after refill can proceed. This replaces true mid-candidate resumption.
Limitation: a new transaction's sigop component of the script charge is
unknown before prevout resolution and is omitted from the estimate. A
high-sigop candidate can still defer after partial charges, as before.

**Peer-facing snapshots.** Charging peer `mempool`/`getdata` and relay
snapshots to the shared admission ledger was evaluated and rejected. At about
300 MB of retained pool, each whole-pool charge would let a few getdata
messages per second starve all admission. Instead, single-transaction getdata
now uses the txid index or a scan of stored wtxids
(`TransactionAdmissionPool::transaction`/`transaction_by_wtxid`). This removes
a pre-existing whole-pool clone and rehash per request. `mempool` responses,
relay polls and operator RPC still clone the pool. They are bounded by
`mempool_max_bytes` and are not charged to the admission ledger.

Remaining admission items: an explicit separate budget for whole-pool
snapshot serving if it is required, the sigop estimate gap, and serialized
admission behind one pool mutex, which is kept as a declared boundary.

Verification of the second pass on Windows: `cargo fmt --check` (both
crates), `cargo clippy --all-targets --all-features` and the fuzz crate
Clippy are clean. `cargo test --lib` passed 882 with 9 ignored, and all
integration targets compile. This pass also applied rustfmt to the first-pass
code (`05a32bd`); CI would otherwise have rejected it.

## Third pass, same day

**Peer `mempool` serving** (`1b0caa0`). `serve_mempool` previously cloned every
retained transaction and rehashed each one for every `mempool` message. It
now reads up to `MAX_INVENTORY_ENTRIES` stored `(txid, wtxid)` pairs through
`TransactionAdmissionPool::inventory_ids` and answers at most once per inbound
connection. Repeats are ignored without disconnecting or penalizing the peer.
`InboundDataSource::mempool` now takes a limit and returns identifiers.
Covered by `mempool_is_served_at_most_once_per_connection`.

**Sigop estimate.** For a transaction whose sigop cost is not yet known, the
up-front work gate now bounds the script sigop charge with
`MAX_STANDARD_TRANSACTION_SIGOP_COST` (16,000); transactions above it are
non-standard and rejected anyway. This adds 1.6e8 per new transaction, so a
25-transaction package estimates about 4e9, still within the 8e9 default
burst. `sigop_worst_case_bound_defers_up_front_without_store_lookups` shows
the formerly missed case now defers before any stage or store work.

**Hostile side-chain feeder** (`9e5c568`). At cap 1,024, a regtest store and
DAG receive valid low-work forks off several active ancestors: mostly one
header deep, every 37th fork three deep. Each batch commits, then runs the
node's select, stage, persist and commit eviction sequence.

| Headers fed | Evicted | Peak side-chain | Final redb file | Debug | Release |
| --- | --- | --- | --- | --- | --- |
| 10,000 (default test) | 8,984 | 1,024 | 2,641,920 B | 2.05 s | 0.71 s |
| 100,000 (`#[ignore]`) | 98,984 | 1,024 | 2,641,920 B | 22.4 s | 4.56 s |

The file size stops growing once the cap saturates. At the end, a fork
evicted early is resubmitted with an extension from its retained ancestor and
becomes active. A bounded reopen keeps the side-chain count within the cap.
This exercises the store and DAG retention path directly, not a live P2P
session, and it does not measure process RSS.

**Declared boundary: serialized admission.** All admission, reconciliation
and snapshot work runs under one `Mutex<TransactionAdmissionPool>`. This
limits admission to one CPU. It is also a hard concurrency bound: no two
candidates can hold overlapping leases or interleave partial pool state.
Sharding is not required for the outbound-only production claim and is not
planned for this release.

Verification of the third pass on Windows (merge `e74f24e`): `cargo fmt --check`
(both crates) and `cargo clippy --all-targets` are clean. `cargo test --lib`
passed 885 with 10 ignored, and all integration targets compile.
