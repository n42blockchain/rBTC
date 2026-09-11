# Open-item review, 2026-09-11

Latest review baseline: `c63a8761254a90d4ea4a4acce306560334b60885`.
The [progress review](UPSTREAM_PROGRESS_REVIEW_2026-09-11.md) reconciles recent
commits, source manifests and acceptance evidence. The table reflects the
subsequent fixes; the fee-decay narrative and its 919-test result below remain
historical acceptance at baseline `568772a`. The broader resource and operational
gates still require their own implementation or evidence.

## Item-by-item disposition

| Item | Current disposition | What remains to close it |
| --- | --- | --- |
| Rolling-fee fractional decay and query cadence | **Closed for the covered behavior in this continuation.** A failing baseline regression and live Core mismatch reproduce the defect; the corrected implementation passes both. | Broader fee-policy equivalence is separate: occupancy accounting, eviction selection and optimizer parity are not established by this check. |
| Core cluster optimizer and broader fee-floor parity | **Partially completed in the subsequent continuation.** Greedy ordering now receives the two-pass refinement, with 4,096 exact Core comparisons and a fixed shared-parent counterexample; see [acceptance](UPSTREAM_CLUSTER_POSTLINEARIZE_GATE.md). | Full work-budgeted optimizer search and incremental-order reuse remain open. Compare pressure and decay under equivalent occupancy regimes; rBTC counts retained serialized bytes while Core's threshold uses dynamic memory usage. |
| Whole-pipeline admission CPU and allocation budget | **Partially completed; aggregate limits remain.** SCRIPT reuse, immutable payload sharing and indexed traversal are measured improvements. Reconciliation performs one contextual validation per entry and preserves paid zero-fee parents; [exceptional growth checks](UPSTREAM_RECONCILIATION_GROWTH_GATE.md) now use bounded cluster views instead of repeated whole-pool indices. Known/duplicate package preflight now avoids candidate cloning; see [review](UPSTREAM_PROGRESS_REVIEW_2026-09-11.md). | Account for new-package replay, fresh UTXO lookup, hashing, other policy checks, metadata copies and snapshots; impose aggregate work/allocation limits across accepted/rejected packages, concurrent peers and repeated chain changes. Preserve contextual validation and atomic admission. |
| Valid competing-header memory/disk limits | **Partially completed; retention limits remain.** The serving copy is bounded to active ancestors. Within-session polls now reuse the validated DAG, eliminating historical replay and a temporary replacement graph; see [acceptance](UPSTREAM_HEADER_RESYNC_GATE.md). | Reserve capacity at both peer and local-block ingress; implement resource deferral, resumable stronger-fork recovery, durable eviction and bounded restart/failover loading. Prove retention plateaus without preventing a valid reorg. |
| 160M-UTXO / 900,000-transition storage lifecycle | **Full run remains.** The million-UTXO compaction/restart/reference lane is accepted at its documented scale. | Run both 64/256 lanes on a suitable volume, retain copy-space/peak-RSS evidence and satisfy the MDBX lifecycle criteria. A smaller run cannot close this gate. |
| Cold-disk and full mainnet replay | **Data and execution evidence remain.** Historical external corpus-window results are documented; the current local generated tests do not replace them. | Supply the immutable corpus and matched starting state, then run comparable engines/configurations with canonical content and I/O evidence. |
| Seven-day public-network soak | **Duration and complete evidence remain.** No accepted frozen report was found in the searched local workspace. | Restore the complete original frozen run or perform a new run after both networks are caught up, including restart/fault exercises and the fail-closed finalizer. Elapsed calendar time alone is insufficient. |
| Intermittent failover-test nonce mismatch | **Reproducible fixture race fixed.** A controlled port-reuse schedule produces the same deficient-peer nonce assertion; failed endpoints now retain bound, unlistened sockets. See [investigation](UPSTREAM_FAILOVER_TEST_ISOLATION.md). | The historical log cannot identify the original foreign client. The demonstrated cross-test route is closed by port ownership; this does not establish broader production failover/resource guarantees. |

Earlier bounded acceptance is already complete for the documented consensus and
parser fixtures, queue ownership/cancellation regressions, the 150,000-run fuzz
campaign, serving projection, real Tor/I2P transport/private wallet waves and
selected generated storage workloads. Their historical “not run” entries do not
represent fresh blockers. Longer fuzz campaigns remain ongoing validation, not
evidence that the completed bounded campaign failed.

The two resource rows above require production changes, not merely rerunning
tests. Their implementation and acceptance sequences are detailed in the
[header gate](UPSTREAM_HEADER_RESOURCE_GATE.md) and
[admission gate](UPSTREAM_ADMISSION_RESOURCE_GATE.md).

## Completed: rolling-fee fractional progress

The old pool stored the rolling rate as an integer and rounded every decay
update. At 600 sat/kvB with the quarter-capacity half-life, an eleven-second
step loses less than half a sat/kvB. Repeated rounding therefore held the rate
at 600 indefinitely. One query after 10,800 seconds instead returned 300,
making policy depend on query/admission cadence.

The pool now retains a private `f64` rate, rounds when returning the effective
integer fee and tests the clear threshold before rounding. A higher eviction
rate suspends decay until another block; lower/equal bumps do not. Public fee
types and fee arithmetic stay integer, and reconciliation preserves fractional
state. The reference behavior is
[Core 31's GetMinFee and trackPackageRemoved](https://github.com/bitcoin/bitcoin/blob/v31.0/src/txmempool.cpp#L766).

Three new unit regressions cover frequent versus sparse queries, all three
occupancy half-lives and their strict quarter/half boundaries, the ten-second
update gate, block gating/rebumping, and clearing just below 50 before integer
rounding. The existing reconciliation case now also checks fractional-state
preservation. The first regression fails on the old implementation with
`600 != 300` after one half-life.

The added live differential uses the official Core 31 binary in isolated
regtest mode. It causes real uniform-fee eviction, establishes the same initial
600 sat/kvB floor in both implementations, verifies no pre-block decay, then
mines and empties both pools. `setmocktime` tests 17 timestamps from zero through
86,400 seconds in the same empty-pool occupancy regime. This advances simulated
time; it is not a day-long wall-clock soak.

| Seconds after the block | Core 31 | Previous rBTC | Fixed rBTC |
| --- | ---: | ---: | ---: |
| 10 | 600 | 600 | 600 |
| 11 | 600 | 600 | 600 |
| 22 | 599 | 600, differential fails here | 599 |
| 10,800 | 300 | Not reached in failed differential | 300 |
| 21,600 | 150 | Not reached | 150 |
| 43,200 | 100 | Not reached | 100 |

The live table compares effective rates including the 100 sat/kvB static relay
floor. The unit tests independently verify the rolling state reaches zero.
The full 17-point fixed trace is retained. This establishes the covered temporal
behavior under matched occupancy, not exact Core memory accounting or optimizer
equivalence.

## Local prerequisites checked during the original review

The workspace filesystem had approximately 222 GiB free. It is a shared home
volume, and that free space is below the two storage lanes' combined configured
256 GiB ceilings before a compact copy and reserve. This is a capacity-planning
gap, not a claim that either lane immediately allocates its entire ceiling. The
full two-lane run was not started on that basis.

A workspace search including ignored files found no `blk[0-9]*.dat` corpus,
`rbtcd-soak-start` frozen binary or generated soak report; only the report scripts
were present. `/home/n42/.bitcoin` was absent. This is a scoped local preflight,
not an exhaustive claim about every disk or external host. Existing historical
external results remain historical until their complete evidence is available.

Evidence for this review is under
`target/upstream-followup/2026-09-11/fee-decay/`: `before-fix.log`,
`core-decay-before-fix.log`, `core-decay-first.log`, `admission-tests.log`,
`core-replacement-final.log`, `all-features-tests.log`, `clippy-final.log`,
`environment.json` and `published-source.json`.

Final acceptance: **70 admission tests passed**; the full all-feature run passed
**919 tests** (880 library + 39 integration), with **29 ignored** and no failures.
Subprocess-helper output is excluded from that total. All **three** explicitly
enabled Core replacement/package-pressure/decay tests passed. Strict
all-target/all-feature Clippy, formatting and diff checks passed. The full suite
used four test threads; this run did not reproduce the earlier nonce failure.
Only documentation changed after these checks.

Reproduction:

```sh
export CARGO_HOME=/tmp/rbtc-cargo-home
cargo test --locked --all-features --lib transaction_admission::tests
RBTC_BITCOIND=/absolute/path/to/bitcoin-31.0/bin/bitcoind \
  cargo test --locked --all-features --test core_replacement_differential \
  -- --ignored --nocapture
cargo test --locked --all-features --no-fail-fast -- --test-threads=4
cargo clippy --locked --all-targets --all-features -- -D warnings
```
