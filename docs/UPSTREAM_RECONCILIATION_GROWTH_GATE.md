# Reconciliation growth checks, 2026-09-11

Baseline: `c01c2adbe8f33330ae75806c511541390d1e6ad6`. This change removes
repeated whole-pool policy-index construction and deletion-index rebuilding
from exceptional sigop-size growth. Aggregate admission work/allocation quotas
remain open.

## Failure and change

Reconciliation already validates every retained transaction once. A script-flag
change can increase sigop-adjusted policy sizes without making scripts invalid.
The previous growth pass checked those entries in reverse insertion order,
against the whole pool. Every TRUC check constructed a version/size map of all
retained entries, and each removal rebuilt the whole pool's spent/position
indices. Independent affected clusters therefore multiplied each other's work.

Two baseline regressions fail with just eight independent parent/child pairs:
the keep-all case visits 144 policy/index entries; the case removing every
oversized TRUC child visits 208. The new regression budget is 64 visits for
those 16-entry fixtures. Counts include the initial reconciliation index rebuild
and the entry loops in TRUC-map construction and removal-index rebuilding;
they are not counts of all instructions, input edges or allocations.

The growth pass now discovers each affected connected component once and makes
a temporary policy view in its original insertion order. That view shares
immutable transaction payloads and copies only component metadata and indices.
It applies the same reverse-order growth checks and descendant removals. Removed
IDs are accumulated, then the main deque and its indices are updated once.
Independent components have no connecting edges, so their processing order
cannot affect these size or TRUC decisions. Within an original component, order
is preserved even after removals split it into smaller components.

Retained components already have a 64-entry bound, and reconciliation only
deletes graph edges. Each temporary view therefore has at most 64 entries and
performs at most 64 growth checks. A conservative bound on instrumented
policy/index-entry visits for an N-entry pool is 131N, including the initial
and final global rebuilds. Local child/input traversal and tree-index lookup
costs are additional; the global pending/removal sets can contain O(N) IDs.
This is a bound on this phase's repeated entry loops, not a wall-clock, RSS,
whole-admission or per-peer quota.

Contextual validation, fresh UTXO lookup, SCRIPT commitment rules, fee metadata,
fee sponsorship and published survivor order are unchanged. Temporary views
do not replace the live pool's orphanage, request caches or rolling-fee state.

## Acceptance

- Real witness-sigop activation fixtures exercise 8, 32 and 64 independent pairs,
  both retaining ordinary children and removing oversized TRUC children. They
  check one contextual validation per entry, bounded policy/index visits,
  expected survivors, shared payload identity, unchanged base UTXOs and stable
  subsequent reconciliation.
- Ninety-six generated graph fixtures compare exact survivors, order, indices,
  retained bytes and surviving metadata against the original global reverse
  pass. Components have 1, 2, 7, 16 and 64 entries, interleaved insertion order,
  branching/shared parents and mixed growth patterns. These synthetic policy
  graphs complement the actual transaction-validation fixtures.

All **80 admission tests** pass. The full all-feature suite passes **940 tests**
(901 library + 39 integration), with **33 ignored**, no failures and four test
threads. All **four live Core 31 differential tests** pass: replacement,
package-pressure, rolling-fee decay and sponsored-package reconciliation.
Those live fixtures do not simulate a script-flag activation; actual size
growth is covered by the ordinary activation regressions above. Strict
all-target/all-feature Clippy, formatting and diff checks pass.

## Generated resource measurement

One preserved release library-test executable runs either the retained original
growth loop or the production component-based loop. Each workload contains
independent, funded TRUC parent/child pairs with witness scripts that increase
sigop size at witness activation. All children must leave and all parents
survive. Contextual validation and metadata refresh happen before the timed
phase; both modes then prune the same prepared graph.

Results below are medians of three fresh-process samples per mode and size,
run sequentially with alternating mode order. Rust 1.85.0, all features,
Linux x86_64, system allocator (library test executable); fixture Redb files
are under `/tmp`. No builds or other validation runs overlap these measurements.

| Independent pairs | Original phase, ms | Component phase, ms | Original entry visits | Component entry visits | Original peak RSS, KiB | Component peak RSS, KiB |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1,024 | 379.253 | 2.574 | 3,145,728 | 6,144 | 12,360 | 16,536 |
| 4,096 | 7,408.651 | 9.502 | 50,331,648 | 24,576 | 18,520 | 20,568 |

For C such pairs, these growth-phase entry loops visit exactly 3C² entries in
the reference and 6C in the new implementation. This excludes the initial
whole-pool index rebuild, which the ordinary regressions include. At 4,096
pairs, reference timings range from 7,387.054 to 7,643.691 ms; component timings
range from 9.368 to 12.870 ms. The timing improvement applies to this exceptional
phase and workload, not whole-node throughput.

Peak RSS comes from `/usr/bin/time -v` and includes fixture construction,
contextual validation, database state and allocator retention. The new mode's
median is higher at both sizes; this is not evidence of reduced process memory.
Its pending/removal sets and temporary component metadata also have a real
allocation cost. The structural 64-entry view bound and payload-identity tests
establish the narrower memory property; whole-node allocation/RSS acceptance
remains open.

Raw samples are in `probe-results.json`, summaries in `probe-summary.json`, and
per-process output/time reports in `{count}-{mode}-{repeat}.{log,time}`. The
preserved `growth-probe`, `environment.json` and `published-source.json` retain
binary/source provenance. Additional validation logs are `all-features-tests.log`,
`core-replacement.log`, `clippy.log` and `release-build.log`.

Evidence directory:
`target/upstream-followup/2026-09-11/reconciliation-growth/`.
`before-fix.log` records both failing resource regressions; the only baseline
source changes were extracting the existing growth loop into a helper and
adding test instrumentation/fixtures. `admission-tests.log` records the fixed
admission regressions.

```sh
export CARGO_HOME=/tmp/rbtc-cargo-home
cargo test --locked --all-features --lib transaction_admission::tests
cargo test --locked --all-features --no-fail-fast -- --test-threads=4
cargo clippy --locked --all-targets --all-features -- -D warnings
RBTC_BITCOIND=/absolute/path/to/bitcoin-31.0/bin/bitcoind \
  cargo test --locked --all-features --test core_replacement_differential \
  -- --ignored --nocapture
RBTC_GROWTH_CLUSTERS=1024 RBTC_GROWTH_MODE=local \
  cargo test --locked --release --all-features --lib \
  reconciliation_growth_resource_probe -- --ignored --nocapture
```

For the benchmark, `RBTC_GROWTH_MODE=reference` runs
the retained old growth loop in the same executable. The probe validates actual
transactions and refreshes their policy metadata before timing only the growth
phase. It does not measure the surrounding contextual replay or whole node.
