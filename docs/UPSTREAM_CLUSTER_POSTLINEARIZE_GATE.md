# Cluster postlinearization acceptance, 2026-09-11

Status: the backward/forward refinement is implemented and accepted for the
production 64-transaction bound. Full Core optimizer parity remains open.
Baseline: `f9650a14113d2374d9828cf702da8ca498d3e9ef`.

## Defect and change

The previous ancestor-set greedy order can combine an unrelated lower-rate
transaction with a profitable shared-parent package. In this connected fixture,
the last transaction links the otherwise independent branch:

| Transaction | Fee | Vsize | Parents |
| --- | ---: | ---: | --- |
| 0 | 0 | 1,000 | None |
| 1 | 1,000 | 100 | 0 |
| 2 | 1,000 | 100 | 0 |
| 3 | 100 | 100 | None |
| 4 | 0 | 1 | 2, 3 |

Greedy emits `[3, 0, 1, 2, 4]`, yielding chunks `2100/1300, 0/1`. Refinement
emits `[0, 1, 2, 3, 4]`, yielding `2000/1200, 100/100, 0/1`: strictly more fee
at intermediate sizes, with identical total fee and size. The added regression
fails on the baseline, and the pinned Core reference produces the refined order.

`Cluster::linearize` now refines its greedy result with a backward pass followed
by a forward pass. Each pass merges dependent adjacent groups or swaps
independent groups when their rates require it. Transitive ancestor/descendant
sets use 64-bit masks; group membership uses index links rather than a vector
allocation per group. Equal rates preserve pass order. Comparisons and sums
retain the existing integer overflow protections.

The algorithm follows
[Core 31 PostLinearize](https://github.com/bitcoin/bitcoin/blob/v31.0/src/cluster_linearize.h#L1765).
Its output preserves topology, does not worsen the input diagram, makes chunks
connected and is optimal for trees with at most one parent per transaction or
at most one child per transaction. The work-budgeted optimizer preceding this
refinement is a separate implementation and acceptance task.

The extra phase uses four fixed 64-slot arrays and two vectors capped at the
cluster size. Its two passes perform at most 4,032 adjacent-group comparisons
for 64 entries; index movement is also bounded. It introduces no recursive or
unbounded search. Inputs beyond 64 retain the existing total greedy behavior;
production callers already enforce the 64-entry component limit. This bounds
the added phase, not total admission CPU, allocations or process RSS.

## Evidence

- **4,096 exact Core comparisons**: 2,048 generated graphs, each with an arbitrary
  topological order and a greedy order. Cases include 0–64 entries, permuted
  labels, trees, stars, dense DAGs, disconnected graphs, zero/equal fees and
  aggregate values near integer limits. Both permutations and exact order match
  the unmodified upstream `PostLinearize` template.
- Independent properties check topological permutations, connected chunks and
  non-worsening diagrams. Small one-parent/one-child trees are compared against
  every possible topological ordering. Moved-leaf fee increases and the larger
  input fallback have regressions.
- An actual admission regression builds five valid transactions with two funded
  roots, two shared-parent children and a joining child. The pool's diagram and
  relay snapshot place the 2,200-sat shared package ahead of the 130-sat branch
  and 100-sat joining transaction. This exercises the production adapter as well
  as the pure primitive.
- Full all-feature suite: **925 passed**, 0 failed, **30 ignored** (886 library +
  39 integration; subprocess helper output excluded), using four test threads.
  The additional ignored gate is the native Core comparison, explicitly run.
- All **three** live Core replacement/package-pressure/fee-decay tests pass.
  Strict root and fuzz-workspace Clippy checks pass.

The fuzz target now checks chunk connectivity and includes the named
`shared-parent-refinement` seed. The pinned ASAN campaign completed **100,000
runs in 109 seconds**, exit 0, with no crash. Root and fuzz formatting checks
also pass. Raw fuzz results are retained; the bounded campaign is not an
exhaustive optimizer proof.

## Reproducing the native oracle

The reference uses the official `bitcoin-31.0.tar.gz`, verified against its
release `SHA256SUMS`:

```text
0ba0ef5eea3aefd96cc1774be274c3d594812cfac0988809d706738bb067b3e3
```

Extract the archive, then compile the small test adapter against that source:

```sh
c++ -std=c++20 -O2 -Wall -Wextra -Werror -DABORT_ON_FAILED_ASSUME \
  -isystem /absolute/path/to/bitcoin-31.0/src \
  contrib/core31_postlinearize.cpp -o /tmp/core31-postlinearize
RBTC_CORE_POSTLINEARIZE=/tmp/core31-postlinearize \
  cargo test --locked --all-features --lib \
  core_31_postlinearization_matches_generated_orders_exactly -- --ignored --nocapture
cargo +nightly-2026-07-13 fuzz run feerate_diagram fuzz/corpus/feerate_diagram \
  -- -runs=100000 -max_len=2048
```

The adapter replaces only the assertion-reporting link hook with an aborting
handler; upstream algorithm, bitsets and arithmetic are compiled unchanged.
Assertions remain enabled. C++ is required only for this optional reference
gate; production refinement is Rust. Set `RBTC_CORE_POSTLINEARIZE_REPORT_DIR`
to an existing directory to retain each comparison's inputs and both outputs.

Local logs and source/binary hashes are under
`target/upstream-followup/2026-09-11/cluster-postlinearize/`, including
`before-fix.log`, `core-postlinearize.log`, the `core31-post-*` fixtures,
`admission-refinement.log`, `all-features-tests.log`, `core-replacement.log`,
`clippy-final.log`, `clippy-fuzz.log`, `fuzz.log` and `published-source.json`.

## Remaining scope

The implementation still begins from greedy ordering. Core's spanning-forest
search, cost accounting, optimal/minimal-chunk result and incremental reuse of
previous linearizations are not implemented by these two passes. Agreement with
the isolated refinement and three live fixtures cannot establish complete
replacement or optimizer parity. Whole-pipeline admission resources, durable
header retention, full storage-scale/mainnet evidence and the seven-day soak
remain open in the [follow-up ledger](UPSTREAM_2026_FOLLOWUP.md).
