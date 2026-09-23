# Optimizer budget acceptance — 2026-09-22

This run rechecked the optimizer against the current source revision
`78026fee557a8941ccc8e8bc5a28a4581d47e03b` (`audit/group-b-decisions`).
The earlier [2026-09-15 report](optimizer-budget-20260915.md) remains unchanged
and records the original acceptance on `3cf44ae`.

## Results

- Pinned Bitcoin Core 31.0 archive SHA-256:
  `0ba0ef5eea3aefd96cc1774be274c3d594812cfac0988809d706738bb067b3e3`.
- Full optimizer differential: **4096/4096 cases passed**, including 2048
  permuted-label cases. Maximum observed Rust search work was **650291** units.
  The Core adapter reported exact optimal diagram and order parity for every
  converged case.
- Exhausted-search fallback regression passed across the tested cluster sizes
  and allowances; it preserved topological validity and did not worsen a valid
  previous diagram.
- Small-graph exhaustive subset/minimal-chunk proof regression passed.

The generated Core summary SHA-256 is
`7f5577f8fe6c9a0f5bb13fbdfc49d1a3997d74d7bc3285628ce627d10f2d1a9a`.
The adapter, extracted Core tree, input/output, and summary are retained under
the ignored `target/optimizer-gate-20260922/` directory; no large database or
benchmark corpus was created.

## Reproduction

```sh
c++ -std=c++20 -O2 -Wall -Wextra -Werror -DABORT_ON_FAILED_ASSUME \
  -isystem target/optimizer-gate-20260922/bitcoin-31.0/src \
  contrib/core31_linearize.cpp \
  -o target/optimizer-gate-20260922/core31-linearize
RBTC_CORE_LINEARIZE="$PWD/target/optimizer-gate-20260922/core31-linearize" \
RBTC_CORE_LINEARIZE_REPORT_DIR="$PWD/target/optimizer-gate-20260922/core-optimizer" \
  cargo test --locked --all-features --lib \
  feerate_diagram::optimizer::tests::full_optimizer_matches_core_optimal_diagrams_and_orders \
  -- --ignored --nocapture
cargo test --locked --all-features --lib \
  feerate_diagram::optimizer::tests::exhausted_search_never_worsens_a_valid_previous_diagram
cargo test --locked --all-features --lib \
  feerate_diagram::optimizer::tests::closure_optimizer_proves_optimal_minimal_chunks_against_all_small_subsets
```

This closes optimizer parity and bounded-budget verification for the recorded
source revision. It is not evidence of whole-pipeline resource acceptance. The
release candidate still needs its final-source evidence check after code is
frozen; later optimizer changes also require rerunning this acceptance.
