# Optimizer budget acceptance — 2026-09-15

This report covers the frozen source commit
`3cf44ae68003ceb78e9f0c77042667b475091716`.

## Evidence

- The full optimizer differential against the pinned Bitcoin Core 31 adapter
  passed `4096/4096` generated cases.
- The cases included `2048` permuted-label cases, zero/equal-fee cases, dense
  and sparse DAGs, invalid previous orders, and cluster sizes from 0 through
  64. The run reported maximum Rust search work of `649961` units.
- The exhausted-search regression passed for cluster sizes 0 through 64 and
  allowances `0`, `1`, `100`, `2000`, `10000`, `100000`, and `1000000`. Every
  result stayed topological and was equal to or better than the valid previous
  diagram while respecting the supplied work allowance.

## Reproduction

```text
RBTC_CORE_LINEARIZE=target/mac-followup-2026-09-13/core31-linearize \
  cargo test --locked --all-features --lib \
  feerate_diagram::optimizer::tests::full_optimizer_matches_core_optimal_diagrams_and_orders \
  -- --ignored --nocapture

cargo test --locked --all-features --lib \
  feerate_diagram::optimizer::tests::exhausted_search_never_worsens_a_valid_previous_diagram \
  -- --nocapture
```

Both commands exited zero on the frozen source. The first command also wrote
the Core adapter output and summary under
`target/current-acceptance-20260915/core-optimizer/`; the generated summary
records `4096` cases and `2048` permuted labels.

This acceptance is limited to optimizer parity and bounded budget behavior. It
does not claim whole-pipeline admission resource acceptance.
