# CI coverage recovery — 2026-09-14

The coverage job on `69382ae` failed although its tests passed. Its GitHub LCOV artifact and the Mac reproduction both covered **86,495 / 98,829 lines (87.5198575%)**, below the existing 90% requirement.

This change adds eleven deterministic tests. Production behavior, coverage scope, ignored tests, and the threshold remain unchanged.

- MPHF snapshot indexes: colliding groups, nonmembers, nonnumeric vout order, changed source identity, corrupt sidecars, and malformed groups rejected before publication.
- FDB import: longest continuation, bounded import, checksum/header corruption, independent ledger verification, and CLI failures without output creation.
- Overlay replay: real script-validating replay on redb and MDBX, direct and buffered commits, stale retained/staged recovery, corpus ceiling, durable restart, compaction and rebase. All four configurations produce matching canonical content. Zero maintenance thresholds are injected only in the tiny test fixture to exercise those branches; this is not an RSS or production-capacity acceptance result.
- Node recovery: stale explorer and optional indexes rebuild from local or simulated-peer history; peer admission retries orphans, persists RBF replacements, and rejects bad scripts and their children.
- Inbound and download paths: unexecuted headers stay hidden, ledger mismatches fail, replacing a connection preserves the new data source, and a failed auxiliary peer falls back without re-fetching received blocks.

## Mac validation

`CARGO_BUILD_JOBS=4 cargo llvm-cov --locked --all-features --lcov --output-path target/ci-followup-20260914/lcov-final.info --fail-under-lines 90` exited **0**.

- **89,265 / 99,085 lines — 90.0893173%**.
- **976 passed, 0 failed, 34 ignored**.
- Strict `cargo clippy --locked --all-targets -- -D warnings` and the all-features equivalent passed.
- `cargo fmt --check` and `git diff --check` passed.

Raw final logs, LCOV, counts, and SHA-256 manifest are retained in the original workspace at `session-state/2026-09-13/evidence/ci-coverage-fix-20260914/`. Earlier failed coverage runs remain in the development worktree's `target/ci-followup-20260914/`.

All local development and verification ran on Mac using small temporary fixtures. The six production acceptance gates keep their previously documented status; this test change does not close the resource or seven-day public-network gates. GitHub CI confirmation is recorded separately after the push.
