# MDBX batch allocation follow-up on macOS (2026-09-14)

The generic MDBX chainstate batch writer copied every surviving created coin
while folding a batch, then decoded and retained an aggregate spent undo that
its caller immediately discarded. It now borrows created coins from the input
and retains aggregate undo only for single-block connect/disconnect callers.
Batch spends still decode each stored coin, including creation-MTP lookup, so
corrupt records abort the transaction. The folded index is released before
encoding durable per-block undo. The scale driver releases its completed input
batch before pruning, metrics and compact-copy maintenance.

This reduces absolute RSS in the reduced maintenance workload, but **does not
close gate 4**. The 256/64 ratio still exceeds 1.5. No default batch size, MDBX
cache/dirty-page setting, disk format or consensus rule changed.

## Mac evidence

Apple M1 Max, 64 GiB RAM, APFS, Rust 1.85.0. All builds, tests and measurements
in this follow-up ran on Mac. No Ubuntu tasks or SSH commands were launched.
Each measured lane used a prebuilt executable and ran serially without this
session compiling concurrently. This was not a cold-disk benchmark or a claim
that unrelated host activity was absent.

Workload unchanged from the original reduced failure: 2M live UTXOs, 4096
synthetic transitions, 5000 updates per transition, 1 GiB geometry, 288 retained
undo rows, 55% compact trigger, 10% minimum reclaim and 50% recompact growth.
Every lane started empty, completed its workload, and passed a separate-process
reopen/full four-table audit. All eight diagnostic lanes share content SHA-256
`d1a9badf1e78d8bdd07324579437b95480db9034da539b610ff7bce43588acdb`.

| Uninstrumented comparison | Batch 64 peak RSS | Batch 256 peak RSS | 256/64 |
| --- | ---: | ---: | ---: |
| Frozen original | 848,134,144 bytes | 2,887,532,544 bytes | 3.404571 |
| Production allocation fix | 741,163,008 bytes | 2,510,520,320 bytes | 3.387271 |

Absolute peaks fell 12.61% and 13.06%, respectively. Both versions compacted
0 times in the 64 lane and 5 times in the 256 lane. The original frozen binary
is `c525ee7`, SHA-256
`8d5e4b95960634dec580bcdc8a2f84ecbeb0f29b8293b339aa5dc9cb8769c6e0`;
its MDBX module and scale-driver sources are byte-identical to pre-fix
`b078798`. The modified binary SHA-256 is
`7a90bb6ab01b55961e8e5f47e394813d04e1f7365e065098968503932c401d70`.
Exact source hashes and the production patch bind that binary to this change.
Harness JSON revision fields describe the invoking worktree, not a reliable
build identity for frozen executables; use the artifact manifest.

A separate instrumented baseline sampled `ps` RSS at internal phase boundaries.
For the first 256 batch, RSS rose from 491440 KiB before folding to 997600 KiB
after folding, then 1242848 KiB after spent-undo materialization. These are
phase observations, including allocator and mapping effects, not isolated
allocation byte counts. Its whole-process ratio was 3.489370. Instrumentation
was removed before building the production comparison and is preserved only
as a diagnostic patch/binary.

## Dirty-page diagnostic, not a production setting

The locally pinned libmdbx source computes its default dirty-page threshold
from total and available host RAM, then bounds it by geometry. A separate
binary applied `txn_dp_limit=4096` to the fixed implementation. The measured
MDBX page size was 16384 bytes, making that threshold 64 MiB. This is not a
whole-process memory cap.

Its 64/256 peaks were 828801024 / 2272985088 bytes, ratio **2.742498**;
content/reopen checks passed and compaction counts remained 0/5. One run with
that threshold still fails the required ratio. Do not infer an adequate global
memory budget or promote this tuning from that result. The production source
was restored to the already tested allocation fix; it retains existing MDBX
defaults. The diagnostic patch, source attribution and binary hash are saved.

## Validation and remaining work

- MDBX unit tests: **15 passed**, including new batch-versus-sequential durable
  content and per-block disconnect coverage, plus rollback on corrupt compact
  coin bytes or missing creation-MTP metadata when aggregate undo is disabled.
- Crash recovery matrix: **1 parent test passed**, exercising all five abrupt
  compact-copy boundaries; the worker test is intentionally ignored in normal
  discovery and explicitly launched by the parent.
- Strict all-target/all-feature Clippy, formatting and diff checks passed.
- Eight workload lanes and eight independent reopens passed content checks.
  The diagnostic wrapper records `ratio_passed=false`; its successful exit
  denotes workload/reopen success, not RSS acceptance. The original runner's
  3.370813 failure and frozen reports remain untouched.

Further work must bound materialization across the input transitions,
per-transaction undo, folded indexes and engine dirty pages together; removing
these copies alone is insufficient. Full-scale maintenance is not rerun while
the reduced maintenance case still fails. Gates 1–3 were not changed by this
follow-up. The completed historical cold replay remains attributed to b078798;
no new mainnet replay or seven-day public soak is claimed for this patch.

Evidence is under `target/mac-followup-20260914/`, with small copies in the
original repository's `session-state/2026-09-13/evidence/mac-followup-20260914/`.
Fresh diagnostic databases were deleted only after independent reopen and full
content comparison. Existing full-scale stores, original failure evidence and
all corpus files were left in place.
