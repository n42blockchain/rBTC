# Admission snapshot consistency and storage RSS diagnosis, 2026-09-13

Continues `a69ee80` in the existing Mac worktree. Gate 2 now pins the durable
transaction-pool snapshot from byte sizing through decoding. Gate 4's reduced
workload still exceeds its RSS ratio even when compaction is disabled. No gate
is closed by this follow-up; no new Linux regression, full-scale maintenance,
mainnet replay or public soak is claimed.

## Stable admission snapshot

Previously, peer admission sized persisted rows in one read transaction, then
opened separate transactions for expiry, active payloads and disconnected
payloads. A concurrent replacement could grow the payload after reservation or
mix admission times with another active set.

`AdmissionSnapshotRead` now owns one redb MVCC read transaction. The peer path
sizes that version, charges/reserves its existing shared budget, then consumes
the same read to decode active transactions, expiry metadata and (when needed)
disconnected transactions. Active transactions are decoded once. The read is
released before candidate validation and durable publication; writers can
commit while it is held. Expiry still rejects metadata that does not match the
active set. Its comparison no longer constructs a second transaction-ID set.

Two new regressions cover an empty sized snapshot followed by store growth,
and another thread replacing active/disconnected data and admission times while
the old snapshot remains pinned. The latter verifies both the old coherent view
and a fresh view of the new committed state.

Final targeted Mac validation:

- Transaction-pool store: **17 passed / 0 failed**, including the two new cases.
- Peer resource deferral and dry-run classification: **2 passed / 0 failed**.
- All-target/all-feature strict Clippy, formatting and diff checks passed.

These are 19 targeted tests, not a new complete cross-platform suite. The pinned
read fixes the persisted-row sizing race; allocation multipliers remain
estimates. Prevout materialization, transient graph/relay/persistence allocations,
concurrent in-memory pool growth, configurable calibrated limits and resumable
large-candidate scheduling still require gate 2 work.

## Reduced storage diagnosis using the frozen binary

Reused the original `c525ee7` release test executable, SHA-256
`8d5e4b95960634dec580bcdc8a2f84ecbeb0f29b8293b339aa5dc9cb8769c6e0`.
The workload remains 2M live entries, 4,096 transitions, 5,000 updates per block,
1 GiB geometry and 288 undo entries. Three lanes ran serially; a diagnostic
wrapper sampled `ps` RSS and source/copy/old directory allocations approximately
every 0.1 seconds. These directory labels are observations, not exact internal
phase instrumentation. Some Rust compilation/checking overlapped this diagnostic;
wall times are not controlled throughput comparisons.

| Lane | Whole-process peak RSS, bytes | Ratio to 64 | Compactions | Sampled simultaneous database allocation, bytes |
| --- | ---: | ---: | ---: | ---: |
| 64, compact enabled | 902,545,408 | 1.000000 | 0 | 383,139,840 |
| 256, compact enabled | 2,886,811,648 | **3.198522** | 5 | 1,004,273,664 |
| 256, compact disabled | 2,834,677,760 | **3.140759** | 0 | 638,566,400 |

Both 256 ratios exceed 1.5. All three workload processes and independent reopen
checks exited 0 and matched the original reduced-workload digest:
`d1a9badf1e78d8bdd07324579437b95480db9034da539b610ff7bce43588acdb`.
Workload/reopen success checks content, not the RSS acceptance ratio. This
wrapper is diagnostic; the original runner's exit 1 / 3.370813 failure is unchanged.

The compact-enabled sampled RSS reached 2,846,670,848 bytes while a copy existed,
and 2,507,210,752 bytes with only the source path present. The no-compaction lane
also exceeded 2.83 GB whole-process RSS. Thus compaction is not necessary for the
reduced-workload failure; disabling it does not resolve the gate. Sampling alone
does not partition allocator retention, input batches, database dirty pages and
mapped pages. The sampled simultaneous disk maximum is a lower bound, not a
continuous peak or a full-scale capacity guarantee.

Next inspect batch lifetimes and duplicated materialization:
`transition` retains created/spent coins and undo for the whole input batch;
`fold_batch_changes` builds another owned net-change view; and the batch caller
discards the aggregate undo returned by `apply_net_changes`. The diagnostic
harness also retains `transitions` through maintenance. Profile these separately
before changing production code or claiming a root cause. Full-scale maintenance
and copy-space acceptance remain outstanding.

## Evidence and disk use

Raw evidence: `target/continuation-2026-09-13/`, including final test/Clippy logs,
`test-summary.json`, the diagnostic scripts, sampled JSONL, workload/reopen
reports, time output, and `maintenance-diagnostic/summary.json`.

The first wrapper completed the 64 churn but failed to launch its reopen due to
an argv slice error. The corrected wrapper reopened that completed database
without rerunning churn, then continued the two 256 lanes. The initial script
is preserved. The frozen harness derives its JSON `revision` from the invoking
worktree, so these raw reports say `a69ee80-dirty`; binary attribution instead
uses the matching frozen artifact SHA-256 and original run manifest.

Each newly created diagnostic database was removed only after its independent
reopen/content verification. All three were removed; only small reports/logs
remain. Existing frozen lane databases, original failure evidence and corpus
were not changed. Windows corpus access is still missing; no new large data
copy or seven-day run was started.
