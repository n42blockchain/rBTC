# Competing-header resource gate

Status: updated 2026-09-11. The active-only serving projection and reuse of the
validated DAG across within-session polls are implemented and measured. Primary
DAG/candidate/disk retention limits are **not implemented or accepted**.

The [resync acceptance](UPSTREAM_HEADER_RESYNC_GATE.md) removes historical
header replay and temporary replacement graphs on ordinary caught-up polls.
At 100,000 competing siblings, eight empty polls perform zero historical
validations instead of 820,000. Startup and peer failover still reload the
durable graph; this does not close the retention/recovery sequence below.

## Findings in this checkout

- `HeaderDag::insert_contextual` checks contextual difficulty before insertion.
  This excludes incorrectly easy headers, but valid side chains still enter
  `HeaderDag::headers` without a count or byte budget.
- `node::sync_headers` stages unseen headers, persists them with
  `RedbHeaderStore::append_batch`, then commits the stage. The 2,000-header
  response cap bounds one message, not successive messages or retained forks.
- `node::stage_submitted_blocks` also persists valid losing headers. A limit
  applied only to peer synchronization would leave this ingress uncovered.
- `RedbHeaderStore::load_dag_with_deployments` reconstructs the retained graph
  on restart. An in-memory eviction alone would not bound disk usage or
  prevent the same headers from being loaded again.
- The inbound serving projection previously cloned the full header DAG. It now
  uses `HeaderDag::active_chain_snapshot` and `refresh_active_chain_snapshot` at
  initialization, resync and submitted-block publication. Only active ancestors
  are copied; losing forks do not allocate header entries in that view. Refresh
  compares tips and replaces the changed suffix while holding the existing
  write lock. Peak memory accounting must still include the primary DAG, staging,
  source reloads, database caches and index overhead.
- The existing changed-suffix reorg optimization reduces a particular traversal
  cost. It does not bound fork retention, repeated reloads or total work.

## Serving projection measurement (2026-09-10)

`examples/header_resource_probe.rs` generates a 2,501-entry active chain and
valid regtest siblings of genesis, persists batches of up to 2,000, and compares
the previous full-copy serving behavior (`full`) with the production projection
(`active`). It uses the daemon's mimalloc allocator by default and identifies
the selected allocator in every JSON record. No peer-level messages, block
bodies or admission penalties are simulated.

The following values are sampled immediately after the last sibling batch.
RSS is the whole probe process's `/proc/self/status` value, not a per-DAG
allocation claim. Database bytes are file length, not an estimate from 80-byte
wire headers. Each mode/count was a separate process run.

| Valid siblings | Full-copy projection entries | Active projection entries | Full-copy RSS (KiB) | Active RSS (KiB) | Database bytes (both modes) |
| --- | --- | --- | --- | --- | --- |
| 50,000 | 52,501 | 2,501 | 111,588 | 65,532 | 34,222,080 |
| 100,000 | 102,501 | 2,501 | 140,688 | 102,344 | 67,907,584 |

The final projection updates measured 868/1,944 microseconds for full copies
at 50,000/100,000 siblings. Active updates were below the one-microsecond
reporting resolution; this is not a claim of zero work. Reopen in the same
process preserved the tip and every retained sibling. Allocator reuse affects
RSS, so that phase is not a cold-process restart measurement. Early runs using
the system allocator are also retained in the reports and are not the table's
daemon-allocator results.

The result closes the extra competing-branch copy in the serving view. It also
demonstrates the remaining problem directly: primary retained entries, disk
length and replay work continue to grow. The observations do not justify
claiming a whole-node bound or choosing a consensus rejection threshold.

Tests cover unchanged serving capacities under 1,000 siblings, exact active
ancestors/MTP/difficulty context, extension, reorg, staged rollback, network
replacement, and local losing-block submission. All-feature regression
(907 passing tests) and live Core block/transport fixtures (9 passing tests)
passed after the production publication paths were changed.

```sh
cargo run --locked --release --example header_resource_probe -- full 2500 100000
cargo run --locked --release --example header_resource_probe -- active 2500 100000
```

JSON lines and `/usr/bin/time -v` reports are in
`target/upstream-followup/2026-09-10/headers-mimalloc-{full,active}-{50000,100000}.*`.

## Implementation sequence

1. Define separate budgets for retained side-chain entries, temporary candidate
   verification and per-peer work. Track active-chain growth separately; its
   historical consensus context must remain available. Select numerical limits
   from measured allocation and I/O costs, including an inbound DAG copy.
2. Introduce a local resource-deferred outcome distinct from consensus invalidity.
   It must not add a peer-invalid penalty or poison the invalid-header cache.
   Record bounded counters for deferral, retained entries and staging work.
3. Reserve capacity before staging and before durable append at every ingress.
   Release reservations on validation failure, persistence failure and batch
   drop. Use an incremental tip/child index to avoid a full graph scan on every
   admission or eviction. Never remove ancestors still needed by retained tips.
4. Provide a bounded recovery path for an evicted fork that later proves stronger:
   re-request from a retained locator/ancestor and verify its work in a bounded
   candidate stage. Pin ancestors needed for MTP, retargets, deployments and
   execution/undo. If the candidate exceeds staging capacity, retain a resumable
   state and retry/fail over; do not label its blocks consensus-invalid.
5. Persist eviction and graph metadata atomically, or compact into a replacement
   store with a recoverable handoff. Apply the same retention rules during
   restart before materializing the graph. Include temporary compaction files
   in the disk budget and verify crash recovery around each handoff.

Simply rejecting all forks after N entries can prevent valid future reorgs.
It does not satisfy this gate. A constant memory cap on the entire historical
active chain also requires a separate indexed-storage architecture.

## Required acceptance scenarios

| Scenario | Required evidence |
| --- | --- |
| Many valid siblings and long losing forks, over many messages | Side-chain and candidate allocations plateau at configured budgets; record peak RSS and disk use separately. |
| Repeated peer reconnect and local block submission | Both paths enforce the same reservation policy; retries do not rebuild the full retained DAG per rejected header. |
| Evicted fork becomes strongest | Re-fetch and promote the fork with correct cumulative work, retarget/MTP context and execution rollback. No peer-invalid score for resource deferral. |
| Cancellation, failed append and staged reorg rollback | No leaked reservation; active tip, serving projection and persisted graph remain consistent. |
| Restart and interrupted compaction | Evicted headers do not reappear; bounded loading and recovery preserve the accepted active chain. |
| Bootstrap, steady state, Testnet minimum-difficulty transitions | Budget handling does not silently alter difficulty rules or strand synchronization. |

Run these against an unbounded reference at small scale for semantic comparison,
then a sustained resource workload. Unit tests alone cannot close RSS, disk or
recovery acceptance. This gate is independent of Core optimizer parity and the
integration results in `UPSTREAM_2026_FOLLOWUP.md`; the Cargo dependency blocker
was resolved on 2026-09-09, but these production resource limits remain open.
