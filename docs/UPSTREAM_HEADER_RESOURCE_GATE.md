# Competing-header resource gate

Status: design and acceptance requirements recorded on 2026-09-09;
production resource limits are **not implemented or accepted**.

## Findings in this checkout

- `HeaderDag::insert_contextual` checks contextual difficulty before insertion.
  This excludes incorrectly easy headers, but valid side chains still enter
  `HeaderDag::headers` without a count or byte budget.
- `node::sync_headers` stages unseen headers, persists them with
  `RedbHeaderStore::append_batch`, then commits the stage. The 2,000-header
  response cap bounds one message, not successive messages or retained forks.
- `node::drain_submitted_blocks` also persists valid losing headers. A limit
  applied only to peer synchronization would leave this ingress uncovered.
- `RedbHeaderStore::load_dag_with_deployments` reconstructs the retained graph
  on restart. An in-memory eviction alone would not bound disk usage or
  prevent the same headers from being loaded again.
- The inbound serving projection clones the header DAG. Peak memory accounting
  must include those copies, staging, database caches and index overhead;
  counting raw 80-byte headers is insufficient.
- The existing changed-suffix reorg optimization reduces a particular traversal
  cost. It does not bound fork retention, repeated reloads or total work.

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
