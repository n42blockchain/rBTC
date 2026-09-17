# Node-owned disk header state and streaming promotion

This is implementation progress toward the Headers production gate, **not gate
closure**. The main node now uses the validated disk index introduced in
`730f307` for synchronization, execution queries, and published serving/template
views. Normal startup replays raw headers in batches of at most 2,000 using the
process-wide work pool; subsequent polls reuse the index. Serving snapshots pin
an immutable read version and exclude losing branches without copying history.

Candidate promotion no longer materializes a complete winning fork into a RAM
DAG. It preflights the per-frame allowance, durably records promotion intent,
imports checked frames, verifies the complete winning tip, and durably clears
intent before publishing the final view or deleting the journal. Legacy DAG
loaders reject unfinished intent. Restart replays raw records privately and
revalidates the journal, skipping only the already imported verified prefix.
Known prefix replay still checks consensus and exact derived identity even if a
later checkpoint floor has already been learned. Unknown old forks remain
rejected. Startup completes pending import before constructing standby seeds,
so the legacy loader's refusal cannot prevent peer connection and recovery.

A staged derived-index transaction is aborted if raw persistence fails. An
ambiguous derived commit poisons the index and requires replay. Persistent child
counts and a leaf table support bounded idle eviction without reconstructing a
RAM child graph. Selected and pinned ancestry remain protected; old read
versions continue to see their original records. Scratch ownership extends to
the last published reader and normal shutdown removes its directory. Candidate
replay and import ping peers between frames on long runs.

Validation on the Mac:

- All-feature library suite: 966 passed, 0 failed, 11 ignored (64.82 seconds).
  Strict all-feature/all-target Clippy passed. The final change only boxes the
  startup recovery future to keep caller futures small; the restart regression
  is rerun after that adjustment. Logs are saved under
  `target/header-node-disk-state-20260917/` with source metadata.
- Restart fixtures cover intent before import, a partially imported stronger
  fork, complete import before intent removal, and a leftover journal after
  intent removal. Another case exercises pre-connection recovery. The first
  network locator uses the complete winner; missing journals fail locally.
- Raw transaction failure after an inserted row rolls back both durable rows
  and the unpublished disk-index transaction. Tests also cover staged abort,
  last-reader cleanup, leaf pins/old read versions, checkpoint replay, ordinary
  reconnect/resync, candidate deferral, local submission and serving projection.
- These are deterministic restart-state tests, not real process-kill or
  sustained whole-node resource acceptance. Earlier index-only RSS measurements
  are not rebound to this changed implementation.

Still open: standby startup retains/clones a full in-memory DAG; offline
reindex paths also retain memory history. Shared total bytes/disk admission,
startup aggregate memory, multiple candidate scheduling and crash-leftover
scratch cleanup remain unfinished. The default candidate file limit remains
128 MiB and must be integrated with the node's total disk policy. The current
per-frame allowance is not a process RSS ceiling; pinned old read versions and
engine caches require combined accounting. Long-running concurrent-peer,
execution/reorg, restart and fault RSS/disk measurements are still required.
Other production gates remain tracked by the closure plan; no public soak or
release publication was started here.
