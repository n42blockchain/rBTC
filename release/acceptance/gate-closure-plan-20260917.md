# Production gate closure plan

The user has explicitly set the objective to closing **all** production gates,
not stopping at individual primitives. The active goal remains unfinished until
implementation and the required acceptance evidence both exist. This plan does
not change acceptance statuses or weaken thresholds.

1. Headers: replace node-owned history and serving copies with fallible disk
   views; atomically publish huge winners from durable raw history; add shared
   work/bytes/disk and total startup-memory admission; measure actual whole-node
   concurrent-peer, execution, restart and fault workloads over sustained runs.
   The current disk-view and block-execution progress is described in
   [the view report](header-views-20260917.md) and
   [node ownership report](header-node-disk-state-20260917.md). Main-node ownership
   is connected; [standby history is shared on disk](header-standby-sharing-20260917.md).
   New marked scratch indexes have [bounded abandoned-owner cleanup](header-scratch-recovery-20260917.md).
   [Live Header storage admission](header-shared-storage-budget-20260917.md) now
   shares cache/read allowances; [persistent file inventory and journal admission](header-persistent-inventory-20260917.md)
   now retain registered file charges through close/restart.
   [Concurrent background Header pipelines](header-background-budget-20260917.md)
   share those allowances. [Snapshot activation and finalization](snapshot-header-memory-20260917.md)
   now use disk history and configured caches. [Offline reindex and verification](offline-header-memory-20260917.md)
   also use disk history. A [shared node memory owner](node-memory-owner-20260917.md)
   now connects chainstate/Header caches and admission candidates with configurable
   startup preflight. [Supporting database caches](supporting-cache-memory-20260917.md)
   and maintenance opens now share that owner too; remaining allocations and
   whole-node acceptance are still open.
2. Admission: finish prevout and all candidate/escaping allocation reservations,
   preserve resumable candidates across work slices, and exercise simultaneous
   peers/local submissions/chain changes. Stable sizing and the existing shared
   ledger are not sufficient evidence of closure.
   [Runtime ownership](admission-runtime-budget-20260917.md) now preserves the
   ledger across session retries and shares it with background validation.
3. Storage maintenance: repair the measured approximately 3.2–3.4 RSS ratio
   failure during maintenance; bound transitions, folded changes, undo and dirty
   pages together, then rerun the failed workload and required scale acceptance.
   Keep the failed historical measurements. Experimental MDBX replacement is
   tracked separately from supported default redb release behavior.
   [Redb journal materialization](journal-materialization-memory-20260917.md) now
   applies one decoded shard at a time; this is not the failed MDBX RSS rerun.
   [MDBX owned-batch consumption](mdbx-consumed-batches-20260917.md) is being
   measured against the unchanged reduced maintenance workload.
   Both borrowed and owned reduced runs still fail RSS; [execution input lifetime](execution-input-lifetime-20260917.md)
   removes additional copies before commit, without claiming a total bound.
   [MDBX environment ownership](mdbx-memory-policy-20260917.md) now fixes spill
   policy across hosts and keeps shared reservations through compact/rebase
   reopen. [Atomic transition consumption](atomic-transition-stream-20260917.md)
   removes the executor's second full transition vector for streaming engines;
   [transition leases](transition-memory-ownership-20260917.md) now follow
   collecting and write-back owners through completion/failure. Reservation
   producers, preparation/spooling and RSS acceptance remain. The default
   [folded commit now preserves atomicity](folded-commit-atomicity-20260917.md)
   on late engine failures; its compatibility copies remain to be bounded.
   [Execution preparation spooling](execution-preparation-spool-20260917.md) now
   evicts completed ordinary redb preparation results and admits codec memory
   and temporary bytes before allocation/growth. Preparation inputs, indexed
   paths, resumable resource scheduling and whole-node acceptance remain open.
4. Optimizer: retain the previously accepted Core differential/budget result as
   historical evidence; rerun affected final-source acceptance and bind exact
   source identity after production changes stop.
5. Historical replay: retain the accepted selected 935001–963350 comparison;
   after final source freeze, review effects and rerun/rebind required acceptance.
   This does not claim genesis-to-tip acceptance.
6. Public soak: only after resource changes are frozen and both networks are
   caught up, run the required full 604,800 seconds with prescribed restart/fault
   exercises. Finalize unmodified measured reports and verify release preflight.

Native signing credentials and signed artifact rehearsal remain separate release
prerequisites. External credentials cannot be manufactured or replaced with test
fixtures. Check their actual availability before release; do not silently mark
those prerequisites satisfied.

Continue on the Mac when feasible, preserve small evidence and avoid duplicate
large datasets. Commit subjects must be English. No release tags or publication
are implied by this engineering goal.
