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
   startup preflight. [Locally computed batch transaction IDs](batch-transaction-id-memory-20260917.md)
   now obtain and retain shared admission before hashing/allocation. [Deferred script payloads](deferred-script-memory-20260917.md)
   retain preallocation admission through queueing, execution and cancellation. [Immutable undo ownership](undo-shared-ownership-20260917.md)
   now shares payloads and retains existing admission through escaped undo handles. [Original block preparation](block-preparation-memory-20260917.md)
   now reserves before worker payload allocation and transfers ownership through net changes and undo. [Retained archive batches](archive-batch-streaming-20260917.md)
   now verify the full stream while retaining only the bounded requested prefix. [Staged recovery reads](staged-batch-recovery-20260917.md)
   are now identity-bound and used by startup/reindex validation; repeated full verification work and publication allocations remain open. The user-selected default is now 32 GiB;
   [bounded coin queries](bounded-coin-reads-20260917.md) check scripts before
   copying in ordinary stores and mutable snapshot overlays.
   [Snapshot-base group queries](snapshot-group-streaming-20260917.md) now
   stream with fixed scratch instead of allocating a complete large group.
   [Snapshot index opens](snapshot-index-memory-20260917.md) now admit decode
   buffers and retain shared reservations for MPHF/fingerprint caches, including
   overlapping old/new bases during rebase.
   [Streamed index publication](snapshot-index-streamed-output-20260917.md)
   removes whole encoded output copies. [Build array admission](snapshot-build-tables-memory-20260917.md)
   now charges MPHF scratch/retained arrays and slot tables through the database
   owner. [Snapshot scans](snapshot-scan-spool-20260917.md) now admit location-list
   growth and spill large decoded groups to charged private files; table storage
   still scales with input and resource denial is not yet resumable. [Supporting database caches](supporting-cache-memory-20260917.md)
   and maintenance opens now share that owner too; remaining allocations and
   whole-node acceptance are still open.
   [AS-map payloads](asmap-memory-20260917.md) now reserve before startup reads
   and embedded copies and retain their charge through shared map ownership. [FIFO work admission and cancellable
   standby waits](header-work-scheduling-20260917.md) now prevent asynchronous
   waiters from being bypassed; full resumable candidate scheduling remains.
   [Replay keepalive during work admission](header-replay-keepalive-20260917.md)
   preserves queued progress through Ping/Pong; Windows recovery timeouts and
   full resumable scheduling remain open.
   [Startup listener progress](startup-listener-progress-20260917.md) now releases
   multi-thread runtime workers during API/pool/peer-store initialization, with
   a reproduced ZMQ greeting starvation regression. [Cooperative embedded AS-map
   validation](asmap-cooperative-startup-20260917.md) addresses a separately
   reproduced coverage shutdown failure and repeated concurrent validation;
   final-source platform acceptance remains open.
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
   Both borrowed and owned reduced runs still fail RSS.
   [Streamed-input measurement](mdbx-streamed-inputs-20260917.md) also fails the
   unchanged 1.5 threshold (1.53015), despite removing the full generated batch;
   none of these three reduced runs triggered automatic compaction.
   [Mapped-page investigation](mdbx-readahead-20260917.md) adds native memory
   diagnostics and disables host-RAM-based read-ahead. Source 257bd32 passes the
   reduced 2M streamed RSS criterion (1.46980) and a separate 4M live-set run with
   1/2 actual compactions (1.13624); full scale and sustained whole-node ownership
   remain open, and historical failures remain preserved.
   [Compaction mapping lifetime](mdbx-copy-lifetime-20260917.md) now closes the
   source before opening/validating the copy, retaining recovery reservations.
   Source 9907c8e repeats the 4M maintenance workload with lower absolute peaks
   (652.5/846.1 MB), ratio 1.29662 and the same 1/2 compactions; full-node gates
   remain open; [execution input lifetime](execution-input-lifetime-20260917.md)
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
   and temporary bytes before allocation/growth. [MDBX and snapshot overlays](overlay-execution-spool-20260917.md)
   now share that context through maintenance too. [Version-index admission](execution-version-admission-20260917.md)
   reserves before building parallel output deltas and their history index.
   [Prefetch ownership](execution-prefetch-ownership-20260917.md) now charges
   discovery/maps and retained query results through refresh and overlay transfer.
   Engine-level read bounds, prepared copies, indexed
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
