# Atomic transition consumption (2026-09-17)

Overall execution memory, maintenance RSS and production gates: **OPEN**.

The executor previously finished every prepared block into a second, complete
transition vector before invoking the store. It now generates transitions in
block order as the store consumes them, after every script verdict is drained.
Ordinary redb and MDBX apply each transition within one unpublished transaction;
they release that transition before requesting the next. No block prefix is
committed separately. Per-block undo and the final execution tip remain atomic.

`ExecutionChainStore::commit_connect_batch_stream` accepts a fallible exact-size
source and an explicit final tip. A late source/read error or a mismatched final
height/hash aborts the whole storage transaction. This surface allows a future
bounded disk spool to be read during atomic publication without collecting its
entire payload first. It is currently fed from the prepared in-memory results;
**there is no execution disk spool in this change**.

Redb normal-mode consumption sorts each transition in place and rejects duplicate
spends/creations before applying it. Spent-age telemetry advances per block in
the same uncommitted transaction. MDBX retains the final checkpoint height for
tier placement and its 256-block bound. Its borrowed/owned batch APIs share the
same error-aware iterator implementation.

The trait compatibility fallback and redb validation-journal path still collect
the source. Snapshot-overlay and write-back implementations currently use that
fallback. Their folding/journal checkpoint semantics are preserved, but their
memory is not thereby bounded. Phase-one versions, phase-two preparation and
prefetch, retained AppliedBlock undo and engine dirty/MVCC pages also remain.

Tradeoffs requiring final-source performance measurement: phase-three finish
work is now serial as the store requests each block; ordinary redb may write and
then delete inter-block ephemeral outputs within the same transaction instead
of cancelling them in an all-batch in-memory fold. Script/preparation workers
remain parallel. Profiling excludes source-generation time from commit time.

Validation:

- Executor module: 15 passed.
- All-feature library suite after implementation: 998 passed, 0 failed,
  12 ignored, 66.36 s.
- Added two cross-engine regression tests: both passed. Normal redb, validation
  journal fallback and MDBX reject a late source error, incorrect final hash,
  duplicate creation and nonempty endpoint for an empty source without leaving
  UTXOs, tip or undo. Valid unsorted changes with inter-block spending commit
  together and survive reopening.
- Strict all-target/all-feature Clippy passed after removing an obsolete doc
  comment. Formatting/diff checks passed.

No RSS measurement or historical replay was run. Existing reduced and historical
maintenance failures remain open. Next required work is bounded preparation and
transition/undo spooling, its shared memory/disk admission and recovery, and
streaming consumption for validation-journal and write-back/overlay paths.
