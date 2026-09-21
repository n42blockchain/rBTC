# Execution spool ownership across storage backends (2026-09-17)

Overall memory, storage RSS and production gates remain **OPEN**.

Node-bound MDBX chainstate and both snapshot-overlay backends now retain the
same execution spool context as ordinary redb. The common executor therefore
spills completed preparation for their `AppliedUndos::Drop` batches as well.
Write-back forwards the context unchanged. Contexts retain the existing node
owner across maintenance close/reopen; they do not create fresh allowances or
reset usage. Temporary files live beside the database path, outside an MDBX
environment directory that may be renamed during compaction.

The existing shared limits and codec apply unchanged: disk reservation before
file growth, codec memory reservation before allocation, record authentication
before decoding counts, and memory leases following actual transition owners.
Standalone stores outside registered node directories still expose no context;
the experimental scale driver that constructs its own full transition vectors
has not acquired spooling merely because its engine supports it.

Real-engine regressions exercise standalone MDBX, MDBX snapshot overlay and redb
snapshot overlay against a registered node owner. For each backend, a late
source failure leaves the original tip, no inserted coins and no undo prefix;
a complete retry publishes both blocks and returns all decoding reservations.
The spool file stays charged until close. Each test then compacts the engine and
repeats these checks through the preserved context, finally verifying zero
memory reservations after all stores drop.

These tests cover backend admission and maintenance ownership, not a total
allocation ceiling. Snapshot collectors still materialize decoded batches;
indexed `Keep` results and preparation inputs, workers, prefetch/version maps,
script queues, engine overhead and fair resumable memory admission remain open.
Neither a new RSS run nor any long-term whole-node acceptance was performed.

Validation: two focused backend tests passed (0.41 s), exercising all three
engines before and after compaction. All-feature library suite: 1,017 passed,
0 failed, 12 ignored (64.06 s). Final strict all-target/all-feature Clippy passed
(7.74 s), formatting and diff checks passed. Previous source f34a82f CI run
35191461583 completed successfully on Windows and Linux (including 90% coverage)
and supply-chain checks. That run excludes this change and the queued FIFO
commit; the earlier Windows timeout evidence is retained, not retroactively
reclassified as success.
