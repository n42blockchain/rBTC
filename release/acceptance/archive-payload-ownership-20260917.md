# Owned archive payload admission through node replay

Selected file records now obtain shared memory admission before allocating raw
bytes. ArchiveBlock owns an immutable Vec plus its lease inside a shared Arc;
cloning a block shares the allocation and reservation. The charge includes the
exact byte allocation and Arc payload/counters. Allocation is released before
its lease. No mutable byte access or ownership-stripping conversion is exposed.
Equality compares bytes, independent of the budget owner.

LedgerBlockBatch now returns Vec<ArchiveBlock>. Its cloned entries share payloads;
moving an individual block out retains its admission. read_owned_block provides
the same contract for one retained record. Node fee-estimator recovery, stale-tip
handling and retained index backfills use this API. Node replay prevalidation
moves ArchiveBlock through PrevalidatedBlock.bytes and staging; carried/prefetched
byte containers also use the immutable type. Ledger staging and archive writing
borrow generic byte slices, preserving archive bytes without materializing a
second raw Vec for this transfer. Direct replay ledger failures retain the local
resource error classification; failed speculative read-ahead is still discarded
for the next primary read to report, as before.

The From<Vec<u8>> compatibility constructor wraps caller-owned bytes without
claiming admission. Network download serialization and newly regenerated raw
bytes still need admission before their original allocation. The legacy
read_block Vec-returning API and serving trait boundary retain compatibility and
still copy without a lasting output owner. These are explicit remaining gaps.
Decoded Bitcoin objects and outer vectors/metadata are also not covered by the
per-block payload charge (including the outer Vec allocation on batch clone).

Validation: all-feature library tests: 1,058 passed, zero failed, 12 ignored
(74.12 s). Final locked all-target/all-feature Clippy passed (9.61 s);
fuzz locked all-target Clippy passed after lock synchronization. The node
ownership regression passed (0.39 s); formatting and diff checks passed.
Regressions test a one-byte admission shortfall,
exact payload-only charge after the decoder drops, shared pointers and aliases,
cross-thread writing, byte-identical output, final-owner refund, and a real
node replay prevalidation/staging transfer with a retained alias.

CI correction: run 35213179066 at a5e2fa3 failed Linux before tests because the
new local codec dependency was absent from fuzz/Cargo.lock. The fuzz lock now
adds only that path dependency, matching the root lock; registry versions stay
unchanged. The failure is retained as evidence, not relabeled successful.

This closes raw archive record ownership on the listed paths, not the total
32 GiB startup/RSS, resumable scheduling, physical disk or sustained whole-node
acceptance gates. Native multithreaded encoding and manifest admission remain
open as well.
