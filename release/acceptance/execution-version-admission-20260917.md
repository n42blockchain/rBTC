# Execution version-index memory admission (2026-09-17)

Overall startup, execution memory and production gates remain **OPEN**.

The parallel executor previously built its complete per-block output deltas and
batch coin-version index before consulting the node memory allowance. Node-bound
backends now reserve an allowance before creating these slots, vectors, hash
buckets, Arc-owned output coins and script copies. The reservation remains live
through parallel preparation, then releases only after the version index is
dropped. Error unwinding follows the same payload-before-reservation order.
Both `AppliedUndos::Keep` and `Drop` paths use this admission, independent of
whether completed preparation will be spooled.

The checked estimate uses the actual transaction input/output counts and script
lengths without allocating another collection. It includes eight hash slots per
possible logical key to cover load-factor rounding and simultaneous old/new
buckets during growth, three vector elements per history/delta entry for
geometric growth/relocation, coin/Arc storage and conservative script space.
The initial output-construction vectors precede hash-index construction, rather
than coexisting with its peak. This is a conservative logical allocation
allowance tied to this implementation, not an observed allocator/RSS ceiling.
Overflow and reservation failure return an explicit `ExecutionMemory` local
resource error. They are not invalid-block evidence and do not punish peers.

The existing real Redb/write-back executor regression now exhausts the shared
owner and checks both indexed and non-indexed modes: each fails specifically at
version memory admission, publishes no tip/pending prefix, creates no spool
bytes and leaks no reservation. Returning the external occupation then permits
the complete batch to succeed, flush and reopen. Node error classification tests
cover both spool and preparation-memory failures.

Remaining allocations include the caller's blocks, transaction-id derivation,
external-input discovery, prefetch UTXOs and overlay maps allocated before this
stage, thread stacks, script queues, prepared results and indexed undo copies.
In particular, prefetch exposes mutable entries for write-back reconciliation;
its eventual reservation must follow both reconciliation growth and transfer
into the overlay, not expire when the prefetch wrapper disappears. None of
those missing owners is implicitly covered by the version reservation.

Resumable admission, bounded spill/readback for all retained owners, whole-node
physical disk accounting, maintenance RSS acceptance and final long-node/public
runs remain required. No RSS or soak measurement was run for this change.

Validation: nine focused execution-spool/backend/error-classification tests
passed (0.49 s), including the strengthened producer-admission assertions.
All-feature library suite: 1,017 passed, 0 failed, 12 ignored (64.36 s). Strict
all-target/all-feature Clippy passed (11.06 s); formatting and diff checks passed.
