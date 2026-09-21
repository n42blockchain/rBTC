# Header retention and fork recovery, 2026-09-16

Automatic idle retention and network fork continuation are implemented. The
header-resources production gate remains open for hard ingress/staging budgets,
bounded startup materialization and sustained RSS/disk acceptance.

## Automatic idle retention

The validating-node loop performs retention when the execution hash exactly
matches the selected header tip. Both peer headers and local submitted headers
reach this shared loop. A reorg or execution lag skips retention.

The side-header target is 50,000, with at most 2,000 removals per idle iteration.
Selection scans the retained index once using a heap bounded by the removal
batch. It selects the highest non-active headers and orders them child-first,
so a removed ancestor never leaves a retained descendant disconnected. Active
history and the executed tip remain protected.

The existing eviction guard stages removals in memory. The redb transaction
atomically removes the hash, insertion-order and reverse-index rows; only a
successful durable commit publishes the in-memory eviction. Failure rolls back
the guard. Reopen therefore does not resurrect evicted rows. Freed redb pages
can be reused; the file is not automatically shrunk.

This is a retention target, not an ingress cap. A burst or an in-progress fork
can exceed it before idle maintenance. The first eviction builds a child index
over retained headers, and startup still reconstructs retained history. The
50,000 target is not a calibrated whole-process memory allowance.

## Network recovery

Previously getheaders always used the active-chain locator. A losing fork with
a full 2,000-header prefix could repeatedly return that prefix, or stop on an
already-known prefix, without obtaining its stronger suffix.

The synchronization loop now advances a locator from the validated response
tip along that fork's own ancestry. This also advances past a full known prefix.
A repeated/non-progressing known response ends the loop rather than cycling.
The cursor changes only after validation and durable publication; cumulative
work still determines the active chain. After eviction or restart, ordinary
locator negotiation reacquires headers from a retained ancestor, validates
them contextually and follows the fork until it can win. Subsequent block
execution continues through the existing validation/reorg path.

## Tests

A loopback peer test creates a 2,001-header active branch and a 2,002-header
competitor. It covers both a retained 2,000-header prefix and automatic eviction
in two 1,000-header batches followed by reopen. The test checks that execution
lag prevents eviction, continuation requests start at the losing fork tip,
the stronger fork wins, and another database reopen preserves that result.
This tests header selection/recovery; it does not supply full block bodies to
prove execution rollback in this new scenario.

The header-filtered suite passed 25 tests with one resource probe ignored;
strict all-target/all-feature Clippy passed. Existing failure-injection tests
cover atomic durable eviction and in-memory rollback.

The full all-feature library suite subsequently passed 936 tests, with zero
failures and 11 explicitly ignored tests (78.72 seconds). Existing optimizer
and replay acceptance reports remain bound to their historical source commit;
they are not fresh release acceptance for these production changes.
