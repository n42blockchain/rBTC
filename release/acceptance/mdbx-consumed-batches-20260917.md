# MDBX atomic batch input lifetime

The execution driver already calls `commit_connect_batch_owned`, but MDBX used
the trait's borrowing fallback. Inputs and undo survived until the whole batch
completed, alongside a batch-wide folded index and engine pages.

MDBX now implements the ownership-taking entry point. Borrowed and owned callers
share one transaction implementation that validates, folds and writes one
transition at a time. An owned transition and its undo drop before the next
transition's temporary index is built. Only the final tip and one commit publish
the checkpoint. A late error aborts earlier writes. The existing 256-block bound
and final-height tier classification are unchanged.

The borrowed API remains supported, retaining caller-owned inputs as required.
The original scale driver still uses it and is unchanged: no batch size, retention,
maintenance trigger, RSS threshold or input shape was relaxed to obtain a pass.
The owned production route additionally benefits from releasing consumed inputs.

This trades cross-block cancellation before disk writes for smaller temporary
indexes. Ephemeral outputs may now be written and removed inside the same engine
transaction. Dirty-page growth, throughput and maintenance behavior therefore
require measurement; lower application allocation alone is not an RSS result.
Batch-vs-sequential content and per-block disconnect tests exercise the owned
route. Borrowed-route tests still cover ephemeral outputs, late rollback, corrupt
spent records and the 256/257 boundary.

The planned reduced measurement repeats the historical failed workload: 2M UTXOs,
4,096 transitions, 5,000 updates each, 1 GiB geometry, 288 undo retention, compact
55%, reclaim 10%, growth 50%, serial 64/256 lanes, RSS ratio <=1.5. Reports every
256 transitions aid monitoring without changing workload or thresholds. The
runner freezes binaries, hashes the source, reopens each store independently and
runs the compaction crash matrix. Results belong to that frozen source only.

No RSS or whole-node gate is closed by this implementation checkpoint. Owned
input generation, engine pages and other concurrent components still need a
shared byte budget. Full-scale, final-source replay and public soak remain open.

Mac checks: 15 MDBX tests passed; full all-feature library tests passed 984
(0 failed, 12 ignored; 67.26 seconds). Strict all-target/all-feature Clippy,
format and diff checks passed. Prior e69421d CI 35184076573 passed all jobs;
that CI result does not cover this new implementation.
