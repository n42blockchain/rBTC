# Validation journal materialization ownership

AssumeUTXO finalization calls `materialize_validation_deltas` before comparing
sets. That redb path previously folded every historical journal update into one
hash map, created a second sorted pointer vector and cloned surviving UTXOs into
a write vector. Its application memory grew with the union of journal keys.

Materialization now processes journal rows in order and one decoded shard at a
time (one record for the legacy format). Each shard moves its UTXOs into sorted
write vectors, applies them to the same uncommitted database transaction and drops
the vectors before the next shard. The decoder retains format, shard membership,
UTXO count and total update-count checks. Distinct-key accounting uses a temporary
redb table, deleted before commit, preserving the previous return-count semantics
without a process-resident set containing the whole journal.

All base updates, journal removal and temporary-table removal still commit
atomically. A late malformed/missing record aborts earlier writes. Journal state
in memory is cleared only after commit. Existing reader versions retain their
previous coherent state. No persisted format or trust rule changes.

A new regression removes the second committed journal row, verifies that failed
materialization publishes no earlier-row base changes or temporary table, restores
the row and successfully retries. Existing journal tests cover restarts, ephemeral
outputs, replacements, canonical shards, delta groups and snapshot identity.

This is a redb application-allocation change, not closure of the measured MDBX
maintenance gate. The historical 2M/4096/5000/1GiB MDBX comparison still has a
256/64 RSS ratio around 3.39 and remains failed. Its owned transitions, undo,
folded indexes and engine dirty pages require combined admission or streaming.

Redb engine dirty pages and the transaction's temporary index can still grow
throughout materialization; atomicity does not imply bounded total RSS or disk.
Decoded shards are bounded by the existing format, not yet by a shared node
memory ledger. Additional B-tree updates can affect performance and temporary
disk space. No new measured RSS improvement, full-scale maintenance acceptance
or whole-node resource ceiling is claimed here.

Mac validation: 31 chain-store tests passed (20.65 seconds), then the full
all-feature library suite passed 984 tests (0 failed, 12 ignored; 69.50 seconds).
Strict all-target/all-feature Clippy and format/diff checks passed. No new
maintenance RSS benchmark or power-loss measurement was performed.
