# Shared admission ledger concurrency check

Source tested: `3cf44ae68003ceb78e9f0c77042667b475091716`.
This is partial evidence; admission-resources remains open.

The standalone harness in
`target/current-acceptance-20260915/concurrent_budget.rs` imports the actual
`src/admission_resources.rs` without modifying it. It was compiled with
`rustc --edition=2024 --test`, using the existing thiserror dependency artifact.

Across 100 rounds, 16 simultaneous threads competed for 64 bytes of candidate
capacity in 16-byte reservations and a shared 100-unit non-refilling work burst
in 10-unit requests. Barriers held all successful leases until the observer
read the counters. Every round observed exactly 64 reserved bytes, 12 memory
deferrals, 100 charged work units, and six work deferrals. Joining the workers
left zero reserved bytes. A subsequent work request remained deferred, while
the released memory capacity could be reserved again.

All three tests passed: the new concurrent test and the source module's two
existing shared-ledger/unwind tests. This establishes the ledger's tested
concurrency behavior. It does not establish complete allocation accounting,
safe sizing across concurrent pool growth, or resumable candidate scheduling.
Those remain required for admission-resources acceptance.
