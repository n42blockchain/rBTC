# Folded batch atomicity (2026-09-17)

Overall production/resource gates remain **OPEN**.

The default `ExecutionChainStore::commit_connect_folded` previously committed
empty coin changes for each preceding block, then applied the folded net coins
with the last block. A missing base coin discovered at that final step left a
committed tip and undo prefix. This affected ordinary redb and MDBX, including
write-back flushes targeting those stores. Snapshot overlay implementations
already override this method.

The default now constructs a single batch with per-block tips/undos and places
the net coins on the final transition. It delegates once to the atomic batch API.
Original per-transition coin lists remain ignored, matching the folded contract.
A failed final application therefore rolls back the complete publication.

Two real-engine regressions first failed on the previous implementation: after
a missing base spend, both engines had advanced to height 1 instead of remaining
at height 0. With the fix, they retain the original tip, leave neither block's
undo nor the new coin behind, and successfully retry the entire two-block batch.
Focused tests: 2 passed, 0 failed. The all-feature library suite passed
1,005 tests with 0 failures and 12 ignored tests (69.08 s). Strict
all-target/all-feature Clippy, formatting and diff checks also passed.

This compatibility path clones undo records and folded coins. It does not close
the allocation budget or disk-spooling gate; engine-specific bounded folded
consumption and producer reservations still need integration. No new RSS or
long-term whole-node acceptance is claimed.
