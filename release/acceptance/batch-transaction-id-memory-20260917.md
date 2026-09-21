# Execution transaction identifier admission

Batch execution without caller-supplied transaction identifiers previously allocated all computed identifiers outside the shared node allowance. `ComputedBatchTransactionIds` now reserves the outer vector and each exact-length identifier array before hashing or allocating. Checked arithmetic rejects estimate overflow for budget-bound stores. The payload precedes its lease in the owner, retaining the allowance throughout execution and releasing it after the arrays on success or error. Caller-supplied identifiers remain owned by their caller; this change does not duplicate their charge or claim that every caller is accounted.

A real budget-bound redb regression builds two arrays totaling 8,192 identifiers, compares every result against transaction hashing, exhausts the shared allowance and verifies rejection of a second allocation without a refund of live data. Dropping the first owner admits an identical retry, and dropping all owners returns usage to the engine baseline without moving the execution tip. Existing execution tests cover use of computed identifiers through commit, errors, and write-back.

Validation: focused regression passed (0.47 s); strict all-target/all-feature Clippy passed (15.38 s). Full all-feature library suite: 1 passed, 0 failed, 0 ignored (0.00 s). Formatting and diff checks passed.

This closes one explicit batch allocation gap, not the total memory gate. Worker preparation, deferred script prevouts/serialization, indexed retained undo, other batch metadata, mapped working sets and sustained whole-node RSS/disk evidence still require completion. Default node reservations remain 32 GiB. No RSS threshold or historical acceptance outcome changed.
