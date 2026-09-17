# Archive scan and visitor scratch admission

File record verification now admits its 64 KiB scratch before allocation, while
the native decoder allowance is already held. Selected-record scans do the same
for skipped-record scratch. Visitor scans additionally reserve each selected
record's payload length before allocating its callback buffer. The callback
borrows that buffer; local declaration/drop order frees bytes before refunding
the reservation. Existing streaming integrity and callback-stop semantics remain.

Regression covers scratch denial with enough budget for the native decoder,
first callback payload denied by one byte (zero callbacks), callback-visible
aggregate charge, release/retry, early stop followed by full verification of a
larger skipped suffix, refund, and unchanged source bytes. The previous native
ownership test now includes its separately admitted scan and visitor buffers.

Validation: all-feature library tests passed: 1,056 passed, zero failed,
12 ignored (72.63 s). Strict all-target/all-feature Clippy passed (17.05 s).
Formatting and diff checks passed.

Returned batch Vec<Vec<u8>> payloads remain an open ownership gap: they cannot be
covered by a function-local lease. LedgerBlockBatch exposes these vectors and
currently derives deep Clone, and replay prevalidation transfers their bytes
into PrevalidatedBlock then the staging batch. Completion requires an owning
representation that follows those transfers and handles clones without detached
unadmitted bytes. Single-block compatibility reads, block-hash decoding objects,
manifest buffers/metadata, multithreaded compression and whole-node acceptance
also remain open. This change does not claim the 32 GiB total RSS gate is closed.
