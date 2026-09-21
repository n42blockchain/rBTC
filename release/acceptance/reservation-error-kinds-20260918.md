# Typed reservation pressure at node boundaries

Shared memory and execution-spool exhaustion now carry distinct typed causes
inside io::Error. Existing diagnostic text is unchanged. Archive conversions
preserve both causes, and native encoder admission failures identify memory
pressure. The node preserves the exact pool through archive-ledger errors and
ChainStore execution-memory/spool errors as LocalBudget(kind).

All local kinds still stop the current peer session. Local resource failures
also return before peer failure bookkeeping can resolve a tried-collision probe
as unsuccessful or record a protocol violation. Ordinary filesystem errors,
corruption and matching error-message text are not classified as reservation
pressure. Untyped local resource errors remain local without an inferred pool.

Validation: full all-feature library suite passed (1,067 passed, zero failed,
12 ignored, 68.93 s). Strict locked all-target/all-feature Clippy and fuzz locked
all-target Clippy passed. Formatting and diff checks passed. After test-only
cleanup and extension, focused tests passed for typed archive/execution/native
propagation and real ledger staging under both exhausted pools. The latter
checks that no staged segment is published, temporary disk admission is
refunded, and staging succeeds after pressure is released. String lookalikes
and ordinary I/O errors do not obtain a typed budget classification.

This change supplies the failure distinction required by recovery; it does not
implement recovery. Automatic downshifting must guard the execution tip and
publication phase, release speculative work coherently, and reuse an existing
identity-bound stage instead of attempting to stage it again. A publication
failure may occur after the execution checkpoint committed: retrying the whole
batch would be wrong. Unknown errors and disk pressure must not be treated as
memory-driven batch retry. Finite retry, staged resumption, startup total memory,
physical disk, and long whole-node acceptance gates remain open.
