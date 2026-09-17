# Streaming initial staged-prefix publication

The first publication of a partial staged segment previously decoded all source blocks into a vector before handing its selected prefix to the archive writer. `commit_staged` now streams that prefix into bounded, admitted compression scratch without retaining all source records.

The prefix writer verifies the complete source identity/framing/digest while counting selected record bytes, then makes a second identity-checked stream pass that feeds only selected records to the compressor. Both passes retain at most one block plus bounded scanner/codec buffers. The destination is not opened until source verification and compression complete, so callback-produced bytes cannot be published before the final source checksum is checked. The temporary compressed file uses the same shared logical disk admission and hard byte ceiling as ordinary archive writing.

Ordinary append and generated-prefix append share the existing publication implementation: slot-capacity/contiguity checks, archive sync, rename, directory sync, index update and cleanup remain in the same order. Existing staged-prefix and interrupted-publication recovery tests exercise this path.

A regression compares prefix bytes/manifest with the existing encoder, verifies source bytes are unchanged, rejects invalid prefix counts, corrupts the source suffix, and confirms failure leaves the previous destination intact with no scratch residue.

The already-published-prefix reconciliation branch still materializes source and retained records to compare them. That branch, native codec/manifest/read buffer admission, total physical disk inventory, local resource error classification and bounded node retry remain open. This change does not close total startup/RSS or full-scale/long-duration acceptance.

Validation: all-feature library suite 1,050 passed, 0 failed, 12 ignored (70.76 s); strict all-target/all-feature Clippy passed (8.03 s). The earlier staged-focused suite passed 13 tests (0.82 s). Formatting and diff checks passed.
