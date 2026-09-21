# Native archive decoder memory admission

All three file-streaming decoder constructors (selected records/visitors, complete
record verification, and consensus block-hash verification) now reserve from the
path-bound node memory owner before creating either the native context or Rust
input buffer. ArchiveDecoder owns both decoder and reservation, in that drop
order, including callback execution, read errors, and early returns.

The allowance is the linked zstd implementation's
`ZSTD_estimateDStreamSize(1 << enforced_window_log)` plus
`zstd_safe::DCtx::in_size()` for the Rust BufReader. The enforced window range
remains 23–27. This covers the native context and streaming input/output memory
for dictionary-free decoding, including concatenated frames under the same
window limit. No dictionaries are loaded. The codec rejects oversized windows.

A small local GPL-3.0-only dependency, rbtc-codec-memory, provides one safe,
allocation-free wrapper around scalar native size/error queries. Its exact
zstd-sys 2.1.0 dependency unifies with the existing locked codec; no registry
package version changes. This isolates the FFI from the application's unsafe-code
prohibition. zstd-sys experimental bindings expose the estimator. The pinned
zstd 1.5.7 source documents the estimate in lib/zstd.h and computes context +
input block + bounded decoding buffer in lib/decompress/zstd_decompress.c.
That implementation frees previous streaming buffers before allocating their
replacement, so concatenated frames do not require two native buffer allowances.

Validation: all-feature library tests passed (1,055 passed, zero failed, 12
ignored, 74.80 s). Strict all-target/all-feature Clippy and separate wrapper
Clippy passed; cargo-deny advisories/bans/licenses/sources all passed. Formatting
and diff checks passed. Regression covers one-byte pressure on all
three file paths, retained reservation inside a real visitor callback, native
read failure, exact refund and retry. Disk-focused tests now allow 16 MiB memory
so their small-file decoder admission does not obscure disk pressure.

Remaining gates: multithreaded encoding has no supported native stream estimator
(the linked API explicitly limits compression estimates to single-threaded use).
Manifest decoding, returned metadata/batches, record scratch and consensus
objects need their own admission/lifetime owners. In-memory compatibility decoder
has no node path and remains unbound. Native allocation sizes are not allocator
RSS or a measured whole-node 32 GiB ceiling. Physical disk and sustained
whole-node/final-source acceptance remain open.
