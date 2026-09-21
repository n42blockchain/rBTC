# Native archive encoder memory admission

Node-bound full-file and prefix writers now use a safe local streaming encoder
whose C allocation callbacks reserve from the same node MemoryBudget before
malloc. Native contexts, worker pools, job buffers and their replacements remain
charged until custom free; the fixed 4,096-entry tracking table and Rust output
buffer are separately admitted before allocation. Context destruction joins
workers before releasing callback state. The root crate still forbids unsafe
code; FFI is isolated in rbtc-codec-memory and pins the patched codec by path.

Allocation denial maps to ArchiveError::ResourceBudget and therefore preserves
the existing node local-resource classification. It does not add automatic
retry. File destinations are created only after successful compression and
validation. Compression level, worker selection, framing and durable ledger
publication ordering remain unchanged. Unbound in-memory compatibility encoding
continues to use the ordinary encoder.

Fault injection exposed a SIGSEGV in pinned zstd's custom calloc (memset on NULL)
and missing custom-free callbacks during partial pool initialization. The
vendored 2.1.0+zstd.1.5.7 package fixes those paths and missing-job-table cleanup.
See vendor/zstd-sys/RBTC_PATCHES.md for provenance and every local patch. Both
root and fuzz lockfiles use this source; system-zstd overrides are rejected.
Historical crash/failure logs are retained, not relabeled as successful runs.

Validation on this Mac:

- Full all-feature library suite: 1,066 passed, zero failed, 12 ignored, 72.88 s.
- Strict locked all-target/all-feature Clippy: passed, 37.45 s.
- Fuzz locked all-target Clippy: passed, 15.39 s.
- Separate wrapper Clippy against patched codec: passed, 9.64 s.
- cargo-deny: advisories, bans, licenses and sources passed.
- Focused native tests: two passed, 0.16 s. They force single-thread and 2/3/4
  workers, compare exact compressed bytes, decode the complete output, inject
  each observed allocation failure and verify zero retained reservations.
  They also cover writer failure and early drop with active workers.
- Full-suite file tests cover denial after metadata admission, unchanged
  destination, anonymous scratch cleanup/disk refund, then successful retry.

The test matrix covers allocation positions observed for this workload; it is
not an exhaustive proof over all codec inputs or OS failures. OS thread stacks,
allocator overhead and synchronization internals remain outside this accounting.
Node metadata, new network serialization, legacy serving copies, pressure
resumption, startup total RSS, whole-node physical disk and sustained/final-source
acceptance remain open. The default 32 GiB reservation budget is not an RSS cap.
