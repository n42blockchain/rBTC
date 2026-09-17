# Single-traversal staged validation

The first bounded staged recovery implementation rescanned and decompressed the whole archive for each 16-block batch. Startup/reindex recovery now uses one identity-checked visitor over the record stream, independent of the number of prefix blocks. Compressed piece verification remains a separate sequential pass. The preceding manifest inspection still fully verifies the archive once; total verification passes are fixed rather than proportional to the number of batches.

Both bounded batch selection and prefix visiting use the same archive scanner. The visitor checks the expected complete manifest before callbacks, holds at most one selected block payload (at most 4,000,000 bytes), and uses fixed scratch for omitted records. It does not accumulate visited payloads. The ledger holds its lock through traversal. Node callbacks validate only; `commit_staged` is called only after final framing, record length and full-record digest verification succeeds.

A false callback stops further callbacks but continues integrity verification through the suffix. The node preserves callback errors after successful integrity verification; archive corruption remains an error even if a callback already stopped. Regression coverage checks ordered visiting, identity mismatch before any callback, and a deliberately incorrect full-record digest rejected after an early-stop callback despite valid compressed-piece checksums. Existing node tests cover early fork/error behavior and unpublished state.

The per-batch repeated-verification issue is resolved by this change. This is not a global work/RSS admission claim: manifest/piece/zstd buffers still require shared accounting, partial-prefix publication still uses whole-archive reads, and node memory-pressure downshift/retry remains pending. Full-scale and long-running acceptance are unchanged and remain open.

Validation: all-feature library suite 1,048 passed, 0 failed, 12 ignored (87.30 s), including the extended identity/late-digest regression and startup visitor tests. Strict all-target/all-feature Clippy passed (10.22 s); formatting and diff checks passed.
