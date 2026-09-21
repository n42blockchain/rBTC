# Archive piece hashing memory admission

File archive publication and every streaming piece-verification path now hash
unchanged 4 MiB transfer pieces through a 64 KiB buffer instead of allocating
one complete transfer piece. The path-bound node memory owner admits that buffer
before allocation. The buffer owns its lease, with the allocation dropped before
the lease. Unbound library paths retain their compatibility behavior.

Short reads accumulate into the same piece digest, interrupted reads retry, and
a final partial piece is hashed exactly once. Archive bytes and piece boundaries
remain unchanged. Admission denial remains a typed local resource error. A write
failure at this step occurs before destination creation/truncation, and unwinding
removes compression scratch and refunds its independent disk allowance.

The regression exercises one-byte pressure, simultaneous scratch owners, release
and retry, interrupted/short reads across a full piece plus a partial piece,
byte-identical publication, and existing-file preservation on denial. Existing
spool tests now allow 128 KiB memory so they still isolate disk pressure.

Validation: all-feature library tests: 1,054 passed, zero failed, 12 ignored
(72.39 s). Final focused regression: one passed (0.10 s). Strict all-target,
all-feature Clippy passed (12.85 s); formatting and diff checks passed.

This accounts only for the piece scratch allocation. Native zstd contexts and
workers, manifest parsing and returned metadata, record decoding and returned
batches still need admission and lifetime ownership. It does not establish a
32 GiB RSS ceiling, physical disk quota, or sustained whole-node acceptance.
