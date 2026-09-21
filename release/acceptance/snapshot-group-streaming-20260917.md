# Bounded snapshot group queries (2026-09-17)

Whole-node memory, resource scheduling and production acceptance: **OPEN**.

Snapshot-base coin queries previously widened a 192-byte probe fourfold until
the requested coin could be decoded, up to the largest txid group recorded in
the index. A lookup near the end of a large valid group therefore materialized
the entire group, despite returning one coin. The previously admitted 128-key
prefetch allowance could not bound this group-sized scratch allocation.

Queries now keep the 192-byte fast probe. Only a truncated probe falls back to
a positioned reader with a fixed 16 KiB buffer. Each preceding script is decoded
and dropped individually; existing snapshot decoding rejects scripts above
10,000 bytes before allocation. The reader is limited by the recorded maximum
group span and actual file length. Concurrent queries keep independent offsets.
Other parse failures return directly instead of repeatedly widening/re-reading.
Both snapshot-overlay engines already call these same base query methods.

A 64-coin group with 10,000-byte scripts verifies the final coin, missing vout,
duplicate batch keys and caller ordering. The probe-capacity assertion FAILS
against the previous implementation and PASSES with streaming. A shortened
recorded ceiling fails even though the missing bytes exist later in the file;
a damaged zero group count is rejected. Existing snapshot/index identity,
corruption, fingerprint and query tests also pass.

This bounds group query scratch, not the work required to scan a large group.
The authenticated count/span still permits lengthy searches; shared work
admission and resumable scheduling remain required. Snapshot index startup
MPHF decode and fingerprint sidecar allocations are not yet charged to the
node owner, and journal lookups/worker queues remain separate gaps. Final-source
throughput, RSS, maintenance and public long-node acceptance remain unproven.

Validation: all-feature library suite 1,022 passed, 0 failed, 12 ignored
(67.97 s). Strict all-target/all-feature Clippy passed (71 s including build
lock wait); format and diff checks passed. Focused index tests: 11 passed.
The old implementation failed the new capacity assertion (exit 101); its log
is retained alongside the passing results. No RSS or throughput measurement
was run, and no previous failed maintenance workload was restarted.
