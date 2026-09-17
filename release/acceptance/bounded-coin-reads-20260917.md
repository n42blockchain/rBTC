# Bounded execution coin reads and 32 GiB default (2026-09-17)

Overall startup memory, RSS, disk and long-node acceptance gates: **OPEN**.

The user-selected default shared node memory reservation budget is now 32 GiB
(34,359,738,368 bytes), with the existing configuration/CLI override retained.
The separate execution spool logical disk allowance remains 16 GiB. This does
not raise individual cache defaults or establish an operating-system RSS cap.

Node-bound ordinary Redb/MDBX point and batch coin queries, and mutable
snapshot-overlay queries for both engines, reject decoded scripts over 10,000
bytes before copying script payloads. Legacy and compact codecs keep their
unrestricted compatibility entry points for unbound library consumers. Compact
raw lengths and compressed script forms are checked before allocation.

MDBX read-only queries borrow raw record bytes through the transaction instead
of first cloning arbitrary records into a Vec. Batch hot hits skip cold reads.
Creation-MTP metadata decodes into a fixed four-byte value, checking length
before copying. Prefetch read failures are explicit local execution errors;
they do not classify a peer's block as consensus-invalid.

Regression coverage includes oversized legacy/raw compact data, truncated raw
compact data with an excessive declared length, compressed script length
limits, existing real Redb refresh preservation, and bound MDBX/both snapshot
overlay point and batch reads before and after compaction. Configuration tests
check the 32 GiB API/CLI default and retain explicit smaller overrides.

Immutable snapshot-base lookups, journal decoding, scans and mutation paths
still need review. Engine page reads/mmap residency, worker copies, script
queues, indexed undo retention, thread stacks and allocator overhead are not
made fully bounded by this change. Startup preflight remains a known-cache
reservation plan rather than a measured whole-node memory ceiling. No new RSS,
physical disk, public catch-up or long soak acceptance is claimed.

Validation: final all-feature library suite passed (1,021 passed, 0 failed,
12 ignored; 68.41 s). Strict all-target/all-feature Clippy passed (36.74 s),
format and diff checks passed. An earlier newly added CLI fixture omitted the
required peer source and failed; adding the loopback peer argument fixed that
test, with the final complete suite passing. Prior-head CI 35194592689 finished
successfully; it is not evidence for this new source.
