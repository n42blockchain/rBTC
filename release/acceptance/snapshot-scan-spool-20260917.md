# Snapshot scan memory and group spool (2026-09-17)

Full startup/RSS, physical disk and sustained production gates: **OPEN**.

Snapshot index scans previously kept every decoded script in a transaction group
until sorting its output indices. They now retain only sorted (vout, offset)
descriptors plus up to 64 KiB of serialized decoded coin bytes. Larger groups
spill into an anonymous temporary file beside the destination index. Each coin
is decoded once from the original snapshot; commitment hashing reads these
first-pass bytes rather than rereading a mutable source file. Duplicate output
indices are rejected before hashing the group.

Before decoding a group, the node owner admits its bounded descriptor array plus
128 KiB for the byte buffer, decoded script and codec scratch. Before spill file
creation/growth it reserves the maximum encoded group size from the same 16 GiB
logical temporary-byte ledger used by execution spools. The format ceiling is
111,111 coins per group, with at most 10,000 script bytes per coin. This disk
reservation is conservative: it covers the group maximum until its file closes.
Small groups never create a file or reserve disk bytes. Payloads/files drop
before their leases on success and error; private temporary data is not a
restart checkpoint and is discarded on close/process exit.

The complete location vector is now admitted before each capacity increase.
Replacement allowance overlaps the prior allocation's lease until allocation
succeeds, so a denied expansion leaves the existing records and charge intact.
The reservation follows the scanned result into MPHF construction and is released
only after the location vector drops. Metadata/scan readers are admitted too;
standalone fingerprint rebuilding now admits scanning and output arrays through
the index-path owner. Both rebase engines already pass their database owner for
external output paths.

Tests exercise in-memory and spilled unordered groups, compare the commitment
against independently generated numeric-vout records, and process 1,000 maximum
length scripts (about 10 MB) with less than 256 KiB of reserved group memory.
They verify disk-pressure refusal before file creation, successful retry after
pressure release, truncated spool errors, duplicate rejection, file cleanup and
memory/disk refunds. Location-growth denial preserves all old records/capacity
and charges; retry succeeds after releasing pressure. An actual 64-coin snapshot
index build spills under its bound owner, peaks below 256 KiB of reservations,
refunds construction resources and retains correct lookups and corruption checks.
These are ledger measurements, not process RSS measurements.

Location and MPHF tables still scale with the complete snapshot. Admission fails
closed on insufficient resources; a general disk-based table fallback and
resumable scheduling are not implemented. Publication files, filesystem metadata
and block allocation are not covered by the logical group-spool quota. Full
rebase materialization and other node startup/execution owners still need work.
Final-source performance and maintenance RSS acceptance remain required; no new
RSS or public soak run is claimed here.

Validation: all-feature library suite 1,038 passed, 0 failed, 12 ignored
(70.43 s); strict all-target/all-feature Clippy passed (25.62 s); fmt and diff
checks passed. Prior-source CI 35200356452/b5d80b7 completed with Windows and
supply-chain success but Linux coverage-run failure: embedded ZMQ test shutdown
exceeded its existing 3-second deadline at tests/embedded_node_api.rs:599. That
failure remains open; this scan change does not claim to repair it.
