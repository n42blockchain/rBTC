# Snapshot index startup memory ownership (2026-09-17)

Total startup memory, physical disk, RSS and production acceptance: **OPEN**.

Opening a node-bound snapshot index now reserves shared memory before its
reader buffers, encoded MPHF section, decoded word/rank tables and index
verification window are allocated. The temporary allowance covers three times
the encoded hash section plus fixed verification/descriptor headroom. After
verification and temporary-buffer destruction, it shrinks to the retained
word/rank/level vector capacities. The index owns that lease after its payload
fields, so error unwinding and final drop release memory in the correct order.

Fingerprint sidecars are checked against the expected on-disk size before
reading. Their encoded and decoded copies obtain a separate allowance before
allocation; a fixed-length read plus one-byte EOF probe cannot expand when the
file grows after the metadata check. Valid decoded fingerprints retain their
own lease. Wrong-sized or corrupt optional hints are ignored and refunded;
insufficient memory is a local open error rather than an uncharged allocation.

Both snapshot-overlay engines pass the node owner of their database path to
the base index explicitly. Base/index files outside the node directory therefore
use the same budget. Rebase opens the new index under that owner while the old
index remains charged; failed admission occurs before the durable identity
switch, preserves the old tip/base, and can be retried after headroom returns.

The new failure-path test exposed an orphan fingerprint sidecar after rebase
admission failure. Its cleanup assertion failed before the fix. Rebase now
checks for a pre-existing sidecar and tracks both newly generated index and
sidecar paths before invoking the builder. Failures before the identity switch
clean both, while successful/durably uncertain switches keep referenced files.
Existing sidecars are rejected without being overwritten or deleted.

Tests cover concurrent live index owners, budget exhaustion, refunds after a
late digest mismatch, exact retained capacities, optional corrupt/oversized
sidecars, and external-base admission before either engine creates a database.
Both engines exercise denied rebase, old-tip preservation, file cleanup, retry,
compaction and final reservation release using actual databases and snapshots.

Limits: snapshot/index BUILD and rebase materialization allocations are still
outside this new open-time admission. Caller-created MTP tables, startup peers
and other collections, journal query plans/historical matches, execution worker
copies and script queues need further integration. Reservation bytes do not
bound allocator/stack/engine page residency or process RSS. No new maintenance
RSS run, throughput acceptance, public catch-up or long soak is claimed.

Validation: final all-feature library suite 1,025 passed, 0 failed, 12 ignored
(71.71 s). Strict all-target/all-feature Clippy passed (16.45 s); formatting and
diff checks passed. The new rebase sidecar-cleanup assertion failed before
cleanup was fixed; both engine rebase/retry paths pass in the final suite.
Prior-head CI 35195831874 (40b629f) completed with Linux/coverage and supply-chain
success, but Windows failed two Header recovery tests (20-second timeout and
unexpected peer-message shape/early EOF). Logs are retained; these failures
are not claimed fixed by snapshot index memory admission.
