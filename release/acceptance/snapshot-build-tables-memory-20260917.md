# Snapshot build array admission (2026-09-17)

Startup/build total memory, RSS, disk and production acceptance: **OPEN**.

Node-bound MPHF builds now reserve before allocating the complete remaining-key
ordinal array, the bounded level descriptor array, and each level's hit/collision
bitmaps and rank samples. Retained level payloads carry their own reservations;
ordinal scratch is released after construction. The first-hit bitmap is reused
for final level words, eliminating the former third simultaneous bitmap.
Overflowing allocation sizes return errors before allocation or key access.

The packed offset table, fingerprint table, occupied bitmap and 128 KiB allowance
for publication scratch are admitted together before allocation. This allowance
remains until publication finishes. Both redb and MDBX rebase callers explicitly
pass their database owner even when new snapshot paths are outside its directory;
the existing base keeps its reservations throughout. Public identity builds
resolve the owner from the index path, matching index-open behavior.

Tests deny both initial ordinal admission and subsequent level scratch admission
before key access, verify retained bytes against actual vector capacities, keep
live results charged under a competing build, and verify refunds on duplicate
keys and oversized inputs. A real snapshot test denies table admission while an
old index remains live, verifies no output publication and exact refund, releases
pressure and retries, and compares the resulting index bytes with the original.
Existing rebase tests exercise external paths, rollback, retry and compaction.

The scanner's complete group-location vector and whole decoded coin groups remain
unadmitted. Rebase materialization, standalone fingerprint construction and other
startup owners still need work. Resource denial fails closed and does not yet
provide resumable scheduling or a disk fallback for arbitrarily large valid
builds. Logical reservations do not cap allocator overhead, RSS or physical disk.
No new maintenance RSS measurement or public-network soak is claimed.

Validation: all-feature library tests 1,035 passed, 0 failed, 12 ignored
(74.29 s), including both new admission regressions and existing overlay rebase
tests. Strict all-target/all-feature Clippy passed (24.46 s) after an equivalent
checked-conversion cleanup. Formatting and diff checks passed.
