# Recovery of abandoned derived header indexes

New derived header scratch directories carry a versioned ownership marker and
an exclusive OS file lock. The lock is retained by the scratch owner shared
between the writer and all immutable read views, including standby overlays.
A stored PID is never used to decide whether an owner is alive.

Creating an index performs bounded collection under a parent coordination lock:
at most 4,096 directory entries are inspected and at most 64 abandoned indexes
are removed. Only a correctly marked, unlocked directory containing the known
regular files is eligible. Collection is nonrecursive and refuses symlinks,
Unix hardlinks, unknown files and old directories without ownership markers.
The permanent parent lock serializes creation, collection and normal cleanup,
including the interval before the new marker exists. On Unix the marker and
directory entries are synced before the index is created.

A real child-process test creates a disk index and stays alive. Collection in a
second process context preserves it; after the test forcibly terminates and
waits for the child, the next scratch creation removes the abandoned index.
Other tests retain live readers and unfamiliar directories and verify that
symlink targets are not modified. Final library, targeted and strict Clippy
results are recorded with the checkpoint logs. The first full run found a
non-atomic test readiness notification and a missing validation-cleanup
allowlist entry for the coordination lock; both were fixed before the final
run. The readiness file now publishes by rename, and a child guard ensures
failed tests also terminate and reap their subprocess. The failed log is kept.

Final Mac results: 970 passed, 0 failed, 12 ignored in 64.08 seconds; strict
all-feature/all-target Clippy, formatting and diff checks passed. The new
ignored entry is a subprocess helper exercised by the passing parent test.

This proves cleanup for the exercised process-termination path on the Mac. It
is not power-loss, Windows, full-node restart or sustained RSS/disk acceptance.
Older unmarked scratch directories are deliberately not deleted automatically.
Collection bounds do not impose a physical disk quota and a crowded parent can
require multiple collection passes. Shared node resource admission, total
startup memory and the other production acceptance gates remain open.
