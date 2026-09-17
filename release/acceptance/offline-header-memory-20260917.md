# Offline node header memory

Freezer reindex, peer reindex (source and final output), resumable output header
preparation and exclusive chain verification now use disk HeaderView/NodeHeaderState
rather than rebuilding full-history HeaderDag instances. Together with snapshot
lifecycle migration, production node.rs has no remaining load_dag calls or
HeaderDag construction. Legacy DAG APIs remain available to other library callers
and fixtures; this is not a repository-wide removal claim.

Reindex preparation replays output in bounded frames, checks the existing prefix
against the pinned source, then validates and persists each missing frame through
NodeHeaderState. Prefix comparison and source traversal charge shared Header work;
prefix scans yield every 2,000 entries. Existing side branches or divergent prefixes
remain errors. Exclusive chain verification reports the raw header count after
pending-promotion recovery, so its count matches the recovered view.

Mac validation: four existing reindex tests passed; the full all-feature library
suite passed 982 tests (0 failed, 12 ignored; 65.16 seconds). A subsequently added
2,001-header regression passed (5.85 seconds), covering a 2,000-header persisted
prefix, completion, repeated reopen and divergent-source rejection with raw count
unchanged. These small deterministic tests are not a large-history RSS acceptance.

This removes full-history in-memory header ownership at these node entry points.
It does not impose an aggregate node memory ceiling or close production gates.
Per-directory disk/cache allowances still need whole-node configuration and
admission alongside chainstate caches, engine/allocator overhead, execution,
network and mempool allocations. Large-fork scheduling, maintenance RSS and
sustained whole-node/public-network acceptance remain outstanding.

Final exclusive-verification regression passed after moving the reported count
after recovery (1.41 seconds); strict all-target/all-feature Clippy, format and
diff checks passed. Prior runtime-owner commit b84e224 passed all jobs in CI
35183210700, including Windows and Linux coverage; that is not CI evidence for
this later source revision.
