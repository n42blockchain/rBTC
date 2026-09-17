# MDBX mapped-page investigation and read-ahead policy (2026-09-17)

Overall RSS, actual maintenance-load and production gates remain **OPEN**.

The frozen b7991cb streaming measurement failed RSS at 1.53015 against 1.5. A
separate diagnostic used those exact frozen binaries for 1,024 transitions on
fresh 2M-UTXO stores, serial 64/256 lanes, while macOS vmmap sampled each process.
It is an instrumented diagnosis, not a repeat acceptance run or a replacement
for the failed result. Both diagnostic processes exited successfully; no database
was copied.

Samples show material resident mapped-file memory (last samples: 159.1 MiB in
64, 300.6 MiB in 256), alongside resident allocator zones (94.5 / 102.6 MiB).
These asynchronous samples are not synchronized to matching transaction heights
and are not peak attribution. They motivate investigating mapped-page behavior,
not a claim that their difference entirely explains the failed RSS ratio.

The pinned MDBX header documents automatic read-ahead decisions based on host
memory and an explicit NORDAHEAD flag. All rBTC MDBX environment opens now set
that flag instead of allowing host-RAM heuristics to populate speculative mapped
pages outside node resource planning. The common environment path also covers
compact-copy and rebase reopens. A read-only wrapper accessor verifies the actual
environment flag; tests check first open, sibling open and reopening an existing
store while retaining shared reservation/refund assertions.

This does not cap pages touched by demand reads, engine dirty pages, allocator
retention or whole-process RSS. Sequential scan throughput may change; final
source performance and sustained whole-node acceptance remain required. Dirty
page limits, geometry, atomic batch size, durability and compaction thresholds
are unchanged. Acceptance must measure the new source with the original 1.5
threshold; neither the flag nor vmmap observations count as a pass.

Validation before measurement: all-feature library suite 1,039 passed, 0 failed,
12 ignored (69.87 s); strict all-target/all-feature Clippy passed (23.83 s);
formatting and diff checks passed.
