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

## Frozen-source measured results

Frozen source for both new measurements:
`257bd32ad3f3316a77af2e0d0621c9c9ba8cdfe4`. Binaries and source hashes were frozen;
no concurrent local builds ran during measurement. The runner independently
reopened each lane and ran its compaction crash matrix. Both runners exited 0
with terminal state `measured`. They are not full-default or whole-node runs.

The first repeats the failed reduced streamed workload: 2M UTXOs, 4,096
transitions, 5,000 updates, 100,000 seed batch, 1 GiB geometry, 288 undo retention,
55% compact trigger, 10% minimum reclaim, 50% repeat growth, serial 64/256,
report interval 256, STREAM_INPUTS=1. Only the production read-ahead policy changed.

| Batch | Peak RSS bytes | Final checkpoint seconds | Automatic compactions |
| --- | ---: | ---: | ---: |
| 64 | 479,870,976 | 46.9148 | 0 |
| 256 | 705,314,816 | 51.7119 | 0 |

**1.4698009492 <= 1.5: numerical RSS criterion passed.** Both content digests match
the previous failed runs exactly:
`d1a9badf1e78d8bdd07324579437b95480db9034da539b610ff7bce43588acdb`.
This single near-threshold pass does not establish statistical attribution or
sustained headroom; the old 1.53015 failure remains preserved.

A separate run increased only the live set to **4M UTXOs**, retaining all other
settings, to exercise actual automatic compaction under the same policy:

| Batch | Peak RSS bytes | Final checkpoint seconds | Automatic compactions |
| --- | ---: | ---: | ---: |
| 64 | 1,115,963,392 | 51.1658 | 1 |
| 256 | 1,268,006,912 | 59.5785 | 2 |

**1.1362441825 <= 1.5: RSS criterion passed with actual maintenance.** Both content
digests and independent-process reopens agree:
`b70539db61ad7be1848418150ecaa1e3cca6c2996be04a2cdb4409f9c5b48bdb`.
This newly measured live-set size is distinct from the historical failed cases;
it does not retroactively turn them into passes. The default 160M-UTXO/900,000
transition scale, complete-node preparation/validation, shared mapped-page memory
ownership and sustained runs remain unverified. Overall production gates stay open.

Original artifacts/databases remain at
`/Users/jieliu/Documents/n42/rBTC-storage-no-readahead-20260917` and
`/Users/jieliu/Documents/n42/rBTC-storage-maintenance-4m-20260917`.
Small reports/hashes and diagnostic vmmap samples are retained under
`session-state/2026-09-17/mdbx-readahead`. No database was copied.

CI 35203118084/b7991cb (including the AS-map shutdown repair) completed successfully
on Windows, Linux with the 90% coverage gate, and supply-chain checks. The
read-ahead commit and prior report were pushed; CI 35204429046/257bd32 remains
in progress and is not yet passing evidence for this source.
