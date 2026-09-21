# Header resource safeguards and measurements

Tested code: `5449b962c31a7f8375eafbccb34de3d4448c8d08`.
Status: partial implementation/evidence; header-resources is NOT accepted.

## Implemented safeguards

Default contextual batch staging checks an emergency ceiling of 2,000,000
non-genesis DAG entries before allocating the staging vectors. Exclusive DAG
ownership prevents concurrent reservation races. Both peer and local submitted
header paths use this entry point. ResourceDeferred is not peer-invalid; a
dropped stage restores entry capacity. This is an entry limit, not calibrated
byte accounting for all DAG indices, serving copies and database caches.

Default durable replay now checks the same count ceiling before materializing
the DAG. Explicit offline load_dag_with_limit remains available. Oversized
stores are refused without deleting data; they are not automatically compacted
or recovered. This can stop synchronization at the ceiling, including a valid
long fork. The disk-backed bounded recovery path is still required before the
complete resource gate can be accepted.

## Linux SSD workload

Host: 192.168.0.166, shared Linux host, default mimalloc allocator. The example
uses a 2,500-header active chain, then 1,000,000 valid regtest siblings in 500
batches of 2,000. TMPDIR points into /data/bench/rbtc-header-resources-20260916/
scratch, on the /data filesystem. Both runs exited zero and checked durable
reopen in the same process. Temporary databases are removed by the example
after verification; raw samples and binaries remain.

| Measurement | No eviction (`active`) | Idle eviction (`retained`) |
| --- | ---: | ---: |
| Final retained side entries | 1,000,000 | 50,000 |
| Maximum sampled database file bytes | 539,504,640 | 67,907,584 |
| Maximum sampled /proc RSS, KiB | 1,016,320 | 120,448 |
| Elapsed through same-process reopen, seconds | 19.404009 | 14.477005 |
| JSON samples | 502 | 1,002 |

The retained workload samples before and after each maintenance batch. At
capacity the temporary side count reaches 52,000 and returns to 50,000.
The input exceeds the retained target twentyfold. This finite burst workload
does not establish long-duration, concurrent-peer, deep-fork, cold-start or
whole-node RSS acceptance. File length is separate from physical write volume.
The two modes were run sequentially; timing is not isolated device performance.
Earlier system-temporary-directory runs are retained separately as active.jsonl
and retained.jsonl and are not the SSD rows above.

Remote raw evidence: `/data/bench/rbtc-header-resources-20260916/`.
Local copy: `target/current-acceptance-20260915/header-resources-20260916/`.

- Executable SHA-256:
  `ceb70ce5f23e2653c5dd1ce8cc8e4943e202a5d1db1d8169435d86edc5ecbb5b`
- retained-ssd.jsonl SHA-256:
  `6ff3418daacad21c49f60770746d9ffee67f8535c2ead263b04f5380347f86f9`
- active-ssd.jsonl SHA-256:
  `a8c58adbe326fbc1fb61d206543efc9f86a714858d6cf2accbaa81df95957272`

Eight header-store tests and seven header-resync tests passed; one explicit
resource probe remained ignored. Strict all-target/all-feature Clippy passed.
The full all-feature library suite then passed 937 tests, with zero failures
and 11 explicit ignores, in 74.05 seconds.

## Remaining work

Independent byte/work budgets, disk-backed candidate spill with resumable
consensus context, capacity-aware startup recovery, full-block reorg/failover
coverage, and long-running whole-node memory/disk measurements remain open.
The emergency count ceiling cannot substitute for that recovery architecture.

## Subsequent Mac continuation

See [batch budgets and Mac measurements](header-budget-continuation-20260916.md).
Its source and workload differ from the Linux measurements above; the original
results are unchanged and the production gate remains open.
