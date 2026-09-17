# MDBX source mapping lifetime during compaction (2026-09-17)

Total node memory, full-scale and sustained production gates: **OPEN**.

Compaction previously kept the source environment mapped while opening and
scanning the copied environment to validate all four tables and their digest.
The source mapping now closes immediately after copy creation, before the copied
engine opens. Its canonical directory remains untouched until verification,
manifest publication and synchronization succeed. The original environment's
reservation stays pinned for error recovery and final reopen; this is a mapping
lifetime reduction, not early release of a still-needed node allowance.

Copy-open, schema, content or pre-publication write/sync failures reopen the
unchanged source before returning an error, remove the failed private candidate
and synchronize the parent directory. If recovery/cleanup I/O itself fails, the
error still propagates. The atomic directory-promotion and rollback protocol is
unchanged after a verified copy is durable. No unchecked copy is promoted.

A new SourceClosed crash boundary exercises abrupt exit before copy verification;
the canonical source remains the recovery authority. The subprocess matrix now
covers six boundaries. Fault injection separately blocks the copy directory and
mutates a well-formed copied database, verifies the source digest/undo and shared
allowance after failure, checks candidate cleanup, writes new data, retries
compaction and verifies a subsequent independent reopen.

The read-ahead status accessor also needed a portability correction: Windows
bindgen generates its flag constant as i32 while mi_mode is u32. Both operands
now convert losslessly to i64. CI 35204429046/257bd32 failed Windows compilation
at that expression; its log is retained. Mac flag tests passed, but final-source
Windows CI is still required and not inferred from Mac execution.

Mapped demand pages and allocator/engine overhead still need total-node resource
ownership and full-scale acceptance. The prior 2M and 4M maintenance passes belong
to 257bd32; any new RSS result must identify the new frozen source.

Final local validation: all-feature library suite 1,040 passed, 0 failed,
12 ignored (67.03 s); strict all-target/all-feature Clippy passed (18.12 s).
The six-boundary subprocess crash matrix passed (1.55 s), with its worker ignored
as a standalone test. Formatting and diff checks passed.

## Frozen-source maintenance measurement

Source `9907c8e1e101b0f1086662ed4d2224ec255053fd`; frozen binaries and source hashes,
Mac, no concurrent local compilation. The same 4M-UTXO maintenance workload as
257bd32 was used: 4,096 transitions, 5,000 updates, seed 100,000, 1 GiB geometry,
288 undo retention, compact 55%, minimum reclaim 10%, repeat growth 50%, serial
64/256, reports every 256, STREAM_INPUTS=1.

| Batch | Peak RSS bytes | Final checkpoint seconds | Automatic compactions |
| --- | ---: | ---: | ---: |
| 64 | 652,509,184 | 48.9844 | 1 |
| 256 | 846,053,376 | 55.5364 | 2 |

**1.2966152765 <= 1.5: this maintenance RSS criterion passed.** Both peaks are
below the prior source's 1,115,963,392 / 1,268,006,912 bytes. The ratio increased
because both absolute peaks fell by different amounts; neither threshold nor
batch size was changed. The runner exited 0 with state measured. Independent
reopens and the six-boundary compaction crash matrix passed. Both content hashes
match each other and the prior 4M workload:
`b70539db61ad7be1848418150ecaa1e3cca6c2996be04a2cdb4409f9c5b48bdb`.

Artifacts/databases remain at
`/Users/jieliu/Documents/n42/rBTC-storage-copy-lifetime-4m-20260917`; small evidence
is copied under session-state/2026-09-17/mdbx-copy-lifetime. Old results retain
their original source identities and statuses. This is not full 160M/900,000
scale, a complete-node reservation proof or long-duration acceptance.

Prior CI 35204429046/257bd32 finished with Linux (including 90% coverage) and
supply-chain success, and the documented Windows flag-type compile failure.
The normalization commit d39ea9c is included in 9907c8e; a new CI run must verify
that platform rather than treating Mac tests as Windows evidence.
