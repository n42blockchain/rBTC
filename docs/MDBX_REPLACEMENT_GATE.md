# MDBX replacement gate

The external full-mainnet replay assignment, including exact transaction-rate
definitions, comparable btcdmdbx revisions, evidence retention, and rules for
avoiding a 771 GB corpus copy on the Mac, is maintained in
[`BTCDMDBX_FULL_REPLAY_TASK.md`](BTCDMDBX_FULL_REPLAY_TASK.md).

MDBX is the leading complete-chainstate replacement candidate, but this
revision does **not** select it as the daemon default. The implementation now
has the machinery needed to run the remaining scale gate without weakening
the atomic UTXO/undo/tip boundary or confusing synthetic churn with Bitcoin
validation.

The corrected external figures below come from btcdmdbx revision `56cc436d`
(`docs/storage_engine_findings_correction.md`). They supersede the sampled
raw-record estimate previously copied into this document.

## What the candidate improves — and what it costs

| evidence | MDBX improvement | MDBX cost or unresolved problem |
|---|---|---|
| Three-round 2M Mac complete-chainstate comparison | 89.01 vs 24.74 redb serving blocks/s (3.60x); 86.56 vs 32.15 IBD-256 (2.69x); 60–71% less allocated space | Warm in-memory scale; not mainnet churn or cold-disk evidence |
| Same Mac lookup workload | 3.149–3.300M lookups/s vs redb 2.057–2.134M (1.53–1.55x) | Working set fits 64 GiB; supplied cold runs put engines within 8% |
| Corrected full scan of the supplied 169,337,275-entry, pruned-undo store | 33.42 GB raw records occupied 50.26 GB live pages (1.50x), not the previously reported 4.3x | The 76.01 GB file was still 2.27x raw and contained 25.75 GB/34% free pages after long-running delete churn; this is not a stock-btcd size comparison |
| Stock btcd MDBX replay, 951,225 blocks | 106.08 GB raw occupied 122.52 GB live pages (1.155x) with approximately zero free-page overhead; it completed the full replay | Stock btcd retains all spend journals, unlike rBTC's bounded undo policy, so its absolute size is an upper-semantics comparison rather than rBTC's expected footprint |
| Completed btcd Pebble/MDBX replay | 200k writes were effectively tied (5,776 vs 5,653 blocks/s); MDBX loaded a 200k-block chain state in 728 ms and completed 951,225 blocks | Pebble was marginally smaller at the same ~830k height, but its 56,000 SST files made an 828,851-block chain-state load exceed one hour; the different load heights prohibit quoting an exact speedup ratio |
| Supplied IBD batch comparison | MDBX reached 32,988 inserts/s, slightly above LevelDB's 30,663 | 800 MiB footprint vs LevelDB's 159 MiB; supplied 256-block peak RSS was about 35% above 64-block |
| New 20k deterministic smoke | Verified copy kept the exact four-table SHA; one copy reduced high-water 3.49→2.18 MB and allocated bytes 4.72→2.18 MB; freelist 1.29 MB→0 | Tiny memory-resident evidence only; copy pauses writes, scans all records for verification, and temporarily needs source plus live copy |
| New clean 64/256 runner smoke | 256 batching reached 1,415 vs 737 synthetic blocks/s; precompiled peak RSS was 64.5 vs 64.3 MiB | Too small to validate the production RSS ratio; full-scale run remains required |

The production-size copy-space implication is explicit: keeping the existing
76.01 GB file while writing approximately 50.26 GB of live pages requires
about 126.27 GB of simultaneous database files before filesystem overhead and
the operator reserve. `compact()` now preflights estimated live bytes plus 10%
(at least 64 MiB) and preserves another 16 GiB free. It fails before creating
the copy if that space is unavailable. Those physical copy numbers are
unchanged by the corrected raw-record scan; only the earlier amplification
ratios were wrong.

## Safety work now implemented

- `audit()` hashes table names and every ordered key/value in `utxo_hot`,
  `utxo_cold`, `undo`, and `meta`, while reporting raw records, live pages,
  freelist pages, logical file length, allocated filesystem bytes, counts, and
  execution tip.
- Compact-copy audits source and destination before either rename. A durable
  manifest carries the expected four-table identity through the swap. Reopen
  accepts the old environment or a fully verified compact copy; a mismatched
  manifest fails closed and preserves the old directory.
- A subprocess test exits without destructors after the verified copy, after
  each rename, and after each parent-directory sync. All five cases reopen to
  the same tip, content SHA, non-empty undo, and UTXO counts.
- The default 128 GiB policy first considers compaction at 55% high-water,
  requires at least 10% of high-water bytes on the freelist, and requires 50%
  growth over the last post-copy size before repeating it. The
  supplied 76.01 GB file is about 55.3% of 128 GiB; its expected compacted live
  size is about 36.6%. The growth guard prevents a large irreducible live set
  from being copied every checkpoint. The post-copy baseline is persisted in
  a strict sidecar and survives restart; losing it can cause extra maintenance
  but cannot change chainstate contents.
- The corrected btcd evidence changes the interpretation, not the need for
  this maintenance path. A sequential full-undo replay had 1.155x live
  amplification and essentially no freelist, while the long-running
  pruned-undo store had a 34% freelist. Compact-copy therefore addresses
  lifecycle churn; it is not evidence that MDBX intrinsically consumes 4.3x
  live space.
- btcdmdbx also found that Go write transactions deadlock if a goroutine moves
  OS threads between begin and commit. That adapter now pins the goroutine.
  rBTC does not use that Go adapter: its vendored Rust binding begins and
  commits every write transaction on one dedicated transaction-manager thread
  and opens the environment with `MDBX_NOTLS` for reads. The finding therefore
  validates a binding invariant rather than requiring an rBTC thread-pin patch.
- MDBX now implements authenticated undo-window pruning. Every undo hash is
  resolved through the header DAG before one atomic delete transaction.
- The ignored scale driver is persistent and resumable. It defaults to 160M
  live P2PKH coins, 900,000 deterministic churn transitions, 5,000
  spend/create pairs per transition, 256-transition commits, 288 retained undo
  rows, a 128 GiB ceiling, and periodic physical metrics. It never calls these
  synthetic transitions mainnet blocks.
- Root data-format schema 4 names the chainstate backend. A schema-3 redb
  directory migrates to `chainstate_backend: "redb"`; the current node rejects
  an `mdbx` manifest without rewriting it. This establishes a fail-closed
  rollback boundary before the future content migrator publishes MDBX.

## Run the full 64/256 gate

Use a dedicated volume. The output directory must not exist; the runner keeps
both databases, reports, logs, host identity, filesystem snapshots, revision,
dirty state, and a combined report hash. It prebuilds outside the timed lanes
so compilation cannot pollute peak RSS.

```sh
RBTC_MDBX_GATE_UTXOS=160000000 \
RBTC_MDBX_GATE_BLOCKS=900000 \
RBTC_MDBX_GATE_UPDATES=5000 \
RBTC_MDBX_GATE_UNDO_RETENTION=288 \
RBTC_MDBX_GATE_CAPACITY_BYTES=137438953472 \
RBTC_MDBX_GATE_COMPACT=1 \
RBTC_MDBX_GATE_MIN_RECLAIM_PERCENT=10 \
contrib/run_mdbx_replacement_gate.sh /dedicated-volume/rbtc-mdbx-gate
```

An interrupted individual lane can be resumed directly against its existing
database. Keep every workload variable identical and raise only the target
height when intentionally extending the run:

```sh
RBTC_MDBX_GATE_DIR=/dedicated-volume/rbtc-mdbx-gate/batch-256/chainstate.mdbx \
RBTC_MDBX_GATE_REPORT=/dedicated-volume/rbtc-mdbx-gate/batch-256/resume.json \
RBTC_MDBX_GATE_UTXOS=160000000 \
RBTC_MDBX_GATE_BLOCKS=900000 \
RBTC_MDBX_GATE_UPDATES=5000 \
RBTC_MDBX_GATE_COMMIT_BATCH=256 \
cargo test --release --all-features --test mdbx_mainnet_scale_gate \
  -- --ignored --nocapture
```

Run the actual abrupt-process recovery matrix independently:

```sh
cargo test --release --all-features --test mdbx_compaction_crash -- --nocapture
```

## Acceptance criteria before selecting MDBX

1. The full synthetic run reaches 160M live coins and 900,000 transitions in
   both 64/256 lanes with no `MDBX_MAP_FULL`, content mismatch, unbounded undo,
   or immediate repeated compaction. Every post-copy audit must preserve the
   pre-copy SHA and clear the freelist represented in the copy.
2. High-water must trigger maintenance near 55% only when at least 10% is
   reclaimable freelist space, remain below the emergency region, and not
   qualify again until at least 50% growth over the prior post-copy mark. The
   report must include compaction duration and simultaneous disk requirement;
   throughput alone is insufficient.
3. Peak RSS for 64 and 256 must be captured from prebuilt binaries. A 256/64
   ratio above 1.5 requires reducing the default IBD batch or an explicit
   memory-budget design; the supplied observation was already about 1.35.
4. A common immutable real mainnet block corpus must still be replayed through
   redb and MDBX with identical cache, validation, retention, batch, and start
   state. Final tip and canonical UTXO identity must match. That corpus is not
   on this Mac, so this remains an external gate.
5. The explicit backend manifest is complete. The daemon must still gain an
   authenticated content migration/publish path and cover AssumeUTXO metadata,
   consensus binding, background validation, offline repair, observability,
   and operational rollback. The current candidate implements the
   UTXO/undo/tip trait but is not yet a drop-in replacement for every redb-only
   node surface.

Only items 1–5 together authorize changing the default. The new recovery and
maintenance code closes capability gaps; it does not manufacture the missing
160M and real-block evidence.
