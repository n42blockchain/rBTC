# MDBX replacement gate

MDBX is the leading complete-chainstate replacement candidate, but this
revision does **not** select it as the daemon default. The implementation now
has the machinery needed to run the remaining scale gate without weakening
the atomic UTXO/undo/tip boundary or confusing synthetic churn with Bitcoin
validation.

## What the candidate improves — and what it costs

| evidence | MDBX improvement | MDBX cost or unresolved problem |
|---|---|---|
| Three-round 2M Mac complete-chainstate comparison | 89.01 vs 24.74 redb serving blocks/s (3.60x); 86.56 vs 32.15 IBD-256 (2.69x); 60–71% less allocated space | Warm in-memory scale; not mainnet churn or cold-disk evidence |
| Same Mac lookup workload | 3.149–3.300M lookups/s vs redb 2.057–2.134M (1.53–1.55x) | Working set fits 64 GiB; supplied cold runs put engines within 8% |
| Supplied 169,337,275-entry mainnet store | Complete MDBX store exists at production cardinality | 11.7 GB raw records occupied 50.26 GB live pages (4.3x) and a 76.01 GB file (6.5x); 25.75 GB/34% was free-page space |
| Supplied IBD batch comparison | MDBX reached 32,988 inserts/s, slightly above LevelDB's 30,663 | 800 MiB footprint vs LevelDB's 159 MiB; supplied 256-block peak RSS was about 35% above 64-block |
| New 20k deterministic smoke | Verified copy kept the exact four-table SHA; one copy reduced high-water 3.49→2.18 MB and allocated bytes 4.72→2.18 MB; freelist 1.29 MB→0 | Tiny memory-resident evidence only; copy pauses writes, scans all records for verification, and temporarily needs source plus live copy |
| New clean 64/256 runner smoke | 256 batching reached 1,415 vs 737 synthetic blocks/s; precompiled peak RSS was 64.5 vs 64.3 MiB | Too small to validate the production RSS ratio; full-scale run remains required |

The production-size copy-space implication is explicit: keeping the existing
76.01 GB file while writing approximately 50.26 GB of live pages requires
about 126.27 GB of simultaneous database files before filesystem overhead and
the operator reserve. `compact()` now preflights estimated live bytes plus 10%
(at least 64 MiB) and preserves another 16 GiB free. It fails before creating
the copy if that space is unavailable.

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
- The default 128 GiB policy first considers compaction at 55% high-water and
  requires 50% growth over the last post-copy size before repeating it. The
  supplied 76.01 GB file is about 55.3% of 128 GiB; its expected compacted live
  size is about 36.6%. The growth guard prevents a large irreducible live set
  from being copied every checkpoint. The post-copy baseline is persisted in
  a strict sidecar and survives restart; losing it can cause extra maintenance
  but cannot change chainstate contents.
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
2. High-water must trigger maintenance near 55%, remain below the emergency
   region, and not qualify again until at least 50% growth over the prior
   post-copy mark. The report must include compaction duration and simultaneous
   disk requirement; throughput alone is insufficient.
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
