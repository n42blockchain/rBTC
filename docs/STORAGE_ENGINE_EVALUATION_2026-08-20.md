# Storage-engine re-evaluation — 2026-08-20

## Decision

Keep redb as the daemon default for this revision. MDBX is now the leading
replacement candidate, not merely an overlay experiment, but it is not ready
to become the default until a mainnet-scale churn and crash-recovery gate has
covered the complete four-table chainstate and its compact-copy swap. Do not
replace redb with Go LevelDB on the strength of this benchmark.

The reason is two-sided. On this Mac, the complete MDBX chainstate is 2.69–3.60
times faster to mutate than the current complete redb chainstate, with
1.53–1.55 times the warm batch-read throughput and 60–71% less allocated
space. Against the storage-only btcd-codec/Go-LevelDB lane, MDBX is 5.68–12.34
times faster to mutate and ends within 0.8% of its post-compaction allocation.
However, the supplied mainnet measurement shows a long-running, high-delete
MDBX chainstate growing to 4.3 times its live bytes and 6.5 times its raw data
on disk before compact copy. The new compact-copy path removes the former
capability blocker; it does not remove the need to prove its operating policy
at 160M+ live coins.

## What was compared

The repository now has two deliberately separate reproducible lanes:

- `tests/storage_engine_comparison.rs` drives the production rBTC redb and
  MDBX chainstores through the same complete state transition: UTXOs,
  transaction-grouped per-block undo, and execution tip. Serving mode commits
  and flushes every block. IBD mode folds exactly 256 blocks into one durable
  transaction. This compares deployable physical designs, so it includes the
  current redb legacy row format versus the new compact MDBX format; it is not
  a codec-normalized engine microbenchmark.
- `contrib/btcd_storage_bench` pins the Go LevelDB revision used by btcd
  v0.26.2 and mirrors btcd's outpoint key, MSB-VLQ header/amount, compressed
  P2PKH coin, no-compression option, and Bloom-10 filter. It applies the same
  logical UTXO/undo/tip workload with `Sync=true`. It deliberately excludes
  btcd's UTXO cache, immutable-treap write cache, block files, block index,
  scripts, and validation. It is therefore labelled “btcd codec + pinned Go
  LevelDB”, never “btcd IBD”.

Official btcd does **not** use rBTC's proposed fixed 36-byte physical key. At
btcd commit `05585e037ba0690572208dbc46d121a49cc0c4c9`, `outpointKey` is the
32-byte wire-order txid followed by an MSB-VLQ vout, hence 33–37 bytes. That
VLQ is compact but is not globally bytewise-monotonic when its encoded length
grows (for example, vout 16,512 encodes `808000`, which sorts before vout
16,384's `ff00`). The measured rBTC format therefore uses a one-byte width tag
plus minimal big-endian vout: 34–37 bytes, true numeric byte order, and direct
cursor range boundaries. It is neither fixed-width nor byte-for-byte btcd.

Every lookup result below is warm: the 2M-record datasets fit comfortably in
64 GiB and the page cache was not purged. The benchmark opens one read view and
one table set per 4,096 caller-ordered requests, with 75% hits and 25% misses.
No cold-read claim is made.

## Controlled Mac results

Host: MacBookPro18,2, Apple M1 Max (10 cores), 64 GiB RAM, internal Apple
APFS SSD, macOS 26.5.1. Toolchains: rustc 1.85.0 and Go 1.26.5. Workload:
2,000,000 live P2PKH UTXOs, 256 blocks, 5,000 spends plus 5,000 creates per
block, and 500,000 lookups. Each number is the median of three fresh-database
rounds. The Rust backend order was redb→MDBX, MDBX→redb, redb→MDBX to expose
fixed-order bias.

| complete chainstate / storage lane | serving blocks/s | IBD-256 blocks/s | serving warm lookups/s | IBD warm lookups/s |
|---|---:|---:|---:|---:|
| rBTC redb | 24.74 | 32.15 | 2.057M | 2.134M |
| rBTC MDBX | **89.01** | **86.56** | **3.149M** | **3.300M** |
| btcd codec + pinned Go LevelDB | 7.22 | 15.24 | 0.358M | 0.690M |

The MDBX/redb write ratios are 3.60x serving and 2.69x IBD-256. Its warm batch
lookup ratios are 1.53x and 1.55x after removing per-key heap allocation and
caching creation MTP by height inside each read view. The Go lane is 12.34x
slower than MDBX in serving mutation and 5.68x slower in IBD mutation under
this stricter direct-to-LevelDB durability boundary. That is not a prediction
of btcd node speed because actual btcd places two caches in front of these
writes.

## Key-format A/B

The key decision was measured separately on the same Go LevelDB lane so engine,
coin value, undo, durability, and workload remained unchanged. Generated
outpoints use vouts 0–3, representing the dominant short-suffix case; the
ordering tests separately cover every encoding-width boundary through
`u32::MAX`:

| vout suffix | total key | serving blocks/s | IBD blocks/s | serving post-compact | IBD post-compact |
|---|---:|---:|---:|---:|---:|
| btcd MSB-VLQ | 33–37 B | 7.22 | 15.24 | **270,540,800 B** | **269,017,088 B** |
| ordered width + BE | 34–37 B | 8.79 | 15.82 | 275,038,208 B | 273,526,784 B |
| fixed BE u32 | 36 B | 8.67 | 16.73 | 284,053,504 B | 282,550,272 B |

The space result was stable: fixed 36-byte keys cost about 5.0% over btcd VLQ;
the ordered variable key cost about 1.7% over VLQ and saved about 3.2% versus
fixed. Timing did not establish a key-format winner. A later VLQ bracket run
measured 9.12 serving blocks/s, reversing the first three-round timing rank,
while its size stayed within 0.03%. Query rates also crossed between runs.
Therefore the format decision uses the stable space result plus required cursor
semantics, not the unstable timing rank.

The 34–37-byte ordered format was selected. It pays roughly 1.7% versus the
smallest format, preserves canonical `(txid, numeric vout)` iteration even for
a transaction with 16,512 or more outputs, and avoids the fixed format's other
3.2%. Its production encoder uses a stack buffer, so variable length does not
allocate per lookup. The MDBX format marker was advanced to version 2; version
1 experimental stores fail closed and require rebuild rather than being
silently reinterpreted.

Key-format report hashes:

| format | round 1 | round 2 | round 3 |
|---|---|---|---|
| fixed 36 | `1f5cdf1b391f9ba2cc6e068eb8e0b2079c2e8b3a45cd78820fa5d3c46a83d246` | `8db6ae2bfd67c309a12d64bdac673de07fefd628fb3496ed74ac9fabc00e4a7d` | `5f3d3b7480000e942f7f9614938c7ad81261f098a016b725ea9fde5cf9e0b92a` |
| ordered variable | `2e37dce4e46bee96d2de0dc3583aaeb11e9dd7afc8db1bc720935e46a81c00da` | `7ea81489b03f0b4cd5cfc6bd1b85deca3a05d6c37bd23e35d8cc40f3dae81db5` | `58ddf78d2843a9c6fc2e6f39e8fafa8183baaf3a09e29898c3051f2e1b6baca0` |

The btcd-VLQ hashes are in the main evidence table below; the bracket run was
`12c18fe2a792e13db7cc6295031bc2a81af1301601213439a58e573bd6619f6e`.

Post-compaction allocated size includes 2M live coins, all retained undo for
the 256 measured blocks, and metadata/tip—not just raw UTXO values:

| lane | serving | IBD-256 | serving bytes/live UTXO | IBD bytes/live UTXO |
|---|---:|---:|---:|---:|
| rBTC redb | 677,933,056 B | 930,787,328 B | 338.97 | 465.39 |
| rBTC MDBX | 271,204,352 B | 271,204,352 B | 135.60 | 135.60 |
| btcd codec + pinned Go LevelDB | 270,540,800 B | 269,017,088 B | 135.27 | 134.51 |

MDBX compact copy reduced the IBD high-water allocation from 361,611,264 to
271,204,352 bytes (25.0%). Its serving workload had little garbage to reclaim.
The MDBX file may contain sparse address-space growth, so APFS allocated
blocks, not only logical length, are the comparable metric. redb's copy
compaction did not reduce the two large files in these runs. Go LevelDB forced
compaction reduced the median serving allocation from 380,928,000 to
270,540,800 bytes and IBD allocation from 342,396,928 to 269,017,088 bytes.

Raw JSON was intentionally not versioned, matching the repository benchmark
policy. The six reports used here had these SHA-256 digests:

| report | SHA-256 |
|---|---|
| Rust round 1 | `4fca42553f7e7b044b0b09e44bc32df52e4bf3c183d3200673760de4b0dd3a94` |
| Rust round 2 | `570850bb112762d0196ab64f5e478295d72f958ef01033cb449d5c9ec2708a6e` |
| Rust round 3 | `b8f768ab18f94da2ed9df8b7b884abc6743efa85fefee0c3d5735c0ec83d9d6b` |
| btcd lane round 1 | `16c1b6bd69fa7116f85ae9dd648cb005ee4d2a8fcc57490883be2836d6a1ec78` |
| btcd lane round 2 | `6e45fc3cab92ef586cbb9b3092b45a40ad757784913b42282de5bebd25e4cad7` |
| btcd lane round 3 | `c35f9b0d8ea9a29f55de2ca81d78d4f5c57d6f6771d5034306f824aee22ceeab` |

## Mainnet evidence supplied with this review

The accompanying Windows 11 report used 32 logical cores, 125 GiB RAM and an
existing 771 GiB mainnet block corpus. Its deterministic 2M-UTXO workload used
the same 5,000-created/4,700-spent block rate, real Bitcoin spend-age sampling,
fsync commits, one read view per block, and forced LSM compaction. It found:

- an unset btcd UTXO cache cut 200,000-block replay from 5,699 to 407 blocks/s,
  a 14x loss that can be mistaken for an engine problem;
- cold lookup rates across LevelDB, Pebble, bbolt, Badger, and MDBX within 8%
  at 2M records, reinforcing that the warm Mac lookup ranking is not decisive;
- MDBX serving and IBD write rates of 24,315 and 32,988 inserts/s, versus
  LevelDB's 67,842 and 30,663; batching helped MDBX but raised its synthetic
  IBD footprint to 800 MiB versus LevelDB's 159 MiB;
- a real 169,337,275-entry MDBX chainstate containing 11.7 GB of raw records
  occupied 50.26 GB of live B-tree pages and a 76.01 GB file, including 25.75
  GB of unreclaimable free pages before compact copy;
- Pebble matched LevelDB's roughly 77 B/UTXO synthetic footprint and increased
  bulk insert rate from 233,945 to 744,477 inserts/s in that Go implementation.

Those measurements explain why the Mac result is not enough to select MDBX
unconditionally. The Mac run proves rBTC's compact complete MDBX design is a
large improvement at 2M live coins. The mainnet run proves long-lived B-tree
delete churn remains the dominant deployment risk, and that an LSM can be a
better physical family at full scale even when a short B-tree benchmark wins.

## Required gate before changing the default

1. Replay a common mainnet block corpus into current redb and the complete
   four-table MDBX store, with the same UTXO cache budget, validation flags,
   undo retention, batch size, and starting state.
2. Continue MDBX past 160M live coins and through enough spend churn to measure
   high-water growth, freelist reuse, and compact-copy frequency. Report raw,
   live-page, allocated, and copied bytes separately.
3. Inject process kills before/after the two directory renames and parent
   directory syncs, then verify reopen exposes either the old or fully compacted
   chainstate with the same tip and all four tables.
4. Hold a 128 GiB hard geometry ceiling and demonstrate an operator threshold
   that compacts before `MDBX_MAP_FULL` without repeated compaction.
5. Measure peak RSS for 64- and 256-block batches. The supplied replay already
   observed roughly 35% more peak memory at 256 blocks, so write throughput
   cannot be considered alone.

Until this gate passes, redb remains the recovery-proven default and MDBX stays
feature-gated. SQLite remains rejected for the UTXO hot path by the existing
2M-point-lookup data. A Rust LSM candidate should be evaluated only if it can
match the same atomic UTXO/undo/tip boundary, bounded cache, crash tests, and
license/build constraints; the btcd/Go Pebble result is a reason to run that
experiment, not permission to transplant its conclusion.

## Reproduction

```sh
RBTC_ENGINE_BENCH_UTXOS=2000000 \
RBTC_ENGINE_BENCH_BLOCKS=256 \
RBTC_ENGINE_BENCH_UPDATES=5000 \
RBTC_ENGINE_BENCH_LOOKUPS=500000 \
RBTC_ENGINE_BENCH_REPORT=/tmp/rbtc-storage-rust.json \
cargo test --release --all-features --test storage_engine_comparison \
  -- --ignored --nocapture

cd contrib/btcd_storage_bench
RBTC_ENGINE_BENCH_UTXOS=2000000 \
RBTC_ENGINE_BENCH_BLOCKS=256 \
RBTC_ENGINE_BENCH_UPDATES=5000 \
RBTC_ENGINE_BENCH_LOOKUPS=500000 \
RBTC_ENGINE_BENCH_REPORT=/tmp/rbtc-storage-btcd.json \
go run .
```

Set `RBTC_ENGINE_BENCH_REVERSE=1` on alternating Rust rounds. A genuine cold
read comparison needs either a dataset larger than memory or a controlled
cache purge; neither condition was present in this run.
