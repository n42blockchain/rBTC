# Local storage pressure run (2026-08-24)

This report records the Mac work that can be run without copying the 762–771 GB
Bitcoin block corpus from the Windows evidence host. It is deliberately split
from the real-block replay: every throughput number below is a generated
storage workload, not Bitcoin validation TPS.

## Host and revision

- Revision under test: `b4e9c00410ef29d2cace90128f1e5b815ad72d00`, clean before the run.
- MacBook Pro `MacBookPro18,2`, Apple M1 Max, 10 cores (8 performance, 2
  efficiency), 64 GB RAM.
- macOS 26.5.1 (`25F80`), Darwin 25.5.0.
- APFS data volume had 913 GiB available before the generated workloads.
- Default `mimalloc` and `--release --locked --all-features` were used unless a
  command below says otherwise.

## Correctness baseline

The following completed before performance work:

```text
cargo fmt --check
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo test --locked --all-features
```

The library reported 821 passed, 0 failed and 5 ignored. All non-ignored
integration and recovery tests also passed. The external Tor/I2P/Core and
mainnet-scale tests remained ignored. Release reruns of the five-boundary MDBX
compact-copy crash matrix and redb abrupt-kill/truncation recovery matrix also
passed.

The compiler printed the existing vendored Bitcoin Core 26 C warning about the
non-NUL-terminated 13-byte BIP340 tag and the existing vendored redb dead-code
warning. Neither is a new rBTC Clippy finding.

## Matched 2M complete-chainstate comparison

Three rounds were run, alternating backend order:

```sh
RBTC_ENGINE_BENCH_UTXOS=2000000 \
RBTC_ENGINE_BENCH_BLOCKS=256 \
RBTC_ENGINE_BENCH_UPDATES=5000 \
RBTC_ENGINE_BENCH_LOOKUPS=500000 \
cargo test --release --locked --all-features \
  --test storage_engine_comparison -- --ignored --nocapture
```

The boundary includes compact UTXOs, per-block undo and execution tip in the
same durable transaction; it excludes blocks and script validation. These are
the three-round medians:

| lane | blocks/s | lookup/s | post-compact allocated |
|---|---:|---:|---:|
| MDBX serving, one block/commit | **103.06** | **3.576M** | **271,208,448 B** |
| redb serving, one block/commit | 27.44 | 2.244M | 677,933,056 B |
| MDBX IBD, 256 blocks/commit | **94.41** | **3.512M** | **271,208,448 B** |
| redb IBD, 256 blocks/commit | 36.53 | 2.259M | 930,787,328 B |

On this warm generated workload MDBX measured 3.76× the redb serving mutation
rate, 2.58× the IBD-256 mutation rate, and 1.56–1.59× the lookup rate. Its
post-compact allocation was 40.0% of redb serving and 29.1% of redb IBD-256.
Execution-order reversal did not change the ranking. Test-body wall time was
45.5–47.8 seconds after compilation; observed peak RSS ranged from 3.34 to
3.78 GB.

This reproduces the direction of the earlier Mac microbenchmark. It does not
override the real-block overlay result because the latter includes snapshot
base reads, validation, write-back and compact/rebase lifecycle work.

## Broader 2M storage and snapshot workload

One generated lane compared the legacy chainstate surface and SQLite with the
same 2M UTXOs, 256 transitions, 5,000 replacements per transition and 500,000
point reads:

| backend | seed ns/coin | mutation median | lookup mean | lookup p99 | bytes before compact |
|---|---:|---:|---:|---:|---:|
| redb, normal repair | 6,031 | 28.28 ms/block | 1,352 ns | 4,250 ns | 539,504,640 |
| redb, quick repair | 9,310 | 35.95 ms/block | 1,347 ns | 3,750 ns | 539,504,640 |
| MDBX UTXO surface | **913** | **8.32 ms/block** | **953 ns** | **2,083 ns** | **268,451,840** |
| SQLite UTXO surface | 3,877 | 9.22 ms/block | 2,436 ns | 6,042 ns | 424,226,312 |

The snapshot lane exported, verified and imported 2M records at 810, 153 and
3,923 ns/record respectively. The 174,000,000 raw record bytes compressed to
2,500,138 bytes because this deterministic fixture repeats scripts and values;
that compression ratio is not representative of mainnet.

## Concurrent MDBX writer/readers

The stress example commits transactions that insert about 200,000 keys and
delete up to 150,000 older keys while two readers repeatedly open fresh read
transactions.

The existing default mode was itself misleading: the example's top-level
command promised a clean run but defaulted to `all`, the intentional negative
control that calls environment-level `Database::info/stat`. On this Mac it
reproduced `MDBX_CORRUPTED` from the writer's `put` in 1.34 seconds. That is the
known unsafe binding control, not the product path; rBTC uses
`Transaction::env_info/env_stat`.

The example now defaults to `all-txn`, which combines point reads and the safe
transaction-scoped statistics. `all` remains an explicit expected-failure
control and unknown mode names fail immediately.

Safe results:

| mode | duration | commits | keys written | point reads | info/stat polls | peak RSS |
|---|---:|---:|---:|---:|---:|---:|
| `reads` | 60.8 s | 102 | 20.4M | 153.128M | 0 | 1.45 GB |
| `info-txn` | 300.7 s | 290 | 58.0M | 0 | 1.109B | 4.20 GB |
| `all-txn` | 301.2 s | 286 | 57.2M | 600.906M | 300,453 | 4.18 GB |

All safe modes exited 0 with no `Corrupted`, `BadTxn`, panic or crash. The
combined lane is the relevant acceptance case: point reads and capacity
statistics both overlap the large writer transaction.

## Scaled churn, undo and compact-copy gate

The repository runner was used with two prebuilt serial lanes:

```sh
RBTC_MDBX_GATE_UTXOS=2000000 \
RBTC_MDBX_GATE_BLOCKS=4096 \
RBTC_MDBX_GATE_UPDATES=5000 \
RBTC_MDBX_GATE_SEED_BATCH=100000 \
RBTC_MDBX_GATE_UNDO_RETENTION=288 \
RBTC_MDBX_GATE_CAPACITY_BYTES=1073741824 \
RBTC_MDBX_GATE_COMPACT=1 \
RBTC_MDBX_GATE_MIN_RECLAIM_PERCENT=10 \
RBTC_MDBX_GATE_REPORT_INTERVAL=512 \
contrib/run_mdbx_replacement_gate.sh <new-output-directory>
```

Each lane maintained 2M live coins while applying 20.48M spends and 20.48M
creates, retaining 288 undo rows. The 1 GiB geometry is intentionally much
tighter than the production 128 GiB policy, so it exercises compact-copy; it
does not predict production copy frequency.

| lane | elapsed | transitions/s | compact copies | peak RSS | final allocated | final free pages |
|---|---:|---:|---:|---:|---:|---:|
| batch 64 | **47.69 s** | **85.89** | 0 | **916,996,096 B** | 383,139,840 B | 70,172,672 B |
| batch 256 | 56.85 s | 72.05 | 5 | 2,934,669,312 B | **361,332,736 B** | **0 B** |

Both lanes reached height 4,096 with exactly 2,000,000 UTXOs, 288 undo rows,
the same synthetic tip, 254,290,746 raw record bytes and the same four-table
content digest:

```text
d1a9badf1e78d8bdd07324579437b95480db9034da539b610ff7bce43588acdb
```

Every compact-copy preserved its pre-copy digest and cleared the freelist. The
64 lane stayed below 55% of the 1 GiB capacity and therefore correctly did not
copy. The 256 lane crossed the threshold with at least 10% reclaimable pages;
its larger copy-on-write transaction then grew at least 50% beyond the latest
post-copy baseline often enough to permit five copies.

At this scale batch 64 was 19.2% faster and batch 256 used 3.20× its peak RSS.
That exceeds the gate's 1.5 RSS warning threshold, but it is a local warning,
not closure of the gate: the required 160M/900,000 run may have a different
ratio and remains external. The result specifically rejects treating the tiny
20k smoke's equal RSS or the production observation's 1.35 ratio as universal.

## Evidence hashes and retention

Raw generated reports remain ignored under
`target/local-bench-b4e9c00-20260824`. After the hashes and this summary were
checked, only the six explicitly named generated database directories were
moved to Trash and their known MDBX files were then permanently deleted; they
cannot be recovered. The reports, gate logs and timing files remain, reducing
the retained benchmark directory from about 6 GB to 104 KB.

| report | SHA-256 |
|---|---|
| matched round 1 | `6a2e5a7f31238b5a5fc22f7afc34f960fda37fce41f65f24fd8f0594db372dea` |
| matched round 2 | `ec029c524fc1ccdbcf8904ef438f40658709ea85e23773f01952ddef34915711` |
| matched round 3 | `62000bd347fa7191f1a2cbc216d9dba484c10d6dbdd277d116c707288c4d01e8` |
| broad storage report | `13d344003780db428e8fed5bb45b2e650c52c76e14cbedcb4d2097aa08b83c2a` |
| scaled 64/256 matrix | `26381c9da776b5545e5535cad04a65317e4b71aacf0d7d547e028c0a06add69d` |

## Decision impact

- The current Mac data supports MDBX for warm compact chainstate operations and
  confirms the transaction-scoped concurrent statistics fix under sustained
  contention.
- The 2M churn lane says batch size and capacity policy can dominate MDBX
  throughput, RSS and copy frequency. Keep 64 and 256 as separate required
  full-scale lanes; do not select 256 from the tiny smoke alone.
- The existing 28,350-real-block result remains the valid execution comparison;
  this report neither replaces it nor supplies genesis-to-tip time/TPS.
- Redb remains the default until the authenticated migration/rollback work and
  full 160M/900,000 lifecycle gate are complete.
