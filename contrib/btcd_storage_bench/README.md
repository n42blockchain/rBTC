# btcd storage comparison lane

This small Go module is the cross-language lane for
`tests/storage_engine_comparison.rs`. It pins the same Go LevelDB revision as
btcd v0.26.2 and mirrors btcd's current `blockchain/chainio.go` outpoint key,
VLQ, amount, and script encoding. LevelDB, Pebble, Badger, and bbolt receive the
same workload, UTXO/undo/tip mutation, lookup batch, and durability boundary.
Every measured chain transition is one synchronous atomic transaction. Badger
therefore reports an explicit error when a 256-block transaction exceeds its
transaction limit; its non-atomic `WriteBatch` is used only for initial
prefill and is not substituted into the measured mutation path.

Engines retain their native internal cache or mmap policy and share the OS
page cache. No btcd UTXO cache sits in front. The lane normalizes codec,
logical transaction, durability, and request order, but does not pretend that
unlike engine caches have identical semantics or memory cost.

It is deliberately labelled **btcd codec + selected Go engine**, not “btcd
IBD”. It does not execute blocks, scripts, the block index, flat-file writes,
or btcd's in-memory UTXO cache. A complete node-to-node IBD comparison
additionally requires the same block corpus, validation flags, cache budgets,
pruning, and sync endpoint.

The Mac used for the 2026-08-20 measurements has only rBTC's pruned `.rblk`
tail segments. It does not have the approximately 771 GiB flat-file mainnet
corpus used by the supplied Windows `cmd/replayblocks` run, and that custom
btcd replay command is not in this repository or upstream btcd. Consequently
the 0-to-900,000 real-block lane remains an external corpus gate. A synthetic
900,000-transition target must never be presented as evidence that height
900,000 was replayed.

Run:

```sh
cd contrib/btcd_storage_bench
go run .
```

The environment variables are the same as the Rust lane:
`RBTC_ENGINE_BENCH_UTXOS`, `RBTC_ENGINE_BENCH_BLOCKS`,
`RBTC_ENGINE_BENCH_UPDATES`, `RBTC_ENGINE_BENCH_LOOKUPS`, and
`RBTC_ENGINE_BENCH_REPORT`.

`RBTC_BTCD_ENGINES` selects a comma-separated ordered subset of `leveldb`,
`pebble`, `badger`, and `bbolt`. `RBTC_BTCD_SCENARIOS` selects `serving`,
`ibd-256`, or both. `RBTC_ENGINE_BENCH_MAX_SECONDS` bounds mutation time at a
completed transaction boundary and records both target and completed blocks.
It does not interrupt a database call in the middle of an atomic transaction.

`RBTC_BTCD_KEY_FORMAT` defaults to `btcd-vlq`. Set it to `fixed36-be` or
`ordered-varint` to isolate rBTC's proposed fixed-width key and a 34–37-byte
order-preserving alternative against btcd's variable-width key on the same
selected engine, coin values, workload, and durability boundary.

## One-hour storage matrix

`run_one_hour_matrix.sh` builds once and runs each engine/scenario serially so
they do not compete for the same SSD. Its default eight lanes each receive a
450-second mutation budget: seven successful lanes total approximately 52.5
minutes, while Badger's oversized atomic IBD lane normally fails immediately;
prefill, lookup, quiescence, and compaction bring wall time close to one hour.
Each lane targets 900,000 deterministic transitions and records how far it
actually reached.

```sh
./run_one_hour_matrix.sh
```

Override `RBTC_MATRIX_SECONDS_PER_LANE`, `RBTC_MATRIX_ENGINES`,
`RBTC_MATRIX_SCENARIOS`, or `RBTC_MATRIX_REPORT_DIR` when a narrower run is
needed. Reports go under `target/` by default and include one JSON file plus
stdout/stderr per lane and a combined `matrix.json`.

To close the separate real-block gate, provide all of the following on one
machine: the immutable flat block corpus through height 900,000, the exact
custom btcd revision containing `cmd/replayblocks` and its database adapters,
at least the measured database size plus compaction headroom for every lane,
and a pinned cache/validation/fsync configuration. Run engines serially from
the same corpus and publish command line, revision, corpus identity, host,
start/end height, wall time, peak RSS, logical/allocated bytes, and errors.
