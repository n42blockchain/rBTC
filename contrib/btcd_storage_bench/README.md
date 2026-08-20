# btcd storage comparison lane

This small Go module is the cross-language lane for
`tests/storage_engine_comparison.rs`. It pins the same Go LevelDB revision as
btcd v0.26.2 and mirrors btcd's current `blockchain/chainio.go` outpoint key,
VLQ, amount, and script encoding. The workload, UTXO/undo/tip mutation, lookup
batch, and durability boundary match the Rust benchmark.

It is deliberately labelled **btcd codec + Go LevelDB**, not “btcd IBD”. It
does not execute blocks, scripts, the block index, flat-file writes, or btcd's
in-memory UTXO cache. A complete node-to-node IBD comparison additionally
requires the same block corpus, validation flags, cache budgets, pruning, and
sync endpoint; this repository does not currently have that corpus on macOS.

Run:

```sh
cd contrib/btcd_storage_bench
go run .
```

The environment variables are the same as the Rust lane:
`RBTC_ENGINE_BENCH_UTXOS`, `RBTC_ENGINE_BENCH_BLOCKS`,
`RBTC_ENGINE_BENCH_UPDATES`, `RBTC_ENGINE_BENCH_LOOKUPS`, and
`RBTC_ENGINE_BENCH_REPORT`.

`RBTC_BTCD_KEY_FORMAT` defaults to `btcd-vlq`. Set it to `fixed36-be` or
`ordered-varint` to isolate rBTC's proposed fixed-width key and a 34–37-byte
order-preserving alternative against btcd's variable-width key on the same
LevelDB, coin values, workload, and durability boundary.
