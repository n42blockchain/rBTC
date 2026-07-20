# Production roadmap and acceptance gates

## Phase 0 — kernel (in progress)

- [x] Reuse `rust-bitcoin`, `bitcoinconsensus`, redb, zstd, and BDK rather than reimplementing their domains.
- [x] Hot/cold UTXO persistence, verified snapshot container, immutable archive pieces, and circular pruning policy.
- [x] Unit tests for storage atomicity, snapshot integrity/anchor rejection, archive tamper detection, pruning, P2P envelope, and explorer routing.
- [ ] Property/fuzz tests for every untrusted parser and crash/recovery tests for the file ring.

## Phase 1 — fully validating node

- [x] Header DAG, proof-of-work validation, and cumulative-work fork selection.
- [ ] Contextual header validation: checkpoints, difficulty adjustment, median-time-past, and reorg undo data.
- [ ] Complete block/contextual validation: BIP34/30/68/113/141/143/147/341/342, coinbase maturity, subsidy, sigops, weight, deployment activation, and all standardness rules kept distinct from consensus.
- [ ] Differential tests against Bitcoin Core test vectors and `bitcoin-cli`/regtest; property tests and cargo-fuzz corpus in CI.
- [ ] P2P peer manager, addrman, compact blocks, headers-first IBD, DoS limits, peer eviction, block relay, and transaction relay.
- [ ] BIP324 v2 transport using the maintained rust-bitcoin BIP324 implementation after interoperability tests with Core.

## Phase 2 — data services

- [ ] Transaction/address/block indexes fed only by validated chain changes and correctly rolled back on reorg.
- [ ] Explorer REST, WebSocket/SSE notifications, pagination, endpoint limits, and an embedded static UI.
- [ ] BDK wallet changeset persistence, encrypted secrets, descriptor import/export, PSBT create/sign/finalize, fee policy, coin control, and broadcast.
- [ ] Authenticated local RPC plus optional REST API; wallet API disabled by default and bound only to localhost.

## Phase 3 — performance and release

- [ ] Benchmark IBD, UTXO lookup/mutation, snapshot import/export, compaction, and compression on NVMe and HDD; publish reproducible benchmark fixtures.
- [ ] Benchmark and tune redb from measurements; validate low-disk behavior, growth/cleanup policy, power-loss recovery, and pruning invariants. Consider an optional RocksDB backend only after reproducible toolchain packaging and comparative benchmarks.
- [ ] CI gates: format, clippy, test, LCOV generation (implemented), a coverage threshold based on the completed validation corpus, Miri where applicable, sanitizers, fuzz regression, dependency/license/security audit, SBOM, reproducible release builds, and signed artifacts.
- [ ] External security review and at least a sustained public testnet/regtest soak before any mainnet wallet recommendation.

## Compatibility policy

Wire messages and block/transaction serialization must use Bitcoin's consensus encoding through `rust-bitcoin`. Consensus script checks use the version-pinned Bitcoin Core library. Snapshot and archive formats are rBTC-specific transport/storage formats, so they are versioned and never advertised as Bitcoin P2P messages without a ratified BIP and interoperability testing.
