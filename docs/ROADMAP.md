# Production roadmap and acceptance gates

## Phase 0 — kernel (in progress)

- [x] Reuse `rust-bitcoin`, `bitcoinconsensus`, redb, zstd, and BDK rather than reimplementing their domains.
- [x] Hot/cold UTXO persistence, verified snapshot container, immutable archive pieces, and circular pruning policy.
- [x] Unit tests for storage atomicity, snapshot integrity/anchor rejection, archive tamper detection, pruning, P2P envelope, and explorer routing.
- [ ] Property/fuzz tests for every untrusted parser and fault-injection tests for every file-ring transition. Manifest-bounded index reconstruction, orphan-rename adoption, and restart-safe partial-segment reorg truncation are implemented.

## Phase 1 — fully validating node

- [x] Header DAG, proof-of-work validation, and cumulative-work fork selection.
- [x] Durable append-only header journal with replayed contextual validation on restart.
- [x] Contextual timestamp validation: median-time-past and the two-hour future-time ceiling.
- [x] Contextual difficulty validation: normal retarget, no-retarget chains, and min-difficulty fallback rules.
- [x] Atomic UTXO undo records for reverse-order chain disconnects, plus durable block-undo encoding/storage.
- [x] Transaction-level UTXO transition with script hooks, amount accounting, and coinbase maturity checks.
- [x] Atomic block UTXO transition with Merkle/coinbase/weight/subsidy checks, one-transaction connect/disconnect, and restart-safe write-ahead recovery across UTXO/undo/execution stores.
- [x] Persisted block/UTXO undo journals drive active-chain rewinds after header reorganization.
- [x] Contextual header validation enforces the pinned Bitcoin Core 26 mainnet/testnet checkpoints.
- [ ] Add minimum-chainwork/assume-valid IBD policy without weakening eventual full validation.
- [ ] Complete block/contextual validation: BIP30 is enforced with its two historical exceptions; BIP34 height, BIP68/113 locks, deployment-aware BIP141 witness commitment/unexpected-witness rules, mutated transaction Merkle trees, buried deployments, Taproot BIP9 state, Core-compatible configurable regtest Taproot activation, the 80,000 legacy/P2SH/witness sigop-cost limit, and Core-style transaction duplicate/null-input/base-size checks are enforced. Remaining gates include full BIP141/143/147/341/342 vector coverage and keeping policy/standardness distinct from consensus.
- [ ] Differential tests against Bitcoin Core test vectors and `bitcoin-cli`/regtest; property tests and cargo-fuzz corpus in CI.
- [x] Async v1 P2P framing with message-size limits, magic validation, and checksum validation.
- [ ] P2P peer manager, addrman, compact blocks, additional DoS limits, peer eviction, block relay, and transaction relay. Outbound v1 handshake, minimum-version/self-connection checks, full-history+witness service gating, `getheaders`, bounded 16-block witness `getdata` pipelining with ordered execution, response validation, and durable continuously polling regtest headers-first/block IBD are implemented. Validated batches feed the pruned ledger through a restart-safe staging/publish protocol, including reorg truncation and missing-suffix backfill.
- [ ] BIP324 v2 transport using the maintained rust-bitcoin BIP324 implementation after interoperability tests with Core.

## Phase 2 — data services

- [x] In-memory explorer index implementation for embedded/regtest use.
- [x] Persistent redb transaction/address/block indexes fed only by validated chain changes, restart-reconciled from the ledger or full-history peers, and correctly rolled back on reorg.
- [x] Explorer UTXO pagination and endpoint input limits, enforced before bounded reads from the persistent index.
- [ ] Explorer WebSocket/SSE notifications. The read-only REST routes, loopback-only daemon listener, CSP-constrained embedded static UI, and persistent backing index are implemented.
- [x] BDK watch-only wallet changeset persistence: owner-only SQLite, transactional address revelation, restart continuity, exact descriptor/network checks, and explicit rejection of secret descriptors.
- [ ] Encrypted wallet secrets, descriptor import/export, PSBT create/sign/finalize, fee policy, coin control, and broadcast.
- [ ] Authenticated local RPC plus optional REST API; wallet API disabled by default and bound only to localhost.

## Phase 3 — performance and release

- [ ] Benchmark IBD, UTXO lookup/mutation, snapshot import/export, compaction, and compression on NVMe and HDD; publish reproducible benchmark fixtures.
- [ ] Benchmark and tune redb from measurements; validate low-disk behavior, growth/cleanup policy, power-loss recovery, and pruning invariants. Consider an optional RocksDB backend only after reproducible toolchain packaging and comparative benchmarks.
- [ ] CI gates: format, clippy, test, LCOV generation (implemented), a coverage threshold based on the completed validation corpus, Miri where applicable, sanitizers, fuzz regression, dependency/license/security audit, SBOM, reproducible release builds, and signed artifacts.
- [ ] External security review and at least a sustained public testnet/regtest soak before any mainnet wallet recommendation.

## Compatibility policy

Wire messages and block/transaction serialization must use Bitcoin's consensus encoding through `rust-bitcoin`. Consensus script checks use the version-pinned Bitcoin Core library. Snapshot and archive formats are rBTC-specific transport/storage formats, so they are versioned and never advertised as Bitcoin P2P messages without a ratified BIP and interoperability testing.

## Current critical path

The durable regtest headers-first/block IBD milestone is implemented, including
active-branch rewinds and interrupted-transition recovery. The next acceptance
milestone is mainnet/testnet-safe consensus activation: complete deployment
state, remaining block rules and Core differential vectors before removing the
regtest execution safety gate. Peer diversity/DoS hardening follows before a
public long-running node. Compression, archive transport, explorer, and wallet
work must not be presented as a substitute for this validating-node path.
