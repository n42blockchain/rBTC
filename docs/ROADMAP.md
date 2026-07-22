# Production roadmap and acceptance gates

## Phase 0 — kernel (in progress)

- [x] Reuse `rust-bitcoin`, `bitcoinconsensus`, redb, zstd, and BDK rather than reimplementing their domains.
- [x] Hot/cold UTXO persistence, verified snapshot container, immutable archive pieces, and circular pruning policy.
- [x] Unit tests for storage atomicity, snapshot integrity/anchor rejection, archive tamper detection, pruning, P2P envelope, and explorer routing.
- [ ] Property/fuzz tests for every untrusted parser and fault-injection tests for every file-ring transition. The v1 P2P envelope now has property coverage for arbitrary bounded input, nonce round trips, checksum corruption, declared oversize, and truncation; the BIP325 commitment-section parser covers arbitrary bounded scripts and payload preservation. Manifest-bounded index reconstruction, orphan-rename adoption, and restart-safe partial-segment reorg truncation are implemented.

## Phase 1 — fully validating node

- [x] Header DAG, proof-of-work validation, and cumulative-work fork selection.
- [x] Durable append-only header journal with replayed contextual validation on restart.
- [x] Contextual timestamp validation: median-time-past and the two-hour future-time ceiling.
- [x] Contextual difficulty validation: normal retarget, no-retarget chains, and min-difficulty fallback rules.
- [x] Atomic UTXO undo records for reverse-order chain disconnects, plus durable block-undo encoding/storage.
- [x] Transaction-level UTXO transition with script hooks, amount accounting, and coinbase maturity checks.
- [x] Atomic block UTXO transition with Merkle/coinbase/weight/subsidy checks; UTXO, per-block undo, and execution tip share one physical redb database and one transaction for connect/disconnect.
- [x] Persisted block/UTXO undo journals drive active-chain rewinds after header reorganization.
- [x] Contextual header validation enforces the pinned Bitcoin Core 26 mainnet/testnet checkpoints.
- [x] Minimum-chainwork IBD completion policy with pinned Core 26 defaults, strict overrides, and low-work peer chains kept in IBD without treating their otherwise valid headers as consensus-invalid.
- [ ] Assume-valid acceleration backed by eventual full validation. Core anchors and overrides are validated against the active header chain, but rBTC intentionally continues checking every script until the background verifier and retained validation data exist.
- [ ] Complete block/contextual validation: BIP30 is enforced with its two historical exceptions; BIP34 height, BIP34/BIP66/BIP65 minimum header versions, BIP68/113 locks, deployment-aware BIP141 witness commitment/unexpected-witness rules, BIP147 NULLDUMMY, default-global-Signet BIP325 block solutions, Core 26's base P2SH/WITNESS/TAPROOT flag set and historical exceptions, mutated transaction Merkle trees, buried deployments, Taproot BIP9 state, Core-compatible configurable regtest Taproot and buried activation heights, network-specific subsidy halving intervals, Core-compatible unspendable-output UTXO pruning, the 80,000 legacy/P2SH/witness sigop-cost limit, Core-style transaction duplicate/null-input/base-size checks, and per-block accumulated-fee `MoneyRange` are enforced. The complete pinned Core 26 transaction-vector corpus now runs offline through rBTC's production script adapter: all 119 valid cases and all 70 invalid cases expressible by public consensus flags, with 9 `BADTX` structure cases and 14 policy-only cases classified separately. The harness also parses all 1,207 `script_tests.json` cases and executes the 230 cases whose full flag set is public consensus API, while constructed Core-backed cases cover BIP341 commitments and BIP342 tapscript execution. A real default Signet block fixture passes the production connection path, while damaged, missing, and malformed solutions are rejected without durable residue. Live Core 26 regtest gates compare two sequential valid blocks, twelve invalid structural/contextual classes, configured BIP34/BIP66/BIP65/BIP141/BIP147 boundaries, and 102-block CSV boundaries for height-relative locks, time-relative locks, and BIP113 absolute lock time. Remaining gates include a broad historical mainnet block corpus, custom-Signet parameter plumbing, and keeping policy/standardness distinct from consensus.
- [ ] Differential tests against Bitcoin Core test vectors and `bitcoin-cli`/regtest. Core 26's complete transaction and script JSON files are pinned in normal CI with the public consensus-API boundary made explicit: the script corpus contributes 148 expected passes and 82 expected failures, while 977 policy-only cases remain classified but deliberately unexecuted through `libbitcoinconsensus`. The optional live Core 26 gates additionally prove matching accept/reject outcomes and no rBTC tip/undo/UTXO residue after rejected candidates across seven configurable activation scenarios. Property tests now cover the complete buried-height integer range, mutation-free deployment parser rejection, and bounded arbitrary v1 P2P frames. Historical mainnet blocks, broader parser properties, cargo-fuzz, and CI-provisioned Core remain.
- [x] Async v1 P2P framing with message-size limits, magic validation, and checksum validation.
- [ ] P2P peer manager, addrman, compact blocks, additional DoS limits, peer eviction, block relay, and transaction relay. Outbound v1 handshake, minimum-version/self-connection checks, full-history+witness service gating, `getheaders`, duplicate-free bounded 16-block witness `getdata` pipelining with ordered execution, unsolicited/notfound response rejection, the 2,000-header response limit, and durable continuously polling regtest/default-Signet headers-first/block IBD are implemented. Validated batches feed the pruned ledger through a restart-safe staging/publish protocol, including reorg truncation and missing-suffix backfill. An end-to-end mock-peer gate sends the real default-Signet block 1 through handshake, header selection, witness-block download, BIP325 validation, atomic chainstate, retained ledger, and persistent explorer projection.
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
- [ ] Complete storage benchmarks on target NVMe and HDD. A deterministic release fixture now compares redb quick-repair on/off with the optional durable MDBX UTXO backend; simulated disk-full, repeated SIGKILL/reopen, transaction abort, and truncated-copy gates enforce old-or-new atomic state. MDBX cannot become selectable production chainstate until it atomically includes undo and execution metadata and passes the same crash matrix.
- [ ] CI gates: format, clippy, test, LCOV generation (implemented), a coverage threshold based on the completed validation corpus, Miri where applicable, sanitizers, fuzz regression, dependency/license/security audit, SBOM, reproducible release builds, and signed artifacts.
- [ ] External security review and at least a sustained public testnet/regtest soak before any mainnet wallet recommendation.

## Compatibility policy

Wire messages and block/transaction serialization must use Bitcoin's consensus encoding through `rust-bitcoin`. Consensus script checks use the version-pinned Bitcoin Core library. Snapshot and archive formats are rBTC-specific transport/storage formats, so they are versioned and never advertised as Bitcoin P2P messages without a ratified BIP and interoperability testing.

## Current critical path

The durable regtest/default-Signet headers-first/block IBD milestone is
implemented, including atomic multi-block checkpoints, active-branch rewinds,
and real default-Signet block execution. The next acceptance milestone is
mainnet/testnet-safe consensus activation: complete deployment state, remaining
block rules and Core differential vectors before widening the execution safety
gate. Peer diversity/DoS hardening follows before a public long-running node.
Compression, archive transport, explorer, and wallet work must not be presented
as a substitute for this validating-node path.
