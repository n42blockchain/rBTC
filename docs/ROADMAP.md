# rBTC production roadmap

Status date: 2026-07-25.

This file is the forward-looking plan. A checked item means that the code,
restart/failure tests, and an acceptance run all exist. Historical implementation
notes belong in the architecture and test history, not in open checkboxes.

## Priority rules

- **P0 — release blocker:** required before describing rBTC as a production
  validating node on the corresponding public network.
- **P1 — normal full-node capability:** common in Bitcoin Core, btcd, or other
  mature nodes and important for operating or contributing a node, but not part
  of the consensus trust proof.
- **P2 — deployment-specific:** useful compatibility or product functionality;
  it must not block a secure outbound-only validating-node release.
- **External:** cannot be completed by repository code alone.

Bitcoin Core 31 is the current behavior/reference baseline. The vendored Core 26
fixtures remain useful immutable historical evidence, but Core 26 is no longer a
current product baseline. btcd is used as an independent implementation reference
for listener, pruning, index, RPC, and operational behavior. Compatibility means
matching documented protocol/consensus behavior, not copying every option or RPC.

Primary references:

- [Bitcoin Core 31 release notes](https://bitcoincore.org/en/releases/31.0/)
- [Bitcoin Core RPC surface](https://bitcoincore.org/en/doc/30.0.0/rpc/)
- [Bitcoin Core node/prune/index startup behavior](https://github.com/bitcoin/bitcoin/blob/master/src/init.cpp)
- [btcd operator configuration](https://github.com/btcsuite/btcd/blob/master/sample-btcd.conf)
- [btcd prune/index consistency checks](https://github.com/btcsuite/btcd/blob/master/btcd.go)

## Completed trust baseline

### Consensus and chainstate

- [x] Header PoW, contextual time/difficulty, cumulative-work fork choice,
  checkpoints, minimum-chainwork policy, durable header replay, and active-chain
  reorganization.
- [x] Full contextual block and transaction validation: historical BIP30/BIP16
  exceptions, buried deployments, BIP68/113, SegWit, Taproot, Signet, subsidy,
  coinbase maturity, weight, Merkle/witness commitments, sigops, and script
  execution.
- [x] Atomic UTXO, execution-tip, and per-block undo commits with disk-full,
  interruption, reopen, and rollback tests.
- [x] Differential Core 26 transaction/script vectors, authenticated historical
  blocks/spends, live regtest comparison gates, and parser/property/fuzz coverage.
- [x] One production chainstate executed mainnet genesis through active height
  959,592/hash
  `000000000000000000019190d596b445008319f199f8ee6f6af0e73cbc440667`,
  then cold-reopened in 14.385 seconds without requesting a block.
- [x] Trusted snapshot activation plus independent, restart-safe genesis
  validation and atomic AssumeUTXO finalization; automatic cleanup is explicit,
  ownership-bound, quarantined, and fail-closed.

### Storage and performance

- [x] A checksum-authenticated zstd block freezer with crash-safe staging,
  circular retention, truncation recovery, physical deletion of retired slots,
  and bounded default retention of 1,008 blocks/1 GiB.
- [x] Block undo retention follows the freezer floor: an undo is deleted only
  after its block leaves the retrievable ledger window; unknown header identities
  fail the whole cleanup before mutation.
- [x] Sorted batch UTXO writes, hot/cold read caching, validation-delta
  row/group/shard indexes, parallel read groups, atomic batch commits, cache-sized
  checkpoints, dual-peer downloads, next-window prefetch, and overlapping
  download/stage/execute work.
- [x] Reproducible storage/IBD benchmarks, compaction verification, simulated
  disk-full tests, and SIGKILL/reopen coverage. Target-device HDD/NVMe numbers are
  performance evidence, not a correctness gate.

### Network, mempool, and services

- [x] Bounded outbound peer manager with concurrent handshakes, hot standbys,
  DNS/persisted candidates, restart-safe addrman-like buckets, cooldown,
  discouragement, group diversity, capability checks, failover, and throughput
  ranking.
- [x] Headers-first IBD, pipelined/dual-peer full-block download, BIP152 compact
  block recovery, transaction inventory/download, BIP339, BIP133, BIP35, bounded
  relay caches, and transaction rebroadcast over established outbound sessions.
- [x] Persistent bounded mempool with consensus/standardness validation, BIP125
  and full-RBF modes, ancestor/descendant limits, CPFP carve-out, rolling minimum
  fee, expiration, orphan recovery, recent-confirmed/reject suppression, reorg
  recovery, and a bounded empirical fee estimator.
- [x] Persistent explorer projections, authenticated bounded JSON-RPC, health,
  readiness, Prometheus metrics, loopback-only REST, strict auth-token rotation,
  audit logging, and crash-safe reconciliation.
- [x] Watch-only descriptor wallet, deterministic address state, UTXO/history
  tracking, coin selection, unsigned PSBT creation, external-signature
  finalization, policy validation, durable bounded broadcast, and reorg-aware
  rebroadcast without daemon-held private keys.
- [x] Ordinary persistent execution is enabled on Bitcoin, legacy testnet,
  Testnet4, Signet, and regtest. Testnet4 enforces BIP94's retarget base and
  timewarp boundary and uses pinned Core 31 trust anchors and seeds. The
  fixed-target experimental mode remains available for reproducible validation
  journals.
- [x] Real Testnet4 headers were validated and persisted from genesis through
  active height 145,734/hash
  `00000000002eaba2ff41604d0126d09e142f6f2afb79ee12abf9ad818e677abf`;
  minimum chainwork passed. A separate ordinary persistent run executed block
  1/hash
  `0000000012982b6d5f621229286b880e909984df669c2afabb102ce311b13f28`
  and stopped exactly at the authenticated target; its cold restart reopened
  chainstate in 34 ms, requested no block, and exited in 3.52 seconds at the
  identical execution and header tips.

### Build and release automation

- [x] Locked formatting, Clippy, full tests, coverage floor, RustSec,
  cargo-deny, CycloneDX SBOM, bounded fuzz regressions, targeted Miri, ASan,
  TSan, MSan, deterministic double builds, and public-network smoke workflows.

## P0 — remaining release blockers

- [ ] **Move the maintained compatibility baseline from Core 26 to Core 31.**
  DNS seeds, minimum-chainwork, and assume-valid anchors are refreshed. Verify
  checkpoint coverage and classify every Core 27–31 consensus, policy, P2P,
  storage, and security change; add differential fixtures for applicable
  changes. Core removed
  `libbitcoinconsensus` in 28, so rBTC must either adopt a maintained script
  engine/kernel boundary or own and continuously patch the vendored engine. The
  acceptance gate is a documented dependency/security decision plus identical
  results across the existing corpus and a new Core 31 regtest matrix.
- [ ] **Complete Testnet4 public acceptance.** BIP94 difficulty/timewarp rules,
  current trust anchors and seeds, and ordinary execution are implemented.
  Genesis-to-tip header validation and exact block-1 execution are accepted.
  Complete a full block/chainstate sync, cold restart, and reorg soak and add its
  fixed chainstate acceptance hash. Do not infer any value from legacy testnet.
- [ ] **Sustained public-network operations soak.** Run Bitcoin and Testnet4 for
  at least seven consecutive days across natural tip updates, peer churn,
  controlled restarts, freezer rotation, mempool persistence, and at least one
  exercised reorg/fault scenario. Record maximum RSS, chainstate/freezer growth,
  restart time, peer diversity, and final hashes.
- [ ] **External security review.** Review consensus boundaries, script-engine
  provenance, P2P resource accounting, snapshot trust, storage recovery,
  authentication, wallet/PSBT handling, and release supply chain; resolve every
  critical/high finding and document accepted lower-risk findings.
- [ ] **Signed supported-platform release.** Exercise the release workflow with
  operator-controlled keys on the declared Linux/macOS platform matrix, verify
  byte-identical artifacts and provenance from a clean checkout, publish the
  SBOM, upgrade/rollback notes, data-format compatibility, and disaster-recovery
  procedure.

These are the only blockers to the first production **outbound-only,
watch-only/external-signer validating-node** claim. Inbound service, an internal
hot wallet, mining, exact Core RPC parity, BIP324, and target-HDD benchmark
numbers are intentionally not hidden P0 requirements.

## P1 — normal full-node and operator completeness

- [ ] **Inbound P2P listener and network contribution.** Add explicit bind/listen
  configuration, inbound handshakes, header/block/compact-block and bounded
  mempool service, upload targets, per-peer work accounting, preferred/manual
  peer handling, eviction, ban/discouragement controls, and integration tests
  against Core and btcd. Listening must be optional; outbound-only mode remains
  supported.
- [ ] **Current relay/policy baseline.** Differentially audit Core 31 changes,
  including current relay fee defaults, package relay, orphan DoS accounting,
  TRUC behavior, replacement, standard scripts, and estimator behavior.
  Consensus and local policy must remain separate modules and separate test
  expectations.
- [ ] **Operator configuration and diagnostics.** Add a bounded network-scoped
  config file, explicit cache/freezer/mempool/peer limits, structured
  rate-limited rotating logs, graceful RPC stop, runtime logging controls, and
  stable equivalents of `getblockchaininfo`, `getnetworkinfo`, `getpeerinfo`,
  `getmempoolinfo`, `getindexinfo`, and `verifychain`. Exact response-field
  parity is not required.
- [ ] **Explicit storage lifecycle.** Add configurable automatic/manual prune
  targets, prune/index incompatibility checks, `reindex` and
  `reindex-chainstate`, bounded offline verification/repair, schema migration
  tests, backup/restore instructions, and observable pruning progress. Never
  silently recreate chainstate from an incomplete freezer.
- [ ] **Optional indexes commonly used by node clients.** Add independently
  rebuildable `txindex`, spent-output index, and BIP157/158 compact-filter index
  with explicit disk cost, sync state, prune compatibility, and peer serving.
- [ ] **Network privacy and reachability controls.** Add proxy/`onlynet`,
  Tor/I2P outbound support, bind/whitebind equivalents, and address-network
  isolation tests. Automatic port mapping is not required.
- [ ] **Operational API breadth.** Add raw transaction submission, mempool
  inspection, UTXO scans/proofs, block/header retrieval modes, wait-for-tip
  primitives, peer controls, and stable error codes. Prefer a small documented
  surface over nominal Core RPC parity.

## P2 — role-specific extensions

- [ ] BIP324 v2 transport with v1 fallback and Core 31 interoperability tests.
- [ ] Mining interfaces (`getblocktemplate`/`submitblock` or a versioned local
  IPC boundary) only if rBTC is deployed for mining.
- [ ] ZMQ-compatible or native bounded event publication for indexers; the
  existing REST/event path remains sufficient for the base node.
- [ ] Encrypted daemon-held keys and in-process signing only as a separately
  threat-modeled wallet product. The default node continues to prefer watch-only
  descriptors and external signers.
- [ ] Alternative atomic chainstate backends only after they include UTXO,
  execution metadata, undo, snapshot markers, and the complete crash matrix in
  one durability boundary. MDBX benchmark availability alone is insufficient.
- [ ] GUI, legacy wallet import, exact Core RPC field parity, and specialized
  index/mining APIs only in response to a concrete deployment requirement.

## Performance work policy

Performance changes remain continuous but are profiling-driven rather than
checkbox-driven. Every optimization must preserve these invariants:

1. Untrusted network bytes are bounded and authenticated before staging.
2. A complete validated checkpoint is the smallest publication unit.
3. Download, stage, validation, prefetch, indexing, and cleanup may overlap only
   when their failure domains cannot publish partial state.
4. Database batches deduplicate and sort keys before writes; read caches are
   bounded, network-scoped where persistent, and invalidated by exact state
   transitions.
5. Blocking database/filesystem work never occupies the async network executor;
   queues, workers, in-flight blocks, and retry state always have explicit caps.
6. A speedup is accepted only with identical final tip, UTXO identity, undo and
   freezer bounds, restart behavior, and fault-injection results.

Future measurements should publish blocks/second, execution and fsync time,
cache hit rate, read/write amplification, maximum RSS, disk plateau, and cold
restart time. Hardware-specific NVMe/HDD reports guide defaults but do not delay
correctness or security work.

## Execution order

1. Finish the Core 31/dependency audit and Testnet4 public acceptance soak.
2. Run the sustained public-network soak while preparing the external review.
3. Close review findings and produce the signed supported-platform release.
4. Build inbound service, operator lifecycle, current relay policy, and optional
   indexes in that order.
5. Select P2 work only from an actual deployment need.
