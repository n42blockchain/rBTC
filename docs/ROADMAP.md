# rBTC production roadmap

Status date: 2026-07-27.

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
  reorganization. Successful header responses use an `O(batch)` rollback guard
  rather than cloning the complete DAG before every durable append.
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
- [x] Local rBTC snapshot activation plus independent, restart-safe genesis
  validation and atomic AssumeUTXO finalization; automatic cleanup is explicit,
  ownership-bound, quarantined, and fail-closed. The base block must be on the
  fully validated maximum-work active header chain. Core 31 v2 compatibility is
  additionally covered by a real external Testnet4 height-120,000 file; snapshot
  source selection remains an explicit operator decision rather than an
  automatic trust service.

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
  and full-RBF modes, Core 31 cluster/TRUC limits, rolling minimum
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
- [x] One ordinary public Testnet4 chainstate validated and executed genesis
  through height 145,735/hash
  `0000000000a3f79d1bd3ee3ca31cdde97e9ae86efe34f72a6aac6d4fe15cb03f`;
  minimum chainwork passed. A cold restart opened chainstate in 52 ms, advanced
  headers and execution to height 145,737/hash
  `0000000000c655393aba8556ccb27913faf4fdfcb90d7bd5c0bdf4e62c923769`,
  and exited in 4.40 seconds. The bounded freezer occupied 5.9 MiB and the full
  directory 3.1 GiB at that acceptance point.
- [x] A real external Core 31 Testnet4 height-120,000 v2 snapshot activated,
  served and executed through live height 145,763 while an independent
  genesis-to-120,000 replay matched all 13,870,119 entries and
  1,350,756,785 canonical bytes. Finalization cleared the assumed marker. A
  cold restart opened in 173 ms, advanced to height 145,766/hash
  `000000000074ec24258d33c6e340032db208128adde0f7841c83fdbbeb3e25ea`,
  and exited in 6.16 seconds.

### Build and release automation

- [x] Locked formatting, Clippy, full tests, coverage floor, RustSec,
  cargo-deny, CycloneDX SBOM, bounded fuzz regressions, targeted Miri, ASan,
  TSan, MSan, deterministic double builds, and public-network smoke workflows.

## P0 — remaining release blockers

- [x] **Move the maintained consensus/reference baseline from Core 26 to Core
  31.** DNS seeds, minimum-chainwork, assume-valid, checkpoints, Testnet4, and
  AssumeUTXO identities are refreshed. The Core 27–31 consensus, policy, P2P,
  storage, RPC, wallet, and security changes are classified in
  [the compatibility decision](CORE31_COMPATIBILITY.md). rBTC explicitly owns
  and patches its narrow vendored script-engine boundary after Core removed
  `libbitcoinconsensus`; it does not claim upstream maintenance. The immutable
  Core 26 corpus remains historical evidence, while an official Core 31.0
  daemon passed all seven live regtest differential matrices. Current relay
  policy is correctly retained as a separate P1 gate rather than conflated
  with consensus compatibility.
- [x] **Finish the original fast-bootstrap contract using Bitcoin's existing
  AssumeUTXO model.** Validate the complete header chain and select its
  maximum-work active branch; accept only a Core 31 chainparams height,
  blockhash, UTXO-set hash, and chain-transaction count; load a
  `dumptxoutset`-compatible snapshot; validate ordinary blocks from that base to
  the live tip; and independently execute genesis to the base before clearing
  the assumed marker. Add bounded resumable download from explicitly configured
  sources, but do not invent a P2P snapshot service or claim that a file checksum
  proves chainstate correctness. The existing rBTC v3 container remains a local
  migration format until Core compatibility is accepted. The Core 31 v2 parser,
  release-pinned identities, exact `hash_serialized` calculation, bounded txid
  grouping, two-pass race closure, atomic activation API, and offline CLI are
  implemented. The external Testnet4 fixture, complete assumed/live/background
  lifecycle, and bounded parallel/resumable HTTPS transport are accepted.
  On 2026-07-26 the Core 31 Mainnet height-935,000 snapshot served and validated
  ordinary blocks through live height 959,688 while the independent chainstate
  replayed genesis through the exact base hash. A simultaneous 11-peer failure
  stopped at atomic height 732,941; restart resumed at 732,942, caught the
  serving chain up, and completed without replaying a committed batch. The
  final identity matched 164,241,311 entries and 15,334,473,795 canonical bytes,
  50,340,320 net active-overlay updates were materialized, and the assumed
  marker cleared. The resumed run took 24,998.34 seconds; the complete two-run
  acceptance including the exercised recovery took about 14.4 hours.
- [x] **Choose the hot/cold UTXO boundary from replay data, not the current
  60-day constant.** Persist a network-scoped histogram of spent-output coin age
  in blocks, and report for candidate windows the share of the current UTXO set
  kept hot versus the share of observed spends it would hit. Select the smallest
  window meeting the documented target (initial evaluation: 99% spend-hit rate)
  on complete mainnet replay plus a live-tip sample; publish sample count,
  quantiles, read amplification, RSS, and IBD/restart impact. Hot/cold remains
  local storage policy and never becomes part of snapshot identity or
  consensus. Exact network-scoped block-age rows, sorted batch
  updates, honest coverage metadata, and same-transaction reorg reversal are
  implemented. Fixed-memory current-UTXO population/byte reporting and a
  fail-closed 99%-hit recommendation gate are implemented. The report also
  emits spend-age quantiles and expected hot-first tier probes, and the chosen
  window can be applied through crash-resumable, sorted 65,536-record atomic
  re-tier batches. Complete replay observed 3,257,609,051 spends: P99 age was
  122,194 blocks, and the smallest candidate reaching the 99% target was
  157,680 blocks (three years) at 99.38467%. The post-base live sample observed
  179,211,528 spends through height 959,730 and confirmed 99.42139%, P99
  129,338, and 1.00578 expected hot-first probes per spend. Applying that
  boundary scanned 166,269,013 UTXOs in 1,029.64 seconds; after a 42-block live
  advance, a resumable re-scan moved only 43,427 newly aged rows and finished
  with 97,862,624 hot / 68,429,071 cold UTXOs. The report's predicted hot count
  exactly matched physical storage. The final active/audit directories occupied
  76/237 GiB, sampled process physical memory peaked at 58.0 GiB during
  finalization, and the re-tiered chainstate cold-opened in 46 ms before
  validating the next 42 blocks.
- [ ] **Sustained public-network operations soak.** Run Bitcoin and Testnet4 for
  at least seven consecutive days across natural tip updates, peer churn,
  controlled restarts, freezer rotation, mempool persistence, and at least one
  exercised reorg/fault scenario. Record maximum RSS, chainstate/freezer growth,
  restart time, peer diversity, and final hashes. The immutable-baseline run
  started at `2026-07-26T22:19:44Z`; its PID-switching monitor now records
  process, peer-group, tip/hash, freezer, persistent-store, and disk evidence,
  and its fail-closed report enforces the seven-day, both-network restart, and
  fault-injection gates. Current evidence and the remaining scheduled exercises
  are in [PUBLIC_NETWORK_SOAK.md](PUBLIC_NETWORK_SOAK.md).
- [x] **External security review.** Review consensus boundaries, script-engine
  provenance, P2P resource accounting, snapshot trust, storage recovery,
  authentication, wallet/PSBT handling, and release supply chain; resolve every
  critical/high finding and document accepted lower-risk findings. The
  third-party full-tree report and main-branch integration disposition are in
  [AUDIT.md](AUDIT.md). Every critical/high and medium issue is fixed with
  regression coverage; the remaining information-level Windows filesystem
  hardening gap is explicitly accepted as a declared platform limitation rather
  than being hidden by a successful cross-compile.
- [ ] **Signed supported-platform release.** Exercise the release workflow with
  operator-controlled keys on the declared Linux/macOS/Windows platform matrix,
  verify byte-identical artifacts and provenance from a clean checkout, publish
  the SBOM, upgrade/rollback notes, data-format compatibility, and
  disaster-recovery procedure. The workflow now fails closed on missing or
  wrong-team credentials; applies and verifies Developer ID/notarization and
  Authenticode/RFC3161 signatures; produces a strict root-data-schema-v3
  manifest, CycloneDX SBOM, per-artifact provenance, and offline
  repository/organization-bound Sigstore manifest bundle; and publishes only
  after the complete draft is assembled. The local `n42appv2` Android/Apple
  signing inventory and exact protected-environment contract are recorded in
  [RELEASE_SIGNING.md](RELEASE_SIGNING.md). A Developer ID Application identity,
  Windows Authenticode identity, release-immutability setting, and real tagged
  clean-host run remain external execution gates.

These are the only blockers to the first production **outbound-only,
watch-only/external-signer validating-node** claim. Inbound service, an internal
hot wallet, mining, exact Core RPC parity, BIP324, and target-HDD benchmark
numbers are intentionally not hidden P0 requirements.

## P1 — normal full-node and operator completeness

The dependency order, module boundaries, `n42-26` embedding contract, and
phase-specific acceptance gates are expanded in
[P1_FULL_NODE_PLAN.md](P1_FULL_NODE_PLAN.md). P1 is library-first: `rbtcd`
remains a thin adapter and every new service must be independently bounded,
optional where appropriate, and host-runtime compatible.

- [x] **Inbound P2P listener and network contribution.** Explicit bind/listen
  configuration, inbound handshakes, header/block/compact-block and bounded
  mempool service, upload targets, per-peer work accounting, preferred/manual
  peer handling, eviction, ban/discouragement controls, and integration tests
  against real Core 31 and btcd 0.26 clients are complete. The listener advertises
  `NODE_NETWORK` only when full history is available and otherwise advertises
  `NODE_NETWORK_LIMITED`; BIP61 rejects without an object hash are checksum
  validated and safely decoded. Listening remains optional and outbound-only
  mode is unchanged.
- [x] **Current relay/policy baseline.** Core 31's 0.1 sat/vB relay/incremental
  defaults, versions 1–3, aggregate 100,000-byte data-carrier limit, 2,500
  legacy-sigop limit, 64-transaction/101-kvB clusters, TRUC topology/size rules,
  one-parent/one-child package fee bumping, ephemeral dust, bounded orphan work,
  rolling minimum fee, persistence/reorg recovery, full-RBF default, and
  estimator bounds are implemented with atomic adversarial tests. Replacement
  remains a documented conservative subset: exact Core feerate-diagram and
  sibling-eviction ordering are P2 policy-fidelity work, not a consensus or
  network-interoperability requirement.
- [x] **Operator configuration and diagnostics.** The library/CLI now share
  strict typed peer, DNS, cache, freezer, mempool, API, consensus, and
  AssumeUTXO bounds, with subsystem status/events available to an embedded
  host. The strict bounded `key=value` config file now supports global/per-network
  scopes, unknown-key rejection, and CLI override precedence. Hot-standby and
  mempool budgets now remain identical across memory, reorg recovery, durable
  persistence, and reopen. Authenticated stable equivalents of
  `getblockchaininfo`, `getnetworkinfo`, `getpeerinfo`, `getmempoolinfo`,
  `getindexinfo`, and `verifychain`, plus delayed graceful `stop`, are complete.
  Standalone JSON diagnostics now use a bounded non-blocking queue, a fixed
  per-second limiter, owner-only size rotation, dropped/error metrics, and
  authenticated runtime level control; embedded hosts use typed event/status
  receivers. Startup and every checkpoint boundary also enforce a configurable
  free-space reserve plus worst-case atomic-write headroom; low space is a
  local fail-safe, never a peer fault, and `getdiskinfo`/status/metrics expose
  the current forecast. An owner-only per-data-directory process lock rejects
  aliases and reports the holding PID/network/start time before any database or
  network open. Exact response-field parity is not required.
- [x] **Explicit storage lifecycle.** Add configurable automatic/manual prune
  targets, prune/index incompatibility checks, `reindex` and
  `reindex-chainstate`, bounded offline verification/repair, schema migration
  tests, backup/restore instructions, and observable pruning progress. Never
  silently recreate chainstate from an incomplete freezer. Bounded
  `--prune-blocks` (288–1,008) and `--prune-max-bytes` (at least 550 MiB)
  startup targets now drive the existing crash-safe physical freezer rotation
  and sorted undo cleanup. Targets and physical-prefix progress are persisted
  and observable. Manual height pruning now requires a read-only plan token,
  a clean full audit, an unchanged index, a 288-block retained suffix, and a
  restart-resumable versioned intent. A fixed-memory, count/byte-bounded `--verify-storage` command
  now verifies freezer pieces and decompressed records without opening mutable
  databases and emits a JSON dry-run repair plan. Exclusive `--verify-chain`
  fully replays stored headers and checks maximum-work header/execution/freezer/
  retained-block/undo consistency with bounded one-pass archive decoding,
  refusing missing stores and making redb recovery semantics explicit.
  `--reindex-from-freezer OUTPUT` now rebuilds into a non-aliasing owned
  directory only when a clean source covers height 1 through the replayed
  maximum-work tip; it ignores source chainstate, batches archive reads,
  overlaps staging and UTXO prefetch, resumes crashes, and promotes only after
  exact-target cross-store verification. `--reindex-chainstate OUTPUT` covers
  incomplete local history by pinning the replayed maximum-work header tip and
  reacquiring witness blocks through bounded full-history peers under complete
  consensus/script validation. The shared code-level index policy now refuses
  activation without required local/peer history and refuses pruning past a
  lagging enabled projection; only explorer/wallet baselines may substitute
  for historical UTXO views.
  The strict network-bound root manifest now inventories every subsystem
  schema, migrates legacy absence only after successful preflight, and refuses
  future rollback before mutable database open. Backup/restore, rollback, and
  disaster decisions are published in `docs/DISASTER_RECOVERY.md`.
- [x] **Optional indexes commonly used by node clients.** Independently
  rebuildable `--txindex`, `--spent-output-index`, and
  `--block-filter-index` stores now have network/kind/schema identities,
  atomic sorted batch writes, durable tips, bounded rollback retention,
  active-chain rewind/catch-up, BIP30 restoration, exact executed-prevout
  BIP158 construction and BIP157 header chaining. They participate in
  automatic/manual prune gates, both explicit full reindex paths, conservative
  checkpoint disk headroom, root schema inventory, typed host status/events,
  `getindexinfo`, and bounded authenticated lookup RPCs. Serving those filters
  on inbound BIP157 P2P messages belongs to the inbound-peer service item
  below, because an outbound-only node has no peer request surface.
- [x] **Network privacy and reachability controls.** IPv4/IPv6 `onlynet`,
  fail-closed SOCKS5 proxy routing, exact-IP protected inbound roles, explicit
  routable advertisement, and address-family isolation are complete. Native
  onion/I2P persistence/discovery is a P2 anonymity extension. Automatic port
  mapping is not required.
- [x] **Operational API breadth.** Authenticated raw submission, cursor-paged
  mempool and retained raw-block reads, header/block metadata, exact
  chainstate outpoints, SSE tip waits, peer/prune/index status, lifecycle
  control, and stable errors are complete. Administrative peer mutation and a
  general chainstate scan are P2 operator conveniences. This bounded surface is
  the supported contract rather than nominal Core RPC parity.

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
- [ ] Native onion/I2P address persistence and name-proxy discovery.
- [ ] Administrative peer mutation and a general bounded offline/cursor
  chainstate scan.
- [ ] Exact Core 31 replacement feerate-diagram and sibling-eviction ordering;
  the supported conservative replacement subset remains interoperable.

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

1. Finish the sustained Bitcoin/Testnet4 public-network soak.
2. Exercise and publish the signed supported-platform release once the native
   signing identities are provisioned.
3. Select P2 work only from an actual deployment need.
