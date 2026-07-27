# P1 full-node completion plan

Status date: 2026-07-27.

This plan expands the P1 section of the production roadmap. P0 fast bootstrap,
maximum-work/snapshot validation, data-backed UTXO tiers, and bounded freezer
retention remain invariants. The public Bitcoin/Testnet4 soak and external
review continue independently; P1 work must not weaken or reset either gate.

## Architecture boundary

The node is library-first. `rbtcd` is a thin command-line adapter; the `rbtc`
crate owns configuration, runtime tasks, shutdown, events, and subsystem
interfaces. A host such as `../n42-26` must be able to launch rBTC from its
existing Tokio task executor, retain a cloneable shutdown controller, and await
the node as a critical task without process signals or `process::exit`.

Direct linkage also has a release-policy gate: rBTC is currently
GPL-3.0-only, while `n42-26` declares MIT OR Apache-2.0. The technical API can
be integrated immediately, but distribution of a combined binary requires an
explicit GPL-compatible licensing decision. Until that decision is recorded,
an authenticated local IPC/process boundary is the non-linking deployment
alternative.

The intended module boundaries are:

| Module | Owns | Must not own |
| --- | --- | --- |
| `node` | builder, host task, shutdown, status/events | consensus rules or CLI exits |
| `p2p` | transport, sessions, bounded peer service | chainstate transactions |
| `peer_store` | address reputation and selection | socket task lifetime |
| `chain_store` | atomic UTXO/undo/execution state | network retries |
| `ledger` | immutable compressed blocks and pruning | UTXO identity |
| `indexes` | rebuildable optional projections | consensus truth |
| `api` | bounded authenticated operator surface | unbounded storage scans |

The concrete sibling ownership and critical-task adapter are specified in
[`N42_EMBEDDING.md`](N42_EMBEDDING.md).

## Ordered delivery

### P1.0 — embeddable runtime and configuration

1. Move the daemon runtime into the library and leave a thin binary.
2. Provide `NodeBuilder`, `NodeHandle`, and cloneable `NodeController`.
3. Replace process-global shutdown/checkpoint state with per-instance runtime
   state so more than one network can be hosted in a process.
4. Add bounded typed configuration for peers, DNS, caches, freezer, mempool,
   APIs, AssumeUTXO, and optional modules; keep CLI parsing as one adapter.
5. Publish bounded status/event receivers for peer, header, execution, reorg,
   pruning, index, and failure transitions.
6. Add an external-crate fixture and an `n42-26` task-executor integration
   fixture.

Acceptance: no node task calls `process::exit`, dropping the host controller
cannot expose a partial checkpoint, two isolated regtest instances can run in
one Tokio process, and the standalone CLI passes its unchanged end-to-end
suite.

Implemented and accepted on 2026-07-27: all six items. The public typed
configuration covers every persistent subsystem and shares fail-fast bounds
with the CLI adapter; typed latest-value status and bounded delta events cover
peer, header, execution, reorg, freezer, index, trust, lifecycle, and failure
state. External-crate tests prove two isolated instances and real regtest P2P
observation. The exact sibling fixture uses Reth's real critical-task executor.
Direct distribution of a combined `n42-26` binary remains behind the licensing
decision described above; that policy gate does not reopen the technical P1.0
work.

### P1.1 — operator lifecycle and diagnostics

1. Add a strict network-scoped config file with CLI override precedence and
   unknown-field rejection.
2. Add explicit bounded cache, peer, mempool, freezer, upload, and index
   budgets with startup reporting.
3. Add structured rate-limited rotating logs and host-supplied tracing/event
   sinks.
4. Add graceful RPC stop, runtime log-level changes, startup lock diagnostics,
   and stable node/network/peer/mempool/index/verification status methods.
5. Add startup disk-space forecasting and low-space fail-safe behavior.

Acceptance: config round-trips without secrets, incompatible settings fail
before opening network sockets, log volume is bounded under hostile peers, and
every durable subsystem exposes tip, lag, footprint, and last error.

Implemented on 2026-07-27: item 1 and the peer/mempool portion of item 2.
`rbtcd --config` accepts a strict 64 KiB
Core/btcd-style file with global and network sections, rejects unknown keys and
duplicate scalars, keeps secrets in referenced owner-only files, and applies
explicit CLI option groups last. Typed hot-standby and mempool ceilings now
govern memory, reorg recovery, persistence, and reopen parsing together;
cache/freezer/resource limits appear in the secret-free startup summary.
Embedded subsystem status plus authenticated `getblockchaininfo`,
`getnetworkinfo`, `getpeerinfo`, `getmempoolinfo`, `getindexinfo`,
`verifychain`, and delayed graceful `stop` complete the status/stop portion of
item 4. Standalone diagnostics are now newline-delimited JSON behind a bounded
non-blocking queue, a fixed per-second limiter, owner-only size rotation, and
authenticated `getloginfo`/`setloglevel`; embedded hosts retain typed
status/event receivers and do not install the process-global sink. This
completes item 3 and the remaining runtime-control portion of item 4. Disk
forecasting and upload/index budgets stay open.

### P1.2 — inbound P2P service

1. Add optional `bind`/`listen`, routability advertisement, connection limits,
   per-IP/netgroup limits, handshake timeouts, and self-connection rejection.
2. Separate inbound/outbound/manual peer roles and implement Core/btcd-style
   preferred peers, discouragement, eviction, and upload accounting.
3. Serve bounded `getheaders`, witness blocks, transactions, addresses,
   feefilter, ping/pong, compact-block negotiation, and BIP35 mempool requests.
4. Serve only blocks retained by the freezer and advertise no unavailable
   history; prune state and service bits must agree.
5. Add global/per-peer upload targets, historical-serving throttles, and
   protocol work budgets.

Acceptance: Core 31 and btcd can sync a regtest chain from rBTC, malformed and
slow peers remain memory/CPU/bandwidth bounded, pruning never produces a false
service claim, and outbound-only mode remains unchanged.

### P1.3 — current transaction relay and policy

1. Differentially pin Core 31 relay fee, standardness, dust, script, RBF, TRUC,
   package, orphan, and topology behavior.
2. Complete ancestor/descendant accounting, package relay, orphan request
   budgets, rolling minimum fee, persistence, and reorg resurrection.
3. Relay through both outbound and inbound peers with trickling,
   deduplication, feefilter, wtxidrelay, and bounded fan-out.
4. Validate estimator behavior across restart and reorg against Core fixtures.

Acceptance: consensus and policy remain separate APIs; live Core differential
tests cover every changed Core 27–31 policy family, and adversarial relay tests
prove fixed memory/work ceilings.

### P1.4 — explicit storage lifecycle

1. Persist and expose automatic/manual prune targets and progress.
2. Add `reindex` from a complete freezer and `reindex-chainstate` only when
   required historical blocks are available from authenticated peers.
3. Add bounded offline `verifychain`, freezer checksum verification, repair
   planning, and dry-run output.
4. Version every schema, test forward migrations and rollback refusal, and
   publish backup/restore/disaster-recovery procedures.
5. Define prune/index compatibility before any optional index can activate.

Acceptance: no command silently recreates chainstate from incomplete local
history; every destructive transition has a dry run, crash matrix, durable
cursor, and exact restart result.

### P1.5 — optional indexes

Implement independently rebuildable `txindex`, spent-output index, and
BIP157/158 compact filters in that order. Each module has its own enable flag,
schema/version, tip, lag, disk forecast, prune compatibility, rollback data,
background catch-up budget, and peer/API serving switch.

Acceptance: disabling/removing an index cannot mutate consensus chainstate;
rebuild and reorg results match a clean build byte-for-byte.

### P1.6 — privacy and reachability

Add proxy/`onlynet`, Tor and I2P outbound transports, DNS isolation, bind and
whitebind equivalents, address-network filtering, and tests preventing
cross-network leaks. Automatic port mapping remains optional and off by
default.

Acceptance: each enabled network has an explicit dial and DNS path; disabled
networks produce no socket or resolver traffic.

### P1.7 — operational API completion

Add authenticated raw transaction submission, mempool inspection, block/header
retrieval, UTXO scans, wait-for-tip, peer controls, prune/index status, and
stable error codes. Expensive scans are offline jobs or cursor-paged bounded
tasks rather than synchronous RPC handlers.

Acceptance: the documented API has request/response byte caps, concurrency and
rate limits, authorization audit coverage, cancellation, and fuzzed parsers.

## Completion accounting

P1.0 is complete and is the dependency of every later phase. P1.1 and P1.4 follow, then P1.2,
P1.3, P1.5, P1.6, and P1.7. A phase is checked in the main roadmap only after
implementation, crash/restart tests, interoperability evidence, operator
documentation, and a public-network acceptance run all exist.
