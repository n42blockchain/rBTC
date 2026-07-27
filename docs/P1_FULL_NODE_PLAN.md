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
completes item 3 and the remaining runtime-control portion of item 4. The
owner-only per-data-directory process lock now fails early with bounded
PID/network/start diagnostics and rejects symlink/hardlink aliases, completing
the startup-lock portion of item 4. Item 5 is
also complete: startup and every safe checkpoint boundary enforce an
operator-configured reserve plus worst-case batch/mempool/log/database
headroom, classify exhaustion as a local failure without peer punishment, and
publish the forecast through status, authenticated RPC, and Prometheus.
Upload/index budgets stay open.

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

Implemented foundation on 2026-07-27: typed/CLI/config-file listen controls,
bounded accepting-side v1 handshake, process-global/per-IP/per-network-group admission,
idle/request/upload budgets, active-chain header service, freezer-only
witness/full/compact block service, vetted address samples, live feefilter,
bounded BIP35/transaction intake, and BIP157/158 serving are covered by real TCP tests. The listener is owned by the
node peer pool rather than an active outbound session, so failover atomically
leases a new reconciled read view without rebinding. The independent
AssumeUTXO validator explicitly disables listening. The basic-filter schema
was advanced because the original index omitted the genesis filter and
therefore chained height 1 from the wrong predecessor. P1.2 remains open for
adaptive eviction/preference, routability advertisement controls,
observable per-peer accounting, inbound transaction fan-out, and recorded
Core 31/btcd interoperability evidence.

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

Implemented on 2026-07-27: the configured automatic/operator-selected freezer
ceilings are now persisted only after recovery and physical trimming succeed.
The strict versioned policy refuses unknown future schemas and survives
file/directory-sync interruption without a partial state. Embedded status and
events, authenticated blockchain status, and Prometheus expose both targets,
the retained range, and physical-prefix progress. Item 1 is complete: manual
height-triggered pruning is an offline two-phase plan/token operation, preserves
at least 288 retained-tip blocks, requires a clean audit and unchanged index,
and resumes a versioned intent after any post-publication crash. The freezer
portion of item 3 is also implemented: `--verify-storage` uses fixed memory and explicit work
budgets, refuses concurrent node access, never opens mutable databases, and
returns machine-readable findings plus an ordered, non-executed repair plan.
The cross-store portion is also complete: exclusive `--verify-chain` fully
replays the header graph and correlates the maximum-work active chain,
execution tip, complete freezer audit, and a bounded retained block/undo
suffix. Its one-pass-per-archive decoder uses one block-sized buffer, reports
redb's recovery-capable open semantics, refuses missing stores, and emits no
semantic repair. The local half of item 2 is complete:
`--reindex-from-freezer OUTPUT` requires clean block coverage from height 1 to
the fully replayed maximum-work header tip, ignores the source chainstate, and
rebuilds a separately owned output with batched archive reads, parallel
structure validation, overlapped staging/UTXO prefetch, sorted commits,
crash-resume, and verified promotion. The authenticated-peer half is also
complete: `--reindex-chainstate OUTPUT` pins the fully replayed maximum-work
source header tip, reacquires witness blocks through bounded full-history
peers, applies the normal dual-window and full consensus/script pipeline, and
promotes only an exact verified target. Item 2 is complete.

Item 4 is complete. Every persistent subsystem has an
explicit version in the strict network-bound data-format inventory. A missing
manifest is the only v0 input and migrates atomically to v1 only after legacy
preflight; future/minimum-reader/component mismatches refuse rollback before a
mutable database opens. Tests cover migration, idempotent reopen, future
version preservation, aliases, and failure-before-publication. Backup, restore,
upgrade/rollback, and corruption decisions are published in
[`DISASTER_RECOVERY.md`](DISASTER_RECOVERY.md). Schema changes beyond v1 must
add their forward migration and crash matrix before changing the inventory.

Item 5 is complete. The shared `index_policy` gate models every current and
planned projection by required first height, durable best height, authenticated
history availability, and baseline semantics. Activation refuses missing
history; pruning refuses to overtake a lagging enabled index. Only
explorer/wallet current-state projections may declare an explicit UTXO
baseline. Transaction, spent-output, and BIP158 indexes always require blocks.
All five families are prune-safe only after their own records are caught up,
and the module has no consensus-store mutation path. P1.4 is complete.

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
