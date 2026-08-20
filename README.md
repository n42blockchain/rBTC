# rBTC

High-performance Rust Bitcoin node kernel, designed around a compact and verifiable UTXO set.

> **Release status:** rBTC is still pre-release and must not hold mainnet private
> keys. Consensus validation, persistent public-network execution, Core 31
> AssumeUTXO, pruned storage, bounded P2P/API services, and the watch-only
> external-signer wallet are implemented. The accepted seven-day public-network
> soak report and first native signed release remain release gates. On
> 2026-08-08 the real Core 31/btcd interoperability acceptance for BIP324 v2,
> inbound v1 handshakes, Tor v3 onion/SOCKS, i2pd SAM, and bounded ZMQ
> publication passed.

### Capability map

| Area | Current capability | Boundary |
| --- | --- | --- |
| Validation | Full header, block, transaction, script, reorg, and atomic chainstate validation on Bitcoin, legacy testnet, Testnet4, Signet, and regtest | Core 31 consensus/policy behavior is tracked; the repository-owned script adapter remains pinned to Core 26's final `libbitcoinconsensus` ABI |
| Fast bootstrap | Core 31 `dumptxoutset` v2 activation, ordinary base-to-tip validation, and independent genesis-to-base replay before finalization | Snapshot selection is explicit and release-pinned; a transport digest is not a trust anchor |
| Storage | redb hot/cold UTXOs, bounded pruned freezer and undo, optional transaction/spent-output/BIP158 indexes, audits, reindex, backup, and recovery procedures | redb is the default production chainstate |
| Snapshot-backed catch-up | Version-2 txid-group MPHF sidecar plus bounded MDBX or redb overlay catch-up, compaction, rebase, and crash-safe swap recovery | Experimental, `--once`-only workflow; the overlay workflow requires `--features mdbx` |
| Networking and services | Bounded outbound failover, optional inbound contribution, persistent mempool, explorer/REST, authenticated JSON-RPC, readiness, and metrics | Loopback APIs remain the default; exact Bitcoin Core RPC parity, mining parity, and hot-wallet signing remain out of scope |
| Wallet and embedding | Watch-only descriptor wallet, unsigned PSBT/external-signature flow, and a library-first multi-instance Tokio API | No daemon-held private keys or in-process signing |

Start with [installation](docs/INSTALLATION.md), validate settings with the
[operator configuration guide](docs/OPERATOR_CONFIG.md), and use the
[architecture](docs/ARCHITECTURE.md), [product maturity](docs/PRODUCT_MATURITY.md),
[roadmap](docs/ROADMAP.md), and [disaster recovery](docs/DISASTER_RECOVERY.md)
documents for implementation detail and release boundaries.

## What is implemented now

- Protocol-compatible Bitcoin P2P v1 message framing through `rust-bitcoin`; no custom wire format. Core 26's 4,000,000-byte message, 256-byte user-agent, 101-hash locator, and 1,000-address response bounds are enforced before unbounded work. Every control or keepalive frame consumes the bounded response budget, pings receive pongs, and a post-handshake `version` is rejected immediately. Modern peers receive Core-ordered BIP339 `wtxidrelay` and BIP155 `sendaddrv2` negotiation before `verack`; bounded `getaddr` decoding supports legacy and IPv4/IPv6 addrv2 responses. One process nonce spans every fallback connection for self-connection detection. Fresh full-history+witness IPv4/IPv6 addresses are quality-filtered into a network-bound, bounded `peers.redb` fallback pool. A persistent random secret assigns learned addresses across 1,024 keyed new buckets and successful handshakes across 256 keyed tried buckets, each capped at 64 entries; old stores generate the secret atomically on first reopen. Pool updates physically prune stale entries, prefer peers that completed prior synchronization sessions over handshake-only records, use known lower successful-handshake latency and then higher completed block-response throughput as tiebreakers within equal reputation, and round-robin both keyed buckets and target `/16` IPv4 or `/32` IPv6 groups so one range cannot monopolize a startup set. Objective wire violations and invalid headers/blocks from learned peers enter a separately bounded, persistent one-hour-to-one-day cooldown; ordinary transport failures do not, a completed synchronization session clears it, and manual connections remain exempt. Public bootstrap uses the network-specific [Bitcoin Core 31 seed list](https://github.com/bitcoin/bitcoin/blob/v31.0/src/kernel/chainparams.cpp), resolves seeds concurrently under per-seed/global bounds, distributes candidates across seed responses, and never admits private, reserved, or actively discouraged public-network results.
- Opt-in BIP324 v2 encrypted transport for outbound peers (`--v2-transport`
  CLI flag or `v2_transport` config key, default off). The ElligatorSwift
  X-only ECDH, network-magic-bound HKDF-SHA256 key schedule, and rekeying
  FSChaCha20/FSChaCha20Poly1305 record ciphers pass the official BIP324
  packet-encoding vectors, including the 224-message rekey boundaries. A
  sans-I/O handshake state machine bounds garbage (4,095 bytes), decoy
  packets, and per-packet contents, recognizes the v1 prefix on the responder
  side, and fails closed on every protocol violation. Outbound connections
  prefer v2 when enabled and retry an address exactly once over v1 after the
  peer closes the v2 attempt, as BIP324 specifies; `version`/`verack`
  negotiation runs unchanged through either framing, and v2 sessions reuse
  the identical 4,000,000-byte message ceiling and misbehavior
  classification. Ignored `RBTC_BITCOIND` integration tests exercise both the
  encrypted session and the v1 fallback against a real Core 31 daemon, and a
  dedicated fuzz target drives the deterministic responder handshake and
  record layer.
- Minimal local block assembly (`block_assembly`) for low-difficulty
  networks: template-driven coinbase construction with the validator's exact
  BIP34 height encoding, always-present segwit witness commitments, subsidy
  plus declared fees, and a bounded 32-bit nonce search. Regtest pipeline
  tests produce their blocks through this module instead of an external
  daemon; it is deliberately not a mining template provider.
- Optional loopback ZMQ notification endpoint (`--zmq-listen` or the
  `zmq_listen` config key) for indexers and Lightning nodes. The narrow
  ZMTP 3.x subset a PUB socket needs — NULL security, READY negotiation
  limited to SUB/XSUB peers, both subscription encodings — is implemented in
  bounded safe Rust rather than binding `libzmq`. Notifications follow
  Core's wire contract (`hashblock`/`hashtx` in display byte order,
  `rawblock`/`rawtx` consensus bytes, `sequence` labels with mempool
  sequence numbers) and are emitted per connected block at the batch commit
  point, per disconnected stale block, and per newly admitted mempool
  transaction; the independent AssumeUTXO validation chain never publishes.
  Distribution runs over one bounded queue whose slow subscribers lose
  counted messages instead of growing node memory. `R` sequence labels
  cover expiry, BIP125 replacement, and capacity eviction, while
  block-confirmed removals stay silent exactly like Core; non-loopback binds
  are refused because the endpoint is unauthenticated.
- Authenticated `testmempoolaccept` reports Core-shaped dry-run admission
  verdicts (`txid`, `wtxid`, `allowed`, `vsize`, `fees.base_sats`,
  `reject-reason`) for a bounded package of at most 25 candidates. The
  evaluation runs the ordinary consensus and relay-policy admission path
  against the current chainstate and mempool on a throwaway pool clone, so no
  candidate is retained, persisted, or relayed on any path — the live pool is
  byte-for-byte unchanged whether the package is accepted or refused.
  Rejection reasons are the admission error truncated to 256 bytes.
- Authenticated `rbtc.scanchainstate` walks the active UTXO set in bounded
  cursor pages of at most 1,000 entries, reusing the chainstate's existing
  fixed-memory paging primitive rather than materializing the set. Each page
  returns the coin's outpoint, value, height, coinbase flag, and script, plus
  a `next_cursor` that is present only when the page filled completely, so an
  exhausted walk terminates without an extra probe.
- Authenticated `listbanned` and `setban ADDRESS add|remove [SECONDS]`
  administer local peer cooldowns durably through the existing peer store. A
  cooldown makes an address an ineligible outbound candidate and refuses its
  inbound connections; it never touches consensus state, and the automatic
  objective-violation cooldown keeps applying independently. Durations are
  bounded by the same one-day ceiling, default to 24 hours, and never shorten
  an active cooldown. Because the stored record requires at least one recorded
  violation, a manual cooldown on an otherwise clean address occupies the
  first escalation step, and removing it discards that again.
- Outbound v3 onion destinations through the existing fail-closed SOCKS5
  proxy. `OnionAddress` validates the base32 alphabet, the 62-character
  length, the version byte, and the address's own SHA3-256 checksum before a
  name can reach a proxy or a store, and it round-trips to and from the
  service public key. The proxy request uses SOCKS5 domain addressing, so the
  name is resolved inside the anonymity network and this host performs no DNS
  lookup for it; the local `version` then advertises an unspecified receiver
  address rather than a routable socket. BIP324 preference and the one-shot
  v1 retry apply unchanged. Learned onion services persist in their own
  bounded address book: `addrv2` TorV3 entries are retained apart from
  routable addresses, deduplicated, and stored in a separate table with an
  independent 1,024-entry ceiling, the same full-history/witness service
  requirement, terrible-entry hygiene, retry backoff, and discouragement
  checks. Keeping the books separate means an onion flood can neither
  displace routable peers nor inherit IP-range diversity rules that do not
  apply to it. `--onlynet onion` restricts outbound work to onion
  destinations and fails closed without `--proxy`, because an onion-only node
  has no other route; mixing onion with an IP family is refused rather than
  silently widened, and eligible onion candidates are reported at selection.
  The outbound scheduler now carries address-type-aware targets, so persisted
  onion candidates are selected, dialed through the proxy, and ranked in the
  same ordered wave as routable peers. Bookkeeping that is only meaningful for
  a routable address is skipped rather than given a fabricated one: an onion
  peer contributes no network-time sample (those are grouped by IP range), is
  not offered to address discovery, does not enter the socket-keyed
  tried-collision or discouragement tables, and records its attempts and
  successes in the onion book instead. A proxyless onion destination is
  refused as a local configuration fault, never as a peer failure. Hosts
  observe every peer through the new `PeerTargetConnected`/
  `PeerTargetDisconnected` events; the socket-typed `PeerConnected`/
  `PeerDisconnected` events and the socket-typed status field are unchanged
  and continue to report routable peers only. I2P remains open.
- `--torcontrol IP:PORT --torcontrol-cookie PATH` publishes an inbound onion
  service forwarding to the `--listen` socket, which the configuration
  requires; a non-loopback control port and a half-supplied pair are refused
  before any database or network open. The generated private key is stored
  owner-only inside the data directory and replayed on the next launch, so
  peers that learned the address keep reaching this node across restarts; it
  is never logged. The service is published without
  `Detach`, so Tor destroys it when the control connection closes, which
  happens unconditionally when the node stops; an explicit `DEL_ONION` is
  attempted first to make the withdrawal immediate, but it is best effort
  because a task spawned during shutdown may never run. The
  published address is then announced to peers that negotiated BIP155, both
  on outbound sessions and in the inbound address relay, and never to a
  legacy `addr` peer, because an onion address has no legacy encoding and
  must not be replaced by a substitute.
- A Tor control-port client publishes and withdraws one ephemeral v3 onion
  service. It speaks only the needed subset — `PROTOCOLINFO` discovery,
  `SAFECOOKIE` challenge-response, `ADD_ONION` with a fresh ED25519-v3 key,
  and `DEL_ONION` — and refuses a non-loopback control port, because reaching
  that port is equivalent to controlling the host's Tor instance. The cookie
  is read only to compute the challenge and never sent; the control port's
  own proof is verified before this node discloses its proof, so a port that
  does not already hold the cookie learns nothing usable. Reply lines and
  continuation counts are bounded, and the returned address is re-validated
  through the same v3 checksum rules as a learned address.
- An I2P SAM v3 client covering the bridge subset a node needs: `HELLO`
  version negotiation, one long-lived `SESSION CREATE` STREAM session whose
  destination key can be persisted and replayed to keep a stable published
  address, and `STREAM CONNECT` on a separate socket per outbound peer, which
  hands back an ordinary stream the existing v1 or BIP324 handshake drives
  unchanged. Addresses use BIP155's I2P form — the 32-byte SHA-256 of the
  destination as a 52-character base32 `.b32.i2p` name — validated
  structurally before reaching a bridge or a store. The bridge is refused
  unless it is loopback, because it can open streams on this node's behalf,
  and session identifiers, destination keys, and reply lines are all bounded.
  Reply lines are read one byte at a time rather than through a buffered
  reader: a router may coalesce a reply with whatever follows it, and after
  `STREAM CONNECT` what follows is the peer's own traffic, so a reader that
  read ahead would either swallow the next reply or silently consume the
  first bytes of the Bitcoin stream.
  Learned I2P destinations follow the same path as onion services:
  `addrv2` I2P entries are retained apart from routable and onion addresses,
  deduplicated, and stored in a third peer-store table with its own
  1,024-entry ceiling, service requirement, hygiene, and retry backoff, so no
  network can displace another. Attempt and success bookkeeping dispatches to
  that book. `--i2psam IP:PORT` names the loopback bridge, refused on any
  other address because it opens streams on this node's behalf; its session
  key is stored owner-only and replayed so the published destination survives
  restarts. Persisted I2P candidates then join the same ordered outbound
  wave, dialled through `STREAM CONNECT` and handed to the ordinary v1 or
  BIP324 handshake. `--onlynet i2p` restricts outbound work to that network
  and fails closed without a bridge. Both anonymity-only restrictions also
  short-circuit DNS seed resolution entirely: running the lookups and then
  discarding every answer would still leak which Bitcoin seeds the node
  consults, which is the leak the restriction exists to prevent. A SOCKS5 proxy is never substituted for
  the bridge — an I2P target has no proxy representation at all — because
  that would connect to the wrong network. The published destination is announced to
  BIP155 peers on outbound sessions and in the inbound address relay, with a
  zero port because I2P peers carry none. Inbound `STREAM ACCEPT` reports the dialling
  peer's Destination alongside the stream, so an accepted peer is recorded
  and ranked like a learned one; the status and destination lines are read
  line-exactly because the very next byte already belongs to the peer. The
  SAM session identifier is derived from the network and data directory
  rather than fixed, so two nodes on one host do not collide with
  `DUPLICATED_ID`. The accept loop runs inside the existing
  inbound service, so I2P and routable peers share one global connection
  semaphore, upload budget, and statistics rather than forming a second,
  independently sized service. The per-source and per-group ceilings cannot
  apply to a peer with no IP, so a separate eight-peer ceiling stands in for
  them, and a small number of accepts is kept outstanding at the bridge
  because awaiting one inline would drop a half-open SAM socket whenever
  another intake won the select. A failing accept retires the I2P intake
  without stopping the TCP one.
- Header batches are validated through an in-place rollback guard and become
  visible only after their durable store append succeeds. Ordinary 2,000-header
  extensions retain `O(batch)` hashes instead of deep-cloning the complete
  mainnet DAG on every response; validation or persistence failure removes the
  staged suffix, while the rare stronger-side-chain rollback reconstructs the
  former active vector from its retained tip.
- A successful peer that hashes to an occupied tried `(bucket, slot)` is retained in new instead of evicting the incumbent immediately. Up to ten challenger/incumbent pairs persist atomically; the next startup probes incumbents ahead of ordinary persisted candidates, retaining a live incumbent or promoting the strongest challenger only after a failed handshake. Legacy stores infer their existing successful records as tried without rewriting them.
- BIP152-capable peers receive a witness-aware version-2 `sendcmpct` preference with high-bandwidth announcements disabled. Compact-block transaction-reference vectors are capped at the consensus-derived 16,666-transaction maximum before routing. Negotiated block downloads reconstruct differential prefilled positions, match unique wtxid short IDs against caller-provided candidates, request only missing indexes with `getblocktxn`, and fall back to a full witness block after a Merkle/witness mismatch.
- Script validation adapter using Bitcoin Core's `libbitcoinconsensus`, including Taproot spent-output and default/custom-Signet BIP325 block-solution validation. The pinned Core 26 library has a transaction-level batch ABI so one transaction is decoded and its shared signature-hash data is precomputed once for all inputs.
- Block script checks use a persistent, bounded host-CPU worker pool after
  ordered prevout and UTXO resolution. Large checkpoints bulk-enqueue
  16-transaction work packets, removing per-transaction queue-lock and result
  channel contention while retaining dynamic load balancing; small jobs remain
  serial, and no block state is committed until every script succeeds.
- Pure-Rust redb chainstate with hot/cold UTXOs, per-block undo, and execution tip committed together in one physical database transaction; IBD supports multi-block durable checkpoints.
- Deterministic zstd UTXO snapshots with bounded-memory two-pass import, in-transaction SHA-256/count verification, mandatory maximum-work active-header anchors, atomic publication, and an AssumeUTXO-style background-validation contract. A separate bounded-memory Core 31 `dumptxoutset` v2 loader uses compiled AssumeUTXO identities and Core's exact UTXO-set hash. Explicit HTTPS sources can be downloaded as bounded 64 MiB ranges with 1–8 workers, checkpoint restart, exact-length enforcement, and atomic publication; a real external Core 31 Testnet4 v2 file passes activation.
- Offline `--build-core-snapshot-index SNAPSHOT --snapshot-index-output FILE` keeps a Core 31 snapshot such as `utxo-935000.dat` as an immutable compressed data source instead of expanding it. The current version-2 sidecar uses a safe-Rust BBhash minimal perfect hash function over txid groups and a bit-packed 34-bit group-offset table. A hit verifies the queried txid and vout against the source bytes before decoding Core's VARINT/script-template record, and the recorded maximum group span bounds every read. Building re-authenticates the release-pinned Core UTXO-set hash. On the real height-935,000 mainnet snapshot, 164,241,311 coins form 113,879,165 groups; the 18-level sidecar is 530,926,239 bytes, down from the former 1,155,791,488-byte outpoint-keyed format.
- The `mdbx` feature adds a snapshot-backed overlay chainstate on that base. Post-base coins, spent-base tombstones, compressed per-block undo, and the execution tip commit atomically behind the same consensus executor used by redb. Both overlay engines understand legacy raw undo records; new `RUZ1` zstd records have a 256 MiB decompression ceiling and an 8 MiB zstd window ceiling. MDBX enforces a hard geometry budget (10 GiB by default), can reclaim copy-on-write garbage through a compact copy, and rebases only when folding the overlay into a new authenticated snapshot is required; redb uses its native compaction with a policy-enforced budget. Rebase and compaction directory swaps restore one interrupted set-aside copy, fail closed without guessing if two candidates exist, and clear stale copies before a new swap. Bounded `--snapshot-overlay-catchup SNAPSHOT --snapshot-overlay-index INDEX --once` reuses ordinary headers-first download, staged/prefetched execution, stale-tip disconnection, and undo pruning. The default atomic validation batch is 256 blocks and can be lowered with `--validation-batch-size`. See [the architecture measurements](docs/ARCHITECTURE.md) for engine comparisons and memory/I/O trade-offs.
- Reorg-consistent spent-output ages are aggregated before sorted chainstate
  writes. Offline `--utxo-activity-report` scans current UTXOs in fixed-size
  pages and compares candidate block-age windows against historical spend-hit
  rates, spend-age quantiles, and estimated two-tier lookup amplification;
  incomplete history can be inspected but cannot produce a recommended
  hot/cold boundary. Once selected,
  `--retier-utxos-window-blocks BLOCKS` applies it offline through sorted
  65,536-record atomic batches with durable restart progress.
- Immutable zstd block archives with 4 MiB piece hashes, authenticated uncompressed-length limits, and legacy-v1 read compatibility, ready for a BitTorrent/webseed transport adapter.
- Configurable circular pruned ledger: defaults are 1,008 blocks (about one week) and 1 GiB; validating-node launches may lower the block window to an explicit operator floor of 288 with `--prune-blocks`, or set a byte target of at least 550 MiB with `--prune-max-bytes`. Applying lower startup targets immediately rotates and physically removes excess segments before catch-up, then atomically publishes the versioned `ledger-policy.json`; an unknown future policy version is never overwritten by an older binary. Status, RPC, Prometheus, and embedded events expose the configured ceilings, retained range, and highest pruned prefix. Validated IBD batches are published through a restart-safe staging protocol; archive-slot renames are directory-synced before their indexes are published, then slots retired by the durable index are physically removed and the directory is synced. Block undo older than the ledger floor is removed in one sorted atomic transaction only after every hash resolves through the authenticated header DAG. UTXO state and headers remain, so long IBD runs do not accumulate logically pruned archives or obsolete disconnect records.
- `--verify-storage` is an exclusively offline, read-only freezer audit. It takes the existing data-directory lock without rewriting its marker, refuses to run beside the node, verifies compressed pieces plus decompressed record hashes/framing with fixed memory, and emits bounded JSON findings and an ordered dry-run repair plan. Explicit segment/byte budgets make an incomplete audit fail visibly; it never opens or creates chainstate, indexes, or file logs.
- `--verify-chain` is the exclusive cross-store companion. It requires an existing versioned directory, fully replays every stored header through PoW/difficulty/checkpoint rules, then correlates the active header, execution tip, complete freezer audit, and 1–1,008 retained block/undo suffix under an explicit decompressed-byte budget. Archive payloads are streamed once per segment with one consensus-sized block in memory. Its JSON states that redb open is recovery-capable; it never creates missing consensus stores or performs a semantic repair.
- `--reindex-from-freezer OUTPUT` independently rebuilds chainstate into a separate owned directory only when the source freezer cleanly covers every active-chain block from height 1 through the fully replayed maximum-work header tip. Archive ranges are read once per aggregate batch; structure checks run in parallel, immutable staging overlaps sorted UTXO prefetch, and consensus/script execution remains authoritative. A crash resumes from the durable execution tip, ordinary service refuses the incomplete output marker, and promotion occurs only after bounded cross-store verification and exact target completion. It never trusts or opens the source chainstate and never overwrites the evidence directory.
- `--reindex-chainstate OUTPUT` is the pruned/corrupt-source counterpart. It fully replays the source headers, pins their maximum-work tip as an immutable height/hash ceiling, copies only that authenticated active header chain, and downloads required witness blocks from bounded full-history peers into a separate directory. The normal parallel block-window, staging/prefetch, sorted chainstate, disk fail-safe, crash-resume, and full consensus/script pipeline remains authoritative; transport hashes do not authenticate UTXOs. The output marker is removed only after exact-tip cross-store verification.
- Manual freezer pruning is two-phase and offline: `--prune-through-height HEIGHT` prints a non-mutating plan token, while repeating it with `--apply-prune-token TOKEN` first requires a clean full freezer audit and an unchanged index. Only complete archive segments are selected, at least 288 retained-tip blocks remain, the new index becomes durable before physical deletion, and a versioned intent resumes every post-publication crash.
- Prune/index compatibility is a shared code-level gate, not an option-name convention. Every projection declares its required start height, durable tip, whether a validated UTXO baseline is meaningful, and whether older authenticated blocks are available locally or from full-history peers. Activation refuses an unavailable build prefix; physical pruning refuses to overtake a lagging enabled index. Explorer and wallet projections may use their explicit current-state/birthday baselines, while tx, spent-output, and BIP158 indexes never pretend a UTXO snapshot reconstructs historical records. Once fully materialized, all projections are independent of old block-file retention and remain outside consensus chainstate.
- `--txindex`, `--spent-output-index`, and `--block-filter-index` enable three network-bound databases with independent schema identities, tips, crash rollback, and rebuild state. A validation window is committed once per index with lexicographically sorted keys rather than once per block. After chainstate commits, independent explorer/index transactions run concurrently with the wallet projection and all join before freezer publication. Transaction lookup preserves and restores BIP30 overwrites; spent-output lookup records the active spender; the compact-filter store builds BIP158 basic filters from the exact executed spent-prevout scripts and chains BIP157 headers. Startup rewinds stale tips and catches up against the maximum-work header chain; transaction history may be reacquired from a full-history peer, while spent-output/filter recovery refuses if the required execution undo has already been pruned. Use `--reindex-chainstate OUTPUT` (or a complete freezer) with the desired flags for that case. Rollback rows are physically removed below the freezer floor, manual and automatic pruning both refuse to strand lagging indexes, and disk preflight reserves an additional worst-case copy-on-write window per enabled index. Authenticated RPC exposes sync state plus bounded `gettxindexlocation`, Core-style `gettxspendingprevout`, and `getblockfilter` queries.
- An optional `--listen IP:PORT` service accepts bounded Bitcoin v1 peers without changing outbound-only defaults. A process-wide listener survives active-outbound failover while an atomic lease exposes only the current fully reconciled maximum-work execution view; the independent AssumeUTXO validation chain never inherits or competes for the port. Handshake, idle, global/per-IP/per-network-group, request-rate, transaction-intake, response-vector, relay-announcement, and rolling historical-upload ceilings are explicit. It serves active headers, only freezer-retained witness/full/compact blocks, bounded mempool transactions, vetted address samples, the live rolling feefilter, and enabled BIP157/158 filters. Peer transactions enter the same durable consensus/policy admission path as outbound traffic. Transactions which survive that path are announced to other accepted peers with negotiated txid/wtxid inventory, their BIP133 fee threshold, fixed-size duplicate suppression, and non-blocking lag handling. Headers/control traffic and the recent 288 blocks remain available after the historical-block upload target is reached.
- Every persistent node directory carries a strict owner-only `.rbtc-data-format.json` inventory binding the network, root/minimum-reader version, and each subsystem schema. A legacy directory receives v1 only after its existing preflight succeeds; future or mismatched versions fail before mutable database open and are never downgraded. Stopped-node backup, restore, upgrade/rollback, and failure decisions are documented in [docs/DISASTER_RECOVERY.md](docs/DISASTER_RECOVERY.md).
- Library-first runtime integration: `rbtcd` is a thin command-line adapter over `rbtc::node`. A Tokio host can configure ordinary persistent nodes with the complete bounded `NodeConfig`/`NodeBuilder` surface, retain a cloneable `NodeController` for each, and await each `NodeHandle` as a critical task without installing process signal handlers. Shutdown and checkpoint barriers are instance-scoped. Latest-value lifecycle and peer/header/execution/index/freezer/trust/error status plus a 32-entry typed delta stream remain available after the wait future moves into a critical-task executor; a slow observer resamples status instead of growing node memory. External-crate tests run two isolated regtest nodes in one Tokio runtime and drive a real P2P handshake through execution/freezer observation. An exact `../n42-26` fixture moves the wait future into Reth's real `TaskExecutor`. The technical P1.0 embedding surface is complete; GPL-compatible combined-distribution policy remains a separate release gate.
- Optional embedded block-explorer UI and REST API, an authenticated bounded read-only JSON-RPC route, plus an optional authenticated, transactionally persisted BDK watch-only descriptor wallet panel/API. The historical explorer projection is maintained only when a loopback API listener is explicitly configured, so ordinary validation does not pay for an unused full transaction index.
- Standalone newline-delimited JSON diagnostics use a bounded non-blocking queue, per-second admission limit, owner-only size-rotated files, and authenticated runtime level control; embedded hosts retain typed status/event receivers without a process-global logger.

## Important safety status

rBTC is **not yet a production release** and must not be trusted with mainnet funds. Mainnet genesis-to-tip validation, ordinary persistent Bitcoin/legacy-testnet/Testnet4/Signet/regtest execution, outbound peer management, bounded optional inbound contribution, current Core 31 relay-policy bounds, explicit storage/index lifecycle, authenticated operator APIs, persistent explorer projections, and crash-safe watch-only/external-signer wallet flows are implemented. Testnet4 and Mainnet Core 31 AssumeUTXO acceptance, the data-backed Mainnet hot/cold boundary, the repository-owned script boundary's Core 31 compatibility/live differential matrix, P1 full-node scope, and external audit integration are complete. The remaining release blockers are an accepted seven-day public-network operations soak report and exercising a signed supported-platform release with provisioned native identities. The exact scope and acceptance gates are in [docs/ROADMAP.md](docs/ROADMAP.md); the `../n42-26` host ownership, tested task-executor fixture, and licensing boundary are in [docs/N42_EMBEDDING.md](docs/N42_EMBEDDING.md).

### Supported platforms

Unix is the primary target, but CI links and runs the full test suite on `windows-latest` as well, because several defect classes are structurally invisible on Unix — directory `fsync`, `#[cfg(unix)]`-gated no-ops, and file-lock error classification have each regressed at least once. Consensus, storage atomicity, and abrupt-kill crash recovery are verified on every platform, but three hardening measures rely on APIs the Rust standard library exposes only on Unix, and are documented no-ops elsewhere:

- The mempool, rebroadcast, and fee-estimator databases are created `0600` on Unix. On Windows they inherit their directory's ACL, so the data directory itself must restrict access.
- Directory `fsync` after an atomic rename — used by snapshot publication, block-archive slot rotation, and the authorization audit log — has no portable Windows equivalent. Renames remain ordered, but the directory entry is not explicitly flushed.
- The authorization audit log's permission, hard-link, and device/inode identity checks are Unix-only, so a Windows audit log is not revalidated for substitution across a reopen.

Findings and per-platform verification status are recorded in [docs/AUDIT.md](docs/AUDIT.md).

## Design choices

| Concern | Choice | Reason |
| --- | --- | --- |
| Bitcoin types and v1 P2P encoding | `rust-bitcoin` | Maintained Rust Bitcoin primitives and consensus serialization. |
| Script interpreter | `bitcoinconsensus` | Repository-owned Core v26.0 boundary, the last release line shipping `libbitcoinconsensus`; includes the Taproot spent-output API. |
| Consensus rules | tracked through Core v31.1 | Public-network rules include Testnet4/BIP94; Core's default regtest keeps BIP94 disabled and uses its 144-block interval. The interpreter pin and tracked rules are separate decisions. |
| UTXO persistence | redb default; MDBX is the leading gated replacement candidate | redb remains the recovery-proven daemon default. `--features mdbx` adds a versioned four-table chainstore with 34–37-byte order-preserving vout keys, Core/btcd compact coins and undo, height-only hot/cold placement, one-view batch reads, atomic UTXO/undo/tip commits, a 128 GiB hard geometry ceiling, and recoverable compact copy. The [2026-08-20 evaluation](docs/STORAGE_ENGINE_EVALUATION_2026-08-20.md) measured substantially faster writes and lower space at 2M coins, but requires mainnet-scale churn/crash evidence before daemon selection or migration is enabled. |
| Wallet | BDK (`bdk_wallet`) | Descriptor, PSBT, coin selection, signing, and sync model without reimplementing wallet correctness. |
| Compression | zstd | Fast decompression and high ratio for snapshots and static block segments. |

## Operator configuration

`rbtcd --config PATH` loads a strict Core/btcd-style `key=value` file with
global and per-network sections, a 64 KiB input ceiling, unknown-key rejection,
and deterministic CLI override precedence. Negative CLI forms such as
`--no-once` and `--no-mempool-full-rbf` allow an explicit command line to
override enabled file booleans. Credentials and descriptors remain in their
owner-only files rather than entering configuration. See
[docs/OPERATOR_CONFIG.md](docs/OPERATOR_CONFIG.md) for the schema and example.
Use `rbtcd --check-config --config PATH` to validate and print the complete
secret-free effective configuration without creating storage or connecting a
peer. Installation, signed-artifact verification, and first-start checks are in
[docs/INSTALLATION.md](docs/INSTALLATION.md). Product release status is
summarized in [docs/PRODUCT_MATURITY.md](docs/PRODUCT_MATURITY.md), and private
vulnerability reporting is defined in [SECURITY.md](SECURITY.md).
The measured storage, traffic, UTXO-tier, maximum-work, and AssumeUTXO
requirements for a future mobile validating mode are assessed in
[docs/MOBILE_FULL_NODE_FEASIBILITY.md](docs/MOBILE_FULL_NODE_FEASIBILITY.md).

## Local checks

```bash
cargo fmt --check
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo test --locked --all-features
cargo llvm-cov --locked --all-features --fail-under-lines 90
cargo audit --deny warnings
cargo deny check
cargo build --locked --release
target/release/rbtcd --version
target/release/rbtcd --help
scripts/verify-reproducible-build.sh
scripts/public-network-sync-smoke.sh
RBTC_FUZZ_RUNS=10000 scripts/run-fuzz-regression.sh
cargo +nightly-2026-07-13 miri test --lib merkle_proof::tests::verifies_left_and_right_transaction_positions
RBTC_BITCOIND=/path/to/bitcoin-core-31/bin/bitcoind cargo test --release --test core_block_differential -- --ignored --nocapture
RBTC_TOR_CONTROL=127.0.0.1:9051 RBTC_TOR_COOKIE=/path/to/control_auth_cookie RBTC_TOR_SOCKS=127.0.0.1:9050 cargo test --release --all-features --test anonymity_network_interop -- --ignored --nocapture
RBTC_I2P_SAM=127.0.0.1:7656 cargo test --release --all-features --test anonymity_network_interop -- --ignored --nocapture
cargo test --release --all-features --test storage_bench -- --ignored --nocapture
cargo test --release --all-features --test storage_engine_comparison -- --ignored --nocapture
```

The storage benchmark generates its block-shaped UTXO population at runtime and
reports machine-readable JSON; no generated database, snapshot, or result is
versioned. It covers redb, MDBX, and a benchmark-only SQLite store — SQLite
being the one surveyed alternative that offers both an engine-enforced size
ceiling (`PRAGMA max_page_count`) and in-place compaction (`VACUUM`), and
already linked into every build through `bdk_wallet`. At a 2,000,000-UTXO
workload its point lookups measured 4,053 ns against redb's 1,584 ns and
MDBX's 2,353 ns, with a markedly worse p99 (10.3 µs against 5.2 and 3.8), so
it is evaluated and recorded rather than adopted; see `docs/ARCHITECTURE.md`. Set `RBTC_BENCH_BLOCKS`, `RBTC_BENCH_UPDATES_PER_BLOCK`,
`RBTC_BENCH_UTXOS`, and `RBTC_BENCH_LOOKUPS` to scale the bounded workload, and
set `RBTC_BENCH_REPORT` to retain the JSON report. The manual Storage benchmark
workflow uploads that report together with runner CPU, filesystem, and block
device metadata so NVMe and HDD runs can be compared without confusing runner
differences with backend results. redb results also measure explicit offline
compaction, report file sizes before and after it, and reopen the compacted
chainstate to verify the execution tip before accepting the result. The same
workflow runs `RBTC_BENCH_IBD_BLOCKS` generated regtest blocks through the
production v1 handshake, headers-first download, script execution, atomic
chainstate, ledger, and explorer path and retains a separate JSON report.
The matched `storage_engine_comparison` gate instead compares the complete
redb and MDBX UTXO/undo/tip transaction and includes a separate
`contrib/btcd_storage_bench` lane for btcd's key/coin codec over pinned Go
LevelDB, Pebble, Badger, and bbolt versions. The mutation path requires one
synchronous atomic UTXO/undo/tip transaction; engines that cannot fit the
256-block transaction are reported as rejected rather than silently split.
Its serial one-hour matrix targets 900,000 deterministic transitions and
records the actually completed count per lane. It explicitly does not call
that storage-only lane a btcd IBD or a height-900,000 mainnet replay result. In
the 56.2-minute Mac run no lane reached the target: bbolt led successful
serving and IBD-256 throughput, Badger rejected the required atomic IBD
checkpoint, and Pebble's sustained IBD result reversed its short-run lead.
Method, three-round Mac medians, timebox results, limitations, and the
default-engine decision are recorded in
[docs/STORAGE_ENGINE_EVALUATION_2026-08-20.md](docs/STORAGE_ENGINE_EVALUATION_2026-08-20.md).
MDBX replacement preparation now includes verified four-table compact-copy,
an abrupt-process recovery matrix, free-space and anti-thrashing policy, and a
resumable 160M/900,000 churn/RSS runner. The completed and still-external gates
are separated in [docs/MDBX_REPLACEMENT_GATE.md](docs/MDBX_REPLACEMENT_GATE.md);
MDBX remains opt-in until the full-scale and real-block evidence passes.

The repository keeps only reviewed, human-named fuzz seeds and minimized
crash/hang regressions. Coverage discoveries with cargo-fuzz's 40-character
hash names remain local and are ignored rather than accumulated in commits.
Because `fuzz/` is an independent Cargo workspace, it explicitly patches
`bitcoinconsensus` and `redb` to the same reviewed vendored trees as the root
workspace. CI checks its lock file before running every target under the dated
`nightly-2026-07-13` toolchain; a local override must likewise name an exact
dated nightly through `RBTC_FUZZ_TOOLCHAIN`.

The optional anonymity-network gate is separate from every other Tor and
I2P test in this repository, all of which run against in-process mocks and
therefore pass without any daemon installed. It exercises the real
protocols end to end: the Tor case authenticates against a live control
port, publishes an ephemeral service, waits for its descriptor to reach the
hash ring, dials that service back through the SOCKS5 port, and completes a
Bitcoin handshake plus a ping exchange over the resulting circuit before
withdrawing the service — so a control-port, SOCKS5-addressing, or
descriptor-timing divergence fails visibly rather than passing against a
mock. The I2P case creates a SAM session on a live router, checks the
published destination is BIP155-shaped, republishes the same address from a
stored key, and confirms an unreachable destination fails within its
deadline instead of hanging. A two-node case then runs both halves against
the router: two sessions must coexist on one bridge, one accepts while the
other dials it, and each side must receive the other's destination in
`addrv2`. Setting `RBTC_I2P_SAM_B` runs the two nodes against separate
bridges instead. A further case dials the inbound service itself rather than
a bare session, so the listener's select, shared connection semaphore,
upload budget, and statistics are exercised on the I2P path too; it asserts
the peer is accounted as accepted and handshaken and rejected by no ceiling
keyed on an address it does not have. The cases that publish to an
anonymity network take a shared lock so they run one at a time: a router
publishes each destination's LeaseSet and builds its tunnels before it is
reachable, and asking one daemon to do that for every case at once produced
`LeaseSet not found` when the suite ran as a whole while each case passed
alone. Each test names the environment variables it
requires and fails with that message when they are absent, so a partially
configured run reports what is missing rather than silently passing.

The optional live differential gate requires the matching `bitcoin-cli` beside a Bitcoin Core 31.0 `bitcoind` and rejects another daemon version. It submits the same mined regtest blocks to Core and through rBTC's production header-DAG/block-connection path, including atomic rejection checks for the persisted tip, undo record, and candidate UTXO. The dependency and Core 27–31 change classification is documented in [docs/CORE31_COMPATIBILITY.md](docs/CORE31_COMPATIBILITY.md).

The weekly/manual public-network smoke gate authenticates and continuously
executes default-Signet blocks 1 through 1,000 and mainnet blocks 1 through
Core 26 checkpoint height 295,000 using the production P2P/IBD/storage path.
That mainnet range includes both historical BIP30 duplicate-transaction
exceptions, the BIP16 exception and P2SH activation, the first subsidy halving,
and BIP34 activation. Signet defaults to a 1 GiB data ceiling, ten-minute
deadline, and one-block execution batches; mainnet defaults to 40 GiB, two
hours, and 252-block high-memory atomic persistence batches filled through
bounded 16-block peer requests.

After observing mainnet block 1,000, the harness deliberately terminates the
process; the current atomic batch may finish before the signal arrives, and
every completed batch explicitly yields so a fully prefetched successor cannot
starve shutdown delivery. A new process must reopen that exact durable state
and stop at the target.
`RBTC_SYNC_RESTART_HEIGHT=0` disables this check or another below-target height
selects it. Both networks reserve another 2 GiB of free space, clean temporary
data on every exit, and accept `RBTC_SYNC_MAX_BYTES`,
`RBTC_SYNC_TIMEOUT_SECONDS`, `RBTC_SYNC_FREE_RESERVE_BYTES`, and
`RBTC_SYNC_BATCH_SIZE` overrides. `RBTC_SYNC_NETWORK` selects `signet` (the
default) or `bitcoin`. A deeper authenticated endpoint can be supplied only as
the explicit `RBTC_SYNC_TARGET_HEIGHT` and `RBTC_SYNC_TARGET_HASH` pair. Set
`RBTC_KEEP_SYNC_DATA=1` only when the bounded test directory is needed for
inspection.

On 2026-07-23 the first height-105,000 restart run completed in 2,350 seconds
using 833,470,464 bytes and exposed a batched BIP30 overlay mismatch before the
successful fresh rerun. The subsequent IBD hot path batches enabled explorer
commits, reuses deployment context, skips repeated structural validation,
reduces progress output, and omits the historical explorer index unless an API
listener is requested. A three-run release benchmark of 1,000 indexed
generated blocks improved from a 26.12-second median at commit `c0e31d1` to
11.03 seconds, a 2.37× throughput increase with identical final hashes; the
unindexed validator completed its first corresponding run in 8.82 seconds. A
resumable production run then stopped exactly at height 193,000 after a final
59,496-block leg in 2,191 seconds. Separate weekly/manual jobs run deterministic
libFuzzer budgets, targeted Miri interpretation, AddressSanitizer,
ThreadSanitizer, and MemorySanitizer. Release tags and manual release workflows
generate a CycloneDX 1.5 SBOM, require two byte-identical all-feature Linux
release builds, and publish signed Sigstore provenance plus an SBOM attestation.

The current mainnet default has advanced to Core 26 checkpoint height 295,000,
with a 40 GiB resource ceiling. The authenticated state was extended in
place through checkpoints 216,116, 225,430, 250,000, 279,000, and 295,000 and executed BIP34
activation on the way. Checkpoint-wide script scheduling first raised an
adjacent live leg from 10.57 to 12.95 blocks/second. Overlapping each block's
script jobs with construction of later cumulative UTXO transitions then
completed the final 6,965 blocks plus recovery in 435.36 seconds (15.99
blocks/second), 12.9% above the adjacent checkpoint-barrier implementation and
about 51% above the original per-block-barrier leg. The daemon stopped at
`0000000000000001ae8c72a0b0c301f67e3afca10e819efa9041e458e9bd7e40`.
After the next storage optimization, the final 4,168 blocks took 246.75 seconds
including cold startup; a steady 1,008-block checkpoint sustained 25.2
blocks/second, about 47% above the adjacent 256-block run. A cold
completed-target restart again requested no block.
Batch UTXO prefetch then reused one redb read snapshot instead of opening a
transaction for every historical input; the final 3,904 blocks to 295,000 took
282.22 seconds including cold startup, and its steady interval improved from
16.19 to 18.45 blocks/second. The exact target and another cold restart passed.
The following storage pass folds a complete checkpoint into one sorted net
UTXO mutation, skips redundant cold-tier probes when that tier is empty, and
does not rebuild a discarded aggregate undo. Authenticated experimental
validation now drops legacy per-block undo on open and omits new undo because
the resulting directory cannot serve reorganizations; ordinary serving and
AssumeUTXO chainstates retain it. On the resumed mainnet directory this removed
309,112 obsolete undo rows, and one offline redb compaction reduced
`chainstate.redb` from 23 GiB to 4.0 GiB. The compact database receives a
validation-only 16 GiB cache on high-memory soak hosts. Spends and creations
are merged into one monotonically ordered B-tree mutation, and transaction IDs
authenticated during Merkle validation are reused during execution. Large
input-prefetch sets are split across ordered concurrent redb read snapshots
while preserving caller order. Block requests remain limited to 16 hashes per
`getdata`, while up to eight such requests (128 responses) are pipelined into
one protocol-bounded receive window. The live validator now uses four-request,
64-block windows so later, larger blocks stay comfortably inside the same
30-second peer budget. The pinned redb write buffer sorts dirty pages by
file offset and coalesces adjacent pages into writes of at most 8 MiB. On the
exact same 1,008-block height-346,921–347,928 batch, total time fell from
117.72 to 72.46 seconds and execution/persistence from 82.95 to 30.77 seconds,
while retaining an atomic committed tip.
At the larger post-BIP66 working set, 1,008 blocks crossed a redb dirty-page
cache threshold: one stable batch took 181.77 seconds. Two adjacent 504-block
checkpoints took 84.71 seconds combined, and three 252-block checkpoints
sustained 12.1 blocks/second without the superlinear commit spike. That result
first moved the soak from 1,008 to 252 blocks while retaining every
explicit 1–1,008 value for measured hosts and chain eras.
For bounded standalone validation checkpoints of at most 256 blocks, the
daemon also requests the next batch's first window only after the current
batch has passed structure validation. Larger checkpoints read on demand so a
public peer is not left blocked on an unread 128-block response throughout a
long script/commit phase.
The peer can transfer those at most 128 authenticated responses while current
scripts and chainstate commit, and the normal ordered receiver consumes and
validates them before requesting anything further. The first two 126-block
checkpoints took 29.0 seconds combined, but their 21-checkpoint long sample
fell to 5.41 blocks/second as twice as many `F_FULLFSYNC` barriers accumulated.
The adjacent 252-block lookahead sample sustained 5.97 blocks/second, so the
mainnet smoke keeps the evidence-backed 252-block default; every explicit
1–1,008 value remains available for measured hosts and chain eras.

On 2026-07-24 the resumed production path reached BIP66 activation height
363,725 and its pinned hash
`00000000000000000379eaa19dce8c9b722d46ae6a57c2f1a988119488b50931`.
A cold completed-target restart requested no blocks and stopped at the same
height/hash.
The next CPU pass replaced one `libbitcoinconsensus` call per input with one
call per transaction while preserving the earliest failing input index. On the
same release historical-full-block fixture—five activation blocks, 8,997
transactions, and 23,331 inputs—elapsed execution fell from 1.47 to 0.44
seconds on the same host, a 3.34× speedup. Core 26's complete public consensus
vectors and the real SegWit, CSV, Taproot, and activation-block fixtures cover
the new ABI.
The optimized production path then stopped exactly at BIP65 height 388,381/hash
`000000000000000004c2b624ed5d7756c508d90fd0da2c7c679febfa6c4735f0`.
A 171.8-second offline compaction at height 381,113 reduced the fragmented
chainstate from 10.88 to 7.48 GB and cut the adjacent execution/persistence
measurement from 72.31 to 13.48 seconds. A cold completed-target restart
advanced only the active header store to height 959,424, requested no blocks,
and exited at the same BIP65 height/hash.

The next storage pass replaced validation-only random base-tree rewrites with
immediate-durability, append-only checkpoint deltas. Legacy `RVD3` records have
a strict fixed-width sorted outpoint index followed by canonical UTXO bytes.
New `RVD5` rows retain that exact per-shard format behind a small manifest but
partition the sorted keyspace into at most 16 high-prefix shards, so a Bloom
candidate reads only its shard instead of one giant fragmented redb value.
Already-written `RVD4` 256-shard rows remain readable; live measurements
rejected that fan-out because its write amplification outweighed the smaller
reads. Checksummed `RVB1` per-record Bloom filters and 16-record aggregate
filters reject old runs before value access. The current aggregate—including
an unfinished group—is rewritten in the same transaction as every complete
delta and execution tip. All required row/shard pairs in one fully sharded
16-row group share a bounded dynamically balanced worker queue; each worker
reuses one read transaction and decodes in place. Matches merge newest-first,
so bounded same-group speculation preserves newest-wins semantics. A group
containing any legacy row retains strict row-at-a-time fallback. Up to 32
hottest RVD3 rows observed by each batch are rewritten one transaction at a
time to sorted RVD5 shards alongside the next batch's read-only UTXO
prefetch, archive staging, and network lookahead.
Existing RVD3 directories still open without an eager migration; missing
filters undergo the prior strict reconstruction.
Ordinary reorganizing stores reject this format. Explicit materialization
folds all runs and clears the delta and filter tables atomically. There is no
relaxed durability or block undo in this fixed-target mode.

At heights 405,518–408,673, adjacent 252-block checkpoints normally completed
in 20.97–30.22 seconds with execution/persistence mostly 6.78–9.90 seconds;
the former base-tree path in the same era had taken roughly 52–95 seconds
total and 35–78 seconds in execution/persistence. Periodically rewriting the
accumulated overlay was rejected after its materialization checkpoints grew
from 86.4 to 185.6 seconds. A fresh 128-block checkpoint A/B produced
7.2–9.3 blocks/second versus 9.6–12.0 blocks/second for adjacent 252-block
checkpoints, so the soak returned to 252. Requesting a complete 252-block
lookahead was also reverted after download time rose to 17.6–29.5 seconds
instead of the preceding 11–17 seconds. The retained configuration is
therefore the measured 252-block checkpoint with one bounded 128-block
lookahead window.

For experimental mainnet checkpoints wider than 64 blocks, up to three ready
standby candidates now survive the chainstate-open phase; the first one that
still passes bounded activation becomes an auxiliary block source. A
252-block batch repeatedly requests paired 64-block windows from the active
and auxiliary peers concurrently. Larger configured batches repeat the same
pairing, while each peer has at most 64 ordered block
responses outstanding. The receiver appends every pair in active-chain order
and retries its auxiliary half on the primary after any request or response
failure. After an unfinished auxiliary response gets two seconds of grace,
the receiver retains every checksum-verified block already delivered and
requests only the missing suffix from the primary. The failed session is
retired and the next of at most three ready auxiliary candidates gets one
bounded trial. This avoids both whole-window duplicate transfer and unbounded
cycling through unreliable public peers. The earlier whole-window fallback
reduced one observed slow-auxiliary download from 40.485 to 27.684 seconds
instead of paying the complete 30-second auxiliary timeout first; retaining
partial progress further bounds that fallback by the actual missing blocks. An
adjacent live sample reduced ordinary median download time only from about
19.4 to 18.8 seconds, so this remains a tail-latency guard rather than the main
speedup. An earlier unread 124-block auxiliary lookahead was rejected after it
exceeded the 30-second bound. The retained execution-overlap path instead
actively drains bounded primary/auxiliary response pairs on a scoped worker
and stores at most one complete configured validation batch as fully received
blocks.

The same production directory then stopped exactly at CSV activation height
419,328/hash
`000000000000000004a1b34462cb8aeebd5799177f7a29cf28f2d1961716b5b5`.
The 71-block tail committed in 12.22 seconds. A cold restart with the optimized
release advanced only the header store from 959,431 to 959,434, requested no
blocks, and exited again at the exact CSV height/hash.

After extending the authenticated ceiling to SegWit height 481,824, the
persisted-filter migration opened the 18 GB chainstate at height 432,684 in
11.454 seconds. The immediately following reopen loaded the same journal in
6.035 seconds, versus the approximately one-to-two-minute rebuild observed
before this change. Its first post-migration 252-block checkpoint completed in
25.624 seconds, including 9.231 seconds of execution/persistence, so the
restart acceleration did not move the scan cost into ordinary checkpoints.

The same directory then completed SegWit activation height 481,824/hash
`0000000000000000001c8018d9cb3b742ef25114f27563e3fc4a1902167f9893`.
Its final 252-block checkpoint committed in 43.780 seconds. A completed-target
restart opened the enlarged chainstate in 13.078 seconds, advanced only the
header store from 959,450 to 959,452, requested no blocks, and exited at the
same exact height/hash.

Post-SegWit tuning measured four 252-block checkpoints at 44.850, 69.021,
53.816, and 46.931 seconds. Four adjacent 504-block checkpoints completed in
66.568, 82.237, 60.608, and 63.879 seconds, equivalent to roughly
30.3–41.1 seconds per 252 blocks. One 1,008-block checkpoint took 126.796
seconds, initially providing no material gain over two stable 504-block
checkpoints. At heights 495,433–498,456, however, three adjacent 1,008-block
checkpoints took 163.505, 185.132, and 169.897 seconds. Their 169.897-second
median is 6.0% below twice the preceding nine-checkpoint 504-block median of
90.361 seconds; the directly adjacent comparison improved 195.315 to 163.505
seconds, or 16.3%. RVD3 did not reproduce the old base-tree superlinear
1,008-block commit. The next 1,008-block batch at height 504,505 nevertheless
exceeded the ledger's independent 1 GiB canonical-record ceiling and failed
before staging or chainstate mutation. The soak therefore selected 756 blocks:
four immediately following checkpoints completed in 124.259–148.291 seconds
and advanced atomically through height 507,528.
An experimental four-peer 504-block downloader was rejected after unreliable
auxiliary peers widened complete-batch time to 73.243–127.829 seconds. The
retained path uses bounded paired 64-block windows and a two-second
slow-auxiliary guard. It preserves partial auxiliary progress and rotates
through at most three already-ready candidates.
Expanding each batch across more simultaneous auxiliaries was also rejected after
five auxiliary-bearing batches took 80.105–105.571 seconds while the following
primary-only batches took 80.482 and 88.089 seconds.

Cross-execution lookahead now actively downloads the complete next configured
validation batch on a scoped network worker while the current checkpoint
stages its archive and performs its sequential UTXO transition. The worker
uses the same bounded primary/auxiliary 64-block pairs, partial-progress
fallback, and auxiliary retirement as foreground download. Each received
window is immediately reduced to compact consensus bytes instead of retaining
an expanded transaction object tree. The next checkpoint decodes and validates
that exact block-hash prefix before using it and downloads only any unfilled
remainder. This can hide the complete 756-block transfer behind otherwise
network-idle persistence; the configured 1,008-block ceiling still bounds the
additional serialized payload to 4 GiB at Bitcoin's consensus maximum.
Because the worker continuously drains every response window, it does not
recreate the unread 128-block response pressure seen in the earlier soak.

The resulting production soak stopped exactly at Taproot activation height
709,632/hash
`0000000000000000000687bca986194dc2c1f949318629b44bb54ec0a94d8244`.
Across 28 checkpoints whose complete input came from the execution-overlapped
cache, foreground download had already fallen below three seconds and total
time had an 87.312-second median, versus 182.653 seconds for an adjacent
pre-overlap checkpoint. The final 252-block tail completed in 50.817 seconds.
A cold completed-target restart opened chainstate in 40.337 seconds, advanced
only the authenticated header DAG from 959,514 to 959,520, made no block
request, and exited again at the exact Taproot height/hash.

Later full-chain blocks first pushed a configured 756-block checkpoint above
the archive's independent 1 GiB canonical-record ceiling at height 764,065.
Validation now selects the longest byte-safe prefix after structure checks,
atomically executes that prefix, and carries every already downloaded and
hash-verified suffix block into the next compact prefetch buffer. The first
live split committed 726 blocks and carried 30 without a duplicate request;
the buffer was replenished to all 756 next-batch blocks during execution.
The same production directory subsequently validated every mainnet block from
genesis through the authenticated height 959,520/hash
`000000000000000000003a8648dadb49e67db65326f85b50651661dd7c237299`,
then extended to the live height 959,592/hash
`000000000000000000019190d596b445008319f199f8ee6f6af0e73cbc440667`.
The 71-block live-tip extension spent 19.200 seconds in the new group-wide
UTXO read queue. Its final cold restart opened chainstate in 14.152 seconds,
requested no block, observed no newer header, and exited at that exact tip.
Physical freezer usage was 418 MiB with 661 GiB still free; validation
chainstate was 243 GiB.

Block-structure validation now divides sufficiently large downloaded batches
across bounded host-CPU workers. Each worker validates expected hash,
deployment context, Merkle/coinbase/weight/witness structure, and transaction
identifiers independently; ordered joins still report the earliest failing
height. On adjacent 1,008-block mainnet checkpoints, structure time fell from
11.489 seconds to 1.652 and 1.705 seconds, an approximately 85% reduction that
removed about 9.8 seconds from each batch without changing execution order or
the atomic commit.

Archive staging now streams each length-prefixed block directly through a
bounded four-worker zstd encoder while hashing the same canonical record
stream, instead of first materializing another batch-sized uncompressed
buffer. Consensus serialization runs inside the already ordered structure
workers. The first live 756-block sample reduced staging from the preceding
6.723–7.175 seconds to 4.949 seconds. Validation-journal bulk reads also
partition large group-Bloom probes across bounded CPU workers while preserving
the serial input partition order; small probes remain serial.
Across the first five complete 756-block checkpoints with all optimizations,
total time had a 124.198-second median versus 135.252 seconds for the preceding
four-checkpoint baseline, an 8.2% end-to-end improvement despite public-peer
variance. Execution/persistence median fell from 59.870 to 50.367 seconds
(15.9%), while staging median fell from 7.098 to 4.453 seconds (37.3%).
The next script-scheduling pass groups serialized transaction checks into
16-transaction worker packets and appends every block's packets under one
queue lock; each worker returns only its earliest failure, and the checkpoint
join still selects the earliest block/transaction globally. On five adjacent
756-block checkpoints at heights 598,249–602,028, execution/persistence times
were 46.015, 46.165, 49.246, 51.255, and 45.921 seconds. Their 46.165-second
median was 10.2% below the preceding five-checkpoint 51.401-second median.
At the larger height-651,925–655,704 block payloads, repeated 128-block
response timeouts had begun replacing otherwise usable public peers. The first
five checkpoints with 64-block validation windows all completed on one
primary session without failover: download time was 93.017–123.082 seconds
with a 95.388-second median, and total time had a 168.974-second median.

For ordinary persistent validation, Bitcoin, legacy testnet, Testnet4, and Signet can bootstrap from the pinned Core 31 seed set; for example, `rbtcd --network bitcoin --data-dir PATH`. Regtest normally supplies `--connect HOST:PORT` or reuses a previously verified peer in `peers.redb`. Repeat `--connect HOST:PORT` to provide up to 16 ordered, deduplicated peers. Repeat `--dns-seed HOST[:PORT]` to replace the pinned defaults, or use `--no-dns-seeds`; explicit peers and fresh persisted candidates are tried before DNS is queried. Each stage starts its bounded candidate handshakes concurrently but still selects the active session in the configured/persisted order; later completed handshakes stay in memory as hot failover sessions while the earlier session runs. Each full-service standby clones the current validated header DAG, then independently requires a nonce-matched ping/pong and performs one bounded `getheaders` validation step every 30 seconds. Its PoW, difficulty, timestamp, checkpoint, and deployment validation is isolated from shared persistence; invalid announcements evict that standby, while activation carries its validated height and the active synchronizer resumes from durable state. Other application messages remain bounded and ordered for activation. After the active socket completely writes a wallet transaction, the same transaction is fanned out through a bounded in-memory ring to every hot standby; a stalled or lagging standby is removed without blocking active synchronization. A failed handshake, missing full-history/witness service, interrupted headers or block transfer, or rejected response advances to the next candidate; durable headers and atomic chainstate let that peer resume the same IBD. Learned connection attempts are committed before each task starts network I/O and receive a persistent one-minute-to-six-hour exponential retry delay. Malformed framing/ordering, bounded-response violations, objectively invalid headers, and invalid downloaded blocks additionally discourage non-manual peers for one hour, doubling to at most one day; the count decays after seven quiet days. Transient I/O, timeouts, future-time headers, missing blocks, obsolete versions, and missing services never receive that penalty. Every successful full-history/Witness handshake is persisted as verified, migrates into the keyed tried bucket set, and clears the ordinary connection delay, so a later launch can omit `--connect` and DNS; promotion frees the learned source's new-bucket quota. The stronger protocol record is cleared only after the requested synchronization session completes successfully, preventing an invalid-block peer from resetting escalation with a clean handshake. Completion time and a saturating completion count are persisted separately from handshake success, survive database reopen, and rank fully proven peers ahead of handshake-only candidates. Within otherwise equal reputation, the latest successful outbound handshake measurement is capped to 1–60,000 ms and lower known latency ranks first. A successful requested synchronization session with downloaded blocks additionally persists exact completed block-payload bytes divided by response-wait time, capped at 1 GB/s; higher known throughput then ranks ahead of lower or legacy-unknown throughput before the existing freshness and target-network-group diversity rules. Successful full-service handshakes request addresses with a three-second bound; newly learned candidates become eligible on the next restart. Sync completion remains based on validated cumulative work, never the peer's untrusted advertised height. The keyed new table now retains up to eight source-group references with Core-style exponentially decreasing admission probability, while tried collisions use exact hashed slots. This remains a bounded outbound peer manager rather than Core's complete addrman or adaptive connected-peer eviction design. Core-compatible `--signetchallenge HEX` selects a custom BIP325 challenge, derives its P2P magic, disables default-Signet trust anchors/seeds, and accepts repeatable `--signetseednode HOST[:PORT]`; use an isolated data directory, whose challenge identity is checked before wallet opening or network I/O. Add `--once` for a bounded sync-and-exit run. Add `--explorer-listen 127.0.0.1:3000` to serve the embedded read-only explorer and REST API; non-loopback binds are rejected until authentication is implemented. Regtest Taproot activation can be overridden with Core-compatible `--vbparams taproot:START:END[:MIN_HEIGHT]`. Buried deployments accept repeatable Core-compatible `--testactivationheight NAME@HEIGHT`, where `NAME` is `segwit`, `bip34`, `dersig`, `cltv`, or `csv`; the last value for a name wins. The complete selected consensus configuration is bound to a fresh execution database and cannot later change in place. An offline Bitcoin/legacy-testnet/Testnet4/Signet/regtest base can be installed only into a fresh data directory with `--assumeutxo-snapshot FILE --snapshot-height HEIGHT --snapshot-blockhash HASH --snapshot-utxo-count COUNT --snapshot-records-bytes BYTES --snapshot-records-sha256 HEX`; every identity value must come from an authenticated channel, the header must already be active in `headers.redb`, and the durable assumed marker remains until background genesis validation and finalization succeed. Core-format activation instead uses `--core-assumeutxo-snapshot FILE` and accepts only a compiled Core 31 identity on the validated maximum-work header chain. Bitcoin headers do not commit to the UTXO set, so no MPT or new consensus proof is added: maximum work authenticates branch membership, Core's release-pinned `hash_serialized` authenticates provisional snapshot contents, base-to-tip blocks are validated normally, and independent genesis-to-base replay must match before the assumed marker clears. Core 31 minimum-chainwork and assume-valid defaults are loaded per supported legacy network and can be overridden with `--minimum-chainwork HEX` and `--assumevalid HASH|0`. A chain below the work floor remains in IBD. Assume-valid currently identifies a reviewed active-chain anchor only: all scripts are still verified. Bitcoin, legacy testnet, Testnet4, Signet, and regtest support ordinary persistent `--data-dir` execution; Testnet4 enforces BIP94 and uses its own Core 31 trust anchors and seeds. For bounded validation and soak only, Bitcoin or legacy testnet can explicitly opt into the same production execution path with `--experimental-network-execution --once` plus a mandatory authenticated `--validate-until-height`/`--validate-until-blockhash` pair; the execution routine repeats that hard-ceiling requirement. This mode prints a funds-safety warning, cannot start an indefinite node or expose explorer/RPC/wallet services, and does not support testnet4 or automatic AssumeUTXO cleanup. Testnet4 defaults to Core 31's two pinned DNS seeds and port 48333; on 2026-07-25 an ordinary public run validated and executed genesis through height 145,735, then a cold restart opened chainstate in 52 ms, advanced to 145,737, executed two new blocks, and exited in 4.40 seconds. Explicit peers or replacement seeds remain supported.

Outbound address families can be constrained with repeatable
`--onlynet ipv4|ipv6` (or `onlynet=` config entries); supplying both restores
dual-stack operation. `--proxy IP:PORT` routes every outbound peer socket
through a no-authentication SOCKS5 proxy. Proxy mode requires
`--no-dns-seeds`, so the process cannot leak seed names to the host resolver;
use explicit IP peers or the persisted verified peer database. Destination
requests carry IP literals, all manual/persisted/resolved candidates are
filtered before dialing, and disabled families create no socket traffic.

Successful full-service handshakes also feed an instance-scoped adjusted clock.
At most one timestamp is retained per IPv4 `/16` or IPv6 `/32`, with 200
samples total. Header future-time checks use the median only after five diverse
samples and only within Bitcoin's 70-minute adjustment bound; insufficient or
extreme samples retain local time. Active and hot-standby header validation
share this clock, while separately embedded nodes remain isolated.

Enable inbound serving only with an explicit address:

```text
rbtcd --network bitcoin --data-dir /srv/rbtc/bitcoin \
  --listen 0.0.0.0:8333 \
  --external-address YOUR_PUBLIC_IP:8333 \
  --whitelist 203.0.113.9 \
  --max-inbound-peers 32 \
  --max-inbound-peers-per-ip 4 \
  --max-upload-bytes-per-day 1073741824 \
  --inbound-requests-per-minute 1200
```

The same keys are available in the strict config file as `inbound_listen`,
`external_address`,
repeatable `whitelist`,
`max_inbound_peers`, `max_inbound_peers_per_ip`,
`max_upload_bytes_per_day`, and `inbound_requests_per_minute`;
`listen=false` preserves outbound-only mode. A zero daily upload target means
unlimited historical upload. Pruned nodes advertise `NETWORK_LIMITED`, never
`NETWORK`, and answer block requests only from the verified freezer suffix.
The external address is optional, must be explicitly routable for the selected
network, and is never inferred from a wildcard bind. When configured it is
announced once to each outbound peer and returned first in bounded inbound
`getaddr` samples with the node's exact service flags.
Each exact `whitelist=IP` source has a protected inbound role: it may bypass
only the per-IP/network-group ceiling, still consumes the global hard ceiling
and every request/upload budget, and cannot be evicted by an ordinary peer.
The listener remains bound across active outbound-peer failover, but refuses
new handshakes while no reconciled execution view is installed.
At the hard inbound connection ceiling, a new socket can replace an
unhandshaked or low-work peer only when it contributes a previously absent
IPv4 `/16` or IPv6 `/32` network group. Duplicate groups are selected first,
same-group arrivals cannot churn incumbents, and the old task is fully reaped
before its permit is transferred, so the configured hard bound is never
exceeded. When the local API is enabled, `/api/v1/status`, `getnetworkinfo`,
`getpeerinfo`, and `/metrics` report live inbound handshakes, per-peer request
and upload accounting, admission rejection reasons, bounded disconnect
reasons, adaptive evictions, and the current 24-hour historical-block upload usage. Prometheus
keeps these metrics aggregate to avoid peer-address label cardinality.

Each launch exclusively locks the owner-only `DATA_DIR/.rbtc.lock` before
opening chain databases or connecting peers. A conflicting process exits with
the recorded PID, network, and start time; stale markers do not prevent restart
because the operating system releases the advisory lock after a crash.

An explicitly selected Core snapshot can be downloaded without opening a node
data directory:

```sh
rbtcd --download-core-assumeutxo https://example.invalid/utxo.dat \
  --snapshot-download-output /isolated/utxo.dat \
  --snapshot-download-bytes EXACT_LENGTH \
  --snapshot-download-workers 4
```

Completed 64 MiB chunks survive restart, while partial current chunks are
retried. The printed SHA-256 is transport evidence only; activation still
requires the compiled Core identity on validated maximum-work headers.

To start a new node from a recent authenticated height rather than replaying
genesis before it can serve, use the explicit three-stage path:

```sh
# 1. Validate and persist the complete header chain and maximum-work branch.
rbtcd --network bitcoin \
  --headers-db /srv/rbtc-active/headers.redb

# 2. Install only a release-pinned Core 31 AssumeUTXO identity on that branch.
rbtcd --network bitcoin --data-dir /srv/rbtc-active \
  --core-assumeutxo-snapshot /srv/snapshots/utxo-935000.dat

# 3. Catch the assumed chain up from 935001 and enter live mode immediately,
#    while a separate chainstate validates genesis through 935000.
rbtcd --network bitcoin --data-dir /srv/rbtc-active \
  --background-assumeutxo /srv/rbtc-genesis-validation
```

The current Core 31 Mainnet anchor is height 935,000. This is deliberately not
an arbitrary-height snapshot interface: Bitcoin headers commit to transactions,
not to the UTXO set. A different recent height is acceptable only after its
block hash, `hash_serialized`, UTXO count, and chain-transaction count become a
reviewed, compiled Core chainparams identity (or after this node has produced
and authenticated its own migration snapshot). The serving chain validates
every post-base block normally. The historical validator independently rebuilds
the base UTXO set, and only an exact identity match clears the assumed marker.
No MPT or new consensus commitment is introduced.

Two txid-group indexes are available for read-mostly tooling. The CLI and
snapshot-overlay path build the version-2 `CoreSnapshotUtxoIndex`: its group
offsets are bit-packed on disk, batch lookups read the table and source snapshot
in file order, and the largest group span observed during the authenticated
build becomes a hard per-lookup read bound. The library-level
`CoreSnapshotIndex::build` retains a separate version-3 format with eight-byte
group offsets and cached source modification time for callers that prefer that
API. Both leave the original Core file untouched, bind their sidecar to its
identity, and verify txid plus vout from the source group before returning a
UTXO. Full activation must still use `verify_core31_snapshot`; neither sidecar
is a trust anchor.

The Core 31 mainnet height-935,000 snapshot and both generated MPHF sidecars
were measured on 2026-07-29 and 2026-08-01:

| Artifact/metric | Measured value |
| --- | ---: |
| Core snapshot bytes | 9,387,990,306 |
| Core snapshot UTXOs | 164,241,311 |
| Core snapshot bytes/UTXO | 57.159738 |
| Distinct txid groups | 113,879,165 |
| Grouped outpoint-key bytes | 3,944,314,134 |
| Grouped key bytes/UTXO | 24.015360 |
| Library v3 sidecar bytes | 957,969,566 |
| Library v3 sidecar/source ratio | 10.2042% |
| Overlay/CLI v2 sidecar bytes | 530,926,239 |
| Overlay/CLI v2 sidecar/source ratio | 5.6554% |
| Overlay/CLI v2 MPHF levels | 18 |
| Overlay/CLI v2 table width | 34 bits/group |

The source snapshot SHA-256 was
`e572ddbe456d254f05fb004cebe225bdb3656074b66f0e9b1c7fa83e1301d486`.
The library v3 sidecar keeps eight-byte group offsets; the overlay/CLI v2
sidecar bit-packs only the group offsets. Values, compressed scripts, and
grouped txids remain exclusively in the original Core snapshot.

An experimental full-copy comparison also migrated the caught-up mainnet redb
UTXO set into MDBX in sorted 20,000-row pages. The verified 2026-07-29 report
contained 166,328,068 records:

| Metric | redb | MDBX |
| --- | ---: | ---: |
| Storage bytes | 73,023,500,288 | 17,448,321,024 |
| Bytes/UTXO | 439.033 | 104.903 |
| MDBX/redb size | 100% | 23.8941% |

The migration took 640.013 seconds, approximately 259,882 rows/second, and
recounted the source and destination to the same record total. This is a
static full-copy result, not yet a claim about long-running write
amplification. `scripts/eval-mdbx-full-copy-day.sh` and
`scripts/summary-mdbx-full-copy-day.sh` provide the separate 24-hour size/RSS
measurement path.

An experimental validation directory remains bound to its original hard
ceiling during ordinary restarts. Once that exact target has completed,
`--extend-validation-target` may raise it to a higher authenticated height/hash
that the validated active header chain already contains. The update is atomic;
an unfinished or unbound directory, a non-forward request, or another hash at
the same height fails closed.

Bounded validation may add `--validation-deferred-repair` to retain redb's
immediate atomic durability while omitting its extra quick-repair allocator
write on every checkpoint. A killed process still reopens to an old-or-new
complete checkpoint, as covered in both persistence modes; recovery itself may
take longer. The mainnet smoke uses this mode by default, and
`RBTC_SYNC_DEFERRED_REPAIR=0` restores crash-fast recovery writes.

A bounded real-mainnet smoke probe can execute only block 1 after validating the current active header chain:

```sh
rbtcd --network bitcoin --data-dir /isolated/probe \
  --experimental-network-execution --once \
  --validate-until-height 1 \
  --validate-until-blockhash 00000000839a8e6886ab5951d76f411475428afc90947ee320161bbf18eb6048
```

On 2026-07-23 this exact production path validated and persisted headers through height 959,340, then downloaded, executed, and durably stopped at block 1 with the pinned hash. This is an acceptance probe, not permission to use the resulting node with funds.

The normal live-service path is `rbtcd --data-dir ACTIVE --network NETWORK [PEER OPTIONS] --background-assumeutxo VALIDATION`. It derives the authenticated height/hash from the active marker and starts the assumed active chain and independent genesis validator as separate runtime tasks with separate connections, peer failover, headers, chainstate, ledger, explorer, and peer databases. Both bulk paths publish sorted, prefix-sharded UTXO deltas instead of randomly rewriting a multi-gigabyte base B-tree at every checkpoint. Reads resolve newest deltas through bounded group/row Bloom filters and parallel shard reads before the immutable base. The serving chain retains block undo; a reorganization first atomically materializes its overlay and then uses the ordinary rollback path. Finalization computes the independent UTXO identity one lexical key prefix at a time, bounds peak memory to one final-set shard, materializes the much smaller base-to-live active overlay once, and clears the assumed marker without taking the explorer or wallet API offline. Durable delta state is self-identifying and resumes after a crash without a special flag.

A long synchronous checkpoint on one task cannot starve the other task's network receive deadlines: blocking stage, UTXO prefetch, validation, and commit sections explicitly yield their Tokio worker, including while an owned prefetch thread drives the next network window. Each concurrent bulk chainstate has an 8 GiB redb cache cap (16 GiB aggregate), instead of the ordinary live node's 1 GiB cache or two competing 16 GiB single-validator caches. Network receive, structure validation, freezer staging, and next-window prefetch remain concurrent, while final redb publication transactions take a process-wide commit turn so random I/O on one physical device does not thrash both stores. If either side exhausts its peers or validation/finalization fails, the combined service cancels the sibling task, fails closed, and retains both resumable directories. `--once` waits for both sides and finalizes before returning, which is useful for bounded deployment gates. The older sequential `--complete-assumeutxo` path remains available. Both modes reject same-directory, parent/child-directory, symlink, and Unix hardlink aliases before validation state is opened.

On 2026-07-26 the Core 31 Mainnet height-935,000 snapshot entered live
service, validated post-base blocks through height 959,688, and concurrently
replayed genesis to the base. A simultaneous 11-peer exhaustion stopped at
height 732,941; restart resumed at 732,942 without replaying a committed batch.
Finalization matched 164,241,311 UTXOs / 15,334,473,795 canonical bytes and
cleared the assumed marker after materializing 50,340,320 net active-overlay
updates. The resumed run took 24,998.34 seconds. After data-backed re-tiering,
the chainstate cold-opened in 46 ms and validated 42 new blocks through height
959,730/hash
`0000000000000000000171bbbd6e93d945499dc33a30747cd1603372a7a1f513`.

The ordinary “successful handshake promotes to tried” rule is conditional on its exact keyed tried slot being vacant. An occupied slot leaves the successful challenger selectable from new and enters the bounded collision queue. Incumbent probes use the same pre-I/O attempt accounting and full-service handshake checks as normal connections; success cancels its collisions, while connection/handshake failure atomically demotes it and promotes the best queued challenger. Learned records persist up to eight independently keyed source-group references, admitted with exponentially decreasing probability; full Core addrman probabilistic selection remains open.

Persisted selection now applies Core-style terrible-entry hygiene before collision probes or ordinary candidates. A peer attempted within the last minute is protected; otherwise a timestamp more than ten minutes in the future, a zero or over-30-day-old address time, three consecutive failures without any success, or ten failures when the last success is over seven days old makes the entry ineligible. Startup atomically removes those rows and sanitizes collision references, while successful handshakes continue to reset the failure count. Full Core addrman probabilistic selection remains open.

The first validation target is durably bound to the validation database and cannot later be changed or moved behind its executed tip; even a restart that omits the validation flags automatically inherits that ceiling. Successful completion retains the validation directory as audit evidence by default. Add `--validation-batch-size N` (1 through 1,008, default 256) to cap each atomic validation checkpoint and, in background mode, the assumed active chain's independent base-to-live checkpoints as well. The measured default reduces batch time by 24.8% versus 64 blocks, at the cost of roughly 35% higher peak working set; memory-constrained hosts should set a lower value explicitly. `--validation-pause-ms MS` (at most 60 seconds) remains isolated to the genesis validator. `--validation-deferred-repair` applies to both bulk chainstate pipelines in background mode: their database commits remain atomic and durable, while redb writes its large allocator-state snapshot once at orderly close instead of after every catch-up checkpoint; an unclean stop can therefore trade a slower first repair for much faster bulk progress. Each checkpoint is downloaded through 16-block protocol requests; four requests may be pipelined into one ordered 64-block peer window and two independent peers can supply adjacent windows concurrently, then chainstate and any enabled explorer projection are committed once for the aggregate batch. The pipelined receiver preserves duplicate, unsolicited, `notfound`, compact-reconstruction, fallback, payload, and message-count checks without increasing one `getdata` exposure. The 1,008-block ceiling matches the default retained-ledger window and could hold approximately 4 GiB of consensus-maximum block payload before validation working state, subject to the ledger's independent 1 GiB canonical-record ceiling. In background mode, while active execution trails its header tip, the validator remains independently live but limits each checkpoint to the smaller of the configured cap and 252 blocks; this keeps worst-case block payload below one GiB and reduces freezer/index/fsync frequency without an artificial pause. Once active serving catches up, the configured cap is restored. Persisted progress and the effective limits are printed after every batch, and `GET /api/v1/validation` reports both tips, the immutable target, remaining blocks, phase, failure, and current throttle state when the explorer listener is enabled. Snapshot origin remains durable after finalization. A fresh explorer atomically streams the current hot/cold UTXO set into a cursor-paged baseline, so current address UTXOs and all post-snapshot blocks are indexed without pretending unavailable pre-snapshot transaction/block history exists.

The batch log reports download, structure, staging, execution, indexing,
publication, and total time so later tuning is based on measured phases.

`--cleanup-validation-dir` is an explicit destructive opt-in for automatic/background completion. rBTC will only claim a validation directory that was absent or empty before it created the validation stores, records a strict, size-bounded owner-only marker bound to the network and snapshot target, and revalidates that marker plus the completed non-assumed chainstate before cleanup. The marker file and its parent directory are synced on Unix before use. Unknown top-level artifacts, symbolic links, special files, a changed target, or an unowned legacy directory fail closed and remain in place. An accepted directory is first atomically renamed to a randomized sibling quarantine; the parent is synced after both the rename and recursive removal. A failed first parent sync rolls the quarantine rename back before any recursive deletion. Do not enable this flag when the validation database is required as audit or recovery evidence.

The lower-level two-step interface remains available: build with `rbtcd --data-dir VALIDATION --network NETWORK [PEER OPTIONS] --validate-until-height HEIGHT --validate-until-blockhash HASH`, then run `rbtcd --data-dir ACTIVE --network NETWORK --finalize-assumeutxo VALIDATION`. Both explicit target values must be taken from the authenticated snapshot identity. Headers may synchronize beyond the target, but block requests, atomic execution batches, the retained ledger, and any explicitly enabled explorer projection stop exactly at it; restart resumes safely and a different active hash or an already-overrun chainstate fails closed. The same resource-limit options apply. Finalization requires the same consensus configuration and never replaces active UTXOs. The logical digest deliberately excludes local `last_touched` tier-aging time while retaining every consensus field; the separate snapshot-record digest still authenticates the complete transported bytes. The lower-level manual finalization path deliberately never performs automatic cleanup.

Post-handshake routing centrally caps `inv`/`getdata`/`notfound` at 50,000 entries, locators at 101 hashes, headers at 2,000, and address messages at 1,000. These limits also apply to unrelated frames injected while another response is pending. Peers supporting BIP130 receive `sendheaders` immediately after handshake so announcements remain headers-first. During ordinary 30-second caught-up polling, the daemon requires a nonce-matched pong within the same 32-frame total response budget before requesting more headers; crossed peer pings are answered without extending it, and a header announcement arriving before the pong is retained for the following sync pass. Retained application frames additionally share a 4,000,000-byte aggregate payload ceiling, preventing the frame-count limit from multiplying into a roughly 124 MB queue.

BIP152 negotiation follows `sendheaders` for protocol 70014+ peers and advertises version 2 decoding without opting into unsolicited high-bandwidth announcements. `cmpctblock`, `getblocktxn`, and `blocktxn` transaction/reference counts above 16,666 are objective protocol violations. After the peer reciprocates version 2, all daemon block-download paths request compact inventory, accept a direct full-block fallback, reconstruct unique local-candidate matches, and request the remaining transaction indexes. Short-ID ambiguity is never guessed; a reconstructed Merkle or witness-commitment mismatch triggers one bounded full witness-block retry. Up to 64 admitted peer transactions plus the 64 most recent unique wallet transactions that completed active-peer delivery are supplied to every validating, ledger, explorer, and wallet-backfill download as witness-ID candidates. Transactions absent from those bounded local sets are requested from the peer.

The P2P session can write one non-coinbase transaction only when it fits Core's 400,000-weight-unit standard relay ceiling. Before the authenticated wallet route publishes a consensus-verified transaction into its eight-entry active-peer channel, a distinct wallet-origin policy checks versions 1–3, minimum non-witness size, standard output templates, push-only and bounded scriptSigs, aggregate push-only data-carrier bytes, dust, and Core 31's 100 sat/kvB relay floor. It then reserves channel capacity and commits the exact transaction to the network-bound, owner-only `rebroadcast.redb`; policy or retained-input conflicts return 400, while a full channel or persistence failure returns 503 without creating a new row. The durable queue retains at most 64 unique wtxids for 14 days, rejects conflicting spends across restart, retries never-sent entries after restart and delivered entries every 12 hours, and suppresses confirmed or noncanonical transactions. Wallet-chain reconciliation clears suppression when a reorganization restores the transaction or its inputs. The route succeeds only after the complete active `tx` frame and durable attempt metadata are written. A failed socket write remains eligible on the next peer, and a successful write also fans out through the eight-entry in-memory standby ring. This proves socket delivery, not peer mempool acceptance or acknowledgement.

Failed hot standbys are reaped and classified while the active session is still running, so objective violations enter persistent discouragement promptly without waiting for failover. At most eight ready automatic hot standbys are retained: the existing manual/persistent-reputation queue order protects stronger peers and evicts from its tail as slower handshakes complete. Manual peers do not consume that soft capacity, every stage remains under the 16-connection hard ceiling, and a local capacity eviction is not recorded as a remote failure. Manual peers and transient failures retain their existing discouragement exemptions.

Unsolicited `tx` frames that arrive while a peer is serving a bounded headers, address, or block response are no longer discarded. Each session first retains their wire order in an independent 64-transaction/4 MB FIFO. Once execution has caught up to an active header chain above minimum chainwork, the daemon drains that queue through a read-only admission pass: confirmed inputs, maturity, finality, BIP68/BIP113 locks, deployment-aware Bitcoin Core script execution, output accounting, Core 31 transaction versions 1–3, standard output templates including multiple push-only data carriers under a 100,000-byte aggregate ceiling, Core's default x-of-3 bare-multisig creation ceiling, recognized spent-prevout templates including historical x-of-16 bare multisig, P2SH's 15-accurate-sigop ceiling, P2WSH's 3,600-byte/100-item/80-byte-item bounds, native-Taproot annex prohibition, the 80-byte tapscript argument bound, P2SH-wrapped upgradable witness-program rejection, upgradable Taproot leaf-version rejection, tapscript `OP_SUCCESS` discouragement, dust, minimum fee, and retained-input conflicts must all pass without mutating chainstate. Dependency-connected wire batches are split from unrelated transactions, capped at 25 transactions/101,000 virtual bytes, topologically ordered regardless of arrival order, and applied atomically to a private UTXO overlay, so an invalid child cannot leave its parent admitted. Outputs of accepted parents support later children. Accepted transactions enter a 64-transaction/4 MB oldest-first pool shared across active-peer failover and become compact-block reconstruction candidates; capacity eviction removes an oldest transaction together with every descendant. Conflicts may atomically replace transactions and descendants up to the 100-transaction work ceiling only when they add no unrelated unconfirmed input, pay more than the entire removed set, and pay Core 31's 100 sat/kvB incremental fee. Full-RBF is the default; `--no-mempool-full-rbf` restores inherited BIP125 signaling as an additional requirement without weakening the other gates. The complete parent-before-child snapshot is committed to the network-bound, owner-only `mempool.redb` before the updated in-memory view is published. Reopen strictly bounds and validates its binary snapshot, rejects network mismatch, and reruns every transaction through the current chainstate and deployment-aware policy before retaining it; stale entries and descendants are atomically removed. Before a stale active block is disconnected, its ledger hash is checked and its non-coinbase transactions enter a separate durable recovery snapshot under the same 64-transaction/4 MB bound. Lower-height parents win capacity pressure; after the replacement chain catches up, recovered transactions re-enter the ordinary admission pass and the recovery snapshot is cleared atomically with the new pool.

Each newly admitted, durably committed transaction is announced to every hot standby except the active source peer through the same eight-slot ring used after wallet delivery. BIP339 peers receive wtxid `inv`; legacy peers receive txid `inv` and may request witness data. For protocol version 70013 and later, a valid BIP133 `feefilter` becomes that session's latest sat/kvB threshold. A pool-origin relay carries its exact fee and sigop-adjusted policy vsize, so an announcement below the peer's threshold is suppressed while the bounded ping exchange still completes; equality is announced. Negative or above-`MoneyRange` filters are ignored, and legacy wallet rebroadcast rows without retained fee metadata bypass this optional optimization rather than risking false suppression. Direct active-peer wallet `tx` delivery is unaffected because BIP133 filters transaction inventory, not explicitly submitted payloads. A nonce-matched bounded ping drives the optional `getdata` exchange immediately, so a peer that already has the transaction completes normally. Matching requests receive `tx`, unknown requests receive `notfound`, duplicate announcements are suppressed inside a 64-transaction/4 MB relay cache, crossed application messages retain wire order, and the global inventory/frame/payload limits still apply. Protocol-60002-or-later peers on full-validation active and hot-standby connections may also send BIP35 `mempool`: the session samples the process-shared validated pool at request time, applies its latest BIP133 filter, selects txid or wtxid inventory according to BIP339, and announces at most the pool's 64 transactions/4 MB while caching exactly those payloads for bounded `getdata` service. Empty, pre-BIP35, and header-only sessions produce no inventory. In the other direction, active and hot-standby transaction inventory crossed during ordinary response processing enters a process-shared request tracker capped at 64 announcements per session and 1,024 overall. Once caught up above minimum chainwork, known pool, orphan, wallet, recent-confirmed, and exact recent-reject entries are forgotten; a source may place at most one request for a transaction hash in flight, in its original announcement order, for Core 26's 60-second retry interval. Exact `tx`/`notfound` outcomes complete that source independently, so `notfound`, timeout, cancellation, or disconnect unlocks another announcing standby without a duplicate simultaneous request. Matching payloads enter the same persisted admission path as unsolicited `tx`, and admission or confirmation forgets both txid and wtxid candidates. Each download still has a 64-reference, 32-extra-frame, and 4 MB aggregate transaction-payload ceiling. Only transactions surviving the complete admission pass are announced; lagging, timed-out, or failed peers follow the existing failover path. Accepted inbound sessions subscribe independently to that same fixed-size ring: each applies negotiated txid/wtxid inventory, its current fee filter, and a 5,000-entry FIFO duplicate set, while a lagged subscriber drops old events without back-pressuring consensus. Peer acknowledgement tracking and probabilistic trickle timing remain separate privacy work.

The active mempool snapshot also stores a versioned last-relay-attempt timestamp for each retained transaction. Newly admitted transactions and legacy snapshots without this metadata are immediately due; after that, a caught-up loop selects at most eight due transactions every 12 hours in parent-before-child order. An attempt is recorded only when the transaction was published into a standby ring with at least one receiver, so the absence of a hot standby cannot suppress a later attempt. Snapshot replacement atomically removes attempt rows for evicted, replaced, confirmed, or otherwise stale transactions. This schedule bounds repeated diffusion attempts but still does not prove peer receipt, acknowledgement, or mempool acceptance.

In addition to the 25-transaction/101,000-vB incoming package bound, every dependency-connected Core 31 mempool cluster is limited to 64 transactions and 101,000 aggregate policy vB. This replaces the obsolete ancestor/descendant limits and CPFP carve-out. Version-3 TRUC transactions are limited to 10,000 vB, a child with an unconfirmed parent is limited to 1,000 vB, connected unconfirmed transactions must also be version 3, and the topology is at most one parent plus one child. Every transaction is independently capped at 2,500 legacy sigops and 16,000 consensus-derived standard sigop cost. Package, cluster, replacement, relay-fee, and eviction-rate calculations use Core's sigop-adjusted virtual size: the larger of transaction weight and sigop cost times 20 bytes, rounded by the witness scale factor. These graph checks run on the private candidate pool after conflict removal and package insertion but before publication, fee completion, or capacity eviction, so a failure leaves the live pool and durable snapshot unchanged.

The same active snapshot stores a complete versioned map from txid to its first admission time. Transactions expire once older than Core's default 336-hour lifetime; an expired parent is removed with every retained descendant before caught-up revalidation. Ordinary snapshot replacement preserves surviving times and prunes removed rows. If an expired transaction is independently received or recovered from a reorganization and passes admission again, its time is reset in the same commit that republishes the pool. Legacy snapshots without this map migrate every active entry as newly admitted, while malformed, duplicate, missing, or non-pool rows fail closed and join the persisted-metadata fuzz surface.

Peer transactions that fail admission only because an input is unavailable enter a separate process-shared orphan pool instead of being discarded. Persisted mempool and reorg-recovery candidates are not mislabeled as peer orphans. The pool retains at most 64 transactions and 4 MB, rejects any orphan above the 400,000-weight-unit standard ceiling, deduplicates txid/wtxid variants, randomly evicts entries under pressure, and expires entries after Core's 20-minute lifetime. Missing parents absent from the submitted package, active pool, orphanage, recent-confirmed set, and confirmed UTXO view enter a globally deduplicated 64-txid request set keyed by source. Parent requests deliberately use legacy txid inventory even after BIP339 negotiation, and successful responses feed the ordinary pending-transaction admission queue. Every connected block contributes both txid and distinct wtxid identifiers to an exact oldest-first set capped at Core 26's 48,000-entry rolling-filter capacity. That set suppresses redundant announced-transaction downloads and recognizes confirmed parents even after their outputs are spent; any active-chain disconnection clears it before reorg transactions are reconsidered. When a parent is admitted, only orphans spending one of its actual output indexes enter a work set keyed by the supplying session. The caught-up scheduler pops one transaction from that source at a time, commits its ordinary atomic admission result, yields, and immediately repeats while work remains; accepted children schedule the next generation without validating the entire orphanage in one turn. A separate exact-outpoint index removes orphans included or made impossible by each successfully connected block without deleting transactions that spend another output of the same parent. A still-missing attempted orphan remains eligible for a later parent, while any terminal consensus, policy, conflict, package, or topology failure removes it. A bounded 1,024-entry exact-txid cache remembers only witness-independent terminal failures and clears whenever the active chain tip changes. If an orphan depends on a cached rejected parent, the child is cached and discarded instead of being retained or triggering another blind fetch. Every removal path rebuilds byte accounting and prunes the exact index, live work sets, and source request state. Source identity is a locally assigned monotonic session ID rather than the remote-controlled version nonce, and every remaining entry, work item, and parent request from that source is removed before normal completion or failover activates another peer. The orphan pool, recent-confirmed/reject caches, requests, and scheduler are intentionally memory-only and do not survive process restart.

Capacity eviction now raises a process-local rolling mempool minimum to the aggregate fee rate of the evicted oldest transaction and its descendants plus Core 31's 100 sat/kvB incremental relay rate. Ordinary transactions independently pay the 100 sat/kvB min-relay floor. An exact one-parent-one-child fee-bumping package may carry a parent below that floor, including zero fee, only when the aggregate package pays the effective rolling minimum; deeper, partial, or replacement packages receive no aggregation. The bump cannot decay until a later caught-up chain tip is observed; it then has a 12-hour half-life, accelerated twofold below half capacity and fourfold below quarter capacity, and clears below 50 sat/kvB. Existing entries are not retroactively repriced during active-chain reconciliation. The rolling value is deliberately absent from the durable mempool snapshot and resets on process restart, matching Core's mempool dump boundary.

`fee_estimates.redb` provides a separate network-bound, owner-only empirical fee history. Every active admitted transaction contributes its exact fee, sigop-adjusted policy vsize, and first eligible block height; existing observations preserve that height across restart. Once execution catches up, retained active-chain blocks advance a 1,008-block journal and move matching transactions into confirmed samples. A shallow reorganization reverses those moves exactly, while a deeper reorganization or a gap outside the retained block ring explicitly clears the history and reanchors it instead of mixing chains. History retains at most 4,096 confirmed observations, and the persisted decoder is strictly bounded and fuzzed. Estimates require at least three mature outcomes and select the lowest whole-sat/vB threshold with at least 85% target success; confirmations slower than the target and old-enough pending transactions both count as failures, avoiding confirmation-only optimism. The authenticated `estimatesmartfee [1..1008]` RPC reports Core-compatible BTC/kvB units and an explicit insufficient-data error rather than inventing a default. This is a bounded local empirical estimator, not Bitcoin Core's complete bucket/decay estimator. PSBT creation can select it only through an explicit `confirmation_target`; an exact `fee_rate_sat_vb` remains available and the two modes are mutually exclusive.

For custom activation schedules, transaction admission keeps block consensus and relay policy separate. It first validates with the active next-block flags, then—only when that set is incomplete—rechecks scripts with every Core 26 standard flag exposed by `libbitcoinconsensus`: P2SH, strict DER signatures, NULLDUMMY, CLTV, CSV, Witness, and Taproot. A Core fixture that is valid with no flags but violates DERSIG/NULLDUMMY is rejected through the distinct standard-script policy path without changing the UTXO set or pool. Fully activated production contexts avoid the redundant second interpreter pass. Core's standard lock-time policy is independent as well: version-2 transactions must satisfy BIP68 for the next block even when a custom chain has not activated CSV. Exact height and 512-second boundaries are tested, while the active block path retains its configured activation semantics.

Core 31 package fee aggregation uses the fee-bumping subpackage boundary rather than the entire submitted set. Parents already paying the rolling floor are excluded, so a rich parent cannot subsidize a low-fee child. The implemented below-minimum exception is deliberately limited to one direct parent plus one child; the aggregate must pay the effective floor, and all failures retain rBTC's atomic publication boundary.

Package identity and size checks also follow Core 26's context-free boundary. Duplicate txids within the submitted package are rejected before mempool lookup, but a single submitted transaction whose txid is already admitted is replaced by the pool's witness variant for dependency resolution; an alternate or invalid submitted witness therefore cannot hide the admitted parent's outputs or overwrite it. Multi-transaction packages are capped by one aggregate 404,000-weight-unit total, avoiding false rejection from summing individually rounded virtual sizes. Singleton submissions skip that package-only ceiling and continue through the ordinary per-transaction 400,000-weight-unit standardness check.

BIP125 replacement additionally enforces Core 26's strict direct-conflict feerate rule. The candidate transaction—or rBTC's atomic replacement package as a whole—must have a higher integer sat/kvB rate than every transaction it directly conflicts with, before the existing aggregate-fee and incremental-relay-fee checks can succeed. Paying enough total bandwidth fee while lowering one direct conflict's feerate is rejected without changing the live or durable pool. The 100-entry work ceiling conservatively sums each direct conflict's complete descendant count before deduplication, so shared descendants cannot hide an expensive replacement traversal. Full-RBF bypasses only opt-in signaling and does not weaken either gate.

BIP125's no-new-unconfirmed-input rule follows Core's parent-transaction identity rather than an over-strict exact-outpoint identity. A replacement may switch from one output to another output of an unconfirmed parent already used by a direct conflict, while adding an input from any unrelated mempool parent remains an atomic rejection. This distinction preserves Core-compatible replacement flexibility without allowing low-fee dependency injection.

## API boundary

The embedded REST routes are deliberately typed behind an `ExplorerIndex` trait:

- `GET /api/v1/health`
- `GET /api/v1/status` (dynamic node/projection/storage state)
- `GET /api/v1/ready` (deployment readiness gate)
- `GET /api/v1/events` (SSE persistent explorer-tip changes)
- `GET /api/v1/validation` (present during background AssumeUTXO operation)
- `GET /api/v1/blocks/{height}`
- `GET /api/v1/tx/{txid}`
- `GET /api/v1/address/{address}/utxos?offset=0&limit=50` (maximum page size 100 and offset 10,000)
- `GET /api/v1/wallet/balance`
- `GET /api/v1/wallet/status`
- `GET /api/v1/wallet/descriptors` (canonical public descriptors only)
- `GET /api/v1/wallet/transactions?offset=0&limit=50` (maximum page size 100 and offset 10,000)
- `GET /api/v1/wallet/utxos?offset=0&limit=50` (maximum page size 100 and offset 10,000)
- `POST /api/v1/wallet/address`
- `POST /api/v1/wallet/psbt` (bounded unsigned BIP174 creation)
- `POST /api/v1/wallet/psbt/finalize` (external signatures; Core-verified raw transaction)
- `POST /rpc` (optional authenticated JSON-RPC 2.0)
- `GET /metrics` (Prometheus text exposition)

The embedded page opens the SSE feed and displays the live persistent explorer tip. Every client first receives a `tip` snapshot, followed only by changes committed to `explorer.redb`: `connected`, `disconnected`, or snapshot-aware `rebased`. The broadcast ring is globally bounded at 128 events and 64 simultaneous streams. A client that falls behind receives `resync` with the missed count and must reconnect or reload its REST state; 15-second SSE comments keep idle intermediaries from silently expiring the stream.

`/api/v1/health` is a process-liveness check. `/api/v1/ready` returns 503 during IBD, block catch-up, explorer/wallet reconciliation, or an unsafe disk forecast, and 200 only when header, execution, explorer, and optional wallet tips agree, the configured minimum chainwork is reached, and free space remains above the next-checkpoint threshold. `/api/v1/status` returns the same decision with hashes, phase, AssumeUTXO independence, hot/cold UTXO counts, compressed ledger footprint, and disk total/available/required/reserve bytes. `/metrics` exposes those bounded counters and gauges without reading archive payloads. Dynamic status and metric responses use `Cache-Control: no-store`; all routes remain loopback-only. Add `--rpc-auth-token-file PATH` beside `--explorer-listen` to mount the independently authenticated `/rpc` route. Its bounded methods are `help`, Core-compatible `getblockhash` and `estimatesmartfee`, `rbtc.getblocksummary`, `rbtc.gettransaction`, paged `rbtc.getaddressutxos`, stable operator equivalents of `getblockchaininfo`, `getnetworkinfo`, `getpeerinfo`, `getmempoolinfo`, `getindexinfo`, and `verifychain`, runtime `getloginfo`/`setloglevel`, disk forecast `getdiskinfo`, plus idempotent `stop`. Active-chain operations additionally include cursor-paged `getrawmempool`, `getblockheader`, retained-block metadata through `getblock`, 24 KiB raw pages through `rbtc.getblockchunk`, exact `gettxout`, asynchronous `rbtc.submitrawtransaction`, and asynchronous `rbtc.submitblock`. Submission returns `queued=true` only after entering the bounded ordinary validation queue; it does not claim mempool admission before consensus/policy evaluation. `getblocktemplate` serves the BIP22/23 fields this node can state truthfully, including package-aware fee-optimal transaction selection bounded by both the block weight and sigop limits, a derived block version, and the default witness commitment; `longpollid` is null, proposal mode is absent, and any chainstate not independently validated from genesis is refused outright rather than served a template built on history it has not verified. `rbtc.submitblock` rejects a malformed or under-target block immediately, then waits for the execution loop to stage its header and connect it under the ordinary rules, answering `connected=true` or `connected=false` with a reason; a node that stops before deciding is reported as undecided rather than as a rejection. Blocks are capped at 32 KiB by the shared 64 KiB body limit, which suits regtest fixtures rather than full blocks. Pruned block payloads return a stable unavailable error. The status methods deliberately expose only locally proven state and do not promise exact Core response-field parity. `stop` returns first, then signals the outer CLI or embedded runtime and waits for an in-flight atomic checkpoint before durable stores close. The RPC token file follows the same owner-only, 32–256 printable-byte, atomic-rotation, and fail-closed rules as the wallet token, but the two files must not alias so access scopes stay separate. Requests are strict JSON-RPC 2.0, reject batches, notifications, unknown envelope fields, invalid IDs, oversized methods/parameters, and bodies above 64 KiB, and never expose internal storage errors.

The wallet router accepts public descriptors only. BDK changesets are committed transactionally to an owner-only SQLite file before a derived address is returned; separate monotonically increasing receive and change cursors are reserved first so a crash can skip an address but cannot return or use it twice. Startup rejects a network or descriptor mismatch. Descriptor import supports a bounded `gap_limit` (default 20, maximum 1,000) on both receive and change keychains and an optional `birthday_height` (default 0). It repeatedly replays only fully validated blocks until the unused-script window converges, records the earliest completed scan boundary only after success, and uses sparse validated checkpoints to avoid fetching raw blocks before the birthday. Lowering a birthday or extending the discovered window triggers a durable rescan from the retained ledger or a full-history peer. On reorg, the wallet rewinds to the execution chain's common ancestor before replay.

Authenticated status, balance, canonical transaction history, current UTXO, address, canonical public-descriptor export, unsigned PSBT creation, external-signature finalization, and peer broadcast routes expose the projection with bounded pagination; fees are returned only when BDK knows every input amount. The two-field descriptor export contains checksummed receive/change public descriptors and round-trips through the strict import parser with default scan policy; network, gap, and birthday remain explicit deployment choices rather than hidden export state. Creation requests are strict JSON no larger than 32 KiB, allow 1–16 network-checked non-dust recipients within `MoneyRange`, require exactly one of `fee_rate_sat_vb` from 1–1,000 or `confirmation_target` from 1–1,008, and consider at most 100 inputs. Target mode must resolve from at least three mature local observations to the same 1–1,000 sat/vB wallet bound; unavailable, insufficient, or out-of-range estimates return 503 without a fallback. An optional `selected_utxos` list is exclusive coin control; an empty list uses BDK automatic selection. Creation requires SegWit or Taproot receive/change descriptors and includes only bounded `witness_utxo` input metadata rather than cloning full previous transactions. BDK enables RBF, computes the fee, creates change from the durably reserved cursor, and returns only an unsigned base64 BIP174 object plus its unsigned txid/counts; PSBTs over 512 KiB or containing any signature/finalization field fail closed.

The finalization request is strict JSON capped at 768 KiB and accepts at most a 512 KiB externally signed PSBT with 1–100 inputs and at most 17 outputs. Every input must still be a current wallet UTXO, submitted `witness_utxo` data must exactly match local validated state, full previous transactions and already-finalized inputs are rejected, and signatures must use `SIGHASH_ALL` or Taproot default. BDK assembles the final scripts, after which the pinned Bitcoin Core 26 consensus engine verifies every input against local prevouts. The returned finalized PSBT, raw transaction, txid, wtxid, fee, and virtual size are not broadcast or persisted as a mempool transaction.

The daemon mounts these watch-only routes only when both `--wallet-descriptors PATH` and `--wallet-auth-token-file PATH` accompany a loopback `--explorer-listen`. Both input files must be regular, bounded, and owner-only on Unix. The descriptor JSON keys are `receive_descriptor`, `change_descriptor`, optional `gap_limit`, and optional `birthday_height`; the token is 32-256 printable ASCII bytes and is sent as `Authorization: Bearer TOKEN`. Replace the owner-only token file atomically to rotate it without restarting; the daemon reloads it within one second, invalidates the old credential, and disables wallet authorization if the replacement is missing, malformed, oversized, non-UTF-8, or over-permissive. Every wallet or RPC authorization attempt is synced before route code runs to the owner-only, single-link `api-auth-audit.jsonl` in the data directory. Its bounded records contain only time, method, query-free fixed route path, and accepted/rejected status—never a token, header, query value, body, or response. The 16 MiB log fails closed with HTTP 503 when full or unwritable; archive or replace it while the daemon is stopped before restarting. Wallet responses use `Cache-Control: no-store`; address revelation has its own mutation limiter, while PSBT creation, finalization, and broadcast share a burst of 20 requests and refill one request per minute. `POST /api/v1/wallet/psbt/broadcast` accepts the same signed non-final PSBT as finalization, repeats all current-UTXO/fee/script checks, applies the separate wallet-origin relay policy, durably queues it before handoff, and returns 400 for policy/conflict rejection or 503 if persistence, bounded queueing, or peer failover do not produce a complete active-socket write within 35 seconds. After that write, every hot standby receives the transaction independently; a timed-out caller loses only its response, not the already durable rebroadcast record. Tokens and descriptors are never accepted directly on the command line or printed. Private descriptors, in-process signing, encrypted secret storage, and a complete Core-style transaction-relay lifecycle remain disabled.
