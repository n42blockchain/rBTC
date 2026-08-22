# Changelog

All notable user-visible changes will be recorded here. Versions follow
Semantic Versioning; a release tag must exactly equal `v` plus the version in
`Cargo.toml`.

## [Unreleased]

### Added

- `--snapshot-overlay-flush-batches N` (1–64) and
  `--snapshot-overlay-flush-coins` put an engine-agnostic write-back layer in
  front of the MDBX or redb overlay during snapshot catch-up: the last N
  batches stay in memory, reads consult the buffer first, and one engine
  commit folds away every coin created and spent inside the window. At the
  default of one batch the engine runs unchanged. Over mainnet
  935,001–963,350 with 16 buffered batches, 81% of the created coins
  (113M) cancelled in memory on both engines: redb catch-up fell from
  3,289 s to 2,606 s (44,689 tx/s, 120 GB written instead of 299 GB) and
  MDBX from 3,823 s to 3,057 s, all lanes at the same final tip; peak
  working set rose by 12–13 GiB at the default 8M-coin buffer.
- The Core snapshot index gains a `<index>.fp` sidecar with one 16-bit txid
  fingerprint per slot, written by the builder and created once on first
  open for older indexes. Lookups reject a key whose slot fingerprint
  differs without touching the offset table or the snapshot, which removes
  nearly every base read behind the overlay commit's duplicate-creation
  probe and the prefetch's absent keys. Over mainnet 935,001–963,350 with
  16 buffered batches, MDBX catch-up fell from 3,057 s to 2,314 s
  (50,340 tx/s, 93 GB written) and redb from 2,606 s to 2,407 s, all with
  the same overlay content digest; a resumed catch-up on the original base
  now keeps the operator-supplied index path instead of deriving one.
- `overlay_audit` (`--features mdbx`) hashes the consensus content of an
  MDBX or redb snapshot overlay read-only — base identity, tip, every
  post-base coin's value/height/coinbase flag/creation MTP/script in key
  order, and every tombstone — so overlays built by different engines or
  commit strategies compare as one digest. The four 2026-08-21 lanes all
  hashed to `aadd289f…7f2819` with 14,554,294 coins and 13,000,529
  tombstones.
- `fdb_ledger_import` imports a btcd flat block file set into a
  `PrunedBlockLedger` (read-only source, chain selection from a base hash,
  CRC-32C checks, ranged re-verification), and `utxo_locality` reports the
  spent-output age histogram of a ledger. Using them, a real-block overlay
  catch-up over mainnet 935,001–963,350 compared the MDBX and redb overlays
  under identical settings: redb finished in 3,289 s against MDBX's 3,823 s
  with 42% less memory and 38% fewer bytes written, both reaching the same
  tip. Over that window 80.7% of inputs spend outputs at most 256 blocks old.
  See `docs/REAL_BLOCK_OVERLAY_REPLAY_2026-08-21.md`.

### Changed

- The feature-gated MDBX chainstate now uses exactly four tables (`utxo_hot`,
  `utxo_cold`, `undo`, and `meta`). Its 34–37-byte physical outpoint keys use a
  wire-order txid plus an order-preserving width-tagged big-endian vout; coin and
  undo values use Bitcoin Core/btcd VLQ amount/script compression, omitting
  obsolete per-coin wall-clock state. Hot/cold placement is height-only, and
  UTXOs, per-block undo, and tip commit atomically. A 256-block IBD transaction
  folds outputs created and spent inside the batch and rejects larger batches.
  The daemon continues to select redb until MDBX migration and operational
  gates are completed.

- The btcd-codec storage comparison now drives pinned LevelDB, Pebble,
  Badger, and bbolt versions through the same synchronous atomic
  UTXO/undo/tip mutation. It records oversized-transaction rejection,
  quiescence, compaction, allocated bytes, target/completed transitions, and a
  serial approximately one-hour 900,000-transition timebox without labelling
  the synthetic workload as btcd IBD or a mainnet height replay. The first
  clean Mac timebox records 3,108–12,033 completed serving transitions,
  sustained Pebble IBD falling behind LevelDB, and Badger rejecting the first
  required atomic IBD checkpoint; the default database decision is unchanged.

- MDBX replacement preparation now audits and hashes all four chainstate
  tables before and after compact-copy, carries that identity through a
  recoverable directory-swap manifest, preflights copy space with a 16 GiB
  reserve, applies a 55%-capacity/10%-reclaim/50%-growth maintenance policy,
  and prunes
  undo through authenticated header heights. Abrupt subprocess exits cover all
  five copy/rename/fsync boundaries. A resumable ignored gate drives 160M live
  coins and up to 900,000 synthetic churn transitions in separately measured
  64/256 lanes; full-scale and real-mainnet replay evidence remain required
  before selecting MDBX by default.

- The root data-format manifest now records the chainstate backend explicitly
  and advances to schema 4. Existing schema-3 redb directories migrate
  atomically, while a manifest naming MDBX is rejected by the redb-only node
  before either backend is opened; this prevents a rollback binary from
  silently creating or serving the wrong chainstate during the staged switch.

- MDBX evaluation text now incorporates the corrected btcd full scans. The
  long-running pruned-undo store held 33.42 GB raw rather than the biased
  11.7 GB estimate (1.50x live, 2.27x file), while a sequential stock-btcd
  replay measured 1.155x live amplification with almost no freelist. The 34%
  churn freelist and compact-copy requirement remain real, but the former
  claim of intrinsic 4.3x MDBX live amplification is withdrawn.

- TRUC (v3) transactions are implicitly replaceable, and a second v3 child
  of a one-child v3 parent now displaces its sibling through the full
  replacement rules (feerate diagram included) instead of failing the
  topology outright — Core's sibling eviction, single transactions only.
  A package can no longer slip a second child past the one-child topology.

- Mempool replacement now follows Bitcoin Core 31's cluster rule: a
  replacement is accepted only when the affected clusters' feerate diagram
  is strictly better afterwards. The BIP125 signaling, absolute-fee,
  incremental-fee, and eviction-bound rules are unchanged; retired is only
  the former "feerate must exceed each direct conflict" heuristic, whose
  question the diagram answers exactly. A live differential against Bitcoin
  Core v31.0.0 agreed on every verdict, including the rich-descendant
  eviction the old heuristic accepted wrongly. The new authenticated
  `getmempoolfeeratediagram` RPC reports the chunks the rule compares.

### Added

- ASN-based peer-address diversity (asmap). A Core-compatible asmap
  interpreter with a real 2026-08 `bitcoin-core/asmap-data` map embedded in
  the binary groups addresses by announcing autonomous system for source
  quotas, bucketing, and candidate diversification; `--asmap
  <path>|embedded|off` (config key `asmap`) selects an operator file or
  disables the derivation. Data provenance and the update procedure are in
  `docs/ASMAP.md`.

- Pure feerate-diagram primitives (`rbtc::feerate_diagram`): overflow-free
  fee/size feerate comparison, validated cluster DAGs, ancestor-set greedy
  linearization, Core-shaped chunking, and feerate-diagram comparison, with
  property tests and a fuzz target. The groundwork the
  diagram-replacement rule above is built on.

- Operator RPCs `addnode`, `gettxoutsetinfo`, and `getmempoolcluster`, all
  authenticated and bounded. `addnode <ip:port> add|onetry|remove` seeds or
  forgets a routable dial candidate; `gettxoutsetinfo [cursor,max]`
  aggregates the UTXO set in resumable bounded windows; `getmempoolcluster
  <txid>` reports a transaction's dependency-connected cluster with its
  count, vsize, and the enforced cluster bounds.

- Private transaction broadcast. `--private-broadcast` (config key
  `private_broadcast`) makes locally originated wallet transactions travel
  exclusively over anonymity networks — onion peers through the Tor SOCKS5
  proxy, I2P peers through a fresh keyless SAM session — never over a
  clearnet peer session or the hot-standby relay fan-out. It requires at
  least one anonymity path and is refused at startup otherwise; an
  undeliverable transaction stays queued rather than falling back to
  clearnet. Semantics are documented in `docs/PRIVATE_BROADCAST.md`.

- CJDNS overlay reachability. `--cjdns-reachable` (config key
  `cjdns_reachable`) declares a local `cjdroute` interface, making
  `fc00::/8` peers storable, dialable, and advertisable; `--onlynet cjdns`
  restricts outbound peers to the overlay. Overlay addresses travel only as
  the BIP155 CJDNS network ID, share one diversity marker group, and every
  unsound configuration combination is refused at startup. Semantics are
  documented in `docs/CJDNS.md`.

- Inbound I2P peers are accepted through the configured SAM session and
  served by the existing inbound service, sharing its capacity and budgets.

- Bitcoin, legacy testnet, Testnet4, Signet, and regtest full-node validation.
- Core 31-compatible AssumeUTXO activation with independent background
  validation.
- Version-2 Core snapshot access indexes keyed by txid group, with batched
  file-ordered lookups and a bounded maximum group span. The measured
  height-935,000 mainnet sidecar is 530,926,239 bytes, 54.1% smaller than the
  former outpoint-keyed overlay format.
- Experimental bounded snapshot-backed catch-up on MDBX or redb overlays,
  including compact/rebase maintenance and a configurable hard or
  policy-enforced capacity budget.
- Bounded outbound and optional inbound P2P services, persistent mempool,
  pruned freezer, optional indexes, authenticated operator APIs, and a
  watch-only external-signer wallet.
- Cross-platform signed-release, SBOM, provenance, reproducibility, audit,
  fuzzing, and public-network soak gates.
- Opt-in BIP324 v2 encrypted transport for outbound peers (`--v2-transport`),
  with the specified one-shot v1 retry when a peer closes the v2 attempt.
- Minimal local block assembly for low-difficulty networks, removing the
  external-daemon dependency from regtest block production in tests.
- Optional loopback ZMQ notification endpoint (`--zmq-listen`) publishing
  Core-compatible `hashblock`, `rawblock`, `hashtx`, `rawtx`, and `sequence`
  topics with bounded slow-subscriber drop accounting.
- Authenticated `testmempoolaccept` returning bounded Core-shaped dry-run
  admission verdicts without mutating the mempool.
- Authenticated `rbtc.scanchainstate` cursor-paged bounded UTXO-set scan.
- Authenticated `listbanned` and `setban` administering durable local peer
  cooldowns.
- Validated v3 onion outbound destinations over SOCKS5 domain addressing.
- A separate bounded onion address book persisting learned v3 services.
- `--onlynet onion` with fail-closed proxy and combination checks.
- A bounded Tor control-port client publishing ephemeral v3 onion services
  with SAFECOOKIE authentication.
- Outbound scheduling of persisted onion peers, with address-type-aware
  `PeerTargetConnected`/`PeerTargetDisconnected` events alongside the
  unchanged socket-typed events.
- `--torcontrol` publishing an inbound onion service with an owner-only,
  reusable service key.
- Announcement of the published onion service to BIP155 peers in both
  directions.
- An I2P SAM v3 client with validated BIP155 `.b32.i2p` addressing.
- A separate bounded I2P address book with `addrv2` ingestion and
  announcement.
- `--i2psam` and `--onlynet i2p` dialling persisted I2P peers through a SAM
  session with a reusable owner-only destination key.

### Fixed

- The I2P SAM session identifier carries a random suffix, so a node
  restarting before its bridge released the previous session no longer
  collides with itself.

- The I2P SAM session identifier is derived per node instead of fixed, so a
  second node on the same bridge no longer fails with `DUPLICATED_ID`.

- `--onlynet onion` and `--onlynet i2p` no longer resolve DNS seeds before
  discarding every answer, which leaked the consulted seeds over the clear
  net.
- The published I2P destination is now announced to BIP155 peers; previously
  the announcement path existed but was never called.
- A non-ASCII Tor control reply is rejected instead of panicking on a
  character-boundary split, before authentication.

- I2P SAM replies are read line-exactly instead of through a buffered
  reader, which discarded a coalesced follow-up reply and could have
  consumed the first bytes of a connected peer's stream. SAM command lines
  are written atomically for i2pd framing, and replayed destination keys omit
  the transient-only signature option.

### Safety

- Snapshot-overlay undo remains readable across the compression upgrade;
  marked zstd records enforce 256 MiB output and 8 MiB window ceilings.
- Interrupted MDBX compaction/rebase swaps restore an unambiguous set-aside
  environment and preserve all candidates while failing closed on ambiguity.
- Atomic validation checkpoints default to 256 blocks after measured replay;
  `--validation-batch-size` remains available for lower-memory hosts.
- `2026-08-08` acceptance refresh: real daemon verification passed for
  BIP324-v2 transport fallback interoperability (`v2_transport_interoperates_with_core`
  and `v2_preference_falls_back_to_v1_against_a_v1_only_core`), real inbound
  Core 31/btcd v1 handshakes, Tor v3 onion/SOCKS and i2pd SAM interoperability,
  and bounded ZMQ topic/filter/subscriber tests.
- rBTC remains pre-release until an accepted seven-day public-network soak
  report and the first native signed release complete. It must not hold
  mainnet private keys.

### Productization

- Added runtime version reporting, side-effect-free configuration validation,
  tag/package/binary version binding, native release artifact smoke tests, and
  a private vulnerability-reporting policy.

[Unreleased]: https://github.com/n42blockchain/rBTC/compare/v0.1.0...HEAD
