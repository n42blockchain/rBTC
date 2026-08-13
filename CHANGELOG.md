# Changelog

All notable user-visible changes will be recorded here. Versions follow
Semantic Versioning; a release tag must exactly equal `v` plus the version in
`Cargo.toml`.

## [Unreleased]

### Added

- ASN-based peer-address diversity (asmap). A Core-compatible asmap
  interpreter with a real 2026-08 `bitcoin-core/asmap-data` map embedded in
  the binary groups addresses by announcing autonomous system for source
  quotas, bucketing, and candidate diversification; `--asmap
  <path>|embedded|off` (config key `asmap`) selects an operator file or
  disables the derivation. Data provenance and the update procedure are in
  `docs/ASMAP.md`.

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
