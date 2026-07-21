# rBTC

High-performance Rust Bitcoin node kernel, designed around a compact and verifiable UTXO set.

## What is implemented now

- Protocol-compatible Bitcoin P2P v1 message framing through `rust-bitcoin`; no custom wire format.
- Script validation adapter using Bitcoin Core's `libbitcoinconsensus`, including Taproot spent-output validation.
- Pure-Rust redb UTXO chainstate with separate hot/cold tables, atomic whole-block transitions, durable undo, reorg rewind, and interrupted-transition recovery.
- Deterministic zstd UTXO snapshots, SHA-256 verification, mandatory header-anchor check, and AssumeUTXO-style background-validation contract.
- Immutable zstd block archives with 4 MiB piece hashes, ready for a BitTorrent/webseed transport adapter.
- Configurable circular pruned ledger: defaults are 1,008 blocks (about one week) and 1 GiB. Validated IBD batches are published through a restart-safe staging protocol; only old block archives rotate, while UTXO state and headers are retained.
- Embedded REST router contracts for a block explorer plus a transactionally persisted BDK watch-only descriptor wallet façade.

## Important safety status

rBTC is **not yet a production full node** and must not be trusted with mainnet funds. Durable regtest headers-first/block IBD, cumulative-work fork choice, persistent explorer projections, and crash-safe watch-only wallet address derivation are implemented, but complete mainnet deployment activation and block rules, the P2P peer manager, encrypted wallet signing, authenticated API serving, and release hardening remain completion gates. The exact plan is in [docs/ROADMAP.md](docs/ROADMAP.md).

## Design choices

| Concern | Choice | Reason |
| --- | --- | --- |
| Bitcoin types and v1 P2P encoding | `rust-bitcoin` | Maintained Rust Bitcoin primitives and consensus serialization. |
| Script interpreter | `bitcoinconsensus` | Reuses Bitcoin Core's consensus library, including Taproot spent-output API. |
| UTXO persistence | redb | Pure-Rust ordered copy-on-write B-trees, ACID transactions, and concurrent reads keep local builds portable. |
| Wallet | BDK (`bdk_wallet`) | Descriptor, PSBT, coin selection, signing, and sync model without reimplementing wallet correctness. |
| Compression | zstd | Fast decompression and high ratio for snapshots and static block segments. |

## Local checks

```bash
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-features
```

For the current safety-gated regtest daemon, `rbtcd --connect HOST:PORT --network regtest --data-dir PATH` stays attached and polls the peer for new headers every 30 seconds. Add `--once` for a bounded sync-and-exit run. Add `--explorer-listen 127.0.0.1:3000` to serve the embedded read-only explorer and REST API; non-loopback binds are rejected until authentication is implemented.

## API boundary

The embedded REST routes are deliberately typed behind an `ExplorerIndex` trait:

- `GET /api/v1/health`
- `GET /api/v1/blocks/{height}`
- `GET /api/v1/tx/{txid}`
- `GET /api/v1/address/{address}/utxos`
- `GET /api/v1/wallet/balance`
- `POST /api/v1/wallet/address`

The wallet router currently accepts public descriptors only. BDK changesets are committed transactionally to an owner-only SQLite file before a derived address is returned; startup rejects a network or descriptor mismatch so a receive address cannot silently be reused after restart. The daemon still does not mount these routes. Wallet endpoints require authentication, CSRF protection for browser sessions, audit/rate limits, and an explicit transaction/signing/broadcast policy before they are enabled. Private descriptors remain rejected until encrypted secret storage is implemented.
