# rBTC

High-performance Rust Bitcoin node kernel, designed around a compact and verifiable UTXO set.

## What is implemented now

- Protocol-compatible Bitcoin P2P v1 message framing through `rust-bitcoin`; no custom wire format.
- Script validation adapter using Bitcoin Core's `libbitcoinconsensus`, including Taproot spent-output validation.
- Pure-Rust redb UTXO chainstate with separate hot and cold tables and atomic cross-tier transactions.
- Deterministic zstd UTXO snapshots, SHA-256 verification, mandatory header-anchor check, and AssumeUTXO-style background-validation contract.
- Immutable zstd block archives with 4 MiB piece hashes, ready for a BitTorrent/webseed transport adapter.
- Configurable circular pruned ledger: defaults are 1,008 blocks (about one week) and 1 GiB. Only old block archives rotate; UTXO state and headers are retained.
- Embedded REST router contracts for a block explorer plus a BDK descriptor wallet façade.

## Important safety status

rBTC is **not yet a production full node** and must not be trusted with mainnet funds. In particular, the service loop, full contextual block validation, header-chain fork choice, P2P peer manager, address/transaction indexer, wallet persistence, authentication, and release hardening are completion gates. The exact plan is in [docs/ROADMAP.md](docs/ROADMAP.md).

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

## API boundary

The embedded REST routes are deliberately typed behind an `ExplorerIndex` trait:

- `GET /api/v1/health`
- `GET /api/v1/blocks/{height}`
- `GET /api/v1/tx/{txid}`
- `GET /api/v1/address/{address}/utxos`
- `GET /api/v1/wallet/balance`
- `POST /api/v1/wallet/address`

Bind the eventual daemon to loopback by default. Wallet endpoints require authentication, encrypted key material, durable BDK changesets, CSRF protection for browser sessions, and an explicit broadcast policy before being exposed beyond localhost.
