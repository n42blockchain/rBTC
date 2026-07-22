# rBTC

High-performance Rust Bitcoin node kernel, designed around a compact and verifiable UTXO set.

## What is implemented now

- Protocol-compatible Bitcoin P2P v1 message framing through `rust-bitcoin`; no custom wire format.
- Script validation adapter using Bitcoin Core's `libbitcoinconsensus`, including Taproot spent-output and default-Signet BIP325 block-solution validation.
- Pure-Rust redb chainstate with hot/cold UTXOs, per-block undo, and execution tip committed together in one physical database transaction; IBD supports multi-block durable checkpoints.
- Deterministic zstd UTXO snapshots, SHA-256 verification, mandatory header-anchor check, and AssumeUTXO-style background-validation contract.
- Immutable zstd block archives with 4 MiB piece hashes, ready for a BitTorrent/webseed transport adapter.
- Configurable circular pruned ledger: defaults are 1,008 blocks (about one week) and 1 GiB. Validated IBD batches are published through a restart-safe staging protocol; only old block archives rotate, while UTXO state and headers are retained.
- Embedded REST router contracts for a block explorer plus a transactionally persisted BDK watch-only descriptor wallet façade.

## Important safety status

rBTC is **not yet a production full node** and must not be trusted with mainnet funds. Durable regtest and default-Signet headers-first/block IBD, cumulative-work fork choice, persistent explorer projections, and crash-safe watch-only wallet address derivation are implemented, but complete mainnet deployment activation and block rules, the P2P peer manager, encrypted wallet signing, authenticated API serving, and release hardening remain completion gates. The exact plan is in [docs/ROADMAP.md](docs/ROADMAP.md).

## Design choices

| Concern | Choice | Reason |
| --- | --- | --- |
| Bitcoin types and v1 P2P encoding | `rust-bitcoin` | Maintained Rust Bitcoin primitives and consensus serialization. |
| Script interpreter | `bitcoinconsensus` | Reuses Bitcoin Core's consensus library, including Taproot spent-output API. |
| UTXO persistence | redb default; optional MDBX experiment | redb keeps default builds pure Rust; `--features mdbx` enables a durable hot/cold UTXO comparison backend, not yet a production chainstate selector. |
| Wallet | BDK (`bdk_wallet`) | Descriptor, PSBT, coin selection, signing, and sync model without reimplementing wallet correctness. |
| Compression | zstd | Fast decompression and high ratio for snapshots and static block segments. |

## Local checks

```bash
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-features
RBTC_BITCOIND=/path/to/bitcoin-core-26/bin/bitcoind cargo test --test core_block_differential -- --ignored --nocapture
cargo test --release --all-features --test storage_bench -- --ignored --nocapture
```

The optional live differential gate requires the matching `bitcoin-cli` beside a Bitcoin Core 26.0 `bitcoind`. It submits the same mined regtest blocks to Core and through rBTC's production header-DAG/block-connection path, including atomic rejection checks for the persisted tip, undo record, and candidate UTXO.

For the current safety-gated validating daemon, `rbtcd --connect HOST:PORT --network regtest|signet --data-dir PATH` stays attached and polls the peer for new headers every 30 seconds. Repeat `--connect HOST:PORT` to provide up to 16 ordered, deduplicated fallback peers. A failed handshake, missing full-history/witness service, interrupted headers or block transfer, or rejected response advances to the next candidate; durable headers and atomic chainstate let that peer resume the same IBD. Sync completion remains based on validated cumulative work, never the peer's untrusted advertised height. This is explicit peer failover, not yet a learned addrman or concurrent peer manager. The `signet` choice means Bitcoin Core's default global Signet parameters and BIP325 challenge; custom Signet parameters are not yet accepted. Add `--once` for a bounded sync-and-exit run. Add `--explorer-listen 127.0.0.1:3000` to serve the embedded read-only explorer and REST API; non-loopback binds are rejected until authentication is implemented. Regtest Taproot activation can be overridden with Core-compatible `--vbparams taproot:START:END[:MIN_HEIGHT]`. Buried deployments accept repeatable Core-compatible `--testactivationheight NAME@HEIGHT`, where `NAME` is `segwit`, `bip34`, `dersig`, `cltv`, or `csv`; the last value for a name wins. The complete selected consensus configuration is bound to a fresh execution database and cannot later change in place. Core 26 minimum-chainwork and assume-valid defaults are loaded per supported legacy network and can be overridden with `--minimum-chainwork HEX` and `--assumevalid HASH|0`. A chain below the work floor remains in IBD. Assume-valid currently identifies a reviewed active-chain anchor only: all scripts are still verified. Mainnet, legacy testnet, and testnet4 may probe or persist headers, but `--data-dir` block execution is rejected before connecting until their remaining acceptance gates close.

## API boundary

The embedded REST routes are deliberately typed behind an `ExplorerIndex` trait:

- `GET /api/v1/health`
- `GET /api/v1/blocks/{height}`
- `GET /api/v1/tx/{txid}`
- `GET /api/v1/address/{address}/utxos?offset=0&limit=50` (maximum page size 100 and offset 10,000)
- `GET /api/v1/wallet/balance`
- `POST /api/v1/wallet/address`

The wallet router currently accepts public descriptors only. BDK changesets are committed transactionally to an owner-only SQLite file before a derived address is returned; startup rejects a network or descriptor mismatch so a receive address cannot silently be reused after restart. The daemon still does not mount these routes. Wallet endpoints require authentication, CSRF protection for browser sessions, audit/rate limits, and an explicit transaction/signing/broadcast policy before they are enabled. Private descriptors remain rejected until encrypted secret storage is implemented.
