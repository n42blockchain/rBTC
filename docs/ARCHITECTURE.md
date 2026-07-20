# Architecture

## Data flow

```text
Bitcoin peers (v1 now; BIP324 v2 later)
        │
        ▼
header chain → contextual validator → libbitcoinconsensus scripts
        │                                  │
        ├────────────── validated blocks ──┤
        ▼                                  ▼
pruned circular ledger                 redb chainstate
zstd + piece hashes                    hot table / cold table
        │                                  │
        ▼                                  ├── verified UTXO snapshot
embedded explorer index                 └── wallet sync source
        │                                          │
        └──────────── REST / embedded browser ────┘
```

## UTXO layout

Each key is the Bitcoin outpoint's 32-byte txid in wire order plus a little-endian `vout`. The record stores amount, creating height, coinbase marker, last-touch time, and raw `scriptPubKey`. `utxo_hot` is the write-optimized active tier; `utxo_cold` contains coins not touched within `hot_window_secs` (default 60 days). Moving tiers is a single redb transaction and never changes consensus data.

redb is selected for its pure-Rust, ordered copy-on-write B-tree tables, ACID transactions, concurrent readers, and crash-safe default behavior. UTXO state is overwhelmingly point lookups plus batched deletes/inserts and needs ordered snapshot iteration. An optional RocksDB backend can be reconsidered after reproducible toolchain packaging and comparative benchmark results; production tuning must come from the target disk/CPU profile rather than copied defaults.

## Snapshot trust model

The snapshot includes an anchor height/hash, count, and a SHA-256 of its canonical uncompressed entry stream. The importer verifies container integrity and checks the anchor against its validated header chain before atomically populating a caller-provided staging chainstate. The daemon service layer must keep the existing chainstate active, synchronize the staged state from the anchor, and validate history from genesis in the background before promotion. It must not declare independently verified state until background validation reaches that anchor. This mirrors Bitcoin Core's AssumeUTXO operational model, but rBTC's container format is intentionally separate.

## Pruned historical ledger

`PrunedBlockLedger` stores zstd-compressed block segments in numbered ring slots. Its policy has both a block-count and byte ceiling; the default `1008` blocks / `1 GiB` means approximately one week of ten-minute blocks. A new segment is first completed in a temporary file and renamed into a slot. The live index then retains the newest contiguous segments satisfying both bounds. Old block bytes are no longer locally queryable after rotation; headers and UTXO state remain. Index/slot recovery after power loss is explicitly a remaining release gate; the file rename prevents partially written archive files, but does not yet provide atomic multi-file metadata recovery.

For torrent distribution, the `ArchiveManifest` has stable 4 MiB compressed-piece SHA-256s. A future transport adapter can map them directly to torrent v2 pieces or verify webseed/range downloads before decompression. It must validate each recovered block through the normal chain validator; archive checksums are not consensus proof.

## Explorer and wallet

The explorer is embedded but logically read-only: the index implementation supplies block, tx, and address-UTXO projections to the Axum router. The wallet is BDK descriptor-based and runs in-process, which avoids putting keys in a browser or inventing PSBT/signing behavior. Sensitive endpoints stay loopback-only until durable changeset storage, encryption, auth, audit logs, rate limits, and test coverage are complete.
