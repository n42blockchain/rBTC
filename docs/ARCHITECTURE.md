# Architecture

## Data flow

```text
Bitcoin peers (v1 now; BIP324 v2 later)
        │
        ▼
header DAG + chainwork → contextual validator → libbitcoinconsensus scripts
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

Block validation runs against a lazy in-memory UTXO overlay and commits the net effect in one redb transaction. Connect and disconnect operations first persist a write-ahead record containing the parent/child tips, aggregate undo, and expected post-state. Since undo and execution metadata currently live in separate redb files, restart recovery compares the touched UTXOs with both recorded states and then idempotently rolls back an incomplete connect or finishes an intended disconnect. A mixed state is treated as corruption rather than guessed through.

## Snapshot trust model

The snapshot includes an anchor height/hash, count, and a SHA-256 of its canonical uncompressed entry stream. The importer verifies container integrity and checks the anchor against its validated header chain before atomically populating a caller-provided staging chainstate. The daemon service layer must keep the existing chainstate active, synchronize the staged state from the anchor, and validate history from genesis in the background before promotion. It must not declare independently verified state until background validation reaches that anchor. This mirrors Bitcoin Core's AssumeUTXO operational model, but rBTC's container format is intentionally separate.

## Pruned historical ledger

`PrunedBlockLedger` stores zstd-compressed block segments in numbered ring slots. Its policy has both a block-count and byte ceiling; the default `1008` blocks / `1 GiB` means approximately one week of ten-minute blocks. A new segment is first completed in a temporary file and renamed into a slot. The live index then retains the newest contiguous segments satisfying both bounds. Old block bytes are no longer locally queryable after rotation; headers and UTXO state remain. On startup the ledger validates indexed slot manifests, adopts a complete contiguous segment whose rename beat its index commit, and reconstructs a missing/corrupt index from the newest contiguous slot chain. Reorg truncation durably records its boundary before deleting newer segments or atomically rewriting a crossing segment, so restart repeats the operation safely.

IBD first writes each downloaded batch to a checksum-protected staging archive. Blocks become visible in the retained ledger only after their UTXO transitions have reached the durable execution tip. On restart the daemon truncates archive data above the recovered active execution tip, publishes only the active validated prefix of a staged batch, and backfills a missing retained suffix from a full-history witness peer. This coordinates the separate redb chainstate and file ring without claiming an atomic transaction across storage engines.

For torrent distribution, the `ArchiveManifest` has stable 4 MiB compressed-piece SHA-256s. A future transport adapter can map them directly to torrent v2 pieces or verify webseed/range downloads before decompression. It must validate each recovered block through the normal chain validator; archive checksums are not consensus proof.

## Explorer and wallet

The explorer is embedded but logically read-only: a redb projection atomically stores active block summaries, transaction confirmations, script-hash keyed current UTXOs, and per-block rollback data. Its durable tip is reconciled against the execution tip on startup; missing projections are replayed from the retained ledger or fetched from a full-history peer, while stale projections use their independent undo records. The Axum router and CSP-constrained static page read this index without exposing chainstate mutation; the daemon accepts only explicit loopback listener addresses. Address UTXO queries validate input and apply capped offset/limit pagination in the redb range iterator, so a request cannot materialize an unbounded result. The wallet is BDK descriptor-based and runs in-process, which avoids putting keys in a browser or inventing PSBT/signing behavior. Its watch-only state uses BDK's transactional SQLite persister under one process mutex: address revelation is committed before response, and reopen checks both descriptors and the network. The database is owner-only on Unix and symlink paths are rejected. Secret descriptors are rejected rather than stored unencrypted. Wallet endpoints remain disabled until encrypted secret storage, authentication, audit logs, rate limits, and signing/broadcast test coverage are complete.
