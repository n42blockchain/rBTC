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

Each key is the Bitcoin outpoint's 32-byte txid in wire order plus a little-endian `vout`. The record stores amount, creating height, coinbase marker, last-touch time, and raw `scriptPubKey`. Outputs whose script begins with `OP_RETURN` or exceeds Core's 10,000-byte script limit affect transaction value accounting but are never inserted into chainstate or the explorer UTXO projection, matching `CScript::IsUnspendable`. `utxo_hot` is the write-optimized active tier; `utxo_cold` contains coins not touched within `hot_window_secs` (default 60 days). Moving tiers is a single redb transaction and never changes consensus data.

redb is selected for the default node because its pure-Rust, ordered copy-on-write B-tree tables, ACID transactions, and concurrent readers keep the build portable. UTXO state is overwhelmingly point lookups plus batched deletes/inserts and needs ordered snapshot iteration. Active UTXOs, per-block undo, and the execution tip now share one physical database and one write transaction; a successful commit exposes all three and an aborted commit exposes none. Legacy split files are rejected instead of being guessed or upgraded in place.

Block validation runs against a lazy in-memory UTXO overlay and commits the net effect in one redb transaction. redb immediate durability and quick-repair/two-phase commit are enabled for active-chain commits. During IBD, up to 16 already validated contiguous blocks form one durable checkpoint while retaining an undo record for every block; once only one new tip block is available it is committed alone. The acceptance invariant is always an old complete checkpoint or a new complete checkpoint, never a mixed UTXO/undo/tip state.

The `mdbx` Cargo feature provides an experimental durable MDBX hot/cold UTXO backend. It is not a production chainstate selector yet because undo and tip metadata must first be moved into the same MDBX transaction. On the local 100-block/100-spend+create release fixture, durable MDBX completed in about 39 ms versus redb's 733 ms without quick repair and 1.43 s with quick repair; those numbers are a direction signal, not a deployment decision, and must be repeated on target NVMe/HDD hardware with full block undo and metadata included.

Recovery gates cover transaction-stage failure, simulated disk-full writes, repeated process SIGKILL followed by reopen, and truncated database copies. A damaged file must either reopen to a complete committed state or be rejected explicitly; it must never be served as partially current chainstate.

## Snapshot trust model

The snapshot includes an anchor height/hash, count, and a SHA-256 of its canonical uncompressed entry stream. The importer verifies container integrity and checks the anchor against its validated header chain before atomically populating a caller-provided staging chainstate. The daemon service layer must keep the existing chainstate active, synchronize the staged state from the anchor, and validate history from genesis in the background before promotion. It must not declare independently verified state until background validation reaches that anchor. This mirrors Bitcoin Core's AssumeUTXO operational model, but rBTC's container format is intentionally separate.

## Pruned historical ledger

`PrunedBlockLedger` stores zstd-compressed block segments in numbered ring slots. Its policy has both a block-count and byte ceiling; the default `1008` blocks / `1 GiB` means approximately one week of ten-minute blocks. A new segment is first completed in a temporary file and renamed into a slot. The live index then retains the newest contiguous segments satisfying both bounds. Old block bytes are no longer locally queryable after rotation; headers and UTXO state remain. On startup the ledger validates indexed slot manifests, adopts a complete contiguous segment whose rename beat its index commit, and reconstructs a missing/corrupt index from the newest contiguous slot chain. Reorg truncation durably records its boundary before deleting newer segments or atomically rewriting a crossing segment, so restart repeats the operation safely.

IBD first writes each downloaded batch to a checksum-protected staging archive. Blocks become visible in the retained ledger only after their UTXO transitions have reached the durable execution tip. On restart the daemon truncates archive data above the recovered active execution tip, publishes only the active validated prefix of a staged batch, and backfills a missing retained suffix from a full-history witness peer. This coordinates the separate redb chainstate and file ring without claiming an atomic transaction across storage engines.

Consensus deployment selection is explicit at every header, structure-validation, and block-execution call. Network defaults mirror the pinned Core 26 parameters; regtest accepts Core's `taproot:start:end[:min_activation_height]` version-bits override with the 144-block/108-signal window and repeatable `name@height` buried overrides for SegWit, BIP34, DER signatures, CLTV, and CSV. Those heights jointly select minimum header versions, BIP34 coinbase commitments, BIP68/113 locks, BIP141 commitments, and BIP147 NULLDUMMY. Matching Core 26, ordinary blocks always receive the mutually dependent P2SH, WITNESS, and TAPROOT interpreter flags; only Core's three historical exception hashes replace that base set. Keeping this interpreter set separate from `segwit_active` prevents both premature commitment enforcement and unsafe `libbitcoinconsensus` flag combinations. A canonical deployment encoding is bound when a fresh execution database is initialized and cannot be changed in place, even while the recorded tip is genesis, because another store may contain an interrupted transition. The default and Taproot-only encoding remains byte-compatible with existing databases; a buried override uses a versioned extension containing all five heights. A restart with different parameters is rejected before header replay, recovery, or block application; older databases can migrate only under the legacy default.

Consensus script regression tests vendor Bitcoin Core 26's complete transaction and script JSON files byte-for-byte, parse Core's script-assembly syntax, and use the same rBTC adapter as block connection. The transaction corpus executes all 119 valid cases plus the 70 invalid cases expressible through public `libbitcoinconsensus` flags; the script corpus parses all 1,207 tests and executes the public-flag subset of 148 expected passes and 82 expected failures, including 62 witness cases. The harness separately accounts for 9 `BADTX` structure cases, 14 transaction policy cases, and 977 script policy cases instead of conflating them with the public consensus API. Constructed Taproot cases additionally exercise Core 26's spent-output ABI, commitment proof, and tapscript result. Explicit opt-in tests start Core 26 regtest and submit identical constructed blocks to `submitblock` and rBTC's production `HeaderDag`/`connect_active_block` path. They cover consecutive valid blocks, twelve structural/contextual rejection classes, configured BIP34/BIP66/BIP65/SegWit boundaries, and a 102-block CSV relative-lock boundary, while verifying a rejected candidate cannot advance or leave durable chainstate. The block transition also bounds cumulative transaction fees with Core's `MoneyRange` rule and rolls back every applied transaction when the bound fails. Broader historical mainnet activation-boundary expansion remains an acceptance gate.

Candidate deployment context also carries the network-derived proof-of-work subsidy. Bitcoin, testnet, and signet use the 210,000-block halving interval; Core-compatible regtest uses 150. The block validator receives the already-selected subsidy explicitly so a test-network interval cannot silently fall back to mainnet rules.

Minimum chainwork is kept outside consensus validity. A lower-work chain can still have every header and block validated and persisted, but the daemon remains in IBD and a bounded `--once` run returns failure rather than claiming synchronization. Defaults match the pinned Core 26 mainnet, legacy testnet, and signet constants; regtest and testnet4 use zero because Core 26 has no testnet4 trust anchor. Assume-valid configuration is parsed and its hash must appear on the active header chain before it is reported. It does not currently disable script checks: doing that safely requires retaining the skipped validation inputs and completing background verification before pruning or declaring independent validity.

For torrent distribution, the `ArchiveManifest` has stable 4 MiB compressed-piece SHA-256s. A future transport adapter can map them directly to torrent v2 pieces or verify webseed/range downloads before decompression. It must validate each recovered block through the normal chain validator; archive checksums are not consensus proof.

## Explorer and wallet

The explorer is embedded but logically read-only: a redb projection atomically stores active block summaries, transaction confirmations, script-hash keyed current UTXOs, and per-block rollback data. Its durable tip is reconciled against the execution tip on startup; missing projections are replayed from the retained ledger or fetched from a full-history peer, while stale projections use their independent undo records. The Axum router and CSP-constrained static page read this index without exposing chainstate mutation; the daemon accepts only explicit loopback listener addresses. Address UTXO queries validate input and apply capped offset/limit pagination in the redb range iterator, so a request cannot materialize an unbounded result. The wallet is BDK descriptor-based and runs in-process, which avoids putting keys in a browser or inventing PSBT/signing behavior. Its watch-only state uses BDK's transactional SQLite persister under one process mutex: address revelation is committed before response, and reopen checks both descriptors and the network. The database is owner-only on Unix and symlink paths are rejected. Secret descriptors are rejected rather than stored unencrypted. Wallet endpoints remain disabled until encrypted secret storage, authentication, audit logs, rate limits, and signing/broadcast test coverage are complete.
