# rBTC

High-performance Rust Bitcoin node kernel, designed around a compact and verifiable UTXO set.

## What is implemented now

- Protocol-compatible Bitcoin P2P v1 message framing through `rust-bitcoin`; no custom wire format. Core 26's 4,000,000-byte message, 256-byte user-agent, 101-hash locator, and 1,000-address response bounds are enforced before unbounded work. Every control or keepalive frame consumes the bounded response budget, pings receive pongs, and a post-handshake `version` is rejected immediately. Modern peers receive Core-ordered BIP339 `wtxidrelay` and BIP155 `sendaddrv2` negotiation before `verack`; bounded `getaddr` decoding supports legacy and IPv4/IPv6 addrv2 responses. One process nonce spans every fallback connection for self-connection detection. Fresh full-history+witness IPv4/IPv6 addresses are quality-filtered into a network-bound, bounded `peers.redb` fallback pool. A persistent random secret assigns learned addresses across 1,024 keyed new buckets and successful handshakes across 256 keyed tried buckets, each capped at 64 entries; old stores generate the secret atomically on first reopen. Pool updates physically prune stale entries, prefer peers that completed prior synchronization sessions over handshake-only records, use known lower successful-handshake latency and then higher completed block-response throughput as tiebreakers within equal reputation, and round-robin both keyed buckets and target `/16` IPv4 or `/32` IPv6 groups so one range cannot monopolize a startup set. Objective wire violations and invalid headers/blocks from learned peers enter a separately bounded, persistent one-hour-to-one-day cooldown; ordinary transport failures do not, a completed synchronization session clears it, and manual connections remain exempt. Public bootstrap uses the network-specific [Bitcoin Core 26 seed list](https://github.com/bitcoin/bitcoin/blob/v26.0/src/kernel/chainparams.cpp), resolves seeds concurrently under per-seed/global bounds, distributes candidates across seed responses, and never admits private, reserved, or actively discouraged public-network results.
- A successful peer that hashes to an occupied tried `(bucket, slot)` is retained in new instead of evicting the incumbent immediately. Up to ten challenger/incumbent pairs persist atomically; the next startup probes incumbents ahead of ordinary persisted candidates, retaining a live incumbent or promoting the strongest challenger only after a failed handshake. Legacy stores infer their existing successful records as tried without rewriting them.
- BIP152-capable peers receive a witness-aware version-2 `sendcmpct` preference with high-bandwidth announcements disabled. Compact-block transaction-reference vectors are capped at the consensus-derived 16,666-transaction maximum before routing. Negotiated block downloads reconstruct differential prefilled positions, match unique wtxid short IDs against caller-provided candidates, request only missing indexes with `getblocktxn`, and fall back to a full witness block after a Merkle/witness mismatch.
- Script validation adapter using Bitcoin Core's `libbitcoinconsensus`, including Taproot spent-output and default/custom-Signet BIP325 block-solution validation. The pinned Core 26 library has a transaction-level batch ABI so one transaction is decoded and its shared signature-hash data is precomputed once for all inputs.
- Block script checks use a persistent, bounded host-CPU worker pool after
  ordered prevout and UTXO resolution; small jobs remain serial, and no block
  state is committed until every script succeeds.
- Pure-Rust redb chainstate with hot/cold UTXOs, per-block undo, and execution tip committed together in one physical database transaction; IBD supports multi-block durable checkpoints.
- Deterministic zstd UTXO snapshots with bounded-memory two-pass import, in-transaction SHA-256/count verification, mandatory trusted active-header anchors, atomic publication, and an AssumeUTXO-style background-validation contract.
- Immutable zstd block archives with 4 MiB piece hashes, authenticated uncompressed-length limits, and legacy-v1 read compatibility, ready for a BitTorrent/webseed transport adapter.
- Configurable circular pruned ledger: defaults are 1,008 blocks (about one week) and 1 GiB. Validated IBD batches are published through a restart-safe staging protocol; archive-slot renames are directory-synced before their indexes are published, and only old block archives rotate while UTXO state and headers are retained.
- Optional embedded block-explorer UI and REST API, an authenticated bounded read-only JSON-RPC route, plus an optional authenticated, transactionally persisted BDK watch-only descriptor wallet panel/API. The historical explorer projection is maintained only when a loopback API listener is explicitly configured, so ordinary validation does not pay for an unused full transaction index.

## Important safety status

rBTC is **not yet a production full node** and must not be trusted with mainnet funds. Durable regtest and Signet headers-first/block IBD, cumulative-work fork choice, persistent explorer projections, and crash-safe watch-only wallet address/validated-chain tracking are implemented, but complete mainnet deployment activation and block rules, the P2P peer manager, encrypted wallet signing, remote API serving, and release hardening remain completion gates. The exact plan is in [docs/ROADMAP.md](docs/ROADMAP.md).

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
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo test --locked --all-features
cargo llvm-cov --locked --all-features --fail-under-lines 90
cargo audit --deny warnings
cargo deny check
scripts/verify-reproducible-build.sh
scripts/public-network-sync-smoke.sh
RBTC_FUZZ_RUNS=10000 scripts/run-fuzz-regression.sh
cargo +nightly miri test --lib merkle_proof::tests::verifies_left_and_right_transaction_positions
RBTC_BITCOIND=/path/to/bitcoin-core-26/bin/bitcoind cargo test --test core_block_differential -- --ignored --nocapture
cargo test --release --all-features --test storage_bench -- --ignored --nocapture
```

The storage benchmark generates its block-shaped UTXO population at runtime and
reports machine-readable JSON; no generated database, snapshot, or result is
versioned. Set `RBTC_BENCH_BLOCKS`, `RBTC_BENCH_UPDATES_PER_BLOCK`,
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

The repository keeps only reviewed, human-named fuzz seeds and minimized
crash/hang regressions. Coverage discoveries with cargo-fuzz's 40-character
hash names remain local and are ignored rather than accumulated in commits.

The optional live differential gate requires the matching `bitcoin-cli` beside a Bitcoin Core 26.0 `bitcoind`. It submits the same mined regtest blocks to Core and through rBTC's production header-DAG/block-connection path, including atomic rejection checks for the persisted tip, undo record, and candidate UTXO.

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
process; the current atomic batch may finish before the signal arrives, then a
new process must reopen that exact durable state and stop at the target.
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
one ordered receive window. The pinned redb write buffer sorts dirty pages by
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
For bounded standalone validation, the daemon also requests the next batch's
first window only after the current batch has passed structure validation.
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
immediate-durability, append-only checkpoint deltas. Each `RVD3` record has a
strict fixed-width sorted outpoint index followed by canonical UTXO bytes, so a
lookup decodes only its matching coin. Checksummed `RVB1` per-record Bloom
filters and completed 16-record aggregate filters reject old runs before redb
value access. New filters enter the same redb transaction as the complete
delta and execution tip. Existing RVD3 directories perform one strict
full-record scan and atomically install the missing filters; later restarts
validate their sizes, checksums, UTXO counts, delta headers, and exact
execution-tip alignment without rebuilding the complete historical index.
Ordinary reorganizing stores reject this format. Explicit materialization
folds all runs and clears the delta and filter tables atomically. There is no
relaxed durability, automatic periodic materialization, or block undo in this
fixed-target mode.

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

For experimental mainnet checkpoints wider than 128 blocks, up to three ready
standby candidates now survive the chainstate-open phase; the first one that
still passes bounded activation becomes an auxiliary block source. A
252-block batch requests 128 blocks from the active peer and 124 from the
auxiliary peer concurrently, preserves active-chain order, and retries the
auxiliary window on the primary after any request or response failure. An
adjacent live sample reduced median download time only from about 19.4 to
18.8 seconds, so this is a modest network improvement rather than the main
speedup. Actively receiving the auxiliary lookahead during execution was
rejected after a 124-block response exceeded the 30-second bound; the retained
design keeps the simpler bounded request lookahead and failover semantics.

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

For the current safety-gated validating daemon, `rbtcd --network signet --data-dir PATH` can bootstrap from Core 26's default Signet seeds, while regtest normally supplies `--connect HOST:PORT` or reuses a previously verified peer in `peers.redb`. Repeat `--connect HOST:PORT` to provide up to 16 ordered, deduplicated peers. Repeat `--dns-seed HOST[:PORT]` to replace the pinned defaults, or use `--no-dns-seeds`; explicit peers and fresh persisted candidates are tried before DNS is queried. Each stage starts its bounded candidate handshakes concurrently but still selects the active session in the configured/persisted order; later completed handshakes stay in memory as hot failover sessions while the earlier session runs. Each full-service standby clones the current validated header DAG, then independently requires a nonce-matched ping/pong and performs one bounded `getheaders` validation step every 30 seconds. Its PoW, difficulty, timestamp, checkpoint, and deployment validation is isolated from shared persistence; invalid announcements evict that standby, while activation carries its validated height and the active synchronizer resumes from durable state. Other application messages remain bounded and ordered for activation. After the active socket completely writes a wallet transaction, the same transaction is fanned out through a bounded in-memory ring to every hot standby; a stalled or lagging standby is removed without blocking active synchronization. A failed handshake, missing full-history/witness service, interrupted headers or block transfer, or rejected response advances to the next candidate; durable headers and atomic chainstate let that peer resume the same IBD. Learned connection attempts are committed before each task starts network I/O and receive a persistent one-minute-to-six-hour exponential retry delay. Malformed framing/ordering, bounded-response violations, objectively invalid headers, and invalid downloaded blocks additionally discourage non-manual peers for one hour, doubling to at most one day; the count decays after seven quiet days. Transient I/O, timeouts, future-time headers, missing blocks, obsolete versions, and missing services never receive that penalty. Every successful full-history/Witness handshake is persisted as verified, migrates into the keyed tried bucket set, and clears the ordinary connection delay, so a later launch can omit `--connect` and DNS; promotion frees the learned source's new-bucket quota. The stronger protocol record is cleared only after the requested synchronization session completes successfully, preventing an invalid-block peer from resetting escalation with a clean handshake. Completion time and a saturating completion count are persisted separately from handshake success, survive database reopen, and rank fully proven peers ahead of handshake-only candidates. Within otherwise equal reputation, the latest successful outbound handshake measurement is capped to 1–60,000 ms and lower known latency ranks first. A successful requested synchronization session with downloaded blocks additionally persists exact completed block-payload bytes divided by response-wait time, capped at 1 GB/s; higher known throughput then ranks ahead of lower or legacy-unknown throughput before the existing freshness and target-network-group diversity rules. Successful full-service handshakes request addresses with a three-second bound; newly learned candidates become eligible on the next restart. Sync completion remains based on validated cumulative work, never the peer's untrusted advertised height. The keyed new table now retains up to eight source-group references with Core-style exponentially decreasing admission probability, while tried collisions use exact hashed slots. This remains a bounded outbound peer manager rather than Core's complete addrman or adaptive connected-peer eviction design. Core-compatible `--signetchallenge HEX` selects a custom BIP325 challenge, derives its P2P magic, disables default-Signet trust anchors/seeds, and accepts repeatable `--signetseednode HOST[:PORT]`; use an isolated data directory, whose challenge identity is checked before wallet opening or network I/O. Add `--once` for a bounded sync-and-exit run. Add `--explorer-listen 127.0.0.1:3000` to serve the embedded read-only explorer and REST API; non-loopback binds are rejected until authentication is implemented. Regtest Taproot activation can be overridden with Core-compatible `--vbparams taproot:START:END[:MIN_HEIGHT]`. Buried deployments accept repeatable Core-compatible `--testactivationheight NAME@HEIGHT`, where `NAME` is `segwit`, `bip34`, `dersig`, `cltv`, or `csv`; the last value for a name wins. The complete selected consensus configuration is bound to a fresh execution database and cannot later change in place. An offline regtest/Signet base can be installed only into a fresh data directory with `--assumeutxo-snapshot FILE --snapshot-height HEIGHT --snapshot-blockhash HASH --snapshot-utxo-count COUNT --snapshot-records-bytes BYTES --snapshot-records-sha256 HEX`; every identity value must come from an authenticated channel, the header must already be active in `headers.redb`, and the durable assumed marker remains until the still-pending background genesis validator exists. Core 26 minimum-chainwork and assume-valid defaults are loaded per supported legacy network and can be overridden with `--minimum-chainwork HEX` and `--assumevalid HASH|0`. A chain below the work floor remains in IBD. Assume-valid currently identifies a reviewed active-chain anchor only: all scripts are still verified. Mainnet, legacy testnet, and testnet4 may probe or persist headers; ordinary `--data-dir` block execution remains rejected before connecting until the remaining acceptance gates close. For bounded validation and soak only, Bitcoin or legacy testnet can explicitly opt into the same production execution path with `--experimental-network-execution --once` plus a mandatory authenticated `--validate-until-height`/`--validate-until-blockhash` pair; the execution routine repeats that hard-ceiling requirement. This mode prints a funds-safety warning, cannot start an indefinite node or expose explorer/RPC/wallet services, and does not support testnet4 or automatic AssumeUTXO cleanup. Because Core 26 predates testnet4, rBTC does not silently mix a newer testnet4 seed list into this pinned compatibility baseline; supply an explicit peer or custom seed for testnet4 header operations.

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

The normal live-service path is `rbtcd --data-dir ACTIVE --network NETWORK [PEER OPTIONS] --background-assumeutxo VALIDATION`. It derives the authenticated height/hash from the active marker, starts the assumed active chain and independent genesis validator concurrently with separate connections, peer failover, headers, chainstate, ledger, explorer, and peer databases, and finalizes from the active execution loop without taking the explorer or wallet API offline. If either side exhausts its peers or validation/finalization fails, the combined service fails closed and retains both resumable directories. `--once` waits for both sides and finalizes before returning, which is useful for bounded deployment gates. The older sequential `--complete-assumeutxo` path remains available. Both modes reject same-directory, parent/child-directory, symlink, and Unix hardlink aliases before validation state is opened.

The ordinary “successful handshake promotes to tried” rule is conditional on its exact keyed tried slot being vacant. An occupied slot leaves the successful challenger selectable from new and enters the bounded collision queue. Incumbent probes use the same pre-I/O attempt accounting and full-service handshake checks as normal connections; success cancels its collisions, while connection/handshake failure atomically demotes it and promotes the best queued challenger. Learned records persist up to eight independently keyed source-group references, admitted with exponentially decreasing probability; full Core addrman probabilistic selection remains open.

Persisted selection now applies Core-style terrible-entry hygiene before collision probes or ordinary candidates. A peer attempted within the last minute is protected; otherwise a timestamp more than ten minutes in the future, a zero or over-30-day-old address time, three consecutive failures without any success, or ten failures when the last success is over seven days old makes the entry ineligible. Startup atomically removes those rows and sanitizes collision references, while successful handshakes continue to reset the failure count. Full Core addrman probabilistic selection remains open.

The first validation target is durably bound to the validation database and cannot later be changed or moved behind its executed tip; even a restart that omits the validation flags automatically inherits that ceiling. Successful completion retains the validation directory as audit evidence by default. Add `--validation-batch-size N` (1 through 1,008, default 64) to cap each atomic validation checkpoint and `--validation-pause-ms MS` (at most 60 seconds) for coarse bandwidth, CPU, and write-I/O throttling. Each checkpoint is downloaded through 16-block protocol requests; eight requests may be pipelined into one ordered 128-block response window, then chainstate and any enabled explorer projection are committed once for the aggregate batch. The pipelined receiver preserves duplicate, unsolicited, `notfound`, compact-reconstruction, fallback, payload, and message-count checks without increasing one `getdata` exposure. Explicit checkpoints above 64 are intended for adequately provisioned validation hosts: the 1,008-block ceiling matches the default retained-ledger window and could hold approximately 4 GiB of consensus-maximum block payload before validation working state, subject to the ledger's independent 1 GiB canonical-record ceiling. In background mode the validator automatically drops to one block per checkpoint and at least a 100 ms pause while active execution trails its header tip, then restores the configured limits when the serving node catches up. Persisted progress and the effective limits are printed after every batch, and `GET /api/v1/validation` reports both tips, the immutable target, remaining blocks, phase, failure, and current throttle state when the explorer listener is enabled. Snapshot origin remains durable after finalization. A fresh explorer atomically streams the current hot/cold UTXO set into a cursor-paged baseline, so current address UTXOs and all post-snapshot blocks are indexed without pretending unavailable pre-snapshot transaction/block history exists.

The batch log reports download, structure, staging, execution, indexing,
publication, and total time so later tuning is based on measured phases.

`--cleanup-validation-dir` is an explicit destructive opt-in for automatic/background completion. rBTC will only claim a validation directory that was absent or empty before it created the validation stores, records a strict, size-bounded owner-only marker bound to the network and snapshot target, and revalidates that marker plus the completed non-assumed chainstate before cleanup. The marker file and its parent directory are synced on Unix before use. Unknown top-level artifacts, symbolic links, special files, a changed target, or an unowned legacy directory fail closed and remain in place. An accepted directory is first atomically renamed to a randomized sibling quarantine; the parent is synced after both the rename and recursive removal. A failed first parent sync rolls the quarantine rename back before any recursive deletion. Do not enable this flag when the validation database is required as audit or recovery evidence.

The lower-level two-step interface remains available: build with `rbtcd --data-dir VALIDATION --network NETWORK [PEER OPTIONS] --validate-until-height HEIGHT --validate-until-blockhash HASH`, then run `rbtcd --data-dir ACTIVE --network NETWORK --finalize-assumeutxo VALIDATION`. Both explicit target values must be taken from the authenticated snapshot identity. Headers may synchronize beyond the target, but block requests, atomic execution batches, the retained ledger, and any explicitly enabled explorer projection stop exactly at it; restart resumes safely and a different active hash or an already-overrun chainstate fails closed. The same resource-limit options apply. Finalization requires the same consensus configuration and never replaces active UTXOs. The logical digest deliberately excludes local `last_touched` tier-aging time while retaining every consensus field; the separate snapshot-record digest still authenticates the complete transported bytes. The lower-level manual finalization path deliberately never performs automatic cleanup.

Post-handshake routing centrally caps `inv`/`getdata`/`notfound` at 50,000 entries, locators at 101 hashes, headers at 2,000, and address messages at 1,000. These limits also apply to unrelated frames injected while another response is pending. Peers supporting BIP130 receive `sendheaders` immediately after handshake so announcements remain headers-first. During ordinary 30-second caught-up polling, the daemon requires a nonce-matched pong within the same 32-frame total response budget before requesting more headers; crossed peer pings are answered without extending it, and a header announcement arriving before the pong is retained for the following sync pass. Retained application frames additionally share a 4,000,000-byte aggregate payload ceiling, preventing the frame-count limit from multiplying into a roughly 124 MB queue.

BIP152 negotiation follows `sendheaders` for protocol 70014+ peers and advertises version 2 decoding without opting into unsolicited high-bandwidth announcements. `cmpctblock`, `getblocktxn`, and `blocktxn` transaction/reference counts above 16,666 are objective protocol violations. After the peer reciprocates version 2, all daemon block-download paths request compact inventory, accept a direct full-block fallback, reconstruct unique local-candidate matches, and request the remaining transaction indexes. Short-ID ambiguity is never guessed; a reconstructed Merkle or witness-commitment mismatch triggers one bounded full witness-block retry. Up to 64 admitted peer transactions plus the 64 most recent unique wallet transactions that completed active-peer delivery are supplied to every validating, ledger, explorer, and wallet-backfill download as witness-ID candidates. Transactions absent from those bounded local sets are requested from the peer.

The P2P session can write one non-coinbase transaction only when it fits Core's 400,000-weight-unit standard relay ceiling. Before the authenticated wallet route publishes a consensus-verified transaction into its eight-entry active-peer channel, a distinct wallet-origin policy checks versions 1–2, minimum non-witness size, standard output templates, push-only and bounded scriptSigs, one bounded data-carrier output, dust, and a 1 sat/vB relay floor. It then reserves channel capacity and commits the exact transaction to the network-bound, owner-only `rebroadcast.redb`; policy or retained-input conflicts return 400, while a full channel or persistence failure returns 503 without creating a new row. The durable queue retains at most 64 unique wtxids for 14 days, rejects conflicting spends across restart, retries never-sent entries after restart and delivered entries every 12 hours, and suppresses confirmed or noncanonical transactions. Wallet-chain reconciliation clears suppression when a reorganization restores the transaction or its inputs. The route succeeds only after the complete active `tx` frame and durable attempt metadata are written. A failed socket write remains eligible on the next peer, and a successful write also fans out through the eight-entry in-memory standby ring. This proves socket delivery, not peer mempool acceptance or acknowledgement.

Failed hot standbys are reaped and classified while the active session is still running, so objective violations enter persistent discouragement promptly without waiting for failover. At most eight ready automatic hot standbys are retained: the existing manual/persistent-reputation queue order protects stronger peers and evicts from its tail as slower handshakes complete. Manual peers do not consume that soft capacity, every stage remains under the 16-connection hard ceiling, and a local capacity eviction is not recorded as a remote failure. Manual peers and transient failures retain their existing discouragement exemptions.

Unsolicited `tx` frames that arrive while a peer is serving a bounded headers, address, or block response are no longer discarded. Each session first retains their wire order in an independent 64-transaction/4 MB FIFO. Once execution has caught up to an active header chain above minimum chainwork, the daemon drains that queue through a read-only admission pass: confirmed inputs, maturity, finality, BIP68/BIP113 locks, deployment-aware Bitcoin Core script execution, output accounting, standard output templates including push-only data carriers and Core's default x-of-3 bare-multisig creation ceiling, recognized spent-prevout templates including historical x-of-16 bare multisig, P2SH's 15-accurate-sigop ceiling, P2WSH's 3,600-byte/100-item/80-byte-item bounds, native-Taproot annex prohibition, the 80-byte tapscript argument bound, P2SH-wrapped upgradable witness-program rejection, upgradable Taproot leaf-version rejection, tapscript `OP_SUCCESS` discouragement, dust, minimum fee, and retained-input conflicts must all pass without mutating chainstate. Dependency-connected wire batches are split from unrelated transactions, capped at 25 transactions/101,000 virtual bytes, topologically ordered regardless of arrival order, and applied atomically to a private UTXO overlay, so an invalid child cannot leave its parent admitted. Outputs of accepted parents support later children. Accepted transactions enter a 64-transaction/4 MB oldest-first pool shared across active-peer failover and become compact-block reconstruction candidates; capacity eviction removes an oldest transaction together with every descendant. Conflicts may atomically replace explicitly or inherited BIP125-signaling transactions and their descendants, up to the 100-transaction BIP125 ceiling, only when they add no unrelated unconfirmed input, pay more than the entire removed set, and pay at least 1 sat/vB of additional fee for the replacement package. The complete parent-before-child snapshot is committed to the network-bound, owner-only `mempool.redb` before the updated in-memory view is published. Reopen strictly bounds and validates its binary snapshot, rejects network mismatch, and reruns every transaction through the current chainstate and deployment-aware policy before retaining it; stale entries and descendants are atomically removed. Before a stale active block is disconnected, its ledger hash is checked and its non-coinbase transactions enter a separate durable recovery snapshot under the same 64-transaction/4 MB bound. Lower-height parents win capacity pressure; after the replacement chain catches up, recovered transactions re-enter the ordinary admission pass and the recovery snapshot is cleared atomically with the new pool. Opt-in replacement remains the default. `--mempool-full-rbf` explicitly permits replacement of non-signaling conflicts, while preserving the same descendant closure, 100-transaction eviction ceiling, unrelated-unconfirmed-input prohibition, higher aggregate fee, and 1 sat/vB incremental relay fee rules.

Each newly admitted, durably committed transaction is announced to every hot standby except the active source peer through the same eight-slot ring used after wallet delivery. BIP339 peers receive wtxid `inv`; legacy peers receive txid `inv` and may request witness data. For protocol version 70013 and later, a valid BIP133 `feefilter` becomes that session's latest sat/kvB threshold. A pool-origin relay carries its exact fee and sigop-adjusted policy vsize, so an announcement below the peer's threshold is suppressed while the bounded ping exchange still completes; equality is announced. Negative or above-`MoneyRange` filters are ignored, and legacy wallet rebroadcast rows without retained fee metadata bypass this optional optimization rather than risking false suppression. Direct active-peer wallet `tx` delivery is unaffected because BIP133 filters transaction inventory, not explicitly submitted payloads. A nonce-matched bounded ping drives the optional `getdata` exchange immediately, so a peer that already has the transaction completes normally. Matching requests receive `tx`, unknown requests receive `notfound`, duplicate announcements are suppressed inside a 64-transaction/4 MB relay cache, crossed application messages retain wire order, and the global inventory/frame/payload limits still apply. Protocol-60002-or-later peers on full-validation active and hot-standby connections may also send BIP35 `mempool`: the session samples the process-shared validated pool at request time, applies its latest BIP133 filter, selects txid or wtxid inventory according to BIP339, and announces at most the pool's 64 transactions/4 MB while caching exactly those payloads for bounded `getdata` service. Empty, pre-BIP35, and header-only sessions produce no inventory. In the other direction, active and hot-standby transaction inventory crossed during ordinary response processing enters a process-shared request tracker capped at 64 announcements per session and 1,024 overall. Once caught up above minimum chainwork, known pool, orphan, wallet, recent-confirmed, and exact recent-reject entries are forgotten; a source may place at most one request for a transaction hash in flight, in its original announcement order, for Core 26's 60-second retry interval. Exact `tx`/`notfound` outcomes complete that source independently, so `notfound`, timeout, cancellation, or disconnect unlocks another announcing standby without a duplicate simultaneous request. Matching payloads enter the same persisted admission path as unsolicited `tx`, and admission or confirmation forgets both txid and wtxid candidates. Each download still has a 64-reference, 32-extra-frame, and 4 MB aggregate transaction-payload ceiling. Only transactions surviving the complete admission pass are announced; lagging, timed-out, or failed peers follow the existing failover path. This is bounded relay and request service over already-established outbound connections, not an inbound listener, a complete Core mempool protocol, peer acknowledgement tracking, or a propagation guarantee.

The active mempool snapshot also stores a versioned last-relay-attempt timestamp for each retained transaction. Newly admitted transactions and legacy snapshots without this metadata are immediately due; after that, a caught-up loop selects at most eight due transactions every 12 hours in parent-before-child order. An attempt is recorded only when the transaction was published into a standby ring with at least one receiver, so the absence of a hot standby cannot suppress a later attempt. Snapshot replacement atomically removes attempt rows for evicted, replaced, confirmed, or otherwise stale transactions. This schedule bounds repeated diffusion attempts but still does not prove peer receipt, acknowledgement, or mempool acceptance.

In addition to the 25-transaction/101,000-vB incoming package bound, every newly admitted transaction is limited to 25 ancestors and 101,000 aggregate ancestor vB, including itself. Every affected ancestor is independently limited to 25 descendants and 101,000 aggregate descendant vB. Core's CPFP carve-out applies only to a single submitted transaction no larger than 10,000 policy vB with exactly one unconfirmed ancestor: that parent alone may reach 26 descendants and 111,000 aggregate descendant vB. Multi-transaction packages, larger children, and deeper chains receive no relaxation. After prevouts are available, no transaction may exceed Core's 16,000 standard sigop-cost ceiling, and package, ancestor, descendant, replacement, relay-fee, and eviction-rate calculations use Core's sigop-adjusted virtual size: the larger of transaction weight and sigop cost times 20 bytes, rounded by the witness scale factor. Thus a compact transaction cannot bypass fee or graph limits with an expensive unexecuted script branch. These graph checks run on the private candidate pool after conflict removal and package insertion but before publication, fee completion, or capacity eviction, so an over-limit package leaves the live pool and durable snapshot unchanged.

The same active snapshot stores a complete versioned map from txid to its first admission time. Transactions expire once older than Core's default 336-hour lifetime; an expired parent is removed with every retained descendant before caught-up revalidation. Ordinary snapshot replacement preserves surviving times and prunes removed rows. If an expired transaction is independently received or recovered from a reorganization and passes admission again, its time is reset in the same commit that republishes the pool. Legacy snapshots without this map migrate every active entry as newly admitted, while malformed, duplicate, missing, or non-pool rows fail closed and join the persisted-metadata fuzz surface.

Peer transactions that fail admission only because an input is unavailable enter a separate process-shared orphan pool instead of being discarded. Persisted mempool and reorg-recovery candidates are not mislabeled as peer orphans. The pool retains at most 64 transactions and 4 MB, rejects any orphan above the 400,000-weight-unit standard ceiling, deduplicates txid/wtxid variants, randomly evicts entries under pressure, and expires entries after Core's 20-minute lifetime. Missing parents absent from the submitted package, active pool, orphanage, recent-confirmed set, and confirmed UTXO view enter a globally deduplicated 64-txid request set keyed by source. Parent requests deliberately use legacy txid inventory even after BIP339 negotiation, and successful responses feed the ordinary pending-transaction admission queue. Every connected block contributes both txid and distinct wtxid identifiers to an exact oldest-first set capped at Core 26's 48,000-entry rolling-filter capacity. That set suppresses redundant announced-transaction downloads and recognizes confirmed parents even after their outputs are spent; any active-chain disconnection clears it before reorg transactions are reconsidered. When a parent is admitted, only orphans spending one of its actual output indexes enter a work set keyed by the supplying session. The caught-up scheduler pops one transaction from that source at a time, commits its ordinary atomic admission result, yields, and immediately repeats while work remains; accepted children schedule the next generation without validating the entire orphanage in one turn. A separate exact-outpoint index removes orphans included or made impossible by each successfully connected block without deleting transactions that spend another output of the same parent. A still-missing attempted orphan remains eligible for a later parent, while any terminal consensus, policy, conflict, package, or topology failure removes it. A bounded 1,024-entry exact-txid cache remembers only witness-independent terminal failures and clears whenever the active chain tip changes. If an orphan depends on a cached rejected parent, the child is cached and discarded instead of being retained or triggering another blind fetch. Every removal path rebuilds byte accounting and prunes the exact index, live work sets, and source request state. Source identity is a locally assigned monotonic session ID rather than the remote-controlled version nonce, and every remaining entry, work item, and parent request from that source is removed before normal completion or failover activates another peer. The orphan pool, recent-confirmed/reject caches, requests, and scheduler are intentionally memory-only and do not survive process restart.

Capacity eviction now raises a process-local rolling mempool minimum to the aggregate fee rate of the evicted oldest transaction and its descendants plus the 1 sat/vB incremental relay rate. Every transaction must still independently pay Core 26's 1 sat/vB min-relay floor. An exact child-with-parents package—at least one new parent followed by one child that directly spends every submitted parent—may satisfy the higher rolling minimum with its aggregate fee and policy vsize; a parent below min-relay, a deeper package, a partial package whose only new entry is the child, or a replacement package receives no aggregation. Like Core 26, the bump cannot decay until a later caught-up chain tip is observed; it then has a 12-hour half-life, accelerated twofold below half capacity and fourfold below quarter capacity, and clears below 0.5 sat/vB. Existing entries are not retroactively repriced during active-chain reconciliation. The rolling value is deliberately absent from the durable mempool snapshot and resets on process restart, matching Core's mempool dump boundary.

`fee_estimates.redb` provides a separate network-bound, owner-only empirical fee history. Every active admitted transaction contributes its exact fee, sigop-adjusted policy vsize, and first eligible block height; existing observations preserve that height across restart. Once execution catches up, retained active-chain blocks advance a 1,008-block journal and move matching transactions into confirmed samples. A shallow reorganization reverses those moves exactly, while a deeper reorganization or a gap outside the retained block ring explicitly clears the history and reanchors it instead of mixing chains. History retains at most 4,096 confirmed observations, and the persisted decoder is strictly bounded and fuzzed. Estimates require at least three mature outcomes and select the lowest whole-sat/vB threshold with at least 85% target success; confirmations slower than the target and old-enough pending transactions both count as failures, avoiding confirmation-only optimism. The authenticated `estimatesmartfee [1..1008]` RPC reports Core-compatible BTC/kvB units and an explicit insufficient-data error rather than inventing a default. This is a bounded local empirical estimator, not Bitcoin Core's complete bucket/decay estimator. PSBT creation can select it only through an explicit `confirmation_target`; an exact `fee_rate_sat_vb` remains available and the two modes are mutually exclusive.

For custom activation schedules, transaction admission keeps block consensus and relay policy separate. It first validates with the active next-block flags, then—only when that set is incomplete—rechecks scripts with every Core 26 standard flag exposed by `libbitcoinconsensus`: P2SH, strict DER signatures, NULLDUMMY, CLTV, CSV, Witness, and Taproot. A Core fixture that is valid with no flags but violates DERSIG/NULLDUMMY is rejected through the distinct standard-script policy path without changing the UTXO set or pool. Fully activated production contexts avoid the redundant second interpreter pass. Core's standard lock-time policy is independent as well: version-2 transactions must satisfy BIP68 for the next block even when a custom chain has not activated CSV. Exact height and 512-second boundaries are tested, while the active block path retains its configured activation semantics.

Core 26 package fee aggregation now uses the same fee-bumping subpackage boundary rather than the entire submitted set. Only parents below the rolling mempool floor remain in the fee calculation with the child; parents that already pay their own way are excluded, so a rich parent cannot subsidize a low-fee child. Aggregation is limited to a tree of mutually independent direct parents plus one child. Every transaction still independently pays min-relay, and all package failures retain rBTC's atomic publication boundary.

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

`/api/v1/health` is a process-liveness check. `/api/v1/ready` returns 503 during IBD, block catch-up, or explorer/wallet reconciliation, and 200 only when header, execution, explorer, and optional wallet tips agree and the configured minimum chainwork is reached. `/api/v1/status` returns the same decision with hashes, phase, AssumeUTXO independence, hot/cold UTXO counts, and compressed ledger footprint. `/metrics` exposes those bounded counters and gauges without reading archive payloads. Dynamic status and metric responses use `Cache-Control: no-store`; all routes remain loopback-only. Add `--rpc-auth-token-file PATH` beside `--explorer-listen` to mount the independently authenticated `/rpc` route. Its bounded read-only methods are `help`, Core-compatible `getblockhash` and `estimatesmartfee`, `rbtc.getblocksummary`, `rbtc.gettransaction`, and paged `rbtc.getaddressutxos`. The RPC token file follows the same owner-only, 32–256 printable-byte, atomic-rotation, and fail-closed rules as the wallet token, but the two files must not alias so access scopes stay separate. Requests are strict JSON-RPC 2.0, reject batches, notifications, unknown envelope fields, invalid IDs, oversized methods/parameters, and bodies above 64 KiB, and never expose internal storage errors.

The wallet router accepts public descriptors only. BDK changesets are committed transactionally to an owner-only SQLite file before a derived address is returned; separate monotonically increasing receive and change cursors are reserved first so a crash can skip an address but cannot return or use it twice. Startup rejects a network or descriptor mismatch. Descriptor import supports a bounded `gap_limit` (default 20, maximum 1,000) on both receive and change keychains and an optional `birthday_height` (default 0). It repeatedly replays only fully validated blocks until the unused-script window converges, records the earliest completed scan boundary only after success, and uses sparse validated checkpoints to avoid fetching raw blocks before the birthday. Lowering a birthday or extending the discovered window triggers a durable rescan from the retained ledger or a full-history peer. On reorg, the wallet rewinds to the execution chain's common ancestor before replay.

Authenticated status, balance, canonical transaction history, current UTXO, address, canonical public-descriptor export, unsigned PSBT creation, external-signature finalization, and peer broadcast routes expose the projection with bounded pagination; fees are returned only when BDK knows every input amount. The two-field descriptor export contains checksummed receive/change public descriptors and round-trips through the strict import parser with default scan policy; network, gap, and birthday remain explicit deployment choices rather than hidden export state. Creation requests are strict JSON no larger than 32 KiB, allow 1–16 network-checked non-dust recipients within `MoneyRange`, require exactly one of `fee_rate_sat_vb` from 1–1,000 or `confirmation_target` from 1–1,008, and consider at most 100 inputs. Target mode must resolve from at least three mature local observations to the same 1–1,000 sat/vB wallet bound; unavailable, insufficient, or out-of-range estimates return 503 without a fallback. An optional `selected_utxos` list is exclusive coin control; an empty list uses BDK automatic selection. Creation requires SegWit or Taproot receive/change descriptors and includes only bounded `witness_utxo` input metadata rather than cloning full previous transactions. BDK enables RBF, computes the fee, creates change from the durably reserved cursor, and returns only an unsigned base64 BIP174 object plus its unsigned txid/counts; PSBTs over 512 KiB or containing any signature/finalization field fail closed.

The finalization request is strict JSON capped at 768 KiB and accepts at most a 512 KiB externally signed PSBT with 1–100 inputs and at most 17 outputs. Every input must still be a current wallet UTXO, submitted `witness_utxo` data must exactly match local validated state, full previous transactions and already-finalized inputs are rejected, and signatures must use `SIGHASH_ALL` or Taproot default. BDK assembles the final scripts, after which the pinned Bitcoin Core 26 consensus engine verifies every input against local prevouts. The returned finalized PSBT, raw transaction, txid, wtxid, fee, and virtual size are not broadcast or persisted as a mempool transaction.

The daemon mounts these watch-only routes only when both `--wallet-descriptors PATH` and `--wallet-auth-token-file PATH` accompany a loopback `--explorer-listen`. Both input files must be regular, bounded, and owner-only on Unix. The descriptor JSON keys are `receive_descriptor`, `change_descriptor`, optional `gap_limit`, and optional `birthday_height`; the token is 32-256 printable ASCII bytes and is sent as `Authorization: Bearer TOKEN`. Replace the owner-only token file atomically to rotate it without restarting; the daemon reloads it within one second, invalidates the old credential, and disables wallet authorization if the replacement is missing, malformed, oversized, non-UTF-8, or over-permissive. Every wallet or RPC authorization attempt is synced before route code runs to the owner-only, single-link `api-auth-audit.jsonl` in the data directory. Its bounded records contain only time, method, query-free fixed route path, and accepted/rejected status—never a token, header, query value, body, or response. The 16 MiB log fails closed with HTTP 503 when full or unwritable; archive or replace it while the daemon is stopped before restarting. Wallet responses use `Cache-Control: no-store`; address revelation has its own mutation limiter, while PSBT creation, finalization, and broadcast share a burst of 20 requests and refill one request per minute. `POST /api/v1/wallet/psbt/broadcast` accepts the same signed non-final PSBT as finalization, repeats all current-UTXO/fee/script checks, applies the separate wallet-origin relay policy, durably queues it before handoff, and returns 400 for policy/conflict rejection or 503 if persistence, bounded queueing, or peer failover do not produce a complete active-socket write within 35 seconds. After that write, every hot standby receives the transaction independently; a timed-out caller loses only its response, not the already durable rebroadcast record. Tokens and descriptors are never accepted directly on the command line or printed. Private descriptors, in-process signing, encrypted secret storage, a general inbound peer listener, and a complete Core-style transaction-relay lifecycle remain disabled.
