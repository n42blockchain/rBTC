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

The production runtime is a library module rather than a binary-owned
singleton. `src/bin/rbtcd.rs` only adapts command-line arguments and process
signals. A Tokio host launches ordinary persistent execution through
`rbtc::node::NodeBuilder`, hands `NodeHandle::wait` to its task executor, and
retains a cloneable `NodeController` for checkpoint-safe shutdown. This matches
the critical-task assembly used by `n42-26` without moving consensus or storage
truth into the host. Each launch owns its shutdown flag and in-flight checkpoint
barrier; an external-crate acceptance test runs two isolated regtest instances
concurrently in one Tokio runtime and shuts them down independently. The public
`NodeConfig` covers ordered preferred peers, DNS policy, ordinary/background/
bulk chainstate caches, freezer retention, mempool policy, loopback APIs,
consensus overrides, and AssumeUTXO validation. CLI parsing and embedded hosts
share the same bounds and cross-field validation before storage or network I/O.
The controller exposes latest-value `NodeLifecycle` and `NodeRuntimeStatus`
through Tokio watch receivers plus a 32-entry broadcast stream of typed
lifecycle, peer, header, execution, reorg, index, freezer, and failure deltas.
Lagging observers receive an explicit lag error and resample the latest status
instead of growing node memory. The external fixture additionally drives a
real regtest P2P handshake, while
`../n42-26/integration-tests/rbtc-embedding` moves `NodeHandle::wait` into
Reth's actual critical-task executor without compiling the unrelated complete
N42 node stack. This closes
the technical P1.0 boundary without making the host the owner of Bitcoin
consensus or storage state.

The header DAG keeps a height-indexed view of the selected best-work branch, so
active-height lookups stay constant-time while an ordinary extension appends
one entry. A stronger side branch rebuilds that index from its committed
ancestors before becoming active. Taproot's BIP9 state is cached at completed
period-end block hashes inside the deployment configuration; this makes the
cache branch-specific, lets cloned validating/standby DAGs share immutable
results, and reduces sequential deployment evaluation to one new period of
work instead of rescanning from genesis for every candidate. A `--vbparams`
override detaches to an empty parameter-specific cache. Hardened mainnet and
legacy-testnet checkpoints are exact-height commitments; after any such hash is
known, the contextual validator also rejects a newly announced fork below the
highest known checkpoint immediately, matching Core's old-fork resource gate.
Header responses may begin with an already known prefix because a retained
announcement can race the following `getheaders` response. Active and standby
sync trim only that exact prefix; a known header after unseen work remains a
malformed duplicate. This keeps benign repeated tip announcements from
evicting every hot standby without weakening contextual validation.
Block locators use the active-chain height index directly for their roughly
logarithmic set of entries. They do not walk almost the entire parent chain for
each standby poll; at a 959,381-header tip this removes about one million hash
lookups per locator from the shared event-loop thread.
Ordinary persistent execution is enabled for Bitcoin, legacy testnet, Testnet4,
Signet, and regtest. The separate
`--experimental-network-execution --once` validation path remains available
only for reproducible Bitcoin or legacy-testnet acceptance journals: it requires
an authenticated height/hash hard ceiling, repeats those constraints inside the
execution routine, prints a funds-safety warning before storage or network
startup, and forbids an indefinite node plus explorer/RPC/wallet and automatic
AssumeUTXO-cleanup modes. This fixed-target mode does not define which networks
the ordinary daemon can execute.
An already completed validation directory can raise its ceiling only through
the explicit extension mode: the new authenticated height/hash must be on the
validated active header chain, must move forward, and is published atomically
only while the execution tip still equals the old target. A restart then
inherits the raised ceiling exactly as it inherited the original one.
The weekly/manual public-network smoke workflow wraps that path with an
authenticated height/hash target, a wall-clock deadline, a measured data
ceiling, a free-space reserve, and exact-target log verification. Its mainnet
default executes through Core 26 checkpoint height 295,000 in 1,008-block
high-memory atomic persistence batches filled through bounded 16-block peer
requests.
After observing block 1,000, it sends a termination signal; the
in-flight atomic batch may finish, then a second process must reopen the same
headers, execution state, UTXOs, undo, and retained ledger before reaching the
target. The range includes both historical BIP30 duplicate-transaction
exceptions, the BIP16 exception, P2SH activation, and the first subsidy
halving boundary. The first deep run exposed that the batch overlay rejected their
spent-and-recreated outpoint even though the durable layer supported it; after
aligning those semantics, the fresh 2026-07-23 restart acceptance run executed
both exceptions and completed height 105,000 in 2,350 seconds using
833,470,464 bytes. After the IBD hot-path optimizations, a resumable run stopped
exactly at height 193,000, exercised the BIP16 exception/activation era, and
proved that a completed-target restart requests no additional block. Its final
optimized 59,496-block leg completed in 2,191 seconds.
The same durable state was then extended to height 210,000 and completed at
hash `000000000000048b95347e83192f69cf0366076336c639f9b7228e9ba171342e`.
Under a concurrent development workload, quick repair processed a 6,592-block
leg at 11.38 blocks/second and deferred repair processed the following
7,016-block leg at 12.74 blocks/second.
After the locator and persistent-script-pool optimizations, an explicit
256-block run crossed checkpoints 216,116 and 225,430 and stopped exactly at
BIP34 activation height 227,931/hash
`000000000000024b89b42a942fe0d9fea3bb44ab7bd1b19115dd6a759c0808b8`.
Its final 6,923 newly executed blocks plus startup/recovery took 848 seconds
(8.16 blocks/second end to end); a completed-target restart requested no block.
The authenticated target was then extended to Core 26 checkpoint 250,000/hash
`000000000000003887df1f29024b06fc2200b55f8af8f35453d7be294df2d214`.
Checkpoint-wide script pooling first raised an adjacent 5,888-block live leg
from 10.57 to 12.95 blocks/second. Starting script jobs as soon as each block's
sequential transition was constructed then completed the final 6,965 blocks
plus recovery in 435.36 seconds (15.99 blocks/second), 12.9% above the adjacent
checkpoint-barrier leg and about 51% above the original per-block-barrier leg.
The exact target and a cold completed-target restart both succeeded without an
additional block request.
The next run reached Core checkpoint height 279,000/hash
`0000000000000001ae8c72a0b0c301f67e3afca10e819efa9041e458e9bd7e40`.
After checkpoint-wide UTXO folding, fresh-output proofs, single-pass durable
mutation, staged-archive reuse, and 1,008-block high-memory checkpoints, its
final 4,168-block leg took 246.75 seconds including cold startup (16.89
blocks/second). One steady 1,008-block checkpoint took 40 seconds (25.2
blocks/second), about 47% above the adjacent 256-block single-pass run's
roughly 17.1 blocks/second. A cold completed-target restart took 15.52 seconds
and requested no block.
The same state next reached Core checkpoint height 295,000/hash
`00000000000000004d9b4ef50f0f9d686fd69db2e03af35a100370c64632a983`.
Batch input prefetch reused one redb read snapshot and open hot/cold tables
instead of starting a durable read transaction per historical input. The final
3,904-block leg completed in 282.22 seconds including cold startup; its measured
steady interval rose from 16.19 to 18.45 blocks/second. The exact target and a
17.46-second completed-target restart both passed without a block request.

## UTXO layout

Each key is the Bitcoin outpoint's 32-byte txid in wire order plus a little-endian `vout`. The record stores amount, creating height, coinbase marker, last-touch time, and raw `scriptPubKey`. Outputs whose script begins with `OP_RETURN` or exceeds Core's 10,000-byte script limit affect transaction value accounting but are never inserted into chainstate or the explorer UTXO projection, matching `CScript::IsUnspendable`. `utxo_hot` is the write-optimized active tier and `utxo_cold` is the inactive tier. Moving tiers never changes consensus data. The legacy in-process aging interface uses a 60-day wall-clock window, but it is not a production boundary: historical snapshot import necessarily observes old coins as newly loaded. The operational replacement uses complete-replay consensus coin age in blocks. After the report selects a boundary, offline `--retier-utxos-window-blocks BLOCKS` scans the merged tiers in key order, classifies by creation height relative to the fixed execution tip, and commits at most 65,536 records per transaction. Its cursor and counters share the same transaction as every move, so a restart resumes without a giant transaction or ambiguous partial result; selecting a different tip/window safely starts a fresh idempotent scan. Tier metadata remains excluded from snapshot finalization and consensus identity.

Spent-output coin age is now accumulated as exact block-age/count rows in the
network-bound chainstate. A checkpoint aggregates ages before sorted writes;
connect and disconnect update the histogram in the same transaction as UTXOs,
undo, and the execution tip. Coverage metadata starts honestly at the first
instrumented block, and a reorg crossing that start clears the sample instead
of presenting a discontinuous history as complete. Offline
`--utxo-activity-report` scans the
outpoint-sorted UTXO tables in fixed-memory pages and reports, for
1/7/30/60/90/180/365-day and 2/3/5/10-year block-count candidates, historical
spend-hit rate, P50/P90/P95/P99/P99.9 spend-age quantiles, expected hot-first
tier probes per spend, current live-set population, and estimated local record
bytes. It emits a 99%-hit recommendation only when coverage is exactly blocks
1 through the execution tip and at least one million spends were observed;
partial upgraded stores remain useful measurements but cannot silently select
a production boundary.

The completed Mainnet genesis-to-935,000 replay observed 3,257,609,051 spends.
Its spend-age quantiles were P50 42, P90 8,299, P95 33,082, P99 122,194, and
P99.9 323,668 blocks. The smallest evaluated window reaching the 99% target was
157,680 blocks (approximately three years): 99.38467% historical spend hits,
65.95593% of base UTXOs, and 67.19178% of estimated base record bytes. A
separate height-935,001-through-959,730 sample contained 179,211,528 spends and
confirmed 99.42139% hits, P99 129,338 blocks, and 1.00578 expected hot-first
lookups per spend.

The selected 157,680-block boundary was physically applied to the live
height-959,688 chainstate by scanning 166,269,013 rows and moving 68,387,004 to
cold in 1,029.64 seconds. After the node cold-opened in 46 ms and validated 42
new blocks, a fresh-tip scan moved only 43,427 newly aged rows and ended with
97,862,624 hot / 68,429,071 cold rows. The report's predicted hot population
matched that physical count exactly. This boundary remains storage policy, not
snapshot or consensus identity; periodic operators may rerun the resumable
command after substantial tip movement.

The first offline scan against the completed Testnet4 chainstate at height
145,737 read 14,160,511 UTXOs in 27.67 seconds. Creation-age candidates retained
0.450% of records at 60 days, 1.029% at 90 days, and 10.037% at 365 days. That
directory predates spend-age instrumentation, so its zero spend samples
correctly produced no recommendation; these population figures are storage
measurements, not evidence for the final activity threshold.

redb is selected for the default node because its pure-Rust, ordered copy-on-write B-tree tables, ACID transactions, and concurrent readers keep the build portable. UTXO state is overwhelmingly point lookups plus batched deletes/inserts and needs ordered snapshot iteration. Active UTXOs, per-block undo, and the execution tip now share one physical database and one write transaction; a successful commit exposes all three and an aborted commit exposes none. Legacy split files are rejected instead of being guessed or upgraded in place.

Block validation runs against a lazy in-memory UTXO overlay and commits the net effect in one redb transaction. redb immediate durability and quick-repair/two-phase commit are enabled for active-chain commits. During IBD, contiguous blocks form a 64-block checkpoint by default; explicit high-memory validation may raise that checkpoint to 1,008 while retaining an undo record for every block in ordinary serving state. The writer folds all per-block changes into one outpoint-sorted checkpoint mutation, so an output created and spent inside the checkpoint never enters redb; retained per-block undo and execution-tip transitions remain independently addressable inside the same atomic transaction. Once only one new tip block is available it is committed alone. The acceptance invariant is always an old complete checkpoint or a new complete checkpoint, never a mixed UTXO/undo/tip state.

Mainnet output-collision handling follows Core's BIP30 optimization: after the
authenticated BIP34 anchor, ordinary blocks skip the redundant durable output
pre-scan, while both historical duplicate-coinbase exceptions still remove and
preserve overwritten coins for exact undo. The resulting fresh-output proof
lets the tentative overlay avoid a second durable lookup for each created
outpoint. A validated block's changes merge directly into the cumulative
checkpoint overlay instead of replaying every input and output through generic
store validation, and the final redb mutation obtains spent coins from its
single remove operation rather than issuing a get followed by a remove.

Within each block, prevout resolution, maturity/lock-time checks, value
accounting, and UTXO mutation remain sequential so an input can consume an
output created earlier in that block. The resolved prevouts are then immutable
script-validation jobs distributed across a persistent, bounded host-CPU
worker pool. Large checkpoints append each block's serialized checks to the
shared queue in 16-transaction work packets under one lock instead of locking
and returning a channel message for every transaction. Workers still pull
packets dynamically, return their earliest failure, and the final ordered
reduction selects the earliest block/transaction globally. Small single-block
input sets stay serial. After IBD constructs one block's
sequential transition, it immediately submits that block's immutable jobs
while the calling thread builds later blocks against the cumulative UTXO
overlay. One checkpoint-wide barrier remains before commit. This removes up
to 1,008 per-block joins and overlaps script work with transition construction
without moving tentative state into redb. Every script must pass before the
checkpoint can commit; out-of-order
completion still selects the earliest failing block and transaction and rolls
back all tentative mutations.
The pinned Core 26 `bitcoinconsensus` source adds a narrow transaction-level
ABI used by those jobs. It decodes the serialized transaction once, constructs
`PrecomputedTransactionData` once, then verifies every input against the same
immutable spent-output vector; the returned failure still identifies the exact
input. The independent `fuzz/` workspace repeats the root workspace's
`bitcoinconsensus` and `redb` path patches explicitly; Cargo patches do not
propagate across workspace roots. Its committed lock file, dated nightly check,
Clippy gate, and ASan regressions therefore exercise these exact reviewed
implementations rather than similarly versioned crates.io packages. The former
public one-input ABI repeated transaction decoding and common
signature-hash precomputation for every input. A hot release A/B on the same
five authenticated activation blocks (8,997 transactions and 23,331 inputs)
fell from 1.47 to 0.44 seconds, a 3.34× execution speedup. The complete pinned
Core 26 public transaction/script corpus plus real CSV, SegWit, Taproot, and
full activation-block fixtures exercise the added boundary.
Three isolated release runs of the historical full-block regression improved
from a 3.59-second median at `534c28c` to 2.36 seconds, a 1.52× speedup.
Against the immediately preceding scoped-thread implementation, a second
three-run hot release A/B improved the same workload from 4.00 seconds to a
3.30-second median (17.5% lower elapsed time).
On the adjacent live mainnet height-227,932+ run, the former per-block barrier
executed 5,888 blocks in 557 seconds including startup (10.57 blocks/second).
The first 3,328 blocks after switching the same durable directory to the
checkpoint-wide barrier took 257 seconds including recovery (12.95
blocks/second), a 22.5% throughput increase while preserving 256-block atomic
tips.
The follow-up producer/consumer overlap completed the final 6,965 blocks to
height 250,000 in 435.36 seconds including recovery (15.99 blocks/second),
12.9% above the adjacent checkpoint-barrier leg's 14.16 blocks/second and
about 51% above the original per-block-barrier leg.

Ordinary serving-chain commits retain redb quick repair. Bounded bulk
validation may explicitly defer the allocator-state repair write while keeping
immediate transaction durability; this reduces write amplification at the cost
of potentially slower post-crash reopen. The repeated SIGKILL/reopen matrix
covers both settings and still requires an old or new complete checkpoint.
Authenticated Bitcoin/testnet validation-only stores additionally discard
historical block undo and omit future undo writes: they have a fixed reviewed
target and are never exposed as reorganizing service nodes. Ordinary serving
and AssumeUTXO stores retain per-block undo.

Fixed-target validation folds a checkpoint's transitions with a keyed hash
table and one final sort, so outputs created and spent inside the same
checkpoint never touch disk. Inputs consuming outputs created earlier in that
same prevalidated batch are removed from the historical prefetch set. The net
change is encoded as immutable sorted validation deltas. Legacy `RVD3` is one
strict 16-byte header, fixed-width sorted outpoint/state/offset/length index,
and contiguous canonical UTXO-data value. `RVD5` retains that representation
inside at most 16 high-prefix shards plus a compact manifest. Shard keys and
the outpoints inside each shard are inserted monotonically, reducing random
write-page churn while a Bloom hit reads at most one bounded slice rather than
one giant fragmented value. The short-lived `RVD4` experiment used 256 shards;
it remains readable, but live 139–172-second checkpoints rejected its write
amplification. Encoding writes each UTXO directly into its aggregate shard
buffer rather than allocating one temporary record per coin. One
immediate-durability redb transaction inserts every shard, its manifest, and
all block transitions; no prefix is visible after failure.

Reads search newest deltas first by binary search and decode only a matched
UTXO. A 10-bit-per-update Bloom filter accompanies every row, and each 16-row
group shares a fixed aggregate Bloom filter, so historical base inputs
normally skip 16 records with three probes. The row filter and current group
aggregate are checksummed and committed atomically with every delta manifest,
including while the newest group is incomplete. Bulk prefetch resolves all
journal hits newest-first and issues one ordered parallel redb base lookup only
for unresolved outpoints. When every candidate-bearing row in one 16-row group
is sharded, all required row/shard pairs enter one dynamically balanced queue
served by at most the host's available workers. Each worker reuses one redb
snapshot and decodes matches while its shard guard is alive. Results are
sorted by descending row before publication, preserving newest-wins semantics
while bounding speculative reads to the current aggregate-Bloom group. A group
containing a legacy row retains strict row-at-a-time fallback. The batch
records which legacy rows caused the most candidate reads. The next checkpoint
rewrites up to 32 of them to RVD5, each in one atomic transaction, while its
archive stage, UTXO read snapshot, and complete network lookahead run
independently. The migration window naturally shrinks when fewer rows qualify.
Live validation migrated 756 hotspot rows without extending the critical path;
row-internal parallel reads reduced adjacent UTXO prefetch from 66–73 seconds
to 34–41 seconds, and the final group-wide queue read the 71-block live-tip
extension in 19.200 seconds.

An older RVD3 database without persisted filters undergoes one strict scan of
record size, ordering, state bits, contiguous offsets, and canonical UTXO
bytes, then installs all row and group filters in one immediate-durability
migration. A directory that already has row and completed-group filters scans
only a missing at-most-15-row partial group once and persists its aggregate.
Subsequent opens validate the redb-protected filter format, byte length,
SHA-256 checksum, per-row UTXO count, delta header, and exact execution-tip
alignment without scanning delta payloads. At height 432,684, the migration
opened the 18 GB production chainstate in 11.454 seconds and the next reopen
took 6.035 seconds, replacing the earlier approximately one-to-two-minute
filter rebuild. Ordinary mode rejects a non-empty delta table, and journal
mode rejects block-undo retention and disconnects. Explicit materialization is
one immediate transaction that folds the logical newest values into the base
trees and clears the journal plus both filter tables; periodic automatic
materialization was rejected because it merely deferred the superlinear
random-write cost.

Experimental fixed-target mainnet validation can reserve up to three ready
standby candidates while the chainstate opens asynchronously and the active
session receives bounded keepalives. It activates the first surviving
candidate as an auxiliary block source. Checkpoints wider than the 64-block
validation window split into ordered primary and auxiliary windows, request
and receive both concurrently, then concatenate them in active-chain order.
Production validation caps both parallel windows at 64 blocks—four 16-block
requests—after later mainnet blocks repeatedly pushed 128-block responses past
the 30-second peer budget. A larger configured
checkpoint downloads its remainder through additional bounded primary
windows. Any auxiliary request or response failure drops that source. An
unfinished auxiliary window is given only two seconds after the primary
window completes; every checksum-verified response already placed in its
caller-owned ordered slot survives cancellation, and the primary requests
only the missing hashes. The next of at most three already-ready auxiliary
candidates may then receive one bounded trial. This avoids a full auxiliary
timeout, whole-window duplicate transfer, and unbounded candidate cycling.
The earlier whole-window fallback bounded one observed slow-auxiliary download at 27.684 seconds
instead of 40.485 seconds. The ordinary measured median moved only from about
19.4 to 18.8 seconds; a more aggressive background-response experiment was
rejected after it exceeded the existing 30-second peer bound.

Transaction IDs computed for Merkle authentication are carried into execution
instead of hashing every transaction serialization again. Large sorted
input-prefetch sets are divided across host-CPU read workers; each owns one
redb read snapshot and the joined result preserves caller order. Empty
cold-tier validation stores skip that B-tree entirely. Dropping
309,112 obsolete undo rows followed by native offline compaction reduced the
live soak database from 23 GiB to 4.0 GiB; the experimental high-memory path
uses a 16 GiB redb cache so that compact working set and a larger write cache fit
without changing ordinary-node defaults.
The default checkpoint remains 64 blocks. Explicit bounded validation may use
up to 1,008 blocks (approximately 4 GiB at the consensus block-size maximum,
with an independent 1 GiB ledger-record ceiling) to amortize staged-ledger and
chainstate durability barriers on adequately provisioned machines; later
full-block eras should retain a lower memory-aware setting.
The post-BIP66 soak located that boundary empirically. One stable 1,008-block
checkpoint took 181.77 seconds after its working set crossed the redb dirty-page
cache threshold. Splitting the adjacent work into two 504-block checkpoints
took 84.71 seconds combined, and three 252-block checkpoints sustained 12.1
blocks/second without the superlinear commit tail. The public mainnet soak
therefore moved first to 252 blocks while the CLI retained the full explicit
range for measured hosts and chain eras.
Standalone bounded validation checkpoints additionally compute an exact
next-batch prefix only after the current batch's downloaded blocks pass
header, Merkle, structure, and deployment checks. A scoped network worker
actively requests and drains the complete next configured batch through
ordinary bounded primary/auxiliary 64-block response pairs while the caller
performs the current checkpoint's archive staging and sequential UTXO
transition. Partial auxiliary progress and the existing primary fallback are
preserved. Each completed window is immediately converted to compact consensus
bytes, releasing the expanded `Block`/transaction object tree before the next
window. The next iteration
decodes those bytes, verifies the stored blocks against its active-header
prefix, validates their structure, and downloads only the remainder. This
double buffer is bounded to 4 GiB by the 1,008-block validation ceiling and
Bitcoin's consensus maximum serialized block size, and never stages,
executes, commits, or requests above the immutable target. Archive stage still
durably precedes chainstate commit; only the independent future-block transfer
overlaps both operations. Continuously
draining the response avoids the
repeatable timeouts caused by leaving 128 responses unread throughout a
756-block execution phase. The daemon uses two Tokio workers so one can drive
the network reactor while the execution side blocks; current-thread test or
embedded runtimes take a deadlock-free serial fallback instead. A mismatch,
timeout, unsolicited response,
compact-block failure, or peer replacement retains the existing fail-closed
path. Although a 126-block checkpoint fits
entirely in one lookahead window and its first two batches took 29.0 seconds
combined, the 21-checkpoint long sample fell to 5.41 blocks/second as twice as
many macOS `F_FULLFSYNC` barriers accumulated. The adjacent 252-block lookahead
sample sustained 5.97 blocks/second, so the public mainnet soak retains 252 as
its measured default.

The pinned redb 2.6 storage backend is vendored with one local write-buffer
change: dirty pages are sorted by file offset and adjacent pages are coalesced
into writes capped at 8 MiB. This preserves page bytes, immediate durability,
and commit ordering while avoiding hash-map iteration order turning one flush
into hundreds of thousands of random small writes. On the exact same mainnet
height-346,921–347,928 batch, total time fell
from 117.72 to 72.46 seconds and execution/persistence from 82.95 to 30.77
seconds. The authenticated run subsequently reached BIP66 height 363,725/hash
`00000000000000000379eaa19dce8c9b722d46ae6a57c2f1a988119488b50931`;
a cold completed-target restart requested no blocks.
The subsequent transaction-level consensus ABI eliminated repeated
transaction parsing and shared signature-hash precomputation across inputs;
the exact authenticated five-block release fixture improved from 1.47 to 0.44
seconds while preserving input-specific failures.
The same production directory subsequently reached BIP65 height 388,381/hash
`000000000000000004c2b624ed5d7756c508d90fd0da2c7c679febfa6c4735f0`.
At height 381,113, 171.8 seconds of offline compaction reduced the chainstate
file from 10.88 to 7.48 GB and improved the adjacent execution/persistence
measurement from 72.31 to 13.48 seconds. The file later expanded under normal
copy-on-write reservation, so the measured benefit is attributed to reclaimed
fragmentation and page locality rather than treating compact file length as a
permanent size bound. A cold completed-target restart advanced only the header
store to height 959,424, requested no block, and stopped at the same BIP65
height/hash.
The append-only validation path subsequently stopped exactly at CSV activation
height 419,328/hash
`000000000000000004a1b34462cb8aeebd5799177f7a29cf28f2d1961716b5b5`.
Its 71-block tail committed in 12.22 seconds. A cold restart advanced only the
active header store from 959,431 to 959,434, requested no blocks, and exited at
the same CSV height/hash.
The target was then atomically extended to SegWit height 481,824/hash
`0000000000000000001c8018d9cb3b742ef25114f27563e3fc4a1902167f9893`.
After persisted Bloom migration, the first 252-block checkpoint at
432,685–432,936 completed in 25.624 seconds, with 9.231 seconds in
execution/persistence.
The same journal then reached SegWit activation height 481,824/hash
`0000000000000000001c8018d9cb3b742ef25114f27563e3fc4a1902167f9893`.
The final 252-block checkpoint took 43.780 seconds. A completed-target restart
opened chainstate in 13.078 seconds, advanced headers from 959,450 to 959,452,
requested no blocks, and stopped at the identical target.
In the post-SegWit era, four 252-block checkpoints took 44.850–69.021 seconds.
Four adjacent 504-block checkpoints took 60.608–82.237 seconds, or
30.3–41.1 seconds per 252 blocks. An initial 1,008-block checkpoint took
126.796 seconds. At heights 495,433–498,456, three later 1,008-block
checkpoints took 163.505, 185.132, and 169.897 seconds. Their median was 6.0%
below twice the preceding nine-checkpoint 504-block median; the directly
adjacent comparison improved by 16.3%. Because RVD3 avoided the old base-tree
superlinear commit, 1,008 remained attractive until the next batch at height
504,505 exceeded the ledger's separate 1 GiB canonical-record ceiling. It
failed before staging or chainstate mutation. The Taproot run therefore uses
756 blocks; its first four checkpoints completed in 124.259–148.291 seconds.
A follow-up that assigned one 128-block window to each of three auxiliaries
was rejected: public-peer failures and independent response tails widened
complete-batch time to 73.243–127.829 seconds. Production validation instead
uses one auxiliary across successive paired 64-block windows, never leaves
more than 64 responses outstanding on either session, and abandons an auxiliary half
that trails the primary by more than two seconds. Each pair is appended in
active-chain order. Partial progress is retained, only missing hashes fail
back to the primary, and at most two remaining ready candidates are tried.

Every validation checkpoint may actively receive one complete next configured
batch on a scoped network worker while chainstate execution runs. Primary and
auxiliary responses are continuously drained in bounded 64-block pairs,
reduced to consensus bytes, and retained only as an ordered hash-checked
prefix for the next iteration. Slow auxiliaries retain partial progress and
fall back to the primary exactly as foreground download does. This can fully
overlap network with both archive staging and the long sequential UTXO
transition without leaving unread responses to time out; the additional
worst-case serialized payload allocation is bounded at 4 GiB by the 1,008-block
validation ceiling.

The durable UTXO read phase is also detached from mutation: it computes the
exact external sorted outpoint set and opens its read snapshot concurrently
with archive staging. The resulting batch-bound value is checked against the
same outpoint sequence before execution, and no chainstate write begins unless
staging succeeded. On the first 560-block live sample, the 41.156-second UTXO
read hid the complete 7.972-second archive stage. Hot legacy-row sharding uses
the same window and must finish before the execution write transaction begins.

This pipeline completed the authenticated mainnet Taproot target at height
709,632/hash
`0000000000000000000687bca986194dc2c1f949318629b44bb54ec0a94d8244`.
The 28 checkpoints served completely from the cross-execution cache had an
87.312-second total median; an adjacent checkpoint before active overlap took
182.653 seconds. The exact 252-block tail took 50.817 seconds. A cold reopen
took 40.337 seconds, persisted six newer headers through 959,520, requested no
blocks, and stopped again at the exact target.

Configured checkpoint count is also constrained by the archive's canonical
record-byte ceiling after structure validation. If a downloaded batch would
exceed 1 GiB, the executor stages and commits its longest byte-safe prefix and
keeps the verified suffix as the exact beginning of the next compact prefetch
buffer. Background download fills only the remaining capacity after that
suffix and starts after its last header, preventing both duplicate requests
and target overrun. The first full-chain boundary at height 764,065 reduced a
756-block request to a 726-block atomic commit, carried 30 blocks, and
replenished the complete 756-block lookahead during execution.

The same production directory completed genesis-to-tip validation at the
authenticated height 959,520/hash
`000000000000000000003a8648dadb49e67db65326f85b50651661dd7c237299`,
then followed the active header chain through height 959,592/hash
`000000000000000000019190d596b445008319f199f8ee6f6af0e73cbc440667`.
A completed-target cold restart opened chainstate in 14.608 seconds and made
no block request. After the two authenticated target extensions, the final
cold restart opened in 14.152 seconds, observed no newer header, requested no
block, and exited zero at the exact execution/header tip. The retained freezer
occupied 418 MiB and chainstate 243 GiB, leaving 661 GiB free.

Large downloaded batches validate their independent block structure on
bounded host-CPU workers before the sequential UTXO transition begins. Work
chunks and joins stay in height order, so a failure remains the earliest
failing block even if a later worker completes first. Adjacent 1,008-block
mainnet checkpoints reduced this phase from 11.489 seconds to 1.652 and 1.705
seconds, approximately 85%, without changing transaction identifiers,
deployment contexts, or the later atomic execution.

The same workers now serialize their validated blocks, and archive encoding
streams those length-prefixed bytes through a bounded four-worker zstd encoder
while incrementally computing the authenticated record digest. This removes a
second batch-sized uncompressed buffer. On the first 756-block production
sample, archive staging fell from the preceding 6.723–7.175 seconds to 4.949
seconds. Large validation-journal prefetches likewise partition each
independent group-Bloom probe over bounded CPU workers and merge both match and
reject partitions in original input order; row lookup and newest-first
resolution remain unchanged.
The first five complete all-optimization checkpoints had a 124.198-second
median versus 135.252 seconds for the preceding four 756-block checkpoints.
Execution/persistence median improved from 59.870 to 50.367 seconds (15.9%),
and staging median improved from 7.098 to 4.453 seconds (37.3%), with identical
atomic checkpoint semantics.
The subsequent packetized script scheduler measured 46.015, 46.165, 49.246,
51.255, and 45.921 seconds for execution/persistence across adjacent
756-block checkpoints at heights 598,249–602,028. Its 46.165-second median
was 10.2% below the immediately preceding five-checkpoint 51.401-second
median, while retaining the same consensus engine, error order, and barrier.
At heights 651,925–655,704, all first five 64-block-window checkpoints
completed on one primary session after multiple public peers had failed the
old 128-block response budget. Download time was 93.017–123.082 seconds with
a 95.388-second median; total time had a 168.974-second median and incurred no
chainstate reopen between checkpoints.

SIGINT and SIGTERM are awaited alongside the daemon future. Cancellation drops
network tasks and closes the durable stores cleanly; synchronous atomic
execution reaches its next async boundary first. Each completed validation
checkpoint explicitly yields before beginning a fully prefetched successor,
so repeated cache hits cannot starve that outer shutdown selector. A
stalled-handshake process test exits successfully after logging that durable
stores are closing, avoiding the allocator-repair cost caused by the operating
system's default abrupt termination.

The `mdbx` Cargo feature provides an experimental durable MDBX hot/cold UTXO backend. It is not a production chainstate selector yet because undo and tip metadata must first be moved into the same MDBX transaction. On the local 100-block/100-spend+create release fixture, durable MDBX completed in about 39 ms versus redb's 733 ms without quick repair and 1.43 s with quick repair; those numbers are a direction signal, not a deployment decision, and must be repeated on target NVMe/HDD hardware with full block undo and metadata included.

Recovery gates cover transaction-stage failure, simulated disk-full writes, repeated process SIGKILL followed by reopen, and truncated database copies. A damaged file must either reopen to a complete committed state or be rejected explicitly; it must never be served as partially current chainstate.

## Snapshot trust model

The target trust model is Bitcoin Core AssumeUTXO, not a new MPT. The node first
validates the complete header chain and selects its maximum-cumulative-work
active branch. A snapshot base block must be the exact blockhash at a
version-pinned Core chainparams height on that branch. Core's compiled UTXO-set
hash and chain-transaction count authenticate the decoded state provisionally;
ordinary validation then advances that state from the base to the live tip.
Because Bitcoin block headers do not contain a UTXO-set root, maximum-work
membership alone does not authenticate arbitrary snapshot contents. Independent
genesis-to-base block execution and an exact UTXO-set hash match are what remove
the assumed-state marker.

The implemented Core 31 loader accepts `dumptxoutset` v2 metadata and grouped
coins, enforces canonical CompactSize/Core VARINT, amount and script
decompression, count/order/height/value/EOF bounds, and computes Core's exact
double-SHA256 UTXO-set commitment before activation. Its second streaming pass
rechecks an rBTC semantic digest inside the atomic chainstate transaction. A
Core database cursor does not promise numeric vout order inside a txid group:
the loader sorts the bounded group numerically for Core's commitment, rejects
duplicate vouts, and then sorts the same group by rBTC's existing
little-endian-vout database key. On 2026-07-25 an external Core 31 Testnet4
height-120,000 v2 file with 13,870,119 coins activated against the compiled
blockhash, `hash_serialized`, and chain-transaction count after both streaming
passes. The assumed chain then executed ordinary blocks through live height
145,763 while a separate directory replayed genesis through exactly 120,000.
That replay matched 13,870,119 entries and 1,350,756,785 canonical bytes, so
finalization cleared the assumed marker. A subsequent cold launch opened the
fully validated chainstate in 173 ms, executed three new blocks through
145,766/hash
`000000000074ec24258d33c6e340032db208128adde0f7841c83fdbbeb3e25ea`,
and exited in 6.16 seconds.

A retained Core snapshot can additionally serve direct point lookups without
being expanded into redb. Offline
`--build-core-snapshot-index SNAPSHOT --snapshot-index-output FILE` streams the
file once under the same canonical-form rules as the activation loader,
re-derives Core's exact UTXO-set commitment against the compiled release
identity, and atomically publishes a sidecar container: a BBhash minimal
perfect hash function over every outpoint (written in safe Rust over keyed
SipHash-2-4 from the vendored `bitcoin_hashes`, expected three to four bits
per key at gamma 2) plus a bit-packed table holding each coin's byte offset and the
backward distance to its txid group header. Field widths are derived from the
actual maxima, so a mainnet-scale table costs roughly seven bytes per coin.
The container binds the snapshot's network, base block hash and height, coin
count, exact length, and full SHA-256, and is sealed by a trailing SHA-256
that open verifies before use; the snapshot header identity and length are
rechecked at open and the full content digest can be re-verified on demand.
Lookups resolve one slot, read the 32-byte txid at the group header and the
coin's CompactSize vout from the file, and only then decompress that single
coin's amount and script template, so results are exact — a foreign outpoint
that the minimal perfect hash function maps to an arbitrary slot is rejected
by the byte comparison, never answered probabilistically. Coins keep Core's
compressed representation on disk; nothing is imported, and the index never
substitutes for activation's trust checks.

The optional `mdbx` feature builds a snapshot-backed overlay chainstate on
that base. One MDBX environment holds four named tables — coins created above
the base, tombstones for base coins spent above it, per-block undo keyed by
block hash, and metadata binding the base identity plus the execution tip —
so one read-write MDBX transaction commits a block's complete UTXO effect,
undo record, and tip advance, with the same compare-and-swap tip linkage the
redb store enforces. Reads resolve overlay → tombstone → base; an overlay
entry shadows the base even when a tombstone coexists, and reorg restores
pick their target from the coin's creation height, covering the full
delete/create/restore matrix for both base and overlay coins. Base coins
synthesize their BIP68 creation median-time-past from a header-derived
per-height table, because Core's snapshot format does not carry it. Block
execution reaches this store through the new `ExecutionChainStore` trait: the
consensus connect/disconnect entry points are generic over it, and the
unified redb store implements the same trait unchanged.

The environment is opened with a hard MDBX geometry ceiling (for example
3 GiB), so growth past the configured budget fails the offending commit
closed with `MDBX_MAP_FULL` instead of expanding — an engine-enforced bound
redb cannot express, and the decisive reason this mode selects MDBX while
redb remains the default unified chainstate; the trade is a vendored C
dependency against the default build's pure-Rust property. Approaching the
ceiling triggers a rebase: a pinned MVCC read view streams the old snapshot
minus tombstones merged with the overlay into a fresh compressed snapshot at
the current tip, deriving the new Core-format UTXO-set commitment during the
write; the access-index rebuild then re-decodes the complete file against
that self-derived identity, so a compression asymmetry cannot survive
publication. One final MDBX transaction clears the overlay, tombstone, and
undo tables and switches the stored identity, after which the folded state
serves from the new immutable base and the capacity budget is available
again. Undo data does not survive a rebase, so disconnection cannot cross
the new base — exactly the contract an AssumeUTXO activation establishes —
and rebases therefore run during catch-up, when the tip is not at
reorganization risk.

`--snapshot-overlay-catchup SNAPSHOT --snapshot-overlay-index INDEX` runs the
complete catch-up on this chainstate. The mode reuses the ordinary outbound
peer pool and failover: after headers-first synchronization it verifies the
base block on the active header chain (a fresh environment resolves the
compiled Core 31 identity; a rebased environment resumes its stored
self-derived identity), derives the base's creation-MTP table from headers,
and then drives the same `download_execute_batch` pipeline the redb node
uses — 16-block protocol requests, parallel structure validation, ledger
staging, prefetch overlap, and the generic consensus executor — against the
overlay store, including stale-tip disconnection back to the active chain
and block-undo pruning below the retained-ledger floor. Between batches it
checks the capacity high-water mark and rebases onto
`utxo-<height>.dat`/`.rbtcidx` beside the current snapshot when the
configured threshold (default 85% of the default 10 GiB ceiling) is reached.
The mode is deliberately bounded: it requires `--once` and `--data-dir`,
conflicts with explorer, wallet, index, and other snapshot or offline modes,
executes to the header tip observed after the initial synchronization, and
exits; a binary built without the `mdbx` feature refuses it at startup.

`--snapshot-overlay-engine redb` runs the same catch-up on a redb overlay
instead, so the two engines can be compared on identical work. The stores are
built to one contract — the same base, the same four logical tables, the same
one-transaction-per-block atomicity, the same `overlay → tombstone → base`
read order, and a shared implementation of the base reader, identity codec,
and rebase merge — and the catch-up loop itself is written once, generic over
an `OverlayCatchupStore` trait. Only two behaviours differ, and they are the
comparison:

- **Budget enforcement.** MDBX's geometry ceiling refuses the offending
  commit outright with `MDBX_MAP_FULL`. redb has no such ceiling, so the
  budget is policy-enforced by measuring the file after each commit, which a
  single batch can overshoot before the next check sees it.
- **Space reclamation.** redb's `compact()` shrinks the file in place;
  `libmdbx-rs` 0.6.6 exposes no equivalent, which is why the MDBX rebase has
  to recreate its environment file. `--snapshot-overlay-compact-percent`
  (default 50, well below the 85 rebase threshold) controls how early the
  redb engine tries reclaiming space before resorting to a full rebase.

On 2026-07-29/30, a real mainnet `utxo-935000.dat` (9,387,990,306 bytes,
164,241,311 coins) was downloaded from a third-party community mirror
(`bitcoin-snapshots.jaonoctus.dev`, `files-vps02.jaonoctus.dev/utxo-935000.dat`)
and indexed offline:

```
rbtcd --build-core-snapshot-index utxo-935000.dat --snapshot-index-output utxo-935000.rbtcidx
```

That authenticated the file against the compiled 935,000 mainnet identity in
2m18s and published a 1,155,791,488-byte index (19 BBhash levels, 3.30 bits
per key). Catch-up then ran to the header tip observed at connect time:

```
rbtcd --network bitcoin --data-dir DATA --once \
  --snapshot-overlay-catchup utxo-935000.dat --snapshot-overlay-index utxo-935000.rbtcidx \
  --snapshot-overlay-capacity-bytes 10737418240
```

A first attempt at a 3 GiB ceiling (the default at the time) reached height 960,203 after
one rebase (at 937,240, 2,240 blocks of live-2024-era Ordinals/Runes-congested
mainnet growth) and then repeatedly hit `MDBX_MAP_FULL`: `last_pgno` — the
copy-on-write B-tree's high-water mark, and the only thing the geometry
ceiling actually checks — never shrinks after `clear_table`, so reusing the
same environment file after a rebase eventually loses the whole budget
regardless of logical content. That soak also surfaced a genuine one-coin
`compress_script` defect (a height-707,034 P2PK output using SEC1's rare
hybrid uncompressed tag 0x06/0x07, which `libsecp256k1` parses as a valid
point but Core's `CompressScript` never compresses; compressing it anyway
made decompression's `serialize_uncompressed` silently normalize the tag to
0x04, breaking the re-verified UTXO-set commitment) and a missing
`ledger.discard_staged()` reconciliation step that let a chainstate-commit
failure leave a staged ledger segment blocking every retry. All three are
fixed: `compress_script` now requires the exact standard tag, `rebase_into`
recreates the environment file instead of clearing its tables in place, and
the driver discards a leftover staged segment before its main loop.

With those fixes and a 10 GiB ceiling, the same snapshot and index drove a
clean run from height 935,000 to the live tip: two rebases (at 947,032 and
956,056, roughly 9,000–9,700 blocks apart — proportionally longer than the
3 GiB ceiling's single 2,240-block interval, confirming budget and interval
scale together), reaching height 960,205 with the overlay at 42% of its
10 GiB budget and exiting 0.

A latest-code cold rerun on 2026-07-30 used the same 164,241,311-coin
`utxo-935000.dat` (9,387,990,306 bytes) and its 1,155,791,488-byte index. It
validated 25,313 blocks in 396 batches and exited 0 at height 960,313. Process
wall time from the first daemon log record was 90.29 minutes; the interval
from the start of the first execution batch through the final commit was
83.07 minutes (305 blocks/min). Batch totals averaged 10.83 seconds, with a
3.00-second minimum and a 55.32-second cold-first-batch maximum. Ten transient
peer warnings were recovered through failover and no error was logged.

The rerun rebased at 947,224 after 12,224 blocks and at 958,296 after another
11,072 blocks. Each rebase paused block progress for about 4.61 minutes:

| Rebase height | Coins | Snapshot bytes | Index bytes | Folded overlay | Dropped tombstones |
|---:|---:|---:|---:|---:|---:|
| 947,224 | 165,489,673 | 9,456,185,634 | 1,164,575,992 | 8,416,963 | 7,168,601 |
| 958,296 | 166,103,491 | 9,485,481,367 | 1,168,896,520 | 7,253,120 | 6,639,302 |

The two measured rebase pauses totaled 9.22 minutes, 11% of execution wall
time. The final 2,017-block overlay occupied 2,080,374,784 logical bytes
(2,063,597,568 allocated bytes), reported as 19% of the hard 10 GiB ceiling.
The active compressed base plus MPHF index was 10,654,377,887 bytes. Benchmark
directories retain prior rebase generations for audit, so their aggregate
disk usage is not the active working-set size.

The redb engine was then measured over a complete run on the same snapshot,
with a 3 GiB budget and compaction enabled at 50%. It reached the same tip
960,220 and exited 0, taking 3.64 hours for 25,220 blocks (115 blocks/min)
across 395 batches, 212 compaction attempts, and 24 rebases:

- **Compaction works, and is what contains the file — but it settles above
  the budget, not at it.** Seventeen of 212 attempts released space, and the
  steady-state pattern is unmistakable: the overlay repeatedly grew to
  exactly 4.00 GiB, compaction brought it back to about 2.94 GiB, and the
  cycle repeated, releasing roughly 1.06 GiB each time. Two further passes
  fired right after a rebase had emptied the tables, taking 2.06 GiB to
  0.25 GiB and 2.00 GiB to 0.26 GiB. So in-place compaction genuinely bounds
  the file — the reclamation `libmdbx-rs` 0.6.6 offers no API for — but it
  bounds it at an equilibrium oscillating between 2.94 and 4.00 GiB. Peak
  usage was 4.00 GiB against a 3.00 GiB budget: **133% of the configured
  limit, permanently**. A measured-after-the-fact budget combined with
  coarse, doubling file growth does not enforce a ceiling; it finds a stable
  orbit above one. MDBX's `MDBX_MAP_FULL` refuses the oversized commit
  instead, which is what a hard budget actually requires.
- **Rebasing dominated the wall clock.** The 24 rebases consumed 122 minutes,
  56% of the run, against 92 minutes (42%) of actual block execution.
  Intervals ranged 640–2,112 blocks with a median of 1,024. Because every
  rebase streams and re-indexes a ~9.4 GB snapshot, rebase frequency — not
  per-block execution speed — is what sets end-to-end catch-up time at a
  small budget.
- **Retained bases cost far more disk than the budget saves.** Each rebase
  publishes a new snapshot and index and, by design, leaves the previous one
  in place as evidence. Twenty-four rebases therefore accumulated about
  248 GB of retained bases — roughly 10.6 GB apiece — to hold an overlay
  budget of 3 GiB. Any deployment running at a budget small enough to rebase
  often needs a retention policy for superseded bases, or the disk saved on
  the overlay is lost many times over beside it.

redb was then run again at a 10 GiB budget, which changes the picture
completely: it reached the tip in 2.40 hours over 25,299 blocks with **zero
rebases**, ending at 57% of budget with the original 935,000 base still in
place. The overlay's steady-state working set is the same in both runs — the
file settles around 4 GiB regardless of what it is told — so the 3 GiB budget
was simply below what this workload needs, and the 24 rebases it forced were
the consequence of that, not of the engine. Set the budget above the working
set and the same engine never rebases at all.

Comparing the two 10 GiB runs is closer to controlled but still not clean:
MDBX covered 940,376→960,205 (19,829 blocks, 55 minutes, 2 rebases, 42% final
usage) having resumed from an already-advanced state, while redb covered
935,000→960,299 (25,299 blocks, 144 minutes, 0 rebases, 57% final usage) from
a cold base. Different block ranges and different starting states make the
raw 361 vs 175 blocks/min figures unusable as an engine verdict.

Normalising by maintenance I/O per block does compare. redb avoided rebases
by compacting 72 times, each rewriting the surviving data: roughly 314 GiB
written to free 100.7 GiB, about **12.7 MiB per block**, against MDBX's two
rebases at roughly 21 GiB — about **1.1 MiB per block**.

Most of that gap turned out to be a tuning defect on the redb side rather
than a property of the engine. The overlay's working set settles near
4.4 GiB, and the compaction trigger sat at 50% of the 10 GiB budget — 5 GiB,
barely above it. Each pass therefore rewrote the whole ~4.4 GiB survivor to
reclaim about 1 GiB, and the file re-crossed the trigger almost immediately.
A percentage-of-budget threshold cannot prevent that on its own: once the
file is above it, every batch qualifies regardless of how little was freed
last time. Compaction now additionally requires the file to have grown half
again over its size after the previous compaction, which ties the decision to
how much garbage has actually re-accumulated and adapts to any budget and
working set. Replaying the run's observed sizes under that rule gives an
estimated 9 compactions and 38 GiB — about **1.5 MiB per block**, close to
MDBX's 1.1.

Raising the threshold instead was considered and rejected on the same replay:
at 75% the trigger would never have fired, since the overlay peaked at
6.61 GiB (66%), so compaction would stop entirely and the file would drift
into the 85% rebase threshold — trading ~4 GiB rewrites for ~10.6 GiB ones.
The threshold stays at 50% and the growth gate does the work.

A latest-code rerun measured those changes on the same snapshot at the same
10 GiB budget. redb reached 960,335 in 76 minutes over 25,335 blocks
(333 blocks/min) with **two** compactions instead of 72, rewriting 10 GiB
instead of 314 — **0.40 MiB per block**, a 32-fold reduction, and below the
0.80 MiB/block the MDBX rerun measured. The growth gate is what did it: the
first compaction fired at 5.01 GiB and the second not until 6.69 GiB, roughly
6,000 blocks later, where the old policy re-fired on nearly every batch.

That figure should not be read as redb now costing less maintenance than MDBX,
because the two runs did not end in the same state. MDBX rebased twice and
finished at 19% of budget with a 2,017-block overlay over a base advanced to
958,296. redb never rebased and finished at 58% with a 25,335-block overlay
over the original 935,000 base. A rebase does more than reclaim pages: it
folds the overlay into a new base, so every later lookup traverses less
overlay before reaching it, and it resets the undo table. redb deferred that
work rather than avoiding it. The honest comparison is that redb's tuned
compaction defers a rebase cheaply, and MDBX's rebase does more work for
twice the write cost — which of those is better depends on whether the run
ends or continues.

SQLite was then benchmarked as a third candidate, since it is the only
surveyed engine offering both capabilities the other two each lack — an
engine-enforced ceiling through `PRAGMA max_page_count`, which redb has no
equivalent for, and in-place compaction through `VACUUM`, which the MDBX
binding does not expose — and it is already linked into every build through
`bdk_wallet`'s rusqlite feature, so adopting it would add no dependency. The
benchmark drives it through the same `UtxoStore` harness as the other two,
with a `WITHOUT ROWID` table keyed by the same 36-byte outpoint so a lookup
is one B-tree descent, and `synchronous = FULL` to match the others' fsync
-per-commit durability.

Point-lookup latency, the operation block execution performs most:

| engine | 10,000 UTXOs | 2,000,000 UTXOs | p99 at 2M |
|---|---|---|---|
| redb | 726 ns | 1,584 ns | 5.2 µs |
| MDBX | 1,933 ns | 2,353 ns | 3.8 µs |
| SQLite | 2,411 ns | 4,053 ns | 10.3 µs |

SQLite is 2.6x slower than redb and 1.7x slower than MDBX at the larger size,
with a p99 roughly double either. That is the cost of the SQL layer on a path
that executes tens of thousands of lookups per block, and it is enough to
rule SQLite out for the UTXO hot path despite its otherwise attractive
feature set. It remains the only candidate that could serve a store needing
both a hard ceiling and in-place compaction, so the measurement is recorded
rather than discarded.

Two cautions about these numbers. MDBX scaled best across the two sizes
(1.2x versus redb's 2.2x), so the ranking at 164M coins is not settled by a
2M-coin run. And the commit column of the same benchmark is not a fair
comparison at all: the redb entry drives the full `RedbChainStore`, committing
tip and undo alongside the UTXO mutation, while the MDBX and SQLite entries
drive a bare UTXO store. Only the lookup column compares like with like.

The latest cold-base MDBX rerun makes the same-budget comparison closer:
MDBX covered 935,000→960,313 in 90.29 process minutes (83.07 execution
minutes), with two rebases and 19% final usage; redb covered
935,000→960,299 in 144 minutes, with zero rebases and 57% final usage. Their
raw process rates are about 280 and 176 blocks/min respectively, but network
peers, filesystem cache state, and block workload still prevent treating that
ratio as an isolated engine microbenchmark. The MDBX rerun wrote
21,275,139,513 bytes of snapshot-plus-index rebase output, about 0.80 MiB per
validated block, making redb's measured 12.7 MiB/block maintenance rate about
sixteen times larger on these same-budget runs.

A further round of write and memory work was then measured on MDBX at the
same 10 GiB budget, over the same `utxo-935000.dat` and index, reaching
960,345 in 397 batches over 25,345 blocks and exiting 0.

Two changes were under test. Block undo is now stored zstd-compressed, chosen
because a per-table breakdown of the completed redb overlay showed undo as
the second largest item — 998,307,764 stored bytes across only 951 retained
blocks, about 1,025 KiB per block — and because MDBX has no compaction, so
anything that slows file growth directly delays a rebase. Separately, the
snapshot index no longer holds its packed offset table in memory.

The results that do not depend on the machine:

| | prior rerun | this run |
|---|---:|---:|
| Rebases | 2 (947,224, 958,296) | **1** (955,096) |
| Rebase output rewritten | 21,275,139,513 bytes | 10,646,320,407 bytes |
| Maintenance write | 0.80 MiB/block | **0.40 MiB/block** |
| Retained undo | 1,025 KiB/block | **442 KiB/block** |
| Final overlay | 19% of budget | 25% of budget |

Halving the maintenance write is a direct consequence of rebasing once rather
than twice, and the deferral is attributable: the prior rerun crossed the 85%
threshold at 947,224, while this one reached it at 955,096, about 7,900 blocks
later, on the same budget and base. Freeing roughly 600 MB of undo is what
bought those blocks.

The undo ratio of 2.32x is a conservative floor, because the two sides are not
measured the same way: redb's 998,307,764 is a logical stored-byte count while
MDBX's 435,425,280 counts whole pages, including partial fill and page
overhead. The compression is at least this good.

Wall-clock is deliberately absent from that table. This run's execution
interval was 62.81 minutes for 25,345 blocks against the prior rerun's 83.07
for 25,313, but the prior rerun's rebase artifacts — `utxo-947224.dat` and
`utxo-958296.dat` — are not present on the machine that produced this one, so
the two ran on different hardware and the ratio measures that as much as any
code change. The 62.81 minutes stands as a new local reference point, not as a
speedup. The single rebase paused block progress for 4.20 minutes, against
about 4.61 for each of the prior two.

Final overlay composition, at 2,724,679,680 bytes of high-water mark:

| table | entries | page bytes |
|---|---:|---:|
| `utxo_overlay` | 4,100,830 | 864,468,992 |
| `utxo_spent_base` | 3,683,808 | 253,349,888 |
| `block_undos` | 961 | 435,425,280 |

Live tables account for 57% of the high-water mark; the remainder is
copy-on-write garbage that `last_pgno` still counts, which is why the
geometry ceiling is reached well before the live data would suggest.

Index memory was measured separately, on the real 164,241,311-coin index,
looking up 200,000 outpoints taken from the snapshot itself. The queries are
shuffled, because a block's inputs bear no relation to where their coins sit
in the base; measuring them in snapshot file order flatters the snapshot reads
and overstates every rate below. Each figure is the mean of three runs on an
otherwise idle machine, which matters more than it sounds — see the caution at
the end of this section.

| variant | peak working set | one at a time | batched |
|---|---:|---:|---:|
| Offset table resident | 2.17 GiB | 211,142 | — |
| Offset table on disk, worst-case window | **0.15 GiB** | 127,609 | 185,678 |
| plus a coin-sized read window | **0.15 GiB** | 149,851 | 201,767 |
| plus one read covering txid and coin | **0.15 GiB** | 168,489 | **238,936** |

Moving the table out of memory cost 40% of the single-lookup rate on its own.
Three changes took it back and past: resolving a batch in file order, reading
a 128-byte window instead of Core's 10,030-byte worst case, and covering the
group txid and the coin in one positioned read when they are close enough
together. The result is that the on-disk path is now **13% faster than the
resident table while holding a fourteenth of its memory** — not the trade-off
this started as.

The batch resolves in three phases. Every slot is computed first, with no I/O.
The offset-table entries are read next in ascending slot order, which is
ascending byte order. The coin records are read last in ascending snapshot
offset. Each phase moves forward through one file rather than jumping around
it, and one window buffer serves the whole batch. Both overlay engines
classify a batch against the overlay and tombstone first and hand the misses
over in a single call.

The order in which those three changes helped is worth recording, because it
did not match expectation. Cutting the read window by 78x barely moved the
batched path — copying was not its cost — while removing one of three
positioned reads per lookup moved it 18%. The remaining cost is per-call
overhead, not bytes.

An in-process cache for the table was considered and rejected. A minimal
perfect hash distributes slots uniformly, so hit rate would scale linearly
with cache size with no working-set knee — the condition under which a
replacement policy earns its keep. Entries are bit-packed at 53 bits against
an LRU's roughly 40 bytes of per-entry overhead, so the page cache holds
several times more table per byte. And the memory would be anonymous rather
than reclaimable, reintroducing exactly what moving the table to disk removed.

A caution about how these were obtained, since it nearly produced two wrong
conclusions. An earlier set of these figures was taken while a 25,000-block
capture was running on the same disk, and the probe's spread under that load
was 9.5%. Measured that way, the combined read appeared to be a 14% regression
and was withheld from a commit on that basis; measured on an idle machine it
is an 18% improvement. The same contamination understated the read-window
change as 2.6% on the batched path when it is 8.7%. Storage benchmarks on a
loaded machine are not measurements.

Batching was then measured on a full catch-up rather than a probe: the same
machine, the same `utxo-935000.dat` and index, the same 10 GiB budget, with
only the batched read added. The node routes every base lookup through it —
`prefetch_prevalidated_active_block_utxos` resolves a whole 64-block batch's
inputs in one `get_many` call, and execution then reads from that result — so
the `utxo-prefetch` timing isolates exactly what changed.

Over 25,344 matched blocks:

| component | one at a time | batched | change |
|---|---:|---:|---:|
| `utxo-prefetch` | 223.6s | 199.2s | **−10.9%** |
| `execution-core` | 2988.3s | 2973.1s | −0.5% |
| `structure` | 121.2s | 122.5s | +1.0% |
| `download` | 63.2s | 91.1s | +44.0% |

The 24.4 seconds saved is real but small against a catch-up: about 0.7% of
total batch time. Two things account for the gap between this and the probe's
47%. `get_many` probes the overlay and tombstone tables for every outpoint
before any base read happens, and that part is unchanged; and the share of
inputs that reach the base at all falls steadily as the overlay fills, from
nearly all of them at 935,000 to a minority by the tip. Measured on the first
49 batches alone, where almost everything misses to the base, the improvement
was 17.8%.

End-to-end wall clock is not usable here and is deliberately not quoted as a
result. This run took 73.67 minutes against the previous 62.81, but the
regression is network: `download` rose 44%, the portion of `execute` outside
`execution-core` — the wait for blocks to arrive — rose from 166.5s to 805.9s,
while `execution-core`, `structure`, `stage`, and `publish` were all flat
within 1%. A soak that fetches 25,000 blocks from live peers cannot resolve a
storage change worth 0.7%, and treating its total as a verdict would report
peer quality as a code result.

The run is a strong control in one respect that does not depend on timing at
all: it rebased at height 955,096, the same height as the previous run, with
byte-identical output — 165,911,302 coins, 9,478,768,311 snapshot bytes,
1,167,552,096 index bytes, 11,823,028 folded overlay entries. Reordering reads
provably did not disturb what gets written.

Replaying blocks from a retained ledger rather than fetching them made the
execution path measurable, and the first measurement overturned the assumption
the optimization work had been proceeding on. With the network removed,
`execution-core` split as commit 67.7%, validate 16.0%, submit 2.3%, and
script-wait 1.7%. Script verification is not the cost of executing a block
here: the deferred batch hides it so completely that the workers finish about
100 ms into a six-second batch and then wait. Storage does.

Splitting the commit the same way located the largest single item in the whole
run:

| commit part | share of commit |
|---|---:|
| Base lookups (nested in mutate) | 36.5% |
| B-tree mutation, excluding those lookups | 26.5% |
| Durable commit, where any fsync lands | 28.5% |
| Block undo | 4.5% |
| Batch fold | 3.7% |

At 36.5% of a commit that is itself 67.7% of execution, those base lookups
were about a quarter of all execution time. They come from the duplicate check
on created coins — one full snapshot index lookup per new output, roughly
250,000 per batch, essentially all misses. An earlier estimate had put them at
65 ms by counting only spent base coins, missing the created path entirely and
landing a factor of twenty-seven low; the estimate would have sent the work
somewhere else entirely.

The check is not redundant and was not weakened. Above the BIP34 anchor the
in-memory execution path deliberately skips its own durable probe and
documents that the commit will catch a collision, which makes this the release
build's only durable duplicate check. Only the read order changed: the probes
are collected and resolved in one file-ordered batch.

Two replays of the same corpus, differing only in that change, paired
batch-for-batch over 80 batches:

| | before | after | change |
|---|---:|---:|---:|
| `commit-base-lookup` | 130.1s | 80.2s | **−38.3%** |
| `core-commit` | 357.9s | 304.6s | −14.9% |
| `execution-core` | 528.0s | 474.7s | **−10.1%** |
| batch total | 584.4s | 530.8s | −9.2% |
| `commit-sync` | 102.4s | 101.9s | −0.5% |
| `core-validate` | 82.5s | 82.0s | −0.7% |

Every untouched component moved by less than 2%, which is the signature a
controlled comparison should have and the reason the replay harness was built:
the networked soak that preceded it moved 19% on peer quality alone and could
not have resolved this.

What remains inside the commit is the durable flush at 28.5% and B-tree
mutation at 26.5%, neither of which has been addressed. Base lookups are still
26.3% of the reduced commit, and their remaining cost is the hash computation
plus two positioned reads per probe. Cutting the probe count rather than
ordering it would need an index keyed by txid instead of by outpoint, since a
transaction's outputs share a txid — which is a concrete, measured argument
for a decision that had been deferred on other grounds.

Relaxing the commit's durability was tried on the strength of that 28.5% and
withdrawn. MDBX offers three modes weaker than `Durable`, and two of them —
`SafeNoSync` and `UtterlyNoSync` — buy write speed by pinning the last steady
commit so freed pages cannot be reused, which grows the database. Under a hard
capacity ceiling where growth is what forces a rebase, and a rebase rewrites
roughly 10 GB, that trade is paid back at a rate that makes it a loss. Only
`NoMetaSync` writes the same pages and defers nothing but metadata, so only it
was implemented and measured.

Two replays of the same corpus, back to back on the same machine under the
same load, paired over 133 batches:

| | durable | no-meta-sync | change |
|---|---:|---:|---:|
| `commit-sync` | 220.4s | 372.8s | **+69.2%** |
| `commit-mutate` | 348.8s | 353.8s | +1.4% |
| `commit-base-lookup` | 147.4s | 147.0s | −0.3% |
| `execution-core` | 967.2s | 1155.9s | +19.5% |
| batch total | 1071.6s | 1276.4s | +19.1% |

Deferring the metapage flush made the flush 69% more expensive, not cheaper,
and every untouched component stayed within 1.4%. The mechanism is in MDBX's
own description: the deferred flush is taken at the next non-read-only commit.
Catch-up commits once per batch with a large dirty set, so nothing is saved —
the deferred metadata is simply paid alongside the next batch's own flush. The
option was removed rather than kept as a knob that costs durability and returns
nothing.

One piece of that work is retained on its own merits. The batch commits the
chainstate before the ledger, so a durable commit can only leave the chainstate
ahead, and the driver's new truncation of the ledger to the executed tip is a
no-op today. It stays because the reverse is not merely inconsistent but
terminal: resuming would append at a height the ledger already covers and fail
on contiguity from then on, with nothing else in the system able to repair it.
The reindex driver already does this for the same reason.

Independent of budget, the enforcement point also stands:
redb cannot hold a hard ceiling by measurement alone, and its equilibrium sat
a third above the number it was given at 3 GiB. Per-batch execution (64 blocks) held steady at
6–12 seconds including download, structure validation, staging, consensus
execution, and publish. `capacity()` reports `last_pgno`-based usage — the
figure `MDBX_MAP_FULL` actually checks — not a freelist-adjusted "logical
bytes" figure, which the same soak found could stay low immediately after a
rebase while the environment was already unable to grow further.

Snapshot distribution remains explicitly operator-selected instead of becoming
a new trust service. `--download-core-assumeutxo` accepts only a bounded,
credential-free HTTPS URL plus an exact expected length and a new output file.
It splits the transfer into independently restartable 64 MiB HTTP ranges, runs
1–8 workers, requires HTTPS across redirects, HTTP 206, identity encoding, and
the exact length of every range, fsyncs completed chunks, and atomically
publishes the assembled file only after a full transport SHA-256 pass. Resume
metadata binds source, output, length, and chunk size; another identity fails
closed. The transport digest detects transfer differences but is deliberately
not a trust anchor: the release-pinned Core UTXO commitment and independent
genesis replay remain authoritative.

The local rBTC snapshot format v3 includes an anchor height/hash, count,
canonical uncompressed byte length, and a SHA-256 of that entry stream.
Container self-checks detect damage but do not establish
authenticity: activation additionally requires an independently distributed
network/height/block-hash/count/record-bytes/records-SHA-256 tuple and an exact
match at that height in the selected active header chain. Trusted startup
compares this manifest identity before zstd decompression, then verifies the file
in a bounded-memory first pass. Authenticated count and byte length cap total
decompression work; records must be strictly outpoint-ordered, manifests are
capped at 64 KiB, and scripts at Core's 10,000-byte consensus bound. A second
streaming pass inserts directly into redb while recomputing count, length, and
SHA-256 inside the activation transaction, closing file-replacement races
without retaining the full UTXO set in RAM. Only an untouched unified chainstate
can accept the snapshot. All UTXOs, the execution tip, a durable
assumed-snapshot marker, and the authenticated count/length/digest enter one
immediate-durability redb commit; an existing UTXO, undo, pending transition,
advanced tip, prior or incomplete marker, decoder/order/count/length/digest
failure, or failed commit rejects the operation without residue. Snapshot
exports publish through a same-directory temporary file, file sync, atomic
rename, and directory sync instead of exposing partial output. Reorganizations
can disconnect blocks added above the base but cannot cross it because the
assumed history has no undo. The offline `--assumeutxo-snapshot` mode requires
all five explicit trust-identity arguments and conflicts with networking, fetch,
explorer, wallet, and one-shot modes.

The marker deliberately survives successful execution and restart, so assumed state is never confused with independent genesis-to-tip validation. Activation persists both the full record-stream digest (which authenticates transport and local tier metadata) and a logical UTXO-set digest over outpoint, value, creation height, coinbase status, creation MTP, and script. The latter deliberately excludes `last_touched`, because independent replay at another wall-clock time must not change consensus identity. `--validate-until-height` plus `--validate-until-blockhash` turns a separate non-assumed data directory into a bounded genesis validator. Header sync may proceed beyond the target, but the target must be on the active header chain and block batching uses it as a hard ceiling; execution, ledger, and projections cannot commit a block above it. A matching restart exits without another block request, while a mismatched hash, assumed source, or already-higher tip is rejected. The resulting directory can be supplied to offline `--finalize-assumeutxo`.

`--background-assumeutxo` orchestrates active assumed-state service and independent genesis validation concurrently. The two runtime tasks share only the process self-connection nonce, one physical-commit turn, and a small synchronized progress record; each owns a separate outbound connection/failover cycle, peer database, headers, chainstate, and ledger. Both catch-up paths atomically append sorted 16-prefix UTXO-delta shards plus per-row and grouped Bloom filters rather than randomly rewriting the complete base tree at every checkpoint. The active side retains block undo. If its selected header branch reorganizes, it atomically folds its current overlay into the base before invoking the ordinary disconnect path. A durable nonempty journal automatically re-enables this mode after restart. Finalization folds the bounded base-to-live active overlay once; the independent genesis result is authenticated without a complete in-memory map by loading one lexically contiguous base prefix, applying only that prefix's chronological shards, sorting it, and feeding the canonical identity before advancing to the next prefix.

Synchronous staging, UTXO prefetch, script execution, and redb work enter Tokio's blocking region, so each worker is replaced while it waits and two concurrent pipelines cannot deadlock the next-window network futures. Each task has a bounded 8 GiB redb cache, keeping the aggregate bulk cache at 16 GiB while reducing random B-tree read amplification relative to the ordinary 1 GiB live-node cache. Network receive, structure validation, immutable freezer staging, and next-window prefetch remain independent; final redb transactions share one process-wide commit turn, avoiding concurrent random-I/O cache thrash on the same physical device without merging their failure domains. Only the API-serving side maintains an explorer projection; the isolated validator does not build an unused historical transaction index. The active loop publishes its execution/header tips. Until they match, the validator continues with the smaller of its configured batch and a bounded 252-block checkpoint, without an artificial pause; the ceiling keeps consensus-maximum payload below one GiB and amortizes freezer/index/fsync work. Once active serving catches up, its configured batch and pause limits are restored. Keeping a bounded independent window avoids per-block network round trips and durable database transactions without enlarging one peer request or partial publication. The loopback explorer exposes this state at `/api/v1/validation`, including the immutable target, both tips, remaining work, lifecycle phase, throttle state, and terminal error. The active loop consumes successful completion, compares the independently streamed UTXO identity, and commits marker removal while its API remains live. An active-side failure aborts and joins validation; a validation or finalization failure aborts and joins active service, terminates the combined service, and leaves resumable state. `--once` waits for both tasks and performs the same finalization after their stores close. `--complete-assumeutxo` remains the sequential operational fallback.

Both paths use the active marker as target authority, reject equal or nested canonical data directories, symlink paths, and Unix inode aliases, and bind the first target as immutable execution metadata after consensus-configuration binding. Restart accepts only the same height/hash, automatically restores a persisted ceiling when the CLI target is omitted, rejects assumed state, and refuses a target behind the durable tip. The atomic execution store independently rejects a different hash at the target or any transition above it. `--validation-batch-size` caps each aggregate atomic chainstate/explorer checkpoint at 1–1,008 blocks and defaults to 64; in background mode the same cap applies independently to active base-to-live execution so both pipelines can use bounded dual-peer windows. The downloader fills a checkpoint through 16-block protocol requests so a larger durability batch does not enlarge one peer request. The 1,008-block ceiling implies approximately 4 GiB of consensus-maximum payload and is an explicit high-memory validation-host setting, with the ledger's independent 1 GiB record ceiling still enforced. `--validation-pause-ms` applies only to the validator, so serving chainstate retains no artificial pause. Deferred allocator repair applies to both background bulk pipelines: ordinary commits remain atomic and durable, orderly database close emits one quick-repair allocator snapshot, and only an unclean stop pays a full repair on the next open. Finalization requires identical network and consensus identities, exact validation tip/base equality, an optional bound-target/base match, active-header membership, and a streaming canonical merge/hash of both validation UTXO tiers. Only marker removal is committed after the identity is rechecked; snapshot-origin metadata remains durable.

Validation storage is retained by default. The destructive `--cleanup-validation-dir` option is accepted only by the automatic completion modes and is gated by a versioned, owner-only marker created only when rBTC first observed an absent or empty directory. The marker uses strict size-bounded JSON and binds a canonical network and target height/hash; its contents and containing directory are synced on Unix before the claim is considered durable. After successful finalization, cleanup canonicalizes the active and validation paths again, reopens the validation chainstate, requires its non-assumed tip and bound target to equal the snapshot base, allowlists top-level rBTC artifacts, and recursively rejects symlinks and special files. It then atomically renames the directory to a randomized sibling quarantine, syncs the parent, removes the quarantine, and syncs the parent again so both namespace transitions are durable. Failure of the first parent sync rolls the rename back before deletion; failure of the final sync reports that removal completed but its namespace durability is uncertain. An unowned legacy directory or any unexpected artifact is preserved with an error; manual two-step finalization never deletes it.

Explorer recovery treats a snapshot as a projection boundary rather than inventing historical undo. On first open, a single explorer transaction clears incompatible projections, reads the chainstate through exclusive cursor pages that merge hot and cold tiers, installs current address UTXOs, and records the execution tip as an immutable baseline. Blocks above it retain normal block/transaction/address indexes and explorer undo. If a later active reorganization reaches the baseline, the current chainstate is atomically re-baselined. Pre-baseline block and transaction history remains explicitly unavailable. rBTC's container remains a private transport format and is not advertised on Bitcoin P2P.

## Pruned historical ledger

`PrunedBlockLedger` stores zstd-compressed block segments in numbered ring slots. Its policy has both a block-count and byte ceiling; the default `1008` blocks / `1 GiB` means approximately one week of ten-minute blocks. After recovery, validation, and physical trimming satisfy the current target, the ledger atomically publishes an owner-only, strictly decoded, versioned `ledger-policy.json`. A future schema version fails closed without rewriting the file; interrupted file or directory synchronization reopens to either the prior complete policy or the new complete policy. Symlink, non-regular, and hard-linked policy files are rejected. Archive format v2 authenticates the exact uncompressed record length in addition to the record and 4 MiB piece hashes; imports enforce a 1 GiB decompression ceiling and Bitcoin's 4,000,000-byte serialized-block ceiling before retaining decoded blocks. Existing v1 archives remain readable under a block-count-derived decompression ceiling. Online ledger archives use zstd level 1 because retention is byte-bounded and encoding lies on the IBD hot path. A downloaded segment is completed and synced before chainstate mutation; when its entire validated prefix commits, the ledger verifies its compressed piece hashes and atomically renames that exact file into the ring instead of decompressing and encoding it a second time. Partial-prefix recovery retains the full decode/re-encode path. A directory sync precedes live-index publication, so the index cannot durably reference an archive namespace transition that was never synced. The live index retains the newest contiguous segments satisfying both bounds. After that index is durable, files absent from it are unlinked and the directory is synced, following the same metadata-first physical-reclamation invariant used by btcd's flat-file store and geth/N42 freezer tables. A restart first adopts any provably contiguous rename that outran its index commit, then removes all remaining unindexed slots; it never deletes from wall-clock age alone. Thus steady state is bounded by the live ledger plus one transient staging/archive publication, rather than by the ring-slot namespace size. Old block bytes are no longer locally queryable after rotation; headers and UTXO state remain. On startup the ledger validates indexed slot manifests, adopts a complete contiguous segment whose rename beat its index commit, and reconstructs a missing/corrupt index from the newest contiguous slot chain. Reorg truncation durably records its boundary before deleting newer segments or atomically rewriting a crossing segment, so restart repeats the operation safely. Tests cover recovery when interruption occurs before mutation, after newer-segment deletion, after the crossing-segment prefix rename, after a wrapped slot is renamed but before its index commit, after a staged validated prefix is renamed but before its index commit, and while retired-slot removal fails; malformed intents fail closed without pruning. A per-ledger durability boundary injects one-shot failures at all file and directory sync points across staging, wrapped-slot replacement, index and policy publication, retired-slot reclamation, truncation, and intent removal; reopen must expose either the intact old state or the complete published state.

The offline freezer audit is deliberately separate from ledger open/recovery.
It acquires a shared existing data-directory lock, so it neither races an
exclusive node nor rewrites the lock marker. A fixed slot namespace avoids an
unbounded directory walk. Explicit segment and compressed-byte budgets bound
work; each selected archive is checked in two sequential passes, first over
4 MiB compressed piece hashes and then through a 64 KiB streaming decompressor
that checks exact record length, SHA-256, block framing, and count. The command
does not create logs or open redb stores. Its JSON distinguishes complete
verification from budget exhaustion and orders restore/reacquisition before
index rebuild or unindexed-file removal; these are plans only, never implicit
repairs.

Manual prefix pruning reuses the same immutable-segment boundary. Its read-only
plan validates the persisted policy, index continuity, slot metadata, and
archive manifests without opening the ledger, then commits all inputs and exact
outputs to a SHA-256 token. Apply takes the exclusive node lock, requires a
complete clean streaming audit, reopens with the persisted policy, and refuses
any stale token before writing. It selects only complete segments and caps the
request so at least 288 retained-tip blocks remain. A versioned, synced intent
precedes mutation; the reduced index is atomically published before unindexed
files are removed and directory-synced. On restart, an unpublished intent
repeats the index transition, a published index cannot re-adopt its old prefix,
and cleanup finishes before the intent is removed. Fault injection covers
intent file/publish/removal, index file/publish, and retired-slot sync.

The data-directory schema boundary is separate from redb's container version.
The owner-only root manifest binds one Bitcoin network and assigns explicit
versions to every persistent subsystem. Existing directories without it are
treated as legacy v0, fully preflighted under their existing rules, and only
then atomically published as v1. Once present, the manifest is validated before
any mutable database open. Strict decoding, a minimum-reader field, exact
component inventory, network binding, owner-only/single-link checks, and
future-version preservation prevent an older binary from silently opening or
downgrading newer state.

Offline cross-store verification has two layers. `--verify-storage` remains
strictly read-only and verifies every freezer container without database open.
`--verify-chain` instead takes the exclusive process lock and explicitly opens
redb in recovery-capable mode. It requires existing headers, chainstate,
freezer, and root manifest, so it cannot manufacture a plausible empty node.
Every persisted header is contextually replayed to reconstruct the maximum-work
chain. The command then requires the execution tip on that chain, a clean
complete freezer audit, freezer/execution tip equality, and matching block
payload/header/undo identities over a bounded retained suffix. Each overlapping
archive is piece-checked and decompressed once with only one consensus-sized
block buffered.

Complete local history has a separate recovery path.
`--reindex-from-freezer OUTPUT` first replays the source header database and
selects its maximum-work active chain, then requires a clean freezer covering
exactly heights 1 through that tip. The source chainstate is neither opened nor
trusted. A separate, non-nested output receives an exact active-header prefix
and an immutable validation target under a durable owner marker. Aggregate
archive reads decompress each overlapping segment once. CPU-parallel structure
validation precedes authoritative contextual execution, while restart-safe
freezer staging overlaps batched UTXO prefetch; chainstate publication and
sorted undo retirement preserve the normal commit order. Recovery discards
unexecuted staging or publishes only the prefix already represented by the
durable execution tip. Ordinary startup refuses the marker. Exact target
completion, a final bounded header/execution/freezer/undo cross-check, and
marker removal are the only promotion path, so the damaged source remains
untouched as evidence.

When local history is incomplete, `--reindex-chainstate OUTPUT` uses the same
ownership and promotion state machine but no source freezer. It fully replays
the source header DAG, enforces minimum chainwork, and pins the selected
maximum-work tip as the output execution store's immutable height/hash ceiling.
Only the authenticated active header chain is copied. The normal bounded peer
manager requires full-history and witness service, can download adjacent block
windows through two peers, and overlaps the next network window with parallel
structure validation, immutable staging, UTXO prefetch, and sorted atomic
execution. Every block hash must match the work-selected chain and every
transition passes complete contextual consensus and script rules, so no new
Merkle-state commitment is introduced or assumed. A reorganization that
removes the pinned target is a refusal. On exact completion the retained
freezer/header/undo relationships are checked, the temporary validation target
is atomically cleared only when it equals the durable tip, and only then is the
owner marker removed.

`index_policy` is the storage-lifecycle boundary between consensus pruning and
rebuildable projections. Each explorer, wallet, transaction, spent-output, or
basic-filter state supplies a durable best height and required first height.
Activation compares its next missing height with the freezer floor and an
explicit authenticated-peer-history capability. A current UTXO baseline is
accepted only by the explorer/wallet families and remains visibly partial
historical coverage; tx, spent-output, and BIP158 filters require blocks.
Pruning refuses to cross a lagging enabled index's next required height. Once
an index is caught up, its self-contained records and rollback data survive
old block-file deletion. The policy module has no chainstate mutation API, so
index disable/removal/rebuild cannot become a consensus transition.

The optional projections are physically separate network- and kind-bound redb
files. `txindex` maps txid to active height/hash/position and stores the
previous row in its block undo so historical BIP30 replacement is reversible.
The spent-output index maps a serialized outpoint to its active spender. The
basic-filter index feeds each executed block and the exact spent-prevout
scripts from chainstate undo into rust-bitcoin's BIP158 constructor, then
persists the filter, filter hash, chained BIP157 header, and block-hash lookup.
Each validation window is one transaction per index: records are sorted by
key, the new tip and every rollback row commit together, and an invalid later
block aborts the whole window. After the authoritative chainstate commit, the
independent explorer and optional-index transactions run concurrently with the
wallet projection; all workers are joined successfully before events or the
freezer publication barrier advance. The stores can therefore lag chainstate
only across a process failure; startup rewinds non-active tips and replays the
missing suffix. A tx index can obtain old active blocks from a full-history peer.
Spent-output and filter reconstruction additionally requires retained
execution undo and fails closed with an explicit full reindex instruction when
that information has crossed the prune floor. Freezer publication waits for
every enabled tip, rollback rows below the retained floor are deleted in a
sorted transaction, and both automatic and manual pruning use the same gate.
Disk preflight adds one maximum serialized block per configured batch and per
enabled index as conservative copy-on-write headroom, while reporting current
index bytes separately. Embedded status/events and authenticated
`getindexinfo`, `gettxindexlocation`, `gettxspendingprevout`, and
`getblockfilter` expose the independent tips and bounded queries.

IBD first writes each downloaded batch to a checksum-protected staging archive. Blocks become visible in the retained ledger only after their UTXO transitions have reached the durable execution tip. On restart the daemon truncates archive data above the recovered active execution tip, publishes only the active validated prefix of a staged batch, and backfills a missing retained suffix from a full-history witness peer. This coordinates the separate redb chainstate and file ring without claiming an atomic transaction across storage engines.

The v1 transport rejects payload declarations above Bitcoin Core 26's 4,000,000-byte protocol maximum before allocating the payload. Handshake user agents are limited to 256 UTF-8 bytes for both local and remote versions, outbound `getheaders` locators are capped at Core's 101 hashes, and `addr`/`addrv2` responses are capped at 1,000 entries. Request/response waits count every raw frame, including answered keepalive pings, against a 32-message budget; this prevents a ping stream from bypassing the bounded state machine. A repeated `version` after handshake is a terminal protocol error. With protocol version 70016 peers the handshake sends BIP339 `wtxidrelay` followed by BIP155 `sendaddrv2` before `verack`, matching Core's ordering; older peers receive only `verack`. The address-discovery primitive requests `getaddr`, converts legacy and BIP155 IPv4/IPv6 entries, ignores unsupported families and zero ports, and deduplicates socket addresses. The validating daemon stores only full-history+witness entries in a network-bound `peers.redb`; public networks reject non-routable and reserved IP space, while regtest permits local testing addresses. Core-inspired timestamp normalization applies a two-hour learned-address penalty and a 30-day horizon. A 64-entry source-group ceiling and 4,096-entry global ceiling bound one peer's influence and disk use. The store atomically generates a 256-bit random bucket key when created or when an old database first reopens; wrong-length metadata fails closed. Domain-separated SHA-256 maps learned address/source groups among 1,024 new buckets and successful address keys/groups among 256 tried buckets, with 64 entries per bucket. A learned record can persist up to eight distinct source-group references; each additional reference is admitted with probability `1 / 2^n`, maps independently, is schema-bounded, and defaults to none for legacy rows. A verified full-service handshake bypasses a saturated new-source quota, becomes tried, and frees all prior new references. Address-pool updates physically discard stale or newly invalid records; capacity eviction retains unfailed peers that completed synchronization sessions, then handshake-only successes, known lower successful-handshake latency, higher completed block-response throughput, and fresher records while enforcing each table's keyed bucket ceiling. Startup candidate selection applies the same persistent reputation, latency, and throughput ordering, round-robins buckets within each target IPv4 `/16` or IPv6 `/32` group, and takes one target group before taking a second. This remains a keyed bounded foundation rather than every Core addrman selection and terrible-entry rule. The daemon generates one cryptographically random self-connection nonce for the process run and reuses it across its fallback connections, rather than assigning a different identity to each peer attempt.

Promotion is collision-aware at the destination's exact keyed tried `(bucket, slot)`. A vacant slot promotes immediately even when the bucket contains other entries; an occupied slot keeps the successful challenger explicitly in new, and a strict size-bounded metadata vector atomically records at most ten challenger/incumbent pairs with the peer table. On restart, unique incumbents are inserted after manual peers and before ordinary candidates. Their attempt is persisted before dialing. A full-service handshake refreshes the incumbent and removes all of its collisions; connection or handshake failure demotes it to new and promotes the best-reputation queued challenger, with all membership and collision changes committed together. Queue overflow discards the oldest collision record without discarding its challenger. Legacy records default to tried exactly when they have prior success. This closes bounded hashed-slot incumbent probing and replacement.

After BIP130 `sendheaders`, protocol 70014+ sessions send BIP152 `sendcmpct(false, 2)`: rBTC declares witness-aware compact-block decoding but does not request unsolicited high-bandwidth announcements. Central routing rejects `cmpctblock` short-ID plus prefilled-transaction counts, `getblocktxn` indexes, or `blocktxn` transaction counts above 16,666, the maximum implied by Bitcoin's 4,000,000-weight block and 240-weight minimum serializable transaction. Receiving a reciprocal version-2 preference enables compact inventory across the validating, wallet-backfill, ledger-recovery, and one-shot fetch paths. Reconstruction expands differential prefilled indexes, requires the coinbase at position zero, maps version-2 wtxid short IDs only when both the block position and local candidate are unique, and requests every remaining index in order. Wrong hashes/counts and impossible prefilled layouts are protocol violations. A transaction or short-ID collision that yields a Merkle/witness-commitment mismatch instead issues one full witness-block fallback, whose normal contextual validator remains authoritative. Every compact download snapshots the bounded peer-admission pool plus an insertion-ordered cache of at most 64 unique wallet transactions that completed active-peer delivery. Re-insertion refreshes a wallet transaction and the 65th unique entry evicts the oldest. High-bandwidth announcements and outbound compact relay remain open.

The daemon accepts an ordered, deduplicated set of at most 16 explicit `--connect` candidates, then fills unused slots with the newest fresh persisted candidates when the process starts. It starts every candidate connection and v1 handshake in that bounded stage concurrently, while consuming completed sessions strictly in the original order. A later handshake can therefore remain owned by an independent hot-standby task while an earlier active session synchronizes; terminal success aborts and joins unused connection tasks so sockets and database handles are released before return. Before dialing, the pool loads the current durable header DAG once; each full-service standby owns a clone, sends a nonce-matched ping every 30 seconds, then performs one bounded `getheaders` step and contextually validates PoW, difficulty, time, checkpoints, and deployments without writing shared storage. This continues while the active peer synchronizes. Invalid headers terminate the standby as an objective protocol failure; successful activation reports its independently validated height, then the normal active path still resumes from authoritative durable headers. Pongs and header responses use the existing 30-second/32-frame/4,000,000-retained-byte bounds. Crossed pings are answered, other application frames are retained in order for the eventual active consumer, and a failed task is classified through normal failover when its priority is reached. Activation is selected only between complete keepalive, header-poll, or transaction-write operations rather than cancelling a partially read or written frame. Only after the entire explicit/persisted stage fails does the daemon query network-specific Bitcoin Core 31 bootstrap seeds and run the same bounded ordered-concurrent procedure for that stage. Seed lookups run concurrently with a five-second per-seed deadline, at most 16 seed authorities, at most 64 results per authority, and the same 16-candidate global connection ceiling. Round-robin selection prevents one response from monopolizing the pool; duplicate, zero-port, private, local, documentation, transition, multicast, reserved, and actively discouraged public-network addresses are discarded before connection. Repeatable `--dns-seed HOST[:PORT]` replaces the pinned list and `--no-dns-seeds` disables lookup. Testnet4 uses its two network-specific Core 31 seeds and port 48333. Connection, handshake, service-capability, header, and block-transfer failures advance to the next candidate; reopening the stores reconstructs the active header and execution tips before the replacement peer is queried. Every candidate attempt increments durable failure state before its task starts network I/O, making crash/restart obey the same exponential retry delay. A successful full-service handshake resets that ordinary state immediately even when the session is waiting behind a higher-priority peer. Separately, objective malformed-frame/ordering/bound violations, invalid PoW/difficulty/checkpoint headers, and invalid freshly downloaded blocks enter a network-bound persistent discouragement table capped at 1,024 addresses. Its cooldown starts at one hour, doubles to one day, and resets its count after seven quiet days. A verified handshake clears only ordinary retry state; the protocol entry survives until the requested synchronization session completes successfully, so a repeat invalid-block sender cannot erase escalation by handshaking. I/O errors, timeouts, missing services or blocks, obsolete versions, disconnected headers, and future-time headers remain ordinary failures. Matching Core 26's manual-connection protection principle, `--connect` candidates do not receive automatic discouragement. Every peer that completes a full-history/Witness handshake is inserted or refreshed as verified and has its ordinary retry delay reset, including explicit and DNS-derived peers. Ordinary delays start at one minute and cap at six hours. A completed requested synchronization session additionally commits its completion time and a saturating completion count; these fields default to zero when old records are decoded, survive reopen, and rank end-to-end-proven peers ahead of otherwise equivalent handshake-only candidates. The daemon also stores the most recent successful outbound handshake duration, capped to 1–60,000 ms; old records default to zero as an explicit unknown value, and known lower latency breaks ties only after failure and completed-session reputation. Each completed requested block batch separately accumulates exact wire payload bytes and response-wait time. Failed or incomplete batches do not contribute; after a successful session with downloaded blocks, the resulting rate is capped at 1 GB/s and higher known throughput breaks the next tie before freshness. Updates retain an existing address's original source group and history while eligible new records can accumulate the bounded extra source references described above. Learned addresses from the current run become candidates on the next restart. Sync completion remains tied to validated cumulative work rather than the peer's untrusted handshake-advertised height. Standbys also relay validated local wallet transactions, but adaptive block-relay scoring and capacity-driven connected-peer eviction remain in the peer-manager gate.

Before persisted collision incumbents or ordinary candidates are selected, the store atomically applies Core-style terrible-entry hygiene. Attempts in the preceding minute are protected. Otherwise, a last-seen time more than ten minutes in the future, zero or over 30 days old, three consecutive failures without a success, or ten failures after more than seven days without success makes the row ineligible and physically removes it together with stale collision references. The same predicate filters read-only candidate queries, and every verified handshake or completed session resets consecutive failures.

Consensus deployment selection is explicit at every header, structure-validation, and block-execution call. Network defaults mirror the pinned Core 31 chainparams; regtest accepts Core's `taproot:start:end[:min_activation_height]` version-bits override with the 144-block/108-signal window and repeatable `name@height` buried overrides for SegWit, BIP34, DER signatures, CLTV, and CSV. Those heights jointly select minimum header versions, BIP34 coinbase commitments, BIP68/113 locks, BIP141 commitments, and BIP147 NULLDUMMY. Matching the final Taproot-capable `libbitcoinconsensus` semantics, ordinary blocks always receive the mutually dependent P2SH, WITNESS, and TAPROOT interpreter flags; Core's three historical exception hashes replace that base set before DERSIG/CLTV/CSV/NULLDUMMY are layered on, preserving every later active rule. BIP30 output-collision checks are disabled only for the two historical repeats or below height 1,983,702 when the active header chain proves Core's mainnet/testnet BIP34 anchor; an alternate fork or missing anchor keeps the conservative check enabled. Default and custom Signet blocks additionally undergo BIP325 commitment extraction, synthetic transaction reconstruction, and their selected challenge execution through the same repository-owned `bitcoinconsensus` engine before any chainstate commit. The custom challenge's CompactSize serialization determines the four-byte P2P message start; unknown custom networks start with zero minimum chainwork, no assume-valid anchor, and no inherited default seed. Keeping interpreter flags and network gates separate from `segwit_active` prevents premature commitment enforcement and unsafe flag combinations. A canonical deployment encoding is bound when a fresh execution database is initialized and cannot be changed in place, even while the recorded tip is genesis, because another store may contain an interrupted transition. The default and Taproot-only encoding remains byte-compatible with existing databases; a buried override uses a versioned extension containing all five heights, and a custom Signet extension contains the exact challenge. A restart with different parameters is rejected before network I/O, wallet opening, header replay, recovery, or block application; older databases can migrate only under the legacy default.

The ordinary headers-first/block-execution daemon is enabled for Bitcoin, legacy testnet, Testnet4, Signet, and regtest. Testnet4 applies BIP94 at both contextual header boundaries: retarget calculation uses the first block of the previous period as its base difficulty, and each new period's first timestamp cannot move more than 600 seconds behind its parent. Its Core 31 minimum-chainwork, assume-valid anchor, DNS seeds, P2P magic, and port are network-specific. A real run validated every Testnet4 header through height 145,734/hash `00000000002eaba2ff41604d0126d09e142f6f2afb79ee12abf9ad818e677abf`, passed minimum chainwork, then an independent ordinary data directory executed block 1/hash `0000000012982b6d5f621229286b880e909984df669c2afabb102ce311b13f28` and stopped exactly. Its cold restart opened chainstate in 34 ms, requested no block, and exited in 3.52 seconds at the same tips. Mainnet activation used the same execution stack to validate genesis through active height 959,592 and cold-reopen without requesting a block. Ordinary stores retain restart-safe undo only for the freezer's retrievable block window; fixed-target experimental validators instead use a non-reorganizing delta journal and cannot expose APIs. A real default-Signet fixture exercises handshake, header download, witness-block retrieval, BIP325 validation, atomic UTXO/undo/tip persistence, pruning, and ledger publication. A wrong custom challenge is rejected without UTXO, undo, or tip residue.

Consensus script regression tests vendor Bitcoin Core 26's complete transaction and script JSON files byte-for-byte as immutable historical evidence, parse Core's script-assembly syntax, and use the same rBTC adapter as block connection. The transaction corpus executes all 119 valid cases plus the 70 invalid cases expressible through public `libbitcoinconsensus` flags; the script corpus parses all 1,207 tests and executes the public-flag subset of 148 expected passes and 82 expected failures, including 62 witness cases. The harness separately accounts for 9 `BADTX` structure cases, 14 transaction policy cases, and 977 script policy cases instead of conflating them with the public consensus API. Constructed Taproot cases additionally exercise Core's spent-output ABI, commitment proof, and tapscript result. Ten raw historical mainnet blocks cover both BIP30 repeats, the BIP16 exception, BIP34/BIP66/BIP65/CSV/SegWit/Taproot activation, and the pre-activation Taproot exception; their committed hashes make the raw bytes self-authenticating, and the larger fixtures are zstd-compressed. Three additional real spends cover SegWit activation, an exact 144-block BIP68 boundary, and the first mainnet Taproot key-path spend. Their spending and previous transactions are authenticated against proof-of-work-valid block headers with transaction-position Merkle branches, and UTXO amounts/scripts are derived from the raw previous transactions. Damaged SegWit and Taproot witnesses are rejected without durable input loss, including after reopening the UTXO database. Five raw blocks—BIP65, CSV, SegWit, Taproot activation, and the Taproot exception—execute all 8,997 transactions against complete minimal external-outpoint UTXO views containing 23,331 inputs. Real heights are retained for every active BIP68 input; successful transaction undo exactly restores each starting view, a late script failure rolls the whole block back, and a completed activation state survives redb reopen. Thirteen pinned mainnet/testnet anchors cover every buried activation and script exception. Explicit opt-in tests require an exact Core 31.0 regtest daemon and submit identical constructed blocks to `submitblock` and rBTC's production `HeaderDag`/`connect_active_block` path. They cover consecutive valid blocks, twelve structural/contextual rejection classes, configured BIP34/BIP66/BIP65/BIP141/BIP147 boundaries, and 102-block CSV boundaries for height-relative locks, time-relative locks, and BIP113 absolute lock time, while verifying a rejected candidate cannot advance or leave durable chainstate. The block transition also bounds cumulative transaction fees with Core's `MoneyRange` rule and rolls back every applied transaction when the bound fails. Historically exact coinbase-origin metadata and production active-header-chain connection at these heights are covered. The dependency decision, Core 27–31 classification, official artifact hash, and seven accepted live matrices are recorded in `CORE31_COMPATIBILITY.md`.

Candidate deployment context also carries the network-derived proof-of-work subsidy. Bitcoin, testnet, and signet use the 210,000-block halving interval; Core-compatible regtest uses 150. The block validator receives the already-selected subsidy explicitly so a test-network interval cannot silently fall back to mainnet rules.

Minimum chainwork is kept outside consensus validity. A lower-work chain can still have every header and block validated and persisted, but the daemon remains in IBD and a bounded `--once` run returns failure rather than claiming synchronization. Defaults match the pinned Core 31 Bitcoin, legacy-testnet, Testnet4, and Signet constants; regtest remains trust-free. Assume-valid configuration is parsed and its hash must appear on the active header chain before it is reported. It does not currently disable script checks: doing that safely requires retaining the skipped validation inputs and completing background verification before pruning or declaring independent validity.

For torrent distribution, the `ArchiveManifest` has stable 4 MiB compressed-piece SHA-256s. A future transport adapter can map them directly to torrent v2 pieces or verify webseed/range downloads before decompression. It must validate each recovered block through the normal chain validator; archive checksums are not consensus proof.

Network-adjusted time is instance-owned and influences only the contextual
future-header bound. Every successful full-service outbound handshake may add
one version timestamp keyed by IPv4 `/16` or IPv6 `/32`; duplicate groups
cannot reweight the median, and storage stops at 200 groups. Fewer than five
groups or a median beyond 70 minutes leaves the offset at zero. Active sync,
AssumeUTXO validation, and standby header probes read the same bounded median;
separate embedded nodes never share samples or mutable clock state.

Every post-handshake frame is resource-checked before application routing, including messages injected while a header, block, address, or pong response is pending. `inv`, `getdata`, and `notfound` vectors are capped at Core's 50,000 entries, remote `getheaders`/`getblocks` locators at 101 hashes, headers at 2,000, and legacy/BIP155 address vectors at 1,000. The 64-entry/4 MiB transaction announcement FIFO has a companion bounded hash index: legacy txid announcements index both ordinary and witness-aware request aliases, wtxid entries stay namespace-separated, and every insertion/replacement/eviction rebuilds at most 128 keys. A maximal `getdata` therefore performs expected constant-time cache lookups instead of 50,000 linear scans of the FIFO. Peers at protocol 70012 or newer receive BIP130 `sendheaders` after handshake; older peers are left unchanged. The active health probe ignores stale pongs, requires its nonce-matched response within the shared 32-frame total budget, answers crossed inbound pings without resetting that budget, and retains non-pong application messages in wire order for the next response consumer. Those retained messages share a second 4,000,000-byte aggregate wire-payload ceiling, measured from the authenticated frame lengths without reserializing decoded objects; dequeuing releases the exact charge. Thus a header announcement arriving before the expected pong feeds the following sync pass instead of being discarded, while several individually legal large messages cannot turn the 32-frame budget into roughly 124 MB of queued payload. The ordinary caught-up loop performs this probe after its 30-second idle interval before issuing the next `getheaders`; the one-second background-validation scheduler relies on its immediate `getheaders` instead of adding a ping every second. Objective per-message overages enter the protocol-violation path; aggregate queue exhaustion and a missing matching pong remain transient failures that advance normal peer failover without durable discouragement.

The session's outbound transaction primitives reject coinbase payloads and anything above Core's 400,000-weight-unit standard transaction limit before allocating a wire frame. The authenticated wallet broadcast route finalizes and consensus-verifies the submitted externally signed PSBT, then independently applies bounded wallet-origin relay policy: versions 1–3, standard size/weight/script templates, push-only bounded scriptSigs, aggregate bounded data carriers, dust, and Core 31's 100 sat/kvB fee floor. Only then does it reserve one slot in the eight-entry handoff channel, commit the exact transaction to `rebroadcast.redb`, and publish the request. Thus policy rejection is distinct from consensus failure, a full channel never creates a row, and a crash after persistence is recovered without depending on the HTTP caller. The store is owner-only on Unix, rejects symlinks and network mismatch, validates every bounded row on open, retains at most 64 unique wtxids, enforces a persistent no-replacement input-conflict rule, expires rows after 14 days, and schedules delivered transactions no more often than every 12 hours. Confirmed and noncanonical rows remain retained but ineligible; reconciliation against BDK's active canonical chain and current wallet UTXOs makes them eligible again after a reorganization. A failed active-peer write leaves the durable schedule unchanged and advances normal failover. After a complete write and atomic attempt update, the daemon publishes the same transaction to the separate eight-entry standby ring. Every hot standby owns an independent cursor and bounded relay exchange, so diffusion latency is not serialized into the API response; lag, timeout, or socket failure removes that standby. A successful call proves active socket delivery rather than peer mempool acceptance or acknowledgement.

The process owns one eight-slot standby transaction ring whether or not the wallet API is enabled. Every pending connection subscribes before handshake completion; the activated source peer is removed from the subscriber set. A wallet transaction enters the ring only after its active socket write and durable attempt update. A peer-origin transaction enters only after the complete candidate pool has passed admission and its redb snapshot has committed; accepted transactions displaced by a later package in the same drain are filtered out. Each hot standby independently announces the transaction under the peer timeout: negotiated BIP339 sessions use wtxid inventory, legacy sessions use txid inventory and accept witness-aware requests. Protocol-70013-or-later sessions retain the latest valid BIP133 `feefilter`; negative or above-`MoneyRange` values are ignored. Pool relay events carry exact fees and sigop-adjusted policy vsize, and inventory whose floor-rounded sat/kvB fee falls below that session's filter is omitted. Equality is announced, the nonce-matched ping still bounds and completes a suppressed exchange, and unknown-fee legacy wallet rows bypass filtering. Active wallet delivery remains an explicit `tx` write and is therefore outside the inventory filter. The same nonce-matched ping response loop serves matching `getdata`, emits `notfound` for unknown entries, preserves crossed application frames, and lets uninterested peers complete normally. Each session retains at most 64 announced transactions/4 MB and suppresses duplicate inventory while retained. Transaction inventory received in the other direction is separated from unrelated application frames, deduplicated, and capped at 64 entries; wtxid references are accepted only after reciprocal negotiation. Active and standby queues drain into a process-shared, memory-only tracker capped at 64 announcements per source and 1,024 overall. At a caught-up, minimum-chainwork tip, known pool/orphan/wallet and recent-confirmed/rejected identifiers are forgotten before the active source selects its ready candidates in announcement order. Only one source may have a hash in flight during Core's 60-second retry interval. The transport reports exact identifiers resolved by `tx` and `notfound`; a missing response completes only that source, while timeout, task cancellation, or disconnect removes it and makes an alternate standby source requestable after activation. Matching `tx` frames enter the existing pending FIFO, admission forgets both transaction identifiers, crossed messages remain ordered, and an aggregate 4 MB transaction-payload limit plus one frame per request and 32 extra frames bounds the exchange. The existing 50,000-entry per-message limit is enforced before capture. Ring lag, response timeout, aggregate exhaustion, or socket failure removes that peer through transient failover rather than objective discouragement. This is bounded relay over already connected outbound peers, not an inbound listener, a complete Core relay/mempool lifecycle, peer-mempool acknowledgement, or a propagation guarantee.

BIP35 request service uses a separate read-only view rather than the rebroadcast schedule. Every full-validation active or hot-standby session owns a cloneable callback into the process-shared admission pool and invokes it only after receiving `mempool` from a protocol-60002-or-later peer. The callback clones the current validated entries under the pool mutex and releases it before any socket write. The session then applies its latest valid BIP133 sat/kvB filter, selects txid or wtxid inventory from reciprocal BIP339 state, and retains the selected transactions in the existing 64-entry/4 MB relay cache before writing one bounded `inv`. A later `getdata` therefore uses the same payloads that were announced. Re-evaluating the callback for every request makes confirmation, replacement, reorg recovery, expiration, and capacity eviction immediately visible without a second mutable snapshot. Header-only sessions have no callback, and empty or obsolete requests are consumed without inventory. The optional inbound service exposes the same bounded validated pool through its independent request budget; the outbound callback remains isolated from listener task lifetime.

The optional inbound listener is process-scoped rather than owned by one active
outbound peer. It completes the shared bounded v1 handshake, advertises only
`NETWORK_LIMITED|WITNESS` plus `COMPACT_FILTERS` when that index is enabled,
and holds a replaceable read-only trait object. A validating session installs
that view only after chainstate, freezer, explorer, wallet, and optional-index
reconciliation; dropping the session lease removes it without rebinding the
socket. Incoming handshakes fail closed while no view is ready. This lets
outbound failover reopen its databases without leaving stale consensus state
reachable and prevents the separate AssumeUTXO genesis validator from opening
the active chain's public port. Active hashes come from the O(1) maximum-work
height map and are clipped at the execution tip. Block payloads must then exist
in the freezer and decode to the exact requested hash. Reachability is never
inferred from a wildcard listener. An
optional external socket is validated against the selected network before any
dial or bind, announced once on each outbound session, and placed first in
inbound `getaddr` samples with the exact `NETWORK_LIMITED`, witness, and
compact-filter service bits. Global/per-IP connection admission, a 20-minute
idle limit, per-peer minute work accounting,
bounded vectors, a rolling historical-upload target, and a recent-288-block
exception contain socket work. BIP157 replies come only from the independently
reconciled basic-filter index. Peer transactions are requested before intake
and enter a separate 64-entry/4 MB FIFO that the active session drains through
the ordinary durable admission pipeline.

Peer-origin rebroadcast state lives in the same `mempool.redb` transaction as the active pool snapshot. It is a strict versioned map from active txid to the last successful standby-ring publication time; unknown or duplicate rows make store opening fail, and pool replacement prunes rows whose transaction disappeared. Missing metadata from an older store is treated as an empty schedule. At a caught-up, minimum-chainwork tip, the daemon selects at most eight missing-or-12-hour-old entries in parent-before-child order. It records only publications for which the ring had at least one receiver, preserving retry eligibility when no hot standby exists. The timestamp therefore represents a bounded local diffusion attempt, not remote delivery or acknowledgement.

The peer-admission pool's network-bound, owner-only `mempool.redb` contains strict versioned parent-before-child snapshots. Its conservative default remains 64 transactions/4 MB; validated operator ceilings may raise it to 300,000 transactions/1 GiB, and the same instance-scoped budget governs memory admission, reorg recovery, durable encoding, relay/admission metadata, and reopen parsing. Admission or reconciliation commits the replacement active snapshot before publishing the cloned in-memory pool, so storage failure preserves the previous live view and a crash after commit is recovered on restart. Before each stale active block is disconnected, its hash is checked against the retained ledger and its non-coinbase transactions are added to a separately committed bounded recovery snapshot; lower-height parents displace higher descendants under pressure. This closes the mid-reorg process-failure window. Once the replacement chain catches up, active, recovered, and newly received transactions share the same admission pass, and publishing the resulting pool clears the recovery snapshot in the same redb transaction. Reopen rejects malformed versions, counts, lengths, transaction encodings, duplicates, conflicts, child-first dependencies, symlinks, permissive Unix modes, and network mismatch. Persisted transactions remain untrusted: after the active execution/header tips agree above minimum chainwork, each is rerun through current consensus, script, standardness, fee, conflict, and package checks before stale rows are removed. Core 31 full-RBF is the default; `--no-mempool-full-rbf` restores inherited BIP125 signaling as an additional predicate. Descendant closure, the 100-entry work limit, unrelated-unconfirmed-input exclusion, higher aggregate fee, and incremental relay fee remain mandatory.

Candidate admission evaluates Core 31 graph policy after replacement removal and package insertion. Every dependency-connected cluster is capped at 64 entries/101,000 policy vB; version-3 TRUC entries additionally require version-homogeneous unconfirmed links, at most one parent plus one child, 10,000 vB per transaction, and 1,000 vB for a child. The superseded ancestor/descendant limits and CPFP carve-out are not part of the production path. Before prevout lookup, admission rejects more than 2,500 context-free legacy sigops. Once consensus supplies the expanded sigop cost, admission rejects values above 16,000, stores policy vsize `ceil(max(weight, sigop_cost × 20) / 4)`, and uses it for package/cluster ceilings, minimum and rolling fee floors, replacement incremental fee, and eviction fee rates. The initial raw-vsize package check still rejects oversized untrusted input before prevout work; the second check prevents high-sigop packages from bypassing the same 101,000-vB boundary. Since the live pool is replaced only after all topology, fee, and capacity checks succeed, failures cannot leak partial index or eviction changes. Standard outputs require push-only `OP_RETURN` tails under a 100,000-byte aggregate data-carrier ceiling and include valid 1-to-3-key bare multisig while rejecting four-key or malformed-key creation. Before the private overlay spends any coin, admission snapshots every prevout script and applies prevout-dependent standardness: recognized spent-output templates, including historically mined valid bare multisig through the script solver's 16-key ceiling, full push-only P2SH `scriptSig` stack evaluation followed by accurate redeem-script sigops capped at 15, witness data only for witness programs, P2WSH script/stack/item ceilings of 3,600 bytes/100/80 bytes, native-Taproot annex rejection, the 80-byte tapscript argument ceiling, and policy flags that discourage P2SH-wrapped upgradable witness programs, future Taproot leaf versions, and tapscript `OP_SUCCESS` opcodes. Consensus execution still runs first; either policy failure discards the private overlay. The pinned public `libbitcoinconsensus` ABI exposes consensus flags only, so execution-dependent standard flags remain explicit policy work rather than being misrepresented as consensus coverage.

`mempool.redb` additionally carries a strict complete txid-to-first-admission-time map. At each caught-up, minimum-chainwork pass, entries older than Core's default 336 hours seed descendant-closure removal before ordinary active-chain reconciliation. The candidate pool, active snapshot, cleared reorg-recovery snapshot, pruned relay attempts, and admission-time map are then published together. Surviving rows retain their original age; new rows receive the pass time; an expired row independently readmitted in that pass is explicitly reset. A crash before the commit retries the old pool, while a crash after it reopens the complete new state. Stores predating the map migrate current active entries at the migration time rather than immediately expiring them.

Missing-input handling is isolated from the durable mempool snapshot. The cloned process-shared admission pool carries an orphanage capped independently at 64 witness transactions and 4 MB; each entry must also fit the 400,000-weight-unit standard transaction ceiling and expires after 20 minutes. Pressure removes a random current entry rather than exposing a predictable FIFO victim. Only transactions received from the active peer may enter it: missing persisted-pool or reorg-recovery candidates are removed through their durable reconciliation path and are never attributed to that peer. Missing parent txids absent from the submitted package, active pool, orphanage, recent-confirmed set, and exact confirmed UTXO view enter a globally deduplicated, source-keyed 64-entry request set. Requests deliberately remain txid inventory after BIP339, and responses rejoin the ordinary pending queue. Each atomically connected block contributes txid and distinct wtxid identifiers to an exact oldest-first set capped at Core 26's 48,000-entry rolling-filter capacity; transaction-inventory and parent selection both consult it, while any successful active-chain disconnection clears the complete set before recovery admission. An exact-outpoint-to-orphan index both applies Core's block cleanup after atomic chainstate connection and selects children that spend an actual output index of each newly admitted parent. Selected txids enter a deduplicated `source -> work set` map. One work item is popped and passed through the unchanged atomic consensus, standardness, replacement, graph, and capacity path per admission turn; if more source work remains, the caught-up loop commits the result, yields to the runtime, and invokes another turn immediately. An accepted child schedules its own exact-output children for later turns, a still-missing attempt remains stored but unscheduled until another parent arrives, and terminal rejection removes it. A 1,024-entry exact-txid recent-reject cache remembers only witness-independent failures, resets on an active-tip change, and prevents retention or another blind request when an orphan depends on a rejected parent. Every insert, expiry, random eviction, block conflict, terminal rejection, successful admission, and source cleanup rebuilds exact byte accounting and prunes the exact index, work sets, and request state to live matching-source txids. Each established `PeerSession` receives an atomic process-local monotonic ID; this untrusted-peer-independent value, not the remote version nonce, owns orphan, work, and request records. `run_connected_peer` removes exactly that source's remaining state on both normal completion and failure before failover. The orphanage, recent-confirmed/reject caches, requests, and work scheduler deliberately start empty after restart.

The in-memory admission pool also owns a rolling minimum fee in sat/kvB. Oldest-first capacity eviction computes the removed descendant closure's aggregate fee/vsize, adds Core 31's 100 sat/kvB incremental relay rate, and raises—but never lowers—the current bump. Ordinary entries pay the 100 sat/kvB min-relay floor on policy vsize. Only a bounded one-parent-one-child non-replacement submission may carry a below-floor parent, including zero fee, when its aggregate fee pays the effective rolling minimum; arbitrary chains, partial submissions, and replacements remain individually priced. Any failure discards the candidate. A changed caught-up execution-tip hash starts Core's 12-hour exponential decay; serialized occupancy below one-half or one-quarter of the hard byte ceiling shortens the half-life by two or four, and a value below 50 sat/kvB becomes zero. Reconciliation temporarily ignores this admission-only gate so an existing low-fee survivor is not retroactively removed. This runtime pressure signal is not part of `mempool.redb` and therefore resets after restart.

Fee estimation is isolated in network-bound `fee_estimates.redb` rather than overloading the admission snapshot. Its pending map holds exact fee, sigop-adjusted policy vsize, and first eligible height for every active local-pool transaction, preserving existing first-seen values during restart reconciliation. At a caught-up execution tip, the daemon walks retained active blocks from the estimator tip before mempool reconciliation, moves matched pending entries into a consecutive block journal, and persists the new tip atomically with those observations. Up to 1,008 blocks and 4,096 confirmations are retained. Disconnecting a retained tip restores its exact pending entries; a deeper reorganization or unavailable ledger prefix clears and reanchors the estimator explicitly. The estimator evaluates confirmed delays together with pending transactions old enough to have missed the target, requires three mature outcomes, and chooses the lowest observed sat/kvB threshold reaching an 85% success ratio before rounding upward to whole sat/vB. Authenticated `estimatesmartfee` exposes targets 1–1,008 in Core's BTC/kvB response units and returns an explicit insufficient-data result. Unsigned-PSBT requests choose either an exact 1–1,000 sat/vB rate or a 1–1,008-block target; target resolution must remain inside the wallet fee bound, consumes no fallback, and is cleared before the lower-level wallet constructor runs. Its strict snapshot decoder joins bounded persisted-metadata fuzzing. This fee method is the sole RPC read outside the explorer index; Core's multi-timescale bucket decay, smart-fee modes, and richer wallet fee strategies remain future parity work.

The standard-script interpreter pass is deliberately layered after active-consensus execution. If a custom deployment context omits any public Core 26 mandatory flag, admission replays the already-collected prevouts through `libbitcoinconsensus` with its complete public flag set and reports failure as standard policy, not block consensus. When all seven public flags are already active, the second pass is skipped. A pinned Core transaction valid under `VERIFY_NONE` but invalid under DERSIG/NULLDUMMY proves the pre-activation boundary and candidate-overlay rollback. The same layering applies Core's `STANDARD_LOCKTIME_VERIFY_FLAGS`: when CSV is inactive for candidate blocks, the retained exact prevouts are rechecked read-only with BIP68 height/time semantics before pool publication. Strict equality fails for both block-height and 512-second MTP locks, and those txid-stable failures are eligible for the bounded recent-reject cache.

Package rolling-fee checks implement Core 31's bounded one-parent-one-child fee-bumping exception without partially publishing intermediate successes. A parent may fall below the ordinary min-relay floor only when it and its direct child together meet the rolling rate over exact sigop-adjusted policy vsize. Parent dependency depth, partial calls, and replacements disable aggregation; the cloned pool preserves all-or-nothing persistence.

Context-free package identity is established from every submitted txid before consulting the active pool, so duplicate submitted witness variants fail atomically. After that check, an active entry with the same txid substitutes its already-validated witness transaction and the submitted variant is counted as present; children resolve the admitted parent's outputs without validating or overwriting the alternate witness. The multi-transaction package ceiling is measured once over the raw aggregate weight and accepts at most 404,000 weight units. It is not a sum of independently rounded virtual sizes, and singletons remain governed by the later 400,000-weight-unit standard transaction limit rather than this package-only bound.

Replacement planning retains the fee and sigop-adjusted policy vsize of every direct conflict before removing the complete descendant closure from the private candidate. Core 26's BIP125 Rule #6 then compares floor-truncated integer sat/kvB rates: the replacement transaction, or the aggregate replacement package supported by rBTC, must strictly exceed each direct conflict independently. This precedes the existing total-conflict-fee and incremental-bandwidth checks, so paying more than the complete removed set cannot compensate for lowering a high-rate direct conflict. Rule #5's work bound is intentionally more conservative than the eventual removal set: each direct conflict's descendant closure contributes separately to the 100-entry potential-replacement count before shared descendants are deduplicated. A valid 25-entry graph with five conflicts sharing twenty descendants therefore counts as 105 and fails before candidate mutation. Candidate cloning preserves atomic rollback, and full-RBF alters only the signaling predicate.

BIP125 Rule #2 records the txids of every parent used by a direct conflict, matching Core's `HasNoNewUnconfirmed` boundary. A candidate input whose parent is still in the mempool is allowed when that parent txid was already represented, even if the candidate selects another output index; a mempool parent absent from that set is rejected as a new unconfirmed dependency. Tests retain the parent while replacing a child across sibling outputs and separately prove that an unrelated parent still fails without pool mutation.

## Explorer and wallet

The explorer projection starts only with an explicit loopback API listener. A validator without a listener neither opens nor advances `explorer.redb`, avoiding an unused full historical transaction/undo index.

The watch-only finalizer is deliberately distinct from in-process wallet signing. It accepts only a strict 768 KiB JSON wrapper around a PSBT no larger than 512 KiB, requires 1–100 unique current-wallet inputs and at most 17 outputs, and rejects full previous transactions, pre-finalized inputs, unsafe sighash modes, mismatched `witness_utxo` data, excessive fees, and spent or foreign outpoints. BDK constructs final scripts from externally supplied partial signatures; rBTC then extracts the transaction and verifies every input with the pinned Bitcoin Core 26 consensus engine against local validated prevouts. The finalize response includes the finalized PSBT and raw transaction identity without network mutation; the separate broadcast route repeats that process, creates only the bounded wallet-origin admission/rebroadcast record described above, waits for an active-peer socket write, and then fans out to hot standbys. It does not provide a general peer-facing mempool. References below to signing/finalization remaining open mean the secret-bearing in-process workflow, not this external-signature verifier.

The explorer is embedded but logically read-only: a redb projection atomically stores active block summaries, transaction confirmations, script-hash keyed current UTXOs, and per-block rollback data. Its durable tip is reconciled against the execution tip on startup; missing projections are replayed from the retained ledger or fetched from a full-history peer, while stale projections use their independent undo records. The Axum router and CSP-constrained static page read this index without exposing chainstate mutation; the daemon accepts only explicit loopback listener addresses. Address UTXO queries validate the selected network before storage access and apply capped offset/limit pagination in the redb range iterator, so malformed input returns 400 and cannot materialize an unbounded result. A bounded SSE fan-out publishes connected, disconnected, and snapshot-rebased tips only after the corresponding explorer transaction commits. Each subscriber begins with the latest durable snapshot. The shared ring retains 128 transitions, at most 64 streams may remain open, 15-second keepalive comments detect dead intermediaries, and a lagged receiver gets an explicit `resync` event instead of a false contiguous history claim. The embedded page consumes this feed for its live-tip display. Liveness remains a cheap static route, while readiness compares complete height/hash pairs for headers, execution, explorer, and the optional wallet plus the minimum-chainwork gate. Dynamic status additionally exposes full-script/AssumeUTXO trust state, hot/cold UTXO counts, and live circular-ledger segment/block/compressed-byte bounds. Prometheus exposition uses those same bounded index reads and never scans archive payloads. IBD, block lag, or projection lag returns readiness 503, and all changing observability responses forbid caching. The optional `/rpc` route is strict JSON-RPC 2.0 with a 64 KiB body ceiling and rejects batches, notifications, unknown envelope fields, and unbounded IDs/methods/pages. In addition to bounded explorer and fee reads, it exposes locally proven blockchain, network, active-peer, mempool, index, and consistency status plus delayed idempotent shutdown; the response surface is stable but deliberately not exact Core field parity. Its owner-only hot-reloaded bearer credential is distinct from the wallet credential; both use non-short-circuit comparison and share a single synchronized authorization audit boundary. The wallet is BDK descriptor-based and runs in-process, which avoids putting keys in a browser. Its watch-only state uses BDK's transactional SQLite persister under one process mutex: separate issuance cursors are durably reserved before a receive address is returned or a change script is used, and reopen checks both descriptors and the network. This permits bounded unsigned PSBT construction without address reuse even if a PSBT is abandoned or the process restarts. Strict requests cap JSON at 32 KiB, recipients at 16, selected or automatically considered inputs at 100, fee rate at 1–1,000 sat/vB, serialized PSBT at 512 KiB, and apply network, dust, `MoneyRange`, exclusive coin-control, unsigned-only, and per-minute mutation gates. Creation requires SegWit or Taproot receive/change descriptors and stores only `witness_utxo` metadata, bounding construction memory without weakening legacy-input signing safety. BDK provides coin selection, RBF sequences, fee calculation, and BIP174 metadata; rBTC persists the reserved change key before returning the base64 PSBT, and the creation path never signs, finalizes, or broadcasts it. After the atomic chainstate commit and explorer update, each validated block advances the wallet checkpoint and relevant transaction graph in one SQLite commit. Startup finds the highest wallet checkpoint shared with the active execution chain, durably removes checkpoints above it, and replays missing executed blocks from the circular ledger or a full-history witness peer before binding the API. Descriptor import pre-reveals bounded receive/change windows, extends them from BDK's highest used indices, and replays from a configurable birthday until the gap window converges. Sparse validated checkpoints skip raw pre-birthday blocks; the earliest completed boundary is updated only after the scan succeeds, so restart, a lower birthday, or new lookahead cannot silently claim incomplete coverage. Scan work is capped at 64 extension passes per synchronization cycle. The stores intentionally do not claim cross-engine atomicity; this ordered, idempotent reconciliation closes each crash window. Authenticated status, balance, canonical newest-first transaction history, current UTXOs, and public descriptor export expose the resulting watch-only projection; confirmations follow BDK's active-chain canonicalization after reorg, and fees remain absent when input values are incomplete. Export serializes only the statically public receive/change descriptor types, including checksums, and its strict two-field object is directly re-importable with explicit default scan policy; network and nondefault scan choices remain deployment configuration. The database is owner-only on Unix and symlink paths are rejected. Secret descriptors are rejected rather than stored unencrypted. Wallet REST mounting is explicit and disabled by default: it requires owner-only descriptor and bearer-token files plus a loopback listener. Authentication uses a non-short-circuit token comparison and all wallet responses forbid caching. The daemon reloads both token files every second: atomic replacement rotates each scoped router state, and any permission, type, size, encoding, or token-grammar failure disables that authorization scope until a valid owner-only file returns. Before any protected handler runs, the router syncs an authorization record containing only time, method, query-free fixed path, and accepted/rejected state. The shared append-only audit file is owner-only, single-link, capped at 16 MiB, and excludes credentials, headers, query values, bodies, and responses; open, append, sync, or capacity failure returns 503 before protected state can be read or changed. Configuration is loaded before peer connection, while incompatible legacy chainstate is rejected before wallet creation. The bounded in-memory peer-transaction pool now provides atomic dependency packages and opt-in BIP125 replacement; persistence, full RBF, complete ancestor policy, and general transaction relay remain open gates alongside encrypted secrets and in-process signing.

Standalone diagnostics use bounded newline-delimited JSON records instead of unstructured progress output. Formatting is skipped below the active severity, admission is limited by both a fixed queue and a per-second budget, and messages have a UTF-8-safe size ceiling. A single writer owns the destination, so peer and validation workers never contend on filesystem writes. The default data-directory sink rotates by bytes to an exact retained-file bound, uses owner-only directory and file permissions, rejects symlink or non-regular destinations, and exposes dropped-record and write-error counters through authenticated RPC and Prometheus. Embedded nodes do not install this process-global sink; hosts consume typed `NodeEvent` values and retain control of their own diagnostics backend.
