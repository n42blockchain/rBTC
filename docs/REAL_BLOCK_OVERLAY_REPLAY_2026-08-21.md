# Real-block overlay replay: MDBX vs redb on mainnet 935,001–963,350 (2026-08-21)

Host: the Windows evidence machine that holds the immutable btcd block corpus
(`D:\btcd26\mainnet\blocks_mdbxdb`, 1,423 `.fdb` files, 762,923,466,164 bytes,
manifest SHA-256 `57ecd6465f3bd93b00bca45c79fbca9df721ed1d44566761f72b567759bdd6c4`,
last block 963,350), AMD Ryzen 9 9950X (16C/32T), 125.6 GiB RAM, Dell CM7 U.2
NVMe for source and destination. Raw artifacts, logs, 60-second resource samples,
tool binaries and `SHA256SUMS.txt` live in
`D:\rbtc-bench\rbtc-replay-20260821\artifacts` on that host; only this summary
is checked in.

## 1. Outcome of the btcdmdbx track (`BTCDMDBX_FULL_REPLAY_TASK.md`)

The Go lanes were prepared exactly as the task book specifies (measurement
patch `--json` with `BestState.TotalTxns` counts and a fast-add/full-validation
split, identical on both lanes; read-only `census` and `corpusmanifest` tools;
fixed parameters; serial lanes). Three lane B attempts on the candidate
`fa64adfa` all failed before completing, each when host memory was exhausted:

| attempt | toolchain | outcome |
|---|---|---|
| 1 | go1.26.5, default GC, beside ~41 GiB of unrelated processes | GC access violation at height ~579,914 after 12 min; working set 108 GiB, free RAM 1.8 GB |
| 2 | go1.26.5, `GOEXPERIMENT=nogreenteagc`, host otherwise idle | source read failed with `ERROR_NO_SYSTEM_RESOURCES` at height 796,851 after 25.6 min; working set 114.8 GiB |
| 3 | go1.27.0, default GC, all module deps updated (mdbx-go v0.43.0), `--memlimit` lowered 112→80 GiB | reached checkpoint 810,071 in **24m01s** (fast-add), then crashed at height ~821k at 40 min when a second replay started on the host and free RAM fell to ~1 GB |

The user closed the track with the figure "about 24 minutes from genesis to the
810,000 checkpoint with fast-add" (attempt 3's measured 24m01s; the developer's
own report was 24 min). No full-replay time, transaction count or durable TPS
was obtained, so those remain unmeasured. The task book's fixed
`--gogc=400 --memlimit=112` leave no headroom on a 125.6 GiB host once MDBX
mmap pages are counted; that is a property of the parameters, not of either
revision. Full-validation throughput above the checkpoint was ~13 blocks/s on
this host before the crash.

## 2. What the rBTC lanes measured

- **Base:** AssumeUTXO snapshot `utxo-935000.dat` (9,387,990,306 bytes,
  164,241,311 coins, block
  `0000000000000000000147034958af1652b2b91bba607beacc5e72a56f0fb5ee`); index
  rebuilt with the same `rbtcd` binary (`--build-core-snapshot-index`, 182 s,
  530,926,247 bytes).
- **Blocks:** a `PrunedBlockLedger` imported from the corpus by the new
  `fdb_ledger_import` tool: 963,351 records scanned, no stale forks, 28,350
  main-chain blocks 935,001–963,350 (42.15 GiB raw → 30,787,878,611 bytes
  compressed, 886 segments), then `verify_block_hashes` over the whole range.
  The window holds 116,470,186 transactions, 275,859,898 outputs (66,752,467
  of them OP_RETURN) and 207,553,666 inputs.
- **Run:** `rbtcd --once --snapshot-overlay-catchup … --snapshot-overlay-index …
  --snapshot-overlay-replay-blocks <ledger> --snapshot-overlay-engine mdbx|redb
  --snapshot-overlay-capacity-bytes 10737418240 --snapshot-overlay-compact-percent 50
  --snapshot-overlay-rebase-percent 85`. Every consensus rule including scripts
  (rBTC has no assume-valid script skip); headers from P2P, blocks only from
  the ledger; 256-block batches. Each lane ran alone on the host (the mdbx lane
  overlapped for ~5 minutes with an idle-priority single-thread ledger scan).

## 3. Results

| metric | MDBX overlay | redb overlay | redb/MDBX |
|---|---|---|---|
| exit code / final tip | 0 / 963,350 `…d66ec69a8f5e611a` | 0 / same | — |
| catch-up (chainstate open → tip) | 3,823 s (1h03m43s) | **3,289 s (0h54m49s)** | 0.86 |
| process wall incl. header sync | 4,017 s | 3,421 s | 0.85 |
| blocks/s · tx/s | 7.41 · 30,462 | **8.62 · 35,415** | 1.16 |
| sum of batch time | 3,569 s | 3,100 s | 0.87 |
| execute (sum) | 3,252 s | 2,802 s | 0.86 |
| core-validate (sum) | 574 s | 503 s | 0.88 |
| utxo-prefetch (sum) | 327 s | 228 s | 0.70 |
| core-script-wait (sum) | 9 s | 8 s | — |
| core-commit (sum / median per batch) | 1,960 s / 13.1 s | 1,762 s / 15.7 s | 0.90 |
| MDBX commit detail (sum) | mutate 898 s (of which snapshot base-lookup 485 s), sync 832 s, fold 110 s, undo 116 s | not reported by the redb engine | — |
| compaction events | 19 compact-copies | 2 compactions | — |
| time outside batches | 254 s | 189 s | — |
| peak working set | 10.33 GiB | **6.00 GiB** | 0.58 |
| process CPU seconds | 16,793 | 16,394 | 0.98 |
| bytes written to the destination volume | 480.4 GB (4.3 GB per batch) | **299.4 GB (2.7 GB per batch)** | 0.62 |
| bytes read from the destination volume | 1,303 GB (11.7 GB per batch) | 30.7 GB | 0.02 |
| final overlay file | 6.0 GiB (after the 19th copy) | 6.7 GiB | — |

Interpretation:

- Under identical work, **redb finished 14% sooner, used 42% less memory and
  wrote 38% fewer bytes**. The MDBX lane's copy-on-write B-tree rewrote
  roughly 4.3 GB per 256-block batch against ~100 MB of net change, and the
  write rate rose from 1.5 GB/min to 11 GB/min as the overlay grew; it also
  triggered 19 compact-copies at the 50% threshold. This reverses the direction
  of the 2M-coin Mac micro-benchmark (MDBX 3.6× faster), which measured a
  memory-resident read/write mix rather than the write-and-compaction
  lifecycle of real churn.
- The MDBX lane's 1.3 TB of reads are not ledger reads (the redb lane read
  31 GB for the same ledger); they are page-ins of the memory-mapped overlay
  plus snapshot reads through the MPHF base index. The split between the two
  was not measured.
- Commit is 55–57% of batch time on both engines; script verification waits
  total under 10 s per lane because it runs in parallel behind the serial
  commit.

## 4. Spent-output age over the same window (`utxo_locality`)

| age of the spent output | share of inputs | cumulative |
|---|---|---|
| created in the same block | 33.50% | 33.50% |
| ≤ 256 blocks (one batch) | 47.21% | 80.71% |
| ≤ 1,008 blocks | 6.68% | 87.39% |
| ≤ 4,096 blocks | 4.13% | 91.52% |
| ≤ 28,350 blocks (window) | 2.22% | 93.74% |
| older than the window | 6.26% | 100.00% |

Four in five spends consume an output younger than one commit batch, and the
existing `commit-fold` only cancels pairs inside a single batch. A write-back
layer spanning several batches would keep most coins from ever reaching the
durable store: per batch roughly 1.9M non-OP_RETURN outputs are created and
1.87M spent, of which ~1.5M are younger than 256 blocks, leaving ~0.4M net
inserts plus ~0.36M old-coin deletes to persist — about one fifth of today's
write traffic. This window is a 2025–26 Ordinals/Runes-era sample; the
2015–2019 distribution should be measured before sizing a cache for genesis IBD.

## 5. Conclusions recorded for the gate

1. Acceptance item 4 (real-block replay through redb and MDBX with identical
   settings) has been run on the Windows host: same final tip, full
   validation, identical batch/capacity/compaction/rebase settings, and —
   with the new read-only `overlay_audit` tool — the same canonical UTXO
   content. All four overlays (MDBX, redb, and both with the write-back
   layer) hold 14,554,294 post-base coins (523,954,584 key bytes,
   798,755,918 value bytes) and 13,000,529 tombstones over the same base
   (`hash_serialized e4b90ef9…025050`) at tip 963,350; their consensus-field
   digest is identical:
   `content_sha256 = aadd289f6edf154e55aec63c9b4c22cd46e2d7836dc55382d0036523247f2819`
   (overlay `1bc8b65d…f61f72`, tombstones `dafe85f5…f05f6`). The tombstone
   count equals the 13,000,529 inputs the locality scan classified as
   spending pre-window coins. Only the raw value bytes differ between lanes,
   because the stored `last_touched` wall-clock field differs per run; the
   audit hashes value, height, coinbase flag, creation MTP and script, and
   reports the raw digest separately. Undo rows (958 / 2,494 / 958 / 3,774)
   depend on when the last prune ran and are excluded from the digest.
2. On this evidence MDBX is not faster than redb for the catch-up write path;
   its advantage in the earlier micro-benchmarks does not carry over to the
   compaction lifecycle. With the write-back layer (section 6) both engines
   see a fifth of the write traffic and redb remains ahead (2,606 s vs
   3,057 s); with the fingerprint sidecar as well (section 7) the engines
   are within 4% (MDBX 2,314 s, redb 2,407 s) and the catch-up is no longer
   bounded by either engine's write path. Nothing measured here argues for
   switching the daemon default to MDBX on performance grounds; the gate's
   remaining items (160M synthetic run, migration surface) stand.
3. A btcd-style fast-add (skip scripts and spend checks below a checkpoint) is
   not wanted. A Core-style assume-valid script skip would save little wall
   clock on 32 threads (script waits ≈ 0.2% of batch time) and is a
   consensus-adjacent change with its own prerequisites
   (`ARCHITECTURE.md`, "Assume-valid configuration …"); re-evaluate only for
   ≤ 8-core targets or if a genesis-IBD profile shows validation waits above
   30% of wall clock.
4. The measured order of work is: cross-batch write-back UTXO cache first
   (engine-agnostic, sized by section 4), then re-measure the commit split;
   consider an append-only overlay folded periodically into the base only if
   sync/mutate still dominate afterwards.

## 6. Write-back layer: the recommended step, measured the same day

`WriteBackChainstate` (`src/write_back_chainstate.rs`, commit a1ed228) buffers
the validated transitions of the last N batches in front of either engine,
serves reads from the buffer first, and hands the engine one batch whose
existing fold cancels every coin created and spent inside the window
(`--snapshot-overlay-flush-batches N`, default 1 = unchanged engine). Same
ledger, snapshot, index and overlay settings as section 3; N = 16; both lanes
ran alone on the host (a user-run Go replay variant overlapped parts of the
`mdbx-wb16` lane — see the caveat below).

| metric | MDBX | redb | **MDBX + write-back 16** | **redb + write-back 16** |
|---|---|---|---|---|
| catch-up (chainstate open → tip) | 3,823 s | 3,289 s | **3,057 s (0.80×)** | **2,606 s (0.68×)** |
| blocks/s · tx/s | 7.41 · 30,462 | 8.62 · 35,415 | 9.27 · 38,098 | **10.88 · 44,689** |
| core-commit (sum) | 1,960 s | 1,762 s | 967 s (0.49×) | 814 s (0.42×) |
| MDBX mutate / sync / base-lookup (sum) | 898 / 832 / 485 s | — | 314 / 163 / 233 s | — |
| flushes · time in flushes | — | — | 8 · 842 s | 7 · 644 s |
| coins written · spends written | — | — | 26.3M · 24.8M | 25.7M · 24.2M |
| coins cancelled in memory | 0 (intra-batch fold only) | 0 | **113.3M (81.1% of created)** | **113.9M (81.6%)** |
| core-validate · utxo-prefetch (sum) | 574 · 327 s | 503 · 228 s | 567 · 448 s | 529 · 264 s |
| compactions | 19 | 2 | 4 | 5 |
| peak working set | 10.3 GiB | 6.0 GiB | 23.4 GiB | 17.9 GiB |
| bytes written to the destination volume | 480 GB | 299 GB | not attributable (see caveat) | **120 GB** |
| final tip | 963,350 `…d66ec69a8f5e611a` | same | same | same |

- The locality prediction held exactly: 81% of created coins never reached
  either engine. The engines' own commit work fell by half or more; what is
  left is validation (≈ 530–570 s), the snapshot-index reads behind
  `utxo-prefetch` and the flush's duplicate-creation probe against the base
  (233 s on MDBX), and the flushes themselves.
- redb with the write-back layer is the fastest configuration measured:
  32% less catch-up time than the MDBX baseline and 21% less than the redb
  baseline, writing a quarter of the baseline's bytes. MDBX with the layer
  beats both baselines but still trails redb + layer by 15%.
- Cost: peak working set rises by 12–13 GiB at the default
  `--snapshot-overlay-flush-coins 8000000` (buffered coins, their undo
  pre-images and the larger engine transaction); the coin limit trades that
  memory for flush frequency.
- Caveat: a user-run Go replay variant (`D:\N42\replay\variants\rb_base.exe`,
  up to 63 GiB working set) was on the host during parts of the `mdbx-wb16`
  lane, so that lane's volume-level byte counts are not attributable and its
  timings may include some contention; `redb-wb16` was started by a waiter
  only after the host was idle and its samples show no other heavy process.
  The lane wrapper now refuses to start beside any `rb_*`, `replayblocks*`,
  `rbtcd*`, `n42-*` process or any process above 8 GiB working set.
- Flush counts differ (8 vs 7) because compaction flushes the buffer first
  and the two engines compact at different moments; the cancelled totals
  differ by the same mechanism.

## 7. Fingerprint sidecar: removing the base probes (2026-08-22)

What the write-back lanes left on the table was dominated by reads of the
base snapshot: the flush's duplicate-creation probe (`commit-base-lookup`)
and the absent-key share of `utxo-prefetch`. Each absent key cost a packed
offset-table read plus a 192-byte snapshot read at a random offset. The
index builder now also writes `<index>.fp` — one 16-bit txid fingerprint per
MPHF slot (commit 0b8b3ae; 113,879,165 slots, 227,758,412 bytes, built on
first open in 54 s for an index written before the sidecar existed) — and a
lookup whose slot fingerprint differs is answered absent without any read.
Same lanes as section 6 with `rbtcd-fp.exe` (write-back 16 + fingerprints +
the resume-path fix 3ba224d); both ran on an idle host.

| metric | MDBX + wb16 | **MDBX + wb16 + fp** | redb + wb16 | **redb + wb16 + fp** |
|---|---|---|---|---|
| catch-up (chainstate open → tip) | 3,057 s | **2,314 s (0.61× baseline)** | 2,606 s | **2,407 s (0.63× baseline)** |
| blocks/s · tx/s | 9.27 · 38,098 | **12.59 · 50,340** | 10.88 · 44,689 | 11.78 · 48,396 |
| core-commit (sum) | 967 s | **711 s** | 814 s | **709 s** |
| MDBX base-lookup / mutate / sync (sum) | 233 / 314 / 163 s | **72 / 135 / 141 s** | — | — |
| utxo-prefetch (sum) | 448 s | **149 s** | 264 s | 197 s |
| core-validate (sum) | 567 s | 560 s | 529 s | 531 s |
| time in flushes | 842 s | 560 s | 644 s | 518 s |
| coins cancelled in memory | 113.3M (81.1%) | 113.3M (81.1%) | 113.9M (81.6%) | 113.9M (81.6%) |
| peak working set | 23.4 GiB | 24.0 GiB | 17.9 GiB | 18.2 GiB |
| bytes written to the destination volume | n/a (shared host) | **93 GB** | 120 GB | 109 GB |
| final tip · overlay content digest | same · `aadd289f…` | same · `aadd289f…` | same · `aadd289f…` | same · `aadd289f…` |

- Fingerprints removed 161 s of base probes and 299 s of prefetch from the
  MDBX lane and 67 s of prefetch from the redb lane; the residual
  `commit-base-lookup` (72 s) is MPHF hashing of ~26M created keys plus the
  one-in-65,536 false matches, with no snapshot reads behind it.
- With both optimisations the engines are within 4% of each other
  (MDBX 2,314 s vs redb 2,407 s), the reverse of the baseline ordering;
  at this point the catch-up is bounded by validation (~530–560 s), the
  remaining member reads of the base, and the flushes themselves rather than
  by either engine's write path. All six overlays hash to the same consensus
  content digest (`overlay_audit`).
- The first fingerprint attempt (`mdbx-wb16-fp-attempt1`) failed at height
  961,624 after 37 min when its first peer — a Knots 29.3 node reporting
  tip 961,637 and "no more headers" — turned out to be on a stale branch and
  the ledger block at 961,632 did not match that header chain; the failover
  then hit a pre-existing resume bug that derived the index path from the
  CLI snapshot directory instead of `--snapshot-overlay-index` (fixed in
  3ba224d). That attempt's partial data was discarded; its first flush had
  already shown base-lookup 18.8 s vs 52.4 s without fingerprints on the same
  blocks.

## 8. Executor clone reduction and run-to-run variance (2026-08-22)

Commit 040d3bb makes the per-block and per-batch `UtxoOverlay` cache each
base read once (one clone and one insert instead of two), lets the executor
hand its transitions to the store by value so the write-back buffer stops
cloning every batch, and times the fold/transition step as `core-apply`.
Lanes `mdbx-wb16-v3` / `redb-wb16-v3` are the fingerprint lanes with that
binary; `mdbx-wb16-fp2` repeats `mdbx-wb16-fp` with the unchanged binary to
measure variance. All ran alone on the host.

| MDBX lane | catch-up | core-validate | core-apply | utxo-prefetch | core-commit | of which engine undo | buffered-batch commit mean |
|---|---|---|---|---|---|---|---|
| wb16 + fp (first run) | 2,314 s | 560 s | — | 149 s | 711 s | 86 s | 1,620 ms |
| wb16 + fp (repeat) | 2,434 s | 591 s | — | 158 s | 742 s | 89 s | — |
| wb16 + fp + v3 | 2,366 s | **521 s** | 170 s | 155 s | 818 s | 122 s | **1,228 ms** |

redb: wb16 + fp 2,407 s → wb16 + fp + v3 2,562 s (core-commit 709 → 780 s).

- Run-to-run variance on the same binary and an idle host is about 5%
  (2,314 vs 2,434 s), so neither v3 lane is distinguishable from its
  predecessor on wall clock. Inside the batch, v3 is consistently faster
  where it changed code paths (validation −7…−12%, buffered-batch commit
  −24%) and consistently slower in the engine flush's undo compression
  (+33 s on MDBX, beyond the 3 s spread between the two fp runs), which is
  attributed to heap locality: the buffer now keeps the executor's own
  per-block allocations instead of compact clones. The change is kept for
  its validation gain and simpler overlay; the flush-side cost is a lead,
  not a measured fix.
- `core-apply` (fold into the batch overlay plus building the transition,
  including a full clone of each block's undo records) is 170–176 s, about
  7% of the catch-up, and is the next bounded target; `core-validate`
  (~520–590 s, serial per block) is the largest remaining item and needs a
  profile before any parallel design.
- All eight overlays (four engines × configurations × two v3 lanes) hash to
  the same consensus content digest.

## 9. Where the serial validation time goes (2026-08-22)

Commit 6dd1005 adds thread-local timers to the per-transaction path and
logs them per batch (`core-validate-prepare/utxo/net/checks`). Measured on
the 1,024-block smoke ledger (four 256-block batches; the host was shared
with a user-run Go replay, so only the proportions are reliable):

| part of `core-validate` | share | what it is |
|---|---|---|
| prepare | ≈ 48% | per input: `get` through the block overlay (mutex, up to three map lookups, one clone), amount/maturity/lock checks, script-check construction |
| utxo apply | ≈ 30% | per transaction: write spends and creations into the block overlay (before 6dd1005 it re-read every spent coin to build undo) |
| net_changes | ≈ 14% | fold the block overlay into spent/created/undo vectors (clones again) |
| header / tip / BIP30 checks | ≈ 0% | BIP30 is off at these heights under the BIP34 anchor |
| loop overhead | ≈ 8% | fee and sigop accounting, deferred-script bookkeeping |

Two trims followed from the profile and are in the same commit: undo
records move into the store transition instead of being cloned when no
explorer or auxiliary index reads them from the applied block
(`AppliedUndos::Drop`), and the block overlay builds undo from the coins
`prepare` already loaded (`apply_with_undo_fresh_outputs_from_prevouts`).
On the smoke ledger they took the utxo-apply step down 10% and the fold
(`core-apply`) down 20% with the content digest unchanged.

What the profile says about the rest: the serial path is bound by hashing
and allocation, not by any single check — each coin passes through three
in-memory overlays (block → batch → write-back) and is cloned about four
times on the way. Trimming one clone at a time yields 5–15% of `validate`
each; a step change needs one of these designs, none of which is a small
patch:

1. Reference-counted coins (`Arc<Utxo>` or borrowed views) through the
   overlays and transitions, removing the clones outright.
2. One batch-scoped map in place of the block/batch/write-back overlay
   chain, so an input costs one lookup instead of up to three mutex-guarded
   ones; `net_changes` then disappears because the map records changes as
   they happen.
3. Resolving the next block's inputs on another thread while the current
   block is checked (the prefetch already runs ahead per batch; this moves
   the per-block map work off the serial path).

A realistic target for the three together is validation at roughly half of
today's 520–590 s, i.e. about 10–12% of the catch-up; after that the lanes
are bounded by the engine flushes and the base's member reads.

## 10. Three structural changes borrowed from btcdmdbx (2026-08-22)

The Go replay's later commits doubled its rate with an asynchronous flush,
a cross-block prepare/connect pipeline and parallel input fetching. The same
three were implemented here (commits e9aa0dc and 83b001d) and first checked
on the 1,024-block smoke ledger with a 2-batch flush interval, on a host
shared with the user's `rb_pipe8` (47 GiB). Every smoke reached 936,024 with
the overlay digest `92989dbf…`.

1. **Asynchronous write-back flush.** The buffered transitions move into an
   in-flight set and a helper thread commits them while validation continues
   into a fresh buffer; reads go pending → in-flight → engine. The flush log
   now prints how long the caller waited: `caller waited 0 ms` in every
   smoke, against a 16–31 s commit that used to stall the loop.
2. **Block pipeline.** Block N's overlay shards become a `BlockDelta`; a
   helper thread derives its net changes, folds them into the batch overlay
   and builds the transition while block N+1 validates through a
   `DeltaView` (N's shards first, batch overlay otherwise).
3. **Sharded batch overlay.** 64 independently locked shards, prefetched
   coins seeded by eight workers when the batch is large.

Back-to-back smokes with the same page-cache state (v6c = async flush only,
v7b = all three; four 256-block batches):

| timer (sum of 4 batches) | v6c | v7b | change |
|---|---|---|---|
| execute (critical path) | 38.65 s | 36.51 s | −5.5% |
| core-validate | 16.76 s | 16.37 s | −2% |
| – prepare | 8.17 s | 9.49 s | +16% |
| – utxo-apply | 5.17 s | 5.43 s | +5% |
| – net_changes | 2.16 s | 0 (moved to the tail) | |
| core-apply (tail thread, overlapped) | 4.89 s | 11.80 s | thread time, off the path |
| utxo-prefetch | 8.30 s | 9.34 s | within noise |

An earlier pair (v6b/v7b) showed a far larger gap (execute 114 s vs 36 s),
but v6b ran straight after the test suite had evicted the snapshot from the
page cache — its first-batch prefetch was 34 s — so only the v6c/v7b pair is
comparable. Read honestly: the pipeline does take net_changes and the fold
off the validation thread, but prepare grows by about the same amount while
the tail runs beside it. The thread time of the tail more than doubles for
the same work, which points at contention rather than at the extra
`DeltaView` lookups (one mutex and one hash probe per input, ≈0.15 s per
batch). The binary uses the Windows system heap; both threads allocate a
`Vec` per coin script while cloning, and the 32 script threads share that
heap. An allocator experiment is the next step before judging the pipeline;
the full-window lanes `mdbx-wb16-v7` and `redb-wb16-v7` are queued behind
the idle-host waiter regardless, because a 5% change on a 40 s smoke is
inside the measured run-to-run variance.

### Full-window result, MDBX (`mdbx-wb16-v7`, idle host, 2026-08-22 11:07Z)

Catch-up 2,465 s (11.50 blocks/s, 47,253 tx/s), tip 963,350, overlay
digest `aadd289f…` identical to the nine earlier overlays. Against
`mdbx-wb16-v3` (2,366 s) and `mdbx-wb16-fp` (2,314 s) this is +4–6%: no
gain, at the edge of the ≈5% run-to-run variance. Where the time moved
(sums over the 111 batches, v3 → v7):

| stage | v3 | v7 |
|---|---|---|
| core-commit (now just the buffer hand-off) | 818 s | 132 s |
| core-validate | 521 s | 776 s |
| core-apply (tail thread time) | 170 s | 351 s |
| utxo-prefetch | 155 s | 209 s |
| publish (includes the five compactions) | 28 s | 389 s |
| sum of batch totals | 2,091 s | 1,954 s |
| caller waited for flushes | — | 167 s |

Eleven flushes ran; the six asynchronous ones took 78–136 s each (v3's
synchronous flushes averaged 74 s) and the five that compaction forced
synchronously made the loop wait 16–58 s each. The flush thread's own
stages grew in proportion (fold 126 → 157 s, undo 122 → 190 s, mutate
144 → 196 s, sync 144 → 171 s). Every stage that now runs beside another
thread got slower by 25–50%, including single-threaded validation, which
is the signature of a shared resource rather than of the algorithms: the
binary allocates through the Windows system heap from the validation
thread, the pipeline tail, the flush thread and the 32 script threads at
once. The three changes are therefore kept as verified-correct (same
digest, same tip, crash contract unchanged) but not yet as a speed-up; the
next step is an allocator with per-thread heaps (opt-in `mimalloc`
feature), measured as v8 against v7 on the same smoke and then the same
window.

### Full-window result, redb (`redb-wb16-v7`, idle host, 2026-08-22 11:52Z)

Catch-up 2,444 s (47,664 tx/s), tip 963,350, digest `aadd289f…` — the
eleventh overlay with the same content. Between `redb-wb16-fp` (2,407 s)
and `redb-wb16-v3` (2,562 s): neutral. The same redistribution as on MDBX:
core-commit 780 → 130 s, core-validate 539 → 684 s, core-apply 176 → 346 s
of tail-thread time, publish 111 → 415 s, twelve flushes of which the
forced synchronous ones made the loop wait 169 s; peak working set 19.8 →
22.5 GiB (the in-flight set holds one extra buffer). Both engines confirm
the same reading: the asynchronous flush and the pipeline remove the engine
commit from the loop's critical path, and something shared gives the time
straight back to validation.

### Allocator experiment (`rbtcd-v8` = v7 + `--features mimalloc`)

Commit 6156c00 adds an opt-in global allocator to the `rbtcd` binary;
nothing else changed. Two alternating pairs of smokes on the idle host
(v7c/v8, v7d/v8b, four batches each, digest `92989dbf…` throughout):

| sum of 4 batches | v7c | v8 | v7d | v8b |
|---|---|---|---|---|
| execute | 34.25 s | 31.02 s | 33.15 s | 30.93 s |
| core-validate | 16.96 s | 14.63 s | 15.19 s | 14.78 s |
| core-submit | 2.21 s | 1.45 s | 2.22 s | 1.33 s |
| core-commit (buffer hand-off) | 4.04 s | 3.53 s | 4.05 s | 3.50 s |
| utxo-prefetch | 8.53 s | 8.02 s | 8.42 s | 7.93 s |

Execute −8% on average with the direction the same in both pairs and in
every stage, which is the first change since the fingerprint sidecar that
moved the critical path on the smoke by more than the noise. The
full-window lanes `mdbx-wb16-v8` and `redb-wb16-v8` are queued; their
result decides whether the feature becomes the default for the release
binaries.

### Full-window result, MDBX with mimalloc (`mdbx-wb16-v8`, idle host, 12:51Z)

Catch-up **1,435 s** (19.76 blocks/s, **81,159 tx/s**), tip 963,350, digest
`aadd289f…` — the twelfth identical overlay. That is −42% against
`mdbx-wb16-v7` (2,465 s), −38% against `mdbx-wb16-fp` (2,314 s) and −62%
against the 3,823 s MDBX baseline of the previous day. The only difference
between v7 and v8 is the allocator. Every stage moved, including the
single-threaded ones (sums over 111 batches, v7 → v8):

| stage | v7 | v8 |
|---|---|---|
| download (ledger read + decode) | 133 s | 90 s |
| structure | 54 s | 29 s |
| core-validate | 776 s | 430 s |
| core-apply (tail thread) | 351 s | 258 s |
| core-submit | 117 s | 54 s |
| utxo-prefetch | 209 s | 141 s |
| publish (compactions, forced flushes) | 389 s | 304 s |
| flush thread: fold / undo / mutate / sync | 157 / 190 / 196 / 171 s | 149 / 85 / 152 / 156 s |
| caller waited for flushes | 167 s | 107 s |
| sum of batch totals | 1,954 s | 1,300 s |
| peak working set | 27.9 GiB | 28.6 GiB |

So the pipeline and the asynchronous flush were working all along; the
Windows system heap was charging for every concurrent allocation and the
cost landed on whichever thread happened to be on the critical path. With
per-thread heaps validation alone is back below its pre-pipeline cost
(430 s against 521–560 s) while the fold, net_changes and the engine commit
run beside it.

### Full-window result, redb with mimalloc (`redb-wb16-v8`, idle host, 13:19Z)

Catch-up **1,583 s** (17.9 blocks/s, **73,564 tx/s**), tip 963,350, digest
`aadd289f…` — the thirteenth identical overlay. −35% against
`redb-wb16-v7` (2,444 s), −34% against `redb-wb16-fp` (2,407 s), −52%
against the 3,289 s redb baseline. The stage picture matches MDBX: validate
684 → 424 s, apply 346 → 258 s, submit 108 → 51 s, download 125 → 88 s,
publish 415 → 301 s, twelve flushes with 119 s of forced waiting; peak
working set 22.4 GiB, unchanged from v7. MDBX is now the faster engine on
this window (1,435 s vs 1,583 s), because its flush thread finishes
sooner and the loop waits less for the forced synchronous flushes.

### Where the window stands after two days

| build | MDBX | redb |
|---|---|---|
| 2026-08-21 baseline (engine per batch) | 3,823 s | 3,289 s |
| + write-back (16 batches) | 3,057 s | 2,606 s |
| + fingerprint sidecar | 2,314 s | 2,407 s |
| + overlay read cache, owned commit (v3) | 2,366 s | 2,562 s |
| + async flush, pipeline, sharded overlay (v7) | 2,465 s | 2,444 s |
| + mimalloc (v8) | **1,435 s** | **1,583 s** |

All thirteen overlays carry the digest `aadd289f…7f2819`. Remaining cost
on v8 (MDBX): validation 430 s (30% of the catch-up, single thread),
publish 304 s (five compactions, each forcing a synchronous flush — the
forced waits alone are 107 s), apply 258 s of tail-thread time, prefetch
141 s. The obvious next items are letting compaction proceed without
draining the write-back layer, and a second validation thread per batch.

### Compaction without draining the write-back layer (`mdbx-wb16-v9`, 19:42Z)

Commits ad62653 (mimalloc default) and ea8dd3e (compaction keeps the
buffer; the loop defers a compaction that would wait for a running
commit). Catch-up 1,434 s, digest `aadd289f…` (fourteenth identical
overlay) — the same wall clock as v8 (1,435 s). What the change did do:
seven flushes instead of eleven, none of them waited on by the loop
except the final one at the tip (86 s, inherent to flushing the last
sixteen batches), four compactions all taken between commits; the flush
thread's own stages fell (undo 85 → 73 s, mutate 152 → 116 s, sync
156 → 134 s) and validation fell 430 → 386 s. What it did not do: the
`publish` stage stayed at 317 s (304 s on v8, 27 s before the
asynchronous flush). Reading that stage's code explains it. Publish is the
ledger commit plus `prune_block_undos_before`, and the latter opens a
read-write transaction on the engine — which on both MDBX and redb means
taking the overlay's writer lock, held by the background flush thread for
the whole 60–90 s commit. Each flush therefore stalled the loop once, for
the remainder of the commit, from inside a stage the flush accounting
does not see: seven flushes × ~45 s ≈ 317 s. The write-back layer now
skips the prune while a commit is in progress (it is housekeeping on rows
that only the ledger window retires; the next batch without a running
commit prunes them), which removes the last place where the loop waits on
the engine. Measured as v10.

`redb-wb16-v9` (20:09Z): 1,490 s (78,145 tx/s), digest `aadd289f…`,
−6% against `redb-wb16-v8` (1,583 s); seven flushes, the loop waited only
for the final one (69 s); publish 285 s for the same reason as on MDBX.

### v10: no more waiting on the engine — and a crash (2026-08-22 evening)

Commit 83c44e9 skips the undo prune while a commit is in flight.
`redb-wb16-v10` (host perturbed by tool builds): 1,508 s, digest
`aadd289f…`, publish 285 → 33 s — the prune was indeed the stall — but the
loop then waited in `start_flush` for 10–36 s per flush (193 s in total),
because sixteen batches now validate faster than one commit lands. Commit
289ebc1 therefore lets the buffer keep accepting up to twice the batch
limit while a commit runs (coin limit unchanged; durable tip lags ≤ 2N).

`mdbx-wb16-v10` crashed after 564 s at block ≈948,312 with an access
violation. The minidump (kept as `mdbx-wb16-v10-crash.dmp`, unwound with
`minidump-stackwalk`, the build's PDB and Microsoft's ntdll symbols) shows
the libmdbx crate's transaction-manager thread inside `mdbx_txn_abort` →
`txn_end` → `pnl_shrink` → `realloc` → `RtlFreeHeap` reading an invalid
page-list pointer, while the flush thread was in `commit_connect_batch`
dropping an uncommitted write transaction. So a step between
`begin_rw_txn` and `commit` failed first, and the abort that followed
crashed inside the engine. The binary did not log the failing step;
commit d257e57 now does, before the transaction drops. Relevant facts:
the crate opens the environment with `MDBX_NOTLS`, begins and ends every
write transaction on one manager thread, and builds libmdbx with
`MDBX_TXN_CHECKOWNER=0`; on Windows the writer lock is a recursive
critical section; every write transaction in this code base is taken
under the overlay's own mutex. v7–v9 each ran a single MDBX lane, so a
latent race that the longer loop/commit overlap of v10 merely exposed is
not excluded. Reproduction queue: `mdbx-wb16-v11a`/`v11b` (v10 plus the
logging) and `mdbx-wb16-v10d`, a build with `MDBX_FORCE_ASSERTIONS` and
mimalloc's `MI_DEBUG=3`.

`mdbx-wb16-v11a` (21:26Z, idle host): **1,170 s** (24.2 blocks/s,
**99,528 tx/s**), digest `aadd289f…`, no crash, no logged commit failure;
publish 26 s, seven flushes of which only the final one was waited for
(86 s); peak working set 30.8 GiB. That is −18% against v8/v9 and, for the
first time, faster than the btcdmdbx pipeline's 1,318 s (1,400 s durable)
on the same window and host.

### An unplanned power-loss test (2026-08-22 23:05Z → 23 00:20Z)

The host lost power while `mdbx-wb16-v12` (commit 289ebc1: the buffer keeps
accepting up to 2N batches while a commit runs) was at block 955,736. After
the reboot the overlay's durable tip read 951,384 — 17 batches behind the
last validated block, inside the documented 2N = 32 bound — and the
interrupted data directory was resumed in place with the same command line.
The node opened the overlay at 951,384, truncated the retained ledger to it,
re-executed 951,385–963,350 in 648 s (three write-back flushes, the last
waited for at the tip as usual) and finished with the overlay digest
`aadd289f…7f2819` — the seventeenth identical overlay, this time reached
through the crash-recovery path rather than a clean run. That is the
write-back layer's crash contract exercised by an actual power cut, with
the asynchronous flush, the pipeline and the non-blocking buffer all in
place.

### v12: the non-blocking buffer, measured (2026-08-23 00:24Z–01:17Z, first lanes after the reboot)

`mdbx-wb16-v12b`: 1,249 s (93,242 tx/s), digest `aadd289f…`. Every flush
stayed at sixteen batches with no waiting — on MDBX each commit already
lands inside the next sixteen-batch window, so 289ebc1 changes nothing
there; the +7% against v11a is the cold page cache after the reboot
(prefetch 145 → 169 s) plus ordinary variance. `redb-wb16-v12`: 1,570 s
(74,208 tx/s), digest `aadd289f…`. Its flushes also stayed at sixteen
batches, and the loop still waited 10–30 s at five of the seven — but not
in `start_flush`: every one of those waits sits next to a compaction. redb
reclaims far less than MDBX (8.6 → 5.4–6.3 GB against MDBX's 7.5 → 3–4.9 GB
for the same content), so the overlay hovers at 80–86% of the 10 GiB
budget, and with the in-flight buffer counted in `used_bytes` the loop's
"compaction may not be deferred at or above the rebase threshold" guard
fires and compaction waits for the running commit. The wait is therefore
the engine's space efficiency, not the buffer. Two ways out, both
configuration rather than code: a larger redb budget (its ceiling is
policy, unlike MDBX's geometry) or a lower compaction trigger so redb
compacts earlier; neither was run tonight.

Crash accounting for the MDBX write path since the v10 crash: v11a, v11b,
v10e (assertion build), the power-loss resume and v12b all completed with
the same digest and without a logged commit failure — five clean runs,
zero reproductions, the DBWIN listener armed throughout.

`redb-wb16-v12-cap16` (05:01Z, the same binary with a 16 GiB overlay
budget): 1,557 s (74,815 tx/s), digest `aadd289f…`, one compaction instead
of six and no waits apart from the final flush — yet no faster than the
10 GiB run (1,570 s), because `utxo-prefetch` rose from 187 s to 347 s: the
larger, rarely compacted redb file answers the per-batch overlay reads more
slowly than the smaller one did. redb's remaining cost on this window is
its own file, not the loop; MDBX finishes the same work in 1,165–1,249 s.

### The crash, found (2026-08-23 05:30Z)

`examples/mdbx_concurrency_stress.rs` reproduces the v10 failure in under
a minute with no rBTC code involved: one thread commits 200k-key write
transactions back to back while two threads behave like the catch-up loop
during a flush. Bisected by reader mode, sixty seconds each:

| readers do | result |
|---|---|
| nothing | 33 commits, clean |
| read transactions + point reads | 31 commits, 324M reads, clean |
| `Database::info()` + `Database::stat()` polls | `MDBX_CORRUPTED` from the writer's `put`, `BadTxn` for readers, then a segfault |
| the same polls through a read transaction (`Transaction::env_info/env_stat`) | 35 commits, 21.8M polls, clean |

The mechanism is in libmdbx's `mdbx_env_stat_ex`/`mdbx_env_info_ex`:
called with a null transaction they ask `env_owned_wrtxn()` for "the
write transaction this thread owns" and, if there is one, read its state.
The libmdbx crate opens every environment with `MDBX_NOTLS` and compiles
the library with `MDBX_TXN_CHECKOWNER=0`, so the ownership test degrades
to "a write transaction exists" and the poll walks the flush thread's
transaction while that thread mutates it. `SnapshotOverlayChainstate::
capacity()` made exactly those two calls, once per batch, from the main
thread; before v10 the loop was always blocked behind the commit when it
reached that point, from v10 on it was not. That also explains the
statistics: the damage lands wherever the writer happens to be, so the
symptom was a failing `put`, a crash inside the abort that followed, or
an assertion — never the same twice, but always during a flush.

Fix (the vendored crate gains transaction-scoped `env_info`/`env_stat`;
both capacity queries open a read transaction for them) is measured as
v13. Nothing else in the code base calls the environment-level
`info()`/`stat()`.

### v13 measured, with a control (2026-08-23 05:44Z–07:55Z)

Four MDBX lanes with the fix and one control, all digest `aadd289f…`, no
crash, no logged commit failure, the debug-output listener armed:

| lane | binary | catch-up | note |
|---|---|---|---|
| mdbx-wb16-v13 | v13 | 1,392 s | cold page cache after the 16 GiB redb lane |
| mdbx-wb16-v13b | v13 | 1,363 s | |
| mdbx-wb16-v13c | v13 | 1,359 s | warm, back-to-back with v13b |
| mdbx-wb16-v11c | v11 (pre-fix) | 1,285 s | control, same host state |
| redb-wb16-v13 | v13 | 1,585 s | redb, same as v12's 1,570 s |

The control settles the question the slower v13 numbers raised: the same
v11 binary that ran 1,165 s and 1,170 s before the power cut takes 1,285 s
after it, with single-threaded validation 402 → 473 s, so the host itself
is about 10% slower since the reboot (the active power plan reads
"Balanced"; nothing was recorded before). Against that control v13 is 6%
slower, inside the run-to-run variance measured earlier; the fix costs one
read transaction per batch and nothing visible. With the root cause fixed,
the 1,165–1,170 s of v11 stands as the window's best MDBX time on the
pre-reboot host, and v13 is the build to carry forward.

### v14: prepare on the validation thread, apply on the tail (2026-08-23 09:43Z)

Commit a04ef05. Batch blocks are no longer written into a block overlay on
the validation thread: each transaction is resolved and checked against a
`BlockPrepareView` (what the block has resolved so far, over the state it
starts from) and nothing is written; the tail derives undo, net change and
the store transition from the prepared transactions. `mdbx-wb16-v14`:
**1,182 s** (98,558 tx/s), digest `aadd289f…`, no crash, peak working set
28.3 GiB (2.5 GiB less — no block overlay). Against the same-day controls
that is −13% on v13c (1,359 s) and −8% on the pre-fix v11c (1,285 s);
`core-validate` fell 489 → 268 s (−45%), `execute` 987 → 832 s.

Why the wall clock did not fall by the full validation saving: the tail
now does the undo and net-change work as well as the fold — 279 s of
thread time, 9.8 ms per block against 9.5 ms of prepare — so the two
stages are balanced and the validation thread waits at the join roughly
220 s over the window (`execute` minus its measured stages). The next
step is therefore the tail: it clones every created coin once for the net
change and again into the batch overlay, and builds undo records the
catch-up never reads unless an explorer or index is attached.

`redb-wb16-v14` (10:07Z): 1,405 s (82,879 tx/s), digest `aadd289f…`, −11%
against `redb-wb16-v13` (1,585 s), validation 470 → 257 s; its compaction-
forced flush waits (16–32 s at five of seven flushes) remain.

### v15: the tail measured, and lightened (2026-08-23 10:47Z)

Commit 1532635 splits `core-apply` into net-change, fold and the rest in
the batch log and stops maintaining the batch overlay's `original` map
during the fold (the overlay's own net change is never read on the
pipelined path). `mdbx-wb16-v15`: **1,140 s** (102,122 tx/s), digest
`aadd289f…`, no crash — the window's best so far. The tail now costs
222 s of thread time: 42 s deriving net changes, 130 s folding into the
batch overlay, about 50 s building undo and the transition; the
validation thread spends 239 s in `core-validate` (167 s of it resolving
inputs and checking transactions). The two stages are still close to
balanced, so the fold is the next target; commit 623470a groups a block's
keys by shard and takes each shard lock once per block (v16).

`mdbx-wb16-v16` (11:47Z, commit 623470a — one shard lock per block):
**1,118 s** (104,133 tx/s), digest `aadd289f…`. The fold fell 130 → 103 s,
but `core-validate-prepare` rose 167 → 206 s: a shard lock held for a
block's whole share of inserts makes the validation thread's reads on that
shard wait. Commit 564b766 takes each shard for at most 64 keys at a time;
commit 02c5fb0 separately tapers the write-back flushes inside the last
window before the ceiling so the final waited-for flush (85–87 s on every
lane so far) covers a batch or two. Both are measured as v18.

`mdbx-wb16-v18` (12:48Z, commits 564b766 + 02c5fb0): **1,062 s**
(109,624 tx/s), digest `aadd289f…`. The taper did what it was meant to:
the final waited-for flush covered 5 batches and 22 s instead of 15
batches and 87 s. The burst fold left the stage split where v16 had it
(prepare 205 s, fold 105 s), so v15's lower prepare was variance rather
than lock contention. Against the btcdmdbx pipeline's 1,318 s (1,400 s
durable) on the same window and host, MDBX is now 19% faster.

`mdbx-wb16-v19` (13:51Z, commit ea7c5e7 — the replay reads the next batch
from the ledger while the current one executes, as the networked path
already downloads it): **1,033 s** (112,746 tx/s), digest `aadd289f…`.
`download` fell 97 → 39 s; what remains of it is decoding the prefetched
bytes and checking their hashes on the loop thread, and `structure`
(32 s) and `stage` (54 s) are still serial per batch.

### v20 and v21: the read-ahead does the batch's preparation (2026-08-23 14:46Z–15:56Z)

Commit 9310eac moves decoding and structure validation of the next batch
into the replay read-ahead thread (`mdbx-wb16-v20`: **1,016 s**,
114,642 tx/s; `download` and `structure` are now 1 s and 0 s over the
window). Commit f3b7c53 moves the UTXO prefetch there as well: the
read-ahead reads the next batch's external inputs through the write-back
layer while the current batch executes, and a new
`ExecutionChainStore::reconcile_prefetch` overlays what the buffers hold
— every change the current batch committed since — before the prefetch
is used, so the early read is exact. `mdbx-wb16-v21`: **965 s**
(120,697 tx/s), digest `aadd289f…`; `utxo-prefetch` fell 155 → 21 s
(the reconcile), `execute` 839 → 755 s, and the read-ahead thread was
busy 488 s of the 965, entirely beside execution. Against the btcdmdbx
pipeline's 1,318 s on the same window and host that is 27% faster.

What is still serial per batch: `core-validate` 307 s (input resolution
and checks), the tail join, `core-commit` 135 s (the hand-off into the
write-back buffer — its maps were std SipHash; commit 847a692 switches
them to ahash), `stage` 72 s (the ledger write and its fsync), and
`core-submit` 60 s (handing scripts to the pool).

`mdbx-wb16-v22` (16:30Z, commits 847a692 — ahash for the write-back
buffers — and aa70698 — the tail hands scripts to the pool): **953 s**
(122,278 tx/s), digest `aadd289f…`; `core-commit` 135 → 95 s and
`core-validate` 307 → 249 s, the latter because every read that misses
the block overlay probes the buffers. The tail is now the longer stage
(apply 231 s plus submit 68 s of thread time against 249 s of
preparation), so commit 3764ed7 splits it into two helpers per block
(net change, fold and transition on one; undo records and script submit
on the other), measured as v23. `redb-wb16-v22`: 1,297 s, digest
`aadd289f…` (v21: 1,296 s).

`mdbx-wb16-v23` (17:21Z, commit 3764ed7 — two helpers per block): **920 s**
(126,635 tx/s), digest `aadd289f…`; `core-apply` (now the fold helper
alone) 231 → 148 s, `core-validate` 249 → 286 s with three threads
sharing the batch overlay and the allocator, `execute` 747 → 714 s.
Against the btcdmdbx pipeline's 1,318 s on the same window and host the
MDBX lane is now 30% faster; against the 3,823 s of the first MDBX lane
two days earlier it is 4.2× faster, with the same overlay content every
time.

### v24: one fold, owned by the buffer (2026-08-23 20:38Z)

Commit 644593f. Coins entering the write-back buffer were cloned into
its created map while the transitions kept their own copies, and the
flush re-folded the whole window from those transitions. The buffer now
drains each transition's coins as it folds and the flush hands the
engine the net change it already holds, sorted, through a new
`commit_connect_folded`. `mdbx-wb16-v24`: **895 s** (130,143 tx/s),
digest `aadd289f…`, peak working set 31.8 → 25.9 GiB, the flush
thread's fold stage gone. `redb-wb16-v24`: 1,232 s, working set
28.5 → 21.4 GiB. Reworking the in-flight test deterministically along
the way documented a real property: a coin created and spent inside one
buffered window cancels and is unknown to the buffers by design.

### v25: the batch prepared in parallel (2026-08-23 21:2xZ)

Commit e5e5c45. What a block does to the coin set — the coins it
creates, with their final values and scripts, and the keys it spends —
follows from its raw transactions alone. A batch is now prepared in
three phases: workers derive every block's transaction-level delta and
the main thread merges them, in block order, into one versioned view of
the batch; eight workers then prepare all 256 blocks at once, each
resolving a key from the latest version an earlier block left (never a
later one) or from the prefetched batch overlay; workers finally build
transitions, undo records and script hand-offs, collected in order. The
sequential prepare pipeline, its delta hand-off and the batch-overlay
fold are gone. On the smoke ledger `core-validate` fell 2.4 → 0.9 s per
256-block batch with the digest unchanged; the full-window lanes are
running.

## 11. Tools added

- `src/bin/fdb_ledger_import.rs` — read-only btcd `.fdb` → ledger import with
  chain selection from a base hash, CRC-32C checks and ranged re-verification.
- `src/bin/utxo_locality.rs` — spent-output age histogram over a ledger.
- `src/bin/overlay_audit.rs` (`--features mdbx`) — read-only consensus-content
  digest of an MDBX or redb overlay (`SnapshotOverlayChainstate::audit_content`,
  `SnapshotOverlayRedbChainstate::audit_content`); scans 14.5M coins in ~5 s.
