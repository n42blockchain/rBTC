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

## 10. Tools added

- `src/bin/fdb_ledger_import.rs` — read-only btcd `.fdb` → ledger import with
  chain selection from a base hash, CRC-32C checks and ranged re-verification.
- `src/bin/utxo_locality.rs` — spent-output age histogram over a ledger.
- `src/bin/overlay_audit.rs` (`--features mdbx`) — read-only consensus-content
  digest of an MDBX or redb overlay (`SnapshotOverlayChainstate::audit_content`,
  `SnapshotOverlayRedbChainstate::audit_content`); scans 14.5M coins in ~5 s.
