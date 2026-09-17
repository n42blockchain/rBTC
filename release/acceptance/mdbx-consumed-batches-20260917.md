# MDBX atomic batch input lifetime

The execution driver already calls `commit_connect_batch_owned`, but MDBX used
the trait's borrowing fallback. Inputs and undo survived until the whole batch
completed, alongside a batch-wide folded index and engine pages.

MDBX now implements the ownership-taking entry point. Borrowed and owned callers
share one transaction implementation that validates, folds and writes one
transition at a time. An owned transition and its undo drop before the next
transition's temporary index is built. Only the final tip and one commit publish
the checkpoint. A late error aborts earlier writes. The existing 256-block bound
and final-height tier classification are unchanged.

The borrowed API remains supported, retaining caller-owned inputs as required.
The original scale driver still uses it and is unchanged: no batch size, retention,
maintenance trigger, RSS threshold or input shape was relaxed to obtain a pass.
The owned production route additionally benefits from releasing consumed inputs.

This trades cross-block cancellation before disk writes for smaller temporary
indexes. Ephemeral outputs may now be written and removed inside the same engine
transaction. Dirty-page growth, throughput and maintenance behavior therefore
require measurement; lower application allocation alone is not an RSS result.
Batch-vs-sequential content and per-block disconnect tests exercise the owned
route. Borrowed-route tests still cover ephemeral outputs, late rollback, corrupt
spent records and the 256/257 boundary.

The planned reduced measurement repeats the historical failed workload: 2M UTXOs,
4,096 transitions, 5,000 updates each, 1 GiB geometry, 288 undo retention, compact
55%, reclaim 10%, growth 50%, serial 64/256 lanes, RSS ratio <=1.5. Reports every
256 transitions aid monitoring without changing workload or thresholds. The
runner freezes binaries, hashes the source, reopens each store independently and
runs the compaction crash matrix. Results belong to that frozen source only.

No RSS or whole-node gate is closed by this implementation checkpoint. Owned
input generation, engine pages and other concurrent components still need a
shared byte budget. Full-scale, final-source replay and public soak remain open.

Mac checks: 15 MDBX tests passed; full all-feature library tests passed 984
(0 failed, 12 ignored; 67.26 seconds). Strict all-target/all-feature Clippy,
format and diff checks passed. Prior e69421d CI 35184076573 passed all jobs;
that CI result does not cover this new implementation.

## Measured reduced result

Frozen source: `1f437325cef47032fec4cb015c2c4264c69aac34`. Both lanes completed
and passed independent-process reopen audits. The runner exited 1 with
`review_required`; RSS acceptance **failed**.

| Batch | Peak RSS bytes | Final checkpoint seconds | Compactions |
| --- | ---: | ---: | ---: |
| 64 | 663,109,632 | 47.78 | 0 |
| 256 | 1,473,953,792 | 48.97 | 0 |

256/64 RSS ratio: **2.2227905023**, exceeding 1.5. Both final content hashes are
`d1a9badf1e78d8bdd07324579437b95480db9034da539b610ff7bce43588acdb`, matching the
historical reduced workload. Neither lane reached the unchanged compaction
trigger, so this run supplies no churn-triggered maintenance evidence. It does
not replace the old maintenance failure or constitute full-scale acceptance.

Evidence remains in `/Users/jieliu/Documents/n42/rBTC-storage-consumed-20260917/`
(matrix, frozen executables, source hashes, attempts and reopen logs); small
copies are under the primary workspace's session-state directory. No database
was copied. This run used the original borrowed-input driver; the production
owned-input route still needs separately identified measurement.

## Ownership-specific follow-up

The driver now accepts `RBTC_MDBX_GATE_CONSUME_INPUTS=1` for a separately
identified owned-input measurement. Default remains 0 (the original borrowed
route). The manifest and each report bind this choice; resume cannot switch
it. Older reports without the field are accepted only as borrowed. Both modes
still construct the same full input batch and commit the same 64/256 blocks
atomically. No threshold or maintenance setting changes. Ten runner evidence
tests and strict all-feature driver Clippy passed before measurement.
