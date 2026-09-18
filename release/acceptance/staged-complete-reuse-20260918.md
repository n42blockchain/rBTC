# Reuse complete unexecuted staged segments

Normal ledger reconciliation and overlay startup now preserve a wholly
unexecuted next segment after verifying its complete archive, active-chain
headers and block structure, including the execution tip hash. Stale or other
restart cases retain their prior reconciliation policy.

The live batch entry reads an existing stage as an identity-bound local source.
It requires the entire segment to fit the configured batch and execution-height
limits. It clears speculative input, disables further speculative reads, and
performs ordinary block/UTXO/script validation. All three execution branches
call stage_or_verify: existing stages must match the original manifest, starting
height, complete count and every serialized block byte under the ledger lock.
No stage is rewritten. Successful execution uses the existing full-segment
publication/fsync protocol. A changed, missing or mismatched stage fails before
execution; a smaller batch limit fails locally without discarding the suffix.

Validation on Mac:

- Full all-feature library suite: 1,071 passed, zero failed, 12 ignored, 69.07 s.
- Strict locked all-target/all-feature Clippy: passed, 28.94 s.
- Fuzz locked all-target Clippy: passed, 3.33 s; formatting/diff checks passed.
- Real redb/ledger test stages two mined Regtest blocks, closes and reopens the
  ledger, then runs normal startup reconciliation. A one-block limit preserves
  the exact original file and genesis checkpoint. Exhausted shared memory also
  preserves both. After releasing pressure and allowing two blocks, the node
  executes/publishes both from the stage with an idle peer, ignoring unrelated
  speculative bytes. Published block bytes match the original corpus.
- Ledger regressions reject changed first height, partial count, changed bytes
  and replaced identity; rejected verification never mutates or publishes data.

During full regression an older codec test failed compressed-byte equality.
Saved outputs were20,972,009 and20,972,006 bytes: the first had160 raw blocks plus
an empty terminal block, while the second marked its160th block terminal. Both
independently decoded to every original byte of the20MiB input. The native test
now validates exact decoded content for both encoders, rather than requiring
identical compressed framing; allocation-denial coverage and small archive byte
comparisons remain. The failed run and diagnostic hashes/summary are preserved.
No production codec or acceptance resource threshold changed for this finding.

Prior exact CI35324717780/901a0fe passed Linux, Windows and supply-chain checks.
Exact CI35388353685/7940749 subsequently passed Linux, Windows and supply-chain
checks. This is not arbitrary-size staged recovery:
a segment exceeding a smaller batch is retained but cannot yet make progress.
Checkpoint-aware suffix preservation and post-commit publication/index recovery,
fair pressure resumption, whole-node startup/RSS/disk and final-source/long-term
acceptance gates remain open.
