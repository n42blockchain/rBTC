# Resume immutable staged segments across execution checkpoints

A staged segment larger than the configured batch or execution-height window
can now advance in smaller batches. Each batch is read and compared against the
same complete archive identity before ordinary block/UTXO/script validation and
execution. The original stage remains immutable until its last block is durably
published; no suffix replacement or extra persistent recovery journal is needed.

`commit_staged_through` takes a cumulative executed count supplied by the node.
It verifies the entire source, checks ledger contiguity, compares every retained
published overlap byte-for-byte, and writes only the missing executed range via
the bounded streaming range writer. Circular retention can remove older prefixes;
recovery requires equality of the overlap that still exists. Existing slot fsync,
rename, index publication and reopen recovery govern each checkpoint. Only final
publication removes the stage. The complete first-batch fast path retains the
existing rename optimization. The older explicitly suffix-discarding API remains
available to callers with that contract.

Normal startup and overlay startup verify the execution tip identity, active
headers and block structure before recovering matching committed prefixes. They
resume unexecuted blocks from the durable tip. When a future suffix has left the
active chain but its executed prefix remains valid, they publish the prefix before
discarding stale future input. Overlay recovery runs after truncating retained
blocks above its execution checkpoint; an earlier missing ledger prefix fails
closed because this startup helper has no peer backfill. Normal startup retains
its peer backfill path.

Comparison now visits only overlapping records while checking full archive
integrity. Its64KiB comparison buffer reserves from the same memory owner before
allocation. Source/comparison spool and range-writer native compression admission
remain shared and bounded. No database copy or database format migration is used.

## Validation on Mac

- Final locked all-feature library suite:1,080 passed, zero failed,12 ignored,
  68.95 seconds. The only subsequent source edit gates the overlay helper with
  `cfg(any(test, feature = "mdbx"))`; all-feature and test behavior is unchanged.
- Final22 staged-focused tests passed in2.60 seconds, including existing staged
  integrity, prefix publication and cleanup regressions.
- Strict locked all-target/all-feature Clippy passed in9.28 seconds; after the
  feature guard, strict default-feature all-target Clippy passed in16.77 seconds
  and fuzz locked all-target Clippy passed in2.60 seconds. Formatting/diff passed.
- Real redb, mined Regtest blocks and an idle localhost peer exercise one-block
  execution with a one-height ceiling, preserving the exact original two-block
  stage. The fixture removes the published prefix to represent a checkpoint
  surviving without publication, closes/reopens BOTH chainstate and ledger, and
  invokes normal/overlay startup recovery. The prefix is republished at the same
  execution tip, memory denial preserves that checkpoint and original bytes,
  releasing pressure allows the second batch and exact final payloads, and no
  block download is supplied by the idle peer.
- Two further real-node cases replace the future header branch and verify that
  startup publishes block1, discards stale block2 and never executes/publishes it.
- Nine injected durability-boundary cases cover SlotArchive, SlotPublish,
  IndexFile and IndexPublish for intermediate/final checkpoints, plus final
  StagedRemoval. Reopen/retry preserves the source until final publication and
  recovers exact output, including a one-slot ring which prunes prior prefixes.
- Repeated checkpoint publication, identity replacement, conflicting retained
  bytes, invalid counts and exhausted shared memory/spool are covered. Budget
  failures leave the original stage and previous published tip intact; releasing
  reservations allows progress and owner counts return to zero.

Earlier full regression passed1,078 tests before the stale-suffix cases and range
comparison refinement. Initial Clippy rejected the enlarged comparison function;
visiting only selected overlaps removed redundant callback branches. Fuzz/default
configuration review found the overlay helper was unused without MDBX; the feature
guard fixes that. Historical intermediate logs are retained separately.

## Remaining acceptance

This closes the previously missing small-checkpoint integration for matching
staged data, not the release resource gate. Existing-stage memory pressure still
requires resumption with available resources/configured smaller batches; automatic
staged downshifts and fair scheduling remain open. Some publication errors require
reopening to recover the durable index. Auxiliary-index/post-commit recovery,
whole-node crash/kill testing, aggregate work admission, physical peak disk and
startup/sustained total RSS require broader acceptance. Each integrity pass still
scans the whole stage; this is not constant-work checkpointing. The original stage
coexists with retained prefixes until completion, so no whole-node disk-limit
claim follows from bounded transient spool reservations.

Previous exact CI35389996036/dee1752 passed all jobs; this source needs its own CI.
No optimizer/storage evidence was rebound, no604800-second soak was accepted,
and no release tag or publication was made. See the [current release audit](release-gate-audit-20260919.md).

Follow-up: exact CI35430419364/9d0b300 passed all jobs.
[Finite staged memory retries](staged-memory-retry-20260919.md) now handle an
unchanged existing stage before execution commit; post-commit retries remain open.
