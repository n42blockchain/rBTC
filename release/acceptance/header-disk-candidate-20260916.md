# Bounded disk header candidates

Status: recovery primitive implemented and measured; **header-resources remains
open**. Implementation starts from `bf0637d2082830e4a54de922030a571960a3fce5`.
The node ingress and winning-fork promotion paths do not yet call this primitive.

## Implementation

`DiskHeaderCandidate` stores one contiguous fork anchored in an already validated
DAG. It reuses the DAG's contextual and structural validators, including MTP,
retarget rules, BIP94, buried versions, checkpoints, PoW and cumulative work.
Those checks were extracted without changing ordinary DAG publication.

The candidate keeps one difficulty interval (at least eleven ancestors), genesis
and applicable checkpoint entries. This is 145 entries after warm-up on regtest;
Bitcoin/Testnet contexts use a 2,016-entry interval plus pinned entries. The
lookup window is private and never published as an active DAG. Neither append
nor replay copies the source DAG or retains the entire candidate in memory.

The journal format binds an anchor hash and consensus-configuration digest in a
72-byte prefix. Each frame has a header count, at most 2,000 raw 80-byte headers,
and a SHA-256 checksum. It persists no trusted chainwork or height caches.
Successful append syncs a complete frame before exposing the new tip. Validation
failures publish no prefix; write failures restore the prior file boundary or
poison the handle until reopen. An exclusive file lock prevents cooperating
writers from concurrently modifying one journal.

Reopen validates every complete frame from the anchor. Only a final incomplete
frame is removed, and only after prior complete frames have passed contextual
validation. Complete checksum or consensus corruption is preserved and rejected.
This is framed journal recovery, not proof against arbitrary filesystem damage.

`start_recovery` opens just the identity and bounded context. `advance` accepts a
maximum frame count and consumable work ledger. If work ends inside a frame, the
prior checked-frame boundary is retained and the frame can be retried later;
spent work is not refunded. `finish` refuses to expose a candidate until replay
is complete. Dropping unfinished recovery leaves committed history intact.
`visit_batches` streams bounded frames to a destination callback and shares its
work ledger. Destination commit/promotion atomicity remains the caller's job.

The default journal file-length limit is 128 MiB. Append checks the complete
frame's charge before validation/allocation/writing; oversized existing journals
are refused. This is a per-file logical byte limit, **not** a global candidate
quota, allocated-disk guarantee or whole-node RSS ceiling. The probe explicitly
selects 256 MiB to exercise more than two million headers.

## Verification

Mac all-feature library suite: **952 passed, 0 failed, 11 ignored**. The
incremental-replay test was then strengthened to exhaust work after the first
header of a two-header frame and passed independently. All nine journal tests
also passed after making their file reads compatible with Windows byte-range
locking; this was a Mac rerun, not a Windows execution. Strict all-target,
all-feature Clippy, formatting and diff checks passed.

New tests cover a 5,000-header candidate against an ordinary full DAG, bounded
contexts across Testnet/Testnet4 retarget boundaries and MTP windows, streamed
export, callback cancellation, invalid batch rollback, exact byte allowance,
replay work deferral, configuration identity, checksummed but consensus-invalid
records, complete corruption preservation, exclusive locking and real read-only
write failure. Every truncation position of a final 116-byte frame is checked:
reopen observes the previous complete tip or the complete new tip.

These are not process-kill/full-node crash tests. The write-failure case forces a
real descriptor failure; it does not emulate every filesystem ENOSPC/fsync fault.

## Mac large-candidate measurement

Command (default-feature release build, mimalloc):

```sh
/usr/bin/time -l target/release/examples/header_candidate_probe 2100000
```

The probe generated **2,100,000 valid regtest headers**, exceeding the in-memory
DAG's default 2,000,000-entry ceiling. It used 2,000-header frames and then started
an independent child process that replayed one frame per scheduling slice.
The child checked header count, height, final hash and cumulative work against
the generated candidate. The source DAG contains only genesis in this workload.

| Measurement | Result |
| --- | ---: |
| Maximum candidate consensus entries | 145 |
| Final journal file length | 168,037,872 bytes |
| Generation time through final append sample | 15.017731 seconds |
| Fresh-process validated replay | 5.588128 seconds |
| Whole command elapsed (`time -l`) | 20.95 seconds |
| Maximum reported RSS (`time -l`) | 3,342,336 bytes |
| Fresh-process final sampled RSS | 3,309,568 bytes |

Phase RSS samples can miss short peaks; the external time output is also
retained. File length is not filesystem allocation or physical write volume.
This short regtest kernel run does not measure a full active DAG, serving views,
peer queues, block execution or mainnet consensus throughput. It is not a
long-duration whole-node acceptance result. The probe deletes its own temporary
journal after verification; no large artifact was copied to another host.

Raw evidence and source/executable hashes:
`target/header-candidate-20260916/` in the development worktree.

## Remaining integration

1. Connect peer/local ingress to a globally bounded candidate scheduler, retain
   its anchor and negotiate continuation from the persisted candidate tip.
   Handle peers returning an older known prefix without duplicating disk state.
2. Promote a stronger disk candidate atomically through header selection and
   block execution without materializing two complete competing DAGs. Preserve
   executed ancestry and recover interrupted promotion.
3. Share byte/work allowances across candidates, retained DAGs, serving views,
   disk writes, reconnects and startup. The existing ordinary-node startup path
   still materializes retained history; the previous 869 MiB fresh-replay
   observation remains relevant and has not been replaced by this kernel result.
4. Freeze the integrated implementation, then run sustained whole-node RSS/disk,
   deep-reorg, failover, cancellation/write-failure and process-crash acceptance.
   The separate seven-day public-network soak has not been started.
