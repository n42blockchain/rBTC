# Persistent validated header views, 2026-09-17 UTC

Status: checked implementation and kernel measurement; **header-resources
remains open**. This continues the node candidate work at `e9f2aff` and removes
concrete in-memory DAG requirements from execution and related read consumers.

## Implemented

`HeaderView` exposes immutable, fallible queries for header identities, selected
heights, ancestors, locators and MTP. Storage errors are distinct from absent
headers and never classify as peer consensus invalidity. Existing in-memory
DAGs implement the interface, retaining their optimized locator behavior.

Block connection, batched connection, disconnect, deployment decisions, IBD
policy, undo retention and assumed-snapshot finalization now accept this view.
The redb, MDBX, snapshot-overlay and write-back implementations propagate
header read failures before committing destructive state changes. Candidate
validation contexts and journals can also obtain their anchor context from a
view instead of requiring a complete in-memory DAG.

`DiskHeaderIndex` builds a fresh derived index from raw, contextually validated
headers. It has an explicit 8 MiB redb cache, at most 2,000 pending headers and
one rolling consensus context (plus its bounded rollback copy). Height, work
and ancestor jumps are derived during validation; an API to trust those caches
on reopen is deliberately absent. Files are created exclusively, so a stale
index is not overwritten or mistaken for validated state. Records include an
identity check and checksum. These detect accidental corruption; they are not
an authenticated substitute for startup revalidation.

Each committed batch yields an immutable redb read version with a cached
selected tip. Older views continue to represent their old branch after a new
winner commits, without cloning historical headers. An ancestor jump clears
the lowest set height bit; lookups use that jump or the direct parent, checking
the expected descending height at every step. No active-chain vector is built.

`HeaderReplayReader` streams at most 2,000 raw records per call from one pinned
source-store version. It validates raw record identity, checks work before
allocation, and advances its cursor only after a complete read. This allows a
startup driver to revalidate into the disk index without materializing a DAG.
The normal node startup driver has not switched to this reader yet.

## Verification

Mac all-feature library suite after storage-consumer migration: **960 passed,
0 failed, 11 ignored**, 58.01 seconds. The execution regression then passed
again after adding undo-pruning, IBD and deployment read-failure assertions.
Final all-feature/all-target strict Clippy, formatting and diff checks passed.
Existing dependency C compiler warnings remain unrelated to this change.

New cases cover:

- Active and side-chain indexed queries versus a validated in-memory reference,
  old read-version isolation across a stronger fork, and readers outliving the
  writer handle.
- Invalid second-header and exhausted-work rollback, exclusive file creation,
  checksummed-record corruption returning a local read error, and an unaffected
  older read version.
- Pinned raw replay across concurrent source growth, read-budget exhaustion
  without cursor movement, and multi-frame index construction after dropping
  the reference DAG.
- Real redb chainstate block connection, disconnect and winning-branch batch
  execution using disk views only. Injected reads fail without changing the
  execution tip or removing undo. Successful pruning resolves disk-backed
  history; failed IBD/deployment reads produce explicit local errors.

## Large active-history probe

Command: `/usr/bin/time -l target/release/examples/header_index_probe 2100000`.
The release probe uses mimalloc, generates valid regtest headers in bounded
batches, and checks sampled historical identities plus the selected locator.
Unlike the prior journal-only probe, it retains queryable indexed active history.

| Measurement | Result |
| --- | ---: |
| Validated non-genesis active headers | 2,100,000 |
| Maximum RSS reported by `time -l` | 27,672,576 bytes |
| Last sampled RSS | 24,544 KiB |
| Final logical index file length | 1,078,472,704 bytes |
| Elapsed wall time | 171.79 seconds |
| Final selected hash | `50c86296fb1746976ba34d75d2e328209dfb0c0d991f5e9b00cb81f8e74bf489` |

The probe exited successfully and removed its temporary index. Concurrent local
build/test activity overlapped this run, so elapsed time is not an isolated
throughput comparison. Logical file length does not measure allocated disk,
write volume or old-snapshot retention. Raw samples, external process metrics,
probe executable hash and relevant source hashes are under
`target/header-views-20260917/`. The executable predates the storage-consumer
signature migration; its index and query implementation was unchanged. This is
kernel evidence, not acceptance bound to a final release commit.

## Still required

The node's owning synchronization/startup state and inbound serving projections
still use HeaderDag. The derived index is not yet their production replacement.
Huge-candidate publication must coordinate authoritative raw history, durable
selected-tip identity and a new read view across crashes; this writer's atomic
batches alone do not establish that whole-node guarantee. Shared bytes/disk and
reader lifetimes, startup total-memory admission, fair candidate scheduling,
and frozen-source long-duration whole-node RSS/disk acceptance remain open.
No public seven-day soak was started.
