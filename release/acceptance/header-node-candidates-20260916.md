# Node disk-header candidates and atomic bounded promotion

Status: node integration implemented for a single candidate and bounded promotion;
**header-resources remains open**. Parent implementation commit:
`532f4a37d6c9b3c9a425d6e0574c9fa30ac38874`.

## Behavior

The primary header synchronization path starts a disk candidate when a new side
branch arrives with at least 50,000 retained side headers. One adjacent
`headers.candidate` journal is available, with the existing 128 MiB logical file
limit. Its continuation locator starts at its validated tip. Disconnects leave
complete batches durable; reconnect/startup revalidates one frame per scheduling
slice, with a task yield between frames. Ordinary retained-DAG startup replay
still happens first. Candidate identity hints are untrusted until full replay.

A winning candidate can be promoted only within an 8 MiB allowance for the input
header vector and the two staging vectors, and the existing two-million-entry
retained-DAG limit. Promotion reserves work before materialization, revalidates
headers in the existing DAG, checks the resulting tip, commits one redb recovery
transaction, and then commits the in-memory staging guard. A failed durable
transaction rolls the DAG back. Successful promotion removes the journal after
the database commit; a leftover already-published journal is idempotently
recognized after restart. This changes the selected header chain; block bodies
and execution still use the existing download/execution pipeline.

Oversized winners return local-resource deferral with the journal intact. This
is **not** out-of-core winner activation and does not make a previously
unpromotable huge winner executable. No partial fork is published. With a
pending journal, idle header eviction is conservatively suspended so its source
anchor cannot disappear. Other competing branches use the ordinary DAG while
the slot is occupied; there is no fair multi-candidate scheduler yet.

## Shared work accounting

Primary ingress, candidate replay/append/promotion, standby-peer headers and
local submitted-block header validation now reserve from one process-wide work
pool, preserved across batches, reconnects and embedded node instances. The
pool has a 1,000,000,000-unit capacity and refills 64,000,000 units/second using
monotonic time. Ordinary slices reserve 32,000,000 units; promotion reserves
256,000,000. Only unused work is returned on lease drop. Failed validation keeps
its spent charge. The peer paths wait for admission; synchronous local submission
leaves its queue untouched when no reservation is available. Work exhaustion
inside an admitted local drain is returned explicitly to its submitter.

These are conservative logical traversal/validation units, not CPU cycles,
wall-clock deadlines, or a strict CPU-usage cap. Offline reindex and ordinary
retained-store startup replay still use their existing per-operation allowances.
This pool is not a shared byte, database-cache, mmap, disk-allocation or RSS
budget, and does not account for every node subsystem.

## Verification on Mac

- All-feature library suite: **955 passed, 0 failed, 11 ignored**, 54.88 seconds.
- Final deterministic shared-work test passed after extracting unused-work
  release accounting; it covers exhaustion, refill, multiple consumers and
  preservation of spent work after an unsuccessful reservation.
- Strict all-feature/all-target Clippy, formatting and whitespace checks passed.
  The pre-existing third-party bitcoinconsensus C compiler warning remains.
- Store test covers losing candidates, preallocation byte deferral, a failure inside a real redb
  transaction injected after its first inserted row, rollback of both
  store and DAG, successful publication, independent reopen, and idempotent
  crash-cleanup recognition without duplicate rows.
- Loopback node header-sync test uses a 2,001-header active chain and a
  2,002-header fork. It spills at an overridden threshold, disconnects after a
  full response, protects the retained side anchor from eviction, resumes the
  disk locator, defers promotion under a one-byte allowance, and promotes on
  restart under the normal allowance. The independently reopened store agrees
  with the selected winner. This tests the production synchronization function,
  not whole-daemon block execution, process-kill faults or resource acceptance.

Small raw logs and source hashes are kept in
`target/header-node-candidates-20260916/`. No large database was retained or
copied; no Linux or public-network soak was started.

## Remaining gate work

1. Persistent active-chain queries and atomic activation/execution for winners
   too large for the bounded in-memory promotion path; fair scheduling of
   multiple candidates and precise anchor pinning.
2. Shared byte/disk reservations covering all live candidate contexts, retained
   DAGs, serving views, peer inputs, storage transactions/caches and their overlap.
3. An enforced total startup-memory limit including ordinary retained-store
   replay and serving/execution views. The previous million-active-header replay
   result of roughly 869 MiB is not invalidated by these changes.
4. Frozen-source, long-duration whole-node RSS and allocated-disk measurements,
   with concurrent peers, block execution, restarts and faults. The earlier
   2.1-million-header / roughly 3.2 MiB journal-kernel measurement remains a
   different workload and cannot close this gate.
