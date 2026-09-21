# Execution preparation spool (2026-09-17)

Overall production, total-memory and sustained RSS gates: **OPEN**.

Node-owned ordinary redb stores now expose a temporary-result context sharing
the node memory owner. Write-back forwards that context. For batches using
`AppliedUndos::Drop` (the node path without explorer/auxiliary indexes needing
returned undo), each preparing worker converts its completed result into a
transition and writes it to an anonymous file in the chainstate directory.
Completed slots retain record descriptors instead of all prepared coins. Script
checks still drain before any execution publication. The atomic commit source
then reads transitions in order with their real memory leases.

All such contexts belonging to the node share a 16 GiB logical spool allowance.
A record reserves its complete encoded size before encoding or file growth;
partial-write failures retain their charges until file close. Codec memory is
reserved before allocating encoded/decoded buffers. The per-record conservative
estimate includes wire buffers, codec scratch, decoded vector elements and
scripts. It is deliberately retained with `LeasedConnectTransition`, including
when a write-back or journal collector keeps the payload after return. A trusted
in-process descriptor carries the size, allowance and SHA-256 digest. Reads
verify the complete record before decoding its stored counts; the spool is not
a persistent interchange format and never trusts disk-supplied descriptors.

Files close before their disk charges are released. Process death deletes these
temporary results; restart uses the already durable raw-block ledger and
committed execution checkpoint. No prepared prefix is published merely because
it reached disk. Spool failures have an explicit local-resource classification,
so an exhausted allowance or damaged local file does not punish the supplying
peer. Status JSON adds `execution_spool_reservations` (limit/used/peak).

Validation covers real producer and consumer paths:

- Exact UTXO metadata, tips and transaction undo round-trip; corrupted vector
  count rejected before decode; memory ownership outlives the spool itself.
- Two independent files share one disk allowance; exhaustion rejects before
  growth. Memory exhaustion prevents encoding/reading and returns unused charges.
- 64 records exceed a 2 MiB memory allowance in aggregate but can be consumed
  individually. Retaining one decoded payload prevents a second reservation;
  releasing it allows progress. All memory/disk charges eventually return to zero.
- The batch executor feeds a real Redb write-back store: exhausted memory leaves
  no accepted prefix; retry succeeds, returned applied undos are empty, disk
  charges clear after preparation, and decoded reservations persist until flush.
  Reopening verifies the final tip and both stored block undo records.
- Earlier-block script failure still wins and leaves no execution publication or
  spool charge. A killed subprocess leaves no temporary result file behind.
- Node error classification preserves the local-resource category.

Local validation: final all-feature library suite **1,012 passed, 0 failed,
12 ignored** (65.16 s). Strict all-target/all-feature Clippy passed (15.06 s),
formatting and diff checks passed. The preceding source's CI run 35190310619
finished with Windows timeout failures in three Header/standby tests; Linux
(including 90% coverage) and supply-chain checks passed. The timeout failures
remain unresolved evidence, not a passing cross-platform result for this change.

Scope still missing: preparation before serialization, prefetch/version maps,
script queues, indexed `Keep` undo results, optional MDBX/snapshot-overlay context
integration, record/worker metadata and full engine overhead. Library stores
outside a bound node directory keep their prior in-memory behavior. Journal and
write-back collectors still retain decoded batch data (now charged on this
path); resumable scheduling and headroom planning must ensure progress under
contention. Spool bytes are independent of Header admission and are not a
physical whole-node disk quota. The codec currently encodes/reads a complete
single record under its allowance, not a constant-size streaming buffer.

No measured RSS improvement, maintenance acceptance, final-source historical
replay or public soak is claimed. The added disk/serialization cost must be
measured with the final execution path. Existing failed storage results remain.
