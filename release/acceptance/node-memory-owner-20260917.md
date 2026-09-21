# Shared node memory owner (2026-09-17)

Overall gate: **OPEN**. These changes establish aggregate reservation ownership;
they do not establish a process RSS ceiling or complete startup memory bound.

## Implementation

- A runtime owns one `MemoryBudget` and one admission ledger. Active/background
  pipelines and reconstructed sessions share them. Candidate leases also debit
  the aggregate allowance; a failed aggregate reservation leaves candidate
  counters unchanged and returns local resource deferral.
- Before dispatching startup/offline actions, bind the data, validation,
  finalization, reindex output and explicit Header directories to this owner.
  Directory registration is atomic, resolves existing aliases and permits new
  output paths without creating files. An escaped database/lease keeps its owner
  alive and prevents another runtime resetting that directory's allowance.
- Chainstate redb caches reserve before opening/creating the engine. A wrapper
  around redb's own public FileBackend preserves platform locking/I/O and holds
  the lease for the engine lifetime, including escaped read transactions.
  Headers retain their existing independent cache/disk limits and also reserve
  caches and in-flight read buffers against the node ledger. Registered
  chainstate reads also reserve their in-flight buffer before reading.
- CLI `--memory-budget-bytes`, config `memory_budget_bytes`, and the typed
  resource config expose the same 16 GiB default (accepted range 1 GiB–1 TiB).
  Startup rejects known simultaneous chainstate cache plans that leave less
  than 1 GiB for Headers/candidates. Explicit cache values are never silently
  shrunk. This is a lower-bound preflight, not complete allocation accounting.
- Default background caches become 4 GiB each (8 GiB combined); bulk validation
  defaults to 8 GiB. Active serving stays at 1 GiB. Performance needs rechecking
  on final-source historical replay.
- Status JSON exposes `memory_reservations` with limit/used/peak; startup logs
  report the configured limit. Reservation counters must not be labeled RSS.

## Validation

- All-feature library suite: 993 passed, 0 failed, 12 ignored (67.15 s).
- A subsequent additional regression plus the other three node memory tests:
  4 passed. Covers background double-cache planning and status usage/release.
- External embedded-node API suite: 7 passed, including two isolated nodes in
  one runtime and graceful host shutdown. Formatting/diff checks passed.
- Strict all-target/all-feature Clippy passed after correcting two field names.
- Engine regression holds a read transaction after dropping Database, verifies
  a second cache cannot bypass the allowance or create its file, then releases
  the read transaction and successfully opens the second database.
- Cross-subsystem test uses actual Headers and chainstate databases plus an
  admission candidate against one small budget. Further tests cover arithmetic
  overflow, unwind, atomic directory conflicts, escaped leases, failed/locked
  database opens, config/CLI override and duplicate options, and rejection before
  creating the configured data directory.

## Remaining scope

Other database caches, executor input/prepared/transition/undo ownership,
prevout and escaping queues, MDBX dirty pages, redb MVCC/dirty pages, mmap and
allocator overhead are not yet all covered. Read buffer ownership after return
relies on the separately reserved engine cache; this is not an independent
proof of every redb allocation. Startup headroom is not a measured RSS reserve.
Huge candidate fairness/resumable work and comprehensive disk accounting remain.

Historical MDBX maintenance RSS failure and both reduced failed measurements
remain unchanged. No whole-node resource acceptance or seven-day public soak
was run for this source. Final-source optimizer/history/release evidence remains
outstanding. Do not close any overall gate from these unit tests.
