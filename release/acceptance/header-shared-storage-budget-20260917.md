# Shared live-header storage admission

The raw header store, primary derived index, shared standby seed and private
standby indexes now use a safe Rust redb `StorageBackend` that reserves bytes
before allocation or file growth. Budgets are shared by canonical parent data
directory, including scratch indexes created below that directory. Current
limits are 512 MiB for configured engine caches plus in-flight read buffers,
and 16 GiB for the combined logical lengths of open header database files.

Cache admission precedes opening the file and constructing the engine. Existing
file length is charged before engine recovery. Resizing reserves growth before
`set_len`; writing beyond the already reserved file length is rejected. A
successful shrink releases its surplus; failed resizing conservatively retains
its reservation. Read ranges and remaining memory allowance are checked before
buffer allocation, and allocation failure propagates as a local error. Returned
read buffers pass to redb, whose configured cache is reserved separately; this
is not exact accounting of every engine allocation or allocator overhead.

The backend holds its reservation through redb's lifetime, including immutable
read versions that survive writer drop. Exclusive file locks remain in place.
The implementation serializes file operations with a mutex and uses `sync_all`
for durability, including requests that permit eventual synchronization. Its
throughput and latency still need measurement in the whole-node workload.

Mac validation: 90 Header-selected tests passed before adding read-buffer
admission; the resulting production code passed 972 library tests, with zero
failures and 12 ignored, in 70.12 seconds. Three budget tests then passed,
including one added after the full run: shared cache/file exhaustion rejects
before mutation; old read transactions retain reservations after writer exit;
and real redb growth exhaustion stays below the file limit and reopens at
exactly the last committed version after allowance is restored. Strict
all-feature/all-target Clippy, formatting and diff checks passed.

**The production resource gate remains open.** This ledger tracks live files,
not closed files, filesystem metadata, orphan inventory or physical disk quota.
All files closing can release the directory ledger while their persistent data
remains on disk. Candidate journals are not yet included. Other directories,
background validators, admission/execution caches, validation metadata and old
MVCC page growth require total-node coordination. Limits are not yet operator
configuration or status metrics. Startup RSS and sustained whole-node disk/RSS
acceptance remain unproven; no prior measurement is rebound to this backend.
