# Persistent Header file accounting and journal admission

Closing a header database or candidate journal no longer releases its logical
file bytes. A bounded `.rbtc-header-files.json` inventory records relative
registered paths; lengths are reconstructed from files when a directory budget
is recreated. Reopening a known file does not double charge it. Registration
retires entries only after observing deletion. Existing oversized files remain
charged even when opening them is refused.

The inventory is limited to 4,096 paths and 1 MiB of encoded data. It is written
by atomic replacement after syncing the new file; Unix directory entries are
also synced. Paths must stay inside the canonical budget directory, and linked
files or ancestor directories are refused. A permanent exclusive
`.rbtc-header-budget.lock` prevents independent processes from creating separate
ledgers for the same directory while any database, read view or journal owns
that budget. These metadata names are explicitly recognized by completed
validation-directory cleanup, which retains its link checks.

Candidate journals now share the same 16 GiB registered-file allowance as raw
header databases and derived indexes. The initial identity and every complete
frame reserve growth before writing. Successful recovery/rollback truncation
releases the removed tail; interrupted writes retain conservative reservations
until actual length can be observed on reopen. Per-candidate batch, work and
128 MiB journal limits remain in force.

Tests exercise closing and recreating the entire ledger, actual file deletion,
reopen without duplicate charges, database/journal sharing, oversized existing
files, escaped paths, oversized catalogs and symlink refusal. Existing tests
continue to exercise transactional growth failure, old read-version ownership,
restart promotion and real scratch-owner process termination. Small logs and
source hashes accompany the checkpoint. The Mac full suite passed 978 tests,
with 0 failures and 12 ignored (65.56 seconds). After tightening registration
of already-existing paths and bounding catalog staging to a fixed filename,
the eight inventory/budget tests were rerun. Strict all-feature/all-target
Clippy, formatting and diff checks passed on the final change.

The total production gate remains open. This accounts registered Header file
lengths, not filesystem metadata, all physical allocation, unregistered legacy
artifacts or other node databases. Background directories still have separate
budgets. Journal buffers and consensus windows do not yet share a complete node
memory allowance, and configuration/status exposure remains unfinished. Catalog replacement uses a fixed staging name, so a crash can leave at most
one bounded staging catalog per directory; the next registration replaces it. No whole-node RSS, power-loss or public-soak acceptance is claimed.
