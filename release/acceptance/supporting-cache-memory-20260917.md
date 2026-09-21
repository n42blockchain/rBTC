# Supporting database memory admission (2026-09-17)

Overall memory/startup/production gates remain **OPEN**.

The node memory ledger now covers configured caches for the supporting redb
stores: peers, mempool persistence, fee estimation, explorer, wallet rebroadcast,
optional indexes, standalone UTXO/execution/undo stores and redb snapshot overlay.
Their existing 1 GiB defaults are preserved. Library stores outside registered
node directories retain independent behavior. Shared handles/read transactions
retain the engine's reservation as established by the node memory owner change.

Maintenance `compact_file` and existing redb overlay identity/audit opens also
use that owner. The adapter opens without file creation, takes redb FileBackend's
platform lock, and rejects an empty file before calling create_with_backend.
This matches vendored redb 2.6's exact open/create distinction in
`page_manager.rs`: only an empty file may be initialized by create. Missing and
empty existing-open inputs remain errors, and failed opens refund reservations.
No vendored source change or relaxed integrity check was needed.

Startup preflight now includes supporting engine caches: peer per pipeline,
active mempool and estimator, optional indexes per pipeline, and enabled
explorer/wallet rebroadcast stores. With current defaults, a basic serving plan
is 5 GiB; dual background is 13 GiB; dual background with all three indexes is
19 GiB before explorer/wallet. These are configured reservation plans including
1 GiB Header/candidate headroom, not measured RSS minima. The 16 GiB default
therefore rejects oversized optional configurations until explicitly increased.
Offline preflight is conservative; runtime opens still enforce the shared cap.

The prior ac3fe74 Windows CI run 35187130337 failed strict Clippy because the
CLI future reached 16,464 bytes on that target. The production CLI now boxes its
node-running future inside the existing select, reducing outer future stack
size while preserving cancellation and shutdown. No lint suppression was added.
Windows verification requires the next source-bound CI run.

Validation:

- All-feature library suite: 996 passed, 0 failed, 12 ignored, 63.67 s.
- After CLI boxing, the CLI side-effect-free configuration/version regression
  passed. It confirms config checks still do not create the data directory.
- Strict all-target/all-feature Clippy and formatting/diff checks passed on Mac.
- New real supporting-store regression holds peers' cache, rejects fee-estimator
  creation before a file appears, then releases peers and successfully opens
  the estimator against the same allowance.
- Existing-open regression covers missing/empty files, nonmutation, refunded
  reservations, and a successful existing database open/release.

Remaining work includes engine dirty/MVCC pages, MDBX, SQLite, mmap, executor
prepared/transition/undo batches, prevout/escaping allocations, complete startup
memory accounting and whole-node RSS acceptance. All historical maintenance RSS
failures remain open. No public soak or final-source replay was run here.
