# One Header allowance for active and background validation

Starting the active assumed chain alongside genesis validation now binds both
canonical data directories to one Header usage ledger before either peer pool
starts. Configured Header cache/read reservations and registered database/journal
file lengths compete for the same 512 MiB/16 GiB allowances; creating the second
pipeline no longer doubles them. Both startup inventories are loaded and their
combined file count/length checked before publishing the binding.

Each directory retains its own bounded relative-path inventory and exclusive
owner lock. Shared registrations save only paths belonging to that directory.
Nested/equal directories and already-live independent ledgers cannot be rebound.
Both spawned pipelines retain a group guard, so outer-future cancellation does
not hand a still-running child a fresh allowance. The caller also retains the
group through finalization. The group tests verify combined exhaustion,
separate catalogs, restart totals, refused oversized combined startup and guard
retention after the original owner is dropped.

Mac full-library regression passed 979 tests, with 0 failures and 12 ignored,
in 63.98 seconds. The final lifecycle adjustment explicitly drops each child
reference after its peer-pool future completes; the real active-chain plus
genesis-validator finalization regression is rerun after that adjustment.
Strict all-feature/all-target Clippy and formatting/diff results are saved with
the checkpoint evidence.

This is aggregate Header admission for the concurrent background mode, not a
total-node memory ceiling. Execution caches still use separate per-pipeline
settings; a misleading old cache-selection test name was corrected to reflect
what it actually proves. Admission candidates, validation metadata/windows,
block bodies, execution/maintenance and allocator overhead remain outside this
Header ledger. Offline source/output directory pairs are not yet grouped here.
Physical disk and sustained whole-node RSS acceptance, Windows lock/cleanup
execution, final-source optimizer/history verification and public soak remain
unproven. No production gate is marked closed by this change.
