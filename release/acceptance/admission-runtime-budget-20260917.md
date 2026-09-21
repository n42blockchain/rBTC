# Runtime admission budget ownership

The peer-pool runtime now owns one AdmissionBudget outside its session retry
loop. Each replacement transaction pool receives that same ledger, so reconnects
cannot replenish work tokens or forget candidate-memory leases retained by old
sessions. Active-chain and background-genesis peer pools also share one ledger.
Normal transaction candidate publication preserves the injected ledger.

Header storage directory owners now have separate pipeline lifetimes while
retaining the common usage ledger. A completed validator drops its directory
owner before publishing completion; the active pipeline retains its own owner.
This permits validation-directory quarantine on Windows while the active node
continues. It also preserves ownership if the spawning future is cancelled.

Regression coverage checks exhausted work across session reconstruction,
outstanding memory leases and simultaneous background reservations, plus actual
validation-directory rename/removal while the active directory remains locked.
The existing concurrent active/genesis runtime test exercises finalization.

CI 35182180351 (403383b) passed Linux tests, the 90% line coverage requirement,
and supply-chain checks. Its Windows test failed validation-directory quarantine
with error 5, motivating the owner-lifetime correction here. That failure remains
historical evidence; the correction still requires a new Windows CI result.

This does not close the aggregate admission or node resource gates. Header,
admission, execution, engine and escaped allocations still need unified total
memory admission; resumable candidate scheduling and startup/RSS/physical-disk
acceptance remain open. No long public-network soak was started.

Mac validation: `cargo test --locked --all-features --lib` passed 981 tests
(0 failed, 12 ignored, 73.08 seconds). Strict all-target/all-feature Clippy,
format and diff checks passed. Ignored acceptance workloads were not run.
