# Header work admission and cancellation (2026-09-17)

Overall Header, admission and whole-node resource gates remain **OPEN**.

The process-shared Header work pool previously made every asynchronous consumer
sleep and race for credits again. It had no ordering between waiters, allowed
synchronous callers to bypass them, and did not wake sleeping consumers when an
unused lease returned credits. A busy consumer could repeatedly beat an older
waiter even though total token accounting remained bounded.

An asynchronous FIFO mutex now owns only the admission position, never the live
work lease. The head waits for either monotonic refill or a returned-credit
notification. Later asynchronous consumers queue; synchronous admission defers
while that queue owns the head. Cancelling either the head or a queued future
removes its position without reserving or leaking credits. The actual pool is
still shared across consumers, and failed work is still charged. Burst capacity,
refill rate and per-batch work limits are unchanged.

Standby Header admission now also selects on its activation channel. Activation
cancels a pending work request and returns the connection without waiting for
credits; no Header mutation has begun at that boundary. An active session can
request any unprocessed headers again. This applies before locator construction
and after receiving headers, before validation admission.

Deterministic tests poll isolated scheduler futures into known queue positions.
They verify FIFO ordering even after a full refund, no synchronous bypass,
cancellation of head and middle waiters, immediate progress after refund, and
standby activation/closed-channel cleanup without lost queue positions. The
existing monotonic refill and failed-work charging test remains in place.

This is work admission fairness, not a complete multi-candidate scheduler.
Synchronous deferrals still need resumable ownership, byte/disk budgets remain
separate, and network keepalive during long waits needs whole-node verification.
The scheduler remains process-shared rather than a separately configured owner
for each embedded node. No global-memory, physical-disk, sustained RSS or public
soak gate is closed by this change.

Prior source 50d1101 CI run 35190310619 failed three Windows Header/standby
wall-clock waits (20 s, 2 s, 2 s); Linux tests/coverage and supply-chain passed.
The ordering defect above is established by code and tests, but is **not proven
to be the cause** of those timeouts. They remain unresolved acceptance evidence;
no timeout or resource threshold was increased.

The test-only standby adapter also now creates a private temporary parent for
its seed. Previously independent test fixtures created scratch indexes directly
under the OS temporary directory and therefore contended for that directory's
scratch-GC parent lock. The fixture owner stays alive through the asynchronous
standby call. Production scratch ownership and the original 2-second assertions
are unchanged. This removes established cross-test coupling without claiming
the remaining Windows recovery timeout is solved.

Local all-feature library validation before the final test-fixture isolation:
**1,015 passed, 0 failed, 12 ignored**, 68.02 s. Four focused scheduler tests passed.
Final fixture validation: all 13 standby-filter tests passed (0.63 s), including
both previously timed-out standby assertions on this Mac. Final strict
all-target/all-feature Clippy passed (9.47 s); formatting and diff checks passed.
This does not substitute for the pending Windows run.
