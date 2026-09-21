# AS-map startup memory admission (2026-09-17)

Total startup memory, RSS, disk and production acceptance: **OPEN**.

Node file and embedded AS-map loads now reserve payload bytes plus 64 KiB of
validation headroom from the shared node budget before allocating. Once the
bounded validation stack is gone, the lease shrinks to payload capacity plus
128 bytes of object/Arc allowance. `Asmap` owns the payload before its lease
in destruction order, and `Arc` views keep both alive until the final drop.
Insufficient allowance and malformed data fail locally and refund temporary
reservations. AS-map validation still precedes the peer-store existence check.

File loads use one opened handle for metadata and data, retain the existing
16 MiB ceiling, and read exactly the observed size into a fixed buffer followed
by a one-byte EOF probe. Replacing the path cannot redirect the read; growth
cannot expand its allocation. Truncation or detected growth fails closed.
Standalone unbudgeted library loads gain the bounded read too. Node embedded
loads use their own admitted bytes rather than the library's process-global
compatibility cache. This means validation/copying occurs per node load; final
startup/retry throughput still needs acceptance on the frozen source.

Regressions cover denied admission before any reader access; file growth and
truncation; invalid structural data; sparse oversized files; failed-open
refunds; a live shared view retaining its charge; pressure while that view is
alive; and the embedded map's admission, lookup equivalence and final refund.

Remaining startup collections and MTP tables, snapshot/index building and
rebase materialization, journal historical read plans, worker/prepared/script
queue allocations and allocator/stack/engine residency remain separate gaps.
No process RSS cap, maintenance acceptance, final-source optimizer/history
acceptance or long-node/public soak is claimed by this change.

Validation: all-feature library suite 1,031 passed, 0 failed, 12 ignored
(69.75 s); all seven embedded-node API integration tests passed (3.26 s).
Strict all-target/all-feature Clippy passed (25.97 s including build-lock wait),
and formatting/diff checks passed. CI 35198913385 for the preceding source
396b045 was still in progress; it is not acceptance evidence for this change.
