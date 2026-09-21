# Transition reservation lifetime (2026-09-17)

Overall memory/spooling/production gates: **OPEN**.

A fallible transition source cannot release its decoding reservation when it
returns a value: some stores buffer that value beyond the method call. The
stream API now takes `LeasedConnectTransition`. The wrapper owns an optional
memory lease, and its field order destroys the payload before returning bytes.
The producer must reserve before constructing/decoding the transition. Conversion
from the old caller-owned transition explicitly carries no reservation; the
current executor still uses that conversion. This change does **not** claim the
executor's existing preparation allocations are now budgeted.

Normal redb and MDBX consume the wrapper directly and release one block's lease
before requesting another. A journal/overlay compatibility collection retains
all leases alongside its payloads, with explicit payload-before-lease drop order.
Unknown trait implementations reject leased sources before mutation unless they
override the method, since the default cannot know whether an implementation
retains its owned inputs after returning. Unleased compatibility remains.

Write-back has an explicit override. Leases follow pending and in-flight folded
state, including failed-but-readable state, rather than the submission stack.
The background sorting copy reserves an additional allowance before cloning
coins. Failed thread spawning moves its unpublished state back into pending
instead of cloning the entire state. Successful flush releases reservations;
failed flush retains its payload and reservations until the store is discarded.
This covers caller-supplied transition estimates, not all write-back metadata or
queries returning independent copies.

Validation:

- All-feature library suite: 1,000 passed, 0 failed, 12 ignored, 67.99 s.
- Three added write-back regressions passed: late source failure leaves no
  buffered prefix and refunds leases; failed spawn preserves readable state and
  its reservation before a successful retry; copy-budget exhaustion leaves the
  underlying tip unchanged and keeps failed readable state charged until drop.
- Two engine regressions also passed with real leases: streaming normal redb and
  MDBX peak at one lease, journal collection peaks at two, and all release after
  success while retaining the existing late-source/endpoint rollback checks.
- Strict all-target/all-feature Clippy, formatting and diff checks passed.

No disk spool exists yet. Needed next: reserve before decoding/generation,
bounded serialization, shared disk allowance and file cleanup/recovery, bounded
phase-two result storage, and bounded applied-undo readback. The write-back copy
path currently fails closed when its extra allowance is unavailable; scheduling
must ensure resumable progress instead of treating this as a complete resource
solution. No RSS acceptance or soak was run, and earlier failures remain open.
