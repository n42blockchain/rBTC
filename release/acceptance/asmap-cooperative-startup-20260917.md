# Cooperative embedded AS-map validation (2026-09-17)

Complete startup bounds, platform acceptance and overall production gates: **OPEN**.

CI 35200356452/b5d80b7 passed Windows and ordinary Linux tests but failed the Linux
coverage run at the embedded ZMQ test's original three-second shutdown deadline.
The same shutdown failure reproduced locally on 0aa971f under cargo llvm-cov,
with a second failure at the existing five-second block notification deadline
(5 passed, 2 failed, 13.72 s). These failed logs are retained.

The node task polls its run future and host shutdown receiver in one select.
block_in_place releases the runtime worker for listener tasks, but does not make
that parent future yield during synchronous AS-map validation. Embedded map
validation now advances a retained validation cursor by at most 1,024 instructions
before yielding. The original validation rules remain shared with the synchronous
library API. The parent can cancel validation without a detached task; payload,
validation stack and memory allowance remain in the owned future.

Cooperative validation alone repaired the local shutdown failure but still left
the block-notification test failing (6 passed, 1 failed, 7.42 s). Multiple nodes
were still repeating validation of the same immutable compiled bytes. A mutex
now serializes first validation and retains only its completed boolean result.
Cancellation unlocks without marking incomplete validation as valid. Subsequent
nodes skip the repeated CPU pass but each still admits and owns its own bytes;
no process-global payload is introduced by this node path. Operator-file
validation and synchronous database initialization remain outside this change.

A deterministic unit test polls validation once, observes Pending with the
payload charged, drops the future and verifies exact refund plus an unset,
unlocked validation cache. It then completes validation, checks a known ASN,
verifies a second load uses cached validation with a distinct Arc allocation,
and checks final refund. Existing malformed-map and address interpretation tests
exercise the shared checker.

The final local coverage embedded API run passed all 7 tests in 4.24 s with
unchanged startup, greeting, message and shutdown deadlines. During intermediate
validation a mock block peer panicked on expected disconnect after shutdown;
that fixture now accepts EOF/reset and is joined at test completion, so its
unexpected failures cannot remain detached. No production error is suppressed.
The controlled local regression and repair support this startup mechanism; they
do not prove every historical timing failure has the same cause or establish a
whole-node RSS/latency ceiling. Final-source platform CI remains required.

Final local validation: all-feature library suite 1,039 passed, 0 failed,
12 ignored (74.31 s); final strict all-target/all-feature Clippy passed (0.51 s,
incremental after the joined-peer fixture change). Formatting/diff checks passed.
CI 35201897125/0aa971f is still running and does not contain this change.
