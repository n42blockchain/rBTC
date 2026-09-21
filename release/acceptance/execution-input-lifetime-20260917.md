# Execution input lifetime before atomic commit

The batch executor previously kept its prefetched base UTXOs alive through
transition construction and commit. It also cloned each prepared delta's
surviving UTXOs, including script buffers, into the transition vectors while
the source delta was still alive. Deferred script work could overlap this phase.

After all preparation workers join, the executor now drops the version index
and base prefetch cache. It drains script work before constructing transitions,
so script-owned inputs finish before that next allocation phase. PreparedDelta
is consumed into net changes, moving script buffers instead of cloning them.
Per-block ordering, earliest-error selection and the final atomic commit are
unchanged. Waiting for scripts earlier can reduce overlap with transition
construction; throughput effects remain to be measured on final-source replay.

The new ownership regression checks that the original script allocation is
transferred, while base spends and outputs created/spent within the block retain
the same net semantics. Existing script-failure, undo and recovery tests pass.
Mac validation: full all-feature library suite 985 passed, 0 failed, 12 ignored
(66.91 seconds); strict all-target/all-feature Clippy, format and diff checks pass.

This does not bound the complete batch: phase-one versions, prepared results,
undo, result vectors, caller-owned blocks and engine pages still need aggregate
admission and potentially spill storage. The synthetic MDBX driver does not
exercise this executor, so its earlier RSS ratios cannot measure this change.
The borrowed 2.223 and owned 2.161 reduced failures remain failed; neither run
triggered maintenance. All resource and sustained whole-node gates remain open.
