# Snapshot header ownership and finalization caches

Snapshot activation (rBTC and Core 31) and standalone/completion AssumeUTXO
finalization now replay raw headers into the bounded derived disk index and pin
an immutable disk view. They no longer call the full-history HeaderDag loader.
The shared loader first recovers a pending durable header promotion, charges
replay work through the existing shared Header work pool, and yields between
bounded frames. Standby peers use that same loader.

Snapshot verification/import APIs now accept fallible HeaderView providers.
Header I/O errors propagate as local HeaderRead errors rather than becoming
anchor mismatch or missing creation-header conclusions. Tests inject lookup
failure before rBTC snapshot activation and verify the execution tip, assumed
marker and UTXOs remain unchanged. Core anchor, indexed coin and streaming coin
reads also preserve the local error. This changes neither trust anchors nor
content commitments.

Snapshot activation now honors the configured active-chain cache. Standalone
finalization uses that cache for each of its two stores, replacing two hardcoded
16 GiB cache settings (default combined setting is now 2 GiB). Live background
finalization uses the configured background cache for its validation store,
replacing the hardcoded 16 GiB setting. These are redb cache allowances, not an
RSS bound; simultaneous caches still require admission to one node budget.

The production total-memory and Headers gates remain open. Offline reindex,
verify and related paths still contain full-history loads. Execution batches,
engine dirty/MVCC pages, snapshot indexes, mempool and escaped allocations need
aggregate reservations and measured whole-node acceptance. Maintenance RSS,
final-source optimizer/history checks and the required public soak are not
closed by this change.

Mac validation: full all-feature library tests passed (982 passed, 0 failed,
12 ignored; 65.93 seconds). Strict all-target/all-feature Clippy and format/diff
checks passed. No large-history RSS or disk acceptance was run in this checkpoint.

Follow-up: [offline header migration](offline-header-memory-20260917.md) removes
the remaining production node reindex/verification DAG loads described above.
