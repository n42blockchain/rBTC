# Header resync: retain the validated DAG between polls

Measured 2026-09-11 after baseline `fdf3cbfdca31102fc3af169f373a70138b627d74`.
This closes repeated durable-header replay and replacement-DAG allocation on
ordinary within-session polls. Side-chain retention, candidate recovery and
durable eviction budgets remain open.

## Reproduced work and implementation

The caught-up serving loop called `sync_headers` on every poll. That function
opened the header store and rebuilt all retained active and competing headers,
even if the peer returned no new data. The old DAG remained alive until its
replacement returned. The ordinary poll interval is 30 seconds, and the
background-validation path can poll every second.

The new regression seeds two active headers and 1,000 valid competing siblings,
then sends eight alternating empty/duplicate responses over real loopback V1
sessions. With the old replay behavior it observes **8,016 historical header
validations**; the current path observes **zero**, with every retained header
and the active tip unchanged. The first failing run introduced the ownership
argument and instrumentation while still ignoring that argument and executing
the old unconditional replay. Its failure log is retained.

`sync_headers` now accepts ownership of the existing DAG from the serving loop.
Startup, headers-only mode, snapshot-overlay initialization and peer failover
pass no existing DAG and still replay the durable record. Within a serving
session, the loop transfers its DAG into the next poll and receives it back.
This transfer preserves the deployment cache and all competing-branch context
without cloning header entries.

Before reusing a DAG, the function checks that its complete deployment
configuration agrees and its non-genesis entry count matches durable metadata.
A mismatch is a local-resource error before any header request, without a
peer-invalid classification. Reuse relies on the existing single-owner serving
loop, serialized peer/local header writes and unchanged database path. The
count check is a structural guard, not an identity digest of database contents.

New headers still take the same path: validate the unseen contiguous suffix,
hold a rollback guard, append the batch atomically, then commit the guard.
Only a successful synchronization publishes the refreshed active-only serving
projection. On cancellation or a peer error the owned working DAG is dropped;
completed durable batches remain available to the next startup/failover replay.
No new resource limit or consensus rejection threshold is introduced here.

## Regression coverage

Five new ordinary tests exercise the production synchronization function:

- Empty/duplicate polls retain all forks and perform no historical replay.
- A locally submitted block is retained across a poll; a previously known
  competing fork becomes strongest, updates the serving projection and reopens
  with the same cumulative-work tip and losing local header.
- A valid header followed by an invalid timestamp in one response persists
  neither header; reopening returns the original tip.
- Cancellation while waiting for the next response preserves the previously
  committed 2,000-header batch and releases the store so restart can reopen it.
- Durable-count or deployment mismatches are local errors, produce no header
  request and perform no historical replay.

The final all-feature run passed **936 tests** (897 library + 39 integration),
with **32 ignored** and no failures. Subprocess-helper results are not counted
twice. Both explicitly enabled live Core 31 BIP34 and BIP66/BIP65 header-boundary
differentials passed. Strict all-target/all-feature Clippy, formatting and diff
checks passed. The full suite used four test threads and did not reproduce the
earlier failover-test nonce mismatch; its cause remains a separate open item.
Only documentation changed after final source validation and measurements.

## Repeated resource comparison

The ignored `node::tests::header_resync::header_resync_resource_probe` exercises
the same production function with 2,501 active entries plus 50,000/100,000 valid
regtest siblings. After initial replay, a loopback V1 peer answers eight polls
with empty header lists. Both modes assert identical locators, tip and retained
entry counts after every poll.

`reload` keeps the old DAG alive while the startup path builds its replacement,
reproducing the previous serving-loop assignment. `reuse` transfers the existing
DAG exactly as the current serving loop does. Both modes run in the **same final
release test executable**, avoiding differences in compiler or fixture code.
This is an algorithm-path comparison, not two independently built revisions.
The test-only replay counter is enabled in both modes.

Each mode/size runs in three fresh processes, alternating reload/reuse order.
Rust 1.85.0, all features and the system allocator are used. The library test
executable has no global mimalloc selection; these RSS values therefore do not
represent the daemon's mimalloc allocator. Redb is on `/tmp` (tmpfs), with warm
kernel/store state. No other builds or tests run during the measurement series.

| Measurement, median of three | 50k reload | 50k reuse | 100k reload | 100k reuse |
| --- | ---: | ---: | ---: | ---: |
| Eight empty polls, ms | 988.440 | 7.995 | 2,246.737 | 10.882 |
| Historical headers replayed | 420,000 | 0 | 820,000 | 0 |
| Whole-process peak RSS, KiB | 69,640 | 51,016 | 131,440 | 83,424 |
| Database file bytes | 34,222,080 | 34,222,080 | 67,907,584 | 67,907,584 |

At 100,000 siblings, reload took 2,086.195–2,306.082 ms and reuse took
10.802–11.037 ms for the eight polls. Peak RSS ranged from 120,784–139,064 KiB
for reload and 82,960–88,472 KiB for reuse. Idle RSS after the final poll had
similar medians (83,412/83,424 KiB); allocator release makes that endpoint a poor
measure of the transient replacement graph. All runs retained 52,501/102,501
headers respectively, including every competing sibling.

RSS and peak RSS are process measurements, including fixture construction,
initial replay, database caches, transport buffers and the test runtime. Peak
RSS includes the entire process lifetime and cannot be assigned exclusively to
one DAG. The replay count directly measures the removed historical work.
Opening the store and allocating bounded response buffers still cost resources
on every poll. This does not establish a whole-node memory plateau.

## Reproduction and remaining work

```sh
export CARGO_HOME=/tmp/rbtc-cargo-home
cargo test --locked --all-features --lib header_resync_ -- --nocapture
cargo test --locked --release --all-features --lib --no-run
RBTC_HEADER_RESYNC_MODE=reload RBTC_HEADER_RESYNC_SIBLINGS=100000 \
  cargo test --locked --release --all-features --lib \
  node::tests::header_resync::header_resync_resource_probe -- --exact --ignored --nocapture
RBTC_HEADER_RESYNC_MODE=reuse RBTC_HEADER_RESYNC_SIBLINGS=100000 \
  cargo test --locked --release --all-features --lib \
  node::tests::header_resync::header_resync_resource_probe -- --exact --ignored --nocapture
cargo test --locked --all-features --no-fail-fast -- --test-threads=4
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo fmt --all --check
```

For process RSS/time measurements, run the resulting test executable directly
under `/usr/bin/time -v` with the same exact test selector, excluding Cargo.
Use the configured target directory if `CARGO_TARGET_DIR` is set.

Local evidence is under `target/upstream-followup/2026-09-11/header-resync/`:
the failing regression, focused/full checks, release build, twelve individual
probe logs/JSON/time reports, aggregated samples, environment/source hashes and
published-source manifest. The small smoke run is excluded from the comparison.

Primary retained side chains and the append-only store can still grow. Startup
and peer failover still reconstruct the full durable graph. Per-peer work
reservations, bounded stronger-fork recovery and durable eviction/restart bounds
require the remaining [header resource gate](UPSTREAM_HEADER_RESOURCE_GATE.md).
Aggregate transaction-admission budgets, the full optimizer, full storage scale
and the seven-day soak remain separate
[open items](UPSTREAM_OPEN_ITEMS_2026-09-11.md).
