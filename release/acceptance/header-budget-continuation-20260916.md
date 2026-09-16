# Header batch budgets and Mac resource continuation

Status: partial implementation and scoped measurements. `header-resources`
remains **open**. This work starts from `2b56c5a8007e5bed9877b3672302017b4d19bd73`.

## Changes

- Contextual batch staging checks a 1 MiB allowance for its accepted-info and
  rollback-hash vectors before either is allocated. Caller input, retained DAG
  tables, active-chain vectors, serving projections and allocator overhead are
  outside this particular allowance.
- A consumable validation-work ledger reserves bounded difficulty/MTP work,
  index growth, reorg ancestry traversal and rollback work before mutation.
  Default staging calls receive 32,000,000 units. Explicit callers can reuse one
  ledger across batches; failed validation and rollback never refund work.
  Units are conservative logical work charges, not CPU cycles. Production
  defaults remain per batch, not a shared node-wide rate limiter.
- Resource exhaustion remains a local deferral. It cannot make a valid header
  peer-invalid or publish a partially accepted batch.
- The header redb page cache is explicitly 64 MiB rather than the engine's
  default 1 GiB. This bounds the configured page cache, not redb repair buffers,
  transactions, the retained DAG, OS caches or whole-process startup RSS.
- The resource probe measures RSS on Mac as well as Linux, reports allocated
  disk bytes separately from file length, supports repeated unique sibling
  churn, and verifies both same-process and fresh-process reopen. Missing RSS
  fails the run. A fresh process does not imply cold OS/device caches.
- `scripts/check-header-resource-probe.py` checks duration, sampling gaps,
  retention counts, independent reopen identity, absolute RSS/disk allowances
  and median growth between the middle and final thirds. Its output explicitly
  excludes production-gate closure. Phase samples can miss transient peaks.

## Verification

Mac all-feature library suite: **942 passed, 0 failed, 11 ignored**. Strict
all-target/all-feature Clippy passed. Four new Rust regressions cover byte
boundary rejection, exact-fit acceptance, budget consumption across failed
attempts, rollback after partial insertion and reorg deferral/retry. Five Python
checker regressions reject incomplete/short evidence, missing RSS, sampling
holes, false independent reopens, resource growth and cap violations.

## Mac measurements

Raw logs, complete test/Clippy output, executable/source SHA-256 identity and
checker results are under `target/header-continuation-20260916/`. Measurements
use a default-feature release executable with mimalloc. No Linux tasks were
started, and the probe removes its own temporary databases after verification.

The retained workload uses 2,500 active headers, 2,000-header insertion batches
and a 50,000-side-header target. It ran **302.048468 seconds** and processed
**8,800,000 unique valid sibling headers**, with 8,804 resource samples including
reopens. Retention stayed at 50,000 after maintenance (52,000 before maintenance).

- Maximum sampled RSS: **110,804,992 bytes**; `/usr/bin/time -l` maximum RSS:
  **110,837,760 bytes**.
- Maximum sampled database file length: **67,907,584 bytes**. Disk allocation is
  sampled separately; neither metric measures total physical write volume.
- Fresh-process reopen endpoint RSS: **43,712,512 bytes**; retained count and
  selected tip match. This endpoint is not a startup high-water mark.
- Final-third / middle-third median ratios: RSS **1.061621**, file length
  **1.0**, allocated disk **1.0**. The remaining RSS increase matters: this
  does not prove indefinite convergence.
- The explicitly scoped 300-second check passed with 256 MiB sampled-RSS and
  128 MiB sampled-disk allowances and a 1.10 median-growth ceiling. The same
  evidence **failed the checker's default 3,600-second requirement**, as it
  should. Neither result accepts the whole-node production gate.

Reproduction (after building the example in release mode):

```sh
RBTC_HEADER_PROBE_SECONDS=300 /usr/bin/time -l \
  target/release/examples/header_resource_probe retained 2500 100000 \
  > retained.jsonl 2> retained.time
python3 scripts/check-header-resource-probe.py retained.jsonl \
  --minimum-seconds 300 --max-rss-mib 256 --max-disk-mib 128
```

The separate **1,000,000-active-header** workload also completed and preserved
its selected tip and retained count across both reopens:

| Phase | RSS at phase endpoint, bytes |
| --- | ---: |
| DAG plus active serving projection after generation | 1,316,487,168 |
| Same-process reopen while retaining serving projection | 1,790,492,672 |
| Fresh process before materializing the DAG | 6,127,616 |
| Fresh process after validated replay | 911,278,080 |

The fresh replay took about **6.02 seconds**, including opening the store.
Database length was **539,504,640 bytes**; sampled allocated disk reached
**372,043,776 bytes**. These are finite workload observations, not hard startup
memory guarantees. In particular the same-process reopen retains the serving
projection and allocator history; it must not be mislabeled fresh startup.
Raw `/usr/bin/time -l` output also preserves the run's high-water measurement.

## Outstanding architecture and acceptance

1. A disk-backed long-fork candidate must retain a durable cursor and bounded
   contextual validation state. Raw spooling alone is insufficient: MTP,
   retarget ancestors, checkpoints and cumulative work must remain validated.
   A winning candidate must reach execution/reorg without rematerializing an
   unbounded second DAG. Interrupted promotion must recover atomically.
2. Node-wide byte and work accounting must cover reconnects, concurrent peers,
   local submissions, persistence, projections and retry scheduling. The new
   per-batch limits are only one component. The existing 2,000,000-entry ceiling
   still defers a valid long fork; this continuation does not remove that gap.
3. Startup still reconstructs retained history. The smaller database cache is
   not a total startup memory bound or an out-of-core loading policy.
4. Whole-node sustained RSS/disk acceptance still needs multi-peer/local churn,
   deep forks, block execution, cancellation/write failure and crash recovery.
   A five-minute sibling kernel workload cannot substitute for those scenarios
   or for the separate seven-day public-network gate.
