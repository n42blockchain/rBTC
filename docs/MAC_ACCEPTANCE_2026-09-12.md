# Mac acceptance continuation, 2026-09-12

Latest implementation and completed Mac measurements: [2026-09-13 follow-up](MAC_FOLLOWUP_2026-09-13.md).
The dated findings below are historical; the six acceptance gates remain open.

Continuation of the audit at `ae73758`, in the independent
`audit/mac-acceptance-20260912` worktree. Read this together with
[AUDIT_2026-09-12.md](AUDIT_2026-09-12.md) and
[ACCEPTANCE_EXECUTION_2026-09-12.md](ACCEPTANCE_EXECUTION_2026-09-12.md).
The changes in this continuation affect test fixtures and diagnostics only.
They do not implement the three outstanding production resource/optimizer gates.

## Host and input readiness

- Apple M1 Max, 10 CPU cores, 64 GiB RAM; macOS 26.6.2, arm64.
- Internal Apple 4 TB SSD, APFS; no attached external physical disk detected.
- Full-run preflight measured 896,906,203,136 available bytes (835.31 GiB),
  exceeding the 443,240,624,948-byte (412.8 GiB) planning requirement.
  The machine was connected to AC power. These are point-in-time readings.
- Rust 1.85.0 native toolchain; Python 3.9.6; local Bitcoin Core 31.0.
- The default Core directory has no `blocks` directory. Searches found only
  small btcd `blk_0_to_14131.dat` fixtures, not a complete Core block corpus.
- A pre-existing `utxo-935000.dat` snapshot (8.7 GiB) and its 1.1 GiB index are
  present in `../rBTC-mainnet-assumeutxo-20260725`. They were inventoried, not
  reauthenticated by this continuation.
- Existing mainnet ledger indices cover 534,997–536,004 (background validation)
  and 964,277–965,243 (active node). Neither supplies the missing contiguous
  blocks after the height-935,000 snapshot. These are index declarations, not
  authenticated corpus manifests or full payload audits.

The Linux `/data` capacity reported in the audit remains valid as historical
host evidence. It is not a volume attached to this Mac. No Linux SSH target
was available in this session, so no claim of a new remote run is made.

## Reproduced Mac failures and fixes

At `ae73758`, the all-feature suite reported **941 passed / 2 failed /
33 ignored**. Unlike the Linux sandbox audit, sockets could be created and
bound. Both failures were caused by the shared failed-peer fixture's assumption
that a bound, unlistened socket immediately refuses incoming connections.
This Mac instead leaves those connects pending. The fixture's own five-second
regression and the name-wave fallback deadline reproduced the problem.

The replacement fixture retains a listener, accepts and immediately closes each
connection, and joins its worker on drop. It explicitly models a failed
transport/handshake. The test still checks exclusive ownership against another
listener, now also checks prompt EOF/reset and release on drop. A standard
thread supports synchronous HTTPS clients as well as current-thread Tokio.
Existing failover, privacy, fallback and nonce assertions were retained.
The all-feature rerun passed **943 tests, 0 failed, 33 ignored**.

The first live Core run passed all nine block comparisons and three of four
replacement comparisons. The sponsored-package case failed while preparing
its regtest funds: `generatetoaddress 101` exceeded the fixture's 15-second
RPC deadline. An isolated rerun reproduced it. Failure diagnostics now retain
Core's datadir after stopping the child. The retained log shows mining still
advancing through heights 95–100 when the timeout initiated shutdown.

Only `generatetoaddress` gets a 120-second bounded preparation deadline.
Ordinary RPCs retain their existing limits; timed-out mutations are not retried.
This change does not relax transaction-policy verdicts or skip a comparison.
The full live Core rerun passed **13 tests**: nine block comparisons and four
replacement/package-pressure/fee-decay/reconciliation comparisons.

Strict all-target/all-feature Clippy, formatting and `git diff --check` passed.
The existing vendored Bitcoin Core C string-initializer warning remains.

## Storage execution and limits

All nine Python runner tests and the monitor, exercise and report shell suites
passed natively on this Mac. The 20,000-live / 512-transition / 100-update
release smoke completed both serial lanes, five compact-copy crash boundaries,
and independent-process reopen audits. Both lane contents matched.

Measured peak RSS was 25,165,824 bytes for batch 64 and 47,939,584 bytes for
batch 256, ratio **1.90495**. The runner correctly returned exit 1 and marked
memory-budget review required. This is compatibility and rejection evidence,
not a passing performance gate. Source/binary hashes, attempts and matrix are
retained in `../rBTC-storage-smoke-ae73758/`.

Full default workload output is `../rBTC-storage-full-20260912/`.
The supervisor was launched with frozen revision `c525ee7` in the detached
`../rBTC-storage-source-c525ee7/` worktree. The exact revision and executable
hashes are recorded by the runner. Both default lanes
retain 160M live UTXOs, perform 900,000 transitions with 5,000 updates each,
use 128 GiB ceilings, and retain the original 64/256 comparison.
The local supervisor uses `caffeinate -i -s` and records read-only memory
pressure, VM counters, swap, AC-power state and filesystem free-space samples
at approximately 30-second intervals. It does not make automatic acceptance
judgments or install a system service.

A live process or a completed seed is not full-scale acceptance. Consult
`execution.json`, checkpoint reports and ultimately `matrix.json`; no completed
full-scale matrix was available when this report was prepared. ETA is valid
only after two advancing churn checkpoints at the actual 160M live set.
The full workload must also be reviewed for high-water/maintenance behavior,
copy space, RSS ratio, recovery and canonical content after it finishes.

```sh
# Run from the detached frozen source worktree.
contrib/run_mdbx_replacement_gate.sh /Users/jieliu/Documents/n42/rBTC-storage-full-20260912 --status
# If interrupted, keep the frozen binaries and all parameters unchanged.
contrib/run_mdbx_replacement_gate.sh /Users/jieliu/Documents/n42/rBTC-storage-full-20260912 --resume
```

## Historical public soak and the six gates

The pre-existing `rBTC-public-soak-20260815` run uses commit `f0418bd`, not the
current audit revision. Its node and monitor PIDs are no longer running.
Process samples end at 2026-09-03T01:54:26Z and tip samples at 01:50:25Z.
The updated finalizer returned exit 1 / `INCOMPLETE`; reported reasons include
a malformed historical event row and missing Testnet4 freezer-rotation evidence.
The stopped run also cannot provide coverage through this continuation date.
No historical files were edited to make this run pass.

| Gate | Status after this continuation | Required next evidence/work |
| --- | --- | --- |
| Complete Core optimizer | Open | Budgeted search, previous-order reuse, optimal/unconverged results and full differential acceptance |
| Aggregate admission budget | Open | Shared accounting across all stages, peers and chain changes, with deferral/backpressure and atomic publication |
| Fork-header retention/recovery | Open | Reservations at both ingress paths, durable eviction, bounded reopen and stronger-fork reacquisition |
| Full 160M / 900k storage | Open; full run is not yet a completed result | Finish both frozen lanes and review all lifecycle/RSS/content metrics |
| Mainnet cold replay | Open | Immutable contiguous corpus and authenticated matching initial state, serial matched engines and cold-cache evidence |
| Seven-day public acceptance | Open | Freeze the final production implementation, catch up both networks, then collect 604,800 seconds and day-two-or-later recovery exercises |

Do not start the formal seven-day clock for an implementation that still needs
changes in the first three rows. The full generated storage run does not need
the missing mainnet corpus; cold replay and public acceptance remain separate.

## Evidence

Local raw evidence is under `target/mac-acceptance-2026-09-12/`:

- `environment.json`, `corpus-inventory.json`, `storage-preflight.log`.
- `runner-tests.log`, `soak-*-tests.log`, `storage-smoke.log`.
- `all-features-tests.log` (original failure), `refused-endpoint-before.log`,
  `failed-endpoint-after.log`, `all-features-after.log` and per-target summaries.
- `core-differential.log`, isolated failure logs,
  `core-mining-timeout-debug.log`, `core-differential-after.log`.
- `clippy-final.log`, `fmt-final.log`, `historical-soak-report.md`.
- `storage-supervisor.py`, its JSON state, `storage-full.log`, and
  `storage-host-samples.jsonl` for the local full run.

Large input snapshots, old node data and prior benchmark databases were not
removed or repurposed. Reports distinguish original failures, fixture fixes,
small-scale rejection evidence and still-incomplete full runs.
