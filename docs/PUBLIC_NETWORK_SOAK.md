# Public-network soak

Status date: 2026-07-27.

## Active acceptance run

The release-gate run started at `2026-07-26T22:19:44Z` and cannot satisfy the
seven-day duration before `2026-08-02T22:19:44Z`. It uses the immutable copied
binary `state/rbtcd-soak-start`, commit
`738afa55322343ed9cfe521621c8994a88de048c`, SHA-256
`78a5c5768bbe77d496b5e05492664156632f09dc0e4e78ab2dadf5e708175a61`.
Replacing `target/release/rbtcd` during later development therefore does not
change the program under test.

Host evidence is under
`/Users/jieliu/Documents/n42/rBTC-public-soak-20260726`. The Bitcoin and
Testnet4 nodes run from separate data directories and PID files. Updating a PID
file after a restart preserves one metrics timeline; it does not reset the
baseline clock.

The two recovery exercises are scheduled immediately after the one-day boundary
at `2026-07-27T22:19:44Z`. The supervisor uses the frozen helper
`state/public-network-soak-exercise-start`, SHA-256
`022473ba8e8cffae5279ab7662ad6deddf08d3439b1ff20b712caae5f6a029d6`,
and records its own progress in `logs/exercise-supervisor.log`. It executes the
Bitcoin graceful restart first and the Testnet4 abrupt-kill recovery second;
the evidence monitor remains independent and running throughout.

At `2026-07-27T12:18Z`, both nodes had remained alive for about 14 hours.
Bitcoin was at height 959840 and Testnet4 at 145944, with execution equal to the
maximum-work header tip. The monitor observed six current Bitcoin peer network
groups and nine Testnet4 groups. Peak RSS so far was 7,216,176 KiB and 749,328
KiB respectively. Both persistent mempool stores were non-empty (2,379,776 and
2,121,728 bytes). The detailed samples observed Bitcoin freezer publication
advance from slot 212 to 214 and Testnet4 from 320 to 323.

These are progress observations, not a completed gate.

## Continuous evidence

`scripts/public-network-soak-monitor.sh` appends:

- one-minute PID, elapsed-time, RSS, and CPU samples;
- established peer endpoints, from which independent IPv4 `/16` or IPv6 `/64`
  groups are derived;
- five-minute maximum-work header hash/height and execution height;
- freezer next slot, retained height range, segment count, and exact bytes;
- persistent mempool and peer-store byte sizes;
- hourly data-directory and filesystem-free-space samples;
- process PID transitions and operator-recorded fault/restart events.

The monitor accepts either a numeric PID or a strict regular PID file. A missing
or exited process is recorded without terminating the collector, so a short
controlled restart remains visible instead of splitting the soak timeline.

`scripts/public-network-soak-exercise.sh` performs a bounded recovery exercise.
It refuses to signal a PID unless the baseline is at least one day old, the
recorded immutable binary still hashes correctly, the process command names
`rbtcd` and the exact data directory, and the latest measured header/execution
tips agree. It atomically switches the PID file, waits for the replacement to
reach a consistent tip no lower than the pre-exercise tip, and records timings,
hashes, persistent-store sizes, and success or failure in the event stream.

`scripts/public-network-soak-report.sh` is a fail-closed finalizer. Its normal
seven-day invocation is:

```bash
scripts/public-network-soak-report.sh \
  /Users/jieliu/Documents/n42/rBTC-public-soak-20260726 \
  604800 >public-network-soak-report.md
```

For progress inspection only:

```bash
RBTC_SOAK_ALLOW_INCOMPLETE=1 \
  scripts/public-network-soak-report.sh \
  /Users/jieliu/Documents/n42/rBTC-public-soak-20260726
```

The finalizer refuses completion unless both networks have process, tip,
freezer, persistence, and peer evidence; at least four peer network groups; a
natural tip advance; freezer rotation; distinct pre/post-restart PIDs; one
completed controlled restart on each network; one completed injected fault
scenario; at least 604,800 seconds since the immutable baseline; and an
executable baseline binary whose current SHA-256 still exactly matches the
recorded identity. Its report retains the final hashes, peak RSS, peer
diversity, freezer/data-directory growth, minimum free space, persistent
mempool size, and measured restart durations rather than reducing acceptance
to a single pass/fail bit.

## Remaining scheduled exercises

After at least one full day of ordinary operation:

1. Gracefully restart Bitcoin with:

   ```bash
   scripts/public-network-soak-exercise.sh \
     /Users/jieliu/Documents/n42/rBTC-public-soak-20260726 bitcoin \
     /Users/jieliu/Documents/n42/rBTC-mainnet-assumeutxo-20260725 graceful
   ```

2. Inject an abrupt Testnet4 termination and verify recovery with:

   ```bash
   scripts/public-network-soak-exercise.sh \
     /Users/jieliu/Documents/n42/rBTC-public-soak-20260726 testnet4 \
     /Users/jieliu/Documents/n42/rBTC-testnet4-full-20260725 abrupt
   ```

   The helper records both the controlled-restart and fault-abrupt-kill
   completion evidence only after the replacement reaches matching
   header/execution tips.
3. Continue through the seven-day boundary. Then retain final hashes, peak RSS,
   data/freezer deltas, peer diversity, restart times, ordinary peer failures,
   and the generated fail-closed report.

No wall-clock-age deletion or manual database editing is part of the exercise.
Freezer rotation must remain index-driven, and restart/fault recovery must use
the same network-bound directory and baseline executable.
