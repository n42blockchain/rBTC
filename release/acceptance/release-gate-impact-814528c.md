# Release gate impact review — frozen candidate `814528c`

- Frozen source commit: `814528c93d9c35545ede165d9cadeda3e4ba2e90`
- Frozen source SHA-256: `c06885232dad3adff1e35bda35b62db65e85cfc41c1c16216d0f4f57a4e44bca`
- Exact-source Linux, Windows, and supply-chain CI passed. The Windows job's
  first run had two existing 2-second standby test timeouts; the same job passed
  on rerun without source changes.

## Storage replay impact

The accepted real-block replay remains
[`storage-replay-20260915.md`](storage-replay-20260915.md), bound to its actual
tested commit `3cf44ae68003ceb78e9f0c77042667b475091716`. It is not relabeled as
having run on this candidate.

The intervening source changes are confined to header-candidate scheduling and
retention in `src/header_candidate.rs`, `src/node/header_state.rs`, and
`src/node/header_sync.rs`, plus their loopback header tests. They change how a
competing header fork is journaled, resumed, and protected during bounded
retention. They do not change block or script validation, UTXO transitions,
chainstate serialization, redb or MDBX transaction/recovery code, snapshot
parsing, or the block replay workload. The new header regression exercises
fork selection, disconnection, journal resume, promotion, and persisted active
tip; the exact-source CI and resource acceptance also pass those header tests.

Decision: retain the storage replay acceptance for the storage/chainstate
claim. Re-running the 28,350-block dual-engine replay would not add coverage for
the header-only changes; current-source header behavior is covered by the
separate header gate. If a future change touches block execution, UTXO state,
snapshot/replay code, storage transactions, or recovery formats, rerun this
gate. The existing replay remains limited to its documented snapshot window;
it is not a genesis-to-tip audit or a substitute for the public soak.

## Canonical resource evidence

The admission and header reports are
[`admission-resources-814528c.md`](admission-resources-814528c.md) and
[`header-resources-814528c.md`](header-resources-814528c.md). Raw logs and
per-file hashes are retained at
`/data/bench/rbtc-resource-acceptance-814528c/`; the checked hash manifest is
[`resource-evidence-814528c.sha256`](resource-evidence-814528c.sha256).

The sustained header probe ran 3,602.03 seconds and generated 302,000,000
siblings. It passed with peak sampled RSS 408,608,768 bytes, peak sampled disk
67,907,584 bytes, and middle-to-final median RSS growth ratio 1.0123. The
admission probe's three-run peak clone RSS delta was 204,800 bytes against the
project's 64 MiB allowance. These are finite project regression envelopes, not
Bitcoin protocol limits or promises about whole-node RSS.
