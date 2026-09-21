# Storage replay acceptance — 2026-09-15

This report covers the frozen source commit
`3cf44ae68003ceb78e9f0c77042667b475091716` and the current release binaries
built from that source on Linux x86_64. Raw evidence remains on the acceptance
host at `/data/bench/rbtc-acceptance-20260915/cold-replay-20260915`.

## Workload and execution

- Authenticated snapshot base: height `935000`, hash
  `0000000000000000000147034958af1652b2b91bba607beacc5e72a56f0fb5ee`.
- Replay window: `935001–963350` (`28350` blocks), full scripts, batch size
  `256`, with one flush batch and an explicit 10 GiB overlay capacity.
- redb completed in `2388.7893797198776` seconds, exit code `0`, with no peer
  errors; mdbx completed in `2328.4342852421105` seconds, exit code `0`, with
  no peer errors.
- Both lanes completed fresh-process overlay audits successfully.

## Cross-engine result

The supervisor reported `phase=complete` and `comparison_passed=true`. All
comparison fields passed: base height/hash, serialized base hash, tip
height/hash, overlay entry/key/value counts, overlay logical hash, tombstone
count/hash, undo count, and canonical content hash.

- Final tip: height `963350`, hash
  `0000000000000000000077140b44f3dc7c0c3730ae69a782d66ec69a8f5e611a`.
- Overlay entries: `14554294`; key bytes `523954584`; value bytes `798755918`.
- Tombstones: `13000529`; undo entries: `958`.
- Canonical content SHA-256, identical in both engines:
  `aadd289f6edf154e55aec63c9b4c22cd46e2d7836dc55382d0036523247f2819`.
- The logical overlay SHA-256 was identical:
  `1bc8b65d630e1d4ac74ebd1742bbd26c7444ff554a529a05db1f2345b0f61f72`.
  Engine-specific raw-file hashes differed as expected.

## Scope

This closes the selected frozen-source snapshot replay gate. It is not a
genesis-to-tip validation, not isolated-device performance evidence, and does
not close the 160M-UTXO/900,000-transition lifecycle gate or the seven-day
public-network soak.
