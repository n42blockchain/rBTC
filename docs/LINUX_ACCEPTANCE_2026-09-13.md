# Linux acceptance and corpus preparation, 2026-09-13

SSH authentication to `n42@192.168.0.166` now succeeds. This continues
[the Mac implementation and storage work](MAC_FOLLOWUP_2026-09-13.md).
The isolated Linux checkout is detached at
`bcb7660c752d08ac554a9d9e7b56dccff68f2b61` under
`/data/bench/rbtc-acceptance-20260913/source`. Existing unrelated projects,
node databases and the Mac's original snapshot are unchanged.

## Host and checks

- Hostname `n42dev`, Linux `7.0.0-30-generic`, x86_64.
- AMD EPYC 9B45: 128 physical cores, 256 logical CPUs; `/proc/meminfo` reports
  143,277,024 KiB total (136.64 GiB) and initially 71,787,108 KiB available.
  The machine already had other workloads and roughly 7.7 GiB of occupied swap.
- `/data` is `/dev/nvme1n1p1`, **XFS**, mounted `rw,noatime`. Initial total
  7,679,362,727,936 bytes and available 2,905,578,819,584 bytes (2.643 TiB).
  These fresh observations supersede assumptions about this host's filesystem
  and currently available capacity; historical audit measurements remain dated.
- Rust 1.85.0 was already installed. Builds used eight jobs and the suite used
  eight test threads to limit concurrent resource demand.

The same committed Rust source passed **960 tests / 0 failures / 34 ignored**
on Linux. Strict all-feature/all-target Clippy passed. Thirteen explicitly
invoked Core 31 tests also passed: nine block/transport and four replacement,
package-pressure, fee-decay and reconciliation comparisons. These are loopback
regtest fixtures, not a public-network acceptance run.

Core 31.0 Linux x86_64 was downloaded into the isolated tools directory; its
archive SHA-256 matched the [official checksum file](https://bitcoincore.org/bin/bitcoin-core-31.0/SHA256SUMS):
`d3e4c58a35b1d0a97a457462c94f55501ad167c660c245cb1ffa565641c65074`.
The pinned native full-optimizer adapter passed all 4,096 cases on Linux.
Input and complete native output files are byte-identical to the Mac run:

| Artifact | SHA-256 |
| --- | --- |
| Core optimizer input | `98f36cbb898ac55988b429c62bc2591873954652460c9a3ff781ebf0a531988b` |
| Core optimizer output | `1592eab133a5d2fbe053f9a257baaf56065e91e62f2d6cb52f2b8b0ad01e890b` |

These are the same 4,096 deterministic cases on another platform, not 8,192
independent cases. Search work and other gate-1 limitations remain as described
in the Mac report. Existing vendored C/dependency build warnings are retained.

## Corpus search and economical transfer

Accessible Linux searches covered `/data`, `/home`, `/mnt`, `/media`, `/srv`,
`/opt`, `/var/lib/bitcoind` and `/var/lib/bitcoin`, excluding build/cache/vendor
control directories where documented by the discovery script. No existing
Bitcoin `blk*.dat`, btcd `.fdb`, height-935000 snapshot or old sidecar was found.
`/data/docker`, `/home/lost+found` and `/opt/containerd` denied traversal; no claim
is made about inaccessible contents. The newly installed example `bitcoin.conf`
is a tool distribution file, not evidence of a synchronized Core datadir.
`/data/blockchain/mainnet-source*` contains N42/EVM-style data, not Bitcoin files.

Mac searches covered Documents, Downloads, the default Core data directory and
attached-volume entries. The only substantial matching Bitcoin source is the
pre-existing height-935000 snapshot and sidecar. The small `blk_0_to_14131.dat`
files are btcd test fixtures. A separate ledger-index search found these old
retained windows:

| Network/location | Declared first–last height | Blocks | Compressed bytes |
| --- | --- | ---: | ---: |
| Bitcoin background validation | 534,997–536,004 | 1,008 | 674,113,582 |
| Bitcoin active node | 964,277–965,243 | 967 | 1,073,328,715 |
| Testnet4 | 149,782–150,786 | 1,005 | 12,476,360 |

These are index declarations, not newly authenticated block payloads. None
provides a contiguous replay starting at 935,001. They were not copied, because
copying old full node directories or these unmatched windows would consume
space without supplying the required initial replay range.

Only `utxo-935000.dat` was transferred from the Mac using partial/in-place rsync
to a dedicated `.partial` name. After transfer, Linux independently streamed
SHA-256, checked length and stable modification time, renamed that same inode
to the final name and made it read-only. This avoids retaining two destination
copies and permits a failed transfer to resume without recopying the source.

- Source: `/Users/jieliu/Documents/n42/rBTC-mainnet-assumeutxo-20260725/utxo-935000.dat`.
- Destination: `/data/bench/rbtc-acceptance-20260913/corpus/utxo-935000.dat`.
- Bytes: **9,387,990,306** (8.743 GiB).
- SHA-256: `e572ddbe456d254f05fb004cebe225bdb3656074b66f0e9b1c7fa83e1301d486`.
- Actual destination allocation after transfer: 9,387,991,040 bytes.

The old Mac index (1,155,791,488 bytes) was not copied. The committed Linux
release binary is used to authenticate the compiled Core 31 UTXO commitment and
build a current access index directly beside the one retained snapshot. The
index build command is offline and does not start a P2P node or a public clock:

```sh
/data/bench/rbtc-acceptance-20260913/target/release/rbtcd \
  --build-core-snapshot-index /data/bench/rbtc-acceptance-20260913/corpus/utxo-935000.dat \
  --snapshot-index-output /data/bench/rbtc-acceptance-20260913/corpus/utxo-935000.rbtcidx
```

The offline build **passed** in 101.49 seconds with 6,516,484 KiB peak RSS
(6.21 GiB). It parsed 164,241,311 coins and matched the code-pinned Core 31
UTXO-set commitment before publishing the index. A separately invoked real-file
smoke test opened that index and resolved the snapshot's first coin and an absent
outpoint correctly. This authenticates the snapshot against the compiled anchor;
it does not establish the base's position in a freshly verified best-work header
chain, or supply the missing replay blocks.

| Retained sidecar | Bytes | SHA-256 |
| --- | ---: | --- |
| `utxo-935000.rbtcidx` | 530,926,247 | `04439a1e495d0b911cc4e1b8042e3a0de8261eb1d539c65537fa7836dd7ff828` |
| `utxo-935000.rbtcidx.fp` | 227,758,412 | `2a3b7da03795b7cfc67c381ba1d3fe9320b10828cfe4ee56d0ef340b82ad6958` |

The index builder also creates a fingerprint sidecar; it is explicitly included
in space accounting. Together the two new sidecars occupy 758,684,659 logical
bytes, 397,106,829 bytes less than the old Mac index alone. Snapshot plus both
sidecars total **10,146,674,965 logical bytes / 10,146,680,832 allocated bytes**
(9.450 GiB). There is no leftover `.partial` snapshot or second destination copy.
All three completed corpus files are read-only.

After verification, only this task's reproducible debug/release intermediates
and extracted upstream source were removed. The exact tested library and two
Core integration executables, the release `rbtcd`, source commits, downloaded
archives and raw evidence were retained. Hard links preserve tested binaries
without another physical copy. They are available under `tools/`; the release
binary also remains at its original path. `build-cleanup.json` records the
precise removed paths and retained executable hashes.

The complete isolated task directory measured **11,293,134,848 allocated bytes**
(10.52 GiB), including corpus, source, tools, tested binaries and reports, before
the final small report copies. Whole-volume free-space deltas can include other
workloads and are not used as an exact reclamation claim. No old benchmark lane,
unrelated node database or Mac source file was deleted.

## Remaining gate boundaries

The complete historical corpus is explicitly identified in the older
[Windows replay report](REAL_BLOCK_OVERLAY_REPLAY_2026-08-21.md) as
`D:\btcd26\mainnet\blocks_mdbxdb` (1,423 `.fdb` files, 762,923,466,164 bytes).
Neither the current repository documentation nor the Mac SSH configuration
provides a usable connection to that Windows host (the Mac has no SSH config).
This path is historical provenance, not a currently accessible source.

A snapshot is not a full block corpus. Cold replay still requires an immutable,
contiguous block range, its parent/hash verification, and a matching start/stop
identity before serial engine runs. No fresh Core IBD, full corpus copy, cold
replay or seven-day public run was started. The requested disk constraint favors
reusing an existing verified compressed range and a single snapshot; an absent
range cannot be reconstructed from the retained sparse Mac windows.

The production implementation remains unfrozen: gate 2 still needs complete
pre-allocation/resumable scheduling work; gate 3 lacks automatic bounded
reacquisition and both-ingress policy activation. Gate 4's 160M Mac measurements
and the separate maintenance RSS review remain unchanged. Cross-platform tests
are additional evidence and do not close those resource or performance gates.

Evidence is retained locally in `target/linux-acceptance-2026-09-13/` and remotely
under `/data/bench/rbtc-acceptance-20260913/evidence/`, including host inventory,
search scope/errors, source identity, commands, full test logs, native oracle
inputs/outputs, transport hashes, index job state and retained-file hashes.
