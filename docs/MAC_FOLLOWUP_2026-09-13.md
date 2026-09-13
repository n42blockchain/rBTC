# Mac implementation and acceptance follow-up, 2026-09-13

Continues [the Mac baseline](MAC_ACCEPTANCE_2026-09-12.md) on
`audit/mac-acceptance-20260912`. The storage evidence remains frozen at
`c525ee768cb2f4ebdeca64f4779ae7fb383e9553`; subsequent optimizer, admission and
header changes are not retroactively attributed to those storage binaries.
Implementation commits: `c49609d` (optimizer/admission) and `95ea5a0`
(header-retention foundations). These are local commits, not a claimed remote
push or production freeze. All six gates remain open, with the narrower completed
evidence below.

## 1. Budgeted optimizer and order reuse

`Cluster::linearize_with_budget` now returns an order, an optimal/minimal proof
flag, consumed search work and whether a valid previous order was reused.
Production `linearize` uses an allowance of 1,000,000 search units. Invalid old
permutations or orders are discarded. A search interrupted by its allowance
publishes only proved maximum-density chunks and retains the previous suffix;
it does not label an unfinished search optimal. Initial greedy/refinement work
and output construction are separately bounded by the 64-entry production
cluster ceiling. Larger pure-function inputs retain the existing fallback.

The Rust search uses exact integer maximum-density closure/min-cut operations.
It is an independent implementation, not a port of Core's spanning-forest search.
Its work units are matrix/edge/reachability operations, **not Core CostModel
units**. Equal numerical budgets therefore do not imply identical intermediate
orders, work or convergence. A bounded 64-cluster cache fingerprints membership,
fees and dependencies, remaps overlapping old orders and keeps updates local to
the private admission candidate. An optimal unchanged cluster needs no new search.

Reference: unmodified [Core v31.0 cluster_linearize.h](https://github.com/bitcoin/bitcoin/blob/v31.0/src/cluster_linearize.h),
called by `contrib/core31_linearize.cpp`. The official `bitcoin-31.0.tar.gz`
source archive SHA-256 is
`0ba0ef5eea3aefd96cc1774be274c3d594812cfac0988809d706738bb067b3e3`.
The adapter preserves assertions and calls native `Linearize` directly.

4,096 native comparisons passed for 0–64 entries, zero/equal/random fee rates,
absent/valid/invalidated old order modes and 2,048 permuted vertex labelings.
Converged orders and fee diagrams match exactly; maximum observed Rust search
work was 649,961 units. Small graphs are also checked against exhaustive
ancestor-closed subset enumeration. Separate exhausted-budget regressions check
non-worsening diagrams, old-order validation and arithmetic boundaries.
This is finite differential evidence, not exhaustive Core policy equivalence,
matching budget-exhaustion paths or a worst-case runtime proof. Gate 1 remains
open pending that acceptance review and wider adversarial policy coverage.

Reproduce after extracting the pinned archive under the evidence directory:

```sh
c++ -std=c++20 -O2 -Wall -Wextra -Werror -DABORT_ON_FAILED_ASSUME \
  -isystem target/mac-followup-2026-09-13/bitcoin-31.0/src \
  contrib/core31_linearize.cpp \
  -o target/mac-followup-2026-09-13/core31-linearize
RBTC_CORE_LINEARIZE="$PWD/target/mac-followup-2026-09-13/core31-linearize" \
RBTC_CORE_LINEARIZE_REPORT_DIR="$PWD/target/mac-followup-2026-09-13/core-optimizer-final" \
  cargo test --locked --all-features --lib full_optimizer_matches_core -- --ignored
```

## 2. Shared admission deferral

`AdmissionBudget` shares a monotonic token ledger across pool/candidate clones,
peers and chain-change attempts. Failed attempts consume their work; rolling
back a candidate cannot restore it. Payload, metadata, prevout, SCRIPT, graph
and snapshot stages expose charged/deferred counters. Candidate reservations
release through an RAII guard, including failures and unwind. Defaults are
8 billion burst units, 1 billion units/second and 512 MiB of estimated concurrent
candidate allocations; these are provisional engineering settings, not measured
whole-node CPU/RSS limits.

The optimizer reserves its search allowance from the same graph ledger.
Admission and reconciliation clone privately after reserving estimated metadata;
`reconcile` now returns `Result` and cannot partially publish removals when a later
entry runs out of resources. Callers propagate this new API explicitly.

The peer pipeline distinguishes resource deferral from invalid transactions,
retains drained peer payloads for retry, and preserves disconnected transactions.
It skips due relay on a deferred pass. On a changed execution tip, retained pool
payload views are withheld until fresh reconciliation succeeds; the pool data
remain available for the next candidate. Snapshot estimates use current persisted
row lengths and retained payloads rather than the configured empty-pool ceiling.
Regression tests cover shared rejected work, candidate rollback after one stale
entry was removed privately, withheld stale views and eventual revalidation,
and deferral both before and after draining a real loopback peer queue. Dry-run
resource exhaustion returns an operation error, not a transaction-invalid verdict;
terminal-rejection caching also explicitly excludes resource deferral.

**Remaining gate 2 work:** reservations are estimates, not allocator interception.
Prevout materialization, snapshot row access/decoding, graph preprocessing and
all escaped relay/persistence allocations still need complete pre-allocation
accounting. Snapshot length inspection is a point-in-time read, not a lease
against concurrent store replacement. Large candidates that cannot fit one
reservation need resumable scheduling; merely retrying the same oversized unit
is insufficient. Expose calibrated operator limits and pressure counters, audit
all chain-tip publication sites, and measure sustained hostile/repeated-context
loads before claiming a whole-pipeline bound. The implementation is not frozen.

## 3. Transactional fork-retention foundations

New explicit maintenance primitives implement leaf-first staged eviction,
active-chain protection, execution/recovery tip pins, atomic durable removal
and retained-row-count checks before DAG materialization. A lazily built child
index is maintained across later insertions, staged insertion rollback and
leaf removal. Failed plans and dropped guards restore every removed header.
A reverse durable sequence index supports deletions without rescanning historical
rows on each removal; legacy indices are filled by a streaming transaction on
first eviction. Store length now counts retained rows rather than the monotonic
insertion sequence, which legitimately has gaps after removal.

Seven header-store tests passed, including five new retention scenarios:
protected ancestry/cancellation, bounded reopen before any historical validation,
failed durable deletion after a private first deletion, legacy-index migration,
and eviction followed by fresh reopen and explicit refetch of a now-stronger
fork. The refetched branch matches an unbounded reference's work-selected tip
and median-time-past. Redb transaction failure cannot publish partial removal.

**Automatic eviction is not enabled at peer or local-block ingress.** These
APIs require the complete corresponding DAG and pinned execution/recovery tips.
The first child-index construction is linear in retained history; bounded reopen
currently defers an oversized store rather than selecting a safe retained subset.
Freed redb pages can be reused, but file shrink/compaction and a physical disk
plateau have not been demonstrated. A bounded resumable candidate, automatic
locator/refetch scheduling, ingress reservations, recovery handoff and execution
rollback proof remain necessary before enabling policy or closing gate 3.

## 4. Full storage result and maintenance supplement

Host: M1 Max, 10 cores, 64 GiB RAM, macOS 26.6.2 arm64, internal Apple 4 TB SSD,
APFS. Both frozen lanes completed the unchanged 160M-live / 900,000-transition /
5,000-update workload with 128 GiB capacity, 288 undo entries and batches 64/256.
The supervisor exited 0. Serial execution including audit/recovery took about
6.15 hours. This is synthetic storage activity, not Bitcoin validation throughput.

| Measured item | Batch 64 | Batch 256 |
| --- | ---: | ---: |
| Seed/churn/final-audit wall seconds | 10,835 | 11,258 |
| Whole-process peak RSS, bytes | 12,854,050,816 | 14,431,485,952 |
| Maximum allocated bytes at checkpoints | 12,376,014,848 | 12,261,507,072 |
| Maximum high-water bytes at checkpoints | 12,360,417,280 | 12,245,778,432 |
| Final allocated bytes | 12,280,922,112 | 12,261,507,072 |
| Final live-page bytes | 11,644,633,088 | 11,921,145,856 |
| Final free-page bytes | 541,032,448 | 288,849,920 |
| Compact-copy maintenance events | 0 | 0 |

RSS ratio is **1.1227189124**, below 1.5. Both final contents match:
`9c3cbbd622234b030ad8b4ccdea5da4360925cf5af0b609ca406f06b651e54ef`.
Both independent-process reopen audits passed; the runner also completed the
five compact-copy crash boundaries. Each final state has 160M hot entries,
zero cold entries, 900,003 metadata entries and 288 undo entries.

739 approximately 30-second host samples recorded AC power throughout, no swap
usage, and at least 866,348,261,376 available filesystem bytes. The lowest
`memory_pressure -Q` reported free percentage was 91%; this is that command's
metric, not a claim that 91% of physical RAM was unused. Checkpoint observations
are not continuous peak-disk measurements. The full run did not reach the
55%-of-128-GiB maintenance trigger and cannot alone establish maintenance cost.

A separate frozen-source supplement used 2M live entries, 4,096 transitions,
5,000 updates and a 1 GiB ceiling. Batch 256 compacted at heights 512, 1,024,
1,280, 3,840 and 4,096; each operation preserved content and reduced reported
free-page bytes to zero. Durations were 2.206, 2.159, 1.436, 1.354 and 1.328 seconds.
Batch 64 did not compact. Both final digests were
`d1a9badf1e78d8bdd07324579437b95480db9034da539b610ff7bce43588acdb`;
fresh-process reopen and crash-boundary checks passed.

Supplement RSS was 838,877,184 / 2,827,698,176 bytes, ratio **3.3708130701**.
The runner correctly returned exit 1 / `review_required`. This failed ratio is
retained and is not averaged with or overridden by the full-scale result.
The supplement demonstrates actual maintenance behavior at reduced capacity;
it is not 160M maintenance evidence. Gate 4 still needs maintenance/RSS review,
including simultaneous copy space and behavior at the intended operating size.
Real replay and backend migration remain separate MDBX acceptance requirements.

Raw frozen outputs: `../rBTC-storage-full-20260912/` and
`../rBTC-storage-maintenance-20260913/`; reviewed measurements and source paths
are recorded in `target/mac-followup-2026-09-13/storage-review.json`.

## 5. Corpus preparation and Linux access

The supplied target is **`n42@192.168.0.166`**. SSH reaches the host but the Mac's
keys are refused (`Permission denied (publickey,password)`); no fresh `/data`
inspection or remote workload is claimed. This Mac already has an Ed25519 public
key and `ssh-copy-id`. Install it from the user's own Mac terminal, entering the
Ubuntu account password only in that terminal:

```sh
ssh-copy-id -i ~/.ssh/id_ed25519.pub n42@192.168.0.166
ssh -o BatchMode=yes n42@192.168.0.166 'whoami; hostname; df -h /data'
```

If password-based SSH login is disabled, the public key must instead be installed
from an existing Ubuntu console/session in `n42`'s `~/.ssh/authorized_keys`
(directory mode 700, file mode 600, owned by `n42`). The Mac private key stays on
the Mac. The historical Linux free-space
measurement does not establish the current corpus inventory.

The existing local snapshot and index were streamed through SHA-256, with size
and modification time unchanged during hashing:

| File under `../rBTC-mainnet-assumeutxo-20260725/` | Bytes | SHA-256 |
| --- | ---: | --- |
| `utxo-935000.dat` | 9,387,990,306 | `e572ddbe456d254f05fb004cebe225bdb3656074b66f0e9b1c7fa83e1301d486` |
| `utxo-935000.rbtcidx` | 1,155,791,488 | `e3be4eef91fa96ad8eba76206213cad4ed45b563826a2a0e4b3d5b9184a005c8` |

Snapshot metadata declares mainnet, version 2, 164,241,311 coins and base hash
`0000000000000000000147034958af1652b2b91bba607beacc5e72a56f0fb5ee`.
The index embeds the snapshot file's matching SHA-256. These are file identity
and metadata checks, not independent authentication of UTXO contents or proof
of a contiguous mainnet block corpus. The old retained ledger windows do not
cover the missing blocks after this base; no complete Core `blk*.dat` set has
been verified on this Mac.

After SSH authentication, inspect `/data` for the exact source format (Core
`blk*.dat` plus any `xor.dat`, btcd `.fdb`, or imported rBTC ledger), immutable
range and snapshot identity. Produce per-file hashes and contiguous parent/hash
verification before scheduling serial matched-engine cold replay. Do not infer
completeness from the presence of a blocks directory or a large disk.

## 6. Public soak and verification evidence

The new seven-day public run was **not started**. Freeze and accept the production
changes first, catch up both networks, then begin the full 604,800-second clock
and perform controlled recovery exercises after day one. The historical stopped
soak remains unchanged and cannot certify this implementation.

Local evidence is in `target/mac-followup-2026-09-13/`: native oracle inputs,
outputs and summaries; resource and retention regressions; all-feature, strict
Clippy and live-Core logs; corpus fingerprints; storage review and a SHA-256
manifest. The all-feature suite passed **960 tests / 0 failed / 34 ignored**;
ignored external/long-running suites remain explicit. All 13 separately invoked
live Core comparisons passed (nine block/transport and four replacement/package/
reconciliation/fee-decay tests). Native optimizer evidence is additional to those
suites. Strict all-target/all-feature Clippy, formatting and diff checks passed.
Vendored Bitcoin Core C warnings are retained.
