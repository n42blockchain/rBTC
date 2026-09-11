# Admission resource gate: shared payloads and indexed dependencies

Measured 2026-09-10 against baseline `2630a2d89c0cc279b4ff9d7b2769d74cb22504b6`.
This closes two concrete sources of repeated work. It does not close the
end-to-end admission CPU/RSS gate.

Subsequent work at baseline `b320752` also removes repeated-prefix validation
and deep payload copying from reconciliation, while preserving sponsored
packages across chain updates. See the separate
[reconciliation acceptance](UPSTREAM_RECONCILIATION_GATE.md). The measurements
and implementation description below record the earlier `568772a` acceptance.

## Implementation and invariants

Previously, every candidate pool clone copied the complete admitted transaction
payloads. Membership and graph queries repeatedly computed transaction IDs,
including hashing unrelated large outputs. Some graph traversals scanned the
whole pool once per visited dependency.

Admitted entries now own an `Arc<Transaction>` and immutable, precomputed txid
and wtxid values. Candidate clones share those payloads. The public snapshot and
relay APIs still return independently owned transactions, and reconciliation
unwraps a uniquely owned payload or clones it before readmission. Candidate
indices and successful SCRIPT commitments remain independently owned; failed
admission still cannot publish candidate mutations into the live pool.

A `BTreeMap<Txid, usize>` indexes positions in the admission deque. Append updates
it, while replacement, removal and capacity eviction rebuild it with the spent
index. Reconciliation clears both. The existing spent-outpoint map already
represents every child edge, so an ordered range over a parent's output indices
replaces full-pool child scans without storing a second child graph. Ancestor,
descendant, cluster and affected-chunk traversal use these indices. Canonical
member order, stable chunk sorting, fee calculations and policy decisions remain
unchanged.

The index and IDs add metadata proportional to retained transaction count.
Cloning still copies metadata, indices and the bounded orphanage. Replay still
resolves fresh UTXOs and validates maturity, locks, fees, standardness and sigops;
SCRIPT reuse still requires the content commitment introduced in `51aee2a`.

## Regression and differential acceptance

- A new regression checks shared payload identity, independent snapshot and
  candidate mutations, deletion index repair and reconciliation.
- A new graph regression compares indexed results with an independent full-scan
  reference across 24 generated branching/multiple-input DAGs, before and after
  descendant removal. It also checks unknown/confirmed roots, unchanged clones,
  positions, IDs, spent edges and total policy vbytes.
- All 67 admission tests pass, including SCRIPT cache, CPFP, TRUC, replacement,
  cluster limits, capacity eviction and relay-order cases.
- Final all-feature suite: **916 passed**, **0 failed**, **28 ignored**
  (877 library + 39 integration; subprocess-helper results not counted twice).
- Both explicitly enabled Core 31 replacement/package-pressure differential
  tests pass. These covered decisions do not establish optimizer or rolling-floor
  equivalence for all workloads.
- Strict all-target/all-feature Clippy, root formatting and diff checks pass.

The first full-suite run had one failure in
`daemon_fails_over_and_resumes_persisted_ibd_with_the_next_peer`: the deficient
peer observed a random-looking local version nonce instead of the test's `99`.
Its isolated rerun and the entire second run passed without any source change.
The cause remains unconfirmed; the initial failure is retained in
`all-features-tests.log`, alongside `failover-focused.log` and
`all-features-final.log`. This continuation does not claim to have fixed that
intermittent failure.

## Reproducible payload-stress comparison

`examples/admission_resource_probe.rs` uses the actual admission pool and a
temporary Redb UTXO store. It creates 128 or 256 transactions: a 16-entry chain
plus independent transactions carrying a 60,000-byte OP_RETURN payload. These
are synthetic regtest transactions admitted under the existing policy. Each run
performs 50 queries of the 16-entry cluster, holds eight explicit pool clones
simultaneously, then admits one additional small funded transaction.

Eight clones deliberately amplify copying costs; this is not a claim that one
production admission creates eight simultaneous clones. The small chain amid
large unrelated outputs deliberately exposes unnecessary scanning and hashing.
It is not a representative mainnet transaction mix.

Both revisions use the exact same probe source, Rust 1.85.0, release mode,
all features and the daemon's mimalloc allocator. Host: Linux x86_64, AMD EPYC
9B45, approximately 136 GiB RAM. The temporary database is under `/tmp` (tmpfs);
these are warm local kernel/store measurements, not cold-disk or whole-node
measurements. Each result below is the median of three fresh-process runs.
Runs were sequential after regression/Core checks completed, alternating which
revision ran first. An earlier new-version sanity run is excluded.

| Measurement | 128 baseline | 128 indexed/shared | 256 baseline | 256 indexed/shared |
| --- | ---: | ---: | ---: | ---: |
| Seed workload, ms | 3,028.435 | 544.406 | 13,633.239 | 2,313.929 |
| 50 small-cluster queries, µs | 397,516 | 115 | 855,478 | 124 |
| Eight simultaneous clones, µs | 3,749 | 24 | 10,724 | 81 |
| Additional admission, µs | 52,884 | 8,282 | 113,163 | 17,875 |
| RSS before clones, KiB | 31,692 | 28,808 | 49,640 | 31,300 |
| RSS with clones alive, KiB | 80,816 | 28,808 | 155,360 | 31,500 |
| Per-run RSS delta for clones, KiB | 49,124 | 0 | 105,720 | 200 |

Across the three 256-entry runs, clone RSS deltas were 105,652–106,980 KiB for
the baseline and 200 KiB for the new version. Additional-admission times were
112,395–116,200 µs versus 17,672–17,886 µs. Retained serialized-byte accounting
was identical between revisions in every run: 6,734,451 bytes after the 128-entry
case's additional admission and 14,429,043 bytes after the 256-entry case.

RSS is read from `/proc/self/status`, not an allocator counter. A zero RSS delta
does not mean zero allocations: the allocator can reuse already resident pages.
The pointer-identity regression directly proves payload sharing; RSS measures
its observed process effect in this workload. Timing ratios are workload-specific.
The additional-admission time still approximately doubles with the pool size,
and cumulative seeding still grows much faster. Neither constant-time admission
nor a whole-pipeline resource bound follows from the small-cluster query result.

Build and run in each revision, copying this same probe into the baseline:

```sh
export CARGO_HOME=/tmp/rbtc-cargo-home
cargo build --locked --release --all-features --example admission_resource_probe
/usr/bin/time -v target/release/examples/admission_resource_probe 128 8
/usr/bin/time -v target/release/examples/admission_resource_probe 256 8
cargo test --locked --all-features --no-fail-fast
cargo clippy --locked --all-targets --all-features -- -D warnings
RBTC_BITCOIND=/absolute/path/to/bitcoin-31.0/bin/bitcoind \
  cargo test --locked --all-features --test core_replacement_differential \
  -- --ignored --nocapture
```

If `CARGO_TARGET_DIR` is set, use its `release/examples/` binary path. Build
sequentially and preserve each revision's executable before building the other.

Local evidence is under
`target/upstream-followup/2026-09-10/admission-index/`: build and acceptance logs,
per-run JSON and `/usr/bin/time` reports, `probe-results.json` with all samples
and min/median/max values, `environment.json` with source/binary hashes, and
`published-source.json`. Generated reports and binaries are local artifacts.

## Remaining acceptance

Admission still replays the retained pool into a fresh UTXO overlay and repeats
input lookup, transaction/prevout hashing, policy work and some whole-pool scans.
Copying indices and orphan payloads, owned relay/persistence snapshots and graph
work also require accounting. The next gate must measure and bound aggregate
allocation/work across rejected and accepted packages, competing peers and
chain-view changes without weakening fresh contextual validation or package
atomicity.

Durable valid-header retention/recovery, full mainnet storage scale, broader
Core optimizer/fee-floor coverage and the seven-day public-network soak remain
separate open gates in [the follow-up ledger](UPSTREAM_2026_FOLLOWUP.md).
