# Admission reconciliation: one contextual pass

Measured 2026-09-11 against `b32075236eef8e4a46b6c8120b462289b19a60c9`.
This closes the repeated-prefix validation and sponsored-package loss defects
in `TransactionAdmissionPool::reconcile`. Whole-pipeline admission CPU and
allocation limits remain open.

## Reproduced failures

The old reconciliation drained the pool into owned transactions and called
single-transaction admission for each one. Each admission cloned the growing
candidate and replayed all preceding survivors. Eight independent valid
transactions therefore performed 36 contextual validations instead of eight.
For an unchanged all-valid pool of N entries, the loop structure performs
N(N+1)/2 validations. Draining also discarded successful SCRIPT commitments and,
when another pool clone retained the payloads, deep-copied those transactions.

Single-transaction readmission also applied the individual relay fee to a
zero-fee parent that had already been accepted with its paying child. A new
regression reproduces both entries disappearing on an unrelated chain update.
The same defect is reproduced against a live Core 31 node: after an explicitly
empty block, Core retains both transactions and baseline rBTC retains neither.
Both baseline failures are retained in the local logs.

## Implementation and invariants

Reconciliation now walks the retained deque once, using one private UTXO overlay.
Each entry resolves current inputs and repeats maturity, absolute/relative locks,
amount accounting, sigop accounting and standardness checks. SCRIPT execution
alone may reuse its successful commitment, which is recomputed from the witness
transaction, ordered prevout amounts/scripts and script flags. Changing height,
MTP or prevouts cannot bypass contextual validation.

The overlay changes only after all checks for that transaction pass. On failure,
the entry is removed; its children are still checked against the current view.
This distinction matters when a parent has been mined: its confirmed outputs
can keep an unmined child valid. Missing/conflicted parent outputs naturally
invalidate dependent children. The base store remains untouched.

Successful entries retain the same immutable `Arc<Transaction>` payloads and
receive refreshed fee, policy-vsize, sigop and SCRIPT metadata. Their established
fee sponsorship is preserved with a zero replay fee; the rolling fee and its
fractional/temporal state are untouched. Survivors keep insertion order. The
overlay is dropped before the spent/position indices and retained-byte total
are rebuilt.

Deleting transactions cannot increase cluster counts or introduce a new TRUC
version edge or an invalid dust-spending child. A script-flag change can increase
sigop-adjusted sizes, however. Such entries are checked against the surviving
cluster/TRUC bounds, in reverse insertion order. A failing entry and its
descendants are removed before checking the next one. This avoids applying
growth checks to unchanged entries and lets each removal recover capacity.

The expensive contextual validation count is at most N per reconciliation,
including rejected entries. This is not a bound on elapsed time, total
allocations, SCRIPT cost, repeated reconciliation calls or the exceptional
size-growth graph passes. The overlay and rebuilt indices remain proportional
to retained inputs/outputs; owned snapshots and ordinary package admission
still have their separate costs.

## Regression and live acceptance

Six new ordinary regressions cover:

- 8/32/64-entry unchanged pools, exactly one contextual validation per entry,
  no repeated SCRIPT execution, shared payload identity and unchanged base data;
- zero-fee CPFP parents, with and without a completely swept ephemeral dust output;
- confirmed parent removal while the child/grandchild survive, followed by
  loss of that confirmed output and removal of both descendants;
- fresh coinbase maturity, height/MTP finality, height/time relative locks,
  input amount and prevout script failures, preserving an unrelated transaction;
- script-flag commitment invalidation and reuse after the new result is cached;
- actual witness-sigop activation increasing TRUC child and cluster sizes,
  with selective descendant removal and stable subsequent reconciliation.

The new explicitly enabled Core differential submits identical ordinary/dust
CPFP packages to both engines. It mines an empty block, a parent-only block and
a child-only block, checking exact mempool transaction-ID sets after each step.
These are six matched state comparisons, with rBTC's base UTXOs advanced to the
corresponding confirmed outputs. The fixture uses Core's
[generateblock RPC](https://github.com/bitcoin/bitcoin/blob/v31.0/src/rpc/mining.cpp#L281)
to select block contents and runs on the official 31.0 binary. It does not
establish general reorg, network, package-policy or optimizer equivalence.
These live transactions disable relative locks and have no absolute lock;
contextual-lock changes are covered by the unit fixtures above.

The final all-feature run passed **931 tests** (892 library + 39 integration),
with **31 ignored** and no failures. Subprocess-helper output is not counted
twice. All four explicitly enabled Core replacement/package-pressure/fee-decay/
reconciliation differentials passed. Strict all-target/all-feature Clippy,
formatting and diff checks passed. The full suite used four test threads and
did not reproduce the earlier failover-test nonce mismatch; that investigation
remains open. Only documentation changed after final source validation.

## Repeated resource comparison

Both revisions use the same updated `examples/admission_resource_probe.rs`,
Rust 1.85.0, all features, release optimization and mimalloc. The existing
128/256-entry synthetic workload contains a 16-entry chain and independent
transactions with 60,000-byte OP_RETURN outputs, followed by one small funded
admission. Reconciliation therefore measures 129/257 retained transactions.
Eight explicit pool clones remain alive, and an owned comparison snapshot is
taken outside the measured interval in both revisions. The probe requires zero
removals and an unchanged transaction snapshot.

The temporary Redb store is on `/tmp` (tmpfs). These are warm local measurements,
not cold-disk or mainnet results. Each revision ran three times in fresh
processes, alternating which revision ran first, without concurrent builds or
test workloads. Every JSON result and `/usr/bin/time` report is retained.

| Measurement, median of three | 129 baseline | 129 current | 257 baseline | 257 current |
| --- | ---: | ---: | ---: | ---: |
| Reconciliation, ms | 584.949 | 9.073 | 2,676.069 | 21.883 |
| Reconciliation RSS delta, KiB | 6,300 | 0 | 14,728 | 8 |

At 257 entries, baseline reconciliation took 2,494.051–2,706.720 ms and current
reconciliation took 20.917–23.637 ms. Baseline RSS deltas were 14,728–16,036 KiB;
all three current deltas were 8 KiB. Both revisions retained exactly 6,734,451
serialized bytes at 129 entries and 14,429,043 at 257 entries, with identical
transactions and no removals. The median speedups were approximately 64× and
122× respectively. Total seeding/admission cost is not included in these ratios
and remains separately recorded in each sample.

RSS is `/proc/self/status` resident memory, not allocator accounting. Allocator
reuse can hide allocation activity; the pointer-identity regression directly
establishes payload sharing. Measured speedups apply to this workload.

## Reproduction and remaining work

```sh
export CARGO_HOME=/tmp/rbtc-cargo-home
cargo test --locked --all-features --lib reconciliation_ -- --nocapture
cargo test --locked --all-features --no-fail-fast -- --test-threads=4
RBTC_BITCOIND=/absolute/path/to/bitcoin-31.0/bin/bitcoind \
  cargo test --locked --all-features --test core_replacement_differential \
  -- --ignored --nocapture
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo fmt --all --check
cargo build --locked --release --all-features --example admission_resource_probe
/usr/bin/time -v target/release/examples/admission_resource_probe 128 8
/usr/bin/time -v target/release/examples/admission_resource_probe 256 8
```

Use the configured target directory if `CARGO_TARGET_DIR` is set. Copy this
probe and the new differential unchanged into a baseline checkout. Save each
revision's executable before building the other revision.

Local evidence is under
`target/upstream-followup/2026-09-11/admission-reconcile/`, including baseline
failures, focused/full/Core/Clippy logs, release-build logs, all benchmark samples,
environment/source hashes and the final published-source manifest.

Ordinary package admission still clones metadata and replays retained entries.
Whole-pipeline work/allocation quotas across peers, accepted/rejected packages,
snapshots and repeated chain updates remain open. The exceptional size-growth
checks also need accounting within that future budget. Header persistence,
complete optimizer search, full storage scale and the seven-day soak remain
separate [open items](UPSTREAM_OPEN_ITEMS_2026-09-11.md).
