# Feerate-diagram replacement track

Status date: 2026-08-15.

Core 31 accepts a mempool replacement only when the resulting mempool's
*feerate diagram* is strictly better than the original's, with the CPFP
carveout removed. When this track opened, rBTC's replacement was a
documented conservative subset (BIP125 signaling plus a feerate strictly
above one direct conflict), so the same mempool could accept a replacement
here that Core 31 rejects, and vice versa. That divergence is now closed:
the admission integration below adopted the diagram rule and its live
differential gate passed on two native platforms.

The plan deliberately leads with the pure-function layer: a wrong diagram
comparison does not fail — it silently changes which replacements are
accepted — so the risk concentrates in the mathematics and is retired by
property tests and fuzzing *before* any admission path depends on it.

## What is implemented (pure layer, `src/feerate_diagram.rs`)

| Piece | Core counterpart | Notes |
| --- | --- | --- |
| `FeeFrac` | `FeeFrac` | Fee/size fraction; feerate comparison by `i128` cross-multiplication, no division, overflow-free for all `i64`/`i32` inputs |
| `Cluster` | cluster DAG | Validated construction: rejects out-of-range/self parents, non-positive sizes, negative fees, unrepresentable aggregate fee/size, and cycles (iterative DFS, no recursion) |
| `Cluster::linearize` | ancestor-set greedy lineariser | Deterministic tie-breaks; topologically valid by construction; O(n³) worst case, microseconds at the enforced 64-transaction cluster bound, total (just slower) beyond it |
| `chunk_linearization` | `ChunkLinearization` | Merges a strictly-higher-feerate later chunk into its predecessor; equal feerates stay separate, exactly as Core chunks |
| `diagram_points` / `compare_diagrams` | feerate-diagram comparison | Piecewise-linear evaluation at every breakpoint of either diagram, `i128` interpolation; shorter diagrams extend flat; returns Equal / Better / Worse / Incomparable |

Invariants held by tests and the `feerate_diagram` fuzz target:

- linearization is a permutation with every parent before its child;
- chunk feerates are non-increasing and totals are preserved exactly;
- the diagram is concave (slopes never increase);
- comparison is reflexive (`Equal` against itself) and antisymmetric;
- malformed inputs (bad orders, bad graphs) are rejected or ignored,
  never a panic, at and beyond the enforced cluster bound.

## Admission integration (completed 2026-08-14)

The replacement rule in `transaction_admission.rs` now runs the diagram
comparison. `prepare_replacement` captures the affected clusters — the
conflicts' clusters and the clusters of the package's in-pool parents —
and their chunked diagram *before* any mutation; after the package is
appended and the cluster bounds validated, the survivors plus the accepted
transactions are re-clustered and the replacement is accepted only when
`compare_diagrams(new, old)` is strictly `Better`. The BIP125 signaling
requirement, the absolute-fee rule, the incremental-fee rule, and the
100-eviction bound stay as independent gates; the retired rule is only the
per-direct-conflict feerate heuristic, whose feerate question the diagram
now answers exactly. `getmempoolfeeratediagram <txid>` serves the same
chunks the rule compares, beside `getmempoolcluster`.

### Differential gate — passed

The live gate ran on 2026-08-14 against the official Bitcoin Core v31.0.0
binary (`tests/core_replacement_differential.rs`): fourteen verdicts over
six scenarios, all agreeing, including the flagship divergence the old
heuristic decided wrongly (a replacement out-rating its direct conflict
while evicting a rich CPFP descendant — both implementations reject on the
feerate question, where the retired rule accepted). The full scenario list
and residues are recorded in
[CORE31_COMPATIBILITY.md](CORE31_COMPATIBILITY.md).

The gate was then repeated on macOS 26.5.1 arm64 against the verified official
Core 31.0 Darwin artifact: `1 passed; 0 failed` in `12.76s`, with the same
fourteen matching verdicts. This supplies a second native platform for the
closed behavior-divergence gate.

## What deliberately remains open

- **Optimal linearization.** Core refines the ancestor-set greedy result
  with a bounded search. On the differential corpus the greedy baseline
  produced no accept/reject divergence; a corpus that splits them would
  reopen this item, and the differential test is the instrument that would
  catch it.
- **TRUC sibling-eviction ordering** — not implemented; tracked on the
  roadmap item.
- **Rich-parent package-feerate differential under pressure** — the
  exclusion rule itself is implemented and unit-tested; comparing it
  against Core needs a rolling-minimum pressure harness, recorded in
  [CORE31_COMPATIBILITY.md](CORE31_COMPATIBILITY.md).

## macOS fuzz acceptance

On 2026-08-14, the first requested 50,000-execution smoke run found a real
constructor-boundary defect after 4,650 executions. An input containing two
individually representable fees whose sum exceeded `i64::MAX` was accepted as
a `Cluster`; later chunk combination saturated that sum and violated exact
total preservation. Replaying the saved artifact reproduced the assertion.

`Cluster::new` now rejects negative fees and aggregate fee/size values that
cannot be represented by `FeeFrac`. The saved artifact then completed without
a crash, the constructor boundary has direct unit coverage, and a fresh run
completed all 50,000 executions with no crash:

```bash
cd fuzz
cargo +nightly-2026-07-13 fuzz run feerate_diagram \
  corpus/feerate_diagram -- -runs=50000 -max_len=2048
```

This establishes the requested bounded smoke gate and demonstrates why the
fuzz target remains part of the integration gate; it is not an exhaustion
claim for arbitrary cluster graphs.
