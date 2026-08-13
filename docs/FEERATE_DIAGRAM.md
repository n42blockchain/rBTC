# Feerate-diagram replacement track

Status date: 2026-08-13.

Core 31 accepts a mempool replacement only when the resulting mempool's
*feerate diagram* is strictly better than the original's, with the CPFP
carveout removed. rBTC's replacement today is the documented conservative
subset (BIP125 signaling plus a feerate strictly above one direct
conflict), so the same mempool can accept a replacement here that Core 31
rejects, and vice versa. Closing that divergence is this track.

The plan deliberately leads with the pure-function layer: a wrong diagram
comparison does not fail — it silently changes which replacements are
accepted — so the risk concentrates in the mathematics and is retired by
property tests and fuzzing *before* any admission path depends on it.

## What is implemented (pure layer, `src/feerate_diagram.rs`)

| Piece | Core counterpart | Notes |
| --- | --- | --- |
| `FeeFrac` | `FeeFrac` | Fee/size fraction; feerate comparison by `i128` cross-multiplication, no division, overflow-free for all `i64`/`i32` inputs |
| `Cluster` | cluster DAG | Validated construction: rejects out-of-range/self parents, non-positive sizes, and cycles (iterative DFS, no recursion) |
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

## What is deliberately not yet implemented

- **Admission integration.** The replacement rule in
  `transaction_admission.rs` still runs the conservative subset. Swapping
  it to diagram comparison is the next phase of this track and lands only
  with the differential gate below.
- **Optimal linearization.** Core refines the ancestor-set greedy result
  with a bounded search and post-linearization passes. The greedy baseline
  is a valid linearization (Core itself falls back to it under its search
  budget), but chunk boundaries can differ from Core's on some clusters.
  This matters exactly when the admission path starts comparing diagrams,
  which is why the differential gate is part of that phase, not this one.
- **`getmempoolfeeratediagram`.** The RPC serves what the admission path
  computes; it lands with the integration.

## Acceptance gate for the integration phase

Differential replacement decisions against a live Bitcoin Core 31 daemon on
an identical mempool — the same regtest differential harness the consensus
and GBT work already uses — with any deltas recorded in
[CORE31_COMPATIBILITY.md](CORE31_COMPATIBILITY.md). Divergent chunk
boundaries from the greedy-versus-optimal gap must either be closed (by
implementing the search) or shown not to change any accept/reject decision
Core makes on the tested corpus.
