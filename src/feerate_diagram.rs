//! Pure feerate-diagram primitives for Core 31 cluster-mempool replacement.
//!
//! Core 31 accepts a replacement only when the resulting mempool's *feerate
//! diagram* is strictly better than the original's. The diagram is built from
//! a cluster's linearization: transactions are ordered topologically to
//! maximise feerate, that order is chunked into groups of non-increasing
//! feerate, and the cumulative `(size, fee)` points of those chunks trace a
//! concave curve. One diagram dominates another when it is at or above it at
//! every size.
//!
//! This module is the pure-function layer of that mechanism — no mempool
//! state, no admission decisions — so its risk (a wrong comparison silently
//! changing which replacements are accepted) can be retired by property
//! tests and fuzzing before any admission path depends on it. Every
//! arithmetic step is overflow-safe: feerate comparisons cross-multiply in
//! `i128`, and cumulative fees and sizes accumulate in `i128`.
//!
//! The types mirror Core's `cluster_linearize.h`: [`FeeFrac`] is its
//! `FeeFrac`, [`chunk_linearization`] is its `ChunkLinearization`, and
//! [`compare_diagrams`] is its `CompareChunks`/feerate-diagram comparison.
//! The linearization here is the ancestor-set greedy baseline; Core's
//! optimal search and post-linearisation are refinements deferred to the
//! admission-integration phase, where a live differential against Core's
//! `cluster_linearize` is the acceptance gate.

use std::cmp::Ordering;

/// A fee amount paired with a virtual size, compared by feerate.
///
/// `fee` is in satoshis and `size` in virtual bytes, matching Core's
/// `FeeFrac`. A zero-size fraction has no feerate and sorts below every
/// positive-size fraction, so it can only appear as the empty accumulator.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FeeFrac {
    /// Total fee in satoshis.
    pub fee: i64,
    /// Total virtual size in vbytes; never negative.
    pub size: i32,
}

impl FeeFrac {
    /// The empty fraction: zero fee over zero size.
    pub const ZERO: Self = Self { fee: 0, size: 0 };

    /// Creates a fraction, saturating a negative size to zero.
    #[must_use]
    pub const fn new(fee: i64, size: i32) -> Self {
        Self {
            fee,
            size: if size < 0 { 0 } else { size },
        }
    }

    /// Combines two fractions, accumulating in `i128` to avoid overflow.
    #[must_use]
    pub fn combined(self, other: Self) -> Self {
        let fee = i128::from(self.fee) + i128::from(other.fee);
        let size = i128::from(self.size) + i128::from(other.size);
        Self {
            fee: i64::try_from(fee).unwrap_or(if fee < 0 { i64::MIN } else { i64::MAX }),
            size: i32::try_from(size).unwrap_or(i32::MAX),
        }
    }

    /// Orders two fractions by feerate (`fee/size`), higher feerate first.
    ///
    /// Uses the cross-multiplication `self.fee * other.size` versus
    /// `other.fee * self.size` in `i128`, so it never divides and never
    /// overflows for any `i64`/`i32` inputs. A zero-size fraction has no
    /// feerate and compares below any positive-size fraction; two zero-size
    /// fractions are equal.
    #[must_use]
    pub fn feerate_cmp(self, other: Self) -> Ordering {
        match (self.size == 0, other.size == 0) {
            (true, true) => Ordering::Equal,
            (true, false) => Ordering::Less,
            (false, true) => Ordering::Greater,
            (false, false) => {
                let left = i128::from(self.fee) * i128::from(other.size);
                let right = i128::from(other.fee) * i128::from(self.size);
                left.cmp(&right)
            }
        }
    }
}

/// A cluster of dependency-connected transactions, keyed by index.
///
/// `parents[i]` lists the direct predecessors transaction `i` depends on
/// (the in-cluster transactions it spends). A valid linearization places
/// every parent before its child.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Cluster {
    entries: Vec<FeeFrac>,
    parents: Vec<Vec<usize>>,
}

/// Reasons a cluster description is not well-formed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ClusterError {
    /// A parent index is out of range.
    ParentOutOfRange,
    /// A transaction lists itself as a parent.
    SelfParent,
    /// The dependency graph contains a cycle.
    Cycle,
    /// A transaction has non-positive virtual size.
    NonPositiveSize,
    /// A transaction has a negative fee, which cannot enter the mempool.
    NegativeFee,
    /// Aggregate fee or size cannot be represented by [`FeeFrac`].
    AggregateOutOfRange,
}

impl Cluster {
    /// Builds a cluster from per-transaction `(fee, size)` fractions and the
    /// direct-parent adjacency list.
    ///
    /// Rejects out-of-range or self parents, non-positive sizes, negative fees,
    /// aggregate values that cannot be represented by [`FeeFrac`], and cycles,
    /// so every later operation can assume a valid mempool DAG whose ancestor
    /// sets combine without saturation.
    pub fn new(entries: Vec<FeeFrac>, parents: Vec<Vec<usize>>) -> Result<Self, ClusterError> {
        if entries.len() != parents.len() {
            return Err(ClusterError::ParentOutOfRange);
        }
        let mut total_fee = 0_i128;
        let mut total_size = 0_i128;
        for entry in &entries {
            if entry.size <= 0 {
                return Err(ClusterError::NonPositiveSize);
            }
            if entry.fee < 0 {
                return Err(ClusterError::NegativeFee);
            }
            total_fee += i128::from(entry.fee);
            total_size += i128::from(entry.size);
        }
        if total_fee > i128::from(i64::MAX) || total_size > i128::from(i32::MAX) {
            return Err(ClusterError::AggregateOutOfRange);
        }
        for (child, list) in parents.iter().enumerate() {
            for &parent in list {
                if parent >= entries.len() {
                    return Err(ClusterError::ParentOutOfRange);
                }
                if parent == child {
                    return Err(ClusterError::SelfParent);
                }
            }
        }
        let cluster = Self { entries, parents };
        if cluster.has_cycle() {
            return Err(ClusterError::Cycle);
        }
        Ok(cluster)
    }

    /// Number of transactions in the cluster.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the cluster is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    fn has_cycle(&self) -> bool {
        // 0 = unvisited, 1 = on stack, 2 = done.
        let mut state = vec![0_u8; self.entries.len()];
        // Iterative DFS to avoid stack overflow on deep clusters.
        for start in 0..self.entries.len() {
            if state[start] != 0 {
                continue;
            }
            let mut stack = vec![(start, 0_usize)];
            state[start] = 1;
            while let Some(&(node, index)) = stack.last() {
                if index < self.parents[node].len() {
                    stack.last_mut().expect("stack non-empty").1 += 1;
                    let parent = self.parents[node][index];
                    match state.get(parent).copied().unwrap_or(2) {
                        0 => {
                            state[parent] = 1;
                            stack.push((parent, 0));
                        }
                        1 => return true,
                        _ => {}
                    }
                } else {
                    state[node] = 2;
                    stack.pop();
                }
            }
        }
        false
    }

    /// Transitive ancestors of `node` (excluding `node`) restricted to
    /// `remaining`.
    fn ancestors_in(&self, node: usize, remaining: &[bool]) -> Vec<usize> {
        let mut seen = vec![false; self.entries.len()];
        let mut collected = Vec::new();
        let mut stack = vec![node];
        seen[node] = true;
        while let Some(current) = stack.pop() {
            for &parent in &self.parents[current] {
                if !seen[parent] && remaining[parent] {
                    seen[parent] = true;
                    collected.push(parent);
                    stack.push(parent);
                }
            }
        }
        collected
    }

    /// Linearizes the cluster into a topologically valid order.
    ///
    /// Ancestor-set greedy, Core's baseline lineariser: repeatedly select the
    /// remaining transaction whose still-remaining ancestor set has the
    /// highest feerate, then emit that whole set in topological order. Ties
    /// are broken deterministically by the smaller ancestor-set size and then
    /// the smaller anchor index, so the output depends only on the input.
    ///
    /// Worst case is O(n³) in the cluster size; admission enforces a
    /// 64-transaction cluster bound, where that is microseconds. The
    /// function stays total for larger inputs (fuzzing feeds them), just not
    /// fast.
    ///
    /// The result is a permutation of `0..len` in which every parent precedes
    /// its children.
    #[must_use]
    pub fn linearize(&self) -> Vec<usize> {
        let count = self.entries.len();
        let mut remaining = vec![true; count];
        let mut order = Vec::with_capacity(count);
        let mut left = count;
        while left > 0 {
            let mut best: Option<(FeeFrac, usize, Vec<usize>)> = None;
            for anchor in 0..count {
                if !remaining[anchor] {
                    continue;
                }
                let mut set = self.ancestors_in(anchor, &remaining);
                set.push(anchor);
                let feerate = set.iter().fold(FeeFrac::ZERO, |total, &index| {
                    total.combined(self.entries[index])
                });
                let candidate_len = set.len();
                let better = match &best {
                    None => true,
                    Some((best_rate, best_len, _)) => {
                        match feerate.feerate_cmp(*best_rate) {
                            Ordering::Greater => true,
                            Ordering::Less => false,
                            // Deterministic tie-break: fewer transactions,
                            // then the smaller anchor index (the anchor is the
                            // largest index in a set closed under ancestry, so
                            // comparing anchors is stable).
                            Ordering::Equal => candidate_len < *best_len,
                        }
                    }
                };
                if better {
                    best = Some((feerate, candidate_len, set));
                }
            }
            let (_, _, set) = best.expect("a non-empty remaining set has a best anchor");
            for index in self.topological_order(&set) {
                remaining[index] = false;
                order.push(index);
                left -= 1;
            }
        }
        order
    }

    /// Orders a subset closed under ancestry so every parent precedes its
    /// child. The subset is small (one ancestor set), so a simple Kahn pass
    /// over it is enough.
    fn topological_order(&self, subset: &[usize]) -> Vec<usize> {
        let in_subset = {
            let mut flags = vec![false; self.entries.len()];
            for &index in subset {
                flags[index] = true;
            }
            flags
        };
        let mut emitted = vec![false; self.entries.len()];
        let mut order = Vec::with_capacity(subset.len());
        // Repeatedly emit any subset node whose in-subset parents are all
        // emitted. Deterministic by scanning ascending indices.
        while order.len() < subset.len() {
            let mut progressed = false;
            for &index in subset {
                if emitted[index] {
                    continue;
                }
                let ready = self.parents[index]
                    .iter()
                    .all(|&parent| !in_subset[parent] || emitted[parent]);
                if ready {
                    emitted[index] = true;
                    order.push(index);
                    progressed = true;
                }
            }
            assert!(progressed, "an ancestor-closed subset is acyclic");
        }
        order
    }

    /// Returns the fee fraction of each transaction, in cluster index order.
    #[must_use]
    pub fn fractions(&self) -> &[FeeFrac] {
        &self.entries
    }
}

/// Chunks a linearization into groups of non-increasing feerate.
///
/// Port of Core's `ChunkLinearization`: each transaction starts as its own
/// chunk; whenever a chunk has strictly higher feerate than the chunk before
/// it, the two merge, because a higher-feerate chunk after a lower one would
/// break the diagram's concavity. The result is a list of chunk fractions in
/// non-increasing feerate order.
///
/// `order` must be a permutation of `0..fractions.len()`; out-of-range or
/// duplicate indices are ignored so a malformed order cannot panic.
#[must_use]
pub fn chunk_linearization(fractions: &[FeeFrac], order: &[usize]) -> Vec<FeeFrac> {
    let mut chunks: Vec<FeeFrac> = Vec::with_capacity(order.len());
    let mut seen = vec![false; fractions.len()];
    for &index in order {
        let Some(entry) = fractions.get(index) else {
            continue;
        };
        if seen[index] {
            continue;
        }
        seen[index] = true;
        let mut chunk = *entry;
        while let Some(&previous) = chunks.last() {
            if previous.feerate_cmp(chunk) == Ordering::Less {
                chunk = chunk.combined(previous);
                chunks.pop();
            } else {
                break;
            }
        }
        chunks.push(chunk);
    }
    chunks
}

/// A cumulative `(size, fee)` point on a feerate diagram.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DiagramPoint {
    /// Cumulative virtual size.
    pub size: i128,
    /// Cumulative fee.
    pub fee: i128,
}

/// Builds the cumulative feerate diagram from chunk fractions.
///
/// The returned points start at `(0, 0)` and add each chunk's `(size, fee)`
/// in turn, so the curve is concave when the chunks are in non-increasing
/// feerate order (as [`chunk_linearization`] produces). Accumulation is in
/// `i128`.
#[must_use]
pub fn diagram_points(chunks: &[FeeFrac]) -> Vec<DiagramPoint> {
    let mut points = Vec::with_capacity(chunks.len() + 1);
    let mut cumulative = DiagramPoint { size: 0, fee: 0 };
    points.push(cumulative);
    for chunk in chunks {
        cumulative = DiagramPoint {
            size: cumulative.size + i128::from(chunk.size),
            fee: cumulative.fee + i128::from(chunk.fee),
        };
        points.push(cumulative);
    }
    points
}

/// The result of comparing two feerate diagrams.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DiagramComparison {
    /// The diagrams are equal at every size.
    Equal,
    /// The first diagram is at or above the second everywhere, and strictly
    /// above at some size.
    Better,
    /// The first diagram is at or below the second everywhere, and strictly
    /// below at some size.
    Worse,
    /// Neither diagram dominates: each is strictly above the other somewhere.
    Incomparable,
}

/// Interpolated cumulative fee of a concave diagram at cumulative size `at`.
///
/// Beyond the diagram's total size the fee is flat at its maximum, matching
/// Core's treatment of a shorter diagram as extended horizontally so two
/// diagrams of different total size remain comparable.
fn fee_at(points: &[DiagramPoint], at: i128) -> i128 {
    if points.is_empty() {
        return 0;
    }
    let last = points[points.len() - 1];
    if at >= last.size {
        return last.fee;
    }
    for window in points.windows(2) {
        let (a, b) = (window[0], window[1]);
        if at >= a.size && at <= b.size {
            let run = b.size - a.size;
            if run == 0 {
                return a.fee.max(b.fee);
            }
            let rise = b.fee - a.fee;
            // Linear interpolation in i128: a.fee + rise * (at - a.size) / run.
            return a.fee + rise * (at - a.size) / run;
        }
    }
    points[0].fee
}

/// Compares two feerate diagrams given their chunk fractions.
///
/// Evaluates both diagrams at every cumulative-size breakpoint of either one
/// — the only places their difference can change sign, since both are
/// piecewise linear — and reports whether the first dominates, is dominated,
/// equals, or is incomparable to the second. This is Core's feerate-diagram
/// comparison, the rule a cluster-mempool replacement must satisfy.
#[must_use]
pub fn compare_diagrams(first: &[FeeFrac], second: &[FeeFrac]) -> DiagramComparison {
    let left = diagram_points(first);
    let right = diagram_points(second);
    let mut breakpoints: Vec<i128> = Vec::with_capacity(left.len() + right.len());
    for point in left.iter().chain(right.iter()) {
        breakpoints.push(point.size);
    }
    breakpoints.sort_unstable();
    breakpoints.dedup();

    let mut first_ahead = false;
    let mut second_ahead = false;
    for size in breakpoints {
        let diff = fee_at(&left, size) - fee_at(&right, size);
        match diff.cmp(&0) {
            Ordering::Greater => first_ahead = true,
            Ordering::Less => second_ahead = true,
            Ordering::Equal => {}
        }
    }
    match (first_ahead, second_ahead) {
        (false, false) => DiagramComparison::Equal,
        (true, false) => DiagramComparison::Better,
        (false, true) => DiagramComparison::Worse,
        (true, true) => DiagramComparison::Incomparable,
    }
}

/// Convenience: linearize, chunk, and return the diagram chunks for a cluster.
#[must_use]
pub fn cluster_diagram(cluster: &Cluster) -> Vec<FeeFrac> {
    let order = cluster.linearize();
    chunk_linearization(cluster.fractions(), &order)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn frac(fee: i64, size: i32) -> FeeFrac {
        FeeFrac::new(fee, size)
    }

    #[test]
    fn feerate_comparison_is_exact_and_overflow_safe() {
        // 10/2 = 5 vs 9/3 = 3 → first is greater.
        assert_eq!(frac(10, 2).feerate_cmp(frac(9, 3)), Ordering::Greater);
        // Equal feerates by cross product: 6/3 == 4/2.
        assert_eq!(frac(6, 3).feerate_cmp(frac(4, 2)), Ordering::Equal);
        // Extreme values must not overflow the i128 cross product.
        assert_eq!(
            frac(i64::MAX, 1).feerate_cmp(frac(i64::MAX, i32::MAX)),
            Ordering::Greater
        );
        // A zero-size accumulator sorts below any real fraction.
        assert_eq!(FeeFrac::ZERO.feerate_cmp(frac(1, 1)), Ordering::Less);
        assert_eq!(FeeFrac::ZERO.feerate_cmp(FeeFrac::ZERO), Ordering::Equal);
    }

    #[test]
    fn chunking_merges_ascending_feerate_into_non_increasing_chunks() {
        // A low-feerate parent (1 sat/vB) followed by a high-feerate child
        // (10 sat/vB) merge into one chunk, because the child cannot form a
        // higher-feerate chunk after its parent.
        let fractions = [frac(1, 1), frac(10, 1)];
        let chunks = chunk_linearization(&fractions, &[0, 1]);
        assert_eq!(chunks, vec![frac(11, 2)]);

        // A high-feerate first, low second: two chunks, strictly decreasing.
        let fractions = [frac(10, 1), frac(1, 1)];
        let chunks = chunk_linearization(&fractions, &[0, 1]);
        assert_eq!(chunks, vec![frac(10, 1), frac(1, 1)]);
        // Non-increasing feerate order holds.
        for window in chunks.windows(2) {
            assert_ne!(window[0].feerate_cmp(window[1]), Ordering::Less);
        }
    }

    #[test]
    fn chunking_ignores_malformed_orders_without_panicking() {
        let fractions = [frac(5, 1), frac(3, 1)];
        // Out-of-range and duplicate indices are skipped.
        let chunks = chunk_linearization(&fractions, &[9, 0, 0, 1, 42]);
        assert_eq!(chunks, vec![frac(5, 1), frac(3, 1)]);
    }

    #[test]
    fn linearization_is_topological_and_orders_a_cpfp_by_ancestor_feerate() {
        // 0: low-fee parent (1 sat/vB); 1: high-fee child spending 0
        // (100 sat/vB); 2: independent mid-fee (5 sat/vB).
        // The child cannot be chosen before its parent, and its ancestor set
        // {0,1} has feerate 101/2 ≈ 50, which beats the independent 5, so the
        // package leads.
        let cluster = Cluster::new(
            vec![frac(1, 1), frac(100, 1), frac(5, 1)],
            vec![vec![], vec![0], vec![]],
        )
        .unwrap();
        let order = cluster.linearize();
        // Parent precedes child.
        let pos = |x: usize| order.iter().position(|&i| i == x).unwrap();
        assert!(pos(0) < pos(1));
        assert_eq!(order.len(), 3);
        // The CPFP package (0 then 1) leads the independent transaction.
        assert!(pos(1) < pos(2));

        let chunks = cluster_diagram(&cluster);
        // First chunk is the {0,1} package at 101/2; second is {2} at 5/1.
        assert_eq!(chunks, vec![frac(101, 2), frac(5, 1)]);
    }

    #[test]
    fn diagram_of_a_linearization_is_concave() {
        let cluster = Cluster::new(
            vec![frac(1, 3), frac(40, 2), frac(9, 1), frac(2, 5)],
            vec![vec![], vec![0], vec![], vec![2]],
        )
        .unwrap();
        let chunks = cluster_diagram(&cluster);
        for window in chunks.windows(2) {
            assert_ne!(
                window[0].feerate_cmp(window[1]),
                Ordering::Less,
                "chunk feerates are non-increasing"
            );
        }
        let points = diagram_points(&chunks);
        // Concavity: each successive segment's slope does not increase.
        for triple in points.windows(3) {
            let (a, b, c) = (triple[0], triple[1], triple[2]);
            let slope_ab = (b.fee - a.fee) * (c.size - b.size);
            let slope_bc = (c.fee - b.fee) * (b.size - a.size);
            assert!(slope_ab >= slope_bc, "slopes are non-increasing");
        }
    }

    #[test]
    fn diagram_comparison_detects_domination_equality_and_incomparability() {
        // B strictly dominates A: same size, more fee.
        let a = [frac(10, 2)];
        let b = [frac(20, 2)];
        assert_eq!(compare_diagrams(&b, &a), DiagramComparison::Better);
        assert_eq!(compare_diagrams(&a, &b), DiagramComparison::Worse);
        assert_eq!(compare_diagrams(&a, &a), DiagramComparison::Equal);

        // Crossing diagrams: A is higher early, B is higher later.
        // A: one chunk 10/1 then flat → at size 1 fee 10; at size 3 fee 10.
        // B: one chunk 12/3 → at size 1 fee 4; at size 3 fee 12.
        let a = [frac(10, 1)];
        let b = [frac(12, 3)];
        assert_eq!(compare_diagrams(&a, &b), DiagramComparison::Incomparable);
        assert_eq!(compare_diagrams(&b, &a), DiagramComparison::Incomparable);
    }

    #[test]
    fn cluster_construction_rejects_malformed_graphs() {
        assert_eq!(
            Cluster::new(vec![frac(1, 1)], vec![vec![5]]),
            Err(ClusterError::ParentOutOfRange)
        );
        assert_eq!(
            Cluster::new(vec![frac(1, 1)], vec![vec![0]]),
            Err(ClusterError::SelfParent)
        );
        assert_eq!(
            Cluster::new(vec![frac(1, 0)], vec![vec![]]),
            Err(ClusterError::NonPositiveSize)
        );
        assert_eq!(
            Cluster::new(vec![frac(-1, 1)], vec![vec![]]),
            Err(ClusterError::NegativeFee)
        );
        assert_eq!(
            Cluster::new(vec![frac(i64::MAX, 1), frac(1, 1)], vec![vec![], vec![]]),
            Err(ClusterError::AggregateOutOfRange)
        );
        assert_eq!(
            Cluster::new(vec![frac(1, i32::MAX), frac(1, 1)], vec![vec![], vec![]]),
            Err(ClusterError::AggregateOutOfRange)
        );
        // 0 → 1 → 0 is a cycle.
        assert_eq!(
            Cluster::new(vec![frac(1, 1), frac(1, 1)], vec![vec![1], vec![0]]),
            Err(ClusterError::Cycle)
        );
    }

    #[test]
    fn a_deep_chain_neither_overflows_nor_recurses_to_death() {
        // A chain much deeper than the enforced 64-transaction cluster bound
        // would blow a recursive DFS; the iterative cycle check and
        // linearizer must handle it. Fees near i64::MAX exercise the wide
        // accumulation.
        let count = 300;
        let entries = (0..count)
            .map(|_| frac(i64::MAX / i64::try_from(count).unwrap(), 1))
            .collect::<Vec<_>>();
        let parents = (0..count)
            .map(|i| if i == 0 { vec![] } else { vec![i - 1] })
            .collect::<Vec<_>>();
        let cluster = Cluster::new(entries, parents).unwrap();
        let order = cluster.linearize();
        assert_eq!(order, (0..count).collect::<Vec<_>>());
        let chunks = cluster_diagram(&cluster);
        // Equal-feerate chunks stay separate (merging requires a strictly
        // higher later chunk, exactly as Core chunks), and the totals are
        // preserved exactly.
        assert_eq!(chunks.len(), count);
        let total = chunks
            .iter()
            .fold(FeeFrac::ZERO, |total, &chunk| total.combined(chunk));
        assert_eq!(
            total.fee,
            (i64::MAX / i64::try_from(count).unwrap()) * i64::try_from(count).unwrap()
        );
        assert_eq!(total.size, i32::try_from(count).unwrap());
        for window in chunks.windows(2) {
            assert_ne!(window[0].feerate_cmp(window[1]), Ordering::Less);
        }
    }
}
