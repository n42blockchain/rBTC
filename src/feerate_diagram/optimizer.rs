//! Budgeted maximum-density closure search for clusters of at most 64 entries.
//!
//! At rate f/s a closure has weight sum(fee*s - f*size). A source/sink
//! minimum cut maximizes that weight, with child-to-parent arcs enforcing
//! ancestry. Positive weight improves the rate; zero proves maximal density.
//! Terminal residual components then identify minimal optimal chunks. Only
//! proved chunks are published: interrupted searches retain the old suffix.

use super::{Cluster, FeeFrac, chunk_linearization_with_members};
use std::cmp::Ordering;

/// Search work allowance used by the production cluster linearizer.
pub const DEFAULT_OPTIMIZER_WORK: u64 = 1_000_000;

/// A topological order and the scope of its optimization proof.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LinearizationResult {
    /// Parent-before-child transaction indices.
    pub order: Vec<usize>,
    /// Every output chunk is proved optimal and minimal.
    pub optimal: bool,
    /// Search operations charged, never exceeding the requested allowance.
    /// Bounded initial ordering/refinement and output construction are separate.
    pub work_used: u64,
    /// A valid supplied order was retained as the non-worsening baseline.
    pub previous_reused: bool,
}

struct Work {
    remaining: u64,
    used: u64,
}

impl Work {
    fn spend(&mut self, amount: u64) -> Option<()> {
        self.remaining = self.remaining.checked_sub(amount)?;
        self.used += amount;
        Some(())
    }
}

fn indices(mut mask: u64) -> impl Iterator<Item = usize> {
    std::iter::from_fn(move || {
        if mask == 0 {
            None
        } else {
            let index = mask.trailing_zeros() as usize;
            mask &= mask - 1;
            Some(index)
        }
    })
}

impl Cluster {
    /// Improves a previous order with a bounded, integer-only closure search.
    ///
    /// A malformed or non-topological previous order is discarded. With no
    /// search allowance, returns the bounded greedy/refinement baseline (or a
    /// refinement of the valid previous order). A partial search publishes only
    /// proved maximum-density chunks and preserves the old order of the suffix,
    /// so budget exhaustion cannot worsen a valid input's fee diagram.
    ///
    /// The allowance bounds search edge visits and residual reachability work.
    /// Initial ordering/refinement and final topological output are separately
    /// bounded by the 64-entry limit; this is not a node-wide admission budget.
    /// Larger pure-function inputs retain the existing greedy fallback and
    /// never claim an optimality proof.
    #[must_use]
    pub fn linearize_with_budget(
        &self,
        previous: Option<&[usize]>,
        max_work: u64,
    ) -> LinearizationResult {
        let previous = previous.filter(|order| self.valid_order(order));
        let mut baseline = previous.map_or_else(|| self.linearize_ancestors(), <[usize]>::to_vec);
        let mut result = LinearizationResult {
            order: Vec::with_capacity(self.len()),
            optimal: false,
            work_used: 0,
            previous_reused: previous.is_some(),
        };
        if self.len() > 64 {
            result.order = baseline;
            return result;
        }
        self.post_linearize(&mut baseline);
        if self.is_empty() {
            result.optimal = true;
            return result;
        }
        let mut work = Work {
            remaining: max_work,
            used: 0,
        };
        let mut remaining = u64::MAX >> (64 - self.len());
        while remaining != 0 {
            let suffix = baseline
                .iter()
                .copied()
                .filter(|index| remaining & (1 << index) != 0)
                .collect::<Vec<_>>();
            let rate = chunk_linearization_with_members(&self.entries, &suffix)[0].fraction;
            let Some(chunk) = self.optimal_chunk(remaining, rate, &mut work) else {
                result.order.extend(suffix);
                break;
            };
            self.append_chunk_order(chunk, &mut result.order);
            remaining &= !chunk;
        }
        result.optimal = remaining == 0;
        result.work_used = work.used;
        result
    }

    fn valid_order(&self, order: &[usize]) -> bool {
        if order.len() != self.len() {
            return false;
        }
        let mut positions = vec![usize::MAX; self.len()];
        for (position, &index) in order.iter().enumerate() {
            if index >= self.len() || positions[index] != usize::MAX {
                return false;
            }
            positions[index] = position;
        }
        self.parents.iter().enumerate().all(|(child, parents)| {
            parents
                .iter()
                .all(|&parent| positions[parent] < positions[child])
        })
    }

    fn fraction_for(&self, mask: u64) -> FeeFrac {
        indices(mask).fold(FeeFrac::ZERO, |total, index| {
            total.combined(self.entries[index])
        })
    }

    fn optimal_chunk(&self, remaining: u64, mut rate: FeeFrac, work: &mut Work) -> Option<u64> {
        loop {
            let (residual, improvement) = self.maximum_closure(remaining, rate, work)?;
            if improvement != 0 {
                let next = self.fraction_for(improvement);
                debug_assert_eq!(next.feerate_cmp(rate), Ordering::Greater);
                rate = next;
            } else {
                return self.minimal_chunk(remaining, &residual, work);
            }
        }
    }

    /// Returns a maximum positive-weight closure, or zero if the rate is optimal.
    fn maximum_closure(
        &self,
        remaining: u64,
        rate: FeeFrac,
        work: &mut Work,
    ) -> Option<(Vec<Vec<i128>>, u64)> {
        let count = self.len();
        let source = count;
        let sink = count + 1;
        let vertices = count + 2;
        work.spend(u64::try_from(vertices * vertices + count).ok()?)?;
        let mut capacity = vec![vec![0_i128; vertices]; vertices];
        let mut positive = 0_i128;
        for index in indices(remaining) {
            let fee = self.entries[index];
            let weight = i128::from(fee.fee) * i128::from(rate.size)
                - i128::from(rate.fee) * i128::from(fee.size);
            if weight > 0 {
                capacity[source][index] = weight;
                positive += weight;
            } else {
                capacity[index][sink] = -weight;
            }
        }
        // An ancestry cut must cost more than the entire finite source cut.
        let infinity = positive + 1;
        for child in indices(remaining) {
            for &parent in &self.parents[child] {
                work.spend(1)?;
                if remaining & (1 << parent) != 0 {
                    capacity[child][parent] = infinity;
                }
            }
        }
        let mut flow = 0;
        loop {
            let mut parent = vec![usize::MAX; vertices];
            parent[source] = source;
            let mut queue = Vec::with_capacity(vertices);
            queue.push(source);
            let mut offset = 0;
            while offset < queue.len() && parent[sink] == usize::MAX {
                let from = queue[offset];
                offset += 1;
                for (to, previous) in parent.iter_mut().enumerate() {
                    work.spend(1)?;
                    if *previous == usize::MAX && capacity[from][to] > 0 {
                        *previous = from;
                        queue.push(to);
                    }
                }
            }
            if parent[sink] == usize::MAX {
                let closure = if flow == positive {
                    0
                } else {
                    indices(remaining)
                        .filter(|&i| parent[i] != usize::MAX)
                        .fold(0, |mask, i| mask | (1 << i))
                };
                return Some((capacity, closure));
            }
            let mut amount = positive - flow;
            let mut to = sink;
            while to != source {
                work.spend(1)?;
                amount = amount.min(capacity[parent[to]][to]);
                to = parent[to];
            }
            to = sink;
            while to != source {
                work.spend(1)?;
                let from = parent[to];
                capacity[from][to] -= amount;
                capacity[to][from] += amount;
                to = from;
            }
            flow += amount;
        }
    }

    /// Terminal transaction SCCs of an optimal residual network are the
    /// inclusion-minimal nonempty zero-weight closures. Ignore arcs to the
    /// source (always selected), and disallow components reaching the sink.
    fn minimal_chunk(
        &self,
        remaining: u64,
        residual: &[Vec<i128>],
        work: &mut Work,
    ) -> Option<u64> {
        let mut reachable = [0_u64; 64];
        let mut reaches_sink = [false; 64];
        for i in indices(remaining) {
            reachable[i] = 1 << i;
            reaches_sink[i] = residual[i][self.len() + 1] > 0;
            for j in indices(remaining) {
                work.spend(1)?;
                if residual[i][j] > 0 {
                    reachable[i] |= 1 << j;
                }
            }
        }
        for k in indices(remaining) {
            for i in indices(remaining) {
                work.spend(1)?;
                if reachable[i] & (1 << k) != 0 {
                    reachable[i] |= reachable[k];
                    reaches_sink[i] |= reaches_sink[k];
                }
            }
        }
        let mut best = None;
        for i in indices(remaining) {
            let mask = reachable[i];
            if reaches_sink[i] {
                continue;
            }
            let mut terminal = true;
            for j in indices(mask) {
                work.spend(1)?;
                terminal &= reachable[j] == mask;
            }
            if !terminal {
                continue;
            }
            let size = self.fraction_for(mask).size;
            // Core's equal-rate chunk tie-break: size, then last transaction.
            let key = (size, 63 - mask.leading_zeros());
            if best.is_none_or(|(old, _)| key < old) {
                best = Some((key, mask));
            }
        }
        debug_assert!(
            best.is_some(),
            "an optimal nonempty closure has a terminal component"
        );
        best.map(|(_, mask)| mask)
    }

    fn append_chunk_order(&self, mut chunk: u64, order: &mut Vec<usize>) {
        while chunk != 0 {
            let next = indices(chunk)
                .filter(|&i| self.parents[i].iter().all(|&p| chunk & (1 << p) == 0))
                .max_by(|&a, &b| {
                    self.entries[a]
                        .feerate_cmp(self.entries[b])
                        .then_with(|| self.entries[b].size.cmp(&self.entries[a].size))
                        .then_with(|| b.cmp(&a))
                })
                .expect("validated DAG has a topological next entry");
            order.push(next);
            chunk &= !(1 << next);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::feerate_diagram::{DiagramComparison, chunk_linearization, compare_diagrams};

    fn next(seed: &mut u64) -> u64 {
        *seed = seed
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        *seed
    }

    fn generated(seed: &mut u64, count: usize) -> Cluster {
        let fees = (0..count)
            .map(|_| {
                FeeFrac::new(
                    i64::try_from(next(seed) % 10000).unwrap(),
                    i32::try_from(next(seed) % 1000 + 1).unwrap(),
                )
            })
            .collect();
        let parents = (0..count)
            .map(|child| (0..child).filter(|_| next(seed) >> 61 == 0).collect())
            .collect();
        Cluster::new(fees, parents).unwrap()
    }

    fn assert_optimal_by_subsets(cluster: &Cluster, result: &LinearizationResult) {
        assert!(result.optimal);
        assert!(cluster.valid_order(&result.order));
        let mut remaining = (1_u64 << cluster.len()) - 1;
        for chunk in chunk_linearization_with_members(&cluster.entries, &result.order) {
            let chosen = chunk.members.iter().fold(0, |mask, &i| mask | (1 << i));
            let mut subset = remaining;
            while subset != 0 {
                let closed = indices(subset).all(|i| {
                    cluster.parents[i]
                        .iter()
                        .all(|&p| remaining & (1 << p) == 0 || subset & (1 << p) != 0)
                });
                if closed {
                    let rate = cluster.fraction_for(subset);
                    assert_ne!(rate.feerate_cmp(chunk.fraction), Ordering::Greater);
                    if subset & !chosen == 0 && subset != chosen {
                        assert_ne!(
                            rate.feerate_cmp(chunk.fraction),
                            Ordering::Equal,
                            "proved chunks must have no proper equally good prefix"
                        );
                    }
                }
                subset = (subset - 1) & remaining;
            }
            remaining &= !chosen;
        }
        assert_eq!(remaining, 0);
    }

    #[test]
    fn closure_optimizer_proves_optimal_minimal_chunks_against_all_small_subsets() {
        let mut seed = 17;
        for case in 0..512 {
            let cluster = generated(&mut seed, case % 8);
            let result = cluster.linearize_with_budget(None, 2_000_000);
            assert_optimal_by_subsets(&cluster, &result);
        }
    }

    #[test]
    fn exhausted_search_never_worsens_a_valid_previous_diagram() {
        let mut seed = 35;
        for count in 0..=64 {
            let cluster = generated(&mut seed, count);
            let old = (0..count).collect::<Vec<_>>();
            for budget in [0, 1, 100, 2000, 10_000, 100_000, 1_000_000] {
                let result = cluster.linearize_with_budget(Some(&old), budget);
                assert!(result.previous_reused);
                assert!(result.work_used <= budget);
                assert!(cluster.valid_order(&result.order));
                let comparison = compare_diagrams(
                    &chunk_linearization(&cluster.entries, &result.order),
                    &chunk_linearization(&cluster.entries, &old),
                );
                assert!(
                    matches!(
                        comparison,
                        DiagramComparison::Better | DiagramComparison::Equal
                    ),
                    "count={count} budget={budget}: {comparison:?}"
                );
            }
        }
    }

    #[test]
    fn invalid_previous_orders_are_rejected_without_losing_topology() {
        let cluster =
            Cluster::new(vec![FeeFrac::new(1, 1); 3], vec![vec![], vec![0], vec![1]]).unwrap();
        for old in [vec![], vec![0, 0, 1], vec![3, 0, 1], vec![2, 1, 0]] {
            let result = cluster.linearize_with_budget(Some(&old), 100_000);
            assert!(!result.previous_reused);
            assert_eq!(result.order, vec![0, 1, 2]);
            assert!(result.optimal);
        }
    }

    #[test]
    fn zero_equal_and_near_limit_fractions_keep_exact_proofs() {
        for entries in [
            vec![FeeFrac::new(0, 1); 7],
            vec![FeeFrac::new(10, 2); 7],
            vec![FeeFrac::new(i64::MAX / 7, i32::MAX / 7); 7],
        ] {
            let cluster = Cluster::new(
                entries,
                vec![
                    vec![],
                    vec![],
                    vec![0, 1],
                    vec![1],
                    vec![2, 3],
                    vec![0],
                    vec![4, 5],
                ],
            )
            .unwrap();
            assert_optimal_by_subsets(&cluster, &cluster.linearize_with_budget(None, 1_000_000));
        }
    }
    fn permute_labels(cluster: &Cluster, seed: &mut u64) -> (Cluster, Vec<usize>) {
        let count = cluster.len();
        let mut old = (0..count).collect::<Vec<_>>();
        // Relabel vertices so parents need not have smaller indices.
        // Keep the old topological sequence in the new index space.
        for i in (1..count).rev() {
            let other = usize::try_from(next(seed) % u64::try_from(i + 1).unwrap()).unwrap();
            old.swap(i, other);
        }
        let mut entries = vec![FeeFrac::ZERO; count];
        let mut parents = vec![Vec::new(); count];
        for i in 0..count {
            entries[old[i]] = cluster.entries[i];
            parents[old[i]] = cluster.parents[i]
                .iter()
                .map(|&parent| old[parent])
                .collect();
        }
        (Cluster::new(entries, parents).unwrap(), old)
    }

    #[test]
    #[ignore = "set RBTC_CORE_LINEARIZE to the pinned Core 31 full optimizer adapter"]
    fn full_optimizer_matches_core_optimal_diagrams_and_orders() {
        use std::{fmt::Write as _, process::Command};
        let binary = std::env::var_os("RBTC_CORE_LINEARIZE").expect("native Core oracle path");
        let directory = tempfile::TempDir::new().unwrap();
        let root = std::env::var_os("RBTC_CORE_LINEARIZE_REPORT_DIR")
            .map_or_else(|| directory.path().to_path_buf(), std::path::PathBuf::from);
        std::fs::create_dir_all(&root).unwrap();
        let mut seed = 193;
        let mut fixtures = Vec::new();
        let mut input = String::from("4096\n");
        for case in 0..4096 {
            let count = case % 65;
            let mut cluster = generated(&mut seed, count);
            if case % 11 == 0 {
                cluster.entries.fill(FeeFrac::new(10, 2));
            } else if case % 13 == 0 {
                for entry in &mut cluster.entries {
                    entry.fee = 0;
                }
            }
            let mode = case % 3;
            let (cluster, mut old) = if case % 2 == 0 {
                permute_labels(&cluster, &mut seed)
            } else {
                (cluster, (0..count).collect::<Vec<_>>())
            };
            if mode == 2 {
                old.reverse();
            }
            writeln!(input, "{count} 100000000 {case} {mode}").unwrap();
            for (i, entry) in cluster.entries.iter().enumerate() {
                write!(
                    input,
                    "{} {} {}",
                    entry.fee,
                    entry.size,
                    cluster.parents[i].len()
                )
                .unwrap();
                for parent in &cluster.parents[i] {
                    write!(input, " {parent}").unwrap();
                }
                input.push('\n');
            }
            for i in &old {
                write!(input, "{i} ").unwrap();
            }
            input.push('\n');
            fixtures.push((cluster, old, mode));
        }
        let path = root.join("core-linearize-input.txt");
        std::fs::write(&path, input).unwrap();
        let output = Command::new(binary).arg(path).output().unwrap();
        std::fs::write(root.join("core-linearize-output.txt"), &output.stdout).unwrap();
        std::fs::write(root.join("core-linearize-stderr.txt"), &output.stderr).unwrap();
        assert!(
            output.status.success(),
            "Core adapter failed: {:?}",
            output.status
        );
        let text = String::from_utf8(output.stdout).unwrap();
        let mut rows = text.lines();
        assert_eq!(rows.next(), Some("RBTC_CORE31_LINEARIZE_V1"));
        let mut maximum_work = 0;
        for (case, (cluster, old, mode)) in fixtures.iter().enumerate() {
            let row = rows
                .next()
                .unwrap()
                .split_whitespace()
                .map(|item| item.parse::<u64>().unwrap())
                .collect::<Vec<_>>();
            assert_eq!(row[0], 1, "Core did not converge: case {case}");
            assert_eq!(usize::try_from(row[2]).unwrap(), cluster.len());
            let expected = row[3..]
                .iter()
                .map(|i| usize::try_from(*i).unwrap())
                .collect::<Vec<_>>();
            let result =
                cluster.linearize_with_budget((*mode != 0).then_some(old), DEFAULT_OPTIMIZER_WORK);
            assert!(result.optimal, "Rust did not converge: case {case}");
            maximum_work = maximum_work.max(result.work_used);
            assert_eq!(
                compare_diagrams(
                    &chunk_linearization(&cluster.entries, &result.order),
                    &chunk_linearization(&cluster.entries, &expected),
                ),
                DiagramComparison::Equal,
                "case {case}"
            );
            assert_eq!(
                result.order, expected,
                "Core optimal/minimal tie ordering: case {case}"
            );
        }
        assert!(rows.next().is_none());
        std::fs::write(
            root.join("core-linearize-summary.json"),
            format!("{{\"cases\":4096,\"permuted_labels\":2048,\"maximum_search_work\":{maximum_work}}}\n"),
        )
        .unwrap();
    }
}
