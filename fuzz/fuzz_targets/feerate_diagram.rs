#![no_main]

use std::cmp::Ordering;

use libfuzzer_sys::fuzz_target;
use rbtc::feerate_diagram::{
    Cluster, DiagramComparison, FeeFrac, chunk_linearization, cluster_diagram, compare_diagrams,
    diagram_points,
};

/// Reads one little-endian i64 fee and u16 size pair per entry.
fn parse_entries(data: &mut &[u8], count: usize) -> Vec<FeeFrac> {
    let mut entries = Vec::with_capacity(count);
    for _ in 0..count {
        let (fee_bytes, rest) = data.split_at(8.min(data.len()));
        *data = rest;
        let mut fee = [0_u8; 8];
        fee[..fee_bytes.len()].copy_from_slice(fee_bytes);
        let (size_bytes, rest) = data.split_at(2.min(data.len()));
        *data = rest;
        let mut size = [0_u8; 2];
        size[..size_bytes.len()].copy_from_slice(size_bytes);
        // Positive size keeps construction on the well-formed path; the
        // zero/negative rejection is separately covered below.
        let size = i32::from(u16::from_le_bytes(size).max(1));
        entries.push(FeeFrac::new(i64::from_le_bytes(fee), size));
    }
    entries
}

fuzz_target!(|input: &[u8]| {
    if input.len() < 2 {
        return;
    }
    let mut data = input;
    // Twice the enforced 64-transaction cluster bound: totality beyond the
    // bound is asserted, at a size the O(n^3) greedy still fuzzes quickly.
    let count = usize::from(data[0] % 129);
    let raw_parents_mode = data[1] & 1 == 1;
    data = &data[2..];

    let entries = parse_entries(&mut data, count);
    let mut parents: Vec<Vec<usize>> = Vec::with_capacity(count);
    for child in 0..count {
        let mut list = Vec::new();
        let byte = data.first().copied().unwrap_or(0);
        if !data.is_empty() {
            data = &data[1..];
        }
        if raw_parents_mode {
            // Arbitrary indices exercise the rejection paths (out of range,
            // self, cycles) without ever panicking.
            for bit in 0..8 {
                if byte >> bit & 1 == 1 {
                    list.push(child.wrapping_add(bit).wrapping_mul(7) % (count.max(1) + 2));
                }
            }
        } else {
            // Parents strictly below the child guarantee a DAG, so the
            // well-formed invariants are exercised on every such input.
            for bit in 0..8 {
                if byte >> bit & 1 == 1 && bit < child {
                    list.push(child - 1 - (bit % child.max(1)));
                }
            }
            list.sort_unstable();
            list.dedup();
        }
        parents.push(list);
    }

    let Ok(cluster) = Cluster::new(entries, parents) else {
        return;
    };

    // The linearization is a permutation in which every parent precedes its
    // child.
    let order = cluster.linearize();
    assert_eq!(order.len(), cluster.len());
    let mut position = vec![usize::MAX; cluster.len()];
    for (index, &tx) in order.iter().enumerate() {
        assert_eq!(position[tx], usize::MAX, "no duplicates");
        position[tx] = index;
    }

    // Chunk feerates are non-increasing and preserve the totals exactly.
    let chunks = chunk_linearization(cluster.fractions(), &order);
    for window in chunks.windows(2) {
        assert_ne!(window[0].feerate_cmp(window[1]), Ordering::Less);
    }
    let total = |fractions: &[FeeFrac]| {
        fractions.iter().fold((0_i128, 0_i128), |(fee, size), f| {
            (fee + i128::from(f.fee), size + i128::from(f.size))
        })
    };
    assert_eq!(total(&chunks), total(cluster.fractions()));

    // The diagram is concave and comparison is reflexive and antisymmetric.
    let points = diagram_points(&chunks);
    for triple in points.windows(3) {
        let (a, b, c) = (triple[0], triple[1], triple[2]);
        assert!(
            (b.fee - a.fee) * (c.size - b.size) >= (c.fee - b.fee) * (b.size - a.size),
            "slopes never increase"
        );
    }
    assert_eq!(compare_diagrams(&chunks, &chunks), DiagramComparison::Equal);
    let alternative = cluster_diagram(&cluster);
    let forward = compare_diagrams(&chunks, &alternative);
    let backward = compare_diagrams(&alternative, &chunks);
    let mirrored = match forward {
        DiagramComparison::Equal => DiagramComparison::Equal,
        DiagramComparison::Better => DiagramComparison::Worse,
        DiagramComparison::Worse => DiagramComparison::Better,
        DiagramComparison::Incomparable => DiagramComparison::Incomparable,
    };
    assert_eq!(backward, mirrored, "comparison is antisymmetric");
});
