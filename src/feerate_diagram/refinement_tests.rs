use super::*;

fn next(state: &mut u64) -> u64 {
    *state = state
        .wrapping_mul(6_364_136_223_846_793_005)
        .wrapping_add(1);
    *state ^ (*state >> 29)
}

fn bounded(state: &mut u64, bound: usize) -> usize {
    usize::try_from(next(state) % u64::try_from(bound).unwrap()).unwrap()
}

// The returned topological order uses permuted labels, including parents whose
// labels exceed their children. Fee modes include zeros, ties and wide totals.
fn generated(seed: u64, count: usize, shape: usize) -> (Cluster, Vec<usize>) {
    let mut state = seed;
    let mut labels = (0..count).collect::<Vec<_>>();
    for end in (1..count).rev() {
        labels.swap(end, bounded(&mut state, end + 1));
    }
    let mut entries = vec![FeeFrac::ZERO; count];
    let mut parents = vec![Vec::new(); count];
    for rank in 0..count {
        let size = i32::try_from(1 + bounded(&mut state, 10_000)).unwrap();
        let fee = match seed % 4 {
            0 => i64::try_from(bounded(&mut state, 100_000)).unwrap(),
            1 => i64::from(size) * 7,
            2 => i64::MAX / i64::try_from(count).unwrap() - i64::from(size),
            _ => {
                if rank % 2 == 0 {
                    0
                } else {
                    10_000
                }
            }
        };
        let size = if seed % 4 == 2 {
            i32::MAX / i32::try_from(count).unwrap() - size
        } else {
            size
        };
        entries[labels[rank]] = FeeFrac::new(fee, size);
        match shape {
            1 if rank > 0 => {
                parents[labels[rank]].push(labels[bounded(&mut state, rank)]);
            }
            2 if rank + 1 < count => {
                let child = rank + 1 + bounded(&mut state, count - rank - 1);
                parents[labels[child]].push(labels[rank]);
            }
            3 => parents[labels[rank]].extend(labels[..rank].iter().copied()),
            4 if rank > 0 => parents[labels[rank]].push(labels[0]),
            0 => {
                for &parent in &labels[..rank] {
                    if bounded(&mut state, 4) == 0 {
                        parents[labels[rank]].push(parent);
                    }
                }
            }
            _ => {}
        }
    }
    (Cluster::new(entries, parents).unwrap(), labels)
}

fn check_order_and_chunks(cluster: &Cluster, order: &[usize]) {
    let mut positions = vec![usize::MAX; cluster.len()];
    assert_eq!(order.len(), cluster.len());
    for (position, &index) in order.iter().enumerate() {
        assert_eq!(positions[index], usize::MAX);
        positions[index] = position;
    }
    for (child, parents) in cluster.parents.iter().enumerate() {
        for &parent in parents {
            assert!(positions[parent] < positions[child]);
        }
    }
    for chunk in chunk_linearization_with_members(cluster.fractions(), order) {
        let mut reached = vec![false; cluster.len()];
        reached[chunk.members[0]] = true;
        loop {
            let mut changed = false;
            for &child in &chunk.members {
                for &parent in &cluster.parents[child] {
                    if chunk.members.contains(&parent) && reached[child] != reached[parent] {
                        reached[child] = true;
                        reached[parent] = true;
                        changed = true;
                    }
                }
            }
            if !changed {
                break;
            }
        }
        assert!(
            chunk.members.iter().all(|&index| reached[index]),
            "chunk must be connected"
        );
    }
}

#[test]
fn refinement_is_topological_connected_and_never_worsens_generated_diagrams() {
    for seed in 0..384 {
        let count = usize::try_from(seed % 65).unwrap();
        let (cluster, arbitrary) = generated(seed, count, usize::try_from(seed % 6).unwrap());
        for mut order in [arbitrary, cluster.linearize_ancestors()] {
            let before = chunk_linearization(cluster.fractions(), &order);
            cluster.post_linearize(&mut order);
            check_order_and_chunks(&cluster, &order);
            let after = chunk_linearization(cluster.fractions(), &order);
            assert!(matches!(
                compare_diagrams(&after, &before),
                DiagramComparison::Equal | DiagramComparison::Better
            ));
        }
    }
}

fn all_topological_orders(
    cluster: &Cluster,
    order: &mut Vec<usize>,
    visit: &mut impl FnMut(&[usize]),
) {
    if order.len() == cluster.len() {
        visit(order);
        return;
    }
    for index in 0..cluster.len() {
        if !order.contains(&index)
            && cluster.parents[index]
                .iter()
                .all(|parent| order.contains(parent))
        {
            order.push(index);
            all_topological_orders(cluster, order, visit);
            order.pop();
        }
    }
}

#[test]
fn refinement_is_optimal_on_small_trees_against_every_topological_order() {
    for seed in 0..64 {
        for shape in [1, 2] {
            let (cluster, _) = generated(seed, 6, shape);
            let refined = cluster_diagram(&cluster);
            all_topological_orders(&cluster, &mut Vec::new(), &mut |order| {
                let alternative = chunk_linearization(cluster.fractions(), order);
                assert!(matches!(
                    compare_diagrams(&refined, &alternative),
                    DiagramComparison::Equal | DiagramComparison::Better
                ));
            });
        }
    }
}

#[test]
fn refinement_recovers_a_moved_leaf_after_a_fee_increase() {
    for seed in 0..128 {
        let (mut cluster, _) = generated(seed, 16, 0);
        let mut order = cluster.linearize();
        let before = chunk_linearization(cluster.fractions(), &order);
        let leaf = (0..cluster.len())
            .find(|index| {
                !cluster
                    .parents
                    .iter()
                    .any(|parents| parents.contains(index))
            })
            .unwrap();
        order.retain(|&index| index != leaf);
        order.push(leaf);
        let total = cluster
            .entries
            .iter()
            .map(|entry| i128::from(entry.fee))
            .sum::<i128>();
        cluster.entries[leaf].fee +=
            i64::try_from((i128::from(i64::MAX) - total).min(1_000)).unwrap();
        cluster.post_linearize(&mut order);
        check_order_and_chunks(&cluster, &order);
        let after = chunk_linearization(cluster.fractions(), &order);
        assert!(matches!(
            compare_diagrams(&after, &before),
            DiagramComparison::Equal | DiagramComparison::Better
        ));
    }
}

#[test]
fn inputs_beyond_the_production_bitset_bound_retain_the_total_baseline() {
    for count in [65, 128] {
        let (cluster, _) = generated(37, count, 1);
        assert_eq!(cluster.linearize(), cluster.linearize_ancestors());
    }
}

#[test]
#[ignore = "build contrib/core31_postlinearize.cpp against Core 31.0 and set RBTC_CORE_POSTLINEARIZE"]
fn core_31_postlinearization_matches_generated_orders_exactly() {
    use std::{
        fmt::Write,
        fs,
        process::Command,
        time::{SystemTime, UNIX_EPOCH},
    };
    let binary =
        std::env::var_os("RBTC_CORE_POSTLINEARIZE").expect("set the pinned Core oracle executable");
    let report_root = std::env::var_os("RBTC_CORE_POSTLINEARIZE_REPORT_DIR");
    let retain = report_root.is_some();
    let root = report_root.map_or_else(std::env::temp_dir, std::path::PathBuf::from);
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let directory = root.join(format!("core31-post-{}-{stamp}", std::process::id()));
    fs::create_dir(&directory).unwrap();
    let mut input = String::from("4096\n");
    let mut expected = Vec::new();
    for seed in 0..2_048 {
        let count = usize::try_from(seed % 65).unwrap();
        let (cluster, arbitrary) = generated(seed, count, usize::try_from(seed % 6).unwrap());
        for mut order in [arbitrary, cluster.linearize_ancestors()] {
            writeln!(input, "{count}").unwrap();
            for (entry, parents) in cluster.entries.iter().zip(&cluster.parents) {
                write!(input, "{} {} {}", entry.fee, entry.size, parents.len()).unwrap();
                for parent in parents {
                    write!(input, " {parent}").unwrap();
                }
                input.push('\n');
            }
            for index in &order {
                write!(input, "{index} ").unwrap();
            }
            input.push('\n');
            cluster.post_linearize(&mut order);
            expected.push(order);
        }
    }
    let input_path = directory.join("inputs.txt");
    fs::write(&input_path, &input).unwrap();
    let output = Command::new(binary).arg(&input_path).output().unwrap();
    fs::write(directory.join("core-output.txt"), &output.stdout).unwrap();
    fs::write(directory.join("core-stderr.txt"), &output.stderr).unwrap();
    let mut expected_text = String::new();
    for order in &expected {
        writeln!(expected_text, "{order:?}").unwrap();
    }
    fs::write(directory.join("rbtc-output.txt"), expected_text).unwrap();
    assert!(
        output.status.success(),
        "Core oracle failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    let mut lines = stdout.lines();
    assert_eq!(lines.next(), Some("RBTC_CORE31_POSTLINEARIZE_V1"));
    for (case, order) in expected.iter().enumerate() {
        let values = lines
            .next()
            .expect("one result per case")
            .split_whitespace()
            .map(|value| value.parse::<usize>().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(values.first(), Some(&order.len()));
        assert_eq!(
            &values[1..],
            order,
            "Core order differs at case {case}; evidence {}",
            directory.display()
        );
    }
    assert!(lines.next().is_none());
    println!(
        "4096 exact Core PostLinearize comparisons passed; evidence {}",
        directory.display()
    );
    if !retain {
        fs::remove_dir_all(directory).unwrap();
    }
}
