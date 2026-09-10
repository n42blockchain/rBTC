//! Measures retained valid sibling headers, serving projections and durable replay.
//!
//! Usage: cargo run --release --example header_resource_probe -- <active|full> <active-headers> <siblings>
//!
//! `full` reproduces the former serving-DAG clone; `active` uses the production
//! active-chain projection. JSON lines report the process RSS (Linux only),
//! logical retained entries and database file length separately. Reopen runs
//! in the same process, so it is not a cold-start RSS measurement. This is a
//! kernel/store workload, not an end-to-end peer or resource-cap acceptance.

use std::{
    env, fs,
    path::Path,
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use bitcoin::{
    Network, TxMerkleNode,
    block::{Header, Version},
    hashes::Hash,
    pow::Target,
};
use rbtc::{header_store::RedbHeaderStore, headers::HeaderDag};

// Match the daemon's allocator selection when comparing process memory.
#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn mine(parent: bitcoin::BlockHash, time: u32, tag: u32) -> Header {
    let mut merkle = [0; 32];
    merkle[..4].copy_from_slice(&tag.to_le_bytes());
    let target = Target::MAX_ATTAINABLE_REGTEST;
    let mut header = Header {
        version: Version::from_consensus(4),
        prev_blockhash: parent,
        merkle_root: TxMerkleNode::from_byte_array(merkle),
        time,
        bits: target.to_compact_lossy(),
        nonce: 0,
    };
    while header.validate_pow(target).is_err() {
        header.nonce = header.nonce.checked_add(1).expect("regtest nonce search");
    }
    header
}

fn rss_kib() -> Option<u64> {
    fs::read_to_string("/proc/self/status")
        .ok()?
        .lines()
        .find_map(|line| {
            line.strip_prefix("VmRSS:")?
                .split_whitespace()
                .next()?
                .parse()
                .ok()
        })
}

fn persist(
    dag: &mut HeaderDag,
    store: &RedbHeaderStore,
    batch: &[Header],
    now: u32,
) -> Result<(), Box<dyn std::error::Error>> {
    let staged = dag.stage_batch_contextual(batch, now)?;
    store.append_batch(batch)?;
    let _ = staged.commit();
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn report(
    mode: &str,
    phase: &str,
    dag: &HeaderDag,
    snapshot: &HeaderDag,
    path: &Path,
    started: Instant,
    update_micros: u128,
) -> Result<(), Box<dyn std::error::Error>> {
    let active_entries = u64::from(dag.active_tip().height) + 1;
    println!(
        "{}",
        serde_json::json!({
            "mode": mode,
            "allocator": if cfg!(feature = "mimalloc") { "mimalloc" } else { "system" },
            "phase": phase,
            "active_entries": active_entries,
            "retained_entries": dag.retained_header_count(),
            "side_entries": u64::try_from(dag.retained_header_count())? - active_entries,
            "projection_entries": snapshot.retained_header_count(),
            "database_bytes": fs::metadata(path)?.len(),
            "rss_kib": rss_kib(),
            "elapsed_micros": started.elapsed().as_micros(),
            "projection_update_micros": update_micros,
        })
    );
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = env::args().skip(1);
    let mode = args.next().ok_or("expected active or full")?;
    if mode != "active" && mode != "full" {
        return Err("expected active or full".into());
    }
    let active: u32 = args.next().ok_or("expected active-header count")?.parse()?;
    let siblings: u32 = args.next().ok_or("expected sibling count")?.parse()?;
    if !(2..=1_000_000).contains(&active) || siblings > 1_000_000 || args.next().is_some() {
        return Err("active count must be 2..1000000 and siblings 0..1000000".into());
    }
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("headers.redb");
    let now = u32::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs())?;
    let mut dag = HeaderDag::new(Network::Regtest);
    let genesis = dag.active_tip();
    let store = RedbHeaderStore::open(&path)?;
    let mut batch = Vec::with_capacity(2_000);
    let mut parent = genesis.header;
    for tag in 1..=active {
        parent = mine(parent.block_hash(), parent.time + 1, tag);
        batch.push(parent);
        if batch.len() == 2_000 || tag == active {
            persist(&mut dag, &store, &batch, now)?;
            batch.clear();
        }
    }
    let expected_tip = dag.active_tip();
    let mut snapshot = dag.active_chain_snapshot();
    let started = Instant::now();
    report(&mode, "active-chain", &dag, &snapshot, &path, started, 0)?;
    for tag in 1..=siblings {
        batch.push(mine(genesis.hash, genesis.header.time + 1, active + tag));
        if batch.len() == 2_000 || tag == siblings {
            persist(&mut dag, &store, &batch, now)?;
            batch.clear();
            let update = Instant::now();
            if mode == "full" {
                snapshot = dag.clone();
            } else {
                dag.refresh_active_chain_snapshot(&mut snapshot);
                assert_eq!(
                    snapshot.retained_header_count(),
                    usize::try_from(active)? + 1
                );
            }
            let update_micros = update.elapsed().as_micros();
            assert_eq!(snapshot.active_tip(), expected_tip);
            assert_eq!(store.len()?, u64::from(active) + u64::from(tag));
            std::hint::black_box(&snapshot);
            report(
                &mode,
                "siblings",
                &dag,
                &snapshot,
                &path,
                started,
                update_micros,
            )?;
        }
    }
    drop(dag);
    drop(store);
    let store = RedbHeaderStore::open(&path)?;
    let reloaded = store.load_dag(Network::Regtest, now)?;
    assert_eq!(reloaded.active_tip(), expected_tip);
    assert_eq!(
        u64::try_from(reloaded.retained_header_count())?,
        store.len()? + 1
    );
    report(
        &mode,
        "reopen-same-process",
        &reloaded,
        &snapshot,
        &path,
        started,
        0,
    )?;
    Ok(())
}
