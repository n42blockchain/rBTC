//! Measures retained valid sibling headers, serving projections and durable replay.
//!
//! Usage: cargo run --release --example header_resource_probe -- <active|full|retained> <active-headers> <siblings>
//!
//! `full` reproduces the former serving-DAG clone; `active` uses the production
//! active-chain projection. JSON lines report the process RSS (Linux/macOS),
//! logical retained entries, allocated disk bytes and database file length separately.
//! Both same-process and fresh-process reopen are verified. Fresh-process reopen
//! does not imply cold OS/device caches. This is a
//! kernel/store workload, not an end-to-end peer or resource-cap acceptance.
//! `retained` additionally runs idle eviction after each batch with a 50,000
//! side-header target and reports both the pre- and post-maintenance samples.
//! Set `RBTC_HEADER_PROBE_SECONDS` to repeat uniquely tagged sibling churn for
//! at least that many seconds. Missing RSS measurements fail the probe.

use std::{
    env, fs,
    path::Path,
    process::Command,
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use bitcoin::{
    Network, TxMerkleNode,
    block::{Header, Version},
    hashes::Hash,
    pow::Target,
};
use rbtc::{
    header_store::RedbHeaderStore,
    headers::{HeaderDag, HeaderInfo},
};

// Match the daemon's allocator selection when comparing process memory.
#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn mine(parent: bitcoin::BlockHash, time: u32, tag: u64) -> Header {
    let mut merkle = [0; 32];
    merkle[..8].copy_from_slice(&tag.to_le_bytes());
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

fn rss_kib() -> Result<u64, Box<dyn std::error::Error>> {
    if cfg!(target_os = "macos") {
        let output = Command::new("ps")
            .args(["-o", "rss=", "-p", &std::process::id().to_string()])
            .output()?;
        if !output.status.success() {
            return Err("ps failed to measure RSS".into());
        }
        return Ok(std::str::from_utf8(&output.stdout)?.trim().parse()?);
    }
    Ok(fs::read_to_string("/proc/self/status")?
        .lines()
        .find_map(|line| line.strip_prefix("VmRSS:"))
        .ok_or("missing VmRSS")?
        .split_whitespace()
        .next()
        .ok_or("empty VmRSS")?
        .parse()?)
}

fn allocated_bytes(path: &Path) -> Result<Option<u64>, std::io::Error> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        Ok(Some(fs::metadata(path)?.blocks() * 512))
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        Ok(None)
    }
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
            "rss_kib": rss_kib()?,
            "database_allocated_bytes": allocated_bytes(path)?,
            "pid": std::process::id(),
            "tip": dag.active_tip().hash.to_string(),
            "elapsed_micros": started.elapsed().as_micros(),
            "projection_update_micros": update_micros,
        })
    );
    Ok(())
}

fn retain_side_headers(
    dag: &mut HeaderDag,
    store: &RedbHeaderStore,
) -> Result<(), Box<dyn std::error::Error>> {
    let expected_tip = dag.active_tip();
    let hashes = dag.side_chain_eviction_plan(50_000, 2_000);
    let stage = dag.stage_leaf_evictions(&hashes, &[expected_tip.hash], 2_000)?;
    store.persist_eviction(&stage)?;
    stage.commit();
    assert!(dag.retained_header_count() <= usize::try_from(expected_tip.height)? + 1 + 50_000);
    Ok(())
}

fn reopen(mut args: impl Iterator<Item = String>) -> Result<(), Box<dyn std::error::Error>> {
    let path = args.next().ok_or("expected database path")?;
    let now = args.next().ok_or("expected adjusted time")?.parse()?;
    let expected_tip = args.next().ok_or("expected tip")?;
    let expected_rows: u64 = args.next().ok_or("expected retained count")?.parse()?;
    if args.next().is_some() {
        return Err("extra reopen arguments".into());
    }
    let started = Instant::now();
    let store = RedbHeaderStore::open(&path)?;
    let initial = HeaderDag::new(Network::Regtest);
    report(
        "reopen",
        "reopen-before-load",
        &initial,
        &initial,
        Path::new(&path),
        started,
        0,
    )?;
    drop(initial);
    let dag = store.load_dag(Network::Regtest, now)?;
    assert_eq!(dag.active_tip().hash.to_string(), expected_tip);
    assert_eq!(store.len()?, expected_rows);
    assert_eq!(dag.retained_header_count() as u64, expected_rows + 1);
    report(
        "reopen",
        "reopen-fresh-process",
        &dag,
        &dag,
        Path::new(&path),
        started,
        0,
    )?;
    Ok(())
}

fn seed_active(
    dag: &mut HeaderDag,
    store: &RedbHeaderStore,
    active: u32,
    now: u32,
) -> Result<(), Box<dyn std::error::Error>> {
    let genesis = dag.active_tip();
    let mut batch = Vec::with_capacity(2_000);
    let mut parent = genesis.header;
    for tag in 1..=active {
        parent = mine(parent.block_hash(), parent.time + 1, u64::from(tag));
        batch.push(parent);
        if batch.len() == 2_000 || tag == active {
            persist(dag, store, &batch, now)?;
            batch.clear();
        }
    }
    Ok(())
}

fn probe_duration(mode: &str, siblings: u32) -> Result<u64, Box<dyn std::error::Error>> {
    let minimum_seconds: u64 =
        env::var("RBTC_HEADER_PROBE_SECONDS").map_or(Ok(0), |value| value.parse())?;
    if minimum_seconds > 0 && (mode != "retained" || siblings == 0) {
        return Err("sustained mode requires retained and nonzero siblings".into());
    }
    Ok(minimum_seconds)
}

fn report_churn(generated: u64, minimum_seconds: u64, started: Instant) {
    println!(
        "{}",
        serde_json::json!({
            "phase": "completed-churn", "generated_siblings": generated,
            "minimum_seconds": minimum_seconds, "elapsed_micros": started.elapsed().as_micros()
        })
    );
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = env::args().skip(1);
    let mode = args.next().ok_or("expected active, full or retained")?;
    if mode == "reopen" {
        return reopen(args);
    }
    if mode != "active" && mode != "full" && mode != "retained" {
        return Err("expected active, full or retained".into());
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
    seed_active(&mut dag, &store, active, now)?;
    let expected_tip = dag.active_tip();
    let mut snapshot = dag.active_chain_snapshot();
    let started = Instant::now();
    report(&mode, "active-chain", &dag, &snapshot, &path, started, 0)?;
    // Sustained runs are opt-in and only meaningful with retention enabled.
    let minimum_seconds = probe_duration(&mode, siblings)?;
    let mut generated = 0_u64;
    loop {
        for tag in 1..=siblings {
            generated = generated.checked_add(1).ok_or("probe tag overflow")?;
            batch.push(mine(
                genesis.hash,
                genesis.header.time + 1,
                u64::from(active)
                    .checked_add(generated)
                    .ok_or("probe tag overflow")?,
            ));
            if batch.len() == 2_000 || tag == siblings {
                persist(&mut dag, &store, &batch, now)?;
                batch.clear();
                if mode == "retained" {
                    report(
                        &mode,
                        "before-retention",
                        &dag,
                        &snapshot,
                        &path,
                        started,
                        0,
                    )?;
                    retain_side_headers(&mut dag, &store)?;
                }
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
                let retained_siblings = if mode == "retained" {
                    generated.min(50_000)
                } else {
                    generated
                };
                assert_eq!(store.len()?, u64::from(active) + retained_siblings);
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
        if started.elapsed().as_secs() >= minimum_seconds {
            break;
        }
    }
    report_churn(generated, minimum_seconds, started);
    let expected_rows = store.len()?;
    drop(dag);
    drop(store);
    verify_reopens(
        &mode,
        &path,
        snapshot,
        now,
        expected_tip,
        expected_rows,
        started,
    )
}

fn verify_reopens(
    mode: &str,
    path: &Path,
    snapshot: HeaderDag,
    now: u32,
    expected_tip: HeaderInfo,
    expected_rows: u64,
    started: Instant,
) -> Result<(), Box<dyn std::error::Error>> {
    let store = RedbHeaderStore::open(path)?;
    let reloaded = store.load_dag(Network::Regtest, now)?;
    assert_eq!(reloaded.active_tip(), expected_tip);
    assert_eq!(
        u64::try_from(reloaded.retained_header_count())?,
        store.len()? + 1
    );
    report(
        mode,
        "reopen-same-process",
        &reloaded,
        &snapshot,
        path,
        started,
        0,
    )?;
    drop(reloaded);
    drop(snapshot);
    drop(store);
    let status = Command::new(env::current_exe()?)
        .arg("reopen")
        .arg(path)
        .arg(now.to_string())
        .arg(expected_tip.hash.to_string())
        .arg(expected_rows.to_string())
        .status()?;
    if !status.success() {
        return Err("fresh-process reopen failed".into());
    }
    Ok(())
}
