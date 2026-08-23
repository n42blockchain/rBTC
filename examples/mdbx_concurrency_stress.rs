//! Stress the libmdbx crate the way the snapshot-overlay catch-up uses it:
//! one thread commits large write transactions back to back while another
//! keeps opening read transactions, reading keys and polling `info()` /
//! `stat()` — the pattern that ran during every flush once the catch-up loop
//! stopped waiting for the engine.
//!
//! Run with `cargo run --release --features mdbx --example mdbx_concurrency_stress -- <dir> <minutes>`.
//! A clean exit prints the counters; a crash or an engine error is the finding.

use std::{
    env,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

use libmdbx::{
    Database, DatabaseOptions, Mode, NoWriteMap, ReadWriteOptions, SyncMode, TableFlags, WriteFlags,
};

#[allow(clippy::too_many_lines)]
fn main() {
    let mut args = env::args().skip(1);
    let dir = args
        .next()
        .expect("usage: mdbx_concurrency_stress <dir> <minutes>");
    let minutes: u64 = args.next().and_then(|m| m.parse().ok()).unwrap_or(5);
    // Reader mode: "all" (reads + env-level info/stat polls), "reads", "info"
    // (env-level polls only), "info-txn" (polls through a read transaction),
    // or "none".
    let mode = args.next().unwrap_or_else(|| "all".to_owned());
    let do_reads = matches!(mode.as_str(), "all" | "reads");
    let do_info = matches!(mode.as_str(), "all" | "info");
    let do_info_txn = mode == "info-txn";
    let reader_count = if mode == "none" { 0 } else { 2 };
    std::fs::create_dir_all(&dir).unwrap();
    let db = Database::<NoWriteMap>::open_with_options(
        &dir,
        DatabaseOptions {
            max_tables: Some(4),
            mode: Mode::ReadWrite(ReadWriteOptions {
                sync_mode: SyncMode::Durable,
                max_size: Some(8 << 30),
                ..ReadWriteOptions::default()
            }),
            ..DatabaseOptions::default()
        },
    )
    .unwrap();
    {
        let txn = db.begin_rw_txn().unwrap();
        txn.create_table(Some("coins"), TableFlags::default())
            .unwrap();
        txn.commit().unwrap();
    }
    let db = Arc::new(db);
    let stop = Arc::new(AtomicBool::new(false));
    let commits = Arc::new(AtomicU64::new(0));
    let reads = Arc::new(AtomicU64::new(0));
    let infos = Arc::new(AtomicU64::new(0));
    let deadline = Instant::now() + Duration::from_secs(minutes * 60);

    // Writer: each commit inserts ~200k coins and deletes ~150k older ones.
    let writer = {
        let db = Arc::clone(&db);
        let stop = Arc::clone(&stop);
        let commits = Arc::clone(&commits);
        thread::spawn(move || {
            let mut next_key: u64 = 0;
            let mut delete_from: u64 = 0;
            let value = vec![0xabu8; 40];
            while Instant::now() < deadline {
                let txn = db.begin_rw_txn().unwrap();
                let table = txn.open_table(Some("coins")).unwrap();
                for _ in 0..200_000 {
                    let key = mix(next_key).to_be_bytes();
                    txn.put(&table, key, &value, WriteFlags::empty()).unwrap();
                    next_key += 1;
                }
                let mut deleted = 0;
                while delete_from + 50_000 < next_key && deleted < 150_000 {
                    let key = mix(delete_from).to_be_bytes();
                    let _ = txn.del(&table, key, None).unwrap();
                    delete_from += 1;
                    deleted += 1;
                }
                txn.commit().unwrap();
                commits.fetch_add(1, Ordering::Relaxed);
            }
            stop.store(true, Ordering::Relaxed);
            next_key
        })
    };

    // Readers: fresh read transactions, point reads and env info/stat polls.
    let mut readers = Vec::new();
    for worker in 0..reader_count {
        let db = Arc::clone(&db);
        let stop = Arc::clone(&stop);
        let reads = Arc::clone(&reads);
        let infos = Arc::clone(&infos);
        readers.push(thread::spawn(move || {
            let mut probe: u64 = worker;
            while !stop.load(Ordering::Relaxed) {
                if do_reads {
                    let txn = db.begin_ro_txn().unwrap();
                    let table = txn.open_table(Some("coins")).unwrap();
                    for _ in 0..2_000 {
                        let key = mix(probe).to_be_bytes();
                        let _: Option<Vec<u8>> = txn.get(&table, &key).unwrap();
                        probe = probe.wrapping_add(7);
                    }
                    reads.fetch_add(2_000, Ordering::Relaxed);
                }
                if do_info {
                    let _ = db.info().unwrap();
                    let _ = db.stat().unwrap();
                    infos.fetch_add(1, Ordering::Relaxed);
                }
                if do_info_txn {
                    let txn = db.begin_ro_txn().unwrap();
                    let _ = txn.env_info().unwrap();
                    let _ = txn.env_stat().unwrap();
                    infos.fetch_add(1, Ordering::Relaxed);
                }
                if !do_reads && !do_info && !do_info_txn {
                    thread::sleep(Duration::from_millis(50));
                }
            }
        }));
    }
    let keys = writer.join().unwrap();
    for reader in readers {
        reader.join().unwrap();
    }
    println!(
        "ok: mode={mode} commits={} keys_written={} reads={} info_polls={}",
        commits.load(Ordering::Relaxed),
        keys,
        reads.load(Ordering::Relaxed),
        infos.load(Ordering::Relaxed)
    );
}

/// Scatter sequential counters over the key space so pages split like real outpoints.
fn mix(x: u64) -> u64 {
    let mut z = x.wrapping_add(0x9e37_79b9_7f4a_7c15);
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}
