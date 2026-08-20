#![cfg(feature = "mdbx")]

//! Resumable, explicit mainnet-scale churn gate for the candidate MDBX store.
//!
//! This is storage-only and deliberately ignored. It does not label synthetic
//! transitions as Bitcoin blocks or replace the real-block differential gate.

use std::{
    env,
    fmt::Write as _,
    fs,
    path::{Path, PathBuf},
    process::Command,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use bitcoin::{BlockHash, OutPoint, Txid, hashes::Hash};
use rbtc::{
    OutPointKey, Utxo,
    chain_store::{ConnectTransition, ExecutionChainStore},
    execution_store::ExecutionTip,
    mdbx_utxo::{
        DEFAULT_CHAINSTATE_CAPACITY_BYTES, DEFAULT_COMPACTION_TRIGGER_PERCENT,
        DEFAULT_RECOMPACT_GROWTH_PERCENT, MdbxChainstateAudit, MdbxChainstateMetrics,
        MdbxCompactionReport, MdbxUtxoStore,
    },
    utxo::{UtxoStore, UtxoUndo},
};
use serde::Serialize;

const DEFAULT_LIVE_UTXOS: u64 = 160_000_000;
const DEFAULT_BLOCKS: u32 = 900_000;
const DEFAULT_UPDATES_PER_BLOCK: u32 = 5_000;
const DEFAULT_SEED_BATCH: u64 = 100_000;
const DEFAULT_COMMIT_BATCH: u32 = 256;
const DEFAULT_UNDO_RETENTION: u32 = 288;
const DEFAULT_REPORT_INTERVAL: u32 = 10_000;

#[derive(Serialize)]
struct WorkloadReport {
    live_utxos: u64,
    target_blocks: u32,
    updates_per_block: u32,
    seed_batch: u64,
    commit_batch: u32,
    undo_retention: u32,
    capacity_bytes: u64,
    compact_enabled: bool,
    compact_trigger_percent: u8,
    recompact_growth_percent: u8,
}

#[derive(Serialize)]
struct MetricsReport {
    height: u32,
    elapsed_seconds: f64,
    blocks_per_second: f64,
    high_water_bytes: u64,
    live_page_bytes: u64,
    free_page_bytes: u64,
    file_bytes: u64,
    allocated_bytes: u64,
    capacity_bytes: u64,
    hot_entries: u64,
    cold_entries: u64,
    undo_entries: u64,
    meta_entries: u64,
}

#[derive(Serialize)]
struct CompactionReport {
    height: u32,
    elapsed_seconds: f64,
    before_bytes: u64,
    after_bytes: u64,
    before_live_page_bytes: u64,
    after_live_page_bytes: u64,
    before_free_page_bytes: u64,
    after_free_page_bytes: u64,
    before_allocated_bytes: u64,
    after_allocated_bytes: u64,
    record_bytes: u64,
    content_sha256: String,
}

#[derive(Serialize)]
struct AuditReport {
    high_water_bytes: u64,
    live_page_bytes: u64,
    free_page_bytes: u64,
    record_bytes: u64,
    file_bytes: u64,
    allocated_bytes: u64,
    capacity_bytes: u64,
    hot_entries: u64,
    cold_entries: u64,
    undo_entries: u64,
    meta_entries: u64,
    tip_height: Option<u32>,
    tip_hash: Option<String>,
    content_sha256: String,
}

#[derive(Serialize)]
struct GateReport {
    schema: u32,
    boundary: &'static str,
    revision: String,
    started_epoch: u64,
    finished_epoch: Option<u64>,
    start_height: u32,
    workload: WorkloadReport,
    checkpoints: Vec<MetricsReport>,
    compactions: Vec<CompactionReport>,
    final_audit: Option<AuditReport>,
}

fn env_u64(name: &str, default: u64) -> u64 {
    env::var(name).ok().map_or(default, |value| {
        value
            .parse::<u64>()
            .unwrap_or_else(|_| panic!("invalid {name}"))
    })
}

fn env_u32(name: &str, default: u32) -> u32 {
    u32::try_from(env_u64(name, u64::from(default)))
        .unwrap_or_else(|_| panic!("{name} exceeds u32"))
}

fn env_u8(name: &str, default: u8) -> u8 {
    u8::try_from(env_u64(name, u64::from(default))).unwrap_or_else(|_| panic!("{name} exceeds u8"))
}

fn enabled(name: &str, default: bool) -> bool {
    env::var_os(name).map_or(default, |value| !value.is_empty() && value != "0")
}

fn epoch() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock after epoch")
        .as_secs()
}

fn revision() -> String {
    let output = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .expect("read revision");
    let mut revision = String::from_utf8_lossy(&output.stdout).trim().to_owned();
    let dirty = Command::new("git")
        .args(["status", "--porcelain", "--untracked-files=no"])
        .output()
        .expect("read worktree state");
    if !dirty.stdout.is_empty() {
        revision.push_str("-dirty");
    }
    revision
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().fold(
        String::with_capacity(bytes.len().saturating_mul(2)),
        |mut encoded, byte| {
            write!(encoded, "{byte:02x}").expect("write to string");
            encoded
        },
    )
}

fn outpoint(slot: u64, generation: u64) -> OutPointKey {
    let mut txid = [0_u8; 32];
    txid[..8].copy_from_slice(&slot.to_be_bytes());
    txid[8..16].copy_from_slice(&generation.to_be_bytes());
    txid[16..24].copy_from_slice(&slot.rotate_left(17).to_be_bytes());
    txid[24..].copy_from_slice(&generation.rotate_left(29).to_be_bytes());
    OutPoint::new(Txid::from_byte_array(txid), 0).into()
}

fn block_hash(height: u32) -> BlockHash {
    let mut bytes = [0_u8; 32];
    for chunk in bytes.chunks_exact_mut(4) {
        chunk.copy_from_slice(&height.to_be_bytes());
    }
    BlockHash::from_byte_array(bytes)
}

fn coin(height: u32) -> Utxo {
    let mut script = vec![0x76, 0xa9, 0x14];
    script.extend_from_slice(&[0x42; 20]);
    script.extend_from_slice(&[0x88, 0xac]);
    Utxo {
        value_sats: 50_000,
        height,
        is_coinbase: false,
        last_touched: 0,
        creation_mtp: height.saturating_mul(600),
        script_pubkey: script,
    }
}

fn prior_updates(total_updates: u64, slot: u64, live_utxos: u64) -> u64 {
    if total_updates <= slot {
        0
    } else {
        (total_updates - 1 - slot) / live_utxos + 1
    }
}

fn previous_height(update_count: u64, slot: u64, live_utxos: u64, updates_per_block: u32) -> u32 {
    if update_count == 0 {
        return 0;
    }
    let update_index = slot.saturating_add((update_count - 1).saturating_mul(live_utxos));
    u32::try_from(update_index / u64::from(updates_per_block) + 1)
        .expect("previous update height fits u32")
}

fn transition(height: u32, live_utxos: u64, updates_per_block: u32) -> ConnectTransition {
    let first = u64::from(height - 1).saturating_mul(u64::from(updates_per_block));
    let mut spent = Vec::with_capacity(updates_per_block as usize);
    let mut created = Vec::with_capacity(updates_per_block as usize);
    let mut undo_spent = Vec::with_capacity(updates_per_block as usize);
    let mut undo_created = Vec::with_capacity(updates_per_block as usize);
    for offset in 0..u64::from(updates_per_block) {
        let update_index = first + offset;
        let slot = update_index % live_utxos;
        let generation = prior_updates(update_index, slot, live_utxos);
        let old = outpoint(slot, generation);
        let new = outpoint(slot, generation + 1);
        spent.push(old);
        created.push((new, coin(height)));
        undo_spent.push((
            old,
            coin(previous_height(
                generation,
                slot,
                live_utxos,
                updates_per_block,
            )),
        ));
        undo_created.push(new);
    }
    ConnectTransition {
        expected_parent: block_hash(height - 1),
        next: ExecutionTip {
            height,
            hash: block_hash(height),
        },
        spent,
        created,
        transaction_undos: vec![UtxoUndo::from_parts(undo_spent, undo_created)],
    }
}

fn metrics(
    height: u32,
    start_height: u32,
    elapsed: Duration,
    value: MdbxChainstateMetrics,
) -> MetricsReport {
    let seconds = elapsed.as_secs_f64();
    MetricsReport {
        height,
        elapsed_seconds: seconds,
        blocks_per_second: f64::from(height.saturating_sub(start_height))
            / seconds.max(f64::EPSILON),
        high_water_bytes: value.high_water_bytes,
        live_page_bytes: value.live_page_bytes,
        free_page_bytes: value.free_page_bytes,
        file_bytes: value.file_bytes,
        allocated_bytes: value.allocated_bytes,
        capacity_bytes: value.capacity_bytes,
        hot_entries: value.hot_entries,
        cold_entries: value.cold_entries,
        undo_entries: value.undo_entries,
        meta_entries: value.meta_entries,
    }
}

fn compaction(height: u32, elapsed: Duration, value: MdbxCompactionReport) -> CompactionReport {
    CompactionReport {
        height,
        elapsed_seconds: elapsed.as_secs_f64(),
        before_bytes: value.before_bytes,
        after_bytes: value.after_bytes,
        before_live_page_bytes: value.before_live_page_bytes,
        after_live_page_bytes: value.after_live_page_bytes,
        before_free_page_bytes: value.before_free_page_bytes,
        after_free_page_bytes: value.after_free_page_bytes,
        before_allocated_bytes: value.before_allocated_bytes,
        after_allocated_bytes: value.after_allocated_bytes,
        record_bytes: value.record_bytes,
        content_sha256: hex(&value.content_sha256),
    }
}

fn audit(value: MdbxChainstateAudit) -> AuditReport {
    AuditReport {
        high_water_bytes: value.high_water_bytes,
        live_page_bytes: value.live_page_bytes,
        free_page_bytes: value.free_page_bytes,
        record_bytes: value.record_bytes,
        file_bytes: value.file_bytes,
        allocated_bytes: value.allocated_bytes,
        capacity_bytes: value.capacity_bytes,
        hot_entries: value.hot_entries,
        cold_entries: value.cold_entries,
        undo_entries: value.undo_entries,
        meta_entries: value.meta_entries,
        tip_height: value.tip.map(|tip| tip.height),
        tip_hash: value.tip.map(|tip| tip.hash.to_string()),
        content_sha256: hex(&value.content_sha256),
    }
}

fn write_report(path: &Path, report: &GateReport) {
    let temporary = path.with_extension("json.tmp");
    fs::write(
        &temporary,
        serde_json::to_vec_pretty(report).expect("encode gate report"),
    )
    .expect("write temporary gate report");
    fs::rename(temporary, path).expect("publish gate report");
}

#[test]
#[ignore = "requires an explicit isolated directory and enough time/disk for the selected scale"]
#[allow(clippy::too_many_lines)]
fn mdbx_mainnet_scale_churn_and_compaction_gate() {
    let database_dir = PathBuf::from(
        env::var_os("RBTC_MDBX_GATE_DIR")
            .expect("RBTC_MDBX_GATE_DIR must name an isolated persistent directory"),
    );
    assert!(!database_dir.as_os_str().is_empty());
    let live_utxos = env_u64("RBTC_MDBX_GATE_UTXOS", DEFAULT_LIVE_UTXOS);
    let target_blocks = env_u32("RBTC_MDBX_GATE_BLOCKS", DEFAULT_BLOCKS);
    let updates_per_block = env_u32("RBTC_MDBX_GATE_UPDATES", DEFAULT_UPDATES_PER_BLOCK);
    let seed_batch = env_u64("RBTC_MDBX_GATE_SEED_BATCH", DEFAULT_SEED_BATCH);
    let commit_batch = env_u32("RBTC_MDBX_GATE_COMMIT_BATCH", DEFAULT_COMMIT_BATCH);
    let undo_retention = env_u32("RBTC_MDBX_GATE_UNDO_RETENTION", DEFAULT_UNDO_RETENTION);
    let report_interval = env_u32("RBTC_MDBX_GATE_REPORT_INTERVAL", DEFAULT_REPORT_INTERVAL);
    let capacity_bytes = env_u64(
        "RBTC_MDBX_GATE_CAPACITY_BYTES",
        DEFAULT_CHAINSTATE_CAPACITY_BYTES,
    );
    let compact_enabled = enabled("RBTC_MDBX_GATE_COMPACT", true);
    let compact_trigger_percent = env_u8(
        "RBTC_MDBX_GATE_COMPACT_PERCENT",
        DEFAULT_COMPACTION_TRIGGER_PERCENT,
    );
    let recompact_growth_percent = env_u8(
        "RBTC_MDBX_GATE_RECOMPACT_GROWTH_PERCENT",
        DEFAULT_RECOMPACT_GROWTH_PERCENT,
    );
    assert!(live_utxos > 0);
    assert!(updates_per_block > 0 && u64::from(updates_per_block) <= live_utxos);
    assert!((1..=256).contains(&commit_batch));
    assert!(seed_batch > 0);
    assert!(report_interval > 0);
    fs::create_dir_all(
        database_dir
            .parent()
            .expect("gate directory needs a parent"),
    )
    .unwrap();
    let report_path = env::var_os("RBTC_MDBX_GATE_REPORT")
        .map_or_else(|| database_dir.with_extension("report.json"), PathBuf::from);

    let started_epoch = epoch();
    let started = Instant::now();
    let mut report = GateReport {
        schema: 1,
        boundary: "synthetic storage churn; not real Bitcoin blocks or validation",
        revision: revision(),
        started_epoch,
        finished_epoch: None,
        start_height: 0,
        workload: WorkloadReport {
            live_utxos,
            target_blocks,
            updates_per_block,
            seed_batch,
            commit_batch,
            undo_retention,
            capacity_bytes,
            compact_enabled,
            compact_trigger_percent,
            recompact_growth_percent,
        },
        checkpoints: Vec::new(),
        compactions: Vec::new(),
        final_audit: None,
    };
    let mut store = MdbxUtxoStore::open_with_capacity(&database_dir, capacity_bytes).unwrap();
    let mut tip = store.execution_tip().ok();
    if tip.is_none() {
        let genesis = ExecutionTip {
            height: 0,
            hash: block_hash(0),
        };
        store.initialize_execution_tip(genesis).unwrap();
        tip = Some(genesis);
    }
    let mut tip = tip.unwrap();
    report.start_height = tip.height;
    let seeded = store.metrics().unwrap().hot_entries + store.metrics().unwrap().cold_entries;
    assert!(
        tip.height > 0 || seeded <= live_utxos,
        "partial seed exceeds target"
    );
    if tip.height == 0 {
        let mut offset = seeded;
        while offset < live_utxos {
            let end = offset.saturating_add(seed_batch).min(live_utxos);
            let entries = (offset..end)
                .map(|slot| (outpoint(slot, 0), coin(0)))
                .collect::<Vec<_>>();
            store.apply(&[], &entries).unwrap();
            offset = end;
            if offset == live_utxos || offset % 10_000_000 == 0 {
                report
                    .checkpoints
                    .push(metrics(0, 0, started.elapsed(), store.metrics().unwrap()));
                write_report(&report_path, &report);
            }
        }
    } else {
        assert_eq!(seeded, live_utxos, "resumed live set differs from target");
    }

    if tip.height > undo_retention {
        for chunk_start in (1..=tip.height - undo_retention).step_by(10_000) {
            let chunk_end = chunk_start
                .saturating_add(9_999)
                .min(tip.height - undo_retention);
            let hashes = (chunk_start..=chunk_end)
                .map(block_hash)
                .collect::<Vec<_>>();
            store.remove_block_undos(&hashes).unwrap();
        }
    }
    let mut last_compacted_bytes = report.compactions.last().map(|row| row.after_bytes);
    while tip.height < target_blocks {
        let end = tip.height.saturating_add(commit_batch).min(target_blocks);
        let transitions = (tip.height + 1..=end)
            .map(|height| transition(height, live_utxos, updates_per_block))
            .collect::<Vec<_>>();
        store.commit_connect_batch(&transitions).unwrap();
        let previous_tip = tip.height;
        tip = transitions.last().expect("non-empty batch").next;
        let previous_prune = previous_tip.saturating_sub(undo_retention);
        let prune_through = tip.height.saturating_sub(undo_retention);
        if prune_through > previous_prune {
            let hashes = (previous_prune + 1..=prune_through)
                .map(block_hash)
                .collect::<Vec<_>>();
            store.remove_block_undos(&hashes).unwrap();
        }
        if tip.height == target_blocks
            || tip.height / report_interval > previous_tip / report_interval
        {
            report.checkpoints.push(metrics(
                tip.height,
                report.start_height,
                started.elapsed(),
                store.metrics().unwrap(),
            ));
            write_report(&report_path, &report);
        }
        if compact_enabled
            && store
                .compaction_is_worthwhile(
                    compact_trigger_percent,
                    recompact_growth_percent,
                    last_compacted_bytes,
                )
                .unwrap()
        {
            let compact_started = Instant::now();
            let compacted = store.compact().unwrap();
            last_compacted_bytes = Some(compacted.after_bytes);
            report
                .compactions
                .push(compaction(tip.height, compact_started.elapsed(), compacted));
            write_report(&report_path, &report);
        }
    }
    report.final_audit = Some(audit(store.audit().unwrap()));
    report.finished_epoch = Some(epoch());
    write_report(&report_path, &report);
    assert_eq!(tip.height, target_blocks);
    assert_eq!(
        store.metrics().unwrap().hot_entries + store.metrics().unwrap().cold_entries,
        live_utxos
    );
    assert!(store.metrics().unwrap().undo_entries <= u64::from(undo_retention));
}
