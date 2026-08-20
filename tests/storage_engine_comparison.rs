//! Opt-in, matched chainstate benchmark for storage-engine decisions.
//!
//! Unlike the older microbenchmark, every measured backend persists the same
//! compact logical UTXO set, per-block undo, and execution tip. Serving mode
//! fsyncs every block; IBD mode folds exactly 256 blocks into one durable
//! transaction. Results are evidence for this host and workload, not a proxy
//! for script verification or complete-node IBD.

use std::{
    fs,
    path::Path,
    time::{Duration, Instant},
};

use bitcoin::{BlockHash, Network, OutPoint, Txid, hashes::Hash};
use rbtc::{
    chain_store::{ChainStoreOptions, ConnectTransition, ExecutionChainStore, RedbChainStore},
    execution_store::ExecutionTip,
    mdbx_utxo::MdbxUtxoStore,
    utxo::{OutPointKey, Utxo, UtxoStore, UtxoUndo},
};
use serde::Serialize;
use tempfile::TempDir;

const DEFAULT_UTXOS: u32 = 200_000;
const DEFAULT_BLOCKS: u32 = 256;
const DEFAULT_UPDATES: u32 = 1_000;
const DEFAULT_LOOKUPS: u32 = 100_000;
const LOOKUP_BATCH: usize = 4_096;

#[derive(Clone, Copy, Debug, Serialize)]
struct Workload {
    utxos: u32,
    blocks: u32,
    updates_per_block: u32,
    lookups: u32,
}

impl Workload {
    fn from_env() -> Self {
        let workload = Self {
            utxos: env_u32("RBTC_ENGINE_BENCH_UTXOS", DEFAULT_UTXOS),
            blocks: env_u32("RBTC_ENGINE_BENCH_BLOCKS", DEFAULT_BLOCKS),
            updates_per_block: env_u32("RBTC_ENGINE_BENCH_UPDATES", DEFAULT_UPDATES),
            lookups: env_u32("RBTC_ENGINE_BENCH_LOOKUPS", DEFAULT_LOOKUPS),
        };
        assert!(workload.utxos > 0 && workload.utxos <= 10_000_000);
        assert!(workload.blocks > 0 && workload.blocks <= 10_000);
        assert!(workload.updates_per_block > 0 && workload.updates_per_block <= workload.utxos);
        assert!(workload.lookups > 0 && workload.lookups <= 10_000_000);
        workload
    }
}

#[derive(Clone, Copy, Debug, Serialize)]
struct Scenario {
    name: &'static str,
    blocks_per_commit: u32,
}

#[derive(Debug, Serialize)]
struct BackendResult {
    backend: &'static str,
    scenario: &'static str,
    blocks_per_commit: u32,
    seed_ns: u64,
    mutation_ns: u64,
    blocks_per_second: f64,
    utxo_changes_per_second: f64,
    lookup_ns: u64,
    lookups_per_second: f64,
    logical_bytes_before_compaction: u64,
    allocated_bytes_before_compaction: u64,
    logical_bytes_after_compaction: u64,
    allocated_bytes_after_compaction: u64,
    compaction_ns: u64,
}

#[derive(Debug, Serialize)]
struct Report {
    schema_version: u32,
    revision: String,
    host: String,
    comparison_boundary: &'static str,
    durability: &'static str,
    lookup_view: &'static str,
    execution_order: &'static str,
    workload: Workload,
    results: Vec<BackendResult>,
}

struct LiveSet {
    keys: Vec<OutPointKey>,
    coins: Vec<Utxo>,
    cursor: usize,
    updates_per_block: usize,
}

impl LiveSet {
    fn new(workload: Workload) -> Self {
        let mut keys = Vec::with_capacity(workload.utxos as usize);
        let mut coins = Vec::with_capacity(workload.utxos as usize);
        for index in 0..workload.utxos {
            keys.push(key(0, index));
            coins.push(coin(0, index));
        }
        Self {
            keys,
            coins,
            cursor: 0,
            updates_per_block: usize::try_from(workload.updates_per_block).expect("u32 fits usize"),
        }
    }

    fn initial_entries(&self) -> Vec<(OutPointKey, Utxo)> {
        self.keys
            .iter()
            .copied()
            .zip(self.coins.iter().cloned())
            .collect()
    }

    fn transition(&mut self, height: u32) -> ConnectTransition {
        let updates = self.updates_per_block;
        let mut spent = Vec::with_capacity(updates);
        let mut created = Vec::with_capacity(updates);
        let mut transaction_undos = Vec::with_capacity(updates.div_ceil(2));
        for pair_start in (0..updates).step_by(2) {
            let mut undo_spent = Vec::with_capacity(2);
            let mut undo_created = Vec::with_capacity(2);
            for offset in 0..2 {
                let ordinal = pair_start + offset;
                if ordinal == updates {
                    break;
                }
                let index = (self.cursor + ordinal) % self.keys.len();
                let old_key = self.keys[index];
                let old_coin = self.coins[index].clone();
                let new_key = key(height, u32::try_from(index).expect("bounded UTXO index"));
                let new_coin = coin(height, u32::try_from(index).expect("bounded UTXO index"));
                spent.push(old_key);
                created.push((new_key, new_coin.clone()));
                undo_spent.push((old_key, old_coin));
                undo_created.push(new_key);
                self.keys[index] = new_key;
                self.coins[index] = new_coin;
            }
            transaction_undos.push(UtxoUndo::from_parts(undo_spent, undo_created));
        }
        self.cursor = (self.cursor + updates) % self.keys.len();
        ConnectTransition {
            expected_parent: block_hash(height - 1),
            next: ExecutionTip {
                height,
                hash: block_hash(height),
            },
            spent,
            created,
            transaction_undos,
        }
    }
}

fn env_u32(name: &str, default: u32) -> u32 {
    std::env::var(name).map_or(default, |value| {
        value
            .parse()
            .unwrap_or_else(|_| panic!("{name} must be an unsigned integer"))
    })
}

fn key(generation: u32, index: u32) -> OutPointKey {
    let mut txid = [0_u8; 32];
    txid[..4].copy_from_slice(&generation.to_be_bytes());
    txid[4..8].copy_from_slice(&index.to_be_bytes());
    txid[8..12].copy_from_slice(&generation.wrapping_mul(0x9e37_79b9).to_be_bytes());
    OutPointKey::from(OutPoint::new(Txid::from_byte_array(txid), index % 4))
}

fn coin(height: u32, index: u32) -> Utxo {
    let mut script = vec![0x76, 0xa9, 0x14];
    script.extend_from_slice(&[index.to_le_bytes()[0]; 20]);
    script.extend_from_slice(&[0x88, 0xac]);
    Utxo {
        value_sats: 50_000 + u64::from(index % 10_000),
        height,
        is_coinbase: false,
        last_touched: 0,
        creation_mtp: height.saturating_mul(600),
        script_pubkey: script,
    }
}

fn block_hash(height: u32) -> BlockHash {
    if height == 0 {
        return bitcoin::blockdata::constants::genesis_block(Network::Regtest).block_hash();
    }
    let mut bytes = [0_u8; 32];
    bytes[..4].copy_from_slice(&height.to_be_bytes());
    bytes[4..8].copy_from_slice(&u32::MAX.to_be_bytes());
    BlockHash::from_byte_array(bytes)
}

fn duration_ns(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

fn run_transitions<C: ExecutionChainStore>(
    store: &C,
    workload: Workload,
    scenario: Scenario,
    live: &mut LiveSet,
) -> Duration {
    let started = Instant::now();
    let mut height = 1;
    while height <= workload.blocks {
        let end = height
            .saturating_add(scenario.blocks_per_commit - 1)
            .min(workload.blocks);
        let transitions = (height..=end)
            .map(|next| live.transition(next))
            .collect::<Vec<_>>();
        store
            .commit_connect_batch(&transitions)
            .expect("commit matched chainstate batch");
        height = end + 1;
    }
    started.elapsed()
}

fn run_lookups<S: UtxoStore>(store: &S, workload: Workload, live: &LiveSet) -> Duration {
    let requests = (0..workload.lookups)
        .map(|ordinal| {
            if ordinal % 4 == 0 {
                key(u32::MAX, ordinal % workload.utxos)
            } else {
                live.keys[usize::try_from(ordinal % workload.utxos).expect("u32 fits usize")]
            }
        })
        .collect::<Vec<_>>();
    let started = Instant::now();
    let mut hits = 0_u32;
    for chunk in requests.chunks(LOOKUP_BATCH) {
        hits += u32::try_from(
            store
                .get_many(chunk)
                .expect("matched batch lookup")
                .iter()
                .filter(|(_, coin)| coin.is_some())
                .count(),
        )
        .expect("hit count fits u32");
    }
    assert_eq!(hits, workload.lookups - workload.lookups.div_ceil(4));
    started.elapsed()
}

fn path_sizes(path: &Path) -> (u64, u64) {
    fn one(path: &Path) -> (u64, u64) {
        let metadata = fs::metadata(path).expect("benchmark path metadata");
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            (metadata.len(), metadata.blocks().saturating_mul(512))
        }
        #[cfg(not(unix))]
        {
            (metadata.len(), metadata.len())
        }
    }
    if path.is_file() {
        return one(path);
    }
    fs::read_dir(path)
        .expect("benchmark database directory")
        .map(|entry| one(&entry.expect("directory entry").path()))
        .fold((0, 0), |total, item| {
            (
                total.0.saturating_add(item.0),
                total.1.saturating_add(item.1),
            )
        })
}

fn rates(workload: Workload, elapsed: Duration) -> (f64, f64) {
    let seconds = elapsed.as_secs_f64();
    (
        f64::from(workload.blocks) / seconds,
        f64::from(workload.blocks) * f64::from(workload.updates_per_block) * 2.0 / seconds,
    )
}

fn redb_result(workload: Workload, scenario: Scenario) -> BackendResult {
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("chainstate.redb");
    let store = RedbChainStore::open_with_options(
        &path,
        Network::Regtest,
        ChainStoreOptions {
            quick_repair: true,
            retain_block_undo: true,
            ..ChainStoreOptions::default()
        },
    )
    .unwrap();
    let mut live = LiveSet::new(workload);
    let seed_started = Instant::now();
    store.apply(&[], &live.initial_entries()).unwrap();
    let seed = seed_started.elapsed();
    let mutation = run_transitions(&store, workload, scenario, &mut live);
    let lookup = run_lookups(&store, workload, &live);
    let before = path_sizes(&path);
    drop(store);
    let compact_started = Instant::now();
    RedbChainStore::compact_file(&path).unwrap();
    let compact = compact_started.elapsed();
    let after = path_sizes(&path);
    let reopened = RedbChainStore::open(&path, Network::Regtest).unwrap();
    assert_eq!(reopened.execution().tip().unwrap().height, workload.blocks);
    let (blocks_per_second, utxo_changes_per_second) = rates(workload, mutation);
    BackendResult {
        backend: "rbtc-redb-complete-chainstate",
        scenario: scenario.name,
        blocks_per_commit: scenario.blocks_per_commit,
        seed_ns: duration_ns(seed),
        mutation_ns: duration_ns(mutation),
        blocks_per_second,
        utxo_changes_per_second,
        lookup_ns: duration_ns(lookup),
        lookups_per_second: f64::from(workload.lookups) / lookup.as_secs_f64(),
        logical_bytes_before_compaction: before.0,
        allocated_bytes_before_compaction: before.1,
        logical_bytes_after_compaction: after.0,
        allocated_bytes_after_compaction: after.1,
        compaction_ns: duration_ns(compact),
    }
}

fn mdbx_result(workload: Workload, scenario: Scenario) -> BackendResult {
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("chainstate.mdbx");
    let mut store = MdbxUtxoStore::open(&path).unwrap();
    store
        .initialize_execution_tip(ExecutionTip {
            height: 0,
            hash: block_hash(0),
        })
        .unwrap();
    let mut live = LiveSet::new(workload);
    let seed_started = Instant::now();
    store.apply(&[], &live.initial_entries()).unwrap();
    let seed = seed_started.elapsed();
    let mutation = run_transitions(&store, workload, scenario, &mut live);
    let lookup = run_lookups(&store, workload, &live);
    let before = path_sizes(&path);
    let compact_started = Instant::now();
    store.compact().unwrap();
    let compact = compact_started.elapsed();
    let after = path_sizes(&path);
    assert_eq!(store.execution_tip().unwrap().height, workload.blocks);
    let (blocks_per_second, utxo_changes_per_second) = rates(workload, mutation);
    BackendResult {
        backend: "rbtc-mdbx-complete-chainstate",
        scenario: scenario.name,
        blocks_per_commit: scenario.blocks_per_commit,
        seed_ns: duration_ns(seed),
        mutation_ns: duration_ns(mutation),
        blocks_per_second,
        utxo_changes_per_second,
        lookup_ns: duration_ns(lookup),
        lookups_per_second: f64::from(workload.lookups) / lookup.as_secs_f64(),
        logical_bytes_before_compaction: before.0,
        allocated_bytes_before_compaction: before.1,
        logical_bytes_after_compaction: after.0,
        allocated_bytes_after_compaction: after.1,
        compaction_ns: duration_ns(compact),
    }
}

fn host_description() -> String {
    let output = std::process::Command::new("uname")
        .args(["-a"])
        .output()
        .expect("read host identity");
    String::from_utf8_lossy(&output.stdout).trim().to_owned()
}

fn revision() -> String {
    let output = std::process::Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .expect("read git revision");
    let revision = String::from_utf8_lossy(&output.stdout).trim().to_owned();
    let dirty = std::process::Command::new("git")
        .args(["status", "--porcelain", "--untracked-files=no"])
        .output()
        .expect("read worktree state");
    if dirty.stdout.is_empty() {
        revision
    } else {
        format!("{revision}-dirty")
    }
}

#[test]
#[ignore = "run explicitly with cargo test --release --all-features --test storage_engine_comparison -- --ignored --nocapture"]
fn matched_complete_chainstate_workload() {
    let workload = Workload::from_env();
    let scenarios = [
        Scenario {
            name: "serving",
            blocks_per_commit: 1,
        },
        Scenario {
            name: "ibd-256",
            blocks_per_commit: 256,
        },
    ];
    assert_eq!(
        workload.blocks % 256,
        0,
        "IBD comparison needs full 256-block batches"
    );
    let reverse = std::env::var_os("RBTC_ENGINE_BENCH_REVERSE")
        .is_some_and(|value| !value.is_empty() && value != "0");
    let mut results = Vec::new();
    for scenario in scenarios {
        if reverse {
            results.push(mdbx_result(workload, scenario));
            results.push(redb_result(workload, scenario));
        } else {
            results.push(redb_result(workload, scenario));
            results.push(mdbx_result(workload, scenario));
        }
    }
    let report = Report {
        schema_version: 1,
        revision: revision(),
        host: host_description(),
        comparison_boundary: "complete rBTC chainstate mutation: compact UTXO set plus per-block undo and execution tip; excludes block/script validation and block files",
        durability: "redb immediate/quick-repair; MDBX durable; complete UTXO+undo+tip transaction",
        lookup_view: "one backend read view and open table set per 4096 caller-ordered lookups",
        execution_order: if reverse {
            "MDBX then redb"
        } else {
            "redb then MDBX"
        },
        workload,
        results,
    };
    let json = serde_json::to_string_pretty(&report).unwrap();
    println!("{json}");
    if let Ok(path) = std::env::var("RBTC_ENGINE_BENCH_REPORT") {
        fs::write(path, format!("{json}\n")).unwrap();
    }
}
