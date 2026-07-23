//! Opt-in storage benchmarks using a deterministic, generated UTXO workload.

use std::{
    fs,
    hint::black_box,
    path::Path,
    time::{Duration, Instant},
};

use bitcoin::{BlockHash, OutPoint, Txid, hashes::Hash};
use rbtc::{
    chain_store::{ChainStoreOptions, RedbChainStore},
    execution_store::ExecutionTip,
    snapshot::{export_snapshot, verify_snapshot},
    utxo::{OutPointKey, RedbUtxoStore, Utxo, UtxoStore},
};
use serde::Serialize;
use tempfile::TempDir;

#[cfg(feature = "mdbx")]
use rbtc::mdbx_utxo::MdbxUtxoStore;

const DEFAULT_BLOCKS: u32 = 100;
const DEFAULT_UPDATES_PER_BLOCK: u32 = 100;
const DEFAULT_UTXO_COUNT: u32 = 10_000;
const DEFAULT_LOOKUPS: u32 = 20_000;
const MAX_BLOCKS: u32 = 10_000;
const MAX_UTXO_COUNT: u32 = 10_000_000;
const MAX_LOOKUPS: u32 = 10_000_000;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
struct Workload {
    blocks: u32,
    updates_per_block: u32,
    utxo_count: u32,
    lookups: u32,
}

impl Workload {
    fn new(
        blocks: u32,
        updates_per_block: u32,
        utxo_count: u32,
        lookups: u32,
    ) -> Result<Self, String> {
        if !(1..=MAX_BLOCKS).contains(&blocks) {
            return Err(format!("blocks must be in 1..={MAX_BLOCKS}"));
        }
        if !(1..=MAX_UTXO_COUNT).contains(&utxo_count) {
            return Err(format!("UTXO count must be in 1..={MAX_UTXO_COUNT}"));
        }
        if updates_per_block == 0 || updates_per_block > utxo_count {
            return Err("updates per block must be in 1..=UTXO count".to_owned());
        }
        if !(1..=MAX_LOOKUPS).contains(&lookups) {
            return Err(format!("lookups must be in 1..={MAX_LOOKUPS}"));
        }
        Ok(Self {
            blocks,
            updates_per_block,
            utxo_count,
            lookups,
        })
    }

    fn from_env() -> Self {
        Self::new(
            env_u32("RBTC_BENCH_BLOCKS", DEFAULT_BLOCKS),
            env_u32("RBTC_BENCH_UPDATES_PER_BLOCK", DEFAULT_UPDATES_PER_BLOCK),
            env_u32("RBTC_BENCH_UTXOS", DEFAULT_UTXO_COUNT),
            env_u32("RBTC_BENCH_LOOKUPS", DEFAULT_LOOKUPS),
        )
        .unwrap_or_else(|error| panic!("invalid storage benchmark workload: {error}"))
    }
}

#[derive(Clone, Copy, Debug, Serialize)]
struct Timings {
    elapsed_ns: u64,
    operations: u64,
    nanoseconds_per_operation: u64,
    p50_ns: Option<u64>,
    p99_ns: Option<u64>,
}

#[derive(Debug, Serialize)]
struct BackendResult {
    backend: &'static str,
    quick_repair: Option<bool>,
    seed: Timings,
    mutation: Timings,
    lookup: Timings,
    database_bytes: u64,
    compaction: Option<CompactionResult>,
}

#[derive(Debug, Serialize)]
struct CompactionResult {
    elapsed_ns: u64,
    performed: bool,
    before_bytes: u64,
    after_bytes: u64,
}

#[derive(Debug, Serialize)]
struct SnapshotResult {
    export: Timings,
    verify: Timings,
    import: Timings,
    utxo_count: u64,
    records_bytes: u64,
    compressed_file_bytes: u64,
}

#[derive(Debug, Serialize)]
struct BenchmarkReport {
    schema_version: u32,
    generated_fixture: bool,
    workload: Workload,
    backends: Vec<BackendResult>,
    snapshot: SnapshotResult,
}

fn env_u32(name: &str, default: u32) -> u32 {
    std::env::var(name).map_or(default, |value| {
        value
            .parse()
            .unwrap_or_else(|_| panic!("{name} must be an unsigned 32-bit integer"))
    })
}

fn key(generation: u32, index: u32) -> OutPointKey {
    let mut bytes = [0_u8; 32];
    bytes[..4].copy_from_slice(&generation.to_le_bytes());
    bytes[4..8].copy_from_slice(&index.to_le_bytes());
    OutPoint::new(Txid::from_byte_array(bytes), 0).into()
}

fn coin(height: u32) -> Utxo {
    let mut script_pubkey = vec![0_u8; 22];
    script_pubkey[1] = 0x14;
    Utxo {
        value_sats: 50_000,
        height,
        is_coinbase: false,
        last_touched: u64::from(height),
        creation_mtp: height,
        script_pubkey,
    }
}

fn initial_entries(workload: Workload) -> Vec<(OutPointKey, Utxo)> {
    (0..workload.utxo_count)
        .map(|index| (key(0, index), coin(0)))
        .collect()
}

fn percentile(samples: &mut [Duration], numerator: usize, denominator: usize) -> Duration {
    samples.sort_unstable();
    samples[(samples.len() - 1) * numerator / denominator]
}

fn duration_ns(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

fn timings(elapsed: Duration, operations: u64, samples: &mut [Duration]) -> Timings {
    let elapsed_ns = duration_ns(elapsed);
    Timings {
        elapsed_ns,
        operations,
        nanoseconds_per_operation: elapsed_ns / operations,
        p50_ns: Some(duration_ns(percentile(samples, 50, 100))),
        p99_ns: Some(duration_ns(percentile(samples, 99, 100))),
    }
}

fn one_phase_timing(elapsed: Duration, operations: u64) -> Timings {
    let elapsed_ns = duration_ns(elapsed);
    Timings {
        elapsed_ns,
        operations,
        nanoseconds_per_operation: elapsed_ns / operations,
        p50_ns: None,
        p99_ns: None,
    }
}

fn run_mutations<S: UtxoStore>(store: &S, workload: Workload) -> Timings {
    let started = Instant::now();
    let mut samples = Vec::with_capacity(workload.blocks as usize);
    for height in 1..=workload.blocks {
        let spent = (0..workload.updates_per_block)
            .map(|index| key(height - 1, index))
            .collect::<Vec<_>>();
        let created = (0..workload.updates_per_block)
            .map(|index| (key(height, index), coin(height)))
            .collect::<Vec<_>>();
        let before = Instant::now();
        store
            .apply(&spent, &created)
            .expect("commit benchmark UTXO batch");
        samples.push(before.elapsed());
    }
    timings(started.elapsed(), u64::from(workload.blocks), &mut samples)
}

fn run_lookups<S: UtxoStore>(store: &S, workload: Workload) -> Timings {
    let started = Instant::now();
    let mut samples = Vec::with_capacity(workload.lookups as usize);
    for sample in 0..workload.lookups {
        let index = sample % workload.utxo_count;
        let expected_hit = sample % 4 != 0;
        let generation = if expected_hit {
            if index < workload.updates_per_block {
                workload.blocks
            } else {
                0
            }
        } else {
            u32::MAX
        };
        let before = Instant::now();
        let found =
            black_box(store.get(black_box(key(generation, index)))).expect("benchmark UTXO lookup");
        samples.push(before.elapsed());
        assert_eq!(found.is_some(), expected_hit);
    }
    timings(started.elapsed(), u64::from(workload.lookups), &mut samples)
}

fn directory_bytes(path: &Path) -> u64 {
    fs::read_dir(path)
        .expect("benchmark database directory")
        .map(|entry| {
            entry
                .expect("benchmark database directory entry")
                .metadata()
                .expect("benchmark database entry metadata")
                .len()
        })
        .sum()
}

fn run_redb(workload: Workload, quick_repair: bool) -> BackendResult {
    let directory = TempDir::new().expect("benchmark tempdir");
    let database_path = directory.path().join("chainstate.redb");
    let store = RedbChainStore::open_with_options(
        &database_path,
        bitcoin::Network::Regtest,
        ChainStoreOptions { quick_repair },
    )
    .expect("open benchmark store");
    let initial = initial_entries(workload);
    let seed_started = Instant::now();
    store.apply(&[], &initial).expect("seed benchmark UTXOs");
    let seed = one_phase_timing(seed_started.elapsed(), u64::from(workload.utxo_count));

    let genesis =
        bitcoin::blockdata::constants::genesis_block(bitcoin::Network::Regtest).block_hash();
    let mut parent = genesis;
    let mutation_started = Instant::now();
    let mut mutation_samples = Vec::with_capacity(workload.blocks as usize);
    for height in 1..=workload.blocks {
        let spent = (0..workload.updates_per_block)
            .map(|index| key(height - 1, index))
            .collect::<Vec<_>>();
        let created = (0..workload.updates_per_block)
            .map(|index| (key(height, index), coin(height)))
            .collect::<Vec<_>>();
        let mut hash_bytes = [0_u8; 32];
        hash_bytes[..4].copy_from_slice(&height.to_le_bytes());
        hash_bytes[4..8].copy_from_slice(&u32::MAX.to_le_bytes());
        let hash = BlockHash::from_byte_array(hash_bytes);
        let before = Instant::now();
        store
            .commit_connect(parent, ExecutionTip { height, hash }, &spent, &created, &[])
            .expect("commit benchmark block");
        mutation_samples.push(before.elapsed());
        parent = hash;
    }
    let mutation = timings(
        mutation_started.elapsed(),
        u64::from(workload.blocks),
        &mut mutation_samples,
    );
    let lookup = run_lookups(&store, workload);
    let database_bytes = fs::metadata(&database_path)
        .expect("benchmark database metadata")
        .len();
    drop(store);
    let compaction_started = Instant::now();
    let performed =
        RedbChainStore::compact_file(&database_path).expect("compact benchmark chainstate");
    let compaction_elapsed = compaction_started.elapsed();
    let after_bytes = fs::metadata(&database_path)
        .expect("compacted benchmark database metadata")
        .len();
    let reopened = RedbChainStore::open(&database_path, bitcoin::Network::Regtest)
        .expect("reopen compacted benchmark chainstate");
    assert_eq!(
        reopened
            .execution()
            .tip()
            .expect("read compacted execution tip")
            .height,
        workload.blocks
    );
    let compaction = CompactionResult {
        elapsed_ns: duration_ns(compaction_elapsed),
        performed,
        before_bytes: database_bytes,
        after_bytes,
    };
    BackendResult {
        backend: "redb-chainstate",
        quick_repair: Some(quick_repair),
        seed,
        mutation,
        lookup,
        database_bytes,
        compaction: Some(compaction),
    }
}

#[cfg(feature = "mdbx")]
fn run_mdbx(workload: Workload) -> BackendResult {
    let directory = TempDir::new().expect("benchmark tempdir");
    let path = directory.path().join("utxo.mdbx");
    let store = MdbxUtxoStore::open(&path).expect("open benchmark store");
    let initial = initial_entries(workload);
    let seed_started = Instant::now();
    store.apply(&[], &initial).expect("seed benchmark UTXOs");
    let seed = one_phase_timing(seed_started.elapsed(), u64::from(workload.utxo_count));
    let mutation = run_mutations(&store, workload);
    let lookup = run_lookups(&store, workload);
    BackendResult {
        backend: "mdbx-utxo",
        quick_repair: None,
        seed,
        mutation,
        lookup,
        database_bytes: directory_bytes(&path),
        compaction: None,
    }
}

fn run_snapshot(workload: Workload) -> SnapshotResult {
    let directory = TempDir::new().expect("snapshot benchmark tempdir");
    let source =
        RedbUtxoStore::open(directory.path().join("source.redb")).expect("open snapshot source");
    source
        .apply(&[], &initial_entries(workload))
        .expect("seed snapshot source");
    let snapshot_path = directory.path().join("benchmark.rbtc");

    let export_started = Instant::now();
    let manifest = export_snapshot(
        &source,
        &snapshot_path,
        "regtest",
        workload.blocks,
        "benchmark-anchor",
    )
    .expect("export benchmark snapshot");
    let export = one_phase_timing(export_started.elapsed(), manifest.utxo_count);

    let verify_started = Instant::now();
    let verified = verify_snapshot(&snapshot_path).expect("verify benchmark snapshot");
    let verify = one_phase_timing(verify_started.elapsed(), manifest.utxo_count);

    let destination = RedbUtxoStore::open(directory.path().join("destination.redb"))
        .expect("open snapshot target");
    let import_started = Instant::now();
    verified
        .install_into(
            &destination,
            "benchmark-anchor",
            u64::from(workload.blocks),
            u64::MAX,
        )
        .expect("import benchmark snapshot");
    let import = one_phase_timing(import_started.elapsed(), manifest.utxo_count);
    assert_eq!(
        destination
            .tier_stats()
            .expect("count imported snapshot UTXOs")
            .hot,
        manifest.utxo_count
    );

    SnapshotResult {
        export,
        verify,
        import,
        utxo_count: manifest.utxo_count,
        records_bytes: manifest.records_bytes,
        compressed_file_bytes: fs::metadata(snapshot_path)
            .expect("snapshot benchmark metadata")
            .len(),
    }
}

fn write_report(report: &BenchmarkReport) {
    let json = serde_json::to_string_pretty(report).expect("serialize benchmark report");
    println!("{json}");
    if let Ok(output) = std::env::var("RBTC_BENCH_REPORT") {
        let output = Path::new(&output);
        if let Some(parent) = output
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            fs::create_dir_all(parent).expect("create benchmark report directory");
        }
        fs::write(output, format!("{json}\n")).expect("write benchmark report");
    }
}

#[test]
fn generated_workload_is_deterministic_and_bounded() {
    let workload = Workload::new(2, 2, 4, 8).unwrap();
    assert_eq!(initial_entries(workload), initial_entries(workload));
    assert_eq!(key(7, 9), key(7, 9));
    assert_ne!(key(7, 9), key(8, 9));
    assert!(Workload::new(0, 1, 1, 1).is_err());
    assert!(Workload::new(1, 2, 1, 1).is_err());
    assert!(Workload::new(1, 1, 1, 0).is_err());
}

#[test]
#[ignore = "run explicitly with cargo test --release --all-features --test storage_bench -- --ignored --nocapture"]
fn reproducible_storage_workload() {
    let workload = Workload::from_env();
    let mut backends = vec![run_redb(workload, false), run_redb(workload, true)];
    #[cfg(feature = "mdbx")]
    backends.push(run_mdbx(workload));
    write_report(&BenchmarkReport {
        schema_version: 2,
        generated_fixture: true,
        workload,
        backends,
        snapshot: run_snapshot(workload),
    });
}
