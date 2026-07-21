//! Opt-in storage benchmarks using a deterministic block-shaped UTXO workload.

use std::time::{Duration, Instant};

use bitcoin::{BlockHash, OutPoint, Txid, hashes::Hash};
use rbtc::{
    chain_store::{ChainStoreOptions, RedbChainStore},
    execution_store::ExecutionTip,
    utxo::{OutPointKey, Utxo, UtxoStore},
};
use tempfile::TempDir;

#[cfg(feature = "mdbx")]
use rbtc::mdbx_utxo::MdbxUtxoStore;

const BLOCKS: u32 = 100;
const UPDATES_PER_BLOCK: u32 = 100;

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

fn percentile(samples: &mut [Duration], numerator: usize, denominator: usize) -> Duration {
    samples.sort_unstable();
    samples[(samples.len() - 1) * numerator / denominator]
}

fn run_redb(quick_repair: bool) -> (Duration, Duration, Duration, u64) {
    let directory = TempDir::new().expect("benchmark tempdir");
    let store = RedbChainStore::open_with_options(
        directory.path().join("chainstate.redb"),
        bitcoin::Network::Regtest,
        ChainStoreOptions { quick_repair },
    )
    .expect("open benchmark store");
    let initial = (0..UPDATES_PER_BLOCK)
        .map(|index| (key(0, index), coin(0)))
        .collect::<Vec<_>>();
    store.apply(&[], &initial).expect("seed benchmark UTXOs");

    let genesis =
        bitcoin::blockdata::constants::genesis_block(bitcoin::Network::Regtest).block_hash();
    let started = Instant::now();
    let mut samples = Vec::with_capacity(BLOCKS as usize);
    let mut parent = genesis;
    for height in 1..=BLOCKS {
        let spent = (0..UPDATES_PER_BLOCK)
            .map(|index| key(height - 1, index))
            .collect::<Vec<_>>();
        let created = (0..UPDATES_PER_BLOCK)
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
        samples.push(before.elapsed());
        parent = hash;
    }
    let elapsed = started.elapsed();
    let bytes = std::fs::metadata(directory.path().join("chainstate.redb"))
        .expect("benchmark database metadata")
        .len();
    let p50 = percentile(&mut samples, 50, 100);
    let p99 = percentile(&mut samples, 99, 100);
    (elapsed, p50, p99, bytes)
}

#[cfg(feature = "mdbx")]
fn run_mdbx() -> (Duration, Duration, Duration, u64) {
    let directory = TempDir::new().expect("benchmark tempdir");
    let path = directory.path().join("utxo.mdbx");
    let store = MdbxUtxoStore::open(&path).expect("open benchmark store");
    let initial = (0..UPDATES_PER_BLOCK)
        .map(|index| (key(0, index), coin(0)))
        .collect::<Vec<_>>();
    store.apply(&[], &initial).expect("seed benchmark UTXOs");

    let started = Instant::now();
    let mut samples = Vec::with_capacity(BLOCKS as usize);
    for height in 1..=BLOCKS {
        let spent = (0..UPDATES_PER_BLOCK)
            .map(|index| key(height - 1, index))
            .collect::<Vec<_>>();
        let created = (0..UPDATES_PER_BLOCK)
            .map(|index| (key(height, index), coin(height)))
            .collect::<Vec<_>>();
        let before = Instant::now();
        store.apply(&spent, &created).expect("commit MDBX block");
        samples.push(before.elapsed());
    }
    let elapsed = started.elapsed();
    let bytes = std::fs::read_dir(&path)
        .expect("MDBX directory")
        .map(|entry| {
            entry
                .expect("MDBX directory entry")
                .metadata()
                .unwrap()
                .len()
        })
        .sum();
    let p50 = percentile(&mut samples, 50, 100);
    let p99 = percentile(&mut samples, 99, 100);
    (elapsed, p50, p99, bytes)
}

#[test]
#[ignore = "run explicitly with cargo test --release --test storage_bench -- --ignored --nocapture"]
fn redb_quick_repair_cost_on_block_batches() {
    for quick_repair in [false, true] {
        let (elapsed, p50, p99, bytes) = run_redb(quick_repair);
        println!(
            "redb quick_repair={quick_repair}: blocks={BLOCKS} updates/block={UPDATES_PER_BLOCK} elapsed={elapsed:?} p50={p50:?} p99={p99:?} bytes={bytes}"
        );
    }
    #[cfg(feature = "mdbx")]
    {
        let (elapsed, p50, p99, bytes) = run_mdbx();
        println!(
            "mdbx durable: blocks={BLOCKS} updates/block={UPDATES_PER_BLOCK} elapsed={elapsed:?} p50={p50:?} p99={p99:?} bytes={bytes}"
        );
    }
}
