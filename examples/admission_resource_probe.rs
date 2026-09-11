//! Compares admission, reconciliation, small-cluster lookup and pool-clone costs.
//! Usage: cargo run --release --example admission_resource_probe -- [entries] [clone-count]
//! Synthetic regtest UTXOs and transactions; no peer traffic or whole-node cap claim.

use std::{env, fs, hint::black_box, time::Instant};

use bitcoin::{
    Amount, Network, OutPoint, ScriptBuf, Sequence, Transaction, TxIn, TxOut, Txid, Witness,
    absolute::LockTime,
    hashes::Hash,
    opcodes,
    script::{Builder, PushBytesBuf},
    transaction::Version,
};
use rbtc::{
    transaction_admission::{TransactionAdmissionContext, TransactionAdmissionPool},
    utxo::{RedbUtxoStore, Utxo, UtxoStore},
};
use tempfile::TempDir;

#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

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

fn context() -> TransactionAdmissionContext {
    TransactionAdmissionContext {
        height: 200,
        parent_mtp: 1_700_000_000,
        script_flags: bitcoinconsensus::VERIFY_ALL_PRE_TAPROOT | bitcoinconsensus::VERIFY_TAPROOT,
        csv_active: true,
        full_rbf: false,
    }
}

fn spend(index: usize) -> (OutPoint, Utxo, Transaction) {
    let witness_script = Builder::new().push_opcode(opcodes::OP_TRUE).into_script();
    let script = ScriptBuf::new_p2wsh(&witness_script.wscript_hash());
    let mut bytes = [0; 32];
    bytes[..8].copy_from_slice(&u64::try_from(index).unwrap().to_le_bytes());
    let outpoint = OutPoint::new(Txid::from_byte_array(bytes), 0);
    let utxo = Utxo {
        value_sats: 1_000_000,
        height: 1,
        is_coinbase: false,
        last_touched: 0,
        creation_mtp: 1,
        script_pubkey: script.to_bytes(),
    };
    let tx = Transaction {
        version: Version::TWO,
        lock_time: LockTime::ZERO,
        input: vec![TxIn {
            previous_output: outpoint,
            script_sig: ScriptBuf::new(),
            sequence: Sequence::MAX,
            witness: Witness::from_slice(&[witness_script.as_bytes()]),
        }],
        output: vec![TxOut {
            value: Amount::from_sat(990_000),
            script_pubkey: script,
        }],
    };
    (outpoint, utxo, tx)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = env::args().skip(1).collect::<Vec<_>>();
    let count = args.first().map_or(Ok(128), |s| s.parse::<usize>())?;
    let copies = args.get(1).map_or(Ok(8), |s| s.parse::<usize>())?;
    assert!(args.len() <= 2 && (16..=1024).contains(&count) && (1..=16).contains(&copies));
    let directory = TempDir::new()?;
    let store = RedbUtxoStore::open(directory.path().join("utxo.redb"))?;
    let mut pool = TransactionAdmissionPool::with_capacity(count + 1, 128_000_000);
    let data = Builder::new()
        .push_opcode(opcodes::all::OP_RETURN)
        .push_slice(PushBytesBuf::try_from(vec![0; 60_000])?)
        .into_script();
    let started = Instant::now();
    let mut parent = None;
    let mut query_tip = None;
    for index in 0..=count {
        let (outpoint, utxo, mut tx) = spend(index + 1);
        store.apply(&[], &[(outpoint.into(), utxo)])?;
        if index < 16 {
            if let Some(txid) = parent {
                tx.input[0].previous_output = OutPoint::new(txid, 0);
            }
            tx.output[0].value = Amount::from_sat(990_000 - u64::try_from(index)? * 10_000);
            parent = Some(tx.compute_txid());
            query_tip = parent;
        } else {
            tx.output.push(TxOut {
                value: Amount::ZERO,
                script_pubkey: data.clone(),
            });
        }
        if index == count {
            break;
        }
        pool.admit(&store, tx, context())?;
    }
    let seed_micros = started.elapsed().as_micros();
    let started = Instant::now();
    for _ in 0..50 {
        let cluster = black_box(pool.cluster_of(black_box(query_tip.unwrap())).unwrap());
        assert_eq!(cluster.0.len(), 16);
    }
    let query_micros = started.elapsed().as_micros();
    let rss_before_clones = rss_kib();
    let started = Instant::now();
    let clones = (0..copies).map(|_| pool.clone()).collect::<Vec<_>>();
    let clone_micros = started.elapsed().as_micros();
    let rss_with_clones = rss_kib();
    assert!(clones.iter().all(|copy| copy.len() == count));
    let (_, _, candidate) = spend(count + 1);
    let started = Instant::now();
    pool.admit(&store, candidate, context())?;
    let admission_micros = started.elapsed().as_micros();
    let before_reconcile = pool.snapshot();
    let rss_before_reconcile = rss_kib();
    let started = Instant::now();
    let reconciled_removed = pool.reconcile(&store, context());
    let reconcile_micros = started.elapsed().as_micros();
    let rss_after_reconcile = rss_kib();
    assert_eq!(reconciled_removed, 0);
    assert_eq!(pool.snapshot(), before_reconcile);
    println!(
        "{}",
        serde_json::json!({
            "network": Network::Regtest.to_string(), "entries": count, "large_payload_bytes": 60000,
            "cluster_entries": 16, "cluster_queries": 50, "simultaneous_clones": copies,
            "allocator": if cfg!(feature = "mimalloc") { "mimalloc" } else { "system" },
            "seed_micros": seed_micros, "query_micros": query_micros, "clone_micros": clone_micros,
            "admission_micros": admission_micros, "rss_before_clones_kib": rss_before_clones,
            "reconcile_micros": reconcile_micros, "reconciled_removed": reconciled_removed,
            "rss_before_reconcile_kib": rss_before_reconcile,
            "rss_after_reconcile_kib": rss_after_reconcile,
            "rss_with_clones_kib": rss_with_clones, "retained_bytes_after_admission": pool.retained_bytes(),
        })
    );
    black_box(clones);
    Ok(())
}
