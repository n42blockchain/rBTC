//! Process-level crash and damaged-file recovery checks for unified chainstate.

use std::{
    fs,
    process::{Command, Stdio},
    thread,
    time::{Duration, Instant},
};

use bitcoin::{BlockHash, Network, OutPoint, Txid, hashes::Hash};
use rbtc::{
    chain_store::RedbChainStore,
    execution_store::ExecutionTip,
    utxo::{OutPointKey, Utxo, UtxoStore},
};
use tempfile::TempDir;

const WIDTH: u32 = 128;

fn key(height: u32, index: u32) -> OutPointKey {
    let mut bytes = [0_u8; 32];
    bytes[..4].copy_from_slice(&height.to_le_bytes());
    bytes[4..8].copy_from_slice(&index.to_le_bytes());
    OutPoint::new(Txid::from_byte_array(bytes), 0).into()
}

fn hash(height: u32) -> BlockHash {
    let mut bytes = [0_u8; 32];
    bytes[..4].copy_from_slice(&height.to_le_bytes());
    BlockHash::from_byte_array(bytes)
}

fn coin(height: u32) -> Utxo {
    Utxo {
        value_sats: 50_000,
        height,
        is_coinbase: false,
        last_touched: u64::from(height),
        creation_mtp: height,
        script_pubkey: vec![0x51],
    }
}

fn assert_consistent(store: &RedbChainStore) {
    let tip = store.execution().tip().unwrap();
    if tip.height == 0 {
        return;
    }
    assert_eq!(tip.hash, hash(tip.height));
    assert!(store.undos().get(tip.hash).unwrap().is_some());
    for index in 0..WIDTH {
        assert_eq!(
            store.get(key(tip.height, index)).unwrap(),
            Some(coin(tip.height))
        );
        assert!(store.get(key(tip.height - 1, index)).unwrap().is_none());
    }
}

#[test]
#[ignore = "subprocess helper, invoked only by repeated_sigkill_recovers_an_atomic_state"]
fn crash_writer_child() {
    let Ok(database) = std::env::var("RBTC_CRASH_DB") else {
        return;
    };
    let marker = std::env::var("RBTC_CRASH_MARKER").unwrap();
    let store = RedbChainStore::open(database, Network::Regtest).unwrap();
    let mut tip = store.execution().tip().unwrap();
    if tip.height == 0 && store.get(key(0, 0)).unwrap().is_none() {
        let initial = (0..WIDTH)
            .map(|index| (key(0, index), coin(0)))
            .collect::<Vec<_>>();
        store.apply(&[], &initial).unwrap();
    }
    fs::write(&marker, tip.height.to_string()).unwrap();
    loop {
        let height = tip.height.checked_add(1).unwrap();
        let spent = (0..WIDTH)
            .map(|index| key(height - 1, index))
            .collect::<Vec<_>>();
        let created = (0..WIDTH)
            .map(|index| (key(height, index), coin(height)))
            .collect::<Vec<_>>();
        let next = ExecutionTip {
            height,
            hash: hash(height),
        };
        store
            .commit_connect(tip.hash, next, &spent, &created, &[])
            .unwrap();
        tip = next;
        fs::write(&marker, height.to_string()).unwrap();
    }
}

#[cfg(unix)]
#[test]
fn repeated_sigkill_recovers_an_atomic_state() {
    let directory = TempDir::new().unwrap();
    let database = directory.path().join("chainstate.redb");
    let marker = directory.path().join("progress");
    let executable = std::env::current_exe().unwrap();
    let mut prior_height = 0;

    for _ in 0..3 {
        let mut child = Command::new(&executable)
            .args(["--exact", "crash_writer_child", "--ignored", "--nocapture"])
            .env("RBTC_CRASH_DB", &database)
            .env("RBTC_CRASH_MARKER", &marker)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            if let Ok(progress) = fs::read_to_string(&marker) {
                if progress
                    .trim()
                    .parse::<u32>()
                    .is_ok_and(|height| height > prior_height)
                {
                    break;
                }
            }
            assert!(Instant::now() < deadline, "crash writer made no progress");
            thread::sleep(Duration::from_millis(5));
        }
        child.kill().unwrap();
        child.wait().unwrap();

        let store = RedbChainStore::open(&database, Network::Regtest).unwrap();
        assert_consistent(&store);
        prior_height = store.execution().tip().unwrap().height;
    }
}

#[test]
fn truncated_database_is_rejected_or_recovers_a_complete_state() {
    let directory = TempDir::new().unwrap();
    let source = directory.path().join("source.redb");
    let damaged = directory.path().join("damaged.redb");
    let store = RedbChainStore::open(&source, Network::Regtest).unwrap();
    let genesis = store.execution().tip().unwrap();
    store.apply(&[], &[(key(0, 0), coin(0))]).unwrap();
    store
        .commit_connect(
            genesis.hash,
            ExecutionTip {
                height: 1,
                hash: hash(1),
            },
            &[key(0, 0)],
            &[(key(1, 0), coin(1))],
            &[],
        )
        .unwrap();
    drop(store);
    fs::copy(&source, &damaged).unwrap();
    let file = fs::OpenOptions::new().write(true).open(&damaged).unwrap();
    let length = file.metadata().unwrap().len();
    file.set_len(length.saturating_sub(4096)).unwrap();
    drop(file);

    if let Ok(recovered) = RedbChainStore::open(&damaged, Network::Regtest) {
        let tip = recovered.execution().tip().unwrap();
        assert!(tip.height == 0 || tip.height == 1);
        if tip.height == 1 {
            assert_eq!(recovered.get(key(1, 0)).unwrap(), Some(coin(1)));
            assert!(recovered.get(key(0, 0)).unwrap().is_none());
            assert!(recovered.undos().get(hash(1)).unwrap().is_some());
        }
    }
}
