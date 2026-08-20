#![cfg(feature = "mdbx")]

//! Abrupt-process compact-copy recovery matrix.

use std::{env, process::Command};

use bitcoin::{BlockHash, OutPoint, Txid, hashes::Hash};
use rbtc::{
    OutPointKey, Utxo,
    chain_store::ExecutionChainStore,
    execution_store::ExecutionTip,
    mdbx_utxo::{MdbxCompactionPhase, MdbxUtxoStore},
    utxo::{UtxoStore, UtxoUndo},
};
use tempfile::TempDir;

fn key(slot: u32, generation: u32) -> OutPointKey {
    let mut bytes = [0_u8; 32];
    bytes[..4].copy_from_slice(&slot.to_be_bytes());
    bytes[4..8].copy_from_slice(&generation.to_be_bytes());
    OutPoint::new(Txid::from_byte_array(bytes), 0).into()
}

fn coin(height: u32) -> Utxo {
    Utxo {
        value_sats: 42,
        height,
        is_coinbase: false,
        last_touched: 0,
        creation_mtp: height.saturating_mul(600),
        script_pubkey: vec![0x51],
    }
}

fn block_hash(height: u32) -> BlockHash {
    let mut bytes = [0_u8; 32];
    bytes[..4].copy_from_slice(&height.to_be_bytes());
    BlockHash::from_byte_array(bytes)
}

fn seed(path: &std::path::Path) -> [u8; 32] {
    let store = MdbxUtxoStore::open(path).unwrap();
    let genesis = ExecutionTip {
        height: 0,
        hash: block_hash(0),
    };
    store.initialize_execution_tip(genesis).unwrap();
    let mut live = (0..512)
        .map(|slot| (key(slot, 0), coin(0)))
        .collect::<Vec<_>>();
    store.apply(&[], &live).unwrap();
    for generation in 1..=8 {
        let spent = live.iter().map(|(key, _)| *key).collect::<Vec<_>>();
        live = (0..512)
            .map(|slot| (key(slot, generation), coin(0)))
            .collect();
        store.apply(&spent, &live).unwrap();
    }
    let replacement = (key(0, 99), coin(1));
    store
        .commit_connect(
            genesis.hash,
            ExecutionTip {
                height: 1,
                hash: block_hash(1),
            },
            &[live[0].0],
            std::slice::from_ref(&replacement),
            &[UtxoUndo::from_parts(
                vec![(live[0].0, live[0].1.clone())],
                vec![replacement.0],
            )],
        )
        .unwrap();
    store.audit().unwrap().content_sha256
}

fn phase_name(phase: MdbxCompactionPhase) -> &'static str {
    match phase {
        MdbxCompactionPhase::CopySynced => "copy-synced",
        MdbxCompactionPhase::SourceRenamed => "source-renamed",
        MdbxCompactionPhase::SourceRenameSynced => "source-rename-synced",
        MdbxCompactionPhase::CopyPromoted => "copy-promoted",
        MdbxCompactionPhase::CopyPromotionSynced => "copy-promotion-synced",
    }
}

fn parse_phase(value: &str) -> MdbxCompactionPhase {
    [
        MdbxCompactionPhase::CopySynced,
        MdbxCompactionPhase::SourceRenamed,
        MdbxCompactionPhase::SourceRenameSynced,
        MdbxCompactionPhase::CopyPromoted,
        MdbxCompactionPhase::CopyPromotionSynced,
    ]
    .into_iter()
    .find(|phase| phase_name(*phase) == value)
    .expect("known crash phase")
}

#[test]
#[ignore = "subprocess worker invoked by the crash-matrix test"]
fn mdbx_compaction_crash_worker() {
    let Some(path) = env::var_os("RBTC_MDBX_CRASH_PATH") else {
        return;
    };
    let phase = parse_phase(&env::var("RBTC_MDBX_CRASH_PHASE").expect("crash phase"));
    let mut store = MdbxUtxoStore::open(path).unwrap();
    store
        .compact_with_phase_hook(|reached| {
            if reached == phase {
                // `exit` skips Rust destructors and reproduces an abrupt
                // process boundary without creating a macOS crash report.
                std::process::exit(86);
            }
        })
        .unwrap();
    panic!("worker did not terminate at requested phase");
}

#[test]
fn abrupt_exit_at_every_compaction_boundary_recovers_exact_four_table_state() {
    let executable = env::current_exe().expect("test executable");
    for phase in [
        MdbxCompactionPhase::CopySynced,
        MdbxCompactionPhase::SourceRenamed,
        MdbxCompactionPhase::SourceRenameSynced,
        MdbxCompactionPhase::CopyPromoted,
        MdbxCompactionPhase::CopyPromotionSynced,
    ] {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("mdbx");
        let expected = seed(&path);
        let status = Command::new(&executable)
            .args([
                "--exact",
                "mdbx_compaction_crash_worker",
                "--ignored",
                "--nocapture",
            ])
            .env("RBTC_MDBX_CRASH_PATH", &path)
            .env("RBTC_MDBX_CRASH_PHASE", phase_name(phase))
            .status()
            .expect("run crash worker");
        assert_eq!(status.code(), Some(86), "phase {}", phase_name(phase));

        let recovered = MdbxUtxoStore::open(&path).unwrap();
        let audit = recovered.audit().unwrap();
        assert_eq!(
            audit.content_sha256,
            expected,
            "phase {}",
            phase_name(phase)
        );
        assert_eq!(audit.hot_entries, 512);
        assert_eq!(audit.undo_entries, 1);
        assert_eq!(audit.tip.unwrap().height, 1);
    }
}
