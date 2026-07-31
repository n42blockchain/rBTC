//! Build and report an immutable BBHash sidecar for a Core AssumeUTXO snapshot.

use std::{env, path::PathBuf};

use bitcoin::Network;
use rbtc::core_snapshot::CoreSnapshotIndex;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut arguments = env::args_os().skip(1);
    let snapshot = PathBuf::from(
        arguments
            .next()
            .ok_or("usage: build_core_snapshot_index SNAPSHOT INDEX")?,
    );
    let index = PathBuf::from(
        arguments
            .next()
            .ok_or("usage: build_core_snapshot_index SNAPSHOT INDEX")?,
    );
    if arguments.next().is_some() {
        return Err("usage: build_core_snapshot_index SNAPSHOT INDEX".into());
    }

    let metadata = CoreSnapshotIndex::build(&snapshot, &index, Network::Bitcoin)?;
    let index = CoreSnapshotIndex::open(&snapshot, &index, Network::Bitcoin)?;
    println!("base_block_hash={}", metadata.base_block_hash);
    println!("utxo_count={}", metadata.coins_count);
    println!("txid_group_count={}", index.group_count());
    println!("serialized_key_bytes={}", index.serialized_key_bytes());
    println!(
        "average_serialized_key_bytes={:.6}",
        index.average_serialized_key_bytes()
    );
    println!("source_sha256={}", hex(&index.snapshot_sha256()));
    Ok(())
}

fn hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}
