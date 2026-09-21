//! Revalidated persistent-header query probe; not whole-node acceptance.
//! Usage: cargo run --release --example header_index_probe -- 2100000
use bitcoin::{
    Network, TxMerkleNode,
    block::{Header, Version},
    hashes::Hash,
    pow::Target,
};
use rbtc::{
    deployments::DeploymentConfig,
    header_index::DiskHeaderIndex,
    headers::{HeaderView, HeaderWorkBudget},
};
use std::{process::Command, time::Instant};

#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let count: u32 = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "2100000".into())
        .parse()?;
    if count == 0 {
        return Err("count must be positive".into());
    }
    let directory = tempfile::TempDir::new()?;
    let path = directory.path().join("derived-index");
    let mut index =
        DiskHeaderIndex::create(&path, DeploymentConfig::for_network(Network::Regtest))?;
    let mut parent = index.snapshot()?.active_tip().header;
    let started = Instant::now();
    let mut batch = Vec::with_capacity(2000);
    let mut checked = Vec::new();
    for height in 1..=count {
        let target = Target::MAX_ATTAINABLE_REGTEST;
        let mut header = Header {
            version: Version::from_consensus(4),
            prev_blockhash: parent.block_hash(),
            merkle_root: TxMerkleNode::all_zeros(),
            time: parent.time + 1,
            bits: target.to_compact_lossy(),
            nonce: 0,
        };
        while header.validate_pow(target).is_err() {
            header.nonce += 1;
        }
        parent = header;
        batch.push(header);
        if height % 100_000 == 0 || height == count {
            checked.push((height, header.block_hash()));
        }
        if batch.len() == 2000 || height == count {
            index.append(&batch, u32::MAX, &mut HeaderWorkBudget::default())?;
            batch.clear();
        }
        if height % 200_000 == 0 || height == count {
            let rss = Command::new("ps")
                .args(["-o", "rss=", "-p", &std::process::id().to_string()])
                .output()?;
            if !rss.status.success() {
                return Err("ps failed".into());
            }
            println!(
                "{}",
                serde_json::json!({"headers": height, "elapsed_micros": started.elapsed().as_micros(),
                "rss_kib": std::str::from_utf8(&rss.stdout)?.trim().parse::<u64>()?, "file_bytes": std::fs::metadata(&path)?.len()})
            );
        }
    }
    let view = index.snapshot()?;
    assert_eq!(view.active_tip().height, count);
    assert_eq!(view.active_tip().hash, parent.block_hash());
    for (height, hash) in checked {
        assert_eq!(view.active_header(height)?.unwrap().hash, hash);
    }
    assert_eq!(view.block_locator()?.first(), Some(&parent.block_hash()));
    println!(
        "{}",
        serde_json::json!({"result":"PASS", "headers":count, "tip":parent.block_hash().to_string(),
        "elapsed_micros":started.elapsed().as_micros(), "file_bytes":std::fs::metadata(&path)?.len()})
    );
    drop(view);
    drop(index);
    directory.close()?;
    Ok(())
}
