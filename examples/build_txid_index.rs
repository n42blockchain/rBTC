//! Builds the txid-keyed MPHF sidecar so it can be compared against the
//! outpoint-keyed one.
//!
//! Usage: build_txid_index <snapshot.dat> <output.idx>

use std::{env, path::PathBuf, time::Instant};

use bitcoin::Network;
use rbtc::core_snapshot::CoreSnapshotIndex;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = env::args().skip(1);
    let snapshot = PathBuf::from(args.next().ok_or("usage: <snapshot.dat> <output.idx>")?);
    let output = PathBuf::from(args.next().ok_or("missing output path")?);
    let started = Instant::now();
    let metadata = CoreSnapshotIndex::build(&snapshot, &output, Network::Bitcoin)?;
    println!(
        "built in {:.1}s: {} coins, base {}, index {} bytes",
        started.elapsed().as_secs_f64(),
        metadata.coins_count,
        metadata.base_block_hash,
        std::fs::metadata(&output)?.len()
    );
    Ok(())
}
