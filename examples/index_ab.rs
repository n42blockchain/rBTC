//! Head-to-head probe of the outpoint-keyed and txid-keyed snapshot indexes.
//!
//! Both are opened in one process and measured alternately, so machine drift —
//! which has moved consecutive runs on this hardware by a factor of two — acts
//! on both sides equally.
//!
//! The miss path is what the commit's duplicate check runs, and the claim
//! under test is that a miss costs the same under either keying, leaving the
//! whole difference to be the 2.6x fewer probes a txid key needs. A miss
//! returns before the coin record is read, so it never consults headers and
//! needs no chain.
//!
//! Usage: index_ab <snapshot.dat> <outpoint-index> <txid-index> [probes]
//!
//! Rates are human-facing statistics.
#![allow(clippy::cast_precision_loss)]

use std::{env, path::PathBuf, time::Instant};

use bitcoin::{Network, OutPoint, Txid, hashes::Hash as _};
use rbtc::{
    core_snapshot::CoreSnapshotIndex, core_snapshot_index::CoreSnapshotUtxoIndex,
    headers::HeaderDag, utxo::OutPointKey,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = env::args().skip(1);
    let snapshot = PathBuf::from(args.next().ok_or("usage: see header")?);
    let outpoint_index = PathBuf::from(args.next().ok_or("missing outpoint index")?);
    let txid_index = PathBuf::from(args.next().ok_or("missing txid index")?);
    let probes: usize = args.next().map_or(Ok(200_000), |value| value.parse())?;

    // Absent txids: a duplicate check almost always asks about a transaction
    // the base has never seen, so this is the realistic query.
    let mut state = 0x9E37_79B9_7F4A_7C15_u64;
    let queries: Vec<OutPoint> = (0..probes)
        .map(|_| {
            let mut bytes = [0_u8; 32];
            for chunk in bytes.chunks_mut(8) {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                chunk.copy_from_slice(&state.to_le_bytes()[..chunk.len()]);
            }
            OutPoint::new(Txid::from_byte_array(bytes), 0)
        })
        .collect();

    let by_outpoint = CoreSnapshotUtxoIndex::open(&outpoint_index, &snapshot)?;
    let mut by_txid = CoreSnapshotIndex::open(&snapshot, &txid_index, Network::Bitcoin)?;
    let headers = HeaderDag::new(Network::Bitcoin);
    println!("txid groups indexed: {}", by_txid.group_count());

    // Alternate the two in three passes so any drift within the run is shared.
    for pass in 1..=3 {
        let started = Instant::now();
        let mut hits = 0_u64;
        for outpoint in &queries {
            if by_outpoint.get(outpoint)?.is_some() {
                hits += 1;
            }
        }
        let outpoint_secs = started.elapsed().as_secs_f64();

        let started = Instant::now();
        let mut txid_hits = 0_u64;
        for outpoint in &queries {
            if by_txid
                .get(OutPointKey::from(*outpoint), &headers, 0)?
                .is_some()
            {
                txid_hits += 1;
            }
        }
        let txid_secs = started.elapsed().as_secs_f64();

        println!(
            "pass {pass}: outpoint-keyed {:.0}/s (hits {hits})   txid-keyed {:.0}/s (hits {txid_hits})   ratio {:.2}x",
            probes as f64 / outpoint_secs,
            probes as f64 / txid_secs,
            outpoint_secs / txid_secs
        );
    }
    Ok(())
}
