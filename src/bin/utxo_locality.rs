//! Measures how far back a block's inputs reach, over blocks held in a
//! [`PrunedBlockLedger`].  The answer decides whether a write-back UTXO cache
//! can absorb most churn before it ever reaches the durable store.
//!
//! For every input of every non-coinbase transaction the tool classifies the
//! spent output by the age of its creating transaction, measured in blocks
//! between creation and spend, using only what the ledger holds: outputs
//! created before the ledger's first block are "older than the window".
//! Outputs created and spent inside the same block are counted separately
//! because they never need any store at all.
//!
//! Usage:
//!   utxo_locality --ledger DIR [--first N] [--last N] [--summary FILE]

use std::{collections::HashMap, env, fs::File, io::Write, path::PathBuf, process, time::Instant};

use bitcoin::{Block, Txid, consensus::Decodable};
use rbtc::ledger::PrunedBlockLedger;

const BATCH_BLOCKS: u32 = 64;
const BATCH_BYTES: u64 = 1 << 30;

/// Age buckets in blocks: spent within the same block, within a 256-block
/// commit batch, within 1,008 blocks (one week), within 4,096, within 28,350
/// (the whole window), or created before the window.
const BUCKETS: [u32; 5] = [0, 256, 1_008, 4_096, u32::MAX];

struct Tally {
    seconds: f64,
    inputs: u64,
    outputs: u64,
    same_block: u64,
    by_bucket: [u64; 5],
    older_than_window: u64,
    coinbase_outputs: u64,
    op_return_outputs: u64,
}

fn usage() -> ! {
    eprintln!("usage: utxo_locality --ledger DIR [--first N] [--last N] [--summary FILE]");
    process::exit(2);
}

fn main() {
    let mut ledger = None;
    let mut first = None;
    let mut last = None;
    let mut summary = None;
    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        let mut value = || args.next().unwrap_or_else(|| usage());
        match arg.as_str() {
            "--ledger" => ledger = Some(PathBuf::from(value())),
            "--first" => first = value().parse().ok(),
            "--last" => last = value().parse().ok(),
            "--summary" => summary = Some(PathBuf::from(value())),
            _ => usage(),
        }
    }
    let Some(ledger) = ledger else { usage() };
    if let Err(error) = run(&ledger, first, last, summary.as_deref()) {
        eprintln!("utxo_locality: {error}");
        process::exit(1);
    }
}

fn run(
    ledger_path: &std::path::Path,
    first: Option<u32>,
    last: Option<u32>,
    summary: Option<&std::path::Path>,
) -> Result<(), Box<dyn std::error::Error>> {
    let started = Instant::now();
    let ledger = PrunedBlockLedger::open_persisted(ledger_path)?;
    let stats = ledger.stats()?;
    let window_first = stats.first_height.ok_or("empty ledger")?;
    let window_last = window_first + stats.blocks - 1;
    let first = first.unwrap_or(window_first).max(window_first);
    let last = last.unwrap_or(window_last).min(window_last);
    if first > last {
        return Err("empty range".into());
    }

    // Every txid created inside the window maps to its creation height.
    // ~4k transactions per block, 32-byte keys: the whole 28k-block window
    // fits comfortably in memory.
    let mut created: HashMap<Txid, u32> = HashMap::with_capacity(64 << 20);
    let mut tally = Tally {
        seconds: 0.0,
        inputs: 0,
        outputs: 0,
        same_block: 0,
        by_bucket: [0; 5],
        older_than_window: 0,
        coinbase_outputs: 0,
        op_return_outputs: 0,
    };
    let mut blocks_seen = 0u32;
    let mut height = window_first;
    while height <= last {
        let batch = ledger.read_block_batch(height, BATCH_BLOCKS, BATCH_BYTES)?;
        if batch.blocks.is_empty() {
            return Err(format!("ledger returned no blocks at {height}").into());
        }
        for raw in &batch.blocks {
            let block = Block::consensus_decode(&mut &raw[..])?;
            let counting = height >= first;
            for (index, tx) in block.txdata.iter().enumerate() {
                let txid = tx.compute_txid();
                if counting {
                    tally.outputs += tx.output.len() as u64;
                    if index == 0 {
                        tally.coinbase_outputs += tx.output.len() as u64;
                    }
                    tally.op_return_outputs += tx
                        .output
                        .iter()
                        .filter(|output| output.script_pubkey.is_op_return())
                        .count() as u64;
                    if index > 0 {
                        for input in &tx.input {
                            tally.inputs += 1;
                            match created.get(&input.previous_output.txid) {
                                Some(&creation) if creation == height => tally.same_block += 1,
                                Some(&creation) => {
                                    let age = height - creation;
                                    let bucket = BUCKETS
                                        .iter()
                                        .position(|&limit| age <= limit)
                                        .unwrap_or(BUCKETS.len() - 1);
                                    tally.by_bucket[bucket] += 1;
                                }
                                None => tally.older_than_window += 1,
                            }
                        }
                    }
                }
                created.insert(txid, height);
            }
            height += 1;
            if counting {
                blocks_seen += 1;
            }
        }
        if blocks_seen % 1024 == 0 {
            eprintln!(
                "height {height}: {} inputs, {} txids tracked, {:.0} s",
                tally.inputs,
                created.len(),
                started.elapsed().as_secs_f64()
            );
        }
    }

    tally.seconds = started.elapsed().as_secs_f64();
    let report = render(
        ledger_path,
        window_first,
        window_last,
        first,
        last,
        blocks_seen,
        &tally,
        created.len(),
    );
    print!("{report}");
    if let Some(path) = summary {
        File::create(path)?.write_all(report.as_bytes())?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn render(
    ledger_path: &std::path::Path,
    window_first: u32,
    window_last: u32,
    first: u32,
    last: u32,
    blocks_seen: u32,
    tally: &Tally,
    txids_tracked: usize,
) -> String {
    let pct = |n: u64| -> f64 {
        if tally.inputs == 0 {
            0.0
        } else {
            #[allow(clippy::cast_precision_loss)]
            {
                n as f64 * 100.0 / tally.inputs as f64
            }
        }
    };
    format!(
        "{{\n  \"ledger\": {:?},\n  \"window_first\": {window_first},\n  \"window_last\": {window_last},\n  \
         \"counted_first\": {first},\n  \"counted_last\": {last},\n  \"blocks\": {blocks_seen},\n  \
         \"outputs_created\": {},\n  \"coinbase_outputs\": {},\n  \"op_return_outputs\": {},\n  \
         \"inputs\": {},\n  \"spent_same_block\": {},\n  \"spent_same_block_pct\": {:.2},\n  \
         \"spent_within_256_blocks\": {},\n  \"spent_within_256_blocks_pct\": {:.2},\n  \
         \"spent_within_1008_blocks\": {},\n  \"spent_within_1008_blocks_pct\": {:.2},\n  \
         \"spent_within_4096_blocks\": {},\n  \"spent_within_4096_blocks_pct\": {:.2},\n  \
         \"spent_within_window\": {},\n  \"spent_within_window_pct\": {:.2},\n  \
         \"spent_older_than_window\": {},\n  \"spent_older_than_window_pct\": {:.2},\n  \
         \"txids_tracked\": {},\n  \"seconds\": {:.1}\n}}\n",
        ledger_path.display().to_string(),
        tally.outputs,
        tally.coinbase_outputs,
        tally.op_return_outputs,
        tally.inputs,
        tally.same_block,
        pct(tally.same_block),
        tally.by_bucket[1],
        pct(tally.by_bucket[1]),
        tally.by_bucket[2],
        pct(tally.by_bucket[2]),
        tally.by_bucket[3],
        pct(tally.by_bucket[3]),
        tally.by_bucket[4],
        pct(tally.by_bucket[4]),
        tally.older_than_window,
        pct(tally.older_than_window),
        txids_tracked,
        tally.seconds,
    )
}
