//! Measures how many outputs a block's transactions create per distinct txid.
//!
//! The commit's duplicate check probes the base once per created output. An
//! index keyed by txid rather than by outpoint would probe once per
//! transaction instead, so the achievable reduction is exactly this ratio —
//! and it is worth measuring on the real workload rather than assuming
//! Bitcoin's long-run average.
//!
//! Usage: txid_fanout <ledger-dir> <first-height> <blocks>
//!
//! The ratios it prints are human-facing statistics, so the lossy numeric
//! conversions behind them are allowed.
#![allow(clippy::cast_precision_loss)]

use std::{env, path::PathBuf};

use bitcoin::{Block, consensus::deserialize};
use rbtc::ledger::PrunedBlockLedger;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = env::args().skip(1);
    let dir = PathBuf::from(args.next().ok_or("usage: <ledger> <first> <blocks>")?);
    let first: u32 = args.next().ok_or("missing first height")?.parse()?;
    let wanted: u32 = args.next().ok_or("missing block count")?.parse()?;

    let ledger = PrunedBlockLedger::open_persisted(&dir)?;
    let mut outputs = 0_u64;
    let mut transactions = 0_u64;
    let mut coinbase_outputs = 0_u64;
    let mut height = first;
    let mut done = 0_u32;
    while done < wanted {
        let batch = ledger.read_block_batch(height, (wanted - done).min(256), 512 * 1024 * 1024)?;
        if batch.blocks.is_empty() {
            break;
        }
        for bytes in &batch.blocks {
            let block: Block = deserialize(bytes)?;
            for (index, transaction) in block.txdata.iter().enumerate() {
                transactions += 1;
                let count = transaction.output.len() as u64;
                outputs += count;
                if index == 0 {
                    coinbase_outputs += count;
                }
            }
            height += 1;
            done += 1;
        }
    }
    println!("blocks={done} transactions={transactions} outputs={outputs}");
    println!(
        "outputs per transaction (probe reduction factor): {:.3}",
        outputs as f64 / transactions as f64
    );
    println!(
        "coinbase outputs: {coinbase_outputs} ({:.2}% of all outputs)",
        coinbase_outputs as f64 * 100.0 / outputs as f64
    );
    println!(
        "probes per 1000 outputs: outpoint-keyed 1000, txid-keyed {:.0}",
        1000.0 * transactions as f64 / outputs as f64
    );
    Ok(())
}
