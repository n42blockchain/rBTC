//! Verifies snapshot-index lookups against real outpoints and reports memory.
//!
//! A diagnostic tool. It walks the snapshot's own txid groups to obtain
//! outpoints that are definitely in the set, looks each one up through the
//! ordinary `get` path, and reports throughput plus the process's peak
//! working set — the number that shows whether the offset table is resident.
//!
//! Usage: snapshot_index_probe <snapshot.dat> <index.rbtcidx> [lookups] [batched]
//!
//! The rates and sizes it prints are human-facing statistics, not inputs to
//! any decision, so the lossy numeric conversions behind them are allowed.
#![allow(clippy::cast_precision_loss, clippy::cast_possible_truncation)]

use std::{
    env,
    fs::File,
    io::{BufReader, Read},
    path::PathBuf,
    time::Instant,
};

use bitcoin::{OutPoint, Txid, hashes::Hash as _};
use rbtc::core_snapshot_index::CoreSnapshotUtxoIndex;

const METADATA_BYTES: usize = 5 + 2 + 4 + 32 + 8;
const MAX_SCRIPT_BYTES: u64 = 10_000;
/// Inputs resolved per batched call, near a full block's spend count.
const BATCH: usize = 4_096;

fn read_compact_size(reader: &mut impl Read) -> std::io::Result<u64> {
    let mut first = [0_u8; 1];
    reader.read_exact(&mut first)?;
    Ok(match first[0] {
        0xFD => {
            let mut buf = [0_u8; 2];
            reader.read_exact(&mut buf)?;
            u64::from(u16::from_le_bytes(buf))
        }
        0xFE => {
            let mut buf = [0_u8; 4];
            reader.read_exact(&mut buf)?;
            u64::from(u32::from_le_bytes(buf))
        }
        0xFF => {
            let mut buf = [0_u8; 8];
            reader.read_exact(&mut buf)?;
            u64::from_le_bytes(buf)
        }
        small => u64::from(small),
    })
}

fn read_varint(reader: &mut impl Read) -> std::io::Result<u64> {
    let mut value = 0_u64;
    loop {
        let mut byte = [0_u8; 1];
        reader.read_exact(&mut byte)?;
        value = (value << 7) | u64::from(byte[0] & 0x7F);
        if byte[0] & 0x80 == 0 {
            return Ok(value);
        }
        value += 1;
    }
}

/// Peak working set in bytes, or `None` where the platform is not wired up.
#[cfg(windows)]
fn peak_memory_bytes() -> Option<u64> {
    // The PID must be this process's, passed in explicitly. PowerShell's own
    // `$PID` refers to the PowerShell process, which would silently report
    // that interpreter's memory instead of ours.
    let output = std::process::Command::new("powershell")
        .args([
            "-NoProfile",
            "-Command",
            &format!("(Get-Process -Id {}).PeakWorkingSet64", std::process::id()),
        ])
        .output()
        .ok()?;
    String::from_utf8(output.stdout).ok()?.trim().parse().ok()
}

#[cfg(not(windows))]
fn peak_memory_bytes() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    let line = status.lines().find(|line| line.starts_with("VmHWM:"))?;
    let kilobytes: u64 = line.split_whitespace().nth(1)?.parse().ok()?;
    Some(kilobytes * 1024)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = env::args().skip(1);
    let snapshot = PathBuf::from(args.next().ok_or("usage: <snapshot> <index> [lookups]")?);
    let index_path = PathBuf::from(args.next().ok_or("usage: <snapshot> <index> [lookups]")?);
    let wanted: usize = args.next().map_or(Ok(200_000), |value| value.parse())?;
    let batched = args.next().is_some_and(|value| value == "batched");

    // Collect outpoints first, so the walk's own buffers are released before
    // the index is opened and the reported peak reflects the index alone.
    println!("collecting {wanted} outpoints from the snapshot...");
    let mut outpoints: Vec<OutPoint> = Vec::with_capacity(wanted);
    {
        let mut reader = BufReader::with_capacity(1 << 20, File::open(&snapshot)?);
        let mut metadata = [0_u8; METADATA_BYTES];
        reader.read_exact(&mut metadata)?;
        while outpoints.len() < wanted {
            let mut txid_bytes = [0_u8; 32];
            reader.read_exact(&mut txid_bytes)?;
            let txid = Txid::from_byte_array(txid_bytes);
            let coins = read_compact_size(&mut reader)?;
            for _ in 0..coins {
                let vout = u32::try_from(read_compact_size(&mut reader)?)?;
                let _code = read_varint(&mut reader)?;
                let _amount = read_varint(&mut reader)?;
                let script_kind = read_varint(&mut reader)?;
                let script_len = match script_kind {
                    0 | 1 => 20,
                    2..=5 => 32,
                    other => other - 6,
                };
                if script_len > MAX_SCRIPT_BYTES {
                    return Err("script length out of range".into());
                }
                let mut script = vec![0_u8; usize::try_from(script_len)?];
                reader.read_exact(&mut script)?;
                if outpoints.len() < wanted {
                    outpoints.push(OutPoint { txid, vout });
                }
            }
        }
    }

    let opened_at = Instant::now();
    let index = CoreSnapshotUtxoIndex::open(&index_path, &snapshot)?;
    let open_seconds = opened_at.elapsed().as_secs_f64();
    println!(
        "opened index in {open_seconds:.2}s: {} coins, base height {}",
        index.coin_count(),
        index.base_height()
    );

    // Shuffle so the queries do not arrive in snapshot file order, which is
    // the realistic case: a block's inputs bear no relation to where their
    // coins sit in the base. Deterministic, so runs stay comparable.
    let mut state = 0x2545_F491_4F6C_DD1D_u64;
    for position in (1..outpoints.len()).rev() {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        outpoints.swap(position, usize::try_from(state % (position as u64 + 1))?);
    }

    let started = Instant::now();
    let mut hits = 0_u64;
    if batched {
        for chunk in outpoints.chunks(BATCH) {
            hits += index
                .get_many(chunk)?
                .into_iter()
                .filter(Option::is_some)
                .count() as u64;
        }
    } else {
        for outpoint in &outpoints {
            if index.get(outpoint)?.is_some() {
                hits += 1;
            }
        }
    }
    let elapsed = started.elapsed().as_secs_f64();
    println!("mode: {}", if batched { "batched" } else { "single" });

    println!(
        "lookups={} hits={hits} misses={} elapsed={elapsed:.2}s rate={:.0}/s",
        outpoints.len(),
        u64::try_from(outpoints.len())? - hits,
        outpoints.len() as f64 / elapsed
    );
    match peak_memory_bytes() {
        Some(bytes) => println!(
            "peak working set: {bytes} bytes ({:.2} GiB)",
            bytes as f64 / (1024.0 * 1024.0 * 1024.0)
        ),
        None => println!("peak working set: unavailable on this platform"),
    }
    if usize::try_from(hits)? != outpoints.len() {
        return Err("every sampled outpoint is in the snapshot and must be found".into());
    }
    Ok(())
}
