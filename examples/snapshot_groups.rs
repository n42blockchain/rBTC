//! Counts txid groups in a Core snapshot and their coin fan-out.
//!
//! An index keyed by txid probes once per transaction instead of once per
//! output, but a hit then has to scan the group for the right vout. That scan
//! costs the group's size, so the trade between the two keyings is decided by
//! two numbers: how many outputs a block creates per transaction, and how many
//! unspent coins the snapshot holds per txid. This measures the second.
//!
//! Usage: snapshot_groups <snapshot.dat>

use std::{
    env,
    fs::File,
    io::{BufReader, Read},
    path::PathBuf,
};

const METADATA_BYTES: usize = 5 + 2 + 4 + 32 + 8;
const MAX_SCRIPT_BYTES: u64 = 10_000;

fn compact_size(reader: &mut impl Read) -> std::io::Result<u64> {
    let mut first = [0_u8; 1];
    reader.read_exact(&mut first)?;
    Ok(match first[0] {
        0xFD => {
            let mut b = [0_u8; 2];
            reader.read_exact(&mut b)?;
            u64::from(u16::from_le_bytes(b))
        }
        0xFE => {
            let mut b = [0_u8; 4];
            reader.read_exact(&mut b)?;
            u64::from(u32::from_le_bytes(b))
        }
        0xFF => {
            let mut b = [0_u8; 8];
            reader.read_exact(&mut b)?;
            u64::from_le_bytes(b)
        }
        small => u64::from(small),
    })
}

fn varint(reader: &mut impl Read) -> std::io::Result<u64> {
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

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = PathBuf::from(env::args().nth(1).ok_or("usage: <snapshot.dat>")?);
    let mut reader = BufReader::with_capacity(4 << 20, File::open(&path)?);
    let mut metadata = [0_u8; METADATA_BYTES];
    reader.read_exact(&mut metadata)?;
    let total: u64 = u64::from_le_bytes(metadata[43..51].try_into()?);

    let mut groups = 0_u64;
    let mut coins = 0_u64;
    let mut histogram = [0_u64; 9];
    while coins < total {
        let mut txid = [0_u8; 32];
        reader.read_exact(&mut txid)?;
        let count = compact_size(&mut reader)?;
        groups += 1;
        histogram[(count.min(8) as usize).saturating_sub(1).min(8)] += 1;
        for _ in 0..count {
            let _vout = compact_size(&mut reader)?;
            let _code = varint(&mut reader)?;
            let _amount = varint(&mut reader)?;
            let kind = varint(&mut reader)?;
            let len = match kind {
                0 | 1 => 20,
                2..=5 => 32,
                other => other - 6,
            };
            if len > MAX_SCRIPT_BYTES {
                return Err("script length out of range".into());
            }
            let mut script = vec![0_u8; usize::try_from(len)?];
            reader.read_exact(&mut script)?;
        }
        coins += count;
    }
    println!("coins={coins} txid_groups={groups}");
    #[allow(clippy::cast_precision_loss)]
    let mean = coins as f64 / groups as f64;
    println!("unspent coins per txid group (in-group scan cost): {mean:.3}");
    for (index, count) in histogram.iter().enumerate() {
        #[allow(clippy::cast_precision_loss)]
        let share = *count as f64 * 100.0 / groups as f64;
        let label = if index == 8 {
            "9+".to_owned()
        } else {
            (index + 1).to_string()
        };
        println!("  groups with {label:>2} coin(s): {count:>12} ({share:5.2}%)");
    }
    Ok(())
}
