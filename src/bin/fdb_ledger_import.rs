//! Imports main-chain blocks from a btcd flat block file set (`*.fdb`) into a
//! [`PrunedBlockLedger`], so `rbtcd --snapshot-overlay-replay-blocks` can
//! replay a recorded mainnet corpus without touching the network.
//!
//! The source directory is only read. Records are framed as btcd writes them:
//! 4-byte network magic, 4-byte little-endian block length, the block, and a
//! big-endian CRC-32C over the preceding bytes. The files also hold stale and
//! side-chain blocks, so the chain is selected by linking `prev_blockhash`
//! from the supplied base hash forward, preferring the child with the longer
//! continuation whenever a height has more than one candidate.
//!
//! Usage:
//!   fdb_ledger_import --src DIR --out LEDGER_DIR --base-hash HEX --base-height N
//!                     [--max-height N] [--segment-blocks 32] [--slots 4096]
//!                     [--summary FILE]

use std::{
    collections::HashMap,
    env,
    fs::{self, File},
    io::{self, BufReader, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    process,
    time::Instant,
};

use bitcoin::{BlockHash, block::Header, consensus::Decodable};
use rbtc::ledger::{LedgerRetention, PrunedBlockLedger};

/// Bitcoin mainnet message start as btcd stores it (little-endian `0xD9B4BEF9`).
const MAINNET_MAGIC: u32 = 0xD9B4_BEF9;
/// Upper bound on a serialized block, matching `wire.MaxBlockPayload`.
const MAX_BLOCK_BYTES: u32 = 4_000_000;
/// How far a fork candidate is followed when siblings compete for a height.
const FORK_LOOKAHEAD: usize = 200;
/// Decompressed-byte budget handed to each ledger verification call.
const VERIFY_BUDGET_BYTES: u64 = 1 << 40;
/// Blocks per verification call; the ledger rejects ranges above 4,096.
const VERIFY_CHUNK_BLOCKS: u32 = 4_096;

struct Record {
    file: usize,
    offset: u64,
    len: u32,
    hash: BlockHash,
    prev: BlockHash,
}

struct Options {
    src: PathBuf,
    out: PathBuf,
    base_hash: BlockHash,
    base_height: u32,
    max_height: Option<u32>,
    segment_blocks: u32,
    slots: u16,
    summary: Option<PathBuf>,
    verify_existing: bool,
}

fn usage() -> ! {
    eprintln!(
        "usage: fdb_ledger_import --src DIR --out LEDGER_DIR --base-hash HEX --base-height N \
         [--max-height N] [--segment-blocks 32] [--slots 4096] [--summary FILE] \n         [--verify-existing]"
    );
    process::exit(2);
}

fn parse_options() -> Options {
    let mut src = None;
    let mut out = None;
    let mut base_hash = None;
    let mut base_height = None;
    let mut max_height = None;
    let mut segment_blocks = 32u32;
    let mut slots = 4096u16;
    let mut summary = None;
    let mut verify_existing = false;

    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        let mut value = || args.next().unwrap_or_else(|| usage());
        match arg.as_str() {
            "--src" => src = Some(PathBuf::from(value())),
            "--out" => out = Some(PathBuf::from(value())),
            "--base-hash" => {
                base_hash = Some(value().parse::<BlockHash>().unwrap_or_else(|error| {
                    eprintln!("invalid --base-hash: {error}");
                    process::exit(2);
                }));
            }
            "--base-height" => base_height = Some(parse_u32(&value(), "--base-height")),
            "--max-height" => max_height = Some(parse_u32(&value(), "--max-height")),
            "--segment-blocks" => segment_blocks = parse_u32(&value(), "--segment-blocks"),
            "--slots" => {
                slots = u16::try_from(parse_u32(&value(), "--slots")).unwrap_or_else(|_| {
                    eprintln!("--slots must fit in u16");
                    process::exit(2);
                });
            }
            "--summary" => summary = Some(PathBuf::from(value())),
            "--verify-existing" => verify_existing = true,
            _ => usage(),
        }
    }

    let (Some(src), Some(out), Some(base_hash), Some(base_height)) =
        (src, out, base_hash, base_height)
    else {
        usage();
    };
    if segment_blocks == 0 || slots == 0 {
        usage();
    }

    Options {
        src,
        out,
        base_hash,
        base_height,
        max_height,
        segment_blocks,
        slots,
        summary,
        verify_existing,
    }
}

fn parse_u32(value: &str, flag: &str) -> u32 {
    value.parse().unwrap_or_else(|error| {
        eprintln!("invalid {flag}: {error}");
        process::exit(2);
    })
}

fn main() {
    let options = parse_options();
    let outcome = if options.verify_existing {
        verify_existing(&options)
    } else {
        run(&options)
    };
    if let Err(error) = outcome {
        eprintln!("fdb_ledger_import: {error}");
        process::exit(1);
    }
}

fn block_files(dir: &Path) -> io::Result<Vec<PathBuf>> {
    let mut paths: Vec<PathBuf> = fs::read_dir(dir)?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "fdb"))
        .collect();
    paths.sort();
    if paths.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!("no .fdb files in {}", dir.display()),
        ));
    }
    Ok(paths)
}

/// Reads only each record's frame and 80-byte header, seeking past the body.
fn scan(paths: &[PathBuf]) -> io::Result<Vec<Record>> {
    let mut records = Vec::new();
    for (file_index, path) in paths.iter().enumerate() {
        let mut reader = BufReader::with_capacity(1 << 20, File::open(path)?);
        let mut offset = 0u64;
        loop {
            let mut frame = [0u8; 8];
            match reader.read_exact(&mut frame) {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(error) => return Err(error),
            }
            let magic = u32::from_le_bytes(frame[0..4].try_into().expect("4 bytes"));
            if magic == 0 {
                // Zero padding marks the end of the written data.
                break;
            }
            if magic != MAINNET_MAGIC {
                return Err(io::Error::other(format!(
                    "{}: network magic {magic:08x} at offset {offset}",
                    path.display()
                )));
            }
            let len = u32::from_le_bytes(frame[4..8].try_into().expect("4 bytes"));
            if !(80..=MAX_BLOCK_BYTES).contains(&len) {
                return Err(io::Error::other(format!(
                    "{}: block length {len} at offset {offset}",
                    path.display()
                )));
            }
            let mut header_bytes = [0u8; 80];
            reader.read_exact(&mut header_bytes)?;
            let header = Header::consensus_decode(&mut &header_bytes[..])
                .map_err(|error| io::Error::other(format!("{}: {error}", path.display())))?;
            records.push(Record {
                file: file_index,
                offset,
                len,
                hash: header.block_hash(),
                prev: header.prev_blockhash,
            });
            // Skip the rest of the block and the 4-byte checksum.
            reader.seek_relative(i64::from(len - 80) + 4)?;
            offset += 8 + u64::from(len) + 4;
        }
        if file_index % 100 == 0 || file_index + 1 == paths.len() {
            eprintln!(
                "scanned {}/{} files, {} records",
                file_index + 1,
                paths.len(),
                records.len()
            );
        }
    }
    Ok(records)
}

/// Length of the longest continuation reachable from `start`, bounded.
fn continuation(
    children: &HashMap<BlockHash, Vec<usize>>,
    records: &[Record],
    start: usize,
    limit: usize,
) -> usize {
    if limit == 0 {
        return 0;
    }
    let Some(kids) = children.get(&records[start].hash) else {
        return 1;
    };
    1 + kids
        .iter()
        .map(|&kid| continuation(children, records, kid, limit - 1))
        .max()
        .unwrap_or(0)
}

/// Selects the chain of record indices that follows `base_hash`.
fn select_chain(records: &[Record], base_hash: BlockHash, max_blocks: Option<u64>) -> Vec<usize> {
    let mut children: HashMap<BlockHash, Vec<usize>> = HashMap::new();
    for (index, record) in records.iter().enumerate() {
        children.entry(record.prev).or_default().push(index);
    }

    let mut chain = Vec::new();
    let mut current = base_hash;
    let mut forks = 0usize;
    while let Some(kids) = children.get(&current) {
        if let Some(max) = max_blocks {
            if u64::try_from(chain.len()).unwrap_or(u64::MAX) >= max {
                break;
            }
        }
        let chosen = if kids.len() == 1 {
            kids[0]
        } else {
            forks += 1;
            *kids
                .iter()
                .max_by_key(|&&kid| continuation(&children, records, kid, FORK_LOOKAHEAD))
                .expect("non-empty")
        };
        chain.push(chosen);
        current = records[chosen].hash;
    }
    eprintln!(
        "selected {} blocks after base, {forks} forks resolved",
        chain.len()
    );
    chain
}

/// Bytes as GiB, for progress output only.
#[allow(clippy::cast_precision_loss)]
fn gib(bytes: u64) -> f64 {
    bytes as f64 / (1u64 << 30) as f64
}

/// CRC-32C (Castagnoli), as btcd's block files use.
fn crc32c(data_parts: &[&[u8]]) -> u32 {
    static TABLE: std::sync::OnceLock<[u32; 256]> = std::sync::OnceLock::new();
    let table = TABLE.get_or_init(|| {
        let mut table = [0u32; 256];
        for (i, entry) in table.iter_mut().enumerate() {
            let mut crc = u32::try_from(i).expect("table index fits u32");
            for _ in 0..8 {
                crc = if crc & 1 == 1 {
                    (crc >> 1) ^ 0x82F6_3B78
                } else {
                    crc >> 1
                };
            }
            *entry = crc;
        }
        table
    });
    let mut crc = !0u32;
    for part in data_parts {
        for &byte in *part {
            crc = table[((crc ^ u32::from(byte)) & 0xff) as usize] ^ (crc >> 8);
        }
    }
    !crc
}

fn read_block(
    files: &mut [Option<File>],
    paths: &[PathBuf],
    record: &Record,
) -> io::Result<Vec<u8>> {
    let file = match &mut files[record.file] {
        Some(file) => file,
        slot @ None => slot.insert(File::open(&paths[record.file])?),
    };
    file.seek(SeekFrom::Start(record.offset))?;
    let mut frame = [0u8; 8];
    file.read_exact(&mut frame)?;
    let mut block = vec![0u8; record.len as usize];
    file.read_exact(&mut block)?;
    let mut stored = [0u8; 4];
    file.read_exact(&mut stored)?;
    let expected = u32::from_be_bytes(stored);
    let actual = crc32c(&[&frame, &block]);
    if actual != expected {
        return Err(io::Error::other(format!(
            "{}: checksum mismatch at offset {} (got {actual:08x}, want {expected:08x})",
            paths[record.file].display(),
            record.offset
        )));
    }
    let header = Header::consensus_decode(&mut &block[..80]).map_err(io::Error::other)?;
    if header.block_hash() != record.hash {
        return Err(io::Error::other("block hash changed between scan and read"));
    }
    Ok(block)
}

/// Verifies `[first, last]` in ranges the ledger accepts (at most 4,096 blocks
/// per call), returning one report per range.
fn verify_chunked(
    ledger: &PrunedBlockLedger,
    first: u32,
    last: u32,
) -> Result<Vec<rbtc::ledger::LedgerBlockHashReport>, Box<dyn std::error::Error>> {
    let mut reports = Vec::new();
    let mut start = first;
    while start <= last {
        let end = start.saturating_add(VERIFY_CHUNK_BLOCKS - 1).min(last);
        reports.push(ledger.verify_block_hashes(start, end, VERIFY_BUDGET_BYTES)?);
        eprintln!("verified {start}..={end}");
        if end == u32::MAX {
            break;
        }
        start = end + 1;
    }
    Ok(reports)
}

/// Renders verification reports without their per-block hash lists.
fn render_reports(reports: &[rbtc::ledger::LedgerBlockHashReport]) -> String {
    let mut out = String::from("[");
    for (i, report) in reports.iter().enumerate() {
        if i > 0 {
            out.push_str(", ");
        }
        let first = report
            .hashes
            .first()
            .map_or_else(|| "null".to_owned(), |(h, hash)| format!("\"{h}:{hash}\""));
        let last = report
            .hashes
            .last()
            .map_or_else(|| "null".to_owned(), |(h, hash)| format!("\"{h}:{hash}\""));
        out.push_str(&format!(
            "{{\"first_height\": {}, \"last_height\": {}, \"blocks\": {}, \"verified_record_bytes\": {}, \"first\": {}, \"last\": {}}}",
            report.first_height,
            report.last_height,
            report.hashes.len(),
            report.verified_record_bytes,
            first,
            last
        ));
    }
    out.push(']');
    out
}

fn verify_existing(options: &Options) -> Result<(), Box<dyn std::error::Error>> {
    let started = Instant::now();
    let ledger = PrunedBlockLedger::open_persisted(&options.out)?;
    let stats = ledger.stats()?;
    let first = stats.first_height.ok_or("ledger retains no blocks")?;
    let last = first
        .checked_add(stats.blocks)
        .and_then(|end| end.checked_sub(1))
        .ok_or("height overflow")?;
    let reports = verify_chunked(&ledger, first, last)?;
    let summary = format!(
        "{{
  \"ledger\": {:?},
  \"first_height\": {first},
  \"last_height\": {last},
           \"ledger_segments\": {},
  \"ledger_blocks\": {},
  \"ledger_compressed_bytes\": {},
           \"verify_seconds\": {:.3},
  \"verify_reports\": {}
}}
",
        options.out.display().to_string(),
        stats.segments,
        stats.blocks,
        stats.bytes,
        started.elapsed().as_secs_f64(),
        render_reports(&reports),
    );
    print!("{summary}");
    if let Some(path) = &options.summary {
        File::create(path)?.write_all(summary.as_bytes())?;
    }
    Ok(())
}

fn run(options: &Options) -> Result<(), Box<dyn std::error::Error>> {
    let started = Instant::now();
    let paths = block_files(&options.src)?;
    eprintln!("{} block files in {}", paths.len(), options.src.display());

    let scan_started = Instant::now();
    let records = scan(&paths)?;
    let scan_seconds = scan_started.elapsed().as_secs_f64();
    eprintln!("scan: {} records in {scan_seconds:.1} s", records.len());

    let max_blocks = options
        .max_height
        .map(|max| u64::from(max.saturating_sub(options.base_height)));
    let chain = select_chain(&records, options.base_hash, max_blocks);
    if chain.is_empty() {
        return Err("no block in the corpus builds on the base hash".into());
    }
    let total = u32::try_from(chain.len())?;
    let first_height = options
        .base_height
        .checked_add(1)
        .ok_or("height overflow")?;
    let last_height = options
        .base_height
        .checked_add(total)
        .ok_or("height overflow")?;
    let segments_needed = chain.len().div_ceil(options.segment_blocks as usize);
    if segments_needed > usize::from(options.slots) {
        return Err(format!(
            "{segments_needed} segments of {} blocks exceed {} slots; raise --slots or --segment-blocks",
            options.segment_blocks, options.slots
        )
        .into());
    }

    if options.out.exists() {
        return Err(format!("output {} already exists", options.out.display()).into());
    }
    let ledger = PrunedBlockLedger::open(
        &options.out,
        LedgerRetention {
            max_blocks: total.checked_add(1).ok_or("retention overflow")?,
            max_bytes: 1 << 44,
            slots: options.slots,
        },
    )?;

    let import_started = Instant::now();
    let mut files: Vec<Option<File>> = (0..paths.len()).map(|_| None).collect();
    let mut raw_bytes = 0u64;
    let mut height = first_height;
    let mut done = 0usize;
    for segment in chain.chunks(options.segment_blocks as usize) {
        let mut blocks = Vec::with_capacity(segment.len());
        for &index in segment {
            let block = read_block(&mut files, &paths, &records[index])?;
            raw_bytes += block.len() as u64;
            blocks.push(block);
        }
        ledger.append(height, &blocks)?;
        height += u32::try_from(segment.len())?;
        done += segment.len();
        if done % (options.segment_blocks as usize * 64) == 0 || done == chain.len() {
            eprintln!(
                "appended {done}/{} blocks, {:.2} GiB raw, {:.0} s",
                chain.len(),
                gib(raw_bytes),
                import_started.elapsed().as_secs_f64()
            );
        }
    }
    let import_seconds = import_started.elapsed().as_secs_f64();

    let verify_started = Instant::now();
    let reports = verify_chunked(&ledger, first_height, last_height)?;
    let verify_seconds = verify_started.elapsed().as_secs_f64();
    let stats = ledger.stats()?;

    let last_hash = records[*chain.last().expect("non-empty")].hash;
    let summary = format!(
        "{{\n  \"source\": {:?},\n  \"ledger\": {:?},\n  \"source_files\": {},\n  \"source_records\": {},\n  \
         \"base_height\": {},\n  \"base_hash\": \"{}\",\n  \"first_height\": {first_height},\n  \"last_height\": {last_height},\n  \
         \"last_hash\": \"{last_hash}\",\n  \"blocks\": {total},\n  \"raw_bytes\": {raw_bytes},\n  \"ledger_segments\": {},\n  \
         \"ledger_blocks\": {},\n  \"ledger_compressed_bytes\": {},\n  \"segment_blocks\": {},\n  \"slots\": {},\n  \
         \"scan_seconds\": {scan_seconds:.3},\n  \"import_seconds\": {import_seconds:.3},\n  \"verify_seconds\": {verify_seconds:.3},\n  \
         \"total_seconds\": {:.3},\n  \"verify_report\": {:?}\n}}\n",
        options.src.display().to_string(),
        options.out.display().to_string(),
        paths.len(),
        records.len(),
        options.base_height,
        options.base_hash,
        stats.segments,
        stats.blocks,
        stats.bytes,
        options.segment_blocks,
        options.slots,
        started.elapsed().as_secs_f64(),
        render_reports(&reports),
    );
    print!("{summary}");
    if let Some(path) = &options.summary {
        let mut file = File::create(path)?;
        file.write_all(summary.as_bytes())?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bitcoin::{Block, Network, consensus::serialize, hashes::Hash};

    fn child(parent: BlockHash, height: u32) -> Block {
        rbtc::block_assembly::assemble_block(&rbtc::block_assembly::BlockTemplate::regtest(
            parent,
            height,
            1_300_000_000 + height,
        ))
        .unwrap()
    }

    fn framed(block: &Block) -> Vec<u8> {
        let raw = serialize(block);
        let mut bytes = MAINNET_MAGIC.to_le_bytes().to_vec();
        bytes.extend_from_slice(&u32::try_from(raw.len()).unwrap().to_le_bytes());
        bytes.extend_from_slice(&raw);
        let checksum = crc32c(&[&bytes]);
        bytes.extend_from_slice(&checksum.to_be_bytes());
        bytes
    }

    #[test]
    fn imports_longest_continuation_and_verifies_the_retained_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("source");
        fs::create_dir(&src).unwrap();
        let base = bitcoin::blockdata::constants::genesis_block(Network::Regtest).block_hash();
        let first = child(base, 1);
        let second = child(first.block_hash(), 2);
        let third = child(second.block_hash(), 3);
        let stale = child(base, 11);
        let mut bytes = framed(&stale);
        bytes.extend(framed(&first));
        bytes.extend(framed(&second));
        fs::write(src.join("00000.fdb"), &bytes).unwrap();
        fs::write(src.join("00001.fdb"), framed(&third)).unwrap();
        fs::write(src.join("ignored.txt"), b"not a block file").unwrap();
        let mut options = Options {
            src: src.clone(),
            out: dir.path().join("ledger"),
            base_hash: base,
            base_height: 0,
            max_height: None,
            segment_blocks: 2,
            slots: 4,
            summary: Some(dir.path().join("summary.json")),
            verify_existing: false,
        };
        run(&options).unwrap();
        let report: serde_json::Value =
            serde_json::from_slice(&fs::read(options.summary.as_ref().unwrap()).unwrap()).unwrap();
        assert_eq!(report["blocks"], 3);
        assert_eq!(report["source_records"], 4);
        assert_eq!(report["last_hash"], third.block_hash().to_string());
        let ledger = PrunedBlockLedger::open_persisted(&options.out).unwrap();
        assert_eq!(
            ledger
                .read_block_batch(1, 3, VERIFY_BUDGET_BYTES)
                .unwrap()
                .blocks,
            [serialize(&first), serialize(&second), serialize(&third)]
        );
        verify_existing(&options).unwrap();
        let report: serde_json::Value =
            serde_json::from_slice(&fs::read(options.summary.as_ref().unwrap()).unwrap()).unwrap();
        assert_eq!(report["verify_reports"][0]["blocks"], 3);
        assert!(
            run(&options)
                .unwrap_err()
                .to_string()
                .contains("already exists")
        );
        assert_eq!(fs::read(src.join("00000.fdb")).unwrap(), bytes);
        options.out = dir.path().join("bounded");
        options.max_height = Some(2);
        run(&options).unwrap();
        assert_eq!(
            PrunedBlockLedger::open_persisted(&options.out)
                .unwrap()
                .stats()
                .unwrap()
                .blocks,
            2
        );
        options.out = dir.path().join("too-small");
        options.max_height = None;
        options.slots = 1;
        assert!(
            run(&options)
                .unwrap_err()
                .to_string()
                .contains("exceed 1 slots")
        );
        assert!(!options.out.exists());
        options.base_hash = BlockHash::all_zeros();
        assert!(run(&options).unwrap_err().to_string().contains("no block"));
    }

    #[test]
    fn rejects_corrupt_frames_checksums_and_changed_headers() {
        assert_eq!(crc32c(&[b"123", b"456789"]), 0xe306_9283);
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(
            block_files(dir.path()).unwrap_err().kind(),
            io::ErrorKind::NotFound
        );
        let path = dir.path().join("00000.fdb");
        let block = child(BlockHash::all_zeros(), 1);
        let valid = framed(&block);
        let paths = vec![path.clone()];
        for (offset, value) in [(0, 1_u32), (4, 79), (4, MAX_BLOCK_BYTES + 1)] {
            let mut bytes = valid.clone();
            bytes[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
            fs::write(&path, bytes).unwrap();
            assert!(scan(&paths).is_err());
        }
        fs::write(&path, &valid).unwrap();
        let mut records = scan(&paths).unwrap();
        let mut corrupt = valid.clone();
        *corrupt.last_mut().unwrap() ^= 1;
        fs::write(&path, corrupt).unwrap();
        assert!(
            read_block(&mut [None], &paths, &records[0])
                .unwrap_err()
                .to_string()
                .contains("checksum mismatch")
        );
        fs::write(&path, &valid).unwrap();
        records[0].hash = BlockHash::all_zeros();
        assert!(
            read_block(&mut [None], &paths, &records[0])
                .unwrap_err()
                .to_string()
                .contains("hash changed")
        );
        fs::write(&path, [0; 8]).unwrap();
        assert!(scan(&paths).unwrap().is_empty());
    }
}
