//! Compare one Core31 `utxo-*.dat` snapshot against MDBX hot/cold UTXO tables.
//! Can also export a static key-index + value payload layout from the snapshot.
//!
//! A diagnostic tool: the ratios it prints are for human reading, and the
//! stream comparison is a three-way merge whose branches read more clearly
//! as ordered `if` arms than as a `match` on an ordering.
#![allow(clippy::cast_precision_loss, clippy::comparison_chain)]

use std::{
    collections::VecDeque,
    env,
    fs::{self, File, OpenOptions},
    io::{self, BufReader, Read, Write},
    path::{Path, PathBuf},
};

#[cfg(feature = "mdbx")]
use bitcoin::hashes::Hash;
#[cfg(feature = "mdbx")]
use rbtc::mdbx_utxo::MdbxUtxoStore;
use rbtc::utxo::OutPointKey;
use sha2::{Digest, Sha256};

const SNAPSHOT_MAGIC: &[u8; 5] = b"utxo\xff";
const SNAPSHOT_VERSION: u16 = 2;
const METADATA_BYTES: usize = 5 + 2 + 4 + 32 + 8;
const MAX_COMPACT_SIZE: u64 = 0x0200_0000;
const MAX_SCRIPT_BYTES: u64 = 10_000;

#[derive(Debug)]
#[allow(dead_code)]
enum ToolError {
    Io(io::Error),
    Msg(&'static str),
}

impl From<io::Error> for ToolError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

type ToolResult<T> = std::result::Result<T, ToolError>;

struct ParsedSnapshotRecord {
    key: OutPointKey,
    value_hash: [u8; 32],
    value_bytes: Vec<u8>,
    height: u32,
    is_coinbase: bool,
}

struct SnapshotReader {
    reader: BufReader<File>,
    remaining: u64,
    previous_txid: Option<[u8; 32]>,
    previous_key: Option<OutPointKey>,
    pending: VecDeque<ParsedSnapshotRecord>,
    duplicate_records: u64,
}

impl SnapshotReader {
    fn new(path: PathBuf) -> ToolResult<Self> {
        let mut reader = BufReader::new(File::open(path)?);
        let mut header = [0_u8; METADATA_BYTES];
        reader.read_exact(&mut header)?;
        if &header[..5] != SNAPSHOT_MAGIC {
            return Err(ToolError::Msg("invalid snapshot magic"));
        }
        let version = u16::from_le_bytes(header[5..7].try_into().expect("fixed metadata"));
        if version != SNAPSHOT_VERSION {
            return Err(ToolError::Msg("unsupported snapshot version"));
        }
        let _network = header[7..11].to_vec();
        let _base = header[11..43].to_vec();
        let coins_count = u64::from_le_bytes(header[43..51].try_into().expect("fixed metadata"));
        if coins_count == 0 {
            return Err(ToolError::Msg("empty snapshot"));
        }
        Ok(Self {
            reader,
            remaining: coins_count,
            previous_txid: None,
            previous_key: None,
            pending: VecDeque::new(),
            duplicate_records: 0,
        })
    }

    fn next(&mut self) -> ToolResult<Option<ParsedSnapshotRecord>> {
        while let Some(row) = self.pending.pop_front() {
            if self.previous_key == Some(row.key) {
                self.duplicate_records = self.duplicate_records.saturating_add(1);
                continue;
            }
            if self.previous_key.is_some_and(|previous| row.key < previous) {
                return Err(ToolError::Msg("snapshot keys not strictly ordered"));
            }
            self.previous_key = Some(row.key);
            return Ok(Some(row));
        }

        if self.remaining == 0 {
            return Ok(None);
        }

        let mut txid = [0_u8; 32];
        self.reader.read_exact(&mut txid)?;
        if self.previous_txid.is_some_and(|previous| txid <= previous) {
            return Err(ToolError::Msg("txids must be strictly ordered"));
        }
        self.previous_txid = Some(txid);

        let group_count = read_compact_size(&mut self.reader)?;
        if group_count == 0 || group_count > self.remaining {
            return Err(ToolError::Msg("invalid group size"));
        }

        let mut group = Vec::with_capacity(
            usize::try_from(group_count).map_err(|_| ToolError::Msg("group too large"))?,
        );
        for _ in 0..group_count {
            let vout = read_compact_size(&mut self.reader)?;
            let vout = u32::try_from(vout).map_err(|_| ToolError::Msg("vout overflow"))?;
            let code = read_core_varint(&mut self.reader)?;
            let code = u32::try_from(code).map_err(|_| ToolError::Msg("height overflow"))?;
            let height = code >> 1;
            let is_coinbase = (code & 1) == 1;
            let compressed_amount = read_core_varint(&mut self.reader)?;
            let value_sats = decompress_amount(compressed_amount)
                .ok_or(ToolError::Msg("amount decode overflow"))?;
            let script = decompress_script(&mut self.reader)?;
            let key = OutPointKey::from(bitcoin::OutPoint::new(
                bitcoin::Txid::from_byte_array(txid),
                vout,
            ));

            let value_bytes = {
                let mut payload = Vec::with_capacity(8 + 4 + 1 + 8 + script.len());
                payload.extend_from_slice(&value_sats.to_le_bytes());
                payload.extend_from_slice(&height.to_le_bytes());
                payload.push(u8::from(is_coinbase));
                payload.extend_from_slice(
                    &u64::try_from(script.len()).expect("u64 fit").to_le_bytes(),
                );
                payload.extend_from_slice(&script);
                payload
            };
            let mut hasher = Sha256::new();
            hasher.update(&value_bytes);
            let value_hash = hasher.finalize().into();
            group.push(ParsedSnapshotRecord {
                key,
                value_hash,
                value_bytes,
                height,
                is_coinbase,
            });
            self.remaining = self
                .remaining
                .checked_sub(1)
                .ok_or(ToolError::Msg("count underflow"))?;
        }
        group.sort_by_key(|row| row.key);
        self.pending = group.into();
        self.next()
    }
}

struct SnapshotMdbxIter {
    store: MdbxUtxoStore,
    after: Option<OutPointKey>,
    buffer: Vec<(OutPointKey, [u8; 32])>,
    index: usize,
    page_size: usize,
}

#[cfg(feature = "mdbx")]
impl SnapshotMdbxIter {
    fn new(store: MdbxUtxoStore) -> Self {
        Self {
            store,
            after: None,
            buffer: Vec::new(),
            index: 0,
            page_size: 200_000,
        }
    }

    fn fill_next_page(&mut self) -> ToolResult<()> {
        self.buffer.clear();
        self.index = 0;
        let page = self
            .store
            .snapshot_page(self.after, self.page_size)
            .map_err(|_| ToolError::Msg("mdbx snapshot read failed"))?;
        for (key, utxo) in page {
            let value_hash = {
                let mut hasher = Sha256::new();
                hasher.update(utxo.value_sats.to_le_bytes());
                hasher.update(utxo.height.to_le_bytes());
                hasher.update([u8::from(utxo.is_coinbase)]);
                hasher.update(
                    u64::try_from(utxo.script_pubkey.len())
                        .expect("usize fits u64")
                        .to_le_bytes(),
                );
                hasher.update(&utxo.script_pubkey);
                hasher.finalize().into()
            };
            self.buffer.push((key, value_hash));
            self.after = Some(key);
        }
        if self.buffer.is_empty() {
            self.after = None;
        }
        Ok(())
    }

    fn next(&mut self) -> ToolResult<Option<(OutPointKey, [u8; 32])>> {
        if self.index >= self.buffer.len() {
            self.fill_next_page()?;
            if self.buffer.is_empty() {
                return Ok(None);
            }
        }
        let item = self.buffer[self.index];
        self.index += 1;
        Ok(Some(item))
    }
}

#[cfg(feature = "mdbx")]
fn run_compare(
    snapshot_path: PathBuf,
    mdbx_path: PathBuf,
    static_dir: Option<PathBuf>,
) -> ToolResult<()> {
    let mut snapshot = SnapshotReader::new(snapshot_path.clone())?;
    let mdbx_store =
        MdbxUtxoStore::open(mdbx_path).map_err(|_| ToolError::Msg("open mdbx failed"))?;
    let mut mdbx_iter = SnapshotMdbxIter::new(mdbx_store);
    let mut mdbx_row = mdbx_iter.next()?;

    let mut base_last: Option<OutPointKey> = None;
    let mut tip_last: Option<OutPointKey> = None;
    let mut added = 0u64;
    let mut removed = 0u64;
    let mut modified = 0u64;
    let mut unchanged = 0u64;
    let mut snapshot_rows = 0u64;
    let mut mdbx_rows = 0u64;

    let mut snapshot_row = snapshot.next()?;
    while snapshot_row.is_some() || mdbx_row.is_some() {
        match (snapshot_row.as_ref(), mdbx_row.as_ref()) {
            (Some(base), Some((tip_key, tip_hash))) => {
                if base_last == Some(base.key) {
                    return Err(ToolError::Msg("duplicate key in snapshot stream"));
                }
                if tip_last == Some(*tip_key) {
                    return Err(ToolError::Msg("duplicate key in mdbx stream"));
                }
                if base.key < *tip_key {
                    removed += 1;
                    snapshot_rows += 1;
                    base_last = Some(base.key);
                    snapshot_row = snapshot.next()?;
                } else if *tip_key < base.key {
                    added += 1;
                    mdbx_rows += 1;
                    tip_last = Some(*tip_key);
                    mdbx_row = mdbx_iter.next()?;
                } else {
                    snapshot_rows += 1;
                    mdbx_rows += 1;
                    base_last = Some(base.key);
                    tip_last = Some(*tip_key);
                    if base.value_hash == *tip_hash {
                        unchanged += 1;
                    } else {
                        modified += 1;
                    }
                    snapshot_row = snapshot.next()?;
                    mdbx_row = mdbx_iter.next()?;
                }
            }
            (Some(base), None) => {
                if base_last == Some(base.key) {
                    return Err(ToolError::Msg("duplicate key in snapshot stream"));
                }
                removed += 1;
                snapshot_rows += 1;
                base_last = Some(base.key);
                snapshot_row = snapshot.next()?;
            }
            (None, Some((tip_key, _))) => {
                if tip_last == Some(*tip_key) {
                    return Err(ToolError::Msg("duplicate key in mdbx stream"));
                }
                added += 1;
                mdbx_rows += 1;
                tip_last = Some(*tip_key);
                mdbx_row = mdbx_iter.next()?;
            }
            (None, None) => break,
        }
    }

    let changed = added + removed + modified;
    let union = added + removed + unchanged + modified;
    println!(
        "base_unique={snapshot_rows}, tip_unique={mdbx_rows}, added={added}, removed={removed}, modified={modified}, unchanged={unchanged}"
    );
    println!(
        "changed={}, union={}, changed/base={:.6}, changed/tip={:.6}, changed/union={:.6}",
        changed,
        union,
        (changed as f64) / (snapshot_rows.max(1) as f64),
        (changed as f64) / (mdbx_rows.max(1) as f64),
        (changed as f64) / (union.max(1) as f64)
    );
    if let Some(directory) = static_dir {
        export_static(snapshot_path, &directory)?;
    }
    let duplicate_records = snapshot.duplicate_records;
    if duplicate_records > 0 {
        println!("deduped_snapshot_records={duplicate_records}");
    }
    Ok(())
}

#[cfg(feature = "mdbx")]
fn export_static(snapshot_path: PathBuf, directory: &Path) -> ToolResult<()> {
    fs::create_dir_all(directory)?;
    let mut snapshot = SnapshotReader::new(snapshot_path)?;
    let mut index = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(directory.join("utxo.keys.idx"))?;
    let mut values = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(directory.join("utxo.values.bin"))?;

    let mut offset = 0u64;
    let mut total_values = 0u64;
    while let Some(row) = snapshot.next()? {
        let value_len = u64::try_from(row.value_bytes.len())
            .map_err(|_| ToolError::Msg("value payload too large"))?;
        index.write_all(row.key.as_bytes())?;
        index.write_all(&offset.to_le_bytes())?;
        index.write_all(&value_len.to_le_bytes())?;
        index.write_all(&row.height.to_le_bytes())?;
        index.write_all(&[u8::from(row.is_coinbase)])?;
        values.write_all(&row.value_bytes)?;
        offset = offset.saturating_add(value_len);
        total_values = total_values.saturating_add(1);
    }
    let manifest = format!(
        "entries={total_values}\nvalues_bytes={offset}\nstatic_format=key(36)+offset(8)+value_len(8)+height(4)+coinbase(1), value=value_sats|height|is_coinbase|script_len|script\n"
    );
    fs::write(directory.join("README.txt"), manifest)?;
    Ok(())
}

#[cfg(not(feature = "mdbx"))]
fn run_compare(_snapshot: PathBuf, _mdbx: PathBuf, _static_dir: Option<PathBuf>) -> ToolResult<()> {
    Err(ToolError::Msg("build with --features mdbx"))
}

fn read_compact_size(reader: &mut impl Read) -> ToolResult<u64> {
    let mut first = [0_u8; 1];
    reader.read_exact(&mut first)?;
    let value = match first[0] {
        0..=252 => u64::from(first[0]),
        253 => {
            let mut bytes = [0_u8; 2];
            reader.read_exact(&mut bytes)?;
            let value = u64::from(u16::from_le_bytes(bytes));
            if value < 253 {
                return Err(ToolError::Msg("non-canonical compact size"));
            }
            value
        }
        254 => {
            let mut bytes = [0_u8; 4];
            reader.read_exact(&mut bytes)?;
            let value = u64::from(u32::from_le_bytes(bytes));
            if value < 0x1_0000 {
                return Err(ToolError::Msg("non-canonical compact size"));
            }
            value
        }
        255 => {
            let mut bytes = [0_u8; 8];
            reader.read_exact(&mut bytes)?;
            let value = u64::from_le_bytes(bytes);
            if value < 0x1_0000_0000 {
                return Err(ToolError::Msg("non-canonical compact size"));
            }
            value
        }
    };
    if value > MAX_COMPACT_SIZE {
        return Err(ToolError::Msg("compact size too large"));
    }
    Ok(value)
}

fn read_core_varint(reader: &mut impl Read) -> ToolResult<u64> {
    let mut value = 0_u64;
    loop {
        let mut byte = [0_u8; 1];
        reader.read_exact(&mut byte)?;
        value = value
            .checked_shl(7)
            .and_then(|shifted| shifted.checked_add(u64::from(byte[0] & 0x7f)))
            .ok_or(ToolError::Msg("varint overflow"))?;
        if byte[0] & 0x80 == 0 {
            return Ok(value);
        }
        value = value
            .checked_add(1)
            .ok_or(ToolError::Msg("varint overflow"))?;
    }
}

fn decompress_amount(mut value: u64) -> Option<u64> {
    if value == 0 {
        return Some(0);
    }
    value -= 1;
    let mut exponent = value % 10;
    value /= 10;
    let mut amount = if exponent < 9 {
        let digit = value % 9 + 1;
        value /= 9;
        value.checked_mul(10)?.checked_add(digit)?
    } else {
        value.checked_add(1)?
    };
    while exponent > 0 {
        amount = amount.checked_mul(10)?;
        exponent -= 1;
    }
    Some(amount)
}

fn decompress_script(reader: &mut impl Read) -> ToolResult<Vec<u8>> {
    let size = read_core_varint(reader)?;
    if size > MAX_SCRIPT_BYTES {
        return Err(ToolError::Msg("script size too large"));
    }
    match size {
        0 => {
            let mut hash = [0_u8; 20];
            reader.read_exact(&mut hash)?;
            let mut script = Vec::with_capacity(25);
            script.extend_from_slice(&[0x76, 0xa9, 0x14]);
            script.extend_from_slice(&hash);
            script.push(0x88);
            script.push(0xac);
            Ok(script)
        }
        1 => {
            let mut hash = [0_u8; 20];
            reader.read_exact(&mut hash)?;
            let mut script = Vec::with_capacity(23);
            script.extend_from_slice(&[0xa9, 0x14]);
            script.extend_from_slice(&hash);
            script.push(0x87);
            Ok(script)
        }
        2 | 3 => {
            let mut x = [0_u8; 32];
            reader.read_exact(&mut x)?;
            let mut script = Vec::with_capacity(35);
            script.push(0x21);
            script.extend_from_slice(&x);
            script.push(0xac);
            Ok(script)
        }
        4 | 5 => {
            let mut compressed = [0_u8; 33];
            compressed[0] = u8::try_from(size - 2).expect("template code in range");
            reader.read_exact(&mut compressed[1..])?;
            let key = bitcoin::secp256k1::PublicKey::from_slice(&compressed)
                .map_err(|_| ToolError::Msg("invalid compressed public key"))?;
            let mut script = Vec::with_capacity(67);
            script.push(0x41);
            script.extend_from_slice(&key.serialize_uncompressed());
            script.push(0xac);
            Ok(script)
        }
        _ => {
            let script_len = size
                .checked_sub(6)
                .ok_or(ToolError::Msg("script size underflow"))?;
            if script_len > MAX_SCRIPT_BYTES {
                return Err(ToolError::Msg("script too large"));
            }
            let mut script = vec![0_u8; usize::try_from(script_len).expect("script fits usize")];
            reader.read_exact(&mut script)?;
            Ok(script)
        }
    }
}

fn parse_args() -> (PathBuf, PathBuf, Option<PathBuf>) {
    let mut snapshot = None;
    let mut mdbx = None;
    let mut static_dir = None;
    let args = env::args().skip(1).collect::<Vec<_>>();
    let mut index = 0usize;
    while index < args.len() {
        match args[index].as_str() {
            "--snapshot" => {
                snapshot = args.get(index + 1).cloned().map(PathBuf::from);
                index += 2;
            }
            "--mdbx" => {
                mdbx = args.get(index + 1).cloned().map(PathBuf::from);
                index += 2;
            }
            "--static-dir" => {
                static_dir = args.get(index + 1).cloned().map(PathBuf::from);
                index += 2;
            }
            _ => {
                break;
            }
        }
    }
    (
        snapshot.unwrap_or_else(|| PathBuf::from("")),
        mdbx.unwrap_or_else(|| PathBuf::from("")),
        static_dir,
    )
}

fn print_usage_and_exit() -> ! {
    eprintln!("usage: compare_snapshot_vs_mdbx --snapshot PATH --mdbx PATH [--static-dir PATH]");
    std::process::exit(1);
}

fn main() -> ToolResult<()> {
    let (snapshot, mdbx_path, static_dir) = parse_args();
    if !snapshot.exists() || !mdbx_path.exists() {
        print_usage_and_exit();
    }
    run_compare(snapshot, mdbx_path, static_dir)
}
