//! Immutable zstd block archives suitable for piece-addressed distribution.

use std::{
    fs::{self, File},
    io::{BufReader, Cursor, Read, Seek, SeekFrom, Write},
    path::Path,
    sync::Arc,
};

use bitcoin::{Block, BlockHash, consensus::deserialize};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

const MAGIC: &[u8; 8] = b"RBTCBLK1";
const FORMAT_VERSION: u16 = 2;
const LEGACY_FORMAT_VERSION: u16 = 1;
const PIECE_SIZE: usize = 4 * 1024 * 1024;
const PIECE_SCRATCH_BYTES: usize = 64 * 1024;
const MAX_MANIFEST_SIZE: usize = 16 * 1024 * 1024;
const MAX_BLOCK_BYTES: usize = 4_000_000;
const MAX_BLOCKS_PER_ARCHIVE: u32 = 100_000;
pub(crate) const MAX_RECORDS_BYTES: u64 = 1024 * 1024 * 1024;
const MAX_CONTAINER_BYTES: u64 = MAX_RECORDS_BYTES + MAX_MANIFEST_SIZE as u64 + 12;
const MAX_PIECES: usize = 261;
// Ledger segments are produced on the IBD hot path and retained only inside a
// fixed byte budget. Level 1 keeps decompression compatibility and integrity
// unchanged while avoiding level-9 CPU cost for data that will soon rotate.
const ARCHIVE_COMPRESSION_LEVEL: i32 = 1;
const MAX_ARCHIVE_COMPRESSION_WORKERS: usize = 4;
const MIN_ARCHIVE_BYTES_PER_COMPRESSION_WORKER: u64 = 32 * 1024 * 1024;
// Zstandard recommends supporting at least an 8 MiB window for interoperable
// streaming frames. Keep that fixed memory floor separate from the authenticated
// decompressed-output ceiling below.
const MIN_ZSTD_WINDOW_LOG: u32 = 23;
const MAX_ZSTD_WINDOW_LOG: u32 = 27;

/// Returns the longest leading block slice that fits one archive's canonical
/// record-byte ceiling.
///
/// Individual blocks are checked against the same consensus payload bound as
/// [`encode_archive`]. A non-empty valid input always admits at least one
/// block because the per-block ceiling is smaller than the archive ceiling.
pub fn bounded_archive_prefix_len(blocks: &[impl AsRef<[u8]>]) -> Result<usize, ArchiveError> {
    bounded_archive_prefix_len_from_lengths(blocks.iter().map(|block| block.as_ref().len()))
}

fn bounded_archive_prefix_len_from_lengths(
    block_lengths: impl IntoIterator<Item = usize>,
) -> Result<usize, ArchiveError> {
    let mut records_bytes = 0_u64;
    let mut block_count = 0;
    for block_len in block_lengths {
        if block_len > MAX_BLOCK_BYTES {
            return Err(ArchiveError::Invalid("block too large"));
        }
        let next_records_bytes = records_bytes
            .checked_add(4)
            .and_then(|bytes| {
                bytes.checked_add(u64::try_from(block_len).expect("block length fits u64"))
            })
            .ok_or(ArchiveError::Invalid("records too large"))?;
        if next_records_bytes > MAX_RECORDS_BYTES {
            return Ok(block_count);
        }
        records_bytes = next_records_bytes;
        block_count += 1;
    }
    Ok(block_count)
}

/// Archive read/write failure.
#[derive(Debug, Error)]
pub enum ArchiveError {
    /// Shared local resource admission failed before allocation or file creation.
    #[error("archive resource admission: {0}")]
    ResourceBudget(std::io::Error),
    /// Filesystem or compression I/O failure.
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    /// Metadata parse failure.
    #[error("manifest: {0}")]
    Manifest(#[from] serde_json::Error),
    /// Invalid immutable archive.
    #[error("invalid block archive: {0}")]
    Invalid(&'static str),
}

/// Immutable serialized block whose clones share payload and any admission.
/// Borrowing bytes never transfers the reservation away from the allocation.
#[derive(Clone, Debug)]
pub struct ArchiveBlock(Arc<ArchiveBlockPayload>);

#[derive(Debug)]
struct ArchiveBlockPayload {
    bytes: Vec<u8>,
    _reservation: Option<crate::node_memory::MemoryLease>,
}

impl ArchiveBlock {
    fn allocation_bytes(length: usize) -> u64 {
        (length + std::mem::size_of::<ArchiveBlockPayload>() + 2 * std::mem::size_of::<usize>())
            as u64
    }

    fn admitted(bytes: Vec<u8>, reservation: Option<crate::node_memory::MemoryLease>) -> Self {
        Self(Arc::new(ArchiveBlockPayload {
            bytes,
            _reservation: reservation,
        }))
    }
}

impl From<Vec<u8>> for ArchiveBlock {
    /// Wraps bytes already owned by the caller; this does not admit new memory.
    fn from(bytes: Vec<u8>) -> Self {
        Self::admitted(bytes, None)
    }
}

impl std::ops::Deref for ArchiveBlock {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        &self.0.bytes
    }
}
impl AsRef<[u8]> for ArchiveBlock {
    fn as_ref(&self) -> &[u8] {
        self
    }
}
impl PartialEq for ArchiveBlock {
    fn eq(&self, other: &Self) -> bool {
        self.as_ref() == other.as_ref()
    }
}
impl Eq for ArchiveBlock {}
impl PartialEq<Vec<u8>> for ArchiveBlock {
    fn eq(&self, other: &Vec<u8>) -> bool {
        self.as_ref() == other.as_slice()
    }
}

/// Sidecar-equivalent data needed by a BitTorrent/webseed transport.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ArchiveManifest {
    /// Container format version.
    pub format_version: u16,
    /// First block height in this archive.
    pub first_height: u32,
    /// Number of consensus-serialized blocks.
    pub block_count: u32,
    /// Number of bytes in the uncompressed length-prefixed block stream.
    #[serde(default)]
    pub records_bytes: u64,
    /// Hash of the uncompressed frame stream.
    pub records_sha256: String,
    /// Fixed transfer piece size.
    pub piece_size: usize,
    /// SHA-256 digest of each compressed transfer piece.
    pub piece_sha256: Vec<String>,
}

/// Creates a zstd archive whose compressed bytes can be safely piece-verified before import.
pub fn write_archive(
    path: impl AsRef<Path>,
    first_height: u32,
    blocks: &[impl AsRef<[u8]>],
) -> Result<ArchiveManifest, ArchiveError> {
    let path = path.as_ref();
    let records_bytes = archive_record_bytes(blocks)?;
    let _spool = crate::node_memory::for_path(path)?
        .map(|budget| {
            budget
                .reserve_spool(MAX_CONTAINER_BYTES)
                .map_err(ArchiveError::ResourceBudget)
        })
        .transpose()?;
    let parent = path
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let temporary = BoundedArchiveFile {
        file: tempfile::tempfile_in(parent)?,
        remaining: MAX_CONTAINER_BYTES,
    };
    let (temporary, records_sha256) = compress_archive(temporary, blocks, records_bytes)?;
    finish_archive_file(
        path,
        first_height,
        u32::try_from(blocks.len()).expect("validated block count"),
        records_bytes,
        records_sha256,
        temporary.file,
    )
}

fn finish_archive_file(
    path: &Path,
    first_height: u32,
    block_count: u32,
    records_bytes: u64,
    records_sha256: String,
    mut compressed: File,
) -> Result<ArchiveManifest, ArchiveError> {
    compressed.seek(SeekFrom::Start(0))?;
    let mut scratch = PieceScratch::new(path)?;
    let mut piece_sha256 = Vec::new();
    while let Some(hash) = scratch.next_hash(&mut compressed)? {
        piece_sha256.push(hash);
    }
    let manifest = ArchiveManifest {
        format_version: FORMAT_VERSION,
        first_height,
        block_count,
        records_bytes,
        records_sha256,
        piece_size: PIECE_SIZE,
        piece_sha256,
    };
    let metadata = serde_json::to_vec(&manifest)?;
    let compressed_bytes = compressed.metadata()?.len();
    if 12 + metadata.len() as u64 + compressed_bytes > MAX_CONTAINER_BYTES {
        return Err(ArchiveError::Invalid("archive too large"));
    }
    compressed.seek(SeekFrom::Start(0))?;
    let mut output = File::create(path)?;
    output.write_all(MAGIC)?;
    output.write_all(
        &u32::try_from(metadata.len())
            .map_err(|_| ArchiveError::Invalid("manifest too large"))?
            .to_le_bytes(),
    )?;
    output.write_all(&metadata)?;
    std::io::copy(&mut compressed, &mut output)?;
    Ok(manifest)
}

/// Encodes a self-verifying archive for file, BitTorrent, or webseed publication.
pub fn encode_archive(
    first_height: u32,
    blocks: &[Vec<u8>],
) -> Result<(ArchiveManifest, Vec<u8>), ArchiveError> {
    let records_bytes = archive_record_bytes(blocks)?;
    let compressed_capacity =
        usize::try_from(records_bytes).expect("bounded record length fits usize");
    let (compressed, records_sha256) = compress_archive(
        Vec::with_capacity(compressed_capacity),
        blocks,
        records_bytes,
    )?;
    let manifest = ArchiveManifest {
        format_version: FORMAT_VERSION,
        first_height,
        block_count: u32::try_from(blocks.len())
            .map_err(|_| ArchiveError::Invalid("too many blocks"))?,
        records_bytes,
        records_sha256,
        piece_size: PIECE_SIZE,
        piece_sha256: compressed.chunks(PIECE_SIZE).map(hash_hex).collect(),
    };
    let metadata = serde_json::to_vec(&manifest)?;
    let metadata_len =
        u32::try_from(metadata.len()).map_err(|_| ArchiveError::Invalid("manifest too large"))?;
    let mut output = Vec::with_capacity(12 + metadata.len() + compressed.len());
    output.extend_from_slice(MAGIC);
    output.extend_from_slice(&metadata_len.to_le_bytes());
    output.extend_from_slice(&metadata);
    output.extend_from_slice(&compressed);
    if u64::try_from(output.len()).expect("container length fits u64") > MAX_CONTAINER_BYTES {
        return Err(ArchiveError::Invalid("archive too large"));
    }
    Ok((manifest, output))
}

/// Checks archive pieces and returns the original consensus-serialized blocks.
pub fn read_archive(
    path: impl AsRef<Path>,
) -> Result<(ArchiveManifest, Vec<Vec<u8>>), ArchiveError> {
    let mut file = File::open(path)?;
    if file.metadata()?.len() > MAX_CONTAINER_BYTES {
        return Err(ArchiveError::Invalid("archive too large"));
    }
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)?;
    decode_archive(&bytes)
}

fn archive_record_bytes(blocks: &[impl AsRef<[u8]>]) -> Result<u64, ArchiveError> {
    if blocks.is_empty() {
        return Err(ArchiveError::Invalid("empty archive"));
    }
    if blocks.len() > usize::try_from(MAX_BLOCKS_PER_ARCHIVE).expect("u32 fits usize") {
        return Err(ArchiveError::Invalid("too many blocks"));
    }
    let mut records_bytes = 0_u64;
    for block in blocks {
        let block = block.as_ref();
        if block.len() > MAX_BLOCK_BYTES {
            return Err(ArchiveError::Invalid("block too large"));
        }
        records_bytes = records_bytes
            .checked_add(4)
            .and_then(|bytes| {
                bytes.checked_add(u64::try_from(block.len()).expect("block length fits u64"))
            })
            .ok_or(ArchiveError::Invalid("records too large"))?;
        if records_bytes > MAX_RECORDS_BYTES {
            return Err(ArchiveError::Invalid("records too large"));
        }
    }
    Ok(records_bytes)
}

fn compress_archive<W: Write>(
    output: W,
    blocks: &[impl AsRef<[u8]>],
    records_bytes: u64,
) -> Result<(W, String), ArchiveError> {
    let mut encoder = archive_encoder(output, records_bytes)?;
    let mut records_hash = Sha256::new();
    for block in blocks {
        let block = block.as_ref();
        let len =
            u32::try_from(block.len()).map_err(|_| ArchiveError::Invalid("block too large"))?;
        let len = len.to_le_bytes();
        records_hash.update(len);
        records_hash.update(block);
        encoder.write_all(&len)?;
        encoder.write_all(block)?;
    }
    let output = encoder.finish()?;
    Ok((output, crate::utxo::hex_lower(&records_hash.finalize())))
}

fn archive_encoder<W: Write>(
    output: W,
    records_bytes: u64,
) -> Result<zstd::stream::Encoder<'static, W>, ArchiveError> {
    let mut encoder = zstd::stream::Encoder::new(output, ARCHIVE_COMPRESSION_LEVEL)?;
    let useful_workers = usize::try_from(
        records_bytes
            .div_ceil(MIN_ARCHIVE_BYTES_PER_COMPRESSION_WORKER)
            .max(1),
    )
    .unwrap_or(usize::MAX);
    let workers = std::thread::available_parallelism()
        .map_or(1, std::num::NonZero::get)
        .min(MAX_ARCHIVE_COMPRESSION_WORKERS)
        .min(useful_workers);
    if workers > 1 {
        encoder.multithread(u32::try_from(workers).expect("compression worker bound fits u32"))?;
    }
    Ok(encoder)
}

/// Re-encodes a verified prefix without retaining the source blocks. The
/// destination is opened only after both complete source verification passes.
pub(crate) fn write_archive_prefix(
    source: &Path,
    expected: &ArchiveManifest,
    count: u32,
    destination: &Path,
) -> Result<ArchiveManifest, ArchiveError> {
    if count == 0 || count > expected.block_count {
        return Err(ArchiveError::Invalid("archive prefix count"));
    }
    let mut records_bytes = 0_u64;
    visit_archive_prefix(source, expected, count, &mut |_, block| {
        records_bytes += 4 + block.len() as u64;
        true
    })?;
    let _spool = crate::node_memory::for_path(destination)?
        .map(|budget| {
            budget
                .reserve_spool(MAX_CONTAINER_BYTES)
                .map_err(ArchiveError::ResourceBudget)
        })
        .transpose()?;
    let parent = destination
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let temporary = BoundedArchiveFile {
        file: tempfile::tempfile_in(parent)?,
        remaining: MAX_CONTAINER_BYTES,
    };
    let mut encoder = archive_encoder(temporary, records_bytes)?;
    let mut digest = Sha256::new();
    let mut failure = None;
    visit_archive_prefix(source, expected, count, &mut |_, block| {
        let length = u32::try_from(block.len())
            .expect("verified record size")
            .to_le_bytes();
        digest.update(length);
        digest.update(block);
        if let Err(error) = encoder
            .write_all(&length)
            .and_then(|()| encoder.write_all(block))
        {
            failure = Some(error);
            return false;
        }
        true
    })?;
    if let Some(error) = failure {
        return Err(error.into());
    }
    let temporary = encoder.finish()?;
    finish_archive_file(
        destination,
        expected.first_height,
        count,
        records_bytes,
        crate::utxo::hex_lower(&digest.finalize()),
        temporary.file,
    )
}

struct BoundedArchiveFile {
    file: File,
    remaining: u64,
}
impl Write for BoundedArchiveFile {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() as u64 > self.remaining {
            return Err(std::io::Error::other(
                "compressed archive temporary byte limit exceeded",
            ));
        }
        let written = self.file.write(bytes)?;
        self.remaining -= written as u64;
        Ok(written)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.file.flush()
    }
}

/// Reads a contiguous byte/count-bounded selection while verifying the whole
/// archive. Skipped records use fixed scratch, never a whole-record allocation.
/// An empty selection means the requested start is absent or its first record
/// exceeds the caller's byte allowance. Records include their four-byte length.
pub(crate) fn read_archive_batch(
    path: impl AsRef<Path>,
    first_height: u32,
    max_blocks: u32,
    max_record_bytes: u64,
) -> Result<(ArchiveManifest, Vec<ArchiveBlock>), ArchiveError> {
    if max_blocks == 0 || max_record_bytes == 0 {
        return Err(ArchiveError::Invalid("archive batch bound"));
    }
    scan_archive_selection(path, None, first_height, max_blocks, max_record_bytes, None)
        .map(|(manifest, blocks, _)| (manifest, blocks))
}

// Callbacks may validate but must not publish: the record digest is checked
// after the final callback, including records beyond the requested prefix.
pub(crate) fn visit_archive_prefix(
    path: impl AsRef<Path>,
    expected: &ArchiveManifest,
    count: u32,
    visit: &mut dyn FnMut(u32, &[u8]) -> bool,
) -> Result<bool, ArchiveError> {
    if count > expected.block_count {
        return Err(ArchiveError::Invalid("archive prefix count"));
    }
    scan_archive_selection(
        path,
        Some(expected),
        expected.first_height,
        count,
        MAX_RECORDS_BYTES,
        Some(visit),
    )
    .map(|(_, _, complete)| complete)
}

type ArchiveVisitor<'a> = &'a mut dyn FnMut(u32, &[u8]) -> bool;

fn scan_archive_selection(
    path: impl AsRef<Path>,
    expected: Option<&ArchiveManifest>,
    first_height: u32,
    max_blocks: u32,
    max_record_bytes: u64,
    mut visit: Option<ArchiveVisitor<'_>>,
) -> Result<(ArchiveManifest, Vec<ArchiveBlock>, bool), ArchiveError> {
    let path = path.as_ref();
    let mut file = File::open(path)?;
    if file.metadata()?.len() > MAX_CONTAINER_BYTES {
        return Err(ArchiveError::Invalid("archive too large"));
    }
    let (manifest, records_limit, payload_offset) = read_manifest_header_from(&mut file)?;
    if expected.is_some_and(|expected| expected != &manifest) {
        return Err(ArchiveError::Invalid("archive identity changed"));
    }
    verify_compressed_pieces_from(path, &mut file, payload_offset, &manifest)?;
    file.seek(SeekFrom::Start(payload_offset))?;
    let decoder = ArchiveDecoder::new(path, file, records_limit)?;
    let mut bounded = decoder.take(records_limit.saturating_add(1));
    let _scratch_reservation = reserve_archive_memory(path, 64 * 1024)?;
    let mut scratch = vec![0_u8; 64 * 1024].into_boxed_slice();
    let mut digest = Sha256::new();
    let mut records_bytes = 0_u64;
    let mut selected_bytes = 0_u64;
    let mut selected_count = 0_u32;
    let mut complete = true;
    let mut stopped = first_height < manifest.first_height;
    let mut blocks = Vec::new();
    for offset in 0..manifest.block_count {
        let mut length = [0_u8; 4];
        bounded
            .read_exact(&mut length)
            .map_err(|error| map_record_read_error(error, "block length"))?;
        digest.update(length);
        let length = usize::try_from(u32::from_le_bytes(length)).expect("u32 fits usize");
        if length > MAX_BLOCK_BYTES {
            return Err(ArchiveError::Invalid("block length"));
        }
        let bytes = 4 + length as u64;
        records_bytes = records_bytes
            .checked_add(bytes)
            .ok_or(ArchiveError::Invalid("records too large"))?;
        if records_bytes > records_limit {
            return Err(ArchiveError::Invalid("records too large"));
        }
        let height = manifest
            .first_height
            .checked_add(offset)
            .ok_or(ArchiveError::Invalid("height overflow"))?;
        let wanted = !stopped && height >= first_height && selected_count < max_blocks;
        if wanted && bytes <= max_record_bytes.saturating_sub(selected_bytes) {
            let reservation_bytes = if visit.is_some() {
                length as u64
            } else {
                ArchiveBlock::allocation_bytes(length)
            };
            let block_reservation = reserve_archive_memory(path, reservation_bytes)?;
            let mut block = vec![0_u8; length];
            bounded
                .read_exact(&mut block)
                .map_err(|error| map_record_read_error(error, "block length"))?;
            digest.update(&block);
            selected_bytes += bytes;
            selected_count += 1;
            if let Some(visit) = visit.as_mut() {
                complete = visit(height, &block);
                stopped |= !complete;
            } else {
                blocks.push(ArchiveBlock::admitted(block, block_reservation));
            }
        } else {
            stopped |= wanted;
            let mut remaining = length;
            while remaining > 0 {
                let take = remaining.min(scratch.len());
                bounded
                    .read_exact(&mut scratch[..take])
                    .map_err(|error| map_record_read_error(error, "block length"))?;
                digest.update(&scratch[..take]);
                remaining -= take;
            }
        }
    }
    let mut trailing = [0_u8; 1];
    if bounded.read(&mut trailing)? != 0 {
        return Err(ArchiveError::Invalid("block count"));
    }
    if manifest.format_version == FORMAT_VERSION && records_bytes != manifest.records_bytes {
        return Err(ArchiveError::Invalid("records length"));
    }
    if crate::utxo::hex_lower(&digest.finalize()) != manifest.records_sha256 {
        return Err(ArchiveError::Invalid("records checksum"));
    }
    Ok((manifest, blocks, complete))
}

/// Checks a bounded in-memory archive and returns its consensus-serialized blocks.
///
/// This is the parser used by file imports and deterministic fuzz regression.
pub fn decode_archive(file: &[u8]) -> Result<(ArchiveManifest, Vec<Vec<u8>>), ArchiveError> {
    let (manifest, records_limit, compressed) = verify_archive_container(file)?;
    let mut records = Vec::new();
    let mut decoder = zstd::stream::Decoder::new(Cursor::new(compressed))?;
    decoder.window_log_max(zstd_window_log(records_limit))?;
    decoder
        .take(records_limit.saturating_add(1))
        .read_to_end(&mut records)?;
    let actual_records_bytes =
        u64::try_from(records.len()).expect("bounded records length fits u64");
    if actual_records_bytes > records_limit {
        return Err(ArchiveError::Invalid("records too large"));
    }
    if manifest.format_version == FORMAT_VERSION && actual_records_bytes != manifest.records_bytes {
        return Err(ArchiveError::Invalid("records length"));
    }
    if hash_hex(&records) != manifest.records_sha256 {
        return Err(ArchiveError::Invalid("records checksum"));
    }
    let mut records = records.as_slice();
    let mut blocks = Vec::with_capacity(
        usize::try_from(manifest.block_count)
            .expect("u32 fits usize")
            .min(1_024),
    );
    while !records.is_empty() {
        if records.len() < 4 {
            return Err(ArchiveError::Invalid("block length"));
        }
        let len = u32::from_le_bytes(records[..4].try_into().expect("checked length"));
        let len = usize::try_from(len).expect("u32 fits usize");
        if len > MAX_BLOCK_BYTES || records.len() < 4 + len {
            return Err(ArchiveError::Invalid("block length"));
        }
        blocks.push(records[4..4 + len].to_vec());
        records = &records[4 + len..];
    }
    if blocks.len() != usize::try_from(manifest.block_count).expect("u32 fits usize") {
        return Err(ArchiveError::Invalid("block count"));
    }
    Ok((manifest, blocks))
}

/// Verifies an archive container and its compressed transfer pieces without
/// decompressing the authenticated record stream.
///
/// This is sufficient when a freshly staged archive is moved unchanged into a
/// ledger slot. Full reads still validate the decompressed length, digest, and
/// individual block framing.
pub(crate) fn verify_archive(path: impl AsRef<Path>) -> Result<ArchiveManifest, ArchiveError> {
    let path = path.as_ref();
    let mut file = File::open(path)?;
    if file.metadata()?.len() > MAX_CONTAINER_BYTES {
        return Err(ArchiveError::Invalid("archive too large"));
    }
    let (manifest, _, payload_offset) = read_manifest_header_from(&mut file)?;
    verify_compressed_pieces_from(path, &mut file, payload_offset, &manifest)?;
    Ok(manifest)
}

/// Fully verifies one immutable archive with bounded memory.
///
/// Compressed piece hashes and the decompressed record hash/framing are checked
/// in two sequential passes. This is intended for offline freezer audits where
/// materializing as much as 1 GiB of block records would be unnecessary.
pub(crate) fn verify_archive_streaming(
    path: impl AsRef<Path>,
) -> Result<ArchiveManifest, ArchiveError> {
    let path = path.as_ref();
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(ArchiveError::Invalid("archive must be a regular file"));
    }
    if metadata.len() > MAX_CONTAINER_BYTES {
        return Err(ArchiveError::Invalid("archive too large"));
    }

    let (manifest, records_limit, payload_offset) = read_manifest_header(path)?;
    verify_compressed_pieces(path, payload_offset, &manifest)?;
    verify_record_stream(path, payload_offset, records_limit, &manifest)?;
    Ok(manifest)
}

/// Fully verifies one archive and returns its consensus block hashes without
/// materializing the complete decompressed record stream.
///
/// The compressed payload is piece-verified first. The second pass keeps at
/// most one consensus-sized block in memory while checking framing, the
/// decompressed record digest, and strict Bitcoin block decoding.
pub(crate) fn verify_archive_block_hashes_streaming(
    path: impl AsRef<Path>,
) -> Result<(ArchiveManifest, Vec<BlockHash>, u64), ArchiveError> {
    let path = path.as_ref();
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(ArchiveError::Invalid("archive must be a regular file"));
    }
    if metadata.len() > MAX_CONTAINER_BYTES {
        return Err(ArchiveError::Invalid("archive too large"));
    }
    let (manifest, records_limit, payload_offset) = read_manifest_header(path)?;
    verify_compressed_pieces(path, payload_offset, &manifest)?;

    let mut file = File::open(path)?;
    file.seek(SeekFrom::Start(payload_offset))?;
    let decoder = ArchiveDecoder::new(path, file, records_limit)?;
    let mut bounded = decoder.take(records_limit.saturating_add(1));
    let mut digest = Sha256::new();
    let mut records_bytes = 0_u64;
    let mut hashes = Vec::with_capacity(
        usize::try_from(manifest.block_count)
            .expect("u32 fits usize")
            .min(1_024),
    );
    for _ in 0..manifest.block_count {
        let mut length = [0_u8; 4];
        bounded
            .read_exact(&mut length)
            .map_err(|error| map_record_read_error(error, "block length"))?;
        digest.update(length);
        records_bytes = records_bytes
            .checked_add(4)
            .ok_or(ArchiveError::Invalid("records too large"))?;
        let length = usize::try_from(u32::from_le_bytes(length)).expect("u32 fits usize");
        if length > MAX_BLOCK_BYTES {
            return Err(ArchiveError::Invalid("block length"));
        }
        let mut block = vec![0_u8; length];
        bounded
            .read_exact(&mut block)
            .map_err(|error| map_record_read_error(error, "block length"))?;
        digest.update(&block);
        records_bytes = records_bytes
            .checked_add(u64::try_from(length).expect("block length fits u64"))
            .ok_or(ArchiveError::Invalid("records too large"))?;
        if records_bytes > records_limit {
            return Err(ArchiveError::Invalid("records too large"));
        }
        let block: Block =
            deserialize(&block).map_err(|_| ArchiveError::Invalid("block consensus encoding"))?;
        hashes.push(block.block_hash());
    }
    let mut trailing = [0_u8; 1];
    if bounded.read(&mut trailing)? != 0 {
        return Err(ArchiveError::Invalid("block count"));
    }
    if manifest.format_version == FORMAT_VERSION && records_bytes != manifest.records_bytes {
        return Err(ArchiveError::Invalid("records length"));
    }
    if crate::utxo::hex_lower(&digest.finalize()) != manifest.records_sha256 {
        return Err(ArchiveError::Invalid("records checksum"));
    }
    Ok((manifest, hashes, records_bytes))
}

fn map_record_read_error(error: std::io::Error, field: &'static str) -> ArchiveError {
    if error.kind() == std::io::ErrorKind::UnexpectedEof {
        ArchiveError::Invalid(field)
    } else {
        ArchiveError::Io(error)
    }
}

fn read_manifest_header(path: &Path) -> Result<(ArchiveManifest, u64, u64), ArchiveError> {
    let mut file = File::open(path)?;
    read_manifest_header_from(&mut file)
}

fn read_manifest_header_from(file: &mut File) -> Result<(ArchiveManifest, u64, u64), ArchiveError> {
    file.seek(SeekFrom::Start(0))?;
    let mut header = [0_u8; 12];
    file.read_exact(&mut header)?;
    if &header[..8] != MAGIC {
        return Err(ArchiveError::Invalid("magic"));
    }
    let metadata_len = usize::try_from(u32::from_le_bytes(
        header[8..12].try_into().expect("fixed manifest header"),
    ))
    .expect("u32 fits usize");
    if metadata_len == 0 || metadata_len > MAX_MANIFEST_SIZE {
        return Err(ArchiveError::Invalid("manifest length"));
    }
    let mut metadata = vec![0_u8; metadata_len];
    file.read_exact(&mut metadata)?;
    let manifest: ArchiveManifest = serde_json::from_slice(&metadata)?;
    let records_limit = validate_manifest(&manifest)?;
    let payload_offset = 12_u64
        .checked_add(u64::try_from(metadata_len).expect("manifest length fits u64"))
        .ok_or(ArchiveError::Invalid("manifest length"))?;
    Ok((manifest, records_limit, payload_offset))
}

// Keep the reservation after the owned buffer so drop releases memory first.
// Returned digest strings and manifests require separate ownership accounting.
struct PieceScratch {
    bytes: Box<[u8]>,
    _reservation: Option<crate::node_memory::MemoryLease>,
}

impl PieceScratch {
    fn new(path: &Path) -> Result<Self, ArchiveError> {
        let reservation = crate::node_memory::for_path(path)?
            .map(|budget| {
                budget
                    .reserve(PIECE_SCRATCH_BYTES as u64)
                    .map_err(ArchiveError::ResourceBudget)
            })
            .transpose()?;
        Ok(Self {
            bytes: vec![0; PIECE_SCRATCH_BYTES].into_boxed_slice(),
            _reservation: reservation,
        })
    }

    fn next_hash(&mut self, reader: &mut impl Read) -> Result<Option<String>, ArchiveError> {
        let mut digest = Sha256::new();
        let mut filled = 0;
        while filled < PIECE_SIZE {
            let take = self.bytes.len().min(PIECE_SIZE - filled);
            let read = match reader.read(&mut self.bytes[..take]) {
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
                result => result?,
            };
            if read == 0 {
                break;
            }
            digest.update(&self.bytes[..read]);
            filled += read;
        }
        Ok((filled != 0).then(|| crate::utxo::hex_lower(&digest.finalize())))
    }
}

fn verify_compressed_pieces(
    path: &Path,
    payload_offset: u64,
    manifest: &ArchiveManifest,
) -> Result<(), ArchiveError> {
    let mut file = File::open(path)?;
    verify_compressed_pieces_from(path, &mut file, payload_offset, manifest)
}

fn verify_compressed_pieces_from(
    path: &Path,
    file: &mut File,
    payload_offset: u64,
    manifest: &ArchiveManifest,
) -> Result<(), ArchiveError> {
    file.seek(SeekFrom::Start(payload_offset))?;
    let mut scratch = PieceScratch::new(path)?;
    let mut piece_index = 0_usize;
    while let Some(actual) = scratch.next_hash(file)? {
        if manifest.piece_sha256.get(piece_index) != Some(&actual) {
            return Err(ArchiveError::Invalid("piece checksum"));
        }
        piece_index += 1;
    }
    if piece_index != manifest.piece_sha256.len() {
        return Err(ArchiveError::Invalid("piece checksum"));
    }
    Ok(())
}

fn verify_record_stream(
    path: &Path,
    payload_offset: u64,
    records_limit: u64,
    manifest: &ArchiveManifest,
) -> Result<(), ArchiveError> {
    let mut file = File::open(path)?;
    file.seek(SeekFrom::Start(payload_offset))?;
    let decoder = ArchiveDecoder::new(path, file, records_limit)?;
    let mut bounded = decoder.take(records_limit.saturating_add(1));
    let _scratch_reservation = reserve_archive_memory(path, 64 * 1024)?;
    let mut buffer = vec![0_u8; 64 * 1024].into_boxed_slice();
    let mut verifier = RecordStreamVerifier::default();
    let mut digest = Sha256::new();
    let mut records_bytes = 0_u64;
    loop {
        let read = bounded.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        records_bytes = records_bytes
            .checked_add(u64::try_from(read).expect("buffer read fits u64"))
            .ok_or(ArchiveError::Invalid("records too large"))?;
        if records_bytes > records_limit {
            return Err(ArchiveError::Invalid("records too large"));
        }
        digest.update(&buffer[..read]);
        verifier.feed(&buffer[..read])?;
    }
    verifier.finish(manifest.block_count)?;
    if manifest.format_version == FORMAT_VERSION && records_bytes != manifest.records_bytes {
        return Err(ArchiveError::Invalid("records length"));
    }
    if crate::utxo::hex_lower(&digest.finalize()) != manifest.records_sha256 {
        return Err(ArchiveError::Invalid("records checksum"));
    }
    Ok(())
}

#[derive(Default)]
struct RecordStreamVerifier {
    length: [u8; 4],
    length_bytes: usize,
    remaining_block_bytes: usize,
    blocks: u32,
}

impl RecordStreamVerifier {
    fn feed(&mut self, mut records: &[u8]) -> Result<(), ArchiveError> {
        while !records.is_empty() {
            if self.remaining_block_bytes > 0 {
                let consumed = self.remaining_block_bytes.min(records.len());
                self.remaining_block_bytes -= consumed;
                records = &records[consumed..];
                if self.remaining_block_bytes == 0 {
                    self.blocks = self
                        .blocks
                        .checked_add(1)
                        .ok_or(ArchiveError::Invalid("block count"))?;
                }
                continue;
            }

            let consumed = (4 - self.length_bytes).min(records.len());
            self.length[self.length_bytes..self.length_bytes + consumed]
                .copy_from_slice(&records[..consumed]);
            self.length_bytes += consumed;
            records = &records[consumed..];
            if self.length_bytes == 4 {
                let length =
                    usize::try_from(u32::from_le_bytes(self.length)).expect("u32 fits usize");
                if length > MAX_BLOCK_BYTES {
                    return Err(ArchiveError::Invalid("block length"));
                }
                self.length_bytes = 0;
                self.remaining_block_bytes = length;
                if length == 0 {
                    self.blocks = self
                        .blocks
                        .checked_add(1)
                        .ok_or(ArchiveError::Invalid("block count"))?;
                }
            }
        }
        Ok(())
    }

    fn finish(self, expected_blocks: u32) -> Result<(), ArchiveError> {
        if self.length_bytes != 0 || self.remaining_block_bytes != 0 {
            return Err(ArchiveError::Invalid("block length"));
        }
        if self.blocks != expected_blocks {
            return Err(ArchiveError::Invalid("block count"));
        }
        Ok(())
    }
}

fn verify_archive_container(file: &[u8]) -> Result<(ArchiveManifest, u64, &[u8]), ArchiveError> {
    if u64::try_from(file.len()).expect("slice length fits u64") > MAX_CONTAINER_BYTES {
        return Err(ArchiveError::Invalid("archive too large"));
    }
    if file.len() < 12 || &file[..8] != MAGIC {
        return Err(ArchiveError::Invalid("magic"));
    }
    let metadata_len = u32::from_le_bytes(file[8..12].try_into().expect("checked header"));
    let metadata_len = usize::try_from(metadata_len).expect("u32 fits usize");
    if metadata_len == 0 || metadata_len > MAX_MANIFEST_SIZE {
        return Err(ArchiveError::Invalid("manifest length"));
    }
    let start = 12_usize
        .checked_add(metadata_len)
        .ok_or(ArchiveError::Invalid("manifest length"))?;
    if start > file.len() {
        return Err(ArchiveError::Invalid("manifest length"));
    }
    let manifest: ArchiveManifest = serde_json::from_slice(&file[12..start])?;
    let records_limit = validate_manifest(&manifest)?;
    let compressed = &file[start..];
    let actual_pieces = compressed
        .chunks(PIECE_SIZE)
        .map(hash_hex)
        .collect::<Vec<_>>();
    if actual_pieces != manifest.piece_sha256 {
        return Err(ArchiveError::Invalid("piece checksum"));
    }
    Ok((manifest, records_limit, compressed))
}

/// Reads only the bounded archive manifest without decompressing block data.
///
/// This is used to reconstruct a rotating ledger index after interruption;
/// full piece and record verification still occurs when block bytes are read.
pub fn read_archive_manifest(path: impl AsRef<Path>) -> Result<ArchiveManifest, ArchiveError> {
    let mut file = fs::File::open(path)?;
    let mut header = [0_u8; 12];
    file.read_exact(&mut header)?;
    if &header[..8] != MAGIC {
        return Err(ArchiveError::Invalid("magic"));
    }
    let metadata_len = usize::try_from(u32::from_le_bytes(
        header[8..12].try_into().expect("fixed manifest header"),
    ))
    .expect("u32 fits usize");
    if metadata_len > MAX_MANIFEST_SIZE {
        return Err(ArchiveError::Invalid("manifest too large"));
    }
    let mut metadata = vec![0_u8; metadata_len];
    file.read_exact(&mut metadata)?;
    let manifest: ArchiveManifest = serde_json::from_slice(&metadata)?;
    validate_manifest(&manifest)?;
    Ok(manifest)
}

fn validate_manifest(manifest: &ArchiveManifest) -> Result<u64, ArchiveError> {
    if !matches!(
        manifest.format_version,
        LEGACY_FORMAT_VERSION | FORMAT_VERSION
    ) || manifest.piece_size != PIECE_SIZE
    {
        return Err(ArchiveError::Invalid("manifest version"));
    }
    if manifest.block_count == 0
        || manifest.block_count > MAX_BLOCKS_PER_ARCHIVE
        || decode_sha256(&manifest.records_sha256).is_none()
        || manifest.piece_sha256.is_empty()
        || manifest.piece_sha256.len() > MAX_PIECES
        || manifest
            .piece_sha256
            .iter()
            .any(|digest| decode_sha256(digest).is_none())
    {
        return Err(ArchiveError::Invalid("manifest fields"));
    }
    let minimum_records = u64::from(manifest.block_count)
        .checked_mul(4)
        .ok_or(ArchiveError::Invalid("records length"))?;
    if manifest.format_version == FORMAT_VERSION {
        if manifest.records_bytes < minimum_records || manifest.records_bytes > MAX_RECORDS_BYTES {
            return Err(ArchiveError::Invalid("records length"));
        }
        return Ok(manifest.records_bytes);
    }
    u64::from(manifest.block_count)
        .checked_mul(u64::try_from(4 + MAX_BLOCK_BYTES).expect("block bound fits u64"))
        .map(|limit| limit.min(MAX_RECORDS_BYTES))
        .ok_or(ArchiveError::Invalid("records length"))
}

fn hash_hex(bytes: &[u8]) -> String {
    crate::utxo::hex_lower(&Sha256::digest(bytes))
}

/// Decodes a lowercase 64-character hexadecimal SHA-256 digest.
///
/// Manifests come from untrusted archive files, so this works on bytes. A
/// 64-*byte* string may contain multi-byte characters, and slicing such a value
/// by byte offsets would panic on a character boundary.
fn decode_sha256(value: &str) -> Option<[u8; 32]> {
    let value = value.as_bytes();
    if value.len() != 64 {
        return None;
    }
    let mut digest = [0_u8; 32];
    for (byte, pair) in digest.iter_mut().zip(value.chunks_exact(2)) {
        *byte = (hex_nibble(pair[0])? << 4) | hex_nibble(pair[1])?;
    }
    Some(digest)
}

fn hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn reserve_archive_memory(
    path: &Path,
    bytes: u64,
) -> Result<Option<crate::node_memory::MemoryLease>, ArchiveError> {
    crate::node_memory::for_path(path)?
        .map(|budget| budget.reserve(bytes).map_err(ArchiveError::ResourceBudget))
        .transpose()
}

// The native decoder and Rust input buffer must die before their allowance.
struct ArchiveDecoder {
    inner: zstd::stream::Decoder<'static, BufReader<File>>,
    _reservation: Option<crate::node_memory::MemoryLease>,
}

impl ArchiveDecoder {
    fn allowance(records_limit: u64) -> Result<u64, ArchiveError> {
        let native = rbtc_codec_memory::decoder_bytes(zstd_window_log(records_limit))
            .ok_or(ArchiveError::Invalid("decoder memory estimate"))?;
        native
            .checked_add(zstd::zstd_safe::DCtx::in_size())
            .and_then(|bytes| u64::try_from(bytes).ok())
            .ok_or(ArchiveError::Invalid("decoder memory estimate"))
    }

    fn new(path: &Path, file: File, records_limit: u64) -> Result<Self, ArchiveError> {
        let allowance = Self::allowance(records_limit)?;
        let reservation = crate::node_memory::for_path(path)?
            .map(|budget| {
                budget
                    .reserve(allowance)
                    .map_err(ArchiveError::ResourceBudget)
            })
            .transpose()?;
        let mut inner = zstd::stream::Decoder::new(file)?;
        inner.window_log_max(zstd_window_log(records_limit))?;
        Ok(Self {
            inner,
            _reservation: reservation,
        })
    }
}

impl Read for ArchiveDecoder {
    fn read(&mut self, output: &mut [u8]) -> std::io::Result<usize> {
        self.inner.read(output)
    }
}

fn zstd_window_log(records_bytes: u64) -> u32 {
    let required = u64::BITS - records_bytes.saturating_sub(1).leading_zeros();
    required.clamp(MIN_ZSTD_WINDOW_LOG, MAX_ZSTD_WINDOW_LOG)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn returned_block_clones_keep_admission_through_writing_and_final_drop() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("owned.rblk");
        let data = vec![3; 100_000];
        let manifest = write_archive(&path, 1, std::slice::from_ref(&data)).unwrap();
        let native = ArchiveDecoder::allowance(manifest.records_bytes).unwrap();
        let payload = ArchiveBlock::allocation_bytes(data.len());
        let limit = native + 64 * 1024 + payload;
        let budget = crate::node_memory::MemoryBudget::new(limit);
        budget.bind(&[dir.path().to_path_buf()]).unwrap();
        let pressure = budget.reserve(1).unwrap();
        assert!(matches!(
            read_archive_batch(&path, 1, 1, MAX_RECORDS_BYTES),
            Err(ArchiveError::ResourceBudget(_))
        ));
        assert_eq!(budget.snapshot().used, 1);
        drop(pressure);
        let (_, blocks) = read_archive_batch(&path, 1, 1, MAX_RECORDS_BYTES).unwrap();
        assert_eq!(budget.snapshot().used, payload);
        let alias = blocks[0].clone();
        assert_eq!(alias.as_ptr(), blocks[0].as_ptr());
        let cloned_batch = blocks.clone();
        drop(blocks);
        assert_eq!(budget.snapshot().used, payload);
        let output = dir.path().join("copy.rblk");
        std::thread::scope(|scope| {
            scope
                .spawn(|| write_archive(&output, 1, &cloned_batch).unwrap())
                .join()
                .unwrap();
        });
        assert_eq!(fs::read(&path).unwrap(), fs::read(output).unwrap());
        drop(cloned_batch);
        assert_eq!(budget.snapshot().used, payload);
        assert_eq!(alias.as_ref(), data);
        drop(alias);
        assert_eq!(budget.snapshot().used, 0);
    }

    #[test]
    fn visitor_scratch_denial_precedes_callback_and_early_stop_refunds() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("visitor.rblk");
        let manifest = write_archive(&path, 1, &[vec![7; 70_000], vec![8; 80_000]]).unwrap();
        let original = fs::read(&path).unwrap();
        let native = ArchiveDecoder::allowance(manifest.records_bytes).unwrap();
        let limit = native + 64 * 1024 + 70_000;
        let budget = crate::node_memory::MemoryBudget::new(limit);
        budget.bind(&[dir.path().to_path_buf()]).unwrap();
        // Native context fits, but the scan buffer cannot be allocated.
        let pressure = budget.reserve(limit - native).unwrap();
        assert!(matches!(
            verify_archive_streaming(&path),
            Err(ArchiveError::ResourceBudget(_))
        ));
        assert_eq!(budget.snapshot().used, limit - native);
        drop(pressure);
        // Scan buffer fits, but the first callback payload misses by one byte.
        let pressure = budget.reserve(1).unwrap();
        let mut calls = 0;
        assert!(matches!(
            visit_archive_prefix(&path, &manifest, 2, &mut |_, _| {
                calls += 1;
                true
            }),
            Err(ArchiveError::ResourceBudget(_))
        ));
        assert_eq!(calls, 0);
        assert_eq!(budget.snapshot().used, 1);
        drop(pressure);
        // Stopping after the first record must verify/skip the larger suffix
        // without admitting or materializing that record's callback payload.
        assert!(
            !visit_archive_prefix(&path, &manifest, 2, &mut |height, raw| {
                calls += 1;
                assert_eq!(height, 1);
                assert_eq!(raw.len(), 70_000);
                assert_eq!(budget.snapshot().used, limit);
                false
            })
            .unwrap()
        );
        assert_eq!(calls, 1);
        assert_eq!(budget.snapshot().used, 0);
        assert_eq!(verify_archive_streaming(&path).unwrap(), manifest);
        assert_eq!(budget.snapshot().used, 0);
        assert_eq!(fs::read(&path).unwrap(), original);
    }

    #[test]
    fn native_decoder_admission_survives_callbacks_and_refunds_failures() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("decoder.rblk");
        let manifest = write_archive(&path, 20, &[vec![9; 512 * 1024]]).unwrap();
        let allowance = ArchiveDecoder::allowance(manifest.records_bytes).unwrap();
        assert!(allowance > 8 * 1024 * 1024);
        assert!(ArchiveDecoder::allowance(MAX_RECORDS_BYTES).unwrap() > 128 * 1024 * 1024);
        assert!(rbtc_codec_memory::decoder_bytes(22).is_none());
        assert!(rbtc_codec_memory::decoder_bytes(28).is_none());
        let payload_allowance = ArchiveBlock::allocation_bytes(512 * 1024);
        let total = allowance + 64 * 1024 + payload_allowance;
        let budget = crate::node_memory::MemoryBudget::new(total);
        budget.bind(&[dir.path().to_path_buf()]).unwrap();
        let pressure = budget.reserve(total - allowance + 1).unwrap();
        for result in [
            read_archive_batch(&path, 20, 1, MAX_RECORDS_BYTES).map(|_| ()),
            verify_archive_streaming(&path).map(|_| ()),
            verify_archive_block_hashes_streaming(&path).map(|_| ()),
        ] {
            assert!(matches!(result, Err(ArchiveError::ResourceBudget(_))));
            assert_eq!(budget.snapshot().used, total - allowance + 1);
        }
        drop(pressure);
        let mut calls = 0;
        assert!(
            visit_archive_prefix(&path, &manifest, 1, &mut |_, raw| {
                calls += 1;
                assert_eq!(raw.len(), 512 * 1024);
                assert_eq!(budget.snapshot().used, allowance + 64 * 1024 + 512 * 1024);
                assert!(budget.reserve(payload_allowance - 512 * 1024 + 1).is_err());
                true
            })
            .unwrap()
        );
        assert_eq!(calls, 1);
        assert_eq!(budget.snapshot().used, 0);
        assert_eq!(verify_archive_streaming(&path).unwrap(), manifest);
        assert_eq!(budget.snapshot().used, 0);
        // A valid archive container is not itself a zstd frame. Exercise an
        // error after context creation, with the allowance held until drop.
        let mut decoder =
            ArchiveDecoder::new(&path, File::open(&path).unwrap(), manifest.records_bytes).unwrap();
        assert_eq!(budget.snapshot().used, allowance);
        assert!(decoder.read(&mut [0; 16]).is_err());
        assert_eq!(budget.snapshot().used, allowance);
        drop(decoder);
        assert_eq!(budget.snapshot().used, 0);
        assert_eq!(
            read_archive_batch(&path, 20, 1, MAX_RECORDS_BYTES)
                .unwrap()
                .1[0]
                .len(),
            512 * 1024
        );
        assert_eq!(budget.snapshot().used, 0);
    }

    #[test]
    fn piece_scratch_admission_lifetime_and_short_reads() {
        struct ShortReads(Cursor<Vec<u8>>, bool);
        impl Read for ShortReads {
            fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
                if !self.1 {
                    self.1 = true;
                    return Err(std::io::ErrorKind::Interrupted.into());
                }
                let count = buffer.len().min(997);
                self.0.read(&mut buffer[..count])
            }
        }
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("scratch.rblk");
        let manifest = write_archive(&path, 10, &[vec![3; 128]]).unwrap();
        let original = fs::read(&path).unwrap();
        let budget = crate::node_memory::MemoryBudget::new(PIECE_SCRATCH_BYTES as u64);
        budget.bind(&[dir.path().to_path_buf()]).unwrap();
        let occupied = budget.reserve(1).unwrap();
        assert!(matches!(
            verify_archive(&path),
            Err(ArchiveError::ResourceBudget(_))
        ));
        assert!(matches!(
            write_archive(&path, 11, &[vec![4]]),
            Err(ArchiveError::ResourceBudget(_))
        ));
        assert_eq!(fs::read(&path).unwrap(), original);
        assert_eq!(budget.spool_snapshot().used, 0);
        drop(occupied);
        let mut scratch = PieceScratch::new(&path).unwrap();
        assert_eq!(budget.snapshot().used, PIECE_SCRATCH_BYTES as u64);
        assert!(matches!(
            PieceScratch::new(&path),
            Err(ArchiveError::ResourceBudget(_))
        ));
        let data = vec![17; PIECE_SIZE + 13];
        let mut input = ShortReads(Cursor::new(data.clone()), false);
        assert_eq!(
            scratch.next_hash(&mut input).unwrap(),
            Some(hash_hex(&data[..PIECE_SIZE]))
        );
        assert_eq!(
            scratch.next_hash(&mut input).unwrap(),
            Some(hash_hex(&data[PIECE_SIZE..]))
        );
        assert_eq!(scratch.next_hash(&mut input).unwrap(), None);
        drop(scratch);
        assert_eq!(budget.snapshot().used, 0);
        assert_eq!(verify_archive(&path).unwrap(), manifest);
        assert_eq!(write_archive(&path, 10, &[vec![3; 128]]).unwrap(), manifest);
        assert_eq!(fs::read(&path).unwrap(), original);
        assert_eq!(budget.snapshot().used, 0);
    }

    #[test]
    fn streamed_prefix_matches_encoding_and_rejects_corrupt_source_before_output() {
        let dir = TempDir::new().unwrap();
        let source = dir.path().join("source.rblk");
        let target = dir.path().join("prefix.rblk");
        let blocks = [vec![1; 1000], vec![2; 5000], vec![3; 256 * 1024]];
        let original = write_archive(&source, 10, &blocks).unwrap();
        let source_bytes = fs::read(&source).unwrap();
        let (expected, encoded) = encode_archive(10, &blocks[..2]).unwrap();
        assert_eq!(
            write_archive_prefix(&source, &original, 2, &target).unwrap(),
            expected
        );
        assert_eq!(fs::read(&target).unwrap(), encoded);
        assert_eq!(fs::read(&source).unwrap(), source_bytes);
        assert!(write_archive_prefix(&source, &original, 0, &target).is_err());
        assert!(write_archive_prefix(&source, &original, 4, &target).is_err());
        let mut corrupt = source_bytes;
        *corrupt.last_mut().unwrap() ^= 1;
        fs::write(&source, corrupt).unwrap();
        assert!(write_archive_prefix(&source, &original, 1, &target).is_err());
        assert_eq!(
            fs::read(&target).unwrap(),
            encoded,
            "failed source verification must not truncate output"
        );
        assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 2);
    }

    #[test]
    fn streamed_archive_write_matches_encoding_and_admits_temporary_disk_first() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("stream.rblk");
        let mut state = 1_u32;
        let blocks: Vec<Vec<u8>> = (0..2)
            .map(|_| {
                (0..3 * 1024 * 1024)
                    .map(|_| {
                        state ^= state << 13;
                        state ^= state >> 17;
                        state ^= state << 5;
                        state.to_le_bytes()[0]
                    })
                    .collect()
            })
            .collect();
        let (expected, bytes) = encode_archive(20, &blocks).unwrap();
        assert!(expected.piece_sha256.len() > 1);
        assert_eq!(write_archive(&path, 20, &blocks).unwrap(), expected);
        assert_eq!(fs::read(&path).unwrap(), bytes);
        assert_eq!(verify_archive(&path).unwrap(), expected);
        let budget = crate::node_memory::MemoryBudget::new(128 * 1024);
        budget.bind(&[dir.path().to_path_buf()]).unwrap();
        let occupied = budget
            .reserve_spool(budget.spool_snapshot().limit - MAX_CONTAINER_BYTES + 1)
            .unwrap();
        assert!(write_archive(&path, 20, &blocks).is_err());
        assert_eq!(
            fs::read(&path).unwrap(),
            bytes,
            "denial precedes destination truncation"
        );
        drop(occupied);
        assert_eq!(write_archive(&path, 20, &blocks).unwrap(), expected);
        assert_eq!(budget.spool_snapshot().used, 0);
        assert_eq!(fs::read(&path).unwrap(), bytes);
        assert_eq!(
            fs::read_dir(dir.path()).unwrap().count(),
            1,
            "anonymous compression scratch is removed"
        );
        let mut limited = BoundedArchiveFile {
            file: tempfile::tempfile().unwrap(),
            remaining: 3,
        };
        limited.write_all(&[1, 2, 3]).unwrap();
        assert!(limited.write_all(&[4]).is_err());
        assert_eq!(limited.file.metadata().unwrap().len(), 3);
    }

    #[test]
    fn bounded_archive_selection_checks_skipped_records_and_keeps_a_prefix() {
        let dir = TempDir::new().unwrap();
        let file = dir.path().join("batch.rblk");
        let blocks = [vec![1], vec![2; 256 * 1024], vec![3]];
        let manifest = write_archive(&file, 10, &blocks).unwrap();
        assert_eq!(
            read_archive_batch(&file, 10, 1, 5).unwrap().1,
            vec![vec![1]]
        );
        assert!(
            read_archive_batch(&file, 11, 2, 5).unwrap().1.is_empty(),
            "an oversized first record cannot be skipped in favor of a later small one"
        );
        assert_eq!(
            read_archive_batch(&file, 12, 1, 5).unwrap().1,
            vec![vec![3]]
        );
        assert!(read_archive_batch(&file, 9, 1, 5).unwrap().1.is_empty());
        assert!(read_archive_batch(&file, 13, 1, 5).unwrap().1.is_empty());
        assert_eq!(read_archive_batch(&file, 10, 3, 300_000).unwrap().1, blocks);
        // Keep compressed piece hashes valid, but invalidate the full record
        // commitment. Even a one-record selection must reject this archive.
        let bytes = fs::read(&file).unwrap();
        let offset =
            12 + usize::try_from(u32::from_le_bytes(bytes[8..12].try_into().unwrap())).unwrap();
        let mut visited = Vec::new();
        assert!(
            visit_archive_prefix(&file, &manifest, 3, &mut |height, raw| {
                visited.push((height, raw.len()));
                true
            })
            .unwrap()
        );
        assert_eq!(visited, vec![(10, 1), (11, 256 * 1024), (12, 1)]);
        let mut invalid = manifest.clone();
        invalid.records_sha256 = "00".repeat(32);
        let metadata = serde_json::to_vec(&invalid).unwrap();
        let mut changed = MAGIC.to_vec();
        changed.extend_from_slice(&u32::try_from(metadata.len()).unwrap().to_le_bytes());
        changed.extend_from_slice(&metadata);
        changed.extend_from_slice(&bytes[offset..]);
        fs::write(&file, changed).unwrap();
        assert!(matches!(
            read_archive_batch(&file, 10, 1, 5),
            Err(ArchiveError::Invalid("records checksum"))
        ));
        let mut calls = 0;
        assert!(matches!(
            visit_archive_prefix(&file, &manifest, 3, &mut |_, _| {
                calls += 1;
                true
            }),
            Err(ArchiveError::Invalid("archive identity changed"))
        ));
        assert_eq!(calls, 0, "identity must be checked before callbacks");
        assert!(matches!(
            visit_archive_prefix(&file, &invalid, 3, &mut |_, _| {
                calls += 1;
                false
            }),
            Err(ArchiveError::Invalid("records checksum"))
        ));
        assert_eq!(calls, 1, "early stop still verifies the omitted suffix");
    }

    #[test]
    fn archive_roundtrips_and_detects_piece_tampering() {
        let dir = TempDir::new().unwrap();
        let file = dir.path().join("00000.rblk");
        let blocks = vec![vec![1, 2, 3], vec![4; 300]];
        let manifest = write_archive(&file, 100, &blocks).unwrap();
        assert_eq!(manifest.format_version, FORMAT_VERSION);
        assert_eq!(manifest.records_bytes, 311);
        assert_eq!(manifest.block_count, 2);
        assert_eq!(read_archive_manifest(&file).unwrap(), manifest);
        assert_eq!(read_archive(&file).unwrap().1, blocks);
        let mut bytes = fs::read(&file).unwrap();
        *bytes.last_mut().unwrap() ^= 1;
        fs::write(&file, bytes).unwrap();
        assert!(matches!(
            read_archive(&file),
            Err(ArchiveError::Invalid("piece checksum"))
        ));
    }

    #[test]
    fn streaming_verifier_checks_complete_records_with_bounded_memory() {
        let dir = TempDir::new().unwrap();
        let file = dir.path().join("streaming.rblk");
        let blocks = vec![vec![1], vec![2; 70_000], Vec::new()];
        let manifest = write_archive(&file, 100, &blocks).unwrap();
        assert_eq!(verify_archive_streaming(&file).unwrap(), manifest);

        let mut bytes = fs::read(&file).unwrap();
        *bytes.last_mut().unwrap() ^= 1;
        fs::write(&file, bytes).unwrap();
        assert!(matches!(
            verify_archive_streaming(file),
            Err(ArchiveError::Invalid("piece checksum"))
        ));
    }

    #[test]
    fn streaming_block_hash_verifier_decodes_each_consensus_record_once() {
        let dir = TempDir::new().unwrap();
        let file = dir.path().join("block-hashes.rblk");
        let block = bitcoin::blockdata::constants::genesis_block(bitcoin::Network::Regtest);
        let blocks = vec![bitcoin::consensus::serialize(&block); 3];
        let manifest = write_archive(&file, 10, &blocks).unwrap();
        let (actual, hashes, records_bytes) = verify_archive_block_hashes_streaming(&file).unwrap();
        assert_eq!(actual, manifest);
        assert_eq!(hashes, vec![block.block_hash(); 3]);
        assert_eq!(records_bytes, manifest.records_bytes);

        let arbitrary = dir.path().join("not-blocks.rblk");
        write_archive(&arbitrary, 10, &[vec![1, 2, 3]]).unwrap();
        assert!(matches!(
            verify_archive_block_hashes_streaming(arbitrary),
            Err(ArchiveError::Invalid("block consensus encoding"))
        ));
    }

    #[test]
    fn fragmented_record_stream_verification_preserves_framing() {
        let one = 1_u32.to_le_bytes();
        let zero = 0_u32.to_le_bytes();
        let records = [one.as_slice(), &[7], zero.as_slice()].concat();
        let mut verifier = RecordStreamVerifier::default();
        for byte in records {
            verifier.feed(&[byte]).unwrap();
        }
        verifier.finish(2).unwrap();

        let mut truncated = RecordStreamVerifier::default();
        truncated.feed(&[2, 0, 0, 0, 1]).unwrap();
        assert!(matches!(
            truncated.finish(1),
            Err(ArchiveError::Invalid("block length"))
        ));
    }

    #[test]
    fn bounds_manifest_records_and_individual_blocks_before_allocation() {
        let dir = TempDir::new().unwrap();
        let file = dir.path().join("bounded.rblk");
        let blocks = vec![vec![7; 300]];
        write_archive(&file, 1, &blocks).unwrap();
        let bytes = fs::read(&file).unwrap();
        let metadata_len =
            usize::try_from(u32::from_le_bytes(bytes[8..12].try_into().unwrap())).unwrap();
        let payload_offset = 12 + metadata_len;
        let mut manifest: ArchiveManifest =
            serde_json::from_slice(&bytes[12..payload_offset]).unwrap();
        manifest.records_bytes = 4;
        let metadata = serde_json::to_vec(&manifest).unwrap();
        let mut bounded = Vec::new();
        bounded.extend_from_slice(MAGIC);
        bounded.extend_from_slice(&u32::try_from(metadata.len()).unwrap().to_le_bytes());
        bounded.extend_from_slice(&metadata);
        bounded.extend_from_slice(&bytes[payload_offset..]);
        assert!(matches!(
            decode_archive(&bounded),
            Err(ArchiveError::Invalid("records too large"))
        ));
        assert!(matches!(
            write_archive(&file, 1, &[vec![0; MAX_BLOCK_BYTES + 1]]),
            Err(ArchiveError::Invalid("block too large"))
        ));
    }

    #[test]
    fn reports_the_longest_prefix_within_the_record_budget() {
        let record_bytes = u64::try_from(MAX_BLOCK_BYTES).unwrap() + 4;
        let fitting_blocks = usize::try_from(MAX_RECORDS_BYTES / record_bytes).unwrap();
        let lengths = vec![MAX_BLOCK_BYTES; fitting_blocks + 1];
        assert_eq!(
            bounded_archive_prefix_len_from_lengths(lengths).unwrap(),
            fitting_blocks
        );
        assert_eq!(
            bounded_archive_prefix_len_from_lengths([1, 2, 3]).unwrap(),
            3
        );
        assert!(matches!(
            bounded_archive_prefix_len_from_lengths([MAX_BLOCK_BYTES + 1]),
            Err(ArchiveError::Invalid("block too large"))
        ));
    }

    #[test]
    fn reads_legacy_v1_archives_with_a_derived_decompression_ceiling() {
        let dir = TempDir::new().unwrap();
        let file = dir.path().join("legacy.rblk");
        let blocks = vec![vec![1, 2, 3]];
        write_archive(&file, 9, &blocks).unwrap();
        let bytes = fs::read(file).unwrap();
        let metadata_len =
            usize::try_from(u32::from_le_bytes(bytes[8..12].try_into().unwrap())).unwrap();
        let payload_offset = 12 + metadata_len;
        let mut manifest: ArchiveManifest =
            serde_json::from_slice(&bytes[12..payload_offset]).unwrap();
        manifest.format_version = LEGACY_FORMAT_VERSION;
        manifest.records_bytes = 0;
        let metadata = serde_json::to_vec(&manifest).unwrap();
        let mut legacy = Vec::new();
        legacy.extend_from_slice(MAGIC);
        legacy.extend_from_slice(&u32::try_from(metadata.len()).unwrap().to_le_bytes());
        legacy.extend_from_slice(&metadata);
        legacy.extend_from_slice(&bytes[payload_offset..]);
        assert_eq!(decode_archive(&legacy).unwrap().1, blocks);
    }

    #[test]
    fn rejects_zstd_windows_larger_than_the_authenticated_record_budget() {
        let records = [3_u8, 0, 0, 0, 1, 2, 3];
        let mut encoder = zstd::stream::Encoder::new(Vec::new(), 1).unwrap();
        encoder.window_log(27).unwrap();
        encoder.include_contentsize(false).unwrap();
        encoder.write_all(&records).unwrap();
        let compressed = encoder.finish().unwrap();
        let manifest = ArchiveManifest {
            format_version: FORMAT_VERSION,
            first_height: 1,
            block_count: 1,
            records_bytes: u64::try_from(records.len()).unwrap(),
            records_sha256: hash_hex(&records),
            piece_size: PIECE_SIZE,
            piece_sha256: vec![hash_hex(&compressed)],
        };
        let metadata = serde_json::to_vec(&manifest).unwrap();
        let mut archive = Vec::new();
        archive.extend_from_slice(MAGIC);
        archive.extend_from_slice(&u32::try_from(metadata.len()).unwrap().to_le_bytes());
        archive.extend_from_slice(&metadata);
        archive.extend_from_slice(&compressed);

        assert!(matches!(decode_archive(&archive), Err(ArchiveError::Io(_))));
    }

    #[test]
    fn rejects_non_ascii_manifest_digests_without_panicking() {
        // Twenty-one three-byte characters plus one ASCII byte is 64 bytes long
        // but has no character boundary at byte offset two.
        let multibyte = "\u{20ac}".repeat(21) + "0";
        assert_eq!(multibyte.len(), 64);
        assert_eq!(decode_sha256(&multibyte), None);
        assert_eq!(decode_sha256(&"g".repeat(64)), None);
        assert_eq!(decode_sha256("00"), None);
        assert_eq!(decode_sha256(&"aB".repeat(32)), Some([0xab; 32]));

        let manifest = serde_json::json!({
            "format_version": FORMAT_VERSION,
            "first_height": 0,
            "block_count": 1,
            "records_bytes": 8,
            "records_sha256": multibyte,
            "piece_size": PIECE_SIZE,
            "piece_sha256": ["00".repeat(32)],
        });
        let metadata = serde_json::to_vec(&manifest).unwrap();
        let mut archive = MAGIC.to_vec();
        archive.extend_from_slice(&u32::try_from(metadata.len()).unwrap().to_le_bytes());
        archive.extend_from_slice(&metadata);
        assert!(matches!(
            decode_archive(&archive),
            Err(ArchiveError::Invalid("manifest fields"))
        ));
        assert!(matches!(
            read_archive_manifest_from_bytes(&archive),
            Err(ArchiveError::Invalid("manifest fields"))
        ));
    }

    fn read_archive_manifest_from_bytes(bytes: &[u8]) -> Result<ArchiveManifest, ArchiveError> {
        let dir = TempDir::new().unwrap();
        let file = dir.path().join("manifest.rblk");
        fs::write(&file, bytes).unwrap();
        read_archive_manifest(file)
    }
}
