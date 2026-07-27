//! Immutable zstd block archives suitable for piece-addressed distribution.

use std::{
    fs::{self, File},
    io::{Cursor, Read, Seek, SeekFrom, Write},
    path::Path,
};

use bitcoin::{Block, BlockHash, consensus::deserialize};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

const MAGIC: &[u8; 8] = b"RBTCBLK1";
const FORMAT_VERSION: u16 = 2;
const LEGACY_FORMAT_VERSION: u16 = 1;
const PIECE_SIZE: usize = 4 * 1024 * 1024;
const MAX_MANIFEST_SIZE: usize = 16 * 1024 * 1024;
const MAX_BLOCK_BYTES: usize = 4_000_000;
const MAX_BLOCKS_PER_ARCHIVE: u32 = 100_000;
const MAX_RECORDS_BYTES: u64 = 1024 * 1024 * 1024;
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
pub fn bounded_archive_prefix_len(blocks: &[Vec<u8>]) -> Result<usize, ArchiveError> {
    bounded_archive_prefix_len_from_lengths(blocks.iter().map(Vec::len))
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
    blocks: &[Vec<u8>],
) -> Result<ArchiveManifest, ArchiveError> {
    let (manifest, output) = encode_archive(first_height, blocks)?;
    fs::write(path, output)?;
    Ok(manifest)
}

/// Encodes a self-verifying archive for file, BitTorrent, or webseed publication.
pub fn encode_archive(
    first_height: u32,
    blocks: &[Vec<u8>],
) -> Result<(ArchiveManifest, Vec<u8>), ArchiveError> {
    if blocks.is_empty() {
        return Err(ArchiveError::Invalid("empty archive"));
    }
    if blocks.len() > usize::try_from(MAX_BLOCKS_PER_ARCHIVE).expect("u32 fits usize") {
        return Err(ArchiveError::Invalid("too many blocks"));
    }
    let mut records_bytes = 0_u64;
    for block in blocks {
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
    let compressed_capacity =
        usize::try_from(records_bytes).expect("bounded record length fits usize");
    let mut encoder = zstd::stream::Encoder::new(
        Vec::with_capacity(compressed_capacity),
        ARCHIVE_COMPRESSION_LEVEL,
    )?;
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
    let mut records_hash = Sha256::new();
    for block in blocks {
        let len =
            u32::try_from(block.len()).map_err(|_| ArchiveError::Invalid("block too large"))?;
        let len = len.to_le_bytes();
        records_hash.update(len);
        records_hash.update(block);
        encoder.write_all(&len)?;
        encoder.write_all(block)?;
    }
    let compressed = encoder.finish()?;
    let manifest = ArchiveManifest {
        format_version: FORMAT_VERSION,
        first_height,
        block_count: u32::try_from(blocks.len())
            .map_err(|_| ArchiveError::Invalid("too many blocks"))?,
        records_bytes,
        records_sha256: format!("{:x}", records_hash.finalize()),
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
    let mut file = File::open(path)?;
    if file.metadata()?.len() > MAX_CONTAINER_BYTES {
        return Err(ArchiveError::Invalid("archive too large"));
    }
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)?;
    let (manifest, _, _) = verify_archive_container(&bytes)?;
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
    let mut decoder = zstd::stream::Decoder::new(file)?;
    decoder.window_log_max(zstd_window_log(records_limit))?;
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
    if format!("{:x}", digest.finalize()) != manifest.records_sha256 {
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

fn verify_compressed_pieces(
    path: &Path,
    payload_offset: u64,
    manifest: &ArchiveManifest,
) -> Result<(), ArchiveError> {
    let mut file = File::open(path)?;
    file.seek(SeekFrom::Start(payload_offset))?;
    let mut piece = vec![0_u8; PIECE_SIZE];
    let mut piece_index = 0_usize;
    loop {
        let mut filled = 0_usize;
        while filled < piece.len() {
            let read = file.read(&mut piece[filled..])?;
            if read == 0 {
                break;
            }
            filled += read;
        }
        if filled == 0 {
            break;
        }
        let actual = hash_hex(&piece[..filled]);
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
    let mut decoder = zstd::stream::Decoder::new(file)?;
    decoder.window_log_max(zstd_window_log(records_limit))?;
    let mut bounded = decoder.take(records_limit.saturating_add(1));
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
    if format!("{:x}", digest.finalize()) != manifest.records_sha256 {
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
    format!("{:x}", Sha256::digest(bytes))
}

fn decode_sha256(value: &str) -> Option<[u8; 32]> {
    if value.len() != 64 {
        return None;
    }
    let mut digest = [0_u8; 32];
    for (index, byte) in digest.iter_mut().enumerate() {
        let offset = index * 2;
        *byte = u8::from_str_radix(&value[offset..offset + 2], 16).ok()?;
    }
    Some(digest)
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
}
