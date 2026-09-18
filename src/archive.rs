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
// Fixed-schema JSON needs fewer than 512 + 261 * 67 bytes.
const GENERATED_MANIFEST_BYTES: usize = 32 * 1024;
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
    Io(std::io::Error),
    /// Metadata parse failure.
    #[error("manifest: {0}")]
    Manifest(#[from] serde_json::Error),
    /// Invalid immutable archive.
    #[error("invalid block archive: {0}")]
    Invalid(&'static str),
}

impl From<std::io::Error> for ArchiveError {
    fn from(error: std::io::Error) -> Self {
        if rbtc_codec_memory::is_admission_error(&error)
            || crate::node_memory::reservation_kind(&error).is_some()
        {
            Self::ResourceBudget(error)
        } else {
            Self::Io(error)
        }
    }
}

impl ArchiveError {
    pub(crate) fn reservation_kind(&self) -> Option<crate::node_memory::ReservationKind> {
        let Self::ResourceBudget(error) = self else {
            return None;
        };
        if rbtc_codec_memory::is_admission_error(error) {
            Some(crate::node_memory::ReservationKind::Memory)
        } else {
            crate::node_memory::reservation_kind(error)
        }
    }
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

/// Immutable block handles with shared, admitted storage for the handle array.
#[derive(Clone, Debug, Default)]
pub struct ArchiveBlocks(Option<Arc<ArchiveBlocksStorage>>);

#[derive(Debug)]
struct ArchiveBlocksStorage {
    blocks: Vec<ArchiveBlock>,
    _reservation: Option<crate::node_memory::MemoryLease>,
}

impl std::ops::Deref for ArchiveBlocks {
    type Target = [ArchiveBlock];
    fn deref(&self) -> &[ArchiveBlock] {
        self.0
            .as_ref()
            .map_or(&[], |storage| storage.blocks.as_slice())
    }
}
impl PartialEq for ArchiveBlocks {
    fn eq(&self, other: &Self) -> bool {
        **self == **other
    }
}
impl Eq for ArchiveBlocks {}
impl PartialEq<Vec<Vec<u8>>> for ArchiveBlocks {
    fn eq(&self, other: &Vec<Vec<u8>>) -> bool {
        **self == *other
    }
}
impl<const N: usize> PartialEq<[Vec<u8>; N]> for ArchiveBlocks {
    fn eq(&self, other: &[Vec<u8>; N]) -> bool {
        **self == *other
    }
}

/// Iterator retaining handle storage until it is dropped; yielded blocks retain
/// their independent payload admission after the iterator is gone.
pub struct ArchiveBlocksIter {
    blocks: ArchiveBlocks,
    offset: usize,
}
impl Iterator for ArchiveBlocksIter {
    type Item = ArchiveBlock;
    fn next(&mut self) -> Option<Self::Item> {
        let block = self.blocks.get(self.offset)?.clone();
        self.offset += 1;
        Some(block)
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.blocks.len() - self.offset;
        (remaining, Some(remaining))
    }
}
impl ExactSizeIterator for ArchiveBlocksIter {}
impl IntoIterator for ArchiveBlocks {
    type Item = ArchiveBlock;
    type IntoIter = ArchiveBlocksIter;
    fn into_iter(self) -> Self::IntoIter {
        ArchiveBlocksIter {
            blocks: self,
            offset: 0,
        }
    }
}
impl<'a> IntoIterator for &'a ArchiveBlocks {
    type Item = &'a ArchiveBlock;
    type IntoIter = std::slice::Iter<'a, ArchiveBlock>;
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

pub(crate) struct ArchiveBlocksBuilder {
    blocks: Vec<ArchiveBlock>,
    capacity: usize,
    reservation: Option<crate::node_memory::MemoryLease>,
}
impl ArchiveBlocksBuilder {
    fn allocation_bytes(capacity: usize) -> Result<u64, ArchiveError> {
        if capacity == 0 {
            return Ok(0);
        }
        capacity
            .checked_mul(std::mem::size_of::<ArchiveBlock>())
            .and_then(|bytes| {
                bytes.checked_add(
                    std::mem::size_of::<ArchiveBlocksStorage>() + 2 * std::mem::size_of::<usize>(),
                )
            })
            .and_then(|bytes| u64::try_from(bytes).ok())
            .ok_or(ArchiveError::Invalid("archive handle capacity overflow"))
    }
    pub(crate) fn new(path: &Path, capacity: usize) -> Result<Self, ArchiveError> {
        let reservation = if capacity == 0 {
            None
        } else {
            reserve_archive_memory(path, Self::allocation_bytes(capacity)?)?
        };
        Ok(Self {
            blocks: Vec::with_capacity(capacity),
            capacity,
            reservation,
        })
    }
    pub(crate) fn len(&self) -> usize {
        self.blocks.len()
    }
    pub(crate) fn is_empty(&self) -> bool {
        self.blocks.is_empty()
    }
    pub(crate) fn push(&mut self, block: ArchiveBlock) -> Result<(), ArchiveError> {
        if self.blocks.len() == self.capacity {
            return Err(ArchiveError::Invalid("archive handle capacity exhausted"));
        }
        self.blocks.push(block);
        Ok(())
    }
    pub(crate) fn finish(self) -> ArchiveBlocks {
        if self.capacity == 0 {
            return ArchiveBlocks::default();
        }
        ArchiveBlocks(Some(Arc::new(ArchiveBlocksStorage {
            blocks: self.blocks,
            _reservation: self.reservation,
        })))
    }
}

/// Sidecar-equivalent data needed by a BitTorrent/webseed transport.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ArchiveManifestFields {
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
    #[serde(deserialize_with = "deserialize_digest")]
    pub records_sha256: String,
    /// Fixed transfer piece size.
    pub piece_size: usize,
    /// SHA-256 digest of each compressed transfer piece.
    #[serde(deserialize_with = "deserialize_pieces")]
    pub piece_sha256: Vec<String>,
}

/// Immutable archive identity; clones share metadata and its admission owner.
#[derive(Clone, Debug)]
pub struct ArchiveManifest(Arc<ManifestStorage>);
#[derive(Debug)]
struct ManifestStorage {
    fields: ArchiveManifestFields,
    _reservation: Option<crate::node_memory::MemoryLease>,
}
impl ArchiveManifest {
    fn admitted(
        fields: ArchiveManifestFields,
        reservation: Option<crate::node_memory::MemoryLease>,
    ) -> Self {
        Self(Arc::new(ManifestStorage {
            fields,
            _reservation: reservation,
        }))
    }
    #[cfg(test)]
    fn fields_mut(&mut self) -> &mut ArchiveManifestFields {
        *self = self.0.fields.clone().into();
        &mut Arc::get_mut(&mut self.0).unwrap().fields
    }
}
impl From<ArchiveManifestFields> for ArchiveManifest {
    /// Wraps caller-owned fields without admitting their existing allocations.
    fn from(fields: ArchiveManifestFields) -> Self {
        Self::admitted(fields, None)
    }
}
impl std::ops::Deref for ArchiveManifest {
    type Target = ArchiveManifestFields;
    fn deref(&self) -> &Self::Target {
        &self.0.fields
    }
}
impl PartialEq for ArchiveManifest {
    fn eq(&self, other: &Self) -> bool {
        self.0.fields == other.0.fields
    }
}
impl Eq for ArchiveManifest {}
impl Serialize for ArchiveManifest {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.0.fields.serialize(serializer)
    }
}
impl<'de> Deserialize<'de> for ArchiveManifest {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        ArchiveManifestFields::deserialize(deserializer).map(Into::into)
    }
}
// Fixed result capacity, separate from the JSON parser's temporary scratch.
const MANIFEST_MEMORY_BYTES: u64 = (std::mem::size_of::<ManifestStorage>()
    + 2 * std::mem::size_of::<usize>()
    + MAX_PIECES * (std::mem::size_of::<String>() + 64)
    + 64) as u64;
fn deserialize_digest<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<String, D::Error> {
    struct DigestVisitor;
    impl serde::de::Visitor<'_> for DigestVisitor {
        type Value = String;
        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("a digest no longer than 64 bytes")
        }
        fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<String, E> {
            if value.len() > 64 {
                return Err(E::custom("archive digest too long"));
            }
            Ok(value.to_owned())
        }
    }
    deserializer.deserialize_str(DigestVisitor)
}
fn deserialize_pieces<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Vec<String>, D::Error> {
    struct BoundedDigest(String);
    impl<'de> Deserialize<'de> for BoundedDigest {
        fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
            deserialize_digest(deserializer).map(Self)
        }
    }
    struct PiecesVisitor;
    impl<'de> serde::de::Visitor<'de> for PiecesVisitor {
        type Value = Vec<String>;
        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("a bounded archive piece list")
        }
        fn visit_seq<A: serde::de::SeqAccess<'de>>(
            self,
            mut sequence: A,
        ) -> Result<Self::Value, A::Error> {
            let mut pieces = Vec::with_capacity(MAX_PIECES);
            while pieces.len() < MAX_PIECES {
                let Some(BoundedDigest(digest)) = sequence.next_element()? else {
                    return Ok(pieces);
                };
                pieces.push(digest);
            }
            if sequence.next_element::<serde::de::IgnoredAny>()?.is_some() {
                return Err(serde::de::Error::custom("too many archive pieces"));
            }
            Ok(pieces)
        }
    }
    deserializer.deserialize_seq(PiecesVisitor)
}

struct ManifestWriteAdmission {
    result: Option<crate::node_memory::MemoryLease>,
    metadata: ManifestWriteBuffer,
    piece: PieceScratch,
}
impl ManifestWriteAdmission {
    fn new(path: &Path) -> Result<Self, ArchiveError> {
        let result = reserve_archive_memory(path, MANIFEST_MEMORY_BYTES)?;
        let reservation = reserve_archive_memory(path, GENERATED_MANIFEST_BYTES as u64)?;
        let piece = PieceScratch::new(path)?;
        Ok(Self {
            result,
            metadata: ManifestWriteBuffer {
                bytes: Vec::with_capacity(GENERATED_MANIFEST_BYTES),
                _reservation: reservation,
            },
            piece,
        })
    }
}
struct ManifestWriteBuffer {
    bytes: Vec<u8>,
    _reservation: Option<crate::node_memory::MemoryLease>,
}
impl Write for ManifestWriteBuffer {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() > GENERATED_MANIFEST_BYTES - self.bytes.len() {
            return Err(std::io::Error::other(
                "generated archive manifest limit exceeded",
            ));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
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
    let admission = ManifestWriteAdmission::new(path)?;
    let parent = path
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let temporary = BoundedArchiveFile {
        file: tempfile::tempfile_in(parent)?,
        remaining: MAX_CONTAINER_BYTES,
    };
    let (temporary, records_sha256) = compress_archive(
        temporary,
        blocks,
        records_bytes,
        crate::node_memory::for_path(path)?,
    )?;
    finish_archive_file(
        path,
        first_height,
        u32::try_from(blocks.len()).expect("validated block count"),
        records_bytes,
        records_sha256,
        temporary.file,
        admission,
    )
}

fn finish_archive_file(
    path: &Path,
    first_height: u32,
    block_count: u32,
    records_bytes: u64,
    records_sha256: String,
    mut compressed: File,
    mut admission: ManifestWriteAdmission,
) -> Result<ArchiveManifest, ArchiveError> {
    compressed.seek(SeekFrom::Start(0))?;
    let mut piece_sha256 = Vec::with_capacity(MAX_PIECES);
    for _ in 0..MAX_PIECES {
        let Some(hash) = admission.piece.next_hash(&mut compressed)? else {
            break;
        };
        piece_sha256.push(hash);
    }
    if compressed.read(&mut [0])? != 0 {
        return Err(ArchiveError::Invalid("too many archive pieces"));
    }
    let fields = ArchiveManifestFields {
        format_version: FORMAT_VERSION,
        first_height,
        block_count,
        records_bytes,
        records_sha256,
        piece_size: PIECE_SIZE,
        piece_sha256,
    };
    let manifest = ArchiveManifest::admitted(fields, admission.result);
    serde_json::to_writer(&mut admission.metadata, &manifest)?;
    let metadata = &admission.metadata.bytes;
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
    output.write_all(metadata)?;
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
        None,
    )?;
    let manifest: ArchiveManifest = ArchiveManifestFields {
        format_version: FORMAT_VERSION,
        first_height,
        block_count: u32::try_from(blocks.len())
            .map_err(|_| ArchiveError::Invalid("too many blocks"))?,
        records_bytes,
        records_sha256,
        piece_size: PIECE_SIZE,
        piece_sha256: compressed.chunks(PIECE_SIZE).map(hash_hex).collect(),
    }
    .into();
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
    memory: Option<crate::node_memory::MemoryBudget>,
) -> Result<(W, String), ArchiveError> {
    let mut encoder = archive_encoder(output, records_bytes, memory)?;
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

struct NativeArchiveBudget(crate::node_memory::MemoryBudget);
impl rbtc_codec_memory::AllocationBudget for NativeArchiveBudget {
    type Lease = crate::node_memory::MemoryLease;
    fn reserve(&self, bytes: usize) -> Option<Self::Lease> {
        self.0.try_reserve(u64::try_from(bytes).ok()?)
    }
}
enum ArchiveEncoder<W: Write> {
    Unbound(zstd::stream::Encoder<'static, W>),
    Admitted(rbtc_codec_memory::Encoder<W, NativeArchiveBudget>),
}
impl<W: Write> ArchiveEncoder<W> {
    fn finish(self) -> Result<W, ArchiveError> {
        match self {
            Self::Unbound(encoder) => encoder.finish().map_err(Into::into),
            Self::Admitted(encoder) => encoder.finish().map_err(Into::into),
        }
    }
}
impl<W: Write> Write for ArchiveEncoder<W> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        match self {
            Self::Unbound(encoder) => encoder.write(bytes),
            Self::Admitted(encoder) => encoder.write(bytes),
        }
    }
    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            Self::Unbound(encoder) => encoder.flush(),
            Self::Admitted(encoder) => encoder.flush(),
        }
    }
}

fn archive_encoder<W: Write>(
    output: W,
    records_bytes: u64,
    memory: Option<crate::node_memory::MemoryBudget>,
) -> Result<ArchiveEncoder<W>, ArchiveError> {
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
    if let Some(memory) = memory {
        let workers = if workers > 1 {
            u32::try_from(workers).expect("bounded workers")
        } else {
            0
        };
        return Ok(ArchiveEncoder::Admitted(rbtc_codec_memory::Encoder::new(
            output,
            NativeArchiveBudget(memory),
            ARCHIVE_COMPRESSION_LEVEL,
            workers,
        )?));
    }
    let mut encoder = zstd::stream::Encoder::new(output, ARCHIVE_COMPRESSION_LEVEL)?;
    if workers > 1 {
        encoder.multithread(u32::try_from(workers).expect("compression worker bound fits u32"))?;
    }
    Ok(ArchiveEncoder::Unbound(encoder))
}

/// Re-encodes a verified prefix without retaining the source blocks. The
/// destination is opened only after both complete source verification passes.
pub(crate) fn write_archive_prefix(
    source: &Path,
    expected: &ArchiveManifest,
    count: u32,
    destination: &Path,
) -> Result<ArchiveManifest, ArchiveError> {
    write_archive_range(source, expected, expected.first_height, count, destination)
}

/// Re-encodes a contiguous range with bounded scratch and one selected record
/// at a time. Both passes verify the complete source, including skipped records.
/// This produces a file only; publication and recovery belong to the ledger.
pub(crate) fn write_archive_range(
    source: &Path,
    expected: &ArchiveManifest,
    first_height: u32,
    count: u32,
    destination: &Path,
) -> Result<ArchiveManifest, ArchiveError> {
    let offset = first_height
        .checked_sub(expected.first_height)
        .ok_or(ArchiveError::Invalid("archive range start"))?;
    if count == 0
        || offset
            .checked_add(count)
            .is_none_or(|end| end > expected.block_count)
    {
        return Err(ArchiveError::Invalid("archive range count"));
    }
    let visit_range = |visit: &mut dyn FnMut(u32, &[u8]) -> bool| {
        scan_archive_selection(
            source,
            Some(expected),
            first_height,
            count,
            MAX_RECORDS_BYTES,
            Some(visit),
        )
        .map(|(_, _, complete)| complete)
    };
    let mut records_bytes = 0_u64;
    visit_range(&mut |_, block| {
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
    let admission = ManifestWriteAdmission::new(destination)?;
    let parent = destination
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let temporary = BoundedArchiveFile {
        file: tempfile::tempfile_in(parent)?,
        remaining: MAX_CONTAINER_BYTES,
    };
    let mut encoder = archive_encoder(
        temporary,
        records_bytes,
        crate::node_memory::for_path(destination)?,
    )?;
    let mut digest = Sha256::new();
    let mut failure = None;
    visit_range(&mut |_, block| {
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
        first_height,
        count,
        records_bytes,
        crate::utxo::hex_lower(&digest.finalize()),
        temporary.file,
        admission,
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
) -> Result<(ArchiveManifest, ArchiveBlocks), ArchiveError> {
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
) -> Result<(ArchiveManifest, ArchiveBlocks, bool), ArchiveError> {
    let path = path.as_ref();
    let mut file = File::open(path)?;
    if file.metadata()?.len() > MAX_CONTAINER_BYTES {
        return Err(ArchiveError::Invalid("archive too large"));
    }
    let (manifest, records_limit, payload_offset) = read_manifest_header_from(path, &mut file)?;
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
    let capacity = if visit.is_some() || first_height < manifest.first_height {
        0
    } else {
        let available = manifest
            .block_count
            .saturating_sub(first_height - manifest.first_height);
        usize::try_from(u64::from(max_blocks.min(available)).min(max_record_bytes / 4))
            .expect("validated archive count fits usize")
    };
    let mut blocks = ArchiveBlocksBuilder::new(path, capacity)?;
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
                blocks.push(ArchiveBlock::admitted(block, block_reservation))?;
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
    Ok((manifest, blocks.finish(), complete))
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
    let (manifest, _, payload_offset) = read_manifest_header_from(path, &mut file)?;
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
    read_manifest_header_from(path, &mut file)
}

fn read_manifest_header_from(
    path: &Path,
    file: &mut File,
) -> Result<(ArchiveManifest, u64, u64), ArchiveError> {
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
    // Source bytes, parser scratch and bounded error formatting, including
    // old/new growth overlap. This is conservative, not an RSS estimate.
    let _parse_reservation = reserve_archive_memory(path, 8 * metadata_len as u64 + 64 * 1024)?;
    let result_reservation = reserve_archive_memory(path, MANIFEST_MEMORY_BYTES)?;
    let mut metadata = vec![0_u8; metadata_len];
    file.read_exact(&mut metadata)?;
    let fields: ArchiveManifestFields = serde_json::from_slice(&metadata)?;
    let manifest = ArchiveManifest::admitted(fields, result_reservation);
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
    read_manifest_header(path.as_ref()).map(|(manifest, _, _)| manifest)
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
    fn native_encoder_accounts_workers_and_refunds_each_injected_allocation_failure() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        struct FailingBudget {
            memory: crate::node_memory::MemoryBudget,
            attempts: Arc<AtomicUsize>,
            fail_at: usize,
        }
        impl rbtc_codec_memory::AllocationBudget for FailingBudget {
            type Lease = crate::node_memory::MemoryLease;
            fn reserve(&self, bytes: usize) -> Option<Self::Lease> {
                if self.attempts.fetch_add(1, Ordering::Relaxed) == self.fail_at {
                    return None;
                }
                self.memory.try_reserve(u64::try_from(bytes).ok()?)
            }
        }
        let mut input = vec![0; 1024 * 1024];
        let mut random = 7_u32;
        for byte in &mut input {
            random ^= random << 13;
            random ^= random >> 17;
            random ^= random << 5;
            *byte = random.to_le_bytes()[0];
        }
        for workers in [0, 2, 3, 4] {
            let memory = crate::node_memory::MemoryBudget::new(256 * 1024 * 1024);
            let attempts = Arc::new(AtomicUsize::new(0));
            let run = |fail_at| -> std::io::Result<Vec<u8>> {
                let budget = FailingBudget {
                    memory: memory.clone(),
                    attempts: attempts.clone(),
                    fail_at,
                };
                let mut encoder = rbtc_codec_memory::Encoder::new(Vec::new(), budget, 1, workers)?;
                for _ in 0..20 {
                    encoder.write_all(&input)?;
                }
                encoder.finish()
            };
            let encoded = run(usize::MAX).unwrap();
            let allocations = attempts.load(Ordering::Relaxed);
            assert!(allocations > if workers == 0 { 3 } else { 10 });
            assert!(memory.snapshot().peak > 1024 * 1024);
            assert!(memory.snapshot().peak <= memory.snapshot().limit);
            assert_eq!(memory.snapshot().used, 0);
            let mut reference = zstd::stream::Encoder::new(Vec::new(), 1).unwrap();
            if workers != 0 {
                reference.multithread(workers).unwrap();
            }
            for _ in 0..20 {
                reference.write_all(&input).unwrap();
            }
            // Streaming buffer boundaries can produce different valid frames.
            // Verify every decoded byte, not compressed-byte determinism or
            // merely the decoded length. Small file-format comparisons remain
            // covered separately by the archive writer tests.
            for compressed in [encoded, reference.finish().unwrap()] {
                let decoded = zstd::decode_all(compressed.as_slice()).unwrap();
                assert_eq!(decoded.len(), 20 * input.len());
                assert!(
                    decoded
                        .chunks_exact(input.len())
                        .all(|chunk| chunk == input)
                );
            }
            // Fail each observed allocation position, including worker-pool and
            // job-buffer construction. Scheduling may reorder later attempts.
            for fail_at in 0..allocations {
                attempts.store(0, Ordering::Relaxed);
                match run(fail_at) {
                    Err(error) => assert!(rbtc_codec_memory::is_admission_error(&error)),
                    Ok(_) => assert!(attempts.load(Ordering::Relaxed) <= fail_at),
                }
                assert_eq!(
                    memory.snapshot().used,
                    0,
                    "allocation {fail_at} leaked admission"
                );
            }
        }
        assert!(matches!(
            ArchiveError::from(std::io::Error::other("ordinary writer failure")),
            ArchiveError::Io(_)
        ));
    }

    #[test]
    fn native_encoder_releases_admission_on_writer_failure_and_early_drop() {
        struct FailingWriter;
        impl Write for FailingWriter {
            fn write(&mut self, _: &[u8]) -> std::io::Result<usize> {
                Err(std::io::Error::other("injected output failure"))
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let memory = crate::node_memory::MemoryBudget::new(64 * 1024 * 1024);
        {
            let mut encoder = rbtc_codec_memory::Encoder::new(
                Vec::new(),
                NativeArchiveBudget(memory.clone()),
                1,
                2,
            )
            .unwrap();
            encoder.write_all(&vec![1; 1024 * 1024]).unwrap();
            assert!(memory.snapshot().used > 0);
        }
        assert_eq!(memory.snapshot().used, 0);
        let mut encoder = rbtc_codec_memory::Encoder::new(
            FailingWriter,
            NativeArchiveBudget(memory.clone()),
            1,
            2,
        )
        .unwrap();
        let result = encoder
            .write_all(&[1, 2, 3])
            .and_then(|()| encoder.finish().map(|_| ()));
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("injected output failure")
        );
        assert_eq!(memory.snapshot().used, 0);
    }

    #[test]
    fn generated_manifest_admission_preserves_outputs_and_survives_prefix_aliases() {
        let dir = TempDir::new().unwrap();
        let source = dir.path().join("source.rblk");
        let target = dir.path().join("target.rblk");
        let source_manifest = write_archive(&source, 1, &[vec![7], vec![8]]).unwrap();
        fs::write(&target, b"preserve this destination").unwrap();
        let budget = crate::node_memory::MemoryBudget::new(16 * 1024 * 1024);
        budget.bind(&[dir.path().to_path_buf()]).unwrap();
        let peak =
            MANIFEST_MEMORY_BYTES + GENERATED_MANIFEST_BYTES as u64 + PIECE_SCRATCH_BYTES as u64;
        let pressure = budget.reserve(budget.snapshot().limit - peak + 1).unwrap();
        assert!(matches!(
            write_archive(&target, 3, &[vec![9]]),
            Err(ArchiveError::ResourceBudget(_))
        ));
        assert_eq!(fs::read(&target).unwrap(), b"preserve this destination");
        assert_eq!(budget.spool_snapshot().used, 0);
        assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 2);
        drop(pressure);
        assert_eq!(budget.snapshot().used, 0);
        // Metadata fits exactly; native encoder admission must fail as a
        // local resource error without truncating the existing destination.
        let pressure = budget.reserve(budget.snapshot().limit - peak).unwrap();
        assert!(matches!(
            write_archive(&target, 3, &[vec![9]]),
            Err(ArchiveError::ResourceBudget(_))
        ));
        assert_eq!(fs::read(&target).unwrap(), b"preserve this destination");
        assert_eq!(budget.spool_snapshot().used, 0);
        assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 2);
        assert_eq!(budget.snapshot().used, budget.snapshot().limit - peak);
        drop(pressure);
        let manifest = write_archive(&target, 3, &[vec![9]]).unwrap();
        assert_eq!(budget.snapshot().used, MANIFEST_MEMORY_BYTES);
        let alias = manifest.clone();
        drop(manifest);
        assert_eq!(budget.snapshot().used, MANIFEST_MEMORY_BYTES);
        assert_eq!(alias, encode_archive(3, &[vec![9]]).unwrap().0);
        drop(alias);
        assert_eq!(budget.snapshot().used, 0);
        let prefix = write_archive_prefix(&source, &source_manifest, 1, &target).unwrap();
        assert_eq!(budget.snapshot().used, MANIFEST_MEMORY_BYTES);
        assert_eq!(
            fs::read(&target).unwrap(),
            encode_archive(1, &[vec![7]]).unwrap().1
        );
        let alias = prefix.clone();
        drop(prefix);
        assert_eq!(budget.snapshot().used, MANIFEST_MEMORY_BYTES);
        drop(alias);
        assert_eq!(budget.snapshot().used, 0);
        assert_eq!(budget.spool_snapshot().used, 0);
    }

    #[test]
    fn generated_manifest_json_stays_within_fixed_capacity() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("maximum.rblk");
        let budget = crate::node_memory::MemoryBudget::new(1024 * 1024);
        budget.bind(&[dir.path().to_path_buf()]).unwrap();
        let mut admission = ManifestWriteAdmission::new(&path).unwrap();
        let fields = ArchiveManifestFields {
            format_version: u16::MAX,
            first_height: u32::MAX,
            block_count: u32::MAX,
            records_bytes: u64::MAX,
            records_sha256: "f".repeat(64),
            piece_size: usize::MAX,
            piece_sha256: vec!["f".repeat(64); MAX_PIECES],
        };
        serde_json::to_writer(&mut admission.metadata, &fields).unwrap();
        assert!(admission.metadata.bytes.len() < 512 + MAX_PIECES * 67);
        assert_eq!(
            admission.metadata.bytes,
            serde_json::to_vec(&fields).unwrap()
        );
        assert_eq!(
            admission.metadata.bytes.capacity(),
            GENERATED_MANIFEST_BYTES
        );
        let length = admission.metadata.bytes.len();
        assert!(
            admission
                .metadata
                .write_all(&vec![0; GENERATED_MANIFEST_BYTES].into_boxed_slice())
                .is_err()
        );
        assert_eq!(admission.metadata.bytes.len(), length);
        assert_eq!(
            admission.metadata.bytes.capacity(),
            GENERATED_MANIFEST_BYTES
        );
        drop(fields);
        drop(admission);
        assert_eq!(budget.snapshot().used, 0);
    }

    #[test]
    fn file_manifest_admission_survives_aliases_and_preserves_wire_identity() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("manifest.rblk");
        let expected = write_archive(&path, 1, &[vec![4; 30]]).unwrap();
        let original = fs::read(&path).unwrap();
        let json_bytes = u64::from(u32::from_le_bytes(original[8..12].try_into().unwrap()));
        let peak = 8 * json_bytes + 64 * 1024 + MANIFEST_MEMORY_BYTES;
        let budget = crate::node_memory::MemoryBudget::new(peak);
        budget.bind(&[dir.path().to_path_buf()]).unwrap();
        let pressure = budget.reserve(1).unwrap();
        assert!(matches!(
            read_archive_manifest(&path),
            Err(ArchiveError::ResourceBudget(_))
        ));
        assert_eq!(budget.snapshot().used, 1);
        drop(pressure);
        let manifest = read_archive_manifest(&path).unwrap();
        assert_eq!(manifest, expected);
        assert_eq!(budget.snapshot().used, MANIFEST_MEMORY_BYTES);
        let alias = manifest.clone();
        assert_eq!(
            manifest.records_sha256.as_ptr(),
            alias.records_sha256.as_ptr()
        );
        assert_eq!(manifest.piece_sha256.as_ptr(), alias.piece_sha256.as_ptr());
        assert_eq!(
            serde_json::to_vec(&manifest).unwrap(),
            serde_json::to_vec(&expected).unwrap()
        );
        drop(manifest);
        assert_eq!(budget.snapshot().used, MANIFEST_MEMORY_BYTES);
        drop(alias);
        assert_eq!(budget.snapshot().used, 0);
        assert_eq!(fs::read(&path).unwrap(), original);
    }

    #[test]
    fn manifest_parser_bounds_digest_outputs_and_piece_arrays_before_validation() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("malicious.rblk");
        let manifest = write_archive(&path, 1, &[vec![4]]).unwrap();
        let budget = crate::node_memory::MemoryBudget::new(16 * 1024 * 1024);
        budget.bind(&[dir.path().to_path_buf()]).unwrap();
        let valid = serde_json::to_value(&manifest).unwrap();
        let mut excessive_pieces = valid.clone();
        excessive_pieces["piece_sha256"] = serde_json::json!(vec!["00".repeat(32); MAX_PIECES + 1]);
        let mut excessive_digest = valid.clone();
        excessive_digest["records_sha256"] = serde_json::json!("0".repeat(128 * 1024));
        for (value, reason) in [
            (excessive_pieces, "too many archive pieces"),
            (excessive_digest, "archive digest too long"),
        ] {
            let metadata = serde_json::to_vec(&value).unwrap();
            let mut bytes = MAGIC.to_vec();
            bytes.extend_from_slice(&u32::try_from(metadata.len()).unwrap().to_le_bytes());
            bytes.extend_from_slice(&metadata);
            fs::write(&path, bytes).unwrap();
            let error = read_archive_manifest(&path).unwrap_err();
            assert!(matches!(error, ArchiveError::Manifest(_)));
            assert!(error.to_string().contains(reason));
            assert_eq!(budget.snapshot().used, 0);
        }
        let metadata = serde_json::to_string(&valid).unwrap();
        let escaped = metadata.replace(&manifest.records_sha256, &"\\u0030".repeat(64));
        let decoded: ArchiveManifest = serde_json::from_str(&escaped).unwrap();
        assert_eq!(decoded.records_sha256, "0".repeat(64));
        // Oversized declared input is rejected by admission before any missing
        // JSON bytes are read or allocated.
        let mut header = MAGIC.to_vec();
        header.extend_from_slice(&u32::try_from(MAX_MANIFEST_SIZE).unwrap().to_le_bytes());
        fs::write(&path, header).unwrap();
        assert!(matches!(
            read_archive_manifest(&path),
            Err(ArchiveError::ResourceBudget(_))
        ));
        assert_eq!(budget.snapshot().used, 0);
    }

    #[test]
    fn handle_storage_is_admitted_shared_and_retained_by_partial_iterators() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("handles.rblk");
        let bytes = ArchiveBlocksBuilder::allocation_bytes(2).unwrap();
        let budget = crate::node_memory::MemoryBudget::new(bytes);
        budget.bind(&[dir.path().to_path_buf()]).unwrap();
        let pressure = budget.reserve(1).unwrap();
        assert!(matches!(
            ArchiveBlocksBuilder::new(&path, 2),
            Err(ArchiveError::ResourceBudget(_))
        ));
        assert!(ArchiveBlocksBuilder::allocation_bytes(usize::MAX).is_err());
        drop(pressure);
        let mut builder = ArchiveBlocksBuilder::new(&path, 2).unwrap();
        assert_eq!(budget.snapshot().used, bytes);
        builder.push(vec![1].into()).unwrap();
        builder.push(vec![2].into()).unwrap();
        assert!(builder.push(vec![3].into()).is_err());
        let blocks = builder.finish();
        let alias = blocks.clone();
        assert_eq!(blocks.as_ptr(), alias.as_ptr());
        assert_eq!(budget.snapshot().used, bytes);
        let mut iterator = blocks.into_iter();
        let escaped = iterator.next().unwrap();
        assert_eq!(iterator.len(), 1);
        drop(alias);
        assert_eq!(budget.snapshot().used, bytes);
        drop(iterator);
        assert_eq!(budget.snapshot().used, 0);
        assert_eq!(escaped.as_ref(), &[1]);
        let empty = ArchiveBlocksBuilder::new(&path, 0).unwrap().finish();
        assert!(empty.is_empty());
        assert_eq!(budget.snapshot().used, 0);
        assert!(empty.into_iter().next().is_none());
    }

    #[test]
    fn returned_block_clones_keep_admission_through_writing_and_final_drop() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("owned.rblk");
        let data = vec![3; 100_000];
        let manifest = write_archive(&path, 1, std::slice::from_ref(&data)).unwrap();
        let native = ArchiveDecoder::allowance(manifest.records_bytes).unwrap();
        let payload = ArchiveBlock::allocation_bytes(data.len());
        let handles = ArchiveBlocksBuilder::allocation_bytes(1).unwrap();
        let limit = native + 64 * 1024 + payload + handles + MANIFEST_MEMORY_BYTES;
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
        assert_eq!(budget.snapshot().used, payload + handles);
        let alias = blocks[0].clone();
        assert_eq!(alias.as_ptr(), blocks[0].as_ptr());
        let cloned_batch = blocks.clone();
        drop(blocks);
        assert_eq!(budget.snapshot().used, payload + handles);
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
        let limit = native + 64 * 1024 + 70_000 + MANIFEST_MEMORY_BYTES;
        let budget = crate::node_memory::MemoryBudget::new(limit);
        budget.bind(&[dir.path().to_path_buf()]).unwrap();
        // Native context fits, but the scan buffer cannot be allocated.
        let pressure = budget
            .reserve(limit - native - MANIFEST_MEMORY_BYTES)
            .unwrap();
        assert!(matches!(
            verify_archive_streaming(&path),
            Err(ArchiveError::ResourceBudget(_))
        ));
        assert_eq!(
            budget.snapshot().used,
            limit - native - MANIFEST_MEMORY_BYTES
        );
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
        let handles = ArchiveBlocksBuilder::allocation_bytes(1).unwrap();
        let total = allowance + 64 * 1024 + payload_allowance + handles + MANIFEST_MEMORY_BYTES;
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
                assert_eq!(
                    budget.snapshot().used,
                    allowance + 64 * 1024 + 512 * 1024 + MANIFEST_MEMORY_BYTES
                );
                assert!(
                    budget
                        .reserve(payload_allowance - 512 * 1024 + handles + 1)
                        .is_err()
                );
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
        let budget = crate::node_memory::MemoryBudget::new(16 * 1024 * 1024);
        budget.bind(&[dir.path().to_path_buf()]).unwrap();
        let occupied = budget
            .reserve(budget.snapshot().limit - PIECE_SCRATCH_BYTES as u64 + 1)
            .unwrap();
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
        let occupied = budget
            .reserve(budget.snapshot().limit - PIECE_SCRATCH_BYTES as u64)
            .unwrap();
        let mut scratch = PieceScratch::new(&path).unwrap();
        assert_eq!(budget.snapshot().used, budget.snapshot().limit);
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
        drop(occupied);
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
    fn streamed_ranges_preserve_heights_and_validate_skipped_records() {
        let dir = TempDir::new().unwrap();
        let source = dir.path().join("source.rblk");
        let target = dir.path().join("range.rblk");
        let blocks = [vec![1; 256 * 1024], vec![2; 1000], vec![3; 4000]];
        let original = write_archive(&source, 10, &blocks).unwrap();
        let source_bytes = fs::read(&source).unwrap();
        for (start, count) in [(10, 3), (11, 1), (11, 2), (12, 1)] {
            let manifest = write_archive_range(&source, &original, start, count, &target).unwrap();
            let (decoded, selected) = read_archive(&target).unwrap();
            assert_eq!(decoded, manifest);
            assert_eq!(manifest.first_height, start);
            assert_eq!(manifest.block_count, count);
            let offset = (start - 10) as usize;
            assert_eq!(&selected[..], &blocks[offset..offset + count as usize]);
            assert_eq!(fs::read(&source).unwrap(), source_bytes);
        }
        let saved = fs::read(&target).unwrap();
        for (start, count) in [(9, 1), (10, 0), (11, 3), (13, 1), (12, u32::MAX)] {
            assert!(write_archive_range(&source, &original, start, count, &target).is_err());
            assert_eq!(fs::read(&target).unwrap(), saved);
        }
        // Valid compressed-piece hashes do not suffice: the full record digest
        // must still be checked when both the prefix and suffix are skipped.
        let mut invalid = original.clone();
        invalid.fields_mut().records_sha256 = "00".repeat(32);
        let metadata = serde_json::to_vec(&invalid).unwrap();
        let offset = 12 + u32::from_le_bytes(source_bytes[8..12].try_into().unwrap()) as usize;
        let mut changed = MAGIC.to_vec();
        changed.extend_from_slice(&u32::try_from(metadata.len()).unwrap().to_le_bytes());
        changed.extend_from_slice(&metadata);
        changed.extend_from_slice(&source_bytes[offset..]);
        fs::write(&source, &changed).unwrap();
        assert!(matches!(
            write_archive_range(&source, &original, 11, 1, &target),
            Err(ArchiveError::Invalid("archive identity changed"))
        ));
        assert!(matches!(
            write_archive_range(&source, &invalid, 11, 1, &target),
            Err(ArchiveError::Invalid("records checksum"))
        ));
        assert_eq!(fs::read(&target).unwrap(), saved);
        assert_eq!(fs::read(&source).unwrap(), changed);
        assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 2);
    }

    #[test]
    fn streamed_suffix_refunds_memory_and_spool_denials_without_replacing_files() {
        let dir = TempDir::new().unwrap();
        let source = dir.path().join("source.rblk");
        let target = dir.path().join("suffix.rblk");
        let original = write_archive(&source, 10, &[vec![1; 300_000], vec![2; 1000]]).unwrap();
        let source_bytes = fs::read(&source).unwrap();
        fs::write(&target, b"preserve").unwrap();
        let budget = crate::node_memory::MemoryBudget::new(64 * 1024 * 1024);
        budget.bind(&[dir.path().to_path_buf()]).unwrap();
        let pressure = budget.reserve(budget.snapshot().limit).unwrap();
        let error = write_archive_range(&source, &original, 11, 1, &target).unwrap_err();
        assert_eq!(
            error.reservation_kind(),
            Some(crate::node_memory::ReservationKind::Memory)
        );
        drop(pressure);
        assert_eq!(budget.snapshot().used, 0);
        let pressure = budget
            .reserve_spool(budget.spool_snapshot().limit - MAX_CONTAINER_BYTES + 1)
            .unwrap();
        let error = write_archive_range(&source, &original, 11, 1, &target).unwrap_err();
        assert_eq!(
            error.reservation_kind(),
            Some(crate::node_memory::ReservationKind::ExecutionSpool)
        );
        assert_eq!(budget.snapshot().used, 0);
        drop(pressure);
        assert_eq!(budget.spool_snapshot().used, 0);
        assert_eq!(fs::read(&target).unwrap(), b"preserve");
        assert_eq!(fs::read(&source).unwrap(), source_bytes);
        let suffix = write_archive_range(&source, &original, 11, 1, &target).unwrap();
        assert_eq!(suffix.first_height, 11);
        assert_eq!(read_archive(&target).unwrap().1, vec![vec![2; 1000]]);
        assert_eq!(budget.snapshot().used, MANIFEST_MEMORY_BYTES);
        drop(suffix);
        assert_eq!(budget.snapshot().used, 0);
        assert_eq!(budget.spool_snapshot().used, 0);
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
        let budget = crate::node_memory::MemoryBudget::new(16 * 1024 * 1024);
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
        invalid.fields_mut().records_sha256 = "00".repeat(32);
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
        manifest.fields_mut().records_bytes = 4;
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
        manifest.fields_mut().format_version = LEGACY_FORMAT_VERSION;
        manifest.fields_mut().records_bytes = 0;
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
        let manifest: ArchiveManifest = ArchiveManifestFields {
            format_version: FORMAT_VERSION,
            first_height: 1,
            block_count: 1,
            records_bytes: u64::try_from(records.len()).unwrap(),
            records_sha256: hash_hex(&records),
            piece_size: PIECE_SIZE,
            piece_sha256: vec![hash_hex(&compressed)],
        }
        .into();
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
