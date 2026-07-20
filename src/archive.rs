//! Immutable zstd block archives suitable for piece-addressed distribution.

use std::{
    fs,
    io::{Cursor, Read},
    path::Path,
};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

const MAGIC: &[u8; 8] = b"RBTCBLK1";
const PIECE_SIZE: usize = 4 * 1024 * 1024;

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
    let mut records = Vec::new();
    for block in blocks {
        let len =
            u32::try_from(block.len()).map_err(|_| ArchiveError::Invalid("block too large"))?;
        records.extend_from_slice(&len.to_le_bytes());
        records.extend_from_slice(block);
    }
    let compressed = zstd::stream::encode_all(Cursor::new(&records), 9)?;
    let manifest = ArchiveManifest {
        format_version: 1,
        first_height,
        block_count: u32::try_from(blocks.len())
            .map_err(|_| ArchiveError::Invalid("too many blocks"))?,
        records_sha256: hash_hex(&records),
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
    fs::write(path, output)?;
    Ok(manifest)
}

/// Checks archive pieces and returns the original consensus-serialized blocks.
pub fn read_archive(
    path: impl AsRef<Path>,
) -> Result<(ArchiveManifest, Vec<Vec<u8>>), ArchiveError> {
    let file = fs::read(path)?;
    if file.len() < 12 || &file[..8] != MAGIC {
        return Err(ArchiveError::Invalid("magic"));
    }
    let metadata_len = u32::from_le_bytes(file[8..12].try_into().expect("checked header"));
    let metadata_len = usize::try_from(metadata_len).expect("u32 fits usize");
    let start = 12_usize
        .checked_add(metadata_len)
        .ok_or(ArchiveError::Invalid("manifest length"))?;
    if start > file.len() {
        return Err(ArchiveError::Invalid("manifest length"));
    }
    let manifest: ArchiveManifest = serde_json::from_slice(&file[12..start])?;
    if manifest.format_version != 1 || manifest.piece_size != PIECE_SIZE {
        return Err(ArchiveError::Invalid("manifest version"));
    }
    let compressed = &file[start..];
    let actual_pieces = compressed
        .chunks(PIECE_SIZE)
        .map(hash_hex)
        .collect::<Vec<_>>();
    if actual_pieces != manifest.piece_sha256 {
        return Err(ArchiveError::Invalid("piece checksum"));
    }
    let mut records = Vec::new();
    zstd::stream::Decoder::new(Cursor::new(compressed))?.read_to_end(&mut records)?;
    if hash_hex(&records) != manifest.records_sha256 {
        return Err(ArchiveError::Invalid("records checksum"));
    }
    let mut records = records.as_slice();
    let mut blocks = Vec::with_capacity(manifest.block_count as usize);
    while !records.is_empty() {
        if records.len() < 4 {
            return Err(ArchiveError::Invalid("block length"));
        }
        let len = u32::from_le_bytes(records[..4].try_into().expect("checked length"));
        let len = usize::try_from(len).expect("u32 fits usize");
        if records.len() < 4 + len {
            return Err(ArchiveError::Invalid("block length"));
        }
        blocks.push(records[4..4 + len].to_vec());
        records = &records[4 + len..];
    }
    if blocks.len() != manifest.block_count as usize {
        return Err(ArchiveError::Invalid("block count"));
    }
    Ok((manifest, blocks))
}

fn hash_hex(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
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
        assert_eq!(manifest.block_count, 2);
        assert_eq!(read_archive(&file).unwrap().1, blocks);
        let mut bytes = fs::read(&file).unwrap();
        *bytes.last_mut().unwrap() ^= 1;
        fs::write(&file, bytes).unwrap();
        assert!(matches!(
            read_archive(&file),
            Err(ArchiveError::Invalid("piece checksum"))
        ));
    }
}
