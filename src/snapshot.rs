//! Self-verifying, zstd-compressed UTXO snapshot files.
//!
//! These files are an rBTC interchange format, not Bitcoin Core's `coins` dump
//! format. Importers must still anchor the manifest's block hash in their fully
//! validated header chain and run background validation from genesis.

use std::{
    collections::BTreeMap,
    fs,
    io::{Cursor, Read},
    path::Path,
};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::utxo::{OutPointKey, Utxo, UtxoError, UtxoStore};

const MAGIC: &[u8; 8] = b"RBTCUTXO";
const VERSION: u16 = 1;

/// Snapshot import and export failures.
#[derive(Debug, Error)]
pub enum SnapshotError {
    /// Filesystem access failed.
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    /// The metadata cannot be parsed.
    #[error("manifest: {0}")]
    Manifest(#[from] serde_json::Error),
    /// Chainstate access failed.
    #[error("utxo: {0}")]
    Utxo(#[from] UtxoError),
    /// The snapshot does not obey the rBTC snapshot format.
    #[error("invalid snapshot: {0}")]
    Invalid(&'static str),
    /// The caller's validated header anchor does not match the snapshot.
    #[error("snapshot anchor does not match validated chain")]
    AnchorMismatch,
}

/// Metadata stored before the compressed records and covered by the record hash.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SnapshotManifest {
    /// Container format version.
    pub format_version: u16,
    /// Bitcoin network name, such as `bitcoin` or `testnet`.
    pub network: String,
    /// Height of the header-chain anchor.
    pub height: u32,
    /// Display-order block hash of the anchor.
    pub block_hash: String,
    /// Number of serialized UTXOs.
    pub utxo_count: u64,
    /// SHA-256 of the uncompressed canonical entry stream, hex encoded.
    pub records_sha256: String,
    /// Compression algorithm used for the entry stream.
    pub compression: String,
}

/// A verified snapshot, ready to be promoted only after header-anchor validation.
#[derive(Debug)]
pub struct VerifiedSnapshot {
    /// Parsed manifest.
    pub manifest: SnapshotManifest,
    entries: BTreeMap<OutPointKey, Utxo>,
}

impl VerifiedSnapshot {
    /// Populates a caller-provided staging store only after anchor verification.
    ///
    /// A daemon must keep its existing chainstate active, validate historical
    /// blocks in the background, and promote this staging store only when that
    /// validation reaches the manifest anchor.
    pub fn install_into<S: UtxoStore>(
        self,
        store: &S,
        expected_block_hash: &str,
        now: u64,
        hot_window_secs: u64,
    ) -> Result<SnapshotManifest, SnapshotError> {
        if self.manifest.block_hash != expected_block_hash {
            return Err(SnapshotError::AnchorMismatch);
        }
        store.replace_all(&self.entries, now, hot_window_secs)?;
        Ok(self.manifest)
    }
}

/// Writes a deterministic compressed snapshot of the store at a known block anchor.
pub fn export_snapshot<S: UtxoStore>(
    store: &S,
    path: impl AsRef<Path>,
    network: impl Into<String>,
    height: u32,
    block_hash: impl Into<String>,
) -> Result<SnapshotManifest, SnapshotError> {
    let entries = store.snapshot_entries()?;
    let records = encode_entries(&entries)?;
    let manifest = SnapshotManifest {
        format_version: VERSION,
        network: network.into(),
        height,
        block_hash: block_hash.into(),
        utxo_count: u64::try_from(entries.len())
            .map_err(|_| SnapshotError::Invalid("too many UTXOs"))?,
        records_sha256: hex_hash(&records),
        compression: "zstd".to_owned(),
    };
    let manifest_bytes = serde_json::to_vec(&manifest)?;
    let manifest_len = u32::try_from(manifest_bytes.len())
        .map_err(|_| SnapshotError::Invalid("manifest too large"))?;
    let compressed = zstd::stream::encode_all(Cursor::new(records), 9)?;
    let mut output = Vec::with_capacity(14 + manifest_bytes.len() + compressed.len());
    output.extend_from_slice(MAGIC);
    output.extend_from_slice(&VERSION.to_le_bytes());
    output.extend_from_slice(&manifest_len.to_le_bytes());
    output.extend_from_slice(&manifest_bytes);
    output.extend_from_slice(&compressed);
    fs::write(path, output)?;
    Ok(manifest)
}

/// Parses and cryptographically checks an rBTC snapshot without mutating chainstate.
pub fn verify_snapshot(path: impl AsRef<Path>) -> Result<VerifiedSnapshot, SnapshotError> {
    let file = fs::read(path)?;
    if file.len() < 14 || &file[..8] != MAGIC {
        return Err(SnapshotError::Invalid("magic"));
    }
    let version = u16::from_le_bytes(file[8..10].try_into().expect("checked header"));
    if version != VERSION {
        return Err(SnapshotError::Invalid("unsupported version"));
    }
    let manifest_len = u32::from_le_bytes(file[10..14].try_into().expect("checked header"));
    let manifest_len = usize::try_from(manifest_len).expect("u32 fits usize");
    let payload_start = 14_usize
        .checked_add(manifest_len)
        .ok_or(SnapshotError::Invalid("manifest length"))?;
    if payload_start > file.len() {
        return Err(SnapshotError::Invalid("manifest length"));
    }
    let manifest: SnapshotManifest = serde_json::from_slice(&file[14..payload_start])?;
    if manifest.format_version != VERSION || manifest.compression != "zstd" {
        return Err(SnapshotError::Invalid("manifest format"));
    }
    let mut records = Vec::new();
    zstd::stream::Decoder::new(Cursor::new(&file[payload_start..]))?.read_to_end(&mut records)?;
    if hex_hash(&records) != manifest.records_sha256 {
        return Err(SnapshotError::Invalid("records checksum"));
    }
    let entries = decode_entries(&records)?;
    if u64::try_from(entries.len()).ok() != Some(manifest.utxo_count) {
        return Err(SnapshotError::Invalid("UTXO count"));
    }
    Ok(VerifiedSnapshot { manifest, entries })
}

fn encode_entries(entries: &BTreeMap<OutPointKey, Utxo>) -> Result<Vec<u8>, SnapshotError> {
    let mut records = Vec::new();
    for (key, utxo) in entries {
        let script_len = u32::try_from(utxo.script_pubkey.len())
            .map_err(|_| SnapshotError::Invalid("script too large"))?;
        records.extend_from_slice(key.as_bytes());
        records.extend_from_slice(&utxo.value_sats.to_le_bytes());
        records.extend_from_slice(&utxo.height.to_le_bytes());
        records.push(u8::from(utxo.is_coinbase));
        records.extend_from_slice(&utxo.last_touched.to_le_bytes());
        records.extend_from_slice(&script_len.to_le_bytes());
        records.extend_from_slice(&utxo.script_pubkey);
    }
    Ok(records)
}

fn decode_entries(mut records: &[u8]) -> Result<BTreeMap<OutPointKey, Utxo>, SnapshotError> {
    let mut entries = BTreeMap::new();
    while !records.is_empty() {
        if records.len() < 61 {
            return Err(SnapshotError::Invalid("entry header"));
        }
        let key = OutPointKey::from_bytes(&records[..36])?;
        let value_sats = u64::from_le_bytes(records[36..44].try_into().expect("checked header"));
        let height = u32::from_le_bytes(records[44..48].try_into().expect("checked header"));
        let is_coinbase = match records[48] {
            0 => false,
            1 => true,
            _ => return Err(SnapshotError::Invalid("coinbase flag")),
        };
        let last_touched = u64::from_le_bytes(records[49..57].try_into().expect("checked header"));
        let script_len = u32::from_le_bytes(records[57..61].try_into().expect("checked header"));
        let script_len = usize::try_from(script_len).expect("u32 fits usize");
        let entry_len = 61_usize
            .checked_add(script_len)
            .ok_or(SnapshotError::Invalid("script length"))?;
        if records.len() < entry_len {
            return Err(SnapshotError::Invalid("script length"));
        }
        let utxo = Utxo {
            value_sats,
            height,
            is_coinbase,
            last_touched,
            script_pubkey: records[61..entry_len].to_vec(),
        };
        if entries.insert(key, utxo).is_some() {
            return Err(SnapshotError::Invalid("duplicate outpoint"));
        }
        records = &records[entry_len..];
    }
    Ok(entries)
}

fn hex_hash(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utxo::{RedbUtxoStore, Utxo};
    use bitcoin::{OutPoint, Txid, hashes::Hash};
    use tempfile::TempDir;

    #[test]
    fn snapshot_roundtrip_requires_anchor_and_preserves_tiers() {
        let source_dir = TempDir::new().unwrap();
        let source = RedbUtxoStore::open(source_dir.path().join("source.redb")).unwrap();
        let key = OutPointKey::from(OutPoint::new(Txid::from_byte_array([9; 32]), 3));
        source
            .apply(
                &[],
                &[(
                    key,
                    Utxo {
                        value_sats: 21,
                        height: 5,
                        is_coinbase: false,
                        last_touched: 1,
                        script_pubkey: vec![0x51],
                    },
                )],
            )
            .unwrap();
        let snapshot_file = source_dir.path().join("snapshot.rbtc");
        let manifest = export_snapshot(&source, &snapshot_file, "regtest", 5, "anchor").unwrap();
        assert_eq!(manifest.utxo_count, 1);
        let destination_dir = TempDir::new().unwrap();
        let destination =
            RedbUtxoStore::open(destination_dir.path().join("destination.redb")).unwrap();
        let verified = verify_snapshot(&snapshot_file).unwrap();
        assert!(matches!(
            verified.install_into(&destination, "other", 100, 60),
            Err(SnapshotError::AnchorMismatch)
        ));
        verify_snapshot(&snapshot_file)
            .unwrap()
            .install_into(&destination, "anchor", 100, 60)
            .unwrap();
        assert_eq!(destination.get(key).unwrap().unwrap().value_sats, 21);
        assert_eq!(destination.tier_stats().unwrap().cold, 1);
    }

    #[test]
    fn rejects_tampered_snapshot() {
        let dir = TempDir::new().unwrap();
        let store = RedbUtxoStore::open(dir.path().join("db.redb")).unwrap();
        let snapshot_file = dir.path().join("snapshot.rbtc");
        export_snapshot(&store, &snapshot_file, "regtest", 0, "anchor").unwrap();
        let mut bytes = fs::read(&snapshot_file).unwrap();
        *bytes.last_mut().unwrap() ^= 1;
        fs::write(&snapshot_file, bytes).unwrap();
        assert!(verify_snapshot(&snapshot_file).is_err());
    }
}
