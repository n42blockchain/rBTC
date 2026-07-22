//! Self-verifying, zstd-compressed UTXO snapshot files.
//!
//! These files are an rBTC interchange format, not Bitcoin Core's `coins` dump
//! format. Importers must still anchor the manifest's block hash in their fully
//! validated header chain and run background validation from genesis.

use std::{
    collections::BTreeMap,
    fs::{self, File},
    io::{BufReader, Cursor, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use bitcoin::{BlockHash, Network};

use crate::{
    chain_store::{ChainStoreError, RedbChainStore, SnapshotContentIdentity},
    execution_store::ExecutionTip,
    headers::HeaderDag,
    utxo::{OutPointKey, Utxo, UtxoError, UtxoStore},
};

const MAGIC: &[u8; 8] = b"RBTCUTXO";
const VERSION: u16 = 3;
const CONTAINER_HEADER_LEN: usize = 14;
const MAX_MANIFEST_BYTES: usize = 64 * 1024;
const MAX_SCRIPT_PUBKEY_BYTES: usize = 10_000;

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
    /// Unified chainstate activation failed.
    #[error("chainstate: {0}")]
    ChainStore(#[from] ChainStoreError),
    /// The snapshot does not obey the rBTC snapshot format.
    #[error("invalid snapshot: {0}")]
    Invalid(&'static str),
    /// The caller's validated header anchor does not match the snapshot.
    #[error("snapshot anchor does not match validated chain")]
    AnchorMismatch,
    /// The snapshot was created for a different Bitcoin network.
    #[error("snapshot network does not match selected network")]
    NetworkMismatch,
    /// The manifest does not match the independently supplied trust anchor.
    #[error("snapshot manifest does not match trusted identity")]
    TrustMismatch,
}

/// Independently distributed identity required before an assumed snapshot can run.
///
/// A checksum copied from the snapshot's own manifest is not a trust anchor. The
/// operator must obtain the base, count, canonical byte length, and digest from
/// release metadata or another authenticated channel, analogous to Bitcoin
/// Core's compiled AssumeUTXO parameters.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotTrustAnchor {
    network: Network,
    height: u32,
    block_hash: BlockHash,
    utxo_count: u64,
    records_bytes: u64,
    records_sha256: String,
}

impl SnapshotTrustAnchor {
    /// Constructs a trusted snapshot identity from authenticated metadata.
    pub fn new(
        network: Network,
        height: u32,
        block_hash: BlockHash,
        utxo_count: u64,
        records_bytes: u64,
        records_sha256: impl Into<String>,
    ) -> Result<Self, SnapshotError> {
        let records_sha256 = records_sha256.into();
        let minimum_bytes = utxo_count.checked_mul(65);
        let maximum_bytes = utxo_count.checked_mul(
            u64::try_from(65 + MAX_SCRIPT_PUBKEY_BYTES).expect("snapshot record bound fits u64"),
        );
        if height == 0
            || minimum_bytes.is_none_or(|minimum| records_bytes < minimum)
            || maximum_bytes.is_none_or(|maximum| records_bytes > maximum)
            || records_sha256.len() != 64
            || !records_sha256
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(SnapshotError::Invalid("trusted snapshot identity"));
        }
        Ok(Self {
            network,
            height,
            block_hash,
            utxo_count,
            records_bytes,
            records_sha256,
        })
    }
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
    /// Number of bytes in the canonical uncompressed entry stream.
    pub records_bytes: u64,
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
    path: PathBuf,
    payload_offset: u64,
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
        let mut entries = BTreeMap::new();
        let mut records = Sha256::new();
        let mut count = 0_u64;
        let mut records_bytes = 0_u64;
        for entry in self.entries()? {
            let (key, utxo) = entry?;
            records.update(key.as_bytes());
            let encoded = utxo.encode()?;
            records.update(&encoded);
            entries.insert(key, utxo);
            count = count
                .checked_add(1)
                .ok_or(SnapshotError::Invalid("UTXO count overflow"))?;
            records_bytes = records_bytes
                .checked_add(u64::try_from(36 + encoded.len()).expect("record length fits u64"))
                .ok_or(SnapshotError::Invalid("records length overflow"))?;
            if count > self.manifest.utxo_count || records_bytes > self.manifest.records_bytes {
                return Err(SnapshotError::Invalid(
                    "snapshot changed after verification",
                ));
            }
        }
        if count != self.manifest.utxo_count
            || records_bytes != self.manifest.records_bytes
            || format!("{:x}", records.finalize()) != self.manifest.records_sha256
        {
            return Err(SnapshotError::Invalid(
                "snapshot changed after verification",
            ));
        }
        store.replace_all(&entries, now, hot_window_secs)?;
        Ok(self.manifest)
    }

    /// Atomically starts a fresh unified chainstate at a trusted active-header anchor.
    ///
    /// This persists an assumed-state marker; successful block connection above
    /// the base must not be presented as independent genesis-to-tip validation.
    pub fn assume_into(
        self,
        store: &RedbChainStore,
        headers: &HeaderDag,
        trusted: &SnapshotTrustAnchor,
        now: u64,
        hot_window_secs: u64,
    ) -> Result<SnapshotManifest, SnapshotError> {
        if headers.network() != trusted.network {
            return Err(SnapshotError::NetworkMismatch);
        }
        validate_manifest_trust(&self.manifest, trusted)?;
        if headers
            .active_header_at(trusted.height)
            .is_none_or(|header| header.hash != trusted.block_hash)
        {
            return Err(SnapshotError::AnchorMismatch);
        }
        let records_sha256 = decode_sha256(&trusted.records_sha256)
            .expect("SnapshotTrustAnchor constructor validated the digest");
        let entries = self.entries()?;
        store.assume_snapshot_entries(
            ExecutionTip {
                height: trusted.height,
                hash: trusted.block_hash,
            },
            SnapshotContentIdentity {
                records_sha256,
                utxo_count: self.manifest.utxo_count,
                records_bytes: self.manifest.records_bytes,
            },
            entries,
            now,
            hot_window_secs,
        )?;
        Ok(self.manifest)
    }

    fn entries(&self) -> Result<SnapshotEntryReader, SnapshotError> {
        SnapshotEntryReader::open(&self.path, self.payload_offset)
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
        records_bytes: u64::try_from(records.len())
            .map_err(|_| SnapshotError::Invalid("records too large"))?,
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
    atomic_write(path.as_ref(), &output)?;
    Ok(manifest)
}

fn atomic_write(path: &Path, contents: &[u8]) -> Result<(), std::io::Error> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let name = path
        .file_name()
        .ok_or_else(|| std::io::Error::other("snapshot path has no file name"))?;
    let mut temporary = None;
    for _ in 0..16 {
        let candidate = parent.join(format!(
            ".{}.{}.{:016x}.tmp",
            name.to_string_lossy(),
            std::process::id(),
            rand::random::<u64>()
        ));
        match fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&candidate)
        {
            Ok(file) => {
                temporary = Some((candidate, file));
                break;
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error),
        }
    }
    let Some((temporary_path, mut file)) = temporary else {
        return Err(std::io::Error::new(
            std::io::ErrorKind::AlreadyExists,
            "could not allocate snapshot temporary file",
        ));
    };
    let result = (|| {
        file.write_all(contents)?;
        file.sync_all()?;
        drop(file);
        fs::rename(&temporary_path, path)?;
        #[cfg(unix)]
        File::open(parent)?.sync_all()?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(temporary_path);
    }
    result
}

/// Parses and cryptographically checks an rBTC snapshot without mutating chainstate.
pub fn verify_snapshot(path: impl AsRef<Path>) -> Result<VerifiedSnapshot, SnapshotError> {
    verify_snapshot_inner(path.as_ref(), None)
}

/// Checks authenticated manifest identity before decompressing untrusted records.
///
/// Node startup should prefer this entry point so a container for another base
/// cannot consume decompression resources before its trust mismatch is known.
pub fn verify_snapshot_with_trust(
    path: impl AsRef<Path>,
    trusted: &SnapshotTrustAnchor,
) -> Result<VerifiedSnapshot, SnapshotError> {
    verify_snapshot_inner(path.as_ref(), Some(trusted))
}

fn verify_snapshot_inner(
    path: &Path,
    trusted: Option<&SnapshotTrustAnchor>,
) -> Result<VerifiedSnapshot, SnapshotError> {
    let path = fs::canonicalize(path)?;
    let (manifest, payload_offset) = read_manifest(&path)?;
    if manifest.format_version != VERSION || manifest.compression != "zstd" {
        return Err(SnapshotError::Invalid("manifest format"));
    }
    if decode_sha256(&manifest.records_sha256).is_none() {
        return Err(SnapshotError::Invalid("records checksum encoding"));
    }
    if let Some(trusted) = trusted {
        validate_manifest_trust(&manifest, trusted)?;
    }
    let mut records = Sha256::new();
    let mut count = 0_u64;
    let mut records_bytes = 0_u64;
    for entry in SnapshotEntryReader::open(&path, payload_offset)? {
        let (key, utxo) = entry?;
        records.update(key.as_bytes());
        let encoded = utxo.encode()?;
        records.update(&encoded);
        count = count
            .checked_add(1)
            .ok_or(SnapshotError::Invalid("UTXO count overflow"))?;
        records_bytes = records_bytes
            .checked_add(u64::try_from(36 + encoded.len()).expect("record length fits u64"))
            .ok_or(SnapshotError::Invalid("records length overflow"))?;
        if count > manifest.utxo_count || records_bytes > manifest.records_bytes {
            return Err(SnapshotError::Invalid("records exceed manifest bounds"));
        }
    }
    if format!("{:x}", records.finalize()) != manifest.records_sha256 {
        return Err(SnapshotError::Invalid("records checksum"));
    }
    if count != manifest.utxo_count {
        return Err(SnapshotError::Invalid("UTXO count"));
    }
    if records_bytes != manifest.records_bytes {
        return Err(SnapshotError::Invalid("records length"));
    }
    Ok(VerifiedSnapshot {
        manifest,
        path,
        payload_offset,
    })
}

fn validate_manifest_trust(
    manifest: &SnapshotManifest,
    trusted: &SnapshotTrustAnchor,
) -> Result<(), SnapshotError> {
    if manifest.network != trusted.network.to_string() {
        return Err(SnapshotError::NetworkMismatch);
    }
    if manifest.height != trusted.height
        || manifest.block_hash != trusted.block_hash.to_string()
        || manifest.utxo_count != trusted.utxo_count
        || manifest.records_bytes != trusted.records_bytes
        || manifest.records_sha256 != trusted.records_sha256
    {
        return Err(SnapshotError::TrustMismatch);
    }
    Ok(())
}

fn read_manifest(path: &Path) -> Result<(SnapshotManifest, u64), SnapshotError> {
    let mut file = File::open(path)?;
    let mut header = [0_u8; CONTAINER_HEADER_LEN];
    read_exact_or_invalid(&mut file, &mut header, "container header")?;
    if &header[..8] != MAGIC {
        return Err(SnapshotError::Invalid("magic"));
    }
    let version = u16::from_le_bytes(header[8..10].try_into().expect("fixed header"));
    if version != VERSION {
        return Err(SnapshotError::Invalid("unsupported version"));
    }
    let manifest_len = u32::from_le_bytes(header[10..14].try_into().expect("fixed header"));
    let manifest_len = usize::try_from(manifest_len).expect("u32 fits usize");
    if manifest_len == 0 || manifest_len > MAX_MANIFEST_BYTES {
        return Err(SnapshotError::Invalid("manifest length"));
    }
    let mut manifest = vec![0_u8; manifest_len];
    read_exact_or_invalid(&mut file, &mut manifest, "manifest length")?;
    let payload_offset = u64::try_from(CONTAINER_HEADER_LEN + manifest_len)
        .expect("bounded manifest offset fits u64");
    Ok((serde_json::from_slice(&manifest)?, payload_offset))
}

fn read_exact_or_invalid(
    reader: &mut impl Read,
    buffer: &mut [u8],
    field: &'static str,
) -> Result<(), SnapshotError> {
    match reader.read_exact(buffer) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => {
            Err(SnapshotError::Invalid(field))
        }
        Err(error) => Err(error.into()),
    }
}

struct SnapshotEntryReader {
    decoder: zstd::stream::read::Decoder<'static, BufReader<File>>,
    previous: Option<OutPointKey>,
    finished: bool,
}

impl SnapshotEntryReader {
    fn open(path: &Path, payload_offset: u64) -> Result<Self, SnapshotError> {
        let mut file = File::open(path)?;
        file.seek(SeekFrom::Start(payload_offset))?;
        Ok(Self {
            decoder: zstd::stream::Decoder::new(file)?,
            previous: None,
            finished: false,
        })
    }

    fn next_entry(&mut self) -> Result<Option<(OutPointKey, Utxo)>, UtxoError> {
        let mut header = [0_u8; 65];
        match self.decoder.read(&mut header[..1])? {
            0 => return Ok(None),
            1 => {}
            _ => unreachable!("one-byte read"),
        }
        self.decoder.read_exact(&mut header[1..])?;
        let key = OutPointKey::from_bytes(&header[..36])?;
        if self.previous.is_some_and(|previous| key <= previous) {
            return Err(UtxoError::Malformed(
                "snapshot outpoints are not strictly ordered",
            ));
        }
        self.previous = Some(key);
        let value_sats = u64::from_le_bytes(header[36..44].try_into().expect("fixed header"));
        let height = u32::from_le_bytes(header[44..48].try_into().expect("fixed header"));
        let is_coinbase = match header[48] {
            0 => false,
            1 => true,
            _ => return Err(UtxoError::Malformed("snapshot coinbase flag")),
        };
        let last_touched = u64::from_le_bytes(header[49..57].try_into().expect("fixed header"));
        let creation_mtp = u32::from_le_bytes(header[57..61].try_into().expect("fixed header"));
        let script_len = u32::from_le_bytes(header[61..65].try_into().expect("fixed header"));
        let script_len = usize::try_from(script_len).expect("u32 fits usize");
        if script_len > MAX_SCRIPT_PUBKEY_BYTES {
            return Err(UtxoError::Malformed(
                "snapshot script exceeds consensus bound",
            ));
        }
        let mut script_pubkey = vec![0_u8; script_len];
        self.decoder.read_exact(&mut script_pubkey)?;
        Ok(Some((
            key,
            Utxo {
                value_sats,
                height,
                is_coinbase,
                last_touched,
                creation_mtp,
                script_pubkey,
            },
        )))
    }
}

impl Iterator for SnapshotEntryReader {
    type Item = Result<(OutPointKey, Utxo), UtxoError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }
        match self.next_entry() {
            Ok(Some(entry)) => Some(Ok(entry)),
            Ok(None) => {
                self.finished = true;
                None
            }
            Err(error) => {
                self.finished = true;
                Some(Err(error))
            }
        }
    }
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
        records.extend_from_slice(&utxo.creation_mtp.to_le_bytes());
        records.extend_from_slice(&script_len.to_le_bytes());
        records.extend_from_slice(&utxo.script_pubkey);
    }
    Ok(records)
}

fn hex_hash(bytes: &[u8]) -> String {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        block_execution::{BlockExecutionError, connect_active_block, disconnect_execution_tip},
        deployments::block_deployment_context,
        utxo::{RedbUtxoStore, Utxo},
    };
    use bitcoin::{
        Amount, Block, OutPoint, ScriptBuf, Sequence, Transaction, TxIn, TxMerkleNode, TxOut, Txid,
        Witness, absolute::LockTime, block::Header, block::Version as HeaderVersion, hashes::Hash,
        pow::Target, transaction::Version,
    };
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
                        creation_mtp: 0,
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

    #[test]
    fn rejects_oversized_manifest_and_script_before_unbounded_allocation() {
        let directory = TempDir::new().unwrap();
        let manifest_bomb = directory.path().join("manifest-bomb.rbtc");
        let mut bytes = Vec::new();
        bytes.extend_from_slice(MAGIC);
        bytes.extend_from_slice(&VERSION.to_le_bytes());
        bytes.extend_from_slice(&u32::try_from(MAX_MANIFEST_BYTES + 1).unwrap().to_le_bytes());
        fs::write(&manifest_bomb, bytes).unwrap();
        assert!(matches!(
            verify_snapshot(manifest_bomb),
            Err(SnapshotError::Invalid("manifest length"))
        ));

        let script_bomb = directory.path().join("script-bomb.rbtc");
        let mut record = vec![0_u8; 65];
        record[..36].copy_from_slice(
            OutPointKey::from(OutPoint::new(Txid::from_byte_array([3; 32]), 0)).as_bytes(),
        );
        record[61..65].copy_from_slice(
            &u32::try_from(MAX_SCRIPT_PUBKEY_BYTES + 1)
                .unwrap()
                .to_le_bytes(),
        );
        write_test_container(&script_bomb, &record, 1);
        assert!(matches!(
            verify_snapshot(script_bomb),
            Err(SnapshotError::Utxo(UtxoError::Malformed(
                "snapshot script exceeds consensus bound"
            )))
        ));
    }

    #[test]
    fn rejects_checksum_valid_but_noncanonical_outpoint_order() {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("unordered.rbtc");
        let mut records = Vec::new();
        for byte in [2, 1] {
            let key = OutPointKey::from(OutPoint::new(Txid::from_byte_array([byte; 32]), 0));
            let utxo = Utxo {
                value_sats: u64::from(byte),
                height: 1,
                is_coinbase: false,
                last_touched: 1,
                creation_mtp: 0,
                script_pubkey: vec![0x51],
            };
            records.extend_from_slice(key.as_bytes());
            records.extend_from_slice(&utxo.encode().unwrap());
        }
        write_test_container(&path, &records, 2);
        assert!(matches!(
            verify_snapshot(path),
            Err(SnapshotError::Utxo(UtxoError::Malformed(
                "snapshot outpoints are not strictly ordered"
            )))
        ));
    }

    #[test]
    fn trusted_verification_rejects_manifest_before_invalid_zstd_payload() {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("wrong-base.rbtc");
        let manifest = SnapshotManifest {
            format_version: VERSION,
            network: "regtest".to_owned(),
            height: 1,
            block_hash: BlockHash::all_zeros().to_string(),
            utxo_count: 0,
            records_bytes: 0,
            records_sha256: "00".repeat(32),
            compression: "zstd".to_owned(),
        };
        let manifest = serde_json::to_vec(&manifest).unwrap();
        let mut container = Vec::new();
        container.extend_from_slice(MAGIC);
        container.extend_from_slice(&VERSION.to_le_bytes());
        container.extend_from_slice(&u32::try_from(manifest.len()).unwrap().to_le_bytes());
        container.extend_from_slice(&manifest);
        container.extend_from_slice(b"not a zstd frame");
        fs::write(&path, container).unwrap();
        let trusted = SnapshotTrustAnchor::new(
            Network::Regtest,
            2,
            BlockHash::all_zeros(),
            0,
            0,
            "11".repeat(32),
        )
        .unwrap();
        assert!(matches!(
            verify_snapshot_with_trust(path, &trusted),
            Err(SnapshotError::TrustMismatch)
        ));
    }

    fn write_test_container(path: &Path, records: &[u8], utxo_count: u64) {
        let manifest = SnapshotManifest {
            format_version: VERSION,
            network: "regtest".to_owned(),
            height: 1,
            block_hash: BlockHash::all_zeros().to_string(),
            utxo_count,
            records_bytes: u64::try_from(records.len()).unwrap(),
            records_sha256: hex_hash(records),
            compression: "zstd".to_owned(),
        };
        let manifest = serde_json::to_vec(&manifest).unwrap();
        let compressed = zstd::stream::encode_all(Cursor::new(records), 1).unwrap();
        let mut container = Vec::new();
        container.extend_from_slice(MAGIC);
        container.extend_from_slice(&VERSION.to_le_bytes());
        container.extend_from_slice(&u32::try_from(manifest.len()).unwrap().to_le_bytes());
        container.extend_from_slice(&manifest);
        container.extend_from_slice(&compressed);
        fs::write(path, container).unwrap();
    }

    fn block(parent: BlockHash, time: u32, height: u8) -> Block {
        let coinbase = Transaction {
            version: Version::ONE,
            lock_time: LockTime::ZERO,
            input: vec![TxIn {
                previous_output: OutPoint::null(),
                script_sig: ScriptBuf::from_bytes(vec![0x50 + height, 0]),
                sequence: Sequence::MAX,
                witness: Witness::default(),
            }],
            output: vec![TxOut {
                value: Amount::from_sat(5_000_000_000),
                script_pubkey: ScriptBuf::new(),
            }],
        };
        let mut block = Block {
            header: Header {
                version: HeaderVersion::from_consensus(4),
                prev_blockhash: parent,
                merkle_root: TxMerkleNode::all_zeros(),
                time,
                bits: Target::MAX_ATTAINABLE_REGTEST.to_compact_lossy(),
                nonce: 0,
            },
            txdata: vec![coinbase],
        };
        block.header.merkle_root = block.compute_merkle_root().unwrap();
        while block
            .header
            .validate_pow(Target::MAX_ATTAINABLE_REGTEST)
            .is_err()
        {
            block.header.nonce += 1;
        }
        block
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn trusted_snapshot_connects_the_next_active_block_and_retains_assumed_marker() {
        let directory = TempDir::new().unwrap();
        let source = RedbUtxoStore::open(directory.path().join("source.redb")).unwrap();
        let source_key = OutPointKey::from(OutPoint::new(Txid::from_byte_array([7; 32]), 0));
        source
            .apply(
                &[],
                &[(
                    source_key,
                    Utxo {
                        value_sats: 42,
                        height: 1,
                        is_coinbase: false,
                        last_touched: 1,
                        creation_mtp: 0,
                        script_pubkey: vec![0x51],
                    },
                )],
            )
            .unwrap();

        let mut headers = HeaderDag::new(Network::Regtest);
        let genesis = headers.active_tip();
        let first = block(genesis.hash, genesis.header.time + 1, 1);
        let first_info = headers
            .insert_contextual(first.header, first.header.time)
            .unwrap();
        let path = directory.path().join("snapshot.rbtc");
        let manifest = export_snapshot(
            &source,
            &path,
            Network::Regtest.to_string(),
            1,
            first_info.hash.to_string(),
        )
        .unwrap();
        let trusted = SnapshotTrustAnchor::new(
            Network::Regtest,
            1,
            first_info.hash,
            manifest.utxo_count,
            manifest.records_bytes,
            manifest.records_sha256.clone(),
        )
        .unwrap();
        let chainstate_path = directory.path().join("chainstate.redb");
        let chainstate = RedbChainStore::open(&chainstate_path, Network::Regtest).unwrap();
        verify_snapshot(&path)
            .unwrap()
            .assume_into(&chainstate, &headers, &trusted, 100, 60)
            .unwrap();
        assert_eq!(chainstate.get(source_key).unwrap().unwrap().value_sats, 42);
        assert_eq!(
            chainstate.execution().assumed_snapshot_base().unwrap(),
            Some(ExecutionTip {
                height: 1,
                hash: first_info.hash
            })
        );
        assert_eq!(
            chainstate
                .execution()
                .assumed_snapshot_records_sha256()
                .unwrap(),
            Some(decode_sha256(&manifest.records_sha256).unwrap())
        );

        let second = block(first_info.hash, first.header.time + 1, 2);
        let second_info = headers
            .insert_contextual(second.header, second.header.time)
            .unwrap();
        let context = block_deployment_context(
            Network::Regtest,
            2,
            second_info.hash,
            second.header.time,
            true,
        );
        connect_active_block(&chainstate, &headers, &second, 101, 60, &context).unwrap();
        drop(chainstate);

        let reopened = RedbChainStore::open(chainstate_path, Network::Regtest).unwrap();
        assert_eq!(reopened.execution().tip().unwrap().height, 2);
        assert_eq!(
            reopened.execution().assumed_snapshot_base().unwrap(),
            Some(ExecutionTip {
                height: 1,
                hash: first_info.hash
            })
        );
        assert!(reopened.undos().get(second_info.hash).unwrap().is_some());
        assert_eq!(
            disconnect_execution_tip(&reopened, &headers, 102, 60).unwrap(),
            ExecutionTip {
                height: 1,
                hash: first_info.hash
            }
        );
        assert!(matches!(
            disconnect_execution_tip(&reopened, &headers, 103, 60),
            Err(BlockExecutionError::DisconnectAssumedSnapshotBase {
                height: 1,
                hash
            }) if hash == first_info.hash
        ));
    }

    #[test]
    fn snapshot_assumption_rejects_untrusted_digest_without_mutation() {
        let directory = TempDir::new().unwrap();
        let source = RedbUtxoStore::open(directory.path().join("source.redb")).unwrap();
        let mut headers = HeaderDag::new(Network::Regtest);
        let genesis = headers.active_tip();
        let first = block(genesis.hash, genesis.header.time + 1, 1);
        let first_info = headers
            .insert_contextual(first.header, first.header.time)
            .unwrap();
        let path = directory.path().join("snapshot.rbtc");
        export_snapshot(&source, &path, "regtest", 1, first_info.hash.to_string()).unwrap();
        let trusted =
            SnapshotTrustAnchor::new(Network::Regtest, 1, first_info.hash, 0, 0, "00".repeat(32))
                .unwrap();
        let chainstate =
            RedbChainStore::open(directory.path().join("chainstate.redb"), Network::Regtest)
                .unwrap();
        let genesis_tip = chainstate.execution().tip().unwrap();
        assert!(matches!(
            verify_snapshot(path)
                .unwrap()
                .assume_into(&chainstate, &headers, &trusted, 100, 60),
            Err(SnapshotError::TrustMismatch)
        ));
        assert_eq!(chainstate.execution().tip().unwrap(), genesis_tip);
        assert_eq!(
            chainstate.execution().assumed_snapshot_base().unwrap(),
            None
        );
        assert!(chainstate.snapshot_entries().unwrap().is_empty());
    }

    #[test]
    fn file_replacement_after_verification_is_rehashed_inside_activation_transaction() {
        let directory = TempDir::new().unwrap();
        let first_source = RedbUtxoStore::open(directory.path().join("first.redb")).unwrap();
        let first_key = OutPointKey::from(OutPoint::new(Txid::from_byte_array([4; 32]), 0));
        first_source
            .apply(
                &[],
                &[(
                    first_key,
                    Utxo {
                        value_sats: 4,
                        height: 1,
                        is_coinbase: false,
                        last_touched: 1,
                        creation_mtp: 0,
                        script_pubkey: vec![0x51],
                    },
                )],
            )
            .unwrap();
        let mut headers = HeaderDag::new(Network::Regtest);
        let genesis = headers.active_tip();
        let first_block = block(genesis.hash, genesis.header.time + 1, 1);
        let anchor = headers
            .insert_contextual(first_block.header, first_block.header.time)
            .unwrap();
        let path = directory.path().join("replaceable.rbtc");
        let manifest =
            export_snapshot(&first_source, &path, "regtest", 1, anchor.hash.to_string()).unwrap();
        let verified = verify_snapshot(&path).unwrap();
        let verified_staging = verify_snapshot(&path).unwrap();
        let trusted = SnapshotTrustAnchor::new(
            Network::Regtest,
            1,
            anchor.hash,
            manifest.utxo_count,
            manifest.records_bytes,
            manifest.records_sha256,
        )
        .unwrap();

        let replacement = RedbUtxoStore::open(directory.path().join("replacement.redb")).unwrap();
        replacement
            .apply(
                &[],
                &[(
                    OutPointKey::from(OutPoint::new(Txid::from_byte_array([5; 32]), 0)),
                    Utxo {
                        value_sats: 5,
                        height: 1,
                        is_coinbase: false,
                        last_touched: 1,
                        creation_mtp: 0,
                        script_pubkey: vec![0x51],
                    },
                )],
            )
            .unwrap();
        export_snapshot(&replacement, &path, "regtest", 1, anchor.hash.to_string()).unwrap();

        let staging = RedbUtxoStore::open(directory.path().join("staging.redb")).unwrap();
        assert!(matches!(
            verified_staging.install_into(&staging, &anchor.hash.to_string(), 100, 60),
            Err(SnapshotError::Invalid(
                "snapshot changed after verification"
            ))
        ));
        assert!(staging.snapshot_entries().unwrap().is_empty());

        let chainstate =
            RedbChainStore::open(directory.path().join("chainstate.redb"), Network::Regtest)
                .unwrap();
        let genesis_tip = chainstate.execution().tip().unwrap();
        assert!(matches!(
            verified.assume_into(&chainstate, &headers, &trusted, 100, 60),
            Err(SnapshotError::ChainStore(
                ChainStoreError::SnapshotDigestMismatch
            ))
        ));
        assert_eq!(chainstate.execution().tip().unwrap(), genesis_tip);
        assert_eq!(
            chainstate.execution().assumed_snapshot_base().unwrap(),
            None
        );
        assert!(chainstate.snapshot_entries().unwrap().is_empty());
    }
}
