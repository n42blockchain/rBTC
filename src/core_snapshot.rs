//! Bitcoin Core 31 `dumptxoutset` parsing and AssumeUTXO authentication.
//!
//! Bitcoin headers do not commit to the UTXO set. A Core snapshot is therefore
//! accepted only at a release-pinned AssumeUTXO base on the validated
//! maximum-work header chain, and only when the decoded UTXO set has Core's
//! compiled `hash_serialized` value.

use std::{
    collections::VecDeque,
    fs::{self, File},
    io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    time::UNIX_EPOCH,
};

use bitcoin::{
    BlockHash, Network, OutPoint, Txid,
    hashes::{Hash as _, HashEngine as _, sha256, sha256d},
    p2p::Magic,
    secp256k1::PublicKey,
};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::{
    chain_store::{ChainStoreError, RedbChainStore, SnapshotContentIdentity},
    chainstate::MAX_MONEY_SATS,
    execution_store::ExecutionTip,
    headers::HeaderDag,
    snapshot::{Core31AssumeUtxoAnchor, core31_assumeutxo_anchors},
    utxo::{OutPointKey, Utxo, UtxoError},
};

const SNAPSHOT_MAGIC: &[u8; 5] = b"utxo\xff";
const SNAPSHOT_VERSION: u16 = 2;
const METADATA_BYTES: usize = 5 + 2 + 4 + 32 + 8;
const MAX_COMPACT_SIZE: u64 = 0x0200_0000;
const MAX_SCRIPT_BYTES: u64 = 10_000;
// Each output contributes at least 9 non-witness bytes, or 36 weight units.
// Transaction overhead makes this a conservative per-txid upper bound.
const MAX_COINS_PER_TXID: u64 = 4_000_000 / 36;
const INDEX_MAGIC: &[u8; 8] = b"RBTCMPH1";
const INDEX_VERSION: u16 = 3;
const MAX_INDEX_LEVELS: usize = 64;
const BBHASH_GAMMA: usize = 2;

/// Failures while parsing or authenticating a Bitcoin Core UTXO snapshot.
#[derive(Debug, Error)]
pub enum CoreSnapshotError {
    /// Filesystem access failed.
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    /// Unified chainstate activation failed.
    #[error("chainstate: {0}")]
    ChainStore(#[from] ChainStoreError),
    /// The file is not a canonical Bitcoin Core 31 snapshot.
    #[error("invalid Bitcoin Core snapshot: {0}")]
    Invalid(&'static str),
    /// The snapshot was created for another network.
    #[error("Bitcoin Core snapshot network does not match selected network")]
    NetworkMismatch,
    /// The base is not one of Bitcoin Core 31's compiled AssumeUTXO identities.
    #[error("snapshot base is not supported by Bitcoin Core 31 chain parameters")]
    UnsupportedBase,
    /// The base block is not at the expected height on the maximum-work header chain.
    #[error("snapshot base does not match the validated maximum-work header chain")]
    AnchorMismatch,
    /// The decoded UTXO set does not match Bitcoin Core's compiled commitment.
    #[error("Bitcoin Core UTXO-set hash mismatch: expected {expected}, got {actual}")]
    HashMismatch {
        /// Release-pinned expected hash.
        expected: &'static str,
        /// Hash computed from decoded snapshot contents.
        actual: String,
    },
}

/// Untrusted metadata encoded at the start of a Core snapshot.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CoreSnapshotMetadata {
    /// Network selected by the file's P2P message magic.
    pub network: Network,
    /// Exact block whose UTXO set was dumped.
    pub base_block_hash: BlockHash,
    /// Number of individual unspent outputs in the file.
    pub coins_count: u64,
}

/// A persistent BBHash-style minimal-perfect-hash sidecar for a Core snapshot.
///
/// The MPHF indexes transaction-id groups rather than individual outpoints.
/// This preserves Core's on-disk txid reuse: each slot stores only the byte
/// offset of one original compressed group. Lookups validate the txid and vout
/// from the snapshot, so a non-member key cannot be returned as a false hit.
#[derive(Debug)]
pub struct CoreSnapshotIndex {
    snapshot: BufReader<File>,
    metadata: CoreSnapshotMetadata,
    snapshot_len: u64,
    snapshot_sha256: [u8; 32],
    serialized_key_bytes: u64,
    levels: Vec<BbHashLevel>,
    offsets: Vec<u64>,
}

#[derive(Debug)]
struct BbHashLevel {
    seed: u64,
    bit_len: usize,
    rank_base: usize,
    bits: Vec<u64>,
    word_ranks: Vec<usize>,
}

#[derive(Clone, Copy)]
struct IndexedGroup {
    txid: [u8; 32],
    offset: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct SourceModified {
    seconds: u64,
    nanos: u32,
}

impl CoreSnapshotIndex {
    /// Builds an immutable MPHF sidecar without rewriting the snapshot.
    ///
    /// The source is structurally parsed, including canonical integer/script
    /// encodings and strict txid/vout uniqueness. Full AssumeUTXO
    /// authentication remains the responsibility of [`verify_core31_snapshot`].
    pub fn build(
        snapshot_path: impl AsRef<Path>,
        index_path: impl AsRef<Path>,
        expected_network: Network,
    ) -> Result<CoreSnapshotMetadata, CoreSnapshotError> {
        let snapshot_path = snapshot_path.as_ref();
        let (mut reader, metadata) = open_snapshot(snapshot_path, expected_network)?;
        let source_before = source_file_identity(snapshot_path)?;
        let snapshot_len = source_before.0;
        let base_height = find_anchor(metadata)?.height;
        let mut groups = Vec::new();
        let mut remaining = metadata.coins_count;
        let mut serialized_key_bytes = 0_u64;
        let mut previous_txid = None;
        while remaining != 0 {
            let offset = reader.stream_position()?;
            let (txid, group_count, group_key_bytes) =
                scan_snapshot_group(&mut reader, remaining, base_height, previous_txid)?;
            groups.push(IndexedGroup { txid, offset });
            remaining -= group_count;
            serialized_key_bytes = serialized_key_bytes
                .checked_add(group_key_bytes)
                .ok_or(CoreSnapshotError::Invalid("serialized key size overflow"))?;
            previous_txid = Some(txid);
        }
        let mut trailing = [0_u8; 1];
        if reader.read(&mut trailing)? != 0 {
            return Err(CoreSnapshotError::Invalid("trailing bytes"));
        }

        let levels = build_bbhash(&groups)?;
        let mut offsets = vec![u64::MAX; groups.len()];
        for group in &groups {
            let slot = bbhash_slot(&levels, &group.txid)
                .ok_or(CoreSnapshotError::Invalid("MPHF construction"))?;
            if offsets[slot] != u64::MAX {
                return Err(CoreSnapshotError::Invalid("MPHF collision"));
            }
            offsets[slot] = group.offset;
        }
        if offsets.contains(&u64::MAX) {
            return Err(CoreSnapshotError::Invalid("incomplete MPHF"));
        }
        let snapshot_sha256 = sha256_file(snapshot_path)?;
        let source_after = source_file_identity(snapshot_path)?;
        if source_after != source_before {
            return Err(CoreSnapshotError::Invalid(
                "source snapshot changed while indexing",
            ));
        }
        write_snapshot_index(
            index_path.as_ref(),
            metadata,
            snapshot_len,
            source_after.1,
            snapshot_sha256,
            serialized_key_bytes,
            &levels,
            &offsets,
        )?;
        Ok(metadata)
    }

    /// Opens a sidecar and checks that it still describes the selected snapshot.
    pub fn open(
        snapshot_path: impl AsRef<Path>,
        index_path: impl AsRef<Path>,
        expected_network: Network,
    ) -> Result<Self, CoreSnapshotError> {
        let snapshot_path = snapshot_path.as_ref();
        let (snapshot, metadata) = open_snapshot(snapshot_path, expected_network)?;
        let (snapshot_len, source_modified) = source_file_identity(snapshot_path)?;
        let (
            indexed_metadata,
            indexed_len,
            indexed_modified,
            indexed_sha256,
            serialized_key_bytes,
            levels,
            offsets,
        ) = read_snapshot_index(index_path.as_ref())?;
        if indexed_metadata != metadata || indexed_len != snapshot_len {
            return Err(CoreSnapshotError::Invalid(
                "MPHF index does not match snapshot",
            ));
        }
        if source_modified != indexed_modified {
            let snapshot_sha256 = sha256_file(snapshot_path)?;
            let source_after = source_file_identity(snapshot_path)?;
            if source_after.0 != snapshot_len || source_after.1 != source_modified {
                return Err(CoreSnapshotError::Invalid(
                    "source snapshot changed while hashing",
                ));
            }
            if snapshot_sha256 != indexed_sha256 {
                return Err(CoreSnapshotError::Invalid(
                    "MPHF source snapshot SHA-256 mismatch",
                ));
            }
            write_snapshot_index(
                index_path.as_ref(),
                metadata,
                snapshot_len,
                source_modified,
                indexed_sha256,
                serialized_key_bytes,
                &levels,
                &offsets,
            )?;
        }
        Ok(Self {
            snapshot,
            metadata,
            snapshot_len,
            snapshot_sha256: indexed_sha256,
            serialized_key_bytes,
            levels,
            offsets,
        })
    }

    /// Returns one UTXO directly from the original compressed-reuse group.
    ///
    /// `headers` supplies creation MTP exactly as the normal Core snapshot
    /// importer does. `last_touched` is caller-selected local metadata.
    pub fn get(
        &mut self,
        outpoint: OutPointKey,
        headers: &HeaderDag,
        last_touched: u64,
    ) -> Result<Option<Utxo>, CoreSnapshotError> {
        if headers.network() != self.metadata.network {
            return Err(CoreSnapshotError::NetworkMismatch);
        }
        let outpoint = outpoint.to_outpoint();
        let txid_bytes = outpoint.txid.to_byte_array();
        let Some(slot) = bbhash_slot(&self.levels, &txid_bytes) else {
            return Ok(None);
        };
        let offset = *self
            .offsets
            .get(slot)
            .ok_or(CoreSnapshotError::Invalid("MPHF slot"))?;
        if offset < u64::try_from(METADATA_BYTES).expect("metadata size fits u64")
            || offset >= self.snapshot_len
        {
            return Err(CoreSnapshotError::Invalid("MPHF offset"));
        }
        self.snapshot.seek(SeekFrom::Start(offset))?;
        let mut stored_txid = [0_u8; 32];
        self.snapshot.read_exact(&mut stored_txid)?;
        if stored_txid != txid_bytes {
            return Ok(None);
        }
        let group_count = read_compact_size(&mut self.snapshot)?;
        if group_count == 0 || group_count > MAX_COINS_PER_TXID {
            return Err(CoreSnapshotError::Invalid("invalid coins-per-txid count"));
        }
        let base_height = find_anchor(self.metadata)?.height;
        let mut found = None;
        for _ in 0..group_count {
            let vout = read_compact_size(&mut self.snapshot)?;
            let vout =
                u32::try_from(vout).map_err(|_| CoreSnapshotError::Invalid("vout overflow"))?;
            if vout == u32::MAX {
                return Err(CoreSnapshotError::Invalid("vout overflow"));
            }
            let utxo = read_indexed_coin(&mut self.snapshot, base_height, headers, last_touched)?;
            if vout == outpoint.vout {
                found = Some(utxo);
            }
        }
        Ok(found)
    }

    /// Number of txid groups represented by the minimal perfect hash.
    #[must_use]
    pub fn group_count(&self) -> usize {
        self.offsets.len()
    }

    /// SHA-256 of every byte in the source AssumeUTXO file.
    #[must_use]
    pub const fn snapshot_sha256(&self) -> [u8; 32] {
        self.snapshot_sha256
    }

    /// Exact number of bytes used by Core's grouped outpoint keys.
    ///
    /// This includes one txid and one group-count CompactSize per group plus
    /// one vout CompactSize per UTXO. Coin values and scripts are excluded.
    #[must_use]
    pub const fn serialized_key_bytes(&self) -> u64 {
        self.serialized_key_bytes
    }

    /// Average grouped AssumeUTXO key bytes per individual UTXO.
    #[must_use]
    pub fn average_serialized_key_bytes(&self) -> f64 {
        self.serialized_key_bytes as f64 / self.metadata.coins_count as f64
    }
}

/// A two-pass verified Core 31 snapshot ready for atomic assumed activation.
#[derive(Debug)]
pub struct VerifiedCore31Snapshot {
    path: PathBuf,
    metadata: CoreSnapshotMetadata,
    anchor: Core31AssumeUtxoAnchor,
    content: SnapshotContentIdentity,
    import_time: u64,
}

impl VerifiedCore31Snapshot {
    /// Returns the authenticated snapshot metadata.
    #[must_use]
    pub const fn metadata(&self) -> CoreSnapshotMetadata {
        self.metadata
    }

    /// Returns the Core 31 release identity used to authenticate this snapshot.
    #[must_use]
    pub const fn anchor(&self) -> Core31AssumeUtxoAnchor {
        self.anchor
    }

    /// Streams the file a second time into a fresh chainstate transaction.
    ///
    /// The rBTC canonical digest computed during verification is rechecked
    /// inside the database transaction, closing a file-replacement race. The
    /// assumed marker remains until independent genesis-to-base validation
    /// reaches the same UTXO-set identity.
    pub fn assume_into(
        self,
        store: &RedbChainStore,
        headers: &HeaderDag,
        hot_window_secs: u64,
    ) -> Result<CoreSnapshotMetadata, CoreSnapshotError> {
        validate_anchor(headers, self.metadata, self.anchor)?;
        let (file, metadata) = open_snapshot(&self.path, headers.network())?;
        if metadata != self.metadata {
            return Err(CoreSnapshotError::Invalid(
                "metadata changed after verification",
            ));
        }
        let entries = CoreCoinReader::new(
            file,
            metadata.coins_count,
            self.anchor.height,
            headers,
            self.import_time,
        );
        store.assume_snapshot_entries(
            ExecutionTip {
                height: self.anchor.height,
                hash: metadata.base_block_hash,
            },
            self.content,
            entries,
            self.import_time,
            hot_window_secs,
        )?;
        Ok(metadata)
    }
}

/// Parses and authenticates a Core 31 `dumptxoutset` file without mutating state.
///
/// Verification is bounded-memory. It validates canonical encodings, ordering,
/// values, heights, exact EOF, Core's double-SHA256 UTXO-set commitment, and the
/// exact base on the selected maximum-work header chain.
pub fn verify_core31_snapshot(
    path: impl AsRef<Path>,
    headers: &HeaderDag,
    import_time: u64,
) -> Result<VerifiedCore31Snapshot, CoreSnapshotError> {
    let path = path.as_ref();
    let (file, metadata) = open_snapshot(path, headers.network())?;
    let anchor = find_anchor(metadata)?;
    validate_anchor(headers, metadata, anchor)?;

    let mut reader = CoreCoinReader::new(
        file,
        metadata.coins_count,
        anchor.height,
        headers,
        import_time,
    );
    let mut records_hash = Sha256::new();
    let mut records_bytes = 0_u64;
    let mut actual_count = 0_u64;
    while let Some((key, utxo)) = reader.next_coin()? {
        records_hash.update(key.as_bytes());
        let encoded = utxo
            .encode()
            .map_err(|_| CoreSnapshotError::Invalid("UTXO encoding"))?;
        records_hash.update(&encoded);
        records_bytes = records_bytes
            .checked_add(
                u64::try_from(key.as_bytes().len() + encoded.len())
                    .expect("record length fits u64"),
            )
            .ok_or(CoreSnapshotError::Invalid("record length overflow"))?;
        actual_count = actual_count
            .checked_add(1)
            .ok_or(CoreSnapshotError::Invalid("coin count overflow"))?;
    }
    if actual_count != metadata.coins_count {
        return Err(CoreSnapshotError::Invalid("coin count mismatch"));
    }
    let actual = sha256d::Hash::from_engine(reader.core_hash).to_string();
    if actual != anchor.hash_serialized {
        return Err(CoreSnapshotError::HashMismatch {
            expected: anchor.hash_serialized,
            actual,
        });
    }

    Ok(VerifiedCore31Snapshot {
        path: path.to_owned(),
        metadata,
        anchor,
        content: SnapshotContentIdentity {
            records_sha256: records_hash.finalize().into(),
            utxo_count: actual_count,
            records_bytes,
        },
        import_time,
    })
}

fn open_snapshot(
    path: &Path,
    expected_network: Network,
) -> Result<(BufReader<File>, CoreSnapshotMetadata), CoreSnapshotError> {
    let mut reader = BufReader::new(File::open(path)?);
    let metadata = read_metadata(&mut reader)?;
    if metadata.network != expected_network {
        return Err(CoreSnapshotError::NetworkMismatch);
    }
    Ok((reader, metadata))
}

fn read_metadata(reader: &mut impl Read) -> Result<CoreSnapshotMetadata, CoreSnapshotError> {
    let mut header = [0_u8; METADATA_BYTES];
    reader.read_exact(&mut header)?;
    if &header[..5] != SNAPSHOT_MAGIC {
        return Err(CoreSnapshotError::Invalid("magic"));
    }
    if u16::from_le_bytes(header[5..7].try_into().expect("fixed metadata")) != SNAPSHOT_VERSION {
        return Err(CoreSnapshotError::Invalid("version"));
    }
    let magic = Magic::from_bytes(header[7..11].try_into().expect("fixed metadata"));
    let network = network_for_magic(magic).ok_or(CoreSnapshotError::Invalid("network magic"))?;
    let base_block_hash =
        BlockHash::from_byte_array(header[11..43].try_into().expect("fixed metadata"));
    let coins_count = u64::from_le_bytes(header[43..51].try_into().expect("fixed metadata"));
    if coins_count == 0 {
        return Err(CoreSnapshotError::Invalid("empty UTXO set"));
    }
    Ok(CoreSnapshotMetadata {
        network,
        base_block_hash,
        coins_count,
    })
}

fn scan_snapshot_group(
    reader: &mut (impl Read + Seek),
    remaining: u64,
    base_height: u32,
    previous_txid: Option<[u8; 32]>,
) -> Result<([u8; 32], u64, u64), CoreSnapshotError> {
    let mut txid = [0_u8; 32];
    reader.read_exact(&mut txid)?;
    if previous_txid.is_some_and(|previous| txid <= previous) {
        return Err(CoreSnapshotError::Invalid(
            "transaction ids are not strictly ordered",
        ));
    }
    let group_count = read_compact_size(reader)?;
    if group_count == 0 || group_count > remaining || group_count > MAX_COINS_PER_TXID {
        return Err(CoreSnapshotError::Invalid("invalid coins-per-txid count"));
    }
    let mut serialized_key_bytes = 32_u64
        .checked_add(compact_size_len(group_count))
        .ok_or(CoreSnapshotError::Invalid("serialized key size overflow"))?;
    let mut vouts =
        Vec::with_capacity(usize::try_from(group_count).expect("bounded group count fits usize"));
    for _ in 0..group_count {
        let vout = read_compact_size(reader)?;
        let vout = u32::try_from(vout).map_err(|_| CoreSnapshotError::Invalid("vout overflow"))?;
        if vout == u32::MAX {
            return Err(CoreSnapshotError::Invalid("vout overflow"));
        }
        serialized_key_bytes = serialized_key_bytes
            .checked_add(compact_size_len(u64::from(vout)))
            .ok_or(CoreSnapshotError::Invalid("serialized key size overflow"))?;
        vouts.push(vout);
        let code = read_core_varint(reader)?;
        let code =
            u32::try_from(code).map_err(|_| CoreSnapshotError::Invalid("coin code overflow"))?;
        if code >> 1 > base_height {
            return Err(CoreSnapshotError::Invalid(
                "coin height above snapshot base",
            ));
        }
        let amount = read_core_varint(reader)?;
        if decompress_amount(amount).is_none_or(|value| value > MAX_MONEY_SATS) {
            return Err(CoreSnapshotError::Invalid("amount overflow"));
        }
        let _ = decompress_script(reader)?;
    }
    vouts.sort_unstable();
    if vouts.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(CoreSnapshotError::Invalid("duplicate output index"));
    }
    Ok((txid, group_count, serialized_key_bytes))
}

const fn compact_size_len(value: u64) -> u64 {
    match value {
        0..=252 => 1,
        253..=0xffff => 3,
        0x1_0000..=0xffff_ffff => 5,
        _ => 9,
    }
}

fn read_indexed_coin(
    reader: &mut impl Read,
    base_height: u32,
    headers: &HeaderDag,
    last_touched: u64,
) -> Result<Utxo, CoreSnapshotError> {
    let code = read_core_varint(reader)?;
    let code = u32::try_from(code).map_err(|_| CoreSnapshotError::Invalid("coin code overflow"))?;
    let height = code >> 1;
    if height > base_height {
        return Err(CoreSnapshotError::Invalid(
            "coin height above snapshot base",
        ));
    }
    let value_sats = decompress_amount(read_core_varint(reader)?)
        .ok_or(CoreSnapshotError::Invalid("amount overflow"))?;
    if value_sats > MAX_MONEY_SATS {
        return Err(CoreSnapshotError::Invalid("amount exceeds MAX_MONEY"));
    }
    let script_pubkey = decompress_script(reader)?;
    let creation_mtp = if height == 0 {
        0
    } else {
        let parent = headers
            .active_header_at(height - 1)
            .ok_or(CoreSnapshotError::Invalid("missing creation header"))?;
        headers
            .median_time_past(parent.hash)
            .ok_or(CoreSnapshotError::Invalid("missing creation MTP"))?
    };
    Ok(Utxo {
        value_sats,
        height,
        is_coinbase: code & 1 == 1,
        last_touched,
        creation_mtp,
        script_pubkey,
    })
}

fn build_bbhash(groups: &[IndexedGroup]) -> Result<Vec<BbHashLevel>, CoreSnapshotError> {
    let mut remaining: Vec<usize> = (0..groups.len()).collect();
    let mut levels = Vec::new();
    let mut rank_base = 0_usize;
    while !remaining.is_empty() {
        if levels.len() >= MAX_INDEX_LEVELS {
            return Err(CoreSnapshotError::Invalid("MPHF level limit"));
        }
        let bit_len = remaining
            .len()
            .checked_mul(BBHASH_GAMMA)
            .and_then(|value| value.checked_next_multiple_of(64))
            .ok_or(CoreSnapshotError::Invalid("MPHF size overflow"))?
            .max(64);
        let seed = 0x9e37_79b9_7f4a_7c15_u64
            .wrapping_mul(u64::try_from(levels.len() + 1).expect("level count fits u64"));
        let mut occupied = vec![0_u64; bit_len / 64];
        let mut collisions = vec![0_u64; bit_len / 64];
        for &index in &remaining {
            let bit = hash_txid(&groups[index].txid, seed)
                % u64::try_from(bit_len).expect("bit length fits u64");
            let bit = usize::try_from(bit).expect("reduced hash fits usize");
            let mask = 1_u64 << (bit % 64);
            if occupied[bit / 64] & mask == 0 {
                occupied[bit / 64] |= mask;
            } else {
                collisions[bit / 64] |= mask;
            }
        }
        for (word, collided) in occupied.iter_mut().zip(&collisions) {
            *word &= !collided;
        }
        let placed: usize = occupied.iter().map(|word| word.count_ones() as usize).sum();
        if placed == 0 {
            return Err(CoreSnapshotError::Invalid("MPHF construction stalled"));
        }
        let mut next = Vec::with_capacity(remaining.len() - placed);
        for index in remaining {
            let bit = hash_txid(&groups[index].txid, seed)
                % u64::try_from(bit_len).expect("bit length fits u64");
            let bit = usize::try_from(bit).expect("reduced hash fits usize");
            if occupied[bit / 64] & (1_u64 << (bit % 64)) == 0 {
                next.push(index);
            }
        }
        levels.push(BbHashLevel {
            seed,
            bit_len,
            rank_base,
            word_ranks: build_word_ranks(&occupied),
            bits: occupied,
        });
        rank_base += placed;
        remaining = next;
    }
    Ok(levels)
}

fn bbhash_slot(levels: &[BbHashLevel], txid: &[u8; 32]) -> Option<usize> {
    for level in levels {
        let bit = hash_txid(txid, level.seed) % u64::try_from(level.bit_len).ok()?;
        let bit = usize::try_from(bit).ok()?;
        let word_index = bit / 64;
        let mask = 1_u64 << (bit % 64);
        if level
            .bits
            .get(word_index)
            .is_some_and(|word| word & mask != 0)
        {
            let preceding_words = *level.word_ranks.get(word_index)?;
            let preceding_bits =
                (level.bits[word_index] & mask.wrapping_sub(1)).count_ones() as usize;
            return Some(level.rank_base + preceding_words + preceding_bits);
        }
    }
    None
}

fn hash_txid(txid: &[u8; 32], seed: u64) -> u64 {
    let mut hash = seed ^ 0xa076_1d64_78bd_642f;
    for chunk in txid.chunks_exact(8) {
        let value = u64::from_le_bytes(chunk.try_into().expect("fixed chunk"));
        hash ^= value.wrapping_add(0xe703_7ed1_a0b4_28db);
        hash = hash.wrapping_mul(0x8ebc_6af0_9c88_c6e3).rotate_left(27);
        hash ^= hash >> 29;
    }
    hash ^ (hash >> 32)
}

fn write_snapshot_index(
    path: &Path,
    metadata: CoreSnapshotMetadata,
    snapshot_len: u64,
    source_modified: SourceModified,
    snapshot_sha256: [u8; 32],
    serialized_key_bytes: u64,
    levels: &[BbHashLevel],
    offsets: &[u64],
) -> Result<(), CoreSnapshotError> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent)?;
    let temporary = path.with_extension("mph.tmp");
    let mut writer = BufWriter::new(File::create(&temporary)?);
    writer.write_all(INDEX_MAGIC)?;
    writer.write_all(&INDEX_VERSION.to_le_bytes())?;
    writer.write_all(&metadata.network.magic().to_bytes())?;
    writer.write_all(&metadata.base_block_hash.to_byte_array())?;
    writer.write_all(&metadata.coins_count.to_le_bytes())?;
    writer.write_all(&snapshot_len.to_le_bytes())?;
    writer.write_all(&source_modified.seconds.to_le_bytes())?;
    writer.write_all(&source_modified.nanos.to_le_bytes())?;
    writer.write_all(&snapshot_sha256)?;
    writer.write_all(&serialized_key_bytes.to_le_bytes())?;
    writer.write_all(
        &u64::try_from(offsets.len())
            .map_err(|_| CoreSnapshotError::Invalid("MPHF group count"))?
            .to_le_bytes(),
    )?;
    writer.write_all(
        &u32::try_from(levels.len())
            .map_err(|_| CoreSnapshotError::Invalid("MPHF level count"))?
            .to_le_bytes(),
    )?;
    for level in levels {
        writer.write_all(&level.seed.to_le_bytes())?;
        writer.write_all(
            &u64::try_from(level.bit_len)
                .map_err(|_| CoreSnapshotError::Invalid("MPHF bit length"))?
                .to_le_bytes(),
        )?;
        for word in &level.bits {
            writer.write_all(&word.to_le_bytes())?;
        }
    }
    for offset in offsets {
        writer.write_all(&offset.to_le_bytes())?;
    }
    writer.flush()?;
    writer.get_ref().sync_all()?;
    fs::rename(temporary, path)?;
    Ok(())
}

fn read_snapshot_index(
    path: &Path,
) -> Result<
    (
        CoreSnapshotMetadata,
        u64,
        SourceModified,
        [u8; 32],
        u64,
        Vec<BbHashLevel>,
        Vec<u64>,
    ),
    CoreSnapshotError,
> {
    let mut reader = BufReader::new(File::open(path)?);
    let mut magic = [0_u8; 8];
    reader.read_exact(&mut magic)?;
    if &magic != INDEX_MAGIC || read_u16(&mut reader)? != INDEX_VERSION {
        return Err(CoreSnapshotError::Invalid("MPHF index header"));
    }
    let mut network_magic = [0_u8; 4];
    reader.read_exact(&mut network_magic)?;
    let network = network_for_magic(Magic::from_bytes(network_magic))
        .ok_or(CoreSnapshotError::Invalid("MPHF network"))?;
    let mut block_hash = [0_u8; 32];
    reader.read_exact(&mut block_hash)?;
    let metadata = CoreSnapshotMetadata {
        network,
        base_block_hash: BlockHash::from_byte_array(block_hash),
        coins_count: read_u64(&mut reader)?,
    };
    let snapshot_len = read_u64(&mut reader)?;
    let source_modified = SourceModified {
        seconds: read_u64(&mut reader)?,
        nanos: read_u32(&mut reader)?,
    };
    if source_modified.nanos >= 1_000_000_000 {
        return Err(CoreSnapshotError::Invalid("MPHF source modification time"));
    }
    let mut snapshot_sha256 = [0_u8; 32];
    reader.read_exact(&mut snapshot_sha256)?;
    let serialized_key_bytes = read_u64(&mut reader)?;
    if serialized_key_bytes < metadata.coins_count {
        return Err(CoreSnapshotError::Invalid("serialized key byte count"));
    }
    let group_count = usize::try_from(read_u64(&mut reader)?)
        .map_err(|_| CoreSnapshotError::Invalid("MPHF group count"))?;
    if group_count == 0
        || u64::try_from(group_count).expect("group count fits u64") > metadata.coins_count
    {
        return Err(CoreSnapshotError::Invalid("MPHF group count"));
    }
    let level_count = usize::try_from(read_u32(&mut reader)?)
        .map_err(|_| CoreSnapshotError::Invalid("MPHF level count"))?;
    if level_count == 0 || level_count > MAX_INDEX_LEVELS {
        return Err(CoreSnapshotError::Invalid("MPHF level count"));
    }
    let mut levels = Vec::with_capacity(level_count);
    let mut rank_base = 0_usize;
    for _ in 0..level_count {
        let seed = read_u64(&mut reader)?;
        let bit_len = usize::try_from(read_u64(&mut reader)?)
            .map_err(|_| CoreSnapshotError::Invalid("MPHF bit length"))?;
        if bit_len == 0 || bit_len % 64 != 0 || bit_len > group_count.saturating_mul(4).max(64) {
            return Err(CoreSnapshotError::Invalid("MPHF bit length"));
        }
        let mut bits = Vec::with_capacity(bit_len / 64);
        for _ in 0..bit_len / 64 {
            bits.push(read_u64(&mut reader)?);
        }
        let placed: usize = bits.iter().map(|word| word.count_ones() as usize).sum();
        levels.push(BbHashLevel {
            seed,
            bit_len,
            rank_base,
            word_ranks: build_word_ranks(&bits),
            bits,
        });
        rank_base = rank_base
            .checked_add(placed)
            .ok_or(CoreSnapshotError::Invalid("MPHF rank overflow"))?;
    }
    if rank_base != group_count {
        return Err(CoreSnapshotError::Invalid("MPHF rank count"));
    }
    let mut offsets = Vec::with_capacity(group_count);
    for _ in 0..group_count {
        offsets.push(read_u64(&mut reader)?);
    }
    let mut trailing = [0_u8; 1];
    if reader.read(&mut trailing)? != 0 {
        return Err(CoreSnapshotError::Invalid("MPHF trailing bytes"));
    }
    Ok((
        metadata,
        snapshot_len,
        source_modified,
        snapshot_sha256,
        serialized_key_bytes,
        levels,
        offsets,
    ))
}

fn build_word_ranks(bits: &[u64]) -> Vec<usize> {
    let mut rank = 0_usize;
    bits.iter()
        .map(|word| {
            let before = rank;
            rank += word.count_ones() as usize;
            before
        })
        .collect()
}

fn sha256_file(path: &Path) -> Result<[u8; 32], CoreSnapshotError> {
    let mut reader = BufReader::new(File::open(path)?);
    let mut digest = Sha256::new();
    let mut buffer = [0_u8; 1024 * 1024];
    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    Ok(digest.finalize().into())
}

fn source_file_identity(path: &Path) -> Result<(u64, SourceModified), CoreSnapshotError> {
    let metadata = fs::metadata(path)?;
    let modified = metadata
        .modified()?
        .duration_since(UNIX_EPOCH)
        .map_err(|_| CoreSnapshotError::Invalid("source modification time before epoch"))?;
    Ok((
        metadata.len(),
        SourceModified {
            seconds: modified.as_secs(),
            nanos: modified.subsec_nanos(),
        },
    ))
}

fn read_u16(reader: &mut impl Read) -> Result<u16, CoreSnapshotError> {
    let mut bytes = [0_u8; 2];
    reader.read_exact(&mut bytes)?;
    Ok(u16::from_le_bytes(bytes))
}

fn read_u32(reader: &mut impl Read) -> Result<u32, CoreSnapshotError> {
    let mut bytes = [0_u8; 4];
    reader.read_exact(&mut bytes)?;
    Ok(u32::from_le_bytes(bytes))
}

fn read_u64(reader: &mut impl Read) -> Result<u64, CoreSnapshotError> {
    let mut bytes = [0_u8; 8];
    reader.read_exact(&mut bytes)?;
    Ok(u64::from_le_bytes(bytes))
}

fn network_for_magic(magic: Magic) -> Option<Network> {
    [
        Network::Bitcoin,
        Network::Testnet,
        Network::Testnet4,
        Network::Signet,
        Network::Regtest,
    ]
    .into_iter()
    .find(|network| network.magic() == magic)
}

fn find_anchor(
    metadata: CoreSnapshotMetadata,
) -> Result<Core31AssumeUtxoAnchor, CoreSnapshotError> {
    core31_assumeutxo_anchors(metadata.network)
        .iter()
        .copied()
        .find(|anchor| anchor.block_hash == metadata.base_block_hash.to_string())
        .ok_or(CoreSnapshotError::UnsupportedBase)
}

fn validate_anchor(
    headers: &HeaderDag,
    metadata: CoreSnapshotMetadata,
    anchor: Core31AssumeUtxoAnchor,
) -> Result<(), CoreSnapshotError> {
    if headers.network() != metadata.network {
        return Err(CoreSnapshotError::NetworkMismatch);
    }
    if headers
        .active_header_at(anchor.height)
        .is_none_or(|header| header.hash != metadata.base_block_hash)
    {
        return Err(CoreSnapshotError::AnchorMismatch);
    }
    Ok(())
}

struct CoreCoinReader<'a, R> {
    reader: R,
    remaining: u64,
    ready: VecDeque<(OutPointKey, Utxo)>,
    previous_txid: Option<[u8; 32]>,
    previous_outpoint: Option<OutPointKey>,
    core_hash: sha256::HashEngine,
    base_height: u32,
    headers: &'a HeaderDag,
    import_time: u64,
    finished: bool,
}

impl<'a, R: Read> CoreCoinReader<'a, R> {
    fn new(
        reader: R,
        coins_count: u64,
        base_height: u32,
        headers: &'a HeaderDag,
        import_time: u64,
    ) -> Self {
        Self {
            reader,
            remaining: coins_count,
            ready: VecDeque::new(),
            previous_txid: None,
            previous_outpoint: None,
            core_hash: sha256d::Hash::engine(),
            base_height,
            headers,
            import_time,
            finished: false,
        }
    }

    fn next_coin(&mut self) -> Result<Option<(OutPointKey, Utxo)>, CoreSnapshotError> {
        if self.finished {
            return Ok(None);
        }
        if let Some((key, utxo)) = self.ready.pop_front() {
            if self
                .previous_outpoint
                .is_some_and(|previous| key <= previous)
            {
                return Err(CoreSnapshotError::Invalid(
                    "outpoints are not strictly ordered",
                ));
            }
            self.previous_outpoint = Some(key);
            return Ok(Some((key, utxo)));
        }
        if self.remaining == 0 {
            let mut trailing = [0_u8; 1];
            return match self.reader.read(&mut trailing)? {
                0 => {
                    self.finished = true;
                    Ok(None)
                }
                _ => Err(CoreSnapshotError::Invalid("trailing bytes")),
            };
        }

        let mut txid_bytes = [0_u8; 32];
        self.reader.read_exact(&mut txid_bytes)?;
        if self
            .previous_txid
            .is_some_and(|previous| txid_bytes <= previous)
        {
            return Err(CoreSnapshotError::Invalid(
                "transaction ids are not strictly ordered",
            ));
        }
        let group_count = read_compact_size(&mut self.reader)?;
        if group_count == 0 || group_count > self.remaining || group_count > MAX_COINS_PER_TXID {
            return Err(CoreSnapshotError::Invalid("invalid coins-per-txid count"));
        }
        self.previous_txid = Some(txid_bytes);
        let txid = Txid::from_byte_array(txid_bytes);
        let mut group = Vec::with_capacity(
            usize::try_from(group_count).expect("bounded group count fits usize"),
        );
        for _ in 0..group_count {
            let vout = read_compact_size(&mut self.reader)?;
            let vout =
                u32::try_from(vout).map_err(|_| CoreSnapshotError::Invalid("vout overflow"))?;
            if vout == u32::MAX {
                return Err(CoreSnapshotError::Invalid("vout overflow"));
            }

            let code = read_core_varint(&mut self.reader)?;
            let code = u32::try_from(code)
                .map_err(|_| CoreSnapshotError::Invalid("coin code overflow"))?;
            let height = code >> 1;
            if height > self.base_height {
                return Err(CoreSnapshotError::Invalid(
                    "coin height above snapshot base",
                ));
            }
            let is_coinbase = code & 1 == 1;
            let compressed_amount = read_core_varint(&mut self.reader)?;
            let value_sats = decompress_amount(compressed_amount)
                .ok_or(CoreSnapshotError::Invalid("amount overflow"))?;
            if value_sats > MAX_MONEY_SATS {
                return Err(CoreSnapshotError::Invalid("amount exceeds MAX_MONEY"));
            }
            let script_pubkey = decompress_script(&mut self.reader)?;
            let creation_mtp = if height == 0 {
                0
            } else {
                let parent = self
                    .headers
                    .active_header_at(height - 1)
                    .ok_or(CoreSnapshotError::Invalid("missing creation header"))?;
                self.headers
                    .median_time_past(parent.hash)
                    .ok_or(CoreSnapshotError::Invalid("missing creation MTP"))?
            };
            let key = OutPointKey::from(OutPoint::new(txid, vout));
            let utxo = Utxo {
                value_sats,
                height,
                is_coinbase,
                last_touched: self.import_time,
                creation_mtp,
                script_pubkey,
            };
            group.push((key, utxo));
            self.remaining -= 1;
        }
        self.queue_group(group)?;
        self.next_coin()
    }

    fn queue_group(
        &mut self,
        mut group: Vec<(OutPointKey, Utxo)>,
    ) -> Result<(), CoreSnapshotError> {
        // Core's database cursor order is not necessarily numeric vout order.
        // ComputeUTXOStats groups into std::map<uint32_t, Coin>, whose hash
        // order is numeric and whose duplicate keys replace. Reject duplicates
        // explicitly before producing the same numeric commitment.
        group.sort_unstable_by_key(|(key, _)| key.to_outpoint().vout);
        if group
            .windows(2)
            .any(|pair| pair[0].0.to_outpoint().vout == pair[1].0.to_outpoint().vout)
        {
            return Err(CoreSnapshotError::Invalid("duplicate output index"));
        }
        for (key, utxo) in &group {
            update_core_utxo_hash(&mut self.core_hash, *key, utxo);
        }
        // rBTC's fixed little-endian vout suffix has another lexical order.
        group.sort_unstable_by_key(|(key, _)| *key);
        self.ready = group.into();
        Ok(())
    }
}

impl<R: Read> Iterator for CoreCoinReader<'_, R> {
    type Item = Result<(OutPointKey, Utxo), UtxoError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.next_coin() {
            Ok(Some(coin)) => Some(Ok(coin)),
            Ok(None) => None,
            Err(_) => {
                self.finished = true;
                Some(Err(UtxoError::Malformed(
                    "invalid Bitcoin Core snapshot entry",
                )))
            }
        }
    }
}

fn read_compact_size(reader: &mut impl Read) -> Result<u64, CoreSnapshotError> {
    let first = read_byte(reader)?;
    let value = match first {
        0..=252 => u64::from(first),
        253 => {
            let mut bytes = [0_u8; 2];
            reader.read_exact(&mut bytes)?;
            let value = u64::from(u16::from_le_bytes(bytes));
            if value < 253 {
                return Err(CoreSnapshotError::Invalid("non-canonical CompactSize"));
            }
            value
        }
        254 => {
            let mut bytes = [0_u8; 4];
            reader.read_exact(&mut bytes)?;
            let value = u64::from(u32::from_le_bytes(bytes));
            if value < 0x1_0000 {
                return Err(CoreSnapshotError::Invalid("non-canonical CompactSize"));
            }
            value
        }
        255 => {
            let mut bytes = [0_u8; 8];
            reader.read_exact(&mut bytes)?;
            let value = u64::from_le_bytes(bytes);
            if value < 0x1_0000_0000 {
                return Err(CoreSnapshotError::Invalid("non-canonical CompactSize"));
            }
            value
        }
    };
    if value > MAX_COMPACT_SIZE {
        return Err(CoreSnapshotError::Invalid("CompactSize exceeds Core limit"));
    }
    Ok(value)
}

fn read_core_varint(reader: &mut impl Read) -> Result<u64, CoreSnapshotError> {
    let mut value = 0_u64;
    loop {
        let byte = read_byte(reader)?;
        value = value
            .checked_shl(7)
            .and_then(|shifted| shifted.checked_add(u64::from(byte & 0x7f)))
            .ok_or(CoreSnapshotError::Invalid("VARINT overflow"))?;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
        value = value
            .checked_add(1)
            .ok_or(CoreSnapshotError::Invalid("VARINT overflow"))?;
    }
}

fn read_byte(reader: &mut impl Read) -> Result<u8, CoreSnapshotError> {
    let mut byte = [0_u8; 1];
    reader.read_exact(&mut byte)?;
    Ok(byte[0])
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

fn decompress_script(reader: &mut impl Read) -> Result<Vec<u8>, CoreSnapshotError> {
    let size = read_core_varint(reader)?;
    match size {
        0 => {
            let mut hash = [0_u8; 20];
            reader.read_exact(&mut hash)?;
            let mut script = Vec::with_capacity(25);
            script.extend_from_slice(&[0x76, 0xa9, 20]);
            script.extend_from_slice(&hash);
            script.extend_from_slice(&[0x88, 0xac]);
            Ok(script)
        }
        1 => {
            let mut hash = [0_u8; 20];
            reader.read_exact(&mut hash)?;
            let mut script = Vec::with_capacity(23);
            script.extend_from_slice(&[0xa9, 20]);
            script.extend_from_slice(&hash);
            script.push(0x87);
            Ok(script)
        }
        2 | 3 => {
            let mut x = [0_u8; 32];
            reader.read_exact(&mut x)?;
            let mut script = Vec::with_capacity(35);
            script.extend_from_slice(&[33, u8::try_from(size).expect("matched size fits u8")]);
            script.extend_from_slice(&x);
            script.push(0xac);
            Ok(script)
        }
        4 | 5 => {
            let mut compressed = [0_u8; 33];
            compressed[0] = u8::try_from(size).expect("matched size fits u8") - 2;
            reader.read_exact(&mut compressed[1..])?;
            let key = PublicKey::from_slice(&compressed)
                .map_err(|_| CoreSnapshotError::Invalid("invalid compressed public key"))?;
            let mut script = Vec::with_capacity(67);
            script.push(65);
            script.extend_from_slice(&key.serialize_uncompressed());
            script.push(0xac);
            Ok(script)
        }
        _ => {
            let script_len = size
                .checked_sub(6)
                .ok_or(CoreSnapshotError::Invalid("script size underflow"))?;
            if script_len > MAX_SCRIPT_BYTES {
                return Err(CoreSnapshotError::Invalid("script exceeds consensus bound"));
            }
            let mut script = vec![
                0_u8;
                usize::try_from(script_len)
                    .expect("10,000-byte script length fits usize")
            ];
            reader.read_exact(&mut script)?;
            Ok(script)
        }
    }
}

fn update_core_utxo_hash(engine: &mut sha256::HashEngine, key: OutPointKey, utxo: &Utxo) {
    engine.input(key.as_bytes());
    engine.input(&((utxo.height << 1) + u32::from(u8::from(utxo.is_coinbase))).to_le_bytes());
    engine.input(&utxo.value_sats.to_le_bytes());
    write_compact_size_hash(engine, utxo.script_pubkey.len() as u64);
    engine.input(&utxo.script_pubkey);
}

fn write_compact_size_hash(engine: &mut sha256::HashEngine, value: u64) {
    if value < 253 {
        engine.input(&[u8::try_from(value).expect("value below 253 fits u8")]);
    } else if let Ok(value) = u16::try_from(value) {
        engine.input(&[253]);
        engine.input(&value.to_le_bytes());
    } else if let Ok(value) = u32::try_from(value) {
        engine.input(&[254]);
        engine.input(&value.to_le_bytes());
    } else {
        engine.input(&[255]);
        engine.input(&value.to_le_bytes());
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;
    use tempfile::NamedTempFile;

    fn metadata_bytes(network: Network, base: BlockHash, coins_count: u64) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(SNAPSHOT_MAGIC);
        bytes.extend_from_slice(&SNAPSHOT_VERSION.to_le_bytes());
        bytes.extend_from_slice(&network.magic().to_bytes());
        bytes.extend_from_slice(base.as_byte_array());
        bytes.extend_from_slice(&coins_count.to_le_bytes());
        bytes
    }

    #[test]
    fn parses_core_v2_metadata() {
        let base = BlockHash::from_byte_array([7_u8; 32]);
        let bytes = metadata_bytes(Network::Testnet4, base, 42);
        assert_eq!(
            read_metadata(&mut Cursor::new(bytes)).unwrap(),
            CoreSnapshotMetadata {
                network: Network::Testnet4,
                base_block_hash: base,
                coins_count: 42,
            }
        );
    }

    #[test]
    fn verification_helpers_bind_network_and_core31_anchor_identity() {
        use std::io::Write as _;

        let anchor = core31_assumeutxo_anchors(Network::Bitcoin)[0];
        let base = anchor.block_hash.parse::<BlockHash>().unwrap();
        let metadata = CoreSnapshotMetadata {
            network: Network::Bitcoin,
            base_block_hash: base,
            coins_count: 1,
        };
        assert_eq!(find_anchor(metadata).unwrap(), anchor);
        assert!(matches!(
            find_anchor(CoreSnapshotMetadata {
                base_block_hash: BlockHash::all_zeros(),
                ..metadata
            }),
            Err(CoreSnapshotError::UnsupportedBase)
        ));
        assert!(matches!(
            validate_anchor(&HeaderDag::new(Network::Regtest), metadata, anchor),
            Err(CoreSnapshotError::NetworkMismatch)
        ));

        let mut file = NamedTempFile::new().unwrap();
        file.write_all(&metadata_bytes(Network::Bitcoin, base, 1))
            .unwrap();
        file.flush().unwrap();
        assert!(matches!(
            open_snapshot(file.path(), Network::Testnet4),
            Err(CoreSnapshotError::NetworkMismatch)
        ));

        let verified = VerifiedCore31Snapshot {
            path: file.path().to_owned(),
            metadata,
            anchor,
            content: SnapshotContentIdentity {
                records_sha256: [0; 32],
                utxo_count: 1,
                records_bytes: 1,
            },
            import_time: 1,
        };
        assert_eq!(verified.metadata(), metadata);
        assert_eq!(verified.anchor(), anchor);
    }

    #[test]
    fn parses_grouped_coin_and_matches_core_hash_serialized() {
        let headers = HeaderDag::new(Network::Regtest);
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&[1_u8; 32]);
        bytes.push(1); // one coin for this txid
        bytes.push(2); // vout
        bytes.push(0); // height zero, not coinbase
        bytes.push(0); // compressed amount 0
        bytes.push(8); // raw script length 2 plus six special encodings
        bytes.extend_from_slice(&[0x51, 0xac]);

        let mut reader = CoreCoinReader::new(Cursor::new(bytes), 1, 0, &headers, 123);
        let (key, mut coin) = reader.next_coin().unwrap().unwrap();
        assert!(reader.next_coin().unwrap().is_none());
        assert_eq!(key.to_outpoint().vout, 2);
        assert_eq!(coin.script_pubkey, [0x51, 0xac]);
        assert_eq!(coin.last_touched, 123);

        coin.height = 100;
        coin.is_coinbase = true;
        let mut hash = sha256d::Hash::engine();
        update_core_utxo_hash(&mut hash, key, &coin);
        assert_eq!(
            sha256d::Hash::from_engine(hash).to_string(),
            "297a5bdfcef53dfef611a70690fa6ad5900cfc8fad5b197d133d9d9bf477a4be"
        );
    }

    #[test]
    fn accepts_database_vout_order_but_hashes_numeric_and_emits_rbtc_key_order() {
        let headers = HeaderDag::new(Network::Regtest);
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&[2_u8; 32]);
        bytes.push(2); // two coins for this txid
        bytes.extend_from_slice(&[253, 0, 1, 0, 0, 6]); // vout 256 and the same coin
        bytes.extend_from_slice(&[1, 0, 0, 6]); // vout 1, height, amount, empty script

        let mut reader = CoreCoinReader::new(Cursor::new(bytes), 2, 0, &headers, 123);
        let first = reader.next_coin().unwrap().unwrap();
        let second = reader.next_coin().unwrap().unwrap();
        assert!(reader.next_coin().unwrap().is_none());
        assert_eq!(first.0.to_outpoint().vout, 256);
        assert_eq!(second.0.to_outpoint().vout, 1);

        let mut expected = sha256d::Hash::engine();
        update_core_utxo_hash(&mut expected, second.0, &second.1);
        update_core_utxo_hash(&mut expected, first.0, &first.1);
        assert_eq!(
            sha256d::Hash::from_engine(reader.core_hash),
            sha256d::Hash::from_engine(expected)
        );
    }

    #[test]
    fn rejects_duplicate_vout_inside_a_group() {
        let headers = HeaderDag::new(Network::Regtest);
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&[3_u8; 32]);
        bytes.push(2);
        bytes.extend_from_slice(&[1, 0, 0, 6]);
        bytes.extend_from_slice(&[1, 0, 0, 6]);

        let error = CoreCoinReader::new(Cursor::new(bytes), 2, 0, &headers, 123)
            .next_coin()
            .unwrap_err();
        assert!(matches!(error, CoreSnapshotError::Invalid(_)));
    }

    #[test]
    fn rejects_noncanonical_compact_size() {
        let error = read_compact_size(&mut Cursor::new([253, 1, 0])).unwrap_err();
        assert!(matches!(error, CoreSnapshotError::Invalid(_)));
    }

    #[test]
    fn decompresses_standard_script_templates() {
        let p2pkh =
            decompress_script(&mut Cursor::new([vec![0], vec![3_u8; 20]].concat())).unwrap();
        assert_eq!(&p2pkh[..3], &[0x76, 0xa9, 20]);
        assert_eq!(&p2pkh[23..], &[0x88, 0xac]);

        let p2sh = decompress_script(&mut Cursor::new([vec![1], vec![4_u8; 20]].concat())).unwrap();
        assert_eq!(&p2sh[..2], &[0xa9, 20]);
        assert_eq!(p2sh.last(), Some(&0x87));
    }

    #[test]
    fn compact_size_varint_amount_and_script_boundaries_are_strict() {
        assert_eq!(read_compact_size(&mut Cursor::new([252])).unwrap(), 252);
        assert_eq!(
            read_compact_size(&mut Cursor::new([253, 253, 0])).unwrap(),
            253
        );
        assert_eq!(
            read_compact_size(&mut Cursor::new([254, 0, 0, 1, 0])).unwrap(),
            0x1_0000
        );
        assert!(matches!(
            read_compact_size(&mut Cursor::new([255, 0, 0, 0, 0, 1, 0, 0, 0])),
            Err(CoreSnapshotError::Invalid(_))
        ));

        assert_eq!(read_core_varint(&mut Cursor::new([0])).unwrap(), 0);
        assert_eq!(read_core_varint(&mut Cursor::new([0x80, 0])).unwrap(), 128);
        assert!(read_core_varint(&mut Cursor::new([0xff; 16])).is_err());
        assert_eq!(decompress_amount(0), Some(0));
        assert_eq!(decompress_amount(1), Some(1));
        assert_eq!(decompress_amount(10), Some(1_000_000_000));
        assert_eq!(decompress_amount(u64::MAX), None);

        let compressed_key = PublicKey::from_slice(&[
            0x02, 0x79, 0xbe, 0x66, 0x7e, 0xf9, 0xdc, 0xbb, 0xac, 0x55, 0xa0, 0x62, 0x95, 0xce,
            0x87, 0x0b, 0x07, 0x02, 0x9b, 0xfc, 0xdb, 0x2d, 0xce, 0x28, 0xd9, 0x59, 0xf2, 0x81,
            0x5b, 0x16, 0xf8, 0x17, 0x98,
        ])
        .unwrap();
        for tag in [2_u8, 3] {
            let mut encoded = vec![tag];
            encoded.extend_from_slice(&compressed_key.serialize()[1..]);
            assert_eq!(
                decompress_script(&mut Cursor::new(encoded)).unwrap().len(),
                35
            );
        }
        for tag in [4_u8, 5] {
            let mut encoded = vec![tag];
            encoded.extend_from_slice(&compressed_key.serialize()[1..]);
            assert_eq!(
                decompress_script(&mut Cursor::new(encoded)).unwrap().len(),
                67
            );
        }
        assert_eq!(
            decompress_script(&mut Cursor::new([8, 0x51, 0xac]))
                .unwrap()
                .as_slice(),
            [0x51, 0xac]
        );
    }

    #[test]
    fn metadata_and_coin_reader_reject_structural_corruption() {
        let mut metadata = Vec::new();
        metadata.extend_from_slice(SNAPSHOT_MAGIC);
        metadata.extend_from_slice(&SNAPSHOT_VERSION.to_le_bytes());
        metadata.extend_from_slice(&Network::Regtest.magic().to_bytes());
        metadata.extend_from_slice(&[0_u8; 32]);
        metadata.extend_from_slice(&0_u64.to_le_bytes());
        assert!(matches!(
            read_metadata(&mut Cursor::new(metadata)),
            Err(CoreSnapshotError::Invalid("empty UTXO set"))
        ));

        let headers = HeaderDag::new(Network::Regtest);
        let cases = [
            [[1_u8; 32].as_slice(), &[0]].concat(),
            [[1_u8; 32].as_slice(), &[2]].concat(),
            [[1_u8; 32].as_slice(), &[1, 255, 0, 0, 0, 0, 1, 0, 0, 0]].concat(),
        ];
        for encoded in cases {
            assert!(
                CoreCoinReader::new(Cursor::new(encoded), 1, 0, &headers, 1)
                    .next_coin()
                    .is_err()
            );
        }
    }
}
