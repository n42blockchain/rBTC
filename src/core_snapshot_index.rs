//! Minimal-perfect-hash access index over a retained Core `dumptxoutset` file.
//!
//! A Bitcoin Core 31 v2 snapshot such as `utxo-935000.dat` already stores every
//! coin in Core's compressed representation: VARINT-compressed amounts plus the
//! six standard script templates. Importing it into redb expands those records
//! to canonical bytes inside a B-tree. This module instead keeps the snapshot
//! file itself as the immutable, compressed data source and adds a sidecar
//! index so a single coin can be decoded directly from the file:
//!
//! - A BBhash minimal perfect hash function ([`crate::mphf`]) maps each of the
//!   snapshot's outpoints to a distinct slot.
//! - A bit-packed table stores, per slot, the coin's byte offset and the
//!   backward distance to its txid group header.
//!
//! Lookups are exact, never probabilistic: the 32-byte txid at the group
//! header and the coin's CompactSize vout are compared against the queried
//! outpoint before any field is returned, so a foreign key that the minimal
//! perfect hash function maps to an arbitrary slot is always rejected.
//!
//! Building authenticates the snapshot offline against a release-pinned Core
//! 31 AssumeUTXO identity by recomputing Core's exact double-SHA256 UTXO-set
//! commitment, so no header chain is required. The index binds the snapshot's
//! network, base block hash, coin count, byte length, and full SHA-256; the
//! container itself is covered by a trailing SHA-256 and fails closed on any
//! damage. Peak build memory is one 52-byte location record per coin (about
//! 8 GiB for the 935,000-height mainnet set), while lookups keep only the
//! hash levels and packed table resident.

use std::{
    fs::{self, File},
    io::{BufReader, Cursor, Read, Write as _},
    path::Path,
};

use bitcoin::{BlockHash, Network, OutPoint, hashes::Hash as _, hashes::sha256d, p2p::Magic};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::{
    chainstate::MAX_MONEY_SATS,
    core_snapshot::{
        CoreSnapshotError, CoreSnapshotMetadata, MAX_COINS_PER_TXID, METADATA_BYTES,
        decompress_amount, decompress_script, find_anchor, network_for_magic, read_compact_size,
        read_core_varint, read_metadata, update_core_utxo_hash,
    },
    mphf::{Mphf, MphfError},
    snapshot::Core31AssumeUtxoAnchor,
    utxo::{OutPointKey, Utxo},
};

const INDEX_MAGIC: &[u8; 8] = b"RBTCMPHF";
const INDEX_VERSION: u16 = 1;
const INDEX_HEADER_BYTES: usize = 100;
const INDEX_DIGEST_BYTES: usize = 32;
/// Fixed build seed keeps the published index byte-reproducible. The key set
/// is authenticated against a release-pinned UTXO-set hash, so an adversary
/// cannot select keys against the fixed SipHash keys.
const INDEX_SEED: u64 = 0x7262_7463_2d69_6478;
/// Upper bound of one encoded coin: CompactSize vout, coin-code VARINT,
/// amount VARINT, script-size VARINT, and Core's 10,000-byte script ceiling.
const MAX_COIN_WINDOW: u64 = 5 + 5 + 10 + 10 + 10_000;

/// Failures while building, opening, or querying a snapshot access index.
#[derive(Debug, Error)]
pub enum CoreSnapshotIndexError {
    /// Filesystem access failed.
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    /// The snapshot file is not a canonical, release-pinned Core 31 snapshot.
    #[error("snapshot: {0}")]
    Snapshot(#[from] CoreSnapshotError),
    /// Minimal-perfect-hash construction or decoding failed.
    #[error("mphf: {0}")]
    Mphf(#[from] MphfError),
    /// The index container is damaged or not canonical.
    #[error("invalid snapshot index: {0}")]
    Invalid(&'static str),
    /// The index was built from a different snapshot file.
    #[error("snapshot index identity mismatch: {0}")]
    IdentityMismatch(&'static str),
    /// The decoded UTXO set does not match the expected commitment.
    #[error("UTXO-set hash mismatch: expected {expected}, got {actual}")]
    CommitmentMismatch {
        /// Expected serialized UTXO-set hash.
        expected: String,
        /// Hash computed from the decoded snapshot contents.
        actual: String,
    },
}

/// The exact identity a snapshot file must decode to.
///
/// For operator-supplied snapshots this comes from a compiled Bitcoin Core 31
/// release anchor; a locally materialized rebase snapshot instead carries the
/// identity derived from the node's own validated chainstate at export time.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotBaseIdentity {
    /// Snapshot base height.
    pub height: u32,
    /// Exact base block hash.
    pub block_hash: BlockHash,
    /// Core's serialized UTXO-set hash in display order.
    pub hash_serialized: String,
}

/// Summary of one published snapshot access index.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CoreSnapshotIndexReport {
    /// Coins indexed; equals the snapshot's authenticated coin count.
    pub coins: u64,
    /// Exact snapshot file length in bytes.
    pub snapshot_bytes: u64,
    /// SHA-256 over the complete snapshot file.
    pub snapshot_sha256: [u8; 32],
    /// Published index file length in bytes.
    pub index_bytes: u64,
    /// BBhash levels needed to place every outpoint.
    pub mphf_levels: u32,
    /// Total minimal-perfect-hash bit-array size.
    pub mphf_bits: u64,
}

/// One coin decoded directly from the retained snapshot file.
///
/// `last_touched` and `creation_mtp` are local storage metadata that the
/// snapshot does not carry; callers importing into chainstate supply them the
/// same way the ordinary Core loader does.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CoreSnapshotCoin {
    /// Value in satoshis.
    pub value_sats: u64,
    /// Block height that created the coin.
    pub height: u32,
    /// Whether the coin came from a coinbase transaction.
    pub is_coinbase: bool,
    /// ScriptPubKey decompressed exactly as Core serializes it on the wire.
    pub script_pubkey: Vec<u8>,
}

struct CoinLocation {
    key: [u8; 36],
    coin_offset: u64,
    backref: u32,
}

/// Builds and atomically publishes an access index beside a retained snapshot.
///
/// The snapshot's base block must be one of Bitcoin Core 31's compiled
/// AssumeUTXO identities for its network; the decoded UTXO set must match the
/// release-pinned `hash_serialized` commitment exactly.
///
/// # Errors
///
/// Fails closed on I/O errors, a non-canonical snapshot, an unsupported base,
/// a UTXO-set hash mismatch, or an already-existing output path.
pub fn build_core_snapshot_index(
    snapshot_path: impl AsRef<Path>,
    index_path: impl AsRef<Path>,
) -> Result<CoreSnapshotIndexReport, CoreSnapshotIndexError> {
    let snapshot_path = snapshot_path.as_ref();
    let mut reader = BufReader::new(File::open(snapshot_path)?);
    let metadata = read_metadata(&mut reader)?;
    let anchor = find_anchor(metadata)?;
    drop(reader);
    build_core_snapshot_index_with_identity(snapshot_path, index_path, &anchor_identity(anchor)?)
}

fn anchor_identity(
    anchor: Core31AssumeUtxoAnchor,
) -> Result<SnapshotBaseIdentity, CoreSnapshotIndexError> {
    Ok(SnapshotBaseIdentity {
        height: anchor.height,
        block_hash: anchor
            .block_hash
            .parse()
            .map_err(|_| CoreSnapshotIndexError::Invalid("compiled anchor block hash"))?,
        hash_serialized: anchor.hash_serialized.to_owned(),
    })
}

/// Builds an access index against an explicitly supplied snapshot identity.
///
/// Production callers use [`build_core_snapshot_index`] for operator-supplied
/// files, which resolves the compiled Core 31 identity; rebase materialization
/// supplies the identity it derived from the node's own validated chainstate.
///
/// # Errors
///
/// Same contract as [`build_core_snapshot_index`].
pub fn build_core_snapshot_index_with_identity(
    snapshot_path: impl AsRef<Path>,
    index_path: impl AsRef<Path>,
    identity: &SnapshotBaseIdentity,
) -> Result<CoreSnapshotIndexReport, CoreSnapshotIndexError> {
    let snapshot_path = snapshot_path.as_ref();
    let index_path = index_path.as_ref();
    if index_path.exists() {
        return Err(CoreSnapshotIndexError::Invalid(
            "index output path already exists",
        ));
    }
    let (metadata, locations, snapshot_sha256, snapshot_bytes) =
        scan_snapshot(snapshot_path, identity)?;

    let coins = u64::try_from(locations.len()).expect("coin count fits u64");
    let mphf = Mphf::build(
        coins,
        |ordinal| locations[usize::try_from(ordinal).expect("coin ordinal fits usize")].key,
        INDEX_SEED,
    )?;

    let max_offset = locations
        .iter()
        .map(|location| location.coin_offset)
        .max()
        .expect("scan yields at least one coin");
    let max_backref = locations
        .iter()
        .map(|location| location.backref)
        .max()
        .expect("scan yields at least one coin");
    let offset_bits = bit_width(max_offset);
    let backref_bits = bit_width(u64::from(max_backref));
    let entry_bits = u64::from(offset_bits) + u64::from(backref_bits);
    let table_words = usize::try_from(
        coins
            .checked_mul(entry_bits)
            .expect("table bits fit u64")
            .div_ceil(64),
    )
    .expect("table words fit usize");
    let mut table = vec![0_u64; table_words];
    let mut occupied = vec![0_u64; usize::try_from(coins.div_ceil(64)).expect("bitmap fits usize")];
    for location in &locations {
        let slot = mphf
            .index(&location.key)
            .ok_or(CoreSnapshotIndexError::Invalid("unmapped build key"))?;
        if slot >= coins {
            return Err(CoreSnapshotIndexError::Invalid("slot out of range"));
        }
        let word = usize::try_from(slot / 64).expect("bitmap fits usize");
        let bit = 1_u64 << (slot % 64);
        if occupied[word] & bit != 0 {
            return Err(CoreSnapshotIndexError::Invalid("duplicate slot"));
        }
        occupied[word] |= bit;
        let base = slot * entry_bits;
        write_bits(&mut table, base, offset_bits, location.coin_offset);
        write_bits(
            &mut table,
            base + u64::from(offset_bits),
            backref_bits,
            u64::from(location.backref),
        );
    }

    let mut bytes = Vec::new();
    bytes.extend_from_slice(INDEX_MAGIC);
    bytes.extend_from_slice(&INDEX_VERSION.to_le_bytes());
    bytes.extend_from_slice(&metadata.network.magic().to_bytes());
    bytes.extend_from_slice(metadata.base_block_hash.as_byte_array());
    bytes.extend_from_slice(&coins.to_le_bytes());
    bytes.extend_from_slice(&snapshot_bytes.to_le_bytes());
    bytes.extend_from_slice(&snapshot_sha256);
    bytes.extend_from_slice(&identity.height.to_le_bytes());
    bytes.push(offset_bits);
    bytes.push(backref_bits);
    debug_assert_eq!(bytes.len(), INDEX_HEADER_BYTES);
    mphf.encode_into(&mut bytes);
    for word in &table {
        bytes.extend_from_slice(&word.to_le_bytes());
    }
    let digest: [u8; 32] = Sha256::digest(&bytes).into();
    bytes.extend_from_slice(&digest);

    publish_atomically(index_path, &bytes)?;
    Ok(CoreSnapshotIndexReport {
        coins,
        snapshot_bytes,
        snapshot_sha256,
        index_bytes: u64::try_from(bytes.len()).expect("index length fits u64"),
        mphf_levels: mphf.level_count(),
        mphf_bits: mphf.bit_len(),
    })
}

/// Streams the snapshot once, enforcing the same canonical-form rules as the
/// activation loader, and returns per-coin file locations plus the transport
/// digest. Rejects the file unless Core's UTXO-set commitment matches the
/// supplied release identity.
#[allow(clippy::too_many_lines)]
fn scan_snapshot(
    snapshot_path: &Path,
    identity: &SnapshotBaseIdentity,
) -> Result<(CoreSnapshotMetadata, Vec<CoinLocation>, [u8; 32], u64), CoreSnapshotIndexError> {
    let mut reader = DigestReader::new(BufReader::new(File::open(snapshot_path)?));
    let metadata = read_metadata(&mut reader)?;
    if metadata.base_block_hash != identity.block_hash {
        return Err(CoreSnapshotError::AnchorMismatch.into());
    }

    let mut locations = Vec::new();
    let mut core_hash = sha256d::Hash::engine();
    let mut previous_txid: Option<[u8; 32]> = None;
    let mut remaining = metadata.coins_count;
    while remaining > 0 {
        let txid_offset = reader.position();
        let mut txid = [0_u8; 32];
        reader.read_exact(&mut txid)?;
        if previous_txid.is_some_and(|previous| txid <= previous) {
            return Err(
                CoreSnapshotError::Invalid("transaction ids are not strictly ordered").into(),
            );
        }
        previous_txid = Some(txid);
        let group_count = read_compact_size(&mut reader)?;
        if group_count == 0 || group_count > remaining || group_count > MAX_COINS_PER_TXID {
            return Err(CoreSnapshotError::Invalid("invalid coins-per-txid count").into());
        }
        let mut group = Vec::with_capacity(
            usize::try_from(group_count).expect("bounded group count fits usize"),
        );
        for _ in 0..group_count {
            let coin_offset = reader.position();
            let vout = read_compact_size(&mut reader)?;
            let vout =
                u32::try_from(vout).map_err(|_| CoreSnapshotError::Invalid("vout overflow"))?;
            if vout == u32::MAX {
                return Err(CoreSnapshotError::Invalid("vout overflow").into());
            }
            let code = read_core_varint(&mut reader)?;
            let code = u32::try_from(code)
                .map_err(|_| CoreSnapshotError::Invalid("coin code overflow"))?;
            let height = code >> 1;
            if height > identity.height {
                return Err(CoreSnapshotError::Invalid("coin height above snapshot base").into());
            }
            let compressed_amount = read_core_varint(&mut reader)?;
            let value_sats = decompress_amount(compressed_amount)
                .ok_or(CoreSnapshotError::Invalid("amount overflow"))?;
            if value_sats > MAX_MONEY_SATS {
                return Err(CoreSnapshotError::Invalid("amount exceeds MAX_MONEY").into());
            }
            let script_pubkey = decompress_script(&mut reader)?;
            let backref = coin_offset
                .checked_sub(txid_offset)
                .and_then(|span| u32::try_from(span).ok())
                .ok_or(CoreSnapshotError::Invalid("group span overflow"))?;
            group.push((
                vout,
                coin_offset,
                backref,
                height,
                code & 1 == 1,
                value_sats,
                script_pubkey,
            ));
            remaining -= 1;
        }
        // Core's database cursor order is not numeric vout order; the
        // commitment hashes the numerically sorted group and duplicate vouts
        // are rejected, exactly as the activation loader does.
        group.sort_unstable_by_key(|(vout, ..)| *vout);
        if group.windows(2).any(|pair| pair[0].0 == pair[1].0) {
            return Err(CoreSnapshotError::Invalid("duplicate output index").into());
        }
        for (vout, coin_offset, backref, height, is_coinbase, value_sats, script_pubkey) in group {
            let mut key = [0_u8; 36];
            key[..32].copy_from_slice(&txid);
            key[32..].copy_from_slice(&vout.to_le_bytes());
            update_core_utxo_hash(
                &mut core_hash,
                OutPointKey::from_bytes(&key).expect("fixed key length"),
                &Utxo {
                    value_sats,
                    height,
                    is_coinbase,
                    last_touched: 0,
                    creation_mtp: 0,
                    script_pubkey,
                },
            );
            locations.push(CoinLocation {
                key,
                coin_offset,
                backref,
            });
        }
    }
    let mut trailing = [0_u8; 1];
    if reader.read(&mut trailing)? != 0 {
        return Err(CoreSnapshotError::Invalid("trailing bytes").into());
    }

    let actual = sha256d::Hash::from_engine(core_hash).to_string();
    if actual != identity.hash_serialized {
        return Err(CoreSnapshotIndexError::CommitmentMismatch {
            expected: identity.hash_serialized.clone(),
            actual,
        });
    }
    let (snapshot_sha256, snapshot_bytes) = reader.finish();
    Ok((metadata, locations, snapshot_sha256, snapshot_bytes))
}

/// A read-only, digest-verified access index over a retained snapshot file.
#[derive(Debug)]
pub struct CoreSnapshotUtxoIndex {
    network: Network,
    base_block_hash: BlockHash,
    base_height: u32,
    coins: u64,
    snapshot_bytes: u64,
    snapshot_sha256: [u8; 32],
    offset_bits: u8,
    backref_bits: u8,
    mphf: Mphf,
    table: Vec<u64>,
    snapshot: File,
}

impl CoreSnapshotUtxoIndex {
    /// Opens an index and binds it to the retained snapshot file.
    ///
    /// The complete index container is verified against its trailing SHA-256,
    /// and the snapshot's metadata header and exact byte length must match
    /// the identity recorded at build time. The snapshot's full content
    /// digest can additionally be rechecked with
    /// [`Self::verify_snapshot_digest`]; individual lookups always verify the
    /// queried txid and vout against snapshot bytes before returning a coin.
    ///
    /// # Errors
    ///
    /// Fails closed on I/O errors, container damage, or identity mismatch.
    #[allow(clippy::too_many_lines)]
    pub fn open(
        index_path: impl AsRef<Path>,
        snapshot_path: impl AsRef<Path>,
    ) -> Result<Self, CoreSnapshotIndexError> {
        let bytes = fs::read(index_path.as_ref())?;
        if bytes.len() < INDEX_HEADER_BYTES + INDEX_DIGEST_BYTES {
            return Err(CoreSnapshotIndexError::Invalid("truncated container"));
        }
        let (body, stored_digest) = bytes.split_at(bytes.len() - INDEX_DIGEST_BYTES);
        let digest: [u8; 32] = Sha256::digest(body).into();
        if digest.as_slice() != stored_digest {
            return Err(CoreSnapshotIndexError::Invalid("container digest mismatch"));
        }
        if &body[..8] != INDEX_MAGIC {
            return Err(CoreSnapshotIndexError::Invalid("magic"));
        }
        if u16::from_le_bytes(body[8..10].try_into().expect("fixed header")) != INDEX_VERSION {
            return Err(CoreSnapshotIndexError::Invalid("version"));
        }
        let magic = Magic::from_bytes(body[10..14].try_into().expect("fixed header"));
        let network =
            network_for_magic(magic).ok_or(CoreSnapshotIndexError::Invalid("network magic"))?;
        let base_block_hash =
            BlockHash::from_byte_array(body[14..46].try_into().expect("fixed header"));
        let coins = u64::from_le_bytes(body[46..54].try_into().expect("fixed header"));
        let snapshot_bytes = u64::from_le_bytes(body[54..62].try_into().expect("fixed header"));
        let snapshot_sha256: [u8; 32] = body[62..94].try_into().expect("fixed header");
        let base_height = u32::from_le_bytes(body[94..98].try_into().expect("fixed header"));
        let offset_bits = body[98];
        let backref_bits = body[99];
        if coins == 0 {
            return Err(CoreSnapshotIndexError::Invalid("empty coin count"));
        }
        if offset_bits == 0 || offset_bits > 63 || backref_bits == 0 || backref_bits > 32 {
            return Err(CoreSnapshotIndexError::Invalid("entry width out of range"));
        }
        let (mphf, consumed) = Mphf::decode(&body[INDEX_HEADER_BYTES..])?;
        if mphf.key_count() != coins {
            return Err(CoreSnapshotIndexError::Invalid(
                "hash function does not cover the coin count",
            ));
        }
        let entry_bits = u64::from(offset_bits) + u64::from(backref_bits);
        let table_words = coins
            .checked_mul(entry_bits)
            .ok_or(CoreSnapshotIndexError::Invalid("table size overflow"))?
            .div_ceil(64);
        let table_bytes = usize::try_from(table_words * 8)
            .map_err(|_| CoreSnapshotIndexError::Invalid("table size overflow"))?;
        let table_start = INDEX_HEADER_BYTES + consumed;
        if body.len() != table_start + table_bytes {
            return Err(CoreSnapshotIndexError::Invalid("table length mismatch"));
        }
        let table: Vec<u64> = body[table_start..]
            .chunks_exact(8)
            .map(|chunk| u64::from_le_bytes(chunk.try_into().expect("chunked by 8")))
            .collect();
        drop(bytes);

        let snapshot = File::open(snapshot_path.as_ref())?;
        if snapshot.metadata()?.len() != snapshot_bytes {
            return Err(CoreSnapshotIndexError::IdentityMismatch(
                "snapshot length changed since the index was built",
            ));
        }
        let mut header = BufReader::new(&snapshot);
        let metadata = read_metadata(&mut header)?;
        drop(header);
        if metadata.network != network
            || metadata.base_block_hash != base_block_hash
            || metadata.coins_count != coins
        {
            return Err(CoreSnapshotIndexError::IdentityMismatch(
                "snapshot metadata does not match the indexed identity",
            ));
        }
        Ok(Self {
            network,
            base_block_hash,
            base_height,
            coins,
            snapshot_bytes,
            snapshot_sha256,
            offset_bits,
            backref_bits,
            mphf,
            table,
            snapshot,
        })
    }

    /// Looks one outpoint up directly in the compressed snapshot file.
    ///
    /// Returns `Ok(None)` for outpoints that are not part of the snapshot's
    /// UTXO set. A hit decodes exactly one coin record; the group txid and
    /// the coin's vout are compared against the query before any data is
    /// returned, so results are exact rather than probabilistic.
    ///
    /// # Errors
    ///
    /// Fails on I/O errors or when snapshot bytes no longer decode
    /// canonically, which indicates the file changed after the index was
    /// built.
    pub fn get(
        &self,
        outpoint: &OutPoint,
    ) -> Result<Option<CoreSnapshotCoin>, CoreSnapshotIndexError> {
        let key = OutPointKey::from(*outpoint);
        let Some(slot) = self.mphf.index(key.as_bytes()) else {
            return Ok(None);
        };
        let entry_bits = u64::from(self.offset_bits) + u64::from(self.backref_bits);
        let base = slot * entry_bits;
        let coin_offset = read_bits(&self.table, base, self.offset_bits);
        let backref = read_bits(
            &self.table,
            base + u64::from(self.offset_bits),
            self.backref_bits,
        );
        let txid_offset = coin_offset
            .checked_sub(backref)
            .filter(|offset| *offset >= u64::try_from(METADATA_BYTES).expect("constant fits u64"))
            .ok_or(CoreSnapshotIndexError::Invalid("backref out of range"))?;
        if coin_offset >= self.snapshot_bytes || backref == 0 {
            return Err(CoreSnapshotIndexError::Invalid("offset out of range"));
        }

        let mut txid = [0_u8; 32];
        read_exact_at(&self.snapshot, &mut txid, txid_offset)?;
        if txid != key.as_bytes()[..32] {
            return Ok(None);
        }

        let window = usize::try_from((self.snapshot_bytes - coin_offset).min(MAX_COIN_WINDOW))
            .expect("bounded window fits usize");
        let mut buffer = vec![0_u8; window];
        read_exact_at(&self.snapshot, &mut buffer, coin_offset)?;
        let mut cursor = Cursor::new(buffer.as_slice());

        let vout = read_compact_size(&mut cursor).map_err(corrupt)?;
        if u64::from(outpoint.vout) != vout {
            return Ok(None);
        }
        let code = read_core_varint(&mut cursor).map_err(corrupt)?;
        let code = u32::try_from(code)
            .map_err(|_| CoreSnapshotIndexError::Invalid("coin code overflow"))?;
        let height = code >> 1;
        if height > self.base_height {
            return Err(CoreSnapshotIndexError::Invalid("coin height above base"));
        }
        let compressed_amount = read_core_varint(&mut cursor).map_err(corrupt)?;
        let value_sats = decompress_amount(compressed_amount)
            .ok_or(CoreSnapshotIndexError::Invalid("amount overflow"))?;
        if value_sats > MAX_MONEY_SATS {
            return Err(CoreSnapshotIndexError::Invalid("amount exceeds MAX_MONEY"));
        }
        let script_pubkey = decompress_script(&mut cursor).map_err(corrupt)?;
        Ok(Some(CoreSnapshotCoin {
            value_sats,
            height,
            is_coinbase: code & 1 == 1,
            script_pubkey,
        }))
    }

    /// Streams the complete snapshot file and rechecks the SHA-256 recorded
    /// at build time.
    ///
    /// # Errors
    ///
    /// Fails on I/O errors or when the file's bytes changed since the index
    /// was built.
    pub fn verify_snapshot_digest(&self) -> Result<(), CoreSnapshotIndexError> {
        let mut hasher = Sha256::new();
        let mut buffer = vec![0_u8; 1024 * 1024];
        let mut position = 0_u64;
        while position < self.snapshot_bytes {
            let filled = usize::try_from((self.snapshot_bytes - position).min(1024 * 1024))
                .expect("bounded read fits usize");
            read_exact_at(&self.snapshot, &mut buffer[..filled], position)?;
            hasher.update(&buffer[..filled]);
            position += u64::try_from(filled).expect("read length fits u64");
        }
        let digest: [u8; 32] = hasher.finalize().into();
        if digest != self.snapshot_sha256 {
            return Err(CoreSnapshotIndexError::IdentityMismatch(
                "snapshot content changed since the index was built",
            ));
        }
        Ok(())
    }

    /// Returns the snapshot's network.
    #[must_use]
    pub const fn network(&self) -> Network {
        self.network
    }

    /// Returns the snapshot's base block hash.
    #[must_use]
    pub const fn base_block_hash(&self) -> BlockHash {
        self.base_block_hash
    }

    /// Returns the snapshot's base height.
    #[must_use]
    pub const fn base_height(&self) -> u32 {
        self.base_height
    }

    /// Returns the number of indexed coins.
    #[must_use]
    pub const fn coin_count(&self) -> u64 {
        self.coins
    }

    /// Returns the exact snapshot file length recorded at build time.
    #[must_use]
    pub const fn snapshot_bytes(&self) -> u64 {
        self.snapshot_bytes
    }

    /// Returns the SHA-256 of the complete snapshot file recorded at build time.
    #[must_use]
    pub const fn snapshot_sha256(&self) -> [u8; 32] {
        self.snapshot_sha256
    }
}

fn corrupt(error: CoreSnapshotError) -> CoreSnapshotIndexError {
    match error {
        CoreSnapshotError::Io(error) => CoreSnapshotIndexError::Io(error),
        other => CoreSnapshotIndexError::Snapshot(other),
    }
}

struct DigestReader<R> {
    inner: R,
    digest: Sha256,
    position: u64,
}

impl<R> DigestReader<R> {
    fn new(inner: R) -> Self {
        Self {
            inner,
            digest: Sha256::new(),
            position: 0,
        }
    }

    const fn position(&self) -> u64 {
        self.position
    }

    fn finish(self) -> ([u8; 32], u64) {
        (self.digest.finalize().into(), self.position)
    }
}

impl<R: Read> Read for DigestReader<R> {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        let read = self.inner.read(buffer)?;
        self.digest.update(&buffer[..read]);
        self.position += u64::try_from(read).expect("read length fits u64");
        Ok(read)
    }
}

fn bit_width(value: u64) -> u8 {
    u8::try_from(64 - value.leading_zeros())
        .expect("bit width fits u8")
        .max(1)
}

fn write_bits(words: &mut [u64], bit_position: u64, width: u8, value: u64) {
    debug_assert!(width > 0 && width < 64 && value >> u32::from(width) == 0);
    let word = usize::try_from(bit_position / 64).expect("table words fit usize");
    let shift = bit_position % 64;
    words[word] |= value << shift;
    if shift + u64::from(width) > 64 {
        words[word + 1] |= value >> (64 - shift);
    }
}

fn read_bits(words: &[u64], bit_position: u64, width: u8) -> u64 {
    let word = usize::try_from(bit_position / 64).expect("table words fit usize");
    let shift = bit_position % 64;
    let mut value = words[word] >> shift;
    if shift + u64::from(width) > 64 {
        value |= words[word + 1] << (64 - shift);
    }
    value & ((1_u64 << width) - 1)
}

#[cfg(unix)]
fn read_exact_at(file: &File, buffer: &mut [u8], offset: u64) -> std::io::Result<()> {
    use std::os::unix::fs::FileExt as _;
    let mut filled = 0_usize;
    while filled < buffer.len() {
        let read = file.read_at(
            &mut buffer[filled..],
            offset + u64::try_from(filled).expect("buffer length fits u64"),
        )?;
        if read == 0 {
            return Err(std::io::Error::from(std::io::ErrorKind::UnexpectedEof));
        }
        filled += read;
    }
    Ok(())
}

#[cfg(windows)]
fn read_exact_at(file: &File, buffer: &mut [u8], offset: u64) -> std::io::Result<()> {
    use std::os::windows::fs::FileExt as _;
    let mut filled = 0_usize;
    while filled < buffer.len() {
        let read = file.seek_read(
            &mut buffer[filled..],
            offset + u64::try_from(filled).expect("buffer length fits u64"),
        )?;
        if read == 0 {
            return Err(std::io::Error::from(std::io::ErrorKind::UnexpectedEof));
        }
        filled += read;
    }
    Ok(())
}

/// Publishes through a same-directory temporary file, file sync, and atomic
/// rename, matching the snapshot downloader's publication protocol.
fn publish_atomically(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let file_name = path
        .file_name()
        .ok_or_else(|| std::io::Error::from(std::io::ErrorKind::InvalidInput))?;
    let mut temporary_name = file_name.to_owned();
    temporary_name.push(".tmp");
    let temporary = path.with_file_name(temporary_name);
    let mut output = File::create(&temporary)?;
    output.write_all(bytes)?;
    output.sync_all()?;
    drop(output);
    fs::rename(&temporary, path)?;
    // Windows has no portable directory fsync; see `diagnostics::sync_directory`.
    #[cfg(unix)]
    if let Some(parent) = path.parent() {
        File::open(parent)?.sync_all()?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use bitcoin::Txid;
    use tempfile::TempDir;

    use super::*;

    const BASE_HEIGHT: u32 = 100;

    enum TestScript {
        Raw(Vec<u8>),
        P2pkh([u8; 20]),
        P2sh([u8; 20]),
        CompressedP2pk(u8, [u8; 32]),
    }

    impl TestScript {
        fn encode_into(&self, out: &mut Vec<u8>) {
            match self {
                Self::Raw(bytes) => {
                    write_core_varint(out, u64::try_from(bytes.len()).unwrap() + 6);
                    out.extend_from_slice(bytes);
                }
                Self::P2pkh(hash) => {
                    write_core_varint(out, 0);
                    out.extend_from_slice(hash);
                }
                Self::P2sh(hash) => {
                    write_core_varint(out, 1);
                    out.extend_from_slice(hash);
                }
                Self::CompressedP2pk(parity, x) => {
                    write_core_varint(out, u64::from(*parity));
                    out.extend_from_slice(x);
                }
            }
        }

        fn expected_script(&self) -> Vec<u8> {
            match self {
                Self::Raw(bytes) => bytes.clone(),
                Self::P2pkh(hash) => [&[0x76, 0xa9, 20][..], hash, &[0x88, 0xac]].concat(),
                Self::P2sh(hash) => [&[0xa9, 20][..], hash, &[0x87]].concat(),
                Self::CompressedP2pk(parity, x) => [&[33, *parity][..], x, &[0xac]].concat(),
            }
        }
    }

    struct TestCoin {
        vout: u32,
        height: u32,
        coinbase: bool,
        amount: u64,
        script: TestScript,
    }

    fn write_compact_size(out: &mut Vec<u8>, value: u64) {
        if value < 253 {
            out.push(u8::try_from(value).unwrap());
        } else if value < 0x1_0000 {
            out.push(253);
            out.extend_from_slice(&u16::try_from(value).unwrap().to_le_bytes());
        } else {
            out.push(254);
            out.extend_from_slice(&u32::try_from(value).unwrap().to_le_bytes());
        }
    }

    fn write_core_varint(out: &mut Vec<u8>, mut value: u64) {
        let mut reversed = Vec::new();
        loop {
            let low = u8::try_from(value & 0x7f).unwrap();
            reversed.push(low | if reversed.is_empty() { 0x00 } else { 0x80 });
            if value <= 0x7f {
                break;
            }
            value = (value >> 7) - 1;
        }
        reversed.reverse();
        out.extend_from_slice(&reversed);
    }

    fn compress_amount(mut amount: u64) -> u64 {
        if amount == 0 {
            return 0;
        }
        let mut exponent = 0_u64;
        while amount % 10 == 0 && exponent < 9 {
            amount /= 10;
            exponent += 1;
        }
        if exponent < 9 {
            let digit = amount % 10;
            amount /= 10;
            1 + (amount * 9 + digit - 1) * 10 + exponent
        } else {
            1 + (amount - 1) * 10 + 9
        }
    }

    /// Encodes a canonical Core v2 snapshot and the exact coins it commits to.
    fn synthetic_snapshot(
        base_hash: [u8; 32],
        groups: &[([u8; 32], Vec<TestCoin>)],
    ) -> (Vec<u8>, BTreeMap<OutPoint, CoreSnapshotCoin>) {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(b"utxo\xff");
        bytes.extend_from_slice(&2_u16.to_le_bytes());
        bytes.extend_from_slice(&Network::Regtest.magic().to_bytes());
        bytes.extend_from_slice(&base_hash);
        let coins: usize = groups.iter().map(|(_, coins)| coins.len()).sum();
        bytes.extend_from_slice(&u64::try_from(coins).unwrap().to_le_bytes());

        let mut expected = BTreeMap::new();
        for (txid, coins) in groups {
            bytes.extend_from_slice(txid);
            write_compact_size(&mut bytes, u64::try_from(coins.len()).unwrap());
            for coin in coins {
                write_compact_size(&mut bytes, u64::from(coin.vout));
                write_core_varint(
                    &mut bytes,
                    u64::from((coin.height << 1) | u32::from(coin.coinbase)),
                );
                write_core_varint(&mut bytes, compress_amount(coin.amount));
                coin.script.encode_into(&mut bytes);
                expected.insert(
                    OutPoint::new(Txid::from_byte_array(*txid), coin.vout),
                    CoreSnapshotCoin {
                        value_sats: coin.amount,
                        height: coin.height,
                        is_coinbase: coin.coinbase,
                        script_pubkey: coin.script.expected_script(),
                    },
                );
            }
        }
        (bytes, expected)
    }

    fn test_groups() -> Vec<([u8; 32], Vec<TestCoin>)> {
        vec![
            (
                [1_u8; 32],
                vec![
                    // Database cursor order deliberately differs from numeric
                    // vout order inside this group.
                    TestCoin {
                        vout: 256,
                        height: 0,
                        coinbase: false,
                        amount: 0,
                        script: TestScript::Raw(vec![0x51, 0xac]),
                    },
                    TestCoin {
                        vout: 0,
                        height: BASE_HEIGHT,
                        coinbase: true,
                        amount: 5_000_000_000,
                        script: TestScript::P2pkh([3_u8; 20]),
                    },
                ],
            ),
            (
                [2_u8; 32],
                vec![TestCoin {
                    vout: 1,
                    height: 55,
                    coinbase: false,
                    amount: 12_345,
                    script: TestScript::P2sh([4_u8; 20]),
                }],
            ),
            (
                [3_u8; 32],
                vec![
                    TestCoin {
                        vout: 0,
                        height: 1,
                        coinbase: false,
                        amount: 1,
                        script: TestScript::CompressedP2pk(2, [9_u8; 32]),
                    },
                    TestCoin {
                        vout: 7,
                        height: 0,
                        coinbase: false,
                        amount: 0,
                        script: TestScript::Raw(Vec::new()),
                    },
                ],
            ),
        ]
    }

    fn identity_with(base_hash: [u8; 32], hash_serialized: &str) -> SnapshotBaseIdentity {
        SnapshotBaseIdentity {
            height: BASE_HEIGHT,
            block_hash: BlockHash::from_byte_array(base_hash),
            hash_serialized: hash_serialized.to_owned(),
        }
    }

    /// Builds against a deliberately wrong commitment to learn the exact
    /// commitment through the strict mismatch error, then returns an identity
    /// carrying that value; the production hashing path stays authoritative.
    fn authenticated_identity(
        snapshot: &Path,
        base_hash: [u8; 32],
        scratch: &Path,
    ) -> SnapshotBaseIdentity {
        let error = build_core_snapshot_index_with_identity(
            snapshot,
            scratch.join("never-published.rbtcidx"),
            &identity_with(base_hash, "wrong"),
        )
        .unwrap_err();
        let CoreSnapshotIndexError::CommitmentMismatch { actual, .. } = error else {
            panic!("expected a UTXO-set hash mismatch, got {error}");
        };
        identity_with(base_hash, &actual)
    }

    #[test]
    fn builds_and_looks_up_every_coin_exactly() {
        let directory = TempDir::new().unwrap();
        let base_hash = [7_u8; 32];
        let (bytes, expected) = synthetic_snapshot(base_hash, &test_groups());
        let snapshot = directory.path().join("utxo.dat");
        fs::write(&snapshot, &bytes).unwrap();

        let identity = authenticated_identity(&snapshot, base_hash, directory.path());
        let index_path = directory.path().join("utxo.rbtcidx");
        let report =
            build_core_snapshot_index_with_identity(&snapshot, &index_path, &identity).unwrap();
        assert_eq!(report.coins, 5);
        assert_eq!(report.snapshot_bytes, u64::try_from(bytes.len()).unwrap());

        let index = CoreSnapshotUtxoIndex::open(&index_path, &snapshot).unwrap();
        assert_eq!(index.coin_count(), 5);
        assert_eq!(index.network(), Network::Regtest);
        assert_eq!(index.base_height(), BASE_HEIGHT);
        index.verify_snapshot_digest().unwrap();

        for (outpoint, coin) in &expected {
            assert_eq!(index.get(outpoint).unwrap().as_ref(), Some(coin));
        }
        let absent_vout = OutPoint::new(Txid::from_byte_array([1_u8; 32]), 1);
        assert_eq!(index.get(&absent_vout).unwrap(), None);
        let absent_txid = OutPoint::new(Txid::from_byte_array([8_u8; 32]), 0);
        assert_eq!(index.get(&absent_txid).unwrap(), None);
    }

    #[test]
    fn rejects_a_wrong_utxo_set_commitment() {
        let directory = TempDir::new().unwrap();
        let base_hash = [7_u8; 32];
        let (bytes, _) = synthetic_snapshot(base_hash, &test_groups());
        let snapshot = directory.path().join("utxo.dat");
        fs::write(&snapshot, &bytes).unwrap();

        let error = build_core_snapshot_index_with_identity(
            &snapshot,
            directory.path().join("utxo.rbtcidx"),
            &identity_with(
                base_hash,
                "e4b90ef9eae834f56c4b64d2d50143cee10ad87994c614d7d04125e2a6025050",
            ),
        )
        .unwrap_err();
        assert!(matches!(
            error,
            CoreSnapshotIndexError::CommitmentMismatch { .. }
        ));
        assert!(!directory.path().join("utxo.rbtcidx").exists());
    }

    #[test]
    fn rejects_tampered_or_truncated_containers() {
        let directory = TempDir::new().unwrap();
        let base_hash = [7_u8; 32];
        let (bytes, _) = synthetic_snapshot(base_hash, &test_groups());
        let snapshot = directory.path().join("utxo.dat");
        fs::write(&snapshot, &bytes).unwrap();
        let identity = authenticated_identity(&snapshot, base_hash, directory.path());
        let index_path = directory.path().join("utxo.rbtcidx");
        build_core_snapshot_index_with_identity(&snapshot, &index_path, &identity).unwrap();

        let container = fs::read(&index_path).unwrap();
        let mut tampered = container.clone();
        tampered[container.len() / 2] ^= 0x01;
        let tampered_path = directory.path().join("tampered.rbtcidx");
        fs::write(&tampered_path, &tampered).unwrap();
        assert!(matches!(
            CoreSnapshotUtxoIndex::open(&tampered_path, &snapshot).unwrap_err(),
            CoreSnapshotIndexError::Invalid("container digest mismatch")
        ));

        let truncated_path = directory.path().join("truncated.rbtcidx");
        fs::write(&truncated_path, &container[..container.len() - 10]).unwrap();
        assert!(matches!(
            CoreSnapshotUtxoIndex::open(&truncated_path, &snapshot).unwrap_err(),
            CoreSnapshotIndexError::Invalid(_)
        ));
    }

    #[test]
    fn rejects_a_replaced_snapshot_file() {
        let directory = TempDir::new().unwrap();
        let base_hash = [7_u8; 32];
        let (bytes, expected) = synthetic_snapshot(base_hash, &test_groups());
        let snapshot = directory.path().join("utxo.dat");
        fs::write(&snapshot, &bytes).unwrap();
        let identity = authenticated_identity(&snapshot, base_hash, directory.path());
        let index_path = directory.path().join("utxo.rbtcidx");
        build_core_snapshot_index_with_identity(&snapshot, &index_path, &identity).unwrap();

        // A longer file is rejected before any lookup.
        let longer = directory.path().join("longer.dat");
        fs::write(&longer, [bytes.as_slice(), &[0]].concat()).unwrap();
        assert!(matches!(
            CoreSnapshotUtxoIndex::open(&index_path, &longer).unwrap_err(),
            CoreSnapshotIndexError::IdentityMismatch(_)
        ));

        // A different base hash in the metadata header is rejected at open.
        let mut other_base = bytes.clone();
        other_base[11] ^= 0x01;
        let other_base_path = directory.path().join("other-base.dat");
        fs::write(&other_base_path, &other_base).unwrap();
        assert!(matches!(
            CoreSnapshotUtxoIndex::open(&index_path, &other_base_path).unwrap_err(),
            CoreSnapshotIndexError::IdentityMismatch(_)
        ));

        // Same-length content damage is caught by the full digest recheck,
        // while lookups still verify the queried txid bytes exactly.
        let mut same_length = bytes;
        let last = same_length.len() - 1;
        same_length[last] ^= 0x01;
        let same_length_path = directory.path().join("same-length.dat");
        fs::write(&same_length_path, &same_length).unwrap();
        let index = CoreSnapshotUtxoIndex::open(&index_path, &same_length_path).unwrap();
        assert!(matches!(
            index.verify_snapshot_digest().unwrap_err(),
            CoreSnapshotIndexError::IdentityMismatch(_)
        ));
        let untouched = OutPoint::new(Txid::from_byte_array([1_u8; 32]), 0);
        assert_eq!(
            index.get(&untouched).unwrap().as_ref(),
            Some(&expected[&untouched])
        );
    }

    /// Opt-in smoke check against a real snapshot and published index; set
    /// `RBTC_CORE_SNAPSHOT` and `RBTC_CORE_SNAPSHOT_INDEX` to run it.
    #[test]
    fn real_snapshot_index_resolves_its_first_coin() {
        let (Ok(snapshot), Ok(index_path)) = (
            std::env::var("RBTC_CORE_SNAPSHOT"),
            std::env::var("RBTC_CORE_SNAPSHOT_INDEX"),
        ) else {
            return;
        };
        let index = CoreSnapshotUtxoIndex::open(&index_path, &snapshot).unwrap();

        let mut reader = BufReader::new(File::open(&snapshot).unwrap());
        let metadata = read_metadata(&mut reader).unwrap();
        assert_eq!(index.coin_count(), metadata.coins_count);
        let mut txid = [0_u8; 32];
        reader.read_exact(&mut txid).unwrap();
        let _group_count = read_compact_size(&mut reader).unwrap();
        let vout = u32::try_from(read_compact_size(&mut reader).unwrap()).unwrap();

        let first = OutPoint::new(Txid::from_byte_array(txid), vout);
        let coin = index
            .get(&first)
            .unwrap()
            .expect("the snapshot's first coin resolves through the index");
        assert!(coin.height <= index.base_height());
        assert!(coin.value_sats <= MAX_MONEY_SATS);
        let absent = OutPoint::new(Txid::from_byte_array(txid), u32::MAX - 1);
        assert_eq!(index.get(&absent).unwrap(), None);
        println!(
            "real-snapshot smoke: coins={} first_coin height={} value_sats={} script_bytes={}",
            index.coin_count(),
            coin.height,
            coin.value_sats,
            coin.script_pubkey.len(),
        );
    }

    #[test]
    fn refuses_an_existing_output_path() {
        let directory = TempDir::new().unwrap();
        let base_hash = [7_u8; 32];
        let (bytes, _) = synthetic_snapshot(base_hash, &test_groups());
        let snapshot = directory.path().join("utxo.dat");
        fs::write(&snapshot, &bytes).unwrap();
        let index_path = directory.path().join("utxo.rbtcidx");
        fs::write(&index_path, b"occupied").unwrap();
        assert!(matches!(
            build_core_snapshot_index_with_identity(
                &snapshot,
                &index_path,
                &identity_with(base_hash, "unchecked"),
            )
            .unwrap_err(),
            CoreSnapshotIndexError::Invalid("index output path already exists")
        ));
        assert_eq!(fs::read(&index_path).unwrap(), b"occupied");
    }
}
