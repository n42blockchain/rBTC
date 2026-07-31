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
//! 8 GiB for the 935,000-height mainnet set). Lookups keep only the hash
//! levels resident — about 68 MiB for that set — and read each packed table
//! entry from the container at its computed bit position, so the roughly
//! 1.02 GiB offset table costs one small positioned read per lookup instead
//! of permanent memory.

use std::{
    fs::File,
    io::{BufReader, Cursor, Read},
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
/// Staging window for streaming the packed offset table at open.
///
/// Must stay a multiple of 8 so the window never splits a 64-bit word.
const INDEX_READ_WINDOW_BYTES: usize = 1024 * 1024;
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
    /// The index container, kept open so the packed table can be read per
    /// lookup instead of held in memory.
    index: File,
    /// Byte offset of the packed offset table within [`Self::index`].
    table_start: u64,
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
        // The container is streamed rather than read whole. Buffering it and
        // then decoding out of that buffer held two copies of a gigabyte-scale
        // structure at once — for the 935,000-height mainnet index, a 1.08 GiB
        // read plus a 1.02 GiB offset table, about 2.16 GiB of peak resident
        // memory to end up holding 1.09 GiB. Streaming keeps only the section
        // being decoded, so the peak is the final structure itself.
        //
        // The cost is that header fields are now parsed before the trailing
        // digest has been checked, so they must not be trusted to size any
        // allocation. They are not: every derived length is reconciled against
        // the file's real length below, which bounds allocation by the bytes
        // that actually exist. The digest is still verified before `open`
        // returns, so no lookup is ever served from unverified content.
        let index_file = File::open(index_path.as_ref())?;
        let container_bytes = index_file.metadata()?.len();
        let container_floor = u64::try_from(INDEX_HEADER_BYTES + INDEX_DIGEST_BYTES)
            .expect("header and digest sizes are small constants");
        if container_bytes < container_floor {
            return Err(CoreSnapshotIndexError::Invalid("truncated container"));
        }
        let mut reader = BufReader::new(index_file);
        let mut container_digest = Sha256::new();

        let mut header = [0_u8; INDEX_HEADER_BYTES];
        reader.read_exact(&mut header)?;
        container_digest.update(header);
        let body = &header[..];
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
        // The hash function's own length is not recorded, but the offset
        // table's is derivable from the header, so the remainder of the body
        // is the hash function. Reconciling the three against the real file
        // length is what keeps a damaged header from sizing an allocation:
        // every section must fit in bytes that exist.
        let entry_bits = u64::from(offset_bits) + u64::from(backref_bits);
        let table_words = coins
            .checked_mul(entry_bits)
            .ok_or(CoreSnapshotIndexError::Invalid("table size overflow"))?
            .div_ceil(64);
        let table_bytes = table_words
            .checked_mul(8)
            .ok_or(CoreSnapshotIndexError::Invalid("table size overflow"))?;
        let body_bytes = container_bytes
            - u64::try_from(INDEX_DIGEST_BYTES).expect("digest size is a small constant");
        let header_bytes =
            u64::try_from(INDEX_HEADER_BYTES).expect("header size is a small constant");
        let mphf_bytes = body_bytes
            .checked_sub(header_bytes)
            .and_then(|rest| rest.checked_sub(table_bytes))
            .filter(|mphf_bytes| *mphf_bytes > 0)
            .ok_or(CoreSnapshotIndexError::Invalid("table length mismatch"))?;

        let mut mphf_section = vec![
            0_u8;
            usize::try_from(mphf_bytes).map_err(|_| {
                CoreSnapshotIndexError::Invalid("hash function exceeds platform limits")
            })?
        ];
        reader.read_exact(&mut mphf_section)?;
        container_digest.update(&mphf_section);
        let (mphf, consumed) = Mphf::decode(&mphf_section)?;
        if consumed != mphf_section.len() {
            return Err(CoreSnapshotIndexError::Invalid(
                "hash function length mismatch",
            ));
        }
        drop(mphf_section);
        if mphf.key_count() != coins {
            return Err(CoreSnapshotIndexError::Invalid(
                "hash function does not cover the coin count",
            ));
        }

        // The packed table is verified but deliberately not retained. It is
        // the overwhelming majority of the container — for the 935,000-height
        // mainnet index, about 1.02 GiB of the 1.08 GiB total — and holding it
        // resident buys nothing a slot lookup cannot get from the file. Each
        // lookup instead reads its own entry at a computed bit position (see
        // `read_table_entry`), trading one small positioned read, which the
        // page cache absorbs for hot regions, against `coins × entry_bits` of
        // permanently resident memory. The lookup already reads the snapshot
        // file for the txid and coin bytes, so this adds a third read to a
        // path that was never memory-resident to begin with.
        let table_start = header_bytes + mphf_bytes;
        let mut window = vec![0_u8; INDEX_READ_WINDOW_BYTES];
        let mut remaining = table_bytes;
        while remaining > 0 {
            let take =
                usize::try_from(remaining.min(
                    u64::try_from(INDEX_READ_WINDOW_BYTES).expect("window is a small constant"),
                ))
                .expect("bounded by the window length");
            let filled = &mut window[..take];
            reader.read_exact(filled)?;
            container_digest.update(&*filled);
            remaining -= u64::try_from(take).expect("bounded by the window length");
        }
        drop(window);

        // Only now is any of the above trustworthy.
        let mut stored_digest = [0_u8; INDEX_DIGEST_BYTES];
        reader.read_exact(&mut stored_digest)?;
        if <[u8; 32]>::from(container_digest.finalize()) != stored_digest {
            return Err(CoreSnapshotIndexError::Invalid("container digest mismatch"));
        }
        let index = reader.into_inner();

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
            index,
            table_start,
            snapshot,
        })
    }

    /// Reads one packed `(coin_offset, backref)` entry from the index file.
    ///
    /// Entries are bit-packed and not byte-aligned, so the read covers the
    /// bytes the entry straddles and the value is shifted out of them. The
    /// widths are validated at open to at most 63 and 32 bits, so an entry
    /// spans at most 95 bits and the covering window is at most 13 bytes —
    /// always a single positioned read.
    ///
    /// The table was written as little-endian 64-bit words, which for bit
    /// addressing is the same sequence as a little-endian byte stream: global
    /// bit `b` lives in byte `b / 8` at bit `b % 8` under either reading.
    fn read_table_entry(&self, slot: u64) -> Result<(u64, u64), CoreSnapshotIndexError> {
        let entry_bits = u64::from(self.offset_bits) + u64::from(self.backref_bits);
        let bit_start = slot * entry_bits;
        let byte_start = bit_start / 8;
        let bit_in_byte = bit_start % 8;
        let span_bytes = usize::try_from((bit_in_byte + entry_bits).div_ceil(8))
            .expect("entry spans at most 13 bytes");
        let mut covering = [0_u8; 16];
        read_exact_at(
            &self.index,
            &mut covering[..span_bytes],
            self.table_start + byte_start,
        )?;
        // Both masks are narrower than 64 bits — open validates the widths at
        // 63 and 32 — so neither value can exceed `u64`.
        let packed = u128::from_le_bytes(covering) >> bit_in_byte;
        let coin_offset = u64::try_from(packed & ((1_u128 << self.offset_bits) - 1))
            .expect("offset width is at most 63 bits");
        let backref =
            u64::try_from((packed >> self.offset_bits) & ((1_u128 << self.backref_bits) - 1))
                .expect("backref width is at most 32 bits");
        Ok((coin_offset, backref))
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
        let entry = self.read_table_entry(slot)?;
        let mut buffer = Vec::new();
        self.decode_located_coin(&key, outpoint.vout, entry, &mut buffer)
    }

    /// Resolves many outpoints in one pass, reading in file order.
    ///
    /// A minimal perfect hash scatters slots uniformly, so resolving outpoints
    /// one at a time walks both the index's offset table and the snapshot in
    /// whatever order the caller happened to supply — the worst case for
    /// readahead, and the dominant cost once the offset table is read from
    /// disk rather than held in memory. This resolves in three phases instead:
    /// every slot is computed first, with no I/O at all; the offset-table
    /// entries are then read in ascending slot order, which is ascending byte
    /// order; and the coin records are read last in ascending snapshot offset.
    /// Each phase moves forward through one file.
    ///
    /// Results are returned in the caller's order, one entry per input.
    /// Duplicate outpoints are resolved independently rather than deduplicated,
    /// since a batch of block inputs cannot contain the same outpoint twice.
    ///
    /// # Errors
    ///
    /// Fails on I/O errors or when snapshot bytes no longer decode
    /// canonically, exactly as [`Self::get`] does.
    pub fn get_many(
        &self,
        outpoints: &[OutPoint],
    ) -> Result<Vec<Option<CoreSnapshotCoin>>, CoreSnapshotIndexError> {
        let keys: Vec<OutPointKey> = outpoints.iter().copied().map(OutPointKey::from).collect();
        let slots: Vec<Option<u64>> = keys
            .iter()
            .map(|key| self.mphf.index(key.as_bytes()))
            .collect();

        // Slot order is byte order: an entry's position is `slot * entry_bits`.
        let mut by_slot: Vec<usize> = (0..outpoints.len())
            .filter(|index| slots[*index].is_some())
            .collect();
        by_slot.sort_unstable_by_key(|index| slots[*index]);
        let mut entries: Vec<Option<(u64, u64)>> = vec![None; outpoints.len()];
        for index in by_slot {
            let slot = slots[index].expect("filtered to placed slots");
            entries[index] = Some(self.read_table_entry(slot)?);
        }

        let mut by_offset: Vec<usize> = (0..outpoints.len())
            .filter(|index| entries[*index].is_some())
            .collect();
        by_offset.sort_unstable_by_key(|index| entries[*index].map(|(offset, _)| offset));
        let mut coins: Vec<Option<CoreSnapshotCoin>> = vec![None; outpoints.len()];
        // One buffer for the whole batch: the per-coin window is bounded by
        // Core's script ceiling, so reusing it avoids an allocation per input.
        let mut buffer = Vec::new();
        for index in by_offset {
            let entry = entries[index].expect("filtered to located entries");
            coins[index] =
                self.decode_located_coin(&keys[index], outpoints[index].vout, entry, &mut buffer)?;
        }
        Ok(coins)
    }

    /// Verifies a located entry against the query and decodes its coin.
    ///
    /// Shared by [`Self::get`] and [`Self::get_many`] so both apply the same
    /// exactness checks: the group txid and the coin's own vout are compared
    /// against the query before any field is returned.
    fn decode_located_coin(
        &self,
        key: &OutPointKey,
        vout: u32,
        (coin_offset, backref): (u64, u64),
        buffer: &mut Vec<u8>,
    ) -> Result<Option<CoreSnapshotCoin>, CoreSnapshotIndexError> {
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
        buffer.clear();
        buffer.resize(window, 0);
        read_exact_at(&self.snapshot, buffer, coin_offset)?;
        let mut cursor = Cursor::new(buffer.as_slice());

        let vout_read = read_compact_size(&mut cursor).map_err(corrupt)?;
        if u64::from(vout) != vout_read {
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
/// rename. Delegates to `snapshot::atomic_write`'s pid- and randomly-suffixed,
/// collision-retrying temporary name instead of a fixed `<name>.tmp` sibling,
/// so two independent builds racing on the same output path (an operator
/// rebuild run against a path a live node's rebase is concurrently writing)
/// cannot collide on the same temporary file.
fn publish_atomically(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    crate::snapshot::atomic_write(path, bytes)
}

#[cfg(test)]
mod tests {
    use std::fs;

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

    /// The batched path reorders its reads internally, so the contract worth
    /// pinning is that it is indistinguishable from calling `get` in a loop:
    /// same answers, same positions, for hits, misses, and repeats alike.
    #[test]
    fn batched_lookups_match_single_lookups_in_caller_order() {
        let directory = TempDir::new().unwrap();
        let base_hash = [7_u8; 32];
        let (bytes, expected) = synthetic_snapshot(base_hash, &test_groups());
        let snapshot = directory.path().join("utxo.dat");
        fs::write(&snapshot, &bytes).unwrap();
        let identity = authenticated_identity(&snapshot, base_hash, directory.path());
        let index_path = directory.path().join("utxo.rbtcidx");
        build_core_snapshot_index_with_identity(&snapshot, &index_path, &identity).unwrap();
        let index = CoreSnapshotUtxoIndex::open(&index_path, &snapshot).unwrap();

        // Deliberately hostile ordering: reversed hits, interleaved with an
        // absent vout under a present txid, an entirely absent txid, and a
        // repeat — none of which the batch may reorder, collapse, or drop.
        let absent_vout = OutPoint::new(Txid::from_byte_array([1_u8; 32]), 1);
        let absent_txid = OutPoint::new(Txid::from_byte_array([8_u8; 32]), 0);
        let present: Vec<OutPoint> = expected.keys().copied().collect();
        let mut queries: Vec<OutPoint> = Vec::new();
        for outpoint in present.iter().rev() {
            queries.push(*outpoint);
            queries.push(absent_vout);
        }
        queries.push(absent_txid);
        queries.push(present[0]);
        queries.push(present[0]);

        let batched = index.get_many(&queries).unwrap();
        assert_eq!(batched.len(), queries.len());
        for (position, outpoint) in queries.iter().enumerate() {
            assert_eq!(
                batched[position],
                index.get(outpoint).unwrap(),
                "position {position} of the batch disagrees with a single lookup"
            );
        }

        // An empty batch is a legitimate call: a block can spend nothing that
        // is not already in the overlay.
        assert!(index.get_many(&[]).unwrap().is_empty());
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

    /// Lookups read each packed entry straight from the container, so the
    /// bit math has to agree with what `write_bits` laid down — including
    /// entries that straddle byte and 64-bit word boundaries, which is the
    /// common case once slots are numbered in the hundreds of millions.
    ///
    /// The widths here are the ones the real 935,000-height mainnet index
    /// uses (34-bit offsets, 19-bit backrefs, 53 bits per entry), because a
    /// width that happens to divide 64 would not exercise straddling at all.
    #[test]
    fn reads_packed_entries_across_byte_and_word_boundaries() {
        const OFFSET_BITS: u8 = 34;
        const BACKREF_BITS: u8 = 19;
        const SLOTS: u64 = 4_096;
        /// A non-zero, odd table start proves the read is relative to it and
        /// does not silently assume alignment.
        const TABLE_START: u64 = 37;

        let entry_bits = u64::from(OFFSET_BITS) + u64::from(BACKREF_BITS);
        let expected: Vec<(u64, u64)> = (0..SLOTS)
            .map(|slot| {
                // Values chosen to keep every bit position in play rather
                // than clustering near zero.
                (
                    slot.wrapping_mul(0x9E37_79B9) & ((1 << OFFSET_BITS) - 1),
                    (slot.wrapping_mul(0x85EB_CA6B) & ((1 << BACKREF_BITS) - 1)) | 1,
                )
            })
            .collect();

        let mut words = vec![0_u64; usize::try_from((SLOTS * entry_bits).div_ceil(64)).unwrap()];
        for (slot, (offset, backref)) in expected.iter().enumerate() {
            let base = u64::try_from(slot).unwrap() * entry_bits;
            write_bits(&mut words, base, OFFSET_BITS, *offset);
            write_bits(
                &mut words,
                base + u64::from(OFFSET_BITS),
                BACKREF_BITS,
                *backref,
            );
        }

        let directory = TempDir::new().unwrap();
        let container_path = directory.path().join("packed.bin");
        let mut container = vec![0xAB_u8; usize::try_from(TABLE_START).unwrap()];
        for word in &words {
            container.extend_from_slice(&word.to_le_bytes());
        }
        // Padding past the end: the last entry's covering window may reach
        // beyond the final word, exactly as it does in a real container whose
        // table is followed by the trailing digest.
        container.extend_from_slice(&[0xCD_u8; INDEX_DIGEST_BYTES]);
        fs::write(&container_path, &container).unwrap();

        let index = CoreSnapshotUtxoIndex {
            network: Network::Bitcoin,
            base_block_hash: BlockHash::from_byte_array([0_u8; 32]),
            base_height: 0,
            coins: SLOTS,
            snapshot_bytes: 0,
            snapshot_sha256: [0_u8; 32],
            offset_bits: OFFSET_BITS,
            backref_bits: BACKREF_BITS,
            mphf: Mphf::build(1, |_| vec![0_u8; 36], INDEX_SEED).unwrap(),
            index: File::open(&container_path).unwrap(),
            table_start: TABLE_START,
            snapshot: File::open(&container_path).unwrap(),
        };

        for (slot, entry) in expected.iter().enumerate() {
            assert_eq!(
                index
                    .read_table_entry(u64::try_from(slot).unwrap())
                    .unwrap(),
                *entry,
                "slot {slot}"
            );
        }
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

        // Truncation is checked at several depths rather than one. Because
        // open streams the container, the section sizes are reconciled against
        // the file's real length, so where the damage lands decides which
        // check fires first — a short tail shrinks the derived hash-function
        // section and is caught decoding it, while deeper cuts fail the
        // length reconciliation or the header itself. The contract under test
        // is that every one of them fails closed, not which check catches it.
        for cut in [1_usize, 10, 64, container.len() / 2, container.len() - 1] {
            let truncated_path = directory.path().join(format!("truncated-{cut}.rbtcidx"));
            fs::write(&truncated_path, &container[..container.len() - cut]).unwrap();
            let error = CoreSnapshotUtxoIndex::open(&truncated_path, &snapshot).unwrap_err();
            assert!(
                matches!(
                    error,
                    CoreSnapshotIndexError::Invalid(_)
                        | CoreSnapshotIndexError::Mphf(_)
                        | CoreSnapshotIndexError::Io(_)
                ),
                "truncation by {cut} must fail closed, got {error}"
            );
        }
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
