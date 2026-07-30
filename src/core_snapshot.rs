//! Bitcoin Core 31 `dumptxoutset` parsing and AssumeUTXO authentication.
//!
//! Bitcoin headers do not commit to the UTXO set. A Core snapshot is therefore
//! accepted only at a release-pinned AssumeUTXO base on the validated
//! maximum-work header chain, and only when the decoded UTXO set has Core's
//! compiled `hash_serialized` value.

use std::{
    collections::VecDeque,
    fs::File,
    io::{BufReader, Read},
    path::{Path, PathBuf},
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
pub(crate) const METADATA_BYTES: usize = 5 + 2 + 4 + 32 + 8;
const MAX_COMPACT_SIZE: u64 = 0x0200_0000;
const MAX_SCRIPT_BYTES: u64 = 10_000;
// Each output contributes at least 9 non-witness bytes, or 36 weight units.
// Transaction overhead makes this a conservative per-txid upper bound.
pub(crate) const MAX_COINS_PER_TXID: u64 = 4_000_000 / 36;

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

pub(crate) fn read_metadata(
    reader: &mut impl Read,
) -> Result<CoreSnapshotMetadata, CoreSnapshotError> {
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

pub(crate) fn network_for_magic(magic: Magic) -> Option<Network> {
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

pub(crate) fn find_anchor(
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

pub(crate) fn read_compact_size(reader: &mut impl Read) -> Result<u64, CoreSnapshotError> {
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

pub(crate) fn read_core_varint(reader: &mut impl Read) -> Result<u64, CoreSnapshotError> {
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

/// Encodes a satoshi amount with Core's `CompressAmount` transform, the exact
/// inverse of [`decompress_amount`].
#[cfg_attr(not(feature = "mdbx"), allow(dead_code))]
pub(crate) fn compress_amount(mut amount: u64) -> u64 {
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

/// Appends Core's `VARINT` encoding, the exact inverse of [`read_core_varint`].
#[cfg_attr(not(feature = "mdbx"), allow(dead_code))]
pub(crate) fn write_core_varint(out: &mut Vec<u8>, mut value: u64) {
    let mut reversed = Vec::with_capacity(10);
    loop {
        let low = u8::try_from(value & 0x7f).expect("seven bits fit u8");
        reversed.push(low | if reversed.is_empty() { 0x00 } else { 0x80 });
        if value <= 0x7f {
            break;
        }
        value = (value >> 7) - 1;
    }
    reversed.reverse();
    out.extend_from_slice(&reversed);
}

/// Appends a canonical Bitcoin CompactSize, the inverse of [`read_compact_size`].
#[cfg_attr(not(feature = "mdbx"), allow(dead_code))]
pub(crate) fn write_compact_size(out: &mut Vec<u8>, value: u64) {
    if value < 253 {
        out.push(u8::try_from(value).expect("small CompactSize fits u8"));
    } else if let Ok(value16) = u16::try_from(value) {
        out.push(253);
        out.extend_from_slice(&value16.to_le_bytes());
    } else if let Ok(value32) = u32::try_from(value) {
        out.push(254);
        out.extend_from_slice(&value32.to_le_bytes());
    } else {
        out.push(255);
        out.extend_from_slice(&value.to_le_bytes());
    }
}

/// Appends Core's compressed script encoding, the exact inverse of
/// [`decompress_script`]: the six standard templates when the script matches
/// one, otherwise the raw bytes behind a `length + 6` size prefix.
#[cfg_attr(not(feature = "mdbx"), allow(dead_code))]
pub(crate) fn compress_script(out: &mut Vec<u8>, script: &[u8]) {
    match script {
        [0x76, 0xa9, 20, hash @ .., 0x88, 0xac] if hash.len() == 20 => {
            write_core_varint(out, 0);
            out.extend_from_slice(hash);
        }
        [0xa9, 20, hash @ .., 0x87] if hash.len() == 20 => {
            write_core_varint(out, 1);
            out.extend_from_slice(hash);
        }
        [33, parity @ (2 | 3), x @ .., 0xac] if x.len() == 32 => {
            write_core_varint(out, u64::from(*parity));
            out.extend_from_slice(x);
        }
        [65, key @ .., 0xac]
            if key.len() == 65 && key[0] == 0x04 && PublicKey::from_slice(key).is_ok() =>
        {
            // Core's CompressScript only compresses the standard uncompressed
            // tag (0x04); SEC1's rarely-used hybrid tags (0x06/0x07) parse as
            // valid points through libsecp256k1 but are never compressed by
            // Core, so compressing them here would be lossy: decompression's
            // `serialize_uncompressed` always re-emits tag 0x04, silently
            // discarding an original hybrid tag. Requiring the exact tag
            // keeps this the precise inverse of `decompress_script`, whose
            // own output is always tag 0x04.
            write_core_varint(out, 4 + u64::from(key[64] & 1));
            out.extend_from_slice(&key[1..33]);
        }
        raw => {
            write_core_varint(
                out,
                u64::try_from(raw.len()).expect("script length fits u64") + 6,
            );
            out.extend_from_slice(raw);
        }
    }
}

pub(crate) fn decompress_amount(mut value: u64) -> Option<u64> {
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

pub(crate) fn decompress_script(reader: &mut impl Read) -> Result<Vec<u8>, CoreSnapshotError> {
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

pub(crate) fn update_core_utxo_hash(
    engine: &mut sha256::HashEngine,
    key: OutPointKey,
    utxo: &Utxo,
) {
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

    use bitcoin::hex::FromHex;

    use super::*;

    #[test]
    fn parses_core_v2_metadata() {
        let base = BlockHash::from_byte_array([7_u8; 32]);
        let mut bytes = Vec::new();
        bytes.extend_from_slice(SNAPSHOT_MAGIC);
        bytes.extend_from_slice(&SNAPSHOT_VERSION.to_le_bytes());
        bytes.extend_from_slice(&Network::Testnet4.magic().to_bytes());
        bytes.extend_from_slice(base.as_byte_array());
        bytes.extend_from_slice(&42_u64.to_le_bytes());
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
    fn amount_compression_roundtrips_core_values() {
        for amount in [
            0_u64,
            1,
            330,
            546,
            1_000,
            12_345,
            50_0000_0000,
            2_099_999_997_690_000,
            u64::from(u32::MAX),
        ] {
            assert_eq!(decompress_amount(compress_amount(amount)), Some(amount));
        }
        for compressed in 0_u64..10_000 {
            if let Some(amount) = decompress_amount(compressed) {
                assert_eq!(compress_amount(amount), compressed);
            }
        }
    }

    #[test]
    fn varint_and_compact_size_encoders_roundtrip() {
        for value in [
            0_u64, 1, 0x7f, 0x80, 0x407f, 0x4080, 252, 253, 65_535, 65_536, 1_000_000,
        ] {
            let mut bytes = Vec::new();
            write_core_varint(&mut bytes, value);
            assert_eq!(read_core_varint(&mut Cursor::new(bytes)).unwrap(), value);

            let mut bytes = Vec::new();
            write_compact_size(&mut bytes, value);
            assert_eq!(read_compact_size(&mut Cursor::new(bytes)).unwrap(), value);
        }
    }

    #[test]
    fn script_compression_roundtrips_templates_and_raw_scripts() {
        let p2pkh = [&[0x76, 0xa9, 20][..], &[7_u8; 20], &[0x88, 0xac]].concat();
        let p2sh = [&[0xa9, 20][..], &[8_u8; 20], &[0x87]].concat();
        let compressed_p2pk = [&[33, 2][..], &[9_u8; 32], &[0xac]].concat();
        let generator_uncompressed = PublicKey::from_slice(
            &[
                &[2][..],
                &[
                    0x79, 0xbe, 0x66, 0x7e, 0xf9, 0xdc, 0xbb, 0xac, 0x55, 0xa0, 0x62, 0x95, 0xce,
                    0x87, 0x0b, 0x07, 0x02, 0x9b, 0xfc, 0xdb, 0x2d, 0xce, 0x28, 0xd9, 0x59, 0xf2,
                    0x81, 0x5b, 0x16, 0xf8, 0x17, 0x98,
                ],
            ]
            .concat(),
        )
        .unwrap()
        .serialize_uncompressed();
        let p2pk_uncompressed = [&[65][..], &generator_uncompressed, &[0xac]].concat();
        let invalid_uncompressed = [&[65, 4][..], &[0xff_u8; 64], &[0xac]].concat();
        // SEC1's rarely-used hybrid tag (0x06 = even Y, redundant with the
        // explicit Y bytes) is a real historic Bitcoin script pattern: height
        // 707,034's coin at the 935,000 mainnet snapshot is a P2PK output
        // using it. `libsecp256k1` parses hybrid keys as valid points, but
        // Core's CompressScript never compresses them, so they must stay raw
        // to round-trip exactly; only the tag byte differs from the
        // already-covered standard-uncompressed case above.
        let mut p2pk_hybrid_even_y = p2pk_uncompressed.clone();
        p2pk_hybrid_even_y[1] = 0x06;
        let raw = vec![0x51, 0xac];
        let empty: Vec<u8> = Vec::new();

        for (script, expected_prefix) in [
            (&p2pkh, 0_u8),
            (&p2sh, 1),
            (&compressed_p2pk, 2),
            (&p2pk_uncompressed, 4),
            (&invalid_uncompressed, 67 + 6),
            (&p2pk_hybrid_even_y, 67 + 6),
            (&raw, 2 + 6),
            (&empty, 6),
        ] {
            let mut bytes = Vec::new();
            compress_script(&mut bytes, script);
            assert_eq!(bytes[0], expected_prefix, "prefix for {script:02x?}");
            let decoded = decompress_script(&mut Cursor::new(bytes)).unwrap();
            assert_eq!(&decoded, script);
        }
    }

    /// Regression test for a real mainnet coin found while rebasing past
    /// height 935,000: a height-707,034 P2PK output using SEC1's hybrid
    /// uncompressed tag 0x07. `compress_script` previously accepted any tag
    /// `PublicKey::from_slice` parses, silently normalizing hybrid tags to
    /// the standard 0x04 on decompression and breaking the UTXO-set
    /// commitment. It must instead leave this script exactly as Core does:
    /// uncompressed, byte for byte.
    #[test]
    fn hybrid_tagged_uncompressed_pubkey_stays_raw_and_exact() {
        let script = <Vec<u8>>::from_hex(
            "410719c980631d9039e3abe7768ec96e91c97cb85073c579d3c8eebe12ec86f42fbf08be7bd54fa7ebe9dad426b6777bdb6ccaa00a69ff040ca526f67ccdd2bf40d3ac",
        )
        .unwrap();
        assert_eq!(script.len(), 67);
        assert_eq!(script[1], 0x07);
        let mut bytes = Vec::new();
        compress_script(&mut bytes, &script);
        // Raw fallback, not the lossy compressed-pubkey path.
        assert_eq!(bytes[0], 67 + 6);
        assert_eq!(decompress_script(&mut Cursor::new(bytes)).unwrap(), script);
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
}
