//! Minimal perfect hash function over a fixed key set (BBhash).
//!
//! This is the BBhash construction (Limasset, Rizk, Chikhi, Peterlongo,
//! "Fast and scalable minimal perfect hashing for massive key sets"): each
//! level hashes the still-unplaced keys into a bit array of `gamma * n`
//! positions, keeps the positions hit exactly once, and carries collided keys
//! into the next level. Ranking the concatenated level bit arrays maps every
//! key of the build set to a distinct index in `0..key_count`.
//!
//! Keys outside the build set map to `None` or to an arbitrary in-range
//! index. Callers that need exact membership must verify every hit against
//! authoritative data, as `core_snapshot_index` does against snapshot bytes.
//!
//! Hashing uses keyed SipHash-2-4 from the already-vendored `bitcoin_hashes`
//! implementation, so persisted functions are stable across platforms and
//! releases. The implementation is entirely safe Rust.

use bitcoin::hashes::siphash24;
use thiserror::Error;

/// Bit positions allocated per still-unplaced key on each level.
const GAMMA: u64 = 2;
/// Hard level ceiling; with `GAMMA = 2` the expected key carry rate per level
/// is below one half, so distinct keys exhaust far earlier. Only duplicate
/// keys can reach this bound, because duplicates collide on every level.
const MAX_LEVELS: u32 = 64;
/// Domain-separation constant mixed into the per-level SipHash key.
const LEVEL_KEY_SALT: u64 = 0x5242_5443_4d50_4846;
/// Words covered by one cumulative rank sample.
const RANK_BLOCK_WORDS: u64 = 8;

/// Failures while building or decoding a minimal perfect hash function.
#[derive(Debug, Error)]
pub enum MphfError {
    /// The build set is empty.
    #[error("cannot build a minimal perfect hash function over an empty key set")]
    Empty,
    /// Some keys still collided after the level ceiling, which distinct keys
    /// cannot reach; the key set contains duplicates.
    #[error("keys were not placed after {MAX_LEVELS} levels; the key set contains duplicates")]
    Unresolvable,
    /// A persisted representation is not canonical.
    #[error("malformed minimal perfect hash function: {0}")]
    Malformed(&'static str),
}

#[derive(Debug)]
struct Level {
    bit_count: u64,
    words: Vec<u64>,
    /// Set bits across all earlier levels.
    rank_before: u64,
    /// Cumulative set bits before each `RANK_BLOCK_WORDS` block of `words`.
    rank_samples: Vec<u64>,
}

impl Level {
    fn new(bit_count: u64, words: Vec<u64>, rank_before: u64) -> Self {
        let block_count = words.len().div_ceil(usize_from(RANK_BLOCK_WORDS));
        let mut rank_samples = Vec::with_capacity(block_count);
        let mut running = 0_u64;
        for (index, word) in words.iter().enumerate() {
            if index % usize_from(RANK_BLOCK_WORDS) == 0 {
                rank_samples.push(running);
            }
            running += u64::from(word.count_ones());
        }
        Self {
            bit_count,
            words,
            rank_before,
            rank_samples,
        }
    }

    fn set_bits(&self) -> u64 {
        self.words
            .iter()
            .map(|word| u64::from(word.count_ones()))
            .sum()
    }

    fn contains(&self, position: u64) -> bool {
        get_bit(&self.words, position)
    }

    /// Counts set bits strictly before `position` within this level.
    fn rank(&self, position: u64) -> u64 {
        let word_index = position / 64;
        let block = word_index / RANK_BLOCK_WORDS;
        let mut rank = self.rank_samples[usize_from(block)];
        for word in block * RANK_BLOCK_WORDS..word_index {
            rank += u64::from(self.words[usize_from(word)].count_ones());
        }
        let partial_mask = (1_u64 << (position % 64)) - 1;
        rank + u64::from((self.words[usize_from(word_index)] & partial_mask).count_ones())
    }
}

/// An immutable minimal perfect hash function over the key set it was built from.
#[derive(Debug)]
pub struct Mphf {
    seed: u64,
    key_count: u64,
    levels: Vec<Level>,
}

impl Mphf {
    /// Builds the function over `key_count` distinct keys.
    ///
    /// `key_at` must return the same bytes for the same ordinal on every
    /// call; the builder revisits still-colliding ordinals once per level.
    ///
    /// # Errors
    ///
    /// Returns [`MphfError::Empty`] for an empty set and
    /// [`MphfError::Unresolvable`] when the set contains duplicate keys.
    pub fn build<K, F>(key_count: u64, key_at: F, seed: u64) -> Result<Self, MphfError>
    where
        K: AsRef<[u8]>,
        F: Fn(u64) -> K,
    {
        if key_count == 0 {
            return Err(MphfError::Empty);
        }
        let mut remaining: Vec<u64> = (0..key_count).collect();
        let mut levels = Vec::new();
        let mut rank_before = 0_u64;
        while !remaining.is_empty() {
            let level_index =
                u32::try_from(levels.len()).expect("level count is bounded by MAX_LEVELS");
            if level_index == MAX_LEVELS {
                return Err(MphfError::Unresolvable);
            }
            let keys_here = u64::try_from(remaining.len()).expect("ordinal count fits u64");
            let bit_count = (keys_here * GAMMA).max(64).next_multiple_of(64);
            let word_count = usize_from(bit_count / 64);
            let mut first_hit = vec![0_u64; word_count];
            let mut collided = vec![0_u64; word_count];
            for &ordinal in &remaining {
                let position = position(seed, level_index, key_at(ordinal).as_ref(), bit_count);
                if get_bit(&first_hit, position) {
                    set_bit(&mut collided, position);
                } else {
                    set_bit(&mut first_hit, position);
                }
            }
            let words: Vec<u64> = first_hit
                .iter()
                .zip(&collided)
                .map(|(hit_word, collided_word)| hit_word & !collided_word)
                .collect();
            remaining.retain(|&ordinal| {
                let position = position(seed, level_index, key_at(ordinal).as_ref(), bit_count);
                get_bit(&collided, position)
            });
            let level = Level::new(bit_count, words, rank_before);
            rank_before += level.set_bits();
            levels.push(level);
        }
        if rank_before != key_count {
            return Err(MphfError::Malformed("placement count mismatch"));
        }
        Ok(Self {
            seed,
            key_count,
            levels,
        })
    }

    /// Maps a key to its index in `0..key_count`.
    ///
    /// Every key of the build set maps to a distinct `Some` index. A foreign
    /// key returns `None` or an arbitrary in-range index; the caller must
    /// verify hits against authoritative data.
    #[must_use]
    pub fn index(&self, key: &[u8]) -> Option<u64> {
        for (level_index, level) in self.levels.iter().enumerate() {
            let level_index =
                u32::try_from(level_index).expect("level count is bounded by MAX_LEVELS");
            let position = position(self.seed, level_index, key, level.bit_count);
            if level.contains(position) {
                return Some(level.rank_before + level.rank(position));
            }
        }
        None
    }

    /// Returns the number of keys in the build set.
    #[must_use]
    pub const fn key_count(&self) -> u64 {
        self.key_count
    }

    /// Returns the number of hash levels.
    #[must_use]
    pub fn level_count(&self) -> u32 {
        u32::try_from(self.levels.len()).expect("level count is bounded by MAX_LEVELS")
    }

    /// Returns the total bit-array size across all levels.
    #[must_use]
    pub fn bit_len(&self) -> u64 {
        self.levels.iter().map(|level| level.bit_count).sum()
    }

    /// Appends the canonical persisted representation.
    pub fn encode_into(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.seed.to_le_bytes());
        out.extend_from_slice(&self.key_count.to_le_bytes());
        out.extend_from_slice(&self.level_count().to_le_bytes());
        for level in &self.levels {
            out.extend_from_slice(&level.bit_count.to_le_bytes());
            for word in &level.words {
                out.extend_from_slice(&word.to_le_bytes());
            }
        }
    }

    /// Returns the exact persisted length in bytes.
    #[must_use]
    pub fn encoded_len(&self) -> u64 {
        let words: u64 = self
            .levels
            .iter()
            .map(|level| level.bit_count / 64)
            .sum::<u64>();
        8 + 8 + 4 + u64::from(self.level_count()) * 8 + words * 8
    }

    /// Decodes a canonical persisted representation from the front of
    /// `bytes` and returns the function plus the number of bytes consumed.
    ///
    /// # Errors
    ///
    /// Returns [`MphfError::Malformed`] for a truncated, non-canonical, or
    /// internally inconsistent representation.
    pub fn decode(bytes: &[u8]) -> Result<(Self, usize), MphfError> {
        let mut cursor = 0_usize;
        let seed = u64::from_le_bytes(take(bytes, &mut cursor, 8)?.try_into().expect("fixed"));
        let key_count = u64::from_le_bytes(take(bytes, &mut cursor, 8)?.try_into().expect("fixed"));
        let level_count =
            u32::from_le_bytes(take(bytes, &mut cursor, 4)?.try_into().expect("fixed"));
        if key_count == 0 {
            return Err(MphfError::Malformed("empty key set"));
        }
        if level_count == 0 || level_count > MAX_LEVELS {
            return Err(MphfError::Malformed("level count out of range"));
        }
        let mut levels = Vec::with_capacity(usize_from(u64::from(level_count)));
        let mut rank_before = 0_u64;
        for _ in 0..level_count {
            let bit_count =
                u64::from_le_bytes(take(bytes, &mut cursor, 8)?.try_into().expect("fixed"));
            if bit_count == 0 || bit_count % 64 != 0 {
                return Err(MphfError::Malformed("level bit count is not canonical"));
            }
            let word_count = bit_count / 64;
            let byte_count = word_count
                .checked_mul(8)
                .and_then(|byte_count| usize::try_from(byte_count).ok())
                .ok_or(MphfError::Malformed("level size overflow"))?;
            let word_bytes = take(bytes, &mut cursor, byte_count)?;
            let words: Vec<u64> = word_bytes
                .chunks_exact(8)
                .map(|chunk| u64::from_le_bytes(chunk.try_into().expect("chunked by 8")))
                .collect();
            let level = Level::new(bit_count, words, rank_before);
            rank_before += level.set_bits();
            levels.push(level);
        }
        if rank_before != key_count {
            return Err(MphfError::Malformed("set bits do not match key count"));
        }
        Ok((
            Self {
                seed,
                key_count,
                levels,
            },
            cursor,
        ))
    }
}

fn position(seed: u64, level_index: u32, key: &[u8], bit_count: u64) -> u64 {
    siphash24::Hash::hash_to_u64_with_keys(seed, LEVEL_KEY_SALT ^ u64::from(level_index), key)
        % bit_count
}

fn get_bit(words: &[u64], position: u64) -> bool {
    words[usize_from(position / 64)] & (1_u64 << (position % 64)) != 0
}

fn set_bit(words: &mut [u64], position: u64) {
    words[usize_from(position / 64)] |= 1_u64 << (position % 64);
}

fn usize_from(value: u64) -> usize {
    usize::try_from(value).expect("in-memory sizes fit usize")
}

fn take<'bytes>(
    bytes: &'bytes [u8],
    cursor: &mut usize,
    length: usize,
) -> Result<&'bytes [u8], MphfError> {
    let end = cursor
        .checked_add(length)
        .ok_or(MphfError::Malformed("length overflow"))?;
    let slice = bytes
        .get(*cursor..end)
        .ok_or(MphfError::Malformed("truncated"))?;
    *cursor = end;
    Ok(slice)
}

#[cfg(test)]
mod tests {
    use rand::{Rng, SeedableRng, rngs::StdRng};

    use super::*;

    fn random_keys(count: usize, seed: u64) -> Vec<[u8; 36]> {
        let mut rng = StdRng::seed_from_u64(seed);
        let mut keys = std::collections::BTreeSet::new();
        while keys.len() < count {
            let mut key = [0_u8; 36];
            rng.fill_bytes(&mut key);
            keys.insert(key);
        }
        keys.into_iter().collect()
    }

    #[test]
    fn build_is_a_bijection_over_the_key_set() {
        let keys = random_keys(10_000, 7);
        let mphf = Mphf::build(10_000, |ordinal| keys[usize_from(ordinal)], 42).unwrap();
        let mut seen = vec![false; keys.len()];
        for key in &keys {
            let index = usize_from(mphf.index(key).expect("build key maps to an index"));
            assert!(index < keys.len());
            assert!(!seen[index], "two keys mapped to slot {index}");
            seen[index] = true;
        }
        assert!(seen.iter().all(|slot| *slot));
        assert_eq!(mphf.key_count(), 10_000);
    }

    #[test]
    fn foreign_keys_stay_in_range_without_panicking() {
        let keys = random_keys(2_000, 11);
        let mphf = Mphf::build(2_000, |ordinal| keys[usize_from(ordinal)], 42).unwrap();
        for foreign in random_keys(2_000, 12) {
            if let Some(index) = mphf.index(&foreign) {
                assert!(index < 2_000);
            }
        }
    }

    #[test]
    fn build_is_deterministic_for_a_seed() {
        let keys = random_keys(1_000, 3);
        let mut first = Vec::new();
        Mphf::build(1_000, |ordinal| keys[usize_from(ordinal)], 9)
            .unwrap()
            .encode_into(&mut first);
        let mut second = Vec::new();
        Mphf::build(1_000, |ordinal| keys[usize_from(ordinal)], 9)
            .unwrap()
            .encode_into(&mut second);
        assert_eq!(first, second);
        let mut other_seed = Vec::new();
        Mphf::build(1_000, |ordinal| keys[usize_from(ordinal)], 10)
            .unwrap()
            .encode_into(&mut other_seed);
        assert_ne!(first, other_seed);
    }

    #[test]
    fn duplicate_keys_are_rejected_instead_of_looping() {
        let error = Mphf::build(2, |_| [5_u8; 36], 1).unwrap_err();
        assert!(matches!(error, MphfError::Unresolvable));
    }

    #[test]
    fn empty_key_sets_are_rejected() {
        let error = Mphf::build(0, |_| [0_u8; 36], 1).unwrap_err();
        assert!(matches!(error, MphfError::Empty));
    }

    #[test]
    fn encoding_roundtrips_and_preserves_every_index() {
        let keys = random_keys(4_096, 21);
        let mphf = Mphf::build(4_096, |ordinal| keys[usize_from(ordinal)], 77).unwrap();
        let mut bytes = Vec::new();
        mphf.encode_into(&mut bytes);
        assert_eq!(u64::try_from(bytes.len()).unwrap(), mphf.encoded_len());
        bytes.extend_from_slice(b"trailer");
        let (decoded, consumed) = Mphf::decode(&bytes).unwrap();
        assert_eq!(u64::try_from(consumed).unwrap(), mphf.encoded_len());
        for key in &keys {
            assert_eq!(decoded.index(key), mphf.index(key));
        }
    }

    #[test]
    fn malformed_encodings_are_rejected() {
        let keys = random_keys(64, 5);
        let mphf = Mphf::build(64, |ordinal| keys[usize_from(ordinal)], 3).unwrap();
        let mut bytes = Vec::new();
        mphf.encode_into(&mut bytes);

        assert!(matches!(
            Mphf::decode(&bytes[..bytes.len() - 1]).unwrap_err(),
            MphfError::Malformed("truncated")
        ));

        let mut zero_levels = bytes.clone();
        zero_levels[16..20].copy_from_slice(&0_u32.to_le_bytes());
        assert!(matches!(
            Mphf::decode(&zero_levels).unwrap_err(),
            MphfError::Malformed(_)
        ));

        let mut wrong_count = bytes.clone();
        wrong_count[8..16].copy_from_slice(&65_u64.to_le_bytes());
        assert!(matches!(
            Mphf::decode(&wrong_count).unwrap_err(),
            MphfError::Malformed("set bits do not match key count")
        ));

        let mut odd_bits = bytes;
        odd_bits[20..28].copy_from_slice(&63_u64.to_le_bytes());
        assert!(matches!(
            Mphf::decode(&odd_bits).unwrap_err(),
            MphfError::Malformed(_)
        ));
    }
}
