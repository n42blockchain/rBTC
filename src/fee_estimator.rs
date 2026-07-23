//! Bounded, restart-safe transaction confirmation observations.
//!
//! The estimator tracks only transactions that passed the local admission
//! policy. Confirmed and sufficiently old unconfirmed observations are used
//! together, avoiding the optimistic bias of sampling confirmations alone.

use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    fs,
    path::Path,
    sync::Mutex,
};

use bitcoin::{BlockHash, Network, Txid, hashes::Hash};
use redb::{Database, ReadableTable, TableDefinition};
use thiserror::Error;

const META: TableDefinition<&str, &[u8]> = TableDefinition::new("fee_estimator_metadata");
const SNAPSHOTS: TableDefinition<&str, &[u8]> = TableDefinition::new("fee_estimator_snapshots");
const GENESIS_KEY: &str = "genesis";
const STATE_KEY: &str = "active";
const STATE_VERSION: u8 = 1;
const TRACKED_BYTES: usize = 32 + 4 + 8 + 4;
const BLOCK_HEADER_BYTES: usize = 4 + 32 + 32 + 2;
const STATE_HEADER_BYTES: usize = 1 + 4 + 32 + 2 + 2;
const MAX_ACTIVE_POOL_TRACKS: usize = 64;
const MAX_BLOCK_HISTORY: usize = 1_008;
const MAX_CONFIRMED_OBSERVATIONS: usize = 4_096;
const MAX_PENDING_OBSERVATIONS: usize = MAX_CONFIRMED_OBSERVATIONS + MAX_ACTIVE_POOL_TRACKS;
const MAX_POLICY_VSIZE: u32 = 400_000;
const MAX_MONEY_SATS: u64 = 21_000_000 * 100_000_000;
const MIN_ESTIMATE_OBSERVATIONS: usize = 3;
const REQUIRED_SUCCESS_PERCENT: usize = 85;

/// One admitted transaction whose confirmation time can be observed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FeeTrack {
    /// Non-witness transaction identifier.
    pub txid: Txid,
    /// Exact transaction fee.
    pub fee_sats: u64,
    /// Sigop-adjusted policy virtual size.
    pub policy_vsize: u32,
}

/// A bounded empirical fee estimate.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FeeEstimate {
    /// Requested confirmation target in blocks.
    pub target_blocks: u16,
    /// Lowest whole-sat/vB threshold meeting the success requirement.
    pub fee_rate_sat_vb: u64,
    /// Mature observations evaluated at the selected threshold.
    pub considered: u32,
    /// Observations that confirmed within the requested target.
    pub successes: u32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct TrackedFee {
    txid: Txid,
    first_seen_height: u32,
    fee_sats: u64,
    policy_vsize: u32,
}

impl TrackedFee {
    fn rate_sat_kvb(self) -> u64 {
        let numerator = u128::from(self.fee_sats).saturating_mul(1_000);
        u64::try_from(numerator / u128::from(self.policy_vsize)).unwrap_or(u64::MAX)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ConfirmedBlock {
    height: u32,
    hash: BlockHash,
    parent: BlockHash,
    observations: Vec<TrackedFee>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct EstimatorState {
    tip_height: u32,
    tip_hash: BlockHash,
    pending: BTreeMap<Txid, TrackedFee>,
    blocks: VecDeque<ConfirmedBlock>,
}

/// Persistent estimator failures.
#[derive(Debug, Error)]
pub enum FeeEstimatorError {
    /// Database open/create failed.
    #[error("fee-estimator database: {0}")]
    Database(#[from] redb::DatabaseError),
    /// Database transaction creation failed.
    #[error("fee-estimator transaction: {0}")]
    Transaction(#[from] redb::TransactionError),
    /// Database table access failed.
    #[error("fee-estimator table: {0}")]
    Table(#[from] redb::TableError),
    /// Database key/value access failed.
    #[error("fee-estimator storage: {0}")]
    Storage(#[from] redb::StorageError),
    /// Database commit failed.
    #[error("fee-estimator commit: {0}")]
    Commit(#[from] redb::CommitError),
    /// Filesystem safety validation failed.
    #[error("fee-estimator file: {0}")]
    File(String),
    /// The database belongs to another Bitcoin network.
    #[error("fee-estimator database belongs to another Bitcoin network")]
    NetworkMismatch,
    /// Submitted or persisted state violates a strict bound or invariant.
    #[error("malformed fee-estimator state: {0}")]
    Malformed(&'static str),
    /// A block does not extend the estimator's current active tip.
    #[error(
        "fee-estimator block {height}:{hash} does not extend {tip_height}:{tip_hash} from parent {parent}"
    )]
    TipMismatch {
        /// Submitted block height.
        height: u32,
        /// Submitted block hash.
        hash: BlockHash,
        /// Submitted parent hash.
        parent: BlockHash,
        /// Current estimator height.
        tip_height: u32,
        /// Current estimator hash.
        tip_hash: BlockHash,
    },
}

/// Network-bound durable confirmation observer and estimator.
pub struct RedbFeeEstimator {
    db: Database,
    write_guard: Mutex<()>,
}

/// Validates one raw persisted estimator snapshot without opening a database.
///
/// This is the production parser entry point used by bounded fuzz regression.
pub fn validate_persisted_fee_estimator_state(bytes: &[u8]) -> Result<(), FeeEstimatorError> {
    decode_state(bytes).map(|_| ())
}

impl RedbFeeEstimator {
    /// Opens or creates an owner-only estimator store.
    pub fn open(path: impl AsRef<Path>, network: Network) -> Result<Self, FeeEstimatorError> {
        let path = path.as_ref();
        validate_file_before_open(path)?;
        let db = Database::create(path)?;
        restrict_file_permissions(path)?;
        let genesis = genesis_hash(network);
        let write = db.begin_write()?;
        {
            let mut meta = write.open_table(META)?;
            if let Some(stored) = meta.get(GENESIS_KEY)? {
                if stored.value() != genesis.as_slice() {
                    return Err(FeeEstimatorError::NetworkMismatch);
                }
            } else {
                meta.insert(GENESIS_KEY, genesis.as_slice())?;
            }
            let mut snapshots = write.open_table(SNAPSHOTS)?;
            if let Some(state) = snapshots.get(STATE_KEY)? {
                decode_state(state.value())?;
            } else {
                let state = EstimatorState {
                    tip_height: 0,
                    tip_hash: BlockHash::from_byte_array(genesis),
                    pending: BTreeMap::new(),
                    blocks: VecDeque::new(),
                };
                let encoded = encode_state(&state)?;
                snapshots.insert(STATE_KEY, encoded.as_slice())?;
            }
        }
        write.commit()?;
        Ok(Self {
            db,
            write_guard: Mutex::new(()),
        })
    }

    /// Returns the active-chain tip through which observations were processed.
    pub fn tip(&self) -> Result<(u32, BlockHash), FeeEstimatorError> {
        let state = self.read_state()?;
        Ok((state.tip_height, state.tip_hash))
    }

    /// Reconciles tracked entries with the complete active local pool.
    ///
    /// Existing entries preserve their first-seen height across restart.
    pub fn track_pool(
        &self,
        active: &[FeeTrack],
        first_seen_height: u32,
    ) -> Result<(), FeeEstimatorError> {
        if active.len() > MAX_ACTIVE_POOL_TRACKS {
            return Err(FeeEstimatorError::Malformed(
                "too many pending fee observations",
            ));
        }
        let mut submitted = BTreeMap::new();
        for entry in active {
            validate_track(entry.fee_sats, entry.policy_vsize)?;
            if submitted.insert(entry.txid, *entry).is_some() {
                return Err(FeeEstimatorError::Malformed(
                    "duplicate pending transaction",
                ));
            }
        }
        self.mutate(|state| {
            state.pending.retain(|txid, _| submitted.contains_key(txid));
            for entry in submitted.values() {
                if let Some(previous) = state.pending.get(&entry.txid) {
                    if previous.fee_sats != entry.fee_sats
                        || previous.policy_vsize != entry.policy_vsize
                    {
                        return Err(FeeEstimatorError::Malformed(
                            "pending transaction metadata changed",
                        ));
                    }
                } else {
                    state.pending.insert(
                        entry.txid,
                        TrackedFee {
                            txid: entry.txid,
                            first_seen_height,
                            fee_sats: entry.fee_sats,
                            policy_vsize: entry.policy_vsize,
                        },
                    );
                }
            }
            Ok(())
        })
    }

    /// Advances one active block and records matching pending transactions.
    pub fn connect_block(
        &self,
        height: u32,
        hash: BlockHash,
        parent: BlockHash,
        transaction_ids: &BTreeSet<Txid>,
    ) -> Result<usize, FeeEstimatorError> {
        self.mutate(|state| {
            let expected_height = state
                .tip_height
                .checked_add(1)
                .ok_or(FeeEstimatorError::Malformed("estimator height overflow"))?;
            if height != expected_height || parent != state.tip_hash {
                return Err(FeeEstimatorError::TipMismatch {
                    height,
                    hash,
                    parent,
                    tip_height: state.tip_height,
                    tip_hash: state.tip_hash,
                });
            }
            let matched = transaction_ids
                .iter()
                .filter_map(|txid| state.pending.remove(txid))
                .collect::<Vec<_>>();
            let matched_len = matched.len();
            state.blocks.push_back(ConfirmedBlock {
                height,
                hash,
                parent,
                observations: matched,
            });
            state.tip_height = height;
            state.tip_hash = hash;
            prune_history(state);
            Ok(matched_len)
        })
    }

    /// Disconnects the current estimator tip and restores its observations.
    pub fn disconnect_tip(&self, expected_hash: BlockHash) -> Result<usize, FeeEstimatorError> {
        self.mutate(|state| {
            if state.tip_hash != expected_hash {
                return Err(FeeEstimatorError::Malformed(
                    "disconnect hash is not the estimator tip",
                ));
            }
            let block = state.blocks.pop_back().ok_or(FeeEstimatorError::Malformed(
                "estimator history does not retain the tip",
            ))?;
            if block.hash != expected_hash || block.height != state.tip_height {
                return Err(FeeEstimatorError::Malformed(
                    "estimator tip history is inconsistent",
                ));
            }
            let restored = block.observations.len();
            for observation in block.observations {
                if let Some(previous) = state.pending.insert(observation.txid, observation) {
                    if previous != observation {
                        return Err(FeeEstimatorError::Malformed(
                            "restored fee observation conflicts with pending state",
                        ));
                    }
                }
            }
            state.tip_height = state.tip_height.saturating_sub(1);
            state.tip_hash = block.parent;
            Ok(restored)
        })
    }

    /// Resets history after a reorganization deeper than the retained journal.
    ///
    /// Pending state is also cleared so the caller can re-track the current
    /// pool with a conservative new first-seen height.
    pub fn reset_tip(&self, height: u32, hash: BlockHash) -> Result<(), FeeEstimatorError> {
        self.mutate(|state| {
            state.tip_height = height;
            state.tip_hash = hash;
            state.pending.clear();
            state.blocks.clear();
            Ok(())
        })
    }

    /// Estimates the lowest whole-sat/vB rate with at least 85% observed
    /// confirmation success for a target.
    ///
    /// At least three mature outcomes are required. Confirmations slower than
    /// the target and pending transactions old enough to have missed the target
    /// both count as failures.
    pub fn estimate(&self, target_blocks: u16) -> Result<Option<FeeEstimate>, FeeEstimatorError> {
        if target_blocks == 0 || usize::from(target_blocks) > MAX_BLOCK_HISTORY {
            return Err(FeeEstimatorError::Malformed(
                "fee target is outside retained history",
            ));
        }
        let state = self.read_state()?;
        let mut rates = state
            .blocks
            .iter()
            .flat_map(|block| block.observations.iter())
            .chain(state.pending.values())
            .map(|observation| observation.rate_sat_kvb())
            .filter(|rate| *rate > 0)
            .collect::<BTreeSet<_>>();
        let target = u32::from(target_blocks);
        let mut selected = None;
        for threshold in std::mem::take(&mut rates) {
            let mut successes = 0_usize;
            let mut considered = 0_usize;
            for block in &state.blocks {
                for observation in &block.observations {
                    if observation.rate_sat_kvb() < threshold {
                        continue;
                    }
                    let delay = block
                        .height
                        .saturating_sub(observation.first_seen_height)
                        .saturating_add(1);
                    considered += 1;
                    if delay <= target {
                        successes += 1;
                    }
                }
            }
            for observation in state.pending.values() {
                if observation.rate_sat_kvb() < threshold {
                    continue;
                }
                let age = state
                    .tip_height
                    .saturating_sub(observation.first_seen_height)
                    .saturating_add(u32::from(state.tip_height >= observation.first_seen_height));
                if age >= target {
                    considered += 1;
                }
            }
            if considered >= MIN_ESTIMATE_OBSERVATIONS
                && successes.saturating_mul(100)
                    >= considered.saturating_mul(REQUIRED_SUCCESS_PERCENT)
            {
                selected = Some(FeeEstimate {
                    target_blocks,
                    fee_rate_sat_vb: threshold.saturating_add(999) / 1_000,
                    considered: u32::try_from(considered).unwrap_or(u32::MAX),
                    successes: u32::try_from(successes).unwrap_or(u32::MAX),
                });
                break;
            }
        }
        Ok(selected)
    }

    fn read_state(&self) -> Result<EstimatorState, FeeEstimatorError> {
        let read = self.db.begin_read()?;
        let snapshots = read.open_table(SNAPSHOTS)?;
        let state = snapshots
            .get(STATE_KEY)?
            .ok_or(FeeEstimatorError::Malformed("missing estimator state"))?;
        decode_state(state.value())
    }

    fn mutate<T>(
        &self,
        mutation: impl FnOnce(&mut EstimatorState) -> Result<T, FeeEstimatorError>,
    ) -> Result<T, FeeEstimatorError> {
        let _guard = self
            .write_guard
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut state = self.read_state()?;
        let result = mutation(&mut state)?;
        let encoded = encode_state(&state)?;
        let write = self.db.begin_write()?;
        {
            let mut snapshots = write.open_table(SNAPSHOTS)?;
            snapshots.insert(STATE_KEY, encoded.as_slice())?;
        }
        write.commit()?;
        Ok(result)
    }
}

fn prune_history(state: &mut EstimatorState) {
    while state.blocks.len() > MAX_BLOCK_HISTORY
        || confirmed_observation_count(&state.blocks) > MAX_CONFIRMED_OBSERVATIONS
    {
        state.blocks.pop_front();
    }
}

fn confirmed_observation_count(blocks: &VecDeque<ConfirmedBlock>) -> usize {
    blocks.iter().map(|block| block.observations.len()).sum()
}

fn validate_track(fee_sats: u64, policy_vsize: u32) -> Result<(), FeeEstimatorError> {
    if fee_sats > MAX_MONEY_SATS {
        return Err(FeeEstimatorError::Malformed(
            "fee observation exceeds MoneyRange",
        ));
    }
    if policy_vsize == 0 || policy_vsize > MAX_POLICY_VSIZE {
        return Err(FeeEstimatorError::Malformed(
            "fee observation policy vsize is invalid",
        ));
    }
    Ok(())
}

fn encode_state(state: &EstimatorState) -> Result<Vec<u8>, FeeEstimatorError> {
    validate_state(state)?;
    let observation_count = confirmed_observation_count(&state.blocks);
    let capacity = STATE_HEADER_BYTES
        .saturating_add(state.pending.len().saturating_mul(TRACKED_BYTES))
        .saturating_add(state.blocks.len().saturating_mul(BLOCK_HEADER_BYTES))
        .saturating_add(observation_count.saturating_mul(TRACKED_BYTES));
    let mut encoded = Vec::with_capacity(capacity);
    encoded.push(STATE_VERSION);
    encoded.extend_from_slice(&state.tip_height.to_le_bytes());
    encoded.extend_from_slice(state.tip_hash.as_byte_array());
    encoded.extend_from_slice(
        &u16::try_from(state.pending.len())
            .map_err(|_| FeeEstimatorError::Malformed("too many pending fee observations"))?
            .to_le_bytes(),
    );
    for observation in state.pending.values() {
        encode_tracked(&mut encoded, *observation);
    }
    encoded.extend_from_slice(
        &u16::try_from(state.blocks.len())
            .map_err(|_| FeeEstimatorError::Malformed("too many fee-estimator blocks"))?
            .to_le_bytes(),
    );
    for block in &state.blocks {
        encoded.extend_from_slice(&block.height.to_le_bytes());
        encoded.extend_from_slice(block.hash.as_byte_array());
        encoded.extend_from_slice(block.parent.as_byte_array());
        encoded.extend_from_slice(
            &u16::try_from(block.observations.len())
                .map_err(|_| {
                    FeeEstimatorError::Malformed("too many observations in estimator block")
                })?
                .to_le_bytes(),
        );
        for observation in &block.observations {
            encode_tracked(&mut encoded, *observation);
        }
    }
    Ok(encoded)
}

fn encode_tracked(encoded: &mut Vec<u8>, observation: TrackedFee) {
    encoded.extend_from_slice(observation.txid.as_byte_array());
    encoded.extend_from_slice(&observation.first_seen_height.to_le_bytes());
    encoded.extend_from_slice(&observation.fee_sats.to_le_bytes());
    encoded.extend_from_slice(&observation.policy_vsize.to_le_bytes());
}

fn decode_state(bytes: &[u8]) -> Result<EstimatorState, FeeEstimatorError> {
    let mut decoder = Decoder::new(bytes);
    if decoder.u8()? != STATE_VERSION {
        return Err(FeeEstimatorError::Malformed(
            "unsupported fee-estimator version",
        ));
    }
    let tip_height = decoder.u32()?;
    let tip_hash = BlockHash::from_byte_array(decoder.array()?);
    let pending_count = usize::from(decoder.u16()?);
    if pending_count > MAX_PENDING_OBSERVATIONS {
        return Err(FeeEstimatorError::Malformed(
            "too many pending fee observations",
        ));
    }
    let mut pending = BTreeMap::new();
    for _ in 0..pending_count {
        let observation = decode_tracked(&mut decoder)?;
        if pending.insert(observation.txid, observation).is_some() {
            return Err(FeeEstimatorError::Malformed(
                "duplicate pending transaction",
            ));
        }
    }
    let block_count = usize::from(decoder.u16()?);
    if block_count > MAX_BLOCK_HISTORY {
        return Err(FeeEstimatorError::Malformed(
            "too many fee-estimator blocks",
        ));
    }
    let mut blocks = VecDeque::with_capacity(block_count);
    let mut total_observations = 0_usize;
    for _ in 0..block_count {
        let height = decoder.u32()?;
        let hash = BlockHash::from_byte_array(decoder.array()?);
        let parent = BlockHash::from_byte_array(decoder.array()?);
        let count = usize::from(decoder.u16()?);
        total_observations = total_observations.saturating_add(count);
        if total_observations > MAX_CONFIRMED_OBSERVATIONS {
            return Err(FeeEstimatorError::Malformed(
                "too many confirmed fee observations",
            ));
        }
        let mut observations = Vec::with_capacity(count);
        for _ in 0..count {
            observations.push(decode_tracked(&mut decoder)?);
        }
        blocks.push_back(ConfirmedBlock {
            height,
            hash,
            parent,
            observations,
        });
    }
    if !decoder.finished() {
        return Err(FeeEstimatorError::Malformed("trailing fee-estimator bytes"));
    }
    let state = EstimatorState {
        tip_height,
        tip_hash,
        pending,
        blocks,
    };
    validate_state(&state)?;
    Ok(state)
}

fn decode_tracked(decoder: &mut Decoder<'_>) -> Result<TrackedFee, FeeEstimatorError> {
    let observation = TrackedFee {
        txid: Txid::from_byte_array(decoder.array()?),
        first_seen_height: decoder.u32()?,
        fee_sats: decoder.u64()?,
        policy_vsize: decoder.u32()?,
    };
    validate_track(observation.fee_sats, observation.policy_vsize)?;
    Ok(observation)
}

fn validate_state(state: &EstimatorState) -> Result<(), FeeEstimatorError> {
    if state.pending.len() > MAX_PENDING_OBSERVATIONS
        || state.blocks.len() > MAX_BLOCK_HISTORY
        || confirmed_observation_count(&state.blocks) > MAX_CONFIRMED_OBSERVATIONS
    {
        return Err(FeeEstimatorError::Malformed(
            "fee-estimator resource bound exceeded",
        ));
    }
    let mut previous = None;
    let mut confirmed = BTreeSet::new();
    for block in &state.blocks {
        if let Some((height, hash)) = previous {
            if block.height != height + 1 || block.parent != hash {
                return Err(FeeEstimatorError::Malformed(
                    "fee-estimator block history is disconnected",
                ));
            }
        }
        for observation in &block.observations {
            validate_track(observation.fee_sats, observation.policy_vsize)?;
            if observation.first_seen_height > block.height {
                return Err(FeeEstimatorError::Malformed(
                    "fee observation predates no confirmation opportunity",
                ));
            }
            if !confirmed.insert(observation.txid) {
                return Err(FeeEstimatorError::Malformed(
                    "duplicate confirmed fee observation",
                ));
            }
        }
        previous = Some((block.height, block.hash));
    }
    if let Some((height, hash)) = previous {
        if height != state.tip_height || hash != state.tip_hash {
            return Err(FeeEstimatorError::Malformed(
                "fee-estimator history does not end at its tip",
            ));
        }
    }
    if state.pending.keys().any(|txid| confirmed.contains(txid)) {
        return Err(FeeEstimatorError::Malformed(
            "fee observation is both pending and confirmed",
        ));
    }
    for observation in state.pending.values() {
        validate_track(observation.fee_sats, observation.policy_vsize)?;
    }
    Ok(())
}

struct Decoder<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Decoder<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8], FeeEstimatorError> {
        let end = self
            .offset
            .checked_add(len)
            .ok_or(FeeEstimatorError::Malformed(
                "fee-estimator length overflow",
            ))?;
        let value = self
            .bytes
            .get(self.offset..end)
            .ok_or(FeeEstimatorError::Malformed(
                "truncated fee-estimator state",
            ))?;
        self.offset = end;
        Ok(value)
    }

    fn u8(&mut self) -> Result<u8, FeeEstimatorError> {
        Ok(self.take(1)?[0])
    }

    fn u16(&mut self) -> Result<u16, FeeEstimatorError> {
        Ok(u16::from_le_bytes(self.array()?))
    }

    fn u32(&mut self) -> Result<u32, FeeEstimatorError> {
        Ok(u32::from_le_bytes(self.array()?))
    }

    fn u64(&mut self) -> Result<u64, FeeEstimatorError> {
        Ok(u64::from_le_bytes(self.array()?))
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N], FeeEstimatorError> {
        self.take(N)?
            .try_into()
            .map_err(|_| FeeEstimatorError::Malformed("invalid fee-estimator field"))
    }

    const fn finished(&self) -> bool {
        self.offset == self.bytes.len()
    }
}

fn genesis_hash(network: Network) -> [u8; 32] {
    bitcoin::blockdata::constants::genesis_block(network)
        .block_hash()
        .to_byte_array()
}

fn validate_file_before_open(path: &Path) -> Result<(), FeeEstimatorError> {
    let Ok(metadata) = fs::symlink_metadata(path) else {
        return Ok(());
    };
    if metadata.file_type().is_symlink() || !metadata.file_type().is_file() {
        return Err(FeeEstimatorError::File(
            "path must be a regular non-symlink file".to_owned(),
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o077 != 0 {
            return Err(FeeEstimatorError::File(
                "permissions must deny group and other access".to_owned(),
            ));
        }
    }
    Ok(())
}

fn restrict_file_permissions(path: &Path) -> Result<(), FeeEstimatorError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))
            .map_err(|error| FeeEstimatorError::File(error.to_string()))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;

    fn hash(marker: u8) -> BlockHash {
        BlockHash::from_byte_array([marker; 32])
    }

    fn track(marker: u8, rate_sat_vb: u64) -> FeeTrack {
        FeeTrack {
            txid: Txid::from_byte_array([marker; 32]),
            fee_sats: rate_sat_vb * 100,
            policy_vsize: 100,
        }
    }

    #[test]
    fn confirmations_and_mature_pending_failures_produce_a_conservative_estimate() {
        let directory = TempDir::new().unwrap();
        let estimator =
            RedbFeeEstimator::open(directory.path().join("fees.redb"), Network::Regtest).unwrap();
        let genesis = hash(0);
        estimator.reset_tip(100, genesis).unwrap();
        let entries = [track(1, 1), track(2, 2), track(3, 2), track(4, 2)];
        estimator.track_pool(&entries, 101).unwrap();
        estimator
            .connect_block(
                101,
                hash(1),
                genesis,
                &BTreeSet::from([entries[1].txid, entries[2].txid, entries[3].txid]),
            )
            .unwrap();
        assert_eq!(
            estimator.estimate(1).unwrap(),
            Some(FeeEstimate {
                target_blocks: 1,
                fee_rate_sat_vb: 2,
                considered: 3,
                successes: 3,
            })
        );
        estimator
            .connect_block(102, hash(2), hash(1), &BTreeSet::new())
            .unwrap();
        assert_eq!(estimator.estimate(1).unwrap().unwrap().fee_rate_sat_vb, 2);
    }

    #[test]
    fn reopen_and_disconnect_restore_exact_pending_metadata() {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("fees.redb");
        let genesis = hash(10);
        let entry = track(11, 7);
        {
            let estimator = RedbFeeEstimator::open(&path, Network::Signet).unwrap();
            estimator.reset_tip(20, genesis).unwrap();
            estimator.track_pool(&[entry], 21).unwrap();
            assert_eq!(
                estimator
                    .connect_block(21, hash(12), genesis, &BTreeSet::from([entry.txid]))
                    .unwrap(),
                1
            );
        }
        let estimator = RedbFeeEstimator::open(&path, Network::Signet).unwrap();
        assert_eq!(estimator.tip().unwrap(), (21, hash(12)));
        assert_eq!(estimator.disconnect_tip(hash(12)).unwrap(), 1);
        assert_eq!(estimator.tip().unwrap(), (20, genesis));
        estimator.track_pool(&[entry], 999).unwrap();
        estimator
            .connect_block(21, hash(13), genesis, &BTreeSet::from([entry.txid]))
            .unwrap();
        let state = estimator.read_state().unwrap();
        assert_eq!(state.blocks[0].observations[0].first_seen_height, 21);
        drop(estimator);
        assert!(matches!(
            RedbFeeEstimator::open(&path, Network::Regtest),
            Err(FeeEstimatorError::NetworkMismatch)
        ));
    }

    #[test]
    fn multi_block_reorg_restores_more_than_the_live_pool_capacity() {
        let directory = TempDir::new().unwrap();
        let estimator =
            RedbFeeEstimator::open(directory.path().join("fees.redb"), Network::Regtest).unwrap();
        let genesis = hash(20);
        estimator.reset_tip(100, genesis).unwrap();
        let first = (1..=64).map(|marker| track(marker, 2)).collect::<Vec<_>>();
        estimator.track_pool(&first, 101).unwrap();
        estimator
            .connect_block(
                101,
                hash(21),
                genesis,
                &first.iter().map(|entry| entry.txid).collect(),
            )
            .unwrap();
        let second = (65..=128)
            .map(|marker| track(marker, 3))
            .collect::<Vec<_>>();
        estimator.track_pool(&second, 102).unwrap();
        estimator
            .connect_block(
                102,
                hash(22),
                hash(21),
                &second.iter().map(|entry| entry.txid).collect(),
            )
            .unwrap();
        let live = (129..=192)
            .map(|marker| track(marker, 4))
            .collect::<Vec<_>>();
        estimator.track_pool(&live, 103).unwrap();

        assert_eq!(estimator.disconnect_tip(hash(22)).unwrap(), 64);
        assert_eq!(estimator.disconnect_tip(hash(21)).unwrap(), 64);
        assert_eq!(estimator.read_state().unwrap().pending.len(), 192);
        estimator.track_pool(&live, 103).unwrap();
        assert_eq!(estimator.read_state().unwrap().pending.len(), 64);
    }

    #[test]
    fn malformed_and_disconnected_updates_do_not_rewrite_state() {
        let directory = TempDir::new().unwrap();
        let estimator =
            RedbFeeEstimator::open(directory.path().join("fees.redb"), Network::Regtest).unwrap();
        let tip = estimator.tip().unwrap();
        assert!(matches!(
            estimator.connect_block(2, hash(1), tip.1, &BTreeSet::new()),
            Err(FeeEstimatorError::TipMismatch { .. })
        ));
        assert_eq!(estimator.tip().unwrap(), tip);
        assert!(
            estimator
                .track_pool(
                    &[FeeTrack {
                        txid: Txid::from_byte_array([1; 32]),
                        fee_sats: 1,
                        policy_vsize: 0,
                    }],
                    1,
                )
                .is_err()
        );
        assert_eq!(estimator.tip().unwrap(), tip);
    }
}
