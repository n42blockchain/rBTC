//! Header DAG and cumulative-work active-chain selection.
//!
//! This module establishes the structural proof-of-work boundary. Difficulty
//! transition rules, median-time-past, checkpoints, and deployment activation
//! are intentionally separate contextual validation gates.

use std::{collections::HashMap, str::FromStr};

use bitcoin::{
    Network,
    block::{BlockHash, Header},
    consensus::Params,
    pow::{CompactTarget, Work},
};
use thiserror::Error;

use crate::deployments::DeploymentConfig;

/// Bitcoin Core's maximum permitted future block timestamp offset.
pub const MAX_FUTURE_BLOCK_TIME_SECS: u32 = 2 * 60 * 60;

/// Stored metadata for a proof-of-work-valid header.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HeaderInfo {
    /// The raw 80-byte Bitcoin block header.
    pub header: Header,
    /// Header's block hash.
    pub hash: BlockHash,
    /// Height measured from the network genesis header.
    pub height: u32,
    /// Sum of per-header work from genesis through this header.
    pub chainwork: Work,
}

/// Rejection reason for a header DAG insertion.
#[derive(Debug, Error)]
pub enum HeaderError {
    /// The header is already present in this DAG.
    #[error("duplicate header {0}")]
    Duplicate(BlockHash),
    /// The parent is not known; headers-first sync must request it first.
    #[error("unknown parent {0}")]
    UnknownParent(BlockHash),
    /// The compact target is zero or exceeds the selected network's PoW limit.
    #[error("target exceeds network proof-of-work limit")]
    InvalidTarget,
    /// The header hash does not satisfy its declared target.
    #[error("invalid proof of work: {0}")]
    InvalidProofOfWork(#[source] bitcoin::block::ValidationError),
    /// The parent height cannot be incremented without overflowing.
    #[error("header height overflow")]
    HeightOverflow,
    /// The timestamp is not strictly later than the parent chain's median time past.
    #[error("header timestamp {time} is not later than median time past {median}")]
    TimeTooOld {
        /// Candidate header timestamp.
        time: u32,
        /// Median of the previous up-to-eleven timestamps.
        median: u32,
    },
    /// The timestamp is too far beyond the network-adjusted clock.
    #[error("header timestamp {time} exceeds adjusted time limit {maximum}")]
    TimeTooNew {
        /// Candidate header timestamp.
        time: u32,
        /// Largest accepted timestamp for the supplied adjusted clock.
        maximum: u32,
    },
    /// The retained header graph lacks an ancestor needed for a retarget.
    #[error("missing retarget ancestor at height {0}")]
    MissingRetargetAncestor(u32),
    /// The compact target does not match the network's next-work rule.
    #[error("unexpected difficulty bits: expected {expected:#010x}, got {actual:#010x}")]
    UnexpectedDifficulty {
        /// Expected Bitcoin compact target.
        expected: u32,
        /// Compact target present in the candidate header.
        actual: u32,
    },
    /// A header at a hardened checkpoint height has the wrong hash.
    #[error("checkpoint mismatch at height {height}: expected {expected}, got {actual}")]
    CheckpointMismatch {
        /// Hardened checkpoint height.
        height: u32,
        /// Hash pinned by the selected network parameters.
        expected: BlockHash,
        /// Candidate header hash.
        actual: BlockHash,
    },
    /// A buried deployment requires a newer block-header version.
    #[error("obsolete block version {actual} at height {height}; minimum version is {required}")]
    ObsoleteVersion {
        /// Candidate height.
        height: u32,
        /// Minimum version selected by buried deployments.
        required: i32,
        /// Candidate's signed consensus version.
        actual: i32,
    },
}

impl HeaderError {
    /// Returns whether the header is objectively invalid consensus data from a peer.
    ///
    /// Future-time headers, disconnected/duplicate batches, and local DAG consistency failures
    /// are not attributed as malicious peer behavior.
    #[must_use]
    pub const fn is_peer_invalid(&self) -> bool {
        matches!(
            self,
            Self::InvalidTarget
                | Self::InvalidProofOfWork(_)
                | Self::TimeTooOld { .. }
                | Self::UnexpectedDifficulty { .. }
                | Self::CheckpointMismatch { .. }
                | Self::ObsoleteVersion { .. }
        )
    }
}

/// In-memory header DAG with the best-work tip selected independently of arrival order.
#[derive(Clone)]
pub struct HeaderDag {
    params: Params,
    deployments: DeploymentConfig,
    headers: HashMap<BlockHash, HeaderInfo>,
    active_tip: BlockHash,
    active_chain: Vec<BlockHash>,
}

impl HeaderDag {
    /// Starts a DAG at the selected network's consensus genesis header.
    #[must_use]
    pub fn new(network: Network) -> Self {
        Self::with_deployments(DeploymentConfig::for_network(network))
    }

    /// Starts a DAG using an explicitly selected deployment configuration.
    #[must_use]
    pub fn with_deployments(deployments: DeploymentConfig) -> Self {
        let network = deployments.network();
        let header = bitcoin::blockdata::constants::genesis_block(network).header;
        let hash = header.block_hash();
        let info = HeaderInfo {
            header,
            hash,
            height: 0,
            chainwork: header.target().to_work(),
        };
        let mut headers = HashMap::new();
        headers.insert(hash, info);
        Self {
            params: Params::new(network),
            deployments,
            headers,
            active_tip: hash,
            active_chain: vec![hash],
        }
    }

    /// Returns the selected network.
    #[must_use]
    pub const fn network(&self) -> Network {
        self.params.network
    }

    /// Returns the highest cumulative-work header.
    #[must_use]
    pub fn active_tip(&self) -> HeaderInfo {
        self.headers[&self.active_tip]
    }

    /// Returns the active-chain header at `height`.
    #[must_use]
    pub fn active_header_at(&self, height: u32) -> Option<HeaderInfo> {
        let index = usize::try_from(height).ok()?;
        self.active_chain
            .get(index)
            .and_then(|hash| self.headers.get(hash))
            .copied()
    }

    /// Builds a standard newest-to-oldest block locator for `getheaders`.
    ///
    /// The first ten entries walk one header at a time; thereafter the step
    /// doubles until genesis. This retains recent reorg precision while
    /// bounding the locator to the protocol's expected small size.
    #[must_use]
    pub fn block_locator(&self) -> Vec<BlockHash> {
        let mut locator = Vec::new();
        let mut current = self.active_tip();
        let mut step = 1_u32;
        while current.height > 0 {
            locator.push(current.hash);
            let next_height = current.height.saturating_sub(step);
            current = self
                .ancestor_at_height(current, next_height)
                .expect("active chain has all ancestors");
            if locator.len() >= 10 {
                step = step.saturating_mul(2);
            }
        }
        locator.push(current.hash);
        locator
    }

    /// Contextually validates a contiguous received header batch atomically.
    ///
    /// If any header fails, this DAG remains unchanged. Persistence callers
    /// should commit the same raw batch before replacing their in-memory DAG,
    /// giving crash recovery a replayable prefix only.
    pub fn validate_batch_contextual(
        &self,
        headers: &[Header],
        adjusted_time: u32,
    ) -> Result<(Self, Vec<HeaderInfo>), HeaderError> {
        let mut candidate = self.clone();
        let mut accepted = Vec::with_capacity(headers.len());
        for header in headers {
            accepted.push(candidate.insert_contextual(*header, adjusted_time)?);
        }
        Ok((candidate, accepted))
    }

    /// Finds any known header, including side-chain headers.
    #[must_use]
    pub fn get(&self, hash: &BlockHash) -> Option<HeaderInfo> {
        self.headers.get(hash).copied()
    }

    /// Calculates the median timestamp of a header and up to ten ancestors.
    #[must_use]
    pub fn median_time_past(&self, hash: BlockHash) -> Option<u32> {
        let mut times = Vec::with_capacity(11);
        let mut current = self.headers.get(&hash).copied()?;
        times.push(current.header.time);
        for _ in 1..11 {
            let Some(parent) = self.headers.get(&current.header.prev_blockhash).copied() else {
                break;
            };
            current = parent;
            times.push(current.header.time);
        }
        times.sort_unstable();
        Some(times[times.len() / 2])
    }

    /// Computes the compact target required for a candidate's parent branch.
    ///
    /// This handles the normal 2016-block retarget, no-retarget networks, and
    /// testnet-style minimum-difficulty exceptions. It does not validate PoW.
    ///
    /// # Errors
    ///
    /// Returns an error if the parent or the retarget-epoch ancestor is absent.
    pub fn expected_next_bits(&self, candidate: &Header) -> Result<CompactTarget, HeaderError> {
        let parent = self
            .headers
            .get(&candidate.prev_blockhash)
            .copied()
            .ok_or(HeaderError::UnknownParent(candidate.prev_blockhash))?;
        let next_height = parent
            .height
            .checked_add(1)
            .ok_or(HeaderError::HeightOverflow)?;
        let interval = self.params.difficulty_adjustment_interval();
        let interval = u32::try_from(interval).expect("Bitcoin interval fits u32");

        if next_height % interval == 0 {
            let boundary_height = next_height - interval;
            let boundary = self
                .ancestor_at_height(parent, boundary_height)
                .ok_or(HeaderError::MissingRetargetAncestor(boundary_height))?;
            let elapsed = u64::from(parent.header.time.saturating_sub(boundary.header.time));
            return Ok(CompactTarget::from_next_work_required(
                parent.header.bits,
                elapsed,
                &self.params,
            ));
        }

        if self.params.no_pow_retargeting {
            return Ok(parent.header.bits);
        }
        if !self.params.allow_min_difficulty_blocks {
            return Ok(parent.header.bits);
        }

        let min_difficulty_time = parent.header.time.saturating_add(
            u32::try_from(self.params.pow_target_spacing * 2).expect("spacing fits u32"),
        );
        if candidate.time > min_difficulty_time {
            return Ok(self.params.max_attainable_target.to_compact_lossy());
        }

        let pow_limit = self.params.max_attainable_target.to_compact_lossy();
        let mut cursor = parent;
        while cursor.height % interval != 0 && cursor.header.bits == pow_limit {
            cursor = self
                .headers
                .get(&cursor.header.prev_blockhash)
                .copied()
                .ok_or(HeaderError::MissingRetargetAncestor(cursor.height - 1))?;
        }
        Ok(cursor.header.bits)
    }

    /// Applies timestamp context before structurally inserting a header.
    ///
    /// `adjusted_time` must come from the P2P network-time subsystem rather
    /// than the local wall clock alone.
    ///
    /// # Errors
    ///
    /// Returns timestamp, structural, target, or proof-of-work validation errors.
    pub fn insert_contextual(
        &mut self,
        header: Header,
        adjusted_time: u32,
    ) -> Result<HeaderInfo, HeaderError> {
        let parent = self
            .headers
            .get(&header.prev_blockhash)
            .copied()
            .ok_or(HeaderError::UnknownParent(header.prev_blockhash))?;
        let height = parent
            .height
            .checked_add(1)
            .ok_or(HeaderError::HeightOverflow)?;
        let required_version = self.deployments.minimum_block_version(height);
        let actual_version = header.version.to_consensus();
        if actual_version < required_version {
            return Err(HeaderError::ObsoleteVersion {
                height,
                required: required_version,
                actual: actual_version,
            });
        }
        let median = self
            .median_time_past(parent.hash)
            .expect("known parent has a median timestamp");
        if header.time <= median {
            return Err(HeaderError::TimeTooOld {
                time: header.time,
                median,
            });
        }
        let maximum = adjusted_time.saturating_add(MAX_FUTURE_BLOCK_TIME_SECS);
        if header.time > maximum {
            return Err(HeaderError::TimeTooNew {
                time: header.time,
                maximum,
            });
        }
        let expected = self.expected_next_bits(&header)?;
        if header.bits != expected {
            return Err(HeaderError::UnexpectedDifficulty {
                expected: expected.to_consensus(),
                actual: header.bits.to_consensus(),
            });
        }
        self.insert(header)
    }

    /// Adds a proof-of-work-valid child and promotes it if it has more chainwork.
    ///
    /// # Errors
    ///
    /// Returns an error for duplicates, missing parents, invalid targets, invalid
    /// proof of work, or unrepresentable heights.
    pub fn insert(&mut self, header: Header) -> Result<HeaderInfo, HeaderError> {
        let hash = header.block_hash();
        if self.headers.contains_key(&hash) {
            return Err(HeaderError::Duplicate(hash));
        }
        let parent = self
            .headers
            .get(&header.prev_blockhash)
            .copied()
            .ok_or(HeaderError::UnknownParent(header.prev_blockhash))?;
        let height = parent
            .height
            .checked_add(1)
            .ok_or(HeaderError::HeightOverflow)?;
        if let Some(expected) = checkpoint_hash(self.params.network, height) {
            if hash != expected {
                return Err(HeaderError::CheckpointMismatch {
                    height,
                    expected,
                    actual: hash,
                });
            }
        }
        let target = header.target();
        if target == bitcoin::pow::Target::ZERO || target > self.params.max_attainable_target {
            return Err(HeaderError::InvalidTarget);
        }
        header
            .validate_pow(target)
            .map_err(HeaderError::InvalidProofOfWork)?;
        let info = HeaderInfo {
            header,
            hash,
            height,
            chainwork: parent.chainwork + target.to_work(),
        };
        self.headers.insert(hash, info);
        if info.chainwork > self.active_tip().chainwork {
            self.promote_active_tip(info);
        }
        Ok(info)
    }

    fn promote_active_tip(&mut self, info: HeaderInfo) {
        if info.header.prev_blockhash == self.active_tip
            && usize::try_from(info.height).ok() == Some(self.active_chain.len())
        {
            self.active_chain.push(info.hash);
            self.active_tip = info.hash;
            return;
        }

        let chain_len = usize::try_from(info.height)
            .expect("header height fits usize")
            .checked_add(1)
            .expect("active-chain length fits usize");
        let mut active_chain = vec![info.hash; chain_len];
        let mut current = info;
        loop {
            active_chain[usize::try_from(current.height).expect("header height fits usize")] =
                current.hash;
            if current.height == 0 {
                break;
            }
            current = self
                .headers
                .get(&current.header.prev_blockhash)
                .copied()
                .expect("known header has every ancestor");
        }
        self.active_tip = info.hash;
        self.active_chain = active_chain;
    }

    fn ancestor_at_height(&self, mut current: HeaderInfo, height: u32) -> Option<HeaderInfo> {
        while current.height > height {
            current = self.headers.get(&current.header.prev_blockhash).copied()?;
        }
        (current.height == height).then_some(current)
    }
}

fn checkpoint_hash(network: Network, height: u32) -> Option<BlockHash> {
    let hash = match (network, height) {
        (Network::Bitcoin, 11_111) => {
            "0000000069e244f73d78e8fd29ba2fd2ed618bd6fa2ee92559f542fdb26e7c1d"
        }
        (Network::Bitcoin, 33_333) => {
            "000000002dd5588a74784eaa7ab0507a18ad16a236e7b1ce69f00d7ddfb5d0a6"
        }
        (Network::Bitcoin, 74_000) => {
            "0000000000573993a3c9e41ce34471c079dcf5f52a0e824a81e7f953b8661a20"
        }
        (Network::Bitcoin, 105_000) => {
            "00000000000291ce28027faea320c8d2b054b2e0fe44a773f3eefb151d6bdc97"
        }
        (Network::Bitcoin, 134_444) => {
            "00000000000005b12ffd4cd315cd34ffd4a594f430ac814c91184a0d42d2b0fe"
        }
        (Network::Bitcoin, 168_000) => {
            "000000000000099e61ea72015e79632f216fe6cb33d7899acb35b75c8303b763"
        }
        (Network::Bitcoin, 193_000) => {
            "000000000000059f452a5f7340de6682a977387c17010ff6e6c3bd83ca8b1317"
        }
        (Network::Bitcoin, 210_000) => {
            "000000000000048b95347e83192f69cf0366076336c639f9b7228e9ba171342e"
        }
        (Network::Bitcoin, 216_116) => {
            "00000000000001b4f4b433e81ee46494af945cf96014816a4e2370f11b23df4e"
        }
        (Network::Bitcoin, 225_430) => {
            "00000000000001c108384350f74090433e7fcf79a606b8e797f065b130575932"
        }
        (Network::Bitcoin, 250_000) => {
            "000000000000003887df1f29024b06fc2200b55f8af8f35453d7be294df2d214"
        }
        (Network::Bitcoin, 279_000) => {
            "0000000000000001ae8c72a0b0c301f67e3afca10e819efa9041e458e9bd7e40"
        }
        (Network::Bitcoin, 295_000) => {
            "00000000000000004d9b4ef50f0f9d686fd69db2e03af35a100370c64632a983"
        }
        (Network::Testnet, 546) => {
            "000000002a936ca763904c3c35fce2f3556c559c0214345d31b1bcebf76acb70"
        }
        _ => return None,
    };
    Some(BlockHash::from_str(hash).expect("hard-coded Bitcoin Core checkpoint"))
}

#[cfg(test)]
mod tests {
    use bitcoin::{
        TxMerkleNode,
        block::{Header, Version},
        hashes::Hash,
        pow::Target,
    };

    use super::*;

    #[test]
    fn peer_invalid_classification_exempts_future_and_disconnected_headers() {
        assert!(HeaderError::InvalidTarget.is_peer_invalid());
        assert!(
            HeaderError::UnexpectedDifficulty {
                expected: 1,
                actual: 2,
            }
            .is_peer_invalid()
        );
        assert!(
            !HeaderError::TimeTooNew {
                time: 2,
                maximum: 1,
            }
            .is_peer_invalid()
        );
        assert!(!HeaderError::UnknownParent(BlockHash::all_zeros()).is_peer_invalid());
    }

    fn mine_child(parent: BlockHash, time: u32) -> Header {
        let target = Params::new(Network::Regtest).max_attainable_target;
        let mut header = Header {
            version: Version::from_consensus(4),
            prev_blockhash: parent,
            merkle_root: TxMerkleNode::all_zeros(),
            time,
            bits: target.to_compact_lossy(),
            nonce: 0,
        };
        while header.validate_pow(target).is_err() {
            header.nonce = header
                .nonce
                .checked_add(1)
                .expect("regtest nonce search succeeds");
        }
        header
    }

    #[test]
    fn accepts_regtest_child_and_selects_best_work_tip() {
        let mut dag = HeaderDag::new(Network::Regtest);
        let genesis = dag.active_tip();
        let child = mine_child(genesis.hash, 1);
        let info = dag.insert(child).unwrap();
        assert_eq!(info.height, 1);
        assert_eq!(dag.active_tip().hash, info.hash);
        assert!(dag.active_tip().chainwork > genesis.chainwork);
    }

    #[test]
    fn active_height_index_tracks_extensions_and_stronger_side_chains() {
        let mut dag = HeaderDag::new(Network::Regtest);
        let genesis = dag.active_tip();
        let first = dag
            .insert(mine_child(genesis.hash, genesis.header.time + 1))
            .unwrap();
        let second = dag
            .insert(mine_child(first.hash, genesis.header.time + 2))
            .unwrap();
        assert_eq!(dag.active_header_at(0), Some(genesis));
        assert_eq!(dag.active_header_at(1), Some(first));
        assert_eq!(dag.active_header_at(2), Some(second));
        assert_eq!(dag.active_header_at(3), None);

        let side_one = dag
            .insert(mine_child(genesis.hash, genesis.header.time + 10))
            .unwrap();
        let side_two = dag
            .insert(mine_child(side_one.hash, genesis.header.time + 11))
            .unwrap();
        assert_eq!(dag.active_tip(), second);
        let side_three = dag
            .insert(mine_child(side_two.hash, genesis.header.time + 12))
            .unwrap();
        assert_eq!(dag.active_tip(), side_three);
        assert_eq!(dag.active_header_at(0), Some(genesis));
        assert_eq!(dag.active_header_at(1), Some(side_one));
        assert_eq!(dag.active_header_at(2), Some(side_two));
        assert_eq!(dag.active_header_at(3), Some(side_three));
    }

    #[test]
    fn contextual_insert_enforces_median_time_past_and_future_limit() {
        let mut dag = HeaderDag::new(Network::Regtest);
        let genesis = dag.active_tip();
        let median = dag.median_time_past(genesis.hash).unwrap();
        let too_old = mine_child(genesis.hash, median);
        assert!(matches!(
            dag.insert_contextual(too_old, median),
            Err(HeaderError::TimeTooOld { .. })
        ));
        let too_new = mine_child(
            genesis.hash,
            median.saturating_add(MAX_FUTURE_BLOCK_TIME_SECS + 1),
        );
        assert!(matches!(
            dag.insert_contextual(too_new, median),
            Err(HeaderError::TimeTooNew { .. })
        ));
        let valid = mine_child(genesis.hash, median + 1);
        assert_eq!(dag.insert_contextual(valid, median + 1).unwrap().height, 1);
    }

    #[test]
    fn contextual_insert_enforces_buried_minimum_block_versions() {
        let bitcoin = DeploymentConfig::for_network(Network::Bitcoin);
        assert_eq!(bitcoin.minimum_block_version(227_930), 1);
        assert_eq!(bitcoin.minimum_block_version(227_931), 2);
        assert_eq!(bitcoin.minimum_block_version(363_725), 3);
        assert_eq!(bitcoin.minimum_block_version(388_381), 4);
        let testnet = DeploymentConfig::for_network(Network::Testnet);
        assert_eq!(testnet.minimum_block_version(21_111), 2);
        assert_eq!(testnet.minimum_block_version(330_776), 3);
        assert_eq!(testnet.minimum_block_version(581_885), 4);

        let mut dag = HeaderDag::new(Network::Regtest);
        let genesis = dag.active_tip();
        let target = Target::MAX_ATTAINABLE_REGTEST;
        let mut obsolete = mine_child(genesis.hash, genesis.header.time + 1);
        obsolete.version = Version::from_consensus(3);
        obsolete.nonce = 0;
        while obsolete.validate_pow(target).is_err() {
            obsolete.nonce = obsolete.nonce.checked_add(1).unwrap();
        }
        assert!(matches!(
            dag.insert_contextual(obsolete, genesis.header.time + 1),
            Err(HeaderError::ObsoleteVersion {
                height: 1,
                required: 4,
                actual: 3
            })
        ));

        let mut delayed = DeploymentConfig::for_network(Network::Regtest);
        delayed.apply_test_activation_height("bip34@10").unwrap();
        delayed.apply_test_activation_height("dersig@10").unwrap();
        delayed.apply_test_activation_height("cltv@10").unwrap();
        let mut dag = HeaderDag::with_deployments(delayed);
        let genesis = dag.active_tip();
        let mut version_one = mine_child(genesis.hash, genesis.header.time + 1);
        version_one.version = Version::from_consensus(1);
        version_one.nonce = 0;
        while version_one.validate_pow(target).is_err() {
            version_one.nonce = version_one.nonce.checked_add(1).unwrap();
        }
        assert_eq!(
            dag.insert_contextual(version_one, genesis.header.time + 1)
                .unwrap()
                .height,
            1
        );
    }

    #[test]
    fn expected_bits_honor_regtest_and_testnet_min_difficulty_rules() {
        let regtest = HeaderDag::new(Network::Regtest);
        let regtest_parent = regtest.active_tip();
        let regtest_candidate = mine_child(regtest_parent.hash, regtest_parent.header.time + 1);
        assert_eq!(
            regtest.expected_next_bits(&regtest_candidate).unwrap(),
            regtest_parent.header.bits
        );

        let mut testnet = HeaderDag::new(Network::Testnet);
        let genesis = testnet.active_tip();
        let hard_target = Params::new(Network::Testnet)
            .max_attainable_target
            .min_transition_threshold();
        let mut parent_header = mine_child(genesis.hash, genesis.header.time + 1);
        parent_header.bits = hard_target.to_compact_lossy();
        let parent = HeaderInfo {
            header: parent_header,
            hash: parent_header.block_hash(),
            height: 1,
            chainwork: genesis.chainwork + hard_target.to_work(),
        };
        testnet.headers.insert(parent.hash, parent);
        let normal = Header {
            time: parent.header.time + 1,
            prev_blockhash: parent.hash,
            ..parent.header
        };
        assert_eq!(
            testnet.expected_next_bits(&normal).unwrap(),
            parent.header.bits
        );
        let delayed = Header {
            time: parent.header.time + 1_201,
            ..normal
        };
        assert_eq!(
            testnet.expected_next_bits(&delayed).unwrap(),
            Params::new(Network::Testnet)
                .max_attainable_target
                .to_compact_lossy()
        );
    }

    #[test]
    fn expected_bits_retargets_at_epoch_boundary() {
        let mut dag = HeaderDag::new(Network::Bitcoin);
        let params = Params::new(Network::Bitcoin);
        let genesis = dag.active_tip();
        let hard_target = params.max_attainable_target.min_transition_threshold();
        let boundary_header = Header {
            time: 1_000,
            ..genesis.header
        };
        let boundary = HeaderInfo {
            header: boundary_header,
            hash: boundary_header.block_hash(),
            height: 0,
            chainwork: genesis.chainwork,
        };
        dag.headers.insert(boundary.hash, boundary);
        let parent_header = Header {
            prev_blockhash: boundary.hash,
            time: boundary.header.time
                + u32::try_from(params.pow_target_timespan / 4).expect("mainnet span fits u32"),
            bits: hard_target.to_compact_lossy(),
            ..boundary.header
        };
        let parent = HeaderInfo {
            header: parent_header,
            hash: parent_header.block_hash(),
            height: 2_015,
            chainwork: boundary.chainwork + hard_target.to_work(),
        };
        dag.headers.insert(parent.hash, parent);
        let candidate = Header {
            prev_blockhash: parent.hash,
            time: parent.header.time + 1,
            ..parent.header
        };
        assert_eq!(
            dag.expected_next_bits(&candidate).unwrap(),
            hard_target.min_transition_threshold().to_compact_lossy()
        );
    }

    #[test]
    fn unknown_parent_is_rejected_before_pow_validation() {
        let mut dag = HeaderDag::new(Network::Regtest);
        let header = Header {
            version: Version::ONE,
            prev_blockhash: BlockHash::all_zeros(),
            merkle_root: TxMerkleNode::all_zeros(),
            time: 1,
            bits: Target::MAX_ATTAINABLE_REGTEST.to_compact_lossy(),
            nonce: 0,
        };
        assert!(matches!(
            dag.insert(header),
            Err(HeaderError::UnknownParent(_))
        ));
    }

    #[test]
    fn duplicate_header_is_rejected() {
        let mut dag = HeaderDag::new(Network::Regtest);
        let header = mine_child(dag.active_tip().hash, 1);
        dag.insert(header).unwrap();
        assert!(matches!(dag.insert(header), Err(HeaderError::Duplicate(_))));
    }

    #[test]
    fn locator_ends_at_genesis_and_validation_batches_are_atomic() {
        let mut dag = HeaderDag::new(Network::Regtest);
        let genesis = dag.active_tip();
        let child = mine_child(genesis.hash, genesis.header.time + 1);
        let (candidate, accepted) = dag.validate_batch_contextual(&[child], child.time).unwrap();
        assert_eq!(accepted[0].height, 1);
        assert_eq!(dag.active_tip().height, 0);
        dag = candidate;

        let locator = dag.block_locator();
        assert_eq!(locator[0], child.block_hash());
        assert_eq!(*locator.last().unwrap(), genesis.hash);
        assert!(matches!(
            dag.validate_batch_contextual(&[child, child], child.time),
            Err(HeaderError::Duplicate(_))
        ));
        assert_eq!(dag.active_tip().hash, child.block_hash());
    }

    #[test]
    fn rejects_a_header_that_conflicts_with_a_core_checkpoint() {
        let mut dag = HeaderDag::new(Network::Bitcoin);
        let genesis = dag.active_tip();
        let parent_header = Header {
            time: genesis.header.time + 1,
            ..genesis.header
        };
        let parent = HeaderInfo {
            header: parent_header,
            hash: parent_header.block_hash(),
            height: 11_110,
            chainwork: genesis.chainwork,
        };
        dag.headers.insert(parent.hash, parent);
        let candidate = Header {
            prev_blockhash: parent.hash,
            time: parent.header.time + 1,
            ..parent.header
        };
        assert!(matches!(
            dag.insert(candidate),
            Err(HeaderError::CheckpointMismatch { height: 11_111, .. })
        ));
        assert_eq!(
            checkpoint_hash(Network::Testnet, 546).unwrap().to_string(),
            "000000002a936ca763904c3c35fce2f3556c559c0214345d31b1bcebf76acb70"
        );
    }
}
