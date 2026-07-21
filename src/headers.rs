//! Header DAG and cumulative-work active-chain selection.
//!
//! This module establishes the structural proof-of-work boundary. Difficulty
//! transition rules, median-time-past, checkpoints, and deployment activation
//! are intentionally separate contextual validation gates.

use std::collections::HashMap;

use bitcoin::{
    Network,
    block::{BlockHash, Header},
    consensus::Params,
    pow::{CompactTarget, Work},
};
use thiserror::Error;

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
}

/// In-memory header DAG with the best-work tip selected independently of arrival order.
#[derive(Clone)]
pub struct HeaderDag {
    params: Params,
    headers: HashMap<BlockHash, HeaderInfo>,
    active_tip: BlockHash,
}

impl HeaderDag {
    /// Starts a DAG at the selected network's consensus genesis header.
    #[must_use]
    pub fn new(network: Network) -> Self {
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
            headers,
            active_tip: hash,
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
        let tip = self.active_tip();
        (height <= tip.height)
            .then(|| self.ancestor_at_height(tip, height))
            .flatten()
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
            height: parent
                .height
                .checked_add(1)
                .ok_or(HeaderError::HeightOverflow)?,
            chainwork: parent.chainwork + target.to_work(),
        };
        if info.chainwork > self.active_tip().chainwork {
            self.active_tip = hash;
        }
        self.headers.insert(hash, info);
        Ok(info)
    }

    fn ancestor_at_height(&self, mut current: HeaderInfo, height: u32) -> Option<HeaderInfo> {
        while current.height > height {
            current = self.headers.get(&current.header.prev_blockhash).copied()?;
        }
        (current.height == height).then_some(current)
    }
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

    fn mine_child(parent: BlockHash, time: u32) -> Header {
        let target = Params::new(Network::Regtest).max_attainable_target;
        let mut header = Header {
            version: Version::ONE,
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
}
