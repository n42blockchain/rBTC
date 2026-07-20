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
    pow::Work,
};
use thiserror::Error;

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
}

/// In-memory header DAG with the best-work tip selected independently of arrival order.
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

    /// Finds any known header, including side-chain headers.
    #[must_use]
    pub fn get(&self, hash: &BlockHash) -> Option<HeaderInfo> {
        self.headers.get(hash).copied()
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
}
