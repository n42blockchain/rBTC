//! Initial-block-download trust floors and assume-valid anchor policy.
//!
//! Minimum chainwork is a policy gate, not a consensus rule: lower-work headers
//! may still be validated and stored, but the node must remain in IBD. The
//! assume-valid hash is tracked only as an externally reviewed anchor. rBTC
//! continues verifying every script until a separate background validator can
//! guarantee eventual full validation.

use std::str::FromStr;

use bitcoin::{BlockHash, Network, pow::Work};
use thiserror::Error;

use crate::headers::HeaderDag;

/// Initial-block-download policy pinned to a selected network.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IbdPolicy {
    network: Network,
    minimum_chainwork: Work,
    assume_valid: Option<BlockHash>,
}

/// Current policy evaluation against the active header chain.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IbdStatus {
    /// Active header height.
    pub height: u32,
    /// Active header chainwork.
    pub chainwork: Work,
    /// Whether the active chain meets the configured work floor.
    pub minimum_chainwork_reached: bool,
    /// Height of the assume-valid anchor when it is known and active.
    pub active_assume_valid_height: Option<u32>,
    /// Whether scripts are still fully checked for every connected block.
    pub full_script_validation: bool,
}

/// Invalid IBD policy or an unsatisfied trust floor.
#[derive(Debug, Error, Eq, PartialEq)]
pub enum IbdPolicyError {
    /// The policy and header DAG select different networks.
    #[error("IBD policy network does not match header network")]
    NetworkMismatch,
    /// A minimum-chainwork override was not a 256-bit hexadecimal integer.
    #[error("invalid minimum chainwork: {0}")]
    MinimumChainwork(String),
    /// An assume-valid override was not a block hash or zero.
    #[error("invalid assume-valid block: {0}")]
    AssumeValid(String),
    /// The peer's best valid header chain remains below the required work.
    #[error("header chainwork {actual} is below required minimum {required}")]
    BelowMinimumChainwork {
        /// Active header chainwork.
        actual: Work,
        /// Configured floor.
        required: Work,
    },
}

impl IbdPolicy {
    /// Returns Bitcoin Core 26's pinned defaults where available.
    ///
    /// Regtest and testnet4 default to a zero work floor and no assume-valid
    /// anchor. Core 26 predates testnet4, so no trust constants are invented.
    #[must_use]
    pub fn for_network(network: Network) -> Self {
        let (minimum_chainwork, assume_valid) = match network {
            Network::Bitcoin => (
                "000000000000000000000000000000000000000052b2559353df4117b7348b64",
                Some("00000000000000000001a0a448d6cf2546b06801389cc030b2b18c6491266815"),
            ),
            Network::Testnet => (
                "000000000000000000000000000000000000000000000b6a51f415a67c0da307",
                Some("0000000000000093bcb68c03a9a168ae252572d348a2eaeba2cdf9231d73206f"),
            ),
            Network::Signet => (
                "000000000000000000000000000000000000000000000000000001ad46be4862",
                Some("0000013d778ba3f914530f11f6b69869c9fab54acff85acd7b8201d111f19b7f"),
            ),
            Network::Testnet4 | Network::Regtest => (
                "0000000000000000000000000000000000000000000000000000000000000000",
                None,
            ),
        };
        Self {
            network,
            minimum_chainwork: Work::from_unprefixed_hex(minimum_chainwork)
                .expect("pinned Core minimum chainwork"),
            assume_valid: assume_valid.map(|hash| {
                BlockHash::from_str(hash).expect("pinned Core assume-valid block hash")
            }),
        }
    }

    /// Applies a hexadecimal minimum-chainwork override.
    pub fn set_minimum_chainwork(&mut self, value: &str) -> Result<(), IbdPolicyError> {
        self.minimum_chainwork =
            parse_work(value).map_err(|()| IbdPolicyError::MinimumChainwork(value.to_owned()))?;
        Ok(())
    }

    /// Applies an assume-valid block override; `0` disables the anchor.
    pub fn set_assume_valid(&mut self, value: &str) -> Result<(), IbdPolicyError> {
        self.assume_valid = if value == "0" {
            None
        } else {
            Some(
                BlockHash::from_str(value)
                    .map_err(|_| IbdPolicyError::AssumeValid(value.to_owned()))?,
            )
        };
        Ok(())
    }

    /// Evaluates the active header chain without changing consensus validity.
    pub fn status(&self, headers: &HeaderDag) -> Result<IbdStatus, IbdPolicyError> {
        if headers.network() != self.network {
            return Err(IbdPolicyError::NetworkMismatch);
        }
        let tip = headers.active_tip();
        let active_assume_valid_height = self.assume_valid.and_then(|hash| {
            let anchor = headers.get(&hash)?;
            headers
                .active_header_at(anchor.height)
                .is_some_and(|active| active.hash == hash)
                .then_some(anchor.height)
        });
        Ok(IbdStatus {
            height: tip.height,
            chainwork: tip.chainwork,
            minimum_chainwork_reached: tip.chainwork >= self.minimum_chainwork,
            active_assume_valid_height,
            full_script_validation: true,
        })
    }

    /// Requires the active chain to meet the work floor before leaving IBD.
    pub fn ensure_minimum_chainwork(
        &self,
        headers: &HeaderDag,
    ) -> Result<IbdStatus, IbdPolicyError> {
        let status = self.status(headers)?;
        if !status.minimum_chainwork_reached {
            return Err(IbdPolicyError::BelowMinimumChainwork {
                actual: status.chainwork,
                required: self.minimum_chainwork,
            });
        }
        Ok(status)
    }
}

fn parse_work(value: &str) -> Result<Work, ()> {
    Work::from_hex(value)
        .or_else(|_| Work::from_unprefixed_hex(value))
        .map_err(|_| ())
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

    fn mine_child(headers: &mut HeaderDag) -> BlockHash {
        let parent = headers.active_tip();
        let mut header = Header {
            version: Version::from_consensus(4),
            prev_blockhash: parent.hash,
            merkle_root: TxMerkleNode::all_zeros(),
            time: parent.header.time + 1,
            bits: Target::MAX_ATTAINABLE_REGTEST.to_compact_lossy(),
            nonce: 0,
        };
        while header.validate_pow(Target::MAX_ATTAINABLE_REGTEST).is_err() {
            header.nonce += 1;
        }
        headers.insert_contextual(header, u32::MAX).unwrap().hash
    }

    #[test]
    fn minimum_chainwork_blocks_ibd_completion_without_rejecting_headers() {
        let mut headers = HeaderDag::new(Network::Regtest);
        let initial = headers.active_tip().chainwork;
        let child = mine_child(&mut headers);
        assert_eq!(headers.active_tip().hash, child);

        let mut policy = IbdPolicy::for_network(Network::Regtest);
        let required = (headers.active_tip().chainwork + initial).to_string();
        policy.set_minimum_chainwork(&required).unwrap();
        let status = policy.status(&headers).unwrap();
        assert!(!status.minimum_chainwork_reached);
        assert!(status.full_script_validation);
        assert!(matches!(
            policy.ensure_minimum_chainwork(&headers),
            Err(IbdPolicyError::BelowMinimumChainwork { .. })
        ));

        mine_child(&mut headers);
        assert!(
            policy
                .ensure_minimum_chainwork(&headers)
                .unwrap()
                .minimum_chainwork_reached
        );
    }

    #[test]
    fn assume_valid_is_only_reported_for_an_active_anchor_and_never_skips_scripts() {
        let mut headers = HeaderDag::new(Network::Regtest);
        let active = mine_child(&mut headers);
        let genesis = headers.active_header_at(0).unwrap();
        let mut side_header = Header {
            version: Version::from_consensus(4),
            prev_blockhash: genesis.hash,
            merkle_root: TxMerkleNode::all_zeros(),
            time: genesis.header.time + 2,
            bits: Target::MAX_ATTAINABLE_REGTEST.to_compact_lossy(),
            nonce: 0,
        };
        while side_header
            .validate_pow(Target::MAX_ATTAINABLE_REGTEST)
            .is_err()
        {
            side_header.nonce += 1;
        }
        let side = headers
            .insert_contextual(side_header, u32::MAX)
            .unwrap()
            .hash;
        assert_eq!(headers.active_tip().hash, active);
        let mut policy = IbdPolicy::for_network(Network::Regtest);
        policy.set_assume_valid(&side.to_string()).unwrap();
        assert_eq!(
            policy.status(&headers).unwrap().active_assume_valid_height,
            None
        );
        policy.set_assume_valid(&active.to_string()).unwrap();
        let status = policy.status(&headers).unwrap();
        assert_eq!(status.active_assume_valid_height, Some(1));
        assert!(status.full_script_validation);

        policy
            .set_assume_valid(&BlockHash::all_zeros().to_string())
            .unwrap();
        assert_eq!(
            policy.status(&headers).unwrap().active_assume_valid_height,
            None
        );
        policy.set_assume_valid("0").unwrap();
        assert_eq!(
            policy.status(&headers).unwrap().active_assume_valid_height,
            None
        );
    }

    #[test]
    fn pinned_defaults_and_overrides_are_strict() {
        let mainnet = IbdPolicy::for_network(Network::Bitcoin);
        assert!(
            !mainnet
                .minimum_chainwork
                .to_be_bytes()
                .iter()
                .all(|byte| *byte == 0)
        );
        assert!(mainnet.assume_valid.is_some());
        assert_eq!(
            mainnet.status(&HeaderDag::new(Network::Regtest)),
            Err(IbdPolicyError::NetworkMismatch)
        );

        let mut regtest = IbdPolicy::for_network(Network::Regtest);
        assert_eq!(
            regtest.set_minimum_chainwork("not-hex"),
            Err(IbdPolicyError::MinimumChainwork("not-hex".to_owned()))
        );
        assert_eq!(
            regtest.set_assume_valid("not-a-hash"),
            Err(IbdPolicyError::AssumeValid("not-a-hash".to_owned()))
        );
    }
}
