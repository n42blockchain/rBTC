//! Network-specific buried deployments and consensus script flags.
//!
//! Values mirror the Bitcoin Core 26 sources bundled by the pinned
//! `bitcoinconsensus` dependency, including regtest's `-vbparams` override
//! semantics for Taproot.

use std::{str::FromStr, sync::Arc};

use bitcoin::{BlockHash, Network, consensus::serialize, hashes::Hash, p2p::Magic};
use thiserror::Error;

use crate::{
    block_execution::BlockDeploymentContext, blockchain::block_subsidy_with_interval,
    headers::HeaderDag, signet::DEFAULT_SIGNET_CHALLENGE,
};

const VERSION_BITS_TOP_MASK: u32 = 0xE000_0000;
const VERSION_BITS_TOP_BITS: u32 = 0x2000_0000;
const TAPROOT_BIT: u32 = 2;
const TAPROOT_START_TIME: u32 = 1_619_222_400;
const TAPROOT_TIMEOUT: u32 = 1_628_640_000;
const ALWAYS_ACTIVE: i64 = -1;
const NEVER_ACTIVE: i64 = -2;
const CONFIG_ENCODING_VERSION: u8 = 1;
const BURIED_CONFIG_ENCODING_VERSION: u8 = 2;
const CUSTOM_SIGNET_CONFIG_ENCODING_VERSION: u8 = 3;
const BITCOIN_HALVING_INTERVAL: u32 = 210_000;
const REGTEST_HALVING_INTERVAL: u32 = 150;
const BIP34_IMPLIES_BIP30_LIMIT: u32 = 1_983_702;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ActivationHeights {
    bip34: u32,
    bip65: u32,
    bip66: u32,
    csv: u32,
    segwit: u32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ThresholdState {
    Defined,
    Started,
    LockedIn,
    Active,
    Failed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct VersionBitsParams {
    period: u32,
    threshold: u32,
    start_time: i64,
    timeout: i64,
    min_activation_height: i32,
}

/// Complete deployment parameters used while validating one network.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeploymentConfig {
    network: Network,
    taproot: VersionBitsParams,
    activation_heights: ActivationHeights,
    signet_challenge: Option<Arc<[u8]>>,
    custom_signet: bool,
}

/// Invalid deployment configuration.
#[derive(Debug, Error, Eq, PartialEq)]
pub enum DeploymentConfigError {
    /// Core-compatible version-bits overrides are a regtest-only facility.
    #[error("--vbparams is only supported with --network regtest")]
    RegtestOnly,
    /// Core-compatible buried-deployment overrides are a regtest-only facility.
    #[error("--testactivationheight is only supported with --network regtest")]
    TestActivationRegtestOnly,
    /// The value did not have Core's deployment:start:end[:min_height] shape.
    #[error("malformed --vbparams; expected deployment:start:end[:min_activation_height]")]
    Malformed,
    /// rBTC does not implement the named version-bits deployment.
    #[error("unsupported version-bits deployment: {0}")]
    UnknownDeployment(String),
    /// Start time was not a signed 64-bit integer.
    #[error("invalid version-bits start time: {0}")]
    StartTime(String),
    /// Timeout was not a signed 64-bit integer.
    #[error("invalid version-bits timeout: {0}")]
    Timeout(String),
    /// Minimum activation height was not a signed 32-bit integer.
    #[error("invalid version-bits minimum activation height: {0}")]
    MinimumActivationHeight(String),
    /// A buried-deployment override did not have Core's name@height shape.
    #[error("malformed --testactivationheight; expected name@height")]
    TestActivationMalformed,
    /// The buried deployment name is not supported by Bitcoin Core 26.
    #[error("unsupported buried deployment: {0}")]
    UnknownBuriedDeployment(String),
    /// The buried activation height was outside Core's accepted range.
    #[error("invalid buried activation height: {0}")]
    BuriedActivationHeight(String),
    /// The deployment configuration and header DAG select different networks.
    #[error("deployment configuration network does not match header network")]
    NetworkMismatch,
    /// Custom Signet parameters only apply to the Signet network.
    #[error("--signetchallenge is only supported with --network signet")]
    SignetOnly,
}

impl DeploymentConfig {
    /// Returns the pinned default deployment parameters for `network`.
    #[must_use]
    pub fn for_network(network: Network) -> Self {
        let taproot = match network {
            Network::Bitcoin => VersionBitsParams {
                period: 2_016,
                threshold: 1_815,
                start_time: i64::from(TAPROOT_START_TIME),
                timeout: i64::from(TAPROOT_TIMEOUT),
                min_activation_height: 709_632,
            },
            Network::Testnet => VersionBitsParams {
                period: 2_016,
                threshold: 1_512,
                start_time: i64::from(TAPROOT_START_TIME),
                timeout: i64::from(TAPROOT_TIMEOUT),
                min_activation_height: 0,
            },
            Network::Regtest => VersionBitsParams {
                period: 144,
                threshold: 108,
                start_time: ALWAYS_ACTIVE,
                timeout: i64::MAX,
                min_activation_height: 0,
            },
            Network::Testnet4 | Network::Signet => VersionBitsParams {
                period: 2_016,
                threshold: 1_815,
                start_time: ALWAYS_ACTIVE,
                timeout: i64::MAX,
                min_activation_height: 0,
            },
        };
        Self {
            network,
            taproot,
            activation_heights: activation_heights(network),
            signet_challenge: (network == Network::Signet)
                .then(|| Arc::<[u8]>::from(DEFAULT_SIGNET_CHALLENGE)),
            custom_signet: false,
        }
    }

    /// Selects the one Core-compatible custom Signet challenge script.
    pub fn set_signet_challenge(
        &mut self,
        challenge: Vec<u8>,
    ) -> Result<(), DeploymentConfigError> {
        if self.network != Network::Signet {
            return Err(DeploymentConfigError::SignetOnly);
        }
        self.signet_challenge = Some(Arc::from(challenge));
        self.custom_signet = true;
        Ok(())
    }

    /// Whether this selects a custom Signet rather than Core's default Signet.
    #[must_use]
    pub fn is_custom_signet(&self) -> bool {
        self.custom_signet
    }

    /// P2P message start derived from the serialized challenge on Signet.
    #[must_use]
    pub fn message_start(&self) -> Magic {
        let Some(challenge) = self.signet_challenge.as_deref() else {
            return self.network.magic();
        };
        let hash = bitcoin::hashes::sha256d::Hash::hash(&serialize(&challenge.to_vec()));
        Magic::from_bytes(
            hash.to_byte_array()[..4]
                .try_into()
                .expect("four-byte hash prefix"),
        )
    }

    /// Applies one Bitcoin Core-compatible regtest `-vbparams` value.
    ///
    /// The currently implemented deployment name is `taproot`. Repeated calls
    /// use the last supplied value, matching Core's option processing.
    pub fn apply_vbparams(&mut self, value: &str) -> Result<(), DeploymentConfigError> {
        if self.network != Network::Regtest {
            return Err(DeploymentConfigError::RegtestOnly);
        }
        let fields = value.split(':').collect::<Vec<_>>();
        if !(3..=4).contains(&fields.len()) {
            return Err(DeploymentConfigError::Malformed);
        }
        if fields[0] != "taproot" {
            return Err(DeploymentConfigError::UnknownDeployment(
                fields[0].to_owned(),
            ));
        }
        let start_time = fields[1]
            .parse::<i64>()
            .map_err(|_| DeploymentConfigError::StartTime(fields[1].to_owned()))?;
        let timeout = fields[2]
            .parse::<i64>()
            .map_err(|_| DeploymentConfigError::Timeout(fields[2].to_owned()))?;
        let min_activation_height = fields.get(3).map_or(Ok(0), |height| {
            height
                .parse::<i32>()
                .map_err(|_| DeploymentConfigError::MinimumActivationHeight((*height).to_owned()))
        })?;
        self.taproot = VersionBitsParams {
            period: 144,
            threshold: 108,
            start_time,
            timeout,
            min_activation_height,
        };
        Ok(())
    }

    /// Applies one Bitcoin Core-compatible regtest `name@height` buried override.
    pub fn apply_test_activation_height(
        &mut self,
        value: &str,
    ) -> Result<(), DeploymentConfigError> {
        if self.network != Network::Regtest {
            return Err(DeploymentConfigError::TestActivationRegtestOnly);
        }
        let (name, height) = value
            .split_once('@')
            .ok_or(DeploymentConfigError::TestActivationMalformed)?;
        let height = height
            .parse::<u32>()
            .ok()
            .filter(|height| *height < i32::MAX as u32)
            .ok_or_else(|| DeploymentConfigError::BuriedActivationHeight(value.to_owned()))?;
        match name {
            "bip34" => self.activation_heights.bip34 = height,
            "dersig" => self.activation_heights.bip66 = height,
            "cltv" => self.activation_heights.bip65 = height,
            "csv" => self.activation_heights.csv = height,
            "segwit" => self.activation_heights.segwit = height,
            _ => {
                return Err(DeploymentConfigError::UnknownBuriedDeployment(
                    name.to_owned(),
                ));
            }
        }
        Ok(())
    }

    /// Canonical bytes that bind persisted execution state to these settings.
    #[must_use]
    pub fn consensus_id(&self) -> Vec<u8> {
        let mut encoded = vec![0_u8; 29];
        encoded[0] = CONFIG_ENCODING_VERSION;
        encoded[1..5].copy_from_slice(&self.taproot.period.to_le_bytes());
        encoded[5..9].copy_from_slice(&self.taproot.threshold.to_le_bytes());
        encoded[9..17].copy_from_slice(&self.taproot.start_time.to_le_bytes());
        encoded[17..25].copy_from_slice(&self.taproot.timeout.to_le_bytes());
        encoded[25..29].copy_from_slice(&self.taproot.min_activation_height.to_le_bytes());
        if self.activation_heights != activation_heights(self.network) {
            encoded[0] = BURIED_CONFIG_ENCODING_VERSION;
            for height in [
                self.activation_heights.bip34,
                self.activation_heights.bip65,
                self.activation_heights.bip66,
                self.activation_heights.csv,
                self.activation_heights.segwit,
            ] {
                encoded.extend_from_slice(&height.to_le_bytes());
            }
        }
        if self.is_custom_signet() {
            encoded[0] = CUSTOM_SIGNET_CONFIG_ENCODING_VERSION;
            let challenge = self
                .signet_challenge
                .as_deref()
                .expect("custom Signet has a challenge");
            encoded.extend_from_slice(
                &u32::try_from(challenge.len())
                    .expect("Bitcoin script length fits u32")
                    .to_le_bytes(),
            );
            encoded.extend_from_slice(challenge);
        }
        encoded
    }

    /// Returns the network whose consensus parameters this value describes.
    #[must_use]
    pub const fn network(&self) -> Network {
        self.network
    }

    pub(crate) const fn minimum_block_version(&self, height: u32) -> i32 {
        minimum_block_version_for_heights(self.activation_heights, height)
    }
}

/// Derives block and script consensus flags for one candidate.
#[must_use]
pub fn block_deployment_context(
    network: Network,
    height: u32,
    block_hash: BlockHash,
    block_time: u32,
    taproot_active: bool,
) -> BlockDeploymentContext {
    block_deployment_context_with_config(
        &DeploymentConfig::for_network(network),
        height,
        block_hash,
        block_time,
        taproot_active,
    )
}

/// Derives candidate consensus flags using an explicitly selected configuration.
#[must_use]
pub fn block_deployment_context_with_config(
    config: &DeploymentConfig,
    height: u32,
    block_hash: BlockHash,
    block_time: u32,
    taproot_active: bool,
) -> BlockDeploymentContext {
    block_deployment_context_with_bip34_anchor(
        config,
        height,
        block_hash,
        block_time,
        taproot_active,
        false,
    )
}

/// Derives candidate flags using the active header chain for Core's BIP30 optimization.
pub fn block_deployment_context_for_headers(
    config: &DeploymentConfig,
    headers: &HeaderDag,
    height: u32,
    block_hash: BlockHash,
    block_time: u32,
    taproot_active: bool,
) -> Result<BlockDeploymentContext, DeploymentConfigError> {
    if headers.network() != config.network {
        return Err(DeploymentConfigError::NetworkMismatch);
    }
    let bip34_anchor_matches = bip34_anchor(config.network).is_some_and(|(anchor_height, hash)| {
        headers
            .active_header_at(anchor_height)
            .is_some_and(|header| header.hash == hash)
    });
    Ok(block_deployment_context_with_bip34_anchor(
        config,
        height,
        block_hash,
        block_time,
        taproot_active,
        bip34_anchor_matches,
    ))
}

fn block_deployment_context_with_bip34_anchor(
    config: &DeploymentConfig,
    height: u32,
    block_hash: BlockHash,
    _block_time: u32,
    _taproot_active: bool,
    bip34_anchor_matches: bool,
) -> BlockDeploymentContext {
    let network = config.network;
    let bip30_enforced = bip30_enforced(network, height, block_hash, bip34_anchor_matches);
    let subsidy_sats = block_subsidy_with_interval(height, halving_interval(network));
    let heights = config.activation_heights;
    // Core 26 enables these mutually dependent interpreter flags for ordinary
    // blocks and handles their historical activation through the exceptions
    // above plus separate consensus gates such as the witness commitment check.
    let mut script_flags = script_flag_exception(network, block_hash).unwrap_or(
        bitcoinconsensus::VERIFY_P2SH
            | bitcoinconsensus::VERIFY_WITNESS
            | bitcoinconsensus::VERIFY_TAPROOT,
    );
    if height >= heights.bip66 {
        script_flags |= bitcoinconsensus::VERIFY_DERSIG;
    }
    if height >= heights.bip65 {
        script_flags |= bitcoinconsensus::VERIFY_CHECKLOCKTIMEVERIFY;
    }
    if height >= heights.csv {
        script_flags |= bitcoinconsensus::VERIFY_CHECKSEQUENCEVERIFY;
    }
    if height >= heights.segwit {
        script_flags |= bitcoinconsensus::VERIFY_NULLDUMMY;
    }
    BlockDeploymentContext {
        script_flags,
        bip34_active: height >= heights.bip34,
        csv_active: height >= heights.csv,
        segwit_active: height >= heights.segwit,
        signet_challenge: config.signet_challenge.clone(),
        bip30_enforced,
        subsidy_sats,
    }
}

const fn halving_interval(network: Network) -> u32 {
    match network {
        Network::Regtest => REGTEST_HALVING_INTERVAL,
        Network::Bitcoin | Network::Testnet | Network::Testnet4 | Network::Signet => {
            BITCOIN_HALVING_INTERVAL
        }
    }
}

/// Whether Taproot is unconditionally active for this network configuration.
#[must_use]
pub fn taproot_always_active(network: Network) -> bool {
    DeploymentConfig::for_network(network).taproot.start_time == ALWAYS_ACTIVE
}

/// Computes Taproot's BIP9 state for a candidate on the active header chain.
///
/// Mainnet and legacy testnet use the Core 26 deployment parameters. Newer
/// test networks and default regtest configure Taproot as always active.
pub fn taproot_active(
    headers: &HeaderDag,
    candidate_height: u32,
    config: &DeploymentConfig,
) -> Result<bool, DeploymentConfigError> {
    if headers.network() != config.network {
        return Err(DeploymentConfigError::NetworkMismatch);
    }
    if config.taproot.start_time == ALWAYS_ACTIVE {
        return Ok(true);
    }
    if config.taproot.start_time == NEVER_ACTIVE {
        return Ok(false);
    }
    Ok(threshold_state(headers, candidate_height, config.taproot) == ThresholdState::Active)
}

fn threshold_state(
    headers: &HeaderDag,
    candidate_height: u32,
    params: VersionBitsParams,
) -> ThresholdState {
    let Some(parent_height) = candidate_height.checked_sub(1) else {
        return ThresholdState::Defined;
    };
    let completed_periods = candidate_height / params.period;
    if completed_periods == 0 {
        return ThresholdState::Defined;
    }
    let mut state = ThresholdState::Defined;
    for period_index in 1..=completed_periods {
        let period_end = period_index
            .checked_mul(params.period)
            .and_then(|height| height.checked_sub(1))
            .expect("active header height is bounded by u32");
        if period_end > parent_height {
            break;
        }
        let Some(period_end_header) = headers.active_header_at(period_end) else {
            return ThresholdState::Defined;
        };
        let period_mtp = i64::from(
            headers
                .median_time_past(period_end_header.hash)
                .expect("active header has median time past"),
        );
        state = match state {
            ThresholdState::Defined if period_mtp >= params.start_time => ThresholdState::Started,
            ThresholdState::Started => {
                let period_start = period_end + 1 - params.period;
                let signals = (period_start..=period_end)
                    .filter_map(|height| headers.active_header_at(height))
                    .filter(|header| signals_taproot(header.header.version.to_consensus()))
                    .count();
                if signals >= usize::try_from(params.threshold).expect("threshold fits usize") {
                    ThresholdState::LockedIn
                } else if period_mtp >= params.timeout {
                    ThresholdState::Failed
                } else {
                    ThresholdState::Started
                }
            }
            ThresholdState::LockedIn
                if i64::from(period_end.saturating_add(1))
                    >= i64::from(params.min_activation_height) =>
            {
                ThresholdState::Active
            }
            other => other,
        };
    }
    state
}

fn signals_taproot(version: i32) -> bool {
    let version = u32::from_ne_bytes(version.to_ne_bytes());
    version & VERSION_BITS_TOP_MASK == VERSION_BITS_TOP_BITS && version & (1 << TAPROOT_BIT) != 0
}

const fn activation_heights(network: Network) -> ActivationHeights {
    match network {
        Network::Bitcoin => ActivationHeights {
            bip34: 227_931,
            bip65: 388_381,
            bip66: 363_725,
            csv: 419_328,
            segwit: 481_824,
        },
        Network::Testnet => ActivationHeights {
            bip34: 21_111,
            bip65: 581_885,
            bip66: 330_776,
            csv: 770_112,
            segwit: 834_624,
        },
        Network::Testnet4 | Network::Signet => ActivationHeights {
            bip34: 1,
            bip65: 1,
            bip66: 1,
            csv: 1,
            segwit: 1,
        },
        Network::Regtest => ActivationHeights {
            bip34: 1,
            bip65: 1,
            bip66: 1,
            csv: 1,
            segwit: 0,
        },
    }
}

const fn minimum_block_version_for_heights(heights: ActivationHeights, height: u32) -> i32 {
    if height >= heights.bip65 {
        4
    } else if height >= heights.bip66 {
        3
    } else if height >= heights.bip34 {
        2
    } else {
        1
    }
}

fn script_flag_exception(network: Network, hash: BlockHash) -> Option<u32> {
    let exception = match network {
        Network::Bitcoin
            if hash
                == parse_hash(
                    "00000000000002dc756eebf4f49723ed8d30cc28a5f108eb94b1ba88ac4f9c22",
                ) =>
        {
            bitcoinconsensus::VERIFY_NONE
        }
        Network::Bitcoin
            if hash
                == parse_hash(
                    "0000000000000000000f14c35b2d841e986ab5441de8c585d5ffe55ea1e395ad",
                ) =>
        {
            bitcoinconsensus::VERIFY_P2SH | bitcoinconsensus::VERIFY_WITNESS
        }
        Network::Testnet
            if hash
                == parse_hash(
                    "00000000dd30457c001f4095d208cc1296b0eed002427aa599874af7a432b105",
                ) =>
        {
            bitcoinconsensus::VERIFY_NONE
        }
        _ => return None,
    };
    Some(exception)
}

fn is_bip30_exception(network: Network, height: u32, hash: BlockHash) -> bool {
    network == Network::Bitcoin
        && ((height == 91_842
            && hash
                == parse_hash("00000000000a4d0a398161ffc163c503763b1f4360639393e0e4c8e300e0caec"))
            || (height == 91_880
                && hash
                    == parse_hash(
                        "00000000000743f190a18c5577a3c2d2a1f610ae9601ac046a38084ccb7cd721",
                    )))
}

fn bip30_enforced(
    network: Network,
    height: u32,
    hash: BlockHash,
    bip34_anchor_matches: bool,
) -> bool {
    if is_bip30_exception(network, height, hash) {
        return false;
    }
    if height >= BIP34_IMPLIES_BIP30_LIMIT {
        return true;
    }
    let above_bip34 =
        bip34_anchor(network).is_some_and(|(anchor_height, _)| height > anchor_height);
    !(above_bip34 && bip34_anchor_matches)
}

fn bip34_anchor(network: Network) -> Option<(u32, BlockHash)> {
    let (height, hash) = match network {
        Network::Bitcoin => (
            227_931,
            "000000000000024b89b42a942fe0d9fea3bb44ab7bd1b19115dd6a759c0808b8",
        ),
        Network::Testnet => (
            21_111,
            "0000000023b3a96d3484e5abb3755c413e7d41500f8e2a5c3f0dd01299cd8ef8",
        ),
        Network::Testnet4 | Network::Signet | Network::Regtest => return None,
    };
    Some((height, parse_hash(hash)))
}

fn parse_hash(hash: &str) -> BlockHash {
    BlockHash::from_str(hash).expect("hard-coded Bitcoin Core block hash")
}

#[cfg(test)]
mod tests {
    use bitcoin::{
        TxMerkleNode,
        block::{Header, Version},
        hashes::Hash,
        hex::FromHex,
        pow::Target,
    };
    use proptest::prelude::*;

    use super::*;

    #[test]
    fn regtest_activates_default_core_rules_at_block_one() {
        let context = block_deployment_context(
            Network::Regtest,
            1,
            BlockHash::all_zeros(),
            1_333_238_399,
            taproot_always_active(Network::Regtest),
        );
        assert!(context.bip34_active);
        assert!(context.csv_active);
        assert!(context.segwit_active);
        assert!(context.signet_challenge.is_none());
        assert_ne!(context.script_flags & bitcoinconsensus::VERIFY_P2SH, 0);
        assert_ne!(
            context.script_flags & bitcoinconsensus::VERIFY_CHECKSEQUENCEVERIFY,
            0
        );
        assert_ne!(context.script_flags & bitcoinconsensus::VERIFY_WITNESS, 0);
        assert_ne!(context.script_flags & bitcoinconsensus::VERIFY_TAPROOT, 0);
    }

    #[test]
    fn only_signet_selects_bip325_block_validation() {
        let hash = BlockHash::all_zeros();
        assert!(
            block_deployment_context(Network::Signet, 1, hash, 0, false)
                .signet_challenge
                .is_some()
        );
        for network in [
            Network::Bitcoin,
            Network::Testnet,
            Network::Testnet4,
            Network::Regtest,
        ] {
            assert!(
                block_deployment_context(network, 1, hash, 0, false)
                    .signet_challenge
                    .is_none()
            );
        }
    }

    #[test]
    fn custom_signet_derives_bip325_message_start_and_execution_identity() {
        let mut config = DeploymentConfig::for_network(Network::Signet);
        assert_eq!(config.message_start(), Network::Signet.magic());
        let default_id = config.consensus_id();
        let mut explicit_default = config.clone();
        explicit_default
            .set_signet_challenge(DEFAULT_SIGNET_CHALLENGE.to_vec())
            .unwrap();
        assert!(explicit_default.is_custom_signet());
        assert_eq!(explicit_default.message_start(), Network::Signet.magic());
        assert_ne!(explicit_default.consensus_id(), default_id);
        let challenge = Vec::from_hex(
            "512103ad5e0edad18cb1f0fc0d28a3d4f1f3e445640337489abb10404f2d1e086be43051ae",
        )
        .unwrap();
        config.set_signet_challenge(challenge.clone()).unwrap();
        assert!(config.is_custom_signet());
        assert_eq!(config.message_start().to_bytes(), [0x7e, 0xc6, 0x53, 0xa5]);
        assert_ne!(config.consensus_id(), default_id);
        assert_eq!(
            config.consensus_id()[0],
            CUSTOM_SIGNET_CONFIG_ENCODING_VERSION
        );
        let context =
            block_deployment_context_with_config(&config, 1, BlockHash::all_zeros(), 0, true);
        assert_eq!(
            context.signet_challenge.as_deref(),
            Some(challenge.as_slice())
        );
    }

    #[test]
    fn custom_signet_parameters_are_network_scoped_and_allow_core_empty_script() {
        assert_eq!(
            DeploymentConfig::for_network(Network::Bitcoin).set_signet_challenge(vec![0x51]),
            Err(DeploymentConfigError::SignetOnly)
        );
        let mut config = DeploymentConfig::for_network(Network::Signet);
        config.set_signet_challenge(Vec::new()).unwrap();
        assert!(config.is_custom_signet());
    }

    #[test]
    fn network_subsidy_halving_intervals_match_core() {
        let hash = BlockHash::all_zeros();
        let regtest_before = block_deployment_context(Network::Regtest, 149, hash, 0, true);
        let regtest_after = block_deployment_context(Network::Regtest, 150, hash, 0, true);
        let mainnet_early = block_deployment_context(Network::Bitcoin, 150, hash, 0, false);
        let mainnet_after = block_deployment_context(Network::Bitcoin, 210_000, hash, 0, false);
        assert_eq!(regtest_before.subsidy_sats, 50 * 100_000_000);
        assert_eq!(regtest_after.subsidy_sats, 25 * 100_000_000);
        assert_eq!(mainnet_early.subsidy_sats, 50 * 100_000_000);
        assert_eq!(mainnet_after.subsidy_sats, 25 * 100_000_000);
    }

    #[test]
    fn mainnet_csv_and_segwit_boundaries_match_pinned_core() {
        let before = block_deployment_context(
            Network::Bitcoin,
            419_327,
            BlockHash::all_zeros(),
            u32::MAX,
            false,
        );
        assert!(!before.csv_active);
        let csv = block_deployment_context(
            Network::Bitcoin,
            419_328,
            BlockHash::all_zeros(),
            u32::MAX,
            false,
        );
        assert!(csv.csv_active);
        assert!(!csv.segwit_active);
        assert_ne!(csv.script_flags & bitcoinconsensus::VERIFY_P2SH, 0);
        assert_ne!(csv.script_flags & bitcoinconsensus::VERIFY_WITNESS, 0);
        assert_ne!(csv.script_flags & bitcoinconsensus::VERIFY_TAPROOT, 0);
        assert_eq!(csv.script_flags & bitcoinconsensus::VERIFY_NULLDUMMY, 0);
        let segwit = block_deployment_context(
            Network::Bitcoin,
            481_824,
            BlockHash::all_zeros(),
            u32::MAX,
            false,
        );
        assert!(segwit.segwit_active);
        assert_ne!(segwit.script_flags & bitcoinconsensus::VERIFY_WITNESS, 0);
        assert_ne!(segwit.script_flags & bitcoinconsensus::VERIFY_NULLDUMMY, 0);
    }

    #[test]
    fn bip30_matches_core_anchor_optimization_exceptions_and_future_limit() {
        let ordinary = BlockHash::all_zeros();
        assert!(bip30_enforced(Network::Bitcoin, 227_931, ordinary, true));
        assert!(!bip30_enforced(Network::Bitcoin, 227_932, ordinary, true));
        assert!(bip30_enforced(Network::Bitcoin, 227_932, ordinary, false));
        assert!(!bip30_enforced(Network::Testnet, 21_112, ordinary, true));
        assert!(bip30_enforced(
            Network::Bitcoin,
            BIP34_IMPLIES_BIP30_LIMIT,
            ordinary,
            true
        ));
        assert!(bip30_enforced(Network::Regtest, 2, ordinary, true));
        assert!(!bip30_enforced(
            Network::Bitcoin,
            91_842,
            parse_hash("00000000000a4d0a398161ffc163c503763b1f4360639393e0e4c8e300e0caec"),
            false
        ));
        assert!(!bip30_enforced(
            Network::Bitcoin,
            91_880,
            parse_hash("00000000000743f190a18c5577a3c2d2a1f610ae9601ac046a38084ccb7cd721"),
            false
        ));
    }

    #[test]
    fn bip9_taproot_state_obeys_period_threshold_and_minimum_height() {
        let mut headers = HeaderDag::new(Network::Regtest);
        for height in 1..12_u32 {
            let parent = headers.active_tip();
            let signals = (4..=6).contains(&height);
            let mut header = Header {
                version: if signals {
                    Version::from_consensus(
                        i32::try_from(VERSION_BITS_TOP_BITS | (1 << TAPROOT_BIT))
                            .expect("version-bits value fits i32"),
                    )
                } else {
                    Version::from_consensus(4)
                },
                prev_blockhash: parent.hash,
                merkle_root: TxMerkleNode::all_zeros(),
                time: TAPROOT_START_TIME + height,
                bits: Target::MAX_ATTAINABLE_REGTEST.to_compact_lossy(),
                nonce: 0,
            };
            while header.validate_pow(Target::MAX_ATTAINABLE_REGTEST).is_err() {
                header.nonce += 1;
            }
            headers.insert_contextual(header, u32::MAX).unwrap();
        }
        let params = VersionBitsParams {
            period: 4,
            threshold: 3,
            start_time: i64::from(TAPROOT_START_TIME),
            timeout: i64::from(TAPROOT_TIMEOUT),
            min_activation_height: 12,
        };
        assert_eq!(
            threshold_state(&headers, 4, params),
            ThresholdState::Started
        );
        assert_eq!(
            threshold_state(&headers, 8, params),
            ThresholdState::LockedIn
        );
        assert_eq!(
            threshold_state(&headers, 12, params),
            ThresholdState::Active
        );
        assert!(
            taproot_active(
                &headers,
                1,
                &DeploymentConfig::for_network(Network::Regtest),
            )
            .unwrap()
        );
    }

    #[test]
    fn parses_core_compatible_regtest_vbparams_and_special_states() {
        let headers = HeaderDag::new(Network::Regtest);
        let mut config = DeploymentConfig::for_network(Network::Regtest);
        assert_eq!(config.consensus_id().len(), 29);
        assert_eq!(config.consensus_id()[0], CONFIG_ENCODING_VERSION);
        config.apply_vbparams("taproot:-2:0").unwrap();
        assert!(!taproot_active(&headers, 1, &config).unwrap());

        config
            .apply_vbparams("taproot:-1:9223372036854775807:-5")
            .unwrap();
        assert!(taproot_active(&headers, 1, &config).unwrap());
        assert_ne!(
            config.consensus_id(),
            DeploymentConfig::for_network(Network::Regtest).consensus_id()
        );
        assert_eq!(config.consensus_id().len(), 29);
    }

    #[test]
    fn core_compatible_buried_overrides_select_every_consensus_boundary() {
        let mut config = DeploymentConfig::for_network(Network::Regtest);
        for value in ["bip34@10", "dersig@11", "cltv@12", "csv@13", "segwit@14"] {
            config.apply_test_activation_height(value).unwrap();
        }
        let context = |height| {
            block_deployment_context_with_config(&config, height, BlockHash::all_zeros(), 0, false)
        };
        assert!(!context(9).bip34_active);
        assert!(context(10).bip34_active);
        assert_eq!(config.minimum_block_version(9), 1);
        assert_eq!(config.minimum_block_version(10), 2);
        assert_eq!(config.minimum_block_version(11), 3);
        assert_eq!(config.minimum_block_version(12), 4);
        assert_eq!(
            context(10).script_flags & bitcoinconsensus::VERIFY_DERSIG,
            0
        );
        assert_ne!(
            context(11).script_flags & bitcoinconsensus::VERIFY_DERSIG,
            0
        );
        assert_ne!(
            context(12).script_flags & bitcoinconsensus::VERIFY_CHECKLOCKTIMEVERIFY,
            0
        );
        assert!(!context(12).csv_active);
        assert!(context(13).csv_active);
        assert_ne!(
            context(13).script_flags & bitcoinconsensus::VERIFY_CHECKSEQUENCEVERIFY,
            0
        );
        assert!(!context(13).segwit_active);
        assert_ne!(
            context(13).script_flags & bitcoinconsensus::VERIFY_WITNESS,
            0
        );
        assert!(context(14).segwit_active);
        assert_ne!(
            context(14).script_flags & bitcoinconsensus::VERIFY_WITNESS,
            0
        );
        assert_eq!(config.consensus_id().len(), 49);
        assert_eq!(config.consensus_id()[0], BURIED_CONFIG_ENCODING_VERSION);
        let original_id = config.consensus_id();
        config.apply_test_activation_height("bip34@11").unwrap();
        assert_ne!(config.consensus_id(), original_id);
    }

    #[test]
    fn buried_overrides_are_strict_regtest_only_and_last_value_wins() {
        let mut config = DeploymentConfig::for_network(Network::Regtest);
        config.apply_test_activation_height("bip34@10").unwrap();
        config.apply_test_activation_height("bip34@20").unwrap();
        assert!(
            !block_deployment_context_with_config(&config, 19, BlockHash::all_zeros(), 0, false,)
                .bip34_active
        );
        assert_eq!(
            config.apply_test_activation_height("bip66@10"),
            Err(DeploymentConfigError::UnknownBuriedDeployment(
                "bip66".to_owned()
            ))
        );
        assert_eq!(
            config.apply_test_activation_height("csv"),
            Err(DeploymentConfigError::TestActivationMalformed)
        );
        assert_eq!(
            config.apply_test_activation_height("csv@2147483647"),
            Err(DeploymentConfigError::BuriedActivationHeight(
                "csv@2147483647".to_owned()
            ))
        );
        assert!(
            config
                .apply_test_activation_height("csv@2147483646")
                .is_ok()
        );
        assert_eq!(
            DeploymentConfig::for_network(Network::Bitcoin).apply_test_activation_height("csv@10"),
            Err(DeploymentConfigError::TestActivationRegtestOnly)
        );
    }

    #[test]
    fn configured_regtest_taproot_uses_core_period_and_threshold() {
        let mut headers = HeaderDag::new(Network::Regtest);
        for height in 1..432_u32 {
            let parent = headers.active_tip();
            let signals = (144..252).contains(&height);
            let mut header = Header {
                version: if signals {
                    Version::from_consensus(
                        i32::try_from(VERSION_BITS_TOP_BITS | (1 << TAPROOT_BIT))
                            .expect("version-bits value fits i32"),
                    )
                } else {
                    Version::from_consensus(4)
                },
                prev_blockhash: parent.hash,
                merkle_root: TxMerkleNode::all_zeros(),
                time: parent.header.time + 1,
                bits: Target::MAX_ATTAINABLE_REGTEST.to_compact_lossy(),
                nonce: 0,
            };
            while header.validate_pow(Target::MAX_ATTAINABLE_REGTEST).is_err() {
                header.nonce += 1;
            }
            headers.insert_contextual(header, u32::MAX).unwrap();
        }
        let mut config = DeploymentConfig::for_network(Network::Regtest);
        config
            .apply_vbparams("taproot:0:9223372036854775807:432")
            .unwrap();

        assert!(!taproot_active(&headers, 144, &config).unwrap());
        assert!(!taproot_active(&headers, 288, &config).unwrap());
        assert!(taproot_active(&headers, 432, &config).unwrap());
    }

    #[test]
    fn rejects_invalid_or_non_regtest_vbparams() {
        let mut regtest = DeploymentConfig::for_network(Network::Regtest);
        assert_eq!(
            regtest.apply_vbparams("unknown:0:1"),
            Err(DeploymentConfigError::UnknownDeployment(
                "unknown".to_owned()
            ))
        );
        assert_eq!(
            regtest.apply_vbparams("taproot:invalid:1"),
            Err(DeploymentConfigError::StartTime("invalid".to_owned()))
        );
        assert_eq!(
            regtest.apply_vbparams("taproot:0:1:2:3"),
            Err(DeploymentConfigError::Malformed)
        );
        assert_eq!(
            DeploymentConfig::for_network(Network::Bitcoin).apply_vbparams("taproot:0:1"),
            Err(DeploymentConfigError::RegtestOnly)
        );
        assert_eq!(
            taproot_active(
                &HeaderDag::new(Network::Bitcoin),
                1,
                &DeploymentConfig::for_network(Network::Regtest),
            ),
            Err(DeploymentConfigError::NetworkMismatch)
        );
    }

    #[test]
    fn header_derived_block_context_rejects_another_network() {
        let headers = HeaderDag::new(Network::Testnet);
        assert_eq!(
            block_deployment_context_for_headers(
                &DeploymentConfig::for_network(Network::Bitcoin),
                &headers,
                1,
                BlockHash::all_zeros(),
                0,
                false,
            ),
            Err(DeploymentConfigError::NetworkMismatch)
        );
    }

    proptest! {
        #[test]
        fn buried_height_parser_matches_core_signed_int_range(height in any::<u32>()) {
            let mut config = DeploymentConfig::for_network(Network::Regtest);
            let result = config.apply_test_activation_height(&format!("csv@{height}"));
            prop_assert_eq!(result.is_ok(), height < i32::MAX as u32);
        }

        #[test]
        fn rejected_buried_override_never_partially_mutates_config(
            value in proptest::string::string_regex("[ -~]{0,64}").unwrap()
        ) {
            let mut config = DeploymentConfig::for_network(Network::Regtest);
            let original = config.clone();
            if config.apply_test_activation_height(&value).is_err() {
                prop_assert_eq!(config, original);
            }
        }
    }
}
