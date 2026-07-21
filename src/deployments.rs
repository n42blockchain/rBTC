//! Network-specific buried deployments and consensus script flags.
//!
//! Values mirror the Bitcoin Core 26 sources bundled by the pinned
//! `bitcoinconsensus` dependency. Version-bits state remains an explicit input
//! for networks where Taproot was not configured `ALWAYS_ACTIVE`.

use std::str::FromStr;

use bitcoin::{BlockHash, Network};

use crate::{block_execution::BlockDeploymentContext, headers::HeaderDag};

const VERSION_BITS_TOP_MASK: u32 = 0xE000_0000;
const VERSION_BITS_TOP_BITS: u32 = 0x2000_0000;
const TAPROOT_BIT: u32 = 2;
const TAPROOT_START_TIME: u32 = 1_619_222_400;
const TAPROOT_TIMEOUT: u32 = 1_628_640_000;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ThresholdState {
    Defined,
    Started,
    LockedIn,
    Active,
    Failed,
}

#[derive(Clone, Copy)]
struct VersionBitsParams {
    period: u32,
    threshold: u32,
    min_activation_height: u32,
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
    let bip30_exception = is_bip30_exception(network, height, block_hash);
    if let Some(script_flags) = script_flag_exception(network, block_hash) {
        return BlockDeploymentContext {
            script_flags,
            bip34_active: height >= activation_heights(network).bip34,
            csv_active: height >= activation_heights(network).csv,
            bip30_exception,
        };
    }
    let heights = activation_heights(network);
    let mut script_flags = bitcoinconsensus::VERIFY_NONE;
    if block_time >= 1_333_238_400 {
        script_flags |= bitcoinconsensus::VERIFY_P2SH;
    }
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
        script_flags |= bitcoinconsensus::VERIFY_NULLDUMMY | bitcoinconsensus::VERIFY_WITNESS;
    }
    if taproot_active {
        script_flags |= bitcoinconsensus::VERIFY_TAPROOT;
    }
    BlockDeploymentContext {
        script_flags,
        bip34_active: height >= heights.bip34,
        csv_active: height >= heights.csv,
        bip30_exception,
    }
}

/// Whether Taproot is unconditionally active for this network configuration.
#[must_use]
pub const fn taproot_always_active(network: Network) -> bool {
    matches!(
        network,
        Network::Testnet4 | Network::Signet | Network::Regtest
    )
}

/// Computes Taproot's BIP9 state for a candidate on the active header chain.
///
/// Mainnet and legacy testnet use the Core 26 deployment parameters. Newer
/// test networks and default regtest configure Taproot as always active.
#[must_use]
pub fn taproot_active(headers: &HeaderDag, candidate_height: u32) -> bool {
    let network = headers.network();
    if taproot_always_active(network) {
        return true;
    }
    let params = match network {
        Network::Bitcoin => VersionBitsParams {
            period: 2_016,
            threshold: 1_815,
            min_activation_height: 709_632,
        },
        Network::Testnet => VersionBitsParams {
            period: 2_016,
            threshold: 1_512,
            min_activation_height: 0,
        },
        Network::Testnet4 | Network::Signet | Network::Regtest => return true,
    };
    threshold_state(headers, candidate_height, params) == ThresholdState::Active
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
        let period_mtp = headers
            .median_time_past(period_end_header.hash)
            .expect("active header has median time past");
        state = match state {
            ThresholdState::Defined if period_mtp >= TAPROOT_START_TIME => ThresholdState::Started,
            ThresholdState::Started => {
                let period_start = period_end + 1 - params.period;
                let signals = (period_start..=period_end)
                    .filter_map(|height| headers.active_header_at(height))
                    .filter(|header| signals_taproot(header.header.version.to_consensus()))
                    .count();
                if signals >= usize::try_from(params.threshold).expect("threshold fits usize") {
                    ThresholdState::LockedIn
                } else if period_mtp >= TAPROOT_TIMEOUT {
                    ThresholdState::Failed
                } else {
                    ThresholdState::Started
                }
            }
            ThresholdState::LockedIn
                if period_end.saturating_add(1) >= params.min_activation_height =>
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

#[derive(Clone, Copy)]
struct ActivationHeights {
    bip34: u32,
    bip65: u32,
    bip66: u32,
    csv: u32,
    segwit: u32,
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

fn parse_hash(hash: &str) -> BlockHash {
    BlockHash::from_str(hash).expect("hard-coded Bitcoin Core block hash")
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
        assert_eq!(context.script_flags & bitcoinconsensus::VERIFY_P2SH, 0);
        assert_ne!(
            context.script_flags & bitcoinconsensus::VERIFY_CHECKSEQUENCEVERIFY,
            0
        );
        assert_ne!(context.script_flags & bitcoinconsensus::VERIFY_WITNESS, 0);
        assert_ne!(context.script_flags & bitcoinconsensus::VERIFY_TAPROOT, 0);
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
        assert_eq!(csv.script_flags & bitcoinconsensus::VERIFY_WITNESS, 0);
        let segwit = block_deployment_context(
            Network::Bitcoin,
            481_824,
            BlockHash::all_zeros(),
            u32::MAX,
            false,
        );
        assert_ne!(segwit.script_flags & bitcoinconsensus::VERIFY_WITNESS, 0);
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
                    Version::ONE
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
        assert!(taproot_active(&headers, 1));
    }
}
