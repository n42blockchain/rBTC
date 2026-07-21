//! Network-specific buried deployments and consensus script flags.
//!
//! Values mirror the Bitcoin Core 26 sources bundled by the pinned
//! `bitcoinconsensus` dependency. Version-bits state remains an explicit input
//! for networks where Taproot was not configured `ALWAYS_ACTIVE`.

use std::str::FromStr;

use bitcoin::{BlockHash, Network};

use crate::block_execution::BlockDeploymentContext;

/// Derives block and script consensus flags for one candidate.
#[must_use]
pub fn block_deployment_context(
    network: Network,
    height: u32,
    block_hash: BlockHash,
    block_time: u32,
    taproot_active: bool,
) -> BlockDeploymentContext {
    if let Some(script_flags) = script_flag_exception(network, block_hash) {
        return BlockDeploymentContext {
            script_flags,
            bip34_active: height >= activation_heights(network).bip34,
            csv_active: height >= activation_heights(network).csv,
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

fn parse_hash(hash: &str) -> BlockHash {
    BlockHash::from_str(hash).expect("hard-coded Bitcoin Core block hash")
}

#[cfg(test)]
mod tests {
    use bitcoin::hashes::Hash;

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
}
