//! Historical activation and exception anchors pinned from Bitcoin Core 26.
//!
//! These hashes make deployment drift visible in normal offline CI. They are
//! not a substitute for executing a broad raw-block corpus with its UTXO view.

use std::str::FromStr;

use bitcoin::{BlockHash, Network};
use rbtc::deployments::block_deployment_context;

fn hash(value: &str) -> BlockHash {
    BlockHash::from_str(value).expect("Core 26 historical hash")
}

#[test]
fn mainnet_buried_activation_anchors_select_the_expected_rules() {
    let cases = [
        (
            227_931,
            "000000000000024b89b42a942fe0d9fea3bb44ab7bd1b19115dd6a759c0808b8",
            true,
            false,
            false,
        ),
        (
            363_725,
            "00000000000000000379eaa19dce8c9b722d46ae6a57c2f1a988119488b50931",
            true,
            false,
            false,
        ),
        (
            388_381,
            "000000000000000004c2b624ed5d7756c508d90fd0da2c7c679febfa6c4735f0",
            true,
            false,
            false,
        ),
        (
            419_328,
            "000000000000000004a1b34462cb8aeebd5799177f7a29cf28f2d1961716b5b5",
            true,
            true,
            false,
        ),
        (
            481_824,
            "0000000000000000001c8018d9cb3b742ef25114f27563e3fc4a1902167f9893",
            true,
            true,
            true,
        ),
    ];
    for (height, block_hash, bip34, csv, segwit) in cases {
        let context =
            block_deployment_context(Network::Bitcoin, height, hash(block_hash), 0, false);
        assert_eq!(context.bip34_active, bip34, "height {height}");
        assert_eq!(context.csv_active, csv, "height {height}");
        assert_eq!(context.segwit_active, segwit, "height {height}");
    }
}

#[test]
fn testnet_buried_activation_anchors_select_the_expected_rules() {
    let cases = [
        (
            21_111,
            "0000000023b3a96d3484e5abb3755c413e7d41500f8e2a5c3f0dd01299cd8ef8",
            true,
            false,
            false,
        ),
        (
            330_776,
            "000000002104c8c45e99a8853285a3b592602a3ccde2b832481da85e9e4ba182",
            true,
            false,
            false,
        ),
        (
            581_885,
            "00000000007f6655f22f98e72ed80d8b06dc761d5da09df0fa1dc4be4f861eb6",
            true,
            false,
            false,
        ),
        (
            770_112,
            "00000000025e930139bac5c6c31a403776da130831ab85be56578f3fa75369bb",
            true,
            true,
            false,
        ),
        (
            834_624,
            "00000000002b980fcd729daaa248fd9316a5200e9b367f4ff2c42453e84201ca",
            true,
            true,
            true,
        ),
    ];
    for (height, block_hash, bip34, csv, segwit) in cases {
        let context =
            block_deployment_context(Network::Testnet, height, hash(block_hash), 0, false);
        assert_eq!(context.bip34_active, bip34, "height {height}");
        assert_eq!(context.csv_active, csv, "height {height}");
        assert_eq!(context.segwit_active, segwit, "height {height}");
    }
}

#[test]
fn historical_script_and_bip30_exception_hashes_are_exact() {
    let mainnet_p2sh = block_deployment_context(
        Network::Bitcoin,
        170_060,
        hash("00000000000002dc756eebf4f49723ed8d30cc28a5f108eb94b1ba88ac4f9c22"),
        0,
        false,
    );
    assert_eq!(mainnet_p2sh.script_flags, bitcoinconsensus::VERIFY_NONE);

    let mainnet_taproot = block_deployment_context(
        Network::Bitcoin,
        709_632,
        hash("0000000000000000000f14c35b2d841e986ab5441de8c585d5ffe55ea1e395ad"),
        0,
        true,
    );
    assert_eq!(
        mainnet_taproot.script_flags,
        bitcoinconsensus::VERIFY_P2SH
            | bitcoinconsensus::VERIFY_WITNESS
            | bitcoinconsensus::VERIFY_DERSIG
            | bitcoinconsensus::VERIFY_CHECKLOCKTIMEVERIFY
            | bitcoinconsensus::VERIFY_CHECKSEQUENCEVERIFY
            | bitcoinconsensus::VERIFY_NULLDUMMY
    );

    let testnet_p2sh = block_deployment_context(
        Network::Testnet,
        0,
        hash("00000000dd30457c001f4095d208cc1296b0eed002427aa599874af7a432b105"),
        0,
        false,
    );
    assert_eq!(testnet_p2sh.script_flags, bitcoinconsensus::VERIFY_NONE);

    for (height, block_hash) in [
        (
            91_842,
            "00000000000a4d0a398161ffc163c503763b1f4360639393e0e4c8e300e0caec",
        ),
        (
            91_880,
            "00000000000743f190a18c5577a3c2d2a1f610ae9601ac046a38084ccb7cd721",
        ),
    ] {
        assert!(
            !block_deployment_context(Network::Bitcoin, height, hash(block_hash), 0, false,)
                .bip30_enforced
        );
    }
}
