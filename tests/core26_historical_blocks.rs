//! Self-authenticating raw mainnet blocks at Core 26 historical exceptions.

use bitcoin::{Block, Network, consensus::deserialize, hex::FromHex};
use rbtc::{
    blockchain::{BlockError, validate_block_structure_with_deployments},
    deployments::block_deployment_context,
};

const BIP30_REPEAT_91842: &str = include_str!(
    "data/bitcoin-core-26/mainnet-00000000000a4d0a398161ffc163c503763b1f4360639393e0e4c8e300e0caec.hex"
);
const BIP30_REPEAT_91880: &str = include_str!(
    "data/bitcoin-core-26/mainnet-00000000000743f190a18c5577a3c2d2a1f610ae9601ac046a38084ccb7cd721.hex"
);
const BIP16_EXCEPTION_170060: &str = include_str!(
    "data/bitcoin-core-26/mainnet-00000000000002dc756eebf4f49723ed8d30cc28a5f108eb94b1ba88ac4f9c22.hex"
);
const BIP34_ACTIVATION_227931_ZSTD: &str = include_str!(
    "data/bitcoin-core-26/mainnet-000000000000024b89b42a942fe0d9fea3bb44ab7bd1b19115dd6a759c0808b8.zst.hex"
);
const BIP66_ACTIVATION_363725: &str = include_str!(
    "data/bitcoin-core-26/mainnet-00000000000000000379eaa19dce8c9b722d46ae6a57c2f1a988119488b50931.hex"
);
const BIP65_ACTIVATION_388381_ZSTD: &[u8] = include_bytes!(
    "data/bitcoin-core-26/mainnet-000000000000000004c2b624ed5d7756c508d90fd0da2c7c679febfa6c4735f0.zst"
);
const CSV_ACTIVATION_419328_ZSTD: &[u8] = include_bytes!(
    "data/bitcoin-core-26/mainnet-000000000000000004a1b34462cb8aeebd5799177f7a29cf28f2d1961716b5b5.zst"
);

fn fixture(encoded: &str) -> Block {
    let compact = encoded.split_whitespace().collect::<String>();
    deserialize(&Vec::<u8>::from_hex(&compact).expect("hex fixture")).expect("raw Core block")
}

fn compressed_fixture(encoded: &str) -> Block {
    let compact = encoded.split_whitespace().collect::<String>();
    let compressed = Vec::<u8>::from_hex(&compact).expect("hex fixture");
    let raw = zstd::stream::decode_all(compressed.as_slice()).expect("zstd fixture");
    deserialize(&raw).expect("raw Core block")
}

fn binary_compressed_fixture(encoded: &[u8]) -> Block {
    let raw = zstd::stream::decode_all(encoded).expect("zstd fixture");
    deserialize(&raw).expect("raw Core block")
}

fn validate_fixture(encoded: &str, height: u32, expected_hash: &str) -> Block {
    validate_decoded_fixture(fixture(encoded), height, expected_hash)
}

fn validate_decoded_fixture(block: Block, height: u32, expected_hash: &str) -> Block {
    assert_eq!(block.block_hash().to_string(), expected_hash);
    block
        .header
        .validate_pow(block.header.target())
        .expect("historical block satisfies its claimed proof of work");
    let context = block_deployment_context(
        Network::Bitcoin,
        height,
        block.block_hash(),
        block.header.time,
        false,
    );
    validate_block_structure_with_deployments(
        &block,
        height,
        context.bip34_active,
        context.segwit_active,
        context.signet_challenge.as_deref(),
    )
    .expect("historical block structure and commitments");
    block
}

#[test]
fn core_bip30_repeat_blocks_are_pinned_and_select_the_exception() {
    for (encoded, height, expected_hash) in [
        (
            BIP30_REPEAT_91842,
            91_842,
            "00000000000a4d0a398161ffc163c503763b1f4360639393e0e4c8e300e0caec",
        ),
        (
            BIP30_REPEAT_91880,
            91_880,
            "00000000000743f190a18c5577a3c2d2a1f610ae9601ac046a38084ccb7cd721",
        ),
    ] {
        let block = validate_fixture(encoded, height, expected_hash);
        let context = block_deployment_context(
            Network::Bitcoin,
            height,
            block.block_hash(),
            block.header.time,
            false,
        );
        assert!(!context.bip30_enforced);
    }
}

#[test]
fn core_bip16_exception_block_uses_no_base_script_flags() {
    let block = validate_fixture(
        BIP16_EXCEPTION_170060,
        170_060,
        "00000000000002dc756eebf4f49723ed8d30cc28a5f108eb94b1ba88ac4f9c22",
    );
    let context = block_deployment_context(
        Network::Bitcoin,
        170_060,
        block.block_hash(),
        block.header.time,
        false,
    );
    assert_eq!(context.script_flags, bitcoinconsensus::VERIFY_NONE);
    assert!(context.bip30_enforced);
}

#[test]
fn historical_fixture_tampering_is_rejected_by_merkle_commitment() {
    let mut block = fixture(BIP16_EXCEPTION_170060);
    block.txdata[1].output[0].script_pubkey = bitcoin::ScriptBuf::from_bytes(vec![0x51]);
    assert!(matches!(
        validate_block_structure_with_deployments(&block, 170_060, false, false, None),
        Err(BlockError::MerkleRoot)
    ));
    assert_eq!(
        block.block_hash().to_string(),
        "00000000000002dc756eebf4f49723ed8d30cc28a5f108eb94b1ba88ac4f9c22"
    );
}

#[test]
fn real_bip34_activation_block_enforces_coinbase_height() {
    let block = compressed_fixture(BIP34_ACTIVATION_227931_ZSTD);
    assert_eq!(
        block.block_hash().to_string(),
        "000000000000024b89b42a942fe0d9fea3bb44ab7bd1b19115dd6a759c0808b8"
    );
    let context = block_deployment_context(
        Network::Bitcoin,
        227_931,
        block.block_hash(),
        block.header.time,
        false,
    );
    assert!(context.bip34_active);
    validate_block_structure_with_deployments(
        &block,
        227_931,
        context.bip34_active,
        context.segwit_active,
        None,
    )
    .unwrap();

    assert!(matches!(
        validate_block_structure_with_deployments(&block, 227_932, true, false, None),
        Err(BlockError::Bip34Height { height: 227_932 })
    ));
}

#[test]
fn real_bip66_activation_block_selects_der_signatures_and_version_three() {
    let block = validate_fixture(
        BIP66_ACTIVATION_363725,
        363_725,
        "00000000000000000379eaa19dce8c9b722d46ae6a57c2f1a988119488b50931",
    );
    let context = block_deployment_context(
        Network::Bitcoin,
        363_725,
        block.block_hash(),
        block.header.time,
        false,
    );
    assert_ne!(context.script_flags & bitcoinconsensus::VERIFY_DERSIG, 0);
    assert_eq!(block.header.version.to_consensus(), 3);
}

#[test]
fn real_bip65_activation_block_selects_cltv_and_version_four() {
    let block = validate_decoded_fixture(
        binary_compressed_fixture(BIP65_ACTIVATION_388381_ZSTD),
        388_381,
        "000000000000000004c2b624ed5d7756c508d90fd0da2c7c679febfa6c4735f0",
    );
    let context = block_deployment_context(
        Network::Bitcoin,
        388_381,
        block.block_hash(),
        block.header.time,
        false,
    );
    assert_ne!(
        context.script_flags & bitcoinconsensus::VERIFY_CHECKLOCKTIMEVERIFY,
        0
    );
    assert!(!context.csv_active);
    assert_eq!(block.header.version.to_consensus(), 4);
}

#[test]
fn real_csv_activation_block_selects_sequence_locks_without_segwit() {
    let block = validate_decoded_fixture(
        binary_compressed_fixture(CSV_ACTIVATION_419328_ZSTD),
        419_328,
        "000000000000000004a1b34462cb8aeebd5799177f7a29cf28f2d1961716b5b5",
    );
    let context = block_deployment_context(
        Network::Bitcoin,
        419_328,
        block.block_hash(),
        block.header.time,
        false,
    );
    assert!(context.csv_active);
    assert!(!context.segwit_active);
    assert_ne!(
        context.script_flags & bitcoinconsensus::VERIFY_CHECKSEQUENCEVERIFY,
        0
    );
    assert!(
        block
            .txdata
            .iter()
            .flat_map(|transaction| &transaction.input)
            .all(|input| input.witness.is_empty())
    );
}
