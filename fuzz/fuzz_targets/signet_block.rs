#![no_main]

use bitcoin::{Block, consensus::deserialize, hex::FromHex};
use libfuzzer_sys::fuzz_target;
use rbtc::signet::validate_default_signet_block_solution;

fuzz_target!(|input: &[u8]| {
    if input.len() > 4_000_000 {
        return;
    }
    let decoded = std::str::from_utf8(input)
        .ok()
        .and_then(|text| Vec::<u8>::from_hex(text.trim()).ok());
    let bytes = decoded.as_deref().unwrap_or(input);
    if let Ok(block) = deserialize::<Block>(bytes) {
        let _ = validate_default_signet_block_solution(&block);
    }
});
