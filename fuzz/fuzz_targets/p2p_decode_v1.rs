#![no_main]

use bitcoin::hex::FromHex;
use libfuzzer_sys::fuzz_target;
use rbtc::p2p::decode_v1;

fuzz_target!(|input: &[u8]| {
    if input.len() > 4_000_024 {
        return;
    }
    let decoded = std::str::from_utf8(input)
        .ok()
        .and_then(|text| Vec::<u8>::from_hex(text.trim()).ok());
    let bytes = decoded.as_deref().unwrap_or(input);
    let _ = decode_v1(bytes);
});
