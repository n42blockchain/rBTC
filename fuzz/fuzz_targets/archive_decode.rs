#![no_main]

use bitcoin::hex::FromHex;
use libfuzzer_sys::fuzz_target;
use rbtc::archive::decode_archive;

fuzz_target!(|input: &[u8]| {
    if input.len() > 1024 * 1024 {
        return;
    }
    let decoded = std::str::from_utf8(input)
        .ok()
        .and_then(|text| Vec::<u8>::from_hex(text.trim()).ok());
    let bytes = decoded.as_deref().unwrap_or(input);
    let _ = decode_archive(bytes);
});
