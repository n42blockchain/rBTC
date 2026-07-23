#![no_main]

use libfuzzer_sys::fuzz_target;
use rbtc::wallet::parse_wallet_descriptor_config;

fuzz_target!(|input: &[u8]| {
    if input.len() > 128 * 1024 {
        return;
    }
    let _ = parse_wallet_descriptor_config(input);
});
