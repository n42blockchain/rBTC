#![no_main]

use libfuzzer_sys::fuzz_target;
use rbtc::wallet::{parse_wallet_psbt_finalize_request, parse_wallet_psbt_request};

fuzz_target!(|input: &[u8]| {
    let _ = parse_wallet_psbt_request(input);
    let _ = parse_wallet_psbt_finalize_request(input);
});
