#![no_main]

use libfuzzer_sys::fuzz_target;
use rbtc::api::is_well_formed_local_rpc_request;

fuzz_target!(|input: &[u8]| {
    let _ = is_well_formed_local_rpc_request(input);
});
