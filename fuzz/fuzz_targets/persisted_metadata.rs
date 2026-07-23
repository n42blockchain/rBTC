#![no_main]

use libfuzzer_sys::fuzz_target;
use rbtc::{
    peer_store::{
        validate_stored_peer_penalty, validate_stored_peer_record,
        validate_stored_tried_collisions,
    },
    validation_owner::parse_validation_directory_owner,
};

fuzz_target!(|input: &[u8]| {
    let Some((&kind, value)) = input.split_first() else {
        return;
    };
    if value.len() > 8 * 1024 {
        return;
    }
    match kind % 4 {
        0 => {
            let _ = parse_validation_directory_owner(value);
        }
        1 => {
            let _ = validate_stored_peer_record(value);
        }
        2 => {
            let _ = validate_stored_peer_penalty(value);
        }
        _ => {
            let _ = validate_stored_tried_collisions(value);
        }
    }
});
