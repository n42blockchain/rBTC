#![no_main]

use libfuzzer_sys::fuzz_target;
use rbtc::{
    peer_store::{
        validate_stored_peer_penalty, validate_stored_peer_record, validate_stored_tried_collisions,
    },
    rebroadcast_store::validate_persisted_rebroadcast_entry,
    transaction_pool_store::validate_persisted_transaction_pool_snapshot,
    validation_owner::parse_validation_directory_owner,
};

fuzz_target!(|input: &[u8]| {
    let Some((&kind, value)) = input.split_first() else {
        return;
    };
    if value.len() > 257 * 1024 {
        return;
    }
    match kind % 6 {
        0 => {
            if value.len() <= 8 * 1024 {
                let _ = parse_validation_directory_owner(value);
            }
        }
        1 => {
            if value.len() <= 8 * 1024 {
                let _ = validate_stored_peer_record(value);
            }
        }
        2 => {
            if value.len() <= 8 * 1024 {
                let _ = validate_stored_peer_penalty(value);
            }
        }
        3 => {
            if value.len() <= 8 * 1024 {
                let _ = validate_stored_tried_collisions(value);
            }
        }
        4 => {
            let split = value.len().min(32);
            let (key, value) = value.split_at(split);
            let _ = validate_persisted_rebroadcast_entry(key, value);
        }
        _ => {
            let _ = validate_persisted_transaction_pool_snapshot(value);
        }
    }
});
