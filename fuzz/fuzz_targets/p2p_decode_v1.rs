#![no_main]

use bitcoin::{
    hashes::{Hash, sha256d},
    hex::FromHex,
};
use libfuzzer_sys::fuzz_target;
use rbtc::p2p::{decode_v1, validate_post_handshake_message};

fuzz_target!(|input: &[u8]| {
    if input.len() > 4_000_024 {
        return;
    }
    let decoded = std::str::from_utf8(input)
        .ok()
        .and_then(|text| Vec::<u8>::from_hex(text.trim()).ok());
    let bytes = decoded.as_deref().unwrap_or(input);
    if let Ok(message) = decode_v1(bytes) {
        let _ = validate_post_handshake_message(message.payload());
    }
    if bytes.len() >= 24 {
        // Checksum and length mutations would otherwise keep almost every
        // interesting payload change outside the inner message decoder.
        let mut framed = bytes.to_vec();
        let payload_len = u32::try_from(framed.len() - 24).unwrap();
        framed[16..20].copy_from_slice(&payload_len.to_le_bytes());
        let checksum = sha256d::Hash::hash(&framed[24..]).to_byte_array();
        framed[20..24].copy_from_slice(&checksum[..4]);
        if let Ok(message) = decode_v1(&framed) {
            let _ = validate_post_handshake_message(message.payload());
        }
    }
});
