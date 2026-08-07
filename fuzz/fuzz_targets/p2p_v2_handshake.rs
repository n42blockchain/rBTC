#![no_main]

use bitcoin::p2p::Magic;
use libfuzzer_sys::fuzz_target;
use rbtc::p2p_v2::{Role, V2Handshake, decode_v2_contents, derive_session_keys};

fuzz_target!(|input: &[u8]| {
    if input.len() > 65_536 {
        return;
    }
    // Message-type and payload decoding of raw packet contents.
    let _ = decode_v2_contents(Magic::BITCOIN, input);
    // Record layer driven with attacker-controlled length and ciphertext.
    let shared = [7u8; 32];
    let (_, mut receiver) =
        derive_session_keys(&shared, Magic::BITCOIN).into_packet_ciphers(Role::Responder);
    if input.len() >= 3 {
        let length = receiver.decrypt_length([input[0], input[1], input[2]]);
        if length <= input.len() {
            let _ = receiver.decrypt_packet(&[], &input[3..]);
        }
    }
    // Deterministic responder handshake fed incrementally, covering the v1
    // prefix check, key consumption, terminator scan, and packet bounds.
    let Some(mut handshake) =
        V2Handshake::responder_with_secret(Magic::BITCOIN, vec![0xaa; 21], [0x11; 32], [0x22; 32])
    else {
        return;
    };
    let split = input.len() / 2;
    if handshake.push_bytes(&input[..split]).is_ok() {
        let _ = handshake.push_bytes(&input[split..]);
    }
});
