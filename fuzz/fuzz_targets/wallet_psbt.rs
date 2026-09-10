#![no_main]

use bitcoin::{
    base64::{Engine as _, engine::general_purpose::STANDARD},
    psbt::Psbt,
};
use libfuzzer_sys::fuzz_target;
use rbtc::wallet::{parse_wallet_psbt_finalize_request, parse_wallet_psbt_request};

// Compile the same private wire checker used by EmbeddedWallet. Do not expose
// a production API solely to let a fuzz target reach this boundary.
#[path = "../../src/psbt_envelope.rs"]
mod psbt_envelope;

const MAX_DECODED_BYTES: usize = 512 * 1024;

fn check_raw(raw: &[u8]) {
    let (inputs, outputs) = Psbt::deserialize(raw)
        .map(|psbt| {
            let canonical = psbt.serialize();
            assert!(
                psbt_envelope::validate_envelope(&canonical, psbt.inputs.len(), psbt.outputs.len())
                    .is_ok()
            );
            (psbt.inputs.len(), psbt.outputs.len())
        })
        .unwrap_or((1, 1));
    if psbt_envelope::validate_envelope(raw, inputs, outputs).is_ok() {
        let mut trailing = raw.to_vec();
        trailing.push(0);
        assert!(psbt_envelope::validate_envelope(&trailing, inputs, outputs).is_err());
        assert!(psbt_envelope::validate_envelope(&raw[..raw.len() - 1], inputs, outputs).is_err());
    }
}

fn check_base64(encoded: &str) {
    let bounded = psbt_envelope::decode_base64(encoded, MAX_DECODED_BYTES);
    let reference = STANDARD.decode(encoded);
    match reference {
        Ok(raw) if raw.len() <= MAX_DECODED_BYTES => {
            assert_eq!(bounded.as_ref().unwrap(), &raw);
            assert_eq!(STANDARD.encode(&raw), encoded);
            check_raw(&raw);
            if !raw.is_empty() {
                assert!(psbt_envelope::decode_base64(encoded, raw.len() - 1).is_err());
            }
        }
        _ => assert!(bounded.is_err()),
    }
}

fuzz_target!(|input: &[u8]| {
    if input.len() > 786_433 {
        return;
    }
    let _ = parse_wallet_psbt_request(input);
    if let Ok(request) = parse_wallet_psbt_finalize_request(input) {
        check_base64(&request.psbt);
    }
    if let Ok(encoded) = std::str::from_utf8(input) {
        check_base64(encoded);
    }
    if input.len() <= MAX_DECODED_BYTES {
        // Raw seeds reach map/witness parsing without first solving base64 or
        // JSON syntax. Encoding them also exercises the production decoder.
        check_base64(&STANDARD.encode(input));
    }
});
