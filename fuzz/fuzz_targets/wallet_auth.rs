#![no_main]

use libfuzzer_sys::fuzz_target;
use rbtc::api::WalletAuthToken;

const TOKEN: &str = "0123456789abcdef0123456789abcdef";

fn exercise(input: &[u8]) {
    let configured = WalletAuthToken::new(TOKEN).expect("fixed fuzz token");
    let _ = configured.authorizes(input);

    if let Ok(candidate) = std::str::from_utf8(input)
        && let Ok(candidate) = WalletAuthToken::new(candidate)
    {
        let mut header = b"Bearer ".to_vec();
        header.extend_from_slice(input);
        assert!(candidate.authorizes(&header));

        if !input.is_empty() {
            let last = header.len() - 1;
            header[last] ^= 1;
            assert!(!candidate.authorizes(&header));
        }
    }
}

fuzz_target!(|input: &[u8]| {
    if input.len() > 4096 {
        return;
    }
    exercise(input);
    if let Some(input) = input.strip_suffix(b"\n") {
        exercise(input);
    }
});
