//! Boundary regressions motivated by btcd 0.26.2 and BIP350.
//!
//! No daemon private-key import is introduced: WIF tests pin the dependency
//! boundary, while actual transaction signatures remain consensus-verified.

use std::str::FromStr;

use bitcoin::secp256k1::{
    Keypair, Message, Secp256k1, SecretKey, XOnlyPublicKey, schnorr::Signature,
};
use bitcoin::{Address, PrivateKey, hex::FromHex};

#[test]
fn bip350_future_witness_addresses_require_the_correct_checksum_family() {
    // Primary vectors: https://github.com/bitcoin/bips/blob/master/bip-0350.mediawiki
    for address in [
        "BC1SW50QGDZ25J",
        "bc1zw508d6qejxtdg4y5r3zarvaryvaxxpcs",
        "bc1p0xlxvlhemja6c4dqv22uapctqupfhlxm9h8z3k2e72q4k9hcz7vqzk5jj0",
    ] {
        assert!(Address::from_str(address).is_ok(), "{address}");
    }
    for address in [
        "bc1p0xlxvlhemja6c4dqv22uapctqupfhlxm9h8z3k2e72q4k9hcz7vqh2y7hd",
        "tb1z0xlxvlhemja6c4dqv22uapctqupfhlxm9h8z3k2e72q4k9hcz7vqglt7rf",
        "BC1S0XLXVLHEMJA6C4DQV22UAPCTQUPFHLXM9H8Z3K2E72Q4K9HCZ7VQ54WELL",
        "bc1qw508d6qejxtdg4y5r3zarvary0c5xw7kemeawh",
    ] {
        assert!(Address::from_str(address).is_err(), "{address}");
    }
}

#[test]
fn wif_rejects_zero_and_scalars_at_or_above_the_group_order() {
    let order =
        Vec::<u8>::from_hex("fffffffffffffffffffffffffffffffebaaedce6af48a03bbfd25e8cd0364141")
            .unwrap();
    for compressed in [false, true] {
        for scalar in [vec![0; 32], order.clone(), vec![0xff; 32]] {
            let mut payload = vec![0x80];
            payload.extend(scalar);
            if compressed {
                payload.push(1);
            }
            let encoded = bitcoin::base58::encode_check(&payload);
            assert!(PrivateKey::from_wif(&encoded).is_err());
        }
        let mut payload = vec![0x80];
        let mut valid = order.clone();
        valid[31] -= 1;
        payload.extend(valid);
        if compressed {
            payload.push(1);
        }
        assert!(PrivateKey::from_wif(&bitcoin::base58::encode_check(&payload)).is_ok());
    }
}

#[test]
fn schnorr_out_of_range_s_cannot_pass_verification() {
    let secp = Secp256k1::new();
    let keypair = Keypair::from_secret_key(&secp, &SecretKey::from_slice(&[1; 32]).unwrap());
    let (public_key, _) = XOnlyPublicKey::from_keypair(&keypair);
    let message = Message::from_digest([2; 32]);
    let valid = secp.sign_schnorr_no_aux_rand(&message, &keypair);
    secp.verify_schnorr(&valid, &message, &public_key).unwrap();
    let order =
        Vec::<u8>::from_hex("fffffffffffffffffffffffffffffffebaaedce6af48a03bbfd25e8cd0364141")
            .unwrap();
    for scalar in [order, vec![0xff; 32]] {
        let mut bytes = valid.as_ref().to_vec();
        bytes[32..].copy_from_slice(&scalar);
        // Raw signature containers may accept arbitrary 64-byte strings;
        // verification, not constructing that container, is the trust gate.
        assert!(Signature::from_slice(&bytes).map_or(true, |signature| {
            secp.verify_schnorr(&signature, &message, &public_key)
                .is_err()
        }));
    }
}
