#![no_main]

use bitcoin::{TxMerkleNode, Txid, hashes::Hash};
use libfuzzer_sys::fuzz_target;
use rbtc::merkle_proof::verify_transaction_merkle_proof;

fuzz_target!(|input: &[u8]| {
    if input.len() < 68 {
        return;
    }
    let txid = Txid::from_byte_array(input[..32].try_into().expect("fixed txid"));
    let position = u32::from_le_bytes(input[32..36].try_into().expect("fixed position"));
    let expected_root =
        TxMerkleNode::from_byte_array(input[36..68].try_into().expect("fixed root"));
    let (sibling_hashes, _) = input[68..].as_chunks::<32>();
    let siblings = sibling_hashes
        .iter()
        .take(33)
        .map(|bytes| TxMerkleNode::from_byte_array(*bytes))
        .collect::<Vec<_>>();
    let _ = verify_transaction_merkle_proof(txid, position, &siblings, expected_root);
});
