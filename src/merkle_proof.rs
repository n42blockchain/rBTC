//! Compact transaction-inclusion proofs for validated block headers.

use bitcoin::{
    TxMerkleNode, Txid,
    hashes::{Hash, sha256d},
};
use thiserror::Error;

/// Failures while validating a transaction Merkle branch.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum MerkleProofError {
    /// A `u32` transaction position cannot have a branch deeper than 32 levels.
    #[error("transaction Merkle branch exceeds 32 levels")]
    BranchTooDeep,
    /// The branch ended before reaching a tree root for the supplied position.
    #[error("transaction position is outside the supplied Merkle branch")]
    PositionOutOfRange,
    /// The reconstructed root does not match the validated block header.
    #[error("transaction Merkle proof root mismatch")]
    RootMismatch,
}

/// Verifies a transaction's position and sibling branch against a block Merkle root.
///
/// Hashes use Bitcoin's internal byte order. Callers must separately validate the
/// header's proof of work and active-chain membership before treating inclusion as
/// confirmed.
pub fn verify_transaction_merkle_proof(
    txid: Txid,
    position: u32,
    siblings: &[TxMerkleNode],
    expected_root: TxMerkleNode,
) -> Result<(), MerkleProofError> {
    if siblings.len() > u32::BITS as usize {
        return Err(MerkleProofError::BranchTooDeep);
    }
    let mut node = TxMerkleNode::from_byte_array(txid.to_byte_array());
    let mut index = position;
    for sibling in siblings {
        node = if index & 1 == 0 {
            hash_pair(node, *sibling)
        } else {
            hash_pair(*sibling, node)
        };
        index >>= 1;
    }
    if index != 0 {
        return Err(MerkleProofError::PositionOutOfRange);
    }
    if node != expected_root {
        return Err(MerkleProofError::RootMismatch);
    }
    Ok(())
}

fn hash_pair(left: TxMerkleNode, right: TxMerkleNode) -> TxMerkleNode {
    let mut bytes = [0_u8; 64];
    bytes[..32].copy_from_slice(left.as_byte_array());
    bytes[32..].copy_from_slice(right.as_byte_array());
    TxMerkleNode::from_byte_array(sha256d::Hash::hash(&bytes).to_byte_array())
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    fn txid(byte: u8) -> Txid {
        Txid::from_byte_array([byte; 32])
    }

    #[test]
    fn verifies_left_and_right_transaction_positions() {
        let left = txid(1);
        let right = txid(2);
        let left_node = TxMerkleNode::from_byte_array(left.to_byte_array());
        let right_node = TxMerkleNode::from_byte_array(right.to_byte_array());
        let root = hash_pair(left_node, right_node);

        assert_eq!(
            verify_transaction_merkle_proof(left, 0, &[right_node], root),
            Ok(())
        );
        assert_eq!(
            verify_transaction_merkle_proof(right, 1, &[left_node], root),
            Ok(())
        );
    }

    #[test]
    fn rejects_wrong_roots_positions_and_unbounded_branches() {
        let leaf = txid(3);
        let sibling = TxMerkleNode::from_byte_array(txid(4).to_byte_array());
        let wrong_root = TxMerkleNode::all_zeros();
        assert_eq!(
            verify_transaction_merkle_proof(leaf, 0, &[sibling], wrong_root),
            Err(MerkleProofError::RootMismatch)
        );
        assert_eq!(
            verify_transaction_merkle_proof(leaf, 2, &[sibling], wrong_root),
            Err(MerkleProofError::PositionOutOfRange)
        );
        assert_eq!(
            verify_transaction_merkle_proof(leaf, 0, &[sibling; 33], wrong_root),
            Err(MerkleProofError::BranchTooDeep)
        );
    }

    proptest! {
        #[test]
        fn arbitrary_bounded_merkle_branches_never_panic(
            leaf in any::<[u8; 32]>(),
            position in any::<u32>(),
            siblings in proptest::collection::vec(any::<[u8; 32]>(), 0..=32),
            root in any::<[u8; 32]>(),
        ) {
            let siblings = siblings
                .into_iter()
                .map(TxMerkleNode::from_byte_array)
                .collect::<Vec<_>>();
            let _ = verify_transaction_merkle_proof(
                Txid::from_byte_array(leaf),
                position,
                &siblings,
                TxMerkleNode::from_byte_array(root),
            );
        }
    }
}
