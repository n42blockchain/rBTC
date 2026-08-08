//! Minimal local block assembly for low-difficulty networks.
//!
//! This module builds consensus-valid blocks on networks whose proof-of-work
//! target is reachable by a bounded nonce search — regtest above all — so
//! differential, interoperability, and pipeline tests can produce blocks
//! without an external daemon. It is deliberately not a mining template
//! provider: there is no fee ordering, no policy selection, no timestamp
//! management, and no support for grinding a competitive public-network
//! target. The coinbase height uses the exact encoding the validator's BIP34
//! check compares against, and every assembled block carries the segwit
//! witness commitment with the reserved coinbase witness, so the module
//! assumes a chain context where segwit is active — true from genesis on
//! regtest.

use bitcoin::{
    Amount, Block, BlockHash, ScriptBuf, Sequence, Transaction, TxIn, TxMerkleNode, TxOut, Witness,
    absolute::LockTime, block::Header, block::Version as HeaderVersion, hashes::Hash, pow::Target,
    transaction::OutPoint, transaction::Version as TransactionVersion,
};
use thiserror::Error;

use crate::blockchain::{block_subsidy_with_interval, encode_script_num_push};

/// Leading bytes of the BIP141 witness-commitment output script.
const WITNESS_COMMITMENT_PREFIX: [u8; 6] = [0x6a, 0x24, 0xaa, 0x21, 0xa9, 0xed];

/// Errors from local block assembly.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum BlockAssemblyError {
    /// No nonce satisfied the target within one 32-bit search.
    #[error("no nonce satisfies the target within one 32-bit search")]
    NonceSpaceExhausted,
    /// A template transaction is itself a coinbase.
    #[error("assembled transactions must not include a coinbase")]
    UnexpectedCoinbase,
}

/// Inputs for one locally assembled block.
pub struct BlockTemplate {
    /// Parent block hash the new block extends.
    pub parent: BlockHash,
    /// Height of the assembled block.
    pub height: u32,
    /// Header timestamp; the caller owns median-time-past validity.
    pub time: u32,
    /// Proof-of-work target the nonce search must satisfy.
    pub target: Target,
    /// Header version.
    pub version: i32,
    /// Output script receiving the coinbase value.
    pub coinbase_script_pubkey: ScriptBuf,
    /// Network subsidy halving interval.
    pub subsidy_halving_interval: u32,
    /// Total fees paid by `transactions`, added to the coinbase value.
    pub fee_sats: u64,
    /// Non-coinbase transactions in final block order; consensus validity of
    /// their spends remains the caller's responsibility.
    pub transactions: Vec<Transaction>,
}

impl BlockTemplate {
    /// Returns a coinbase-only regtest template extending `parent`.
    #[must_use]
    pub fn regtest(parent: BlockHash, height: u32, time: u32) -> Self {
        Self {
            parent,
            height,
            time,
            target: Target::MAX_ATTAINABLE_REGTEST,
            version: 4,
            coinbase_script_pubkey: ScriptBuf::new(),
            subsidy_halving_interval: crate::deployments::halving_interval(
                bitcoin::Network::Regtest,
            ),
            fee_sats: 0,
            transactions: Vec::new(),
        }
    }
}

/// Assembles and proof-of-work-grinds one block from `template`.
///
/// The coinbase commits the BIP34 height with the validator's exact
/// encoding, pads the script signature into the 2-to-100-byte consensus
/// range, carries the reserved segwit witness plus the matching witness
/// commitment over all template transactions, and pays the height subsidy
/// plus the declared fees to the template's output script.
///
/// # Errors
///
/// Returns an error when a template transaction is a coinbase or when no
/// nonce in the 32-bit search space satisfies the target.
pub fn assemble_block(template: &BlockTemplate) -> Result<Block, BlockAssemblyError> {
    if template.transactions.iter().any(Transaction::is_coinbase) {
        return Err(BlockAssemblyError::UnexpectedCoinbase);
    }
    let mut script_sig = encode_script_num_push(template.height);
    script_sig.push(0);
    let coinbase = Transaction {
        version: TransactionVersion::ONE,
        lock_time: LockTime::ZERO,
        input: vec![TxIn {
            previous_output: OutPoint::null(),
            script_sig: ScriptBuf::from_bytes(script_sig),
            sequence: Sequence::MAX,
            witness: Witness::from_slice(&[[0u8; 32]]),
        }],
        output: vec![TxOut {
            value: Amount::from_sat(
                block_subsidy_with_interval(template.height, template.subsidy_halving_interval)
                    .saturating_add(template.fee_sats),
            ),
            script_pubkey: template.coinbase_script_pubkey.clone(),
        }],
    };
    let mut txdata = Vec::with_capacity(1 + template.transactions.len());
    txdata.push(coinbase);
    txdata.extend(template.transactions.iter().cloned());
    let mut block = Block {
        header: Header {
            version: HeaderVersion::from_consensus(template.version),
            prev_blockhash: template.parent,
            merkle_root: TxMerkleNode::all_zeros(),
            time: template.time,
            bits: template.target.to_compact_lossy(),
            nonce: 0,
        },
        txdata,
    };
    let witness_root = block
        .witness_root()
        .expect("an assembled block always has a coinbase");
    let commitment = Block::compute_witness_commitment(&witness_root, &[0u8; 32]);
    block.txdata[0].output.push(TxOut {
        value: Amount::ZERO,
        script_pubkey: ScriptBuf::from_bytes(
            WITNESS_COMMITMENT_PREFIX
                .iter()
                .copied()
                .chain(commitment.to_byte_array())
                .collect(),
        ),
    });
    block.header.merkle_root = block
        .compute_merkle_root()
        .expect("an assembled block always has transactions");
    let mut nonce: u32 = 0;
    loop {
        block.header.nonce = nonce;
        if block.header.validate_pow(template.target).is_ok() {
            return Ok(block);
        }
        nonce = match nonce.checked_add(1) {
            Some(next) => next,
            None => return Err(BlockAssemblyError::NonceSpaceExhausted),
        };
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::blockchain::validate_block_structure_with_deployments;
    use bitcoin::Network;

    fn regtest_genesis() -> BlockHash {
        bitcoin::blockdata::constants::genesis_block(Network::Regtest).block_hash()
    }

    fn structure_is_valid(block: &Block, height: u32) {
        validate_block_structure_with_deployments(block, height, true, true, None)
            .expect("assembled block passes the validator's structure checks");
    }

    #[test]
    fn assembles_consensus_valid_coinbase_only_blocks() {
        let mut parent = regtest_genesis();
        for height in 1..=3u32 {
            let block = assemble_block(&BlockTemplate::regtest(
                parent,
                height,
                1_296_688_602 + height * 600,
            ))
            .expect("regtest block assembles");
            assert_eq!(block.header.prev_blockhash, parent);
            structure_is_valid(&block, height);
            assert_eq!(
                block.txdata[0].output[0].value.to_sat(),
                block_subsidy_with_interval(height, 150),
            );
            block
                .header
                .validate_pow(Target::MAX_ATTAINABLE_REGTEST)
                .expect("ground nonce satisfies the regtest target");
            parent = block.block_hash();
        }
    }

    #[test]
    fn bip34_height_encodings_match_the_validator() {
        for height in [1u32, 2, 16, 17, 255, 500_000] {
            let block = assemble_block(&BlockTemplate::regtest(
                regtest_genesis(),
                height,
                1_296_688_602,
            ))
            .expect("regtest block assembles");
            structure_is_valid(&block, height);
        }
    }

    #[test]
    fn commits_to_witness_data_of_included_transactions() {
        let witness_spend = Transaction {
            version: TransactionVersion::TWO,
            lock_time: LockTime::ZERO,
            input: vec![TxIn {
                previous_output: OutPoint {
                    txid: bitcoin::Txid::all_zeros(),
                    vout: 1,
                },
                script_sig: ScriptBuf::new(),
                sequence: Sequence::MAX,
                witness: Witness::from_slice(&[[0x42u8; 8]]),
            }],
            output: vec![TxOut {
                value: Amount::from_sat(1),
                script_pubkey: ScriptBuf::new(),
            }],
        };
        let mut template = BlockTemplate::regtest(regtest_genesis(), 2, 1_296_689_202);
        template.fee_sats = 7;
        template.transactions = vec![witness_spend];
        let block = assemble_block(&template).expect("witness block assembles");
        structure_is_valid(&block, 2);
        assert_eq!(
            block.txdata[0].output[0].value.to_sat(),
            block_subsidy_with_interval(2, 150) + 7,
        );
        let commitment_output = &block.txdata[0].output[1];
        assert!(
            commitment_output.script_pubkey.as_bytes()[..6] == WITNESS_COMMITMENT_PREFIX,
            "the coinbase must carry a witness commitment output"
        );
    }

    #[test]
    fn rejects_a_template_containing_a_coinbase() {
        let mut template = BlockTemplate::regtest(regtest_genesis(), 1, 1_296_688_602);
        let stray_coinbase =
            assemble_block(&BlockTemplate::regtest(regtest_genesis(), 1, 1_296_688_602))
                .expect("assembles")
                .txdata
                .remove(0);
        template.transactions = vec![stray_coinbase];
        assert_eq!(
            assemble_block(&template),
            Err(BlockAssemblyError::UnexpectedCoinbase)
        );
    }
}
