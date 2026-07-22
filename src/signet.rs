//! BIP325 Signet block-solution validation.
//!
//! Bitcoin Core commits a script solution inside the coinbase witness
//! commitment, then verifies a synthetic transaction against the network's
//! Signet challenge. Transaction and script execution remain delegated to the
//! pinned `bitcoinconsensus` library.

use std::{io::Cursor, iter};

use bitcoin::{
    Amount, Block, Network, OutPoint, ScriptBuf, Sequence, Transaction, TxIn, TxOut, Witness,
    absolute::LockTime,
    consensus::{Decodable, Encodable},
    transaction::Version,
};
use thiserror::Error;

use crate::{
    consensus::{ConsensusError, verify_transaction_scripts_with_flags},
    utxo::Utxo,
};

const SIGNET_HEADER: [u8; 4] = [0xec, 0xc7, 0xda, 0xa2];
const WITNESS_COMMITMENT_HEADER: [u8; 6] = [0x6a, 0x24, 0xaa, 0x21, 0xa9, 0xed];

/// Bitcoin Core 26's default global Signet challenge.
pub const DEFAULT_SIGNET_CHALLENGE: &[u8] = &[
    0x51, 0x21, 0x03, 0xad, 0x5e, 0x0e, 0xda, 0xd1, 0x8c, 0xb1, 0xf0, 0xfc, 0x0d, 0x28, 0xa3, 0xd4,
    0xf1, 0xf3, 0xe4, 0x45, 0x64, 0x03, 0x37, 0x48, 0x9a, 0xbb, 0x10, 0x40, 0x4f, 0x2d, 0x1e, 0x08,
    0x6b, 0xe4, 0x30, 0x21, 0x03, 0x59, 0xef, 0x50, 0x21, 0x96, 0x4f, 0xe2, 0x2d, 0x6f, 0x8e, 0x05,
    0xb2, 0x46, 0x3c, 0x95, 0x40, 0xce, 0x96, 0x88, 0x3f, 0xe3, 0xb2, 0x78, 0x76, 0x0f, 0x04, 0x8f,
    0x51, 0x89, 0xf2, 0xe6, 0xc4, 0x52, 0xae,
];

/// Signet commitment parsing or challenge-script failure.
#[derive(Debug, Error)]
pub enum SignetError {
    /// A non-genesis Signet block did not contain a witness commitment output.
    #[error("Signet block has no witness commitment")]
    MissingWitnessCommitment,
    /// The embedded scriptSig/witness solution was not canonically encoded.
    #[error("malformed Signet block solution")]
    MalformedSolution,
    /// Bitcoin Core's script engine rejected the reconstructed solution.
    #[error("invalid Signet challenge solution: {0}")]
    Script(#[from] ConsensusError),
}

/// Validates a block against Bitcoin's default global Signet challenge.
pub fn validate_default_signet_block_solution(block: &Block) -> Result<(), SignetError> {
    if block.block_hash()
        == bitcoin::blockdata::constants::genesis_block(Network::Signet).block_hash()
    {
        return Ok(());
    }
    validate_signet_block_solution(block, DEFAULT_SIGNET_CHALLENGE)
}

/// Validates a non-genesis block against an explicit BIP325 challenge script.
pub fn validate_signet_block_solution(block: &Block, challenge: &[u8]) -> Result<(), SignetError> {
    let commitment_index = block
        .txdata
        .first()
        .and_then(|coinbase| {
            coinbase.output.iter().rposition(|output| {
                output.script_pubkey.len() >= 38
                    && output.script_pubkey.as_bytes()[..6] == WITNESS_COMMITMENT_HEADER
            })
        })
        .ok_or(SignetError::MissingWitnessCommitment)?;
    let mut modified_coinbase = block.txdata[0].clone();
    let (replacement, solution) = extract_solution(
        modified_coinbase.output[commitment_index]
            .script_pubkey
            .as_bytes(),
    );
    modified_coinbase.output[commitment_index].script_pubkey = ScriptBuf::from_bytes(replacement);
    let (script_sig, witness) = solution.map_or_else(
        || Ok((ScriptBuf::new(), Witness::new())),
        |bytes| decode_solution(&bytes),
    )?;

    let signet_merkle = bitcoin::merkle_tree::calculate_root(
        iter::once(modified_coinbase.compute_txid().to_raw_hash()).chain(
            block.txdata[1..]
                .iter()
                .map(|transaction| transaction.compute_txid().to_raw_hash()),
        ),
    )
    .expect("a Signet solution always has a coinbase");
    let mut block_data = Vec::with_capacity(72);
    block
        .header
        .version
        .to_consensus()
        .consensus_encode(&mut block_data)
        .expect("vectors do not fail");
    block
        .header
        .prev_blockhash
        .consensus_encode(&mut block_data)
        .expect("vectors do not fail");
    signet_merkle
        .consensus_encode(&mut block_data)
        .expect("vectors do not fail");
    block
        .header
        .time
        .consensus_encode(&mut block_data)
        .expect("vectors do not fail");

    let mut to_spend_script = vec![0x00];
    append_push(&mut to_spend_script, &block_data);
    let to_spend = Transaction {
        version: Version(0),
        lock_time: LockTime::ZERO,
        input: vec![TxIn {
            previous_output: OutPoint::null(),
            script_sig: ScriptBuf::from_bytes(to_spend_script),
            sequence: Sequence::from_consensus(0),
            witness: Witness::new(),
        }],
        output: vec![TxOut {
            value: Amount::ZERO,
            script_pubkey: ScriptBuf::from_bytes(challenge.to_vec()),
        }],
    };
    let to_sign = Transaction {
        version: Version(0),
        lock_time: LockTime::ZERO,
        input: vec![TxIn {
            previous_output: OutPoint::new(to_spend.compute_txid(), 0),
            script_sig,
            sequence: Sequence::from_consensus(0),
            witness,
        }],
        output: vec![TxOut {
            value: Amount::ZERO,
            script_pubkey: ScriptBuf::from_bytes(vec![0x6a]),
        }],
    };
    let prevout = Utxo {
        value_sats: 0,
        height: 0,
        is_coinbase: false,
        last_touched: 0,
        creation_mtp: 0,
        script_pubkey: challenge.to_vec(),
    };
    let flags = bitcoinconsensus::VERIFY_P2SH
        | bitcoinconsensus::VERIFY_WITNESS
        | bitcoinconsensus::VERIFY_DERSIG
        | bitcoinconsensus::VERIFY_NULLDUMMY;
    verify_transaction_scripts_with_flags(&to_sign, &[prevout], flags)?;
    Ok(())
}

fn decode_solution(bytes: &[u8]) -> Result<(ScriptBuf, Witness), SignetError> {
    let mut cursor = Cursor::new(bytes);
    let script_sig =
        ScriptBuf::consensus_decode(&mut cursor).map_err(|_| SignetError::MalformedSolution)?;
    let witness =
        Witness::consensus_decode(&mut cursor).map_err(|_| SignetError::MalformedSolution)?;
    if usize::try_from(cursor.position()).ok() != Some(bytes.len()) {
        return Err(SignetError::MalformedSolution);
    }
    Ok((script_sig, witness))
}

fn extract_solution(script: &[u8]) -> (Vec<u8>, Option<Vec<u8>>) {
    let mut replacement = Vec::with_capacity(script.len());
    let mut solution = None;
    let mut cursor = 0;
    while cursor < script.len() {
        let Some((opcode, mut pushdata)) = read_script_op(script, &mut cursor) else {
            break;
        };
        if pushdata.is_empty() {
            replacement.push(opcode);
        } else {
            if solution.is_none()
                && pushdata.len() > SIGNET_HEADER.len()
                && pushdata.starts_with(&SIGNET_HEADER)
            {
                solution = Some(pushdata.split_off(SIGNET_HEADER.len()));
            }
            append_push(&mut replacement, &pushdata);
        }
    }
    if solution.is_some() {
        (replacement, solution)
    } else {
        (script.to_vec(), None)
    }
}

fn read_script_op(script: &[u8], cursor: &mut usize) -> Option<(u8, Vec<u8>)> {
    let opcode = *script.get(*cursor)?;
    *cursor += 1;
    let length = match opcode {
        0x00..=0x4b => usize::from(opcode),
        0x4c => usize::from(*script.get(*cursor).inspect(|_| *cursor += 1)?),
        0x4d => {
            let bytes: [u8; 2] = script
                .get(*cursor..cursor.checked_add(2)?)?
                .try_into()
                .ok()?;
            *cursor += 2;
            usize::from(u16::from_le_bytes(bytes))
        }
        0x4e => {
            let bytes: [u8; 4] = script
                .get(*cursor..cursor.checked_add(4)?)?
                .try_into()
                .ok()?;
            *cursor += 4;
            usize::try_from(u32::from_le_bytes(bytes)).ok()?
        }
        _ => return Some((opcode, Vec::new())),
    };
    let end = cursor.checked_add(length)?;
    let data = script.get(*cursor..end)?.to_vec();
    *cursor = end;
    Some((opcode, data))
}

fn append_push(script: &mut Vec<u8>, data: &[u8]) {
    match data.len() {
        0..=75 => script.push(u8::try_from(data.len()).expect("small push length")),
        76..=255 => {
            script.push(0x4c);
            script.push(u8::try_from(data.len()).expect("PUSHDATA1 length"));
        }
        256..=65_535 => {
            script.push(0x4d);
            script.extend_from_slice(
                &u16::try_from(data.len())
                    .expect("PUSHDATA2 length")
                    .to_le_bytes(),
            );
        }
        _ => {
            script.push(0x4e);
            script.extend_from_slice(
                &u32::try_from(data.len())
                    .expect("Bitcoin scripts fit PUSHDATA4")
                    .to_le_bytes(),
            );
        }
    }
    script.extend_from_slice(data);
}

#[cfg(test)]
mod tests {
    use bitcoin::{
        TxMerkleNode,
        block::{Header, Version as HeaderVersion},
        consensus::deserialize,
        hashes::Hash,
        hex::FromHex,
        pow::CompactTarget,
    };
    use proptest::prelude::*;

    use super::*;

    const SIGNET_BLOCK_ONE: &str = include_str!("../tests/data/bitcoin-core-26/signet-block-1.hex");

    fn custom_block(commitment_script: Vec<u8>) -> Block {
        let coinbase = Transaction {
            version: Version::ONE,
            lock_time: LockTime::ZERO,
            input: vec![TxIn {
                previous_output: OutPoint::null(),
                script_sig: ScriptBuf::from_bytes(vec![0x01, 0x01]),
                sequence: Sequence::MAX,
                witness: Witness::new(),
            }],
            output: vec![TxOut {
                value: Amount::ZERO,
                script_pubkey: ScriptBuf::from_bytes(commitment_script),
            }],
        };
        Block {
            header: Header {
                version: HeaderVersion::ONE,
                prev_blockhash: bitcoin::BlockHash::all_zeros(),
                merkle_root: TxMerkleNode::all_zeros(),
                time: 1,
                bits: CompactTarget::from_consensus(0),
                nonce: 0,
            },
            txdata: vec![coinbase],
        }
    }

    #[test]
    fn validates_real_default_signet_block_and_rejects_damaged_solution() {
        let bytes = Vec::<u8>::from_hex(SIGNET_BLOCK_ONE.trim()).unwrap();
        let block: Block = deserialize(&bytes).unwrap();
        validate_default_signet_block_solution(&block).unwrap();

        let mut damaged = block;
        let script = damaged.txdata[0].output[1].script_pubkey.as_mut_bytes();
        let header = script
            .windows(SIGNET_HEADER.len())
            .position(|window| window == SIGNET_HEADER)
            .unwrap();
        script[header + SIGNET_HEADER.len() + 8] ^= 1;
        assert!(matches!(
            validate_default_signet_block_solution(&damaged),
            Err(SignetError::Script(_))
        ));
    }

    #[test]
    fn custom_true_challenge_needs_commitment_but_no_solution() {
        validate_default_signet_block_solution(&bitcoin::blockdata::constants::genesis_block(
            Network::Signet,
        ))
        .unwrap();

        let mut commitment = WITNESS_COMMITMENT_HEADER.to_vec();
        commitment.extend_from_slice(&[0; 32]);
        let block = custom_block(commitment);
        validate_signet_block_solution(&block, &[0x51]).unwrap();
        assert!(matches!(
            validate_signet_block_solution(&block, &[0x00]),
            Err(SignetError::Script(_))
        ));

        let missing = custom_block(vec![0x6a]);
        assert!(matches!(
            validate_signet_block_solution(&missing, &[0x51]),
            Err(SignetError::MissingWitnessCommitment)
        ));
    }

    #[test]
    fn rejects_malformed_embedded_solution() {
        let mut commitment = WITNESS_COMMITMENT_HEADER.to_vec();
        commitment.extend_from_slice(&[0; 32]);
        let mut embedded = SIGNET_HEADER.to_vec();
        embedded.extend_from_slice(&[3, 1]);
        append_push(&mut commitment, &embedded);
        assert!(matches!(
            validate_signet_block_solution(&custom_block(commitment), &[0x51]),
            Err(SignetError::MalformedSolution)
        ));
    }

    #[test]
    fn commitment_without_signet_section_is_not_normalized() {
        let non_minimal_push = vec![0x6a, 0x4c, 0x01, 0x01];
        assert_eq!(
            extract_solution(&non_minimal_push),
            (non_minimal_push, None)
        );
    }

    proptest! {
        #[test]
        fn extracting_arbitrary_bounded_scripts_never_panics(
            script in proptest::collection::vec(any::<u8>(), 0..=4096)
        ) {
            let _ = extract_solution(&script);
        }

        #[test]
        fn signet_section_extraction_preserves_payload(payload in proptest::collection::vec(any::<u8>(), 1..=512)) {
            let mut section = SIGNET_HEADER.to_vec();
            section.extend_from_slice(&payload);
            let mut script = vec![0x6a];
            append_push(&mut script, &section);
            let (_, extracted) = extract_solution(&script);
            prop_assert_eq!(extracted, Some(payload));
        }
    }
}
