//! Differential execution of Bitcoin Core 26's transaction vector corpus.

use std::{collections::HashMap, str::FromStr};

use bitcoin::{
    OutPoint, ScriptBuf, Transaction, Txid,
    consensus::deserialize,
    hex::FromHex,
    opcodes::{Opcode, all::*},
    script::{Builder, PushBytesBuf},
};
use rbtc::{consensus::verify_transaction_scripts_with_flags, utxo::Utxo};
use serde_json::Value;

const VALID_VECTORS: &str = include_str!("data/bitcoin-core-26/tx_valid.json");
const INVALID_VECTORS: &str = include_str!("data/bitcoin-core-26/tx_invalid.json");

fn opcode(name: &str) -> Opcode {
    match name.strip_prefix("OP_").unwrap_or(name) {
        "1ADD" => OP_1ADD,
        "1SUB" => OP_1SUB,
        "ADD" => OP_ADD,
        "CHECKLOCKTIMEVERIFY" => OP_CLTV,
        "CHECKMULTISIG" => OP_CHECKMULTISIG,
        "CHECKSEQUENCEVERIFY" => OP_CSV,
        "CHECKSIG" => OP_CHECKSIG,
        "CHECKSIGVERIFY" => OP_CHECKSIGVERIFY,
        "CODESEPARATOR" => OP_CODESEPARATOR,
        "DROP" => OP_DROP,
        "DUP" => OP_DUP,
        "ENDIF" => OP_ENDIF,
        "EQUAL" => OP_EQUAL,
        "EQUALVERIFY" => OP_EQUALVERIFY,
        "HASH160" => OP_HASH160,
        "IF" => OP_IF,
        "NOT" => OP_NOT,
        "VERIFY" => OP_VERIFY,
        other => panic!("unsupported Core 26 vector opcode: {other}"),
    }
}

fn parse_core_asm(asm: &str) -> ScriptBuf {
    let mut script = Vec::new();
    for word in asm.split_ascii_whitespace() {
        if let Ok(number) = word.parse::<i64>() {
            assert!((-0xffff_ffff..=0xffff_ffff).contains(&number));
            script.extend(Builder::new().push_int(number).into_script().into_bytes());
        } else if let Some(raw) = word.strip_prefix("0x") {
            script.extend(Vec::<u8>::from_hex(raw).expect("Core vector contains valid raw hex"));
        } else if word.starts_with('\'') && word.ends_with('\'') {
            let data = word[1..word.len() - 1].as_bytes().to_vec();
            let data = PushBytesBuf::try_from(data).expect("Core vector push fits script limits");
            script.extend(Builder::new().push_slice(data).into_script().into_bytes());
        } else {
            script.push(opcode(word).to_u8());
        }
    }
    ScriptBuf::from_bytes(script)
}

fn consensus_flag(name: &str) -> Option<u32> {
    match name {
        "NONE" => Some(bitcoinconsensus::VERIFY_NONE),
        "P2SH" => Some(bitcoinconsensus::VERIFY_P2SH),
        "DERSIG" => Some(bitcoinconsensus::VERIFY_DERSIG),
        "NULLDUMMY" => Some(bitcoinconsensus::VERIFY_NULLDUMMY),
        "CHECKLOCKTIMEVERIFY" => Some(bitcoinconsensus::VERIFY_CHECKLOCKTIMEVERIFY),
        "CHECKSEQUENCEVERIFY" => Some(bitcoinconsensus::VERIFY_CHECKSEQUENCEVERIFY),
        "WITNESS" => Some(bitcoinconsensus::VERIFY_WITNESS),
        _ => None,
    }
}

fn valid_flags(excluded: &str) -> u32 {
    let mut flags = bitcoinconsensus::VERIFY_ALL_PRE_TAPROOT;
    for name in excluded.split(',') {
        if let Some(excluded) = consensus_flag(name) {
            flags &= !excluded;
        }
    }
    if flags & bitcoinconsensus::VERIFY_P2SH == 0 {
        flags &= !bitcoinconsensus::VERIFY_WITNESS;
    }
    flags
}

fn invalid_flags(names: &str) -> Option<u32> {
    names
        .split(',')
        .try_fold(0, |flags, name| Some(flags | consensus_flag(name)?))
}

fn vector_rows(json: &str) -> Vec<Value> {
    serde_json::from_str::<Vec<Value>>(json)
        .expect("vendored Core vector JSON")
        .into_iter()
        .filter(|row| {
            row.as_array().is_some_and(|row| {
                row.len() == 3 && row[0].is_array() && row[1].is_string() && row[2].is_string()
            })
        })
        .collect()
}

fn transaction_and_prevouts(row: &[Value]) -> (Transaction, Vec<Utxo>) {
    let inputs = row[0].as_array().expect("filtered vector inputs");
    let mut by_outpoint = HashMap::with_capacity(inputs.len());
    for input in inputs {
        let input = input.as_array().expect("Core vector prevout array");
        assert!((3..=4).contains(&input.len()));
        let txid = Txid::from_str(input[0].as_str().expect("Core vector prevout txid"))
            .expect("Core vector txid hex");
        let vout = input[1]
            .as_i64()
            .and_then(|index| {
                u32::try_from(index)
                    .ok()
                    .or((index == -1).then_some(u32::MAX))
            })
            .expect("Core vector prevout index fits uint32 or is -1");
        let script = parse_core_asm(input[2].as_str().expect("Core vector script asm"));
        let value_sats = input.get(3).map_or(0, |amount| {
            amount.as_u64().expect("Core vector amount is non-negative")
        });
        by_outpoint.insert(
            OutPoint::new(txid, vout),
            Utxo {
                value_sats,
                height: 0,
                is_coinbase: false,
                last_touched: 0,
                creation_mtp: 0,
                script_pubkey: script.into_bytes(),
            },
        );
    }

    let encoded = Vec::<u8>::from_hex(row[1].as_str().expect("Core vector transaction hex"))
        .expect("Core vector transaction encoding");
    let transaction: Transaction = deserialize(&encoded).expect("Core vector transaction");
    let prevouts = transaction
        .input
        .iter()
        .map(|input| {
            by_outpoint
                .get(&input.previous_output)
                .unwrap_or_else(|| panic!("missing Core vector prevout {}", input.previous_output))
                .clone()
        })
        .collect();
    (transaction, prevouts)
}

#[test]
fn core_26_valid_transaction_corpus_matches() {
    let rows = vector_rows(VALID_VECTORS);
    assert_eq!(rows.len(), 119, "unexpected Core valid corpus size");
    for (index, row) in rows.iter().enumerate() {
        let row = row.as_array().expect("filtered vector row");
        let (transaction, prevouts) = transaction_and_prevouts(row);
        let excluded = row[2].as_str().expect("Core vector excluded flags");
        verify_transaction_scripts_with_flags(&transaction, &prevouts, valid_flags(excluded))
            .unwrap_or_else(|error| panic!("Core valid vector {index} failed: {error}"));
    }
}

#[test]
fn core_26_invalid_consensus_flag_corpus_matches() {
    let rows = vector_rows(INVALID_VECTORS);
    assert_eq!(rows.len(), 93, "unexpected Core invalid corpus size");
    let mut executed = 0;
    let mut bad_transaction = 0;
    let mut policy_only = 0;
    for (index, row) in rows.iter().enumerate() {
        let row = row.as_array().expect("filtered vector row");
        let names = row[2].as_str().expect("Core vector flags");
        if names == "BADTX" {
            bad_transaction += 1;
            continue;
        }
        let Some(flags) = invalid_flags(names) else {
            policy_only += 1;
            continue;
        };
        let (transaction, prevouts) = transaction_and_prevouts(row);
        assert!(
            verify_transaction_scripts_with_flags(&transaction, &prevouts, flags).is_err(),
            "Core invalid vector {index} unexpectedly passed with {names}",
        );
        executed += 1;
    }
    assert_eq!(executed, 70);
    assert_eq!(bad_transaction, 9);
    assert_eq!(policy_only, 14);
}
