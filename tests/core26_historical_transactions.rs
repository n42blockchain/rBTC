//! Authenticated real-mainnet transaction checks at CSV, SegWit, and Taproot boundaries.

use std::{collections::HashMap, str::FromStr};

use bitcoin::{
    Network, OutPoint, Transaction, TxMerkleNode, Txid, Witness, block::Header,
    consensus::deserialize, hashes::Hash, hex::FromHex,
};
use rbtc::{
    chainstate::{ChainstateError, apply_transaction_with_context},
    consensus::verify_transaction_scripts_with_flags,
    deployments::block_deployment_context,
    merkle_proof::{MerkleProofError, verify_transaction_merkle_proof},
    utxo::{OutPointKey, RedbUtxoStore, Utxo, UtxoStore},
};
use serde::Deserialize;
use tempfile::TempDir;

const FIXTURES: &str =
    include_str!("data/bitcoin-core-26/authenticated-historical-transactions.json");

#[derive(Clone, Debug, Deserialize)]
struct HistoricalTransaction {
    name: String,
    height: u32,
    block_hash: String,
    block_header: String,
    position: u32,
    merkle: Vec<String>,
    parent_mtp: u32,
    transaction: String,
    prev_transactions: Vec<HistoricalPrevTransaction>,
}

#[derive(Clone, Debug, Deserialize)]
struct HistoricalPrevTransaction {
    transaction: String,
    height: u32,
    creation_mtp: u32,
    block_hash: String,
    block_header: String,
    position: u32,
    merkle: Vec<String>,
}

struct MaterializedFixture {
    fixture: HistoricalTransaction,
    header: Header,
    transaction: Transaction,
    prevouts: Vec<Utxo>,
    entries: Vec<(OutPointKey, Utxo)>,
}

fn fixtures() -> Vec<HistoricalTransaction> {
    serde_json::from_str(FIXTURES).expect("historical transaction fixtures")
}

fn authenticated_header(
    txid: Txid,
    block_hash: &str,
    encoded_header: &str,
    position: u32,
    encoded_siblings: &[String],
) -> Header {
    let header: Header =
        deserialize(&Vec::<u8>::from_hex(encoded_header).expect("block header hex"))
            .expect("block header");
    assert_eq!(header.block_hash().to_string(), block_hash);
    header
        .validate_pow(header.target())
        .expect("historical header satisfies its claimed proof of work");
    let siblings = encoded_siblings
        .iter()
        .map(|hash| TxMerkleNode::from_str(hash).expect("Merkle sibling"))
        .collect::<Vec<_>>();
    verify_transaction_merkle_proof(txid, position, &siblings, header.merkle_root)
        .expect("transaction inclusion proof");
    header
}

fn materialize(name: &str) -> MaterializedFixture {
    let fixture = fixtures()
        .into_iter()
        .find(|fixture| fixture.name == name)
        .unwrap_or_else(|| panic!("missing historical fixture {name}"));
    let transaction: Transaction =
        deserialize(&Vec::<u8>::from_hex(&fixture.transaction).expect("transaction hex"))
            .expect("transaction");
    let header = authenticated_header(
        transaction.compute_txid(),
        &fixture.block_hash,
        &fixture.block_header,
        fixture.position,
        &fixture.merkle,
    );

    let previous = fixture
        .prev_transactions
        .iter()
        .map(|previous| {
            let transaction: Transaction = deserialize(
                &Vec::<u8>::from_hex(&previous.transaction).expect("previous transaction hex"),
            )
            .expect("previous transaction");
            authenticated_header(
                transaction.compute_txid(),
                &previous.block_hash,
                &previous.block_header,
                previous.position,
                &previous.merkle,
            );
            (transaction.compute_txid(), (transaction, previous))
        })
        .collect::<HashMap<_, _>>();
    let mut prevouts = Vec::with_capacity(transaction.input.len());
    let mut entries = Vec::with_capacity(transaction.input.len());
    for input in &transaction.input {
        let (previous_transaction, metadata) = previous
            .get(&input.previous_output.txid)
            .expect("fixture contains every previous transaction");
        let output = previous_transaction
            .output
            .get(usize::try_from(input.previous_output.vout).expect("vout fits usize"))
            .expect("previous output exists");
        let utxo = Utxo {
            value_sats: output.value.to_sat(),
            height: metadata.height,
            is_coinbase: previous_transaction.is_coinbase(),
            last_touched: u64::from(header.time),
            creation_mtp: metadata.creation_mtp,
            script_pubkey: output.script_pubkey.as_bytes().to_vec(),
        };
        prevouts.push(utxo.clone());
        entries.push((input.previous_output.into(), utxo));
    }
    assert_eq!(previous.len(), transaction.input.len());
    MaterializedFixture {
        fixture,
        header,
        transaction,
        prevouts,
        entries,
    }
}

fn store_with_entries(entries: &[(OutPointKey, Utxo)]) -> (TempDir, RedbUtxoStore) {
    let directory = TempDir::new().unwrap();
    let store = RedbUtxoStore::open(directory.path().join("chainstate.redb")).unwrap();
    store.apply(&[], entries).unwrap();
    (directory, store)
}

fn context(materialized: &MaterializedFixture) -> rbtc::block_execution::BlockDeploymentContext {
    block_deployment_context(
        Network::Bitcoin,
        materialized.fixture.height,
        materialized.header.block_hash(),
        materialized.header.time,
        true,
    )
}

fn execute(
    store: &RedbUtxoStore,
    materialized: &MaterializedFixture,
    transaction: &Transaction,
) -> Result<(), ChainstateError> {
    let context = context(materialized);
    apply_transaction_with_context(
        store,
        transaction,
        materialized.fixture.height,
        u64::from(materialized.header.time),
        materialized.fixture.parent_mtp,
        materialized.fixture.parent_mtp,
        context.script_flags,
        context.csv_active,
    )
    .map(|_| ())
}

fn damaged_witness(transaction: &Transaction) -> Transaction {
    let mut damaged = transaction.clone();
    let mut witness = damaged.input[0]
        .witness
        .iter()
        .map(<[u8]>::to_vec)
        .collect::<Vec<_>>();
    assert!(!witness.is_empty());
    witness[0][0] ^= 1;
    damaged.input[0].witness = Witness::from_slice(&witness);
    damaged
}

#[test]
fn real_transactions_and_prevouts_are_authenticated_by_pow_merkle_and_txids() {
    for name in [
        "csv-height-boundary",
        "segwit-activation",
        "first-taproot-keypath",
    ] {
        let materialized = materialize(name);
        let mut wrong_txid = materialized.transaction.compute_txid().to_byte_array();
        wrong_txid[0] ^= 1;
        let siblings = materialized
            .fixture
            .merkle
            .iter()
            .map(|hash| TxMerkleNode::from_str(hash).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            verify_transaction_merkle_proof(
                Txid::from_byte_array(wrong_txid),
                materialized.fixture.position,
                &siblings,
                materialized.header.merkle_root,
            ),
            Err(MerkleProofError::RootMismatch),
            "{}",
            materialized.fixture.name
        );
    }
}

#[test]
fn real_csv_transaction_passes_at_its_exact_144_block_relative_height() {
    let materialized = materialize("csv-height-boundary");
    assert_eq!(materialized.fixture.height, 709_632);
    assert_eq!(materialized.entries[0].1.height, 709_488);
    assert_eq!(
        materialized.transaction.input[0]
            .sequence
            .to_consensus_u32(),
        144
    );

    let (directory, store) = store_with_entries(&materialized.entries);
    execute(&store, &materialized, &materialized.transaction).unwrap();
    assert!(
        store
            .get(materialized.transaction.input[0].previous_output.into())
            .unwrap()
            .is_none()
    );
    let created = OutPoint::new(materialized.transaction.compute_txid(), 0);
    assert!(store.get(created.into()).unwrap().is_some());
    drop(store);
    let reopened = RedbUtxoStore::open(directory.path().join("chainstate.redb")).unwrap();
    assert!(reopened.get(created.into()).unwrap().is_some());

    let mut too_young = materialized.entries.clone();
    too_young[0].1.height += 1;
    let (_directory, store) = store_with_entries(&too_young);
    assert!(matches!(
        execute(&store, &materialized, &materialized.transaction),
        Err(ChainstateError::RelativeHeightLock { .. })
    ));
    assert!(store.get(too_young[0].0).unwrap().is_some());
}

#[test]
fn real_segwit_activation_spend_enforces_witness_and_rejects_without_residue() {
    let materialized = materialize("segwit-activation");
    assert_eq!(materialized.fixture.height, 481_824);
    assert!(context(&materialized).segwit_active);
    let damaged = damaged_witness(&materialized.transaction);
    let (directory, store) = store_with_entries(&materialized.entries);
    assert!(matches!(
        execute(&store, &materialized, &damaged),
        Err(ChainstateError::Script(_))
    ));
    let spent = materialized.transaction.input[0].previous_output.into();
    assert!(store.get(spent).unwrap().is_some());
    drop(store);
    let reopened = RedbUtxoStore::open(directory.path().join("chainstate.redb")).unwrap();
    assert!(reopened.get(spent).unwrap().is_some());

    let flags_without_witness = context(&materialized).script_flags
        & !bitcoinconsensus::VERIFY_WITNESS
        & !bitcoinconsensus::VERIFY_TAPROOT;
    verify_transaction_scripts_with_flags(&damaged, &materialized.prevouts, flags_without_witness)
        .expect("a v0 witness program was anyone-can-spend without the witness flag");
}

#[test]
fn first_real_taproot_keypath_spend_enforces_schnorr_signature() {
    let materialized = materialize("first-taproot-keypath");
    assert_eq!(materialized.fixture.height, 709_635);
    assert_eq!(
        materialized.prevouts[0].script_pubkey.first(),
        Some(&bitcoin::opcodes::all::OP_PUSHNUM_1.to_u8())
    );
    let (_directory, store) = store_with_entries(&materialized.entries);
    execute(&store, &materialized, &materialized.transaction).unwrap();

    let damaged = damaged_witness(&materialized.transaction);
    let (_directory, store) = store_with_entries(&materialized.entries);
    assert!(matches!(
        execute(&store, &materialized, &damaged),
        Err(ChainstateError::Script(_))
    ));
    assert!(store.get(materialized.entries[0].0).unwrap().is_some());
    verify_transaction_scripts_with_flags(
        &damaged,
        &materialized.prevouts,
        context(&materialized).script_flags & !bitcoinconsensus::VERIFY_TAPROOT,
    )
    .expect("a v1 witness program was anyone-can-spend without the Taproot flag");
}
