//! Complete mainnet activation-block execution against compact external UTXO views.

use std::{
    collections::{BTreeMap, BTreeSet},
    str::FromStr,
    sync::Mutex,
};

use bitcoin::{Block, Network, Txid, consensus::deserialize, hex::FromHex};
use rbtc::{
    blockchain::{BlockError, apply_block_with_deployments},
    chainstate::{COINBASE_MATURITY, ChainstateError},
    deployments::block_deployment_context,
    utxo::{OutPointKey, RedbUtxoStore, TierStats, Utxo, UtxoError, UtxoStore, UtxoUndo},
};
use serde::Deserialize;
use tempfile::TempDir;

const BIP65_BLOCK: &[u8] = include_bytes!(
    "data/bitcoin-core-26/mainnet-000000000000000004c2b624ed5d7756c508d90fd0da2c7c679febfa6c4735f0.zst"
);
const BIP65_UTXOS: &[u8] = include_bytes!(
    "data/bitcoin-core-26/mainnet-000000000000000004c2b624ed5d7756c508d90fd0da2c7c679febfa6c4735f0.utxos.json.zst"
);
const CSV_BLOCK: &[u8] = include_bytes!(
    "data/bitcoin-core-26/mainnet-000000000000000004a1b34462cb8aeebd5799177f7a29cf28f2d1961716b5b5.zst"
);
const CSV_UTXOS: &[u8] = include_bytes!(
    "data/bitcoin-core-26/mainnet-000000000000000004a1b34462cb8aeebd5799177f7a29cf28f2d1961716b5b5.utxos.json.zst"
);
const SEGWIT_BLOCK: &[u8] = include_bytes!(
    "data/bitcoin-core-26/mainnet-0000000000000000001c8018d9cb3b742ef25114f27563e3fc4a1902167f9893.zst"
);
const SEGWIT_UTXOS: &[u8] = include_bytes!(
    "data/bitcoin-core-26/mainnet-0000000000000000001c8018d9cb3b742ef25114f27563e3fc4a1902167f9893.utxos.json.zst"
);
const TAPROOT_BLOCK: &[u8] = include_bytes!(
    "data/bitcoin-core-26/mainnet-0000000000000000000687bca986194dc2c1f949318629b44bb54ec0a94d8244.zst"
);
const TAPROOT_UTXOS: &[u8] = include_bytes!(
    "data/bitcoin-core-26/mainnet-0000000000000000000687bca986194dc2c1f949318629b44bb54ec0a94d8244.utxos.json.zst"
);
const TAPROOT_EXCEPTION_BLOCK: &[u8] = include_bytes!(
    "data/bitcoin-core-26/mainnet-0000000000000000000f14c35b2d841e986ab5441de8c585d5ffe55ea1e395ad.zst"
);
const TAPROOT_EXCEPTION_UTXOS: &[u8] = include_bytes!(
    "data/bitcoin-core-26/mainnet-0000000000000000000f14c35b2d841e986ab5441de8c585d5ffe55ea1e395ad.utxos.json.zst"
);

struct ActivationCase {
    name: &'static str,
    height: u32,
    hash: &'static str,
    parent_mtp: u32,
    block: &'static [u8],
    utxos: &'static [u8],
    expected_utxos: usize,
    expected_coinbase_origins: usize,
}

fn cases() -> [ActivationCase; 5] {
    [
        ActivationCase {
            name: "BIP65",
            height: 388_381,
            hash: "000000000000000004c2b624ed5d7756c508d90fd0da2c7c679febfa6c4735f0",
            parent_mtp: 1_450_112_079,
            block: BIP65_BLOCK,
            utxos: BIP65_UTXOS,
            expected_utxos: 1_576,
            expected_coinbase_origins: 5,
        },
        ActivationCase {
            name: "CSV",
            height: 419_328,
            hash: "000000000000000004a1b34462cb8aeebd5799177f7a29cf28f2d1961716b5b5",
            parent_mtp: 1_467_667_249,
            block: CSV_BLOCK,
            utxos: CSV_UTXOS,
            expected_utxos: 4_398,
            expected_coinbase_origins: 0,
        },
        ActivationCase {
            name: "SegWit",
            height: 481_824,
            hash: "0000000000000000001c8018d9cb3b742ef25114f27563e3fc4a1902167f9893",
            parent_mtp: 1_503_536_364,
            block: SEGWIT_BLOCK,
            utxos: SEGWIT_UTXOS,
            expected_utxos: 4_897,
            expected_coinbase_origins: 2,
        },
        ActivationCase {
            name: "Taproot",
            height: 709_632,
            hash: "0000000000000000000687bca986194dc2c1f949318629b44bb54ec0a94d8244",
            parent_mtp: 1_636_860_027,
            block: TAPROOT_BLOCK,
            utxos: TAPROOT_UTXOS,
            expected_utxos: 6_303,
            expected_coinbase_origins: 1,
        },
        ActivationCase {
            name: "Taproot exception",
            height: 692_261,
            hash: "0000000000000000000f14c35b2d841e986ab5441de8c585d5ffe55ea1e395ad",
            parent_mtp: 1_627_020_976,
            block: TAPROOT_EXCEPTION_BLOCK,
            utxos: TAPROOT_EXCEPTION_UTXOS,
            expected_utxos: 6_157,
            expected_coinbase_origins: 0,
        },
    ]
}

#[derive(Deserialize)]
struct FixtureUtxo {
    txid: String,
    vout: u32,
    value_sats: u64,
    script_pubkey: String,
    height: u32,
    creation_mtp: u32,
    is_coinbase: bool,
}

#[derive(Default)]
struct MemoryUtxoStore {
    entries: Mutex<BTreeMap<OutPointKey, Utxo>>,
}

impl MemoryUtxoStore {
    fn from_entries(entries: BTreeMap<OutPointKey, Utxo>) -> Self {
        Self {
            entries: Mutex::new(entries),
        }
    }
}

impl UtxoStore for MemoryUtxoStore {
    fn get(&self, outpoint: OutPointKey) -> Result<Option<Utxo>, UtxoError> {
        Ok(self
            .entries
            .lock()
            .expect("memory UTXO lock")
            .get(&outpoint)
            .cloned())
    }

    fn apply(
        &self,
        spent: &[OutPointKey],
        created: &[(OutPointKey, Utxo)],
    ) -> Result<(), UtxoError> {
        self.apply_with_undo(spent, created).map(|_| ())
    }

    fn apply_with_undo(
        &self,
        spent: &[OutPointKey],
        created: &[(OutPointKey, Utxo)],
    ) -> Result<UtxoUndo, UtxoError> {
        let mut entries = self.entries.lock().expect("memory UTXO lock");
        let mut spent_set = BTreeSet::new();
        let mut previous = Vec::with_capacity(spent.len());
        for outpoint in spent {
            if !spent_set.insert(*outpoint) {
                return Err(UtxoError::DuplicateSpend(*outpoint));
            }
            previous.push((
                *outpoint,
                entries
                    .get(outpoint)
                    .cloned()
                    .ok_or(UtxoError::Missing(*outpoint))?,
            ));
        }
        let mut created_set = BTreeSet::new();
        for (outpoint, _) in created {
            if !created_set.insert(*outpoint)
                || (entries.contains_key(outpoint) && !spent_set.contains(outpoint))
            {
                return Err(UtxoError::Duplicate(*outpoint));
            }
        }
        for outpoint in spent {
            entries.remove(outpoint);
        }
        for (outpoint, utxo) in created {
            entries.insert(*outpoint, utxo.clone());
        }
        Ok(UtxoUndo::from_parts(
            previous,
            created.iter().map(|(outpoint, _)| *outpoint).collect(),
        ))
    }

    fn undo(&self, undo: &UtxoUndo, _now: u64, _hot_window_secs: u64) -> Result<(), UtxoError> {
        let mut entries = self.entries.lock().expect("memory UTXO lock");
        for outpoint in undo.created() {
            entries.remove(outpoint);
        }
        for (outpoint, utxo) in undo.spent() {
            if entries.insert(*outpoint, utxo.clone()).is_some() {
                return Err(UtxoError::Duplicate(*outpoint));
            }
        }
        Ok(())
    }

    fn age_to_cold(&self, _now: u64, _hot_window_secs: u64) -> Result<u64, UtxoError> {
        Ok(0)
    }

    fn snapshot_entries(&self) -> Result<BTreeMap<OutPointKey, Utxo>, UtxoError> {
        Ok(self.entries.lock().expect("memory UTXO lock").clone())
    }

    fn replace_all(
        &self,
        entries: &BTreeMap<OutPointKey, Utxo>,
        _now: u64,
        _hot_window_secs: u64,
    ) -> Result<(), UtxoError> {
        self.entries
            .lock()
            .expect("memory UTXO lock")
            .clone_from(entries);
        Ok(())
    }

    fn tier_stats(&self) -> Result<TierStats, UtxoError> {
        Ok(TierStats {
            hot: u64::try_from(self.entries.lock().expect("memory UTXO lock").len())
                .expect("fixture count fits u64"),
            cold: 0,
        })
    }
}

fn block(case: &ActivationCase) -> Block {
    let raw = zstd::stream::decode_all(case.block).expect("zstd block");
    let block: Block = deserialize(&raw).expect("historical block");
    assert_eq!(block.block_hash().to_string(), case.hash, "{}", case.name);
    block
        .header
        .validate_pow(block.header.target())
        .expect("historical block satisfies its claimed proof of work");
    block
}

fn utxos(case: &ActivationCase, block: &Block) -> BTreeMap<OutPointKey, Utxo> {
    let raw = zstd::stream::decode_all(case.utxos).expect("zstd UTXO fixture");
    let fixtures: Vec<FixtureUtxo> = serde_json::from_slice(&raw).expect("UTXO fixture JSON");
    assert_eq!(fixtures.len(), case.expected_utxos, "{}", case.name);
    let entries = fixtures
        .into_iter()
        .map(|fixture| {
            let txid = Txid::from_str(&fixture.txid).expect("fixture txid");
            let outpoint = bitcoin::OutPoint::new(txid, fixture.vout);
            let utxo = Utxo {
                value_sats: fixture.value_sats,
                height: fixture.height,
                is_coinbase: fixture.is_coinbase,
                last_touched: u64::from(block.header.time),
                creation_mtp: fixture.creation_mtp,
                script_pubkey: Vec::<u8>::from_hex(&fixture.script_pubkey)
                    .expect("fixture scriptPubKey"),
            };
            (outpoint.into(), utxo)
        })
        .collect::<BTreeMap<_, _>>();
    assert_eq!(entries.len(), case.expected_utxos, "{}", case.name);
    entries
}

fn assert_complete_external_view(
    case: &ActivationCase,
    block: &Block,
    entries: &BTreeMap<OutPointKey, Utxo>,
) {
    let transaction_positions = block
        .txdata
        .iter()
        .enumerate()
        .map(|(index, transaction)| (transaction.compute_txid(), index))
        .collect::<BTreeMap<_, _>>();
    let mut external = BTreeSet::new();
    for (spending_index, transaction) in block.txdata.iter().enumerate().skip(1) {
        for input in &transaction.input {
            if let Some(creating_index) = transaction_positions.get(&input.previous_output.txid) {
                assert!(
                    creating_index < &spending_index,
                    "{} forward spend",
                    case.name
                );
            } else {
                external.insert(OutPointKey::from(input.previous_output));
            }
        }
    }
    assert_eq!(
        external,
        entries.keys().copied().collect(),
        "{} external UTXO view",
        case.name
    );
}

fn assert_exact_origin_metadata(case: &ActivationCase, entries: &BTreeMap<OutPointKey, Utxo>) {
    assert_eq!(
        entries.values().filter(|utxo| utxo.is_coinbase).count(),
        case.expected_coinbase_origins,
        "{} coinbase origins",
        case.name
    );
    for (outpoint, utxo) in entries {
        assert!(
            (1..case.height).contains(&utxo.height),
            "{} {outpoint} has invalid origin height {}",
            case.name,
            utxo.height
        );
        assert!(
            (1..=case.parent_mtp).contains(&utxo.creation_mtp),
            "{} {outpoint} has invalid origin parent MTP {}",
            case.name,
            utxo.creation_mtp
        );
        if utxo.is_coinbase {
            assert!(
                case.height >= utxo.height.saturating_add(COINBASE_MATURITY),
                "{} {outpoint} spends an immature fixture coinbase",
                case.name
            );
        }
    }
}

fn execute(
    case: &ActivationCase,
    block: &Block,
    store: &MemoryUtxoStore,
) -> Result<rbtc::blockchain::AppliedBlock, BlockError> {
    let deployments = block_deployment_context(
        Network::Bitcoin,
        case.height,
        block.block_hash(),
        block.header.time,
        true,
    );
    apply_block_with_deployments(
        store,
        block,
        case.height,
        u64::from(block.header.time),
        case.parent_mtp,
        60 * 24 * 60 * 60,
        deployments.script_flags,
        deployments.bip34_active,
        deployments.csv_active,
        deployments.segwit_active,
        None,
        deployments.subsidy_sats,
    )
}

fn execute_with_script_flags(
    case: &ActivationCase,
    block: &Block,
    store: &MemoryUtxoStore,
    script_flags: u32,
) -> Result<rbtc::blockchain::AppliedBlock, BlockError> {
    let deployments = block_deployment_context(
        Network::Bitcoin,
        case.height,
        block.block_hash(),
        block.header.time,
        true,
    );
    apply_block_with_deployments(
        store,
        block,
        case.height,
        u64::from(block.header.time),
        case.parent_mtp,
        60 * 24 * 60 * 60,
        script_flags,
        deployments.bip34_active,
        deployments.csv_active,
        deployments.segwit_active,
        None,
        deployments.subsidy_sats,
    )
}

#[test]
fn complete_mainnet_activation_blocks_execute_and_undo_from_external_utxos() {
    let mut transaction_count = 0_usize;
    let mut external_utxo_count = 0_usize;
    for case in cases() {
        let block = block(&case);
        let before = utxos(&case, &block);
        transaction_count += block.txdata.len();
        external_utxo_count += before.len();
        assert_complete_external_view(&case, &block, &before);
        assert_exact_origin_metadata(&case, &before);
        let store = MemoryUtxoStore::from_entries(before.clone());
        let applied = execute(&case, &block, &store).unwrap_or_else(|error| {
            panic!("{} activation block execution failed: {error}", case.name)
        });
        assert_eq!(applied.hash, block.block_hash());
        assert_eq!(applied.transaction_undos.len(), block.txdata.len());
        for undo in applied.transaction_undos.iter().rev() {
            store
                .undo(undo, u64::from(block.header.time), 60 * 24 * 60 * 60)
                .unwrap();
        }
        assert_eq!(store.snapshot_entries().unwrap(), before, "{}", case.name);
    }
    assert_eq!(transaction_count, 8_997);
    assert_eq!(external_utxo_count, 23_331);
}

#[test]
fn exact_coinbase_origin_rejects_a_spend_one_block_before_maturity() {
    let (case, block, mut before, outpoint) = cases()
        .into_iter()
        .find_map(|case| {
            let block = block(&case);
            let before = utxos(&case, &block);
            let outpoint = before
                .iter()
                .find_map(|(outpoint, utxo)| utxo.is_coinbase.then_some(*outpoint))?;
            Some((case, block, before, outpoint))
        })
        .expect("at least one historical block spends an external coinbase");
    before.get_mut(&outpoint).unwrap().height = case.height - (COINBASE_MATURITY - 1);
    let store = MemoryUtxoStore::from_entries(before.clone());
    assert!(matches!(
        execute(&case, &block, &store),
        Err(BlockError::Transaction {
            source: ChainstateError::ImmatureCoinbase {
                outpoint: rejected,
                matures_at,
            },
            ..
        }) if rejected == outpoint && matures_at == case.height + 1
    ));
    assert_eq!(store.snapshot_entries().unwrap(), before);
}

#[test]
fn exact_bip68_origin_height_drives_the_relative_lock_boundary() {
    let (case, block, mut before, transaction_index, outpoint, relative) = cases()
        .into_iter()
        .filter(|case| case.height >= 419_328)
        .find_map(|case| {
            let block = block(&case);
            let before = utxos(&case, &block);
            let mut candidate = None;
            for (index, transaction) in block.txdata.iter().enumerate().skip(1) {
                if transaction.version.0 < 2 {
                    continue;
                }
                for input in &transaction.input {
                    let outpoint = OutPointKey::from(input.previous_output);
                    let Some(utxo) = before.get(&outpoint) else {
                        continue;
                    };
                    let sequence = input.sequence;
                    let relative = sequence.to_consensus_u32() & 0x0000_FFFF;
                    if sequence.is_relative_lock_time()
                        && sequence.is_height_locked()
                        && relative > 0
                        && case.height >= utxo.height.saturating_add(relative)
                    {
                        let slack = case
                            .height
                            .saturating_sub(utxo.height.saturating_add(relative));
                        if candidate
                            .as_ref()
                            .is_none_or(|(_, _, _, best_slack)| slack < *best_slack)
                        {
                            candidate = Some((index, outpoint, relative, slack));
                        }
                    }
                }
            }
            let (transaction_index, outpoint, relative, _) = candidate?;
            Some((case, block, before, transaction_index, outpoint, relative))
        })
        .expect("a historical complete block has a height-relative external spend");
    before.get_mut(&outpoint).unwrap().height = case.height - relative + 1;
    let store = MemoryUtxoStore::from_entries(before.clone());
    assert!(matches!(
        execute(&case, &block, &store),
        Err(BlockError::Transaction {
            index,
            source: ChainstateError::RelativeHeightLock {
                outpoint: rejected,
                minimum_height,
            },
        }) if index == transaction_index
            && rejected == outpoint
            && minimum_height == case.height
    ));
    assert_eq!(store.snapshot_entries().unwrap(), before);
}

#[test]
fn late_historical_script_failure_rolls_back_the_whole_block() {
    let case = cases()
        .into_iter()
        .find(|case| case.name == "Taproot")
        .unwrap();
    let block = block(&case);
    let mut before = utxos(&case, &block);
    let (failure_index, poisoned) = block
        .txdata
        .iter()
        .enumerate()
        .rev()
        .find_map(|(index, transaction)| {
            transaction.input.iter().find_map(|input| {
                let outpoint = OutPointKey::from(input.previous_output);
                before.contains_key(&outpoint).then_some((index, outpoint))
            })
        })
        .expect("late transaction spends an external UTXO");
    assert!(failure_index > block.txdata.len() / 2);
    before.get_mut(&poisoned).unwrap().script_pubkey = vec![0x6a];
    let store = MemoryUtxoStore::from_entries(before.clone());
    assert!(matches!(
        execute(&case, &block, &store),
        Err(BlockError::Transaction { index, .. }) if index == failure_index
    ));
    assert_eq!(store.snapshot_entries().unwrap(), before);
}

#[test]
fn taproot_exception_is_required_for_the_real_pre_activation_anyone_can_spend() {
    let case = cases()
        .into_iter()
        .find(|case| case.name == "Taproot exception")
        .unwrap();
    let block = block(&case);
    let before = utxos(&case, &block);
    let deployments = block_deployment_context(
        Network::Bitcoin,
        case.height,
        block.block_hash(),
        block.header.time,
        false,
    );
    assert_eq!(
        deployments.script_flags & bitcoinconsensus::VERIFY_TAPROOT,
        0
    );
    let exception_txid =
        Txid::from_str("b10c007c60e14f9d087e0291d4d0c7869697c6681d979c6639dbd960792b4d41").unwrap();
    let (exception_index, exception_transaction) = block
        .txdata
        .iter()
        .enumerate()
        .find(|(_, transaction)| transaction.compute_txid() == exception_txid)
        .expect("Core Taproot exception transaction");
    let anyone_can_spend_inputs = exception_transaction
        .input
        .iter()
        .filter(|input| {
            before
                .get(&OutPointKey::from(input.previous_output))
                .is_some_and(|utxo| utxo.script_pubkey.starts_with(&[0x51, 0x20]))
        })
        .collect::<Vec<_>>();
    assert_eq!(anyone_can_spend_inputs.len(), 4);
    assert!(
        anyone_can_spend_inputs
            .iter()
            .all(|input| input.witness.is_empty())
    );
    let store = MemoryUtxoStore::from_entries(before.clone());
    execute(&case, &block, &store).expect("Core's historical exception accepts the real block");

    let store = MemoryUtxoStore::from_entries(before.clone());
    assert!(matches!(
        execute_with_script_flags(
            &case,
            &block,
            &store,
            deployments.script_flags | bitcoinconsensus::VERIFY_TAPROOT,
        ),
        Err(BlockError::Transaction { index, .. }) if index == exception_index
    ));
    assert_eq!(store.snapshot_entries().unwrap(), before);
}

#[test]
fn complete_activation_result_survives_redb_reopen() {
    let case = cases()
        .into_iter()
        .find(|case| case.name == "BIP65")
        .unwrap();
    let block = block(&case);
    let store = MemoryUtxoStore::from_entries(utxos(&case, &block));
    execute(&case, &block, &store).unwrap();
    let expected = store.snapshot_entries().unwrap();

    let directory = TempDir::new().unwrap();
    let path = directory.path().join("historical-chainstate.redb");
    let durable = RedbUtxoStore::open(&path).unwrap();
    durable
        .replace_all(&expected, u64::from(block.header.time), 60 * 24 * 60 * 60)
        .unwrap();
    drop(durable);
    let reopened = RedbUtxoStore::open(path).unwrap();
    assert_eq!(reopened.snapshot_entries().unwrap(), expected);
}
