//! Optional live block-level differential test against Bitcoin Core 26.

use std::{
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::{Mutex, MutexGuard},
    thread,
    time::{Duration, Instant},
};

use bitcoin::{
    Amount, Block, BlockHash, Network, OutPoint, ScriptBuf, Sequence, Transaction, TxIn,
    TxMerkleNode, TxOut, Witness,
    absolute::LockTime,
    block::{Header, Version as HeaderVersion},
    consensus::encode::serialize_hex,
    hashes::Hash,
    pow::Target,
    transaction::Version,
};
use rbtc::{
    block_execution::connect_active_block,
    blockchain::block_subsidy_with_interval,
    chain_store::RedbChainStore,
    deployments::{DeploymentConfig, block_deployment_context_for_headers},
    headers::HeaderDag,
    utxo::{OutPointKey, UtxoStore},
};
use tempfile::TempDir;

static CORE_NODE_LOCK: Mutex<()> = Mutex::new(());

struct CoreNode {
    child: Option<Child>,
    cli: PathBuf,
    data_dir: PathBuf,
    rpc_port: u16,
    _serial_guard: MutexGuard<'static, ()>,
}

impl CoreNode {
    fn start(bitcoind: &Path) -> Self {
        Self::start_with_args(bitcoind, &[])
    }

    fn start_with_args(bitcoind: &Path, extra_args: &[&str]) -> Self {
        let serial_guard = CORE_NODE_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let cli = bitcoind.with_file_name("bitcoin-cli");
        assert!(cli.is_file(), "bitcoin-cli must be next to bitcoind");
        let data_dir = TempDir::new().unwrap().keep();
        let rpc_port = unused_port();
        let mut command = Command::new(bitcoind);
        command
            .args([
                "-regtest",
                "-server=1",
                "-listen=0",
                "-dnsseed=0",
                "-discover=0",
                "-printtoconsole=0",
                "-rpcbind=127.0.0.1",
                "-rpcallowip=127.0.0.1",
            ])
            .args(extra_args)
            .arg(format!("-datadir={}", data_dir.display()))
            .arg(format!("-rpcport={rpc_port}"))
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        let child = command.spawn().unwrap();
        let mut node = Self {
            child: Some(child),
            cli,
            data_dir,
            rpc_port,
            _serial_guard: serial_guard,
        };
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            if node.rpc(&["getblockcount"]).is_ok() {
                return node;
            }
            assert!(Instant::now() < deadline, "Bitcoin Core RPC did not start");
            assert!(
                node.child.as_mut().unwrap().try_wait().unwrap().is_none(),
                "Bitcoin Core exited during startup"
            );
            thread::sleep(Duration::from_millis(50));
        }
    }

    fn rpc(&self, arguments: &[&str]) -> Result<String, String> {
        let output = Command::new(&self.cli)
            .args(["-regtest", "-rpcclienttimeout=5"])
            .arg(format!("-datadir={}", self.data_dir.display()))
            .arg(format!("-rpcport={}", self.rpc_port))
            .args(arguments)
            .output()
            .map_err(|error| error.to_string())?;
        if output.status.success() {
            Ok(String::from_utf8(output.stdout)
                .map_err(|error| error.to_string())?
                .trim()
                .to_owned())
        } else {
            Err(String::from_utf8_lossy(&output.stderr).trim().to_owned())
        }
    }

    fn submit(&self, block: &Block) -> String {
        self.rpc(&["submitblock", &serialize_hex(block)])
            .unwrap_or_else(|error| error)
    }
}

impl Drop for CoreNode {
    fn drop(&mut self) {
        let _ = self.rpc(&["stop"]);
        if let Some(mut child) = self.child.take() {
            let deadline = Instant::now() + Duration::from_secs(5);
            while Instant::now() < deadline {
                if child.try_wait().ok().flatten().is_some() {
                    break;
                }
                thread::sleep(Duration::from_millis(20));
            }
            if child.try_wait().ok().flatten().is_none() {
                let _ = child.kill();
                let _ = child.wait();
            }
        }
        let _ = std::fs::remove_dir_all(&self.data_dir);
    }
}

fn unused_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn coinbase(height: u32, value: u64) -> Transaction {
    let mut height_prefix = match height {
        0 => vec![0x00],
        1..=16 => vec![0x50 + u8::try_from(height).unwrap()],
        _ => {
            let mut value = height;
            let mut encoded = Vec::new();
            while value > 0 {
                encoded.push((value & 0xff) as u8);
                value >>= 8;
            }
            if encoded.last().is_some_and(|byte| byte & 0x80 != 0) {
                encoded.push(0);
            }
            let mut prefix = vec![u8::try_from(encoded.len()).unwrap()];
            prefix.extend(encoded);
            prefix
        }
    };
    height_prefix.push(0);
    Transaction {
        version: Version::ONE,
        lock_time: LockTime::ZERO,
        input: vec![TxIn {
            previous_output: OutPoint::null(),
            script_sig: ScriptBuf::from_bytes(height_prefix),
            sequence: Sequence::MAX,
            witness: Witness::new(),
        }],
        output: vec![TxOut {
            value: Amount::from_sat(value),
            script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
        }],
    }
}

fn coinbase_with_witness_commitment(height: u32, value: u64, valid: bool) -> Transaction {
    let mut transaction = coinbase(height, value);
    let reserved = [0_u8; 32];
    if valid {
        transaction.input[0].witness = Witness::from_slice(&[reserved]);
    }
    let commitment = if valid {
        Block::compute_witness_commitment(&bitcoin::WitnessMerkleNode::all_zeros(), &reserved)
            .to_byte_array()
            .to_vec()
    } else {
        vec![0; 32]
    };
    transaction.output.push(TxOut {
        value: Amount::ZERO,
        script_pubkey: ScriptBuf::from_bytes(
            [vec![0x6a, 0x24, 0xaa, 0x21, 0xa9, 0xed], commitment].concat(),
        ),
    });
    transaction
}

fn spend(previous_output: OutPoint, sequence: Sequence, value: u64) -> Transaction {
    spend_with_context(
        previous_output,
        sequence,
        value,
        LockTime::ZERO,
        ScriptBuf::new(),
    )
}

fn spend_with_context(
    previous_output: OutPoint,
    sequence: Sequence,
    value: u64,
    lock_time: LockTime,
    script_sig: ScriptBuf,
) -> Transaction {
    Transaction {
        version: Version::TWO,
        lock_time,
        input: vec![TxIn {
            previous_output,
            script_sig,
            sequence,
            witness: Witness::new(),
        }],
        output: vec![TxOut {
            value: Amount::from_sat(value),
            script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
        }],
    }
}

fn median_time_past(blocks: &[Block]) -> u32 {
    let start = blocks.len().saturating_sub(11);
    let mut times = blocks[start..]
        .iter()
        .map(|block| block.header.time)
        .collect::<Vec<_>>();
    times.sort_unstable();
    times[times.len() / 2]
}

fn mine_and_submit_prefix(
    core: &CoreNode,
    count: u32,
    mut reward_at_height: impl FnMut(u32) -> Transaction,
) -> Vec<Block> {
    let genesis = bitcoin::blockdata::constants::genesis_block(Network::Regtest);
    let mut blocks = Vec::with_capacity(usize::try_from(count).unwrap());
    let mut parent = genesis.block_hash();
    let mut time = genesis.header.time;
    for height in 1..=count {
        time += 1;
        let block = mine_block(parent, time, vec![reward_at_height(height)]);
        assert!(core.submit(&block).is_empty());
        parent = block.block_hash();
        blocks.push(block);
    }
    blocks
}

fn mine_block(parent: BlockHash, time: u32, transactions: Vec<Transaction>) -> Block {
    let target = Target::MAX_ATTAINABLE_REGTEST;
    let mut block = Block {
        header: Header {
            version: HeaderVersion::from_consensus(4),
            prev_blockhash: parent,
            merkle_root: TxMerkleNode::all_zeros(),
            time,
            bits: target.to_compact_lossy(),
            nonce: 0,
        },
        txdata: transactions,
    };
    block.header.merkle_root = block
        .compute_merkle_root()
        .unwrap_or_else(TxMerkleNode::all_zeros);
    remine(&mut block);
    block
}

fn remine(block: &mut Block) {
    let target = Target::MAX_ATTAINABLE_REGTEST;
    block.header.nonce = 0;
    while block.header.validate_pow(target).is_err() {
        block.header.nonce = block.header.nonce.checked_add(1).unwrap();
    }
}

struct RbtcOutcome {
    accepted: bool,
    tip_height: u32,
    candidate_undo: bool,
    candidate_output: bool,
}

fn rbtc_outcome(blocks: &[Block]) -> RbtcOutcome {
    rbtc_outcome_with_config(blocks, &DeploymentConfig::for_network(Network::Regtest))
}

fn rbtc_outcome_with_config(blocks: &[Block], deployments: &DeploymentConfig) -> RbtcOutcome {
    assert!(!blocks.is_empty());
    let directory = TempDir::new().unwrap();
    let store =
        RedbChainStore::open(directory.path().join("chainstate.redb"), Network::Regtest).unwrap();
    let mut headers = HeaderDag::with_deployments(deployments.clone());
    let mut accepted = true;
    for (offset, block) in blocks.iter().enumerate() {
        let height = u32::try_from(offset + 1).unwrap();
        if headers.insert_contextual(block.header, u32::MAX).is_err() {
            accepted = false;
            break;
        }
        let context = block_deployment_context_for_headers(
            deployments,
            &headers,
            height,
            block.block_hash(),
            block.header.time,
            true,
        )
        .unwrap();
        if connect_active_block(
            &store,
            &headers,
            block,
            u64::from(block.header.time),
            60,
            &context,
        )
        .is_err()
        {
            accepted = false;
            break;
        }
    }
    let candidate = blocks.last().unwrap();
    let candidate_output = candidate.txdata.first().is_some_and(|transaction| {
        !transaction.output.is_empty()
            && store
                .get(OutPointKey::from(OutPoint::new(
                    transaction.compute_txid(),
                    0,
                )))
                .unwrap()
                .is_some()
    });
    RbtcOutcome {
        accepted,
        tip_height: store.execution().tip().unwrap().height,
        candidate_undo: store.undos().get(candidate.block_hash()).unwrap().is_some(),
        candidate_output,
    }
}

#[test]
#[ignore = "set RBTC_BITCOIND to a Bitcoin Core 26 bitcoind and run explicitly"]
#[allow(clippy::too_many_lines)]
fn core_26_and_rbtc_agree_on_end_to_end_block_results() {
    let bitcoind = PathBuf::from(
        std::env::var_os("RBTC_BITCOIND").expect("RBTC_BITCOIND must identify Core 26 bitcoind"),
    );
    let core = CoreNode::start(&bitcoind);
    let network_info = core.rpc(&["getnetworkinfo"]).unwrap();
    assert!(
        network_info.contains("\"version\": 260000"),
        "RBTC_BITCOIND must be Bitcoin Core 26.0: {network_info}"
    );

    let genesis = bitcoin::blockdata::constants::genesis_block(Network::Regtest);
    let subsidy = block_subsidy_with_interval(1, 150);
    let first = mine_block(
        genesis.block_hash(),
        genesis.header.time + 1,
        vec![coinbase(1, subsidy)],
    );
    assert!(core.submit(&first).is_empty());
    let first_outcome = rbtc_outcome(std::slice::from_ref(&first));
    assert!(first_outcome.accepted);
    assert_eq!(first_outcome.tip_height, 1);
    assert!(first_outcome.candidate_undo);
    assert!(first_outcome.candidate_output);

    let valid = mine_block(
        first.block_hash(),
        first.header.time + 1,
        vec![coinbase(2, subsidy)],
    );
    let mut invalid = Vec::new();

    let excessive = mine_block(
        first.block_hash(),
        first.header.time + 2,
        vec![coinbase(2, subsidy + 1)],
    );
    invalid.push(("excessive coinbase", "bad-cb-amount", excessive));

    let wrong_height = mine_block(
        first.block_hash(),
        first.header.time + 3,
        vec![coinbase(3, subsidy)],
    );
    invalid.push(("wrong BIP34 height", "bad-cb-height", wrong_height));

    let mut second_coinbase = coinbase(2, 0);
    second_coinbase.input[0].script_sig.push_slice([0x01]);
    let multiple = mine_block(
        first.block_hash(),
        first.header.time + 4,
        vec![coinbase(2, subsidy), second_coinbase],
    );
    invalid.push(("multiple coinbase", "bad-cb-multiple", multiple));

    let mut bad_merkle = valid.clone();
    bad_merkle.header.time = first.header.time + 5;
    bad_merkle.header.merkle_root = TxMerkleNode::all_zeros();
    remine(&mut bad_merkle);
    invalid.push(("bad transaction merkle root", "bad-txnmrklroot", bad_merkle));

    let empty = mine_block(first.block_hash(), first.header.time + 6, Vec::new());
    invalid.push(("empty block", "Block does not start with a coinbase", empty));

    let ordinary_input = TxIn {
        previous_output: OutPoint::new(bitcoin::Txid::from_byte_array([1; 32]), 0),
        script_sig: ScriptBuf::new(),
        sequence: Sequence::MAX,
        witness: Witness::new(),
    };
    let ordinary_output = TxOut {
        value: Amount::ZERO,
        script_pubkey: ScriptBuf::new(),
    };
    let ordinary = Transaction {
        version: Version::ONE,
        lock_time: LockTime::ZERO,
        input: vec![ordinary_input.clone()],
        output: vec![ordinary_output.clone()],
    };
    let missing_coinbase = mine_block(
        first.block_hash(),
        first.header.time + 7,
        vec![ordinary.clone()],
    );
    invalid.push((
        "missing coinbase",
        "Block does not start with a coinbase",
        missing_coinbase,
    ));

    let mut short_coinbase = coinbase(2, subsidy);
    short_coinbase.input[0].script_sig = ScriptBuf::from_bytes(vec![0x52]);
    let short_coinbase = mine_block(
        first.block_hash(),
        first.header.time + 8,
        vec![short_coinbase],
    );
    invalid.push(("short coinbase script", "bad-cb-length", short_coinbase));

    let mut long_coinbase = coinbase(2, subsidy);
    long_coinbase.input[0].script_sig = ScriptBuf::from_bytes([vec![0x52], vec![0; 100]].concat());
    let long_coinbase = mine_block(
        first.block_hash(),
        first.header.time + 9,
        vec![long_coinbase],
    );
    invalid.push(("long coinbase script", "bad-cb-length", long_coinbase));

    let mut no_outputs = coinbase(2, subsidy);
    no_outputs.output.clear();
    let no_outputs = mine_block(first.block_hash(), first.header.time + 10, vec![no_outputs]);
    invalid.push((
        "transaction without outputs",
        "bad-txns-vout-empty",
        no_outputs,
    ));

    let no_inputs = Transaction {
        version: Version::ONE,
        lock_time: LockTime::ZERO,
        input: Vec::new(),
        output: vec![ordinary_output.clone()],
    };
    let no_inputs = mine_block(
        first.block_hash(),
        first.header.time + 11,
        vec![coinbase(2, subsidy), no_inputs],
    );
    invalid.push((
        "transaction without inputs",
        "Block decode failed",
        no_inputs,
    ));

    let mut duplicate_inputs = ordinary.clone();
    duplicate_inputs.input.push(ordinary_input.clone());
    let duplicate_inputs = mine_block(
        first.block_hash(),
        first.header.time + 12,
        vec![coinbase(2, subsidy), duplicate_inputs],
    );
    invalid.push((
        "duplicate transaction inputs",
        "bad-txns-inputs-duplicate",
        duplicate_inputs,
    ));

    let null_prevout = Transaction {
        version: Version::ONE,
        lock_time: LockTime::ZERO,
        input: vec![
            TxIn {
                previous_output: OutPoint::null(),
                script_sig: ScriptBuf::new(),
                sequence: Sequence::MAX,
                witness: Witness::new(),
            },
            ordinary_input,
        ],
        output: vec![ordinary_output],
    };
    let null_prevout = mine_block(
        first.block_hash(),
        first.header.time + 13,
        vec![coinbase(2, subsidy), null_prevout],
    );
    invalid.push((
        "null previous output",
        "bad-txns-prevout-null",
        null_prevout,
    ));

    for (name, expected_rejection, block) in invalid {
        let rejection = core.submit(&block);
        assert!(
            rejection.contains(expected_rejection),
            "unexpected Core result for {name}: {rejection}"
        );
        let outcome = rbtc_outcome(&[first.clone(), block]);
        assert!(!outcome.accepted, "rBTC accepted {name}");
        assert_eq!(outcome.tip_height, 1, "rBTC advanced tip for {name}");
        assert!(!outcome.candidate_undo, "rBTC stored undo for {name}");
        assert!(!outcome.candidate_output, "rBTC stored UTXO for {name}");
        println!("{name}: Core={rejection}");
    }

    assert!(core.submit(&valid).is_empty());
    let valid_outcome = rbtc_outcome(&[first, valid]);
    assert!(valid_outcome.accepted);
    assert_eq!(valid_outcome.tip_height, 2);
    assert!(valid_outcome.candidate_undo);
    assert!(valid_outcome.candidate_output);
    assert_eq!(core.rpc(&["getblockcount"]).unwrap(), "2");
}

#[test]
#[ignore = "set RBTC_BITCOIND to a Bitcoin Core 26 bitcoind and run explicitly"]
fn core_26_and_rbtc_agree_on_bip34_override_boundary() {
    let bitcoind = PathBuf::from(
        std::env::var_os("RBTC_BITCOIND").expect("RBTC_BITCOIND must identify Core 26 bitcoind"),
    );
    let core = CoreNode::start_with_args(&bitcoind, &["-testactivationheight=bip34@2"]);
    assert!(
        core.rpc(&["getnetworkinfo"])
            .unwrap()
            .contains("\"version\": 260000")
    );
    let mut deployments = DeploymentConfig::for_network(Network::Regtest);
    deployments.apply_test_activation_height("bip34@2").unwrap();

    let genesis = bitcoin::blockdata::constants::genesis_block(Network::Regtest);
    let subsidy = block_subsidy_with_interval(1, 150);
    let mut pre_activation_coinbase = coinbase(1, subsidy);
    pre_activation_coinbase.input[0].script_sig = ScriptBuf::from_bytes(vec![0x00, 0x00]);
    let first = mine_block(
        genesis.block_hash(),
        genesis.header.time + 1,
        vec![pre_activation_coinbase],
    );
    assert!(core.submit(&first).is_empty());
    let first_outcome = rbtc_outcome_with_config(std::slice::from_ref(&first), &deployments);
    assert!(first_outcome.accepted);
    assert_eq!(first_outcome.tip_height, 1);

    let mut wrong_coinbase = coinbase(2, subsidy);
    wrong_coinbase.input[0].script_sig = ScriptBuf::from_bytes(vec![0x51, 0x00]);
    let wrong = mine_block(
        first.block_hash(),
        first.header.time + 1,
        vec![wrong_coinbase],
    );
    assert_eq!(core.submit(&wrong), "bad-cb-height");
    let wrong_outcome = rbtc_outcome_with_config(&[first.clone(), wrong], &deployments);
    assert!(!wrong_outcome.accepted);
    assert_eq!(wrong_outcome.tip_height, 1);
    assert!(!wrong_outcome.candidate_undo);
    assert!(!wrong_outcome.candidate_output);

    let valid = mine_block(
        first.block_hash(),
        first.header.time + 2,
        vec![coinbase(2, subsidy)],
    );
    assert!(core.submit(&valid).is_empty());
    let valid_outcome = rbtc_outcome_with_config(&[first, valid], &deployments);
    assert!(valid_outcome.accepted);
    assert_eq!(valid_outcome.tip_height, 2);
    assert!(valid_outcome.candidate_undo);
    assert!(valid_outcome.candidate_output);
    assert_eq!(core.rpc(&["getblockcount"]).unwrap(), "2");
}

#[test]
#[ignore = "set RBTC_BITCOIND to a Bitcoin Core 26 bitcoind and run explicitly"]
fn core_26_and_rbtc_agree_on_bip66_and_bip65_header_boundaries() {
    let bitcoind = PathBuf::from(
        std::env::var_os("RBTC_BITCOIND").expect("RBTC_BITCOIND must identify Core 26 bitcoind"),
    );
    let core = CoreNode::start_with_args(
        &bitcoind,
        &[
            "-testactivationheight=dersig@2",
            "-testactivationheight=cltv@3",
        ],
    );
    let mut deployments = DeploymentConfig::for_network(Network::Regtest);
    deployments
        .apply_test_activation_height("dersig@2")
        .unwrap();
    deployments.apply_test_activation_height("cltv@3").unwrap();
    let genesis = bitcoin::blockdata::constants::genesis_block(Network::Regtest);
    let subsidy = block_subsidy_with_interval(1, 150);

    let mut first = mine_block(
        genesis.block_hash(),
        genesis.header.time + 1,
        vec![coinbase(1, subsidy)],
    );
    first.header.version = HeaderVersion::from_consensus(2);
    remine(&mut first);
    assert!(core.submit(&first).is_empty());
    assert!(rbtc_outcome_with_config(std::slice::from_ref(&first), &deployments).accepted);

    let mut obsolete_two = mine_block(
        first.block_hash(),
        first.header.time + 1,
        vec![coinbase(2, subsidy)],
    );
    obsolete_two.header.version = HeaderVersion::from_consensus(2);
    remine(&mut obsolete_two);
    assert!(core.submit(&obsolete_two).contains("bad-version"));
    let rejected = rbtc_outcome_with_config(&[first.clone(), obsolete_two], &deployments);
    assert!(!rejected.accepted);
    assert_eq!(rejected.tip_height, 1);
    assert!(!rejected.candidate_undo);
    assert!(!rejected.candidate_output);

    let mut second = mine_block(
        first.block_hash(),
        first.header.time + 1,
        vec![coinbase(2, subsidy)],
    );
    second.header.version = HeaderVersion::from_consensus(3);
    remine(&mut second);
    assert!(core.submit(&second).is_empty());

    let mut obsolete_three = mine_block(
        second.block_hash(),
        second.header.time + 1,
        vec![coinbase(3, subsidy)],
    );
    obsolete_three.header.version = HeaderVersion::from_consensus(3);
    remine(&mut obsolete_three);
    assert!(core.submit(&obsolete_three).contains("bad-version"));
    let rejected = rbtc_outcome_with_config(
        &[first.clone(), second.clone(), obsolete_three],
        &deployments,
    );
    assert!(!rejected.accepted);
    assert_eq!(rejected.tip_height, 2);
    assert!(!rejected.candidate_undo);
    assert!(!rejected.candidate_output);

    let third = mine_block(
        second.block_hash(),
        second.header.time + 1,
        vec![coinbase(3, subsidy)],
    );
    assert!(core.submit(&third).is_empty());
    let accepted = rbtc_outcome_with_config(&[first, second, third], &deployments);
    assert!(accepted.accepted);
    assert_eq!(accepted.tip_height, 3);
    assert_eq!(core.rpc(&["getblockcount"]).unwrap(), "3");
}

#[test]
#[ignore = "set RBTC_BITCOIND to a Bitcoin Core 26 bitcoind and run explicitly"]
fn core_26_and_rbtc_agree_on_segwit_override_boundary() {
    let bitcoind = PathBuf::from(
        std::env::var_os("RBTC_BITCOIND").expect("RBTC_BITCOIND must identify Core 26 bitcoind"),
    );
    let core = CoreNode::start_with_args(&bitcoind, &["-testactivationheight=segwit@2"]);
    let mut deployments = DeploymentConfig::for_network(Network::Regtest);
    deployments
        .apply_test_activation_height("segwit@2")
        .unwrap();
    let genesis = bitcoin::blockdata::constants::genesis_block(Network::Regtest);
    let subsidy = block_subsidy_with_interval(1, 150);

    let first = mine_block(
        genesis.block_hash(),
        genesis.header.time + 1,
        vec![coinbase_with_witness_commitment(1, subsidy, false)],
    );
    assert!(core.submit(&first).is_empty());
    assert!(rbtc_outcome_with_config(std::slice::from_ref(&first), &deployments).accepted);

    let mut malformed_coinbase = coinbase_with_witness_commitment(2, subsidy, false);
    malformed_coinbase.input[0].witness = Witness::from_slice(&[[0_u8]]);
    let malformed = mine_block(
        first.block_hash(),
        first.header.time + 1,
        vec![malformed_coinbase],
    );
    assert_eq!(core.submit(&malformed), "bad-witness-nonce-size");
    let rejected = rbtc_outcome_with_config(&[first.clone(), malformed], &deployments);
    assert!(!rejected.accepted);
    assert_eq!(rejected.tip_height, 1);
    assert!(!rejected.candidate_undo);
    assert!(!rejected.candidate_output);

    let valid = mine_block(
        first.block_hash(),
        first.header.time + 2,
        vec![coinbase_with_witness_commitment(2, subsidy, true)],
    );
    assert!(core.submit(&valid).is_empty());
    let accepted = rbtc_outcome_with_config(&[first, valid], &deployments);
    assert!(accepted.accepted);
    assert_eq!(accepted.tip_height, 2);
    assert!(accepted.candidate_undo);
    assert_eq!(core.rpc(&["getblockcount"]).unwrap(), "2");
}

#[test]
#[ignore = "set RBTC_BITCOIND to a Bitcoin Core 26 bitcoind and run explicitly"]
fn core_26_and_rbtc_agree_on_csv_relative_lock_boundary() {
    let bitcoind = PathBuf::from(
        std::env::var_os("RBTC_BITCOIND").expect("RBTC_BITCOIND must identify Core 26 bitcoind"),
    );
    let core = CoreNode::start_with_args(&bitcoind, &["-testactivationheight=csv@102"]);
    let mut deployments = DeploymentConfig::for_network(Network::Regtest);
    deployments.apply_test_activation_height("csv@102").unwrap();
    let subsidy = block_subsidy_with_interval(1, 150);
    let mut blocks = mine_and_submit_prefix(&core, 100, |height| coinbase(height, subsidy));
    let mut parent = blocks.last().unwrap().block_hash();
    let mut time = blocks.last().unwrap().header.time;

    let first_coinbase = OutPoint::new(blocks[0].txdata[0].compute_txid(), 0);
    time += 1;
    let pre_activation = mine_block(
        parent,
        time,
        vec![
            coinbase(101, subsidy),
            spend(first_coinbase, Sequence::from_height(101), subsidy),
        ],
    );
    assert!(core.submit(&pre_activation).is_empty());
    let spendable = OutPoint::new(pre_activation.txdata[1].compute_txid(), 0);
    parent = pre_activation.block_hash();
    blocks.push(pre_activation);
    let accepted = rbtc_outcome_with_config(&blocks, &deployments);
    assert!(accepted.accepted);
    assert_eq!(accepted.tip_height, 101);

    time += 1;
    let relative_locked = mine_block(
        parent,
        time,
        vec![
            coinbase(102, subsidy),
            spend(spendable, Sequence::from_height(2), subsidy),
        ],
    );
    assert_eq!(core.submit(&relative_locked), "bad-txns-nonfinal");
    let mut rejected_chain = blocks.clone();
    rejected_chain.push(relative_locked);
    let rejected = rbtc_outcome_with_config(&rejected_chain, &deployments);
    assert!(!rejected.accepted);
    assert_eq!(rejected.tip_height, 101);
    assert!(!rejected.candidate_undo);
    assert!(!rejected.candidate_output);

    let valid = mine_block(
        parent,
        time + 1,
        vec![
            coinbase(102, subsidy),
            spend(spendable, Sequence::from_height(1), subsidy),
        ],
    );
    assert!(core.submit(&valid).is_empty());
    blocks.push(valid);
    let accepted = rbtc_outcome_with_config(&blocks, &deployments);
    assert!(accepted.accepted);
    assert_eq!(accepted.tip_height, 102);
    assert_eq!(core.rpc(&["getblockcount"]).unwrap(), "102");
}

#[test]
#[ignore = "set RBTC_BITCOIND to a Bitcoin Core 26 bitcoind and run explicitly"]
fn core_26_and_rbtc_agree_on_bip113_and_time_based_bip68_boundaries() {
    let bitcoind = PathBuf::from(
        std::env::var_os("RBTC_BITCOIND").expect("RBTC_BITCOIND must identify Core 26 bitcoind"),
    );
    let core = CoreNode::start_with_args(&bitcoind, &["-testactivationheight=csv@102"]);
    let mut deployments = DeploymentConfig::for_network(Network::Regtest);
    deployments.apply_test_activation_height("csv@102").unwrap();
    let subsidy = block_subsidy_with_interval(1, 150);
    let mut blocks = mine_and_submit_prefix(&core, 100, |height| coinbase(height, subsidy));
    let mut parent = blocks.last().unwrap().block_hash();
    let mut time = blocks.last().unwrap().header.time;

    let first_coinbase = OutPoint::new(blocks[0].txdata[0].compute_txid(), 0);
    time += 1;
    let pre_activation = mine_block(
        parent,
        time,
        vec![
            coinbase(101, subsidy),
            spend(first_coinbase, Sequence::MAX, subsidy),
        ],
    );
    assert!(core.submit(&pre_activation).is_empty());
    let relative_prevout = OutPoint::new(pre_activation.txdata[1].compute_txid(), 0);
    parent = pre_activation.block_hash();
    blocks.push(pre_activation);
    assert!(rbtc_outcome_with_config(&blocks, &deployments).accepted);

    let parent_mtp = median_time_past(&blocks);
    let second_coinbase = OutPoint::new(blocks[1].txdata[0].compute_txid(), 0);
    let absolute_locked = mine_block(
        parent,
        time + 1,
        vec![
            coinbase(102, subsidy),
            spend_with_context(
                second_coinbase,
                Sequence::ENABLE_RBF_NO_LOCKTIME,
                subsidy,
                LockTime::from_time(parent_mtp).unwrap(),
                ScriptBuf::new(),
            ),
        ],
    );
    assert_eq!(core.submit(&absolute_locked), "bad-txns-nonfinal");
    let mut rejected_chain = blocks.clone();
    rejected_chain.push(absolute_locked);
    let rejected = rbtc_outcome_with_config(&rejected_chain, &deployments);
    assert!(!rejected.accepted);
    assert_eq!(rejected.tip_height, 101);
    assert!(!rejected.candidate_undo);
    assert!(!rejected.candidate_output);

    let relative_locked = mine_block(
        parent,
        time + 2,
        vec![
            coinbase(102, subsidy),
            spend(
                relative_prevout,
                Sequence::from_512_second_intervals(1),
                subsidy,
            ),
        ],
    );
    assert_eq!(core.submit(&relative_locked), "bad-txns-nonfinal");
    let mut rejected_chain = blocks.clone();
    rejected_chain.push(relative_locked);
    let rejected = rbtc_outcome_with_config(&rejected_chain, &deployments);
    assert!(!rejected.accepted);
    assert_eq!(rejected.tip_height, 101);
    assert!(!rejected.candidate_undo);
    assert!(!rejected.candidate_output);

    let valid = mine_block(
        parent,
        time + 3,
        vec![
            coinbase(102, subsidy),
            spend_with_context(
                second_coinbase,
                Sequence::ENABLE_RBF_NO_LOCKTIME,
                subsidy,
                LockTime::from_time(parent_mtp - 1).unwrap(),
                ScriptBuf::new(),
            ),
            spend(
                relative_prevout,
                Sequence::from_512_second_intervals(0),
                subsidy,
            ),
        ],
    );
    assert!(core.submit(&valid).is_empty());
    blocks.push(valid);
    let accepted = rbtc_outcome_with_config(&blocks, &deployments);
    assert!(accepted.accepted);
    assert_eq!(accepted.tip_height, 102);
    assert_eq!(core.rpc(&["getblockcount"]).unwrap(), "102");
}

#[test]
#[ignore = "set RBTC_BITCOIND to a Bitcoin Core 26 bitcoind and run explicitly"]
fn core_26_and_rbtc_agree_on_bip147_nulldummy_boundary() {
    let bitcoind = PathBuf::from(
        std::env::var_os("RBTC_BITCOIND").expect("RBTC_BITCOIND must identify Core 26 bitcoind"),
    );
    let core = CoreNode::start_with_args(&bitcoind, &["-testactivationheight=segwit@102"]);
    let mut deployments = DeploymentConfig::for_network(Network::Regtest);
    deployments
        .apply_test_activation_height("segwit@102")
        .unwrap();
    let subsidy = block_subsidy_with_interval(1, 150);
    let null_dummy_script = ScriptBuf::from_bytes(vec![0x00, 0x00, 0xae]);
    let mut blocks = mine_and_submit_prefix(&core, 100, |height| {
        let mut reward = coinbase(height, subsidy);
        if height <= 2 {
            reward.output[0].script_pubkey = null_dummy_script.clone();
        }
        reward
    });
    let mut parent = blocks.last().unwrap().block_hash();
    let mut time = blocks.last().unwrap().header.time;

    let first_coinbase = OutPoint::new(blocks[0].txdata[0].compute_txid(), 0);
    time += 1;
    let pre_activation = mine_block(
        parent,
        time,
        vec![
            coinbase(101, subsidy),
            spend_with_context(
                first_coinbase,
                Sequence::MAX,
                subsidy,
                LockTime::ZERO,
                ScriptBuf::from_bytes(vec![0x01, 0x01]),
            ),
        ],
    );
    assert!(core.submit(&pre_activation).is_empty());
    parent = pre_activation.block_hash();
    blocks.push(pre_activation);
    assert!(rbtc_outcome_with_config(&blocks, &deployments).accepted);

    let second_coinbase = OutPoint::new(blocks[1].txdata[0].compute_txid(), 0);
    let non_null_dummy = mine_block(
        parent,
        time + 1,
        vec![
            coinbase(102, subsidy),
            spend_with_context(
                second_coinbase,
                Sequence::MAX,
                subsidy,
                LockTime::ZERO,
                ScriptBuf::from_bytes(vec![0x01, 0x01]),
            ),
        ],
    );
    let core_rejection = core.submit(&non_null_dummy);
    assert!(
        !core_rejection.is_empty(),
        "Core unexpectedly accepted the non-null BIP147 dummy"
    );
    let mut rejected_chain = blocks.clone();
    rejected_chain.push(non_null_dummy);
    let rejected = rbtc_outcome_with_config(&rejected_chain, &deployments);
    assert!(!rejected.accepted);
    assert_eq!(rejected.tip_height, 101);
    assert!(!rejected.candidate_undo);
    assert!(!rejected.candidate_output);

    let valid = mine_block(
        parent,
        time + 2,
        vec![
            coinbase(102, subsidy),
            spend_with_context(
                second_coinbase,
                Sequence::MAX,
                subsidy,
                LockTime::ZERO,
                ScriptBuf::from_bytes(vec![0x00]),
            ),
        ],
    );
    assert!(core.submit(&valid).is_empty());
    blocks.push(valid);
    let accepted = rbtc_outcome_with_config(&blocks, &deployments);
    assert!(accepted.accepted);
    assert_eq!(accepted.tip_height, 102);
    assert_eq!(core.rpc(&["getblockcount"]).unwrap(), "102");
}
