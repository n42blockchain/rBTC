//! Optional live block-level differential test against Bitcoin Core 26.

use std::{
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
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
    deployments::block_deployment_context,
    headers::HeaderDag,
    utxo::{OutPointKey, UtxoStore},
};
use tempfile::TempDir;

struct CoreNode {
    child: Option<Child>,
    cli: PathBuf,
    data_dir: PathBuf,
    rpc_port: u16,
}

impl CoreNode {
    fn start(bitcoind: &Path) -> Self {
        let cli = bitcoind.with_file_name("bitcoin-cli");
        assert!(cli.is_file(), "bitcoin-cli must be next to bitcoind");
        let data_dir = TempDir::new().unwrap().keep();
        let rpc_port = unused_port();
        let child = Command::new(bitcoind)
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
            .arg(format!("-datadir={}", data_dir.display()))
            .arg(format!("-rpcport={rpc_port}"))
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let mut node = Self {
            child: Some(child),
            cli,
            data_dir,
            rpc_port,
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
        self.rpc(&["submitblock", &serialize_hex(block)]).unwrap()
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

fn coinbase(height: u8, value: u64) -> Transaction {
    let height_opcode = 0x50_u8.checked_add(height).unwrap();
    Transaction {
        version: Version::ONE,
        lock_time: LockTime::ZERO,
        input: vec![TxIn {
            previous_output: OutPoint::null(),
            script_sig: ScriptBuf::from_bytes(vec![height_opcode, 0]),
            sequence: Sequence::MAX,
            witness: Witness::new(),
        }],
        output: vec![TxOut {
            value: Amount::from_sat(value),
            script_pubkey: ScriptBuf::new(),
        }],
    }
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
    block.header.merkle_root = block.compute_merkle_root().unwrap();
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
    assert!(!blocks.is_empty());
    let directory = TempDir::new().unwrap();
    let store =
        RedbChainStore::open(directory.path().join("chainstate.redb"), Network::Regtest).unwrap();
    let mut headers = HeaderDag::new(Network::Regtest);
    let mut accepted = true;
    for (offset, block) in blocks.iter().enumerate() {
        let height = u32::try_from(offset + 1).unwrap();
        headers.insert_contextual(block.header, u32::MAX).unwrap();
        let context = block_deployment_context(
            Network::Regtest,
            height,
            block.block_hash(),
            block.header.time,
            true,
        );
        if connect_active_block(
            &store,
            &headers,
            block,
            u64::from(block.header.time),
            60,
            context,
        )
        .is_err()
        {
            accepted = false;
            break;
        }
    }
    let candidate = blocks.last().unwrap();
    let output = OutPointKey::from(OutPoint::new(candidate.txdata[0].compute_txid(), 0));
    RbtcOutcome {
        accepted,
        tip_height: store.execution().tip().unwrap().height,
        candidate_undo: store.undos().get(candidate.block_hash()).unwrap().is_some(),
        candidate_output: store.get(output).unwrap().is_some(),
    }
}

#[test]
#[ignore = "set RBTC_BITCOIND to a Bitcoin Core 26 bitcoind and run explicitly"]
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
    invalid.push(("excessive coinbase", excessive));

    let wrong_height = mine_block(
        first.block_hash(),
        first.header.time + 3,
        vec![coinbase(3, subsidy)],
    );
    invalid.push(("wrong BIP34 height", wrong_height));

    let mut second_coinbase = coinbase(2, 0);
    second_coinbase.input[0].script_sig.push_slice([0x01]);
    let multiple = mine_block(
        first.block_hash(),
        first.header.time + 4,
        vec![coinbase(2, subsidy), second_coinbase],
    );
    invalid.push(("multiple coinbase", multiple));

    let mut bad_merkle = valid.clone();
    bad_merkle.header.time = first.header.time + 5;
    bad_merkle.header.merkle_root = TxMerkleNode::all_zeros();
    remine(&mut bad_merkle);
    invalid.push(("bad transaction merkle root", bad_merkle));

    for (name, block) in invalid {
        let rejection = core.submit(&block);
        assert!(!rejection.is_empty(), "Core accepted {name}");
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
