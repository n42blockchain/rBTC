//! Optional live replacement-decision differential against Bitcoin Core 31.
//!
//! Core 31 accepts a mempool replacement only when the affected clusters'
//! feerate diagram is strictly better afterwards. rBTC adopted the same rule
//! from its pure `feerate_diagram` layer; this suite submits identical
//! conflict scenarios to a real Core 31 daemon and to rBTC's admission pool
//! and requires the accept/reject verdicts to agree. The scenarios include
//! the case the pre-diagram heuristic decided *wrongly* (a replacement
//! out-rating its direct conflict while evicting a rich descendant), so a
//! silent regression to the old rule cannot pass.
//!
//! ```bash
//! RBTC_BITCOIND=/path/to/bitcoin-31.0/bin/bitcoind \
//!   cargo test --release --test core_replacement_differential -- --ignored --nocapture
//! ```

use std::{
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::{Mutex, MutexGuard},
    thread,
    time::{Duration, Instant},
};

use bitcoin::{
    Amount, Network, OutPoint, ScriptBuf, Sequence, Transaction, TxIn, TxOut, Txid, WScriptHash,
    Witness,
    absolute::LockTime,
    consensus::encode::{deserialize, serialize_hex},
    hashes::{Hash, hex::FromHex},
    opcodes,
    transaction::Version,
};
use rbtc::{
    transaction_admission::{
        TransactionAdmissionContext, TransactionAdmissionError, TransactionAdmissionPool,
    },
    utxo::{OutPointKey, RedbUtxoStore, Utxo, UtxoStore},
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
        let serial_guard = CORE_NODE_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let cli = bitcoind.with_file_name("bitcoin-cli");
        assert!(
            cli.is_file() || cli.with_extension("exe").is_file(),
            "bitcoin-cli must be next to bitcoind"
        );
        let cli = if cli.is_file() {
            cli
        } else {
            cli.with_extension("exe")
        };
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
                "-fallbackfee=0.0001",
            ])
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
            .args(["-regtest", "-rpcclienttimeout=15"])
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

fn core_31_bitcoind() -> PathBuf {
    PathBuf::from(
        std::env::var_os("RBTC_BITCOIND")
            .expect("RBTC_BITCOIND must identify Bitcoin Core 31 bitcoind"),
    )
}

/// The anyone-can-spend P2WSH used for every scenario output: witness
/// script `OP_TRUE`, spendable with a fixed one-item witness, standard on
/// both implementations, and signature-free so scenario shapes stay exact.
fn op_true_script() -> ScriptBuf {
    ScriptBuf::new_p2wsh(&WScriptHash::hash(&[opcodes::OP_TRUE.to_u8()]))
}

fn op_true_witness() -> Witness {
    Witness::from_slice(&[vec![opcodes::OP_TRUE.to_u8()]])
}

const FUNDING_SATS: u64 = 1_000_000;

fn rbf_input(previous: OutPoint) -> TxIn {
    TxIn {
        previous_output: previous,
        script_sig: ScriptBuf::new(),
        sequence: Sequence(0xffff_fffd),
        witness: op_true_witness(),
    }
}

/// Builds a spend of `previous` (worth `input_sats`) paying `fee_sats`, with
/// an optional OP_RETURN pad to control virtual size.
fn build_spend(previous: OutPoint, input_sats: u64, fee_sats: u64, pad: usize) -> Transaction {
    let mut output = vec![TxOut {
        value: Amount::from_sat(input_sats - fee_sats),
        script_pubkey: op_true_script(),
    }];
    if pad > 0 {
        let mut script = vec![
            opcodes::all::OP_RETURN.to_u8(),
            opcodes::all::OP_PUSHDATA2.to_u8(),
        ];
        script.extend(u16::try_from(pad).unwrap().to_le_bytes());
        script.extend(vec![0_u8; pad]);
        output.push(TxOut {
            value: Amount::ZERO,
            script_pubkey: ScriptBuf::from_bytes(script),
        });
    }
    Transaction {
        version: Version::TWO,
        lock_time: LockTime::ZERO,
        input: vec![rbf_input(previous)],
        output,
    }
}

fn context() -> TransactionAdmissionContext {
    TransactionAdmissionContext {
        height: 200,
        parent_mtp: 1_700_000_000,
        script_flags: bitcoinconsensus::VERIFY_P2SH | bitcoinconsensus::VERIFY_WITNESS,
        csv_active: true,
        full_rbf: false,
    }
}

struct Differential {
    core: CoreNode,
    pool: TransactionAdmissionPool,
    store: RedbUtxoStore,
    _store_dir: TempDir,
    fundings: Vec<OutPoint>,
}

impl Differential {
    fn set_up(count: usize) -> Self {
        let core = CoreNode::start(&core_31_bitcoind());
        let network_info = core.rpc(&["getnetworkinfo"]).unwrap();
        assert!(
            network_info.contains("\"version\": 310000"),
            "RBTC_BITCOIND must be Bitcoin Core 31.0: {network_info}"
        );
        core.rpc(&["createwallet", "differential"]).unwrap();
        let miner = core.rpc(&["getnewaddress"]).unwrap();
        core.rpc(&["generatetoaddress", "101", &miner]).unwrap();

        let script = op_true_script();
        let address = bitcoin::Address::from_script(&script, Network::Regtest).unwrap();
        let mut fundings = Vec::with_capacity(count);
        for _ in 0..count {
            let txid = core
                .rpc(&["sendtoaddress", &address.to_string(), "0.01"])
                .unwrap();
            let raw = core.rpc(&["getrawtransaction", &txid]).unwrap();
            let funding: Transaction = deserialize(&Vec::<u8>::from_hex(&raw).unwrap()).unwrap();
            let vout = funding
                .output
                .iter()
                .position(|output| output.script_pubkey == script)
                .expect("the wallet paid the differential script");
            fundings.push(OutPoint::new(
                funding.compute_txid(),
                u32::try_from(vout).unwrap(),
            ));
        }
        core.rpc(&["generatetoaddress", "1", &miner]).unwrap();

        let store_dir = TempDir::new().unwrap();
        let store = RedbUtxoStore::open(store_dir.path().join("chainstate.redb")).unwrap();
        let seeded = fundings
            .iter()
            .map(|outpoint| {
                (
                    OutPointKey::from(*outpoint),
                    Utxo {
                        value_sats: FUNDING_SATS,
                        height: 102,
                        is_coinbase: false,
                        last_touched: 0,
                        creation_mtp: 0,
                        script_pubkey: script.to_bytes(),
                    },
                )
            })
            .collect::<Vec<_>>();
        store.apply(&[], &seeded).unwrap();

        Self {
            core,
            pool: TransactionAdmissionPool::default(),
            store,
            _store_dir: store_dir,
            fundings,
        }
    }

    /// Submits one transaction to both mempools and requires one verdict.
    fn agree(&mut self, scenario: &str, transaction: &Transaction) -> bool {
        let hex = serialize_hex(transaction);
        let core_verdict = self.core.rpc(&["sendrawtransaction", &hex, "0"]);
        let rbtc_verdict = self.pool.admit(&self.store, transaction.clone(), context());
        let core_accepted = core_verdict.is_ok();
        let rbtc_accepted = rbtc_verdict.is_ok();
        assert_eq!(
            core_accepted,
            rbtc_accepted,
            "{scenario}: Core said {core_verdict:?}, rBTC said {}",
            match &rbtc_verdict {
                Ok(outcome) => format!("{outcome:?}"),
                Err(error) => format!("{error}"),
            }
        );
        println!(
            "{scenario}: both {}",
            if core_accepted {
                "accepted"
            } else {
                "rejected"
            }
        );
        core_accepted
    }

    /// Requires that both mempools rejected specifically on the feerate
    /// question — Core citing its diagram check, rBTC returning its
    /// diagram-comparison error — so an agreement produced by two different
    /// unrelated refusals cannot satisfy the diagram scenarios.
    fn agree_diagram_rejection(&mut self, scenario: &str, transaction: &Transaction) {
        let hex = serialize_hex(transaction);
        let core_verdict = self.core.rpc(&["sendrawtransaction", &hex, "0"]);
        let rbtc_verdict = self.pool.admit(&self.store, transaction.clone(), context());
        let core_message = core_verdict.expect_err(&format!("{scenario}: Core must reject"));
        assert!(
            core_message.contains("insufficient fee")
                || core_message.contains("feerate diagram")
                || core_message.contains("does not improve"),
            "{scenario}: Core rejected for an unexpected reason: {core_message}"
        );
        assert!(
            matches!(
                rbtc_verdict,
                Err(TransactionAdmissionError::ReplacementDiagramNotImproved { .. })
            ),
            "{scenario}: rBTC must reject on the diagram comparison"
        );
        println!("{scenario}: both rejected on the feerate question");
    }

    fn in_core_mempool(&self, txid: Txid) -> bool {
        self.core
            .rpc(&["getmempoolentry", &txid.to_string()])
            .is_ok()
    }
}

#[test]
#[ignore = "set RBTC_BITCOIND to a Bitcoin Core 31 bitcoind and run explicitly"]
#[allow(clippy::too_many_lines)]
fn core_31_and_rbtc_agree_on_replacement_decisions() {
    let mut differential = Differential::set_up(6);
    let fundings = differential.fundings.clone();

    // S1 — a same-shape fee bump strictly improves the diagram.
    let original = build_spend(fundings[0], FUNDING_SATS, 1_000, 0);
    assert!(differential.agree("S1 original", &original));
    let bump = build_spend(fundings[0], FUNDING_SATS - 1, 5_000, 0);
    assert!(differential.agree("S1 fee bump", &bump));

    // S2 — a larger, lower-feerate replacement pays more in total but sits
    // below the original early in the diagram: incomparable, rejected.
    let original = build_spend(fundings[1], FUNDING_SATS, 10_000, 0);
    assert!(differential.agree("S2 original", &original));
    let bloated = build_spend(fundings[1], FUNDING_SATS - 1, 16_000, 4_000);
    differential.agree_diagram_rejection("S2 bloated lower-rate replacement", &bloated);

    // S3 — the flagship divergence from the pre-diagram heuristic: the
    // replacement out-rates its direct conflict and out-pays the evicted
    // pair in absolute fee, but the pair's CPFP chunk beats it early in
    // the diagram.
    let parent = build_spend(fundings[2], FUNDING_SATS, 300, 0);
    assert!(differential.agree("S3 low-rate parent", &parent));
    let bumped = build_spend(
        OutPoint::new(parent.compute_txid(), 0),
        FUNDING_SATS - 300,
        50_000,
        0,
    );
    assert!(differential.agree("S3 rich child", &bumped));
    let usurper = build_spend(fundings[2], FUNDING_SATS - 1, 60_300, 4_000);
    differential.agree_diagram_rejection("S3 rich-descendant eviction", &usurper);
    assert!(
        differential.in_core_mempool(bumped.compute_txid()),
        "S3: the rich child survives in Core's mempool"
    );

    // S4 — replacing the same shape of pair with a compact transaction that
    // out-pays the whole cluster improves the diagram everywhere.
    let parent = build_spend(fundings[3], FUNDING_SATS, 300, 0);
    assert!(differential.agree("S4 low-rate parent", &parent));
    let bumped = build_spend(
        OutPoint::new(parent.compute_txid(), 0),
        FUNDING_SATS - 300,
        50_000,
        0,
    );
    assert!(differential.agree("S4 rich child", &bumped));
    let compact = build_spend(fundings[3], FUNDING_SATS - 1, 60_300, 0);
    assert!(differential.agree("S4 whole-cluster bump", &compact));
    assert!(
        !differential.in_core_mempool(bumped.compute_txid()),
        "S4: the evicted child leaves Core's mempool"
    );

    // S5 — equal total fee never replaces.
    let original = build_spend(fundings[4], FUNDING_SATS, 10_000, 0);
    assert!(differential.agree("S5 original", &original));
    let equal = build_spend(fundings[4], FUNDING_SATS - 1, 10_000, 0);
    assert!(!differential.agree("S5 equal-fee replacement", &equal));

    // S6 — a bump below the incremental relay floor never replaces.
    let original = build_spend(fundings[5], FUNDING_SATS, 10_000, 0);
    assert!(differential.agree("S6 original", &original));
    let trickle = build_spend(fundings[5], FUNDING_SATS - 1, 10_001, 0);
    assert!(!differential.agree("S6 sub-incremental bump", &trickle));
}
