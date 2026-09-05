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
        Self::start_with_args(bitcoind, &[])
    }

    fn start_with_args(bitcoind: &Path, extra_args: &[&str]) -> Self {
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

    /// Like [`Self::rpc`], but passes `lines` as arguments through
    /// `bitcoin-cli -stdin`, sidestepping the platform command-line length
    /// limit for large transaction payloads.
    fn rpc_with_stdin(&self, arguments: &[&str], lines: &[&str]) -> Result<String, String> {
        use std::io::Write;
        let mut child = Command::new(&self.cli)
            .args(["-regtest", "-rpcclienttimeout=15", "-stdin"])
            .arg(format!("-datadir={}", self.data_dir.display()))
            .arg(format!("-rpcport={}", self.rpc_port))
            .args(arguments)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|error| error.to_string())?;
        child
            .stdin
            .take()
            .expect("stdin is piped")
            .write_all(format!("{}\n", lines.join("\n")).as_bytes())
            .map_err(|error| error.to_string())?;
        let output = child
            .wait_with_output()
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

/// Builds a v3 (TRUC) spend splitting the change across `outputs` equal
/// anyone-can-spend outputs.
fn build_spend_v3(
    previous: OutPoint,
    input_sats: u64,
    fee_sats: u64,
    outputs: usize,
) -> Transaction {
    let share = (input_sats - fee_sats) / u64::try_from(outputs).unwrap();
    Transaction {
        version: Version(3),
        lock_time: LockTime::ZERO,
        input: vec![rbf_input(previous)],
        output: (0..outputs)
            .map(|_| TxOut {
                value: Amount::from_sat(share),
                script_pubkey: op_true_script(),
            })
            .collect(),
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
    let mut differential = Differential::set_up(7);
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

    // S7 — TRUC sibling eviction: a second v3 child of a one-child v3
    // parent must displace its sibling through the full replacement rules,
    // with no BIP125 signaling anywhere (v3 is implicitly replaceable), or
    // be refused.
    let parent = build_spend_v3(fundings[6], FUNDING_SATS, 10_000, 2);
    assert!(differential.agree("S7 v3 parent", &parent));
    let share = parent.output[0].value.to_sat();
    let sibling = build_spend_v3(OutPoint::new(parent.compute_txid(), 0), share, 5_000, 1);
    assert!(differential.agree("S7 first child", &sibling));
    let cheap = build_spend_v3(OutPoint::new(parent.compute_txid(), 1), share, 3_000, 1);
    assert!(
        !differential.agree("S7 worse second child", &cheap),
        "a worse-paying sibling never displaces the incumbent"
    );
    let rich = build_spend_v3(OutPoint::new(parent.compute_txid(), 1), share, 12_000, 1);
    assert!(differential.agree("S7 better second child", &rich));
    assert!(
        !differential.in_core_mempool(sibling.compute_txid()),
        "S7: the evicted sibling leaves Core's mempool"
    );
    assert!(
        !differential
            .pool
            .snapshot()
            .iter()
            .any(|transaction| transaction.compute_txid() == sibling.compute_txid()),
        "S7: the evicted sibling leaves rBTC's pool"
    );
}

/// Distinguishes the rich-parent package-feerate question under real
/// rolling-minimum pressure: with the mempool minimum raised above the
/// child's own feerate, an above-minimum parent must not subsidise a
/// below-minimum child, while a below-minimum parent may be lifted by a
/// rich child. Both mempools are pressured with the same ascending-feerate
/// filler stream; the scenario fees sit far from either implementation's
/// exact minimum so the verdicts do not depend on the minimums matching
/// numerically.
#[test]
#[ignore = "set RBTC_BITCOIND to a Bitcoin Core 31 bitcoind and run explicitly"]
#[allow(clippy::too_many_lines)]
fn core_31_and_rbtc_agree_on_package_feerate_under_pressure() {
    const FILLERS: usize = 90;
    const FILLER_INPUT_SATS: u64 = 20_000_000;
    let core = CoreNode::start_with_args(&core_31_bitcoind(), &["-maxmempool=5"]);
    core.rpc(&["createwallet", "pressure"]).unwrap();
    let miner = core.rpc(&["getnewaddress"]).unwrap();
    core.rpc(&["generatetoaddress", "101", &miner]).unwrap();

    let script = op_true_script();
    let address = bitcoin::Address::from_script(&script, Network::Regtest).unwrap();
    // The wallet funds one large anyone-can-spend seed; a hand-built
    // fan-out then creates every scenario output in a single transaction,
    // sidestepping wallet ancestor-depth and duplicate-address limits.
    let seed_txid = core
        .rpc(&["sendtoaddress", &address.to_string(), "20"])
        .unwrap();
    let raw = core.rpc(&["getrawtransaction", &seed_txid]).unwrap();
    let seed: Transaction = deserialize(&Vec::<u8>::from_hex(&raw).unwrap()).unwrap();
    let seed_vout = seed
        .output
        .iter()
        .position(|output| output.script_pubkey == script)
        .unwrap();
    let fan = Transaction {
        version: Version::TWO,
        lock_time: LockTime::ZERO,
        input: vec![rbf_input(OutPoint::new(
            seed.compute_txid(),
            u32::try_from(seed_vout).unwrap(),
        ))],
        output: (0..FILLERS + 4)
            .map(|_| TxOut {
                value: Amount::from_sat(FILLER_INPUT_SATS),
                script_pubkey: script.clone(),
            })
            .collect(),
    };
    core.rpc(&["sendrawtransaction", &serialize_hex(&fan), "0"])
        .unwrap();
    let fundings = (0..FILLERS + 4)
        .map(|vout| OutPoint::new(fan.compute_txid(), u32::try_from(vout).unwrap()))
        .collect::<Vec<_>>();
    core.rpc(&["generatetoaddress", "1", &miner]).unwrap();

    let store_dir = TempDir::new().unwrap();
    let store = RedbUtxoStore::open(store_dir.path().join("chainstate.redb")).unwrap();
    let seeded = fundings
        .iter()
        .map(|outpoint| {
            (
                OutPointKey::from(*outpoint),
                Utxo {
                    value_sats: FILLER_INPUT_SATS,
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
    // A pool small enough that the identical filler stream forces
    // evictions and raises the rolling minimum, as Core's 5 MB cap does.
    let mut pool = TransactionAdmissionPool::with_capacity(10_000, 4_500_000);

    // Fill both mempools with identical ~95 kvB fillers at ascending
    // feerates until both minimums clear 2 sat/vB.
    let mut raised = false;
    for (index, funding) in fundings.iter().take(FILLERS).enumerate() {
        let rate = 2 + 2 * u64::try_from(index).unwrap();
        let filler = build_spend(*funding, FILLER_INPUT_SATS, 60_500 * rate, 60_000);
        core.rpc_with_stdin(&["sendrawtransaction"], &[&serialize_hex(&filler), "0"])
            .unwrap();
        pool.admit(&store, filler, context()).unwrap();
        let info = core.rpc(&["getmempoolinfo"]).unwrap();
        let core_min_btc_kvb = info
            .split("\"mempoolminfee\":")
            .nth(1)
            .and_then(|tail| tail.split(',').next())
            .and_then(|value| value.trim().parse::<f64>().ok())
            .unwrap();
        let rbtc_min_sat_kvb = pool.rolling_minimum_fee_sat_kvb(0);
        if index % 10 == 9 {
            println!(
                "filler {}: core {core_min_btc_kvb} BTC/kvB, rbtc {rbtc_min_sat_kvb} sat/kvB",
                index + 1
            );
        }
        if core_min_btc_kvb > 0.000_02 && rbtc_min_sat_kvb > 2_000 {
            println!(
                "pressure reached after {} fillers: core {core_min_btc_kvb} BTC/kvB, rbtc {rbtc_min_sat_kvb} sat/kvB",
                index + 1
            );
            raised = true;
            break;
        }
    }
    assert!(raised, "the filler stream must raise both rolling minimums");
    // Scenario fees derive from the measured minimums, so the verdicts do
    // not depend on the two implementations raising identical values: the
    // rich transactions pay four times the higher minimum, and the poor
    // ones ~1 sat/vB, below both.
    let info = core.rpc(&["getmempoolinfo"]).unwrap();
    let core_min_btc_kvb = info
        .split("\"mempoolminfee\":")
        .nth(1)
        .and_then(|tail| tail.split(',').next())
        .and_then(|value| value.trim().parse::<f64>().ok())
        .unwrap();
    // The float is Core's own JSON rendering of an integer satoshi rate;
    // the product is far below 2^53, so the cast is exact.
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let core_min_sat_kvb = (core_min_btc_kvb * 100_000_000.0).round() as u64;
    let ceiling_sat_kvb = core_min_sat_kvb.max(pool.rolling_minimum_fee_sat_kvb(0));
    assert!(
        (2_000..1_000_000).contains(&ceiling_sat_kvb),
        "the raised minimums stay in a range the funding can outbid: {ceiling_sat_kvb}"
    );
    let rich_fee = |vsize: usize| ceiling_sat_kvb * 4 * u64::try_from(vsize).unwrap() / 1_000;
    let probe = build_spend(fundings[FILLERS], FILLER_INPUT_SATS, 10_000, 0);
    let scenario_vsize = probe.vsize();

    // Scenario A — rich parent (four times the higher minimum), poor
    // child (~1 sat/vB, below both minimums). The parent must not subsidise the
    // child: the child stays out of both mempools. Core admits the parent
    // individually inside package evaluation, while rBTC's package
    // admission is deliberately atomic and refuses the pair; resubmitting
    // the parent alone must then succeed, which pins the same per-
    // transaction outcome (parent in, child out) on both sides.
    let rich_parent = build_spend(
        fundings[FILLERS],
        FILLER_INPUT_SATS,
        rich_fee(scenario_vsize),
        0,
    );
    let poor_child = build_spend(
        OutPoint::new(rich_parent.compute_txid(), 0),
        rich_parent.output[0].value.to_sat(),
        120,
        0,
    );
    let package = format!(
        "[\"{}\",\"{}\"]",
        serialize_hex(&rich_parent),
        serialize_hex(&poor_child)
    );
    core.rpc(&["submitpackage", &package]).ok();
    assert!(
        core.rpc(&["getmempoolentry", &rich_parent.compute_txid().to_string()])
            .is_ok(),
        "A: Core admits the above-minimum parent"
    );
    assert!(
        core.rpc(&["getmempoolentry", &poor_child.compute_txid().to_string()])
            .is_err(),
        "A: Core refuses the below-minimum child despite its rich parent"
    );
    let package_verdict = pool.admit_package(
        &store,
        vec![rich_parent.clone(), poor_child.clone()],
        context(),
    );
    assert!(
        package_verdict.is_err(),
        "A: rBTC's atomic package refuses the pair because the rich parent \
         cannot subsidise the child"
    );
    pool.admit(&store, rich_parent.clone(), context())
        .expect("A: the above-minimum parent stands alone in rBTC too");
    assert!(
        pool.admit(&store, poor_child, context()).is_err(),
        "A: the below-minimum child stays out of rBTC"
    );

    // Scenario B — poor parent (~1 sat/vB, below both minimums), rich
    // child paying eight times the higher minimum, lifting the pair: the 1p1c bump admits both,
    // on both sides.
    let poor_parent = build_spend(fundings[FILLERS + 1], FILLER_INPUT_SATS, 120, 0);
    let rich_child = build_spend(
        OutPoint::new(poor_parent.compute_txid(), 0),
        poor_parent.output[0].value.to_sat(),
        rich_fee(scenario_vsize) * 2,
        0,
    );
    let package = format!(
        "[\"{}\",\"{}\"]",
        serialize_hex(&poor_parent),
        serialize_hex(&rich_child)
    );
    core.rpc(&["submitpackage", &package]).unwrap();
    assert!(
        core.rpc(&["getmempoolentry", &poor_parent.compute_txid().to_string()])
            .is_ok(),
        "B: Core admits the lifted parent"
    );
    assert!(
        core.rpc(&["getmempoolentry", &rich_child.compute_txid().to_string()])
            .is_ok(),
        "B: Core admits the lifting child"
    );
    let outcome = pool
        .admit_package(
            &store,
            vec![poor_parent.clone(), rich_child.clone()],
            context(),
        )
        .expect("B: rBTC's 1p1c package feerate lifts the poor parent");
    assert_eq!(outcome.accepted.len(), 2);
    println!("pressure differential: A (no subsidy) and B (1p1c lift) agree per transaction");
}
