//! Command line entry point for the rBTC node daemon.

use std::{
    env, fs,
    net::SocketAddr,
    path::PathBuf,
    process,
    str::FromStr,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use bitcoin::{BlockHash, Network, hashes::Hash};
use rbtc::{
    block_execution::{BlockDeploymentContext, connect_active_block},
    execution_store::RedbExecutionStore,
    header_store::RedbHeaderStore,
    headers::HeaderDag,
    p2p::{MAX_HEADERS_PER_RESPONSE, connect_outbound},
    undo_store::RedbUndoStore,
    utxo::{DEFAULT_HOT_WINDOW_SECS, RedbUtxoStore},
};
use tokio::time::timeout;

const PEER_TIMEOUT: Duration = Duration::from_secs(30);
const USER_AGENT: &str = "/rbtcd:0.1.0/";

struct Options {
    remote: SocketAddr,
    network: Network,
    fetch_block: Option<BlockHash>,
    headers_db: Option<PathBuf>,
    data_dir: Option<PathBuf>,
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    match parse_options(env::args().skip(1)) {
        Ok(Some(options)) => {
            if let Err(error) = run(options).await {
                eprintln!("rbtcd: {error}");
                process::exit(1);
            }
        }
        Ok(None) => print_usage(),
        Err(error) => {
            eprintln!("rbtcd: {error}\n");
            print_usage();
            process::exit(2);
        }
    }
}

async fn run(options: Options) -> Result<(), String> {
    let mut session = timeout(
        PEER_TIMEOUT,
        connect_outbound(
            options.remote,
            options.network.magic(),
            rand::random(),
            USER_AGENT.to_owned(),
            0,
        ),
    )
    .await
    .map_err(|_| {
        format!(
            "peer handshake timed out after {} seconds",
            PEER_TIMEOUT.as_secs()
        )
    })?
    .map_err(|error| error.to_string())?;

    let remote = session.remote_version();
    println!(
        "connected to {}: version={}, height={}, agent={}",
        options.remote, remote.version, remote.start_height, remote.user_agent
    );

    if let Some(hash) = options.fetch_block {
        timeout(PEER_TIMEOUT, session.request_witness_blocks(&[hash]))
            .await
            .map_err(|_| "getdata request timed out".to_owned())?
            .map_err(|error| error.to_string())?;
        let block = timeout(PEER_TIMEOUT, session.receive_requested_block(hash))
            .await
            .map_err(|_| {
                format!(
                    "block response timed out after {} seconds",
                    PEER_TIMEOUT.as_secs()
                )
            })?
            .map_err(|error| error.to_string())?;
        println!(
            "received block {} ({} transactions); not applied: IBD chainstate integration is pending",
            block.block_hash(),
            block.txdata.len()
        );
        return Ok(());
    }

    if let Some(path) = options.data_dir {
        return sync_regtest_node(&mut session, options.network, path).await;
    }

    if let Some(path) = options.headers_db {
        sync_headers(&mut session, options.network, path).await?;
        return Ok(());
    }

    let genesis = bitcoin::blockdata::constants::genesis_block(options.network).block_hash();
    request_headers(&mut session, vec![genesis]).await?;
    let headers = receive_headers(&mut session).await?;
    println!(
        "received {} headers; pass --headers-db PATH to validate, persist, and continue IBD",
        headers.len()
    );
    Ok(())
}

async fn sync_headers(
    session: &mut rbtc::p2p::PeerSession<tokio::net::TcpStream>,
    network: Network,
    path: PathBuf,
) -> Result<HeaderDag, String> {
    let store = RedbHeaderStore::open(path).map_err(|error| error.to_string())?;
    let mut dag = store
        .load_dag(network, unix_time()?)
        .map_err(|error| error.to_string())?;
    println!(
        "resuming headers-first sync from height {} (local-clock fallback until network-time aggregation lands)",
        dag.active_tip().height
    );

    loop {
        request_headers(session, dag.block_locator()).await?;
        let headers = receive_headers(session).await?;
        let count = headers.len();
        if count == 0 {
            break;
        }
        let (candidate, _) = dag
            .validate_batch_contextual(&headers, unix_time()?)
            .map_err(|error| error.to_string())?;
        store
            .append_batch(&headers)
            .map_err(|error| error.to_string())?;
        dag = candidate;
        println!(
            "validated and persisted {count} headers; active height {}",
            dag.active_tip().height
        );
        if count < MAX_HEADERS_PER_RESPONSE {
            break;
        }
    }
    println!(
        "headers sync complete at height {}",
        dag.active_tip().height
    );
    Ok(dag)
}

async fn sync_regtest_node(
    session: &mut rbtc::p2p::PeerSession<tokio::net::TcpStream>,
    network: Network,
    data_dir: PathBuf,
) -> Result<(), String> {
    if network != Network::Regtest {
        return Err(
            "block execution is currently safety-gated to regtest until deployment activation is complete"
                .to_owned(),
        );
    }
    fs::create_dir_all(&data_dir)
        .map_err(|error| format!("create data directory {}: {error}", data_dir.display()))?;
    let headers = sync_headers(session, network, data_dir.join("headers.redb")).await?;
    let chainstate =
        RedbUtxoStore::open(data_dir.join("chainstate.redb")).map_err(|error| error.to_string())?;
    let undo_store =
        RedbUndoStore::open(data_dir.join("undo.redb")).map_err(|error| error.to_string())?;
    let execution_store = RedbExecutionStore::open(data_dir.join("execution.redb"), network)
        .map_err(|error| error.to_string())?;

    loop {
        let tip = execution_store.tip().map_err(|error| error.to_string())?;
        if tip.height >= headers.active_tip().height {
            println!("block execution caught up at height {}", tip.height);
            return Ok(());
        }
        let next_height = tip
            .height
            .checked_add(1)
            .ok_or_else(|| "execution height overflow".to_owned())?;
        let expected = headers
            .active_header_at(next_height)
            .ok_or_else(|| format!("missing active header at height {next_height}"))?;
        timeout(
            PEER_TIMEOUT,
            session.request_witness_blocks(&[expected.hash]),
        )
        .await
        .map_err(|_| format!("getdata timed out for block {}", expected.hash))?
        .map_err(|error| error.to_string())?;
        let block = timeout(PEER_TIMEOUT, session.receive_requested_block(expected.hash))
            .await
            .map_err(|_| format!("block response timed out for {}", expected.hash))?
            .map_err(|error| error.to_string())?;
        connect_active_block(
            &chainstate,
            &undo_store,
            &execution_store,
            &headers,
            &block,
            u64::from(unix_time()?),
            DEFAULT_HOT_WINDOW_SECS,
            BlockDeploymentContext::default(),
        )
        .map_err(|error| error.to_string())?;
        println!(
            "validated and executed block {next_height}:{}",
            expected.hash
        );
    }
}

async fn request_headers(
    session: &mut rbtc::p2p::PeerSession<tokio::net::TcpStream>,
    locator: Vec<BlockHash>,
) -> Result<(), String> {
    timeout(
        PEER_TIMEOUT,
        session.request_headers(locator, BlockHash::all_zeros()),
    )
    .await
    .map_err(|_| "getheaders request timed out".to_owned())?
    .map_err(|error| error.to_string())
}

async fn receive_headers(
    session: &mut rbtc::p2p::PeerSession<tokio::net::TcpStream>,
) -> Result<Vec<bitcoin::block::Header>, String> {
    timeout(PEER_TIMEOUT, session.receive_headers())
        .await
        .map_err(|_| {
            format!(
                "headers response timed out after {} seconds",
                PEER_TIMEOUT.as_secs()
            )
        })?
        .map_err(|error| error.to_string())
}

fn unix_time() -> Result<u32, String> {
    let seconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| "system clock is before the Unix epoch".to_owned())?
        .as_secs();
    u32::try_from(seconds)
        .map_err(|_| "system clock does not fit Bitcoin timestamp range".to_owned())
}

fn parse_options(args: impl Iterator<Item = String>) -> Result<Option<Options>, String> {
    let mut args = args.peekable();
    if args.peek().is_none() {
        return Ok(None);
    }

    let mut remote = None;
    let mut network = Network::Bitcoin;
    let mut fetch_block = None;
    let mut headers_db = None;
    let mut data_dir = None;
    while let Some(argument) = args.next() {
        match argument.as_str() {
            "--help" | "-h" => return Ok(None),
            "--connect" => {
                let address = args
                    .next()
                    .ok_or_else(|| "--connect requires HOST:PORT".to_owned())?;
                remote = Some(
                    address
                        .parse()
                        .map_err(|_| format!("invalid peer address: {address}"))?,
                );
            }
            "--network" => {
                let value = args
                    .next()
                    .ok_or_else(|| "--network requires a network name".to_owned())?;
                network = Network::from_str(&value)
                    .map_err(|_| format!("unsupported network: {value}"))?;
            }
            "--fetch-block" => {
                let value = args
                    .next()
                    .ok_or_else(|| "--fetch-block requires a block hash".to_owned())?;
                fetch_block = Some(
                    BlockHash::from_str(&value)
                        .map_err(|_| format!("invalid block hash: {value}"))?,
                );
            }
            "--headers-db" => {
                headers_db =
                    Some(PathBuf::from(args.next().ok_or_else(|| {
                        "--headers-db requires a file path".to_owned()
                    })?));
            }
            "--data-dir" => {
                data_dir =
                    Some(PathBuf::from(args.next().ok_or_else(|| {
                        "--data-dir requires a directory path".to_owned()
                    })?));
            }
            _ => return Err(format!("unknown option: {argument}")),
        }
    }

    let remote = remote.ok_or_else(|| "--connect is required".to_owned())?;
    Ok(Some(Options {
        remote,
        network,
        fetch_block,
        headers_db,
        data_dir,
    }))
}

fn print_usage() {
    println!(
        "rbtcd {}\n\nUSAGE:\n  rbtcd --connect HOST:PORT [--network bitcoin|testnet|testnet4|signet|regtest]\n  rbtcd --connect HOST:PORT --headers-db PATH [--network NETWORK]\n  rbtcd --connect HOST:PORT --data-dir PATH --network regtest\n  rbtcd --connect HOST:PORT --fetch-block BLOCK_HASH [--network NETWORK]",
        env!("CARGO_PKG_VERSION")
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use bitcoin::{
        Amount, Block, OutPoint, ScriptBuf, Sequence, Transaction, TxIn, TxMerkleNode, TxOut,
        Witness,
        absolute::LockTime,
        block::{Header, Version},
        p2p::{Address, ServiceFlags, message::NetworkMessage, message_network::VersionMessage},
        pow::Target,
        transaction::Version as TransactionVersion,
    };
    use rbtc::{
        blockchain::block_subsidy,
        execution_store::RedbExecutionStore,
        header_store::RedbHeaderStore,
        p2p::V1Transport,
        undo_store::RedbUndoStore,
        utxo::{OutPointKey, RedbUtxoStore, UtxoStore},
    };
    use tempfile::TempDir;
    use tokio::net::TcpListener;

    fn peer_version(nonce: u64) -> VersionMessage {
        let receiver: SocketAddr = "127.0.0.1:18444".parse().unwrap();
        let sender: SocketAddr = "0.0.0.0:0".parse().unwrap();
        VersionMessage::new(
            ServiceFlags::NONE,
            0,
            Address::new(&receiver, ServiceFlags::NONE),
            Address::new(&sender, ServiceFlags::NONE),
            nonce,
            "/rbtcd:test-peer/".to_owned(),
            1,
        )
    }

    fn mine_regtest_child(parent: BlockHash, time: u32) -> Header {
        let target = Target::MAX_ATTAINABLE_REGTEST;
        let mut header = Header {
            version: Version::ONE,
            prev_blockhash: parent,
            merkle_root: TxMerkleNode::all_zeros(),
            time,
            bits: target.to_compact_lossy(),
            nonce: 0,
        };
        while header.validate_pow(target).is_err() {
            header.nonce = header.nonce.checked_add(1).unwrap();
        }
        header
    }

    fn regtest_block(parent: BlockHash, time: u32) -> Block {
        let coinbase = Transaction {
            version: TransactionVersion::ONE,
            lock_time: LockTime::ZERO,
            input: vec![TxIn {
                previous_output: OutPoint::null(),
                script_sig: ScriptBuf::from_bytes(vec![1, 1]),
                sequence: Sequence::MAX,
                witness: Witness::default(),
            }],
            output: vec![TxOut {
                value: Amount::from_sat(block_subsidy(1)),
                script_pubkey: ScriptBuf::new(),
            }],
        };
        let mut block = Block {
            header: Header {
                version: Version::ONE,
                prev_blockhash: parent,
                merkle_root: TxMerkleNode::all_zeros(),
                time,
                bits: Target::MAX_ATTAINABLE_REGTEST.to_compact_lossy(),
                nonce: 0,
            },
            txdata: vec![coinbase],
        };
        block.header.merkle_root = block.compute_merkle_root().unwrap();
        while block
            .header
            .validate_pow(Target::MAX_ATTAINABLE_REGTEST)
            .is_err()
        {
            block.header.nonce = block.header.nonce.checked_add(1).unwrap();
        }
        block
    }

    #[test]
    fn parses_a_header_probe() {
        let options = parse_options(
            ["--connect", "127.0.0.1:18444", "--network", "regtest"]
                .into_iter()
                .map(str::to_owned),
        )
        .unwrap()
        .unwrap();
        assert_eq!(options.remote, "127.0.0.1:18444".parse().unwrap());
        assert_eq!(options.network, Network::Regtest);
        assert!(options.fetch_block.is_none());
        assert!(options.headers_db.is_none());
        assert!(options.data_dir.is_none());
    }

    #[tokio::test]
    async fn daemon_validates_and_persists_a_header_sync() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let remote = listener.local_addr().unwrap();
        let genesis = bitcoin::blockdata::constants::genesis_block(Network::Regtest).header;
        let child = mine_regtest_child(genesis.block_hash(), genesis.time + 1);
        let child_hash = child.block_hash();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut peer = V1Transport::new(stream, Network::Regtest.magic());
            assert!(matches!(
                peer.read_message().await.unwrap().into_payload(),
                NetworkMessage::Version(_)
            ));
            peer.write_message(NetworkMessage::Version(peer_version(5)))
                .await
                .unwrap();
            assert!(matches!(
                peer.read_message().await.unwrap().into_payload(),
                NetworkMessage::Verack
            ));
            peer.write_message(NetworkMessage::Verack).await.unwrap();
            match peer.read_message().await.unwrap().into_payload() {
                NetworkMessage::GetHeaders(request) => {
                    assert_eq!(request.locator_hashes, vec![genesis.block_hash()]);
                    peer.write_message(NetworkMessage::Headers(vec![child]))
                        .await
                        .unwrap();
                }
                message => panic!("expected first getheaders, got {message:?}"),
            }
        });

        let directory = TempDir::new().unwrap();
        let header_path = directory.path().join("headers.redb");
        run(Options {
            remote,
            network: Network::Regtest,
            fetch_block: None,
            headers_db: Some(header_path.clone()),
            data_dir: None,
        })
        .await
        .unwrap();
        server.await.unwrap();

        let store = RedbHeaderStore::open(header_path).unwrap();
        assert_eq!(
            store
                .load_dag(Network::Regtest, unix_time().unwrap())
                .unwrap()
                .active_tip()
                .hash,
            child_hash
        );
    }

    #[tokio::test]
    async fn daemon_downloads_executes_and_recovers_a_regtest_block() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let remote = listener.local_addr().unwrap();
        let genesis = bitcoin::blockdata::constants::genesis_block(Network::Regtest).header;
        let block = regtest_block(genesis.block_hash(), genesis.time + 1);
        let block_hash = block.block_hash();
        let coinbase_outpoint = OutPointKey::from(OutPoint::new(block.txdata[0].compute_txid(), 0));
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut peer = V1Transport::new(stream, Network::Regtest.magic());
            assert!(matches!(
                peer.read_message().await.unwrap().into_payload(),
                NetworkMessage::Version(_)
            ));
            peer.write_message(NetworkMessage::Version(peer_version(6)))
                .await
                .unwrap();
            assert!(matches!(
                peer.read_message().await.unwrap().into_payload(),
                NetworkMessage::Verack
            ));
            peer.write_message(NetworkMessage::Verack).await.unwrap();
            match peer.read_message().await.unwrap().into_payload() {
                NetworkMessage::GetHeaders(_) => peer
                    .write_message(NetworkMessage::Headers(vec![block.header]))
                    .await
                    .unwrap(),
                message => panic!("expected getheaders, got {message:?}"),
            }
            match peer.read_message().await.unwrap().into_payload() {
                NetworkMessage::GetData(inventory) => {
                    assert_eq!(
                        inventory,
                        vec![bitcoin::p2p::message_blockdata::Inventory::WitnessBlock(
                            block_hash
                        )]
                    );
                    peer.write_message(NetworkMessage::Block(block))
                        .await
                        .unwrap();
                }
                message => panic!("expected getdata, got {message:?}"),
            }
        });

        let directory = TempDir::new().unwrap();
        run(Options {
            remote,
            network: Network::Regtest,
            fetch_block: None,
            headers_db: None,
            data_dir: Some(directory.path().to_path_buf()),
        })
        .await
        .unwrap();
        server.await.unwrap();

        let execution =
            RedbExecutionStore::open(directory.path().join("execution.redb"), Network::Regtest)
                .unwrap();
        assert_eq!(execution.tip().unwrap().height, 1);
        assert_eq!(execution.tip().unwrap().hash, block_hash);
        let undo = RedbUndoStore::open(directory.path().join("undo.redb")).unwrap();
        assert!(undo.get(block_hash).unwrap().is_some());
        let chainstate = RedbUtxoStore::open(directory.path().join("chainstate.redb")).unwrap();
        assert!(chainstate.get(coinbase_outpoint).unwrap().is_some());
    }
}
