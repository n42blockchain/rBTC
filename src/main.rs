//! Command line entry point for the rBTC node daemon.

use std::{
    env,
    net::SocketAddr,
    path::PathBuf,
    process,
    str::FromStr,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use bitcoin::{BlockHash, Network, hashes::Hash};
use rbtc::{
    header_store::RedbHeaderStore,
    p2p::{MAX_HEADERS_PER_RESPONSE, connect_outbound},
};
use tokio::time::timeout;

const PEER_TIMEOUT: Duration = Duration::from_secs(30);
const USER_AGENT: &str = "/rbtcd:0.1.0/";

struct Options {
    remote: SocketAddr,
    network: Network,
    fetch_block: Option<BlockHash>,
    headers_db: Option<PathBuf>,
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

    if let Some(path) = options.headers_db {
        return sync_headers(&mut session, options.network, path).await;
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
) -> Result<(), String> {
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
    Ok(())
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
            _ => return Err(format!("unknown option: {argument}")),
        }
    }

    let remote = remote.ok_or_else(|| "--connect is required".to_owned())?;
    Ok(Some(Options {
        remote,
        network,
        fetch_block,
        headers_db,
    }))
}

fn print_usage() {
    println!(
        "rbtcd {}\n\nUSAGE:\n  rbtcd --connect HOST:PORT [--network bitcoin|testnet|testnet4|signet|regtest]\n  rbtcd --connect HOST:PORT --headers-db PATH [--network NETWORK]\n  rbtcd --connect HOST:PORT --fetch-block BLOCK_HASH [--network NETWORK]",
        env!("CARGO_PKG_VERSION")
    );
}

#[cfg(test)]
mod tests {
    use super::*;

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
    }
}
