//! Command line entry point for the rBTC node daemon.

use std::{env, net::SocketAddr, process, str::FromStr, time::Duration};

use bitcoin::{BlockHash, Network, hashes::Hash};
use rbtc::p2p::connect_outbound;
use tokio::time::timeout;

const PEER_TIMEOUT: Duration = Duration::from_secs(30);
const USER_AGENT: &str = "/rbtcd:0.1.0/";

struct Options {
    remote: SocketAddr,
    network: Network,
    fetch_block: Option<BlockHash>,
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

    let genesis = bitcoin::blockdata::constants::genesis_block(options.network).block_hash();
    timeout(
        PEER_TIMEOUT,
        session.request_headers(vec![genesis], BlockHash::all_zeros()),
    )
    .await
    .map_err(|_| "getheaders request timed out".to_owned())?
    .map_err(|error| error.to_string())?;
    let headers = timeout(PEER_TIMEOUT, session.receive_headers())
        .await
        .map_err(|_| {
            format!(
                "headers response timed out after {} seconds",
                PEER_TIMEOUT.as_secs()
            )
        })?
        .map_err(|error| error.to_string())?;
    println!(
        "received {} headers; contextual validation, persistent storage, and IBD scheduling are pending",
        headers.len()
    );
    Ok(())
}

fn parse_options(args: impl Iterator<Item = String>) -> Result<Option<Options>, String> {
    let mut args = args.peekable();
    if args.peek().is_none() {
        return Ok(None);
    }

    let mut remote = None;
    let mut network = Network::Bitcoin;
    let mut fetch_block = None;
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
            _ => return Err(format!("unknown option: {argument}")),
        }
    }

    let remote = remote.ok_or_else(|| "--connect is required".to_owned())?;
    Ok(Some(Options {
        remote,
        network,
        fetch_block,
    }))
}

fn print_usage() {
    println!(
        "rbtcd {}\n\nUSAGE:\n  rbtcd --connect HOST:PORT [--network bitcoin|testnet|testnet4|signet|regtest]\n  rbtcd --connect HOST:PORT --fetch-block BLOCK_HASH [--network NETWORK]",
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
    }
}
