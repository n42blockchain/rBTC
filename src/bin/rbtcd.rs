//! Thin command-line entry point for the embeddable rBTC node runtime.

use std::{env, process};

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() {
    if let Err(error) = rbtc::node::run_cli(env::args().skip(1)).await {
        eprintln!("rbtcd: {error}");
        process::exit(error.exit_code());
    }
}
