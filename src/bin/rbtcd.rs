//! Thin command-line entry point for the embeddable rBTC node runtime.

use std::{env, process};

// Opt-in allocator: block execution, the script threads and the pipeline
// tail all allocate a script buffer per coin, and the system heap serialises
// them; mimalloc keeps per-thread heaps.
#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() {
    if let Err(error) = rbtc::node::run_cli(env::args().skip(1)).await {
        eprintln!("rbtcd: {error}");
        process::exit(error.exit_code());
    }
}
