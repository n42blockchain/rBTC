//! Command line entry point for the rBTC node daemon.

fn main() {
    println!(
        "rbtcd {} — node kernel initialized; see `rbtcd --help` once the service layer lands.",
        env!("CARGO_PKG_VERSION")
    );
}
