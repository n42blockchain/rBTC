//! Reports per-table page and entry counts from an MDBX overlay environment.
//!
//! The MDBX counterpart of `redb_inspect`. It answers where an overlay's
//! bytes actually went — which table holds them, and how that compares to the
//! file's own high-water mark, the figure the geometry ceiling enforces.
//!
//! Opened read-only, so it is safe to point at a running node's environment.
//!
//! Usage: mdbx_inspect <overlay-database-dir>
//!
//! The percentages it prints are human-facing statistics.
#![allow(clippy::cast_precision_loss)]

#[cfg(not(feature = "mdbx"))]
fn main() {
    eprintln!("this tool requires the `mdbx` feature");
    std::process::exit(1);
}

#[cfg(feature = "mdbx")]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    use std::{env, path::PathBuf};

    use libmdbx::{Database, DatabaseOptions, Mode, NoWriteMap};

    let directory = PathBuf::from(
        env::args()
            .nth(1)
            .ok_or("usage: mdbx_inspect <overlay-database-dir>")?,
    );

    let database: Database<NoWriteMap> = Database::open_with_options(
        &directory,
        DatabaseOptions {
            max_tables: Some(8),
            mode: Mode::ReadOnly,
            ..DatabaseOptions::default()
        },
    )?;

    let transaction = database.begin_ro_txn()?;
    let environment = database.stat()?;
    let page_bytes = u64::from(environment.page_size());
    let info = database.info()?;

    println!("directory: {}", directory.display());
    println!("page_size: {page_bytes}");
    println!(
        "last_pgno (high-water mark): {} pages = {} bytes",
        info.last_pgno(),
        u64::try_from(info.last_pgno())? * page_bytes
    );
    println!("map_size: {} bytes", info.map_size());
    println!();

    let mut total_pages = 0_u64;
    for name in ["utxo_overlay", "utxo_spent_base", "block_undos", "meta"] {
        let table = match transaction.open_table(Some(name)) {
            Ok(table) => table,
            Err(error) => {
                println!("{name:<18} unavailable: {error}");
                continue;
            }
        };
        let stat = transaction.table_stat(&table)?;
        let pages = u64::try_from(stat.leaf_pages() + stat.branch_pages() + stat.overflow_pages())?;
        total_pages += pages;
        println!(
            "{name:<18} entries={:<12} pages={:<10} bytes={:<14} depth={}",
            stat.entries(),
            pages,
            pages * page_bytes,
            stat.depth()
        );
    }

    let accounted = total_pages * page_bytes;
    let high_water = u64::try_from(info.last_pgno())? * page_bytes;
    println!();
    println!("table bytes total: {accounted}");
    println!(
        "high-water mark:   {high_water} ({:.1}% accounted for by live tables)",
        if high_water == 0 {
            0.0
        } else {
            accounted as f64 * 100.0 / high_water as f64
        }
    );
    Ok(())
}
