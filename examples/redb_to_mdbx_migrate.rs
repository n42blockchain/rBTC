//! Migrate a full Redb chainstate UTXO set into the experimental MDBX backend.
//!
//! Use with:
//!   cargo run --locked --release --example redb_to_mdbx_migrate --features mdbx -- \
//!     --source /path/to/chainstate.redb \
//!     --target /path/to/mdbx-chainstate \
//!     --batch-size 20000

#[cfg(feature = "mdbx")]
use std::env;
#[cfg(feature = "mdbx")]
use std::error::Error;
#[cfg(feature = "mdbx")]
use std::fmt::Write as _;
#[cfg(feature = "mdbx")]
use std::fs::{self, File};
#[cfg(feature = "mdbx")]
use std::io::Write as _;
#[cfg(feature = "mdbx")]
use std::path::{Path, PathBuf};
#[cfg(feature = "mdbx")]
use std::time::Instant;

#[cfg(feature = "mdbx")]
use rbtc::mdbx_utxo::MdbxUtxoStore;
#[cfg(feature = "mdbx")]
use rbtc::utxo::{OutPointKey, RedbUtxoStore, UtxoStore};
#[cfg(feature = "mdbx")]
use serde_json::json;

#[cfg(feature = "mdbx")]
fn usage() -> String {
    "usage: redb_to_mdbx_migrate --source REDB_PATH --target MDBX_DIR \
--batch-size N --report PATH [--overwrite] [--verify]"
        .to_owned()
}

#[cfg(feature = "mdbx")]
#[derive(Clone)]
struct Config {
    source: PathBuf,
    target: PathBuf,
    batch_size: usize,
    overwrite: bool,
    verify: bool,
    report_path: Option<PathBuf>,
}

#[cfg(feature = "mdbx")]
fn parse_args() -> Result<Config, String> {
    let mut source = None;
    let mut target = None;
    let mut batch_size = 20_000usize;
    let mut overwrite = false;
    let mut verify = false;
    let mut report_path = None;

    let args = env::args().skip(1).collect::<Vec<_>>();
    let mut index = 0usize;
    while index < args.len() {
        let argument = &args[index];
        match argument.as_str() {
            "--source" => {
                let value = args.get(index + 1).ok_or("--source requires a path")?;
                source = Some(PathBuf::from(value));
                index += 2;
            }
            "--target" => {
                let value = args.get(index + 1).ok_or("--target requires a path")?;
                target = Some(PathBuf::from(value));
                index += 2;
            }
            "--batch-size" => {
                batch_size = args
                    .get(index + 1)
                    .ok_or("--batch-size requires a positive integer")?
                    .parse::<usize>()
                    .map_err(|_| "--batch-size must be a positive integer")?;
                if batch_size == 0 {
                    return Err("--batch-size must be greater than zero".to_owned());
                }
                index += 2;
            }
            "--report" => {
                report_path = Some(
                    args.get(index + 1)
                        .map(PathBuf::from)
                        .ok_or("--report requires a path")?,
                );
                index += 2;
            }
            "--overwrite" => {
                overwrite = true;
                index += 1;
            }
            "--verify" => {
                verify = true;
                index += 1;
            }
            "--help" => return Err(usage()),
            _ => return Err(format!("unknown argument '{argument}'\n{}", usage())),
        }
    }

    let source = source.ok_or_else(|| format!("--source is required\n{}", usage()))?;
    let target = target.ok_or_else(|| format!("--target is required\n{}", usage()))?;
    Ok(Config {
        source,
        target,
        batch_size,
        overwrite,
        verify,
        report_path,
    })
}

#[cfg(feature = "mdbx")]
fn dir_bytes(path: &Path) -> std::io::Result<u64> {
    let mut total = 0u64;
    let mut stack = vec![PathBuf::from(path)];
    while let Some(current) = stack.pop() {
        for entry in fs::read_dir(current)? {
            let entry = entry?;
            let metadata = entry.metadata()?;
            if metadata.is_file() {
                total += metadata.len();
            } else if metadata.is_dir() {
                stack.push(entry.path());
            }
        }
    }
    Ok(total)
}

#[cfg(feature = "mdbx")]
fn count_redb(store: &RedbUtxoStore, page_size: usize) -> Result<u64, Box<dyn Error>> {
    let mut total = 0u64;
    let mut after = None::<OutPointKey>;
    loop {
        let page = store.snapshot_page(after, page_size)?;
        if page.is_empty() {
            break;
        }
        total += u64::try_from(page.len())?;
        after = page.last().map(|row| row.0);
        if page.len() < page_size {
            break;
        }
    }
    Ok(total)
}

#[cfg(feature = "mdbx")]
fn migration_start(config: &Config) -> Result<(), Box<dyn Error>> {
    if config.target.exists() && !config.overwrite {
        let contains_files = fs::read_dir(&config.target)?.next().is_some();
        if contains_files {
            return Err(format!(
                "target exists and is not empty: {}",
                config.target.display()
            )
            .into());
        }
    }
    if config.target.exists() {
        fs::remove_dir_all(&config.target)?;
    }
    fs::create_dir_all(&config.target)?;

    let redb_store = RedbUtxoStore::open(&config.source)?;
    let mdbx_store = MdbxUtxoStore::open(&config.target)?;

    let start = Instant::now();
    let mut transferred = 0u64;
    let mut batch: Vec<(OutPointKey, rbtc::utxo::Utxo)> = Vec::new();
    let mut after = None::<OutPointKey>;
    loop {
        batch.clear();
        let page = redb_store.snapshot_page(after, config.batch_size)?;
        let page_len = page.len();
        if page.is_empty() {
            break;
        }
        batch.extend(page);
        if batch.is_empty() {
            break;
        }
        mdbx_store.apply(&[], &batch)?;
        transferred += u64::try_from(batch.len())?;
        after = Some(batch.last().unwrap().0);
        if transferred % u64::try_from(config.batch_size)? == 0 {
            eprintln!(
                "migrated {} entries, elapsed_ms={}",
                transferred,
                start.elapsed().as_millis()
            );
        }
        if page_len < config.batch_size {
            break;
        }
    }

    let elapsed = start.elapsed();
    let redb_bytes = fs::metadata(&config.source)?.len();
    let mdbx_bytes = dir_bytes(&config.target)?;
    let mdbx_stats = mdbx_store.tier_stats()?;
    let source_count = if config.verify {
        Some(count_redb(&redb_store, config.batch_size)?)
    } else {
        None
    };

    let mut report = json!({
        "source_path": config.source,
        "target_path": config.target,
        "batch_size": config.batch_size,
        "migrated_records": transferred,
        "elapsed_seconds": elapsed.as_secs_f64(),
        "source_redb_bytes": redb_bytes,
        "target_mdbx_bytes": mdbx_bytes,
        "target_mdbx_hot": mdbx_stats.hot,
        "target_mdbx_cold": mdbx_stats.cold,
    });
    if let Some(source_count) = source_count {
        report["source_snapshot_count"] = serde_json::Value::from(source_count);
    }

    let report_text = serde_json::to_string_pretty(&report)?;
    if let Some(path) = config.report_path.as_ref() {
        let mut file = File::create(path)?;
        file.write_all(report_text.as_bytes())?;
        file.write_all(b"\n")?;
    }
    let mut out = String::new();
    let _ = writeln!(&mut out, "{report_text}");
    println!("{out}");
    Ok(())
}

#[cfg(feature = "mdbx")]
fn main() -> Result<(), Box<dyn Error>> {
    let config = parse_args().map_err(|error| error.clone())?;
    migration_start(&config)?;
    Ok(())
}

#[cfg(not(feature = "mdbx"))]
fn main() {
    eprintln!("Build this example with --features mdbx");
    std::process::exit(1);
}
