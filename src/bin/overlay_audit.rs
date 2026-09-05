//! Hashes the logical content of a snapshot-overlay chainstate so two overlays
//! built by different engines, batch sizes or write-back settings can be
//! compared with one string.
//!
//! The store is opened read-only and scanned in full; the base snapshot is
//! not needed.  Output is JSON on stdout (and optionally to a file).
//!
//! Usage:
//!   overlay_audit --engine mdbx --database DATA_DIR/overlay-mdbx [--capacity-bytes N] [--out FILE]
//!   overlay_audit --engine redb --database DATA_DIR/overlay.redb [--out FILE]

use std::{env, fs::File, io::Write, path::PathBuf, process, time::Instant};

use rbtc::snapshot_overlay::{OverlayContentAudit, SnapshotOverlayChainstate};
use rbtc::snapshot_overlay_redb::SnapshotOverlayRedbChainstate;

const DEFAULT_CAPACITY_BYTES: u64 = 10 * 1024 * 1024 * 1024;

fn usage() -> ! {
    eprintln!(
        "usage: overlay_audit --engine mdbx|redb --database PATH [--capacity-bytes N] [--out FILE]"
    );
    process::exit(2);
}

fn main() {
    let mut engine = None;
    let mut database = None;
    let mut capacity = DEFAULT_CAPACITY_BYTES;
    let mut out = None;
    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        let mut value = || args.next().unwrap_or_else(|| usage());
        match arg.as_str() {
            "--engine" => engine = Some(value()),
            "--database" => database = Some(PathBuf::from(value())),
            "--capacity-bytes" => capacity = value().parse().unwrap_or_else(|_| usage()),
            "--out" => out = Some(PathBuf::from(value())),
            _ => usage(),
        }
    }
    let (Some(engine), Some(database)) = (engine, database) else {
        usage()
    };

    let started = Instant::now();
    let audit = match engine.as_str() {
        "mdbx" => SnapshotOverlayChainstate::audit_content(&database, capacity),
        "redb" => SnapshotOverlayRedbChainstate::audit_content(&database),
        _ => usage(),
    };
    let audit = match audit {
        Ok(audit) => audit,
        Err(error) => {
            eprintln!("overlay_audit: {error}");
            process::exit(1);
        }
    };
    let report = render(&database, &audit, started.elapsed().as_secs_f64());
    print!("{report}");
    if let Some(path) = out {
        if let Err(error) = File::create(&path).and_then(|mut f| f.write_all(report.as_bytes())) {
            eprintln!("overlay_audit: write {}: {error}", path.display());
            process::exit(1);
        }
    }
}

fn render(database: &std::path::Path, audit: &OverlayContentAudit, seconds: f64) -> String {
    let mut out = String::new();
    out.push_str(
        "{
",
    );
    out.push_str(&format!(
        "  \"database\": {:?},
",
        database.display().to_string()
    ));
    out.push_str(&format!(
        "  \"engine\": \"{}\",
",
        audit.engine
    ));
    out.push_str(&format!(
        "  \"base_height\": {},
",
        audit.identity.height
    ));
    out.push_str(&format!(
        "  \"base_hash\": \"{}\",
",
        audit.identity.block_hash
    ));
    out.push_str(&format!(
        "  \"base_hash_serialized\": \"{}\",
",
        audit.identity.hash_serialized
    ));
    out.push_str(&format!(
        "  \"tip_height\": {},
",
        audit.tip.height
    ));
    out.push_str(&format!(
        "  \"tip_hash\": \"{}\",
",
        audit.tip.hash
    ));
    out.push_str(&format!(
        "  \"overlay_entries\": {},
",
        audit.overlay_entries
    ));
    out.push_str(&format!(
        "  \"overlay_key_bytes\": {},
",
        audit.overlay_key_bytes
    ));
    out.push_str(&format!(
        "  \"overlay_value_bytes\": {},
",
        audit.overlay_value_bytes
    ));
    out.push_str(&format!(
        "  \"overlay_sha256\": \"{}\",
",
        audit.overlay_sha256
    ));
    out.push_str(&format!(
        "  \"overlay_raw_sha256\": \"{}\",
",
        audit.overlay_raw_sha256
    ));
    out.push_str(&format!(
        "  \"tombstone_entries\": {},
",
        audit.tombstone_entries
    ));
    out.push_str(&format!(
        "  \"tombstone_sha256\": \"{}\",
",
        audit.tombstone_sha256
    ));
    out.push_str(&format!(
        "  \"undo_entries\": {},
",
        audit.undo_entries
    ));
    out.push_str(&format!(
        "  \"content_sha256\": \"{}\",
",
        audit.content_sha256
    ));
    out.push_str(&format!(
        "  \"seconds\": {seconds:.3}
"
    ));
    out.push_str(
        "}
",
    );
    out
}
