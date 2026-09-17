//! Disk-candidate kernel probe, not node-wide resource acceptance.
//! Usage: cargo run --release --example header_candidate_probe -- 2100000
//! A fresh child process replays one frame per slice and checks the final tip.
use bitcoin::{
    Network, TxMerkleNode,
    block::{Header, Version},
    hashes::Hash,
    pow::Target,
};
use rbtc::{
    header_candidate::{DiskHeaderCandidate, HeaderCandidateLimits},
    headers::{HeaderDag, HeaderWorkBudget},
};
use std::{env, fs, path::Path, process::Command, time::Instant};

#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

fn rss_kib() -> Result<u64> {
    if cfg!(target_os = "macos") {
        let output = Command::new("ps")
            .args(["-o", "rss=", "-p", &std::process::id().to_string()])
            .output()?;
        if !output.status.success() {
            return Err("ps failed".into());
        }
        Ok(std::str::from_utf8(&output.stdout)?.trim().parse()?)
    } else {
        Ok(fs::read_to_string("/proc/self/status")?
            .lines()
            .find_map(|line| line.strip_prefix("VmRSS:"))
            .ok_or("missing RSS")?
            .split_whitespace()
            .next()
            .ok_or("empty RSS")?
            .parse()?)
    }
}

fn limits() -> HeaderCandidateLimits {
    HeaderCandidateLimits {
        max_file_bytes: 256 * 1024 * 1024,
        ..HeaderCandidateLimits::default()
    }
}

fn report(
    phase: &str,
    count: u64,
    context: Option<usize>,
    bytes: u64,
    started: Instant,
) -> Result<()> {
    println!(
        "{}",
        serde_json::json!({ "phase": phase, "pid": std::process::id(),
        "headers": count, "context_entries": context, "file_bytes": bytes,
        "rss_kib": rss_kib()?, "elapsed_micros": started.elapsed().as_micros() })
    );
    Ok(())
}

fn reopen(path: &Path, count: u32, expected_hash: &str, expected_work: &str) -> Result<()> {
    let started = Instant::now();
    let source = HeaderDag::new(Network::Regtest);
    report("before-replay", 0, None, fs::metadata(path)?.len(), started)?;
    let mut recovery = DiskHeaderCandidate::start_recovery(
        path,
        &source,
        source.active_tip().hash,
        limits(),
        &mut HeaderWorkBudget::default(),
    )?;
    loop {
        let done = recovery.advance(1, u32::MAX, &mut HeaderWorkBudget::default())?;
        if recovery.validated_headers() % 200_000 == 0 {
            report(
                "replay-slice",
                recovery.validated_headers(),
                None,
                fs::metadata(path)?.len(),
                started,
            )?;
        }
        if done {
            break;
        }
    }
    let candidate = recovery.finish()?;
    assert_eq!(candidate.len(), u64::from(count));
    assert_eq!(candidate.tip().height, count);
    assert_eq!(candidate.tip().hash.to_string(), expected_hash);
    assert_eq!(format!("{:?}", candidate.tip().chainwork), expected_work);
    assert!(candidate.resident_context_entries() <= 145);
    report(
        "fresh-replay-complete",
        candidate.len(),
        Some(candidate.resident_context_entries()),
        candidate.file_bytes(),
        started,
    )
}

fn mine(parent: Header) -> Header {
    let mut header = Header {
        version: Version::from_consensus(4),
        prev_blockhash: parent.block_hash(),
        merkle_root: TxMerkleNode::all_zeros(),
        time: parent.time + 1,
        bits: Target::MAX_ATTAINABLE_REGTEST.to_compact_lossy(),
        nonce: 0,
    };
    while header.validate_pow(header.target()).is_err() {
        header.nonce += 1;
    }
    header
}

fn main() -> Result<()> {
    let mut args = env::args().skip(1);
    let first = args.next().ok_or("expected header count")?;
    if first == "reopen" {
        let path = args.next().ok_or("expected path")?;
        let count = args.next().ok_or("expected count")?.parse()?;
        let hash = args.next().ok_or("expected tip")?;
        let work = args.next().ok_or("expected chainwork")?;
        if args.next().is_some() {
            return Err("extra arguments".into());
        }
        return reopen(Path::new(&path), count, &hash, &work);
    }
    let count: u32 = first.parse()?;
    if !(1..=2_500_000).contains(&count) || args.next().is_some() {
        return Err("count must be 1..2500000".into());
    }
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("candidate.headers");
    let source = HeaderDag::new(Network::Regtest);
    let mut candidate = DiskHeaderCandidate::open(
        &path,
        &source,
        source.active_tip().hash,
        u32::MAX,
        limits(),
        &mut HeaderWorkBudget::default(),
    )?;
    let started = Instant::now();
    let mut parent = source.active_tip().header;
    let mut batch = Vec::with_capacity(2000);
    for height in 1..=count {
        parent = mine(parent);
        batch.push(parent);
        if batch.len() == 2000 || height == count {
            candidate.append(&batch, u32::MAX, &mut HeaderWorkBudget::default())?;
            batch.clear();
            assert!(candidate.resident_context_entries() <= 145);
            if height % 200_000 == 0 || height == count {
                report(
                    "append",
                    candidate.len(),
                    Some(candidate.resident_context_entries()),
                    candidate.file_bytes(),
                    started,
                )?;
            }
        }
    }
    let expected = candidate.tip();
    drop(candidate);
    let status = Command::new(env::current_exe()?)
        .arg("reopen")
        .arg(&path)
        .arg(count.to_string())
        .arg(expected.hash.to_string())
        .arg(format!("{:?}", expected.chainwork))
        .status()?;
    if !status.success() {
        return Err("fresh replay failed".into());
    }
    Ok(())
}
