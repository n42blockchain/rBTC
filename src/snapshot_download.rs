//! Bounded, restartable HTTPS range download for explicitly selected snapshots.
//!
//! Transport checks only establish that all requested bytes were recovered.
//! Snapshot activation must still authenticate the decoded UTXO set against a
//! release-pinned Bitcoin Core identity on the maximum-work header chain.

use std::{
    fs::{self, File, OpenOptions},
    io::{BufReader, BufWriter, Read, Write},
    path::{Path, PathBuf},
    process::Command,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

/// Default independently restartable HTTP range size.
pub const DEFAULT_SNAPSHOT_CHUNK_BYTES: u64 = 64 * 1024 * 1024;
/// Default number of simultaneous range transfers.
pub const DEFAULT_SNAPSHOT_DOWNLOAD_WORKERS: usize = 4;
/// Hard ceiling for one downloaded snapshot.
pub const MAX_SNAPSHOT_DOWNLOAD_BYTES: u64 = 64 * 1024 * 1024 * 1024;
/// Hard ceiling for parallel range workers.
pub const MAX_SNAPSHOT_DOWNLOAD_WORKERS: usize = 8;
const MAX_SOURCE_BYTES: usize = 2_048;

/// Explicit bounded snapshot transfer parameters.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotDownloadConfig {
    /// Operator-selected HTTPS URL.
    pub source: String,
    /// Final file path, published atomically only after every range is complete.
    pub output: PathBuf,
    /// Exact expected transport length.
    pub expected_bytes: u64,
    /// Number of parallel range workers.
    pub workers: usize,
    /// Restartable range size.
    pub chunk_bytes: u64,
}

/// Completed transport identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotDownloadReport {
    /// Final file path.
    pub output: PathBuf,
    /// Exact assembled byte count.
    pub bytes: u64,
    /// SHA-256 of the downloaded file, for transport diagnostics only.
    pub transport_sha256: [u8; 32],
    /// Number of independently restartable chunks.
    pub chunks: usize,
}

/// Snapshot transport failures.
#[derive(Debug, Error)]
pub enum SnapshotDownloadError {
    /// The requested transport parameters are unsafe or incomplete.
    #[error("invalid snapshot download: {0}")]
    Invalid(&'static str),
    /// Filesystem operation failed.
    #[error("snapshot download io: {0}")]
    Io(#[from] std::io::Error),
    /// Persisted resume metadata does not describe this transfer.
    #[error("snapshot download state belongs to another source, output, or length")]
    ResumeMismatch,
    /// The HTTPS range helper failed.
    #[error("snapshot range {index} failed: {message}")]
    Range {
        /// Zero-based range index.
        index: usize,
        /// Bounded process/status diagnostic.
        message: String,
    },
    /// A worker thread panicked.
    #[error("snapshot download worker panicked")]
    WorkerPanic,
    /// Persisted JSON is malformed.
    #[error("snapshot download metadata: {0}")]
    Json(#[from] serde_json::Error),
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct ResumeManifest {
    version: u8,
    source: String,
    output: PathBuf,
    expected_bytes: u64,
    chunk_bytes: u64,
}

fn validate(config: &SnapshotDownloadConfig) -> Result<(), SnapshotDownloadError> {
    if config.source.len() > MAX_SOURCE_BYTES
        || !config.source.starts_with("https://")
        || config.source.contains('@')
        || config.source.contains('?')
        || config.source.contains('#')
    {
        return Err(SnapshotDownloadError::Invalid(
            "source must be a bounded credential-free HTTPS URL without a query or fragment",
        ));
    }
    if !(1..=MAX_SNAPSHOT_DOWNLOAD_BYTES).contains(&config.expected_bytes) {
        return Err(SnapshotDownloadError::Invalid(
            "expected length must be between 1 byte and 64 GiB",
        ));
    }
    if !(1..=MAX_SNAPSHOT_DOWNLOAD_WORKERS).contains(&config.workers) {
        return Err(SnapshotDownloadError::Invalid(
            "worker count must be between 1 and 8",
        ));
    }
    if config.chunk_bytes == 0 || config.chunk_bytes > MAX_SNAPSHOT_DOWNLOAD_BYTES {
        return Err(SnapshotDownloadError::Invalid("invalid chunk size"));
    }
    if config.output.file_name().is_none() {
        return Err(SnapshotDownloadError::Invalid(
            "output must name a regular file",
        ));
    }
    if config.output.exists() {
        return Err(SnapshotDownloadError::Invalid(
            "final output already exists",
        ));
    }
    Ok(())
}

fn state_directory(output: &Path) -> Result<PathBuf, SnapshotDownloadError> {
    let parent = output
        .parent()
        .ok_or(SnapshotDownloadError::Invalid("output has no parent"))?;
    let name = output
        .file_name()
        .ok_or(SnapshotDownloadError::Invalid("output has no file name"))?
        .to_string_lossy();
    Ok(parent.join(format!(".{name}.rbtc-parts")))
}

fn write_manifest(path: &Path, manifest: &ResumeManifest) -> Result<(), SnapshotDownloadError> {
    let encoded = serde_json::to_vec(manifest)?;
    let temporary = path.with_extension("json.tmp");
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temporary)?;
    file.write_all(&encoded)?;
    file.sync_all()?;
    fs::rename(temporary, path)?;
    Ok(())
}

fn prepare_state(
    config: &SnapshotDownloadConfig,
) -> Result<(PathBuf, ResumeManifest), SnapshotDownloadError> {
    let state = state_directory(&config.output)?;
    if state.exists() {
        let metadata = fs::symlink_metadata(&state)?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return Err(SnapshotDownloadError::Invalid(
                "resume state must be a directory, not a symlink",
            ));
        }
    } else {
        fs::create_dir(&state)?;
    }
    let manifest = ResumeManifest {
        version: 1,
        source: config.source.clone(),
        output: config.output.clone(),
        expected_bytes: config.expected_bytes,
        chunk_bytes: config.chunk_bytes,
    };
    let path = state.join("manifest.json");
    if path.exists() {
        let existing: ResumeManifest = serde_json::from_reader(File::open(&path)?)?;
        if existing != manifest {
            return Err(SnapshotDownloadError::ResumeMismatch);
        }
    } else {
        write_manifest(&path, &manifest)?;
    }
    Ok((state, manifest))
}

fn chunk_bounds(manifest: &ResumeManifest, index: usize) -> (u64, u64) {
    let start = u64::try_from(index)
        .expect("chunk index fits u64")
        .saturating_mul(manifest.chunk_bytes);
    let end = start
        .saturating_add(manifest.chunk_bytes)
        .min(manifest.expected_bytes)
        - 1;
    (start, end)
}

fn download_chunk(
    state: &Path,
    manifest: &ResumeManifest,
    index: usize,
) -> Result<(), SnapshotDownloadError> {
    let (start, end) = chunk_bounds(manifest, index);
    let expected = end - start + 1;
    let completed = state.join(format!("chunk-{index:06}.part"));
    if completed.exists() {
        if fs::symlink_metadata(&completed)?.file_type().is_symlink() {
            return Err(SnapshotDownloadError::Invalid(
                "completed chunk cannot be a symlink",
            ));
        }
        if completed.metadata()?.len() == expected {
            return Ok(());
        }
        return Err(SnapshotDownloadError::Range {
            index,
            message: "completed chunk has the wrong length".to_owned(),
        });
    }
    let temporary = state.join(format!("chunk-{index:06}.downloading"));
    if temporary.exists() {
        fs::remove_file(&temporary)?;
    }
    let range = format!("{start}-{end}");
    let maximum = expected.to_string();
    let result = Command::new("curl")
        .args([
            "--fail",
            "--silent",
            "--show-error",
            "--location",
            "--proto",
            "=https",
            "--proto-redir",
            "=https",
            "--max-redirs",
            "3",
            "--connect-timeout",
            "30",
            "--max-time",
            "600",
            "--header",
            "Accept-Encoding: identity",
            "--range",
            &range,
            "--max-filesize",
            &maximum,
            "--output",
        ])
        .arg(&temporary)
        .args(["--write-out", "%{http_code}", &manifest.source])
        .output()?;
    let status = String::from_utf8_lossy(&result.stdout);
    if !result.status.success() || status != "206" {
        return Err(SnapshotDownloadError::Range {
            index,
            message: format!(
                "curl status={} http={} error={}",
                result.status,
                status,
                String::from_utf8_lossy(&result.stderr).trim()
            ),
        });
    }
    if temporary.metadata()?.len() != expected {
        return Err(SnapshotDownloadError::Range {
            index,
            message: "server returned a truncated or oversized range".to_owned(),
        });
    }
    File::open(&temporary)?.sync_all()?;
    fs::rename(temporary, completed)?;
    Ok(())
}

fn assemble(
    config: &SnapshotDownloadConfig,
    state: &Path,
    chunks: usize,
) -> Result<SnapshotDownloadReport, SnapshotDownloadError> {
    let temporary = config.output.with_extension("rbtc-assembling");
    if temporary.exists() {
        fs::remove_file(&temporary)?;
    }
    let file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temporary)?;
    let mut output = BufWriter::with_capacity(1024 * 1024, file);
    let mut digest = Sha256::new();
    let mut bytes = 0_u64;
    for index in 0..chunks {
        let mut input = BufReader::with_capacity(
            1024 * 1024,
            File::open(state.join(format!("chunk-{index:06}.part")))?,
        );
        let mut buffer = vec![0_u8; 1024 * 1024];
        loop {
            let read = input.read(&mut buffer)?;
            if read == 0 {
                break;
            }
            output.write_all(&buffer[..read])?;
            digest.update(&buffer[..read]);
            bytes = bytes
                .checked_add(u64::try_from(read).expect("buffer length fits u64"))
                .ok_or(SnapshotDownloadError::Invalid("assembled length overflow"))?;
        }
    }
    if bytes != config.expected_bytes {
        return Err(SnapshotDownloadError::Invalid(
            "assembled snapshot length mismatch",
        ));
    }
    output.flush()?;
    output.get_ref().sync_all()?;
    drop(output);
    fs::rename(&temporary, &config.output)?;
    if let Some(parent) = config.output.parent() {
        File::open(parent)?.sync_all()?;
    }
    Ok(SnapshotDownloadReport {
        output: config.output.clone(),
        bytes,
        transport_sha256: digest.finalize().into(),
        chunks,
    })
}

/// Downloads all exact ranges concurrently, resumes completed chunks, and
/// atomically publishes the assembled file.
pub fn download_snapshot(
    config: &SnapshotDownloadConfig,
) -> Result<SnapshotDownloadReport, SnapshotDownloadError> {
    validate(config)?;
    let (state, manifest) = prepare_state(config)?;
    let chunks_u64 = config.expected_bytes.div_ceil(config.chunk_bytes);
    let chunks = usize::try_from(chunks_u64)
        .map_err(|_| SnapshotDownloadError::Invalid("too many chunks"))?;
    let next = Arc::new(AtomicUsize::new(0));
    let failure = Arc::new(Mutex::new(None));
    std::thread::scope(|scope| {
        let mut handles = Vec::with_capacity(config.workers.min(chunks));
        for _ in 0..config.workers.min(chunks) {
            let next = Arc::clone(&next);
            let failure = Arc::clone(&failure);
            let state = &state;
            let manifest = &manifest;
            handles.push(scope.spawn(move || {
                loop {
                    if failure.lock().expect("failure lock not poisoned").is_some() {
                        break;
                    }
                    let index = next.fetch_add(1, Ordering::Relaxed);
                    if index >= chunks {
                        break;
                    }
                    if let Err(error) = download_chunk(state, manifest, index) {
                        *failure.lock().expect("failure lock not poisoned") = Some(error);
                        break;
                    }
                }
            }));
        }
        for handle in handles {
            if handle.join().is_err() {
                return Err(SnapshotDownloadError::WorkerPanic);
            }
        }
        Ok(())
    })?;
    if let Some(error) = failure.lock().expect("failure lock not poisoned").take() {
        return Err(error);
    }
    let report = assemble(config, &state, chunks)?;
    for index in 0..chunks {
        fs::remove_file(state.join(format!("chunk-{index:06}.part")))?;
    }
    fs::remove_file(state.join("manifest.json"))?;
    fs::remove_dir(state)?;
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn rejects_unsafe_or_unbounded_sources() {
        let directory = TempDir::new().unwrap();
        for source in [
            "http://example.com/a",
            "https://user@example.com/a",
            "https://example.com/a?token=secret",
            "https://example.com/a#fragment",
        ] {
            assert!(matches!(
                validate(&SnapshotDownloadConfig {
                    source: source.to_owned(),
                    output: directory.path().join("snapshot.dat"),
                    expected_bytes: 1,
                    workers: 1,
                    chunk_bytes: 1,
                }),
                Err(SnapshotDownloadError::Invalid(_))
            ));
        }
    }

    #[test]
    fn manifest_refuses_another_resume_identity() {
        let directory = TempDir::new().unwrap();
        let config = SnapshotDownloadConfig {
            source: "https://example.com/one".to_owned(),
            output: directory.path().join("snapshot.dat"),
            expected_bytes: 10,
            workers: 1,
            chunk_bytes: 4,
        };
        prepare_state(&config).unwrap();
        let changed = SnapshotDownloadConfig {
            source: "https://example.com/two".to_owned(),
            ..config
        };
        assert!(matches!(
            prepare_state(&changed),
            Err(SnapshotDownloadError::ResumeMismatch)
        ));
    }

    #[test]
    fn assembles_completed_chunks_in_order_and_publishes_atomically() {
        let directory = TempDir::new().unwrap();
        let config = SnapshotDownloadConfig {
            source: "https://example.com/snapshot".to_owned(),
            output: directory.path().join("snapshot.dat"),
            expected_bytes: 7,
            workers: 2,
            chunk_bytes: 3,
        };
        let (state, _) = prepare_state(&config).unwrap();
        fs::write(state.join("chunk-000000.part"), b"abc").unwrap();
        fs::write(state.join("chunk-000001.part"), b"def").unwrap();
        fs::write(state.join("chunk-000002.part"), b"g").unwrap();

        let report = assemble(&config, &state, 3).unwrap();
        assert_eq!(report.bytes, 7);
        assert_eq!(report.chunks, 3);
        assert_eq!(fs::read(&config.output).unwrap(), b"abcdefg");
        assert_eq!(
            report.transport_sha256,
            Sha256::digest(b"abcdefg").as_slice()
        );
        assert!(!config.output.with_extension("rbtc-assembling").exists());
    }
}
