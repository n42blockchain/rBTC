//! Bounded structured diagnostics for the standalone daemon.

use std::{
    fs::{self, File, OpenOptions},
    io::{self, Write},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicU8, AtomicU64, Ordering},
        mpsc::{self, SyncSender, TrySendError},
    },
    time::{SystemTime, UNIX_EPOCH},
};

use serde::Serialize;

const LOG_QUEUE_CAPACITY: usize = 4_096;
const MAX_RECORDS_PER_SECOND: u32 = 500;
const MAX_LOG_MESSAGE_BYTES: usize = 4_096;
/// Minimum supported size of one rotating log file.
pub const MIN_LOG_FILE_BYTES: u64 = 1024 * 1024;
/// Maximum supported size of one rotating log file.
pub const MAX_LOG_FILE_BYTES: u64 = 1024 * 1024 * 1024;
/// Minimum retained file count, including the active file.
pub const MIN_LOG_FILES: u8 = 2;
/// Maximum retained file count, including the active file.
pub const MAX_LOG_FILES: u8 = 20;

/// Runtime diagnostic severity.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
#[repr(u8)]
pub enum LogLevel {
    /// Errors that terminate or materially degrade a subsystem.
    Error = 1,
    /// Recoverable failures and operator-actionable conditions.
    Warn = 2,
    /// Ordinary lifecycle and synchronization progress.
    Info = 3,
    /// Verbose troubleshooting detail.
    Debug = 4,
}

impl LogLevel {
    /// Parses one stable lowercase level.
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "error" => Some(Self::Error),
            "warn" => Some(Self::Warn),
            "info" => Some(Self::Info),
            "debug" => Some(Self::Debug),
            _ => None,
        }
    }

    /// Stable lowercase representation.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Error => "error",
            Self::Warn => "warn",
            Self::Info => "info",
            Self::Debug => "debug",
        }
    }
}

/// Standalone rotating-log configuration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogConfig {
    /// Initial runtime severity.
    pub level: LogLevel,
    /// Optional output directory; absent writes structured records to stderr.
    pub directory: Option<PathBuf>,
    /// Maximum bytes in one file before rotation.
    pub max_file_bytes: u64,
    /// Total files retained, including the active file.
    pub max_files: u8,
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            level: LogLevel::Info,
            directory: None,
            max_file_bytes: 16 * 1024 * 1024,
            max_files: 5,
        }
    }
}

#[derive(Serialize)]
struct LogRecord<'a> {
    timestamp_unix_ms: u128,
    level: &'a str,
    target: &'a str,
    message: &'a str,
}

struct OwnedRecord {
    timestamp_unix_ms: u128,
    level: LogLevel,
    target: &'static str,
    message: String,
}

enum WorkerMessage {
    Record(OwnedRecord),
    Flush(mpsc::Sender<()>),
}

struct RateState {
    second: u64,
    emitted: u32,
}

struct Logger {
    level: AtomicU8,
    sender: SyncSender<WorkerMessage>,
    rate: Mutex<RateState>,
    dropped: AtomicU64,
    write_errors: Arc<AtomicU64>,
}

static LOGGER: OnceLock<Logger> = OnceLock::new();

/// Installs the one process-wide standalone logger.
///
/// Embedded hosts may omit this and consume typed node events instead.
pub fn install(config: &LogConfig) -> Result<(), String> {
    if !(MIN_LOG_FILE_BYTES..=MAX_LOG_FILE_BYTES).contains(&config.max_file_bytes) {
        return Err(format!(
            "log file size must be between {MIN_LOG_FILE_BYTES} and {MAX_LOG_FILE_BYTES} bytes"
        ));
    }
    if !(MIN_LOG_FILES..=MAX_LOG_FILES).contains(&config.max_files) {
        return Err(format!(
            "log file count must be between {MIN_LOG_FILES} and {MAX_LOG_FILES}"
        ));
    }
    if LOGGER.get().is_some() {
        set_level(config.level);
        return Ok(());
    }
    let destination = config
        .directory
        .as_ref()
        .map(|directory| {
            RotatingFile::open(directory, config.max_file_bytes, config.max_files)
                .map(Destination::File)
        })
        .transpose()
        .map_err(|error| format!("initialize rotating log: {error}"))?
        .unwrap_or(Destination::Stderr);
    let (sender, receiver) = mpsc::sync_channel(LOG_QUEUE_CAPACITY);
    let write_errors = Arc::new(AtomicU64::new(0));
    let worker_write_errors = Arc::clone(&write_errors);
    std::thread::Builder::new()
        .name("rbtc-log-writer".to_owned())
        .spawn(move || log_worker(receiver, destination, &worker_write_errors))
        .map_err(|error| format!("spawn log writer: {error}"))?;
    LOGGER
        .set(Logger {
            level: AtomicU8::new(config.level as u8),
            sender,
            rate: Mutex::new(RateState {
                second: unix_seconds(),
                emitted: 0,
            }),
            dropped: AtomicU64::new(0),
            write_errors,
        })
        .map_err(|_| "standalone logger was initialized concurrently".to_owned())
}

/// Changes the process-wide runtime severity.
pub fn set_level(level: LogLevel) {
    if let Some(logger) = LOGGER.get() {
        logger.level.store(level as u8, Ordering::Release);
    }
}

/// Whether the standalone process logger is installed.
#[must_use]
pub fn is_installed() -> bool {
    LOGGER.get().is_some()
}

/// Returns the current process-wide severity, or the default before install.
#[must_use]
pub fn level() -> LogLevel {
    let value = LOGGER.get().map_or(LogLevel::Info as u8, |logger| {
        logger.level.load(Ordering::Acquire)
    });
    match value {
        1 => LogLevel::Error,
        2 => LogLevel::Warn,
        4 => LogLevel::Debug,
        _ => LogLevel::Info,
    }
}

/// Whether a standalone sink would currently accept this severity.
#[must_use]
pub fn enabled(level: LogLevel) -> bool {
    LOGGER
        .get()
        .is_some_and(|logger| level as u8 <= logger.level.load(Ordering::Acquire))
}

/// Number of records dropped by the queue or per-second limiter.
#[must_use]
pub fn dropped_records() -> u64 {
    LOGGER
        .get()
        .map_or(0, |logger| logger.dropped.load(Ordering::Relaxed))
}

/// Number of rotating-log destination write or flush failures.
#[must_use]
pub fn write_errors() -> u64 {
    LOGGER
        .get()
        .map_or(0, |logger| logger.write_errors.load(Ordering::Relaxed))
}

/// Emits one bounded structured record.
pub fn emit(level: LogLevel, target: &'static str, message: String) {
    let Some(logger) = LOGGER.get() else {
        return;
    };
    if level as u8 > logger.level.load(Ordering::Acquire) {
        return;
    }
    let now_seconds = unix_seconds();
    {
        let mut rate = logger
            .rate
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if rate.second != now_seconds {
            rate.second = now_seconds;
            rate.emitted = 0;
        }
        if rate.emitted >= MAX_RECORDS_PER_SECOND {
            logger.dropped.fetch_add(1, Ordering::Relaxed);
            return;
        }
        rate.emitted += 1;
    }
    let record = OwnedRecord {
        timestamp_unix_ms: unix_millis(),
        level,
        target,
        message: truncate_message(message),
    };
    if let Err(TrySendError::Full(_) | TrySendError::Disconnected(_)) =
        logger.sender.try_send(WorkerMessage::Record(record))
    {
        logger.dropped.fetch_add(1, Ordering::Relaxed);
    }
}

fn truncate_message(mut message: String) -> String {
    if message.len() <= MAX_LOG_MESSAGE_BYTES {
        return message;
    }
    let mut end = MAX_LOG_MESSAGE_BYTES;
    while !message.is_char_boundary(end) {
        end -= 1;
    }
    message.truncate(end);
    message
}

/// Flushes accepted records without blocking indefinitely.
pub fn flush() {
    let Some(logger) = LOGGER.get() else {
        return;
    };
    let (sent, received) = mpsc::channel();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    let mut message = WorkerMessage::Flush(sent);
    loop {
        match logger.sender.try_send(message) {
            Ok(()) => {
                let remaining = deadline.saturating_duration_since(std::time::Instant::now());
                let _ = received.recv_timeout(remaining);
                break;
            }
            Err(TrySendError::Full(returned)) if std::time::Instant::now() < deadline => {
                message = returned;
                std::thread::yield_now();
            }
            Err(TrySendError::Full(_) | TrySendError::Disconnected(_)) => break,
        }
    }
}

#[allow(clippy::needless_pass_by_value)]
fn log_worker(
    receiver: mpsc::Receiver<WorkerMessage>,
    mut destination: Destination,
    write_errors: &AtomicU64,
) {
    while let Ok(message) = receiver.recv() {
        match message {
            WorkerMessage::Record(record) => {
                let serialized = serde_json::to_string(&LogRecord {
                    timestamp_unix_ms: record.timestamp_unix_ms,
                    level: record.level.as_str(),
                    target: record.target,
                    message: &record.message,
                });
                if let Ok(mut line) = serialized {
                    line.push('\n');
                    if destination.write_line(line.as_bytes()).is_err() {
                        write_errors.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
            WorkerMessage::Flush(completion) => {
                if destination.flush().is_err() {
                    write_errors.fetch_add(1, Ordering::Relaxed);
                }
                let _ = completion.send(());
            }
        }
    }
}

enum Destination {
    Stderr,
    File(RotatingFile),
}

impl Destination {
    fn write_line(&mut self, line: &[u8]) -> io::Result<()> {
        match self {
            Self::Stderr => io::stderr().lock().write_all(line),
            Self::File(file) => file.write_line(line),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            Self::Stderr => io::stderr().lock().flush(),
            Self::File(file) => file.flush(),
        }
    }
}

struct RotatingFile {
    directory: PathBuf,
    active: File,
    current_bytes: u64,
    max_file_bytes: u64,
    max_files: u8,
}

impl RotatingFile {
    fn open(directory: &Path, max_file_bytes: u64, max_files: u8) -> io::Result<Self> {
        let existed = directory.exists();
        validate_log_directory(directory)?;
        if existed {
            require_owner_only_directory(directory)?;
        } else {
            fs::create_dir_all(directory)?;
            restrict_directory_permissions(directory)?;
        }
        let active_path = directory.join("rbtc.log");
        reject_non_regular_log(&active_path)?;
        let active = open_owner_only_append(&active_path)?;
        restrict_file_permissions(&active_path)?;
        let current_bytes = active.metadata()?.len();
        Ok(Self {
            directory: directory.to_path_buf(),
            active,
            current_bytes,
            max_file_bytes,
            max_files,
        })
    }

    fn write_line(&mut self, line: &[u8]) -> io::Result<()> {
        if self.current_bytes > 0
            && self
                .current_bytes
                .saturating_add(u64::try_from(line.len()).unwrap_or(u64::MAX))
                > self.max_file_bytes
        {
            self.rotate()?;
        }
        self.active.write_all(line)?;
        self.current_bytes = self
            .current_bytes
            .saturating_add(u64::try_from(line.len()).unwrap_or(u64::MAX));
        Ok(())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.active.flush()
    }

    fn rotate(&mut self) -> io::Result<()> {
        self.active.flush()?;
        self.active.sync_data()?;
        for index in (1..self.max_files).rev() {
            let source = rotated_path(&self.directory, index - 1);
            let destination = rotated_path(&self.directory, index);
            if destination.exists() {
                fs::remove_file(&destination)?;
            }
            if source.exists() {
                fs::rename(source, destination)?;
            }
        }
        self.active = open_owner_only_truncate(&self.directory.join("rbtc.log"))?;
        self.current_bytes = 0;
        sync_directory(&self.directory)
    }
}

fn rotated_path(directory: &Path, index: u8) -> PathBuf {
    if index == 0 {
        directory.join("rbtc.log")
    } else {
        directory.join(format!("rbtc.log.{index}"))
    }
}

fn validate_log_directory(directory: &Path) -> io::Result<()> {
    match fs::symlink_metadata(directory) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => Err(
            io::Error::new(io::ErrorKind::InvalidInput, "log path must be a directory"),
        ),
        Ok(_) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

fn reject_non_regular_log(path: &Path) -> io::Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_file() => {
            Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "log path must be a regular file",
            ))
        }
        Ok(_) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

fn open_owner_only_append(path: &Path) -> io::Result<File> {
    let mut options = OpenOptions::new();
    options.create(true).append(true);
    owner_only(&mut options);
    options.open(path)
}

fn open_owner_only_truncate(path: &Path) -> io::Result<File> {
    let mut options = OpenOptions::new();
    options.create(true).write(true).truncate(true);
    owner_only(&mut options);
    options.open(path)
}

fn owner_only(options: &mut OpenOptions) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
}

fn restrict_directory_permissions(path: &Path) -> io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
    }
    Ok(())
}

fn require_owner_only_directory(path: &Path) -> io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if fs::metadata(path)?.permissions().mode() & 0o077 != 0 {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "log directory permissions must deny group and other access",
            ));
        }
    }
    Ok(())
}

fn restrict_file_permissions(path: &Path) -> io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    }
    Ok(())
}

fn sync_directory(path: &Path) -> io::Result<()> {
    File::open(path)?.sync_all()
}

fn unix_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_secs())
}

fn unix_millis() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_millis())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn rotating_writer_bounds_files_and_keeps_json_lines() {
        let directory = TempDir::new().unwrap();
        restrict_directory_permissions(directory.path()).unwrap();
        let mut writer = RotatingFile::open(directory.path(), 120, 3).unwrap();
        for marker in 0..20 {
            let line = format!(
                "{{\"marker\":{marker},\"payload\":\"{}\"}}\n",
                "x".repeat(32)
            );
            writer.write_line(line.as_bytes()).unwrap();
        }
        writer.flush().unwrap();
        let files = fs::read_dir(directory.path())
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect::<Vec<_>>();
        assert!(files.len() <= 3);
        for path in files {
            let contents = fs::read_to_string(path).unwrap();
            for line in contents.lines() {
                serde_json::from_str::<serde_json::Value>(line).unwrap();
            }
        }
    }

    #[test]
    fn log_levels_are_strict_and_stable() {
        for (text, level) in [
            ("error", LogLevel::Error),
            ("warn", LogLevel::Warn),
            ("info", LogLevel::Info),
            ("debug", LogLevel::Debug),
        ] {
            assert_eq!(LogLevel::parse(text), Some(level));
            assert_eq!(level.as_str(), text);
        }
        assert_eq!(LogLevel::parse("trace"), None);
        assert_eq!(LogLevel::parse("INFO"), None);
    }

    #[test]
    fn messages_are_utf8_safely_bounded() {
        let message = "界".repeat(MAX_LOG_MESSAGE_BYTES);
        let truncated = truncate_message(message);
        assert!(truncated.len() <= MAX_LOG_MESSAGE_BYTES);
        assert!(truncated.chars().all(|character| character == '界'));
    }
}
