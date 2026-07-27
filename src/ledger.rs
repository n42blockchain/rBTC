//! Configurable, circular historical block retention.
//!
//! This component is intentionally independent of UTXO state. Deleting an old
//! block segment is pruning, not an undo of validated chainstate.

use std::{
    fs::{self, File},
    io::{self, Read, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex, MutexGuard},
};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::archive::{
    ArchiveError, ArchiveManifest, read_archive, read_archive_manifest, verify_archive,
    verify_archive_streaming, write_archive,
};

const INDEX_FILE: &str = "ledger-index.json";
const TRUNCATE_FILE: &str = "ledger-truncate";
const STAGED_FILE: &str = "ledger-staged.rblk";
const POLICY_FILE: &str = "ledger-policy.json";
const LEDGER_POLICY_SCHEMA_VERSION: u32 = 1;
const MAX_LEDGER_POLICY_BYTES: u64 = 1_024;
const MAX_LEDGER_INDEX_BYTES: u64 = 1024 * 1024;
const MAX_AUDIT_SLOT_NAMESPACE: u16 = 4_096;

/// Default approximate one-week historical retention at ten-minute blocks.
pub const DEFAULT_RETENTION_BLOCKS: u32 = 1_008;
/// Default maximum compressed ledger footprint (1 GiB).
pub const DEFAULT_MAX_BYTES: u64 = 1024 * 1024 * 1024;

/// Retention settings for the rotating archive.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct LedgerRetention {
    /// At most this many blocks remain retrievable from local historical storage.
    pub max_blocks: u32,
    /// At most this many compressed bytes are retained.
    pub max_bytes: u64,
    /// Number of files in the circular slot set.
    pub slots: u16,
}

/// Current durable footprint of the live circular-ledger index.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct LedgerStats {
    /// Number of immutable archive segments in the live index.
    pub segments: u32,
    /// Number of retrievable blocks across all live segments.
    pub blocks: u32,
    /// Compressed bytes occupied by all live segments.
    pub bytes: u64,
    /// Oldest retained block height, if the ledger is non-empty.
    pub first_height: Option<u32>,
    /// Newest retained block height, if the ledger is non-empty.
    pub tip_height: Option<u32>,
}

/// Read-only, work-bounded verification result for one freezer directory.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct LedgerAuditReport {
    /// The audit never repairs or opens a mutable database.
    pub read_only: bool,
    /// Every discovered archive inside the configured namespace was verified.
    pub complete: bool,
    /// Persisted policy when its supported schema decoded successfully.
    pub policy: Option<LedgerRetention>,
    /// Maximum archive count this invocation was allowed to verify.
    pub max_segments: u32,
    /// Maximum compressed archive bytes this invocation was allowed to read.
    pub max_bytes: u64,
    /// Archives whose compressed pieces and decompressed records both verified.
    pub verified_segments: u32,
    /// Blocks authenticated by those verified archives.
    pub verified_blocks: u64,
    /// Compressed bytes read from verified archives.
    pub verified_bytes: u64,
    /// Oldest verified height in the selected contiguous chain.
    pub first_height: Option<u32>,
    /// Newest verified height in the selected contiguous chain.
    pub tip_height: Option<u32>,
    /// Whether the durable index selected that verified chain exactly.
    pub index_valid: bool,
    /// Bounded integrity or lifecycle findings.
    pub issues: Vec<String>,
    /// Non-executed recovery actions in safe dependency order.
    pub repair_plan: Vec<String>,
}

impl Default for LedgerRetention {
    fn default() -> Self {
        Self {
            max_blocks: DEFAULT_RETENTION_BLOCKS,
            max_bytes: DEFAULT_MAX_BYTES,
            // One slot per retained block preserves the full window even when
            // a caught-up node receives and publishes one block at a time.
            slots: 1_008,
        }
    }
}

/// Pruned-ledger failure.
#[derive(Debug, Error)]
pub enum LedgerError {
    /// File operation failed.
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    /// Archive construction failed.
    #[error("archive: {0}")]
    Archive(#[from] ArchiveError),
    /// Index serialization failed.
    #[error("index: {0}")]
    Index(#[from] serde_json::Error),
    /// Settings or sequence violates the ledger contract.
    #[error("invalid ledger operation: {0}")]
    Invalid(&'static str),
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct Segment {
    first_height: u32,
    block_count: u32,
    slot: u16,
    bytes: u64,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
struct LedgerIndex {
    next_slot: u16,
    segments: Vec<Segment>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct LedgerPolicy {
    schema_version: u32,
    max_blocks: u32,
    max_bytes: u64,
    slots: u16,
}

impl LedgerPolicy {
    const fn current(retention: LedgerRetention) -> Self {
        Self {
            schema_version: LEDGER_POLICY_SCHEMA_VERSION,
            max_blocks: retention.max_blocks,
            max_bytes: retention.max_bytes,
            slots: retention.slots,
        }
    }
}

/// Checksum-verified downloaded blocks awaiting ledger publication.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StagedSegment {
    /// Archive metadata, including the first height and block count.
    pub manifest: ArchiveManifest,
    /// Consensus-serialized blocks in height order.
    pub blocks: Vec<Vec<u8>>,
}

/// A rotating file-ring for locally retained, consensus-serialized blocks.
pub struct PrunedBlockLedger {
    root: PathBuf,
    retention: LedgerRetention,
    write_guard: Mutex<()>,
    durability: Arc<dyn LedgerDurability>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LedgerSyncPoint {
    StagedArchive,
    StagedPublish,
    StagedRemoval,
    SlotArchive,
    SlotPublish,
    IndexFile,
    IndexPublish,
    PolicyFile,
    PolicyPublish,
    RetiredSlotRemoval,
    TruncateIntentFile,
    TruncateIntentPublish,
    TruncateArchive,
    TruncateMutation,
    TruncateIntentRemoval,
}

trait LedgerDurability: Send + Sync {
    fn sync(&self, point: LedgerSyncPoint, path: &Path) -> io::Result<()>;
}

struct OsLedgerDurability;

impl LedgerDurability for OsLedgerDurability {
    fn sync(&self, _point: LedgerSyncPoint, path: &Path) -> io::Result<()> {
        File::open(path)?.sync_all()
    }
}

impl PrunedBlockLedger {
    /// Verifies the freezer without creating, deleting, renaming, or opening a
    /// mutable database.
    ///
    /// Both archive count and compressed bytes are bounded. When either budget
    /// is exhausted, the report is explicitly incomplete and contains only a
    /// dry-run repair plan.
    pub fn audit(
        root: impl AsRef<Path>,
        max_segments: u32,
        max_bytes: u64,
    ) -> Result<LedgerAuditReport, LedgerError> {
        if max_segments == 0 || max_bytes == 0 {
            return Err(LedgerError::Invalid(
                "ledger audit budgets must be non-zero",
            ));
        }
        audit_ledger(root.as_ref(), max_segments, max_bytes)
    }

    /// Opens a ledger rooted in an application-specific directory.
    pub fn open(root: impl AsRef<Path>, retention: LedgerRetention) -> Result<Self, LedgerError> {
        Self::open_with_durability(root, retention, Arc::new(OsLedgerDurability))
    }

    fn open_with_durability(
        root: impl AsRef<Path>,
        retention: LedgerRetention,
        durability: Arc<dyn LedgerDurability>,
    ) -> Result<Self, LedgerError> {
        if retention.max_blocks == 0 || retention.max_bytes == 0 || retention.slots == 0 {
            return Err(LedgerError::Invalid(
                "all retention limits must be non-zero",
            ));
        }
        fs::create_dir_all(root.as_ref())?;
        let ledger = Self {
            root: root.as_ref().to_path_buf(),
            retention,
            write_guard: Mutex::new(()),
            durability,
        };
        ledger.recover_index()?;
        ledger.recover_truncation()?;
        let index = ledger.read_index()?;
        ledger.cleanup_unindexed_slots_locked(&index)?;
        // Publish the configured policy only after recovery and physical
        // pruning have made the on-disk ledger satisfy it.
        ledger.persist_retention_policy()?;
        Ok(ledger)
    }

    /// Returns the configured pruning bounds.
    #[must_use]
    pub const fn retention(&self) -> LedgerRetention {
        self.retention
    }

    /// Appends a contiguous segment, then rotates old slots until both bounds hold.
    ///
    /// The write uses a temporary file plus rename, so a sudden shutdown leaves
    /// either the old slot or a complete new slot, never a partial archive.
    pub fn append(
        &self,
        first_height: u32,
        blocks: &[Vec<u8>],
    ) -> Result<ArchiveManifest, LedgerError> {
        let _guard = self.lock();
        self.append_locked(first_height, blocks)
    }

    /// Durably stages a downloaded segment before its chainstate transition.
    ///
    /// Only one segment may be staged. It is not visible through retained
    /// reads until [`Self::commit_staged`] publishes its validated prefix.
    pub fn stage(&self, first_height: u32, blocks: &[Vec<u8>]) -> Result<(), LedgerError> {
        if blocks.is_empty() {
            return Err(LedgerError::Invalid("empty staged segment"));
        }
        let block_count =
            u32::try_from(blocks.len()).map_err(|_| LedgerError::Invalid("too many blocks"))?;
        if block_count > self.retention.max_blocks {
            return Err(LedgerError::Invalid(
                "staged segment exceeds maximum blocks",
            ));
        }
        let _guard = self.lock();
        if self.staged_path().exists() {
            return Err(LedgerError::Invalid("staged segment already exists"));
        }
        let temporary = self.root.join("ledger-staged.rblk.new");
        write_archive(&temporary, first_height, blocks)?;
        if fs::metadata(&temporary)?.len() > self.retention.max_bytes {
            fs::remove_file(temporary)?;
            return Err(LedgerError::Invalid("staged segment exceeds maximum bytes"));
        }
        self.sync(LedgerSyncPoint::StagedArchive, &temporary)?;
        fs::rename(temporary, self.staged_path())?;
        self.sync_directory(LedgerSyncPoint::StagedPublish)
    }

    /// Returns the checksum-verified segment awaiting publication, if any.
    pub fn staged(&self) -> Result<Option<StagedSegment>, LedgerError> {
        let _guard = self.lock();
        match read_archive(self.staged_path()) {
            Ok((manifest, blocks)) => Ok(Some(StagedSegment { manifest, blocks })),
            Err(ArchiveError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
                Ok(None)
            }
            Err(error) => Err(error.into()),
        }
    }

    /// Publishes the first `block_count` staged blocks and discards the rest.
    ///
    /// Repeating this after interruption is safe: if the same prefix is
    /// already the retained ledger tip, its bytes are verified before the
    /// staging file is removed.
    pub fn commit_staged(&self, block_count: u32) -> Result<(), LedgerError> {
        if block_count == 0 {
            return Err(LedgerError::Invalid("empty staged commit"));
        }
        let _guard = self.lock();
        let manifest = verify_archive(self.staged_path())?;
        if block_count > manifest.block_count {
            return Err(LedgerError::Invalid("staged commit exceeds segment"));
        }
        let index = self.read_index()?;
        let retained_next = index
            .segments
            .last()
            .map(segment_end_exclusive)
            .transpose()?;
        if retained_next.is_none() || retained_next == Some(manifest.first_height) {
            if block_count == manifest.block_count {
                return self.publish_staged_locked(&manifest, index);
            }
            let (_, blocks) = read_archive(self.staged_path())?;
            let count = usize::try_from(block_count).expect("staged block count fits usize");
            self.append_locked(manifest.first_height, &blocks[..count])?;
        } else if manifest.first_height.checked_add(block_count) == retained_next {
            let (_, blocks) = read_archive(self.staged_path())?;
            let count = usize::try_from(block_count).expect("staged block count fits usize");
            if !self.retained_bytes_match(&index, manifest.first_height, &blocks[..count])? {
                return Err(LedgerError::Invalid(
                    "staged segment does not extend ledger tip",
                ));
            }
        } else {
            return Err(LedgerError::Invalid(
                "staged segment does not extend ledger tip",
            ));
        }
        fs::remove_file(self.staged_path())?;
        self.sync_directory(LedgerSyncPoint::StagedRemoval)
    }

    fn publish_staged_locked(
        &self,
        manifest: &ArchiveManifest,
        mut index: LedgerIndex,
    ) -> Result<(), LedgerError> {
        let bytes = fs::metadata(self.staged_path())?.len();
        if bytes > self.retention.max_bytes {
            return Err(LedgerError::Invalid("single segment exceeds maximum bytes"));
        }
        let slot = index.next_slot % self.retention.slots;
        index.segments.retain(|segment| segment.slot != slot);
        let destination = self.slot_path(slot);
        fs::rename(self.staged_path(), &destination)?;
        self.sync_directory(LedgerSyncPoint::SlotPublish)?;
        index.segments.push(Segment {
            first_height: manifest.first_height,
            block_count: manifest.block_count,
            slot,
            bytes,
        });
        index.next_slot = (slot + 1) % self.retention.slots;
        while exceeds(&index.segments, self.retention) {
            index.segments.remove(0);
        }
        self.write_index(&index)?;
        self.cleanup_unindexed_slots_locked(&index)?;
        Ok(())
    }

    /// Removes an uncommitted staged segment, if one exists.
    pub fn discard_staged(&self) -> Result<(), LedgerError> {
        let _guard = self.lock();
        match fs::remove_file(self.staged_path()) {
            Ok(()) => self.sync_directory(LedgerSyncPoint::StagedRemoval),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    fn append_locked(
        &self,
        first_height: u32,
        blocks: &[Vec<u8>],
    ) -> Result<ArchiveManifest, LedgerError> {
        if blocks.is_empty() {
            return Err(LedgerError::Invalid("empty segment"));
        }
        let mut index = self.read_index()?;
        let block_count =
            u32::try_from(blocks.len()).map_err(|_| LedgerError::Invalid("too many blocks"))?;
        if block_count > self.retention.max_blocks {
            return Err(LedgerError::Invalid(
                "single segment exceeds maximum blocks",
            ));
        }
        if let Some(last) = index.segments.last() {
            let expected = last
                .first_height
                .checked_add(last.block_count)
                .ok_or(LedgerError::Invalid("height overflow"))?;
            if first_height != expected {
                return Err(LedgerError::Invalid("segment is not contiguous"));
            }
        }
        let slot = index.next_slot % self.retention.slots;
        // A slot is overwritten only after it has been removed from the live index.
        index.segments.retain(|segment| segment.slot != slot);
        let destination = self.slot_path(slot);
        let temporary = destination.with_extension("rblk.new");
        let manifest = write_archive(&temporary, first_height, blocks)?;
        let bytes = fs::metadata(&temporary)?.len();
        if bytes > self.retention.max_bytes {
            fs::remove_file(temporary)?;
            return Err(LedgerError::Invalid("single segment exceeds maximum bytes"));
        }
        self.sync(LedgerSyncPoint::SlotArchive, &temporary)?;
        fs::rename(&temporary, &destination)?;
        self.sync_directory(LedgerSyncPoint::SlotPublish)?;
        index.segments.push(Segment {
            first_height,
            block_count,
            slot,
            bytes,
        });
        index.next_slot = (slot + 1) % self.retention.slots;
        while exceeds(&index.segments, self.retention) {
            index.segments.remove(0);
        }
        self.write_index(&index)?;
        self.cleanup_unindexed_slots_locked(&index)?;
        Ok(manifest)
    }

    /// Returns the retained inclusive height ranges in chronological order.
    pub fn retained_ranges(&self) -> Result<Vec<(u32, u32)>, LedgerError> {
        let _guard = self.lock();
        self.read_index()?
            .segments
            .into_iter()
            .map(|segment| {
                let end = segment_end_inclusive(&segment)?;
                Ok((segment.first_height, end))
            })
            .collect()
    }

    /// Returns the newest locally retained block height, if any.
    pub fn retained_tip(&self) -> Result<Option<u32>, LedgerError> {
        let _guard = self.lock();
        self.read_index()?
            .segments
            .last()
            .map(segment_end_inclusive)
            .transpose()
    }

    /// Returns bounded live-index counts without reading archive payloads.
    pub fn stats(&self) -> Result<LedgerStats, LedgerError> {
        let _guard = self.lock();
        let index = self.read_index()?;
        let mut blocks = 0_u32;
        let mut bytes = 0_u64;
        for segment in &index.segments {
            blocks = blocks
                .checked_add(segment.block_count)
                .ok_or(LedgerError::Invalid("retained block count overflow"))?;
            bytes = bytes
                .checked_add(segment.bytes)
                .ok_or(LedgerError::Invalid("retained byte count overflow"))?;
        }
        Ok(LedgerStats {
            segments: u32::try_from(index.segments.len())
                .map_err(|_| LedgerError::Invalid("retained segment count overflow"))?,
            blocks,
            bytes,
            first_height: index.segments.first().map(|segment| segment.first_height),
            tip_height: index
                .segments
                .last()
                .map(segment_end_inclusive)
                .transpose()?,
        })
    }

    /// Reads one consensus-serialized block by height when it is retained.
    ///
    /// The complete containing archive is checksum-verified before the block
    /// is returned. A pruned or not-yet-appended height returns `None`.
    pub fn read_block(&self, height: u32) -> Result<Option<Vec<u8>>, LedgerError> {
        let _guard = self.lock();
        let index = self.read_index()?;
        self.read_block_from_index(&index, height)
    }

    /// Removes every retained block at or above `first_removed_height`.
    ///
    /// A durable intent makes deletion and partial-segment rewriting
    /// idempotent across process interruption.
    pub fn truncate_from(&self, first_removed_height: u32) -> Result<(), LedgerError> {
        let _guard = self.lock();
        self.write_truncate_intent(first_removed_height)?;
        self.apply_truncation(first_removed_height)?;
        self.clear_truncate_intent()
    }

    fn lock(&self) -> MutexGuard<'_, ()> {
        self.write_guard.lock().expect("ledger lock not poisoned")
    }
    fn slot_path(&self, slot: u16) -> PathBuf {
        self.root.join(format!("blk-{slot:04}.rblk"))
    }
    fn index_path(&self) -> PathBuf {
        self.root.join(INDEX_FILE)
    }
    fn policy_path(&self) -> PathBuf {
        self.root.join(POLICY_FILE)
    }
    fn truncate_path(&self) -> PathBuf {
        self.root.join(TRUNCATE_FILE)
    }
    fn staged_path(&self) -> PathBuf {
        self.root.join(STAGED_FILE)
    }
    fn read_index(&self) -> Result<LedgerIndex, LedgerError> {
        match fs::read(self.index_path()) {
            Ok(bytes) => Ok(serde_json::from_slice(&bytes)?),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                Ok(LedgerIndex::default())
            }
            Err(error) => Err(error.into()),
        }
    }
    fn write_index(&self, index: &LedgerIndex) -> Result<(), LedgerError> {
        let temporary = self.root.join("ledger-index.json.new");
        let mut file = File::create(&temporary)?;
        file.write_all(&serde_json::to_vec(index)?)?;
        drop(file);
        self.sync(LedgerSyncPoint::IndexFile, &temporary)?;
        fs::rename(&temporary, self.index_path())?;
        self.sync_directory(LedgerSyncPoint::IndexPublish)
    }

    fn persist_retention_policy(&self) -> Result<(), LedgerError> {
        let expected = LedgerPolicy::current(self.retention);
        match fs::symlink_metadata(self.policy_path()) {
            Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_file() => {
                return Err(LedgerError::Invalid("ledger policy must be a regular file"));
            }
            #[cfg(unix)]
            Ok(metadata) if std::os::unix::fs::MetadataExt::nlink(&metadata) != 1 => {
                return Err(LedgerError::Invalid(
                    "ledger policy must not be hard-linked",
                ));
            }
            Ok(metadata) => {
                if metadata.len() > MAX_LEDGER_POLICY_BYTES {
                    return Err(LedgerError::Invalid("ledger policy is oversized"));
                }
                let mut bytes = Vec::new();
                File::open(self.policy_path())?
                    .take(MAX_LEDGER_POLICY_BYTES.saturating_add(1))
                    .read_to_end(&mut bytes)?;
                if u64::try_from(bytes.len()).unwrap_or(u64::MAX) > MAX_LEDGER_POLICY_BYTES {
                    return Err(LedgerError::Invalid("ledger policy is oversized"));
                }
                let actual: LedgerPolicy = serde_json::from_slice(&bytes)?;
                if actual.schema_version != LEDGER_POLICY_SCHEMA_VERSION {
                    return Err(LedgerError::Invalid(
                        "unsupported ledger policy schema version",
                    ));
                }
                if actual == expected {
                    return Ok(());
                }
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        let temporary = self.root.join("ledger-policy.json.new");
        match fs::symlink_metadata(&temporary) {
            Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_file() => {
                return Err(LedgerError::Invalid(
                    "temporary ledger policy must be a regular file",
                ));
            }
            #[cfg(unix)]
            Ok(metadata) if std::os::unix::fs::MetadataExt::nlink(&metadata) != 1 => {
                return Err(LedgerError::Invalid(
                    "temporary ledger policy must not be hard-linked",
                ));
            }
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        let mut options = fs::OpenOptions::new();
        options.create(true).write(true).truncate(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(&temporary)?;
        file.write_all(&serde_json::to_vec(&expected)?)?;
        drop(file);
        self.sync(LedgerSyncPoint::PolicyFile, &temporary)?;
        fs::rename(&temporary, self.policy_path())?;
        self.sync_directory(LedgerSyncPoint::PolicyPublish)
    }

    fn cleanup_unindexed_slots_locked(&self, index: &LedgerIndex) -> Result<u64, LedgerError> {
        let live_slots = index
            .segments
            .iter()
            .map(|segment| segment.slot)
            .collect::<std::collections::BTreeSet<_>>();
        let mut reclaimed = 0_u64;
        let mut removed = false;
        for slot in 0..self.retention.slots {
            if live_slots.contains(&slot) {
                continue;
            }
            let path = self.slot_path(slot);
            let bytes = fs::metadata(&path).map_or(0, |metadata| metadata.len());
            match fs::remove_file(path) {
                Ok(()) => {
                    reclaimed = reclaimed.saturating_add(bytes);
                    removed = true;
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
        }
        if removed {
            self.sync_directory(LedgerSyncPoint::RetiredSlotRemoval)?;
        }
        Ok(reclaimed)
    }

    fn recover_index(&self) -> Result<(), LedgerError> {
        let scanned = self.scan_segments();
        let persisted = fs::read(self.index_path())
            .ok()
            .and_then(|bytes| serde_json::from_slice::<LedgerIndex>(&bytes).ok())
            .filter(|index| valid_index(index, &scanned, self.retention.slots));
        let mut segments = if let Some(index) = persisted.filter(|index| !index.segments.is_empty())
        {
            let mut segments = index.segments;
            while let Some(expected) = segments
                .last()
                .and_then(|last| last.first_height.checked_add(last.block_count))
            {
                let Some(next) = scanned.iter().find(|segment| {
                    segment.first_height == expected
                        && !segments.iter().any(|live| live.slot == segment.slot)
                }) else {
                    break;
                };
                segments.push(next.clone());
            }
            segments
        } else {
            best_contiguous_chain(&scanned)
        };
        while exceeds(&segments, self.retention) {
            segments.remove(0);
        }
        let next_slot = segments
            .last()
            .map_or(0, |segment| (segment.slot + 1) % self.retention.slots);
        self.write_index(&LedgerIndex {
            next_slot,
            segments,
        })
    }

    fn scan_segments(&self) -> Vec<Segment> {
        let mut segments = Vec::new();
        for slot in 0..self.retention.slots {
            let path = self.slot_path(slot);
            let Ok(manifest) = read_archive_manifest(&path) else {
                continue;
            };
            let Ok(metadata) = fs::metadata(path) else {
                continue;
            };
            if manifest.block_count == 0 || metadata.len() > self.retention.max_bytes {
                continue;
            }
            segments.push(Segment {
                first_height: manifest.first_height,
                block_count: manifest.block_count,
                slot,
                bytes: metadata.len(),
            });
        }
        segments.sort_by_key(|segment| (segment.first_height, segment.slot));
        segments
    }

    fn recover_truncation(&self) -> Result<(), LedgerError> {
        let bytes = match fs::read(self.truncate_path()) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(error.into()),
        };
        let height = u32::from_le_bytes(
            bytes
                .as_slice()
                .try_into()
                .map_err(|_| LedgerError::Invalid("truncate intent"))?,
        );
        self.apply_truncation(height)?;
        self.clear_truncate_intent()
    }

    fn write_truncate_intent(&self, height: u32) -> Result<(), LedgerError> {
        let temporary = self.root.join("ledger-truncate.new");
        let mut file = File::create(&temporary)?;
        file.write_all(&height.to_le_bytes())?;
        drop(file);
        self.sync(LedgerSyncPoint::TruncateIntentFile, &temporary)?;
        fs::rename(&temporary, self.truncate_path())?;
        self.sync_directory(LedgerSyncPoint::TruncateIntentPublish)
    }

    fn clear_truncate_intent(&self) -> Result<(), LedgerError> {
        match fs::remove_file(self.truncate_path()) {
            Ok(()) => self.sync_directory(LedgerSyncPoint::TruncateIntentRemoval),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    fn apply_truncation(&self, height: u32) -> Result<(), LedgerError> {
        for slot in 0..self.retention.slots {
            let path = self.slot_path(slot);
            let manifest = match read_archive_manifest(&path) {
                Ok(manifest) => manifest,
                Err(ArchiveError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
                    continue;
                }
                Err(error) => return Err(error.into()),
            };
            let end = manifest
                .first_height
                .checked_add(manifest.block_count)
                .ok_or(LedgerError::Invalid("height overflow"))?;
            if manifest.first_height >= height {
                fs::remove_file(path)?;
            } else if end > height {
                let (_, mut blocks) = read_archive(&path)?;
                let keep = usize::try_from(height - manifest.first_height)
                    .expect("retained block count fits usize");
                blocks.truncate(keep);
                let temporary = path.with_extension("rblk.truncate");
                write_archive(&temporary, manifest.first_height, &blocks)?;
                self.sync(LedgerSyncPoint::TruncateArchive, &temporary)?;
                fs::rename(temporary, path)?;
            }
        }
        self.sync_directory(LedgerSyncPoint::TruncateMutation)?;
        self.recover_index()
    }

    fn sync_directory(&self, point: LedgerSyncPoint) -> Result<(), LedgerError> {
        self.sync(point, &self.root)
    }

    fn sync(&self, point: LedgerSyncPoint, path: &Path) -> Result<(), LedgerError> {
        self.durability.sync(point, path)?;
        Ok(())
    }

    fn retained_bytes_match(
        &self,
        index: &LedgerIndex,
        first_height: u32,
        expected: &[Vec<u8>],
    ) -> Result<bool, LedgerError> {
        for (offset, expected) in expected.iter().enumerate() {
            let height = first_height
                .checked_add(u32::try_from(offset).expect("staged block offset fits u32"))
                .ok_or(LedgerError::Invalid("height overflow"))?;
            let Some(actual) = self.read_block_from_index(index, height)? else {
                return Ok(false);
            };
            if actual != *expected {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn read_block_from_index(
        &self,
        index: &LedgerIndex,
        height: u32,
    ) -> Result<Option<Vec<u8>>, LedgerError> {
        let Some(segment) = index
            .segments
            .iter()
            .find(|segment| segment_contains(segment, height))
        else {
            return Ok(None);
        };
        let (manifest, blocks) = read_archive(self.slot_path(segment.slot))?;
        if manifest.first_height != segment.first_height
            || manifest.block_count != segment.block_count
        {
            return Err(LedgerError::Invalid("archive does not match ledger index"));
        }
        let offset = usize::try_from(height - segment.first_height)
            .expect("archive block offset fits usize");
        blocks
            .get(offset)
            .cloned()
            .map(Some)
            .ok_or(LedgerError::Invalid("archive block missing"))
    }
}

#[allow(clippy::too_many_lines)]
fn audit_ledger(
    root: &Path,
    max_segments: u32,
    max_bytes: u64,
) -> Result<LedgerAuditReport, LedgerError> {
    let root_metadata = fs::symlink_metadata(root)?;
    if root_metadata.file_type().is_symlink() || !root_metadata.is_dir() {
        return Err(LedgerError::Invalid(
            "ledger audit root must be a directory",
        ));
    }
    let mut report = LedgerAuditReport {
        read_only: true,
        complete: true,
        policy: None,
        max_segments,
        max_bytes,
        verified_segments: 0,
        verified_blocks: 0,
        verified_bytes: 0,
        first_height: None,
        tip_height: None,
        index_valid: false,
        issues: Vec::new(),
        repair_plan: Vec::new(),
    };

    let policy_path = root.join(POLICY_FILE);
    let policy = match read_bounded_audit_file(&policy_path, MAX_LEDGER_POLICY_BYTES) {
        Ok(Some(bytes)) => match serde_json::from_slice::<LedgerPolicy>(&bytes) {
            Ok(policy) if policy.schema_version == LEDGER_POLICY_SCHEMA_VERSION => Some(policy),
            Ok(_) => {
                push_unique(
                    &mut report.issues,
                    "unsupported ledger policy schema version",
                );
                None
            }
            Err(_) => {
                push_unique(&mut report.issues, "malformed ledger policy");
                None
            }
        },
        Ok(None) => {
            push_unique(&mut report.issues, "ledger policy is missing");
            None
        }
        Err(_) => {
            push_unique(
                &mut report.issues,
                "ledger policy is unreadable, oversized, or not a regular file",
            );
            None
        }
    };
    report.policy = policy.map(|policy| LedgerRetention {
        max_blocks: policy.max_blocks,
        max_bytes: policy.max_bytes,
        slots: policy.slots,
    });
    let slot_namespace = policy
        .filter(|policy| policy.slots > 0 && policy.slots <= MAX_AUDIT_SLOT_NAMESPACE)
        .map_or(
            u16::try_from(DEFAULT_RETENTION_BLOCKS).expect("default slot count fits u16"),
            |policy| policy.slots,
        );
    if policy.is_some_and(|policy| policy.slots == 0 || policy.slots > MAX_AUDIT_SLOT_NAMESPACE) {
        push_unique(
            &mut report.issues,
            "ledger policy slot namespace exceeds the audit bound",
        );
        report.complete = false;
    }

    let mut scanned = Vec::new();
    let mut attempted_segments = 0_u32;
    let mut attempted_bytes = 0_u64;
    for slot in 0..slot_namespace {
        let path = root.join(format!("blk-{slot:04}.rblk"));
        let metadata = match fs::symlink_metadata(&path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
            Err(_) => {
                push_unique(&mut report.issues, "an archive slot is unreadable");
                continue;
            }
        };
        if metadata.file_type().is_symlink() || !metadata.is_file() {
            push_unique(&mut report.issues, "an archive slot is not a regular file");
            continue;
        }
        if attempted_segments >= max_segments
            || metadata.len() > max_bytes.saturating_sub(attempted_bytes)
        {
            report.complete = false;
            push_unique(&mut report.issues, "ledger audit work budget was exhausted");
            continue;
        }
        attempted_segments += 1;
        attempted_bytes = attempted_bytes.saturating_add(metadata.len());
        if let Ok(manifest) = verify_archive_streaming(&path) {
            report.verified_segments += 1;
            report.verified_blocks = report
                .verified_blocks
                .saturating_add(u64::from(manifest.block_count));
            report.verified_bytes = report.verified_bytes.saturating_add(metadata.len());
            scanned.push(Segment {
                first_height: manifest.first_height,
                block_count: manifest.block_count,
                slot,
                bytes: metadata.len(),
            });
        } else {
            push_unique(
                &mut report.issues,
                "an archive failed checksum, decompression, or record framing verification",
            );
            push_unique(
                &mut report.repair_plan,
                "restore the failed archive from an authenticated source before rebuilding any index",
            );
        }
    }

    let index = match read_bounded_audit_file(&root.join(INDEX_FILE), MAX_LEDGER_INDEX_BYTES) {
        Ok(Some(bytes)) => {
            if let Ok(index) = serde_json::from_slice::<LedgerIndex>(&bytes) {
                Some(index)
            } else {
                push_unique(&mut report.issues, "ledger index is malformed");
                None
            }
        }
        Ok(None) if scanned.is_empty() => Some(LedgerIndex::default()),
        Ok(None) => {
            push_unique(&mut report.issues, "ledger index is missing");
            None
        }
        Err(_) => {
            push_unique(
                &mut report.issues,
                "ledger index is unreadable, oversized, or not a regular file",
            );
            None
        }
    };

    let selected = if report.complete {
        match index {
            Some(index) if valid_index(&index, &scanned, slot_namespace) => {
                report.index_valid = true;
                index.segments
            }
            _ => {
                push_unique(
                    &mut report.issues,
                    "ledger index does not select the verified contiguous archive chain",
                );
                best_contiguous_chain(&scanned)
            }
        }
    } else {
        Vec::new()
    };
    if report.complete && !report.index_valid {
        push_unique(
            &mut report.repair_plan,
            "rebuild the ledger index from the newest verified contiguous archive chain",
        );
    }
    if !report.complete {
        push_unique(
            &mut report.repair_plan,
            "rerun the read-only audit with larger explicit work budgets before planning repairs",
        );
    }

    if let Some(first) = selected.first() {
        report.first_height = Some(first.first_height);
    }
    if let Some(last) = selected.last() {
        report.tip_height = Some(segment_end_inclusive(last)?);
    }
    if let Some(retention) = report.policy {
        if report.complete && exceeds(&selected, retention) {
            push_unique(
                &mut report.issues,
                "selected ledger chain exceeds its persisted retention policy",
            );
            push_unique(
                &mut report.repair_plan,
                "apply normal crash-safe startup recovery to enforce the persisted retention policy",
            );
        }
    }

    if report.complete {
        let selected_slots = selected
            .iter()
            .map(|segment| segment.slot)
            .collect::<std::collections::BTreeSet<_>>();
        if scanned
            .iter()
            .any(|segment| !selected_slots.contains(&segment.slot))
        {
            push_unique(
                &mut report.issues,
                "verified archive slots exist outside the selected ledger chain",
            );
            push_unique(
                &mut report.repair_plan,
                "remove only verified unindexed archive slots after the rebuilt index is durable",
            );
        }
    }

    for (path, issue, action) in [
        (
            root.join(STAGED_FILE),
            "a staged archive awaits recovery",
            "recover or discard the staged archive only after comparing it with the active execution tip",
        ),
        (
            root.join(TRUNCATE_FILE),
            "a truncation intent awaits recovery",
            "resume the durable truncation intent before serving blocks",
        ),
        (
            root.join("ledger-index.json.new"),
            "a temporary ledger index awaits recovery",
            "run normal startup recovery to adopt or remove the temporary index safely",
        ),
        (
            root.join("ledger-policy.json.new"),
            "a temporary ledger policy awaits recovery",
            "run normal startup recovery to publish or remove the temporary policy safely",
        ),
    ] {
        match fs::symlink_metadata(path) {
            Ok(_) => {
                push_unique(&mut report.issues, issue);
                push_unique(&mut report.repair_plan, action);
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(_) => push_unique(&mut report.issues, "a recovery marker is unreadable"),
        }
    }
    if report.policy.is_none() {
        push_unique(
            &mut report.repair_plan,
            "publish a supported retention policy only after archive recovery and trimming succeed",
        );
    }
    Ok(report)
}

fn read_bounded_audit_file(path: &Path, limit: u64) -> io::Result<Option<Vec<u8>>> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    if metadata.file_type().is_symlink() || !metadata.is_file() || metadata.len() > limit {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "audit input is not a bounded regular file",
        ));
    }
    let mut bytes = Vec::new();
    File::open(path)?
        .take(limit.saturating_add(1))
        .read_to_end(&mut bytes)?;
    if u64::try_from(bytes.len()).unwrap_or(u64::MAX) > limit {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "audit input exceeds its byte bound",
        ));
    }
    Ok(Some(bytes))
}

fn push_unique(values: &mut Vec<String>, value: &str) {
    if !values.iter().any(|existing| existing == value) {
        values.push(value.to_owned());
    }
}

fn valid_index(index: &LedgerIndex, scanned: &[Segment], slots: u16) -> bool {
    if index.next_slot >= slots {
        return false;
    }
    let mut expected = None;
    let mut used_slots = std::collections::BTreeSet::new();
    for segment in &index.segments {
        if segment.slot >= slots
            || !used_slots.insert(segment.slot)
            || !scanned.contains(segment)
            || expected.is_some_and(|height| height != segment.first_height)
        {
            return false;
        }
        expected = segment.first_height.checked_add(segment.block_count);
        if expected.is_none() {
            return false;
        }
    }
    true
}

fn segment_end_inclusive(segment: &Segment) -> Result<u32, LedgerError> {
    let offset = segment
        .block_count
        .checked_sub(1)
        .ok_or(LedgerError::Invalid("empty segment"))?;
    segment
        .first_height
        .checked_add(offset)
        .ok_or(LedgerError::Invalid("height overflow"))
}

fn segment_end_exclusive(segment: &Segment) -> Result<u32, LedgerError> {
    segment
        .first_height
        .checked_add(segment.block_count)
        .ok_or(LedgerError::Invalid("height overflow"))
}

fn segment_contains(segment: &Segment, height: u32) -> bool {
    height >= segment.first_height
        && segment
            .first_height
            .checked_add(segment.block_count)
            .is_some_and(|end| height < end)
}

fn best_contiguous_chain(scanned: &[Segment]) -> Vec<Segment> {
    let mut best = Vec::new();
    for first in 0..scanned.len() {
        let mut chain = vec![scanned[first].clone()];
        let mut used_slots = std::collections::BTreeSet::from([scanned[first].slot]);
        loop {
            let Some(expected) = chain
                .last()
                .and_then(|segment| segment.first_height.checked_add(segment.block_count))
            else {
                break;
            };
            let Some(next) = scanned.iter().find(|segment| {
                segment.first_height == expected && !used_slots.contains(&segment.slot)
            }) else {
                break;
            };
            used_slots.insert(next.slot);
            chain.push(next.clone());
        }
        let chain_end = chain
            .last()
            .and_then(|segment| segment.first_height.checked_add(segment.block_count))
            .unwrap_or(0);
        let best_end = best
            .last()
            .and_then(|segment: &Segment| segment.first_height.checked_add(segment.block_count))
            .unwrap_or(0);
        if (chain_end, chain.len()) > (best_end, best.len()) {
            best = chain;
        }
    }
    best
}

fn exceeds(segments: &[Segment], retention: LedgerRetention) -> bool {
    let block_count = segments
        .iter()
        .map(|segment| u64::from(segment.block_count))
        .sum::<u64>();
    let bytes = segments.iter().map(|segment| segment.bytes).sum::<u64>();
    block_count > u64::from(retention.max_blocks) || bytes > retention.max_bytes
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};
    use tempfile::TempDir;

    struct FailOnceDurability {
        target: LedgerSyncPoint,
        armed: AtomicBool,
        failed: AtomicBool,
    }

    impl FailOnceDurability {
        fn new(target: LedgerSyncPoint) -> Self {
            Self {
                target,
                armed: AtomicBool::new(false),
                failed: AtomicBool::new(false),
            }
        }

        fn arm(&self) {
            self.armed.store(true, Ordering::SeqCst);
        }

        fn did_fail(&self) -> bool {
            self.failed.load(Ordering::SeqCst)
        }
    }

    impl LedgerDurability for FailOnceDurability {
        fn sync(&self, point: LedgerSyncPoint, path: &Path) -> io::Result<()> {
            if self.armed.load(Ordering::SeqCst)
                && point == self.target
                && self
                    .failed
                    .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                    .is_ok()
            {
                return Err(io::Error::other(format!(
                    "injected ledger sync failure at {point:?}"
                )));
            }
            OsLedgerDurability.sync(point, path)
        }
    }

    #[test]
    fn rotates_to_retention_window() {
        let dir = TempDir::new().unwrap();
        let ledger = PrunedBlockLedger::open(
            dir.path(),
            LedgerRetention {
                max_blocks: 3,
                max_bytes: 1_000_000,
                slots: 3,
            },
        )
        .unwrap();
        ledger.append(10, &[vec![10]]).unwrap();
        ledger.append(11, &[vec![11]]).unwrap();
        ledger.append(12, &[vec![12]]).unwrap();
        ledger.append(13, &[vec![13]]).unwrap();
        assert_eq!(
            ledger.retained_ranges().unwrap(),
            vec![(11, 11), (12, 12), (13, 13)]
        );
        let stats = ledger.stats().unwrap();
        assert_eq!(stats.segments, 3);
        assert_eq!(stats.blocks, 3);
        assert!(stats.bytes > 0);
        assert_eq!(stats.first_height, Some(11));
        assert_eq!(stats.tip_height, Some(13));
    }

    #[test]
    fn retention_policy_is_versioned_persisted_and_updated() {
        let dir = TempDir::new().unwrap();
        let first = LedgerRetention {
            max_blocks: 2,
            max_bytes: 1_000_000,
            slots: 2,
        };
        drop(PrunedBlockLedger::open(dir.path(), first).unwrap());

        let policy_path = dir.path().join(POLICY_FILE);
        assert_eq!(
            serde_json::from_slice::<LedgerPolicy>(&fs::read(&policy_path).unwrap()).unwrap(),
            LedgerPolicy::current(first)
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                fs::metadata(&policy_path).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }

        let updated = LedgerRetention {
            max_blocks: 3,
            max_bytes: 2_000_000,
            slots: 3,
        };
        drop(PrunedBlockLedger::open(dir.path(), updated).unwrap());
        assert_eq!(
            serde_json::from_slice::<LedgerPolicy>(&fs::read(policy_path).unwrap()).unwrap(),
            LedgerPolicy::current(updated)
        );
    }

    #[test]
    fn future_retention_policy_schema_is_refused_without_rewrite() {
        let dir = TempDir::new().unwrap();
        let policy_path = dir.path().join(POLICY_FILE);
        let future = br#"{"schema_version":2,"max_blocks":2,"max_bytes":1000000,"slots":2}"#;
        fs::write(&policy_path, future).unwrap();

        assert!(matches!(
            PrunedBlockLedger::open(
                dir.path(),
                LedgerRetention {
                    max_blocks: 2,
                    max_bytes: 1_000_000,
                    slots: 2,
                }
            ),
            Err(LedgerError::Invalid(
                "unsupported ledger policy schema version"
            ))
        ));
        assert_eq!(fs::read(policy_path).unwrap(), future);
    }

    #[test]
    fn retention_policy_sync_failures_reopen_to_a_complete_policy() {
        let original = LedgerRetention {
            max_blocks: 2,
            max_bytes: 1_000_000,
            slots: 2,
        };
        let updated = LedgerRetention {
            max_blocks: 3,
            max_bytes: 2_000_000,
            slots: 3,
        };

        for point in [LedgerSyncPoint::PolicyFile, LedgerSyncPoint::PolicyPublish] {
            let dir = TempDir::new().unwrap();
            drop(PrunedBlockLedger::open(dir.path(), original).unwrap());
            let durability = Arc::new(FailOnceDurability::new(point));
            durability.arm();
            assert!(matches!(
                PrunedBlockLedger::open_with_durability(dir.path(), updated, durability.clone()),
                Err(LedgerError::Io(_))
            ));
            assert!(durability.did_fail());

            drop(PrunedBlockLedger::open(dir.path(), updated).unwrap());
            assert_eq!(
                serde_json::from_slice::<LedgerPolicy>(
                    &fs::read(dir.path().join(POLICY_FILE)).unwrap()
                )
                .unwrap(),
                LedgerPolicy::current(updated)
            );
        }
    }

    #[cfg(unix)]
    #[test]
    fn retention_policy_rejects_symlinks_and_hardlinks() {
        use std::os::unix::fs::symlink;

        let symlink_dir = TempDir::new().unwrap();
        let outside = symlink_dir.path().join("outside");
        fs::write(&outside, b"unchanged").unwrap();
        symlink(&outside, symlink_dir.path().join(POLICY_FILE)).unwrap();
        assert!(matches!(
            PrunedBlockLedger::open(symlink_dir.path(), LedgerRetention::default()),
            Err(LedgerError::Invalid("ledger policy must be a regular file"))
        ));
        assert_eq!(fs::read(outside).unwrap(), b"unchanged");

        let hardlink_dir = TempDir::new().unwrap();
        let source = hardlink_dir.path().join("source");
        fs::write(&source, b"unchanged").unwrap();
        fs::hard_link(&source, hardlink_dir.path().join(POLICY_FILE)).unwrap();
        assert!(matches!(
            PrunedBlockLedger::open(hardlink_dir.path(), LedgerRetention::default()),
            Err(LedgerError::Invalid(
                "ledger policy must not be hard-linked"
            ))
        ));
        assert_eq!(fs::read(source).unwrap(), b"unchanged");
    }

    #[test]
    fn read_only_audit_verifies_complete_freezer_without_mutation() {
        let dir = TempDir::new().unwrap();
        let retention = LedgerRetention {
            max_blocks: 3,
            max_bytes: 1_000_000,
            slots: 3,
        };
        let ledger = PrunedBlockLedger::open(dir.path(), retention).unwrap();
        ledger.append(10, &[vec![10]]).unwrap();
        ledger.append(11, &[vec![11]]).unwrap();
        let paths = [
            dir.path().join(POLICY_FILE),
            dir.path().join(INDEX_FILE),
            ledger.slot_path(0),
            ledger.slot_path(1),
        ];
        let before = paths
            .iter()
            .map(|path| fs::read(path).unwrap())
            .collect::<Vec<_>>();
        drop(ledger);

        let report = PrunedBlockLedger::audit(dir.path(), 3, 1_000_000).unwrap();
        assert!(report.read_only);
        assert!(report.complete);
        assert!(report.index_valid);
        assert_eq!(report.policy, Some(retention));
        assert_eq!(report.verified_segments, 2);
        assert_eq!(report.verified_blocks, 2);
        assert_eq!(report.first_height, Some(10));
        assert_eq!(report.tip_height, Some(11));
        assert!(report.issues.is_empty());
        assert!(report.repair_plan.is_empty());
        assert_eq!(
            paths
                .iter()
                .map(|path| fs::read(path).unwrap())
                .collect::<Vec<_>>(),
            before
        );
    }

    #[test]
    fn read_only_audit_reports_corruption_and_index_repair_plan() {
        let dir = TempDir::new().unwrap();
        let ledger = PrunedBlockLedger::open(
            dir.path(),
            LedgerRetention {
                max_blocks: 2,
                max_bytes: 1_000_000,
                slots: 2,
            },
        )
        .unwrap();
        ledger.append(10, &[vec![10]]).unwrap();
        ledger.append(11, &[vec![11]]).unwrap();
        let damaged_path = ledger.slot_path(1);
        let mut damaged = fs::read(&damaged_path).unwrap();
        *damaged.last_mut().unwrap() ^= 1;
        fs::write(&damaged_path, &damaged).unwrap();
        drop(ledger);

        let report = PrunedBlockLedger::audit(dir.path(), 2, 1_000_000).unwrap();
        assert!(report.complete);
        assert!(!report.index_valid);
        assert_eq!(report.verified_segments, 1);
        assert!(report.issues.iter().any(|issue| issue.contains("checksum")));
        assert!(
            report
                .repair_plan
                .iter()
                .any(|action| action.contains("rebuild the ledger index"))
        );
        assert_eq!(fs::read(damaged_path).unwrap(), damaged);
    }

    #[test]
    fn incomplete_audit_never_plans_a_destructive_repair() {
        let dir = TempDir::new().unwrap();
        let ledger = PrunedBlockLedger::open(
            dir.path(),
            LedgerRetention {
                max_blocks: 2,
                max_bytes: 1_000_000,
                slots: 2,
            },
        )
        .unwrap();
        ledger.append(10, &[vec![10]]).unwrap();
        ledger.append(11, &[vec![11]]).unwrap();
        drop(ledger);

        let report = PrunedBlockLedger::audit(dir.path(), 1, 1_000_000).unwrap();
        assert!(!report.complete);
        assert_eq!(report.verified_segments, 1);
        assert!(
            report
                .repair_plan
                .iter()
                .any(|action| action.contains("rerun the read-only audit"))
        );
        assert!(
            !report
                .repair_plan
                .iter()
                .any(|action| action.contains("remove"))
        );
    }

    #[test]
    fn missing_index_audit_plans_rebuild_but_does_not_create_it() {
        let dir = TempDir::new().unwrap();
        let ledger = PrunedBlockLedger::open(dir.path(), LedgerRetention::default()).unwrap();
        ledger.append(10, &[vec![10]]).unwrap();
        drop(ledger);
        fs::remove_file(dir.path().join(INDEX_FILE)).unwrap();

        let report = PrunedBlockLedger::audit(dir.path(), 2, 1_000_000).unwrap();
        assert!(report.complete);
        assert!(!report.index_valid);
        assert_eq!(report.first_height, Some(10));
        assert_eq!(report.tip_height, Some(10));
        assert!(!dir.path().join(INDEX_FILE).exists());
    }

    #[test]
    fn physically_removes_slots_pruned_before_ring_wraparound() {
        let dir = TempDir::new().unwrap();
        let ledger = PrunedBlockLedger::open(
            dir.path(),
            LedgerRetention {
                max_blocks: 2,
                max_bytes: 1_000_000,
                slots: 8,
            },
        )
        .unwrap();
        for height in 10..=13 {
            ledger
                .append(height, &[vec![u8::try_from(height).unwrap()]])
                .unwrap();
        }

        assert_eq!(ledger.retained_ranges().unwrap(), vec![(12, 12), (13, 13)]);
        assert!(!ledger.slot_path(0).exists());
        assert!(!ledger.slot_path(1).exists());
        assert!(ledger.slot_path(2).exists());
        assert!(ledger.slot_path(3).exists());
    }

    #[test]
    fn reopen_removes_legacy_slots_absent_from_the_durable_index() {
        let dir = TempDir::new().unwrap();
        let retention = LedgerRetention {
            max_blocks: 2,
            max_bytes: 1_000_000,
            slots: 8,
        };
        let ledger = PrunedBlockLedger::open(dir.path(), retention).unwrap();
        let mut segments = Vec::new();
        for (slot, height) in (0_u16..4).zip(10_u32..14) {
            let path = ledger.slot_path(slot);
            write_archive(&path, height, &[vec![u8::try_from(height).unwrap()]]).unwrap();
            if slot >= 2 {
                segments.push(Segment {
                    first_height: height,
                    block_count: 1,
                    slot,
                    bytes: fs::metadata(path).unwrap().len(),
                });
            }
        }
        ledger
            .write_index(&LedgerIndex {
                next_slot: 4,
                segments,
            })
            .unwrap();
        drop(ledger);

        let reopened = PrunedBlockLedger::open(dir.path(), retention).unwrap();
        assert_eq!(
            reopened.retained_ranges().unwrap(),
            vec![(12, 12), (13, 13)]
        );
        assert!(!reopened.slot_path(0).exists());
        assert!(!reopened.slot_path(1).exists());
        assert!(reopened.slot_path(2).exists());
        assert!(reopened.slot_path(3).exists());
    }

    #[test]
    fn rejects_gaps() {
        let dir = TempDir::new().unwrap();
        let ledger = PrunedBlockLedger::open(dir.path(), LedgerRetention::default()).unwrap();
        ledger.append(10, &[vec![10]]).unwrap();
        assert!(matches!(
            ledger.append(12, &[vec![12]]),
            Err(LedgerError::Invalid("segment is not contiguous"))
        ));
    }

    #[test]
    fn rebuilds_missing_index_and_adopts_a_renamed_orphan_segment() {
        let dir = TempDir::new().unwrap();
        let retention = LedgerRetention {
            max_blocks: 10,
            max_bytes: 1_000_000,
            slots: 3,
        };
        let ledger = PrunedBlockLedger::open(dir.path(), retention).unwrap();
        write_archive(ledger.slot_path(0), 10, &[vec![10]]).unwrap();
        drop(ledger);

        let ledger = PrunedBlockLedger::open(dir.path(), retention).unwrap();
        assert_eq!(ledger.retained_ranges().unwrap(), vec![(10, 10)]);
        write_archive(ledger.slot_path(1), 11, &[vec![11]]).unwrap();
        drop(ledger);

        let ledger = PrunedBlockLedger::open(dir.path(), retention).unwrap();
        assert_eq!(ledger.retained_ranges().unwrap(), vec![(10, 10), (11, 11)]);
        fs::write(ledger.index_path(), b"not json").unwrap();
        drop(ledger);

        let recovered = PrunedBlockLedger::open(dir.path(), retention).unwrap();
        assert_eq!(
            recovered.retained_ranges().unwrap(),
            vec![(10, 10), (11, 11)]
        );
    }

    #[test]
    fn recovers_a_wrapped_slot_rename_before_the_index_commit() {
        let dir = TempDir::new().unwrap();
        let retention = LedgerRetention {
            max_blocks: 2,
            max_bytes: 1_000_000,
            slots: 2,
        };
        let ledger = PrunedBlockLedger::open(dir.path(), retention).unwrap();
        ledger.append(10, &[vec![10]]).unwrap();
        ledger.append(11, &[vec![11]]).unwrap();

        // Simulate interruption after append_locked renamed the new archive
        // over the wrapped slot but before it published the replacement index.
        write_archive(ledger.slot_path(0), 12, &[vec![12]]).unwrap();
        drop(ledger);

        let recovered = PrunedBlockLedger::open(dir.path(), retention).unwrap();
        assert_eq!(
            recovered.retained_ranges().unwrap(),
            vec![(11, 11), (12, 12)]
        );
        assert_eq!(recovered.read_block(10).unwrap(), None);
        assert_eq!(recovered.read_block(11).unwrap(), Some(vec![11]));
        assert_eq!(recovered.read_block(12).unwrap(), Some(vec![12]));
    }

    #[test]
    fn truncates_a_segment_prefix_and_removes_newer_segments() {
        let dir = TempDir::new().unwrap();
        let retention = LedgerRetention {
            max_blocks: 10,
            max_bytes: 1_000_000,
            slots: 3,
        };
        let ledger = PrunedBlockLedger::open(dir.path(), retention).unwrap();
        ledger.append(10, &[vec![10], vec![11], vec![12]]).unwrap();
        ledger.append(13, &[vec![13], vec![14]]).unwrap();

        ledger.truncate_from(12).unwrap();

        assert_eq!(ledger.retained_ranges().unwrap(), vec![(10, 11)]);
        let (manifest, blocks) = read_archive(ledger.slot_path(0)).unwrap();
        assert_eq!(manifest.first_height, 10);
        assert_eq!(manifest.block_count, 2);
        assert_eq!(blocks, vec![vec![10], vec![11]]);
        assert!(!ledger.slot_path(1).exists());
        drop(ledger);

        let reopened = PrunedBlockLedger::open(dir.path(), retention).unwrap();
        assert_eq!(reopened.retained_ranges().unwrap(), vec![(10, 11)]);
        reopened.append(12, &[vec![42]]).unwrap();
        assert_eq!(
            reopened.retained_ranges().unwrap(),
            vec![(10, 11), (12, 12)]
        );
    }

    #[test]
    fn resumes_an_interrupted_truncation_intent_on_open() {
        let dir = TempDir::new().unwrap();
        let retention = LedgerRetention {
            max_blocks: 10,
            max_bytes: 1_000_000,
            slots: 3,
        };
        let ledger = PrunedBlockLedger::open(dir.path(), retention).unwrap();
        ledger.append(10, &[vec![10], vec![11], vec![12]]).unwrap();
        ledger.append(13, &[vec![13]]).unwrap();
        ledger.write_truncate_intent(12).unwrap();
        drop(ledger);

        let recovered = PrunedBlockLedger::open(dir.path(), retention).unwrap();

        assert_eq!(recovered.retained_ranges().unwrap(), vec![(10, 11)]);
        assert!(!recovered.truncate_path().exists());
    }

    #[test]
    fn truncation_recovery_finishes_after_newer_segment_deletion() {
        let dir = TempDir::new().unwrap();
        let retention = LedgerRetention {
            max_blocks: 10,
            max_bytes: 1_000_000,
            slots: 3,
        };
        let ledger = PrunedBlockLedger::open(dir.path(), retention).unwrap();
        ledger.append(10, &[vec![10], vec![11], vec![12]]).unwrap();
        ledger.append(13, &[vec![13], vec![14]]).unwrap();
        ledger.write_truncate_intent(12).unwrap();
        fs::remove_file(ledger.slot_path(1)).unwrap();
        drop(ledger);

        let recovered = PrunedBlockLedger::open(dir.path(), retention).unwrap();
        assert_eq!(recovered.retained_ranges().unwrap(), vec![(10, 11)]);
        assert_eq!(recovered.read_block(11).unwrap(), Some(vec![11]));
        assert_eq!(recovered.read_block(12).unwrap(), None);
        assert!(!recovered.truncate_path().exists());
    }

    #[test]
    fn truncation_recovery_finishes_after_prefix_rewrite() {
        let dir = TempDir::new().unwrap();
        let retention = LedgerRetention {
            max_blocks: 10,
            max_bytes: 1_000_000,
            slots: 3,
        };
        let ledger = PrunedBlockLedger::open(dir.path(), retention).unwrap();
        ledger.append(10, &[vec![10], vec![11], vec![12]]).unwrap();
        ledger.append(13, &[vec![13], vec![14]]).unwrap();
        ledger.write_truncate_intent(12).unwrap();
        write_archive(ledger.slot_path(0), 10, &[vec![10], vec![11]]).unwrap();
        drop(ledger);

        let recovered = PrunedBlockLedger::open(dir.path(), retention).unwrap();
        assert_eq!(recovered.retained_ranges().unwrap(), vec![(10, 11)]);
        assert!(!recovered.slot_path(1).exists());
        assert!(!recovered.truncate_path().exists());
    }

    #[test]
    fn malformed_truncation_intent_fails_closed_without_pruning() {
        let dir = TempDir::new().unwrap();
        let retention = LedgerRetention {
            max_blocks: 10,
            max_bytes: 1_000_000,
            slots: 3,
        };
        let ledger = PrunedBlockLedger::open(dir.path(), retention).unwrap();
        ledger.append(10, &[vec![10], vec![11]]).unwrap();
        fs::write(ledger.truncate_path(), [1, 2, 3]).unwrap();
        drop(ledger);

        assert!(matches!(
            PrunedBlockLedger::open(dir.path(), retention),
            Err(LedgerError::Invalid("truncate intent"))
        ));
        let (_, blocks) = read_archive(dir.path().join("blk-0000.rblk")).unwrap();
        assert_eq!(blocks, vec![vec![10], vec![11]]);
    }

    #[test]
    fn reads_retained_blocks_by_height_and_reports_the_tip() {
        let dir = TempDir::new().unwrap();
        let ledger = PrunedBlockLedger::open(dir.path(), LedgerRetention::default()).unwrap();
        assert_eq!(ledger.retained_tip().unwrap(), None);
        assert_eq!(ledger.read_block(10).unwrap(), None);

        ledger.append(10, &[vec![10], vec![11]]).unwrap();
        ledger.append(12, &[vec![12]]).unwrap();

        assert_eq!(ledger.retained_tip().unwrap(), Some(12));
        assert_eq!(ledger.read_block(9).unwrap(), None);
        assert_eq!(ledger.read_block(10).unwrap(), Some(vec![10]));
        assert_eq!(ledger.read_block(11).unwrap(), Some(vec![11]));
        assert_eq!(ledger.read_block(12).unwrap(), Some(vec![12]));
        assert_eq!(ledger.read_block(13).unwrap(), None);
    }

    #[test]
    fn staged_blocks_are_hidden_until_a_validated_prefix_is_committed() {
        let dir = TempDir::new().unwrap();
        let ledger = PrunedBlockLedger::open(dir.path(), LedgerRetention::default()).unwrap();
        ledger.stage(10, &[vec![10], vec![11], vec![12]]).unwrap();

        assert_eq!(ledger.retained_tip().unwrap(), None);
        assert_eq!(ledger.staged().unwrap().unwrap().blocks.len(), 3);
        ledger.commit_staged(2).unwrap();

        assert_eq!(ledger.retained_ranges().unwrap(), vec![(10, 11)]);
        assert_eq!(ledger.read_block(10).unwrap(), Some(vec![10]));
        assert_eq!(ledger.read_block(11).unwrap(), Some(vec![11]));
        assert_eq!(ledger.read_block(12).unwrap(), None);
        assert!(ledger.staged().unwrap().is_none());
    }

    #[test]
    fn full_staged_segment_is_published_without_reencoding() {
        let dir = TempDir::new().unwrap();
        let durability = Arc::new(FailOnceDurability::new(LedgerSyncPoint::SlotArchive));
        let ledger = PrunedBlockLedger::open_with_durability(
            dir.path(),
            LedgerRetention::default(),
            durability.clone(),
        )
        .unwrap();
        ledger.stage(10, &[vec![10], vec![11], vec![12]]).unwrap();
        durability.arm();

        ledger.commit_staged(3).unwrap();

        assert!(!durability.did_fail());
        assert!(ledger.staged().unwrap().is_none());
        assert_eq!(ledger.retained_ranges().unwrap(), vec![(10, 12)]);
        assert_eq!(ledger.read_block(11).unwrap(), Some(vec![11]));
    }

    #[test]
    fn staged_commit_recovers_after_the_prefix_was_already_published() {
        let dir = TempDir::new().unwrap();
        let ledger = PrunedBlockLedger::open(dir.path(), LedgerRetention::default()).unwrap();
        ledger.stage(10, &[vec![10], vec![11]]).unwrap();
        ledger.append(10, &[vec![10], vec![11]]).unwrap();

        ledger.commit_staged(2).unwrap();

        assert_eq!(ledger.retained_ranges().unwrap(), vec![(10, 11)]);
        assert!(ledger.staged().unwrap().is_none());
    }

    #[test]
    fn staged_commit_recovers_after_archive_rename_before_index_commit() {
        let dir = TempDir::new().unwrap();
        let retention = LedgerRetention {
            max_blocks: 10,
            max_bytes: 1_000_000,
            slots: 3,
        };
        let ledger = PrunedBlockLedger::open(dir.path(), retention).unwrap();
        ledger.append(10, &[vec![10], vec![11]]).unwrap();
        ledger.stage(12, &[vec![12], vec![13]]).unwrap();

        // Simulate commit_staged/append_locked publishing its archive rename
        // and then losing power before write_index.
        write_archive(ledger.slot_path(1), 12, &[vec![12]]).unwrap();
        drop(ledger);

        let recovered = PrunedBlockLedger::open(dir.path(), retention).unwrap();
        assert_eq!(
            recovered.retained_ranges().unwrap(),
            vec![(10, 11), (12, 12)]
        );
        assert_eq!(recovered.staged().unwrap().unwrap().blocks.len(), 2);
        recovered.commit_staged(1).unwrap();
        assert!(recovered.staged().unwrap().is_none());
        assert_eq!(recovered.read_block(12).unwrap(), Some(vec![12]));
        assert_eq!(recovered.read_block(13).unwrap(), None);
    }

    #[test]
    fn rejects_a_segment_larger_than_the_block_retention_limit() {
        let dir = TempDir::new().unwrap();
        let ledger = PrunedBlockLedger::open(
            dir.path(),
            LedgerRetention {
                max_blocks: 1,
                max_bytes: 1_000_000,
                slots: 2,
            },
        )
        .unwrap();

        assert!(matches!(
            ledger.append(10, &[vec![10], vec![11]]),
            Err(LedgerError::Invalid(
                "single segment exceeds maximum blocks"
            ))
        ));
        assert_eq!(ledger.retained_tip().unwrap(), None);
    }

    #[test]
    fn staged_archive_sync_failures_reopen_to_a_complete_state() {
        for point in [
            LedgerSyncPoint::StagedArchive,
            LedgerSyncPoint::StagedPublish,
        ] {
            let dir = TempDir::new().unwrap();
            let durability = Arc::new(FailOnceDurability::new(point));
            let ledger = PrunedBlockLedger::open_with_durability(
                dir.path(),
                LedgerRetention::default(),
                durability.clone(),
            )
            .unwrap();
            durability.arm();

            assert!(matches!(
                ledger.stage(10, &[vec![10], vec![11]]),
                Err(LedgerError::Io(_))
            ));
            assert!(durability.did_fail());
            drop(ledger);

            let recovered =
                PrunedBlockLedger::open(dir.path(), LedgerRetention::default()).unwrap();
            match point {
                LedgerSyncPoint::StagedArchive => {
                    assert!(recovered.staged().unwrap().is_none());
                }
                LedgerSyncPoint::StagedPublish => {
                    assert_eq!(
                        recovered.staged().unwrap().unwrap().blocks,
                        vec![vec![10], vec![11]]
                    );
                }
                _ => unreachable!(),
            }
            assert_eq!(recovered.retained_tip().unwrap(), None);
        }
    }

    #[test]
    fn wrapped_slot_sync_failures_recover_the_old_or_published_ring() {
        let retention = LedgerRetention {
            max_blocks: 2,
            max_bytes: 1_000_000,
            slots: 2,
        };
        for point in [
            LedgerSyncPoint::SlotArchive,
            LedgerSyncPoint::SlotPublish,
            LedgerSyncPoint::IndexFile,
            LedgerSyncPoint::IndexPublish,
        ] {
            let dir = TempDir::new().unwrap();
            let durability = Arc::new(FailOnceDurability::new(point));
            let ledger =
                PrunedBlockLedger::open_with_durability(dir.path(), retention, durability.clone())
                    .unwrap();
            ledger.append(10, &[vec![10]]).unwrap();
            ledger.append(11, &[vec![11]]).unwrap();
            durability.arm();

            assert!(matches!(
                ledger.append(12, &[vec![12]]),
                Err(LedgerError::Io(_))
            ));
            assert!(durability.did_fail());
            drop(ledger);

            let recovered = PrunedBlockLedger::open(dir.path(), retention).unwrap();
            if point == LedgerSyncPoint::SlotArchive {
                assert_eq!(
                    recovered.retained_ranges().unwrap(),
                    vec![(10, 10), (11, 11)]
                );
                assert_eq!(recovered.read_block(10).unwrap(), Some(vec![10]));
                assert_eq!(recovered.read_block(12).unwrap(), None);
            } else {
                assert_eq!(
                    recovered.retained_ranges().unwrap(),
                    vec![(11, 11), (12, 12)]
                );
                assert_eq!(recovered.read_block(10).unwrap(), None);
                assert_eq!(recovered.read_block(12).unwrap(), Some(vec![12]));
            }
        }
    }

    #[test]
    fn staged_removal_sync_failure_keeps_the_published_prefix_recoverable() {
        let dir = TempDir::new().unwrap();
        let durability = Arc::new(FailOnceDurability::new(LedgerSyncPoint::StagedRemoval));
        let ledger = PrunedBlockLedger::open_with_durability(
            dir.path(),
            LedgerRetention::default(),
            durability.clone(),
        )
        .unwrap();
        ledger.stage(10, &[vec![10], vec![11], vec![12]]).unwrap();
        durability.arm();

        assert!(matches!(ledger.commit_staged(2), Err(LedgerError::Io(_))));
        assert!(durability.did_fail());
        drop(ledger);

        let recovered = PrunedBlockLedger::open(dir.path(), LedgerRetention::default()).unwrap();
        assert_eq!(recovered.retained_ranges().unwrap(), vec![(10, 11)]);
        assert!(recovered.staged().unwrap().is_none());
    }

    #[test]
    fn retired_slot_sync_failure_reopens_to_the_pruned_ring() {
        let dir = TempDir::new().unwrap();
        let retention = LedgerRetention {
            max_blocks: 2,
            max_bytes: 1_000_000,
            slots: 4,
        };
        let durability = Arc::new(FailOnceDurability::new(LedgerSyncPoint::RetiredSlotRemoval));
        let ledger =
            PrunedBlockLedger::open_with_durability(dir.path(), retention, durability.clone())
                .unwrap();
        ledger.append(10, &[vec![10]]).unwrap();
        ledger.append(11, &[vec![11]]).unwrap();
        durability.arm();

        assert!(matches!(
            ledger.append(12, &[vec![12]]),
            Err(LedgerError::Io(_))
        ));
        assert!(durability.did_fail());
        drop(ledger);

        let recovered = PrunedBlockLedger::open(dir.path(), retention).unwrap();
        assert_eq!(
            recovered.retained_ranges().unwrap(),
            vec![(11, 11), (12, 12)]
        );
        assert_eq!(recovered.read_block(10).unwrap(), None);
        assert_eq!(recovered.read_block(12).unwrap(), Some(vec![12]));
    }

    #[test]
    fn truncation_sync_failures_resume_or_preserve_the_old_ring() {
        let retention = LedgerRetention {
            max_blocks: 10,
            max_bytes: 1_000_000,
            slots: 3,
        };
        for point in [
            LedgerSyncPoint::TruncateIntentFile,
            LedgerSyncPoint::TruncateIntentPublish,
            LedgerSyncPoint::TruncateArchive,
            LedgerSyncPoint::TruncateMutation,
            LedgerSyncPoint::IndexFile,
            LedgerSyncPoint::IndexPublish,
            LedgerSyncPoint::TruncateIntentRemoval,
        ] {
            let dir = TempDir::new().unwrap();
            let durability = Arc::new(FailOnceDurability::new(point));
            let ledger =
                PrunedBlockLedger::open_with_durability(dir.path(), retention, durability.clone())
                    .unwrap();
            ledger.append(10, &[vec![10], vec![11], vec![12]]).unwrap();
            ledger.append(13, &[vec![13], vec![14]]).unwrap();
            durability.arm();

            assert!(matches!(ledger.truncate_from(12), Err(LedgerError::Io(_))));
            assert!(durability.did_fail());
            drop(ledger);

            let recovered = PrunedBlockLedger::open(dir.path(), retention).unwrap();
            if point == LedgerSyncPoint::TruncateIntentFile {
                assert_eq!(
                    recovered.retained_ranges().unwrap(),
                    vec![(10, 12), (13, 14)]
                );
                assert_eq!(recovered.read_block(14).unwrap(), Some(vec![14]));
            } else {
                assert_eq!(recovered.retained_ranges().unwrap(), vec![(10, 11)]);
                assert_eq!(recovered.read_block(11).unwrap(), Some(vec![11]));
                assert_eq!(recovered.read_block(12).unwrap(), None);
                assert!(!recovered.truncate_path().exists());
            }
        }
    }
}
