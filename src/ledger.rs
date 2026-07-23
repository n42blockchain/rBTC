//! Configurable, circular historical block retention.
//!
//! This component is intentionally independent of UTXO state. Deleting an old
//! block segment is pruning, not an undo of validated chainstate.

use std::{
    fs::{self, File},
    io::{self, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex, MutexGuard},
};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::archive::{
    ArchiveError, ArchiveManifest, read_archive, read_archive_manifest, write_archive,
};

const INDEX_FILE: &str = "ledger-index.json";
const TRUNCATE_FILE: &str = "ledger-truncate";
const STAGED_FILE: &str = "ledger-staged.rblk";

/// Default approximate one-week historical retention at ten-minute blocks.
pub const DEFAULT_RETENTION_BLOCKS: u32 = 1_008;
/// Default maximum compressed ledger footprint (1 GiB).
pub const DEFAULT_MAX_BYTES: u64 = 1024 * 1024 * 1024;

/// Retention settings for the rotating archive.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
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
        let (manifest, blocks) = read_archive(self.staged_path())?;
        if block_count > manifest.block_count {
            return Err(LedgerError::Invalid("staged commit exceeds segment"));
        }
        let count = usize::try_from(block_count).expect("staged block count fits usize");
        let prefix = &blocks[..count];
        let index = self.read_index()?;
        let retained_next = index
            .segments
            .last()
            .map(segment_end_exclusive)
            .transpose()?;
        match retained_next {
            None => {
                self.append_locked(manifest.first_height, prefix)?;
            }
            Some(next) if next == manifest.first_height => {
                self.append_locked(manifest.first_height, prefix)?;
            }
            Some(next)
                if manifest.first_height.checked_add(block_count) == Some(next)
                    && self.retained_bytes_match(&index, manifest.first_height, prefix)? => {}
            Some(_) => {
                return Err(LedgerError::Invalid(
                    "staged segment does not extend ledger tip",
                ));
            }
        }
        fs::remove_file(self.staged_path())?;
        self.sync_directory(LedgerSyncPoint::StagedRemoval)
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
