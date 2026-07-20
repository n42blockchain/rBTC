//! Configurable, circular historical block retention.
//!
//! This component is intentionally independent of UTXO state. Deleting an old
//! block segment is pruning, not an undo of validated chainstate.

use std::{
    fs,
    path::{Path, PathBuf},
    sync::{Mutex, MutexGuard},
};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::archive::{ArchiveError, ArchiveManifest, write_archive};

const INDEX_FILE: &str = "ledger-index.json";

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

impl Default for LedgerRetention {
    fn default() -> Self {
        Self {
            max_blocks: DEFAULT_RETENTION_BLOCKS,
            max_bytes: DEFAULT_MAX_BYTES,
            slots: 8,
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

#[derive(Clone, Debug, Deserialize, Serialize)]
struct Segment {
    first_height: u32,
    block_count: u32,
    slot: u16,
    bytes: u64,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
struct LedgerIndex {
    next_slot: u16,
    segments: Vec<Segment>,
}

/// A rotating file-ring for locally retained, consensus-serialized blocks.
pub struct PrunedBlockLedger {
    root: PathBuf,
    retention: LedgerRetention,
    write_guard: Mutex<()>,
}

impl PrunedBlockLedger {
    /// Opens a ledger rooted in an application-specific directory.
    pub fn open(root: impl AsRef<Path>, retention: LedgerRetention) -> Result<Self, LedgerError> {
        if retention.max_blocks == 0 || retention.max_bytes == 0 || retention.slots == 0 {
            return Err(LedgerError::Invalid(
                "all retention limits must be non-zero",
            ));
        }
        fs::create_dir_all(root.as_ref())?;
        Ok(Self {
            root: root.as_ref().to_path_buf(),
            retention,
            write_guard: Mutex::new(()),
        })
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
        if blocks.is_empty() {
            return Err(LedgerError::Invalid("empty segment"));
        }
        let _guard = self.lock();
        let mut index = self.read_index()?;
        let block_count =
            u32::try_from(blocks.len()).map_err(|_| LedgerError::Invalid("too many blocks"))?;
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
            return Err(LedgerError::Invalid("single segment exceeds maximum bytes"));
        }
        fs::rename(&temporary, &destination)?;
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
                let end = segment
                    .first_height
                    .checked_add(segment.block_count - 1)
                    .ok_or(LedgerError::Invalid("height overflow"))?;
                Ok((segment.first_height, end))
            })
            .collect()
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
        fs::write(&temporary, serde_json::to_vec(index)?)?;
        fs::rename(temporary, self.index_path())?;
        Ok(())
    }
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
    use tempfile::TempDir;

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
}
