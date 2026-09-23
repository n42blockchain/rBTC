//! Bounded disk journal for one contiguous, contextually validated header fork.
//!
//! This is a recovery primitive, not a node promotion policy. The caller must
//! pin the anchor in its validated DAG and separately arrange atomic promotion
//! and block execution. Reopen revalidates raw headers; disk chainwork is never
//! trusted. Only a final incomplete frame is discarded after successful replay.

use crate::headers::{CandidateContext, HeaderError, HeaderInfo, HeaderView, HeaderWorkBudget};
use bitcoin::{
    BlockHash,
    block::Header,
    consensus::{deserialize, serialize},
    hashes::Hash,
};
use fs2::FileExt;
use sha2::{Digest, Sha256};
use std::{
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::Path,
};
use thiserror::Error;

const PREFIX_LEN: usize = 72;
const PREFIX_BYTES: u64 = PREFIX_LEN as u64;
const MAX_BATCH: usize = 2_000;
const MAGIC: &[u8; 8] = b"RBTCFK01";

/// File-length and batch bounds checked before writes or frame allocation.
#[derive(Clone, Copy, Debug)]
pub struct HeaderCandidateLimits {
    /// Maximum journal file length, including framing. Filesystem metadata and
    /// allocation granularity are separate from this logical byte limit.
    pub max_file_bytes: u64,
    /// Maximum newly appended headers per frame, at most 2,000. Reopen accepts
    /// historical frames up to the format's fixed 2,000-header ceiling.
    pub max_batch_headers: usize,
}

impl Default for HeaderCandidateLimits {
    fn default() -> Self {
        Self {
            max_file_bytes: 128 * 1024 * 1024,
            max_batch_headers: MAX_BATCH,
        }
    }
}

/// Local journal/recovery errors; resource and I/O failures are not peer faults.
#[derive(Debug, Error)]
pub enum HeaderCandidateError {
    /// Filesystem or exclusive-lock failure.
    #[error("header candidate I/O: {0}")]
    Io(#[from] std::io::Error),
    /// Invalid journal format, identity or checksum; the file is preserved.
    #[error("malformed header candidate: {0}")]
    Malformed(&'static str),
    /// Byte or batch allowance exhausted before mutation.
    #[error("header candidate resource deferred: {0}")]
    Deferred(&'static str),
    /// Header consensus validation or logical-work deferral.
    #[error("header candidate validation: {0}")]
    Header(#[from] HeaderError),
}

/// Exclusive, append-only fork journal with bounded in-memory consensus context.
/// A successful append syncs a checksum-protected complete frame before exposing
/// the new tip. Failed appends restore the prior file boundary or poison this
/// handle. A crash may expose the old or new complete batch, never a prefix of it.
pub struct DiskHeaderCandidate {
    file: File,
    storage: crate::header_storage_budget::RegisteredFile,
    context: CandidateContext,
    anchor: BlockHash,
    limits: HeaderCandidateLimits,
    bytes: u64,
    count: u64,
    poisoned: bool,
}

impl DiskHeaderCandidate {
    /// Reads an untrusted anchor hint without loading candidate history.
    /// The caller must resolve it in a validated DAG and replay the journal.
    pub fn stored_anchor(
        path: impl AsRef<Path>,
    ) -> Result<Option<BlockHash>, HeaderCandidateError> {
        let mut file = match File::open(path) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        let mut prefix = [0; PREFIX_LEN];
        file.read_exact(&mut prefix)?;
        if &prefix[..8] != MAGIC {
            return Err(HeaderCandidateError::Malformed("journal identity"));
        }
        Ok(Some(BlockHash::from_byte_array(
            prefix[8..40].try_into().expect("fixed hash width"),
        )))
    }

    /// Opens/creates a candidate and streams contextual revalidation from its
    /// pinned anchor. The supplied work allowance also bounds startup replay.
    /// Work deferral leaves the existing journal, including any tail, untouched.
    pub fn open(
        path: impl AsRef<Path>,
        source: &dyn HeaderView,
        anchor: BlockHash,
        adjusted_time: u32,
        limits: HeaderCandidateLimits,
        work: &mut HeaderWorkBudget,
    ) -> Result<Self, HeaderCandidateError> {
        let mut recovery = Self::start_recovery(path, source, anchor, limits, work)?;
        recovery.advance(usize::MAX, adjusted_time, work)?;
        recovery.finish()
    }

    /// Opens the journal identity and bounded anchor context without replaying
    /// history. Drive `advance` in scheduler-sized slices before `finish`.
    pub fn start_recovery<'a>(
        path: impl AsRef<Path>,
        source: &'a dyn HeaderView,
        anchor: BlockHash,
        limits: HeaderCandidateLimits,
        work: &mut HeaderWorkBudget,
    ) -> Result<HeaderCandidateRecovery<'a>, HeaderCandidateError> {
        if limits.max_batch_headers == 0
            || limits.max_batch_headers > MAX_BATCH
            || limits.max_file_bytes < PREFIX_BYTES
        {
            return Err(HeaderCandidateError::Deferred("invalid journal limits"));
        }
        // Charge context construction before bounded ancestry copies.
        work.consume(16_384)?;
        let context = CandidateContext::new(source, anchor)?;
        let path = path.as_ref();
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path)?;
        file.try_lock_exclusive()?;
        let length = file.metadata()?.len();
        let storage = crate::header_storage_budget::RegisteredFile::open(path, length)?;
        if length > limits.max_file_bytes {
            return Err(HeaderCandidateError::Deferred(
                "existing file exceeds byte allowance",
            ));
        }
        let mut prefix = Vec::with_capacity(PREFIX_LEN);
        prefix.extend_from_slice(MAGIC);
        prefix.extend_from_slice(anchor.as_byte_array());
        prefix.extend_from_slice(&Sha256::digest(context.consensus_id()));
        if length == 0 {
            storage.reserve(PREFIX_BYTES)?;
            file.write_all(&prefix)?;
            file.sync_all()?;
            #[cfg(unix)]
            File::open(
                path.parent()
                    .filter(|p| !p.as_os_str().is_empty())
                    .unwrap_or(Path::new(".")),
            )?
            .sync_all()?;
        } else {
            let mut actual = [0; PREFIX_LEN];
            file.read_exact(&mut actual)?;
            if actual.as_slice() != prefix {
                return Err(HeaderCandidateError::Malformed(
                    "anchor/configuration identity",
                ));
            }
        }
        let result = Self {
            file,
            storage,
            context,
            anchor,
            limits,
            bytes: PREFIX_BYTES,
            count: 0,
            poisoned: false,
        };
        Ok(HeaderCandidateRecovery {
            candidate: result,
            source,
            end: length.max(PREFIX_BYTES),
        })
    }

    /// Last fully validated and durably committed candidate header.
    pub fn tip(&self) -> HeaderInfo {
        self.context.tip()
    }

    /// Returns the validated anchor without reopening the exclusively locked journal.
    pub fn anchor(&self) -> BlockHash {
        self.anchor
    }
    /// Number of candidate headers, excluding the anchor.
    pub const fn len(&self) -> u64 {
        self.count
    }
    /// Whether the journal contains only its anchor identity.
    pub const fn is_empty(&self) -> bool {
        self.count == 0
    }
    /// Committed logical file bytes, including framing.
    pub const fn file_bytes(&self) -> u64 {
        self.bytes
    }
    /// Retained consensus entries; independent of total fork length.
    pub fn resident_context_entries(&self) -> usize {
        self.context.entries()
    }

    /// Locator starts at the durable candidate tip, falling back to its pinned
    /// anchor's original ancestry. No full candidate ancestry vector is built.
    pub fn block_locator(
        &self,
        source: &dyn HeaderView,
        work: &mut HeaderWorkBudget,
    ) -> Result<Vec<BlockHash>, HeaderCandidateError> {
        let anchor = source
            .header(&self.anchor)
            .map_err(HeaderError::from)?
            .ok_or(HeaderError::UnknownParent(self.anchor))?;
        work.consume(u64::from(anchor.height) + 64)?;
        let mut locator = source
            .branch_locator(self.anchor)
            .map_err(HeaderError::from)?
            .ok_or(HeaderError::UnknownParent(self.anchor))?;
        if self.tip().hash != self.anchor {
            locator.insert(0, self.tip().hash);
        }
        Ok(locator)
    }

    /// Contextually validates a contiguous batch and durably commits it whole.
    /// The retained DAG and active tip are never modified by this primitive.
    pub fn append(
        &mut self,
        batch: &[Header],
        adjusted_time: u32,
        work: &mut HeaderWorkBudget,
    ) -> Result<(), HeaderCandidateError> {
        if self.poisoned {
            return Err(HeaderCandidateError::Malformed(
                "poisoned handle; reopen required",
            ));
        }
        if batch.is_empty() {
            return Ok(());
        }
        if batch.len() > self.limits.max_batch_headers {
            return Err(HeaderCandidateError::Deferred("batch size"));
        }
        let frame_bytes = 4 + batch.len() * 80 + 32;
        let end = self
            .bytes
            .checked_add(frame_bytes as u64)
            .filter(|end| *end <= self.limits.max_file_bytes)
            .ok_or(HeaderCandidateError::Deferred("file byte allowance"))?;
        work.consume(self.context.entries() as u64 + frame_bytes as u64 / 64 + 1)?;
        let mut next = self.context.clone();
        let mut frame = Vec::with_capacity(frame_bytes);
        frame.extend_from_slice(
            &u32::try_from(batch.len())
                .expect("bounded batch")
                .to_le_bytes(),
        );
        for header in batch {
            next.accept(*header, adjusted_time, work)?;
            frame.extend_from_slice(&serialize(header));
        }
        frame.extend_from_slice(&Sha256::digest(&frame));
        if self.file.metadata()?.len() != self.bytes {
            return Err(HeaderCandidateError::Malformed(
                "journal changed outside locked handle",
            ));
        }
        self.storage.reserve(end)?;
        self.file.seek(SeekFrom::Start(self.bytes))?;
        self.poisoned = true;
        if let Err(error) = self
            .file
            .write_all(&frame)
            .and_then(|()| self.file.sync_data())
        {
            self.file.set_len(self.bytes)?;
            self.file.sync_data()?;
            self.storage.truncated(self.bytes)?;
            self.poisoned = false;
            return Err(error.into());
        }
        self.context = next;
        self.bytes = end;
        self.count += batch.len() as u64;
        self.poisoned = false;
        Ok(())
    }

    /// Opens a bounded read cursor over this already validated journal. Holding
    /// the cursor prevents appends through this handle until it is dropped.
    pub fn reader(&mut self) -> Result<HeaderCandidateReader<'_>, HeaderCandidateError> {
        if self.poisoned {
            return Err(HeaderCandidateError::Malformed(
                "poisoned handle; reopen required",
            ));
        }
        Ok(HeaderCandidateReader {
            candidate: self,
            offset: PREFIX_BYTES,
        })
    }

    /// Streams committed frames without materializing the candidate chain.
    /// The callback shares the reader's work ledger and owns its publication policy.
    pub fn visit_batches(
        &mut self,
        work: &mut HeaderWorkBudget,
        mut visit: impl FnMut(&[Header], &mut HeaderWorkBudget) -> Result<(), HeaderCandidateError>,
    ) -> Result<(), HeaderCandidateError> {
        let mut reader = self.reader()?;
        while let Some(batch) = reader.next_batch(work)? {
            visit(&batch, work)?;
        }
        Ok(())
    }
}

/// A read cursor whose position advances only after a complete checked frame.
/// The caller may yield or replenish a shared lease between bounded frames.
pub struct HeaderCandidateReader<'a> {
    candidate: &'a mut DiskHeaderCandidate,
    offset: u64,
}
impl HeaderCandidateReader<'_> {
    /// Reads one committed frame. Failure leaves its read boundary unchanged.
    pub fn next_batch(
        &mut self,
        work: &mut HeaderWorkBudget,
    ) -> Result<Option<Vec<Header>>, HeaderCandidateError> {
        if self.offset == self.candidate.bytes {
            return Ok(None);
        }
        let batch = read_frame(
            &mut self.candidate.file,
            self.offset,
            self.candidate.bytes,
            work,
        )?
        .ok_or(HeaderCandidateError::Malformed(
            "committed frame is truncated",
        ))?;
        self.offset += 36 + batch.len() as u64 * 80;
        Ok(Some(batch))
    }
}

/// Incremental startup revalidation. Only the successfully checked prefix's
/// bounded context is retained between slices. No usable candidate is returned
/// until every complete frame has passed contextual validation.
pub struct HeaderCandidateRecovery<'a> {
    source: &'a dyn HeaderView,
    candidate: DiskHeaderCandidate,
    end: u64,
}

impl HeaderCandidateRecovery<'_> {
    /// Validated header count so far, for scheduler progress reporting.
    pub const fn validated_headers(&self) -> u64 {
        self.candidate.count
    }

    /// Advances at most `max_batches` complete frames. Exhausted work preserves
    /// the previous checked-frame boundary, so a later slice can retry safely.
    /// Returns true only after the complete file and any partial tail are handled.
    pub fn advance(
        &mut self,
        max_batches: usize,
        now: u32,
        work: &mut HeaderWorkBudget,
    ) -> Result<bool, HeaderCandidateError> {
        if self.candidate.poisoned {
            return Err(HeaderCandidateError::Malformed(
                "poisoned recovery; reopen required",
            ));
        }
        if max_batches == 0 {
            return Err(HeaderCandidateError::Deferred("zero replay slice"));
        }
        if self.candidate.file.metadata()?.len() != self.end {
            return Err(HeaderCandidateError::Malformed(
                "journal changed during recovery",
            ));
        }
        for _ in 0..max_batches {
            if self.candidate.bytes == self.end {
                return Ok(true);
            }
            let Some(batch) = read_frame(
                &mut self.candidate.file,
                self.candidate.bytes,
                self.end,
                work,
            )?
            else {
                // No complete corrupt frame is discarded. A failed truncation
                // or sync requires reopening instead of continuing stale state.
                self.candidate.poisoned = true;
                self.candidate.file.set_len(self.candidate.bytes)?;
                self.candidate.file.sync_data()?;
                self.candidate.storage.truncated(self.candidate.bytes)?;
                self.end = self.candidate.bytes;
                self.candidate.poisoned = false;
                return Ok(true);
            };
            work.consume(self.candidate.context.entries() as u64)?;
            let mut next = self.candidate.context.clone();
            for header in &batch {
                work.consume(2)?;
                let known = self
                    .source
                    .header(&header.block_hash())
                    .map_err(HeaderError::from)?;
                next.accept_replayed(*header, now, work, known)?;
            }
            self.candidate.context = next;
            self.candidate.bytes += 36 + batch.len() as u64 * 80;
            self.candidate.count += batch.len() as u64;
        }
        Ok(self.candidate.bytes == self.end)
    }

    /// Releases a fully replayed candidate for appending or streamed promotion.
    /// Calling this early returns local deferral and leaves the file untouched.
    pub fn finish(self) -> Result<DiskHeaderCandidate, HeaderCandidateError> {
        if self.candidate.poisoned || self.candidate.bytes != self.end {
            return Err(HeaderCandidateError::Deferred("recovery has not completed"));
        }
        Ok(self.candidate)
    }
}

// None means only a final incomplete frame, not a complete corrupt record.
fn read_frame(
    file: &mut File,
    offset: u64,
    end: u64,
    work: &mut HeaderWorkBudget,
) -> Result<Option<Vec<Header>>, HeaderCandidateError> {
    if end - offset < 4 {
        return Ok(None);
    }
    file.seek(SeekFrom::Start(offset))?;
    let mut count = [0; 4];
    file.read_exact(&mut count)?;
    let count = u32::from_le_bytes(count) as usize;
    if count == 0 || count > MAX_BATCH {
        return Err(HeaderCandidateError::Malformed("frame count"));
    }
    let frame_bytes = 36 + count * 80;
    if end - offset < frame_bytes as u64 {
        return Ok(None);
    }
    work.consume(frame_bytes as u64 / 64 + count as u64 + 1)?;
    let mut frame = vec![0; frame_bytes];
    file.seek(SeekFrom::Start(offset))?;
    file.read_exact(&mut frame)?;
    let payload_end = frame_bytes - 32;
    if Sha256::digest(&frame[..payload_end]).as_slice() != &frame[payload_end..] {
        return Err(HeaderCandidateError::Malformed("frame checksum"));
    }
    let batch = frame[4..payload_end]
        .chunks_exact(80)
        .map(|raw| deserialize(raw).map_err(|_| HeaderCandidateError::Malformed("header encoding")))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Some(batch))
}

#[cfg(test)]
use crate::headers::HeaderDag;
#[cfg(test)]
mod tests;
