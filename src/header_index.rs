//! Revalidated disk header index with immutable selected-chain read views.
//!
//! This is a derived index: construction starts from genesis and accepts raw
//! headers only through contextual validation. There is deliberately no API to
//! reopen persisted height/work caches as trusted state. Callers must replay
//! authoritative raw history when creating a fresh index after restart.

use std::{collections::HashMap, path::Path, sync::Arc};

use bitcoin::{
    BlockHash,
    block::Header,
    consensus::{deserialize, serialize},
    hashes::Hash,
    pow::Work,
};
use redb::{Database, ReadTransaction, ReadableTable, TableDefinition, WriteTransaction};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::{
    deployments::DeploymentConfig,
    headers::{
        CandidateContext, HeaderDag, HeaderError, HeaderInfo, HeaderReadError, HeaderView,
        HeaderWorkBudget,
    },
};

const RECORDS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("validated_header_records");
const CHILDREN: TableDefinition<&[u8], u64> = TableDefinition::new("validated_header_children");
const LEAVES: TableDefinition<&[u8], ()> = TableDefinition::new("validated_header_leaves");
mod retention;
mod scratch;
pub use retention::StagedDiskHeaderEviction;
use scratch::Scratch;
const MAX_BATCH: usize = 2_000;
const CACHE_BYTES: usize = 8 * 1024 * 1024;

/// Failure building a local, revalidated disk index.
#[derive(Debug, Error)]
pub enum HeaderIndexError {
    /// A raw header failed validation or its work allowance was exhausted.
    #[error("header index validation: {0}")]
    Header(#[from] HeaderError),
    /// A local read or write failed.
    #[error("header index storage: {0}")]
    Read(#[from] HeaderReadError),
    /// The request exceeds the bounded staging frame.
    #[error("header index batch exceeds {MAX_BATCH} headers")]
    BatchLimit,
}

fn local(error: impl std::fmt::Display) -> HeaderReadError {
    HeaderReadError::Unavailable(error.to_string())
}

#[derive(Clone, Copy)]
struct Record {
    info: HeaderInfo,
    skip: BlockHash,
}

impl Record {
    fn encode(self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(180);
        bytes.extend(serialize(&self.info.header));
        bytes.extend(self.info.height.to_le_bytes());
        bytes.extend(self.info.chainwork.to_be_bytes());
        bytes.extend(self.skip.to_byte_array());
        let digest = Sha256::digest(&bytes);
        bytes.extend(digest);
        bytes
    }
    fn decode(hash: BlockHash, bytes: &[u8]) -> Result<Self, HeaderReadError> {
        if bytes.len() != 180 || Sha256::digest(&bytes[..148]).as_slice() != &bytes[148..] {
            return Err(HeaderReadError::Inconsistent(
                "header index record checksum",
            ));
        }
        let header: Header = deserialize(&bytes[..80]).map_err(local)?;
        if header.block_hash() != hash {
            return Err(HeaderReadError::Inconsistent("header index key mismatch"));
        }
        Ok(Self {
            info: HeaderInfo {
                header,
                hash,
                height: u32::from_le_bytes(bytes[80..84].try_into().expect("fixed height")),
                chainwork: Work::from_be_bytes(bytes[84..116].try_into().expect("fixed work")),
            },
            skip: BlockHash::from_byte_array(bytes[116..148].try_into().expect("fixed hash")),
        })
    }
}

/// A validated, immutable database read version. Cloning shares its read pin,
/// not the historical header contents. The active tip is fixed for this view.
#[derive(Clone)]
pub struct DiskHeaderView {
    transaction: Arc<ReadTransaction>,
    deployments: DeploymentConfig,
    tip: HeaderInfo,
    scratch: Option<Arc<Scratch>>,
    base: Option<Arc<DiskHeaderView>>,
    len: u64,
}

impl DiskHeaderView {
    fn record(&self, hash: BlockHash) -> Result<Option<Record>, HeaderReadError> {
        let table = self.transaction.open_table(RECORDS).map_err(local)?;
        let record = table
            .get(hash.as_byte_array().as_slice())
            .map_err(local)?
            .map(|value| Record::decode(hash, value.value()))
            .transpose()?;
        match (record, &self.base) {
            (None, Some(base)) => base.record(hash),
            (record, _) => Ok(record),
        }
    }
}

// Clearing the lowest set bit supplies a deterministic strictly lower ancestor.
// The fallback parent step permits all requested heights without a RAM height map.
fn ancestor(
    tip: BlockHash,
    height: u32,
    mut lookup: impl FnMut(BlockHash) -> Result<Option<Record>, HeaderReadError>,
) -> Result<Option<HeaderInfo>, HeaderReadError> {
    let Some(mut current) = lookup(tip)? else {
        return Ok(None);
    };
    if height > current.info.height {
        return Ok(None);
    }
    while current.info.height > height {
        let skip_height = current.info.height & (current.info.height - 1);
        let (next, expected) = if skip_height >= height {
            (current.skip, skip_height)
        } else {
            (current.info.header.prev_blockhash, current.info.height - 1)
        };
        current = lookup(next)?.ok_or(HeaderReadError::Inconsistent("missing indexed ancestor"))?;
        if current.info.height != expected {
            return Err(HeaderReadError::Inconsistent("indexed ancestry height"));
        }
    }
    Ok(Some(current.info))
}

impl HeaderView for DiskHeaderView {
    fn deployments(&self) -> &DeploymentConfig {
        &self.deployments
    }
    fn active_tip(&self) -> HeaderInfo {
        self.tip
    }
    fn header(&self, hash: &BlockHash) -> Result<Option<HeaderInfo>, HeaderReadError> {
        Ok(self.record(*hash)?.map(|record| record.info))
    }
    fn active_header(&self, height: u32) -> Result<Option<HeaderInfo>, HeaderReadError> {
        self.ancestor(self.tip.hash, height)
    }
    fn ancestor(&self, tip: BlockHash, height: u32) -> Result<Option<HeaderInfo>, HeaderReadError> {
        ancestor(tip, height, |hash| self.record(hash))
    }
}

struct PendingView<'a> {
    base: &'a DiskHeaderView,
    records: &'a HashMap<BlockHash, Record>,
    tip: HeaderInfo,
}
impl PendingView<'_> {
    fn record(&self, hash: BlockHash) -> Result<Option<Record>, HeaderReadError> {
        if let Some(record) = self.records.get(&hash) {
            return Ok(Some(*record));
        }
        self.base.record(hash)
    }
}
impl HeaderView for PendingView<'_> {
    fn deployments(&self) -> &DeploymentConfig {
        self.base.deployments()
    }
    fn active_tip(&self) -> HeaderInfo {
        self.tip
    }
    fn header(&self, hash: &BlockHash) -> Result<Option<HeaderInfo>, HeaderReadError> {
        Ok(self.record(*hash)?.map(|record| record.info))
    }
    fn active_header(&self, height: u32) -> Result<Option<HeaderInfo>, HeaderReadError> {
        self.ancestor(self.tip.hash, height)
    }
    fn ancestor(&self, tip: BlockHash, height: u32) -> Result<Option<HeaderInfo>, HeaderReadError> {
        ancestor(tip, height, |hash| self.record(hash))
    }
}

/// Bounded staging writer for a fresh, derived header index. Readers see only
/// complete committed batches and retain their old chain after a later reorg.
/// This does not own or replace the authoritative raw-header recovery journal.
pub struct DiskHeaderIndex {
    db: Database,
    deployments: DeploymentConfig,
    tip: HeaderInfo,
    context: Option<CandidateContext>,
    len: u64,
    poisoned: bool,
    scratch: Option<Arc<Scratch>>,
    base: Option<Arc<DiskHeaderView>>,
}

/// An unpublished index transaction. Dropping aborts all inserted records;
/// commit updates the writer's validated context only after durable success.
pub struct StagedDiskHeaders<'a> {
    index: &'a mut DiskHeaderIndex,
    transaction: WriteTransaction,
    context: Option<CandidateContext>,
    tip: HeaderInfo,
    count: u64,
}
impl StagedDiskHeaders<'_> {
    /// Selected tip if this complete transaction commits.
    pub const fn active_tip(&self) -> HeaderInfo {
        self.tip
    }
    /// Publishes the complete validated batch. An ambiguous commit error poisons
    /// the derived index, requiring rebuild from authoritative raw history.
    pub fn commit(self) -> Result<(), HeaderIndexError> {
        let Self {
            index,
            transaction,
            context,
            tip,
            count,
        } = self;
        if let Err(error) = transaction.commit() {
            index.poisoned = true;
            return Err(local(error).into());
        }
        index.context = context;
        index.tip = tip;
        index.len = count;
        Ok(())
    }
}

impl DiskHeaderIndex {
    /// Creates a new file exclusively; never overwrites an existing index.
    /// The page cache is 8 MiB, separate from transaction and OS page buffers.
    pub fn create(
        path: impl AsRef<Path>,
        deployments: DeploymentConfig,
    ) -> Result<Self, HeaderIndexError> {
        drop(
            std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(path.as_ref())
                .map_err(local)?,
        );
        let db = Database::builder()
            .set_cache_size(CACHE_BYTES)
            .create(path)
            .map_err(local)?;
        let tip = HeaderDag::with_deployments(deployments.clone()).active_tip();
        let transaction = db.begin_write().map_err(local)?;
        transaction
            .open_table(RECORDS)
            .map_err(local)?
            .insert(
                tip.hash.as_byte_array().as_slice(),
                Record {
                    info: tip,
                    skip: tip.hash,
                }
                .encode()
                .as_slice(),
            )
            .map_err(local)?;
        transaction
            .open_table(CHILDREN)
            .map_err(local)?
            .insert(tip.hash.as_byte_array().as_slice(), 0)
            .map_err(local)?;
        transaction
            .open_table(LEAVES)
            .map_err(local)?
            .insert(tip.hash.as_byte_array().as_slice(), ())
            .map_err(local)?;
        transaction.commit().map_err(local)?;
        Ok(Self {
            db,
            deployments,
            tip,
            context: None,
            len: 0,
            poisoned: false,
            scratch: None,
            base: None,
        })
    }

    /// Creates an owned temporary index on the selected filesystem. Every
    /// published read view retains its cleanup lease until the last reader exits.
    pub fn create_scratch(
        parent: impl AsRef<Path>,
        deployments: DeploymentConfig,
    ) -> Result<Self, HeaderIndexError> {
        let scratch = Arc::new(Scratch::create(parent.as_ref()).map_err(local)?);
        let mut index = Self::create(scratch.path().join("index.redb"), deployments)?;
        index.scratch = Some(scratch);
        Ok(index)
    }

    /// Takes an immutable validated read version without copying history.
    pub fn snapshot(&self) -> Result<DiskHeaderView, HeaderIndexError> {
        if self.poisoned {
            return Err(local("derived header index requires rebuild").into());
        }
        Ok(DiskHeaderView {
            transaction: Arc::new(self.db.begin_read().map_err(local)?),
            deployments: self.deployments.clone(),
            tip: self.tip,
            scratch: self.scratch.clone(),
            base: self.base.clone(),
            len: self.len,
        })
    }

    /// Creates a private scratch overlay sharing one validated read-only seed.
    /// Only new headers occupy its tables; seed history and cache are shared.
    /// Nested overlays are refused to keep lookup depth bounded at two reads.
    pub(crate) fn overlay(base: Arc<DiskHeaderView>) -> Result<Self, HeaderIndexError> {
        if base.base.is_some() {
            return Err(local("nested header overlays are not supported").into());
        }
        let parent = base
            .scratch
            .as_ref()
            .and_then(|directory| directory.path().parent())
            .ok_or_else(|| local("header overlay requires an owned scratch seed"))?;
        let mut index = Self::create_scratch(parent, base.deployments.clone())?;
        index.tip = base.tip;
        index.len = base.len;
        index.base = Some(base);
        Ok(index)
    }

    /// Number of validated non-genesis entries, including side branches.
    pub const fn len(&self) -> u64 {
        self.len
    }
    /// Whether only genesis has been validated.
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Validates at most 2,000 headers and commits them atomically. All history
    /// lookups are disk-backed; only the batch and one consensus window are held.
    /// Failure preserves both previous reader views and this writer's context.
    pub fn append(
        &mut self,
        batch: &[Header],
        now: u32,
        work: &mut HeaderWorkBudget,
    ) -> Result<(), HeaderIndexError> {
        self.stage(batch, now, work)?.commit()
    }

    /// Validates and prepares at most 2,000 records without publishing them.
    /// This guard lets a node persist raw recovery history before committing
    /// the derived index and publishing its next immutable read view.
    pub fn stage<'a>(
        &'a mut self,
        batch: &[Header],
        now: u32,
        work: &mut HeaderWorkBudget,
    ) -> Result<StagedDiskHeaders<'a>, HeaderIndexError> {
        if batch.len() > MAX_BATCH {
            return Err(HeaderIndexError::BatchLimit);
        }
        work.consume(
            2 * self.context.as_ref().map_or(0, CandidateContext::entries) as u64
                + 2 * batch.len() as u64,
        )?;
        let base = self.snapshot()?;
        let mut records = HashMap::with_capacity(batch.len());
        let mut context = self.context.clone();
        let mut tip = self.tip;
        for header in batch {
            // Covers skip-index reads (at most 32 levels of at most 32 steps),
            // hashing, and bounded batch metadata before the first lookup.
            work.consume(if self.base.is_some() { 4_096 } else { 2_048 })?;
            let pending = PendingView {
                base: &base,
                records: &records,
                tip,
            };
            let hash = header.block_hash();
            if pending.header(&hash)?.is_some() {
                return Err(HeaderError::Duplicate(hash).into());
            }
            if context
                .as_ref()
                .is_none_or(|context| context.tip().hash != header.prev_blockhash)
            {
                work.consume(
                    (if self.base.is_some() { 4 } else { 2 })
                        * crate::headers::core_params(self.deployments.network())
                            .difficulty_adjustment_interval()
                        + 128,
                )?;
                context = Some(CandidateContext::new(&pending, header.prev_blockhash)?);
            }
            let info = context
                .as_mut()
                .expect("context initialized")
                .accept(*header, now, work)?;
            let skip = pending
                .ancestor(header.prev_blockhash, info.height & (info.height - 1))?
                .ok_or(HeaderReadError::Inconsistent("missing new skip ancestor"))?
                .hash;
            if info.chainwork > tip.chainwork {
                tip = info;
            }
            records.insert(hash, Record { info, skip });
        }
        let count = self
            .len
            .checked_add(batch.len() as u64)
            .ok_or(HeaderReadError::Inconsistent("header count overflow"))?;
        let transaction = self.db.begin_write().map_err(local)?;
        {
            let mut table = transaction.open_table(RECORDS).map_err(local)?;
            let mut children = transaction.open_table(CHILDREN).map_err(local)?;
            let mut leaves = transaction.open_table(LEAVES).map_err(local)?;
            for (hash, record) in &records {
                table
                    .insert(hash.as_byte_array().as_slice(), record.encode().as_slice())
                    .map_err(local)?;
                children
                    .insert(hash.as_byte_array().as_slice(), 0)
                    .map_err(local)?;
                leaves
                    .insert(hash.as_byte_array().as_slice(), ())
                    .map_err(local)?;
            }
            for record in records.values() {
                let parent = record.info.header.prev_blockhash;
                let count = children
                    .get(parent.as_byte_array().as_slice())
                    .map_err(local)?
                    .map(|value| value.value());
                let count = match count {
                    Some(count) => count,
                    None if self.base.is_some() && base.header(&parent)?.is_some() => 0,
                    None => {
                        return Err(
                            HeaderReadError::Inconsistent("missing parent child count").into()
                        );
                    }
                };
                let count = count
                    .checked_add(1)
                    .ok_or(HeaderReadError::Inconsistent("child count overflow"))?;
                children
                    .insert(parent.as_byte_array().as_slice(), count)
                    .map_err(local)?;
                leaves
                    .remove(parent.as_byte_array().as_slice())
                    .map_err(local)?;
            }
        }
        Ok(StagedDiskHeaders {
            index: self,
            transaction,
            context,
            tip,
            count,
        })
    }
}

#[cfg(test)]
mod tests;
