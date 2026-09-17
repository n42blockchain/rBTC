//! Bounded leaf eviction using persistent child counts rather than a RAM graph.
use super::{
    BlockHash, CHILDREN, DiskHeaderIndex, HeaderIndexError, HeaderInfo, HeaderReadError,
    HeaderWorkBudget, LEAVES, RECORDS, Record, StagedDiskHeaders, local,
};
use bitcoin::hashes::Hash;
use redb::ReadableTable;

/// An unpublished disk-index eviction. Persist the same raw-record deletion
/// before committing this guard; dropping it aborts all derived-index changes.
pub struct StagedDiskHeaderEviction<'a> {
    stage: StagedDiskHeaders<'a>,
    evicted: Vec<HeaderInfo>,
}
impl StagedDiskHeaderEviction<'_> {
    /// Exact leaf-first records selected for deletion.
    pub fn evicted(&self) -> &[HeaderInfo] {
        &self.evicted
    }
    /// Publishes all selected removals after authoritative persistence succeeds.
    pub fn commit(self) -> Result<(), HeaderIndexError> {
        self.stage.commit()
    }
}
impl DiskHeaderIndex {
    /// Stages at most 2,000 losing leaf removals while preserving selected and
    /// explicitly pinned ancestry. No complete child map or fork list is built.
    pub fn stage_eviction(
        &mut self,
        side_target: usize,
        max_removals: usize,
        pinned: &[BlockHash],
        work: &mut HeaderWorkBudget,
    ) -> Result<StagedDiskHeaderEviction<'_>, HeaderIndexError> {
        if self.base.is_some() {
            return Err(local("shared header overlays do not support eviction").into());
        }
        if self.poisoned {
            return Err(local("derived header index requires rebuild").into());
        }
        let side = self.len.saturating_sub(u64::from(self.tip.height));
        let needed = usize::try_from(side.saturating_sub(side_target as u64))
            .unwrap_or(usize::MAX)
            .min(max_removals)
            .min(2_000);
        work.consume(needed as u64 * (128 + pinned.len() as u64))?;
        let transaction = self.db.begin_write().map_err(local)?;
        let mut evicted = Vec::with_capacity(needed);
        {
            let mut headers = transaction.open_table(RECORDS).map_err(local)?;
            let mut children = transaction.open_table(CHILDREN).map_err(local)?;
            let mut leaves = transaction.open_table(LEAVES).map_err(local)?;
            for _ in 0..needed {
                let mut chosen = None;
                for row in leaves.iter().map_err(local)? {
                    let (key, _) = row.map_err(local)?;
                    let hash = BlockHash::from_slice(key.value()).map_err(local)?;
                    if hash != self.tip.hash && !pinned.contains(&hash) {
                        chosen = Some(hash);
                        break;
                    }
                }
                let Some(hash) = chosen else {
                    break;
                };
                let key = hash.as_byte_array().as_slice();
                let info = Record::decode(
                    hash,
                    headers
                        .get(key)
                        .map_err(local)?
                        .ok_or(HeaderReadError::Inconsistent("missing leaf record"))?
                        .value(),
                )?
                .info;
                if info.height == 0
                    || children.get(key).map_err(local)?.map(|value| value.value()) != Some(0)
                {
                    return Err(HeaderReadError::Inconsistent(
                        "eviction requires a non-genesis leaf",
                    )
                    .into());
                }
                let parent = info.header.prev_blockhash;
                let parent_key = parent.as_byte_array().as_slice();
                let count = children
                    .get(parent_key)
                    .map_err(local)?
                    .ok_or(HeaderReadError::Inconsistent("missing leaf parent count"))?
                    .value();
                let count = count
                    .checked_sub(1)
                    .ok_or(HeaderReadError::Inconsistent("leaf parent count underflow"))?;
                children.insert(parent_key, count).map_err(local)?;
                if count == 0 {
                    leaves.insert(parent_key, ()).map_err(local)?;
                }
                headers.remove(key).map_err(local)?;
                children.remove(key).map_err(local)?;
                leaves.remove(key).map_err(local)?;
                evicted.push(info);
            }
        }
        let count = self.len - evicted.len() as u64;
        let tip = self.tip;
        Ok(StagedDiskHeaderEviction {
            stage: StagedDiskHeaders {
                index: self,
                transaction,
                context: None,
                tip,
                count,
            },
            evicted,
        })
    }
}
