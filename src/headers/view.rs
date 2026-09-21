//! Fallible, immutable header queries shared by memory and persistent views.

use super::{BlockHash, CandidateContext, HeaderDag, HeaderError, HeaderInfo, Network};
use crate::deployments::DeploymentConfig;
use thiserror::Error;

/// A local failure reading a previously validated header view.
/// This must never be classified as peer consensus invalidity.
#[derive(Clone, Debug, Eq, PartialEq, Error)]
pub enum HeaderReadError {
    /// Storage or another local provider could not complete the read.
    #[error("header view unavailable: {0}")]
    Unavailable(String),
    /// An existing header lacks required validated ancestry.
    #[error("inconsistent header view: {0}")]
    Inconsistent(&'static str),
}

/// An immutable selected-chain view whose history need not reside in RAM.
///
/// Implementations must pin one coherent view for their lifetime. A missing
/// lookup is distinct from an I/O failure: callers must propagate failures
/// before publishing execution, deployment or trust-policy results.
pub trait HeaderView: Send + Sync {
    /// Consensus configuration used to validate this view.
    fn deployments(&self) -> &DeploymentConfig;
    /// Consensus network of this view.
    fn network(&self) -> Network {
        self.deployments().network()
    }
    /// Validated selected tip, cached when the view is acquired.
    fn active_tip(&self) -> HeaderInfo;
    /// Looks up a validated header, including retained non-active branches.
    fn header(&self, hash: &BlockHash) -> Result<Option<HeaderInfo>, HeaderReadError>;
    /// Looks up a height on this view's selected chain.
    fn active_header(&self, height: u32) -> Result<Option<HeaderInfo>, HeaderReadError>;

    /// Resolves membership without treating failed storage reads as absence.
    fn active_height(&self, hash: BlockHash) -> Result<Option<u32>, HeaderReadError> {
        let Some(info) = self.header(&hash)? else {
            return Ok(None);
        };
        Ok(self
            .active_header(info.height)?
            .filter(|active| active.hash == hash)
            .map(|_| info.height))
    }

    /// Resolves ancestry on a retained branch; providers may use a skip index.
    fn ancestor(&self, tip: BlockHash, height: u32) -> Result<Option<HeaderInfo>, HeaderReadError> {
        let Some(mut current) = self.header(&tip)? else {
            return Ok(None);
        };
        if height > current.height {
            return Ok(None);
        }
        while current.height > height {
            let parent = self
                .header(&current.header.prev_blockhash)?
                .ok_or(HeaderReadError::Inconsistent("missing branch ancestor"))?;
            if parent.height.checked_add(1) != Some(current.height) {
                return Err(HeaderReadError::Inconsistent("branch ancestry height"));
            }
            current = parent;
        }
        Ok(Some(current))
    }

    /// Builds a locator on a retained branch, without changing chain selection.
    fn branch_locator(&self, tip: BlockHash) -> Result<Option<Vec<BlockHash>>, HeaderReadError> {
        let Some(info) = self.header(&tip)? else {
            return Ok(None);
        };
        let mut locator = Vec::with_capacity(43);
        let mut height = info.height;
        let mut step = 1_u32;
        loop {
            locator.push(
                self.ancestor(tip, height)?
                    .ok_or(HeaderReadError::Inconsistent(
                        "missing branch locator header",
                    ))?
                    .hash,
            );
            if height == 0 {
                return Ok(Some(locator));
            }
            height = height.saturating_sub(step);
            if locator.len() > 10 {
                step = step.saturating_mul(2);
            }
        }
    }

    /// Derives the next-work rule from a bounded window of this branch.
    fn expected_next_bits(
        &self,
        candidate: &bitcoin::block::Header,
    ) -> Result<bitcoin::pow::CompactTarget, HeaderError> {
        CandidateContext::new(self, candidate.prev_blockhash)?.expected_next_bits(candidate)
    }

    /// Computes MTP with a fixed eleven-entry stack buffer.
    fn median_time_past(&self, hash: BlockHash) -> Result<Option<u32>, HeaderReadError> {
        let Some(mut current) = self.header(&hash)? else {
            return Ok(None);
        };
        let mut times = [0; 11];
        let mut count = 0;
        loop {
            times[count] = current.header.time;
            count += 1;
            if count == times.len() || current.height == 0 {
                break;
            }
            let parent = self
                .header(&current.header.prev_blockhash)?
                .ok_or(HeaderReadError::Inconsistent("missing MTP ancestor"))?;
            if parent.height.checked_add(1) != Some(current.height) {
                return Err(HeaderReadError::Inconsistent("MTP ancestry height"));
            }
            current = parent;
        }
        times[..count].sort_unstable();
        Ok(Some(times[count / 2]))
    }

    /// Builds the standard locator without copying the selected history.
    fn block_locator(&self) -> Result<Vec<BlockHash>, HeaderReadError> {
        let mut locator = Vec::with_capacity(43);
        let mut height = self.active_tip().height;
        let mut step = 1_u32;
        loop {
            locator.push(
                self.active_header(height)?
                    .ok_or(HeaderReadError::Inconsistent(
                        "missing active locator header",
                    ))?
                    .hash,
            );
            if height == 0 {
                return Ok(locator);
            }
            height = height.saturating_sub(step);
            if locator.len() > 10 {
                step = step.saturating_mul(2);
            }
        }
    }
}

impl HeaderView for HeaderDag {
    fn deployments(&self) -> &DeploymentConfig {
        &self.deployments
    }
    fn network(&self) -> Network {
        self.network()
    }
    fn active_tip(&self) -> HeaderInfo {
        self.active_tip()
    }
    fn header(&self, hash: &BlockHash) -> Result<Option<HeaderInfo>, HeaderReadError> {
        Ok(self.get(hash))
    }
    fn ancestor(&self, tip: BlockHash, height: u32) -> Result<Option<HeaderInfo>, HeaderReadError> {
        let Some(info) = self.get(&tip) else {
            return Ok(None);
        };
        if self.active_height_of(tip).is_some() {
            return Ok((height <= info.height)
                .then(|| self.active_header_at(height))
                .flatten());
        }
        Ok(self.ancestor_at_height(info, height))
    }
    fn branch_locator(&self, tip: BlockHash) -> Result<Option<Vec<BlockHash>>, HeaderReadError> {
        Ok(self.block_locator_from(tip))
    }
    fn block_locator(&self) -> Result<Vec<BlockHash>, HeaderReadError> {
        Ok(self.block_locator())
    }
    fn active_header(&self, height: u32) -> Result<Option<HeaderInfo>, HeaderReadError> {
        Ok(self.active_header_at(height))
    }
}

impl From<HeaderReadError> for String {
    fn from(error: HeaderReadError) -> Self {
        error.to_string()
    }
}

/// A published node header view. Legacy memory serving is retained during
/// migration; a disk version pins its database snapshot without copying history.
pub enum HeaderSnapshot {
    /// Active-only legacy in-memory serving projection.
    Memory(Box<HeaderDag>),
    /// Immutable validated persistent read version.
    Disk(crate::header_index::DiskHeaderView),
}
impl HeaderSnapshot {
    fn view(&self) -> &dyn HeaderView {
        match self {
            Self::Memory(view) => view.as_ref(),
            Self::Disk(view) => view,
        }
    }
}
impl From<HeaderDag> for HeaderSnapshot {
    fn from(view: HeaderDag) -> Self {
        Self::Memory(Box::new(view))
    }
}
impl From<crate::header_index::DiskHeaderView> for HeaderSnapshot {
    fn from(view: crate::header_index::DiskHeaderView) -> Self {
        Self::Disk(view)
    }
}
impl HeaderView for HeaderSnapshot {
    fn deployments(&self) -> &DeploymentConfig {
        self.view().deployments()
    }
    fn active_tip(&self) -> HeaderInfo {
        self.view().active_tip()
    }
    fn header(&self, hash: &BlockHash) -> Result<Option<HeaderInfo>, HeaderReadError> {
        let Some(info) = self.view().header(hash)? else {
            return Ok(None);
        };
        Ok(self
            .view()
            .active_header(info.height)?
            .filter(|active| active.hash == *hash))
    }
    fn active_header(&self, height: u32) -> Result<Option<HeaderInfo>, HeaderReadError> {
        self.view().active_header(height)
    }
    fn ancestor(&self, tip: BlockHash, height: u32) -> Result<Option<HeaderInfo>, HeaderReadError> {
        if self.header(&tip)?.is_none() {
            return Ok(None);
        }
        self.view().ancestor(tip, height)
    }
    fn branch_locator(&self, tip: BlockHash) -> Result<Option<Vec<BlockHash>>, HeaderReadError> {
        if self.header(&tip)?.is_none() {
            return Ok(None);
        }
        self.view().branch_locator(tip)
    }
    fn block_locator(&self) -> Result<Vec<BlockHash>, HeaderReadError> {
        self.view().block_locator()
    }
}
