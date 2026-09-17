//! Bounded consensus context for a single contiguous disk-backed candidate.

use std::collections::VecDeque;

use super::{
    BlockHash, Header, HeaderDag, HeaderError, HeaderInfo, HeaderView, HeaderWorkBudget,
    checkpoint_hash, checkpoint_heights,
};

#[derive(Clone)]
pub(crate) struct CandidateContext {
    // A validation lookup window, never a publishable DAG. Only read-only
    // consensus validators use it; its genesis-only active index is not a
    // representation of the candidate chain. The separate tip owns progress.
    dag: HeaderDag,
    recent: VecDeque<BlockHash>,
    window: usize,
    tip: HeaderInfo,
}

impl CandidateContext {
    pub(crate) fn new(source: &dyn HeaderView, anchor: BlockHash) -> Result<Self, HeaderError> {
        let tip = source
            .header(&anchor)?
            .ok_or(HeaderError::UnknownParent(anchor))?;
        let window =
            usize::try_from(super::core_params(source.network()).difficulty_adjustment_interval())
                .expect("network interval fits usize")
                .max(11);
        let mut dag = HeaderDag::with_deployments(source.deployments().clone());
        let mut recent = VecDeque::with_capacity(window + 1);
        let mut current = tip;
        for _ in 0..window {
            dag.headers.insert(current.hash, current);
            recent.push_front(current.hash);
            if current.height == 0 {
                break;
            }
            current = source
                .header(&current.header.prev_blockhash)?
                .ok_or(HeaderError::UnknownParent(current.header.prev_blockhash))?;
        }
        // Preserve the source's checkpoint floor even when the fork anchor is
        // older than it. These few pinned entries are not a second chain copy.
        for height in checkpoint_heights(source.network()) {
            if let Some(hash) = checkpoint_hash(source.network(), *height) {
                if let Some(info) = source.header(&hash)? {
                    dag.headers.insert(info.hash, info);
                }
            }
        }
        Ok(Self {
            dag,
            recent,
            window,
            tip,
        })
    }

    pub(crate) const fn tip(&self) -> HeaderInfo {
        self.tip
    }
    pub(crate) fn entries(&self) -> usize {
        self.dag.headers.len()
    }
    pub(crate) fn consensus_id(&self) -> Vec<u8> {
        self.dag.deployments.consensus_id()
    }

    pub(crate) fn accept(
        &mut self,
        header: Header,
        now: u32,
        work: &mut HeaderWorkBudget,
    ) -> Result<HeaderInfo, HeaderError> {
        // Includes bounded map maintenance as well as the difficulty/MTP walks.
        work.consume(4 * self.window as u64 + 128)?;
        if header.prev_blockhash != self.tip.hash {
            return Err(HeaderError::UnknownParent(header.prev_blockhash));
        }
        let info = self.dag.validate_contextual(header, now)?;
        self.dag.headers.insert(info.hash, info);
        self.recent.push_back(info.hash);
        if self.recent.len() > self.window {
            let old = self.recent.pop_front().expect("nonempty context");
            let entry = self.dag.headers[&old];
            if entry.height != 0 && checkpoint_hash(self.dag.network(), entry.height) != Some(old) {
                self.dag.headers.remove(&old);
            }
        }
        self.tip = info;
        Ok(info)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bitcoin::{Network, block::Version, pow::CompactTarget};

    #[test]
    fn bounded_context_preserves_testnet_retarget_and_mtp_ancestors() {
        // Synthetic ancestry isolates context selection from expensive testnet
        // mining. Full consensus acceptance is tested with mined regtest chains.
        for network in [Network::Testnet, Network::Testnet4] {
            let mut source = HeaderDag::new(network);
            let mut parent = source.active_tip();
            for height in 1..=6048 {
                let mut header = parent.header;
                header.version = Version::from_consensus(4);
                header.prev_blockhash = parent.hash;
                header.time += 600;
                header.bits = if height % 2016 == 0 {
                    CompactTarget::from_consensus(0x1c0f_fff0)
                } else {
                    source.params.max_attainable_target.to_compact_lossy()
                };
                let info = HeaderInfo {
                    header,
                    hash: header.block_hash(),
                    height,
                    chainwork: parent.chainwork + header.target().to_work(),
                };
                source.headers.insert(info.hash, info);
                parent = info;
                if [2015, 2016, 2017, 4031, 4032, 6047, 6048].contains(&height) {
                    let context = CandidateContext::new(&source, info.hash).unwrap();
                    assert!(context.entries() <= 2017);
                    assert_eq!(
                        context.dag.median_time_past(info.hash),
                        source.median_time_past(info.hash)
                    );
                    for spacing in [600, 1201] {
                        let mut child = header;
                        child.prev_blockhash = info.hash;
                        child.time += spacing;
                        assert_eq!(
                            context.dag.expected_next_bits(&child).unwrap(),
                            source.expected_next_bits(&child).unwrap()
                        );
                    }
                }
            }
        }
    }
}
