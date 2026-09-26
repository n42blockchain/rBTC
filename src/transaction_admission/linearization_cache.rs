//! Bounded, candidate-local reuse of previously computed cluster orders.

use std::{collections::VecDeque, sync::Mutex};

use bitcoin::Txid;

use crate::admission_resources::{AdmissionBudget, AdmissionDeferred, AdmissionStage};
use crate::feerate_diagram::{Cluster, DEFAULT_OPTIMIZER_WORK, LinearizationResult};

const MAX_CACHED_CLUSTERS: usize = 64;
pub(super) const CLUSTER_SCAN_WORK: u64 = 64 * 64 * 64;

#[derive(Clone)]
struct CachedCluster {
    members: Vec<Txid>,
    cluster: Cluster,
    result: LinearizationResult,
}

#[derive(Default)]
pub(super) struct LinearizationCache(Mutex<VecDeque<CachedCluster>>);

impl Clone for LinearizationCache {
    fn clone(&self) -> Self {
        // A rejected candidate must not publish ordering history to the live pool.
        Self(Mutex::new(
            self.0.lock().expect("linearization cache lock").clone(),
        ))
    }
}

impl LinearizationCache {
    pub(super) fn linearize(
        &self,
        members: &[Txid],
        cluster: &Cluster,
        budget: &AdmissionBudget,
    ) -> Result<LinearizationResult, AdmissionDeferred> {
        budget.charge(AdmissionStage::Graph, CLUSTER_SCAN_WORK)?;
        let mut cache = self.0.lock().expect("linearization cache lock");
        if let Some(existing) = cache
            .iter()
            .find(|item| item.members == members && item.cluster == *cluster && item.result.optimal)
        {
            let mut result = existing.result.clone();
            result.work_used = 0;
            result.previous_reused = true;
            return Ok(result);
        }
        let previous = cache
            .iter()
            .rev()
            .find(|item| {
                item.members
                    .iter()
                    .any(|txid| members.binary_search(txid).is_ok())
            })
            .map(|item| {
                let mut order = item
                    .result
                    .order
                    .iter()
                    .filter_map(|&index| members.binary_search(&item.members[index]).ok())
                    .collect::<Vec<_>>();
                let present = order.iter().copied().fold(0_u64, |mask, i| mask | (1 << i));
                order.extend((0..members.len()).filter(|i| present & (1 << i) == 0));
                order
            });
        budget.charge(AdmissionStage::Graph, DEFAULT_OPTIMIZER_WORK)?;
        let result = cluster.linearize_with_budget(previous.as_deref(), DEFAULT_OPTIMIZER_WORK);
        cache.retain(|item| item.members != members);
        if cache.len() == MAX_CACHED_CLUSTERS {
            cache.pop_front();
        }
        cache.push_back(CachedCluster {
            members: members.to_vec(),
            cluster: cluster.clone(),
            result: result.clone(),
        });
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::feerate_diagram::FeeFrac;
    use bitcoin::hashes::Hash;

    fn id(value: u8) -> Txid {
        Txid::from_byte_array([value; 32])
    }

    #[test]
    fn cached_proofs_require_identical_fees_and_dependencies_and_stay_candidate_local() {
        let cache = LinearizationCache::default();
        let members = [id(1), id(2)];
        let cluster = Cluster::new(
            vec![FeeFrac::new(1, 2), FeeFrac::new(10, 1)],
            vec![vec![], vec![0]],
        )
        .unwrap();
        let first = cache
            .linearize(&members, &cluster, &AdmissionBudget::default())
            .unwrap();
        assert!(first.optimal && first.work_used > 0);
        assert_eq!(
            cache
                .linearize(&members, &cluster, &AdmissionBudget::default())
                .unwrap()
                .work_used,
            0
        );
        let candidate = cache.clone();
        let changed = Cluster::new(
            vec![FeeFrac::new(20, 2), FeeFrac::new(10, 1)],
            vec![vec![], vec![0]],
        )
        .unwrap();
        let result = candidate
            .linearize(&members, &changed, &AdmissionBudget::default())
            .unwrap();
        assert!(result.previous_reused && result.work_used > 0);
        assert_eq!(cache.0.lock().unwrap()[0].cluster, cluster);
        let disconnected =
            Cluster::new(changed.fractions().to_vec(), vec![vec![], vec![]]).unwrap();
        assert!(
            candidate
                .linearize(&members, &disconnected, &AdmissionBudget::default())
                .unwrap()
                .work_used
                > 0
        );
    }

    #[test]
    fn history_evicts_old_clusters_at_a_fixed_bound() {
        let cache = LinearizationCache::default();
        let cluster = Cluster::new(vec![FeeFrac::new(1, 1)], vec![vec![]]).unwrap();
        for value in 0..100 {
            cache
                .linearize(&[id(value)], &cluster, &AdmissionBudget::default())
                .unwrap();
        }
        let entries = cache.0.lock().unwrap();
        assert_eq!(entries.len(), MAX_CACHED_CLUSTERS);
        assert_eq!(entries.front().unwrap().members, vec![id(36)]);
    }
}
