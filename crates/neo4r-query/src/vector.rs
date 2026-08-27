use hnsw_rs::prelude::{DistCosine, DistL2, Hnsw};
use neo4r_core::{Node, NodeId, Value};
use std::cmp::Ordering;
use std::collections::HashMap;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum VectorMetric {
    Cosine,
    L2,
}

#[derive(Clone, Debug, PartialEq)]
pub struct VectorSearch {
    pub query: Vec<f32>,
    pub k: usize,
    pub metric: VectorMetric,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct VectorHit {
    pub node_id: NodeId,
    pub score: f32,
}

pub trait VectorIndex {
    fn upsert(&mut self, node_id: NodeId, vector: Vec<f32>);
    fn delete(&mut self, node_id: NodeId);
    fn search(&self, search: &VectorSearch) -> Vec<VectorHit>;
}

pub trait VectorIndexProvider: Send + Sync {
    fn search(
        &self,
        label: Option<&str>,
        property_key: &str,
        search: &VectorSearch,
    ) -> Option<Vec<VectorHit>>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HnswVectorIndexConfig {
    pub metric: VectorMetric,
    pub max_nb_connection: usize,
    pub max_layer: usize,
    pub ef_construction: usize,
    pub ef_search: usize,
    pub expected_elements: usize,
}

impl Default for HnswVectorIndexConfig {
    fn default() -> Self {
        Self {
            metric: VectorMetric::Cosine,
            max_nb_connection: 16,
            max_layer: 16,
            ef_construction: 200,
            ef_search: 64,
            expected_elements: 1024,
        }
    }
}

pub struct HnswVectorIndex {
    config: HnswVectorIndexConfig,
    entries: HashMap<NodeId, Vec<f32>>,
    index: HnswIndex,
}

enum HnswIndex {
    Cosine(Hnsw<'static, f32, DistCosine>),
    L2(Hnsw<'static, f32, DistL2>),
}

impl HnswVectorIndex {
    pub fn new(config: HnswVectorIndexConfig) -> Self {
        let index = new_hnsw_index(config);
        Self {
            config,
            entries: HashMap::new(),
            index,
        }
    }

    pub fn from_nodes<'a>(
        nodes: impl IntoIterator<Item = &'a Node>,
        property_key: &str,
        config: HnswVectorIndexConfig,
    ) -> Self {
        let mut index = Self::new(config);
        for node in nodes {
            if let Some(Value::Vector(vector)) = node.properties.get(property_key) {
                index.upsert(node.id, vector.clone());
            }
        }
        index
    }

    pub fn metric(&self) -> VectorMetric {
        self.config.metric
    }

    pub fn from_entries(
        entries: impl IntoIterator<Item = (NodeId, Vec<f32>)>,
        config: HnswVectorIndexConfig,
    ) -> Self {
        let mut index = Self::new(config);
        for (node_id, vector) in entries {
            index.upsert(node_id, vector);
        }
        index
    }

    pub fn entries(&self) -> impl Iterator<Item = (NodeId, &Vec<f32>)> {
        self.entries
            .iter()
            .map(|(node_id, vector)| (*node_id, vector))
    }

    fn rebuild(&mut self) {
        self.index = new_hnsw_index(self.config);
        for (node_id, vector) in &self.entries {
            insert_hnsw(&self.index, *node_id, vector);
        }
    }
}

impl VectorIndex for HnswVectorIndex {
    fn upsert(&mut self, node_id: NodeId, vector: Vec<f32>) {
        let replaces_existing = self.entries.insert(node_id, vector.clone()).is_some();
        if replaces_existing {
            self.rebuild();
        } else {
            insert_hnsw(&self.index, node_id, &vector);
        }
    }

    fn delete(&mut self, node_id: NodeId) {
        if self.entries.remove(&node_id).is_some() {
            self.rebuild();
        }
    }

    fn search(&self, search: &VectorSearch) -> Vec<VectorHit> {
        if search.metric != self.config.metric {
            return search_entries_exact(
                self.entries
                    .iter()
                    .map(|(node_id, vector)| (*node_id, vector)),
                search,
            );
        }
        let ef = self.config.ef_search.max(search.k);
        let candidate_count = self.entries.len().min(ef.max(search.k));
        if candidate_count == 0 {
            return Vec::new();
        }
        if candidate_count == self.entries.len() {
            return search_entries_exact(
                self.entries
                    .iter()
                    .map(|(node_id, vector)| (*node_id, vector)),
                search,
            );
        }
        let hits = match &self.index {
            HnswIndex::Cosine(index) => index.search(&search.query, candidate_count, ef),
            HnswIndex::L2(index) => index.search(&search.query, candidate_count, ef),
        };
        let candidates = hits
            .iter()
            .filter_map(|hit| {
                let node_id = hit.d_id as NodeId;
                self.entries.get(&node_id).map(|vector| (node_id, vector))
            })
            .collect::<Vec<_>>();
        search_entries_exact(candidates, search)
    }
}

#[derive(Clone, Debug, Default)]
pub struct BruteForceVectorIndex {
    entries: Vec<(NodeId, Vec<f32>)>,
}

impl BruteForceVectorIndex {
    pub fn from_nodes<'a>(nodes: impl IntoIterator<Item = &'a Node>, property_key: &str) -> Self {
        let entries = nodes
            .into_iter()
            .filter_map(|node| match node.properties.get(property_key) {
                Some(Value::Vector(vector)) => Some((node.id, vector.clone())),
                _ => None,
            })
            .collect();
        Self { entries }
    }

    pub fn search_cosine(&self, query: &[f32], k: usize) -> Vec<VectorHit> {
        self.search(&VectorSearch {
            query: query.to_vec(),
            k,
            metric: VectorMetric::Cosine,
        })
    }
}

impl VectorIndex for BruteForceVectorIndex {
    fn upsert(&mut self, node_id: NodeId, vector: Vec<f32>) {
        self.delete(node_id);
        self.entries.push((node_id, vector));
    }

    fn delete(&mut self, node_id: NodeId) {
        self.entries
            .retain(|(entry_node_id, _)| *entry_node_id != node_id);
    }

    fn search(&self, search: &VectorSearch) -> Vec<VectorHit> {
        search_entries_exact(
            self.entries
                .iter()
                .map(|(node_id, vector)| (*node_id, vector)),
            search,
        )
    }
}

fn new_hnsw_index(config: HnswVectorIndexConfig) -> HnswIndex {
    match config.metric {
        VectorMetric::Cosine => HnswIndex::Cosine(Hnsw::<f32, DistCosine>::new(
            config.max_nb_connection,
            config.expected_elements,
            config.max_layer,
            config.ef_construction,
            DistCosine {},
        )),
        VectorMetric::L2 => HnswIndex::L2(Hnsw::<f32, DistL2>::new(
            config.max_nb_connection,
            config.expected_elements,
            config.max_layer,
            config.ef_construction,
            DistL2 {},
        )),
    }
}

fn insert_hnsw(index: &HnswIndex, node_id: NodeId, vector: &[f32]) {
    match index {
        HnswIndex::Cosine(index) => index.insert((vector, node_id as usize)),
        HnswIndex::L2(index) => index.insert((vector, node_id as usize)),
    }
}

fn search_entries_exact<'a>(
    entries: impl IntoIterator<Item = (NodeId, &'a Vec<f32>)>,
    search: &VectorSearch,
) -> Vec<VectorHit> {
    let mut hits = match search.metric {
        VectorMetric::Cosine => entries
            .into_iter()
            .filter_map(|(node_id, vector)| {
                cosine_similarity(vector, &search.query).map(|score| VectorHit { node_id, score })
            })
            .collect::<Vec<_>>(),
        VectorMetric::L2 => entries
            .into_iter()
            .filter_map(|(node_id, vector)| {
                l2_distance(vector, &search.query).map(|distance| VectorHit {
                    node_id,
                    score: -distance,
                })
            })
            .collect::<Vec<_>>(),
    };
    sort_hits_descending(&mut hits);
    hits.truncate(search.k);
    hits
}

fn sort_hits_descending(hits: &mut [VectorHit]) {
    hits.sort_by(|left, right| {
        right
            .score
            .partial_cmp(&left.score)
            .unwrap_or(Ordering::Equal)
            .then_with(|| left.node_id.cmp(&right.node_id))
    });
}

pub fn cosine_similarity(left: &[f32], right: &[f32]) -> Option<f32> {
    if left.len() != right.len() || left.is_empty() {
        return None;
    }

    let mut dot = 0.0_f32;
    let mut left_norm = 0.0_f32;
    let mut right_norm = 0.0_f32;
    for (left, right) in left.iter().zip(right.iter()) {
        dot += left * right;
        left_norm += left * left;
        right_norm += right * right;
    }
    if left_norm == 0.0 || right_norm == 0.0 {
        return None;
    }
    Some(dot / (left_norm.sqrt() * right_norm.sqrt()))
}

pub fn l2_distance(left: &[f32], right: &[f32]) -> Option<f32> {
    if left.len() != right.len() || left.is_empty() {
        return None;
    }
    Some(
        left.iter()
            .zip(right.iter())
            .map(|(left, right)| {
                let delta = left - right;
                delta * delta
            })
            .sum::<f32>()
            .sqrt(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use neo4r_core::Properties;

    fn node(id: NodeId, vector: Vec<f32>) -> Node {
        let mut properties = Properties::new();
        properties.insert("embedding".to_string(), Value::Vector(vector));
        Node {
            id,
            labels: Vec::new(),
            properties,
        }
    }

    #[test]
    fn computes_cosine_similarity() {
        assert_eq!(cosine_similarity(&[1.0, 0.0], &[1.0, 0.0]), Some(1.0));
        assert_eq!(cosine_similarity(&[1.0, 0.0], &[0.0, 1.0]), Some(0.0));
        assert_eq!(cosine_similarity(&[1.0], &[1.0, 0.0]), None);
    }

    #[test]
    fn computes_l2_distance() {
        assert_eq!(l2_distance(&[1.0, 2.0], &[1.0, 4.0]), Some(2.0));
        assert_eq!(l2_distance(&[1.0], &[1.0, 0.0]), None);
    }

    #[test]
    fn brute_force_index_returns_nearest_vectors() {
        let nodes = vec![
            node(3, vec![0.0, 1.0]),
            node(1, vec![1.0, 0.0]),
            node(2, vec![0.9, 0.1]),
        ];
        let index = BruteForceVectorIndex::from_nodes(&nodes, "embedding");

        let hits = index.search_cosine(&[1.0, 0.0], 2);

        assert_eq!(
            hits.iter().map(|hit| hit.node_id).collect::<Vec<_>>(),
            vec![1, 2]
        );
    }

    #[test]
    fn vector_index_upsert_and_delete_updates_results() {
        let mut index = BruteForceVectorIndex::default();
        index.upsert(1, vec![0.0, 1.0]);
        index.upsert(2, vec![1.0, 0.0]);
        index.upsert(1, vec![0.9, 0.1]);

        let search = VectorSearch {
            query: vec![1.0, 0.0],
            k: 2,
            metric: VectorMetric::Cosine,
        };
        assert_eq!(
            index
                .search(&search)
                .iter()
                .map(|hit| hit.node_id)
                .collect::<Vec<_>>(),
            vec![2, 1]
        );

        index.delete(2);
        assert_eq!(
            index
                .search(&search)
                .iter()
                .map(|hit| hit.node_id)
                .collect::<Vec<_>>(),
            vec![1]
        );
    }

    #[test]
    fn vector_index_supports_l2_metric() {
        let nodes = vec![node(1, vec![10.0, 10.0]), node(2, vec![1.0, 1.0])];
        let index = BruteForceVectorIndex::from_nodes(&nodes, "embedding");

        let hits = index.search(&VectorSearch {
            query: vec![0.0, 0.0],
            k: 1,
            metric: VectorMetric::L2,
        });

        assert_eq!(hits[0].node_id, 2);
    }

    #[test]
    fn hnsw_index_returns_nearest_vectors() {
        let nodes = vec![
            node(3, vec![0.0, 1.0]),
            node(1, vec![1.0, 0.0]),
            node(2, vec![0.9, 0.1]),
        ];
        let index = HnswVectorIndex::from_nodes(
            &nodes,
            "embedding",
            HnswVectorIndexConfig {
                expected_elements: nodes.len(),
                ..HnswVectorIndexConfig::default()
            },
        );

        let hits = index.search(&VectorSearch {
            query: vec![1.0, 0.0],
            k: 2,
            metric: VectorMetric::Cosine,
        });

        assert_eq!(
            hits.iter().map(|hit| hit.node_id).collect::<Vec<_>>(),
            vec![1, 2]
        );
    }

    #[test]
    fn hnsw_index_upsert_and_delete_rebuilds_results() {
        let mut index = HnswVectorIndex::new(HnswVectorIndexConfig {
            expected_elements: 4,
            ..HnswVectorIndexConfig::default()
        });
        index.upsert(1, vec![0.0, 1.0]);
        index.upsert(2, vec![1.0, 0.0]);
        index.upsert(1, vec![0.9, 0.1]);

        let search = VectorSearch {
            query: vec![1.0, 0.0],
            k: 2,
            metric: VectorMetric::Cosine,
        };
        assert_eq!(
            index
                .search(&search)
                .iter()
                .map(|hit| hit.node_id)
                .collect::<Vec<_>>(),
            vec![2, 1]
        );

        index.delete(2);
        assert_eq!(
            index
                .search(&search)
                .iter()
                .map(|hit| hit.node_id)
                .collect::<Vec<_>>(),
            vec![1]
        );
    }

    #[test]
    fn hnsw_index_supports_l2_metric() {
        let nodes = vec![node(1, vec![10.0, 10.0]), node(2, vec![1.0, 1.0])];
        let index = HnswVectorIndex::from_nodes(
            &nodes,
            "embedding",
            HnswVectorIndexConfig {
                metric: VectorMetric::L2,
                expected_elements: nodes.len(),
                ..HnswVectorIndexConfig::default()
            },
        );

        let hits = index.search(&VectorSearch {
            query: vec![0.0, 0.0],
            k: 1,
            metric: VectorMetric::L2,
        });

        assert_eq!(hits[0].node_id, 2);
    }
}
