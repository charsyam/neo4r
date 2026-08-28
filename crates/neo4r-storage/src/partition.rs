use crate::{
    GraphInvariantReport, KeyValueStore, KvGraphStore, RocksKvSnapshot, RocksKvStore, StorageError,
    StorageResult,
};
use neo4r_core::{
    BoundaryNode, Command, GraphRead, GraphReadError, GraphReadResult, Node, NodeId, Relationship,
    RelationshipId, ShardId, Value,
};
use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

pub type LocalPartitionId = u64;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LocalPartitionMap {
    partition_count: LocalPartitionId,
    assignments: BTreeMap<ShardId, LocalPartitionId>,
}

impl LocalPartitionMap {
    pub fn new(partition_count: usize) -> StorageResult<Self> {
        if partition_count == 0 {
            return Err(StorageError::CorruptStore(
                "local partition map requires at least one partition".to_string(),
            ));
        }
        Ok(Self {
            partition_count: partition_count as LocalPartitionId,
            assignments: BTreeMap::new(),
        })
    }

    pub fn with_assignment(
        mut self,
        shard_id: ShardId,
        partition_id: LocalPartitionId,
    ) -> StorageResult<Self> {
        self.validate_partition_id(partition_id)?;
        self.assignments.insert(shard_id, partition_id);
        Ok(self)
    }

    pub fn local_partition_id_for_shard(&self, shard_id: ShardId) -> LocalPartitionId {
        self.assignments
            .get(&shard_id)
            .copied()
            .unwrap_or_else(|| shard_id % self.partition_count)
    }

    pub fn partition_count(&self) -> LocalPartitionId {
        self.partition_count
    }

    fn validate_partition_id(&self, partition_id: LocalPartitionId) -> StorageResult<()> {
        if partition_id >= self.partition_count {
            return Err(StorageError::CorruptStore(format!(
                "local partition {partition_id} is outside partition count {}",
                self.partition_count
            )));
        }
        Ok(())
    }
}

pub struct PartitionedGraphStore<KV> {
    partitions: Vec<KvGraphStore<KV>>,
    placement: LocalPartitionMap,
}

impl<KV: KeyValueStore> PartitionedGraphStore<KV> {
    pub fn new(partitions: Vec<KvGraphStore<KV>>) -> StorageResult<Self> {
        let placement = LocalPartitionMap::new(partitions.len())?;
        Self::with_placement(partitions, placement)
    }

    pub fn with_placement(
        partitions: Vec<KvGraphStore<KV>>,
        placement: LocalPartitionMap,
    ) -> StorageResult<Self> {
        if partitions.is_empty() {
            return Err(StorageError::CorruptStore(
                "partitioned graph store requires at least one partition".to_string(),
            ));
        }
        if partitions.len() as LocalPartitionId != placement.partition_count() {
            return Err(StorageError::CorruptStore(format!(
                "partition count {} does not match placement count {}",
                partitions.len(),
                placement.partition_count()
            )));
        }
        Ok(Self {
            partitions,
            placement,
        })
    }

    pub fn partition_count(&self) -> usize {
        self.partitions.len()
    }

    pub fn local_partition_id_for_shard(&self, shard_id: ShardId) -> LocalPartitionId {
        self.placement.local_partition_id_for_shard(shard_id)
    }

    pub fn placement(&self) -> &LocalPartitionMap {
        &self.placement
    }

    pub fn partition_for_shard(&self, shard_id: ShardId) -> StorageResult<&KvGraphStore<KV>> {
        let index = self.partition_index_for_shard(shard_id);
        self.partitions
            .get(index)
            .ok_or_else(|| StorageError::CorruptStore(format!("missing local partition {index}")))
    }

    pub fn partition_for_shard_mut(
        &mut self,
        shard_id: ShardId,
    ) -> StorageResult<&mut KvGraphStore<KV>> {
        let index = self.partition_index_for_shard(shard_id);
        self.partitions
            .get_mut(index)
            .ok_or_else(|| StorageError::CorruptStore(format!("missing local partition {index}")))
    }

    pub fn apply(&mut self, shard_id: ShardId, command: &Command) -> StorageResult<()> {
        self.partition_for_shard_mut(shard_id)?.apply(command)
    }

    pub fn verify_invariants(&self) -> StorageResult<GraphInvariantReport> {
        let mut merged = GraphInvariantReport::default();
        for partition in &self.partitions {
            let report = partition.verify_invariants()?;
            merged.missing_index_keys.extend(report.missing_index_keys);
            merged
                .unexpected_index_keys
                .extend(report.unexpected_index_keys);
        }
        Ok(merged)
    }

    pub fn repair_indexes(&mut self) -> StorageResult<GraphInvariantReport> {
        let mut merged = GraphInvariantReport::default();
        for partition in &mut self.partitions {
            let report = partition.repair_indexes()?;
            merged.missing_index_keys.extend(report.missing_index_keys);
            merged
                .unexpected_index_keys
                .extend(report.unexpected_index_keys);
        }
        Ok(merged)
    }

    pub fn partitions(&self) -> &[KvGraphStore<KV>] {
        &self.partitions
    }

    pub fn into_partitions(self) -> Vec<KvGraphStore<KV>> {
        self.partitions
    }

    pub fn relationships(&self) -> StorageResult<Vec<Relationship>> {
        let mut relationships = Vec::new();
        for partition in &self.partitions {
            relationships.extend(partition.relationships()?);
        }
        Ok(relationships)
    }

    pub fn boundary_nodes(&self) -> StorageResult<Vec<BoundaryNode>> {
        let mut nodes = Vec::new();
        for partition in &self.partitions {
            nodes.extend(partition.boundary_nodes()?);
        }
        Ok(nodes)
    }

    fn partition_index_for_shard(&self, shard_id: ShardId) -> usize {
        self.local_partition_id_for_shard(shard_id) as usize
    }
}

impl PartitionedGraphStore<RocksKvStore> {
    pub fn open_rocks(data_dir: impl AsRef<Path>, partition_count: usize) -> StorageResult<Self> {
        if partition_count == 0 {
            return Err(StorageError::CorruptStore(
                "partition count must be greater than zero".to_string(),
            ));
        }

        let partitions_dir = data_dir.as_ref().join("partitions");
        fs::create_dir_all(&partitions_dir)?;

        let mut partitions = Vec::with_capacity(partition_count);
        for partition_id in 0..partition_count {
            let rocks_path = partitions_dir
                .join(format!("{partition_id:04}"))
                .join("rocksdb");
            fs::create_dir_all(&rocks_path)?;
            partitions.push(KvGraphStore::new(RocksKvStore::open(rocks_path)?));
        }

        Self::new(partitions)
    }

    pub fn snapshot(&self) -> StorageResult<PartitionedGraphStore<RocksKvSnapshot>> {
        PartitionedGraphStore::with_placement(
            self.partitions
                .iter()
                .map(|partition| partition.snapshot().map(KvGraphStore::new))
                .collect::<StorageResult<Vec<_>>>()?,
            self.placement.clone(),
        )
    }
}

impl<KV: KeyValueStore> GraphRead for PartitionedGraphStore<KV> {
    fn node(&self, id: NodeId) -> GraphReadResult<Option<Node>> {
        for partition in &self.partitions {
            if let Some(node) = partition.node(id).map_err(graph_read_error)? {
                return Ok(Some(node));
            }
        }
        Ok(None)
    }

    fn boundary_node(&self, id: NodeId) -> GraphReadResult<Option<BoundaryNode>> {
        for partition in &self.partitions {
            if let Some(node) = partition.boundary_node(id).map_err(graph_read_error)? {
                return Ok(Some(node));
            }
        }
        Ok(None)
    }

    fn nodes(&self) -> GraphReadResult<Vec<Node>> {
        let mut nodes = Vec::new();
        for partition in &self.partitions {
            nodes.extend(partition.nodes().map_err(graph_read_error)?);
        }
        Ok(nodes)
    }

    fn node_ids(&self) -> GraphReadResult<Vec<NodeId>> {
        let mut ids = Vec::new();
        for partition in &self.partitions {
            ids.extend(partition.node_ids().map_err(graph_read_error)?);
        }
        ids.sort_unstable();
        ids.dedup();
        Ok(ids)
    }

    fn relationship(&self, id: RelationshipId) -> GraphReadResult<Option<Relationship>> {
        for partition in &self.partitions {
            if let Some(relationship) = partition.relationship(id).map_err(graph_read_error)? {
                return Ok(Some(relationship));
            }
        }
        Ok(None)
    }

    fn node_ids_by_label(&self, label: &str) -> GraphReadResult<Vec<NodeId>> {
        let mut ids = Vec::new();
        for partition in &self.partitions {
            ids.extend(
                partition
                    .node_ids_by_label(label)
                    .map_err(graph_read_error)?,
            );
        }
        ids.sort_unstable();
        ids.dedup();
        Ok(ids)
    }

    fn node_ids_by_label_property(
        &self,
        label: &str,
        property_key: &str,
        property_value: &Value,
    ) -> GraphReadResult<Vec<NodeId>> {
        let mut ids = Vec::new();
        for partition in &self.partitions {
            ids.extend(
                partition
                    .node_ids_by_label_property(label, property_key, property_value)
                    .map_err(graph_read_error)?,
            );
        }
        ids.sort_unstable();
        ids.dedup();
        Ok(ids)
    }

    fn boundary_node_ids_by_label_property(
        &self,
        label: &str,
        property_key: &str,
        property_value: &Value,
    ) -> GraphReadResult<Vec<NodeId>> {
        let mut ids = Vec::new();
        for partition in &self.partitions {
            ids.extend(
                partition
                    .boundary_node_ids_by_label_property(label, property_key, property_value)
                    .map_err(graph_read_error)?,
            );
        }
        ids.sort_unstable();
        ids.dedup();
        Ok(ids)
    }

    fn outgoing(&self, node_id: NodeId) -> GraphReadResult<Vec<Relationship>> {
        let mut relationships = Vec::new();
        for partition in &self.partitions {
            relationships.extend(partition.outgoing(node_id).map_err(graph_read_error)?);
        }
        Ok(relationships)
    }

    fn outgoing_by_type(
        &self,
        node_id: NodeId,
        rel_type: &str,
    ) -> GraphReadResult<Vec<Relationship>> {
        let mut relationships = Vec::new();
        for partition in &self.partitions {
            relationships.extend(
                partition
                    .outgoing_by_type(node_id, rel_type)
                    .map_err(graph_read_error)?,
            );
        }
        Ok(relationships)
    }

    fn incoming(&self, node_id: NodeId) -> GraphReadResult<Vec<Relationship>> {
        let mut relationships = Vec::new();
        for partition in &self.partitions {
            relationships.extend(partition.incoming(node_id).map_err(graph_read_error)?);
        }
        Ok(relationships)
    }

    fn incoming_by_type(
        &self,
        node_id: NodeId,
        rel_type: &str,
    ) -> GraphReadResult<Vec<Relationship>> {
        let mut relationships = Vec::new();
        for partition in &self.partitions {
            relationships.extend(
                partition
                    .incoming_by_type(node_id, rel_type)
                    .map_err(graph_read_error)?,
            );
        }
        Ok(relationships)
    }
}

fn graph_read_error(err: StorageError) -> GraphReadError {
    GraphReadError::Store(err.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MemoryKvStore;
    use neo4r_core::{Properties, ShardMap, ShardPolicy};
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn rejects_empty_partition_list() {
        match PartitionedGraphStore::<MemoryKvStore>::new(Vec::new()) {
            Err(StorageError::CorruptStore(_)) => {}
            Err(err) => panic!("unexpected error: {err}"),
            Ok(_) => panic!("expected empty partition list to fail"),
        }
    }

    #[test]
    fn routes_shards_to_local_partitions() {
        let store = memory_partitioned_store(4);

        assert_eq!(store.local_partition_id_for_shard(0), 0);
        assert_eq!(store.local_partition_id_for_shard(3), 3);
        assert_eq!(store.local_partition_id_for_shard(4), 0);
        assert_eq!(store.local_partition_id_for_shard(9), 1);
    }

    #[test]
    fn explicit_placement_overrides_modulo_routing() {
        let placement = LocalPartitionMap::new(4)
            .unwrap()
            .with_assignment(9, 3)
            .unwrap();
        let store = PartitionedGraphStore::with_placement(memory_partitions(4), placement).unwrap();

        assert_eq!(store.local_partition_id_for_shard(9), 3);
        assert_eq!(store.local_partition_id_for_shard(10), 2);
    }

    #[test]
    fn rejects_placement_outside_partition_count() {
        let err = LocalPartitionMap::new(2)
            .unwrap()
            .with_assignment(1, 2)
            .unwrap_err();

        assert!(matches!(err, StorageError::CorruptStore(_)));
    }

    #[test]
    fn applies_commands_to_the_partition_for_the_shard() {
        let mut store = memory_partitioned_store(2);
        let shard_map = ShardMap::new(4).unwrap();
        let command = Command::CreateNode {
            id: 3,
            labels: vec!["Person".to_string()],
            properties: properties(&[("name", Value::String("Alice".to_string()))]),
        };

        store.apply(shard_map.owner_of_node(3), &command).unwrap();

        assert!(store.partitions()[0].node(3).unwrap().is_none());
        assert!(store.partitions()[1].node(3).unwrap().is_some());
        assert_eq!(store.node_ids_by_label("Person").unwrap(), vec![3],);
    }

    #[test]
    fn opens_multiple_rocksdb_partition_directories() {
        let path = temp_path("partitioned-rocks");
        {
            let mut store = PartitionedGraphStore::open_rocks(&path, 3).unwrap();
            store
                .apply(
                    5,
                    &Command::CreateNode {
                        id: 5,
                        labels: vec!["Person".to_string()],
                        properties: properties(&[("name", Value::String("Bob".to_string()))]),
                    },
                )
                .unwrap();

            assert_eq!(store.partition_count(), 3);
            assert!(path.join("partitions/0000/rocksdb").is_dir());
            assert!(path.join("partitions/0001/rocksdb").is_dir());
            assert!(path.join("partitions/0002/rocksdb").is_dir());
        }

        {
            let store = PartitionedGraphStore::open_rocks(&path, 3).unwrap();
            assert!(store.node(5).unwrap().is_some());
        }

        let _ = fs::remove_dir_all(path);
    }

    fn memory_partitioned_store(partition_count: usize) -> PartitionedGraphStore<MemoryKvStore> {
        PartitionedGraphStore::new(memory_partitions(partition_count)).unwrap()
    }

    fn memory_partitions(partition_count: usize) -> Vec<KvGraphStore<MemoryKvStore>> {
        (0..partition_count)
            .map(|_| KvGraphStore::new(MemoryKvStore::new()))
            .collect()
    }

    fn properties(entries: &[(&str, Value)]) -> Properties {
        entries
            .iter()
            .map(|(key, value)| ((*key).to_string(), value.clone()))
            .collect()
    }

    fn temp_path(name: &str) -> std::path::PathBuf {
        let mut path = std::env::temp_dir();
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        path.push(format!("neo4r-{name}-{}-{nanos}", std::process::id()));
        let _ = fs::remove_dir_all(&path);
        path
    }
}
