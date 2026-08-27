use crate::{
    DatabaseError, DatabaseResult, NoopShardReplicator, ReplicationOutcome, ShardReplicator,
};
mod vector_index;
use neo4r_core::{
    BoundaryNode, Command, GraphError, GraphRead, GraphReadError, GraphReadResult, HybridClock,
    HybridTimestamp, LogEntry, LogIndex, Node, NodeId, Properties, Relationship, RelationshipId,
    ServerId, ShardId, ShardMap, ShardPlacement, ShardPolicy, ShardReplica, ShardRole,
    ShardRoutingTable, Value,
};
use neo4r_query::{
    CypherEngine, QueryCursor, QueryEngine, QueryError, QueryParams, QueryRow, QueryValue,
    VecQueryCursor,
};
use neo4r_storage::{
    CheckpointStore, ClusterMembership, ClusterMembershipStore, ClusterNode,
    ClusterShardAssignment, CommitStore, IndexCatalog, IndexCatalogStore, IndexDefinition,
    IndexKind, NodeMembershipState, PartitionedGraphStore, RocksKvSnapshot, RocksKvStore,
    SegmentedShardLog, ShardAssignmentState, ShardMetadataStore, StorageError,
};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, ErrorKind, Write};
use std::path::{Path, PathBuf};
use std::sync::{mpsc, Arc, Mutex, MutexGuard};
use std::thread;
use std::time::{Duration, Instant};
pub use vector_index::VectorIndexStatus;
#[cfg(test)]
use vector_index::VECTOR_INDEX_CACHE_MAGIC;
use vector_index::{
    load_vector_index_cache, save_vector_index_cache, vector_definition_parts, vector_metric_name,
    PersistentVectorIndexes, SharedVectorIndexProvider,
};

#[derive(Clone, Debug)]
pub struct DatabaseConfig {
    pub data_dir: PathBuf,
    pub shard_count: u64,
    pub local_partition_count: usize,
    pub log_entries_per_segment: LogIndex,
    pub checkpoint_interval: LogIndex,
    pub wal_sync_interval: LogIndex,
    pub group_commit_max_entries: usize,
    pub group_commit_max_delay: Duration,
    pub server_id: ServerId,
    pub routing_table: Option<ShardRoutingTable>,
}

impl DatabaseConfig {
    pub fn new(
        data_dir: impl Into<PathBuf>,
        shard_count: u64,
        local_partition_count: usize,
    ) -> Self {
        Self {
            data_dir: data_dir.into(),
            shard_count,
            local_partition_count,
            log_entries_per_segment: 1024,
            checkpoint_interval: 128,
            wal_sync_interval: 128,
            group_commit_max_entries: 32,
            group_commit_max_delay: Duration::from_millis(1),
            server_id: 0,
            routing_table: None,
        }
    }

    pub fn with_log_entries_per_segment(mut self, entries_per_segment: LogIndex) -> Self {
        self.log_entries_per_segment = entries_per_segment;
        self
    }

    pub fn with_checkpoint_interval(mut self, checkpoint_interval: LogIndex) -> Self {
        self.checkpoint_interval = checkpoint_interval;
        self
    }

    pub fn with_wal_sync_interval(mut self, wal_sync_interval: LogIndex) -> Self {
        self.wal_sync_interval = wal_sync_interval;
        self
    }

    pub fn with_group_commit_max_entries(mut self, max_entries: usize) -> Self {
        self.group_commit_max_entries = max_entries;
        self
    }

    pub fn with_group_commit_max_delay(mut self, max_delay: Duration) -> Self {
        self.group_commit_max_delay = max_delay;
        self
    }

    pub fn with_server_id(mut self, server_id: ServerId) -> Self {
        self.server_id = server_id;
        self
    }

    pub fn with_routing_table(mut self, routing_table: ShardRoutingTable) -> Self {
        self.routing_table = Some(routing_table);
        self
    }
}

pub struct Neo4rDatabase {
    config: DatabaseConfig,
    shard_map: ShardMap,
    store: PartitionedGraphStore<RocksKvStore>,
    logs: Vec<SegmentedShardLog>,
    checkpoints: Vec<CheckpointStore>,
    commits: Vec<CommitStore>,
    shard_metadata: ShardMetadataStore,
    membership_store: ClusterMembershipStore,
    membership: ClusterMembership,
    rebalance_plan_store: RebalancePlanStore,
    rebalance_execution_store: RebalanceExecutionStore,
    rebalance_execution: Option<RebalanceExecution>,
    cluster_metadata_store: ClusterMetadataStore,
    cluster_metadata: ClusterMetadataState,
    metadata_log_store: MetadataOperationLogStore,
    statistics_store: StatisticsCatalogStore,
    statistics: StatisticsCatalog,
    read_cache: Mutex<ReadPathCache>,
    read_cache_stats: Mutex<ReadCacheStats>,
    index_catalog_store: IndexCatalogStore,
    index_catalog: IndexCatalog,
    vector_indexes: Arc<Mutex<PersistentVectorIndexes>>,
    routing_table: ShardRoutingTable,
    next_log_indexes: Vec<LogIndex>,
    commit_indexes: Vec<LogIndex>,
    match_indexes: Vec<BTreeMap<ServerId, LogIndex>>,
    next_node_id: NodeId,
    next_relationship_id: RelationshipId,
    clock: HybridClock,
    query_engine: CypherEngine,
    replicator: Arc<dyn ShardReplicator>,
}

#[derive(Clone)]
pub struct Neo4rDatabaseHandle {
    inner: Arc<Mutex<Neo4rDatabase>>,
    writer: Arc<WriterActor>,
}

pub struct Neo4rReadSnapshot {
    store: PartitionedGraphStore<RocksKvSnapshot>,
    shard_map: ShardMap,
    timestamp: HybridTimestamp,
    applied_indexes: Vec<LogIndex>,
    committed_indexes: Vec<LogIndex>,
    query_engine: CypherEngine,
}

pub struct Neo4rReadTransaction {
    snapshot: Neo4rReadSnapshot,
    options: QueryOptions,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReadConsistency {
    Strong,
    FollowerStale,
    BoundedStaleness { max_staleness_ms: u64 },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReadIsolation {
    ReadCommitted,
    Snapshot,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct QueryOptions {
    pub consistency: ReadConsistency,
    pub isolation: ReadIsolation,
}

impl Default for QueryOptions {
    fn default() -> Self {
        Self {
            consistency: ReadConsistency::Strong,
            isolation: ReadIsolation::ReadCommitted,
        }
    }
}

impl QueryOptions {
    pub fn with_consistency(mut self, consistency: ReadConsistency) -> Self {
        self.consistency = consistency;
        self
    }

    pub fn with_isolation(mut self, isolation: ReadIsolation) -> Self {
        self.isolation = isolation;
        self
    }
}

impl Neo4rDatabaseHandle {
    pub fn open(config: DatabaseConfig) -> DatabaseResult<Self> {
        Self::open_with_replicator(config, Arc::new(NoopShardReplicator))
    }

    pub fn open_with_replicator(
        config: DatabaseConfig,
        replicator: Arc<dyn ShardReplicator>,
    ) -> DatabaseResult<Self> {
        let inner = Arc::new(Mutex::new(Neo4rDatabase::open_with_replicator(
            config, replicator,
        )?));
        let writer = spawn_writer_actor(inner.clone());
        Ok(Self { inner, writer })
    }

    pub fn open_path(
        data_dir: impl AsRef<Path>,
        shard_count: u64,
        local_partition_count: usize,
    ) -> DatabaseResult<Self> {
        Self::open(DatabaseConfig::new(
            data_dir.as_ref(),
            shard_count,
            local_partition_count,
        ))
    }

    pub fn create_node(
        &self,
        labels: Vec<String>,
        properties: Properties,
    ) -> DatabaseResult<NodeId> {
        match self.send_write(WriteOperation::CreateNode { labels, properties })? {
            WriteResponse::NodeId(id) => Ok(id),
            response => Err(unexpected_write_response(response)),
        }
    }

    pub fn create_node_on_shard(
        &self,
        shard_id: ShardId,
        labels: Vec<String>,
        properties: Properties,
    ) -> DatabaseResult<NodeId> {
        match self.send_write(WriteOperation::CreateNodeOnShard {
            shard_id,
            labels,
            properties,
        })? {
            WriteResponse::NodeId(id) => Ok(id),
            response => Err(unexpected_write_response(response)),
        }
    }

    pub fn create_relationship(
        &self,
        from: NodeId,
        to: NodeId,
        rel_type: String,
        properties: Properties,
    ) -> DatabaseResult<RelationshipId> {
        match self.send_write(WriteOperation::CreateRelationship {
            from,
            to,
            rel_type,
            properties,
        })? {
            WriteResponse::RelationshipId(id) => Ok(id),
            response => Err(unexpected_write_response(response)),
        }
    }

    pub fn set_node_property(&self, id: NodeId, key: String, value: Value) -> DatabaseResult<()> {
        expect_unit(self.send_write(WriteOperation::SetNodeProperty { id, key, value })?)
    }

    pub fn remove_node_property(&self, id: NodeId, key: String) -> DatabaseResult<()> {
        expect_unit(self.send_write(WriteOperation::RemoveNodeProperty { id, key })?)
    }

    pub fn add_node_label(&self, id: NodeId, label: String) -> DatabaseResult<()> {
        expect_unit(self.send_write(WriteOperation::AddNodeLabel { id, label })?)
    }

    pub fn remove_node_label(&self, id: NodeId, label: String) -> DatabaseResult<()> {
        expect_unit(self.send_write(WriteOperation::RemoveNodeLabel { id, label })?)
    }

    pub fn set_relationship_property(
        &self,
        id: RelationshipId,
        key: String,
        value: Value,
    ) -> DatabaseResult<()> {
        expect_unit(self.send_write(WriteOperation::SetRelationshipProperty { id, key, value })?)
    }

    pub fn remove_relationship_property(
        &self,
        id: RelationshipId,
        key: String,
    ) -> DatabaseResult<()> {
        expect_unit(self.send_write(WriteOperation::RemoveRelationshipProperty { id, key })?)
    }

    pub fn delete_relationship(&self, id: RelationshipId) -> DatabaseResult<()> {
        expect_unit(self.send_write(WriteOperation::DeleteRelationship { id })?)
    }

    pub fn relationship_owner_shard(&self, id: RelationshipId) -> DatabaseResult<ShardId> {
        self.inner
            .lock()
            .map_err(|_| DatabaseError::LockPoisoned)?
            .relationship_owner_shard(id)
    }

    pub fn node(&self, id: NodeId) -> DatabaseResult<Option<Node>> {
        self.inner
            .lock()
            .map_err(|_| DatabaseError::LockPoisoned)?
            .node(id)
    }

    pub fn relationship(&self, id: RelationshipId) -> DatabaseResult<Option<Relationship>> {
        self.inner
            .lock()
            .map_err(|_| DatabaseError::LockPoisoned)?
            .relationship(id)
    }

    pub fn delete_node(&self, id: NodeId) -> DatabaseResult<()> {
        expect_unit(self.send_write(WriteOperation::DeleteNode { id })?)
    }

    pub fn execute_cypher(&self, query: &str) -> DatabaseResult<Vec<QueryRow>> {
        self.execute_cypher_with_params(query, QueryParams::new())
    }

    pub fn execute_cypher_with_params(
        &self,
        query: &str,
        params: QueryParams,
    ) -> DatabaseResult<Vec<QueryRow>> {
        if let Some(rows) = self.show_index_rows_for_query(query)? {
            return Ok(rows);
        }
        if let Some(rows) = self.show_constraint_rows_for_query(query)? {
            return Ok(rows);
        }
        match parse_write_cypher(query, &params)? {
            Some(WriteCypher::CreateNode {
                variable,
                labels,
                properties,
                assignments,
                replacement,
                returns,
            }) => {
                let properties = create_properties_after_set(properties, assignments, replacement);
                let id = self.create_node(labels.clone(), properties.clone())?;
                Ok(return_created_node(
                    variable, returns, id, labels, properties,
                ))
            }
            Some(WriteCypher::CreateRelationship {
                variable,
                from_matcher,
                to_matcher,
                rel_type,
                properties,
                assignments,
                replacement,
                returns,
            }) => {
                let properties = create_properties_after_set(properties, assignments, replacement);
                let from_ids = self.match_node_ids(&from_matcher, &params)?;
                let to_ids = self.match_node_ids(&to_matcher, &params)?;
                let mut rows = Vec::new();
                for from in &from_ids {
                    for to in &to_ids {
                        let id = self.create_relationship(
                            *from,
                            *to,
                            rel_type.clone(),
                            properties.clone(),
                        )?;
                        rows.extend(return_created_relationship(
                            variable.clone(),
                            returns.clone(),
                            Relationship::new(id, *from, *to, rel_type.clone(), properties.clone()),
                        ));
                    }
                }
                Ok(rows)
            }
            Some(WriteCypher::MergeNode { .. }) => {
                self.lock()?.execute_cypher_with_params(query, &params)
            }
            Some(WriteCypher::MergeRelationship { .. }) => {
                self.lock()?.execute_cypher_with_params(query, &params)
            }
            Some(WriteCypher::SetNodeProperty {
                matcher,
                assignments,
                returns,
            }) => {
                let node_ids = self.match_node_ids(&matcher, &params)?;
                for id in &node_ids {
                    for assignment in &assignments {
                        apply_node_property_assignment_with_handle(self, *id, assignment)?;
                    }
                }
                return_nodes_after_write(&node_ids, returns.as_ref(), |id| self.node(id))
            }
            Some(WriteCypher::ReplaceNodeProperties {
                matcher,
                properties,
                returns,
            }) => {
                let node_ids = self.match_node_ids(&matcher, &params)?;
                for id in &node_ids {
                    let current = self
                        .node(*id)?
                        .ok_or(DatabaseError::Graph(GraphError::NodeNotFound(*id)))?;
                    replace_node_properties_with_handle(
                        self,
                        *id,
                        &current.properties,
                        &properties,
                    )?;
                }
                return_nodes_after_write(&node_ids, returns.as_ref(), |id| self.node(id))
            }
            Some(WriteCypher::RemoveNodeProperty {
                matcher,
                keys,
                returns,
            }) => {
                let node_ids = self.match_node_ids(&matcher, &params)?;
                for id in &node_ids {
                    for key in &keys {
                        self.remove_node_property(*id, key.clone())?;
                    }
                }
                return_nodes_after_write(&node_ids, returns.as_ref(), |id| self.node(id))
            }
            Some(WriteCypher::AddNodeLabel {
                matcher,
                labels,
                returns,
            }) => {
                let node_ids = self.match_node_ids(&matcher, &params)?;
                for id in &node_ids {
                    for label in &labels {
                        self.add_node_label(*id, label.clone())?;
                    }
                }
                return_nodes_after_write(&node_ids, returns.as_ref(), |id| self.node(id))
            }
            Some(WriteCypher::RemoveNodeLabel {
                matcher,
                labels,
                returns,
            }) => {
                let node_ids = self.match_node_ids(&matcher, &params)?;
                for id in &node_ids {
                    for label in &labels {
                        self.remove_node_label(*id, label.clone())?;
                    }
                }
                return_nodes_after_write(&node_ids, returns.as_ref(), |id| self.node(id))
            }
            Some(WriteCypher::SetRelationshipProperty {
                matcher,
                assignments,
                returns,
            }) => {
                let relationship_ids = self.match_relationship_ids(&matcher, &params)?;
                for id in &relationship_ids {
                    for assignment in &assignments {
                        apply_relationship_property_assignment_with_handle(self, *id, assignment)?;
                    }
                }
                return_relationships_after_write(&relationship_ids, returns.as_ref(), |id| {
                    self.relationship(id)
                })
            }
            Some(WriteCypher::ReplaceRelationshipProperties {
                matcher,
                properties,
                returns,
            }) => {
                let relationship_ids = self.match_relationship_ids(&matcher, &params)?;
                for id in &relationship_ids {
                    let current = self
                        .relationship(*id)?
                        .ok_or(DatabaseError::Graph(GraphError::RelationshipNotFound(*id)))?;
                    replace_relationship_properties_with_handle(
                        self,
                        *id,
                        &current.properties,
                        &properties,
                    )?;
                }
                return_relationships_after_write(&relationship_ids, returns.as_ref(), |id| {
                    self.relationship(id)
                })
            }
            Some(WriteCypher::RemoveRelationshipProperty {
                matcher,
                keys,
                returns,
            }) => {
                let relationship_ids = self.match_relationship_ids(&matcher, &params)?;
                for id in &relationship_ids {
                    for key in &keys {
                        self.remove_relationship_property(*id, key.clone())?;
                    }
                }
                return_relationships_after_write(&relationship_ids, returns.as_ref(), |id| {
                    self.relationship(id)
                })
            }
            Some(WriteCypher::DeleteNode { matcher, returns }) => {
                let node_ids = self.match_node_ids(&matcher, &params)?;
                let rows =
                    return_nodes_after_write(&node_ids, returns.as_ref(), |id| self.node(id))?;
                for id in &node_ids {
                    self.delete_node(*id)?;
                }
                Ok(rows)
            }
            Some(WriteCypher::DeleteRelationship { matcher, returns }) => {
                let relationship_ids = self.match_relationship_ids(&matcher, &params)?;
                let rows =
                    return_relationships_after_write(&relationship_ids, returns.as_ref(), |id| {
                        self.relationship(id)
                    })?;
                for id in &relationship_ids {
                    self.delete_relationship(*id)?;
                }
                Ok(rows)
            }
            Some(WriteCypher::CreateNodePropertyIndex {
                name,
                label,
                property,
                if_not_exists,
            }) => {
                if if_not_exists {
                    self.lock()?
                        .create_node_property_index_if_not_exists(name, label, property)?;
                } else {
                    self.create_node_property_index(name, label, property)?;
                }
                Ok(Vec::new())
            }
            Some(WriteCypher::CreateUniqueNodePropertyConstraint {
                name,
                label,
                property,
                if_not_exists,
            }) => {
                if if_not_exists {
                    self.lock()?
                        .create_unique_node_property_constraint_if_not_exists(
                            name, label, property,
                        )?;
                } else {
                    self.lock()?
                        .create_unique_node_property_constraint(name, label, property)?;
                }
                Ok(Vec::new())
            }
            Some(WriteCypher::CreateVectorIndex {
                name,
                label,
                property,
                dimensions,
                metric,
                if_not_exists,
            }) => {
                if if_not_exists {
                    self.lock()?.create_vector_index_if_not_exists(
                        name, label, property, dimensions, metric,
                    )?;
                } else {
                    self.create_vector_index(name, label, property, dimensions, metric)?;
                }
                Ok(Vec::new())
            }
            Some(WriteCypher::RebuildVectorIndex { name }) => {
                self.rebuild_vector_index(&name)?;
                Ok(Vec::new())
            }
            Some(WriteCypher::DropIndex { name, if_exists }) => {
                if if_exists {
                    self.drop_index_if_exists(&name)?;
                } else {
                    self.drop_index(&name)?;
                }
                Ok(Vec::new())
            }
            Some(WriteCypher::DropConstraint { name, if_exists }) => {
                if if_exists {
                    self.drop_constraint_if_exists(&name)?;
                } else {
                    self.drop_constraint(&name)?;
                }
                Ok(Vec::new())
            }
            None => self.query_with_params(query, params),
        }
    }

    pub fn execute_cypher_on_shard(
        &self,
        shard_id: ShardId,
        query: &str,
        params: QueryParams,
    ) -> DatabaseResult<Vec<QueryRow>> {
        self.lock()?
            .execute_cypher_on_shard(shard_id, query, &params)
    }

    pub fn execute_cypher_mutation_batch_on_shard(
        &self,
        shard_id: ShardId,
        writes: Vec<(String, QueryParams)>,
    ) -> DatabaseResult<usize> {
        self.lock()?
            .execute_cypher_mutation_batch_on_shard(shard_id, writes)
    }

    pub fn execute_cypher_mutation_batch(
        &self,
        writes: Vec<(String, QueryParams)>,
    ) -> DatabaseResult<usize> {
        self.lock()?.execute_cypher_mutation_batch(writes)
    }

    pub fn execute_staged_cypher_transaction_on_shard(
        &self,
        shard_id: ShardId,
        writes: Vec<(String, QueryParams)>,
    ) -> DatabaseResult<usize> {
        self.lock()?
            .execute_staged_cypher_transaction_on_shard(shard_id, writes)
    }

    pub fn execute_create_node_cypher_on_shard(
        &self,
        shard_id: ShardId,
        query: &str,
        params: QueryParams,
    ) -> DatabaseResult<Vec<QueryRow>> {
        self.execute_cypher_on_shard(shard_id, query, params)
    }

    pub fn write_cypher_target_shards(
        &self,
        query: &str,
        params: QueryParams,
    ) -> DatabaseResult<Vec<ShardId>> {
        self.lock()?.write_cypher_target_shards(query, &params)
    }

    pub fn query(&self, query: &str) -> DatabaseResult<Vec<QueryRow>> {
        self.query_with_options(query, QueryOptions::default())
    }

    pub fn query_with_params(
        &self,
        query: &str,
        params: QueryParams,
    ) -> DatabaseResult<Vec<QueryRow>> {
        self.query_with_params_and_options(query, params, QueryOptions::default())
    }

    pub fn query_with_options(
        &self,
        query: &str,
        options: QueryOptions,
    ) -> DatabaseResult<Vec<QueryRow>> {
        self.query_with_params_and_options(query, QueryParams::new(), options)
    }

    pub fn query_with_params_and_options(
        &self,
        query: &str,
        params: QueryParams,
        options: QueryOptions,
    ) -> DatabaseResult<Vec<QueryRow>> {
        if let Some(rows) = self.show_index_rows_for_query(query)? {
            validate_read_isolation(options.isolation);
            return Ok(rows);
        }
        if let Some(rows) = self.show_constraint_rows_for_query(query)? {
            validate_read_isolation(options.isolation);
            return Ok(rows);
        }
        let snapshot = self.read_snapshot()?;
        validate_read_options(&snapshot, options)?;
        snapshot.query_with_params(query, &params)
    }

    pub fn query_shard(&self, shard_id: ShardId, query: &str) -> DatabaseResult<Vec<QueryRow>> {
        self.query_shard_with_params_and_options(
            shard_id,
            query,
            QueryParams::new(),
            QueryOptions::default(),
        )
    }

    pub fn query_shard_with_params(
        &self,
        shard_id: ShardId,
        query: &str,
        params: QueryParams,
    ) -> DatabaseResult<Vec<QueryRow>> {
        self.query_shard_with_params_and_options(shard_id, query, params, QueryOptions::default())
    }

    pub fn query_shard_with_params_and_options(
        &self,
        shard_id: ShardId,
        query: &str,
        params: QueryParams,
        options: QueryOptions,
    ) -> DatabaseResult<Vec<QueryRow>> {
        {
            let database = self.lock()?;
            if shard_id >= database.shard_count() {
                return Err(DatabaseError::MissingShardLog(shard_id));
            }
            database.ensure_local_copy(shard_id)?;
        }
        let snapshot = self.read_snapshot()?;
        validate_read_options(&snapshot, options)?;
        snapshot.query_shard_with_params(shard_id, query, &params)
    }

    pub fn query_cursor(&self, query: &str) -> DatabaseResult<Box<dyn QueryCursor>> {
        self.query_cursor_with_options(query, QueryOptions::default())
    }

    pub fn query_cursor_with_params(
        &self,
        query: &str,
        params: QueryParams,
    ) -> DatabaseResult<Box<dyn QueryCursor>> {
        self.query_cursor_with_params_and_options(query, params, QueryOptions::default())
    }

    pub fn query_cursor_with_options(
        &self,
        query: &str,
        options: QueryOptions,
    ) -> DatabaseResult<Box<dyn QueryCursor>> {
        self.query_cursor_with_params_and_options(query, QueryParams::new(), options)
    }

    pub fn query_cursor_with_params_and_options(
        &self,
        query: &str,
        params: QueryParams,
        options: QueryOptions,
    ) -> DatabaseResult<Box<dyn QueryCursor>> {
        if let Some(rows) = self.show_index_rows_for_query(query)? {
            validate_read_isolation(options.isolation);
            return Ok(Box::new(VecQueryCursor::new(rows)));
        }
        if let Some(rows) = self.show_constraint_rows_for_query(query)? {
            validate_read_isolation(options.isolation);
            return Ok(Box::new(VecQueryCursor::new(rows)));
        }
        let snapshot = self.read_snapshot()?;
        validate_read_options(&snapshot, options)?;
        snapshot.into_query_cursor_with_params(query, params)
    }

    pub fn query_cursor_with_staged_writes(
        &self,
        query: &str,
        params: QueryParams,
        options: QueryOptions,
        staged_writes: &[(String, QueryParams)],
    ) -> DatabaseResult<Box<dyn QueryCursor>> {
        if let Some(rows) = self.show_index_rows_for_query(query)? {
            validate_read_isolation(options.isolation);
            return Ok(Box::new(VecQueryCursor::new(rows)));
        }
        if let Some(rows) = self.show_constraint_rows_for_query(query)? {
            validate_read_isolation(options.isolation);
            return Ok(Box::new(VecQueryCursor::new(rows)));
        }
        let snapshot = self.read_snapshot()?;
        validate_read_options(&snapshot, options)?;
        let overlay = snapshot.build_staged_overlay(staged_writes)?;
        let graph = StagedOverlayGraph {
            base: &snapshot.store,
            node_overlay: &overlay.nodes,
            relationship_overlay: &overlay.relationships,
        };
        Ok(Box::new(VecQueryCursor::new(
            snapshot
                .query_engine
                .execute_with_params(&graph, query, &params)?,
        )))
    }

    pub fn query_shard_with_staged_writes(
        &self,
        shard_id: ShardId,
        query: &str,
        params: QueryParams,
        options: QueryOptions,
        staged_writes: &[(String, QueryParams)],
    ) -> DatabaseResult<Vec<QueryRow>> {
        {
            let database = self.lock()?;
            if shard_id >= database.shard_count() {
                return Err(DatabaseError::MissingShardLog(shard_id));
            }
            database.ensure_local_copy(shard_id)?;
        }
        if let Some(rows) = self.show_index_rows_for_query(query)? {
            validate_read_isolation(options.isolation);
            return Ok(rows);
        }
        if let Some(rows) = self.show_constraint_rows_for_query(query)? {
            validate_read_isolation(options.isolation);
            return Ok(rows);
        }
        let snapshot = self.read_snapshot()?;
        validate_read_options(&snapshot, options)?;
        snapshot.query_shard_with_staged_writes(shard_id, query, &params, staged_writes)
    }

    pub fn read_snapshot(&self) -> DatabaseResult<Neo4rReadSnapshot> {
        self.lock()?.read_snapshot()
    }

    pub fn begin_read_transaction(&self) -> DatabaseResult<Neo4rReadTransaction> {
        self.begin_read_transaction_with_options(
            QueryOptions::default().with_isolation(ReadIsolation::Snapshot),
        )
    }

    pub fn begin_read_transaction_with_options(
        &self,
        options: QueryOptions,
    ) -> DatabaseResult<Neo4rReadTransaction> {
        let snapshot = self.read_snapshot()?;
        validate_read_options(&snapshot, options)?;
        Ok(Neo4rReadTransaction { snapshot, options })
    }

    pub fn apply_replicated_entry(&self, entry: LogEntry) -> DatabaseResult<()> {
        expect_unit(self.send_write(WriteOperation::ApplyReplicatedEntry(entry))?)
    }

    pub fn apply_replicated_entries(&self, entries: Vec<LogEntry>) -> DatabaseResult<()> {
        expect_unit(self.send_write(WriteOperation::ApplyReplicatedEntries(entries))?)
    }

    pub fn shard_count(&self) -> DatabaseResult<u64> {
        Ok(self.lock()?.shard_count())
    }

    pub fn local_partition_count(&self) -> DatabaseResult<usize> {
        Ok(self.lock()?.local_partition_count())
    }

    pub fn data_dir(&self) -> DatabaseResult<PathBuf> {
        Ok(self.lock()?.data_dir().to_path_buf())
    }

    pub fn query_route(&self) -> DatabaseResult<QueryRoute> {
        Ok(self.lock()?.query_route())
    }

    pub fn query_plan(&self, query: &str) -> DatabaseResult<DistributedQueryPlan> {
        self.query_plan_with_params(query, QueryParams::new())
    }

    pub fn query_plan_with_params(
        &self,
        query: &str,
        params: QueryParams,
    ) -> DatabaseResult<DistributedQueryPlan> {
        Ok(self.lock()?.query_plan(query, &params))
    }

    pub fn profile_query(&self, query: &str, params: QueryParams) -> DatabaseResult<QueryProfile> {
        let planning_start = Instant::now();
        let plan = self.query_plan_with_params(query, params.clone())?;
        let planning_elapsed_micros = planning_start.elapsed().as_micros();

        let before_cache = self.lock()?.read_cache_stats()?;
        let execution_start = Instant::now();
        let rows = self.query_with_params(query, params)?;
        let execution_elapsed_micros = execution_start.elapsed().as_micros();
        let database = self.lock()?;
        let statistics = database.statistics_catalog()?;
        let after_cache = database.read_cache_stats()?;

        Ok(QueryProfile {
            operators: vec![query_operator_profile(
                &plan.access_plan,
                plan.estimated_rows,
                rows.len(),
                execution_elapsed_micros,
            )],
            metrics: QueryMetrics {
                planning_elapsed_micros,
                execution_elapsed_micros,
                rows_returned: rows.len(),
                scanned_nodes: estimated_scanned_nodes(&statistics, &plan.access_plan),
                scanned_relationships: estimated_scanned_relationships(
                    &statistics,
                    &plan.access_plan,
                ),
                index_count: database.index_catalog.indexes.len(),
                read_cache_hits: after_cache.hits.saturating_sub(before_cache.hits),
                read_cache_misses: after_cache.misses.saturating_sub(before_cache.misses),
                index_cache_hits: after_cache
                    .index_hits
                    .saturating_sub(before_cache.index_hits),
                index_cache_misses: after_cache
                    .index_misses
                    .saturating_sub(before_cache.index_misses),
            },
            plan,
        })
    }

    pub fn storage_status(&self) -> DatabaseResult<StorageStatus> {
        self.lock()?.storage_status()
    }

    pub fn statistics_catalog(&self) -> DatabaseResult<StatisticsCatalog> {
        self.lock()?.statistics_catalog()
    }

    pub fn checkpoint_now(&self) -> DatabaseResult<StorageMaintenanceResult> {
        self.lock()?.checkpoint_now()
    }

    pub fn compact_storage(&self) -> DatabaseResult<StorageMaintenanceResult> {
        self.lock()?.compact_storage()
    }

    pub fn metadata_operations(&self) -> DatabaseResult<Vec<MetadataOperationRecord>> {
        self.lock()?.metadata_operations()
    }

    pub fn cluster_status(&self) -> DatabaseResult<ClusterStatus> {
        Ok(self.lock()?.cluster_status())
    }

    pub fn install_routing_table(&self, routing_table: ShardRoutingTable) -> DatabaseResult<()> {
        self.lock()?.install_routing_table(routing_table)
    }

    pub fn register_replication_peer(
        &self,
        server_id: ServerId,
        address: impl Into<String>,
    ) -> DatabaseResult<()> {
        self.lock()?
            .register_replication_peer(server_id, address.into())
    }

    pub fn unregister_replication_peer(&self, server_id: ServerId) -> DatabaseResult<()> {
        self.lock()?.unregister_replication_peer(server_id)
    }

    pub fn routing_table(&self) -> DatabaseResult<ShardRoutingTable> {
        Ok(self.lock()?.routing_table().clone())
    }

    pub fn register_cluster_node(
        &self,
        server_id: ServerId,
        address: impl Into<String>,
    ) -> DatabaseResult<ClusterMembership> {
        self.lock()?
            .register_cluster_node(server_id, address.into())
    }

    pub fn request_cluster_join(
        &self,
        server_id: ServerId,
        address: impl Into<String>,
        protocol_version: u64,
        storage_version: u64,
        shard_count: u64,
    ) -> DatabaseResult<ClusterMembership> {
        self.lock()?.request_cluster_join(
            server_id,
            address.into(),
            protocol_version,
            storage_version,
            shard_count,
        )
    }

    pub fn accept_cluster_join(&self, server_id: ServerId) -> DatabaseResult<ClusterMembership> {
        self.lock()?.accept_cluster_join(server_id)
    }

    pub fn reject_cluster_join(
        &self,
        server_id: ServerId,
        reason: impl Into<String>,
    ) -> DatabaseResult<ClusterMembership> {
        self.lock()?.reject_cluster_join(server_id, reason.into())
    }

    pub fn decommission_cluster_node(
        &self,
        server_id: ServerId,
    ) -> DatabaseResult<ClusterMembership> {
        self.lock()?.decommission_cluster_node(server_id)
    }

    pub fn cluster_membership(&self) -> DatabaseResult<ClusterMembership> {
        Ok(self.lock()?.cluster_membership().clone())
    }

    pub fn cluster_metadata(&self) -> DatabaseResult<ClusterMetadataState> {
        Ok(self.lock()?.cluster_metadata().clone())
    }

    pub fn set_metadata_authority(
        &self,
        server_id: ServerId,
    ) -> DatabaseResult<ClusterMetadataState> {
        self.lock()?.set_metadata_authority(server_id)
    }

    pub fn set_rebalance_policy(
        &self,
        policy: RebalancePolicy,
    ) -> DatabaseResult<ClusterMetadataState> {
        self.lock()?.set_rebalance_policy(policy)
    }

    pub fn plan_rebalance(&self) -> DatabaseResult<RebalancePlan> {
        self.lock()?.plan_rebalance()
    }

    pub fn start_rebalance_plan(&self) -> DatabaseResult<RebalanceExecution> {
        self.lock()?.start_rebalance_plan()
    }

    pub fn cancel_rebalance_plan(&self) -> DatabaseResult<RebalanceExecution> {
        self.lock()?.cancel_rebalance_plan()
    }

    pub fn rebalance_status(&self) -> DatabaseResult<Option<RebalanceExecution>> {
        Ok(self.lock()?.rebalance_status().cloned())
    }

    pub fn advance_rebalance(&self) -> DatabaseResult<RebalanceAdvanceResult> {
        self.lock()?.advance_rebalance()
    }

    pub fn cluster_management_status(&self) -> DatabaseResult<ClusterManagementStatus> {
        Ok(self.lock()?.cluster_management_status())
    }

    pub fn apply_rebalance_step(&self, step: RebalanceStep) -> DatabaseResult<ShardRoutingTable> {
        self.lock()?.apply_rebalance_step(step)
    }

    pub fn prepare_rebalance_step(&self, step: RebalanceStep) -> DatabaseResult<ClusterMembership> {
        self.lock()?.prepare_rebalance_step(step)
    }

    pub fn mark_shard_caught_up(
        &self,
        shard_id: ShardId,
        server_id: ServerId,
        match_index: LogIndex,
    ) -> DatabaseResult<ClusterMembership> {
        self.lock()?
            .mark_shard_caught_up(shard_id, server_id, match_index)
    }

    pub fn committed_indexes(&self) -> DatabaseResult<Vec<LogIndex>> {
        Ok(self.lock()?.committed_indexes())
    }

    pub fn log_entries_from(
        &self,
        shard_id: ShardId,
        start_index: LogIndex,
    ) -> DatabaseResult<Vec<LogEntry>> {
        self.lock()?.log_entries_from(shard_id, start_index)
    }

    pub fn create_node_property_index(
        &self,
        name: impl Into<String>,
        label: impl Into<String>,
        property: impl Into<String>,
    ) -> DatabaseResult<()> {
        self.lock()?
            .create_node_property_index(name, label, property)
    }

    pub fn create_node_property_index_if_not_exists(
        &self,
        name: impl Into<String>,
        label: impl Into<String>,
        property: impl Into<String>,
    ) -> DatabaseResult<()> {
        self.lock()?
            .create_node_property_index_if_not_exists(name, label, property)
    }

    pub fn create_unique_node_property_constraint(
        &self,
        name: impl Into<String>,
        label: impl Into<String>,
        property: impl Into<String>,
    ) -> DatabaseResult<()> {
        self.lock()?
            .create_unique_node_property_constraint(name, label, property)
    }

    pub fn create_unique_node_property_constraint_if_not_exists(
        &self,
        name: impl Into<String>,
        label: impl Into<String>,
        property: impl Into<String>,
    ) -> DatabaseResult<()> {
        self.lock()?
            .create_unique_node_property_constraint_if_not_exists(name, label, property)
    }

    pub fn create_vector_index(
        &self,
        name: impl Into<String>,
        label: impl Into<String>,
        property: impl Into<String>,
        dimensions: usize,
        metric: impl Into<String>,
    ) -> DatabaseResult<()> {
        self.lock()?
            .create_vector_index(name, label, property, dimensions, metric)
    }

    pub fn create_vector_index_if_not_exists(
        &self,
        name: impl Into<String>,
        label: impl Into<String>,
        property: impl Into<String>,
        dimensions: usize,
        metric: impl Into<String>,
    ) -> DatabaseResult<()> {
        self.lock()?
            .create_vector_index_if_not_exists(name, label, property, dimensions, metric)
    }

    pub fn drop_index(&self, name: &str) -> DatabaseResult<()> {
        self.lock()?.drop_index(name)
    }

    pub fn drop_index_if_exists(&self, name: &str) -> DatabaseResult<()> {
        self.lock()?.drop_index_if_exists(name)
    }

    pub fn drop_constraint(&self, name: &str) -> DatabaseResult<()> {
        self.lock()?.drop_constraint(name)
    }

    pub fn drop_constraint_if_exists(&self, name: &str) -> DatabaseResult<()> {
        self.lock()?.drop_constraint_if_exists(name)
    }

    pub fn list_indexes(&self) -> DatabaseResult<Vec<IndexDefinition>> {
        Ok(self.lock()?.list_indexes())
    }

    pub fn show_indexes(&self) -> DatabaseResult<Vec<QueryRow>> {
        Ok(format_index_rows(&self.list_indexes()?))
    }

    pub fn show_index(&self, name: &str) -> DatabaseResult<Vec<QueryRow>> {
        Ok(vec![format_index_row_by_name(&self.list_indexes()?, name)?])
    }

    pub fn show_vector_indexes(&self) -> DatabaseResult<Vec<QueryRow>> {
        Ok(format_vector_index_rows(&self.list_indexes()?))
    }

    pub fn show_vector_index(&self, name: &str) -> DatabaseResult<Vec<QueryRow>> {
        Ok(vec![format_vector_index_row_by_name(
            &self.list_indexes()?,
            name,
        )?])
    }

    pub fn show_vector_index_status(&self) -> DatabaseResult<Vec<QueryRow>> {
        Ok(format_vector_index_status_rows(
            &self.vector_index_status()?,
        ))
    }

    pub fn show_vector_index_status_by_name(&self, name: &str) -> DatabaseResult<Vec<QueryRow>> {
        Ok(format_vector_index_status_rows(&[
            self.vector_index_status_by_name(name)?
        ]))
    }

    fn show_index_rows_for_query(&self, query: &str) -> DatabaseResult<Option<Vec<QueryRow>>> {
        if let Some(name) = show_vector_index_status_name(query)? {
            Ok(Some(self.show_vector_index_status_by_name(&name)?))
        } else if is_show_vector_index_status_cypher(query) {
            Ok(Some(self.show_vector_index_status()?))
        } else if let Some(name) = show_vector_index_name(query)? {
            Ok(Some(self.show_vector_index(&name)?))
        } else if let Some(name) = show_index_name(query)? {
            Ok(Some(self.show_index(&name)?))
        } else if is_show_vector_indexes_cypher(query) {
            Ok(Some(self.show_vector_indexes()?))
        } else if is_show_indexes_cypher(query) {
            Ok(Some(self.show_indexes()?))
        } else {
            Ok(None)
        }
    }

    pub fn show_constraints(&self) -> DatabaseResult<Vec<QueryRow>> {
        Ok(format_constraint_rows(&self.list_indexes()?))
    }

    pub fn show_constraint(&self, name: &str) -> DatabaseResult<Vec<QueryRow>> {
        Ok(vec![format_constraint_row_by_name(
            &self.list_indexes()?,
            name,
        )?])
    }

    fn show_constraint_rows_for_query(&self, query: &str) -> DatabaseResult<Option<Vec<QueryRow>>> {
        if let Some(name) = show_constraint_name(query)? {
            Ok(Some(self.show_constraint(&name)?))
        } else if is_show_constraints_cypher(query) {
            Ok(Some(self.show_constraints()?))
        } else {
            Ok(None)
        }
    }

    pub fn index_catalog(&self) -> DatabaseResult<IndexCatalog> {
        Ok(self.lock()?.index_catalog())
    }

    pub fn install_index_catalog(&self, catalog: IndexCatalog) -> DatabaseResult<()> {
        self.lock()?.install_index_catalog(catalog)
    }

    pub fn rebuild_vector_indexes(&self) -> DatabaseResult<()> {
        self.lock()?.rebuild_vector_indexes()
    }

    pub fn rebuild_vector_index(&self, name: &str) -> DatabaseResult<()> {
        self.lock()?.rebuild_vector_index(name)
    }

    pub fn vector_index_status(&self) -> DatabaseResult<Vec<VectorIndexStatus>> {
        self.lock()?.vector_index_status()
    }

    pub fn vector_index_status_by_name(&self, name: &str) -> DatabaseResult<VectorIndexStatus> {
        self.lock()?.vector_index_status_by_name(name)
    }

    fn lock(&self) -> DatabaseResult<MutexGuard<'_, Neo4rDatabase>> {
        self.inner.lock().map_err(|_| DatabaseError::LockPoisoned)
    }

    fn send_write(&self, operation: WriteOperation) -> DatabaseResult<WriteResponse> {
        let (response_tx, response_rx) = mpsc::channel();
        self.writer.send(WriteRequest {
            operation,
            response: response_tx,
        })?;
        response_rx
            .recv()
            .map_err(|_| DatabaseError::WriterUnavailable)?
    }

    fn match_node_ids(
        &self,
        matcher: &NodeMatcher,
        params: &QueryParams,
    ) -> DatabaseResult<Vec<NodeId>> {
        query_match_node_ids(
            |query| self.query_with_params(query, params.clone()),
            matcher,
        )
    }

    fn match_relationship_ids(
        &self,
        matcher: &RelationshipMatcher,
        params: &QueryParams,
    ) -> DatabaseResult<Vec<RelationshipId>> {
        query_match_relationship_ids(
            |query| self.query_with_params(query, params.clone()),
            matcher,
        )
    }
}

impl Neo4rReadSnapshot {
    pub fn timestamp(&self) -> HybridTimestamp {
        self.timestamp
    }

    pub fn applied_indexes(&self) -> &[LogIndex] {
        &self.applied_indexes
    }

    pub fn committed_indexes(&self) -> &[LogIndex] {
        &self.committed_indexes
    }

    pub fn query(&self, query: &str) -> DatabaseResult<Vec<QueryRow>> {
        Ok(self.query_engine.execute(&self.store, query)?)
    }

    pub fn node(&self, id: NodeId) -> DatabaseResult<Option<Node>> {
        self.store.node(id).map_err(DatabaseError::from)
    }

    pub fn relationship(&self, id: RelationshipId) -> DatabaseResult<Option<Relationship>> {
        self.store.relationship(id).map_err(DatabaseError::from)
    }

    pub fn query_with_params(
        &self,
        query: &str,
        params: &QueryParams,
    ) -> DatabaseResult<Vec<QueryRow>> {
        Ok(self
            .query_engine
            .execute_with_params(&self.store, query, params)?)
    }

    pub fn query_shard(&self, shard_id: ShardId, query: &str) -> DatabaseResult<Vec<QueryRow>> {
        self.query_shard_with_params(shard_id, query, &QueryParams::new())
    }

    pub fn query_shard_with_params(
        &self,
        shard_id: ShardId,
        query: &str,
        params: &QueryParams,
    ) -> DatabaseResult<Vec<QueryRow>> {
        let graph = ShardScopedGraphRead::new(&self.store, self.shard_map, shard_id)?;
        Ok(self
            .query_engine
            .execute_with_params(&graph, query, params)?)
    }

    fn query_shard_with_staged_writes(
        &self,
        shard_id: ShardId,
        query: &str,
        params: &QueryParams,
        staged_writes: &[(String, QueryParams)],
    ) -> DatabaseResult<Vec<QueryRow>> {
        let overlay = self.build_staged_overlay(staged_writes)?;
        let overlay_graph = StagedOverlayGraph {
            base: &self.store,
            node_overlay: &overlay.nodes,
            relationship_overlay: &overlay.relationships,
        };
        let graph = ShardScopedGraphRead::new(&overlay_graph, self.shard_map, shard_id)?;
        Ok(self
            .query_engine
            .execute_with_params(&graph, query, params)?)
    }

    pub fn query_cursor(&self, query: &str) -> DatabaseResult<Box<dyn QueryCursor>> {
        Ok(self.query_engine.execute_cursor(&self.store, query)?)
    }

    pub fn query_cursor_with_params(
        &self,
        query: &str,
        params: &QueryParams,
    ) -> DatabaseResult<Box<dyn QueryCursor>> {
        Ok(self
            .query_engine
            .execute_cursor_with_params(&self.store, query, params)?)
    }

    pub fn into_query_cursor(self, query: &str) -> DatabaseResult<Box<dyn QueryCursor>> {
        self.into_query_cursor_with_params(query, QueryParams::new())
    }

    pub fn into_query_cursor_with_params(
        self,
        query: &str,
        params: QueryParams,
    ) -> DatabaseResult<Box<dyn QueryCursor>> {
        let engine = self.query_engine;
        Ok(engine.execute_owned_cursor_with_params(Arc::new(self.store), query, params)?)
    }

    fn build_staged_overlay(
        &self,
        staged_writes: &[(String, QueryParams)],
    ) -> DatabaseResult<StagedOverlay> {
        let mut node_overlay = HashMap::new();
        let mut relationship_overlay = HashMap::new();
        let mut temp_node_ids = BTreeSet::new();
        let mut temp_relationship_ids = BTreeSet::new();
        let mut next_temp_node_id = STAGED_TEMP_NODE_ID_START;
        let mut next_temp_relationship_id = STAGED_TEMP_RELATIONSHIP_ID_START;
        for (query, params) in staged_writes {
            match parse_write_cypher(query, params)? {
                Some(WriteCypher::CreateNode {
                    labels,
                    properties,
                    assignments,
                    replacement,
                    ..
                }) => {
                    let properties =
                        create_properties_after_set(properties, assignments, replacement);
                    let id = allocate_staged_id(&mut next_temp_node_id)?;
                    temp_node_ids.insert(id);
                    node_overlay.insert(id, Some(Node::new(id, labels, properties)));
                }
                Some(WriteCypher::CreateRelationship {
                    from_matcher,
                    to_matcher,
                    rel_type,
                    properties,
                    assignments,
                    replacement,
                    ..
                }) => {
                    let properties =
                        create_properties_after_set(properties, assignments, replacement);
                    let relationships = {
                        let graph = StagedOverlayGraph {
                            base: &self.store,
                            node_overlay: &node_overlay,
                            relationship_overlay: &relationship_overlay,
                        };
                        let from_ids = query_match_node_ids(
                            |match_query| {
                                Ok(self.query_engine.execute_with_params(
                                    &graph,
                                    match_query,
                                    params,
                                )?)
                            },
                            &from_matcher,
                        )?;
                        let to_ids = query_match_node_ids(
                            |match_query| {
                                Ok(self.query_engine.execute_with_params(
                                    &graph,
                                    match_query,
                                    params,
                                )?)
                            },
                            &to_matcher,
                        )?;
                        let mut relationships = Vec::new();
                        for from in &from_ids {
                            for to in &to_ids {
                                let id = allocate_staged_id(&mut next_temp_relationship_id)?;
                                temp_relationship_ids.insert(id);
                                relationships.push(Relationship::new(
                                    id,
                                    *from,
                                    *to,
                                    rel_type.clone(),
                                    properties.clone(),
                                ));
                            }
                        }
                        relationships
                    };
                    for relationship in relationships {
                        relationship_overlay.insert(relationship.id, Some(relationship));
                    }
                }
                Some(WriteCypher::MergeNode {
                    labels,
                    properties,
                    on_create,
                    on_create_replacement,
                    on_match,
                    on_match_replacement,
                    ..
                }) => {
                    let mut node = {
                        let graph = StagedOverlayGraph {
                            base: &self.store,
                            node_overlay: &node_overlay,
                            relationship_overlay: &relationship_overlay,
                        };
                        find_merge_node_in_graph(&graph, &labels, &properties)?
                    };
                    match node.as_mut() {
                        Some(node) => {
                            node.properties = properties_after_set(
                                node.properties.clone(),
                                &on_match,
                                on_match_replacement.as_ref(),
                            );
                            node_overlay.insert(node.id, Some(node.clone()));
                        }
                        None => {
                            let id = allocate_staged_id(&mut next_temp_node_id)?;
                            temp_node_ids.insert(id);
                            let create_properties = properties_after_set(
                                properties,
                                &on_create,
                                on_create_replacement.as_ref(),
                            );
                            node_overlay.insert(id, Some(Node::new(id, labels, create_properties)));
                        }
                    }
                }
                Some(WriteCypher::MergeRelationship {
                    from_matcher,
                    to_matcher,
                    rel_type,
                    properties,
                    on_create,
                    on_create_replacement,
                    on_match,
                    on_match_replacement,
                    ..
                }) => {
                    let merged_relationships = {
                        let graph = StagedOverlayGraph {
                            base: &self.store,
                            node_overlay: &node_overlay,
                            relationship_overlay: &relationship_overlay,
                        };
                        let from_ids = query_match_node_ids(
                            |match_query| {
                                Ok(self.query_engine.execute_with_params(
                                    &graph,
                                    match_query,
                                    params,
                                )?)
                            },
                            &from_matcher,
                        )?;
                        let to_ids = query_match_node_ids(
                            |match_query| {
                                Ok(self.query_engine.execute_with_params(
                                    &graph,
                                    match_query,
                                    params,
                                )?)
                            },
                            &to_matcher,
                        )?;
                        let mut relationships = Vec::new();
                        for from in &from_ids {
                            for to in &to_ids {
                                match find_merge_relationship_in_graph(
                                    &graph,
                                    *from,
                                    *to,
                                    &rel_type,
                                    &properties,
                                )? {
                                    Some(mut relationship) => {
                                        relationship.properties = properties_after_set(
                                            relationship.properties.clone(),
                                            &on_match,
                                            on_match_replacement.as_ref(),
                                        );
                                        relationships.push((false, relationship));
                                    }
                                    None => {
                                        let id =
                                            allocate_staged_id(&mut next_temp_relationship_id)?;
                                        let create_properties = properties_after_set(
                                            properties.clone(),
                                            &on_create,
                                            on_create_replacement.as_ref(),
                                        );
                                        relationships.push((
                                            true,
                                            Relationship::new(
                                                id,
                                                *from,
                                                *to,
                                                rel_type.clone(),
                                                create_properties,
                                            ),
                                        ));
                                    }
                                }
                            }
                        }
                        relationships
                    };
                    for (created, relationship) in merged_relationships {
                        if created {
                            temp_relationship_ids.insert(relationship.id);
                        }
                        relationship_overlay.insert(relationship.id, Some(relationship));
                    }
                }
                Some(WriteCypher::SetNodeProperty {
                    matcher,
                    assignments,
                    ..
                }) => {
                    let updated_nodes = {
                        let graph = StagedOverlayGraph {
                            base: &self.store,
                            node_overlay: &node_overlay,
                            relationship_overlay: &relationship_overlay,
                        };
                        let node_ids = query_match_node_ids(
                            |match_query| {
                                Ok(self.query_engine.execute_with_params(
                                    &graph,
                                    match_query,
                                    params,
                                )?)
                            },
                            &matcher,
                        )?;
                        let mut updated_nodes = Vec::new();
                        for id in node_ids {
                            let mut node = graph.node(id)?.ok_or(GraphError::NodeNotFound(id))?;
                            apply_assignments_to_properties(&mut node.properties, &assignments);
                            updated_nodes.push((id, node));
                        }
                        updated_nodes
                    };
                    for (id, node) in updated_nodes {
                        node_overlay.insert(id, Some(node));
                    }
                }
                Some(WriteCypher::ReplaceNodeProperties {
                    matcher,
                    properties,
                    ..
                }) => {
                    let updated_nodes = {
                        let graph = StagedOverlayGraph {
                            base: &self.store,
                            node_overlay: &node_overlay,
                            relationship_overlay: &relationship_overlay,
                        };
                        let node_ids = query_match_node_ids(
                            |match_query| {
                                Ok(self.query_engine.execute_with_params(
                                    &graph,
                                    match_query,
                                    params,
                                )?)
                            },
                            &matcher,
                        )?;
                        let mut updated_nodes = Vec::new();
                        for id in node_ids {
                            let mut node = graph.node(id)?.ok_or(GraphError::NodeNotFound(id))?;
                            node.properties = properties.clone();
                            updated_nodes.push((id, node));
                        }
                        updated_nodes
                    };
                    for (id, node) in updated_nodes {
                        node_overlay.insert(id, Some(node));
                    }
                }
                Some(WriteCypher::RemoveNodeProperty { matcher, keys, .. }) => {
                    let updated_nodes = {
                        let graph = StagedOverlayGraph {
                            base: &self.store,
                            node_overlay: &node_overlay,
                            relationship_overlay: &relationship_overlay,
                        };
                        let node_ids = query_match_node_ids(
                            |match_query| {
                                Ok(self.query_engine.execute_with_params(
                                    &graph,
                                    match_query,
                                    params,
                                )?)
                            },
                            &matcher,
                        )?;
                        let mut updated_nodes = Vec::new();
                        for id in node_ids {
                            let mut node = graph.node(id)?.ok_or(GraphError::NodeNotFound(id))?;
                            for key in &keys {
                                node.properties.remove(key);
                            }
                            updated_nodes.push((id, node));
                        }
                        updated_nodes
                    };
                    for (id, node) in updated_nodes {
                        node_overlay.insert(id, Some(node));
                    }
                }
                Some(WriteCypher::AddNodeLabel {
                    matcher, labels, ..
                }) => {
                    let updated_nodes = {
                        let graph = StagedOverlayGraph {
                            base: &self.store,
                            node_overlay: &node_overlay,
                            relationship_overlay: &relationship_overlay,
                        };
                        let node_ids = query_match_node_ids(
                            |match_query| {
                                Ok(self.query_engine.execute_with_params(
                                    &graph,
                                    match_query,
                                    params,
                                )?)
                            },
                            &matcher,
                        )?;
                        let mut updated_nodes = Vec::new();
                        for id in node_ids {
                            let mut node = graph.node(id)?.ok_or(GraphError::NodeNotFound(id))?;
                            for label in &labels {
                                if !node.labels.iter().any(|existing| existing == label) {
                                    node.labels.push(label.clone());
                                }
                            }
                            updated_nodes.push((id, node));
                        }
                        updated_nodes
                    };
                    for (id, node) in updated_nodes {
                        node_overlay.insert(id, Some(node));
                    }
                }
                Some(WriteCypher::RemoveNodeLabel {
                    matcher, labels, ..
                }) => {
                    let updated_nodes = {
                        let graph = StagedOverlayGraph {
                            base: &self.store,
                            node_overlay: &node_overlay,
                            relationship_overlay: &relationship_overlay,
                        };
                        let node_ids = query_match_node_ids(
                            |match_query| {
                                Ok(self.query_engine.execute_with_params(
                                    &graph,
                                    match_query,
                                    params,
                                )?)
                            },
                            &matcher,
                        )?;
                        let mut updated_nodes = Vec::new();
                        for id in node_ids {
                            let mut node = graph.node(id)?.ok_or(GraphError::NodeNotFound(id))?;
                            node.labels
                                .retain(|existing| !labels.iter().any(|label| label == existing));
                            updated_nodes.push((id, node));
                        }
                        updated_nodes
                    };
                    for (id, node) in updated_nodes {
                        node_overlay.insert(id, Some(node));
                    }
                }
                Some(WriteCypher::SetRelationshipProperty {
                    matcher,
                    assignments,
                    ..
                }) => {
                    let updated_relationships = {
                        let graph = StagedOverlayGraph {
                            base: &self.store,
                            node_overlay: &node_overlay,
                            relationship_overlay: &relationship_overlay,
                        };
                        let relationship_ids = query_match_relationship_ids(
                            |match_query| {
                                Ok(self.query_engine.execute_with_params(
                                    &graph,
                                    match_query,
                                    params,
                                )?)
                            },
                            &matcher,
                        )?;
                        let mut updated_relationships = Vec::new();
                        for id in relationship_ids {
                            let mut relationship = graph
                                .relationship(id)?
                                .ok_or(GraphError::RelationshipNotFound(id))?;
                            apply_assignments_to_properties(
                                &mut relationship.properties,
                                &assignments,
                            );
                            updated_relationships.push((id, relationship));
                        }
                        updated_relationships
                    };
                    for (id, relationship) in updated_relationships {
                        relationship_overlay.insert(id, Some(relationship));
                    }
                }
                Some(WriteCypher::ReplaceRelationshipProperties {
                    matcher,
                    properties,
                    ..
                }) => {
                    let updated_relationships = {
                        let graph = StagedOverlayGraph {
                            base: &self.store,
                            node_overlay: &node_overlay,
                            relationship_overlay: &relationship_overlay,
                        };
                        let relationship_ids = query_match_relationship_ids(
                            |match_query| {
                                Ok(self.query_engine.execute_with_params(
                                    &graph,
                                    match_query,
                                    params,
                                )?)
                            },
                            &matcher,
                        )?;
                        let mut updated_relationships = Vec::new();
                        for id in relationship_ids {
                            let mut relationship = graph
                                .relationship(id)?
                                .ok_or(GraphError::RelationshipNotFound(id))?;
                            relationship.properties = properties.clone();
                            updated_relationships.push((id, relationship));
                        }
                        updated_relationships
                    };
                    for (id, relationship) in updated_relationships {
                        relationship_overlay.insert(id, Some(relationship));
                    }
                }
                Some(WriteCypher::RemoveRelationshipProperty { matcher, keys, .. }) => {
                    let updated_relationships = {
                        let graph = StagedOverlayGraph {
                            base: &self.store,
                            node_overlay: &node_overlay,
                            relationship_overlay: &relationship_overlay,
                        };
                        let relationship_ids = query_match_relationship_ids(
                            |match_query| {
                                Ok(self.query_engine.execute_with_params(
                                    &graph,
                                    match_query,
                                    params,
                                )?)
                            },
                            &matcher,
                        )?;
                        let mut updated_relationships = Vec::new();
                        for id in relationship_ids {
                            let mut relationship = graph
                                .relationship(id)?
                                .ok_or(GraphError::RelationshipNotFound(id))?;
                            for key in &keys {
                                relationship.properties.remove(key);
                            }
                            updated_relationships.push((id, relationship));
                        }
                        updated_relationships
                    };
                    for (id, relationship) in updated_relationships {
                        relationship_overlay.insert(id, Some(relationship));
                    }
                }
                Some(WriteCypher::DeleteRelationship { matcher, .. }) => {
                    let relationship_ids = {
                        let graph = StagedOverlayGraph {
                            base: &self.store,
                            node_overlay: &node_overlay,
                            relationship_overlay: &relationship_overlay,
                        };
                        query_match_relationship_ids(
                            |match_query| {
                                Ok(self.query_engine.execute_with_params(
                                    &graph,
                                    match_query,
                                    params,
                                )?)
                            },
                            &matcher,
                        )?
                    };
                    for id in relationship_ids {
                        relationship_overlay.insert(id, None);
                    }
                }
                Some(WriteCypher::DeleteNode { matcher, .. }) => {
                    let (node_ids, relationship_ids) = {
                        let graph = StagedOverlayGraph {
                            base: &self.store,
                            node_overlay: &node_overlay,
                            relationship_overlay: &relationship_overlay,
                        };
                        let node_ids = query_match_node_ids(
                            |match_query| {
                                Ok(self.query_engine.execute_with_params(
                                    &graph,
                                    match_query,
                                    params,
                                )?)
                            },
                            &matcher,
                        )?;
                        let mut relationship_ids = BTreeSet::new();
                        for id in &node_ids {
                            for relationship in graph.outgoing(*id)? {
                                relationship_ids.insert(relationship.id);
                            }
                            for relationship in graph.incoming(*id)? {
                                relationship_ids.insert(relationship.id);
                            }
                        }
                        (node_ids, relationship_ids)
                    };
                    for id in node_ids {
                        node_overlay.insert(id, None);
                    }
                    for id in relationship_ids {
                        relationship_overlay.insert(id, None);
                    }
                }
                Some(_) => {
                    return Err(DatabaseError::InvalidConfig(
                        "read-your-writes overlay currently supports staged node and relationship CREATE, MERGE, SET, REMOVE, and DELETE only"
                            .to_string(),
                    ));
                }
                None => {
                    return Err(DatabaseError::InvalidConfig(
                        "read-your-writes overlay requires staged write queries".to_string(),
                    ));
                }
            }
        }
        Ok(StagedOverlay {
            nodes: node_overlay,
            relationships: relationship_overlay,
            temp_node_ids,
            temp_relationship_ids,
        })
    }
}

struct StagedOverlay {
    nodes: HashMap<NodeId, Option<Node>>,
    relationships: HashMap<RelationshipId, Option<Relationship>>,
    temp_node_ids: BTreeSet<NodeId>,
    temp_relationship_ids: BTreeSet<RelationshipId>,
}

const STAGED_TEMP_NODE_ID_START: NodeId = NodeId::MAX;
const STAGED_TEMP_RELATIONSHIP_ID_START: RelationshipId = RelationshipId::MAX;

fn allocate_staged_id(next_id: &mut u64) -> DatabaseResult<u64> {
    let id = *next_id;
    *next_id = next_id.checked_sub(1).ok_or_else(|| {
        DatabaseError::InvalidConfig("staged transaction id space exhausted".to_string())
    })?;
    Ok(id)
}

struct StagedOverlayGraph<'a> {
    base: &'a PartitionedGraphStore<RocksKvSnapshot>,
    node_overlay: &'a HashMap<NodeId, Option<Node>>,
    relationship_overlay: &'a HashMap<RelationshipId, Option<Relationship>>,
}

impl GraphRead for StagedOverlayGraph<'_> {
    fn node(&self, id: NodeId) -> GraphReadResult<Option<Node>> {
        if let Some(node) = self.node_overlay.get(&id) {
            return Ok(node.clone());
        }
        self.base.node(id)
    }

    fn boundary_node(&self, id: NodeId) -> GraphReadResult<Option<BoundaryNode>> {
        self.base.boundary_node(id)
    }

    fn nodes(&self) -> GraphReadResult<Vec<Node>> {
        let mut nodes = self
            .base
            .nodes()?
            .into_iter()
            .filter(|node| !self.node_overlay.contains_key(&node.id))
            .collect::<Vec<_>>();
        nodes.extend(
            self.node_overlay
                .values()
                .filter_map(|node| node.as_ref().cloned()),
        );
        nodes.sort_by_key(|node| node.id);
        Ok(nodes)
    }

    fn node_ids(&self) -> GraphReadResult<Vec<NodeId>> {
        Ok(self.nodes()?.into_iter().map(|node| node.id).collect())
    }

    fn relationship(&self, id: RelationshipId) -> GraphReadResult<Option<Relationship>> {
        if let Some(relationship) = self.relationship_overlay.get(&id) {
            let Some(relationship) = relationship.clone() else {
                return Ok(None);
            };
            if self.relationship_has_hidden_endpoint(&relationship) {
                return Ok(None);
            }
            return Ok(Some(relationship));
        }
        let Some(relationship) = self.base.relationship(id)? else {
            return Ok(None);
        };
        if self.relationship_has_hidden_endpoint(&relationship) {
            return Ok(None);
        }
        Ok(Some(relationship))
    }

    fn node_ids_by_label(&self, label: &str) -> GraphReadResult<Vec<NodeId>> {
        let mut ids = self
            .base
            .node_ids_by_label(label)?
            .into_iter()
            .filter(|id| !self.node_overlay.contains_key(id))
            .collect::<BTreeSet<_>>();
        for node in self.node_overlay.values().filter_map(|node| node.as_ref()) {
            if node.labels.iter().any(|candidate| candidate == label) {
                ids.insert(node.id);
            }
        }
        Ok(ids.into_iter().collect())
    }

    fn node_ids_by_label_property(
        &self,
        label: &str,
        property_key: &str,
        property_value: &Value,
    ) -> GraphReadResult<Vec<NodeId>> {
        let mut ids = self
            .base
            .node_ids_by_label_property(label, property_key, property_value)?
            .into_iter()
            .filter(|id| !self.node_overlay.contains_key(id))
            .collect::<BTreeSet<_>>();
        for node in self.node_overlay.values().filter_map(|node| node.as_ref()) {
            let has_label = node.labels.iter().any(|candidate| candidate == label);
            if has_label && node.properties.get(property_key) == Some(property_value) {
                ids.insert(node.id);
            }
        }
        Ok(ids.into_iter().collect())
    }

    fn boundary_node_ids_by_label_property(
        &self,
        label: &str,
        property_key: &str,
        property_value: &Value,
    ) -> GraphReadResult<Vec<NodeId>> {
        self.base
            .boundary_node_ids_by_label_property(label, property_key, property_value)
    }

    fn outgoing(&self, node_id: NodeId) -> GraphReadResult<Vec<Relationship>> {
        self.ensure_overlay_node_visible(node_id)?;
        self.overlay_relationships(self.base.outgoing(node_id)?, |relationship| {
            relationship.from == node_id
        })
    }

    fn outgoing_by_type(
        &self,
        node_id: NodeId,
        rel_type: &str,
    ) -> GraphReadResult<Vec<Relationship>> {
        self.ensure_overlay_node_visible(node_id)?;
        self.overlay_relationships(
            self.base.outgoing_by_type(node_id, rel_type)?,
            |relationship| relationship.from == node_id && relationship.rel_type == rel_type,
        )
    }

    fn incoming(&self, node_id: NodeId) -> GraphReadResult<Vec<Relationship>> {
        self.ensure_overlay_node_visible(node_id)?;
        self.overlay_relationships(self.base.incoming(node_id)?, |relationship| {
            relationship.to == node_id
        })
    }

    fn incoming_by_type(
        &self,
        node_id: NodeId,
        rel_type: &str,
    ) -> GraphReadResult<Vec<Relationship>> {
        self.ensure_overlay_node_visible(node_id)?;
        self.overlay_relationships(
            self.base.incoming_by_type(node_id, rel_type)?,
            |relationship| relationship.to == node_id && relationship.rel_type == rel_type,
        )
    }
}

impl StagedOverlayGraph<'_> {
    fn ensure_overlay_node_visible(&self, node_id: NodeId) -> GraphReadResult<()> {
        if matches!(self.node_overlay.get(&node_id), Some(None)) {
            Err(GraphReadError::Store(format!(
                "node {node_id} is hidden by staged transaction overlay"
            )))
        } else {
            Ok(())
        }
    }

    fn overlay_relationships(
        &self,
        relationships: Vec<Relationship>,
        include_overlay: impl Fn(&Relationship) -> bool,
    ) -> GraphReadResult<Vec<Relationship>> {
        let mut merged = relationships
            .into_iter()
            .filter_map(
                |relationship| match self.relationship_overlay.get(&relationship.id) {
                    Some(Some(relationship)) => Some(relationship.clone()),
                    Some(None) => None,
                    None if self.relationship_has_hidden_endpoint(&relationship) => None,
                    None => Some(relationship),
                },
            )
            .collect::<Vec<_>>();
        let existing_ids = merged
            .iter()
            .map(|relationship| relationship.id)
            .collect::<BTreeSet<_>>();
        merged.extend(
            self.relationship_overlay
                .iter()
                .filter(|(id, _)| !existing_ids.contains(id))
                .filter_map(|(_, relationship)| relationship.as_ref())
                .filter(|relationship| {
                    include_overlay(relationship)
                        && !self.relationship_has_hidden_endpoint(relationship)
                })
                .cloned(),
        );
        merged.sort_by_key(|relationship| relationship.id);
        Ok(merged)
    }

    fn relationship_has_hidden_endpoint(&self, relationship: &Relationship) -> bool {
        matches!(self.node_overlay.get(&relationship.from), Some(None))
            || matches!(self.node_overlay.get(&relationship.to), Some(None))
    }
}

struct ShardScopedGraphRead<'a, G: ?Sized> {
    graph: &'a G,
    shard_map: ShardMap,
    shard_id: ShardId,
}

impl<'a, G: ?Sized> ShardScopedGraphRead<'a, G> {
    fn new(graph: &'a G, shard_map: ShardMap, shard_id: ShardId) -> DatabaseResult<Self> {
        if shard_id >= shard_map.shard_count() {
            return Err(DatabaseError::MissingShardLog(shard_id));
        }
        Ok(Self {
            graph,
            shard_map,
            shard_id,
        })
    }

    fn owns_node(&self, id: NodeId) -> bool {
        self.shard_map.owner_of_node(id) == self.shard_id
    }

    fn owns_relationship(&self, relationship: &Relationship) -> bool {
        self.shard_map.owner_of_relationship(
            relationship.from,
            relationship.to,
            &relationship.rel_type,
        ) == self.shard_id
    }
}

impl<G: GraphRead + ?Sized> GraphRead for ShardScopedGraphRead<'_, G> {
    fn node(&self, id: NodeId) -> GraphReadResult<Option<Node>> {
        if self.owns_node(id) {
            self.graph.node(id)
        } else {
            Ok(None)
        }
    }

    fn boundary_node(&self, id: NodeId) -> GraphReadResult<Option<BoundaryNode>> {
        self.graph.boundary_node(id)
    }

    fn nodes(&self) -> GraphReadResult<Vec<Node>> {
        Ok(self
            .graph
            .nodes()?
            .into_iter()
            .filter(|node| self.owns_node(node.id))
            .collect())
    }

    fn node_ids(&self) -> GraphReadResult<Vec<NodeId>> {
        Ok(self
            .graph
            .node_ids()?
            .into_iter()
            .filter(|id| self.owns_node(*id))
            .collect())
    }

    fn relationship(&self, id: RelationshipId) -> GraphReadResult<Option<Relationship>> {
        Ok(self
            .graph
            .relationship(id)?
            .filter(|relationship| self.owns_relationship(relationship)))
    }

    fn node_ids_by_label(&self, label: &str) -> GraphReadResult<Vec<NodeId>> {
        Ok(self
            .graph
            .node_ids_by_label(label)?
            .into_iter()
            .filter(|id| self.owns_node(*id))
            .collect())
    }

    fn node_ids_by_label_property(
        &self,
        label: &str,
        property_key: &str,
        property_value: &Value,
    ) -> GraphReadResult<Vec<NodeId>> {
        Ok(self
            .graph
            .node_ids_by_label_property(label, property_key, property_value)?
            .into_iter()
            .filter(|id| self.owns_node(*id))
            .collect())
    }

    fn boundary_node_ids_by_label_property(
        &self,
        label: &str,
        property_key: &str,
        property_value: &Value,
    ) -> GraphReadResult<Vec<NodeId>> {
        self.graph
            .boundary_node_ids_by_label_property(label, property_key, property_value)
    }

    fn outgoing(&self, node_id: NodeId) -> GraphReadResult<Vec<Relationship>> {
        if !self.owns_node(node_id) {
            return Ok(Vec::new());
        }
        Ok(self
            .graph
            .outgoing(node_id)?
            .into_iter()
            .filter(|relationship| self.owns_relationship(relationship))
            .collect())
    }

    fn outgoing_by_type(
        &self,
        node_id: NodeId,
        rel_type: &str,
    ) -> GraphReadResult<Vec<Relationship>> {
        if !self.owns_node(node_id) {
            return Ok(Vec::new());
        }
        Ok(self
            .graph
            .outgoing_by_type(node_id, rel_type)?
            .into_iter()
            .filter(|relationship| self.owns_relationship(relationship))
            .collect())
    }

    fn incoming(&self, node_id: NodeId) -> GraphReadResult<Vec<Relationship>> {
        Ok(self
            .graph
            .incoming(node_id)?
            .into_iter()
            .filter(|relationship| self.owns_relationship(relationship))
            .collect())
    }

    fn incoming_by_type(
        &self,
        node_id: NodeId,
        rel_type: &str,
    ) -> GraphReadResult<Vec<Relationship>> {
        Ok(self
            .graph
            .incoming_by_type(node_id, rel_type)?
            .into_iter()
            .filter(|relationship| self.owns_relationship(relationship))
            .collect())
    }
}

impl Neo4rReadTransaction {
    pub fn timestamp(&self) -> HybridTimestamp {
        self.snapshot.timestamp()
    }

    pub fn applied_indexes(&self) -> &[LogIndex] {
        self.snapshot.applied_indexes()
    }

    pub fn committed_indexes(&self) -> &[LogIndex] {
        self.snapshot.committed_indexes()
    }

    pub fn options(&self) -> QueryOptions {
        self.options
    }

    pub fn query(&self, query: &str) -> DatabaseResult<Vec<QueryRow>> {
        self.snapshot.query(query)
    }

    pub fn query_with_params(
        &self,
        query: &str,
        params: &QueryParams,
    ) -> DatabaseResult<Vec<QueryRow>> {
        self.snapshot.query_with_params(query, params)
    }

    pub fn query_shard_with_params(
        &self,
        shard_id: ShardId,
        query: &str,
        params: &QueryParams,
    ) -> DatabaseResult<Vec<QueryRow>> {
        self.snapshot
            .query_shard_with_params(shard_id, query, params)
    }

    pub fn query_cursor(&self, query: &str) -> DatabaseResult<Box<dyn QueryCursor>> {
        self.snapshot.query_cursor(query)
    }

    pub fn query_cursor_with_params(
        &self,
        query: &str,
        params: &QueryParams,
    ) -> DatabaseResult<Box<dyn QueryCursor>> {
        self.snapshot.query_cursor_with_params(query, params)
    }
}

struct WriteRequest {
    operation: WriteOperation,
    response: mpsc::Sender<DatabaseResult<WriteResponse>>,
}

struct WriterActor {
    sender: Mutex<Option<mpsc::Sender<WriteRequest>>>,
    join: Mutex<Option<thread::JoinHandle<()>>>,
}

impl WriterActor {
    fn send(&self, request: WriteRequest) -> DatabaseResult<()> {
        let sender = self
            .sender
            .lock()
            .map_err(|_| DatabaseError::LockPoisoned)?
            .as_ref()
            .cloned()
            .ok_or(DatabaseError::WriterUnavailable)?;
        sender
            .send(request)
            .map_err(|_| DatabaseError::WriterUnavailable)
    }
}

impl Drop for WriterActor {
    fn drop(&mut self) {
        if let Ok(mut sender) = self.sender.lock() {
            sender.take();
        }
        if let Ok(mut join) = self.join.lock() {
            if let Some(join) = join.take() {
                let _ = join.join();
            }
        }
    }
}

enum WriteOperation {
    CreateNode {
        labels: Vec<String>,
        properties: Properties,
    },
    CreateNodeOnShard {
        shard_id: ShardId,
        labels: Vec<String>,
        properties: Properties,
    },
    CreateRelationship {
        from: NodeId,
        to: NodeId,
        rel_type: String,
        properties: Properties,
    },
    SetNodeProperty {
        id: NodeId,
        key: String,
        value: Value,
    },
    RemoveNodeProperty {
        id: NodeId,
        key: String,
    },
    AddNodeLabel {
        id: NodeId,
        label: String,
    },
    RemoveNodeLabel {
        id: NodeId,
        label: String,
    },
    SetRelationshipProperty {
        id: RelationshipId,
        key: String,
        value: Value,
    },
    RemoveRelationshipProperty {
        id: RelationshipId,
        key: String,
    },
    DeleteRelationship {
        id: RelationshipId,
    },
    DeleteNode {
        id: NodeId,
    },
    ApplyReplicatedEntry(LogEntry),
    ApplyReplicatedEntries(Vec<LogEntry>),
}

#[derive(Debug)]
enum WriteResponse {
    NodeId(NodeId),
    RelationshipId(RelationshipId),
    Unit,
}

fn spawn_writer_actor(inner: Arc<Mutex<Neo4rDatabase>>) -> Arc<WriterActor> {
    let (tx, rx) = mpsc::channel::<WriteRequest>();
    let join = thread::spawn(move || {
        while let Ok(first) = rx.recv() {
            let (max_entries, max_delay) = match group_commit_config(&inner) {
                Ok(config) => config,
                Err(err) => {
                    let _ = first.response.send(Err(err));
                    continue;
                }
            };
            let mut batch = vec![first];
            while batch.len() < max_entries {
                match rx.recv_timeout(max_delay) {
                    Ok(request) => batch.push(request),
                    Err(mpsc::RecvTimeoutError::Timeout) => break,
                    Err(mpsc::RecvTimeoutError::Disconnected) => break,
                }
            }
            execute_write_batch(&inner, batch);
        }
    });
    Arc::new(WriterActor {
        sender: Mutex::new(Some(tx)),
        join: Mutex::new(Some(join)),
    })
}

fn group_commit_config(inner: &Arc<Mutex<Neo4rDatabase>>) -> DatabaseResult<(usize, Duration)> {
    let database = inner.lock().map_err(|_| DatabaseError::LockPoisoned)?;
    Ok((
        database.config.group_commit_max_entries.max(1),
        database.config.group_commit_max_delay,
    ))
}

fn execute_write_batch(inner: &Arc<Mutex<Neo4rDatabase>>, batch: Vec<WriteRequest>) {
    let mut database = match inner.lock().map_err(|_| DatabaseError::LockPoisoned) {
        Ok(database) => database,
        Err(err) => {
            for request in batch {
                let _ = request.response.send(Err(error_for_batch_response(&err)));
            }
            return;
        }
    };
    let mut prepared = Vec::new();
    for request in batch {
        match request.operation {
            WriteOperation::ApplyReplicatedEntry(entry) => {
                let result = database
                    .apply_replicated_entry(entry)
                    .map(|_| WriteResponse::Unit);
                let _ = request.response.send(result);
            }
            WriteOperation::ApplyReplicatedEntries(entries) => {
                let result = database
                    .apply_replicated_entries(entries)
                    .map(|_| WriteResponse::Unit);
                let _ = request.response.send(result);
            }
            operation => match database.prepare_local_write(operation) {
                Ok(prepared_write) => prepared.push((prepared_write, request.response)),
                Err(err) => {
                    let _ = request.response.send(Err(err));
                }
            },
        }
    }
    if prepared.is_empty() {
        return;
    }
    let entries = prepared
        .iter()
        .map(|(prepared_write, _)| prepared_write.entry.clone())
        .collect::<Vec<_>>();
    let flush_result = database.flush_group_commit(&entries);
    for (prepared_write, response) in prepared {
        let result = match &flush_result {
            Ok(()) => Ok(prepared_write.response),
            Err(err) => Err(error_for_batch_response(err)),
        };
        let _ = response.send(result);
    }
}

fn expect_unit(response: WriteResponse) -> DatabaseResult<()> {
    match response {
        WriteResponse::Unit => Ok(()),
        response => Err(unexpected_write_response(response)),
    }
}

fn unexpected_write_response(response: WriteResponse) -> DatabaseError {
    DatabaseError::UnexpectedWriteResponse(format!("{response:?}"))
}

fn error_for_batch_response(err: &DatabaseError) -> DatabaseError {
    match err {
        DatabaseError::InvalidConfig(message) => DatabaseError::InvalidConfig(message.clone()),
        DatabaseError::MissingShardLog(shard_id) => DatabaseError::MissingShardLog(*shard_id),
        DatabaseError::ShardNotLocal {
            shard_id,
            server_id,
        } => DatabaseError::ShardNotLocal {
            shard_id: *shard_id,
            server_id: *server_id,
        },
        DatabaseError::ShardNotPrimary {
            shard_id,
            server_id,
            primary_server_id,
        } => DatabaseError::ShardNotPrimary {
            shard_id: *shard_id,
            server_id: *server_id,
            primary_server_id: *primary_server_id,
        },
        DatabaseError::UnexpectedLogIndex {
            shard_id,
            expected,
            actual,
        } => DatabaseError::UnexpectedLogIndex {
            shard_id: *shard_id,
            expected: *expected,
            actual: *actual,
        },
        DatabaseError::LogConflict {
            shard_id,
            index,
            message,
        } => DatabaseError::LogConflict {
            shard_id: *shard_id,
            index: *index,
            message: message.clone(),
        },
        DatabaseError::Replication(message) => DatabaseError::Replication(message.clone()),
        DatabaseError::LockPoisoned => DatabaseError::LockPoisoned,
        DatabaseError::WriterUnavailable => DatabaseError::WriterUnavailable,
        DatabaseError::UnexpectedWriteResponse(message) => {
            DatabaseError::UnexpectedWriteResponse(message.clone())
        }
        DatabaseError::Graph(_)
        | DatabaseError::GraphRead(_)
        | DatabaseError::Query(_)
        | DatabaseError::Storage(_) => DatabaseError::Replication(err.to_string()),
    }
}

struct PreparedWrite {
    entry: LogEntry,
    response: WriteResponse,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum QueryRoute {
    LocalOnly,
    RequiresRemoteShards(Vec<ShardId>),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RemoteTraversalPolicy {
    BoundaryCacheOnly,
    RemoteShardHop(Vec<ShardId>),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DistributedQueryPlan {
    pub route: QueryRoute,
    pub traversal_policy: RemoteTraversalPolicy,
    pub uses_boundary_cache: bool,
    pub access_plan: QueryAccessPlan,
    pub estimated_cost: u64,
    pub estimated_rows: u64,
    pub remote_shard_count: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum QueryAccessPlan {
    NodeUniqueIndexSeek {
        label: String,
        property: String,
    },
    NodeIndexSeek {
        label: String,
        property: String,
    },
    NodeLabelScan {
        label: String,
    },
    NodeFullScan,
    VectorIndexSeek {
        label: Option<String>,
        property: String,
        metric: String,
    },
    RelationshipTypeScan {
        rel_type: String,
    },
    RelationshipScan,
    Unsupported {
        reason: String,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryProfile {
    pub plan: DistributedQueryPlan,
    pub metrics: QueryMetrics,
    pub operators: Vec<QueryOperatorProfile>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryMetrics {
    pub planning_elapsed_micros: u128,
    pub execution_elapsed_micros: u128,
    pub rows_returned: usize,
    pub scanned_nodes: usize,
    pub scanned_relationships: usize,
    pub index_count: usize,
    pub read_cache_hits: u64,
    pub read_cache_misses: u64,
    pub index_cache_hits: u64,
    pub index_cache_misses: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryOperatorProfile {
    pub name: String,
    pub estimated_rows: u64,
    pub actual_rows: usize,
    pub elapsed_micros: u128,
    pub children: Vec<QueryOperatorProfile>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StorageStatus {
    pub data_dir: PathBuf,
    pub total_bytes: u64,
    pub file_count: u64,
    pub wal_segment_count: u64,
    pub checkpoint_file_count: u64,
    pub metadata_file_count: u64,
    pub committed_indexes: Vec<LogIndex>,
    pub read_cache_hits: u64,
    pub read_cache_misses: u64,
    pub index_cache_hits: u64,
    pub index_cache_misses: u64,
    pub wal_pruned_until: Vec<LogIndex>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct StatisticsCatalog {
    pub node_count: usize,
    pub relationship_count: usize,
    pub label_counts: Vec<(String, usize)>,
    pub relationship_type_counts: Vec<(String, usize)>,
    pub index_count: usize,
    pub vector_index_count: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StorageMaintenanceResult {
    pub action: String,
    pub files_touched: u64,
    pub bytes_observed: u64,
    pub pruned_until: Vec<LogIndex>,
}

#[derive(Clone, Debug, Default)]
struct ReadPathCache {
    nodes: HashMap<NodeId, Option<Node>>,
    relationships: HashMap<RelationshipId, Option<Relationship>>,
    index_lookups: HashMap<String, Vec<u64>>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct ReadCacheStats {
    hits: u64,
    misses: u64,
    index_hits: u64,
    index_misses: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetadataOperationRecord {
    pub index: u64,
    pub term: u64,
    pub operation: String,
    pub config_epoch: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterStatus {
    pub server_id: ServerId,
    pub routing_version: u64,
    pub shard_count: u64,
    pub local_partition_count: usize,
    pub shards: Vec<ShardStatus>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ShardStatus {
    pub shard_id: ShardId,
    pub primary_server_id: Option<ServerId>,
    pub replica_server_ids: Vec<ServerId>,
    pub has_local_copy: bool,
    pub is_local_primary: bool,
    pub applied_index: LogIndex,
    pub committed_index: LogIndex,
    pub match_indexes: Vec<(ServerId, LogIndex)>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RebalancePlan {
    pub plan_id: u64,
    pub state: RebalancePlanState,
    pub from_routing_version: u64,
    pub target_routing_version: u64,
    pub steps: Vec<RebalanceStep>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RebalancePolicy {
    pub replication_factor: usize,
    pub max_steps_per_plan: usize,
}

impl Default for RebalancePolicy {
    fn default() -> Self {
        Self {
            replication_factor: 2,
            max_steps_per_plan: usize::MAX,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterMetadataState {
    pub authority_server_id: ServerId,
    pub term: u64,
    pub config_epoch: u64,
    pub policy: RebalancePolicy,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RebalanceExecution {
    pub plan: RebalancePlan,
    pub state: RebalancePlanState,
    pub current_step: usize,
    pub steps: Vec<RebalanceStepExecution>,
    pub last_error: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RebalanceStepExecution {
    pub step_index: usize,
    pub step: RebalanceStep,
    pub state: RebalanceStepState,
    pub attempts: u64,
    pub retryable: bool,
    pub last_error: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RebalanceStepState {
    Pending,
    Preparing,
    CatchingUp,
    Ready,
    Applying,
    Applied,
    Failed,
    Cancelled,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RebalanceAdvanceResult {
    pub execution: RebalanceExecution,
    pub action: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterManagementStatus {
    pub metadata: ClusterMetadataState,
    pub membership: ClusterMembership,
    pub rebalance_plan: Option<RebalancePlan>,
    pub rebalance_execution: Option<RebalanceExecution>,
    pub routing_version: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RebalancePlanState {
    Proposed,
    Running,
    Completed,
    Failed,
    Cancelled,
}

#[derive(Clone, Debug)]
struct RebalancePlanStore {
    path: PathBuf,
}

impl RebalancePlanStore {
    fn open(data_dir: impl AsRef<Path>) -> DatabaseResult<Self> {
        let cluster_dir = data_dir.as_ref().join("cluster");
        fs::create_dir_all(&cluster_dir).map_err(StorageError::Io)?;
        Ok(Self {
            path: cluster_dir.join("rebalance-plan.txt"),
        })
    }

    fn save(&self, plan: &RebalancePlan) -> DatabaseResult<()> {
        let tmp_path = self.path.with_extension("txt.tmp");
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp_path)
            .map_err(StorageError::Io)?;
        writeln!(
            file,
            "N4RPLAN1\t{}\t{}\t{}\t{}",
            plan.plan_id,
            encode_rebalance_plan_state(plan.state),
            plan.from_routing_version,
            plan.target_routing_version
        )
        .map_err(StorageError::Io)?;
        for step in &plan.steps {
            writeln!(file, "{}", encode_rebalance_step(step)).map_err(StorageError::Io)?;
        }
        file.sync_all().map_err(StorageError::Io)?;
        drop(file);
        fs::rename(&tmp_path, &self.path).map_err(StorageError::Io)?;
        if let Some(parent) = self.path.parent() {
            File::open(parent)
                .map_err(StorageError::Io)?
                .sync_all()
                .map_err(StorageError::Io)?;
        }
        Ok(())
    }

    fn load(&self) -> DatabaseResult<Option<RebalancePlan>> {
        let file = match File::open(&self.path) {
            Ok(file) => file,
            Err(err) if err.kind() == ErrorKind::NotFound => return Ok(None),
            Err(err) => return Err(StorageError::Io(err).into()),
        };
        let mut lines = BufReader::new(file).lines();
        let header = lines
            .next()
            .transpose()
            .map_err(StorageError::Io)?
            .ok_or_else(|| {
                StorageError::CorruptStore("missing rebalance plan header".to_string())
            })?;
        let header_parts = header.split('\t').collect::<Vec<_>>();
        if header_parts.len() != 5 || header_parts[0] != "N4RPLAN1" {
            return Err(
                StorageError::CorruptStore("invalid rebalance plan header".to_string()).into(),
            );
        }
        let mut steps = Vec::new();
        for line in lines {
            let line = line.map_err(StorageError::Io)?;
            if line.is_empty() {
                continue;
            }
            steps.push(decode_rebalance_step(&line)?);
        }
        Ok(Some(RebalancePlan {
            plan_id: parse_plan_u64(header_parts[1], "rebalance plan id")?,
            state: decode_rebalance_plan_state(header_parts[2])?,
            from_routing_version: parse_plan_u64(
                header_parts[3],
                "rebalance plan source routing version",
            )?,
            target_routing_version: parse_plan_u64(
                header_parts[4],
                "rebalance plan target routing version",
            )?,
            steps,
        }))
    }

    fn next_plan_id(&self) -> DatabaseResult<u64> {
        Ok(self
            .load()?
            .map(|plan| plan.plan_id.saturating_add(1))
            .unwrap_or(1))
    }
}

#[derive(Clone, Debug)]
struct RebalanceExecutionStore {
    path: PathBuf,
}

impl RebalanceExecutionStore {
    fn open(data_dir: impl AsRef<Path>) -> DatabaseResult<Self> {
        let cluster_dir = data_dir.as_ref().join("cluster");
        fs::create_dir_all(&cluster_dir).map_err(StorageError::Io)?;
        Ok(Self {
            path: cluster_dir.join("rebalance-execution.txt"),
        })
    }

    fn save(&self, execution: &RebalanceExecution) -> DatabaseResult<()> {
        let tmp_path = self.path.with_extension("txt.tmp");
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp_path)
            .map_err(StorageError::Io)?;
        writeln!(
            file,
            "N4REXEC1\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
            execution.plan.plan_id,
            encode_rebalance_plan_state(execution.state),
            execution.current_step,
            execution.plan.from_routing_version,
            execution.plan.target_routing_version,
            execution.steps.len(),
            sanitize_cluster_text(&execution.last_error)
        )
        .map_err(StorageError::Io)?;
        for step in &execution.steps {
            writeln!(
                file,
                "step\t{}\t{}\t{}\t{}\t{}\t{}",
                step.step_index,
                encode_rebalance_step_state(step.state),
                step.attempts,
                step.retryable as u8,
                sanitize_cluster_text(&step.last_error),
                encode_rebalance_step(&step.step)
            )
            .map_err(StorageError::Io)?;
        }
        file.sync_all().map_err(StorageError::Io)?;
        drop(file);
        fs::rename(&tmp_path, &self.path).map_err(StorageError::Io)?;
        if let Some(parent) = self.path.parent() {
            File::open(parent)
                .map_err(StorageError::Io)?
                .sync_all()
                .map_err(StorageError::Io)?;
        }
        Ok(())
    }

    fn load(&self) -> DatabaseResult<Option<RebalanceExecution>> {
        let file = match File::open(&self.path) {
            Ok(file) => file,
            Err(err) if err.kind() == ErrorKind::NotFound => return Ok(None),
            Err(err) => return Err(StorageError::Io(err).into()),
        };
        let mut lines = BufReader::new(file).lines();
        let header = lines
            .next()
            .transpose()
            .map_err(StorageError::Io)?
            .ok_or_else(|| {
                StorageError::CorruptStore("missing rebalance execution header".to_string())
            })?;
        let header_parts = header.split('\t').collect::<Vec<_>>();
        if header_parts.len() != 8 || header_parts[0] != "N4REXEC1" {
            return Err(StorageError::CorruptStore(
                "invalid rebalance execution header".to_string(),
            )
            .into());
        }
        let mut steps = Vec::new();
        for line in lines {
            let line = line.map_err(StorageError::Io)?;
            if line.is_empty() {
                continue;
            }
            let parts = line.split('\t').collect::<Vec<_>>();
            if parts.len() < 9 || parts[0] != "step" {
                return Err(StorageError::CorruptStore(
                    "invalid rebalance execution step".to_string(),
                )
                .into());
            }
            steps.push(RebalanceStepExecution {
                step_index: parse_plan_usize(parts[1], "rebalance step index")?,
                state: decode_rebalance_step_state(parts[2])?,
                attempts: parse_plan_u64(parts[3], "rebalance step attempts")?,
                retryable: parse_plan_bool(parts[4], "rebalance step retryable")?,
                last_error: parts[5].to_string(),
                step: decode_rebalance_step(&parts[6..].join("\t"))?,
            });
        }
        Ok(Some(RebalanceExecution {
            plan: RebalancePlan {
                plan_id: parse_plan_u64(header_parts[1], "rebalance execution plan id")?,
                state: decode_rebalance_plan_state(header_parts[2])?,
                from_routing_version: parse_plan_u64(
                    header_parts[4],
                    "rebalance execution source routing version",
                )?,
                target_routing_version: parse_plan_u64(
                    header_parts[5],
                    "rebalance execution target routing version",
                )?,
                steps: steps.iter().map(|step| step.step.clone()).collect(),
            },
            state: decode_rebalance_plan_state(header_parts[2])?,
            current_step: parse_plan_usize(header_parts[3], "rebalance execution current step")?,
            steps,
            last_error: header_parts[7].to_string(),
        }))
    }
}

#[derive(Clone, Debug)]
struct ClusterMetadataStore {
    path: PathBuf,
}

impl ClusterMetadataStore {
    fn open(data_dir: impl AsRef<Path>) -> DatabaseResult<Self> {
        let cluster_dir = data_dir.as_ref().join("cluster");
        fs::create_dir_all(&cluster_dir).map_err(StorageError::Io)?;
        Ok(Self {
            path: cluster_dir.join("metadata-authority.txt"),
        })
    }

    fn save(&self, metadata: &ClusterMetadataState) -> DatabaseResult<()> {
        let tmp_path = self.path.with_extension("txt.tmp");
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp_path)
            .map_err(StorageError::Io)?;
        writeln!(
            file,
            "N4RMETA1\t{}\t{}\t{}\t{}\t{}",
            metadata.authority_server_id,
            metadata.term,
            metadata.config_epoch,
            metadata.policy.replication_factor,
            metadata.policy.max_steps_per_plan
        )
        .map_err(StorageError::Io)?;
        file.sync_all().map_err(StorageError::Io)?;
        drop(file);
        fs::rename(&tmp_path, &self.path).map_err(StorageError::Io)?;
        Ok(())
    }

    fn load(&self) -> DatabaseResult<Option<ClusterMetadataState>> {
        let file = match File::open(&self.path) {
            Ok(file) => file,
            Err(err) if err.kind() == ErrorKind::NotFound => return Ok(None),
            Err(err) => return Err(StorageError::Io(err).into()),
        };
        let mut lines = BufReader::new(file).lines();
        let header = lines
            .next()
            .transpose()
            .map_err(StorageError::Io)?
            .ok_or_else(|| StorageError::CorruptStore("missing cluster metadata".to_string()))?;
        let parts = header.split('\t').collect::<Vec<_>>();
        if parts.len() != 6 || parts[0] != "N4RMETA1" {
            return Err(StorageError::CorruptStore("invalid cluster metadata".to_string()).into());
        }
        Ok(Some(ClusterMetadataState {
            authority_server_id: parse_plan_u64(parts[1], "metadata authority server id")?,
            term: parse_plan_u64(parts[2], "metadata authority term")?,
            config_epoch: parse_plan_u64(parts[3], "metadata config epoch")?,
            policy: RebalancePolicy {
                replication_factor: parse_plan_usize(parts[4], "metadata replication factor")?,
                max_steps_per_plan: parse_plan_usize(parts[5], "metadata max steps per plan")?,
            },
        }))
    }
}

#[derive(Clone, Debug)]
struct MetadataOperationLogStore {
    path: PathBuf,
}

impl MetadataOperationLogStore {
    fn open(data_dir: impl AsRef<Path>) -> DatabaseResult<Self> {
        let cluster_dir = data_dir.as_ref().join("cluster");
        fs::create_dir_all(&cluster_dir).map_err(StorageError::Io)?;
        let store = Self {
            path: cluster_dir.join("metadata-log.txt"),
        };
        if !store.path.exists() {
            let mut file = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&store.path)
                .map_err(StorageError::Io)?;
            writeln!(file, "N4RMETALOG1").map_err(StorageError::Io)?;
            file.sync_all().map_err(StorageError::Io)?;
        }
        Ok(store)
    }

    fn append(
        &self,
        term: u64,
        config_epoch: u64,
        operation: &str,
    ) -> DatabaseResult<MetadataOperationRecord> {
        let index = self.next_index()?;
        let operation = sanitize_cluster_text(operation);
        let mut file = OpenOptions::new()
            .append(true)
            .open(&self.path)
            .map_err(StorageError::Io)?;
        writeln!(file, "{index}\t{term}\t{config_epoch}\t{operation}").map_err(StorageError::Io)?;
        file.sync_all().map_err(StorageError::Io)?;
        Ok(MetadataOperationRecord {
            index,
            term,
            operation,
            config_epoch,
        })
    }

    fn load(&self) -> DatabaseResult<Vec<MetadataOperationRecord>> {
        let file = File::open(&self.path).map_err(StorageError::Io)?;
        let mut lines = BufReader::new(file).lines();
        let header = lines
            .next()
            .transpose()
            .map_err(StorageError::Io)?
            .ok_or_else(|| StorageError::CorruptStore("missing metadata log header".to_string()))?;
        if header != "N4RMETALOG1" {
            return Err(
                StorageError::CorruptStore("invalid metadata log header".to_string()).into(),
            );
        }
        let mut records = Vec::new();
        for line in lines {
            let line = line.map_err(StorageError::Io)?;
            if line.is_empty() {
                continue;
            }
            let parts = line.split('\t').collect::<Vec<_>>();
            if parts.len() != 4 {
                return Err(
                    StorageError::CorruptStore("invalid metadata log record".to_string()).into(),
                );
            }
            records.push(MetadataOperationRecord {
                index: parse_plan_u64(parts[0], "metadata log index")?,
                term: parse_plan_u64(parts[1], "metadata log term")?,
                config_epoch: parse_plan_u64(parts[2], "metadata log config epoch")?,
                operation: parts[3].to_string(),
            });
        }
        Ok(records)
    }

    fn next_index(&self) -> DatabaseResult<u64> {
        Ok(self
            .load()?
            .last()
            .map(|record| record.index.saturating_add(1))
            .unwrap_or(1))
    }
}

#[derive(Clone, Debug)]
struct StatisticsCatalogStore {
    path: PathBuf,
}

impl StatisticsCatalogStore {
    fn open(data_dir: impl AsRef<Path>) -> DatabaseResult<Self> {
        let cluster_dir = data_dir.as_ref().join("cluster");
        fs::create_dir_all(&cluster_dir).map_err(StorageError::Io)?;
        Ok(Self {
            path: cluster_dir.join("statistics-catalog.txt"),
        })
    }

    fn save(&self, statistics: &StatisticsCatalog) -> DatabaseResult<()> {
        let tmp_path = self.path.with_extension("txt.tmp");
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp_path)
            .map_err(StorageError::Io)?;
        writeln!(
            file,
            "N4RSTATS1\t{}\t{}\t{}\t{}",
            statistics.node_count,
            statistics.relationship_count,
            statistics.index_count,
            statistics.vector_index_count
        )
        .map_err(StorageError::Io)?;
        for (label, count) in &statistics.label_counts {
            writeln!(file, "label\t{}\t{}", sanitize_cluster_text(label), count)
                .map_err(StorageError::Io)?;
        }
        for (rel_type, count) in &statistics.relationship_type_counts {
            writeln!(
                file,
                "relationship_type\t{}\t{}",
                sanitize_cluster_text(rel_type),
                count
            )
            .map_err(StorageError::Io)?;
        }
        file.sync_all().map_err(StorageError::Io)?;
        drop(file);
        fs::rename(&tmp_path, &self.path).map_err(StorageError::Io)?;
        Ok(())
    }

    fn load(&self) -> DatabaseResult<Option<StatisticsCatalog>> {
        let file = match File::open(&self.path) {
            Ok(file) => file,
            Err(err) if err.kind() == ErrorKind::NotFound => return Ok(None),
            Err(err) => return Err(StorageError::Io(err).into()),
        };
        let mut lines = BufReader::new(file).lines();
        let header = lines
            .next()
            .transpose()
            .map_err(StorageError::Io)?
            .ok_or_else(|| {
                StorageError::CorruptStore("missing statistics catalog header".to_string())
            })?;
        let parts = header.split('\t').collect::<Vec<_>>();
        if parts.len() != 5 || parts[0] != "N4RSTATS1" {
            return Err(StorageError::CorruptStore(
                "invalid statistics catalog header".to_string(),
            )
            .into());
        }
        let mut label_counts = Vec::new();
        let mut relationship_type_counts = Vec::new();
        for line in lines {
            let line = line.map_err(StorageError::Io)?;
            let parts = line.split('\t').collect::<Vec<_>>();
            match parts.as_slice() {
                ["label", label, count] => {
                    label_counts.push((
                        (*label).to_string(),
                        parse_plan_usize(count, "label count")?,
                    ));
                }
                ["relationship_type", rel_type, count] => {
                    relationship_type_counts.push((
                        (*rel_type).to_string(),
                        parse_plan_usize(count, "relationship type count")?,
                    ));
                }
                _ => {
                    return Err(StorageError::CorruptStore(
                        "invalid statistics catalog record".to_string(),
                    )
                    .into())
                }
            }
        }
        Ok(Some(StatisticsCatalog {
            node_count: parse_plan_usize(parts[1], "statistics node count")?,
            relationship_count: parse_plan_usize(parts[2], "statistics relationship count")?,
            index_count: parse_plan_usize(parts[3], "statistics index count")?,
            vector_index_count: parse_plan_usize(parts[4], "statistics vector index count")?,
            label_counts,
            relationship_type_counts,
        }))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RebalanceStep {
    AddReplica {
        shard_id: ShardId,
        server_id: ServerId,
    },
    TransferPrimary {
        shard_id: ShardId,
        from: ServerId,
        to: ServerId,
    },
    RemoveReplica {
        shard_id: ShardId,
        server_id: ServerId,
    },
}

fn encode_rebalance_plan_state(state: RebalancePlanState) -> &'static str {
    match state {
        RebalancePlanState::Proposed => "proposed",
        RebalancePlanState::Running => "running",
        RebalancePlanState::Completed => "completed",
        RebalancePlanState::Failed => "failed",
        RebalancePlanState::Cancelled => "cancelled",
    }
}

fn decode_rebalance_plan_state(input: &str) -> DatabaseResult<RebalancePlanState> {
    match input {
        "proposed" => Ok(RebalancePlanState::Proposed),
        "running" => Ok(RebalancePlanState::Running),
        "completed" => Ok(RebalancePlanState::Completed),
        "failed" => Ok(RebalancePlanState::Failed),
        "cancelled" => Ok(RebalancePlanState::Cancelled),
        _ => Err(
            StorageError::CorruptStore(format!("unknown rebalance plan state {input:?}")).into(),
        ),
    }
}

fn encode_rebalance_step(step: &RebalanceStep) -> String {
    match step {
        RebalanceStep::AddReplica {
            shard_id,
            server_id,
        } => format!("ADD_REPLICA\t{shard_id}\t{server_id}"),
        RebalanceStep::TransferPrimary { shard_id, from, to } => {
            format!("TRANSFER_PRIMARY\t{shard_id}\t{from}\t{to}")
        }
        RebalanceStep::RemoveReplica {
            shard_id,
            server_id,
        } => format!("REMOVE_REPLICA\t{shard_id}\t{server_id}"),
    }
}

fn encode_rebalance_step_state(state: RebalanceStepState) -> &'static str {
    match state {
        RebalanceStepState::Pending => "pending",
        RebalanceStepState::Preparing => "preparing",
        RebalanceStepState::CatchingUp => "catching_up",
        RebalanceStepState::Ready => "ready",
        RebalanceStepState::Applying => "applying",
        RebalanceStepState::Applied => "applied",
        RebalanceStepState::Failed => "failed",
        RebalanceStepState::Cancelled => "cancelled",
    }
}

fn decode_rebalance_step_state(input: &str) -> DatabaseResult<RebalanceStepState> {
    match input {
        "pending" => Ok(RebalanceStepState::Pending),
        "preparing" => Ok(RebalanceStepState::Preparing),
        "catching_up" => Ok(RebalanceStepState::CatchingUp),
        "ready" => Ok(RebalanceStepState::Ready),
        "applying" => Ok(RebalanceStepState::Applying),
        "applied" => Ok(RebalanceStepState::Applied),
        "failed" => Ok(RebalanceStepState::Failed),
        "cancelled" => Ok(RebalanceStepState::Cancelled),
        _ => Err(
            StorageError::CorruptStore(format!("unknown rebalance step state {input:?}")).into(),
        ),
    }
}

fn decode_rebalance_step(line: &str) -> DatabaseResult<RebalanceStep> {
    let parts = line.split('\t').collect::<Vec<_>>();
    match parts.first().copied() {
        Some("ADD_REPLICA") if parts.len() == 3 => Ok(RebalanceStep::AddReplica {
            shard_id: parse_plan_u64(parts[1], "rebalance add replica shard id")?,
            server_id: parse_plan_u64(parts[2], "rebalance add replica server id")?,
        }),
        Some("TRANSFER_PRIMARY") if parts.len() == 4 => Ok(RebalanceStep::TransferPrimary {
            shard_id: parse_plan_u64(parts[1], "rebalance transfer shard id")?,
            from: parse_plan_u64(parts[2], "rebalance transfer source server id")?,
            to: parse_plan_u64(parts[3], "rebalance transfer target server id")?,
        }),
        Some("REMOVE_REPLICA") if parts.len() == 3 => Ok(RebalanceStep::RemoveReplica {
            shard_id: parse_plan_u64(parts[1], "rebalance remove replica shard id")?,
            server_id: parse_plan_u64(parts[2], "rebalance remove replica server id")?,
        }),
        _ => Err(StorageError::CorruptStore("invalid rebalance step record".to_string()).into()),
    }
}

fn parse_plan_u64(input: &str, name: &str) -> DatabaseResult<u64> {
    input
        .parse::<u64>()
        .map_err(|_| StorageError::CorruptStore(format!("invalid {name}")).into())
}

fn parse_plan_usize(input: &str, name: &str) -> DatabaseResult<usize> {
    input
        .parse::<usize>()
        .map_err(|_| StorageError::CorruptStore(format!("invalid {name}")).into())
}

fn parse_plan_bool(input: &str, name: &str) -> DatabaseResult<bool> {
    match input {
        "0" => Ok(false),
        "1" => Ok(true),
        _ => Err(StorageError::CorruptStore(format!("invalid {name}")).into()),
    }
}

fn sanitize_cluster_text(input: &str) -> String {
    input
        .chars()
        .map(|ch| {
            if matches!(ch, '\t' | '\n' | '\r') {
                ' '
            } else {
                ch
            }
        })
        .collect()
}

enum WriteCypher {
    CreateNode {
        variable: String,
        labels: Vec<String>,
        properties: Properties,
        assignments: Vec<PropertyAssignment>,
        replacement: Option<Properties>,
        returns: Option<WriteReturnItems>,
    },
    CreateRelationship {
        variable: String,
        from_matcher: NodeMatcher,
        to_matcher: NodeMatcher,
        rel_type: String,
        properties: Properties,
        assignments: Vec<PropertyAssignment>,
        replacement: Option<Properties>,
        returns: Option<WriteReturnItems>,
    },
    MergeNode {
        labels: Vec<String>,
        properties: Properties,
        on_create: Vec<PropertyAssignment>,
        on_create_replacement: Option<Properties>,
        on_match: Vec<PropertyAssignment>,
        on_match_replacement: Option<Properties>,
        returns: Option<WriteReturnItems>,
    },
    MergeRelationship {
        from_matcher: NodeMatcher,
        to_matcher: NodeMatcher,
        rel_type: String,
        properties: Properties,
        on_create: Vec<PropertyAssignment>,
        on_create_replacement: Option<Properties>,
        on_match: Vec<PropertyAssignment>,
        on_match_replacement: Option<Properties>,
        returns: Option<WriteReturnItems>,
    },
    SetNodeProperty {
        matcher: NodeMatcher,
        assignments: Vec<PropertyAssignment>,
        returns: Option<WriteReturnItems>,
    },
    ReplaceNodeProperties {
        matcher: NodeMatcher,
        properties: Properties,
        returns: Option<WriteReturnItems>,
    },
    RemoveNodeProperty {
        matcher: NodeMatcher,
        keys: Vec<String>,
        returns: Option<WriteReturnItems>,
    },
    AddNodeLabel {
        matcher: NodeMatcher,
        labels: Vec<String>,
        returns: Option<WriteReturnItems>,
    },
    RemoveNodeLabel {
        matcher: NodeMatcher,
        labels: Vec<String>,
        returns: Option<WriteReturnItems>,
    },
    SetRelationshipProperty {
        matcher: RelationshipMatcher,
        assignments: Vec<PropertyAssignment>,
        returns: Option<WriteReturnItems>,
    },
    ReplaceRelationshipProperties {
        matcher: RelationshipMatcher,
        properties: Properties,
        returns: Option<WriteReturnItems>,
    },
    RemoveRelationshipProperty {
        matcher: RelationshipMatcher,
        keys: Vec<String>,
        returns: Option<WriteReturnItems>,
    },
    DeleteNode {
        matcher: NodeMatcher,
        returns: Option<WriteReturnItems>,
    },
    DeleteRelationship {
        matcher: RelationshipMatcher,
        returns: Option<WriteReturnItems>,
    },
    CreateNodePropertyIndex {
        name: String,
        label: String,
        property: String,
        if_not_exists: bool,
    },
    CreateUniqueNodePropertyConstraint {
        name: String,
        label: String,
        property: String,
        if_not_exists: bool,
    },
    CreateVectorIndex {
        name: String,
        label: String,
        property: String,
        dimensions: usize,
        metric: String,
        if_not_exists: bool,
    },
    RebuildVectorIndex {
        name: String,
    },
    DropIndex {
        name: String,
        if_exists: bool,
    },
    DropConstraint {
        name: String,
        if_exists: bool,
    },
}

struct NodeMatcher {
    variable: String,
    match_query: String,
}

struct RelationshipMatcher {
    variable: String,
    match_query: String,
}

#[derive(Clone, Debug, PartialEq)]
struct PropertyAssignment {
    key: String,
    value: Value,
}

#[derive(Default)]
struct MergeSetClauses {
    on_create: Vec<PropertyAssignment>,
    on_create_replacement: Option<Properties>,
    on_match: Vec<PropertyAssignment>,
    on_match_replacement: Option<Properties>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum WriteReturnItem {
    Variable(String),
    Property { variable: String, key: String },
}

type WriteReturnItems = Vec<WriteReturnItem>;

#[derive(Clone, Debug, PartialEq)]
pub struct CreateNodeRoutingKey {
    pub labels: Vec<String>,
    pub properties: Properties,
}

pub fn create_node_routing_key(
    query: &str,
    params: &QueryParams,
) -> DatabaseResult<Option<CreateNodeRoutingKey>> {
    match parse_write_cypher(query, params)? {
        Some(WriteCypher::CreateNode {
            labels,
            properties,
            assignments,
            replacement,
            ..
        }) => {
            let properties = create_properties_after_set(properties, assignments, replacement);
            Ok(Some(CreateNodeRoutingKey { labels, properties }))
        }
        _ => Ok(None),
    }
}

pub fn merge_node_routing_key(
    query: &str,
    params: &QueryParams,
) -> DatabaseResult<Option<CreateNodeRoutingKey>> {
    match parse_write_cypher(query, params)? {
        Some(WriteCypher::MergeNode {
            labels, properties, ..
        }) => Ok(Some(CreateNodeRoutingKey { labels, properties })),
        _ => Ok(None),
    }
}

fn parse_write_cypher(query: &str, params: &QueryParams) -> DatabaseResult<Option<WriteCypher>> {
    let input = query.trim();
    if input.is_empty() {
        return Ok(None);
    }
    if starts_with_keyword(input, "CREATE VECTOR INDEX") {
        return parse_create_vector_index_ddl(input).map(Some);
    }
    if starts_with_keyword(input, "CREATE CONSTRAINT") {
        return parse_create_unique_node_property_constraint_ddl(input).map(Some);
    }
    if starts_with_keyword(input, "CREATE INDEX") {
        return parse_create_index_ddl(input).map(Some);
    }
    if starts_with_keyword(input, "DROP INDEX") {
        return parse_drop_index_ddl(input).map(Some);
    }
    if starts_with_keyword(input, "DROP CONSTRAINT") {
        return parse_drop_constraint_ddl(input).map(Some);
    }
    if starts_with_keyword(input, "REBUILD VECTOR INDEX") {
        return parse_rebuild_vector_index_ddl(input).map(Some);
    }
    if starts_with_keyword(input, "CREATE") {
        return parse_create_node_write(input, params).map(Some);
    }
    if starts_with_keyword(input, "MERGE") {
        return parse_merge_node_write(input, params).map(Some);
    }
    if !starts_with_keyword(input, "MATCH") {
        return Ok(None);
    }
    if let Some((match_part, merge_part)) = split_keyword(input, "MERGE") {
        return parse_merge_relationship_write(match_part, merge_part, params).map(Some);
    }
    if let Some((match_part, create_part)) = split_keyword(input, "CREATE") {
        return parse_create_relationship_write(match_part, create_part, params).map(Some);
    }
    if let Some((match_part, set_part)) = split_keyword(input, "SET") {
        return parse_set_property(match_part, set_part, params).map(Some);
    }
    if let Some((match_part, remove_part)) = split_keyword(input, "REMOVE") {
        return parse_remove_property(match_part, remove_part, params).map(Some);
    }
    if let Some((match_part, delete_part)) = split_keyword(input, "DETACH DELETE") {
        return parse_delete(match_part, delete_part, params).map(Some);
    }
    if let Some((match_part, delete_part)) = split_keyword(input, "DELETE") {
        return parse_delete(match_part, delete_part, params).map(Some);
    }
    Ok(None)
}

fn is_show_indexes_cypher(query: &str) -> bool {
    query.trim().eq_ignore_ascii_case("SHOW INDEXES")
}

fn show_index_name(query: &str) -> DatabaseResult<Option<String>> {
    let input = query.trim();
    if !starts_with_keyword(input, "SHOW INDEX") || is_show_indexes_cypher(input) {
        return Ok(None);
    }
    let name = strip_keyword(input, "SHOW INDEX")?.trim();
    ensure_write_parse(
        !name.contains(char::is_whitespace),
        "SHOW INDEX requires a single index name",
    )?;
    validate_identifier_write(name)?;
    Ok(Some(name.to_string()))
}

fn is_show_vector_indexes_cypher(query: &str) -> bool {
    query.trim().eq_ignore_ascii_case("SHOW VECTOR INDEXES")
}

fn is_show_vector_index_status_cypher(query: &str) -> bool {
    query
        .trim()
        .eq_ignore_ascii_case("SHOW VECTOR INDEX STATUS")
}

fn show_vector_index_status_name(query: &str) -> DatabaseResult<Option<String>> {
    let input = query.trim();
    if !starts_with_keyword(input, "SHOW VECTOR INDEX STATUS")
        || is_show_vector_index_status_cypher(input)
    {
        return Ok(None);
    }
    let name = strip_keyword(input, "SHOW VECTOR INDEX STATUS")?.trim();
    ensure_write_parse(
        !name.contains(char::is_whitespace),
        "SHOW VECTOR INDEX STATUS requires a single index name",
    )?;
    validate_identifier_write(name)?;
    Ok(Some(name.to_string()))
}

fn show_vector_index_name(query: &str) -> DatabaseResult<Option<String>> {
    let input = query.trim();
    if !starts_with_keyword(input, "SHOW VECTOR INDEX")
        || is_show_vector_indexes_cypher(input)
        || starts_with_keyword(input, "SHOW VECTOR INDEX STATUS")
    {
        return Ok(None);
    }
    let name = strip_keyword(input, "SHOW VECTOR INDEX")?.trim();
    ensure_write_parse(
        !name.contains(char::is_whitespace),
        "SHOW VECTOR INDEX requires a single index name",
    )?;
    validate_identifier_write(name)?;
    Ok(Some(name.to_string()))
}

fn is_show_constraints_cypher(query: &str) -> bool {
    query.trim().eq_ignore_ascii_case("SHOW CONSTRAINTS")
}

fn show_constraint_name(query: &str) -> DatabaseResult<Option<String>> {
    let input = query.trim();
    if !starts_with_keyword(input, "SHOW CONSTRAINT") || is_show_constraints_cypher(input) {
        return Ok(None);
    }
    let name = strip_keyword(input, "SHOW CONSTRAINT")?.trim();
    ensure_write_parse(
        !name.contains(char::is_whitespace),
        "SHOW CONSTRAINT requires a single constraint name",
    )?;
    validate_identifier_write(name)?;
    Ok(Some(name.to_string()))
}

fn format_index_rows(indexes: &[IndexDefinition]) -> Vec<QueryRow> {
    indexes
        .iter()
        .map(|index| {
            let mut row = QueryRow::new();
            row.insert(
                "name",
                QueryValue::Scalar(Value::String(index.name.clone())),
            );
            row.insert(
                "label",
                QueryValue::Scalar(Value::String(index.label.clone())),
            );
            row.insert(
                "property",
                QueryValue::Scalar(Value::String(index.property.clone())),
            );
            match &index.kind {
                IndexKind::NodeProperty => {
                    row.insert(
                        "type",
                        QueryValue::Scalar(Value::String("node".to_string())),
                    );
                    row.insert("dimensions", QueryValue::Scalar(Value::Null));
                    row.insert("metric", QueryValue::Scalar(Value::Null));
                }
                IndexKind::UniqueNodeProperty => {
                    row.insert(
                        "type",
                        QueryValue::Scalar(Value::String("unique_node".to_string())),
                    );
                    row.insert("dimensions", QueryValue::Scalar(Value::Null));
                    row.insert("metric", QueryValue::Scalar(Value::Null));
                }
                IndexKind::Vector { dimensions, metric } => {
                    row.insert(
                        "type",
                        QueryValue::Scalar(Value::String("vector".to_string())),
                    );
                    row.insert(
                        "dimensions",
                        QueryValue::Scalar(Value::Int(*dimensions as i64)),
                    );
                    row.insert("metric", QueryValue::Scalar(Value::String(metric.clone())));
                }
            }
            row
        })
        .collect()
}

fn format_vector_index_rows(indexes: &[IndexDefinition]) -> Vec<QueryRow> {
    format_index_rows(
        &indexes
            .iter()
            .filter(|index| matches!(index.kind, IndexKind::Vector { .. }))
            .cloned()
            .collect::<Vec<_>>(),
    )
}

fn format_vector_index_status_rows(statuses: &[VectorIndexStatus]) -> Vec<QueryRow> {
    statuses
        .iter()
        .map(|status| {
            let mut row = QueryRow::new();
            row.insert(
                "name",
                QueryValue::Scalar(Value::String(status.name.clone())),
            );
            row.insert(
                "label",
                QueryValue::Scalar(Value::String(status.label.clone())),
            );
            row.insert(
                "property",
                QueryValue::Scalar(Value::String(status.property.clone())),
            );
            row.insert(
                "dimensions",
                QueryValue::Scalar(Value::Int(status.dimensions as i64)),
            );
            row.insert(
                "metric",
                QueryValue::Scalar(Value::String(status.metric.clone())),
            );
            row.insert(
                "entries",
                QueryValue::Scalar(Value::Int(status.entries as i64)),
            );
            row
        })
        .collect()
}

fn format_index_row_by_name(indexes: &[IndexDefinition], name: &str) -> DatabaseResult<QueryRow> {
    let Some(index) = indexes.iter().find(|index| index.name == name) else {
        return Err(DatabaseError::InvalidConfig(format!(
            "index {name:?} does not exist"
        )));
    };
    Ok(format_index_rows(std::slice::from_ref(index))
        .into_iter()
        .next()
        .expect("one index row"))
}

fn format_vector_index_row_by_name(
    indexes: &[IndexDefinition],
    name: &str,
) -> DatabaseResult<QueryRow> {
    let Some(index) = indexes.iter().find(|index| index.name == name) else {
        return Err(DatabaseError::InvalidConfig(format!(
            "vector index {name:?} does not exist"
        )));
    };
    if !matches!(index.kind, IndexKind::Vector { .. }) {
        return Err(DatabaseError::InvalidConfig(format!(
            "index {name:?} is not a vector index"
        )));
    }
    Ok(format_index_rows(std::slice::from_ref(index))
        .into_iter()
        .next()
        .expect("one index row"))
}

fn format_constraint_rows(indexes: &[IndexDefinition]) -> Vec<QueryRow> {
    indexes
        .iter()
        .filter(|index| matches!(index.kind, IndexKind::UniqueNodeProperty))
        .map(|index| {
            let mut row = QueryRow::new();
            row.insert(
                "name",
                QueryValue::Scalar(Value::String(index.name.clone())),
            );
            row.insert(
                "type",
                QueryValue::Scalar(Value::String("unique_node_property".to_string())),
            );
            row.insert(
                "label",
                QueryValue::Scalar(Value::String(index.label.clone())),
            );
            row.insert(
                "property",
                QueryValue::Scalar(Value::String(index.property.clone())),
            );
            row
        })
        .collect()
}

fn format_constraint_row_by_name(
    indexes: &[IndexDefinition],
    name: &str,
) -> DatabaseResult<QueryRow> {
    let Some(index) = indexes.iter().find(|index| index.name == name) else {
        return Err(DatabaseError::InvalidConfig(format!(
            "constraint {name:?} does not exist"
        )));
    };
    if !matches!(index.kind, IndexKind::UniqueNodeProperty) {
        return Err(DatabaseError::InvalidConfig(format!(
            "index {name:?} is not a constraint"
        )));
    }
    Ok(format_constraint_rows(std::slice::from_ref(index))
        .into_iter()
        .next()
        .expect("one constraint row"))
}

fn parse_create_index_ddl(input: &str) -> DatabaseResult<WriteCypher> {
    let body = strip_keyword(input, "CREATE INDEX")?;
    let (name, target) = split_first_token(body, "CREATE INDEX requires index name")?;
    validate_identifier_write(name)?;
    let (target, if_not_exists) = strip_if_not_exists_prefix(target);
    let (label, property) = parse_index_target(target)?;
    Ok(WriteCypher::CreateNodePropertyIndex {
        name: name.to_string(),
        label,
        property,
        if_not_exists,
    })
}

fn parse_create_unique_node_property_constraint_ddl(input: &str) -> DatabaseResult<WriteCypher> {
    let body = strip_keyword(input, "CREATE CONSTRAINT")?;
    let (name, target) = split_first_token(body, "CREATE CONSTRAINT requires constraint name")?;
    validate_identifier_write(name)?;
    let (target, if_not_exists) = strip_if_not_exists_prefix(target);
    let (for_part, require_part) = split_keyword(target, "REQUIRE").ok_or_else(|| {
        write_parse_error("CREATE CONSTRAINT requires REQUIRE variable.property IS UNIQUE")
    })?;
    let node =
        parse_node_pattern_write(strip_keyword(for_part.trim(), "FOR")?, &QueryParams::new())?;
    ensure_write_parse(
        node.labels.len() == 1 && node.properties.is_empty(),
        "constraint target node pattern must contain one label and no properties",
    )?;
    let (property_ref, unique_part) = split_keyword(require_part, "IS")
        .ok_or_else(|| write_parse_error("CREATE CONSTRAINT requires IS UNIQUE"))?;
    ensure_write_parse(
        unique_part.trim().eq_ignore_ascii_case("UNIQUE"),
        "CREATE CONSTRAINT only supports IS UNIQUE",
    )?;
    let (variable, property) = parse_property_ref_write(property_ref)?;
    ensure_write_parse(
        variable == node.variable,
        "constraint property variable must match the target node variable",
    )?;
    Ok(WriteCypher::CreateUniqueNodePropertyConstraint {
        name: name.to_string(),
        label: node.labels[0].clone(),
        property,
        if_not_exists,
    })
}

fn parse_create_vector_index_ddl(input: &str) -> DatabaseResult<WriteCypher> {
    let body = strip_keyword(input, "CREATE VECTOR INDEX")?;
    let (name, rest) = split_first_token(body, "CREATE VECTOR INDEX requires index name")?;
    validate_identifier_write(name)?;
    let (rest, if_not_exists) = strip_if_not_exists_prefix(rest);
    let (target, dimensions_part) = split_keyword(rest, "DIMENSIONS")
        .ok_or_else(|| write_parse_error("CREATE VECTOR INDEX requires DIMENSIONS"))?;
    let (dimensions, metric_part) = split_first_token(
        dimensions_part,
        "CREATE VECTOR INDEX requires vector dimensions",
    )?;
    let dimensions = dimensions
        .parse::<usize>()
        .map_err(|_| write_parse_error("vector dimensions must be an unsigned integer"))?;
    let metric = strip_keyword(metric_part, "METRIC")?.trim();
    validate_identifier_write(metric)?;
    let (label, property) = parse_index_target(target)?;
    Ok(WriteCypher::CreateVectorIndex {
        name: name.to_string(),
        label,
        property,
        dimensions,
        metric: metric.to_string(),
        if_not_exists,
    })
}

fn parse_drop_index_ddl(input: &str) -> DatabaseResult<WriteCypher> {
    let body = strip_keyword(input, "DROP INDEX")?;
    let (name, if_exists) = strip_if_exists_suffix(body);
    validate_identifier_write(name)?;
    Ok(WriteCypher::DropIndex {
        name: name.to_string(),
        if_exists,
    })
}

fn parse_drop_constraint_ddl(input: &str) -> DatabaseResult<WriteCypher> {
    let body = strip_keyword(input, "DROP CONSTRAINT")?;
    let (name, if_exists) = strip_if_exists_suffix(body);
    validate_identifier_write(name)?;
    Ok(WriteCypher::DropConstraint {
        name: name.to_string(),
        if_exists,
    })
}

fn parse_rebuild_vector_index_ddl(input: &str) -> DatabaseResult<WriteCypher> {
    let body = strip_keyword(input, "REBUILD VECTOR INDEX")?;
    ensure_write_parse(
        !body.contains(char::is_whitespace),
        "REBUILD VECTOR INDEX requires a single index name",
    )?;
    validate_identifier_write(body)?;
    Ok(WriteCypher::RebuildVectorIndex {
        name: body.to_string(),
    })
}

fn strip_if_exists_suffix(input: &str) -> (&str, bool) {
    let input = input.trim();
    match strip_keyword_suffix(input, "IF EXISTS") {
        Some(name) => (name.trim(), true),
        None => (input, false),
    }
}

fn strip_if_not_exists_prefix(input: &str) -> (&str, bool) {
    match strip_keyword(input.trim(), "IF NOT EXISTS") {
        Ok(rest) => (rest.trim(), true),
        Err(_) => (input.trim(), false),
    }
}

fn split_first_token<'a>(input: &'a str, missing: &str) -> DatabaseResult<(&'a str, &'a str)> {
    let input = input.trim();
    let Some(index) = input.find(char::is_whitespace) else {
        return Err(write_parse_error(missing));
    };
    let head = input[..index].trim();
    let rest = input[index..].trim();
    ensure_write_parse(!head.is_empty() && !rest.is_empty(), missing)?;
    Ok((head, rest))
}

fn parse_index_target(input: &str) -> DatabaseResult<(String, String)> {
    let input = input.trim();
    if starts_with_keyword(input, "FOR") {
        return parse_for_on_index_target(input);
    }
    let target = strip_keyword(input, "ON")?;
    parse_legacy_index_target(target)
}

fn parse_for_on_index_target(input: &str) -> DatabaseResult<(String, String)> {
    let body = strip_keyword(input, "FOR")?;
    let (pattern, on_part) =
        split_keyword(body, "ON").ok_or_else(|| write_parse_error("index target requires ON"))?;
    let node = parse_node_pattern_write(pattern.trim(), &QueryParams::new())?;
    ensure_write_parse(
        node.labels.len() == 1 && node.properties.is_empty(),
        "index target node pattern must contain one label and no properties",
    )?;
    let property_ref = strip_wrapping_write(on_part.trim(), '(', ')')?;
    let (variable, property) = parse_property_ref_write(property_ref)?;
    ensure_write_parse(
        variable == node.variable,
        "index property variable must match the indexed node variable",
    )?;
    Ok((node.labels[0].clone(), property))
}

fn parse_legacy_index_target(input: &str) -> DatabaseResult<(String, String)> {
    let input = input.trim();
    let label_start = input
        .strip_prefix(':')
        .ok_or_else(|| write_parse_error("index target must start with :Label(property)"))?;
    let open = label_start
        .find('(')
        .ok_or_else(|| write_parse_error("index target requires property parentheses"))?;
    let close = label_start
        .rfind(')')
        .ok_or_else(|| write_parse_error("index target requires property parentheses"))?;
    ensure_write_parse(
        close == label_start.len() - 1 && open < close,
        "index target must end after property parentheses",
    )?;
    let label = label_start[..open].trim();
    let property = label_start[open + 1..close].trim();
    validate_identifier_write(label)?;
    validate_identifier_write(property)?;
    Ok((label.to_string(), property.to_string()))
}

fn parse_create_node_write(input: &str, params: &QueryParams) -> DatabaseResult<WriteCypher> {
    let body = strip_keyword(input, "CREATE")?;
    let (body, returns) = parse_optional_write_return(body)?;
    let (pattern, set_part) = match split_keyword(body, "SET") {
        Some((pattern, set_part)) => (pattern.trim(), Some(set_part.trim())),
        None => (body.trim(), None),
    };
    let NodePatternWrite {
        variable,
        labels,
        properties,
    } = parse_create_node_pattern_write(pattern, params)?;
    let replacement = match set_part {
        Some(set_part) => parse_property_replacement(
            set_part,
            &variable,
            params,
            "CREATE SET replacement variable must match the created variable",
        )?,
        None => None,
    };
    let assignments = match (set_part, replacement.as_ref()) {
        (Some(set_part), None) => parse_set_assignments(
            set_part,
            &variable,
            params,
            "CREATE SET variable must match the created variable",
        )?,
        _ => Vec::new(),
    };
    ensure_write_return_matches(returns.as_ref(), &variable, "CREATE RETURN")?;
    Ok(WriteCypher::CreateNode {
        variable,
        labels,
        properties,
        assignments,
        replacement,
        returns,
    })
}

fn parse_create_node_pattern_write(
    input: &str,
    params: &QueryParams,
) -> DatabaseResult<NodePatternWrite> {
    let inner = strip_wrapping_write(input.trim(), '(', ')')?;
    let (head, properties) = match top_level_brace_start(inner) {
        Some(index) => {
            let head = inner[..index].trim();
            let properties = parse_property_map(&inner[index..], params)?;
            (head, properties)
        }
        None => (inner.trim(), Properties::new()),
    };
    if !head.starts_with(':') && !head.is_empty() {
        return parse_node_pattern_write(input, params);
    }
    let labels = head
        .trim_start_matches(':')
        .split(':')
        .map(str::trim)
        .filter(|label| !label.is_empty())
        .map(|label| {
            validate_identifier_write(label)?;
            Ok(label.to_string())
        })
        .collect::<DatabaseResult<Vec<_>>>()?;
    Ok(NodePatternWrite {
        variable: "__neo4r_anonymous_node".to_string(),
        labels,
        properties,
    })
}

fn parse_merge_node_write(input: &str, params: &QueryParams) -> DatabaseResult<WriteCypher> {
    let body = strip_keyword(input, "MERGE")?;
    let (body, returns) = parse_optional_write_return(body)?;
    let NodePatternWrite {
        variable,
        labels,
        properties,
    } = parse_create_node_pattern_write(parse_merge_pattern_part(body)?, params)?;
    let clauses = parse_merge_set_clauses(body, &variable, params)?;
    ensure_write_return_matches(returns.as_ref(), &variable, "MERGE RETURN")?;
    Ok(WriteCypher::MergeNode {
        labels,
        properties,
        on_create: clauses.on_create,
        on_create_replacement: clauses.on_create_replacement,
        on_match: clauses.on_match,
        on_match_replacement: clauses.on_match_replacement,
        returns,
    })
}

fn parse_set_node_property(
    match_part: &str,
    set_part: &str,
    params: &QueryParams,
) -> DatabaseResult<WriteCypher> {
    let matcher = parse_node_matcher_body(strip_keyword(match_part.trim(), "MATCH")?, params)?;
    let (set_part, returns) = parse_optional_write_return(set_part)?;
    if let Some(properties) = parse_property_replacement(
        set_part,
        &matcher.variable,
        params,
        "SET replacement variable must match the MATCH variable",
    )? {
        ensure_write_return_matches(returns.as_ref(), &matcher.variable, "SET RETURN")?;
        return Ok(WriteCypher::ReplaceNodeProperties {
            matcher,
            properties,
            returns,
        });
    }
    let assignments = parse_set_assignments(
        set_part,
        &matcher.variable,
        params,
        "SET variable must match the MATCH variable",
    )?;
    ensure_write_return_matches(returns.as_ref(), &matcher.variable, "SET RETURN")?;
    Ok(WriteCypher::SetNodeProperty {
        matcher,
        assignments,
        returns,
    })
}

fn parse_create_relationship_write(
    match_part: &str,
    create_part: &str,
    params: &QueryParams,
) -> DatabaseResult<WriteCypher> {
    let (from_matcher, to_matcher) = parse_relationship_endpoint_matchers(
        match_part,
        params,
        "relationship CREATE requires two MATCH node patterns",
    )?;
    let (pattern, returns) = parse_optional_write_return(create_part)?;
    let (pattern, set_part) = match split_keyword(pattern, "SET") {
        Some((pattern, set_part)) => (pattern.trim(), Some(set_part.trim())),
        None => (pattern.trim(), None),
    };
    let RelationshipPatternWrite {
        variable,
        from_variable,
        to_variable,
        rel_type,
        properties,
    } = parse_relationship_pattern_write(pattern, params)?;
    let replacement = match set_part {
        Some(set_part) => parse_property_replacement(
            set_part,
            &variable,
            params,
            "relationship CREATE SET replacement variable must match the created relationship variable",
        )?,
        None => None,
    };
    let assignments = match (set_part, replacement.as_ref()) {
        (Some(set_part), None) => parse_set_assignments(
            set_part,
            &variable,
            params,
            "relationship CREATE SET variable must match the created relationship variable",
        )?,
        _ => Vec::new(),
    };
    ensure_write_parse(
        from_variable == from_matcher.variable,
        "relationship CREATE source variable must match the first MATCH variable",
    )?;
    ensure_write_parse(
        to_variable == to_matcher.variable,
        "relationship CREATE target variable must match the second MATCH variable",
    )?;
    ensure_write_return_matches(returns.as_ref(), &variable, "relationship CREATE RETURN")?;
    Ok(WriteCypher::CreateRelationship {
        variable,
        from_matcher,
        to_matcher,
        rel_type,
        properties,
        assignments,
        replacement,
        returns,
    })
}

fn parse_merge_relationship_write(
    match_part: &str,
    merge_part: &str,
    params: &QueryParams,
) -> DatabaseResult<WriteCypher> {
    let (from_matcher, to_matcher) = parse_relationship_endpoint_matchers(
        match_part,
        params,
        "relationship MERGE requires two MATCH node patterns",
    )?;
    let (merge_part, returns) = parse_optional_write_return(merge_part)?;
    let RelationshipPatternWrite {
        variable,
        from_variable,
        to_variable,
        rel_type,
        properties,
    } = parse_relationship_pattern_write(parse_merge_pattern_part(merge_part)?, params)?;
    let clauses = parse_merge_set_clauses(merge_part, &variable, params)?;
    ensure_write_parse(
        from_variable == from_matcher.variable,
        "relationship MERGE source variable must match the first MATCH variable",
    )?;
    ensure_write_parse(
        to_variable == to_matcher.variable,
        "relationship MERGE target variable must match the second MATCH variable",
    )?;
    ensure_write_return_matches(returns.as_ref(), &variable, "relationship MERGE RETURN")?;
    Ok(WriteCypher::MergeRelationship {
        from_matcher,
        to_matcher,
        rel_type,
        properties,
        on_create: clauses.on_create,
        on_create_replacement: clauses.on_create_replacement,
        on_match: clauses.on_match,
        on_match_replacement: clauses.on_match_replacement,
        returns,
    })
}

fn parse_relationship_endpoint_matchers(
    match_part: &str,
    params: &QueryParams,
    missing_message: &str,
) -> DatabaseResult<(NodeMatcher, NodeMatcher)> {
    let body = strip_keyword(match_part.trim(), "MATCH")?;
    if let Some((left_match, right_match)) = split_keyword(body, "MATCH") {
        return Ok((
            parse_node_matcher_body(left_match.trim(), params)?,
            parse_node_matcher_body(right_match.trim(), params)?,
        ));
    }

    let patterns = split_top_level_commas(body)?;
    ensure_write_parse(patterns.len() == 2, missing_message)?;
    Ok((
        parse_node_matcher_body(patterns[0], params)?,
        parse_node_matcher_body(patterns[1], params)?,
    ))
}

fn parse_set_property(
    match_part: &str,
    set_part: &str,
    params: &QueryParams,
) -> DatabaseResult<WriteCypher> {
    if match_part.contains("->") {
        return parse_set_relationship_property(match_part, set_part, params);
    }
    if !set_part.contains('=') {
        return parse_add_node_label(match_part, set_part, params);
    }
    parse_set_node_property(match_part, set_part, params)
}

fn parse_remove_property(
    match_part: &str,
    remove_part: &str,
    params: &QueryParams,
) -> DatabaseResult<WriteCypher> {
    if match_part.contains("->") {
        return parse_remove_relationship_property(match_part, remove_part, params);
    }
    if remove_part.contains(':') {
        return parse_remove_node_label(match_part, remove_part, params);
    }
    parse_remove_node_property(match_part, remove_part, params)
}

fn parse_remove_node_property(
    match_part: &str,
    remove_part: &str,
    params: &QueryParams,
) -> DatabaseResult<WriteCypher> {
    let matcher = parse_node_matcher_body(strip_keyword(match_part.trim(), "MATCH")?, params)?;
    let (remove_part, returns) = parse_optional_write_return(remove_part)?;
    let keys = parse_remove_keys(
        remove_part,
        &matcher.variable,
        "REMOVE variable must match the MATCH variable",
    )?;
    ensure_write_return_matches(returns.as_ref(), &matcher.variable, "REMOVE RETURN")?;
    Ok(WriteCypher::RemoveNodeProperty {
        matcher,
        keys,
        returns,
    })
}

fn parse_add_node_label(
    match_part: &str,
    set_part: &str,
    params: &QueryParams,
) -> DatabaseResult<WriteCypher> {
    let matcher = parse_node_matcher_body(strip_keyword(match_part.trim(), "MATCH")?, params)?;
    let (set_part, returns) = parse_optional_write_return(set_part)?;
    let labels = parse_label_refs(
        set_part,
        &matcher.variable,
        "SET label variable must match the MATCH variable",
    )?;
    ensure_write_return_matches(returns.as_ref(), &matcher.variable, "SET RETURN")?;
    Ok(WriteCypher::AddNodeLabel {
        matcher,
        labels,
        returns,
    })
}

fn parse_remove_node_label(
    match_part: &str,
    remove_part: &str,
    params: &QueryParams,
) -> DatabaseResult<WriteCypher> {
    let matcher = parse_node_matcher_body(strip_keyword(match_part.trim(), "MATCH")?, params)?;
    let (remove_part, returns) = parse_optional_write_return(remove_part)?;
    let labels = parse_label_refs(
        remove_part,
        &matcher.variable,
        "REMOVE label variable must match the MATCH variable",
    )?;
    ensure_write_return_matches(returns.as_ref(), &matcher.variable, "REMOVE RETURN")?;
    Ok(WriteCypher::RemoveNodeLabel {
        matcher,
        labels,
        returns,
    })
}

fn parse_remove_relationship_property(
    match_part: &str,
    remove_part: &str,
    params: &QueryParams,
) -> DatabaseResult<WriteCypher> {
    let matcher = parse_relationship_matcher(match_part, params)?;
    let (remove_part, returns) = parse_optional_write_return(remove_part)?;
    let keys = parse_remove_keys(
        remove_part,
        &matcher.variable,
        "REMOVE variable must match the MATCH relationship variable",
    )?;
    ensure_write_return_matches(returns.as_ref(), &matcher.variable, "REMOVE RETURN")?;
    Ok(WriteCypher::RemoveRelationshipProperty {
        matcher,
        keys,
        returns,
    })
}

fn parse_set_relationship_property(
    match_part: &str,
    set_part: &str,
    params: &QueryParams,
) -> DatabaseResult<WriteCypher> {
    let matcher = parse_relationship_matcher(match_part, params)?;
    let (set_part, returns) = parse_optional_write_return(set_part)?;
    if let Some(properties) = parse_property_replacement(
        set_part,
        &matcher.variable,
        params,
        "SET replacement variable must match the MATCH relationship variable",
    )? {
        ensure_write_return_matches(returns.as_ref(), &matcher.variable, "SET RETURN")?;
        return Ok(WriteCypher::ReplaceRelationshipProperties {
            matcher,
            properties,
            returns,
        });
    }
    let assignments = parse_set_assignments(
        set_part,
        &matcher.variable,
        params,
        "SET variable must match the MATCH relationship variable",
    )?;
    ensure_write_return_matches(returns.as_ref(), &matcher.variable, "SET RETURN")?;
    Ok(WriteCypher::SetRelationshipProperty {
        matcher,
        assignments,
        returns,
    })
}

fn parse_property_replacement(
    set_part: &str,
    expected_variable: &str,
    params: &QueryParams,
    variable_mismatch_message: &str,
) -> DatabaseResult<Option<Properties>> {
    let entries = split_top_level_commas(set_part.trim())?;
    if entries.len() != 1 {
        return Ok(None);
    }
    let entry = entries[0];
    if entry.contains("+=") {
        return Ok(None);
    }
    let Some((left, right)) = entry.split_once('=') else {
        return Ok(None);
    };
    let variable = left.trim();
    if variable.contains('.') {
        return Ok(None);
    }
    validate_identifier_write(variable)?;
    ensure_write_parse(variable == expected_variable, variable_mismatch_message)?;
    Ok(Some(parse_property_map(right.trim(), params)?))
}

fn parse_set_assignments(
    set_part: &str,
    expected_variable: &str,
    params: &QueryParams,
    variable_mismatch_message: &str,
) -> DatabaseResult<Vec<PropertyAssignment>> {
    let mut assignments = Vec::new();
    for assignment in split_top_level_commas(set_part.trim())? {
        if let Some((left, right)) = assignment.split_once("+=") {
            let variable = left.trim();
            validate_identifier_write(variable)?;
            ensure_write_parse(variable == expected_variable, variable_mismatch_message)?;
            let properties = parse_property_map(right.trim(), params)?;
            ensure_write_parse(
                !properties.is_empty(),
                "SET += requires at least one property",
            )?;
            assignments.extend(
                properties
                    .into_iter()
                    .map(|(key, value)| PropertyAssignment { key, value }),
            );
            continue;
        }
        let (left, right) = assignment
            .split_once('=')
            .ok_or_else(|| write_parse_error("SET must use variable.property = value"))?;
        let (variable, key) = parse_property_ref_write(left.trim())?;
        ensure_write_parse(variable == expected_variable, variable_mismatch_message)?;
        assignments.push(PropertyAssignment {
            key,
            value: parse_write_property_value(right.trim(), params)?,
        });
    }
    ensure_write_parse(
        !assignments.is_empty(),
        "SET requires at least one assignment",
    )?;
    Ok(assignments)
}

fn parse_merge_pattern_part(input: &str) -> DatabaseResult<&str> {
    let create_index = find_keyword(input, "ON CREATE SET");
    let match_index = find_keyword(input, "ON MATCH SET");
    let pattern_end = [create_index, match_index]
        .into_iter()
        .flatten()
        .min()
        .unwrap_or(input.len());
    let pattern = input[..pattern_end].trim();
    ensure_write_parse(!pattern.is_empty(), "MERGE requires a pattern")?;
    Ok(pattern)
}

fn parse_merge_set_clauses(
    input: &str,
    expected_variable: &str,
    params: &QueryParams,
) -> DatabaseResult<MergeSetClauses> {
    let mut clauses = Vec::new();
    if let Some(index) = find_keyword(input, "ON CREATE SET") {
        clauses.push((index, "ON CREATE SET"));
    }
    if let Some(index) = find_keyword(input, "ON MATCH SET") {
        clauses.push((index, "ON MATCH SET"));
    }
    clauses.sort_by_key(|(index, _)| *index);

    let mut parsed = MergeSetClauses::default();
    for (position, (index, keyword)) in clauses.iter().enumerate() {
        let start = index + keyword.len();
        let end = clauses
            .get(position + 1)
            .map(|(next_index, _)| *next_index)
            .unwrap_or(input.len());
        let set_part = &input[start..end];
        let replacement = parse_property_replacement(
            set_part,
            expected_variable,
            params,
            "MERGE SET replacement variable must match the MERGE variable",
        )?;
        let assignments = match replacement.as_ref() {
            Some(_) => Vec::new(),
            None => parse_set_assignments(
                set_part,
                expected_variable,
                params,
                "MERGE SET variable must match the MERGE variable",
            )?,
        };
        match *keyword {
            "ON CREATE SET" => {
                ensure_write_parse(
                    parsed.on_create.is_empty() && parsed.on_create_replacement.is_none(),
                    "duplicate ON CREATE SET clause",
                )?;
                parsed.on_create = assignments;
                parsed.on_create_replacement = replacement;
            }
            "ON MATCH SET" => {
                ensure_write_parse(
                    parsed.on_match.is_empty() && parsed.on_match_replacement.is_none(),
                    "duplicate ON MATCH SET clause",
                )?;
                parsed.on_match = assignments;
                parsed.on_match_replacement = replacement;
            }
            _ => unreachable!(),
        }
    }
    Ok(parsed)
}

fn parse_remove_keys(
    remove_part: &str,
    expected_variable: &str,
    variable_mismatch_message: &str,
) -> DatabaseResult<Vec<String>> {
    let mut keys = Vec::new();
    for property_ref in split_top_level_commas(remove_part.trim())? {
        let (variable, key) = parse_property_ref_write(property_ref)?;
        ensure_write_parse(variable == expected_variable, variable_mismatch_message)?;
        keys.push(key);
    }
    ensure_write_parse(!keys.is_empty(), "REMOVE requires at least one property")?;
    Ok(keys)
}

fn parse_label_refs(
    input: &str,
    expected_variable: &str,
    variable_mismatch_message: &str,
) -> DatabaseResult<Vec<String>> {
    let mut labels = Vec::new();
    for label_ref in split_top_level_commas(input.trim())? {
        let (variable, label_part) = label_ref
            .split_once(':')
            .ok_or_else(|| write_parse_error("label update must use variable:Label"))?;
        ensure_write_parse(
            variable.trim() == expected_variable,
            variable_mismatch_message,
        )?;
        for label in label_part.split(':') {
            let label = label.trim();
            validate_identifier_write(label)?;
            labels.push(label.to_string());
        }
    }
    ensure_write_parse(
        !labels.is_empty(),
        "label update requires at least one label",
    )?;
    labels.sort();
    labels.dedup();
    Ok(labels)
}

fn parse_delete(
    match_part: &str,
    delete_part: &str,
    params: &QueryParams,
) -> DatabaseResult<WriteCypher> {
    if match_part.contains("->") {
        return parse_delete_relationship(match_part, delete_part, params);
    }
    parse_delete_node(match_part, delete_part, params)
}

fn parse_delete_node(
    match_part: &str,
    delete_part: &str,
    params: &QueryParams,
) -> DatabaseResult<WriteCypher> {
    let matcher = parse_node_matcher(match_part, params)?;
    let (delete_part, returns) = parse_optional_write_return(delete_part)?;
    let variable = parse_return_variable(delete_part)?;
    ensure_write_parse(
        variable == matcher.variable,
        "DELETE variable must match the MATCH variable",
    )?;
    ensure_write_return_matches(returns.as_ref(), &matcher.variable, "DELETE RETURN")?;
    Ok(WriteCypher::DeleteNode { matcher, returns })
}

fn parse_node_matcher(input: &str, params: &QueryParams) -> DatabaseResult<NodeMatcher> {
    parse_node_matcher_body(strip_keyword(input.trim(), "MATCH")?.trim(), params)
}

fn parse_node_matcher_body(input: &str, params: &QueryParams) -> DatabaseResult<NodeMatcher> {
    let match_body = input.trim();
    let (pattern, predicate) = match split_keyword(match_body, "WHERE") {
        Some((pattern, predicate)) => (pattern.trim(), Some(predicate.trim())),
        None => (match_body, None),
    };
    let NodePatternWrite {
        variable,
        properties,
        ..
    } = parse_node_pattern_write(pattern, params)?;
    let match_pattern = strip_node_pattern_properties(pattern)?;
    let match_query = if let Some(predicate) = predicate {
        ensure_write_parse(
            properties.is_empty(),
            "MATCH pattern properties cannot be combined with WHERE",
        )?;
        format!("MATCH {match_pattern} WHERE {predicate} RETURN {variable}")
    } else if properties.is_empty() {
        format!("MATCH {match_pattern} RETURN {variable}")
    } else {
        ensure_write_parse(
            properties.len() == 1,
            "MATCH pattern property lookup supports one property",
        )?;
        let (key, value) = properties.iter().next().unwrap();
        format!(
            "MATCH {match_pattern} WHERE {variable}.{key} = {} RETURN {variable}",
            write_value_literal(value)?
        )
    };
    Ok(NodeMatcher {
        variable,
        match_query,
    })
}

fn parse_delete_relationship(
    match_part: &str,
    delete_part: &str,
    params: &QueryParams,
) -> DatabaseResult<WriteCypher> {
    let matcher = parse_relationship_matcher(match_part, params)?;
    let (delete_part, returns) = parse_optional_write_return(delete_part)?;
    let variable = parse_return_variable(delete_part)?;
    ensure_write_parse(
        variable == matcher.variable,
        "DELETE variable must match the MATCH relationship variable",
    )?;
    ensure_write_return_matches(returns.as_ref(), &matcher.variable, "DELETE RETURN")?;
    Ok(WriteCypher::DeleteRelationship { matcher, returns })
}

fn parse_relationship_matcher(
    input: &str,
    params: &QueryParams,
) -> DatabaseResult<RelationshipMatcher> {
    let match_body = strip_keyword(input.trim(), "MATCH")?.trim();
    let (pattern, predicate) = match split_keyword(match_body, "WHERE") {
        Some((pattern, predicate)) => (pattern.trim(), Some(predicate.trim())),
        None => (match_body, None),
    };
    let relationship = parse_relationship_pattern_write(pattern, params)?;
    let predicate = relationship_matcher_predicate(&relationship, predicate)?;
    Ok(RelationshipMatcher {
        variable: relationship.variable.clone(),
        match_query: format!(
            "MATCH {}{} RETURN {}",
            strip_relationship_properties(pattern)?,
            predicate,
            relationship.variable
        ),
    })
}

fn relationship_matcher_predicate(
    relationship: &RelationshipPatternWrite,
    explicit_predicate: Option<&str>,
) -> DatabaseResult<String> {
    let mut predicates = relationship
        .properties
        .iter()
        .map(|(key, value)| {
            Ok(format!(
                "{}.{key} = {}",
                relationship.variable,
                write_value_literal(value)?
            ))
        })
        .collect::<DatabaseResult<Vec<_>>>()?;
    predicates.sort();
    if let Some(predicate) = explicit_predicate {
        predicates.push(predicate.trim().to_string());
    }
    if predicates.is_empty() {
        Ok(String::new())
    } else {
        Ok(format!(" WHERE {}", predicates.join(" AND ")))
    }
}

struct NodePatternWrite {
    variable: String,
    labels: Vec<String>,
    properties: Properties,
}

struct RelationshipPatternWrite {
    variable: String,
    from_variable: String,
    to_variable: String,
    rel_type: String,
    properties: Properties,
}

fn parse_node_pattern_write(input: &str, params: &QueryParams) -> DatabaseResult<NodePatternWrite> {
    let inner = strip_wrapping_write(input.trim(), '(', ')')?;
    let (head, properties) = match top_level_brace_start(inner) {
        Some(index) => {
            let head = inner[..index].trim();
            let properties = parse_property_map(&inner[index..], params)?;
            (head, properties)
        }
        None => (inner.trim(), Properties::new()),
    };
    let mut parts = head.split(':').map(str::trim);
    let variable = parts
        .next()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| write_parse_error("node pattern requires a variable"))?;
    validate_identifier_write(variable)?;
    let labels = parts
        .map(|label| {
            validate_identifier_write(label)?;
            Ok(label.to_string())
        })
        .collect::<DatabaseResult<Vec<_>>>()?;
    Ok(NodePatternWrite {
        variable: variable.to_string(),
        labels,
        properties,
    })
}

fn parse_relationship_pattern_write(
    input: &str,
    params: &QueryParams,
) -> DatabaseResult<RelationshipPatternWrite> {
    let compact = input
        .chars()
        .filter(|ch| !ch.is_whitespace())
        .collect::<String>();
    let (left, to_part) = compact
        .split_once("->")
        .ok_or_else(|| write_parse_error("relationship pattern must use ->"))?;
    let (from_part, rel_part) = left
        .split_once("-")
        .ok_or_else(|| write_parse_error("relationship pattern must contain -[r:TYPE]->"))?;
    let from_variable = parse_node_pattern_write(from_part, &QueryParams::new())?.variable;
    let to_variable = parse_node_pattern_write(to_part, &QueryParams::new())?.variable;
    let inner = strip_wrapping_write(rel_part, '[', ']')?;
    let (head, properties) = match top_level_brace_start(inner) {
        Some(index) => {
            let head = inner[..index].trim();
            let properties = parse_property_map(&inner[index..], params)?;
            (head, properties)
        }
        None => (inner.trim(), Properties::new()),
    };
    let (variable, rel_type) = head
        .split_once(':')
        .ok_or_else(|| write_parse_error("relationship pattern requires variable:type or :type"))?;
    if !variable.is_empty() {
        validate_identifier_write(variable)?;
    }
    validate_identifier_write(rel_type)?;
    Ok(RelationshipPatternWrite {
        variable: variable.to_string(),
        from_variable,
        to_variable,
        rel_type: rel_type.to_string(),
        properties,
    })
}

fn parse_property_map(input: &str, params: &QueryParams) -> DatabaseResult<Properties> {
    if let Some(name) = input.trim().strip_prefix('$') {
        validate_identifier_write(name)?;
        return match params.get(name) {
            Some(Value::Map(properties)) => {
                validate_property_map_values(properties)?;
                Ok(properties.clone())
            }
            Some(value) => Err(write_parse_error(&format!(
                "query parameter ${name} must be a property map, got {value:?}"
            ))),
            None => Err(write_parse_error(&format!(
                "missing query parameter ${name}"
            ))),
        };
    }
    let inner = strip_wrapping_write(input.trim(), '{', '}')?;
    if inner.trim().is_empty() {
        return Ok(Properties::new());
    }
    let mut properties = Properties::new();
    for entry in split_top_level_commas(inner)? {
        let (key, value) = entry
            .split_once(':')
            .ok_or_else(|| write_parse_error("property map entries must use key: value"))?;
        let key = key.trim();
        validate_identifier_write(key)?;
        properties.insert(
            key.to_string(),
            parse_write_property_value(value.trim(), params)?,
        );
    }
    Ok(properties)
}

fn parse_write_property_value(input: &str, params: &QueryParams) -> DatabaseResult<Value> {
    let value = parse_write_value(input, params)?;
    ensure_storable_property_value(&value)?;
    Ok(value)
}

fn validate_property_map_values(properties: &Properties) -> DatabaseResult<()> {
    for value in properties.values() {
        ensure_storable_property_value(value)?;
    }
    Ok(())
}

fn ensure_storable_property_value(value: &Value) -> DatabaseResult<()> {
    ensure_write_parse(
        !matches!(value, Value::Map(_)),
        "graph properties do not support nested map values",
    )
}

fn validate_storable_properties(properties: &Properties) -> DatabaseResult<()> {
    for value in properties.values() {
        validate_storable_property_value(value)?;
    }
    Ok(())
}

fn validate_storable_property_value(value: &Value) -> DatabaseResult<()> {
    if matches!(value, Value::Map(_)) {
        return Err(DatabaseError::InvalidConfig(
            "graph properties do not support nested map values".to_string(),
        ));
    }
    Ok(())
}

fn parse_write_value(input: &str, params: &QueryParams) -> DatabaseResult<Value> {
    if let Some(name) = input.strip_prefix('$') {
        validate_identifier_write(name)?;
        return params
            .get(name)
            .cloned()
            .ok_or_else(|| write_parse_error(&format!("missing query parameter ${name}")));
    }
    if input.starts_with('[') {
        return parse_vector_value(input);
    }
    if let Some(value) = input
        .strip_prefix('"')
        .and_then(|value| value.strip_suffix('"'))
    {
        return Ok(Value::String(value.to_string()));
    }
    if input.eq_ignore_ascii_case("null") {
        return Ok(Value::Null);
    }
    if input.eq_ignore_ascii_case("true") {
        return Ok(Value::Bool(true));
    }
    if input.eq_ignore_ascii_case("false") {
        return Ok(Value::Bool(false));
    }
    if let Ok(value) = input.parse::<i64>() {
        return Ok(Value::Int(value));
    }
    if let Ok(value) = input.parse::<f64>() {
        return Ok(Value::Float(value));
    }
    Err(write_parse_error(&format!("unsupported literal {input:?}")))
}

fn parse_vector_value(input: &str) -> DatabaseResult<Value> {
    let inner = strip_wrapping_write(input.trim(), '[', ']')?;
    if inner.trim().is_empty() {
        return Err(write_parse_error(
            "vector literal must contain at least one value",
        ));
    }
    let vector = inner
        .split(',')
        .map(|item| {
            item.trim().parse::<f32>().map_err(|_| {
                write_parse_error(&format!("invalid vector element {:?}", item.trim()))
            })
        })
        .collect::<DatabaseResult<Vec<_>>>()?;
    Ok(Value::Vector(vector))
}

fn parse_property_ref_write(input: &str) -> DatabaseResult<(String, String)> {
    let (variable, key) = input
        .split_once('.')
        .ok_or_else(|| write_parse_error("property reference must use variable.property"))?;
    validate_identifier_write(variable.trim())?;
    validate_identifier_write(key.trim())?;
    Ok((variable.trim().to_string(), key.trim().to_string()))
}

fn parse_return_variable(input: &str) -> DatabaseResult<String> {
    let variable = input.trim();
    validate_identifier_write(variable)?;
    Ok(variable.to_string())
}

fn parse_optional_write_return<'a>(
    input: &'a str,
) -> DatabaseResult<(&'a str, Option<WriteReturnItems>)> {
    match split_keyword(input, "RETURN") {
        Some((body, returns)) => Ok((body.trim(), Some(parse_write_return_items(returns)?))),
        None => Ok((input.trim(), None)),
    }
}

fn parse_write_return_items(input: &str) -> DatabaseResult<WriteReturnItems> {
    let mut items = Vec::new();
    for item in split_top_level_commas(input.trim())? {
        items.push(parse_write_return_item(item)?);
    }
    ensure_write_parse(!items.is_empty(), "write RETURN requires at least one item")?;
    Ok(items)
}

fn parse_write_return_item(input: &str) -> DatabaseResult<WriteReturnItem> {
    let input = input.trim();
    if let Some((variable, key)) = input.split_once('.') {
        validate_identifier_write(variable.trim())?;
        validate_identifier_write(key.trim())?;
        Ok(WriteReturnItem::Property {
            variable: variable.trim().to_string(),
            key: key.trim().to_string(),
        })
    } else {
        validate_identifier_write(input)?;
        Ok(WriteReturnItem::Variable(input.to_string()))
    }
}

fn ensure_write_return_matches(
    returns: Option<&WriteReturnItems>,
    expected_variable: &str,
    context: &str,
) -> DatabaseResult<()> {
    let Some(returns) = returns else {
        return Ok(());
    };
    for item in returns {
        let variable = match item {
            WriteReturnItem::Variable(variable) => variable,
            WriteReturnItem::Property { variable, .. } => variable,
        };
        ensure_write_parse(
            variable == expected_variable,
            &format!("{context} variable must match the MATCH variable"),
        )?;
    }
    Ok(())
}

fn query_match_node_ids(
    run_query: impl FnOnce(&str) -> DatabaseResult<Vec<QueryRow>>,
    matcher: &NodeMatcher,
) -> DatabaseResult<Vec<NodeId>> {
    let rows = run_query(&matcher.match_query)?;
    rows.into_iter()
        .map(|row| match row.get(&matcher.variable) {
            Some(QueryValue::Node(node)) => Ok(node.id),
            Some(QueryValue::BoundaryNode(node)) => Err(DatabaseError::Replication(format!(
                "write target node {} is a boundary cache node",
                node.id
            ))),
            Some(value) => Err(write_parse_error(&format!(
                "MATCH returned non-node value {value:?}"
            ))),
            None => Err(write_parse_error(
                "MATCH did not return the target variable",
            )),
        })
        .collect()
}

fn query_match_relationship_ids(
    run_query: impl FnOnce(&str) -> DatabaseResult<Vec<QueryRow>>,
    matcher: &RelationshipMatcher,
) -> DatabaseResult<Vec<RelationshipId>> {
    let rows = run_query(&matcher.match_query)?;
    rows.into_iter()
        .map(|row| match row.get(&matcher.variable) {
            Some(QueryValue::Relationship(relationship)) => Ok(relationship.id),
            Some(value) => Err(write_parse_error(&format!(
                "MATCH returned non-relationship value {value:?}"
            ))),
            None => Err(write_parse_error(
                "MATCH did not return the target relationship",
            )),
        })
        .collect()
}

fn find_merge_node_in_graph(
    graph: &impl GraphRead,
    labels: &[String],
    properties: &Properties,
) -> DatabaseResult<Option<Node>> {
    let mut nodes = graph.nodes()?;
    nodes.sort_by_key(|node| node.id);
    Ok(nodes
        .into_iter()
        .find(|node| node_matches_merge_pattern(node, labels, properties)))
}

fn find_merge_relationship_in_graph(
    graph: &impl GraphRead,
    from: NodeId,
    to: NodeId,
    rel_type: &str,
    properties: &Properties,
) -> DatabaseResult<Option<Relationship>> {
    let mut relationships = graph.outgoing_by_type(from, rel_type)?;
    relationships.sort_by_key(|relationship| relationship.id);
    Ok(relationships.into_iter().find(|relationship| {
        relationship.to == to
            && properties
                .iter()
                .all(|(key, value)| relationship.properties.get(key) == Some(value))
    }))
}

fn matches_target_shard(target_shard: Option<ShardId>, shard_id: ShardId) -> bool {
    target_shard
        .map(|target| target == shard_id)
        .unwrap_or(true)
}

fn ensure_metadata_target_shard(target_shard: Option<ShardId>) -> DatabaseResult<()> {
    if matches_target_shard(target_shard, 0) {
        Ok(())
    } else {
        Err(DatabaseError::InvalidConfig(
            "index metadata Cypher must target shard 0".to_string(),
        ))
    }
}

fn return_created_node(
    variable: String,
    returns: Option<WriteReturnItems>,
    id: NodeId,
    labels: Vec<String>,
    properties: Properties,
) -> Vec<QueryRow> {
    let _ = variable;
    return_node_for_write(Node::new(id, labels, properties), returns.as_ref())
}

fn return_created_relationship(
    variable: String,
    returns: Option<WriteReturnItems>,
    relationship: Relationship,
) -> Vec<QueryRow> {
    let _ = variable;
    return_relationship_for_write(relationship, returns.as_ref())
}

fn return_node_for_write(node: Node, returns: Option<&WriteReturnItems>) -> Vec<QueryRow> {
    let Some(returns) = returns else {
        return Vec::new();
    };
    vec![write_node_return_row(&node, returns)]
}

fn return_relationship_for_write(
    relationship: Relationship,
    returns: Option<&WriteReturnItems>,
) -> Vec<QueryRow> {
    let Some(returns) = returns else {
        return Vec::new();
    };
    vec![write_relationship_return_row(&relationship, returns)]
}

fn apply_assignments_to_properties(
    properties: &mut Properties,
    assignments: &[PropertyAssignment],
) {
    for assignment in assignments {
        if matches!(assignment.value, Value::Null) {
            properties.remove(&assignment.key);
        } else {
            properties.insert(assignment.key.clone(), assignment.value.clone());
        }
    }
}

fn create_properties_after_set(
    mut properties: Properties,
    assignments: Vec<PropertyAssignment>,
    replacement: Option<Properties>,
) -> Properties {
    if let Some(replacement) = replacement {
        properties_without_null_values(replacement)
    } else {
        apply_assignments_to_properties(&mut properties, &assignments);
        properties
    }
}

fn properties_after_set(
    mut properties: Properties,
    assignments: &[PropertyAssignment],
    replacement: Option<&Properties>,
) -> Properties {
    if let Some(replacement) = replacement {
        properties_without_null_values(replacement.clone())
    } else {
        apply_assignments_to_properties(&mut properties, assignments);
        properties
    }
}

fn replace_node_properties(
    db: &mut Neo4rDatabase,
    id: NodeId,
    before: &Properties,
    after: &Properties,
) -> DatabaseResult<()> {
    for key in property_removes(before, after) {
        db.remove_node_property(id, key)?;
    }
    for (key, value) in property_sets(before, after) {
        db.set_node_property(id, key, value)?;
    }
    Ok(())
}

fn replace_relationship_properties(
    db: &mut Neo4rDatabase,
    id: RelationshipId,
    before: &Properties,
    after: &Properties,
) -> DatabaseResult<()> {
    for key in property_removes(before, after) {
        db.remove_relationship_property(id, key)?;
    }
    for (key, value) in property_sets(before, after) {
        db.set_relationship_property(id, key, value)?;
    }
    Ok(())
}

fn apply_node_property_assignment(
    db: &mut Neo4rDatabase,
    id: NodeId,
    assignment: &PropertyAssignment,
) -> DatabaseResult<()> {
    if matches!(assignment.value, Value::Null) {
        db.remove_node_property(id, assignment.key.clone())
    } else {
        db.set_node_property(id, assignment.key.clone(), assignment.value.clone())
    }
}

fn apply_relationship_property_assignment(
    db: &mut Neo4rDatabase,
    id: RelationshipId,
    assignment: &PropertyAssignment,
) -> DatabaseResult<()> {
    if matches!(assignment.value, Value::Null) {
        db.remove_relationship_property(id, assignment.key.clone())
    } else {
        db.set_relationship_property(id, assignment.key.clone(), assignment.value.clone())
    }
}

fn replace_node_properties_with_handle(
    db: &Neo4rDatabaseHandle,
    id: NodeId,
    before: &Properties,
    after: &Properties,
) -> DatabaseResult<()> {
    for key in property_removes(before, after) {
        db.remove_node_property(id, key)?;
    }
    for (key, value) in property_sets(before, after) {
        db.set_node_property(id, key, value)?;
    }
    Ok(())
}

fn replace_relationship_properties_with_handle(
    db: &Neo4rDatabaseHandle,
    id: RelationshipId,
    before: &Properties,
    after: &Properties,
) -> DatabaseResult<()> {
    for key in property_removes(before, after) {
        db.remove_relationship_property(id, key)?;
    }
    for (key, value) in property_sets(before, after) {
        db.set_relationship_property(id, key, value)?;
    }
    Ok(())
}

fn apply_node_property_assignment_with_handle(
    db: &Neo4rDatabaseHandle,
    id: NodeId,
    assignment: &PropertyAssignment,
) -> DatabaseResult<()> {
    if matches!(assignment.value, Value::Null) {
        db.remove_node_property(id, assignment.key.clone())
    } else {
        db.set_node_property(id, assignment.key.clone(), assignment.value.clone())
    }
}

fn apply_relationship_property_assignment_with_handle(
    db: &Neo4rDatabaseHandle,
    id: RelationshipId,
    assignment: &PropertyAssignment,
) -> DatabaseResult<()> {
    if matches!(assignment.value, Value::Null) {
        db.remove_relationship_property(id, assignment.key.clone())
    } else {
        db.set_relationship_property(id, assignment.key.clone(), assignment.value.clone())
    }
}

fn property_sets(before: &Properties, after: &Properties) -> Vec<(String, Value)> {
    let mut keys = after.keys().cloned().collect::<Vec<_>>();
    keys.sort();
    keys.into_iter()
        .filter_map(|key| {
            let value = after.get(&key)?;
            if matches!(value, Value::Null) {
                return None;
            }
            if before.get(&key) == Some(value) {
                None
            } else {
                Some((key, value.clone()))
            }
        })
        .collect()
}

fn property_removes(before: &Properties, after: &Properties) -> Vec<String> {
    let mut keys = before.keys().cloned().collect::<Vec<_>>();
    keys.sort();
    keys.into_iter()
        .filter(|key| !after.contains_key(key) || matches!(after.get(key), Some(Value::Null)))
        .collect()
}

fn properties_without_null_values(properties: Properties) -> Properties {
    properties
        .into_iter()
        .filter(|(_, value)| !matches!(value, Value::Null))
        .collect()
}

fn append_property_delta_commands(
    commands: &mut Vec<Command>,
    before: &Properties,
    after: &Properties,
    mut set_command: impl FnMut(String, Value) -> Command,
    mut remove_command: impl FnMut(String) -> Command,
) {
    let keys = before
        .keys()
        .chain(after.keys())
        .cloned()
        .collect::<BTreeSet<_>>();
    let mut removes = Vec::new();
    let mut sets = Vec::new();
    for key in keys {
        match (before.get(&key), after.get(&key)) {
            (Some(before), Some(after)) if before == after => {}
            (_, Some(after)) => sets.push(set_command(key, after.clone())),
            (Some(_), None) => removes.push(remove_command(key)),
            (None, None) => {}
        }
    }
    commands.extend(removes);
    commands.extend(sets);
}

fn append_label_delta_commands(
    commands: &mut Vec<Command>,
    before: &[String],
    after: &[String],
    mut add_command: impl FnMut(String) -> Command,
    mut remove_command: impl FnMut(String) -> Command,
) {
    let before = before.iter().cloned().collect::<BTreeSet<_>>();
    let after = after.iter().cloned().collect::<BTreeSet<_>>();
    for label in after.difference(&before) {
        commands.push(add_command(label.clone()));
    }
    for label in before.difference(&after) {
        commands.push(remove_command(label.clone()));
    }
}

fn return_nodes_after_write(
    ids: &[NodeId],
    returns: Option<&WriteReturnItems>,
    mut load: impl FnMut(NodeId) -> DatabaseResult<Option<Node>>,
) -> DatabaseResult<Vec<QueryRow>> {
    let Some(returns) = returns else {
        return Ok(Vec::new());
    };
    let mut rows = Vec::new();
    for id in ids {
        if let Some(node) = load(*id)? {
            rows.push(write_node_return_row(&node, returns));
        }
    }
    Ok(rows)
}

fn return_relationships_after_write(
    ids: &[RelationshipId],
    returns: Option<&WriteReturnItems>,
    mut load: impl FnMut(RelationshipId) -> DatabaseResult<Option<Relationship>>,
) -> DatabaseResult<Vec<QueryRow>> {
    let Some(returns) = returns else {
        return Ok(Vec::new());
    };
    let mut rows = Vec::new();
    for id in ids {
        if let Some(relationship) = load(*id)? {
            rows.push(write_relationship_return_row(&relationship, returns));
        }
    }
    Ok(rows)
}

fn write_node_return_row(node: &Node, returns: &[WriteReturnItem]) -> QueryRow {
    let mut row = QueryRow::new();
    for item in returns {
        match item {
            WriteReturnItem::Variable(variable) => {
                row.insert(variable.clone(), QueryValue::Node(node.clone()));
            }
            WriteReturnItem::Property { variable, key } => {
                row.insert(
                    format!("{variable}.{key}"),
                    QueryValue::Scalar(node.properties.get(key).cloned().unwrap_or(Value::Null)),
                );
            }
        }
    }
    row
}

fn write_relationship_return_row(
    relationship: &Relationship,
    returns: &[WriteReturnItem],
) -> QueryRow {
    let mut row = QueryRow::new();
    for item in returns {
        match item {
            WriteReturnItem::Variable(variable) => {
                row.insert(
                    variable.clone(),
                    QueryValue::Relationship(relationship.clone()),
                );
            }
            WriteReturnItem::Property { variable, key } => {
                row.insert(
                    format!("{variable}.{key}"),
                    QueryValue::Scalar(
                        relationship
                            .properties
                            .get(key)
                            .cloned()
                            .unwrap_or(Value::Null),
                    ),
                );
            }
        }
    }
    row
}

fn strip_node_pattern_properties(input: &str) -> DatabaseResult<String> {
    let input = input.trim();
    let Some(index) = top_level_brace_start(input) else {
        return Ok(input.to_string());
    };
    ensure_write_parse(input.ends_with(')'), "node pattern must end with )")?;
    Ok(format!("{}{}", input[..index].trim_end(), ")"))
}

fn strip_relationship_properties(input: &str) -> DatabaseResult<String> {
    let mut output = String::with_capacity(input.len());
    let mut depth = 0_i32;
    let mut in_string = false;
    for ch in input.chars() {
        match ch {
            '"' if depth == 0 => {
                in_string = !in_string;
                output.push(ch);
            }
            '{' if !in_string => depth += 1,
            '}' if !in_string => {
                depth -= 1;
                ensure_write_parse(depth >= 0, "unbalanced property literal")?;
            }
            _ if depth == 0 => output.push(ch),
            _ => {}
        }
    }
    ensure_write_parse(depth == 0, "unbalanced property literal")?;
    Ok(output)
}

fn write_value_literal(value: &Value) -> DatabaseResult<String> {
    match value {
        Value::Null => Ok("null".to_string()),
        Value::Bool(value) => Ok(value.to_string()),
        Value::Int(value) => Ok(value.to_string()),
        Value::Float(value) => Ok(value.to_string()),
        Value::String(value) => Ok(format!("\"{}\"", value.replace('"', "\\\""))),
        Value::Vector(_) => Err(write_parse_error(
            "MATCH pattern property lookup does not support vector values",
        )),
        Value::Map(_) => Err(write_parse_error(
            "MATCH pattern property lookup does not support map values",
        )),
    }
}

fn starts_with_keyword(input: &str, keyword: &str) -> bool {
    let Some(head) = input.get(..keyword.len()) else {
        return false;
    };
    head.eq_ignore_ascii_case(keyword)
        && input[keyword.len()..]
            .chars()
            .next()
            .is_none_or(|ch| ch.is_whitespace())
}

fn strip_keyword<'a>(input: &'a str, keyword: &str) -> DatabaseResult<&'a str> {
    ensure_write_parse(starts_with_keyword(input, keyword), "expected keyword")?;
    Ok(input[keyword.len()..].trim())
}

fn strip_keyword_suffix<'a>(input: &'a str, keyword: &str) -> Option<&'a str> {
    let input = input.trim();
    let suffix_start = input.len().checked_sub(keyword.len())?;
    let suffix = input.get(suffix_start..)?;
    if !suffix.eq_ignore_ascii_case(keyword) {
        return None;
    }
    if suffix_start > 0
        && !input[..suffix_start]
            .chars()
            .next_back()
            .is_some_and(|ch| ch.is_whitespace())
    {
        return None;
    }
    Some(input[..suffix_start].trim_end())
}

fn split_keyword<'a>(input: &'a str, keyword: &str) -> Option<(&'a str, &'a str)> {
    let needle = format!(" {} ", keyword.to_ascii_uppercase());
    let haystack = input.to_ascii_uppercase();
    haystack.find(&needle).map(|index| {
        let split = index + 1;
        (&input[..index], &input[split + keyword.len()..])
    })
}

fn find_keyword(input: &str, keyword: &str) -> Option<usize> {
    let needle = format!(" {} ", keyword.to_ascii_uppercase());
    input
        .to_ascii_uppercase()
        .find(&needle)
        .map(|index| index + 1)
}

fn strip_wrapping_write(input: &str, open: char, close: char) -> DatabaseResult<&str> {
    ensure_write_parse(
        input.starts_with(open) && input.ends_with(close),
        "invalid wrapping",
    )?;
    Ok(&input[open.len_utf8()..input.len() - close.len_utf8()])
}

fn top_level_brace_start(input: &str) -> Option<usize> {
    let mut in_string = false;
    for (index, ch) in input.char_indices() {
        match ch {
            '"' => in_string = !in_string,
            '{' if !in_string => return Some(index),
            _ => {}
        }
    }
    None
}

fn split_top_level_commas(input: &str) -> DatabaseResult<Vec<&str>> {
    let mut entries = Vec::new();
    let mut start = 0;
    let mut depth = 0_i32;
    let mut in_string = false;
    for (index, ch) in input.char_indices() {
        match ch {
            '"' => in_string = !in_string,
            '[' | '{' if !in_string => depth += 1,
            ']' | '}' if !in_string => depth -= 1,
            ',' if !in_string && depth == 0 => {
                entries.push(input[start..index].trim());
                start = index + ch.len_utf8();
            }
            _ => {}
        }
        ensure_write_parse(depth >= 0, "unbalanced property literal")?;
    }
    entries.push(input[start..].trim());
    ensure_write_parse(
        !entries.iter().any(|entry| entry.is_empty()),
        "empty property entry",
    )?;
    Ok(entries)
}

fn validate_identifier_write(input: &str) -> DatabaseResult<()> {
    let mut chars = input.chars();
    let Some(first) = chars.next() else {
        return Err(write_parse_error("identifier cannot be empty"));
    };
    ensure_write_parse(
        first.is_ascii_alphabetic() || first == '_',
        "identifier must start with a letter or underscore",
    )?;
    ensure_write_parse(
        chars.all(|ch| ch.is_ascii_alphanumeric() || ch == '_'),
        "identifier may only contain letters, digits, or underscores",
    )
}

fn ensure_write_parse(condition: bool, message: &str) -> DatabaseResult<()> {
    if condition {
        Ok(())
    } else {
        Err(write_parse_error(message))
    }
}

fn write_parse_error(message: &str) -> DatabaseError {
    DatabaseError::Query(QueryError::Parse(message.to_string()))
}

fn validate_read_options(
    snapshot: &Neo4rReadSnapshot,
    options: QueryOptions,
) -> DatabaseResult<()> {
    validate_read_isolation(options.isolation);
    validate_read_consistency(snapshot, options.consistency)
}

fn validate_read_isolation(isolation: ReadIsolation) {
    match isolation {
        ReadIsolation::ReadCommitted | ReadIsolation::Snapshot => {}
    }
}

fn validate_read_consistency(
    snapshot: &Neo4rReadSnapshot,
    consistency: ReadConsistency,
) -> DatabaseResult<()> {
    match consistency {
        ReadConsistency::Strong => {
            if snapshot.applied_indexes() == snapshot.committed_indexes() {
                Ok(())
            } else {
                Err(DatabaseError::Replication(
                    "strong read requires applied indexes to match committed indexes".to_string(),
                ))
            }
        }
        ReadConsistency::FollowerStale => Ok(()),
        ReadConsistency::BoundedStaleness { max_staleness_ms } => {
            let now_ms = HybridClock::new().tick().physical_ms;
            let age = now_ms.saturating_sub(snapshot.timestamp().physical_ms);
            if age <= max_staleness_ms {
                Ok(())
            } else {
                Err(DatabaseError::Replication(format!(
                    "snapshot staleness {age}ms exceeds bound {max_staleness_ms}ms"
                )))
            }
        }
    }
}

impl Neo4rDatabase {
    pub fn open(config: DatabaseConfig) -> DatabaseResult<Self> {
        Self::open_with_replicator(config, Arc::new(NoopShardReplicator))
    }

    pub fn open_with_replicator(
        config: DatabaseConfig,
        replicator: Arc<dyn ShardReplicator>,
    ) -> DatabaseResult<Self> {
        validate_config(&config)?;
        let shard_map = ShardMap::new(config.shard_count).ok_or_else(|| {
            DatabaseError::InvalidConfig("shard count must be greater than zero".to_string())
        })?;
        let store =
            PartitionedGraphStore::open_rocks(&config.data_dir, config.local_partition_count)?;
        let logs = open_logs(&config)?;
        let checkpoints = open_checkpoints(&config)?;
        let commits = open_commits(&config)?;
        let shard_metadata = ShardMetadataStore::open(&config.data_dir)?;
        let membership_store = ClusterMembershipStore::open(&config.data_dir)?;
        let membership = load_or_initialize_membership(&config, &membership_store)?;
        let rebalance_plan_store = RebalancePlanStore::open(&config.data_dir)?;
        let rebalance_execution_store = RebalanceExecutionStore::open(&config.data_dir)?;
        let rebalance_execution = rebalance_execution_store.load()?;
        let cluster_metadata_store = ClusterMetadataStore::open(&config.data_dir)?;
        let metadata_log_store = MetadataOperationLogStore::open(&config.data_dir)?;
        let statistics_store = StatisticsCatalogStore::open(&config.data_dir)?;
        let index_catalog_store = IndexCatalogStore::open(&config.data_dir)?;
        let index_catalog = index_catalog_store.load()?.unwrap_or_default();
        let routing_table = load_or_initialize_routing_table(&config, &shard_metadata)?;
        let cluster_metadata =
            load_or_initialize_cluster_metadata(&config, &cluster_metadata_store, &routing_table)?;
        let statistics = statistics_store.load()?.unwrap_or_default();
        let commit_indexes = load_commit_indexes(&commits)?;
        let vector_indexes = Arc::new(Mutex::new(PersistentVectorIndexes::default()));
        let query_engine = CypherEngine::with_vector_indexes(Arc::new(
            SharedVectorIndexProvider::new(vector_indexes.clone()),
        ));

        let mut database = Self {
            config,
            shard_map,
            store,
            logs,
            checkpoints,
            commits,
            shard_metadata,
            membership_store,
            membership,
            rebalance_plan_store,
            rebalance_execution_store,
            rebalance_execution,
            cluster_metadata_store,
            cluster_metadata,
            metadata_log_store,
            statistics_store,
            statistics,
            read_cache: Mutex::new(ReadPathCache::default()),
            read_cache_stats: Mutex::new(ReadCacheStats::default()),
            index_catalog_store,
            index_catalog,
            vector_indexes,
            routing_table,
            next_log_indexes: vec![1; shard_map.shard_count() as usize],
            commit_indexes,
            match_indexes: vec![BTreeMap::new(); shard_map.shard_count() as usize],
            next_node_id: 0,
            next_relationship_id: 0,
            clock: HybridClock::new(),
            query_engine,
            replicator,
        };
        database.replay_logs()?;
        database.recover_allocators_from_store()?;
        database.load_or_rebuild_vector_indexes()?;
        Ok(database)
    }

    pub fn open_path(
        data_dir: impl AsRef<Path>,
        shard_count: u64,
        local_partition_count: usize,
    ) -> DatabaseResult<Self> {
        Self::open(DatabaseConfig::new(
            data_dir.as_ref(),
            shard_count,
            local_partition_count,
        ))
    }

    pub fn create_node(
        &mut self,
        labels: Vec<String>,
        properties: Properties,
    ) -> DatabaseResult<NodeId> {
        let id = self.allocate_node_id();
        let command = Command::CreateNode {
            id,
            labels,
            properties,
        };
        let shard_id = self.shard_map.owner_of_node(id);
        self.write_command(shard_id, command)?;
        Ok(id)
    }

    pub fn create_relationship(
        &mut self,
        from: NodeId,
        to: NodeId,
        rel_type: String,
        properties: Properties,
    ) -> DatabaseResult<RelationshipId> {
        self.ensure_local_node_exists(from)?;
        self.ensure_node_or_boundary_exists(to)?;
        let id = self.allocate_relationship_id();
        let shard_id = self.shard_map.owner_of_relationship(from, to, &rel_type);
        let command = Command::CreateRelationship {
            id,
            from,
            to,
            rel_type,
            properties,
        };
        self.write_command(shard_id, command)?;
        Ok(id)
    }

    pub fn set_node_property(
        &mut self,
        id: NodeId,
        key: String,
        value: Value,
    ) -> DatabaseResult<()> {
        self.ensure_local_node_exists(id)?;
        let shard_id = self.shard_map.owner_of_node(id);
        self.write_command(shard_id, Command::SetNodeProperty { id, key, value })
    }

    pub fn remove_node_property(&mut self, id: NodeId, key: String) -> DatabaseResult<()> {
        self.ensure_local_node_exists(id)?;
        let shard_id = self.shard_map.owner_of_node(id);
        self.write_command(shard_id, Command::RemoveNodeProperty { id, key })
    }

    pub fn add_node_label(&mut self, id: NodeId, label: String) -> DatabaseResult<()> {
        self.ensure_local_node_exists(id)?;
        let shard_id = self.shard_map.owner_of_node(id);
        self.write_command(shard_id, Command::AddNodeLabel { id, label })
    }

    pub fn remove_node_label(&mut self, id: NodeId, label: String) -> DatabaseResult<()> {
        self.ensure_local_node_exists(id)?;
        let shard_id = self.shard_map.owner_of_node(id);
        self.write_command(shard_id, Command::RemoveNodeLabel { id, label })
    }

    pub fn set_relationship_property(
        &mut self,
        id: RelationshipId,
        key: String,
        value: Value,
    ) -> DatabaseResult<()> {
        let shard_id = self.relationship_owner_shard(id)?;
        self.write_command(
            shard_id,
            Command::SetRelationshipProperty { id, key, value },
        )
    }

    pub fn remove_relationship_property(
        &mut self,
        id: RelationshipId,
        key: String,
    ) -> DatabaseResult<()> {
        let shard_id = self.relationship_owner_shard(id)?;
        self.write_command(shard_id, Command::RemoveRelationshipProperty { id, key })
    }

    pub fn delete_relationship(&mut self, id: RelationshipId) -> DatabaseResult<()> {
        let shard_id = self.relationship_owner_shard(id)?;
        self.write_command(shard_id, Command::DeleteRelationship { id })
    }

    pub fn delete_node(&mut self, id: NodeId) -> DatabaseResult<()> {
        self.ensure_local_node_exists(id)?;
        let shard_id = self.shard_map.owner_of_node(id);
        self.write_command(shard_id, Command::DeleteNode { id })
    }

    pub fn execute_cypher(&mut self, query: &str) -> DatabaseResult<Vec<QueryRow>> {
        self.execute_cypher_with_params(query, &QueryParams::new())
    }

    pub fn execute_cypher_with_params(
        &mut self,
        query: &str,
        params: &QueryParams,
    ) -> DatabaseResult<Vec<QueryRow>> {
        self.execute_write_cypher_on_optional_shard(query, params, None)
    }

    fn execute_cypher_on_shard(
        &mut self,
        shard_id: ShardId,
        query: &str,
        params: &QueryParams,
    ) -> DatabaseResult<Vec<QueryRow>> {
        self.validate_shard_id(shard_id)?;
        self.execute_write_cypher_on_optional_shard(query, params, Some(shard_id))
    }

    fn write_cypher_target_shards(
        &mut self,
        query: &str,
        params: &QueryParams,
    ) -> DatabaseResult<Vec<ShardId>> {
        let mut shards = BTreeSet::new();
        match parse_write_cypher(query, params)? {
            Some(WriteCypher::CreateNode { .. }) => {
                for status in self.cluster_status().shards {
                    if status.is_local_primary {
                        shards.insert(status.shard_id);
                    }
                }
                if shards.is_empty() {
                    shards.insert(0);
                }
            }
            Some(WriteCypher::CreateRelationship { from_matcher, .. }) => {
                for from in self.match_node_ids(&from_matcher, params)? {
                    shards.insert(self.shard_map.owner_of_node(from));
                }
            }
            Some(WriteCypher::MergeNode { .. }) => {
                for status in self.cluster_status().shards {
                    if status.is_local_primary {
                        shards.insert(status.shard_id);
                    }
                }
                if shards.is_empty() {
                    shards.insert(0);
                }
            }
            Some(WriteCypher::MergeRelationship { from_matcher, .. }) => {
                for from in self.match_node_ids(&from_matcher, params)? {
                    shards.insert(self.shard_map.owner_of_node(from));
                }
            }
            Some(WriteCypher::SetNodeProperty { matcher, .. })
            | Some(WriteCypher::ReplaceNodeProperties { matcher, .. })
            | Some(WriteCypher::RemoveNodeProperty { matcher, .. })
            | Some(WriteCypher::AddNodeLabel { matcher, .. })
            | Some(WriteCypher::RemoveNodeLabel { matcher, .. })
            | Some(WriteCypher::DeleteNode { matcher, .. }) => {
                for id in self.match_node_ids(&matcher, params)? {
                    shards.insert(self.shard_map.owner_of_node(id));
                }
            }
            Some(WriteCypher::SetRelationshipProperty { matcher, .. })
            | Some(WriteCypher::ReplaceRelationshipProperties { matcher, .. })
            | Some(WriteCypher::RemoveRelationshipProperty { matcher, .. })
            | Some(WriteCypher::DeleteRelationship { matcher, .. }) => {
                for id in self.match_relationship_ids(&matcher, params)? {
                    shards.insert(self.relationship_owner_shard(id)?);
                }
            }
            Some(WriteCypher::CreateNodePropertyIndex { .. })
            | Some(WriteCypher::CreateUniqueNodePropertyConstraint { .. })
            | Some(WriteCypher::CreateVectorIndex { .. })
            | Some(WriteCypher::RebuildVectorIndex { .. })
            | Some(WriteCypher::DropIndex { .. })
            | Some(WriteCypher::DropConstraint { .. }) => {
                shards.insert(0);
            }
            None => {}
        }
        Ok(shards.into_iter().collect())
    }

    fn execute_cypher_mutation_batch_on_shard(
        &mut self,
        shard_id: ShardId,
        writes: Vec<(String, QueryParams)>,
    ) -> DatabaseResult<usize> {
        self.validate_shard_id(shard_id)?;
        self.execute_cypher_mutation_batch_inner(writes, Some(shard_id))
    }

    fn execute_cypher_mutation_batch(
        &mut self,
        writes: Vec<(String, QueryParams)>,
    ) -> DatabaseResult<usize> {
        self.execute_cypher_mutation_batch_inner(writes, None)
    }

    fn execute_staged_cypher_transaction_on_shard(
        &mut self,
        shard_id: ShardId,
        writes: Vec<(String, QueryParams)>,
    ) -> DatabaseResult<usize> {
        self.validate_shard_id(shard_id)?;
        let snapshot = self.read_snapshot()?;
        let overlay = snapshot.build_staged_overlay(&writes)?;
        let commands =
            self.commands_from_staged_overlay_on_shard(shard_id, &snapshot.store, overlay)?;
        let mut entries = Vec::with_capacity(commands.len());
        for command in commands {
            entries.push(self.append_local_command(shard_id, command, false)?);
        }
        if entries.is_empty() {
            return Ok(0);
        }
        self.flush_group_commit(&entries)?;
        Ok(entries.len())
    }

    fn commands_from_staged_overlay_on_shard(
        &mut self,
        target_shard: ShardId,
        base: &PartitionedGraphStore<RocksKvSnapshot>,
        overlay: StagedOverlay,
    ) -> DatabaseResult<Vec<Command>> {
        let mut commands = Vec::new();
        let mut temp_node_id_map = HashMap::new();

        let mut temp_nodes = overlay
            .temp_node_ids
            .iter()
            .filter_map(|id| overlay.nodes.get(id).and_then(|node| node.as_ref()))
            .cloned()
            .collect::<Vec<_>>();
        temp_nodes.sort_by(|left, right| right.id.cmp(&left.id));
        for node in temp_nodes {
            let id = self.allocate_node_id_for_shard(target_shard);
            temp_node_id_map.insert(node.id, id);
            commands.push(Command::CreateNode {
                id,
                labels: node.labels,
                properties: node.properties,
            });
        }

        let mut temp_relationships = overlay
            .temp_relationship_ids
            .iter()
            .filter_map(|id| {
                overlay
                    .relationships
                    .get(id)
                    .and_then(|relationship| relationship.as_ref())
            })
            .cloned()
            .collect::<Vec<_>>();
        temp_relationships.sort_by(|left, right| right.id.cmp(&left.id));
        for relationship in temp_relationships {
            let from = temp_node_id_map
                .get(&relationship.from)
                .copied()
                .unwrap_or(relationship.from);
            let to = temp_node_id_map
                .get(&relationship.to)
                .copied()
                .unwrap_or(relationship.to);
            let owner = self
                .shard_map
                .owner_of_relationship(from, to, &relationship.rel_type);
            if owner != target_shard {
                return Err(DatabaseError::InvalidConfig(format!(
                    "staged relationship CREATE targets shard {owner}, expected {target_shard}"
                )));
            }
            commands.push(Command::CreateRelationship {
                id: self.allocate_relationship_id(),
                from,
                to,
                rel_type: relationship.rel_type,
                properties: relationship.properties,
            });
        }

        let mut base_node_ids = overlay
            .nodes
            .keys()
            .filter(|id| !overlay.temp_node_ids.contains(id))
            .copied()
            .collect::<Vec<_>>();
        base_node_ids.sort_unstable();
        for id in base_node_ids {
            if self.shard_map.owner_of_node(id) != target_shard {
                continue;
            }
            match overlay.nodes.get(&id) {
                Some(Some(node)) => {
                    let Some(base_node) = base.node(id)? else {
                        return Err(DatabaseError::Graph(GraphError::NodeNotFound(id)));
                    };
                    append_property_delta_commands(
                        &mut commands,
                        &base_node.properties,
                        &node.properties,
                        |key, value| Command::SetNodeProperty { id, key, value },
                        |key| Command::RemoveNodeProperty { id, key },
                    );
                    append_label_delta_commands(
                        &mut commands,
                        &base_node.labels,
                        &node.labels,
                        |label| Command::AddNodeLabel { id, label },
                        |label| Command::RemoveNodeLabel { id, label },
                    );
                }
                Some(None) => {
                    if base.node(id)?.is_some() {
                        commands.push(Command::DeleteNode { id });
                    }
                }
                None => {}
            }
        }

        let mut base_relationship_ids = overlay
            .relationships
            .keys()
            .filter(|id| !overlay.temp_relationship_ids.contains(id))
            .copied()
            .collect::<Vec<_>>();
        base_relationship_ids.sort_unstable();
        for id in base_relationship_ids {
            let Some(base_relationship) = base.relationship(id)? else {
                continue;
            };
            let owner = self.shard_map.owner_of_relationship(
                base_relationship.from,
                base_relationship.to,
                &base_relationship.rel_type,
            );
            if owner != target_shard {
                continue;
            }
            match overlay.relationships.get(&id) {
                Some(Some(relationship)) => {
                    append_property_delta_commands(
                        &mut commands,
                        &base_relationship.properties,
                        &relationship.properties,
                        |key, value| Command::SetRelationshipProperty { id, key, value },
                        |key| Command::RemoveRelationshipProperty { id, key },
                    );
                }
                Some(None) => commands.push(Command::DeleteRelationship { id }),
                None => {}
            }
        }

        Ok(commands)
    }

    fn execute_cypher_mutation_batch_inner(
        &mut self,
        writes: Vec<(String, QueryParams)>,
        target_shard: Option<ShardId>,
    ) -> DatabaseResult<usize> {
        let mut commands = Vec::new();
        for (query, params) in writes {
            match parse_write_cypher(&query, &params)? {
                Some(WriteCypher::CreateNode {
                    labels,
                    properties,
                    assignments,
                    replacement,
                    ..
                }) => {
                    let properties =
                        create_properties_after_set(properties, assignments, replacement);
                    let shard_id = target_shard.ok_or_else(|| {
                        DatabaseError::InvalidConfig(
                            "batched CREATE node requires an explicit target shard".to_string(),
                        )
                    })?;
                    let id = self.allocate_node_id_for_shard(shard_id);
                    commands.push((
                        shard_id,
                        Command::CreateNode {
                            id,
                            labels,
                            properties,
                        },
                    ));
                }
                Some(WriteCypher::MergeNode {
                    labels,
                    properties,
                    on_create,
                    on_create_replacement,
                    on_match,
                    on_match_replacement,
                    ..
                }) => {
                    let shard_id = target_shard.ok_or_else(|| {
                        DatabaseError::InvalidConfig(
                            "batched MERGE node requires an explicit target shard".to_string(),
                        )
                    })?;
                    if let Some(node) =
                        self.find_merge_node(&labels, &properties, Some(shard_id))?
                    {
                        if let Some(replacement) = on_match_replacement.as_ref() {
                            for key in property_removes(&node.properties, replacement) {
                                commands.push((
                                    shard_id,
                                    Command::RemoveNodeProperty { id: node.id, key },
                                ));
                            }
                            for (key, value) in property_sets(&node.properties, replacement) {
                                commands.push((
                                    shard_id,
                                    Command::SetNodeProperty {
                                        id: node.id,
                                        key,
                                        value,
                                    },
                                ));
                            }
                        } else {
                            for assignment in &on_match {
                                if matches!(assignment.value, Value::Null) {
                                    commands.push((
                                        shard_id,
                                        Command::RemoveNodeProperty {
                                            id: node.id,
                                            key: assignment.key.clone(),
                                        },
                                    ));
                                } else {
                                    commands.push((
                                        shard_id,
                                        Command::SetNodeProperty {
                                            id: node.id,
                                            key: assignment.key.clone(),
                                            value: assignment.value.clone(),
                                        },
                                    ));
                                }
                            }
                        }
                    } else {
                        let create_properties = properties_after_set(
                            properties.clone(),
                            &on_create,
                            on_create_replacement.as_ref(),
                        );
                        let id = self.allocate_node_id_for_shard(shard_id);
                        commands.push((
                            shard_id,
                            Command::CreateNode {
                                id,
                                labels,
                                properties: create_properties,
                            },
                        ));
                    }
                }
                Some(WriteCypher::CreateRelationship {
                    from_matcher,
                    to_matcher,
                    rel_type,
                    properties,
                    assignments,
                    replacement,
                    ..
                }) => {
                    let properties =
                        create_properties_after_set(properties, assignments, replacement);
                    let from_ids = self.match_node_ids(&from_matcher, &params)?;
                    let to_ids = self.match_node_ids(&to_matcher, &params)?;
                    for from in &from_ids {
                        self.ensure_local_node_exists(*from)?;
                        for to in &to_ids {
                            self.ensure_node_or_boundary_exists(*to)?;
                            let shard_id =
                                self.shard_map.owner_of_relationship(*from, *to, &rel_type);
                            if matches_target_shard(target_shard, shard_id) {
                                let id = self.allocate_relationship_id();
                                commands.push((
                                    shard_id,
                                    Command::CreateRelationship {
                                        id,
                                        from: *from,
                                        to: *to,
                                        rel_type: rel_type.clone(),
                                        properties: properties.clone(),
                                    },
                                ));
                            }
                        }
                    }
                }
                Some(WriteCypher::MergeRelationship {
                    from_matcher,
                    to_matcher,
                    rel_type,
                    properties,
                    on_create,
                    on_create_replacement,
                    on_match,
                    on_match_replacement,
                    ..
                }) => {
                    let from_ids = self.match_node_ids(&from_matcher, &params)?;
                    let to_ids = self.match_node_ids(&to_matcher, &params)?;
                    for from in &from_ids {
                        self.ensure_local_node_exists(*from)?;
                        for to in &to_ids {
                            self.ensure_node_or_boundary_exists(*to)?;
                            let shard_id =
                                self.shard_map.owner_of_relationship(*from, *to, &rel_type);
                            if !matches_target_shard(target_shard, shard_id) {
                                continue;
                            }
                            if let Some(relationship) =
                                self.find_merge_relationship(*from, *to, &rel_type, &properties)?
                            {
                                if let Some(replacement) = on_match_replacement.as_ref() {
                                    for key in
                                        property_removes(&relationship.properties, replacement)
                                    {
                                        commands.push((
                                            shard_id,
                                            Command::RemoveRelationshipProperty {
                                                id: relationship.id,
                                                key,
                                            },
                                        ));
                                    }
                                    for (key, value) in
                                        property_sets(&relationship.properties, replacement)
                                    {
                                        commands.push((
                                            shard_id,
                                            Command::SetRelationshipProperty {
                                                id: relationship.id,
                                                key,
                                                value,
                                            },
                                        ));
                                    }
                                } else {
                                    for assignment in &on_match {
                                        if matches!(assignment.value, Value::Null) {
                                            commands.push((
                                                shard_id,
                                                Command::RemoveRelationshipProperty {
                                                    id: relationship.id,
                                                    key: assignment.key.clone(),
                                                },
                                            ));
                                        } else {
                                            commands.push((
                                                shard_id,
                                                Command::SetRelationshipProperty {
                                                    id: relationship.id,
                                                    key: assignment.key.clone(),
                                                    value: assignment.value.clone(),
                                                },
                                            ));
                                        }
                                    }
                                }
                            } else {
                                let create_properties = properties_after_set(
                                    properties.clone(),
                                    &on_create,
                                    on_create_replacement.as_ref(),
                                );
                                let id = self.allocate_relationship_id();
                                commands.push((
                                    shard_id,
                                    Command::CreateRelationship {
                                        id,
                                        from: *from,
                                        to: *to,
                                        rel_type: rel_type.clone(),
                                        properties: create_properties,
                                    },
                                ));
                            }
                        }
                    }
                }
                Some(WriteCypher::SetNodeProperty {
                    matcher,
                    assignments,
                    ..
                }) => {
                    for id in self.match_node_ids(&matcher, &params)? {
                        let shard_id = self.shard_map.owner_of_node(id);
                        if matches_target_shard(target_shard, shard_id) {
                            for assignment in &assignments {
                                if matches!(assignment.value, Value::Null) {
                                    commands.push((
                                        shard_id,
                                        Command::RemoveNodeProperty {
                                            id,
                                            key: assignment.key.clone(),
                                        },
                                    ));
                                } else {
                                    commands.push((
                                        shard_id,
                                        Command::SetNodeProperty {
                                            id,
                                            key: assignment.key.clone(),
                                            value: assignment.value.clone(),
                                        },
                                    ));
                                }
                            }
                        }
                    }
                }
                Some(WriteCypher::ReplaceNodeProperties {
                    matcher,
                    properties,
                    ..
                }) => {
                    for id in self.match_node_ids(&matcher, &params)? {
                        let shard_id = self.shard_map.owner_of_node(id);
                        if matches_target_shard(target_shard, shard_id) {
                            let current = self
                                .node(id)?
                                .ok_or(DatabaseError::Graph(GraphError::NodeNotFound(id)))?;
                            for key in property_removes(&current.properties, &properties) {
                                commands.push((shard_id, Command::RemoveNodeProperty { id, key }));
                            }
                            for (key, value) in property_sets(&current.properties, &properties) {
                                commands
                                    .push((shard_id, Command::SetNodeProperty { id, key, value }));
                            }
                        }
                    }
                }
                Some(WriteCypher::RemoveNodeProperty { matcher, keys, .. }) => {
                    for id in self.match_node_ids(&matcher, &params)? {
                        let shard_id = self.shard_map.owner_of_node(id);
                        if matches_target_shard(target_shard, shard_id) {
                            for key in &keys {
                                commands.push((
                                    shard_id,
                                    Command::RemoveNodeProperty {
                                        id,
                                        key: key.clone(),
                                    },
                                ));
                            }
                        }
                    }
                }
                Some(WriteCypher::AddNodeLabel {
                    matcher, labels, ..
                }) => {
                    for id in self.match_node_ids(&matcher, &params)? {
                        let shard_id = self.shard_map.owner_of_node(id);
                        if matches_target_shard(target_shard, shard_id) {
                            for label in &labels {
                                commands.push((
                                    shard_id,
                                    Command::AddNodeLabel {
                                        id,
                                        label: label.clone(),
                                    },
                                ));
                            }
                        }
                    }
                }
                Some(WriteCypher::RemoveNodeLabel {
                    matcher, labels, ..
                }) => {
                    for id in self.match_node_ids(&matcher, &params)? {
                        let shard_id = self.shard_map.owner_of_node(id);
                        if matches_target_shard(target_shard, shard_id) {
                            for label in &labels {
                                commands.push((
                                    shard_id,
                                    Command::RemoveNodeLabel {
                                        id,
                                        label: label.clone(),
                                    },
                                ));
                            }
                        }
                    }
                }
                Some(WriteCypher::SetRelationshipProperty {
                    matcher,
                    assignments,
                    ..
                }) => {
                    for id in self.match_relationship_ids(&matcher, &params)? {
                        let shard_id = self.relationship_owner_shard(id)?;
                        if matches_target_shard(target_shard, shard_id) {
                            for assignment in &assignments {
                                if matches!(assignment.value, Value::Null) {
                                    commands.push((
                                        shard_id,
                                        Command::RemoveRelationshipProperty {
                                            id,
                                            key: assignment.key.clone(),
                                        },
                                    ));
                                } else {
                                    commands.push((
                                        shard_id,
                                        Command::SetRelationshipProperty {
                                            id,
                                            key: assignment.key.clone(),
                                            value: assignment.value.clone(),
                                        },
                                    ));
                                }
                            }
                        }
                    }
                }
                Some(WriteCypher::ReplaceRelationshipProperties {
                    matcher,
                    properties,
                    ..
                }) => {
                    for id in self.match_relationship_ids(&matcher, &params)? {
                        let shard_id = self.relationship_owner_shard(id)?;
                        if matches_target_shard(target_shard, shard_id) {
                            let current = self.relationship(id)?.ok_or(DatabaseError::Graph(
                                GraphError::RelationshipNotFound(id),
                            ))?;
                            for key in property_removes(&current.properties, &properties) {
                                commands.push((
                                    shard_id,
                                    Command::RemoveRelationshipProperty { id, key },
                                ));
                            }
                            for (key, value) in property_sets(&current.properties, &properties) {
                                commands.push((
                                    shard_id,
                                    Command::SetRelationshipProperty { id, key, value },
                                ));
                            }
                        }
                    }
                }
                Some(WriteCypher::RemoveRelationshipProperty { matcher, keys, .. }) => {
                    for id in self.match_relationship_ids(&matcher, &params)? {
                        let shard_id = self.relationship_owner_shard(id)?;
                        if matches_target_shard(target_shard, shard_id) {
                            for key in &keys {
                                commands.push((
                                    shard_id,
                                    Command::RemoveRelationshipProperty {
                                        id,
                                        key: key.clone(),
                                    },
                                ));
                            }
                        }
                    }
                }
                Some(WriteCypher::DeleteNode { matcher, .. }) => {
                    for id in self.match_node_ids(&matcher, &params)? {
                        let shard_id = self.shard_map.owner_of_node(id);
                        if matches_target_shard(target_shard, shard_id) {
                            commands.push((shard_id, Command::DeleteNode { id }));
                        }
                    }
                }
                Some(WriteCypher::DeleteRelationship { matcher, .. }) => {
                    for id in self.match_relationship_ids(&matcher, &params)? {
                        let shard_id = self.relationship_owner_shard(id)?;
                        if matches_target_shard(target_shard, shard_id) {
                            commands.push((shard_id, Command::DeleteRelationship { id }));
                        }
                    }
                }
                Some(WriteCypher::CreateNodePropertyIndex { .. })
                | Some(WriteCypher::CreateUniqueNodePropertyConstraint { .. })
                | Some(WriteCypher::CreateVectorIndex { .. })
                | Some(WriteCypher::RebuildVectorIndex { .. })
                | Some(WriteCypher::DropIndex { .. })
                | Some(WriteCypher::DropConstraint { .. }) => {
                    return Err(DatabaseError::InvalidConfig(
                        "Cypher mutation batch only supports CREATE, MERGE, SET, REMOVE, and DELETE writes"
                            .to_string(),
                    ));
                }
                None => {
                    return Err(DatabaseError::InvalidConfig(
                        "Cypher mutation batch requires write queries".to_string(),
                    ));
                }
            }
        }

        let mut entries = Vec::with_capacity(commands.len());
        let mut shards = BTreeSet::new();
        for (shard_id, _) in &commands {
            shards.insert(*shard_id);
        }
        for shard_id in shards {
            self.ensure_local_primary(shard_id)?;
        }
        for (shard_id, command) in commands {
            entries.push(self.append_local_command(shard_id, command, false)?);
        }
        if entries.is_empty() {
            return Ok(0);
        }
        self.flush_group_commit(&entries)?;
        Ok(entries.len())
    }

    fn execute_write_cypher_on_optional_shard(
        &mut self,
        query: &str,
        params: &QueryParams,
        target_shard: Option<ShardId>,
    ) -> DatabaseResult<Vec<QueryRow>> {
        match parse_write_cypher(query, params)? {
            Some(WriteCypher::CreateNode {
                variable,
                labels,
                properties,
                assignments,
                replacement,
                returns,
            }) => {
                let properties = create_properties_after_set(properties, assignments, replacement);
                let id = if let Some(shard_id) = target_shard {
                    self.validate_shard_id(shard_id)?;
                    let id = self.allocate_node_id_for_shard(shard_id);
                    let command = Command::CreateNode {
                        id,
                        labels: labels.clone(),
                        properties: properties.clone(),
                    };
                    self.write_command(shard_id, command)?;
                    id
                } else {
                    self.create_node(labels.clone(), properties.clone())?
                };
                Ok(return_created_node(
                    variable, returns, id, labels, properties,
                ))
            }
            Some(WriteCypher::CreateRelationship {
                variable,
                from_matcher,
                to_matcher,
                rel_type,
                properties,
                assignments,
                replacement,
                returns,
            }) => {
                let properties = create_properties_after_set(properties, assignments, replacement);
                let from_ids = self.match_node_ids(&from_matcher, params)?;
                let to_ids = self.match_node_ids(&to_matcher, params)?;
                let mut rows = Vec::new();
                for from in &from_ids {
                    if !matches_target_shard(target_shard, self.shard_map.owner_of_node(*from)) {
                        continue;
                    }
                    for to in &to_ids {
                        let id = self.create_relationship(
                            *from,
                            *to,
                            rel_type.clone(),
                            properties.clone(),
                        )?;
                        rows.extend(return_created_relationship(
                            variable.clone(),
                            returns.clone(),
                            Relationship::new(id, *from, *to, rel_type.clone(), properties.clone()),
                        ));
                    }
                }
                Ok(rows)
            }
            Some(WriteCypher::MergeNode {
                labels,
                properties,
                on_create,
                on_create_replacement,
                on_match,
                on_match_replacement,
                returns,
                ..
            }) => {
                if let Some(node) = self.find_merge_node(&labels, &properties, target_shard)? {
                    let node = if on_match.is_empty() && on_match_replacement.is_none() {
                        node
                    } else {
                        let after = properties_after_set(
                            node.properties.clone(),
                            &on_match,
                            on_match_replacement.as_ref(),
                        );
                        replace_node_properties(self, node.id, &node.properties, &after)?;
                        self.node(node.id)?.ok_or_else(|| {
                            DatabaseError::Graph(GraphError::NodeNotFound(node.id))
                        })?
                    };
                    return Ok(return_node_for_write(node, returns.as_ref()));
                }
                let create_properties = properties_after_set(
                    properties.clone(),
                    &on_create,
                    on_create_replacement.as_ref(),
                );
                let id = if let Some(shard_id) = target_shard {
                    self.validate_shard_id(shard_id)?;
                    let id = self.allocate_node_id_for_shard(shard_id);
                    self.write_command(
                        shard_id,
                        Command::CreateNode {
                            id,
                            labels: labels.clone(),
                            properties: create_properties.clone(),
                        },
                    )?;
                    id
                } else {
                    self.create_node(labels.clone(), create_properties.clone())?
                };
                Ok(return_node_for_write(
                    Node::new(id, labels, create_properties),
                    returns.as_ref(),
                ))
            }
            Some(WriteCypher::MergeRelationship {
                from_matcher,
                to_matcher,
                rel_type,
                properties,
                on_create,
                on_create_replacement,
                on_match,
                on_match_replacement,
                returns,
            }) => {
                let from_ids = self.match_node_ids(&from_matcher, params)?;
                let to_ids = self.match_node_ids(&to_matcher, params)?;
                let mut rows = Vec::new();
                for from in &from_ids {
                    if !matches_target_shard(target_shard, self.shard_map.owner_of_node(*from)) {
                        continue;
                    }
                    for to in &to_ids {
                        if let Some(relationship) =
                            self.find_merge_relationship(*from, *to, &rel_type, &properties)?
                        {
                            let relationship =
                                if on_match.is_empty() && on_match_replacement.is_none() {
                                    relationship
                                } else {
                                    let after = properties_after_set(
                                        relationship.properties.clone(),
                                        &on_match,
                                        on_match_replacement.as_ref(),
                                    );
                                    replace_relationship_properties(
                                        self,
                                        relationship.id,
                                        &relationship.properties,
                                        &after,
                                    )?;
                                    self.relationship(relationship.id)?.ok_or_else(|| {
                                        DatabaseError::Graph(GraphError::RelationshipNotFound(
                                            relationship.id,
                                        ))
                                    })?
                                };
                            rows.extend(return_relationship_for_write(
                                relationship,
                                returns.as_ref(),
                            ));
                            continue;
                        }
                        let create_properties = properties_after_set(
                            properties.clone(),
                            &on_create,
                            on_create_replacement.as_ref(),
                        );
                        let id = self.create_relationship(
                            *from,
                            *to,
                            rel_type.clone(),
                            create_properties.clone(),
                        )?;
                        rows.extend(return_relationship_for_write(
                            Relationship::new(id, *from, *to, rel_type.clone(), create_properties),
                            returns.as_ref(),
                        ));
                    }
                }
                Ok(rows)
            }
            Some(WriteCypher::SetNodeProperty {
                matcher,
                assignments,
                returns,
            }) => {
                let node_ids = self.match_node_ids(&matcher, params)?;
                let mut affected_ids = Vec::new();
                for id in &node_ids {
                    if !matches_target_shard(target_shard, self.shard_map.owner_of_node(*id)) {
                        continue;
                    }
                    for assignment in &assignments {
                        apply_node_property_assignment(self, *id, assignment)?;
                    }
                    affected_ids.push(*id);
                }
                return_nodes_after_write(&affected_ids, returns.as_ref(), |id| self.node(id))
            }
            Some(WriteCypher::ReplaceNodeProperties {
                matcher,
                properties,
                returns,
            }) => {
                let node_ids = self.match_node_ids(&matcher, params)?;
                let mut affected_ids = Vec::new();
                for id in &node_ids {
                    if !matches_target_shard(target_shard, self.shard_map.owner_of_node(*id)) {
                        continue;
                    }
                    let current = self
                        .node(*id)?
                        .ok_or(DatabaseError::Graph(GraphError::NodeNotFound(*id)))?;
                    replace_node_properties(self, *id, &current.properties, &properties)?;
                    affected_ids.push(*id);
                }
                return_nodes_after_write(&affected_ids, returns.as_ref(), |id| self.node(id))
            }
            Some(WriteCypher::RemoveNodeProperty {
                matcher,
                keys,
                returns,
            }) => {
                let node_ids = self.match_node_ids(&matcher, params)?;
                let mut affected_ids = Vec::new();
                for id in &node_ids {
                    if !matches_target_shard(target_shard, self.shard_map.owner_of_node(*id)) {
                        continue;
                    }
                    for key in &keys {
                        self.remove_node_property(*id, key.clone())?;
                    }
                    affected_ids.push(*id);
                }
                return_nodes_after_write(&affected_ids, returns.as_ref(), |id| self.node(id))
            }
            Some(WriteCypher::AddNodeLabel {
                matcher,
                labels,
                returns,
            }) => {
                let node_ids = self.match_node_ids(&matcher, params)?;
                let mut affected_ids = Vec::new();
                for id in &node_ids {
                    if !matches_target_shard(target_shard, self.shard_map.owner_of_node(*id)) {
                        continue;
                    }
                    for label in &labels {
                        self.add_node_label(*id, label.clone())?;
                    }
                    affected_ids.push(*id);
                }
                return_nodes_after_write(&affected_ids, returns.as_ref(), |id| self.node(id))
            }
            Some(WriteCypher::RemoveNodeLabel {
                matcher,
                labels,
                returns,
            }) => {
                let node_ids = self.match_node_ids(&matcher, params)?;
                let mut affected_ids = Vec::new();
                for id in &node_ids {
                    if !matches_target_shard(target_shard, self.shard_map.owner_of_node(*id)) {
                        continue;
                    }
                    for label in &labels {
                        self.remove_node_label(*id, label.clone())?;
                    }
                    affected_ids.push(*id);
                }
                return_nodes_after_write(&affected_ids, returns.as_ref(), |id| self.node(id))
            }
            Some(WriteCypher::SetRelationshipProperty {
                matcher,
                assignments,
                returns,
            }) => {
                let relationship_ids = self.match_relationship_ids(&matcher, params)?;
                let mut affected_ids = Vec::new();
                for id in &relationship_ids {
                    if !matches_target_shard(target_shard, self.relationship_owner_shard(*id)?) {
                        continue;
                    }
                    for assignment in &assignments {
                        apply_relationship_property_assignment(self, *id, assignment)?;
                    }
                    affected_ids.push(*id);
                }
                return_relationships_after_write(&affected_ids, returns.as_ref(), |id| {
                    self.relationship(id)
                })
            }
            Some(WriteCypher::ReplaceRelationshipProperties {
                matcher,
                properties,
                returns,
            }) => {
                let relationship_ids = self.match_relationship_ids(&matcher, params)?;
                let mut affected_ids = Vec::new();
                for id in &relationship_ids {
                    if !matches_target_shard(target_shard, self.relationship_owner_shard(*id)?) {
                        continue;
                    }
                    let current = self
                        .relationship(*id)?
                        .ok_or(DatabaseError::Graph(GraphError::RelationshipNotFound(*id)))?;
                    replace_relationship_properties(self, *id, &current.properties, &properties)?;
                    affected_ids.push(*id);
                }
                return_relationships_after_write(&affected_ids, returns.as_ref(), |id| {
                    self.relationship(id)
                })
            }
            Some(WriteCypher::RemoveRelationshipProperty {
                matcher,
                keys,
                returns,
            }) => {
                let relationship_ids = self.match_relationship_ids(&matcher, params)?;
                let mut affected_ids = Vec::new();
                for id in &relationship_ids {
                    if !matches_target_shard(target_shard, self.relationship_owner_shard(*id)?) {
                        continue;
                    }
                    for key in &keys {
                        self.remove_relationship_property(*id, key.clone())?;
                    }
                    affected_ids.push(*id);
                }
                return_relationships_after_write(&affected_ids, returns.as_ref(), |id| {
                    self.relationship(id)
                })
            }
            Some(WriteCypher::DeleteNode { matcher, returns }) => {
                let node_ids = self.match_node_ids(&matcher, params)?;
                let mut affected_ids = Vec::new();
                for id in &node_ids {
                    if !matches_target_shard(target_shard, self.shard_map.owner_of_node(*id)) {
                        continue;
                    }
                    affected_ids.push(*id);
                }
                let rows =
                    return_nodes_after_write(&affected_ids, returns.as_ref(), |id| self.node(id))?;
                for id in &affected_ids {
                    self.delete_node(*id)?;
                }
                Ok(rows)
            }
            Some(WriteCypher::DeleteRelationship { matcher, returns }) => {
                let relationship_ids = self.match_relationship_ids(&matcher, params)?;
                let mut affected_ids = Vec::new();
                for id in &relationship_ids {
                    if !matches_target_shard(target_shard, self.relationship_owner_shard(*id)?) {
                        continue;
                    }
                    affected_ids.push(*id);
                }
                let rows =
                    return_relationships_after_write(&affected_ids, returns.as_ref(), |id| {
                        self.relationship(id)
                    })?;
                for id in &affected_ids {
                    self.delete_relationship(*id)?;
                }
                Ok(rows)
            }
            Some(WriteCypher::CreateNodePropertyIndex {
                name,
                label,
                property,
                if_not_exists,
            }) => {
                ensure_metadata_target_shard(target_shard)?;
                if if_not_exists {
                    self.create_node_property_index_if_not_exists(name, label, property)?;
                } else {
                    self.create_node_property_index(name, label, property)?;
                }
                Ok(Vec::new())
            }
            Some(WriteCypher::CreateUniqueNodePropertyConstraint {
                name,
                label,
                property,
                if_not_exists,
            }) => {
                ensure_metadata_target_shard(target_shard)?;
                if if_not_exists {
                    self.create_unique_node_property_constraint_if_not_exists(
                        name, label, property,
                    )?;
                } else {
                    self.create_unique_node_property_constraint(name, label, property)?;
                }
                Ok(Vec::new())
            }
            Some(WriteCypher::CreateVectorIndex {
                name,
                label,
                property,
                dimensions,
                metric,
                if_not_exists,
            }) => {
                ensure_metadata_target_shard(target_shard)?;
                if if_not_exists {
                    self.create_vector_index_if_not_exists(
                        name, label, property, dimensions, metric,
                    )?;
                } else {
                    self.create_vector_index(name, label, property, dimensions, metric)?;
                }
                Ok(Vec::new())
            }
            Some(WriteCypher::RebuildVectorIndex { name }) => {
                ensure_metadata_target_shard(target_shard)?;
                self.rebuild_vector_index(&name)?;
                Ok(Vec::new())
            }
            Some(WriteCypher::DropIndex { name, if_exists }) => {
                ensure_metadata_target_shard(target_shard)?;
                if if_exists {
                    self.drop_index_if_exists(&name)?;
                } else {
                    self.drop_index(&name)?;
                }
                Ok(Vec::new())
            }
            Some(WriteCypher::DropConstraint { name, if_exists }) => {
                ensure_metadata_target_shard(target_shard)?;
                if if_exists {
                    self.drop_constraint_if_exists(&name)?;
                } else {
                    self.drop_constraint(&name)?;
                }
                Ok(Vec::new())
            }
            None => self.query_with_params(query, params),
        }
    }

    pub fn query(&self, query: &str) -> DatabaseResult<Vec<QueryRow>> {
        Ok(self.query_engine.execute(&self.store, query)?)
    }

    pub fn node(&self, id: NodeId) -> DatabaseResult<Option<Node>> {
        if let Some(node) = self
            .read_cache
            .lock()
            .map_err(|_| DatabaseError::LockPoisoned)?
            .nodes
            .get(&id)
            .cloned()
        {
            self.observe_read_cache_hit()?;
            return Ok(node);
        }
        self.observe_read_cache_miss()?;
        let node = self.store.node(id).map_err(DatabaseError::from)?;
        self.read_cache
            .lock()
            .map_err(|_| DatabaseError::LockPoisoned)?
            .nodes
            .insert(id, node.clone());
        Ok(node)
    }

    pub fn relationship(&self, id: RelationshipId) -> DatabaseResult<Option<Relationship>> {
        if let Some(relationship) = self
            .read_cache
            .lock()
            .map_err(|_| DatabaseError::LockPoisoned)?
            .relationships
            .get(&id)
            .cloned()
        {
            self.observe_read_cache_hit()?;
            return Ok(relationship);
        }
        self.observe_read_cache_miss()?;
        let relationship = self.store.relationship(id).map_err(DatabaseError::from)?;
        self.read_cache
            .lock()
            .map_err(|_| DatabaseError::LockPoisoned)?
            .relationships
            .insert(id, relationship.clone());
        Ok(relationship)
    }

    pub fn query_with_params(
        &self,
        query: &str,
        params: &QueryParams,
    ) -> DatabaseResult<Vec<QueryRow>> {
        if let Some(rows) = self.show_index_rows_for_query(query)? {
            return Ok(rows);
        }
        if let Some(rows) = self.show_constraint_rows_for_query(query)? {
            return Ok(rows);
        }
        Ok(self
            .query_engine
            .execute_with_params(&self.store, query, params)?)
    }

    pub fn query_cursor(&self, query: &str) -> DatabaseResult<Box<dyn QueryCursor>> {
        if let Some(rows) = self.show_index_rows_for_query(query)? {
            return Ok(Box::new(VecQueryCursor::new(rows)));
        }
        if let Some(rows) = self.show_constraint_rows_for_query(query)? {
            return Ok(Box::new(VecQueryCursor::new(rows)));
        }
        Ok(self.query_engine.execute_cursor(&self.store, query)?)
    }

    pub fn query_cursor_with_params(
        &self,
        query: &str,
        params: &QueryParams,
    ) -> DatabaseResult<Box<dyn QueryCursor>> {
        if let Some(rows) = self.show_index_rows_for_query(query)? {
            return Ok(Box::new(VecQueryCursor::new(rows)));
        }
        if let Some(rows) = self.show_constraint_rows_for_query(query)? {
            return Ok(Box::new(VecQueryCursor::new(rows)));
        }
        Ok(self
            .query_engine
            .execute_cursor_with_params(&self.store, query, params)?)
    }

    pub fn shard_count(&self) -> u64 {
        self.shard_map.shard_count()
    }

    pub fn local_partition_count(&self) -> usize {
        self.store.partition_count()
    }

    pub fn data_dir(&self) -> &Path {
        &self.config.data_dir
    }

    pub fn routing_table(&self) -> &ShardRoutingTable {
        &self.routing_table
    }

    pub fn log_entries_from(
        &self,
        shard_id: ShardId,
        start_index: LogIndex,
    ) -> DatabaseResult<Vec<LogEntry>> {
        Ok(self.log(shard_id)?.replay_from(start_index)?)
    }

    pub fn create_node_property_index(
        &mut self,
        name: impl Into<String>,
        label: impl Into<String>,
        property: impl Into<String>,
    ) -> DatabaseResult<()> {
        self.add_index_definition(IndexDefinition::node_property(name, label, property))
    }

    pub fn create_node_property_index_if_not_exists(
        &mut self,
        name: impl Into<String>,
        label: impl Into<String>,
        property: impl Into<String>,
    ) -> DatabaseResult<()> {
        self.add_index_definition_if_not_exists(IndexDefinition::node_property(
            name, label, property,
        ))
    }

    pub fn create_unique_node_property_constraint(
        &mut self,
        name: impl Into<String>,
        label: impl Into<String>,
        property: impl Into<String>,
    ) -> DatabaseResult<()> {
        self.add_index_definition(IndexDefinition::unique_node_property(name, label, property))
    }

    pub fn create_unique_node_property_constraint_if_not_exists(
        &mut self,
        name: impl Into<String>,
        label: impl Into<String>,
        property: impl Into<String>,
    ) -> DatabaseResult<()> {
        self.add_index_definition_if_not_exists(IndexDefinition::unique_node_property(
            name, label, property,
        ))
    }

    pub fn create_vector_index(
        &mut self,
        name: impl Into<String>,
        label: impl Into<String>,
        property: impl Into<String>,
        dimensions: usize,
        metric: impl Into<String>,
    ) -> DatabaseResult<()> {
        if dimensions == 0 {
            return Err(DatabaseError::InvalidConfig(
                "vector index dimensions must be greater than zero".to_string(),
            ));
        }
        self.add_index_definition(IndexDefinition::vector(
            name, label, property, dimensions, metric,
        ))
    }

    pub fn create_vector_index_if_not_exists(
        &mut self,
        name: impl Into<String>,
        label: impl Into<String>,
        property: impl Into<String>,
        dimensions: usize,
        metric: impl Into<String>,
    ) -> DatabaseResult<()> {
        if dimensions == 0 {
            return Err(DatabaseError::InvalidConfig(
                "vector index dimensions must be greater than zero".to_string(),
            ));
        }
        self.add_index_definition_if_not_exists(IndexDefinition::vector(
            name, label, property, dimensions, metric,
        ))
    }

    pub fn drop_index(&mut self, name: &str) -> DatabaseResult<()> {
        let before = self.index_catalog.indexes.len();
        self.index_catalog
            .indexes
            .retain(|index| index.name != name);
        if self.index_catalog.indexes.len() == before {
            return Err(DatabaseError::InvalidConfig(format!(
                "index {name:?} does not exist"
            )));
        }
        self.index_catalog.version += 1;
        self.index_catalog_store.save(&self.index_catalog)?;
        self.vector_indexes
            .lock()
            .map_err(|_| DatabaseError::LockPoisoned)?
            .remove(name);
        self.save_vector_index_cache()?;
        Ok(())
    }

    pub fn drop_index_if_exists(&mut self, name: &str) -> DatabaseResult<()> {
        if self
            .index_catalog
            .indexes
            .iter()
            .any(|index| index.name == name)
        {
            self.drop_index(name)
        } else {
            Ok(())
        }
    }

    pub fn drop_constraint(&mut self, name: &str) -> DatabaseResult<()> {
        let Some(index) = self
            .index_catalog
            .indexes
            .iter()
            .find(|index| index.name == name)
        else {
            return Err(DatabaseError::InvalidConfig(format!(
                "constraint {name:?} does not exist"
            )));
        };
        if !matches!(index.kind, IndexKind::UniqueNodeProperty) {
            return Err(DatabaseError::InvalidConfig(format!(
                "index {name:?} is not a constraint"
            )));
        }
        self.drop_index(name)
    }

    pub fn drop_constraint_if_exists(&mut self, name: &str) -> DatabaseResult<()> {
        let Some(index) = self
            .index_catalog
            .indexes
            .iter()
            .find(|index| index.name == name)
        else {
            return Ok(());
        };
        if !matches!(index.kind, IndexKind::UniqueNodeProperty) {
            return Err(DatabaseError::InvalidConfig(format!(
                "index {name:?} is not a constraint"
            )));
        }
        self.drop_index(name)
    }

    pub fn list_indexes(&self) -> Vec<IndexDefinition> {
        self.index_catalog.indexes.clone()
    }

    pub fn show_indexes(&self) -> Vec<QueryRow> {
        format_index_rows(&self.index_catalog.indexes)
    }

    pub fn show_index(&self, name: &str) -> DatabaseResult<Vec<QueryRow>> {
        Ok(vec![format_index_row_by_name(
            &self.index_catalog.indexes,
            name,
        )?])
    }

    pub fn show_vector_indexes(&self) -> Vec<QueryRow> {
        format_vector_index_rows(&self.index_catalog.indexes)
    }

    pub fn show_vector_index(&self, name: &str) -> DatabaseResult<Vec<QueryRow>> {
        Ok(vec![format_vector_index_row_by_name(
            &self.index_catalog.indexes,
            name,
        )?])
    }

    pub fn show_vector_index_status(&self) -> DatabaseResult<Vec<QueryRow>> {
        Ok(format_vector_index_status_rows(
            &self.vector_index_status()?,
        ))
    }

    pub fn show_vector_index_status_by_name(&self, name: &str) -> DatabaseResult<Vec<QueryRow>> {
        Ok(format_vector_index_status_rows(&[
            self.vector_index_status_by_name(name)?
        ]))
    }

    fn show_index_rows_for_query(&self, query: &str) -> DatabaseResult<Option<Vec<QueryRow>>> {
        if let Some(name) = show_vector_index_status_name(query)? {
            Ok(Some(self.show_vector_index_status_by_name(&name)?))
        } else if is_show_vector_index_status_cypher(query) {
            Ok(Some(self.show_vector_index_status()?))
        } else if let Some(name) = show_vector_index_name(query)? {
            Ok(Some(self.show_vector_index(&name)?))
        } else if let Some(name) = show_index_name(query)? {
            Ok(Some(self.show_index(&name)?))
        } else if is_show_vector_indexes_cypher(query) {
            Ok(Some(self.show_vector_indexes()))
        } else if is_show_indexes_cypher(query) {
            Ok(Some(self.show_indexes()))
        } else {
            Ok(None)
        }
    }

    pub fn show_constraints(&self) -> Vec<QueryRow> {
        format_constraint_rows(&self.index_catalog.indexes)
    }

    pub fn show_constraint(&self, name: &str) -> DatabaseResult<Vec<QueryRow>> {
        Ok(vec![format_constraint_row_by_name(
            &self.index_catalog.indexes,
            name,
        )?])
    }

    fn show_constraint_rows_for_query(&self, query: &str) -> DatabaseResult<Option<Vec<QueryRow>>> {
        if let Some(name) = show_constraint_name(query)? {
            Ok(Some(self.show_constraint(&name)?))
        } else if is_show_constraints_cypher(query) {
            Ok(Some(self.show_constraints()))
        } else {
            Ok(None)
        }
    }

    pub fn vector_index_status(&self) -> DatabaseResult<Vec<VectorIndexStatus>> {
        Ok(self
            .vector_indexes
            .lock()
            .map_err(|_| DatabaseError::LockPoisoned)?
            .status())
    }

    pub fn vector_index_status_by_name(&self, name: &str) -> DatabaseResult<VectorIndexStatus> {
        let statuses = self.vector_index_status()?;
        statuses
            .into_iter()
            .find(|status| status.name == name)
            .ok_or_else(|| {
                DatabaseError::InvalidConfig(format!("vector index {name:?} does not exist"))
            })
    }

    pub fn index_catalog(&self) -> IndexCatalog {
        self.index_catalog.clone()
    }

    pub fn install_index_catalog(&mut self, catalog: IndexCatalog) -> DatabaseResult<()> {
        validate_index_catalog(&catalog)?;
        if catalog.version < self.index_catalog.version {
            return Err(DatabaseError::InvalidConfig(format!(
                "index catalog version must not move backwards from {} to {}",
                self.index_catalog.version, catalog.version
            )));
        }
        if catalog.version == self.index_catalog.version
            && catalog.indexes == self.index_catalog.indexes
        {
            return Ok(());
        }
        self.validate_index_catalog_against_store(&catalog)?;
        let vector_indexes = self.build_vector_indexes_for_catalog(&catalog)?;
        self.index_catalog_store.save(&catalog)?;
        self.index_catalog = catalog;
        *self
            .vector_indexes
            .lock()
            .map_err(|_| DatabaseError::LockPoisoned)? = vector_indexes;
        self.save_vector_index_cache()
    }

    fn match_node_ids(
        &self,
        matcher: &NodeMatcher,
        params: &QueryParams,
    ) -> DatabaseResult<Vec<NodeId>> {
        query_match_node_ids(|query| self.query_with_params(query, params), matcher)
    }

    fn match_relationship_ids(
        &self,
        matcher: &RelationshipMatcher,
        params: &QueryParams,
    ) -> DatabaseResult<Vec<RelationshipId>> {
        query_match_relationship_ids(|query| self.query_with_params(query, params), matcher)
    }

    pub fn install_routing_table(
        &mut self,
        routing_table: ShardRoutingTable,
    ) -> DatabaseResult<()> {
        self.ensure_metadata_authority()?;
        validate_routing_table(&routing_table, self.shard_map.shard_count())?;
        if routing_table.version <= self.routing_table.version {
            return Err(DatabaseError::InvalidConfig(format!(
                "routing table version must increase from {} to a greater version",
                self.routing_table.version
            )));
        }
        self.shard_metadata.save(&routing_table)?;
        self.replicator
            .install_routing_table(routing_table.clone())?;
        self.cluster_metadata.config_epoch = routing_table.version;
        self.cluster_metadata_store.save(&self.cluster_metadata)?;
        self.append_metadata_operation("install_routing_table")?;
        self.routing_table = routing_table;
        Ok(())
    }

    pub fn register_replication_peer(
        &mut self,
        server_id: ServerId,
        address: String,
    ) -> DatabaseResult<()> {
        self.replicator.register_peer_address(server_id, address)
    }

    pub fn unregister_replication_peer(&mut self, server_id: ServerId) -> DatabaseResult<()> {
        self.replicator.unregister_peer_address(server_id)
    }

    pub fn cluster_membership(&self) -> &ClusterMembership {
        &self.membership
    }

    pub fn cluster_metadata(&self) -> &ClusterMetadataState {
        &self.cluster_metadata
    }

    pub fn set_metadata_authority(
        &mut self,
        server_id: ServerId,
    ) -> DatabaseResult<ClusterMetadataState> {
        self.cluster_metadata.authority_server_id = server_id;
        self.cluster_metadata.term = self.cluster_metadata.term.saturating_add(1);
        self.cluster_metadata_store.save(&self.cluster_metadata)?;
        self.append_metadata_operation("set_metadata_authority")?;
        Ok(self.cluster_metadata.clone())
    }

    pub fn set_rebalance_policy(
        &mut self,
        policy: RebalancePolicy,
    ) -> DatabaseResult<ClusterMetadataState> {
        self.ensure_metadata_authority()?;
        if policy.replication_factor == 0 {
            return Err(DatabaseError::InvalidConfig(
                "replication factor must be greater than zero".to_string(),
            ));
        }
        if policy.max_steps_per_plan == 0 {
            return Err(DatabaseError::InvalidConfig(
                "max steps per plan must be greater than zero".to_string(),
            ));
        }
        self.cluster_metadata.policy = policy;
        self.cluster_metadata_store.save(&self.cluster_metadata)?;
        self.append_metadata_operation("set_rebalance_policy")?;
        Ok(self.cluster_metadata.clone())
    }

    pub fn register_cluster_node(
        &mut self,
        server_id: ServerId,
        address: String,
    ) -> DatabaseResult<ClusterMembership> {
        self.ensure_metadata_authority()?;
        validate_cluster_node_address(&address)?;
        if let Some(node) = self
            .membership
            .nodes
            .iter_mut()
            .find(|node| node.server_id == server_id)
        {
            node.address = address;
            if matches!(
                node.state,
                NodeMembershipState::Removed
                    | NodeMembershipState::Dead
                    | NodeMembershipState::Rejected
            ) {
                node.state = NodeMembershipState::Joining;
                node.rejection_reason.clear();
            }
        } else {
            self.membership.nodes.push(ClusterNode {
                server_id,
                address,
                state: NodeMembershipState::Joining,
                protocol_version: 0,
                storage_version: 0,
                shard_count: self.shard_map.shard_count(),
                rejection_reason: String::new(),
            });
        }
        self.save_membership()?;
        self.append_metadata_operation("register_cluster_node")?;
        Ok(self.membership.clone())
    }

    pub fn request_cluster_join(
        &mut self,
        server_id: ServerId,
        address: String,
        protocol_version: u64,
        storage_version: u64,
        shard_count: u64,
    ) -> DatabaseResult<ClusterMembership> {
        self.ensure_metadata_authority()?;
        validate_cluster_node_address(&address)?;
        let rejection_reason = self.join_rejection_reason(server_id, shard_count);
        let state = if rejection_reason.is_empty() {
            NodeMembershipState::Negotiating
        } else {
            NodeMembershipState::Rejected
        };
        match self
            .membership
            .nodes
            .iter_mut()
            .find(|node| node.server_id == server_id)
        {
            Some(node) => {
                node.address = address;
                node.protocol_version = protocol_version;
                node.storage_version = storage_version;
                node.shard_count = shard_count;
                node.state = state;
                node.rejection_reason = rejection_reason;
            }
            None => self.membership.nodes.push(ClusterNode {
                server_id,
                address,
                state,
                protocol_version,
                storage_version,
                shard_count,
                rejection_reason,
            }),
        }
        self.save_membership()?;
        self.append_metadata_operation("request_cluster_join")?;
        Ok(self.membership.clone())
    }

    pub fn accept_cluster_join(
        &mut self,
        server_id: ServerId,
    ) -> DatabaseResult<ClusterMembership> {
        self.ensure_metadata_authority()?;
        let Some(node) = self
            .membership
            .nodes
            .iter_mut()
            .find(|node| node.server_id == server_id)
        else {
            return Err(DatabaseError::InvalidConfig(format!(
                "cluster node {server_id} does not exist"
            )));
        };
        if node.state != NodeMembershipState::Negotiating {
            return Err(DatabaseError::InvalidConfig(format!(
                "cluster node {server_id} is not negotiating"
            )));
        }
        node.state = NodeMembershipState::Joining;
        node.rejection_reason.clear();
        self.save_membership()?;
        self.append_metadata_operation("accept_cluster_join")?;
        Ok(self.membership.clone())
    }

    pub fn reject_cluster_join(
        &mut self,
        server_id: ServerId,
        reason: String,
    ) -> DatabaseResult<ClusterMembership> {
        self.ensure_metadata_authority()?;
        validate_rejection_reason(&reason)?;
        let Some(node) = self
            .membership
            .nodes
            .iter_mut()
            .find(|node| node.server_id == server_id)
        else {
            return Err(DatabaseError::InvalidConfig(format!(
                "cluster node {server_id} does not exist"
            )));
        };
        node.state = NodeMembershipState::Rejected;
        node.rejection_reason = reason;
        self.save_membership()?;
        self.append_metadata_operation("reject_cluster_join")?;
        Ok(self.membership.clone())
    }

    pub fn decommission_cluster_node(
        &mut self,
        server_id: ServerId,
    ) -> DatabaseResult<ClusterMembership> {
        self.ensure_metadata_authority()?;
        let Some(node) = self
            .membership
            .nodes
            .iter_mut()
            .find(|node| node.server_id == server_id)
        else {
            return Err(DatabaseError::InvalidConfig(format!(
                "cluster node {server_id} does not exist"
            )));
        };
        node.state = if self
            .routing_table
            .placements
            .iter()
            .any(|placement| placement.has_server(server_id))
        {
            NodeMembershipState::Draining
        } else {
            NodeMembershipState::Removed
        };
        self.save_membership()?;
        self.append_metadata_operation("decommission_cluster_node")?;
        Ok(self.membership.clone())
    }

    pub fn plan_rebalance(&mut self) -> DatabaseResult<RebalancePlan> {
        self.ensure_metadata_authority()?;
        let mut steps = self.generate_rebalance_steps();
        if steps.len() > self.cluster_metadata.policy.max_steps_per_plan {
            steps.truncate(self.cluster_metadata.policy.max_steps_per_plan);
        }
        let plan = RebalancePlan {
            plan_id: self.rebalance_plan_store.next_plan_id()?,
            state: RebalancePlanState::Proposed,
            from_routing_version: self.routing_table.version,
            target_routing_version: self.routing_table.version + steps.len() as u64,
            steps,
        };
        self.rebalance_plan_store.save(&plan)?;
        self.append_metadata_operation("plan_rebalance")?;
        Ok(plan)
    }

    fn generate_rebalance_steps(&self) -> Vec<RebalanceStep> {
        let mut steps = Vec::new();
        let replication_factor = self.cluster_metadata.policy.replication_factor;
        let mut planned_replica_counts = self
            .routing_table
            .placements
            .iter()
            .map(|placement| (placement.shard_id, placement.replicas.len()))
            .collect::<BTreeMap<_, _>>();
        let joining = self
            .membership
            .nodes
            .iter()
            .filter(|node| node.state == NodeMembershipState::Joining)
            .map(|node| node.server_id)
            .collect::<Vec<_>>();
        for server_id in joining {
            for placement in &self.routing_table.placements {
                let replica_count = planned_replica_counts
                    .get(&placement.shard_id)
                    .copied()
                    .unwrap_or_default();
                if replica_count >= replication_factor {
                    continue;
                }
                if !placement.has_server(server_id)
                    && !self.has_active_assignment(placement.shard_id, server_id)
                {
                    steps.push(RebalanceStep::AddReplica {
                        shard_id: placement.shard_id,
                        server_id,
                    });
                    planned_replica_counts.insert(placement.shard_id, replica_count + 1);
                }
            }
        }

        for node in self
            .membership
            .nodes
            .iter()
            .filter(|node| node.state == NodeMembershipState::Draining)
        {
            for placement in &self.routing_table.placements {
                if placement.primary_server_id() == Some(node.server_id) {
                    if let Some(target) = placement
                        .replicas
                        .iter()
                        .find(|replica| replica.server_id != node.server_id)
                        .map(|replica| replica.server_id)
                    {
                        steps.push(RebalanceStep::TransferPrimary {
                            shard_id: placement.shard_id,
                            from: node.server_id,
                            to: target,
                        });
                    }
                } else if placement.has_server(node.server_id) {
                    steps.push(RebalanceStep::RemoveReplica {
                        shard_id: placement.shard_id,
                        server_id: node.server_id,
                    });
                }
            }
        }
        steps
    }

    pub fn start_rebalance_plan(&mut self) -> DatabaseResult<RebalanceExecution> {
        self.ensure_metadata_authority()?;
        if let Some(execution) = &self.rebalance_execution {
            if matches!(
                execution.state,
                RebalancePlanState::Proposed | RebalancePlanState::Running
            ) {
                return Ok(execution.clone());
            }
        }
        let mut plan = self.plan_rebalance()?;
        plan.state = RebalancePlanState::Running;
        self.rebalance_plan_store.save(&plan)?;
        let execution = RebalanceExecution {
            current_step: 0,
            steps: plan
                .steps
                .iter()
                .cloned()
                .enumerate()
                .map(|(step_index, step)| RebalanceStepExecution {
                    step_index,
                    step,
                    state: RebalanceStepState::Pending,
                    attempts: 0,
                    retryable: true,
                    last_error: String::new(),
                })
                .collect(),
            plan,
            state: RebalancePlanState::Running,
            last_error: String::new(),
        };
        self.rebalance_execution_store.save(&execution)?;
        self.rebalance_execution = Some(execution.clone());
        Ok(execution)
    }

    pub fn cancel_rebalance_plan(&mut self) -> DatabaseResult<RebalanceExecution> {
        self.ensure_metadata_authority()?;
        let mut execution = self.rebalance_execution.clone().ok_or_else(|| {
            DatabaseError::InvalidConfig("no active rebalance execution".to_string())
        })?;
        execution.state = RebalancePlanState::Cancelled;
        execution.plan.state = RebalancePlanState::Cancelled;
        for step in &mut execution.steps {
            if step.state != RebalanceStepState::Applied {
                step.state = RebalanceStepState::Cancelled;
            }
        }
        self.rebalance_plan_store.save(&execution.plan)?;
        self.rebalance_execution_store.save(&execution)?;
        self.rebalance_execution = Some(execution.clone());
        Ok(execution)
    }

    pub fn rebalance_status(&self) -> Option<&RebalanceExecution> {
        self.rebalance_execution.as_ref()
    }

    pub fn advance_rebalance(&mut self) -> DatabaseResult<RebalanceAdvanceResult> {
        self.ensure_metadata_authority()?;
        if self.rebalance_execution.is_none() {
            self.start_rebalance_plan()?;
        }
        let mut execution = self.rebalance_execution.clone().ok_or_else(|| {
            DatabaseError::InvalidConfig("no active rebalance execution".to_string())
        })?;
        if execution.state != RebalancePlanState::Running {
            return Ok(RebalanceAdvanceResult {
                execution,
                action: "idle".to_string(),
            });
        }
        let Some(step_index) = execution
            .steps
            .iter()
            .position(|step| step.state != RebalanceStepState::Applied)
        else {
            execution.state = RebalancePlanState::Completed;
            execution.plan.state = RebalancePlanState::Completed;
            self.rebalance_plan_store.save(&execution.plan)?;
            self.rebalance_execution_store.save(&execution)?;
            self.rebalance_execution = Some(execution.clone());
            return Ok(RebalanceAdvanceResult {
                execution,
                action: "completed".to_string(),
            });
        };
        execution.current_step = step_index;
        let step = execution.steps[step_index].step.clone();
        execution.steps[step_index].attempts =
            execution.steps[step_index].attempts.saturating_add(1);
        let result = self.advance_rebalance_step(&step, &mut execution.steps[step_index]);
        let action = match result {
            Ok(action) => {
                execution.last_error.clear();
                action
            }
            Err(err) => {
                execution.state = RebalancePlanState::Failed;
                execution.plan.state = RebalancePlanState::Failed;
                execution.last_error = sanitize_cluster_text(&err.to_string());
                execution.steps[step_index].state = RebalanceStepState::Failed;
                execution.steps[step_index].last_error = execution.last_error.clone();
                execution.steps[step_index].retryable = is_retryable_rebalance_error(&err);
                "failed".to_string()
            }
        };
        if execution
            .steps
            .iter()
            .all(|step| step.state == RebalanceStepState::Applied)
        {
            execution.state = RebalancePlanState::Completed;
            execution.plan.state = RebalancePlanState::Completed;
        }
        self.rebalance_plan_store.save(&execution.plan)?;
        self.rebalance_execution_store.save(&execution)?;
        self.rebalance_execution = Some(execution.clone());
        Ok(RebalanceAdvanceResult { execution, action })
    }

    pub fn cluster_management_status(&self) -> ClusterManagementStatus {
        ClusterManagementStatus {
            metadata: self.cluster_metadata.clone(),
            membership: self.membership.clone(),
            rebalance_plan: self.rebalance_plan_store.load().ok().flatten(),
            rebalance_execution: self.rebalance_execution.clone(),
            routing_version: self.routing_table.version,
        }
    }

    pub fn prepare_rebalance_step(
        &mut self,
        step: RebalanceStep,
    ) -> DatabaseResult<ClusterMembership> {
        self.ensure_metadata_authority()?;
        match step {
            RebalanceStep::AddReplica {
                shard_id,
                server_id,
            } => {
                self.ensure_joining_or_active_node(server_id)?;
                if self
                    .routing_table
                    .placement(shard_id)
                    .map(|placement| placement.has_server(server_id))
                    .unwrap_or(false)
                {
                    return Err(DatabaseError::InvalidConfig(format!(
                        "server {server_id} already serves shard {shard_id}"
                    )));
                }
                match self.assignment_mut(shard_id, server_id) {
                    Some(assignment) => {
                        assignment.state = ShardAssignmentState::CatchingUp;
                        assignment.match_index = 0;
                    }
                    None => self
                        .membership
                        .shard_assignments
                        .push(ClusterShardAssignment {
                            shard_id,
                            server_id,
                            state: ShardAssignmentState::CatchingUp,
                            match_index: 0,
                        }),
                }
            }
            RebalanceStep::TransferPrimary { shard_id, from, to } => {
                let placement = self.routing_table.placement(shard_id).ok_or_else(|| {
                    DatabaseError::InvalidConfig(format!("routing table missing shard {shard_id}"))
                })?;
                if placement.primary_server_id() != Some(from) || !placement.has_server(to) {
                    return Err(DatabaseError::InvalidConfig(
                        "primary transfer step does not match current placement".to_string(),
                    ));
                }
            }
            RebalanceStep::RemoveReplica {
                shard_id,
                server_id,
            } => {
                let placement = self.routing_table.placement(shard_id).ok_or_else(|| {
                    DatabaseError::InvalidConfig(format!("routing table missing shard {shard_id}"))
                })?;
                if !placement.has_server(server_id) {
                    return Err(DatabaseError::InvalidConfig(format!(
                        "server {server_id} does not serve shard {shard_id}"
                    )));
                }
            }
        }
        self.save_membership()?;
        Ok(self.membership.clone())
    }

    pub fn mark_shard_caught_up(
        &mut self,
        shard_id: ShardId,
        server_id: ServerId,
        match_index: LogIndex,
    ) -> DatabaseResult<ClusterMembership> {
        self.ensure_metadata_authority()?;
        let Some(assignment) = self.assignment_mut(shard_id, server_id) else {
            return Err(DatabaseError::InvalidConfig(format!(
                "shard {shard_id} server {server_id} has no prepared assignment"
            )));
        };
        if !matches!(
            assignment.state,
            ShardAssignmentState::Planned
                | ShardAssignmentState::CatchingUp
                | ShardAssignmentState::CaughtUp
        ) {
            return Err(DatabaseError::InvalidConfig(format!(
                "shard {shard_id} server {server_id} assignment is not catch-up eligible"
            )));
        }
        assignment.state = ShardAssignmentState::CaughtUp;
        assignment.match_index = match_index;
        self.save_membership()?;
        Ok(self.membership.clone())
    }

    pub fn apply_rebalance_step(
        &mut self,
        step: RebalanceStep,
    ) -> DatabaseResult<ShardRoutingTable> {
        self.ensure_metadata_authority()?;
        let mut routing_table = self.routing_table.clone();
        routing_table.version = routing_table.version.saturating_add(1);
        match step {
            RebalanceStep::AddReplica {
                shard_id,
                server_id,
            } => {
                self.ensure_assignment_caught_up(shard_id, server_id)?;
                let placement = mutable_placement(&mut routing_table, shard_id)?;
                if !placement.has_server(server_id) {
                    placement.replicas.push(ShardReplica::replica(server_id));
                }
            }
            RebalanceStep::TransferPrimary { shard_id, from, to } => {
                self.ensure_primary_transfer_target_ready(shard_id, to)?;
                let placement = mutable_placement(&mut routing_table, shard_id)?;
                if placement.primary_server_id() != Some(from) {
                    return Err(DatabaseError::InvalidConfig(format!(
                        "shard {shard_id} primary is not server {from}"
                    )));
                }
                if !placement.has_server(to) {
                    return Err(DatabaseError::InvalidConfig(format!(
                        "server {to} is not a replica of shard {shard_id}"
                    )));
                }
                for replica in &mut placement.replicas {
                    replica.role = if replica.server_id == to {
                        ShardRole::Primary
                    } else {
                        ShardRole::Replica
                    };
                }
            }
            RebalanceStep::RemoveReplica {
                shard_id,
                server_id,
            } => {
                let placement = mutable_placement(&mut routing_table, shard_id)?;
                if !placement.has_server(server_id) {
                    return Err(DatabaseError::InvalidConfig(format!(
                        "server {server_id} does not serve shard {shard_id}"
                    )));
                }
                if placement.primary_server_id() == Some(server_id) {
                    return Err(DatabaseError::InvalidConfig(format!(
                        "cannot remove primary server {server_id} from shard {shard_id}"
                    )));
                }
                if placement.replicas.len() <= 1 {
                    return Err(DatabaseError::InvalidConfig(format!(
                        "cannot remove the last replica from shard {shard_id}"
                    )));
                }
                placement
                    .replicas
                    .retain(|replica| replica.server_id != server_id);
            }
        }
        self.install_routing_table(routing_table.clone())?;
        self.finish_applied_assignments(&step)?;
        self.finish_drained_nodes()?;
        Ok(routing_table)
    }

    fn advance_rebalance_step(
        &mut self,
        step: &RebalanceStep,
        execution_step: &mut RebalanceStepExecution,
    ) -> DatabaseResult<String> {
        match step {
            RebalanceStep::AddReplica {
                shard_id,
                server_id,
            } => match execution_step.state {
                RebalanceStepState::Pending
                | RebalanceStepState::Preparing
                | RebalanceStepState::Failed => {
                    execution_step.state = RebalanceStepState::Preparing;
                    self.prepare_rebalance_step(step.clone())?;
                    execution_step.state = RebalanceStepState::CatchingUp;
                    Ok("prepared".to_string())
                }
                RebalanceStepState::CatchingUp => {
                    let committed_index = self.committed_index(*shard_id)?;
                    let observed_index = self.observed_match_index(*shard_id, *server_id);
                    if observed_index >= committed_index {
                        self.mark_shard_caught_up(*shard_id, *server_id, observed_index)?;
                        execution_step.state = RebalanceStepState::Ready;
                        Ok("caught_up".to_string())
                    } else {
                        Ok(format!(
                            "waiting_for_catch_up shard={shard_id} server={server_id} match_index={observed_index} committed_index={committed_index}"
                        ))
                    }
                }
                RebalanceStepState::Ready | RebalanceStepState::Applying => {
                    execution_step.state = RebalanceStepState::Applying;
                    self.apply_rebalance_step(step.clone())?;
                    execution_step.state = RebalanceStepState::Applied;
                    Ok("applied".to_string())
                }
                RebalanceStepState::Applied => Ok("already_applied".to_string()),
                RebalanceStepState::Cancelled => Ok("cancelled".to_string()),
            },
            RebalanceStep::TransferPrimary { .. } | RebalanceStep::RemoveReplica { .. } => {
                match execution_step.state {
                    RebalanceStepState::Pending
                    | RebalanceStepState::Preparing
                    | RebalanceStepState::Ready
                    | RebalanceStepState::Applying
                    | RebalanceStepState::Failed => {
                        execution_step.state = RebalanceStepState::Applying;
                        self.apply_rebalance_step(step.clone())?;
                        execution_step.state = RebalanceStepState::Applied;
                        Ok("applied".to_string())
                    }
                    RebalanceStepState::Applied => Ok("already_applied".to_string()),
                    RebalanceStepState::CatchingUp => Ok("ready".to_string()),
                    RebalanceStepState::Cancelled => Ok("cancelled".to_string()),
                }
            }
        }
    }

    fn finish_applied_assignments(&mut self, step: &RebalanceStep) -> DatabaseResult<()> {
        match step {
            RebalanceStep::AddReplica {
                shard_id,
                server_id,
            } => {
                if let Some(assignment) = self.assignment_mut(*shard_id, *server_id) {
                    assignment.state = ShardAssignmentState::ServingReplica;
                }
            }
            RebalanceStep::TransferPrimary { shard_id, to, .. } => {
                if let Some(assignment) = self.assignment_mut(*shard_id, *to) {
                    assignment.state = ShardAssignmentState::ServingPrimary;
                }
            }
            RebalanceStep::RemoveReplica {
                shard_id,
                server_id,
            } => {
                if let Some(assignment) = self.assignment_mut(*shard_id, *server_id) {
                    assignment.state = ShardAssignmentState::Removed;
                }
            }
        }
        self.save_membership()
    }

    fn finish_drained_nodes(&mut self) -> DatabaseResult<()> {
        let mut changed = false;
        for node in &mut self.membership.nodes {
            if node.state == NodeMembershipState::Joining
                && self
                    .routing_table
                    .placements
                    .iter()
                    .any(|placement| placement.has_server(node.server_id))
            {
                node.state = NodeMembershipState::Active;
                changed = true;
            }
            if node.state == NodeMembershipState::Draining
                && !self
                    .routing_table
                    .placements
                    .iter()
                    .any(|placement| placement.has_server(node.server_id))
            {
                node.state = NodeMembershipState::Removed;
                changed = true;
            }
        }
        if changed {
            self.save_membership()?;
        }
        Ok(())
    }

    fn join_rejection_reason(&self, server_id: ServerId, shard_count: u64) -> String {
        if server_id == 0 {
            return "server id must be greater than zero".to_string();
        }
        if shard_count != self.shard_map.shard_count() {
            return format!(
                "shard count mismatch: requested {shard_count}, cluster {}",
                self.shard_map.shard_count()
            );
        }
        if self
            .membership
            .nodes
            .iter()
            .any(|node| node.server_id == server_id && node.state == NodeMembershipState::Active)
        {
            return format!("server id {server_id} is already active");
        }
        String::new()
    }

    fn ensure_joining_or_active_node(&self, server_id: ServerId) -> DatabaseResult<()> {
        match self
            .membership
            .nodes
            .iter()
            .find(|node| node.server_id == server_id)
        {
            Some(node)
                if matches!(
                    node.state,
                    NodeMembershipState::Joining | NodeMembershipState::Active
                ) =>
            {
                Ok(())
            }
            Some(node) => Err(DatabaseError::InvalidConfig(format!(
                "cluster node {server_id} is {:?}, not joining or active",
                node.state
            ))),
            None => Err(DatabaseError::InvalidConfig(format!(
                "cluster node {server_id} does not exist"
            ))),
        }
    }

    fn ensure_metadata_authority(&self) -> DatabaseResult<()> {
        if self.cluster_metadata.authority_server_id != self.config.server_id {
            return Err(DatabaseError::InvalidConfig(format!(
                "server {} is not metadata authority {}; forward cluster metadata changes to the authority",
                self.config.server_id, self.cluster_metadata.authority_server_id
            )));
        }
        Ok(())
    }

    fn ensure_write_epoch(&self) -> DatabaseResult<()> {
        if self.cluster_metadata.config_epoch != self.routing_table.version {
            return Err(DatabaseError::InvalidConfig(format!(
                "stale write epoch: metadata epoch {}, routing version {}",
                self.cluster_metadata.config_epoch, self.routing_table.version
            )));
        }
        Ok(())
    }

    fn append_metadata_operation(
        &self,
        operation: &str,
    ) -> DatabaseResult<MetadataOperationRecord> {
        self.metadata_log_store.append(
            self.cluster_metadata.term,
            self.cluster_metadata.config_epoch,
            operation,
        )
    }

    fn committed_index(&self, shard_id: ShardId) -> DatabaseResult<LogIndex> {
        self.commit_indexes
            .get(shard_id as usize)
            .copied()
            .ok_or_else(|| {
                DatabaseError::InvalidConfig(format!("routing table missing shard {shard_id}"))
            })
    }

    fn observed_match_index(&self, shard_id: ShardId, server_id: ServerId) -> LogIndex {
        self.match_indexes
            .get(shard_id as usize)
            .and_then(|matches| matches.get(&server_id))
            .copied()
            .or_else(|| {
                self.assignment(shard_id, server_id)
                    .map(|assignment| assignment.match_index)
            })
            .unwrap_or_default()
    }

    fn ensure_assignment_caught_up(
        &self,
        shard_id: ShardId,
        server_id: ServerId,
    ) -> DatabaseResult<()> {
        match self.assignment(shard_id, server_id) {
            Some(assignment) if assignment.state == ShardAssignmentState::CaughtUp => {
                self.ensure_assignment_match_index(shard_id, server_id, assignment.match_index)
            }
            Some(assignment) => Err(DatabaseError::InvalidConfig(format!(
                "shard {shard_id} server {server_id} assignment is {:?}, not caught_up",
                assignment.state
            ))),
            None => Err(DatabaseError::InvalidConfig(format!(
                "shard {shard_id} server {server_id} must be prepared and caught up before applying"
            ))),
        }
    }

    fn ensure_primary_transfer_target_ready(
        &self,
        shard_id: ShardId,
        server_id: ServerId,
    ) -> DatabaseResult<()> {
        let Some(assignment) = self.assignment(shard_id, server_id) else {
            return Ok(());
        };
        if !matches!(
            assignment.state,
            ShardAssignmentState::CaughtUp
                | ShardAssignmentState::ServingReplica
                | ShardAssignmentState::ServingPrimary
        ) {
            return Err(DatabaseError::InvalidConfig(format!(
                "shard {shard_id} server {server_id} assignment is {:?}, not transfer ready",
                assignment.state
            )));
        }
        self.ensure_assignment_match_index(shard_id, server_id, assignment.match_index)
    }

    fn ensure_assignment_match_index(
        &self,
        shard_id: ShardId,
        server_id: ServerId,
        match_index: LogIndex,
    ) -> DatabaseResult<()> {
        let committed_index = self.committed_index(shard_id)?;
        if match_index < committed_index {
            return Err(DatabaseError::InvalidConfig(format!(
                "shard {shard_id} server {server_id} match index {match_index} is behind committed index {committed_index}"
            )));
        }
        Ok(())
    }

    fn has_active_assignment(&self, shard_id: ShardId, server_id: ServerId) -> bool {
        self.assignment(shard_id, server_id)
            .map(|assignment| {
                !matches!(
                    assignment.state,
                    ShardAssignmentState::Removed | ShardAssignmentState::Removing
                )
            })
            .unwrap_or(false)
    }

    fn assignment(
        &self,
        shard_id: ShardId,
        server_id: ServerId,
    ) -> Option<&ClusterShardAssignment> {
        self.membership
            .shard_assignments
            .iter()
            .find(|assignment| assignment.shard_id == shard_id && assignment.server_id == server_id)
    }

    fn assignment_mut(
        &mut self,
        shard_id: ShardId,
        server_id: ServerId,
    ) -> Option<&mut ClusterShardAssignment> {
        self.membership
            .shard_assignments
            .iter_mut()
            .find(|assignment| assignment.shard_id == shard_id && assignment.server_id == server_id)
    }

    fn save_membership(&mut self) -> DatabaseResult<()> {
        self.membership.version = self.membership.version.saturating_add(1);
        self.membership.nodes.sort_by_key(|node| node.server_id);
        self.membership
            .shard_assignments
            .sort_by_key(|assignment| (assignment.shard_id, assignment.server_id));
        self.membership_store.save(&self.membership)?;
        Ok(())
    }

    fn add_index_definition(&mut self, index: IndexDefinition) -> DatabaseResult<()> {
        validate_index_definition(&index)?;
        if self
            .index_catalog
            .indexes
            .iter()
            .any(|existing| existing.name == index.name)
        {
            return Err(DatabaseError::InvalidConfig(format!(
                "index {:?} already exists",
                index.name
            )));
        }
        if matches!(index.kind, IndexKind::Vector { .. })
            && self.index_catalog.indexes.iter().any(|existing| {
                matches!(existing.kind, IndexKind::Vector { .. })
                    && existing.label == index.label
                    && existing.property == index.property
                    && vector_definition_parts(existing).map(|(_, metric)| metric)
                        == vector_definition_parts(&index).map(|(_, metric)| metric)
            })
        {
            return Err(DatabaseError::InvalidConfig(format!(
                "vector index already exists for label {:?} property {:?}",
                index.label, index.property
            )));
        }
        if matches!(index.kind, IndexKind::UniqueNodeProperty)
            && self.index_catalog.indexes.iter().any(|existing| {
                matches!(existing.kind, IndexKind::UniqueNodeProperty)
                    && existing.label == index.label
                    && existing.property == index.property
            })
        {
            return Err(DatabaseError::InvalidConfig(format!(
                "unique constraint already exists for label {:?} property {:?}",
                index.label, index.property
            )));
        }
        if matches!(index.kind, IndexKind::UniqueNodeProperty) {
            self.validate_existing_unique_node_property_constraint(&index)?;
        }
        if matches!(index.kind, IndexKind::Vector { .. }) {
            self.validate_existing_vector_index_values(&index)?;
            let nodes = self.store.nodes()?;
            self.vector_indexes
                .lock()
                .map_err(|_| DatabaseError::LockPoisoned)?
                .insert_definition(&index, &nodes);
        }
        self.index_catalog.indexes.push(index);
        self.index_catalog.version += 1;
        self.index_catalog_store.save(&self.index_catalog)?;
        self.save_vector_index_cache()?;
        Ok(())
    }

    fn add_index_definition_if_not_exists(&mut self, index: IndexDefinition) -> DatabaseResult<()> {
        validate_index_definition(&index)?;
        if let Some(existing) = self
            .index_catalog
            .indexes
            .iter()
            .find(|existing| existing.name == index.name)
        {
            if existing == &index {
                return Ok(());
            }
            return Err(DatabaseError::InvalidConfig(format!(
                "index {:?} already exists with a different definition",
                index.name
            )));
        }
        self.add_index_definition(index)
    }

    pub fn rebuild_vector_indexes(&mut self) -> DatabaseResult<()> {
        let indexes = self.build_vector_indexes_for_catalog(&self.index_catalog)?;
        *self
            .vector_indexes
            .lock()
            .map_err(|_| DatabaseError::LockPoisoned)? = indexes;
        self.save_vector_index_cache()?;
        Ok(())
    }

    pub fn rebuild_vector_index(&mut self, name: &str) -> DatabaseResult<()> {
        let Some(definition) = self
            .index_catalog
            .indexes
            .iter()
            .find(|index| index.name == name)
            .cloned()
        else {
            return Err(DatabaseError::InvalidConfig(format!(
                "vector index {name:?} does not exist"
            )));
        };
        if !matches!(definition.kind, IndexKind::Vector { .. }) {
            return Err(DatabaseError::InvalidConfig(format!(
                "index {name:?} is not a vector index"
            )));
        }
        self.validate_existing_vector_index_values(&definition)?;
        let nodes = self.store.nodes()?;
        let mut indexes = self
            .vector_indexes
            .lock()
            .map_err(|_| DatabaseError::LockPoisoned)?;
        indexes.remove(name);
        indexes.insert_definition(&definition, &nodes);
        drop(indexes);
        self.save_vector_index_cache()?;
        Ok(())
    }

    fn build_vector_indexes_for_catalog(
        &self,
        catalog: &IndexCatalog,
    ) -> DatabaseResult<PersistentVectorIndexes> {
        let nodes = self.store.nodes()?;
        let mut indexes = PersistentVectorIndexes::default();
        for definition in &catalog.indexes {
            indexes.insert_definition(definition, &nodes);
        }
        Ok(indexes)
    }

    fn validate_index_catalog_against_store(&self, catalog: &IndexCatalog) -> DatabaseResult<()> {
        for index in &catalog.indexes {
            match index.kind {
                IndexKind::UniqueNodeProperty => {
                    self.validate_existing_unique_node_property_constraint(index)?;
                }
                IndexKind::Vector { .. } => {
                    self.validate_existing_vector_index_values(index)?;
                }
                IndexKind::NodeProperty => {}
            }
        }
        Ok(())
    }

    fn load_or_rebuild_vector_indexes(&mut self) -> DatabaseResult<()> {
        match load_vector_index_cache(self.vector_index_cache_path(), &self.index_catalog)? {
            Some(indexes) => {
                *self
                    .vector_indexes
                    .lock()
                    .map_err(|_| DatabaseError::LockPoisoned)? = indexes;
                Ok(())
            }
            None => self.rebuild_vector_indexes(),
        }
    }

    fn save_vector_index_cache(&self) -> DatabaseResult<()> {
        let indexes = self
            .vector_indexes
            .lock()
            .map_err(|_| DatabaseError::LockPoisoned)?;
        save_vector_index_cache(
            self.vector_index_cache_path(),
            &self.index_catalog,
            &indexes,
        )
    }

    fn vector_index_cache_path(&self) -> PathBuf {
        self.config
            .data_dir
            .join("indexes")
            .join("vector-cache.bin")
    }

    fn find_merge_node(
        &self,
        labels: &[String],
        properties: &Properties,
        target_shard: Option<ShardId>,
    ) -> DatabaseResult<Option<Node>> {
        if let Some((label, property, value)) = self.merge_node_lookup_key(labels, properties) {
            for id in self
                .store
                .node_ids_by_label_property(label, property, value)?
            {
                if !matches_target_shard(target_shard, self.shard_map.owner_of_node(id)) {
                    continue;
                }
                let Some(node) = self.store.node(id)? else {
                    continue;
                };
                if node_matches_merge_pattern(&node, labels, properties) {
                    return Ok(Some(node));
                }
            }
            return Ok(None);
        }

        for node in self.store.nodes()? {
            if !matches_target_shard(target_shard, self.shard_map.owner_of_node(node.id)) {
                continue;
            }
            if node_matches_merge_pattern(&node, labels, properties) {
                return Ok(Some(node));
            }
        }
        Ok(None)
    }

    fn merge_node_lookup_key<'a>(
        &'a self,
        labels: &'a [String],
        properties: &'a Properties,
    ) -> Option<(&'a str, &'a str, &'a Value)> {
        self.merge_node_lookup_key_for_kind(labels, properties, true)
            .or_else(|| self.merge_node_lookup_key_for_kind(labels, properties, false))
    }

    fn merge_node_lookup_key_for_kind<'a>(
        &'a self,
        labels: &'a [String],
        properties: &'a Properties,
        unique: bool,
    ) -> Option<(&'a str, &'a str, &'a Value)> {
        for index in &self.index_catalog.indexes {
            let kind_matches = match index.kind {
                IndexKind::UniqueNodeProperty => unique,
                IndexKind::NodeProperty => !unique,
                IndexKind::Vector { .. } => false,
            };
            if !kind_matches || !labels.iter().any(|label| label == &index.label) {
                continue;
            }
            let Some(value) = properties.get(&index.property) else {
                continue;
            };
            if matches!(value, Value::Null) {
                continue;
            }
            return Some((&index.label, &index.property, value));
        }
        None
    }

    fn find_merge_relationship(
        &self,
        from: NodeId,
        to: NodeId,
        rel_type: &str,
        properties: &Properties,
    ) -> DatabaseResult<Option<Relationship>> {
        for relationship in self.store.outgoing_by_type(from, rel_type)? {
            if relationship.to != to {
                continue;
            }
            if properties
                .iter()
                .all(|(key, value)| relationship.properties.get(key) == Some(value))
            {
                return Ok(Some(relationship));
            }
        }
        Ok(None)
    }

    fn validate_existing_unique_node_property_constraint(
        &self,
        index: &IndexDefinition,
    ) -> DatabaseResult<()> {
        let mut seen = Vec::<(Value, NodeId)>::new();
        for node in self.store.nodes()? {
            if !node.labels.iter().any(|label| label == &index.label) {
                continue;
            }
            let Some(value) = node.properties.get(&index.property) else {
                continue;
            };
            if matches!(value, Value::Null) {
                continue;
            }
            if let Some((_, existing_id)) =
                seen.iter().find(|(seen_value, _)| *seen_value == *value)
            {
                return Err(DatabaseError::InvalidConfig(format!(
                    "unique constraint {:?} would be violated by nodes {} and {} for {}.{} = {:?}",
                    index.name, existing_id, node.id, index.label, index.property, value
                )));
            }
            seen.push((value.clone(), node.id));
        }
        Ok(())
    }

    fn validate_existing_vector_index_values(&self, index: &IndexDefinition) -> DatabaseResult<()> {
        let Some((dimensions, _)) = vector_definition_parts(index) else {
            return Ok(());
        };
        for node in self.store.nodes()? {
            self.validate_node_vector_indexed_property(&node, index, dimensions)?;
        }
        Ok(())
    }

    fn validate_unique_constraints_for_command(&self, command: &Command) -> DatabaseResult<()> {
        match command {
            Command::CreateNode {
                id,
                labels,
                properties,
            } => {
                for index in self.unique_node_property_constraints_for(labels, None) {
                    if let Some(value) = properties.get(&index.property) {
                        self.ensure_unique_node_property_value(
                            &index.label,
                            &index.property,
                            value,
                            Some(*id),
                        )?;
                    }
                }
            }
            Command::SetNodeProperty { id, key, value } => {
                let Some(node) = self.store.node(*id)? else {
                    return Ok(());
                };
                for index in self.unique_node_property_constraints_for(&node.labels, Some(key)) {
                    self.ensure_unique_node_property_value(
                        &index.label,
                        &index.property,
                        value,
                        Some(*id),
                    )?;
                }
            }
            Command::AddNodeLabel { id, label } => {
                let Some(mut node) = self.store.node(*id)? else {
                    return Ok(());
                };
                if node.labels.iter().any(|existing| existing == label) {
                    return Ok(());
                }
                node.labels.push(label.clone());
                for index in self.unique_node_property_constraints_for(&node.labels, None) {
                    if let Some(value) = node.properties.get(&index.property) {
                        self.ensure_unique_node_property_value(
                            &index.label,
                            &index.property,
                            value,
                            Some(*id),
                        )?;
                    }
                }
            }
            Command::CreateRelationship { .. }
            | Command::SetRelationshipProperty { .. }
            | Command::RemoveNodeProperty { .. }
            | Command::RemoveNodeLabel { .. }
            | Command::RemoveRelationshipProperty { .. }
            | Command::DeleteNode { .. }
            | Command::DeleteRelationship { .. }
            | Command::UpsertBoundaryNode { .. } => {}
        }
        Ok(())
    }

    fn validate_vector_indexes_for_command(&self, command: &Command) -> DatabaseResult<()> {
        match command {
            Command::CreateNode {
                labels, properties, ..
            } => self.validate_vector_indexed_properties(labels, properties),
            Command::SetNodeProperty { id, key, value } => {
                let Some(mut node) = self.store.node(*id)? else {
                    return Ok(());
                };
                node.properties.insert(key.clone(), value.clone());
                self.validate_vector_indexed_properties(&node.labels, &node.properties)
            }
            Command::AddNodeLabel { id, label } => {
                let Some(mut node) = self.store.node(*id)? else {
                    return Ok(());
                };
                if !node.labels.iter().any(|existing| existing == label) {
                    node.labels.push(label.clone());
                }
                self.validate_vector_indexed_properties(&node.labels, &node.properties)
            }
            Command::CreateRelationship { .. }
            | Command::SetRelationshipProperty { .. }
            | Command::RemoveNodeProperty { .. }
            | Command::RemoveNodeLabel { .. }
            | Command::RemoveRelationshipProperty { .. }
            | Command::DeleteNode { .. }
            | Command::DeleteRelationship { .. }
            | Command::UpsertBoundaryNode { .. } => Ok(()),
        }
    }

    fn validate_storable_properties_for_command(command: &Command) -> DatabaseResult<()> {
        match command {
            Command::CreateNode { properties, .. }
            | Command::CreateRelationship { properties, .. }
            | Command::UpsertBoundaryNode { properties, .. } => {
                validate_storable_properties(properties)
            }
            Command::SetNodeProperty { value, .. }
            | Command::SetRelationshipProperty { value, .. } => {
                validate_storable_property_value(value)
            }
            Command::RemoveNodeProperty { .. }
            | Command::RemoveNodeLabel { .. }
            | Command::RemoveRelationshipProperty { .. }
            | Command::AddNodeLabel { .. }
            | Command::DeleteNode { .. }
            | Command::DeleteRelationship { .. } => Ok(()),
        }
    }

    fn validate_replicated_vector_indexes_for_command(
        &self,
        command: &Command,
        node_overlay: &mut HashMap<NodeId, Option<Node>>,
    ) -> DatabaseResult<()> {
        match command {
            Command::CreateNode {
                id,
                labels,
                properties,
            } => {
                self.validate_vector_indexed_properties(labels, properties)?;
                node_overlay.insert(
                    *id,
                    Some(Node::new(*id, labels.clone(), properties.clone())),
                );
                Ok(())
            }
            Command::SetNodeProperty { id, key, value } => {
                let Some(mut node) = self.overlay_node(node_overlay, *id)? else {
                    return Ok(());
                };
                node.properties.insert(key.clone(), value.clone());
                self.validate_vector_indexed_properties(&node.labels, &node.properties)?;
                node_overlay.insert(*id, Some(node));
                Ok(())
            }
            Command::RemoveNodeProperty { id, key } => {
                if let Some(mut node) = self.overlay_node(node_overlay, *id)? {
                    node.properties.remove(key);
                    node_overlay.insert(*id, Some(node));
                }
                Ok(())
            }
            Command::AddNodeLabel { id, label } => {
                let Some(mut node) = self.overlay_node(node_overlay, *id)? else {
                    return Ok(());
                };
                if !node.labels.iter().any(|existing| existing == label) {
                    node.labels.push(label.clone());
                }
                self.validate_vector_indexed_properties(&node.labels, &node.properties)?;
                node_overlay.insert(*id, Some(node));
                Ok(())
            }
            Command::RemoveNodeLabel { id, label } => {
                if let Some(mut node) = self.overlay_node(node_overlay, *id)? {
                    node.labels.retain(|existing| existing != label);
                    node_overlay.insert(*id, Some(node));
                }
                Ok(())
            }
            Command::DeleteNode { id } => {
                node_overlay.insert(*id, None);
                Ok(())
            }
            Command::CreateRelationship { .. }
            | Command::SetRelationshipProperty { .. }
            | Command::RemoveRelationshipProperty { .. }
            | Command::DeleteRelationship { .. }
            | Command::UpsertBoundaryNode { .. } => Ok(()),
        }
    }

    fn overlay_node(
        &self,
        node_overlay: &HashMap<NodeId, Option<Node>>,
        id: NodeId,
    ) -> DatabaseResult<Option<Node>> {
        if let Some(node) = node_overlay.get(&id) {
            return Ok(node.clone());
        }
        self.store.node(id).map_err(DatabaseError::from)
    }

    fn validate_vector_indexed_properties(
        &self,
        labels: &[String],
        properties: &Properties,
    ) -> DatabaseResult<()> {
        for index in &self.index_catalog.indexes {
            let Some((dimensions, _)) = vector_definition_parts(index) else {
                continue;
            };
            if !labels.iter().any(|label| label == &index.label) {
                continue;
            }
            self.validate_vector_indexed_property_value(index, properties, dimensions)?;
        }
        Ok(())
    }

    fn validate_node_vector_indexed_property(
        &self,
        node: &Node,
        index: &IndexDefinition,
        dimensions: usize,
    ) -> DatabaseResult<()> {
        if !node.labels.iter().any(|label| label == &index.label) {
            return Ok(());
        }
        self.validate_vector_indexed_property_value(index, &node.properties, dimensions)
    }

    fn validate_vector_indexed_property_value(
        &self,
        index: &IndexDefinition,
        properties: &Properties,
        dimensions: usize,
    ) -> DatabaseResult<()> {
        let Some(value) = properties.get(&index.property) else {
            return Ok(());
        };
        let Value::Vector(vector) = value else {
            return Err(DatabaseError::InvalidConfig(format!(
                "vector indexed property {}.{} must be a vector",
                index.label, index.property
            )));
        };
        if vector.len() != dimensions {
            return Err(DatabaseError::InvalidConfig(format!(
                "vector indexed property {}.{} expected {} dimensions, got {}",
                index.label,
                index.property,
                dimensions,
                vector.len()
            )));
        }
        Ok(())
    }

    fn unique_node_property_constraints_for(
        &self,
        labels: &[String],
        property: Option<&str>,
    ) -> Vec<IndexDefinition> {
        self.index_catalog
            .indexes
            .iter()
            .filter(|index| matches!(index.kind, IndexKind::UniqueNodeProperty))
            .filter(|index| labels.iter().any(|label| label == &index.label))
            .filter(|index| property.is_none_or(|property| property == index.property))
            .cloned()
            .collect()
    }

    fn ensure_unique_node_property_value(
        &self,
        label: &str,
        property: &str,
        value: &Value,
        except_node_id: Option<NodeId>,
    ) -> DatabaseResult<()> {
        if matches!(value, Value::Null) {
            return Ok(());
        }
        for existing_id in self
            .store
            .node_ids_by_label_property(label, property, value)?
        {
            if Some(existing_id) != except_node_id {
                return Err(DatabaseError::InvalidConfig(format!(
                    "unique constraint violation for {label}.{property} = {value:?}; existing node {existing_id}"
                )));
            }
        }
        Ok(())
    }

    fn snapshot_store(&self) -> DatabaseResult<PartitionedGraphStore<RocksKvSnapshot>> {
        Ok(self.store.snapshot()?)
    }

    fn read_snapshot(&self) -> DatabaseResult<Neo4rReadSnapshot> {
        Ok(Neo4rReadSnapshot {
            store: self.snapshot_store()?,
            shard_map: self.shard_map,
            timestamp: self.clock.now(),
            applied_indexes: self.applied_indexes(),
            committed_indexes: self.committed_indexes(),
            query_engine: CypherEngine::with_vector_indexes(Arc::new(
                SharedVectorIndexProvider::new(self.vector_indexes.clone()),
            )),
        })
    }

    fn write_command(&mut self, shard_id: ShardId, command: Command) -> DatabaseResult<()> {
        let entry = self.append_local_command(shard_id, command, true)?;
        let outcome = self.replicator.publish(&entry)?;
        self.observe_replication_outcome(&entry, &outcome)?;
        self.commit_entry(&entry)?;
        self.apply_entry(&entry)
    }

    fn prepare_local_write(&mut self, operation: WriteOperation) -> DatabaseResult<PreparedWrite> {
        match operation {
            WriteOperation::CreateNode { labels, properties } => {
                let id = self.allocate_node_id();
                let command = Command::CreateNode {
                    id,
                    labels,
                    properties,
                };
                let shard_id = self.shard_map.owner_of_node(id);
                let entry = self.append_local_command(shard_id, command, false)?;
                Ok(PreparedWrite {
                    entry,
                    response: WriteResponse::NodeId(id),
                })
            }
            WriteOperation::CreateNodeOnShard {
                shard_id,
                labels,
                properties,
            } => {
                self.validate_shard_id(shard_id)?;
                let id = self.allocate_node_id_for_shard(shard_id);
                let command = Command::CreateNode {
                    id,
                    labels,
                    properties,
                };
                let entry = self.append_local_command(shard_id, command, false)?;
                Ok(PreparedWrite {
                    entry,
                    response: WriteResponse::NodeId(id),
                })
            }
            WriteOperation::CreateRelationship {
                from,
                to,
                rel_type,
                properties,
            } => {
                self.ensure_local_node_exists(from)?;
                self.ensure_node_or_boundary_exists(to)?;
                let id = self.allocate_relationship_id();
                let shard_id = self.shard_map.owner_of_relationship(from, to, &rel_type);
                let command = Command::CreateRelationship {
                    id,
                    from,
                    to,
                    rel_type,
                    properties,
                };
                let entry = self.append_local_command(shard_id, command, false)?;
                Ok(PreparedWrite {
                    entry,
                    response: WriteResponse::RelationshipId(id),
                })
            }
            WriteOperation::SetNodeProperty { id, key, value } => {
                self.ensure_local_node_exists(id)?;
                let shard_id = self.shard_map.owner_of_node(id);
                let entry = self.append_local_command(
                    shard_id,
                    Command::SetNodeProperty { id, key, value },
                    false,
                )?;
                Ok(PreparedWrite {
                    entry,
                    response: WriteResponse::Unit,
                })
            }
            WriteOperation::RemoveNodeProperty { id, key } => {
                self.ensure_local_node_exists(id)?;
                let shard_id = self.shard_map.owner_of_node(id);
                let entry = self.append_local_command(
                    shard_id,
                    Command::RemoveNodeProperty { id, key },
                    false,
                )?;
                Ok(PreparedWrite {
                    entry,
                    response: WriteResponse::Unit,
                })
            }
            WriteOperation::AddNodeLabel { id, label } => {
                self.ensure_local_node_exists(id)?;
                let shard_id = self.shard_map.owner_of_node(id);
                let entry = self.append_local_command(
                    shard_id,
                    Command::AddNodeLabel { id, label },
                    false,
                )?;
                Ok(PreparedWrite {
                    entry,
                    response: WriteResponse::Unit,
                })
            }
            WriteOperation::RemoveNodeLabel { id, label } => {
                self.ensure_local_node_exists(id)?;
                let shard_id = self.shard_map.owner_of_node(id);
                let entry = self.append_local_command(
                    shard_id,
                    Command::RemoveNodeLabel { id, label },
                    false,
                )?;
                Ok(PreparedWrite {
                    entry,
                    response: WriteResponse::Unit,
                })
            }
            WriteOperation::SetRelationshipProperty { id, key, value } => {
                let shard_id = self.relationship_owner_shard(id)?;
                let entry = self.append_local_command(
                    shard_id,
                    Command::SetRelationshipProperty { id, key, value },
                    false,
                )?;
                Ok(PreparedWrite {
                    entry,
                    response: WriteResponse::Unit,
                })
            }
            WriteOperation::RemoveRelationshipProperty { id, key } => {
                let shard_id = self.relationship_owner_shard(id)?;
                let entry = self.append_local_command(
                    shard_id,
                    Command::RemoveRelationshipProperty { id, key },
                    false,
                )?;
                Ok(PreparedWrite {
                    entry,
                    response: WriteResponse::Unit,
                })
            }
            WriteOperation::DeleteRelationship { id } => {
                let shard_id = self.relationship_owner_shard(id)?;
                let entry =
                    self.append_local_command(shard_id, Command::DeleteRelationship { id }, false)?;
                Ok(PreparedWrite {
                    entry,
                    response: WriteResponse::Unit,
                })
            }
            WriteOperation::DeleteNode { id } => {
                self.ensure_local_node_exists(id)?;
                let shard_id = self.shard_map.owner_of_node(id);
                let entry =
                    self.append_local_command(shard_id, Command::DeleteNode { id }, false)?;
                Ok(PreparedWrite {
                    entry,
                    response: WriteResponse::Unit,
                })
            }
            WriteOperation::ApplyReplicatedEntry(_) | WriteOperation::ApplyReplicatedEntries(_) => {
                Err(DatabaseError::UnexpectedWriteResponse(
                    "replicated entry cannot be prepared as a local write".to_string(),
                ))
            }
        }
    }

    fn append_local_command(
        &mut self,
        shard_id: ShardId,
        command: Command,
        sync: bool,
    ) -> DatabaseResult<LogEntry> {
        self.ensure_write_epoch()?;
        self.ensure_local_primary(shard_id)?;
        Self::validate_storable_properties_for_command(&command)?;
        self.validate_vector_indexes_for_command(&command)?;
        self.validate_unique_constraints_for_command(&command)?;
        let log_index = self.next_log_index(shard_id)?;
        let timestamp = self.clock.tick();
        let entry = LogEntry::new_with_metadata(
            shard_id,
            0,
            log_index,
            self.config.server_id,
            self.routing_table.version,
            timestamp,
            command,
        );
        self.log(shard_id)?
            .append_with_sync(&entry, sync && self.should_sync_wal(entry.index))?;
        self.observe_log_position(&entry);
        Ok(entry)
    }

    fn flush_group_commit(&mut self, entries: &[LogEntry]) -> DatabaseResult<()> {
        let outcomes = self.replicator.publish_batch(entries)?;
        for (entry, outcome) in entries.iter().zip(outcomes.iter()) {
            self.observe_replication_outcome(entry, outcome)?;
        }
        self.flush_entries(entries)
    }

    fn flush_entries(&mut self, entries: &[LogEntry]) -> DatabaseResult<()> {
        let mut segments = BTreeMap::new();
        for entry in entries {
            let segment_start = self
                .log(entry.shard_id)?
                .segment_start_for_index(entry.index);
            segments.insert((entry.shard_id, segment_start), entry.index);
        }
        for ((shard_id, _), index) in segments {
            self.log(shard_id)?.sync_segment_for_index(index)?;
        }
        self.commit_entries(entries)?;
        for entry in entries {
            self.apply_entry(entry)?;
        }
        Ok(())
    }

    pub fn apply_replicated_entry(&mut self, entry: LogEntry) -> DatabaseResult<()> {
        self.apply_replicated_entries(vec![entry])
    }

    pub fn apply_replicated_entries(&mut self, entries: Vec<LogEntry>) -> DatabaseResult<()> {
        if entries.is_empty() {
            return Ok(());
        }
        let mut accepted = Vec::new();
        let mut node_overlay = HashMap::<NodeId, Option<Node>>::new();
        let mut next_indexes = HashMap::<ShardId, LogIndex>::new();
        for entry in entries {
            self.validate_replicated_entry_metadata(&entry)?;
            self.ensure_local_copy(entry.shard_id)?;
            let expected = match next_indexes.get(&entry.shard_id) {
                Some(expected) => *expected,
                None => self.next_log_index(entry.shard_id)?,
            };
            if entry.index < expected {
                self.ensure_duplicate_entry_matches(&entry)?;
                continue;
            }
            if entry.index != expected {
                return Err(DatabaseError::UnexpectedLogIndex {
                    shard_id: entry.shard_id,
                    expected,
                    actual: entry.index,
                });
            }
            Self::validate_storable_properties_for_command(&entry.command)?;
            self.validate_replicated_vector_indexes_for_command(&entry.command, &mut node_overlay)?;
            next_indexes.insert(entry.shard_id, expected + 1);
            accepted.push(entry);
        }
        for entry in &accepted {
            self.log(entry.shard_id)?.append_with_sync(entry, false)?;
            self.observe_log_position(entry);
        }
        self.flush_entries(&accepted)
    }

    fn validate_replicated_entry_metadata(&self, entry: &LogEntry) -> DatabaseResult<()> {
        if entry.config_version != 0 && entry.config_version != self.routing_table.version {
            return Err(DatabaseError::LogConflict {
                shard_id: entry.shard_id,
                index: entry.index,
                message: format!(
                    "config version mismatch: entry {}, local {}",
                    entry.config_version, self.routing_table.version
                ),
            });
        }
        Ok(())
    }

    fn ensure_duplicate_entry_matches(&self, entry: &LogEntry) -> DatabaseResult<()> {
        let Some(existing) = self.log(entry.shard_id)?.entry(entry.index)? else {
            return Err(DatabaseError::LogConflict {
                shard_id: entry.shard_id,
                index: entry.index,
                message: "duplicate entry is below next index but missing locally".to_string(),
            });
        };
        if existing == *entry {
            Ok(())
        } else {
            Err(DatabaseError::LogConflict {
                shard_id: entry.shard_id,
                index: entry.index,
                message: "duplicate entry payload differs from local log".to_string(),
            })
        }
    }

    fn apply_entry(&mut self, entry: &LogEntry) -> DatabaseResult<()> {
        self.store.apply(entry.shard_id, &entry.command)?;
        self.invalidate_read_cache();
        self.update_vector_indexes_for_command(&entry.command)?;
        self.observe_entry(entry);
        self.refresh_statistics_catalog()?;
        if self.should_checkpoint(entry.index) {
            self.checkpoint(entry.shard_id)?.save_with_timestamp(
                entry.term,
                entry.index,
                entry.timestamp,
            )?;
        }
        Ok(())
    }

    fn observe_read_cache_hit(&self) -> DatabaseResult<()> {
        let mut stats = self
            .read_cache_stats
            .lock()
            .map_err(|_| DatabaseError::LockPoisoned)?;
        stats.hits = stats.hits.saturating_add(1);
        Ok(())
    }

    fn observe_read_cache_miss(&self) -> DatabaseResult<()> {
        let mut stats = self
            .read_cache_stats
            .lock()
            .map_err(|_| DatabaseError::LockPoisoned)?;
        stats.misses = stats.misses.saturating_add(1);
        Ok(())
    }

    fn read_cache_stats(&self) -> DatabaseResult<ReadCacheStats> {
        self.read_cache_stats
            .lock()
            .map_err(|_| DatabaseError::LockPoisoned)
            .map(|stats| *stats)
    }

    fn invalidate_read_cache(&self) {
        if let Ok(mut cache) = self.read_cache.lock() {
            cache.nodes.clear();
            cache.relationships.clear();
            cache.index_lookups.clear();
        }
    }

    fn update_vector_indexes_for_command(&mut self, command: &Command) -> DatabaseResult<()> {
        match command {
            Command::CreateNode { id, .. }
            | Command::SetNodeProperty { id, .. }
            | Command::RemoveNodeProperty { id, .. }
            | Command::AddNodeLabel { id, .. }
            | Command::RemoveNodeLabel { id, .. } => {
                if let Some(node) = self.store.node(*id)? {
                    let mut indexes = self
                        .vector_indexes
                        .lock()
                        .map_err(|_| DatabaseError::LockPoisoned)?;
                    indexes.update_node(&node);
                    let should_save = !indexes.is_empty();
                    drop(indexes);
                    if should_save {
                        self.save_vector_index_cache()?;
                    }
                }
            }
            Command::DeleteNode { id } => {
                let mut indexes = self
                    .vector_indexes
                    .lock()
                    .map_err(|_| DatabaseError::LockPoisoned)?;
                indexes.delete_node(*id);
                let should_save = !indexes.is_empty();
                drop(indexes);
                if should_save {
                    self.save_vector_index_cache()?;
                }
            }
            Command::CreateRelationship { .. }
            | Command::SetRelationshipProperty { .. }
            | Command::RemoveRelationshipProperty { .. }
            | Command::DeleteRelationship { .. }
            | Command::UpsertBoundaryNode { .. } => {}
        }
        Ok(())
    }

    fn ensure_local_primary(&self, shard_id: ShardId) -> DatabaseResult<()> {
        let primary_server_id = self.routing_table.primary_server_id(shard_id);
        if primary_server_id == Some(self.config.server_id) {
            Ok(())
        } else {
            Err(DatabaseError::ShardNotPrimary {
                shard_id,
                server_id: self.config.server_id,
                primary_server_id,
            })
        }
    }

    fn ensure_local_copy(&self, shard_id: ShardId) -> DatabaseResult<()> {
        if self
            .routing_table
            .has_local_copy(shard_id, self.config.server_id)
        {
            Ok(())
        } else {
            Err(DatabaseError::ShardNotLocal {
                shard_id,
                server_id: self.config.server_id,
            })
        }
    }

    fn ensure_local_node_exists(&self, id: NodeId) -> DatabaseResult<()> {
        if self.store.node(id)?.is_some() {
            Ok(())
        } else {
            Err(GraphError::NodeNotFound(id).into())
        }
    }

    fn ensure_node_or_boundary_exists(&self, id: NodeId) -> DatabaseResult<()> {
        if self.store.node(id)?.is_some() || self.store.boundary_node(id)?.is_some() {
            Ok(())
        } else {
            Err(GraphError::NodeNotFound(id).into())
        }
    }

    fn relationship_owner_shard(&self, id: RelationshipId) -> DatabaseResult<ShardId> {
        let relationship = self
            .store
            .relationship(id)?
            .ok_or(GraphError::RelationshipNotFound(id))?;
        Ok(self.shard_map.owner_of_relationship(
            relationship.from,
            relationship.to,
            &relationship.rel_type,
        ))
    }

    fn replay_logs(&mut self) -> DatabaseResult<()> {
        for shard_id in 0..self.shard_map.shard_count() {
            let start_index = self
                .checkpoint(shard_id)?
                .load()?
                .map(|checkpoint| checkpoint.last_applied_index.saturating_add(1))
                .unwrap_or(0);
            let entries = self.log(shard_id)?.replay_from(start_index)?;
            for entry in entries {
                self.observe_log_position(&entry);
                if entry.index <= self.commit_index(entry.shard_id)? {
                    self.apply_entry(&entry)?;
                }
            }
        }
        Ok(())
    }

    fn recover_allocators_from_store(&mut self) -> DatabaseResult<()> {
        self.next_node_id = self
            .store
            .nodes()?
            .into_iter()
            .map(|node| node.id.saturating_add(1))
            .max()
            .unwrap_or(self.next_node_id);
        self.next_relationship_id = self
            .store
            .relationships()?
            .into_iter()
            .map(|relationship| relationship.id.saturating_add(1))
            .max()
            .unwrap_or(self.next_relationship_id);
        for shard_id in 0..self.shard_map.shard_count() {
            if let Some(checkpoint) = self.checkpoint(shard_id)?.load()? {
                self.next_log_indexes[shard_id as usize] = self.next_log_indexes[shard_id as usize]
                    .max(checkpoint.last_applied_index.saturating_add(1));
                self.clock.observe(checkpoint.timestamp);
            }
        }
        Ok(())
    }

    fn observe_entry(&mut self, entry: &LogEntry) {
        self.observe_log_position(entry);
        self.clock.observe(entry.timestamp);
        match &entry.command {
            Command::CreateNode { id, .. } | Command::UpsertBoundaryNode { id, .. } => {
                self.next_node_id = self.next_node_id.max(id.saturating_add(1));
            }
            Command::CreateRelationship { id, .. } => {
                self.next_relationship_id = self.next_relationship_id.max(id.saturating_add(1));
            }
            Command::SetNodeProperty { .. }
            | Command::RemoveNodeProperty { .. }
            | Command::AddNodeLabel { .. }
            | Command::RemoveNodeLabel { .. }
            | Command::SetRelationshipProperty { .. }
            | Command::RemoveRelationshipProperty { .. }
            | Command::DeleteRelationship { .. }
            | Command::DeleteNode { .. } => {}
        }
    }

    fn observe_log_position(&mut self, entry: &LogEntry) {
        let index_slot = &mut self.next_log_indexes[entry.shard_id as usize];
        *index_slot = (*index_slot).max(entry.index.saturating_add(1));
    }

    fn observe_replication_outcome(
        &mut self,
        entry: &LogEntry,
        outcome: &ReplicationOutcome,
    ) -> DatabaseResult<()> {
        let match_indexes = self
            .match_indexes
            .get_mut(entry.shard_id as usize)
            .ok_or(DatabaseError::MissingShardLog(entry.shard_id))?;
        for (server_id, shard_id, index) in &outcome.acked_match_indexes {
            if *shard_id != entry.shard_id {
                continue;
            }
            let slot = match_indexes.entry(*server_id).or_default();
            *slot = (*slot).max(*index);
        }
        for server_id in &outcome.acked_server_ids {
            let slot = match_indexes.entry(*server_id).or_default();
            *slot = (*slot).max(entry.index);
        }
        Ok(())
    }

    fn commit_entry(&mut self, entry: &LogEntry) -> DatabaseResult<()> {
        let expected = self.commit_index(entry.shard_id)?.saturating_add(1);
        if entry.index != expected {
            return Err(DatabaseError::Replication(format!(
                "cannot commit non-contiguous shard {} entry {} while commit index is {}",
                entry.shard_id,
                entry.index,
                expected.saturating_sub(1)
            )));
        }
        self.commits
            .get(entry.shard_id as usize)
            .ok_or(DatabaseError::MissingShardLog(entry.shard_id))?
            .save(entry.term, entry.index)?;
        self.commit_indexes[entry.shard_id as usize] = entry.index;
        Ok(())
    }

    fn commit_entries(&mut self, entries: &[LogEntry]) -> DatabaseResult<()> {
        let mut last_by_shard = BTreeMap::<ShardId, (u64, LogIndex)>::new();
        for entry in entries {
            let expected = self.commit_index(entry.shard_id)?.saturating_add(1);
            if entry.index != expected {
                return Err(DatabaseError::Replication(format!(
                    "cannot commit non-contiguous shard {} entry {} while commit index is {}",
                    entry.shard_id,
                    entry.index,
                    expected.saturating_sub(1)
                )));
            }
            self.commit_indexes[entry.shard_id as usize] = entry.index;
            last_by_shard.insert(entry.shard_id, (entry.term, entry.index));
        }
        for (shard_id, (term, index)) in last_by_shard {
            self.commits
                .get(shard_id as usize)
                .ok_or(DatabaseError::MissingShardLog(shard_id))?
                .save(term, index)?;
        }
        Ok(())
    }

    fn allocate_node_id(&mut self) -> NodeId {
        let id = self.next_node_id;
        self.next_node_id += 1;
        id
    }

    fn allocate_node_id_for_shard(&mut self, shard_id: ShardId) -> NodeId {
        while self.shard_map.owner_of_node(self.next_node_id) != shard_id {
            self.next_node_id += 1;
        }
        self.allocate_node_id()
    }

    fn validate_shard_id(&self, shard_id: ShardId) -> DatabaseResult<()> {
        if shard_id >= self.shard_map.shard_count() {
            return Err(DatabaseError::MissingShardLog(shard_id));
        }
        Ok(())
    }

    fn allocate_relationship_id(&mut self) -> RelationshipId {
        let id = self.next_relationship_id;
        self.next_relationship_id += 1;
        id
    }

    fn next_log_index(&self, shard_id: ShardId) -> DatabaseResult<LogIndex> {
        self.next_log_indexes
            .get(shard_id as usize)
            .copied()
            .ok_or(DatabaseError::MissingShardLog(shard_id))
    }

    fn log(&self, shard_id: ShardId) -> DatabaseResult<&SegmentedShardLog> {
        self.logs
            .get(shard_id as usize)
            .ok_or(DatabaseError::MissingShardLog(shard_id))
    }

    fn checkpoint(&self, shard_id: ShardId) -> DatabaseResult<&CheckpointStore> {
        self.checkpoints
            .get(shard_id as usize)
            .ok_or(DatabaseError::MissingShardLog(shard_id))
    }

    fn should_checkpoint(&self, index: LogIndex) -> bool {
        index != 0 && index % self.config.checkpoint_interval == 0
    }

    fn should_sync_wal(&self, index: LogIndex) -> bool {
        index != 0 && index % self.config.wal_sync_interval == 0
    }

    fn applied_indexes(&self) -> Vec<LogIndex> {
        self.next_log_indexes
            .iter()
            .map(|next| next.saturating_sub(1))
            .collect()
    }

    fn committed_indexes(&self) -> Vec<LogIndex> {
        self.commit_indexes.clone()
    }

    fn commit_index(&self, shard_id: ShardId) -> DatabaseResult<LogIndex> {
        self.commit_indexes
            .get(shard_id as usize)
            .copied()
            .ok_or(DatabaseError::MissingShardLog(shard_id))
    }

    fn query_route(&self) -> QueryRoute {
        let remote_shards = (0..self.shard_map.shard_count())
            .filter(|shard_id| {
                !self
                    .routing_table
                    .has_local_copy(*shard_id, self.config.server_id)
            })
            .collect::<Vec<_>>();
        if remote_shards.is_empty() {
            QueryRoute::LocalOnly
        } else {
            QueryRoute::RequiresRemoteShards(remote_shards)
        }
    }

    fn query_plan(&self, query: &str, params: &QueryParams) -> DistributedQueryPlan {
        self.observe_index_cache_probe(query);
        let route = self.query_route();
        let access_plan = self.query_access_plan(query, params);
        let statistics = self
            .statistics_catalog()
            .unwrap_or_else(|_| StatisticsCatalog {
                node_count: 0,
                relationship_count: 0,
                label_counts: Vec::new(),
                relationship_type_counts: Vec::new(),
                index_count: self.index_catalog.indexes.len(),
                vector_index_count: 0,
            });
        let traversal_policy = match &route {
            QueryRoute::LocalOnly => RemoteTraversalPolicy::BoundaryCacheOnly,
            QueryRoute::RequiresRemoteShards(shards) => {
                RemoteTraversalPolicy::RemoteShardHop(shards.clone())
            }
        };
        let remote_shard_count = match &route {
            QueryRoute::LocalOnly => 0,
            QueryRoute::RequiresRemoteShards(shards) => shards.len(),
        };
        let estimated_rows = estimate_rows(&statistics, &access_plan);
        let estimated_cost = estimate_query_cost(&statistics, &access_plan, remote_shard_count);
        DistributedQueryPlan {
            route,
            traversal_policy,
            uses_boundary_cache: true,
            access_plan,
            estimated_cost,
            estimated_rows,
            remote_shard_count,
        }
    }

    fn observe_index_cache_probe(&self, query: &str) {
        let key = format!("v{}:{query}", self.routing_table.version);
        if let (Ok(mut cache), Ok(mut stats)) =
            (self.read_cache.lock(), self.read_cache_stats.lock())
        {
            if cache.index_lookups.contains_key(&key) {
                stats.index_hits = stats.index_hits.saturating_add(1);
            } else {
                stats.index_misses = stats.index_misses.saturating_add(1);
                cache.index_lookups.insert(key, Vec::new());
            }
        }
    }

    fn statistics_catalog(&self) -> DatabaseResult<StatisticsCatalog> {
        if self.statistics.node_count != 0
            || self.statistics.relationship_count != 0
            || self.statistics.index_count != 0
            || self.statistics.vector_index_count != 0
            || !self.statistics.label_counts.is_empty()
            || !self.statistics.relationship_type_counts.is_empty()
        {
            return Ok(self.statistics.clone());
        }
        self.compute_statistics_catalog()
    }

    fn compute_statistics_catalog(&self) -> DatabaseResult<StatisticsCatalog> {
        let nodes = self.store.nodes().map_err(DatabaseError::GraphRead)?;
        let relationships = self.store.relationships()?;
        let mut label_counts = BTreeMap::new();
        for node in &nodes {
            for label in &node.labels {
                *label_counts.entry(label.clone()).or_insert(0) += 1;
            }
        }
        let mut relationship_type_counts = BTreeMap::new();
        for relationship in &relationships {
            *relationship_type_counts
                .entry(relationship.rel_type.clone())
                .or_insert(0) += 1;
        }
        Ok(StatisticsCatalog {
            node_count: nodes.len(),
            relationship_count: relationships.len(),
            label_counts: label_counts.into_iter().collect(),
            relationship_type_counts: relationship_type_counts.into_iter().collect(),
            index_count: self.index_catalog.indexes.len(),
            vector_index_count: self
                .index_catalog
                .indexes
                .iter()
                .filter(|index| matches!(index.kind, IndexKind::Vector { .. }))
                .count(),
        })
    }

    fn refresh_statistics_catalog(&mut self) -> DatabaseResult<()> {
        self.statistics = self.compute_statistics_catalog()?;
        self.statistics_store.save(&self.statistics)
    }

    fn storage_status(&self) -> DatabaseResult<StorageStatus> {
        let stats = collect_storage_files(&self.config.data_dir)?;
        let cache_stats = self.read_cache_stats()?;
        Ok(StorageStatus {
            data_dir: self.config.data_dir.clone(),
            total_bytes: stats.total_bytes,
            file_count: stats.file_count,
            wal_segment_count: stats.wal_segment_count,
            checkpoint_file_count: stats.checkpoint_file_count,
            metadata_file_count: stats.metadata_file_count,
            committed_indexes: self.committed_indexes(),
            read_cache_hits: cache_stats.hits,
            read_cache_misses: cache_stats.misses,
            index_cache_hits: cache_stats.index_hits,
            index_cache_misses: cache_stats.index_misses,
            wal_pruned_until: self.committed_indexes(),
        })
    }

    fn checkpoint_now(&mut self) -> DatabaseResult<StorageMaintenanceResult> {
        let timestamp = self.clock.tick();
        let committed_indexes = self.committed_indexes();
        let mut files_touched = 0;
        for (shard_index, index) in committed_indexes.iter().enumerate() {
            self.checkpoint(shard_index as ShardId)?
                .save_with_timestamp(0, *index, timestamp)?;
            files_touched += 1;
        }
        let stats = collect_storage_files(&self.config.data_dir)?;
        Ok(StorageMaintenanceResult {
            action: "checkpoint".to_string(),
            files_touched,
            bytes_observed: stats.total_bytes,
            pruned_until: committed_indexes,
        })
    }

    fn compact_storage(&mut self) -> DatabaseResult<StorageMaintenanceResult> {
        let before = collect_storage_files(&self.config.data_dir)?;
        self.invalidate_read_cache();
        Ok(StorageMaintenanceResult {
            action: "compact_observe".to_string(),
            files_touched: before.file_count,
            bytes_observed: before.total_bytes,
            pruned_until: self.committed_indexes(),
        })
    }

    fn metadata_operations(&self) -> DatabaseResult<Vec<MetadataOperationRecord>> {
        self.metadata_log_store.load()
    }

    fn query_access_plan(&self, query: &str, params: &QueryParams) -> QueryAccessPlan {
        match self.try_query_access_plan(query, params) {
            Ok(plan) => plan,
            Err(err) => QueryAccessPlan::Unsupported {
                reason: err.to_string(),
            },
        }
    }

    fn try_query_access_plan(
        &self,
        query: &str,
        params: &QueryParams,
    ) -> DatabaseResult<QueryAccessPlan> {
        let input = strip_keyword(query.trim(), "MATCH")?;
        let (before_return, _) = split_keyword(input, "RETURN")
            .ok_or_else(|| write_parse_error("query requires RETURN"))?;
        let (pattern_part, predicate_part) = match split_keyword(before_return, "WHERE") {
            Some((pattern, predicate)) => (pattern.trim(), Some(predicate.trim())),
            None => (before_return.trim(), None),
        };
        if pattern_part.contains("->") {
            return Ok(self.relationship_access_plan(pattern_part));
        }
        let node = parse_node_pattern_write(pattern_part, params)?;
        if let Some(label) = node.labels.first() {
            if let Some(property) = self.best_indexed_node_property(label, node.properties.keys()) {
                return Ok(property);
            }
        }
        if let Some(predicate) = predicate_part {
            if let Some((property, metric)) =
                vector_predicate_for_variable(predicate, &node.variable)
            {
                if let Some(label) = node.labels.first() {
                    if self.has_vector_index(label, &property, &metric) {
                        return Ok(QueryAccessPlan::VectorIndexSeek {
                            label: Some(label.clone()),
                            property,
                            metric,
                        });
                    }
                }
            }
            if let Some(property) = property_predicate_for_variable(predicate, &node.variable) {
                if let Some(label) = node.labels.first() {
                    if self.has_unique_node_property_constraint(label, &property) {
                        return Ok(QueryAccessPlan::NodeUniqueIndexSeek {
                            label: label.clone(),
                            property,
                        });
                    }
                    if self.has_node_property_index(label, &property) {
                        return Ok(QueryAccessPlan::NodeIndexSeek {
                            label: label.clone(),
                            property,
                        });
                    }
                }
            }
        }
        if let Some(label) = node.labels.first() {
            Ok(QueryAccessPlan::NodeLabelScan {
                label: label.clone(),
            })
        } else {
            Ok(QueryAccessPlan::NodeFullScan)
        }
    }

    fn best_indexed_node_property<'a>(
        &self,
        label: &str,
        properties: impl Iterator<Item = &'a String>,
    ) -> Option<QueryAccessPlan> {
        let properties = properties.collect::<Vec<_>>();
        properties
            .iter()
            .find(|property| self.has_unique_node_property_constraint(label, property))
            .map(|property| QueryAccessPlan::NodeUniqueIndexSeek {
                label: label.to_string(),
                property: (*property).clone(),
            })
            .or_else(|| {
                properties
                    .iter()
                    .find(|property| self.has_node_property_index(label, property))
                    .map(|property| QueryAccessPlan::NodeIndexSeek {
                        label: label.to_string(),
                        property: (*property).clone(),
                    })
            })
    }

    fn has_vector_index(&self, label: &str, property: &str, metric: &str) -> bool {
        self.index_catalog.indexes.iter().any(|index| {
            index.label == label
                && index.property == property
                && matches!(index.kind, IndexKind::Vector { .. })
                && vector_definition_parts(index)
                    .map(|(_, index_metric)| vector_metric_name(index_metric) == metric)
                    .unwrap_or(false)
        })
    }

    fn relationship_access_plan(&self, pattern: &str) -> QueryAccessPlan {
        let compact = pattern
            .chars()
            .filter(|ch| !ch.is_whitespace())
            .collect::<String>();
        let Some((left, _)) = compact.split_once("->") else {
            return QueryAccessPlan::RelationshipScan;
        };
        let Some((_, rel_part)) = left.split_once("-") else {
            return QueryAccessPlan::RelationshipScan;
        };
        let Ok(inner) = strip_wrapping_write(rel_part, '[', ']') else {
            return QueryAccessPlan::RelationshipScan;
        };
        let rel_type = inner
            .split_once(':')
            .map(|(_, rel_type)| rel_type)
            .or_else(|| inner.strip_prefix(':'));
        match rel_type.filter(|rel_type| !rel_type.is_empty()) {
            Some(rel_type) => QueryAccessPlan::RelationshipTypeScan {
                rel_type: rel_type.to_string(),
            },
            None => QueryAccessPlan::RelationshipScan,
        }
    }

    fn has_unique_node_property_constraint(&self, label: &str, property: &str) -> bool {
        self.index_catalog.indexes.iter().any(|index| {
            matches!(index.kind, IndexKind::UniqueNodeProperty)
                && index.label == label
                && index.property == property
        })
    }

    fn has_node_property_index(&self, label: &str, property: &str) -> bool {
        self.index_catalog.indexes.iter().any(|index| {
            matches!(index.kind, IndexKind::NodeProperty)
                && index.label == label
                && index.property == property
        })
    }

    fn cluster_status(&self) -> ClusterStatus {
        let applied_indexes = self.applied_indexes();
        let committed_indexes = self.committed_indexes();
        let shards = self
            .routing_table
            .placements
            .iter()
            .map(|placement| {
                let primary_server_id = placement.primary_server_id();
                let shard_index = placement.shard_id as usize;
                ShardStatus {
                    shard_id: placement.shard_id,
                    primary_server_id,
                    replica_server_ids: placement
                        .replicas
                        .iter()
                        .filter(|replica| replica.server_id != primary_server_id.unwrap_or(0))
                        .map(|replica| replica.server_id)
                        .collect(),
                    has_local_copy: placement.has_server(self.config.server_id),
                    is_local_primary: primary_server_id == Some(self.config.server_id),
                    applied_index: applied_indexes.get(shard_index).copied().unwrap_or(0),
                    committed_index: committed_indexes.get(shard_index).copied().unwrap_or(0),
                    match_indexes: self
                        .match_indexes
                        .get(shard_index)
                        .map(|indexes| {
                            indexes
                                .iter()
                                .map(|(server_id, index)| (*server_id, *index))
                                .collect()
                        })
                        .unwrap_or_default(),
                }
            })
            .collect();
        ClusterStatus {
            server_id: self.config.server_id,
            routing_version: self.routing_table.version,
            shard_count: self.shard_map.shard_count(),
            local_partition_count: self.store.partition_count(),
            shards,
        }
    }
}

fn validate_config(config: &DatabaseConfig) -> DatabaseResult<()> {
    if config.shard_count == 0 {
        return Err(DatabaseError::InvalidConfig(
            "shard count must be greater than zero".to_string(),
        ));
    }
    if config.local_partition_count == 0 {
        return Err(DatabaseError::InvalidConfig(
            "local partition count must be greater than zero".to_string(),
        ));
    }
    if config.log_entries_per_segment == 0 {
        return Err(DatabaseError::InvalidConfig(
            "log entries per segment must be greater than zero".to_string(),
        ));
    }
    if config.checkpoint_interval == 0 {
        return Err(DatabaseError::InvalidConfig(
            "checkpoint interval must be greater than zero".to_string(),
        ));
    }
    if config.wal_sync_interval == 0 {
        return Err(DatabaseError::InvalidConfig(
            "WAL sync interval must be greater than zero".to_string(),
        ));
    }
    Ok(())
}

fn validate_index_definition(index: &IndexDefinition) -> DatabaseResult<()> {
    validate_catalog_identifier("index name", &index.name)?;
    validate_catalog_identifier("index label", &index.label)?;
    validate_catalog_identifier("index property", &index.property)?;
    if let IndexKind::Vector { dimensions, metric } = &index.kind {
        if *dimensions == 0 {
            return Err(DatabaseError::InvalidConfig(
                "vector index dimensions must be greater than zero".to_string(),
            ));
        }
        if !metric.eq_ignore_ascii_case("cosine") && !metric.eq_ignore_ascii_case("l2") {
            return Err(DatabaseError::InvalidConfig(format!(
                "unsupported vector index metric {metric:?}"
            )));
        }
    }
    Ok(())
}

fn validate_index_catalog(catalog: &IndexCatalog) -> DatabaseResult<()> {
    let mut names = std::collections::HashSet::new();
    let mut unique_node_properties = std::collections::HashSet::new();
    for index in &catalog.indexes {
        validate_index_definition(index)?;
        if !names.insert(index.name.clone()) {
            return Err(DatabaseError::InvalidConfig(format!(
                "duplicate index name {:?}",
                index.name
            )));
        }
        if matches!(index.kind, IndexKind::UniqueNodeProperty)
            && !unique_node_properties.insert((index.label.clone(), index.property.clone()))
        {
            return Err(DatabaseError::InvalidConfig(format!(
                "duplicate unique constraint for label {:?} property {:?}",
                index.label, index.property
            )));
        }
    }
    Ok(())
}

fn node_matches_merge_pattern(node: &Node, labels: &[String], properties: &Properties) -> bool {
    labels
        .iter()
        .all(|label| node.labels.iter().any(|node_label| node_label == label))
        && properties
            .iter()
            .all(|(key, value)| node.properties.get(key) == Some(value))
}

fn property_predicate_for_variable(predicate: &str, variable: &str) -> Option<String> {
    for part in split_write_and_predicates(predicate) {
        let Some((left, _)) = part.split_once('=') else {
            continue;
        };
        let Ok((predicate_variable, key)) = parse_property_ref_write(left.trim()) else {
            continue;
        };
        if predicate_variable == variable {
            return Some(key);
        }
    }
    None
}

fn vector_predicate_for_variable(predicate: &str, variable: &str) -> Option<(String, String)> {
    for part in split_write_and_predicates(predicate) {
        let input = part.trim();
        let Some(inner) = input
            .strip_prefix("vector.knn(")
            .and_then(|value| value.strip_suffix(')'))
        else {
            continue;
        };
        let Ok(args) = split_top_level_commas(inner) else {
            continue;
        };
        if !(args.len() == 3 || args.len() == 4) {
            continue;
        }
        let Ok((predicate_variable, key)) = parse_property_ref_write(args[0]) else {
            continue;
        };
        if predicate_variable != variable {
            continue;
        }
        let metric = if args.len() == 4 {
            args[3]
                .trim()
                .strip_prefix('"')
                .and_then(|value| value.strip_suffix('"'))
                .unwrap_or(args[3].trim())
                .to_string()
        } else {
            "cosine".to_string()
        };
        return Some((key, metric));
    }
    None
}

fn split_write_and_predicates(input: &str) -> Vec<&str> {
    input
        .split(" AND ")
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .collect()
}

fn validate_catalog_identifier(kind: &str, value: &str) -> DatabaseResult<()> {
    if value.trim().is_empty() || value.contains(['\t', '\n', '\r']) {
        return Err(DatabaseError::InvalidConfig(format!(
            "{kind} must be a non-empty token"
        )));
    }
    Ok(())
}

fn open_logs(config: &DatabaseConfig) -> DatabaseResult<Vec<SegmentedShardLog>> {
    (0..config.shard_count)
        .map(|shard_id| {
            SegmentedShardLog::open(&config.data_dir, shard_id, config.log_entries_per_segment)
                .map_err(DatabaseError::from)
        })
        .collect()
}

fn open_checkpoints(config: &DatabaseConfig) -> DatabaseResult<Vec<CheckpointStore>> {
    (0..config.shard_count)
        .map(|shard_id| {
            CheckpointStore::open(&config.data_dir, shard_id).map_err(DatabaseError::from)
        })
        .collect()
}

fn open_commits(config: &DatabaseConfig) -> DatabaseResult<Vec<CommitStore>> {
    (0..config.shard_count)
        .map(|shard_id| CommitStore::open(&config.data_dir, shard_id).map_err(DatabaseError::from))
        .collect()
}

fn load_commit_indexes(commits: &[CommitStore]) -> DatabaseResult<Vec<LogIndex>> {
    commits
        .iter()
        .map(|commit| {
            Ok(commit
                .load()?
                .map(|loaded| loaded.index)
                .unwrap_or_default())
        })
        .collect()
}

fn load_or_initialize_routing_table(
    config: &DatabaseConfig,
    store: &ShardMetadataStore,
) -> DatabaseResult<ShardRoutingTable> {
    if let Some(table) = store.load()? {
        validate_routing_table(&table, config.shard_count)?;
        return Ok(table);
    }

    let table = config
        .routing_table
        .clone()
        .unwrap_or_else(|| ShardRoutingTable::single_server(config.shard_count, config.server_id));
    validate_routing_table(&table, config.shard_count)?;
    store.save(&table)?;
    Ok(table)
}

fn load_or_initialize_membership(
    config: &DatabaseConfig,
    store: &ClusterMembershipStore,
) -> DatabaseResult<ClusterMembership> {
    if let Some(membership) = store.load()? {
        return Ok(membership);
    }
    let membership = ClusterMembership {
        version: 1,
        nodes: vec![ClusterNode {
            server_id: config.server_id,
            address: String::new(),
            state: NodeMembershipState::Active,
            protocol_version: 0,
            storage_version: 0,
            shard_count: config.shard_count,
            rejection_reason: String::new(),
        }],
        shard_assignments: Vec::new(),
    };
    store.save(&membership)?;
    Ok(membership)
}

fn load_or_initialize_cluster_metadata(
    config: &DatabaseConfig,
    store: &ClusterMetadataStore,
    routing_table: &ShardRoutingTable,
) -> DatabaseResult<ClusterMetadataState> {
    if let Some(mut metadata) = store.load()? {
        if metadata.config_epoch == 0 {
            metadata.config_epoch = routing_table.version;
            store.save(&metadata)?;
        }
        return Ok(metadata);
    }
    let metadata = ClusterMetadataState {
        authority_server_id: config.server_id,
        term: 1,
        config_epoch: routing_table.version,
        policy: RebalancePolicy::default(),
    };
    store.save(&metadata)?;
    Ok(metadata)
}

#[derive(Default)]
struct StorageFileStats {
    total_bytes: u64,
    file_count: u64,
    wal_segment_count: u64,
    checkpoint_file_count: u64,
    metadata_file_count: u64,
}

fn collect_storage_files(data_dir: &Path) -> DatabaseResult<StorageFileStats> {
    let mut stats = StorageFileStats::default();
    collect_storage_files_inner(data_dir, &mut stats)?;
    Ok(stats)
}

fn collect_storage_files_inner(path: &Path, stats: &mut StorageFileStats) -> DatabaseResult<()> {
    let entries = match fs::read_dir(path) {
        Ok(entries) => entries,
        Err(err) if err.kind() == ErrorKind::NotFound => return Ok(()),
        Err(err) => return Err(StorageError::Io(err).into()),
    };
    for entry in entries {
        let entry = entry.map_err(StorageError::Io)?;
        let path = entry.path();
        let metadata = entry.metadata().map_err(StorageError::Io)?;
        if metadata.is_dir() {
            collect_storage_files_inner(&path, stats)?;
            continue;
        }
        stats.file_count += 1;
        stats.total_bytes = stats.total_bytes.saturating_add(metadata.len());
        let name = path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or_default();
        if name.ends_with(".log") || name.contains("segment") {
            stats.wal_segment_count += 1;
        }
        if name.contains("checkpoint") || name.ends_with(".bin") {
            stats.checkpoint_file_count += 1;
        }
        if matches!(
            name,
            "routing-table.txt"
                | "membership.txt"
                | "rebalance-plan.txt"
                | "rebalance-execution.txt"
                | "metadata-authority.txt"
                | "index-catalog.txt"
        ) {
            stats.metadata_file_count += 1;
        }
    }
    Ok(())
}

fn estimate_rows(statistics: &StatisticsCatalog, plan: &QueryAccessPlan) -> u64 {
    match plan {
        QueryAccessPlan::NodeUniqueIndexSeek { .. } => 1,
        QueryAccessPlan::NodeIndexSeek { label, .. } => label_count(statistics, label).max(1) / 10,
        QueryAccessPlan::NodeLabelScan { label } => label_count(statistics, label),
        QueryAccessPlan::NodeFullScan => statistics.node_count as u64,
        QueryAccessPlan::VectorIndexSeek { .. } => 10,
        QueryAccessPlan::RelationshipTypeScan { rel_type } => {
            relationship_type_count(statistics, rel_type)
        }
        QueryAccessPlan::RelationshipScan => statistics.relationship_count as u64,
        QueryAccessPlan::Unsupported { .. } => {
            (statistics.node_count + statistics.relationship_count) as u64
        }
    }
}

fn estimate_query_cost(
    statistics: &StatisticsCatalog,
    plan: &QueryAccessPlan,
    remote_shard_count: usize,
) -> u64 {
    let base = match plan {
        QueryAccessPlan::NodeUniqueIndexSeek { .. } => 1,
        QueryAccessPlan::NodeIndexSeek { label, .. } => label_count(statistics, label).max(1) / 20,
        QueryAccessPlan::NodeLabelScan { label } => label_count(statistics, label).max(1),
        QueryAccessPlan::NodeFullScan => statistics.node_count.max(1) as u64,
        QueryAccessPlan::VectorIndexSeek { .. } => 25,
        QueryAccessPlan::RelationshipTypeScan { rel_type } => {
            relationship_type_count(statistics, rel_type).max(1)
        }
        QueryAccessPlan::RelationshipScan => statistics.relationship_count.max(1) as u64,
        QueryAccessPlan::Unsupported { .. } => {
            (statistics.node_count + statistics.relationship_count).max(1) as u64
        }
    };
    base.saturating_add(remote_shard_count as u64 * 100)
}

fn estimated_scanned_nodes(statistics: &StatisticsCatalog, plan: &QueryAccessPlan) -> usize {
    match plan {
        QueryAccessPlan::NodeUniqueIndexSeek { .. }
        | QueryAccessPlan::NodeIndexSeek { .. }
        | QueryAccessPlan::VectorIndexSeek { .. } => estimate_rows(statistics, plan) as usize,
        QueryAccessPlan::NodeLabelScan { label } => label_count(statistics, label) as usize,
        QueryAccessPlan::NodeFullScan | QueryAccessPlan::Unsupported { .. } => {
            statistics.node_count
        }
        QueryAccessPlan::RelationshipTypeScan { .. } | QueryAccessPlan::RelationshipScan => 0,
    }
}

fn estimated_scanned_relationships(
    statistics: &StatisticsCatalog,
    plan: &QueryAccessPlan,
) -> usize {
    match plan {
        QueryAccessPlan::RelationshipTypeScan { rel_type } => {
            relationship_type_count(statistics, rel_type) as usize
        }
        QueryAccessPlan::RelationshipScan | QueryAccessPlan::Unsupported { .. } => {
            statistics.relationship_count
        }
        _ => 0,
    }
}

fn query_operator_profile(
    access_plan: &QueryAccessPlan,
    estimated_rows: u64,
    actual_rows: usize,
    elapsed_micros: u128,
) -> QueryOperatorProfile {
    let scan = QueryOperatorProfile {
        name: format_access_plan_name(access_plan).to_string(),
        estimated_rows,
        actual_rows,
        elapsed_micros,
        children: Vec::new(),
    };
    QueryOperatorProfile {
        name: "Project".to_string(),
        estimated_rows,
        actual_rows,
        elapsed_micros,
        children: vec![scan],
    }
}

fn format_access_plan_name(access_plan: &QueryAccessPlan) -> &'static str {
    match access_plan {
        QueryAccessPlan::NodeUniqueIndexSeek { .. } => "NodeUniqueIndexSeek",
        QueryAccessPlan::NodeIndexSeek { .. } => "NodeIndexSeek",
        QueryAccessPlan::NodeLabelScan { .. } => "NodeLabelScan",
        QueryAccessPlan::NodeFullScan => "NodeFullScan",
        QueryAccessPlan::VectorIndexSeek { .. } => "VectorIndexSeek",
        QueryAccessPlan::RelationshipTypeScan { .. } => "RelationshipTypeScan",
        QueryAccessPlan::RelationshipScan => "RelationshipScan",
        QueryAccessPlan::Unsupported { .. } => "Unsupported",
    }
}

fn label_count(statistics: &StatisticsCatalog, label: &str) -> u64 {
    statistics
        .label_counts
        .iter()
        .find(|(candidate, _)| candidate == label)
        .map(|(_, count)| *count as u64)
        .unwrap_or_default()
}

fn relationship_type_count(statistics: &StatisticsCatalog, rel_type: &str) -> u64 {
    statistics
        .relationship_type_counts
        .iter()
        .find(|(candidate, _)| candidate == rel_type)
        .map(|(_, count)| *count as u64)
        .unwrap_or_default()
}

fn validate_routing_table(table: &ShardRoutingTable, shard_count: u64) -> DatabaseResult<()> {
    if table.version == 0 {
        return Err(DatabaseError::InvalidConfig(
            "routing table version must be greater than zero".to_string(),
        ));
    }
    if table.placements.len() != shard_count as usize {
        return Err(DatabaseError::InvalidConfig(format!(
            "routing table must contain {shard_count} shard placements"
        )));
    }
    for shard_id in 0..shard_count {
        let Some(placement) = table.placement(shard_id) else {
            return Err(DatabaseError::InvalidConfig(format!(
                "routing table missing shard {shard_id}"
            )));
        };
        if placement.primary_server_id().is_none() {
            return Err(DatabaseError::InvalidConfig(format!(
                "routing table shard {shard_id} has no primary"
            )));
        }
    }
    Ok(())
}

fn mutable_placement(
    routing_table: &mut ShardRoutingTable,
    shard_id: ShardId,
) -> DatabaseResult<&mut ShardPlacement> {
    routing_table
        .placements
        .iter_mut()
        .find(|placement| placement.shard_id == shard_id)
        .ok_or_else(|| {
            DatabaseError::InvalidConfig(format!("routing table missing shard {shard_id}"))
        })
}

fn validate_cluster_node_address(address: &str) -> DatabaseResult<()> {
    if address.contains(['\t', '\n', '\r']) {
        return Err(DatabaseError::InvalidConfig(
            "cluster node address contains invalid characters".to_string(),
        ));
    }
    Ok(())
}

fn validate_rejection_reason(reason: &str) -> DatabaseResult<()> {
    if reason.contains(['\t', '\n', '\r']) {
        return Err(DatabaseError::InvalidConfig(
            "cluster join rejection reason contains invalid characters".to_string(),
        ));
    }
    Ok(())
}

fn is_retryable_rebalance_error(err: &DatabaseError) -> bool {
    match err {
        DatabaseError::InvalidConfig(message) => {
            message.contains("caught up")
                || message.contains("catch-up")
                || message.contains("behind committed index")
                || message.contains("transfer ready")
        }
        DatabaseError::Replication(_) | DatabaseError::WriterUnavailable => true,
        _ => false,
    }
}

#[cfg(test)]
mod tests;
