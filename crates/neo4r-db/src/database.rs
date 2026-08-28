use crate::{
    AppendEntriesRequest, AppendEntriesResponse, DatabaseError, DatabaseResult,
    InstallSnapshotRequest, InstallSnapshotResponse, NoopShardReplicator, RaftCore, RaftMembership,
    RaftPersistentStateStore, RaftRole, RaftSnapshotMetadata, ReplicationOutcome,
    RequestVoteRequest, RequestVoteResponse, ShardReplicator,
};
mod vector_index;
use neo4r_core::{
    BoundaryNode, Command, GraphError, GraphRead, GraphReadError, GraphReadResult, HybridClock,
    HybridTimestamp, LogEntry, LogIndex, Node, NodeId, Properties, Relationship, RelationshipId,
    ServerId, ShardId, ShardMap, ShardPlacement, ShardPolicy, ShardReplica, ShardRole,
    ShardRoutingTable, Term, Value,
};
use neo4r_query::{
    CypherEngine, QueryCursor, QueryEngine, QueryError, QueryParams, QueryRow, QueryValue,
    VecQueryCursor,
};
use neo4r_storage::{
    CheckpointStore, ClusterMembership, ClusterMembershipStore, ClusterNode,
    ClusterShardAssignment, CommitStore, GraphInvariantReport, IndexCatalog, IndexCatalogStore,
    IndexDefinition, IndexKind, NodeMembershipState, PartitionedGraphStore, RocksKvSnapshot,
    RocksKvStore, SegmentedShardLog, ShardAssignmentState, ShardMetadataStore, StorageError,
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
    pub raft_enabled: bool,
    pub failure_injection: FailureInjection,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct FailureInjection {
    pub fail_after_commit_before_apply: bool,
    pub fail_before_snapshot_payload_save: bool,
    pub fail_after_snapshot_payload_save_before_metadata: bool,
    pub fail_after_snapshot_prune_before_apply: bool,
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
            raft_enabled: false,
            failure_injection: FailureInjection::default(),
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

    pub fn with_raft_enabled(mut self, enabled: bool) -> Self {
        self.raft_enabled = enabled;
        self
    }

    pub fn with_failure_injection(mut self, failure_injection: FailureInjection) -> Self {
        self.failure_injection = failure_injection;
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
    index_lifecycle_store: IndexLifecycleStore,
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
    raft_groups: Option<RaftShardConsensus>,
}

#[derive(Clone)]
pub struct Neo4rDatabaseHandle {
    inner: Arc<Mutex<Neo4rDatabase>>,
    writer: Arc<WriterActor>,
}

struct RaftShardConsensus {
    groups: Vec<RaftCore>,
    leader_contacts: Vec<Option<Instant>>,
}

impl RaftShardConsensus {
    fn open(
        config: &DatabaseConfig,
        routing_table: &ShardRoutingTable,
        logs: &[SegmentedShardLog],
        commit_indexes: &[LogIndex],
    ) -> DatabaseResult<Self> {
        let mut groups = Vec::with_capacity(logs.len());
        for shard_id in 0..logs.len() as ShardId {
            let placement = routing_table.placement(shard_id).ok_or_else(|| {
                DatabaseError::Replication(format!(
                    "missing routing placement for raft shard {shard_id}"
                ))
            })?;
            let voters = placement
                .replicas
                .iter()
                .map(|replica| replica.server_id)
                .collect::<Vec<_>>();
            let membership = RaftMembership::new(voters)?;
            let state_store = RaftPersistentStateStore::open(
                config
                    .data_dir
                    .join("raft")
                    .join(format!("shard-{shard_id}.state")),
            );
            let log = logs[shard_id as usize].replay_from(0)?;
            let snapshot = neo4r_storage::SnapshotStore::open(&config.data_dir, shard_id)?
                .load()?
                .map(|snapshot| RaftSnapshotMetadata {
                    shard_id: snapshot.shard_id,
                    last_included_term: snapshot.last_included_term,
                    last_included_index: snapshot.last_included_index,
                });
            let commit_index = commit_indexes
                .get(shard_id as usize)
                .copied()
                .unwrap_or_default();
            let mut group = RaftCore::open_with_log_and_snapshot(
                config.server_id,
                shard_id,
                state_store,
                membership,
                log,
                snapshot,
                commit_index,
            )?;
            if placement.primary_server_id() == Some(config.server_id) {
                if group.current_term() == 0 {
                    let _ = group.start_election()?;
                }
                group.become_leader();
            }
            groups.push(group);
        }
        Ok(Self {
            leader_contacts: vec![None; groups.len()],
            groups,
        })
    }

    fn group_mut(&mut self, shard_id: ShardId) -> DatabaseResult<&mut RaftCore> {
        self.groups
            .get_mut(shard_id as usize)
            .ok_or(DatabaseError::MissingShardLog(shard_id))
    }

    fn record_leader_contact(&mut self, shard_id: ShardId) -> DatabaseResult<()> {
        let slot = self
            .leader_contacts
            .get_mut(shard_id as usize)
            .ok_or(DatabaseError::MissingShardLog(shard_id))?;
        *slot = Some(Instant::now());
        Ok(())
    }

    fn should_start_election(&self, shard_id: ShardId, timeout: Duration) -> DatabaseResult<bool> {
        let group = self
            .groups
            .get(shard_id as usize)
            .ok_or(DatabaseError::MissingShardLog(shard_id))?;
        if group.role() == &RaftRole::Leader {
            return Ok(false);
        }
        let due = self
            .leader_contacts
            .get(shard_id as usize)
            .copied()
            .flatten()
            .is_none_or(|contact| contact.elapsed() >= timeout);
        Ok(due)
    }
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

mod handle_admin;
mod handle_raft_read;
mod handle_write_query;

mod metadata_types;
mod read_snapshot;
mod staged_overlay;
mod write_cypher_helpers;
mod write_cypher_model;
mod write_cypher_parse;

mod db_cluster;
mod db_index_validation;
mod db_maintenance_plan;
mod db_open_write;
mod db_raft_apply;
mod db_write_schema;
mod helpers;

use helpers::*;
pub(super) use metadata_types::*;
pub use metadata_types::{
    ClusterManagementStatus, ClusterMetadataState, ClusterStatus, IndexLifecycleStatus,
    MetadataOperationRecord, RaftShardStatus, RebalanceAdvanceResult, RebalanceAutomationSummary,
    RebalanceExecution, RebalancePlan, RebalancePlanState, RebalancePolicy, RebalanceStep,
    RebalanceStepExecution, RebalanceStepState, ShardStatus, StatisticsCatalog,
    StorageMaintenanceResult, StorageStatus,
};
pub(super) use staged_overlay::*;
pub use staged_overlay::{
    DistributedQueryPlan, QueryAccessPlan, QueryMetrics, QueryOperatorProfile, QueryProfile,
    QueryRoute, RemoteTraversalPolicy,
};
use write_cypher_helpers::{parse_property_ref_write, split_top_level_commas};
pub use write_cypher_model::{
    create_node_routing_key, merge_node_routing_key, CreateNodeRoutingKey,
};

#[cfg(test)]
mod tests;
