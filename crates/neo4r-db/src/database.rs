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

include!("database/handle_write_query.rs");
include!("database/handle_raft_read.rs");
include!("database/handle_admin.rs");

include!("database/read_snapshot.rs");
include!("database/staged_overlay.rs");
include!("database/metadata_types.rs");
include!("database/write_cypher_model.rs");
include!("database/write_cypher_parse.rs");
include!("database/write_cypher_helpers.rs");

include!("database/db_open_write.rs");
include!("database/db_write_schema.rs");
include!("database/db_cluster.rs");
include!("database/db_index_validation.rs");
include!("database/db_raft_apply.rs");
include!("database/db_maintenance_plan.rs");
fn summarize_rebalance_automation(
    execution: Option<&RebalanceExecution>,
) -> RebalanceAutomationSummary {
    let Some(execution) = execution else {
        return RebalanceAutomationSummary {
            state: "idle".to_string(),
            pending_steps: 0,
            running_steps: 0,
            ready_steps: 0,
            applied_steps: 0,
            failed_steps: 0,
            blocked_reason: String::new(),
        };
    };
    let mut summary = RebalanceAutomationSummary {
        state: format!("{:?}", execution.state).to_ascii_lowercase(),
        pending_steps: 0,
        running_steps: 0,
        ready_steps: 0,
        applied_steps: 0,
        failed_steps: 0,
        blocked_reason: execution.last_error.clone(),
    };
    for step in &execution.steps {
        match step.state {
            RebalanceStepState::Pending => summary.pending_steps += 1,
            RebalanceStepState::Preparing
            | RebalanceStepState::CatchingUp
            | RebalanceStepState::Applying => summary.running_steps += 1,
            RebalanceStepState::Ready => summary.ready_steps += 1,
            RebalanceStepState::Applied => summary.applied_steps += 1,
            RebalanceStepState::Failed => {
                summary.failed_steps += 1;
                if summary.blocked_reason.is_empty() {
                    summary.blocked_reason = step.last_error.clone();
                }
            }
            RebalanceStepState::Cancelled => {
                if summary.blocked_reason.is_empty() {
                    summary.blocked_reason = "cancelled".to_string();
                }
            }
        }
    }
    summary
}

fn snapshot_payload_checksum(snapshot_store: &neo4r_storage::SnapshotStore) -> DatabaseResult<u64> {
    let Some(payload) = snapshot_store.load_payload()? else {
        return Ok(0);
    };
    Ok(payload.iter().fold(0xcbf29ce484222325_u64, |hash, byte| {
        hash.wrapping_mul(0x100000001b3).wrapping_add(*byte as u64)
    }))
}

fn storage_invariant_maintenance_result(
    action: &str,
    report: &GraphInvariantReport,
) -> StorageMaintenanceResult {
    let missing = report.missing_index_keys.len() as u64;
    let unexpected = report.unexpected_index_keys.len() as u64;
    StorageMaintenanceResult {
        action: action.to_string(),
        files_touched: missing.saturating_add(unexpected),
        bytes_observed: missing.saturating_add(unexpected),
        pruned_until: Vec::new(),
        safety_manifest: format!(
            "storage_invariant_manifest:v1 clean={} missing_index_keys={} unexpected_index_keys={}",
            report.is_clean(),
            missing,
            unexpected
        ),
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
        QueryAccessPlan::NodeIndexSeek { label, .. } => {
            estimate_indexed_label_rows(statistics, label)
        }
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
        QueryAccessPlan::NodeIndexSeek { label, .. } => {
            estimate_indexed_label_rows(statistics, label).max(1)
        }
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

fn access_plan_reason(
    plan: &QueryAccessPlan,
    statistics: &StatisticsCatalog,
    remote_shard_count: usize,
) -> String {
    let local_reason = match plan {
        QueryAccessPlan::NodeUniqueIndexSeek { label, property } => {
            format!("unique constraint on {label}.{property}")
        }
        QueryAccessPlan::NodeIndexSeek { label, property } => {
            format!(
                "property index on {label}.{property}; label_cardinality={} property_cardinality={} estimated_rows={}",
                label_count(statistics, label),
                node_property_count(statistics, property),
                estimate_indexed_label_rows(statistics, label)
            )
        }
        QueryAccessPlan::NodeLabelScan { label } => {
            format!(
                "label cardinality {} for {label}",
                label_count(statistics, label)
            )
        }
        QueryAccessPlan::NodeFullScan => {
            format!(
                "no selective node access path; nodes={} indexes={}",
                statistics.node_count, statistics.index_count
            )
        }
        QueryAccessPlan::VectorIndexSeek {
            label,
            property,
            metric,
        } => {
            let label = label.as_deref().unwrap_or("*");
            format!("vector index on {label}.{property} metric={metric}")
        }
        QueryAccessPlan::RelationshipTypeScan { rel_type } => format!(
            "relationship type cardinality {} for {rel_type}",
            relationship_type_count(statistics, rel_type)
        ),
        QueryAccessPlan::RelationshipScan => format!(
            "no selective relationship access path; relationships={}",
            statistics.relationship_count
        ),
        QueryAccessPlan::Unsupported { reason } => format!("unsupported planner path: {reason}"),
    };
    if remote_shard_count == 0 {
        local_reason
    } else {
        format!("{local_reason}; remote_shard_penalty={remote_shard_count}")
    }
}

fn estimate_indexed_label_rows(statistics: &StatisticsCatalog, label: &str) -> u64 {
    let label_rows = label_count(statistics, label).max(1);
    let index_bonus = (statistics.index_count as u64).max(1);
    let divisor = 8_u64.saturating_add(index_bonus.min(16));
    label_rows.div_ceil(divisor).max(1)
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

fn node_property_count(statistics: &StatisticsCatalog, property: &str) -> u64 {
    statistics
        .node_property_counts
        .iter()
        .find(|(candidate, _)| candidate == property)
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
