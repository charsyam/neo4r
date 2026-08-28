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
    pub node_property_counts: Vec<(String, usize)>,
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
    pub safety_manifest: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IndexLifecycleStatus {
    pub name: String,
    pub state: String,
    pub backfilled_entries: usize,
    pub failure: String,
}

#[derive(Clone, Debug)]
struct IndexLifecycleStore {
    path: PathBuf,
}

impl IndexLifecycleStore {
    fn open(data_dir: impl AsRef<Path>) -> DatabaseResult<Self> {
        let index_dir = data_dir.as_ref().join("indexes");
        fs::create_dir_all(&index_dir).map_err(StorageError::Io)?;
        Ok(Self {
            path: index_dir.join("lifecycle.txt"),
        })
    }

    fn save_status(&self, status: &IndexLifecycleStatus) -> DatabaseResult<()> {
        let mut statuses = self.load()?;
        statuses.retain(|existing| existing.name != status.name);
        statuses.push(status.clone());
        statuses.sort_by(|left, right| left.name.cmp(&right.name));
        self.save_all(&statuses)
    }

    fn load(&self) -> DatabaseResult<Vec<IndexLifecycleStatus>> {
        let text = match fs::read_to_string(&self.path) {
            Ok(text) => text,
            Err(err) if err.kind() == ErrorKind::NotFound => return Ok(Vec::new()),
            Err(err) => return Err(StorageError::Io(err).into()),
        };
        let mut statuses = Vec::new();
        for line in text.lines() {
            if line.trim().is_empty() {
                continue;
            }
            let parts = line.split('\t').collect::<Vec<_>>();
            if parts.len() != 4 {
                return Err(StorageError::CorruptStore(
                    "invalid index lifecycle record".to_string(),
                )
                .into());
            }
            statuses.push(IndexLifecycleStatus {
                name: parts[0].to_string(),
                state: parts[1].to_string(),
                backfilled_entries: parse_plan_usize(parts[2], "index lifecycle backfilled")?,
                failure: parts[3].to_string(),
            });
        }
        Ok(statuses)
    }

    fn save_all(&self, statuses: &[IndexLifecycleStatus]) -> DatabaseResult<()> {
        let tmp_path = self.path.with_extension("txt.tmp");
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp_path)
            .map_err(StorageError::Io)?;
        for status in statuses {
            writeln!(
                file,
                "{}\t{}\t{}\t{}",
                sanitize_cluster_text(&status.name),
                sanitize_cluster_text(&status.state),
                status.backfilled_entries,
                sanitize_cluster_text(&status.failure)
            )
            .map_err(StorageError::Io)?;
        }
        file.sync_all().map_err(StorageError::Io)?;
        drop(file);
        fs::rename(&tmp_path, &self.path).map_err(StorageError::Io)?;
        Ok(())
    }
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
pub struct RaftShardStatus {
    pub shard_id: ShardId,
    pub term: Term,
    pub role: RaftRole,
    pub leader_id: Option<ServerId>,
    pub commit_index: LogIndex,
    pub last_log_index: LogIndex,
    pub snapshot_index: LogIndex,
    pub joint_consensus: bool,
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
    pub rebalance_automation: RebalanceAutomationSummary,
    pub routing_version: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RebalanceAutomationSummary {
    pub state: String,
    pub pending_steps: usize,
    pub running_steps: usize,
    pub ready_steps: usize,
    pub applied_steps: usize,
    pub failed_steps: usize,
    pub blocked_reason: String,
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
        for (property, count) in &statistics.node_property_counts {
            writeln!(
                file,
                "node_property\t{}\t{}",
                sanitize_cluster_text(property),
                count
            )
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
        let mut node_property_counts = Vec::new();
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
                ["node_property", property, count] => {
                    node_property_counts.push((
                        (*property).to_string(),
                        parse_plan_usize(count, "node property count")?,
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
            node_property_counts,
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
