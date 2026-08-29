use super::*;

pub(in crate::database) fn summarize_rebalance_automation(
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

pub(in crate::database) fn snapshot_payload_checksum(
    snapshot_store: &neo4r_storage::SnapshotStore,
) -> DatabaseResult<u64> {
    let Some(payload) = snapshot_store.load_payload()? else {
        return Ok(0);
    };
    Ok(payload.iter().fold(0xcbf29ce484222325_u64, |hash, byte| {
        hash.wrapping_mul(0x100000001b3).wrapping_add(*byte as u64)
    }))
}

pub(in crate::database) fn storage_invariant_maintenance_result(
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

pub(in crate::database) fn validate_config(config: &DatabaseConfig) -> DatabaseResult<()> {
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

pub(in crate::database) fn validate_index_definition(
    index: &IndexDefinition,
) -> DatabaseResult<()> {
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

pub(in crate::database) fn validate_index_catalog(catalog: &IndexCatalog) -> DatabaseResult<()> {
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

pub(in crate::database) fn node_matches_merge_pattern(
    node: &Node,
    labels: &[String],
    properties: &Properties,
) -> bool {
    labels
        .iter()
        .all(|label| node.labels.iter().any(|node_label| node_label == label))
        && properties
            .iter()
            .all(|(key, value)| node.properties.get(key) == Some(value))
}

pub(in crate::database) fn property_predicate_for_variable(
    predicate: &str,
    variable: &str,
) -> Option<String> {
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

pub(in crate::database) fn vector_predicate_for_variable(
    predicate: &str,
    variable: &str,
) -> Option<(String, String)> {
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

pub(in crate::database) fn split_write_and_predicates(input: &str) -> Vec<&str> {
    input
        .split(" AND ")
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .collect()
}

pub(in crate::database) fn validate_catalog_identifier(
    kind: &str,
    value: &str,
) -> DatabaseResult<()> {
    if value.trim().is_empty() || value.contains(['\t', '\n', '\r']) {
        return Err(DatabaseError::InvalidConfig(format!(
            "{kind} must be a non-empty token"
        )));
    }
    Ok(())
}

pub(in crate::database) fn open_logs(
    config: &DatabaseConfig,
) -> DatabaseResult<Vec<SegmentedShardLog>> {
    (0..config.shard_count)
        .map(|shard_id| {
            SegmentedShardLog::open(&config.data_dir, shard_id, config.log_entries_per_segment)
                .map_err(DatabaseError::from)
        })
        .collect()
}

pub(in crate::database) fn open_checkpoints(
    config: &DatabaseConfig,
) -> DatabaseResult<Vec<CheckpointStore>> {
    (0..config.shard_count)
        .map(|shard_id| {
            CheckpointStore::open(&config.data_dir, shard_id).map_err(DatabaseError::from)
        })
        .collect()
}

pub(in crate::database) fn open_commits(
    config: &DatabaseConfig,
) -> DatabaseResult<Vec<CommitStore>> {
    (0..config.shard_count)
        .map(|shard_id| CommitStore::open(&config.data_dir, shard_id).map_err(DatabaseError::from))
        .collect()
}

pub(in crate::database) fn load_commit_indexes(
    commits: &[CommitStore],
) -> DatabaseResult<Vec<LogIndex>> {
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

pub(in crate::database) fn load_or_initialize_routing_table(
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

pub(in crate::database) fn load_or_initialize_membership(
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

pub(in crate::database) fn load_or_initialize_cluster_metadata(
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
pub(in crate::database) struct StorageFileStats {
    pub(in crate::database) total_bytes: u64,
    pub(in crate::database) file_count: u64,
    pub(in crate::database) wal_segment_count: u64,
    pub(in crate::database) checkpoint_file_count: u64,
    pub(in crate::database) metadata_file_count: u64,
}

pub(in crate::database) fn collect_storage_files(
    data_dir: &Path,
) -> DatabaseResult<StorageFileStats> {
    let mut stats = StorageFileStats::default();
    collect_storage_files_inner(data_dir, &mut stats)?;
    Ok(stats)
}

pub(in crate::database) fn collect_storage_files_inner(
    path: &Path,
    stats: &mut StorageFileStats,
) -> DatabaseResult<()> {
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

pub(in crate::database) fn estimate_rows(
    statistics: &StatisticsCatalog,
    plan: &QueryAccessPlan,
) -> u64 {
    match plan {
        QueryAccessPlan::NodeUniqueIndexSeek { .. } => 1,
        QueryAccessPlan::NodeIndexSeek { label, property } => {
            estimate_indexed_property_rows(statistics, label, property)
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

pub(in crate::database) fn estimate_query_cost(
    statistics: &StatisticsCatalog,
    plan: &QueryAccessPlan,
    remote_shard_count: usize,
) -> u64 {
    let base = match plan {
        QueryAccessPlan::NodeUniqueIndexSeek { .. } => 1,
        QueryAccessPlan::NodeIndexSeek { label, property } => {
            estimate_indexed_property_rows(statistics, label, property).max(1)
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

pub(in crate::database) fn access_plan_reason(
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
                estimate_indexed_property_rows(statistics, label, property)
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

pub(in crate::database) fn estimate_indexed_label_rows(
    statistics: &StatisticsCatalog,
    label: &str,
) -> u64 {
    let label_rows = label_count(statistics, label).max(1);
    let index_bonus = (statistics.index_count as u64).max(1);
    let divisor = 8_u64.saturating_add(index_bonus.min(16));
    label_rows.div_ceil(divisor).max(1)
}

pub(in crate::database) fn estimate_indexed_property_rows(
    statistics: &StatisticsCatalog,
    label: &str,
    property: &str,
) -> u64 {
    let label_rows = label_count(statistics, label).max(1);
    let property_rows = node_property_count(statistics, property).max(1);
    let total_rows = (statistics.node_count as u64).max(1);
    let intersection = label_rows
        .saturating_mul(property_rows)
        .div_ceil(total_rows)
        .max(1);
    intersection.min(estimate_indexed_label_rows(statistics, label))
}

pub(in crate::database) fn estimated_scanned_nodes(
    statistics: &StatisticsCatalog,
    plan: &QueryAccessPlan,
) -> usize {
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

pub(in crate::database) fn estimated_scanned_relationships(
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

pub(in crate::database) fn query_operator_profile(
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

pub(in crate::database) fn format_access_plan_name(access_plan: &QueryAccessPlan) -> &'static str {
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

pub(in crate::database) fn label_count(statistics: &StatisticsCatalog, label: &str) -> u64 {
    statistics
        .label_counts
        .iter()
        .find(|(candidate, _)| candidate == label)
        .map(|(_, count)| *count as u64)
        .unwrap_or_default()
}

pub(in crate::database) fn relationship_type_count(
    statistics: &StatisticsCatalog,
    rel_type: &str,
) -> u64 {
    statistics
        .relationship_type_counts
        .iter()
        .find(|(candidate, _)| candidate == rel_type)
        .map(|(_, count)| *count as u64)
        .unwrap_or_default()
}

pub(in crate::database) fn node_property_count(
    statistics: &StatisticsCatalog,
    property: &str,
) -> u64 {
    statistics
        .node_property_counts
        .iter()
        .find(|(candidate, _)| candidate == property)
        .map(|(_, count)| *count as u64)
        .unwrap_or_default()
}

pub(in crate::database) fn validate_routing_table(
    table: &ShardRoutingTable,
    shard_count: u64,
) -> DatabaseResult<()> {
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

pub(in crate::database) fn mutable_placement(
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

pub(in crate::database) fn validate_cluster_node_address(address: &str) -> DatabaseResult<()> {
    if address.contains(['\t', '\n', '\r']) {
        return Err(DatabaseError::InvalidConfig(
            "cluster node address contains invalid characters".to_string(),
        ));
    }
    Ok(())
}

pub(in crate::database) fn validate_rejection_reason(reason: &str) -> DatabaseResult<()> {
    if reason.contains(['\t', '\n', '\r']) {
        return Err(DatabaseError::InvalidConfig(
            "cluster join rejection reason contains invalid characters".to_string(),
        ));
    }
    Ok(())
}

pub(in crate::database) fn is_retryable_rebalance_error(err: &DatabaseError) -> bool {
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
