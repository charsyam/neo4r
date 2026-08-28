use super::*;

pub(super) fn format_cluster_status(status: &ClusterStatus) -> String {
    let shards = status
        .shards
        .iter()
        .map(format_shard_status)
        .collect::<Vec<_>>()
        .join(",");
    format!(
        "server={} routing_version={} shards={} partitions={} [{}]",
        status.server_id,
        status.routing_version,
        status.shard_count,
        status.local_partition_count,
        shards
    )
}

pub(super) fn format_raft_status(status: &[neo4r_db::RaftShardStatus]) -> String {
    let shards = status
        .iter()
        .map(|shard| {
            format!(
                "{}:term={}:role={:?}:leader={}:commit={}:last_log={}:snapshot={}:joint={}",
                shard.shard_id,
                shard.term,
                shard.role,
                shard
                    .leader_id
                    .map(|leader| leader.to_string())
                    .unwrap_or_else(|| "none".to_string()),
                shard.commit_index,
                shard.last_log_index,
                shard.snapshot_index,
                shard.joint_consensus
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    format!("raft_shards={} [{}]", status.len(), shards)
}

pub(super) fn format_cluster_membership(membership: &ClusterMembership) -> String {
    let nodes = membership
        .nodes
        .iter()
        .map(|node| {
            format!(
                "{}:{}:{}:protocol={}:storage={}:shards={}:reason={}",
                node.server_id,
                format_node_state(node.state),
                node.address,
                node.protocol_version,
                node.storage_version,
                node.shard_count,
                node.rejection_reason
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    let assignments = membership
        .shard_assignments
        .iter()
        .map(|assignment| {
            format!(
                "shard={}:server={}:state={}:match={}",
                assignment.shard_id,
                assignment.server_id,
                format_assignment_state(assignment.state),
                assignment.match_index
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    format!(
        "version={} nodes=[{}] assignments=[{}]",
        membership.version, nodes, assignments
    )
}

pub(super) fn format_node_state(state: NodeMembershipState) -> &'static str {
    match state {
        NodeMembershipState::Negotiating => "negotiating",
        NodeMembershipState::Joining => "joining",
        NodeMembershipState::Active => "active",
        NodeMembershipState::Draining => "draining",
        NodeMembershipState::Leaving => "leaving",
        NodeMembershipState::Removed => "removed",
        NodeMembershipState::Dead => "dead",
        NodeMembershipState::Rejected => "rejected",
    }
}

pub(super) fn format_assignment_state(state: ShardAssignmentState) -> &'static str {
    match state {
        ShardAssignmentState::Planned => "planned",
        ShardAssignmentState::CatchingUp => "catching_up",
        ShardAssignmentState::CaughtUp => "caught_up",
        ShardAssignmentState::ServingReplica => "serving_replica",
        ShardAssignmentState::Promoting => "promoting",
        ShardAssignmentState::ServingPrimary => "serving_primary",
        ShardAssignmentState::Removing => "removing",
        ShardAssignmentState::Removed => "removed",
    }
}

pub(super) fn format_cluster_metadata(metadata: &ClusterMetadataState) -> String {
    format!(
        "authority={} term={} config_epoch={} policy=replication_factor:{}:max_steps:{}",
        metadata.authority_server_id,
        metadata.term,
        metadata.config_epoch,
        metadata.policy.replication_factor,
        metadata.policy.max_steps_per_plan
    )
}

pub(super) fn format_cluster_management_status(status: &ClusterManagementStatus) -> String {
    format!(
        "{{\"routing_version\":{},\"metadata\":\"{}\",\"membership\":\"{}\",\"rebalance_plan\":\"{}\",\"rebalance_execution\":\"{}\",\"rebalance_automation\":\"{}\"}}",
        status.routing_version,
        escape_json_fragment(&format_cluster_metadata(&status.metadata)),
        escape_json_fragment(&format_cluster_membership(&status.membership)),
        escape_json_fragment(
            &status
                .rebalance_plan
                .as_ref()
                .map(format_rebalance_plan)
                .unwrap_or_else(|| "none".to_string())
        ),
        escape_json_fragment(
            &status
                .rebalance_execution
                .as_ref()
                .map(format_rebalance_execution)
                .unwrap_or_else(|| "none".to_string())
        ),
        escape_json_fragment(&format_rebalance_automation(
            &status.rebalance_automation
        ))
    )
}

pub(super) fn format_rebalance_automation(
    summary: &neo4r_db::RebalanceAutomationSummary,
) -> String {
    format!(
        "state={} pending={} running={} ready={} applied={} failed={} blocked_reason={}",
        summary.state,
        summary.pending_steps,
        summary.running_steps,
        summary.ready_steps,
        summary.applied_steps,
        summary.failed_steps,
        summary.blocked_reason
    )
}

pub(super) fn format_rebalance_plan(plan: &RebalancePlan) -> String {
    let steps = plan
        .steps
        .iter()
        .map(format_rebalance_step)
        .collect::<Vec<_>>()
        .join(",");
    format!(
        "plan_id={} state={} from_routing_version={} target_routing_version={} steps={} [{}]",
        plan.plan_id,
        format_rebalance_plan_state(plan.state),
        plan.from_routing_version,
        plan.target_routing_version,
        plan.steps.len(),
        steps
    )
}

pub(super) fn format_rebalance_execution(execution: &RebalanceExecution) -> String {
    let steps = execution
        .steps
        .iter()
        .map(|step| {
            format!(
                "{}:{}:attempts={}:retryable={}:error={}:{}",
                step.step_index,
                format_rebalance_step_state(step.state),
                step.attempts,
                step.retryable,
                step.last_error,
                format_rebalance_step(&step.step)
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    format!(
        "plan_id={} state={} current_step={} last_error={} steps=[{}]",
        execution.plan.plan_id,
        format_rebalance_plan_state(execution.state),
        execution.current_step,
        execution.last_error,
        steps
    )
}

pub(super) fn format_rebalance_plan_state(state: RebalancePlanState) -> &'static str {
    match state {
        RebalancePlanState::Proposed => "proposed",
        RebalancePlanState::Running => "running",
        RebalancePlanState::Completed => "completed",
        RebalancePlanState::Failed => "failed",
        RebalancePlanState::Cancelled => "cancelled",
    }
}

pub(super) fn format_rebalance_step_state(state: RebalanceStepState) -> &'static str {
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

pub(super) fn format_rebalance_step(step: &RebalanceStep) -> String {
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

pub(super) fn escape_json_fragment(input: &str) -> String {
    input
        .chars()
        .flat_map(|ch| match ch {
            '\\' => "\\\\".chars().collect::<Vec<_>>(),
            '"' => "\\\"".chars().collect::<Vec<_>>(),
            '\n' => "\\n".chars().collect::<Vec<_>>(),
            '\r' => "\\r".chars().collect::<Vec<_>>(),
            '\t' => "\\t".chars().collect::<Vec<_>>(),
            _ => vec![ch],
        })
        .collect()
}

pub(crate) fn format_query_plan(plan: &DistributedQueryPlan) -> String {
    format!(
        "route={} traversal={} boundary_cache={} access={} reason={} cost_model={} cost={} rows={} remote_shards={}",
        format_query_route(&plan.route),
        format_traversal_policy(&plan.traversal_policy),
        plan.uses_boundary_cache,
        format_access_plan(&plan.access_plan),
        plan.access_reason,
        plan.cost_model_version,
        plan.estimated_cost,
        plan.estimated_rows,
        plan.remote_shard_count
    )
}

pub(super) fn format_query_profile(profile: &QueryProfile) -> String {
    format!(
        "plan=[{}] operators=[{}] metrics=planning_us:{}:execution_us:{}:rows:{}:scanned_nodes:{}:scanned_relationships:{}:indexes:{}:cache_hits:{}:cache_misses:{}:index_cache_hits:{}:index_cache_misses:{}",
        format_query_plan(&profile.plan),
        profile
            .operators
            .iter()
            .map(format_query_operator_profile)
            .collect::<Vec<_>>()
            .join(","),
        profile.metrics.planning_elapsed_micros,
        profile.metrics.execution_elapsed_micros,
        profile.metrics.rows_returned,
        profile.metrics.scanned_nodes,
        profile.metrics.scanned_relationships,
        profile.metrics.index_count,
        profile.metrics.read_cache_hits,
        profile.metrics.read_cache_misses,
        profile.metrics.index_cache_hits,
        profile.metrics.index_cache_misses
    )
}

pub(super) fn format_query_operator_profile(profile: &QueryOperatorProfile) -> String {
    let children = profile
        .children
        .iter()
        .map(format_query_operator_profile)
        .collect::<Vec<_>>()
        .join("|");
    format!(
        "{}:estimated={}:actual={}:elapsed_us={}:children=[{}]",
        profile.name, profile.estimated_rows, profile.actual_rows, profile.elapsed_micros, children
    )
}

pub(super) fn format_storage_status(status: &StorageStatus) -> String {
    format!(
        "data_dir={} total_bytes={} files={} wal_segments={} checkpoints={} metadata_files={} committed=[{}] cache_hits={} cache_misses={} index_cache_hits={} index_cache_misses={} wal_pruned_until=[{}]",
        status.data_dir.display(),
        status.total_bytes,
        status.file_count,
        status.wal_segment_count,
        status.checkpoint_file_count,
        status.metadata_file_count,
        status
            .committed_indexes
            .iter()
            .map(|index| index.to_string())
            .collect::<Vec<_>>()
            .join(","),
        status.read_cache_hits,
        status.read_cache_misses,
        status.index_cache_hits,
        status.index_cache_misses,
        status
            .wal_pruned_until
            .iter()
            .map(|index| index.to_string())
            .collect::<Vec<_>>()
            .join(",")
    )
}

pub(super) fn format_statistics_catalog(statistics: &StatisticsCatalog) -> String {
    format!(
        "nodes={} relationships={} labels=[{}] node_properties=[{}] relationship_types=[{}] indexes={} vector_indexes={}",
        statistics.node_count,
        statistics.relationship_count,
        statistics
            .label_counts
            .iter()
            .map(|(label, count)| format!("{label}:{count}"))
            .collect::<Vec<_>>()
            .join(","),
        statistics
            .node_property_counts
            .iter()
            .map(|(property, count)| format!("{property}:{count}"))
            .collect::<Vec<_>>()
            .join(","),
        statistics
            .relationship_type_counts
            .iter()
            .map(|(rel_type, count)| format!("{rel_type}:{count}"))
            .collect::<Vec<_>>()
            .join(","),
        statistics.index_count,
        statistics.vector_index_count
    )
}

pub(super) fn format_storage_maintenance(result: &StorageMaintenanceResult) -> String {
    format!(
        "action={} files_touched={} bytes_observed={} pruned_until=[{}] safety_manifest={}",
        result.action,
        result.files_touched,
        result.bytes_observed,
        result
            .pruned_until
            .iter()
            .map(|index| index.to_string())
            .collect::<Vec<_>>()
            .join(","),
        result.safety_manifest
    )
}

pub(super) fn format_metadata_log(records: &[MetadataOperationRecord]) -> String {
    let entries = records
        .iter()
        .map(|record| {
            format!(
                "{}:term={}:epoch={}:op={}",
                record.index, record.term, record.config_epoch, record.operation
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    format!("count={} [{}]", records.len(), entries)
}

pub(super) fn format_query_route(route: &QueryRoute) -> String {
    match route {
        QueryRoute::LocalOnly => "local".to_string(),
        QueryRoute::RequiresRemoteShards(shards) => {
            format!("remote({})", format_shard_list(shards))
        }
    }
}

pub(super) fn format_traversal_policy(policy: &RemoteTraversalPolicy) -> String {
    match policy {
        RemoteTraversalPolicy::BoundaryCacheOnly => "boundary_cache".to_string(),
        RemoteTraversalPolicy::RemoteShardHop(shards) => {
            format!("remote_hop({})", format_shard_list(shards))
        }
    }
}

pub(super) fn format_access_plan(plan: &QueryAccessPlan) -> String {
    match plan {
        QueryAccessPlan::NodeUniqueIndexSeek { label, property } => {
            format!("node_unique_index_seek({label}.{property})")
        }
        QueryAccessPlan::NodeIndexSeek { label, property } => {
            format!("node_index_seek({label}.{property})")
        }
        QueryAccessPlan::NodeLabelScan { label } => format!("node_label_scan({label})"),
        QueryAccessPlan::NodeFullScan => "node_full_scan".to_string(),
        QueryAccessPlan::VectorIndexSeek {
            label,
            property,
            metric,
        } => {
            let label = label.as_deref().unwrap_or("*");
            format!("vector_index_seek({label}.{property},metric={metric})")
        }
        QueryAccessPlan::RelationshipTypeScan { rel_type } => {
            format!("relationship_type_scan({rel_type})")
        }
        QueryAccessPlan::RelationshipScan => "relationship_scan".to_string(),
        QueryAccessPlan::Unsupported { reason } => format!("unsupported({reason})"),
    }
}

pub(super) fn format_shard_list(shards: &[ShardId]) -> String {
    shards
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(",")
}

pub(super) fn format_vector_index_status(statuses: &[neo4r_db::VectorIndexStatus]) -> String {
    if statuses.is_empty() {
        return "none".to_string();
    }
    statuses
        .iter()
        .map(|status| {
            format!(
                "{}:{}:{}:dimensions={}:metric={}:entries={}",
                status.name,
                status.label,
                status.property,
                status.dimensions,
                status.metric,
                status.entries
            )
        })
        .collect::<Vec<_>>()
        .join(",")
}

pub(super) fn format_shard_status(status: &ShardStatus) -> String {
    let primary = status
        .primary_server_id
        .map(|server_id| server_id.to_string())
        .unwrap_or_else(|| "none".to_string());
    let replicas = status
        .replica_server_ids
        .iter()
        .map(|server_id| server_id.to_string())
        .collect::<Vec<_>>()
        .join("|");
    let matches = status
        .match_indexes
        .iter()
        .map(|(server_id, index)| format!("{server_id}:{index}"))
        .collect::<Vec<_>>()
        .join("|");
    format!(
        "shard={} primary={} replicas={} local={} local_primary={} applied={} committed={} match={}",
        status.shard_id,
        primary,
        replicas,
        status.has_local_copy,
        status.is_local_primary,
        status.applied_index,
        status.committed_index,
        matches
    )
}
