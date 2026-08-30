use super::*;

pub(in crate::database) fn parse_plan_u64(input: &str, name: &str) -> DatabaseResult<u64> {
    input
        .parse::<u64>()
        .map_err(|_| StorageError::CorruptStore(format!("invalid {name}")).into())
}

pub(in crate::database) fn parse_plan_usize(input: &str, name: &str) -> DatabaseResult<usize> {
    input
        .parse::<usize>()
        .map_err(|_| StorageError::CorruptStore(format!("invalid {name}")).into())
}

pub(in crate::database) fn parse_plan_bool(input: &str, name: &str) -> DatabaseResult<bool> {
    match input {
        "0" => Ok(false),
        "1" => Ok(true),
        _ => Err(StorageError::CorruptStore(format!("invalid {name}")).into()),
    }
}

pub(in crate::database) fn sanitize_cluster_text(input: &str) -> String {
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

pub(in crate::database) fn encode_cluster_bootstrap_mode(
    mode: ClusterBootstrapMode,
) -> &'static str {
    match mode {
        ClusterBootstrapMode::JoinExisting => "join_existing",
        ClusterBootstrapMode::RecoverFromData => "recover_from_data",
    }
}

pub(in crate::database) fn decode_cluster_bootstrap_mode(
    value: &str,
) -> DatabaseResult<ClusterBootstrapMode> {
    match value {
        "join_existing" => Ok(ClusterBootstrapMode::JoinExisting),
        "recover_from_data" => Ok(ClusterBootstrapMode::RecoverFromData),
        _ => Err(
            StorageError::CorruptStore(format!("unknown cluster bootstrap mode {value:?}")).into(),
        ),
    }
}

pub(in crate::database) fn encode_node_membership_state(
    state: NodeMembershipState,
) -> &'static str {
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

pub(in crate::database) fn decode_node_membership_state(
    value: &str,
) -> DatabaseResult<NodeMembershipState> {
    match value {
        "negotiating" => Ok(NodeMembershipState::Negotiating),
        "joining" => Ok(NodeMembershipState::Joining),
        "active" => Ok(NodeMembershipState::Active),
        "draining" => Ok(NodeMembershipState::Draining),
        "leaving" => Ok(NodeMembershipState::Leaving),
        "removed" => Ok(NodeMembershipState::Removed),
        "dead" => Ok(NodeMembershipState::Dead),
        "rejected" => Ok(NodeMembershipState::Rejected),
        _ => {
            Err(StorageError::CorruptStore(format!("unknown cluster node state {value:?}")).into())
        }
    }
}

pub(in crate::database) fn encode_shard_assignment_state(
    state: ShardAssignmentState,
) -> &'static str {
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

pub(in crate::database) fn decode_shard_assignment_state(
    value: &str,
) -> DatabaseResult<ShardAssignmentState> {
    match value {
        "planned" => Ok(ShardAssignmentState::Planned),
        "catching_up" => Ok(ShardAssignmentState::CatchingUp),
        "caught_up" => Ok(ShardAssignmentState::CaughtUp),
        "serving_replica" => Ok(ShardAssignmentState::ServingReplica),
        "promoting" => Ok(ShardAssignmentState::Promoting),
        "serving_primary" => Ok(ShardAssignmentState::ServingPrimary),
        "removing" => Ok(ShardAssignmentState::Removing),
        "removed" => Ok(ShardAssignmentState::Removed),
        _ => Err(StorageError::CorruptStore(format!(
            "unknown cluster shard assignment state {value:?}"
        ))
        .into()),
    }
}
