use super::*;

impl Neo4rDatabase {
    pub(in crate::database) fn join_rejection_reason(
        &self,
        server_id: ServerId,
        shard_count: u64,
    ) -> String {
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

    pub(in crate::database) fn ensure_joining_or_active_node(
        &self,
        server_id: ServerId,
    ) -> DatabaseResult<()> {
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

    pub(in crate::database) fn ensure_metadata_authority(&self) -> DatabaseResult<()> {
        if self.cluster_metadata.authority_server_id != self.config.server_id {
            return Err(DatabaseError::InvalidConfig(format!(
                "server {} is not metadata authority {}; forward cluster metadata changes to the authority",
                self.config.server_id, self.cluster_metadata.authority_server_id
            )));
        }
        Ok(())
    }

    pub(in crate::database) fn ensure_write_epoch(&self) -> DatabaseResult<()> {
        if self.cluster_metadata.config_epoch != self.routing_table.version {
            return Err(DatabaseError::InvalidConfig(format!(
                "stale write epoch: metadata epoch {}, routing version {}",
                self.cluster_metadata.config_epoch, self.routing_table.version
            )));
        }
        Ok(())
    }

    pub(in crate::database) fn append_metadata_operation(
        &self,
        operation: &str,
    ) -> DatabaseResult<MetadataOperationRecord> {
        self.metadata_log_store.append(
            self.cluster_metadata.term,
            self.cluster_metadata.config_epoch,
            operation,
        )
    }

    pub(in crate::database) fn committed_index(
        &self,
        shard_id: ShardId,
    ) -> DatabaseResult<LogIndex> {
        self.commit_indexes
            .get(shard_id as usize)
            .copied()
            .ok_or_else(|| {
                DatabaseError::InvalidConfig(format!("routing table missing shard {shard_id}"))
            })
    }

    pub(in crate::database) fn observed_match_index(
        &self,
        shard_id: ShardId,
        server_id: ServerId,
    ) -> LogIndex {
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

    pub(in crate::database) fn ensure_assignment_caught_up(
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

    pub(in crate::database) fn ensure_primary_transfer_target_ready(
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

    pub(in crate::database) fn ensure_assignment_match_index(
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

    pub(in crate::database) fn has_active_assignment(
        &self,
        shard_id: ShardId,
        server_id: ServerId,
    ) -> bool {
        self.assignment(shard_id, server_id)
            .map(|assignment| {
                !matches!(
                    assignment.state,
                    ShardAssignmentState::Removed | ShardAssignmentState::Removing
                )
            })
            .unwrap_or(false)
    }

    pub(in crate::database) fn assignment(
        &self,
        shard_id: ShardId,
        server_id: ServerId,
    ) -> Option<&ClusterShardAssignment> {
        self.membership
            .shard_assignments
            .iter()
            .find(|assignment| assignment.shard_id == shard_id && assignment.server_id == server_id)
    }

    pub(in crate::database) fn assignment_mut(
        &mut self,
        shard_id: ShardId,
        server_id: ServerId,
    ) -> Option<&mut ClusterShardAssignment> {
        self.membership
            .shard_assignments
            .iter_mut()
            .find(|assignment| assignment.shard_id == shard_id && assignment.server_id == server_id)
    }

    pub(in crate::database) fn save_membership(&mut self) -> DatabaseResult<()> {
        self.membership.version = self.membership.version.saturating_add(1);
        self.membership.nodes.sort_by_key(|node| node.server_id);
        self.membership
            .shard_assignments
            .sort_by_key(|assignment| (assignment.shard_id, assignment.server_id));
        self.membership_store.save(&self.membership)?;
        Ok(())
    }
}
