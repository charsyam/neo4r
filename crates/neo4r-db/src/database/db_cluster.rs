use super::metadata_types::*;
use super::*;

mod metadata;
mod rebalance;

impl Neo4rDatabase {
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
        self.append_replicated_config_change_phase(
            "enter_joint",
            "install_routing_table",
            &routing_table,
        )?;
        self.begin_joint_consensus_for_routing(&routing_table)?;
        self.append_replicated_config_change_phase(
            "install",
            "install_routing_table",
            &routing_table,
        )?;
        self.shard_metadata.save(&routing_table)?;
        self.replicator
            .install_routing_table(routing_table.clone())?;
        self.cluster_metadata.config_epoch = routing_table.version;
        self.cluster_metadata_store.save(&self.cluster_metadata)?;
        self.append_metadata_operation("install_routing_table")?;
        self.routing_table = routing_table;
        self.rebuild_raft_groups()?;
        let routing_table = self.routing_table.clone();
        self.append_replicated_config_change_phase(
            "finalize_joint",
            "install_routing_table",
            &routing_table,
        )?;
        self.finalize_joint_consensus_for_routing(&routing_table)?;
        Ok(())
    }

    pub fn register_replication_peer(
        &mut self,
        server_id: ServerId,
        address: String,
    ) -> DatabaseResult<()> {
        self.replicator.register_peer_address(server_id, address)
    }

    pub fn register_replication_peer_endpoint(
        &mut self,
        server_id: ServerId,
        endpoint: ReplicationEndpoint,
    ) -> DatabaseResult<()> {
        if endpoint.address.trim().is_empty() {
            return Err(DatabaseError::InvalidConfig(
                "replication endpoint address must not be empty".to_string(),
            ));
        }
        self.replicator.register_peer_endpoint(server_id, endpoint)
    }

    pub fn unregister_replication_peer(&mut self, server_id: ServerId) -> DatabaseResult<()> {
        self.replicator.unregister_peer_address(server_id)
    }

    pub fn replication_channel_metrics(&self) -> Option<ReplicationChannelMetricsSnapshot> {
        self.replicator.channel_metrics_snapshot()
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
}
