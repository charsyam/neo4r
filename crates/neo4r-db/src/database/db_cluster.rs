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

    pub fn plan_node_catch_up(&self, server_id: ServerId) -> DatabaseResult<NodeCatchUpPlan> {
        let Some(node) = self
            .membership
            .nodes
            .iter()
            .find(|node| node.server_id == server_id)
        else {
            return Err(DatabaseError::InvalidConfig(format!(
                "cluster node {server_id} does not exist"
            )));
        };
        if matches!(
            node.state,
            NodeMembershipState::Rejected
                | NodeMembershipState::Removed
                | NodeMembershipState::Dead
                | NodeMembershipState::Draining
        ) {
            return Err(DatabaseError::InvalidConfig(format!(
                "cluster node {server_id} is not catch-up eligible"
            )));
        }

        let mut sources = Vec::new();
        for assignment in self
            .membership
            .shard_assignments
            .iter()
            .filter(|assignment| assignment.server_id == server_id)
        {
            if !matches!(
                assignment.state,
                ShardAssignmentState::Planned
                    | ShardAssignmentState::CatchingUp
                    | ShardAssignmentState::CaughtUp
            ) {
                continue;
            }
            let placement = self
                .routing_table
                .placement(assignment.shard_id)
                .ok_or_else(|| {
                    DatabaseError::InvalidConfig(format!(
                        "routing table missing shard {}",
                        assignment.shard_id
                    ))
                })?;
            let primary_server_id = placement.primary_server_id().ok_or_else(|| {
                DatabaseError::InvalidConfig(format!(
                    "routing table missing primary for shard {}",
                    assignment.shard_id
                ))
            })?;
            let primary_address = self
                .membership
                .nodes
                .iter()
                .find(|node| node.server_id == primary_server_id)
                .map(|node| node.address.clone())
                .unwrap_or_default();
            let target_index = self.committed_index(assignment.shard_id)?;
            let snapshot_required = assignment.match_index == 0 && target_index > 0;
            let start_index = if snapshot_required {
                1
            } else {
                assignment.match_index.saturating_add(1)
            };
            sources.push(NodeCatchUpSource {
                shard_id: assignment.shard_id,
                primary_server_id,
                primary_address,
                snapshot_required,
                start_index,
                target_index,
                current_match_index: assignment.match_index,
            });
        }
        sources.sort_by_key(|source| source.shard_id);
        let ready_to_promote = !sources.is_empty()
            && sources
                .iter()
                .all(|source| source.current_match_index >= source.target_index);
        Ok(NodeCatchUpPlan {
            server_id,
            routing_version: self.routing_table.version,
            metadata_term: self.cluster_metadata.term,
            sources,
            ready_to_promote,
        })
    }

    pub fn build_cluster_bootstrap_manifest(
        &self,
        mode: ClusterBootstrapMode,
        cluster_id: impl Into<String>,
        database_id: impl Into<String>,
    ) -> DatabaseResult<ClusterBootstrapManifest> {
        let mut shards = Vec::new();
        for shard_id in 0..self.shard_map.shard_count() {
            let snapshot_store =
                neo4r_storage::SnapshotStore::open(&self.config.data_dir, shard_id)?;
            let snapshot = snapshot_store.load()?;
            let checksum = snapshot_payload_checksum(&snapshot_store)?;
            shards.push(ClusterBootstrapShard {
                shard_id,
                commit_index: self.committed_index(shard_id)?,
                snapshot_index: snapshot
                    .as_ref()
                    .map(|snapshot| snapshot.last_included_index)
                    .unwrap_or_default(),
                snapshot_term: snapshot
                    .as_ref()
                    .map(|snapshot| snapshot.last_included_term)
                    .unwrap_or_default(),
                snapshot_checksum: checksum,
            });
        }
        Ok(ClusterBootstrapManifest {
            format_version: 1,
            mode,
            cluster_id: cluster_id.into(),
            database_id: database_id.into(),
            seed_server_id: self.config.server_id,
            shard_count: self.shard_map.shard_count(),
            routing_version: self.routing_table.version,
            metadata_term: self.cluster_metadata.term,
            config_epoch: self.cluster_metadata.config_epoch,
            force_new_cluster_required: mode == ClusterBootstrapMode::RecoverFromData,
            shards,
            membership: self.membership.clone(),
        })
    }

    pub fn write_cluster_bootstrap_manifest(
        &self,
        mode: ClusterBootstrapMode,
        cluster_id: impl Into<String>,
        database_id: impl Into<String>,
    ) -> DatabaseResult<ClusterBootstrapManifest> {
        let manifest = self.build_cluster_bootstrap_manifest(mode, cluster_id, database_id)?;
        self.validate_cluster_bootstrap_manifest(&manifest)?;
        self.bootstrap_manifest_store.save(&manifest)?;
        Ok(manifest)
    }

    pub fn load_cluster_bootstrap_manifest(
        &self,
    ) -> DatabaseResult<Option<ClusterBootstrapManifest>> {
        self.bootstrap_manifest_store.load()
    }

    pub fn validate_cluster_bootstrap_manifest(
        &self,
        manifest: &ClusterBootstrapManifest,
    ) -> DatabaseResult<()> {
        if manifest.format_version != 1 {
            return Err(DatabaseError::InvalidConfig(format!(
                "unsupported cluster bootstrap manifest version {}",
                manifest.format_version
            )));
        }
        if manifest.cluster_id.trim().is_empty() {
            return Err(DatabaseError::InvalidConfig(
                "cluster bootstrap manifest cluster_id must not be empty".to_string(),
            ));
        }
        if manifest.database_id.trim().is_empty() {
            return Err(DatabaseError::InvalidConfig(
                "cluster bootstrap manifest database_id must not be empty".to_string(),
            ));
        }
        if manifest.shard_count != self.shard_map.shard_count() {
            return Err(DatabaseError::InvalidConfig(format!(
                "cluster bootstrap manifest shard count {} does not match local shard count {}",
                manifest.shard_count,
                self.shard_map.shard_count()
            )));
        }
        if manifest.routing_version != self.routing_table.version {
            return Err(DatabaseError::InvalidConfig(format!(
                "cluster bootstrap manifest routing version {} does not match local routing version {}",
                manifest.routing_version, self.routing_table.version
            )));
        }
        if manifest.config_epoch != self.cluster_metadata.config_epoch {
            return Err(DatabaseError::InvalidConfig(format!(
                "cluster bootstrap manifest config epoch {} does not match local config epoch {}",
                manifest.config_epoch, self.cluster_metadata.config_epoch
            )));
        }
        if manifest.force_new_cluster_required
            != (manifest.mode == ClusterBootstrapMode::RecoverFromData)
        {
            return Err(DatabaseError::InvalidConfig(
                "cluster bootstrap manifest force flag does not match mode".to_string(),
            ));
        }
        if manifest.shards.len() != self.shard_map.shard_count() as usize {
            return Err(DatabaseError::InvalidConfig(
                "cluster bootstrap manifest must include every shard".to_string(),
            ));
        }
        let mut seen = BTreeSet::new();
        for shard in &manifest.shards {
            if shard.shard_id >= self.shard_map.shard_count() || !seen.insert(shard.shard_id) {
                return Err(DatabaseError::InvalidConfig(format!(
                    "cluster bootstrap manifest has invalid shard {}",
                    shard.shard_id
                )));
            }
            let local_commit = self.committed_index(shard.shard_id)?;
            if shard.commit_index != local_commit {
                return Err(DatabaseError::InvalidConfig(format!(
                    "cluster bootstrap manifest shard {} commit {} does not match local commit {}",
                    shard.shard_id, shard.commit_index, local_commit
                )));
            }
            let snapshot_store =
                neo4r_storage::SnapshotStore::open(&self.config.data_dir, shard.shard_id)?;
            let snapshot = snapshot_store.load()?;
            let local_snapshot_index = snapshot
                .as_ref()
                .map(|snapshot| snapshot.last_included_index)
                .unwrap_or_default();
            let local_snapshot_term = snapshot
                .as_ref()
                .map(|snapshot| snapshot.last_included_term)
                .unwrap_or_default();
            let local_checksum = snapshot_payload_checksum(&snapshot_store)?;
            if shard.snapshot_index != local_snapshot_index
                || shard.snapshot_term != local_snapshot_term
                || shard.snapshot_checksum != local_checksum
            {
                return Err(DatabaseError::InvalidConfig(format!(
                    "cluster bootstrap manifest shard {} snapshot metadata does not match local data",
                    shard.shard_id
                )));
            }
            if shard.snapshot_index > shard.commit_index {
                return Err(DatabaseError::InvalidConfig(format!(
                    "cluster bootstrap manifest shard {} snapshot is ahead of commit",
                    shard.shard_id
                )));
            }
        }
        Ok(())
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
