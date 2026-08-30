use super::metadata_types::*;
use super::*;

mod metadata;
mod rebalance;

impl Neo4rDatabase {
    pub fn execute_node_catch_up_plan(
        &mut self,
        plan: &NodeCatchUpPlan,
        source: &mut impl NodeCatchUpDataSource,
        max_entries_per_request: Option<usize>,
    ) -> DatabaseResult<NodeCatchUpExecution> {
        if plan.routing_version != self.routing_table.version {
            return Err(DatabaseError::Replication(format!(
                "catch-up plan routing version {} does not match local routing version {}",
                plan.routing_version, self.routing_table.version
            )));
        }
        if max_entries_per_request == Some(0) {
            return Err(DatabaseError::Replication(
                "catch-up max entries must be greater than zero".to_string(),
            ));
        }
        let mut installed_snapshots = 0;
        let mut replayed_entries = 0;
        let mut shard_results = Vec::new();
        for plan_source in &plan.sources {
            let mut replay_start_index = plan_source.start_index;
            let mut snapshot_installed = false;
            if plan_source.snapshot_required {
                if let Some(snapshot) = source.install_snapshot_request(plan_source)? {
                    self.install_catch_up_snapshot(snapshot)?;
                    installed_snapshots += 1;
                    snapshot_installed = true;
                    replay_start_index = plan_source.start_index.max(
                        plan_source
                            .target_index
                            .min(plan_source.current_match_index)
                            .saturating_add(1),
                    );
                }
            }
            let mut next_index = replay_start_index;
            let mut shard_replayed = 0;
            while next_index <= plan_source.target_index {
                let entries =
                    source.log_entries(plan_source, next_index, max_entries_per_request)?;
                if entries.is_empty() {
                    break;
                }
                let count = entries.len();
                let last_index = entries
                    .last()
                    .map(|entry| entry.index)
                    .unwrap_or(next_index);
                self.apply_replicated_entries(entries)?;
                shard_replayed += count;
                replayed_entries += count;
                next_index = last_index.saturating_add(1);
                if max_entries_per_request.is_some_and(|max| count < max) {
                    break;
                }
            }
            let match_index = self.committed_index(plan_source.shard_id)?;
            shard_results.push(NodeCatchUpShardExecution {
                shard_id: plan_source.shard_id,
                snapshot_installed,
                replay_start_index,
                replay_end_index: next_index.saturating_sub(1),
                replayed_entries: shard_replayed,
                match_index,
            });
        }
        let ready_to_promote = !shard_results.is_empty()
            && shard_results
                .iter()
                .zip(plan.sources.iter())
                .all(|(result, source)| result.match_index >= source.target_index);
        Ok(NodeCatchUpExecution {
            server_id: plan.server_id,
            installed_snapshots,
            replayed_entries,
            shard_results,
            ready_to_promote,
        })
    }

    fn install_catch_up_snapshot(&mut self, request: InstallSnapshotRequest) -> DatabaseResult<()> {
        if self.raft_groups.is_some() {
            let response = self.install_raft_snapshot(request)?;
            if !response.success {
                return Err(DatabaseError::Replication(format!(
                    "snapshot install rejected at index {}",
                    response.last_included_index
                )));
            }
            return Ok(());
        }
        let metadata = request.metadata;
        if !request.payload.is_empty() {
            let snapshot_store =
                neo4r_storage::SnapshotStore::open(&self.config.data_dir, metadata.shard_id)?;
            snapshot_store.save_payload(&request.payload)?;
            if let Some(snapshot) = snapshot_store.load()? {
                self.apply_loaded_snapshot(metadata.shard_id, &snapshot)?;
            }
        }
        self.install_raft_snapshot_metadata(metadata)?;
        Ok(())
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

    pub fn bootstrap_safety_decision(
        &self,
        manifest: &ClusterBootstrapManifest,
        expected_cluster_id: &str,
        force_new_cluster: bool,
    ) -> BootstrapSafetyDecision {
        if manifest.cluster_id != expected_cluster_id {
            return BootstrapSafetyDecision {
                allowed: false,
                mode: manifest.mode,
                requires_force_new_cluster: manifest.force_new_cluster_required,
                expected_cluster_id: expected_cluster_id.to_string(),
                observed_cluster_id: manifest.cluster_id.clone(),
                reason: "cluster id mismatch".to_string(),
            };
        }
        if manifest.force_new_cluster_required && !force_new_cluster {
            return BootstrapSafetyDecision {
                allowed: false,
                mode: manifest.mode,
                requires_force_new_cluster: true,
                expected_cluster_id: expected_cluster_id.to_string(),
                observed_cluster_id: manifest.cluster_id.clone(),
                reason: "force-new-cluster confirmation required".to_string(),
            };
        }
        let validation = self.validate_cluster_bootstrap_manifest(manifest);
        BootstrapSafetyDecision {
            allowed: validation.is_ok(),
            mode: manifest.mode,
            requires_force_new_cluster: manifest.force_new_cluster_required,
            expected_cluster_id: expected_cluster_id.to_string(),
            observed_cluster_id: manifest.cluster_id.clone(),
            reason: validation
                .err()
                .map(|err| err.to_string())
                .unwrap_or_else(|| "ok".to_string()),
        }
    }

    pub fn backup_bootstrap_link(
        &self,
        backup_manifest_path: impl Into<PathBuf>,
        manifest: &ClusterBootstrapManifest,
    ) -> DatabaseResult<BackupBootstrapLink> {
        self.validate_cluster_bootstrap_manifest(manifest)?;
        let backup_manifest_path = backup_manifest_path.into();
        Ok(BackupBootstrapLink {
            backup_manifest_path: backup_manifest_path.clone(),
            bootstrap_cluster_id: manifest.cluster_id.clone(),
            database_id: manifest.database_id.clone(),
            shard_count: manifest.shard_count,
            safe_to_seed: backup_manifest_path.exists() && !manifest.shards.is_empty(),
        })
    }

    pub fn topology_observation(&self) -> TopologyObservation {
        let joining_nodes = self
            .membership
            .nodes
            .iter()
            .filter(|node| node.state == NodeMembershipState::Joining)
            .count();
        let draining_nodes = self
            .membership
            .nodes
            .iter()
            .filter(|node| node.state == NodeMembershipState::Draining)
            .count();
        let catching_up_assignments = self
            .membership
            .shard_assignments
            .iter()
            .filter(|assignment| assignment.state == ShardAssignmentState::CatchingUp)
            .count();
        let caught_up_assignments = self
            .membership
            .shard_assignments
            .iter()
            .filter(|assignment| assignment.state == ShardAssignmentState::CaughtUp)
            .count();
        let recommended_action = if catching_up_assignments > 0 {
            "execute_catch_up".to_string()
        } else if caught_up_assignments > 0 || joining_nodes > 0 || draining_nodes > 0 {
            "advance_rebalance".to_string()
        } else {
            "idle".to_string()
        };
        TopologyObservation {
            joining_nodes,
            catching_up_assignments,
            caught_up_assignments,
            draining_nodes,
            recommended_action,
        }
    }

    pub fn operational_safety_decision(
        &self,
        operation: &str,
        supplied_confirmation: Option<&str>,
    ) -> OperationalSafetyDecision {
        let token = format!(
            "{}:{}:{}",
            sanitize_cluster_text(operation),
            self.cluster_metadata.config_epoch,
            self.routing_table.version
        );
        let dangerous = matches!(
            operation,
            "recover_from_data"
                | "force_new_cluster"
                | "decommission_node"
                | "apply_gc"
                | "restore_snapshot"
                | "restore_pitr"
                | "token_revoke_all"
                | "rbac_grant"
                | "rbac_revoke"
        );
        let allowed = !dangerous || supplied_confirmation == Some(token.as_str());
        OperationalSafetyDecision {
            allowed,
            confirmation_required: dangerous,
            confirmation_token: token,
            reason: if allowed {
                "ok".to_string()
            } else {
                "confirmation token required".to_string()
            },
        }
    }

    pub fn chaos_checks_for_join_catch_up(&self) -> Vec<ClusterChaosCheck> {
        let topology = self.topology_observation();
        vec![
            ClusterChaosCheck {
                scenario: "join_during_leader_restart".to_string(),
                passed: self.cluster_metadata.config_epoch >= self.routing_table.version,
                checked_invariant: "config_epoch_not_behind_routing_version".to_string(),
            },
            ClusterChaosCheck {
                scenario: "snapshot_transfer_retry".to_string(),
                passed: self.membership.shard_assignments.iter().all(|assignment| {
                    assignment.match_index
                        <= self
                            .committed_index(assignment.shard_id)
                            .unwrap_or_default()
                }),
                checked_invariant: "assignment_match_index_not_ahead_of_commit".to_string(),
            },
            ClusterChaosCheck {
                scenario: "rebalance_control_loop".to_string(),
                passed: topology.recommended_action != "execute_catch_up"
                    || topology.catching_up_assignments > 0,
                checked_invariant: "topology_recommendation_matches_membership".to_string(),
            },
        ]
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
