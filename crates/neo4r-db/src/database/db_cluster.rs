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

    pub fn unregister_replication_peer(&mut self, server_id: ServerId) -> DatabaseResult<()> {
        self.replicator.unregister_peer_address(server_id)
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

    pub fn plan_rebalance(&mut self) -> DatabaseResult<RebalancePlan> {
        self.ensure_metadata_authority()?;
        let mut steps = self.generate_rebalance_steps();
        if steps.len() > self.cluster_metadata.policy.max_steps_per_plan {
            steps.truncate(self.cluster_metadata.policy.max_steps_per_plan);
        }
        let plan = RebalancePlan {
            plan_id: self.rebalance_plan_store.next_plan_id()?,
            state: RebalancePlanState::Proposed,
            from_routing_version: self.routing_table.version,
            target_routing_version: self.routing_table.version + steps.len() as u64,
            steps,
        };
        self.rebalance_plan_store.save(&plan)?;
        self.append_metadata_operation("plan_rebalance")?;
        Ok(plan)
    }

    fn generate_rebalance_steps(&self) -> Vec<RebalanceStep> {
        let mut steps = Vec::new();
        let replication_factor = self.cluster_metadata.policy.replication_factor;
        let mut planned_replica_counts = self
            .routing_table
            .placements
            .iter()
            .map(|placement| (placement.shard_id, placement.replicas.len()))
            .collect::<BTreeMap<_, _>>();
        let joining = self
            .membership
            .nodes
            .iter()
            .filter(|node| node.state == NodeMembershipState::Joining)
            .map(|node| node.server_id)
            .collect::<Vec<_>>();
        for server_id in joining {
            for placement in &self.routing_table.placements {
                let replica_count = planned_replica_counts
                    .get(&placement.shard_id)
                    .copied()
                    .unwrap_or_default();
                if replica_count >= replication_factor {
                    continue;
                }
                if !placement.has_server(server_id)
                    && !self.has_active_assignment(placement.shard_id, server_id)
                {
                    steps.push(RebalanceStep::AddReplica {
                        shard_id: placement.shard_id,
                        server_id,
                    });
                    planned_replica_counts.insert(placement.shard_id, replica_count + 1);
                }
            }
        }

        for node in self
            .membership
            .nodes
            .iter()
            .filter(|node| node.state == NodeMembershipState::Draining)
        {
            for placement in &self.routing_table.placements {
                if placement.primary_server_id() == Some(node.server_id) {
                    if let Some(target) = placement
                        .replicas
                        .iter()
                        .find(|replica| replica.server_id != node.server_id)
                        .map(|replica| replica.server_id)
                    {
                        steps.push(RebalanceStep::TransferPrimary {
                            shard_id: placement.shard_id,
                            from: node.server_id,
                            to: target,
                        });
                    }
                } else if placement.has_server(node.server_id) {
                    steps.push(RebalanceStep::RemoveReplica {
                        shard_id: placement.shard_id,
                        server_id: node.server_id,
                    });
                }
            }
        }
        steps
    }

    pub fn start_rebalance_plan(&mut self) -> DatabaseResult<RebalanceExecution> {
        self.ensure_metadata_authority()?;
        if let Some(execution) = &self.rebalance_execution {
            if matches!(
                execution.state,
                RebalancePlanState::Proposed | RebalancePlanState::Running
            ) {
                return Ok(execution.clone());
            }
        }
        let mut plan = self.plan_rebalance()?;
        plan.state = RebalancePlanState::Running;
        self.rebalance_plan_store.save(&plan)?;
        let execution = RebalanceExecution {
            current_step: 0,
            steps: plan
                .steps
                .iter()
                .cloned()
                .enumerate()
                .map(|(step_index, step)| RebalanceStepExecution {
                    step_index,
                    step,
                    state: RebalanceStepState::Pending,
                    attempts: 0,
                    retryable: true,
                    last_error: String::new(),
                })
                .collect(),
            plan,
            state: RebalancePlanState::Running,
            last_error: String::new(),
        };
        self.rebalance_execution_store.save(&execution)?;
        self.rebalance_execution = Some(execution.clone());
        Ok(execution)
    }

    pub fn cancel_rebalance_plan(&mut self) -> DatabaseResult<RebalanceExecution> {
        self.ensure_metadata_authority()?;
        let mut execution = self.rebalance_execution.clone().ok_or_else(|| {
            DatabaseError::InvalidConfig("no active rebalance execution".to_string())
        })?;
        execution.state = RebalancePlanState::Cancelled;
        execution.plan.state = RebalancePlanState::Cancelled;
        for step in &mut execution.steps {
            if step.state != RebalanceStepState::Applied {
                step.state = RebalanceStepState::Cancelled;
            }
        }
        self.rebalance_plan_store.save(&execution.plan)?;
        self.rebalance_execution_store.save(&execution)?;
        self.rebalance_execution = Some(execution.clone());
        Ok(execution)
    }

    pub fn rebalance_status(&self) -> Option<&RebalanceExecution> {
        self.rebalance_execution.as_ref()
    }

    pub fn advance_rebalance(&mut self) -> DatabaseResult<RebalanceAdvanceResult> {
        self.ensure_metadata_authority()?;
        if self.rebalance_execution.is_none() {
            self.start_rebalance_plan()?;
        }
        let mut execution = self.rebalance_execution.clone().ok_or_else(|| {
            DatabaseError::InvalidConfig("no active rebalance execution".to_string())
        })?;
        if execution.state != RebalancePlanState::Running {
            return Ok(RebalanceAdvanceResult {
                execution,
                action: "idle".to_string(),
            });
        }
        let Some(step_index) = execution
            .steps
            .iter()
            .position(|step| step.state != RebalanceStepState::Applied)
        else {
            execution.state = RebalancePlanState::Completed;
            execution.plan.state = RebalancePlanState::Completed;
            self.rebalance_plan_store.save(&execution.plan)?;
            self.rebalance_execution_store.save(&execution)?;
            self.rebalance_execution = Some(execution.clone());
            return Ok(RebalanceAdvanceResult {
                execution,
                action: "completed".to_string(),
            });
        };
        execution.current_step = step_index;
        let step = execution.steps[step_index].step.clone();
        execution.steps[step_index].attempts =
            execution.steps[step_index].attempts.saturating_add(1);
        let result = self.advance_rebalance_step(&step, &mut execution.steps[step_index]);
        let action = match result {
            Ok(action) => {
                execution.last_error.clear();
                action
            }
            Err(err) => {
                execution.state = RebalancePlanState::Failed;
                execution.plan.state = RebalancePlanState::Failed;
                execution.last_error = sanitize_cluster_text(&err.to_string());
                execution.steps[step_index].state = RebalanceStepState::Failed;
                execution.steps[step_index].last_error = execution.last_error.clone();
                execution.steps[step_index].retryable = is_retryable_rebalance_error(&err);
                "failed".to_string()
            }
        };
        if execution
            .steps
            .iter()
            .all(|step| step.state == RebalanceStepState::Applied)
        {
            execution.state = RebalancePlanState::Completed;
            execution.plan.state = RebalancePlanState::Completed;
        }
        self.rebalance_plan_store.save(&execution.plan)?;
        self.rebalance_execution_store.save(&execution)?;
        self.rebalance_execution = Some(execution.clone());
        Ok(RebalanceAdvanceResult { execution, action })
    }

    pub fn cluster_management_status(&self) -> ClusterManagementStatus {
        ClusterManagementStatus {
            metadata: self.cluster_metadata.clone(),
            membership: self.membership.clone(),
            rebalance_plan: self.rebalance_plan_store.load().ok().flatten(),
            rebalance_execution: self.rebalance_execution.clone(),
            rebalance_automation: summarize_rebalance_automation(self.rebalance_execution.as_ref()),
            routing_version: self.routing_table.version,
        }
    }

    pub fn prepare_rebalance_step(
        &mut self,
        step: RebalanceStep,
    ) -> DatabaseResult<ClusterMembership> {
        self.ensure_metadata_authority()?;
        match step {
            RebalanceStep::AddReplica {
                shard_id,
                server_id,
            } => {
                self.ensure_joining_or_active_node(server_id)?;
                if self
                    .routing_table
                    .placement(shard_id)
                    .map(|placement| placement.has_server(server_id))
                    .unwrap_or(false)
                {
                    return Err(DatabaseError::InvalidConfig(format!(
                        "server {server_id} already serves shard {shard_id}"
                    )));
                }
                match self.assignment_mut(shard_id, server_id) {
                    Some(assignment) => {
                        assignment.state = ShardAssignmentState::CatchingUp;
                        assignment.match_index = 0;
                    }
                    None => self
                        .membership
                        .shard_assignments
                        .push(ClusterShardAssignment {
                            shard_id,
                            server_id,
                            state: ShardAssignmentState::CatchingUp,
                            match_index: 0,
                        }),
                }
            }
            RebalanceStep::TransferPrimary { shard_id, from, to } => {
                let placement = self.routing_table.placement(shard_id).ok_or_else(|| {
                    DatabaseError::InvalidConfig(format!("routing table missing shard {shard_id}"))
                })?;
                if placement.primary_server_id() != Some(from) || !placement.has_server(to) {
                    return Err(DatabaseError::InvalidConfig(
                        "primary transfer step does not match current placement".to_string(),
                    ));
                }
            }
            RebalanceStep::RemoveReplica {
                shard_id,
                server_id,
            } => {
                let placement = self.routing_table.placement(shard_id).ok_or_else(|| {
                    DatabaseError::InvalidConfig(format!("routing table missing shard {shard_id}"))
                })?;
                if !placement.has_server(server_id) {
                    return Err(DatabaseError::InvalidConfig(format!(
                        "server {server_id} does not serve shard {shard_id}"
                    )));
                }
            }
        }
        self.save_membership()?;
        Ok(self.membership.clone())
    }

    pub fn mark_shard_caught_up(
        &mut self,
        shard_id: ShardId,
        server_id: ServerId,
        match_index: LogIndex,
    ) -> DatabaseResult<ClusterMembership> {
        self.ensure_metadata_authority()?;
        let Some(assignment) = self.assignment_mut(shard_id, server_id) else {
            return Err(DatabaseError::InvalidConfig(format!(
                "shard {shard_id} server {server_id} has no prepared assignment"
            )));
        };
        if !matches!(
            assignment.state,
            ShardAssignmentState::Planned
                | ShardAssignmentState::CatchingUp
                | ShardAssignmentState::CaughtUp
        ) {
            return Err(DatabaseError::InvalidConfig(format!(
                "shard {shard_id} server {server_id} assignment is not catch-up eligible"
            )));
        }
        assignment.state = ShardAssignmentState::CaughtUp;
        assignment.match_index = match_index;
        self.save_membership()?;
        Ok(self.membership.clone())
    }

    pub fn apply_rebalance_step(
        &mut self,
        step: RebalanceStep,
    ) -> DatabaseResult<ShardRoutingTable> {
        self.ensure_metadata_authority()?;
        let mut routing_table = self.routing_table.clone();
        routing_table.version = routing_table.version.saturating_add(1);
        match step {
            RebalanceStep::AddReplica {
                shard_id,
                server_id,
            } => {
                self.ensure_assignment_caught_up(shard_id, server_id)?;
                let placement = mutable_placement(&mut routing_table, shard_id)?;
                if !placement.has_server(server_id) {
                    placement.replicas.push(ShardReplica::replica(server_id));
                }
            }
            RebalanceStep::TransferPrimary { shard_id, from, to } => {
                self.ensure_primary_transfer_target_ready(shard_id, to)?;
                let placement = mutable_placement(&mut routing_table, shard_id)?;
                if placement.primary_server_id() != Some(from) {
                    return Err(DatabaseError::InvalidConfig(format!(
                        "shard {shard_id} primary is not server {from}"
                    )));
                }
                if !placement.has_server(to) {
                    return Err(DatabaseError::InvalidConfig(format!(
                        "server {to} is not a replica of shard {shard_id}"
                    )));
                }
                for replica in &mut placement.replicas {
                    replica.role = if replica.server_id == to {
                        ShardRole::Primary
                    } else {
                        ShardRole::Replica
                    };
                }
            }
            RebalanceStep::RemoveReplica {
                shard_id,
                server_id,
            } => {
                let placement = mutable_placement(&mut routing_table, shard_id)?;
                if !placement.has_server(server_id) {
                    return Err(DatabaseError::InvalidConfig(format!(
                        "server {server_id} does not serve shard {shard_id}"
                    )));
                }
                if placement.primary_server_id() == Some(server_id) {
                    return Err(DatabaseError::InvalidConfig(format!(
                        "cannot remove primary server {server_id} from shard {shard_id}"
                    )));
                }
                if placement.replicas.len() <= 1 {
                    return Err(DatabaseError::InvalidConfig(format!(
                        "cannot remove the last replica from shard {shard_id}"
                    )));
                }
                placement
                    .replicas
                    .retain(|replica| replica.server_id != server_id);
            }
        }
        self.install_routing_table(routing_table.clone())?;
        self.finish_applied_assignments(&step)?;
        self.finish_drained_nodes()?;
        Ok(routing_table)
    }

    fn advance_rebalance_step(
        &mut self,
        step: &RebalanceStep,
        execution_step: &mut RebalanceStepExecution,
    ) -> DatabaseResult<String> {
        match step {
            RebalanceStep::AddReplica {
                shard_id,
                server_id,
            } => match execution_step.state {
                RebalanceStepState::Pending
                | RebalanceStepState::Preparing
                | RebalanceStepState::Failed => {
                    execution_step.state = RebalanceStepState::Preparing;
                    self.prepare_rebalance_step(step.clone())?;
                    execution_step.state = RebalanceStepState::CatchingUp;
                    Ok("prepared".to_string())
                }
                RebalanceStepState::CatchingUp => {
                    let committed_index = self.committed_index(*shard_id)?;
                    let observed_index = self.observed_match_index(*shard_id, *server_id);
                    if observed_index >= committed_index {
                        self.mark_shard_caught_up(*shard_id, *server_id, observed_index)?;
                        execution_step.state = RebalanceStepState::Ready;
                        Ok("caught_up".to_string())
                    } else {
                        Ok(format!(
                            "waiting_for_catch_up shard={shard_id} server={server_id} match_index={observed_index} committed_index={committed_index}"
                        ))
                    }
                }
                RebalanceStepState::Ready | RebalanceStepState::Applying => {
                    execution_step.state = RebalanceStepState::Applying;
                    self.apply_rebalance_step(step.clone())?;
                    execution_step.state = RebalanceStepState::Applied;
                    Ok("applied".to_string())
                }
                RebalanceStepState::Applied => Ok("already_applied".to_string()),
                RebalanceStepState::Cancelled => Ok("cancelled".to_string()),
            },
            RebalanceStep::TransferPrimary { .. } | RebalanceStep::RemoveReplica { .. } => {
                match execution_step.state {
                    RebalanceStepState::Pending
                    | RebalanceStepState::Preparing
                    | RebalanceStepState::Ready
                    | RebalanceStepState::Applying
                    | RebalanceStepState::Failed => {
                        execution_step.state = RebalanceStepState::Applying;
                        self.apply_rebalance_step(step.clone())?;
                        execution_step.state = RebalanceStepState::Applied;
                        Ok("applied".to_string())
                    }
                    RebalanceStepState::Applied => Ok("already_applied".to_string()),
                    RebalanceStepState::CatchingUp => Ok("ready".to_string()),
                    RebalanceStepState::Cancelled => Ok("cancelled".to_string()),
                }
            }
        }
    }

    fn finish_applied_assignments(&mut self, step: &RebalanceStep) -> DatabaseResult<()> {
        match step {
            RebalanceStep::AddReplica {
                shard_id,
                server_id,
            } => {
                if let Some(assignment) = self.assignment_mut(*shard_id, *server_id) {
                    assignment.state = ShardAssignmentState::ServingReplica;
                }
            }
            RebalanceStep::TransferPrimary { shard_id, to, .. } => {
                if let Some(assignment) = self.assignment_mut(*shard_id, *to) {
                    assignment.state = ShardAssignmentState::ServingPrimary;
                }
            }
            RebalanceStep::RemoveReplica {
                shard_id,
                server_id,
            } => {
                if let Some(assignment) = self.assignment_mut(*shard_id, *server_id) {
                    assignment.state = ShardAssignmentState::Removed;
                }
            }
        }
        self.save_membership()
    }

    fn finish_drained_nodes(&mut self) -> DatabaseResult<()> {
        let mut changed = false;
        for node in &mut self.membership.nodes {
            if node.state == NodeMembershipState::Joining
                && self
                    .routing_table
                    .placements
                    .iter()
                    .any(|placement| placement.has_server(node.server_id))
            {
                node.state = NodeMembershipState::Active;
                changed = true;
            }
            if node.state == NodeMembershipState::Draining
                && !self
                    .routing_table
                    .placements
                    .iter()
                    .any(|placement| placement.has_server(node.server_id))
            {
                node.state = NodeMembershipState::Removed;
                changed = true;
            }
        }
        if changed {
            self.save_membership()?;
        }
        Ok(())
    }

    fn join_rejection_reason(&self, server_id: ServerId, shard_count: u64) -> String {
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

    fn ensure_joining_or_active_node(&self, server_id: ServerId) -> DatabaseResult<()> {
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

    fn ensure_metadata_authority(&self) -> DatabaseResult<()> {
        if self.cluster_metadata.authority_server_id != self.config.server_id {
            return Err(DatabaseError::InvalidConfig(format!(
                "server {} is not metadata authority {}; forward cluster metadata changes to the authority",
                self.config.server_id, self.cluster_metadata.authority_server_id
            )));
        }
        Ok(())
    }

    fn ensure_write_epoch(&self) -> DatabaseResult<()> {
        if self.cluster_metadata.config_epoch != self.routing_table.version {
            return Err(DatabaseError::InvalidConfig(format!(
                "stale write epoch: metadata epoch {}, routing version {}",
                self.cluster_metadata.config_epoch, self.routing_table.version
            )));
        }
        Ok(())
    }

    fn append_metadata_operation(
        &self,
        operation: &str,
    ) -> DatabaseResult<MetadataOperationRecord> {
        self.metadata_log_store.append(
            self.cluster_metadata.term,
            self.cluster_metadata.config_epoch,
            operation,
        )
    }

    fn committed_index(&self, shard_id: ShardId) -> DatabaseResult<LogIndex> {
        self.commit_indexes
            .get(shard_id as usize)
            .copied()
            .ok_or_else(|| {
                DatabaseError::InvalidConfig(format!("routing table missing shard {shard_id}"))
            })
    }

    fn observed_match_index(&self, shard_id: ShardId, server_id: ServerId) -> LogIndex {
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

    fn ensure_assignment_caught_up(
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

    fn ensure_primary_transfer_target_ready(
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

    fn ensure_assignment_match_index(
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

    fn has_active_assignment(&self, shard_id: ShardId, server_id: ServerId) -> bool {
        self.assignment(shard_id, server_id)
            .map(|assignment| {
                !matches!(
                    assignment.state,
                    ShardAssignmentState::Removed | ShardAssignmentState::Removing
                )
            })
            .unwrap_or(false)
    }

    fn assignment(
        &self,
        shard_id: ShardId,
        server_id: ServerId,
    ) -> Option<&ClusterShardAssignment> {
        self.membership
            .shard_assignments
            .iter()
            .find(|assignment| assignment.shard_id == shard_id && assignment.server_id == server_id)
    }

    fn assignment_mut(
        &mut self,
        shard_id: ShardId,
        server_id: ServerId,
    ) -> Option<&mut ClusterShardAssignment> {
        self.membership
            .shard_assignments
            .iter_mut()
            .find(|assignment| assignment.shard_id == shard_id && assignment.server_id == server_id)
    }

    fn save_membership(&mut self) -> DatabaseResult<()> {
        self.membership.version = self.membership.version.saturating_add(1);
        self.membership.nodes.sort_by_key(|node| node.server_id);
        self.membership
            .shard_assignments
            .sort_by_key(|assignment| (assignment.shard_id, assignment.server_id));
        self.membership_store.save(&self.membership)?;
        Ok(())
    }

}
