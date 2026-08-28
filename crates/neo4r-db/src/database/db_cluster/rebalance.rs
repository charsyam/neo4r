use super::*;

impl Neo4rDatabase {
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

    pub(in crate::database) fn generate_rebalance_steps(&self) -> Vec<RebalanceStep> {
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

    pub(in crate::database) fn advance_rebalance_step(
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

    pub(in crate::database) fn finish_applied_assignments(
        &mut self,
        step: &RebalanceStep,
    ) -> DatabaseResult<()> {
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

    pub(in crate::database) fn finish_drained_nodes(&mut self) -> DatabaseResult<()> {
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
}
