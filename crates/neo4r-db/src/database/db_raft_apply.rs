use super::metadata_types::*;
use super::*;

impl Neo4rDatabase {
    pub(super) fn append_local_command(
        &mut self,
        shard_id: ShardId,
        command: Command,
        sync: bool,
    ) -> DatabaseResult<LogEntry> {
        self.ensure_write_epoch()?;
        self.ensure_local_primary(shard_id)?;
        Self::validate_storable_properties_for_command(&command)?;
        self.validate_vector_indexes_for_command(&command)?;
        self.validate_unique_constraints_for_command(&command)?;
        let log_index = self.next_log_index(shard_id)?;
        let term = self.local_raft_term_for_append(shard_id)?;
        let timestamp = self.clock.tick();
        let entry = LogEntry::new_with_metadata(
            shard_id,
            term,
            log_index,
            self.config.server_id,
            self.routing_table.version,
            timestamp,
            command,
        );
        self.log(shard_id)?
            .append_with_sync(&entry, sync && self.should_sync_wal(entry.index))?;
        self.append_entry_to_local_raft(&entry)?;
        self.observe_log_position(&entry);
        Ok(entry)
    }

    pub(super) fn local_raft_term_for_append(&mut self, shard_id: ShardId) -> DatabaseResult<u64> {
        let Some(raft_groups) = self.raft_groups.as_mut() else {
            return Ok(0);
        };
        let group = raft_groups.group_mut(shard_id)?;
        if group.role() != &RaftRole::Leader {
            return Err(DatabaseError::Replication(format!(
                "local server {} is not raft leader for shard {shard_id}",
                self.config.server_id
            )));
        }
        Ok(group.current_term())
    }

    pub(super) fn append_entry_to_local_raft(&mut self, entry: &LogEntry) -> DatabaseResult<()> {
        let Some(raft_groups) = self.raft_groups.as_mut() else {
            return Ok(());
        };
        raft_groups
            .group_mut(entry.shard_id)?
            .append_existing_local_entry(entry.clone())?;
        Ok(())
    }

    pub(super) fn flush_group_commit(&mut self, entries: &[LogEntry]) -> DatabaseResult<()> {
        let outcomes = self.replicator.publish_batch(entries)?;
        for (entry, outcome) in entries.iter().zip(outcomes.iter()) {
            self.observe_replication_outcome(entry, outcome)?;
            self.observe_raft_replication_outcome(entry, outcome)?;
        }
        self.flush_entries(entries)
    }

    pub(super) fn flush_entries(&mut self, entries: &[LogEntry]) -> DatabaseResult<()> {
        let mut segments = BTreeMap::new();
        for entry in entries {
            let segment_start = self
                .log(entry.shard_id)?
                .segment_start_for_index(entry.index);
            segments.insert((entry.shard_id, segment_start), entry.index);
        }
        for ((shard_id, _), index) in segments {
            self.log(shard_id)?.sync_segment_for_index(index)?;
        }
        self.commit_entries(entries)?;
        for entry in entries {
            self.apply_entry(entry)?;
        }
        Ok(())
    }

    pub fn apply_replicated_entry(&mut self, entry: LogEntry) -> DatabaseResult<()> {
        self.apply_replicated_entries(vec![entry])
    }

    pub fn apply_replicated_entries(&mut self, entries: Vec<LogEntry>) -> DatabaseResult<()> {
        let leader_commit = entries
            .iter()
            .map(|entry| entry.index)
            .max()
            .unwrap_or_default();
        self.append_replicated_entries(entries)?;
        self.commit_and_apply_through(leader_commit)
    }

    pub fn apply_raft_append_entries(
        &mut self,
        shard_id: ShardId,
        entries: Vec<LogEntry>,
        leader_commit: LogIndex,
    ) -> DatabaseResult<()> {
        let response =
            self.apply_raft_append_entries_with_response(shard_id, entries, leader_commit)?;
        if response.success {
            Ok(())
        } else {
            Err(DatabaseError::LogConflict {
                shard_id,
                index: response.match_index,
                message: "raft append entries rejected by follower log".to_string(),
            })
        }
    }

    pub fn apply_raft_append_entries_with_response(
        &mut self,
        shard_id: ShardId,
        entries: Vec<LogEntry>,
        leader_commit: LogIndex,
    ) -> DatabaseResult<AppendEntriesResponse> {
        let response = self.observe_raft_append_entries(shard_id, &entries, leader_commit)?;
        if !response.success {
            return Ok(response);
        }
        self.truncate_replicated_log_for_append(shard_id, &entries)?;
        self.append_replicated_entries(entries)?;
        self.commit_and_apply_through(leader_commit)?;
        Ok(response)
    }

    pub fn request_raft_vote(
        &mut self,
        shard_id: ShardId,
        request: RequestVoteRequest,
    ) -> DatabaseResult<RequestVoteResponse> {
        let Some(raft_groups) = self.raft_groups.as_mut() else {
            return Err(DatabaseError::Replication(
                "raft is not enabled for this database".to_string(),
            ));
        };
        raft_groups.group_mut(shard_id)?.request_vote(request)
    }

    pub fn install_raft_snapshot(
        &mut self,
        request: InstallSnapshotRequest,
    ) -> DatabaseResult<InstallSnapshotResponse> {
        let metadata = request.metadata.clone();
        let payload = request.payload.clone();
        let shard_id = metadata.shard_id;
        self.ensure_local_copy(shard_id)?;
        let response = {
            let Some(raft_groups) = self.raft_groups.as_mut() else {
                return Err(DatabaseError::Replication(
                    "raft is not enabled for this database".to_string(),
                ));
            };
            raft_groups.group_mut(shard_id)?.install_snapshot(request)?
        };
        if response.success {
            if !payload.is_empty() {
                self.checkpoint(metadata.shard_id)?;
                if self
                    .config
                    .failure_injection
                    .fail_before_snapshot_payload_save
                {
                    return Err(DatabaseError::Replication(
                        "injected failure before snapshot payload save".to_string(),
                    ));
                }
                let snapshot_store =
                    neo4r_storage::SnapshotStore::open(&self.config.data_dir, metadata.shard_id)?;
                snapshot_store.save_payload(&payload)?;
                if self
                    .config
                    .failure_injection
                    .fail_after_snapshot_payload_save_before_metadata
                {
                    return Err(DatabaseError::Replication(
                        "injected failure after snapshot payload save before metadata".to_string(),
                    ));
                }
                if let Some(snapshot) = snapshot_store.load()? {
                    self.apply_loaded_snapshot(metadata.shard_id, &snapshot)?;
                }
            }
            self.install_raft_snapshot_metadata(metadata)?;
        }
        Ok(response)
    }

    pub fn install_snapshot_request_for_shard(
        &mut self,
        shard_id: ShardId,
    ) -> DatabaseResult<Option<InstallSnapshotRequest>> {
        self.ensure_local_copy(shard_id)?;
        let snapshot_store = neo4r_storage::SnapshotStore::open(&self.config.data_dir, shard_id)?;
        let Some(snapshot) = snapshot_store.load()? else {
            return Ok(None);
        };
        let payload = snapshot_store.load_payload()?.unwrap_or_default();
        let term = self
            .raft_groups
            .as_mut()
            .map(|groups| groups.group_mut(shard_id).map(|group| group.current_term()))
            .transpose()?
            .unwrap_or(snapshot.last_included_term);
        Ok(Some(InstallSnapshotRequest {
            term: term.max(snapshot.last_included_term),
            leader_id: self.config.server_id,
            metadata: RaftSnapshotMetadata {
                shard_id,
                last_included_term: snapshot.last_included_term,
                last_included_index: snapshot.last_included_index,
            },
            payload,
        }))
    }

    pub(super) fn apply_loaded_snapshot(
        &mut self,
        shard_id: ShardId,
        snapshot: &neo4r_storage::LoadedSnapshot,
    ) -> DatabaseResult<()> {
        if snapshot.shard_id != shard_id {
            return Err(DatabaseError::InvalidConfig(format!(
                "snapshot shard {} does not match install shard {shard_id}",
                snapshot.shard_id
            )));
        }
        self.prune_snapshot_shard_state(shard_id)?;
        if self
            .config
            .failure_injection
            .fail_after_snapshot_prune_before_apply
        {
            return Err(DatabaseError::Replication(
                "injected failure after snapshot prune before apply".to_string(),
            ));
        }
        for node in snapshot.graph.nodes() {
            self.store.apply(
                shard_id,
                &Command::CreateNode {
                    id: node.id,
                    labels: node.labels.clone(),
                    properties: node.properties.clone(),
                },
            )?;
        }
        for boundary in snapshot.graph.boundary_nodes() {
            self.store.apply(
                shard_id,
                &Command::UpsertBoundaryNode {
                    id: boundary.id,
                    owner_shard: boundary.owner_shard,
                    labels: boundary.labels.clone(),
                    properties: boundary.properties.clone(),
                    version: boundary.version,
                },
            )?;
        }
        for relationship in snapshot.graph.relationships() {
            self.store.apply(
                shard_id,
                &Command::CreateRelationship {
                    id: relationship.id,
                    from: relationship.from,
                    to: relationship.to,
                    rel_type: relationship.rel_type.clone(),
                    properties: relationship.properties.clone(),
                },
            )?;
        }
        self.rebuild_vector_indexes()?;
        self.refresh_statistics_catalog()?;
        Ok(())
    }

    pub(super) fn prune_snapshot_shard_state(&mut self, shard_id: ShardId) -> DatabaseResult<()> {
        let mut relationship_ids = self
            .store
            .relationships()?
            .into_iter()
            .filter(|relationship| {
                self.shard_map.owner_of_relationship(
                    relationship.from,
                    relationship.to,
                    &relationship.rel_type,
                ) == shard_id
            })
            .map(|relationship| relationship.id)
            .collect::<Vec<_>>();
        relationship_ids.sort_unstable();
        relationship_ids.dedup();
        for relationship_id in relationship_ids {
            self.store.apply(
                shard_id,
                &Command::DeleteRelationship {
                    id: relationship_id,
                },
            )?;
        }

        let mut node_ids = self
            .store
            .nodes()?
            .into_iter()
            .filter(|node| self.shard_map.owner_of_node(node.id) == shard_id)
            .map(|node| node.id)
            .collect::<Vec<_>>();
        node_ids.sort_unstable();
        node_ids.dedup();
        for node_id in node_ids {
            self.store
                .apply(shard_id, &Command::DeleteNode { id: node_id })?;
        }
        Ok(())
    }

    pub(super) fn install_raft_snapshot_metadata(
        &mut self,
        metadata: RaftSnapshotMetadata,
    ) -> DatabaseResult<()> {
        let shard_id = metadata.shard_id;
        self.commits
            .get(shard_id as usize)
            .ok_or(DatabaseError::MissingShardLog(shard_id))?
            .save(metadata.last_included_term, metadata.last_included_index)?;
        self.checkpoint(shard_id)?.save_with_timestamp(
            metadata.last_included_term,
            metadata.last_included_index,
            self.clock.now(),
        )?;
        if let Some(slot) = self.commit_indexes.get_mut(shard_id as usize) {
            *slot = (*slot).max(metadata.last_included_index);
        }
        if let Some(slot) = self.next_log_indexes.get_mut(shard_id as usize) {
            *slot = (*slot).max(metadata.last_included_index.saturating_add(1));
        }
        Ok(())
    }

    pub fn start_raft_election(&mut self, shard_id: ShardId) -> DatabaseResult<RequestVoteRequest> {
        let Some(raft_groups) = self.raft_groups.as_mut() else {
            return Err(DatabaseError::Replication(
                "raft is not enabled for this database".to_string(),
            ));
        };
        let group = raft_groups.group_mut(shard_id)?;
        if group.role() == &RaftRole::Leader {
            return Err(DatabaseError::Replication(format!(
                "local server is already raft leader for shard {shard_id}"
            )));
        }
        group.start_election()
    }

    pub fn record_raft_vote_response(
        &mut self,
        shard_id: ShardId,
        voter_id: ServerId,
        response: RequestVoteResponse,
    ) -> DatabaseResult<bool> {
        let Some(raft_groups) = self.raft_groups.as_mut() else {
            return Err(DatabaseError::Replication(
                "raft is not enabled for this database".to_string(),
            ));
        };
        raft_groups
            .group_mut(shard_id)?
            .record_vote_response(voter_id, response)
    }

    pub fn local_raft_shards(&self) -> DatabaseResult<Vec<ShardId>> {
        if self.raft_groups.is_none() {
            return Ok(Vec::new());
        }
        Ok(self
            .routing_table
            .placements
            .iter()
            .filter(|placement| placement.has_server(self.config.server_id))
            .map(|placement| placement.shard_id)
            .collect())
    }

    pub fn raft_status(&self) -> DatabaseResult<Vec<RaftShardStatus>> {
        let Some(raft_groups) = self.raft_groups.as_ref() else {
            return Ok(Vec::new());
        };
        Ok(raft_groups
            .groups
            .iter()
            .map(|group| RaftShardStatus {
                shard_id: group.shard_id(),
                term: group.current_term(),
                role: group.role().clone(),
                leader_id: group.leader_id(),
                commit_index: group.commit_index(),
                last_log_index: group.last_log_index(),
                snapshot_index: group
                    .snapshot()
                    .map(|snapshot| snapshot.last_included_index)
                    .unwrap_or_default(),
                joint_consensus: group.membership().is_joint(),
            })
            .collect())
    }

    pub fn raft_election_candidates(&self, timeout: Duration) -> DatabaseResult<Vec<ShardId>> {
        let Some(raft_groups) = self.raft_groups.as_ref() else {
            return Ok(Vec::new());
        };
        let mut candidates = Vec::new();
        for placement in &self.routing_table.placements {
            if placement.has_server(self.config.server_id)
                && raft_groups.should_start_election(placement.shard_id, timeout)?
            {
                candidates.push(placement.shard_id);
            }
        }
        Ok(candidates)
    }

    pub(super) fn append_replicated_entries(
        &mut self,
        entries: Vec<LogEntry>,
    ) -> DatabaseResult<()> {
        if entries.is_empty() {
            return Ok(());
        }
        let mut accepted = Vec::new();
        let mut node_overlay = HashMap::<NodeId, Option<Node>>::new();
        let mut next_indexes = HashMap::<ShardId, LogIndex>::new();
        for entry in entries {
            self.validate_replicated_entry_metadata(&entry)?;
            self.ensure_local_copy(entry.shard_id)?;
            let expected = match next_indexes.get(&entry.shard_id) {
                Some(expected) => *expected,
                None => self.next_log_index(entry.shard_id)?,
            };
            if entry.index < expected {
                self.ensure_duplicate_entry_matches(&entry)?;
                continue;
            }
            if entry.index != expected {
                return Err(DatabaseError::UnexpectedLogIndex {
                    shard_id: entry.shard_id,
                    expected,
                    actual: entry.index,
                });
            }
            Self::validate_storable_properties_for_command(&entry.command)?;
            self.validate_replicated_vector_indexes_for_command(&entry.command, &mut node_overlay)?;
            next_indexes.insert(entry.shard_id, expected + 1);
            accepted.push(entry);
        }
        for entry in &accepted {
            self.log(entry.shard_id)?.append_with_sync(entry, false)?;
            self.observe_log_position(entry);
        }
        for entry in &accepted {
            self.log(entry.shard_id)?
                .sync_segment_for_index(entry.index)?;
        }
        Ok(())
    }

    pub(super) fn truncate_replicated_log_for_append(
        &mut self,
        shard_id: ShardId,
        entries: &[LogEntry],
    ) -> DatabaseResult<()> {
        for entry in entries {
            if entry.shard_id != shard_id {
                continue;
            }
            let Some(existing) = self.log(shard_id)?.entry(entry.index)? else {
                continue;
            };
            if existing == *entry {
                continue;
            }
            let committed = self.commit_index(shard_id)?;
            if entry.index <= committed {
                return Err(DatabaseError::LogConflict {
                    shard_id,
                    index: entry.index,
                    message: "cannot truncate committed replicated log entry".to_string(),
                });
            }
            self.log(shard_id)?.truncate_from(entry.index)?;
            if let Some(slot) = self.next_log_indexes.get_mut(shard_id as usize) {
                *slot = entry.index;
            }
            break;
        }
        Ok(())
    }

    pub(super) fn observe_raft_append_entries(
        &mut self,
        heartbeat_shard_id: ShardId,
        entries: &[LogEntry],
        leader_commit: LogIndex,
    ) -> DatabaseResult<AppendEntriesResponse> {
        if self.raft_groups.is_none() {
            return Ok(AppendEntriesResponse {
                term: 0,
                success: true,
                match_index: leader_commit,
                conflict_index: None,
                conflict_term: None,
            });
        }
        let mut entries_by_shard = BTreeMap::<ShardId, Vec<LogEntry>>::new();
        for entry in entries {
            entries_by_shard
                .entry(entry.shard_id)
                .or_default()
                .push(entry.clone());
        }
        if entries_by_shard.is_empty() {
            entries_by_shard.insert(heartbeat_shard_id, Vec::new());
        }
        let mut last_response = None;
        for (shard_id, shard_entries) in entries_by_shard {
            let prev_log_index = shard_entries
                .first()
                .map(|entry| entry.index.saturating_sub(1))
                .unwrap_or_else(|| {
                    self.raft_groups
                        .as_ref()
                        .and_then(|raft_groups| {
                            raft_groups
                                .groups
                                .get(shard_id as usize)
                                .map(RaftCore::last_log_index)
                        })
                        .unwrap_or_default()
                });
            let prev_log_term = if prev_log_index == 0 {
                0
            } else {
                self.log(shard_id)?
                    .entry(prev_log_index)?
                    .map(|entry| entry.term)
                    .unwrap_or_default()
            };
            let term = shard_entries
                .last()
                .map(|entry| entry.term)
                .unwrap_or_else(|| {
                    self.raft_groups
                        .as_ref()
                        .and_then(|raft_groups| {
                            raft_groups
                                .groups
                                .get(shard_id as usize)
                                .map(RaftCore::current_term)
                        })
                        .unwrap_or_default()
                });
            let leader_id = shard_entries
                .last()
                .map(|entry| entry.origin_server_id)
                .unwrap_or_default();
            let group = self
                .raft_groups
                .as_mut()
                .ok_or(DatabaseError::MissingShardLog(shard_id))?
                .group_mut(shard_id)?;
            let response = group.append_entries(AppendEntriesRequest {
                term,
                leader_id,
                prev_log_index,
                prev_log_term,
                entries: shard_entries,
                leader_commit,
            })?;
            if !response.success {
                return Ok(response);
            }
            self.raft_groups
                .as_mut()
                .ok_or(DatabaseError::MissingShardLog(shard_id))?
                .record_leader_contact(shard_id)?;
            last_response = Some(response);
        }
        Ok(last_response.unwrap_or(AppendEntriesResponse {
            term: 0,
            success: true,
            match_index: leader_commit,
            conflict_index: None,
            conflict_term: None,
        }))
    }

    pub(super) fn commit_and_apply_through(
        &mut self,
        leader_commit: LogIndex,
    ) -> DatabaseResult<()> {
        if leader_commit == 0 {
            return Ok(());
        }
        for shard_id in 0..self.shard_map.shard_count() {
            loop {
                let next = self.commit_index(shard_id)?.saturating_add(1);
                if next > leader_commit {
                    break;
                }
                let Some(entry) = self.log(shard_id)?.entry(next)? else {
                    break;
                };
                self.commit_entry(&entry)?;
                self.apply_entry(&entry)?;
            }
        }
        Ok(())
    }

    pub(super) fn validate_replicated_entry_metadata(
        &self,
        entry: &LogEntry,
    ) -> DatabaseResult<()> {
        if entry.config_version != 0 && entry.config_version != self.routing_table.version {
            return Err(DatabaseError::LogConflict {
                shard_id: entry.shard_id,
                index: entry.index,
                message: format!(
                    "config version mismatch: entry {}, local {}",
                    entry.config_version, self.routing_table.version
                ),
            });
        }
        Ok(())
    }

    pub(super) fn ensure_duplicate_entry_matches(&self, entry: &LogEntry) -> DatabaseResult<()> {
        let Some(existing) = self.log(entry.shard_id)?.entry(entry.index)? else {
            return Err(DatabaseError::LogConflict {
                shard_id: entry.shard_id,
                index: entry.index,
                message: "duplicate entry is below next index but missing locally".to_string(),
            });
        };
        if existing == *entry {
            Ok(())
        } else {
            Err(DatabaseError::LogConflict {
                shard_id: entry.shard_id,
                index: entry.index,
                message: "duplicate entry payload differs from local log".to_string(),
            })
        }
    }

    pub(super) fn apply_entry(&mut self, entry: &LogEntry) -> DatabaseResult<()> {
        self.apply_cluster_config_change(&entry.command)?;
        self.store.apply(entry.shard_id, &entry.command)?;
        self.invalidate_read_cache();
        self.update_vector_indexes_for_command(&entry.command)?;
        self.observe_entry(entry);
        self.refresh_statistics_catalog()?;
        if self.should_checkpoint(entry.index) {
            self.checkpoint(entry.shard_id)?.save_with_timestamp(
                entry.term,
                entry.index,
                entry.timestamp,
            )?;
        }
        Ok(())
    }

    pub(super) fn apply_cluster_config_change(&mut self, command: &Command) -> DatabaseResult<()> {
        let Command::ClusterConfigChange {
            phase,
            routing_table,
            ..
        } = command
        else {
            return Ok(());
        };
        if phase != "install" {
            return Ok(());
        }
        if routing_table.version < self.routing_table.version {
            return Ok(());
        }
        validate_routing_table(routing_table, self.shard_map.shard_count())?;
        if routing_table.version == self.routing_table.version
            && routing_table.placements == self.routing_table.placements
        {
            return Ok(());
        }
        self.shard_metadata.save(routing_table)?;
        self.replicator
            .install_routing_table(routing_table.clone())?;
        self.cluster_metadata.config_epoch = routing_table.version;
        self.cluster_metadata_store.save(&self.cluster_metadata)?;
        self.routing_table = routing_table.clone();
        self.rebuild_raft_groups()?;
        Ok(())
    }

    pub(super) fn observe_read_cache_hit(&self) -> DatabaseResult<()> {
        let mut stats = self
            .read_cache_stats
            .lock()
            .map_err(|_| DatabaseError::LockPoisoned)?;
        stats.hits = stats.hits.saturating_add(1);
        Ok(())
    }

    pub(super) fn observe_read_cache_miss(&self) -> DatabaseResult<()> {
        let mut stats = self
            .read_cache_stats
            .lock()
            .map_err(|_| DatabaseError::LockPoisoned)?;
        stats.misses = stats.misses.saturating_add(1);
        Ok(())
    }

    pub(super) fn read_cache_stats(&self) -> DatabaseResult<ReadCacheStats> {
        self.read_cache_stats
            .lock()
            .map_err(|_| DatabaseError::LockPoisoned)
            .map(|stats| *stats)
    }

    pub(super) fn invalidate_read_cache(&self) {
        if let Ok(mut cache) = self.read_cache.lock() {
            cache.nodes.clear();
            cache.relationships.clear();
            cache.index_lookups.clear();
        }
    }

    pub(super) fn update_vector_indexes_for_command(
        &mut self,
        command: &Command,
    ) -> DatabaseResult<()> {
        match command {
            Command::CreateNode { id, .. }
            | Command::SetNodeProperty { id, .. }
            | Command::RemoveNodeProperty { id, .. }
            | Command::AddNodeLabel { id, .. }
            | Command::RemoveNodeLabel { id, .. } => {
                if let Some(node) = self.store.node(*id)? {
                    let mut indexes = self
                        .vector_indexes
                        .lock()
                        .map_err(|_| DatabaseError::LockPoisoned)?;
                    indexes.update_node(&node);
                    let should_save = !indexes.is_empty();
                    drop(indexes);
                    if should_save {
                        self.save_vector_index_cache()?;
                    }
                }
            }
            Command::DeleteNode { id } => {
                let mut indexes = self
                    .vector_indexes
                    .lock()
                    .map_err(|_| DatabaseError::LockPoisoned)?;
                indexes.delete_node(*id);
                let should_save = !indexes.is_empty();
                drop(indexes);
                if should_save {
                    self.save_vector_index_cache()?;
                }
            }
            Command::CreateRelationship { .. }
            | Command::SetRelationshipProperty { .. }
            | Command::RemoveRelationshipProperty { .. }
            | Command::DeleteRelationship { .. }
            | Command::UpsertBoundaryNode { .. }
            | Command::ClusterConfigChange { .. } => {}
        }
        Ok(())
    }

    pub(super) fn ensure_local_primary(&self, shard_id: ShardId) -> DatabaseResult<()> {
        let primary_server_id = self.routing_table.primary_server_id(shard_id);
        if primary_server_id == Some(self.config.server_id) {
            Ok(())
        } else {
            Err(DatabaseError::ShardNotPrimary {
                shard_id,
                server_id: self.config.server_id,
                primary_server_id,
            })
        }
    }

    pub(super) fn ensure_local_copy(&self, shard_id: ShardId) -> DatabaseResult<()> {
        if self
            .routing_table
            .has_local_copy(shard_id, self.config.server_id)
        {
            Ok(())
        } else {
            Err(DatabaseError::ShardNotLocal {
                shard_id,
                server_id: self.config.server_id,
            })
        }
    }

    pub(super) fn ensure_local_node_exists(&self, id: NodeId) -> DatabaseResult<()> {
        if self.store.node(id)?.is_some() {
            Ok(())
        } else {
            Err(GraphError::NodeNotFound(id).into())
        }
    }

    pub(super) fn ensure_node_or_boundary_exists(&self, id: NodeId) -> DatabaseResult<()> {
        if self.store.node(id)?.is_some() || self.store.boundary_node(id)?.is_some() {
            Ok(())
        } else {
            Err(GraphError::NodeNotFound(id).into())
        }
    }

    pub(super) fn relationship_owner_shard(&self, id: RelationshipId) -> DatabaseResult<ShardId> {
        let relationship = self
            .store
            .relationship(id)?
            .ok_or(GraphError::RelationshipNotFound(id))?;
        Ok(self.shard_map.owner_of_relationship(
            relationship.from,
            relationship.to,
            &relationship.rel_type,
        ))
    }

    pub(super) fn replay_logs(&mut self) -> DatabaseResult<()> {
        for shard_id in 0..self.shard_map.shard_count() {
            let start_index = self
                .checkpoint(shard_id)?
                .load()?
                .map(|checkpoint| checkpoint.last_applied_index.saturating_add(1))
                .unwrap_or(0);
            let entries = self.log(shard_id)?.replay_from(start_index)?;
            for entry in entries {
                self.observe_log_position(&entry);
                if entry.index <= self.commit_index(entry.shard_id)? {
                    self.apply_entry(&entry)?;
                }
            }
        }
        Ok(())
    }
}
