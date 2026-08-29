use super::metadata_types::*;
use super::staged_overlay::*;
use super::write_cypher_helpers::*;
use super::*;

impl Neo4rDatabase {
    pub(super) fn recover_allocators_from_store(&mut self) -> DatabaseResult<()> {
        self.next_node_id = self
            .store
            .nodes()?
            .into_iter()
            .map(|node| node.id.saturating_add(1))
            .max()
            .unwrap_or(self.next_node_id);
        self.next_relationship_id = self
            .store
            .relationships()?
            .into_iter()
            .map(|relationship| relationship.id.saturating_add(1))
            .max()
            .unwrap_or(self.next_relationship_id);
        for shard_id in 0..self.shard_map.shard_count() {
            if let Some(checkpoint) = self.checkpoint(shard_id)?.load()? {
                self.next_log_indexes[shard_id as usize] = self.next_log_indexes[shard_id as usize]
                    .max(checkpoint.last_applied_index.saturating_add(1));
                self.clock.observe(checkpoint.timestamp);
            }
        }
        Ok(())
    }

    pub(super) fn observe_entry(&mut self, entry: &LogEntry) {
        self.observe_log_position(entry);
        self.clock.observe(entry.timestamp);
        match &entry.command {
            Command::CreateNode { id, .. } | Command::UpsertBoundaryNode { id, .. } => {
                self.next_node_id = self.next_node_id.max(id.saturating_add(1));
            }
            Command::CreateRelationship { id, .. } => {
                self.next_relationship_id = self.next_relationship_id.max(id.saturating_add(1));
            }
            Command::SetNodeProperty { .. }
            | Command::RemoveNodeProperty { .. }
            | Command::AddNodeLabel { .. }
            | Command::RemoveNodeLabel { .. }
            | Command::SetRelationshipProperty { .. }
            | Command::RemoveRelationshipProperty { .. }
            | Command::DeleteRelationship { .. }
            | Command::DeleteNode { .. }
            | Command::ClusterConfigChange { .. } => {}
        }
    }

    pub(super) fn observe_log_position(&mut self, entry: &LogEntry) {
        let index_slot = &mut self.next_log_indexes[entry.shard_id as usize];
        *index_slot = (*index_slot).max(entry.index.saturating_add(1));
    }

    pub(super) fn observe_replication_outcome(
        &mut self,
        entry: &LogEntry,
        outcome: &ReplicationOutcome,
    ) -> DatabaseResult<()> {
        let match_indexes = self
            .match_indexes
            .get_mut(entry.shard_id as usize)
            .ok_or(DatabaseError::MissingShardLog(entry.shard_id))?;
        for (server_id, shard_id, index) in &outcome.acked_match_indexes {
            if *shard_id != entry.shard_id {
                continue;
            }
            let slot = match_indexes.entry(*server_id).or_default();
            *slot = (*slot).max(*index);
        }
        for server_id in &outcome.acked_server_ids {
            let slot = match_indexes.entry(*server_id).or_default();
            *slot = (*slot).max(entry.index);
        }
        Ok(())
    }

    pub(super) fn observe_raft_replication_outcome(
        &mut self,
        entry: &LogEntry,
        outcome: &ReplicationOutcome,
    ) -> DatabaseResult<()> {
        let Some(raft_groups) = self.raft_groups.as_mut() else {
            return Ok(());
        };
        let group = raft_groups.group_mut(entry.shard_id)?;
        for (server_id, shard_id, index) in &outcome.acked_match_indexes {
            if *shard_id == entry.shard_id {
                group.record_replication_match(*server_id, *index)?;
            }
        }
        for server_id in &outcome.acked_server_ids {
            group.record_replication_match(*server_id, entry.index)?;
        }
        if group.commit_index() < entry.index {
            return Err(DatabaseError::Replication(format!(
                "raft quorum did not commit shard {} entry {}",
                entry.shard_id, entry.index
            )));
        }
        Ok(())
    }

    pub(super) fn commit_entry(&mut self, entry: &LogEntry) -> DatabaseResult<()> {
        let expected = self.commit_index(entry.shard_id)?.saturating_add(1);
        if entry.index != expected {
            return Err(DatabaseError::Replication(format!(
                "cannot commit non-contiguous shard {} entry {} while commit index is {}",
                entry.shard_id,
                entry.index,
                expected.saturating_sub(1)
            )));
        }
        self.commits
            .get(entry.shard_id as usize)
            .ok_or(DatabaseError::MissingShardLog(entry.shard_id))?
            .save(entry.term, entry.index)?;
        self.commit_indexes[entry.shard_id as usize] = entry.index;
        Ok(())
    }

    pub(super) fn commit_entries(&mut self, entries: &[LogEntry]) -> DatabaseResult<()> {
        let mut last_by_shard = BTreeMap::<ShardId, (u64, LogIndex)>::new();
        for entry in entries {
            let expected = self.commit_index(entry.shard_id)?.saturating_add(1);
            if entry.index != expected {
                return Err(DatabaseError::Replication(format!(
                    "cannot commit non-contiguous shard {} entry {} while commit index is {}",
                    entry.shard_id,
                    entry.index,
                    expected.saturating_sub(1)
                )));
            }
            self.commit_indexes[entry.shard_id as usize] = entry.index;
            last_by_shard.insert(entry.shard_id, (entry.term, entry.index));
        }
        for (shard_id, (term, index)) in last_by_shard {
            self.commits
                .get(shard_id as usize)
                .ok_or(DatabaseError::MissingShardLog(shard_id))?
                .save(term, index)?;
        }
        Ok(())
    }

    pub(super) fn allocate_node_id(&mut self) -> NodeId {
        let id = self.next_node_id;
        self.next_node_id += 1;
        id
    }

    pub(super) fn allocate_node_id_for_shard(&mut self, shard_id: ShardId) -> NodeId {
        while self.shard_map.owner_of_node(self.next_node_id) != shard_id {
            self.next_node_id += 1;
        }
        self.allocate_node_id()
    }

    pub(super) fn validate_shard_id(&self, shard_id: ShardId) -> DatabaseResult<()> {
        if shard_id >= self.shard_map.shard_count() {
            return Err(DatabaseError::MissingShardLog(shard_id));
        }
        Ok(())
    }

    pub(super) fn allocate_relationship_id(&mut self) -> RelationshipId {
        let id = self.next_relationship_id;
        self.next_relationship_id += 1;
        id
    }

    pub(super) fn next_log_index(&self, shard_id: ShardId) -> DatabaseResult<LogIndex> {
        self.next_log_indexes
            .get(shard_id as usize)
            .copied()
            .ok_or(DatabaseError::MissingShardLog(shard_id))
    }

    pub(super) fn log(&self, shard_id: ShardId) -> DatabaseResult<&SegmentedShardLog> {
        self.logs
            .get(shard_id as usize)
            .ok_or(DatabaseError::MissingShardLog(shard_id))
    }

    pub(super) fn checkpoint(&self, shard_id: ShardId) -> DatabaseResult<&CheckpointStore> {
        self.checkpoints
            .get(shard_id as usize)
            .ok_or(DatabaseError::MissingShardLog(shard_id))
    }

    pub(super) fn should_checkpoint(&self, index: LogIndex) -> bool {
        index != 0 && index % self.config.checkpoint_interval == 0
    }

    pub(super) fn should_sync_wal(&self, index: LogIndex) -> bool {
        index != 0 && index % self.config.wal_sync_interval == 0
    }

    pub(super) fn applied_indexes(&self) -> Vec<LogIndex> {
        self.next_log_indexes
            .iter()
            .map(|next| next.saturating_sub(1))
            .collect()
    }

    pub(super) fn committed_indexes(&self) -> Vec<LogIndex> {
        self.commit_indexes.clone()
    }

    pub(super) fn commit_index(&self, shard_id: ShardId) -> DatabaseResult<LogIndex> {
        self.commit_indexes
            .get(shard_id as usize)
            .copied()
            .ok_or(DatabaseError::MissingShardLog(shard_id))
    }

    pub(super) fn query_route(&self) -> QueryRoute {
        let remote_shards = (0..self.shard_map.shard_count())
            .filter(|shard_id| {
                !self
                    .routing_table
                    .has_local_copy(*shard_id, self.config.server_id)
            })
            .collect::<Vec<_>>();
        if remote_shards.is_empty() {
            QueryRoute::LocalOnly
        } else {
            QueryRoute::RequiresRemoteShards(remote_shards)
        }
    }

    pub(super) fn query_plan(&self, query: &str, params: &QueryParams) -> DistributedQueryPlan {
        self.observe_index_cache_probe(query);
        let route = self.query_route();
        let access_plan = self.query_access_plan(query, params);
        let statistics = self
            .statistics_catalog()
            .unwrap_or_else(|_| StatisticsCatalog {
                node_count: 0,
                relationship_count: 0,
                label_counts: Vec::new(),
                node_property_counts: Vec::new(),
                relationship_type_counts: Vec::new(),
                index_count: self.index_catalog.indexes.len(),
                vector_index_count: 0,
            });
        let traversal_policy = match &route {
            QueryRoute::LocalOnly => RemoteTraversalPolicy::BoundaryCacheOnly,
            QueryRoute::RequiresRemoteShards(shards) => {
                RemoteTraversalPolicy::RemoteShardHop(shards.clone())
            }
        };
        let remote_shard_count = match &route {
            QueryRoute::LocalOnly => 0,
            QueryRoute::RequiresRemoteShards(shards) => shards.len(),
        };
        let estimated_rows = estimate_rows(&statistics, &access_plan);
        let estimated_cost = estimate_query_cost(&statistics, &access_plan, remote_shard_count);
        let access_reason = access_plan_reason(&access_plan, &statistics, remote_shard_count);
        DistributedQueryPlan {
            route,
            traversal_policy,
            uses_boundary_cache: true,
            access_plan,
            access_reason,
            cost_model_version: 3,
            estimated_cost,
            estimated_rows,
            remote_shard_count,
        }
    }

    pub(super) fn observe_index_cache_probe(&self, query: &str) {
        let key = format!("v{}:{query}", self.routing_table.version);
        if let (Ok(mut cache), Ok(mut stats)) =
            (self.read_cache.lock(), self.read_cache_stats.lock())
        {
            if cache.index_lookups.contains_key(&key) {
                stats.index_hits = stats.index_hits.saturating_add(1);
            } else {
                stats.index_misses = stats.index_misses.saturating_add(1);
                cache.index_lookups.insert(key, Vec::new());
            }
        }
    }

    pub(super) fn statistics_catalog(&self) -> DatabaseResult<StatisticsCatalog> {
        if self.statistics.node_count != 0
            || self.statistics.relationship_count != 0
            || self.statistics.index_count != 0
            || self.statistics.vector_index_count != 0
            || !self.statistics.label_counts.is_empty()
            || !self.statistics.relationship_type_counts.is_empty()
        {
            return Ok(self.statistics.clone());
        }
        self.compute_statistics_catalog()
    }

    pub(super) fn compute_statistics_catalog(&self) -> DatabaseResult<StatisticsCatalog> {
        let nodes = self.store.nodes().map_err(DatabaseError::GraphRead)?;
        let relationships = self.store.relationships()?;
        let mut label_counts = BTreeMap::new();
        let mut node_property_counts = BTreeMap::new();
        for node in &nodes {
            for label in &node.labels {
                *label_counts.entry(label.clone()).or_insert(0) += 1;
            }
            for key in node.properties.keys() {
                *node_property_counts.entry(key.clone()).or_insert(0) += 1;
            }
        }
        let mut relationship_type_counts = BTreeMap::new();
        for relationship in &relationships {
            *relationship_type_counts
                .entry(relationship.rel_type.clone())
                .or_insert(0) += 1;
        }
        Ok(StatisticsCatalog {
            node_count: nodes.len(),
            relationship_count: relationships.len(),
            label_counts: label_counts.into_iter().collect(),
            node_property_counts: node_property_counts.into_iter().collect(),
            relationship_type_counts: relationship_type_counts.into_iter().collect(),
            index_count: self.index_catalog.indexes.len(),
            vector_index_count: self
                .index_catalog
                .indexes
                .iter()
                .filter(|index| matches!(index.kind, IndexKind::Vector { .. }))
                .count(),
        })
    }

    pub(super) fn refresh_statistics_catalog(&mut self) -> DatabaseResult<()> {
        self.statistics = self.compute_statistics_catalog()?;
        self.statistics_store.save(&self.statistics)
    }

    pub(super) fn storage_status(&self) -> DatabaseResult<StorageStatus> {
        let stats = collect_storage_files(&self.config.data_dir)?;
        let cache_stats = self.read_cache_stats()?;
        Ok(StorageStatus {
            data_dir: self.config.data_dir.clone(),
            total_bytes: stats.total_bytes,
            file_count: stats.file_count,
            wal_segment_count: stats.wal_segment_count,
            checkpoint_file_count: stats.checkpoint_file_count,
            metadata_file_count: stats.metadata_file_count,
            committed_indexes: self.committed_indexes(),
            read_cache_hits: cache_stats.hits,
            read_cache_misses: cache_stats.misses,
            index_cache_hits: cache_stats.index_hits,
            index_cache_misses: cache_stats.index_misses,
            wal_pruned_until: self.committed_indexes(),
        })
    }

    pub(super) fn checkpoint_now(&mut self) -> DatabaseResult<StorageMaintenanceResult> {
        let timestamp = self.clock.tick();
        let committed_indexes = self.committed_indexes();
        let mut files_touched = 0;
        for (shard_index, index) in committed_indexes.iter().enumerate() {
            self.checkpoint(shard_index as ShardId)?
                .save_with_timestamp(0, *index, timestamp)?;
            files_touched += 1;
        }
        let stats = collect_storage_files(&self.config.data_dir)?;
        Ok(StorageMaintenanceResult {
            action: "checkpoint".to_string(),
            files_touched,
            bytes_observed: stats.total_bytes,
            pruned_until: committed_indexes,
            safety_manifest: "checkpoint_manifest:v1".to_string(),
        })
    }

    pub(super) fn compact_storage(&mut self) -> DatabaseResult<StorageMaintenanceResult> {
        let before = collect_storage_files(&self.config.data_dir)?;
        self.invalidate_read_cache();
        Ok(StorageMaintenanceResult {
            action: "compact_observe".to_string(),
            files_touched: before.file_count,
            bytes_observed: before.total_bytes,
            pruned_until: self.committed_indexes(),
            safety_manifest: "compact_manifest:v1 mode=observe".to_string(),
        })
    }

    pub(super) fn verify_storage_invariants(&mut self) -> DatabaseResult<StorageMaintenanceResult> {
        let report = self.store.verify_invariants()?;
        Ok(storage_invariant_maintenance_result(
            "verify_invariants",
            &report,
        ))
    }

    pub(super) fn repair_storage_invariants(&mut self) -> DatabaseResult<StorageMaintenanceResult> {
        let report = self.store.repair_indexes()?;
        Ok(storage_invariant_maintenance_result(
            "repair_invariants",
            &report,
        ))
    }

    pub(super) fn snapshot_now(&mut self) -> DatabaseResult<StorageMaintenanceResult> {
        let mut files_touched = 0;
        let mut bytes_observed = 0;
        let mut manifest_shards = Vec::new();
        let committed_indexes = self.committed_indexes();
        for shard_id in 0..self.shard_map.shard_count() {
            let commit_index = committed_indexes
                .get(shard_id as usize)
                .copied()
                .unwrap_or_default();
            if commit_index == 0 {
                continue;
            }
            let graph = self.shard_graph_state(shard_id)?;
            let term = self
                .log(shard_id)?
                .entry(commit_index)?
                .map(|entry| entry.term)
                .unwrap_or_default();
            let snapshot_store =
                neo4r_storage::SnapshotStore::open(&self.config.data_dir, shard_id)?;
            snapshot_store.save(&graph, term, commit_index)?;
            let payload_bytes = snapshot_store
                .load_payload()?
                .map(|payload| payload.len() as u64)
                .unwrap_or_default();
            let checksum = snapshot_payload_checksum(&snapshot_store)?;
            bytes_observed += payload_bytes;
            manifest_shards.push(format!(
                "{shard_id}:{term}:{commit_index}:{payload_bytes}:{checksum}"
            ));
            if let Some(raft_groups) = self.raft_groups.as_mut() {
                raft_groups
                    .group_mut(shard_id)?
                    .compact_log_to_snapshot(RaftSnapshotMetadata {
                        shard_id,
                        last_included_term: term,
                        last_included_index: commit_index,
                    })?;
            }
            files_touched += 1;
        }
        Ok(StorageMaintenanceResult {
            action: "snapshot".to_string(),
            files_touched,
            bytes_observed,
            pruned_until: committed_indexes,
            safety_manifest: format!(
                "snapshot_manifest:v1 shards={} raft_log_compacted=true",
                manifest_shards.join(",")
            ),
        })
    }

    pub(super) fn restore_snapshot(
        &mut self,
        shard_id: ShardId,
    ) -> DatabaseResult<StorageMaintenanceResult> {
        self.ensure_local_copy(shard_id)?;
        let snapshot_store = neo4r_storage::SnapshotStore::open(&self.config.data_dir, shard_id)?;
        let snapshot = snapshot_store.load()?.ok_or_else(|| {
            DatabaseError::InvalidConfig(format!("snapshot for shard {shard_id} does not exist"))
        })?;
        let bytes_observed = snapshot_store
            .load_payload()?
            .map(|payload| payload.len() as u64)
            .unwrap_or_default();
        self.write_snapshot_restore_manifest(shard_id, &snapshot, bytes_observed)?;
        self.apply_loaded_snapshot(shard_id, &snapshot)?;
        let metadata = RaftSnapshotMetadata {
            shard_id,
            last_included_term: snapshot.last_included_term,
            last_included_index: snapshot.last_included_index,
        };
        self.install_raft_snapshot_metadata(metadata.clone())?;
        if let Some(raft_groups) = self.raft_groups.as_mut() {
            let server_id = self.config.server_id;
            let term = raft_groups.group_mut(shard_id)?.current_term();
            raft_groups
                .group_mut(shard_id)?
                .install_snapshot(InstallSnapshotRequest {
                    term: term.max(metadata.last_included_term),
                    leader_id: server_id,
                    metadata,
                    payload: Vec::new(),
                })?;
        }
        self.clear_snapshot_restore_manifest()?;
        Ok(StorageMaintenanceResult {
            action: "restore_snapshot".to_string(),
            files_touched: 1,
            bytes_observed,
            pruned_until: self.committed_indexes(),
            safety_manifest: format!(
                "restore_snapshot_manifest:v1 shard={shard_id} term={} index={} bytes={} checksum={} materialized_state_replaced=true",
                snapshot.last_included_term,
                snapshot.last_included_index,
                bytes_observed,
                snapshot_payload_checksum(&snapshot_store)?
            ),
        })
    }

    pub(super) fn recover_pending_snapshot_restore(&mut self) -> DatabaseResult<()> {
        let Some((shard_id, expected_index)) = self.load_snapshot_restore_manifest()? else {
            return Ok(());
        };
        self.ensure_local_copy(shard_id)?;
        let snapshot_store = neo4r_storage::SnapshotStore::open(&self.config.data_dir, shard_id)?;
        let snapshot = snapshot_store.load()?.ok_or_else(|| {
            DatabaseError::InvalidConfig(format!(
                "pending restore for shard {shard_id} has no snapshot payload"
            ))
        })?;
        if snapshot.last_included_index != expected_index {
            return Err(DatabaseError::InvalidConfig(format!(
                "pending restore expected shard {shard_id} snapshot index {expected_index}, found {}",
                snapshot.last_included_index
            )));
        }
        self.apply_loaded_snapshot(shard_id, &snapshot)?;
        self.install_raft_snapshot_metadata(RaftSnapshotMetadata {
            shard_id,
            last_included_term: snapshot.last_included_term,
            last_included_index: snapshot.last_included_index,
        })?;
        self.clear_snapshot_restore_manifest()
    }

    pub(super) fn snapshot_restore_manifest_path(&self) -> PathBuf {
        self.config.data_dir.join("system").join("restore.pending")
    }

    pub(super) fn write_snapshot_restore_manifest(
        &self,
        shard_id: ShardId,
        snapshot: &neo4r_storage::LoadedSnapshot,
        bytes_observed: u64,
    ) -> DatabaseResult<()> {
        let path = self.snapshot_restore_manifest_path();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(StorageError::Io)?;
        }
        let tmp_path = path.with_extension("pending.tmp");
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp_path)
            .map_err(StorageError::Io)?;
        writeln!(
            file,
            "restore_snapshot_manifest:v1\t{}\t{}\t{}\t{}",
            shard_id, snapshot.last_included_term, snapshot.last_included_index, bytes_observed
        )
        .map_err(StorageError::Io)?;
        file.sync_all().map_err(StorageError::Io)?;
        drop(file);
        fs::rename(&tmp_path, &path).map_err(StorageError::Io)?;
        Ok(())
    }

    pub(super) fn load_snapshot_restore_manifest(
        &self,
    ) -> DatabaseResult<Option<(ShardId, LogIndex)>> {
        let path = self.snapshot_restore_manifest_path();
        let text = match fs::read_to_string(&path) {
            Ok(text) => text,
            Err(err) if err.kind() == ErrorKind::NotFound => return Ok(None),
            Err(err) => return Err(StorageError::Io(err).into()),
        };
        let parts = text.trim().split('\t').collect::<Vec<_>>();
        if parts.len() != 5 || parts[0] != "restore_snapshot_manifest:v1" {
            return Err(DatabaseError::InvalidConfig(
                "invalid pending restore manifest".to_string(),
            ));
        }
        let shard_id = parts[1].parse::<ShardId>().map_err(|_| {
            DatabaseError::InvalidConfig("invalid pending restore shard id".to_string())
        })?;
        let index = parts[3].parse::<LogIndex>().map_err(|_| {
            DatabaseError::InvalidConfig("invalid pending restore snapshot index".to_string())
        })?;
        Ok(Some((shard_id, index)))
    }

    pub(super) fn clear_snapshot_restore_manifest(&self) -> DatabaseResult<()> {
        let path = self.snapshot_restore_manifest_path();
        match fs::remove_file(path) {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == ErrorKind::NotFound => Ok(()),
            Err(err) => Err(StorageError::Io(err).into()),
        }
    }

    pub(super) fn shard_graph_state(
        &self,
        shard_id: ShardId,
    ) -> DatabaseResult<neo4r_core::GraphState> {
        let mut graph = neo4r_core::GraphState::new();
        for node in self.store.nodes()? {
            if self.shard_map.owner_of_node(node.id) == shard_id {
                graph.apply(Command::CreateNode {
                    id: node.id,
                    labels: node.labels,
                    properties: node.properties,
                })?;
            }
        }
        for boundary in self.store.boundary_nodes()? {
            if boundary.owner_shard == shard_id {
                graph.apply(Command::UpsertBoundaryNode {
                    id: boundary.id,
                    owner_shard: boundary.owner_shard,
                    labels: boundary.labels,
                    properties: boundary.properties,
                    version: boundary.version,
                })?;
            }
        }
        for relationship in self.store.relationships()? {
            if self.shard_map.owner_of_relationship(
                relationship.from,
                relationship.to,
                &relationship.rel_type,
            ) == shard_id
            {
                graph.apply(Command::CreateRelationship {
                    id: relationship.id,
                    from: relationship.from,
                    to: relationship.to,
                    rel_type: relationship.rel_type,
                    properties: relationship.properties,
                })?;
            }
        }
        Ok(graph)
    }

    pub(super) fn metadata_operations(&self) -> DatabaseResult<Vec<MetadataOperationRecord>> {
        self.metadata_log_store.load()
    }

    pub(super) fn query_access_plan(&self, query: &str, params: &QueryParams) -> QueryAccessPlan {
        match self.try_query_access_plan(query, params) {
            Ok(plan) => plan,
            Err(err) => QueryAccessPlan::Unsupported {
                reason: err.to_string(),
            },
        }
    }

    pub(super) fn try_query_access_plan(
        &self,
        query: &str,
        params: &QueryParams,
    ) -> DatabaseResult<QueryAccessPlan> {
        if let Some(parsed) = self.parsed_read_query_if_supported(query, params)? {
            if matches!(parsed.pattern, neo4r_query::Pattern::Outgoing { .. }) {
                return Ok(self.relationship_access_plan_from_parsed(&parsed.pattern));
            }
        }
        let input = strip_keyword(query.trim(), "MATCH")?;
        let (before_return, _) = split_keyword(input, "RETURN")
            .ok_or_else(|| write_parse_error("query requires RETURN"))?;
        let (pattern_part, predicate_part) = match split_keyword(before_return, "WHERE") {
            Some((pattern, predicate)) => (pattern.trim(), Some(predicate.trim())),
            None => (before_return.trim(), None),
        };
        if pattern_part.contains("->") {
            return Ok(self.relationship_access_plan(pattern_part));
        }
        let node = parse_node_pattern_write(pattern_part, params)?;
        if let Some(label) = node.labels.first() {
            if let Some(property) = self.best_indexed_node_property(label, node.properties.keys()) {
                return Ok(property);
            }
        }
        if let Some(predicate) = predicate_part {
            if let Some((property, metric)) =
                vector_predicate_for_variable(predicate, &node.variable)
            {
                if let Some(label) = node.labels.first() {
                    if self.has_vector_index(label, &property, &metric) {
                        return Ok(QueryAccessPlan::VectorIndexSeek {
                            label: Some(label.clone()),
                            property,
                            metric,
                        });
                    }
                }
            }
            if let Some(property) = property_predicate_for_variable(predicate, &node.variable) {
                if let Some(label) = node.labels.first() {
                    if self.has_unique_node_property_constraint(label, &property) {
                        return Ok(QueryAccessPlan::NodeUniqueIndexSeek {
                            label: label.clone(),
                            property,
                        });
                    }
                    if self.has_node_property_index(label, &property) {
                        return Ok(QueryAccessPlan::NodeIndexSeek {
                            label: label.clone(),
                            property,
                        });
                    }
                }
            }
        }
        if let Some(label) = node.labels.first() {
            Ok(QueryAccessPlan::NodeLabelScan {
                label: label.clone(),
            })
        } else {
            Ok(QueryAccessPlan::NodeFullScan)
        }
    }

    pub(super) fn parsed_read_query_if_supported(
        &self,
        query: &str,
        params: &QueryParams,
    ) -> Result<Option<neo4r_query::ParsedCypherQuery>, QueryError> {
        let parsed = match self.query_engine.parse(query, params) {
            Ok(parsed) => parsed,
            Err(QueryError::Parse(_)) | Err(QueryError::Unsupported(_)) => return Ok(None),
        };
        self.query_engine.analyze(query, params)?;
        Ok(Some(parsed))
    }

    pub(super) fn best_indexed_node_property<'a>(
        &self,
        label: &str,
        properties: impl Iterator<Item = &'a String>,
    ) -> Option<QueryAccessPlan> {
        let properties = properties.collect::<Vec<_>>();
        properties
            .iter()
            .find(|property| self.has_unique_node_property_constraint(label, property))
            .map(|property| QueryAccessPlan::NodeUniqueIndexSeek {
                label: label.to_string(),
                property: (*property).clone(),
            })
            .or_else(|| {
                properties
                    .iter()
                    .filter(|property| self.has_node_property_index(label, property))
                    .min_by_key(|property| {
                        estimate_indexed_property_rows(&self.statistics, label, property)
                    })
                    .map(|property| QueryAccessPlan::NodeIndexSeek {
                        label: label.to_string(),
                        property: (*property).clone(),
                    })
            })
    }

    pub(super) fn has_vector_index(&self, label: &str, property: &str, metric: &str) -> bool {
        self.index_catalog.indexes.iter().any(|index| {
            index.label == label
                && index.property == property
                && matches!(index.kind, IndexKind::Vector { .. })
                && vector_definition_parts(index)
                    .map(|(_, index_metric)| vector_metric_name(index_metric) == metric)
                    .unwrap_or(false)
        })
    }

    pub(super) fn relationship_access_plan(&self, pattern: &str) -> QueryAccessPlan {
        let compact = pattern
            .chars()
            .filter(|ch| !ch.is_whitespace())
            .collect::<String>();
        let Some((left, _)) = compact.split_once("->") else {
            return QueryAccessPlan::RelationshipScan;
        };
        let Some((_, rel_part)) = left.split_once("-") else {
            return QueryAccessPlan::RelationshipScan;
        };
        let Ok(inner) = strip_wrapping_write(rel_part, '[', ']') else {
            return QueryAccessPlan::RelationshipScan;
        };
        let rel_type = inner
            .split_once(':')
            .map(|(_, rel_type)| rel_type)
            .or_else(|| inner.strip_prefix(':'));
        match rel_type.filter(|rel_type| !rel_type.is_empty()) {
            Some(rel_type) => QueryAccessPlan::RelationshipTypeScan {
                rel_type: rel_type.to_string(),
            },
            None => QueryAccessPlan::RelationshipScan,
        }
    }

    pub(super) fn relationship_access_plan_from_parsed(
        &self,
        pattern: &neo4r_query::Pattern,
    ) -> QueryAccessPlan {
        match pattern {
            neo4r_query::Pattern::Outgoing {
                rel_type: Some(rel_type),
                ..
            } => QueryAccessPlan::RelationshipTypeScan {
                rel_type: rel_type.clone(),
            },
            neo4r_query::Pattern::Outgoing { .. } => QueryAccessPlan::RelationshipScan,
            neo4r_query::Pattern::Node(_) => QueryAccessPlan::Unsupported {
                reason: "expected relationship pattern".to_string(),
            },
        }
    }

    pub(super) fn has_unique_node_property_constraint(&self, label: &str, property: &str) -> bool {
        self.index_catalog.indexes.iter().any(|index| {
            matches!(index.kind, IndexKind::UniqueNodeProperty)
                && index.label == label
                && index.property == property
        })
    }

    pub(super) fn has_node_property_index(&self, label: &str, property: &str) -> bool {
        self.index_catalog.indexes.iter().any(|index| {
            matches!(index.kind, IndexKind::NodeProperty)
                && index.label == label
                && index.property == property
        })
    }

    pub(super) fn cluster_status(&self) -> ClusterStatus {
        let applied_indexes = self.applied_indexes();
        let committed_indexes = self.committed_indexes();
        let shards = self
            .routing_table
            .placements
            .iter()
            .map(|placement| {
                let primary_server_id = placement.primary_server_id();
                let shard_index = placement.shard_id as usize;
                ShardStatus {
                    shard_id: placement.shard_id,
                    primary_server_id,
                    replica_server_ids: placement
                        .replicas
                        .iter()
                        .filter(|replica| replica.server_id != primary_server_id.unwrap_or(0))
                        .map(|replica| replica.server_id)
                        .collect(),
                    has_local_copy: placement.has_server(self.config.server_id),
                    is_local_primary: primary_server_id == Some(self.config.server_id),
                    applied_index: applied_indexes.get(shard_index).copied().unwrap_or(0),
                    committed_index: committed_indexes.get(shard_index).copied().unwrap_or(0),
                    match_indexes: self
                        .match_indexes
                        .get(shard_index)
                        .map(|indexes| {
                            indexes
                                .iter()
                                .map(|(server_id, index)| (*server_id, *index))
                                .collect()
                        })
                        .unwrap_or_default(),
                }
            })
            .collect();
        ClusterStatus {
            server_id: self.config.server_id,
            routing_version: self.routing_table.version,
            shard_count: self.shard_map.shard_count(),
            local_partition_count: self.store.partition_count(),
            shards,
        }
    }
}
