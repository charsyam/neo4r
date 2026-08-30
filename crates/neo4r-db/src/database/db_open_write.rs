use super::metadata_types::*;
use super::staged_overlay::*;
use super::write_cypher_helpers::*;
use super::write_cypher_model::*;
use super::*;

impl Neo4rDatabase {
    pub fn open(config: DatabaseConfig) -> DatabaseResult<Self> {
        Self::open_with_replicator(config, Arc::new(NoopShardReplicator))
    }

    pub fn open_with_replicator(
        config: DatabaseConfig,
        replicator: Arc<dyn ShardReplicator>,
    ) -> DatabaseResult<Self> {
        validate_config(&config)?;
        let shard_map = ShardMap::new(config.shard_count).ok_or_else(|| {
            DatabaseError::InvalidConfig("shard count must be greater than zero".to_string())
        })?;
        let store =
            PartitionedGraphStore::open_rocks(&config.data_dir, config.local_partition_count)?;
        let logs = open_logs(&config)?;
        let checkpoints = open_checkpoints(&config)?;
        let commits = open_commits(&config)?;
        let shard_metadata = ShardMetadataStore::open(&config.data_dir)?;
        let membership_store = ClusterMembershipStore::open(&config.data_dir)?;
        let membership = load_or_initialize_membership(&config, &membership_store)?;
        let bootstrap_manifest_store = ClusterBootstrapManifestStore::open(&config.data_dir)?;
        let rebalance_plan_store = RebalancePlanStore::open(&config.data_dir)?;
        let rebalance_execution_store = RebalanceExecutionStore::open(&config.data_dir)?;
        let rebalance_execution = rebalance_execution_store.load()?;
        let cluster_metadata_store = ClusterMetadataStore::open(&config.data_dir)?;
        let metadata_log_store = MetadataOperationLogStore::open(&config.data_dir)?;
        let statistics_store = StatisticsCatalogStore::open(&config.data_dir)?;
        let index_catalog_store = IndexCatalogStore::open(&config.data_dir)?;
        let index_catalog = index_catalog_store.load()?.unwrap_or_default();
        let index_lifecycle_store = IndexLifecycleStore::open(&config.data_dir)?;
        let routing_table = load_or_initialize_routing_table(&config, &shard_metadata)?;
        let cluster_metadata =
            load_or_initialize_cluster_metadata(&config, &cluster_metadata_store, &routing_table)?;
        let statistics = statistics_store.load()?.unwrap_or_default();
        let commit_indexes = load_commit_indexes(&commits)?;
        let raft_groups = if config.raft_enabled {
            Some(RaftShardConsensus::open(
                &config,
                &routing_table,
                &logs,
                &commit_indexes,
            )?)
        } else {
            None
        };
        let vector_indexes = Arc::new(Mutex::new(PersistentVectorIndexes::default()));
        let query_engine = CypherEngine::with_vector_indexes(Arc::new(
            SharedVectorIndexProvider::new(vector_indexes.clone()),
        ));

        let mut database = Self {
            config,
            shard_map,
            store,
            logs,
            checkpoints,
            commits,
            shard_metadata,
            membership_store,
            membership,
            bootstrap_manifest_store,
            rebalance_plan_store,
            rebalance_execution_store,
            rebalance_execution,
            cluster_metadata_store,
            cluster_metadata,
            metadata_log_store,
            statistics_store,
            statistics,
            read_cache: Mutex::new(ReadPathCache::default()),
            read_cache_stats: Mutex::new(ReadCacheStats::default()),
            index_catalog_store,
            index_catalog,
            index_lifecycle_store,
            vector_indexes,
            routing_table,
            next_log_indexes: vec![1; shard_map.shard_count() as usize],
            commit_indexes,
            match_indexes: vec![BTreeMap::new(); shard_map.shard_count() as usize],
            next_node_id: 0,
            next_relationship_id: 0,
            clock: HybridClock::new(),
            query_engine,
            replicator,
            raft_groups,
        };
        database.replay_logs()?;
        database.recover_allocators_from_store()?;
        database.load_or_rebuild_vector_indexes()?;
        database.recover_pending_snapshot_restore()?;
        database.resume_index_builds()?;
        Ok(database)
    }

    pub fn open_path(
        data_dir: impl AsRef<Path>,
        shard_count: u64,
        local_partition_count: usize,
    ) -> DatabaseResult<Self> {
        Self::open(DatabaseConfig::new(
            data_dir.as_ref(),
            shard_count,
            local_partition_count,
        ))
    }

    pub fn create_node(
        &mut self,
        labels: Vec<String>,
        properties: Properties,
    ) -> DatabaseResult<NodeId> {
        let id = self.allocate_node_id();
        let command = Command::CreateNode {
            id,
            labels,
            properties,
        };
        let shard_id = self.shard_map.owner_of_node(id);
        self.write_command(shard_id, command)?;
        Ok(id)
    }

    pub fn create_relationship(
        &mut self,
        from: NodeId,
        to: NodeId,
        rel_type: String,
        properties: Properties,
    ) -> DatabaseResult<RelationshipId> {
        self.ensure_local_node_exists(from)?;
        self.ensure_node_or_boundary_exists(to)?;
        let id = self.allocate_relationship_id();
        let shard_id = self.shard_map.owner_of_relationship(from, to, &rel_type);
        let command = Command::CreateRelationship {
            id,
            from,
            to,
            rel_type,
            properties,
        };
        self.write_command(shard_id, command)?;
        Ok(id)
    }

    pub fn set_node_property(
        &mut self,
        id: NodeId,
        key: String,
        value: Value,
    ) -> DatabaseResult<()> {
        self.ensure_local_node_exists(id)?;
        let shard_id = self.shard_map.owner_of_node(id);
        self.write_command(shard_id, Command::SetNodeProperty { id, key, value })
    }

    pub fn remove_node_property(&mut self, id: NodeId, key: String) -> DatabaseResult<()> {
        self.ensure_local_node_exists(id)?;
        let shard_id = self.shard_map.owner_of_node(id);
        self.write_command(shard_id, Command::RemoveNodeProperty { id, key })
    }

    pub fn add_node_label(&mut self, id: NodeId, label: String) -> DatabaseResult<()> {
        self.ensure_local_node_exists(id)?;
        let shard_id = self.shard_map.owner_of_node(id);
        self.write_command(shard_id, Command::AddNodeLabel { id, label })
    }

    pub fn remove_node_label(&mut self, id: NodeId, label: String) -> DatabaseResult<()> {
        self.ensure_local_node_exists(id)?;
        let shard_id = self.shard_map.owner_of_node(id);
        self.write_command(shard_id, Command::RemoveNodeLabel { id, label })
    }

    pub fn set_relationship_property(
        &mut self,
        id: RelationshipId,
        key: String,
        value: Value,
    ) -> DatabaseResult<()> {
        let shard_id = self.relationship_owner_shard(id)?;
        self.write_command(
            shard_id,
            Command::SetRelationshipProperty { id, key, value },
        )
    }

    pub fn remove_relationship_property(
        &mut self,
        id: RelationshipId,
        key: String,
    ) -> DatabaseResult<()> {
        let shard_id = self.relationship_owner_shard(id)?;
        self.write_command(shard_id, Command::RemoveRelationshipProperty { id, key })
    }

    pub fn delete_relationship(&mut self, id: RelationshipId) -> DatabaseResult<()> {
        let shard_id = self.relationship_owner_shard(id)?;
        self.write_command(shard_id, Command::DeleteRelationship { id })
    }

    pub fn delete_node(&mut self, id: NodeId) -> DatabaseResult<()> {
        self.ensure_local_node_exists(id)?;
        let shard_id = self.shard_map.owner_of_node(id);
        self.write_command(shard_id, Command::DeleteNode { id })
    }

    pub fn execute_cypher(&mut self, query: &str) -> DatabaseResult<Vec<QueryRow>> {
        self.execute_cypher_with_params(query, &QueryParams::new())
    }

    pub fn execute_cypher_with_params(
        &mut self,
        query: &str,
        params: &QueryParams,
    ) -> DatabaseResult<Vec<QueryRow>> {
        self.execute_write_cypher_on_optional_shard(query, params, None)
    }

    pub(super) fn execute_cypher_on_shard(
        &mut self,
        shard_id: ShardId,
        query: &str,
        params: &QueryParams,
    ) -> DatabaseResult<Vec<QueryRow>> {
        self.validate_shard_id(shard_id)?;
        self.execute_write_cypher_on_optional_shard(query, params, Some(shard_id))
    }

    pub(super) fn write_cypher_target_shards(
        &mut self,
        query: &str,
        params: &QueryParams,
    ) -> DatabaseResult<Vec<ShardId>> {
        let mut shards = BTreeSet::new();
        match parse_write_cypher(query, params)? {
            Some(WriteCypher::CreateNode { .. })
            | Some(WriteCypher::CreateNodeThenRelationship { .. }) => {
                for status in self.cluster_status().shards {
                    if status.is_local_primary {
                        shards.insert(status.shard_id);
                    }
                }
                if shards.is_empty() {
                    shards.insert(0);
                }
            }
            Some(WriteCypher::CreateRelationship { from_matcher, .. }) => {
                for from in self.match_node_ids(&from_matcher, params)? {
                    shards.insert(self.shard_map.owner_of_node(from));
                }
            }
            Some(WriteCypher::MergeNode { .. }) => {
                for status in self.cluster_status().shards {
                    if status.is_local_primary {
                        shards.insert(status.shard_id);
                    }
                }
                if shards.is_empty() {
                    shards.insert(0);
                }
            }
            Some(WriteCypher::MergeRelationship { from_matcher, .. }) => {
                for from in self.match_node_ids(&from_matcher, params)? {
                    shards.insert(self.shard_map.owner_of_node(from));
                }
            }
            Some(WriteCypher::SetNodeProperty { matcher, .. })
            | Some(WriteCypher::ReplaceNodeProperties { matcher, .. })
            | Some(WriteCypher::RemoveNodeProperty { matcher, .. })
            | Some(WriteCypher::AddNodeLabel { matcher, .. })
            | Some(WriteCypher::RemoveNodeLabel { matcher, .. })
            | Some(WriteCypher::DeleteNode { matcher, .. }) => {
                for id in self.match_node_ids(&matcher, params)? {
                    shards.insert(self.shard_map.owner_of_node(id));
                }
            }
            Some(WriteCypher::SetRelationshipProperty { matcher, .. })
            | Some(WriteCypher::ReplaceRelationshipProperties { matcher, .. })
            | Some(WriteCypher::RemoveRelationshipProperty { matcher, .. })
            | Some(WriteCypher::DeleteRelationship { matcher, .. }) => {
                for id in self.match_relationship_ids(&matcher, params)? {
                    shards.insert(self.relationship_owner_shard(id)?);
                }
            }
            Some(WriteCypher::CreateNodePropertyIndex { .. })
            | Some(WriteCypher::CreateUniqueNodePropertyConstraint { .. })
            | Some(WriteCypher::CreateVectorIndex { .. })
            | Some(WriteCypher::RebuildVectorIndex { .. })
            | Some(WriteCypher::DropIndex { .. })
            | Some(WriteCypher::DropConstraint { .. }) => {
                shards.insert(0);
            }
            None => {}
        }
        Ok(shards.into_iter().collect())
    }

    pub(super) fn execute_cypher_mutation_batch_on_shard(
        &mut self,
        shard_id: ShardId,
        writes: Vec<(String, QueryParams)>,
    ) -> DatabaseResult<usize> {
        self.validate_shard_id(shard_id)?;
        self.execute_cypher_mutation_batch_inner(writes, Some(shard_id))
    }

    pub(super) fn execute_cypher_mutation_batch(
        &mut self,
        writes: Vec<(String, QueryParams)>,
    ) -> DatabaseResult<usize> {
        self.execute_cypher_mutation_batch_inner(writes, None)
    }

    pub(super) fn execute_staged_cypher_transaction_on_shard(
        &mut self,
        shard_id: ShardId,
        writes: Vec<(String, QueryParams)>,
    ) -> DatabaseResult<usize> {
        self.validate_shard_id(shard_id)?;
        let snapshot = self.read_snapshot()?;
        let overlay = snapshot.build_staged_overlay(&writes)?;
        let commands =
            self.commands_from_staged_overlay_on_shard(shard_id, &snapshot.store, overlay)?;
        let mut entries = Vec::with_capacity(commands.len());
        for command in commands {
            entries.push(self.append_local_command(shard_id, command, false)?);
        }
        if entries.is_empty() {
            return Ok(0);
        }
        self.flush_group_commit(&entries)?;
        Ok(entries.len())
    }

    pub(super) fn commands_from_staged_overlay_on_shard(
        &mut self,
        target_shard: ShardId,
        base: &PartitionedGraphStore<RocksKvSnapshot>,
        overlay: StagedOverlay,
    ) -> DatabaseResult<Vec<Command>> {
        let mut commands = Vec::new();
        let mut temp_node_id_map = HashMap::new();

        let mut temp_nodes = overlay
            .temp_node_ids
            .iter()
            .filter_map(|id| overlay.nodes.get(id).and_then(|node| node.as_ref()))
            .cloned()
            .collect::<Vec<_>>();
        temp_nodes.sort_by(|left, right| right.id.cmp(&left.id));
        for node in temp_nodes {
            let id = self.allocate_node_id_for_shard(target_shard);
            temp_node_id_map.insert(node.id, id);
            commands.push(Command::CreateNode {
                id,
                labels: node.labels,
                properties: node.properties,
            });
        }

        let mut temp_relationships = overlay
            .temp_relationship_ids
            .iter()
            .filter_map(|id| {
                overlay
                    .relationships
                    .get(id)
                    .and_then(|relationship| relationship.as_ref())
            })
            .cloned()
            .collect::<Vec<_>>();
        temp_relationships.sort_by(|left, right| right.id.cmp(&left.id));
        for relationship in temp_relationships {
            let from = temp_node_id_map
                .get(&relationship.from)
                .copied()
                .unwrap_or(relationship.from);
            let to = temp_node_id_map
                .get(&relationship.to)
                .copied()
                .unwrap_or(relationship.to);
            let owner = self
                .shard_map
                .owner_of_relationship(from, to, &relationship.rel_type);
            if owner != target_shard {
                return Err(DatabaseError::InvalidConfig(format!(
                    "staged relationship CREATE targets shard {owner}, expected {target_shard}"
                )));
            }
            commands.push(Command::CreateRelationship {
                id: self.allocate_relationship_id(),
                from,
                to,
                rel_type: relationship.rel_type,
                properties: relationship.properties,
            });
        }

        let mut base_node_ids = overlay
            .nodes
            .keys()
            .filter(|id| !overlay.temp_node_ids.contains(id))
            .copied()
            .collect::<Vec<_>>();
        base_node_ids.sort_unstable();
        for id in base_node_ids {
            if self.shard_map.owner_of_node(id) != target_shard {
                continue;
            }
            match overlay.nodes.get(&id) {
                Some(Some(node)) => {
                    let Some(base_node) = base.node(id)? else {
                        return Err(DatabaseError::Graph(GraphError::NodeNotFound(id)));
                    };
                    append_property_delta_commands(
                        &mut commands,
                        &base_node.properties,
                        &node.properties,
                        |key, value| Command::SetNodeProperty { id, key, value },
                        |key| Command::RemoveNodeProperty { id, key },
                    );
                    append_label_delta_commands(
                        &mut commands,
                        &base_node.labels,
                        &node.labels,
                        |label| Command::AddNodeLabel { id, label },
                        |label| Command::RemoveNodeLabel { id, label },
                    );
                }
                Some(None) => {
                    if base.node(id)?.is_some() {
                        commands.push(Command::DeleteNode { id });
                    }
                }
                None => {}
            }
        }

        let mut base_relationship_ids = overlay
            .relationships
            .keys()
            .filter(|id| !overlay.temp_relationship_ids.contains(id))
            .copied()
            .collect::<Vec<_>>();
        base_relationship_ids.sort_unstable();
        for id in base_relationship_ids {
            let Some(base_relationship) = base.relationship(id)? else {
                continue;
            };
            let owner = self.shard_map.owner_of_relationship(
                base_relationship.from,
                base_relationship.to,
                &base_relationship.rel_type,
            );
            if owner != target_shard {
                continue;
            }
            match overlay.relationships.get(&id) {
                Some(Some(relationship)) => {
                    append_property_delta_commands(
                        &mut commands,
                        &base_relationship.properties,
                        &relationship.properties,
                        |key, value| Command::SetRelationshipProperty { id, key, value },
                        |key| Command::RemoveRelationshipProperty { id, key },
                    );
                }
                Some(None) => commands.push(Command::DeleteRelationship { id }),
                None => {}
            }
        }

        Ok(commands)
    }

    pub(super) fn execute_cypher_mutation_batch_inner(
        &mut self,
        writes: Vec<(String, QueryParams)>,
        target_shard: Option<ShardId>,
    ) -> DatabaseResult<usize> {
        let mut commands = Vec::new();
        for (query, params) in writes {
            match parse_write_cypher(&query, &params)? {
                Some(WriteCypher::CreateNode {
                    labels,
                    properties,
                    assignments,
                    replacement,
                    ..
                }) => {
                    let properties =
                        create_properties_after_set(properties, assignments, replacement);
                    let shard_id = target_shard.ok_or_else(|| {
                        DatabaseError::InvalidConfig(
                            "batched CREATE node requires an explicit target shard".to_string(),
                        )
                    })?;
                    let id = self.allocate_node_id_for_shard(shard_id);
                    commands.push((
                        shard_id,
                        Command::CreateNode {
                            id,
                            labels,
                            properties,
                        },
                    ));
                }
                Some(WriteCypher::MergeNode {
                    labels,
                    properties,
                    on_create,
                    on_create_replacement,
                    on_match,
                    on_match_replacement,
                    ..
                }) => {
                    let shard_id = target_shard.ok_or_else(|| {
                        DatabaseError::InvalidConfig(
                            "batched MERGE node requires an explicit target shard".to_string(),
                        )
                    })?;
                    if let Some(node) =
                        self.find_merge_node(&labels, &properties, Some(shard_id))?
                    {
                        if let Some(replacement) = on_match_replacement.as_ref() {
                            for key in property_removes(&node.properties, replacement) {
                                commands.push((
                                    shard_id,
                                    Command::RemoveNodeProperty { id: node.id, key },
                                ));
                            }
                            for (key, value) in property_sets(&node.properties, replacement) {
                                commands.push((
                                    shard_id,
                                    Command::SetNodeProperty {
                                        id: node.id,
                                        key,
                                        value,
                                    },
                                ));
                            }
                        } else {
                            for assignment in &on_match {
                                if matches!(assignment.value, Value::Null) {
                                    commands.push((
                                        shard_id,
                                        Command::RemoveNodeProperty {
                                            id: node.id,
                                            key: assignment.key.clone(),
                                        },
                                    ));
                                } else {
                                    commands.push((
                                        shard_id,
                                        Command::SetNodeProperty {
                                            id: node.id,
                                            key: assignment.key.clone(),
                                            value: assignment.value.clone(),
                                        },
                                    ));
                                }
                            }
                        }
                    } else {
                        let create_properties = properties_after_set(
                            properties.clone(),
                            &on_create,
                            on_create_replacement.as_ref(),
                        );
                        let id = self.allocate_node_id_for_shard(shard_id);
                        commands.push((
                            shard_id,
                            Command::CreateNode {
                                id,
                                labels,
                                properties: create_properties,
                            },
                        ));
                    }
                }
                Some(WriteCypher::CreateRelationship {
                    from_matcher,
                    to_matcher,
                    rel_type,
                    properties,
                    assignments,
                    replacement,
                    ..
                }) => {
                    let properties =
                        create_properties_after_set(properties, assignments, replacement);
                    let from_ids = self.match_node_ids(&from_matcher, &params)?;
                    let to_ids = self.match_node_ids(&to_matcher, &params)?;
                    for from in &from_ids {
                        self.ensure_local_node_exists(*from)?;
                        for to in &to_ids {
                            self.ensure_node_or_boundary_exists(*to)?;
                            let shard_id =
                                self.shard_map.owner_of_relationship(*from, *to, &rel_type);
                            if matches_target_shard(target_shard, shard_id) {
                                let id = self.allocate_relationship_id();
                                commands.push((
                                    shard_id,
                                    Command::CreateRelationship {
                                        id,
                                        from: *from,
                                        to: *to,
                                        rel_type: rel_type.clone(),
                                        properties: properties.clone(),
                                    },
                                ));
                            }
                        }
                    }
                }
                Some(WriteCypher::MergeRelationship {
                    from_matcher,
                    to_matcher,
                    rel_type,
                    properties,
                    on_create,
                    on_create_replacement,
                    on_match,
                    on_match_replacement,
                    ..
                }) => {
                    let from_ids = self.match_node_ids(&from_matcher, &params)?;
                    let to_ids = self.match_node_ids(&to_matcher, &params)?;
                    for from in &from_ids {
                        self.ensure_local_node_exists(*from)?;
                        for to in &to_ids {
                            self.ensure_node_or_boundary_exists(*to)?;
                            let shard_id =
                                self.shard_map.owner_of_relationship(*from, *to, &rel_type);
                            if !matches_target_shard(target_shard, shard_id) {
                                continue;
                            }
                            if let Some(relationship) =
                                self.find_merge_relationship(*from, *to, &rel_type, &properties)?
                            {
                                if let Some(replacement) = on_match_replacement.as_ref() {
                                    for key in
                                        property_removes(&relationship.properties, replacement)
                                    {
                                        commands.push((
                                            shard_id,
                                            Command::RemoveRelationshipProperty {
                                                id: relationship.id,
                                                key,
                                            },
                                        ));
                                    }
                                    for (key, value) in
                                        property_sets(&relationship.properties, replacement)
                                    {
                                        commands.push((
                                            shard_id,
                                            Command::SetRelationshipProperty {
                                                id: relationship.id,
                                                key,
                                                value,
                                            },
                                        ));
                                    }
                                } else {
                                    for assignment in &on_match {
                                        if matches!(assignment.value, Value::Null) {
                                            commands.push((
                                                shard_id,
                                                Command::RemoveRelationshipProperty {
                                                    id: relationship.id,
                                                    key: assignment.key.clone(),
                                                },
                                            ));
                                        } else {
                                            commands.push((
                                                shard_id,
                                                Command::SetRelationshipProperty {
                                                    id: relationship.id,
                                                    key: assignment.key.clone(),
                                                    value: assignment.value.clone(),
                                                },
                                            ));
                                        }
                                    }
                                }
                            } else {
                                let create_properties = properties_after_set(
                                    properties.clone(),
                                    &on_create,
                                    on_create_replacement.as_ref(),
                                );
                                let id = self.allocate_relationship_id();
                                commands.push((
                                    shard_id,
                                    Command::CreateRelationship {
                                        id,
                                        from: *from,
                                        to: *to,
                                        rel_type: rel_type.clone(),
                                        properties: create_properties,
                                    },
                                ));
                            }
                        }
                    }
                }
                Some(WriteCypher::SetNodeProperty {
                    matcher,
                    assignments,
                    ..
                }) => {
                    for id in self.match_node_ids(&matcher, &params)? {
                        let shard_id = self.shard_map.owner_of_node(id);
                        if matches_target_shard(target_shard, shard_id) {
                            for assignment in &assignments {
                                if matches!(assignment.value, Value::Null) {
                                    commands.push((
                                        shard_id,
                                        Command::RemoveNodeProperty {
                                            id,
                                            key: assignment.key.clone(),
                                        },
                                    ));
                                } else {
                                    commands.push((
                                        shard_id,
                                        Command::SetNodeProperty {
                                            id,
                                            key: assignment.key.clone(),
                                            value: assignment.value.clone(),
                                        },
                                    ));
                                }
                            }
                        }
                    }
                }
                Some(WriteCypher::ReplaceNodeProperties {
                    matcher,
                    properties,
                    ..
                }) => {
                    for id in self.match_node_ids(&matcher, &params)? {
                        let shard_id = self.shard_map.owner_of_node(id);
                        if matches_target_shard(target_shard, shard_id) {
                            let current = self
                                .node(id)?
                                .ok_or(DatabaseError::Graph(GraphError::NodeNotFound(id)))?;
                            for key in property_removes(&current.properties, &properties) {
                                commands.push((shard_id, Command::RemoveNodeProperty { id, key }));
                            }
                            for (key, value) in property_sets(&current.properties, &properties) {
                                commands
                                    .push((shard_id, Command::SetNodeProperty { id, key, value }));
                            }
                        }
                    }
                }
                Some(WriteCypher::RemoveNodeProperty { matcher, keys, .. }) => {
                    for id in self.match_node_ids(&matcher, &params)? {
                        let shard_id = self.shard_map.owner_of_node(id);
                        if matches_target_shard(target_shard, shard_id) {
                            for key in &keys {
                                commands.push((
                                    shard_id,
                                    Command::RemoveNodeProperty {
                                        id,
                                        key: key.clone(),
                                    },
                                ));
                            }
                        }
                    }
                }
                Some(WriteCypher::AddNodeLabel {
                    matcher, labels, ..
                }) => {
                    for id in self.match_node_ids(&matcher, &params)? {
                        let shard_id = self.shard_map.owner_of_node(id);
                        if matches_target_shard(target_shard, shard_id) {
                            for label in &labels {
                                commands.push((
                                    shard_id,
                                    Command::AddNodeLabel {
                                        id,
                                        label: label.clone(),
                                    },
                                ));
                            }
                        }
                    }
                }
                Some(WriteCypher::RemoveNodeLabel {
                    matcher, labels, ..
                }) => {
                    for id in self.match_node_ids(&matcher, &params)? {
                        let shard_id = self.shard_map.owner_of_node(id);
                        if matches_target_shard(target_shard, shard_id) {
                            for label in &labels {
                                commands.push((
                                    shard_id,
                                    Command::RemoveNodeLabel {
                                        id,
                                        label: label.clone(),
                                    },
                                ));
                            }
                        }
                    }
                }
                Some(WriteCypher::SetRelationshipProperty {
                    matcher,
                    assignments,
                    ..
                }) => {
                    for id in self.match_relationship_ids(&matcher, &params)? {
                        let shard_id = self.relationship_owner_shard(id)?;
                        if matches_target_shard(target_shard, shard_id) {
                            for assignment in &assignments {
                                if matches!(assignment.value, Value::Null) {
                                    commands.push((
                                        shard_id,
                                        Command::RemoveRelationshipProperty {
                                            id,
                                            key: assignment.key.clone(),
                                        },
                                    ));
                                } else {
                                    commands.push((
                                        shard_id,
                                        Command::SetRelationshipProperty {
                                            id,
                                            key: assignment.key.clone(),
                                            value: assignment.value.clone(),
                                        },
                                    ));
                                }
                            }
                        }
                    }
                }
                Some(WriteCypher::ReplaceRelationshipProperties {
                    matcher,
                    properties,
                    ..
                }) => {
                    for id in self.match_relationship_ids(&matcher, &params)? {
                        let shard_id = self.relationship_owner_shard(id)?;
                        if matches_target_shard(target_shard, shard_id) {
                            let current = self.relationship(id)?.ok_or(DatabaseError::Graph(
                                GraphError::RelationshipNotFound(id),
                            ))?;
                            for key in property_removes(&current.properties, &properties) {
                                commands.push((
                                    shard_id,
                                    Command::RemoveRelationshipProperty { id, key },
                                ));
                            }
                            for (key, value) in property_sets(&current.properties, &properties) {
                                commands.push((
                                    shard_id,
                                    Command::SetRelationshipProperty { id, key, value },
                                ));
                            }
                        }
                    }
                }
                Some(WriteCypher::RemoveRelationshipProperty { matcher, keys, .. }) => {
                    for id in self.match_relationship_ids(&matcher, &params)? {
                        let shard_id = self.relationship_owner_shard(id)?;
                        if matches_target_shard(target_shard, shard_id) {
                            for key in &keys {
                                commands.push((
                                    shard_id,
                                    Command::RemoveRelationshipProperty {
                                        id,
                                        key: key.clone(),
                                    },
                                ));
                            }
                        }
                    }
                }
                Some(WriteCypher::DeleteNode { matcher, .. }) => {
                    for id in self.match_node_ids(&matcher, &params)? {
                        let shard_id = self.shard_map.owner_of_node(id);
                        if matches_target_shard(target_shard, shard_id) {
                            commands.push((shard_id, Command::DeleteNode { id }));
                        }
                    }
                }
                Some(WriteCypher::DeleteRelationship { matcher, .. }) => {
                    for id in self.match_relationship_ids(&matcher, &params)? {
                        let shard_id = self.relationship_owner_shard(id)?;
                        if matches_target_shard(target_shard, shard_id) {
                            commands.push((shard_id, Command::DeleteRelationship { id }));
                        }
                    }
                }
                Some(WriteCypher::CreateNodePropertyIndex { .. })
                | Some(WriteCypher::CreateUniqueNodePropertyConstraint { .. })
                | Some(WriteCypher::CreateVectorIndex { .. })
                | Some(WriteCypher::RebuildVectorIndex { .. })
                | Some(WriteCypher::DropIndex { .. })
                | Some(WriteCypher::DropConstraint { .. })
                | Some(WriteCypher::CreateNodeThenRelationship { .. }) => {
                    return Err(DatabaseError::InvalidConfig(
                        "Cypher mutation batch only supports CREATE, MERGE, SET, REMOVE, and DELETE writes"
                            .to_string(),
                    ));
                }
                None => {
                    return Err(DatabaseError::InvalidConfig(
                        "Cypher mutation batch requires write queries".to_string(),
                    ));
                }
            }
        }

        let mut entries = Vec::with_capacity(commands.len());
        let mut shards = BTreeSet::new();
        for (shard_id, _) in &commands {
            shards.insert(*shard_id);
        }
        for shard_id in shards {
            self.ensure_local_primary(shard_id)?;
        }
        for (shard_id, command) in commands {
            entries.push(self.append_local_command(shard_id, command, false)?);
        }
        if entries.is_empty() {
            return Ok(0);
        }
        self.flush_group_commit(&entries)?;
        Ok(entries.len())
    }
}
