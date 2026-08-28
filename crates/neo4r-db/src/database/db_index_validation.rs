impl Neo4rDatabase {
    fn rebuild_raft_groups(&mut self) -> DatabaseResult<()> {
        if self.raft_groups.is_none() {
            return Ok(());
        }
        self.raft_groups = Some(RaftShardConsensus::open(
            &self.config,
            &self.routing_table,
            &self.logs,
            &self.commit_indexes,
        )?);
        Ok(())
    }

    fn begin_joint_consensus_for_routing(
        &mut self,
        routing_table: &ShardRoutingTable,
    ) -> DatabaseResult<()> {
        let Some(raft_groups) = self.raft_groups.as_mut() else {
            return Ok(());
        };
        for placement in &routing_table.placements {
            let voters = placement
                .replicas
                .iter()
                .map(|replica| replica.server_id)
                .collect::<Vec<_>>();
            if let Some(group) = raft_groups.groups.get_mut(placement.shard_id as usize) {
                group.begin_joint_consensus(voters)?;
            }
        }
        Ok(())
    }

    fn finalize_joint_consensus_for_routing(
        &mut self,
        routing_table: &ShardRoutingTable,
    ) -> DatabaseResult<()> {
        let Some(raft_groups) = self.raft_groups.as_mut() else {
            return Ok(());
        };
        for placement in &routing_table.placements {
            if let Some(group) = raft_groups.groups.get_mut(placement.shard_id as usize) {
                group.finalize_joint_consensus();
            }
        }
        Ok(())
    }

    fn append_replicated_config_change_phase(
        &mut self,
        phase: &str,
        description: &str,
        routing_table: &ShardRoutingTable,
    ) -> DatabaseResult<()> {
        if self.raft_groups.is_none() {
            return Ok(());
        }
        let voters = routing_table
            .placements
            .iter()
            .flat_map(|placement| placement.replicas.iter().map(|replica| replica.server_id))
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        self.write_command(
            0,
            Command::ClusterConfigChange {
                phase: phase.to_string(),
                description: format!("{description}:version={}", routing_table.version),
                voters,
                routing_table: routing_table.clone(),
            },
        )
    }

    fn add_index_definition(&mut self, index: IndexDefinition) -> DatabaseResult<()> {
        validate_index_definition(&index)?;
        if self
            .index_catalog
            .indexes
            .iter()
            .any(|existing| existing.name == index.name)
        {
            return Err(DatabaseError::InvalidConfig(format!(
                "index {:?} already exists",
                index.name
            )));
        }
        if matches!(index.kind, IndexKind::Vector { .. })
            && self.index_catalog.indexes.iter().any(|existing| {
                matches!(existing.kind, IndexKind::Vector { .. })
                    && existing.label == index.label
                    && existing.property == index.property
                    && vector_definition_parts(existing).map(|(_, metric)| metric)
                        == vector_definition_parts(&index).map(|(_, metric)| metric)
            })
        {
            return Err(DatabaseError::InvalidConfig(format!(
                "vector index already exists for label {:?} property {:?}",
                index.label, index.property
            )));
        }
        if matches!(index.kind, IndexKind::UniqueNodeProperty)
            && self.index_catalog.indexes.iter().any(|existing| {
                matches!(existing.kind, IndexKind::UniqueNodeProperty)
                    && existing.label == index.label
                    && existing.property == index.property
            })
        {
            return Err(DatabaseError::InvalidConfig(format!(
                "unique constraint already exists for label {:?} property {:?}",
                index.label, index.property
            )));
        }
        if matches!(index.kind, IndexKind::UniqueNodeProperty) {
            self.validate_existing_unique_node_property_constraint(&index)?;
        }
        self.save_index_lifecycle_state(&index, "building", 0, "")?;
        let backfilled_entries = self.index_backfilled_entries(&index)?;
        if matches!(index.kind, IndexKind::Vector { .. }) {
            self.validate_existing_vector_index_values(&index)?;
            let nodes = self.store.nodes()?;
            self.vector_indexes
                .lock()
                .map_err(|_| DatabaseError::LockPoisoned)?
                .insert_definition(&index, &nodes);
        }
        let lifecycle_index = index.clone();
        self.index_catalog.indexes.push(index);
        self.index_catalog.version += 1;
        self.index_catalog_store.save(&self.index_catalog)?;
        self.save_vector_index_cache()?;
        self.save_index_lifecycle_state(&lifecycle_index, "ready", backfilled_entries, "")?;
        Ok(())
    }

    fn add_index_definition_if_not_exists(&mut self, index: IndexDefinition) -> DatabaseResult<()> {
        validate_index_definition(&index)?;
        if let Some(existing) = self
            .index_catalog
            .indexes
            .iter()
            .find(|existing| existing.name == index.name)
        {
            if existing == &index {
                return Ok(());
            }
            return Err(DatabaseError::InvalidConfig(format!(
                "index {:?} already exists with a different definition",
                index.name
            )));
        }
        self.add_index_definition(index)
    }

    pub fn rebuild_vector_indexes(&mut self) -> DatabaseResult<()> {
        for index in self.index_catalog.indexes.clone() {
            if matches!(index.kind, IndexKind::Vector { .. }) {
                self.save_index_lifecycle_state(&index, "rebuilding", 0, "")?;
            }
        }
        let indexes = self.build_vector_indexes_for_catalog(&self.index_catalog)?;
        *self
            .vector_indexes
            .lock()
            .map_err(|_| DatabaseError::LockPoisoned)? = indexes;
        self.save_vector_index_cache()?;
        for index in self.index_catalog.indexes.clone() {
            if matches!(index.kind, IndexKind::Vector { .. }) {
                let backfilled_entries = self.index_backfilled_entries(&index)?;
                self.save_index_lifecycle_state(&index, "ready", backfilled_entries, "")?;
            }
        }
        Ok(())
    }

    pub fn rebuild_vector_index(&mut self, name: &str) -> DatabaseResult<()> {
        let Some(definition) = self
            .index_catalog
            .indexes
            .iter()
            .find(|index| index.name == name)
            .cloned()
        else {
            return Err(DatabaseError::InvalidConfig(format!(
                "vector index {name:?} does not exist"
            )));
        };
        if !matches!(definition.kind, IndexKind::Vector { .. }) {
            return Err(DatabaseError::InvalidConfig(format!(
                "index {name:?} is not a vector index"
            )));
        }
        self.save_index_lifecycle_state(&definition, "rebuilding", 0, "")?;
        self.validate_existing_vector_index_values(&definition)?;
        let nodes = self.store.nodes()?;
        let mut indexes = self
            .vector_indexes
            .lock()
            .map_err(|_| DatabaseError::LockPoisoned)?;
        indexes.remove(name);
        indexes.insert_definition(&definition, &nodes);
        drop(indexes);
        self.save_vector_index_cache()?;
        self.save_index_lifecycle_state(
            &definition,
            "ready",
            self.index_backfilled_entries(&definition)?,
            "",
        )?;
        Ok(())
    }

    fn resume_index_builds(&mut self) -> DatabaseResult<()> {
        let persisted = self.index_lifecycle_store.load()?;
        let resumable = persisted
            .into_iter()
            .filter(|status| status.state == "building" || status.state == "rebuilding")
            .map(|status| status.name)
            .collect::<Vec<_>>();
        for name in resumable {
            let Some(definition) = self
                .index_catalog
                .indexes
                .iter()
                .find(|index| index.name == name)
                .cloned()
            else {
                continue;
            };
            if matches!(definition.kind, IndexKind::Vector { .. }) {
                self.rebuild_vector_index(&definition.name)?;
            } else {
                let backfilled_entries = self.index_backfilled_entries(&definition)?;
                self.save_index_lifecycle_state(&definition, "ready", backfilled_entries, "")?;
            }
        }
        Ok(())
    }

    fn build_vector_indexes_for_catalog(
        &self,
        catalog: &IndexCatalog,
    ) -> DatabaseResult<PersistentVectorIndexes> {
        let nodes = self.store.nodes()?;
        let mut indexes = PersistentVectorIndexes::default();
        for definition in &catalog.indexes {
            indexes.insert_definition(definition, &nodes);
        }
        Ok(indexes)
    }

    fn validate_index_catalog_against_store(&self, catalog: &IndexCatalog) -> DatabaseResult<()> {
        for index in &catalog.indexes {
            match index.kind {
                IndexKind::UniqueNodeProperty => {
                    self.validate_existing_unique_node_property_constraint(index)?;
                }
                IndexKind::Vector { .. } => {
                    self.validate_existing_vector_index_values(index)?;
                }
                IndexKind::NodeProperty => {}
            }
        }
        Ok(())
    }

    fn load_or_rebuild_vector_indexes(&mut self) -> DatabaseResult<()> {
        match load_vector_index_cache(self.vector_index_cache_path(), &self.index_catalog)? {
            Some(indexes) => {
                *self
                    .vector_indexes
                    .lock()
                    .map_err(|_| DatabaseError::LockPoisoned)? = indexes;
                Ok(())
            }
            None => self.rebuild_vector_indexes(),
        }
    }

    fn save_vector_index_cache(&self) -> DatabaseResult<()> {
        let indexes = self
            .vector_indexes
            .lock()
            .map_err(|_| DatabaseError::LockPoisoned)?;
        save_vector_index_cache(
            self.vector_index_cache_path(),
            &self.index_catalog,
            &indexes,
        )
    }

    fn vector_index_cache_path(&self) -> PathBuf {
        self.config
            .data_dir
            .join("indexes")
            .join("vector-cache.bin")
    }

    fn find_merge_node(
        &self,
        labels: &[String],
        properties: &Properties,
        target_shard: Option<ShardId>,
    ) -> DatabaseResult<Option<Node>> {
        if let Some((label, property, value)) = self.merge_node_lookup_key(labels, properties) {
            for id in self
                .store
                .node_ids_by_label_property(label, property, value)?
            {
                if !matches_target_shard(target_shard, self.shard_map.owner_of_node(id)) {
                    continue;
                }
                let Some(node) = self.store.node(id)? else {
                    continue;
                };
                if node_matches_merge_pattern(&node, labels, properties) {
                    return Ok(Some(node));
                }
            }
            return Ok(None);
        }

        for node in self.store.nodes()? {
            if !matches_target_shard(target_shard, self.shard_map.owner_of_node(node.id)) {
                continue;
            }
            if node_matches_merge_pattern(&node, labels, properties) {
                return Ok(Some(node));
            }
        }
        Ok(None)
    }

    fn merge_node_lookup_key<'a>(
        &'a self,
        labels: &'a [String],
        properties: &'a Properties,
    ) -> Option<(&'a str, &'a str, &'a Value)> {
        self.merge_node_lookup_key_for_kind(labels, properties, true)
            .or_else(|| self.merge_node_lookup_key_for_kind(labels, properties, false))
    }

    fn merge_node_lookup_key_for_kind<'a>(
        &'a self,
        labels: &'a [String],
        properties: &'a Properties,
        unique: bool,
    ) -> Option<(&'a str, &'a str, &'a Value)> {
        for index in &self.index_catalog.indexes {
            let kind_matches = match index.kind {
                IndexKind::UniqueNodeProperty => unique,
                IndexKind::NodeProperty => !unique,
                IndexKind::Vector { .. } => false,
            };
            if !kind_matches || !labels.iter().any(|label| label == &index.label) {
                continue;
            }
            let Some(value) = properties.get(&index.property) else {
                continue;
            };
            if matches!(value, Value::Null) {
                continue;
            }
            return Some((&index.label, &index.property, value));
        }
        None
    }

    fn find_merge_relationship(
        &self,
        from: NodeId,
        to: NodeId,
        rel_type: &str,
        properties: &Properties,
    ) -> DatabaseResult<Option<Relationship>> {
        for relationship in self.store.outgoing_by_type(from, rel_type)? {
            if relationship.to != to {
                continue;
            }
            if properties
                .iter()
                .all(|(key, value)| relationship.properties.get(key) == Some(value))
            {
                return Ok(Some(relationship));
            }
        }
        Ok(None)
    }

    fn validate_existing_unique_node_property_constraint(
        &self,
        index: &IndexDefinition,
    ) -> DatabaseResult<()> {
        let mut seen = Vec::<(Value, NodeId)>::new();
        for node in self.store.nodes()? {
            if !node.labels.iter().any(|label| label == &index.label) {
                continue;
            }
            let Some(value) = node.properties.get(&index.property) else {
                continue;
            };
            if matches!(value, Value::Null) {
                continue;
            }
            if let Some((_, existing_id)) =
                seen.iter().find(|(seen_value, _)| *seen_value == *value)
            {
                return Err(DatabaseError::InvalidConfig(format!(
                    "unique constraint {:?} would be violated by nodes {} and {} for {}.{} = {:?}",
                    index.name, existing_id, node.id, index.label, index.property, value
                )));
            }
            seen.push((value.clone(), node.id));
        }
        Ok(())
    }

    fn validate_existing_vector_index_values(&self, index: &IndexDefinition) -> DatabaseResult<()> {
        let Some((dimensions, _)) = vector_definition_parts(index) else {
            return Ok(());
        };
        for node in self.store.nodes()? {
            self.validate_node_vector_indexed_property(&node, index, dimensions)?;
        }
        Ok(())
    }

    fn validate_unique_constraints_for_command(&self, command: &Command) -> DatabaseResult<()> {
        match command {
            Command::CreateNode {
                id,
                labels,
                properties,
            } => {
                for index in self.unique_node_property_constraints_for(labels, None) {
                    if let Some(value) = properties.get(&index.property) {
                        self.ensure_unique_node_property_value(
                            &index.label,
                            &index.property,
                            value,
                            Some(*id),
                        )?;
                    }
                }
            }
            Command::SetNodeProperty { id, key, value } => {
                let Some(node) = self.store.node(*id)? else {
                    return Ok(());
                };
                for index in self.unique_node_property_constraints_for(&node.labels, Some(key)) {
                    self.ensure_unique_node_property_value(
                        &index.label,
                        &index.property,
                        value,
                        Some(*id),
                    )?;
                }
            }
            Command::AddNodeLabel { id, label } => {
                let Some(mut node) = self.store.node(*id)? else {
                    return Ok(());
                };
                if node.labels.iter().any(|existing| existing == label) {
                    return Ok(());
                }
                node.labels.push(label.clone());
                for index in self.unique_node_property_constraints_for(&node.labels, None) {
                    if let Some(value) = node.properties.get(&index.property) {
                        self.ensure_unique_node_property_value(
                            &index.label,
                            &index.property,
                            value,
                            Some(*id),
                        )?;
                    }
                }
            }
            Command::CreateRelationship { .. }
            | Command::SetRelationshipProperty { .. }
            | Command::RemoveNodeProperty { .. }
            | Command::RemoveNodeLabel { .. }
            | Command::RemoveRelationshipProperty { .. }
            | Command::DeleteNode { .. }
            | Command::DeleteRelationship { .. }
            | Command::UpsertBoundaryNode { .. }
            | Command::ClusterConfigChange { .. } => {}
        }
        Ok(())
    }

    fn validate_vector_indexes_for_command(&self, command: &Command) -> DatabaseResult<()> {
        match command {
            Command::CreateNode {
                labels, properties, ..
            } => self.validate_vector_indexed_properties(labels, properties),
            Command::SetNodeProperty { id, key, value } => {
                let Some(mut node) = self.store.node(*id)? else {
                    return Ok(());
                };
                node.properties.insert(key.clone(), value.clone());
                self.validate_vector_indexed_properties(&node.labels, &node.properties)
            }
            Command::AddNodeLabel { id, label } => {
                let Some(mut node) = self.store.node(*id)? else {
                    return Ok(());
                };
                if !node.labels.iter().any(|existing| existing == label) {
                    node.labels.push(label.clone());
                }
                self.validate_vector_indexed_properties(&node.labels, &node.properties)
            }
            Command::CreateRelationship { .. }
            | Command::SetRelationshipProperty { .. }
            | Command::RemoveNodeProperty { .. }
            | Command::RemoveNodeLabel { .. }
            | Command::RemoveRelationshipProperty { .. }
            | Command::DeleteNode { .. }
            | Command::DeleteRelationship { .. }
            | Command::UpsertBoundaryNode { .. }
            | Command::ClusterConfigChange { .. } => Ok(()),
        }
    }

    fn validate_storable_properties_for_command(command: &Command) -> DatabaseResult<()> {
        match command {
            Command::CreateNode { properties, .. }
            | Command::CreateRelationship { properties, .. }
            | Command::UpsertBoundaryNode { properties, .. } => {
                validate_storable_properties(properties)
            }
            Command::SetNodeProperty { value, .. }
            | Command::SetRelationshipProperty { value, .. } => {
                validate_storable_property_value(value)
            }
            Command::RemoveNodeProperty { .. }
            | Command::RemoveNodeLabel { .. }
            | Command::RemoveRelationshipProperty { .. }
            | Command::AddNodeLabel { .. }
            | Command::DeleteNode { .. }
            | Command::DeleteRelationship { .. }
            | Command::ClusterConfigChange { .. } => Ok(()),
        }
    }

    fn validate_replicated_vector_indexes_for_command(
        &self,
        command: &Command,
        node_overlay: &mut HashMap<NodeId, Option<Node>>,
    ) -> DatabaseResult<()> {
        match command {
            Command::CreateNode {
                id,
                labels,
                properties,
            } => {
                self.validate_vector_indexed_properties(labels, properties)?;
                node_overlay.insert(
                    *id,
                    Some(Node::new(*id, labels.clone(), properties.clone())),
                );
                Ok(())
            }
            Command::SetNodeProperty { id, key, value } => {
                let Some(mut node) = self.overlay_node(node_overlay, *id)? else {
                    return Ok(());
                };
                node.properties.insert(key.clone(), value.clone());
                self.validate_vector_indexed_properties(&node.labels, &node.properties)?;
                node_overlay.insert(*id, Some(node));
                Ok(())
            }
            Command::RemoveNodeProperty { id, key } => {
                if let Some(mut node) = self.overlay_node(node_overlay, *id)? {
                    node.properties.remove(key);
                    node_overlay.insert(*id, Some(node));
                }
                Ok(())
            }
            Command::AddNodeLabel { id, label } => {
                let Some(mut node) = self.overlay_node(node_overlay, *id)? else {
                    return Ok(());
                };
                if !node.labels.iter().any(|existing| existing == label) {
                    node.labels.push(label.clone());
                }
                self.validate_vector_indexed_properties(&node.labels, &node.properties)?;
                node_overlay.insert(*id, Some(node));
                Ok(())
            }
            Command::RemoveNodeLabel { id, label } => {
                if let Some(mut node) = self.overlay_node(node_overlay, *id)? {
                    node.labels.retain(|existing| existing != label);
                    node_overlay.insert(*id, Some(node));
                }
                Ok(())
            }
            Command::DeleteNode { id } => {
                node_overlay.insert(*id, None);
                Ok(())
            }
            Command::CreateRelationship { .. }
            | Command::SetRelationshipProperty { .. }
            | Command::RemoveRelationshipProperty { .. }
            | Command::DeleteRelationship { .. }
            | Command::UpsertBoundaryNode { .. }
            | Command::ClusterConfigChange { .. } => Ok(()),
        }
    }

    fn overlay_node(
        &self,
        node_overlay: &HashMap<NodeId, Option<Node>>,
        id: NodeId,
    ) -> DatabaseResult<Option<Node>> {
        if let Some(node) = node_overlay.get(&id) {
            return Ok(node.clone());
        }
        self.store.node(id).map_err(DatabaseError::from)
    }

    fn validate_vector_indexed_properties(
        &self,
        labels: &[String],
        properties: &Properties,
    ) -> DatabaseResult<()> {
        for index in &self.index_catalog.indexes {
            let Some((dimensions, _)) = vector_definition_parts(index) else {
                continue;
            };
            if !labels.iter().any(|label| label == &index.label) {
                continue;
            }
            self.validate_vector_indexed_property_value(index, properties, dimensions)?;
        }
        Ok(())
    }

    fn validate_node_vector_indexed_property(
        &self,
        node: &Node,
        index: &IndexDefinition,
        dimensions: usize,
    ) -> DatabaseResult<()> {
        if !node.labels.iter().any(|label| label == &index.label) {
            return Ok(());
        }
        self.validate_vector_indexed_property_value(index, &node.properties, dimensions)
    }

    fn validate_vector_indexed_property_value(
        &self,
        index: &IndexDefinition,
        properties: &Properties,
        dimensions: usize,
    ) -> DatabaseResult<()> {
        let Some(value) = properties.get(&index.property) else {
            return Ok(());
        };
        let Value::Vector(vector) = value else {
            return Err(DatabaseError::InvalidConfig(format!(
                "vector indexed property {}.{} must be a vector",
                index.label, index.property
            )));
        };
        if vector.len() != dimensions {
            return Err(DatabaseError::InvalidConfig(format!(
                "vector indexed property {}.{} expected {} dimensions, got {}",
                index.label,
                index.property,
                dimensions,
                vector.len()
            )));
        }
        Ok(())
    }

    fn unique_node_property_constraints_for(
        &self,
        labels: &[String],
        property: Option<&str>,
    ) -> Vec<IndexDefinition> {
        self.index_catalog
            .indexes
            .iter()
            .filter(|index| matches!(index.kind, IndexKind::UniqueNodeProperty))
            .filter(|index| labels.iter().any(|label| label == &index.label))
            .filter(|index| property.is_none_or(|property| property == index.property))
            .cloned()
            .collect()
    }

    fn ensure_unique_node_property_value(
        &self,
        label: &str,
        property: &str,
        value: &Value,
        except_node_id: Option<NodeId>,
    ) -> DatabaseResult<()> {
        if matches!(value, Value::Null) {
            return Ok(());
        }
        for existing_id in self
            .store
            .node_ids_by_label_property(label, property, value)?
        {
            if Some(existing_id) != except_node_id {
                return Err(DatabaseError::InvalidConfig(format!(
                    "unique constraint violation for {label}.{property} = {value:?}; existing node {existing_id}"
                )));
            }
        }
        Ok(())
    }

    fn snapshot_store(&self) -> DatabaseResult<PartitionedGraphStore<RocksKvSnapshot>> {
        Ok(self.store.snapshot()?)
    }

    fn read_snapshot(&self) -> DatabaseResult<Neo4rReadSnapshot> {
        Ok(Neo4rReadSnapshot {
            store: self.snapshot_store()?,
            shard_map: self.shard_map,
            timestamp: self.clock.now(),
            applied_indexes: self.applied_indexes(),
            committed_indexes: self.committed_indexes(),
            query_engine: CypherEngine::with_vector_indexes(Arc::new(
                SharedVectorIndexProvider::new(self.vector_indexes.clone()),
            )),
        })
    }

    fn ensure_raft_read_index(&self) -> DatabaseResult<()> {
        let Some(raft_groups) = self.raft_groups.as_ref() else {
            return Ok(());
        };
        for placement in &self.routing_table.placements {
            if !placement.has_server(self.config.server_id) {
                continue;
            }
            let group = raft_groups
                .groups
                .get(placement.shard_id as usize)
                .ok_or(DatabaseError::MissingShardLog(placement.shard_id))?;
            let read_index = group.leader_lease_read_index()?;
            let committed = self
                .commit_indexes
                .get(placement.shard_id as usize)
                .copied()
                .ok_or(DatabaseError::MissingShardLog(placement.shard_id))?;
            if committed < read_index {
                return Err(DatabaseError::Replication(format!(
                    "strong read requires local commit index {committed} to reach raft read-index {read_index} for shard {}",
                    placement.shard_id
                )));
            }
        }
        Ok(())
    }

    fn write_command(&mut self, shard_id: ShardId, command: Command) -> DatabaseResult<()> {
        let entry = self.append_local_command(shard_id, command, true)?;
        let outcome = self.replicator.publish(&entry)?;
        self.observe_replication_outcome(&entry, &outcome)?;
        self.observe_raft_replication_outcome(&entry, &outcome)?;
        self.commit_entry(&entry)?;
        self.maybe_inject_failure_after_commit_before_apply()?;
        self.apply_entry(&entry)
    }

    fn maybe_inject_failure_after_commit_before_apply(&self) -> DatabaseResult<()> {
        if self.config.failure_injection.fail_after_commit_before_apply {
            return Err(DatabaseError::Replication(
                "injected failure after commit before apply".to_string(),
            ));
        }
        Ok(())
    }

    fn prepare_local_write(&mut self, operation: WriteOperation) -> DatabaseResult<PreparedWrite> {
        match operation {
            WriteOperation::CreateNode { labels, properties } => {
                let id = self.allocate_node_id();
                let command = Command::CreateNode {
                    id,
                    labels,
                    properties,
                };
                let shard_id = self.shard_map.owner_of_node(id);
                let entry = self.append_local_command(shard_id, command, false)?;
                Ok(PreparedWrite {
                    entry,
                    response: WriteResponse::NodeId(id),
                })
            }
            WriteOperation::CreateNodeOnShard {
                shard_id,
                labels,
                properties,
            } => {
                self.validate_shard_id(shard_id)?;
                let id = self.allocate_node_id_for_shard(shard_id);
                let command = Command::CreateNode {
                    id,
                    labels,
                    properties,
                };
                let entry = self.append_local_command(shard_id, command, false)?;
                Ok(PreparedWrite {
                    entry,
                    response: WriteResponse::NodeId(id),
                })
            }
            WriteOperation::CreateRelationship {
                from,
                to,
                rel_type,
                properties,
            } => {
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
                let entry = self.append_local_command(shard_id, command, false)?;
                Ok(PreparedWrite {
                    entry,
                    response: WriteResponse::RelationshipId(id),
                })
            }
            WriteOperation::SetNodeProperty { id, key, value } => {
                self.ensure_local_node_exists(id)?;
                let shard_id = self.shard_map.owner_of_node(id);
                let entry = self.append_local_command(
                    shard_id,
                    Command::SetNodeProperty { id, key, value },
                    false,
                )?;
                Ok(PreparedWrite {
                    entry,
                    response: WriteResponse::Unit,
                })
            }
            WriteOperation::RemoveNodeProperty { id, key } => {
                self.ensure_local_node_exists(id)?;
                let shard_id = self.shard_map.owner_of_node(id);
                let entry = self.append_local_command(
                    shard_id,
                    Command::RemoveNodeProperty { id, key },
                    false,
                )?;
                Ok(PreparedWrite {
                    entry,
                    response: WriteResponse::Unit,
                })
            }
            WriteOperation::AddNodeLabel { id, label } => {
                self.ensure_local_node_exists(id)?;
                let shard_id = self.shard_map.owner_of_node(id);
                let entry = self.append_local_command(
                    shard_id,
                    Command::AddNodeLabel { id, label },
                    false,
                )?;
                Ok(PreparedWrite {
                    entry,
                    response: WriteResponse::Unit,
                })
            }
            WriteOperation::RemoveNodeLabel { id, label } => {
                self.ensure_local_node_exists(id)?;
                let shard_id = self.shard_map.owner_of_node(id);
                let entry = self.append_local_command(
                    shard_id,
                    Command::RemoveNodeLabel { id, label },
                    false,
                )?;
                Ok(PreparedWrite {
                    entry,
                    response: WriteResponse::Unit,
                })
            }
            WriteOperation::SetRelationshipProperty { id, key, value } => {
                let shard_id = self.relationship_owner_shard(id)?;
                let entry = self.append_local_command(
                    shard_id,
                    Command::SetRelationshipProperty { id, key, value },
                    false,
                )?;
                Ok(PreparedWrite {
                    entry,
                    response: WriteResponse::Unit,
                })
            }
            WriteOperation::RemoveRelationshipProperty { id, key } => {
                let shard_id = self.relationship_owner_shard(id)?;
                let entry = self.append_local_command(
                    shard_id,
                    Command::RemoveRelationshipProperty { id, key },
                    false,
                )?;
                Ok(PreparedWrite {
                    entry,
                    response: WriteResponse::Unit,
                })
            }
            WriteOperation::DeleteRelationship { id } => {
                let shard_id = self.relationship_owner_shard(id)?;
                let entry =
                    self.append_local_command(shard_id, Command::DeleteRelationship { id }, false)?;
                Ok(PreparedWrite {
                    entry,
                    response: WriteResponse::Unit,
                })
            }
            WriteOperation::DeleteNode { id } => {
                self.ensure_local_node_exists(id)?;
                let shard_id = self.shard_map.owner_of_node(id);
                let entry =
                    self.append_local_command(shard_id, Command::DeleteNode { id }, false)?;
                Ok(PreparedWrite {
                    entry,
                    response: WriteResponse::Unit,
                })
            }
            WriteOperation::ApplyReplicatedEntry(_)
            | WriteOperation::ApplyReplicatedEntries(_)
            | WriteOperation::ApplyRaftAppendEntries { .. } => {
                Err(DatabaseError::UnexpectedWriteResponse(
                    "replicated entry cannot be prepared as a local write".to_string(),
                ))
            }
        }
    }

}
