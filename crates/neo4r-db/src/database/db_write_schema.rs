impl Neo4rDatabase {
    fn execute_write_cypher_on_optional_shard(
        &mut self,
        query: &str,
        params: &QueryParams,
        target_shard: Option<ShardId>,
    ) -> DatabaseResult<Vec<QueryRow>> {
        match parse_write_cypher(query, params)? {
            Some(WriteCypher::CreateNode {
                variable,
                labels,
                properties,
                assignments,
                replacement,
                returns,
            }) => {
                let properties = create_properties_after_set(properties, assignments, replacement);
                let id = if let Some(shard_id) = target_shard {
                    self.validate_shard_id(shard_id)?;
                    let id = self.allocate_node_id_for_shard(shard_id);
                    let command = Command::CreateNode {
                        id,
                        labels: labels.clone(),
                        properties: properties.clone(),
                    };
                    self.write_command(shard_id, command)?;
                    id
                } else {
                    self.create_node(labels.clone(), properties.clone())?
                };
                Ok(return_created_node(
                    variable, returns, id, labels, properties,
                ))
            }
            Some(WriteCypher::CreateRelationship {
                variable,
                from_matcher,
                to_matcher,
                rel_type,
                properties,
                assignments,
                replacement,
                returns,
            }) => {
                let properties = create_properties_after_set(properties, assignments, replacement);
                let from_ids = self.match_node_ids(&from_matcher, params)?;
                let to_ids = self.match_node_ids(&to_matcher, params)?;
                let mut rows = Vec::new();
                for from in &from_ids {
                    if !matches_target_shard(target_shard, self.shard_map.owner_of_node(*from)) {
                        continue;
                    }
                    for to in &to_ids {
                        let id = self.create_relationship(
                            *from,
                            *to,
                            rel_type.clone(),
                            properties.clone(),
                        )?;
                        rows.extend(return_created_relationship(
                            variable.clone(),
                            returns.clone(),
                            Relationship::new(id, *from, *to, rel_type.clone(), properties.clone()),
                        ));
                    }
                }
                Ok(rows)
            }
            Some(WriteCypher::CreateNodeThenRelationship {
                node_variable,
                labels,
                node_properties,
                node_assignments,
                node_replacement,
                matched_matcher,
                relationship_variable,
                created_node_is_from,
                rel_type,
                relationship_properties,
                relationship_assignments,
                relationship_replacement,
                returns,
            }) => {
                let node_properties = create_properties_after_set(
                    node_properties,
                    node_assignments,
                    node_replacement,
                );
                let relationship_properties = create_properties_after_set(
                    relationship_properties,
                    relationship_assignments,
                    relationship_replacement,
                );
                let node_id = if let Some(shard_id) = target_shard {
                    self.validate_shard_id(shard_id)?;
                    let id = self.allocate_node_id_for_shard(shard_id);
                    let command = Command::CreateNode {
                        id,
                        labels: labels.clone(),
                        properties: node_properties.clone(),
                    };
                    self.write_command(shard_id, command)?;
                    id
                } else {
                    self.create_node(labels.clone(), node_properties.clone())?
                };
                let node = Node::new(node_id, labels, node_properties);
                let matched_ids = self.match_node_ids(&matched_matcher, params)?;
                let mut rows = Vec::new();
                for matched_id in matched_ids {
                    let (from, to) = if created_node_is_from {
                        (node_id, matched_id)
                    } else {
                        (matched_id, node_id)
                    };
                    let relationship_id = self.create_relationship(
                        from,
                        to,
                        rel_type.clone(),
                        relationship_properties.clone(),
                    )?;
                    let relationship = Relationship::new(
                        relationship_id,
                        from,
                        to,
                        rel_type.clone(),
                        relationship_properties.clone(),
                    );
                    if let Some(returns) = returns.as_ref() {
                        rows.push(write_node_relationship_return_row(
                            &node_variable,
                            &node,
                            &relationship_variable,
                            &relationship,
                            returns,
                        ));
                    }
                }
                Ok(rows)
            }
            Some(WriteCypher::MergeNode {
                labels,
                properties,
                on_create,
                on_create_replacement,
                on_match,
                on_match_replacement,
                returns,
                ..
            }) => {
                if let Some(node) = self.find_merge_node(&labels, &properties, target_shard)? {
                    let node = if on_match.is_empty() && on_match_replacement.is_none() {
                        node
                    } else {
                        let after = properties_after_set(
                            node.properties.clone(),
                            &on_match,
                            on_match_replacement.as_ref(),
                        );
                        replace_node_properties(self, node.id, &node.properties, &after)?;
                        self.node(node.id)?.ok_or_else(|| {
                            DatabaseError::Graph(GraphError::NodeNotFound(node.id))
                        })?
                    };
                    return Ok(return_node_for_write(node, returns.as_ref()));
                }
                let create_properties = properties_after_set(
                    properties.clone(),
                    &on_create,
                    on_create_replacement.as_ref(),
                );
                let id = if let Some(shard_id) = target_shard {
                    self.validate_shard_id(shard_id)?;
                    let id = self.allocate_node_id_for_shard(shard_id);
                    self.write_command(
                        shard_id,
                        Command::CreateNode {
                            id,
                            labels: labels.clone(),
                            properties: create_properties.clone(),
                        },
                    )?;
                    id
                } else {
                    self.create_node(labels.clone(), create_properties.clone())?
                };
                Ok(return_node_for_write(
                    Node::new(id, labels, create_properties),
                    returns.as_ref(),
                ))
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
                returns,
            }) => {
                let from_ids = self.match_node_ids(&from_matcher, params)?;
                let to_ids = self.match_node_ids(&to_matcher, params)?;
                let mut rows = Vec::new();
                for from in &from_ids {
                    if !matches_target_shard(target_shard, self.shard_map.owner_of_node(*from)) {
                        continue;
                    }
                    for to in &to_ids {
                        if let Some(relationship) =
                            self.find_merge_relationship(*from, *to, &rel_type, &properties)?
                        {
                            let relationship =
                                if on_match.is_empty() && on_match_replacement.is_none() {
                                    relationship
                                } else {
                                    let after = properties_after_set(
                                        relationship.properties.clone(),
                                        &on_match,
                                        on_match_replacement.as_ref(),
                                    );
                                    replace_relationship_properties(
                                        self,
                                        relationship.id,
                                        &relationship.properties,
                                        &after,
                                    )?;
                                    self.relationship(relationship.id)?.ok_or_else(|| {
                                        DatabaseError::Graph(GraphError::RelationshipNotFound(
                                            relationship.id,
                                        ))
                                    })?
                                };
                            rows.extend(return_relationship_for_write(
                                relationship,
                                returns.as_ref(),
                            ));
                            continue;
                        }
                        let create_properties = properties_after_set(
                            properties.clone(),
                            &on_create,
                            on_create_replacement.as_ref(),
                        );
                        let id = self.create_relationship(
                            *from,
                            *to,
                            rel_type.clone(),
                            create_properties.clone(),
                        )?;
                        rows.extend(return_relationship_for_write(
                            Relationship::new(id, *from, *to, rel_type.clone(), create_properties),
                            returns.as_ref(),
                        ));
                    }
                }
                Ok(rows)
            }
            Some(WriteCypher::SetNodeProperty {
                matcher,
                assignments,
                returns,
            }) => {
                let node_ids = self.match_node_ids(&matcher, params)?;
                let mut affected_ids = Vec::new();
                for id in &node_ids {
                    if !matches_target_shard(target_shard, self.shard_map.owner_of_node(*id)) {
                        continue;
                    }
                    for assignment in &assignments {
                        apply_node_property_assignment(self, *id, assignment)?;
                    }
                    affected_ids.push(*id);
                }
                return_nodes_after_write(&affected_ids, returns.as_ref(), |id| self.node(id))
            }
            Some(WriteCypher::ReplaceNodeProperties {
                matcher,
                properties,
                returns,
            }) => {
                let node_ids = self.match_node_ids(&matcher, params)?;
                let mut affected_ids = Vec::new();
                for id in &node_ids {
                    if !matches_target_shard(target_shard, self.shard_map.owner_of_node(*id)) {
                        continue;
                    }
                    let current = self
                        .node(*id)?
                        .ok_or(DatabaseError::Graph(GraphError::NodeNotFound(*id)))?;
                    replace_node_properties(self, *id, &current.properties, &properties)?;
                    affected_ids.push(*id);
                }
                return_nodes_after_write(&affected_ids, returns.as_ref(), |id| self.node(id))
            }
            Some(WriteCypher::RemoveNodeProperty {
                matcher,
                keys,
                returns,
            }) => {
                let node_ids = self.match_node_ids(&matcher, params)?;
                let mut affected_ids = Vec::new();
                for id in &node_ids {
                    if !matches_target_shard(target_shard, self.shard_map.owner_of_node(*id)) {
                        continue;
                    }
                    for key in &keys {
                        self.remove_node_property(*id, key.clone())?;
                    }
                    affected_ids.push(*id);
                }
                return_nodes_after_write(&affected_ids, returns.as_ref(), |id| self.node(id))
            }
            Some(WriteCypher::AddNodeLabel {
                matcher,
                labels,
                returns,
            }) => {
                let node_ids = self.match_node_ids(&matcher, params)?;
                let mut affected_ids = Vec::new();
                for id in &node_ids {
                    if !matches_target_shard(target_shard, self.shard_map.owner_of_node(*id)) {
                        continue;
                    }
                    for label in &labels {
                        self.add_node_label(*id, label.clone())?;
                    }
                    affected_ids.push(*id);
                }
                return_nodes_after_write(&affected_ids, returns.as_ref(), |id| self.node(id))
            }
            Some(WriteCypher::RemoveNodeLabel {
                matcher,
                labels,
                returns,
            }) => {
                let node_ids = self.match_node_ids(&matcher, params)?;
                let mut affected_ids = Vec::new();
                for id in &node_ids {
                    if !matches_target_shard(target_shard, self.shard_map.owner_of_node(*id)) {
                        continue;
                    }
                    for label in &labels {
                        self.remove_node_label(*id, label.clone())?;
                    }
                    affected_ids.push(*id);
                }
                return_nodes_after_write(&affected_ids, returns.as_ref(), |id| self.node(id))
            }
            Some(WriteCypher::SetRelationshipProperty {
                matcher,
                assignments,
                returns,
            }) => {
                let relationship_ids = self.match_relationship_ids(&matcher, params)?;
                let mut affected_ids = Vec::new();
                for id in &relationship_ids {
                    if !matches_target_shard(target_shard, self.relationship_owner_shard(*id)?) {
                        continue;
                    }
                    for assignment in &assignments {
                        apply_relationship_property_assignment(self, *id, assignment)?;
                    }
                    affected_ids.push(*id);
                }
                return_relationships_after_write(&affected_ids, returns.as_ref(), |id| {
                    self.relationship(id)
                })
            }
            Some(WriteCypher::ReplaceRelationshipProperties {
                matcher,
                properties,
                returns,
            }) => {
                let relationship_ids = self.match_relationship_ids(&matcher, params)?;
                let mut affected_ids = Vec::new();
                for id in &relationship_ids {
                    if !matches_target_shard(target_shard, self.relationship_owner_shard(*id)?) {
                        continue;
                    }
                    let current = self
                        .relationship(*id)?
                        .ok_or(DatabaseError::Graph(GraphError::RelationshipNotFound(*id)))?;
                    replace_relationship_properties(self, *id, &current.properties, &properties)?;
                    affected_ids.push(*id);
                }
                return_relationships_after_write(&affected_ids, returns.as_ref(), |id| {
                    self.relationship(id)
                })
            }
            Some(WriteCypher::RemoveRelationshipProperty {
                matcher,
                keys,
                returns,
            }) => {
                let relationship_ids = self.match_relationship_ids(&matcher, params)?;
                let mut affected_ids = Vec::new();
                for id in &relationship_ids {
                    if !matches_target_shard(target_shard, self.relationship_owner_shard(*id)?) {
                        continue;
                    }
                    for key in &keys {
                        self.remove_relationship_property(*id, key.clone())?;
                    }
                    affected_ids.push(*id);
                }
                return_relationships_after_write(&affected_ids, returns.as_ref(), |id| {
                    self.relationship(id)
                })
            }
            Some(WriteCypher::DeleteNode { matcher, returns }) => {
                let node_ids = self.match_node_ids(&matcher, params)?;
                let mut affected_ids = Vec::new();
                for id in &node_ids {
                    if !matches_target_shard(target_shard, self.shard_map.owner_of_node(*id)) {
                        continue;
                    }
                    affected_ids.push(*id);
                }
                let rows =
                    return_nodes_after_write(&affected_ids, returns.as_ref(), |id| self.node(id))?;
                for id in &affected_ids {
                    self.delete_node(*id)?;
                }
                Ok(rows)
            }
            Some(WriteCypher::DeleteRelationship { matcher, returns }) => {
                let relationship_ids = self.match_relationship_ids(&matcher, params)?;
                let mut affected_ids = Vec::new();
                for id in &relationship_ids {
                    if !matches_target_shard(target_shard, self.relationship_owner_shard(*id)?) {
                        continue;
                    }
                    affected_ids.push(*id);
                }
                let rows =
                    return_relationships_after_write(&affected_ids, returns.as_ref(), |id| {
                        self.relationship(id)
                    })?;
                for id in &affected_ids {
                    self.delete_relationship(*id)?;
                }
                Ok(rows)
            }
            Some(WriteCypher::CreateNodePropertyIndex {
                name,
                label,
                property,
                if_not_exists,
            }) => {
                ensure_metadata_target_shard(target_shard)?;
                if if_not_exists {
                    self.create_node_property_index_if_not_exists(name, label, property)?;
                } else {
                    self.create_node_property_index(name, label, property)?;
                }
                Ok(Vec::new())
            }
            Some(WriteCypher::CreateUniqueNodePropertyConstraint {
                name,
                label,
                property,
                if_not_exists,
            }) => {
                ensure_metadata_target_shard(target_shard)?;
                if if_not_exists {
                    self.create_unique_node_property_constraint_if_not_exists(
                        name, label, property,
                    )?;
                } else {
                    self.create_unique_node_property_constraint(name, label, property)?;
                }
                Ok(Vec::new())
            }
            Some(WriteCypher::CreateVectorIndex {
                name,
                label,
                property,
                dimensions,
                metric,
                if_not_exists,
            }) => {
                ensure_metadata_target_shard(target_shard)?;
                if if_not_exists {
                    self.create_vector_index_if_not_exists(
                        name, label, property, dimensions, metric,
                    )?;
                } else {
                    self.create_vector_index(name, label, property, dimensions, metric)?;
                }
                Ok(Vec::new())
            }
            Some(WriteCypher::RebuildVectorIndex { name }) => {
                ensure_metadata_target_shard(target_shard)?;
                self.rebuild_vector_index(&name)?;
                Ok(Vec::new())
            }
            Some(WriteCypher::DropIndex { name, if_exists }) => {
                ensure_metadata_target_shard(target_shard)?;
                if if_exists {
                    self.drop_index_if_exists(&name)?;
                } else {
                    self.drop_index(&name)?;
                }
                Ok(Vec::new())
            }
            Some(WriteCypher::DropConstraint { name, if_exists }) => {
                ensure_metadata_target_shard(target_shard)?;
                if if_exists {
                    self.drop_constraint_if_exists(&name)?;
                } else {
                    self.drop_constraint(&name)?;
                }
                Ok(Vec::new())
            }
            None => self.query_with_params(query, params),
        }
    }

    pub fn query(&self, query: &str) -> DatabaseResult<Vec<QueryRow>> {
        Ok(self.query_engine.execute(&self.store, query)?)
    }

    pub fn node(&self, id: NodeId) -> DatabaseResult<Option<Node>> {
        if let Some(node) = self
            .read_cache
            .lock()
            .map_err(|_| DatabaseError::LockPoisoned)?
            .nodes
            .get(&id)
            .cloned()
        {
            self.observe_read_cache_hit()?;
            return Ok(node);
        }
        self.observe_read_cache_miss()?;
        let node = self.store.node(id).map_err(DatabaseError::from)?;
        self.read_cache
            .lock()
            .map_err(|_| DatabaseError::LockPoisoned)?
            .nodes
            .insert(id, node.clone());
        Ok(node)
    }

    pub fn relationship(&self, id: RelationshipId) -> DatabaseResult<Option<Relationship>> {
        if let Some(relationship) = self
            .read_cache
            .lock()
            .map_err(|_| DatabaseError::LockPoisoned)?
            .relationships
            .get(&id)
            .cloned()
        {
            self.observe_read_cache_hit()?;
            return Ok(relationship);
        }
        self.observe_read_cache_miss()?;
        let relationship = self.store.relationship(id).map_err(DatabaseError::from)?;
        self.read_cache
            .lock()
            .map_err(|_| DatabaseError::LockPoisoned)?
            .relationships
            .insert(id, relationship.clone());
        Ok(relationship)
    }

    pub fn query_with_params(
        &self,
        query: &str,
        params: &QueryParams,
    ) -> DatabaseResult<Vec<QueryRow>> {
        if let Some(rows) = self.show_index_rows_for_query(query)? {
            return Ok(rows);
        }
        if let Some(rows) = self.show_constraint_rows_for_query(query)? {
            return Ok(rows);
        }
        Ok(self
            .query_engine
            .execute_with_params(&self.store, query, params)?)
    }

    pub fn query_cursor(&self, query: &str) -> DatabaseResult<Box<dyn QueryCursor>> {
        if let Some(rows) = self.show_index_rows_for_query(query)? {
            return Ok(Box::new(VecQueryCursor::new(rows)));
        }
        if let Some(rows) = self.show_constraint_rows_for_query(query)? {
            return Ok(Box::new(VecQueryCursor::new(rows)));
        }
        Ok(self.query_engine.execute_cursor(&self.store, query)?)
    }

    pub fn query_cursor_with_params(
        &self,
        query: &str,
        params: &QueryParams,
    ) -> DatabaseResult<Box<dyn QueryCursor>> {
        if let Some(rows) = self.show_index_rows_for_query(query)? {
            return Ok(Box::new(VecQueryCursor::new(rows)));
        }
        if let Some(rows) = self.show_constraint_rows_for_query(query)? {
            return Ok(Box::new(VecQueryCursor::new(rows)));
        }
        Ok(self
            .query_engine
            .execute_cursor_with_params(&self.store, query, params)?)
    }

    pub fn shard_count(&self) -> u64 {
        self.shard_map.shard_count()
    }

    pub fn local_partition_count(&self) -> usize {
        self.store.partition_count()
    }

    pub fn data_dir(&self) -> &Path {
        &self.config.data_dir
    }

    pub fn routing_table(&self) -> &ShardRoutingTable {
        &self.routing_table
    }

    pub fn log_entries_from(
        &self,
        shard_id: ShardId,
        start_index: LogIndex,
    ) -> DatabaseResult<Vec<LogEntry>> {
        Ok(self.log(shard_id)?.replay_from(start_index)?)
    }

    pub fn create_node_property_index(
        &mut self,
        name: impl Into<String>,
        label: impl Into<String>,
        property: impl Into<String>,
    ) -> DatabaseResult<()> {
        self.add_index_definition(IndexDefinition::node_property(name, label, property))
    }

    pub fn create_node_property_index_if_not_exists(
        &mut self,
        name: impl Into<String>,
        label: impl Into<String>,
        property: impl Into<String>,
    ) -> DatabaseResult<()> {
        self.add_index_definition_if_not_exists(IndexDefinition::node_property(
            name, label, property,
        ))
    }

    pub fn create_unique_node_property_constraint(
        &mut self,
        name: impl Into<String>,
        label: impl Into<String>,
        property: impl Into<String>,
    ) -> DatabaseResult<()> {
        self.add_index_definition(IndexDefinition::unique_node_property(name, label, property))
    }

    pub fn create_unique_node_property_constraint_if_not_exists(
        &mut self,
        name: impl Into<String>,
        label: impl Into<String>,
        property: impl Into<String>,
    ) -> DatabaseResult<()> {
        self.add_index_definition_if_not_exists(IndexDefinition::unique_node_property(
            name, label, property,
        ))
    }

    pub fn create_vector_index(
        &mut self,
        name: impl Into<String>,
        label: impl Into<String>,
        property: impl Into<String>,
        dimensions: usize,
        metric: impl Into<String>,
    ) -> DatabaseResult<()> {
        if dimensions == 0 {
            return Err(DatabaseError::InvalidConfig(
                "vector index dimensions must be greater than zero".to_string(),
            ));
        }
        self.add_index_definition(IndexDefinition::vector(
            name, label, property, dimensions, metric,
        ))
    }

    pub fn create_vector_index_if_not_exists(
        &mut self,
        name: impl Into<String>,
        label: impl Into<String>,
        property: impl Into<String>,
        dimensions: usize,
        metric: impl Into<String>,
    ) -> DatabaseResult<()> {
        if dimensions == 0 {
            return Err(DatabaseError::InvalidConfig(
                "vector index dimensions must be greater than zero".to_string(),
            ));
        }
        self.add_index_definition_if_not_exists(IndexDefinition::vector(
            name, label, property, dimensions, metric,
        ))
    }

    pub fn drop_index(&mut self, name: &str) -> DatabaseResult<()> {
        let before = self.index_catalog.indexes.len();
        self.index_catalog
            .indexes
            .retain(|index| index.name != name);
        if self.index_catalog.indexes.len() == before {
            return Err(DatabaseError::InvalidConfig(format!(
                "index {name:?} does not exist"
            )));
        }
        self.index_catalog.version += 1;
        self.index_catalog_store.save(&self.index_catalog)?;
        self.vector_indexes
            .lock()
            .map_err(|_| DatabaseError::LockPoisoned)?
            .remove(name);
        self.save_vector_index_cache()?;
        Ok(())
    }

    pub fn drop_index_if_exists(&mut self, name: &str) -> DatabaseResult<()> {
        if self
            .index_catalog
            .indexes
            .iter()
            .any(|index| index.name == name)
        {
            self.drop_index(name)
        } else {
            Ok(())
        }
    }

    pub fn drop_constraint(&mut self, name: &str) -> DatabaseResult<()> {
        let Some(index) = self
            .index_catalog
            .indexes
            .iter()
            .find(|index| index.name == name)
        else {
            return Err(DatabaseError::InvalidConfig(format!(
                "constraint {name:?} does not exist"
            )));
        };
        if !matches!(index.kind, IndexKind::UniqueNodeProperty) {
            return Err(DatabaseError::InvalidConfig(format!(
                "index {name:?} is not a constraint"
            )));
        }
        self.drop_index(name)
    }

    pub fn drop_constraint_if_exists(&mut self, name: &str) -> DatabaseResult<()> {
        let Some(index) = self
            .index_catalog
            .indexes
            .iter()
            .find(|index| index.name == name)
        else {
            return Ok(());
        };
        if !matches!(index.kind, IndexKind::UniqueNodeProperty) {
            return Err(DatabaseError::InvalidConfig(format!(
                "index {name:?} is not a constraint"
            )));
        }
        self.drop_index(name)
    }

    pub fn list_indexes(&self) -> Vec<IndexDefinition> {
        self.index_catalog.indexes.clone()
    }

    pub fn index_lifecycle_status(&self) -> DatabaseResult<Vec<IndexLifecycleStatus>> {
        let persisted = self
            .index_lifecycle_store
            .load()?
            .into_iter()
            .map(|status| (status.name.clone(), status))
            .collect::<BTreeMap<_, _>>();
        self.index_catalog
            .indexes
            .iter()
            .map(|index| {
                if let Some(status) = persisted.get(&index.name) {
                    return Ok(status.clone());
                }
                let status = IndexLifecycleStatus {
                    name: index.name.clone(),
                    state: "ready".to_string(),
                    backfilled_entries: self.index_backfilled_entries(index)?,
                    failure: String::new(),
                };
                Ok(status)
            })
            .collect()
    }

    pub fn show_indexes(&self) -> Vec<QueryRow> {
        format_index_rows(&self.index_catalog.indexes)
    }

    pub fn show_index(&self, name: &str) -> DatabaseResult<Vec<QueryRow>> {
        Ok(vec![format_index_row_by_name(
            &self.index_catalog.indexes,
            name,
        )?])
    }

    pub fn show_vector_indexes(&self) -> Vec<QueryRow> {
        format_vector_index_rows(&self.index_catalog.indexes)
    }

    pub fn show_vector_index(&self, name: &str) -> DatabaseResult<Vec<QueryRow>> {
        Ok(vec![format_vector_index_row_by_name(
            &self.index_catalog.indexes,
            name,
        )?])
    }

    pub fn show_vector_index_status(&self) -> DatabaseResult<Vec<QueryRow>> {
        Ok(format_vector_index_status_rows(
            &self.vector_index_status()?,
        ))
    }

    pub fn show_vector_index_status_by_name(&self, name: &str) -> DatabaseResult<Vec<QueryRow>> {
        Ok(format_vector_index_status_rows(&[
            self.vector_index_status_by_name(name)?
        ]))
    }

    fn show_index_rows_for_query(&self, query: &str) -> DatabaseResult<Option<Vec<QueryRow>>> {
        if let Some(name) = show_vector_index_status_name(query)? {
            Ok(Some(self.show_vector_index_status_by_name(&name)?))
        } else if is_show_vector_index_status_cypher(query) {
            Ok(Some(self.show_vector_index_status()?))
        } else if let Some(name) = show_vector_index_name(query)? {
            Ok(Some(self.show_vector_index(&name)?))
        } else if let Some(name) = show_index_name(query)? {
            Ok(Some(self.show_index(&name)?))
        } else if is_show_vector_indexes_cypher(query) {
            Ok(Some(self.show_vector_indexes()))
        } else if is_show_indexes_cypher(query) {
            Ok(Some(self.show_indexes()))
        } else {
            Ok(None)
        }
    }

    pub fn show_constraints(&self) -> Vec<QueryRow> {
        format_constraint_rows(&self.index_catalog.indexes)
    }

    pub fn show_constraint(&self, name: &str) -> DatabaseResult<Vec<QueryRow>> {
        Ok(vec![format_constraint_row_by_name(
            &self.index_catalog.indexes,
            name,
        )?])
    }

    fn show_constraint_rows_for_query(&self, query: &str) -> DatabaseResult<Option<Vec<QueryRow>>> {
        if let Some(name) = show_constraint_name(query)? {
            Ok(Some(self.show_constraint(&name)?))
        } else if is_show_constraints_cypher(query) {
            Ok(Some(self.show_constraints()))
        } else {
            Ok(None)
        }
    }

    pub fn vector_index_status(&self) -> DatabaseResult<Vec<VectorIndexStatus>> {
        Ok(self
            .vector_indexes
            .lock()
            .map_err(|_| DatabaseError::LockPoisoned)?
            .status())
    }

    pub fn vector_index_status_by_name(&self, name: &str) -> DatabaseResult<VectorIndexStatus> {
        let statuses = self.vector_index_status()?;
        statuses
            .into_iter()
            .find(|status| status.name == name)
            .ok_or_else(|| {
                DatabaseError::InvalidConfig(format!("vector index {name:?} does not exist"))
            })
    }

    fn index_backfilled_entries(&self, index: &IndexDefinition) -> DatabaseResult<usize> {
        Ok(self
            .store
            .nodes()?
            .into_iter()
            .filter(|node| {
                node.labels.iter().any(|label| label == &index.label)
                    && node.properties.contains_key(&index.property)
            })
            .count())
    }

    fn save_index_lifecycle_state(
        &self,
        index: &IndexDefinition,
        state: &str,
        backfilled_entries: usize,
        failure: &str,
    ) -> DatabaseResult<()> {
        self.index_lifecycle_store
            .save_status(&IndexLifecycleStatus {
                name: index.name.clone(),
                state: state.to_string(),
                backfilled_entries,
                failure: failure.to_string(),
            })
    }

    pub fn index_catalog(&self) -> IndexCatalog {
        self.index_catalog.clone()
    }

    pub fn install_index_catalog(&mut self, catalog: IndexCatalog) -> DatabaseResult<()> {
        validate_index_catalog(&catalog)?;
        if catalog.version < self.index_catalog.version {
            return Err(DatabaseError::InvalidConfig(format!(
                "index catalog version must not move backwards from {} to {}",
                self.index_catalog.version, catalog.version
            )));
        }
        if catalog.version == self.index_catalog.version
            && catalog.indexes == self.index_catalog.indexes
        {
            return Ok(());
        }
        self.validate_index_catalog_against_store(&catalog)?;
        let vector_indexes = self.build_vector_indexes_for_catalog(&catalog)?;
        self.index_catalog_store.save(&catalog)?;
        self.index_catalog = catalog;
        *self
            .vector_indexes
            .lock()
            .map_err(|_| DatabaseError::LockPoisoned)? = vector_indexes;
        self.save_vector_index_cache()
    }

    fn match_node_ids(
        &self,
        matcher: &NodeMatcher,
        params: &QueryParams,
    ) -> DatabaseResult<Vec<NodeId>> {
        query_match_node_ids(|query| self.query_with_params(query, params), matcher)
    }

    fn match_relationship_ids(
        &self,
        matcher: &RelationshipMatcher,
        params: &QueryParams,
    ) -> DatabaseResult<Vec<RelationshipId>> {
        query_match_relationship_ids(|query| self.query_with_params(query, params), matcher)
    }

}
