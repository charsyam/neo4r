impl Neo4rDatabaseHandle {
    pub fn open(config: DatabaseConfig) -> DatabaseResult<Self> {
        Self::open_with_replicator(config, Arc::new(NoopShardReplicator))
    }

    pub fn open_with_replicator(
        config: DatabaseConfig,
        replicator: Arc<dyn ShardReplicator>,
    ) -> DatabaseResult<Self> {
        let inner = Arc::new(Mutex::new(Neo4rDatabase::open_with_replicator(
            config, replicator,
        )?));
        let writer = spawn_writer_actor(inner.clone());
        Ok(Self { inner, writer })
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
        &self,
        labels: Vec<String>,
        properties: Properties,
    ) -> DatabaseResult<NodeId> {
        match self.send_write(WriteOperation::CreateNode { labels, properties })? {
            WriteResponse::NodeId(id) => Ok(id),
            response => Err(unexpected_write_response(response)),
        }
    }

    pub fn create_node_on_shard(
        &self,
        shard_id: ShardId,
        labels: Vec<String>,
        properties: Properties,
    ) -> DatabaseResult<NodeId> {
        match self.send_write(WriteOperation::CreateNodeOnShard {
            shard_id,
            labels,
            properties,
        })? {
            WriteResponse::NodeId(id) => Ok(id),
            response => Err(unexpected_write_response(response)),
        }
    }

    pub fn create_relationship(
        &self,
        from: NodeId,
        to: NodeId,
        rel_type: String,
        properties: Properties,
    ) -> DatabaseResult<RelationshipId> {
        match self.send_write(WriteOperation::CreateRelationship {
            from,
            to,
            rel_type,
            properties,
        })? {
            WriteResponse::RelationshipId(id) => Ok(id),
            response => Err(unexpected_write_response(response)),
        }
    }

    pub fn set_node_property(&self, id: NodeId, key: String, value: Value) -> DatabaseResult<()> {
        expect_unit(self.send_write(WriteOperation::SetNodeProperty { id, key, value })?)
    }

    pub fn remove_node_property(&self, id: NodeId, key: String) -> DatabaseResult<()> {
        expect_unit(self.send_write(WriteOperation::RemoveNodeProperty { id, key })?)
    }

    pub fn add_node_label(&self, id: NodeId, label: String) -> DatabaseResult<()> {
        expect_unit(self.send_write(WriteOperation::AddNodeLabel { id, label })?)
    }

    pub fn remove_node_label(&self, id: NodeId, label: String) -> DatabaseResult<()> {
        expect_unit(self.send_write(WriteOperation::RemoveNodeLabel { id, label })?)
    }

    pub fn set_relationship_property(
        &self,
        id: RelationshipId,
        key: String,
        value: Value,
    ) -> DatabaseResult<()> {
        expect_unit(self.send_write(WriteOperation::SetRelationshipProperty { id, key, value })?)
    }

    pub fn remove_relationship_property(
        &self,
        id: RelationshipId,
        key: String,
    ) -> DatabaseResult<()> {
        expect_unit(self.send_write(WriteOperation::RemoveRelationshipProperty { id, key })?)
    }

    pub fn delete_relationship(&self, id: RelationshipId) -> DatabaseResult<()> {
        expect_unit(self.send_write(WriteOperation::DeleteRelationship { id })?)
    }

    pub fn relationship_owner_shard(&self, id: RelationshipId) -> DatabaseResult<ShardId> {
        self.inner
            .lock()
            .map_err(|_| DatabaseError::LockPoisoned)?
            .relationship_owner_shard(id)
    }

    pub fn node(&self, id: NodeId) -> DatabaseResult<Option<Node>> {
        self.inner
            .lock()
            .map_err(|_| DatabaseError::LockPoisoned)?
            .node(id)
    }

    pub fn relationship(&self, id: RelationshipId) -> DatabaseResult<Option<Relationship>> {
        self.inner
            .lock()
            .map_err(|_| DatabaseError::LockPoisoned)?
            .relationship(id)
    }

    pub fn delete_node(&self, id: NodeId) -> DatabaseResult<()> {
        expect_unit(self.send_write(WriteOperation::DeleteNode { id })?)
    }

    pub fn execute_cypher(&self, query: &str) -> DatabaseResult<Vec<QueryRow>> {
        self.execute_cypher_with_params(query, QueryParams::new())
    }

    pub fn execute_cypher_with_params(
        &self,
        query: &str,
        params: QueryParams,
    ) -> DatabaseResult<Vec<QueryRow>> {
        if let Some(rows) = self.show_index_rows_for_query(query)? {
            return Ok(rows);
        }
        if let Some(rows) = self.show_constraint_rows_for_query(query)? {
            return Ok(rows);
        }
        match parse_write_cypher(query, &params)? {
            Some(WriteCypher::CreateNode {
                variable,
                labels,
                properties,
                assignments,
                replacement,
                returns,
            }) => {
                let properties = create_properties_after_set(properties, assignments, replacement);
                let id = self.create_node(labels.clone(), properties.clone())?;
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
                let from_ids = self.match_node_ids(&from_matcher, &params)?;
                let to_ids = self.match_node_ids(&to_matcher, &params)?;
                let mut rows = Vec::new();
                for from in &from_ids {
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
                let node_id = self.create_node(labels.clone(), node_properties.clone())?;
                let node = Node::new(node_id, labels, node_properties);
                let matched_ids = self.match_node_ids(&matched_matcher, &params)?;
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
            Some(WriteCypher::MergeNode { .. }) => {
                self.lock()?.execute_cypher_with_params(query, &params)
            }
            Some(WriteCypher::MergeRelationship { .. }) => {
                self.lock()?.execute_cypher_with_params(query, &params)
            }
            Some(WriteCypher::SetNodeProperty {
                matcher,
                assignments,
                returns,
            }) => {
                let node_ids = self.match_node_ids(&matcher, &params)?;
                for id in &node_ids {
                    for assignment in &assignments {
                        apply_node_property_assignment_with_handle(self, *id, assignment)?;
                    }
                }
                return_nodes_after_write(&node_ids, returns.as_ref(), |id| self.node(id))
            }
            Some(WriteCypher::ReplaceNodeProperties {
                matcher,
                properties,
                returns,
            }) => {
                let node_ids = self.match_node_ids(&matcher, &params)?;
                for id in &node_ids {
                    let current = self
                        .node(*id)?
                        .ok_or(DatabaseError::Graph(GraphError::NodeNotFound(*id)))?;
                    replace_node_properties_with_handle(
                        self,
                        *id,
                        &current.properties,
                        &properties,
                    )?;
                }
                return_nodes_after_write(&node_ids, returns.as_ref(), |id| self.node(id))
            }
            Some(WriteCypher::RemoveNodeProperty {
                matcher,
                keys,
                returns,
            }) => {
                let node_ids = self.match_node_ids(&matcher, &params)?;
                for id in &node_ids {
                    for key in &keys {
                        self.remove_node_property(*id, key.clone())?;
                    }
                }
                return_nodes_after_write(&node_ids, returns.as_ref(), |id| self.node(id))
            }
            Some(WriteCypher::AddNodeLabel {
                matcher,
                labels,
                returns,
            }) => {
                let node_ids = self.match_node_ids(&matcher, &params)?;
                for id in &node_ids {
                    for label in &labels {
                        self.add_node_label(*id, label.clone())?;
                    }
                }
                return_nodes_after_write(&node_ids, returns.as_ref(), |id| self.node(id))
            }
            Some(WriteCypher::RemoveNodeLabel {
                matcher,
                labels,
                returns,
            }) => {
                let node_ids = self.match_node_ids(&matcher, &params)?;
                for id in &node_ids {
                    for label in &labels {
                        self.remove_node_label(*id, label.clone())?;
                    }
                }
                return_nodes_after_write(&node_ids, returns.as_ref(), |id| self.node(id))
            }
            Some(WriteCypher::SetRelationshipProperty {
                matcher,
                assignments,
                returns,
            }) => {
                let relationship_ids = self.match_relationship_ids(&matcher, &params)?;
                for id in &relationship_ids {
                    for assignment in &assignments {
                        apply_relationship_property_assignment_with_handle(self, *id, assignment)?;
                    }
                }
                return_relationships_after_write(&relationship_ids, returns.as_ref(), |id| {
                    self.relationship(id)
                })
            }
            Some(WriteCypher::ReplaceRelationshipProperties {
                matcher,
                properties,
                returns,
            }) => {
                let relationship_ids = self.match_relationship_ids(&matcher, &params)?;
                for id in &relationship_ids {
                    let current = self
                        .relationship(*id)?
                        .ok_or(DatabaseError::Graph(GraphError::RelationshipNotFound(*id)))?;
                    replace_relationship_properties_with_handle(
                        self,
                        *id,
                        &current.properties,
                        &properties,
                    )?;
                }
                return_relationships_after_write(&relationship_ids, returns.as_ref(), |id| {
                    self.relationship(id)
                })
            }
            Some(WriteCypher::RemoveRelationshipProperty {
                matcher,
                keys,
                returns,
            }) => {
                let relationship_ids = self.match_relationship_ids(&matcher, &params)?;
                for id in &relationship_ids {
                    for key in &keys {
                        self.remove_relationship_property(*id, key.clone())?;
                    }
                }
                return_relationships_after_write(&relationship_ids, returns.as_ref(), |id| {
                    self.relationship(id)
                })
            }
            Some(WriteCypher::DeleteNode { matcher, returns }) => {
                let node_ids = self.match_node_ids(&matcher, &params)?;
                let rows =
                    return_nodes_after_write(&node_ids, returns.as_ref(), |id| self.node(id))?;
                for id in &node_ids {
                    self.delete_node(*id)?;
                }
                Ok(rows)
            }
            Some(WriteCypher::DeleteRelationship { matcher, returns }) => {
                let relationship_ids = self.match_relationship_ids(&matcher, &params)?;
                let rows =
                    return_relationships_after_write(&relationship_ids, returns.as_ref(), |id| {
                        self.relationship(id)
                    })?;
                for id in &relationship_ids {
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
                if if_not_exists {
                    self.lock()?
                        .create_node_property_index_if_not_exists(name, label, property)?;
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
                if if_not_exists {
                    self.lock()?
                        .create_unique_node_property_constraint_if_not_exists(
                            name, label, property,
                        )?;
                } else {
                    self.lock()?
                        .create_unique_node_property_constraint(name, label, property)?;
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
                if if_not_exists {
                    self.lock()?.create_vector_index_if_not_exists(
                        name, label, property, dimensions, metric,
                    )?;
                } else {
                    self.create_vector_index(name, label, property, dimensions, metric)?;
                }
                Ok(Vec::new())
            }
            Some(WriteCypher::RebuildVectorIndex { name }) => {
                self.rebuild_vector_index(&name)?;
                Ok(Vec::new())
            }
            Some(WriteCypher::DropIndex { name, if_exists }) => {
                if if_exists {
                    self.drop_index_if_exists(&name)?;
                } else {
                    self.drop_index(&name)?;
                }
                Ok(Vec::new())
            }
            Some(WriteCypher::DropConstraint { name, if_exists }) => {
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

}
