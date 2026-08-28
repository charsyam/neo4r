impl Neo4rReadSnapshot {
    pub fn timestamp(&self) -> HybridTimestamp {
        self.timestamp
    }

    pub fn applied_indexes(&self) -> &[LogIndex] {
        &self.applied_indexes
    }

    pub fn committed_indexes(&self) -> &[LogIndex] {
        &self.committed_indexes
    }

    pub fn query(&self, query: &str) -> DatabaseResult<Vec<QueryRow>> {
        Ok(self.query_engine.execute(&self.store, query)?)
    }

    pub fn node(&self, id: NodeId) -> DatabaseResult<Option<Node>> {
        self.store.node(id).map_err(DatabaseError::from)
    }

    pub fn relationship(&self, id: RelationshipId) -> DatabaseResult<Option<Relationship>> {
        self.store.relationship(id).map_err(DatabaseError::from)
    }

    pub fn query_with_params(
        &self,
        query: &str,
        params: &QueryParams,
    ) -> DatabaseResult<Vec<QueryRow>> {
        Ok(self
            .query_engine
            .execute_with_params(&self.store, query, params)?)
    }

    pub fn query_shard(&self, shard_id: ShardId, query: &str) -> DatabaseResult<Vec<QueryRow>> {
        self.query_shard_with_params(shard_id, query, &QueryParams::new())
    }

    pub fn query_shard_with_params(
        &self,
        shard_id: ShardId,
        query: &str,
        params: &QueryParams,
    ) -> DatabaseResult<Vec<QueryRow>> {
        let graph = ShardScopedGraphRead::new(&self.store, self.shard_map, shard_id)?;
        Ok(self
            .query_engine
            .execute_with_params(&graph, query, params)?)
    }

    fn query_shard_with_staged_writes(
        &self,
        shard_id: ShardId,
        query: &str,
        params: &QueryParams,
        staged_writes: &[(String, QueryParams)],
    ) -> DatabaseResult<Vec<QueryRow>> {
        let overlay = self.build_staged_overlay(staged_writes)?;
        let overlay_graph = StagedOverlayGraph {
            base: &self.store,
            node_overlay: &overlay.nodes,
            relationship_overlay: &overlay.relationships,
        };
        let graph = ShardScopedGraphRead::new(&overlay_graph, self.shard_map, shard_id)?;
        Ok(self
            .query_engine
            .execute_with_params(&graph, query, params)?)
    }

    pub fn query_cursor(&self, query: &str) -> DatabaseResult<Box<dyn QueryCursor>> {
        Ok(self.query_engine.execute_cursor(&self.store, query)?)
    }

    pub fn query_cursor_with_params(
        &self,
        query: &str,
        params: &QueryParams,
    ) -> DatabaseResult<Box<dyn QueryCursor>> {
        Ok(self
            .query_engine
            .execute_cursor_with_params(&self.store, query, params)?)
    }

    pub fn into_query_cursor(self, query: &str) -> DatabaseResult<Box<dyn QueryCursor>> {
        self.into_query_cursor_with_params(query, QueryParams::new())
    }

    pub fn into_query_cursor_with_params(
        self,
        query: &str,
        params: QueryParams,
    ) -> DatabaseResult<Box<dyn QueryCursor>> {
        let engine = self.query_engine;
        Ok(engine.execute_owned_cursor_with_params(Arc::new(self.store), query, params)?)
    }

    fn build_staged_overlay(
        &self,
        staged_writes: &[(String, QueryParams)],
    ) -> DatabaseResult<StagedOverlay> {
        let mut node_overlay = HashMap::new();
        let mut relationship_overlay = HashMap::new();
        let mut temp_node_ids = BTreeSet::new();
        let mut temp_relationship_ids = BTreeSet::new();
        let mut next_temp_node_id = STAGED_TEMP_NODE_ID_START;
        let mut next_temp_relationship_id = STAGED_TEMP_RELATIONSHIP_ID_START;
        for (query, params) in staged_writes {
            match parse_write_cypher(query, params)? {
                Some(WriteCypher::CreateNode {
                    labels,
                    properties,
                    assignments,
                    replacement,
                    ..
                }) => {
                    let properties =
                        create_properties_after_set(properties, assignments, replacement);
                    let id = allocate_staged_id(&mut next_temp_node_id)?;
                    temp_node_ids.insert(id);
                    node_overlay.insert(id, Some(Node::new(id, labels, properties)));
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
                    let relationships = {
                        let graph = StagedOverlayGraph {
                            base: &self.store,
                            node_overlay: &node_overlay,
                            relationship_overlay: &relationship_overlay,
                        };
                        let from_ids = query_match_node_ids(
                            |match_query| {
                                Ok(self.query_engine.execute_with_params(
                                    &graph,
                                    match_query,
                                    params,
                                )?)
                            },
                            &from_matcher,
                        )?;
                        let to_ids = query_match_node_ids(
                            |match_query| {
                                Ok(self.query_engine.execute_with_params(
                                    &graph,
                                    match_query,
                                    params,
                                )?)
                            },
                            &to_matcher,
                        )?;
                        let mut relationships = Vec::new();
                        for from in &from_ids {
                            for to in &to_ids {
                                let id = allocate_staged_id(&mut next_temp_relationship_id)?;
                                temp_relationship_ids.insert(id);
                                relationships.push(Relationship::new(
                                    id,
                                    *from,
                                    *to,
                                    rel_type.clone(),
                                    properties.clone(),
                                ));
                            }
                        }
                        relationships
                    };
                    for relationship in relationships {
                        relationship_overlay.insert(relationship.id, Some(relationship));
                    }
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
                    let mut node = {
                        let graph = StagedOverlayGraph {
                            base: &self.store,
                            node_overlay: &node_overlay,
                            relationship_overlay: &relationship_overlay,
                        };
                        find_merge_node_in_graph(&graph, &labels, &properties)?
                    };
                    match node.as_mut() {
                        Some(node) => {
                            node.properties = properties_after_set(
                                node.properties.clone(),
                                &on_match,
                                on_match_replacement.as_ref(),
                            );
                            node_overlay.insert(node.id, Some(node.clone()));
                        }
                        None => {
                            let id = allocate_staged_id(&mut next_temp_node_id)?;
                            temp_node_ids.insert(id);
                            let create_properties = properties_after_set(
                                properties,
                                &on_create,
                                on_create_replacement.as_ref(),
                            );
                            node_overlay.insert(id, Some(Node::new(id, labels, create_properties)));
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
                    let merged_relationships = {
                        let graph = StagedOverlayGraph {
                            base: &self.store,
                            node_overlay: &node_overlay,
                            relationship_overlay: &relationship_overlay,
                        };
                        let from_ids = query_match_node_ids(
                            |match_query| {
                                Ok(self.query_engine.execute_with_params(
                                    &graph,
                                    match_query,
                                    params,
                                )?)
                            },
                            &from_matcher,
                        )?;
                        let to_ids = query_match_node_ids(
                            |match_query| {
                                Ok(self.query_engine.execute_with_params(
                                    &graph,
                                    match_query,
                                    params,
                                )?)
                            },
                            &to_matcher,
                        )?;
                        let mut relationships = Vec::new();
                        for from in &from_ids {
                            for to in &to_ids {
                                match find_merge_relationship_in_graph(
                                    &graph,
                                    *from,
                                    *to,
                                    &rel_type,
                                    &properties,
                                )? {
                                    Some(mut relationship) => {
                                        relationship.properties = properties_after_set(
                                            relationship.properties.clone(),
                                            &on_match,
                                            on_match_replacement.as_ref(),
                                        );
                                        relationships.push((false, relationship));
                                    }
                                    None => {
                                        let id =
                                            allocate_staged_id(&mut next_temp_relationship_id)?;
                                        let create_properties = properties_after_set(
                                            properties.clone(),
                                            &on_create,
                                            on_create_replacement.as_ref(),
                                        );
                                        relationships.push((
                                            true,
                                            Relationship::new(
                                                id,
                                                *from,
                                                *to,
                                                rel_type.clone(),
                                                create_properties,
                                            ),
                                        ));
                                    }
                                }
                            }
                        }
                        relationships
                    };
                    for (created, relationship) in merged_relationships {
                        if created {
                            temp_relationship_ids.insert(relationship.id);
                        }
                        relationship_overlay.insert(relationship.id, Some(relationship));
                    }
                }
                Some(WriteCypher::SetNodeProperty {
                    matcher,
                    assignments,
                    ..
                }) => {
                    let updated_nodes = {
                        let graph = StagedOverlayGraph {
                            base: &self.store,
                            node_overlay: &node_overlay,
                            relationship_overlay: &relationship_overlay,
                        };
                        let node_ids = query_match_node_ids(
                            |match_query| {
                                Ok(self.query_engine.execute_with_params(
                                    &graph,
                                    match_query,
                                    params,
                                )?)
                            },
                            &matcher,
                        )?;
                        let mut updated_nodes = Vec::new();
                        for id in node_ids {
                            let mut node = graph.node(id)?.ok_or(GraphError::NodeNotFound(id))?;
                            apply_assignments_to_properties(&mut node.properties, &assignments);
                            updated_nodes.push((id, node));
                        }
                        updated_nodes
                    };
                    for (id, node) in updated_nodes {
                        node_overlay.insert(id, Some(node));
                    }
                }
                Some(WriteCypher::ReplaceNodeProperties {
                    matcher,
                    properties,
                    ..
                }) => {
                    let updated_nodes = {
                        let graph = StagedOverlayGraph {
                            base: &self.store,
                            node_overlay: &node_overlay,
                            relationship_overlay: &relationship_overlay,
                        };
                        let node_ids = query_match_node_ids(
                            |match_query| {
                                Ok(self.query_engine.execute_with_params(
                                    &graph,
                                    match_query,
                                    params,
                                )?)
                            },
                            &matcher,
                        )?;
                        let mut updated_nodes = Vec::new();
                        for id in node_ids {
                            let mut node = graph.node(id)?.ok_or(GraphError::NodeNotFound(id))?;
                            node.properties = properties.clone();
                            updated_nodes.push((id, node));
                        }
                        updated_nodes
                    };
                    for (id, node) in updated_nodes {
                        node_overlay.insert(id, Some(node));
                    }
                }
                Some(WriteCypher::RemoveNodeProperty { matcher, keys, .. }) => {
                    let updated_nodes = {
                        let graph = StagedOverlayGraph {
                            base: &self.store,
                            node_overlay: &node_overlay,
                            relationship_overlay: &relationship_overlay,
                        };
                        let node_ids = query_match_node_ids(
                            |match_query| {
                                Ok(self.query_engine.execute_with_params(
                                    &graph,
                                    match_query,
                                    params,
                                )?)
                            },
                            &matcher,
                        )?;
                        let mut updated_nodes = Vec::new();
                        for id in node_ids {
                            let mut node = graph.node(id)?.ok_or(GraphError::NodeNotFound(id))?;
                            for key in &keys {
                                node.properties.remove(key);
                            }
                            updated_nodes.push((id, node));
                        }
                        updated_nodes
                    };
                    for (id, node) in updated_nodes {
                        node_overlay.insert(id, Some(node));
                    }
                }
                Some(WriteCypher::AddNodeLabel {
                    matcher, labels, ..
                }) => {
                    let updated_nodes = {
                        let graph = StagedOverlayGraph {
                            base: &self.store,
                            node_overlay: &node_overlay,
                            relationship_overlay: &relationship_overlay,
                        };
                        let node_ids = query_match_node_ids(
                            |match_query| {
                                Ok(self.query_engine.execute_with_params(
                                    &graph,
                                    match_query,
                                    params,
                                )?)
                            },
                            &matcher,
                        )?;
                        let mut updated_nodes = Vec::new();
                        for id in node_ids {
                            let mut node = graph.node(id)?.ok_or(GraphError::NodeNotFound(id))?;
                            for label in &labels {
                                if !node.labels.iter().any(|existing| existing == label) {
                                    node.labels.push(label.clone());
                                }
                            }
                            updated_nodes.push((id, node));
                        }
                        updated_nodes
                    };
                    for (id, node) in updated_nodes {
                        node_overlay.insert(id, Some(node));
                    }
                }
                Some(WriteCypher::RemoveNodeLabel {
                    matcher, labels, ..
                }) => {
                    let updated_nodes = {
                        let graph = StagedOverlayGraph {
                            base: &self.store,
                            node_overlay: &node_overlay,
                            relationship_overlay: &relationship_overlay,
                        };
                        let node_ids = query_match_node_ids(
                            |match_query| {
                                Ok(self.query_engine.execute_with_params(
                                    &graph,
                                    match_query,
                                    params,
                                )?)
                            },
                            &matcher,
                        )?;
                        let mut updated_nodes = Vec::new();
                        for id in node_ids {
                            let mut node = graph.node(id)?.ok_or(GraphError::NodeNotFound(id))?;
                            node.labels
                                .retain(|existing| !labels.iter().any(|label| label == existing));
                            updated_nodes.push((id, node));
                        }
                        updated_nodes
                    };
                    for (id, node) in updated_nodes {
                        node_overlay.insert(id, Some(node));
                    }
                }
                Some(WriteCypher::SetRelationshipProperty {
                    matcher,
                    assignments,
                    ..
                }) => {
                    let updated_relationships = {
                        let graph = StagedOverlayGraph {
                            base: &self.store,
                            node_overlay: &node_overlay,
                            relationship_overlay: &relationship_overlay,
                        };
                        let relationship_ids = query_match_relationship_ids(
                            |match_query| {
                                Ok(self.query_engine.execute_with_params(
                                    &graph,
                                    match_query,
                                    params,
                                )?)
                            },
                            &matcher,
                        )?;
                        let mut updated_relationships = Vec::new();
                        for id in relationship_ids {
                            let mut relationship = graph
                                .relationship(id)?
                                .ok_or(GraphError::RelationshipNotFound(id))?;
                            apply_assignments_to_properties(
                                &mut relationship.properties,
                                &assignments,
                            );
                            updated_relationships.push((id, relationship));
                        }
                        updated_relationships
                    };
                    for (id, relationship) in updated_relationships {
                        relationship_overlay.insert(id, Some(relationship));
                    }
                }
                Some(WriteCypher::ReplaceRelationshipProperties {
                    matcher,
                    properties,
                    ..
                }) => {
                    let updated_relationships = {
                        let graph = StagedOverlayGraph {
                            base: &self.store,
                            node_overlay: &node_overlay,
                            relationship_overlay: &relationship_overlay,
                        };
                        let relationship_ids = query_match_relationship_ids(
                            |match_query| {
                                Ok(self.query_engine.execute_with_params(
                                    &graph,
                                    match_query,
                                    params,
                                )?)
                            },
                            &matcher,
                        )?;
                        let mut updated_relationships = Vec::new();
                        for id in relationship_ids {
                            let mut relationship = graph
                                .relationship(id)?
                                .ok_or(GraphError::RelationshipNotFound(id))?;
                            relationship.properties = properties.clone();
                            updated_relationships.push((id, relationship));
                        }
                        updated_relationships
                    };
                    for (id, relationship) in updated_relationships {
                        relationship_overlay.insert(id, Some(relationship));
                    }
                }
                Some(WriteCypher::RemoveRelationshipProperty { matcher, keys, .. }) => {
                    let updated_relationships = {
                        let graph = StagedOverlayGraph {
                            base: &self.store,
                            node_overlay: &node_overlay,
                            relationship_overlay: &relationship_overlay,
                        };
                        let relationship_ids = query_match_relationship_ids(
                            |match_query| {
                                Ok(self.query_engine.execute_with_params(
                                    &graph,
                                    match_query,
                                    params,
                                )?)
                            },
                            &matcher,
                        )?;
                        let mut updated_relationships = Vec::new();
                        for id in relationship_ids {
                            let mut relationship = graph
                                .relationship(id)?
                                .ok_or(GraphError::RelationshipNotFound(id))?;
                            for key in &keys {
                                relationship.properties.remove(key);
                            }
                            updated_relationships.push((id, relationship));
                        }
                        updated_relationships
                    };
                    for (id, relationship) in updated_relationships {
                        relationship_overlay.insert(id, Some(relationship));
                    }
                }
                Some(WriteCypher::DeleteRelationship { matcher, .. }) => {
                    let relationship_ids = {
                        let graph = StagedOverlayGraph {
                            base: &self.store,
                            node_overlay: &node_overlay,
                            relationship_overlay: &relationship_overlay,
                        };
                        query_match_relationship_ids(
                            |match_query| {
                                Ok(self.query_engine.execute_with_params(
                                    &graph,
                                    match_query,
                                    params,
                                )?)
                            },
                            &matcher,
                        )?
                    };
                    for id in relationship_ids {
                        relationship_overlay.insert(id, None);
                    }
                }
                Some(WriteCypher::DeleteNode { matcher, .. }) => {
                    let (node_ids, relationship_ids) = {
                        let graph = StagedOverlayGraph {
                            base: &self.store,
                            node_overlay: &node_overlay,
                            relationship_overlay: &relationship_overlay,
                        };
                        let node_ids = query_match_node_ids(
                            |match_query| {
                                Ok(self.query_engine.execute_with_params(
                                    &graph,
                                    match_query,
                                    params,
                                )?)
                            },
                            &matcher,
                        )?;
                        let mut relationship_ids = BTreeSet::new();
                        for id in &node_ids {
                            for relationship in graph.outgoing(*id)? {
                                relationship_ids.insert(relationship.id);
                            }
                            for relationship in graph.incoming(*id)? {
                                relationship_ids.insert(relationship.id);
                            }
                        }
                        (node_ids, relationship_ids)
                    };
                    for id in node_ids {
                        node_overlay.insert(id, None);
                    }
                    for id in relationship_ids {
                        relationship_overlay.insert(id, None);
                    }
                }
                Some(_) => {
                    return Err(DatabaseError::InvalidConfig(
                        "read-your-writes overlay currently supports staged node and relationship CREATE, MERGE, SET, REMOVE, and DELETE only"
                            .to_string(),
                    ));
                }
                None => {
                    return Err(DatabaseError::InvalidConfig(
                        "read-your-writes overlay requires staged write queries".to_string(),
                    ));
                }
            }
        }
        Ok(StagedOverlay {
            nodes: node_overlay,
            relationships: relationship_overlay,
            temp_node_ids,
            temp_relationship_ids,
        })
    }
}
