use super::metadata_types::*;
use super::write_cypher_helpers::*;
use super::write_cypher_model::*;
use super::*;

mod read_api;
mod schema_index;

impl Neo4rDatabase {
    pub(super) fn execute_write_cypher_on_optional_shard(
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
}
