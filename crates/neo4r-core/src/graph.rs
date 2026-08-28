use crate::command::Command;
use crate::error::{GraphError, Result};
use crate::model::{Node, NodeId, Properties, Relationship, RelationshipId, Value, ValueKey};
use crate::shard::{BoundaryNode, ShardId};
use std::collections::{HashMap, HashSet};

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct LabelPropertyKey {
    pub label: String,
    pub property_key: String,
    pub property_value: ValueKey,
}

impl LabelPropertyKey {
    pub fn new(label: String, property_key: String, property_value: ValueKey) -> Self {
        Self {
            label,
            property_key,
            property_value,
        }
    }
}

#[derive(Clone, Debug, Default, Eq, Hash, PartialEq)]
pub struct RelationshipTypeKey {
    pub node_id: NodeId,
    pub rel_type: String,
}

impl RelationshipTypeKey {
    pub fn new(node_id: NodeId, rel_type: String) -> Self {
        Self { node_id, rel_type }
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
struct GraphIndexes {
    label_nodes: HashMap<String, HashSet<NodeId>>,
    label_property_nodes: HashMap<LabelPropertyKey, HashSet<NodeId>>,
    boundary_label_property_nodes: HashMap<LabelPropertyKey, HashSet<NodeId>>,
    outgoing_by_type: HashMap<RelationshipTypeKey, Vec<RelationshipId>>,
    incoming_by_type: HashMap<RelationshipTypeKey, Vec<RelationshipId>>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct GraphState {
    nodes: HashMap<NodeId, Node>,
    boundary_nodes: HashMap<NodeId, BoundaryNode>,
    relationships: HashMap<RelationshipId, Relationship>,
    outgoing: HashMap<NodeId, Vec<RelationshipId>>,
    incoming: HashMap<NodeId, Vec<RelationshipId>>,
    indexes: GraphIndexes,
    next_node_id: NodeId,
    next_relationship_id: RelationshipId,
}

impl GraphState {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn allocate_node_id(&mut self) -> NodeId {
        let id = self.next_node_id;
        self.next_node_id += 1;
        id
    }

    pub fn allocate_relationship_id(&mut self) -> RelationshipId {
        let id = self.next_relationship_id;
        self.next_relationship_id += 1;
        id
    }

    pub fn node(&self, id: NodeId) -> Option<&Node> {
        self.nodes.get(&id)
    }

    pub fn boundary_node(&self, id: NodeId) -> Option<&BoundaryNode> {
        self.boundary_nodes.get(&id)
    }

    pub fn relationship(&self, id: RelationshipId) -> Option<&Relationship> {
        self.relationships.get(&id)
    }

    pub fn nodes(&self) -> impl Iterator<Item = &Node> {
        self.nodes.values()
    }

    pub fn node_ids(&self) -> Vec<NodeId> {
        let mut ids = self.nodes.keys().copied().collect::<Vec<_>>();
        ids.sort_unstable();
        ids
    }

    pub fn boundary_nodes(&self) -> impl Iterator<Item = &BoundaryNode> {
        self.boundary_nodes.values()
    }

    pub fn relationships(&self) -> impl Iterator<Item = &Relationship> {
        self.relationships.values()
    }

    pub fn node_ids_by_label(&self, label: &str) -> Vec<NodeId> {
        sorted_ids(self.indexes.label_nodes.get(label))
    }

    pub fn node_ids_by_label_property(
        &self,
        label: &str,
        property_key: &str,
        property_value: &Value,
    ) -> Vec<NodeId> {
        let key = LabelPropertyKey::new(
            label.to_string(),
            property_key.to_string(),
            ValueKey::from(property_value),
        );
        sorted_ids(self.indexes.label_property_nodes.get(&key))
    }

    pub fn boundary_node_ids_by_label_property(
        &self,
        label: &str,
        property_key: &str,
        property_value: &Value,
    ) -> Vec<NodeId> {
        let key = LabelPropertyKey::new(
            label.to_string(),
            property_key.to_string(),
            ValueKey::from(property_value),
        );
        sorted_ids(self.indexes.boundary_label_property_nodes.get(&key))
    }

    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    pub fn boundary_node_count(&self) -> usize {
        self.boundary_nodes.len()
    }

    pub fn relationship_count(&self) -> usize {
        self.relationships.len()
    }

    pub fn outgoing(&self, node_id: NodeId) -> Result<Vec<&Relationship>> {
        self.ensure_node_exists(node_id)?;
        Ok(self
            .outgoing
            .get(&node_id)
            .into_iter()
            .flat_map(|ids| ids.iter())
            .filter_map(|id| self.relationships.get(id))
            .collect())
    }

    pub fn outgoing_by_type(&self, node_id: NodeId, rel_type: &str) -> Result<Vec<&Relationship>> {
        self.ensure_node_exists(node_id)?;
        let key = RelationshipTypeKey::new(node_id, rel_type.to_string());
        Ok(self
            .indexes
            .outgoing_by_type
            .get(&key)
            .into_iter()
            .flat_map(|ids| ids.iter())
            .filter_map(|id| self.relationships.get(id))
            .collect())
    }

    pub fn incoming(&self, node_id: NodeId) -> Result<Vec<&Relationship>> {
        self.ensure_node_exists(node_id)?;
        Ok(self
            .incoming
            .get(&node_id)
            .into_iter()
            .flat_map(|ids| ids.iter())
            .filter_map(|id| self.relationships.get(id))
            .collect())
    }

    pub fn incoming_by_type(&self, node_id: NodeId, rel_type: &str) -> Result<Vec<&Relationship>> {
        self.ensure_node_exists(node_id)?;
        let key = RelationshipTypeKey::new(node_id, rel_type.to_string());
        Ok(self
            .indexes
            .incoming_by_type
            .get(&key)
            .into_iter()
            .flat_map(|ids| ids.iter())
            .filter_map(|id| self.relationships.get(id))
            .collect())
    }

    pub fn apply(&mut self, command: Command) -> Result<()> {
        match command {
            Command::CreateNode {
                id,
                labels,
                properties,
            } => self.create_node(id, labels, properties),
            Command::CreateRelationship {
                id,
                from,
                to,
                rel_type,
                properties,
            } => self.create_relationship(id, from, to, rel_type, properties),
            Command::UpsertBoundaryNode {
                id,
                owner_shard,
                labels,
                properties,
                version,
            } => self.upsert_boundary_node(id, owner_shard, labels, properties, version),
            Command::SetNodeProperty { id, key, value } => self.set_node_property(id, key, value),
            Command::RemoveNodeProperty { id, key } => self.remove_node_property(id, key),
            Command::AddNodeLabel { id, label } => self.add_node_label(id, label),
            Command::RemoveNodeLabel { id, label } => self.remove_node_label(id, label),
            Command::SetRelationshipProperty { id, key, value } => {
                self.set_relationship_property(id, key, value)
            }
            Command::RemoveRelationshipProperty { id, key } => {
                self.remove_relationship_property(id, key)
            }
            Command::DeleteRelationship { id } => self.delete_relationship(id),
            Command::DeleteNode { id } => self.delete_node(id),
            Command::ClusterConfigChange { .. } => Ok(()),
        }
    }

    fn create_node(
        &mut self,
        id: NodeId,
        labels: Vec<String>,
        properties: Properties,
    ) -> Result<()> {
        if self.nodes.contains_key(&id) {
            return Err(GraphError::NodeAlreadyExists(id));
        }

        self.next_node_id = self.next_node_id.max(id.saturating_add(1));
        let node = Node::new(id, labels, properties);
        index_node(&mut self.indexes, &node);
        self.nodes.insert(id, node);
        self.outgoing.entry(id).or_default();
        self.incoming.entry(id).or_default();
        Ok(())
    }

    fn create_relationship(
        &mut self,
        id: RelationshipId,
        from: NodeId,
        to: NodeId,
        rel_type: String,
        properties: Properties,
    ) -> Result<()> {
        if self.relationships.contains_key(&id) {
            return Err(GraphError::RelationshipAlreadyExists(id));
        }
        if !self.nodes.contains_key(&from) {
            return Err(GraphError::RelationshipEndpointNotFound {
                relationship_id: id,
                node_id: from,
            });
        }
        if !self.nodes.contains_key(&to) {
            if self.boundary_nodes.contains_key(&to) {
                self.next_relationship_id = self.next_relationship_id.max(id.saturating_add(1));
                let relationship = Relationship::new(id, from, to, rel_type, properties);
                index_relationship(&mut self.indexes, &relationship, false);
                self.relationships.insert(id, relationship);
                self.outgoing.entry(from).or_default().push(id);
                return Ok(());
            }
            return Err(GraphError::RelationshipEndpointNotFound {
                relationship_id: id,
                node_id: to,
            });
        }

        self.next_relationship_id = self.next_relationship_id.max(id.saturating_add(1));
        let relationship = Relationship::new(id, from, to, rel_type, properties);
        index_relationship(&mut self.indexes, &relationship, true);
        self.relationships.insert(id, relationship);
        self.outgoing.entry(from).or_default().push(id);
        self.incoming.entry(to).or_default().push(id);
        Ok(())
    }

    fn upsert_boundary_node(
        &mut self,
        id: NodeId,
        owner_shard: ShardId,
        labels: Vec<String>,
        properties: Properties,
        version: u64,
    ) -> Result<()> {
        if let Some(old) = self.boundary_nodes.get(&id) {
            remove_boundary_node_index(&mut self.indexes, old);
        }
        let boundary_node = BoundaryNode::new(id, owner_shard, labels, properties, version);
        index_boundary_node(&mut self.indexes, &boundary_node);
        self.boundary_nodes.insert(id, boundary_node);
        Ok(())
    }

    fn set_node_property(&mut self, id: NodeId, key: String, value: Value) -> Result<()> {
        let node = self
            .nodes
            .get_mut(&id)
            .ok_or(GraphError::NodeNotFound(id))?;
        remove_node_index(&mut self.indexes, node);
        node.properties.insert(key, value);
        index_node(&mut self.indexes, node);
        Ok(())
    }

    fn remove_node_property(&mut self, id: NodeId, key: String) -> Result<()> {
        let node = self
            .nodes
            .get_mut(&id)
            .ok_or(GraphError::NodeNotFound(id))?;
        remove_node_index(&mut self.indexes, node);
        node.properties.remove(&key);
        index_node(&mut self.indexes, node);
        Ok(())
    }

    fn add_node_label(&mut self, id: NodeId, label: String) -> Result<()> {
        let node = self
            .nodes
            .get_mut(&id)
            .ok_or(GraphError::NodeNotFound(id))?;
        if node.labels.iter().any(|existing| existing == &label) {
            return Ok(());
        }
        remove_node_index(&mut self.indexes, node);
        node.labels.push(label);
        index_node(&mut self.indexes, node);
        Ok(())
    }

    fn remove_node_label(&mut self, id: NodeId, label: String) -> Result<()> {
        let node = self
            .nodes
            .get_mut(&id)
            .ok_or(GraphError::NodeNotFound(id))?;
        if !node.labels.iter().any(|existing| existing == &label) {
            return Ok(());
        }
        remove_node_index(&mut self.indexes, node);
        node.labels.retain(|existing| existing != &label);
        index_node(&mut self.indexes, node);
        Ok(())
    }

    fn set_relationship_property(
        &mut self,
        id: RelationshipId,
        key: String,
        value: Value,
    ) -> Result<()> {
        let relationship = self
            .relationships
            .get_mut(&id)
            .ok_or(GraphError::RelationshipNotFound(id))?;
        relationship.properties.insert(key, value);
        Ok(())
    }

    fn remove_relationship_property(&mut self, id: RelationshipId, key: String) -> Result<()> {
        let relationship = self
            .relationships
            .get_mut(&id)
            .ok_or(GraphError::RelationshipNotFound(id))?;
        relationship.properties.remove(&key);
        Ok(())
    }

    fn delete_relationship(&mut self, id: RelationshipId) -> Result<()> {
        let relationship = self
            .relationships
            .remove(&id)
            .ok_or(GraphError::RelationshipNotFound(id))?;

        remove_relationship_index(
            &mut self.indexes,
            &relationship,
            self.nodes.contains_key(&relationship.to),
        );
        remove_relationship_id(self.outgoing.get_mut(&relationship.from), id);
        remove_relationship_id(self.incoming.get_mut(&relationship.to), id);
        Ok(())
    }

    fn delete_node(&mut self, id: NodeId) -> Result<()> {
        self.ensure_node_exists(id)?;

        let mut relationship_ids = Vec::new();
        if let Some(outgoing) = self.outgoing.get(&id) {
            relationship_ids.extend(outgoing.iter().copied());
        }
        if let Some(incoming) = self.incoming.get(&id) {
            relationship_ids.extend(incoming.iter().copied());
        }
        relationship_ids.sort_unstable();
        relationship_ids.dedup();

        for relationship_id in relationship_ids {
            self.delete_relationship(relationship_id)?;
        }

        if let Some(node) = self.nodes.remove(&id) {
            remove_node_index(&mut self.indexes, &node);
        }
        self.outgoing.remove(&id);
        self.incoming.remove(&id);
        Ok(())
    }

    fn ensure_node_exists(&self, id: NodeId) -> Result<()> {
        if self.nodes.contains_key(&id) {
            Ok(())
        } else {
            Err(GraphError::NodeNotFound(id))
        }
    }
}

fn remove_relationship_id(ids: Option<&mut Vec<RelationshipId>>, id: RelationshipId) {
    if let Some(ids) = ids {
        ids.retain(|candidate| *candidate != id);
    }
}

fn sorted_ids(ids: Option<&HashSet<NodeId>>) -> Vec<NodeId> {
    let mut ids = ids
        .into_iter()
        .flat_map(|ids| ids.iter().copied())
        .collect::<Vec<_>>();
    ids.sort_unstable();
    ids
}

fn index_node(indexes: &mut GraphIndexes, node: &Node) {
    for label in &node.labels {
        indexes
            .label_nodes
            .entry(label.clone())
            .or_default()
            .insert(node.id);
        for (property_key, property_value) in &node.properties {
            indexes
                .label_property_nodes
                .entry(LabelPropertyKey::new(
                    label.clone(),
                    property_key.clone(),
                    ValueKey::from(property_value),
                ))
                .or_default()
                .insert(node.id);
        }
    }
}

fn remove_node_index(indexes: &mut GraphIndexes, node: &Node) {
    for label in &node.labels {
        remove_node_id(indexes.label_nodes.get_mut(label), node.id);
        for (property_key, property_value) in &node.properties {
            remove_node_id(
                indexes.label_property_nodes.get_mut(&LabelPropertyKey::new(
                    label.clone(),
                    property_key.clone(),
                    ValueKey::from(property_value),
                )),
                node.id,
            );
        }
    }
}

fn index_boundary_node(indexes: &mut GraphIndexes, node: &BoundaryNode) {
    for label in &node.labels {
        for (property_key, property_value) in &node.properties {
            indexes
                .boundary_label_property_nodes
                .entry(LabelPropertyKey::new(
                    label.clone(),
                    property_key.clone(),
                    ValueKey::from(property_value),
                ))
                .or_default()
                .insert(node.id);
        }
    }
}

fn remove_boundary_node_index(indexes: &mut GraphIndexes, node: &BoundaryNode) {
    for label in &node.labels {
        for (property_key, property_value) in &node.properties {
            remove_node_id(
                indexes
                    .boundary_label_property_nodes
                    .get_mut(&LabelPropertyKey::new(
                        label.clone(),
                        property_key.clone(),
                        ValueKey::from(property_value),
                    )),
                node.id,
            );
        }
    }
}

fn remove_node_id(ids: Option<&mut HashSet<NodeId>>, id: NodeId) {
    if let Some(ids) = ids {
        ids.remove(&id);
    }
}

fn index_relationship(indexes: &mut GraphIndexes, relationship: &Relationship, has_local_to: bool) {
    indexes
        .outgoing_by_type
        .entry(RelationshipTypeKey::new(
            relationship.from,
            relationship.rel_type.clone(),
        ))
        .or_default()
        .push(relationship.id);
    if has_local_to {
        indexes
            .incoming_by_type
            .entry(RelationshipTypeKey::new(
                relationship.to,
                relationship.rel_type.clone(),
            ))
            .or_default()
            .push(relationship.id);
    }
}

fn remove_relationship_index(
    indexes: &mut GraphIndexes,
    relationship: &Relationship,
    has_local_to: bool,
) {
    remove_relationship_id(
        indexes.outgoing_by_type.get_mut(&RelationshipTypeKey::new(
            relationship.from,
            relationship.rel_type.clone(),
        )),
        relationship.id,
    );
    if has_local_to {
        remove_relationship_id(
            indexes.incoming_by_type.get_mut(&RelationshipTypeKey::new(
                relationship.to,
                relationship.rel_type.clone(),
            )),
            relationship.id,
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn properties(entries: &[(&str, Value)]) -> Properties {
        entries
            .iter()
            .map(|(key, value)| ((*key).to_string(), value.clone()))
            .collect()
    }

    #[test]
    fn creates_and_reads_node() {
        let mut graph = GraphState::new();

        graph
            .apply(Command::CreateNode {
                id: 7,
                labels: vec!["Person".to_string()],
                properties: properties(&[("name", Value::String("Alice".to_string()))]),
            })
            .unwrap();

        let node = graph.node(7).unwrap();
        assert_eq!(node.labels, vec!["Person"]);
        assert_eq!(
            node.properties.get("name"),
            Some(&Value::String("Alice".to_string()))
        );
    }

    #[test]
    fn creates_relationship_and_traverses_both_directions() {
        let mut graph = GraphState::new();
        graph
            .apply(Command::CreateNode {
                id: 1,
                labels: vec![],
                properties: Properties::new(),
            })
            .unwrap();
        graph
            .apply(Command::CreateNode {
                id: 2,
                labels: vec![],
                properties: Properties::new(),
            })
            .unwrap();

        graph
            .apply(Command::CreateRelationship {
                id: 10,
                from: 1,
                to: 2,
                rel_type: "KNOWS".to_string(),
                properties: Properties::new(),
            })
            .unwrap();

        assert_eq!(graph.outgoing(1).unwrap()[0].id, 10);
        assert_eq!(graph.incoming(2).unwrap()[0].id, 10);
    }

    #[test]
    fn rejects_relationship_with_missing_endpoint() {
        let mut graph = GraphState::new();

        let err = graph
            .apply(Command::CreateRelationship {
                id: 1,
                from: 1,
                to: 2,
                rel_type: "KNOWS".to_string(),
                properties: Properties::new(),
            })
            .unwrap_err();

        assert_eq!(
            err,
            GraphError::RelationshipEndpointNotFound {
                relationship_id: 1,
                node_id: 1
            }
        );
    }

    #[test]
    fn property_updates_are_applied_deterministically() {
        let mut graph = GraphState::new();
        graph
            .apply(Command::CreateNode {
                id: 1,
                labels: vec![],
                properties: Properties::new(),
            })
            .unwrap();

        graph
            .apply(Command::SetNodeProperty {
                id: 1,
                key: "name".to_string(),
                value: Value::String("Alice".to_string()),
            })
            .unwrap();
        graph
            .apply(Command::SetNodeProperty {
                id: 1,
                key: "name".to_string(),
                value: Value::String("Bob".to_string()),
            })
            .unwrap();

        assert_eq!(
            graph.node(1).unwrap().properties.get("name"),
            Some(&Value::String("Bob".to_string()))
        );
    }

    #[test]
    fn deleting_node_removes_attached_relationships() {
        let mut graph = GraphState::new();
        for id in [1, 2, 3] {
            graph
                .apply(Command::CreateNode {
                    id,
                    labels: vec![],
                    properties: Properties::new(),
                })
                .unwrap();
        }
        graph
            .apply(Command::CreateRelationship {
                id: 10,
                from: 1,
                to: 2,
                rel_type: "KNOWS".to_string(),
                properties: Properties::new(),
            })
            .unwrap();
        graph
            .apply(Command::CreateRelationship {
                id: 11,
                from: 3,
                to: 1,
                rel_type: "KNOWS".to_string(),
                properties: Properties::new(),
            })
            .unwrap();

        graph.apply(Command::DeleteNode { id: 1 }).unwrap();

        assert_eq!(graph.node_count(), 2);
        assert_eq!(graph.relationship_count(), 0);
        assert!(graph.outgoing(2).unwrap().is_empty());
        assert!(graph.incoming(3).unwrap().is_empty());
    }

    #[test]
    fn replaying_same_commands_produces_same_state() {
        let commands = vec![
            Command::CreateNode {
                id: 1,
                labels: vec!["Person".to_string()],
                properties: properties(&[("name", Value::String("Alice".to_string()))]),
            },
            Command::CreateNode {
                id: 2,
                labels: vec!["Person".to_string()],
                properties: properties(&[("name", Value::String("Bob".to_string()))]),
            },
            Command::CreateRelationship {
                id: 1,
                from: 1,
                to: 2,
                rel_type: "KNOWS".to_string(),
                properties: Properties::new(),
            },
        ];

        let mut leader = GraphState::new();
        let mut follower = GraphState::new();

        for command in commands {
            leader.apply(command.clone()).unwrap();
            follower.apply(command).unwrap();
        }

        assert_eq!(leader, follower);
    }

    #[test]
    fn relationship_can_target_boundary_node() {
        let mut graph = GraphState::new();
        graph
            .apply(Command::CreateNode {
                id: 1,
                labels: vec!["Person".to_string()],
                properties: Properties::new(),
            })
            .unwrap();
        graph
            .apply(Command::UpsertBoundaryNode {
                id: 2,
                owner_shard: 2,
                labels: vec!["Person".to_string()],
                properties: properties(&[("name", Value::String("RemoteBob".to_string()))]),
                version: 1,
            })
            .unwrap();

        graph
            .apply(Command::CreateRelationship {
                id: 10,
                from: 1,
                to: 2,
                rel_type: "KNOWS".to_string(),
                properties: Properties::new(),
            })
            .unwrap();

        assert_eq!(graph.boundary_node_count(), 1);
        assert_eq!(graph.outgoing(1).unwrap()[0].to, 2);
        assert!(graph.incoming(2).is_err());
    }

    #[test]
    fn indexes_nodes_by_label_and_label_property() {
        let mut graph = GraphState::new();
        graph
            .apply(Command::CreateNode {
                id: 1,
                labels: vec!["Person".to_string()],
                properties: properties(&[("name", Value::String("Alice".to_string()))]),
            })
            .unwrap();
        graph
            .apply(Command::CreateNode {
                id: 2,
                labels: vec!["Person".to_string()],
                properties: properties(&[("name", Value::String("Bob".to_string()))]),
            })
            .unwrap();

        assert_eq!(graph.node_ids_by_label("Person"), vec![1, 2]);
        assert_eq!(
            graph.node_ids_by_label_property("Person", "name", &Value::String("Alice".to_string())),
            vec![1]
        );

        graph
            .apply(Command::SetNodeProperty {
                id: 1,
                key: "name".to_string(),
                value: Value::String("Alicia".to_string()),
            })
            .unwrap();

        assert!(graph
            .node_ids_by_label_property("Person", "name", &Value::String("Alice".to_string()))
            .is_empty());
        assert_eq!(
            graph.node_ids_by_label_property(
                "Person",
                "name",
                &Value::String("Alicia".to_string())
            ),
            vec![1]
        );

        graph
            .apply(Command::RemoveNodeProperty {
                id: 1,
                key: "name".to_string(),
            })
            .unwrap();

        assert_eq!(graph.node_ids_by_label("Person"), vec![1, 2]);
        assert!(graph
            .node_ids_by_label_property("Person", "name", &Value::String("Alicia".to_string()))
            .is_empty());
        assert!(!graph.node(1).unwrap().properties.contains_key("name"));
    }

    #[test]
    fn indexes_relationships_by_type() {
        let mut graph = GraphState::new();
        for id in [1, 2, 3] {
            graph
                .apply(Command::CreateNode {
                    id,
                    labels: vec![],
                    properties: Properties::new(),
                })
                .unwrap();
        }
        graph
            .apply(Command::CreateRelationship {
                id: 10,
                from: 1,
                to: 2,
                rel_type: "KNOWS".to_string(),
                properties: Properties::new(),
            })
            .unwrap();
        graph
            .apply(Command::CreateRelationship {
                id: 11,
                from: 1,
                to: 3,
                rel_type: "LIKES".to_string(),
                properties: Properties::new(),
            })
            .unwrap();

        assert_eq!(graph.outgoing_by_type(1, "KNOWS").unwrap()[0].id, 10);
        assert_eq!(graph.incoming_by_type(2, "KNOWS").unwrap()[0].id, 10);
        assert!(graph.outgoing_by_type(1, "MISSING").unwrap().is_empty());
    }

    #[test]
    fn indexes_boundary_nodes_by_label_property() {
        let mut graph = GraphState::new();
        graph
            .apply(Command::UpsertBoundaryNode {
                id: 100,
                owner_shard: 7,
                labels: vec!["Person".to_string()],
                properties: properties(&[("status", Value::String("active".to_string()))]),
                version: 1,
            })
            .unwrap();

        assert_eq!(
            graph.boundary_node_ids_by_label_property(
                "Person",
                "status",
                &Value::String("active".to_string())
            ),
            vec![100]
        );

        graph
            .apply(Command::UpsertBoundaryNode {
                id: 100,
                owner_shard: 7,
                labels: vec!["Person".to_string()],
                properties: properties(&[("status", Value::String("inactive".to_string()))]),
                version: 2,
            })
            .unwrap();

        assert!(graph
            .boundary_node_ids_by_label_property(
                "Person",
                "status",
                &Value::String("active".to_string())
            )
            .is_empty());
    }
}
