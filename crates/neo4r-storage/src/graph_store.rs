use crate::codec::{decode_command, encode_command};
use crate::{KeyValueStore, StorageError, StorageResult};
use crate::{RocksKvSnapshot, RocksKvStore};
use neo4r_core::{
    BoundaryNode, Command, GraphRead, GraphReadError, GraphReadResult, Node, NodeId, Properties,
    Relationship, RelationshipId, Value,
};

const EMPTY: &[u8] = &[];

pub struct KvGraphStore<KV> {
    kv: KV,
}

impl<KV: KeyValueStore> KvGraphStore<KV> {
    pub fn new(kv: KV) -> Self {
        Self { kv }
    }

    pub fn into_inner(self) -> KV {
        self.kv
    }

    pub fn apply(&mut self, command: &Command) -> StorageResult<()> {
        match command {
            Command::CreateNode {
                id,
                labels,
                properties,
            } => self.create_node(*id, labels, properties),
            Command::CreateRelationship {
                id,
                from,
                to,
                rel_type,
                properties,
            } => self.create_relationship(*id, *from, *to, rel_type, properties),
            Command::UpsertBoundaryNode {
                id,
                owner_shard,
                labels,
                properties,
                version,
            } => self.upsert_boundary_node(*id, *owner_shard, labels, properties, *version),
            Command::SetNodeProperty { id, key, value } => self.set_node_property(*id, key, value),
            Command::RemoveNodeProperty { id, key } => self.remove_node_property(*id, key),
            Command::AddNodeLabel { id, label } => self.add_node_label(*id, label),
            Command::RemoveNodeLabel { id, label } => self.remove_node_label(*id, label),
            Command::SetRelationshipProperty { id, key, value } => {
                self.set_relationship_property(*id, key, value)
            }
            Command::RemoveRelationshipProperty { id, key } => {
                self.remove_relationship_property(*id, key)
            }
            Command::DeleteRelationship { id } => self.delete_relationship(*id),
            Command::DeleteNode { id } => self.delete_node(*id),
        }
    }

    pub fn node(&self, id: NodeId) -> StorageResult<Option<Node>> {
        self.kv.get(&node_key(id))?.map(decode_node).transpose()
    }

    pub fn nodes(&self) -> StorageResult<Vec<Node>> {
        self.kv
            .scan_prefix(&node_prefix())?
            .into_iter()
            .map(|(_, value)| decode_node(value))
            .collect()
    }

    pub fn node_ids(&self) -> StorageResult<Vec<NodeId>> {
        ids_from_keys(self.kv.scan_prefix(&node_prefix())?)
    }

    pub fn boundary_node(&self, id: NodeId) -> StorageResult<Option<BoundaryNode>> {
        self.kv
            .get(&boundary_node_key(id))?
            .map(decode_boundary_node)
            .transpose()
    }

    pub fn relationship(&self, id: RelationshipId) -> StorageResult<Option<Relationship>> {
        self.kv
            .get(&relationship_key(id))?
            .map(decode_relationship)
            .transpose()
    }

    pub fn relationships(&self) -> StorageResult<Vec<Relationship>> {
        self.kv
            .scan_prefix(&relationship_prefix())?
            .into_iter()
            .map(|(_, value)| decode_relationship(value))
            .collect()
    }

    pub fn node_ids_by_label(&self, label: &str) -> StorageResult<Vec<NodeId>> {
        ids_from_keys(self.kv.scan_prefix(&label_prefix(label))?)
    }

    pub fn node_ids_by_label_property(
        &self,
        label: &str,
        property_key: &str,
        property_value: &Value,
    ) -> StorageResult<Vec<NodeId>> {
        ids_from_keys(self.kv.scan_prefix(&label_property_prefix(
            label,
            property_key,
            property_value,
        ))?)
    }

    pub fn boundary_node_ids_by_label_property(
        &self,
        label: &str,
        property_key: &str,
        property_value: &Value,
    ) -> StorageResult<Vec<NodeId>> {
        ids_from_keys(self.kv.scan_prefix(&boundary_label_property_prefix(
            label,
            property_key,
            property_value,
        ))?)
    }

    pub fn outgoing_by_type(
        &self,
        node_id: NodeId,
        rel_type: &str,
    ) -> StorageResult<Vec<Relationship>> {
        self.relationships_from_index(&outgoing_type_prefix(node_id, rel_type))
    }

    pub fn outgoing(&self, node_id: NodeId) -> StorageResult<Vec<Relationship>> {
        self.relationships_from_index(&outgoing_prefix(node_id))
    }

    pub fn incoming_by_type(
        &self,
        node_id: NodeId,
        rel_type: &str,
    ) -> StorageResult<Vec<Relationship>> {
        self.relationships_from_index(&incoming_type_prefix(node_id, rel_type))
    }

    pub fn incoming(&self, node_id: NodeId) -> StorageResult<Vec<Relationship>> {
        self.relationships_from_index(&incoming_prefix(node_id))
    }

    fn create_node(
        &mut self,
        id: NodeId,
        labels: &[String],
        properties: &Properties,
    ) -> StorageResult<()> {
        let command = Command::CreateNode {
            id,
            labels: labels.to_vec(),
            properties: properties.clone(),
        };
        self.kv.put(&node_key(id), &encode_command(&command))?;
        self.index_node(id, labels, properties)
    }

    fn upsert_boundary_node(
        &mut self,
        id: NodeId,
        owner_shard: u64,
        labels: &[String],
        properties: &Properties,
        version: u64,
    ) -> StorageResult<()> {
        if let Some(old) = self.boundary_node(id)? {
            self.remove_boundary_node_indexes(&old)?;
        }
        let command = Command::UpsertBoundaryNode {
            id,
            owner_shard,
            labels: labels.to_vec(),
            properties: properties.clone(),
            version,
        };
        self.kv
            .put(&boundary_node_key(id), &encode_command(&command))?;
        self.index_boundary_node(id, labels, properties)
    }

    fn create_relationship(
        &mut self,
        id: RelationshipId,
        from: NodeId,
        to: NodeId,
        rel_type: &str,
        properties: &Properties,
    ) -> StorageResult<()> {
        let command = Command::CreateRelationship {
            id,
            from,
            to,
            rel_type: rel_type.to_string(),
            properties: properties.clone(),
        };
        self.kv
            .put(&relationship_key(id), &encode_command(&command))?;
        self.kv.put(&outgoing_key(from, id), EMPTY)?;
        self.kv.put(&outgoing_type_key(from, rel_type, id), EMPTY)?;
        if self.node(to)?.is_some() {
            self.kv.put(&incoming_key(to, id), EMPTY)?;
            self.kv.put(&incoming_type_key(to, rel_type, id), EMPTY)?;
        }
        Ok(())
    }

    fn set_node_property(&mut self, id: NodeId, key: &str, value: &Value) -> StorageResult<()> {
        let Some(mut node) = self.node(id)? else {
            return Ok(());
        };
        self.remove_node_indexes(&node)?;
        node.properties.insert(key.to_string(), value.clone());
        self.create_node(node.id, &node.labels, &node.properties)
    }

    fn remove_node_property(&mut self, id: NodeId, key: &str) -> StorageResult<()> {
        let Some(mut node) = self.node(id)? else {
            return Ok(());
        };
        self.remove_node_indexes(&node)?;
        node.properties.remove(key);
        self.create_node(node.id, &node.labels, &node.properties)
    }

    fn add_node_label(&mut self, id: NodeId, label: &str) -> StorageResult<()> {
        let Some(mut node) = self.node(id)? else {
            return Ok(());
        };
        if node.labels.iter().any(|existing| existing == label) {
            return Ok(());
        }
        self.remove_node_indexes(&node)?;
        node.labels.push(label.to_string());
        self.create_node(node.id, &node.labels, &node.properties)
    }

    fn remove_node_label(&mut self, id: NodeId, label: &str) -> StorageResult<()> {
        let Some(mut node) = self.node(id)? else {
            return Ok(());
        };
        if !node.labels.iter().any(|existing| existing == label) {
            return Ok(());
        }
        self.remove_node_indexes(&node)?;
        node.labels.retain(|existing| existing != label);
        self.create_node(node.id, &node.labels, &node.properties)
    }

    fn set_relationship_property(
        &mut self,
        id: RelationshipId,
        key: &str,
        value: &Value,
    ) -> StorageResult<()> {
        let Some(mut relationship) = self.relationship(id)? else {
            return Ok(());
        };
        relationship
            .properties
            .insert(key.to_string(), value.clone());
        let command = Command::CreateRelationship {
            id: relationship.id,
            from: relationship.from,
            to: relationship.to,
            rel_type: relationship.rel_type,
            properties: relationship.properties,
        };
        self.kv
            .put(&relationship_key(id), &encode_command(&command))
    }

    fn remove_relationship_property(&mut self, id: RelationshipId, key: &str) -> StorageResult<()> {
        let Some(mut relationship) = self.relationship(id)? else {
            return Ok(());
        };
        relationship.properties.remove(key);
        let command = Command::CreateRelationship {
            id: relationship.id,
            from: relationship.from,
            to: relationship.to,
            rel_type: relationship.rel_type,
            properties: relationship.properties,
        };
        self.kv
            .put(&relationship_key(id), &encode_command(&command))
    }

    fn delete_relationship(&mut self, id: RelationshipId) -> StorageResult<()> {
        let Some(relationship) = self.relationship(id)? else {
            return Ok(());
        };
        self.kv.delete(&relationship_key(id))?;
        self.kv.delete(&outgoing_key(relationship.from, id))?;
        self.kv.delete(&outgoing_type_key(
            relationship.from,
            &relationship.rel_type,
            id,
        ))?;
        self.kv.delete(&incoming_key(relationship.to, id))?;
        self.kv.delete(&incoming_type_key(
            relationship.to,
            &relationship.rel_type,
            id,
        ))?;
        Ok(())
    }

    fn delete_node(&mut self, id: NodeId) -> StorageResult<()> {
        let Some(node) = self.node(id)? else {
            return Ok(());
        };

        let mut relationship_ids = ids_from_keys(self.kv.scan_prefix(&outgoing_prefix(id))?)?;
        relationship_ids.extend(ids_from_keys(self.kv.scan_prefix(&incoming_prefix(id))?)?);
        relationship_ids.sort_unstable();
        relationship_ids.dedup();
        for relationship_id in relationship_ids {
            self.delete_relationship(relationship_id)?;
        }

        self.remove_node_indexes(&node)?;
        self.kv.delete(&node_key(id))
    }

    fn index_node(
        &mut self,
        id: NodeId,
        labels: &[String],
        properties: &Properties,
    ) -> StorageResult<()> {
        for label in labels {
            self.kv.put(&label_key(label, id), EMPTY)?;
            for (property_key, property_value) in properties {
                self.kv.put(
                    &label_property_key(label, property_key, property_value, id),
                    EMPTY,
                )?;
            }
        }
        Ok(())
    }

    fn remove_node_indexes(&mut self, node: &Node) -> StorageResult<()> {
        for label in &node.labels {
            self.kv.delete(&label_key(label, node.id))?;
            for (property_key, property_value) in &node.properties {
                self.kv.delete(&label_property_key(
                    label,
                    property_key,
                    property_value,
                    node.id,
                ))?;
            }
        }
        Ok(())
    }

    fn index_boundary_node(
        &mut self,
        id: NodeId,
        labels: &[String],
        properties: &Properties,
    ) -> StorageResult<()> {
        for label in labels {
            for (property_key, property_value) in properties {
                self.kv.put(
                    &boundary_label_property_key(label, property_key, property_value, id),
                    EMPTY,
                )?;
            }
        }
        Ok(())
    }

    fn remove_boundary_node_indexes(&mut self, node: &BoundaryNode) -> StorageResult<()> {
        for label in &node.labels {
            for (property_key, property_value) in &node.properties {
                self.kv.delete(&boundary_label_property_key(
                    label,
                    property_key,
                    property_value,
                    node.id,
                ))?;
            }
        }
        Ok(())
    }

    fn relationships_from_index(&self, prefix: &[u8]) -> StorageResult<Vec<Relationship>> {
        self.kv
            .scan_prefix(prefix)?
            .into_iter()
            .map(|(key, _)| {
                let id = id_from_key(&key)?;
                self.relationship(id)?
                    .ok_or_else(|| StorageError::CorruptStore(format!("missing relationship {id}")))
            })
            .collect()
    }
}

impl KvGraphStore<RocksKvStore> {
    pub fn snapshot(&self) -> StorageResult<RocksKvSnapshot> {
        self.kv.snapshot()
    }
}

impl<KV: KeyValueStore> GraphRead for KvGraphStore<KV> {
    fn node(&self, id: NodeId) -> GraphReadResult<Option<Node>> {
        KvGraphStore::node(self, id).map_err(graph_read_error)
    }

    fn boundary_node(&self, id: NodeId) -> GraphReadResult<Option<BoundaryNode>> {
        KvGraphStore::boundary_node(self, id).map_err(graph_read_error)
    }

    fn nodes(&self) -> GraphReadResult<Vec<Node>> {
        KvGraphStore::nodes(self).map_err(graph_read_error)
    }

    fn node_ids(&self) -> GraphReadResult<Vec<NodeId>> {
        KvGraphStore::node_ids(self).map_err(graph_read_error)
    }

    fn relationship(&self, id: RelationshipId) -> GraphReadResult<Option<Relationship>> {
        KvGraphStore::relationship(self, id).map_err(graph_read_error)
    }

    fn node_ids_by_label(&self, label: &str) -> GraphReadResult<Vec<NodeId>> {
        KvGraphStore::node_ids_by_label(self, label).map_err(graph_read_error)
    }

    fn node_ids_by_label_property(
        &self,
        label: &str,
        property_key: &str,
        property_value: &Value,
    ) -> GraphReadResult<Vec<NodeId>> {
        KvGraphStore::node_ids_by_label_property(self, label, property_key, property_value)
            .map_err(graph_read_error)
    }

    fn boundary_node_ids_by_label_property(
        &self,
        label: &str,
        property_key: &str,
        property_value: &Value,
    ) -> GraphReadResult<Vec<NodeId>> {
        KvGraphStore::boundary_node_ids_by_label_property(self, label, property_key, property_value)
            .map_err(graph_read_error)
    }

    fn outgoing(&self, node_id: NodeId) -> GraphReadResult<Vec<Relationship>> {
        KvGraphStore::outgoing(self, node_id).map_err(graph_read_error)
    }

    fn outgoing_by_type(
        &self,
        node_id: NodeId,
        rel_type: &str,
    ) -> GraphReadResult<Vec<Relationship>> {
        KvGraphStore::outgoing_by_type(self, node_id, rel_type).map_err(graph_read_error)
    }

    fn incoming(&self, node_id: NodeId) -> GraphReadResult<Vec<Relationship>> {
        KvGraphStore::incoming(self, node_id).map_err(graph_read_error)
    }

    fn incoming_by_type(
        &self,
        node_id: NodeId,
        rel_type: &str,
    ) -> GraphReadResult<Vec<Relationship>> {
        KvGraphStore::incoming_by_type(self, node_id, rel_type).map_err(graph_read_error)
    }
}

fn graph_read_error(err: StorageError) -> GraphReadError {
    GraphReadError::Store(err.to_string())
}

fn decode_node(payload: Vec<u8>) -> StorageResult<Node> {
    match decode_command(&payload)? {
        Command::CreateNode {
            id,
            labels,
            properties,
        } => Ok(Node::new(id, labels, properties)),
        _ => Err(StorageError::CorruptStore(
            "expected node record".to_string(),
        )),
    }
}

fn decode_boundary_node(payload: Vec<u8>) -> StorageResult<BoundaryNode> {
    match decode_command(&payload)? {
        Command::UpsertBoundaryNode {
            id,
            owner_shard,
            labels,
            properties,
            version,
        } => Ok(BoundaryNode::new(
            id,
            owner_shard,
            labels,
            properties,
            version,
        )),
        _ => Err(StorageError::CorruptStore(
            "expected boundary node record".to_string(),
        )),
    }
}

fn decode_relationship(payload: Vec<u8>) -> StorageResult<Relationship> {
    match decode_command(&payload)? {
        Command::CreateRelationship {
            id,
            from,
            to,
            rel_type,
            properties,
        } => Ok(Relationship::new(id, from, to, rel_type, properties)),
        _ => Err(StorageError::CorruptStore(
            "expected relationship record".to_string(),
        )),
    }
}

fn ids_from_keys(entries: Vec<(Vec<u8>, Vec<u8>)>) -> StorageResult<Vec<u64>> {
    let mut ids = entries
        .into_iter()
        .map(|(key, _)| id_from_key(&key))
        .collect::<StorageResult<Vec<_>>>()?;
    ids.sort_unstable();
    Ok(ids)
}

fn id_from_key(key: &[u8]) -> StorageResult<u64> {
    let Some(bytes) = key.get(key.len().saturating_sub(8)..) else {
        return Err(StorageError::CorruptStore("key is too short".to_string()));
    };
    Ok(u64::from_be_bytes(bytes.try_into().unwrap()))
}

fn node_key(id: NodeId) -> Vec<u8> {
    key_with_id(b"n", id)
}

fn node_prefix() -> Vec<u8> {
    Vec::from(&b"n/"[..])
}

fn boundary_node_key(id: NodeId) -> Vec<u8> {
    key_with_id(b"bn", id)
}

fn relationship_key(id: RelationshipId) -> Vec<u8> {
    key_with_id(b"r", id)
}

fn relationship_prefix() -> Vec<u8> {
    Vec::from(&b"r/"[..])
}

fn outgoing_prefix(node_id: NodeId) -> Vec<u8> {
    key_prefix_with_id(b"out", node_id)
}

fn outgoing_key(node_id: NodeId, relationship_id: RelationshipId) -> Vec<u8> {
    let mut key = outgoing_prefix(node_id);
    key.extend_from_slice(&relationship_id.to_be_bytes());
    key
}

fn incoming_prefix(node_id: NodeId) -> Vec<u8> {
    key_prefix_with_id(b"in", node_id)
}

fn incoming_key(node_id: NodeId, relationship_id: RelationshipId) -> Vec<u8> {
    let mut key = incoming_prefix(node_id);
    key.extend_from_slice(&relationship_id.to_be_bytes());
    key
}

fn outgoing_type_prefix(node_id: NodeId, rel_type: &str) -> Vec<u8> {
    let mut key = key_prefix_with_id(b"outt", node_id);
    push_component(&mut key, rel_type.as_bytes());
    key
}

fn outgoing_type_key(node_id: NodeId, rel_type: &str, relationship_id: RelationshipId) -> Vec<u8> {
    let mut key = outgoing_type_prefix(node_id, rel_type);
    key.extend_from_slice(&relationship_id.to_be_bytes());
    key
}

fn incoming_type_prefix(node_id: NodeId, rel_type: &str) -> Vec<u8> {
    let mut key = key_prefix_with_id(b"int", node_id);
    push_component(&mut key, rel_type.as_bytes());
    key
}

fn incoming_type_key(node_id: NodeId, rel_type: &str, relationship_id: RelationshipId) -> Vec<u8> {
    let mut key = incoming_type_prefix(node_id, rel_type);
    key.extend_from_slice(&relationship_id.to_be_bytes());
    key
}

fn label_prefix(label: &str) -> Vec<u8> {
    let mut key = Vec::from(&b"l/"[..]);
    push_component(&mut key, label.as_bytes());
    key
}

fn label_key(label: &str, node_id: NodeId) -> Vec<u8> {
    let mut key = label_prefix(label);
    key.extend_from_slice(&node_id.to_be_bytes());
    key
}

fn label_property_prefix(label: &str, property_key: &str, property_value: &Value) -> Vec<u8> {
    let mut key = Vec::from(&b"lp/"[..]);
    push_component(&mut key, label.as_bytes());
    push_component(&mut key, property_key.as_bytes());
    push_component(&mut key, &encode_value_for_key(property_value));
    key
}

fn label_property_key(
    label: &str,
    property_key: &str,
    property_value: &Value,
    node_id: NodeId,
) -> Vec<u8> {
    let mut key = label_property_prefix(label, property_key, property_value);
    key.extend_from_slice(&node_id.to_be_bytes());
    key
}

fn boundary_label_property_prefix(
    label: &str,
    property_key: &str,
    property_value: &Value,
) -> Vec<u8> {
    let mut key = Vec::from(&b"blp/"[..]);
    push_component(&mut key, label.as_bytes());
    push_component(&mut key, property_key.as_bytes());
    push_component(&mut key, &encode_value_for_key(property_value));
    key
}

fn boundary_label_property_key(
    label: &str,
    property_key: &str,
    property_value: &Value,
    node_id: NodeId,
) -> Vec<u8> {
    let mut key = boundary_label_property_prefix(label, property_key, property_value);
    key.extend_from_slice(&node_id.to_be_bytes());
    key
}

fn key_with_id(prefix: &[u8], id: u64) -> Vec<u8> {
    let mut key = Vec::from(prefix);
    key.push(b'/');
    key.extend_from_slice(&id.to_be_bytes());
    key
}

fn key_prefix_with_id(prefix: &[u8], id: u64) -> Vec<u8> {
    let mut key = key_with_id(prefix, id);
    key.push(b'/');
    key
}

fn push_component(key: &mut Vec<u8>, value: &[u8]) {
    key.extend_from_slice(&(value.len() as u32).to_be_bytes());
    key.extend_from_slice(value);
    key.push(b'/');
}

fn encode_value_for_key(value: &Value) -> Vec<u8> {
    let mut out = Vec::new();
    match value {
        Value::Null => out.push(0),
        Value::Bool(value) => {
            out.push(1);
            out.push(u8::from(*value));
        }
        Value::Int(value) => {
            out.push(2);
            out.extend_from_slice(&value.to_be_bytes());
        }
        Value::Float(value) => {
            out.push(3);
            out.extend_from_slice(&value.to_bits().to_be_bytes());
        }
        Value::String(value) => {
            out.push(4);
            out.extend_from_slice(value.as_bytes());
        }
        Value::Vector(value) => {
            out.push(5);
            out.extend_from_slice(&(value.len() as u32).to_be_bytes());
            for item in value {
                out.extend_from_slice(&item.to_bits().to_be_bytes());
            }
        }
        Value::Map(value) => {
            out.push(6);
            let mut entries = value.iter().collect::<Vec<_>>();
            entries.sort_by(|(left, _), (right, _)| left.cmp(right));
            out.extend_from_slice(&(entries.len() as u32).to_be_bytes());
            for (key, value) in entries {
                out.extend_from_slice(&(key.len() as u32).to_be_bytes());
                out.extend_from_slice(key.as_bytes());
                let value = encode_value_for_key(value);
                out.extend_from_slice(&(value.len() as u32).to_be_bytes());
                out.extend_from_slice(&value);
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MemoryKvStore;

    #[test]
    fn stores_and_finds_nodes_by_label_property_index() {
        let mut store = KvGraphStore::new(MemoryKvStore::new());
        store
            .apply(&Command::CreateNode {
                id: 1,
                labels: vec!["Person".to_string()],
                properties: properties(&[("name", Value::String("Alice".to_string()))]),
            })
            .unwrap();
        store
            .apply(&Command::CreateNode {
                id: 2,
                labels: vec!["Person".to_string()],
                properties: properties(&[("name", Value::String("Bob".to_string()))]),
            })
            .unwrap();

        assert_eq!(store.node_ids_by_label("Person").unwrap(), vec![1, 2]);
        assert_eq!(
            store
                .node_ids_by_label_property("Person", "name", &Value::String("Alice".to_string()))
                .unwrap(),
            vec![1]
        );
    }

    #[test]
    fn updates_property_index_when_node_property_changes() {
        let mut store = KvGraphStore::new(MemoryKvStore::new());
        store
            .apply(&Command::CreateNode {
                id: 1,
                labels: vec!["Person".to_string()],
                properties: properties(&[("name", Value::String("Alice".to_string()))]),
            })
            .unwrap();
        store
            .apply(&Command::SetNodeProperty {
                id: 1,
                key: "name".to_string(),
                value: Value::String("Alicia".to_string()),
            })
            .unwrap();

        assert!(store
            .node_ids_by_label_property("Person", "name", &Value::String("Alice".to_string()))
            .unwrap()
            .is_empty());
        assert_eq!(
            store
                .node_ids_by_label_property("Person", "name", &Value::String("Alicia".to_string()))
                .unwrap(),
            vec![1]
        );
    }

    #[test]
    fn removes_node_property_and_property_index() {
        let mut store = KvGraphStore::new(MemoryKvStore::new());
        store
            .apply(&Command::CreateNode {
                id: 1,
                labels: vec!["Person".to_string()],
                properties: properties(&[("name", Value::String("Alice".to_string()))]),
            })
            .unwrap();

        store
            .apply(&Command::RemoveNodeProperty {
                id: 1,
                key: "name".to_string(),
            })
            .unwrap();

        assert_eq!(store.node_ids_by_label("Person").unwrap(), vec![1]);
        assert!(store
            .node_ids_by_label_property("Person", "name", &Value::String("Alice".to_string()))
            .unwrap()
            .is_empty());
        assert!(!store
            .node(1)
            .unwrap()
            .unwrap()
            .properties
            .contains_key("name"));
    }

    #[test]
    fn updates_indexes_when_node_labels_change() {
        let mut store = KvGraphStore::new(MemoryKvStore::new());
        store
            .apply(&Command::CreateNode {
                id: 1,
                labels: vec!["Person".to_string()],
                properties: properties(&[("name", Value::String("Alice".to_string()))]),
            })
            .unwrap();

        store
            .apply(&Command::AddNodeLabel {
                id: 1,
                label: "Employee".to_string(),
            })
            .unwrap();

        assert_eq!(store.node_ids_by_label("Employee").unwrap(), vec![1]);
        assert_eq!(
            store
                .node_ids_by_label_property("Employee", "name", &Value::String("Alice".to_string()))
                .unwrap(),
            vec![1]
        );

        store
            .apply(&Command::RemoveNodeLabel {
                id: 1,
                label: "Person".to_string(),
            })
            .unwrap();

        assert!(store.node_ids_by_label("Person").unwrap().is_empty());
        assert!(store
            .node_ids_by_label_property("Person", "name", &Value::String("Alice".to_string()))
            .unwrap()
            .is_empty());
        assert_eq!(store.node_ids_by_label("Employee").unwrap(), vec![1]);
    }

    #[test]
    fn stores_relationship_type_adjacency_index() {
        let mut store = KvGraphStore::new(MemoryKvStore::new());
        for id in [1, 2, 3] {
            store
                .apply(&Command::CreateNode {
                    id,
                    labels: vec![],
                    properties: Properties::new(),
                })
                .unwrap();
        }
        store
            .apply(&Command::CreateRelationship {
                id: 10,
                from: 1,
                to: 2,
                rel_type: "KNOWS".to_string(),
                properties: Properties::new(),
            })
            .unwrap();
        store
            .apply(&Command::CreateRelationship {
                id: 11,
                from: 1,
                to: 3,
                rel_type: "LIKES".to_string(),
                properties: Properties::new(),
            })
            .unwrap();

        assert_eq!(store.outgoing_by_type(1, "KNOWS").unwrap()[0].id, 10);
        assert_eq!(store.incoming_by_type(2, "KNOWS").unwrap()[0].id, 10);
    }

    #[test]
    fn stores_boundary_node_property_index() {
        let mut store = KvGraphStore::new(MemoryKvStore::new());
        store
            .apply(&Command::UpsertBoundaryNode {
                id: 20,
                owner_shard: 2,
                labels: vec!["Person".to_string()],
                properties: properties(&[("status", Value::String("active".to_string()))]),
                version: 1,
            })
            .unwrap();

        assert_eq!(
            store
                .boundary_node_ids_by_label_property(
                    "Person",
                    "status",
                    &Value::String("active".to_string())
                )
                .unwrap(),
            vec![20]
        );
    }

    fn properties(entries: &[(&str, Value)]) -> Properties {
        entries
            .iter()
            .map(|(key, value)| ((*key).to_string(), value.clone()))
            .collect()
    }
}
