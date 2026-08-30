use super::*;
use crate::{KvWrite, MemoryKvStore};
use std::collections::BTreeMap;

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
fn relationship_create_uses_one_atomic_write_batch() {
    let kv = CountingKvStore::with_nodes([1, 2]);
    let mut store = KvGraphStore::new(kv);

    store
        .apply(&Command::CreateRelationship {
            id: 10,
            from: 1,
            to: 2,
            rel_type: "KNOWS".to_string(),
            properties: Properties::new(),
        })
        .unwrap();
    let kv = store.into_inner();

    assert_eq!(kv.write_batch_calls, 1);
    assert_eq!(kv.last_batch_len, 5);
    assert!(kv.data.contains_key(&relationship_key(10)));
    assert!(kv.data.contains_key(&outgoing_key(1, 10)));
    assert!(kv.data.contains_key(&outgoing_type_key(1, "KNOWS", 10)));
    assert!(kv.data.contains_key(&incoming_key(2, 10)));
    assert!(kv.data.contains_key(&incoming_type_key(2, "KNOWS", 10)));
}

#[test]
fn apply_batch_uses_one_write_batch_and_sees_staged_nodes() {
    let kv = CountingKvStore::default();
    let mut store = KvGraphStore::new(kv);

    store
        .apply_batch(&[
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
                id: 10,
                from: 1,
                to: 2,
                rel_type: "KNOWS".to_string(),
                properties: Properties::new(),
            },
            Command::SetNodeProperty {
                id: 1,
                key: "name".to_string(),
                value: Value::String("Alicia".to_string()),
            },
        ])
        .unwrap();
    let kv = store.into_inner();

    assert_eq!(kv.write_batch_calls, 1);
    assert!(kv.data.contains_key(&relationship_key(10)));
    assert!(kv.data.contains_key(&outgoing_key(1, 10)));
    assert!(kv.data.contains_key(&incoming_key(2, 10)));
    assert!(kv.data.contains_key(&label_property_key(
        "Person",
        "name",
        &Value::String("Alicia".to_string()),
        1
    )));
    assert!(!kv.data.contains_key(&label_property_key(
        "Person",
        "name",
        &Value::String("Alice".to_string()),
        1
    )));
}

#[test]
fn failed_relationship_write_batch_leaves_no_partial_indexes() {
    let kv = CountingKvStore::with_nodes([1, 2]).with_fail_writes(true);
    let mut store = KvGraphStore::new(kv);

    let err = store
        .apply(&Command::CreateRelationship {
            id: 10,
            from: 1,
            to: 2,
            rel_type: "KNOWS".to_string(),
            properties: Properties::new(),
        })
        .unwrap_err();
    let kv = store.into_inner();

    assert!(err.to_string().contains("injected write batch failure"));
    assert_eq!(kv.write_batch_calls, 1);
    assert!(!kv.data.contains_key(&relationship_key(10)));
    assert!(!kv.data.contains_key(&outgoing_key(1, 10)));
    assert!(!kv.data.contains_key(&outgoing_type_key(1, "KNOWS", 10)));
    assert!(!kv.data.contains_key(&incoming_key(2, 10)));
    assert!(!kv.data.contains_key(&incoming_type_key(2, "KNOWS", 10)));
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

#[test]
fn verifies_and_repairs_materialized_indexes() {
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
            properties: Properties::new(),
        })
        .unwrap();
    store
        .apply(&Command::CreateRelationship {
            id: 10,
            from: 1,
            to: 2,
            rel_type: "KNOWS".to_string(),
            properties: Properties::new(),
        })
        .unwrap();

    store.kv.delete(&label_key("Person", 1)).unwrap();
    store.kv.put(b"out/garbage", EMPTY).unwrap();

    let broken = store.verify_invariants().unwrap();
    assert_eq!(broken.missing_index_keys.len(), 1);
    assert_eq!(broken.unexpected_index_keys.len(), 1);

    let repaired = store.repair_indexes().unwrap();
    assert!(!repaired.is_clean());
    assert!(store.verify_invariants().unwrap().is_clean());
    assert_eq!(store.node_ids_by_label("Person").unwrap(), vec![1, 2]);
    assert_eq!(store.outgoing_by_type(1, "KNOWS").unwrap()[0].id, 10);
}

#[test]
fn failed_batch_does_not_partially_apply_logical_command() {
    let mut store = KvGraphStore::new(FailingBatchStore::new());
    let err = store
        .apply(&Command::CreateRelationship {
            id: 10,
            from: 1,
            to: 2,
            rel_type: "KNOWS".to_string(),
            properties: Properties::new(),
        })
        .unwrap_err();

    assert!(err.to_string().contains("injected batch failure"));
    assert!(store.relationship(10).unwrap().is_none());
    assert!(store.outgoing(1).unwrap().is_empty());
    assert!(store.verify_invariants().unwrap().is_clean());
}

struct FailingBatchStore {
    data: BTreeMap<Vec<u8>, Vec<u8>>,
}

impl FailingBatchStore {
    fn new() -> Self {
        Self {
            data: BTreeMap::new(),
        }
    }
}

impl KeyValueStore for FailingBatchStore {
    fn get(&self, key: &[u8]) -> StorageResult<Option<Vec<u8>>> {
        Ok(self.data.get(key).cloned())
    }

    fn put(&mut self, key: &[u8], value: &[u8]) -> StorageResult<()> {
        self.data.insert(key.to_vec(), value.to_vec());
        Ok(())
    }

    fn delete(&mut self, key: &[u8]) -> StorageResult<()> {
        self.data.remove(key);
        Ok(())
    }

    fn write_batch(&mut self, _batch: KvWriteBatch) -> StorageResult<()> {
        Err(StorageError::CorruptStore(
            "injected batch failure".to_string(),
        ))
    }

    fn scan_prefix(&self, prefix: &[u8]) -> StorageResult<Vec<(Vec<u8>, Vec<u8>)>> {
        Ok(self
            .data
            .range(prefix.to_vec()..)
            .take_while(|(key, _)| key.starts_with(prefix))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect())
    }
}

fn properties(entries: &[(&str, Value)]) -> Properties {
    entries
        .iter()
        .map(|(key, value)| ((*key).to_string(), value.clone()))
        .collect()
}

#[derive(Clone, Debug, Default)]
struct CountingKvStore {
    data: BTreeMap<Vec<u8>, Vec<u8>>,
    write_batch_calls: usize,
    last_batch_len: usize,
    fail_writes: bool,
}

impl CountingKvStore {
    fn with_nodes(ids: impl IntoIterator<Item = NodeId>) -> Self {
        let mut store = Self::default();
        for id in ids {
            let command = Command::CreateNode {
                id,
                labels: vec!["Person".to_string()],
                properties: Properties::new(),
            };
            store.data.insert(node_key(id), encode_command(&command));
        }
        store
    }

    fn with_fail_writes(mut self, fail_writes: bool) -> Self {
        self.fail_writes = fail_writes;
        self
    }
}

impl KeyValueStore for CountingKvStore {
    fn get(&self, key: &[u8]) -> StorageResult<Option<Vec<u8>>> {
        Ok(self.data.get(key).cloned())
    }

    fn put(&mut self, key: &[u8], value: &[u8]) -> StorageResult<()> {
        self.data.insert(key.to_vec(), value.to_vec());
        Ok(())
    }

    fn delete(&mut self, key: &[u8]) -> StorageResult<()> {
        self.data.remove(key);
        Ok(())
    }

    fn write_batch(&mut self, batch: KvWriteBatch) -> StorageResult<()> {
        self.write_batch_calls += 1;
        self.last_batch_len = batch.operations().len();
        if self.fail_writes {
            return Err(StorageError::CorruptStore(
                "injected write batch failure".to_string(),
            ));
        }
        for operation in batch.operations() {
            match operation {
                KvWrite::Put { key, value } => {
                    self.data.insert(key.clone(), value.clone());
                }
                KvWrite::Delete { key } => {
                    self.data.remove(key);
                }
            }
        }
        Ok(())
    }

    fn scan_prefix(&self, prefix: &[u8]) -> StorageResult<Vec<(Vec<u8>, Vec<u8>)>> {
        Ok(self
            .data
            .range(prefix.to_vec()..)
            .take_while(|(key, _)| key.starts_with(prefix))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect())
    }
}
