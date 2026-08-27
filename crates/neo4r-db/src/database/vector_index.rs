use crate::{DatabaseError, DatabaseResult};
use neo4r_core::{Node, NodeId, Value};
use neo4r_query::{
    HnswVectorIndex, HnswVectorIndexConfig, VectorHit, VectorIndex, VectorIndexProvider,
    VectorMetric, VectorSearch,
};
use neo4r_storage::{IndexCatalog, IndexDefinition, IndexKind, StorageError};
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{ErrorKind, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

pub(super) const VECTOR_INDEX_CACHE_MAGIC: &[u8] = b"N4RVIC1\n";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VectorIndexStatus {
    pub name: String,
    pub label: String,
    pub property: String,
    pub dimensions: usize,
    pub metric: String,
    pub entries: usize,
}

#[derive(Clone)]
pub(super) struct SharedVectorIndexProvider {
    indexes: Arc<Mutex<PersistentVectorIndexes>>,
}

impl SharedVectorIndexProvider {
    pub(super) fn new(indexes: Arc<Mutex<PersistentVectorIndexes>>) -> Self {
        Self { indexes }
    }
}

impl VectorIndexProvider for SharedVectorIndexProvider {
    fn search(
        &self,
        label: Option<&str>,
        property_key: &str,
        search: &VectorSearch,
    ) -> Option<Vec<VectorHit>> {
        self.indexes
            .lock()
            .ok()
            .and_then(|indexes| indexes.search(label, property_key, search))
    }
}

#[derive(Default)]
pub(super) struct PersistentVectorIndexes {
    pub(super) indexes: HashMap<String, CachedVectorIndex>,
}

pub(super) struct CachedVectorIndex {
    label: String,
    property: String,
    dimensions: usize,
    metric: VectorMetric,
    index: HnswVectorIndex,
}

impl PersistentVectorIndexes {
    pub(super) fn is_empty(&self) -> bool {
        self.indexes.is_empty()
    }

    fn search(
        &self,
        label: Option<&str>,
        property_key: &str,
        search: &VectorSearch,
    ) -> Option<Vec<VectorHit>> {
        let label = label?;
        self.indexes.values().find_map(|index| {
            if index.label == label
                && index.property == property_key
                && index.metric == search.metric
            {
                Some(index.index.search(search))
            } else {
                None
            }
        })
    }

    pub(super) fn insert_definition(&mut self, definition: &IndexDefinition, nodes: &[Node]) {
        let Some((dimensions, metric)) = vector_definition_parts(definition) else {
            return;
        };
        let mut index = HnswVectorIndex::new(HnswVectorIndexConfig {
            metric,
            expected_elements: nodes.len().max(1),
            ..HnswVectorIndexConfig::default()
        });
        for node in nodes {
            upsert_node_into_vector_index(&mut index, node, definition, dimensions);
        }
        self.indexes.insert(
            definition.name.clone(),
            CachedVectorIndex {
                label: definition.label.clone(),
                property: definition.property.clone(),
                dimensions,
                metric,
                index,
            },
        );
    }

    fn insert_cached(
        &mut self,
        definition: &IndexDefinition,
        entries: Vec<(NodeId, Vec<f32>)>,
    ) -> DatabaseResult<()> {
        let Some((dimensions, metric)) = vector_definition_parts(definition) else {
            return Ok(());
        };
        let expected_elements = entries.len().max(1);
        let index = HnswVectorIndex::from_entries(
            entries,
            HnswVectorIndexConfig {
                metric,
                expected_elements,
                ..HnswVectorIndexConfig::default()
            },
        );
        self.indexes.insert(
            definition.name.clone(),
            CachedVectorIndex {
                label: definition.label.clone(),
                property: definition.property.clone(),
                dimensions,
                metric,
                index,
            },
        );
        Ok(())
    }

    pub(super) fn snapshots(&self) -> Vec<VectorIndexCacheSnapshot> {
        let mut snapshots = self
            .indexes
            .iter()
            .map(|(name, index)| VectorIndexCacheSnapshot {
                name: name.clone(),
                entries: index
                    .index
                    .entries()
                    .map(|(node_id, vector)| (node_id, vector.clone()))
                    .collect(),
            })
            .collect::<Vec<_>>();
        snapshots.sort_by(|left, right| left.name.cmp(&right.name));
        snapshots
    }

    pub(super) fn status(&self) -> Vec<VectorIndexStatus> {
        let mut statuses = self
            .indexes
            .iter()
            .map(|(name, index)| VectorIndexStatus {
                name: name.clone(),
                label: index.label.clone(),
                property: index.property.clone(),
                dimensions: index.dimensions,
                metric: vector_metric_name(index.metric).to_string(),
                entries: index.index.entries().count(),
            })
            .collect::<Vec<_>>();
        statuses.sort_by(|left, right| left.name.cmp(&right.name));
        statuses
    }

    pub(super) fn remove(&mut self, name: &str) {
        self.indexes.remove(name);
    }

    pub(super) fn update_node(&mut self, node: &Node) {
        for index in self.indexes.values_mut() {
            if node.labels.iter().any(|label| label == &index.label) {
                match node.properties.get(&index.property) {
                    Some(Value::Vector(vector)) if vector.len() == index.dimensions => {
                        index.index.upsert(node.id, vector.clone());
                    }
                    _ => index.index.delete(node.id),
                }
            } else {
                index.index.delete(node.id);
            }
        }
    }

    pub(super) fn delete_node(&mut self, node_id: NodeId) {
        for index in self.indexes.values_mut() {
            index.index.delete(node_id);
        }
    }
}

pub(super) struct VectorIndexCacheSnapshot {
    pub(super) name: String,
    pub(super) entries: Vec<(NodeId, Vec<f32>)>,
}

pub(super) fn vector_definition_parts(index: &IndexDefinition) -> Option<(usize, VectorMetric)> {
    let IndexKind::Vector { dimensions, metric } = &index.kind else {
        return None;
    };
    let metric = parse_vector_metric(metric)?;
    Some((*dimensions, metric))
}

pub(super) fn parse_vector_metric(metric: &str) -> Option<VectorMetric> {
    if metric.eq_ignore_ascii_case("cosine") {
        Some(VectorMetric::Cosine)
    } else if metric.eq_ignore_ascii_case("l2") {
        Some(VectorMetric::L2)
    } else {
        None
    }
}

pub(super) fn vector_metric_name(metric: VectorMetric) -> &'static str {
    match metric {
        VectorMetric::Cosine => "cosine",
        VectorMetric::L2 => "l2",
    }
}

pub(super) fn save_vector_index_cache(
    path: PathBuf,
    catalog: &IndexCatalog,
    indexes: &PersistentVectorIndexes,
) -> DatabaseResult<()> {
    let Some(parent) = path.parent() else {
        return Err(DatabaseError::InvalidConfig(
            "vector index cache path has no parent".to_string(),
        ));
    };
    fs::create_dir_all(parent).map_err(storage_io_error)?;
    let tmp_path = path.with_extension("bin.tmp");
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&tmp_path)
        .map_err(storage_io_error)?;

    file.write_all(VECTOR_INDEX_CACHE_MAGIC)
        .map_err(storage_io_error)?;
    write_cache_u64(&mut file, catalog.version)?;
    let definitions = vector_index_definitions(catalog);
    write_cache_u64(&mut file, definitions.len() as u64)?;
    let snapshots = indexes
        .snapshots()
        .into_iter()
        .map(|snapshot| (snapshot.name.clone(), snapshot))
        .collect::<HashMap<_, _>>();
    for definition in definitions {
        let Some((dimensions, metric)) = vector_definition_parts(definition) else {
            continue;
        };
        write_cache_string(&mut file, &definition.name)?;
        write_cache_string(&mut file, &definition.label)?;
        write_cache_string(&mut file, &definition.property)?;
        write_cache_u64(&mut file, dimensions as u64)?;
        write_cache_string(&mut file, vector_metric_name(metric))?;
        let entries = snapshots
            .get(&definition.name)
            .map(|snapshot| snapshot.entries.as_slice())
            .unwrap_or(&[]);
        write_cache_u64(&mut file, entries.len() as u64)?;
        for (node_id, vector) in entries {
            write_cache_u64(&mut file, *node_id)?;
            write_cache_u64(&mut file, vector.len() as u64)?;
            for value in vector {
                file.write_all(&value.to_be_bytes())
                    .map_err(storage_io_error)?;
            }
        }
    }
    file.sync_all().map_err(storage_io_error)?;
    drop(file);
    fs::rename(&tmp_path, &path).map_err(storage_io_error)?;
    sync_parent_dir(parent)?;
    Ok(())
}

pub(super) fn load_vector_index_cache(
    path: PathBuf,
    catalog: &IndexCatalog,
) -> DatabaseResult<Option<PersistentVectorIndexes>> {
    let mut file = match File::open(&path) {
        Ok(file) => file,
        Err(err) if err.kind() == ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(storage_io_error(err)),
    };
    let mut magic = [0; VECTOR_INDEX_CACHE_MAGIC.len()];
    if file.read_exact(&mut magic).is_err() || magic != VECTOR_INDEX_CACHE_MAGIC {
        return Ok(None);
    }
    let version = match read_cache_u64(&mut file) {
        Ok(version) => version,
        Err(_) => return Ok(None),
    };
    if version != catalog.version {
        return Ok(None);
    }
    let definitions = vector_index_definitions(catalog);
    let index_count = match read_cache_u64(&mut file) {
        Ok(count) => count as usize,
        Err(_) => return Ok(None),
    };
    if index_count != definitions.len() {
        return Ok(None);
    }
    let mut indexes = PersistentVectorIndexes::default();
    for expected in definitions {
        let Some((expected_dimensions, expected_metric)) = vector_definition_parts(expected) else {
            return Ok(None);
        };
        let name = read_cache_string(&mut file).ok();
        let label = read_cache_string(&mut file).ok();
        let property = read_cache_string(&mut file).ok();
        let dimensions = read_cache_u64(&mut file).ok().map(|value| value as usize);
        let metric = read_cache_string(&mut file)
            .ok()
            .and_then(|metric| parse_vector_metric(&metric));
        if name.as_deref() != Some(&expected.name)
            || label.as_deref() != Some(&expected.label)
            || property.as_deref() != Some(&expected.property)
            || dimensions != Some(expected_dimensions)
            || metric != Some(expected_metric)
        {
            return Ok(None);
        }
        let entry_count = match read_cache_u64(&mut file) {
            Ok(count) => count as usize,
            Err(_) => return Ok(None),
        };
        let mut entries = Vec::with_capacity(entry_count);
        for _ in 0..entry_count {
            let node_id = match read_cache_u64(&mut file) {
                Ok(node_id) => node_id,
                Err(_) => return Ok(None),
            };
            let len = match read_cache_u64(&mut file) {
                Ok(len) => len as usize,
                Err(_) => return Ok(None),
            };
            if len != expected_dimensions {
                return Ok(None);
            }
            let mut vector = Vec::with_capacity(len);
            for _ in 0..len {
                let mut bytes = [0; 4];
                if file.read_exact(&mut bytes).is_err() {
                    return Ok(None);
                }
                vector.push(f32::from_be_bytes(bytes));
            }
            entries.push((node_id, vector));
        }
        indexes.insert_cached(expected, entries)?;
    }
    let mut trailing = [0; 1];
    if file.read(&mut trailing).map_err(storage_io_error)? != 0 {
        return Ok(None);
    }
    Ok(Some(indexes))
}

fn vector_index_definitions(catalog: &IndexCatalog) -> Vec<&IndexDefinition> {
    let mut definitions = catalog
        .indexes
        .iter()
        .filter(|index| matches!(index.kind, IndexKind::Vector { .. }))
        .collect::<Vec<_>>();
    definitions.sort_by(|left, right| left.name.cmp(&right.name));
    definitions
}

fn write_cache_u64(file: &mut File, value: u64) -> DatabaseResult<()> {
    file.write_all(&value.to_be_bytes())
        .map_err(storage_io_error)
}

fn read_cache_u64(file: &mut File) -> DatabaseResult<u64> {
    let mut bytes = [0; 8];
    file.read_exact(&mut bytes).map_err(storage_io_error)?;
    Ok(u64::from_be_bytes(bytes))
}

fn write_cache_string(file: &mut File, value: &str) -> DatabaseResult<()> {
    write_cache_u64(file, value.len() as u64)?;
    file.write_all(value.as_bytes()).map_err(storage_io_error)
}

fn read_cache_string(file: &mut File) -> DatabaseResult<String> {
    let len = read_cache_u64(file)? as usize;
    let mut bytes = vec![0; len];
    file.read_exact(&mut bytes).map_err(storage_io_error)?;
    String::from_utf8(bytes).map_err(|_| {
        DatabaseError::Storage(StorageError::CorruptStore(
            "invalid vector index cache string".to_string(),
        ))
    })
}

fn sync_parent_dir(path: &Path) -> DatabaseResult<()> {
    File::open(path)
        .and_then(|file| file.sync_all())
        .map_err(storage_io_error)
}

fn storage_io_error(err: std::io::Error) -> DatabaseError {
    DatabaseError::Storage(StorageError::Io(err))
}

fn upsert_node_into_vector_index(
    index: &mut HnswVectorIndex,
    node: &Node,
    definition: &IndexDefinition,
    dimensions: usize,
) {
    if !node.labels.iter().any(|label| label == &definition.label) {
        return;
    }
    let Some(Value::Vector(vector)) = node.properties.get(&definition.property) else {
        return;
    };
    if vector.len() == dimensions {
        index.upsert(node.id, vector.clone());
    }
}
