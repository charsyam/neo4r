use crate::codec::{decode_command, encode_command};
use crate::{StorageError, StorageResult};
use neo4r_core::{Command, GraphState, LogIndex, ShardId, Term};
use std::fs::{self, File, OpenOptions};
use std::io::{ErrorKind, Read, Write};
use std::path::{Path, PathBuf};

const MAGIC: &[u8] = b"N4RSNP1\n";

#[derive(Clone, Debug, PartialEq)]
pub struct LoadedSnapshot {
    pub shard_id: ShardId,
    pub last_included_term: Term,
    pub last_included_index: LogIndex,
    pub graph: GraphState,
}

#[derive(Clone, Debug)]
pub struct SnapshotStore {
    shard_id: ShardId,
    path: PathBuf,
}

impl SnapshotStore {
    pub fn open(data_dir: impl AsRef<Path>, shard_id: ShardId) -> StorageResult<Self> {
        let shard_dir = data_dir.as_ref().join("shards").join(shard_id.to_string());
        fs::create_dir_all(&shard_dir)?;
        Ok(Self {
            shard_id,
            path: shard_dir.join("snapshot.bin"),
        })
    }

    pub fn shard_id(&self) -> ShardId {
        self.shard_id
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn save(
        &self,
        graph: &GraphState,
        last_included_term: Term,
        last_included_index: LogIndex,
    ) -> StorageResult<()> {
        let tmp_path = self.path.with_extension("bin.tmp");
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp_path)?;

        file.write_all(MAGIC)?;
        write_u64(&mut file, self.shard_id)?;
        write_u64(&mut file, last_included_term)?;
        write_u64(&mut file, last_included_index)?;

        let mut nodes = graph.nodes().collect::<Vec<_>>();
        nodes.sort_by_key(|node| node.id);
        write_u32(&mut file, nodes.len())?;
        for node in nodes {
            write_command(
                &mut file,
                &Command::CreateNode {
                    id: node.id,
                    labels: node.labels.clone(),
                    properties: node.properties.clone(),
                },
            )?;
        }

        let mut boundary_nodes = graph.boundary_nodes().collect::<Vec<_>>();
        boundary_nodes.sort_by_key(|node| node.id);
        write_u32(&mut file, boundary_nodes.len())?;
        for node in boundary_nodes {
            write_command(
                &mut file,
                &Command::UpsertBoundaryNode {
                    id: node.id,
                    owner_shard: node.owner_shard,
                    labels: node.labels.clone(),
                    properties: node.properties.clone(),
                    version: node.version,
                },
            )?;
        }

        let mut relationships = graph.relationships().collect::<Vec<_>>();
        relationships.sort_by_key(|relationship| relationship.id);
        write_u32(&mut file, relationships.len())?;
        for relationship in relationships {
            write_command(
                &mut file,
                &Command::CreateRelationship {
                    id: relationship.id,
                    from: relationship.from,
                    to: relationship.to,
                    rel_type: relationship.rel_type.clone(),
                    properties: relationship.properties.clone(),
                },
            )?;
        }

        file.sync_all()?;
        drop(file);
        fs::rename(&tmp_path, &self.path)?;
        sync_parent_dir(&self.path)?;
        Ok(())
    }

    pub fn load(&self) -> StorageResult<Option<LoadedSnapshot>> {
        let mut file = match File::open(&self.path) {
            Ok(file) => file,
            Err(err) if err.kind() == ErrorKind::NotFound => return Ok(None),
            Err(err) => return Err(StorageError::Io(err)),
        };

        validate_header(&mut file)?;
        let shard_id = read_u64(&mut file)?;
        if shard_id != self.shard_id {
            return Err(StorageError::WrongShard {
                expected: self.shard_id,
                actual: shard_id,
            });
        }
        let last_included_term = read_u64(&mut file)?;
        let last_included_index = read_u64(&mut file)?;

        let mut graph = GraphState::new();

        let node_count = read_u32(&mut file)?;
        for _ in 0..node_count {
            let command = read_command(&mut file)?;
            match command {
                Command::CreateNode { .. } => graph.apply(command).map_err(|err| {
                    StorageError::CorruptSnapshot(format!("invalid node command: {err}"))
                })?,
                _ => {
                    return Err(StorageError::CorruptSnapshot(
                        "expected node command".to_string(),
                    ))
                }
            }
        }

        let boundary_node_count = read_u32(&mut file)?;
        for _ in 0..boundary_node_count {
            let command = read_command(&mut file)?;
            match command {
                Command::UpsertBoundaryNode { .. } => graph.apply(command).map_err(|err| {
                    StorageError::CorruptSnapshot(format!("invalid boundary node command: {err}"))
                })?,
                _ => {
                    return Err(StorageError::CorruptSnapshot(
                        "expected boundary node command".to_string(),
                    ))
                }
            }
        }

        let relationship_count = read_u32(&mut file)?;
        for _ in 0..relationship_count {
            let command = read_command(&mut file)?;
            match command {
                Command::CreateRelationship { .. } => graph.apply(command).map_err(|err| {
                    StorageError::CorruptSnapshot(format!("invalid relationship command: {err}"))
                })?,
                _ => {
                    return Err(StorageError::CorruptSnapshot(
                        "expected relationship command".to_string(),
                    ))
                }
            }
        }

        ensure_eof(&mut file)?;
        Ok(Some(LoadedSnapshot {
            shard_id,
            last_included_term,
            last_included_index,
            graph,
        }))
    }
}

fn write_command(file: &mut File, command: &Command) -> StorageResult<()> {
    let payload = encode_command(command);
    write_u32(file, payload.len())?;
    file.write_all(&payload)?;
    Ok(())
}

fn read_command(file: &mut File) -> StorageResult<Command> {
    let len = read_u32(file)? as usize;
    let mut payload = vec![0; len];
    file.read_exact(&mut payload).map_err(|err| {
        if err.kind() == ErrorKind::UnexpectedEof {
            StorageError::CorruptSnapshot("truncated command payload".to_string())
        } else {
            StorageError::Io(err)
        }
    })?;
    decode_command(&payload)
}

fn validate_header(file: &mut File) -> StorageResult<()> {
    let mut header = [0; MAGIC.len()];
    file.read_exact(&mut header).map_err(|err| {
        if err.kind() == ErrorKind::UnexpectedEof {
            StorageError::CorruptSnapshot("missing header".to_string())
        } else {
            StorageError::Io(err)
        }
    })?;

    if header == MAGIC {
        Ok(())
    } else {
        Err(StorageError::CorruptSnapshot("invalid header".to_string()))
    }
}

fn write_u32(file: &mut File, value: usize) -> StorageResult<()> {
    let value = u32::try_from(value)
        .map_err(|_| StorageError::CorruptSnapshot("snapshot section is too large".to_string()))?;
    file.write_all(&value.to_be_bytes())?;
    Ok(())
}

fn read_u32(file: &mut File) -> StorageResult<u32> {
    let mut bytes = [0; 4];
    file.read_exact(&mut bytes).map_err(|err| {
        if err.kind() == ErrorKind::UnexpectedEof {
            StorageError::CorruptSnapshot("truncated u32".to_string())
        } else {
            StorageError::Io(err)
        }
    })?;
    Ok(u32::from_be_bytes(bytes))
}

fn write_u64(file: &mut File, value: u64) -> StorageResult<()> {
    file.write_all(&value.to_be_bytes())?;
    Ok(())
}

fn read_u64(file: &mut File) -> StorageResult<u64> {
    let mut bytes = [0; 8];
    file.read_exact(&mut bytes).map_err(|err| {
        if err.kind() == ErrorKind::UnexpectedEof {
            StorageError::CorruptSnapshot("truncated u64".to_string())
        } else {
            StorageError::Io(err)
        }
    })?;
    Ok(u64::from_be_bytes(bytes))
}

fn ensure_eof(file: &mut File) -> StorageResult<()> {
    let mut trailing = [0; 1];
    match file.read(&mut trailing) {
        Ok(0) => Ok(()),
        Ok(_) => Err(StorageError::CorruptSnapshot("trailing bytes".to_string())),
        Err(err) => Err(StorageError::Io(err)),
    }
}

fn sync_parent_dir(path: &Path) -> StorageResult<()> {
    let Some(parent) = path.parent() else {
        return Ok(());
    };
    File::open(parent)?.sync_all()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use neo4r_core::{Properties, Value};
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn missing_snapshot_loads_as_none() {
        let dir = temp_dir("neo4r-missing-snapshot");
        let store = SnapshotStore::open(&dir, 1).unwrap();

        assert_eq!(store.load().unwrap(), None);

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn saves_and_loads_shard_snapshot() {
        let dir = temp_dir("neo4r-snapshot");
        let graph = graph();
        let store = SnapshotStore::open(&dir, 3).unwrap();

        store.save(&graph, 7, 42).unwrap();
        let loaded = store.load().unwrap().unwrap();

        assert_eq!(loaded.shard_id, 3);
        assert_eq!(loaded.last_included_term, 7);
        assert_eq!(loaded.last_included_index, 42);
        assert_eq!(loaded.graph, graph);

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn saves_and_loads_boundary_nodes() {
        let dir = temp_dir("neo4r-boundary-snapshot");
        let mut graph = GraphState::new();
        graph
            .apply(Command::CreateNode {
                id: 1,
                labels: vec!["Person".to_string()],
                properties: properties(&[("name", Value::String("Alice".to_string()))]),
            })
            .unwrap();
        graph
            .apply(Command::UpsertBoundaryNode {
                id: 2,
                owner_shard: 2,
                labels: vec!["Person".to_string()],
                properties: properties(&[("name", Value::String("RemoteBob".to_string()))]),
                version: 3,
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

        let store = SnapshotStore::open(&dir, 1).unwrap();
        store.save(&graph, 1, 10).unwrap();
        let loaded = store.load().unwrap().unwrap();

        assert_eq!(loaded.graph, graph);
        assert_eq!(loaded.graph.boundary_node_count(), 1);
        assert_eq!(
            loaded
                .graph
                .boundary_node(2)
                .unwrap()
                .properties
                .get("name"),
            Some(&Value::String("RemoteBob".to_string()))
        );

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn snapshot_rejects_wrong_shard() {
        let dir = temp_dir("neo4r-wrong-snapshot");
        let graph = graph();
        let source = SnapshotStore::open(&dir, 3).unwrap();
        let target = SnapshotStore::open(&dir, 4).unwrap();
        source.save(&graph, 1, 2).unwrap();
        fs::copy(source.path(), target.path()).unwrap();

        let err = target.load().unwrap_err();

        match err {
            StorageError::WrongShard {
                expected: 4,
                actual: 3,
            } => {}
            other => panic!("unexpected error: {other}"),
        }

        let _ = fs::remove_dir_all(dir);
    }

    fn graph() -> GraphState {
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
        graph
            .apply(Command::CreateRelationship {
                id: 1,
                from: 1,
                to: 2,
                rel_type: "KNOWS".to_string(),
                properties: Properties::new(),
            })
            .unwrap();
        graph
    }

    fn properties(entries: &[(&str, Value)]) -> Properties {
        entries
            .iter()
            .map(|(key, value)| ((*key).to_string(), value.clone()))
            .collect()
    }

    fn temp_dir(prefix: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "{}-{}",
            prefix,
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ))
    }
}
