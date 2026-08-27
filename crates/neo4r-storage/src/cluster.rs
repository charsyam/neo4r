use crate::{StorageError, StorageResult};
use neo4r_core::{ShardPlacement, ShardReplica, ShardRole, ShardRoutingTable};
use std::fs::{self, File, OpenOptions};
use std::io::{ErrorKind, Read, Write};
use std::path::{Path, PathBuf};

const MAGIC: &[u8] = b"N4RRT1\n";

#[derive(Clone, Debug)]
pub struct ShardMetadataStore {
    path: PathBuf,
}

impl ShardMetadataStore {
    pub fn open(data_dir: impl AsRef<Path>) -> StorageResult<Self> {
        let cluster_dir = data_dir.as_ref().join("cluster");
        fs::create_dir_all(&cluster_dir)?;
        Ok(Self {
            path: cluster_dir.join("shard-routing.bin"),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn save(&self, table: &ShardRoutingTable) -> StorageResult<()> {
        let tmp_path = self.path.with_extension("bin.tmp");
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp_path)?;

        file.write_all(MAGIC)?;
        write_u64(&mut file, table.version)?;
        write_u64(&mut file, table.placements.len() as u64)?;
        for placement in &table.placements {
            write_u64(&mut file, placement.shard_id)?;
            write_u64(&mut file, placement.replicas.len() as u64)?;
            for replica in &placement.replicas {
                write_u64(&mut file, replica.server_id)?;
                write_u8(&mut file, encode_role(replica.role))?;
            }
        }
        file.sync_all()?;
        drop(file);
        fs::rename(&tmp_path, &self.path)?;
        sync_parent_dir(&self.path)?;
        Ok(())
    }

    pub fn load(&self) -> StorageResult<Option<ShardRoutingTable>> {
        let mut file = match File::open(&self.path) {
            Ok(file) => file,
            Err(err) if err.kind() == ErrorKind::NotFound => return Ok(None),
            Err(err) => return Err(StorageError::Io(err)),
        };

        validate_header(&mut file)?;
        let version = read_u64(&mut file)?;
        let placement_count = read_u64(&mut file)?;
        let mut placements = Vec::with_capacity(placement_count as usize);
        for _ in 0..placement_count {
            let shard_id = read_u64(&mut file)?;
            let replica_count = read_u64(&mut file)?;
            let mut replicas = Vec::with_capacity(replica_count as usize);
            for _ in 0..replica_count {
                let server_id = read_u64(&mut file)?;
                let role = decode_role(read_u8(&mut file)?)?;
                replicas.push(ShardReplica { server_id, role });
            }
            placements.push(ShardPlacement { shard_id, replicas });
        }
        ensure_eof(&mut file)?;

        Ok(Some(ShardRoutingTable {
            version,
            placements,
        }))
    }
}

fn validate_header(file: &mut File) -> StorageResult<()> {
    let mut header = [0; MAGIC.len()];
    file.read_exact(&mut header).map_err(|err| {
        if err.kind() == ErrorKind::UnexpectedEof {
            StorageError::CorruptStore("missing shard routing header".to_string())
        } else {
            StorageError::Io(err)
        }
    })?;

    if header == MAGIC {
        Ok(())
    } else {
        Err(StorageError::CorruptStore(
            "invalid shard routing header".to_string(),
        ))
    }
}

fn encode_role(role: ShardRole) -> u8 {
    match role {
        ShardRole::Primary => 1,
        ShardRole::Replica => 2,
    }
}

fn decode_role(role: u8) -> StorageResult<ShardRole> {
    match role {
        1 => Ok(ShardRole::Primary),
        2 => Ok(ShardRole::Replica),
        _ => Err(StorageError::CorruptStore(format!(
            "unknown shard replica role {role}"
        ))),
    }
}

fn write_u8(file: &mut File, value: u8) -> StorageResult<()> {
    file.write_all(&[value])?;
    Ok(())
}

fn write_u64(file: &mut File, value: u64) -> StorageResult<()> {
    file.write_all(&value.to_be_bytes())?;
    Ok(())
}

fn read_u8(file: &mut File) -> StorageResult<u8> {
    let mut bytes = [0; 1];
    file.read_exact(&mut bytes).map_err(|err| {
        if err.kind() == ErrorKind::UnexpectedEof {
            StorageError::CorruptStore("truncated shard routing u8".to_string())
        } else {
            StorageError::Io(err)
        }
    })?;
    Ok(bytes[0])
}

fn read_u64(file: &mut File) -> StorageResult<u64> {
    let mut bytes = [0; 8];
    file.read_exact(&mut bytes).map_err(|err| {
        if err.kind() == ErrorKind::UnexpectedEof {
            StorageError::CorruptStore("truncated shard routing u64".to_string())
        } else {
            StorageError::Io(err)
        }
    })?;
    Ok(u64::from_be_bytes(bytes))
}

fn ensure_eof(file: &mut File) -> StorageResult<()> {
    let mut trailing = [0; 1];
    match file.read(&mut trailing)? {
        0 => Ok(()),
        _ => Err(StorageError::CorruptStore(
            "trailing shard routing bytes".to_string(),
        )),
    }
}

fn sync_parent_dir(path: &Path) -> StorageResult<()> {
    if let Some(parent) = path.parent() {
        File::open(parent)?.sync_all()?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn missing_routing_table_loads_as_none() {
        let dir = temp_dir("neo4r-missing-routing");
        let store = ShardMetadataStore::open(&dir).unwrap();

        assert_eq!(store.load().unwrap(), None);

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn saves_and_loads_shard_routing_table() {
        let dir = temp_dir("neo4r-routing");
        let store = ShardMetadataStore::open(&dir).unwrap();
        let table = ShardRoutingTable {
            version: 4,
            placements: vec![
                ShardPlacement::new(
                    0,
                    vec![ShardReplica::primary(10), ShardReplica::replica(11)],
                ),
                ShardPlacement::new(
                    1,
                    vec![ShardReplica::primary(11), ShardReplica::replica(10)],
                ),
            ],
        };

        store.save(&table).unwrap();

        assert_eq!(store.load().unwrap(), Some(table));

        let _ = fs::remove_dir_all(dir);
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
