use crate::{StorageError, StorageResult};
use neo4r_core::{HybridTimestamp, LogIndex, ShardId, Term};
use std::fs::{self, File, OpenOptions};
use std::io::{ErrorKind, Read, Write};
use std::path::{Path, PathBuf};

const MAGIC_V1: &[u8] = b"N4RCHK1\n";
const MAGIC_V2: &[u8] = b"N4RCHK2\n";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LoadedCheckpoint {
    pub shard_id: ShardId,
    pub last_applied_term: Term,
    pub last_applied_index: LogIndex,
    pub timestamp: HybridTimestamp,
}

#[derive(Clone, Debug)]
pub struct CheckpointStore {
    shard_id: ShardId,
    path: PathBuf,
}

impl CheckpointStore {
    pub fn open(data_dir: impl AsRef<Path>, shard_id: ShardId) -> StorageResult<Self> {
        let shard_dir = data_dir.as_ref().join("shards").join(shard_id.to_string());
        fs::create_dir_all(&shard_dir)?;
        Ok(Self {
            shard_id,
            path: shard_dir.join("checkpoint.bin"),
        })
    }

    pub fn shard_id(&self) -> ShardId {
        self.shard_id
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn save(&self, last_applied_term: Term, last_applied_index: LogIndex) -> StorageResult<()> {
        self.save_with_timestamp(
            last_applied_term,
            last_applied_index,
            HybridTimestamp::zero(),
        )
    }

    pub fn save_with_timestamp(
        &self,
        last_applied_term: Term,
        last_applied_index: LogIndex,
        timestamp: HybridTimestamp,
    ) -> StorageResult<()> {
        let tmp_path = self.path.with_extension("bin.tmp");
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp_path)?;

        file.write_all(MAGIC_V2)?;
        write_u64(&mut file, self.shard_id)?;
        write_u64(&mut file, last_applied_term)?;
        write_u64(&mut file, last_applied_index)?;
        write_u64(&mut file, timestamp.physical_ms)?;
        write_u64(&mut file, timestamp.logical as u64)?;
        file.sync_all()?;
        drop(file);
        fs::rename(&tmp_path, &self.path)?;
        sync_parent_dir(&self.path)?;
        Ok(())
    }

    pub fn load(&self) -> StorageResult<Option<LoadedCheckpoint>> {
        let mut file = match File::open(&self.path) {
            Ok(file) => file,
            Err(err) if err.kind() == ErrorKind::NotFound => return Ok(None),
            Err(err) => return Err(StorageError::Io(err)),
        };

        let version = read_header(&mut file)?;
        let shard_id = read_u64(&mut file)?;
        if shard_id != self.shard_id {
            return Err(StorageError::WrongShard {
                expected: self.shard_id,
                actual: shard_id,
            });
        }
        let last_applied_term = read_u64(&mut file)?;
        let last_applied_index = read_u64(&mut file)?;
        let timestamp = read_checkpoint_timestamp(&mut file, version)?;
        ensure_eof(&mut file)?;

        Ok(Some(LoadedCheckpoint {
            shard_id,
            last_applied_term,
            last_applied_index,
            timestamp,
        }))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CheckpointFormatVersion {
    V1,
    V2,
}

fn read_header(file: &mut File) -> StorageResult<CheckpointFormatVersion> {
    let mut header = [0; MAGIC_V1.len()];
    file.read_exact(&mut header).map_err(|err| {
        if err.kind() == ErrorKind::UnexpectedEof {
            StorageError::CorruptSnapshot("missing checkpoint header".to_string())
        } else {
            StorageError::Io(err)
        }
    })?;

    if header == MAGIC_V1 {
        Ok(CheckpointFormatVersion::V1)
    } else if header == MAGIC_V2 {
        Ok(CheckpointFormatVersion::V2)
    } else {
        Err(StorageError::CorruptSnapshot(
            "invalid checkpoint header".to_string(),
        ))
    }
}

fn read_checkpoint_timestamp(
    file: &mut File,
    version: CheckpointFormatVersion,
) -> StorageResult<HybridTimestamp> {
    let mut trailing = Vec::new();
    file.read_to_end(&mut trailing)?;
    match (version, trailing.len()) {
        (CheckpointFormatVersion::V1, 0) => Ok(HybridTimestamp::zero()),
        (CheckpointFormatVersion::V1 | CheckpointFormatVersion::V2, 16) => {
            let physical_ms = u64::from_be_bytes(trailing[0..8].try_into().unwrap());
            let logical = u64::from_be_bytes(trailing[8..16].try_into().unwrap());
            Ok(HybridTimestamp::new(physical_ms, logical as u32))
        }
        (CheckpointFormatVersion::V2, 0) => Err(StorageError::CorruptSnapshot(
            "missing checkpoint timestamp".to_string(),
        )),
        _ => Err(StorageError::CorruptSnapshot(
            "invalid checkpoint timestamp payload".to_string(),
        )),
    }
}

fn write_u64(file: &mut File, value: u64) -> StorageResult<()> {
    file.write_all(&value.to_be_bytes())?;
    Ok(())
}

fn read_u64(file: &mut File) -> StorageResult<u64> {
    let mut bytes = [0; 8];
    file.read_exact(&mut bytes).map_err(|err| {
        if err.kind() == ErrorKind::UnexpectedEof {
            StorageError::CorruptSnapshot("truncated checkpoint u64".to_string())
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
        _ => Err(StorageError::CorruptSnapshot(
            "trailing checkpoint bytes".to_string(),
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
    fn missing_checkpoint_loads_as_none() {
        let dir = temp_dir("neo4r-missing-checkpoint");
        let store = CheckpointStore::open(&dir, 1).unwrap();

        assert_eq!(store.load().unwrap(), None);

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn saves_and_loads_checkpoint() {
        let dir = temp_dir("neo4r-checkpoint");
        let store = CheckpointStore::open(&dir, 3).unwrap();

        store.save(7, 42).unwrap();
        let loaded = store.load().unwrap().unwrap();

        assert_eq!(loaded.shard_id, 3);
        assert_eq!(loaded.last_applied_term, 7);
        assert_eq!(loaded.last_applied_index, 42);
        assert_eq!(loaded.timestamp, HybridTimestamp::zero());

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn saves_and_loads_checkpoint_timestamp() {
        let dir = temp_dir("neo4r-checkpoint-timestamp");
        let store = CheckpointStore::open(&dir, 3).unwrap();

        store
            .save_with_timestamp(7, 42, HybridTimestamp::new(1234, 5))
            .unwrap();
        let loaded = store.load().unwrap().unwrap();

        assert_eq!(loaded.timestamp, HybridTimestamp::new(1234, 5));

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn loads_legacy_checkpoint_without_timestamp() {
        let dir = temp_dir("neo4r-legacy-checkpoint");
        let store = CheckpointStore::open(&dir, 3).unwrap();
        {
            let mut file = File::create(store.path()).unwrap();
            file.write_all(MAGIC_V1).unwrap();
            write_u64(&mut file, 3).unwrap();
            write_u64(&mut file, 7).unwrap();
            write_u64(&mut file, 42).unwrap();
        }

        let loaded = store.load().unwrap().unwrap();

        assert_eq!(loaded.last_applied_term, 7);
        assert_eq!(loaded.last_applied_index, 42);
        assert_eq!(loaded.timestamp, HybridTimestamp::zero());

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn rejects_wrong_shard_checkpoint() {
        let dir = temp_dir("neo4r-wrong-checkpoint");
        let source = CheckpointStore::open(&dir, 3).unwrap();
        let target = CheckpointStore::open(&dir, 4).unwrap();

        source.save(1, 9).unwrap();
        fs::copy(source.path(), target.path()).unwrap();

        assert!(matches!(
            target.load(),
            Err(StorageError::WrongShard {
                expected: 4,
                actual: 3
            })
        ));

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
