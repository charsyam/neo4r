use crate::{StorageError, StorageResult};
use neo4r_core::{LogIndex, ShardId, Term};
use std::fs::{self, File, OpenOptions};
use std::io::{ErrorKind, Read, Write};
use std::path::{Path, PathBuf};

const MAGIC: &[u8] = b"N4RCMT1\n";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LoadedCommit {
    pub shard_id: ShardId,
    pub term: Term,
    pub index: LogIndex,
}

#[derive(Clone, Debug)]
pub struct CommitStore {
    shard_id: ShardId,
    path: PathBuf,
}

impl CommitStore {
    pub fn open(data_dir: impl AsRef<Path>, shard_id: ShardId) -> StorageResult<Self> {
        let shard_dir = data_dir.as_ref().join("shards").join(shard_id.to_string());
        fs::create_dir_all(&shard_dir)?;
        Ok(Self {
            shard_id,
            path: shard_dir.join("commit.bin"),
        })
    }

    pub fn save(&self, term: Term, index: LogIndex) -> StorageResult<()> {
        let tmp_path = self.path.with_extension("bin.tmp");
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp_path)?;
        file.write_all(MAGIC)?;
        write_u64(&mut file, self.shard_id)?;
        write_u64(&mut file, term)?;
        write_u64(&mut file, index)?;
        file.sync_all()?;
        drop(file);
        fs::rename(&tmp_path, &self.path)?;
        sync_parent_dir(&self.path)?;
        Ok(())
    }

    pub fn load(&self) -> StorageResult<Option<LoadedCommit>> {
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
        let term = read_u64(&mut file)?;
        let index = read_u64(&mut file)?;
        ensure_eof(&mut file)?;
        Ok(Some(LoadedCommit {
            shard_id,
            term,
            index,
        }))
    }
}

fn validate_header(file: &mut File) -> StorageResult<()> {
    let mut header = [0; MAGIC.len()];
    file.read_exact(&mut header).map_err(|err| {
        if err.kind() == ErrorKind::UnexpectedEof {
            StorageError::CorruptStore("missing commit header".to_string())
        } else {
            StorageError::Io(err)
        }
    })?;
    if header == MAGIC {
        Ok(())
    } else {
        Err(StorageError::CorruptStore(
            "invalid commit header".to_string(),
        ))
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
            StorageError::CorruptStore("truncated commit u64".to_string())
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
            "trailing commit bytes".to_string(),
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
    fn missing_commit_loads_as_none() {
        let dir = temp_dir("neo4r-missing-commit");
        let store = CommitStore::open(&dir, 1).unwrap();

        assert_eq!(store.load().unwrap(), None);

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn saves_and_loads_commit() {
        let dir = temp_dir("neo4r-commit");
        let store = CommitStore::open(&dir, 3).unwrap();

        store.save(7, 42).unwrap();

        assert_eq!(
            store.load().unwrap().unwrap(),
            LoadedCommit {
                shard_id: 3,
                term: 7,
                index: 42
            }
        );

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
