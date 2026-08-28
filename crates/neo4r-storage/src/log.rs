use crate::codec::{decode_command, decode_log_entry, encode_command, encode_log_entry};
use neo4r_core::{Command, LogEntry, LogIndex, ShardId};
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{self, ErrorKind, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

const MAGIC_V1: &[u8] = b"N4RLOG1\n";
const MAGIC_V2: &[u8] = b"N4RLOG2\n";

pub type StorageResult<T> = std::result::Result<T, StorageError>;

#[derive(Debug)]
pub enum StorageError {
    Io(io::Error),
    CorruptLog(String),
    CorruptSnapshot(String),
    CorruptStore(String),
    WrongShard { expected: ShardId, actual: ShardId },
}

impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(err) => write!(f, "{err}"),
            Self::CorruptLog(message) => write!(f, "corrupt command log: {message}"),
            Self::CorruptSnapshot(message) => write!(f, "corrupt snapshot: {message}"),
            Self::CorruptStore(message) => write!(f, "corrupt graph store: {message}"),
            Self::WrongShard { expected, actual } => {
                write!(f, "wrong shard entry: expected {expected}, got {actual}")
            }
        }
    }
}

impl std::error::Error for StorageError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(err) => Some(err),
            Self::CorruptLog(_) => None,
            Self::CorruptSnapshot(_) => None,
            Self::CorruptStore(_) => None,
            Self::WrongShard { .. } => None,
        }
    }
}

impl From<io::Error> for StorageError {
    fn from(err: io::Error) -> Self {
        Self::Io(err)
    }
}

pub struct CommandLog {
    file: File,
}

impl CommandLog {
    pub fn open(path: impl AsRef<Path>) -> StorageResult<Self> {
        let file = open_log_file(path)?;
        Ok(Self { file })
    }

    pub fn append(&mut self, command: &Command) -> StorageResult<()> {
        let payload = encode_command(command);
        write_payload(&mut self.file, &payload, true)
    }

    pub fn replay(&mut self) -> StorageResult<Vec<Command>> {
        self.file.seek(SeekFrom::Start(0))?;
        validate_header(&mut self.file)?;

        let mut commands = Vec::new();
        loop {
            let Some(payload) = read_payload(&mut self.file)? else {
                break;
            };
            commands.push(decode_command(&payload)?);
        }

        self.file.seek(SeekFrom::End(0))?;
        Ok(commands)
    }
}

pub struct ShardLog {
    shard_id: ShardId,
    file: File,
    path: PathBuf,
}

impl ShardLog {
    pub fn open(data_dir: impl AsRef<Path>, shard_id: ShardId) -> StorageResult<Self> {
        let shard_dir = data_dir.as_ref().join("shards").join(shard_id.to_string());
        fs::create_dir_all(&shard_dir)?;

        let path = shard_dir.join("command.log");
        let file = open_log_file(&path)?;
        Ok(Self {
            shard_id,
            file,
            path,
        })
    }

    pub fn shard_id(&self) -> ShardId {
        self.shard_id
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn append(&mut self, entry: &LogEntry) -> StorageResult<()> {
        if entry.shard_id != self.shard_id {
            return Err(StorageError::WrongShard {
                expected: self.shard_id,
                actual: entry.shard_id,
            });
        }

        let payload = encode_log_entry(entry);
        write_payload(&mut self.file, &payload, true)
    }

    pub fn replay(&mut self) -> StorageResult<Vec<LogEntry>> {
        self.file.seek(SeekFrom::Start(0))?;
        validate_header(&mut self.file)?;

        let mut entries = Vec::new();
        loop {
            let Some(payload) = read_payload(&mut self.file)? else {
                break;
            };
            let entry = decode_log_entry(&payload)?;
            if entry.shard_id != self.shard_id {
                return Err(StorageError::WrongShard {
                    expected: self.shard_id,
                    actual: entry.shard_id,
                });
            }
            entries.push(entry);
        }

        self.file.seek(SeekFrom::End(0))?;
        Ok(entries)
    }
}

pub struct SegmentedShardLog {
    shard_id: ShardId,
    entries_per_segment: LogIndex,
    segments_dir: PathBuf,
}

impl SegmentedShardLog {
    pub fn open(
        data_dir: impl AsRef<Path>,
        shard_id: ShardId,
        entries_per_segment: LogIndex,
    ) -> StorageResult<Self> {
        if entries_per_segment == 0 {
            return Err(StorageError::CorruptLog(
                "entries per segment must be greater than zero".to_string(),
            ));
        }

        let segments_dir = data_dir
            .as_ref()
            .join("shards")
            .join(shard_id.to_string())
            .join("segments");
        fs::create_dir_all(&segments_dir)?;
        Ok(Self {
            shard_id,
            entries_per_segment,
            segments_dir,
        })
    }

    pub fn shard_id(&self) -> ShardId {
        self.shard_id
    }

    pub fn entries_per_segment(&self) -> LogIndex {
        self.entries_per_segment
    }

    pub fn segments_dir(&self) -> &Path {
        &self.segments_dir
    }

    pub fn segment_path_for_index(&self, index: LogIndex) -> PathBuf {
        self.segments_dir
            .join(format!("{:020}.log", self.segment_start_for_index(index)))
    }

    pub fn append(&self, entry: &LogEntry) -> StorageResult<()> {
        self.append_with_sync(entry, true)
    }

    pub fn append_with_sync(&self, entry: &LogEntry, sync: bool) -> StorageResult<()> {
        if entry.shard_id != self.shard_id {
            return Err(StorageError::WrongShard {
                expected: self.shard_id,
                actual: entry.shard_id,
            });
        }

        let mut file = open_log_file(self.segment_path_for_index(entry.index))?;
        write_payload(&mut file, &encode_log_entry(entry), sync)
    }

    pub fn sync_segment_for_index(&self, index: LogIndex) -> StorageResult<()> {
        let file = open_log_file(self.segment_path_for_index(index))?;
        file.sync_all()?;
        Ok(())
    }

    pub fn replay(&self) -> StorageResult<Vec<LogEntry>> {
        self.replay_from(0)
    }

    pub fn replay_from(&self, start_index: LogIndex) -> StorageResult<Vec<LogEntry>> {
        let mut entries = Vec::new();
        for path in self.segment_paths_from(start_index)? {
            let mut file = open_log_file(path)?;
            file.seek(SeekFrom::Start(0))?;
            validate_header(&mut file)?;
            loop {
                let Some(payload) = read_payload(&mut file)? else {
                    break;
                };
                let entry = decode_log_entry(&payload)?;
                if entry.shard_id != self.shard_id {
                    return Err(StorageError::WrongShard {
                        expected: self.shard_id,
                        actual: entry.shard_id,
                    });
                }
                if entry.index >= start_index {
                    entries.push(entry);
                }
            }
        }
        Ok(entries)
    }

    pub fn entry(&self, index: LogIndex) -> StorageResult<Option<LogEntry>> {
        Ok(self
            .replay_from(index)?
            .into_iter()
            .find(|entry| entry.index == index))
    }

    pub fn truncate_from(&self, index: LogIndex) -> StorageResult<()> {
        let retained = self
            .replay()?
            .into_iter()
            .filter(|entry| entry.index < index)
            .collect::<Vec<_>>();
        let tmp_dir = self.segments_dir.with_extension("segments.tmp");
        if tmp_dir.exists() {
            fs::remove_dir_all(&tmp_dir)?;
        }
        fs::create_dir_all(&tmp_dir)?;
        let tmp_log = SegmentedShardLog {
            shard_id: self.shard_id,
            entries_per_segment: self.entries_per_segment,
            segments_dir: tmp_dir.clone(),
        };
        for entry in &retained {
            tmp_log.append(entry)?;
        }
        let old_dir = self.segments_dir.with_extension("segments.old");
        if old_dir.exists() {
            fs::remove_dir_all(&old_dir)?;
        }
        fs::rename(&self.segments_dir, &old_dir)?;
        fs::rename(&tmp_dir, &self.segments_dir)?;
        sync_parent_dir(&self.segments_dir)?;
        fs::remove_dir_all(old_dir)?;
        Ok(())
    }

    fn segment_paths_from(&self, start_index: LogIndex) -> StorageResult<Vec<PathBuf>> {
        let start_segment = self.segment_start_for_index(start_index);
        let mut paths = Vec::new();
        for entry in fs::read_dir(&self.segments_dir)? {
            let path = entry?.path();
            if !path.is_file() {
                continue;
            }
            let Some(segment_start) = segment_start_from_path(&path)? else {
                continue;
            };
            if segment_start >= start_segment {
                paths.push((segment_start, path));
            }
        }
        paths.sort_by_key(|(segment_start, _)| *segment_start);
        Ok(paths.into_iter().map(|(_, path)| path).collect())
    }

    pub fn segment_start_for_index(&self, index: LogIndex) -> LogIndex {
        if index == 0 {
            0
        } else {
            ((index - 1) / self.entries_per_segment) * self.entries_per_segment + 1
        }
    }
}

fn open_log_file(path: impl AsRef<Path>) -> StorageResult<File> {
    let mut file = OpenOptions::new()
        .create(true)
        .read(true)
        .append(true)
        .open(path)?;

    if file.metadata()?.len() == 0 {
        file.write_all(MAGIC_V2)?;
        file.flush()?;
    } else {
        validate_header(&mut file)?;
    }

    file.seek(SeekFrom::End(0))?;
    Ok(file)
}

fn segment_start_from_path(path: &Path) -> StorageResult<Option<LogIndex>> {
    if path.extension().and_then(|extension| extension.to_str()) != Some("log") {
        return Ok(None);
    }
    let Some(stem) = path.file_stem().and_then(|stem| stem.to_str()) else {
        return Err(StorageError::CorruptLog(format!(
            "invalid segment file name {}",
            path.display()
        )));
    };
    stem.parse::<LogIndex>()
        .map(Some)
        .map_err(|_| StorageError::CorruptLog(format!("invalid segment file name {stem}")))
}

fn write_payload(file: &mut File, payload: &[u8], sync: bool) -> StorageResult<()> {
    let len = u32::try_from(payload.len())
        .map_err(|_| StorageError::CorruptLog("log payload is too large".to_string()))?;

    file.write_all(&len.to_be_bytes())?;
    file.write_all(payload)?;
    file.flush()?;
    if sync {
        file.sync_all()?;
    }
    Ok(())
}

fn read_payload(file: &mut File) -> StorageResult<Option<Vec<u8>>> {
    let Some(len) = read_entry_len(file)? else {
        return Ok(None);
    };
    let mut payload = vec![0; len as usize];
    file.read_exact(&mut payload).map_err(|err| {
        if err.kind() == ErrorKind::UnexpectedEof {
            StorageError::CorruptLog("truncated command payload".to_string())
        } else {
            StorageError::Io(err)
        }
    })?;
    Ok(Some(payload))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LogFileFormatVersion {
    V1,
    V2,
}

fn validate_header(file: &mut File) -> StorageResult<LogFileFormatVersion> {
    file.seek(SeekFrom::Start(0))?;
    let mut header = [0; MAGIC_V1.len()];
    file.read_exact(&mut header).map_err(|err| {
        if err.kind() == ErrorKind::UnexpectedEof {
            StorageError::CorruptLog("missing header".to_string())
        } else {
            StorageError::Io(err)
        }
    })?;

    if header == MAGIC_V1 {
        Ok(LogFileFormatVersion::V1)
    } else if header == MAGIC_V2 {
        Ok(LogFileFormatVersion::V2)
    } else {
        Err(StorageError::CorruptLog("invalid header".to_string()))
    }
}

fn read_entry_len(file: &mut File) -> StorageResult<Option<u32>> {
    let mut len = [0; 4];
    let read = file.read(&mut len)?;
    if read == 0 {
        return Ok(None);
    }
    if read < len.len() {
        file.read_exact(&mut len[read..]).map_err(|err| {
            if err.kind() == ErrorKind::UnexpectedEof {
                StorageError::CorruptLog("truncated command length".to_string())
            } else {
                StorageError::Io(err)
            }
        })?;
    }
    Ok(Some(u32::from_be_bytes(len)))
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
    use neo4r_core::{GraphState, Properties, Value};
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn appends_and_replays_commands() {
        let path = std::env::temp_dir().join(format!(
            "neo4r-command-log-{}.bin",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));

        let commands = vec![
            Command::CreateNode {
                id: 1,
                labels: vec!["Person".to_string()],
                properties: Properties::new(),
            },
            Command::CreateNode {
                id: 2,
                labels: vec!["Person".to_string()],
                properties: Properties::new(),
            },
            Command::CreateRelationship {
                id: 1,
                from: 1,
                to: 2,
                rel_type: "KNOWS".to_string(),
                properties: Properties::new(),
            },
            Command::SetNodeProperty {
                id: 1,
                key: "name".to_string(),
                value: Value::String("Alice".to_string()),
            },
        ];

        {
            let mut log = CommandLog::open(&path).unwrap();
            for command in &commands {
                log.append(command).unwrap();
            }
        }

        let mut log = CommandLog::open(&path).unwrap();
        let replayed = log.replay().unwrap();
        assert_eq!(commands, replayed);

        let mut graph = GraphState::new();
        for command in replayed {
            graph.apply(command).unwrap();
        }
        assert_eq!(graph.node_count(), 2);
        assert_eq!(graph.relationship_count(), 1);

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn shard_log_uses_shard_local_path_and_replays_entries() {
        let dir = temp_dir("neo4r-shard-log");
        let entries = vec![
            LogEntry::new(
                2,
                1,
                1,
                Command::CreateNode {
                    id: 2,
                    labels: vec!["Person".to_string()],
                    properties: Properties::new(),
                },
            ),
            LogEntry::new(
                2,
                1,
                2,
                Command::SetNodeProperty {
                    id: 2,
                    key: "name".to_string(),
                    value: Value::String("Alice".to_string()),
                },
            ),
        ];

        {
            let mut log = ShardLog::open(&dir, 2).unwrap();
            assert_eq!(log.shard_id(), 2);
            assert!(log.path().ends_with("shards/2/command.log"));
            for entry in &entries {
                log.append(entry).unwrap();
            }
        }

        let mut log = ShardLog::open(&dir, 2).unwrap();
        let replayed = log.replay().unwrap();
        assert_eq!(replayed, entries);

        let mut graph = GraphState::new();
        for entry in replayed {
            graph.apply(entry.command).unwrap();
        }
        assert_eq!(graph.node_count(), 1);

        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn shard_log_replays_legacy_and_versioned_entries_in_same_file() {
        let dir = temp_dir("neo4r-shard-log-mixed-format");
        let mut legacy_payload = Vec::new();
        let command = Command::CreateNode {
            id: 2,
            labels: vec!["Person".to_string()],
            properties: Properties::new(),
        };
        let encoded_command = encode_command(&command);
        legacy_payload.extend_from_slice(&2_u64.to_be_bytes());
        legacy_payload.extend_from_slice(&1_u64.to_be_bytes());
        legacy_payload.extend_from_slice(&1_u64.to_be_bytes());
        legacy_payload.extend_from_slice(&(encoded_command.len() as u32).to_be_bytes());
        legacy_payload.extend_from_slice(&encoded_command);

        let legacy_path = dir.join("shards").join("2").join("command.log");
        std::fs::create_dir_all(legacy_path.parent().unwrap()).unwrap();
        {
            let mut file = File::create(&legacy_path).unwrap();
            file.write_all(MAGIC_V1).unwrap();
            write_payload(&mut file, &legacy_payload, true).unwrap();
        }

        {
            let mut log = ShardLog::open(&dir, 2).unwrap();
            log.append(&LogEntry::new(
                2,
                1,
                2,
                Command::SetNodeProperty {
                    id: 2,
                    key: "name".to_string(),
                    value: Value::String("Alice".to_string()),
                },
            ))
            .unwrap();
        }

        let mut log = ShardLog::open(&dir, 2).unwrap();
        let replayed = log.replay().unwrap();

        assert_eq!(replayed.len(), 2);
        assert_eq!(replayed[0].timestamp, neo4r_core::HybridTimestamp::zero());
        assert_eq!(
            replayed.iter().map(|entry| entry.index).collect::<Vec<_>>(),
            vec![1, 2]
        );

        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn shard_log_rejects_entries_for_other_shards() {
        let dir = temp_dir("neo4r-wrong-shard-log");
        let mut log = ShardLog::open(&dir, 1).unwrap();

        let err = log
            .append(&LogEntry::new(
                2,
                1,
                1,
                Command::CreateNode {
                    id: 2,
                    labels: vec![],
                    properties: Properties::new(),
                },
            ))
            .unwrap_err();

        match err {
            StorageError::WrongShard {
                expected: 1,
                actual: 2,
            } => {}
            other => panic!("unexpected error: {other}"),
        }

        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn segmented_shard_log_splits_entries_by_log_index() {
        let dir = temp_dir("neo4r-segmented-shard-log");
        let log = SegmentedShardLog::open(&dir, 2, 2).unwrap();

        for index in 1..=5 {
            log.append(&LogEntry::new(
                2,
                1,
                index,
                Command::CreateNode {
                    id: index,
                    labels: vec!["Person".to_string()],
                    properties: Properties::new(),
                },
            ))
            .unwrap();
        }

        assert!(log
            .segment_path_for_index(1)
            .ends_with("00000000000000000001.log"));
        assert!(log
            .segment_path_for_index(2)
            .ends_with("00000000000000000001.log"));
        assert!(log
            .segment_path_for_index(3)
            .ends_with("00000000000000000003.log"));
        assert!(log
            .segment_path_for_index(5)
            .ends_with("00000000000000000005.log"));

        let replayed = log.replay().unwrap();
        assert_eq!(
            replayed.iter().map(|entry| entry.index).collect::<Vec<_>>(),
            vec![1, 2, 3, 4, 5]
        );

        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn segmented_shard_log_replays_from_position() {
        let dir = temp_dir("neo4r-segmented-shard-log-from");
        let log = SegmentedShardLog::open(&dir, 3, 2).unwrap();

        for index in 1..=5 {
            log.append(&LogEntry::new(
                3,
                1,
                index,
                Command::SetNodeProperty {
                    id: 1,
                    key: "position".to_string(),
                    value: Value::Int(index as i64),
                },
            ))
            .unwrap();
        }

        let replayed = log.replay_from(4).unwrap();

        assert_eq!(
            replayed.iter().map(|entry| entry.index).collect::<Vec<_>>(),
            vec![4, 5]
        );

        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn segmented_shard_log_truncates_suffix_durably() {
        let dir = temp_dir("neo4r-segmented-shard-log-truncate");
        let log = SegmentedShardLog::open(&dir, 3, 2).unwrap();

        for index in 1..=5 {
            log.append(&LogEntry::new(
                3,
                index,
                index,
                Command::SetNodeProperty {
                    id: 1,
                    key: "position".to_string(),
                    value: Value::Int(index as i64),
                },
            ))
            .unwrap();
        }

        log.truncate_from(4).unwrap();
        log.append(&LogEntry::new(
            3,
            9,
            4,
            Command::SetNodeProperty {
                id: 1,
                key: "position".to_string(),
                value: Value::Int(99),
            },
        ))
        .unwrap();

        let reopened = SegmentedShardLog::open(&dir, 3, 2).unwrap();
        let replayed = reopened.replay().unwrap();
        assert_eq!(
            replayed
                .iter()
                .map(|entry| (entry.index, entry.term))
                .collect::<Vec<_>>(),
            vec![(1, 1), (2, 2), (3, 3), (4, 9)]
        );

        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn segmented_shard_log_rejects_entries_for_other_shards() {
        let dir = temp_dir("neo4r-segmented-wrong-shard-log");
        let log = SegmentedShardLog::open(&dir, 1, 2).unwrap();

        let err = log
            .append(&LogEntry::new(
                2,
                1,
                1,
                Command::CreateNode {
                    id: 2,
                    labels: vec![],
                    properties: Properties::new(),
                },
            ))
            .unwrap_err();

        match err {
            StorageError::WrongShard {
                expected: 1,
                actual: 2,
            } => {}
            other => panic!("unexpected error: {other}"),
        }

        let _ = std::fs::remove_dir_all(dir);
    }

    fn temp_dir(prefix: &str) -> std::path::PathBuf {
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
