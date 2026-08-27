use crate::{StorageError, StorageResult};
use std::collections::BTreeSet;
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, ErrorKind, Write};
use std::path::{Path, PathBuf};

const MAGIC: &str = "N4RTXD1";

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TransactionDecision {
    Commit,
    Abort,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransactionParticipantRecord {
    pub location: String,
    pub shard_id: u64,
    pub prepared_id: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransactionDecisionRecord {
    pub tx_id: u64,
    pub decision: TransactionDecision,
    pub participants: Vec<TransactionParticipantRecord>,
}

#[derive(Clone, Debug)]
pub struct TransactionDecisionStore {
    path: PathBuf,
}

impl TransactionDecisionStore {
    pub fn open(data_dir: impl AsRef<Path>) -> StorageResult<Self> {
        let tx_dir = data_dir.as_ref().join("transactions");
        fs::create_dir_all(&tx_dir)?;
        Ok(Self {
            path: tx_dir.join("decisions.log"),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn append(&self, record: &TransactionDecisionRecord) -> StorageResult<()> {
        let needs_header = match fs::metadata(&self.path) {
            Ok(metadata) => metadata.len() == 0,
            Err(err) if err.kind() == ErrorKind::NotFound => true,
            Err(err) => return Err(StorageError::Io(err)),
        };
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        if needs_header {
            writeln!(file, "{MAGIC}")?;
        }
        writeln!(file, "{}", encode_record(record))?;
        file.sync_all()?;
        Ok(())
    }

    pub fn load(&self) -> StorageResult<Vec<TransactionDecisionRecord>> {
        let file = match File::open(&self.path) {
            Ok(file) => file,
            Err(err) if err.kind() == ErrorKind::NotFound => return Ok(Vec::new()),
            Err(err) => return Err(StorageError::Io(err)),
        };
        let mut lines = BufReader::new(file).lines();
        let header = lines.next().transpose()?.ok_or_else(|| {
            StorageError::CorruptStore("missing transaction decision header".to_string())
        })?;
        if header != MAGIC {
            return Err(StorageError::CorruptStore(
                "invalid transaction decision header".to_string(),
            ));
        }
        let mut records = Vec::new();
        for line in lines {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }
            records.push(decode_record(&line)?);
        }
        Ok(records)
    }

    pub fn save_all(&self, records: &[TransactionDecisionRecord]) -> StorageResult<()> {
        let tmp_path = self.path.with_extension("log.tmp");
        {
            let mut file = OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(&tmp_path)?;
            writeln!(file, "{MAGIC}")?;
            for record in records {
                writeln!(file, "{}", encode_record(record))?;
            }
            file.sync_all()?;
        }
        fs::rename(&tmp_path, &self.path)?;
        if let Some(parent) = self.path.parent() {
            File::open(parent)?.sync_all()?;
        }
        Ok(())
    }

    pub fn remove_tx_ids(&self, tx_ids: &BTreeSet<u64>) -> StorageResult<usize> {
        if tx_ids.is_empty() {
            return Ok(0);
        }
        let records = self.load()?;
        let before = records.len();
        let retained = records
            .into_iter()
            .filter(|record| !tx_ids.contains(&record.tx_id))
            .collect::<Vec<_>>();
        let removed = before - retained.len();
        if removed > 0 {
            self.save_all(&retained)?;
        }
        Ok(removed)
    }
}

fn encode_record(record: &TransactionDecisionRecord) -> String {
    let decision = match record.decision {
        TransactionDecision::Commit => "commit",
        TransactionDecision::Abort => "abort",
    };
    let participants = record
        .participants
        .iter()
        .map(|participant| {
            format!(
                "{}:{}:{}",
                hex_encode(participant.location.as_bytes()),
                participant.shard_id,
                participant.prepared_id
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    format!("{decision}\t{}\t{participants}", record.tx_id)
}

fn decode_record(line: &str) -> StorageResult<TransactionDecisionRecord> {
    let parts = line.split('\t').collect::<Vec<_>>();
    if parts.len() != 3 {
        return Err(StorageError::CorruptStore(
            "malformed transaction decision record".to_string(),
        ));
    }
    let decision = match parts[0] {
        "commit" => TransactionDecision::Commit,
        "abort" => TransactionDecision::Abort,
        value => {
            return Err(StorageError::CorruptStore(format!(
                "unknown transaction decision {value}"
            )))
        }
    };
    let tx_id = parse_u64(parts[1], "transaction id")?;
    let mut participants = Vec::new();
    if !parts[2].is_empty() {
        for item in parts[2].split(',') {
            let fields = item.split(':').collect::<Vec<_>>();
            if fields.len() != 3 {
                return Err(StorageError::CorruptStore(
                    "malformed transaction participant record".to_string(),
                ));
            }
            participants.push(TransactionParticipantRecord {
                location: String::from_utf8(hex_decode(fields[0])?).map_err(|_| {
                    StorageError::CorruptStore("participant location is not utf-8".to_string())
                })?,
                shard_id: parse_u64(fields[1], "participant shard id")?,
                prepared_id: parse_u64(fields[2], "participant prepared id")?,
            });
        }
    }
    Ok(TransactionDecisionRecord {
        tx_id,
        decision,
        participants,
    })
}

fn parse_u64(input: &str, name: &str) -> StorageResult<u64> {
    input.parse::<u64>().map_err(|_| {
        StorageError::CorruptStore(format!("invalid transaction decision {name}: {input}"))
    })
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

fn hex_decode(input: &str) -> StorageResult<Vec<u8>> {
    if !input.len().is_multiple_of(2) {
        return Err(StorageError::CorruptStore(
            "odd transaction decision hex length".to_string(),
        ));
    }
    let mut bytes = Vec::with_capacity(input.len() / 2);
    for chunk in input.as_bytes().chunks(2) {
        bytes.push((hex_nibble(chunk[0])? << 4) | hex_nibble(chunk[1])?);
    }
    Ok(bytes)
}

fn hex_nibble(byte: u8) -> StorageResult<u8> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err(StorageError::CorruptStore(
            "invalid transaction decision hex byte".to_string(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn decision_store_appends_and_loads_records() {
        let dir = std::env::temp_dir().join(format!(
            "neo4r-tx-decision-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let store = TransactionDecisionStore::open(&dir).unwrap();
        let record = TransactionDecisionRecord {
            tx_id: 7,
            decision: TransactionDecision::Commit,
            participants: vec![
                TransactionParticipantRecord {
                    location: "local".to_string(),
                    shard_id: 0,
                    prepared_id: 1,
                },
                TransactionParticipantRecord {
                    location: "remote:127.0.0.1:7777".to_string(),
                    shard_id: 1,
                    prepared_id: 2,
                },
            ],
        };

        store.append(&record).unwrap();
        assert_eq!(store.load().unwrap(), vec![record]);
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn decision_store_removes_completed_transactions() {
        let dir = std::env::temp_dir().join(format!(
            "neo4r-tx-decision-remove-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let store = TransactionDecisionStore::open(&dir).unwrap();
        let first = TransactionDecisionRecord {
            tx_id: 7,
            decision: TransactionDecision::Commit,
            participants: vec![TransactionParticipantRecord {
                location: "local".to_string(),
                shard_id: 0,
                prepared_id: 1,
            }],
        };
        let second = TransactionDecisionRecord {
            tx_id: 8,
            decision: TransactionDecision::Abort,
            participants: vec![TransactionParticipantRecord {
                location: "remote:127.0.0.1:7777".to_string(),
                shard_id: 1,
                prepared_id: 2,
            }],
        };

        store.append(&first).unwrap();
        store.append(&second).unwrap();

        assert_eq!(store.remove_tx_ids(&BTreeSet::from([7])).unwrap(), 1);
        assert_eq!(store.load().unwrap(), vec![second]);
        let _ = fs::remove_dir_all(dir);
    }
}
