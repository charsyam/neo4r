use crate::{StorageError, StorageResult};
use neo4r_core::{Command, HybridTimestamp, LogEntry, Properties, Value};

const CREATE_NODE: u8 = 1;
const CREATE_RELATIONSHIP: u8 = 2;
const SET_NODE_PROPERTY: u8 = 3;
const SET_RELATIONSHIP_PROPERTY: u8 = 4;
const DELETE_RELATIONSHIP: u8 = 5;
const DELETE_NODE: u8 = 6;
const UPSERT_BOUNDARY_NODE: u8 = 7;
const REMOVE_NODE_PROPERTY: u8 = 8;
const REMOVE_RELATIONSHIP_PROPERTY: u8 = 9;
const ADD_NODE_LABEL: u8 = 10;
const REMOVE_NODE_LABEL: u8 = 11;

const VALUE_NULL: u8 = 0;
const VALUE_BOOL: u8 = 1;
const VALUE_INT: u8 = 2;
const VALUE_FLOAT: u8 = 3;
const VALUE_STRING: u8 = 4;
const VALUE_VECTOR: u8 = 5;
const VALUE_MAP: u8 = 6;

const LOG_ENTRY_V2_MAGIC: &[u8] = b"N4RLE2\n";
const LOG_ENTRY_V3_MAGIC: &[u8] = b"N4RLE3\n";

pub fn encode_command(command: &Command) -> Vec<u8> {
    let mut out = Vec::new();
    match command {
        Command::CreateNode {
            id,
            labels,
            properties,
        } => {
            write_u8(&mut out, CREATE_NODE);
            write_u64(&mut out, *id);
            write_strings(&mut out, labels);
            write_properties(&mut out, properties);
        }
        Command::CreateRelationship {
            id,
            from,
            to,
            rel_type,
            properties,
        } => {
            write_u8(&mut out, CREATE_RELATIONSHIP);
            write_u64(&mut out, *id);
            write_u64(&mut out, *from);
            write_u64(&mut out, *to);
            write_string(&mut out, rel_type);
            write_properties(&mut out, properties);
        }
        Command::UpsertBoundaryNode {
            id,
            owner_shard,
            labels,
            properties,
            version,
        } => {
            write_u8(&mut out, UPSERT_BOUNDARY_NODE);
            write_u64(&mut out, *id);
            write_u64(&mut out, *owner_shard);
            write_strings(&mut out, labels);
            write_properties(&mut out, properties);
            write_u64(&mut out, *version);
        }
        Command::SetNodeProperty { id, key, value } => {
            write_u8(&mut out, SET_NODE_PROPERTY);
            write_u64(&mut out, *id);
            write_string(&mut out, key);
            write_value(&mut out, value);
        }
        Command::RemoveNodeProperty { id, key } => {
            write_u8(&mut out, REMOVE_NODE_PROPERTY);
            write_u64(&mut out, *id);
            write_string(&mut out, key);
        }
        Command::AddNodeLabel { id, label } => {
            write_u8(&mut out, ADD_NODE_LABEL);
            write_u64(&mut out, *id);
            write_string(&mut out, label);
        }
        Command::RemoveNodeLabel { id, label } => {
            write_u8(&mut out, REMOVE_NODE_LABEL);
            write_u64(&mut out, *id);
            write_string(&mut out, label);
        }
        Command::SetRelationshipProperty { id, key, value } => {
            write_u8(&mut out, SET_RELATIONSHIP_PROPERTY);
            write_u64(&mut out, *id);
            write_string(&mut out, key);
            write_value(&mut out, value);
        }
        Command::RemoveRelationshipProperty { id, key } => {
            write_u8(&mut out, REMOVE_RELATIONSHIP_PROPERTY);
            write_u64(&mut out, *id);
            write_string(&mut out, key);
        }
        Command::DeleteRelationship { id } => {
            write_u8(&mut out, DELETE_RELATIONSHIP);
            write_u64(&mut out, *id);
        }
        Command::DeleteNode { id } => {
            write_u8(&mut out, DELETE_NODE);
            write_u64(&mut out, *id);
        }
    }
    out
}

pub fn encode_log_entry(entry: &LogEntry) -> Vec<u8> {
    let command = encode_command(&entry.command);
    let mut out = Vec::new();
    out.extend_from_slice(LOG_ENTRY_V3_MAGIC);
    write_u64(&mut out, entry.shard_id);
    write_u64(&mut out, entry.term);
    write_u64(&mut out, entry.index);
    write_u64(&mut out, entry.origin_server_id);
    write_u64(&mut out, entry.config_version);
    write_u64(&mut out, entry.timestamp.physical_ms);
    write_u32(&mut out, entry.timestamp.logical);
    write_u32(&mut out, command.len() as u32);
    out.extend_from_slice(&command);
    out
}

pub fn decode_command(input: &[u8]) -> StorageResult<Command> {
    let mut reader = Reader::new(input);
    let tag = reader.read_u8()?;
    let command = match tag {
        CREATE_NODE => Command::CreateNode {
            id: reader.read_u64()?,
            labels: reader.read_strings()?,
            properties: reader.read_properties()?,
        },
        CREATE_RELATIONSHIP => Command::CreateRelationship {
            id: reader.read_u64()?,
            from: reader.read_u64()?,
            to: reader.read_u64()?,
            rel_type: reader.read_string()?,
            properties: reader.read_properties()?,
        },
        UPSERT_BOUNDARY_NODE => Command::UpsertBoundaryNode {
            id: reader.read_u64()?,
            owner_shard: reader.read_u64()?,
            labels: reader.read_strings()?,
            properties: reader.read_properties()?,
            version: reader.read_u64()?,
        },
        SET_NODE_PROPERTY => Command::SetNodeProperty {
            id: reader.read_u64()?,
            key: reader.read_string()?,
            value: reader.read_value()?,
        },
        REMOVE_NODE_PROPERTY => Command::RemoveNodeProperty {
            id: reader.read_u64()?,
            key: reader.read_string()?,
        },
        ADD_NODE_LABEL => Command::AddNodeLabel {
            id: reader.read_u64()?,
            label: reader.read_string()?,
        },
        REMOVE_NODE_LABEL => Command::RemoveNodeLabel {
            id: reader.read_u64()?,
            label: reader.read_string()?,
        },
        SET_RELATIONSHIP_PROPERTY => Command::SetRelationshipProperty {
            id: reader.read_u64()?,
            key: reader.read_string()?,
            value: reader.read_value()?,
        },
        REMOVE_RELATIONSHIP_PROPERTY => Command::RemoveRelationshipProperty {
            id: reader.read_u64()?,
            key: reader.read_string()?,
        },
        DELETE_RELATIONSHIP => Command::DeleteRelationship {
            id: reader.read_u64()?,
        },
        DELETE_NODE => Command::DeleteNode {
            id: reader.read_u64()?,
        },
        _ => {
            return Err(StorageError::CorruptLog(format!(
                "unknown command tag {tag}"
            )))
        }
    };
    reader.finish()?;
    Ok(command)
}

pub fn decode_log_entry(input: &[u8]) -> StorageResult<LogEntry> {
    if let Some(payload) = input.strip_prefix(LOG_ENTRY_V3_MAGIC) {
        return decode_log_entry_v3(payload);
    }
    if let Some(payload) = input.strip_prefix(LOG_ENTRY_V2_MAGIC) {
        return decode_log_entry_v2(payload);
    }

    decode_log_entry_v1(input)
}

fn decode_log_entry_v3(input: &[u8]) -> StorageResult<LogEntry> {
    let mut reader = Reader::new(input);
    let shard_id = reader.read_u64()?;
    let term = reader.read_u64()?;
    let index = reader.read_u64()?;
    let origin_server_id = reader.read_u64()?;
    let config_version = reader.read_u64()?;
    let timestamp = HybridTimestamp::new(reader.read_u64()?, reader.read_u32()?);
    let command_len = reader.read_u32()? as usize;
    let command = decode_command(reader.read_exact(command_len)?)?;
    reader.finish()?;
    Ok(LogEntry::new_with_metadata(
        shard_id,
        term,
        index,
        origin_server_id,
        config_version,
        timestamp,
        command,
    ))
}

fn decode_log_entry_v2(input: &[u8]) -> StorageResult<LogEntry> {
    let mut reader = Reader::new(input);
    let shard_id = reader.read_u64()?;
    let term = reader.read_u64()?;
    let index = reader.read_u64()?;
    let timestamp = HybridTimestamp::new(reader.read_u64()?, reader.read_u32()?);
    let command_len = reader.read_u32()? as usize;
    let command = decode_command(reader.read_exact(command_len)?)?;
    reader.finish()?;
    Ok(LogEntry::new_with_timestamp(
        shard_id, term, index, timestamp, command,
    ))
}

fn decode_log_entry_v1(input: &[u8]) -> StorageResult<LogEntry> {
    let mut reader = Reader::new(input);
    let shard_id = reader.read_u64()?;
    let term = reader.read_u64()?;
    let index = reader.read_u64()?;
    let command_len = reader.read_u32()? as usize;
    let command = decode_command(reader.read_exact(command_len)?)?;
    reader.finish()?;
    Ok(LogEntry::new(shard_id, term, index, command))
}

fn write_u8(out: &mut Vec<u8>, value: u8) {
    out.push(value);
}

fn write_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_be_bytes());
}

fn write_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_be_bytes());
}

fn write_i64(out: &mut Vec<u8>, value: i64) {
    out.extend_from_slice(&value.to_be_bytes());
}

fn write_string(out: &mut Vec<u8>, value: &str) {
    write_u32(out, value.len() as u32);
    out.extend_from_slice(value.as_bytes());
}

fn write_strings(out: &mut Vec<u8>, values: &[String]) {
    write_u32(out, values.len() as u32);
    for value in values {
        write_string(out, value);
    }
}

fn write_properties(out: &mut Vec<u8>, properties: &Properties) {
    let mut entries = properties.iter().collect::<Vec<_>>();
    entries.sort_by(|(left, _), (right, _)| left.cmp(right));

    write_u32(out, entries.len() as u32);
    for (key, value) in entries {
        write_string(out, key);
        write_value(out, value);
    }
}

fn write_value(out: &mut Vec<u8>, value: &Value) {
    match value {
        Value::Null => write_u8(out, VALUE_NULL),
        Value::Bool(value) => {
            write_u8(out, VALUE_BOOL);
            write_u8(out, u8::from(*value));
        }
        Value::Int(value) => {
            write_u8(out, VALUE_INT);
            write_i64(out, *value);
        }
        Value::Float(value) => {
            write_u8(out, VALUE_FLOAT);
            write_u64(out, value.to_bits());
        }
        Value::String(value) => {
            write_u8(out, VALUE_STRING);
            write_string(out, value);
        }
        Value::Vector(value) => {
            write_u8(out, VALUE_VECTOR);
            write_u32(out, value.len() as u32);
            for item in value {
                write_u32(out, item.to_bits());
            }
        }
        Value::Map(value) => {
            write_u8(out, VALUE_MAP);
            write_properties(out, value);
        }
    }
}

struct Reader<'a> {
    input: &'a [u8],
    position: usize,
}

impl<'a> Reader<'a> {
    fn new(input: &'a [u8]) -> Self {
        Self { input, position: 0 }
    }

    fn finish(&self) -> StorageResult<()> {
        if self.position == self.input.len() {
            Ok(())
        } else {
            Err(StorageError::CorruptLog("trailing bytes".to_string()))
        }
    }

    fn read_u8(&mut self) -> StorageResult<u8> {
        Ok(self.read_exact(1)?[0])
    }

    fn read_u32(&mut self) -> StorageResult<u32> {
        let bytes = self.read_exact(4)?;
        Ok(u32::from_be_bytes(bytes.try_into().unwrap()))
    }

    fn read_u64(&mut self) -> StorageResult<u64> {
        let bytes = self.read_exact(8)?;
        Ok(u64::from_be_bytes(bytes.try_into().unwrap()))
    }

    fn read_i64(&mut self) -> StorageResult<i64> {
        let bytes = self.read_exact(8)?;
        Ok(i64::from_be_bytes(bytes.try_into().unwrap()))
    }

    fn read_string(&mut self) -> StorageResult<String> {
        let len = self.read_u32()? as usize;
        let bytes = self.read_exact(len)?;
        String::from_utf8(bytes.to_vec())
            .map_err(|_| StorageError::CorruptLog("invalid utf-8 string".to_string()))
    }

    fn read_strings(&mut self) -> StorageResult<Vec<String>> {
        let len = self.read_u32()? as usize;
        let mut values = Vec::with_capacity(len);
        for _ in 0..len {
            values.push(self.read_string()?);
        }
        Ok(values)
    }

    fn read_properties(&mut self) -> StorageResult<Properties> {
        let len = self.read_u32()? as usize;
        let mut properties = Properties::with_capacity(len);
        for _ in 0..len {
            let key = self.read_string()?;
            let value = self.read_value()?;
            properties.insert(key, value);
        }
        Ok(properties)
    }

    fn read_value(&mut self) -> StorageResult<Value> {
        match self.read_u8()? {
            VALUE_NULL => Ok(Value::Null),
            VALUE_BOOL => Ok(Value::Bool(self.read_u8()? != 0)),
            VALUE_INT => Ok(Value::Int(self.read_i64()?)),
            VALUE_FLOAT => Ok(Value::Float(f64::from_bits(self.read_u64()?))),
            VALUE_STRING => Ok(Value::String(self.read_string()?)),
            VALUE_VECTOR => {
                let len = self.read_u32()? as usize;
                let mut values = Vec::with_capacity(len);
                for _ in 0..len {
                    values.push(f32::from_bits(self.read_u32()?));
                }
                Ok(Value::Vector(values))
            }
            VALUE_MAP => Ok(Value::Map(self.read_properties()?)),
            tag => Err(StorageError::CorruptLog(format!("unknown value tag {tag}"))),
        }
    }

    fn read_exact(&mut self, len: usize) -> StorageResult<&'a [u8]> {
        let end = self
            .position
            .checked_add(len)
            .ok_or_else(|| StorageError::CorruptLog("length overflow".to_string()))?;
        if end > self.input.len() {
            return Err(StorageError::CorruptLog("truncated payload".to_string()));
        }
        let bytes = &self.input[self.position..end];
        self.position = end;
        Ok(bytes)
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
    fn command_round_trip_preserves_payload() {
        let command = Command::CreateRelationship {
            id: 9,
            from: 1,
            to: 2,
            rel_type: "KNOWS".to_string(),
            properties: properties(&[
                ("active", Value::Bool(true)),
                ("score", Value::Float(3.5)),
                ("since", Value::Int(2026)),
                ("note", Value::String("cluster-safe".to_string())),
                ("embedding", Value::Vector(vec![1.0, 0.25, 0.0])),
            ]),
        };

        assert_eq!(decode_command(&encode_command(&command)).unwrap(), command);
    }

    #[test]
    fn log_entry_round_trip_preserves_routing_metadata() {
        let entry = LogEntry::new_with_metadata(
            3,
            5,
            8,
            12,
            7,
            HybridTimestamp::new(1234, 2),
            Command::SetNodeProperty {
                id: 42,
                key: "status".to_string(),
                value: Value::String("active".to_string()),
            },
        );

        assert_eq!(decode_log_entry(&encode_log_entry(&entry)).unwrap(), entry);
    }

    #[test]
    fn remove_property_commands_round_trip() {
        let commands = vec![
            Command::RemoveNodeProperty {
                id: 42,
                key: "status".to_string(),
            },
            Command::RemoveRelationshipProperty {
                id: 7,
                key: "weight".to_string(),
            },
        ];

        for command in commands {
            assert_eq!(decode_command(&encode_command(&command)).unwrap(), command);
        }
    }

    #[test]
    fn label_update_commands_round_trip() {
        let commands = vec![
            Command::AddNodeLabel {
                id: 42,
                label: "Employee".to_string(),
            },
            Command::RemoveNodeLabel {
                id: 42,
                label: "Person".to_string(),
            },
        ];

        for command in commands {
            assert_eq!(decode_command(&encode_command(&command)).unwrap(), command);
        }
    }

    #[test]
    fn legacy_log_entry_decodes_with_zero_timestamp() {
        let command = Command::SetNodeProperty {
            id: 42,
            key: "status".to_string(),
            value: Value::String("active".to_string()),
        };
        let encoded_command = encode_command(&command);
        let mut payload = Vec::new();
        write_u64(&mut payload, 3);
        write_u64(&mut payload, 5);
        write_u64(&mut payload, 8);
        write_u32(&mut payload, encoded_command.len() as u32);
        payload.extend_from_slice(&encoded_command);

        assert_eq!(
            decode_log_entry(&payload).unwrap(),
            LogEntry::new(3, 5, 8, command)
        );
    }

    #[test]
    fn v2_log_entry_decodes_without_replication_metadata() {
        let command = Command::SetNodeProperty {
            id: 42,
            key: "status".to_string(),
            value: Value::String("active".to_string()),
        };
        let encoded_command = encode_command(&command);
        let mut payload = Vec::new();
        payload.extend_from_slice(LOG_ENTRY_V2_MAGIC);
        write_u64(&mut payload, 3);
        write_u64(&mut payload, 5);
        write_u64(&mut payload, 8);
        write_u64(&mut payload, 1234);
        write_u32(&mut payload, 2);
        write_u32(&mut payload, encoded_command.len() as u32);
        payload.extend_from_slice(&encoded_command);

        assert_eq!(
            decode_log_entry(&payload).unwrap(),
            LogEntry::new_with_timestamp(3, 5, 8, HybridTimestamp::new(1234, 2), command)
        );
    }

    #[test]
    fn boundary_node_command_round_trip_preserves_cache_payload() {
        let command = Command::UpsertBoundaryNode {
            id: 99,
            owner_shard: 7,
            labels: vec!["Person".to_string()],
            properties: properties(&[("name", Value::String("Bob".to_string()))]),
            version: 42,
        };

        assert_eq!(decode_command(&encode_command(&command)).unwrap(), command);
    }
}
