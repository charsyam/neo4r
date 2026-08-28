//! Shared wire protocol codecs for neo4r clients and servers.

use neo4r_core::{BoundaryNode, Node, Properties, Relationship, Value};
use neo4r_query::{QueryParams, QueryRow, QueryValue};
use std::io::{self, Read, Write};

const MAGIC: [u8; 4] = *b"N4R1";
const VERSION: u8 = 1;
const HEADER_LEN: usize = 20;
const MAX_PAYLOAD_LEN: usize = 16 * 1024 * 1024;

pub fn native_protocol_version() -> u8 {
    VERSION
}

pub fn native_protocol_version_range() -> std::ops::RangeInclusive<u8> {
    VERSION..=VERSION
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum NativeMessageType {
    Ping = 1,
    Quit = 2,
    Query = 3,
    Command = 4,
    Fetch = 5,
    CloseCursor = 6,
    Cancel = 7,
    Response = 128,
    Error = 129,
}

impl NativeMessageType {
    fn from_u8(value: u8) -> io::Result<Self> {
        match value {
            1 => Ok(Self::Ping),
            2 => Ok(Self::Quit),
            3 => Ok(Self::Query),
            4 => Ok(Self::Command),
            5 => Ok(Self::Fetch),
            6 => Ok(Self::CloseCursor),
            7 => Ok(Self::Cancel),
            128 => Ok(Self::Response),
            129 => Ok(Self::Error),
            _ => Err(invalid_data(format!(
                "unknown native message type: {value}"
            ))),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NativeFrame {
    pub message_type: NativeMessageType,
    pub flags: u16,
    pub request_id: u64,
    pub payload: Vec<u8>,
}

impl NativeFrame {
    pub fn new(message_type: NativeMessageType, request_id: u64, payload: Vec<u8>) -> Self {
        Self {
            message_type,
            flags: 0,
            request_id,
            payload,
        }
    }

    pub fn payload_text(&self) -> io::Result<&str> {
        std::str::from_utf8(&self.payload)
            .map_err(|_| invalid_data("native frame payload is not valid UTF-8"))
    }
}

pub fn read_frame(reader: &mut impl Read) -> io::Result<Option<NativeFrame>> {
    let mut header = [0_u8; HEADER_LEN];
    match reader.read_exact(&mut header) {
        Ok(()) => {}
        Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(err) => return Err(err),
    }

    if header[0..4] != MAGIC {
        return Err(invalid_data("invalid native protocol magic"));
    }
    if header[4] != VERSION {
        return Err(invalid_data(format!(
            "unsupported native protocol version: {}",
            header[4]
        )));
    }

    let message_type = NativeMessageType::from_u8(header[5])?;
    let flags = u16::from_be_bytes([header[6], header[7]]);
    let payload_len = u32::from_be_bytes([header[8], header[9], header[10], header[11]]) as usize;
    if payload_len > MAX_PAYLOAD_LEN {
        return Err(invalid_data(format!(
            "native frame payload too large: {payload_len}"
        )));
    }
    let request_id = u64::from_be_bytes([
        header[12], header[13], header[14], header[15], header[16], header[17], header[18],
        header[19],
    ]);

    let mut payload = vec![0_u8; payload_len];
    reader.read_exact(&mut payload)?;

    Ok(Some(NativeFrame {
        message_type,
        flags,
        request_id,
        payload,
    }))
}

pub fn write_frame(writer: &mut impl Write, frame: &NativeFrame) -> io::Result<()> {
    if frame.payload.len() > MAX_PAYLOAD_LEN {
        return Err(invalid_data(format!(
            "native frame payload too large: {}",
            frame.payload.len()
        )));
    }

    let mut header = [0_u8; HEADER_LEN];
    header[0..4].copy_from_slice(&MAGIC);
    header[4] = VERSION;
    header[5] = frame.message_type as u8;
    header[6..8].copy_from_slice(&frame.flags.to_be_bytes());
    header[8..12].copy_from_slice(&(frame.payload.len() as u32).to_be_bytes());
    header[12..20].copy_from_slice(&frame.request_id.to_be_bytes());

    writer.write_all(&header)?;
    writer.write_all(&frame.payload)?;
    writer.flush()
}

pub fn encode_query_payload(query: &str, params: &QueryParams) -> String {
    let mut parts = vec![query.to_string()];
    let mut keys = params.keys().collect::<Vec<_>>();
    keys.sort();
    for key in keys {
        if let Some(value) = params.get(key) {
            parts.push(format!("{key}={}", encode_command_value(value)));
        }
    }
    parts.join("\t")
}

pub fn parse_query_payload(payload: &str) -> Result<(String, QueryParams), String> {
    let mut parts = payload.split('\t');
    let query = parts
        .next()
        .filter(|query| !query.is_empty())
        .ok_or_else(|| "QUERY requires a cypher string".to_string())?
        .to_string();
    let params = parse_command_properties(parts)?;
    Ok((query, params))
}

pub fn encode_query_batch_payload(writes: &[(String, QueryParams)]) -> String {
    let mut parts = vec![writes.len().to_string()];
    for (query, params) in writes {
        parts.push(hex_encode(query.as_bytes()));
        parts.push(encode_properties(params));
    }
    parts.join("\t")
}

pub fn decode_query_batch_payload(payload: &str) -> Result<Vec<(String, QueryParams)>, String> {
    let mut parts = payload.split('\t');
    let count = parse_usize(parts.next(), "QUERY_WRITE_BATCH_SHARD requires write count")?;
    let mut writes = Vec::with_capacity(count);
    for _ in 0..count {
        let query = parts
            .next()
            .ok_or_else(|| "QUERY_WRITE_BATCH_SHARD missing encoded query".to_string())
            .and_then(|value| {
                String::from_utf8(hex_decode(value)?)
                    .map_err(|_| "query is not valid UTF-8".to_string())
            })?;
        let params = parts
            .next()
            .ok_or_else(|| "QUERY_WRITE_BATCH_SHARD missing encoded params".to_string())
            .and_then(decode_properties)?;
        writes.push((query, params));
    }
    if parts.next().is_some() {
        return Err("QUERY_WRITE_BATCH_SHARD got extra fields".to_string());
    }
    Ok(writes)
}

pub fn encode_query_rows(rows: &[QueryRow]) -> String {
    rows.iter()
        .map(encode_query_row)
        .collect::<Vec<_>>()
        .join("|")
}

pub fn decode_query_rows(input: &str) -> Result<Vec<QueryRow>, String> {
    if input.is_empty() {
        return Ok(Vec::new());
    }
    input.split('|').map(decode_query_row).collect()
}

#[derive(Clone, Debug, PartialEq)]
pub struct ResultStart {
    pub cursor_id: u64,
    pub total_rows: Option<usize>,
    pub rows: Vec<QueryRow>,
    pub has_more: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ResultPage {
    pub cursor_id: u64,
    pub rows: Vec<QueryRow>,
    pub has_more: bool,
}

pub fn parse_result_start_response(payload: &str) -> Result<ResultStart, String> {
    let parts = payload.splitn(7, '\t').collect::<Vec<_>>();
    if parts.len() != 7 || parts[0] != "OK" || parts[1] != "RESULT_START" {
        return Err(format!("expected RESULT_START response, got: {payload}"));
    }
    let row_count = parts[4]
        .parse::<usize>()
        .map_err(|_| "RESULT_START row count must be an unsigned integer".to_string())?;
    let rows = decode_query_rows(parts[6])?;
    if rows.len() != row_count {
        return Err(format!(
            "RESULT_START row count mismatch: header {row_count}, decoded {}",
            rows.len()
        ));
    }
    Ok(ResultStart {
        cursor_id: parse_u64(parts[2], "RESULT_START cursor id")?,
        total_rows: if parts[3] == "UNKNOWN" {
            None
        } else {
            Some(parse_usize(Some(parts[3]), "RESULT_START total rows")?)
        },
        rows,
        has_more: parse_bool_token(parts[5], "RESULT_START has_more")?,
    })
}

pub fn parse_result_page_response(payload: &str) -> Result<ResultPage, String> {
    let parts = payload.splitn(6, '\t').collect::<Vec<_>>();
    if parts.len() != 6 || parts[0] != "OK" || parts[1] != "RESULT_PAGE" {
        return Err(format!("expected RESULT_PAGE response, got: {payload}"));
    }
    let row_count = parts[3]
        .parse::<usize>()
        .map_err(|_| "RESULT_PAGE row count must be an unsigned integer".to_string())?;
    let rows = decode_query_rows(parts[5])?;
    if rows.len() != row_count {
        return Err(format!(
            "RESULT_PAGE row count mismatch: header {row_count}, decoded {}",
            rows.len()
        ));
    }
    Ok(ResultPage {
        cursor_id: parse_u64(parts[2], "RESULT_PAGE cursor id")?,
        rows,
        has_more: parse_bool_token(parts[4], "RESULT_PAGE has_more")?,
    })
}

pub fn parse_rows_response(payload: &str) -> Result<Vec<QueryRow>, String> {
    let parts = payload.splitn(4, '\t').collect::<Vec<_>>();
    if parts.len() != 4 || parts[0] != "OK" || parts[1] != "ROWS" {
        return Err(format!("expected ROWS response, got: {payload}"));
    }
    let count = parts[2]
        .parse::<usize>()
        .map_err(|_| "ROWS count must be an unsigned integer".to_string())?;
    let rows = decode_query_rows(parts[3])?;
    if rows.len() != count {
        return Err(format!(
            "ROWS count mismatch: header {count}, decoded {}",
            rows.len()
        ));
    }
    Ok(rows)
}

pub fn response_field(payload: &str, expected_kind: &str) -> Result<String, String> {
    let parts = payload.splitn(3, '\t').collect::<Vec<_>>();
    if parts.len() != 3 || parts[0] != "OK" || parts[1] != expected_kind {
        return Err(format!("expected {expected_kind} response, got: {payload}"));
    }
    Ok(unescape_response(parts[2]))
}

pub fn escape_response(input: &str) -> String {
    input.replace('\\', "\\\\").replace('\n', "\\n")
}

pub fn unescape_response(input: &str) -> String {
    let mut output = String::new();
    let mut escaped = false;
    for ch in input.chars() {
        if escaped {
            match ch {
                'n' => output.push('\n'),
                '\\' => output.push('\\'),
                other => {
                    output.push('\\');
                    output.push(other);
                }
            }
            escaped = false;
        } else if ch == '\\' {
            escaped = true;
        } else {
            output.push(ch);
        }
    }
    if escaped {
        output.push('\\');
    }
    output
}

pub fn encode_properties(properties: &Properties) -> String {
    let mut keys = properties.keys().collect::<Vec<_>>();
    keys.sort();
    keys.into_iter()
        .filter_map(|key| {
            properties
                .get(key)
                .map(|value| format!("{}~{}", hex_encode(key.as_bytes()), encode_value(value)))
        })
        .collect::<Vec<_>>()
        .join(",")
}

pub fn decode_properties(input: &str) -> Result<Properties, String> {
    let mut properties = Properties::new();
    if input.is_empty() {
        return Ok(properties);
    }
    for entry in input.split(',') {
        let (key, value) = entry
            .split_once('~')
            .ok_or_else(|| format!("property entry missing '~': {entry}"))?;
        let key = String::from_utf8(hex_decode(key)?)
            .map_err(|_| "key is not valid UTF-8".to_string())?;
        properties.insert(key, decode_value(value)?);
    }
    Ok(properties)
}

pub fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

pub fn hex_decode(input: &str) -> Result<Vec<u8>, String> {
    if !input.len().is_multiple_of(2) {
        return Err(format!("hex string has odd length: {input}"));
    }
    let mut bytes = Vec::with_capacity(input.len() / 2);
    for chunk in input.as_bytes().chunks(2) {
        let high = hex_nibble(chunk[0])?;
        let low = hex_nibble(chunk[1])?;
        bytes.push((high << 4) | low);
    }
    Ok(bytes)
}

fn encode_query_row(row: &QueryRow) -> String {
    let mut keys = row.values().keys().collect::<Vec<_>>();
    keys.sort();
    keys.into_iter()
        .filter_map(|key| {
            row.values().get(key).map(|value| {
                format!(
                    "{}={}",
                    hex_encode(key.as_bytes()),
                    encode_query_value(value)
                )
            })
        })
        .collect::<Vec<_>>()
        .join(";")
}

fn decode_query_row(input: &str) -> Result<QueryRow, String> {
    let mut row = QueryRow::new();
    if input.is_empty() {
        return Ok(row);
    }
    for cell in input.split(';') {
        let (name, value) = cell
            .split_once('=')
            .ok_or_else(|| format!("row cell missing '=': {cell}"))?;
        let name = String::from_utf8(hex_decode(name)?)
            .map_err(|_| "row cell name is not valid UTF-8".to_string())?;
        row.insert(name, decode_query_value(value)?);
    }
    Ok(row)
}

fn encode_query_value(value: &QueryValue) -> String {
    match value {
        QueryValue::Scalar(value) => format!("V:{}", encode_value(value)),
        QueryValue::Node(node) => format!(
            "N:{}:{}:{}",
            node.id,
            encode_labels(&node.labels),
            encode_properties(&node.properties)
        ),
        QueryValue::BoundaryNode(node) => format!(
            "B:{}:{}:{}:{}:{}",
            node.id,
            node.owner_shard,
            node.version,
            encode_labels(&node.labels),
            encode_properties(&node.properties)
        ),
        QueryValue::Relationship(relationship) => format!(
            "R:{}:{}:{}:{}:{}",
            relationship.id,
            relationship.from,
            relationship.to,
            hex_encode(relationship.rel_type.as_bytes()),
            encode_properties(&relationship.properties)
        ),
    }
}

fn decode_query_value(input: &str) -> Result<QueryValue, String> {
    let (kind, payload) = input
        .split_once(':')
        .ok_or_else(|| format!("query value missing kind: {input}"))?;
    match kind {
        "V" => Ok(QueryValue::Scalar(decode_value(payload)?)),
        "N" => {
            let parts = payload.splitn(3, ':').collect::<Vec<_>>();
            if parts.len() != 3 {
                return Err("node query value requires id, labels, properties".to_string());
            }
            Ok(QueryValue::Node(Node::new(
                parse_u64(parts[0], "node id")?,
                decode_labels(parts[1])?,
                decode_properties(parts[2])?,
            )))
        }
        "B" => {
            let parts = payload.splitn(5, ':').collect::<Vec<_>>();
            if parts.len() != 5 {
                return Err(
                    "boundary node query value requires id, owner, version, labels, properties"
                        .to_string(),
                );
            }
            Ok(QueryValue::BoundaryNode(BoundaryNode::new(
                parse_u64(parts[0], "boundary node id")?,
                parse_u64(parts[1], "boundary owner shard")?,
                decode_labels(parts[3])?,
                decode_properties(parts[4])?,
                parse_u64(parts[2], "boundary version")?,
            )))
        }
        "R" => {
            let parts = payload.splitn(5, ':').collect::<Vec<_>>();
            if parts.len() != 5 {
                return Err(
                    "relationship query value requires id, from, to, type, properties".to_string(),
                );
            }
            let rel_type = String::from_utf8(hex_decode(parts[3])?)
                .map_err(|_| "relationship type is not valid UTF-8".to_string())?;
            Ok(QueryValue::Relationship(Relationship::new(
                parse_u64(parts[0], "relationship id")?,
                parse_u64(parts[1], "relationship from")?,
                parse_u64(parts[2], "relationship to")?,
                rel_type,
                decode_properties(parts[4])?,
            )))
        }
        _ => Err(format!("unknown query value kind: {kind}")),
    }
}

fn encode_value(value: &Value) -> String {
    match value {
        Value::Null => "n".to_string(),
        Value::Bool(value) => format!("b:{}", u8::from(*value)),
        Value::Int(value) => format!("i:{value}"),
        Value::Float(value) => format!("f:{}", value.to_bits()),
        Value::String(value) => format!("s:{}", hex_encode(value.as_bytes())),
        Value::Vector(values) => format!(
            "v:{}",
            values
                .iter()
                .map(|value| value.to_bits().to_string())
                .collect::<Vec<_>>()
                .join(",")
        ),
        Value::Map(values) => format!("m:{}", hex_encode(encode_properties(values).as_bytes())),
    }
}

fn decode_value(input: &str) -> Result<Value, String> {
    if input == "n" {
        return Ok(Value::Null);
    }
    let (kind, payload) = input
        .split_once(':')
        .ok_or_else(|| format!("typed value missing kind: {input}"))?;
    match kind {
        "b" => match payload {
            "0" => Ok(Value::Bool(false)),
            "1" => Ok(Value::Bool(true)),
            _ => Err(format!("invalid bool payload: {payload}")),
        },
        "i" => payload
            .parse::<i64>()
            .map(Value::Int)
            .map_err(|_| format!("invalid int payload: {payload}")),
        "f" => payload
            .parse::<u64>()
            .map(f64::from_bits)
            .map(Value::Float)
            .map_err(|_| format!("invalid float payload: {payload}")),
        "s" => String::from_utf8(hex_decode(payload)?)
            .map(Value::String)
            .map_err(|_| "string payload is not valid UTF-8".to_string()),
        "v" => {
            if payload.is_empty() {
                return Ok(Value::Vector(Vec::new()));
            }
            payload
                .split(',')
                .map(|item| {
                    item.parse::<u32>()
                        .map(f32::from_bits)
                        .map_err(|_| format!("invalid vector payload: {item}"))
                })
                .collect::<Result<Vec<_>, _>>()
                .map(Value::Vector)
        }
        "m" => String::from_utf8(hex_decode(payload)?)
            .map_err(|_| "map payload is not valid UTF-8".to_string())
            .and_then(|payload| decode_properties(&payload))
            .map(Value::Map),
        _ => Err(format!("unknown value kind: {kind}")),
    }
}

fn encode_command_value(value: &Value) -> String {
    match value {
        Value::Null => "n:".to_string(),
        Value::Bool(value) => format!("b:{value}"),
        Value::Int(value) => format!("i:{value}"),
        Value::Float(value) => format!("f:{value}"),
        Value::String(value) => format!("s:{value}"),
        Value::Vector(values) => format!(
            "v:{}",
            values
                .iter()
                .map(|value| value.to_string())
                .collect::<Vec<_>>()
                .join(",")
        ),
        Value::Map(values) => format!("m:{}", hex_encode(encode_properties(values).as_bytes())),
    }
}

fn parse_command_properties<'a>(
    parts: impl Iterator<Item = &'a str>,
) -> Result<QueryParams, String> {
    let mut properties = QueryParams::new();
    for part in parts {
        let (key, value) = part
            .split_once('=')
            .ok_or_else(|| format!("property must be key=value: {part}"))?;
        properties.insert(key.to_string(), parse_command_value(value)?);
    }
    Ok(properties)
}

fn parse_command_value(value: &str) -> Result<Value, String> {
    let (kind, raw) = value
        .split_once(':')
        .ok_or_else(|| format!("value must use a typed prefix like s:value or i:1: {value}"))?;
    match kind {
        "n" if raw.is_empty() => Ok(Value::Null),
        "n" => Err("null values must be encoded as n:".to_string()),
        "b" => raw
            .parse::<bool>()
            .map(Value::Bool)
            .map_err(|_| format!("invalid bool value: {raw}")),
        "i" => raw
            .parse::<i64>()
            .map(Value::Int)
            .map_err(|_| format!("invalid int value: {raw}")),
        "f" => raw
            .parse::<f64>()
            .map(Value::Float)
            .map_err(|_| format!("invalid float value: {raw}")),
        "s" => Ok(Value::String(raw.to_string())),
        "v" => {
            if raw.is_empty() {
                return Ok(Value::Vector(Vec::new()));
            }
            raw.split(',')
                .map(|value| {
                    value
                        .parse::<f32>()
                        .map_err(|_| format!("invalid vector value: {value}"))
                })
                .collect::<Result<Vec<_>, _>>()
                .map(Value::Vector)
        }
        "m" => String::from_utf8(hex_decode(raw)?)
            .map_err(|_| "map payload is not valid UTF-8".to_string())
            .and_then(|payload| decode_properties(&payload))
            .map(Value::Map),
        _ => Err(format!("unknown value type prefix: {kind}")),
    }
}

fn encode_labels(labels: &[String]) -> String {
    labels
        .iter()
        .map(|label| hex_encode(label.as_bytes()))
        .collect::<Vec<_>>()
        .join(",")
}

fn decode_labels(input: &str) -> Result<Vec<String>, String> {
    if input.is_empty() {
        return Ok(Vec::new());
    }
    input
        .split(',')
        .map(|label| {
            String::from_utf8(hex_decode(label)?)
                .map_err(|_| "label is not valid UTF-8".to_string())
        })
        .collect()
}

fn parse_bool_token(input: &str, name: &str) -> Result<bool, String> {
    input
        .parse::<bool>()
        .map_err(|_| format!("{name} must be true or false"))
}

fn parse_usize(input: Option<&str>, message: &str) -> Result<usize, String> {
    input
        .ok_or_else(|| message.to_string())?
        .parse::<usize>()
        .map_err(|_| format!("{message}: value must be an unsigned integer"))
}

fn parse_u64(input: &str, name: &str) -> Result<u64, String> {
    input
        .parse::<u64>()
        .map_err(|_| format!("{name} must be an unsigned integer"))
}

fn hex_nibble(byte: u8) -> Result<u8, String> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err(format!("invalid hex byte: {}", byte as char)),
    }
}

fn invalid_data(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frame_round_trip_preserves_header_and_payload() {
        let frame = NativeFrame {
            message_type: NativeMessageType::Query,
            flags: 7,
            request_id: 42,
            payload: b"MATCH (n) RETURN n".to_vec(),
        };
        let mut bytes = Vec::new();

        write_frame(&mut bytes, &frame).unwrap();
        let decoded = read_frame(&mut bytes.as_slice()).unwrap().unwrap();

        assert_eq!(decoded, frame);
    }

    #[test]
    fn native_frame_encoding_matches_golden_header() {
        let frame = NativeFrame {
            message_type: NativeMessageType::Command,
            flags: 0x0102,
            request_id: 0x0102_0304_0506_0708,
            payload: b"PING".to_vec(),
        };
        let mut bytes = Vec::new();

        write_frame(&mut bytes, &frame).unwrap();

        assert_eq!(
            bytes,
            vec![
                b'N', b'4', b'R', b'1', 1, 4, 1, 2, 0, 0, 0, 4, 1, 2, 3, 4, 5, 6, 7, 8, b'P', b'I',
                b'N', b'G',
            ]
        );
    }

    #[test]
    fn native_protocol_version_range_is_explicit() {
        assert_eq!(native_protocol_version(), 1);
        assert_eq!(native_protocol_version_range(), 1..=1);
    }

    #[test]
    fn native_frame_rejects_legacy_or_unknown_magic() {
        let bytes = vec![
            b'N', b'4', b'R', b'0', 1, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1,
        ];

        let err = read_frame(&mut bytes.as_slice()).unwrap_err();

        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("invalid native protocol magic"));
    }

    #[test]
    fn query_payload_round_trips_typed_params() {
        let mut params = QueryParams::new();
        params.insert("name".to_string(), Value::String("Alice".to_string()));
        params.insert("age".to_string(), Value::Int(42));
        params.insert("active".to_string(), Value::Bool(true));

        let payload = encode_query_payload("MATCH (n) RETURN n", &params);
        let (query, decoded) = parse_query_payload(&payload).unwrap();

        assert_eq!(query, "MATCH (n) RETURN n");
        assert_eq!(decoded, params);
    }

    #[test]
    fn query_rows_round_trip_scalars_nodes_and_relationships() {
        let mut row = QueryRow::new();
        row.insert(
            "n.name",
            QueryValue::Scalar(Value::String("Alice".to_string())),
        );
        row.insert(
            "n",
            QueryValue::Node(Node::new(
                7,
                vec!["Person".to_string()],
                [("name".to_string(), Value::String("Alice".to_string()))].into(),
            )),
        );
        row.insert(
            "r",
            QueryValue::Relationship(Relationship::new(
                3,
                7,
                8,
                "KNOWS".to_string(),
                [("since".to_string(), Value::Int(2026))].into(),
            )),
        );

        let encoded = encode_query_rows(&[row.clone()]);

        assert_eq!(decode_query_rows(&encoded).unwrap(), vec![row]);
    }
}
