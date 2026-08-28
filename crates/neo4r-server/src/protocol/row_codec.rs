use super::*;

pub fn encode_query_rows(rows: &[QueryRow]) -> String {
    neo4r_protocol::encode_query_rows(rows)
}

pub fn decode_query_rows(input: &str) -> Result<Vec<QueryRow>, String> {
    neo4r_protocol::decode_query_rows(input)
}

pub(super) fn decode_value(input: &str) -> Result<Value, String> {
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

pub(super) fn decode_properties(input: &str) -> Result<Properties, String> {
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

pub(super) fn parse_u64_token(input: &str, name: &str) -> Result<u64, String> {
    input
        .parse::<u64>()
        .map_err(|_| format!("{name} must be an unsigned integer"))
}

pub(super) fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

pub(super) fn hex_decode(input: &str) -> Result<Vec<u8>, String> {
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

pub(super) fn hex_nibble(byte: u8) -> Result<u8, String> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err(format!("invalid hex byte: {}", byte as char)),
    }
}

pub(super) fn split_once_tab(line: &str) -> Option<(&str, &str)> {
    line.split_once('\t')
}

pub fn parse_query_payload(payload: &str) -> Result<(String, QueryParams), String> {
    neo4r_protocol::parse_query_payload(payload)
}

pub fn encode_query_batch_payload(writes: &[(String, QueryParams)]) -> String {
    neo4r_protocol::encode_query_batch_payload(writes)
}

pub fn decode_query_batch_payload(payload: &str) -> Result<Vec<(String, QueryParams)>, String> {
    neo4r_protocol::decode_query_batch_payload(payload)
}

pub(super) fn parse_labels(value: &str) -> Result<Vec<String>, String> {
    let labels = value
        .split(',')
        .filter(|label| !label.is_empty())
        .map(validate_token)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(labels)
}

pub(super) fn parse_properties<'a>(
    parts: impl Iterator<Item = &'a str>,
) -> Result<Properties, String> {
    let mut properties = Properties::new();
    for part in parts {
        let (key, value) = part
            .split_once('=')
            .ok_or_else(|| format!("property must be key=value: {part}"))?;
        properties.insert(validate_token(key)?, parse_value(value)?);
    }
    Ok(properties)
}

pub(super) fn parse_value(value: &str) -> Result<Value, String> {
    let (kind, raw) = value
        .split_once(':')
        .ok_or_else(|| format!("value must use a typed prefix like s:value or i:1: {value}"))?;
    match kind {
        "n" => {
            if raw.is_empty() {
                Ok(Value::Null)
            } else {
                Err("null values must be encoded as n:".to_string())
            }
        }
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
        "v" => parse_vector_value(raw),
        "m" => String::from_utf8(hex_decode(raw)?)
            .map_err(|_| "map payload is not valid UTF-8".to_string())
            .and_then(|payload| decode_properties(&payload))
            .map(Value::Map),
        _ => Err(format!("unknown value type prefix: {kind}")),
    }
}

pub(super) fn parse_vector_value(raw: &str) -> Result<Value, String> {
    if raw.is_empty() {
        return Err("vector value must contain at least one float".to_string());
    }
    raw.split(',')
        .map(|item| {
            item.parse::<f32>()
                .map_err(|_| format!("invalid vector element: {item}"))
        })
        .collect::<Result<Vec<_>, _>>()
        .map(Value::Vector)
}

pub(super) fn parse_u64(value: Option<&str>, missing: &str) -> Result<u64, String> {
    value
        .filter(|value| !value.is_empty())
        .ok_or_else(|| missing.to_string())?
        .parse()
        .map_err(|_| missing.to_string())
}

pub(super) fn parse_usize(value: Option<&str>, missing: &str) -> Result<usize, String> {
    value
        .filter(|value| !value.is_empty())
        .ok_or_else(|| missing.to_string())?
        .parse()
        .map_err(|_| missing.to_string())
}

pub(super) fn parse_optional_if_not_exists<'a>(
    value: Option<&'a str>,
    mut remaining: impl Iterator<Item = &'a str>,
    command: &str,
) -> Result<bool, String> {
    let Some(value) = value else {
        return Ok(false);
    };
    if value != "IF_NOT_EXISTS" {
        return Err(format!(
            "{command} optional final argument must be IF_NOT_EXISTS"
        ));
    }
    if remaining.next().is_some() {
        return Err(format!(
            "{command} IF_NOT_EXISTS must be the final argument"
        ));
    }
    Ok(true)
}

pub(super) fn parse_optional_if_exists<'a>(
    value: Option<&'a str>,
    mut remaining: impl Iterator<Item = &'a str>,
    command: &str,
) -> Result<bool, String> {
    let Some(value) = value else {
        return Ok(false);
    };
    if value != "IF_EXISTS" {
        return Err(format!(
            "{command} optional final argument must be IF_EXISTS"
        ));
    }
    if remaining.next().is_some() {
        return Err(format!("{command} IF_EXISTS must be the final argument"));
    }
    Ok(true)
}

pub(super) fn parse_single_id(value: &str, missing: &str) -> Result<u64, String> {
    if value.contains('\t') {
        return Err(format!("{missing}; got extra fields"));
    }
    parse_u64(Some(value), missing)
}

pub(super) fn parse_single_key(value: &str, missing: &str) -> Result<String, String> {
    if value.contains('\t') {
        return Err(format!("{missing}; got extra fields"));
    }
    parse_key(Some(value), missing)
}

pub(super) fn parse_key(value: Option<&str>, missing: &str) -> Result<String, String> {
    value
        .filter(|value| !value.is_empty())
        .ok_or_else(|| missing.to_string())
        .and_then(validate_token)
}

pub(super) fn parse_address(value: Option<&str>, missing: &str) -> Result<String, String> {
    value
        .filter(|value| !value.is_empty())
        .ok_or_else(|| missing.to_string())
        .and_then(validate_token)
}

pub(super) fn validate_token(value: &str) -> Result<String, String> {
    if value.is_empty() {
        return Err("empty token".to_string());
    }
    if value.contains(['\t', '\n', '\r']) {
        return Err(format!("token contains a control separator: {value:?}"));
    }
    Ok(value.to_string())
}

pub(super) fn escape_response(value: &str) -> String {
    value.replace('\\', "\\\\").replace('\t', "\\t")
}
