use super::*;

pub(crate) fn management_response_json(response: &BackendResponse) -> String {
    format!(
        "{{\"response\":\"{}\"}}",
        json_escape(&format_response(response))
    )
}

pub(crate) fn storage_maintenance_json(result: &neo4r_db::StorageMaintenanceResult) -> String {
    format!(
        "{{\"action\":\"{}\",\"files_touched\":{},\"bytes_observed\":{},\"pruned_until\":[{}],\"safety_manifest\":\"{}\"}}",
        json_escape(&result.action),
        result.files_touched,
        result.bytes_observed,
        result
            .pruned_until
            .iter()
            .map(u64::to_string)
            .collect::<Vec<_>>()
            .join(","),
        json_escape(&result.safety_manifest)
    )
}

pub(crate) fn json_error(message: impl AsRef<str>) -> String {
    format!("{{\"error\":\"{}\"}}", json_escape(message.as_ref()))
}

pub(crate) fn query_examples_json() -> String {
    let examples = [
        ("match_all", "MATCH (n) RETURN n"),
        (
            "create_person",
            "CREATE (n:Person {name: $name, role: $role, age: $age, status: \"active\"}) RETURN n",
        ),
        (
            "create_with_relationship",
            "CREATE (n:Person {name: $name, role: $role, age: $age, status: \"active\"})\nWITH n\nMATCH (c:Company {name: $company})\nCREATE (n)-[r:WORKS_AT {since: $since}]->(c)\nRETURN n, r",
        ),
        (
            "profile_work",
            "MATCH (a:Person)-[r:WORKS_AT]->(b:Company) RETURN a.name, b.name, r.since",
        ),
    ];
    format!(
        "{{\"examples\":[{}]}}",
        examples
            .iter()
            .map(|(name, query)| format!(
                "{{\"name\":\"{}\",\"query\":\"{}\"}}",
                json_escape(name),
                json_escape(query)
            ))
            .collect::<Vec<_>>()
            .join(",")
    )
}

pub(crate) fn query_row_json(row: &QueryRow) -> String {
    let mut keys = row.values().keys().collect::<Vec<_>>();
    keys.sort();
    let fields = keys
        .into_iter()
        .filter_map(|key| {
            row.get(key)
                .map(|value| format!("\"{}\":{}", json_escape(key), query_value_json(value)))
        })
        .collect::<Vec<_>>()
        .join(",");
    format!("{{{fields}}}")
}

pub(crate) fn query_value_json(value: &QueryValue) -> String {
    match value {
        QueryValue::Scalar(value) => value_json(value),
        QueryValue::Node(node) => format!(
            "{{\"kind\":\"node\",\"id\":{},\"labels\":{},\"properties\":{}}}",
            node.id,
            string_array_json(&node.labels),
            properties_json(&node.properties)
        ),
        QueryValue::BoundaryNode(node) => format!(
            "{{\"kind\":\"boundary_node\",\"id\":{},\"owner_shard\":{},\"version\":{},\"labels\":{},\"properties\":{}}}",
            node.id,
            node.owner_shard,
            node.version,
            string_array_json(&node.labels),
            properties_json(&node.properties)
        ),
        QueryValue::Relationship(relationship) => format!(
            "{{\"kind\":\"relationship\",\"id\":{},\"from\":{},\"to\":{},\"type\":\"{}\",\"properties\":{}}}",
            relationship.id,
            relationship.from,
            relationship.to,
            json_escape(&relationship.rel_type),
            properties_json(&relationship.properties)
        ),
    }
}

pub(crate) fn properties_json(properties: &Properties) -> String {
    let mut keys = properties.keys().collect::<Vec<_>>();
    keys.sort();
    let fields = keys
        .into_iter()
        .filter_map(|key| {
            properties
                .get(key)
                .map(|value| format!("\"{}\":{}", json_escape(key), value_json(value)))
        })
        .collect::<Vec<_>>()
        .join(",");
    format!("{{{fields}}}")
}

pub(crate) fn value_json(value: &Value) -> String {
    match value {
        Value::Null => "null".to_string(),
        Value::Bool(value) => value.to_string(),
        Value::Int(value) => value.to_string(),
        Value::Float(value) => {
            if value.is_finite() {
                value.to_string()
            } else {
                "null".to_string()
            }
        }
        Value::String(value) => format!("\"{}\"", json_escape(value)),
        Value::Vector(values) => format!(
            "[{}]",
            values
                .iter()
                .map(|value| {
                    if value.is_finite() {
                        value.to_string()
                    } else {
                        "null".to_string()
                    }
                })
                .collect::<Vec<_>>()
                .join(",")
        ),
        Value::Map(values) => properties_json(values),
    }
}

pub(crate) fn string_array_json(values: &[String]) -> String {
    format!(
        "[{}]",
        values
            .iter()
            .map(|value| format!("\"{}\"", json_escape(value)))
            .collect::<Vec<_>>()
            .join(",")
    )
}

pub(crate) fn json_escape(input: &str) -> String {
    let mut output = String::new();
    for ch in input.chars() {
        match ch {
            '"' => output.push_str("\\\""),
            '\\' => output.push_str("\\\\"),
            '\n' => output.push_str("\\n"),
            '\r' => output.push_str("\\r"),
            '\t' => output.push_str("\\t"),
            ch if ch.is_control() => output.push_str(&format!("\\u{:04x}", ch as u32)),
            ch => output.push(ch),
        }
    }
    output
}

pub(crate) fn extract_json_string_field(input: &str, field: &str) -> Result<String, String> {
    let needle = format!("\"{field}\"");
    let start = input
        .find(&needle)
        .ok_or_else(|| format!("missing JSON string field: {field}"))?;
    let rest = &input[start + needle.len()..];
    let colon = rest
        .find(':')
        .ok_or_else(|| format!("missing ':' after JSON field: {field}"))?;
    let rest = rest[colon + 1..].trim_start();
    parse_json_string(rest).map(|(value, _)| value)
}

pub(crate) fn extract_optional_json_string_field(input: &str, field: &str) -> Result<Option<String>, String> {
    let Some(rest) = find_json_field_value(input, field)? else {
        return Ok(None);
    };
    parse_json_string(rest.trim_start()).map(|(value, _)| Some(value))
}

pub(crate) fn extract_optional_json_bool_field(input: &str, field: &str) -> Result<bool, String> {
    let Some(rest) = find_json_field_value(input, field)? else {
        return Ok(false);
    };
    let rest = rest.trim_start();
    if rest.starts_with("true") {
        Ok(true)
    } else if rest.starts_with("false") {
        Ok(false)
    } else {
        Err(format!("JSON bool field {field} must be true or false"))
    }
}

pub(crate) fn database_from_use_clause(query: &str) -> Result<Option<String>, String> {
    split_database_use_clause(query).map(|(database, _)| database)
}

pub(crate) fn strip_database_use_clause(query: &str) -> Result<String, String> {
    split_database_use_clause(query).map(|(_, query)| query)
}

pub(crate) fn split_database_use_clause(query: &str) -> Result<(Option<String>, String), String> {
    let trimmed = query.trim_start();
    let Some(after_use) = strip_keyword(trimmed, "USE") else {
        return Ok((None, query.to_string()));
    };
    let after_use = after_use.trim_start();
    if after_use.is_empty() {
        return Err("USE requires a database name".to_string());
    }

    let (database, rest) = if let Some(after_tick) = after_use.strip_prefix('`') {
        let Some(end) = after_tick.find('`') else {
            return Err("unterminated database name in USE clause".to_string());
        };
        (
            after_tick[..end].to_string(),
            after_tick[end + '`'.len_utf8()..].trim_start(),
        )
    } else {
        let end = after_use
            .char_indices()
            .find(|(_, ch)| ch.is_whitespace() || *ch == ';')
            .map(|(index, _)| index)
            .unwrap_or(after_use.len());
        if end == 0 {
            return Err("USE requires a database name".to_string());
        }
        (after_use[..end].to_string(), after_use[end..].trim_start())
    };
    validate_database_name(&database)?;

    let rest = rest.strip_prefix(';').unwrap_or(rest).trim_start();
    if rest.is_empty() {
        return Err("USE requires a following query".to_string());
    }
    Ok((Some(database), rest.to_string()))
}

pub(crate) fn strip_keyword<'a>(input: &'a str, keyword: &str) -> Option<&'a str> {
    let prefix = input.get(..keyword.len())?;
    if !prefix.eq_ignore_ascii_case(keyword) {
        return None;
    }
    let rest = &input[keyword.len()..];
    match rest.chars().next() {
        Some(ch) if ch.is_whitespace() => Some(rest),
        None => Some(rest),
        _ => None,
    }
}

pub(crate) fn parse_json_params_field(input: &str) -> Result<QueryParams, String> {
    let Some(params_start) = find_json_field_value(input, "params")? else {
        return Ok(QueryParams::new());
    };
    let params_start = params_start.trim_start();
    if params_start.starts_with("null") {
        return Ok(QueryParams::new());
    }
    let entries = parse_json_object(params_start)?;
    let mut params = QueryParams::new();
    for (key, value) in entries {
        params.insert(key, value);
    }
    Ok(params)
}

pub(crate) fn find_json_field_value<'a>(input: &'a str, field: &str) -> Result<Option<&'a str>, String> {
    let needle = format!("\"{field}\"");
    let Some(start) = input.find(&needle) else {
        return Ok(None);
    };
    let rest = &input[start + needle.len()..];
    let colon = rest
        .find(':')
        .ok_or_else(|| format!("missing ':' after JSON field: {field}"))?;
    Ok(Some(&rest[colon + 1..]))
}

pub(crate) fn parse_json_object(input: &str) -> Result<Vec<(String, Value)>, String> {
    let mut rest = input.trim_start();
    if !rest.starts_with('{') {
        return Err("params must be a JSON object".to_string());
    }
    rest = &rest[1..];
    let mut entries = Vec::new();
    loop {
        rest = rest.trim_start();
        if rest.starts_with('}') {
            return Ok(entries);
        }
        let (key, after_key) = parse_json_string(rest)?;
        rest = after_key.trim_start();
        if !rest.starts_with(':') {
            return Err("expected ':' after params key".to_string());
        }
        let (value, after_value) = parse_json_value(&rest[1..])?;
        entries.push((key, value));
        rest = after_value.trim_start();
        if rest.starts_with(',') {
            rest = &rest[1..];
            continue;
        }
        if rest.starts_with('}') {
            return Ok(entries);
        }
        return Err("expected ',' or '}' in params object".to_string());
    }
}

pub(crate) fn parse_json_value(input: &str) -> Result<(Value, &str), String> {
    let input = input.trim_start();
    if input.starts_with('"') {
        let (value, rest) = parse_json_string(input)?;
        return Ok((Value::String(value), rest));
    }
    if let Some(rest) = input.strip_prefix("true") {
        return Ok((Value::Bool(true), rest));
    }
    if let Some(rest) = input.strip_prefix("false") {
        return Ok((Value::Bool(false), rest));
    }
    if let Some(rest) = input.strip_prefix("null") {
        return Ok((Value::Null, rest));
    }
    if input.starts_with('[') {
        let (values, rest) = parse_json_number_array(input)?;
        return Ok((Value::Vector(values), rest));
    }
    parse_json_number(input)
}

pub(crate) fn parse_json_number(input: &str) -> Result<(Value, &str), String> {
    let input = input.trim_start();
    let end = input
        .char_indices()
        .find(|(_, ch)| !matches!(ch, '-' | '+' | '.' | '0'..='9' | 'e' | 'E'))
        .map(|(index, _)| index)
        .unwrap_or(input.len());
    if end == 0 {
        return Err("expected JSON scalar value".to_string());
    }
    let number = &input[..end];
    let rest = &input[end..];
    if number.contains('.') || number.contains('e') || number.contains('E') {
        number
            .parse::<f64>()
            .map(|value| (Value::Float(value), rest))
            .map_err(|_| "invalid JSON float".to_string())
    } else {
        number
            .parse::<i64>()
            .map(|value| (Value::Int(value), rest))
            .map_err(|_| "invalid JSON integer".to_string())
    }
}

pub(crate) fn parse_json_number_array(input: &str) -> Result<(Vec<f32>, &str), String> {
    let mut rest = input.trim_start();
    if !rest.starts_with('[') {
        return Err("expected JSON array".to_string());
    }
    rest = &rest[1..];
    let mut values = Vec::new();
    loop {
        rest = rest.trim_start();
        if rest.starts_with(']') {
            return Ok((values, &rest[1..]));
        }
        let (value, after_value) = parse_json_number(rest)?;
        match value {
            Value::Int(value) => values.push(value as f32),
            Value::Float(value) => values.push(value as f32),
            _ => return Err("JSON vector values must be numeric".to_string()),
        }
        rest = after_value.trim_start();
        if rest.starts_with(',') {
            rest = &rest[1..];
            continue;
        }
        if rest.starts_with(']') {
            return Ok((values, &rest[1..]));
        }
        return Err("expected ',' or ']' in JSON vector".to_string());
    }
}

pub(crate) fn parse_json_string(input: &str) -> Result<(String, &str), String> {
    let mut chars = input.char_indices();
    if chars.next().map(|(_, ch)| ch) != Some('"') {
        return Err("expected JSON string".to_string());
    }
    let mut output = String::new();
    let mut escaped = false;
    for (index, ch) in chars {
        if escaped {
            match ch {
                '"' => output.push('"'),
                '\\' => output.push('\\'),
                '/' => output.push('/'),
                'b' => output.push('\u{0008}'),
                'f' => output.push('\u{000c}'),
                'n' => output.push('\n'),
                'r' => output.push('\r'),
                't' => output.push('\t'),
                other => return Err(format!("unsupported JSON escape: \\{other}")),
            }
            escaped = false;
        } else if ch == '\\' {
            escaped = true;
        } else if ch == '"' {
            return Ok((output, &input[index + ch.len_utf8()..]));
        } else {
            output.push(ch);
        }
    }
    Err("unterminated JSON string".to_string())
}
