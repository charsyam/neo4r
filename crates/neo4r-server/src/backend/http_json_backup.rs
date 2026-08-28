struct HttpRequest {
    method: String,
    path: String,
    query: HashMap<String, String>,
    headers: HashMap<String, String>,
    body: String,
}

impl HttpRequest {
    fn query_value(&self, key: &str) -> Option<String> {
        self.query.get(key).cloned()
    }

    fn header(&self, key: &str) -> Option<&str> {
        self.headers
            .get(&key.to_ascii_lowercase())
            .map(String::as_str)
    }
}

struct HttpResponse {
    status: u16,
    reason: &'static str,
    content_type: &'static str,
    body: String,
}

impl HttpResponse {
    fn html(body: &str) -> Self {
        Self {
            status: 200,
            reason: "OK",
            content_type: "text/html; charset=utf-8",
            body: body.to_string(),
        }
    }

    fn json(body: String) -> Self {
        Self::json_status(200, body)
    }

    fn json_status(status: u16, body: String) -> Self {
        let reason = match status {
            200 => "OK",
            400 => "Bad Request",
            401 => "Unauthorized",
            403 => "Forbidden",
            404 => "Not Found",
            500 => "Internal Server Error",
            _ => "OK",
        };
        Self {
            status,
            reason,
            content_type: "application/json; charset=utf-8",
            body,
        }
    }
}

fn read_http_request(stream: TcpStream) -> io::Result<HttpRequest> {
    let mut reader = BufReader::new(stream);
    let mut request_line = String::new();
    if reader.read_line(&mut request_line)? == 0 {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "empty HTTP request",
        ));
    }
    let mut request_parts = request_line.split_whitespace();
    let method = request_parts.next().unwrap_or("").to_string();
    let target = request_parts.next().unwrap_or("/");
    let (path, query) = parse_http_target(target);

    let mut content_length = 0usize;
    let mut headers = HashMap::new();
    let mut line = String::new();
    loop {
        line.clear();
        if reader.read_line(&mut line)? == 0 {
            break;
        }
        let header = line.trim_end_matches(['\r', '\n']);
        if header.is_empty() {
            break;
        }
        if let Some((name, value)) = header.split_once(':') {
            headers.insert(name.trim().to_ascii_lowercase(), value.trim().to_string());
            if name.eq_ignore_ascii_case("content-length") {
                content_length = value.trim().parse::<usize>().unwrap_or(0);
            }
        }
    }

    let mut body = vec![0_u8; content_length];
    if content_length > 0 {
        reader.read_exact(&mut body)?;
    }

    Ok(HttpRequest {
        method,
        path,
        query,
        headers,
        body: String::from_utf8_lossy(&body).into_owned(),
    })
}

fn write_http_response(mut stream: TcpStream, response: HttpResponse) -> io::Result<()> {
    let body = response.body.as_bytes();
    write!(
        stream,
        "HTTP/1.1 {} {}\r\ncontent-type: {}\r\ncontent-length: {}\r\ncache-control: no-store\r\nconnection: close\r\n\r\n",
        response.status,
        response.reason,
        response.content_type,
        body.len()
    )?;
    stream.write_all(body)?;
    stream.flush()
}

fn parse_http_target(target: &str) -> (String, HashMap<String, String>) {
    let (path, query) = target.split_once('?').unwrap_or((target, ""));
    let mut values = HashMap::new();
    for pair in query.split('&').filter(|pair| !pair.is_empty()) {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        values.insert(percent_decode(key), percent_decode(value));
    }
    (path.to_string(), values)
}

fn percent_decode(input: &str) -> String {
    let mut output = Vec::with_capacity(input.len());
    let mut bytes = input.as_bytes().iter().copied();
    while let Some(byte) = bytes.next() {
        match byte {
            b'+' => output.push(b' '),
            b'%' => {
                let high = bytes.next();
                let low = bytes.next();
                if let (Some(high), Some(low)) = (high, low) {
                    if let (Some(high), Some(low)) = (hex_value(high), hex_value(low)) {
                        output.push((high << 4) | low);
                    }
                }
            }
            byte => output.push(byte),
        }
    }
    String::from_utf8_lossy(&output).into_owned()
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn management_response_json(response: &BackendResponse) -> String {
    format!(
        "{{\"response\":\"{}\"}}",
        json_escape(&format_response(response))
    )
}

fn storage_maintenance_json(result: &neo4r_db::StorageMaintenanceResult) -> String {
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

fn json_error(message: impl AsRef<str>) -> String {
    format!("{{\"error\":\"{}\"}}", json_escape(message.as_ref()))
}

fn query_examples_json() -> String {
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

fn query_row_json(row: &QueryRow) -> String {
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

fn query_value_json(value: &QueryValue) -> String {
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

fn properties_json(properties: &Properties) -> String {
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

fn value_json(value: &Value) -> String {
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

fn string_array_json(values: &[String]) -> String {
    format!(
        "[{}]",
        values
            .iter()
            .map(|value| format!("\"{}\"", json_escape(value)))
            .collect::<Vec<_>>()
            .join(",")
    )
}

fn json_escape(input: &str) -> String {
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

fn extract_json_string_field(input: &str, field: &str) -> Result<String, String> {
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

fn extract_optional_json_string_field(input: &str, field: &str) -> Result<Option<String>, String> {
    let Some(rest) = find_json_field_value(input, field)? else {
        return Ok(None);
    };
    parse_json_string(rest.trim_start()).map(|(value, _)| Some(value))
}

fn extract_optional_json_bool_field(input: &str, field: &str) -> Result<bool, String> {
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

fn database_from_use_clause(query: &str) -> Result<Option<String>, String> {
    split_database_use_clause(query).map(|(database, _)| database)
}

fn strip_database_use_clause(query: &str) -> Result<String, String> {
    split_database_use_clause(query).map(|(_, query)| query)
}

fn split_database_use_clause(query: &str) -> Result<(Option<String>, String), String> {
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

fn strip_keyword<'a>(input: &'a str, keyword: &str) -> Option<&'a str> {
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

fn parse_json_params_field(input: &str) -> Result<QueryParams, String> {
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

fn find_json_field_value<'a>(input: &'a str, field: &str) -> Result<Option<&'a str>, String> {
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

fn parse_json_object(input: &str) -> Result<Vec<(String, Value)>, String> {
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

fn parse_json_value(input: &str) -> Result<(Value, &str), String> {
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

fn parse_json_number(input: &str) -> Result<(Value, &str), String> {
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

fn parse_json_number_array(input: &str) -> Result<(Vec<f32>, &str), String> {
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

fn parse_json_string(input: &str) -> Result<(String, &str), String> {
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

fn copy_dir_all(source: &Path, target: &Path) -> io::Result<()> {
    fs::create_dir_all(target)?;
    for entry in fs::read_dir(source)? {
        let entry = entry?;
        let source_path = entry.path();
        let target_path = target.join(entry.file_name());
        if entry.file_type()?.is_dir() {
            copy_dir_all(&source_path, &target_path)?;
        } else {
            if let Some(parent) = target_path.parent() {
                fs::create_dir_all(parent)?;
            }
            fs::copy(&source_path, &target_path)?;
        }
    }
    Ok(())
}

#[derive(Default)]
struct BackupManifestStats {
    file_count: u64,
    total_bytes: u64,
    checksum: u64,
}

fn collect_backup_manifest_stats(path: &Path) -> io::Result<BackupManifestStats> {
    let mut stats = BackupManifestStats::default();
    collect_backup_manifest_stats_inner(path, &mut stats)?;
    Ok(stats)
}

fn collect_backup_manifest_stats_inner(
    path: &Path,
    stats: &mut BackupManifestStats,
) -> io::Result<()> {
    let mut entries = fs::read_dir(path)?.collect::<Result<Vec<_>, _>>()?;
    entries.sort_by_key(|entry| entry.path());
    for entry in entries {
        let metadata = entry.metadata()?;
        if metadata.is_dir() {
            collect_backup_manifest_stats_inner(&entry.path(), stats)?;
        } else if entry.file_name().to_string_lossy() == BACKUP_MANIFEST_FILE {
            continue;
        } else {
            let path = entry.path();
            stats.file_count += 1;
            stats.total_bytes = stats.total_bytes.saturating_add(metadata.len());
            stats.checksum = checksum_file(&path, stats.checksum)?;
        }
    }
    Ok(())
}

fn verify_backup_manifest(path: &Path, stats: &BackupManifestStats) -> io::Result<()> {
    let manifest = fs::read_to_string(path.join(BACKUP_MANIFEST_FILE))?;
    let fields = manifest
        .lines()
        .filter_map(|line| line.split_once('='))
        .collect::<HashMap<_, _>>();
    if fields.get("neo4r_backup_manifest_version") != Some(&"1") {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "unsupported backup manifest version",
        ));
    }
    verify_manifest_u64(&fields, "file_count", stats.file_count)?;
    verify_manifest_u64(&fields, "total_bytes", stats.total_bytes)?;
    if fields.contains_key("checksum") {
        verify_manifest_u64(&fields, "checksum", stats.checksum)?;
    }
    Ok(())
}

fn verify_manifest_u64(fields: &HashMap<&str, &str>, key: &str, actual: u64) -> io::Result<()> {
    let expected = fields
        .get(key)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, format!("missing {key}")))?;
    let expected = expected.parse::<u64>().map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid backup manifest {key}"),
        )
    })?;
    if expected != actual {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("backup manifest {key} mismatch: expected {expected}, actual {actual}"),
        ));
    }
    Ok(())
}

fn checksum_file(path: &Path, seed: u64) -> io::Result<u64> {
    let mut file = File::open(path)?;
    let mut buffer = [0_u8; 8192];
    let mut hash = seed ^ 0xcbf29ce484222325;
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            return Ok(hash);
        }
        for byte in &buffer[..read] {
            hash = hash.wrapping_mul(0x100000001b3).wrapping_add(*byte as u64);
        }
    }
}
