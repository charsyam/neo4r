use super::*;

pub(super) fn assert_response(reader: &mut BufReader<TcpStream>, expected: &str) {
    assert_eq!(read_line(reader), expected);
}

pub(super) fn read_line(reader: &mut BufReader<TcpStream>) -> String {
    let mut line = String::new();
    reader.read_line(&mut line).unwrap();
    line
}

pub(super) fn assert_native_response(
    stream: &mut TcpStream,
    message_type: NativeMessageType,
    request_id: u64,
    expected_payload: &str,
) {
    let payload = read_native_payload(stream, message_type, request_id);
    assert_eq!(payload, expected_payload);
}

pub(super) fn read_native_payload(
    stream: &mut TcpStream,
    message_type: NativeMessageType,
    request_id: u64,
) -> String {
    let frame = read_frame(stream).unwrap().unwrap();
    let payload = String::from_utf8(frame.payload).unwrap();
    assert_eq!(frame.message_type, message_type, "{payload}");
    assert_eq!(frame.request_id, request_id, "{payload}");
    payload
}

pub(super) fn test_map_param(entries: &[(&str, Value)]) -> String {
    let mut entries = entries
        .iter()
        .map(|(key, value)| (key.to_string(), test_encoded_value(value)))
        .collect::<Vec<_>>();
    entries.sort_by(|left, right| left.0.cmp(&right.0));
    let encoded = entries
        .into_iter()
        .map(|(key, value)| format!("{}~{value}", test_hex_encode(key.as_bytes())))
        .collect::<Vec<_>>()
        .join(",");
    test_hex_encode(encoded.as_bytes())
}

pub(super) fn web_request(backend: TcpBackend, request: &str) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let server = thread::spawn(move || backend.serve_web_listener_once(listener).unwrap());
    let mut stream = TcpStream::connect(addr).unwrap();
    stream.write_all(request.as_bytes()).unwrap();
    stream.shutdown(std::net::Shutdown::Write).unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).unwrap();
    server.join().unwrap();
    response
}

pub(super) fn first_backup_payload_file(path: &Path) -> Option<PathBuf> {
    if path.is_file() {
        return (path.file_name()?.to_string_lossy() != BACKUP_MANIFEST_FILE).then(|| path.into());
    }
    let mut entries = fs::read_dir(path).ok()?.flatten().collect::<Vec<_>>();
    entries.sort_by_key(|entry| entry.path());
    for entry in entries {
        if let Some(payload) = first_backup_payload_file(&entry.path()) {
            return Some(payload);
        }
    }
    None
}

pub(super) fn test_encoded_value(value: &Value) -> String {
    match value {
        Value::Null => "n".to_string(),
        Value::Bool(value) => format!("b:{}", u8::from(*value)),
        Value::Int(value) => format!("i:{value}"),
        Value::Float(value) => format!("f:{}", value.to_bits()),
        Value::String(value) => format!("s:{}", test_hex_encode(value.as_bytes())),
        Value::Vector(values) => format!(
            "v:{}",
            values
                .iter()
                .map(|value| value.to_bits().to_string())
                .collect::<Vec<_>>()
                .join(",")
        ),
        Value::Map(values) => {
            let entries = values
                .iter()
                .map(|(key, value)| (key.as_str(), value.clone()))
                .collect::<Vec<_>>();
            format!("m:{}", test_map_param(&entries))
        }
    }
}

pub(super) fn test_hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

pub(super) fn temp_dir(prefix: &str) -> std::path::PathBuf {
    let mut path = std::env::temp_dir();
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    path.push(format!("{prefix}-{}-{nanos}", std::process::id()));
    let _ = fs::remove_dir_all(&path);
    path
}
