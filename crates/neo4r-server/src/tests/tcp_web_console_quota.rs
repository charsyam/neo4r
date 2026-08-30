use super::*;

#[test]
pub(super) fn web_query_enforces_tenant_result_row_quota() {
    let dir = temp_dir("neo4r-web-tenant-result-quota");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1)).unwrap();
    db.execute_cypher("CREATE (n:Person {name: \"Alice\"})")
        .unwrap();
    db.execute_cypher("CREATE (n:Person {name: \"Bob\"})")
        .unwrap();
    let backend = TcpBackend::new(db).with_tenant_quota(None, Some(1));
    let body = "{\"query\":\"MATCH (n:Person) RETURN n.name\",\"params\":{}}";
    let response = web_request(
        backend,
        &format!(
            "POST /api/query HTTP/1.1\r\nhost: localhost\r\ncontent-length: {}\r\n\r\n{}",
            body.len(),
            body
        ),
    );

    assert!(response.contains("HTTP/1.1 500"));
    assert!(response.contains("tenant result row quota exceeded"));
    let _ = fs::remove_dir_all(dir);
}

#[test]
pub(super) fn native_query_enforces_default_tenant_result_row_quota() {
    let dir = temp_dir("neo4r-native-tenant-result-quota");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1)).unwrap();
    db.execute_cypher("CREATE (n:Person {name: \"Alice\"})")
        .unwrap();
    db.execute_cypher("CREATE (n:Person {name: \"Bob\"})")
        .unwrap();
    let backend = TcpBackend::new(db).with_tenant_quota(None, Some(1));
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let server = thread::spawn(move || backend.serve_listener_once(listener).unwrap());
    let mut stream = TcpStream::connect(addr).unwrap();

    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Query,
            1,
            b"MATCH (n:Person) RETURN n.name".to_vec(),
        ),
    )
    .unwrap();
    let payload = read_native_payload(&mut stream, NativeMessageType::Error, 1);

    assert!(payload.contains("tenant result row quota exceeded"));
    stream.shutdown(std::net::Shutdown::Both).unwrap();
    server.join().unwrap();
    let _ = fs::remove_dir_all(dir);
}

#[test]
pub(super) fn native_query_holds_tenant_concurrency_quota_until_cursor_is_exhausted() {
    let dir = temp_dir("neo4r-native-tenant-cursor-quota");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1)).unwrap();
    db.execute_cypher("CREATE (n:Person {name: \"Alice\"})")
        .unwrap();
    db.execute_cypher("CREATE (n:Person {name: \"Bob\"})")
        .unwrap();
    let backend = TcpBackend::with_config(
        db,
        TcpBackendConfig {
            worker_count: 1,
            queue_capacity: 4,
            default_page_size: 1,
            read_preference: QueryReadPreference::Primary,
            catch_up_connect_timeout: Duration::from_secs(1),
        },
    )
    .with_tenant_quota(Some(1), None);
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let server = thread::spawn(move || backend.serve_listener_once(listener).unwrap());
    let mut stream = TcpStream::connect(addr).unwrap();

    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Query,
            1,
            b"MATCH (n:Person) RETURN n.name".to_vec(),
        ),
    )
    .unwrap();
    let start = read_native_payload(&mut stream, NativeMessageType::Response, 1);
    let parts = start.split('\t').collect::<Vec<_>>();
    assert_eq!(parts[0..3], ["OK", "RESULT_START", "1"]);
    assert_eq!(parts[3], "2");
    assert_eq!(parts[4], "1");
    assert_eq!(parts[5], "true");

    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Query,
            2,
            b"MATCH (n:Person) RETURN n.name".to_vec(),
        ),
    )
    .unwrap();
    let blocked = read_native_payload(&mut stream, NativeMessageType::Error, 2);
    assert!(blocked.contains("tenant quota exceeded for database"));

    write_frame(
        &mut stream,
        &NativeFrame::new(NativeMessageType::Fetch, 3, b"1\t1".to_vec()),
    )
    .unwrap();
    let page = read_native_payload(&mut stream, NativeMessageType::Response, 3);
    let parts = page.split('\t').collect::<Vec<_>>();
    assert_eq!(parts[0..3], ["OK", "RESULT_PAGE", "1"]);
    assert_eq!(parts[3], "1");
    assert_eq!(parts[4], "false");

    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Query,
            4,
            b"MATCH (n:Person) RETURN n.name".to_vec(),
        ),
    )
    .unwrap();
    let restarted = read_native_payload(&mut stream, NativeMessageType::Response, 4);
    let parts = restarted.split('\t').collect::<Vec<_>>();
    assert_eq!(parts[0..3], ["OK", "RESULT_START", "2"]);
    assert_eq!(parts[4], "1");
    assert_eq!(parts[5], "true");

    write_frame(
        &mut stream,
        &NativeFrame::new(NativeMessageType::Quit, 5, Vec::new()),
    )
    .unwrap();
    assert_native_response(&mut stream, NativeMessageType::Response, 5, "OK\tBYE");
    server.join().unwrap();
    let _ = fs::remove_dir_all(dir);
}
