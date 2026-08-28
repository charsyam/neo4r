#![allow(unused_imports)]
use super::*;

#[test]
pub(super) fn pending_request_store_tracks_queued_cancellation_by_session() {
    let store = PendingRequestStore::default();

    store.register(10, 1).unwrap();
    assert!(store.cancel(10, 1).unwrap());
    assert!(!store.cancel(11, 1).unwrap());
    assert!(store.take_cancelled(10, 1).unwrap());
    assert!(!store.cancel(10, 1).unwrap());

    store.register(10, 2).unwrap();
    store.register(11, 2).unwrap();
    store.close_session(10).unwrap();
    assert!(!store.cancel(10, 2).unwrap());
    assert!(store.cancel(11, 2).unwrap());
}

#[test]
pub(super) fn native_worker_pool_reports_full_queue_without_blocking_session() {
    let (job_tx, _job_rx) = mpsc::sync_channel::<NativeJob>(1);
    let pending_requests = PendingRequestStore::default();
    let pool = NativeWorkerPool {
        jobs: Arc::new(Mutex::new(Some(job_tx))),
        joins: Arc::new(Mutex::new(Vec::new())),
        pending_requests: pending_requests.clone(),
    };
    let (response_tx, response_rx) = mpsc::channel::<NativeFrame>();

    pool.submit(
        10,
        NativeFrame::new(NativeMessageType::Ping, 1, Vec::new()),
        response_tx.clone(),
    )
    .unwrap();
    pool.submit(
        10,
        NativeFrame::new(NativeMessageType::Ping, 2, Vec::new()),
        response_tx,
    )
    .unwrap();

    let response = response_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert_eq!(response.message_type, NativeMessageType::Error);
    assert_eq!(response.request_id, 2);
    assert_eq!(
        response.payload_text().unwrap(),
        "ERR\tnative worker queue full"
    );
    assert!(pending_requests.cancel(10, 1).unwrap());
    assert!(!pending_requests.cancel(10, 2).unwrap());
}

#[test]
pub(super) fn native_cancel_reports_missed_request_when_not_pending() {
    let dir = temp_dir("neo4r-native-cancel-missed");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1)).unwrap();
    let backend = TcpBackend::with_config(
        db,
        TcpBackendConfig {
            worker_count: 1,
            queue_capacity: 4,
            default_page_size: 2,
            read_preference: QueryReadPreference::Primary,
            catch_up_connect_timeout: Duration::from_secs(1),
        },
    );
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let server = thread::spawn(move || backend.serve_listener_once(listener).unwrap());

    let mut stream = TcpStream::connect(addr).unwrap();
    write_frame(
        &mut stream,
        &NativeFrame::new(NativeMessageType::Cancel, 1, b"999".to_vec()),
    )
    .unwrap();
    assert_native_response(
        &mut stream,
        NativeMessageType::Response,
        1,
        "OK\tCANCEL_MISSED\t999",
    );

    write_frame(
        &mut stream,
        &NativeFrame::new(NativeMessageType::Quit, 2, Vec::new()),
    )
    .unwrap();
    assert_native_response(&mut stream, NativeMessageType::Response, 2, "OK\tBYE");

    server.join().unwrap();
    let _ = fs::remove_dir_all(dir);
}

#[test]
pub(super) fn native_query_uses_cursor_and_fetch_pages() {
    let dir = temp_dir("neo4r-native-cursor");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1)).unwrap();
    db.create_node(vec!["Person".to_string()], Default::default())
        .unwrap();
    db.create_node(vec!["Person".to_string()], Default::default())
        .unwrap();
    db.create_node(vec!["Person".to_string()], Default::default())
        .unwrap();
    let backend = TcpBackend::with_config(
        db,
        TcpBackendConfig {
            worker_count: 1,
            queue_capacity: 4,
            default_page_size: 2,
            read_preference: QueryReadPreference::Primary,
            catch_up_connect_timeout: Duration::from_secs(1),
        },
    );
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let server = thread::spawn(move || backend.serve_listener_once(listener).unwrap());

    let mut stream = TcpStream::connect(addr).unwrap();
    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Query,
            1,
            b"MATCH (n:Person) RETURN n".to_vec(),
        ),
    )
    .unwrap();
    let start = read_native_payload(&mut stream, NativeMessageType::Response, 1);
    let parts = start.split('\t').collect::<Vec<_>>();
    assert_eq!(parts[0..3], ["OK", "RESULT_START", "1"]);
    assert_eq!(parts[3], "3");
    assert_eq!(parts[4], "2");
    assert_eq!(parts[5], "true");

    write_frame(
        &mut stream,
        &NativeFrame::new(NativeMessageType::Fetch, 2, b"1\t2".to_vec()),
    )
    .unwrap();
    let page = read_native_payload(&mut stream, NativeMessageType::Response, 2);
    let parts = page.split('\t').collect::<Vec<_>>();
    assert_eq!(parts[0..3], ["OK", "RESULT_PAGE", "1"]);
    assert_eq!(parts[3], "1");
    assert_eq!(parts[4], "false");

    write_frame(
        &mut stream,
        &NativeFrame::new(NativeMessageType::CloseCursor, 3, b"1".to_vec()),
    )
    .unwrap();
    assert_native_response(
        &mut stream,
        NativeMessageType::Response,
        3,
        "OK\tCURSOR_CLOSED\t1",
    );

    write_frame(
        &mut stream,
        &NativeFrame::new(NativeMessageType::CloseCursor, 4, b"1".to_vec()),
    )
    .unwrap();
    assert_native_response(
        &mut stream,
        NativeMessageType::Response,
        4,
        "OK\tCURSOR_CLOSED\t1",
    );

    write_frame(
        &mut stream,
        &NativeFrame::new(NativeMessageType::Quit, 5, Vec::new()),
    )
    .unwrap();
    assert_native_response(&mut stream, NativeMessageType::Response, 5, "OK\tBYE");

    server.join().unwrap();
    let _ = fs::remove_dir_all(dir);
}

#[test]
pub(super) fn native_query_can_execute_cypher_write() {
    let dir = temp_dir("neo4r-native-cypher-write");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1)).unwrap();
    let backend = TcpBackend::with_config(
        db,
        TcpBackendConfig {
            worker_count: 1,
            queue_capacity: 4,
            default_page_size: 2,
            read_preference: QueryReadPreference::Primary,
            catch_up_connect_timeout: Duration::from_secs(1),
        },
    );
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let server = thread::spawn(move || backend.serve_listener_once(listener).unwrap());

    let mut stream = TcpStream::connect(addr).unwrap();
    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Query,
            1,
            b"CREATE (n:Person {name: $name}) RETURN n.name\tname=s:Alice".to_vec(),
        ),
    )
    .unwrap();
    let start = read_native_payload(&mut stream, NativeMessageType::Response, 1);
    let parts = start.split('\t').collect::<Vec<_>>();
    assert_eq!(parts[0..3], ["OK", "RESULT_START", "1"]);
    assert_eq!(parts[3], "1");
    assert_eq!(parts[4], "1");
    assert_eq!(parts[5], "false");
    let rows = decode_query_rows(parts[6]).unwrap();
    assert_eq!(
        rows[0].get("n.name"),
        Some(&neo4r_query::QueryValue::Scalar(Value::String(
            "Alice".to_string()
        )))
    );

    write_frame(
        &mut stream,
        &NativeFrame::new(NativeMessageType::Quit, 2, Vec::new()),
    )
    .unwrap();
    assert_native_response(&mut stream, NativeMessageType::Response, 2, "OK\tBYE");

    server.join().unwrap();
    let _ = fs::remove_dir_all(dir);
}

#[test]
pub(super) fn native_query_forwards_index_cypher_to_metadata_primary() {
    let local_dir = temp_dir("neo4r-native-index-cypher-local");
    let remote_dir = temp_dir("neo4r-native-index-cypher-remote");
    let routing_table = ShardRoutingTable {
        version: 11,
        placements: vec![ShardPlacement::new(
            0,
            vec![ShardReplica::primary(2), ShardReplica::replica(1)],
        )],
    };
    let local_db = Neo4rDatabaseHandle::open(
        DatabaseConfig::new(&local_dir, 1, 1)
            .with_server_id(1)
            .with_routing_table(routing_table.clone()),
    )
    .unwrap();
    let remote_db = Neo4rDatabaseHandle::open(
        DatabaseConfig::new(&remote_dir, 1, 1)
            .with_server_id(2)
            .with_routing_table(routing_table),
    )
    .unwrap();

    let remote_backend = TcpBackend::new(remote_db.clone());
    let remote_listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let remote_addr = remote_listener.local_addr().unwrap();
    let remote_server =
        thread::spawn(move || remote_backend.serve_listener_once(remote_listener).unwrap());
    let local_backend = TcpBackend::new(local_db.clone());
    local_backend
        .register_query_peer(2, remote_addr.to_string())
        .unwrap();
    let local_listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let local_addr = local_listener.local_addr().unwrap();
    let local_server =
        thread::spawn(move || local_backend.serve_listener_once(local_listener).unwrap());

    let mut stream = TcpStream::connect(local_addr).unwrap();
    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Query,
            1,
            b"CREATE INDEX person_name FOR (n:Person) ON (n.name)".to_vec(),
        ),
    )
    .unwrap();
    let start = read_native_payload(&mut stream, NativeMessageType::Response, 1);
    let parts = start.split('\t').collect::<Vec<_>>();
    assert_eq!(parts[0..2], ["OK", "RESULT_START"]);
    assert_eq!(parts[3], "0");

    write_frame(
        &mut stream,
        &NativeFrame::new(NativeMessageType::Quit, 2, Vec::new()),
    )
    .unwrap();
    assert_native_response(&mut stream, NativeMessageType::Response, 2, "OK\tBYE");

    local_server.join().unwrap();
    remote_server.join().unwrap();
    let indexes = remote_db.list_indexes().unwrap();
    assert_eq!(indexes.len(), 1);
    assert_eq!(indexes[0].name, "person_name");
    assert!(local_db.list_indexes().unwrap().is_empty());

    drop(local_db);
    drop(remote_db);
    let _ = fs::remove_dir_all(local_dir);
    let _ = fs::remove_dir_all(remote_dir);
}

#[test]
pub(super) fn native_query_can_execute_constraint_cypher_write() {
    let dir = temp_dir("neo4r-native-constraint-cypher-write");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1)).unwrap();
    let backend = TcpBackend::with_config(
        db.clone(),
        TcpBackendConfig {
            worker_count: 1,
            queue_capacity: 4,
            default_page_size: 2,
            read_preference: QueryReadPreference::Primary,
            catch_up_connect_timeout: Duration::from_secs(1),
        },
    );
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let server = thread::spawn(move || backend.serve_listener_once(listener).unwrap());

    let mut stream = TcpStream::connect(addr).unwrap();
    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Query,
            1,
            b"CREATE CONSTRAINT person_email_unique FOR (n:Person) REQUIRE n.email IS UNIQUE"
                .to_vec(),
        ),
    )
    .unwrap();
    let start = read_native_payload(&mut stream, NativeMessageType::Response, 1);
    let parts = start.split('\t').collect::<Vec<_>>();
    assert_eq!(parts[0..3], ["OK", "RESULT_START", "1"]);
    assert_eq!(parts[3], "0");
    assert_eq!(parts[4], "0");
    assert_eq!(parts[5], "false");

    write_frame(
        &mut stream,
        &NativeFrame::new(NativeMessageType::Quit, 2, Vec::new()),
    )
    .unwrap();
    assert_native_response(&mut stream, NativeMessageType::Response, 2, "OK\tBYE");

    server.join().unwrap();
    let indexes = db.list_indexes().unwrap();
    assert_eq!(indexes.len(), 1);
    assert_eq!(indexes[0].name, "person_email_unique");
    assert_eq!(indexes[0].kind, IndexKind::UniqueNodeProperty);

    drop(db);
    let _ = fs::remove_dir_all(dir);
}

#[test]
pub(super) fn native_query_can_execute_relationship_cypher_write() {
    let dir = temp_dir("neo4r-native-cypher-relationship-write");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1)).unwrap();
    db.create_node(
        vec!["Person".to_string()],
        [("name".to_string(), Value::String("Alice".to_string()))]
            .into_iter()
            .collect(),
    )
    .unwrap();
    db.create_node(
        vec!["Person".to_string()],
        [("name".to_string(), Value::String("Bob".to_string()))]
            .into_iter()
            .collect(),
    )
    .unwrap();
    let backend = TcpBackend::with_config(
        db,
        TcpBackendConfig {
            worker_count: 1,
            queue_capacity: 4,
            default_page_size: 2,
            read_preference: QueryReadPreference::Primary,
            catch_up_connect_timeout: Duration::from_secs(1),
        },
    );
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let server = thread::spawn(move || backend.serve_listener_once(listener).unwrap());

    let mut stream = TcpStream::connect(addr).unwrap();
    write_frame(
            &mut stream,
            &NativeFrame::new(
                NativeMessageType::Query,
                1,
                b"MATCH (a:Person {name: $from}) MATCH (b:Person {name: $to}) CREATE (a)-[r:KNOWS {since: $since}]->(b) RETURN r\tfrom=s:Alice\tto=s:Bob\tsince=i:2026".to_vec(),
            ),
        )
        .unwrap();
    let start = read_native_payload(&mut stream, NativeMessageType::Response, 1);
    let parts = start.split('\t').collect::<Vec<_>>();
    assert_eq!(parts[0..3], ["OK", "RESULT_START", "1"]);
    assert_eq!(parts[3], "1");

    write_frame(
        &mut stream,
        &NativeFrame::new(NativeMessageType::Quit, 2, Vec::new()),
    )
    .unwrap();
    assert_native_response(&mut stream, NativeMessageType::Response, 2, "OK\tBYE");

    server.join().unwrap();
    let _ = fs::remove_dir_all(dir);
}

#[test]
pub(super) fn native_query_can_show_indexes() {
    let dir = temp_dir("neo4r-native-show-indexes");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1)).unwrap();
    db.execute_cypher("CREATE INDEX person_name FOR (n:Person) ON (n.name)")
        .unwrap();
    db.execute_cypher(
        "CREATE CONSTRAINT person_email_unique FOR (n:Person) REQUIRE n.email IS UNIQUE",
    )
    .unwrap();
    db.execute_cypher(
        "CREATE VECTOR INDEX doc_embedding ON :Document(embedding) DIMENSIONS 2 METRIC cosine",
    )
    .unwrap();
    let backend = TcpBackend::with_config(
        db,
        TcpBackendConfig {
            worker_count: 1,
            queue_capacity: 4,
            default_page_size: 4,
            read_preference: QueryReadPreference::Primary,
            catch_up_connect_timeout: Duration::from_secs(1),
        },
    );
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let server = thread::spawn(move || backend.serve_listener_once(listener).unwrap());

    let mut stream = TcpStream::connect(addr).unwrap();
    write_frame(
        &mut stream,
        &NativeFrame::new(NativeMessageType::Query, 1, b"SHOW INDEXES".to_vec()),
    )
    .unwrap();
    let start = read_native_payload(&mut stream, NativeMessageType::Response, 1);
    let parts = start.split('\t').collect::<Vec<_>>();
    assert_eq!(parts[0..3], ["OK", "RESULT_START", "1"]);
    assert_eq!(parts[3], "3");
    assert_eq!(parts[4], "3");
    let rows = decode_query_rows(parts[6]).unwrap();
    assert_eq!(
        rows[0].get("name"),
        Some(&neo4r_query::QueryValue::Scalar(Value::String(
            "person_name".to_string()
        )))
    );

    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Query,
            2,
            b"SHOW INDEX person_name".to_vec(),
        ),
    )
    .unwrap();
    let start = read_native_payload(&mut stream, NativeMessageType::Response, 2);
    let parts = start.split('\t').collect::<Vec<_>>();
    assert_eq!(parts[0..3], ["OK", "RESULT_START", "2"]);
    assert_eq!(parts[3], "1");
    let rows = decode_query_rows(parts[6]).unwrap();
    assert_eq!(
        rows[0].get("name"),
        Some(&neo4r_query::QueryValue::Scalar(Value::String(
            "person_name".to_string()
        )))
    );

    write_frame(
        &mut stream,
        &NativeFrame::new(NativeMessageType::Query, 3, b"SHOW VECTOR INDEXES".to_vec()),
    )
    .unwrap();
    let start = read_native_payload(&mut stream, NativeMessageType::Response, 3);
    let parts = start.split('\t').collect::<Vec<_>>();
    assert_eq!(parts[0..3], ["OK", "RESULT_START", "3"]);
    assert_eq!(parts[3], "1");
    let rows = decode_query_rows(parts[6]).unwrap();
    assert_eq!(
        rows[0].get("name"),
        Some(&neo4r_query::QueryValue::Scalar(Value::String(
            "doc_embedding".to_string()
        )))
    );
    assert_eq!(
        rows[0].get("type"),
        Some(&neo4r_query::QueryValue::Scalar(Value::String(
            "vector".to_string()
        )))
    );

    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Query,
            4,
            b"SHOW VECTOR INDEX doc_embedding".to_vec(),
        ),
    )
    .unwrap();
    let start = read_native_payload(&mut stream, NativeMessageType::Response, 4);
    let parts = start.split('\t').collect::<Vec<_>>();
    assert_eq!(parts[0..3], ["OK", "RESULT_START", "4"]);
    assert_eq!(parts[3], "1");
    let rows = decode_query_rows(parts[6]).unwrap();
    assert_eq!(
        rows[0].get("name"),
        Some(&neo4r_query::QueryValue::Scalar(Value::String(
            "doc_embedding".to_string()
        )))
    );

    write_frame(
        &mut stream,
        &NativeFrame::new(NativeMessageType::Query, 5, b"SHOW CONSTRAINTS".to_vec()),
    )
    .unwrap();
    let start = read_native_payload(&mut stream, NativeMessageType::Response, 5);
    let parts = start.split('\t').collect::<Vec<_>>();
    assert_eq!(parts[0..3], ["OK", "RESULT_START", "5"]);
    assert_eq!(parts[3], "1");
    let rows = decode_query_rows(parts[6]).unwrap();
    assert_eq!(
        rows[0].get("name"),
        Some(&neo4r_query::QueryValue::Scalar(Value::String(
            "person_email_unique".to_string()
        )))
    );

    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Query,
            6,
            b"SHOW CONSTRAINT person_email_unique".to_vec(),
        ),
    )
    .unwrap();
    let start = read_native_payload(&mut stream, NativeMessageType::Response, 6);
    let parts = start.split('\t').collect::<Vec<_>>();
    assert_eq!(parts[0..3], ["OK", "RESULT_START", "6"]);
    assert_eq!(parts[3], "1");
    let rows = decode_query_rows(parts[6]).unwrap();
    assert_eq!(
        rows[0].get("name"),
        Some(&neo4r_query::QueryValue::Scalar(Value::String(
            "person_email_unique".to_string()
        )))
    );

    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Query,
            7,
            b"DROP CONSTRAINT person_email_unique".to_vec(),
        ),
    )
    .unwrap();
    let start = read_native_payload(&mut stream, NativeMessageType::Response, 7);
    let parts = start.split('\t').collect::<Vec<_>>();
    assert_eq!(parts[0..3], ["OK", "RESULT_START", "7"]);
    assert_eq!(parts[3], "0");

    write_frame(
        &mut stream,
        &NativeFrame::new(NativeMessageType::Query, 8, b"SHOW CONSTRAINTS".to_vec()),
    )
    .unwrap();
    let start = read_native_payload(&mut stream, NativeMessageType::Response, 8);
    let parts = start.split('\t').collect::<Vec<_>>();
    assert_eq!(parts[0..3], ["OK", "RESULT_START", "8"]);
    assert_eq!(parts[3], "0");

    write_frame(
        &mut stream,
        &NativeFrame::new(NativeMessageType::Quit, 9, Vec::new()),
    )
    .unwrap();
    assert_native_response(&mut stream, NativeMessageType::Response, 9, "OK\tBYE");

    server.join().unwrap();
    let _ = fs::remove_dir_all(dir);
}

#[test]
pub(super) fn native_command_can_install_routing_table_and_read_cluster_status() {
    let dir = temp_dir("neo4r-native-install-routing");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 2, 2).with_server_id(10)).unwrap();
    let backend = TcpBackend::with_config(
        db,
        TcpBackendConfig {
            worker_count: 1,
            queue_capacity: 8,
            default_page_size: 10,
            read_preference: QueryReadPreference::Primary,
            catch_up_connect_timeout: Duration::from_secs(1),
        },
    );
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let server = thread::spawn(move || backend.serve_listener_once(listener).unwrap());

    let mut stream = TcpStream::connect(addr).unwrap();
    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Command,
            1,
            b"INSTALL_ROUTING_TABLE\t2\t0:10:11\t1:11:10".to_vec(),
        ),
    )
    .unwrap();
    assert_native_response(&mut stream, NativeMessageType::Response, 1, "OK");

    write_frame(
        &mut stream,
        &NativeFrame::new(NativeMessageType::Command, 2, b"CLUSTER_STATUS".to_vec()),
    )
    .unwrap();
    let status = read_native_payload(&mut stream, NativeMessageType::Response, 2);
    assert!(status.starts_with("OK\tCLUSTER_STATUS\t"));
    assert!(status.contains("routing_version=2"));
    assert!(status.contains("shard=1 primary=11 replicas=10 local=true local_primary=false"));

    write_frame(
        &mut stream,
        &NativeFrame::new(NativeMessageType::Quit, 3, Vec::new()),
    )
    .unwrap();
    assert_native_response(&mut stream, NativeMessageType::Response, 3, "OK\tBYE");

    server.join().unwrap();
    let _ = fs::remove_dir_all(dir);
}

#[test]
pub(super) fn native_command_reports_structured_management_statuses() {
    let dir = temp_dir("neo4r-native-management-status");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1).with_server_id(1)).unwrap();
    let backend = TcpBackend::with_config(
        db,
        TcpBackendConfig {
            worker_count: 1,
            queue_capacity: 8,
            default_page_size: 10,
            read_preference: QueryReadPreference::Primary,
            catch_up_connect_timeout: Duration::from_secs(1),
        },
    );
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let server = thread::spawn(move || backend.serve_listener_once(listener).unwrap());

    let mut stream = TcpStream::connect(addr).unwrap();
    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Command,
            1,
            b"CREATE_NODE\tPerson\tname=s:alice".to_vec(),
        ),
    )
    .unwrap();
    assert_native_response(&mut stream, NativeMessageType::Response, 1, "OK\tNODE\t0");

    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Command,
            2,
            b"PROFILE\tMATCH (n:Person) RETURN n".to_vec(),
        ),
    )
    .unwrap();
    let profile = read_native_payload(&mut stream, NativeMessageType::Response, 2);
    assert!(profile.starts_with("OK\tPROFILE\t"));
    assert!(profile.contains("operators="));

    write_frame(
        &mut stream,
        &NativeFrame::new(NativeMessageType::Command, 3, b"STORAGE_STATUS".to_vec()),
    )
    .unwrap();
    let storage = read_native_payload(&mut stream, NativeMessageType::Response, 3);
    assert!(storage.starts_with("OK\tSTORAGE_STATUS\t"));
    assert!(storage.contains("wal_pruned_until="));

    write_frame(
        &mut stream,
        &NativeFrame::new(NativeMessageType::Command, 4, b"METADATA_LOG".to_vec()),
    )
    .unwrap();
    let metadata = read_native_payload(&mut stream, NativeMessageType::Response, 4);
    assert!(metadata.starts_with("OK\tMETADATA_LOG\t"));

    write_frame(
        &mut stream,
        &NativeFrame::new(NativeMessageType::Quit, 5, Vec::new()),
    )
    .unwrap();
    assert_native_response(&mut stream, NativeMessageType::Response, 5, "OK\tBYE");

    server.join().unwrap();
    let _ = fs::remove_dir_all(dir);
}

#[test]
pub(super) fn parses_transaction_query_plan_command() {
    let command = parse_transaction_command("TX_QUERY_PLAN\t7\tMATCH (n:Person) RETURN n").unwrap();

    let Some(TransactionCommand::QueryPlan {
        tx_id,
        query,
        params,
    }) = command
    else {
        panic!("expected TX_QUERY_PLAN command");
    };
    assert_eq!(tx_id, 7);
    assert_eq!(query, "MATCH (n:Person) RETURN n");
    assert!(params.is_empty());
}

#[test]
pub(super) fn parses_prepared_query_commands() {
    let Some(PreparedQueryCommand::Prepare { query }) =
        parse_prepared_query_command("PREPARE_QUERY\tMATCH (n:Person) RETURN n").unwrap()
    else {
        panic!("expected PREPARE_QUERY command");
    };
    assert_eq!(query, "MATCH (n:Person) RETURN n");

    let Some(PreparedQueryCommand::Execute {
        prepared_id,
        params,
    }) = parse_prepared_query_command("EXECUTE_PREPARED\t9\tname=s:Alice\tactive=b:true").unwrap()
    else {
        panic!("expected EXECUTE_PREPARED command");
    };
    assert_eq!(prepared_id, 9);
    assert_eq!(
        params.get("name"),
        Some(&Value::String("Alice".to_string()))
    );
    assert_eq!(params.get("active"), Some(&Value::Bool(true)));

    let Some(PreparedQueryCommand::QueryPlan {
        prepared_id,
        params,
    }) = parse_prepared_query_command("PREPARED_QUERY_PLAN\t9\tname=s:Alice").unwrap()
    else {
        panic!("expected PREPARED_QUERY_PLAN command");
    };
    assert_eq!(prepared_id, 9);
    assert_eq!(
        params.get("name"),
        Some(&Value::String("Alice".to_string()))
    );

    let Some(PreparedQueryCommand::Route {
        prepared_id,
        params,
    }) = parse_prepared_query_command("PREPARED_QUERY_ROUTE\t9\tname=s:Alice").unwrap()
    else {
        panic!("expected PREPARED_QUERY_ROUTE command");
    };
    assert_eq!(prepared_id, 9);
    assert_eq!(
        params.get("name"),
        Some(&Value::String("Alice".to_string()))
    );

    let Some(PreparedQueryCommand::Describe { prepared_id }) =
        parse_prepared_query_command("DESCRIBE_PREPARED\t9").unwrap()
    else {
        panic!("expected DESCRIBE_PREPARED command");
    };
    assert_eq!(prepared_id, 9);

    let Some(TransactionCommand::ExecutePrepared {
        tx_id,
        prepared_id,
        params,
    }) = parse_transaction_command("TX_EXECUTE_PREPARED\t7\t9\tname=s:Bob").unwrap()
    else {
        panic!("expected TX_EXECUTE_PREPARED command");
    };
    assert_eq!(tx_id, 7);
    assert_eq!(prepared_id, 9);
    assert_eq!(params.get("name"), Some(&Value::String("Bob".to_string())));

    let Some(TransactionCommand::PreparedQueryPlan {
        tx_id,
        prepared_id,
        params,
    }) = parse_transaction_command("TX_PREPARED_QUERY_PLAN\t7\t9\tname=s:Carol").unwrap()
    else {
        panic!("expected TX_PREPARED_QUERY_PLAN command");
    };
    assert_eq!(tx_id, 7);
    assert_eq!(prepared_id, 9);
    assert_eq!(
        params.get("name"),
        Some(&Value::String("Carol".to_string()))
    );

    let Some(TransactionCommand::PreparedQueryRoute {
        tx_id,
        prepared_id,
        params,
    }) = parse_transaction_command("TX_PREPARED_QUERY_ROUTE\t7\t9\tname=s:Dana").unwrap()
    else {
        panic!("expected TX_PREPARED_QUERY_ROUTE command");
    };
    assert_eq!(tx_id, 7);
    assert_eq!(prepared_id, 9);
    assert_eq!(params.get("name"), Some(&Value::String("Dana".to_string())));
}

#[test]
pub(super) fn describes_query_parameters_without_string_literals() {
    assert_eq!(
        describe_query_parameters(
            r#"MATCH (n:Person {name: $name}) WHERE n.note = "$ignored" AND n.age >= $age SET n += $props RETURN $name"#
        ),
        vec!["age".to_string(), "name".to_string(), "props".to_string()]
    );
}

#[test]
pub(super) fn describes_prepared_query_kind_and_parameters() {
    assert_eq!(
        format_prepared_query_describe(
            7,
            "MATCH (n:Person {name: $name}) RETURN n",
            "READ_LOCAL".to_string(),
            vec!["name".to_string()],
        ),
        "OK\tPREPARED_QUERY_DESC\t7\tREAD\tREAD_LOCAL\t1\tname"
    );
    assert_eq!(
        format_prepared_query_describe(
            8,
            "MATCH (n:Person) SET n.status = $status",
            "WRITE_TARGET_DYNAMIC".to_string(),
            vec!["status".to_string()],
        ),
        "OK\tPREPARED_QUERY_DESC\t8\tWRITE\tWRITE_TARGET_DYNAMIC\t1\tstatus"
    );
    assert_eq!(
        format_prepared_query_describe(
            9,
            "CREATE CONSTRAINT person_email_unique FOR (n:Person) REQUIRE n.email IS UNIQUE",
            "SCHEMA".to_string(),
            Vec::new(),
        ),
        "OK\tPREPARED_QUERY_DESC\t9\tSCHEMA\tSCHEMA\t0\t"
    );
}
