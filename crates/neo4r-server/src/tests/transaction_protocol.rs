#![allow(unused_imports)]
use super::*;

#[test]
pub(super) fn prepared_query_routing_hint_reports_read_and_write_routes() {
    let dir = temp_dir("neo4r-prepared-query-routing-hint");
    let routing_table = ShardRoutingTable {
        version: 7,
        placements: vec![
            ShardPlacement::new(0, vec![ShardReplica::primary(1)]),
            ShardPlacement::new(1, vec![ShardReplica::primary(2)]),
        ],
    };
    let db = Neo4rDatabaseHandle::open(
        DatabaseConfig::new(&dir, 2, 1)
            .with_server_id(1)
            .with_routing_table(routing_table),
    )
    .unwrap();

    assert_eq!(
        prepared_query_routing_hint(&db, "MATCH (n:Person) RETURN n").unwrap(),
        "READ_REMOTE:1"
    );
    assert_eq!(
        prepared_query_routing_hint(&db, "CREATE (n:Person {name: $name})").unwrap(),
        "WRITE_SHARD_BY_PARAM"
    );
    let mut params = neo4r_query::QueryParams::new();
    params.insert("name".to_string(), Value::String("Alice".to_string()));
    let routed =
        prepared_query_routing_hint_with_params(&db, "CREATE (n:Person {name: $name})", &params)
            .unwrap();
    assert!(routed == "WRITE_SHARD:0" || routed == "WRITE_SHARD:1");
    assert_eq!(
        prepared_query_routing_hint(
            &db,
            "MATCH (n:Person) WHERE n.name = $name SET n.status = $status"
        )
        .unwrap(),
        "WRITE_TARGET_DYNAMIC"
    );
    assert_eq!(
        prepared_query_routing_hint(
            &db,
            "CREATE CONSTRAINT person_email_unique FOR (n:Person) REQUIRE n.email IS UNIQUE"
        )
        .unwrap(),
        "SCHEMA"
    );

    drop(db);
    let _ = fs::remove_dir_all(dir);
}

#[test]
pub(super) fn parses_transaction_status_commands() {
    assert!(matches!(
        parse_transaction_command("TX_STATUS\t7").unwrap(),
        Some(TransactionCommand::Status { tx_id: 7 })
    ));
    assert!(matches!(
        parse_transaction_command("KILL_TX\t7").unwrap(),
        Some(TransactionCommand::Kill { tx_id: 7 })
    ));
    assert!(matches!(
        parse_transaction_command("TX_PREPARED_STATUS\t11").unwrap(),
        Some(TransactionCommand::PreparedStatus { prepared_id: 11 })
    ));
    assert!(matches!(
        parse_transaction_command("LIST_PREPARED_TX").unwrap(),
        Some(TransactionCommand::ListPrepared)
    ));
    assert!(matches!(
        parse_transaction_command("LIST_ALL_TX").unwrap(),
        Some(TransactionCommand::ListAll)
    ));
}

#[test]
pub(super) fn transaction_store_lists_all_session_transactions() {
    let store = TransactionStore::default();
    let first_tx = store.insert(
        7,
        NativeTransaction::ReadWrite {
            isolation: ReadIsolation::ReadCommitted,
            ownership_epoch: 1,
            staged_writes: Vec::new(),
            conflict_keys: BTreeSet::new(),
        },
    );
    let second_tx = store.insert(
        9,
        NativeTransaction::ReadWrite {
            isolation: ReadIsolation::Snapshot,
            ownership_epoch: 1,
            staged_writes: Vec::new(),
            conflict_keys: BTreeSet::new(),
        },
    );
    store
        .stage_write(
            7,
            first_tx,
            "CREATE (n:Person {name: \"Alice\"}) RETURN n".to_string(),
            neo4r_query::QueryParams::new(),
        )
        .unwrap();

    assert_eq!(
        format_tx_list_all(store.list_all().unwrap()),
        format!(
            "OK\tTX_LIST_ALL\t2\t7:{first_tx}:READ_WRITE:READ_COMMITTED:1:1,9:{second_tx}:READ_WRITE:SNAPSHOT:0:1"
        )
    );
    assert_eq!(
        format_tx_list(store.list(7).unwrap()),
        format!("OK\tTX_LIST\t1\t{first_tx}:READ_WRITE:READ_COMMITTED:1:1")
    );
}

#[test]
pub(super) fn transaction_store_rejects_duplicate_staged_write_conflicts() {
    let store = TransactionStore::default();
    let tx_id = store.insert(
        7,
        NativeTransaction::ReadWrite {
            isolation: ReadIsolation::ReadCommitted,
            ownership_epoch: 1,
            staged_writes: Vec::new(),
            conflict_keys: BTreeSet::new(),
        },
    );
    let query = "MATCH (n:Person {name: \"Alice\"}) SET n.age = 31 RETURN n".to_string();
    store
        .stage_write(7, tx_id, query.clone(), neo4r_query::QueryParams::new())
        .unwrap();
    let err = store
        .stage_write(7, tx_id, query, neo4r_query::QueryParams::new())
        .unwrap_err();

    assert!(err.contains("write conflict"));
}

#[test]
pub(super) fn native_read_write_transaction_reports_status() {
    let dir = temp_dir("neo4r-native-tx-status");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1)).unwrap();
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
            b"BEGIN_TX\tREAD_WRITE READ_COMMITTED".to_vec(),
        ),
    )
    .unwrap();
    let begin = read_native_payload(&mut stream, NativeMessageType::Response, 1);
    let tx_id = begin.split('\t').collect::<Vec<_>>()[2]
        .parse::<u64>()
        .unwrap();

    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Command,
            2,
            format!("TX_STATUS\t{tx_id}").into_bytes(),
        ),
    )
    .unwrap();
    assert_native_response(
        &mut stream,
        NativeMessageType::Response,
        2,
        &format!("OK\tTX_STATUS\t{tx_id}\tREAD_WRITE\tREAD_COMMITTED\t0\townership_epoch=1"),
    );

    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Command,
            3,
            format!("TX_QUERY\t{tx_id}\tCREATE (n:Person {{name: \"Alice\"}}) RETURN n")
                .into_bytes(),
        ),
    )
    .unwrap();
    assert_native_response(
        &mut stream,
        NativeMessageType::Response,
        3,
        &format!("OK\tTX_STAGED\t{tx_id}\t1"),
    );

    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Command,
            4,
            format!("TX_STATUS\t{tx_id}").into_bytes(),
        ),
    )
    .unwrap();
    assert_native_response(
        &mut stream,
        NativeMessageType::Response,
        4,
        &format!("OK\tTX_STATUS\t{tx_id}\tREAD_WRITE\tREAD_COMMITTED\t1\townership_epoch=1"),
    );

    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Command,
            5,
            format!("COMMIT_TX\t{tx_id}").into_bytes(),
        ),
    )
    .unwrap();
    assert_native_response(
        &mut stream,
        NativeMessageType::Response,
        5,
        &format!("OK\tTX_COMMIT\t{tx_id}\t1"),
    );

    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Command,
            6,
            format!("TX_STATUS\t{tx_id}").into_bytes(),
        ),
    )
    .unwrap();
    let missing = read_native_payload(&mut stream, NativeMessageType::Error, 6);
    assert!(missing.contains("unknown transaction"));

    write_frame(
        &mut stream,
        &NativeFrame::new(NativeMessageType::Quit, 7, Vec::new()),
    )
    .unwrap();
    assert_native_response(&mut stream, NativeMessageType::Response, 7, "OK\tBYE");

    server.join().unwrap();
    let _ = fs::remove_dir_all(dir);
}

#[test]
pub(super) fn native_transaction_query_reads_stable_snapshot() {
    let dir = temp_dir("neo4r-native-tx");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1)).unwrap();
    db.create_node(
        vec!["Person".to_string()],
        [(
            "name".to_string(),
            neo4r_core::Value::String("Alice".to_string()),
        )]
        .into_iter()
        .collect(),
    )
    .unwrap();
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
            b"BEGIN_TX\tSNAPSHOT".to_vec(),
        ),
    )
    .unwrap();
    let begin = read_native_payload(&mut stream, NativeMessageType::Response, 1);
    let begin_parts = begin.split('\t').collect::<Vec<_>>();
    assert_eq!(begin_parts[0..2], ["OK", "TX_BEGIN"]);
    assert_eq!(begin_parts[3], "READ_ONLY");
    assert_eq!(begin_parts[4], "SNAPSHOT");
    let tx_id = begin_parts[2].parse::<u64>().unwrap();

    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Command,
            2,
            b"QUERY\tCREATE (n:Person {name: $name}) RETURN n\tname=s:Bob".to_vec(),
        ),
    )
    .unwrap();
    let write_response = read_native_payload(&mut stream, NativeMessageType::Response, 2);
    assert!(write_response.starts_with("OK\tROWS\t1"));

    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Command,
            3,
            format!("TX_QUERY\t{tx_id}\tMATCH (n:Person) RETURN n").into_bytes(),
        ),
    )
    .unwrap();
    let query = read_native_payload(&mut stream, NativeMessageType::Response, 3);
    let query_parts = query.split('\t').collect::<Vec<_>>();
    assert_eq!(query_parts[0..3], ["OK", "RESULT_START", "1"]);
    assert_eq!(query_parts[3], "1");
    assert_eq!(query_parts[4], "1");
    assert_eq!(query_parts[5], "false");

    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Command,
            4,
            format!("COMMIT_TX\t{tx_id}").into_bytes(),
        ),
    )
    .unwrap();
    assert_native_response(
        &mut stream,
        NativeMessageType::Response,
        4,
        &format!("OK\tTX_COMMIT\t{tx_id}"),
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
pub(super) fn native_read_committed_transaction_reads_latest_statement_snapshot() {
    let dir = temp_dir("neo4r-native-tx-read-committed");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1)).unwrap();
    db.create_node(
        vec!["Person".to_string()],
        [(
            "name".to_string(),
            neo4r_core::Value::String("Alice".to_string()),
        )]
        .into_iter()
        .collect(),
    )
    .unwrap();
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
            b"BEGIN_TX\tREAD_COMMITTED".to_vec(),
        ),
    )
    .unwrap();
    let begin = read_native_payload(&mut stream, NativeMessageType::Response, 1);
    let begin_parts = begin.split('\t').collect::<Vec<_>>();
    assert_eq!(begin_parts[0..2], ["OK", "TX_BEGIN"]);
    assert_eq!(begin_parts[3], "READ_ONLY");
    assert_eq!(begin_parts[4], "READ_COMMITTED");
    let tx_id = begin_parts[2].parse::<u64>().unwrap();

    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Command,
            2,
            format!("TX_QUERY\t{tx_id}\tMATCH (n:Person) RETURN n").into_bytes(),
        ),
    )
    .unwrap();
    let first = read_native_payload(&mut stream, NativeMessageType::Response, 2);
    let first_parts = first.split('\t').collect::<Vec<_>>();
    assert_eq!(first_parts[0..3], ["OK", "RESULT_START", "1"]);
    assert_eq!(first_parts[3], "1");

    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Command,
            3,
            b"QUERY\tCREATE (n:Person {name: $name}) RETURN n\tname=s:Bob".to_vec(),
        ),
    )
    .unwrap();
    let write_response = read_native_payload(&mut stream, NativeMessageType::Response, 3);
    assert!(write_response.starts_with("OK\tROWS\t1"));

    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Command,
            4,
            format!("TX_QUERY\t{tx_id}\tMATCH (n:Person) RETURN n").into_bytes(),
        ),
    )
    .unwrap();
    let second = read_native_payload(&mut stream, NativeMessageType::Response, 4);
    let second_parts = second.split('\t').collect::<Vec<_>>();
    assert_eq!(second_parts[0..3], ["OK", "RESULT_START", "2"]);
    assert_eq!(second_parts[3], "2");

    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Command,
            5,
            format!("COMMIT_TX\t{tx_id}").into_bytes(),
        ),
    )
    .unwrap();
    assert_native_response(
        &mut stream,
        NativeMessageType::Response,
        5,
        &format!("OK\tTX_COMMIT\t{tx_id}"),
    );

    write_frame(
        &mut stream,
        &NativeFrame::new(NativeMessageType::Quit, 6, Vec::new()),
    )
    .unwrap();
    assert_native_response(&mut stream, NativeMessageType::Response, 6, "OK\tBYE");

    server.join().unwrap();
    let _ = fs::remove_dir_all(dir);
}

#[test]
pub(super) fn native_transaction_query_plan_reports_access_path() {
    let dir = temp_dir("neo4r-native-tx-query-plan");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1)).unwrap();
    db.execute_cypher("CREATE INDEX person_name FOR (n:Person) ON (n.name)")
        .unwrap();
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
            b"BEGIN_TX\tSNAPSHOT".to_vec(),
        ),
    )
    .unwrap();
    let begin = read_native_payload(&mut stream, NativeMessageType::Response, 1);
    let tx_id = begin.split('\t').collect::<Vec<_>>()[2]
        .parse::<u64>()
        .unwrap();

    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Command,
            2,
            format!(
                "TX_QUERY_PLAN\t{tx_id}\tMATCH (n:Person {{name: $name}}) RETURN n\tname=s:Alice"
            )
            .into_bytes(),
        ),
    )
    .unwrap();
    let plan = read_native_payload(&mut stream, NativeMessageType::Response, 2);
    assert!(plan.starts_with("OK\tQUERY_PLAN\t"));
    assert!(plan.contains("route=local"));
    assert!(plan.contains("access=node_index_seek(Person.name)"));
    assert!(plan.contains("tx_mode=READ_ONLY"));
    assert!(plan.contains("tx_isolation=SNAPSHOT"));
    assert!(plan.contains("staged_writes=0"));
    assert!(plan.contains("staged_overlay=none"));

    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Command,
            3,
            format!("COMMIT_TX\t{tx_id}").into_bytes(),
        ),
    )
    .unwrap();
    assert_native_response(
        &mut stream,
        NativeMessageType::Response,
        3,
        &format!("OK\tTX_COMMIT\t{tx_id}"),
    );

    write_frame(
        &mut stream,
        &NativeFrame::new(NativeMessageType::Quit, 4, Vec::new()),
    )
    .unwrap();
    assert_native_response(&mut stream, NativeMessageType::Response, 4, "OK\tBYE");

    server.join().unwrap();
    let _ = fs::remove_dir_all(dir);
}

#[test]
pub(super) fn native_read_write_transaction_query_plan_reports_staged_overlay() {
    let dir = temp_dir("neo4r-native-tx-query-plan-staged");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1)).unwrap();
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
            b"BEGIN_TX\tREAD_WRITE READ_COMMITTED".to_vec(),
        ),
    )
    .unwrap();
    let begin = read_native_payload(&mut stream, NativeMessageType::Response, 1);
    let begin_parts = begin.split('\t').collect::<Vec<_>>();
    assert_eq!(begin_parts[3], "READ_WRITE");
    assert_eq!(begin_parts[4], "READ_COMMITTED");
    let tx_id = begin_parts[2].parse::<u64>().unwrap();

    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Command,
            2,
            format!("TX_QUERY_PLAN\t{tx_id}\tMATCH (n:Person) RETURN n").into_bytes(),
        ),
    )
    .unwrap();
    let before = read_native_payload(&mut stream, NativeMessageType::Response, 2);
    assert!(before.starts_with("OK\tQUERY_PLAN\t"));
    assert!(before.contains("tx_mode=READ_WRITE"));
    assert!(before.contains("tx_isolation=READ_COMMITTED"));
    assert!(before.contains("staged_writes=0"));
    assert!(before.contains("staged_overlay=none"));

    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Command,
            3,
            format!("TX_QUERY\t{tx_id}\tCREATE (n:Person {{name: \"Alice\"}}) RETURN n")
                .into_bytes(),
        ),
    )
    .unwrap();
    assert_native_response(
        &mut stream,
        NativeMessageType::Response,
        3,
        &format!("OK\tTX_STAGED\t{tx_id}\t1"),
    );

    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Command,
            4,
            format!("TX_QUERY_PLAN\t{tx_id}\tMATCH (n:Person) RETURN n").into_bytes(),
        ),
    )
    .unwrap();
    let after = read_native_payload(&mut stream, NativeMessageType::Response, 4);
    assert!(after.starts_with("OK\tQUERY_PLAN\t"));
    assert!(after.contains("tx_mode=READ_WRITE"));
    assert!(after.contains("tx_isolation=READ_COMMITTED"));
    assert!(after.contains("staged_writes=1"));
    assert!(after.contains("staged_overlay=pending"));

    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Command,
            5,
            format!("ROLLBACK_TX\t{tx_id}").into_bytes(),
        ),
    )
    .unwrap();
    assert_native_response(
        &mut stream,
        NativeMessageType::Response,
        5,
        &format!("OK\tTX_ROLLBACK\t{tx_id}"),
    );

    write_frame(
        &mut stream,
        &NativeFrame::new(NativeMessageType::Quit, 6, Vec::new()),
    )
    .unwrap();
    assert_native_response(&mut stream, NativeMessageType::Response, 6, "OK\tBYE");

    server.join().unwrap();
    let _ = fs::remove_dir_all(dir);
}

#[test]
pub(super) fn native_read_write_transaction_stages_writes_until_commit() {
    let dir = temp_dir("neo4r-native-read-write-tx");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1)).unwrap();
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
            b"BEGIN_TX\tREAD_WRITE READ_COMMITTED".to_vec(),
        ),
    )
    .unwrap();
    let begin = read_native_payload(&mut stream, NativeMessageType::Response, 1);
    let begin_parts = begin.split('\t').collect::<Vec<_>>();
    assert_eq!(begin_parts[3], "READ_WRITE");
    assert_eq!(begin_parts[4], "READ_COMMITTED");
    let tx_id = begin_parts[2].parse::<u64>().unwrap();

    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Command,
            2,
            format!("TX_QUERY\t{tx_id}\tCREATE (n:Person {{name: $name}}) RETURN n\tname=s:Alice")
                .into_bytes(),
        ),
    )
    .unwrap();
    assert_native_response(
        &mut stream,
        NativeMessageType::Response,
        2,
        &format!("OK\tTX_STAGED\t{tx_id}\t1"),
    );

    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Command,
            3,
            format!("TX_QUERY\t{tx_id}\tMATCH (n:Person) RETURN n").into_bytes(),
        ),
    )
    .unwrap();
    let staged_read = read_native_payload(&mut stream, NativeMessageType::Response, 3);
    let staged_parts = staged_read.split('\t').collect::<Vec<_>>();
    assert_eq!(staged_parts[0..2], ["OK", "RESULT_START"]);
    assert_eq!(staged_parts[3], "1");
    let rows = decode_query_rows(staged_parts[6]).unwrap();
    let Some(neo4r_query::QueryValue::Node(node)) = rows[0].get("n") else {
        panic!("expected staged node row");
    };
    assert_eq!(node.labels, vec!["Person"]);
    assert_eq!(
        node.properties.get("name"),
        Some(&Value::String("Alice".to_string()))
    );

    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Query,
            4,
            b"MATCH (n:Person) RETURN n".to_vec(),
        ),
    )
    .unwrap();
    let before_commit = read_native_payload(&mut stream, NativeMessageType::Response, 4);
    let before_parts = before_commit.split('\t').collect::<Vec<_>>();
    assert_eq!(before_parts[0..2], ["OK", "RESULT_START"]);
    assert_eq!(before_parts[3], "0");

    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Command,
            5,
            format!("COMMIT_TX\t{tx_id}").into_bytes(),
        ),
    )
    .unwrap();
    assert_native_response(
        &mut stream,
        NativeMessageType::Response,
        5,
        &format!("OK\tTX_COMMIT\t{tx_id}\t1"),
    );

    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Query,
            6,
            b"MATCH (n:Person) RETURN n".to_vec(),
        ),
    )
    .unwrap();
    let after_commit = read_native_payload(&mut stream, NativeMessageType::Response, 6);
    let after_parts = after_commit.split('\t').collect::<Vec<_>>();
    assert_eq!(after_parts[0..2], ["OK", "RESULT_START"]);
    assert_eq!(after_parts[3], "1");

    write_frame(
        &mut stream,
        &NativeFrame::new(NativeMessageType::Quit, 7, Vec::new()),
    )
    .unwrap();
    assert_native_response(&mut stream, NativeMessageType::Response, 7, "OK\tBYE");

    server.join().unwrap();
    let _ = fs::remove_dir_all(dir);
}

#[test]
pub(super) fn native_read_write_transaction_reads_staged_relationship_creates() {
    let dir = temp_dir("neo4r-native-read-write-tx-create-relationship-overlay");
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
            b"BEGIN_TX\tREAD_WRITE READ_COMMITTED".to_vec(),
        ),
    )
    .unwrap();
    let begin = read_native_payload(&mut stream, NativeMessageType::Response, 1);
    let tx_id = begin.split('\t').collect::<Vec<_>>()[2]
        .parse::<u64>()
        .unwrap();

    write_frame(
            &mut stream,
            &NativeFrame::new(
                NativeMessageType::Command,
                2,
                format!(
                    "TX_QUERY\t{tx_id}\tMATCH (a:Person {{name: $from}}), (b:Person {{name: $to}}) CREATE (a)-[r:KNOWS {{weight: $weight}}]->(b) SET r.status = $status RETURN r\tfrom=s:Alice\tto=s:Bob\tweight=i:3\tstatus=s:staged"
                )
                .into_bytes(),
            ),
        )
        .unwrap();
    assert_native_response(
        &mut stream,
        NativeMessageType::Response,
        2,
        &format!("OK\tTX_STAGED\t{tx_id}\t1"),
    );

    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Command,
            3,
            format!("TX_QUERY\t{tx_id}\tMATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN r")
                .into_bytes(),
        ),
    )
    .unwrap();
    let staged_read = read_native_payload(&mut stream, NativeMessageType::Response, 3);
    let staged_parts = staged_read.split('\t').collect::<Vec<_>>();
    assert_eq!(staged_parts[0..2], ["OK", "RESULT_START"]);
    assert_eq!(staged_parts[3], "1");
    let rows = decode_query_rows(staged_parts[6]).unwrap();
    let Some(neo4r_query::QueryValue::Relationship(relationship)) = rows[0].get("r") else {
        panic!("expected staged relationship row");
    };
    assert_eq!(relationship.rel_type, "KNOWS");
    assert_eq!(relationship.properties.get("weight"), Some(&Value::Int(3)));
    assert_eq!(
        relationship.properties.get("status"),
        Some(&Value::String("staged".to_string()))
    );

    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Query,
            4,
            b"MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN r".to_vec(),
        ),
    )
    .unwrap();
    let committed_read = read_native_payload(&mut stream, NativeMessageType::Response, 4);
    let committed_parts = committed_read.split('\t').collect::<Vec<_>>();
    assert_eq!(committed_parts[0..2], ["OK", "RESULT_START"]);
    assert_eq!(committed_parts[4], "0");

    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Command,
            5,
            format!("ROLLBACK_TX\t{tx_id}").into_bytes(),
        ),
    )
    .unwrap();
    assert_native_response(
        &mut stream,
        NativeMessageType::Response,
        5,
        &format!("OK\tTX_ROLLBACK\t{tx_id}"),
    );

    write_frame(
        &mut stream,
        &NativeFrame::new(NativeMessageType::Quit, 6, Vec::new()),
    )
    .unwrap();
    assert_native_response(&mut stream, NativeMessageType::Response, 6, "OK\tBYE");

    server.join().unwrap();
    let _ = fs::remove_dir_all(dir);
}
