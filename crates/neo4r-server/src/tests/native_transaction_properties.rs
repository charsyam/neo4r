#![allow(unused_imports)]
use super::*;

#[test]
pub(super) fn native_read_write_transaction_set_null_removes_property() {
    let dir = temp_dir("neo4r-native-read-write-tx-set-null");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1)).unwrap();
    db.execute_cypher(r#"CREATE (n:Person {name: "Alice", status: "active"})"#)
        .unwrap();
    let backend = TcpBackend::with_config(
        db.clone(),
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
                    "TX_QUERY\t{tx_id}\tMATCH (n:Person) WHERE n.name = $name SET n.status = null\tname=s:Alice"
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
                format!(
                    "TX_QUERY\t{tx_id}\tMATCH (n:Person) WHERE n.name = $name RETURN n.status\tname=s:Alice"
                )
                .into_bytes(),
            ),
        )
        .unwrap();
    let staged_read = read_native_payload(&mut stream, NativeMessageType::Response, 3);
    let staged_parts = staged_read.split('\t').collect::<Vec<_>>();
    assert_eq!(staged_parts[0..3], ["OK", "RESULT_START", "1"]);
    let rows = decode_query_rows(staged_parts[6]).unwrap();
    assert_eq!(
        rows[0].get("n.status"),
        Some(&neo4r_query::QueryValue::Scalar(Value::Null))
    );

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
        &format!("OK\tTX_COMMIT\t{tx_id}\t1"),
    );

    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Query,
            5,
            b"MATCH (n:Person) WHERE n.name = \"Alice\" RETURN n.status".to_vec(),
        ),
    )
    .unwrap();
    let committed_read = read_native_payload(&mut stream, NativeMessageType::Response, 5);
    let committed_parts = committed_read.split('\t').collect::<Vec<_>>();
    assert_eq!(committed_parts[0..2], ["OK", "RESULT_START"]);
    assert_eq!(committed_parts[3], "1");
    let rows = decode_query_rows(committed_parts[6]).unwrap();
    assert_eq!(
        rows[0].get("n.status"),
        Some(&neo4r_query::QueryValue::Scalar(Value::Null))
    );

    write_frame(
        &mut stream,
        &NativeFrame::new(NativeMessageType::Quit, 6, Vec::new()),
    )
    .unwrap();
    assert_native_response(&mut stream, NativeMessageType::Response, 6, "OK\tBYE");

    server.join().unwrap();
    assert!(db
        .query(r#"MATCH (n:Person) WHERE n.status = "active" RETURN n"#)
        .unwrap()
        .is_empty());
    let _ = fs::remove_dir_all(dir);
}

#[test]
pub(super) fn native_read_write_transaction_reads_staged_node_property_replacement() {
    let dir = temp_dir("neo4r-native-read-write-tx-read-your-replacement");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1)).unwrap();
    db.create_node(
        vec!["Person".to_string()],
        [
            ("name".to_string(), Value::String("Alice".to_string())),
            ("age".to_string(), Value::Int(42)),
        ]
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
                    "TX_QUERY\t{tx_id}\tMATCH (n:Person) WHERE n.name = $name SET n = {{name: $name, status: $status}}\tname=s:Alice\tstatus=s:active"
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
                format!(
                    "TX_QUERY\t{tx_id}\tMATCH (n:Person) WHERE n.status = $status RETURN n.status, n.age\tstatus=s:active"
                )
                .into_bytes(),
            ),
        )
        .unwrap();
    let staged_read = read_native_payload(&mut stream, NativeMessageType::Response, 3);
    let staged_parts = staged_read.split('\t').collect::<Vec<_>>();
    assert_eq!(staged_parts[0..3], ["OK", "RESULT_START", "1"]);
    assert_eq!(staged_parts[3], "1");
    let rows = decode_query_rows(staged_parts[6]).unwrap();
    assert_eq!(
        rows[0].get("n.status"),
        Some(&neo4r_query::QueryValue::Scalar(Value::String(
            "active".to_string()
        )))
    );
    assert_eq!(
        rows[0].get("n.age"),
        Some(&neo4r_query::QueryValue::Scalar(Value::Null))
    );

    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Query,
            4,
            b"MATCH (n:Person) WHERE n.status = \"active\" RETURN n".to_vec(),
        ),
    )
    .unwrap();
    let committed_read = read_native_payload(&mut stream, NativeMessageType::Response, 4);
    let committed_parts = committed_read.split('\t').collect::<Vec<_>>();
    assert_eq!(committed_parts[3], "0");

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
            b"MATCH (n:Person) WHERE n.status = \"active\" RETURN n.age".to_vec(),
        ),
    )
    .unwrap();
    let committed_read = read_native_payload(&mut stream, NativeMessageType::Response, 6);
    let committed_parts = committed_read.split('\t').collect::<Vec<_>>();
    assert_eq!(committed_parts[3], "1");
    let rows = decode_query_rows(committed_parts[6]).unwrap();
    assert_eq!(
        rows[0].get("n.age"),
        Some(&neo4r_query::QueryValue::Scalar(Value::Null))
    );

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
pub(super) fn native_read_write_transaction_accepts_parameterized_property_map_replacement() {
    let dir = temp_dir("neo4r-native-read-write-tx-parameterized-map-replacement");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1)).unwrap();
    db.create_node(
        vec!["Person".to_string()],
        [
            ("name".to_string(), Value::String("Alice".to_string())),
            ("age".to_string(), Value::Int(42)),
        ]
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

    let props = test_map_param(&[
        ("name", Value::String("Alice".to_string())),
        ("status", Value::String("active".to_string())),
    ]);
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
                    "TX_QUERY\t{tx_id}\tMATCH (n:Person) WHERE n.name = $name SET n = $props\tname=s:Alice\tprops=m:{props}"
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
                format!(
                    "TX_QUERY\t{tx_id}\tMATCH (n:Person) WHERE n.name = $name RETURN n.status, n.age\tname=s:Alice"
                )
                .into_bytes(),
            ),
        )
        .unwrap();
    let staged_read = read_native_payload(&mut stream, NativeMessageType::Response, 3);
    let staged_parts = staged_read.split('\t').collect::<Vec<_>>();
    assert_eq!(staged_parts[0..3], ["OK", "RESULT_START", "1"]);
    assert_eq!(staged_parts[3], "1");
    let rows = decode_query_rows(staged_parts[6]).unwrap();
    assert_eq!(
        rows[0].get("n.status"),
        Some(&neo4r_query::QueryValue::Scalar(Value::String(
            "active".to_string()
        )))
    );
    assert_eq!(
        rows[0].get("n.age"),
        Some(&neo4r_query::QueryValue::Scalar(Value::Null))
    );

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
        &format!("OK\tTX_COMMIT\t{tx_id}\t1"),
    );

    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Query,
            5,
            b"MATCH (n:Person) WHERE n.name = \"Alice\" RETURN n.status, n.age".to_vec(),
        ),
    )
    .unwrap();
    let committed_read = read_native_payload(&mut stream, NativeMessageType::Response, 5);
    let committed_parts = committed_read.split('\t').collect::<Vec<_>>();
    assert_eq!(committed_parts[0..2], ["OK", "RESULT_START"]);
    assert_eq!(committed_parts[3], "1");
    let rows = decode_query_rows(committed_parts[6]).unwrap();
    assert_eq!(
        rows[0].get("n.status"),
        Some(&neo4r_query::QueryValue::Scalar(Value::String(
            "active".to_string()
        )))
    );
    assert_eq!(
        rows[0].get("n.age"),
        Some(&neo4r_query::QueryValue::Scalar(Value::Null))
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
pub(super) fn native_read_write_transaction_reads_staged_relationship_property_updates() {
    let dir = temp_dir("neo4r-native-read-write-tx-read-your-relationship-writes");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1)).unwrap();
    let alice = db
        .create_node(
            vec!["Person".to_string()],
            [("name".to_string(), Value::String("Alice".to_string()))]
                .into_iter()
                .collect(),
        )
        .unwrap();
    let bob = db
        .create_node(
            vec!["Person".to_string()],
            [("name".to_string(), Value::String("Bob".to_string()))]
                .into_iter()
                .collect(),
        )
        .unwrap();
    db.create_relationship(alice, bob, "KNOWS".to_string(), Default::default())
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
                    "TX_QUERY\t{tx_id}\tMATCH (a:Person)-[r:KNOWS]->(b:Person) SET r.weight = $weight\tweight=i:7"
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
            format!("TX_QUERY\t{tx_id}\tMATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN r.weight")
                .into_bytes(),
        ),
    )
    .unwrap();
    let staged_read = read_native_payload(&mut stream, NativeMessageType::Response, 3);
    let staged_parts = staged_read.split('\t').collect::<Vec<_>>();
    assert_eq!(staged_parts[0..3], ["OK", "RESULT_START", "1"]);
    assert_eq!(staged_parts[3], "1");
    let rows = decode_query_rows(staged_parts[6]).unwrap();
    assert_eq!(
        rows[0].get("r.weight"),
        Some(&neo4r_query::QueryValue::Scalar(Value::Int(7)))
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
    let rows = decode_query_rows(committed_parts[6]).unwrap();
    assert_eq!(rows.len(), 1);
    let Some(neo4r_query::QueryValue::Relationship(relationship)) = rows[0].get("r") else {
        panic!("expected relationship row");
    };
    assert!(!relationship.properties.contains_key("weight"));

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
        &NativeFrame::new(NativeMessageType::Quit, 6, Vec::new()),
    )
    .unwrap();
    assert_native_response(&mut stream, NativeMessageType::Response, 6, "OK\tBYE");

    server.join().unwrap();
    let _ = fs::remove_dir_all(dir);
}

#[test]
pub(super) fn native_read_write_transaction_reads_staged_relationship_property_replacement() {
    let dir = temp_dir("neo4r-native-read-write-tx-read-your-relationship-replacement");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1)).unwrap();
    let alice = db
        .create_node(
            vec!["Person".to_string()],
            [("name".to_string(), Value::String("Alice".to_string()))]
                .into_iter()
                .collect(),
        )
        .unwrap();
    let bob = db
        .create_node(
            vec!["Person".to_string()],
            [("name".to_string(), Value::String("Bob".to_string()))]
                .into_iter()
                .collect(),
        )
        .unwrap();
    db.create_relationship(
        alice,
        bob,
        "KNOWS".to_string(),
        [
            ("since".to_string(), Value::Int(2026)),
            ("weight".to_string(), Value::Int(7)),
        ]
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
                    "TX_QUERY\t{tx_id}\tMATCH (a:Person)-[r:KNOWS]->(b:Person) WHERE r.weight = $weight SET r = {{status: $status}}\tweight=i:7\tstatus=s:final"
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
                format!(
                    "TX_QUERY\t{tx_id}\tMATCH (a:Person)-[r:KNOWS]->(b:Person) WHERE r.status = $status RETURN r.status, r.weight\tstatus=s:final"
                )
                .into_bytes(),
            ),
        )
        .unwrap();
    let staged_read = read_native_payload(&mut stream, NativeMessageType::Response, 3);
    let staged_parts = staged_read.split('\t').collect::<Vec<_>>();
    assert_eq!(staged_parts[0..3], ["OK", "RESULT_START", "1"]);
    assert_eq!(staged_parts[3], "1");
    let rows = decode_query_rows(staged_parts[6]).unwrap();
    assert_eq!(
        rows[0].get("r.status"),
        Some(&neo4r_query::QueryValue::Scalar(Value::String(
            "final".to_string()
        )))
    );
    assert_eq!(
        rows[0].get("r.weight"),
        Some(&neo4r_query::QueryValue::Scalar(Value::Null))
    );

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
        &format!("OK\tTX_COMMIT\t{tx_id}\t1"),
    );

    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Query,
            5,
            b"MATCH (a:Person)-[r:KNOWS]->(b:Person) WHERE r.status = \"final\" RETURN r".to_vec(),
        ),
    )
    .unwrap();
    let committed_read = read_native_payload(&mut stream, NativeMessageType::Response, 5);
    let committed_parts = committed_read.split('\t').collect::<Vec<_>>();
    assert_eq!(committed_parts[0..2], ["OK", "RESULT_START"]);
    assert_eq!(committed_parts[4], "1");
    let rows = decode_query_rows(committed_parts[6]).unwrap();
    let Some(neo4r_query::QueryValue::Relationship(relationship)) = rows[0].get("r") else {
        panic!("expected relationship row");
    };
    assert_eq!(
        relationship.properties.get("status"),
        Some(&Value::String("final".to_string()))
    );
    assert!(!relationship.properties.contains_key("weight"));

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
pub(super) fn native_read_write_transaction_reads_staged_deletes() {
    let dir = temp_dir("neo4r-native-read-write-tx-read-your-deletes");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1)).unwrap();
    let alice = db
        .create_node(
            vec!["Person".to_string()],
            [("name".to_string(), Value::String("Alice".to_string()))]
                .into_iter()
                .collect(),
        )
        .unwrap();
    let bob = db
        .create_node(
            vec!["Person".to_string()],
            [("name".to_string(), Value::String("Bob".to_string()))]
                .into_iter()
                .collect(),
        )
        .unwrap();
    db.create_relationship(alice, bob, "KNOWS".to_string(), Default::default())
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
                    "TX_QUERY\t{tx_id}\tMATCH (n:Person) WHERE n.name = $name DETACH DELETE n\tname=s:Bob"
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
            format!("TX_QUERY\t{tx_id}\tMATCH (n:Person) RETURN n").into_bytes(),
        ),
    )
    .unwrap();
    let node_read = read_native_payload(&mut stream, NativeMessageType::Response, 3);
    let node_parts = node_read.split('\t').collect::<Vec<_>>();
    let rows = decode_query_rows(node_parts[6]).unwrap();
    assert_eq!(rows.len(), 1);
    let Some(neo4r_query::QueryValue::Node(node)) = rows[0].get("n") else {
        panic!("expected node row");
    };
    assert_eq!(
        node.properties.get("name"),
        Some(&Value::String("Alice".to_string()))
    );

    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Command,
            4,
            format!("TX_QUERY\t{tx_id}\tMATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN r")
                .into_bytes(),
        ),
    )
    .unwrap();
    let relationship_read = read_native_payload(&mut stream, NativeMessageType::Response, 4);
    let relationship_parts = relationship_read.split('\t').collect::<Vec<_>>();
    assert!(decode_query_rows(relationship_parts[6]).unwrap().is_empty());

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
pub(super) fn native_read_write_transaction_group_commits_batchable_sets() {
    let dir = temp_dir("neo4r-native-read-write-tx-batch");
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
        db.clone(),
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
            format!("TX_QUERY\t{tx_id}\tMATCH (n:Person) SET n.status = $status\tstatus=s:active")
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
            format!(
                "TX_QUERY\t{tx_id}\tMATCH (n:Person) SET n.reviewed = $reviewed\treviewed=b:true"
            )
            .into_bytes(),
        ),
    )
    .unwrap();
    assert_native_response(
        &mut stream,
        NativeMessageType::Response,
        3,
        &format!("OK\tTX_STAGED\t{tx_id}\t2"),
    );

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
        &format!("OK\tTX_COMMIT\t{tx_id}\t2"),
    );

    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Query,
            5,
            b"MATCH (n:Person) WHERE n.status = \"active\" RETURN n.reviewed".to_vec(),
        ),
    )
    .unwrap();
    let rows = read_native_payload(&mut stream, NativeMessageType::Response, 5);
    let parts = rows.split('\t').collect::<Vec<_>>();
    assert_eq!(parts[0..2], ["OK", "RESULT_START"]);
    assert_eq!(parts[3], "2");
    assert_eq!(parts[4], "2");
    assert_eq!(db.committed_indexes().unwrap(), vec![6]);

    write_frame(
        &mut stream,
        &NativeFrame::new(NativeMessageType::Quit, 6, Vec::new()),
    )
    .unwrap();
    assert_native_response(&mut stream, NativeMessageType::Response, 6, "OK\tBYE");

    server.join().unwrap();
    let _ = fs::remove_dir_all(dir);
}
