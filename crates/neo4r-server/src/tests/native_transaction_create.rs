#![allow(unused_imports)]
use super::*;

#[test]
pub(super) fn native_read_write_transaction_commits_staged_create_property_replacement() {
    let dir = temp_dir("neo4r-native-read-write-tx-create-replace-map");
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
                format!(
                    "TX_QUERY\t{tx_id}\tCREATE (n:Person {{name: $name, stale: true}}) SET n = {{name: $name, status: $status}} RETURN n\tname=s:Alice\tstatus=s:active"
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
                    "TX_QUERY\t{tx_id}\tMATCH (n:Person) WHERE n.name = $name RETURN n.status, n.stale\tname=s:Alice"
                )
                .into_bytes(),
            ),
        )
        .unwrap();
    let staged_read = read_native_payload(&mut stream, NativeMessageType::Response, 3);
    let staged_parts = staged_read.split('\t').collect::<Vec<_>>();
    assert_eq!(staged_parts[0..2], ["OK", "RESULT_START"]);
    assert_eq!(staged_parts[3], "1");
    let rows = decode_query_rows(staged_parts[6]).unwrap();
    assert_eq!(
        rows[0].get("n.status"),
        Some(&neo4r_query::QueryValue::Scalar(Value::String(
            "active".to_string()
        )))
    );
    assert_eq!(
        rows[0].get("n.stale"),
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
            b"MATCH (n:Person) WHERE n.status = \"active\" RETURN n.stale".to_vec(),
        ),
    )
    .unwrap();
    let committed_read = read_native_payload(&mut stream, NativeMessageType::Response, 5);
    let committed_parts = committed_read.split('\t').collect::<Vec<_>>();
    assert_eq!(committed_parts[0..2], ["OK", "RESULT_START"]);
    assert_eq!(committed_parts[3], "1");
    let rows = decode_query_rows(committed_parts[6]).unwrap();
    assert_eq!(
        rows[0].get("n.stale"),
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
pub(super) fn native_read_write_transaction_commits_staged_relationship_create_property_replacement(
) {
    let dir = temp_dir("neo4r-native-read-write-tx-create-rel-replace-map");
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
                    "TX_QUERY\t{tx_id}\tMATCH (a:Person {{name: $from}}), (b:Person {{name: $to}}) CREATE (a)-[r:KNOWS {{weight: $weight, stale: true}}]->(b) SET r = {{status: $status}} RETURN r\tfrom=s:Alice\tto=s:Bob\tweight=i:3\tstatus=s:created"
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
    assert_eq!(
        relationship.properties.get("status"),
        Some(&Value::String("created".to_string()))
    );
    assert_eq!(relationship.properties.get("weight"), None);
    assert_eq!(relationship.properties.get("stale"), None);

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
            b"MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN r".to_vec(),
        ),
    )
    .unwrap();
    let committed_read = read_native_payload(&mut stream, NativeMessageType::Response, 5);
    let committed_parts = committed_read.split('\t').collect::<Vec<_>>();
    assert_eq!(committed_parts[0..2], ["OK", "RESULT_START"]);
    assert_eq!(committed_parts[4], "1");
    let rows = decode_query_rows(committed_parts[6]).unwrap();
    let Some(neo4r_query::QueryValue::Relationship(relationship)) = rows[0].get("r") else {
        panic!("expected committed relationship row");
    };
    assert_eq!(relationship.rel_type, "KNOWS");
    assert_eq!(
        relationship.properties.get("status"),
        Some(&Value::String("created".to_string()))
    );
    assert_eq!(relationship.properties.get("weight"), None);
    assert_eq!(relationship.properties.get("stale"), None);

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
pub(super) fn native_read_write_transaction_commits_staged_create_then_set() {
    let dir = temp_dir("neo4r-native-read-write-tx-create-then-set");
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
            format!("TX_QUERY\t{tx_id}\tCREATE (n:Person {{name: $name}}) RETURN n\tname=s:Carol")
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
                    "TX_QUERY\t{tx_id}\tMATCH (n:Person) WHERE n.name = $name SET n.status = $status\tname=s:Carol\tstatus=s:active"
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
                format!(
                    "TX_QUERY\t{tx_id}\tMATCH (n:Person) WHERE n.name = $name RETURN n.status\tname=s:Carol"
                )
                .into_bytes(),
            ),
        )
        .unwrap();
    let staged_read = read_native_payload(&mut stream, NativeMessageType::Response, 4);
    let staged_parts = staged_read.split('\t').collect::<Vec<_>>();
    assert_eq!(staged_parts[0..2], ["OK", "RESULT_START"]);
    assert_eq!(staged_parts[3], "1");
    let rows = decode_query_rows(staged_parts[6]).unwrap();
    assert_eq!(
        rows[0].get("n.status"),
        Some(&neo4r_query::QueryValue::Scalar(Value::String(
            "active".to_string()
        )))
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
        &format!("OK\tTX_COMMIT\t{tx_id}\t2"),
    );

    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Query,
            6,
            b"MATCH (n:Person) WHERE n.name = \"Carol\" RETURN n.status".to_vec(),
        ),
    )
    .unwrap();
    let committed_read = read_native_payload(&mut stream, NativeMessageType::Response, 6);
    let committed_parts = committed_read.split('\t').collect::<Vec<_>>();
    assert_eq!(committed_parts[0..2], ["OK", "RESULT_START"]);
    assert_eq!(committed_parts[4], "1");
    let rows = decode_query_rows(committed_parts[6]).unwrap();
    assert_eq!(
        rows[0].get("n.status"),
        Some(&neo4r_query::QueryValue::Scalar(Value::String(
            "active".to_string()
        )))
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
pub(super) fn native_read_write_transaction_commits_staged_nodes_and_relationship() {
    let dir = temp_dir("neo4r-native-read-write-tx-create-graph");
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
            format!("TX_QUERY\t{tx_id}\tCREATE (n:Person {{name: $name}})\tname=s:Alice")
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
            format!("TX_QUERY\t{tx_id}\tCREATE (n:Person {{name: $name}})\tname=s:Bob")
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
                format!(
                    "TX_QUERY\t{tx_id}\tMATCH (a:Person {{name: $from}}), (b:Person {{name: $to}}) CREATE (a)-[r:KNOWS {{weight: $weight}}]->(b)\tfrom=s:Alice\tto=s:Bob\tweight=i:5"
                )
                .into_bytes(),
            ),
        )
        .unwrap();
    assert_native_response(
        &mut stream,
        NativeMessageType::Response,
        4,
        &format!("OK\tTX_STAGED\t{tx_id}\t3"),
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
        &format!("OK\tTX_COMMIT\t{tx_id}\t3"),
    );

    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Query,
            6,
            b"MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN r".to_vec(),
        ),
    )
    .unwrap();
    let committed_read = read_native_payload(&mut stream, NativeMessageType::Response, 6);
    let committed_parts = committed_read.split('\t').collect::<Vec<_>>();
    assert_eq!(committed_parts[0..2], ["OK", "RESULT_START"]);
    assert_eq!(committed_parts[4], "1");
    let rows = decode_query_rows(committed_parts[6]).unwrap();
    let Some(neo4r_query::QueryValue::Relationship(relationship)) = rows[0].get("r") else {
        panic!("expected committed relationship");
    };
    assert_eq!(relationship.properties.get("weight"), Some(&Value::Int(5)));

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
pub(super) fn native_read_write_transaction_merges_staged_node_on_match() {
    let dir = temp_dir("neo4r-native-read-write-tx-merge-node-overlay");
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
            format!("TX_QUERY\t{tx_id}\tCREATE (n:Person {{name: $name}})\tname=s:Alice")
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
                    "TX_QUERY\t{tx_id}\tMERGE (n:Person {{name: $name}}) ON MATCH SET n.status = $status\tname=s:Alice\tstatus=s:matched"
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
            b"MATCH (n:Person) WHERE n.name = \"Alice\" RETURN n.status".to_vec(),
        ),
    )
    .unwrap();
    let committed_read = read_native_payload(&mut stream, NativeMessageType::Response, 5);
    let committed_parts = committed_read.split('\t').collect::<Vec<_>>();
    assert_eq!(committed_parts[0..2], ["OK", "RESULT_START"]);
    assert_eq!(committed_parts[4], "1");
    let rows = decode_query_rows(committed_parts[6]).unwrap();
    assert_eq!(
        rows[0].get("n.status"),
        Some(&neo4r_query::QueryValue::Scalar(Value::String(
            "matched".to_string()
        )))
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
pub(super) fn native_read_write_transaction_merges_staged_relationship_on_match() {
    let dir = temp_dir("neo4r-native-read-write-tx-merge-relationship-overlay");
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

    for (request_id, name) in [(2, "Alice"), (3, "Bob")] {
        write_frame(
            &mut stream,
            &NativeFrame::new(
                NativeMessageType::Command,
                request_id,
                format!("TX_QUERY\t{tx_id}\tCREATE (n:Person {{name: $name}})\tname=s:{name}")
                    .into_bytes(),
            ),
        )
        .unwrap();
        assert_native_response(
            &mut stream,
            NativeMessageType::Response,
            request_id,
            &format!("OK\tTX_STAGED\t{tx_id}\t{}", request_id - 1),
        );
    }

    write_frame(
            &mut stream,
            &NativeFrame::new(
                NativeMessageType::Command,
                4,
                format!(
                    "TX_QUERY\t{tx_id}\tMATCH (a:Person {{name: $from}}), (b:Person {{name: $to}}) MERGE (a)-[r:KNOWS {{kind: $kind}}]->(b) ON CREATE SET r.since = $since\tfrom=s:Alice\tto=s:Bob\tkind=s:friend\tsince=i:2026"
                )
                .into_bytes(),
            ),
        )
        .unwrap();
    assert_native_response(
        &mut stream,
        NativeMessageType::Response,
        4,
        &format!("OK\tTX_STAGED\t{tx_id}\t3"),
    );

    write_frame(
            &mut stream,
            &NativeFrame::new(
                NativeMessageType::Command,
                5,
                format!(
                    "TX_QUERY\t{tx_id}\tMATCH (a:Person {{name: $from}}), (b:Person {{name: $to}}) MERGE (a)-[r:KNOWS {{kind: $kind}}]->(b) ON MATCH SET r.status = $status\tfrom=s:Alice\tto=s:Bob\tkind=s:friend\tstatus=s:seen"
                )
                .into_bytes(),
            ),
        )
        .unwrap();
    assert_native_response(
        &mut stream,
        NativeMessageType::Response,
        5,
        &format!("OK\tTX_STAGED\t{tx_id}\t4"),
    );

    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Command,
            6,
            format!("COMMIT_TX\t{tx_id}").into_bytes(),
        ),
    )
    .unwrap();
    assert_native_response(
        &mut stream,
        NativeMessageType::Response,
        6,
        &format!("OK\tTX_COMMIT\t{tx_id}\t4"),
    );

    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Query,
            7,
            b"MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN r".to_vec(),
        ),
    )
    .unwrap();
    let committed_read = read_native_payload(&mut stream, NativeMessageType::Response, 7);
    let committed_parts = committed_read.split('\t').collect::<Vec<_>>();
    assert_eq!(committed_parts[0..2], ["OK", "RESULT_START"]);
    assert_eq!(committed_parts[4], "1");
    let rows = decode_query_rows(committed_parts[6]).unwrap();
    let Some(neo4r_query::QueryValue::Relationship(relationship)) = rows[0].get("r") else {
        panic!("expected committed relationship");
    };
    assert_eq!(
        relationship.properties.get("kind"),
        Some(&Value::String("friend".to_string()))
    );
    assert_eq!(
        relationship.properties.get("since"),
        Some(&Value::Int(2026))
    );
    assert_eq!(
        relationship.properties.get("status"),
        Some(&Value::String("seen".to_string()))
    );

    write_frame(
        &mut stream,
        &NativeFrame::new(NativeMessageType::Quit, 8, Vec::new()),
    )
    .unwrap();
    assert_native_response(&mut stream, NativeMessageType::Response, 8, "OK\tBYE");

    server.join().unwrap();
    let _ = fs::remove_dir_all(dir);
}

#[test]
pub(super) fn native_read_write_transaction_reads_staged_node_property_updates() {
    let dir = temp_dir("neo4r-native-read-write-tx-read-your-writes");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1)).unwrap();
    db.create_node(
        vec!["Person".to_string()],
        [("name".to_string(), Value::String("Alice".to_string()))]
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
                    "TX_QUERY\t{tx_id}\tMATCH (n:Person) WHERE n.name = $name SET n.status = $status\tname=s:Alice\tstatus=s:active"
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
    assert_eq!(staged_parts[3], "1");
    let rows = decode_query_rows(staged_parts[6]).unwrap();
    assert_eq!(
        rows[0].get("n.status"),
        Some(&neo4r_query::QueryValue::Scalar(Value::String(
            "active".to_string()
        )))
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
    let committed_read = read_native_payload(&mut stream, NativeMessageType::Response, 4);
    let committed_parts = committed_read.split('\t').collect::<Vec<_>>();
    assert_eq!(committed_parts[3], "1");
    let rows = decode_query_rows(committed_parts[6]).unwrap();
    let Some(neo4r_query::QueryValue::Node(node)) = rows[0].get("n") else {
        panic!("expected node row");
    };
    assert!(!node.properties.contains_key("status"));

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
