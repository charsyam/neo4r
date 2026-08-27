use super::*;
use neo4r_core::{
    Command, HybridTimestamp, LogEntry, ShardPlacement, ShardReplica, ShardRoutingTable, Value,
};
use neo4r_db::{QueryAccessPlan, ReadConsistency, TcpShardReplicator};
use neo4r_storage::IndexKind;
use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::sync::{Arc, Barrier};
use std::time::{SystemTime, UNIX_EPOCH};

#[test]
fn tcp_backend_handles_ping_create_query_and_quit() {
    let dir = temp_dir("neo4r-tcp-backend");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1)).unwrap();
    let backend = TcpBackend::new(db);
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let server = thread::spawn(move || backend.serve_listener_once(listener).unwrap());

    let mut stream = TcpStream::connect(addr).unwrap();

    write_frame(
        &mut stream,
        &NativeFrame::new(NativeMessageType::Ping, 1, Vec::new()),
    )
    .unwrap();
    assert_native_response(&mut stream, NativeMessageType::Response, 1, "OK\tPONG");

    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Command,
            2,
            b"CREATE_NODE\tPerson\tname=s:alice\tage=i:42".to_vec(),
        ),
    )
    .unwrap();
    assert_native_response(&mut stream, NativeMessageType::Response, 2, "OK\tNODE\t0");

    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Query,
            3,
            b"MATCH (n:Person) WHERE n.name = \"alice\" RETURN n.name".to_vec(),
        ),
    )
    .unwrap();
    let query_response = read_native_payload(&mut stream, NativeMessageType::Response, 3);
    assert!(query_response.starts_with("OK\tRESULT_START\t"));
    let rows = decode_query_rows(query_response.split('\t').nth(6).unwrap()).unwrap();
    assert_eq!(
        rows[0].get("n.name"),
        Some(&neo4r_query::QueryValue::Scalar(Value::String(
            "alice".to_string()
        )))
    );

    write_frame(
            &mut stream,
            &NativeFrame::new(
                NativeMessageType::Query,
                4,
                b"CREATE (n:Person {name: \"Alice\"}) SET n.status = \"active\" RETURN n.name, n.status"
                    .to_vec(),
            ),
        )
        .unwrap();
    let write_response = read_native_payload(&mut stream, NativeMessageType::Response, 4);
    assert!(write_response.starts_with("OK\tRESULT_START\t"));
    let rows = decode_query_rows(write_response.split('\t').nth(6).unwrap()).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get("n.name"),
        Some(&neo4r_query::QueryValue::Scalar(Value::String(
            "Alice".to_string()
        )))
    );
    assert_eq!(
        rows[0].get("n.status"),
        Some(&neo4r_query::QueryValue::Scalar(Value::String(
            "active".to_string()
        )))
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
fn web_console_serves_index_and_graph_api() {
    let dir = temp_dir("neo4r-web-console");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1)).unwrap();
    db.execute_cypher(r#"CREATE (n:Person {name: "Alice"})"#)
        .unwrap();
    db.execute_cypher(r#"CREATE (n:Person {name: "Bob"})"#)
        .unwrap();
    db.execute_cypher(
        r#"MATCH (a:Person {name: "Alice"}), (b:Person {name: "Bob"}) CREATE (a)-[r:KNOWS {since: 2026}]->(b) RETURN r"#,
    )
    .unwrap();

    let index = web_request(
        TcpBackend::new(db.clone()),
        "GET / HTTP/1.1\r\nhost: localhost\r\n\r\n",
    );
    assert!(index.contains("HTTP/1.1 200 OK"));
    assert!(index.contains("neo4r graph console"));
    assert!(index.contains("three.module.js"));
    assert!(index.contains("id=\"labels\""));
    assert!(index.contains(".graph-label.edge"));
    assert!(index.contains("function nodeLabel"));

    let graph = web_request(
        TcpBackend::new(db.clone()),
        "GET /api/graph?limit=10 HTTP/1.1\r\nhost: localhost\r\n\r\n",
    );
    assert!(graph.contains("HTTP/1.1 200 OK"));
    assert!(graph.contains("\"nodes\""));
    assert!(graph.contains("\"Alice\""));
    assert!(graph.contains("\"relationships\""));
    assert!(graph.contains("\"KNOWS\""));

    let body = "{\"query\":\"MATCH (n:Person) WHERE n.name = \\\"Alice\\\" RETURN n\"}";
    let query = web_request(
        TcpBackend::new(db),
        &format!(
            "POST /api/query HTTP/1.1\r\nhost: localhost\r\ncontent-length: {}\r\n\r\n{}",
            body.len(),
            body
        ),
    );
    assert!(query.contains("HTTP/1.1 200 OK"));
    assert!(query.contains("\"rows\""));

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn native_prepared_query_executes_with_params_and_transactions() {
    let dir = temp_dir("neo4r-native-prepared-query");
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
            b"PREPARE_QUERY\tCREATE (n:Person {name: $name}) RETURN n.name".to_vec(),
        ),
    )
    .unwrap();
    assert_native_response(
        &mut stream,
        NativeMessageType::Response,
        1,
        "OK\tPREPARED_QUERY\t1",
    );

    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Command,
            2,
            b"DESCRIBE_PREPARED\t1".to_vec(),
        ),
    )
    .unwrap();
    assert_native_response(
        &mut stream,
        NativeMessageType::Response,
        2,
        "OK\tPREPARED_QUERY_DESC\t1\tWRITE\tWRITE_SHARD_BY_PARAM\t1\tname",
    );

    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Command,
            3,
            b"PREPARED_QUERY_ROUTE\t1\tname=s:Alice".to_vec(),
        ),
    )
    .unwrap();
    let route = read_native_payload(&mut stream, NativeMessageType::Response, 3);
    assert!(
        route == "OK\tPREPARED_QUERY_ROUTE\t1\tWRITE_SHARD:0"
            || route == "OK\tPREPARED_QUERY_ROUTE\t1\tWRITE_SHARD:1"
    );

    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Command,
            4,
            b"EXECUTE_PREPARED\t1\tname=s:Alice".to_vec(),
        ),
    )
    .unwrap();
    let start = read_native_payload(&mut stream, NativeMessageType::Response, 4);
    let parts = start.split('\t').collect::<Vec<_>>();
    assert_eq!(parts[0..3], ["OK", "RESULT_START", "1"]);
    assert_eq!(parts[3], "1");
    let rows = decode_query_rows(parts[6]).unwrap();
    assert_eq!(
        rows[0].get("n.name"),
        Some(&neo4r_query::QueryValue::Scalar(Value::String(
            "Alice".to_string()
        )))
    );

    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Command,
            5,
            b"PREPARE_QUERY\tMATCH (n:Person) WHERE n.name = $name SET n.status = $status".to_vec(),
        ),
    )
    .unwrap();
    assert_native_response(
        &mut stream,
        NativeMessageType::Response,
        5,
        "OK\tPREPARED_QUERY\t2",
    );

    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Command,
            6,
            b"DESCRIBE_PREPARED\t2".to_vec(),
        ),
    )
    .unwrap();
    assert_native_response(
        &mut stream,
        NativeMessageType::Response,
        6,
        "OK\tPREPARED_QUERY_DESC\t2\tWRITE\tWRITE_TARGET_DYNAMIC\t2\tname,status",
    );

    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Command,
            7,
            b"BEGIN_TX\tREAD_WRITE".to_vec(),
        ),
    )
    .unwrap();
    let begin = read_native_payload(&mut stream, NativeMessageType::Response, 7);
    let tx_id = begin.split('\t').collect::<Vec<_>>()[2]
        .parse::<u64>()
        .unwrap();

    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Command,
            8,
            format!("TX_PREPARED_QUERY_ROUTE\t{tx_id}\t2\tname=s:Alice\tstatus=s:active")
                .into_bytes(),
        ),
    )
    .unwrap();
    let tx_route = read_native_payload(&mut stream, NativeMessageType::Response, 8);
    assert_eq!(
            tx_route,
            format!(
                "OK\tTX_PREPARED_QUERY_ROUTE\t{tx_id}\t2\tWRITE_TARGET_DYNAMIC\ttx_mode=READ_WRITE tx_isolation=SNAPSHOT staged_writes=0 staged_overlay=none"
            )
        );

    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Command,
            9,
            format!("TX_EXECUTE_PREPARED\t{tx_id}\t2\tname=s:Alice\tstatus=s:active").into_bytes(),
        ),
    )
    .unwrap();
    assert_native_response(
        &mut stream,
        NativeMessageType::Response,
        9,
        &format!("OK\tTX_STAGED\t{tx_id}\t1"),
    );

    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Command,
            10,
            format!("COMMIT_TX\t{tx_id}").into_bytes(),
        ),
    )
    .unwrap();
    assert_native_response(
        &mut stream,
        NativeMessageType::Response,
        10,
        &format!("OK\tTX_COMMIT\t{tx_id}\t1"),
    );

    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Query,
            11,
            b"MATCH (n:Person) WHERE n.name = $name RETURN n.status\tname=s:Alice".to_vec(),
        ),
    )
    .unwrap();
    let start = read_native_payload(&mut stream, NativeMessageType::Response, 11);
    let parts = start.split('\t').collect::<Vec<_>>();
    let rows = decode_query_rows(parts[6]).unwrap();
    assert_eq!(
        rows[0].get("n.status"),
        Some(&neo4r_query::QueryValue::Scalar(Value::String(
            "active".to_string()
        )))
    );

    write_frame(
        &mut stream,
        &NativeFrame::new(NativeMessageType::Command, 12, b"LIST_PREPARED".to_vec()),
    )
    .unwrap();
    let list = read_native_payload(&mut stream, NativeMessageType::Response, 12);
    assert!(list.starts_with("OK\tPREPARED_QUERY_LIST\t2\t"));
    assert!(list.contains("1:CREATE (n:Person {name: $name}) RETURN n.name"));

    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Command,
            13,
            b"CLOSE_PREPARED\t1".to_vec(),
        ),
    )
    .unwrap();
    assert_native_response(
        &mut stream,
        NativeMessageType::Response,
        13,
        "OK\tPREPARED_QUERY_CLOSED\t1",
    );

    write_frame(
        &mut stream,
        &NativeFrame::new(NativeMessageType::Quit, 14, Vec::new()),
    )
    .unwrap();
    assert_native_response(&mut stream, NativeMessageType::Response, 14, "OK\tBYE");

    server.join().unwrap();
    let _ = fs::remove_dir_all(dir);
}

#[test]
fn native_prepared_query_rejects_missing_params_before_execution() {
    let dir = temp_dir("neo4r-native-prepared-query-missing-params");
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
            b"PREPARE_QUERY\tCREATE (n:Person {name: $name, tenant: $tenant}) RETURN n.name"
                .to_vec(),
        ),
    )
    .unwrap();
    assert_native_response(
        &mut stream,
        NativeMessageType::Response,
        1,
        "OK\tPREPARED_QUERY\t1",
    );

    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Command,
            2,
            b"EXECUTE_PREPARED\t1\tname=s:Alice".to_vec(),
        ),
    )
    .unwrap();
    let response = read_native_payload(&mut stream, NativeMessageType::Error, 2);
    assert!(response.contains("prepared query 1 missing parameter(s): tenant"));

    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Command,
            3,
            b"PREPARED_QUERY_PLAN\t1\ttenant=s:acme".to_vec(),
        ),
    )
    .unwrap();
    let response = read_native_payload(&mut stream, NativeMessageType::Error, 3);
    assert!(response.contains("prepared query 1 missing parameter(s): name"));

    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Command,
            4,
            b"BEGIN_TX\tREAD_WRITE".to_vec(),
        ),
    )
    .unwrap();
    let begin = read_native_payload(&mut stream, NativeMessageType::Response, 4);
    let tx_id = begin.split('\t').collect::<Vec<_>>()[2]
        .parse::<u64>()
        .unwrap();

    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Command,
            5,
            format!("TX_EXECUTE_PREPARED\t{tx_id}\t1\tname=s:Alice").into_bytes(),
        ),
    )
    .unwrap();
    let response = read_native_payload(&mut stream, NativeMessageType::Error, 5);
    assert!(response.contains("prepared query 1 missing parameter(s): tenant"));

    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Command,
            6,
            format!("TX_PREPARED_QUERY_PLAN\t{tx_id}\t1\tname=s:Alice").into_bytes(),
        ),
    )
    .unwrap();
    let response = read_native_payload(&mut stream, NativeMessageType::Error, 6);
    assert!(response.contains("prepared query 1 missing parameter(s): tenant"));

    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Command,
            7,
            format!("TX_STATUS\t{tx_id}").into_bytes(),
        ),
    )
    .unwrap();
    assert_native_response(
        &mut stream,
        NativeMessageType::Response,
        7,
        &format!("OK\tTX_STATUS\t{tx_id}\tREAD_WRITE\tSNAPSHOT\t0"),
    );

    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Command,
            8,
            format!("ROLLBACK_TX\t{tx_id}").into_bytes(),
        ),
    )
    .unwrap();
    assert_native_response(
        &mut stream,
        NativeMessageType::Response,
        8,
        &format!("OK\tTX_ROLLBACK\t{tx_id}"),
    );
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
fn native_prepared_query_plan_reports_access_path_and_tx_context() {
    let dir = temp_dir("neo4r-native-prepared-query-plan");
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
            b"PREPARE_QUERY\tMATCH (n:Person {name: $name}) RETURN n".to_vec(),
        ),
    )
    .unwrap();
    assert_native_response(
        &mut stream,
        NativeMessageType::Response,
        1,
        "OK\tPREPARED_QUERY\t1",
    );

    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Command,
            2,
            b"PREPARED_QUERY_PLAN\t1\tname=s:Alice".to_vec(),
        ),
    )
    .unwrap();
    let plan = read_native_payload(&mut stream, NativeMessageType::Response, 2);
    assert!(plan.starts_with("OK\tQUERY_PLAN\t"));
    assert!(plan.contains("route=local"));
    assert!(plan.contains("access=node_index_seek(Person.name)"));

    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Command,
            3,
            b"BEGIN_TX\tREAD_WRITE\tSNAPSHOT".to_vec(),
        ),
    )
    .unwrap();
    let begin = read_native_payload(&mut stream, NativeMessageType::Response, 3);
    let tx_id = begin.split('\t').collect::<Vec<_>>()[2]
        .parse::<u64>()
        .unwrap();

    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Command,
            4,
            format!("TX_PREPARED_QUERY_PLAN\t{tx_id}\t1\tname=s:Alice").into_bytes(),
        ),
    )
    .unwrap();
    let tx_plan = read_native_payload(&mut stream, NativeMessageType::Response, 4);
    assert!(tx_plan.starts_with("OK\tQUERY_PLAN\t"));
    assert!(tx_plan.contains("access=node_index_seek(Person.name)"));
    assert!(tx_plan.contains("tx_mode=READ_WRITE"));
    assert!(tx_plan.contains("tx_isolation=SNAPSHOT"));
    assert!(tx_plan.contains("staged_writes=0"));
    assert!(tx_plan.contains("staged_overlay=none"));

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
fn native_prepared_query_is_session_scoped() {
    let dir = temp_dir("neo4r-native-prepared-query-session-scope");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1)).unwrap();
    let backend = TcpBackend::with_config(
        db,
        TcpBackendConfig {
            worker_count: 2,
            queue_capacity: 8,
            default_page_size: 10,
            read_preference: QueryReadPreference::Primary,
            catch_up_connect_timeout: Duration::from_secs(1),
        },
    );
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let server = thread::spawn(move || {
        let mut handlers = Vec::new();
        for _ in 0..2 {
            let (stream, _) = listener.accept().unwrap();
            let backend = backend.clone();
            handlers.push(thread::spawn(move || {
                backend.handle_stream(stream).unwrap()
            }));
        }
        for handler in handlers {
            handler.join().unwrap();
        }
    });

    let mut owner = TcpStream::connect(addr).unwrap();
    write_frame(
        &mut owner,
        &NativeFrame::new(
            NativeMessageType::Command,
            1,
            b"PREPARE_QUERY\tCREATE (n:Person {name: $name}) RETURN n.name".to_vec(),
        ),
    )
    .unwrap();
    assert_native_response(
        &mut owner,
        NativeMessageType::Response,
        1,
        "OK\tPREPARED_QUERY\t1",
    );

    let mut other = TcpStream::connect(addr).unwrap();
    write_frame(
        &mut other,
        &NativeFrame::new(
            NativeMessageType::Command,
            1,
            b"EXECUTE_PREPARED\t1\tname=s:Eve".to_vec(),
        ),
    )
    .unwrap();
    let response = read_native_payload(&mut other, NativeMessageType::Error, 1);
    assert!(response.contains("prepared query 1 belongs to another session"));

    write_frame(
        &mut owner,
        &NativeFrame::new(
            NativeMessageType::Command,
            2,
            b"EXECUTE_PREPARED\t1\tname=s:Alice".to_vec(),
        ),
    )
    .unwrap();
    let start = read_native_payload(&mut owner, NativeMessageType::Response, 2);
    let parts = start.split('\t').collect::<Vec<_>>();
    assert_eq!(parts[0..3], ["OK", "RESULT_START", "1"]);
    let rows = decode_query_rows(parts[6]).unwrap();
    assert_eq!(
        rows[0].get("n.name"),
        Some(&neo4r_query::QueryValue::Scalar(Value::String(
            "Alice".to_string()
        )))
    );

    write_frame(
        &mut other,
        &NativeFrame::new(NativeMessageType::Quit, 2, Vec::new()),
    )
    .unwrap();
    assert_native_response(&mut other, NativeMessageType::Response, 2, "OK\tBYE");
    write_frame(
        &mut owner,
        &NativeFrame::new(NativeMessageType::Quit, 3, Vec::new()),
    )
    .unwrap();
    assert_native_response(&mut owner, NativeMessageType::Response, 3, "OK\tBYE");

    server.join().unwrap();
    let _ = fs::remove_dir_all(dir);
}

#[test]
fn tcp_backend_reports_parse_errors() {
    let dir = temp_dir("neo4r-tcp-parse-error");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1)).unwrap();
    let backend = TcpBackend::new(db);
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let server = thread::spawn(move || backend.serve_listener_once(listener).unwrap());

    let mut stream = TcpStream::connect(addr).unwrap();
    write_frame(
        &mut stream,
        &NativeFrame::new(NativeMessageType::Command, 1, b"CREATE_NODE".to_vec()),
    )
    .unwrap();
    let response = read_native_payload(&mut stream, NativeMessageType::Error, 1);
    assert!(response.starts_with("ERR\tCREATE_NODE requires labels"));

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
fn backend_distributed_query_fans_out_to_remote_shards() {
    let local_dir = temp_dir("neo4r-distributed-query-local");
    let remote_dir = temp_dir("neo4r-distributed-query-remote");
    let routing_table = ShardRoutingTable {
        version: 3,
        placements: vec![
            ShardPlacement::new(0, vec![ShardReplica::primary(1)]),
            ShardPlacement::new(1, vec![ShardReplica::primary(2)]),
        ],
    };
    let local_db = Neo4rDatabaseHandle::open(
        DatabaseConfig::new(&local_dir, 2, 2)
            .with_server_id(1)
            .with_routing_table(routing_table.clone()),
    )
    .unwrap();
    local_db
        .create_node(
            vec!["Person".to_string()],
            [("name".to_string(), Value::String("Alice".to_string()))]
                .into_iter()
                .collect(),
        )
        .unwrap();
    let remote_db = Neo4rDatabaseHandle::open(
        DatabaseConfig::new(&remote_dir, 2, 2)
            .with_server_id(2)
            .with_routing_table(routing_table),
    )
    .unwrap();
    remote_db
        .apply_replicated_entry(LogEntry::new_with_metadata(
            1,
            1,
            1,
            2,
            3,
            HybridTimestamp::new(1, 0),
            Command::CreateNode {
                id: 1,
                labels: vec!["Person".to_string()],
                properties: [("name".to_string(), Value::String("Bob".to_string()))]
                    .into_iter()
                    .collect(),
            },
        ))
        .unwrap();
    remote_db
        .apply_replicated_entry(LogEntry::new_with_metadata(
            1,
            1,
            2,
            2,
            3,
            HybridTimestamp::new(2, 0),
            Command::CreateNode {
                id: 3,
                labels: vec!["Person".to_string()],
                properties: [("name".to_string(), Value::String("Carol".to_string()))]
                    .into_iter()
                    .collect(),
            },
        ))
        .unwrap();

    let remote_backend = TcpBackend::with_config(
        remote_db.clone(),
        TcpBackendConfig {
            default_page_size: 1,
            ..TcpBackendConfig::default()
        },
    );
    let remote_listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let remote_addr = remote_listener.local_addr().unwrap();
    let remote_server =
        thread::spawn(move || remote_backend.serve_listener_once(remote_listener).unwrap());

    let local_backend = TcpBackend::new(local_db.clone());
    local_backend
        .register_query_peer(2, remote_addr.to_string())
        .unwrap();
    let response = local_backend.execute_backend_request(
        parse_request("QUERY_DISTRIBUTED\tMATCH (n:Person) RETURN n.name").unwrap(),
    );

    let BackendResponse::OkRows { count, debug_rows } = response else {
        panic!("expected distributed rows");
    };
    assert_eq!(count, 3);
    let mut names = decode_query_rows(&debug_rows)
        .unwrap()
        .into_iter()
        .filter_map(|row| match row.get("n.name") {
            Some(neo4r_query::QueryValue::Scalar(Value::String(name))) => Some(name.clone()),
            _ => None,
        })
        .collect::<Vec<_>>();
    names.sort();
    assert_eq!(
        names,
        vec!["Alice".to_string(), "Bob".to_string(), "Carol".to_string()]
    );

    remote_server.join().unwrap();
    drop(local_backend);
    drop(local_db);
    drop(remote_db);
    let _ = fs::remove_dir_all(local_dir);
    let _ = fs::remove_dir_all(remote_dir);
}

#[test]
fn distributed_query_primary_preference_requires_primary_peer() {
    let local_dir = temp_dir("neo4r-distributed-primary-preference");
    let routing_table = ShardRoutingTable {
        version: 4,
        placements: vec![
            ShardPlacement::new(0, vec![ShardReplica::primary(1)]),
            ShardPlacement::new(1, vec![ShardReplica::primary(2), ShardReplica::replica(3)]),
        ],
    };
    let local_db = Neo4rDatabaseHandle::open(
        DatabaseConfig::new(&local_dir, 2, 2)
            .with_server_id(1)
            .with_routing_table(routing_table),
    )
    .unwrap();
    let local_backend = TcpBackend::new(local_db.clone());
    local_backend
        .register_query_peer(3, "127.0.0.1:9".to_string())
        .unwrap();

    let response = local_backend.execute_backend_request(
        parse_request("QUERY_DISTRIBUTED\tMATCH (n:Person) RETURN n.name").unwrap(),
    );

    assert!(matches!(
        response,
        BackendResponse::Err(message)
            if message.contains("missing query peer address for primary server 2")
    ));

    drop(local_backend);
    drop(local_db);
    let _ = fs::remove_dir_all(local_dir);
}

#[test]
fn distributed_query_prefer_replica_uses_replica_peer() {
    let local_dir = temp_dir("neo4r-distributed-prefer-replica-local");
    let replica_dir = temp_dir("neo4r-distributed-prefer-replica-remote");
    let routing_table = ShardRoutingTable {
        version: 4,
        placements: vec![
            ShardPlacement::new(0, vec![ShardReplica::primary(1)]),
            ShardPlacement::new(1, vec![ShardReplica::primary(2), ShardReplica::replica(3)]),
        ],
    };
    let local_db = Neo4rDatabaseHandle::open(
        DatabaseConfig::new(&local_dir, 2, 2)
            .with_server_id(1)
            .with_routing_table(routing_table.clone()),
    )
    .unwrap();
    let replica_db = Neo4rDatabaseHandle::open(
        DatabaseConfig::new(&replica_dir, 2, 2)
            .with_server_id(3)
            .with_routing_table(routing_table),
    )
    .unwrap();
    replica_db
        .apply_replicated_entry(LogEntry::new_with_metadata(
            1,
            1,
            1,
            2,
            4,
            HybridTimestamp::new(1, 0),
            Command::CreateNode {
                id: 1,
                labels: vec!["Person".to_string()],
                properties: [("name".to_string(), Value::String("ReplicaBob".to_string()))]
                    .into_iter()
                    .collect(),
            },
        ))
        .unwrap();

    let replica_backend = TcpBackend::new(replica_db.clone());
    let replica_listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let replica_addr = replica_listener.local_addr().unwrap();
    let replica_server = thread::spawn(move || {
        replica_backend
            .serve_listener_once(replica_listener)
            .unwrap()
    });
    let local_backend = TcpBackend::with_config(
        local_db.clone(),
        TcpBackendConfig {
            read_preference: QueryReadPreference::PreferReplica,
            ..TcpBackendConfig::default()
        },
    );
    local_backend
        .register_query_peer(3, replica_addr.to_string())
        .unwrap();

    let response = local_backend.execute_backend_request(
        parse_request("QUERY_DISTRIBUTED\tMATCH (n:Person) RETURN n.name").unwrap(),
    );

    let BackendResponse::OkRows { count, debug_rows } = response else {
        panic!("expected replica rows");
    };
    assert_eq!(count, 1);
    let rows = decode_query_rows(&debug_rows).unwrap();
    assert_eq!(
        rows[0].get("n.name"),
        Some(&neo4r_query::QueryValue::Scalar(Value::String(
            "ReplicaBob".to_string()
        )))
    );

    replica_server.join().unwrap();
    drop(local_backend);
    drop(local_db);
    drop(replica_db);
    let _ = fs::remove_dir_all(local_dir);
    let _ = fs::remove_dir_all(replica_dir);
}

#[test]
fn native_command_forwards_shard_write_to_remote_primary() {
    let local_dir = temp_dir("neo4r-forward-write-local");
    let remote_dir = temp_dir("neo4r-forward-write-remote");
    let routing_table = ShardRoutingTable {
        version: 5,
        placements: vec![
            ShardPlacement::new(0, vec![ShardReplica::primary(1)]),
            ShardPlacement::new(1, vec![ShardReplica::primary(2), ShardReplica::replica(1)]),
        ],
    };
    let local_db = Neo4rDatabaseHandle::open(
        DatabaseConfig::new(&local_dir, 2, 2)
            .with_server_id(1)
            .with_routing_table(routing_table.clone()),
    )
    .unwrap();
    let remote_db = Neo4rDatabaseHandle::open(
        DatabaseConfig::new(&remote_dir, 2, 2)
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
            NativeMessageType::Command,
            1,
            b"CREATE_NODE_SHARD\t1\tPerson\tname=s:RemoteAlice".to_vec(),
        ),
    )
    .unwrap();
    assert_native_response(&mut stream, NativeMessageType::Response, 1, "OK\tNODE\t1");

    write_frame(
        &mut stream,
        &NativeFrame::new(NativeMessageType::Quit, 2, Vec::new()),
    )
    .unwrap();
    assert_native_response(&mut stream, NativeMessageType::Response, 2, "OK\tBYE");

    local_server.join().unwrap();
    remote_server.join().unwrap();
    let rows = remote_db
        .query_shard(
            1,
            r#"MATCH (n:Person) WHERE n.name = "RemoteAlice" RETURN n.name"#,
        )
        .unwrap();
    assert_eq!(rows.len(), 1);

    drop(local_db);
    drop(remote_db);
    let _ = fs::remove_dir_all(local_dir);
    let _ = fs::remove_dir_all(remote_dir);
}

#[test]
fn native_command_forwards_relationship_cud_to_remote_primary() {
    let local_dir = temp_dir("neo4r-forward-rel-cud-local");
    let remote_dir = temp_dir("neo4r-forward-rel-cud-remote");
    let routing_table = ShardRoutingTable {
        version: 11,
        placements: vec![ShardPlacement::new(
            0,
            vec![ShardReplica::primary(2), ShardReplica::replica(1)],
        )],
    };
    let local_db =
        Neo4rDatabaseHandle::open(DatabaseConfig::new(&local_dir, 1, 1).with_server_id(1)).unwrap();
    local_db
        .install_routing_table(routing_table.clone())
        .unwrap();
    let remote_db = Neo4rDatabaseHandle::open(
        DatabaseConfig::new(&remote_dir, 1, 1)
            .with_server_id(2)
            .with_routing_table(routing_table),
    )
    .unwrap();
    let entries = vec![
        LogEntry::new_with_metadata(
            0,
            1,
            1,
            2,
            11,
            HybridTimestamp::new(1, 0),
            Command::CreateNode {
                id: 0,
                labels: vec!["Person".to_string()],
                properties: [("name".to_string(), Value::String("Alice".to_string()))]
                    .into_iter()
                    .collect(),
            },
        ),
        LogEntry::new_with_metadata(
            0,
            1,
            2,
            2,
            11,
            HybridTimestamp::new(2, 0),
            Command::CreateNode {
                id: 1,
                labels: vec!["Person".to_string()],
                properties: [("name".to_string(), Value::String("Bob".to_string()))]
                    .into_iter()
                    .collect(),
            },
        ),
        LogEntry::new_with_metadata(
            0,
            1,
            3,
            2,
            11,
            HybridTimestamp::new(3, 0),
            Command::CreateRelationship {
                id: 0,
                from: 0,
                to: 1,
                rel_type: "KNOWS".to_string(),
                properties: Default::default(),
            },
        ),
    ];
    for entry in entries {
        remote_db.apply_replicated_entry(entry.clone()).unwrap();
        local_db.apply_replicated_entry(entry).unwrap();
    }

    let remote_backend = TcpBackend::new(remote_db.clone());
    let remote_listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let remote_addr = remote_listener.local_addr().unwrap();
    let remote_server = thread::spawn(move || {
        for _ in 0..2 {
            let (stream, _) = remote_listener.accept().unwrap();
            remote_backend.handle_native_stream(stream).unwrap();
        }
    });
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
            NativeMessageType::Command,
            1,
            b"SET_RELATIONSHIP_PROPERTY\t0\tsince\ti:2026".to_vec(),
        ),
    )
    .unwrap();
    assert_native_response(&mut stream, NativeMessageType::Response, 1, "OK");
    assert_eq!(
        remote_db
            .query("MATCH (a:Person)-[r:KNOWS]->(b:Person) WHERE r.since = 2026 RETURN r.since")
            .unwrap()
            .len(),
        1
    );

    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Command,
            2,
            b"DELETE_RELATIONSHIP\t0".to_vec(),
        ),
    )
    .unwrap();
    assert_native_response(&mut stream, NativeMessageType::Response, 2, "OK");

    write_frame(
        &mut stream,
        &NativeFrame::new(NativeMessageType::Quit, 3, Vec::new()),
    )
    .unwrap();
    assert_native_response(&mut stream, NativeMessageType::Response, 3, "OK\tBYE");

    local_server.join().unwrap();
    remote_server.join().unwrap();
    assert_eq!(
        remote_db
            .query("MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN r")
            .unwrap()
            .len(),
        0
    );

    drop(local_db);
    drop(remote_db);
    let _ = fs::remove_dir_all(local_dir);
    let _ = fs::remove_dir_all(remote_dir);
}

#[test]
fn native_command_forwards_node_label_cud_to_remote_primary() {
    let local_dir = temp_dir("neo4r-forward-label-cud-local");
    let remote_dir = temp_dir("neo4r-forward-label-cud-remote");
    let routing_table = ShardRoutingTable {
        version: 13,
        placements: vec![ShardPlacement::new(
            0,
            vec![ShardReplica::primary(2), ShardReplica::replica(1)],
        )],
    };
    let local_db =
        Neo4rDatabaseHandle::open(DatabaseConfig::new(&local_dir, 1, 1).with_server_id(1)).unwrap();
    local_db
        .install_routing_table(routing_table.clone())
        .unwrap();
    let remote_db = Neo4rDatabaseHandle::open(
        DatabaseConfig::new(&remote_dir, 1, 1)
            .with_server_id(2)
            .with_routing_table(routing_table),
    )
    .unwrap();
    let entry = LogEntry::new_with_metadata(
        0,
        1,
        1,
        2,
        13,
        HybridTimestamp::new(1, 0),
        Command::CreateNode {
            id: 0,
            labels: vec!["Person".to_string()],
            properties: [("name".to_string(), Value::String("Alice".to_string()))]
                .into_iter()
                .collect(),
        },
    );
    remote_db.apply_replicated_entry(entry.clone()).unwrap();
    local_db.apply_replicated_entry(entry).unwrap();

    let remote_backend = TcpBackend::new(remote_db.clone());
    let remote_listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let remote_addr = remote_listener.local_addr().unwrap();
    let remote_server = thread::spawn(move || {
        for _ in 0..2 {
            let (stream, _) = remote_listener.accept().unwrap();
            remote_backend.handle_native_stream(stream).unwrap();
        }
    });
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
            NativeMessageType::Command,
            1,
            b"ADD_NODE_LABEL\t0\tEmployee".to_vec(),
        ),
    )
    .unwrap();
    assert_native_response(&mut stream, NativeMessageType::Response, 1, "OK");
    assert_eq!(
        remote_db
            .query(r#"MATCH (n:Employee) WHERE n.name = "Alice" RETURN n.name"#)
            .unwrap()
            .len(),
        1
    );

    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Command,
            2,
            b"REMOVE_NODE_LABEL\t0\tPerson".to_vec(),
        ),
    )
    .unwrap();
    assert_native_response(&mut stream, NativeMessageType::Response, 2, "OK");

    write_frame(
        &mut stream,
        &NativeFrame::new(NativeMessageType::Quit, 3, Vec::new()),
    )
    .unwrap();
    assert_native_response(&mut stream, NativeMessageType::Response, 3, "OK\tBYE");

    local_server.join().unwrap();
    remote_server.join().unwrap();
    assert!(remote_db
        .query(r#"MATCH (n:Person) WHERE n.name = "Alice" RETURN n.name"#)
        .unwrap()
        .is_empty());

    drop(local_db);
    drop(remote_db);
    let _ = fs::remove_dir_all(local_dir);
    let _ = fs::remove_dir_all(remote_dir);
}

#[test]
fn native_command_forwards_index_catalog_writes_to_metadata_primary() {
    let local_dir = temp_dir("neo4r-forward-index-local");
    let remote_dir = temp_dir("neo4r-forward-index-remote");
    let routing_table = ShardRoutingTable {
        version: 12,
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
            NativeMessageType::Command,
            1,
            b"CREATE_VECTOR_INDEX\tdoc_embedding\tDocument\tembedding\t2\tcosine".to_vec(),
        ),
    )
    .unwrap();
    assert_native_response(&mut stream, NativeMessageType::Response, 1, "OK");

    write_frame(
        &mut stream,
        &NativeFrame::new(NativeMessageType::Quit, 2, Vec::new()),
    )
    .unwrap();
    assert_native_response(&mut stream, NativeMessageType::Response, 2, "OK\tBYE");

    local_server.join().unwrap();
    remote_server.join().unwrap();
    assert!(local_db.list_indexes().unwrap().is_empty());
    assert_eq!(remote_db.list_indexes().unwrap().len(), 1);

    drop(local_db);
    drop(remote_db);
    let _ = fs::remove_dir_all(local_dir);
    let _ = fs::remove_dir_all(remote_dir);
}

#[test]
fn native_command_syncs_index_catalog_from_peer() {
    let local_dir = temp_dir("neo4r-sync-index-local");
    let remote_dir = temp_dir("neo4r-sync-index-remote");
    let routing_table = ShardRoutingTable {
        version: 13,
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
    local_db
        .apply_replicated_entry(LogEntry::new_with_metadata(
            0,
            1,
            1,
            2,
            13,
            HybridTimestamp::new(1, 0),
            Command::CreateNode {
                id: 0,
                labels: vec!["Document".to_string()],
                properties: [
                    ("title".to_string(), Value::String("local".to_string())),
                    ("embedding".to_string(), Value::Vector(vec![1.0, 0.0])),
                ]
                .into_iter()
                .collect(),
            },
        ))
        .unwrap();
    let remote_db = Neo4rDatabaseHandle::open(
        DatabaseConfig::new(&remote_dir, 1, 1)
            .with_server_id(2)
            .with_routing_table(routing_table),
    )
    .unwrap();
    remote_db
        .create_vector_index("doc_embedding", "Document", "embedding", 2, "cosine")
        .unwrap();
    assert!(local_db.list_indexes().unwrap().is_empty());

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
            NativeMessageType::Command,
            1,
            b"SYNC_INDEX_CATALOG_FROM_PEER\t2".to_vec(),
        ),
    )
    .unwrap();
    assert_native_response(&mut stream, NativeMessageType::Response, 1, "OK");

    write_frame(
        &mut stream,
        &NativeFrame::new(NativeMessageType::Quit, 2, Vec::new()),
    )
    .unwrap();
    assert_native_response(&mut stream, NativeMessageType::Response, 2, "OK\tBYE");

    local_server.join().unwrap();
    remote_server.join().unwrap();
    assert_eq!(
        local_db.list_indexes().unwrap(),
        remote_db.list_indexes().unwrap()
    );
    assert_eq!(
            local_db
                .query(
                    "MATCH (n:Document) WHERE vector.knn(n.embedding, [1.0, 0.0], 1) RETURN n.title",
                )
                .unwrap()
                .len(),
            1
        );
    assert_eq!(
        local_db
            .query_plan(
                "MATCH (n:Document) WHERE vector.knn(n.embedding, [1.0, 0.0], 1) RETURN n.title",
            )
            .unwrap()
            .access_plan,
        QueryAccessPlan::VectorIndexSeek {
            label: Some("Document".to_string()),
            property: "embedding".to_string(),
            metric: "cosine".to_string(),
        }
    );
    let status = local_db
        .execute_cypher("SHOW VECTOR INDEX STATUS doc_embedding")
        .unwrap();
    assert_eq!(status.len(), 1);
    assert_eq!(
        status[0].get("entries"),
        Some(&neo4r_query::QueryValue::Scalar(Value::Int(1)))
    );

    drop(local_db);
    drop(remote_db);
    let reopened_local = Neo4rDatabaseHandle::open(
        DatabaseConfig::new(&local_dir, 1, 1)
            .with_server_id(1)
            .with_routing_table(ShardRoutingTable {
                version: 13,
                placements: vec![ShardPlacement::new(
                    0,
                    vec![ShardReplica::primary(2), ShardReplica::replica(1)],
                )],
            }),
    )
    .unwrap();
    assert_eq!(
        reopened_local
            .query_plan(
                "MATCH (n:Document) WHERE vector.knn(n.embedding, [1.0, 0.0], 1) RETURN n.title",
            )
            .unwrap()
            .access_plan,
        QueryAccessPlan::VectorIndexSeek {
            label: Some("Document".to_string()),
            property: "embedding".to_string(),
            metric: "cosine".to_string(),
        }
    );
    assert_eq!(
            reopened_local
                .query(
                    "MATCH (n:Document) WHERE vector.knn(n.embedding, [1.0, 0.0], 1) RETURN n.title",
                )
                .unwrap()
                .len(),
            1
        );
    let status = reopened_local
        .execute_cypher("SHOW VECTOR INDEX STATUS doc_embedding")
        .unwrap();
    assert_eq!(status.len(), 1);
    assert_eq!(
        status[0].get("entries"),
        Some(&neo4r_query::QueryValue::Scalar(Value::Int(1)))
    );
    drop(reopened_local);
    let _ = fs::remove_dir_all(local_dir);
    let _ = fs::remove_dir_all(remote_dir);
}

#[test]
fn native_command_rebuilds_vector_indexes() {
    let dir = temp_dir("neo4r-native-rebuild-vector-indexes");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1)).unwrap();
    let backend = TcpBackend::new(db);
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let server = thread::spawn(move || backend.serve_listener_once(listener).unwrap());

    let mut stream = TcpStream::connect(addr).unwrap();
    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Command,
            1,
            b"CREATE_VECTOR_INDEX\tdoc_embedding\tDocument\tembedding\t2\tcosine".to_vec(),
        ),
    )
    .unwrap();
    assert_native_response(&mut stream, NativeMessageType::Response, 1, "OK");

    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Command,
            2,
            b"CREATE_NODE\tDocument\ttitle=s:near\tembedding=v:1.0,0.0".to_vec(),
        ),
    )
    .unwrap();
    assert_native_response(&mut stream, NativeMessageType::Response, 2, "OK\tNODE\t0");

    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Command,
            3,
            b"CREATE_NODE\tDocument\ttitle=s:far\tembedding=v:0.0,1.0".to_vec(),
        ),
    )
    .unwrap();
    assert_native_response(&mut stream, NativeMessageType::Response, 3, "OK\tNODE\t1");

    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Command,
            4,
            b"REBUILD_VECTOR_INDEXES".to_vec(),
        ),
    )
    .unwrap();
    assert_native_response(&mut stream, NativeMessageType::Response, 4, "OK");

    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Command,
            5,
            b"VECTOR_INDEX_STATUS".to_vec(),
        ),
    )
    .unwrap();
    let status = read_native_payload(&mut stream, NativeMessageType::Response, 5);
    assert!(status.starts_with("OK\tVECTOR_INDEX_STATUS\t"));
    assert!(status.contains("doc_embedding:Document:embedding"));
    assert!(status.contains("dimensions=2"));
    assert!(status.contains("metric=cosine"));
    assert!(status.contains("entries=2"));

    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Command,
            6,
            b"VECTOR_INDEX_STATUS\tdoc_embedding".to_vec(),
        ),
    )
    .unwrap();
    assert_native_response(
            &mut stream,
            NativeMessageType::Response,
            6,
            "OK\tVECTOR_INDEX_STATUS\tdoc_embedding:Document:embedding:dimensions=2:metric=cosine:entries=2",
        );

    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Command,
            7,
            b"REBUILD_VECTOR_INDEX\tdoc_embedding".to_vec(),
        ),
    )
    .unwrap();
    assert_native_response(&mut stream, NativeMessageType::Response, 7, "OK");

    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Query,
            8,
            b"MATCH (n:Document) WHERE vector.knn(n.embedding, [1.0, 0.0], 1) RETURN n.title"
                .to_vec(),
        ),
    )
    .unwrap();
    let response = read_native_payload(&mut stream, NativeMessageType::Response, 8);
    assert!(response.starts_with("OK\tRESULT_START\t"));
    let rows = decode_query_rows(response.split('\t').nth(6).unwrap()).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get("n.title"),
        Some(&neo4r_query::QueryValue::Scalar(Value::String(
            "near".to_string()
        )))
    );

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
fn native_vector_index_status_survives_reopen() {
    let dir = temp_dir("neo4r-native-vector-status-reopen");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1)).unwrap();
    db.create_vector_index("doc_embedding", "Document", "embedding", 2, "cosine")
        .unwrap();
    db.create_node(
        vec!["Document".to_string()],
        [
            ("title".to_string(), Value::String("near".to_string())),
            (
                "embedding".to_string(),
                Value::Vector(vec![1.0_f32, 0.0_f32]),
            ),
        ]
        .into_iter()
        .collect(),
    )
    .unwrap();
    db.create_node(
        vec!["Document".to_string()],
        [
            ("title".to_string(), Value::String("far".to_string())),
            (
                "embedding".to_string(),
                Value::Vector(vec![0.0_f32, 1.0_f32]),
            ),
        ]
        .into_iter()
        .collect(),
    )
    .unwrap();
    drop(db);

    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1)).unwrap();
    let backend = TcpBackend::new(db);
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let server = thread::spawn(move || backend.serve_listener_once(listener).unwrap());

    let mut stream = TcpStream::connect(addr).unwrap();
    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Command,
            1,
            b"VECTOR_INDEX_STATUS\tdoc_embedding".to_vec(),
        ),
    )
    .unwrap();
    assert_native_response(
            &mut stream,
            NativeMessageType::Response,
            1,
            "OK\tVECTOR_INDEX_STATUS\tdoc_embedding:Document:embedding:dimensions=2:metric=cosine:entries=2",
        );

    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Query,
            2,
            b"MATCH (n:Document) WHERE vector.knn(n.embedding, [1.0, 0.0], 1) RETURN n.title"
                .to_vec(),
        ),
    )
    .unwrap();
    let response = read_native_payload(&mut stream, NativeMessageType::Response, 2);
    assert!(response.starts_with("OK\tRESULT_START\t"));
    let rows = decode_query_rows(response.split('\t').nth(6).unwrap()).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get("n.title"),
        Some(&neo4r_query::QueryValue::Scalar(Value::String(
            "near".to_string()
        )))
    );

    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Query,
            3,
            b"SHOW VECTOR INDEX STATUS doc_embedding".to_vec(),
        ),
    )
    .unwrap();
    let status = read_native_payload(&mut stream, NativeMessageType::Response, 3);
    assert!(status.starts_with("OK\tRESULT_START\t"));
    let rows = decode_query_rows(status.split('\t').nth(6).unwrap()).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get("entries"),
        Some(&neo4r_query::QueryValue::Scalar(Value::Int(2)))
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
fn native_query_forwards_create_node_to_remote_primary() {
    let local_dir = temp_dir("neo4r-forward-cypher-write-local");
    let remote_dir = temp_dir("neo4r-forward-cypher-write-remote");
    let routing_table = ShardRoutingTable {
        version: 6,
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
            b"CREATE (n:Person {name: $name}) RETURN n\tname=s:RemoteCypher".to_vec(),
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
    let Some(neo4r_query::QueryValue::Node(node)) = rows[0].get("n") else {
        panic!("expected created node");
    };
    assert_eq!(
        node.properties.get("name"),
        Some(&Value::String("RemoteCypher".to_string()))
    );

    write_frame(
        &mut stream,
        &NativeFrame::new(NativeMessageType::Quit, 2, Vec::new()),
    )
    .unwrap();
    assert_native_response(&mut stream, NativeMessageType::Response, 2, "OK\tBYE");

    local_server.join().unwrap();
    remote_server.join().unwrap();
    assert_eq!(
        remote_db
            .query(r#"MATCH (n:Person) WHERE n.name = "RemoteCypher" RETURN n.name"#)
            .unwrap()
            .len(),
        1
    );

    drop(local_db);
    drop(remote_db);
    let _ = fs::remove_dir_all(local_dir);
    let _ = fs::remove_dir_all(remote_dir);
}

#[test]
fn native_query_routes_create_node_by_stable_hash_across_shards() {
    let local_dir = temp_dir("neo4r-create-hash-route-local");
    let remote_dir = temp_dir("neo4r-create-hash-route-remote");
    let routing_table = ShardRoutingTable {
        version: 7,
        placements: vec![
            ShardPlacement::new(0, vec![ShardReplica::primary(1)]),
            ShardPlacement::new(1, vec![ShardReplica::primary(2), ShardReplica::replica(1)]),
        ],
    };
    let local_db = Neo4rDatabaseHandle::open(
        DatabaseConfig::new(&local_dir, 2, 2)
            .with_server_id(1)
            .with_routing_table(routing_table.clone()),
    )
    .unwrap();
    let remote_db = Neo4rDatabaseHandle::open(
        DatabaseConfig::new(&remote_dir, 2, 2)
            .with_server_id(2)
            .with_routing_table(routing_table),
    )
    .unwrap();

    let query = "CREATE (n:Person {name: $name}) RETURN n";
    let status = local_db.cluster_status().unwrap();
    let canonical_params = [("name".to_string(), Value::String("same-user".to_string()))]
        .into_iter()
        .collect();
    let spaced_query = "  CREATE   ( n : Person { name : $name } )   RETURN   n  ";
    assert_eq!(
        select_create_node_write_shard(&status, query, &canonical_params)
            .unwrap()
            .shard_id,
        select_create_node_write_shard(&status, spaced_query, &canonical_params)
            .unwrap()
            .shard_id
    );
    let create_set_query = "CREATE (n:Person {name: $name}) SET n.active = true RETURN n";
    let create_set_equivalent_query = "CREATE (n:Person {active: true, name: $name}) RETURN n";
    assert_eq!(
        select_create_node_write_shard(&status, create_set_query, &canonical_params)
            .unwrap()
            .shard_id,
        select_create_node_write_shard(&status, create_set_equivalent_query, &canonical_params)
            .unwrap()
            .shard_id
    );

    let mut local_name = None;
    let mut remote_name = None;
    for candidate in 0..128 {
        let name = format!("user-{candidate}");
        let params = [("name".to_string(), Value::String(name.clone()))]
            .into_iter()
            .collect();
        let shard_id = select_create_node_write_shard(&status, query, &params)
            .unwrap()
            .shard_id;
        if shard_id == 0 && local_name.is_none() {
            local_name = Some(name);
        } else if shard_id == 1 && remote_name.is_none() {
            remote_name = Some(name);
        }
        if local_name.is_some() && remote_name.is_some() {
            break;
        }
    }
    let local_name = local_name.expect("expected a candidate for shard 0");
    let remote_name = remote_name.expect("expected a candidate for shard 1");

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
            format!("{query}\tname=s:{local_name}").into_bytes(),
        ),
    )
    .unwrap();
    let local_response = read_native_payload(&mut stream, NativeMessageType::Response, 1);
    assert!(local_response.starts_with("OK\tRESULT_START\t"));

    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Query,
            2,
            format!("{query}\tname=s:{remote_name}").into_bytes(),
        ),
    )
    .unwrap();
    let remote_response = read_native_payload(&mut stream, NativeMessageType::Response, 2);
    assert!(remote_response.starts_with("OK\tRESULT_START\t"));

    write_frame(
        &mut stream,
        &NativeFrame::new(NativeMessageType::Quit, 3, Vec::new()),
    )
    .unwrap();
    assert_native_response(&mut stream, NativeMessageType::Response, 3, "OK\tBYE");

    local_server.join().unwrap();
    remote_server.join().unwrap();
    assert_eq!(
        local_db
            .query(&format!(
                r#"MATCH (n:Person) WHERE n.name = "{local_name}" RETURN n"#
            ))
            .unwrap()
            .len(),
        1
    );
    assert!(local_db
        .query(&format!(
            r#"MATCH (n:Person) WHERE n.name = "{remote_name}" RETURN n"#
        ))
        .unwrap()
        .is_empty());
    assert_eq!(
        remote_db
            .query(&format!(
                r#"MATCH (n:Person) WHERE n.name = "{remote_name}" RETURN n"#
            ))
            .unwrap()
            .len(),
        1
    );
    assert!(remote_db
        .query(&format!(
            r#"MATCH (n:Person) WHERE n.name = "{local_name}" RETURN n"#
        ))
        .unwrap()
        .is_empty());

    drop(local_db);
    drop(remote_db);
    let _ = fs::remove_dir_all(local_dir);
    let _ = fs::remove_dir_all(remote_dir);
}

#[test]
fn native_query_routes_merge_node_to_single_owner_shard() {
    let local_dir = temp_dir("neo4r-merge-hash-route-local");
    let remote_dir = temp_dir("neo4r-merge-hash-route-remote");
    let routing_table = ShardRoutingTable {
        version: 8,
        placements: vec![
            ShardPlacement::new(0, vec![ShardReplica::primary(1)]),
            ShardPlacement::new(1, vec![ShardReplica::primary(2), ShardReplica::replica(1)]),
        ],
    };
    let local_db = Neo4rDatabaseHandle::open(
        DatabaseConfig::new(&local_dir, 2, 2)
            .with_server_id(1)
            .with_routing_table(routing_table.clone()),
    )
    .unwrap();
    let remote_db = Neo4rDatabaseHandle::open(
        DatabaseConfig::new(&remote_dir, 2, 2)
            .with_server_id(2)
            .with_routing_table(routing_table),
    )
    .unwrap();

    let query = "MERGE (n:Person {email: $email}) RETURN n";
    let status = local_db.cluster_status().unwrap();
    let mut local_email = None;
    let mut remote_email = None;
    for candidate in 0..128 {
        let email = format!("user-{candidate}@example.com");
        let params = [("email".to_string(), Value::String(email.clone()))]
            .into_iter()
            .collect();
        let shard_id = select_merge_node_write_shard(&status, query, &params)
            .unwrap()
            .shard_id;
        if shard_id == 0 && local_email.is_none() {
            local_email = Some(email);
        } else if shard_id == 1 && remote_email.is_none() {
            remote_email = Some(email);
        }
        if local_email.is_some() && remote_email.is_some() {
            break;
        }
    }
    let local_email = local_email.expect("expected a candidate for shard 0");
    let remote_email = remote_email.expect("expected a candidate for shard 1");

    let remote_backend = TcpBackend::new(remote_db.clone());
    let remote_listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let remote_addr = remote_listener.local_addr().unwrap();
    let remote_server = thread::spawn(move || {
        remote_backend
            .clone()
            .serve_listener_once(remote_listener.try_clone().unwrap())
            .unwrap();
        remote_backend.serve_listener_once(remote_listener).unwrap();
    });
    let local_backend = TcpBackend::new(local_db.clone());
    local_backend
        .register_query_peer(2, remote_addr.to_string())
        .unwrap();
    let local_listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let local_addr = local_listener.local_addr().unwrap();
    let local_server =
        thread::spawn(move || local_backend.serve_listener_once(local_listener).unwrap());

    let mut stream = TcpStream::connect(local_addr).unwrap();
    for (request_id, email) in [(1, local_email.as_str()), (2, remote_email.as_str())] {
        for repeat in 0..2 {
            write_frame(
                &mut stream,
                &NativeFrame::new(
                    NativeMessageType::Query,
                    request_id + repeat * 10,
                    format!("{query}\temail=s:{email}").into_bytes(),
                ),
            )
            .unwrap();
            let response = read_native_payload(
                &mut stream,
                NativeMessageType::Response,
                request_id + repeat * 10,
            );
            assert!(response.starts_with("OK\tRESULT_START\t"));
        }
    }

    write_frame(
        &mut stream,
        &NativeFrame::new(NativeMessageType::Quit, 99, Vec::new()),
    )
    .unwrap();
    assert_native_response(&mut stream, NativeMessageType::Response, 99, "OK\tBYE");

    local_server.join().unwrap();
    remote_server.join().unwrap();
    assert_eq!(
        local_db
            .query(&format!(
                r#"MATCH (n:Person) WHERE n.email = "{local_email}" RETURN n"#
            ))
            .unwrap()
            .len(),
        1
    );
    assert!(local_db
        .query(&format!(
            r#"MATCH (n:Person) WHERE n.email = "{remote_email}" RETURN n"#
        ))
        .unwrap()
        .is_empty());
    assert_eq!(
        remote_db
            .query(&format!(
                r#"MATCH (n:Person) WHERE n.email = "{remote_email}" RETURN n"#
            ))
            .unwrap()
            .len(),
        1
    );
    assert!(remote_db
        .query(&format!(
            r#"MATCH (n:Person) WHERE n.email = "{local_email}" RETURN n"#
        ))
        .unwrap()
        .is_empty());

    drop(local_db);
    drop(remote_db);
    let _ = fs::remove_dir_all(local_dir);
    let _ = fs::remove_dir_all(remote_dir);
}

#[test]
fn native_query_forwards_set_node_to_remote_primary() {
    let local_dir = temp_dir("neo4r-forward-cypher-set-local");
    let remote_dir = temp_dir("neo4r-forward-cypher-set-remote");
    let routing_table = ShardRoutingTable {
        version: 7,
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
    let entry = LogEntry::new_with_metadata(
        0,
        1,
        1,
        2,
        7,
        HybridTimestamp::new(1, 0),
        Command::CreateNode {
            id: 0,
            labels: vec!["Person".to_string()],
            properties: [("name".to_string(), Value::String("Alice".to_string()))]
                .into_iter()
                .collect(),
        },
    );
    remote_db.apply_replicated_entry(entry.clone()).unwrap();
    local_db.apply_replicated_entry(entry).unwrap();

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
                b"MATCH (n:Person) WHERE n.name = $name SET n.status = $status\tname=s:Alice\tstatus=s:active".to_vec(),
            ),
        )
        .unwrap();
    let start = read_native_payload(&mut stream, NativeMessageType::Response, 1);
    let parts = start.split('\t').collect::<Vec<_>>();
    assert_eq!(parts[0..3], ["OK", "RESULT_START", "1"]);
    assert_eq!(parts[3], "0");

    write_frame(
        &mut stream,
        &NativeFrame::new(NativeMessageType::Quit, 2, Vec::new()),
    )
    .unwrap();
    assert_native_response(&mut stream, NativeMessageType::Response, 2, "OK\tBYE");

    local_server.join().unwrap();
    remote_server.join().unwrap();
    assert_eq!(
        remote_db
            .query(r#"MATCH (n:Person) WHERE n.status = "active" RETURN n.name"#)
            .unwrap()
            .len(),
        1
    );

    drop(local_db);
    drop(remote_db);
    let _ = fs::remove_dir_all(local_dir);
    let _ = fs::remove_dir_all(remote_dir);
}

#[test]
fn native_query_forwards_create_relationship_to_remote_primary() {
    let local_dir = temp_dir("neo4r-forward-cypher-rel-local");
    let remote_dir = temp_dir("neo4r-forward-cypher-rel-remote");
    let routing_table = ShardRoutingTable {
        version: 8,
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
    for (index, id, name) in [(1, 0, "Alice"), (2, 1, "Bob")] {
        let entry = LogEntry::new_with_metadata(
            0,
            1,
            index,
            2,
            8,
            HybridTimestamp::new(index, 0),
            Command::CreateNode {
                id,
                labels: vec!["Person".to_string()],
                properties: [("name".to_string(), Value::String(name.to_string()))]
                    .into_iter()
                    .collect(),
            },
        );
        remote_db.apply_replicated_entry(entry.clone()).unwrap();
        local_db.apply_replicated_entry(entry).unwrap();
    }

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
                b"MATCH (a:Person {name: $from}) MATCH (b:Person {name: $to}) CREATE (a)-[r:KNOWS {since: $since}]->(b) RETURN r\tfrom=s:Alice\tto=s:Bob\tsince=i:2026".to_vec(),
            ),
        )
        .unwrap();
    let start = read_native_payload(&mut stream, NativeMessageType::Response, 1);
    let parts = start.split('\t').collect::<Vec<_>>();
    assert_eq!(parts[0..3], ["OK", "RESULT_START", "1"]);
    assert_eq!(parts[3], "1");
    let rows = decode_query_rows(parts[6]).unwrap();
    let Some(neo4r_query::QueryValue::Relationship(relationship)) = rows[0].get("r") else {
        panic!("expected created relationship");
    };
    assert_eq!(relationship.rel_type, "KNOWS");

    write_frame(
        &mut stream,
        &NativeFrame::new(NativeMessageType::Quit, 2, Vec::new()),
    )
    .unwrap();
    assert_native_response(&mut stream, NativeMessageType::Response, 2, "OK\tBYE");

    local_server.join().unwrap();
    remote_server.join().unwrap();
    assert_eq!(
        remote_db
            .query("MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN r")
            .unwrap()
            .len(),
        1
    );

    drop(local_db);
    drop(remote_db);
    let _ = fs::remove_dir_all(local_dir);
    let _ = fs::remove_dir_all(remote_dir);
}

#[test]
fn backend_query_peer_management_updates_distributed_query_routes() {
    let local_dir = temp_dir("neo4r-query-peer-management-local");
    let remote_dir = temp_dir("neo4r-query-peer-management-remote");
    let routing_table = ShardRoutingTable {
        version: 3,
        placements: vec![
            ShardPlacement::new(0, vec![ShardReplica::primary(1)]),
            ShardPlacement::new(1, vec![ShardReplica::primary(2)]),
        ],
    };
    let local_db = Neo4rDatabaseHandle::open(
        DatabaseConfig::new(&local_dir, 2, 2)
            .with_server_id(1)
            .with_routing_table(routing_table.clone()),
    )
    .unwrap();
    let remote_db = Neo4rDatabaseHandle::open(
        DatabaseConfig::new(&remote_dir, 2, 2)
            .with_server_id(2)
            .with_routing_table(routing_table),
    )
    .unwrap();
    remote_db
        .apply_replicated_entry(LogEntry::new_with_metadata(
            1,
            1,
            1,
            2,
            3,
            HybridTimestamp::new(1, 0),
            Command::CreateNode {
                id: 1,
                labels: vec!["Person".to_string()],
                properties: [("name".to_string(), Value::String("Bob".to_string()))]
                    .into_iter()
                    .collect(),
            },
        ))
        .unwrap();

    let remote_backend = TcpBackend::new(remote_db.clone());
    let remote_listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let remote_addr = remote_listener.local_addr().unwrap();
    let remote_server =
        thread::spawn(move || remote_backend.serve_listener_once(remote_listener).unwrap());
    let local_backend = TcpBackend::new(local_db.clone());

    assert_eq!(
        local_backend.execute_backend_request(
            parse_request(&format!("REGISTER_QUERY_PEER\t2\t{remote_addr}")).unwrap(),
        ),
        BackendResponse::OkUnit
    );
    assert_eq!(
        local_backend.execute_backend_request(parse_request("LIST_QUERY_PEERS").unwrap()),
        BackendResponse::OkQueryPeers(format!("2={remote_addr}"))
    );
    assert!(matches!(
        local_backend.execute_backend_request(
            parse_request("QUERY_DISTRIBUTED\tMATCH (n:Person) RETURN n.name").unwrap()
        ),
        BackendResponse::OkRows { count: 1, .. }
    ));
    assert_eq!(
        local_backend.execute_backend_request(parse_request("UNREGISTER_QUERY_PEER\t2").unwrap()),
        BackendResponse::OkUnit
    );
    assert!(matches!(
        local_backend.execute_backend_request(
            parse_request("QUERY_DISTRIBUTED\tMATCH (n:Person) RETURN n.name").unwrap()
        ),
        BackendResponse::Err(message) if message.contains("missing query peer address")
    ));

    remote_server.join().unwrap();
    drop(local_backend);
    drop(local_db);
    drop(remote_db);
    let _ = fs::remove_dir_all(local_dir);
    let _ = fs::remove_dir_all(remote_dir);
}

#[test]
fn native_command_distributed_query_returns_cursor_rows() {
    let local_dir = temp_dir("neo4r-native-distributed-local");
    let remote_dir = temp_dir("neo4r-native-distributed-remote");
    let routing_table = ShardRoutingTable {
        version: 3,
        placements: vec![
            ShardPlacement::new(0, vec![ShardReplica::primary(1)]),
            ShardPlacement::new(1, vec![ShardReplica::primary(2)]),
        ],
    };
    let local_db = Neo4rDatabaseHandle::open(
        DatabaseConfig::new(&local_dir, 2, 2)
            .with_server_id(1)
            .with_routing_table(routing_table.clone()),
    )
    .unwrap();
    local_db
        .create_node(
            vec!["Person".to_string()],
            [("name".to_string(), Value::String("Alice".to_string()))]
                .into_iter()
                .collect(),
        )
        .unwrap();
    let remote_db = Neo4rDatabaseHandle::open(
        DatabaseConfig::new(&remote_dir, 2, 2)
            .with_server_id(2)
            .with_routing_table(routing_table),
    )
    .unwrap();
    remote_db
        .apply_replicated_entry(LogEntry::new_with_metadata(
            1,
            1,
            1,
            2,
            3,
            HybridTimestamp::new(1, 0),
            Command::CreateNode {
                id: 1,
                labels: vec!["Person".to_string()],
                properties: [("name".to_string(), Value::String("Bob".to_string()))]
                    .into_iter()
                    .collect(),
            },
        ))
        .unwrap();
    remote_db
        .apply_replicated_entry(LogEntry::new_with_metadata(
            1,
            1,
            2,
            2,
            3,
            HybridTimestamp::new(2, 0),
            Command::CreateNode {
                id: 3,
                labels: vec!["Person".to_string()],
                properties: [("name".to_string(), Value::String("Carol".to_string()))]
                    .into_iter()
                    .collect(),
            },
        ))
        .unwrap();

    let remote_backend = TcpBackend::with_config(
        remote_db.clone(),
        TcpBackendConfig {
            default_page_size: 1,
            ..TcpBackendConfig::default()
        },
    );
    let remote_listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let remote_addr = remote_listener.local_addr().unwrap();
    let remote_server =
        thread::spawn(move || remote_backend.serve_listener_once(remote_listener).unwrap());
    let local_backend = TcpBackend::with_config(
        local_db.clone(),
        TcpBackendConfig {
            default_page_size: 1,
            ..TcpBackendConfig::default()
        },
    );
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
            NativeMessageType::Command,
            1,
            b"QUERY_DISTRIBUTED\tMATCH (n:Person) RETURN n.name".to_vec(),
        ),
    )
    .unwrap();
    let start = read_native_payload(&mut stream, NativeMessageType::Response, 1);
    let parts = start.split('\t').collect::<Vec<_>>();
    assert_eq!(parts[0..3], ["OK", "RESULT_START", "1"]);
    assert_eq!(parts[3], "3");
    assert_eq!(parts[4], "1");
    assert_eq!(parts[5], "true");
    let first_page = decode_query_rows(parts[6]).unwrap();
    assert_eq!(first_page.len(), 1);

    write_frame(
        &mut stream,
        &NativeFrame::new(NativeMessageType::Fetch, 2, b"1\t2".to_vec()),
    )
    .unwrap();
    let page = read_native_payload(&mut stream, NativeMessageType::Response, 2);
    let parts = page.split('\t').collect::<Vec<_>>();
    assert_eq!(parts[0..3], ["OK", "RESULT_PAGE", "1"]);
    assert_eq!(parts[3], "2");
    assert_eq!(parts[4], "false");
    let second_page = decode_query_rows(parts[5]).unwrap();
    let mut names = first_page
        .into_iter()
        .chain(second_page)
        .filter_map(|row| match row.get("n.name") {
            Some(neo4r_query::QueryValue::Scalar(Value::String(name))) => Some(name.clone()),
            _ => None,
        })
        .collect::<Vec<_>>();
    names.sort();
    assert_eq!(
        names,
        vec!["Alice".to_string(), "Bob".to_string(), "Carol".to_string()]
    );

    write_frame(
        &mut stream,
        &NativeFrame::new(NativeMessageType::Quit, 3, Vec::new()),
    )
    .unwrap();
    assert_native_response(&mut stream, NativeMessageType::Response, 3, "OK\tBYE");

    local_server.join().unwrap();
    remote_server.join().unwrap();
    drop(local_db);
    drop(remote_db);
    let _ = fs::remove_dir_all(local_dir);
    let _ = fs::remove_dir_all(remote_dir);
}

#[test]
fn native_transaction_distributed_query_reads_remote_shards() {
    let local_dir = temp_dir("neo4r-native-tx-distributed-local");
    let remote_dir = temp_dir("neo4r-native-tx-distributed-remote");
    let routing_table = ShardRoutingTable {
        version: 10,
        placements: vec![
            ShardPlacement::new(0, vec![ShardReplica::primary(1)]),
            ShardPlacement::new(1, vec![ShardReplica::primary(2)]),
        ],
    };
    let local_db = Neo4rDatabaseHandle::open(
        DatabaseConfig::new(&local_dir, 2, 2)
            .with_server_id(1)
            .with_routing_table(routing_table.clone()),
    )
    .unwrap();
    local_db
        .create_node_on_shard(
            0,
            vec!["Person".to_string()],
            [("name".to_string(), Value::String("Local".to_string()))]
                .into_iter()
                .collect(),
        )
        .unwrap();
    let remote_db = Neo4rDatabaseHandle::open(
        DatabaseConfig::new(&remote_dir, 2, 2)
            .with_server_id(2)
            .with_routing_table(routing_table),
    )
    .unwrap();
    remote_db
        .create_node_on_shard(
            1,
            vec!["Person".to_string()],
            [("name".to_string(), Value::String("Remote".to_string()))]
                .into_iter()
                .collect(),
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
            format!("TX_QUERY_DISTRIBUTED\t{tx_id}\tMATCH (n:Person) RETURN n.name").into_bytes(),
        ),
    )
    .unwrap();
    let start = read_native_payload(&mut stream, NativeMessageType::Response, 2);
    let parts = start.split('\t').collect::<Vec<_>>();
    assert_eq!(parts[0..3], ["OK", "RESULT_START", "1"]);
    assert_eq!(parts[3], "2");
    assert_eq!(parts[4], "2");
    assert_eq!(parts[5], "false");
    let mut names = decode_query_rows(parts[6])
        .unwrap()
        .into_iter()
        .filter_map(|row| match row.get("n.name") {
            Some(neo4r_query::QueryValue::Scalar(Value::String(name))) => Some(name.clone()),
            _ => None,
        })
        .collect::<Vec<_>>();
    names.sort();
    assert_eq!(names, vec!["Local".to_string(), "Remote".to_string()]);

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

    local_server.join().unwrap();
    remote_server.join().unwrap();
    drop(local_db);
    drop(remote_db);
    let _ = fs::remove_dir_all(local_dir);
    let _ = fs::remove_dir_all(remote_dir);
}

#[test]
fn native_read_write_transaction_distributed_query_reads_local_staged_writes() {
    let local_dir = temp_dir("neo4r-native-rw-tx-distributed-staged-local");
    let remote_dir = temp_dir("neo4r-native-rw-tx-distributed-staged-remote");
    let routing_table = ShardRoutingTable {
        version: 10,
        placements: vec![
            ShardPlacement::new(0, vec![ShardReplica::primary(1)]),
            ShardPlacement::new(1, vec![ShardReplica::primary(2)]),
        ],
    };
    let local_db = Neo4rDatabaseHandle::open(
        DatabaseConfig::new(&local_dir, 2, 2)
            .with_server_id(1)
            .with_routing_table(routing_table.clone()),
    )
    .unwrap();
    local_db
        .create_node_on_shard(
            0,
            vec!["Person".to_string()],
            [
                ("name".to_string(), Value::String("Local".to_string())),
                ("status".to_string(), Value::String("old".to_string())),
            ]
            .into_iter()
            .collect(),
        )
        .unwrap();
    let remote_db = Neo4rDatabaseHandle::open(
        DatabaseConfig::new(&remote_dir, 2, 2)
            .with_server_id(2)
            .with_routing_table(routing_table),
    )
    .unwrap();
    remote_db
        .create_node_on_shard(
            1,
            vec!["Person".to_string()],
            [
                ("name".to_string(), Value::String("Remote".to_string())),
                ("status".to_string(), Value::String("committed".to_string())),
            ]
            .into_iter()
            .collect(),
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
            NativeMessageType::Command,
            1,
            b"BEGIN_TX\tREAD_WRITE".to_vec(),
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
                    "TX_QUERY\t{tx_id}\tMATCH (n:Person) WHERE n.name = $name SET n.status = $status\tname=s:Local\tstatus=s:staged"
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
            format!("TX_QUERY_DISTRIBUTED\t{tx_id}\tMATCH (n:Person) RETURN n.name, n.status")
                .into_bytes(),
        ),
    )
    .unwrap();
    let start = read_native_payload(&mut stream, NativeMessageType::Response, 3);
    let parts = start.split('\t').collect::<Vec<_>>();
    assert_eq!(parts[0..3], ["OK", "RESULT_START", "1"]);
    assert_eq!(parts[3], "2");
    let rows = decode_query_rows(parts[6]).unwrap();
    assert_eq!(rows.len(), 2);
    let mut observed = rows
        .iter()
        .filter_map(|row| {
            let name = match row.get("n.name") {
                Some(neo4r_query::QueryValue::Scalar(Value::String(name))) => name.clone(),
                _ => return None,
            };
            let status = match row.get("n.status") {
                Some(neo4r_query::QueryValue::Scalar(Value::String(status))) => status.clone(),
                _ => return None,
            };
            Some((name, status))
        })
        .collect::<Vec<_>>();
    observed.sort();
    assert_eq!(
        observed,
        vec![
            ("Local".to_string(), "staged".to_string()),
            ("Remote".to_string(), "committed".to_string()),
        ]
    );

    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Command,
            4,
            format!("ROLLBACK_TX\t{tx_id}").into_bytes(),
        ),
    )
    .unwrap();
    assert_native_response(
        &mut stream,
        NativeMessageType::Response,
        4,
        &format!("OK\tTX_ROLLBACK\t{tx_id}"),
    );
    write_frame(
        &mut stream,
        &NativeFrame::new(NativeMessageType::Quit, 5, Vec::new()),
    )
    .unwrap();
    assert_native_response(&mut stream, NativeMessageType::Response, 5, "OK\tBYE");

    local_server.join().unwrap();
    remote_server.join().unwrap();
    assert_eq!(
        local_db
            .query(r#"MATCH (n:Person) WHERE n.name = "Local" RETURN n.status"#)
            .unwrap()[0]
            .get("n.status"),
        Some(&neo4r_query::QueryValue::Scalar(Value::String(
            "old".to_string()
        )))
    );
    drop(local_db);
    drop(remote_db);
    let _ = fs::remove_dir_all(local_dir);
    let _ = fs::remove_dir_all(remote_dir);
}

#[test]
fn native_read_write_transaction_distributed_query_reads_remote_staged_writes() {
    let local_dir = temp_dir("neo4r-native-rw-tx-distributed-remote-staged-local");
    let remote_dir = temp_dir("neo4r-native-rw-tx-distributed-remote-staged-remote");
    let routing_table = ShardRoutingTable {
        version: 10,
        placements: vec![
            ShardPlacement::new(0, vec![ShardReplica::primary(1)]),
            ShardPlacement::new(1, vec![ShardReplica::primary(2)]),
        ],
    };
    let local_db = Neo4rDatabaseHandle::open(
        DatabaseConfig::new(&local_dir, 2, 2)
            .with_server_id(1)
            .with_routing_table(routing_table.clone()),
    )
    .unwrap();
    let remote_db = Neo4rDatabaseHandle::open(
        DatabaseConfig::new(&remote_dir, 2, 2)
            .with_server_id(2)
            .with_routing_table(routing_table),
    )
    .unwrap();

    let status = local_db.cluster_status().unwrap();
    let remote_name = (0..100)
        .map(|index| format!("RemoteCreate{index}"))
        .find(|name| {
            let query = "CREATE (n:Person {name: $name}) RETURN n";
            let params = [("name".to_string(), Value::String(name.clone()))]
                .into_iter()
                .collect();
            select_create_node_write_shard(&status, query, &params)
                .map(|shard| shard.shard_id == 1)
                .unwrap_or(false)
        })
        .expect("expected test routing key for remote shard");

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
            NativeMessageType::Command,
            1,
            b"BEGIN_TX\tREAD_WRITE".to_vec(),
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
                    "TX_QUERY\t{tx_id}\tCREATE (n:Person {{name: $name}}) RETURN n\tname=s:{remote_name}"
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
            format!("TX_QUERY_DISTRIBUTED\t{tx_id}\tMATCH (n:Person) RETURN n.name").into_bytes(),
        ),
    )
    .unwrap();
    let start = read_native_payload(&mut stream, NativeMessageType::Response, 3);
    let parts = start.split('\t').collect::<Vec<_>>();
    assert_eq!(parts[0..3], ["OK", "RESULT_START", "1"]);
    assert_eq!(parts[3], "1");
    let rows = decode_query_rows(parts[6]).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get("n.name"),
        Some(&neo4r_query::QueryValue::Scalar(Value::String(remote_name)))
    );

    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Command,
            4,
            format!("ROLLBACK_TX\t{tx_id}").into_bytes(),
        ),
    )
    .unwrap();
    assert_native_response(
        &mut stream,
        NativeMessageType::Response,
        4,
        &format!("OK\tTX_ROLLBACK\t{tx_id}"),
    );
    write_frame(
        &mut stream,
        &NativeFrame::new(NativeMessageType::Quit, 5, Vec::new()),
    )
    .unwrap();
    assert_native_response(&mut stream, NativeMessageType::Response, 5, "OK\tBYE");

    local_server.join().unwrap();
    remote_server.join().unwrap();
    assert!(remote_db
        .query("MATCH (n:Person) RETURN n")
        .unwrap()
        .is_empty());
    drop(local_db);
    drop(remote_db);
    let _ = fs::remove_dir_all(local_dir);
    let _ = fs::remove_dir_all(remote_dir);
}

#[test]
fn line_protocol_remains_available_explicitly() {
    let dir = temp_dir("neo4r-line-backend");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1)).unwrap();
    let backend = TcpBackend::new(db);
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        backend.handle_line_stream(stream).unwrap();
    });

    let mut stream = TcpStream::connect(addr).unwrap();
    let mut reader = BufReader::new(stream.try_clone().unwrap());

    writeln!(stream, "PING").unwrap();
    assert_response(&mut reader, "OK\tPONG\n");
    writeln!(stream, "QUIT").unwrap();
    assert_response(&mut reader, "OK\tBYE\n");

    server.join().unwrap();
    let _ = fs::remove_dir_all(dir);
}

#[test]
fn native_protocol_correlates_responses_by_request_id() {
    let dir = temp_dir("neo4r-native-correlation");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1)).unwrap();
    let backend = TcpBackend::new(db);
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let server = thread::spawn(move || backend.serve_listener_once(listener).unwrap());

    let mut stream = TcpStream::connect(addr).unwrap();
    write_frame(
        &mut stream,
        &NativeFrame::new(NativeMessageType::Ping, 100, Vec::new()),
    )
    .unwrap();
    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Command,
            101,
            b"CREATE_NODE\tPerson\tname=s:bob".to_vec(),
        ),
    )
    .unwrap();

    let first = read_frame(&mut stream).unwrap().unwrap();
    let second = read_frame(&mut stream).unwrap().unwrap();
    let mut ids = vec![first.request_id, second.request_id];
    ids.sort_unstable();
    assert_eq!(ids, vec![100, 101]);

    write_frame(
        &mut stream,
        &NativeFrame::new(NativeMessageType::Quit, 102, Vec::new()),
    )
    .unwrap();
    assert_native_response(&mut stream, NativeMessageType::Response, 102, "OK\tBYE");

    server.join().unwrap();
    let _ = fs::remove_dir_all(dir);
}

#[test]
fn cursor_store_scopes_cursors_to_session_and_cleans_up_session() {
    let store = CursorStore::default();
    let first_cursor = store.insert(10, Box::new(VecQueryCursor::new(vec![QueryRow::new()])));
    let second_cursor = store.insert(11, Box::new(VecQueryCursor::new(vec![QueryRow::new()])));

    let err = store.fetch(11, first_cursor, 1).unwrap_err();
    assert!(err.contains("belongs to another session"));
    let err = store.close(10, second_cursor).unwrap_err();
    assert!(err.contains("belongs to another session"));

    let page = store.fetch(10, first_cursor, 1).unwrap();
    assert_eq!(page.rows.len(), 1);
    assert!(!page.has_more);
    assert!(store.fetch(10, first_cursor, 1).is_err());
    store.close(10, first_cursor).unwrap();
    store.close(10, first_cursor).unwrap();

    let third_cursor = store.insert(10, Box::new(VecQueryCursor::new(vec![QueryRow::new()])));
    assert_eq!(store.close_session(10).unwrap(), 1);
    assert!(store.fetch(10, third_cursor, 1).is_err());
    assert_eq!(store.close_session(11).unwrap(), 1);
}

#[test]
fn pending_request_store_tracks_queued_cancellation_by_session() {
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
fn native_worker_pool_reports_full_queue_without_blocking_session() {
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
fn native_cancel_reports_missed_request_when_not_pending() {
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
fn native_query_uses_cursor_and_fetch_pages() {
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
fn native_query_can_execute_cypher_write() {
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
fn native_query_forwards_index_cypher_to_metadata_primary() {
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
fn native_query_can_execute_constraint_cypher_write() {
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
fn native_query_can_execute_relationship_cypher_write() {
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
fn native_query_can_show_indexes() {
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
fn native_command_can_install_routing_table_and_read_cluster_status() {
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
fn native_command_reports_structured_management_statuses() {
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
fn parses_transaction_query_plan_command() {
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
fn parses_prepared_query_commands() {
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
fn describes_query_parameters_without_string_literals() {
    assert_eq!(
        describe_query_parameters(
            r#"MATCH (n:Person {name: $name}) WHERE n.note = "$ignored" AND n.age >= $age SET n += $props RETURN $name"#
        ),
        vec!["age".to_string(), "name".to_string(), "props".to_string()]
    );
}

#[test]
fn describes_prepared_query_kind_and_parameters() {
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

#[test]
fn prepared_query_routing_hint_reports_read_and_write_routes() {
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
fn parses_transaction_status_commands() {
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
fn transaction_store_lists_all_session_transactions() {
    let store = TransactionStore::default();
    let first_tx = store.insert(
        7,
        NativeTransaction::ReadWrite {
            isolation: ReadIsolation::ReadCommitted,
            staged_writes: Vec::new(),
        },
    );
    let second_tx = store.insert(
        9,
        NativeTransaction::ReadWrite {
            isolation: ReadIsolation::Snapshot,
            staged_writes: Vec::new(),
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
                "OK\tTX_LIST_ALL\t2\t7:{first_tx}:READ_WRITE:READ_COMMITTED:1,9:{second_tx}:READ_WRITE:SNAPSHOT:0"
            )
        );
    assert_eq!(
        format_tx_list(store.list(7).unwrap()),
        format!("OK\tTX_LIST\t1\t{first_tx}:READ_WRITE:READ_COMMITTED:1")
    );
}

#[test]
fn native_read_write_transaction_reports_status() {
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
        &format!("OK\tTX_STATUS\t{tx_id}\tREAD_WRITE\tREAD_COMMITTED\t0"),
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
        &format!("OK\tTX_STATUS\t{tx_id}\tREAD_WRITE\tREAD_COMMITTED\t1"),
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
fn native_transaction_query_reads_stable_snapshot() {
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
fn native_read_committed_transaction_reads_latest_statement_snapshot() {
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
fn native_transaction_query_plan_reports_access_path() {
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
fn native_read_write_transaction_query_plan_reports_staged_overlay() {
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
fn native_read_write_transaction_stages_writes_until_commit() {
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
fn native_read_write_transaction_reads_staged_relationship_creates() {
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

#[test]
fn native_read_write_transaction_commits_staged_create_property_replacement() {
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
fn native_read_write_transaction_commits_staged_relationship_create_property_replacement() {
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
fn native_read_write_transaction_commits_staged_create_then_set() {
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
fn native_read_write_transaction_commits_staged_nodes_and_relationship() {
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
fn native_read_write_transaction_merges_staged_node_on_match() {
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
fn native_read_write_transaction_merges_staged_relationship_on_match() {
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
fn native_read_write_transaction_reads_staged_node_property_updates() {
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

#[test]
fn native_read_write_transaction_set_null_removes_property() {
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
fn native_read_write_transaction_reads_staged_node_property_replacement() {
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
fn native_read_write_transaction_accepts_parameterized_property_map_replacement() {
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
fn native_read_write_transaction_reads_staged_relationship_property_updates() {
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
fn native_read_write_transaction_reads_staged_relationship_property_replacement() {
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
fn native_read_write_transaction_reads_staged_deletes() {
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
fn native_read_write_transaction_group_commits_batchable_sets() {
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

#[test]
fn native_read_write_transaction_group_commits_property_replacements() {
    let dir = temp_dir("neo4r-native-read-write-tx-replacement-batch");
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
    db.create_node(
        vec!["Person".to_string()],
        [
            ("name".to_string(), Value::String("Bob".to_string())),
            ("age".to_string(), Value::Int(43)),
        ]
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
    let tx_id = begin.split('\t').collect::<Vec<_>>()[2]
        .parse::<u64>()
        .unwrap();

    write_frame(
            &mut stream,
            &NativeFrame::new(
                NativeMessageType::Command,
                2,
                format!(
                    "TX_QUERY\t{tx_id}\tMATCH (n:Person) WHERE n.name = \"Alice\" SET n = {{name: \"Alice\", status: \"active\"}}"
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
                    "TX_QUERY\t{tx_id}\tMATCH (n:Person) WHERE n.name = \"Bob\" SET n = {{name: \"Bob\", status: \"active\"}}"
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
            b"MATCH (n:Person) WHERE n.status = \"active\" RETURN n.age".to_vec(),
        ),
    )
    .unwrap();
    let rows = read_native_payload(&mut stream, NativeMessageType::Response, 5);
    let parts = rows.split('\t').collect::<Vec<_>>();
    assert_eq!(parts[0..2], ["OK", "RESULT_START"]);
    assert_eq!(parts[3], "2");
    let rows = decode_query_rows(parts[6]).unwrap();
    assert!(rows
        .iter()
        .all(|row| { row.get("n.age") == Some(&neo4r_query::QueryValue::Scalar(Value::Null)) }));

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
fn native_read_write_transaction_group_commits_property_map_merges() {
    let dir = temp_dir("neo4r-native-read-write-tx-map-merge-batch");
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
    let tx_id = begin.split('\t').collect::<Vec<_>>()[2]
        .parse::<u64>()
        .unwrap();

    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Command,
            2,
            format!(
                "TX_QUERY\t{tx_id}\tMATCH (n:Person) SET n += {{status: $status}}\tstatus=s:active"
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
                    "TX_QUERY\t{tx_id}\tMATCH (n:Person) SET n += {{reviewed: $reviewed}}\treviewed=b:true"
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
    let rows = decode_query_rows(parts[6]).unwrap();
    assert!(rows.iter().all(|row| {
        row.get("n.reviewed") == Some(&neo4r_query::QueryValue::Scalar(Value::Bool(true)))
    }));

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
fn native_read_write_transaction_group_commits_property_replacement_merges() {
    let dir = temp_dir("neo4r-native-read-write-tx-replace-merge-batch");
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
    let tx_id = begin.split('\t').collect::<Vec<_>>()[2]
        .parse::<u64>()
        .unwrap();

    write_frame(
            &mut stream,
            &NativeFrame::new(
                NativeMessageType::Command,
                2,
                format!(
                    "TX_QUERY\t{tx_id}\tMERGE (n:Person {{email: $email}}) ON CREATE SET n = {{email: $email, created: $created}} ON MATCH SET n = {{email: $email, seen: $seen}} RETURN n\temail=s:alice@example.com\tcreated=i:1\tseen=i:2"
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
                    "TX_QUERY\t{tx_id}\tMERGE (n:Person {{email: $email}}) ON CREATE SET n = {{email: $email, created: $created}} ON MATCH SET n = {{email: $email, seen: $seen}} RETURN n\temail=s:alice@example.com\tcreated=i:1\tseen=i:2"
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
                    "TX_QUERY\t{tx_id}\tMATCH (a:Person) WHERE a.name = $from MATCH (b:Person) WHERE b.name = $to MERGE (a)-[r:KNOWS {{since: $since}}]->(b) ON CREATE SET r = {{since: $since, created: $created}} ON MATCH SET r = {{since: $since, seen: $seen}} RETURN r\tfrom=s:Alice\tto=s:Bob\tsince=i:2026\tcreated=i:1\tseen=i:2"
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
                    "TX_QUERY\t{tx_id}\tMATCH (a:Person) WHERE a.name = $from MATCH (b:Person) WHERE b.name = $to MERGE (a)-[r:KNOWS {{since: $since}}]->(b) ON CREATE SET r = {{since: $since, created: $created}} ON MATCH SET r = {{since: $since, seen: $seen}} RETURN r\tfrom=s:Alice\tto=s:Bob\tsince=i:2026\tcreated=i:1\tseen=i:2"
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
                format!(
                    "TX_QUERY\t{tx_id}\tMATCH (n:Person) WHERE n.email = $email RETURN n.created, n.seen\temail=s:alice@example.com"
                )
                .into_bytes(),
            ),
        )
        .unwrap();
    let rows = read_native_payload(&mut stream, NativeMessageType::Response, 6);
    let parts = rows.split('\t').collect::<Vec<_>>();
    assert_eq!(parts[0..2], ["OK", "RESULT_START"]);
    assert_eq!(parts[3], "1");
    let rows = decode_query_rows(parts[6]).unwrap();
    assert_eq!(
        rows[0].get("n.created"),
        Some(&neo4r_query::QueryValue::Scalar(Value::Null))
    );
    assert_eq!(
        rows[0].get("n.seen"),
        Some(&neo4r_query::QueryValue::Scalar(Value::Int(2)))
    );

    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Command,
            7,
            format!("TX_QUERY\t{tx_id}\tMATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN r")
                .into_bytes(),
        ),
    )
    .unwrap();
    let rows = read_native_payload(&mut stream, NativeMessageType::Response, 7);
    let parts = rows.split('\t').collect::<Vec<_>>();
    assert_eq!(parts[0..2], ["OK", "RESULT_START"]);
    assert_eq!(parts[3], "1");
    let rows = decode_query_rows(parts[6]).unwrap();
    let Some(neo4r_query::QueryValue::Relationship(relationship)) = rows[0].get("r") else {
        panic!("expected relationship row");
    };
    assert_eq!(relationship.properties.get("created"), None);
    assert_eq!(relationship.properties.get("seen"), Some(&Value::Int(2)));

    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Command,
            8,
            format!("COMMIT_TX\t{tx_id}").into_bytes(),
        ),
    )
    .unwrap();
    assert_native_response(
        &mut stream,
        NativeMessageType::Response,
        8,
        &format!("OK\tTX_COMMIT\t{tx_id}\t4"),
    );

    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Query,
            9,
            b"MATCH (n:Person) WHERE n.email = \"alice@example.com\" RETURN n.created, n.seen"
                .to_vec(),
        ),
    )
    .unwrap();
    let rows = read_native_payload(&mut stream, NativeMessageType::Response, 9);
    let parts = rows.split('\t').collect::<Vec<_>>();
    assert_eq!(parts[0..2], ["OK", "RESULT_START"]);
    assert_eq!(parts[3], "1");
    let rows = decode_query_rows(parts[6]).unwrap();
    assert_eq!(
        rows[0].get("n.created"),
        Some(&neo4r_query::QueryValue::Scalar(Value::Null))
    );
    assert_eq!(
        rows[0].get("n.seen"),
        Some(&neo4r_query::QueryValue::Scalar(Value::Int(2)))
    );

    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Query,
            10,
            b"MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN r".to_vec(),
        ),
    )
    .unwrap();
    let rows = read_native_payload(&mut stream, NativeMessageType::Response, 10);
    let parts = rows.split('\t').collect::<Vec<_>>();
    assert_eq!(parts[0..2], ["OK", "RESULT_START"]);
    assert_eq!(parts[4], "1");
    let rows = decode_query_rows(parts[6]).unwrap();
    let Some(neo4r_query::QueryValue::Relationship(relationship)) = rows[0].get("r") else {
        panic!("expected committed relationship row");
    };
    assert_eq!(relationship.properties.get("created"), None);
    assert_eq!(relationship.properties.get("seen"), Some(&Value::Int(2)));

    write_frame(
        &mut stream,
        &NativeFrame::new(NativeMessageType::Quit, 11, Vec::new()),
    )
    .unwrap();
    assert_native_response(&mut stream, NativeMessageType::Response, 11, "OK\tBYE");

    server.join().unwrap();
    let _ = fs::remove_dir_all(dir);
}

#[test]
fn native_read_write_transaction_forwards_commit_to_remote_primary() {
    let local_dir = temp_dir("neo4r-native-tx-forward-local");
    let remote_dir = temp_dir("neo4r-native-tx-forward-remote");
    let routing_table = ShardRoutingTable {
        version: 9,
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
            NativeMessageType::Command,
            1,
            b"BEGIN_TX\tREAD_WRITE".to_vec(),
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
                "TX_QUERY\t{tx_id}\tCREATE (n:Person {{name: $name}}) RETURN n\tname=s:TxRemote"
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
            format!("COMMIT_TX\t{tx_id}").into_bytes(),
        ),
    )
    .unwrap();
    assert_native_response(
        &mut stream,
        NativeMessageType::Response,
        3,
        &format!("OK\tTX_COMMIT\t{tx_id}\t1"),
    );

    write_frame(
        &mut stream,
        &NativeFrame::new(NativeMessageType::Quit, 4, Vec::new()),
    )
    .unwrap();
    assert_native_response(&mut stream, NativeMessageType::Response, 4, "OK\tBYE");

    local_server.join().unwrap();
    remote_server.join().unwrap();
    assert_eq!(
        remote_db
            .query(r#"MATCH (n:Person) WHERE n.name = "TxRemote" RETURN n.name"#)
            .unwrap()
            .len(),
        1
    );
    let decisions = TransactionDecisionStore::open(&local_dir)
        .unwrap()
        .load()
        .unwrap();
    assert!(
        decisions.is_empty(),
        "successful remote 2PC commit should clear decision log for tx {tx_id}"
    );

    drop(local_db);
    drop(remote_db);
    let _ = fs::remove_dir_all(local_dir);
    let _ = fs::remove_dir_all(remote_dir);
}

#[test]
fn native_read_write_transaction_prepared_commits_remote_merge_node() {
    let local_dir = temp_dir("neo4r-native-tx-merge-forward-local");
    let remote_dir = temp_dir("neo4r-native-tx-merge-forward-remote");
    let routing_table = ShardRoutingTable {
        version: 15,
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
            NativeMessageType::Command,
            1,
            b"BEGIN_TX\tREAD_WRITE".to_vec(),
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
                    "TX_QUERY\t{tx_id}\tMERGE (n:Person {{email: $email}}) ON CREATE SET n.created = $created ON MATCH SET n.seen = $seen RETURN n\temail=s:merge@example.com\tcreated=i:1\tseen=i:2"
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
            format!("COMMIT_TX\t{tx_id}").into_bytes(),
        ),
    )
    .unwrap();
    assert_native_response(
        &mut stream,
        NativeMessageType::Response,
        3,
        &format!("OK\tTX_COMMIT\t{tx_id}\t1"),
    );

    write_frame(
        &mut stream,
        &NativeFrame::new(NativeMessageType::Quit, 4, Vec::new()),
    )
    .unwrap();
    assert_native_response(&mut stream, NativeMessageType::Response, 4, "OK\tBYE");

    local_server.join().unwrap();
    remote_server.join().unwrap();
    let rows = remote_db
        .query(r#"MATCH (n:Person) WHERE n.email = "merge@example.com" RETURN n"#)
        .unwrap();
    assert_eq!(rows.len(), 1);
    let Some(neo4r_query::QueryValue::Node(node)) = rows[0].get("n") else {
        panic!("expected merged node");
    };
    assert_eq!(node.properties.get("created"), Some(&Value::Int(1)));
    assert_eq!(node.properties.get("seen"), None);
    let decisions = TransactionDecisionStore::open(&local_dir)
        .unwrap()
        .load()
        .unwrap();
    assert!(
        decisions.is_empty(),
        "successful remote merge 2PC commit should clear decision log for tx {tx_id}"
    );

    drop(local_db);
    drop(remote_db);
    let _ = fs::remove_dir_all(local_dir);
    let _ = fs::remove_dir_all(remote_dir);
}

#[test]
fn native_prepared_write_batch_commits_merge_relationship() {
    let dir = temp_dir("neo4r-native-prepared-merge-relationship");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1)).unwrap();
    db.create_node_on_shard(
        0,
        vec!["Person".to_string()],
        [("name".to_string(), Value::String("Alice".to_string()))]
            .into_iter()
            .collect(),
    )
    .unwrap();
    db.create_node_on_shard(
        0,
        vec!["Person".to_string()],
        [("name".to_string(), Value::String("Bob".to_string()))]
            .into_iter()
            .collect(),
    )
    .unwrap();

    let backend = TcpBackend::new(db.clone());
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let server = thread::spawn(move || backend.serve_listener_once(listener).unwrap());

    let query = "MATCH (a:Person) WHERE a.name = $from MATCH (b:Person) WHERE b.name = $to MERGE (a)-[r:KNOWS {since: $since}]->(b) ON CREATE SET r.created = $created ON MATCH SET r.seen = $seen RETURN r";
    let mut params = neo4r_query::QueryParams::new();
    params.insert("from".to_string(), Value::String("Alice".to_string()));
    params.insert("to".to_string(), Value::String("Bob".to_string()));
    params.insert("since".to_string(), Value::Int(2026));
    params.insert("created".to_string(), Value::Int(1));
    params.insert("seen".to_string(), Value::Int(2));
    let writes = vec![(query.to_string(), params)];
    let prepare_payload = format_tx_prepare_write_batch_shard_payload(0, &writes);

    let mut stream = TcpStream::connect(addr).unwrap();
    write_frame(
        &mut stream,
        &NativeFrame::new(NativeMessageType::Command, 1, prepare_payload.into_bytes()),
    )
    .unwrap();
    let prepared = read_native_payload(&mut stream, NativeMessageType::Response, 1);
    let prepared_id = parse_tx_prepared_response(&prepared).unwrap();

    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Command,
            2,
            format!("TX_COMMIT_PREPARED\t{prepared_id}").into_bytes(),
        ),
    )
    .unwrap();
    assert_native_response(
        &mut stream,
        NativeMessageType::Response,
        2,
        &format!("OK\tTX_PREPARED_COMMIT\t{prepared_id}"),
    );

    write_frame(
        &mut stream,
        &NativeFrame::new(NativeMessageType::Quit, 3, Vec::new()),
    )
    .unwrap();
    assert_native_response(&mut stream, NativeMessageType::Response, 3, "OK\tBYE");

    server.join().unwrap();
    let rows = db
        .query("MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN r")
        .unwrap();
    assert_eq!(rows.len(), 1);
    let Some(neo4r_query::QueryValue::Relationship(relationship)) = rows[0].get("r") else {
        panic!("expected merged relationship");
    };
    assert_eq!(
        relationship.properties.get("since"),
        Some(&Value::Int(2026))
    );
    assert_eq!(relationship.properties.get("created"), Some(&Value::Int(1)));
    assert_eq!(relationship.properties.get("seen"), None);

    drop(db);
    let _ = fs::remove_dir_all(dir);
}

#[test]
fn native_prepared_write_batch_commits_merge_property_replacements() {
    let dir = temp_dir("neo4r-native-prepared-merge-replacements");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1)).unwrap();
    db.create_node_on_shard(
        0,
        vec!["Person".to_string()],
        [
            (
                "email".to_string(),
                Value::String("alice@example.com".to_string()),
            ),
            ("created".to_string(), Value::Int(1)),
        ]
        .into_iter()
        .collect(),
    )
    .unwrap();
    db.create_node_on_shard(
        0,
        vec!["Person".to_string()],
        [("name".to_string(), Value::String("Alice".to_string()))]
            .into_iter()
            .collect(),
    )
    .unwrap();
    db.create_node_on_shard(
        0,
        vec!["Person".to_string()],
        [("name".to_string(), Value::String("Bob".to_string()))]
            .into_iter()
            .collect(),
    )
    .unwrap();

    let backend = TcpBackend::new(db.clone());
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let server = thread::spawn(move || backend.serve_listener_once(listener).unwrap());

    let mut node_params = neo4r_query::QueryParams::new();
    node_params.insert(
        "email".to_string(),
        Value::String("alice@example.com".to_string()),
    );
    node_params.insert("created".to_string(), Value::Int(10));
    node_params.insert("seen".to_string(), Value::Int(2));
    let mut rel_params = neo4r_query::QueryParams::new();
    rel_params.insert("from".to_string(), Value::String("Alice".to_string()));
    rel_params.insert("to".to_string(), Value::String("Bob".to_string()));
    rel_params.insert("since".to_string(), Value::Int(2026));
    rel_params.insert("created".to_string(), Value::Int(1));
    rel_params.insert("seen".to_string(), Value::Int(2));
    let writes = vec![
            (
                "MERGE (n:Person {email: $email}) ON CREATE SET n = {email: $email, created: $created} ON MATCH SET n = {email: $email, seen: $seen} RETURN n".to_string(),
                node_params,
            ),
            (
                "MATCH (a:Person) WHERE a.name = $from MATCH (b:Person) WHERE b.name = $to MERGE (a)-[r:KNOWS {since: $since}]->(b) ON CREATE SET r = {since: $since, created: $created} ON MATCH SET r = {since: $since, seen: $seen} RETURN r".to_string(),
                rel_params,
            ),
        ];
    let prepare_payload = format_tx_prepare_write_batch_shard_payload(0, &writes);

    let mut stream = TcpStream::connect(addr).unwrap();
    write_frame(
        &mut stream,
        &NativeFrame::new(NativeMessageType::Command, 1, prepare_payload.into_bytes()),
    )
    .unwrap();
    let prepared = read_native_payload(&mut stream, NativeMessageType::Response, 1);
    let prepared_id = parse_tx_prepared_response(&prepared).unwrap();

    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Command,
            2,
            format!("TX_COMMIT_PREPARED\t{prepared_id}").into_bytes(),
        ),
    )
    .unwrap();
    assert_native_response(
        &mut stream,
        NativeMessageType::Response,
        2,
        &format!("OK\tTX_PREPARED_COMMIT\t{prepared_id}"),
    );

    write_frame(
        &mut stream,
        &NativeFrame::new(NativeMessageType::Quit, 3, Vec::new()),
    )
    .unwrap();
    assert_native_response(&mut stream, NativeMessageType::Response, 3, "OK\tBYE");

    server.join().unwrap();
    let rows = db
        .query(r#"MATCH (n:Person) WHERE n.email = "alice@example.com" RETURN n"#)
        .unwrap();
    assert_eq!(rows.len(), 1);
    let Some(neo4r_query::QueryValue::Node(node)) = rows[0].get("n") else {
        panic!("expected merged node");
    };
    assert_eq!(node.properties.get("created"), None);
    assert_eq!(node.properties.get("seen"), Some(&Value::Int(2)));
    let rows = db
        .query("MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN r")
        .unwrap();
    assert_eq!(rows.len(), 1);
    let Some(neo4r_query::QueryValue::Relationship(relationship)) = rows[0].get("r") else {
        panic!("expected merged relationship");
    };
    assert_eq!(
        relationship.properties.get("since"),
        Some(&Value::Int(2026))
    );
    assert_eq!(relationship.properties.get("created"), Some(&Value::Int(1)));
    assert_eq!(relationship.properties.get("seen"), None);

    drop(db);
    let _ = fs::remove_dir_all(dir);
}

#[test]
fn native_read_write_transaction_prepared_commits_remote_merge_relationship() {
    let local_dir = temp_dir("neo4r-native-tx-merge-rel-forward-local");
    let remote_dir = temp_dir("neo4r-native-tx-merge-rel-forward-remote");
    let routing_table = ShardRoutingTable {
        version: 17,
        placements: vec![ShardPlacement::new(
            0,
            vec![ShardReplica::primary(2), ShardReplica::replica(1)],
        )],
    };
    let local_db =
        Neo4rDatabaseHandle::open(DatabaseConfig::new(&local_dir, 1, 1).with_server_id(1)).unwrap();
    local_db
        .create_node_on_shard(
            0,
            vec!["Person".to_string()],
            [("name".to_string(), Value::String("Alice".to_string()))]
                .into_iter()
                .collect(),
        )
        .unwrap();
    local_db
        .create_node_on_shard(
            0,
            vec!["Person".to_string()],
            [("name".to_string(), Value::String("Bob".to_string()))]
                .into_iter()
                .collect(),
        )
        .unwrap();
    local_db
        .install_routing_table(routing_table.clone())
        .unwrap();

    let remote_db = Neo4rDatabaseHandle::open(
        DatabaseConfig::new(&remote_dir, 1, 1)
            .with_server_id(2)
            .with_routing_table(routing_table),
    )
    .unwrap();
    remote_db
        .create_node_on_shard(
            0,
            vec!["Person".to_string()],
            [("name".to_string(), Value::String("Alice".to_string()))]
                .into_iter()
                .collect(),
        )
        .unwrap();
    remote_db
        .create_node_on_shard(
            0,
            vec!["Person".to_string()],
            [("name".to_string(), Value::String("Bob".to_string()))]
                .into_iter()
                .collect(),
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
            NativeMessageType::Command,
            1,
            b"BEGIN_TX\tREAD_WRITE".to_vec(),
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
                    "TX_QUERY\t{tx_id}\tMATCH (a:Person) WHERE a.name = $from MATCH (b:Person) WHERE b.name = $to MERGE (a)-[r:KNOWS {{since: $since}}]->(b) ON CREATE SET r.created = $created ON MATCH SET r.seen = $seen RETURN r\tfrom=s:Alice\tto=s:Bob\tsince=i:2026\tcreated=i:1\tseen=i:2"
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
            format!("COMMIT_TX\t{tx_id}").into_bytes(),
        ),
    )
    .unwrap();
    assert_native_response(
        &mut stream,
        NativeMessageType::Response,
        3,
        &format!("OK\tTX_COMMIT\t{tx_id}\t1"),
    );

    write_frame(
        &mut stream,
        &NativeFrame::new(NativeMessageType::Quit, 4, Vec::new()),
    )
    .unwrap();
    assert_native_response(&mut stream, NativeMessageType::Response, 4, "OK\tBYE");

    local_server.join().unwrap();
    remote_server.join().unwrap();
    assert!(local_db
        .query("MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN r")
        .unwrap()
        .is_empty());
    let rows = remote_db
        .query("MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN r")
        .unwrap();
    assert_eq!(rows.len(), 1);
    let Some(neo4r_query::QueryValue::Relationship(relationship)) = rows[0].get("r") else {
        panic!("expected merged relationship");
    };
    assert_eq!(
        relationship.properties.get("since"),
        Some(&Value::Int(2026))
    );
    assert_eq!(relationship.properties.get("created"), Some(&Value::Int(1)));
    assert_eq!(relationship.properties.get("seen"), None);
    let decisions = TransactionDecisionStore::open(&local_dir)
        .unwrap()
        .load()
        .unwrap();
    assert!(
        decisions.is_empty(),
        "successful remote relationship 2PC commit should clear decision log for tx {tx_id}"
    );

    drop(local_db);
    drop(remote_db);
    let _ = fs::remove_dir_all(local_dir);
    let _ = fs::remove_dir_all(remote_dir);
}

#[test]
fn native_read_write_transaction_batch_forwards_sets_to_remote_primary() {
    let local_dir = temp_dir("neo4r-native-tx-batch-forward-local");
    let remote_dir = temp_dir("neo4r-native-tx-batch-forward-remote");
    let routing_table = ShardRoutingTable {
        version: 10,
        placements: vec![ShardPlacement::new(
            0,
            vec![ShardReplica::primary(2), ShardReplica::replica(1)],
        )],
    };
    let local_db =
        Neo4rDatabaseHandle::open(DatabaseConfig::new(&local_dir, 1, 1).with_server_id(1)).unwrap();
    local_db
        .create_node_on_shard(
            0,
            vec!["Person".to_string()],
            [("name".to_string(), Value::String("Alice".to_string()))]
                .into_iter()
                .collect(),
        )
        .unwrap();
    local_db
        .create_node_on_shard(
            0,
            vec!["Person".to_string()],
            [("name".to_string(), Value::String("Bob".to_string()))]
                .into_iter()
                .collect(),
        )
        .unwrap();
    local_db
        .install_routing_table(routing_table.clone())
        .unwrap();
    let remote_db = Neo4rDatabaseHandle::open(
        DatabaseConfig::new(&remote_dir, 1, 1)
            .with_server_id(2)
            .with_routing_table(routing_table),
    )
    .unwrap();
    remote_db
        .create_node_on_shard(
            0,
            vec!["Person".to_string()],
            [("name".to_string(), Value::String("Alice".to_string()))]
                .into_iter()
                .collect(),
        )
        .unwrap();
    remote_db
        .create_node_on_shard(
            0,
            vec!["Person".to_string()],
            [("name".to_string(), Value::String("Bob".to_string()))]
                .into_iter()
                .collect(),
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
            NativeMessageType::Command,
            1,
            b"BEGIN_TX\tREAD_WRITE".to_vec(),
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
                    "TX_QUERY\t{tx_id}\tMATCH (n:Person) WHERE n.name = $name SET n.status = $status\tname=s:Alice\tstatus=s:remote"
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
                    "TX_QUERY\t{tx_id}\tMATCH (n:Person) WHERE n.name = $name SET n.reviewed = $reviewed\tname=s:Alice\treviewed=b:true"
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
        &NativeFrame::new(NativeMessageType::Quit, 5, Vec::new()),
    )
    .unwrap();
    assert_native_response(&mut stream, NativeMessageType::Response, 5, "OK\tBYE");

    local_server.join().unwrap();
    remote_server.join().unwrap();
    let rows = remote_db
        .query(r#"MATCH (n:Person) WHERE n.status = "remote" RETURN n.reviewed"#)
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get("n.reviewed"),
        Some(&neo4r_query::QueryValue::Scalar(Value::Bool(true)))
    );
    assert_eq!(remote_db.committed_indexes().unwrap(), vec![4]);

    drop(local_db);
    drop(remote_db);
    let _ = fs::remove_dir_all(local_dir);
    let _ = fs::remove_dir_all(remote_dir);
}

#[test]
fn native_read_write_transaction_prepared_commits_remote_detach_delete() {
    let local_dir = temp_dir("neo4r-native-tx-remote-detach-delete-local");
    let remote_dir = temp_dir("neo4r-native-tx-remote-detach-delete-remote");
    let routing_table = ShardRoutingTable {
        version: 10,
        placements: vec![ShardPlacement::new(
            0,
            vec![ShardReplica::primary(2), ShardReplica::replica(1)],
        )],
    };
    let local_db =
        Neo4rDatabaseHandle::open(DatabaseConfig::new(&local_dir, 1, 1).with_server_id(1)).unwrap();
    let local_alice = local_db
        .create_node_on_shard(
            0,
            vec!["Person".to_string()],
            [("name".to_string(), Value::String("Alice".to_string()))]
                .into_iter()
                .collect(),
        )
        .unwrap();
    let local_bob = local_db
        .create_node_on_shard(
            0,
            vec!["Person".to_string()],
            [("name".to_string(), Value::String("Bob".to_string()))]
                .into_iter()
                .collect(),
        )
        .unwrap();
    local_db
        .create_relationship(
            local_alice,
            local_bob,
            "KNOWS".to_string(),
            Default::default(),
        )
        .unwrap();
    local_db
        .install_routing_table(routing_table.clone())
        .unwrap();

    let remote_db = Neo4rDatabaseHandle::open(
        DatabaseConfig::new(&remote_dir, 1, 1)
            .with_server_id(2)
            .with_routing_table(routing_table),
    )
    .unwrap();
    let remote_alice = remote_db
        .create_node_on_shard(
            0,
            vec!["Person".to_string()],
            [("name".to_string(), Value::String("Alice".to_string()))]
                .into_iter()
                .collect(),
        )
        .unwrap();
    let remote_bob = remote_db
        .create_node_on_shard(
            0,
            vec!["Person".to_string()],
            [("name".to_string(), Value::String("Bob".to_string()))]
                .into_iter()
                .collect(),
        )
        .unwrap();
    remote_db
        .create_relationship(
            remote_alice,
            remote_bob,
            "KNOWS".to_string(),
            Default::default(),
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
            NativeMessageType::Command,
            1,
            b"BEGIN_TX\tREAD_WRITE".to_vec(),
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
            format!("COMMIT_TX\t{tx_id}").into_bytes(),
        ),
    )
    .unwrap();
    assert_native_response(
        &mut stream,
        NativeMessageType::Response,
        3,
        &format!("OK\tTX_COMMIT\t{tx_id}\t1"),
    );
    write_frame(
        &mut stream,
        &NativeFrame::new(NativeMessageType::Quit, 4, Vec::new()),
    )
    .unwrap();
    assert_native_response(&mut stream, NativeMessageType::Response, 4, "OK\tBYE");

    local_server.join().unwrap();
    remote_server.join().unwrap();
    assert_eq!(
        local_db
            .query(r#"MATCH (n:Person) WHERE n.name = "Bob" RETURN n"#)
            .unwrap()
            .len(),
        1
    );
    assert!(remote_db
        .query(r#"MATCH (n:Person) WHERE n.name = "Bob" RETURN n"#)
        .unwrap()
        .is_empty());
    assert!(remote_db
        .query("MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN r")
        .unwrap()
        .is_empty());

    drop(local_db);
    drop(remote_db);
    let _ = fs::remove_dir_all(local_dir);
    let _ = fs::remove_dir_all(remote_dir);
}

#[test]
fn backend_recovers_commit_decision_for_remote_prepared_participant() {
    let local_dir = temp_dir("neo4r-tx-decision-recovery-local");
    let remote_dir = temp_dir("neo4r-tx-decision-recovery-remote");
    let local_db =
        Neo4rDatabaseHandle::open(DatabaseConfig::new(&local_dir, 1, 1).with_server_id(1)).unwrap();
    let local_backend = TcpBackend::new(local_db.clone());
    let remote_db =
        Neo4rDatabaseHandle::open(DatabaseConfig::new(&remote_dir, 1, 1).with_server_id(2))
            .unwrap();
    remote_db
        .create_node(
            vec!["Person".to_string()],
            [("name".to_string(), Value::String("Alice".to_string()))]
                .into_iter()
                .collect(),
        )
        .unwrap();

    let remote_backend = TcpBackend::new(remote_db.clone());
    let remote_listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let remote_addr = remote_listener.local_addr().unwrap();
    let remote_server = thread::spawn(move || {
        for _ in 0..2 {
            let (stream, _) = remote_listener.accept().unwrap();
            remote_backend.handle_stream(stream).unwrap();
        }
    });

    let writes = vec![(
        "MATCH (n:Person) SET n.status = $status".to_string(),
        [("status".to_string(), Value::String("recovered".to_string()))]
            .into_iter()
            .collect(),
    )];
    let mut stream = TcpStream::connect(remote_addr).unwrap();
    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Command,
            1,
            format_tx_prepare_write_batch_shard_payload(0, &writes).into_bytes(),
        ),
    )
    .unwrap();
    let prepared = read_native_payload(&mut stream, NativeMessageType::Response, 1);
    let prepared_id = parse_tx_prepared_response(&prepared).unwrap();
    write_frame(
        &mut stream,
        &NativeFrame::new(NativeMessageType::Quit, 2, Vec::new()),
    )
    .unwrap();
    assert_native_response(&mut stream, NativeMessageType::Response, 2, "OK\tBYE");
    assert!(remote_db
        .query(r#"MATCH (n:Person) WHERE n.status = "recovered" RETURN n"#)
        .unwrap()
        .is_empty());

    let decision_store = TransactionDecisionStore::open(&local_dir).unwrap();
    decision_store
        .append(&TransactionDecisionRecord {
            tx_id: 99,
            decision: TransactionDecision::Commit,
            participants: vec![TransactionParticipantRecord {
                location: format!("remote:{remote_addr}"),
                shard_id: 0,
                prepared_id,
            }],
        })
        .unwrap();

    assert_eq!(local_backend.recover_transaction_decisions().unwrap(), 1);
    assert!(decision_store.load().unwrap().is_empty());
    assert_eq!(local_backend.recover_transaction_decisions().unwrap(), 0);
    assert_eq!(
        remote_db
            .query(r#"MATCH (n:Person) WHERE n.status = "recovered" RETURN n"#)
            .unwrap()
            .len(),
        1
    );

    remote_server.join().unwrap();
    drop(local_backend);
    drop(local_db);
    drop(remote_db);
    let _ = fs::remove_dir_all(local_dir);
    let _ = fs::remove_dir_all(remote_dir);
}

#[test]
fn native_command_recovers_remote_transaction_decisions_on_demand() {
    let local_dir = temp_dir("neo4r-tx-decision-command-recovery-local");
    let remote_dir = temp_dir("neo4r-tx-decision-command-recovery-remote");
    let local_db =
        Neo4rDatabaseHandle::open(DatabaseConfig::new(&local_dir, 1, 1).with_server_id(1)).unwrap();
    let local_backend = TcpBackend::new(local_db.clone());
    let remote_db =
        Neo4rDatabaseHandle::open(DatabaseConfig::new(&remote_dir, 1, 1).with_server_id(2))
            .unwrap();
    remote_db
        .create_node(
            vec!["Person".to_string()],
            [("name".to_string(), Value::String("Alice".to_string()))]
                .into_iter()
                .collect(),
        )
        .unwrap();

    let remote_backend = TcpBackend::new(remote_db.clone());
    let remote_listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let remote_addr = remote_listener.local_addr().unwrap();
    let remote_server = thread::spawn(move || {
        for _ in 0..2 {
            let (stream, _) = remote_listener.accept().unwrap();
            remote_backend.handle_stream(stream).unwrap();
        }
    });

    let writes = vec![(
        "MATCH (n:Person) SET n.status = $status".to_string(),
        [(
            "status".to_string(),
            Value::String("command_recovered".to_string()),
        )]
        .into_iter()
        .collect(),
    )];
    let mut stream = TcpStream::connect(remote_addr).unwrap();
    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Command,
            1,
            format_tx_prepare_write_batch_shard_payload(0, &writes).into_bytes(),
        ),
    )
    .unwrap();
    let prepared = read_native_payload(&mut stream, NativeMessageType::Response, 1);
    let prepared_id = parse_tx_prepared_response(&prepared).unwrap();
    write_frame(
        &mut stream,
        &NativeFrame::new(NativeMessageType::Quit, 2, Vec::new()),
    )
    .unwrap();
    assert_native_response(&mut stream, NativeMessageType::Response, 2, "OK\tBYE");

    let decision_store = TransactionDecisionStore::open(&local_dir).unwrap();
    decision_store
        .append(&TransactionDecisionRecord {
            tx_id: 104,
            decision: TransactionDecision::Commit,
            participants: vec![TransactionParticipantRecord {
                location: format!("remote:{remote_addr}"),
                shard_id: 0,
                prepared_id,
            }],
        })
        .unwrap();

    assert_eq!(
        local_backend.execute_backend_request(parse_request("RECOVER_TX_DECISIONS").unwrap()),
        BackendResponse::OkTransactionRecovery(1)
    );
    assert!(decision_store.load().unwrap().is_empty());
    assert_eq!(
        remote_db
            .query(r#"MATCH (n:Person) WHERE n.status = "command_recovered" RETURN n"#)
            .unwrap()
            .len(),
        1
    );

    remote_server.join().unwrap();
    drop(local_backend);
    drop(local_db);
    drop(remote_db);
    let _ = fs::remove_dir_all(local_dir);
    let _ = fs::remove_dir_all(remote_dir);
}

#[test]
fn persistent_backend_recovers_commit_decision_for_local_prepared_participant() {
    let dir = temp_dir("neo4r-local-tx-decision-recovery");
    let config = DatabaseConfig::new(&dir, 1, 1).with_server_id(1);
    let db = Neo4rDatabaseHandle::open(config.clone()).unwrap();
    db.create_node(
        vec!["Person".to_string()],
        [("name".to_string(), Value::String("Alice".to_string()))]
            .into_iter()
            .collect(),
    )
    .unwrap();

    let backend =
        TcpBackend::with_persistent_config(db.clone(), TcpBackendConfig::default()).unwrap();
    let writes = vec![(
        "MATCH (n:Person) SET n.status = $status".to_string(),
        [("status".to_string(), Value::String("recovered".to_string()))]
            .into_iter()
            .collect(),
    )];
    let prepared_id = backend.prepared_transactions.prepare(0, writes).unwrap();
    assert!(db
        .query(r#"MATCH (n:Person) WHERE n.status = "recovered" RETURN n"#)
        .unwrap()
        .is_empty());

    let decision_store = TransactionDecisionStore::open(&dir).unwrap();
    decision_store
        .append(&TransactionDecisionRecord {
            tx_id: 101,
            decision: TransactionDecision::Commit,
            participants: vec![TransactionParticipantRecord {
                location: "local".to_string(),
                shard_id: 0,
                prepared_id,
            }],
        })
        .unwrap();
    drop(backend);
    drop(db);

    let db = Neo4rDatabaseHandle::open(config).unwrap();
    let backend =
        TcpBackend::with_persistent_config(db.clone(), TcpBackendConfig::default()).unwrap();
    assert!(decision_store.load().unwrap().is_empty());
    assert_eq!(
        db.query(r#"MATCH (n:Person) WHERE n.status = "recovered" RETURN n"#)
            .unwrap()
            .len(),
        1
    );
    assert!(PreparedTransactionStore::open(
        dir.join("transactions").join(PREPARED_TRANSACTIONS_FILE)
    )
    .unwrap()
    .take(prepared_id)
    .unwrap_err()
    .contains("unknown prepared transaction"));
    assert_eq!(backend.recover_transaction_decisions().unwrap(), 0);

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn native_command_recovers_local_transaction_decisions_on_demand() {
    let dir = temp_dir("neo4r-local-tx-decision-command-recovery");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1).with_server_id(1)).unwrap();
    db.create_node(
        vec!["Person".to_string()],
        [("name".to_string(), Value::String("Alice".to_string()))]
            .into_iter()
            .collect(),
    )
    .unwrap();
    let backend = TcpBackend::new(db.clone());
    let writes = vec![(
        "MATCH (n:Person) SET n.status = $status".to_string(),
        [("status".to_string(), Value::String("recovered".to_string()))]
            .into_iter()
            .collect(),
    )];
    let prepared_id = backend.prepared_transactions.prepare(0, writes).unwrap();
    let decision_store = TransactionDecisionStore::open(&dir).unwrap();
    decision_store
        .append(&TransactionDecisionRecord {
            tx_id: 103,
            decision: TransactionDecision::Commit,
            participants: vec![TransactionParticipantRecord {
                location: "local".to_string(),
                shard_id: 0,
                prepared_id,
            }],
        })
        .unwrap();

    assert_eq!(
        backend.execute_backend_request(parse_request("RECOVER_TX_DECISIONS").unwrap()),
        BackendResponse::OkTransactionRecovery(1)
    );
    assert!(decision_store.load().unwrap().is_empty());
    assert_eq!(
        db.query(r#"MATCH (n:Person) WHERE n.status = "recovered" RETURN n"#)
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        backend.execute_backend_request(parse_request("RECOVER_TX_DECISIONS").unwrap()),
        BackendResponse::OkTransactionRecovery(0)
    );

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn native_command_lists_durable_transaction_decisions() {
    let dir = temp_dir("neo4r-tx-decision-command-list");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1).with_server_id(1)).unwrap();
    let backend = TcpBackend::new(db);
    let decision_store = TransactionDecisionStore::open(&dir).unwrap();
    decision_store
        .append(&TransactionDecisionRecord {
            tx_id: 201,
            decision: TransactionDecision::Commit,
            participants: vec![TransactionParticipantRecord {
                location: "local".to_string(),
                shard_id: 0,
                prepared_id: 11,
            }],
        })
        .unwrap();
    decision_store
        .append(&TransactionDecisionRecord {
            tx_id: 202,
            decision: TransactionDecision::Abort,
            participants: vec![TransactionParticipantRecord {
                location: "remote:127.0.0.1:17687".to_string(),
                shard_id: 1,
                prepared_id: 12,
            }],
        })
        .unwrap();

    let BackendResponse::OkTransactionDecisions(text) =
        backend.execute_backend_request(parse_request("LIST_TX_DECISIONS").unwrap())
    else {
        panic!("expected transaction decision list response");
    };
    assert!(text.contains("count=2"));
    assert!(text.contains("tx=201"));
    assert!(text.contains("decision=commit"));
    assert!(text.contains("local@0#11"));
    assert!(text.contains("tx=202"));
    assert!(text.contains("decision=abort"));
    assert!(text.contains("remote:127.0.0.1:17687@1#12"));

    decision_store
        .remove_tx_ids(&BTreeSet::from([201, 202]))
        .unwrap();
    assert_eq!(
        backend.execute_backend_request(parse_request("LIST_TX_DECISIONS").unwrap()),
        BackendResponse::OkTransactionDecisions("count=0 entries=".to_string())
    );

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn native_tcp_lists_and_recovers_durable_transaction_decisions() {
    let dir = temp_dir("neo4r-native-tx-decision-list-recover");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1).with_server_id(1)).unwrap();
    db.create_node(
        vec!["Person".to_string()],
        [("name".to_string(), Value::String("Alice".to_string()))]
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
    let writes = vec![(
        "MATCH (n:Person) SET n.status = $status".to_string(),
        [("status".to_string(), Value::String("recovered".to_string()))]
            .into_iter()
            .collect(),
    )];
    let prepared_id = backend.prepared_transactions.prepare(0, writes).unwrap();
    let decision_store = TransactionDecisionStore::open(&dir).unwrap();
    decision_store
        .append(&TransactionDecisionRecord {
            tx_id: 203,
            decision: TransactionDecision::Commit,
            participants: vec![TransactionParticipantRecord {
                location: "local".to_string(),
                shard_id: 0,
                prepared_id,
            }],
        })
        .unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let server = thread::spawn(move || backend.serve_listener_once(listener).unwrap());

    let mut stream = TcpStream::connect(addr).unwrap();
    write_frame(
        &mut stream,
        &NativeFrame::new(NativeMessageType::Command, 1, b"LIST_TX_DECISIONS".to_vec()),
    )
    .unwrap();
    let list = read_native_payload(&mut stream, NativeMessageType::Response, 1);
    assert!(list.starts_with("OK\tTX_DECISIONS\tcount=1"));
    assert!(list.contains("tx=203"));
    assert!(list.contains("decision=commit"));
    assert!(list.contains(&format!("local@0#{prepared_id}")));

    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Command,
            2,
            b"RECOVER_TX_DECISIONS".to_vec(),
        ),
    )
    .unwrap();
    assert_native_response(
        &mut stream,
        NativeMessageType::Response,
        2,
        "OK\tTX_RECOVERY\t1",
    );

    write_frame(
        &mut stream,
        &NativeFrame::new(NativeMessageType::Command, 3, b"LIST_TX_DECISIONS".to_vec()),
    )
    .unwrap();
    assert_native_response(
        &mut stream,
        NativeMessageType::Response,
        3,
        "OK\tTX_DECISIONS\tcount=0 entries=",
    );

    write_frame(
        &mut stream,
        &NativeFrame::new(NativeMessageType::Quit, 4, Vec::new()),
    )
    .unwrap();
    assert_native_response(&mut stream, NativeMessageType::Response, 4, "OK\tBYE");
    server.join().unwrap();

    assert_eq!(
        db.query(r#"MATCH (n:Person) WHERE n.status = "recovered" RETURN n"#)
            .unwrap()
            .len(),
        1
    );
    assert!(decision_store.load().unwrap().is_empty());

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn persistent_backend_recovers_local_mixed_prepared_commit_with_staged_overlay() {
    let dir = temp_dir("neo4r-local-mixed-tx-decision-recovery");
    let config = DatabaseConfig::new(&dir, 1, 1).with_server_id(1);
    let db = Neo4rDatabaseHandle::open(config.clone()).unwrap();
    let backend =
        TcpBackend::with_persistent_config(db.clone(), TcpBackendConfig::default()).unwrap();
    let writes = vec![
        (
            "CREATE (n:Person {name: $name}) RETURN n".to_string(),
            [(
                "name".to_string(),
                Value::String("RecoveredCreate".to_string()),
            )]
            .into_iter()
            .collect(),
        ),
        (
            "MATCH (n:Person) WHERE n.name = $name SET n.status = $status".to_string(),
            [
                (
                    "name".to_string(),
                    Value::String("RecoveredCreate".to_string()),
                ),
                ("status".to_string(), Value::String("recovered".to_string())),
            ]
            .into_iter()
            .collect(),
        ),
    ];
    let prepared_id = backend.prepared_transactions.prepare(0, writes).unwrap();
    assert!(db
        .query(r#"MATCH (n:Person) WHERE n.name = "RecoveredCreate" RETURN n"#)
        .unwrap()
        .is_empty());

    let decision_store = TransactionDecisionStore::open(&dir).unwrap();
    decision_store
        .append(&TransactionDecisionRecord {
            tx_id: 102,
            decision: TransactionDecision::Commit,
            participants: vec![TransactionParticipantRecord {
                location: "local".to_string(),
                shard_id: 0,
                prepared_id,
            }],
        })
        .unwrap();
    drop(backend);
    drop(db);

    let db = Neo4rDatabaseHandle::open(config).unwrap();
    let backend =
        TcpBackend::with_persistent_config(db.clone(), TcpBackendConfig::default()).unwrap();
    assert!(decision_store.load().unwrap().is_empty());
    assert_eq!(
        db.query(r#"MATCH (n:Person) WHERE n.name = "RecoveredCreate" RETURN n.status"#)
            .unwrap()[0]
            .get("n.status"),
        Some(&neo4r_query::QueryValue::Scalar(Value::String(
            "recovered".to_string()
        )))
    );
    assert!(PreparedTransactionStore::open(
        dir.join("transactions").join(PREPARED_TRANSACTIONS_FILE)
    )
    .unwrap()
    .take(prepared_id)
    .unwrap_err()
    .contains("unknown prepared transaction"));
    assert_eq!(backend.recover_transaction_decisions().unwrap(), 0);

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn persistent_prepared_transaction_store_removes_committed_batches() {
    let dir = temp_dir("neo4r-prepared-store-remove");
    let path = dir.join("transactions").join(PREPARED_TRANSACTIONS_FILE);
    let store = PreparedTransactionStore::open(&path).unwrap();
    let prepared_id = store
        .prepare(
            3,
            vec![(
                "MATCH (n:Person) SET n.status = $status".to_string(),
                [("status".to_string(), Value::String("active".to_string()))]
                    .into_iter()
                    .collect(),
            )],
        )
        .unwrap();
    let reloaded = PreparedTransactionStore::open(&path).unwrap();
    let batch = reloaded.take(prepared_id).unwrap();
    assert_eq!(batch.shard_id, 3);
    assert_eq!(batch.writes.len(), 1);
    assert!(PreparedTransactionStore::open(&path)
        .unwrap()
        .take(prepared_id)
        .unwrap_err()
        .contains("unknown prepared transaction"));

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn persistent_prepared_transaction_store_keeps_concurrent_prepares() {
    let dir = temp_dir("neo4r-prepared-store-concurrent");
    let path = dir.join("transactions").join(PREPARED_TRANSACTIONS_FILE);
    let store = PreparedTransactionStore::open(&path).unwrap();
    let thread_count = 8;
    let barrier = Arc::new(Barrier::new(thread_count));
    let mut workers = Vec::new();

    for worker_id in 0..thread_count {
        let store = store.clone();
        let barrier = barrier.clone();
        workers.push(thread::spawn(move || {
            barrier.wait();
            store
                .prepare(
                    worker_id as u64,
                    vec![(
                        "MATCH (n:Person) SET n.worker = $worker".to_string(),
                        [("worker".to_string(), Value::Int(worker_id as i64))]
                            .into_iter()
                            .collect(),
                    )],
                )
                .unwrap()
        }));
    }

    let mut prepared_ids = Vec::new();
    for worker in workers {
        prepared_ids.push(worker.join().unwrap());
    }
    prepared_ids.sort_unstable();

    let reloaded = PreparedTransactionStore::open(&path).unwrap();
    for prepared_id in prepared_ids {
        let batch = reloaded.take(prepared_id).unwrap();
        assert_eq!(batch.writes.len(), 1);
    }

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn persistent_prepared_transaction_store_reports_status() {
    let dir = temp_dir("neo4r-prepared-store-status");
    let path = dir.join("transactions").join(PREPARED_TRANSACTIONS_FILE);
    let store = PreparedTransactionStore::open(&path).unwrap();
    let prepared_id = store
        .prepare(
            3,
            vec![(
                "MATCH (n:Person) SET n.status = $status".to_string(),
                [("status".to_string(), Value::String("active".to_string()))]
                    .into_iter()
                    .collect(),
            )],
        )
        .unwrap();

    let info = store.status(prepared_id).unwrap();
    assert_eq!(info.prepared_id, prepared_id);
    assert_eq!(info.shard_id, 3);
    assert_eq!(info.write_count, 1);
    store.take(prepared_id).unwrap();
    assert!(store
        .status(prepared_id)
        .unwrap_err()
        .contains("unknown prepared transaction"));

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn persistent_prepared_transaction_prepare_failure_rolls_back_memory() {
    let dir = temp_dir("neo4r-prepared-store-prepare-failure");
    let path = dir.join("transactions").join(PREPARED_TRANSACTIONS_FILE);
    let store = PreparedTransactionStore::open(&path).unwrap();
    fs::remove_file(&path).unwrap_or(());
    fs::create_dir_all(&path).unwrap();

    let err = store
        .prepare(
            0,
            vec![(
                "MATCH (n:Person) SET n.status = $status".to_string(),
                [("status".to_string(), Value::String("active".to_string()))]
                    .into_iter()
                    .collect(),
            )],
        )
        .unwrap_err();
    assert!(err.contains("prepared transaction store"));
    assert!(store
        .take(1)
        .unwrap_err()
        .contains("unknown prepared transaction"));

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn persistent_prepared_transaction_take_failure_restores_memory() {
    let dir = temp_dir("neo4r-prepared-store-take-failure");
    let path = dir.join("transactions").join(PREPARED_TRANSACTIONS_FILE);
    let store = PreparedTransactionStore::open(&path).unwrap();
    let prepared_id = store
        .prepare(
            0,
            vec![(
                "MATCH (n:Person) SET n.status = $status".to_string(),
                [("status".to_string(), Value::String("active".to_string()))]
                    .into_iter()
                    .collect(),
            )],
        )
        .unwrap();
    fs::remove_file(&path).unwrap();
    fs::create_dir_all(&path).unwrap();

    let err = store.take(prepared_id).unwrap_err();
    assert!(err.contains("prepared transaction store"));

    fs::remove_dir_all(&path).unwrap();
    let batch = store.take(prepared_id).unwrap();
    assert_eq!(batch.shard_id, 0);
    assert!(PreparedTransactionStore::open(&path)
        .unwrap()
        .take(prepared_id)
        .unwrap_err()
        .contains("unknown prepared transaction"));

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn backend_recovers_abort_decision_for_remote_prepared_participant() {
    let local_dir = temp_dir("neo4r-tx-abort-recovery-local");
    let remote_dir = temp_dir("neo4r-tx-abort-recovery-remote");
    let local_db =
        Neo4rDatabaseHandle::open(DatabaseConfig::new(&local_dir, 1, 1).with_server_id(1)).unwrap();
    let local_backend = TcpBackend::new(local_db.clone());
    let remote_db =
        Neo4rDatabaseHandle::open(DatabaseConfig::new(&remote_dir, 1, 1).with_server_id(2))
            .unwrap();
    remote_db
        .create_node(
            vec!["Person".to_string()],
            [("name".to_string(), Value::String("Alice".to_string()))]
                .into_iter()
                .collect(),
        )
        .unwrap();

    let remote_backend = TcpBackend::new(remote_db.clone());
    let remote_listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let remote_addr = remote_listener.local_addr().unwrap();
    let remote_server = thread::spawn(move || {
        for _ in 0..3 {
            let (stream, _) = remote_listener.accept().unwrap();
            remote_backend.handle_stream(stream).unwrap();
        }
    });

    let writes = vec![(
        "MATCH (n:Person) SET n.status = $status".to_string(),
        [("status".to_string(), Value::String("aborted".to_string()))]
            .into_iter()
            .collect(),
    )];
    let mut stream = TcpStream::connect(remote_addr).unwrap();
    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Command,
            1,
            format_tx_prepare_write_batch_shard_payload(0, &writes).into_bytes(),
        ),
    )
    .unwrap();
    let prepared = read_native_payload(&mut stream, NativeMessageType::Response, 1);
    let prepared_id = parse_tx_prepared_response(&prepared).unwrap();
    write_frame(
        &mut stream,
        &NativeFrame::new(NativeMessageType::Quit, 2, Vec::new()),
    )
    .unwrap();
    assert_native_response(&mut stream, NativeMessageType::Response, 2, "OK\tBYE");

    let decision_store = TransactionDecisionStore::open(&local_dir).unwrap();
    decision_store
        .append(&TransactionDecisionRecord {
            tx_id: 100,
            decision: TransactionDecision::Abort,
            participants: vec![TransactionParticipantRecord {
                location: format!("remote:{remote_addr}"),
                shard_id: 0,
                prepared_id,
            }],
        })
        .unwrap();

    assert_eq!(local_backend.recover_transaction_decisions().unwrap(), 1);
    assert!(decision_store.load().unwrap().is_empty());
    assert_eq!(local_backend.recover_transaction_decisions().unwrap(), 0);
    assert!(remote_db
        .query(r#"MATCH (n:Person) WHERE n.status = "aborted" RETURN n"#)
        .unwrap()
        .is_empty());

    let mut stream = TcpStream::connect(remote_addr).unwrap();
    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Command,
            3,
            format!("TX_COMMIT_PREPARED\t{prepared_id}").into_bytes(),
        ),
    )
    .unwrap();
    let err = read_native_payload(&mut stream, NativeMessageType::Error, 3);
    assert!(err.contains("unknown prepared transaction"));
    write_frame(
        &mut stream,
        &NativeFrame::new(NativeMessageType::Quit, 4, Vec::new()),
    )
    .unwrap();
    assert_native_response(&mut stream, NativeMessageType::Response, 4, "OK\tBYE");

    remote_server.join().unwrap();
    drop(local_backend);
    drop(local_db);
    drop(remote_db);
    let _ = fs::remove_dir_all(local_dir);
    let _ = fs::remove_dir_all(remote_dir);
}

#[test]
fn native_prepared_write_batch_aborts_or_commits_on_participant() {
    let dir = temp_dir("neo4r-native-prepared-batch");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1)).unwrap();
    db.create_node(
        vec!["Person".to_string()],
        [("name".to_string(), Value::String("Alice".to_string()))]
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

    let writes = vec![
        (
            "MATCH (n:Person) SET n.status = $status".to_string(),
            [("status".to_string(), Value::String("prepared".to_string()))]
                .into_iter()
                .collect(),
        ),
        (
            "CREATE (n:Person {name: $name}) RETURN n".to_string(),
            [("name".to_string(), Value::String("Carol".to_string()))]
                .into_iter()
                .collect(),
        ),
    ];
    let mut stream = TcpStream::connect(addr).unwrap();
    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Command,
            1,
            format_tx_prepare_write_batch_shard_payload(0, &writes).into_bytes(),
        ),
    )
    .unwrap();
    let prepared = read_native_payload(&mut stream, NativeMessageType::Response, 1);
    let prepared_id = parse_tx_prepared_response(&prepared).unwrap();

    write_frame(
        &mut stream,
        &NativeFrame::new(NativeMessageType::Command, 2, b"LIST_PREPARED_TX".to_vec()),
    )
    .unwrap();
    assert_native_response(
        &mut stream,
        NativeMessageType::Response,
        2,
        &format!("OK\tTX_PREPARED_LIST\t1\t{prepared_id}:0:2"),
    );

    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Command,
            3,
            format!("TX_PREPARED_STATUS\t{prepared_id}").into_bytes(),
        ),
    )
    .unwrap();
    assert_native_response(
        &mut stream,
        NativeMessageType::Response,
        3,
        &format!("OK\tTX_PREPARED_STATUS\t{prepared_id}\t0\t2"),
    );

    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Command,
            4,
            format!("TX_ABORT_PREPARED\t{prepared_id}").into_bytes(),
        ),
    )
    .unwrap();
    assert_native_response(
        &mut stream,
        NativeMessageType::Response,
        4,
        &format!("OK\tTX_PREPARED_ABORT\t{prepared_id}"),
    );
    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Command,
            5,
            format!("TX_PREPARED_STATUS\t{prepared_id}").into_bytes(),
        ),
    )
    .unwrap();
    let missing = read_native_payload(&mut stream, NativeMessageType::Error, 5);
    assert!(missing.contains("unknown prepared transaction"));
    write_frame(
        &mut stream,
        &NativeFrame::new(NativeMessageType::Command, 6, b"LIST_PREPARED_TX".to_vec()),
    )
    .unwrap();
    assert_native_response(
        &mut stream,
        NativeMessageType::Response,
        6,
        "OK\tTX_PREPARED_LIST\t0\t",
    );
    assert!(db
        .query(r#"MATCH (n:Person) WHERE n.status = "prepared" RETURN n"#)
        .unwrap()
        .is_empty());
    assert!(db
        .query(r#"MATCH (n:Person) WHERE n.name = "Carol" RETURN n"#)
        .unwrap()
        .is_empty());

    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Command,
            7,
            format_tx_prepare_write_batch_shard_payload(0, &writes).into_bytes(),
        ),
    )
    .unwrap();
    let prepared = read_native_payload(&mut stream, NativeMessageType::Response, 7);
    let prepared_id = parse_tx_prepared_response(&prepared).unwrap();
    write_frame(
        &mut stream,
        &NativeFrame::new(NativeMessageType::Command, 8, b"LIST_PREPARED_TX".to_vec()),
    )
    .unwrap();
    assert_native_response(
        &mut stream,
        NativeMessageType::Response,
        8,
        &format!("OK\tTX_PREPARED_LIST\t1\t{prepared_id}:0:2"),
    );
    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Command,
            9,
            format!("TX_COMMIT_PREPARED\t{prepared_id}").into_bytes(),
        ),
    )
    .unwrap();
    assert_native_response(
        &mut stream,
        NativeMessageType::Response,
        9,
        &format!("OK\tTX_PREPARED_COMMIT\t{prepared_id}"),
    );
    write_frame(
        &mut stream,
        &NativeFrame::new(NativeMessageType::Command, 10, b"LIST_PREPARED_TX".to_vec()),
    )
    .unwrap();
    assert_native_response(
        &mut stream,
        NativeMessageType::Response,
        10,
        "OK\tTX_PREPARED_LIST\t0\t",
    );

    assert_eq!(
        db.query(r#"MATCH (n:Person) WHERE n.status = "prepared" RETURN n"#)
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        db.query(r#"MATCH (n:Person) WHERE n.name = "Carol" RETURN n"#)
            .unwrap()
            .len(),
        1
    );

    write_frame(
        &mut stream,
        &NativeFrame::new(NativeMessageType::Quit, 11, Vec::new()),
    )
    .unwrap();
    assert_native_response(&mut stream, NativeMessageType::Response, 11, "OK\tBYE");

    server.join().unwrap();
    drop(db);
    let _ = fs::remove_dir_all(dir);
}

#[test]
fn native_read_write_transaction_group_commits_local_multi_shard_sets() {
    let dir = temp_dir("neo4r-native-multi-shard-tx-batch");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 2, 2)).unwrap();
    db.create_node_on_shard(
        0,
        vec!["Person".to_string()],
        [("name".to_string(), Value::String("Alice".to_string()))]
            .into_iter()
            .collect(),
    )
    .unwrap();
    db.create_node_on_shard(
        1,
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
            b"BEGIN_TX\tREAD_WRITE".to_vec(),
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
            format!("COMMIT_TX\t{tx_id}").into_bytes(),
        ),
    )
    .unwrap();
    assert_native_response(
        &mut stream,
        NativeMessageType::Response,
        3,
        &format!("OK\tTX_COMMIT\t{tx_id}\t1"),
    );

    write_frame(
        &mut stream,
        &NativeFrame::new(NativeMessageType::Quit, 4, Vec::new()),
    )
    .unwrap();
    assert_native_response(&mut stream, NativeMessageType::Response, 4, "OK\tBYE");

    server.join().unwrap();
    assert_eq!(
        db.query(r#"MATCH (n:Person) WHERE n.status = "active" RETURN n"#)
            .unwrap()
            .len(),
        2
    );
    assert_eq!(db.committed_indexes().unwrap(), vec![2, 2]);

    drop(db);
    let _ = fs::remove_dir_all(dir);
}

#[test]
fn native_read_write_transaction_prepare_commits_local_multi_shard_detach_delete() {
    let dir = temp_dir("neo4r-native-multi-shard-tx-detach-delete");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 2, 2)).unwrap();
    db.create_node_on_shard(
        0,
        vec!["Person".to_string()],
        [("name".to_string(), Value::String("Alice".to_string()))]
            .into_iter()
            .collect(),
    )
    .unwrap();
    db.create_node_on_shard(
        1,
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
            b"BEGIN_TX\tREAD_WRITE".to_vec(),
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
            format!("TX_QUERY\t{tx_id}\tMATCH (n:Person) DETACH DELETE n").into_bytes(),
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
            format!("COMMIT_TX\t{tx_id}").into_bytes(),
        ),
    )
    .unwrap();
    assert_native_response(
        &mut stream,
        NativeMessageType::Response,
        3,
        &format!("OK\tTX_COMMIT\t{tx_id}\t1"),
    );
    write_frame(
        &mut stream,
        &NativeFrame::new(NativeMessageType::Quit, 4, Vec::new()),
    )
    .unwrap();
    assert_native_response(&mut stream, NativeMessageType::Response, 4, "OK\tBYE");

    server.join().unwrap();
    assert!(db.query("MATCH (n:Person) RETURN n").unwrap().is_empty());
    assert_eq!(db.committed_indexes().unwrap(), vec![2, 2]);

    drop(db);
    let _ = fs::remove_dir_all(dir);
}

#[test]
fn native_read_write_transaction_prepare_commits_remote_multi_shard_sets() {
    let local_dir = temp_dir("neo4r-native-multi-remote-tx-local");
    let remote0_dir = temp_dir("neo4r-native-multi-remote-tx-remote0");
    let remote1_dir = temp_dir("neo4r-native-multi-remote-tx-remote1");
    let routing_table = ShardRoutingTable {
        version: 12,
        placements: vec![
            ShardPlacement::new(0, vec![ShardReplica::primary(2), ShardReplica::replica(1)]),
            ShardPlacement::new(1, vec![ShardReplica::primary(3), ShardReplica::replica(1)]),
        ],
    };
    let local_db =
        Neo4rDatabaseHandle::open(DatabaseConfig::new(&local_dir, 2, 2).with_server_id(1)).unwrap();
    local_db
        .create_node_on_shard(
            0,
            vec!["Person".to_string()],
            [("name".to_string(), Value::String("Alice".to_string()))]
                .into_iter()
                .collect(),
        )
        .unwrap();
    local_db
        .create_node_on_shard(
            1,
            vec!["Person".to_string()],
            [("name".to_string(), Value::String("Bob".to_string()))]
                .into_iter()
                .collect(),
        )
        .unwrap();
    local_db
        .install_routing_table(routing_table.clone())
        .unwrap();

    let remote0_db = Neo4rDatabaseHandle::open(
        DatabaseConfig::new(&remote0_dir, 2, 2)
            .with_server_id(2)
            .with_routing_table(routing_table.clone()),
    )
    .unwrap();
    remote0_db
        .create_node_on_shard(
            0,
            vec!["Person".to_string()],
            [("name".to_string(), Value::String("Alice".to_string()))]
                .into_iter()
                .collect(),
        )
        .unwrap();
    let remote1_db = Neo4rDatabaseHandle::open(
        DatabaseConfig::new(&remote1_dir, 2, 2)
            .with_server_id(3)
            .with_routing_table(routing_table),
    )
    .unwrap();
    remote1_db
        .create_node_on_shard(
            1,
            vec!["Person".to_string()],
            [("name".to_string(), Value::String("Bob".to_string()))]
                .into_iter()
                .collect(),
        )
        .unwrap();

    let remote0_backend = TcpBackend::new(remote0_db.clone());
    let remote0_listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let remote0_addr = remote0_listener.local_addr().unwrap();
    let remote0_server = thread::spawn(move || {
        remote0_backend
            .serve_listener_once(remote0_listener)
            .unwrap()
    });
    let remote1_backend = TcpBackend::new(remote1_db.clone());
    let remote1_listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let remote1_addr = remote1_listener.local_addr().unwrap();
    let remote1_server = thread::spawn(move || {
        remote1_backend
            .serve_listener_once(remote1_listener)
            .unwrap()
    });
    let local_backend = TcpBackend::new(local_db.clone());
    local_backend
        .register_query_peer(2, remote0_addr.to_string())
        .unwrap();
    local_backend
        .register_query_peer(3, remote1_addr.to_string())
        .unwrap();
    let local_listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let local_addr = local_listener.local_addr().unwrap();
    let local_server =
        thread::spawn(move || local_backend.serve_listener_once(local_listener).unwrap());

    let mut stream = TcpStream::connect(local_addr).unwrap();
    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Command,
            1,
            b"BEGIN_TX\tREAD_WRITE".to_vec(),
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
                "TX_QUERY\t{tx_id}\tMATCH (n:Person) SET n.status = $status\tstatus=s:committed"
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
            format!("COMMIT_TX\t{tx_id}").into_bytes(),
        ),
    )
    .unwrap();
    assert_native_response(
        &mut stream,
        NativeMessageType::Response,
        3,
        &format!("OK\tTX_COMMIT\t{tx_id}\t1"),
    );

    write_frame(
        &mut stream,
        &NativeFrame::new(NativeMessageType::Quit, 4, Vec::new()),
    )
    .unwrap();
    assert_native_response(&mut stream, NativeMessageType::Response, 4, "OK\tBYE");

    local_server.join().unwrap();
    remote0_server.join().unwrap();
    remote1_server.join().unwrap();
    assert_eq!(
        remote0_db
            .query(r#"MATCH (n:Person) WHERE n.status = "committed" RETURN n"#)
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        remote1_db
            .query(r#"MATCH (n:Person) WHERE n.status = "committed" RETURN n"#)
            .unwrap()
            .len(),
        1
    );
    assert!(local_db
        .query(r#"MATCH (n:Person) WHERE n.status = "committed" RETURN n"#)
        .unwrap()
        .is_empty());

    drop(local_db);
    drop(remote0_db);
    drop(remote1_db);
    let _ = fs::remove_dir_all(local_dir);
    let _ = fs::remove_dir_all(remote0_dir);
    let _ = fs::remove_dir_all(remote1_dir);
}

#[test]
fn native_read_write_transaction_prepare_commits_remote_multi_shard_property_replacements() {
    let local_dir = temp_dir("neo4r-native-multi-remote-replace-tx-local");
    let remote0_dir = temp_dir("neo4r-native-multi-remote-replace-tx-remote0");
    let remote1_dir = temp_dir("neo4r-native-multi-remote-replace-tx-remote1");
    let routing_table = ShardRoutingTable {
        version: 14,
        placements: vec![
            ShardPlacement::new(0, vec![ShardReplica::primary(2), ShardReplica::replica(1)]),
            ShardPlacement::new(1, vec![ShardReplica::primary(3), ShardReplica::replica(1)]),
        ],
    };
    let local_db =
        Neo4rDatabaseHandle::open(DatabaseConfig::new(&local_dir, 2, 2).with_server_id(1)).unwrap();
    local_db
        .create_node_on_shard(
            0,
            vec!["Person".to_string()],
            [("name".to_string(), Value::String("Alice".to_string()))]
                .into_iter()
                .collect(),
        )
        .unwrap();
    local_db
        .create_node_on_shard(
            1,
            vec!["Person".to_string()],
            [("name".to_string(), Value::String("Bob".to_string()))]
                .into_iter()
                .collect(),
        )
        .unwrap();
    local_db
        .install_routing_table(routing_table.clone())
        .unwrap();

    let remote0_db = Neo4rDatabaseHandle::open(
        DatabaseConfig::new(&remote0_dir, 2, 2)
            .with_server_id(2)
            .with_routing_table(routing_table.clone()),
    )
    .unwrap();
    remote0_db
        .create_node_on_shard(
            0,
            vec!["Person".to_string()],
            [("name".to_string(), Value::String("Alice".to_string()))]
                .into_iter()
                .collect(),
        )
        .unwrap();
    let remote1_db = Neo4rDatabaseHandle::open(
        DatabaseConfig::new(&remote1_dir, 2, 2)
            .with_server_id(3)
            .with_routing_table(routing_table),
    )
    .unwrap();
    remote1_db
        .create_node_on_shard(
            1,
            vec!["Person".to_string()],
            [("name".to_string(), Value::String("Bob".to_string()))]
                .into_iter()
                .collect(),
        )
        .unwrap();

    let remote0_backend = TcpBackend::new(remote0_db.clone());
    let remote0_listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let remote0_addr = remote0_listener.local_addr().unwrap();
    let remote0_server = thread::spawn(move || {
        remote0_backend
            .serve_listener_once(remote0_listener)
            .unwrap()
    });
    let remote1_backend = TcpBackend::new(remote1_db.clone());
    let remote1_listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let remote1_addr = remote1_listener.local_addr().unwrap();
    let remote1_server = thread::spawn(move || {
        remote1_backend
            .serve_listener_once(remote1_listener)
            .unwrap()
    });
    let local_backend = TcpBackend::new(local_db.clone());
    local_backend
        .register_query_peer(2, remote0_addr.to_string())
        .unwrap();
    local_backend
        .register_query_peer(3, remote1_addr.to_string())
        .unwrap();
    let local_listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let local_addr = local_listener.local_addr().unwrap();
    let local_server =
        thread::spawn(move || local_backend.serve_listener_once(local_listener).unwrap());

    let mut stream = TcpStream::connect(local_addr).unwrap();
    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Command,
            1,
            b"BEGIN_TX\tREAD_WRITE".to_vec(),
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
                    "TX_QUERY\t{tx_id}\tMATCH (n:Person) SET n = {{status: $status}}\tstatus=s:replaced"
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
            format!("COMMIT_TX\t{tx_id}").into_bytes(),
        ),
    )
    .unwrap();
    assert_native_response(
        &mut stream,
        NativeMessageType::Response,
        3,
        &format!("OK\tTX_COMMIT\t{tx_id}\t1"),
    );

    write_frame(
        &mut stream,
        &NativeFrame::new(NativeMessageType::Quit, 4, Vec::new()),
    )
    .unwrap();
    assert_native_response(&mut stream, NativeMessageType::Response, 4, "OK\tBYE");

    local_server.join().unwrap();
    remote0_server.join().unwrap();
    remote1_server.join().unwrap();
    assert_eq!(
        remote0_db
            .query(r#"MATCH (n:Person) WHERE n.status = "replaced" RETURN n.name"#)
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        remote1_db
            .query(r#"MATCH (n:Person) WHERE n.status = "replaced" RETURN n.name"#)
            .unwrap()
            .len(),
        1
    );
    assert!(local_db
        .query(r#"MATCH (n:Person) WHERE n.status = "replaced" RETURN n"#)
        .unwrap()
        .is_empty());

    drop(local_db);
    drop(remote0_db);
    drop(remote1_db);
    let _ = fs::remove_dir_all(local_dir);
    let _ = fs::remove_dir_all(remote0_dir);
    let _ = fs::remove_dir_all(remote1_dir);
}

#[test]
fn native_read_write_transaction_prepare_commits_remote_multi_shard_parameterized_map_replacements()
{
    let local_dir = temp_dir("neo4r-native-multi-remote-param-map-replace-tx-local");
    let remote0_dir = temp_dir("neo4r-native-multi-remote-param-map-replace-tx-remote0");
    let remote1_dir = temp_dir("neo4r-native-multi-remote-param-map-replace-tx-remote1");
    let routing_table = ShardRoutingTable {
        version: 16,
        placements: vec![
            ShardPlacement::new(0, vec![ShardReplica::primary(2), ShardReplica::replica(1)]),
            ShardPlacement::new(1, vec![ShardReplica::primary(3), ShardReplica::replica(1)]),
        ],
    };
    let local_db =
        Neo4rDatabaseHandle::open(DatabaseConfig::new(&local_dir, 2, 2).with_server_id(1)).unwrap();
    local_db
        .create_node_on_shard(
            0,
            vec!["Person".to_string()],
            [("name".to_string(), Value::String("Alice".to_string()))]
                .into_iter()
                .collect(),
        )
        .unwrap();
    local_db
        .create_node_on_shard(
            1,
            vec!["Person".to_string()],
            [("name".to_string(), Value::String("Bob".to_string()))]
                .into_iter()
                .collect(),
        )
        .unwrap();
    local_db
        .install_routing_table(routing_table.clone())
        .unwrap();

    let remote0_db = Neo4rDatabaseHandle::open(
        DatabaseConfig::new(&remote0_dir, 2, 2)
            .with_server_id(2)
            .with_routing_table(routing_table.clone()),
    )
    .unwrap();
    remote0_db
        .create_node_on_shard(
            0,
            vec!["Person".to_string()],
            [("name".to_string(), Value::String("Alice".to_string()))]
                .into_iter()
                .collect(),
        )
        .unwrap();
    let remote1_db = Neo4rDatabaseHandle::open(
        DatabaseConfig::new(&remote1_dir, 2, 2)
            .with_server_id(3)
            .with_routing_table(routing_table),
    )
    .unwrap();
    remote1_db
        .create_node_on_shard(
            1,
            vec!["Person".to_string()],
            [("name".to_string(), Value::String("Bob".to_string()))]
                .into_iter()
                .collect(),
        )
        .unwrap();

    let remote0_backend = TcpBackend::new(remote0_db.clone());
    let remote0_listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let remote0_addr = remote0_listener.local_addr().unwrap();
    let remote0_server = thread::spawn(move || {
        remote0_backend
            .serve_listener_once(remote0_listener)
            .unwrap()
    });
    let remote1_backend = TcpBackend::new(remote1_db.clone());
    let remote1_listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let remote1_addr = remote1_listener.local_addr().unwrap();
    let remote1_server = thread::spawn(move || {
        remote1_backend
            .serve_listener_once(remote1_listener)
            .unwrap()
    });
    let local_backend = TcpBackend::new(local_db.clone());
    local_backend
        .register_query_peer(2, remote0_addr.to_string())
        .unwrap();
    local_backend
        .register_query_peer(3, remote1_addr.to_string())
        .unwrap();
    let local_listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let local_addr = local_listener.local_addr().unwrap();
    let local_server =
        thread::spawn(move || local_backend.serve_listener_once(local_listener).unwrap());

    let props = test_map_param(&[
        ("status", Value::String("replaced".to_string())),
        ("score", Value::Int(9)),
    ]);
    let mut stream = TcpStream::connect(local_addr).unwrap();
    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Command,
            1,
            b"BEGIN_TX\tREAD_WRITE".to_vec(),
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
            format!("TX_QUERY\t{tx_id}\tMATCH (n:Person) SET n = $props\tprops=m:{props}")
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
            format!("COMMIT_TX\t{tx_id}").into_bytes(),
        ),
    )
    .unwrap();
    assert_native_response(
        &mut stream,
        NativeMessageType::Response,
        3,
        &format!("OK\tTX_COMMIT\t{tx_id}\t1"),
    );

    write_frame(
        &mut stream,
        &NativeFrame::new(NativeMessageType::Quit, 4, Vec::new()),
    )
    .unwrap();
    assert_native_response(&mut stream, NativeMessageType::Response, 4, "OK\tBYE");

    local_server.join().unwrap();
    remote0_server.join().unwrap();
    remote1_server.join().unwrap();
    for db in [&remote0_db, &remote1_db] {
        let rows = db
            .query(r#"MATCH (n:Person) WHERE n.status = "replaced" RETURN n.score, n.name"#)
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].get("n.score"),
            Some(&neo4r_query::QueryValue::Scalar(Value::Int(9)))
        );
        assert_eq!(
            rows[0].get("n.name"),
            Some(&neo4r_query::QueryValue::Scalar(Value::Null))
        );
    }
    assert!(local_db
        .query(r#"MATCH (n:Person) WHERE n.status = "replaced" RETURN n"#)
        .unwrap()
        .is_empty());

    drop(local_db);
    drop(remote0_db);
    drop(remote1_db);
    let _ = fs::remove_dir_all(local_dir);
    let _ = fs::remove_dir_all(remote0_dir);
    let _ = fs::remove_dir_all(remote1_dir);
}

#[test]
fn native_read_write_transaction_prepare_commits_remote_multi_shard_property_map_merges() {
    let local_dir = temp_dir("neo4r-native-multi-remote-map-merge-tx-local");
    let remote0_dir = temp_dir("neo4r-native-multi-remote-map-merge-tx-remote0");
    let remote1_dir = temp_dir("neo4r-native-multi-remote-map-merge-tx-remote1");
    let routing_table = ShardRoutingTable {
        version: 15,
        placements: vec![
            ShardPlacement::new(0, vec![ShardReplica::primary(2), ShardReplica::replica(1)]),
            ShardPlacement::new(1, vec![ShardReplica::primary(3), ShardReplica::replica(1)]),
        ],
    };
    let local_db =
        Neo4rDatabaseHandle::open(DatabaseConfig::new(&local_dir, 2, 2).with_server_id(1)).unwrap();
    local_db
        .create_node_on_shard(
            0,
            vec!["Person".to_string()],
            [("name".to_string(), Value::String("Alice".to_string()))]
                .into_iter()
                .collect(),
        )
        .unwrap();
    local_db
        .create_node_on_shard(
            1,
            vec!["Person".to_string()],
            [("name".to_string(), Value::String("Bob".to_string()))]
                .into_iter()
                .collect(),
        )
        .unwrap();
    local_db
        .install_routing_table(routing_table.clone())
        .unwrap();

    let remote0_db = Neo4rDatabaseHandle::open(
        DatabaseConfig::new(&remote0_dir, 2, 2)
            .with_server_id(2)
            .with_routing_table(routing_table.clone()),
    )
    .unwrap();
    remote0_db
        .create_node_on_shard(
            0,
            vec!["Person".to_string()],
            [("name".to_string(), Value::String("Alice".to_string()))]
                .into_iter()
                .collect(),
        )
        .unwrap();
    let remote1_db = Neo4rDatabaseHandle::open(
        DatabaseConfig::new(&remote1_dir, 2, 2)
            .with_server_id(3)
            .with_routing_table(routing_table),
    )
    .unwrap();
    remote1_db
        .create_node_on_shard(
            1,
            vec!["Person".to_string()],
            [("name".to_string(), Value::String("Bob".to_string()))]
                .into_iter()
                .collect(),
        )
        .unwrap();

    let remote0_backend = TcpBackend::new(remote0_db.clone());
    let remote0_listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let remote0_addr = remote0_listener.local_addr().unwrap();
    let remote0_server = thread::spawn(move || {
        remote0_backend
            .serve_listener_once(remote0_listener)
            .unwrap()
    });
    let remote1_backend = TcpBackend::new(remote1_db.clone());
    let remote1_listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let remote1_addr = remote1_listener.local_addr().unwrap();
    let remote1_server = thread::spawn(move || {
        remote1_backend
            .serve_listener_once(remote1_listener)
            .unwrap()
    });
    let local_backend = TcpBackend::new(local_db.clone());
    local_backend
        .register_query_peer(2, remote0_addr.to_string())
        .unwrap();
    local_backend
        .register_query_peer(3, remote1_addr.to_string())
        .unwrap();
    let local_listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let local_addr = local_listener.local_addr().unwrap();
    let local_server =
        thread::spawn(move || local_backend.serve_listener_once(local_listener).unwrap());

    let mut stream = TcpStream::connect(local_addr).unwrap();
    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Command,
            1,
            b"BEGIN_TX\tREAD_WRITE".to_vec(),
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
                "TX_QUERY\t{tx_id}\tMATCH (n:Person) SET n += {{status: $status}}\tstatus=s:merged"
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
            format!("COMMIT_TX\t{tx_id}").into_bytes(),
        ),
    )
    .unwrap();
    assert_native_response(
        &mut stream,
        NativeMessageType::Response,
        3,
        &format!("OK\tTX_COMMIT\t{tx_id}\t1"),
    );

    write_frame(
        &mut stream,
        &NativeFrame::new(NativeMessageType::Quit, 4, Vec::new()),
    )
    .unwrap();
    assert_native_response(&mut stream, NativeMessageType::Response, 4, "OK\tBYE");

    local_server.join().unwrap();
    remote0_server.join().unwrap();
    remote1_server.join().unwrap();
    assert_eq!(
        remote0_db
            .query(r#"MATCH (n:Person) WHERE n.status = "merged" RETURN n.name"#)
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        remote1_db
            .query(r#"MATCH (n:Person) WHERE n.status = "merged" RETURN n.name"#)
            .unwrap()
            .len(),
        1
    );
    assert!(local_db
        .query(r#"MATCH (n:Person) WHERE n.status = "merged" RETURN n"#)
        .unwrap()
        .is_empty());

    drop(local_db);
    drop(remote0_db);
    drop(remote1_db);
    let _ = fs::remove_dir_all(local_dir);
    let _ = fs::remove_dir_all(remote0_dir);
    let _ = fs::remove_dir_all(remote1_dir);
}

#[test]
fn native_read_write_transaction_prepare_commits_mixed_local_remote_sets() {
    let local_dir = temp_dir("neo4r-native-mixed-tx-local");
    let remote_dir = temp_dir("neo4r-native-mixed-tx-remote");
    let routing_table = ShardRoutingTable {
        version: 13,
        placements: vec![
            ShardPlacement::new(0, vec![ShardReplica::primary(1)]),
            ShardPlacement::new(1, vec![ShardReplica::primary(2), ShardReplica::replica(1)]),
        ],
    };
    let local_db =
        Neo4rDatabaseHandle::open(DatabaseConfig::new(&local_dir, 2, 2).with_server_id(1)).unwrap();
    local_db
        .create_node_on_shard(
            0,
            vec!["Person".to_string()],
            [("name".to_string(), Value::String("Local".to_string()))]
                .into_iter()
                .collect(),
        )
        .unwrap();
    local_db
        .create_node_on_shard(
            1,
            vec!["Person".to_string()],
            [(
                "name".to_string(),
                Value::String("RemoteReplica".to_string()),
            )]
            .into_iter()
            .collect(),
        )
        .unwrap();
    local_db
        .install_routing_table(routing_table.clone())
        .unwrap();

    let remote_db = Neo4rDatabaseHandle::open(
        DatabaseConfig::new(&remote_dir, 2, 2)
            .with_server_id(2)
            .with_routing_table(routing_table),
    )
    .unwrap();
    remote_db
        .create_node_on_shard(
            1,
            vec!["Person".to_string()],
            [("name".to_string(), Value::String("Remote".to_string()))]
                .into_iter()
                .collect(),
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
            NativeMessageType::Command,
            1,
            b"BEGIN_TX\tREAD_WRITE".to_vec(),
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
            format!("TX_QUERY\t{tx_id}\tMATCH (n:Person) SET n.status = $status\tstatus=s:mixed")
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
            format!("COMMIT_TX\t{tx_id}").into_bytes(),
        ),
    )
    .unwrap();
    assert_native_response(
        &mut stream,
        NativeMessageType::Response,
        3,
        &format!("OK\tTX_COMMIT\t{tx_id}\t1"),
    );

    write_frame(
        &mut stream,
        &NativeFrame::new(NativeMessageType::Quit, 4, Vec::new()),
    )
    .unwrap();
    assert_native_response(&mut stream, NativeMessageType::Response, 4, "OK\tBYE");

    local_server.join().unwrap();
    remote_server.join().unwrap();
    assert_eq!(
        local_db
            .query(r#"MATCH (n:Person) WHERE n.status = "mixed" RETURN n"#)
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        remote_db
            .query(r#"MATCH (n:Person) WHERE n.status = "mixed" RETURN n"#)
            .unwrap()
            .len(),
        1
    );
    assert_eq!(local_db.committed_indexes().unwrap(), vec![2, 1]);
    assert_eq!(remote_db.committed_indexes().unwrap(), vec![0, 2]);
    let decisions = TransactionDecisionStore::open(&local_dir)
        .unwrap()
        .load()
        .unwrap();
    assert!(
        decisions.is_empty(),
        "successful mixed 2PC commit should clear decision log for tx {tx_id}"
    );

    drop(local_db);
    drop(remote_db);
    let _ = fs::remove_dir_all(local_dir);
    let _ = fs::remove_dir_all(remote_dir);
}

#[test]
fn native_read_write_transaction_prepare_commits_mixed_local_remote_merge_nodes() {
    let local_dir = temp_dir("neo4r-native-mixed-merge-tx-local");
    let remote_dir = temp_dir("neo4r-native-mixed-merge-tx-remote");
    let routing_table = ShardRoutingTable {
        version: 16,
        placements: vec![
            ShardPlacement::new(0, vec![ShardReplica::primary(1)]),
            ShardPlacement::new(1, vec![ShardReplica::primary(2), ShardReplica::replica(1)]),
        ],
    };
    let local_db = Neo4rDatabaseHandle::open(
        DatabaseConfig::new(&local_dir, 2, 2)
            .with_server_id(1)
            .with_routing_table(routing_table.clone()),
    )
    .unwrap();
    let remote_db = Neo4rDatabaseHandle::open(
        DatabaseConfig::new(&remote_dir, 2, 2)
            .with_server_id(2)
            .with_routing_table(routing_table),
    )
    .unwrap();

    let query = "MERGE (n:Person {email: $email}) ON CREATE SET n.created = $created ON MATCH SET n.seen = $seen RETURN n";
    let status = local_db.cluster_status().unwrap();
    let mut local_email = None;
    let mut remote_email = None;
    for candidate in 0..128 {
        let email = format!("tx-merge-{candidate}@example.com");
        let params = [
            ("email".to_string(), Value::String(email.clone())),
            ("created".to_string(), Value::Int(1)),
            ("seen".to_string(), Value::Int(2)),
        ]
        .into_iter()
        .collect();
        let shard_id = select_merge_node_write_shard(&status, query, &params)
            .unwrap()
            .shard_id;
        if shard_id == 0 && local_email.is_none() {
            local_email = Some(email);
        } else if shard_id == 1 && remote_email.is_none() {
            remote_email = Some(email);
        }
        if local_email.is_some() && remote_email.is_some() {
            break;
        }
    }
    let local_email = local_email.expect("expected a candidate for shard 0");
    let remote_email = remote_email.expect("expected a candidate for shard 1");

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
            NativeMessageType::Command,
            1,
            b"BEGIN_TX\tREAD_WRITE".to_vec(),
        ),
    )
    .unwrap();
    let begin = read_native_payload(&mut stream, NativeMessageType::Response, 1);
    let tx_id = begin.split('\t').collect::<Vec<_>>()[2]
        .parse::<u64>()
        .unwrap();

    for (request_id, email) in [(2, local_email.as_str()), (3, remote_email.as_str())] {
        write_frame(
            &mut stream,
            &NativeFrame::new(
                NativeMessageType::Command,
                request_id,
                format!("TX_QUERY\t{tx_id}\t{query}\temail=s:{email}\tcreated=i:1\tseen=i:2")
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
        &NativeFrame::new(NativeMessageType::Quit, 5, Vec::new()),
    )
    .unwrap();
    assert_native_response(&mut stream, NativeMessageType::Response, 5, "OK\tBYE");

    local_server.join().unwrap();
    remote_server.join().unwrap();
    assert_eq!(
        local_db
            .query(&format!(
                r#"MATCH (n:Person) WHERE n.email = "{local_email}" RETURN n"#
            ))
            .unwrap()
            .len(),
        1
    );
    assert!(local_db
        .query(&format!(
            r#"MATCH (n:Person) WHERE n.email = "{remote_email}" RETURN n"#
        ))
        .unwrap()
        .is_empty());
    assert_eq!(
        remote_db
            .query(&format!(
                r#"MATCH (n:Person) WHERE n.email = "{remote_email}" RETURN n"#
            ))
            .unwrap()
            .len(),
        1
    );
    assert!(remote_db
        .query(&format!(
            r#"MATCH (n:Person) WHERE n.email = "{local_email}" RETURN n"#
        ))
        .unwrap()
        .is_empty());
    let decisions = TransactionDecisionStore::open(&local_dir)
        .unwrap()
        .load()
        .unwrap();
    assert!(
        decisions.is_empty(),
        "successful mixed merge 2PC commit should clear decision log for tx {tx_id}"
    );

    drop(local_db);
    drop(remote_db);
    let _ = fs::remove_dir_all(local_dir);
    let _ = fs::remove_dir_all(remote_dir);
}

#[test]
fn native_read_write_transaction_commits_multi_shard_mixed_create_and_set() {
    let dir = temp_dir("neo4r-native-multi-shard-mixed-create-set");
    let routing_table = ShardRoutingTable {
        version: 14,
        placements: vec![
            ShardPlacement::new(0, vec![ShardReplica::primary(1)]),
            ShardPlacement::new(1, vec![ShardReplica::primary(1)]),
        ],
    };
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 2, 2).with_server_id(1)).unwrap();
    db.create_node_on_shard(
        0,
        vec!["Person".to_string()],
        [("name".to_string(), Value::String("Local".to_string()))]
            .into_iter()
            .collect(),
    )
    .unwrap();
    db.create_node_on_shard(
        1,
        vec!["Person".to_string()],
        [(
            "name".to_string(),
            Value::String("RemoteReplica".to_string()),
        )]
        .into_iter()
        .collect(),
    )
    .unwrap();
    db.install_routing_table(routing_table).unwrap();

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
            b"BEGIN_TX\tREAD_WRITE".to_vec(),
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
                "TX_QUERY\t{tx_id}\tCREATE (n:Person {{name: $name}}) RETURN n\tname=s:Created"
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
            format!("TX_QUERY\t{tx_id}\tMATCH (n:Person) SET n.status = $status\tstatus=s:mixed")
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
        &NativeFrame::new(NativeMessageType::Quit, 5, Vec::new()),
    )
    .unwrap();
    assert_native_response(&mut stream, NativeMessageType::Response, 5, "OK\tBYE");

    server.join().unwrap();
    assert_eq!(
        db.query(r#"MATCH (n:Person) WHERE n.status = "mixed" RETURN n"#)
            .unwrap()
            .len(),
        3
    );
    assert_eq!(
        db.query(r#"MATCH (n:Person) WHERE n.name = "Created" RETURN n.status"#)
            .unwrap()[0]
            .get("n.status"),
        Some(&neo4r_query::QueryValue::Scalar(Value::String(
            "mixed".to_string()
        )))
    );

    drop(db);
    let _ = fs::remove_dir_all(dir);
}

#[test]
fn native_read_write_transaction_commits_multi_shard_filtered_sets() {
    let dir = temp_dir("neo4r-native-multi-shard-tx-filtered-set");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 2, 2)).unwrap();
    db.create_node_on_shard(
        0,
        vec!["Person".to_string()],
        [("name".to_string(), Value::String("Alice".to_string()))]
            .into_iter()
            .collect(),
    )
    .unwrap();
    db.create_node_on_shard(
        1,
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
            b"BEGIN_TX\tREAD_WRITE".to_vec(),
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
                    "TX_QUERY\t{tx_id}\tMATCH (n:Person {{name: $name}}) SET n.status = $status\tname=s:Alice\tstatus=s:active"
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
                    "TX_QUERY\t{tx_id}\tMATCH (n:Person {{name: $name}}) SET n.status = $status\tname=s:Bob\tstatus=s:active"
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
            b"MATCH (n:Person) WHERE n.status = \"active\" RETURN n.name".to_vec(),
        ),
    )
    .unwrap();
    let rows = read_native_payload(&mut stream, NativeMessageType::Response, 5);
    let parts = rows.split('\t').collect::<Vec<_>>();
    assert_eq!(parts[0..3], ["OK", "RESULT_START", "1"]);
    assert_eq!(parts[3], "2");

    write_frame(
        &mut stream,
        &NativeFrame::new(NativeMessageType::Quit, 6, Vec::new()),
    )
    .unwrap();
    assert_native_response(&mut stream, NativeMessageType::Response, 6, "OK\tBYE");

    server.join().unwrap();
    assert_eq!(
        db.query(r#"MATCH (n:Person) WHERE n.status = "active" RETURN n"#)
            .unwrap()
            .len(),
        2
    );
    drop(db);
    let _ = fs::remove_dir_all(dir);
}

#[test]
fn native_read_write_transaction_discards_staged_writes_on_rollback() {
    let dir = temp_dir("neo4r-native-read-write-rollback");
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
            b"BEGIN_TX\tREAD_WRITE".to_vec(),
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
            format!("TX_QUERY\t{tx_id}\tCREATE (n:Person {{name: \"Alice\"}}) RETURN n")
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
            format!("ROLLBACK_TX\t{tx_id}").into_bytes(),
        ),
    )
    .unwrap();
    assert_native_response(
        &mut stream,
        NativeMessageType::Response,
        3,
        &format!("OK\tTX_ROLLBACK\t{tx_id}"),
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
    let rows = read_native_payload(&mut stream, NativeMessageType::Response, 4);
    let parts = rows.split('\t').collect::<Vec<_>>();
    assert_eq!(parts[0..3], ["OK", "RESULT_START", "1"]);
    assert_eq!(parts[3], "0");

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
fn native_transaction_lists_and_cleans_up_session_transactions() {
    let dir = temp_dir("neo4r-native-tx-list-cleanup");
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
    let transactions = backend.transactions.clone();
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
            format!("TX_QUERY\t{tx_id}\tCREATE (n:Person {{name: \"Alice\"}}) RETURN n")
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
        &NativeFrame::new(NativeMessageType::Command, 3, b"LIST_TX".to_vec()),
    )
    .unwrap();
    assert_native_response(
        &mut stream,
        NativeMessageType::Response,
        3,
        &format!("OK\tTX_LIST\t1\t{tx_id}:READ_WRITE:READ_COMMITTED:1"),
    );

    drop(stream);
    server.join().unwrap();
    assert!(transactions.transactions.lock().unwrap().is_empty());
    let _ = fs::remove_dir_all(dir);
}

#[test]
fn native_transaction_lists_all_sessions() {
    let dir = temp_dir("neo4r-native-tx-list-all-sessions");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1)).unwrap();
    let backend = TcpBackend::with_config(
        db,
        TcpBackendConfig {
            worker_count: 2,
            queue_capacity: 8,
            default_page_size: 10,
            read_preference: QueryReadPreference::Primary,
            catch_up_connect_timeout: Duration::from_secs(1),
        },
    );
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let server = thread::spawn(move || {
        let mut handlers = Vec::new();
        for _ in 0..2 {
            let (stream, _) = listener.accept().unwrap();
            let backend = backend.clone();
            handlers.push(thread::spawn(move || {
                backend.handle_stream(stream).unwrap()
            }));
        }
        for handler in handlers {
            handler.join().unwrap();
        }
    });

    let mut first = TcpStream::connect(addr).unwrap();
    write_frame(
        &mut first,
        &NativeFrame::new(
            NativeMessageType::Command,
            1,
            b"BEGIN_TX\tREAD_WRITE READ_COMMITTED".to_vec(),
        ),
    )
    .unwrap();
    let first_begin = read_native_payload(&mut first, NativeMessageType::Response, 1);
    let first_tx = first_begin.split('\t').collect::<Vec<_>>()[2]
        .parse::<u64>()
        .unwrap();

    write_frame(
        &mut first,
        &NativeFrame::new(
            NativeMessageType::Command,
            2,
            format!("TX_QUERY\t{first_tx}\tCREATE (n:Person {{name: \"Alice\"}}) RETURN n")
                .into_bytes(),
        ),
    )
    .unwrap();
    assert_native_response(
        &mut first,
        NativeMessageType::Response,
        2,
        &format!("OK\tTX_STAGED\t{first_tx}\t1"),
    );

    let mut second = TcpStream::connect(addr).unwrap();
    write_frame(
        &mut second,
        &NativeFrame::new(
            NativeMessageType::Command,
            1,
            b"BEGIN_TX\tREAD_WRITE".to_vec(),
        ),
    )
    .unwrap();
    let second_begin = read_native_payload(&mut second, NativeMessageType::Response, 1);
    let second_tx = second_begin.split('\t').collect::<Vec<_>>()[2]
        .parse::<u64>()
        .unwrap();

    write_frame(
        &mut first,
        &NativeFrame::new(NativeMessageType::Command, 3, b"LIST_TX".to_vec()),
    )
    .unwrap();
    assert_native_response(
        &mut first,
        NativeMessageType::Response,
        3,
        &format!("OK\tTX_LIST\t1\t{first_tx}:READ_WRITE:READ_COMMITTED:1"),
    );

    write_frame(
        &mut first,
        &NativeFrame::new(NativeMessageType::Command, 4, b"LIST_ALL_TX".to_vec()),
    )
    .unwrap();
    let list_all = read_native_payload(&mut first, NativeMessageType::Response, 4);
    let parts = list_all.split('\t').collect::<Vec<_>>();
    assert_eq!(parts[0..3], ["OK", "TX_LIST_ALL", "2"]);
    assert!(parts[3].contains(&format!(":{first_tx}:READ_WRITE:READ_COMMITTED:1")));
    assert!(parts[3].contains(&format!(":{second_tx}:READ_WRITE:SNAPSHOT:0")));

    write_frame(
        &mut first,
        &NativeFrame::new(
            NativeMessageType::Command,
            5,
            format!("KILL_TX\t{second_tx}").into_bytes(),
        ),
    )
    .unwrap();
    let kill = read_native_payload(&mut first, NativeMessageType::Response, 5);
    assert!(kill.starts_with(&format!("OK\tTX_KILL\t{second_tx}\t")));

    write_frame(
        &mut second,
        &NativeFrame::new(
            NativeMessageType::Command,
            2,
            format!("COMMIT_TX\t{second_tx}").into_bytes(),
        ),
    )
    .unwrap();
    let commit = read_native_payload(&mut second, NativeMessageType::Error, 2);
    assert!(commit.contains(&format!("unknown transaction: {second_tx}")));

    write_frame(
        &mut second,
        &NativeFrame::new(NativeMessageType::Quit, 3, Vec::new()),
    )
    .unwrap();
    assert_native_response(&mut second, NativeMessageType::Response, 3, "OK\tBYE");
    write_frame(
        &mut first,
        &NativeFrame::new(NativeMessageType::Quit, 6, Vec::new()),
    )
    .unwrap();
    assert_native_response(&mut first, NativeMessageType::Response, 6, "OK\tBYE");

    server.join().unwrap();
    let _ = fs::remove_dir_all(dir);
}

#[test]
fn native_read_only_transaction_rejects_write_query() {
    let dir = temp_dir("neo4r-native-read-only-write-reject");
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
        &NativeFrame::new(NativeMessageType::Command, 1, b"BEGIN_TX".to_vec()),
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
            format!("TX_QUERY\t{tx_id}\tCREATE (n:Person {{name: \"Alice\"}}) RETURN n")
                .into_bytes(),
        ),
    )
    .unwrap();
    let response = read_native_payload(&mut stream, NativeMessageType::Error, 2);
    assert!(response.contains("read-only"));

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
fn native_read_write_transaction_rejects_schema_ddl() {
    let dir = temp_dir("neo4r-native-read-write-schema-ddl-reject");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1)).unwrap();
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
            b"BEGIN_TX\tREAD_WRITE".to_vec(),
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
                    "TX_QUERY\t{tx_id}\tCREATE CONSTRAINT person_email_unique FOR (n:Person) REQUIRE n.email IS UNIQUE"
                )
                .into_bytes(),
            ),
        )
        .unwrap();
    let response = read_native_payload(&mut stream, NativeMessageType::Error, 2);
    assert!(response.contains("schema DDL is not supported"));

    write_frame(
        &mut stream,
        &NativeFrame::new(NativeMessageType::Command, 3, b"LIST_TX".to_vec()),
    )
    .unwrap();
    assert_native_response(
        &mut stream,
        NativeMessageType::Response,
        3,
        &format!("OK\tTX_LIST\t1\t{tx_id}:READ_WRITE:SNAPSHOT:0"),
    );

    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Command,
            4,
            format!("ROLLBACK_TX\t{tx_id}").into_bytes(),
        ),
    )
    .unwrap();
    assert_native_response(
        &mut stream,
        NativeMessageType::Response,
        4,
        &format!("OK\tTX_ROLLBACK\t{tx_id}"),
    );
    write_frame(
        &mut stream,
        &NativeFrame::new(NativeMessageType::Quit, 5, Vec::new()),
    )
    .unwrap();
    assert_native_response(&mut stream, NativeMessageType::Response, 5, "OK\tBYE");

    server.join().unwrap();
    assert!(db.list_indexes().unwrap().is_empty());

    drop(db);
    let _ = fs::remove_dir_all(dir);
}

#[test]
fn replication_listener_accepts_tcp_replicator_batches() {
    let primary_dir = temp_dir("neo4r-server-repl-primary");
    let replica_dir = temp_dir("neo4r-server-repl-replica");
    let routing_table = ShardRoutingTable {
        version: 3,
        placements: vec![ShardPlacement::new(
            0,
            vec![ShardReplica::primary(1), ShardReplica::replica(2)],
        )],
    };
    let replica = Neo4rDatabaseHandle::open(
        DatabaseConfig::new(&replica_dir, 1, 1)
            .with_server_id(2)
            .with_routing_table(routing_table.clone()),
    )
    .unwrap();
    let backend = TcpBackend::new(replica.clone());
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let server = thread::spawn(move || backend.serve_replication_listener_once(listener).unwrap());

    let replicator = Arc::new(TcpShardReplicator::new(routing_table.clone()));
    replicator.register_peer(2, address).unwrap();
    let primary = Neo4rDatabaseHandle::open_with_replicator(
        DatabaseConfig::new(&primary_dir, 1, 1)
            .with_server_id(1)
            .with_routing_table(routing_table),
        replicator,
    )
    .unwrap();

    primary
        .create_node(
            vec!["Person".to_string()],
            [("name".to_string(), Value::String("Alice".to_string()))]
                .into_iter()
                .collect(),
        )
        .unwrap();

    server.join().unwrap();
    assert_eq!(
        replica
            .query(r#"MATCH (n:Person) WHERE n.name = "Alice" RETURN n.name"#)
            .unwrap()
            .len(),
        1
    );

    drop(primary);
    drop(replica);
    let _ = fs::remove_dir_all(primary_dir);
    let _ = fs::remove_dir_all(replica_dir);
}

#[test]
fn replication_listener_accepts_group_commit_entry_batches() {
    let primary_dir = temp_dir("neo4r-server-repl-batch-primary");
    let replica_dir = temp_dir("neo4r-server-repl-batch-replica");
    let routing_table = ShardRoutingTable {
        version: 3,
        placements: vec![ShardPlacement::new(
            0,
            vec![ShardReplica::primary(1), ShardReplica::replica(2)],
        )],
    };
    let replica = Neo4rDatabaseHandle::open(
        DatabaseConfig::new(&replica_dir, 1, 1)
            .with_server_id(2)
            .with_routing_table(routing_table.clone()),
    )
    .unwrap();
    let backend = TcpBackend::new(replica.clone());
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let server = thread::spawn(move || backend.serve_replication_listener_once(listener).unwrap());

    let replicator = Arc::new(TcpShardReplicator::new(routing_table.clone()));
    replicator.register_peer(2, address).unwrap();
    let primary = Neo4rDatabaseHandle::open_with_replicator(
        DatabaseConfig::new(&primary_dir, 1, 1)
            .with_server_id(1)
            .with_routing_table(routing_table),
        replicator,
    )
    .unwrap();

    let writes = ["Alice", "Bob", "Carol"]
        .into_iter()
        .map(|name| {
            (
                "CREATE (n:Person {name: $name})".to_string(),
                [("name".to_string(), Value::String(name.to_string()))]
                    .into_iter()
                    .collect::<neo4r_query::QueryParams>(),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        primary
            .execute_cypher_mutation_batch_on_shard(0, writes)
            .unwrap(),
        3
    );

    server.join().unwrap();
    assert_eq!(replica.query("MATCH (n:Person) RETURN n").unwrap().len(), 3);
    assert_eq!(replica.read_snapshot().unwrap().applied_indexes(), &[3]);

    drop(primary);
    drop(replica);
    let _ = fs::remove_dir_all(primary_dir);
    let _ = fs::remove_dir_all(replica_dir);
}

#[test]
fn replication_listener_accepts_consecutive_entry_batches() {
    let primary_dir = temp_dir("neo4r-server-repl-consecutive-primary");
    let replica_dir = temp_dir("neo4r-server-repl-consecutive-replica");
    let routing_table = ShardRoutingTable {
        version: 3,
        placements: vec![ShardPlacement::new(
            0,
            vec![ShardReplica::primary(1), ShardReplica::replica(2)],
        )],
    };
    let replica = Neo4rDatabaseHandle::open(
        DatabaseConfig::new(&replica_dir, 1, 1)
            .with_server_id(2)
            .with_routing_table(routing_table.clone()),
    )
    .unwrap();
    let backend = TcpBackend::new(replica.clone());
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let server = thread::spawn(move || {
        for _ in 0..2 {
            let (stream, _) = listener.accept().unwrap();
            backend.handle_replication_stream(stream).unwrap();
        }
    });

    let replicator = Arc::new(TcpShardReplicator::new(routing_table.clone()));
    replicator.register_peer(2, address).unwrap();
    let primary = Neo4rDatabaseHandle::open_with_replicator(
        DatabaseConfig::new(&primary_dir, 1, 1)
            .with_server_id(1)
            .with_routing_table(routing_table),
        replicator,
    )
    .unwrap();

    for names in [["Alice", "Bob"], ["Carol", "Dave"]] {
        let writes = names
            .into_iter()
            .map(|name| {
                (
                    "CREATE (n:Person {name: $name})".to_string(),
                    [("name".to_string(), Value::String(name.to_string()))]
                        .into_iter()
                        .collect::<neo4r_query::QueryParams>(),
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(
            primary
                .execute_cypher_mutation_batch_on_shard(0, writes)
                .unwrap(),
            2
        );
    }

    server.join().unwrap();
    assert_eq!(replica.query("MATCH (n:Person) RETURN n").unwrap().len(), 4);
    assert_eq!(replica.read_snapshot().unwrap().applied_indexes(), &[4]);

    drop(primary);
    drop(replica);
    let _ = fs::remove_dir_all(primary_dir);
    let _ = fs::remove_dir_all(replica_dir);
}

#[test]
fn backend_catch_up_from_primaries_fetches_replica_shard_logs() {
    let primary_dir = temp_dir("neo4r-server-catch-up-primary");
    let replica_dir = temp_dir("neo4r-server-catch-up-replica");
    let routing_table = ShardRoutingTable {
        version: 3,
        placements: vec![ShardPlacement::new(
            0,
            vec![ShardReplica::primary(1), ShardReplica::replica(2)],
        )],
    };
    let primary = Neo4rDatabaseHandle::open(
        DatabaseConfig::new(&primary_dir, 1, 1)
            .with_server_id(1)
            .with_routing_table(routing_table.clone()),
    )
    .unwrap();
    primary
        .create_node(
            vec!["Person".to_string()],
            [("name".to_string(), Value::String("Alice".to_string()))]
                .into_iter()
                .collect(),
        )
        .unwrap();
    let primary_backend = TcpBackend::new(primary.clone());
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let server = thread::spawn(move || {
        primary_backend
            .serve_replication_listener_once(listener)
            .unwrap()
    });

    let replica = Neo4rDatabaseHandle::open(
        DatabaseConfig::new(&replica_dir, 1, 1)
            .with_server_id(2)
            .with_routing_table(routing_table),
    )
    .unwrap();
    let replica_backend = TcpBackend::with_config(
        replica.clone(),
        TcpBackendConfig {
            worker_count: 1,
            queue_capacity: 8,
            default_page_size: 10,
            read_preference: QueryReadPreference::Primary,
            catch_up_connect_timeout: Duration::from_millis(250),
        },
    );
    replica_backend
        .register_replication_peer(1, address)
        .unwrap();

    let response =
        replica_backend.execute_backend_request(parse_request("CATCH_UP_FROM_PRIMARIES").unwrap());

    server.join().unwrap();
    assert_eq!(
        response,
        BackendResponse::OkCatchUp("shard=0 primary=1 start=1 end=1 fetched=1".to_string())
    );
    assert_eq!(
        replica
            .query(r#"MATCH (n:Person) WHERE n.name = "Alice" RETURN n.name"#)
            .unwrap()
            .len(),
        1
    );

    drop(primary);
    drop(replica);
    let _ = fs::remove_dir_all(primary_dir);
    let _ = fs::remove_dir_all(replica_dir);
}

#[test]
fn backend_catch_up_from_primaries_fetches_batches_idempotently() {
    let primary_dir = temp_dir("neo4r-server-catch-up-batch-primary");
    let replica_dir = temp_dir("neo4r-server-catch-up-batch-replica");
    let routing_table = ShardRoutingTable {
        version: 3,
        placements: vec![ShardPlacement::new(
            0,
            vec![ShardReplica::primary(1), ShardReplica::replica(2)],
        )],
    };
    let primary = Neo4rDatabaseHandle::open(
        DatabaseConfig::new(&primary_dir, 1, 1)
            .with_server_id(1)
            .with_routing_table(routing_table.clone()),
    )
    .unwrap();
    let writes = ["Alice", "Bob", "Carol"]
        .into_iter()
        .map(|name| {
            (
                "CREATE (n:Person {name: $name})".to_string(),
                [("name".to_string(), Value::String(name.to_string()))]
                    .into_iter()
                    .collect::<neo4r_query::QueryParams>(),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        primary
            .execute_cypher_mutation_batch_on_shard(0, writes)
            .unwrap(),
        3
    );
    let primary_backend = TcpBackend::new(primary.clone());
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let server = thread::spawn(move || {
        for _ in 0..2 {
            let (stream, _) = listener.accept().unwrap();
            primary_backend.handle_replication_stream(stream).unwrap();
        }
    });

    let replica = Neo4rDatabaseHandle::open(
        DatabaseConfig::new(&replica_dir, 1, 1)
            .with_server_id(2)
            .with_routing_table(routing_table),
    )
    .unwrap();
    let replica_backend = TcpBackend::new(replica.clone());
    replica_backend
        .register_replication_peer(1, address)
        .unwrap();

    let first =
        replica_backend.execute_backend_request(parse_request("CATCH_UP_FROM_PRIMARIES").unwrap());
    assert_eq!(
        first,
        BackendResponse::OkCatchUp("shard=0 primary=1 start=1 end=3 fetched=3".to_string())
    );
    assert_eq!(replica.query("MATCH (n:Person) RETURN n").unwrap().len(), 3);
    assert_eq!(replica.read_snapshot().unwrap().applied_indexes(), &[3]);

    let second =
        replica_backend.execute_backend_request(parse_request("CATCH_UP_FROM_PRIMARIES").unwrap());
    assert_eq!(
        second,
        BackendResponse::OkCatchUp("shard=0 primary=1 start=4 end=3 fetched=0".to_string())
    );
    assert_eq!(replica.query("MATCH (n:Person) RETURN n").unwrap().len(), 3);

    server.join().unwrap();
    drop(primary);
    drop(replica);
    let _ = fs::remove_dir_all(primary_dir);
    let _ = fs::remove_dir_all(replica_dir);
}

#[test]
fn backend_catch_up_from_primary_targets_one_peer() {
    let primary_dir = temp_dir("neo4r-server-catch-up-one-primary");
    let replica_dir = temp_dir("neo4r-server-catch-up-one-replica");
    let routing_table = ShardRoutingTable {
        version: 3,
        placements: vec![
            ShardPlacement::new(0, vec![ShardReplica::primary(1), ShardReplica::replica(2)]),
            ShardPlacement::new(1, vec![ShardReplica::primary(3), ShardReplica::replica(2)]),
        ],
    };
    let primary = Neo4rDatabaseHandle::open(
        DatabaseConfig::new(&primary_dir, 2, 1)
            .with_server_id(1)
            .with_routing_table(routing_table.clone()),
    )
    .unwrap();
    primary
        .create_node_on_shard(
            0,
            vec!["Person".to_string()],
            [("name".to_string(), Value::String("Alice".to_string()))]
                .into_iter()
                .collect(),
        )
        .unwrap();
    let primary_backend = TcpBackend::new(primary.clone());
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let server = thread::spawn(move || {
        primary_backend
            .serve_replication_listener_once(listener)
            .unwrap()
    });

    let replica = Neo4rDatabaseHandle::open(
        DatabaseConfig::new(&replica_dir, 2, 1)
            .with_server_id(2)
            .with_routing_table(routing_table),
    )
    .unwrap();
    let replica_backend = TcpBackend::new(replica.clone());
    replica_backend
        .register_replication_peer(1, address)
        .unwrap();

    let response =
        replica_backend.execute_backend_request(parse_request("CATCH_UP_FROM_PRIMARY\t1").unwrap());

    server.join().unwrap();
    assert_eq!(
        response,
        BackendResponse::OkCatchUp("shard=0 primary=1 start=1 end=1 fetched=1".to_string())
    );
    assert_eq!(
        replica
            .query(r#"MATCH (n:Person) WHERE n.name = "Alice" RETURN n.name"#)
            .unwrap()
            .len(),
        1
    );

    drop(primary);
    drop(replica);
    let _ = fs::remove_dir_all(primary_dir);
    let _ = fs::remove_dir_all(replica_dir);
}

#[test]
fn backend_catch_up_plan_reports_target_shards() {
    let dir = temp_dir("neo4r-server-catch-up-plan");
    let routing_table = ShardRoutingTable {
        version: 3,
        placements: vec![
            ShardPlacement::new(0, vec![ShardReplica::primary(1), ShardReplica::replica(2)]),
            ShardPlacement::new(1, vec![ShardReplica::primary(3), ShardReplica::replica(2)]),
        ],
    };
    let db = Neo4rDatabaseHandle::open(
        DatabaseConfig::new(&dir, 2, 1)
            .with_server_id(2)
            .with_routing_table(routing_table),
    )
    .unwrap();
    let backend = TcpBackend::new(db);
    backend
        .register_replication_peer(1, "127.0.0.1:17687")
        .unwrap();

    assert_eq!(
        backend.execute_backend_request(parse_request("CATCH_UP_PLAN").unwrap()),
        BackendResponse::OkCatchUpPlan(
            "shard=0 primary=1 start=1 peer=registered,shard=1 primary=3 start=1 peer=missing"
                .to_string()
        )
    );
    assert_eq!(
        backend.execute_backend_request(parse_request("CATCH_UP_PLAN_PRIMARY\t3").unwrap()),
        BackendResponse::OkCatchUpPlan("shard=1 primary=3 start=1 peer=missing".to_string())
    );

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn backend_replication_peer_status_reports_roles_and_missing_addresses() {
    let dir = temp_dir("neo4r-server-replication-peer-status");
    let routing_table = ShardRoutingTable {
        version: 3,
        placements: vec![
            ShardPlacement::new(0, vec![ShardReplica::primary(1), ShardReplica::replica(2)]),
            ShardPlacement::new(1, vec![ShardReplica::primary(3), ShardReplica::replica(2)]),
            ShardPlacement::new(2, vec![ShardReplica::primary(2), ShardReplica::replica(1)]),
        ],
    };
    let db = Neo4rDatabaseHandle::open(
        DatabaseConfig::new(&dir, 3, 1)
            .with_server_id(2)
            .with_routing_table(routing_table),
    )
    .unwrap();
    let backend = TcpBackend::new(db);
    backend
        .register_replication_peer(1, "127.0.0.1:17687")
        .unwrap();

    assert_eq!(
            backend.execute_backend_request(parse_request("REPLICATION_PEER_STATUS").unwrap()),
            BackendResponse::OkReplicationPeerStatus(
                "server=1 address=127.0.0.1:17687 primary_shards=0 replica_shards=2,server=3 address=missing primary_shards=1 replica_shards=-"
                    .to_string()
            )
        );
    assert_eq!(
        backend.execute_backend_request(parse_request("REPLICATION_PEER_STATUS\t3").unwrap()),
        BackendResponse::OkReplicationPeerStatus(
            "server=3 address=missing primary_shards=1 replica_shards=-".to_string()
        )
    );

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn native_tcp_catches_up_from_primary() {
    let primary_dir = temp_dir("neo4r-native-catch-up-one-primary");
    let replica_dir = temp_dir("neo4r-native-catch-up-one-replica");
    let routing_table = ShardRoutingTable {
        version: 3,
        placements: vec![ShardPlacement::new(
            0,
            vec![ShardReplica::primary(1), ShardReplica::replica(2)],
        )],
    };
    let primary = Neo4rDatabaseHandle::open(
        DatabaseConfig::new(&primary_dir, 1, 1)
            .with_server_id(1)
            .with_routing_table(routing_table.clone()),
    )
    .unwrap();
    primary
        .create_node(
            vec!["Person".to_string()],
            [("name".to_string(), Value::String("Alice".to_string()))]
                .into_iter()
                .collect(),
        )
        .unwrap();
    let primary_backend = TcpBackend::new(primary.clone());
    let replication_listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let replication_address = replication_listener.local_addr().unwrap().to_string();
    let replication_server = thread::spawn(move || {
        primary_backend
            .serve_replication_listener_once(replication_listener)
            .unwrap()
    });

    let replica = Neo4rDatabaseHandle::open(
        DatabaseConfig::new(&replica_dir, 1, 1)
            .with_server_id(2)
            .with_routing_table(routing_table),
    )
    .unwrap();
    let replica_backend = TcpBackend::with_config(
        replica.clone(),
        TcpBackendConfig {
            worker_count: 1,
            queue_capacity: 8,
            default_page_size: 10,
            read_preference: QueryReadPreference::Primary,
            catch_up_connect_timeout: Duration::from_secs(1),
        },
    );
    let native_listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let native_address = native_listener.local_addr().unwrap();
    let native_server = thread::spawn(move || {
        replica_backend
            .serve_listener_once(native_listener)
            .unwrap()
    });

    let mut stream = TcpStream::connect(native_address).unwrap();
    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Command,
            1,
            format!("REGISTER_REPLICATION_PEER\t1\t{replication_address}").into_bytes(),
        ),
    )
    .unwrap();
    assert_native_response(&mut stream, NativeMessageType::Response, 1, "OK");

    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Command,
            2,
            b"CATCH_UP_FROM_PRIMARY\t1".to_vec(),
        ),
    )
    .unwrap();
    assert_native_response(
        &mut stream,
        NativeMessageType::Response,
        2,
        "OK\tCATCH_UP\tshard=0 primary=1 start=1 end=1 fetched=1",
    );

    write_frame(
        &mut stream,
        &NativeFrame::new(NativeMessageType::Quit, 3, Vec::new()),
    )
    .unwrap();
    assert_native_response(&mut stream, NativeMessageType::Response, 3, "OK\tBYE");

    native_server.join().unwrap();
    replication_server.join().unwrap();
    assert_eq!(
        replica
            .query(r#"MATCH (n:Person) WHERE n.name = "Alice" RETURN n.name"#)
            .unwrap()
            .len(),
        1
    );

    drop(primary);
    drop(replica);
    let _ = fs::remove_dir_all(primary_dir);
    let _ = fs::remove_dir_all(replica_dir);
}

#[test]
fn native_tcp_reports_catch_up_plan() {
    let dir = temp_dir("neo4r-native-catch-up-plan");
    let routing_table = ShardRoutingTable {
        version: 3,
        placements: vec![ShardPlacement::new(
            0,
            vec![ShardReplica::primary(1), ShardReplica::replica(2)],
        )],
    };
    let db = Neo4rDatabaseHandle::open(
        DatabaseConfig::new(&dir, 1, 1)
            .with_server_id(2)
            .with_routing_table(routing_table),
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
            b"REGISTER_REPLICATION_PEER\t1\t127.0.0.1:17687".to_vec(),
        ),
    )
    .unwrap();
    assert_native_response(&mut stream, NativeMessageType::Response, 1, "OK");

    write_frame(
        &mut stream,
        &NativeFrame::new(NativeMessageType::Command, 2, b"CATCH_UP_PLAN".to_vec()),
    )
    .unwrap();
    assert_native_response(
        &mut stream,
        NativeMessageType::Response,
        2,
        "OK\tCATCH_UP_PLAN\tshard=0 primary=1 start=1 peer=registered",
    );

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
fn native_tcp_reports_replication_peer_status() {
    let dir = temp_dir("neo4r-native-replication-peer-status");
    let routing_table = ShardRoutingTable {
        version: 3,
        placements: vec![ShardPlacement::new(
            0,
            vec![ShardReplica::primary(1), ShardReplica::replica(2)],
        )],
    };
    let db = Neo4rDatabaseHandle::open(
        DatabaseConfig::new(&dir, 1, 1)
            .with_server_id(2)
            .with_routing_table(routing_table),
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
            b"REGISTER_REPLICATION_PEER\t1\t127.0.0.1:17687".to_vec(),
        ),
    )
    .unwrap();
    assert_native_response(&mut stream, NativeMessageType::Response, 1, "OK");

    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Command,
            2,
            b"REPLICATION_PEER_STATUS".to_vec(),
        ),
    )
    .unwrap();
    assert_native_response(
            &mut stream,
            NativeMessageType::Response,
            2,
            "OK\tREPLICATION_PEER_STATUS\tserver=1 address=127.0.0.1:17687 primary_shards=0 replica_shards=-",
        );

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
fn backend_catch_up_from_primaries_accepts_batch_limit() {
    let primary_dir = temp_dir("neo4r-server-catch-up-limited-primary");
    let replica_dir = temp_dir("neo4r-server-catch-up-limited-replica");
    let routing_table = ShardRoutingTable {
        version: 3,
        placements: vec![ShardPlacement::new(
            0,
            vec![ShardReplica::primary(1), ShardReplica::replica(2)],
        )],
    };
    let primary = Neo4rDatabaseHandle::open(
        DatabaseConfig::new(&primary_dir, 1, 1)
            .with_server_id(1)
            .with_routing_table(routing_table.clone()),
    )
    .unwrap();
    for name in ["Alice", "Bob", "Carol", "Dave", "Eve"] {
        primary
            .execute_cypher_with_params(
                "CREATE (n:Person {name: $name})",
                [("name".to_string(), Value::String(name.to_string()))]
                    .into_iter()
                    .collect(),
            )
            .unwrap();
    }
    let primary_backend = TcpBackend::new(primary.clone());
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let server = thread::spawn(move || {
        for _ in 0..3 {
            let (stream, _) = listener.accept().unwrap();
            primary_backend.handle_replication_stream(stream).unwrap();
        }
    });

    let replica = Neo4rDatabaseHandle::open(
        DatabaseConfig::new(&replica_dir, 1, 1)
            .with_server_id(2)
            .with_routing_table(routing_table),
    )
    .unwrap();
    let replica_backend = TcpBackend::new(replica.clone());
    replica_backend
        .register_replication_peer(1, address)
        .unwrap();

    let response = replica_backend
        .execute_backend_request(parse_request("CATCH_UP_FROM_PRIMARIES\t2").unwrap());

    server.join().unwrap();
    assert_eq!(
        response,
        BackendResponse::OkCatchUp("shard=0 primary=1 start=1 end=5 fetched=5".to_string())
    );
    assert_eq!(replica.query("MATCH (n:Person) RETURN n").unwrap().len(), 5);
    assert_eq!(replica.read_snapshot().unwrap().applied_indexes(), &[5]);

    drop(primary);
    drop(replica);
    let _ = fs::remove_dir_all(primary_dir);
    let _ = fs::remove_dir_all(replica_dir);
}

#[test]
fn replication_listener_serves_multiple_catch_up_connections_until_shutdown() {
    let primary_dir = temp_dir("neo4r-server-repl-listener-until-primary");
    let replica_dir = temp_dir("neo4r-server-repl-listener-until-replica");
    let routing_table = ShardRoutingTable {
        version: 3,
        placements: vec![ShardPlacement::new(
            0,
            vec![ShardReplica::primary(1), ShardReplica::replica(2)],
        )],
    };
    let primary = Neo4rDatabaseHandle::open(
        DatabaseConfig::new(&primary_dir, 1, 1)
            .with_server_id(1)
            .with_routing_table(routing_table.clone()),
    )
    .unwrap();
    for name in ["Alice", "Bob", "Carol", "Dave", "Eve"] {
        primary
            .execute_cypher_with_params(
                "CREATE (n:Person {name: $name})",
                [("name".to_string(), Value::String(name.to_string()))]
                    .into_iter()
                    .collect(),
            )
            .unwrap();
    }

    let primary_backend = TcpBackend::new(primary.clone());
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let (shutdown_tx, shutdown_rx) = mpsc::channel();
    let server = thread::spawn(move || {
        primary_backend
            .serve_replication_listener_until(listener, shutdown_rx)
            .unwrap()
    });

    let replica = Neo4rDatabaseHandle::open(
        DatabaseConfig::new(&replica_dir, 1, 1)
            .with_server_id(2)
            .with_routing_table(routing_table),
    )
    .unwrap();
    let fetched =
        catch_up_from_tcp_primary_batched(&replica, &address, Duration::from_secs(1), 0, 1, 2)
            .unwrap();
    assert_eq!(fetched, 5);
    shutdown_tx.send(()).unwrap();
    server.join().unwrap();

    assert_eq!(replica.query("MATCH (n:Person) RETURN n").unwrap().len(), 5);
    assert_eq!(replica.read_snapshot().unwrap().applied_indexes(), &[5]);

    drop(primary);
    drop(replica);
    let _ = fs::remove_dir_all(primary_dir);
    let _ = fs::remove_dir_all(replica_dir);
}

#[test]
fn persistent_backend_catch_up_uses_reloaded_replication_peers() {
    let primary_dir = temp_dir("neo4r-server-persistent-catch-up-primary");
    let replica_dir = temp_dir("neo4r-server-persistent-catch-up-replica");
    let routing_table = ShardRoutingTable {
        version: 3,
        placements: vec![ShardPlacement::new(
            0,
            vec![ShardReplica::primary(1), ShardReplica::replica(2)],
        )],
    };
    let primary = Neo4rDatabaseHandle::open(
        DatabaseConfig::new(&primary_dir, 1, 1)
            .with_server_id(1)
            .with_routing_table(routing_table.clone()),
    )
    .unwrap();
    primary
        .create_node(
            vec!["Person".to_string()],
            [("name".to_string(), Value::String("Alice".to_string()))]
                .into_iter()
                .collect(),
        )
        .unwrap();
    let primary_backend = TcpBackend::new(primary.clone());
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let server = thread::spawn(move || {
        primary_backend
            .serve_replication_listener_once(listener)
            .unwrap()
    });

    let replica = Neo4rDatabaseHandle::open(
        DatabaseConfig::new(&replica_dir, 1, 1)
            .with_server_id(2)
            .with_routing_table(routing_table),
    )
    .unwrap();
    let initial_backend =
        TcpBackend::with_persistent_config(replica.clone(), TcpBackendConfig::default()).unwrap();
    initial_backend
        .register_replication_peer(1, address)
        .unwrap();
    drop(initial_backend);

    let reloaded_backend =
        TcpBackend::with_persistent_config(replica.clone(), TcpBackendConfig::default()).unwrap();
    let results = reloaded_backend.catch_up_from_primaries().unwrap();

    server.join().unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].primary_server_id, 1);
    assert_eq!(results[0].start_index, 1);
    assert_eq!(results[0].end_index, 1);
    assert_eq!(results[0].fetched_entries, 1);
    assert_eq!(
        replica
            .query(r#"MATCH (n:Person) WHERE n.name = "Alice" RETURN n.name"#)
            .unwrap()
            .len(),
        1
    );

    drop(primary);
    drop(replica);
    let _ = fs::remove_dir_all(primary_dir);
    let _ = fs::remove_dir_all(replica_dir);
}

#[test]
fn backend_register_replication_peer_updates_write_replicator() {
    let primary_dir = temp_dir("neo4r-server-register-repl-primary");
    let replica_dir = temp_dir("neo4r-server-register-repl-replica");
    let routing_table = ShardRoutingTable {
        version: 3,
        placements: vec![ShardPlacement::new(
            0,
            vec![ShardReplica::primary(1), ShardReplica::replica(2)],
        )],
    };
    let replica = Neo4rDatabaseHandle::open(
        DatabaseConfig::new(&replica_dir, 1, 1)
            .with_server_id(2)
            .with_routing_table(routing_table.clone()),
    )
    .unwrap();
    let replica_backend = TcpBackend::new(replica.clone());
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let server = thread::spawn(move || {
        replica_backend
            .serve_replication_listener_once(listener)
            .unwrap()
    });

    let replicator = Arc::new(TcpShardReplicator::new(routing_table.clone()));
    let primary = Neo4rDatabaseHandle::open_with_replicator(
        DatabaseConfig::new(&primary_dir, 1, 1)
            .with_server_id(1)
            .with_routing_table(routing_table),
        replicator,
    )
    .unwrap();
    let primary_backend = TcpBackend::new(primary.clone());
    assert_eq!(
        primary_backend.execute_backend_request(
            parse_request(&format!("REGISTER_REPLICATION_PEER\t2\t{address}")).unwrap()
        ),
        BackendResponse::OkUnit
    );

    primary
        .create_node(
            vec!["Person".to_string()],
            [("name".to_string(), Value::String("Alice".to_string()))]
                .into_iter()
                .collect(),
        )
        .unwrap();

    server.join().unwrap();
    let status =
        primary_backend.execute_backend_request(parse_request("REPLICATION_STATUS").unwrap());
    let BackendResponse::OkReplicationStatus(status) = status else {
        panic!("expected replication status response");
    };
    assert!(status.contains("server=1"));
    assert!(status.contains(&format!("peers=2={address}")));
    assert!(status.contains("shard:0:primary=1:replicas=2"));
    assert!(
        status.contains("2:"),
        "replication status did not expose replica match index: {status}"
    );
    assert!(
        status.contains("lag=2:0"),
        "replication status did not expose zero replica lag after ack: {status}"
    );

    assert_eq!(
        replica
            .query(r#"MATCH (n:Person) WHERE n.name = "Alice" RETURN n.name"#)
            .unwrap()
            .len(),
        1
    );

    drop(primary);
    drop(replica);
    let _ = fs::remove_dir_all(primary_dir);
    let _ = fs::remove_dir_all(replica_dir);
}

#[test]
fn replication_shard_status_reports_unknown_and_numeric_lag() {
    let status = format_replication_shard_status(&neo4r_db::ShardStatus {
        shard_id: 0,
        primary_server_id: Some(1),
        replica_server_ids: vec![2, 3],
        has_local_copy: true,
        is_local_primary: true,
        applied_index: 7,
        committed_index: 7,
        match_indexes: vec![(2, 5)],
    });

    assert!(status.contains("match=2:5"));
    assert!(status.contains("lag=2:2|3:unknown"));
}

#[test]
fn backend_replication_quorum_succeeds_with_one_missing_replica_peer() {
    let primary_dir = temp_dir("neo4r-server-repl-quorum-primary");
    let replica_dir = temp_dir("neo4r-server-repl-quorum-replica");
    let routing_table = ShardRoutingTable {
        version: 3,
        placements: vec![ShardPlacement::new(
            0,
            vec![
                ShardReplica::primary(1),
                ShardReplica::replica(2),
                ShardReplica::replica(3),
            ],
        )],
    };
    let replica = Neo4rDatabaseHandle::open(
        DatabaseConfig::new(&replica_dir, 1, 1)
            .with_server_id(2)
            .with_routing_table(routing_table.clone()),
    )
    .unwrap();
    let replica_backend = TcpBackend::new(replica.clone());
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let server = thread::spawn(move || {
        replica_backend
            .serve_replication_listener_once(listener)
            .unwrap()
    });

    let replicator = Arc::new(
        TcpShardReplicator::new(routing_table.clone())
            .with_ack_policy(neo4r_db::ReplicationAckPolicy::Quorum)
            .with_connect_timeout(Duration::from_millis(10)),
    );
    let primary = Neo4rDatabaseHandle::open_with_replicator(
        DatabaseConfig::new(&primary_dir, 1, 1)
            .with_server_id(1)
            .with_routing_table(routing_table),
        replicator,
    )
    .unwrap();
    let primary_backend = TcpBackend::new(primary.clone());
    primary_backend
        .register_replication_peer(2, address)
        .unwrap();

    primary
        .create_node(
            vec!["Person".to_string()],
            [("name".to_string(), Value::String("Alice".to_string()))]
                .into_iter()
                .collect(),
        )
        .unwrap();

    server.join().unwrap();
    assert_eq!(
        replica
            .query(r#"MATCH (n:Person) WHERE n.name = "Alice" RETURN n.name"#)
            .unwrap()
            .len(),
        1
    );
    assert_eq!(replica.read_snapshot().unwrap().applied_indexes(), &[1]);

    drop(primary);
    drop(replica);
    let _ = fs::remove_dir_all(primary_dir);
    let _ = fs::remove_dir_all(replica_dir);
}

#[test]
fn backend_replication_all_fails_with_one_missing_replica_peer() {
    let primary_dir = temp_dir("neo4r-server-repl-all-fail-primary");
    let replica_dir = temp_dir("neo4r-server-repl-all-fail-replica");
    let routing_table = ShardRoutingTable {
        version: 3,
        placements: vec![ShardPlacement::new(
            0,
            vec![
                ShardReplica::primary(1),
                ShardReplica::replica(2),
                ShardReplica::replica(3),
            ],
        )],
    };
    let replica = Neo4rDatabaseHandle::open(
        DatabaseConfig::new(&replica_dir, 1, 1)
            .with_server_id(2)
            .with_routing_table(routing_table.clone()),
    )
    .unwrap();

    let replicator = Arc::new(
        TcpShardReplicator::new(routing_table.clone())
            .with_ack_policy(neo4r_db::ReplicationAckPolicy::All)
            .with_connect_timeout(Duration::from_millis(10)),
    );
    let primary = Neo4rDatabaseHandle::open_with_replicator(
        DatabaseConfig::new(&primary_dir, 1, 1)
            .with_server_id(1)
            .with_routing_table(routing_table),
        replicator,
    )
    .unwrap();
    let primary_backend = TcpBackend::new(primary.clone());

    let response = primary_backend
        .execute_backend_request(parse_request("CREATE_NODE\tPerson\tname=s:Alice").unwrap());
    let BackendResponse::Err(message) = response else {
        panic!("expected replication failure, got {response:?}");
    };
    assert!(message.contains("replication ack policy cannot be satisfied"));
    assert!(primary
        .query_with_options(
            r#"MATCH (n:Person) WHERE n.name = "Alice" RETURN n.name"#,
            QueryOptions::default().with_consistency(ReadConsistency::FollowerStale),
        )
        .unwrap()
        .is_empty());
    assert!(replica
        .query(r#"MATCH (n:Person) WHERE n.name = "Alice" RETURN n.name"#)
        .unwrap()
        .is_empty());

    drop(primary);
    drop(replica);
    let _ = fs::remove_dir_all(primary_dir);
    let _ = fs::remove_dir_all(replica_dir);
}

#[test]
fn backend_replication_async_allows_missing_replica_peer() {
    let primary_dir = temp_dir("neo4r-server-repl-async-primary");
    let routing_table = ShardRoutingTable {
        version: 3,
        placements: vec![ShardPlacement::new(
            0,
            vec![ShardReplica::primary(1), ShardReplica::replica(2)],
        )],
    };
    let replicator = Arc::new(
        TcpShardReplicator::new(routing_table.clone())
            .with_ack_policy(neo4r_db::ReplicationAckPolicy::Async)
            .with_connect_timeout(Duration::from_millis(10)),
    );
    let primary = Neo4rDatabaseHandle::open_with_replicator(
        DatabaseConfig::new(&primary_dir, 1, 1)
            .with_server_id(1)
            .with_routing_table(routing_table),
        replicator,
    )
    .unwrap();
    let primary_backend = TcpBackend::new(primary.clone());

    let response = primary_backend
        .execute_backend_request(parse_request("CREATE_NODE\tPerson\tname=s:Alice").unwrap());
    let BackendResponse::OkNode(id) = response else {
        panic!("expected async replication write success, got {response:?}");
    };
    assert_eq!(id, 0);
    assert_eq!(
        primary
            .query(r#"MATCH (n:Person) WHERE n.name = "Alice" RETURN n.name"#)
            .unwrap()
            .len(),
        1
    );
    assert_eq!(primary.committed_indexes().unwrap(), vec![1]);

    drop(primary);
    let _ = fs::remove_dir_all(primary_dir);
}

#[test]
fn persistent_backend_reloads_query_and_replication_peers() {
    let dir = temp_dir("neo4r-server-persistent-peers");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1)).unwrap();
    let backend =
        TcpBackend::with_persistent_config(db.clone(), TcpBackendConfig::default()).unwrap();
    backend.register_query_peer(2, "127.0.0.1:17687").unwrap();
    backend
        .register_replication_peer(3, "127.0.0.1:17688")
        .unwrap();
    drop(backend);

    let reloaded =
        TcpBackend::with_persistent_config(db.clone(), TcpBackendConfig::default()).unwrap();

    assert_eq!(
        reloaded.list_query_peers().unwrap(),
        vec![(2, "127.0.0.1:17687".to_string())]
    );
    assert_eq!(
        reloaded.list_replication_peers().unwrap(),
        vec![(3, "127.0.0.1:17688".to_string())]
    );

    drop(reloaded);
    drop(db);
    let _ = fs::remove_dir_all(dir);
}

#[test]
fn persistent_backend_replication_peer_status_uses_reloaded_peers() {
    let dir = temp_dir("neo4r-server-persistent-peer-status");
    let routing_table = ShardRoutingTable {
        version: 3,
        placements: vec![
            ShardPlacement::new(0, vec![ShardReplica::primary(1), ShardReplica::replica(2)]),
            ShardPlacement::new(1, vec![ShardReplica::primary(2), ShardReplica::replica(1)]),
        ],
    };
    let db = Neo4rDatabaseHandle::open(
        DatabaseConfig::new(&dir, 2, 1)
            .with_server_id(2)
            .with_routing_table(routing_table),
    )
    .unwrap();
    let backend =
        TcpBackend::with_persistent_config(db.clone(), TcpBackendConfig::default()).unwrap();
    assert_eq!(
        backend.execute_backend_request(parse_request("REPLICATION_PEER_STATUS").unwrap()),
        BackendResponse::OkReplicationPeerStatus(
            "server=1 address=missing primary_shards=0 replica_shards=1".to_string()
        )
    );
    backend
        .register_replication_peer(1, "127.0.0.1:17687")
        .unwrap();
    drop(backend);

    let reloaded =
        TcpBackend::with_persistent_config(db.clone(), TcpBackendConfig::default()).unwrap();
    assert_eq!(
        reloaded.execute_backend_request(parse_request("REPLICATION_PEER_STATUS").unwrap()),
        BackendResponse::OkReplicationPeerStatus(
            "server=1 address=127.0.0.1:17687 primary_shards=0 replica_shards=1".to_string()
        )
    );

    drop(reloaded);
    drop(db);
    let _ = fs::remove_dir_all(dir);
}

#[test]
fn persistent_backend_catch_up_plan_uses_reloaded_replication_peers() {
    let dir = temp_dir("neo4r-server-persistent-catch-up-plan");
    let routing_table = ShardRoutingTable {
        version: 3,
        placements: vec![ShardPlacement::new(
            0,
            vec![ShardReplica::primary(1), ShardReplica::replica(2)],
        )],
    };
    let db = Neo4rDatabaseHandle::open(
        DatabaseConfig::new(&dir, 1, 1)
            .with_server_id(2)
            .with_routing_table(routing_table),
    )
    .unwrap();
    let backend =
        TcpBackend::with_persistent_config(db.clone(), TcpBackendConfig::default()).unwrap();
    assert_eq!(
        backend.execute_backend_request(parse_request("CATCH_UP_PLAN").unwrap()),
        BackendResponse::OkCatchUpPlan("shard=0 primary=1 start=1 peer=missing".to_string())
    );
    backend
        .register_replication_peer(1, "127.0.0.1:17687")
        .unwrap();
    drop(backend);

    let reloaded =
        TcpBackend::with_persistent_config(db.clone(), TcpBackendConfig::default()).unwrap();
    assert_eq!(
        reloaded.execute_backend_request(parse_request("CATCH_UP_PLAN").unwrap()),
        BackendResponse::OkCatchUpPlan("shard=0 primary=1 start=1 peer=registered".to_string())
    );

    drop(reloaded);
    drop(db);
    let _ = fs::remove_dir_all(dir);
}

#[test]
fn persistent_backend_reloads_replication_peer_into_new_replicator() {
    let primary_dir = temp_dir("neo4r-server-persistent-repl-primary");
    let replica_dir = temp_dir("neo4r-server-persistent-repl-replica");
    let routing_table = ShardRoutingTable {
        version: 3,
        placements: vec![ShardPlacement::new(
            0,
            vec![ShardReplica::primary(1), ShardReplica::replica(2)],
        )],
    };
    let replica = Neo4rDatabaseHandle::open(
        DatabaseConfig::new(&replica_dir, 1, 1)
            .with_server_id(2)
            .with_routing_table(routing_table.clone()),
    )
    .unwrap();
    let replica_backend = TcpBackend::new(replica.clone());
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let server = thread::spawn(move || {
        replica_backend
            .serve_replication_listener_once(listener)
            .unwrap()
    });

    let first_replicator = Arc::new(TcpShardReplicator::new(routing_table.clone()));
    let primary = Neo4rDatabaseHandle::open_with_replicator(
        DatabaseConfig::new(&primary_dir, 1, 1)
            .with_server_id(1)
            .with_routing_table(routing_table.clone()),
        first_replicator,
    )
    .unwrap();
    let backend =
        TcpBackend::with_persistent_config(primary.clone(), TcpBackendConfig::default()).unwrap();
    backend.register_replication_peer(2, address).unwrap();
    drop(backend);
    drop(primary);

    let reloaded_replicator = Arc::new(TcpShardReplicator::new(routing_table.clone()));
    let reloaded_primary = Neo4rDatabaseHandle::open_with_replicator(
        DatabaseConfig::new(&primary_dir, 1, 1)
            .with_server_id(1)
            .with_routing_table(routing_table),
        reloaded_replicator,
    )
    .unwrap();
    let _reloaded_backend =
        TcpBackend::with_persistent_config(reloaded_primary.clone(), TcpBackendConfig::default())
            .unwrap();

    reloaded_primary
        .create_node(
            vec!["Person".to_string()],
            [("name".to_string(), Value::String("Alice".to_string()))]
                .into_iter()
                .collect(),
        )
        .unwrap();

    server.join().unwrap();
    assert_eq!(
        replica
            .query(r#"MATCH (n:Person) WHERE n.name = "Alice" RETURN n.name"#)
            .unwrap()
            .len(),
        1
    );

    drop(reloaded_primary);
    drop(replica);
    let _ = fs::remove_dir_all(primary_dir);
    let _ = fs::remove_dir_all(replica_dir);
}

#[test]
fn persistent_backends_catch_up_then_live_replicate_with_reloaded_peers() {
    let primary_dir = temp_dir("neo4r-server-persistent-catchup-live-primary");
    let replica_dir = temp_dir("neo4r-server-persistent-catchup-live-replica");
    let routing_table = ShardRoutingTable {
        version: 3,
        placements: vec![ShardPlacement::new(
            0,
            vec![ShardReplica::primary(1), ShardReplica::replica(2)],
        )],
    };

    let primary_listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let primary_addr = primary_listener.local_addr().unwrap().to_string();
    let replica_listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let replica_addr = replica_listener.local_addr().unwrap().to_string();

    let initial_primary = Neo4rDatabaseHandle::open(
        DatabaseConfig::new(&primary_dir, 1, 1)
            .with_server_id(1)
            .with_routing_table(routing_table.clone()),
    )
    .unwrap();
    initial_primary
        .create_node(
            vec!["Person".to_string()],
            [("name".to_string(), Value::String("Alice".to_string()))]
                .into_iter()
                .collect(),
        )
        .unwrap();
    let initial_primary_backend =
        TcpBackend::with_persistent_config(initial_primary.clone(), TcpBackendConfig::default())
            .unwrap();
    initial_primary_backend
        .register_replication_peer(2, replica_addr.clone())
        .unwrap();
    drop(initial_primary_backend);
    drop(initial_primary);

    let initial_replica = Neo4rDatabaseHandle::open(
        DatabaseConfig::new(&replica_dir, 1, 1)
            .with_server_id(2)
            .with_routing_table(routing_table.clone()),
    )
    .unwrap();
    let initial_replica_backend =
        TcpBackend::with_persistent_config(initial_replica.clone(), TcpBackendConfig::default())
            .unwrap();
    initial_replica_backend
        .register_replication_peer(1, primary_addr)
        .unwrap();
    drop(initial_replica_backend);
    drop(initial_replica);

    let reloaded_primary_replicator = Arc::new(TcpShardReplicator::new(routing_table.clone()));
    let reloaded_primary = Neo4rDatabaseHandle::open_with_replicator(
        DatabaseConfig::new(&primary_dir, 1, 1)
            .with_server_id(1)
            .with_routing_table(routing_table.clone()),
        reloaded_primary_replicator,
    )
    .unwrap();
    let reloaded_primary_backend =
        TcpBackend::with_persistent_config(reloaded_primary.clone(), TcpBackendConfig::default())
            .unwrap();
    let reloaded_replica = Neo4rDatabaseHandle::open(
        DatabaseConfig::new(&replica_dir, 1, 1)
            .with_server_id(2)
            .with_routing_table(routing_table),
    )
    .unwrap();
    let reloaded_replica_backend =
        TcpBackend::with_persistent_config(reloaded_replica.clone(), TcpBackendConfig::default())
            .unwrap();

    let catch_up_primary_backend = reloaded_primary_backend.clone();
    let catch_up_server = thread::spawn(move || {
        catch_up_primary_backend
            .serve_replication_listener_once(primary_listener)
            .unwrap()
    });
    let results = reloaded_replica_backend.catch_up_from_primaries().unwrap();
    catch_up_server.join().unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].start_index, 1);
    assert_eq!(results[0].end_index, 1);
    assert_eq!(results[0].fetched_entries, 1);
    assert_eq!(
        reloaded_replica
            .query(r#"MATCH (n:Person) WHERE n.name = "Alice" RETURN n.name"#)
            .unwrap()
            .len(),
        1
    );

    let live_replica_backend = reloaded_replica_backend.clone();
    let live_replica_server = thread::spawn(move || {
        live_replica_backend
            .serve_replication_listener_once(replica_listener)
            .unwrap()
    });
    reloaded_primary
        .create_node(
            vec!["Person".to_string()],
            [("name".to_string(), Value::String("Bob".to_string()))]
                .into_iter()
                .collect(),
        )
        .unwrap();
    live_replica_server.join().unwrap();
    assert_eq!(
        reloaded_replica
            .query(r#"MATCH (n:Person) WHERE n.name = "Bob" RETURN n.name"#)
            .unwrap()
            .len(),
        1
    );
    assert_eq!(reloaded_replica.committed_indexes().unwrap(), vec![2]);

    drop(reloaded_primary_backend);
    drop(reloaded_replica_backend);
    drop(reloaded_primary);
    drop(reloaded_replica);
    let _ = fs::remove_dir_all(primary_dir);
    let _ = fs::remove_dir_all(replica_dir);
}

fn assert_response(reader: &mut BufReader<TcpStream>, expected: &str) {
    assert_eq!(read_line(reader), expected);
}

fn read_line(reader: &mut BufReader<TcpStream>) -> String {
    let mut line = String::new();
    reader.read_line(&mut line).unwrap();
    line
}

fn assert_native_response(
    stream: &mut TcpStream,
    message_type: NativeMessageType,
    request_id: u64,
    expected_payload: &str,
) {
    let payload = read_native_payload(stream, message_type, request_id);
    assert_eq!(payload, expected_payload);
}

fn read_native_payload(
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

fn test_map_param(entries: &[(&str, Value)]) -> String {
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

fn web_request(backend: TcpBackend, request: &str) -> String {
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

fn test_encoded_value(value: &Value) -> String {
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

fn test_hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

fn temp_dir(prefix: &str) -> std::path::PathBuf {
    let mut path = std::env::temp_dir();
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    path.push(format!("{prefix}-{}-{nanos}", std::process::id()));
    let _ = fs::remove_dir_all(&path);
    path
}
