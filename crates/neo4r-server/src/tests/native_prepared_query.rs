#![allow(unused_imports)]
use super::*;

#[test]
pub(super) fn native_prepared_query_rejects_missing_params_before_execution() {
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
        &format!("OK\tTX_STATUS\t{tx_id}\tREAD_WRITE\tSNAPSHOT\t0\townership_epoch=1"),
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
pub(super) fn native_prepared_query_plan_reports_access_path_and_tx_context() {
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
pub(super) fn native_prepared_query_is_session_scoped() {
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
pub(super) fn tcp_backend_reports_parse_errors() {
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
pub(super) fn backend_distributed_query_fans_out_to_remote_shards() {
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
pub(super) fn distributed_query_primary_preference_requires_primary_peer() {
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
pub(super) fn distributed_query_prefer_replica_uses_replica_peer() {
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
pub(super) fn native_command_forwards_shard_write_to_remote_primary() {
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
pub(super) fn native_command_forwards_relationship_cud_to_remote_primary() {
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
pub(super) fn native_command_forwards_node_label_cud_to_remote_primary() {
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
