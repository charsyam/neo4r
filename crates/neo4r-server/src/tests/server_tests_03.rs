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
