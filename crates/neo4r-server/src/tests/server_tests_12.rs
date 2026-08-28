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
fn raft_transport_appends_then_commits_replicated_entries() {
    let primary_dir = temp_dir("neo4r-server-raft-primary");
    let replica_dir = temp_dir("neo4r-server-raft-replica");
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
            .with_routing_table(routing_table.clone())
            .with_raft_enabled(true),
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

    let replicator =
        Arc::new(TcpShardReplicator::new(routing_table.clone()).with_raft_transport(true));
    replicator.register_peer(2, address).unwrap();
    let primary = Neo4rDatabaseHandle::open_with_replicator(
        DatabaseConfig::new(&primary_dir, 1, 1)
            .with_server_id(1)
            .with_routing_table(routing_table)
            .with_raft_enabled(true),
        replicator,
    )
    .unwrap();

    primary
        .create_node(
            vec!["Person".to_string()],
            [("name".to_string(), Value::String("RaftAlice".to_string()))]
                .into_iter()
                .collect(),
        )
        .unwrap();

    server.join().unwrap();
    assert!(replica
        .query(r#"MATCH (n:Person) WHERE n.name = "RaftAlice" RETURN n.name"#)
        .is_err());
    assert!(replica
        .raft_election_candidates(Duration::from_secs(60))
        .unwrap()
        .is_empty());
    assert_eq!(
        replica
            .query_with_options(
                r#"MATCH (n:Person) WHERE n.name = "RaftAlice" RETURN n.name"#,
                QueryOptions::default().with_consistency(ReadConsistency::FollowerStale),
            )
            .unwrap()
            .len(),
        1
    );
    assert_eq!(replica.read_snapshot().unwrap().committed_indexes(), &[1]);
    assert_eq!(replica.read_snapshot().unwrap().applied_indexes(), &[1]);

    drop(primary);
    drop(replica);
    let _ = fs::remove_dir_all(primary_dir);
    let _ = fs::remove_dir_all(replica_dir);
}
