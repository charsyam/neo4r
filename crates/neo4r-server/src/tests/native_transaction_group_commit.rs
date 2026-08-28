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
