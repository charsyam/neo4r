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
