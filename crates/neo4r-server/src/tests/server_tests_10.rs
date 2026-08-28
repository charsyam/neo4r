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
