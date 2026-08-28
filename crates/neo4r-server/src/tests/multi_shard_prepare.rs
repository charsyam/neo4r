#![allow(unused_imports)]
use super::*;

#[test]
pub(super) fn native_read_write_transaction_prepare_commits_remote_multi_shard_property_replacements(
) {
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
pub(super) fn native_read_write_transaction_prepare_commits_remote_multi_shard_parameterized_map_replacements(
) {
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
pub(super) fn native_read_write_transaction_prepare_commits_remote_multi_shard_property_map_merges()
{
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
pub(super) fn native_read_write_transaction_prepare_commits_mixed_local_remote_sets() {
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
pub(super) fn native_read_write_transaction_prepare_commits_mixed_local_remote_merge_nodes() {
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
