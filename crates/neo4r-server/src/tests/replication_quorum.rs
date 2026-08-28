#![allow(unused_imports)]
use super::*;

#[test]
pub(super) fn backend_register_replication_peer_updates_write_replicator() {
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
pub(super) fn replication_shard_status_reports_unknown_and_numeric_lag() {
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
pub(super) fn backend_replication_quorum_succeeds_with_one_missing_replica_peer() {
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
pub(super) fn backend_replication_all_fails_with_one_missing_replica_peer() {
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
pub(super) fn backend_replication_async_allows_missing_replica_peer() {
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
pub(super) fn persistent_backend_reloads_query_and_replication_peers() {
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
pub(super) fn persistent_backend_replication_peer_status_uses_reloaded_peers() {
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
pub(super) fn persistent_backend_catch_up_plan_uses_reloaded_replication_peers() {
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
pub(super) fn persistent_backend_reloads_replication_peer_into_new_replicator() {
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
pub(super) fn persistent_backends_catch_up_then_live_replicate_with_reloaded_peers() {
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

pub(super) fn assert_response(reader: &mut BufReader<TcpStream>, expected: &str) {
    assert_eq!(read_line(reader), expected);
}

pub(super) fn read_line(reader: &mut BufReader<TcpStream>) -> String {
    let mut line = String::new();
    reader.read_line(&mut line).unwrap();
    line
}

pub(super) fn assert_native_response(
    stream: &mut TcpStream,
    message_type: NativeMessageType,
    request_id: u64,
    expected_payload: &str,
) {
    let payload = read_native_payload(stream, message_type, request_id);
    assert_eq!(payload, expected_payload);
}

pub(super) fn read_native_payload(
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

pub(super) fn test_map_param(entries: &[(&str, Value)]) -> String {
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

pub(super) fn web_request(backend: TcpBackend, request: &str) -> String {
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

pub(super) fn first_backup_payload_file(path: &Path) -> Option<PathBuf> {
    if path.is_file() {
        return (path.file_name()?.to_string_lossy() != BACKUP_MANIFEST_FILE).then(|| path.into());
    }
    let mut entries = fs::read_dir(path).ok()?.flatten().collect::<Vec<_>>();
    entries.sort_by_key(|entry| entry.path());
    for entry in entries {
        if let Some(payload) = first_backup_payload_file(&entry.path()) {
            return Some(payload);
        }
    }
    None
}

pub(super) fn test_encoded_value(value: &Value) -> String {
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

pub(super) fn test_hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

pub(super) fn temp_dir(prefix: &str) -> std::path::PathBuf {
    let mut path = std::env::temp_dir();
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    path.push(format!("{prefix}-{}-{nanos}", std::process::id()));
    let _ = fs::remove_dir_all(&path);
    path
}
