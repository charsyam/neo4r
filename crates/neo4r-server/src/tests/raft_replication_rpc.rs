#![allow(unused_imports)]
use super::*;

#[test]
pub(super) fn raft_vote_rpc_persists_vote_on_replica() {
    let dir = temp_dir("neo4r-server-raft-vote");
    let routing_table = ShardRoutingTable {
        version: 3,
        placements: vec![ShardPlacement::new(
            0,
            vec![ShardReplica::primary(1), ShardReplica::replica(2)],
        )],
    };
    let replica = Neo4rDatabaseHandle::open(
        DatabaseConfig::new(&dir, 1, 1)
            .with_server_id(2)
            .with_routing_table(routing_table)
            .with_raft_enabled(true),
    )
    .unwrap();
    let backend = TcpBackend::new(replica);
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let server = thread::spawn(move || backend.serve_replication_listener_once(listener).unwrap());

    let response = request_tcp_raft_vote(
        &address,
        Duration::from_secs(1),
        0,
        RequestVoteRequest {
            term: 2,
            candidate_id: 1,
            last_log_index: 0,
            last_log_term: 0,
        },
    )
    .unwrap();

    server.join().unwrap();
    assert_eq!(response.term, 2);
    assert!(response.vote_granted);

    let _ = fs::remove_dir_all(dir);
}

#[test]
pub(super) fn raft_election_round_promotes_candidate_after_peer_vote() {
    let candidate_dir = temp_dir("neo4r-server-raft-election-candidate");
    let voter_dir = temp_dir("neo4r-server-raft-election-voter");
    let routing_table = ShardRoutingTable {
        version: 3,
        placements: vec![ShardPlacement::new(
            0,
            vec![
                ShardReplica::primary(3),
                ShardReplica::replica(1),
                ShardReplica::replica(2),
            ],
        )],
    };
    let voter = Neo4rDatabaseHandle::open(
        DatabaseConfig::new(&voter_dir, 1, 1)
            .with_server_id(2)
            .with_routing_table(routing_table.clone())
            .with_raft_enabled(true),
    )
    .unwrap();
    let backend = TcpBackend::new(voter);
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let server = thread::spawn(move || backend.serve_replication_listener_once(listener).unwrap());

    let replicator =
        Arc::new(TcpShardReplicator::new(routing_table.clone()).with_raft_transport(true));
    replicator.register_peer(2, address).unwrap();
    let candidate = Neo4rDatabaseHandle::open_with_replicator(
        DatabaseConfig::new(&candidate_dir, 1, 1)
            .with_server_id(1)
            .with_routing_table(routing_table)
            .with_raft_enabled(true),
        replicator.clone(),
    )
    .unwrap();

    assert_eq!(replicator.run_raft_election_round(&candidate).unwrap(), 1);
    server.join().unwrap();

    drop(candidate);
    let _ = fs::remove_dir_all(candidate_dir);
    let _ = fs::remove_dir_all(voter_dir);
}

#[test]
pub(super) fn replication_listener_accepts_consecutive_entry_batches() {
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
pub(super) fn backend_catch_up_from_primaries_fetches_replica_shard_logs() {
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
pub(super) fn backend_catch_up_from_primaries_fetches_batches_idempotently() {
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
pub(super) fn backend_catch_up_from_primary_targets_one_peer() {
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
pub(super) fn backend_catch_up_plan_reports_target_shards() {
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
pub(super) fn backend_replication_peer_status_reports_roles_and_missing_addresses() {
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
pub(super) fn native_tcp_catches_up_from_primary() {
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
pub(super) fn native_tcp_reports_catch_up_plan() {
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
pub(super) fn native_tcp_reports_replication_peer_status() {
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
pub(super) fn backend_catch_up_from_primaries_accepts_batch_limit() {
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
pub(super) fn replication_listener_serves_multiple_catch_up_connections_until_shutdown() {
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
pub(super) fn persistent_backend_catch_up_uses_reloaded_replication_peers() {
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
