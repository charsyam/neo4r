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
pub(super) fn backend_rejects_replication_peer_identity_cycles() {
    let dir = temp_dir("neo4r-server-register-repl-self-cycle");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1).with_server_id(7)).unwrap();
    let backend = TcpBackend::new(db.clone());

    let alias_cycle = backend.execute_backend_request(
        parse_request("REGISTER_REPLICATION_PEER\t8\t127.0.0.1:17687\t7\ttcp").unwrap(),
    );

    let BackendResponse::Err(message) = alias_cycle else {
        panic!("expected alias cycle rejection");
    };
    assert!(message.contains("node_id 7 cannot point to local server"));

    let direct_cycle = backend.execute_backend_request(
        parse_request("REGISTER_REPLICATION_PEER\t7\t127.0.0.1:17687\t8\ttcp").unwrap(),
    );

    let BackendResponse::Err(message) = direct_cycle else {
        panic!("expected direct cycle rejection");
    };
    assert!(message.contains("replication peer 7 cannot point to local server"));

    drop(db);
    let _ = fs::remove_dir_all(dir);
}

#[test]
pub(super) fn gossip_node_materializes_query_address_book_without_replication_endpoint() {
    let dir = temp_dir("neo4r-server-gossip-node-address-book");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1).with_server_id(1)).unwrap();
    let backend = TcpBackend::new(db.clone());

    assert_eq!(
        backend.execute_backend_request(
            parse_request("GOSSIP_NODE\t2\t127.0.0.1:17688\t127.0.0.1:18688\t7\t30000").unwrap()
        ),
        BackendResponse::OkGossip("accepted=true".to_string())
    );
    assert_eq!(
        backend.execute_backend_request(
            parse_request("GOSSIP_NODE\t2\t127.0.0.1:17689\t127.0.0.1:18689\t6\t30000").unwrap()
        ),
        BackendResponse::OkGossip("accepted=false".to_string())
    );

    let BackendResponse::OkGossip(nodes) =
        backend.execute_backend_request(parse_request("LIST_GOSSIP_NODES").unwrap())
    else {
        panic!("expected gossip node list");
    };
    assert!(nodes.contains("2:query=127.0.0.1:17688"));
    assert!(nodes.contains("replication=127.0.0.1:18688"));
    assert!(nodes.contains("incarnation=7"));
    assert!(nodes.contains("state=alive"));

    assert_eq!(
        backend.list_query_peers().unwrap(),
        vec![(2, "127.0.0.1:17688".to_string())]
    );
    assert!(backend.list_replication_peers().unwrap().is_empty());

    drop(db);
    let _ = fs::remove_dir_all(dir);
}

#[test]
pub(super) fn gossip_refresh_from_membership_seeds_address_books() {
    let dir = temp_dir("neo4r-server-gossip-refresh-membership");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1).with_server_id(1)).unwrap();
    db.request_cluster_join(2, "127.0.0.1:17688".to_string(), 1, 1, 1)
        .unwrap();
    let backend = TcpBackend::new(db.clone());

    assert_eq!(
        backend.execute_backend_request(parse_request("GOSSIP_REFRESH_MEMBERSHIP").unwrap()),
        BackendResponse::OkGossip("accepted=1".to_string())
    );

    let BackendResponse::OkGossip(nodes) =
        backend.execute_backend_request(parse_request("LIST_GOSSIP_NODES").unwrap())
    else {
        panic!("expected gossip node list");
    };
    assert!(nodes.contains("2:query=127.0.0.1:17688"));
    assert!(nodes.contains("replication=127.0.0.1:17688"));
    assert!(nodes.contains("state=alive"));
    assert_eq!(
        backend.list_query_peers().unwrap(),
        vec![(2, "127.0.0.1:17688".to_string())]
    );

    drop(db);
    let _ = fs::remove_dir_all(dir);
}

#[test]
pub(super) fn backend_rejects_indirect_replication_peer_identity_cycles() {
    let dir = temp_dir("neo4r-server-register-repl-indirect-cycle");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1).with_server_id(1)).unwrap();
    let backend = TcpBackend::new(db.clone());

    assert_eq!(
        backend.execute_backend_request(
            parse_request("REGISTER_REPLICATION_PEER\t2\t127.0.0.1:17688\t3\ttcp").unwrap()
        ),
        BackendResponse::OkUnit
    );
    assert_eq!(
        backend.execute_backend_request(
            parse_request("REGISTER_REPLICATION_PEER\t3\t127.0.0.1:17689\t4\ttcp").unwrap()
        ),
        BackendResponse::OkUnit
    );

    let response = backend.execute_backend_request(
        parse_request("REGISTER_REPLICATION_PEER\t4\t127.0.0.1:17690\t2\ttcp").unwrap(),
    );

    let BackendResponse::Err(message) = response else {
        panic!("expected indirect cycle rejection");
    };
    assert!(message.contains("replication peer identity cycle detected"));

    drop(db);
    let _ = fs::remove_dir_all(dir);
}

#[test]
pub(super) fn backend_negotiates_replication_peer_identity_before_registration() {
    let primary_dir = temp_dir("neo4r-server-negotiate-repl-primary");
    let replica_dir = temp_dir("neo4r-server-negotiate-repl-replica");
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

    let primary = Neo4rDatabaseHandle::open(
        DatabaseConfig::new(&primary_dir, 1, 1)
            .with_server_id(1)
            .with_routing_table(routing_table),
    )
    .unwrap();
    let primary_backend = TcpBackend::new(primary.clone());
    assert_eq!(
        primary_backend.execute_backend_request(
            parse_request(&format!("NEGOTIATE_REPLICATION_PEER\t2\t{address}\t2")).unwrap()
        ),
        BackendResponse::OkUnit
    );

    server.join().unwrap();
    assert_eq!(
        primary_backend.list_replication_peers().unwrap(),
        vec![(2, address)]
    );

    drop(primary);
    drop(replica);
    let _ = fs::remove_dir_all(primary_dir);
    let _ = fs::remove_dir_all(replica_dir);
}

#[test]
pub(super) fn backend_negotiation_rejects_non_member_peer() {
    let dir = temp_dir("neo4r-server-negotiate-repl-non-member");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1).with_server_id(1)).unwrap();
    let backend = TcpBackend::new(db.clone());

    let response = backend.execute_backend_request(
        parse_request("NEGOTIATE_REPLICATION_PEER\t2\t127.0.0.1:17688\t2").unwrap(),
    );

    let BackendResponse::Err(message) = response else {
        panic!("expected non-member rejection");
    };
    assert!(message.contains("not present in the routing table"));

    drop(db);
    let _ = fs::remove_dir_all(dir);
}

#[test]
pub(super) fn backend_cluster_registry_reports_routing_and_query_peers() {
    let dir = temp_dir("neo4r-server-cluster-registry");
    let routing_table = ShardRoutingTable {
        version: 9,
        placements: vec![ShardPlacement::new(
            0,
            vec![ShardReplica::primary(1), ShardReplica::replica(2)],
        )],
    };
    let db = Neo4rDatabaseHandle::open(
        DatabaseConfig::new(&dir, 1, 1)
            .with_server_id(1)
            .with_routing_table(routing_table),
    )
    .unwrap();
    let backend = TcpBackend::new(db.clone());
    backend.register_query_peer(2, "127.0.0.1:17688").unwrap();

    let response = backend.execute_backend_request(parse_request("CLUSTER_REGISTRY").unwrap());

    let BackendResponse::OkClusterRegistry(registry) = response else {
        panic!("expected cluster registry response");
    };
    assert!(registry.contains("database=default"));
    assert!(registry.contains("local_server=1"));
    assert!(registry.contains("routing_version=9"));
    assert!(registry.contains("ownership_epoch=9"));
    assert!(registry.contains("membership_index="));
    assert!(registry.contains("metadata_index="));
    assert!(registry.contains("generated_at="));
    assert!(registry.contains("ttl_ms=5000"));
    assert!(registry.contains("migration=idle"));
    assert!(registry.contains("raft="));
    assert!(registry.contains("query_peers=2:127.0.0.1:17688"));
    assert!(registry.contains("shard=0:replicas=1:primary|2:replica"));

    drop(db);
    let _ = fs::remove_dir_all(dir);
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
