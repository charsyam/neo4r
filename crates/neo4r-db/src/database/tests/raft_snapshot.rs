#![allow(unused_imports)]
use super::*;
use neo4r_core::{GraphState, ShardPlacement, ShardReplica, Term, Value};
use neo4r_query::QueryValue;
use std::fs;
use std::net::TcpListener;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[test]
pub(super) fn raft_append_truncates_divergent_segmented_wal_suffix() {
    let dir = temp_dir("facade-raft-divergent-wal-truncate");
    let routing_table = ShardRoutingTable {
        version: 1,
        placements: vec![ShardPlacement::new(
            0,
            vec![ShardReplica::primary(9), ShardReplica::replica(1)],
        )],
    };
    let log = neo4r_storage::SegmentedShardLog::open(&dir, 0, 2).unwrap();
    log.append(&LogEntry::new(
        0,
        1,
        1,
        Command::CreateNode {
            id: 0,
            labels: vec!["Person".to_string()],
            properties: Properties::new(),
        },
    ))
    .unwrap();
    log.append(&LogEntry::new(
        0,
        2,
        2,
        Command::SetNodeProperty {
            id: 0,
            key: "name".to_string(),
            value: Value::String("OldSuffix".to_string()),
        },
    ))
    .unwrap();
    neo4r_storage::CommitStore::open(&dir, 0)
        .unwrap()
        .save(1, 1)
        .unwrap();

    let db = Neo4rDatabaseHandle::open(
        DatabaseConfig::new(&dir, 1, 1)
            .with_server_id(1)
            .with_routing_table(routing_table)
            .with_raft_enabled(true),
    )
    .unwrap();
    db.apply_raft_append_entries(
        0,
        vec![LogEntry::new(
            0,
            3,
            2,
            Command::SetNodeProperty {
                id: 0,
                key: "name".to_string(),
                value: Value::String("NewSuffix".to_string()),
            },
        )],
        2,
    )
    .unwrap();

    let reopened_log = neo4r_storage::SegmentedShardLog::open(&dir, 0, 2).unwrap();
    let replayed = reopened_log.replay().unwrap();
    assert_eq!(replayed.len(), 2);
    assert_eq!(replayed[1].term, 3);
    assert_eq!(
        db.query_with_options(
            r#"MATCH (n:Person) WHERE n.name = "NewSuffix" RETURN n.name"#,
            QueryOptions::default().with_consistency(ReadConsistency::FollowerStale),
        )
        .unwrap()
        .len(),
        1
    );

    drop(db);
    let _ = fs::remove_dir_all(dir);
}

#[test]
pub(super) fn raft_strong_read_requires_leader_lease_but_follower_stale_can_read() {
    let dir = temp_dir("facade-raft-read-index-lease");
    let routing_table = ShardRoutingTable {
        version: 1,
        placements: vec![ShardPlacement::new(0, vec![ShardReplica::primary(1)])],
    };
    let db = Neo4rDatabaseHandle::open(
        DatabaseConfig::new(&dir, 1, 1)
            .with_server_id(1)
            .with_routing_table(routing_table)
            .with_raft_enabled(true),
    )
    .unwrap();
    let payload = snapshot_payload(&dir, 0, 2, 7, "LeaseSnapshotAlice");
    db.install_raft_snapshot(crate::InstallSnapshotRequest {
        term: 2,
        leader_id: 9,
        metadata: crate::RaftSnapshotMetadata {
            shard_id: 0,
            last_included_term: 2,
            last_included_index: 7,
        },
        payload,
    })
    .unwrap();

    assert!(matches!(
        db.query(r#"MATCH (n:Person) RETURN n"#),
        Err(DatabaseError::Replication(message)) if message.contains("leader lease")
    ));
    assert_eq!(
        db.query_with_options(
            r#"MATCH (n:Person) RETURN n"#,
            QueryOptions::default().with_consistency(ReadConsistency::FollowerStale),
        )
        .unwrap()
        .len(),
        1
    );

    drop(db);
    let _ = fs::remove_dir_all(dir);
}

#[test]
pub(super) fn tcp_install_snapshot_updates_replica_snapshot_payload() {
    let dir = temp_dir("facade-tcp-install-snapshot");
    let routing_table = ShardRoutingTable {
        version: 1,
        placements: vec![ShardPlacement::new(0, vec![ShardReplica::primary(1)])],
    };
    let replica = Neo4rDatabaseHandle::open(
        DatabaseConfig::new(&dir, 1, 1)
            .with_server_id(1)
            .with_routing_table(routing_table)
            .with_raft_enabled(true),
    )
    .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let replica_for_listener = replica.clone();
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        crate::handle_tcp_replication_stream(&replica_for_listener, &mut stream).unwrap();
    });

    let payload = snapshot_payload(&dir, 0, 4, 11, "TcpSnapshotAlice");
    let response = crate::request_tcp_install_snapshot(
        &address,
        Duration::from_secs(1),
        crate::InstallSnapshotRequest {
            term: 5,
            leader_id: 9,
            metadata: crate::RaftSnapshotMetadata {
                shard_id: 0,
                last_included_term: 4,
                last_included_index: 11,
            },
            payload: payload.clone(),
        },
    )
    .unwrap();

    assert!(response.success);
    assert_eq!(response.last_included_index, 11);
    assert_eq!(replica.read_snapshot().unwrap().committed_indexes(), &[11]);
    assert_eq!(
        neo4r_storage::SnapshotStore::open(&dir, 0)
            .unwrap()
            .load_payload()
            .unwrap(),
        Some(payload)
    );
    assert_eq!(
        replica
            .query_with_options(
                r#"MATCH (n:Person) WHERE n.name = "TcpSnapshotAlice" RETURN n.name"#,
                QueryOptions::default().with_consistency(ReadConsistency::FollowerStale),
            )
            .unwrap()
            .len(),
        1
    );

    server.join().unwrap();
    drop(replica);
    let _ = fs::remove_dir_all(dir);
}

#[test]
pub(super) fn raft_snapshot_install_then_append_survives_reopen() {
    let dir = temp_dir("facade-raft-snapshot-append-reopen");
    let routing_table = ShardRoutingTable {
        version: 1,
        placements: vec![ShardPlacement::new(0, vec![ShardReplica::primary(1)])],
    };
    let replica = Neo4rDatabaseHandle::open(
        DatabaseConfig::new(&dir, 1, 1)
            .with_server_id(1)
            .with_routing_table(routing_table.clone())
            .with_raft_enabled(true),
    )
    .unwrap();
    let payload = snapshot_payload(&dir, 0, 4, 11, "SnapshotAppendAlice");
    replica
        .install_raft_snapshot(crate::InstallSnapshotRequest {
            term: 5,
            leader_id: 9,
            metadata: crate::RaftSnapshotMetadata {
                shard_id: 0,
                last_included_term: 4,
                last_included_index: 11,
            },
            payload,
        })
        .unwrap();
    replica
        .apply_raft_append_entries(
            0,
            vec![LogEntry::new(
                0,
                5,
                12,
                Command::SetNodeProperty {
                    id: 1,
                    key: "city".to_string(),
                    value: Value::String("Seoul".to_string()),
                },
            )],
            12,
        )
        .unwrap();
    drop(replica);

    let reopened = Neo4rDatabaseHandle::open(
        DatabaseConfig::new(&dir, 1, 1)
            .with_server_id(1)
            .with_routing_table(routing_table)
            .with_raft_enabled(true),
    )
    .unwrap();
    let rows = reopened
        .query_with_options(
            r#"MATCH (n:Person) WHERE n.name = "SnapshotAppendAlice" RETURN n.city"#,
            QueryOptions::default().with_consistency(ReadConsistency::FollowerStale),
        )
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get("n.city"),
        Some(&QueryValue::Scalar(Value::String("Seoul".to_string())))
    );
    assert_eq!(reopened.read_snapshot().unwrap().committed_indexes(), &[12]);

    let _ = fs::remove_dir_all(dir);
}

#[test]
pub(super) fn tcp_raft_append_falls_back_to_install_snapshot_on_rejection() {
    let dir = temp_dir("facade-tcp-append-fallback-snapshot");
    let routing_table = ShardRoutingTable {
        version: 1,
        placements: vec![ShardPlacement::new(0, vec![ShardReplica::primary(1)])],
    };
    let replica = Neo4rDatabaseHandle::open(
        DatabaseConfig::new(&dir, 1, 1)
            .with_server_id(1)
            .with_routing_table(routing_table)
            .with_raft_enabled(true),
    )
    .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let replica_for_listener = replica.clone();
    let server = thread::spawn(move || {
        for _ in 0..2 {
            let (mut stream, _) = listener.accept().unwrap();
            let _ = crate::handle_tcp_replication_stream(&replica_for_listener, &mut stream);
        }
    });

    let bad_append_entry = LogEntry::new(
        1,
        5,
        12,
        Command::CreateNode {
            id: 99,
            labels: vec!["Person".to_string()],
            properties: properties(&[("name", Value::String("BadShard".to_string()))]),
        },
    );
    let payload = snapshot_payload(&dir, 0, 4, 11, "FallbackSnapshotAlice");
    let acks = crate::request_tcp_raft_append_or_install_snapshot(
        &address,
        Duration::from_secs(1),
        0,
        12,
        &[bad_append_entry],
        crate::InstallSnapshotRequest {
            term: 5,
            leader_id: 9,
            metadata: crate::RaftSnapshotMetadata {
                shard_id: 0,
                last_included_term: 4,
                last_included_index: 11,
            },
            payload,
        },
    )
    .unwrap();

    assert_eq!(acks, vec![(0, 11)]);
    assert_eq!(
        replica
            .query_with_options(
                r#"MATCH (n:Person) WHERE n.name = "FallbackSnapshotAlice" RETURN n.name"#,
                QueryOptions::default().with_consistency(ReadConsistency::FollowerStale),
            )
            .unwrap()
            .len(),
        1
    );

    server.join().unwrap();
    drop(replica);
    let _ = fs::remove_dir_all(dir);
}

#[test]
pub(super) fn tcp_raft_replication_pump_sends_heartbeat_to_registered_peer() {
    let primary_dir = temp_dir("facade-tcp-raft-pump-primary");
    let replica_dir = temp_dir("facade-tcp-raft-pump-replica");
    let routing_table = ShardRoutingTable {
        version: 1,
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
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let replica_for_listener = replica.clone();
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        crate::handle_tcp_replication_stream(&replica_for_listener, &mut stream).unwrap();
    });

    let replicator =
        crate::TcpShardReplicator::new(routing_table.clone()).with_raft_transport(true);
    replicator.register_peer(2, address).unwrap();
    let primary = Neo4rDatabaseHandle::open(
        DatabaseConfig::new(&primary_dir, 1, 1)
            .with_server_id(1)
            .with_routing_table(routing_table)
            .with_raft_enabled(true),
    )
    .unwrap();

    assert_eq!(replicator.run_raft_replication_pump(&primary).unwrap(), 1);

    server.join().unwrap();
    drop(primary);
    drop(replica);
    let _ = fs::remove_dir_all(primary_dir);
    let _ = fs::remove_dir_all(replica_dir);
}

#[test]
pub(super) fn tcp_raft_replication_pump_catches_up_replica_with_committed_entries() {
    let primary_dir = temp_dir("facade-tcp-raft-pump-entry-primary");
    let replica_dir = temp_dir("facade-tcp-raft-pump-entry-replica");
    let primary_routing_table = ShardRoutingTable {
        version: 1,
        placements: vec![ShardPlacement::new(0, vec![ShardReplica::primary(1)])],
    };
    let replica_routing_table = ShardRoutingTable {
        version: 1,
        placements: vec![ShardPlacement::new(
            0,
            vec![ShardReplica::primary(1), ShardReplica::replica(2)],
        )],
    };
    let primary = Neo4rDatabaseHandle::open(
        DatabaseConfig::new(&primary_dir, 1, 1)
            .with_server_id(1)
            .with_routing_table(primary_routing_table)
            .with_raft_enabled(true),
    )
    .unwrap();
    primary
        .create_node(
            vec!["Person".to_string()],
            properties(&[("name", Value::String("PumpCatchUpAlice".to_string()))]),
        )
        .unwrap();
    let replica = Neo4rDatabaseHandle::open(
        DatabaseConfig::new(&replica_dir, 1, 1)
            .with_server_id(2)
            .with_routing_table(replica_routing_table.clone())
            .with_raft_enabled(true),
    )
    .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let replica_for_listener = replica.clone();
    let server = thread::spawn(move || {
        for _ in 0..2 {
            let (mut stream, _) = listener.accept().unwrap();
            let _ = crate::handle_tcp_replication_stream(&replica_for_listener, &mut stream);
        }
    });

    let replicator =
        crate::TcpShardReplicator::new(replica_routing_table).with_raft_transport(true);
    replicator.register_peer(2, address).unwrap();

    assert_eq!(replicator.run_raft_replication_pump(&primary).unwrap(), 1);
    assert_eq!(
        replica
            .query_with_options(
                r#"MATCH (n:Person) WHERE n.name = "PumpCatchUpAlice" RETURN n.name"#,
                QueryOptions::default().with_consistency(ReadConsistency::FollowerStale),
            )
            .unwrap()
            .len(),
        1
    );

    server.join().unwrap();
    drop(primary);
    drop(replica);
    let _ = fs::remove_dir_all(primary_dir);
    let _ = fs::remove_dir_all(replica_dir);
}

#[test]
pub(super) fn tcp_replicator_applies_primary_writes_to_replica() {
    let primary_dir = temp_dir("facade-tcp-primary");
    let replica_dir = temp_dir("facade-tcp-replica");
    let routing_table = ShardRoutingTable {
        version: 3,
        placements: vec![ShardPlacement::new(
            0,
            vec![ShardReplica::primary(1), ShardReplica::replica(2)],
        )],
    };
    let replica = Neo4rDatabaseHandle::open(
        DatabaseConfig::new(&replica_dir, 1, 2)
            .with_server_id(2)
            .with_routing_table(routing_table.clone()),
    )
    .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let replica_for_listener = replica.clone();
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        crate::handle_tcp_replication_stream(&replica_for_listener, &mut stream).unwrap();
    });

    let replicator = Arc::new(crate::TcpShardReplicator::new(routing_table.clone()));
    replicator.register_peer(2, address).unwrap();
    let primary = Neo4rDatabaseHandle::open_with_replicator(
        DatabaseConfig::new(&primary_dir, 1, 2)
            .with_server_id(1)
            .with_routing_table(routing_table),
        replicator,
    )
    .unwrap();

    primary
        .create_node(
            vec!["Person".to_string()],
            properties(&[("name", Value::String("Alice".to_string()))]),
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

    let _ = fs::remove_dir_all(primary_dir);
    let _ = fs::remove_dir_all(replica_dir);
}

#[test]
pub(super) fn tcp_replicator_batches_group_commit_to_replica() {
    let primary_dir = temp_dir("facade-tcp-batch-primary");
    let replica_dir = temp_dir("facade-tcp-batch-replica");
    let write_count = 8;
    let routing_table = ShardRoutingTable {
        version: 3,
        placements: vec![ShardPlacement::new(
            0,
            vec![ShardReplica::primary(1), ShardReplica::replica(2)],
        )],
    };
    let replica = Neo4rDatabaseHandle::open(
        DatabaseConfig::new(&replica_dir, 1, 2)
            .with_server_id(2)
            .with_routing_table(routing_table.clone()),
    )
    .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let replica_for_listener = replica.clone();
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        crate::handle_tcp_replication_stream(&replica_for_listener, &mut stream).unwrap();
    });

    let replicator = Arc::new(crate::TcpShardReplicator::new(routing_table.clone()));
    replicator.register_peer(2, address).unwrap();
    let primary = Neo4rDatabaseHandle::open_with_replicator(
        DatabaseConfig::new(&primary_dir, 1, 2)
            .with_server_id(1)
            .with_routing_table(routing_table)
            .with_group_commit_max_entries(write_count)
            .with_group_commit_max_delay(Duration::from_millis(20)),
        replicator,
    )
    .unwrap();
    let barrier = Arc::new(Barrier::new(write_count));

    let mut workers = Vec::new();
    for worker_id in 0..write_count {
        let primary = primary.clone();
        let barrier = barrier.clone();
        workers.push(thread::spawn(move || {
            barrier.wait();
            primary
                .create_node(
                    vec!["Person".to_string()],
                    properties(&[("worker", Value::Int(worker_id as i64))]),
                )
                .unwrap();
        }));
    }
    for worker in workers {
        worker.join().unwrap();
    }
    server.join().unwrap();

    assert_eq!(
        replica.query("MATCH (n:Person) RETURN n").unwrap().len(),
        write_count
    );
    assert_eq!(
        replica.read_snapshot().unwrap().applied_indexes(),
        &[write_count as u64]
    );

    drop(primary);
    drop(replica);
    let _ = fs::remove_dir_all(primary_dir);
    let _ = fs::remove_dir_all(replica_dir);
}

#[test]
pub(super) fn tcp_replicator_retries_until_replica_listener_is_available() {
    let primary_dir = temp_dir("facade-tcp-retry-primary");
    let replica_dir = temp_dir("facade-tcp-retry-replica");
    let routing_table = ShardRoutingTable {
        version: 3,
        placements: vec![ShardPlacement::new(
            0,
            vec![ShardReplica::primary(1), ShardReplica::replica(2)],
        )],
    };
    let replica = Neo4rDatabaseHandle::open(
        DatabaseConfig::new(&replica_dir, 1, 2)
            .with_server_id(2)
            .with_routing_table(routing_table.clone()),
    )
    .unwrap();
    let reserved = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = reserved.local_addr().unwrap();
    drop(reserved);

    let replica_for_listener = replica.clone();
    let server = thread::spawn(move || {
        thread::sleep(Duration::from_millis(30));
        let listener = TcpListener::bind(address).unwrap();
        let (mut stream, _) = listener.accept().unwrap();
        crate::handle_tcp_replication_stream(&replica_for_listener, &mut stream).unwrap();
    });

    let replicator = Arc::new(
        crate::TcpShardReplicator::new(routing_table.clone())
            .with_connect_timeout(Duration::from_millis(10))
            .with_retry(10, Duration::from_millis(10)),
    );
    replicator.register_peer(2, address.to_string()).unwrap();
    let primary = Neo4rDatabaseHandle::open_with_replicator(
        DatabaseConfig::new(&primary_dir, 1, 2)
            .with_server_id(1)
            .with_routing_table(routing_table),
        replicator,
    )
    .unwrap();

    primary
        .create_node(
            vec!["Person".to_string()],
            properties(&[("name", Value::String("Alice".to_string()))]),
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
pub(super) fn tcp_replicator_quorum_succeeds_with_one_missing_replica() {
    let primary_dir = temp_dir("facade-tcp-quorum-primary");
    let replica_dir = temp_dir("facade-tcp-quorum-replica");
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
        DatabaseConfig::new(&replica_dir, 1, 2)
            .with_server_id(2)
            .with_routing_table(routing_table.clone()),
    )
    .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let replica_for_listener = replica.clone();
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        crate::handle_tcp_replication_stream(&replica_for_listener, &mut stream).unwrap();
    });

    let replicator = Arc::new(
        crate::TcpShardReplicator::new(routing_table.clone())
            .with_ack_policy(crate::ReplicationAckPolicy::Quorum)
            .with_connect_timeout(Duration::from_millis(10)),
    );
    replicator.register_peer(2, address).unwrap();
    let primary = Neo4rDatabaseHandle::open_with_replicator(
        DatabaseConfig::new(&primary_dir, 1, 2)
            .with_server_id(1)
            .with_routing_table(routing_table),
        replicator,
    )
    .unwrap();

    primary
        .create_node(
            vec!["Person".to_string()],
            properties(&[("name", Value::String("Alice".to_string()))]),
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
pub(super) fn tcp_replicator_all_policy_fails_with_one_missing_replica() {
    let primary_dir = temp_dir("facade-tcp-all-fail-primary");
    let replica_dir = temp_dir("facade-tcp-all-fail-replica");
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
        DatabaseConfig::new(&replica_dir, 1, 2)
            .with_server_id(2)
            .with_routing_table(routing_table.clone()),
    )
    .unwrap();

    let replicator = Arc::new(
        crate::TcpShardReplicator::new(routing_table.clone())
            .with_ack_policy(crate::ReplicationAckPolicy::All)
            .with_connect_timeout(Duration::from_millis(10)),
    );
    let primary = Neo4rDatabaseHandle::open_with_replicator(
        DatabaseConfig::new(&primary_dir, 1, 2)
            .with_server_id(1)
            .with_routing_table(routing_table),
        replicator,
    )
    .unwrap();

    let err = primary
        .create_node(
            vec!["Person".to_string()],
            properties(&[("name", Value::String("Alice".to_string()))]),
        )
        .unwrap_err();

    assert!(matches!(err, DatabaseError::Replication(_)));
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
pub(super) fn tcp_catch_up_fetches_missing_entries_from_primary_log() {
    let primary_dir = temp_dir("facade-tcp-catchup-primary");
    let replica_dir = temp_dir("facade-tcp-catchup-replica");
    let routing_table = ShardRoutingTable {
        version: 3,
        placements: vec![ShardPlacement::new(
            0,
            vec![ShardReplica::primary(1), ShardReplica::replica(2)],
        )],
    };
    let primary = Neo4rDatabaseHandle::open_with_replicator(
        DatabaseConfig::new(&primary_dir, 1, 2)
            .with_server_id(1)
            .with_routing_table(routing_table.clone()),
        Arc::new(crate::NoopShardReplicator),
    )
    .unwrap();
    primary
        .create_node(
            vec!["Person".to_string()],
            properties(&[("name", Value::String("Alice".to_string()))]),
        )
        .unwrap();
    primary
        .create_node(
            vec!["Person".to_string()],
            properties(&[("name", Value::String("Bob".to_string()))]),
        )
        .unwrap();

    let replica = Neo4rDatabaseHandle::open(
        DatabaseConfig::new(&replica_dir, 1, 2)
            .with_server_id(2)
            .with_routing_table(routing_table),
    )
    .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let primary_for_listener = primary.clone();
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        crate::handle_tcp_replication_stream(&primary_for_listener, &mut stream).unwrap();
    });

    let applied =
        crate::catch_up_from_tcp_primary(&replica, &address, Duration::from_secs(1), 0, 1).unwrap();

    server.join().unwrap();
    assert_eq!(applied, 2);
    assert_eq!(replica.query("MATCH (n:Person) RETURN n").unwrap().len(), 2);
    assert_eq!(replica.read_snapshot().unwrap().applied_indexes(), &[2]);

    drop(primary);
    drop(replica);
    let _ = fs::remove_dir_all(primary_dir);
    let _ = fs::remove_dir_all(replica_dir);
}

#[test]
pub(super) fn tcp_catch_up_can_fetch_missing_entries_in_limited_batches() {
    let primary_dir = temp_dir("facade-tcp-catchup-batched-primary");
    let replica_dir = temp_dir("facade-tcp-catchup-batched-replica");
    let routing_table = ShardRoutingTable {
        version: 3,
        placements: vec![ShardPlacement::new(
            0,
            vec![ShardReplica::primary(1), ShardReplica::replica(2)],
        )],
    };
    let primary = Neo4rDatabaseHandle::open_with_replicator(
        DatabaseConfig::new(&primary_dir, 1, 2)
            .with_server_id(1)
            .with_routing_table(routing_table.clone()),
        Arc::new(crate::NoopShardReplicator),
    )
    .unwrap();
    for name in ["Alice", "Bob", "Carol", "Dave", "Eve"] {
        primary
            .create_node(
                vec!["Person".to_string()],
                properties(&[("name", Value::String(name.to_string()))]),
            )
            .unwrap();
    }

    let replica = Neo4rDatabaseHandle::open(
        DatabaseConfig::new(&replica_dir, 1, 2)
            .with_server_id(2)
            .with_routing_table(routing_table),
    )
    .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let primary_for_listener = primary.clone();
    let server = thread::spawn(move || {
        for _ in 0..3 {
            let (mut stream, _) = listener.accept().unwrap();
            crate::handle_tcp_replication_stream(&primary_for_listener, &mut stream).unwrap();
        }
    });

    let applied = crate::catch_up_from_tcp_primary_batched(
        &replica,
        &address,
        Duration::from_secs(1),
        0,
        1,
        2,
    )
    .unwrap();

    server.join().unwrap();
    assert_eq!(applied, 5);
    assert_eq!(replica.query("MATCH (n:Person) RETURN n").unwrap().len(), 5);
    assert_eq!(replica.read_snapshot().unwrap().applied_indexes(), &[5]);

    drop(primary);
    drop(replica);
    let _ = fs::remove_dir_all(primary_dir);
    let _ = fs::remove_dir_all(replica_dir);
}

#[test]
pub(super) fn tcp_catch_up_from_primaries_uses_local_committed_positions() {
    let primary_dir = temp_dir("facade-tcp-auto-catchup-primary");
    let replica_dir = temp_dir("facade-tcp-auto-catchup-replica");
    let routing_table = ShardRoutingTable {
        version: 3,
        placements: vec![ShardPlacement::new(
            0,
            vec![ShardReplica::primary(1), ShardReplica::replica(2)],
        )],
    };
    let primary = Neo4rDatabaseHandle::open_with_replicator(
        DatabaseConfig::new(&primary_dir, 1, 2)
            .with_server_id(1)
            .with_routing_table(routing_table.clone()),
        Arc::new(crate::NoopShardReplicator),
    )
    .unwrap();
    primary
        .create_node(
            vec!["Person".to_string()],
            properties(&[("name", Value::String("Alice".to_string()))]),
        )
        .unwrap();
    primary
        .create_node(
            vec!["Person".to_string()],
            properties(&[("name", Value::String("Bob".to_string()))]),
        )
        .unwrap();

    let replica = Neo4rDatabaseHandle::open(
        DatabaseConfig::new(&replica_dir, 1, 2)
            .with_server_id(2)
            .with_routing_table(routing_table.clone()),
    )
    .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let primary_for_listener = primary.clone();
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        crate::handle_tcp_replication_stream(&primary_for_listener, &mut stream).unwrap();
    });
    let peer_addresses = BTreeMap::from([(1, address)]);

    let results = crate::catch_up_from_tcp_primaries(
        &replica,
        &routing_table,
        &peer_addresses,
        2,
        Duration::from_secs(1),
    )
    .unwrap();

    server.join().unwrap();
    assert_eq!(
        results,
        vec![crate::TcpCatchUpResult {
            shard_id: 0,
            start_index: 1,
            end_index: 2,
            fetched_entries: 2,
            primary_server_id: 1,
        }]
    );
    assert_eq!(replica.query("MATCH (n:Person) RETURN n").unwrap().len(), 2);
    assert_eq!(replica.committed_indexes().unwrap(), vec![2]);

    drop(primary);
    drop(replica);
    let _ = fs::remove_dir_all(primary_dir);
    let _ = fs::remove_dir_all(replica_dir);
}
