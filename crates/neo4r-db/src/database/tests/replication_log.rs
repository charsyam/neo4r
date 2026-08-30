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
pub(super) fn local_write_entries_include_origin_and_config_metadata() {
    let dir = temp_dir("facade-local-entry-metadata");
    let config = DatabaseConfig::new(&dir, 1, 2)
        .with_server_id(10)
        .with_log_entries_per_segment(16);
    let mut db = Neo4rDatabase::open(config).unwrap();

    db.create_node(vec!["Person".to_string()], Properties::new())
        .unwrap();
    let entries = db.log(0).unwrap().replay().unwrap();

    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].origin_server_id, 10);
    assert_eq!(entries[0].config_version, 1);
    assert!(entries[0].timestamp > HybridTimestamp::zero());

    let _ = fs::remove_dir_all(dir);
}

#[test]
pub(super) fn pitr_timestamp_target_selects_committed_entries_at_or_before_target() {
    let dir = temp_dir("facade-pitr-timestamp-target");
    let log = neo4r_storage::SegmentedShardLog::open(&dir, 0, 4).unwrap();
    for (index, timestamp) in [
        (1, HybridTimestamp::new(1_000, 1)),
        (2, HybridTimestamp::new(2_000, 1)),
        (3, HybridTimestamp::new(3_000, 1)),
    ] {
        log.append(&LogEntry::new_with_metadata(
            0,
            1,
            index,
            7,
            9,
            timestamp,
            Command::CreateNode {
                id: index,
                labels: vec!["Pitr".to_string()],
                properties: Properties::new(),
            },
        ))
        .unwrap();
    }
    neo4r_storage::CommitStore::open(&dir, 0)
        .unwrap()
        .save(1, 2)
        .unwrap();

    let target = HybridTimestamp::new(2_500, 1);
    let selected = log
        .replay()
        .unwrap()
        .into_iter()
        .filter(|entry| entry.index <= 2 && entry.timestamp <= target)
        .collect::<Vec<_>>();

    assert_eq!(selected.len(), 2);
    assert_eq!(selected.last().unwrap().index, 2);
    assert!(selected.iter().all(|entry| entry.timestamp <= target));

    let _ = fs::remove_dir_all(dir);
}

#[test]
pub(super) fn pitr_restore_to_timestamp_rewrites_state_and_truncates_wal_suffix() {
    let dir = temp_dir("facade-pitr-restore-to-timestamp");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1)).unwrap();
    db.execute_cypher(r#"CREATE (n:Pitr {name: "before"})"#)
        .unwrap();
    let first_timestamp = db.log_entries_from(0, 1).unwrap()[0].timestamp;
    db.execute_cypher(r#"CREATE (n:Pitr {name: "after"})"#)
        .unwrap();
    assert_eq!(
        db.query_with_options(
            r#"MATCH (n:Pitr) RETURN n.name"#,
            QueryOptions::default().with_consistency(ReadConsistency::FollowerStale),
        )
        .unwrap()
        .len(),
        2
    );

    let result = db.restore_to_timestamp(first_timestamp).unwrap();

    assert_eq!(result.action, "restore_pitr");
    assert_eq!(result.pruned_until, vec![1]);
    assert!(result.safety_manifest.contains("wal_suffix_truncated=true"));
    assert_eq!(db.committed_indexes().unwrap(), vec![1]);
    assert_eq!(db.log_entries_from(0, 1).unwrap().len(), 1);
    assert_eq!(
        db.query_with_options(
            r#"MATCH (n:Pitr) WHERE n.name = "before" RETURN n.name"#,
            QueryOptions::default().with_consistency(ReadConsistency::FollowerStale),
        )
        .unwrap()
        .len(),
        1
    );
    assert_eq!(
        db.query_with_options(
            r#"MATCH (n:Pitr) WHERE n.name = "after" RETURN n.name"#,
            QueryOptions::default().with_consistency(ReadConsistency::FollowerStale),
        )
        .unwrap()
        .len(),
        0
    );
    let _ = fs::remove_dir_all(dir);
}

#[test]
pub(super) fn replicated_entry_is_applied_without_being_local_primary() {
    let dir = temp_dir("facade-replicated-apply");
    let routing_table = ShardRoutingTable {
        version: 3,
        placements: vec![ShardPlacement::new(
            0,
            vec![ShardReplica::primary(1), ShardReplica::replica(2)],
        )],
    };
    let mut db = Neo4rDatabase::open(
        DatabaseConfig::new(&dir, 1, 2)
            .with_server_id(2)
            .with_routing_table(routing_table),
    )
    .unwrap();
    let entry = LogEntry::new_with_metadata(
        0,
        7,
        1,
        1,
        3,
        HybridTimestamp::new(1234, 1),
        Command::CreateNode {
            id: 42,
            labels: vec!["Person".to_string()],
            properties: properties(&[("name", Value::String("Alice".to_string()))]),
        },
    );

    db.apply_replicated_entry(entry).unwrap();

    assert_eq!(
        db.query(r#"MATCH (n:Person) WHERE n.name = "Alice" RETURN n.name"#)
            .unwrap()
            .len(),
        1
    );
    assert_eq!(db.read_snapshot().unwrap().applied_indexes(), &[1]);

    let _ = fs::remove_dir_all(dir);
}

#[test]
pub(super) fn raft_append_commit_applies_only_the_target_shard() {
    let dir = temp_dir("facade-raft-append-shard-local-commit");
    let routing_table = ShardRoutingTable {
        version: 3,
        placements: vec![
            ShardPlacement::new(0, vec![ShardReplica::primary(1), ShardReplica::replica(2)]),
            ShardPlacement::new(1, vec![ShardReplica::primary(1), ShardReplica::replica(2)]),
        ],
    };
    let mut db = Neo4rDatabase::open(
        DatabaseConfig::new(&dir, 2, 2)
            .with_server_id(2)
            .with_routing_table(routing_table)
            .with_raft_enabled(true),
    )
    .unwrap();
    db.apply_replicated_entry(LogEntry::new_with_metadata(
        1,
        7,
        1,
        1,
        3,
        HybridTimestamp::new(1234, 1),
        Command::CreateNode {
            id: 101,
            labels: vec!["OtherShard".to_string()],
            properties: Properties::new(),
        },
    ))
    .unwrap();
    assert_eq!(db.committed_indexes(), vec![0, 1]);

    let response = db
        .apply_raft_append_entries_with_response(
            0,
            vec![LogEntry::new_with_metadata(
                0,
                7,
                1,
                1,
                3,
                HybridTimestamp::new(1234, 2),
                Command::CreateNode {
                    id: 42,
                    labels: vec!["TargetShard".to_string()],
                    properties: Properties::new(),
                },
            )],
            1,
        )
        .unwrap();

    assert!(response.success);
    assert!(response.durable);
    assert_eq!(db.committed_indexes(), vec![1, 1]);
    assert_eq!(db.read_snapshot().unwrap().applied_indexes(), &[1, 1]);

    let _ = fs::remove_dir_all(dir);
}

#[test]
pub(super) fn raft_append_requires_leader_and_config_authority_stamps() {
    let dir = temp_dir("facade-raft-append-authority-stamps");
    let routing_table = ShardRoutingTable {
        version: 3,
        placements: vec![ShardPlacement::new(
            0,
            vec![ShardReplica::primary(1), ShardReplica::replica(2)],
        )],
    };
    let mut db = Neo4rDatabase::open(
        DatabaseConfig::new(&dir, 1, 2)
            .with_server_id(2)
            .with_routing_table(routing_table)
            .with_raft_enabled(true),
    )
    .unwrap();

    let missing_config = db
        .apply_raft_append_entries_with_response(
            0,
            vec![LogEntry::new_with_metadata(
                0,
                7,
                1,
                1,
                0,
                HybridTimestamp::new(1234, 1),
                Command::CreateNode {
                    id: 42,
                    labels: vec!["TargetShard".to_string()],
                    properties: Properties::new(),
                },
            )],
            1,
        )
        .unwrap_err();
    assert!(missing_config
        .to_string()
        .contains("missing config authority stamp"));

    let missing_leader = db
        .apply_raft_append_entries_with_response(
            0,
            vec![LogEntry::new_with_metadata(
                0,
                7,
                1,
                0,
                3,
                HybridTimestamp::new(1234, 1),
                Command::CreateNode {
                    id: 42,
                    labels: vec!["TargetShard".to_string()],
                    properties: Properties::new(),
                },
            )],
            1,
        )
        .unwrap_err();
    assert!(missing_leader
        .to_string()
        .contains("missing leader authority stamp"));
    assert_eq!(db.committed_indexes(), vec![0]);
    assert!(db.log(0).unwrap().entry(1).unwrap().is_none());

    let _ = fs::remove_dir_all(dir);
}

#[test]
pub(super) fn replicated_vector_indexed_write_is_rejected_before_wal_append() {
    let dir = temp_dir("facade-replicated-vector-validation");
    let routing_table = ShardRoutingTable {
        version: 3,
        placements: vec![ShardPlacement::new(
            0,
            vec![ShardReplica::primary(1), ShardReplica::replica(2)],
        )],
    };
    let mut db = Neo4rDatabase::open(
        DatabaseConfig::new(&dir, 1, 2)
            .with_server_id(2)
            .with_routing_table(routing_table),
    )
    .unwrap();
    db.create_vector_index("doc_embedding", "Document", "embedding", 2, "cosine")
        .unwrap();

    let err = db
        .apply_replicated_entry(LogEntry::new_with_metadata(
            0,
            7,
            1,
            1,
            3,
            HybridTimestamp::new(1234, 1),
            Command::CreateNode {
                id: 42,
                labels: vec!["Document".to_string()],
                properties: properties(&[("embedding", Value::Vector(vec![1.0]))]),
            },
        ))
        .unwrap_err();

    assert!(matches!(err, DatabaseError::InvalidConfig(_)));
    assert!(db.log(0).unwrap().entry(1).unwrap().is_none());
    assert!(db.query("MATCH (n:Document) RETURN n").unwrap().is_empty());
    assert_eq!(db.committed_indexes(), vec![0]);

    let _ = fs::remove_dir_all(dir);
}

#[test]
pub(super) fn replicated_map_property_write_is_rejected_before_wal_append() {
    let dir = temp_dir("facade-replicated-map-property-validation");
    let routing_table = ShardRoutingTable {
        version: 3,
        placements: vec![ShardPlacement::new(
            0,
            vec![ShardReplica::primary(1), ShardReplica::replica(2)],
        )],
    };
    let mut db = Neo4rDatabase::open(
        DatabaseConfig::new(&dir, 1, 2)
            .with_server_id(2)
            .with_routing_table(routing_table),
    )
    .unwrap();

    let err = db
        .apply_replicated_entry(LogEntry::new_with_metadata(
            0,
            7,
            1,
            1,
            3,
            HybridTimestamp::new(1234, 1),
            Command::CreateNode {
                id: 42,
                labels: vec!["Person".to_string()],
                properties: properties(&[(
                    "profile",
                    Value::Map(properties(&[("nested", Value::Bool(true))])),
                )]),
            },
        ))
        .unwrap_err();

    assert!(matches!(err, DatabaseError::InvalidConfig(_)));
    assert!(db.log(0).unwrap().entry(1).unwrap().is_none());
    assert!(db.query("MATCH (n:Person) RETURN n").unwrap().is_empty());
    assert_eq!(db.committed_indexes(), vec![0]);

    let _ = fs::remove_dir_all(dir);
}

#[test]
pub(super) fn replicated_vector_index_validation_uses_batch_overlay() {
    let dir = temp_dir("facade-replicated-vector-batch-validation");
    let routing_table = ShardRoutingTable {
        version: 3,
        placements: vec![ShardPlacement::new(
            0,
            vec![ShardReplica::primary(1), ShardReplica::replica(2)],
        )],
    };
    let mut db = Neo4rDatabase::open(
        DatabaseConfig::new(&dir, 1, 2)
            .with_server_id(2)
            .with_routing_table(routing_table),
    )
    .unwrap();
    db.create_vector_index("doc_embedding", "Document", "embedding", 2, "cosine")
        .unwrap();

    let err = db
        .apply_replicated_entries(vec![
            LogEntry::new_with_metadata(
                0,
                7,
                1,
                1,
                3,
                HybridTimestamp::new(1234, 1),
                Command::CreateNode {
                    id: 42,
                    labels: vec!["Document".to_string()],
                    properties: Properties::new(),
                },
            ),
            LogEntry::new_with_metadata(
                0,
                7,
                2,
                1,
                3,
                HybridTimestamp::new(1234, 2),
                Command::SetNodeProperty {
                    id: 42,
                    key: "embedding".to_string(),
                    value: Value::Vector(vec![1.0]),
                },
            ),
        ])
        .unwrap_err();

    assert!(matches!(err, DatabaseError::InvalidConfig(_)));
    assert!(db.log(0).unwrap().entry(1).unwrap().is_none());
    assert!(db.log(0).unwrap().entry(2).unwrap().is_none());
    assert!(db.query("MATCH (n:Document) RETURN n").unwrap().is_empty());
    assert_eq!(db.committed_indexes(), vec![0]);

    let _ = fs::remove_dir_all(dir);
}

#[test]
pub(super) fn replicated_duplicate_with_different_payload_is_rejected() {
    let dir = temp_dir("facade-replicated-conflict");
    let routing_table = ShardRoutingTable {
        version: 3,
        placements: vec![ShardPlacement::new(
            0,
            vec![ShardReplica::primary(1), ShardReplica::replica(2)],
        )],
    };
    let mut db = Neo4rDatabase::open(
        DatabaseConfig::new(&dir, 1, 2)
            .with_server_id(2)
            .with_routing_table(routing_table),
    )
    .unwrap();
    db.apply_replicated_entry(LogEntry::new_with_metadata(
        0,
        7,
        1,
        1,
        3,
        HybridTimestamp::new(1234, 1),
        Command::CreateNode {
            id: 42,
            labels: vec!["Person".to_string()],
            properties: Properties::new(),
        },
    ))
    .unwrap();

    let err = db
        .apply_replicated_entry(LogEntry::new_with_metadata(
            0,
            7,
            1,
            1,
            3,
            HybridTimestamp::new(1234, 1),
            Command::CreateNode {
                id: 43,
                labels: vec!["Person".to_string()],
                properties: Properties::new(),
            },
        ))
        .unwrap_err();

    assert!(matches!(err, DatabaseError::LogConflict { .. }));

    let _ = fs::remove_dir_all(dir);
}

#[test]
pub(super) fn replicated_entry_rejects_wrong_config_version() {
    let dir = temp_dir("facade-replicated-config-conflict");
    let routing_table = ShardRoutingTable {
        version: 3,
        placements: vec![ShardPlacement::new(
            0,
            vec![ShardReplica::primary(1), ShardReplica::replica(2)],
        )],
    };
    let mut db = Neo4rDatabase::open(
        DatabaseConfig::new(&dir, 1, 2)
            .with_server_id(2)
            .with_routing_table(routing_table),
    )
    .unwrap();

    let err = db
        .apply_replicated_entry(LogEntry::new_with_metadata(
            0,
            7,
            1,
            1,
            99,
            HybridTimestamp::new(1234, 1),
            Command::CreateNode {
                id: 42,
                labels: vec!["Person".to_string()],
                properties: Properties::new(),
            },
        ))
        .unwrap_err();

    assert!(matches!(err, DatabaseError::LogConflict { .. }));

    let _ = fs::remove_dir_all(dir);
}

#[test]
pub(super) fn local_write_publishes_log_entry_to_replicator() {
    let dir = temp_dir("facade-replicator-publish");
    let replicator = Arc::new(RecordingReplicator::default());
    let mut db = Neo4rDatabase::open_with_replicator(
        DatabaseConfig::new(&dir, 1, 2).with_server_id(10),
        replicator.clone(),
    )
    .unwrap();

    db.create_node(vec!["Person".to_string()], Properties::new())
        .unwrap();

    let published = replicator.entries();
    assert_eq!(published.len(), 1);
    assert_eq!(published[0].shard_id, 0);
    assert_eq!(published[0].index, 1);
    assert_eq!(published[0].origin_server_id, 10);

    let _ = fs::remove_dir_all(dir);
}

#[test]
pub(super) fn replicated_apply_does_not_publish_entry_again() {
    let dir = temp_dir("facade-replicator-no-loop");
    let replicator = Arc::new(RecordingReplicator::default());
    let routing_table = ShardRoutingTable {
        version: 3,
        placements: vec![ShardPlacement::new(
            0,
            vec![ShardReplica::primary(1), ShardReplica::replica(2)],
        )],
    };
    let mut db = Neo4rDatabase::open_with_replicator(
        DatabaseConfig::new(&dir, 1, 2)
            .with_server_id(2)
            .with_routing_table(routing_table),
        replicator.clone(),
    )
    .unwrap();

    db.apply_replicated_entry(LogEntry::new_with_metadata(
        0,
        7,
        1,
        1,
        3,
        HybridTimestamp::new(1234, 1),
        Command::CreateNode {
            id: 42,
            labels: vec!["Person".to_string()],
            properties: Properties::new(),
        },
    ))
    .unwrap();

    assert!(replicator.entries().is_empty());

    let _ = fs::remove_dir_all(dir);
}

#[test]
pub(super) fn in_process_replicator_applies_primary_writes_to_replica() {
    let primary_dir = temp_dir("facade-inprocess-primary");
    let replica_dir = temp_dir("facade-inprocess-replica");
    let routing_table = ShardRoutingTable {
        version: 3,
        placements: vec![ShardPlacement::new(
            0,
            vec![ShardReplica::primary(1), ShardReplica::replica(2)],
        )],
    };
    let replicator = Arc::new(crate::InProcessShardReplicator::new(routing_table.clone()));
    let primary = Neo4rDatabaseHandle::open_with_replicator(
        DatabaseConfig::new(&primary_dir, 1, 2)
            .with_server_id(1)
            .with_routing_table(routing_table.clone()),
        replicator.clone(),
    )
    .unwrap();
    let replica = Neo4rDatabaseHandle::open(
        DatabaseConfig::new(&replica_dir, 1, 2)
            .with_server_id(2)
            .with_routing_table(routing_table),
    )
    .unwrap();
    replicator.register_peer(2, replica.clone()).unwrap();

    primary
        .create_node(
            vec!["Person".to_string()],
            properties(&[("name", Value::String("Alice".to_string()))]),
        )
        .unwrap();

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
pub(super) fn install_routing_table_updates_replicator_targets() {
    let primary_dir = temp_dir("facade-install-routing-primary");
    let replica_dir = temp_dir("facade-install-routing-replica");
    let initial_routing_table = ShardRoutingTable {
        version: 3,
        placements: vec![ShardPlacement::new(
            0,
            vec![ShardReplica::primary(1), ShardReplica::replica(2)],
        )],
    };
    let updated_routing_table = ShardRoutingTable {
        version: 4,
        placements: vec![ShardPlacement::new(
            0,
            vec![ShardReplica::primary(1), ShardReplica::replica(3)],
        )],
    };
    let replicator = Arc::new(crate::InProcessShardReplicator::new(
        initial_routing_table.clone(),
    ));
    let primary = Neo4rDatabaseHandle::open_with_replicator(
        DatabaseConfig::new(&primary_dir, 1, 2)
            .with_server_id(1)
            .with_routing_table(initial_routing_table),
        replicator.clone(),
    )
    .unwrap();
    let replica = Neo4rDatabaseHandle::open(
        DatabaseConfig::new(&replica_dir, 1, 2)
            .with_server_id(3)
            .with_routing_table(updated_routing_table.clone()),
    )
    .unwrap();
    replicator.register_peer(3, replica.clone()).unwrap();

    primary
        .install_routing_table(updated_routing_table)
        .unwrap();
    primary
        .create_node(
            vec!["Person".to_string()],
            properties(&[("name", Value::String("Alice".to_string()))]),
        )
        .unwrap();

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
pub(super) fn raft_routing_install_records_config_change_entry() {
    let dir = temp_dir("facade-raft-config-change");
    let initial_routing_table = ShardRoutingTable {
        version: 3,
        placements: vec![ShardPlacement::new(0, vec![ShardReplica::primary(1)])],
    };
    let updated_routing_table = ShardRoutingTable {
        version: 4,
        placements: vec![ShardPlacement::new(0, vec![ShardReplica::primary(1)])],
    };
    let db = Neo4rDatabaseHandle::open(
        DatabaseConfig::new(&dir, 1, 1)
            .with_server_id(1)
            .with_routing_table(initial_routing_table)
            .with_raft_enabled(true),
    )
    .unwrap();

    db.install_routing_table(updated_routing_table).unwrap();
    let entries = db.log_entries_from(0, 1).unwrap();
    let phases = entries
        .iter()
        .filter_map(|entry| match &entry.command {
            Command::ClusterConfigChange {
                phase,
                description,
                voters,
                routing_table,
            } if description.contains("install_routing_table:version=4") => {
                assert_eq!(voters, &vec![1]);
                assert_eq!(routing_table.version, 4);
                Some(phase.as_str())
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(phases, vec!["enter_joint", "install", "finalize_joint"]);
    assert!(db
        .raft_status()
        .unwrap()
        .iter()
        .all(|status| !status.joint_consensus));

    drop(db);
    let _ = fs::remove_dir_all(dir);
}

#[test]
pub(super) fn raft_snapshot_install_advances_commit_checkpoint_and_recovers() {
    let dir = temp_dir("facade-raft-install-snapshot");
    let routing_table = ShardRoutingTable {
        version: 1,
        placements: vec![ShardPlacement::new(0, vec![ShardReplica::primary(1)])],
    };
    let db = Neo4rDatabaseHandle::open(
        DatabaseConfig::new(&dir, 1, 1)
            .with_server_id(1)
            .with_routing_table(routing_table.clone())
            .with_raft_enabled(true),
    )
    .unwrap();
    db.create_node(
        vec!["Person".to_string()],
        properties(&[("name", Value::String("StaleSnapshotNode".to_string()))]),
    )
    .unwrap();

    let payload = snapshot_payload(&dir, 0, 2, 7, "SnapshotAlice");
    let response = db
        .install_raft_snapshot(crate::InstallSnapshotRequest {
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
    assert!(response.success);
    assert_eq!(db.read_snapshot().unwrap().committed_indexes(), &[7]);
    assert_eq!(db.read_snapshot().unwrap().applied_indexes(), &[7]);
    assert_eq!(
        db.query_with_options(
            r#"MATCH (n:Person) WHERE n.name = "SnapshotAlice" RETURN n.name"#,
            QueryOptions::default().with_consistency(ReadConsistency::FollowerStale),
        )
        .unwrap()
        .len(),
        1
    );
    assert_eq!(
        db.query_with_options(
            r#"MATCH (n:Person) WHERE n.name = "StaleSnapshotNode" RETURN n.name"#,
            QueryOptions::default().with_consistency(ReadConsistency::FollowerStale),
        )
        .unwrap()
        .len(),
        0
    );

    drop(db);
    let reopened = Neo4rDatabaseHandle::open(
        DatabaseConfig::new(&dir, 1, 1)
            .with_server_id(1)
            .with_routing_table(routing_table)
            .with_raft_enabled(true),
    )
    .unwrap();
    assert_eq!(reopened.read_snapshot().unwrap().committed_indexes(), &[7]);
    assert_eq!(reopened.read_snapshot().unwrap().applied_indexes(), &[7]);

    drop(reopened);
    let _ = fs::remove_dir_all(dir);
}

#[test]
pub(super) fn raft_snapshot_fault_injection_persists_payload_before_metadata() {
    let dir = temp_dir("facade-raft-install-snapshot-fault");
    let routing_table = ShardRoutingTable {
        version: 1,
        placements: vec![ShardPlacement::new(0, vec![ShardReplica::primary(1)])],
    };
    let db = Neo4rDatabaseHandle::open(
        DatabaseConfig::new(&dir, 1, 1)
            .with_server_id(1)
            .with_routing_table(routing_table.clone())
            .with_raft_enabled(true)
            .with_failure_injection(FailureInjection {
                fail_after_snapshot_payload_save_before_metadata: true,
                ..FailureInjection::default()
            }),
    )
    .unwrap();
    let payload = snapshot_payload(&dir, 0, 4, 11, "FaultSnapshotAlice");
    let err = db
        .install_raft_snapshot(crate::InstallSnapshotRequest {
            term: 5,
            leader_id: 9,
            metadata: crate::RaftSnapshotMetadata {
                shard_id: 0,
                last_included_term: 4,
                last_included_index: 11,
            },
            payload: payload.clone(),
        })
        .unwrap_err();

    assert!(format!("{err}").contains("injected failure after snapshot payload save"));
    assert_eq!(
        neo4r_storage::SnapshotStore::open(&dir, 0)
            .unwrap()
            .load_payload()
            .unwrap(),
        Some(payload.clone())
    );
    assert_eq!(db.read_snapshot().unwrap().committed_indexes(), &[0]);

    drop(db);
    let retried = Neo4rDatabaseHandle::open(
        DatabaseConfig::new(&dir, 1, 1)
            .with_server_id(1)
            .with_routing_table(routing_table)
            .with_raft_enabled(true),
    )
    .unwrap();
    let response = retried
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
    assert!(response.success);
    assert_eq!(retried.read_snapshot().unwrap().committed_indexes(), &[11]);
    assert_eq!(
        retried
            .query_with_options(
                r#"MATCH (n:Person) WHERE n.name = "FaultSnapshotAlice" RETURN n.name"#,
                QueryOptions::default().with_consistency(ReadConsistency::FollowerStale),
            )
            .unwrap()
            .len(),
        1
    );

    drop(retried);
    let _ = fs::remove_dir_all(dir);
}

#[test]
pub(super) fn raft_snapshot_fault_injection_after_prune_leaves_missing_payload_apply_observable() {
    let dir = temp_dir("facade-raft-install-snapshot-prune-fault");
    let routing_table = ShardRoutingTable {
        version: 1,
        placements: vec![ShardPlacement::new(0, vec![ShardReplica::primary(1)])],
    };
    let db = Neo4rDatabaseHandle::open(
        DatabaseConfig::new(&dir, 1, 1)
            .with_server_id(1)
            .with_routing_table(routing_table)
            .with_raft_enabled(true)
            .with_failure_injection(FailureInjection {
                fail_after_snapshot_prune_before_apply: true,
                ..FailureInjection::default()
            }),
    )
    .unwrap();
    db.create_node(
        vec!["Person".to_string()],
        properties(&[("name", Value::String("PrunedBeforeApply".to_string()))]),
    )
    .unwrap();
    let payload = snapshot_payload(&dir, 0, 4, 11, "NotAppliedYet");
    let err = db
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
        .unwrap_err();

    assert!(format!("{err}").contains("injected failure after snapshot prune"));
    assert_eq!(
        db.query_with_options(
            r#"MATCH (n:Person) WHERE n.name = "PrunedBeforeApply" RETURN n.name"#,
            QueryOptions::default().with_consistency(ReadConsistency::FollowerStale),
        )
        .unwrap()
        .len(),
        0
    );

    drop(db);
    let _ = fs::remove_dir_all(dir);
}

#[test]
pub(super) fn raft_snapshot_now_generates_payload_and_compacts_local_raft_log() {
    let dir = temp_dir("facade-raft-snapshot-now");
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
    db.create_node(
        vec!["Person".to_string()],
        properties(&[("name", Value::String("SnapshotNowAlice".to_string()))]),
    )
    .unwrap();

    let result = db.snapshot_now().unwrap();
    assert_eq!(result.action, "snapshot");
    assert_eq!(result.pruned_until, vec![1]);
    assert!(result.bytes_observed > 0);
    assert_eq!(
        neo4r_storage::SnapshotStore::open(&dir, 0)
            .unwrap()
            .load()
            .unwrap()
            .unwrap()
            .last_included_index,
        1
    );
    assert_eq!(db.raft_status().unwrap()[0].snapshot_index, 1);

    drop(db);

    let reopened = Neo4rDatabaseHandle::open(
        DatabaseConfig::new(&dir, 1, 1)
            .with_server_id(1)
            .with_routing_table(ShardRoutingTable {
                version: 1,
                placements: vec![ShardPlacement::new(0, vec![ShardReplica::primary(1)])],
            })
            .with_raft_enabled(true),
    )
    .unwrap();
    assert_eq!(reopened.raft_status().unwrap()[0].snapshot_index, 1);
    drop(reopened);
    let _ = fs::remove_dir_all(dir);
}

#[test]
pub(super) fn restore_snapshot_replaces_shard_state_from_saved_payload() {
    let dir = temp_dir("facade-restore-snapshot");
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
    db.create_node(
        vec!["Person".to_string()],
        properties(&[("name", Value::String("RestoreAlice".to_string()))]),
    )
    .unwrap();
    db.snapshot_now().unwrap();
    db.create_node(
        vec!["Person".to_string()],
        properties(&[("name", Value::String("RestoreBob".to_string()))]),
    )
    .unwrap();

    let result = db.restore_snapshot(0).unwrap();
    assert_eq!(result.action, "restore_snapshot");
    assert_eq!(
        db.query_with_options(
            r#"MATCH (n:Person) WHERE n.name = "RestoreAlice" RETURN n.name"#,
            QueryOptions::default().with_consistency(ReadConsistency::FollowerStale),
        )
        .unwrap()
        .len(),
        1
    );
    assert_eq!(
        db.query_with_options(
            r#"MATCH (n:Person) WHERE n.name = "RestoreBob" RETURN n.name"#,
            QueryOptions::default().with_consistency(ReadConsistency::FollowerStale),
        )
        .unwrap()
        .len(),
        0
    );
    assert_eq!(db.raft_status().unwrap()[0].snapshot_index, 1);

    drop(db);
    let _ = fs::remove_dir_all(dir);
}

#[test]
pub(super) fn pending_restore_manifest_recovers_snapshot_replacement_on_reopen() {
    let dir = temp_dir("facade-pending-restore-recover");
    let routing_table = ShardRoutingTable {
        version: 1,
        placements: vec![ShardPlacement::new(0, vec![ShardReplica::primary(1)])],
    };
    {
        let db = Neo4rDatabaseHandle::open(
            DatabaseConfig::new(&dir, 1, 1)
                .with_server_id(1)
                .with_routing_table(routing_table.clone())
                .with_raft_enabled(true),
        )
        .unwrap();
        db.create_node(
            vec!["Person".to_string()],
            properties(&[("name", Value::String("PendingRestoreAlice".to_string()))]),
        )
        .unwrap();
        db.snapshot_now().unwrap();
    }
    {
        let db = Neo4rDatabaseHandle::open(
            DatabaseConfig::new(&dir, 1, 1)
                .with_server_id(1)
                .with_routing_table(routing_table.clone())
                .with_raft_enabled(true)
                .with_failure_injection(FailureInjection {
                    fail_after_snapshot_prune_before_apply: true,
                    ..FailureInjection::default()
                }),
        )
        .unwrap();
        db.create_node(
            vec!["Person".to_string()],
            properties(&[("name", Value::String("PendingRestoreBob".to_string()))]),
        )
        .unwrap();
        assert!(matches!(
            db.restore_snapshot(0),
            Err(DatabaseError::Replication(message))
                if message.contains("after snapshot prune before apply")
        ));
    }

    let recovered = Neo4rDatabaseHandle::open(
        DatabaseConfig::new(&dir, 1, 1)
            .with_server_id(1)
            .with_routing_table(routing_table)
            .with_raft_enabled(true),
    )
    .unwrap();
    assert_eq!(
        recovered
            .query_with_options(
                r#"MATCH (n:Person) WHERE n.name = "PendingRestoreAlice" RETURN n.name"#,
                QueryOptions::default().with_consistency(ReadConsistency::FollowerStale),
            )
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        recovered
            .query_with_options(
                r#"MATCH (n:Person) WHERE n.name = "PendingRestoreBob" RETURN n.name"#,
                QueryOptions::default().with_consistency(ReadConsistency::FollowerStale),
            )
            .unwrap()
            .len(),
        0
    );

    drop(recovered);
    let _ = fs::remove_dir_all(dir);
}
