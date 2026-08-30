#![allow(unused_imports)]
use super::*;
use crate::{InstallSnapshotRequest, RaftSnapshotMetadata, SnapshotChunkAssembler};
use neo4r_core::{
    Command, GraphState, HybridTimestamp, LogEntry, ShardPlacement, ShardReplica, Term, Value,
};
use neo4r_query::QueryValue;
use std::fs;
use std::net::TcpListener;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[test]
pub(super) fn cluster_rebalance_execution_advances_and_persists_status() {
    let dir = temp_dir("facade-rebalance-execution");
    {
        let db =
            Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1).with_server_id(1)).unwrap();
        db.register_cluster_node(2, "127.0.0.1:17688").unwrap();
        let execution = db.start_rebalance_plan().unwrap();
        assert_eq!(execution.state, RebalancePlanState::Running);
        assert_eq!(execution.steps.len(), 1);

        let prepared = db.advance_rebalance().unwrap();
        assert_eq!(prepared.action, "prepared");
        assert_eq!(
            prepared.execution.steps[0].state,
            RebalanceStepState::CatchingUp
        );

        let caught_up = db.advance_rebalance().unwrap();
        assert_eq!(caught_up.action, "caught_up");
        assert_eq!(
            caught_up.execution.steps[0].state,
            RebalanceStepState::Ready
        );
    }
    {
        let db =
            Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1).with_server_id(1)).unwrap();
        assert_eq!(
            db.rebalance_status().unwrap().unwrap().steps[0].state,
            RebalanceStepState::Ready
        );
        let applied = db.advance_rebalance().unwrap();
        assert_eq!(applied.action, "applied");
        assert!(db
            .routing_table()
            .unwrap()
            .placement(0)
            .unwrap()
            .has_server(2));
        let completed = db.advance_rebalance().unwrap();
        assert_eq!(completed.execution.state, RebalancePlanState::Completed);
    }

    let _ = fs::remove_dir_all(dir);
}

#[test]
pub(super) fn cluster_rebalance_reports_snapshot_bootstrap_before_catch_up() {
    let dir = temp_dir("facade-rebalance-snapshot-bootstrap");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1).with_server_id(1)).unwrap();
    db.create_node(
        vec!["Person".to_string()],
        properties(&[("name", Value::String("BootstrapAlice".to_string()))]),
    )
    .unwrap();
    db.register_cluster_node(2, "127.0.0.1:17688").unwrap();

    let prepared = db.advance_rebalance().unwrap();
    assert_eq!(prepared.action, "prepared");
    let waiting = db.advance_rebalance().unwrap();
    assert!(waiting.action.contains("snapshot_bootstrap_required"));
    assert!(waiting.action.contains("committed_index=1"));

    let _ = fs::remove_dir_all(dir);
}

#[test]
pub(super) fn cluster_metadata_authority_guards_metadata_mutations() {
    let dir = temp_dir("facade-metadata-authority");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1).with_server_id(1)).unwrap();

    let metadata = db.set_metadata_authority(2).unwrap();
    assert_eq!(metadata.authority_server_id, 2);
    let err = db.register_cluster_node(3, "127.0.0.1:17689").unwrap_err();
    assert!(err.to_string().contains("not metadata authority"));

    let _ = fs::remove_dir_all(dir);
}

#[test]
pub(super) fn cluster_rebalance_policy_limits_replica_additions() {
    let dir = temp_dir("facade-rebalance-policy");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 2, 1).with_server_id(1)).unwrap();
    db.set_rebalance_policy(RebalancePolicy {
        replication_factor: 1,
        max_steps_per_plan: 4,
    })
    .unwrap();
    db.register_cluster_node(2, "127.0.0.1:17688").unwrap();
    assert!(db.plan_rebalance().unwrap().steps.is_empty());

    db.set_rebalance_policy(RebalancePolicy {
        replication_factor: 2,
        max_steps_per_plan: 1,
    })
    .unwrap();
    let plan = db.plan_rebalance().unwrap();
    assert_eq!(plan.steps.len(), 1);

    let _ = fs::remove_dir_all(dir);
}

#[test]
pub(super) fn performance_profile_statistics_storage_and_read_cache_are_reported() {
    let dir = temp_dir("facade-performance-observability");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1).with_server_id(1)).unwrap();
    let alice = db
        .create_node(
            vec!["Person".to_string()],
            properties(&[("name", Value::String("Alice".to_string()))]),
        )
        .unwrap();
    let bob = db
        .create_node(vec!["Person".to_string()], Properties::new())
        .unwrap();
    db.create_relationship(alice, bob, "KNOWS".to_string(), Properties::new())
        .unwrap();

    db.node(alice).unwrap();
    db.node(alice).unwrap();
    let storage = db.storage_status().unwrap();
    assert!(storage.read_cache_hits >= 1);
    assert!(storage.read_cache_misses >= 1);

    let statistics = db.statistics_catalog().unwrap();
    assert_eq!(statistics.node_count, 2);
    assert_eq!(statistics.relationship_count, 1);
    assert!(statistics
        .label_counts
        .iter()
        .any(|(label, count)| label == "Person" && *count == 2));
    assert!(statistics
        .node_property_counts
        .iter()
        .any(|(property, count)| property == "name" && *count == 1));
    assert!(statistics
        .relationship_type_counts
        .iter()
        .any(|(rel_type, count)| rel_type == "KNOWS" && *count == 1));

    let profile = db
        .profile_query(r#"MATCH (n:Person) RETURN n"#, QueryParams::new())
        .unwrap();
    assert_eq!(profile.metrics.rows_returned, 2);
    assert_eq!(profile.plan.estimated_rows, 2);
    assert!(profile.plan.estimated_cost >= 2);

    assert_eq!(db.checkpoint_now().unwrap().action, "checkpoint");
    assert_eq!(db.compact_storage().unwrap().action, "compact_observe");

    let _ = fs::remove_dir_all(dir);
}

#[test]
pub(super) fn engine_hardening_persists_statistics_and_metadata_log_across_reopen() {
    let dir = temp_dir("facade-engine-hardening-recovery");
    {
        let db =
            Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1).with_server_id(1)).unwrap();
        db.create_node(
            vec!["Person".to_string()],
            properties(&[("name", Value::String("Alice".to_string()))]),
        )
        .unwrap();
        db.register_cluster_node(2, "127.0.0.1:17688").unwrap();
        assert!(db.metadata_operations().unwrap().iter().any(|record| {
            record.operation == "register_cluster_node" && record.config_epoch == 1
        }));
        assert_eq!(db.statistics_catalog().unwrap().node_count, 1);
    }
    {
        let db =
            Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1).with_server_id(1)).unwrap();
        let statistics = db.statistics_catalog().unwrap();
        assert_eq!(statistics.node_count, 1);
        assert!(statistics
            .label_counts
            .iter()
            .any(|(label, count)| label == "Person" && *count == 1));
        assert!(statistics
            .node_property_counts
            .iter()
            .any(|(property, count)| property == "name" && *count == 1));
        assert!(db
            .metadata_operations()
            .unwrap()
            .iter()
            .any(|record| record.operation == "register_cluster_node"));
    }

    let _ = fs::remove_dir_all(dir);
}

#[test]
pub(super) fn cluster_join_request_negotiates_before_joining() {
    let dir = temp_dir("facade-cluster-join-negotiation");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 2, 1).with_server_id(1)).unwrap();

    let rejected = db
        .request_cluster_join(2, "127.0.0.1:17688", 1, 1, 3)
        .unwrap();
    assert!(rejected.nodes.iter().any(|node| {
        node.server_id == 2
            && node.state == NodeMembershipState::Rejected
            && node.rejection_reason.contains("shard count mismatch")
    }));

    let negotiating = db
        .request_cluster_join(2, "127.0.0.1:17688", 1, 1, 2)
        .unwrap();
    assert!(negotiating.nodes.iter().any(|node| {
        node.server_id == 2
            && node.state == NodeMembershipState::Negotiating
            && node.protocol_version == 1
            && node.storage_version == 1
            && node.shard_count == 2
    }));

    let joined = db.accept_cluster_join(2).unwrap();
    assert!(joined
        .nodes
        .iter()
        .any(|node| node.server_id == 2 && node.state == NodeMembershipState::Joining));

    let _ = fs::remove_dir_all(dir);
}

#[test]
pub(super) fn cluster_join_catch_up_plan_requires_snapshot_then_wal_tail() {
    let dir = temp_dir("facade-cluster-join-catch-up-plan");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1).with_server_id(1)).unwrap();
    db.register_cluster_node(1, "127.0.0.1:17687").unwrap();
    db.create_node(
        vec!["Person".to_string()],
        properties(&[("name", Value::String("CatchUpAlice".to_string()))]),
    )
    .unwrap();
    db.request_cluster_join(2, "127.0.0.1:17688", 1, 1, 1)
        .unwrap();
    db.accept_cluster_join(2).unwrap();
    db.prepare_rebalance_step(RebalanceStep::AddReplica {
        shard_id: 0,
        server_id: 2,
    })
    .unwrap();

    let plan = db.plan_node_catch_up(2).unwrap();
    assert_eq!(plan.server_id, 2);
    assert_eq!(plan.sources.len(), 1);
    assert_eq!(plan.sources[0].shard_id, 0);
    assert_eq!(plan.sources[0].primary_server_id, 1);
    assert_eq!(plan.sources[0].primary_address, "127.0.0.1:17687");
    assert!(plan.sources[0].snapshot_required);
    assert_eq!(plan.sources[0].start_index, 1);
    assert_eq!(plan.sources[0].target_index, 1);
    assert!(!plan.ready_to_promote);

    db.mark_shard_caught_up(0, 2, 1).unwrap();
    let ready = db.plan_node_catch_up(2).unwrap();
    assert!(ready.ready_to_promote);
    assert!(!ready.sources[0].snapshot_required);
    assert_eq!(ready.sources[0].start_index, 2);

    let _ = fs::remove_dir_all(dir);
}

#[test]
pub(super) fn cluster_bootstrap_manifest_persists_recover_from_data_boundary() {
    let dir = temp_dir("facade-cluster-bootstrap-manifest");
    {
        let db =
            Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 2, 1).with_server_id(9)).unwrap();
        db.create_node(
            vec!["Person".to_string()],
            properties(&[("name", Value::String("SeedAlice".to_string()))]),
        )
        .unwrap();
        db.snapshot_now().unwrap();
        let manifest = db
            .write_cluster_bootstrap_manifest(
                ClusterBootstrapMode::RecoverFromData,
                "new-cluster",
                "tenant-a",
            )
            .unwrap();
        assert_eq!(manifest.mode, ClusterBootstrapMode::RecoverFromData);
        assert!(manifest.force_new_cluster_required);
        assert_eq!(manifest.seed_server_id, 9);
        assert_eq!(manifest.shard_count, 2);
        assert_eq!(manifest.shards.len(), 2);
        assert!(manifest
            .shards
            .iter()
            .any(|shard| { shard.shard_id == 0 && shard.commit_index >= shard.snapshot_index }));
    }
    {
        let db =
            Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 2, 1).with_server_id(9)).unwrap();
        let manifest = db.load_cluster_bootstrap_manifest().unwrap().unwrap();
        assert_eq!(manifest.cluster_id, "new-cluster");
        assert_eq!(manifest.database_id, "tenant-a");
        db.validate_cluster_bootstrap_manifest(&manifest).unwrap();

        let mut stale = manifest.clone();
        stale.shards[0].commit_index = stale.shards[0].commit_index.saturating_add(1);
        let err = db.validate_cluster_bootstrap_manifest(&stale).unwrap_err();
        assert!(err.to_string().contains("does not match local commit"));
    }

    let _ = fs::remove_dir_all(dir);
}

#[test]
pub(super) fn catch_up_executor_replays_plan_and_promotes_caught_up_node() {
    struct FixtureSource {
        snapshot: InstallSnapshotRequest,
        entries: Vec<LogEntry>,
    }

    impl NodeCatchUpDataSource for FixtureSource {
        fn install_snapshot_request(
            &mut self,
            _source: &NodeCatchUpSource,
        ) -> DatabaseResult<Option<InstallSnapshotRequest>> {
            Ok(Some(self.snapshot.clone()))
        }

        fn log_entries(
            &mut self,
            _source: &NodeCatchUpSource,
            start_index: LogIndex,
            max_entries: Option<usize>,
        ) -> DatabaseResult<Vec<LogEntry>> {
            let mut entries = self
                .entries
                .iter()
                .filter(|entry| entry.index >= start_index)
                .cloned()
                .collect::<Vec<_>>();
            if let Some(max_entries) = max_entries {
                entries.truncate(max_entries);
            }
            Ok(entries)
        }
    }

    let dir = temp_dir("facade-catch-up-executor");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1).with_server_id(1)).unwrap();
    db.register_cluster_node(1, "127.0.0.1:17687").unwrap();
    db.register_cluster_node(2, "127.0.0.1:17688").unwrap();
    db.prepare_rebalance_step(RebalanceStep::AddReplica {
        shard_id: 0,
        server_id: 2,
    })
    .unwrap();
    let plan = NodeCatchUpPlan {
        server_id: 2,
        routing_version: db.routing_table().unwrap().version,
        metadata_term: db.cluster_metadata().unwrap().term,
        sources: vec![NodeCatchUpSource {
            shard_id: 0,
            primary_server_id: 1,
            primary_address: "127.0.0.1:17687".to_string(),
            snapshot_required: true,
            start_index: 2,
            target_index: 2,
            current_match_index: 0,
        }],
        ready_to_promote: false,
    };
    let mut source = FixtureSource {
        snapshot: InstallSnapshotRequest {
            term: 1,
            leader_id: 1,
            metadata: RaftSnapshotMetadata {
                shard_id: 0,
                last_included_term: 1,
                last_included_index: 1,
            },
            payload: snapshot_payload(&dir, 0, 1, 1, "SnapshotAlice"),
        },
        entries: vec![LogEntry::new_with_metadata(
            0,
            1,
            2,
            1,
            plan.routing_version,
            HybridTimestamp::new(2, 0),
            Command::CreateNode {
                id: 2,
                labels: vec!["Person".to_string()],
                properties: properties(&[("name", Value::String("TailBob".to_string()))]),
            },
        )],
    };

    let execution = db
        .execute_node_catch_up_plan(&plan, &mut source, Some(1))
        .unwrap();
    assert_eq!(execution.installed_snapshots, 1);
    assert_eq!(execution.replayed_entries, 1);
    assert!(execution.ready_to_promote);
    assert_eq!(db.node(1).unwrap().unwrap().labels, vec!["Person"]);
    assert_eq!(db.node(2).unwrap().unwrap().labels, vec!["Person"]);

    db.mark_shard_caught_up(0, 2, execution.shard_results[0].match_index)
        .unwrap();
    db.promote_caught_up_node_to_voter(2).unwrap();
    assert!(db
        .routing_table()
        .unwrap()
        .placement(0)
        .unwrap()
        .has_server(2));

    let _ = fs::remove_dir_all(dir);
}

#[test]
pub(super) fn bootstrap_safety_topology_backup_and_chaos_contracts_are_enforced() {
    let dir = temp_dir("facade-bootstrap-production-contracts");
    let backup_manifest = dir.join("backup-manifest.txt");
    fs::create_dir_all(&dir).unwrap();
    fs::write(&backup_manifest, "backup ok").unwrap();
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1).with_server_id(1)).unwrap();
    db.register_cluster_node(2, "127.0.0.1:17688").unwrap();
    db.prepare_rebalance_step(RebalanceStep::AddReplica {
        shard_id: 0,
        server_id: 2,
    })
    .unwrap();
    let manifest = db
        .write_cluster_bootstrap_manifest(
            ClusterBootstrapMode::RecoverFromData,
            "recovered-cluster",
            "tenant-a",
        )
        .unwrap();

    let blocked = db
        .bootstrap_safety_decision(&manifest, "recovered-cluster", false)
        .unwrap();
    assert!(!blocked.allowed);
    assert!(blocked.requires_force_new_cluster);
    let allowed = db
        .bootstrap_safety_decision(&manifest, "recovered-cluster", true)
        .unwrap();
    assert!(allowed.allowed);
    let mismatched = db
        .bootstrap_safety_decision(&manifest, "old-cluster", true)
        .unwrap();
    assert!(!mismatched.allowed);
    assert!(mismatched.reason.contains("cluster id mismatch"));

    let link = db
        .backup_bootstrap_link(&backup_manifest, &manifest)
        .unwrap();
    assert!(link.safe_to_seed);
    assert_eq!(link.database_id, "tenant-a");

    let topology = db.topology_observation().unwrap();
    assert_eq!(topology.recommended_action, "execute_catch_up");
    let safety = db
        .operational_safety_decision("recover_from_data", None)
        .unwrap();
    assert!(!safety.allowed);
    let confirmed = db
        .operational_safety_decision("recover_from_data", Some(&safety.confirmation_token))
        .unwrap();
    assert!(confirmed.allowed);
    for operation in [
        "restore_pitr",
        "token_revoke_all",
        "rbac_grant",
        "rbac_revoke",
    ] {
        let safety = db.operational_safety_decision(operation, None).unwrap();
        assert!(!safety.allowed, "{operation} must require confirmation");
        assert!(
            db.operational_safety_decision(operation, Some(&safety.confirmation_token))
                .unwrap()
                .allowed
        );
    }
    assert!(db
        .chaos_checks_for_join_catch_up()
        .unwrap()
        .iter()
        .all(|check| check.passed));

    let _ = fs::remove_dir_all(dir);
}

#[test]
pub(super) fn snapshot_chunk_resume_token_reports_next_offset() {
    let request = InstallSnapshotRequest {
        term: 3,
        leader_id: 1,
        metadata: RaftSnapshotMetadata {
            shard_id: 7,
            last_included_term: 2,
            last_included_index: 11,
        },
        payload: b"abcdef".to_vec(),
    };
    let mut chunks = request.chunks(3).into_iter();
    let first = chunks.next().unwrap();
    let mut assembler = SnapshotChunkAssembler::new(first).unwrap();
    let token = assembler.resume_token();
    assert_eq!(token.shard_id, 7);
    assert_eq!(token.snapshot_index, 11);
    assert_eq!(token.next_offset, 3);
    assert!(!token.completed);
    let assembled = assembler.push(chunks.next().unwrap()).unwrap().unwrap();
    assert_eq!(assembled.payload, b"abcdef");
    assert!(assembler.resume_token().completed);
}

#[test]
pub(super) fn cluster_membership_decommission_plans_primary_transfer_and_replica_removal() {
    let dir = temp_dir("facade-cluster-decommission");
    let table = ShardRoutingTable {
        version: 3,
        placements: vec![ShardPlacement::new(
            0,
            vec![ShardReplica::primary(1), ShardReplica::replica(2)],
        )],
    };
    let db = Neo4rDatabaseHandle::open(
        DatabaseConfig::new(&dir, 1, 1)
            .with_server_id(1)
            .with_routing_table(table),
    )
    .unwrap();
    db.register_cluster_node(2, "127.0.0.1:17688").unwrap();

    db.decommission_cluster_node(1).unwrap();
    let plan = db.plan_rebalance().unwrap();
    assert_eq!(
        plan.steps,
        vec![RebalanceStep::TransferPrimary {
            shard_id: 0,
            from: 1,
            to: 2,
        }]
    );
    db.apply_rebalance_step(plan.steps[0].clone()).unwrap();
    let next_plan = db.plan_rebalance().unwrap();
    assert_eq!(
        next_plan.steps,
        vec![RebalanceStep::RemoveReplica {
            shard_id: 0,
            server_id: 1,
        }]
    );
    db.apply_rebalance_step(next_plan.steps[0].clone()).unwrap();
    assert!(db
        .cluster_membership()
        .unwrap()
        .nodes
        .iter()
        .any(|node| node.server_id == 1 && node.state == NodeMembershipState::Removed));

    let _ = fs::remove_dir_all(dir);
}

pub(super) fn open_test_db(dir: &Path) -> Neo4rDatabase {
    Neo4rDatabase::open(DatabaseConfig::new(dir, 1, 2).with_log_entries_per_segment(2)).unwrap()
}

pub(super) fn properties(entries: &[(&str, Value)]) -> Properties {
    entries
        .iter()
        .map(|(key, value)| ((*key).to_string(), value.clone()))
        .collect()
}

pub(super) fn snapshot_payload(
    dir: &Path,
    shard_id: ShardId,
    last_included_term: Term,
    last_included_index: LogIndex,
    name: &str,
) -> Vec<u8> {
    let snapshot_dir = dir.join("snapshot-source");
    let _ = fs::remove_dir_all(&snapshot_dir);
    let store = neo4r_storage::SnapshotStore::open(&snapshot_dir, shard_id).unwrap();
    let mut graph = GraphState::new();
    graph
        .apply(Command::CreateNode {
            id: 1,
            labels: vec!["Person".to_string()],
            properties: properties(&[("name", Value::String(name.to_string()))]),
        })
        .unwrap();
    store
        .save(&graph, last_included_term, last_included_index)
        .unwrap();
    store.load_payload().unwrap().unwrap()
}

pub(super) fn temp_dir(prefix: &str) -> PathBuf {
    let mut path = std::env::temp_dir();
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    path.push(format!("neo4r-{prefix}-{}-{nanos}", std::process::id()));
    let _ = fs::remove_dir_all(&path);
    path
}

#[derive(Default)]
pub(super) struct RecordingReplicator {
    entries: Mutex<Vec<LogEntry>>,
}

impl RecordingReplicator {
    pub(super) fn entries(&self) -> Vec<LogEntry> {
        self.entries.lock().unwrap().clone()
    }
}

impl ShardReplicator for RecordingReplicator {
    fn publish(&self, entry: &LogEntry) -> DatabaseResult<ReplicationOutcome> {
        self.entries.lock().unwrap().push(entry.clone());
        Ok(ReplicationOutcome::local(entry.origin_server_id))
    }
}
