#[test]
fn cluster_rebalance_execution_advances_and_persists_status() {
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
fn cluster_metadata_authority_guards_metadata_mutations() {
    let dir = temp_dir("facade-metadata-authority");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1).with_server_id(1)).unwrap();

    let metadata = db.set_metadata_authority(2).unwrap();
    assert_eq!(metadata.authority_server_id, 2);
    let err = db.register_cluster_node(3, "127.0.0.1:17689").unwrap_err();
    assert!(err.to_string().contains("not metadata authority"));

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn cluster_rebalance_policy_limits_replica_additions() {
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
fn performance_profile_statistics_storage_and_read_cache_are_reported() {
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
fn engine_hardening_persists_statistics_and_metadata_log_across_reopen() {
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
fn cluster_join_request_negotiates_before_joining() {
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
fn cluster_membership_decommission_plans_primary_transfer_and_replica_removal() {
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

fn open_test_db(dir: &Path) -> Neo4rDatabase {
    Neo4rDatabase::open(DatabaseConfig::new(dir, 1, 2).with_log_entries_per_segment(2)).unwrap()
}

fn properties(entries: &[(&str, Value)]) -> Properties {
    entries
        .iter()
        .map(|(key, value)| ((*key).to_string(), value.clone()))
        .collect()
}

fn snapshot_payload(
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

fn temp_dir(prefix: &str) -> PathBuf {
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
struct RecordingReplicator {
    entries: Mutex<Vec<LogEntry>>,
}

impl RecordingReplicator {
    fn entries(&self) -> Vec<LogEntry> {
        self.entries.lock().unwrap().clone()
    }
}

impl ShardReplicator for RecordingReplicator {
    fn publish(&self, entry: &LogEntry) -> DatabaseResult<ReplicationOutcome> {
        self.entries.lock().unwrap().push(entry.clone());
        Ok(ReplicationOutcome::local(entry.origin_server_id))
    }
}
