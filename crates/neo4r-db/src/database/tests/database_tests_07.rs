#[test]
fn tcp_catch_up_is_idempotent_before_live_replication_continues() {
    let primary_dir = temp_dir("facade-tcp-catchup-live-primary");
    let replica_dir = temp_dir("facade-tcp-catchup-live-replica");
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
    for name in ["Alice", "Bob"] {
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
            .with_routing_table(routing_table.clone()),
    )
    .unwrap();
    let catch_up_listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let catch_up_address = catch_up_listener.local_addr().unwrap().to_string();
    let primary_for_listener = primary.clone();
    let catch_up_server = thread::spawn(move || {
        for _ in 0..2 {
            let (mut stream, _) = catch_up_listener.accept().unwrap();
            crate::handle_tcp_replication_stream(&primary_for_listener, &mut stream).unwrap();
        }
    });
    let peer_addresses = BTreeMap::from([(1, catch_up_address)]);

    let first_results = crate::catch_up_from_tcp_primaries(
        &replica,
        &routing_table,
        &peer_addresses,
        2,
        Duration::from_secs(1),
    )
    .unwrap();
    let second_results = crate::catch_up_from_tcp_primaries(
        &replica,
        &routing_table,
        &peer_addresses,
        2,
        Duration::from_secs(1),
    )
    .unwrap();

    catch_up_server.join().unwrap();
    assert_eq!(
        first_results,
        vec![crate::TcpCatchUpResult {
            shard_id: 0,
            start_index: 1,
            end_index: 2,
            fetched_entries: 2,
            primary_server_id: 1,
        }]
    );
    assert_eq!(
        second_results,
        vec![crate::TcpCatchUpResult {
            shard_id: 0,
            start_index: 3,
            end_index: 2,
            fetched_entries: 0,
            primary_server_id: 1,
        }]
    );
    assert_eq!(replica.query("MATCH (n:Person) RETURN n").unwrap().len(), 2);
    assert_eq!(replica.committed_indexes().unwrap(), vec![2]);

    drop(primary);
    let live_listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let live_address = live_listener.local_addr().unwrap().to_string();
    let replica_for_listener = replica.clone();
    let live_server = thread::spawn(move || {
        let (mut stream, _) = live_listener.accept().unwrap();
        crate::handle_tcp_replication_stream(&replica_for_listener, &mut stream).unwrap();
    });

    let replicator = Arc::new(crate::TcpShardReplicator::new(routing_table.clone()));
    replicator.register_peer(2, live_address).unwrap();
    let live_primary = Neo4rDatabaseHandle::open_with_replicator(
        DatabaseConfig::new(&primary_dir, 1, 2)
            .with_server_id(1)
            .with_routing_table(routing_table),
        replicator,
    )
    .unwrap();
    live_primary
        .create_node(
            vec!["Person".to_string()],
            properties(&[("name", Value::String("Carol".to_string()))]),
        )
        .unwrap();

    live_server.join().unwrap();
    assert_eq!(live_primary.committed_indexes().unwrap(), vec![3]);
    assert_eq!(replica.committed_indexes().unwrap(), vec![3]);
    assert_eq!(
        replica
            .query(r#"MATCH (n:Person) WHERE n.name = "Carol" RETURN n.name"#)
            .unwrap()
            .len(),
        1
    );

    drop(live_primary);
    drop(replica);
    let _ = fs::remove_dir_all(primary_dir);
    let _ = fs::remove_dir_all(replica_dir);
}

#[test]
fn in_process_replicator_batches_group_commit_to_replica() {
    let primary_dir = temp_dir("facade-inprocess-batch-primary");
    let replica_dir = temp_dir("facade-inprocess-batch-replica");
    let write_count = 8;
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
            .with_routing_table(routing_table.clone())
            .with_group_commit_max_entries(write_count)
            .with_group_commit_max_delay(Duration::from_millis(20)),
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
                .unwrap()
        }));
    }
    for worker in workers {
        worker.join().unwrap();
    }

    assert_eq!(
        primary.committed_indexes().unwrap(),
        vec![write_count as u64]
    );
    assert_eq!(
        replica.committed_indexes().unwrap(),
        vec![write_count as u64]
    );
    assert_eq!(
        replica.query("MATCH (n:Person) RETURN n").unwrap().len(),
        write_count
    );

    let _ = fs::remove_dir_all(primary_dir);
    let _ = fs::remove_dir_all(replica_dir);
}

#[test]
fn in_process_replicator_reports_missing_replica_peer() {
    let dir = temp_dir("facade-inprocess-missing-peer");
    let routing_table = ShardRoutingTable {
        version: 3,
        placements: vec![ShardPlacement::new(
            0,
            vec![ShardReplica::primary(1), ShardReplica::replica(2)],
        )],
    };
    let replicator = Arc::new(crate::InProcessShardReplicator::new(routing_table.clone()));
    let primary = Neo4rDatabaseHandle::open_with_replicator(
        DatabaseConfig::new(&dir, 1, 2)
            .with_server_id(1)
            .with_routing_table(routing_table),
        replicator,
    )
    .unwrap();

    let err = primary
        .create_node(vec!["Person".to_string()], Properties::new())
        .unwrap_err();

    assert!(matches!(err, DatabaseError::Replication(_)));
    assert_eq!(primary.committed_indexes().unwrap(), vec![0]);

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn uncommitted_wal_entry_is_not_replayed_after_reopen() {
    let dir = temp_dir("facade-uncommitted-replay");
    let routing_table = ShardRoutingTable {
        version: 3,
        placements: vec![ShardPlacement::new(
            0,
            vec![ShardReplica::primary(1), ShardReplica::replica(2)],
        )],
    };
    {
        let replicator = Arc::new(crate::InProcessShardReplicator::new(routing_table.clone()));
        let mut primary = Neo4rDatabase::open_with_replicator(
            DatabaseConfig::new(&dir, 1, 2)
                .with_server_id(1)
                .with_routing_table(routing_table.clone()),
            replicator,
        )
        .unwrap();

        assert!(matches!(
            primary.create_node(
                vec!["Person".to_string()],
                properties(&[("name", Value::String("Alice".to_string()))]),
            ),
            Err(DatabaseError::Replication(_))
        ));
    }

    let reopened = Neo4rDatabase::open(
        DatabaseConfig::new(&dir, 1, 2)
            .with_server_id(1)
            .with_routing_table(routing_table),
    )
    .unwrap();

    assert_eq!(
        reopened.query("MATCH (n:Person) RETURN n").unwrap().len(),
        0
    );
    assert_eq!(reopened.committed_indexes(), vec![0]);

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn committed_config_change_replays_routing_metadata_after_reopen() {
    let dir = temp_dir("facade-config-change-replay");
    let initial_routing_table = ShardRoutingTable {
        version: 3,
        placements: vec![ShardPlacement::new(0, vec![ShardReplica::primary(1)])],
    };
    let updated_routing_table = ShardRoutingTable {
        version: 4,
        placements: vec![ShardPlacement::new(
            0,
            vec![ShardReplica::primary(1), ShardReplica::replica(2)],
        )],
    };
    {
        let db = Neo4rDatabase::open(
            DatabaseConfig::new(&dir, 1, 1)
                .with_server_id(1)
                .with_routing_table(initial_routing_table.clone()),
        )
        .unwrap();
        let entry = LogEntry::new(
            0,
            1,
            1,
            Command::ClusterConfigChange {
                phase: "install".to_string(),
                description: "install_routing_table:version=4".to_string(),
                voters: vec![1, 2],
                routing_table: updated_routing_table.clone(),
            },
        );
        db.log(0).unwrap().append(&entry).unwrap();
        db.commits[0].save(1, 1).unwrap();
    }

    let reopened = Neo4rDatabase::open(
        DatabaseConfig::new(&dir, 1, 1)
            .with_server_id(1)
            .with_routing_table(initial_routing_table),
    )
    .unwrap();

    assert_eq!(reopened.routing_table().version, 4);
    assert_eq!(
        reopened.routing_table().placement(0).unwrap().replicas,
        updated_routing_table.placement(0).unwrap().replicas
    );

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn quorum_replication_succeeds_when_majority_acks() {
    let primary_dir = temp_dir("facade-quorum-primary");
    let replica_dir = temp_dir("facade-quorum-replica");
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
    let replicator = Arc::new(
        crate::InProcessShardReplicator::new(routing_table.clone())
            .with_ack_policy(crate::ReplicationAckPolicy::Quorum),
    );
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

    let _ = fs::remove_dir_all(primary_dir);
    let _ = fs::remove_dir_all(replica_dir);
}

#[test]
fn async_replication_allows_missing_replica_peer() {
    let dir = temp_dir("facade-async-missing-peer");
    let routing_table = ShardRoutingTable {
        version: 3,
        placements: vec![ShardPlacement::new(
            0,
            vec![ShardReplica::primary(1), ShardReplica::replica(2)],
        )],
    };
    let replicator = Arc::new(
        crate::InProcessShardReplicator::new(routing_table.clone())
            .with_ack_policy(crate::ReplicationAckPolicy::Async),
    );
    let primary = Neo4rDatabaseHandle::open_with_replicator(
        DatabaseConfig::new(&dir, 1, 2)
            .with_server_id(1)
            .with_routing_table(routing_table),
        replicator,
    )
    .unwrap();

    primary
        .create_node(vec!["Person".to_string()], Properties::new())
        .unwrap();

    assert_eq!(primary.query("MATCH (n:Person) RETURN n").unwrap().len(), 1);
    assert_eq!(primary.committed_indexes().unwrap(), vec![1]);

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn local_write_rejects_non_primary_shard() {
    let dir = temp_dir("facade-non-primary-write");
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
        .create_node(vec!["Person".to_string()], Properties::new())
        .unwrap_err();

    assert!(matches!(
        err,
        DatabaseError::ShardNotPrimary {
            shard_id: 0,
            server_id: 2,
            primary_server_id: Some(1)
        }
    ));

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn create_node_on_shard_allocates_id_owned_by_requested_shard() {
    let dir = temp_dir("facade-create-node-on-shard");
    let routing_table = ShardRoutingTable {
        version: 2,
        placements: vec![
            ShardPlacement::new(0, vec![ShardReplica::primary(1)]),
            ShardPlacement::new(1, vec![ShardReplica::primary(1)]),
        ],
    };
    let db = Neo4rDatabaseHandle::open(
        DatabaseConfig::new(&dir, 2, 2)
            .with_server_id(1)
            .with_routing_table(routing_table),
    )
    .unwrap();

    let id = db
        .create_node_on_shard(
            1,
            vec!["Person".to_string()],
            properties(&[("name", Value::String("ShardOne".to_string()))]),
        )
        .unwrap();

    assert_eq!(id % 2, 1);
    assert_eq!(
        db.query_shard(1, "MATCH (n:Person) RETURN n.name")
            .unwrap()
            .len(),
        1
    );

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn execute_create_node_cypher_on_shard_returns_created_node() {
    let dir = temp_dir("facade-create-node-cypher-on-shard");
    let routing_table = ShardRoutingTable {
        version: 2,
        placements: vec![
            ShardPlacement::new(0, vec![ShardReplica::primary(1)]),
            ShardPlacement::new(1, vec![ShardReplica::primary(1)]),
        ],
    };
    let db = Neo4rDatabaseHandle::open(
        DatabaseConfig::new(&dir, 2, 2)
            .with_server_id(1)
            .with_routing_table(routing_table),
    )
    .unwrap();
    let rows = db
        .execute_create_node_cypher_on_shard(
            1,
            "CREATE (n:Person {name: $name}) RETURN n",
            [("name".to_string(), Value::String("ShardCypher".to_string()))]
                .into_iter()
                .collect(),
        )
        .unwrap();

    assert_eq!(rows.len(), 1);
    let Some(QueryValue::Node(node)) = rows[0].get("n") else {
        panic!("expected created node");
    };
    assert_eq!(node.id % 2, 1);
    assert_eq!(
        node.properties.get("name"),
        Some(&Value::String("ShardCypher".to_string()))
    );

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn routing_metadata_persists_across_reopen() {
    let dir = temp_dir("facade-routing-persistence");
    let routing_table = ShardRoutingTable {
        version: 5,
        placements: vec![
            ShardPlacement::new(
                0,
                vec![ShardReplica::primary(10), ShardReplica::replica(11)],
            ),
            ShardPlacement::new(
                1,
                vec![ShardReplica::primary(11), ShardReplica::replica(10)],
            ),
        ],
    };

    {
        Neo4rDatabase::open(
            DatabaseConfig::new(&dir, 2, 2)
                .with_server_id(10)
                .with_routing_table(routing_table.clone()),
        )
        .unwrap();
    }

    let reopened = Neo4rDatabase::open(DatabaseConfig::new(&dir, 2, 2).with_server_id(10)).unwrap();

    assert_eq!(reopened.routing_table(), &routing_table);

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn query_route_reports_remote_shards() {
    let dir = temp_dir("facade-query-route");
    let routing_table = ShardRoutingTable {
        version: 5,
        placements: vec![
            ShardPlacement::new(0, vec![ShardReplica::primary(10)]),
            ShardPlacement::new(1, vec![ShardReplica::primary(11)]),
        ],
    };
    let db = Neo4rDatabaseHandle::open(
        DatabaseConfig::new(&dir, 2, 2)
            .with_server_id(10)
            .with_routing_table(routing_table),
    )
    .unwrap();

    assert_eq!(
        db.query_route().unwrap(),
        QueryRoute::RequiresRemoteShards(vec![1])
    );
    assert_eq!(
        db.query_plan("MATCH (n:Person) RETURN n").unwrap(),
        DistributedQueryPlan {
            route: QueryRoute::RequiresRemoteShards(vec![1]),
            traversal_policy: RemoteTraversalPolicy::RemoteShardHop(vec![1]),
            uses_boundary_cache: true,
            access_plan: QueryAccessPlan::NodeLabelScan {
                label: "Person".to_string(),
            },
            access_reason: "label cardinality 0 for Person; remote_shard_penalty=1".to_string(),
            cost_model_version: 3,
            estimated_cost: 101,
            estimated_rows: 0,
            remote_shard_count: 1,
        }
    );

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn query_plan_reports_read_access_path() {
    let dir = temp_dir("facade-query-access-plan");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1)).unwrap();
    db.execute_cypher("CREATE INDEX person_name FOR (n:Person) ON (n.name)")
        .unwrap();
    db.execute_cypher(
        "CREATE CONSTRAINT person_email_unique FOR (n:Person) REQUIRE n.email IS UNIQUE",
    )
    .unwrap();
    db.execute_cypher(
        "CREATE VECTOR INDEX doc_embedding ON :Document(embedding) DIMENSIONS 2 METRIC l2",
    )
    .unwrap();

    assert_eq!(
        db.query_plan(r#"MATCH (n:Person) WHERE n.email = "a@example.com" RETURN n"#)
            .unwrap()
            .access_plan,
        QueryAccessPlan::NodeUniqueIndexSeek {
            label: "Person".to_string(),
            property: "email".to_string(),
        }
    );
    assert_eq!(
        db.query_plan(r#"MATCH (n:Person) WHERE n.name = "Alice" RETURN n"#)
            .unwrap()
            .access_plan,
        QueryAccessPlan::NodeIndexSeek {
            label: "Person".to_string(),
            property: "name".to_string(),
        }
    );
    assert_eq!(
        db.query_plan_with_params(
            "MATCH (n:Person {name: $name}) RETURN n",
            [("name".to_string(), Value::String("Alice".to_string()))]
                .into_iter()
                .collect()
        )
        .unwrap()
        .access_plan,
        QueryAccessPlan::NodeIndexSeek {
            label: "Person".to_string(),
            property: "name".to_string(),
        }
    );
    assert_eq!(
        db.query_plan_with_params(
            "MATCH (n:Person {email: $email, name: $name}) RETURN n",
            [
                (
                    "email".to_string(),
                    Value::String("a@example.com".to_string())
                ),
                ("name".to_string(), Value::String("Alice".to_string())),
            ]
            .into_iter()
            .collect()
        )
        .unwrap()
        .access_plan,
        QueryAccessPlan::NodeUniqueIndexSeek {
            label: "Person".to_string(),
            property: "email".to_string(),
        }
    );
    assert_eq!(
        db.query_plan(
            r#"MATCH (n:Document) WHERE vector.knn(n.embedding, [0.0, 1.0], 3, "l2") RETURN n"#
        )
        .unwrap()
        .access_plan,
        QueryAccessPlan::VectorIndexSeek {
            label: Some("Document".to_string()),
            property: "embedding".to_string(),
            metric: "l2".to_string(),
        }
    );
    assert_eq!(
        db.query_plan("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN b")
            .unwrap()
            .access_plan,
        QueryAccessPlan::RelationshipTypeScan {
            rel_type: "KNOWS".to_string(),
        }
    );
    assert!(matches!(
        db.query_plan("MATCH (n:Person) RETURN m.name")
            .unwrap()
            .access_plan,
        QueryAccessPlan::Unsupported { reason } if reason.contains("variable \"m\" is not bound")
    ));

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn cluster_status_reports_shard_positions_and_roles() {
    let dir = temp_dir("facade-cluster-status");
    let routing_table = ShardRoutingTable {
        version: 7,
        placements: vec![
            ShardPlacement::new(
                0,
                vec![ShardReplica::primary(10), ShardReplica::replica(11)],
            ),
            ShardPlacement::new(
                1,
                vec![ShardReplica::primary(11), ShardReplica::replica(10)],
            ),
        ],
    };
    let db = Neo4rDatabaseHandle::open(
        DatabaseConfig::new(&dir, 2, 2)
            .with_server_id(10)
            .with_routing_table(routing_table),
    )
    .unwrap();
    db.create_node(
        vec!["Person".to_string()],
        properties(&[("name", Value::String("Alice".to_string()))]),
    )
    .unwrap();

    let status = db.cluster_status().unwrap();

    assert_eq!(status.server_id, 10);
    assert_eq!(status.routing_version, 7);
    assert_eq!(status.shard_count, 2);
    assert_eq!(status.local_partition_count, 2);
    assert_eq!(status.shards.len(), 2);
    assert_eq!(status.shards[0].primary_server_id, Some(10));
    assert_eq!(status.shards[0].replica_server_ids, vec![11]);
    assert!(status.shards[0].has_local_copy);
    assert!(status.shards[0].is_local_primary);
    assert_eq!(status.shards[0].applied_index, 1);
    assert_eq!(status.shards[0].committed_index, 1);
    assert_eq!(status.shards[1].primary_server_id, Some(11));
    assert_eq!(status.shards[1].replica_server_ids, vec![10]);
    assert!(status.shards[1].has_local_copy);
    assert!(!status.shards[1].is_local_primary);
    assert_eq!(status.shards[1].applied_index, 0);
    assert_eq!(status.shards[1].committed_index, 0);

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn query_with_strong_consistency_reads_committed_snapshot() {
    let dir = temp_dir("facade-read-consistency");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 2)).unwrap();

    db.create_node(
        vec!["Person".to_string()],
        properties(&[("name", Value::String("Alice".to_string()))]),
    )
    .unwrap();

    assert_eq!(
        db.query_with_options(
            r#"MATCH (n:Person) WHERE n.name = "Alice" RETURN n.name"#,
            QueryOptions::default().with_consistency(ReadConsistency::Strong),
        )
        .unwrap()
        .len(),
        1
    );

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn cluster_membership_registers_nodes_and_plans_rebalance() {
    let dir = temp_dir("facade-cluster-membership");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 2, 1).with_server_id(1)).unwrap();

    let membership = db.register_cluster_node(2, "127.0.0.1:17688").unwrap();
    assert_eq!(membership.version, 2);
    assert!(membership
        .nodes
        .iter()
        .any(|node| node.server_id == 2 && node.state == NodeMembershipState::Joining));

    let plan = db.plan_rebalance().unwrap();
    assert_eq!(plan.plan_id, 1);
    assert_eq!(plan.state, RebalancePlanState::Proposed);
    assert_eq!(plan.from_routing_version, 1);
    assert_eq!(plan.target_routing_version, 3);
    assert_eq!(
        plan.steps,
        vec![
            RebalanceStep::AddReplica {
                shard_id: 0,
                server_id: 2,
            },
            RebalanceStep::AddReplica {
                shard_id: 1,
                server_id: 2,
            },
        ]
    );

    assert!(db.apply_rebalance_step(plan.steps[0].clone()).is_err());
    db.prepare_rebalance_step(plan.steps[0].clone()).unwrap();
    assert!(db
        .cluster_membership()
        .unwrap()
        .shard_assignments
        .iter()
        .any(|assignment| assignment.shard_id == 0
            && assignment.server_id == 2
            && assignment.state == ShardAssignmentState::CatchingUp));
    db.mark_shard_caught_up(0, 2, 0).unwrap();
    db.apply_rebalance_step(plan.steps[0].clone()).unwrap();
    assert!(db
        .routing_table()
        .unwrap()
        .placement(0)
        .unwrap()
        .has_server(2));
    assert!(db
        .cluster_membership()
        .unwrap()
        .nodes
        .iter()
        .any(|node| node.server_id == 2 && node.state == NodeMembershipState::Active));

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn cluster_rebalance_plan_id_survives_reopen() {
    let dir = temp_dir("facade-rebalance-plan-store");
    {
        let db =
            Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1).with_server_id(1)).unwrap();
        db.register_cluster_node(2, "127.0.0.1:17688").unwrap();
        let plan = db.plan_rebalance().unwrap();
        assert_eq!(plan.plan_id, 1);
    }
    {
        let db =
            Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1).with_server_id(1)).unwrap();
        let plan = db.plan_rebalance().unwrap();
        assert_eq!(plan.plan_id, 2);
        assert_eq!(plan.state, RebalancePlanState::Proposed);
    }

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn cluster_rebalance_rejects_stale_caught_up_assignment() {
    let dir = temp_dir("facade-rebalance-stale-caught-up");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1).with_server_id(1)).unwrap();
    db.create_node_on_shard(
        0,
        vec!["Person".to_string()],
        properties(&[("name", Value::String("Alice".to_string()))]),
    )
    .unwrap();
    db.register_cluster_node(2, "127.0.0.1:17688").unwrap();
    let plan = db.plan_rebalance().unwrap();
    let step = plan.steps[0].clone();

    db.prepare_rebalance_step(step.clone()).unwrap();
    db.mark_shard_caught_up(0, 2, 0).unwrap();
    let err = db.apply_rebalance_step(step.clone()).unwrap_err();
    assert!(err.to_string().contains("is behind committed index"));

    db.mark_shard_caught_up(0, 2, 1).unwrap();
    db.apply_rebalance_step(step).unwrap();

    let _ = fs::remove_dir_all(dir);
}
