#![allow(unused_imports)]

use super::super::*;
use super::*;
use crate::execute_request;
use neo4r_core::{BoundaryNode, Node, Properties, Relationship, Value};
use neo4r_db::{DatabaseConfig, IndexCatalog, IndexDefinition, Neo4rDatabaseHandle};
use neo4r_protocol::encode_properties;
use neo4r_query::QueryValue;
use std::fs;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

#[test]
pub(super) fn parses_create_node_with_typed_properties() {
    let request = parse_request("CREATE_NODE\tPerson,User\tname=s:alice\tage=i:42").unwrap();

    let BackendRequest::CreateNode { labels, properties } = request else {
        panic!("unexpected request");
    };
    assert_eq!(labels, vec!["Person".to_string(), "User".to_string()]);
    assert_eq!(
        properties.get("name"),
        Some(&Value::String("alice".to_string()))
    );
    assert_eq!(properties.get("age"), Some(&Value::Int(42)));
}

#[test]
pub(super) fn parses_create_node_on_shard_with_typed_properties() {
    let request = parse_request("CREATE_NODE_SHARD\t1\tPerson\tname=s:alice\tage=i:42").unwrap();

    let BackendRequest::CreateNodeOnShard {
        shard_id,
        labels,
        properties,
    } = request
    else {
        panic!("unexpected request");
    };
    assert_eq!(shard_id, 1);
    assert_eq!(labels, vec!["Person".to_string()]);
    assert_eq!(
        properties.get("name"),
        Some(&Value::String("alice".to_string()))
    );
    assert_eq!(properties.get("age"), Some(&Value::Int(42)));
}

#[test]
pub(super) fn parses_remove_property_commands() {
    assert_eq!(
        parse_request("REMOVE_NODE_PROPERTY\t7\tstatus").unwrap(),
        BackendRequest::RemoveNodeProperty {
            id: 7,
            key: "status".to_string(),
        }
    );
    assert_eq!(
        parse_request("REMOVE_RELATIONSHIP_PROPERTY\t9\tweight").unwrap(),
        BackendRequest::RemoveRelationshipProperty {
            id: 9,
            key: "weight".to_string(),
        }
    );
}

#[test]
pub(super) fn parses_node_label_commands() {
    assert_eq!(
        parse_request("ADD_NODE_LABEL\t7\tEmployee").unwrap(),
        BackendRequest::AddNodeLabel {
            id: 7,
            label: "Employee".to_string(),
        }
    );
    assert_eq!(
        parse_request("REMOVE_NODE_LABEL\t7\tPerson").unwrap(),
        BackendRequest::RemoveNodeLabel {
            id: 7,
            label: "Person".to_string(),
        }
    );
}

#[test]
pub(super) fn rejects_unknown_value_prefix() {
    let err = parse_request("CREATE_NODE\tPerson\tname=x:alice").unwrap_err();

    assert_eq!(err, "unknown value type prefix: x");
}

#[test]
pub(super) fn parses_query_with_typed_params() {
    let props = [
        ("score".to_string(), Value::Int(7)),
        ("status".to_string(), Value::String("active".to_string())),
    ]
    .into_iter()
    .collect();
    let encoded_props = hex_encode(encode_properties(&props).as_bytes());
    let request = parse_request(
            &format!(
                "QUERY\tMATCH (n:Document) WHERE vector.knn(n.embedding, $embedding, $k, $metric) RETURN n.title\tembedding=v:1.0,0.0\tk=i:4\tmetric=s:l2\tprops=m:{encoded_props}"
            ),
        )
        .unwrap();

    let BackendRequest::Query { query, params } = request else {
        panic!("unexpected request");
    };
    assert!(query.contains("vector.knn"));
    assert_eq!(
        params.get("embedding"),
        Some(&Value::Vector(vec![1.0, 0.0]))
    );
    assert_eq!(params.get("k"), Some(&Value::Int(4)));
    assert_eq!(params.get("metric"), Some(&Value::String("l2".to_string())));
    assert_eq!(params.get("props"), Some(&Value::Map(props)));
}

#[test]
pub(super) fn parses_query_plan_with_typed_params() {
    let request =
        parse_request("QUERY_PLAN\tMATCH (n:Person) WHERE n.name = $name RETURN n\tname=s:Alice")
            .unwrap();

    let BackendRequest::QueryPlan { query, params } = request else {
        panic!("unexpected request");
    };
    assert!(query.starts_with("MATCH"));
    assert_eq!(
        params.get("name"),
        Some(&Value::String("Alice".to_string()))
    );
}

#[test]
pub(super) fn parses_query_write_shard_with_typed_params() {
    let request = parse_request(
        "QUERY_WRITE_SHARD\t1\tCREATE (n:Person {name: $name}) RETURN n.name\tname=s:alice",
    )
    .unwrap();

    let BackendRequest::QueryWriteShard {
        shard_id,
        query,
        params,
    } = request
    else {
        panic!("unexpected request");
    };
    assert_eq!(shard_id, 1);
    assert!(query.starts_with("CREATE"));
    assert_eq!(
        params.get("name"),
        Some(&Value::String("alice".to_string()))
    );
}

#[test]
pub(super) fn query_write_batch_shard_codec_round_trips_params() {
    let writes = vec![
        (
            "MATCH (n:Person) SET n.status = $status".to_string(),
            [(
                "status".to_string(),
                Value::String("active\tready".to_string()),
            )]
            .into_iter()
            .collect(),
        ),
        (
            "MATCH (n:Person) SET n.reviewed = $reviewed".to_string(),
            [("reviewed".to_string(), Value::Bool(true))]
                .into_iter()
                .collect(),
        ),
        (
            "MATCH (n:Person) SET n += $props".to_string(),
            [(
                "props".to_string(),
                Value::Map(
                    [("status".to_string(), Value::String("ready".to_string()))]
                        .into_iter()
                        .collect(),
                ),
            )]
            .into_iter()
            .collect(),
        ),
    ];
    let request = parse_request(&format!(
        "QUERY_WRITE_BATCH_SHARD\t1\t{}",
        encode_query_batch_payload(&writes)
    ))
    .unwrap();

    assert_eq!(
        request,
        BackendRequest::QueryWriteBatchShard {
            shard_id: 1,
            writes
        }
    );
}

#[test]
pub(super) fn query_staged_shard_codec_uses_first_batch_entry_as_read() {
    let read_params = [("name".to_string(), Value::String("Alice".to_string()))]
        .into_iter()
        .collect();
    let staged_params = [("status".to_string(), Value::String("staged".to_string()))]
        .into_iter()
        .collect();
    let batch = vec![
        ("MATCH (n:Person) RETURN n.status".to_string(), read_params),
        (
            "MATCH (n:Person) WHERE n.name = $name SET n.status = $status".to_string(),
            staged_params,
        ),
    ];
    let request = parse_request(&format!(
        "QUERY_STAGED_SHARD\t1\t{}",
        encode_query_batch_payload(&batch)
    ))
    .unwrap();

    let BackendRequest::QueryStagedShard {
        shard_id,
        query,
        params,
        staged_writes,
    } = request
    else {
        panic!("unexpected request");
    };
    assert_eq!(shard_id, 1);
    assert_eq!(query, batch[0].0);
    assert_eq!(params, batch[0].1);
    assert_eq!(staged_writes, vec![batch[1].clone()]);
}

#[test]
pub(super) fn index_catalog_codec_round_trips_definitions() {
    let catalog = IndexCatalog {
        version: 7,
        indexes: vec![
            IndexDefinition::node_property("person_name", "Person", "name"),
            IndexDefinition::unique_node_property("person_email_unique", "Person", "email"),
            IndexDefinition::vector("doc_embedding", "Document", "embedding", 3, "cosine"),
        ],
    };

    let encoded = encode_index_catalog(&catalog);
    assert_eq!(decode_index_catalog(&encoded).unwrap(), catalog);
    assert_eq!(
        parse_request(&format!("INSTALL_INDEX_CATALOG\t{encoded}")).unwrap(),
        BackendRequest::InstallIndexCatalog(catalog)
    );
}

#[test]
pub(super) fn query_row_codec_round_trips_scalars_nodes_and_relationships() {
    let mut row = QueryRow::new();
    row.insert(
        "name",
        QueryValue::Scalar(Value::String("Alice\tA".to_string())),
    );
    row.insert(
        "n",
        QueryValue::Node(Node::new(
            7,
            vec!["Person".to_string()],
            [("age".to_string(), Value::Int(42))].into_iter().collect(),
        )),
    );
    row.insert(
        "r",
        QueryValue::Relationship(Relationship::new(
            9,
            7,
            8,
            "KNOWS".to_string(),
            [("since".to_string(), Value::Int(2026))]
                .into_iter()
                .collect(),
        )),
    );
    row.insert(
        "b",
        QueryValue::BoundaryNode(BoundaryNode::new(
            8,
            1,
            vec!["Person".to_string()],
            [("name".to_string(), Value::String("Bob".to_string()))]
                .into_iter()
                .collect(),
            3,
        )),
    );

    let encoded = encode_query_rows(&[row.clone()]);

    assert_eq!(decode_query_rows(&encoded).unwrap(), vec![row]);
}

#[test]
pub(super) fn query_shard_parses_and_executes_against_one_shard() {
    let dir = temp_dir("server-query-shard");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 2, 2)).unwrap();
    db.create_node(
        vec!["Person".to_string()],
        [("name".to_string(), Value::String("Alice".to_string()))]
            .into_iter()
            .collect(),
    )
    .unwrap();
    db.create_node(
        vec!["Person".to_string()],
        [("name".to_string(), Value::String("Bob".to_string()))]
            .into_iter()
            .collect(),
    )
    .unwrap();

    let request = parse_request(
        "QUERY_SHARD\t1\tMATCH (n:Person) WHERE n.name = $name RETURN n.name\tname=s:Bob",
    )
    .unwrap();
    let BackendRequest::QueryShard {
        shard_id,
        query,
        params,
    } = &request
    else {
        panic!("unexpected request");
    };
    assert_eq!(*shard_id, 1);
    assert!(query.contains("MATCH"));
    assert_eq!(params.get("name"), Some(&Value::String("Bob".to_string())));

    assert!(matches!(
        execute_request(&db, request),
        BackendResponse::OkRows { count: 1, .. }
    ));
    assert!(matches!(
        execute_request(
            &db,
            parse_request(
                "QUERY_SHARD\t0\tMATCH (n:Person) WHERE n.name = $name RETURN n.name\tname=s:Bob"
            )
            .unwrap()
        ),
        BackendResponse::OkRows { count: 0, .. }
    ));

    let _ = fs::remove_dir_all(dir);
}

#[test]
pub(super) fn query_distributed_parses_but_requires_backend_coordinator() {
    let dir = temp_dir("server-query-distributed");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 2, 2)).unwrap();

    let request = parse_request(
        "QUERY_DISTRIBUTED\tMATCH (n:Person) WHERE n.name = $name RETURN n.name\tname=s:Alice",
    )
    .unwrap();
    let BackendRequest::QueryDistributed { query, params } = &request else {
        panic!("unexpected request");
    };
    assert!(query.contains("MATCH"));
    assert_eq!(
        params.get("name"),
        Some(&Value::String("Alice".to_string()))
    );
    assert!(matches!(
        execute_request(&db, request),
        BackendResponse::Err(message) if message.contains("requires a backend coordinator")
    ));

    let _ = fs::remove_dir_all(dir);
}

#[test]
pub(super) fn peer_management_and_catch_up_parse_but_require_backend_coordinator() {
    let dir = temp_dir("server-query-peer-management");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1)).unwrap();

    assert_eq!(
        parse_request("REGISTER_QUERY_PEER\t2\t127.0.0.1:7688").unwrap(),
        BackendRequest::RegisterQueryPeer {
            server_id: 2,
            address: "127.0.0.1:7688".to_string(),
        }
    );
    assert_eq!(
        parse_request("UNREGISTER_QUERY_PEER\t2").unwrap(),
        BackendRequest::UnregisterQueryPeer(2)
    );
    assert_eq!(
        parse_request("LIST_QUERY_PEERS").unwrap(),
        BackendRequest::ListQueryPeers
    );
    assert_eq!(
        parse_request("REGISTER_REPLICATION_PEER\t3\t127.0.0.1:7689").unwrap(),
        BackendRequest::RegisterReplicationPeer {
            server_id: 3,
            address: "127.0.0.1:7689".to_string(),
            node_id: None,
            transport: None,
        }
    );
    assert_eq!(
        parse_request("REGISTER_REPLICATION_PEER\t3\t127.0.0.1:7689\t30\ttcp").unwrap(),
        BackendRequest::RegisterReplicationPeer {
            server_id: 3,
            address: "127.0.0.1:7689".to_string(),
            node_id: Some(30),
            transport: Some(neo4r_db::ReplicationChannelKind::Tcp),
        }
    );
    assert_eq!(
        parse_request("NEGOTIATE_REPLICATION_PEER\t3\t127.0.0.1:7689\t30").unwrap(),
        BackendRequest::NegotiateReplicationPeer {
            server_id: 3,
            address: "127.0.0.1:7689".to_string(),
            node_id: Some(30),
        }
    );
    assert_eq!(
        parse_request("UNREGISTER_REPLICATION_PEER\t3").unwrap(),
        BackendRequest::UnregisterReplicationPeer(3)
    );
    assert_eq!(
        parse_request("LIST_REPLICATION_PEERS").unwrap(),
        BackendRequest::ListReplicationPeers
    );
    assert_eq!(
        parse_request("REPLICATION_PEER_STATUS").unwrap(),
        BackendRequest::ReplicationPeerStatus { server_id: None }
    );
    assert_eq!(
        parse_request("REPLICATION_PEER_STATUS\t3").unwrap(),
        BackendRequest::ReplicationPeerStatus { server_id: Some(3) }
    );
    assert_eq!(
        parse_request("REPLICATION_STATUS").unwrap(),
        BackendRequest::ReplicationStatus
    );
    assert_eq!(
        parse_request("ROUTING_TABLE").unwrap(),
        BackendRequest::RoutingTable
    );
    assert_eq!(
        parse_request("CLUSTER_REGISTRY").unwrap(),
        BackendRequest::ClusterRegistry
    );
    assert_eq!(
        parse_request("CAPABILITIES").unwrap(),
        BackendRequest::Capabilities
    );
    assert_eq!(
        parse_request("CATCH_UP_FROM_PRIMARIES").unwrap(),
        BackendRequest::CatchUpFromPrimaries {
            max_entries_per_request: None,
        }
    );
    assert_eq!(
        parse_request("CATCH_UP_FROM_PRIMARIES\t2").unwrap(),
        BackendRequest::CatchUpFromPrimaries {
            max_entries_per_request: Some(2),
        }
    );
    assert_eq!(
        parse_request("CATCH_UP_FROM_PRIMARY\t3").unwrap(),
        BackendRequest::CatchUpFromPrimary {
            server_id: 3,
            max_entries_per_request: None,
        }
    );
    assert_eq!(
        parse_request("CATCH_UP_FROM_PRIMARY\t3\t2").unwrap(),
        BackendRequest::CatchUpFromPrimary {
            server_id: 3,
            max_entries_per_request: Some(2),
        }
    );
    assert_eq!(
        parse_request("CATCH_UP_PLAN").unwrap(),
        BackendRequest::CatchUpPlan { server_id: None }
    );
    assert_eq!(
        parse_request("CATCH_UP_PLAN_PRIMARY\t3").unwrap(),
        BackendRequest::CatchUpPlan { server_id: Some(3) }
    );
    assert!(matches!(
        execute_request(
            &db,
            parse_request("REGISTER_QUERY_PEER\t2\t127.0.0.1:7688").unwrap()
        ),
        BackendResponse::Err(message) if message.contains("backend coordinator")
    ));
    assert!(matches!(
        execute_request(&db, parse_request("CAPABILITIES").unwrap()),
        BackendResponse::OkCapabilities(capabilities)
            if capabilities.contains("ownership_epoch=true")
                && capabilities.contains("snapshot_bootstrap=true")
    ));
    assert!(matches!(
        execute_request(&db, parse_request("CATCH_UP_FROM_PRIMARIES").unwrap()),
        BackendResponse::Err(message) if message.contains("backend coordinator")
    ));

    let _ = fs::remove_dir_all(dir);
}

#[test]
pub(super) fn query_request_can_execute_cypher_write() {
    let dir = temp_dir("server-cypher-write");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 2, 2)).unwrap();

    let response = execute_request(
        &db,
        parse_request("QUERY\tCREATE (n:Person {name: $name}) RETURN n\tname=s:Alice").unwrap(),
    );

    assert!(matches!(response, BackendResponse::OkRows { count: 1, .. }));
    assert_eq!(
        db.query(r#"MATCH (n:Person) WHERE n.name = "Alice" RETURN n"#)
            .unwrap()
            .len(),
        1
    );

    let _ = fs::remove_dir_all(dir);
}

#[test]
pub(super) fn query_plan_request_reports_access_path() {
    let dir = temp_dir("server-query-plan");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1)).unwrap();
    db.execute_cypher("CREATE INDEX person_name FOR (n:Person) ON (n.name)")
        .unwrap();

    let response = execute_request(
        &db,
        parse_request("QUERY_PLAN\tMATCH (n:Person {name: $name}) RETURN n\tname=s:Alice").unwrap(),
    );

    let BackendResponse::OkQueryPlan(plan) = response else {
        panic!("expected query plan response");
    };
    assert!(plan.contains("route=local"));
    assert!(plan.contains("access=node_index_seek(Person.name)"));
    assert_eq!(
        format_response(&BackendResponse::OkQueryPlan(plan.clone())),
        format!("OK\tQUERY_PLAN\t{plan}")
    );

    let _ = fs::remove_dir_all(dir);
}

#[test]
pub(super) fn performance_commands_report_profile_storage_and_statistics() {
    let dir = temp_dir("server-performance-protocol");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1)).unwrap();
    execute_request(
        &db,
        parse_request("QUERY\tCREATE (n:Person {name: $name}) RETURN n\tname=s:Alice").unwrap(),
    );

    let profile = execute_request(
        &db,
        parse_request("PROFILE\tMATCH (n:Person) RETURN n").unwrap(),
    );
    let BackendResponse::OkQueryProfile(profile) = profile else {
        panic!("expected profile response");
    };
    assert!(profile.contains("metrics="));
    assert!(profile.contains("cost="));

    let storage = execute_request(&db, parse_request("STORAGE_STATUS").unwrap());
    let BackendResponse::OkStorageStatus(storage) = storage else {
        panic!("expected storage status");
    };
    assert!(storage.contains("total_bytes="));

    let statistics = execute_request(&db, parse_request("STATISTICS").unwrap());
    let BackendResponse::OkStatistics(statistics) = statistics else {
        panic!("expected statistics");
    };
    assert!(statistics.contains("nodes=1"));
    assert!(statistics.contains("node_properties=[name:1]"));

    let checkpoint = execute_request(&db, parse_request("CHECKPOINT_NOW").unwrap());
    assert!(matches!(
        checkpoint,
        BackendResponse::OkStorageMaintenance(result) if result.contains("action=checkpoint")
    ));

    let compact = execute_request(&db, parse_request("COMPACT_STORAGE").unwrap());
    assert!(matches!(
        compact,
        BackendResponse::OkStorageMaintenance(result) if result.contains("action=compact_observe")
    ));

    let snapshot = execute_request(&db, parse_request("SNAPSHOT_NOW").unwrap());
    assert!(matches!(
        snapshot,
        BackendResponse::OkStorageMaintenance(result) if result.contains("action=snapshot")
    ));

    let verify = execute_request(&db, parse_request("VERIFY_INVARIANTS").unwrap());
    assert!(matches!(
        verify,
        BackendResponse::OkStorageMaintenance(result)
            if result.contains("action=verify_invariants") && result.contains("clean=true")
    ));
    let repair = execute_request(&db, parse_request("REPAIR_INVARIANTS").unwrap());
    assert!(matches!(
        repair,
        BackendResponse::OkStorageMaintenance(result)
            if result.contains("action=repair_invariants")
    ));
    assert_eq!(
        parse_request("VERIFY_INVARIANTS\t0").unwrap_err(),
        "VERIFY_INVARIANTS does not take arguments"
    );

    let backup = execute_request(&db, parse_request("BACKUP_NOW").unwrap());
    assert!(matches!(
        backup,
        BackendResponse::OkStorageMaintenance(result) if result.contains("snapshot_manifest")
    ));

    let restore = execute_request(&db, parse_request("RESTORE_SNAPSHOT\t0").unwrap());
    assert!(matches!(
        restore,
        BackendResponse::OkStorageMaintenance(result) if result.contains("action=restore_snapshot")
    ));
    assert_eq!(
        parse_request("RESTORE_SNAPSHOT").unwrap_err(),
        "RESTORE_SNAPSHOT requires shard id"
    );

    let raft = execute_request(&db, parse_request("RAFT_STATUS").unwrap());
    assert!(matches!(
        raft,
        BackendResponse::OkClusterStatus(result) if result.contains("raft_shards=")
    ));
    assert_eq!(
        parse_request("RAFT_LEADER_TRANSFER\t0\t2").unwrap(),
        BackendRequest::RaftLeaderTransfer {
            shard_id: 0,
            transferee_id: 2
        }
    );
    assert_eq!(
        parse_request("RAFT_LEADER_TRANSFER\t0").unwrap_err(),
        "RAFT_LEADER_TRANSFER requires transferee id"
    );

    let _ = fs::remove_dir_all(dir);
}

#[test]
pub(super) fn remove_property_commands_execute_against_database() {
    let dir = temp_dir("server-remove-property");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 2, 2)).unwrap();
    let alice = db
        .create_node(
            vec!["Person".to_string()],
            [("status".to_string(), Value::String("active".to_string()))]
                .into_iter()
                .collect(),
        )
        .unwrap();
    let bob = db
        .create_node(vec!["Person".to_string()], Properties::new())
        .unwrap();
    let rel = db
        .create_relationship(
            alice,
            bob,
            "KNOWS".to_string(),
            [("weight".to_string(), Value::Int(3))]
                .into_iter()
                .collect(),
        )
        .unwrap();

    assert_eq!(
        execute_request(
            &db,
            BackendRequest::RemoveNodeProperty {
                id: alice,
                key: "status".to_string(),
            },
        ),
        BackendResponse::OkUnit
    );
    assert_eq!(
        execute_request(
            &db,
            BackendRequest::RemoveRelationshipProperty {
                id: rel,
                key: "weight".to_string(),
            },
        ),
        BackendResponse::OkUnit
    );

    assert!(db
        .query(r#"MATCH (n:Person) WHERE n.status = "active" RETURN n"#)
        .unwrap()
        .is_empty());
    assert!(db
        .query(r#"MATCH (a:Person)-[r:KNOWS]->(b:Person) WHERE r.weight = 3 RETURN r"#)
        .unwrap()
        .is_empty());

    let _ = fs::remove_dir_all(dir);
}

#[test]
pub(super) fn command_property_map_values_return_error_before_wal_append() {
    let dir = temp_dir("server-command-map-property-validation");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1)).unwrap();
    let map_value = Value::Map(
        [("nested".to_string(), Value::Bool(true))]
            .into_iter()
            .collect(),
    );
    let encoded_map = hex_encode(
        encode_properties(match &map_value {
            Value::Map(values) => values,
            _ => unreachable!(),
        })
        .as_bytes(),
    );

    let create = execute_request(
        &db,
        parse_request(&format!("CREATE_NODE\tPerson\tprofile=m:{encoded_map}")).unwrap(),
    );

    assert!(matches!(create, BackendResponse::Err(message) if message.contains("nested map")));
    assert!(db.query("MATCH (n:Person) RETURN n").unwrap().is_empty());
    assert_eq!(db.committed_indexes().unwrap(), vec![0]);

    let alice = db
        .create_node(
            vec!["Person".to_string()],
            [("name".to_string(), Value::String("Alice".to_string()))]
                .into_iter()
                .collect(),
        )
        .unwrap();
    let set = execute_request(
        &db,
        parse_request(&format!(
            "SET_NODE_PROPERTY\t{alice}\tprofile\tm:{encoded_map}"
        ))
        .unwrap(),
    );

    assert!(matches!(set, BackendResponse::Err(message) if message.contains("nested map")));
    assert_eq!(db.committed_indexes().unwrap(), vec![1]);

    let _ = fs::remove_dir_all(dir);
}

#[test]
pub(super) fn cluster_status_command_reports_database_positions() {
    let dir = temp_dir("server-cluster-status");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1).with_server_id(1)).unwrap();
    db.create_node(
        vec!["Person".to_string()],
        [("name".to_string(), Value::String("Alice".to_string()))]
            .into_iter()
            .collect(),
    )
    .unwrap();

    let response = execute_request(&db, parse_request("CLUSTER_STATUS").unwrap());
    let text = format_response(&response);

    assert!(text.starts_with("OK\tCLUSTER_STATUS\t"));
    assert!(text.contains("server=1"));
    assert!(text.contains("routing_version=1"));
    assert!(text.contains("shards=1"));
    assert!(text.contains("partitions=1"));
    assert!(text.contains("shard=0"));
    assert!(text.contains("primary=1"));
    assert!(text.contains("local=true"));
    assert!(text.contains("local_primary=true"));
    assert!(text.contains("applied=1"));
    assert!(text.contains("committed=1"));

    let _ = fs::remove_dir_all(dir);
}

#[test]
pub(super) fn cluster_membership_commands_execute_against_database() {
    let dir = temp_dir("server-membership-protocol");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 2, 1).with_server_id(1)).unwrap();

    assert_eq!(
            execute_request(
                &db,
                parse_request("JOIN_REQUEST\t2\t127.0.0.1:17688\t1\t1\t2").unwrap()
            ),
            BackendResponse::OkClusterNodes("version=2 nodes=[1:active::protocol=0:storage=0:shards=2:reason=,2:negotiating:127.0.0.1:17688:protocol=1:storage=1:shards=2:reason=] assignments=[]".to_string())
        );
    assert!(matches!(
        parse_request("JOIN_ACCEPT\t2").unwrap(),
        BackendRequest::JoinAccept(2)
    ));
    execute_request(&db, parse_request("JOIN_ACCEPT\t2").unwrap());
    let response = execute_request(&db, parse_request("LIST_NODES").unwrap());
    let BackendResponse::OkClusterNodes(nodes) = response else {
        panic!("expected cluster nodes");
    };
    assert!(nodes.contains("2:joining:127.0.0.1:17688"));
    assert_eq!(
            execute_request(
                &db,
                parse_request("JOIN_REQUEST\t3\t127.0.0.1:17689\t1\t1\t3").unwrap()
            ),
            BackendResponse::OkClusterNodes("version=4 nodes=[1:active::protocol=0:storage=0:shards=2:reason=,2:joining:127.0.0.1:17688:protocol=1:storage=1:shards=2:reason=,3:rejected:127.0.0.1:17689:protocol=1:storage=1:shards=3:reason=shard count mismatch: requested 3, cluster 2] assignments=[]".to_string())
        );
    let response = execute_request(&db, parse_request("PLAN_REBALANCE").unwrap());
    let BackendResponse::OkRebalancePlan(plan) = response else {
        panic!("expected rebalance plan");
    };
    assert!(plan.contains("ADD_REPLICA\t0\t2"));

    assert!(matches!(
        execute_request(
            &db,
            parse_request("APPLY_REBALANCE_STEP\tADD_REPLICA\t0\t2").unwrap()
        ),
        BackendResponse::Err(message) if message.contains("must be prepared and caught up")
    ));
    let response = execute_request(
        &db,
        parse_request("PREPARE_REBALANCE_STEP\tADD_REPLICA\t0\t2").unwrap(),
    );
    let BackendResponse::OkClusterNodes(nodes) = response else {
        panic!("expected cluster nodes");
    };
    assert!(nodes.contains("state=catching_up"));
    execute_request(&db, parse_request("MARK_SHARD_CAUGHT_UP\t0\t2\t0").unwrap());
    assert_eq!(
        execute_request(
            &db,
            parse_request("APPLY_REBALANCE_STEP\tADD_REPLICA\t0\t2").unwrap()
        ),
        BackendResponse::OkClusterStatus("routing_version=2".to_string())
    );
    let response = execute_request(&db, parse_request("LIST_NODES").unwrap());
    let BackendResponse::OkClusterNodes(nodes) = response else {
        panic!("expected cluster nodes");
    };
    assert!(nodes.contains("2:active:127.0.0.1:17688"));

    let _ = fs::remove_dir_all(dir);
}

#[test]
pub(super) fn cluster_management_commands_report_structured_status() {
    let dir = temp_dir("server-cluster-management-protocol");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1).with_server_id(1)).unwrap();

    let response = execute_request(&db, parse_request("METADATA_AUTHORITY").unwrap());
    let BackendResponse::OkClusterManagementStatus(metadata) = response else {
        panic!("expected metadata status");
    };
    assert!(metadata.contains("authority=1"));
    assert_eq!(
        execute_request(&db, parse_request("SET_REBALANCE_POLICY\t2\t4").unwrap()),
        BackendResponse::OkClusterManagementStatus(
            "authority=1 term=1 config_epoch=1 policy=replication_factor:2:max_steps:4".to_string()
        )
    );

    execute_request(
        &db,
        parse_request("JOIN_REQUEST\t2\t127.0.0.1:17688\t1\t1\t1").unwrap(),
    );
    execute_request(&db, parse_request("JOIN_ACCEPT\t2").unwrap());
    let started = execute_request(&db, parse_request("START_REBALANCE").unwrap());
    let BackendResponse::OkRebalanceExecution(started) = started else {
        panic!("expected rebalance execution");
    };
    assert!(started.contains("state=running"));
    assert!(started.contains("ADD_REPLICA"));

    let advanced = execute_request(&db, parse_request("ADVANCE_REBALANCE").unwrap());
    let BackendResponse::OkRebalanceExecution(advanced) = advanced else {
        panic!("expected rebalance advance");
    };
    assert!(advanced.contains("action=prepared"));

    let status = execute_request(&db, parse_request("CLUSTER_MANAGEMENT_STATUS").unwrap());
    let BackendResponse::OkClusterManagementStatus(status) = status else {
        panic!("expected cluster management status");
    };
    assert!(status.contains("\"metadata\""));
    assert!(status.contains("\"rebalance_execution\""));

    let _ = fs::remove_dir_all(dir);
}
