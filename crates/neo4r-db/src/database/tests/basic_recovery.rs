use neo4r_core::{GraphState, ShardPlacement, ShardReplica, Term, Value};
use neo4r_query::QueryValue;
use std::fs;
use std::net::TcpListener;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[test]
fn creates_nodes_relationships_and_queries_them() {
    let dir = temp_dir("facade-query");
    let mut db = open_test_db(&dir);

    let alice = db
        .create_node(
            vec!["Person".to_string()],
            properties(&[("name", Value::String("Alice".to_string()))]),
        )
        .unwrap();
    let bob = db
        .create_node(
            vec!["Person".to_string()],
            properties(&[("name", Value::String("Bob".to_string()))]),
        )
        .unwrap();
    let relationship = db
        .create_relationship(alice, bob, "KNOWS".to_string(), Properties::new())
        .unwrap();

    assert_eq!(relationship, 0);
    let rows = db
        .query(r#"MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE a.name = "Alice" RETURN b.name"#)
        .unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get("b.name"),
        Some(&QueryValue::Scalar(Value::String("Bob".to_string())))
    );

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn reopens_and_replays_segmented_logs() {
    let dir = temp_dir("facade-reopen");
    {
        let mut db = open_test_db(&dir);
        for name in ["Alice", "Bob", "Carol"] {
            db.create_node(
                vec!["Person".to_string()],
                properties(&[("name", Value::String(name.to_string()))]),
            )
            .unwrap();
        }
    }

    {
        let mut db = open_test_db(&dir);
        let rows = db
            .query(r#"MATCH (n:Person) WHERE n.name = "Carol" RETURN n.name"#)
            .unwrap();

        assert_eq!(rows.len(), 1);
        let dave = db
            .create_node(
                vec!["Person".to_string()],
                properties(&[("name", Value::String("Dave".to_string()))]),
            )
            .unwrap();
        assert_eq!(dave, 3);
    }

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn reopens_and_rebuilds_vector_search_from_replayed_properties() {
    let dir = temp_dir("facade-vector-reopen");
    {
        let mut db = open_test_db(&dir);
        db.create_node(
            vec!["Document".to_string()],
            properties(&[
                ("title", Value::String("near".to_string())),
                ("embedding", Value::Vector(vec![1.0, 0.0])),
            ]),
        )
        .unwrap();
        db.create_node(
            vec!["Document".to_string()],
            properties(&[
                ("title", Value::String("far".to_string())),
                ("embedding", Value::Vector(vec![0.0, 1.0])),
            ]),
        )
        .unwrap();
    }

    {
        let db = open_test_db(&dir);
        let rows = db
            .query("MATCH (n:Document) WHERE vector.knn(n.embedding, [1.0, 0.0], 1) RETURN n.title")
            .unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].get("n.title"),
            Some(&QueryValue::Scalar(Value::String("near".to_string())))
        );
    }

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn updates_and_deletes_nodes_through_durable_api() {
    let dir = temp_dir("facade-node-cud");
    {
        let mut db = open_test_db(&dir);
        let alice = db
            .create_node(
                vec!["Person".to_string()],
                properties(&[("name", Value::String("Alice".to_string()))]),
            )
            .unwrap();

        db.set_node_property(
            alice,
            "name".to_string(),
            Value::String("Alicia".to_string()),
        )
        .unwrap();

        let rows = db
            .query(r#"MATCH (n:Person) WHERE n.name = "Alicia" RETURN n.name"#)
            .unwrap();
        assert_eq!(rows.len(), 1);

        db.delete_node(alice).unwrap();
    }

    {
        let db = open_test_db(&dir);
        let rows = db.query("MATCH (n:Person) RETURN n").unwrap();
        assert!(rows.is_empty());
    }

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn execute_cypher_adds_and_removes_node_labels() {
    let dir = temp_dir("facade-cypher-label-cud");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 2)).unwrap();
    db.execute_cypher(r#"CREATE (n:Person {name: "Alice"})"#)
        .unwrap();

    let rows = db
        .execute_cypher(r#"MATCH (n:Person) WHERE n.name = "Alice" SET n:Employee RETURN n"#)
        .unwrap();
    assert_eq!(rows.len(), 1);
    let QueryValue::Node(node) = rows[0].get("n").unwrap() else {
        panic!("expected node row");
    };
    assert!(node.labels.iter().any(|label| label == "Person"));
    assert!(node.labels.iter().any(|label| label == "Employee"));

    assert_eq!(
        db.query(r#"MATCH (n:Employee) WHERE n.name = "Alice" RETURN n.name"#)
            .unwrap()
            .len(),
        1
    );

    let rows = db
        .execute_cypher(r#"MATCH (n:Employee) WHERE n.name = "Alice" REMOVE n:Person RETURN n"#)
        .unwrap();
    assert_eq!(rows.len(), 1);
    let QueryValue::Node(node) = rows[0].get("n").unwrap() else {
        panic!("expected node row");
    };
    assert!(!node.labels.iter().any(|label| label == "Person"));
    assert!(node.labels.iter().any(|label| label == "Employee"));
    assert!(db
        .query(r#"MATCH (n:Person) WHERE n.name = "Alice" RETURN n"#)
        .unwrap()
        .is_empty());

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn reopens_and_replays_node_label_updates() {
    let dir = temp_dir("facade-label-replay");
    {
        let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 2)).unwrap();
        db.execute_cypher(r#"CREATE (n:Person {name: "Alice"})"#)
            .unwrap();
        db.execute_cypher(r#"MATCH (n:Person) WHERE n.name = "Alice" SET n:Employee"#)
            .unwrap();
    }

    {
        let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 2)).unwrap();
        assert_eq!(
            db.query(r#"MATCH (n:Employee) WHERE n.name = "Alice" RETURN n.name"#)
                .unwrap()
                .len(),
            1
        );
    }

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn adding_label_validates_vector_indexed_property_shape() {
    let dir = temp_dir("facade-label-vector-validation");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 2)).unwrap();
    db.execute_cypher(
        "CREATE VECTOR INDEX doc_embedding ON :Document(embedding) DIMENSIONS 2 METRIC cosine",
    )
    .unwrap();
    db.create_node(
        Vec::new(),
        properties(&[("embedding", Value::Vector(vec![1.0]))]),
    )
    .unwrap();

    let err = db.execute_cypher("MATCH (n) SET n:Document").unwrap_err();
    assert!(matches!(err, DatabaseError::InvalidConfig(_)));
    assert!(db.query("MATCH (n:Document) RETURN n").unwrap().is_empty());

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn staged_overlay_reads_node_label_updates() {
    let dir = temp_dir("facade-staged-label-overlay");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 2)).unwrap();
    db.execute_cypher(r#"CREATE (n:Person {name: "Alice"})"#)
        .unwrap();
    let staged = vec![(
        r#"MATCH (n:Person) WHERE n.name = "Alice" SET n:Employee"#.to_string(),
        QueryParams::new(),
    )];
    let mut cursor = db
        .query_cursor_with_staged_writes(
            r#"MATCH (n:Employee) WHERE n.name = "Alice" RETURN n.name"#,
            QueryParams::new(),
            QueryOptions::default(),
            &staged,
        )
        .unwrap();

    let rows = cursor.fetch(10).rows;
    assert_eq!(rows.len(), 1);
    assert!(db
        .query(r#"MATCH (n:Employee) WHERE n.name = "Alice" RETURN n"#)
        .unwrap()
        .is_empty());

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn staged_overlay_reads_property_map_replacements() {
    let dir = temp_dir("facade-staged-replace-map-overlay");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 2)).unwrap();
    db.execute_cypher(r#"CREATE (n:Person {name: "Alice", stale: true})"#)
        .unwrap();
    let staged = vec![(
        r#"MATCH (n:Person) WHERE n.name = "Alice" SET n = {name: "Alice", status: "active"}"#
            .to_string(),
        QueryParams::new(),
    )];
    let mut cursor = db
        .query_cursor_with_staged_writes(
            r#"MATCH (n:Person) WHERE n.status = "active" RETURN n.stale"#,
            QueryParams::new(),
            QueryOptions::default(),
            &staged,
        )
        .unwrap();

    let rows = cursor.fetch(10).rows;
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get("n.stale"),
        Some(&QueryValue::Scalar(Value::Null))
    );
    assert!(db
        .query(r#"MATCH (n:Person) WHERE n.status = "active" RETURN n"#)
        .unwrap()
        .is_empty());

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn updates_and_deletes_relationships_through_durable_api() {
    let dir = temp_dir("facade-relationship-cud");
    {
        let mut db = open_test_db(&dir);
        let alice = db
            .create_node(
                vec!["Person".to_string()],
                properties(&[("name", Value::String("Alice".to_string()))]),
            )
            .unwrap();
        let bob = db
            .create_node(
                vec!["Person".to_string()],
                properties(&[("name", Value::String("Bob".to_string()))]),
            )
            .unwrap();
        let relationship = db
            .create_relationship(alice, bob, "KNOWS".to_string(), Properties::new())
            .unwrap();

        db.set_relationship_property(relationship, "since".to_string(), Value::Int(2026))
            .unwrap();
        db.delete_relationship(relationship).unwrap();
    }

    {
        let db = open_test_db(&dir);
        let rows = db
            .query(r#"MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN b.name"#)
            .unwrap();
        assert!(rows.is_empty());
    }

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn rejects_writes_that_reference_missing_records() {
    let dir = temp_dir("facade-missing-records");
    let mut db = open_test_db(&dir);

    let create_relationship =
        db.create_relationship(10, 11, "KNOWS".to_string(), Properties::new());
    assert!(matches!(
        create_relationship,
        Err(DatabaseError::Graph(GraphError::NodeNotFound(10)))
    ));
    assert!(matches!(
        db.set_node_property(10, "name".to_string(), Value::String("Ghost".to_string())),
        Err(DatabaseError::Graph(GraphError::NodeNotFound(10)))
    ));
    assert!(matches!(
        db.delete_relationship(99),
        Err(DatabaseError::Graph(GraphError::RelationshipNotFound(99)))
    ));

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn writes_segment_files_by_shard_log_position() {
    let dir = temp_dir("facade-segments");
    {
        let mut db = open_test_db(&dir);
        db.create_node(vec!["Person".to_string()], Properties::new())
            .unwrap();
        db.create_node(vec!["Person".to_string()], Properties::new())
            .unwrap();
        db.create_node(vec!["Person".to_string()], Properties::new())
            .unwrap();
    }

    assert!(dir
        .join("shards/0/segments/00000000000000000001.log")
        .is_file());
    assert!(dir
        .join("shards/0/segments/00000000000000000003.log")
        .is_file());

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn replays_wal_entry_written_after_checkpoint_but_before_store_apply() {
    let dir = temp_dir("facade-wal-replay");
    let config = DatabaseConfig::new(&dir, 1, 2)
        .with_log_entries_per_segment(2)
        .with_checkpoint_interval(1);

    {
        let mut db = Neo4rDatabase::open(config.clone()).unwrap();
        db.create_node(
            vec!["Person".to_string()],
            properties(&[("name", Value::String("Alice".to_string()))]),
        )
        .unwrap();

        db.log(0)
            .unwrap()
            .append(&LogEntry::new(
                0,
                0,
                2,
                Command::CreateNode {
                    id: 1,
                    labels: vec!["Person".to_string()],
                    properties: properties(&[("name", Value::String("Bob".to_string()))]),
                },
            ))
            .unwrap();
        db.commits[0].save(0, 2).unwrap();
    }

    {
        let db = Neo4rDatabase::open(config).unwrap();
        let rows = db
            .query(r#"MATCH (n:Person) WHERE n.name = "Bob" RETURN n.name"#)
            .unwrap();

        assert_eq!(rows.len(), 1);
    }

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn failure_injection_after_commit_before_apply_recovers_on_reopen() {
    let dir = temp_dir("facade-fail-after-commit-before-apply");
    let config = DatabaseConfig::new(&dir, 1, 1).with_failure_injection(FailureInjection {
        fail_after_commit_before_apply: true,
        ..FailureInjection::default()
    });
    {
        let mut db = Neo4rDatabase::open(config).unwrap();
        let err = db
            .create_node(
                vec!["Person".to_string()],
                properties(&[("name", Value::String("Alice".to_string()))]),
            )
            .unwrap_err();
        assert!(
            matches!(err, DatabaseError::Replication(message) if message.contains("injected failure"))
        );
        assert_eq!(
            db.query(r#"MATCH (n:Person) WHERE n.name = "Alice" RETURN n.name"#)
                .unwrap()
                .len(),
            0
        );
    }

    let db = Neo4rDatabase::open(DatabaseConfig::new(&dir, 1, 1)).unwrap();
    assert_eq!(
        db.query(r#"MATCH (n:Person) WHERE n.name = "Alice" RETURN n.name"#)
            .unwrap()
            .len(),
        1
    );

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn concurrent_handle_serializes_writes_and_assigns_unique_ids() {
    let dir = temp_dir("facade-concurrent-writes");
    let db = Neo4rDatabaseHandle::open(
        DatabaseConfig::new(&dir, 4, 2)
            .with_log_entries_per_segment(128)
            .with_checkpoint_interval(128)
            .with_wal_sync_interval(128),
    )
    .unwrap();
    let thread_count = 4;
    let writes_per_thread = 50;
    let barrier = Arc::new(Barrier::new(thread_count));

    let mut workers = Vec::new();
    for worker_id in 0..thread_count {
        let db = db.clone();
        let barrier = barrier.clone();
        workers.push(thread::spawn(move || {
            barrier.wait();
            let mut ids = Vec::new();
            for local_id in 0..writes_per_thread {
                ids.push(
                    db.create_node(
                        vec!["Person".to_string()],
                        properties(&[
                            ("worker", Value::Int(worker_id as i64)),
                            ("local_id", Value::Int(local_id as i64)),
                        ]),
                    )
                    .unwrap(),
                );
            }
            ids
        }));
    }

    let mut ids = Vec::new();
    for worker in workers {
        ids.extend(worker.join().unwrap());
    }
    ids.sort_unstable();
    ids.dedup();

    assert_eq!(ids.len(), thread_count * writes_per_thread);
    assert_eq!(ids[0], 0);
    assert_eq!(
        ids[ids.len() - 1],
        (thread_count * writes_per_thread - 1) as u64
    );
    assert_eq!(
        db.query("MATCH (n:Person) RETURN n").unwrap().len(),
        thread_count * writes_per_thread
    );

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn group_commit_batches_concurrent_local_writes() {
    let dir = temp_dir("facade-group-commit");
    let write_count = 8;
    let db = Neo4rDatabaseHandle::open(
        DatabaseConfig::new(&dir, 1, 2)
            .with_group_commit_max_entries(write_count)
            .with_group_commit_max_delay(Duration::from_millis(20)),
    )
    .unwrap();
    let barrier = Arc::new(Barrier::new(write_count));

    let mut workers = Vec::new();
    for worker_id in 0..write_count {
        let db = db.clone();
        let barrier = barrier.clone();
        workers.push(thread::spawn(move || {
            barrier.wait();
            db.create_node(
                vec!["Person".to_string()],
                properties(&[("worker", Value::Int(worker_id as i64))]),
            )
            .unwrap()
        }));
    }

    let mut ids = Vec::new();
    for worker in workers {
        ids.push(worker.join().unwrap());
    }
    ids.sort_unstable();

    assert_eq!(ids, (0..write_count as u64).collect::<Vec<_>>());
    assert_eq!(db.committed_indexes().unwrap(), vec![write_count as u64]);
    assert_eq!(
        db.query("MATCH (n:Person) RETURN n").unwrap().len(),
        write_count
    );

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn execute_cypher_applies_index_ddl_to_catalog() {
    let dir = temp_dir("facade-cypher-index-ddl");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1)).unwrap();

    db.execute_cypher("CREATE INDEX person_name FOR (n:Person) ON (n.name)")
        .unwrap();
    db.execute_cypher(
        "CREATE VECTOR INDEX doc_embedding ON :Document(embedding) DIMENSIONS 2 METRIC cosine",
    )
    .unwrap();
    db.execute_cypher(
        "CREATE CONSTRAINT person_email_unique FOR (n:Person) REQUIRE n.email IS UNIQUE",
    )
    .unwrap();

    let version = db.index_catalog().unwrap().version;
    db.execute_cypher("CREATE INDEX person_name IF NOT EXISTS FOR (n:Person) ON (n.name)")
        .unwrap();
    db.execute_cypher(
            "CREATE VECTOR INDEX doc_embedding IF NOT EXISTS ON :Document(embedding) DIMENSIONS 2 METRIC cosine",
        )
        .unwrap();
    db.execute_cypher(
            "CREATE CONSTRAINT person_email_unique IF NOT EXISTS FOR (n:Person) REQUIRE n.email IS UNIQUE",
        )
        .unwrap();
    assert_eq!(db.index_catalog().unwrap().version, version);
    assert!(matches!(
        db.execute_cypher(
            "CREATE INDEX person_name IF NOT EXISTS FOR (n:Person) ON (n.nickname)"
        ),
        Err(DatabaseError::InvalidConfig(message)) if message.contains("different definition")
    ));
    db.execute_cypher("REBUILD VECTOR INDEX doc_embedding")
        .unwrap();
    assert!(matches!(
        db.execute_cypher("REBUILD VECTOR INDEX missing_vector"),
        Err(DatabaseError::InvalidConfig(message)) if message.contains("does not exist")
    ));
    assert!(matches!(
        db.execute_cypher("REBUILD VECTOR INDEX doc_embedding extra"),
        Err(DatabaseError::Query(QueryError::Parse(message))) if message.contains("single index name")
    ));

    let indexes = db.list_indexes().unwrap();
    assert_eq!(indexes.len(), 3);
    assert_eq!(indexes[0].name, "person_name");
    assert_eq!(indexes[0].label, "Person");
    assert_eq!(indexes[0].property, "name");
    assert_eq!(indexes[0].kind, IndexKind::NodeProperty);
    assert_eq!(indexes[1].name, "doc_embedding");
    assert_eq!(indexes[1].label, "Document");
    assert_eq!(indexes[1].property, "embedding");
    assert_eq!(
        indexes[1].kind,
        IndexKind::Vector {
            dimensions: 2,
            metric: "cosine".to_string()
        }
    );
    assert_eq!(indexes[2].name, "person_email_unique");
    assert_eq!(indexes[2].label, "Person");
    assert_eq!(indexes[2].property, "email");
    assert_eq!(indexes[2].kind, IndexKind::UniqueNodeProperty);

    let rows = db.execute_cypher("SHOW INDEXES").unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(
        rows[0].get("name"),
        Some(&QueryValue::Scalar(Value::String(
            "person_name".to_string()
        )))
    );
    assert_eq!(
        rows[0].get("type"),
        Some(&QueryValue::Scalar(Value::String("node".to_string())))
    );
    assert_eq!(
        rows[0].get("state"),
        Some(&QueryValue::Scalar(Value::String("ready".to_string())))
    );
    assert_eq!(
        rows[1].get("type"),
        Some(&QueryValue::Scalar(Value::String("vector".to_string())))
    );
    assert_eq!(
        rows[1].get("dimensions"),
        Some(&QueryValue::Scalar(Value::Int(2)))
    );
    assert_eq!(
        rows[1].get("metric"),
        Some(&QueryValue::Scalar(Value::String("cosine".to_string())))
    );
    let single_index = db.execute_cypher("SHOW INDEX person_name").unwrap();
    assert_eq!(single_index.len(), 1);
    assert_eq!(
        single_index[0].get("name"),
        Some(&QueryValue::Scalar(Value::String(
            "person_name".to_string()
        )))
    );
    assert_eq!(
        single_index[0].get("type"),
        Some(&QueryValue::Scalar(Value::String("node".to_string())))
    );
    let lifecycle = db.index_lifecycle_status().unwrap();
    assert_eq!(lifecycle.len(), 3);
    assert!(lifecycle.iter().all(|status| status.state == "ready"));
    assert!(matches!(
        db.execute_cypher("SHOW INDEX missing_index"),
        Err(DatabaseError::InvalidConfig(message)) if message.contains("does not exist")
    ));
    assert!(matches!(
        db.execute_cypher("SHOW INDEX person_name extra"),
        Err(DatabaseError::Query(QueryError::Parse(message))) if message.contains("single index name")
    ));
    let vector_rows = db.execute_cypher("SHOW VECTOR INDEXES").unwrap();
    assert_eq!(vector_rows.len(), 1);
    assert_eq!(
        vector_rows[0].get("name"),
        Some(&QueryValue::Scalar(Value::String(
            "doc_embedding".to_string()
        )))
    );
    assert_eq!(
        vector_rows[0].get("type"),
        Some(&QueryValue::Scalar(Value::String("vector".to_string())))
    );
    let vector_row = db
        .execute_cypher("SHOW VECTOR INDEX doc_embedding")
        .unwrap();
    assert_eq!(vector_row.len(), 1);
    assert_eq!(
        vector_row[0].get("name"),
        Some(&QueryValue::Scalar(Value::String(
            "doc_embedding".to_string()
        )))
    );
    assert!(matches!(
        db.execute_cypher("SHOW VECTOR INDEX person_name"),
        Err(DatabaseError::InvalidConfig(message)) if message.contains("not a vector index")
    ));
    assert!(matches!(
        db.execute_cypher("SHOW VECTOR INDEX missing_vector"),
        Err(DatabaseError::InvalidConfig(message)) if message.contains("does not exist")
    ));
    assert!(matches!(
        db.execute_cypher("SHOW VECTOR INDEX doc_embedding extra"),
        Err(DatabaseError::Query(QueryError::Parse(message))) if message.contains("single index name")
    ));
    let vector_status = db.execute_cypher("SHOW VECTOR INDEX STATUS").unwrap();
    assert_eq!(vector_status.len(), 1);
    assert_eq!(
        vector_status[0].get("name"),
        Some(&QueryValue::Scalar(Value::String(
            "doc_embedding".to_string()
        )))
    );
    assert_eq!(
        vector_status[0].get("entries"),
        Some(&QueryValue::Scalar(Value::Int(0)))
    );
    let vector_status = db
        .execute_cypher("SHOW VECTOR INDEX STATUS doc_embedding")
        .unwrap();
    assert_eq!(vector_status.len(), 1);
    assert_eq!(
        vector_status[0].get("metric"),
        Some(&QueryValue::Scalar(Value::String("cosine".to_string())))
    );
    assert!(matches!(
        db.execute_cypher("SHOW VECTOR INDEX STATUS missing_vector"),
        Err(DatabaseError::InvalidConfig(message)) if message.contains("does not exist")
    ));
    assert!(matches!(
        db.execute_cypher("SHOW VECTOR INDEX STATUS doc_embedding extra"),
        Err(DatabaseError::Query(QueryError::Parse(message))) if message.contains("single index name")
    ));
    let mut vector_cursor = db.query_cursor("SHOW VECTOR INDEXES").unwrap();
    let vector_page = vector_cursor.fetch(10);
    assert_eq!(vector_page.rows.len(), 1);
    assert!(!vector_page.has_more);
    let mut vector_status_cursor = db.query_cursor("SHOW VECTOR INDEX STATUS").unwrap();
    let vector_status_page = vector_status_cursor.fetch(10);
    assert_eq!(vector_status_page.rows.len(), 1);
    assert!(!vector_status_page.has_more);
    let mut cursor = db.query_cursor("SHOW INDEXES").unwrap();
    let page = cursor.fetch(2);
    assert_eq!(page.rows.len(), 2);
    assert!(page.has_more);
    let constraints = db.execute_cypher("SHOW CONSTRAINTS").unwrap();
    assert_eq!(constraints.len(), 1);
    assert_eq!(
        constraints[0].get("name"),
        Some(&QueryValue::Scalar(Value::String(
            "person_email_unique".to_string()
        )))
    );
    let constraint = db
        .execute_cypher("SHOW CONSTRAINT person_email_unique")
        .unwrap();
    assert_eq!(constraint.len(), 1);
    assert_eq!(
        constraint[0].get("name"),
        Some(&QueryValue::Scalar(Value::String(
            "person_email_unique".to_string()
        )))
    );
    assert!(matches!(
        db.execute_cypher("SHOW CONSTRAINT person_name"),
        Err(DatabaseError::InvalidConfig(message)) if message.contains("not a constraint")
    ));
    assert!(matches!(
        db.execute_cypher("SHOW CONSTRAINT missing_constraint"),
        Err(DatabaseError::InvalidConfig(message)) if message.contains("does not exist")
    ));
    assert!(matches!(
        db.execute_cypher("SHOW CONSTRAINT person_email_unique extra"),
        Err(DatabaseError::Query(QueryError::Parse(message))) if message.contains("single constraint name")
    ));
    assert_eq!(
        constraints[0].get("type"),
        Some(&QueryValue::Scalar(Value::String(
            "unique_node_property".to_string()
        )))
    );
    assert_eq!(
        constraints[0].get("property"),
        Some(&QueryValue::Scalar(Value::String("email".to_string())))
    );

    let err = db
        .execute_cypher("DROP CONSTRAINT doc_embedding")
        .unwrap_err();
    assert!(matches!(
        err,
        DatabaseError::InvalidConfig(message) if message.contains("is not a constraint")
    ));
    let version = db.index_catalog().unwrap().version;
    db.execute_cypher("DROP CONSTRAINT missing_constraint IF EXISTS")
        .unwrap();
    db.execute_cypher("DROP INDEX missing_index IF EXISTS")
        .unwrap();
    assert_eq!(db.index_catalog().unwrap().version, version);
    let err = db
        .execute_cypher("DROP CONSTRAINT doc_embedding IF EXISTS")
        .unwrap_err();
    assert!(matches!(
        err,
        DatabaseError::InvalidConfig(message) if message.contains("is not a constraint")
    ));

    db.execute_cypher("DROP CONSTRAINT person_email_unique")
        .unwrap();
    assert!(db.execute_cypher("SHOW CONSTRAINTS").unwrap().is_empty());

    db.execute_cypher("DROP INDEX person_name").unwrap();
    db.execute_cypher("DROP INDEX person_name IF EXISTS")
        .unwrap();
    db.execute_cypher("DROP CONSTRAINT person_email_unique IF EXISTS")
        .unwrap();
    let indexes = db.list_indexes().unwrap();
    assert_eq!(indexes.len(), 1);
    assert_eq!(indexes[0].name, "doc_embedding");

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn unique_node_property_constraint_rejects_duplicate_writes() {
    let dir = temp_dir("facade-unique-node-property");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1)).unwrap();
    db.execute_cypher(
        "CREATE CONSTRAINT person_email_unique FOR (n:Person) REQUIRE n.email IS UNIQUE",
    )
    .unwrap();
    db.create_node(
        vec!["Person".to_string()],
        properties(&[("email", Value::String("alice@example.com".to_string()))]),
    )
    .unwrap();

    assert!(matches!(
        db.create_node(
            vec!["Person".to_string()],
            properties(&[("email", Value::String("alice@example.com".to_string()))]),
        ),
        Err(DatabaseError::InvalidConfig(_))
    ));
    let bob = db
        .create_node(
            vec!["Person".to_string()],
            properties(&[("email", Value::String("bob@example.com".to_string()))]),
        )
        .unwrap();
    assert!(matches!(
        db.set_node_property(
            bob,
            "email".to_string(),
            Value::String("alice@example.com".to_string()),
        ),
        Err(DatabaseError::InvalidConfig(_))
    ));

    let _ = fs::remove_dir_all(dir);
}
