use super::*;
use neo4r_core::{ShardPlacement, ShardReplica, Value};
use neo4r_query::QueryValue;
use std::fs;
use std::net::TcpListener;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};

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

#[test]
fn unique_node_property_constraint_validates_existing_data_and_persists() {
    let dir = temp_dir("facade-unique-node-property-reopen");
    {
        let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1)).unwrap();
        db.create_node(
            vec!["Person".to_string()],
            properties(&[("email", Value::String("alice@example.com".to_string()))]),
        )
        .unwrap();
        db.create_node(
            vec!["Person".to_string()],
            properties(&[("email", Value::String("bob@example.com".to_string()))]),
        )
        .unwrap();
        db.execute_cypher(
            "CREATE CONSTRAINT person_email_unique FOR (n:Person) REQUIRE n.email IS UNIQUE",
        )
        .unwrap();
    }

    {
        let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1)).unwrap();
        assert_eq!(
            db.list_indexes().unwrap(),
            vec![IndexDefinition::unique_node_property(
                "person_email_unique",
                "Person",
                "email"
            )]
        );
        assert!(matches!(
            db.create_node(
                vec!["Person".to_string()],
                properties(&[("email", Value::String("alice@example.com".to_string()))]),
            ),
            Err(DatabaseError::InvalidConfig(_))
        ));
    }

    let duplicate_dir = temp_dir("facade-unique-node-property-existing-duplicate");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&duplicate_dir, 1, 1)).unwrap();
    db.create_node(
        vec!["Person".to_string()],
        properties(&[("email", Value::String("dupe@example.com".to_string()))]),
    )
    .unwrap();
    db.create_node(
        vec!["Person".to_string()],
        properties(&[("email", Value::String("dupe@example.com".to_string()))]),
    )
    .unwrap();
    assert!(matches!(
        db.execute_cypher(
            "CREATE CONSTRAINT person_email_unique FOR (n:Person) REQUIRE n.email IS UNIQUE",
        ),
        Err(DatabaseError::InvalidConfig(_))
    ));

    let _ = fs::remove_dir_all(dir);
    let _ = fs::remove_dir_all(duplicate_dir);
}

#[test]
fn merge_node_lookup_prefers_unique_constraint_index() {
    let dir = temp_dir("facade-merge-node-index-lookup");
    let mut db = Neo4rDatabase::open(DatabaseConfig::new(&dir, 1, 1)).unwrap();
    db.create_node_property_index("person_tenant", "Person", "tenant")
        .unwrap();
    db.create_unique_node_property_constraint("person_email_unique", "Person", "email")
        .unwrap();

    let labels = vec!["Person".to_string()];
    let props = properties(&[
        ("tenant", Value::String("acme".to_string())),
        ("email", Value::String("alice@example.com".to_string())),
    ]);
    let key = db.merge_node_lookup_key(&labels, &props).unwrap();

    assert_eq!(key.0, "Person");
    assert_eq!(key.1, "email");
    assert_eq!(key.2, &Value::String("alice@example.com".to_string()));

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn concurrent_handle_allows_safe_read_write_access() {
    let dir = temp_dir("facade-concurrent-read-write");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 2, 2)).unwrap();
    let writer = {
        let db = db.clone();
        thread::spawn(move || {
            for id in 0..100 {
                db.create_node(
                    vec!["Person".to_string()],
                    properties(&[("name", Value::String(format!("user-{id}")))]),
                )
                .unwrap();
            }
        })
    };
    let reader = {
        let db = db.clone();
        thread::spawn(move || {
            for _ in 0..25 {
                db.query("MATCH (n:Person) RETURN n").unwrap();
            }
        })
    };

    writer.join().unwrap();
    reader.join().unwrap();

    assert_eq!(db.query("MATCH (n:Person) RETURN n").unwrap().len(), 100);

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn read_snapshot_keeps_query_view_stable_while_writes_continue() {
    let dir = temp_dir("facade-snapshot-isolation");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 2, 2)).unwrap();
    db.create_node(
        vec!["Person".to_string()],
        properties(&[("name", Value::String("Alice".to_string()))]),
    )
    .unwrap();

    let snapshot = db.read_snapshot().unwrap();
    assert!(snapshot.timestamp() > HybridTimestamp::zero());
    assert_eq!(snapshot.applied_indexes(), &[1, 0]);

    db.create_node(
        vec!["Person".to_string()],
        properties(&[("name", Value::String("Bob".to_string()))]),
    )
    .unwrap();

    assert_eq!(
        snapshot.query("MATCH (n:Person) RETURN n").unwrap().len(),
        1
    );
    assert_eq!(db.query("MATCH (n:Person) RETURN n").unwrap().len(), 2);

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn read_committed_is_statement_scoped() {
    let dir = temp_dir("facade-read-committed-isolation");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 2, 2)).unwrap();
    let options = QueryOptions::default().with_isolation(ReadIsolation::ReadCommitted);

    db.create_node(
        vec!["Person".to_string()],
        properties(&[("name", Value::String("Alice".to_string()))]),
    )
    .unwrap();

    assert_eq!(
        db.query_with_options("MATCH (n:Person) RETURN n", options)
            .unwrap()
            .len(),
        1
    );

    db.create_node(
        vec!["Person".to_string()],
        properties(&[("name", Value::String("Bob".to_string()))]),
    )
    .unwrap();

    assert_eq!(
        db.query_with_options("MATCH (n:Person) RETURN n", options)
            .unwrap()
            .len(),
        2
    );

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn snapshot_read_transaction_reuses_one_view_across_queries() {
    let dir = temp_dir("facade-read-transaction-isolation");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 2, 2)).unwrap();
    db.create_node(
        vec!["Person".to_string()],
        properties(&[("name", Value::String("Alice".to_string()))]),
    )
    .unwrap();

    let tx = db.begin_read_transaction().unwrap();
    assert_eq!(tx.options().isolation, ReadIsolation::Snapshot);

    db.create_node(
        vec!["Person".to_string()],
        properties(&[("name", Value::String("Bob".to_string()))]),
    )
    .unwrap();

    assert_eq!(tx.query("MATCH (n:Person) RETURN n").unwrap().len(), 1);
    assert_eq!(
        tx.query(r#"MATCH (n:Person) WHERE n.name = "Bob" RETURN n"#)
            .unwrap()
            .len(),
        0
    );
    assert_eq!(db.query("MATCH (n:Person) RETURN n").unwrap().len(), 2);

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn query_cursor_owns_snapshot_view() {
    let dir = temp_dir("facade-cursor-snapshot");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 2, 2)).unwrap();
    db.create_node(
        vec!["Person".to_string()],
        properties(&[("name", Value::String("Alice".to_string()))]),
    )
    .unwrap();

    let mut cursor = db.query_cursor("MATCH (n:Person) RETURN n").unwrap();

    db.create_node(
        vec!["Person".to_string()],
        properties(&[("name", Value::String("Bob".to_string()))]),
    )
    .unwrap();

    let page = cursor.fetch(10);
    assert_eq!(cursor.total_rows(), Some(1));
    assert_eq!(page.rows.len(), 1);
    assert!(!page.has_more);
    assert_eq!(db.query("MATCH (n:Person) RETURN n").unwrap().len(), 2);

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn query_with_params_filters_property_and_vector_search() {
    let dir = temp_dir("facade-query-params");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 2, 2)).unwrap();
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

    let mut params = QueryParams::new();
    params.insert("title".to_string(), Value::String("near".to_string()));
    assert_eq!(
        db.query_with_params(
            "MATCH (n:Document) WHERE n.title = $title RETURN n.title",
            params,
        )
        .unwrap()
        .len(),
        1
    );

    let mut params = QueryParams::new();
    params.insert("embedding".to_string(), Value::Vector(vec![1.0, 0.0]));
    params.insert("k".to_string(), Value::Int(1));
    params.insert("metric".to_string(), Value::String("cosine".to_string()));
    let rows = db
            .query_with_params(
                "MATCH (n:Document) WHERE vector.knn(n.embedding, $embedding, $k, $metric) RETURN n.title",
                params,
            )
            .unwrap();
    assert_eq!(
        rows[0].get("n.title"),
        Some(&QueryValue::Scalar(Value::String("near".to_string())))
    );

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn query_orders_skips_and_limits_results() {
    let dir = temp_dir("facade-query-result-modifiers");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 2, 2)).unwrap();
    for (name, score) in [("Alice", 30), ("Bob", 10), ("Carol", 20), ("Dave", 40)] {
        db.create_node(
            vec!["Person".to_string()],
            properties(&[
                ("name", Value::String(name.to_string())),
                ("score", Value::Int(score)),
            ]),
        )
        .unwrap();
    }

    let rows = db
        .query("MATCH (n:Person) RETURN n.name ORDER BY n.score DESC SKIP 1 LIMIT 2")
        .unwrap();

    assert_eq!(rows.len(), 2);
    assert_eq!(
        rows[0].get("n.name"),
        Some(&QueryValue::Scalar(Value::String("Alice".to_string())))
    );
    assert_eq!(
        rows[1].get("n.name"),
        Some(&QueryValue::Scalar(Value::String("Carol".to_string())))
    );

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn query_filters_with_comparison_predicates() {
    let dir = temp_dir("facade-query-comparison-predicates");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 2, 2)).unwrap();
    for (name, score) in [("Alice", 30), ("Bob", 10), ("Carol", 20), ("Dave", 40)] {
        db.create_node(
            vec!["Person".to_string()],
            properties(&[
                ("name", Value::String(name.to_string())),
                ("score", Value::Int(score)),
            ]),
        )
        .unwrap();
    }

    let mut params = QueryParams::new();
    params.insert("min".to_string(), Value::Int(20));
    params.insert("max".to_string(), Value::Int(40));
    let rows = db
            .query_with_params(
                "MATCH (n:Person) WHERE n.score >= $min AND n.score < $max RETURN n.name ORDER BY n.score ASC",
                params,
            )
            .unwrap();

    assert_eq!(rows.len(), 2);
    assert_eq!(
        rows[0].get("n.name"),
        Some(&QueryValue::Scalar(Value::String("Carol".to_string())))
    );
    assert_eq!(
        rows[1].get("n.name"),
        Some(&QueryValue::Scalar(Value::String("Alice".to_string())))
    );

    let mut params = QueryParams::new();
    params.insert("low".to_string(), Value::Int(10));
    params.insert("high".to_string(), Value::Int(40));
    let rows = db
            .query_with_params(
                "MATCH (n:Person) WHERE n.score = $low OR n.score = $high RETURN n.name ORDER BY n.score ASC",
                params,
            )
            .unwrap();

    assert_eq!(rows.len(), 2);
    assert_eq!(
        rows[0].get("n.name"),
        Some(&QueryValue::Scalar(Value::String("Bob".to_string())))
    );
    assert_eq!(
        rows[1].get("n.name"),
        Some(&QueryValue::Scalar(Value::String("Dave".to_string())))
    );

    let mut params = QueryParams::new();
    params.insert("low".to_string(), Value::Int(10));
    params.insert("high".to_string(), Value::Int(40));
    params.insert("max".to_string(), Value::Int(40));
    let rows = db
            .query_with_params(
                "MATCH (n:Person) WHERE (n.score = $low OR n.score = $high) AND n.score < $max RETURN n.name",
                params,
            )
            .unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get("n.name"),
        Some(&QueryValue::Scalar(Value::String("Bob".to_string())))
    );

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn query_returns_distinct_results() {
    let dir = temp_dir("facade-query-distinct");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 2, 2)).unwrap();
    for (name, status) in [
        ("Alice", "active"),
        ("Bob", "inactive"),
        ("Carol", "active"),
        ("Dave", "pending"),
    ] {
        db.create_node(
            vec!["Person".to_string()],
            properties(&[
                ("name", Value::String(name.to_string())),
                ("status", Value::String(status.to_string())),
            ]),
        )
        .unwrap();
    }

    let rows = db
        .query("MATCH (n:Person) RETURN DISTINCT n.status ORDER BY n.status ASC LIMIT 2")
        .unwrap();

    assert_eq!(rows.len(), 2);
    assert_eq!(
        rows[0].get("n.status"),
        Some(&QueryValue::Scalar(Value::String("active".to_string())))
    );
    assert_eq!(
        rows[1].get("n.status"),
        Some(&QueryValue::Scalar(Value::String("inactive".to_string())))
    );

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn query_filters_with_null_predicates() {
    let dir = temp_dir("facade-query-null-predicates");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 2, 2)).unwrap();
    db.create_node(
        vec!["Person".to_string()],
        properties(&[
            ("name", Value::String("Alice".to_string())),
            ("status", Value::String("active".to_string())),
        ]),
    )
    .unwrap();
    db.create_node(
        vec!["Person".to_string()],
        properties(&[("name", Value::String("Bob".to_string()))]),
    )
    .unwrap();

    let rows = db
        .query("MATCH (n:Person) WHERE n.status IS NULL RETURN n.name")
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get("n.name"),
        Some(&QueryValue::Scalar(Value::String("Bob".to_string())))
    );

    let rows = db
        .query("MATCH (n:Person) WHERE n.status IS NOT NULL RETURN n.name")
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get("n.name"),
        Some(&QueryValue::Scalar(Value::String("Alice".to_string())))
    );

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn query_counts_matching_results() {
    let dir = temp_dir("facade-query-count");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 2, 2)).unwrap();
    for (name, active) in [("Alice", true), ("Bob", false), ("Carol", true)] {
        db.create_node(
            vec!["Person".to_string()],
            properties(&[
                ("name", Value::String(name.to_string())),
                ("active", Value::Bool(active)),
            ]),
        )
        .unwrap();
    }

    let rows = db
        .query("MATCH (n:Person) WHERE n.active = true RETURN count(n)")
        .unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get("count(n)"),
        Some(&QueryValue::Scalar(Value::Int(2)))
    );

    let rows = db
        .query("MATCH (n:Person) RETURN n.active, count(n) ORDER BY n.active DESC")
        .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(
        rows[0].get("n.active"),
        Some(&QueryValue::Scalar(Value::Bool(true)))
    );
    assert_eq!(
        rows[0].get("count(n)"),
        Some(&QueryValue::Scalar(Value::Int(2)))
    );
    assert_eq!(
        rows[1].get("n.active"),
        Some(&QueryValue::Scalar(Value::Bool(false)))
    );
    assert_eq!(
        rows[1].get("count(n)"),
        Some(&QueryValue::Scalar(Value::Int(1)))
    );

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn create_node_routing_key_uses_parsed_labels_and_properties() {
    let params = [("name".to_string(), Value::String("Alice".to_string()))]
        .into_iter()
        .collect();
    let key = create_node_routing_key(
        "CREATE (n:Person:User {name: $name, active: true}) RETURN n",
        &params,
    )
    .unwrap()
    .unwrap();

    assert_eq!(key.labels, vec!["Person".to_string(), "User".to_string()]);
    assert_eq!(
        key.properties,
        properties(&[
            ("active", Value::Bool(true)),
            ("name", Value::String("Alice".to_string()))
        ])
    );

    let equivalent = create_node_routing_key(
        "  CREATE ( other : Person : User { active : true, name : $name } ) RETURN other ",
        &params,
    )
    .unwrap()
    .unwrap();
    assert_eq!(key, equivalent);

    let with_set = create_node_routing_key(
        "CREATE (n:Person:User {name: $name}) SET n.active = true RETURN n",
        &params,
    )
    .unwrap()
    .unwrap();
    assert_eq!(with_set, key);

    let anonymous =
        create_node_routing_key("CREATE (:Person:User {name: $name, active: true})", &params)
            .unwrap()
            .unwrap();
    assert_eq!(anonymous, key);
}

#[test]
fn execute_cypher_creates_anonymous_node() {
    let dir = temp_dir("facade-cypher-create-anonymous-node");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1)).unwrap();
    let mut params = QueryParams::new();
    params.insert("name".to_string(), Value::String("Alice".to_string()));

    let rows = db
        .execute_cypher_with_params("CREATE (:Person {name: $name})", params)
        .unwrap();
    assert!(rows.is_empty());
    let rows = db
        .query(r#"MATCH (n:Person) WHERE n.name = "Alice" RETURN n.name"#)
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get("n.name"),
        Some(&QueryValue::Scalar(Value::String("Alice".to_string())))
    );

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn merge_node_routing_key_uses_parsed_labels_and_properties() {
    let params = [
        (
            "email".to_string(),
            Value::String("alice@example.com".to_string()),
        ),
        ("tenant".to_string(), Value::String("acme".to_string())),
    ]
    .into_iter()
    .collect();
    let key = merge_node_routing_key(
        "MERGE (n:Person {email: $email, tenant: $tenant}) ON CREATE SET n.created = true RETURN n",
        &params,
    )
    .unwrap()
    .unwrap();

    assert_eq!(key.labels, vec!["Person".to_string()]);
    assert_eq!(
        key.properties,
        properties(&[
            ("email", Value::String("alice@example.com".to_string())),
            ("tenant", Value::String("acme".to_string()))
        ])
    );

    let equivalent = merge_node_routing_key(
        " MERGE ( other : Person { tenant : $tenant, email : $email } ) RETURN other ",
        &params,
    )
    .unwrap()
    .unwrap();
    assert_eq!(key, equivalent);

    let anonymous =
        merge_node_routing_key("MERGE (:Person {email: $email, tenant: $tenant})", &params)
            .unwrap()
            .unwrap();
    assert_eq!(key, anonymous);
}

#[test]
fn query_shard_reads_only_requested_owner_shard() {
    let dir = temp_dir("facade-query-shard");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 2, 2)).unwrap();
    db.create_node(
        vec!["Person".to_string()],
        properties(&[("name", Value::String("Alice".to_string()))]),
    )
    .unwrap();
    db.create_node(
        vec!["Person".to_string()],
        properties(&[("name", Value::String("Bob".to_string()))]),
    )
    .unwrap();

    let shard0 = db.query_shard(0, "MATCH (n:Person) RETURN n.name").unwrap();
    let shard1 = db.query_shard(1, "MATCH (n:Person) RETURN n.name").unwrap();

    assert_eq!(shard0.len(), 1);
    assert_eq!(
        shard0[0].get("n.name"),
        Some(&QueryValue::Scalar(Value::String("Alice".to_string())))
    );
    assert_eq!(shard1.len(), 1);
    assert_eq!(
        shard1[0].get("n.name"),
        Some(&QueryValue::Scalar(Value::String("Bob".to_string())))
    );
    assert!(matches!(
        db.query_shard(2, "MATCH (n:Person) RETURN n"),
        Err(DatabaseError::MissingShardLog(2))
    ));

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn index_catalog_persists_across_reopen() {
    let dir = temp_dir("facade-index-catalog");
    {
        let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 2, 2)).unwrap();
        db.create_node_property_index("person_name", "Person", "name")
            .unwrap();
        db.create_vector_index("doc_embedding", "Document", "embedding", 2, "cosine")
            .unwrap();
        assert!(matches!(
            db.create_node_property_index("person_name", "Person", "nickname"),
            Err(DatabaseError::InvalidConfig(_))
        ));
    }

    {
        let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 2, 2)).unwrap();
        assert_eq!(
            db.list_indexes().unwrap(),
            vec![
                IndexDefinition::node_property("person_name", "Person", "name"),
                IndexDefinition::vector("doc_embedding", "Document", "embedding", 2, "cosine"),
            ]
        );
        db.drop_index("person_name").unwrap();
        assert_eq!(
            db.list_indexes().unwrap(),
            vec![IndexDefinition::vector(
                "doc_embedding",
                "Document",
                "embedding",
                2,
                "cosine"
            )]
        );
    }

    {
        let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 2, 2)).unwrap();
        assert_eq!(
            db.list_indexes().unwrap(),
            vec![IndexDefinition::vector(
                "doc_embedding",
                "Document",
                "embedding",
                2,
                "cosine"
            )]
        );
        let rows = db.execute_cypher("SHOW INDEXES").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].get("name"),
            Some(&QueryValue::Scalar(Value::String(
                "doc_embedding".to_string()
            )))
        );
        assert_eq!(
            rows[0].get("type"),
            Some(&QueryValue::Scalar(Value::String("vector".to_string())))
        );
        let vector_rows = db.execute_cypher("SHOW VECTOR INDEXES").unwrap();
        assert_eq!(vector_rows.len(), 1);
        assert_eq!(
            vector_rows[0].get("name"),
            Some(&QueryValue::Scalar(Value::String(
                "doc_embedding".to_string()
            )))
        );
        let vector_row = db
            .execute_cypher("SHOW VECTOR INDEX doc_embedding")
            .unwrap();
        assert_eq!(vector_row.len(), 1);
    }

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn vector_index_rejects_dimension_mismatches() {
    let dir = temp_dir("facade-vector-index-dimensions");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 2, 2)).unwrap();
    db.create_node(
        vec!["Document".to_string()],
        properties(&[("embedding", Value::Vector(vec![1.0, 0.0, 0.0]))]),
    )
    .unwrap();

    let err = db
        .create_vector_index("doc_embedding", "Document", "embedding", 2, "cosine")
        .unwrap_err();
    assert!(matches!(err, DatabaseError::InvalidConfig(_)));
    assert!(db.list_indexes().unwrap().is_empty());

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn vector_index_rejects_invalid_indexed_writes_before_wal_append() {
    let dir = temp_dir("facade-vector-index-write-validation");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1)).unwrap();
    db.create_vector_index("doc_embedding", "Document", "embedding", 2, "cosine")
        .unwrap();

    assert!(matches!(
        db.create_node(
            vec!["Document".to_string()],
            properties(&[("embedding", Value::Vector(vec![1.0]))]),
        ),
        Err(DatabaseError::InvalidConfig(_))
    ));
    assert!(db.query("MATCH (n:Document) RETURN n").unwrap().is_empty());
    assert_eq!(db.committed_indexes().unwrap(), vec![0]);

    let id = db
        .create_node(
            vec!["Document".to_string()],
            properties(&[("embedding", Value::Vector(vec![1.0, 0.0]))]),
        )
        .unwrap();
    assert_eq!(db.committed_indexes().unwrap(), vec![1]);
    assert!(matches!(
        db.set_node_property(id, "embedding".to_string(), Value::Vector(vec![1.0])),
        Err(DatabaseError::InvalidConfig(_))
    ));
    assert_eq!(db.committed_indexes().unwrap(), vec![1]);
    let rows = db
        .query("MATCH (n:Document) WHERE vector.knn(n.embedding, [1.0, 0.0], 1) RETURN n")
        .unwrap();
    assert_eq!(rows.len(), 1);

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn graph_writes_reject_map_values_before_wal_append() {
    let dir = temp_dir("facade-map-property-write-validation");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1)).unwrap();

    let map_value = Value::Map(properties(&[("nested", Value::Bool(true))]));
    assert!(matches!(
        db.create_node(
            vec!["Person".to_string()],
            properties(&[("profile", map_value.clone())]),
        ),
        Err(DatabaseError::InvalidConfig(_))
    ));
    assert!(db.query("MATCH (n:Person) RETURN n").unwrap().is_empty());
    assert_eq!(db.committed_indexes().unwrap(), vec![0]);

    let alice = db
        .create_node(
            vec!["Person".to_string()],
            properties(&[("name", Value::String("Alice".to_string()))]),
        )
        .unwrap();
    assert!(matches!(
        db.set_node_property(alice, "profile".to_string(), map_value.clone()),
        Err(DatabaseError::InvalidConfig(_))
    ));

    let bob = db
        .create_node(
            vec!["Person".to_string()],
            properties(&[("name", Value::String("Bob".to_string()))]),
        )
        .unwrap();
    let relationship = db
        .create_relationship(alice, bob, "KNOWS".to_string(), Properties::new())
        .unwrap();
    assert!(matches!(
        db.set_relationship_property(relationship, "profile".to_string(), map_value.clone()),
        Err(DatabaseError::InvalidConfig(_))
    ));
    assert!(matches!(
        db.create_relationship(
            alice,
            bob,
            "LIKES".to_string(),
            properties(&[("profile", map_value)])
        ),
        Err(DatabaseError::InvalidConfig(_))
    ));

    assert_eq!(db.committed_indexes().unwrap(), vec![3]);
    assert!(db
        .query("MATCH (n:Person) WHERE n.profile = true RETURN n")
        .unwrap()
        .is_empty());

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn vector_index_cache_rebuilds_from_catalog_and_tracks_writes() {
    let dir = temp_dir("facade-vector-index-cache");
    let cache_path = dir.join("indexes").join("vector-cache.bin");
    {
        let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 2, 2)).unwrap();
        db.create_vector_index("doc_embedding", "Document", "embedding", 2, "cosine")
            .unwrap();
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
        assert!(cache_path.exists());
    }

    {
        let mut db = Neo4rDatabase::open(DatabaseConfig::new(&dir, 2, 2)).unwrap();
        assert_eq!(db.vector_indexes.lock().unwrap().indexes.len(), 1);
        assert_eq!(
            db.vector_index_status().unwrap(),
            vec![VectorIndexStatus {
                name: "doc_embedding".to_string(),
                label: "Document".to_string(),
                property: "embedding".to_string(),
                dimensions: 2,
                metric: "cosine".to_string(),
                entries: 2,
            }]
        );
        let status_rows = db.execute_cypher("SHOW VECTOR INDEX STATUS").unwrap();
        assert_eq!(status_rows.len(), 1);
        assert_eq!(
            status_rows[0].get("entries"),
            Some(&QueryValue::Scalar(Value::Int(2)))
        );
        let status_rows = db
            .execute_cypher("SHOW VECTOR INDEX STATUS doc_embedding")
            .unwrap();
        assert_eq!(status_rows.len(), 1);
        assert_eq!(
            status_rows[0].get("name"),
            Some(&QueryValue::Scalar(Value::String(
                "doc_embedding".to_string()
            )))
        );
        assert!(
            load_vector_index_cache(cache_path.clone(), &db.index_catalog())
                .unwrap()
                .is_some()
        );
        let rows = db
            .query("MATCH (n:Document) WHERE vector.knn(n.embedding, [1.0, 0.0], 1) RETURN n.title")
            .unwrap();
        assert_eq!(
            rows[0].get("n.title"),
            Some(&QueryValue::Scalar(Value::String("near".to_string())))
        );
        db.rebuild_vector_indexes().unwrap();
        let rows = db
            .query("MATCH (n:Document) WHERE vector.knn(n.embedding, [1.0, 0.0], 1) RETURN n.title")
            .unwrap();
        assert_eq!(
            rows[0].get("n.title"),
            Some(&QueryValue::Scalar(Value::String("near".to_string())))
        );
        db.rebuild_vector_index("doc_embedding").unwrap();
        assert!(matches!(
            db.rebuild_vector_index("missing_vector"),
            Err(DatabaseError::InvalidConfig(message)) if message.contains("does not exist")
        ));

        let near = db
            .query(r#"MATCH (n:Document) WHERE n.title = "near" RETURN n"#)
            .unwrap();
        let Some(QueryValue::Node(node)) = near[0].get("n") else {
            panic!("expected near node");
        };
        db.set_node_property(
            node.id,
            "embedding".to_string(),
            Value::Vector(vec![-1.0, 0.0]),
        )
        .unwrap();
        let rows = db
            .query("MATCH (n:Document) WHERE vector.knn(n.embedding, [1.0, 0.0], 1) RETURN n.title")
            .unwrap();
        assert_ne!(
            rows[0].get("n.title"),
            Some(&QueryValue::Scalar(Value::String("near".to_string())))
        );
        let cached = load_vector_index_cache(cache_path.clone(), &db.index_catalog())
            .unwrap()
            .unwrap();
        assert_eq!(db.vector_index_status().unwrap()[0].entries, 2);
        let snapshot = cached
            .snapshots()
            .into_iter()
            .find(|snapshot| snapshot.name == "doc_embedding")
            .unwrap();
        assert!(snapshot
            .entries
            .iter()
            .any(|(_, vector)| vector == &vec![-1.0, 0.0]));
    }

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn corrupt_vector_index_cache_falls_back_to_rebuild() {
    let dir = temp_dir("facade-vector-index-cache-corrupt");
    let cache_path = dir.join("indexes").join("vector-cache.bin");
    {
        let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1)).unwrap();
        db.create_vector_index("doc_embedding", "Document", "embedding", 2, "cosine")
            .unwrap();
        db.create_node(
            vec!["Document".to_string()],
            properties(&[
                ("title", Value::String("near".to_string())),
                ("embedding", Value::Vector(vec![1.0, 0.0])),
            ]),
        )
        .unwrap();
    }
    fs::write(&cache_path, b"not a vector cache").unwrap();

    {
        let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1)).unwrap();
        let rows = db
            .query("MATCH (n:Document) WHERE vector.knn(n.embedding, [1.0, 0.0], 1) RETURN n.title")
            .unwrap();
        assert_eq!(
            rows[0].get("n.title"),
            Some(&QueryValue::Scalar(Value::String("near".to_string())))
        );
    }

    let bytes = fs::read(cache_path).unwrap();
    assert!(bytes.starts_with(VECTOR_INDEX_CACHE_MAGIC));

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn vector_index_rebuild_excludes_removed_properties_and_deleted_nodes() {
    let dir = temp_dir("facade-vector-index-rebuild-removals");
    {
        let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1)).unwrap();
        db.create_vector_index("doc_embedding", "Document", "embedding", 2, "cosine")
            .unwrap();
        let removed = db
            .create_node(
                vec!["Document".to_string()],
                properties(&[
                    ("title", Value::String("removed".to_string())),
                    ("embedding", Value::Vector(vec![1.0, 0.0])),
                ]),
            )
            .unwrap();
        let deleted = db
            .create_node(
                vec!["Document".to_string()],
                properties(&[
                    ("title", Value::String("deleted".to_string())),
                    ("embedding", Value::Vector(vec![0.9, 0.1])),
                ]),
            )
            .unwrap();
        db.create_node(
            vec!["Document".to_string()],
            properties(&[
                ("title", Value::String("kept".to_string())),
                ("embedding", Value::Vector(vec![0.0, 1.0])),
            ]),
        )
        .unwrap();

        db.remove_node_property(removed, "embedding".to_string())
            .unwrap();
        db.delete_node(deleted).unwrap();
    }

    {
        let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1)).unwrap();
        let rows = db
            .query("MATCH (n:Document) WHERE vector.knn(n.embedding, [1.0, 0.0], 3) RETURN n.title")
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].get("n.title"),
            Some(&QueryValue::Scalar(Value::String("kept".to_string())))
        );
    }

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn dropped_vector_index_stays_absent_after_reopen() {
    let dir = temp_dir("facade-vector-index-drop-reopen");
    {
        let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1)).unwrap();
        db.create_vector_index("doc_embedding", "Document", "embedding", 2, "cosine")
            .unwrap();
        db.create_node(
            vec!["Document".to_string()],
            properties(&[
                ("title", Value::String("near".to_string())),
                ("embedding", Value::Vector(vec![1.0, 0.0])),
            ]),
        )
        .unwrap();
        db.drop_index("doc_embedding").unwrap();
        assert!(db.list_indexes().unwrap().is_empty());
    }

    {
        let mut db = Neo4rDatabase::open(DatabaseConfig::new(&dir, 1, 1)).unwrap();
        assert!(db.list_indexes().is_empty());
        assert!(db.vector_indexes.lock().unwrap().indexes.is_empty());
        db.rebuild_vector_indexes().unwrap();
        assert!(db.vector_indexes.lock().unwrap().indexes.is_empty());

        let plan = db.query_plan(
            "MATCH (n:Document) WHERE vector.knn(n.embedding, [1.0, 0.0], 1) RETURN n",
            &QueryParams::new(),
        );
        assert_eq!(
            plan.access_plan,
            QueryAccessPlan::NodeLabelScan {
                label: "Document".to_string()
            }
        );
        assert_eq!(
            db.query(
                "MATCH (n:Document) WHERE vector.knn(n.embedding, [1.0, 0.0], 1) RETURN n.title",
            )
            .unwrap()
            .len(),
            1
        );
    }

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn stale_vector_index_cache_is_rebuilt_after_catalog_definition_changes() {
    let dir = temp_dir("facade-vector-index-cache-stale-definition");
    let cache_path = dir.join("indexes").join("vector-cache.bin");
    let stale_cache;
    {
        let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1)).unwrap();
        db.create_node(
            vec!["Document".to_string()],
            properties(&[
                ("title", Value::String("near".to_string())),
                ("embedding", Value::Vector(vec![1.0, 0.0])),
                ("embedding_v2", Value::Vector(vec![0.0, 1.0])),
            ]),
        )
        .unwrap();
        db.create_node(
            vec!["Document".to_string()],
            properties(&[
                ("title", Value::String("far".to_string())),
                ("embedding", Value::Vector(vec![0.0, 1.0])),
                ("embedding_v2", Value::Vector(vec![1.0, 0.0])),
            ]),
        )
        .unwrap();
        db.create_vector_index("doc_embedding", "Document", "embedding", 2, "cosine")
            .unwrap();
        stale_cache = fs::read(&cache_path).unwrap();
        db.drop_index("doc_embedding").unwrap();
        db.create_vector_index("doc_embedding_v2", "Document", "embedding_v2", 2, "cosine")
            .unwrap();
    }

    fs::write(&cache_path, stale_cache).unwrap();
    {
        let catalog = IndexCatalogStore::open(&dir)
            .unwrap()
            .load()
            .unwrap()
            .unwrap();
        assert!(load_vector_index_cache(cache_path.clone(), &catalog)
            .unwrap()
            .is_none());
    }

    {
        let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1)).unwrap();
        let rows = db
            .query(
                "MATCH (n:Document) WHERE vector.knn(n.embedding_v2, [0.0, 1.0], 1) RETURN n.title",
            )
            .unwrap();
        assert_eq!(
            rows[0].get("n.title"),
            Some(&QueryValue::Scalar(Value::String("near".to_string())))
        );
        let cached = load_vector_index_cache(cache_path, &db.index_catalog().unwrap())
            .unwrap()
            .unwrap();
        let snapshots = cached.snapshots();
        assert_eq!(snapshots.len(), 1);
        assert_eq!(snapshots[0].name, "doc_embedding_v2");
    }

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn install_index_catalog_rejects_unique_constraint_violated_by_existing_nodes() {
    let dir = temp_dir("facade-index-catalog-install-unique-violation");
    let mut db = Neo4rDatabase::open(DatabaseConfig::new(&dir, 1, 1)).unwrap();
    db.create_node(
        vec!["Person".to_string()],
        properties(&[("email", Value::String("same@example.com".to_string()))]),
    )
    .unwrap();
    db.create_node(
        vec!["Person".to_string()],
        properties(&[("email", Value::String("same@example.com".to_string()))]),
    )
    .unwrap();

    let catalog = IndexCatalog {
        version: db.index_catalog().version + 1,
        indexes: vec![IndexDefinition::unique_node_property(
            "person_email_unique",
            "Person",
            "email",
        )],
    };

    let err = db.install_index_catalog(catalog).unwrap_err();
    assert!(err.to_string().contains("would be violated by nodes"));
    assert!(db.index_catalog().indexes.is_empty());
    assert!(IndexCatalogStore::open(&dir)
        .unwrap()
        .load()
        .unwrap()
        .is_none());

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn install_index_catalog_rejects_vector_definition_invalid_for_existing_nodes() {
    let dir = temp_dir("facade-index-catalog-install-vector-violation");
    let mut db = Neo4rDatabase::open(DatabaseConfig::new(&dir, 1, 1)).unwrap();
    db.create_node(
        vec!["Document".to_string()],
        properties(&[("embedding", Value::Vector(vec![1.0, 0.0]))]),
    )
    .unwrap();

    let catalog = IndexCatalog {
        version: db.index_catalog().version + 1,
        indexes: vec![IndexDefinition::vector(
            "doc_embedding",
            "Document",
            "embedding",
            3,
            "cosine",
        )],
    };

    let err = db.install_index_catalog(catalog).unwrap_err();
    assert!(err.to_string().contains("expected 3 dimensions, got 2"));
    assert!(db.index_catalog().indexes.is_empty());
    assert!(db.vector_indexes.lock().unwrap().indexes.is_empty());
    assert!(IndexCatalogStore::open(&dir)
        .unwrap()
        .load()
        .unwrap()
        .is_none());

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn install_index_catalog_rebuilds_vector_cache_only_after_validation() {
    let dir = temp_dir("facade-index-catalog-install-vector-cache");
    let cache_path = dir.join("indexes").join("vector-cache.bin");
    {
        let mut db = Neo4rDatabase::open(DatabaseConfig::new(&dir, 1, 1)).unwrap();
        db.create_node(
            vec!["Document".to_string()],
            properties(&[
                ("title", Value::String("near".to_string())),
                ("embedding", Value::Vector(vec![1.0, 0.0])),
            ]),
        )
        .unwrap();
        let catalog = IndexCatalog {
            version: db.index_catalog().version + 1,
            indexes: vec![IndexDefinition::vector(
                "doc_embedding",
                "Document",
                "embedding",
                2,
                "cosine",
            )],
        };

        db.install_index_catalog(catalog).unwrap();
        assert!(cache_path.exists());
    }

    {
        let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1)).unwrap();
        let rows = db
            .query("MATCH (n:Document) WHERE vector.knn(n.embedding, [1.0, 0.0], 1) RETURN n.title")
            .unwrap();
        assert_eq!(
            rows[0].get("n.title"),
            Some(&QueryValue::Scalar(Value::String("near".to_string())))
        );
    }

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn execute_cypher_creates_node_with_params() {
    let dir = temp_dir("facade-cypher-create");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 2, 2)).unwrap();
    let mut params = QueryParams::new();
    params.insert("name".to_string(), Value::String("Alice".to_string()));
    params.insert("age".to_string(), Value::Int(42));
    params.insert("embedding".to_string(), Value::Vector(vec![1.0, 0.0]));

    let rows = db
        .execute_cypher_with_params(
            "CREATE (n:Person {name: $name, age: $age, embedding: $embedding}) RETURN n",
            params,
        )
        .unwrap();

    assert_eq!(rows.len(), 1);
    let Some(QueryValue::Node(node)) = rows[0].get("n") else {
        panic!("expected created node in RETURN n");
    };
    assert_eq!(node.labels, vec!["Person".to_string()]);
    assert_eq!(
        db.query(r#"MATCH (n:Person) WHERE n.name = "Alice" RETURN n.age"#)
            .unwrap()[0]
            .get("n.age"),
        Some(&QueryValue::Scalar(Value::Int(42)))
    );

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn execute_cypher_create_node_applies_set_assignments() {
    let dir = temp_dir("facade-cypher-create-set");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 2, 2)).unwrap();
    let mut params = QueryParams::new();
    params.insert("name".to_string(), Value::String("Alice".to_string()));
    params.insert("status".to_string(), Value::String("active".to_string()));

    let rows = db
        .execute_cypher_with_params(
            "CREATE (n:Person {name: $name}) SET n.status = $status, n.created = true RETURN n",
            params,
        )
        .unwrap();

    let Some(QueryValue::Node(node)) = rows[0].get("n") else {
        panic!("expected created node");
    };
    assert_eq!(
        node.properties.get("status"),
        Some(&Value::String("active".to_string()))
    );
    assert_eq!(node.properties.get("created"), Some(&Value::Bool(true)));
    let rows = db
        .query(r#"MATCH (n:Person) WHERE n.status = "active" RETURN n.created"#)
        .unwrap();
    assert_eq!(
        rows[0].get("n.created"),
        Some(&QueryValue::Scalar(Value::Bool(true)))
    );

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn execute_cypher_create_node_replaces_properties_from_map() {
    let dir = temp_dir("facade-cypher-create-replace-map");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1)).unwrap();

    let rows = db
            .execute_cypher(
                r#"CREATE (n:Person {name: "Alice", stale: true}) SET n = {name: "Alice", status: "active"} RETURN n"#,
            )
            .unwrap();

    assert_eq!(rows.len(), 1);
    let Some(QueryValue::Node(node)) = rows[0].get("n") else {
        panic!("expected node");
    };
    assert_eq!(
        node.properties.get("status"),
        Some(&Value::String("active".to_string()))
    );
    assert!(!node.properties.contains_key("stale"));
    assert_eq!(
        db.query(r#"MATCH (n:Person) WHERE n.stale = true RETURN n"#)
            .unwrap()
            .len(),
        0
    );

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn execute_cypher_create_returns_created_properties() {
    let dir = temp_dir("facade-cypher-create-return-property");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1)).unwrap();
    let mut params = QueryParams::new();
    params.insert("name".to_string(), Value::String("Alice".to_string()));
    params.insert("status".to_string(), Value::String("active".to_string()));

    let rows = db
        .execute_cypher_with_params(
            "CREATE (n:Person {name: $name}) SET n.status = $status RETURN n.status",
            params,
        )
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get("n.status"),
        Some(&QueryValue::Scalar(Value::String("active".to_string())))
    );

    db.execute_cypher(r#"CREATE (n:Person {name: "Bob"})"#)
        .unwrap();
    let mut params = QueryParams::new();
    params.insert("from".to_string(), Value::String("Alice".to_string()));
    params.insert("to".to_string(), Value::String("Bob".to_string()));
    params.insert("since".to_string(), Value::Int(2026));

    let rows = db
            .execute_cypher_with_params(
                "MATCH (a:Person {name: $from}), (b:Person {name: $to}) CREATE (a)-[r:KNOWS]->(b) SET r.since = $since RETURN r.since",
                params,
            )
            .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get("r.since"),
        Some(&QueryValue::Scalar(Value::Int(2026)))
    );

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn execute_cypher_merges_node_idempotently() {
    let dir = temp_dir("facade-cypher-merge-node");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 2, 2)).unwrap();
    let mut params = QueryParams::new();
    params.insert("name".to_string(), Value::String("Alice".to_string()));
    params.insert("tenant".to_string(), Value::String("acme".to_string()));

    let rows = db
        .execute_cypher_with_params(
            "MERGE (n:Person:Account {name: $name, tenant: $tenant}) RETURN n.name",
            params.clone(),
        )
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get("n.name"),
        Some(&QueryValue::Scalar(Value::String("Alice".to_string())))
    );

    let rows = db
        .execute_cypher_with_params(
            "MERGE (n:Person:Account {name: $name, tenant: $tenant}) RETURN n",
            params,
        )
        .unwrap();
    assert_eq!(rows.len(), 1);
    let Some(QueryValue::Node(node)) = rows[0].get("n") else {
        panic!("expected merged node in RETURN n");
    };
    assert_eq!(
        node.labels,
        vec!["Person".to_string(), "Account".to_string()]
    );

    assert_eq!(
        db.query(r#"MATCH (n:Person) WHERE n.name = "Alice" RETURN n"#)
            .unwrap()
            .len(),
        1
    );

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn execute_cypher_merge_node_uses_unique_constraint_key() {
    let dir = temp_dir("facade-cypher-merge-node-unique");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1)).unwrap();
    db.execute_cypher(
        "CREATE CONSTRAINT person_email_unique FOR (n:Person) REQUIRE n.email IS UNIQUE",
    )
    .unwrap();
    db.create_node(
        vec!["Person".to_string()],
        properties(&[
            ("email", Value::String("alice@example.com".to_string())),
            ("tenant", Value::String("acme".to_string())),
        ]),
    )
    .unwrap();

    let rows = db
        .execute_cypher(
            r#"MERGE (n:Person {email: "alice@example.com", tenant: "acme"}) RETURN n.email"#,
        )
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get("n.email"),
        Some(&QueryValue::Scalar(Value::String(
            "alice@example.com".to_string()
        )))
    );
    assert_eq!(
        db.query(r#"MATCH (n:Person) WHERE n.email = "alice@example.com" RETURN n"#)
            .unwrap()
            .len(),
        1
    );

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn execute_cypher_merges_anonymous_node_idempotently() {
    let dir = temp_dir("facade-cypher-merge-anonymous-node");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1)).unwrap();
    let mut params = QueryParams::new();
    params.insert(
        "email".to_string(),
        Value::String("alice@example.com".to_string()),
    );

    let first = db
        .execute_cypher_with_params("MERGE (:Person {email: $email})", params.clone())
        .unwrap();
    let second = db
        .execute_cypher_with_params("MERGE (:Person {email: $email})", params)
        .unwrap();
    assert!(first.is_empty());
    assert!(second.is_empty());
    assert_eq!(
        db.query(r#"MATCH (n:Person) WHERE n.email = "alice@example.com" RETURN n"#)
            .unwrap()
            .len(),
        1
    );

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn execute_cypher_merge_node_applies_on_create_and_on_match_set() {
    let dir = temp_dir("facade-cypher-merge-node-on-set");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1)).unwrap();
    let mut params = QueryParams::new();
    params.insert(
        "email".to_string(),
        Value::String("alice@example.com".to_string()),
    );
    params.insert("created".to_string(), Value::Int(1));
    params.insert("seen".to_string(), Value::Int(10));

    let rows = db
            .execute_cypher_with_params(
                "MERGE (n:Person {email: $email}) ON CREATE SET n.created = $created ON MATCH SET n.seen = $seen RETURN n",
                params.clone(),
            )
            .unwrap();
    assert_eq!(rows.len(), 1);
    let Some(QueryValue::Node(node)) = rows[0].get("n") else {
        panic!("expected merged node");
    };
    assert_eq!(node.properties.get("created"), Some(&Value::Int(1)));
    assert_eq!(node.properties.get("seen"), None);

    let rows = db
            .execute_cypher_with_params(
                "MERGE (n:Person {email: $email}) ON CREATE SET n.created = 2 ON MATCH SET n.seen = $seen RETURN n",
                params,
            )
            .unwrap();
    assert_eq!(rows.len(), 1);
    let Some(QueryValue::Node(node)) = rows[0].get("n") else {
        panic!("expected matched node");
    };
    assert_eq!(node.properties.get("created"), Some(&Value::Int(1)));
    assert_eq!(node.properties.get("seen"), Some(&Value::Int(10)));
    assert_eq!(
        db.query(r#"MATCH (n:Person) WHERE n.email = "alice@example.com" RETURN n"#)
            .unwrap()
            .len(),
        1
    );

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn execute_cypher_merge_node_replaces_properties_from_on_set_maps() {
    let dir = temp_dir("facade-cypher-merge-node-on-replace-map");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1)).unwrap();

    let first = db
            .execute_cypher(
                r#"MERGE (n:Person {email: "alice@example.com"}) ON CREATE SET n = {email: "alice@example.com", created: 1} ON MATCH SET n = {email: "alice@example.com", seen: 1} RETURN n"#,
            )
            .unwrap();
    let Some(QueryValue::Node(node)) = first[0].get("n") else {
        panic!("expected created node");
    };
    assert_eq!(node.properties.get("created"), Some(&Value::Int(1)));
    assert_eq!(node.properties.get("seen"), None);

    let second = db
            .execute_cypher(
                r#"MERGE (n:Person {email: "alice@example.com"}) ON CREATE SET n = {email: "alice@example.com", created: 2} ON MATCH SET n = {email: "alice@example.com", seen: 1} RETURN n"#,
            )
            .unwrap();
    let Some(QueryValue::Node(node)) = second[0].get("n") else {
        panic!("expected matched node");
    };
    assert_eq!(node.properties.get("seen"), Some(&Value::Int(1)));
    assert_eq!(node.properties.get("created"), None);
    assert_eq!(
        db.query(r#"MATCH (n:Person) WHERE n.email = "alice@example.com" RETURN n"#)
            .unwrap()
            .len(),
        1
    );

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn execute_cypher_sets_node_property() {
    let dir = temp_dir("facade-cypher-set");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 2, 2)).unwrap();
    db.create_node(
        vec!["Person".to_string()],
        properties(&[
            ("name", Value::String("Alice".to_string())),
            ("tenant", Value::String("acme".to_string())),
        ]),
    )
    .unwrap();
    let mut params = QueryParams::new();
    params.insert("name".to_string(), Value::String("Alice".to_string()));
    params.insert("tenant".to_string(), Value::String("acme".to_string()));
    params.insert("status".to_string(), Value::String("active".to_string()));

    let returned = db
            .execute_cypher_with_params(
                "MATCH (n:Person) WHERE n.name = $name AND n.tenant = $tenant SET n.status = $status RETURN n.status",
                params,
            )
            .unwrap();
    assert_eq!(
        returned[0].get("n.status"),
        Some(&QueryValue::Scalar(Value::String("active".to_string())))
    );

    let rows = db
        .query(r#"MATCH (n:Person) WHERE n.status = "active" RETURN n.name"#)
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get("n.name"),
        Some(&QueryValue::Scalar(Value::String("Alice".to_string())))
    );

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn execute_cypher_set_null_removes_node_property() {
    let dir = temp_dir("facade-cypher-set-null-node");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1)).unwrap();
    db.execute_cypher(r#"CREATE (n:Person {name: "Alice", status: "active", stale: true})"#)
        .unwrap();

    let rows = db
        .execute_cypher(
            r#"MATCH (n:Person) WHERE n.name = "Alice" SET n.status = null RETURN n.status"#,
        )
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get("n.status"),
        Some(&QueryValue::Scalar(Value::Null))
    );
    assert!(db
        .query(r#"MATCH (n:Person) WHERE n.status = "active" RETURN n"#)
        .unwrap()
        .is_empty());

    let rows = db
            .execute_cypher(
                r#"MATCH (n:Person) WHERE n.name = "Alice" SET n += {stale: null, reviewed: true} RETURN n"#,
            )
            .unwrap();
    let Some(QueryValue::Node(node)) = rows[0].get("n") else {
        panic!("expected node");
    };
    assert_eq!(node.properties.get("stale"), None);
    assert_eq!(node.properties.get("reviewed"), Some(&Value::Bool(true)));

    let rows = db
            .execute_cypher(
                r#"MATCH (n:Person) WHERE n.name = "Alice" SET n = {name: "Alice", skipped: null} RETURN n"#,
            )
            .unwrap();
    let Some(QueryValue::Node(node)) = rows[0].get("n") else {
        panic!("expected node");
    };
    assert_eq!(
        node.properties.get("name"),
        Some(&Value::String("Alice".to_string()))
    );
    assert_eq!(node.properties.get("skipped"), None);
    assert_eq!(node.properties.get("reviewed"), None);

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn execute_cypher_sets_multiple_node_properties() {
    let dir = temp_dir("facade-cypher-set-multiple");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 2, 2)).unwrap();
    db.create_node(
        vec!["Person".to_string()],
        properties(&[("name", Value::String("Alice".to_string()))]),
    )
    .unwrap();
    let mut params = QueryParams::new();
    params.insert("name".to_string(), Value::String("Alice".to_string()));
    params.insert("status".to_string(), Value::String("active".to_string()));
    params.insert("score".to_string(), Value::Int(42));

    let returned = db
            .execute_cypher_with_params(
                "MATCH (n:Person) WHERE n.name = $name SET n.status = $status, n.score = $score RETURN n",
                params,
            )
            .unwrap();

    assert_eq!(returned.len(), 1);
    let Some(QueryValue::Node(node)) = returned[0].get("n") else {
        panic!("expected node in RETURN n");
    };
    assert_eq!(
        node.properties.get("status"),
        Some(&Value::String("active".to_string()))
    );
    assert_eq!(node.properties.get("score"), Some(&Value::Int(42)));
    let rows = db
        .query(r#"MATCH (n:Person) WHERE n.status = "active" AND n.score = 42 RETURN n.name"#)
        .unwrap();
    assert_eq!(rows.len(), 1);

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn execute_cypher_write_returns_multiple_items() {
    let dir = temp_dir("facade-cypher-write-return-multiple");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1)).unwrap();

    let rows = db
        .execute_cypher(
            r#"CREATE (n:Person {name: "Alice"}) SET n.status = "active" RETURN n.name, n.status"#,
        )
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get("n.name"),
        Some(&QueryValue::Scalar(Value::String("Alice".to_string())))
    );
    assert_eq!(
        rows[0].get("n.status"),
        Some(&QueryValue::Scalar(Value::String("active".to_string())))
    );

    let alice = db
        .query(r#"MATCH (n:Person) WHERE n.name = "Alice" RETURN n"#)
        .unwrap()[0]
        .get("n")
        .and_then(|value| match value {
            QueryValue::Node(node) => Some(node.id),
            _ => None,
        })
        .unwrap();
    let bob = db
        .create_node(
            vec!["Person".to_string()],
            properties(&[("name", Value::String("Bob".to_string()))]),
        )
        .unwrap();
    db.create_relationship(
        alice,
        bob,
        "KNOWS".to_string(),
        properties(&[("since", Value::Int(2026))]),
    )
    .unwrap();

    let rows = db
            .execute_cypher(
                r#"MATCH (a:Person)-[r:KNOWS {since: 2026}]->(b:Person) SET r.status = "active" RETURN r.since, r.status"#,
            )
            .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get("r.since"),
        Some(&QueryValue::Scalar(Value::Int(2026)))
    );
    assert_eq!(
        rows[0].get("r.status"),
        Some(&QueryValue::Scalar(Value::String("active".to_string())))
    );

    let err = db
            .execute_cypher(
                r#"MATCH (n:Person) WHERE n.name = "Alice" SET n.status = "reviewed" RETURN n.name, m.status"#,
            )
            .unwrap_err();
    assert!(err
        .to_string()
        .contains("SET RETURN variable must match the MATCH variable"));

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn execute_cypher_sets_node_properties_from_map() {
    let dir = temp_dir("facade-cypher-set-map");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1)).unwrap();
    db.execute_cypher(
        r#"CREATE (n:Person {name: "Alice"}) SET n += {status: "active", score: 42} RETURN n"#,
    )
    .unwrap();

    let rows = db
        .query(r#"MATCH (n:Person) WHERE n.status = "active" AND n.score = 42 RETURN n.name"#)
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get("n.name"),
        Some(&QueryValue::Scalar(Value::String("Alice".to_string())))
    );

    db.execute_cypher(
            r#"MATCH (n:Person) WHERE n.name = "Alice" SET n += {status: "reviewed", reviewed: true} RETURN n"#,
        )
        .unwrap();
    let rows = db
        .query(
            r#"MATCH (n:Person) WHERE n.status = "reviewed" AND n.reviewed = true RETURN n.score"#,
        )
        .unwrap();
    assert_eq!(
        rows[0].get("n.score"),
        Some(&QueryValue::Scalar(Value::Int(42)))
    );

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn execute_cypher_accepts_parameterized_property_maps() {
    let dir = temp_dir("facade-cypher-parameterized-property-map");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1)).unwrap();
    db.execute_cypher(r#"CREATE (n:Person {name: "Alice", stale: true})"#)
        .unwrap();
    db.execute_cypher(r#"CREATE (n:Person {name: "Bob"})"#)
        .unwrap();

    let mut set_params = QueryParams::new();
    set_params.insert("name".to_string(), Value::String("Alice".to_string()));
    set_params.insert(
        "props".to_string(),
        Value::Map(properties(&[
            ("status", Value::String("active".to_string())),
            ("score", Value::Int(7)),
        ])),
    );
    let rows = db
        .execute_cypher_with_params(
            r#"MATCH (n:Person) WHERE n.name = $name SET n += $props RETURN n"#,
            set_params,
        )
        .unwrap();
    let Some(QueryValue::Node(node)) = rows[0].get("n") else {
        panic!("expected node");
    };
    assert_eq!(
        node.properties.get("status"),
        Some(&Value::String("active".to_string()))
    );
    assert_eq!(node.properties.get("score"), Some(&Value::Int(7)));
    assert_eq!(node.properties.get("stale"), Some(&Value::Bool(true)));

    let mut replace_params = QueryParams::new();
    replace_params.insert("name".to_string(), Value::String("Alice".to_string()));
    replace_params.insert(
        "props".to_string(),
        Value::Map(properties(&[
            ("name", Value::String("Alice".to_string())),
            ("status", Value::String("replaced".to_string())),
        ])),
    );
    let rows = db
        .execute_cypher_with_params(
            r#"MATCH (n:Person) WHERE n.name = $name SET n = $props RETURN n"#,
            replace_params,
        )
        .unwrap();
    let Some(QueryValue::Node(node)) = rows[0].get("n") else {
        panic!("expected node");
    };
    assert_eq!(
        node.properties.get("status"),
        Some(&Value::String("replaced".to_string()))
    );
    assert_eq!(node.properties.get("score"), None);
    assert_eq!(node.properties.get("stale"), None);

    let mut create_params = QueryParams::new();
    create_params.insert(
        "props".to_string(),
        Value::Map(properties(&[
            ("name", Value::String("Carol".to_string())),
            ("status", Value::String("created".to_string())),
        ])),
    );
    let rows = db
        .execute_cypher_with_params(
            r#"CREATE (n:Person {stale: true}) SET n = $props RETURN n"#,
            create_params,
        )
        .unwrap();
    let Some(QueryValue::Node(node)) = rows[0].get("n") else {
        panic!("expected created node");
    };
    assert_eq!(
        node.properties.get("status"),
        Some(&Value::String("created".to_string()))
    );
    assert_eq!(node.properties.get("stale"), None);

    let mut rel_params = QueryParams::new();
    rel_params.insert("from".to_string(), Value::String("Alice".to_string()));
    rel_params.insert("to".to_string(), Value::String("Bob".to_string()));
    rel_params.insert(
        "props".to_string(),
        Value::Map(properties(&[
            ("status", Value::String("connected".to_string())),
            ("weight", Value::Int(3)),
        ])),
    );
    let rows = db
            .execute_cypher_with_params(
                r#"MATCH (a:Person {name: $from}), (b:Person {name: $to}) CREATE (a)-[r:KNOWS {stale: true}]->(b) SET r = $props RETURN r"#,
                rel_params,
            )
            .unwrap();
    let Some(QueryValue::Relationship(relationship)) = rows[0].get("r") else {
        panic!("expected relationship");
    };
    assert_eq!(
        relationship.properties.get("status"),
        Some(&Value::String("connected".to_string()))
    );
    assert_eq!(relationship.properties.get("weight"), Some(&Value::Int(3)));
    assert_eq!(relationship.properties.get("stale"), None);

    let mut invalid_params = QueryParams::new();
    invalid_params.insert("name".to_string(), Value::String("Alice".to_string()));
    invalid_params.insert(
        "props".to_string(),
        Value::Map(properties(&[(
            "nested",
            Value::Map(properties(&[("bad", Value::Bool(true))])),
        )])),
    );
    let err = db
        .execute_cypher_with_params(
            r#"MATCH (n:Person) WHERE n.name = $name SET n += $props"#,
            invalid_params,
        )
        .unwrap_err();
    assert!(err
        .to_string()
        .contains("graph properties do not support nested map values"));

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn execute_cypher_replaces_node_properties_from_map() {
    let dir = temp_dir("facade-cypher-replace-node-map");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1)).unwrap();
    db.execute_cypher(r#"CREATE (n:Person {name: "Alice", status: "old", stale: true})"#)
        .unwrap();

    let rows = db
            .execute_cypher(
                r#"MATCH (n:Person) WHERE n.name = "Alice" SET n = {name: "Alice", status: "active"} RETURN n"#,
            )
            .unwrap();
    assert_eq!(rows.len(), 1);
    let Some(QueryValue::Node(node)) = rows[0].get("n") else {
        panic!("expected node");
    };
    assert_eq!(
        node.properties.get("status"),
        Some(&Value::String("active".to_string()))
    );
    assert!(!node.properties.contains_key("stale"));
    assert_eq!(
        db.query(r#"MATCH (n:Person) WHERE n.stale = true RETURN n"#)
            .unwrap()
            .len(),
        0
    );

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn execute_cypher_removes_node_property_and_updates_indexes() {
    let dir = temp_dir("facade-cypher-remove-node");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 2, 2)).unwrap();
    db.execute_cypher(
        "CREATE VECTOR INDEX person_embedding ON :Person(embedding) DIMENSIONS 2 METRIC cosine",
    )
    .unwrap();
    db.create_node(
        vec!["Person".to_string()],
        properties(&[
            ("name", Value::String("Alice".to_string())),
            ("status", Value::String("active".to_string())),
            ("embedding", Value::Vector(vec![1.0, 0.0])),
        ]),
    )
    .unwrap();

    let returned = db
            .execute_cypher(
                r#"MATCH (n:Person) WHERE n.name = "Alice" REMOVE n.status, n.embedding RETURN n.status"#,
            )
            .unwrap();
    assert_eq!(
        returned[0].get("n.status"),
        Some(&QueryValue::Scalar(Value::Null))
    );

    assert!(db
        .query(r#"MATCH (n:Person) WHERE n.status = "active" RETURN n"#)
        .unwrap()
        .is_empty());
    assert_eq!(
        db.query(r#"MATCH (n:Person) WHERE n.name = "Alice" RETURN n"#)
            .unwrap()
            .len(),
        1
    );

    assert!(db
        .query("MATCH (n:Person) WHERE vector.knn(n.embedding, [1.0, 0.0], 1) RETURN n")
        .unwrap()
        .is_empty());

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn execute_cypher_removes_relationship_property() {
    let dir = temp_dir("facade-cypher-remove-relationship");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 2, 2)).unwrap();
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
    db.create_relationship(
        alice,
        bob,
        "KNOWS".to_string(),
        properties(&[
            ("status", Value::String("active".to_string())),
            ("weight", Value::Int(7)),
        ]),
    )
    .unwrap();

    let returned = db
            .execute_cypher(
                r#"MATCH (a:Person)-[r:KNOWS]->(b:Person) WHERE r.status = "active" REMOVE r.status, r.weight RETURN r.status"#,
            )
            .unwrap();
    assert_eq!(
        returned[0].get("r.status"),
        Some(&QueryValue::Scalar(Value::Null))
    );

    assert!(db
        .query(r#"MATCH (a:Person)-[r:KNOWS]->(b:Person) WHERE r.status = "active" RETURN r"#)
        .unwrap()
        .is_empty());
    assert!(db
        .query(r#"MATCH (a:Person)-[r:KNOWS]->(b:Person) WHERE r.weight = 7 RETURN r"#)
        .unwrap()
        .is_empty());
    assert_eq!(
        db.query(r#"MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN r"#)
            .unwrap()
            .len(),
        1
    );

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn execute_cypher_merges_relationship_idempotently() {
    let dir = temp_dir("facade-cypher-merge-relationship");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 2, 2)).unwrap();
    db.create_node(
        vec!["Person".to_string()],
        properties(&[("name", Value::String("Alice".to_string()))]),
    )
    .unwrap();
    db.create_node(
        vec!["Person".to_string()],
        properties(&[("name", Value::String("Bob".to_string()))]),
    )
    .unwrap();
    let mut params = QueryParams::new();
    params.insert("from".to_string(), Value::String("Alice".to_string()));
    params.insert("to".to_string(), Value::String("Bob".to_string()));
    params.insert("since".to_string(), Value::Int(2026));

    let query = "MATCH (a:Person) WHERE a.name = $from MATCH (b:Person) WHERE b.name = $to MERGE (a)-[r:KNOWS {since: $since}]->(b) RETURN r";
    let first = db
        .execute_cypher_with_params(query, params.clone())
        .unwrap();
    let second = db
        .execute_cypher_with_params(query, params.clone())
        .unwrap();
    assert_eq!(first.len(), 1);
    assert_eq!(second.len(), 1);
    let Some(QueryValue::Relationship(first_relationship)) = first[0].get("r") else {
        panic!("expected first MERGE to return relationship");
    };
    let Some(QueryValue::Relationship(second_relationship)) = second[0].get("r") else {
        panic!("expected second MERGE to return relationship");
    };
    assert_eq!(first_relationship.id, second_relationship.id);
    assert_eq!(
        first_relationship.properties.get("since"),
        Some(&Value::Int(2026))
    );
    assert_eq!(
        db.query("MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN r")
            .unwrap()
            .len(),
        1
    );

    let rows = db
        .execute_cypher_with_params(
            "MATCH (a:Person {name: $from}), (b:Person {name: $to}) CREATE (a)-[:LIKES]->(b)",
            params.clone(),
        )
        .unwrap();
    assert!(rows.is_empty());
    assert_eq!(
        db.query("MATCH (a:Person)-[r:LIKES]->(b:Person) RETURN r")
            .unwrap()
            .len(),
        1
    );

    let mut likes_params = params.clone();
    likes_params.insert("reason".to_string(), Value::String("graph".to_string()));
    let rows = db
            .execute_cypher_with_params(
                "MATCH (a:Person {name: $from}), (b:Person {name: $to}) CREATE (a)-[:LIKES {reason: $reason}]->(b)",
                likes_params,
            )
            .unwrap();
    assert!(rows.is_empty());
    let rows = db
        .query("MATCH (a:Person)-[r:LIKES]->(b:Person) RETURN r")
        .unwrap();
    assert_eq!(rows.len(), 2);
    assert!(rows.iter().any(|row| {
        matches!(
            row.get("r"),
            Some(QueryValue::Relationship(relationship))
                if relationship.properties.get("reason")
                    == Some(&Value::String("graph".to_string()))
        )
    }));

    let first_merge = db
        .execute_cypher_with_params(
            "MATCH (a:Person {name: $from}), (b:Person {name: $to}) MERGE (a)-[:FOLLOWS]->(b)",
            params.clone(),
        )
        .unwrap();
    let second_merge = db
        .execute_cypher_with_params(
            "MATCH (a:Person {name: $from}), (b:Person {name: $to}) MERGE (a)-[:FOLLOWS]->(b)",
            params.clone(),
        )
        .unwrap();
    assert!(first_merge.is_empty());
    assert!(second_merge.is_empty());
    assert_eq!(
        db.query("MATCH (a:Person)-[r:FOLLOWS]->(b:Person) RETURN r")
            .unwrap()
            .len(),
        1
    );

    let mut follows_params = params.clone();
    follows_params.insert("channel".to_string(), Value::String("email".to_string()));
    let first_merge = db
            .execute_cypher_with_params(
                "MATCH (a:Person {name: $from}), (b:Person {name: $to}) MERGE (a)-[:FOLLOWS {channel: $channel}]->(b)",
                follows_params.clone(),
            )
            .unwrap();
    let second_merge = db
            .execute_cypher_with_params(
                "MATCH (a:Person {name: $from}), (b:Person {name: $to}) MERGE (a)-[:FOLLOWS {channel: $channel}]->(b)",
                follows_params,
            )
            .unwrap();
    assert!(first_merge.is_empty());
    assert!(second_merge.is_empty());
    let rows = db
        .query("MATCH (a:Person)-[r:FOLLOWS]->(b:Person) RETURN r")
        .unwrap();
    assert_eq!(rows.len(), 2);
    assert!(rows.iter().any(|row| {
        matches!(
            row.get("r"),
            Some(QueryValue::Relationship(relationship))
                if relationship.properties.get("channel")
                    == Some(&Value::String("email".to_string()))
        )
    }));

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn execute_cypher_merge_relationship_applies_on_create_and_on_match_set() {
    let dir = temp_dir("facade-cypher-merge-relationship-on-set");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 2, 2)).unwrap();
    db.create_node(
        vec!["Person".to_string()],
        properties(&[("name", Value::String("Alice".to_string()))]),
    )
    .unwrap();
    db.create_node(
        vec!["Person".to_string()],
        properties(&[("name", Value::String("Bob".to_string()))]),
    )
    .unwrap();
    let query = "MATCH (a:Person) WHERE a.name = $from MATCH (b:Person) WHERE b.name = $to MERGE (a)-[r:KNOWS {since: $since}]->(b) ON CREATE SET r.created = $created ON MATCH SET r.seen = $seen RETURN r";
    let mut params = QueryParams::new();
    params.insert("from".to_string(), Value::String("Alice".to_string()));
    params.insert("to".to_string(), Value::String("Bob".to_string()));
    params.insert("since".to_string(), Value::Int(2026));
    params.insert("created".to_string(), Value::Int(1));
    params.insert("seen".to_string(), Value::Int(10));

    let first = db
        .execute_cypher_with_params(query, params.clone())
        .unwrap();
    let Some(QueryValue::Relationship(first_relationship)) = first[0].get("r") else {
        panic!("expected created relationship");
    };
    assert_eq!(
        first_relationship.properties.get("created"),
        Some(&Value::Int(1))
    );
    assert_eq!(first_relationship.properties.get("seen"), None);

    let second = db.execute_cypher_with_params(query, params).unwrap();
    let Some(QueryValue::Relationship(second_relationship)) = second[0].get("r") else {
        panic!("expected matched relationship");
    };
    assert_eq!(first_relationship.id, second_relationship.id);
    assert_eq!(
        second_relationship.properties.get("created"),
        Some(&Value::Int(1))
    );
    assert_eq!(
        second_relationship.properties.get("seen"),
        Some(&Value::Int(10))
    );
    assert_eq!(
        db.query("MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN r")
            .unwrap()
            .len(),
        1
    );

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn execute_cypher_merge_relationship_replaces_properties_from_on_set_maps() {
    let dir = temp_dir("facade-cypher-merge-relationship-on-replace-map");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1)).unwrap();
    db.execute_cypher(r#"CREATE (n:Person {name: "Alice"})"#)
        .unwrap();
    db.execute_cypher(r#"CREATE (n:Person {name: "Bob"})"#)
        .unwrap();

    let query = r#"MATCH (a:Person) WHERE a.name = "Alice" MATCH (b:Person) WHERE b.name = "Bob" MERGE (a)-[r:KNOWS {since: 2026}]->(b) ON CREATE SET r = {since: 2026, created: 1} ON MATCH SET r = {since: 2026, seen: 1} RETURN r"#;
    let first = db.execute_cypher(query).unwrap();
    let Some(QueryValue::Relationship(relationship)) = first[0].get("r") else {
        panic!("expected created relationship");
    };
    assert_eq!(relationship.properties.get("created"), Some(&Value::Int(1)));
    assert_eq!(relationship.properties.get("seen"), None);

    let second = db.execute_cypher(query).unwrap();
    let Some(QueryValue::Relationship(relationship)) = second[0].get("r") else {
        panic!("expected matched relationship");
    };
    assert_eq!(relationship.properties.get("seen"), Some(&Value::Int(1)));
    assert_eq!(relationship.properties.get("created"), None);
    assert_eq!(
        db.query("MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN r")
            .unwrap()
            .len(),
        1
    );

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn execute_cypher_mutation_batch_group_commits_single_shard_writes() {
    let dir = temp_dir("facade-cypher-batch-set");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1)).unwrap();
    db.create_node(
        vec!["Person".to_string()],
        properties(&[("name", Value::String("Alice".to_string()))]),
    )
    .unwrap();
    db.create_node(
        vec!["Person".to_string()],
        properties(&[("name", Value::String("Bob".to_string()))]),
    )
    .unwrap();

    let mut alice = QueryParams::new();
    alice.insert("name".to_string(), Value::String("Alice".to_string()));
    alice.insert("status".to_string(), Value::String("active".to_string()));
    alice.insert("score".to_string(), Value::Int(1));
    let mut bob = QueryParams::new();
    bob.insert("name".to_string(), Value::String("Bob".to_string()));
    bob.insert("status".to_string(), Value::String("active".to_string()));
    bob.insert("score".to_string(), Value::Int(2));

    let write_count = db
            .execute_cypher_mutation_batch_on_shard(
                0,
                vec![
                    (
                        "MATCH (n:Person) WHERE n.name = $name SET n.status = $status, n.score = $score".to_string(),
                        alice,
                    ),
                    (
                        "MATCH (n:Person) WHERE n.name = $name SET n.status = $status, n.score = $score".to_string(),
                        bob,
                    ),
                ],
            )
            .unwrap();

    assert_eq!(write_count, 4);
    let rows = db
        .query(r#"MATCH (n:Person) WHERE n.status = "active" RETURN n.name"#)
        .unwrap();
    assert_eq!(rows.len(), 2);
    let rows = db
        .query("MATCH (n:Person) WHERE n.score = 2 RETURN n.name")
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get("n.name"),
        Some(&QueryValue::Scalar(Value::String("Bob".to_string())))
    );
    assert_eq!(db.committed_indexes().unwrap(), vec![6]);

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn execute_cypher_mutation_batch_replaces_property_maps() {
    let dir = temp_dir("facade-cypher-batch-replace-map");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1)).unwrap();
    db.execute_cypher(r#"CREATE (n:Person {name: "Alice", stale: true, score: 1})"#)
        .unwrap();

    let write_count = db
            .execute_cypher_mutation_batch_on_shard(
                0,
                vec![(
                    r#"MATCH (n:Person) WHERE n.name = "Alice" SET n = {name: "Alice", status: "active"}"#
                        .to_string(),
                    QueryParams::new(),
                )],
            )
            .unwrap();

    assert_eq!(write_count, 3);
    let rows = db
        .query(r#"MATCH (n:Person) WHERE n.status = "active" RETURN n.score"#)
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get("n.score"),
        Some(&QueryValue::Scalar(Value::Null))
    );
    assert!(db
        .query(r#"MATCH (n:Person) WHERE n.stale = true RETURN n"#)
        .unwrap()
        .is_empty());

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn execute_cypher_mutation_batch_set_null_removes_property() {
    let dir = temp_dir("facade-cypher-batch-set-null");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1)).unwrap();
    db.execute_cypher(r#"CREATE (n:Person {name: "Alice", status: "active"})"#)
        .unwrap();

    let write_count = db
        .execute_cypher_mutation_batch_on_shard(
            0,
            vec![(
                r#"MATCH (n:Person) WHERE n.name = "Alice" SET n.status = null"#.to_string(),
                QueryParams::new(),
            )],
        )
        .unwrap();

    assert_eq!(write_count, 1);
    assert_eq!(db.committed_indexes().unwrap(), vec![2]);
    assert!(db
        .query(r#"MATCH (n:Person) WHERE n.status = "active" RETURN n"#)
        .unwrap()
        .is_empty());
    let rows = db
        .query(r#"MATCH (n:Person) WHERE n.name = "Alice" RETURN n.status"#)
        .unwrap();
    assert_eq!(
        rows[0].get("n.status"),
        Some(&QueryValue::Scalar(Value::Null))
    );

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn execute_cypher_mutation_batch_group_commits_single_shard_creates() {
    let dir = temp_dir("facade-cypher-batch-create");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1)).unwrap();
    db.create_node(
        vec!["Person".to_string()],
        properties(&[("name", Value::String("Alice".to_string()))]),
    )
    .unwrap();
    db.create_node(
        vec!["Person".to_string()],
        properties(&[("name", Value::String("Bob".to_string()))]),
    )
    .unwrap();

    let mut carol = QueryParams::new();
    carol.insert("name".to_string(), Value::String("Carol".to_string()));
    carol.insert("status".to_string(), Value::String("new".to_string()));
    let mut knows = QueryParams::new();
    knows.insert("from".to_string(), Value::String("Alice".to_string()));
    knows.insert("to".to_string(), Value::String("Bob".to_string()));
    knows.insert("weight".to_string(), Value::Int(9));
    knows.insert(
        "rel_status".to_string(),
        Value::String("created".to_string()),
    );

    let write_count = db
            .execute_cypher_mutation_batch_on_shard(
                0,
                vec![
                    (
                        "CREATE (n:Person {name: $name}) SET n.status = $status RETURN n"
                            .to_string(),
                        carol,
                    ),
                    (
                        "MATCH (a:Person {name: $from}), (b:Person {name: $to}) CREATE (a)-[r:KNOWS {weight: $weight}]->(b) SET r.status = $rel_status RETURN r"
                            .to_string(),
                        knows,
                    ),
                ],
            )
            .unwrap();

    assert_eq!(write_count, 2);
    let rows = db
        .query(r#"MATCH (n:Person) WHERE n.name = "Carol" RETURN n.status"#)
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get("n.status"),
        Some(&QueryValue::Scalar(Value::String("new".to_string())))
    );
    let rows = db
        .query("MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN r")
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get("r").and_then(|value| match value {
            QueryValue::Relationship(relationship) => relationship.properties.get("weight"),
            _ => None,
        }),
        Some(&Value::Int(9))
    );
    assert_eq!(
        rows[0].get("r").and_then(|value| match value {
            QueryValue::Relationship(relationship) => relationship.properties.get("status"),
            _ => None,
        }),
        Some(&Value::String("created".to_string()))
    );
    assert_eq!(db.committed_indexes().unwrap(), vec![4]);

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn execute_cypher_mutation_batch_group_commits_create_property_replacements() {
    let dir = temp_dir("facade-cypher-batch-create-replace-map");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1)).unwrap();
    db.create_node(
        vec!["Person".to_string()],
        properties(&[("name", Value::String("Alice".to_string()))]),
    )
    .unwrap();
    db.create_node(
        vec!["Person".to_string()],
        properties(&[("name", Value::String("Bob".to_string()))]),
    )
    .unwrap();

    let write_count = db
            .execute_cypher_mutation_batch_on_shard(
                0,
                vec![
                    (
                        r#"CREATE (n:Person {name: "Carol", stale: true}) SET n = {name: "Carol", status: "created"} RETURN n"#
                            .to_string(),
                        QueryParams::new(),
                    ),
                    (
                        r#"MATCH (a:Person {name: "Alice"}), (b:Person {name: "Bob"}) CREATE (a)-[r:KNOWS {stale: true}]->(b) SET r = {status: "created"} RETURN r"#
                            .to_string(),
                        QueryParams::new(),
                    ),
                ],
            )
            .unwrap();

    assert_eq!(write_count, 2);
    let rows = db
        .query(r#"MATCH (n:Person) WHERE n.name = "Carol" RETURN n"#)
        .unwrap();
    let Some(QueryValue::Node(node)) = rows[0].get("n") else {
        panic!("expected node");
    };
    assert_eq!(
        node.properties.get("status"),
        Some(&Value::String("created".to_string()))
    );
    assert!(!node.properties.contains_key("stale"));
    let rows = db
        .query(r#"MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN r"#)
        .unwrap();
    let Some(QueryValue::Relationship(relationship)) = rows[0].get("r") else {
        panic!("expected relationship");
    };
    assert_eq!(
        relationship.properties.get("status"),
        Some(&Value::String("created".to_string()))
    );
    assert!(!relationship.properties.contains_key("stale"));

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn execute_cypher_mutation_batch_group_commits_merge_node() {
    let dir = temp_dir("facade-cypher-batch-merge-node");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1)).unwrap();

    let query = "MERGE (n:Person {email: $email}) ON CREATE SET n.created = $created ON MATCH SET n.seen = $seen RETURN n";
    let mut params = QueryParams::new();
    params.insert(
        "email".to_string(),
        Value::String("alice@example.com".to_string()),
    );
    params.insert("created".to_string(), Value::Int(1));
    params.insert("seen".to_string(), Value::Int(2));

    let created = db
        .execute_cypher_mutation_batch_on_shard(0, vec![(query.to_string(), params.clone())])
        .unwrap();
    assert_eq!(created, 1);
    let matched = db
        .execute_cypher_mutation_batch_on_shard(0, vec![(query.to_string(), params)])
        .unwrap();
    assert_eq!(matched, 1);

    let rows = db
        .query(r#"MATCH (n:Person) WHERE n.email = "alice@example.com" RETURN n"#)
        .unwrap();
    assert_eq!(rows.len(), 1);
    let Some(QueryValue::Node(node)) = rows[0].get("n") else {
        panic!("expected node");
    };
    assert_eq!(node.properties.get("created"), Some(&Value::Int(1)));
    assert_eq!(node.properties.get("seen"), Some(&Value::Int(2)));
    assert_eq!(db.committed_indexes().unwrap(), vec![2]);

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn execute_cypher_mutation_batch_group_commits_merge_node_replacement_maps() {
    let dir = temp_dir("facade-cypher-batch-merge-node-replace-map");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1)).unwrap();

    let query = r#"MERGE (n:Person {email: $email}) ON CREATE SET n = {email: $email, created: $created} ON MATCH SET n = {email: $email, seen: $seen} RETURN n"#;
    let mut params = QueryParams::new();
    params.insert(
        "email".to_string(),
        Value::String("alice@example.com".to_string()),
    );
    params.insert("created".to_string(), Value::Int(1));
    params.insert("seen".to_string(), Value::Int(2));

    let created = db
        .execute_cypher_mutation_batch_on_shard(0, vec![(query.to_string(), params.clone())])
        .unwrap();
    assert_eq!(created, 1);
    let matched = db
        .execute_cypher_mutation_batch_on_shard(0, vec![(query.to_string(), params)])
        .unwrap();
    assert_eq!(matched, 2);

    let rows = db
        .query(r#"MATCH (n:Person) WHERE n.email = "alice@example.com" RETURN n"#)
        .unwrap();
    assert_eq!(rows.len(), 1);
    let Some(QueryValue::Node(node)) = rows[0].get("n") else {
        panic!("expected node");
    };
    assert_eq!(node.properties.get("created"), None);
    assert_eq!(node.properties.get("seen"), Some(&Value::Int(2)));
    assert_eq!(db.committed_indexes().unwrap(), vec![3]);

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn execute_cypher_mutation_batch_group_commits_merge_relationship() {
    let dir = temp_dir("facade-cypher-batch-merge-relationship");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1)).unwrap();
    db.create_node(
        vec!["Person".to_string()],
        properties(&[("name", Value::String("Alice".to_string()))]),
    )
    .unwrap();
    db.create_node(
        vec!["Person".to_string()],
        properties(&[("name", Value::String("Bob".to_string()))]),
    )
    .unwrap();

    let query = "MATCH (a:Person) WHERE a.name = $from MATCH (b:Person) WHERE b.name = $to MERGE (a)-[r:KNOWS {since: $since}]->(b) ON CREATE SET r.created = $created ON MATCH SET r.seen = $seen RETURN r";
    let mut params = QueryParams::new();
    params.insert("from".to_string(), Value::String("Alice".to_string()));
    params.insert("to".to_string(), Value::String("Bob".to_string()));
    params.insert("since".to_string(), Value::Int(2026));
    params.insert("created".to_string(), Value::Int(1));
    params.insert("seen".to_string(), Value::Int(2));

    let created = db
        .execute_cypher_mutation_batch_on_shard(0, vec![(query.to_string(), params.clone())])
        .unwrap();
    assert_eq!(created, 1);
    let matched = db
        .execute_cypher_mutation_batch_on_shard(0, vec![(query.to_string(), params)])
        .unwrap();
    assert_eq!(matched, 1);

    let rows = db
        .query("MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN r")
        .unwrap();
    assert_eq!(rows.len(), 1);
    let Some(QueryValue::Relationship(relationship)) = rows[0].get("r") else {
        panic!("expected relationship");
    };
    assert_eq!(
        relationship.properties.get("since"),
        Some(&Value::Int(2026))
    );
    assert_eq!(relationship.properties.get("created"), Some(&Value::Int(1)));
    assert_eq!(relationship.properties.get("seen"), Some(&Value::Int(2)));
    assert_eq!(db.committed_indexes().unwrap(), vec![4]);

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn execute_cypher_mutation_batch_group_commits_merge_relationship_replacement_maps() {
    let dir = temp_dir("facade-cypher-batch-merge-relationship-replace-map");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1)).unwrap();
    db.create_node(
        vec!["Person".to_string()],
        properties(&[("name", Value::String("Alice".to_string()))]),
    )
    .unwrap();
    db.create_node(
        vec!["Person".to_string()],
        properties(&[("name", Value::String("Bob".to_string()))]),
    )
    .unwrap();

    let query = r#"MATCH (a:Person) WHERE a.name = $from MATCH (b:Person) WHERE b.name = $to MERGE (a)-[r:KNOWS {since: $since}]->(b) ON CREATE SET r = {since: $since, created: $created} ON MATCH SET r = {since: $since, seen: $seen} RETURN r"#;
    let mut params = QueryParams::new();
    params.insert("from".to_string(), Value::String("Alice".to_string()));
    params.insert("to".to_string(), Value::String("Bob".to_string()));
    params.insert("since".to_string(), Value::Int(2026));
    params.insert("created".to_string(), Value::Int(1));
    params.insert("seen".to_string(), Value::Int(2));

    let created = db
        .execute_cypher_mutation_batch_on_shard(0, vec![(query.to_string(), params.clone())])
        .unwrap();
    assert_eq!(created, 1);
    let matched = db
        .execute_cypher_mutation_batch_on_shard(0, vec![(query.to_string(), params)])
        .unwrap();
    assert_eq!(matched, 2);

    let rows = db
        .query("MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN r")
        .unwrap();
    assert_eq!(rows.len(), 1);
    let Some(QueryValue::Relationship(relationship)) = rows[0].get("r") else {
        panic!("expected relationship");
    };
    assert_eq!(relationship.properties.get("created"), None);
    assert_eq!(relationship.properties.get("seen"), Some(&Value::Int(2)));
    assert_eq!(db.committed_indexes().unwrap(), vec![5]);

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn execute_cypher_mutation_batch_group_commits_multiple_local_shards() {
    let dir = temp_dir("facade-cypher-batch-multi-shard");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 2, 2)).unwrap();
    db.create_node_on_shard(
        0,
        vec!["Person".to_string()],
        properties(&[("name", Value::String("Alice".to_string()))]),
    )
    .unwrap();
    db.create_node_on_shard(
        1,
        vec!["Person".to_string()],
        properties(&[("name", Value::String("Bob".to_string()))]),
    )
    .unwrap();

    let mut params = QueryParams::new();
    params.insert("status".to_string(), Value::String("active".to_string()));
    let write_count = db
        .execute_cypher_mutation_batch(vec![(
            "MATCH (n:Person) SET n.status = $status".to_string(),
            params,
        )])
        .unwrap();

    assert_eq!(write_count, 2);
    assert_eq!(
        db.query(r#"MATCH (n:Person) WHERE n.status = "active" RETURN n"#)
            .unwrap()
            .len(),
        2
    );
    assert_eq!(db.committed_indexes().unwrap(), vec![2, 2]);

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn execute_cypher_deletes_node() {
    let dir = temp_dir("facade-cypher-delete");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 2, 2)).unwrap();
    db.create_node(
        vec!["Person".to_string()],
        properties(&[("name", Value::String("Alice".to_string()))]),
    )
    .unwrap();
    db.create_node(
        vec!["Person".to_string()],
        properties(&[("name", Value::String("Bob".to_string()))]),
    )
    .unwrap();

    let rows = db
        .execute_cypher(r#"MATCH (n:Person) WHERE n.name = "Alice" DELETE n RETURN n.name"#)
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get("n.name"),
        Some(&QueryValue::Scalar(Value::String("Alice".to_string())))
    );

    assert_eq!(db.query("MATCH (n:Person) RETURN n").unwrap().len(), 1);
    assert_eq!(
        db.query("MATCH (n:Person) RETURN n.name").unwrap()[0].get("n.name"),
        Some(&QueryValue::Scalar(Value::String("Bob".to_string())))
    );

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn execute_cypher_deletes_node_with_parameterized_matcher() {
    let dir = temp_dir("facade-cypher-delete-param");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 2, 2)).unwrap();
    db.create_node(
        vec!["Person".to_string()],
        properties(&[("name", Value::String("Alice".to_string()))]),
    )
    .unwrap();
    db.create_node(
        vec!["Person".to_string()],
        properties(&[("name", Value::String("Bob".to_string()))]),
    )
    .unwrap();

    let rows = db
        .execute_cypher_with_params(
            "MATCH (n:Person {name: $name}) DELETE n RETURN n.name",
            [("name".to_string(), Value::String("Alice".to_string()))]
                .into_iter()
                .collect(),
        )
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get("n.name"),
        Some(&QueryValue::Scalar(Value::String("Alice".to_string())))
    );
    assert_eq!(
        db.query("MATCH (n:Person) RETURN n.name").unwrap()[0].get("n.name"),
        Some(&QueryValue::Scalar(Value::String("Bob".to_string())))
    );

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn execute_cypher_detach_deletes_node_and_relationships() {
    let dir = temp_dir("facade-cypher-detach-delete");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 2, 2)).unwrap();
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
    let carol = db
        .create_node(
            vec!["Person".to_string()],
            properties(&[("name", Value::String("Carol".to_string()))]),
        )
        .unwrap();
    db.create_relationship(alice, bob, "KNOWS".to_string(), Properties::new())
        .unwrap();
    db.create_relationship(carol, alice, "KNOWS".to_string(), Properties::new())
        .unwrap();

    let rows = db
        .execute_cypher(r#"MATCH (n:Person) WHERE n.name = "Alice" DETACH DELETE n RETURN n.name"#)
        .unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get("n.name"),
        Some(&QueryValue::Scalar(Value::String("Alice".to_string())))
    );
    assert_eq!(db.query("MATCH (n:Person) RETURN n").unwrap().len(), 2);
    assert!(db
        .query("MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN r")
        .unwrap()
        .is_empty());

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn execute_cypher_detach_deletes_node_with_parameterized_where() {
    let dir = temp_dir("facade-cypher-detach-delete-param");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 2, 2)).unwrap();
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
    db.create_relationship(alice, bob, "KNOWS".to_string(), Properties::new())
        .unwrap();

    let rows = db
        .execute_cypher_with_params(
            "MATCH (n:Person) WHERE n.name = $name DETACH DELETE n RETURN n.name",
            [("name".to_string(), Value::String("Alice".to_string()))]
                .into_iter()
                .collect(),
        )
        .unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get("n.name"),
        Some(&QueryValue::Scalar(Value::String("Alice".to_string())))
    );
    assert_eq!(db.query("MATCH (n:Person) RETURN n").unwrap().len(), 1);
    assert!(db
        .query("MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN r")
        .unwrap()
        .is_empty());

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn execute_cypher_creates_sets_and_deletes_relationships() {
    let dir = temp_dir("facade-cypher-relationship-cud");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 2, 2)).unwrap();
    db.create_node(
        vec!["Person".to_string()],
        properties(&[("name", Value::String("Alice".to_string()))]),
    )
    .unwrap();
    db.create_node(
        vec!["Person".to_string()],
        properties(&[("name", Value::String("Bob".to_string()))]),
    )
    .unwrap();
    let mut params = QueryParams::new();
    params.insert("from".to_string(), Value::String("Alice".to_string()));
    params.insert("to".to_string(), Value::String("Bob".to_string()));
    params.insert("since".to_string(), Value::Int(2026));
    params.insert("status".to_string(), Value::String("new".to_string()));

    let rows = db
            .execute_cypher_with_params(
                "MATCH (a:Person {name: $from}) MATCH (b:Person {name: $to}) CREATE (a)-[r:KNOWS {since: $since}]->(b) SET r.status = $status RETURN r",
                params,
            )
            .unwrap();

    assert_eq!(rows.len(), 1);
    let Some(QueryValue::Relationship(relationship)) = rows[0].get("r") else {
        panic!("expected created relationship in RETURN r");
    };
    assert_eq!(relationship.rel_type, "KNOWS");
    assert_eq!(
        relationship.properties.get("since"),
        Some(&Value::Int(2026))
    );
    assert_eq!(
        relationship.properties.get("status"),
        Some(&Value::String("new".to_string()))
    );

    let rows = db
        .execute_cypher(
            "MATCH (a:Person)-[r:KNOWS]->(b:Person) WHERE r.since = 2026 DELETE r RETURN r.since",
        )
        .unwrap();
    assert_eq!(rows.len(), 1);
    let rows = db
            .execute_cypher(
                "MATCH (a:Person {name: \"Alice\"}) MATCH (b:Person {name: \"Bob\"}) CREATE (a)-[r:KNOWS {since: 2027, stale: true}]->(b) SET r = {status: \"created\"} RETURN r",
            )
            .unwrap();
    let Some(QueryValue::Relationship(relationship)) = rows[0].get("r") else {
        panic!("expected relationship in RETURN r");
    };
    assert_eq!(
        relationship.properties.get("status"),
        Some(&Value::String("created".to_string()))
    );
    assert!(!relationship.properties.contains_key("since"));
    assert!(!relationship.properties.contains_key("stale"));

    db.execute_cypher(
            "MATCH (a:Person)-[r:KNOWS]->(b:Person) WHERE r.status = \"created\" SET r.status = \"active\", r.weight = 7",
        )
        .unwrap();
    let rows = db
            .query("MATCH (a:Person)-[r:KNOWS]->(b:Person) WHERE r.status = \"active\" AND r.weight = 7 RETURN r")
            .unwrap();
    let Some(QueryValue::Relationship(relationship)) = rows[0].get("r") else {
        panic!("expected relationship in RETURN r");
    };
    assert_eq!(
        relationship.properties.get("status"),
        Some(&Value::String("active".to_string()))
    );
    assert_eq!(relationship.properties.get("weight"), Some(&Value::Int(7)));

    let rows = db
            .execute_cypher(
                "MATCH (a:Person)-[r:KNOWS]->(b:Person) WHERE r.weight = 7 SET r += {status: \"reviewed\", score: 99} RETURN r",
            )
            .unwrap();
    let Some(QueryValue::Relationship(relationship)) = rows[0].get("r") else {
        panic!("expected relationship in RETURN r");
    };
    assert_eq!(
        relationship.properties.get("status"),
        Some(&Value::String("reviewed".to_string()))
    );
    assert_eq!(relationship.properties.get("score"), Some(&Value::Int(99)));

    let rows = db
            .execute_cypher(
                "MATCH (a:Person)-[r:KNOWS]->(b:Person) WHERE r.score = 99 SET r = {status: \"final\"} RETURN r",
            )
            .unwrap();
    let Some(QueryValue::Relationship(relationship)) = rows[0].get("r") else {
        panic!("expected relationship in RETURN r");
    };
    assert_eq!(
        relationship.properties.get("status"),
        Some(&Value::String("final".to_string()))
    );
    assert!(!relationship.properties.contains_key("score"));
    assert!(!relationship.properties.contains_key("weight"));

    let rows = db
            .execute_cypher(
                "MATCH (a:Person)-[r:KNOWS]->(b:Person) WHERE r.status = \"final\" SET r.status = null RETURN r.status",
            )
            .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get("r.status"),
        Some(&QueryValue::Scalar(Value::Null))
    );
    assert!(db
        .query("MATCH (a:Person)-[r:KNOWS]->(b:Person) WHERE r.status = \"final\" RETURN r")
        .unwrap()
        .is_empty());

    db.execute_cypher(
        "MATCH (a:Person)-[r:KNOWS]->(b:Person) SET r += {status: \"final\", stale: null}",
    )
    .unwrap();
    let rows = db
        .query("MATCH (a:Person)-[r:KNOWS]->(b:Person) WHERE r.status = \"final\" RETURN r")
        .unwrap();
    let Some(QueryValue::Relationship(relationship)) = rows[0].get("r") else {
        panic!("expected relationship in RETURN r");
    };
    assert_eq!(
        relationship.properties.get("status"),
        Some(&Value::String("final".to_string()))
    );
    assert!(!relationship.properties.contains_key("stale"));

    let rows = db
            .execute_cypher(
                "MATCH (a:Person)-[r:KNOWS]->(b:Person) WHERE r.status = \"final\" DELETE r RETURN r.status",
            )
            .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get("r.status"),
        Some(&QueryValue::Scalar(Value::String("final".to_string())))
    );
    assert!(db
        .query("MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN r")
        .unwrap()
        .is_empty());

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn execute_cypher_creates_and_merges_relationships_from_comma_match() {
    let dir = temp_dir("facade-cypher-comma-match-relationship-write");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 2, 2)).unwrap();
    db.create_node(
        vec!["Person".to_string()],
        properties(&[("name", Value::String("Alice".to_string()))]),
    )
    .unwrap();
    db.create_node(
        vec!["Person".to_string()],
        properties(&[("name", Value::String("Bob".to_string()))]),
    )
    .unwrap();

    let mut params = QueryParams::new();
    params.insert("from".to_string(), Value::String("Alice".to_string()));
    params.insert("to".to_string(), Value::String("Bob".to_string()));
    params.insert("since".to_string(), Value::Int(2026));
    let rows = db
            .execute_cypher_with_params(
                "MATCH (a:Person {name: $from}), (b:Person {name: $to}) CREATE (a)-[r:KNOWS {since: $since}]->(b) RETURN r",
                params.clone(),
            )
            .unwrap();
    assert_eq!(rows.len(), 1);

    let first_merge = db
            .execute_cypher_with_params(
                "MATCH (a:Person {name: $from}), (b:Person {name: $to}) MERGE (a)-[r:KNOWS {since: $since}]->(b) RETURN r",
                params.clone(),
            )
            .unwrap();
    let second_merge = db
            .execute_cypher_with_params(
                "MATCH (a:Person {name: $from}), (b:Person {name: $to}) MERGE (a)-[r:KNOWS {since: $since}]->(b) RETURN r",
                params,
            )
            .unwrap();

    assert_eq!(first_merge.len(), 1);
    assert_eq!(second_merge.len(), 1);
    assert_eq!(
        db.query("MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN r")
            .unwrap()
            .len(),
        1
    );

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn execute_cypher_relationship_cud_matches_parameterized_pattern_properties() {
    let dir = temp_dir("facade-cypher-relationship-cud-params");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 2, 2)).unwrap();
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
    let carol = db
        .create_node(
            vec!["Person".to_string()],
            properties(&[("name", Value::String("Carol".to_string()))]),
        )
        .unwrap();
    db.create_relationship(
        alice,
        bob,
        "KNOWS".to_string(),
        properties(&[("since", Value::Int(2026))]),
    )
    .unwrap();
    db.create_relationship(
        alice,
        carol,
        "KNOWS".to_string(),
        properties(&[("since", Value::Int(2027))]),
    )
    .unwrap();

    let mut set_params = QueryParams::new();
    set_params.insert("since".to_string(), Value::Int(2026));
    set_params.insert("status".to_string(), Value::String("active".to_string()));
    let rows = db
            .execute_cypher_with_params(
                "MATCH (a:Person)-[r:KNOWS {since: $since}]->(b:Person) SET r.status = $status RETURN r.status",
                set_params,
            )
            .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get("r.status"),
        Some(&QueryValue::Scalar(Value::String("active".to_string())))
    );
    assert_eq!(
        db.query(r#"MATCH (a:Person)-[r:KNOWS]->(b:Person) WHERE r.status = "active" RETURN r"#)
            .unwrap()
            .len(),
        1
    );

    let mut remove_params = QueryParams::new();
    remove_params.insert("since".to_string(), Value::Int(2026));
    let rows = db
            .execute_cypher_with_params(
                "MATCH (a:Person)-[r:KNOWS {since: $since}]->(b:Person) REMOVE r.status RETURN r.status",
                remove_params,
            )
            .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get("r.status"),
        Some(&QueryValue::Scalar(Value::Null))
    );
    assert!(db
        .query(r#"MATCH (a:Person)-[r:KNOWS]->(b:Person) WHERE r.status = "active" RETURN r"#)
        .unwrap()
        .is_empty());

    let mut delete_params = QueryParams::new();
    delete_params.insert("since".to_string(), Value::Int(2027));
    let rows = db
        .execute_cypher_with_params(
            "MATCH (a:Person)-[r:KNOWS {since: $since}]->(b:Person) DELETE r RETURN r.since",
            delete_params,
        )
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get("r.since"),
        Some(&QueryValue::Scalar(Value::Int(2027)))
    );
    assert_eq!(
        db.query("MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN r")
            .unwrap()
            .len(),
        1
    );

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn execute_cypher_relationship_cud_matches_parameterized_where_predicates() {
    let dir = temp_dir("facade-cypher-relationship-cud-where-params");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 2, 2)).unwrap();
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
    db.create_relationship(
        alice,
        bob,
        "KNOWS".to_string(),
        properties(&[("since", Value::Int(2026))]),
    )
    .unwrap();
    db.create_relationship(
        alice,
        bob,
        "KNOWS".to_string(),
        properties(&[("since", Value::Int(2027))]),
    )
    .unwrap();

    let rows = db
            .execute_cypher_with_params(
                "MATCH (a:Person)-[r:KNOWS]->(b:Person) WHERE r.since = $since SET r.status = $status RETURN r.status",
                [
                    ("since".to_string(), Value::Int(2026)),
                    ("status".to_string(), Value::String("active".to_string())),
                ]
                .into_iter()
                .collect(),
            )
            .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get("r.status"),
        Some(&QueryValue::Scalar(Value::String("active".to_string())))
    );

    let rows = db
            .execute_cypher_with_params(
                "MATCH (a:Person)-[r:KNOWS]->(b:Person) WHERE r.status = $status REMOVE r.status RETURN r.status",
                [("status".to_string(), Value::String("active".to_string()))]
                    .into_iter()
                    .collect(),
            )
            .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get("r.status"),
        Some(&QueryValue::Scalar(Value::Null))
    );

    let rows = db
        .execute_cypher_with_params(
            "MATCH (a:Person)-[r:KNOWS]->(b:Person) WHERE r.since = $since DELETE r RETURN r.since",
            [("since".to_string(), Value::Int(2027))]
                .into_iter()
                .collect(),
        )
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get("r.since"),
        Some(&QueryValue::Scalar(Value::Int(2027)))
    );
    assert_eq!(
        db.query("MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN r")
            .unwrap()
            .len(),
        1
    );

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn query_cursor_with_params_owns_snapshot_view() {
    let dir = temp_dir("facade-cursor-params");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 2, 2)).unwrap();
    db.create_node(
        vec!["Person".to_string()],
        properties(&[("name", Value::String("Alice".to_string()))]),
    )
    .unwrap();

    let mut params = QueryParams::new();
    params.insert("name".to_string(), Value::String("Alice".to_string()));
    let mut cursor = db
        .query_cursor_with_params(
            "MATCH (n:Person) WHERE n.name = $name RETURN n.name",
            params,
        )
        .unwrap();

    db.create_node(
        vec!["Person".to_string()],
        properties(&[("name", Value::String("Alice".to_string()))]),
    )
    .unwrap();

    let page = cursor.fetch(10);
    assert_eq!(page.rows.len(), 1);
    assert!(!page.has_more);
    assert_eq!(
        db.query(r#"MATCH (n:Person) WHERE n.name = "Alice" RETURN n.name"#)
            .unwrap()
            .len(),
        2
    );

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn full_scan_query_cursor_reads_snapshot_by_id() {
    let dir = temp_dir("facade-full-scan-cursor");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 2, 2)).unwrap();
    db.create_node(vec!["Person".to_string()], Properties::new())
        .unwrap();
    db.create_node(vec!["Company".to_string()], Properties::new())
        .unwrap();

    let mut cursor = db.query_cursor("MATCH (n) RETURN n").unwrap();
    db.create_node(vec!["Later".to_string()], Properties::new())
        .unwrap();

    let first = cursor.fetch(1);
    assert_eq!(first.rows.len(), 1);
    assert!(first.has_more);
    let second = cursor.fetch(10);
    assert_eq!(second.rows.len(), 1);
    assert!(!second.has_more);
    assert_eq!(cursor.total_rows(), Some(2));
    assert_eq!(db.query("MATCH (n) RETURN n").unwrap().len(), 3);

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn traversal_query_cursor_reads_snapshot_by_page() {
    let dir = temp_dir("facade-traversal-cursor");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 2, 2)).unwrap();
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
    let carol = db
        .create_node(
            vec!["Person".to_string()],
            properties(&[("name", Value::String("Carol".to_string()))]),
        )
        .unwrap();
    db.create_relationship(alice, bob, "KNOWS".to_string(), Properties::new())
        .unwrap();
    db.create_relationship(alice, carol, "KNOWS".to_string(), Properties::new())
        .unwrap();

    let mut cursor = db
        .query_cursor(r#"MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE a.name = "Alice" RETURN b"#)
        .unwrap();
    let dave = db
        .create_node(
            vec!["Person".to_string()],
            properties(&[("name", Value::String("Dave".to_string()))]),
        )
        .unwrap();
    db.create_relationship(alice, dave, "KNOWS".to_string(), Properties::new())
        .unwrap();

    assert_eq!(cursor.total_rows(), None);
    let first = cursor.fetch(1);
    assert_eq!(first.rows.len(), 1);
    assert!(first.has_more);
    let second = cursor.fetch(1);
    assert_eq!(second.rows.len(), 1);
    assert!(!second.has_more);
    let third = cursor.fetch(1);
    assert!(third.rows.is_empty());
    assert!(!third.has_more);

    assert_eq!(
        db.query(r#"MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE a.name = "Alice" RETURN b"#)
            .unwrap()
            .len(),
        3
    );

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn local_write_entries_include_origin_and_config_metadata() {
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
fn replicated_entry_is_applied_without_being_local_primary() {
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
fn replicated_vector_indexed_write_is_rejected_before_wal_append() {
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
fn replicated_map_property_write_is_rejected_before_wal_append() {
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
fn replicated_vector_index_validation_uses_batch_overlay() {
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
fn replicated_duplicate_with_different_payload_is_rejected() {
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
fn replicated_entry_rejects_wrong_config_version() {
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
fn local_write_publishes_log_entry_to_replicator() {
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
fn replicated_apply_does_not_publish_entry_again() {
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
fn in_process_replicator_applies_primary_writes_to_replica() {
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
fn install_routing_table_updates_replicator_targets() {
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
fn tcp_replicator_applies_primary_writes_to_replica() {
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
fn tcp_replicator_batches_group_commit_to_replica() {
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
fn tcp_replicator_retries_until_replica_listener_is_available() {
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
fn tcp_replicator_quorum_succeeds_with_one_missing_replica() {
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
fn tcp_replicator_all_policy_fails_with_one_missing_replica() {
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
fn tcp_catch_up_fetches_missing_entries_from_primary_log() {
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
fn tcp_catch_up_can_fetch_missing_entries_in_limited_batches() {
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
fn tcp_catch_up_from_primaries_uses_local_committed_positions() {
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
