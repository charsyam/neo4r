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

    let rows = db
        .query("MATCH (n:Person) RETURN count(*) ORDER BY count(*) DESC LIMIT 1")
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get("count(*)"),
        Some(&QueryValue::Scalar(Value::Int(3)))
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
        let lifecycle = db.index_lifecycle_status().unwrap();
        assert_eq!(lifecycle.len(), 1);
        assert_eq!(lifecycle[0].name, "doc_embedding");
        assert_eq!(lifecycle[0].state, "ready");
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
fn index_lifecycle_resumes_building_status_on_reopen() {
    let dir = temp_dir("facade-index-lifecycle-resume");
    let index = IndexDefinition::node_property("person_name", "Person", "name");
    {
        let mut db = Neo4rDatabase::open(DatabaseConfig::new(&dir, 1, 1)).unwrap();
        db.create_node(
            vec!["Person".to_string()],
            properties(&[("name", Value::String("Alice".to_string()))]),
        )
        .unwrap();
        db.add_index_definition(index.clone()).unwrap();
        db.save_index_lifecycle_state(&index, "building", 0, "")
            .unwrap();
    }

    let reopened = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1)).unwrap();
    let lifecycle = reopened.index_lifecycle_status().unwrap();
    assert_eq!(lifecycle.len(), 1);
    assert_eq!(lifecycle[0].name, "person_name");
    assert_eq!(lifecycle[0].state, "ready");
    assert_eq!(lifecycle[0].backfilled_entries, 1);

    drop(reopened);
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
