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
pub(super) fn vector_index_cache_rebuilds_from_catalog_and_tracks_writes() {
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
pub(super) fn corrupt_vector_index_cache_falls_back_to_rebuild() {
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
pub(super) fn vector_index_rebuild_excludes_removed_properties_and_deleted_nodes() {
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
pub(super) fn dropped_vector_index_stays_absent_after_reopen() {
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
pub(super) fn stale_vector_index_cache_is_rebuilt_after_catalog_definition_changes() {
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
pub(super) fn install_index_catalog_rejects_unique_constraint_violated_by_existing_nodes() {
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
pub(super) fn install_index_catalog_rejects_vector_definition_invalid_for_existing_nodes() {
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
pub(super) fn install_index_catalog_rebuilds_vector_cache_only_after_validation() {
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
pub(super) fn execute_cypher_creates_node_with_params() {
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
pub(super) fn execute_cypher_create_node_applies_set_assignments() {
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
pub(super) fn execute_cypher_create_node_replaces_properties_from_map() {
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
pub(super) fn execute_cypher_create_returns_created_properties() {
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
pub(super) fn execute_cypher_create_node_with_match_creates_relationship() {
    let dir = temp_dir("facade-cypher-create-with-match-relationship");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1)).unwrap();
    db.execute_cypher(r#"CREATE (c:Company {name: "Neo4r Labs"})"#)
        .unwrap();

    let rows = db
        .execute_cypher(
            r#"CREATE (n:Person {name: "Grace", role: "Backend Engineer", age: 31, status: "active"})
WITH n
MATCH (c:Company {name: "Neo4r Labs"})
CREATE (n)-[r:WORKS_AT {since: 2026}]->(c)
RETURN n, r"#,
        )
        .unwrap();

    assert_eq!(rows.len(), 1);
    assert!(matches!(rows[0].get("n"), Some(QueryValue::Node(_))));
    assert!(matches!(
        rows[0].get("r"),
        Some(QueryValue::Relationship(relationship)) if relationship.rel_type == "WORKS_AT"
    ));
    assert_eq!(
        db.query(
            r#"MATCH (a:Person)-[r:WORKS_AT]->(b:Company) WHERE a.name = "Grace" RETURN r.since"#
        )
        .unwrap()[0]
            .get("r.since"),
        Some(&QueryValue::Scalar(Value::Int(2026)))
    );

    let _ = fs::remove_dir_all(dir);
}

#[test]
pub(super) fn execute_cypher_merges_node_idempotently() {
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
pub(super) fn execute_cypher_merge_node_uses_unique_constraint_key() {
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
pub(super) fn execute_cypher_merges_anonymous_node_idempotently() {
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
pub(super) fn execute_cypher_merge_node_applies_on_create_and_on_match_set() {
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
pub(super) fn execute_cypher_merge_node_replaces_properties_from_on_set_maps() {
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
pub(super) fn execute_cypher_sets_node_property() {
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
pub(super) fn execute_cypher_set_null_removes_node_property() {
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
