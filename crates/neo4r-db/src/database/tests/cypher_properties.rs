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
pub(super) fn execute_cypher_sets_multiple_node_properties() {
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
pub(super) fn execute_cypher_write_returns_multiple_items() {
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
pub(super) fn execute_cypher_sets_node_properties_from_map() {
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
pub(super) fn execute_cypher_accepts_parameterized_property_maps() {
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
pub(super) fn execute_cypher_replaces_node_properties_from_map() {
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
pub(super) fn execute_cypher_removes_node_property_and_updates_indexes() {
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
pub(super) fn execute_cypher_removes_relationship_property() {
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
pub(super) fn execute_cypher_merges_relationship_idempotently() {
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
pub(super) fn execute_cypher_merge_relationship_applies_on_create_and_on_match_set() {
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
pub(super) fn execute_cypher_merge_relationship_replaces_properties_from_on_set_maps() {
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
pub(super) fn execute_cypher_mutation_batch_group_commits_single_shard_writes() {
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
pub(super) fn execute_cypher_mutation_batch_replaces_property_maps() {
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
pub(super) fn execute_cypher_mutation_batch_set_null_removes_property() {
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
pub(super) fn execute_cypher_mutation_batch_group_commits_single_shard_creates() {
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
pub(super) fn execute_cypher_mutation_batch_group_commits_create_property_replacements() {
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
