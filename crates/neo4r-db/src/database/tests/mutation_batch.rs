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
pub(super) fn execute_cypher_mutation_batch_group_commits_merge_node() {
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
pub(super) fn execute_cypher_mutation_batch_group_commits_merge_node_replacement_maps() {
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
pub(super) fn execute_cypher_mutation_batch_group_commits_merge_relationship() {
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
pub(super) fn execute_cypher_mutation_batch_group_commits_merge_relationship_replacement_maps() {
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
pub(super) fn execute_cypher_mutation_batch_group_commits_multiple_local_shards() {
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
pub(super) fn execute_cypher_deletes_node() {
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
pub(super) fn execute_cypher_deletes_node_with_parameterized_matcher() {
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
pub(super) fn execute_cypher_detach_deletes_node_and_relationships() {
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
pub(super) fn execute_cypher_detach_deletes_node_with_parameterized_where() {
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
pub(super) fn execute_cypher_creates_sets_and_deletes_relationships() {
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
pub(super) fn execute_cypher_creates_and_merges_relationships_from_comma_match() {
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
pub(super) fn execute_cypher_relationship_cud_matches_parameterized_pattern_properties() {
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
pub(super) fn execute_cypher_relationship_cud_matches_parameterized_where_predicates() {
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
pub(super) fn query_cursor_with_params_owns_snapshot_view() {
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
pub(super) fn full_scan_query_cursor_reads_snapshot_by_id() {
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
pub(super) fn traversal_query_cursor_reads_snapshot_by_page() {
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
