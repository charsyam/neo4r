use neo4r_core::{Properties, Value};
use neo4r_db::{DatabaseConfig, Neo4rDatabase};
use neo4r_query::QueryValue;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

const NODE_CASES: usize = 360;
const REL_CASES: usize = 180;

#[test]
fn data_correctness_bulk_node_lifecycle_matches_expected_state() {
    let dir = temp_dir("data-correctness-node-lifecycle");
    let mut db = open_correctness_db(&dir);
    let mut expected = BTreeMap::new();
    let mut checks = 0usize;

    for id in 0..NODE_CASES {
        let node_id = db
            .create_node(
                vec!["Person".to_string()],
                properties(&[
                    ("name", Value::String(format!("user-{id:03}"))),
                    ("bucket", Value::Int((id % 12) as i64)),
                    ("score", Value::Int((id * 10) as i64)),
                    ("active", Value::Bool(id % 2 == 0)),
                ]),
            )
            .unwrap();
        assert_eq!(node_id as usize, id, "node id should be allocated in order");
        checks += 1;
        expected.insert(
            node_id,
            ExpectedNode {
                name: format!("user-{id:03}"),
                bucket: (id % 12) as i64,
                score: (id * 10) as i64,
                active: Some(id % 2 == 0),
                status: None,
            },
        );
    }

    for (node_id, expected_node) in &expected {
        let node = db.node(*node_id).unwrap().expect("node should exist");
        assert_node_matches(*node_id, &node, expected_node);
        checks += 5;
    }

    for bucket in 0..12 {
        let rows = db
            .query(&format!(
                "MATCH (n:Person) WHERE n.bucket = {bucket} RETURN n.name"
            ))
            .unwrap();
        assert_eq!(rows.len(), NODE_CASES / 12, "bucket {bucket} row count");
        checks += 1;
    }

    for id in (0..NODE_CASES).step_by(3) {
        db.set_node_property(
            id as u64,
            "status".to_string(),
            Value::String("updated".to_string()),
        )
        .unwrap();
        expected.get_mut(&(id as u64)).unwrap().status = Some("updated".to_string());
        checks += 1;
    }
    for id in (0..NODE_CASES).step_by(5) {
        db.remove_node_property(id as u64, "active".to_string())
            .unwrap();
        expected.get_mut(&(id as u64)).unwrap().active = None;
        checks += 1;
    }

    for (node_id, expected_node) in &expected {
        let node = db
            .node(*node_id)
            .unwrap()
            .expect("node should exist after update");
        assert_node_matches(*node_id, &node, expected_node);
        checks += 5;
    }

    let updated_rows = db
        .query(r#"MATCH (n:Person) WHERE n.status = "updated" RETURN n.name"#)
        .unwrap();
    assert_eq!(updated_rows.len(), NODE_CASES.div_ceil(3));
    checks += 1;

    let active_rows = db
        .query("MATCH (n:Person) WHERE n.active = true RETURN n.name")
        .unwrap();
    let expected_active = (0..NODE_CASES)
        .filter(|id| id % 2 == 0 && id % 5 != 0)
        .count();
    assert_eq!(active_rows.len(), expected_active);
    checks += 1;

    let deleted_ids = (0..NODE_CASES)
        .step_by(7)
        .map(|id| id as u64)
        .collect::<Vec<_>>();
    for node_id in &deleted_ids {
        db.delete_node(*node_id).unwrap();
        expected.remove(node_id);
        checks += 1;
    }

    let rows = db.query("MATCH (n:Person) RETURN n").unwrap();
    assert_eq!(rows.len(), expected.len());
    checks += 1;

    for node_id in deleted_ids {
        assert!(
            db.node(node_id).unwrap().is_none(),
            "deleted node {node_id}"
        );
        checks += 1;
    }

    assert!(
        checks >= 1_000,
        "correctness suite should execute at least 1000 checks, got {checks}"
    );
    drop(db);
    let _ = fs::remove_dir_all(dir);
}

#[test]
fn data_correctness_cypher_mutations_return_expected_rows() {
    let dir = temp_dir("data-correctness-cypher-mut");
    let mut db = open_correctness_db(&dir);
    let mut checks = 0usize;

    for id in 0..120 {
        let rows = db
            .execute_cypher(&format!(
                r#"CREATE (n:Account {{name: "acct-{id:03}", balance: {}}}) RETURN n.name, n.balance"#,
                id as i64 * 100
            ))
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_scalar_string(&rows[0], "n.name", &format!("acct-{id:03}"));
        assert_scalar_int(&rows[0], "n.balance", id as i64 * 100);
        checks += 3;
    }

    for id in 0..120 {
        let rows = db
            .execute_cypher(&format!(
                r#"MATCH (n:Account) WHERE n.name = "acct-{id:03}" SET n.balance = {}, n.tier = "gold" RETURN n.balance, n.tier"#,
                id as i64 * 100 + 7
            ))
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_scalar_int(&rows[0], "n.balance", id as i64 * 100 + 7);
        assert_scalar_string(&rows[0], "n.tier", "gold");
        checks += 3;
    }

    for id in (0..120).step_by(4) {
        db.execute_cypher(&format!(
            r#"MATCH (n:Account) WHERE n.name = "acct-{id:03}" REMOVE n.tier RETURN n.tier"#
        ))
        .unwrap();
        let rows = db
            .query(&format!(
                r#"MATCH (n:Account) WHERE n.name = "acct-{id:03}" AND n.tier = "gold" RETURN n"#
            ))
            .unwrap();
        assert!(rows.is_empty());
        checks += 1;
    }

    for id in (1..120).step_by(4) {
        let rows = db
            .query(&format!(
                r#"MATCH (n:Account) WHERE n.name = "acct-{id:03}" AND n.tier = "gold" RETURN n.tier"#
            ))
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_scalar_string(&rows[0], "n.tier", "gold");
        checks += 2;
    }

    let rows = db
        .query(r#"MATCH (n:Account) WHERE n.tier = "gold" RETURN n.name"#)
        .unwrap();
    assert_eq!(rows.len(), 90);
    checks += 1;

    for id in (0..120).step_by(6) {
        let rows = db
            .execute_cypher(&format!(
                r#"MATCH (n:Account) WHERE n.name = "acct-{id:03}" DELETE n RETURN n.name"#
            ))
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_scalar_string(&rows[0], "n.name", &format!("acct-{id:03}"));
        checks += 2;
    }

    let rows = db.query("MATCH (n:Account) RETURN n").unwrap();
    assert_eq!(rows.len(), 100);
    checks += 1;

    assert!(checks >= 750, "expected at least 750 checks, got {checks}");
    drop(db);
    let _ = fs::remove_dir_all(dir);
}

#[test]
fn data_correctness_relationship_lifecycle_and_detach_delete() {
    let dir = temp_dir("data-correctness-rel-lifecycle");
    let mut db = open_correctness_db(&dir);
    let mut node_ids = Vec::new();
    let mut rel_ids = Vec::new();
    let mut checks = 0usize;

    for id in 0..=REL_CASES {
        let node_id = db
            .create_node(
                vec!["Person".to_string()],
                properties(&[("name", Value::String(format!("person-{id:03}")))]),
            )
            .unwrap();
        node_ids.push(node_id);
        checks += 1;
    }

    for id in 0..REL_CASES {
        let rel_id = db
            .create_relationship(
                node_ids[id],
                node_ids[id + 1],
                "KNOWS".to_string(),
                properties(&[
                    ("ordinal", Value::Int(id as i64)),
                    ("active", Value::Bool(id % 2 == 0)),
                ]),
            )
            .unwrap();
        rel_ids.push(rel_id);
        checks += 1;
    }

    for id in 0..REL_CASES {
        let rows = db
            .query(&format!(
                r#"MATCH (a:Person)-[r:KNOWS]->(b:Person) WHERE r.ordinal = {id} RETURN a.name, b.name, r.active"#
            ))
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_scalar_string(&rows[0], "a.name", &format!("person-{id:03}"));
        assert_scalar_string(&rows[0], "b.name", &format!("person-{:03}", id + 1));
        assert_scalar_bool(&rows[0], "r.active", id % 2 == 0);
        checks += 4;
    }

    for (id, rel_id) in rel_ids.iter().copied().enumerate().step_by(3) {
        db.set_relationship_property(rel_id, "status".to_string(), Value::String("seen".into()))
            .unwrap();
        let rows = db
            .query(&format!(
                r#"MATCH (a:Person)-[r:KNOWS]->(b:Person) WHERE r.ordinal = {id} RETURN r.status"#
            ))
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_scalar_string(&rows[0], "r.status", "seen");
        checks += 3;
    }

    for (id, rel_id) in rel_ids.iter().copied().enumerate().step_by(5) {
        db.remove_relationship_property(rel_id, "active".to_string())
            .unwrap();
        let rows = db
            .query(&format!(
                r#"MATCH (a:Person)-[r:KNOWS]->(b:Person) WHERE r.ordinal = {id} AND r.active = true RETURN r"#
            ))
            .unwrap();
        assert!(rows.is_empty());
        checks += 2;
    }

    for id in (2..REL_CASES).step_by(5) {
        let rows = db
            .query(&format!(
                r#"MATCH (a:Person)-[r:KNOWS]->(b:Person) WHERE r.ordinal = {id} AND r.active = true RETURN r.active"#
            ))
            .unwrap();
        if id % 2 == 0 {
            assert_eq!(rows.len(), 1);
            assert_scalar_bool(&rows[0], "r.active", true);
            checks += 2;
        } else {
            assert!(rows.is_empty());
            checks += 1;
        }
    }

    for rel_id in rel_ids.iter().copied().step_by(11) {
        db.delete_relationship(rel_id).unwrap();
        checks += 1;
    }
    let deleted_rel_count = rel_ids.iter().step_by(11).count();
    assert_eq!(
        db.query("MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN r")
            .unwrap()
            .len(),
        REL_CASES - deleted_rel_count
    );
    checks += 1;

    let detach_rows = db
        .execute_cypher(
            r#"MATCH (n:Person) WHERE n.name = "person-090" DETACH DELETE n RETURN n.name"#,
        )
        .unwrap();
    assert_eq!(detach_rows.len(), 1);
    assert_scalar_string(&detach_rows[0], "n.name", "person-090");
    checks += 2;

    assert!(db
        .query(r#"MATCH (n:Person) WHERE n.name = "person-090" RETURN n"#)
        .unwrap()
        .is_empty());
    assert!(db
        .query(r#"MATCH (a:Person)-[r:KNOWS]->(b:Person) WHERE a.name = "person-090" RETURN r"#)
        .unwrap()
        .is_empty());
    assert!(db
        .query(r#"MATCH (a:Person)-[r:KNOWS]->(b:Person) WHERE b.name = "person-090" RETURN r"#)
        .unwrap()
        .is_empty());
    checks += 3;

    assert!(
        checks >= 1_000,
        "expected at least 1000 checks, got {checks}"
    );
    drop(db);
    let _ = fs::remove_dir_all(dir);
}

#[test]
fn data_correctness_reopen_preserves_mutated_state() {
    let dir = temp_dir("data-correctness-reopen");
    {
        let mut db = open_correctness_db(&dir);
        for id in 0..96 {
            db.execute_cypher(&format!(
                r#"CREATE (n:Session {{name: "session-{id:03}", slot: {}, stale: true}})"#,
                id % 8
            ))
            .unwrap();
        }
        for id in (0..96).step_by(2) {
            db.execute_cypher(&format!(
                r#"MATCH (n:Session) WHERE n.name = "session-{id:03}" SET n.status = "even""#
            ))
            .unwrap();
        }
        for id in (0..96).step_by(3) {
            db.execute_cypher(&format!(
                r#"MATCH (n:Session) WHERE n.name = "session-{id:03}" REMOVE n.stale"#
            ))
            .unwrap();
        }
        for id in (0..96).step_by(10) {
            db.execute_cypher(&format!(
                r#"MATCH (n:Session) WHERE n.name = "session-{id:03}" DELETE n"#
            ))
            .unwrap();
        }
    }

    let db = open_correctness_db(&dir);
    let deleted = (0..96).step_by(10).collect::<BTreeSet<_>>();
    let rows = db.query("MATCH (n:Session) RETURN n").unwrap();
    assert_eq!(rows.len(), 96 - deleted.len());

    let even_rows = db
        .query(r#"MATCH (n:Session) WHERE n.status = "even" RETURN n.name"#)
        .unwrap();
    let expected_even = (0..96)
        .filter(|id| id % 2 == 0 && !deleted.contains(id))
        .count();
    assert_eq!(even_rows.len(), expected_even);

    let stale_rows = db
        .query("MATCH (n:Session) WHERE n.stale = true RETURN n.name")
        .unwrap();
    let expected_stale = (0..96)
        .filter(|id| id % 3 != 0 && !deleted.contains(id))
        .count();
    assert_eq!(stale_rows.len(), expected_stale);

    for slot in 0..8 {
        let rows = db
            .query(&format!(
                "MATCH (n:Session) WHERE n.slot = {slot} RETURN n.name"
            ))
            .unwrap();
        let expected_slot = (0..96)
            .filter(|id| id % 8 == slot && !deleted.contains(id))
            .count();
        assert_eq!(rows.len(), expected_slot, "slot {slot}");
    }

    drop(db);
    let _ = fs::remove_dir_all(dir);
}

#[derive(Debug)]
struct ExpectedNode {
    name: String,
    bucket: i64,
    score: i64,
    active: Option<bool>,
    status: Option<String>,
}

fn assert_node_matches(node_id: u64, node: &neo4r_core::Node, expected: &ExpectedNode) {
    assert_eq!(node.id, node_id);
    assert!(node.labels.iter().any(|label| label == "Person"));
    assert_eq!(
        node.properties.get("name"),
        Some(&Value::String(expected.name.clone()))
    );
    assert_eq!(
        node.properties.get("bucket"),
        Some(&Value::Int(expected.bucket))
    );
    assert_eq!(
        node.properties.get("score"),
        Some(&Value::Int(expected.score))
    );
    match expected.active {
        Some(active) => assert_eq!(node.properties.get("active"), Some(&Value::Bool(active))),
        None => assert!(!node.properties.contains_key("active")),
    }
    match &expected.status {
        Some(status) => assert_eq!(
            node.properties.get("status"),
            Some(&Value::String(status.clone()))
        ),
        None => assert!(!node.properties.contains_key("status")),
    }
}

fn assert_scalar_string(row: &neo4r_query::QueryRow, key: &str, expected: &str) {
    assert_eq!(
        row.get(key),
        Some(&QueryValue::Scalar(Value::String(expected.to_string()))),
        "{key}"
    );
}

fn assert_scalar_int(row: &neo4r_query::QueryRow, key: &str, expected: i64) {
    assert_eq!(
        row.get(key),
        Some(&QueryValue::Scalar(Value::Int(expected))),
        "{key}"
    );
}

fn assert_scalar_bool(row: &neo4r_query::QueryRow, key: &str, expected: bool) {
    assert_eq!(
        row.get(key),
        Some(&QueryValue::Scalar(Value::Bool(expected))),
        "{key}"
    );
}

fn open_correctness_db(dir: &PathBuf) -> Neo4rDatabase {
    Neo4rDatabase::open(DatabaseConfig::new(dir, 8, 4)).unwrap()
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
