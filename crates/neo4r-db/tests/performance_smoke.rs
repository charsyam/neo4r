use neo4r_core::{Properties, Value};
use neo4r_db::{DatabaseConfig, Neo4rDatabase};
use neo4r_query::QueryValue;
use std::fs;
use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const NODE_COUNT: usize = 320;
const VECTOR_COUNT: usize = 128;

#[test]
fn perf_smoke_writes_nodes_relationships_and_indexed_queries() {
    let dir = temp_dir("perf-smoke-write-query");
    let started = Instant::now();
    let mut db = open_perf_db(&dir);

    for id in 0..NODE_COUNT {
        db.create_node(
            vec!["Person".to_string()],
            properties(&[
                ("name", Value::String(format!("user-{id}"))),
                ("group", Value::Int((id % 16) as i64)),
                ("active", Value::Bool(id % 2 == 0)),
            ]),
        )
        .unwrap();
    }
    for from in 0..NODE_COUNT - 1 {
        db.create_relationship(
            from as u64,
            (from + 1) as u64,
            "KNOWS".to_string(),
            Properties::new(),
        )
        .unwrap();
    }

    let rows = db
        .query(r#"MATCH (n:Person) WHERE n.group = 7 RETURN n.name"#)
        .unwrap();
    assert_eq!(rows.len(), NODE_COUNT / 16);

    let traversal = db
        .query(r#"MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE a.name = "user-0" RETURN b.name"#)
        .unwrap();
    assert_eq!(traversal.len(), 1);
    assert_eq!(
        traversal[0].get("b.name"),
        Some(&QueryValue::Scalar(Value::String("user-1".to_string())))
    );

    assert_elapsed_under("write/index/traversal perf smoke", started.elapsed());
    drop(db);
    let _ = fs::remove_dir_all(dir);
}

#[test]
fn perf_smoke_cursor_paginates_traversal_without_materializing_client_side() {
    let dir = temp_dir("perf-smoke-cursor");
    let started = Instant::now();
    let mut db = open_perf_db(&dir);
    let root = db
        .create_node(
            vec!["Person".to_string()],
            properties(&[("name", Value::String("root".to_string()))]),
        )
        .unwrap();
    for id in 0..128 {
        let child = db
            .create_node(
                vec!["Person".to_string()],
                properties(&[("name", Value::String(format!("child-{id}")))]),
            )
            .unwrap();
        db.create_relationship(root, child, "KNOWS".to_string(), Properties::new())
            .unwrap();
    }

    let mut cursor = db
        .query_cursor(
            r#"MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE a.name = "root" RETURN b.name"#,
        )
        .unwrap();
    let mut fetched = 0;
    loop {
        let page = cursor.fetch(17);
        fetched += page.rows.len();
        if !page.has_more {
            break;
        }
    }
    assert_eq!(fetched, 128);

    assert_elapsed_under("cursor traversal perf smoke", started.elapsed());
    drop(db);
    let _ = fs::remove_dir_all(dir);
}

#[test]
fn perf_smoke_vector_knn_uses_hnsw_query_path() {
    let dir = temp_dir("perf-smoke-vector");
    let started = Instant::now();
    let mut db = open_perf_db(&dir);
    for id in 0..VECTOR_COUNT {
        let x = 1.0 - (id as f32 * 0.001);
        let y = id as f32 * 0.001;
        db.create_node(
            vec!["Document".to_string()],
            properties(&[
                ("title", Value::String(format!("doc-{id}"))),
                ("embedding", Value::Vector(vec![x, y, 0.0, 0.0])),
            ]),
        )
        .unwrap();
    }

    let rows = db
        .query("MATCH (n:Document) WHERE vector.knn(n.embedding, [1.0, 0.0, 0.0, 0.0], 8) RETURN n.title")
        .unwrap();

    assert_eq!(rows.len(), 8);
    assert_eq!(
        rows[0].get("n.title"),
        Some(&QueryValue::Scalar(Value::String("doc-0".to_string())))
    );

    assert_elapsed_under("vector knn perf smoke", started.elapsed());
    drop(db);
    let _ = fs::remove_dir_all(dir);
}

#[test]
fn perf_smoke_reopen_replay_keeps_indexed_and_vector_queries_fast_enough() {
    let dir = temp_dir("perf-smoke-reopen");
    let config = perf_config(&dir);
    let started = Instant::now();
    {
        let mut db = Neo4rDatabase::open(config.clone()).unwrap();
        for id in 0..160 {
            db.create_node(
                vec!["Document".to_string()],
                properties(&[
                    ("title", Value::String(format!("doc-{id}"))),
                    ("bucket", Value::Int((id % 8) as i64)),
                    (
                        "embedding",
                        Value::Vector(vec![1.0 - id as f32 * 0.001, 0.0]),
                    ),
                ]),
            )
            .unwrap();
        }
    }

    let db = Neo4rDatabase::open(config).unwrap();
    let indexed = db
        .query(r#"MATCH (n:Document) WHERE n.bucket = 3 RETURN n.title"#)
        .unwrap();
    assert_eq!(indexed.len(), 20);

    let vector = db
        .query("MATCH (n:Document) WHERE vector.knn(n.embedding, [1.0, 0.0], 4) RETURN n.title")
        .unwrap();
    assert_eq!(vector.len(), 4);

    assert_elapsed_under("reopen replay perf smoke", started.elapsed());
    drop(db);
    let _ = fs::remove_dir_all(dir);
}

fn open_perf_db(dir: &PathBuf) -> Neo4rDatabase {
    Neo4rDatabase::open(perf_config(dir)).unwrap()
}

fn perf_config(dir: &PathBuf) -> DatabaseConfig {
    DatabaseConfig::new(dir, 4, 2)
        .with_log_entries_per_segment(128)
        .with_checkpoint_interval(128)
        .with_wal_sync_interval(128)
        .with_group_commit_max_entries(32)
        .with_group_commit_max_delay(Duration::from_millis(1))
}

fn assert_elapsed_under(name: &str, elapsed: Duration) {
    let limit = std::env::var("NEO4R_PERF_SMOKE_MAX_MS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .map(Duration::from_millis)
        .unwrap_or(Duration::from_secs(30));
    assert!(
        elapsed <= limit,
        "{name} took {:?}, over {:?}",
        elapsed,
        limit
    );
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
