use neo4r_core::{
    Command, HybridTimestamp, LogEntry, Properties, ShardPlacement, ShardReplica, Value,
};
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

#[test]
fn perf_smoke_mutation_mix_preserves_index_latency_budget() {
    let dir = temp_dir("perf-smoke-mutation-mix");
    let started = Instant::now();
    let mut db = open_perf_db(&dir);
    let mut node_ids = Vec::with_capacity(240);
    let mut relationship_ids = Vec::with_capacity(239);

    for id in 0..240 {
        node_ids.push(
            db.create_node(
                vec!["Account".to_string()],
                properties(&[
                    ("name", Value::String(format!("account-{id}"))),
                    ("tier", Value::Int((id % 12) as i64)),
                    ("active", Value::Bool(true)),
                ]),
            )
            .unwrap(),
        );
    }
    for window in node_ids.windows(2) {
        relationship_ids.push(
            db.create_relationship(window[0], window[1], "LINKS".to_string(), Properties::new())
                .unwrap(),
        );
    }

    for (offset, node_id) in node_ids.iter().take(120).enumerate() {
        db.set_node_property(
            *node_id,
            "tier".to_string(),
            Value::Int((offset % 6) as i64),
        )
        .unwrap();
    }
    for node_id in node_ids.iter().skip(120).take(40) {
        db.remove_node_property(*node_id, "active".to_string())
            .unwrap();
    }
    for relationship_id in relationship_ids.iter().step_by(11).copied() {
        db.delete_relationship(relationship_id).unwrap();
    }

    let mut samples = Vec::new();
    for tier in 0..12 {
        let query = format!(r#"MATCH (n:Account) WHERE n.tier = {tier} RETURN n.name"#);
        samples.push(timed_duration(|| {
            let _ = db.query(&query).unwrap();
        }));
    }

    let stats = LatencyStats::from_samples(&samples);
    eprintln!(
        "mutation_indexed_query_latency samples={} p50_ms={:.3} p99_ms={:.3}",
        samples.len(),
        stats.p50.as_secs_f64() * 1000.0,
        stats.p99.as_secs_f64() * 1000.0
    );
    assert_latency_under(
        "mutation indexed query p99",
        stats.p99,
        env_ms("NEO4R_PERF_QUERY_P99_MS", 250),
    );
    assert_elapsed_under("mutation mix perf smoke", started.elapsed());
    drop(db);
    let _ = fs::remove_dir_all(dir);
}

#[test]
fn perf_smoke_repeated_indexed_queries_report_p50_and_p99() {
    let dir = temp_dir("perf-smoke-query-latency");
    let mut db = open_perf_db(&dir);

    for id in 0..384 {
        db.create_node(
            vec!["Event".to_string()],
            properties(&[
                ("name", Value::String(format!("event-{id}"))),
                ("bucket", Value::Int((id % 32) as i64)),
            ]),
        )
        .unwrap();
    }

    let mut samples = Vec::with_capacity(96);
    for iteration in 0..96 {
        let bucket = iteration % 32;
        let query = format!(r#"MATCH (n:Event) WHERE n.bucket = {bucket} RETURN n.name"#);
        samples.push(timed_duration(|| {
            let rows = db.query(&query).unwrap();
            assert_eq!(rows.len(), 12);
        }));
    }

    let stats = LatencyStats::from_samples(&samples);
    eprintln!(
        "indexed_query_latency samples={} p50_ms={:.3} p99_ms={:.3}",
        samples.len(),
        stats.p50.as_secs_f64() * 1000.0,
        stats.p99.as_secs_f64() * 1000.0
    );
    assert_latency_under(
        "indexed query p50",
        stats.p50,
        env_ms("NEO4R_PERF_QUERY_P50_MS", 25),
    );
    assert_latency_under(
        "indexed query p99",
        stats.p99,
        env_ms("NEO4R_PERF_QUERY_P99_MS", 250),
    );
    drop(db);
    let _ = fs::remove_dir_all(dir);
}

#[test]
fn perf_smoke_raft_append_batch_reports_replication_latency() {
    let dir = temp_dir("perf-smoke-raft-append-batch");
    let routing_table = neo4r_core::ShardRoutingTable {
        version: 1,
        placements: vec![ShardPlacement::new(
            0,
            vec![ShardReplica::primary(1), ShardReplica::replica(2)],
        )],
    };
    let mut db = Neo4rDatabase::open(
        DatabaseConfig::new(&dir, 1, 1)
            .with_server_id(2)
            .with_routing_table(routing_table)
            .with_raft_enabled(true)
            .with_log_entries_per_segment(128)
            .with_checkpoint_interval(128)
            .with_wal_sync_interval(128),
    )
    .unwrap();
    let entries: Vec<_> = (1..=160)
        .map(|index| {
            LogEntry::new_with_metadata(
                0,
                7,
                index,
                1,
                1,
                HybridTimestamp::new(1_700_000_000, index as u32),
                Command::CreateNode {
                    id: index,
                    labels: vec!["ReplicaPerf".to_string()],
                    properties: properties(&[("group", Value::Int((index % 8) as i64))]),
                },
            )
        })
        .collect();

    let elapsed = timed_duration(|| {
        let response = db
            .apply_raft_append_entries_with_response(0, entries, 160)
            .unwrap();
        assert!(response.success);
        assert!(response.durable);
        assert_eq!(response.match_index, 160);
    });
    let rows = db
        .query(r#"MATCH (n:ReplicaPerf) WHERE n.group = 3 RETURN n.group"#)
        .unwrap();
    assert_eq!(rows.len(), 20);
    eprintln!(
        "raft_append_batch_latency entries=160 elapsed_ms={:.3}",
        elapsed.as_secs_f64() * 1000.0
    );
    assert_latency_under(
        "raft append batch p99",
        elapsed,
        env_ms("NEO4R_PERF_REPLICATION_APPEND_P99_MS", 1000),
    );
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

fn assert_latency_under(name: &str, observed: Duration, limit: Duration) {
    assert!(
        observed <= limit,
        "{name} took {:?}, over {:?}",
        observed,
        limit
    );
}

fn timed_duration(f: impl FnOnce()) -> Duration {
    let started = Instant::now();
    f();
    started.elapsed()
}

#[derive(Debug)]
struct LatencyStats {
    p50: Duration,
    p99: Duration,
}

impl LatencyStats {
    fn from_samples(samples: &[Duration]) -> Self {
        assert!(!samples.is_empty());
        let mut sorted = samples.to_vec();
        sorted.sort_unstable();
        Self {
            p50: percentile(&sorted, 50),
            p99: percentile(&sorted, 99),
        }
    }
}

fn percentile(sorted: &[Duration], percentile: usize) -> Duration {
    let rank = (sorted.len() * percentile).div_ceil(100);
    sorted[rank.saturating_sub(1).min(sorted.len() - 1)]
}

fn env_ms(key: &str, default: u64) -> Duration {
    std::env::var(key)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .map(Duration::from_millis)
        .unwrap_or(Duration::from_millis(default))
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
