use neo4r_core::{Properties, Value};
use neo4r_db::{DatabaseConfig, Neo4rDatabase};
use std::fs;
use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let node_count = env_usize("NEO4R_PERF_NODES", 5_000);
    let relationship_count = node_count.saturating_sub(1);
    let update_count = env_usize("NEO4R_PERF_UPDATES", node_count / 10);
    let shard_count = env_u64("NEO4R_PERF_SHARDS", 16);
    let partition_count = env_usize("NEO4R_PERF_PARTITIONS", 4);
    let entries_per_segment = env_u64("NEO4R_PERF_SEGMENT_ENTRIES", 4_096);
    let checkpoint_interval = env_u64("NEO4R_PERF_CHECKPOINT_INTERVAL", 128);
    let wal_sync_interval = env_u64("NEO4R_PERF_WAL_SYNC_INTERVAL", 128);
    let data_dir = temp_dir("neo4r-basic-perf");

    let config = DatabaseConfig::new(&data_dir, shard_count, partition_count)
        .with_log_entries_per_segment(entries_per_segment)
        .with_checkpoint_interval(checkpoint_interval)
        .with_wal_sync_interval(wal_sync_interval);
    let total = Instant::now();
    let mut db = Neo4rDatabase::open(config.clone())?;

    let create_nodes = timed(|| {
        for id in 0..node_count {
            db.create_node(
                vec!["Person".to_string()],
                properties(&[
                    ("name", Value::String(format!("user-{id}"))),
                    ("group", Value::Int((id % 100) as i64)),
                    ("active", Value::Bool(id % 2 == 0)),
                ]),
            )?;
        }
        Ok::<_, Box<dyn std::error::Error>>(())
    })?;

    let create_relationships = timed(|| {
        for from in 0..relationship_count {
            db.create_relationship(
                from as u64,
                (from + 1) as u64,
                "KNOWS".to_string(),
                Properties::new(),
            )?;
        }
        Ok::<_, Box<dyn std::error::Error>>(())
    })?;

    let updates = timed(|| {
        for id in 0..update_count {
            db.set_node_property(id as u64, "score".to_string(), Value::Int(id as i64))?;
        }
        Ok::<_, Box<dyn std::error::Error>>(())
    })?;

    let indexed_query = timed(|| {
        let rows = db.query(r#"MATCH (n:Person) WHERE n.group = 42 RETURN n.name"#)?;
        println!("indexed_query_rows={}", rows.len());
        Ok::<_, Box<dyn std::error::Error>>(())
    })?;

    let traversal_query = timed(|| {
        let rows = db.query(
            r#"MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE a.name = "user-0" RETURN b.name"#,
        )?;
        println!("traversal_query_rows={}", rows.len());
        Ok::<_, Box<dyn std::error::Error>>(())
    })?;

    drop(db);

    let reopen = timed(|| {
        let db = Neo4rDatabase::open(config)?;
        let rows = db.query(r#"MATCH (n:Person) WHERE n.name = "user-0" RETURN n.name"#)?;
        println!("reopen_check_rows={}", rows.len());
        Ok::<_, Box<dyn std::error::Error>>(())
    })?;

    let total_elapsed = total.elapsed();
    print_metric("create_nodes", node_count, create_nodes);
    print_metric(
        "create_relationships",
        relationship_count,
        create_relationships,
    );
    print_metric("set_node_property", update_count, updates);
    print_metric("indexed_query", 1, indexed_query);
    print_metric("traversal_query", 1, traversal_query);
    print_metric("reopen_replay", 1, reopen);
    print_metric(
        "total",
        node_count + relationship_count + update_count,
        total_elapsed,
    );

    let _ = fs::remove_dir_all(data_dir);
    Ok(())
}

fn timed<T>(
    f: impl FnOnce() -> Result<T, Box<dyn std::error::Error>>,
) -> Result<Duration, Box<dyn std::error::Error>> {
    let started = Instant::now();
    f()?;
    Ok(started.elapsed())
}

fn print_metric(name: &str, operations: usize, elapsed: Duration) {
    let seconds = elapsed.as_secs_f64();
    let ops_per_sec = if seconds == 0.0 {
        0.0
    } else {
        operations as f64 / seconds
    };
    println!(
        "{name}: ops={operations} elapsed_ms={:.3} ops_per_sec={:.1}",
        seconds * 1000.0,
        ops_per_sec
    );
}

fn properties(entries: &[(&str, Value)]) -> Properties {
    entries
        .iter()
        .map(|(key, value)| ((*key).to_string(), value.clone()))
        .collect()
}

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn temp_dir(prefix: &str) -> PathBuf {
    let mut path = std::env::temp_dir();
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    path.push(format!("{prefix}-{}-{nanos}", std::process::id()));
    let _ = fs::remove_dir_all(&path);
    path
}
