use neo4r_core::{Properties, ShardPlacement, ShardReplica, ShardRoutingTable, Value};
use neo4r_db::{
    BatchReadQuery, BatchWriteOperation, BatchWriteOutput, DatabaseConfig,
    InProcessShardReplicator, Neo4rDatabase, Neo4rDatabaseHandle,
};
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
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
    let replication_node_count = env_usize("NEO4R_REPLICATION_PERF_NODES", node_count.min(2_000));
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

    let batch_data_dir = temp_dir("neo4r-basic-perf-batch");
    let batch_config = DatabaseConfig::new(&batch_data_dir, shard_count, partition_count)
        .with_log_entries_per_segment(entries_per_segment)
        .with_checkpoint_interval(checkpoint_interval)
        .with_wal_sync_interval(wal_sync_interval);
    run_batch_benchmark(batch_config, node_count, relationship_count, update_count)?;
    run_replicated_batch_benchmark(
        shard_count,
        partition_count,
        entries_per_segment,
        checkpoint_interval,
        wal_sync_interval,
        replication_node_count,
    )?;

    let _ = fs::remove_dir_all(data_dir);
    let _ = fs::remove_dir_all(batch_data_dir);
    Ok(())
}

fn run_replicated_batch_benchmark(
    shard_count: u64,
    partition_count: usize,
    entries_per_segment: u64,
    checkpoint_interval: u64,
    wal_sync_interval: u64,
    node_count: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let relationship_count = node_count.saturating_sub(1);
    let update_count = node_count / 10;
    let primary_dir = temp_dir("neo4r-basic-perf-repl-primary");
    let replica_dir = temp_dir("neo4r-basic-perf-repl-replica");
    let routing_table = routing_table_all_primary_with_replica(shard_count, 1, 2);
    let replicator = Arc::new(InProcessShardReplicator::new(routing_table.clone()));
    let primary = Neo4rDatabaseHandle::open_with_replicator(
        perf_config_for(
            &primary_dir,
            shard_count,
            partition_count,
            entries_per_segment,
            checkpoint_interval,
            wal_sync_interval,
        )
        .with_server_id(1)
        .with_routing_table(routing_table.clone()),
        replicator.clone(),
    )?;
    let replica = Neo4rDatabaseHandle::open(
        perf_config_for(
            &replica_dir,
            shard_count,
            partition_count,
            entries_per_segment,
            checkpoint_interval,
            wal_sync_interval,
        )
        .with_server_id(2)
        .with_routing_table(routing_table),
    )?;
    replicator.register_peer(2, replica.clone())?;

    let total = Instant::now();
    let create_nodes = timed(|| {
        let outputs = primary.execute_batch_write(
            (0..node_count)
                .map(|id| BatchWriteOperation::CreateNode {
                    labels: vec!["Person".to_string()],
                    properties: properties(&[
                        ("name", Value::String(format!("user-{id}"))),
                        ("group", Value::Int((id % 100) as i64)),
                        ("active", Value::Bool(id % 2 == 0)),
                    ]),
                })
                .collect(),
        )?;
        assert_eq!(outputs.len(), node_count);
        Ok::<_, Box<dyn std::error::Error>>(())
    })?;

    let create_relationships = timed(|| {
        let outputs = primary.execute_batch_write(
            (0..relationship_count)
                .map(|from| BatchWriteOperation::CreateRelationship {
                    from: from as u64,
                    to: (from + 1) as u64,
                    rel_type: "KNOWS".to_string(),
                    properties: Properties::new(),
                })
                .collect(),
        )?;
        assert_eq!(outputs.len(), relationship_count);
        Ok::<_, Box<dyn std::error::Error>>(())
    })?;

    let updates = timed(|| {
        let outputs = primary.execute_batch_write(
            (0..update_count)
                .map(|id| BatchWriteOperation::SetNodeProperty {
                    id: id as u64,
                    key: "score".to_string(),
                    value: Value::Int(id as i64),
                })
                .collect(),
        )?;
        assert_eq!(outputs.len(), update_count);
        Ok::<_, Box<dyn std::error::Error>>(())
    })?;

    let replica_visible = timed(|| {
        let results = replica.execute_batch_read(vec![
            BatchReadQuery::new(r#"MATCH (n:Person) WHERE n.group = 42 RETURN n.name"#),
            BatchReadQuery::new(
                r#"MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE a.name = "user-0" RETURN b.name"#,
            ),
            BatchReadQuery::new(r#"MATCH (n:Person) WHERE n.score = 42 RETURN n.name"#),
        ])?;
        println!("replicated_batch_indexed_query_rows={}", results[0].len());
        println!("replicated_batch_traversal_query_rows={}", results[1].len());
        println!("replicated_batch_update_query_rows={}", results[2].len());
        Ok::<_, Box<dyn std::error::Error>>(())
    })?;
    let total_elapsed = total.elapsed();

    print_metric(
        "replicated_batch_create_nodes_e2e",
        node_count,
        create_nodes,
    );
    print_metric(
        "replicated_batch_create_relationships_e2e",
        relationship_count,
        create_relationships,
    );
    print_metric(
        "replicated_batch_set_node_property_e2e",
        update_count,
        updates,
    );
    print_metric("replicated_batch_replica_visible_reads", 3, replica_visible);
    print_metric(
        "replicated_batch_total_e2e",
        node_count + relationship_count + update_count,
        total_elapsed,
    );
    println!(
        "replicated_batch_committed_indexes_primary={:?}",
        primary.committed_indexes()?
    );
    println!(
        "replicated_batch_committed_indexes_replica={:?}",
        replica.committed_indexes()?
    );

    drop(primary);
    drop(replica);
    let _ = fs::remove_dir_all(primary_dir);
    let _ = fs::remove_dir_all(replica_dir);
    Ok(())
}

fn run_batch_benchmark(
    config: DatabaseConfig,
    node_count: usize,
    relationship_count: usize,
    update_count: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let batch_total = Instant::now();
    let mut db = Neo4rDatabase::open(config)?;

    let create_nodes = timed(|| {
        let outputs = db.execute_batch_write(
            (0..node_count)
                .map(|id| BatchWriteOperation::CreateNode {
                    labels: vec!["Person".to_string()],
                    properties: properties(&[
                        ("name", Value::String(format!("user-{id}"))),
                        ("group", Value::Int((id % 100) as i64)),
                        ("active", Value::Bool(id % 2 == 0)),
                    ]),
                })
                .collect(),
        )?;
        assert_eq!(outputs.len(), node_count);
        assert!(outputs
            .iter()
            .all(|output| matches!(output, BatchWriteOutput::NodeId(_))));
        Ok::<_, Box<dyn std::error::Error>>(())
    })?;

    let create_relationships = timed(|| {
        let outputs = db.execute_batch_write(
            (0..relationship_count)
                .map(|from| BatchWriteOperation::CreateRelationship {
                    from: from as u64,
                    to: (from + 1) as u64,
                    rel_type: "KNOWS".to_string(),
                    properties: Properties::new(),
                })
                .collect(),
        )?;
        assert_eq!(outputs.len(), relationship_count);
        assert!(outputs
            .iter()
            .all(|output| matches!(output, BatchWriteOutput::RelationshipId(_))));
        Ok::<_, Box<dyn std::error::Error>>(())
    })?;

    let updates = timed(|| {
        let outputs = db.execute_batch_write(
            (0..update_count)
                .map(|id| BatchWriteOperation::SetNodeProperty {
                    id: id as u64,
                    key: "score".to_string(),
                    value: Value::Int(id as i64),
                })
                .collect(),
        )?;
        assert_eq!(outputs.len(), update_count);
        assert!(outputs
            .iter()
            .all(|output| matches!(output, BatchWriteOutput::Unit)));
        Ok::<_, Box<dyn std::error::Error>>(())
    })?;

    let reads = timed(|| {
        let results = db.execute_batch_read(vec![
            BatchReadQuery::new(r#"MATCH (n:Person) WHERE n.group = 42 RETURN n.name"#),
            BatchReadQuery::new(
                r#"MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE a.name = "user-0" RETURN b.name"#,
            ),
            BatchReadQuery::new(r#"MATCH (n:Person) WHERE n.score = 42 RETURN n.name"#),
        ])?;
        println!("batch_indexed_query_rows={}", results[0].len());
        println!("batch_traversal_query_rows={}", results[1].len());
        println!("batch_update_query_rows={}", results[2].len());
        Ok::<_, Box<dyn std::error::Error>>(())
    })?;

    let total_elapsed = batch_total.elapsed();
    print_metric("batch_create_nodes", node_count, create_nodes);
    print_metric(
        "batch_create_relationships",
        relationship_count,
        create_relationships,
    );
    print_metric("batch_set_node_property", update_count, updates);
    print_metric("batch_read_queries", 3, reads);
    print_metric(
        "batch_total",
        node_count + relationship_count + update_count,
        total_elapsed,
    );
    Ok(())
}

fn perf_config_for(
    dir: &PathBuf,
    shard_count: u64,
    partition_count: usize,
    entries_per_segment: u64,
    checkpoint_interval: u64,
    wal_sync_interval: u64,
) -> DatabaseConfig {
    DatabaseConfig::new(dir, shard_count, partition_count)
        .with_log_entries_per_segment(entries_per_segment)
        .with_checkpoint_interval(checkpoint_interval)
        .with_wal_sync_interval(wal_sync_interval)
}

fn routing_table_all_primary_with_replica(
    shard_count: u64,
    primary_server_id: u64,
    replica_server_id: u64,
) -> ShardRoutingTable {
    ShardRoutingTable {
        version: 1,
        placements: (0..shard_count)
            .map(|shard_id| {
                ShardPlacement::new(
                    shard_id,
                    vec![
                        ShardReplica::primary(primary_server_id),
                        ShardReplica::replica(replica_server_id),
                    ],
                )
            })
            .collect(),
    }
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
