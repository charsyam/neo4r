use super::*;
use neo4r_core::{ShardPlacement, ShardReplica, Value};
use std::fs;
use std::net::TcpListener;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

#[test]
pub(super) fn tcp_snapshot_fetch_serves_primary_snapshot_for_node_catch_up() {
    let primary_dir = temp_dir("facade-tcp-snapshot-fetch-primary");
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
            .with_routing_table(routing_table),
        Arc::new(crate::NoopShardReplicator),
    )
    .unwrap();
    primary
        .create_node(
            vec!["Person".to_string()],
            properties(&[("name", Value::String("Alice".to_string()))]),
        )
        .unwrap();
    primary.snapshot_now().unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let primary_for_listener = primary.clone();
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        crate::handle_tcp_replication_stream(&primary_for_listener, &mut stream).unwrap();
    });

    let snapshot = crate::request_tcp_snapshot_fetch(&address, Duration::from_secs(1), 0)
        .unwrap()
        .expect("primary should have a snapshot");
    server.join().unwrap();

    assert_eq!(snapshot.metadata.shard_id, 0);
    assert_eq!(snapshot.metadata.last_included_index, 1);
    assert!(!snapshot.payload.is_empty());

    drop(primary);
    let _ = fs::remove_dir_all(primary_dir);
}

#[test]
pub(super) fn tcp_snapshot_fetch_resumes_chunked_snapshot() {
    let primary_dir = temp_dir("facade-tcp-snapshot-fetch-chunked");
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
            .with_routing_table(routing_table),
        Arc::new(crate::NoopShardReplicator),
    )
    .unwrap();
    for i in 0..20 {
        primary
            .create_node(
                vec!["Person".to_string()],
                properties(&[("name", Value::String(format!("Alice-{i}")))]),
            )
            .unwrap();
    }
    primary.snapshot_now().unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let primary_for_listener = primary.clone();
    let server = thread::spawn(move || {
        for _ in 0..3 {
            let (mut stream, _) = listener.accept().unwrap();
            crate::handle_tcp_replication_stream(&primary_for_listener, &mut stream).unwrap();
        }
    });

    let first = crate::request_tcp_snapshot_fetch_chunk(&address, Duration::from_secs(1), 0, 0, 16)
        .unwrap();
    let first_chunk = first.snapshot.clone().expect("snapshot chunk");
    assert_eq!(first_chunk.offset, 0);
    assert_eq!(first.resume_offset, 16);
    assert!(!first_chunk.done);

    let second = crate::request_tcp_snapshot_fetch_chunk(
        &address,
        Duration::from_secs(1),
        0,
        first.resume_offset,
        first.total_len as usize,
    )
    .unwrap();
    assert_eq!(second.checksum, first.checksum);
    assert_eq!(second.total_len, first.total_len);
    assert!(second.snapshot.as_ref().unwrap().done);

    let full = crate::request_tcp_snapshot_fetch(&address, Duration::from_secs(1), 0)
        .unwrap()
        .expect("assembled snapshot");
    server.join().unwrap();

    assert_eq!(full.payload.len() as u64, first.total_len);
    assert_eq!(full.metadata.last_included_index, 20);

    drop(primary);
    let _ = fs::remove_dir_all(primary_dir);
}
