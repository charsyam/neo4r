use super::tcp_responses::*;
use super::*;
use neo4r_core::{Command, ShardPlacement, ShardReplica};
use std::fs;
use std::net::TcpListener;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

fn temp_dir(name: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("neo4r-replication-{name}-{nanos}"))
}

#[test]
fn tcp_replication_channel_reports_tcp_kind_and_default_config() {
    let channel = TcpReplicationChannel;
    let config = ReplicationChannelConfig::default();

    assert_eq!(channel.kind(), ReplicationChannelKind::Tcp);
    assert_eq!(config.connect_timeout, std::time::Duration::from_secs(1));
    assert_eq!(config.max_attempts, 1);
    assert_eq!(config.retry_backoff, std::time::Duration::from_millis(10));
}

#[test]
fn unsupported_replication_channels_are_explicit_placeholders() {
    let udp = UnsupportedReplicationChannel::udp();
    let rdma = UnsupportedReplicationChannel::rdma();

    assert_eq!(udp.kind(), ReplicationChannelKind::Udp);
    assert_eq!(rdma.kind(), ReplicationChannelKind::Rdma);
    let err = udp
        .send_replication_batch("127.0.0.1:1", &ReplicationChannelConfig::default(), &[])
        .unwrap_err();
    assert!(err
        .to_string()
        .contains("Udp replication channel is not implemented"));
}

#[test]
fn replication_ack_positions_codec_round_trips() {
    let positions = vec![(0, 3), (2, 9)];

    let encoded = encode_replication_ack_positions(&positions);
    let decoded = decode_replication_ack_positions(&encoded).unwrap();

    assert_eq!(decoded, positions);
}

#[test]
fn replication_ack_positions_preserve_each_entry_index() {
    let entries = vec![
        LogEntry::new(0, 1, 7, neo4r_core::Command::DeleteNode { id: 7 }),
        LogEntry::new(0, 1, 8, neo4r_core::Command::DeleteNode { id: 8 }),
        LogEntry::new(1, 1, 2, neo4r_core::Command::DeleteNode { id: 2 }),
    ];

    assert_eq!(
        replication_ack_positions(&entries),
        vec![(0, 7), (0, 8), (1, 2)]
    );
}

#[test]
fn replication_ack_positions_accepts_legacy_empty_payload() {
    assert_eq!(decode_replication_ack_positions(&[]).unwrap(), Vec::new());
}

#[test]
fn raft_append_response_codec_round_trips_conflict_hints() {
    let response = TcpRaftAppendResponse {
        append: AppendEntriesResponse {
            term: 7,
            success: false,
            match_index: 3,
            conflict_index: Some(2),
            conflict_term: Some(5),
        },
        ack_positions: vec![(0, 3)],
    };

    let encoded = encode_tcp_raft_append_response(&response);
    let decoded = decode_tcp_raft_append_response(&encoded).unwrap();

    assert_eq!(decoded, response);
}

#[test]
fn tcp_raft_append_response_frame_carries_rejection_hints() {
    let dir = temp_dir("raft-append-response-frame-reject");
    let routing_table = ShardRoutingTable {
        version: 1,
        placements: vec![ShardPlacement::new(0, vec![ShardReplica::primary(1)])],
    };
    let replica = Neo4rDatabaseHandle::open(
        crate::DatabaseConfig::new(&dir, 1, 1)
            .with_server_id(1)
            .with_routing_table(routing_table)
            .with_raft_enabled(true),
    )
    .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let replica_for_listener = replica.clone();
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let _ = handle_tcp_replication_stream(&replica_for_listener, &mut stream);
    });

    let mut stream = TcpStream::connect(address).unwrap();
    let entry = LogEntry::new(0, 3, 8, Command::DeleteNode { id: 42 });
    write_tcp_raft_append_request(&mut stream, 0, 8, &[entry]).unwrap();
    stream.flush().unwrap();
    let response = read_tcp_raft_append_response(&mut stream).unwrap();

    assert!(!response.append.success);
    assert_eq!(response.append.term, 3);
    assert!(response.append.conflict_index.is_some());
    assert!(response.ack_positions.is_empty());

    server.join().unwrap();
    drop(replica);
    let _ = fs::remove_dir_all(dir);
}

#[test]
fn raft_append_response_rejects_truncated_ack_payload() {
    let response = TcpRaftAppendResponse {
        append: AppendEntriesResponse {
            term: 1,
            success: true,
            match_index: 4,
            conflict_index: None,
            conflict_term: None,
        },
        ack_positions: vec![(0, 4)],
    };
    let mut encoded = encode_tcp_raft_append_response(&response);
    encoded.pop();

    let err = decode_tcp_raft_append_response(&encoded).unwrap_err();

    assert!(err
        .to_string()
        .contains("invalid raft append ack payload length"));
}

#[test]
fn replication_ack_positions_rejects_truncated_payloads() {
    let err = decode_replication_ack_positions(&[0, 0, 0]).unwrap_err();

    assert!(err
        .to_string()
        .contains("truncated replication ack payload"));
}

#[test]
fn tcp_catch_up_validation_rejects_wrong_shard_response() {
    let entries = vec![LogEntry::new(
        1,
        1,
        7,
        neo4r_core::Command::DeleteNode { id: 7 },
    )];

    let err = validate_tcp_catch_up_entries(0, 7, None, &entries).unwrap_err();

    assert!(err
        .to_string()
        .contains("catch-up response returned shard 1 for requested shard 0"));
}

#[test]
fn tcp_catch_up_validation_rejects_non_contiguous_indexes() {
    let entries = vec![
        LogEntry::new(0, 1, 7, neo4r_core::Command::DeleteNode { id: 7 }),
        LogEntry::new(0, 1, 9, neo4r_core::Command::DeleteNode { id: 9 }),
    ];

    let err = validate_tcp_catch_up_entries(0, 7, None, &entries).unwrap_err();

    assert!(err
        .to_string()
        .contains("catch-up response returned shard 0 index 9, expected 8"));
}

#[test]
fn tcp_catch_up_validation_rejects_responses_over_requested_limit() {
    let entries = vec![
        LogEntry::new(0, 1, 7, neo4r_core::Command::DeleteNode { id: 7 }),
        LogEntry::new(0, 1, 8, neo4r_core::Command::DeleteNode { id: 8 }),
    ];

    let err = validate_tcp_catch_up_entries(0, 7, Some(1), &entries).unwrap_err();

    assert!(err
        .to_string()
        .contains("catch-up response returned 2 entries, exceeding requested limit 1"));
}

#[test]
fn tcp_catch_up_rejects_malformed_response_before_applying_entries() {
    let dir = temp_dir("malformed-response");
    let routing_table = ShardRoutingTable {
        version: 3,
        placements: vec![ShardPlacement::new(
            0,
            vec![ShardReplica::primary(1), ShardReplica::replica(2)],
        )],
    };
    let replica = Neo4rDatabaseHandle::open(
        crate::DatabaseConfig::new(&dir, 1, 1)
            .with_server_id(2)
            .with_routing_table(routing_table),
    )
    .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let magic = read_magic_bytes(&mut stream).unwrap();
        assert_eq!(magic, TCP_CATCH_UP_REQUEST_MAGIC);
        let request = read_tcp_catch_up_request_after_magic(&mut stream, None).unwrap();
        assert_eq!(request.shard_id, 0);
        assert_eq!(request.start_index, 1);
        write_tcp_catch_up_response(
            &mut stream,
            &Ok(vec![LogEntry::new(
                0,
                1,
                2,
                Command::CreateNode {
                    id: 42,
                    labels: vec!["Person".to_string()],
                    properties: Default::default(),
                },
            )]),
        )
        .unwrap();
    });

    let err =
        catch_up_from_tcp_primary(&replica, &address, Duration::from_secs(1), 0, 1).unwrap_err();

    assert!(err
        .to_string()
        .contains("catch-up response returned shard 0 index 2, expected 1"));
    assert!(replica.log_entries_from(0, 1).unwrap().is_empty());
    assert!(replica
        .query("MATCH (n:Person) RETURN n")
        .unwrap()
        .is_empty());
    assert_eq!(replica.committed_indexes().unwrap(), vec![0]);
    server.join().unwrap();

    drop(replica);
    let _ = fs::remove_dir_all(dir);
}

#[test]
fn tcp_replicator_rejects_peer_response_without_exact_entry_ack() {
    let routing_table = ShardRoutingTable {
        version: 1,
        placements: vec![ShardPlacement::new(
            0,
            vec![ShardReplica::primary(1), ShardReplica::replica(2)],
        )],
    };
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let magic = read_magic_bytes(&mut stream).unwrap();
        assert_eq!(magic, TCP_REPLICATION_REQUEST_MAGIC);
        let entries = read_tcp_replication_request_after_magic(&mut stream).unwrap();
        assert_eq!(entries.len(), 1);
        write_tcp_replication_response(&mut stream, &Ok(Vec::new())).unwrap();
    });

    let replicator = TcpShardReplicator::new(routing_table);
    replicator.register_peer(2, address).unwrap();
    let entry = LogEntry::new_with_metadata(
        0,
        1,
        7,
        1,
        1,
        neo4r_core::HybridTimestamp::zero(),
        neo4r_core::Command::DeleteNode { id: 7 },
    );

    let err = replicator.publish(&entry).unwrap_err();
    assert!(err
        .to_string()
        .contains("tcp peer 2 ack did not include shard 0 index 7"));
    server.join().unwrap();
}
