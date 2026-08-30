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
    assert_eq!(
        config.retransmit_timeout,
        std::time::Duration::from_millis(50)
    );
    assert_eq!(config.max_in_flight_batches, 1024);
}

#[test]
fn replication_channel_metrics_track_in_flight_and_backpressure() {
    let metrics = ReplicationChannelMetrics::default();

    metrics.record_send(2, 128);
    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.in_flight_batches, 1);
    assert_eq!(snapshot.max_in_flight_batches, 1);

    metrics.record_backpressure_rejection();
    metrics.record_failure();
    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.in_flight_batches, 0);
    assert_eq!(snapshot.backpressure_rejections, 1);
}

#[test]
fn unsupported_replication_channels_are_explicit_placeholders() {
    let udp = UnsupportedReplicationChannel::udp();
    let rdma = UnsupportedReplicationChannel::rdma();
    let endpoint = ReplicationEndpoint::udp("127.0.0.1:1", 1200);

    assert_eq!(udp.kind(), ReplicationChannelKind::Udp);
    assert_eq!(rdma.kind(), ReplicationChannelKind::Rdma);
    let err = udp
        .send_replication_batch(&endpoint, &ReplicationChannelConfig::default(), &[])
        .unwrap_err();
    assert!(err
        .to_string()
        .contains("Udp replication channel is not implemented"));
}

#[test]
fn replication_endpoint_records_transport_and_capabilities() {
    let tcp = ReplicationEndpoint::tcp("127.0.0.1:17687");
    let udp = ReplicationEndpoint::udp("127.0.0.1:17688", 1200);

    assert_eq!(tcp.kind, ReplicationChannelKind::Tcp);
    assert!(tcp.capabilities.raft_append);
    assert!(tcp.capabilities.snapshot);
    assert_eq!(udp.kind, ReplicationChannelKind::Udp);
    assert!(!udp.capabilities.raft_append);
    assert_eq!(udp.capabilities.max_frame_bytes, Some(1200));
}

#[test]
fn replication_channel_negotiation_uses_preference_order() {
    let offer = ReplicationChannelOffer {
        server_id: 2,
        endpoints: vec![
            ReplicationEndpoint::tcp("127.0.0.1:17687"),
            ReplicationEndpoint::udp("127.0.0.1:17688", 1200),
        ],
    };

    let agreement = negotiate_replication_channel(
        &[ReplicationChannelKind::Udp, ReplicationChannelKind::Tcp],
        offer,
    )
    .unwrap();

    assert_eq!(agreement.server_id, 2);
    assert_eq!(agreement.endpoint.kind, ReplicationChannelKind::Udp);
}

#[test]
fn replication_channel_negotiation_filters_by_required_capabilities() {
    let offer = ReplicationChannelOffer {
        server_id: 2,
        endpoints: vec![
            ReplicationEndpoint::udp("127.0.0.1:17688", 1200),
            ReplicationEndpoint::tcp("127.0.0.1:17687"),
        ],
    };

    let agreement = negotiate_replication_channel_with_capabilities(
        &[ReplicationChannelKind::Udp, ReplicationChannelKind::Tcp],
        offer,
        &ReplicationChannelCapabilities {
            raft_append: true,
            vote: true,
            snapshot: true,
            catch_up: true,
            max_frame_bytes: None,
            fault_profile: ReplicationTransportFaultProfile::reliable_stream(),
        },
    )
    .unwrap();

    assert_eq!(agreement.endpoint.kind, ReplicationChannelKind::Tcp);
}

#[test]
fn replication_channel_negotiation_rejects_missing_required_capabilities() {
    let offer = ReplicationChannelOffer {
        server_id: 2,
        endpoints: vec![ReplicationEndpoint::udp("127.0.0.1:17688", 1200)],
    };

    let err = negotiate_replication_channel_with_capabilities(
        &[ReplicationChannelKind::Udp],
        offer,
        &ReplicationChannelCapabilities {
            raft_append: true,
            vote: true,
            snapshot: false,
            catch_up: false,
            max_frame_bytes: None,
            fault_profile: ReplicationTransportFaultProfile::reliable_stream(),
        },
    )
    .unwrap_err();

    assert!(err.to_string().contains("required capabilities"));
}

#[test]
fn udp_replication_channel_has_explicit_reliability_boundary() {
    let channel = UdpReplicationChannel::prototype(1200);
    let endpoint = ReplicationEndpoint::udp("127.0.0.1:17688", 1200);

    let acked = channel
        .send_raft_append_batch(&endpoint, &ReplicationChannelConfig::default(), 0, 0, &[])
        .unwrap();
    assert!(acked.is_empty());

    let err = channel
        .request_vote(
            &endpoint,
            &ReplicationChannelConfig::default(),
            0,
            RequestVoteRequest {
                term: 1,
                candidate_id: 1,
                last_log_index: 0,
                last_log_term: 0,
            },
        )
        .unwrap_err();
    assert!(err
        .to_string()
        .contains("udp raft vote response path is not implemented"));
}

#[test]
fn reliable_datagram_receiver_reassembles_out_of_order_fragments_once() {
    let mut receiver = ReliableDatagramReceiver::default();
    let frames = ReliableDatagramFrame::fragment_payload(7, 11, b"abcdef", 2);

    assert_eq!(frames.len(), 3);
    assert_eq!(receiver.accept(frames[1].clone()), None);
    assert_eq!(receiver.accept(frames[0].clone()), None);
    assert_eq!(receiver.accept(frames[2].clone()), Some(b"abcdef".to_vec()));
    assert_eq!(receiver.accept(frames[2].clone()), None);
}

#[test]
fn reliable_datagram_frame_carries_ack_sequence() {
    let ack = ReliableDatagramFrame::ack(1, 2, 9);

    assert_eq!(ack.stream_id, 1);
    assert_eq!(ack.sequence, 2);
    assert_eq!(ack.ack, Some(9));
    assert!(ack.payload.is_empty());
}

#[test]
fn reliable_datagram_frame_wire_format_round_trips_and_rejects_truncation() {
    let frame = ReliableDatagramFrame {
        stream_id: 3,
        sequence: 8,
        ack: Some(7),
        fragment_index: 1,
        fragment_count: 3,
        payload: b"payload".to_vec(),
    };

    let encoded = frame.encode();
    assert_eq!(ReliableDatagramFrame::decode(&encoded).unwrap(), frame);

    let mut truncated = encoded;
    truncated.pop();
    assert!(ReliableDatagramFrame::decode(&truncated)
        .unwrap_err()
        .to_string()
        .contains("payload length"));
}

#[test]
fn reliable_datagram_socket_sends_and_receives_encoded_frame() {
    let left = ReliableDatagramSocket::bind("127.0.0.1:0", 1500).unwrap();
    let right = ReliableDatagramSocket::bind("127.0.0.1:0", 1500).unwrap();
    right
        .set_read_timeout(Some(std::time::Duration::from_secs(1)))
        .unwrap();
    let frame = ReliableDatagramFrame::single(1, 2, b"hello".to_vec());

    left.send_frame_to(&frame, right.local_addr().unwrap())
        .unwrap();
    let (received, source) = right.recv_frame_from().unwrap();

    assert_eq!(received, frame);
    assert_eq!(source, left.local_addr().unwrap());
}

#[test]
fn rdma_provider_trait_builds_reliable_endpoint_and_validates_availability() {
    let provider = MockRdmaReplicationProvider::available("mock-rdma");
    let endpoint = provider.endpoint("rdma://node-a".to_string());

    assert_eq!(provider.provider_name(), "mock-rdma");
    assert_eq!(endpoint.kind, ReplicationChannelKind::Rdma);
    assert!(endpoint.capabilities.raft_append);
    assert!(provider.validate().is_ok());

    let err = MockRdmaReplicationProvider::unavailable("missing-rdma")
        .validate()
        .unwrap_err();
    assert!(err.to_string().contains("missing-rdma"));
}

#[test]
fn replication_transport_fault_profiles_make_udp_reliability_requirements_explicit() {
    let tcp = ReplicationEndpoint::tcp("127.0.0.1:17687");
    let udp = ReplicationEndpoint::udp("127.0.0.1:17688", 1200);

    assert!(!tcp.capabilities.fault_profile.requires_reliable_delivery());
    assert!(udp.capabilities.fault_profile.requires_reliable_delivery());
    assert!(udp.capabilities.fault_profile.may_drop);
    assert!(udp.capabilities.fault_profile.may_duplicate);
    assert!(udp.capabilities.fault_profile.may_reorder);
}

#[test]
fn tcp_replicator_registers_endpoint_and_tracks_channel_metrics() {
    let routing_table = ShardRoutingTable {
        version: 1,
        placements: vec![ShardPlacement::new(
            0,
            vec![ShardReplica::primary(1), ShardReplica::replica(2)],
        )],
    };
    let replicator = TcpShardReplicator::new(routing_table);

    replicator
        .register_peer_endpoint(2, ReplicationEndpoint::tcp("127.0.0.1:1"))
        .unwrap();
    let entry = LogEntry::new(0, 1, 1, Command::DeleteNode { id: 1 });
    let err = replicator.publish(&entry).unwrap_err();
    let metrics = replicator.channel_metrics();

    assert!(err.to_string().contains("replication ack policy"));
    assert_eq!(metrics.sent_batches, 1);
    assert_eq!(metrics.failed_batches, 1);
    assert_eq!(metrics.sent_entries, 1);
    assert_eq!(metrics.append_conflicts, 0);
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
        .contains("replication peer 2 ack did not include shard 0 index 7"));
    server.join().unwrap();
}

#[test]
fn tcp_raft_pre_vote_codec_round_trips() {
    let request = PreVoteRequest {
        next_term: 7,
        candidate_id: 2,
        last_log_index: 11,
        last_log_term: 6,
    };
    let mut bytes = Vec::new();
    super::tcp_requests::write_tcp_raft_pre_vote_request(&mut bytes, 0, &request).unwrap();
    let mut cursor = std::io::Cursor::new(&bytes[TCP_RAFT_PRE_VOTE_REQUEST_MAGIC.len() + 8..]);

    assert_eq!(
        super::tcp_requests::read_tcp_raft_pre_vote_request_after_magic(&mut cursor).unwrap(),
        request
    );

    let response = PreVoteResponse {
        term: 7,
        vote_granted: true,
    };
    let mut response_bytes = Vec::new();
    super::tcp_responses::write_tcp_raft_pre_vote_response(
        &mut response_bytes,
        &Ok(response.clone()),
    )
    .unwrap();
    assert_eq!(
        super::tcp_responses::read_tcp_raft_pre_vote_response(&mut std::io::Cursor::new(
            response_bytes
        ))
        .unwrap(),
        response
    );
}

#[test]
fn tcp_raft_leader_transfer_response_codec_round_trips() {
    let request = RequestVoteRequest {
        term: 9,
        candidate_id: 3,
        last_log_index: 14,
        last_log_term: 8,
    };
    let mut bytes = Vec::new();
    super::tcp_responses::write_tcp_raft_leader_transfer_response(&mut bytes, &Ok(request.clone()))
        .unwrap();

    assert_eq!(
        super::tcp_responses::read_tcp_raft_leader_transfer_response(&mut std::io::Cursor::new(
            bytes
        ))
        .unwrap(),
        request
    );
}
