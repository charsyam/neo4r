use super::*;

#[test]
pub(super) fn native_read_write_transaction_rejects_stale_ownership_epoch_on_commit() {
    let dir = temp_dir("neo4r-native-tx-stale-epoch");
    let routing_table = ShardRoutingTable {
        version: 1,
        placements: vec![ShardPlacement::new(0, vec![ShardReplica::primary(1)])],
    };
    let db = Neo4rDatabaseHandle::open(
        DatabaseConfig::new(&dir, 1, 1)
            .with_server_id(1)
            .with_routing_table(routing_table),
    )
    .unwrap();
    let backend = TcpBackend::with_config(
        db.clone(),
        TcpBackendConfig {
            worker_count: 1,
            queue_capacity: 8,
            default_page_size: 10,
            read_preference: QueryReadPreference::Primary,
            catch_up_connect_timeout: Duration::from_secs(1),
        },
    );
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let server = thread::spawn(move || backend.serve_listener_once(listener).unwrap());

    let mut stream = TcpStream::connect(addr).unwrap();
    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Command,
            1,
            b"BEGIN_TX\tREAD_WRITE READ_COMMITTED".to_vec(),
        ),
    )
    .unwrap();
    let begin = read_native_payload(&mut stream, NativeMessageType::Response, 1);
    assert!(begin.contains("ownership_epoch=1"));
    let tx_id = begin.split('\t').collect::<Vec<_>>()[2]
        .parse::<u64>()
        .unwrap();

    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Command,
            2,
            format!("TX_QUERY\t{tx_id}\tCREATE (n:Person {{name: \"EpochAlice\"}}) RETURN n")
                .into_bytes(),
        ),
    )
    .unwrap();
    assert_native_response(
        &mut stream,
        NativeMessageType::Response,
        2,
        &format!("OK\tTX_STAGED\t{tx_id}\t1"),
    );

    db.install_routing_table(ShardRoutingTable {
        version: 2,
        placements: vec![ShardPlacement::new(0, vec![ShardReplica::primary(1)])],
    })
    .unwrap();

    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Command,
            3,
            format!("COMMIT_TX\t{tx_id}").into_bytes(),
        ),
    )
    .unwrap();
    let err = read_native_payload(&mut stream, NativeMessageType::Error, 3);
    assert!(err.contains("ERR\tSTALE_EPOCH"));
    assert!(err.contains("tx_epoch=1"));
    assert!(err.contains("current_epoch=2"));
    assert!(err.contains("retryable=true"));

    write_frame(
        &mut stream,
        &NativeFrame::new(NativeMessageType::Quit, 4, Vec::new()),
    )
    .unwrap();
    assert_native_response(&mut stream, NativeMessageType::Response, 4, "OK\tBYE");

    server.join().unwrap();
    drop(db);
    let _ = fs::remove_dir_all(dir);
}
