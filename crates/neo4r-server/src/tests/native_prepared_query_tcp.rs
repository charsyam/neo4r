use super::*;

#[test]
pub(super) fn native_prepared_query_executes_with_params_and_transactions() {
    let dir = temp_dir("neo4r-native-prepared-query");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1)).unwrap();
    let backend = TcpBackend::with_config(
        db,
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
            b"PREPARE_QUERY\tCREATE (n:Person {name: $name}) RETURN n.name".to_vec(),
        ),
    )
    .unwrap();
    assert_native_response(
        &mut stream,
        NativeMessageType::Response,
        1,
        "OK\tPREPARED_QUERY\t1",
    );

    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Command,
            2,
            b"DESCRIBE_PREPARED\t1".to_vec(),
        ),
    )
    .unwrap();
    assert_native_response(
        &mut stream,
        NativeMessageType::Response,
        2,
        "OK\tPREPARED_QUERY_DESC\t1\tWRITE\tWRITE_SHARD_BY_PARAM\t1\tname",
    );

    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Command,
            3,
            b"PREPARED_QUERY_ROUTE\t1\tname=s:Alice".to_vec(),
        ),
    )
    .unwrap();
    let route = read_native_payload(&mut stream, NativeMessageType::Response, 3);
    assert!(
        route == "OK\tPREPARED_QUERY_ROUTE\t1\tWRITE_SHARD:0"
            || route == "OK\tPREPARED_QUERY_ROUTE\t1\tWRITE_SHARD:1"
    );

    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Command,
            4,
            b"EXECUTE_PREPARED\t1\tname=s:Alice".to_vec(),
        ),
    )
    .unwrap();
    let start = read_native_payload(&mut stream, NativeMessageType::Response, 4);
    let parts = start.split('\t').collect::<Vec<_>>();
    assert_eq!(parts[0..3], ["OK", "RESULT_START", "1"]);
    assert_eq!(parts[3], "1");
    let rows = decode_query_rows(parts[6]).unwrap();
    assert_eq!(
        rows[0].get("n.name"),
        Some(&neo4r_query::QueryValue::Scalar(Value::String(
            "Alice".to_string()
        )))
    );

    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Command,
            5,
            b"PREPARE_QUERY\tMATCH (n:Person) WHERE n.name = $name SET n.status = $status".to_vec(),
        ),
    )
    .unwrap();
    assert_native_response(
        &mut stream,
        NativeMessageType::Response,
        5,
        "OK\tPREPARED_QUERY\t2",
    );

    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Command,
            6,
            b"DESCRIBE_PREPARED\t2".to_vec(),
        ),
    )
    .unwrap();
    assert_native_response(
        &mut stream,
        NativeMessageType::Response,
        6,
        "OK\tPREPARED_QUERY_DESC\t2\tWRITE\tWRITE_TARGET_DYNAMIC\t2\tname,status",
    );

    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Command,
            7,
            b"BEGIN_TX\tREAD_WRITE".to_vec(),
        ),
    )
    .unwrap();
    let begin = read_native_payload(&mut stream, NativeMessageType::Response, 7);
    let tx_id = begin.split('\t').collect::<Vec<_>>()[2]
        .parse::<u64>()
        .unwrap();

    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Command,
            8,
            format!("TX_PREPARED_QUERY_ROUTE\t{tx_id}\t2\tname=s:Alice\tstatus=s:active")
                .into_bytes(),
        ),
    )
    .unwrap();
    let tx_route = read_native_payload(&mut stream, NativeMessageType::Response, 8);
    assert_eq!(
        tx_route,
        format!(
            "OK\tTX_PREPARED_QUERY_ROUTE\t{tx_id}\t2\tWRITE_TARGET_DYNAMIC\ttx_mode=READ_WRITE tx_isolation=SNAPSHOT staged_writes=0 staged_overlay=none ownership_epoch=1"
        )
    );

    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Command,
            9,
            format!("TX_EXECUTE_PREPARED\t{tx_id}\t2\tname=s:Alice\tstatus=s:active").into_bytes(),
        ),
    )
    .unwrap();
    assert_native_response(
        &mut stream,
        NativeMessageType::Response,
        9,
        &format!("OK\tTX_STAGED\t{tx_id}\t1"),
    );

    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Command,
            10,
            format!("COMMIT_TX\t{tx_id}").into_bytes(),
        ),
    )
    .unwrap();
    assert_native_response(
        &mut stream,
        NativeMessageType::Response,
        10,
        &format!("OK\tTX_COMMIT\t{tx_id}\t1"),
    );

    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Query,
            11,
            b"MATCH (n:Person) WHERE n.name = $name RETURN n.status\tname=s:Alice".to_vec(),
        ),
    )
    .unwrap();
    let start = read_native_payload(&mut stream, NativeMessageType::Response, 11);
    let parts = start.split('\t').collect::<Vec<_>>();
    let rows = decode_query_rows(parts[6]).unwrap();
    assert_eq!(
        rows[0].get("n.status"),
        Some(&neo4r_query::QueryValue::Scalar(Value::String(
            "active".to_string()
        )))
    );

    write_frame(
        &mut stream,
        &NativeFrame::new(NativeMessageType::Command, 12, b"LIST_PREPARED".to_vec()),
    )
    .unwrap();
    let list = read_native_payload(&mut stream, NativeMessageType::Response, 12);
    assert!(list.starts_with("OK\tPREPARED_QUERY_LIST\t2\t"));
    assert!(list.contains("1:CREATE (n:Person {name: $name}) RETURN n.name"));

    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Command,
            13,
            b"CLOSE_PREPARED\t1".to_vec(),
        ),
    )
    .unwrap();
    assert_native_response(
        &mut stream,
        NativeMessageType::Response,
        13,
        "OK\tPREPARED_QUERY_CLOSED\t1",
    );

    write_frame(
        &mut stream,
        &NativeFrame::new(NativeMessageType::Quit, 14, Vec::new()),
    )
    .unwrap();
    assert_native_response(&mut stream, NativeMessageType::Response, 14, "OK\tBYE");

    server.join().unwrap();
    let _ = fs::remove_dir_all(dir);
}
