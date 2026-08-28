#![allow(unused_imports)]
use super::*;

#[test]
pub(super) fn native_command_forwards_index_catalog_writes_to_metadata_primary() {
    let local_dir = temp_dir("neo4r-forward-index-local");
    let remote_dir = temp_dir("neo4r-forward-index-remote");
    let routing_table = ShardRoutingTable {
        version: 12,
        placements: vec![ShardPlacement::new(
            0,
            vec![ShardReplica::primary(2), ShardReplica::replica(1)],
        )],
    };
    let local_db = Neo4rDatabaseHandle::open(
        DatabaseConfig::new(&local_dir, 1, 1)
            .with_server_id(1)
            .with_routing_table(routing_table.clone()),
    )
    .unwrap();
    let remote_db = Neo4rDatabaseHandle::open(
        DatabaseConfig::new(&remote_dir, 1, 1)
            .with_server_id(2)
            .with_routing_table(routing_table),
    )
    .unwrap();

    let remote_backend = TcpBackend::new(remote_db.clone());
    let remote_listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let remote_addr = remote_listener.local_addr().unwrap();
    let remote_server =
        thread::spawn(move || remote_backend.serve_listener_once(remote_listener).unwrap());
    let local_backend = TcpBackend::new(local_db.clone());
    local_backend
        .register_query_peer(2, remote_addr.to_string())
        .unwrap();
    let local_listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let local_addr = local_listener.local_addr().unwrap();
    let local_server =
        thread::spawn(move || local_backend.serve_listener_once(local_listener).unwrap());

    let mut stream = TcpStream::connect(local_addr).unwrap();
    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Command,
            1,
            b"CREATE_VECTOR_INDEX\tdoc_embedding\tDocument\tembedding\t2\tcosine".to_vec(),
        ),
    )
    .unwrap();
    assert_native_response(&mut stream, NativeMessageType::Response, 1, "OK");

    write_frame(
        &mut stream,
        &NativeFrame::new(NativeMessageType::Quit, 2, Vec::new()),
    )
    .unwrap();
    assert_native_response(&mut stream, NativeMessageType::Response, 2, "OK\tBYE");

    local_server.join().unwrap();
    remote_server.join().unwrap();
    assert!(local_db.list_indexes().unwrap().is_empty());
    assert_eq!(remote_db.list_indexes().unwrap().len(), 1);

    drop(local_db);
    drop(remote_db);
    let _ = fs::remove_dir_all(local_dir);
    let _ = fs::remove_dir_all(remote_dir);
}

#[test]
pub(super) fn native_command_syncs_index_catalog_from_peer() {
    let local_dir = temp_dir("neo4r-sync-index-local");
    let remote_dir = temp_dir("neo4r-sync-index-remote");
    let routing_table = ShardRoutingTable {
        version: 13,
        placements: vec![ShardPlacement::new(
            0,
            vec![ShardReplica::primary(2), ShardReplica::replica(1)],
        )],
    };
    let local_db = Neo4rDatabaseHandle::open(
        DatabaseConfig::new(&local_dir, 1, 1)
            .with_server_id(1)
            .with_routing_table(routing_table.clone()),
    )
    .unwrap();
    local_db
        .apply_replicated_entry(LogEntry::new_with_metadata(
            0,
            1,
            1,
            2,
            13,
            HybridTimestamp::new(1, 0),
            Command::CreateNode {
                id: 0,
                labels: vec!["Document".to_string()],
                properties: [
                    ("title".to_string(), Value::String("local".to_string())),
                    ("embedding".to_string(), Value::Vector(vec![1.0, 0.0])),
                ]
                .into_iter()
                .collect(),
            },
        ))
        .unwrap();
    let remote_db = Neo4rDatabaseHandle::open(
        DatabaseConfig::new(&remote_dir, 1, 1)
            .with_server_id(2)
            .with_routing_table(routing_table),
    )
    .unwrap();
    remote_db
        .create_vector_index("doc_embedding", "Document", "embedding", 2, "cosine")
        .unwrap();
    assert!(local_db.list_indexes().unwrap().is_empty());

    let remote_backend = TcpBackend::new(remote_db.clone());
    let remote_listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let remote_addr = remote_listener.local_addr().unwrap();
    let remote_server =
        thread::spawn(move || remote_backend.serve_listener_once(remote_listener).unwrap());
    let local_backend = TcpBackend::new(local_db.clone());
    local_backend
        .register_query_peer(2, remote_addr.to_string())
        .unwrap();
    let local_listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let local_addr = local_listener.local_addr().unwrap();
    let local_server =
        thread::spawn(move || local_backend.serve_listener_once(local_listener).unwrap());

    let mut stream = TcpStream::connect(local_addr).unwrap();
    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Command,
            1,
            b"SYNC_INDEX_CATALOG_FROM_PEER\t2".to_vec(),
        ),
    )
    .unwrap();
    assert_native_response(&mut stream, NativeMessageType::Response, 1, "OK");

    write_frame(
        &mut stream,
        &NativeFrame::new(NativeMessageType::Quit, 2, Vec::new()),
    )
    .unwrap();
    assert_native_response(&mut stream, NativeMessageType::Response, 2, "OK\tBYE");

    local_server.join().unwrap();
    remote_server.join().unwrap();
    assert_eq!(
        local_db.list_indexes().unwrap(),
        remote_db.list_indexes().unwrap()
    );
    assert_eq!(
            local_db
                .query(
                    "MATCH (n:Document) WHERE vector.knn(n.embedding, [1.0, 0.0], 1) RETURN n.title",
                )
                .unwrap()
                .len(),
            1
        );
    assert_eq!(
        local_db
            .query_plan(
                "MATCH (n:Document) WHERE vector.knn(n.embedding, [1.0, 0.0], 1) RETURN n.title",
            )
            .unwrap()
            .access_plan,
        QueryAccessPlan::VectorIndexSeek {
            label: Some("Document".to_string()),
            property: "embedding".to_string(),
            metric: "cosine".to_string(),
        }
    );
    let status = local_db
        .execute_cypher("SHOW VECTOR INDEX STATUS doc_embedding")
        .unwrap();
    assert_eq!(status.len(), 1);
    assert_eq!(
        status[0].get("entries"),
        Some(&neo4r_query::QueryValue::Scalar(Value::Int(1)))
    );

    drop(local_db);
    drop(remote_db);
    let reopened_local = Neo4rDatabaseHandle::open(
        DatabaseConfig::new(&local_dir, 1, 1)
            .with_server_id(1)
            .with_routing_table(ShardRoutingTable {
                version: 13,
                placements: vec![ShardPlacement::new(
                    0,
                    vec![ShardReplica::primary(2), ShardReplica::replica(1)],
                )],
            }),
    )
    .unwrap();
    assert_eq!(
        reopened_local
            .query_plan(
                "MATCH (n:Document) WHERE vector.knn(n.embedding, [1.0, 0.0], 1) RETURN n.title",
            )
            .unwrap()
            .access_plan,
        QueryAccessPlan::VectorIndexSeek {
            label: Some("Document".to_string()),
            property: "embedding".to_string(),
            metric: "cosine".to_string(),
        }
    );
    assert_eq!(
            reopened_local
                .query(
                    "MATCH (n:Document) WHERE vector.knn(n.embedding, [1.0, 0.0], 1) RETURN n.title",
                )
                .unwrap()
                .len(),
            1
        );
    let status = reopened_local
        .execute_cypher("SHOW VECTOR INDEX STATUS doc_embedding")
        .unwrap();
    assert_eq!(status.len(), 1);
    assert_eq!(
        status[0].get("entries"),
        Some(&neo4r_query::QueryValue::Scalar(Value::Int(1)))
    );
    drop(reopened_local);
    let _ = fs::remove_dir_all(local_dir);
    let _ = fs::remove_dir_all(remote_dir);
}

#[test]
pub(super) fn native_command_rebuilds_vector_indexes() {
    let dir = temp_dir("neo4r-native-rebuild-vector-indexes");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1)).unwrap();
    let backend = TcpBackend::new(db);
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let server = thread::spawn(move || backend.serve_listener_once(listener).unwrap());

    let mut stream = TcpStream::connect(addr).unwrap();
    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Command,
            1,
            b"CREATE_VECTOR_INDEX\tdoc_embedding\tDocument\tembedding\t2\tcosine".to_vec(),
        ),
    )
    .unwrap();
    assert_native_response(&mut stream, NativeMessageType::Response, 1, "OK");

    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Command,
            2,
            b"CREATE_NODE\tDocument\ttitle=s:near\tembedding=v:1.0,0.0".to_vec(),
        ),
    )
    .unwrap();
    assert_native_response(&mut stream, NativeMessageType::Response, 2, "OK\tNODE\t0");

    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Command,
            3,
            b"CREATE_NODE\tDocument\ttitle=s:far\tembedding=v:0.0,1.0".to_vec(),
        ),
    )
    .unwrap();
    assert_native_response(&mut stream, NativeMessageType::Response, 3, "OK\tNODE\t1");

    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Command,
            4,
            b"REBUILD_VECTOR_INDEXES".to_vec(),
        ),
    )
    .unwrap();
    assert_native_response(&mut stream, NativeMessageType::Response, 4, "OK");

    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Command,
            5,
            b"VECTOR_INDEX_STATUS".to_vec(),
        ),
    )
    .unwrap();
    let status = read_native_payload(&mut stream, NativeMessageType::Response, 5);
    assert!(status.starts_with("OK\tVECTOR_INDEX_STATUS\t"));
    assert!(status.contains("doc_embedding:Document:embedding"));
    assert!(status.contains("dimensions=2"));
    assert!(status.contains("metric=cosine"));
    assert!(status.contains("entries=2"));

    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Command,
            6,
            b"VECTOR_INDEX_STATUS\tdoc_embedding".to_vec(),
        ),
    )
    .unwrap();
    assert_native_response(
            &mut stream,
            NativeMessageType::Response,
            6,
            "OK\tVECTOR_INDEX_STATUS\tdoc_embedding:Document:embedding:dimensions=2:metric=cosine:entries=2",
        );

    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Command,
            7,
            b"REBUILD_VECTOR_INDEX\tdoc_embedding".to_vec(),
        ),
    )
    .unwrap();
    assert_native_response(&mut stream, NativeMessageType::Response, 7, "OK");

    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Query,
            8,
            b"MATCH (n:Document) WHERE vector.knn(n.embedding, [1.0, 0.0], 1) RETURN n.title"
                .to_vec(),
        ),
    )
    .unwrap();
    let response = read_native_payload(&mut stream, NativeMessageType::Response, 8);
    assert!(response.starts_with("OK\tRESULT_START\t"));
    let rows = decode_query_rows(response.split('\t').nth(6).unwrap()).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get("n.title"),
        Some(&neo4r_query::QueryValue::Scalar(Value::String(
            "near".to_string()
        )))
    );

    write_frame(
        &mut stream,
        &NativeFrame::new(NativeMessageType::Quit, 9, Vec::new()),
    )
    .unwrap();
    assert_native_response(&mut stream, NativeMessageType::Response, 9, "OK\tBYE");

    server.join().unwrap();
    let _ = fs::remove_dir_all(dir);
}

#[test]
pub(super) fn native_vector_index_status_survives_reopen() {
    let dir = temp_dir("neo4r-native-vector-status-reopen");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1)).unwrap();
    db.create_vector_index("doc_embedding", "Document", "embedding", 2, "cosine")
        .unwrap();
    db.create_node(
        vec!["Document".to_string()],
        [
            ("title".to_string(), Value::String("near".to_string())),
            (
                "embedding".to_string(),
                Value::Vector(vec![1.0_f32, 0.0_f32]),
            ),
        ]
        .into_iter()
        .collect(),
    )
    .unwrap();
    db.create_node(
        vec!["Document".to_string()],
        [
            ("title".to_string(), Value::String("far".to_string())),
            (
                "embedding".to_string(),
                Value::Vector(vec![0.0_f32, 1.0_f32]),
            ),
        ]
        .into_iter()
        .collect(),
    )
    .unwrap();
    drop(db);

    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1)).unwrap();
    let backend = TcpBackend::new(db);
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let server = thread::spawn(move || backend.serve_listener_once(listener).unwrap());

    let mut stream = TcpStream::connect(addr).unwrap();
    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Command,
            1,
            b"VECTOR_INDEX_STATUS\tdoc_embedding".to_vec(),
        ),
    )
    .unwrap();
    assert_native_response(
            &mut stream,
            NativeMessageType::Response,
            1,
            "OK\tVECTOR_INDEX_STATUS\tdoc_embedding:Document:embedding:dimensions=2:metric=cosine:entries=2",
        );

    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Query,
            2,
            b"MATCH (n:Document) WHERE vector.knn(n.embedding, [1.0, 0.0], 1) RETURN n.title"
                .to_vec(),
        ),
    )
    .unwrap();
    let response = read_native_payload(&mut stream, NativeMessageType::Response, 2);
    assert!(response.starts_with("OK\tRESULT_START\t"));
    let rows = decode_query_rows(response.split('\t').nth(6).unwrap()).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get("n.title"),
        Some(&neo4r_query::QueryValue::Scalar(Value::String(
            "near".to_string()
        )))
    );

    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Query,
            3,
            b"SHOW VECTOR INDEX STATUS doc_embedding".to_vec(),
        ),
    )
    .unwrap();
    let status = read_native_payload(&mut stream, NativeMessageType::Response, 3);
    assert!(status.starts_with("OK\tRESULT_START\t"));
    let rows = decode_query_rows(status.split('\t').nth(6).unwrap()).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get("entries"),
        Some(&neo4r_query::QueryValue::Scalar(Value::Int(2)))
    );

    write_frame(
        &mut stream,
        &NativeFrame::new(NativeMessageType::Quit, 4, Vec::new()),
    )
    .unwrap();
    assert_native_response(&mut stream, NativeMessageType::Response, 4, "OK\tBYE");

    server.join().unwrap();
    let _ = fs::remove_dir_all(dir);
}

#[test]
pub(super) fn native_query_forwards_create_node_to_remote_primary() {
    let local_dir = temp_dir("neo4r-forward-cypher-write-local");
    let remote_dir = temp_dir("neo4r-forward-cypher-write-remote");
    let routing_table = ShardRoutingTable {
        version: 6,
        placements: vec![ShardPlacement::new(
            0,
            vec![ShardReplica::primary(2), ShardReplica::replica(1)],
        )],
    };
    let local_db = Neo4rDatabaseHandle::open(
        DatabaseConfig::new(&local_dir, 1, 1)
            .with_server_id(1)
            .with_routing_table(routing_table.clone()),
    )
    .unwrap();
    let remote_db = Neo4rDatabaseHandle::open(
        DatabaseConfig::new(&remote_dir, 1, 1)
            .with_server_id(2)
            .with_routing_table(routing_table),
    )
    .unwrap();

    let remote_backend = TcpBackend::new(remote_db.clone());
    let remote_listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let remote_addr = remote_listener.local_addr().unwrap();
    let remote_server =
        thread::spawn(move || remote_backend.serve_listener_once(remote_listener).unwrap());
    let local_backend = TcpBackend::new(local_db.clone());
    local_backend
        .register_query_peer(2, remote_addr.to_string())
        .unwrap();
    let local_listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let local_addr = local_listener.local_addr().unwrap();
    let local_server =
        thread::spawn(move || local_backend.serve_listener_once(local_listener).unwrap());

    let mut stream = TcpStream::connect(local_addr).unwrap();
    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Query,
            1,
            b"CREATE (n:Person {name: $name}) RETURN n\tname=s:RemoteCypher".to_vec(),
        ),
    )
    .unwrap();
    let start = read_native_payload(&mut stream, NativeMessageType::Response, 1);
    let parts = start.split('\t').collect::<Vec<_>>();
    assert_eq!(parts[0..3], ["OK", "RESULT_START", "1"]);
    assert_eq!(parts[3], "1");
    assert_eq!(parts[4], "1");
    assert_eq!(parts[5], "false");
    let rows = decode_query_rows(parts[6]).unwrap();
    let Some(neo4r_query::QueryValue::Node(node)) = rows[0].get("n") else {
        panic!("expected created node");
    };
    assert_eq!(
        node.properties.get("name"),
        Some(&Value::String("RemoteCypher".to_string()))
    );

    write_frame(
        &mut stream,
        &NativeFrame::new(NativeMessageType::Quit, 2, Vec::new()),
    )
    .unwrap();
    assert_native_response(&mut stream, NativeMessageType::Response, 2, "OK\tBYE");

    local_server.join().unwrap();
    remote_server.join().unwrap();
    assert_eq!(
        remote_db
            .query(r#"MATCH (n:Person) WHERE n.name = "RemoteCypher" RETURN n.name"#)
            .unwrap()
            .len(),
        1
    );

    drop(local_db);
    drop(remote_db);
    let _ = fs::remove_dir_all(local_dir);
    let _ = fs::remove_dir_all(remote_dir);
}

#[test]
pub(super) fn native_query_routes_create_node_by_stable_hash_across_shards() {
    let local_dir = temp_dir("neo4r-create-hash-route-local");
    let remote_dir = temp_dir("neo4r-create-hash-route-remote");
    let routing_table = ShardRoutingTable {
        version: 7,
        placements: vec![
            ShardPlacement::new(0, vec![ShardReplica::primary(1)]),
            ShardPlacement::new(1, vec![ShardReplica::primary(2), ShardReplica::replica(1)]),
        ],
    };
    let local_db = Neo4rDatabaseHandle::open(
        DatabaseConfig::new(&local_dir, 2, 2)
            .with_server_id(1)
            .with_routing_table(routing_table.clone()),
    )
    .unwrap();
    let remote_db = Neo4rDatabaseHandle::open(
        DatabaseConfig::new(&remote_dir, 2, 2)
            .with_server_id(2)
            .with_routing_table(routing_table),
    )
    .unwrap();

    let query = "CREATE (n:Person {name: $name}) RETURN n";
    let status = local_db.cluster_status().unwrap();
    let canonical_params = [("name".to_string(), Value::String("same-user".to_string()))]
        .into_iter()
        .collect();
    let spaced_query = "  CREATE   ( n : Person { name : $name } )   RETURN   n  ";
    assert_eq!(
        select_create_node_write_shard(&status, query, &canonical_params)
            .unwrap()
            .shard_id,
        select_create_node_write_shard(&status, spaced_query, &canonical_params)
            .unwrap()
            .shard_id
    );
    let create_set_query = "CREATE (n:Person {name: $name}) SET n.active = true RETURN n";
    let create_set_equivalent_query = "CREATE (n:Person {active: true, name: $name}) RETURN n";
    assert_eq!(
        select_create_node_write_shard(&status, create_set_query, &canonical_params)
            .unwrap()
            .shard_id,
        select_create_node_write_shard(&status, create_set_equivalent_query, &canonical_params)
            .unwrap()
            .shard_id
    );

    let mut local_name = None;
    let mut remote_name = None;
    for candidate in 0..128 {
        let name = format!("user-{candidate}");
        let params = [("name".to_string(), Value::String(name.clone()))]
            .into_iter()
            .collect();
        let shard_id = select_create_node_write_shard(&status, query, &params)
            .unwrap()
            .shard_id;
        if shard_id == 0 && local_name.is_none() {
            local_name = Some(name);
        } else if shard_id == 1 && remote_name.is_none() {
            remote_name = Some(name);
        }
        if local_name.is_some() && remote_name.is_some() {
            break;
        }
    }
    let local_name = local_name.expect("expected a candidate for shard 0");
    let remote_name = remote_name.expect("expected a candidate for shard 1");

    let remote_backend = TcpBackend::new(remote_db.clone());
    let remote_listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let remote_addr = remote_listener.local_addr().unwrap();
    let remote_server =
        thread::spawn(move || remote_backend.serve_listener_once(remote_listener).unwrap());
    let local_backend = TcpBackend::new(local_db.clone());
    local_backend
        .register_query_peer(2, remote_addr.to_string())
        .unwrap();
    let local_listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let local_addr = local_listener.local_addr().unwrap();
    let local_server =
        thread::spawn(move || local_backend.serve_listener_once(local_listener).unwrap());

    let mut stream = TcpStream::connect(local_addr).unwrap();
    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Query,
            1,
            format!("{query}\tname=s:{local_name}").into_bytes(),
        ),
    )
    .unwrap();
    let local_response = read_native_payload(&mut stream, NativeMessageType::Response, 1);
    assert!(local_response.starts_with("OK\tRESULT_START\t"));

    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Query,
            2,
            format!("{query}\tname=s:{remote_name}").into_bytes(),
        ),
    )
    .unwrap();
    let remote_response = read_native_payload(&mut stream, NativeMessageType::Response, 2);
    assert!(remote_response.starts_with("OK\tRESULT_START\t"));

    write_frame(
        &mut stream,
        &NativeFrame::new(NativeMessageType::Quit, 3, Vec::new()),
    )
    .unwrap();
    assert_native_response(&mut stream, NativeMessageType::Response, 3, "OK\tBYE");

    local_server.join().unwrap();
    remote_server.join().unwrap();
    assert_eq!(
        local_db
            .query(&format!(
                r#"MATCH (n:Person) WHERE n.name = "{local_name}" RETURN n"#
            ))
            .unwrap()
            .len(),
        1
    );
    assert!(local_db
        .query(&format!(
            r#"MATCH (n:Person) WHERE n.name = "{remote_name}" RETURN n"#
        ))
        .unwrap()
        .is_empty());
    assert_eq!(
        remote_db
            .query(&format!(
                r#"MATCH (n:Person) WHERE n.name = "{remote_name}" RETURN n"#
            ))
            .unwrap()
            .len(),
        1
    );
    assert!(remote_db
        .query(&format!(
            r#"MATCH (n:Person) WHERE n.name = "{local_name}" RETURN n"#
        ))
        .unwrap()
        .is_empty());

    drop(local_db);
    drop(remote_db);
    let _ = fs::remove_dir_all(local_dir);
    let _ = fs::remove_dir_all(remote_dir);
}

#[test]
pub(super) fn native_query_routes_merge_node_to_single_owner_shard() {
    let local_dir = temp_dir("neo4r-merge-hash-route-local");
    let remote_dir = temp_dir("neo4r-merge-hash-route-remote");
    let routing_table = ShardRoutingTable {
        version: 8,
        placements: vec![
            ShardPlacement::new(0, vec![ShardReplica::primary(1)]),
            ShardPlacement::new(1, vec![ShardReplica::primary(2), ShardReplica::replica(1)]),
        ],
    };
    let local_db = Neo4rDatabaseHandle::open(
        DatabaseConfig::new(&local_dir, 2, 2)
            .with_server_id(1)
            .with_routing_table(routing_table.clone()),
    )
    .unwrap();
    let remote_db = Neo4rDatabaseHandle::open(
        DatabaseConfig::new(&remote_dir, 2, 2)
            .with_server_id(2)
            .with_routing_table(routing_table),
    )
    .unwrap();

    let query = "MERGE (n:Person {email: $email}) RETURN n";
    let status = local_db.cluster_status().unwrap();
    let mut local_email = None;
    let mut remote_email = None;
    for candidate in 0..128 {
        let email = format!("user-{candidate}@example.com");
        let params = [("email".to_string(), Value::String(email.clone()))]
            .into_iter()
            .collect();
        let shard_id = select_merge_node_write_shard(&status, query, &params)
            .unwrap()
            .shard_id;
        if shard_id == 0 && local_email.is_none() {
            local_email = Some(email);
        } else if shard_id == 1 && remote_email.is_none() {
            remote_email = Some(email);
        }
        if local_email.is_some() && remote_email.is_some() {
            break;
        }
    }
    let local_email = local_email.expect("expected a candidate for shard 0");
    let remote_email = remote_email.expect("expected a candidate for shard 1");

    let remote_backend = TcpBackend::new(remote_db.clone());
    let remote_listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let remote_addr = remote_listener.local_addr().unwrap();
    let remote_server = thread::spawn(move || {
        remote_backend
            .clone()
            .serve_listener_once(remote_listener.try_clone().unwrap())
            .unwrap();
        remote_backend.serve_listener_once(remote_listener).unwrap();
    });
    let local_backend = TcpBackend::new(local_db.clone());
    local_backend
        .register_query_peer(2, remote_addr.to_string())
        .unwrap();
    let local_listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let local_addr = local_listener.local_addr().unwrap();
    let local_server =
        thread::spawn(move || local_backend.serve_listener_once(local_listener).unwrap());

    let mut stream = TcpStream::connect(local_addr).unwrap();
    for (request_id, email) in [(1, local_email.as_str()), (2, remote_email.as_str())] {
        for repeat in 0..2 {
            write_frame(
                &mut stream,
                &NativeFrame::new(
                    NativeMessageType::Query,
                    request_id + repeat * 10,
                    format!("{query}\temail=s:{email}").into_bytes(),
                ),
            )
            .unwrap();
            let response = read_native_payload(
                &mut stream,
                NativeMessageType::Response,
                request_id + repeat * 10,
            );
            assert!(response.starts_with("OK\tRESULT_START\t"));
        }
    }

    write_frame(
        &mut stream,
        &NativeFrame::new(NativeMessageType::Quit, 99, Vec::new()),
    )
    .unwrap();
    assert_native_response(&mut stream, NativeMessageType::Response, 99, "OK\tBYE");

    local_server.join().unwrap();
    remote_server.join().unwrap();
    assert_eq!(
        local_db
            .query(&format!(
                r#"MATCH (n:Person) WHERE n.email = "{local_email}" RETURN n"#
            ))
            .unwrap()
            .len(),
        1
    );
    assert!(local_db
        .query(&format!(
            r#"MATCH (n:Person) WHERE n.email = "{remote_email}" RETURN n"#
        ))
        .unwrap()
        .is_empty());
    assert_eq!(
        remote_db
            .query(&format!(
                r#"MATCH (n:Person) WHERE n.email = "{remote_email}" RETURN n"#
            ))
            .unwrap()
            .len(),
        1
    );
    assert!(remote_db
        .query(&format!(
            r#"MATCH (n:Person) WHERE n.email = "{local_email}" RETURN n"#
        ))
        .unwrap()
        .is_empty());

    drop(local_db);
    drop(remote_db);
    let _ = fs::remove_dir_all(local_dir);
    let _ = fs::remove_dir_all(remote_dir);
}

#[test]
pub(super) fn native_query_forwards_set_node_to_remote_primary() {
    let local_dir = temp_dir("neo4r-forward-cypher-set-local");
    let remote_dir = temp_dir("neo4r-forward-cypher-set-remote");
    let routing_table = ShardRoutingTable {
        version: 7,
        placements: vec![ShardPlacement::new(
            0,
            vec![ShardReplica::primary(2), ShardReplica::replica(1)],
        )],
    };
    let local_db = Neo4rDatabaseHandle::open(
        DatabaseConfig::new(&local_dir, 1, 1)
            .with_server_id(1)
            .with_routing_table(routing_table.clone()),
    )
    .unwrap();
    let remote_db = Neo4rDatabaseHandle::open(
        DatabaseConfig::new(&remote_dir, 1, 1)
            .with_server_id(2)
            .with_routing_table(routing_table),
    )
    .unwrap();
    let entry = LogEntry::new_with_metadata(
        0,
        1,
        1,
        2,
        7,
        HybridTimestamp::new(1, 0),
        Command::CreateNode {
            id: 0,
            labels: vec!["Person".to_string()],
            properties: [("name".to_string(), Value::String("Alice".to_string()))]
                .into_iter()
                .collect(),
        },
    );
    remote_db.apply_replicated_entry(entry.clone()).unwrap();
    local_db.apply_replicated_entry(entry).unwrap();

    let remote_backend = TcpBackend::new(remote_db.clone());
    let remote_listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let remote_addr = remote_listener.local_addr().unwrap();
    let remote_server =
        thread::spawn(move || remote_backend.serve_listener_once(remote_listener).unwrap());
    let local_backend = TcpBackend::new(local_db.clone());
    local_backend
        .register_query_peer(2, remote_addr.to_string())
        .unwrap();
    let local_listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let local_addr = local_listener.local_addr().unwrap();
    let local_server =
        thread::spawn(move || local_backend.serve_listener_once(local_listener).unwrap());

    let mut stream = TcpStream::connect(local_addr).unwrap();
    write_frame(
            &mut stream,
            &NativeFrame::new(
                NativeMessageType::Query,
                1,
                b"MATCH (n:Person) WHERE n.name = $name SET n.status = $status\tname=s:Alice\tstatus=s:active".to_vec(),
            ),
        )
        .unwrap();
    let start = read_native_payload(&mut stream, NativeMessageType::Response, 1);
    let parts = start.split('\t').collect::<Vec<_>>();
    assert_eq!(parts[0..3], ["OK", "RESULT_START", "1"]);
    assert_eq!(parts[3], "0");

    write_frame(
        &mut stream,
        &NativeFrame::new(NativeMessageType::Quit, 2, Vec::new()),
    )
    .unwrap();
    assert_native_response(&mut stream, NativeMessageType::Response, 2, "OK\tBYE");

    local_server.join().unwrap();
    remote_server.join().unwrap();
    assert_eq!(
        remote_db
            .query(r#"MATCH (n:Person) WHERE n.status = "active" RETURN n.name"#)
            .unwrap()
            .len(),
        1
    );

    drop(local_db);
    drop(remote_db);
    let _ = fs::remove_dir_all(local_dir);
    let _ = fs::remove_dir_all(remote_dir);
}
