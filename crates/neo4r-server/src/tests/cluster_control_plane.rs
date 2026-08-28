use super::*;

fn auth_headers() -> std::collections::HashMap<String, String> {
    [(
        "authorization".to_string(),
        "Bearer admin:secret".to_string(),
    )]
    .into_iter()
    .collect()
}

#[test]
pub(super) fn http_cluster_registry_reports_freshness_metadata() {
    let dir = temp_dir("neo4r-server-http-cluster-registry");
    let routing_table = ShardRoutingTable {
        version: 11,
        placements: vec![ShardPlacement::new(0, vec![ShardReplica::primary(1)])],
    };
    let db = Neo4rDatabaseHandle::open(
        DatabaseConfig::new(&dir, 1, 1)
            .with_server_id(1)
            .with_routing_table(routing_table),
    )
    .unwrap();
    let backend = TcpBackend::with_config(db.clone(), TcpBackendConfig::default())
        .with_web_options(Some("admin:secret".to_string()), Duration::from_millis(250));
    let response = backend.execute_http_request(&HttpRequest {
        method: "GET".to_string(),
        path: "/api/cluster/registry".to_string(),
        query: std::collections::HashMap::new(),
        headers: auth_headers(),
        body: String::new(),
    });

    assert_eq!(response.status, 200);
    assert!(response.body.contains("\"routing_version\":11"));
    assert!(response.body.contains("\"ownership_epoch\":11"));
    assert!(response.body.contains("\"metadata_index\":"));
    assert!(response.body.contains("\"generated_at_ms\":"));
    assert!(response.body.contains("\"ttl_ms\":5000"));
    assert!(response.body.contains("\"migration_state\":\"idle\""));
    assert!(response
        .body
        .contains("\"write_authority\":\"shard_primary_and_raft_leader\""));
    assert!(response.body.contains("\"raft_shards\":"));

    drop(db);
    let _ = fs::remove_dir_all(dir);
}

#[test]
pub(super) fn http_data_path_rejects_stale_ownership_epoch() {
    let dir = temp_dir("neo4r-server-http-stale-ownership-epoch");
    let routing_table = ShardRoutingTable {
        version: 7,
        placements: vec![ShardPlacement::new(0, vec![ShardReplica::primary(1)])],
    };
    let db = Neo4rDatabaseHandle::open(
        DatabaseConfig::new(&dir, 1, 1)
            .with_server_id(1)
            .with_routing_table(routing_table),
    )
    .unwrap();
    let backend = TcpBackend::with_config(db.clone(), TcpBackendConfig::default())
        .with_web_options(Some("admin:secret".to_string()), Duration::from_millis(250));
    let mut headers = auth_headers();
    headers.insert("x-neo4r-ownership-epoch".to_string(), "6".to_string());
    let response = backend.execute_http_request(&HttpRequest {
        method: "POST".to_string(),
        path: "/api/query".to_string(),
        query: std::collections::HashMap::new(),
        headers,
        body: "{\"query\":\"MATCH (n) RETURN n\",\"params\":{}}".to_string(),
    });

    assert_eq!(response.status, 409);
    assert!(response.body.contains("stale ownership epoch"));
    assert!(response.body.contains("\"ownership_epoch\":7"));

    let metrics = backend.execute_http_request(&HttpRequest {
        method: "GET".to_string(),
        path: "/api/metrics".to_string(),
        query: std::collections::HashMap::new(),
        headers: auth_headers(),
        body: String::new(),
    });
    assert_eq!(metrics.status, 200);
    assert!(metrics.body.contains("\"stale_epoch_rejections\":1"));
    assert!(metrics.body.contains("\"migration_state\":\"idle\""));

    drop(db);
    let _ = fs::remove_dir_all(dir);
}

#[test]
pub(super) fn http_capabilities_reports_protocol_features() {
    let dir = temp_dir("neo4r-server-http-capabilities");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1).with_server_id(1)).unwrap();
    let backend = TcpBackend::with_config(db.clone(), TcpBackendConfig::default())
        .with_web_options(Some("admin:secret".to_string()), Duration::from_millis(250));
    let response = backend.execute_http_request(&HttpRequest {
        method: "GET".to_string(),
        path: "/api/capabilities".to_string(),
        query: std::collections::HashMap::new(),
        headers: auth_headers(),
        body: String::new(),
    });

    assert_eq!(response.status, 200);
    assert!(response.body.contains("\"ownership_epoch\":\"true\""));
    assert!(response.body.contains("\"native_protocol_min\":\"1\""));
    assert!(response.body.contains("\"native_protocol_max\":\"1\""));
    assert!(response
        .body
        .contains("\"write_authority\":\"shard_primary_and_raft_leader\""));
    assert!(response.body.contains("\"read_index\":\"true\""));
    assert!(response.body.contains("\"transaction_epoch\":\"true\""));
    assert!(response.body.contains("\"typed_epoch_conflict\":\"true\""));

    drop(db);
    let _ = fs::remove_dir_all(dir);
}

#[test]
pub(super) fn http_query_accepts_bounded_staleness_read_consistency() {
    let dir = temp_dir("neo4r-server-http-read-consistency");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1).with_server_id(1)).unwrap();
    db.create_node(
        vec!["Person".to_string()],
        [("name".to_string(), Value::String("ReadAlice".to_string()))]
            .into_iter()
            .collect(),
    )
    .unwrap();
    let backend = TcpBackend::with_config(db.clone(), TcpBackendConfig::default())
        .with_web_options(Some("admin:secret".to_string()), Duration::from_millis(250));
    let response = backend.execute_http_request(&HttpRequest {
        method: "POST".to_string(),
        path: "/api/query".to_string(),
        query: std::collections::HashMap::new(),
        headers: auth_headers(),
        body: "{\"query\":\"MATCH (n:Person) RETURN n.name\",\"params\":{},\"read_consistency\":\"bounded_staleness\",\"max_staleness_ms\":1000}".to_string(),
    });

    assert_eq!(response.status, 200);
    assert!(response.body.contains("ReadAlice"));

    drop(db);
    let _ = fs::remove_dir_all(dir);
}
