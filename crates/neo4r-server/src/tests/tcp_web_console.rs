#![allow(unused_imports)]
use super::*;

#[test]
pub(super) fn tcp_backend_handles_ping_create_query_and_quit() {
    let dir = temp_dir("neo4r-tcp-backend");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1)).unwrap();
    let backend = TcpBackend::new(db);
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let server = thread::spawn(move || backend.serve_listener_once(listener).unwrap());

    let mut stream = TcpStream::connect(addr).unwrap();

    write_frame(
        &mut stream,
        &NativeFrame::new(NativeMessageType::Ping, 1, Vec::new()),
    )
    .unwrap();
    assert_native_response(&mut stream, NativeMessageType::Response, 1, "OK\tPONG");

    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Command,
            2,
            b"CREATE_NODE\tPerson\tname=s:alice\tage=i:42".to_vec(),
        ),
    )
    .unwrap();
    assert_native_response(&mut stream, NativeMessageType::Response, 2, "OK\tNODE\t0");

    write_frame(
        &mut stream,
        &NativeFrame::new(
            NativeMessageType::Query,
            3,
            b"MATCH (n:Person) WHERE n.name = \"alice\" RETURN n.name".to_vec(),
        ),
    )
    .unwrap();
    let query_response = read_native_payload(&mut stream, NativeMessageType::Response, 3);
    assert!(query_response.starts_with("OK\tRESULT_START\t"));
    let rows = decode_query_rows(query_response.split('\t').nth(6).unwrap()).unwrap();
    assert_eq!(
        rows[0].get("n.name"),
        Some(&neo4r_query::QueryValue::Scalar(Value::String(
            "alice".to_string()
        )))
    );

    write_frame(
            &mut stream,
            &NativeFrame::new(
                NativeMessageType::Query,
                4,
                b"CREATE (n:Person {name: \"Alice\"}) SET n.status = \"active\" RETURN n.name, n.status"
                    .to_vec(),
            ),
        )
        .unwrap();
    let write_response = read_native_payload(&mut stream, NativeMessageType::Response, 4);
    assert!(write_response.starts_with("OK\tRESULT_START\t"));
    let rows = decode_query_rows(write_response.split('\t').nth(6).unwrap()).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get("n.name"),
        Some(&neo4r_query::QueryValue::Scalar(Value::String(
            "Alice".to_string()
        )))
    );
    assert_eq!(
        rows[0].get("n.status"),
        Some(&neo4r_query::QueryValue::Scalar(Value::String(
            "active".to_string()
        )))
    );

    write_frame(
        &mut stream,
        &NativeFrame::new(NativeMessageType::Quit, 5, Vec::new()),
    )
    .unwrap();
    assert_native_response(&mut stream, NativeMessageType::Response, 5, "OK\tBYE");

    server.join().unwrap();
    let _ = fs::remove_dir_all(dir);
}

fn assert_contains_all(text: &str, needles: &[&str]) {
    assert!(
        needles.iter().all(|needle| text.contains(needle)),
        "missing one of {needles:?} in {text}"
    );
}

#[test]
pub(super) fn web_console_serves_index_and_graph_api() {
    let dir = temp_dir("neo4r-web-console");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1)).unwrap();
    db.execute_cypher(r#"CREATE (n:Person {name: "Alice"})"#)
        .unwrap();
    db.execute_cypher(r#"CREATE (n:Person {name: "Bob"})"#)
        .unwrap();
    db.execute_cypher(
        r#"MATCH (a:Person {name: "Alice"}), (b:Person {name: "Bob"}) CREATE (a)-[r:KNOWS {since: 2026}]->(b) RETURN r"#,
    )
    .unwrap();

    let index = web_request(
        TcpBackend::new(db.clone()),
        "GET / HTTP/1.1\r\nhost: localhost\r\n\r\n",
    );
    assert!(index.contains("HTTP/1.1 200 OK"));
    assert!(index.contains("neo4r graph console"));
    assert!(index.contains("three.module.js"));
    assert!(index.contains("id=\"labels\""));
    assert!(index.contains(".graph-label.edge"));
    assert!(index.contains("function nodeLabel"));
    assert!(index.contains("id=\"examples\""));
    assert!(index.contains("id=\"params\""));
    assert!(index.contains("id=\"authToken\""));
    assert!(index.contains("id=\"database\""));
    assert!(index.contains("id=\"createDatabase\""));
    assert!(index.contains("id=\"invokeToken\""));
    assert!(index.contains("id=\"revokeToken\""));
    assert!(index.contains("id=\"auditLog\""));
    assert!(index.contains("id=\"cleanupTokens\""));
    assert!(index.contains("id=\"backup\""));
    assert!(index.contains("id=\"restoreDryRun\""));
    assert!(index.contains("id=\"restoreConfirm\""));
    assert!(index.contains("id=\"restoreApply\""));
    assert!(index.contains("id=\"maintenanceOn\""));
    assert!(index.contains("id=\"maintenanceOff\""));
    assert!(index.contains("id=\"raftStatus\""));
    assert!(index.contains("id=\"snapshotNow\""));
    assert!(index.contains("id=\"verifyInvariants\""));
    assert!(index.contains("id=\"repairInvariants\""));

    let graph = web_request(
        TcpBackend::new(db.clone()),
        "GET /api/graph?limit=10 HTTP/1.1\r\nhost: localhost\r\n\r\n",
    );
    assert!(graph.contains("HTTP/1.1 200 OK"));
    assert!(graph.contains("\"nodes\""));
    assert!(graph.contains("\"Alice\""));
    assert!(graph.contains("\"relationships\""));
    assert!(graph.contains("\"KNOWS\""));

    let examples = web_request(
        TcpBackend::new(db.clone()),
        "GET /api/examples HTTP/1.1\r\nhost: localhost\r\n\r\n",
    );
    assert!(examples.contains("HTTP/1.1 200 OK"));
    assert!(examples.contains("create_with_relationship"));

    let body = "{\"query\":\"MATCH (n:Person) WHERE n.name = $name RETURN n\",\"params\":{\"name\":\"Alice\"}}";
    let query = web_request(
        TcpBackend::new(db.clone()),
        &format!(
            "POST /api/query HTTP/1.1\r\nhost: localhost\r\ncontent-length: {}\r\n\r\n{}",
            body.len(),
            body
        ),
    );
    assert!(query.contains("HTTP/1.1 200 OK"));
    assert!(query.contains("\"columns\""));
    assert!(query.contains("\"rows\""));
    assert!(query.contains("\"plan\":null"));
    assert!(query.contains("\"database\":\"default\""));
    assert!(query.contains("\"Alice\""));

    let plan = web_request(
        TcpBackend::new(db.clone()),
        &format!(
            "POST /api/query-plan HTTP/1.1\r\nhost: localhost\r\ncontent-length: {}\r\n\r\n{}",
            body.len(),
            body
        ),
    );
    assert!(plan.contains("HTTP/1.1 200 OK"));
    assert!(plan.contains("\"plan\""));

    let profile = web_request(
        TcpBackend::new(db.clone()),
        &format!(
            "POST /api/profile HTTP/1.1\r\nhost: localhost\r\ncontent-length: {}\r\n\r\n{}",
            body.len(),
            body
        ),
    );
    assert!(profile.contains("HTTP/1.1 200 OK"));
    assert!(profile.contains("PROFILE"));

    let metrics = web_request(
        TcpBackend::new(db.clone()),
        "GET /api/metrics HTTP/1.1\r\nhost: localhost\r\n\r\n",
    );
    assert!(metrics.contains("HTTP/1.1 200 OK"));
    assert_contains_all(
        &metrics,
        &[
            "\"http_requests\"",
            "\"auth_failures\"",
            "\"auth_rate_limited\"",
            "\"db_nodes\"",
            "\"db_committed_indexes\"",
            "\"db_applied_indexes\"",
            "\"db_shard_count\"",
            "\"index_ready_count\"",
            "\"tenant_database_count\"",
            "\"raft_group_count\"",
            "\"raft_term_max\"",
            "\"web_user_token_count\"",
            "\"web_audit_event_count\"",
            "\"replication_sent_batches\"",
            "\"raft_election_rounds\"",
            "\"raft_append_conflicts\"",
            "\"raft_snapshot_installs\"",
        ],
    );

    let prometheus = web_request(
        TcpBackend::new(db.clone()),
        "GET /metrics HTTP/1.1\r\nhost: localhost\r\n\r\n",
    );
    assert!(prometheus.contains("HTTP/1.1 200 OK"));
    assert!(prometheus.contains("content-type: text/plain; version=0.0.4; charset=utf-8"));
    assert_contains_all(
        &prometheus,
        &[
            "neo4r_http_requests_total",
            "neo4r_auth_failures_total",
            "neo4r_auth_rate_limited_total",
            "neo4r_db_nodes",
            "neo4r_db_committed_index_max",
            "neo4r_index_ready",
            "neo4r_raft_term_max",
            "neo4r_web_audit_events",
            "neo4r_replication_channel_sent_batches_total",
            "neo4r_raft_election_rounds_total",
            "neo4r_raft_append_conflicts_total",
            "neo4r_raft_snapshot_install_duration_ms_total",
            "neo4r_database_shard_lag",
            "neo4r_database_db_nodes{database=\"default\"}",
            "neo4r_database_shard_committed_index{database=\"default\",shard=\"0\",server=\"0\",role=\"unknown\"}",
        ],
    );

    let cluster = web_request(
        TcpBackend::new(db.clone()),
        "GET /api/cluster HTTP/1.1\r\nhost: localhost\r\n\r\n",
    );
    assert!(cluster.contains("HTTP/1.1 200 OK"));
    assert!(cluster.contains("response"));
    assert!(cluster.contains("metadata"));

    let backup_dir = temp_dir("neo4r-web-console-backup");
    let backup_body = format!("{{\"path\":\"{}\"}}", backup_dir.display());
    let backup = web_request(
        TcpBackend::new(db.clone()),
        &format!(
            "POST /api/backup HTTP/1.1\r\nhost: localhost\r\ncontent-length: {}\r\n\r\n{}",
            backup_body.len(),
            backup_body
        ),
    );
    assert!(backup.contains("HTTP/1.1 200 OK"));
    assert!(backup.contains("\"target\""));
    assert!(backup.contains("\"manifest\""));
    assert!(backup.contains("\"checksum\""));
    let manifest = fs::read_to_string(backup_dir.join(BACKUP_MANIFEST_FILE)).unwrap();
    assert!(manifest.contains("neo4r_backup_manifest_version=1"));
    assert!(manifest.contains("database=default"));

    db.execute_cypher(r#"CREATE (n:Person {name: "DryRunOnly"})"#)
        .unwrap();
    let restore_dry_run_body =
        format!("{{\"path\":\"{}\",\"dry_run\":true}}", backup_dir.display());
    let restore_dry_run = web_request(
        TcpBackend::new(db.clone()),
        &format!(
            "POST /api/restore HTTP/1.1\r\nhost: localhost\r\ncontent-length: {}\r\n\r\n{}",
            restore_dry_run_body.len(),
            restore_dry_run_body
        ),
    );
    assert!(restore_dry_run.contains("HTTP/1.1 200 OK"));
    assert!(restore_dry_run.contains("\"dry_run\":true"));
    assert!(restore_dry_run.contains("\"verified\":true"));
    assert!(restore_dry_run.contains("\"checksum\""));
    assert_eq!(
        db.execute_cypher(r#"MATCH (n:Person) WHERE n.name = "DryRunOnly" RETURN n.name"#)
            .unwrap()
            .len(),
        1
    );

    let restore_without_confirm_body = format!(
        "{{\"path\":\"{}\",\"dry_run\":false}}",
        backup_dir.display()
    );
    let restore_without_confirm = web_request(
        TcpBackend::new(db.clone()),
        &format!(
            "POST /api/restore HTTP/1.1\r\nhost: localhost\r\ncontent-length: {}\r\n\r\n{}",
            restore_without_confirm_body.len(),
            restore_without_confirm_body
        ),
    );
    assert!(restore_without_confirm.contains("HTTP/1.1 500 Internal Server Error"));
    assert!(restore_without_confirm.contains("confirm"));

    let restore_locked_body = format!(
        "{{\"path\":\"{}\",\"dry_run\":false,\"confirm\":\"RESTORE\"}}",
        backup_dir.display()
    );
    let restore_without_maintenance = web_request(
        TcpBackend::new(db.clone()),
        &format!(
            "POST /api/restore HTTP/1.1\r\nhost: localhost\r\ncontent-length: {}\r\n\r\n{}",
            restore_locked_body.len(),
            restore_locked_body
        ),
    );
    assert!(restore_without_maintenance.contains("HTTP/1.1 500 Internal Server Error"));
    assert!(restore_without_maintenance.contains("maintenance mode"));

    let maintenance_on_body = "{\"enabled\":true}";
    let maintenance_on = web_request(
        TcpBackend::new(db.clone()),
        &format!(
            "POST /api/admin/maintenance-mode HTTP/1.1\r\nhost: localhost\r\ncontent-length: {}\r\n\r\n{}",
            maintenance_on_body.len(),
            maintenance_on_body
        ),
    );
    assert!(maintenance_on.contains("HTTP/1.1 200 OK"));
    assert!(maintenance_on.contains("\"maintenance_mode\":true"));

    let restore_lock = dir.join("system").join("restore.lock");
    fs::create_dir_all(restore_lock.parent().unwrap()).unwrap();
    fs::write(&restore_lock, b"held").unwrap();
    let restore_locked = web_request(
        TcpBackend::new(db.clone()),
        &format!(
            "POST /api/restore HTTP/1.1\r\nhost: localhost\r\ncontent-length: {}\r\n\r\n{}",
            restore_locked_body.len(),
            restore_locked_body
        ),
    );
    assert!(restore_locked.contains("HTTP/1.1 500 Internal Server Error"));
    assert!(restore_locked.contains("restore lock"));
    fs::remove_file(&restore_lock).unwrap();

    let verify_invariants = web_request(
        TcpBackend::new(db.clone())
            .with_web_options(Some("admin:secret".to_string()), Duration::from_millis(250)),
        "POST /api/admin/verify-invariants HTTP/1.1\r\nhost: localhost\r\nauthorization: Bearer admin:secret\r\ncontent-length: 2\r\n\r\n{}",
    );
    assert!(verify_invariants.contains("HTTP/1.1 200 OK"));
    assert!(verify_invariants.contains("\"action\":\"verify_invariants\""));
    assert!(verify_invariants.contains("clean=true"));

    let repair_invariants = web_request(
        TcpBackend::new(db.clone())
            .with_web_options(Some("admin:secret".to_string()), Duration::from_millis(250)),
        "POST /api/admin/repair-invariants HTTP/1.1\r\nhost: localhost\r\nauthorization: Bearer admin:secret\r\ncontent-length: 2\r\n\r\n{}",
    );
    assert!(repair_invariants.contains("HTTP/1.1 200 OK"));
    assert!(repair_invariants.contains("\"action\":\"repair_invariants\""));

    let backup_payload = first_backup_payload_file(&backup_dir).unwrap();
    fs::write(&backup_payload, b"tampered backup payload").unwrap();
    let restore_tampered = web_request(
        TcpBackend::new(db.clone()),
        &format!(
            "POST /api/restore HTTP/1.1\r\nhost: localhost\r\ncontent-length: {}\r\n\r\n{}",
            restore_dry_run_body.len(),
            restore_dry_run_body
        ),
    );
    assert!(restore_tampered.contains("HTTP/1.1 500 Internal Server Error"));
    assert!(restore_tampered.contains("backup manifest"));

    let secure_backend = TcpBackend::new(db.clone())
        .with_web_options(Some("secret".to_string()), Duration::from_millis(250));
    let unauthorized = web_request(
        secure_backend.clone(),
        "GET /api/metrics HTTP/1.1\r\nhost: localhost\r\n\r\n",
    );
    assert!(unauthorized.contains("HTTP/1.1 401 Unauthorized"));
    let mut rate_limited = String::new();
    for _ in 0..5 {
        rate_limited = web_request(
            secure_backend.clone(),
            "GET /api/metrics HTTP/1.1\r\nhost: localhost\r\n\r\n",
        );
    }
    assert!(rate_limited.contains("HTTP/1.1 429"));

    let reader_query_body = "{\"query\":\"MATCH (n) RETURN n\"}";
    let reader_forbidden = web_request(
        TcpBackend::new(db.clone()).with_web_options(
            Some("reader:secret".to_string()),
            Duration::from_millis(250),
        ),
        &format!(
            "POST /api/query HTTP/1.1\r\nhost: localhost\r\nauthorization: Bearer reader:secret\r\ncontent-length: {}\r\n\r\n{}",
            reader_query_body.len(),
            reader_query_body
        ),
    );
    assert!(reader_forbidden.contains("HTTP/1.1 403 Forbidden"));

    let authorized = web_request(
        secure_backend.clone(),
        "GET /api/metrics?token=secret HTTP/1.1\r\nhost: localhost\r\n\r\n",
    );
    assert!(authorized.contains("HTTP/1.1 200 OK"));
    assert!(authorized.contains("\"auth_failures\":6"));
    assert!(authorized.contains("\"auth_rate_limited\":1"));

    let cookie_authorized = web_request(
        secure_backend.clone(),
        "GET /api/metrics HTTP/1.1\r\nhost: localhost\r\ncookie: neo4r.session=secret\r\n\r\n",
    );
    assert!(cookie_authorized.contains("HTTP/1.1 200 OK"));

    let add_user_body =
        "{\"name\":\"alice\",\"token_id\":\"main\",\"role\":\"writer\",\"token\":\"alice-token\",\"expired_at\":\"0\"}";
    let add_user = web_request(
        TcpBackend::new(db.clone())
            .with_web_options(Some("admin:secret".to_string()), Duration::from_millis(250)),
        &format!(
            "POST /api/admin/invoke-token HTTP/1.1\r\nhost: localhost\r\nauthorization: Bearer admin:secret\r\ncontent-length: {}\r\n\r\n{}",
            add_user_body.len(),
            add_user_body
        ),
    );
    assert!(add_user.contains("HTTP/1.1 200 OK"));
    assert!(add_user.contains("\"alice\""));
    assert!(add_user.contains("\"token_id\":\"main\""));

    let writer_query_body = "{\"query\":\"MATCH (n) RETURN n\"}";
    let writer_query = web_request(
        TcpBackend::new(db.clone()),
        &format!(
            "POST /api/query HTTP/1.1\r\nhost: localhost\r\nauthorization: Bearer alice-token\r\ncontent-length: {}\r\n\r\n{}",
            writer_query_body.len(),
            writer_query_body
        ),
    );
    assert!(writer_query.contains("HTTP/1.1 200 OK"));

    let users_after_use = web_request(
        TcpBackend::new(db.clone())
            .with_web_options(Some("admin:secret".to_string()), Duration::from_millis(250)),
        "GET /api/admin/users HTTP/1.1\r\nhost: localhost\r\nauthorization: Bearer admin:secret\r\n\r\n",
    );
    assert!(users_after_use.contains("HTTP/1.1 200 OK"));
    assert!(!users_after_use.contains("\"last_used_at\":0"));

    let expired_user_body =
        "{\"name\":\"alice\",\"token_id\":\"old\",\"role\":\"writer\",\"token\":\"expired-token\",\"expired_at\":\"1\"}";
    let expired_user = web_request(
        TcpBackend::new(db.clone())
            .with_web_options(Some("admin:secret".to_string()), Duration::from_millis(250)),
        &format!(
            "POST /api/admin/invoke-token HTTP/1.1\r\nhost: localhost\r\nauthorization: Bearer admin:secret\r\ncontent-length: {}\r\n\r\n{}",
            expired_user_body.len(),
            expired_user_body
        ),
    );
    assert!(expired_user.contains("HTTP/1.1 200 OK"));
    assert!(expired_user.contains("\"active\":false"));

    let expired_query = web_request(
        TcpBackend::new(db.clone()),
        &format!(
            "POST /api/query HTTP/1.1\r\nhost: localhost\r\nauthorization: Bearer expired-token\r\ncontent-length: {}\r\n\r\n{}",
            writer_query_body.len(),
            writer_query_body
        ),
    );
    assert!(expired_query.contains("HTTP/1.1 401 Unauthorized"));

    let cleanup = web_request(
        TcpBackend::new(db.clone())
            .with_web_options(Some("admin:secret".to_string()), Duration::from_millis(250)),
        "POST /api/admin/cleanup-expired-tokens HTTP/1.1\r\nhost: localhost\r\nauthorization: Bearer admin:secret\r\ncontent-length: 2\r\n\r\n{}",
    );
    assert!(cleanup.contains("HTTP/1.1 200 OK"));
    assert!(cleanup.contains("\"removed\":1"));

    let users_after_cleanup = web_request(
        TcpBackend::new(db.clone())
            .with_web_options(Some("admin:secret".to_string()), Duration::from_millis(250)),
        "GET /api/admin/users HTTP/1.1\r\nhost: localhost\r\nauthorization: Bearer admin:secret\r\n\r\n",
    );
    assert!(users_after_cleanup.contains("HTTP/1.1 200 OK"));
    assert!(!users_after_cleanup.contains("\"token_id\":\"old\""));

    let revoke_body = "{\"name\":\"alice\",\"token_id\":\"main\"}";
    let revoke = web_request(
        TcpBackend::new(db.clone())
            .with_web_options(Some("admin:secret".to_string()), Duration::from_millis(250)),
        &format!(
            "POST /api/admin/revoke-token HTTP/1.1\r\nhost: localhost\r\nauthorization: Bearer admin:secret\r\ncontent-length: {}\r\n\r\n{}",
            revoke_body.len(),
            revoke_body
        ),
    );
    assert!(revoke.contains("HTTP/1.1 200 OK"));
    assert!(revoke.contains("\"revoked\":true"));

    let revoked_query = web_request(
        TcpBackend::new(db.clone()),
        &format!(
            "POST /api/query HTTP/1.1\r\nhost: localhost\r\nauthorization: Bearer alice-token\r\ncontent-length: {}\r\n\r\n{}",
            writer_query_body.len(),
            writer_query_body
        ),
    );
    assert!(revoked_query.contains("HTTP/1.1 401 Unauthorized"));

    let _ = fs::remove_dir_all(backup_dir);
    let _ = fs::remove_dir_all(dir);
}

#[test]
pub(super) fn web_console_isolates_tenant_databases_and_scopes_tokens() {
    let dir = temp_dir("neo4r-web-tenants");
    let config = DatabaseConfig::new(&dir, 1, 1);
    let db = Neo4rDatabaseHandle::open(config.clone()).unwrap();

    let databases = web_request(
        TcpBackend::new(db.clone())
            .with_multi_tenant_config(config.clone())
            .unwrap()
            .with_web_options(Some("admin:secret".to_string()), Duration::from_millis(250)),
        "GET /api/admin/databases HTTP/1.1\r\nhost: localhost\r\nauthorization: Bearer admin:secret\r\n\r\n",
    );
    assert!(databases.contains("HTTP/1.1 200 OK"));
    assert!(databases.contains("\"default\""));

    let create_db_body = "{\"name\":\"tenant_a\"}";
    let create_db = web_request(
        TcpBackend::new(db.clone())
            .with_multi_tenant_config(config.clone())
            .unwrap()
            .with_web_options(Some("admin:secret".to_string()), Duration::from_millis(250)),
        &format!(
            "POST /api/admin/databases HTTP/1.1\r\nhost: localhost\r\nauthorization: Bearer admin:secret\r\ncontent-length: {}\r\n\r\n{}",
            create_db_body.len(),
            create_db_body
        ),
    );
    assert!(create_db.contains("HTTP/1.1 200 OK"));
    assert!(create_db.contains("\"tenant_a\""));

    let tenant_create_body =
        "{\"database\":\"tenant_a\",\"query\":\"CREATE (n:Tenant {name: \\\"OnlyTenant\\\"}) RETURN n\"}";
    let tenant_create = web_request(
        TcpBackend::new(db.clone())
            .with_multi_tenant_config(config.clone())
            .unwrap()
            .with_web_options(Some("admin:secret".to_string()), Duration::from_millis(250)),
        &format!(
            "POST /api/query HTTP/1.1\r\nhost: localhost\r\nauthorization: Bearer admin:secret\r\ncontent-length: {}\r\n\r\n{}",
            tenant_create_body.len(),
            tenant_create_body
        ),
    );
    assert!(tenant_create.contains("HTTP/1.1 200 OK"));
    assert!(tenant_create.contains("OnlyTenant"));

    let tenant_graph = web_request(
        TcpBackend::new(db.clone())
            .with_multi_tenant_config(config.clone())
            .unwrap()
            .with_web_options(Some("admin:secret".to_string()), Duration::from_millis(250)),
        "GET /api/graph?db=tenant_a HTTP/1.1\r\nhost: localhost\r\nauthorization: Bearer admin:secret\r\n\r\n",
    );
    assert!(tenant_graph.contains("HTTP/1.1 200 OK"));
    assert!(tenant_graph.contains("OnlyTenant"));

    let default_graph = web_request(
        TcpBackend::new(db.clone())
            .with_multi_tenant_config(config.clone())
            .unwrap()
            .with_web_options(Some("admin:secret".to_string()), Duration::from_millis(250)),
        "GET /api/graph HTTP/1.1\r\nhost: localhost\r\nauthorization: Bearer admin:secret\r\n\r\n",
    );
    assert!(default_graph.contains("HTTP/1.1 200 OK"));
    assert!(!default_graph.contains("OnlyTenant"));

    let scoped_user_body =
        "{\"name\":\"tenant_user\",\"token_id\":\"main\",\"role\":\"reader\",\"token\":\"tenant-token\",\"database\":\"tenant_a\",\"database_role\":\"writer\",\"expired_at\":\"0\"}";
    let scoped_user = web_request(
        TcpBackend::new(db.clone())
            .with_multi_tenant_config(config.clone())
            .unwrap()
            .with_web_options(Some("admin:secret".to_string()), Duration::from_millis(250)),
        &format!(
            "POST /api/admin/invoke-token HTTP/1.1\r\nhost: localhost\r\nauthorization: Bearer admin:secret\r\ncontent-length: {}\r\n\r\n{}",
            scoped_user_body.len(),
            scoped_user_body
        ),
    );
    assert!(scoped_user.contains("HTTP/1.1 200 OK"));
    assert!(scoped_user.contains("tenant_a=writer"));

    let tenant_query = web_request(
        TcpBackend::new(db.clone())
            .with_multi_tenant_config(config.clone())
            .unwrap(),
        &format!(
            "POST /api/query HTTP/1.1\r\nhost: localhost\r\nauthorization: Bearer tenant-token\r\ncontent-length: {}\r\n\r\n{}",
            tenant_create_body.len(),
            tenant_create_body
        ),
    );
    assert!(tenant_query.contains("HTTP/1.1 200 OK"));

    let selected_database = web_request(
        TcpBackend::new(db.clone())
            .with_multi_tenant_config(config.clone())
            .unwrap(),
        "GET /api/database?db=tenant_a HTTP/1.1\r\nhost: localhost\r\nauthorization: Bearer tenant-token\r\n\r\n",
    );
    assert!(selected_database.contains("HTTP/1.1 200 OK"));
    assert!(selected_database.contains("\"database\":\"tenant_a\""));

    let use_database_body = "{\"database\":\"tenant_a\"}";
    let use_database = web_request(
        TcpBackend::new(db.clone())
            .with_multi_tenant_config(config.clone())
            .unwrap(),
        &format!(
            "POST /api/use-database HTTP/1.1\r\nhost: localhost\r\nauthorization: Bearer tenant-token\r\ncontent-length: {}\r\n\r\n{}",
            use_database_body.len(),
            use_database_body
        ),
    );
    assert!(use_database.contains("HTTP/1.1 200 OK"));
    assert!(use_database.contains("\"database\":\"tenant_a\""));

    let use_create_body =
        "{\"query\":\"USE tenant_a CREATE (n:Tenant {name: \\\"UseSyntaxTenant\\\"}) RETURN n\"}";
    let use_create = web_request(
        TcpBackend::new(db.clone())
            .with_multi_tenant_config(config.clone())
            .unwrap(),
        &format!(
            "POST /api/query HTTP/1.1\r\nhost: localhost\r\nauthorization: Bearer tenant-token\r\ncontent-length: {}\r\n\r\n{}",
            use_create_body.len(),
            use_create_body
        ),
    );
    assert!(use_create.contains("HTTP/1.1 200 OK"));
    assert!(use_create.contains("UseSyntaxTenant"));

    let use_plan_body = "{\"query\":\"USE tenant_a MATCH (n:Tenant) RETURN n\"}";
    let use_plan = web_request(
        TcpBackend::new(db.clone())
            .with_multi_tenant_config(config.clone())
            .unwrap(),
        &format!(
            "POST /api/query-plan HTTP/1.1\r\nhost: localhost\r\nauthorization: Bearer tenant-token\r\ncontent-length: {}\r\n\r\n{}",
            use_plan_body.len(),
            use_plan_body
        ),
    );
    assert!(use_plan.contains("HTTP/1.1 200 OK"));

    let tenant_backup_dir = temp_dir("neo4r-web-tenant-backup");
    let tenant_backup_body = format!(
        "{{\"database\":\"tenant_a\",\"path\":\"{}\"}}",
        tenant_backup_dir.display()
    );
    let tenant_backup_forbidden = web_request(
        TcpBackend::new(db.clone())
            .with_multi_tenant_config(config.clone())
            .unwrap(),
        &format!(
            "POST /api/backup HTTP/1.1\r\nhost: localhost\r\nauthorization: Bearer tenant-token\r\ncontent-length: {}\r\n\r\n{}",
            tenant_backup_body.len(),
            tenant_backup_body
        ),
    );
    assert!(tenant_backup_forbidden.contains("HTTP/1.1 403 Forbidden"));

    let tenant_restore_body = format!(
        "{{\"database\":\"tenant_a\",\"path\":\"{}\",\"dry_run\":true}}",
        tenant_backup_dir.display()
    );
    let tenant_restore_forbidden = web_request(
        TcpBackend::new(db.clone())
            .with_multi_tenant_config(config.clone())
            .unwrap(),
        &format!(
            "POST /api/restore HTTP/1.1\r\nhost: localhost\r\nauthorization: Bearer tenant-token\r\ncontent-length: {}\r\n\r\n{}",
            tenant_restore_body.len(),
            tenant_restore_body
        ),
    );
    assert!(tenant_restore_forbidden.contains("HTTP/1.1 403 Forbidden"));

    let tenant_snapshot_forbidden = web_request(
        TcpBackend::new(db.clone())
            .with_multi_tenant_config(config.clone())
            .unwrap(),
        "POST /api/admin/cluster/snapshot?db=tenant_a HTTP/1.1\r\nhost: localhost\r\nauthorization: Bearer tenant-token\r\ncontent-length: 2\r\n\r\n{}",
    );
    assert!(tenant_snapshot_forbidden.contains("HTTP/1.1 403 Forbidden"));

    let tenant_migration_forbidden = web_request(
        TcpBackend::new(db.clone())
            .with_multi_tenant_config(config.clone())
            .unwrap(),
        "POST /api/admin/cluster/migration/advance?db=tenant_a HTTP/1.1\r\nhost: localhost\r\nauthorization: Bearer tenant-token\r\ncontent-length: 2\r\n\r\n{}",
    );
    assert!(tenant_migration_forbidden.contains("HTTP/1.1 403 Forbidden"));

    let tenant_backup = web_request(
        TcpBackend::new(db.clone())
            .with_multi_tenant_config(config.clone())
            .unwrap()
            .with_web_options(Some("admin:secret".to_string()), Duration::from_millis(250)),
        &format!(
            "POST /api/backup HTTP/1.1\r\nhost: localhost\r\nauthorization: Bearer admin:secret\r\ncontent-length: {}\r\n\r\n{}",
            tenant_backup_body.len(),
            tenant_backup_body
        ),
    );
    assert!(tenant_backup.contains("HTTP/1.1 200 OK"));
    assert!(tenant_backup.contains("\"manifest\""));

    let tenant_restore_default_body = format!(
        "{{\"path\":\"{}\",\"dry_run\":true}}",
        tenant_backup_dir.display()
    );
    let tenant_backup_default_restore = web_request(
        TcpBackend::new(db.clone())
            .with_multi_tenant_config(config.clone())
            .unwrap()
            .with_web_options(Some("admin:secret".to_string()), Duration::from_millis(250)),
        &format!(
            "POST /api/restore HTTP/1.1\r\nhost: localhost\r\nauthorization: Bearer admin:secret\r\ncontent-length: {}\r\n\r\n{}",
            tenant_restore_default_body.len(),
            tenant_restore_default_body
        ),
    );
    assert!(tenant_backup_default_restore.contains("HTTP/1.1 500 Internal Server Error"));
    assert!(tenant_backup_default_restore.contains("database mismatch"));

    let active_delete_db_body = "{\"name\":\"tenant_a\"}";
    let active_delete_db = web_request(
        TcpBackend::new(db.clone())
            .with_multi_tenant_config(config.clone())
            .unwrap()
            .with_web_options(Some("admin:secret".to_string()), Duration::from_millis(250)),
        &format!(
            "POST /api/admin/delete-database HTTP/1.1\r\nhost: localhost\r\nauthorization: Bearer admin:secret\r\ncontent-length: {}\r\n\r\n{}",
            active_delete_db_body.len(),
            active_delete_db_body
        ),
    );
    assert!(active_delete_db.contains("HTTP/1.1 400 Bad Request"));
    assert!(active_delete_db.contains("must be disabled before delete"));

    let disable_db_body = "{\"name\":\"tenant_a\"}";
    let disable_db = web_request(
        TcpBackend::new(db.clone())
            .with_multi_tenant_config(config.clone())
            .unwrap()
            .with_web_options(Some("admin:secret".to_string()), Duration::from_millis(250)),
        &format!(
            "POST /api/admin/disable-database HTTP/1.1\r\nhost: localhost\r\nauthorization: Bearer admin:secret\r\ncontent-length: {}\r\n\r\n{}",
            disable_db_body.len(),
            disable_db_body
        ),
    );
    assert!(disable_db.contains("HTTP/1.1 200 OK"));
    assert!(disable_db.contains("\"disabled\":true"));

    let disabled_access = web_request(
        TcpBackend::new(db.clone())
            .with_multi_tenant_config(config.clone())
            .unwrap(),
        "GET /api/database?db=tenant_a HTTP/1.1\r\nhost: localhost\r\nauthorization: Bearer tenant-token\r\n\r\n",
    );
    assert!(disabled_access.contains("HTTP/1.1 400 Bad Request"));

    let enable_db = web_request(
        TcpBackend::new(db.clone())
            .with_multi_tenant_config(config.clone())
            .unwrap()
            .with_web_options(Some("admin:secret".to_string()), Duration::from_millis(250)),
        &format!(
            "POST /api/admin/enable-database HTTP/1.1\r\nhost: localhost\r\nauthorization: Bearer admin:secret\r\ncontent-length: {}\r\n\r\n{}",
            disable_db_body.len(),
            disable_db_body
        ),
    );
    assert!(enable_db.contains("HTTP/1.1 200 OK"));
    assert!(enable_db.contains("\"disabled\":false"));

    let default_query_body = "{\"query\":\"MATCH (n) RETURN n\"}";
    let default_forbidden = web_request(
        TcpBackend::new(db.clone())
            .with_multi_tenant_config(config.clone())
            .unwrap(),
        &format!(
            "POST /api/query HTTP/1.1\r\nhost: localhost\r\nauthorization: Bearer tenant-token\r\ncontent-length: {}\r\n\r\n{}",
            default_query_body.len(),
            default_query_body
        ),
    );
    assert!(default_forbidden.contains("HTTP/1.1 401 Unauthorized"));

    let use_default_body = "{\"query\":\"USE default MATCH (n) RETURN n\"}";
    let use_default_forbidden = web_request(
        TcpBackend::new(db.clone())
            .with_multi_tenant_config(config)
            .unwrap(),
        &format!(
            "POST /api/query HTTP/1.1\r\nhost: localhost\r\nauthorization: Bearer tenant-token\r\ncontent-length: {}\r\n\r\n{}",
            use_default_body.len(),
            use_default_body
        ),
    );
    assert!(use_default_forbidden.contains("HTTP/1.1 401 Unauthorized"));

    let redisable_db = web_request(
        TcpBackend::new(db.clone())
            .with_multi_tenant_config(DatabaseConfig::new(&dir, 1, 1))
            .unwrap()
            .with_web_options(Some("admin:secret".to_string()), Duration::from_millis(250)),
        &format!(
            "POST /api/admin/disable-database HTTP/1.1\r\nhost: localhost\r\nauthorization: Bearer admin:secret\r\ncontent-length: {}\r\n\r\n{}",
            disable_db_body.len(),
            disable_db_body
        ),
    );
    assert!(redisable_db.contains("HTTP/1.1 200 OK"));

    let delete_db = web_request(
        TcpBackend::new(db.clone())
            .with_multi_tenant_config(DatabaseConfig::new(&dir, 1, 1))
            .unwrap()
            .with_web_options(Some("admin:secret".to_string()), Duration::from_millis(250)),
        &format!(
            "POST /api/admin/delete-database HTTP/1.1\r\nhost: localhost\r\nauthorization: Bearer admin:secret\r\ncontent-length: {}\r\n\r\n{}",
            disable_db_body.len(),
            disable_db_body
        ),
    );
    assert!(delete_db.contains("HTTP/1.1 200 OK"));
    assert!(delete_db.contains("\"revoked_tokens\":1"));
    assert!(!delete_db.contains("\"tenant_a\""));

    let audit = web_request(
        TcpBackend::new(db.clone())
            .with_multi_tenant_config(DatabaseConfig::new(&dir, 1, 1))
            .unwrap()
            .with_web_options(Some("admin:secret".to_string()), Duration::from_millis(250)),
        "GET /api/admin/audit-log HTTP/1.1\r\nhost: localhost\r\nauthorization: Bearer admin:secret\r\n\r\n",
    );
    assert!(audit.contains("HTTP/1.1 200 OK"));
    assert!(audit.contains("database.delete"));
    assert!(audit.contains("token.invoke"));

    let filtered_audit = web_request(
        TcpBackend::new(db.clone())
            .with_web_options(Some("admin:secret".to_string()), Duration::from_millis(250)),
        "GET /api/admin/audit-log?action=token&limit=1 HTTP/1.1\r\nhost: localhost\r\nauthorization: Bearer admin:secret\r\n\r\n",
    );
    assert!(filtered_audit.contains("HTTP/1.1 200 OK"));
    assert!(filtered_audit.contains("token."));
    assert!(!filtered_audit.contains("database.delete"));

    let metrics = web_request(
        TcpBackend::new(db.clone())
            .with_multi_tenant_config(DatabaseConfig::new(&dir, 1, 1))
            .unwrap()
            .with_web_options(Some("admin:secret".to_string()), Duration::from_millis(250)),
        "GET /api/metrics HTTP/1.1\r\nhost: localhost\r\nauthorization: Bearer admin:secret\r\n\r\n",
    );
    assert!(metrics.contains("HTTP/1.1 200 OK"));
    assert!(metrics.contains("\"web_user_token_count\""));
    assert!(metrics.contains("\"web_audit_event_count\""));

    let raft_status = web_request(
        TcpBackend::new(db.clone())
            .with_multi_tenant_config(DatabaseConfig::new(&dir, 1, 1))
            .unwrap()
            .with_web_options(Some("admin:secret".to_string()), Duration::from_millis(250)),
        "GET /api/admin/raft-status HTTP/1.1\r\nhost: localhost\r\nauthorization: Bearer admin:secret\r\n\r\n",
    );
    assert!(raft_status.contains("HTTP/1.1 200 OK"));
    assert!(raft_status.contains("\"raft_shards\""));

    let snapshot_now = web_request(
        TcpBackend::new(db)
            .with_multi_tenant_config(DatabaseConfig::new(&dir, 1, 1))
            .unwrap()
            .with_web_options(Some("admin:secret".to_string()), Duration::from_millis(250)),
        "POST /api/admin/snapshot-now HTTP/1.1\r\nhost: localhost\r\nauthorization: Bearer admin:secret\r\ncontent-length: 2\r\n\r\n{}",
    );
    assert!(snapshot_now.contains("HTTP/1.1 200 OK"));
    assert!(snapshot_now.contains("\"action\":\"snapshot\""));

    let _ = fs::remove_dir_all(dir);
    let _ = fs::remove_dir_all(tenant_backup_dir);
}
