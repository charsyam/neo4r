#![allow(unused_imports)]
use super::*;

#[test]
pub(super) fn web_session_csrf_and_logout_are_enforced() {
    let dir = temp_dir("neo4r-web-session-security");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1)).unwrap();
    fs::create_dir_all(dir.join("system")).unwrap();
    let backend = TcpBackend::new(db.clone())
        .with_web_options(Some("admin:secret".to_string()), Duration::from_millis(250));
    let session_body = "{\"token\":\"admin:secret\"}";
    let session = web_request(
        backend.clone(),
        &format!(
            "POST /api/session HTTP/1.1\r\nhost: localhost\r\ncontent-length: {}\r\n\r\n{}",
            session_body.len(),
            session_body
        ),
    );
    assert!(session.contains("HTTP/1.1 200 OK"), "{session}");
    let session_id = json_response_field(&session, "session_id");
    let csrf_token = json_response_field(&session, "csrf_token");
    let query_body = "{\"query\":\"MATCH (n) RETURN n\"}";

    let without_csrf = web_request(
        backend.clone(),
        &format!(
            "POST /api/query HTTP/1.1\r\nhost: localhost\r\ncookie: neo4r.session={session_id}\r\ncontent-length: {}\r\n\r\n{}",
            query_body.len(),
            query_body
        ),
    );
    assert!(without_csrf.contains("HTTP/1.1 403 Forbidden"));

    let with_csrf = web_request(
        backend.clone(),
        &format!(
            "POST /api/query HTTP/1.1\r\nhost: localhost\r\ncookie: neo4r.session={session_id}\r\nx-neo4r-csrf: {csrf_token}\r\ncontent-length: {}\r\n\r\n{}",
            query_body.len(),
            query_body
        ),
    );
    assert!(with_csrf.contains("HTTP/1.1 200 OK"));

    let logout = web_request(
        backend.clone(),
        &format!(
            "POST /api/session/logout HTTP/1.1\r\nhost: localhost\r\ncookie: neo4r.session={session_id}\r\ncontent-length: 0\r\n\r\n"
        ),
    );
    assert!(logout.contains("HTTP/1.1 200 OK"));

    let after_logout = web_request(
        backend,
        &format!(
            "GET /api/metrics HTTP/1.1\r\nhost: localhost\r\ncookie: neo4r.session={session_id}\r\n\r\n"
        ),
    );
    assert!(after_logout.contains("HTTP/1.1 401 Unauthorized"));
    let _ = fs::remove_dir_all(dir);
}

#[test]
pub(super) fn restore_maintenance_drains_native_backend_writes() {
    let dir = temp_dir("neo4r-native-restore-drain");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1)).unwrap();
    let backend = TcpBackend::new(db.clone());
    let enabled = web_request(
        backend.clone(),
        "POST /api/admin/maintenance-mode HTTP/1.1\r\nhost: localhost\r\ncontent-length: 16\r\n\r\n{\"enabled\":true}",
    );
    assert!(enabled.contains("HTTP/1.1 200 OK"));

    let response = backend.execute_backend_request(BackendRequest::CreateNode {
        labels: vec!["Person".to_string()],
        properties: Properties::new(),
    });
    assert!(
        matches!(response, BackendResponse::Err(message) if message.contains("draining mutating requests"))
    );
    let _ = fs::remove_dir_all(dir);
}

#[test]
pub(super) fn pitr_restore_plan_requires_admin_and_reports_target_indexes() {
    let dir = temp_dir("neo4r-web-pitr-plan");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1)).unwrap();
    db.execute_cypher(r#"CREATE (n:Pitr {name: "Before"})"#)
        .unwrap();
    let body =
        "{\"target_physical_ms\":18446744073709551615,\"target_logical\":0,\"dry_run\":true}";

    let forbidden = web_request(
        TcpBackend::new(db.clone())
            .with_web_options(Some("writer:secret".to_string()), Duration::from_millis(250)),
        &format!(
            "POST /api/admin/restore-pitr HTTP/1.1\r\nhost: localhost\r\nauthorization: Bearer writer:secret\r\ncontent-length: {}\r\n\r\n{}",
            body.len(),
            body
        ),
    );
    assert!(forbidden.contains("HTTP/1.1 403 Forbidden"));

    let plan = web_request(
        TcpBackend::new(db.clone())
            .with_web_options(Some("admin:secret".to_string()), Duration::from_millis(250)),
        &format!(
            "POST /api/admin/restore-pitr HTTP/1.1\r\nhost: localhost\r\nauthorization: Bearer admin:secret\r\ncontent-length: {}\r\n\r\n{}",
            body.len(),
            body
        ),
    );
    assert!(plan.contains("HTTP/1.1 200 OK"), "{plan}");
    assert!(plan.contains("\"dry_run\":true"));
    assert!(plan.contains("\"target_index\":1"));
    assert!(plan.contains("\"selected_entries\":1"));
    let _ = fs::remove_dir_all(dir);
}

fn json_response_field(response: &str, name: &str) -> String {
    response
        .split(&format!("\"{name}\":\""))
        .nth(1)
        .and_then(|part| part.split('"').next())
        .unwrap()
        .to_string()
}
