use super::*;

#[test]
fn parses_cli_args() {
    let args = CliArgs::parse([
        "--addr".to_string(),
        "127.0.0.1:9000".to_string(),
        "--query".to_string(),
        "MATCH (n) RETURN n".to_string(),
        "--history-file".to_string(),
        "/tmp/neo4r-history".to_string(),
    ])
    .unwrap();

    assert_eq!(args.address, "127.0.0.1:9000");
    assert_eq!(args.query, Some("MATCH (n) RETURN n".to_string()));
    assert_eq!(args.history_file, Some(PathBuf::from("/tmp/neo4r-history")));
}

#[test]
fn parses_native_tls_cli_args() {
    let args = CliArgs::parse([
        "--addr".to_string(),
        "127.0.0.1:9000".to_string(),
        "--tls-ca".to_string(),
        "/etc/neo4r/tls/ca.crt".to_string(),
        "--tls-server-name".to_string(),
        "neo4r.local".to_string(),
        "--tls-client-cert".to_string(),
        "/etc/neo4r/tls/client.crt".to_string(),
        "--tls-client-key".to_string(),
        "/etc/neo4r/tls/client.key".to_string(),
    ])
    .unwrap();

    assert_eq!(args.tls_ca, Some(PathBuf::from("/etc/neo4r/tls/ca.crt")));
    assert_eq!(args.tls_server_name, Some("neo4r.local".to_string()));
    assert_eq!(
        args.tls_client_cert,
        Some(PathBuf::from("/etc/neo4r/tls/client.crt"))
    );
    assert_eq!(
        args.tls_client_key,
        Some(PathBuf::from("/etc/neo4r/tls/client.key"))
    );
    assert_eq!(default_tls_server_name("127.0.0.1:9000"), "127.0.0.1");

    let err = CliArgs::parse([
        "--tls-ca".to_string(),
        "/etc/neo4r/tls/ca.crt".to_string(),
        "--tls-client-cert".to_string(),
        "/etc/neo4r/tls/client.crt".to_string(),
    ])
    .unwrap_err()
    .to_string();
    assert!(err.contains("--tls-client-cert and --tls-client-key must be provided together"));
}

#[test]
fn parses_plan_and_admin_args() {
    let plan = CliArgs::parse(["--plan".to_string(), "MATCH (n) RETURN n".to_string()]).unwrap();
    assert_eq!(plan.plan, Some("MATCH (n) RETURN n".to_string()));

    let admin = CliArgs::parse([
        "--http-host".to_string(),
        "127.0.0.1".to_string(),
        "--http-port".to_string(),
        "18080".to_string(),
        "--admin-token".to_string(),
        "admin:secret".to_string(),
        "--database".to_string(),
        "tenant_a".to_string(),
        "--backup".to_string(),
        "/tmp/neo4r-backup".to_string(),
    ])
    .unwrap();
    assert_eq!(admin.http_port, 18080);
    assert_eq!(admin.database, Some("tenant_a".to_string()));
    assert_eq!(admin.backup_path, Some("/tmp/neo4r-backup".to_string()));
    assert!(admin.has_admin_action());
}

#[test]
fn parses_cli_subcommands() {
    let query = CliArgs::parse(["query".to_string(), "MATCH (n) RETURN n".to_string()]).unwrap();
    assert_eq!(query.query, Some("MATCH (n) RETURN n".to_string()));

    let admin = CliArgs::parse(["admin".to_string(), "users".to_string()]).unwrap();
    assert!(admin.list_users);

    let prune = CliArgs::parse([
        "admin".to_string(),
        "prune-audit".to_string(),
        "90".to_string(),
    ])
    .unwrap();
    assert_eq!(prune.prune_audit_retention_days, Some(90));

    let pitr = CliArgs::parse([
        "admin".to_string(),
        "restore-pitr".to_string(),
        "12345".to_string(),
        "--restore-pitr-target-logical".to_string(),
        "7".to_string(),
    ])
    .unwrap();
    assert_eq!(pitr.restore_pitr_target_ms, Some(12345));
    assert_eq!(pitr.restore_pitr_target_logical, 7);

    let backup = CliArgs::parse([
        "backup".to_string(),
        "create".to_string(),
        "/tmp/neo4r-backup".to_string(),
    ])
    .unwrap();
    assert_eq!(backup.backup_path, Some("/tmp/neo4r-backup".to_string()));
}

#[test]
fn rejects_restore_without_confirmation() {
    let err = CliArgs::parse(["--restore".to_string(), "/tmp/neo4r-backup".to_string()])
        .unwrap_err()
        .to_string();

    assert!(err.contains("--restore requires --restore-confirm RESTORE"));
}

#[test]
fn trims_query_terminator() {
    assert_eq!(
        trim_query_terminator("MATCH (n) RETURN n;\n"),
        "MATCH (n) RETURN n"
    );
}

#[test]
fn keeps_history_deduplicated_in_execution_order() {
    let path = std::env::temp_dir().join(format!("neo4r-cli-history-{}", std::process::id()));
    let _ = fs::remove_file(&path);

    remember_query(true, &path, "MATCH (n) RETURN n").unwrap();
    remember_query(true, &path, "MATCH (m) RETURN m").unwrap();
    remember_query(true, &path, "MATCH (n) RETURN n").unwrap();

    assert_eq!(
        read_history(&path),
        vec![
            "MATCH (m) RETURN m".to_string(),
            "MATCH (n) RETURN n".to_string()
        ]
    );
    let _ = fs::remove_file(path);
}

#[test]
fn parses_transaction_begin_response() {
    assert_eq!(
        parse_tx_id("OK\tTX_BEGIN\t42\tREAD_ONLY\tSNAPSHOT").unwrap(),
        42
    );
    assert!(parse_tx_id("OK").is_err());
}
