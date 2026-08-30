use super::*;

#[test]
fn loads_server_args_from_yaml_config_file() {
    let path = temp_file("server-config-yaml").with_extension("yml");
    fs::write(
        &path,
        r#"
server:
  bind: 127.0.0.1:9100
  data_dir: /tmp/neo4r-yaml
  server_id: 2
  primary_server_id: 1
database:
  shards: 4
  partitions: 2
replication:
  bind: 127.0.0.1:9702
  transport: tcp
  ack: quorum
  connect_timeout_ms: 750
  retry_attempts: 3
  retry_backoff_ms: 25
  peers:
    - server_id: 3
      address: 127.0.0.1:9703
      role: replica
    - server_id: 4
      address: 127.0.0.1:9704
  catch_up_on_startup: true
query:
  read_preference: prefer-replica
  peers:
    - server_id: 5
      address: 127.0.0.1:7689
gossip:
  advertise_query: 10.0.0.2:7687
  advertise_replication: 10.0.0.2:9702
  interval_ms: 500
  ttl_ms: 2000
  fanout: 2
  auth_token: gossip-secret-token
  auto_negotiate_replication: true
  seed_peers:
    - server_id: 6
      address: 127.0.0.1:7686
web:
  bind: 127.0.0.1:7474
  auth_token: secret
maintenance:
  sync_index_catalog_on_startup: true
  recover_transactions_on_startup: true
production:
  tls_mode: external
  replication_tls_mode: required
  web_tls_mode: required
  native_tls_cert: /etc/neo4r/tls/server.crt
  native_tls_key: /etc/neo4r/tls/server.key
  native_tls_client_ca: /etc/neo4r/tls/ca.crt
  native_tls_require_client_auth: true
  web_tls_cert: /etc/neo4r/tls/web.crt
  web_tls_key: /etc/neo4r/tls/web.key
  web_tls_client_ca: /etc/neo4r/tls/ca.crt
  web_tls_require_client_auth: true
  replication_tls_cert: /etc/neo4r/tls/replication.crt
  replication_tls_key: /etc/neo4r/tls/replication.key
  replication_tls_client_ca: /etc/neo4r/tls/ca.crt
  replication_tls_require_client_auth: true
  replication_tls_ca: /etc/neo4r/tls/ca.crt
  replication_tls_server_name: neo4r-replication.local
  replication_tls_client_cert: /etc/neo4r/tls/client.crt
  replication_tls_client_key: /etc/neo4r/tls/client.key
  min_native_protocol_version: 1
  max_native_protocol_version: 1
  backup_drill_max_age_hours: 24
  wal_archive_dir: /var/lib/neo4r/wal-archive
  restore_drill_manifest: docs/pitr_restore_drill.yml
  audit_retention_days: 90
  secret_rotation_days: 30
  tenant_max_concurrent_queries: 128
  tenant_max_result_rows: 100000
  data_format_version: 1
  upgrade_manifest: docs/rolling_upgrade_manifest.yml
  raft_lease_clock_drift_bound_ms: 50
  raft_lease_message_delay_bound_ms: 200
  observability_alerts: docs/prometheus_alerts.yml
  repair_check_on_startup: true
  query_regression_corpus: docs/query_regression_corpus.yml
  chaos_gate_required: true
  runbook: docs/production_runbook.md
  systemd_unit: packaging/neo4r-server.service
  logrotate: packaging/neo4r.logrotate
"#,
    )
    .unwrap();

    let args = ServerArgs::parse(["--config".to_string(), path.display().to_string()]).unwrap();

    assert_eq!(args.bind_addr, "127.0.0.1:9100");
    assert_eq!(args.data_dir, PathBuf::from("/tmp/neo4r-yaml"));
    assert_eq!(args.shard_count, 4);
    assert_eq!(args.partition_count, 2);
    assert_eq!(args.server_id, 2);
    assert_eq!(args.primary_server_id, 1);
    assert_eq!(
        args.replication_bind_addr,
        Some("127.0.0.1:9702".to_string())
    );
    assert_eq!(args.replication_transport, ReplicationChannelKind::Tcp);
    assert_eq!(args.replication_ack_policy, ReplicationAckPolicy::Quorum);
    assert_eq!(args.replication_connect_timeout_ms, 750);
    assert_eq!(args.replication_retry_attempts, 3);
    assert_eq!(args.replication_retry_backoff_ms, 25);
    assert_eq!(
        args.replica_peers,
        vec![ReplicaPeer {
            server_id: 3,
            address: "127.0.0.1:9703".to_string(),
        }]
    );
    assert_eq!(
        args.peers,
        vec![ReplicaPeer {
            server_id: 4,
            address: "127.0.0.1:9704".to_string(),
        }]
    );
    assert_eq!(
        args.query_peers,
        vec![ReplicaPeer {
            server_id: 5,
            address: "127.0.0.1:7689".to_string(),
        }]
    );
    assert_eq!(
        args.gossip_advertise_query_addr.as_deref(),
        Some("10.0.0.2:7687")
    );
    assert_eq!(
        args.gossip_advertise_replication_addr.as_deref(),
        Some("10.0.0.2:9702")
    );
    assert_eq!(args.gossip_interval_ms, Some(500));
    assert_eq!(args.gossip_ttl_ms, 2000);
    assert_eq!(args.gossip_fanout, 2);
    assert_eq!(
        args.gossip_auth_token.as_deref(),
        Some("gossip-secret-token")
    );
    assert!(args.gossip_auto_negotiate_replication);
    assert_eq!(
        args.gossip_seed_peers,
        vec![ReplicaPeer {
            server_id: 6,
            address: "127.0.0.1:7686".to_string(),
        }]
    );
    assert_eq!(args.read_preference, QueryReadPreference::PreferReplica);
    assert_eq!(args.web_bind_addr, Some("127.0.0.1:7474".to_string()));
    assert_eq!(args.web_auth_token, Some("secret".to_string()));
    assert!(args.catch_up_on_startup);
    assert!(args.sync_index_catalog_on_startup);
    assert!(args.recover_transactions_on_startup);
    assert_eq!(args.tls_mode, ProductionSecurityMode::External);
    assert_eq!(args.replication_tls_mode, ProductionSecurityMode::Required);
    assert_eq!(args.web_tls_mode, ProductionSecurityMode::Required);
    assert_eq!(
        args.native_tls_cert_path,
        Some(PathBuf::from("/etc/neo4r/tls/server.crt"))
    );
    assert_eq!(
        args.native_tls_key_path,
        Some(PathBuf::from("/etc/neo4r/tls/server.key"))
    );
    assert_eq!(
        args.native_tls_client_ca_path,
        Some(PathBuf::from("/etc/neo4r/tls/ca.crt"))
    );
    assert!(args.native_tls_require_client_auth);
    assert_eq!(
        args.web_tls_cert_path,
        Some(PathBuf::from("/etc/neo4r/tls/web.crt"))
    );
    assert_eq!(
        args.web_tls_key_path,
        Some(PathBuf::from("/etc/neo4r/tls/web.key"))
    );
    assert_eq!(
        args.web_tls_client_ca_path,
        Some(PathBuf::from("/etc/neo4r/tls/ca.crt"))
    );
    assert!(args.web_tls_require_client_auth);
    assert_eq!(
        args.replication_tls_cert_path,
        Some(PathBuf::from("/etc/neo4r/tls/replication.crt"))
    );
    assert_eq!(
        args.replication_tls_key_path,
        Some(PathBuf::from("/etc/neo4r/tls/replication.key"))
    );
    assert_eq!(
        args.replication_tls_client_ca_path,
        Some(PathBuf::from("/etc/neo4r/tls/ca.crt"))
    );
    assert!(args.replication_tls_require_client_auth);
    assert_eq!(
        args.replication_tls_ca_path,
        Some(PathBuf::from("/etc/neo4r/tls/ca.crt"))
    );
    assert_eq!(
        args.replication_tls_server_name,
        Some("neo4r-replication.local".to_string())
    );
    assert_eq!(
        args.replication_tls_client_cert_path,
        Some(PathBuf::from("/etc/neo4r/tls/client.crt"))
    );
    assert_eq!(
        args.replication_tls_client_key_path,
        Some(PathBuf::from("/etc/neo4r/tls/client.key"))
    );
    assert_eq!(args.min_native_protocol_version, Some(1));
    assert_eq!(args.max_native_protocol_version, Some(1));
    assert_eq!(args.backup_drill_max_age_hours, Some(24));
    assert_eq!(
        args.wal_archive_dir,
        Some(PathBuf::from("/var/lib/neo4r/wal-archive"))
    );
    assert_eq!(
        args.restore_drill_manifest_path,
        Some(PathBuf::from("docs/pitr_restore_drill.yml"))
    );
    assert_eq!(args.audit_retention_days, Some(90));
    assert_eq!(args.secret_rotation_days, Some(30));
    assert_eq!(args.tenant_max_concurrent_queries, Some(128));
    assert_eq!(args.tenant_max_result_rows, Some(100000));
    assert_eq!(args.data_format_version, Some(1));
    assert_eq!(
        args.upgrade_manifest_path,
        Some(PathBuf::from("docs/rolling_upgrade_manifest.yml"))
    );
    assert_eq!(args.raft_lease_clock_drift_bound_ms, Some(50));
    assert_eq!(args.raft_lease_message_delay_bound_ms, Some(200));
    assert_eq!(
        args.observability_alerts_path,
        Some(PathBuf::from("docs/prometheus_alerts.yml"))
    );
    assert!(args.repair_check_on_startup);
    assert_eq!(
        args.query_regression_corpus_path,
        Some(PathBuf::from("docs/query_regression_corpus.yml"))
    );
    assert!(args.chaos_gate_required);
    assert_eq!(
        args.runbook_path,
        Some(PathBuf::from("docs/production_runbook.md"))
    );
    assert_eq!(
        args.systemd_unit_path,
        Some(PathBuf::from("packaging/neo4r-server.service"))
    );
    assert_eq!(
        args.logrotate_path,
        Some(PathBuf::from("packaging/neo4r.logrotate"))
    );

    let _ = fs::remove_file(path);
}
