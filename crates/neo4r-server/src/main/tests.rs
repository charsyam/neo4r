use super::*;
use std::fs;
use std::time::{SystemTime, UNIX_EPOCH};

#[test]
fn parses_server_args() {
    let args = ServerArgs::parse([
        "--bind".to_string(),
        "127.0.0.1:9000".to_string(),
        "--data-dir".to_string(),
        "/tmp/neo4r".to_string(),
        "--shards".to_string(),
        "8".to_string(),
        "--partitions".to_string(),
        "2".to_string(),
        "--server-id".to_string(),
        "10".to_string(),
        "--workers".to_string(),
        "3".to_string(),
        "--queue-capacity".to_string(),
        "32".to_string(),
        "--page-size".to_string(),
        "16".to_string(),
        "--read-preference".to_string(),
        "prefer-replica".to_string(),
        "--primary-server-id".to_string(),
        "10".to_string(),
        "--routing-table".to_string(),
        "/tmp/neo4r-routing.txt".to_string(),
        "--replica-peer".to_string(),
        "11=127.0.0.1:9701".to_string(),
        "--peer".to_string(),
        "10=127.0.0.1:9700".to_string(),
        "--query-peer".to_string(),
        "12=127.0.0.1:7688".to_string(),
        "--replication-bind".to_string(),
        "127.0.0.1:9700".to_string(),
        "--replication-transport".to_string(),
        "rdma".to_string(),
        "--web-bind".to_string(),
        "127.0.0.1:7474".to_string(),
        "--web-auth-token".to_string(),
        "secret".to_string(),
        "--slow-query-threshold-ms".to_string(),
        "50".to_string(),
        "--replication-ack".to_string(),
        "quorum".to_string(),
        "--replication-connect-timeout-ms".to_string(),
        "750".to_string(),
        "--replication-retry-attempts".to_string(),
        "3".to_string(),
        "--replication-retry-backoff-ms".to_string(),
        "25".to_string(),
        "--catch-up-on-startup".to_string(),
        "--catch-up-interval-ms".to_string(),
        "1000".to_string(),
        "--catch-up-batch-size".to_string(),
        "128".to_string(),
        "--sync-index-catalog-on-startup".to_string(),
        "--sync-index-catalog-interval-ms".to_string(),
        "2000".to_string(),
        "--recover-transactions-on-startup".to_string(),
        "--recover-transactions-interval-ms".to_string(),
        "3000".to_string(),
        "--daemonize".to_string(),
    ])
    .unwrap();

    assert_eq!(args.bind_addr, "127.0.0.1:9000");
    assert_eq!(args.data_dir, PathBuf::from("/tmp/neo4r"));
    assert_eq!(args.shard_count, 8);
    assert_eq!(args.web_auth_token, Some("secret".to_string()));
    assert_eq!(args.slow_query_threshold_ms, 50);
    assert_eq!(args.partition_count, 2);
    assert_eq!(args.server_id, 10);
    assert_eq!(args.worker_count, 3);
    assert_eq!(args.queue_capacity, 32);
    assert_eq!(args.default_page_size, 16);
    assert_eq!(args.read_preference, QueryReadPreference::PreferReplica);
    assert_eq!(args.primary_server_id, 10);
    assert_eq!(
        args.routing_table_path,
        Some(PathBuf::from("/tmp/neo4r-routing.txt"))
    );
    assert_eq!(
        args.replica_peers,
        vec![ReplicaPeer {
            server_id: 11,
            address: "127.0.0.1:9701".to_string(),
        }]
    );
    assert_eq!(
        args.peers,
        vec![ReplicaPeer {
            server_id: 10,
            address: "127.0.0.1:9700".to_string(),
        }]
    );
    assert_eq!(
        args.query_peers,
        vec![ReplicaPeer {
            server_id: 12,
            address: "127.0.0.1:7688".to_string(),
        }]
    );
    assert_eq!(
        args.replication_bind_addr,
        Some("127.0.0.1:9700".to_string())
    );
    assert_eq!(args.replication_transport, ReplicationChannelKind::Rdma);
    assert_eq!(args.web_bind_addr, Some("127.0.0.1:7474".to_string()));
    assert_eq!(args.replication_ack_policy, ReplicationAckPolicy::Quorum);
    assert_eq!(args.replication_connect_timeout_ms, 750);
    assert_eq!(args.replication_retry_attempts, 3);
    assert_eq!(args.replication_retry_backoff_ms, 25);
    assert!(args.catch_up_on_startup);
    assert_eq!(args.catch_up_interval_ms, Some(1000));
    assert_eq!(args.catch_up_batch_size, Some(128));
    assert!(args.sync_index_catalog_on_startup);
    assert_eq!(args.sync_index_catalog_interval_ms, Some(2000));
    assert!(args.recover_transactions_on_startup);
    assert_eq!(args.recover_transactions_interval_ms, Some(3000));
    assert!(args.daemonize);
}

#[test]
fn defaults_replication_transport_to_tcp() {
    let args = ServerArgs::parse([]).unwrap();

    assert_eq!(args.replication_transport, ReplicationChannelKind::Tcp);
}

#[test]
fn parses_config_validation_flags_and_dump_summary() {
    let args = ServerArgs::parse([
        "--check-config".to_string(),
        "--bind".to_string(),
        "127.0.0.1:9001".to_string(),
    ])
    .unwrap();

    assert!(args.check_config);
    assert!(!args.production_check);
    assert!(!args.dump_config);
    assert!(args.to_yaml_summary().contains("bind: 127.0.0.1:9001"));

    let err =
        ServerArgs::parse(["--check-config".to_string(), "--dump-config".to_string()]).unwrap_err();
    assert_eq!(
        err,
        "--check-config, --production-check, and --dump-config cannot be combined"
    );
}

#[test]
fn production_check_rejects_development_defaults() {
    let args = ServerArgs::parse(["--production-check".to_string()]).unwrap();
    let err = args.validate_production().unwrap_err();

    assert!(err.contains("--bind must not be loopback-only"));
    assert!(err.contains("--data-dir must be an absolute path"));
    assert!(err.contains("--web-bind is required"));
    assert!(err.contains("--web-auth-token must be set"));
}

#[test]
fn production_check_accepts_hardened_single_node_config() {
    let args = ServerArgs::parse([
        "--production-check".to_string(),
        "--bind".to_string(),
        "0.0.0.0:7687".to_string(),
        "--web-bind".to_string(),
        "0.0.0.0:17687".to_string(),
        "--web-auth-token".to_string(),
        "admin:long-production-token".to_string(),
        "--data-dir".to_string(),
        "/var/lib/neo4r".to_string(),
    ])
    .unwrap();

    args.validate_production().unwrap();
}

#[test]
fn production_check_requires_cluster_catchup_controls() {
    let args = ServerArgs::parse([
        "--production-check".to_string(),
        "--bind".to_string(),
        "0.0.0.0:7687".to_string(),
        "--web-bind".to_string(),
        "0.0.0.0:17687".to_string(),
        "--web-auth-token".to_string(),
        "admin:long-production-token".to_string(),
        "--data-dir".to_string(),
        "/var/lib/neo4r".to_string(),
        "--replication-bind".to_string(),
        "0.0.0.0:18687".to_string(),
        "--replica-peer".to_string(),
        "2=10.0.0.2:18687".to_string(),
    ])
    .unwrap();
    let err = args.validate_production().unwrap_err();

    assert!(err.contains("--catch-up-on-startup is required"));
    assert!(err.contains("--catch-up-interval-ms is required"));
    assert!(err.contains("--catch-up-batch-size is required"));
}

#[test]
fn key_value_config_supports_production_check_flag() {
    let path = temp_file("server-production-check-config");
    fs::write(&path, "production_check=true\n").unwrap();

    let args = ServerArgs::parse(["--config".to_string(), path.display().to_string()]).unwrap();

    assert!(args.production_check);
    let _ = fs::remove_file(path);
}

#[test]
fn production_check_rejects_async_replication_ack() {
    let args = ServerArgs::parse([
        "--production-check".to_string(),
        "--bind".to_string(),
        "0.0.0.0:7687".to_string(),
        "--web-bind".to_string(),
        "0.0.0.0:17687".to_string(),
        "--web-auth-token".to_string(),
        "admin:long-production-token".to_string(),
        "--data-dir".to_string(),
        "/var/lib/neo4r".to_string(),
        "--replication-ack".to_string(),
        "async".to_string(),
    ])
    .unwrap();

    assert!(args
        .validate_production()
        .unwrap_err()
        .contains("--replication-ack async"));
}

#[test]
fn loads_server_args_from_config_file_and_allows_cli_override() {
    let path = temp_file("server-config");
    fs::write(
        &path,
        [
            "bind=127.0.0.1:9100",
            "data_dir=/tmp/neo4r-config",
            "shards=4",
            "server_id=2",
            "primary_server_id=1",
            "replica_peer=3=127.0.0.1:9703",
            "peer=1=127.0.0.1:9701",
            "query_peer=4=127.0.0.1:7689",
            "replication_bind=127.0.0.1:9702",
            "replication_transport=rdma",
            "catch_up_on_startup=true",
            "recover_transactions_on_startup=yes",
        ]
        .join("\n"),
    )
    .unwrap();

    let args = ServerArgs::parse([
        "--config".to_string(),
        path.display().to_string(),
        "--bind".to_string(),
        "127.0.0.1:9999".to_string(),
        "--peer".to_string(),
        "5=127.0.0.1:9705".to_string(),
    ])
    .unwrap();

    assert_eq!(args.bind_addr, "127.0.0.1:9999");
    assert_eq!(args.data_dir, PathBuf::from("/tmp/neo4r-config"));
    assert_eq!(args.shard_count, 4);
    assert_eq!(args.server_id, 2);
    assert_eq!(args.primary_server_id, 1);
    assert_eq!(args.replication_transport, ReplicationChannelKind::Rdma);
    assert!(args.catch_up_on_startup);
    assert!(args.recover_transactions_on_startup);
    assert_eq!(
        args.peers,
        vec![
            ReplicaPeer {
                server_id: 1,
                address: "127.0.0.1:9701".to_string(),
            },
            ReplicaPeer {
                server_id: 5,
                address: "127.0.0.1:9705".to_string(),
            },
        ]
    );

    let _ = fs::remove_file(path);
}

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
  transport: rdma
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
web:
  bind: 127.0.0.1:7474
  auth_token: secret
maintenance:
  sync_index_catalog_on_startup: true
  recover_transactions_on_startup: true
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
    assert_eq!(args.replication_transport, ReplicationChannelKind::Rdma);
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
    assert_eq!(args.read_preference, QueryReadPreference::PreferReplica);
    assert_eq!(args.web_bind_addr, Some("127.0.0.1:7474".to_string()));
    assert_eq!(args.web_auth_token, Some("secret".to_string()));
    assert!(args.catch_up_on_startup);
    assert!(args.sync_index_catalog_on_startup);
    assert!(args.recover_transactions_on_startup);

    let _ = fs::remove_file(path);
}

#[test]
fn rejects_zero_catch_up_interval() {
    assert_eq!(
        ServerArgs::parse([
            "--server-id".to_string(),
            "2".to_string(),
            "--primary-server-id".to_string(),
            "1".to_string(),
            "--catch-up-interval-ms".to_string(),
            "0".to_string(),
        ])
        .unwrap_err(),
        "--catch-up-interval-ms must be greater than zero"
    );
}

#[test]
fn rejects_zero_replication_connect_timeout() {
    assert_eq!(
        ServerArgs::parse([
            "--replication-connect-timeout-ms".to_string(),
            "0".to_string(),
        ])
        .unwrap_err(),
        "--replication-connect-timeout-ms must be greater than zero"
    );
}

#[test]
fn rejects_zero_catch_up_batch_size() {
    assert_eq!(
        ServerArgs::parse([
            "--server-id".to_string(),
            "2".to_string(),
            "--primary-server-id".to_string(),
            "1".to_string(),
            "--catch-up-batch-size".to_string(),
            "0".to_string(),
        ])
        .unwrap_err(),
        "--catch-up-batch-size must be greater than zero"
    );
}

#[test]
fn rejects_zero_sync_index_catalog_interval() {
    assert_eq!(
        ServerArgs::parse([
            "--sync-index-catalog-interval-ms".to_string(),
            "0".to_string(),
        ])
        .unwrap_err(),
        "--sync-index-catalog-interval-ms must be greater than zero"
    );
}

#[test]
fn rejects_zero_recover_transactions_interval() {
    assert_eq!(
        ServerArgs::parse([
            "--recover-transactions-interval-ms".to_string(),
            "0".to_string(),
        ])
        .unwrap_err(),
        "--recover-transactions-interval-ms must be greater than zero"
    );
}

#[test]
fn daemon_child_args_preserve_replication_and_recovery_options() {
    let args = ServerArgs::parse([
        "--bind".to_string(),
        "127.0.0.1:9000".to_string(),
        "--data-dir".to_string(),
        "/tmp/neo4r".to_string(),
        "--server-id".to_string(),
        "2".to_string(),
        "--primary-server-id".to_string(),
        "1".to_string(),
        "--replica-peer".to_string(),
        "2=127.0.0.1:9702".to_string(),
        "--peer".to_string(),
        "1=127.0.0.1:9701".to_string(),
        "--query-peer".to_string(),
        "1=127.0.0.1:7687".to_string(),
        "--replication-bind".to_string(),
        "127.0.0.1:9702".to_string(),
        "--web-bind".to_string(),
        "127.0.0.1:7474".to_string(),
        "--web-auth-token".to_string(),
        "secret".to_string(),
        "--slow-query-threshold-ms".to_string(),
        "75".to_string(),
        "--replication-ack".to_string(),
        "quorum".to_string(),
        "--replication-connect-timeout-ms".to_string(),
        "750".to_string(),
        "--replication-retry-attempts".to_string(),
        "5".to_string(),
        "--replication-retry-backoff-ms".to_string(),
        "50".to_string(),
        "--catch-up-on-startup".to_string(),
        "--catch-up-interval-ms".to_string(),
        "250".to_string(),
        "--catch-up-batch-size".to_string(),
        "64".to_string(),
        "--sync-index-catalog-on-startup".to_string(),
        "--sync-index-catalog-interval-ms".to_string(),
        "500".to_string(),
        "--recover-transactions-on-startup".to_string(),
        "--recover-transactions-interval-ms".to_string(),
        "750".to_string(),
    ])
    .unwrap();

    let child_args = daemon_child_args(&args);

    assert!(child_args.contains(&"--replication-bind".to_string()));
    assert!(child_args.contains(&"127.0.0.1:9702".to_string()));
    assert!(child_args.contains(&"--web-bind".to_string()));
    assert!(child_args.contains(&"127.0.0.1:7474".to_string()));
    assert!(child_args.contains(&"--web-auth-token".to_string()));
    assert!(child_args.contains(&"secret".to_string()));
    assert!(child_args.contains(&"--slow-query-threshold-ms".to_string()));
    assert!(child_args.contains(&"75".to_string()));
    assert!(child_args.contains(&"--replication-ack".to_string()));
    assert!(child_args.contains(&"quorum".to_string()));
    assert!(child_args.contains(&"--replication-connect-timeout-ms".to_string()));
    assert!(child_args.contains(&"750".to_string()));
    assert!(child_args.contains(&"--replication-retry-attempts".to_string()));
    assert!(child_args.contains(&"5".to_string()));
    assert!(child_args.contains(&"--replication-retry-backoff-ms".to_string()));
    assert!(child_args.contains(&"50".to_string()));
    assert!(child_args.contains(&"--replica-peer".to_string()));
    assert!(child_args.contains(&"2=127.0.0.1:9702".to_string()));
    assert!(child_args.contains(&"--peer".to_string()));
    assert!(child_args.contains(&"1=127.0.0.1:9701".to_string()));
    assert!(child_args.contains(&"--query-peer".to_string()));
    assert!(child_args.contains(&"1=127.0.0.1:7687".to_string()));
    assert!(child_args.contains(&"--catch-up-on-startup".to_string()));
    assert!(child_args.contains(&"--catch-up-interval-ms".to_string()));
    assert!(child_args.contains(&"250".to_string()));
    assert!(child_args.contains(&"--catch-up-batch-size".to_string()));
    assert!(child_args.contains(&"64".to_string()));
    assert!(child_args.contains(&"--sync-index-catalog-on-startup".to_string()));
    assert!(child_args.contains(&"--sync-index-catalog-interval-ms".to_string()));
    assert!(child_args.contains(&"500".to_string()));
    assert!(child_args.contains(&"--recover-transactions-on-startup".to_string()));
    assert!(child_args.contains(&"--recover-transactions-interval-ms".to_string()));
    assert!(child_args.contains(&"750".to_string()));
    assert!(!child_args.contains(&"--daemonize".to_string()));
}

#[test]
fn builds_cluster_routing_table_from_replication_args() {
    let args = ServerArgs::parse([
        "--server-id".to_string(),
        "1".to_string(),
        "--primary-server-id".to_string(),
        "1".to_string(),
        "--shards".to_string(),
        "2".to_string(),
        "--replica-peer".to_string(),
        "2=127.0.0.1:9702".to_string(),
        "--peer".to_string(),
        "3=127.0.0.1:9703".to_string(),
    ])
    .unwrap();

    let table = args.routing_table().unwrap().unwrap();

    assert_eq!(table.version, 1);
    assert_eq!(table.placements.len(), 2);
    for placement in &table.placements {
        assert_eq!(
            placement.replicas,
            vec![ShardReplica::primary(1), ShardReplica::replica(2)]
        );
    }
    assert_eq!(metadata_primary_server_id(&table).unwrap(), 1);
}

#[test]
fn replica_node_routing_table_includes_local_replica() {
    let args = ServerArgs::parse([
        "--server-id".to_string(),
        "2".to_string(),
        "--primary-server-id".to_string(),
        "1".to_string(),
        "--shards".to_string(),
        "1".to_string(),
        "--replication-bind".to_string(),
        "127.0.0.1:9702".to_string(),
    ])
    .unwrap();

    let table = args.routing_table().unwrap().unwrap();

    assert_eq!(
        table.placements[0].replicas,
        vec![ShardReplica::primary(1), ShardReplica::replica(2)]
    );
}

#[test]
fn parses_explicit_routing_table_config() {
    let table = parse_routing_table_config(
        r#"
            # shard routing table
            version=7
            shard=0 primary=1 replicas=2,3
            shard=1 primary=2 replicas=1,3
            "#,
    )
    .unwrap();

    assert_eq!(table.version, 7);
    assert_eq!(
        table.placements,
        vec![
            ShardPlacement::new(
                0,
                vec![
                    ShardReplica::primary(1),
                    ShardReplica::replica(2),
                    ShardReplica::replica(3),
                ],
            ),
            ShardPlacement::new(
                1,
                vec![
                    ShardReplica::primary(2),
                    ShardReplica::replica(1),
                    ShardReplica::replica(3),
                ],
            ),
        ]
    );
}

#[test]
fn rejects_non_contiguous_routing_table_config() {
    let err = parse_routing_table_config(
        r#"
            version=7
            shard=1 primary=1 replicas=2
            "#,
    )
    .unwrap_err();

    assert!(err.contains("contiguous"));
}

#[test]
fn loads_routing_table_from_config_file() {
    let path = temp_file("neo4r-routing-config");
    fs::write(
        &path,
        r#"
            version=9
            shard=0 primary=2 replicas=1
            "#,
    )
    .unwrap();
    let args = ServerArgs::parse([
        "--server-id".to_string(),
        "1".to_string(),
        "--routing-table".to_string(),
        path.display().to_string(),
    ])
    .unwrap();

    let table = args.routing_table().unwrap().unwrap();

    assert_eq!(table.version, 9);
    assert_eq!(
        table.placements[0].replicas,
        vec![ShardReplica::primary(2), ShardReplica::replica(1)]
    );
    let _ = fs::remove_file(path);
}

fn temp_file(prefix: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "{}-{}-{}",
        prefix,
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}
