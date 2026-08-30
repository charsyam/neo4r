use super::*;

pub(in crate::runtime) fn spawn_daemon(args: &ServerArgs) -> io::Result<()> {
    let mut child_args = daemon_child_args(args);
    let dev_null = OpenOptions::new()
        .read(true)
        .write(true)
        .open("/dev/null")?;
    let child = Command::new(std::env::current_exe()?)
        .args(child_args.drain(..))
        .stdin(Stdio::from(dev_null.try_clone()?))
        .stdout(Stdio::from(dev_null.try_clone()?))
        .stderr(Stdio::from(dev_null))
        .spawn()?;
    println!("{}", child.id());
    Ok(())
}

pub(in crate::runtime) fn daemon_child_args(args: &ServerArgs) -> Vec<String> {
    let mut child_args = vec![
        "--bind".to_string(),
        args.bind_addr.clone(),
        "--data-dir".to_string(),
        args.data_dir.display().to_string(),
        "--shards".to_string(),
        args.shard_count.to_string(),
        "--partitions".to_string(),
        args.partition_count.to_string(),
        "--server-id".to_string(),
        args.server_id.to_string(),
        "--workers".to_string(),
        args.worker_count.to_string(),
        "--queue-capacity".to_string(),
        args.queue_capacity.to_string(),
        "--page-size".to_string(),
        args.default_page_size.to_string(),
        "--read-preference".to_string(),
        format_read_preference(args.read_preference).to_string(),
        "--primary-server-id".to_string(),
        args.primary_server_id.to_string(),
        "--replication-ack".to_string(),
        format_ack_policy(args.replication_ack_policy).to_string(),
        "--replication-transport".to_string(),
        format_replication_transport(args.replication_transport).to_string(),
        "--replication-retry-attempts".to_string(),
        args.replication_retry_attempts.to_string(),
        "--replication-retry-backoff-ms".to_string(),
        args.replication_retry_backoff_ms.to_string(),
        "--replication-connect-timeout-ms".to_string(),
        args.replication_connect_timeout_ms.to_string(),
        "--replication-max-in-flight-batches".to_string(),
        args.replication_max_in_flight_batches.to_string(),
    ];
    if let Some(addr) = &args.replication_bind_addr {
        child_args.push("--replication-bind".to_string());
        child_args.push(addr.clone());
    }
    if let Some(addr) = &args.web_bind_addr {
        child_args.push("--web-bind".to_string());
        child_args.push(addr.clone());
    }
    if let Some(token) = &args.web_auth_token {
        child_args.push("--web-auth-token".to_string());
        child_args.push(token.clone());
    }
    child_args.push("--slow-query-threshold-ms".to_string());
    child_args.push(args.slow_query_threshold_ms.to_string());
    if let Some(path) = &args.routing_table_path {
        child_args.push("--routing-table".to_string());
        child_args.push(path.display().to_string());
    }
    for peer in &args.replica_peers {
        child_args.push("--replica-peer".to_string());
        child_args.push(format!("{}={}", peer.server_id, peer.address));
    }
    for peer in &args.peers {
        child_args.push("--peer".to_string());
        child_args.push(format!("{}={}", peer.server_id, peer.address));
    }
    for peer in &args.query_peers {
        child_args.push("--query-peer".to_string());
        child_args.push(format!("{}={}", peer.server_id, peer.address));
    }
    for peer in &args.gossip_seed_peers {
        child_args.push("--gossip-seed-peer".to_string());
        child_args.push(format!("{}={}", peer.server_id, peer.address));
    }
    if let Some(addr) = &args.gossip_advertise_query_addr {
        child_args.push("--gossip-advertise-query".to_string());
        child_args.push(addr.clone());
    }
    if let Some(addr) = &args.gossip_advertise_replication_addr {
        child_args.push("--gossip-advertise-replication".to_string());
        child_args.push(addr.clone());
    }
    if let Some(interval_ms) = args.gossip_interval_ms {
        child_args.push("--gossip-interval-ms".to_string());
        child_args.push(interval_ms.to_string());
    }
    child_args.push("--gossip-ttl-ms".to_string());
    child_args.push(args.gossip_ttl_ms.to_string());
    child_args.push("--gossip-fanout".to_string());
    child_args.push(args.gossip_fanout.to_string());
    if let Some(token) = &args.gossip_auth_token {
        child_args.push("--gossip-auth-token".to_string());
        child_args.push(token.clone());
    }
    if args.gossip_auto_negotiate_replication {
        child_args.push("--gossip-auto-negotiate-replication".to_string());
    }
    if args.catch_up_on_startup {
        child_args.push("--catch-up-on-startup".to_string());
    }
    if let Some(interval_ms) = args.catch_up_interval_ms {
        child_args.push("--catch-up-interval-ms".to_string());
        child_args.push(interval_ms.to_string());
    }
    if let Some(batch_size) = args.catch_up_batch_size {
        child_args.push("--catch-up-batch-size".to_string());
        child_args.push(batch_size.to_string());
    }
    if args.sync_index_catalog_on_startup {
        child_args.push("--sync-index-catalog-on-startup".to_string());
    }
    if let Some(interval_ms) = args.sync_index_catalog_interval_ms {
        child_args.push("--sync-index-catalog-interval-ms".to_string());
        child_args.push(interval_ms.to_string());
    }
    if args.recover_transactions_on_startup {
        child_args.push("--recover-transactions-on-startup".to_string());
    }
    if let Some(interval_ms) = args.recover_transactions_interval_ms {
        child_args.push("--recover-transactions-interval-ms".to_string());
        child_args.push(interval_ms.to_string());
    }
    child_args.push("--tls-mode".to_string());
    child_args.push(format_production_security_mode(args.tls_mode).to_string());
    child_args.push("--replication-tls-mode".to_string());
    child_args.push(format_production_security_mode(args.replication_tls_mode).to_string());
    child_args.push("--web-tls-mode".to_string());
    child_args.push(format_production_security_mode(args.web_tls_mode).to_string());
    if let Some(path) = &args.native_tls_cert_path {
        child_args.push("--native-tls-cert".to_string());
        child_args.push(path.display().to_string());
    }
    if let Some(path) = &args.native_tls_key_path {
        child_args.push("--native-tls-key".to_string());
        child_args.push(path.display().to_string());
    }
    if let Some(path) = &args.native_tls_client_ca_path {
        child_args.push("--native-tls-client-ca".to_string());
        child_args.push(path.display().to_string());
    }
    if args.native_tls_require_client_auth {
        child_args.push("--native-tls-require-client-auth".to_string());
    }
    if let Some(path) = &args.web_tls_cert_path {
        child_args.push("--web-tls-cert".to_string());
        child_args.push(path.display().to_string());
    }
    if let Some(path) = &args.web_tls_key_path {
        child_args.push("--web-tls-key".to_string());
        child_args.push(path.display().to_string());
    }
    if let Some(path) = &args.web_tls_client_ca_path {
        child_args.push("--web-tls-client-ca".to_string());
        child_args.push(path.display().to_string());
    }
    if args.web_tls_require_client_auth {
        child_args.push("--web-tls-require-client-auth".to_string());
    }
    if let Some(path) = &args.replication_tls_cert_path {
        child_args.push("--replication-tls-cert".to_string());
        child_args.push(path.display().to_string());
    }
    if let Some(path) = &args.replication_tls_key_path {
        child_args.push("--replication-tls-key".to_string());
        child_args.push(path.display().to_string());
    }
    if let Some(path) = &args.replication_tls_client_ca_path {
        child_args.push("--replication-tls-client-ca".to_string());
        child_args.push(path.display().to_string());
    }
    if args.replication_tls_require_client_auth {
        child_args.push("--replication-tls-require-client-auth".to_string());
    }
    if let Some(path) = &args.replication_tls_ca_path {
        child_args.push("--replication-tls-ca".to_string());
        child_args.push(path.display().to_string());
    }
    if let Some(server_name) = &args.replication_tls_server_name {
        child_args.push("--replication-tls-server-name".to_string());
        child_args.push(server_name.clone());
    }
    if let Some(path) = &args.replication_tls_client_cert_path {
        child_args.push("--replication-tls-client-cert".to_string());
        child_args.push(path.display().to_string());
    }
    if let Some(path) = &args.replication_tls_client_key_path {
        child_args.push("--replication-tls-client-key".to_string());
        child_args.push(path.display().to_string());
    }
    if let Some(version) = args.min_native_protocol_version {
        child_args.push("--min-native-protocol-version".to_string());
        child_args.push(version.to_string());
    }
    if let Some(version) = args.max_native_protocol_version {
        child_args.push("--max-native-protocol-version".to_string());
        child_args.push(version.to_string());
    }
    if let Some(hours) = args.backup_drill_max_age_hours {
        child_args.push("--backup-drill-max-age-hours".to_string());
        child_args.push(hours.to_string());
    }
    if let Some(path) = &args.wal_archive_dir {
        child_args.push("--wal-archive-dir".to_string());
        child_args.push(path.display().to_string());
    }
    if let Some(path) = &args.restore_drill_manifest_path {
        child_args.push("--restore-drill-manifest".to_string());
        child_args.push(path.display().to_string());
    }
    if let Some(days) = args.audit_retention_days {
        child_args.push("--audit-retention-days".to_string());
        child_args.push(days.to_string());
    }
    if let Some(days) = args.secret_rotation_days {
        child_args.push("--secret-rotation-days".to_string());
        child_args.push(days.to_string());
    }
    if let Some(limit) = args.tenant_max_concurrent_queries {
        child_args.push("--tenant-max-concurrent-queries".to_string());
        child_args.push(limit.to_string());
    }
    if let Some(limit) = args.tenant_max_result_rows {
        child_args.push("--tenant-max-result-rows".to_string());
        child_args.push(limit.to_string());
    }
    if let Some(version) = args.data_format_version {
        child_args.push("--data-format-version".to_string());
        child_args.push(version.to_string());
    }
    if let Some(path) = &args.upgrade_manifest_path {
        child_args.push("--upgrade-manifest".to_string());
        child_args.push(path.display().to_string());
    }
    if let Some(ms) = args.raft_lease_clock_drift_bound_ms {
        child_args.push("--raft-lease-clock-drift-bound-ms".to_string());
        child_args.push(ms.to_string());
    }
    if let Some(ms) = args.raft_lease_message_delay_bound_ms {
        child_args.push("--raft-lease-message-delay-bound-ms".to_string());
        child_args.push(ms.to_string());
    }
    if let Some(path) = &args.observability_alerts_path {
        child_args.push("--observability-alerts".to_string());
        child_args.push(path.display().to_string());
    }
    if args.repair_check_on_startup {
        child_args.push("--repair-check-on-startup".to_string());
    }
    if let Some(path) = &args.query_regression_corpus_path {
        child_args.push("--query-regression-corpus".to_string());
        child_args.push(path.display().to_string());
    }
    if args.chaos_gate_required {
        child_args.push("--chaos-gate-required".to_string());
    }
    if let Some(path) = &args.runbook_path {
        child_args.push("--runbook".to_string());
        child_args.push(path.display().to_string());
    }
    if let Some(path) = &args.systemd_unit_path {
        child_args.push("--systemd-unit".to_string());
        child_args.push(path.display().to_string());
    }
    if let Some(path) = &args.logrotate_path {
        child_args.push("--logrotate".to_string());
        child_args.push(path.display().to_string());
    }
    child_args
}

pub(in crate::runtime) fn load_routing_table_config(
    path: &PathBuf,
) -> Result<ShardRoutingTable, String> {
    parse_routing_table_config(
        &fs::read_to_string(path)
            .map_err(|err| format!("failed to read routing table {}: {err}", path.display()))?,
    )
}

pub(in crate::runtime) fn parse_routing_table_config(
    input: &str,
) -> Result<ShardRoutingTable, String> {
    let mut version = None;
    let mut placements = Vec::new();
    for (line_no, raw_line) in input.lines().enumerate() {
        let line = raw_line.split('#').next().unwrap_or("").trim();
        if line.is_empty() {
            continue;
        }
        if let Some(raw_version) = line.strip_prefix("version=") {
            version = Some(parse_config_u64(raw_version, "version", line_no)?);
            continue;
        }
        let mut shard_id = None;
        let mut primary = None;
        let mut replicas = Vec::new();
        for token in line.split_whitespace() {
            let (key, value) = token
                .split_once('=')
                .ok_or_else(|| format!("routing line {} token must be key=value", line_no + 1))?;
            match key {
                "shard" => shard_id = Some(parse_config_u64(value, "shard", line_no)?),
                "primary" => primary = Some(parse_config_u64(value, "primary", line_no)?),
                "replicas" => {
                    if !value.is_empty() {
                        replicas = value
                            .split(',')
                            .filter(|value| !value.is_empty())
                            .map(|value| parse_config_u64(value, "replica", line_no))
                            .collect::<Result<Vec<_>, _>>()?;
                    }
                }
                _ => {
                    return Err(format!(
                        "routing line {} has unknown key {key:?}",
                        line_no + 1
                    ))
                }
            }
        }
        let shard_id =
            shard_id.ok_or_else(|| format!("routing line {} missing shard", line_no + 1))?;
        let primary =
            primary.ok_or_else(|| format!("routing line {} missing primary", line_no + 1))?;
        if primary == 0 {
            return Err(format!(
                "routing line {} primary must be greater than zero",
                line_no + 1
            ));
        }
        let mut shard_replicas = vec![ShardReplica::primary(primary)];
        for replica in replicas {
            if replica == 0 {
                return Err(format!(
                    "routing line {} replica must be greater than zero",
                    line_no + 1
                ));
            }
            if replica != primary && !shard_replicas.iter().any(|item| item.server_id == replica) {
                shard_replicas.push(ShardReplica::replica(replica));
            }
        }
        placements.push(ShardPlacement::new(shard_id, shard_replicas));
    }
    let version = version.ok_or_else(|| "routing table config missing version".to_string())?;
    if version == 0 {
        return Err("routing table version must be greater than zero".to_string());
    }
    placements.sort_by_key(|placement| placement.shard_id);
    if placements.is_empty() {
        return Err("routing table config must contain at least one shard".to_string());
    }
    for (expected, placement) in placements.iter().enumerate() {
        if placement.shard_id != expected as u64 {
            return Err(format!(
                "routing table shards must be contiguous from 0; expected {expected}, got {}",
                placement.shard_id
            ));
        }
        let primary_count = placement
            .replicas
            .iter()
            .filter(|replica| replica.role == ShardRole::Primary)
            .count();
        if primary_count != 1 {
            return Err(format!(
                "routing shard {} must have exactly one primary",
                placement.shard_id
            ));
        }
    }
    Ok(ShardRoutingTable {
        version,
        placements,
    })
}

pub(in crate::runtime) fn metadata_primary_server_id(
    routing_table: &ShardRoutingTable,
) -> Result<u64, String> {
    routing_table
        .placements
        .iter()
        .find(|placement| placement.shard_id == 0)
        .and_then(|placement| {
            placement
                .replicas
                .iter()
                .find(|replica| replica.role == ShardRole::Primary)
                .map(|replica| replica.server_id)
        })
        .ok_or_else(|| "routing table missing shard 0 primary".to_string())
}

pub(in crate::runtime) fn parse_config_u64(
    value: &str,
    field: &str,
    line_no: usize,
) -> Result<u64, String> {
    value
        .parse::<u64>()
        .map_err(|_| format!("routing line {} has invalid {field}", line_no + 1))
}

pub(in crate::runtime) fn parse_replica_peer(value: &str) -> Result<ReplicaPeer, String> {
    parse_peer(value, "--replica-peer")
}

pub(in crate::runtime) fn parse_peer(
    value: &str,
    option_name: &str,
) -> Result<ReplicaPeer, String> {
    let (server_id, address) = value
        .split_once('=')
        .ok_or_else(|| format!("{option_name} must be SERVER_ID=ADDR"))?;
    let server_id = server_id
        .parse::<u64>()
        .map_err(|_| format!("{option_name} server id has an invalid value"))?;
    if server_id == 0 {
        return Err(format!("{option_name} server id must be greater than zero"));
    }
    if address.is_empty() {
        return Err(format!("{option_name} address cannot be empty"));
    }
    Ok(ReplicaPeer {
        server_id,
        address: address.to_string(),
    })
}

pub(in crate::runtime) fn parse_ack_policy(value: &str) -> Result<ReplicationAckPolicy, String> {
    match value {
        "all" | "ALL" => Ok(ReplicationAckPolicy::All),
        "quorum" | "QUORUM" => Ok(ReplicationAckPolicy::Quorum),
        "async" | "ASYNC" => Ok(ReplicationAckPolicy::Async),
        _ => Err("--replication-ack must be all, quorum, or async".to_string()),
    }
}

pub(in crate::runtime) fn parse_replication_transport(
    value: &str,
) -> Result<ReplicationChannelKind, String> {
    match value.to_ascii_lowercase().as_str() {
        "tcp" => Ok(ReplicationChannelKind::Tcp),
        "rdma" => Ok(ReplicationChannelKind::Rdma),
        "udp" => Err("udp is not supported for raft replication transport".to_string()),
        "custom" => {
            Err("custom replication transport requires explicit provider wiring".to_string())
        }
        _ => Err("--replication-transport must be tcp or rdma".to_string()),
    }
}

pub(in crate::runtime) fn parse_production_security_mode(
    value: &str,
) -> Result<ProductionSecurityMode, String> {
    match value.to_ascii_lowercase().as_str() {
        "disabled" => Ok(ProductionSecurityMode::Disabled),
        "external" => Ok(ProductionSecurityMode::External),
        "required" => Ok(ProductionSecurityMode::Required),
        _ => Err("security mode must be disabled, external, or required".to_string()),
    }
}

pub(in crate::runtime) fn format_production_security_mode(
    mode: ProductionSecurityMode,
) -> &'static str {
    match mode {
        ProductionSecurityMode::Disabled => "disabled",
        ProductionSecurityMode::External => "external",
        ProductionSecurityMode::Required => "required",
    }
}

pub(in crate::runtime) fn format_replication_transport(
    transport: ReplicationChannelKind,
) -> &'static str {
    match transport {
        ReplicationChannelKind::Tcp => "tcp",
        ReplicationChannelKind::Rdma => "rdma",
        ReplicationChannelKind::Udp => "udp",
        ReplicationChannelKind::Custom => "custom",
    }
}

pub(in crate::runtime) fn replication_endpoint(
    address: String,
    transport: ReplicationChannelKind,
) -> Result<ReplicationEndpoint, String> {
    match transport {
        ReplicationChannelKind::Tcp => Ok(ReplicationEndpoint::tcp(address)),
        #[cfg(feature = "rdma")]
        ReplicationChannelKind::Rdma => Ok(ReplicationEndpoint::rdma(address)),
        #[cfg(not(feature = "rdma"))]
        ReplicationChannelKind::Rdma => {
            Err("--replication-transport rdma requires --features rdma".to_string())
        }
        ReplicationChannelKind::Udp => {
            Err("udp is not supported for raft replication transport".to_string())
        }
        ReplicationChannelKind::Custom => {
            Err("custom replication transport requires explicit provider wiring".to_string())
        }
    }
}

pub(in crate::runtime) fn replication_channel(
    transport: ReplicationChannelKind,
    tls_config: Option<ReplicationTlsConfig>,
) -> Result<Arc<dyn ReplicationChannel>, String> {
    match transport {
        ReplicationChannelKind::Tcp => Ok(match tls_config {
            Some(config) => {
                Arc::new(TlsReplicationChannel::new(config)) as Arc<dyn ReplicationChannel>
            }
            None => Arc::new(TcpReplicationChannel) as Arc<dyn ReplicationChannel>,
        }),
        #[cfg(feature = "rdma")]
        ReplicationChannelKind::Rdma => Ok(Arc::new(RdmaReplicationChannel::default())),
        #[cfg(not(feature = "rdma"))]
        ReplicationChannelKind::Rdma => {
            Err("--replication-transport rdma requires --features rdma".to_string())
        }
        ReplicationChannelKind::Udp => {
            Err("udp is not supported for raft replication transport".to_string())
        }
        ReplicationChannelKind::Custom => {
            Err("custom replication transport requires explicit provider wiring".to_string())
        }
    }
}

pub(in crate::runtime) fn parse_read_preference(
    value: &str,
) -> Result<QueryReadPreference, String> {
    match value {
        "primary" | "PRIMARY" => Ok(QueryReadPreference::Primary),
        "prefer-replica" | "PREFER_REPLICA" | "prefer_replica" => {
            Ok(QueryReadPreference::PreferReplica)
        }
        _ => Err("--read-preference must be primary or prefer-replica".to_string()),
    }
}

pub(in crate::runtime) fn is_local_bind(address: &str) -> bool {
    address.starts_with("127.")
        || address.starts_with("localhost:")
        || address.starts_with("[::1]:")
        || address.starts_with("::1:")
}

pub(in crate::runtime) fn is_temp_or_dev_data_dir(path: &PathBuf) -> bool {
    path == &PathBuf::from("data") || path.starts_with("/tmp") || path.starts_with("/var/tmp")
}

pub(in crate::runtime) fn is_strong_admin_token(token: &str) -> bool {
    let lowered = token.to_ascii_lowercase();
    token.len() >= 16
        && !matches!(
            token,
            "secret" | "admin:secret" | "admin:change-me" | "change-me" | "changeme"
        )
        && !lowered.contains("change-me")
        && !lowered.contains("replace-with")
        && !lowered.contains("changeme")
}

pub(in crate::runtime) fn format_ack_policy(policy: ReplicationAckPolicy) -> &'static str {
    match policy {
        ReplicationAckPolicy::All => "all",
        ReplicationAckPolicy::Quorum => "quorum",
        ReplicationAckPolicy::Async => "async",
    }
}

pub(in crate::runtime) fn format_read_preference(preference: QueryReadPreference) -> &'static str {
    match preference {
        QueryReadPreference::Primary => "primary",
        QueryReadPreference::PreferReplica => "prefer-replica",
    }
}

pub(in crate::runtime) fn next_arg(
    args: &mut impl Iterator<Item = String>,
    name: &str,
) -> Result<String, String> {
    args.next()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| format!("{name} requires a value"))
}

pub(in crate::runtime) fn parse_next<T: std::str::FromStr>(
    args: &mut impl Iterator<Item = String>,
    name: &str,
) -> Result<T, String> {
    next_arg(args, name)?
        .parse()
        .map_err(|_| format!("{name} has an invalid value"))
}

pub(in crate::runtime) fn usage() -> String {
    "usage: neo4r-server [--config PATH] [--check-config] [--production-check] [--dump-config] [--bind ADDR] [--web-bind ADDR] [--web-auth-token TOKEN] [--slow-query-threshold-ms MS] [--data-dir DIR] [--shards N] [--partitions N] [--server-id ID] [--primary-server-id ID] [--replica-peer SERVER_ID=ADDR] [--peer SERVER_ID=ADDR] [--query-peer SERVER_ID=ADDR] [--gossip-seed-peer SERVER_ID=ADDR] [--gossip-advertise-query ADDR] [--gossip-advertise-replication ADDR] [--gossip-interval-ms MS] [--gossip-ttl-ms MS] [--gossip-fanout N] [--gossip-auth-token TOKEN] [--gossip-auto-negotiate-replication] [--read-preference primary|prefer-replica] [--replication-bind ADDR] [--replication-transport tcp|rdma] [--replication-ack all|quorum|async] [--replication-connect-timeout-ms MS] [--replication-retry-attempts N] [--replication-retry-backoff-ms MS] [--replication-max-in-flight-batches N] [--catch-up-on-startup] [--catch-up-interval-ms MS] [--catch-up-batch-size N] [--sync-index-catalog-on-startup] [--sync-index-catalog-interval-ms MS] [--recover-transactions-on-startup] [--recover-transactions-interval-ms MS] [--tls-mode disabled|external|required] [--replication-tls-mode disabled|external|required] [--web-tls-mode disabled|external|required] [--native-tls-cert CERT.pem] [--native-tls-key KEY.pem] [--native-tls-client-ca CA.pem] [--native-tls-require-client-auth] [--web-tls-cert CERT.pem] [--web-tls-key KEY.pem] [--web-tls-client-ca CA.pem] [--web-tls-require-client-auth] [--replication-tls-cert CERT.pem] [--replication-tls-key KEY.pem] [--replication-tls-client-ca CA.pem] [--replication-tls-require-client-auth] [--replication-tls-ca CA.pem] [--replication-tls-server-name DNS_NAME] [--replication-tls-client-cert CERT.pem] [--replication-tls-client-key KEY.pem] [--min-native-protocol-version N] [--max-native-protocol-version N] [--backup-drill-max-age-hours N] [--wal-archive-dir DIR] [--restore-drill-manifest PATH] [--audit-retention-days N] [--secret-rotation-days N] [--tenant-max-concurrent-queries N] [--tenant-max-result-rows N] [--data-format-version N] [--upgrade-manifest PATH] [--raft-lease-clock-drift-bound-ms N] [--raft-lease-message-delay-bound-ms N] [--observability-alerts PATH] [--repair-check-on-startup] [--query-regression-corpus PATH] [--chaos-gate-required] [--runbook PATH] [--systemd-unit PATH] [--logrotate PATH] [--workers N] [--queue-capacity N] [--page-size N] [--daemonize]".to_string()
}

pub(in crate::runtime) fn default_worker_count() -> usize {
    std::thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(4)
}
