use serde::Deserialize;
use std::fs;
use std::path::{Path, PathBuf};

pub(super) fn expand_config_args(args: Vec<String>) -> Result<Vec<String>, String> {
    let mut expanded = Vec::new();
    for path in config_paths_from_args(&args)? {
        expanded.extend(load_config_args(&path)?);
    }
    expanded.extend(args_without_config_args(&args)?);
    Ok(expanded)
}

fn config_paths_from_args(args: &[String]) -> Result<Vec<PathBuf>, String> {
    let mut paths = Vec::new();
    let mut index = 0;
    while index < args.len() {
        if args[index] == "--config" {
            index += 1;
            let Some(path) = args.get(index) else {
                return Err("--config requires a path".to_string());
            };
            paths.push(PathBuf::from(path));
        }
        index += 1;
    }
    Ok(paths)
}

fn args_without_config_args(args: &[String]) -> Result<Vec<String>, String> {
    let mut output = Vec::new();
    let mut index = 0;
    while index < args.len() {
        if args[index] == "--config" {
            index += 1;
            if args.get(index).is_none() {
                return Err("--config requires a path".to_string());
            }
        } else {
            output.push(args[index].clone());
        }
        index += 1;
    }
    Ok(output)
}

fn load_config_args(path: &Path) -> Result<Vec<String>, String> {
    match path.extension().and_then(|ext| ext.to_str()) {
        Some("yaml" | "yml") => load_yaml_config_args(path),
        _ => load_key_value_config_args(path),
    }
}

fn load_key_value_config_args(path: &Path) -> Result<Vec<String>, String> {
    let contents = fs::read_to_string(path)
        .map_err(|err| format!("failed to read config {}: {err}", path.display()))?;
    let mut args = Vec::new();
    for (line_no, raw_line) in contents.lines().enumerate() {
        let line = raw_line.split('#').next().unwrap_or("").trim();
        if line.is_empty() {
            continue;
        }
        let (key, value) = line
            .split_once('=')
            .ok_or_else(|| format!("config line {} must be key=value", line_no + 1))?;
        append_config_arg(&mut args, key.trim(), value.trim(), line_no)?;
    }
    Ok(args)
}

fn load_yaml_config_args(path: &Path) -> Result<Vec<String>, String> {
    let contents = fs::read_to_string(path)
        .map_err(|err| format!("failed to read config {}: {err}", path.display()))?;
    let config = serde_yaml::from_str::<ServerConfigFile>(&contents)
        .map_err(|err| format!("failed to parse yaml config {}: {err}", path.display()))?;
    Ok(config.into_args())
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct ServerConfigFile {
    #[serde(default)]
    server: ServerSection,
    #[serde(default)]
    database: DatabaseSection,
    #[serde(default)]
    replication: ReplicationSection,
    #[serde(default)]
    query: QuerySection,
    #[serde(default)]
    gossip: GossipSection,
    #[serde(default)]
    web: WebSection,
    #[serde(default)]
    maintenance: MaintenanceSection,
    #[serde(default)]
    production: ProductionSection,
}

impl ServerConfigFile {
    fn into_args(self) -> Vec<String> {
        let mut args = Vec::new();
        push_option(&mut args, "--bind", self.server.bind);
        push_option_path(&mut args, "--data-dir", self.server.data_dir);
        push_option_display(&mut args, "--server-id", self.server.server_id);
        push_option_display(&mut args, "--workers", self.server.workers);
        push_option_display(&mut args, "--queue-capacity", self.server.queue_capacity);
        push_option_display(&mut args, "--page-size", self.server.page_size);
        push_option_display(
            &mut args,
            "--primary-server-id",
            self.server.primary_server_id,
        );
        push_option_path(&mut args, "--routing-table", self.server.routing_table);
        push_bool(&mut args, "--daemonize", self.server.daemonize);

        push_option_display(&mut args, "--shards", self.database.shards);
        push_option_display(&mut args, "--partitions", self.database.partitions);

        push_option(&mut args, "--read-preference", self.query.read_preference);
        for peer in self.query.peers {
            push_peer(&mut args, "--query-peer", peer);
        }

        push_option(
            &mut args,
            "--gossip-advertise-query",
            self.gossip.advertise_query,
        );
        push_option(
            &mut args,
            "--gossip-advertise-replication",
            self.gossip.advertise_replication,
        );
        push_option_display(&mut args, "--gossip-interval-ms", self.gossip.interval_ms);
        push_option_display(&mut args, "--gossip-ttl-ms", self.gossip.ttl_ms);
        push_option_display(&mut args, "--gossip-fanout", self.gossip.fanout);
        push_option(&mut args, "--gossip-auth-token", self.gossip.auth_token);
        push_bool(
            &mut args,
            "--gossip-auto-negotiate-replication",
            self.gossip.auto_negotiate_replication,
        );
        for peer in self.gossip.seed_peers {
            push_peer(&mut args, "--gossip-seed-peer", peer);
        }

        push_option(&mut args, "--replication-bind", self.replication.bind);
        push_option(
            &mut args,
            "--replication-transport",
            self.replication.transport,
        );
        push_option(&mut args, "--replication-ack", self.replication.ack);
        push_option_display(
            &mut args,
            "--replication-connect-timeout-ms",
            self.replication.connect_timeout_ms,
        );
        push_option_display(
            &mut args,
            "--replication-retry-attempts",
            self.replication.retry_attempts,
        );
        push_option_display(
            &mut args,
            "--replication-retry-backoff-ms",
            self.replication.retry_backoff_ms,
        );
        push_option_display(
            &mut args,
            "--replication-max-in-flight-batches",
            self.replication.max_in_flight_batches,
        );
        for peer in self.replication.replica_peers {
            push_peer(&mut args, "--replica-peer", peer);
        }
        for peer in self.replication.peers {
            match peer.role.as_deref().unwrap_or("peer") {
                "replica" => push_peer(&mut args, "--replica-peer", peer),
                "query" => push_peer(&mut args, "--query-peer", peer),
                _ => push_peer(&mut args, "--peer", peer),
            }
        }
        push_bool(
            &mut args,
            "--catch-up-on-startup",
            self.replication.catch_up_on_startup,
        );
        push_option_display(
            &mut args,
            "--catch-up-interval-ms",
            self.replication.catch_up_interval_ms,
        );
        push_option_display(
            &mut args,
            "--catch-up-batch-size",
            self.replication.catch_up_batch_size,
        );

        push_option(&mut args, "--web-bind", self.web.bind);
        push_option(&mut args, "--web-auth-token", self.web.auth_token);
        push_option_display(
            &mut args,
            "--slow-query-threshold-ms",
            self.web.slow_query_threshold_ms,
        );

        push_bool(
            &mut args,
            "--sync-index-catalog-on-startup",
            self.maintenance.sync_index_catalog_on_startup,
        );
        push_option_display(
            &mut args,
            "--sync-index-catalog-interval-ms",
            self.maintenance.sync_index_catalog_interval_ms,
        );
        push_bool(
            &mut args,
            "--recover-transactions-on-startup",
            self.maintenance.recover_transactions_on_startup,
        );
        push_option_display(
            &mut args,
            "--recover-transactions-interval-ms",
            self.maintenance.recover_transactions_interval_ms,
        );
        push_option(&mut args, "--tls-mode", self.production.tls_mode);
        push_option(
            &mut args,
            "--replication-tls-mode",
            self.production.replication_tls_mode,
        );
        push_option(&mut args, "--web-tls-mode", self.production.web_tls_mode);
        push_option_path(
            &mut args,
            "--native-tls-cert",
            self.production.native_tls_cert,
        );
        push_option_path(
            &mut args,
            "--native-tls-key",
            self.production.native_tls_key,
        );
        push_option_path(
            &mut args,
            "--native-tls-client-ca",
            self.production.native_tls_client_ca,
        );
        push_bool(
            &mut args,
            "--native-tls-require-client-auth",
            self.production.native_tls_require_client_auth,
        );
        push_option_path(&mut args, "--web-tls-cert", self.production.web_tls_cert);
        push_option_path(&mut args, "--web-tls-key", self.production.web_tls_key);
        push_option_path(
            &mut args,
            "--web-tls-client-ca",
            self.production.web_tls_client_ca,
        );
        push_bool(
            &mut args,
            "--web-tls-require-client-auth",
            self.production.web_tls_require_client_auth,
        );
        push_option_path(
            &mut args,
            "--replication-tls-cert",
            self.production.replication_tls_cert,
        );
        push_option_path(
            &mut args,
            "--replication-tls-key",
            self.production.replication_tls_key,
        );
        push_option_path(
            &mut args,
            "--replication-tls-client-ca",
            self.production.replication_tls_client_ca,
        );
        push_bool(
            &mut args,
            "--replication-tls-require-client-auth",
            self.production.replication_tls_require_client_auth,
        );
        push_option_path(
            &mut args,
            "--replication-tls-ca",
            self.production.replication_tls_ca,
        );
        push_option(
            &mut args,
            "--replication-tls-server-name",
            self.production.replication_tls_server_name,
        );
        push_option_path(
            &mut args,
            "--replication-tls-client-cert",
            self.production.replication_tls_client_cert,
        );
        push_option_path(
            &mut args,
            "--replication-tls-client-key",
            self.production.replication_tls_client_key,
        );
        push_option_display(
            &mut args,
            "--min-native-protocol-version",
            self.production.min_native_protocol_version,
        );
        push_option_display(
            &mut args,
            "--max-native-protocol-version",
            self.production.max_native_protocol_version,
        );
        push_option_display(
            &mut args,
            "--backup-drill-max-age-hours",
            self.production.backup_drill_max_age_hours,
        );
        push_option_path(
            &mut args,
            "--wal-archive-dir",
            self.production.wal_archive_dir,
        );
        push_option_path(
            &mut args,
            "--restore-drill-manifest",
            self.production.restore_drill_manifest,
        );
        push_option_display(
            &mut args,
            "--audit-retention-days",
            self.production.audit_retention_days,
        );
        push_option_display(
            &mut args,
            "--secret-rotation-days",
            self.production.secret_rotation_days,
        );
        push_option_display(
            &mut args,
            "--tenant-max-concurrent-queries",
            self.production.tenant_max_concurrent_queries,
        );
        push_option_display(
            &mut args,
            "--tenant-max-result-rows",
            self.production.tenant_max_result_rows,
        );
        push_option_display(
            &mut args,
            "--data-format-version",
            self.production.data_format_version,
        );
        push_option_path(
            &mut args,
            "--upgrade-manifest",
            self.production.upgrade_manifest,
        );
        push_option_display(
            &mut args,
            "--raft-lease-clock-drift-bound-ms",
            self.production.raft_lease_clock_drift_bound_ms,
        );
        push_option_display(
            &mut args,
            "--raft-lease-message-delay-bound-ms",
            self.production.raft_lease_message_delay_bound_ms,
        );
        push_option_path(
            &mut args,
            "--observability-alerts",
            self.production.observability_alerts,
        );
        push_bool(
            &mut args,
            "--repair-check-on-startup",
            self.production.repair_check_on_startup,
        );
        push_option_path(
            &mut args,
            "--query-regression-corpus",
            self.production.query_regression_corpus,
        );
        push_bool(
            &mut args,
            "--chaos-gate-required",
            self.production.chaos_gate_required,
        );
        push_option_path(&mut args, "--runbook", self.production.runbook);
        push_option_path(&mut args, "--systemd-unit", self.production.systemd_unit);
        push_option_path(&mut args, "--logrotate", self.production.logrotate);
        args
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct ServerSection {
    bind: Option<String>,
    data_dir: Option<PathBuf>,
    server_id: Option<u64>,
    workers: Option<usize>,
    queue_capacity: Option<usize>,
    page_size: Option<usize>,
    primary_server_id: Option<u64>,
    routing_table: Option<PathBuf>,
    daemonize: Option<bool>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct DatabaseSection {
    shards: Option<u64>,
    partitions: Option<usize>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReplicationSection {
    bind: Option<String>,
    transport: Option<String>,
    ack: Option<String>,
    connect_timeout_ms: Option<u64>,
    retry_attempts: Option<usize>,
    retry_backoff_ms: Option<u64>,
    max_in_flight_batches: Option<usize>,
    #[serde(default)]
    peers: Vec<PeerConfig>,
    #[serde(default)]
    replica_peers: Vec<PeerConfig>,
    catch_up_on_startup: Option<bool>,
    catch_up_interval_ms: Option<u64>,
    catch_up_batch_size: Option<usize>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct QuerySection {
    read_preference: Option<String>,
    #[serde(default)]
    peers: Vec<PeerConfig>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct GossipSection {
    advertise_query: Option<String>,
    advertise_replication: Option<String>,
    interval_ms: Option<u64>,
    ttl_ms: Option<u64>,
    fanout: Option<usize>,
    auth_token: Option<String>,
    auto_negotiate_replication: Option<bool>,
    #[serde(default)]
    seed_peers: Vec<PeerConfig>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct WebSection {
    bind: Option<String>,
    auth_token: Option<String>,
    slow_query_threshold_ms: Option<u64>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct MaintenanceSection {
    sync_index_catalog_on_startup: Option<bool>,
    sync_index_catalog_interval_ms: Option<u64>,
    recover_transactions_on_startup: Option<bool>,
    recover_transactions_interval_ms: Option<u64>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProductionSection {
    tls_mode: Option<String>,
    replication_tls_mode: Option<String>,
    web_tls_mode: Option<String>,
    native_tls_cert: Option<PathBuf>,
    native_tls_key: Option<PathBuf>,
    native_tls_client_ca: Option<PathBuf>,
    native_tls_require_client_auth: Option<bool>,
    web_tls_cert: Option<PathBuf>,
    web_tls_key: Option<PathBuf>,
    web_tls_client_ca: Option<PathBuf>,
    web_tls_require_client_auth: Option<bool>,
    replication_tls_cert: Option<PathBuf>,
    replication_tls_key: Option<PathBuf>,
    replication_tls_client_ca: Option<PathBuf>,
    replication_tls_require_client_auth: Option<bool>,
    replication_tls_ca: Option<PathBuf>,
    replication_tls_server_name: Option<String>,
    replication_tls_client_cert: Option<PathBuf>,
    replication_tls_client_key: Option<PathBuf>,
    min_native_protocol_version: Option<u8>,
    max_native_protocol_version: Option<u8>,
    backup_drill_max_age_hours: Option<u64>,
    wal_archive_dir: Option<PathBuf>,
    restore_drill_manifest: Option<PathBuf>,
    audit_retention_days: Option<u64>,
    secret_rotation_days: Option<u64>,
    tenant_max_concurrent_queries: Option<usize>,
    tenant_max_result_rows: Option<usize>,
    data_format_version: Option<u32>,
    upgrade_manifest: Option<PathBuf>,
    raft_lease_clock_drift_bound_ms: Option<u64>,
    raft_lease_message_delay_bound_ms: Option<u64>,
    observability_alerts: Option<PathBuf>,
    repair_check_on_startup: Option<bool>,
    query_regression_corpus: Option<PathBuf>,
    chaos_gate_required: Option<bool>,
    runbook: Option<PathBuf>,
    systemd_unit: Option<PathBuf>,
    logrotate: Option<PathBuf>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PeerConfig {
    server_id: u64,
    address: String,
    role: Option<String>,
}

fn push_option(args: &mut Vec<String>, name: &str, value: Option<String>) {
    if let Some(value) = value {
        args.push(name.to_string());
        args.push(value);
    }
}

fn push_option_path(args: &mut Vec<String>, name: &str, value: Option<PathBuf>) {
    if let Some(value) = value {
        args.push(name.to_string());
        args.push(value.display().to_string());
    }
}

fn push_option_display<T: std::fmt::Display>(args: &mut Vec<String>, name: &str, value: Option<T>) {
    if let Some(value) = value {
        args.push(name.to_string());
        args.push(value.to_string());
    }
}

fn push_bool(args: &mut Vec<String>, name: &str, value: Option<bool>) {
    if value == Some(true) {
        args.push(name.to_string());
    }
}

fn push_peer(args: &mut Vec<String>, name: &str, peer: PeerConfig) {
    args.push(name.to_string());
    args.push(format!("{}={}", peer.server_id, peer.address));
}

fn append_config_arg(
    args: &mut Vec<String>,
    key: &str,
    value: &str,
    line_no: usize,
) -> Result<(), String> {
    let key = key.replace('_', "-");
    match key.as_str() {
        "catch-up-on-startup"
        | "sync-index-catalog-on-startup"
        | "recover-transactions-on-startup"
        | "gossip-auto-negotiate-replication"
        | "daemonize"
        | "check-config"
        | "production-check"
        | "dump-config" => {
            if parse_config_bool(value, line_no)? {
                args.push(format!("--{key}"));
            }
        }
        "bind"
        | "data-dir"
        | "shards"
        | "partitions"
        | "server-id"
        | "workers"
        | "queue-capacity"
        | "page-size"
        | "read-preference"
        | "primary-server-id"
        | "routing-table"
        | "replica-peer"
        | "peer"
        | "query-peer"
        | "gossip-seed-peer"
        | "gossip-advertise-query"
        | "gossip-advertise-replication"
        | "gossip-interval-ms"
        | "gossip-ttl-ms"
        | "gossip-fanout"
        | "gossip-auth-token"
        | "replication-bind"
        | "replication-transport"
        | "replication-ack"
        | "replication-connect-timeout-ms"
        | "replication-retry-attempts"
        | "replication-retry-backoff-ms"
        | "replication-max-in-flight-batches"
        | "catch-up-interval-ms"
        | "catch-up-batch-size"
        | "sync-index-catalog-interval-ms"
        | "recover-transactions-interval-ms"
        | "web-bind"
        | "web-auth-token"
        | "slow-query-threshold-ms"
        | "tls-mode"
        | "replication-tls-mode"
        | "web-tls-mode"
        | "native-tls-cert"
        | "native-tls-key"
        | "native-tls-client-ca"
        | "web-tls-cert"
        | "web-tls-key"
        | "web-tls-client-ca"
        | "replication-tls-cert"
        | "replication-tls-key"
        | "replication-tls-client-ca"
        | "replication-tls-ca"
        | "replication-tls-server-name"
        | "replication-tls-client-cert"
        | "replication-tls-client-key"
        | "min-native-protocol-version"
        | "max-native-protocol-version"
        | "backup-drill-max-age-hours"
        | "wal-archive-dir"
        | "restore-drill-manifest"
        | "audit-retention-days"
        | "secret-rotation-days"
        | "tenant-max-concurrent-queries"
        | "tenant-max-result-rows"
        | "data-format-version"
        | "upgrade-manifest"
        | "raft-lease-clock-drift-bound-ms"
        | "raft-lease-message-delay-bound-ms"
        | "observability-alerts"
        | "query-regression-corpus"
        | "runbook"
        | "systemd-unit"
        | "logrotate" => {
            if value.is_empty() {
                return Err(format!("config line {} value cannot be empty", line_no + 1));
            }
            args.push(format!("--{key}"));
            args.push(value.to_string());
        }
        "native-tls-require-client-auth"
        | "web-tls-require-client-auth"
        | "replication-tls-require-client-auth"
        | "repair-check-on-startup"
        | "chaos-gate-required" => {
            if parse_config_bool(value, line_no)? {
                args.push(format!("--{key}"));
            }
        }
        _ => return Err(format!("config line {} has unknown key {key}", line_no + 1)),
    }
    Ok(())
}

fn parse_config_bool(value: &str, line_no: usize) -> Result<bool, String> {
    match value.to_ascii_lowercase().as_str() {
        "true" | "yes" | "1" | "on" => Ok(true),
        "false" | "no" | "0" | "off" => Ok(false),
        _ => Err(format!(
            "config line {} boolean value must be true or false",
            line_no + 1
        )),
    }
}
