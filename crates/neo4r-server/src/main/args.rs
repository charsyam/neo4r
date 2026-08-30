use super::*;

#[derive(Debug)]
pub(super) struct ServerArgs {
    pub(super) bind_addr: String,
    pub(super) data_dir: PathBuf,
    pub(super) shard_count: u64,
    pub(super) partition_count: usize,
    pub(super) server_id: u64,
    pub(super) worker_count: usize,
    pub(super) queue_capacity: usize,
    pub(super) default_page_size: usize,
    pub(super) read_preference: QueryReadPreference,
    pub(super) primary_server_id: u64,
    pub(super) routing_table_path: Option<PathBuf>,
    pub(super) replica_peers: Vec<ReplicaPeer>,
    pub(super) peers: Vec<ReplicaPeer>,
    pub(super) query_peers: Vec<ReplicaPeer>,
    pub(super) replication_bind_addr: Option<String>,
    pub(super) replication_transport: ReplicationChannelKind,
    pub(super) replication_ack_policy: ReplicationAckPolicy,
    pub(super) replication_connect_timeout_ms: u64,
    pub(super) replication_retry_attempts: usize,
    pub(super) replication_retry_backoff_ms: u64,
    pub(super) catch_up_on_startup: bool,
    pub(super) catch_up_interval_ms: Option<u64>,
    pub(super) catch_up_batch_size: Option<usize>,
    pub(super) sync_index_catalog_on_startup: bool,
    pub(super) sync_index_catalog_interval_ms: Option<u64>,
    pub(super) recover_transactions_on_startup: bool,
    pub(super) recover_transactions_interval_ms: Option<u64>,
    pub(super) web_bind_addr: Option<String>,
    pub(super) web_auth_token: Option<String>,
    pub(super) slow_query_threshold_ms: u64,
    pub(super) tls_mode: ProductionSecurityMode,
    pub(super) replication_tls_mode: ProductionSecurityMode,
    pub(super) web_tls_mode: ProductionSecurityMode,
    pub(super) native_tls_cert_path: Option<PathBuf>,
    pub(super) native_tls_key_path: Option<PathBuf>,
    pub(super) native_tls_client_ca_path: Option<PathBuf>,
    pub(super) native_tls_require_client_auth: bool,
    pub(super) replication_tls_cert_path: Option<PathBuf>,
    pub(super) replication_tls_key_path: Option<PathBuf>,
    pub(super) replication_tls_client_ca_path: Option<PathBuf>,
    pub(super) replication_tls_require_client_auth: bool,
    pub(super) replication_tls_ca_path: Option<PathBuf>,
    pub(super) replication_tls_server_name: Option<String>,
    pub(super) replication_tls_client_cert_path: Option<PathBuf>,
    pub(super) replication_tls_client_key_path: Option<PathBuf>,
    pub(super) min_native_protocol_version: Option<u8>,
    pub(super) max_native_protocol_version: Option<u8>,
    pub(super) backup_drill_max_age_hours: Option<u64>,
    pub(super) wal_archive_dir: Option<PathBuf>,
    pub(super) restore_drill_manifest_path: Option<PathBuf>,
    pub(super) audit_retention_days: Option<u64>,
    pub(super) secret_rotation_days: Option<u64>,
    pub(super) tenant_max_concurrent_queries: Option<usize>,
    pub(super) tenant_max_result_rows: Option<usize>,
    pub(super) data_format_version: Option<u32>,
    pub(super) upgrade_manifest_path: Option<PathBuf>,
    pub(super) raft_lease_clock_drift_bound_ms: Option<u64>,
    pub(super) raft_lease_message_delay_bound_ms: Option<u64>,
    pub(super) observability_alerts_path: Option<PathBuf>,
    pub(super) repair_check_on_startup: bool,
    pub(super) query_regression_corpus_path: Option<PathBuf>,
    pub(super) chaos_gate_required: bool,
    pub(super) runbook_path: Option<PathBuf>,
    pub(super) systemd_unit_path: Option<PathBuf>,
    pub(super) logrotate_path: Option<PathBuf>,
    pub(super) daemonize: bool,
    pub(super) check_config: bool,
    pub(super) production_check: bool,
    pub(super) dump_config: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ReplicaPeer {
    pub(super) server_id: u64,
    pub(super) address: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ProductionSecurityMode {
    Disabled,
    External,
    Required,
}

impl ServerArgs {
    pub(super) fn parse(args: impl IntoIterator<Item = String>) -> Result<Self, String> {
        let args = args.into_iter().collect::<Vec<_>>();
        let expanded = config::expand_config_args(args)?;
        Self::parse_expanded(expanded)
    }

    pub(super) fn parse_expanded(args: impl IntoIterator<Item = String>) -> Result<Self, String> {
        let mut parsed = Self {
            bind_addr: "127.0.0.1:7687".to_string(),
            data_dir: PathBuf::from("data"),
            shard_count: 1,
            partition_count: 1,
            server_id: 1,
            worker_count: default_worker_count(),
            queue_capacity: 1024,
            default_page_size: 128,
            read_preference: QueryReadPreference::Primary,
            primary_server_id: 1,
            routing_table_path: None,
            replica_peers: Vec::new(),
            peers: Vec::new(),
            query_peers: Vec::new(),
            replication_bind_addr: None,
            replication_transport: ReplicationChannelKind::Tcp,
            replication_ack_policy: ReplicationAckPolicy::All,
            replication_connect_timeout_ms: 1000,
            replication_retry_attempts: 1,
            replication_retry_backoff_ms: 10,
            catch_up_on_startup: false,
            catch_up_interval_ms: None,
            catch_up_batch_size: None,
            sync_index_catalog_on_startup: false,
            sync_index_catalog_interval_ms: None,
            recover_transactions_on_startup: false,
            recover_transactions_interval_ms: None,
            web_bind_addr: None,
            web_auth_token: None,
            slow_query_threshold_ms: 250,
            tls_mode: ProductionSecurityMode::Disabled,
            replication_tls_mode: ProductionSecurityMode::Disabled,
            web_tls_mode: ProductionSecurityMode::Disabled,
            native_tls_cert_path: None,
            native_tls_key_path: None,
            native_tls_client_ca_path: None,
            native_tls_require_client_auth: false,
            replication_tls_cert_path: None,
            replication_tls_key_path: None,
            replication_tls_client_ca_path: None,
            replication_tls_require_client_auth: false,
            replication_tls_ca_path: None,
            replication_tls_server_name: None,
            replication_tls_client_cert_path: None,
            replication_tls_client_key_path: None,
            min_native_protocol_version: None,
            max_native_protocol_version: None,
            backup_drill_max_age_hours: None,
            wal_archive_dir: None,
            restore_drill_manifest_path: None,
            audit_retention_days: None,
            secret_rotation_days: None,
            tenant_max_concurrent_queries: None,
            tenant_max_result_rows: None,
            data_format_version: None,
            upgrade_manifest_path: None,
            raft_lease_clock_drift_bound_ms: None,
            raft_lease_message_delay_bound_ms: None,
            observability_alerts_path: None,
            repair_check_on_startup: false,
            query_regression_corpus_path: None,
            chaos_gate_required: false,
            runbook_path: None,
            systemd_unit_path: None,
            logrotate_path: None,
            daemonize: false,
            check_config: false,
            production_check: false,
            dump_config: false,
        };
        let mut args = args.into_iter();
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--bind" => parsed.bind_addr = next_arg(&mut args, "--bind")?,
                "--config" => return Err("--config must be handled before parsing".to_string()),
                "--data-dir" => parsed.data_dir = PathBuf::from(next_arg(&mut args, "--data-dir")?),
                "--shards" => parsed.shard_count = parse_next(&mut args, "--shards")?,
                "--partitions" => parsed.partition_count = parse_next(&mut args, "--partitions")?,
                "--server-id" => parsed.server_id = parse_next(&mut args, "--server-id")?,
                "--workers" => parsed.worker_count = parse_next(&mut args, "--workers")?,
                "--queue-capacity" => {
                    parsed.queue_capacity = parse_next(&mut args, "--queue-capacity")?
                }
                "--page-size" => parsed.default_page_size = parse_next(&mut args, "--page-size")?,
                "--read-preference" => {
                    parsed.read_preference =
                        parse_read_preference(&next_arg(&mut args, "--read-preference")?)?
                }
                "--primary-server-id" => {
                    parsed.primary_server_id = parse_next(&mut args, "--primary-server-id")?
                }
                "--routing-table" => {
                    parsed.routing_table_path =
                        Some(PathBuf::from(next_arg(&mut args, "--routing-table")?))
                }
                "--replica-peer" => parsed
                    .replica_peers
                    .push(parse_replica_peer(&next_arg(&mut args, "--replica-peer")?)?),
                "--peer" => parsed
                    .peers
                    .push(parse_peer(&next_arg(&mut args, "--peer")?, "--peer")?),
                "--query-peer" => parsed.query_peers.push(parse_peer(
                    &next_arg(&mut args, "--query-peer")?,
                    "--query-peer",
                )?),
                "--replication-bind" => {
                    parsed.replication_bind_addr = Some(next_arg(&mut args, "--replication-bind")?)
                }
                "--replication-transport" => {
                    parsed.replication_transport = parse_replication_transport(&next_arg(
                        &mut args,
                        "--replication-transport",
                    )?)?
                }
                "--replication-ack" => {
                    parsed.replication_ack_policy =
                        parse_ack_policy(&next_arg(&mut args, "--replication-ack")?)?
                }
                "--replication-connect-timeout-ms" => {
                    parsed.replication_connect_timeout_ms =
                        parse_next(&mut args, "--replication-connect-timeout-ms")?
                }
                "--replication-retry-attempts" => {
                    parsed.replication_retry_attempts =
                        parse_next(&mut args, "--replication-retry-attempts")?
                }
                "--replication-retry-backoff-ms" => {
                    parsed.replication_retry_backoff_ms =
                        parse_next(&mut args, "--replication-retry-backoff-ms")?
                }
                "--catch-up-on-startup" => parsed.catch_up_on_startup = true,
                "--catch-up-interval-ms" => {
                    parsed.catch_up_interval_ms =
                        Some(parse_next(&mut args, "--catch-up-interval-ms")?)
                }
                "--catch-up-batch-size" => {
                    parsed.catch_up_batch_size =
                        Some(parse_next(&mut args, "--catch-up-batch-size")?)
                }
                "--sync-index-catalog-on-startup" => parsed.sync_index_catalog_on_startup = true,
                "--sync-index-catalog-interval-ms" => {
                    parsed.sync_index_catalog_interval_ms =
                        Some(parse_next(&mut args, "--sync-index-catalog-interval-ms")?)
                }
                "--recover-transactions-on-startup" => {
                    parsed.recover_transactions_on_startup = true
                }
                "--recover-transactions-interval-ms" => {
                    parsed.recover_transactions_interval_ms =
                        Some(parse_next(&mut args, "--recover-transactions-interval-ms")?)
                }
                "--web-bind" => parsed.web_bind_addr = Some(next_arg(&mut args, "--web-bind")?),
                "--web-auth-token" => {
                    parsed.web_auth_token = Some(next_arg(&mut args, "--web-auth-token")?)
                }
                "--slow-query-threshold-ms" => {
                    parsed.slow_query_threshold_ms =
                        parse_next(&mut args, "--slow-query-threshold-ms")?
                }
                "--tls-mode" => {
                    parsed.tls_mode =
                        parse_production_security_mode(&next_arg(&mut args, "--tls-mode")?)?
                }
                "--replication-tls-mode" => {
                    parsed.replication_tls_mode = parse_production_security_mode(&next_arg(
                        &mut args,
                        "--replication-tls-mode",
                    )?)?
                }
                "--web-tls-mode" => {
                    parsed.web_tls_mode =
                        parse_production_security_mode(&next_arg(&mut args, "--web-tls-mode")?)?
                }
                "--native-tls-cert" => {
                    parsed.native_tls_cert_path =
                        Some(PathBuf::from(next_arg(&mut args, "--native-tls-cert")?))
                }
                "--native-tls-key" => {
                    parsed.native_tls_key_path =
                        Some(PathBuf::from(next_arg(&mut args, "--native-tls-key")?))
                }
                "--native-tls-client-ca" => {
                    parsed.native_tls_client_ca_path = Some(PathBuf::from(next_arg(
                        &mut args,
                        "--native-tls-client-ca",
                    )?))
                }
                "--native-tls-require-client-auth" => parsed.native_tls_require_client_auth = true,
                "--replication-tls-cert" => {
                    parsed.replication_tls_cert_path = Some(PathBuf::from(next_arg(
                        &mut args,
                        "--replication-tls-cert",
                    )?))
                }
                "--replication-tls-key" => {
                    parsed.replication_tls_key_path =
                        Some(PathBuf::from(next_arg(&mut args, "--replication-tls-key")?))
                }
                "--replication-tls-client-ca" => {
                    parsed.replication_tls_client_ca_path = Some(PathBuf::from(next_arg(
                        &mut args,
                        "--replication-tls-client-ca",
                    )?))
                }
                "--replication-tls-require-client-auth" => {
                    parsed.replication_tls_require_client_auth = true
                }
                "--replication-tls-ca" => {
                    parsed.replication_tls_ca_path =
                        Some(PathBuf::from(next_arg(&mut args, "--replication-tls-ca")?))
                }
                "--replication-tls-server-name" => {
                    parsed.replication_tls_server_name =
                        Some(next_arg(&mut args, "--replication-tls-server-name")?)
                }
                "--replication-tls-client-cert" => {
                    parsed.replication_tls_client_cert_path = Some(PathBuf::from(next_arg(
                        &mut args,
                        "--replication-tls-client-cert",
                    )?))
                }
                "--replication-tls-client-key" => {
                    parsed.replication_tls_client_key_path = Some(PathBuf::from(next_arg(
                        &mut args,
                        "--replication-tls-client-key",
                    )?))
                }
                "--min-native-protocol-version" => {
                    parsed.min_native_protocol_version =
                        Some(parse_next(&mut args, "--min-native-protocol-version")?)
                }
                "--max-native-protocol-version" => {
                    parsed.max_native_protocol_version =
                        Some(parse_next(&mut args, "--max-native-protocol-version")?)
                }
                "--backup-drill-max-age-hours" => {
                    parsed.backup_drill_max_age_hours =
                        Some(parse_next(&mut args, "--backup-drill-max-age-hours")?)
                }
                "--wal-archive-dir" => {
                    parsed.wal_archive_dir =
                        Some(PathBuf::from(next_arg(&mut args, "--wal-archive-dir")?))
                }
                "--restore-drill-manifest" => {
                    parsed.restore_drill_manifest_path = Some(PathBuf::from(next_arg(
                        &mut args,
                        "--restore-drill-manifest",
                    )?))
                }
                "--audit-retention-days" => {
                    parsed.audit_retention_days =
                        Some(parse_next(&mut args, "--audit-retention-days")?)
                }
                "--secret-rotation-days" => {
                    parsed.secret_rotation_days =
                        Some(parse_next(&mut args, "--secret-rotation-days")?)
                }
                "--tenant-max-concurrent-queries" => {
                    parsed.tenant_max_concurrent_queries =
                        Some(parse_next(&mut args, "--tenant-max-concurrent-queries")?)
                }
                "--tenant-max-result-rows" => {
                    parsed.tenant_max_result_rows =
                        Some(parse_next(&mut args, "--tenant-max-result-rows")?)
                }
                "--data-format-version" => {
                    parsed.data_format_version =
                        Some(parse_next(&mut args, "--data-format-version")?)
                }
                "--upgrade-manifest" => {
                    parsed.upgrade_manifest_path =
                        Some(PathBuf::from(next_arg(&mut args, "--upgrade-manifest")?))
                }
                "--raft-lease-clock-drift-bound-ms" => {
                    parsed.raft_lease_clock_drift_bound_ms =
                        Some(parse_next(&mut args, "--raft-lease-clock-drift-bound-ms")?)
                }
                "--raft-lease-message-delay-bound-ms" => {
                    parsed.raft_lease_message_delay_bound_ms = Some(parse_next(
                        &mut args,
                        "--raft-lease-message-delay-bound-ms",
                    )?)
                }
                "--observability-alerts" => {
                    parsed.observability_alerts_path = Some(PathBuf::from(next_arg(
                        &mut args,
                        "--observability-alerts",
                    )?))
                }
                "--repair-check-on-startup" => parsed.repair_check_on_startup = true,
                "--query-regression-corpus" => {
                    parsed.query_regression_corpus_path = Some(PathBuf::from(next_arg(
                        &mut args,
                        "--query-regression-corpus",
                    )?))
                }
                "--chaos-gate-required" => parsed.chaos_gate_required = true,
                "--runbook" => {
                    parsed.runbook_path = Some(PathBuf::from(next_arg(&mut args, "--runbook")?))
                }
                "--systemd-unit" => {
                    parsed.systemd_unit_path =
                        Some(PathBuf::from(next_arg(&mut args, "--systemd-unit")?))
                }
                "--logrotate" => {
                    parsed.logrotate_path = Some(PathBuf::from(next_arg(&mut args, "--logrotate")?))
                }
                "--daemonize" => parsed.daemonize = true,
                "--check-config" => parsed.check_config = true,
                "--production-check" => parsed.production_check = true,
                "--dump-config" => parsed.dump_config = true,
                "--help" | "-h" => return Err(usage()),
                value => return Err(format!("unknown argument: {value}\n{}", usage())),
            }
        }
        parsed.validate_runtime()?;
        let config_actions = usize::from(parsed.check_config)
            + usize::from(parsed.production_check)
            + usize::from(parsed.dump_config);
        if config_actions > 1 {
            return Err(
                "--check-config, --production-check, and --dump-config cannot be combined"
                    .to_string(),
            );
        }
        Ok(parsed)
    }

    pub(super) fn cluster_requested(&self) -> bool {
        self.primary_server_id != self.server_id
            || !self.replica_peers.is_empty()
            || !self.peers.is_empty()
            || self.replication_bind_addr.is_some()
            || self.routing_table_path.is_some()
    }

    pub(super) fn to_yaml_summary(&self) -> String {
        let mut output = String::new();
        output.push_str("server:\n");
        output.push_str(&format!("  bind: {}\n", self.bind_addr));
        output.push_str(&format!("  data_dir: {}\n", self.data_dir.display()));
        output.push_str(&format!("  server_id: {}\n", self.server_id));
        output.push_str(&format!(
            "  primary_server_id: {}\n",
            self.primary_server_id
        ));
        output.push_str(&format!("  workers: {}\n", self.worker_count));
        output.push_str(&format!("  queue_capacity: {}\n", self.queue_capacity));
        output.push_str(&format!("  page_size: {}\n", self.default_page_size));
        if let Some(path) = &self.routing_table_path {
            output.push_str(&format!("  routing_table: {}\n", path.display()));
        }
        output.push_str(&format!("  daemonize: {}\n", self.daemonize));
        output.push_str("database:\n");
        output.push_str(&format!("  shards: {}\n", self.shard_count));
        output.push_str(&format!("  partitions: {}\n", self.partition_count));
        output.push_str("replication:\n");
        if let Some(address) = &self.replication_bind_addr {
            output.push_str(&format!("  bind: {address}\n"));
        }
        output.push_str(&format!(
            "  transport: {}\n",
            format_replication_transport(self.replication_transport)
        ));
        output.push_str(&format!(
            "  ack: {}\n",
            format_ack_policy(self.replication_ack_policy)
        ));
        output.push_str(&format!(
            "  connect_timeout_ms: {}\n",
            self.replication_connect_timeout_ms
        ));
        output.push_str(&format!(
            "  retry_attempts: {}\n",
            self.replication_retry_attempts
        ));
        output.push_str(&format!(
            "  retry_backoff_ms: {}\n",
            self.replication_retry_backoff_ms
        ));
        output.push_str("  replica_peers:\n");
        for peer in &self.replica_peers {
            output.push_str(&format!(
                "    - server_id: {}\n      address: {}\n",
                peer.server_id, peer.address
            ));
        }
        output.push_str("  peers:\n");
        for peer in &self.peers {
            output.push_str(&format!(
                "    - server_id: {}\n      address: {}\n",
                peer.server_id, peer.address
            ));
        }
        output.push_str("query:\n");
        output.push_str(&format!(
            "  read_preference: {}\n",
            format_read_preference(self.read_preference)
        ));
        output.push_str("  peers:\n");
        for peer in &self.query_peers {
            output.push_str(&format!(
                "    - server_id: {}\n      address: {}\n",
                peer.server_id, peer.address
            ));
        }
        output.push_str("web:\n");
        if let Some(address) = &self.web_bind_addr {
            output.push_str(&format!("  bind: {address}\n"));
        }
        output.push_str(&format!(
            "  slow_query_threshold_ms: {}\n",
            self.slow_query_threshold_ms
        ));
        output.push_str("maintenance:\n");
        output.push_str(&format!(
            "  sync_index_catalog_on_startup: {}\n",
            self.sync_index_catalog_on_startup
        ));
        if let Some(ms) = self.sync_index_catalog_interval_ms {
            output.push_str(&format!("  sync_index_catalog_interval_ms: {ms}\n"));
        }
        output.push_str(&format!(
            "  recover_transactions_on_startup: {}\n",
            self.recover_transactions_on_startup
        ));
        if let Some(ms) = self.recover_transactions_interval_ms {
            output.push_str(&format!("  recover_transactions_interval_ms: {ms}\n"));
        }
        output.push_str("production:\n");
        output.push_str(&format!(
            "  tls_mode: {}\n",
            format_production_security_mode(self.tls_mode)
        ));
        output.push_str(&format!(
            "  replication_tls_mode: {}\n",
            format_production_security_mode(self.replication_tls_mode)
        ));
        output.push_str(&format!(
            "  web_tls_mode: {}\n",
            format_production_security_mode(self.web_tls_mode)
        ));
        if let Some(path) = &self.native_tls_cert_path {
            output.push_str(&format!("  native_tls_cert: {}\n", path.display()));
        }
        if let Some(path) = &self.native_tls_key_path {
            output.push_str(&format!("  native_tls_key: {}\n", path.display()));
        }
        if let Some(path) = &self.native_tls_client_ca_path {
            output.push_str(&format!("  native_tls_client_ca: {}\n", path.display()));
        }
        output.push_str(&format!(
            "  native_tls_require_client_auth: {}\n",
            self.native_tls_require_client_auth
        ));
        if let Some(path) = &self.replication_tls_cert_path {
            output.push_str(&format!("  replication_tls_cert: {}\n", path.display()));
        }
        if let Some(path) = &self.replication_tls_key_path {
            output.push_str(&format!("  replication_tls_key: {}\n", path.display()));
        }
        if let Some(path) = &self.replication_tls_client_ca_path {
            output.push_str(&format!(
                "  replication_tls_client_ca: {}\n",
                path.display()
            ));
        }
        output.push_str(&format!(
            "  replication_tls_require_client_auth: {}\n",
            self.replication_tls_require_client_auth
        ));
        if let Some(path) = &self.replication_tls_ca_path {
            output.push_str(&format!("  replication_tls_ca: {}\n", path.display()));
        }
        if let Some(server_name) = &self.replication_tls_server_name {
            output.push_str(&format!("  replication_tls_server_name: {server_name}\n"));
        }
        if let Some(path) = &self.replication_tls_client_cert_path {
            output.push_str(&format!(
                "  replication_tls_client_cert: {}\n",
                path.display()
            ));
        }
        if let Some(path) = &self.replication_tls_client_key_path {
            output.push_str(&format!(
                "  replication_tls_client_key: {}\n",
                path.display()
            ));
        }
        if let Some(version) = self.min_native_protocol_version {
            output.push_str(&format!("  min_native_protocol_version: {version}\n"));
        }
        if let Some(version) = self.max_native_protocol_version {
            output.push_str(&format!("  max_native_protocol_version: {version}\n"));
        }
        if let Some(hours) = self.backup_drill_max_age_hours {
            output.push_str(&format!("  backup_drill_max_age_hours: {hours}\n"));
        }
        if let Some(path) = &self.wal_archive_dir {
            output.push_str(&format!("  wal_archive_dir: {}\n", path.display()));
        }
        if let Some(path) = &self.restore_drill_manifest_path {
            output.push_str(&format!("  restore_drill_manifest: {}\n", path.display()));
        }
        if let Some(days) = self.audit_retention_days {
            output.push_str(&format!("  audit_retention_days: {days}\n"));
        }
        if let Some(days) = self.secret_rotation_days {
            output.push_str(&format!("  secret_rotation_days: {days}\n"));
        }
        if let Some(limit) = self.tenant_max_concurrent_queries {
            output.push_str(&format!("  tenant_max_concurrent_queries: {limit}\n"));
        }
        if let Some(limit) = self.tenant_max_result_rows {
            output.push_str(&format!("  tenant_max_result_rows: {limit}\n"));
        }
        if let Some(version) = self.data_format_version {
            output.push_str(&format!("  data_format_version: {version}\n"));
        }
        if let Some(path) = &self.upgrade_manifest_path {
            output.push_str(&format!("  upgrade_manifest: {}\n", path.display()));
        }
        if let Some(ms) = self.raft_lease_clock_drift_bound_ms {
            output.push_str(&format!("  raft_lease_clock_drift_bound_ms: {ms}\n"));
        }
        if let Some(ms) = self.raft_lease_message_delay_bound_ms {
            output.push_str(&format!("  raft_lease_message_delay_bound_ms: {ms}\n"));
        }
        if let Some(path) = &self.observability_alerts_path {
            output.push_str(&format!("  observability_alerts: {}\n", path.display()));
        }
        output.push_str(&format!(
            "  repair_check_on_startup: {}\n",
            self.repair_check_on_startup
        ));
        if let Some(path) = &self.query_regression_corpus_path {
            output.push_str(&format!("  query_regression_corpus: {}\n", path.display()));
        }
        output.push_str(&format!(
            "  chaos_gate_required: {}\n",
            self.chaos_gate_required
        ));
        if let Some(path) = &self.runbook_path {
            output.push_str(&format!("  runbook: {}\n", path.display()));
        }
        if let Some(path) = &self.systemd_unit_path {
            output.push_str(&format!("  systemd_unit: {}\n", path.display()));
        }
        if let Some(path) = &self.logrotate_path {
            output.push_str(&format!("  logrotate: {}\n", path.display()));
        }
        output
    }

    pub(super) fn routing_table(&self) -> Result<Option<ShardRoutingTable>, String> {
        if let Some(path) = &self.routing_table_path {
            return load_routing_table_config(path).map(Some);
        }
        let cluster_requested = self.primary_server_id != self.server_id
            || !self.replica_peers.is_empty()
            || self.replication_bind_addr.is_some();
        if !cluster_requested {
            return Ok(None);
        }
        let mut replicas = vec![ShardReplica::primary(self.primary_server_id)];
        for peer in &self.replica_peers {
            replicas.push(ShardReplica::replica(peer.server_id));
        }
        if self.server_id != self.primary_server_id
            && !replicas
                .iter()
                .any(|replica| replica.server_id == self.server_id)
        {
            replicas.push(ShardReplica::replica(self.server_id));
        }
        Ok(Some(ShardRoutingTable {
            version: 1,
            placements: (0..self.shard_count)
                .map(|shard_id| ShardPlacement::new(shard_id, replicas.clone()))
                .collect(),
        }))
    }
}

#[path = "args_production.rs"]
mod args_production;

#[path = "args_support.rs"]
mod args_support;
pub(super) use args_support::*;
