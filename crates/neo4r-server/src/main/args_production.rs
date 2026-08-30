use super::*;

impl ServerArgs {
    pub(in crate::runtime) fn validate_runtime(&self) -> Result<(), String> {
        if self.shard_count == 0 {
            return Err("--shards must be greater than zero".to_string());
        }
        if self.partition_count == 0 {
            return Err("--partitions must be greater than zero".to_string());
        }
        if self.worker_count == 0 {
            return Err("--workers must be greater than zero".to_string());
        }
        if self.queue_capacity == 0 {
            return Err("--queue-capacity must be greater than zero".to_string());
        }
        if self.default_page_size == 0 {
            return Err("--page-size must be greater than zero".to_string());
        }
        if self.primary_server_id == 0 {
            return Err("--primary-server-id must be greater than zero".to_string());
        }
        if self.replication_retry_attempts == 0 {
            return Err("--replication-retry-attempts must be greater than zero".to_string());
        }
        if self.replication_connect_timeout_ms == 0 {
            return Err("--replication-connect-timeout-ms must be greater than zero".to_string());
        }
        if self.replication_max_in_flight_batches == 0 {
            return Err(
                "--replication-max-in-flight-batches must be greater than zero".to_string(),
            );
        }
        if self.catch_up_interval_ms == Some(0) {
            return Err("--catch-up-interval-ms must be greater than zero".to_string());
        }
        if self.catch_up_batch_size == Some(0) {
            return Err("--catch-up-batch-size must be greater than zero".to_string());
        }
        if self.sync_index_catalog_interval_ms == Some(0) {
            return Err("--sync-index-catalog-interval-ms must be greater than zero".to_string());
        }
        if self.recover_transactions_interval_ms == Some(0) {
            return Err("--recover-transactions-interval-ms must be greater than zero".to_string());
        }
        if self.gossip_interval_ms == Some(0) {
            return Err("--gossip-interval-ms must be greater than zero".to_string());
        }
        if self.gossip_ttl_ms == 0 {
            return Err("--gossip-ttl-ms must be greater than zero".to_string());
        }
        if self.gossip_fanout == 0 {
            return Err("--gossip-fanout must be greater than zero".to_string());
        }
        if self.slow_query_threshold_ms == 0 {
            return Err("--slow-query-threshold-ms must be greater than zero".to_string());
        }
        if self.tls_mode == ProductionSecurityMode::Required {
            if self.native_tls_cert_path.is_none() {
                return Err("--native-tls-cert is required when --tls-mode required".to_string());
            }
            if self.native_tls_key_path.is_none() {
                return Err("--native-tls-key is required when --tls-mode required".to_string());
            }
        }
        if self.native_tls_require_client_auth && self.native_tls_client_ca_path.is_none() {
            return Err(
                "--native-tls-client-ca is required when native client auth is enabled".to_string(),
            );
        }
        if self.web_tls_mode == ProductionSecurityMode::Required {
            if self.web_tls_cert_path.is_none() {
                return Err("--web-tls-cert is required when --web-tls-mode required".to_string());
            }
            if self.web_tls_key_path.is_none() {
                return Err("--web-tls-key is required when --web-tls-mode required".to_string());
            }
        }
        if self.web_tls_require_client_auth && self.web_tls_client_ca_path.is_none() {
            return Err(
                "--web-tls-client-ca is required when web client auth is enabled".to_string(),
            );
        }
        if self.replication_tls_mode == ProductionSecurityMode::Required {
            if self.replication_transport != ReplicationChannelKind::Tcp {
                return Err(
                    "--replication-tls-mode required is only supported with tcp transport"
                        .to_string(),
                );
            }
            if self.replication_tls_cert_path.is_none() {
                return Err(
                    "--replication-tls-cert is required when --replication-tls-mode required"
                        .to_string(),
                );
            }
            if self.replication_tls_key_path.is_none() {
                return Err(
                    "--replication-tls-key is required when --replication-tls-mode required"
                        .to_string(),
                );
            }
            if self.replication_tls_ca_path.is_none() {
                return Err(
                    "--replication-tls-ca is required when --replication-tls-mode required"
                        .to_string(),
                );
            }
            if self.replication_tls_server_name.is_none() {
                return Err(
                    "--replication-tls-server-name is required when --replication-tls-mode required"
                        .to_string(),
                );
            }
        }
        if self.replication_tls_require_client_auth && self.replication_tls_client_ca_path.is_none()
        {
            return Err(
                "--replication-tls-client-ca is required when replication client auth is enabled"
                    .to_string(),
            );
        }
        if self.replication_tls_client_cert_path.is_some()
            != self.replication_tls_client_key_path.is_some()
        {
            return Err(
                "--replication-tls-client-cert and --replication-tls-client-key must be provided together"
                    .to_string(),
            );
        }
        if let (Some(min), Some(max)) = (
            self.min_native_protocol_version,
            self.max_native_protocol_version,
        ) {
            if min > max {
                return Err(
                    "--min-native-protocol-version must be <= --max-native-protocol-version"
                        .to_string(),
                );
            }
        }
        if self.backup_drill_max_age_hours == Some(0) {
            return Err("--backup-drill-max-age-hours must be greater than zero".to_string());
        }
        if self.data_format_version == Some(0) {
            return Err("--data-format-version must be greater than zero".to_string());
        }
        if self.raft_lease_clock_drift_bound_ms == Some(0) {
            return Err("--raft-lease-clock-drift-bound-ms must be greater than zero".to_string());
        }
        if self.raft_lease_message_delay_bound_ms == Some(0) {
            return Err(
                "--raft-lease-message-delay-bound-ms must be greater than zero".to_string(),
            );
        }
        if self.audit_retention_days == Some(0) {
            return Err("--audit-retention-days must be greater than zero".to_string());
        }
        if self.secret_rotation_days == Some(0) {
            return Err("--secret-rotation-days must be greater than zero".to_string());
        }
        if self.tenant_max_concurrent_queries == Some(0) {
            return Err("--tenant-max-concurrent-queries must be greater than zero".to_string());
        }
        if self.tenant_max_result_rows == Some(0) {
            return Err("--tenant-max-result-rows must be greater than zero".to_string());
        }
        for peer in &self.replica_peers {
            if peer.server_id == self.primary_server_id {
                return Err("--replica-peer cannot reference the primary server id".to_string());
            }
        }
        Ok(())
    }

    pub(in crate::runtime) fn validate_production(&self) -> Result<(), String> {
        self.validate_runtime()?;
        let mut issues = Vec::new();
        if is_local_bind(&self.bind_addr) {
            issues.push("--bind must not be loopback-only for production".to_string());
        }
        if self.data_dir.is_relative() {
            issues.push("--data-dir must be an absolute path for production".to_string());
        }
        if is_temp_or_dev_data_dir(&self.data_dir) {
            issues.push("--data-dir must not point at a temp/dev data directory".to_string());
        }
        if self.web_bind_addr.is_none() {
            issues.push("--web-bind is required for production admin/metrics access".to_string());
        }
        if let Some(address) = &self.web_bind_addr {
            if is_local_bind(address) {
                issues.push("--web-bind must not be loopback-only for production".to_string());
            }
        }
        match self.web_auth_token.as_deref() {
            Some(token) if is_strong_admin_token(token) => {}
            _ => issues.push(
                "--web-auth-token must be set to a non-default token of at least 16 bytes"
                    .to_string(),
            ),
        }
        if self.replication_ack_policy == ReplicationAckPolicy::Async {
            issues.push("--replication-ack async is not production safe".to_string());
        }
        if self.tls_mode == ProductionSecurityMode::Disabled {
            issues.push("--tls-mode must be external or required for production".to_string());
        }
        if self.web_tls_mode == ProductionSecurityMode::Disabled {
            issues.push("--web-tls-mode must be external or required for production".to_string());
        }
        if self.cluster_requested() && self.replication_tls_mode == ProductionSecurityMode::Disabled
        {
            issues.push(
                "--replication-tls-mode must be external or required for clustered production"
                    .to_string(),
            );
        }
        let supported_versions = neo4r_protocol::native_protocol_version_range();
        let supported_min = *supported_versions.start();
        let supported_max = *supported_versions.end();
        if self.min_native_protocol_version != Some(supported_min)
            || self.max_native_protocol_version != Some(supported_max)
        {
            issues.push(format!(
                "native protocol compatibility window must be explicitly pinned to {supported_min}..={supported_max}"
            ));
        }
        if self.backup_drill_max_age_hours.is_none() {
            issues.push("--backup-drill-max-age-hours is required for production".to_string());
        }
        if self.wal_archive_dir.is_none() {
            issues.push("--wal-archive-dir is required for production PITR readiness".to_string());
        }
        if self.restore_drill_manifest_path.is_none() {
            issues.push(
                "--restore-drill-manifest is required for production PITR drills".to_string(),
            );
        }
        if self.audit_retention_days.is_none() {
            issues.push("--audit-retention-days is required for production".to_string());
        }
        if self.secret_rotation_days.is_none() {
            issues.push("--secret-rotation-days is required for production".to_string());
        }
        if self.tenant_max_concurrent_queries.is_none() {
            issues.push("--tenant-max-concurrent-queries is required for production".to_string());
        }
        if self.tenant_max_result_rows.is_none() {
            issues.push("--tenant-max-result-rows is required for production".to_string());
        }
        if self.data_format_version != Some(1) {
            issues.push("--data-format-version must be pinned to 1 for this release".to_string());
        }
        if self.upgrade_manifest_path.is_none() {
            issues.push("--upgrade-manifest is required for rolling upgrade readiness".to_string());
        }
        if self.raft_lease_clock_drift_bound_ms.is_none() {
            issues
                .push("--raft-lease-clock-drift-bound-ms is required for lease reads".to_string());
        }
        if self.raft_lease_message_delay_bound_ms.is_none() {
            issues.push(
                "--raft-lease-message-delay-bound-ms is required for lease reads".to_string(),
            );
        }
        if self.observability_alerts_path.is_none() {
            issues.push("--observability-alerts is required for production".to_string());
        }
        if !self.repair_check_on_startup {
            issues.push("--repair-check-on-startup is required for production".to_string());
        }
        if self.query_regression_corpus_path.is_none() {
            issues.push("--query-regression-corpus is required for production".to_string());
        }
        if !self.chaos_gate_required {
            issues.push("--chaos-gate-required is required for production".to_string());
        }
        if self.runbook_path.is_none() {
            issues.push("--runbook is required for production operations".to_string());
        }
        if self.systemd_unit_path.is_none() {
            issues.push("--systemd-unit is required for production packaging".to_string());
        }
        if self.logrotate_path.is_none() {
            issues.push("--logrotate is required for production packaging".to_string());
        }
        if self.read_preference == QueryReadPreference::PreferReplica
            && self.query_peers.is_empty()
            && self.peers.is_empty()
        {
            issues.push("--read-preference prefer-replica requires query peers".to_string());
        }
        if self.cluster_requested() {
            if self.replication_bind_addr.is_none() {
                issues.push("--replication-bind is required for clustered production".to_string());
            }
            if self.gossip_interval_ms.is_none() {
                issues
                    .push("--gossip-interval-ms is required for clustered production".to_string());
            }
            if self.gossip_seed_peers.is_empty() {
                issues.push("--gossip-seed-peer is required for clustered production".to_string());
            }
            match self.gossip_auth_token.as_deref() {
                Some(token) if is_strong_admin_token(token) => {}
                _ => issues.push(
                    "--gossip-auth-token must be set to a non-default token of at least 16 bytes"
                        .to_string(),
                ),
            }
            if self.replica_peers.is_empty() && self.peers.is_empty() {
                issues.push(
                    "clustered production requires at least one replication peer".to_string(),
                );
            }
            if !self.catch_up_on_startup {
                issues
                    .push("--catch-up-on-startup is required for clustered production".to_string());
            }
            if self.catch_up_interval_ms.is_none() {
                issues.push(
                    "--catch-up-interval-ms is required for clustered production".to_string(),
                );
            }
            if self.catch_up_batch_size.is_none() {
                issues
                    .push("--catch-up-batch-size is required for clustered production".to_string());
            }
        }
        if !issues.is_empty() {
            return Err(format!(
                "production check failed:\n- {}",
                issues.join("\n- ")
            ));
        }
        Ok(())
    }

    pub(in crate::runtime) fn native_tls_config(&self) -> Result<Option<NativeTlsConfig>, String> {
        if self.tls_mode != ProductionSecurityMode::Required {
            return Ok(None);
        }
        Ok(Some(NativeTlsConfig {
            cert_path: self
                .native_tls_cert_path
                .clone()
                .ok_or("--native-tls-cert is required when --tls-mode required")?,
            key_path: self
                .native_tls_key_path
                .clone()
                .ok_or("--native-tls-key is required when --tls-mode required")?,
            client_ca_path: self.native_tls_client_ca_path.clone(),
            require_client_auth: self.native_tls_require_client_auth,
        }))
    }

    pub(in crate::runtime) fn replication_tls_acceptor_config(
        &self,
    ) -> Result<Option<NativeTlsConfig>, String> {
        if self.replication_tls_mode != ProductionSecurityMode::Required {
            return Ok(None);
        }
        Ok(Some(NativeTlsConfig {
            cert_path: self
                .replication_tls_cert_path
                .clone()
                .ok_or("--replication-tls-cert is required when --replication-tls-mode required")?,
            key_path: self
                .replication_tls_key_path
                .clone()
                .ok_or("--replication-tls-key is required when --replication-tls-mode required")?,
            client_ca_path: self.replication_tls_client_ca_path.clone(),
            require_client_auth: self.replication_tls_require_client_auth,
        }))
    }

    pub(in crate::runtime) fn web_tls_config(&self) -> Result<Option<NativeTlsConfig>, String> {
        if self.web_tls_mode != ProductionSecurityMode::Required {
            return Ok(None);
        }
        Ok(Some(NativeTlsConfig {
            cert_path: self
                .web_tls_cert_path
                .clone()
                .ok_or("--web-tls-cert is required when --web-tls-mode required")?,
            key_path: self
                .web_tls_key_path
                .clone()
                .ok_or("--web-tls-key is required when --web-tls-mode required")?,
            client_ca_path: self.web_tls_client_ca_path.clone(),
            require_client_auth: self.web_tls_require_client_auth,
        }))
    }

    pub(in crate::runtime) fn replication_tls_channel_config(
        &self,
    ) -> Result<Option<ReplicationTlsConfig>, String> {
        if self.replication_tls_mode != ProductionSecurityMode::Required {
            return Ok(None);
        }
        Ok(Some(ReplicationTlsConfig {
            server_name: self
                .replication_tls_server_name
                .clone()
                .unwrap_or_else(|| "localhost".to_string()),
            ca_cert_path: self
                .replication_tls_ca_path
                .clone()
                .ok_or("--replication-tls-ca is required when --replication-tls-mode required")?,
            client_cert_path: self.replication_tls_client_cert_path.clone(),
            client_key_path: self.replication_tls_client_key_path.clone(),
        }))
    }
}
