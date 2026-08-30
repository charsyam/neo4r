use super::*;

pub(crate) struct WebMetricsSnapshot {
    pub(crate) http_requests: u64,
    pub(crate) http_errors: u64,
    pub(crate) auth_failures: u64,
    pub(crate) auth_rate_limited: u64,
    pub(crate) queries: u64,
    pub(crate) query_errors: u64,
    pub(crate) slow_queries: u64,
    pub(crate) slow_query_threshold_ms: u128,
    pub(crate) registry_requests: u64,
    pub(crate) stale_epoch_rejections: u64,
    pub(crate) redirects: u64,
    pub(crate) migration_state: String,
    pub(crate) db_nodes: usize,
    pub(crate) db_relationships: usize,
    pub(crate) db_indexes: usize,
    pub(crate) db_vector_indexes: usize,
    pub(crate) db_shard_count: u64,
    pub(crate) db_local_partition_count: usize,
    pub(crate) db_committed_indexes: Vec<u64>,
    pub(crate) db_applied_indexes: Vec<u64>,
    pub(crate) tenant_database_count: usize,
    pub(crate) tenant_disabled_count: usize,
    pub(crate) index_ready_count: usize,
    pub(crate) index_building_count: usize,
    pub(crate) index_rebuilding_count: usize,
    pub(crate) index_failed_count: usize,
    pub(crate) raft_group_count: usize,
    pub(crate) raft_leader_count: usize,
    pub(crate) raft_term_max: u64,
    pub(crate) raft_snapshot_index_max: u64,
    pub(crate) raft_joint_consensus_count: usize,
    pub(crate) web_user_token_count: usize,
    pub(crate) web_audit_event_count: usize,
    pub(crate) replication_sent_batches: usize,
    pub(crate) replication_acked_batches: usize,
    pub(crate) replication_failed_batches: usize,
    pub(crate) replication_sent_entries: usize,
    pub(crate) replication_sent_bytes: u64,
    pub(crate) raft_election_rounds: usize,
    pub(crate) raft_append_conflicts: usize,
    pub(crate) raft_snapshot_installs: usize,
    pub(crate) raft_snapshot_install_millis: u64,
    pub(crate) query_plan_cost_model_version: u64,
    pub(crate) backup_restore_last_success_timestamp_seconds: u64,
    pub(crate) storage_repair_last_success_timestamp_seconds: u64,
    pub(crate) storage_repair_failures: u64,
}

impl WebMetricsSnapshot {
    pub(crate) fn to_json(&self) -> String {
        format!(
            "{{\"http_requests\":{},\"http_errors\":{},\"auth_failures\":{},\"auth_rate_limited\":{},\"queries\":{},\"query_errors\":{},\"slow_queries\":{},\"slow_query_threshold_ms\":{},\"registry_requests\":{},\"stale_epoch_rejections\":{},\"redirects\":{},\"migration_state\":\"{}\",\"db_nodes\":{},\"db_relationships\":{},\"db_indexes\":{},\"db_vector_indexes\":{},\"db_shard_count\":{},\"db_local_partition_count\":{},\"db_committed_indexes\":[{}],\"db_applied_indexes\":[{}],\"tenant_database_count\":{},\"tenant_disabled_count\":{},\"index_ready_count\":{},\"index_building_count\":{},\"index_rebuilding_count\":{},\"index_failed_count\":{},\"raft_group_count\":{},\"raft_leader_count\":{},\"raft_term_max\":{},\"raft_snapshot_index_max\":{},\"raft_joint_consensus_count\":{},\"web_user_token_count\":{},\"web_audit_event_count\":{},\"replication_sent_batches\":{},\"replication_acked_batches\":{},\"replication_failed_batches\":{},\"replication_sent_entries\":{},\"replication_sent_bytes\":{},\"raft_election_rounds\":{},\"raft_append_conflicts\":{},\"raft_snapshot_installs\":{},\"raft_snapshot_install_millis\":{},\"query_plan_cost_model_version\":{},\"backup_restore_last_success_timestamp_seconds\":{},\"storage_repair_last_success_timestamp_seconds\":{},\"storage_repair_failures\":{}}}",
            self.http_requests,
            self.http_errors,
            self.auth_failures,
            self.auth_rate_limited,
            self.queries,
            self.query_errors,
            self.slow_queries,
            self.slow_query_threshold_ms,
            self.registry_requests,
            self.stale_epoch_rejections,
            self.redirects,
            json_escape(&self.migration_state),
            self.db_nodes,
            self.db_relationships,
            self.db_indexes,
            self.db_vector_indexes,
            self.db_shard_count,
            self.db_local_partition_count,
            self.db_committed_indexes
                .iter()
                .map(u64::to_string)
                .collect::<Vec<_>>()
                .join(","),
            self.db_applied_indexes
                .iter()
                .map(u64::to_string)
                .collect::<Vec<_>>()
                .join(","),
            self.tenant_database_count,
            self.tenant_disabled_count,
            self.index_ready_count,
            self.index_building_count,
            self.index_rebuilding_count,
            self.index_failed_count,
            self.raft_group_count,
            self.raft_leader_count,
            self.raft_term_max,
            self.raft_snapshot_index_max,
            self.raft_joint_consensus_count,
            self.web_user_token_count,
            self.web_audit_event_count,
            self.replication_sent_batches,
            self.replication_acked_batches,
            self.replication_failed_batches,
            self.replication_sent_entries,
            self.replication_sent_bytes,
            self.raft_election_rounds,
            self.raft_append_conflicts,
            self.raft_snapshot_installs,
            self.raft_snapshot_install_millis,
            self.query_plan_cost_model_version,
            self.backup_restore_last_success_timestamp_seconds,
            self.storage_repair_last_success_timestamp_seconds,
            self.storage_repair_failures
        )
    }

    pub(crate) fn to_prometheus(&self, database_name: &str, server_id: u64) -> String {
        let committed_max = self
            .db_committed_indexes
            .iter()
            .copied()
            .max()
            .unwrap_or_default();
        let applied_max = self
            .db_applied_indexes
            .iter()
            .copied()
            .max()
            .unwrap_or_default();
        let mut metrics = [
            prometheus_metric("neo4r_http_requests_total", self.http_requests),
            prometheus_metric("neo4r_http_errors_total", self.http_errors),
            prometheus_metric("neo4r_auth_failures_total", self.auth_failures),
            prometheus_metric("neo4r_auth_rate_limited_total", self.auth_rate_limited),
            prometheus_metric("neo4r_queries_total", self.queries),
            prometheus_metric("neo4r_query_errors_total", self.query_errors),
            prometheus_metric("neo4r_slow_queries_total", self.slow_queries),
            prometheus_metric(
                "neo4r_slow_query_threshold_ms",
                self.slow_query_threshold_ms as u64,
            ),
            prometheus_metric("neo4r_registry_requests_total", self.registry_requests),
            prometheus_metric(
                "neo4r_stale_epoch_rejections_total",
                self.stale_epoch_rejections,
            ),
            prometheus_metric("neo4r_redirects_total", self.redirects),
            prometheus_metric("neo4r_db_nodes", self.db_nodes as u64),
            prometheus_metric("neo4r_db_relationships", self.db_relationships as u64),
            prometheus_metric("neo4r_db_indexes", self.db_indexes as u64),
            prometheus_metric("neo4r_db_vector_indexes", self.db_vector_indexes as u64),
            prometheus_metric("neo4r_db_shards", self.db_shard_count),
            prometheus_metric(
                "neo4r_db_local_partitions",
                self.db_local_partition_count as u64,
            ),
            prometheus_metric("neo4r_db_committed_index_max", committed_max),
            prometheus_metric("neo4r_db_applied_index_max", applied_max),
            prometheus_metric("neo4r_tenant_databases", self.tenant_database_count as u64),
            prometheus_metric(
                "neo4r_tenant_disabled_databases",
                self.tenant_disabled_count as u64,
            ),
            prometheus_metric("neo4r_index_ready", self.index_ready_count as u64),
            prometheus_metric("neo4r_index_building", self.index_building_count as u64),
            prometheus_metric("neo4r_index_rebuilding", self.index_rebuilding_count as u64),
            prometheus_metric("neo4r_index_failed", self.index_failed_count as u64),
            prometheus_metric("neo4r_raft_groups", self.raft_group_count as u64),
            prometheus_metric("neo4r_raft_leaders", self.raft_leader_count as u64),
            prometheus_metric("neo4r_raft_term_max", self.raft_term_max),
            prometheus_metric(
                "neo4r_raft_snapshot_index_max",
                self.raft_snapshot_index_max,
            ),
            prometheus_metric(
                "neo4r_raft_joint_consensus_groups",
                self.raft_joint_consensus_count as u64,
            ),
            prometheus_metric("neo4r_web_user_tokens", self.web_user_token_count as u64),
            prometheus_metric("neo4r_web_audit_events", self.web_audit_event_count as u64),
            prometheus_metric(
                "neo4r_replication_channel_sent_batches_total",
                self.replication_sent_batches as u64,
            ),
            prometheus_metric(
                "neo4r_replication_channel_acked_batches_total",
                self.replication_acked_batches as u64,
            ),
            prometheus_metric(
                "neo4r_replication_channel_failed_batches_total",
                self.replication_failed_batches as u64,
            ),
            prometheus_metric(
                "neo4r_replication_channel_sent_entries_total",
                self.replication_sent_entries as u64,
            ),
            prometheus_metric(
                "neo4r_replication_channel_sent_bytes_total",
                self.replication_sent_bytes,
            ),
            prometheus_metric(
                "neo4r_raft_election_rounds_total",
                self.raft_election_rounds as u64,
            ),
            prometheus_metric(
                "neo4r_raft_append_conflicts_total",
                self.raft_append_conflicts as u64,
            ),
            prometheus_metric(
                "neo4r_raft_snapshot_installs_total",
                self.raft_snapshot_installs as u64,
            ),
            prometheus_metric(
                "neo4r_raft_snapshot_install_duration_ms_total",
                self.raft_snapshot_install_millis,
            ),
            prometheus_metric(
                "neo4r_query_plan_cost_model_version",
                self.query_plan_cost_model_version,
            ),
            prometheus_metric(
                "neo4r_backup_restore_last_success_timestamp_seconds",
                self.backup_restore_last_success_timestamp_seconds,
            ),
            prometheus_metric(
                "neo4r_storage_repair_last_success_timestamp_seconds",
                self.storage_repair_last_success_timestamp_seconds,
            ),
            prometheus_metric(
                "neo4r_storage_repair_failures_total",
                self.storage_repair_failures,
            ),
        ]
        .join("");
        metrics.push_str(&prometheus_database_metric(
            "neo4r_database_db_nodes",
            database_name,
            self.db_nodes as u64,
        ));
        metrics.push_str(&prometheus_database_metric(
            "neo4r_database_db_relationships",
            database_name,
            self.db_relationships as u64,
        ));
        metrics.push_str(&prometheus_database_metric(
            "neo4r_database_committed_index_max",
            database_name,
            committed_max,
        ));
        metrics.push_str(&prometheus_database_metric(
            "neo4r_database_applied_index_max",
            database_name,
            applied_max,
        ));
        metrics.push_str(&prometheus_database_metric(
            "neo4r_database_raft_groups",
            database_name,
            self.raft_group_count as u64,
        ));
        for (shard_id, committed_index) in self.db_committed_indexes.iter().copied().enumerate() {
            metrics.push_str(&prometheus_shard_metric(
                "neo4r_database_shard_committed_index",
                database_name,
                shard_id as u64,
                server_id,
                "unknown",
                committed_index,
            ));
        }
        for (shard_id, applied_index) in self.db_applied_indexes.iter().copied().enumerate() {
            metrics.push_str(&prometheus_shard_metric(
                "neo4r_database_shard_applied_index",
                database_name,
                shard_id as u64,
                server_id,
                "unknown",
                applied_index,
            ));
        }
        for shard_id in 0..self
            .db_committed_indexes
            .len()
            .max(self.db_applied_indexes.len())
        {
            let committed = self
                .db_committed_indexes
                .get(shard_id)
                .copied()
                .unwrap_or_default();
            let applied = self
                .db_applied_indexes
                .get(shard_id)
                .copied()
                .unwrap_or_default();
            metrics.push_str(&prometheus_shard_metric(
                "neo4r_database_shard_lag",
                database_name,
                shard_id as u64,
                server_id,
                "unknown",
                committed.saturating_sub(applied),
            ));
        }
        metrics
    }
}

impl TcpBackend {
    pub(crate) fn graph_json(
        &self,
        db: &Neo4rDatabaseHandle,
        limit: Option<String>,
    ) -> Result<String, String> {
        let limit = limit
            .as_deref()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(1000);
        let node_rows = db
            .query("MATCH (n) RETURN n")
            .map_err(|err| err.to_string())?;
        let rel_rows = db.query("MATCH (a)-[r]->(b) RETURN r").unwrap_or_default();

        let mut nodes = Vec::new();
        let mut seen_nodes = BTreeSet::new();
        for row in node_rows.iter().take(limit) {
            if let Some(QueryValue::Node(node)) = row.get("n") {
                if seen_nodes.insert(node.id) {
                    nodes.push(format!(
                        "{{\"id\":{},\"labels\":{},\"properties\":{}}}",
                        node.id,
                        string_array_json(&node.labels),
                        properties_json(&node.properties)
                    ));
                }
            }
        }

        let mut relationships = Vec::new();
        for row in rel_rows.iter().take(limit) {
            if let Some(QueryValue::Relationship(relationship)) = row.get("r") {
                relationships.push(format!(
                    "{{\"id\":{},\"from\":{},\"to\":{},\"type\":\"{}\",\"properties\":{}}}",
                    relationship.id,
                    relationship.from,
                    relationship.to,
                    json_escape(&relationship.rel_type),
                    properties_json(&relationship.properties)
                ));
            }
        }

        Ok(format!(
            "{{\"nodes\":[{}],\"relationships\":[{}]}}",
            nodes.join(","),
            relationships.join(",")
        ))
    }

    pub(crate) fn query_json(
        &self,
        db: &Neo4rDatabaseHandle,
        database_name: &str,
        query: &str,
        params: QueryParams,
        options: QueryOptions,
    ) -> Result<String, String> {
        self.metrics.queries.fetch_add(1, Ordering::Relaxed);
        let _quota_permit = self.tenant_quota.acquire_query(database_name)?;
        let started = Instant::now();
        let rows = match if is_write_cypher(query) {
            db.execute_cypher_with_params(query, params)
        } else {
            db.query_with_params_and_options(query, params, options)
        } {
            Ok(rows) => rows,
            Err(err) => {
                self.metrics.query_errors.fetch_add(1, Ordering::Relaxed);
                return Err(err.to_string());
            }
        };
        if let Err(err) = self
            .tenant_quota
            .validate_result_rows(database_name, rows.len())
        {
            self.metrics.query_errors.fetch_add(1, Ordering::Relaxed);
            return Err(err);
        }
        self.audit_query_spill_if_needed(database_name, &rows);
        let elapsed = started.elapsed();
        if elapsed >= self.slow_query_threshold {
            self.record_slow_query(query, elapsed);
        }
        let columns = query_columns_json(&rows);
        Ok(format!(
            "{{\"columns\":{},\"rows\":[{}],\"plan\":null,\"database\":\"{}\"}}",
            columns,
            rows.iter()
                .map(query_row_json)
                .collect::<Vec<_>>()
                .join(","),
            json_escape(database_name)
        ))
    }

    pub(crate) fn query_plan_json(
        &self,
        db: &Neo4rDatabaseHandle,
        query: &str,
        params: QueryParams,
    ) -> Result<String, String> {
        let plan = db
            .query_plan_with_params(query, params)
            .map_err(|err| err.to_string())?;
        Ok(format!(
            "{{\"plan\":\"{}\",\"explain\":{{\"cost_model_version\":{},\"estimated_cost\":{},\"estimated_rows\":{},\"remote_shard_count\":{},\"selectivity_estimate\":{:.6}}}}}",
            json_escape(&format_query_plan(&plan)),
            plan.cost_model_version,
            plan.estimated_cost,
            plan.estimated_rows,
            plan.remote_shard_count,
            selectivity_estimate(plan.estimated_rows, plan.estimated_cost)
        ))
    }

    pub(crate) fn profile_json(
        &self,
        db: &Neo4rDatabaseHandle,
        query: &str,
        params: QueryParams,
    ) -> Result<String, String> {
        let response = execute_request(
            db,
            BackendRequest::Profile {
                query: query.to_string(),
                params,
            },
        );
        Ok(management_response_json(&response))
    }

    pub(crate) fn raft_status_json(&self, db: &Neo4rDatabaseHandle) -> String {
        let shards = db.raft_status().unwrap_or_default();
        let shards_json = shards
            .iter()
            .map(|shard| {
                format!(
                    "{{\"shard_id\":{},\"term\":{},\"role\":\"{:?}\",\"leader_id\":{},\"commit_index\":{},\"last_log_index\":{},\"snapshot_index\":{},\"joint_consensus\":{}}}",
                    shard.shard_id,
                    shard.term,
                    shard.role,
                    shard
                        .leader_id
                        .map(|leader| leader.to_string())
                        .unwrap_or_else(|| "null".to_string()),
                    shard.commit_index,
                    shard.last_log_index,
                    shard.snapshot_index,
                    shard.joint_consensus
                )
            })
            .collect::<Vec<_>>()
            .join(",");
        format!("{{\"raft_shards\":[{shards_json}]}}")
    }

    pub(crate) fn routing_table_json(db: &Neo4rDatabaseHandle) -> String {
        let Ok(routing_table) = db.routing_table() else {
            return "{\"version\":0,\"ownership_epoch\":0,\"shards\":[]}".to_string();
        };
        let shards = routing_table
            .placements
            .iter()
            .map(|placement| {
                let replicas = placement
                    .replicas
                    .iter()
                    .map(|replica| {
                        let role = match replica.role {
                            ShardRole::Primary => "primary",
                            ShardRole::Replica => "replica",
                        };
                        format!(
                            "{{\"server_id\":{},\"role\":\"{}\"}}",
                            replica.server_id, role
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(",");
                format!(
                    "{{\"shard_id\":{},\"primary_server_id\":{},\"replicas\":[{}]}}",
                    placement.shard_id,
                    placement
                        .primary_server_id()
                        .map(|server_id| server_id.to_string())
                        .unwrap_or_else(|| "null".to_string()),
                    replicas
                )
            })
            .collect::<Vec<_>>()
            .join(",");
        format!(
            "{{\"version\":{},\"ownership_epoch\":{},\"shards\":[{}]}}",
            routing_table.version, routing_table.version, shards
        )
    }

    pub(crate) fn cluster_registry_json(
        &self,
        db: &Neo4rDatabaseHandle,
        database_name: &str,
    ) -> String {
        let status = db.cluster_status().ok();
        let generated_at_ms = unix_millis_now();
        let ttl_ms = 5_000_u64;
        let management = db.cluster_management_status().ok();
        let membership_index = management
            .as_ref()
            .map(|management| management.membership.version)
            .unwrap_or_default();
        let metadata_index = db
            .metadata_operations()
            .map(|records| {
                records
                    .last()
                    .map(|record| record.index)
                    .unwrap_or_default()
            })
            .unwrap_or_default();
        let migration_state = management
            .as_ref()
            .and_then(|management| management.rebalance_execution.as_ref())
            .map(|execution| format!("{:?}", execution.state))
            .unwrap_or_else(|| "idle".to_string());
        let raft_shards_json = db
            .raft_status()
            .unwrap_or_default()
            .iter()
            .map(|shard| {
                format!(
                    "{{\"shard_id\":{},\"term\":{},\"role\":\"{:?}\",\"leader_id\":{}}}",
                    shard.shard_id,
                    shard.term,
                    shard.role,
                    shard
                        .leader_id
                        .map(|leader| leader.to_string())
                        .unwrap_or_else(|| "null".to_string())
                )
            })
            .collect::<Vec<_>>()
            .join(",");
        let peers = self.query_peers.list().unwrap_or_default();
        let peers_json = peers
            .iter()
            .map(|(server_id, address)| {
                format!(
                    "{{\"server_id\":{},\"address\":\"{}\"}}",
                    server_id,
                    json_escape(address)
                )
            })
            .collect::<Vec<_>>()
            .join(",");
        let routing_json = Self::routing_table_json(db);
        let routing_version = db
            .routing_table()
            .map(|routing_table| routing_table.version)
            .unwrap_or_default();
        format!(
            "{{\"database\":\"{}\",\"local_server_id\":{},\"routing_version\":{},\"ownership_epoch\":{},\"membership_index\":{},\"metadata_index\":{},\"generated_at_ms\":{},\"ttl_ms\":{},\"migration_state\":\"{}\",\"write_authority\":\"shard_primary_and_raft_leader\",\"routing\":{},\"raft_shards\":[{}],\"query_peers\":[{}]}}",
            json_escape(database_name),
            status.map(|status| status.server_id).unwrap_or_default(),
            routing_version,
            routing_version,
            membership_index,
            metadata_index,
            generated_at_ms,
            ttl_ms,
            json_escape(&migration_state),
            routing_json,
            raft_shards_json,
            peers_json
        )
    }

    pub(crate) fn capabilities_json(&self) -> String {
        let capabilities = format_protocol_capabilities()
            .split_whitespace()
            .filter_map(|part| part.split_once('='))
            .map(|(key, value)| format!("\"{}\":\"{}\"", json_escape(key), json_escape(value)))
            .collect::<Vec<_>>()
            .join(",");
        format!("{{\"capabilities\":{{{capabilities}}}}}")
    }

    pub(crate) fn metrics_json(&self, db: &Neo4rDatabaseHandle) -> String {
        let metrics = self.metrics_snapshot(db);
        metrics.to_json()
    }

    pub(crate) fn metrics_prometheus(
        &self,
        db: &Neo4rDatabaseHandle,
        database_name: &str,
    ) -> String {
        let metrics = self.metrics_snapshot(db);
        let server_id = db
            .cluster_status()
            .map(|status| status.server_id)
            .unwrap_or_default();
        let mut output = metrics.to_prometheus(database_name, server_id);
        for status in db.raft_status().unwrap_or_default() {
            output.push_str(&prometheus_shard_metric(
                "neo4r_raft_shard_commit_index",
                database_name,
                status.shard_id,
                server_id,
                &format!("{:?}", status.role),
                status.commit_index,
            ));
            output.push_str(&prometheus_shard_metric(
                "neo4r_raft_shard_last_log_index",
                database_name,
                status.shard_id,
                server_id,
                &format!("{:?}", status.role),
                status.last_log_index,
            ));
            output.push_str(&prometheus_shard_metric(
                "neo4r_raft_shard_snapshot_index",
                database_name,
                status.shard_id,
                server_id,
                &format!("{:?}", status.role),
                status.snapshot_index,
            ));
            output.push_str(&prometheus_shard_metric(
                "neo4r_raft_shard_leader_lease_remaining_ms",
                database_name,
                status.shard_id,
                server_id,
                &format!("{:?}", status.role),
                status.leader_lease_remaining_ms,
            ));
        }
        output
    }

    pub(crate) fn metrics_snapshot(&self, db: &Neo4rDatabaseHandle) -> WebMetricsSnapshot {
        let statistics = db.statistics_catalog().ok();
        let committed_indexes = db.committed_indexes().unwrap_or_default();
        let applied_indexes = db
            .read_snapshot()
            .map(|snapshot| snapshot.applied_indexes().to_vec())
            .unwrap_or_default();
        let index_lifecycle = db.index_lifecycle_status().unwrap_or_default();
        let index_ready_count = index_lifecycle
            .iter()
            .filter(|status| status.state == "ready")
            .count();
        let index_building_count = index_lifecycle
            .iter()
            .filter(|status| status.state == "building")
            .count();
        let index_rebuilding_count = index_lifecycle
            .iter()
            .filter(|status| status.state == "rebuilding")
            .count();
        let index_failed_count = index_lifecycle
            .iter()
            .filter(|status| status.state == "failed")
            .count();
        let tenant_records = self
            .tenant_databases
            .as_ref()
            .and_then(|manager| manager.list_database_records().ok());
        let tenant_database_count = tenant_records
            .as_ref()
            .map(|records| records.len())
            .unwrap_or(1);
        let tenant_disabled_count = tenant_records
            .as_ref()
            .map(|records| records.iter().filter(|record| record.disabled).count())
            .unwrap_or(0);
        let raft_status = db.raft_status().unwrap_or_default();
        let raft_group_count = raft_status.len();
        let raft_leader_count = raft_status
            .iter()
            .filter(|status| format!("{:?}", status.role) == "Leader")
            .count();
        let raft_term_max = raft_status
            .iter()
            .map(|status| status.term)
            .max()
            .unwrap_or_default();
        let raft_snapshot_index_max = raft_status
            .iter()
            .map(|status| status.snapshot_index)
            .max()
            .unwrap_or_default();
        let raft_joint_consensus_count = raft_status
            .iter()
            .filter(|status| status.joint_consensus)
            .count();
        let migration_state = db
            .cluster_management_status()
            .ok()
            .and_then(|status| status.rebalance_execution)
            .map(|execution| format!("{:?}", execution.state))
            .unwrap_or_else(|| "idle".to_string());
        let web_user_token_count = self
            .web_user_tokens
            .as_ref()
            .and_then(|store| store.list().ok())
            .map(|tokens| tokens.len())
            .unwrap_or_default();
        let audit_events = self
            .web_audit
            .as_ref()
            .and_then(|store| store.list().ok())
            .unwrap_or_default();
        let web_audit_event_count = audit_events.len();
        let latest_backup_or_restore =
            latest_audit_unix_seconds(&audit_events, &["backup.", "restore."]);
        let latest_repair = latest_audit_unix_seconds(&audit_events, &["repair."]);
        let repair_failures = audit_events
            .iter()
            .filter(|event| event.action == "repair.failure")
            .count() as u64;
        let replication = db.replication_channel_metrics().ok().flatten();
        WebMetricsSnapshot {
            http_requests: self.metrics.http_requests.load(Ordering::Relaxed),
            http_errors: self.metrics.http_errors.load(Ordering::Relaxed),
            auth_failures: self.metrics.auth_failures.load(Ordering::Relaxed),
            auth_rate_limited: self.metrics.auth_rate_limited.load(Ordering::Relaxed),
            queries: self.metrics.queries.load(Ordering::Relaxed),
            query_errors: self.metrics.query_errors.load(Ordering::Relaxed),
            slow_queries: self.metrics.slow_queries.load(Ordering::Relaxed),
            slow_query_threshold_ms: self.slow_query_threshold.as_millis(),
            registry_requests: self.metrics.registry_requests.load(Ordering::Relaxed),
            stale_epoch_rejections: self.metrics.stale_epoch_rejections.load(Ordering::Relaxed),
            redirects: self.metrics.redirects.load(Ordering::Relaxed),
            migration_state,
            db_nodes: statistics
                .as_ref()
                .map(|statistics| statistics.node_count)
                .unwrap_or_default(),
            db_relationships: statistics
                .as_ref()
                .map(|statistics| statistics.relationship_count)
                .unwrap_or_default(),
            db_indexes: statistics
                .as_ref()
                .map(|statistics| statistics.index_count)
                .unwrap_or_default(),
            db_vector_indexes: statistics
                .as_ref()
                .map(|statistics| statistics.vector_index_count)
                .unwrap_or_default(),
            db_shard_count: db.shard_count().unwrap_or_default(),
            db_local_partition_count: db.local_partition_count().unwrap_or_default(),
            db_committed_indexes: committed_indexes,
            db_applied_indexes: applied_indexes,
            tenant_database_count,
            tenant_disabled_count,
            index_ready_count,
            index_building_count,
            index_rebuilding_count,
            index_failed_count,
            raft_group_count,
            raft_leader_count,
            raft_term_max,
            raft_snapshot_index_max,
            raft_joint_consensus_count,
            web_user_token_count,
            web_audit_event_count,
            replication_sent_batches: replication
                .as_ref()
                .map(|metrics| metrics.sent_batches)
                .unwrap_or_default(),
            replication_acked_batches: replication
                .as_ref()
                .map(|metrics| metrics.acked_batches)
                .unwrap_or_default(),
            replication_failed_batches: replication
                .as_ref()
                .map(|metrics| metrics.failed_batches)
                .unwrap_or_default(),
            replication_sent_entries: replication
                .as_ref()
                .map(|metrics| metrics.sent_entries)
                .unwrap_or_default(),
            replication_sent_bytes: replication
                .as_ref()
                .map(|metrics| metrics.sent_bytes)
                .unwrap_or_default(),
            raft_election_rounds: replication
                .as_ref()
                .map(|metrics| metrics.election_rounds)
                .unwrap_or_default(),
            raft_append_conflicts: replication
                .as_ref()
                .map(|metrics| metrics.append_conflicts)
                .unwrap_or_default(),
            raft_snapshot_installs: replication
                .as_ref()
                .map(|metrics| metrics.snapshot_installs)
                .unwrap_or_default(),
            raft_snapshot_install_millis: replication
                .as_ref()
                .map(|metrics| metrics.snapshot_install_millis)
                .unwrap_or_default(),
            query_plan_cost_model_version: 3,
            backup_restore_last_success_timestamp_seconds: latest_backup_or_restore,
            storage_repair_last_success_timestamp_seconds: latest_repair,
            storage_repair_failures: repair_failures,
        }
    }

    pub(crate) fn record_slow_query(&self, query: &str, elapsed: Duration) {
        self.metrics.slow_queries.fetch_add(1, Ordering::Relaxed);
        let unix_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_millis())
            .unwrap_or(0);
        self.slow_queries.push(SlowQueryEntry {
            unix_ms,
            elapsed_ms: elapsed.as_millis(),
            query: query.to_string(),
        });
    }

    pub(crate) fn slow_queries_json(&self) -> String {
        let entries = self.slow_queries.entries();
        format!(
            "{{\"queries\":[{}]}}",
            entries
                .iter()
                .map(|entry| format!(
                    "{{\"unix_ms\":{},\"elapsed_ms\":{},\"query\":\"{}\"}}",
                    entry.unix_ms,
                    entry.elapsed_ms,
                    json_escape(&entry.query)
                ))
                .collect::<Vec<_>>()
                .join(",")
        )
    }

    pub(crate) fn backup_to_path(
        &self,
        db: &Neo4rDatabaseHandle,
        database_name: &str,
        path: &str,
    ) -> Result<String, String> {
        let source = db.data_dir().map_err(|err| err.to_string())?;
        let target = PathBuf::from(path);
        copy_dir_all(&source, &target).map_err(|err| err.to_string())?;
        let stats = collect_backup_manifest_stats(&target).map_err(|err| err.to_string())?;
        let commit_indexes = db.committed_indexes().map_err(|err| err.to_string())?;
        let commit_marker = commit_indexes
            .iter()
            .map(|index| index.to_string())
            .collect::<Vec<_>>()
            .join(",");
        let manifest = format!(
            "neo4r_backup_manifest_version=1\ndatabase={}\nsource={}\ntarget={}\nfile_count={}\ntotal_bytes={}\nchecksum={}\ncommit_indexes={}\n",
            database_name,
            source.display(),
            target.display(),
            stats.file_count,
            stats.total_bytes,
            stats.checksum,
            commit_marker
        );
        fs::write(target.join(BACKUP_MANIFEST_FILE), manifest).map_err(|err| err.to_string())?;
        self.audit_admin(
            "backup.create",
            database_name,
            &format!("target={} commit_indexes={commit_marker}", target.display()),
        );
        Ok(format!(
            "{{\"source\":\"{}\",\"target\":\"{}\",\"manifest\":\"{}\",\"file_count\":{},\"total_bytes\":{},\"checksum\":{},\"commit_indexes\":[{}]}}",
            json_escape(&source.display().to_string()),
            json_escape(&target.display().to_string()),
            json_escape(BACKUP_MANIFEST_FILE),
            stats.file_count,
            stats.total_bytes,
            stats.checksum,
            commit_marker
        ))
    }

    pub(crate) fn restore_from_path(
        &self,
        db: &Neo4rDatabaseHandle,
        database_name: &str,
        path: &str,
        dry_run: bool,
    ) -> Result<String, String> {
        let source = PathBuf::from(path);
        if !source.join(BACKUP_MANIFEST_FILE).is_file() {
            return Err(format!(
                "backup manifest {} is missing",
                source.join(BACKUP_MANIFEST_FILE).display()
            ));
        }
        let stats = collect_backup_manifest_stats(&source).map_err(|err| err.to_string())?;
        verify_backup_manifest(&source, &stats, database_name).map_err(|err| err.to_string())?;
        let target = db.data_dir().map_err(|err| err.to_string())?;
        if !dry_run && !self.restore_maintenance_mode_enabled(db)? {
            return Err(
                "destructive restore requires maintenance mode before data files are replaced"
                    .to_string(),
            );
        }
        let _lock = if dry_run {
            None
        } else {
            Some(RestoreLock::acquire(&target)?)
        };
        if !dry_run {
            copy_dir_all(&source, &target).map_err(|err| err.to_string())?;
        }
        self.audit_admin(
            if dry_run {
                "restore.verify"
            } else {
                "restore.apply"
            },
            database_name,
            &format!("source={} dry_run={dry_run}", source.display()),
        );
        Ok(format!(
            "{{\"source\":\"{}\",\"target\":\"{}\",\"manifest\":\"{}\",\"dry_run\":{},\"verified\":true,\"file_count\":{},\"total_bytes\":{},\"checksum\":{}}}",
            json_escape(&source.display().to_string()),
            json_escape(&target.display().to_string()),
            json_escape(BACKUP_MANIFEST_FILE),
            dry_run,
            stats.file_count,
            stats.total_bytes,
            stats.checksum
        ))
    }

    pub(crate) fn maintenance_mode_json(
        &self,
        db: &Neo4rDatabaseHandle,
        enabled: bool,
    ) -> Result<String, String> {
        let path = restore_maintenance_mode_path(db)?;
        if enabled {
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent).map_err(|err| err.to_string())?;
            }
            fs::write(&path, b"maintenance_mode=restore\n").map_err(|err| err.to_string())?;
        } else if path.exists() {
            fs::remove_file(&path).map_err(|err| err.to_string())?;
        }
        Ok(format!(
            "{{\"maintenance_mode\":{},\"path\":\"{}\"}}",
            enabled,
            json_escape(&path.display().to_string())
        ))
    }

    pub(crate) fn restore_maintenance_mode_enabled(
        &self,
        db: &Neo4rDatabaseHandle,
    ) -> Result<bool, String> {
        Ok(restore_maintenance_mode_path(db)?.is_file())
    }
}

fn query_columns_json(rows: &[QueryRow]) -> String {
    let mut columns = BTreeSet::new();
    for row in rows {
        for key in row.values().keys() {
            columns.insert(key.clone());
        }
    }
    format!(
        "[{}]",
        columns
            .iter()
            .map(|column| format!("\"{}\"", json_escape(column)))
            .collect::<Vec<_>>()
            .join(",")
    )
}

fn selectivity_estimate(estimated_rows: u64, estimated_cost: u64) -> f64 {
    if estimated_cost == 0 {
        return 1.0;
    }
    (estimated_rows as f64 / estimated_cost as f64).clamp(0.0, 1.0)
}
