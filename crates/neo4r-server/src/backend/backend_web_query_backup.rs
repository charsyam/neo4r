impl TcpBackend {
    fn graph_json(
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

    fn query_json(
        &self,
        db: &Neo4rDatabaseHandle,
        query: &str,
        params: QueryParams,
    ) -> Result<String, String> {
        self.metrics.queries.fetch_add(1, Ordering::Relaxed);
        let started = Instant::now();
        let rows = match db.execute_cypher_with_params(query, params) {
            Ok(rows) => rows,
            Err(err) => {
                self.metrics.query_errors.fetch_add(1, Ordering::Relaxed);
                return Err(err.to_string());
            }
        };
        let elapsed = started.elapsed();
        if elapsed >= self.slow_query_threshold {
            self.record_slow_query(query, elapsed);
        }
        Ok(format!(
            "{{\"rows\":[{}]}}",
            rows.iter()
                .map(query_row_json)
                .collect::<Vec<_>>()
                .join(",")
        ))
    }

    fn query_plan_json(
        &self,
        db: &Neo4rDatabaseHandle,
        query: &str,
        params: QueryParams,
    ) -> Result<String, String> {
        let plan = db
            .query_plan_with_params(query, params)
            .map_err(|err| err.to_string())?;
        Ok(format!(
            "{{\"plan\":\"{}\"}}",
            json_escape(&format_query_plan(&plan))
        ))
    }

    fn profile_json(
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

    fn raft_status_json(&self, db: &Neo4rDatabaseHandle) -> String {
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

    fn metrics_json(&self, db: &Neo4rDatabaseHandle) -> String {
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
        let web_user_token_count = self
            .web_user_tokens
            .as_ref()
            .and_then(|store| store.list().ok())
            .map(|tokens| tokens.len())
            .unwrap_or_default();
        let web_audit_event_count = self
            .web_audit
            .as_ref()
            .and_then(|store| store.list().ok())
            .map(|events| events.len())
            .unwrap_or_default();
        format!(
            "{{\"http_requests\":{},\"http_errors\":{},\"queries\":{},\"query_errors\":{},\"slow_queries\":{},\"slow_query_threshold_ms\":{},\"db_nodes\":{},\"db_relationships\":{},\"db_indexes\":{},\"db_vector_indexes\":{},\"db_shard_count\":{},\"db_local_partition_count\":{},\"db_committed_indexes\":[{}],\"db_applied_indexes\":[{}],\"tenant_database_count\":{},\"tenant_disabled_count\":{},\"index_ready_count\":{},\"index_building_count\":{},\"index_rebuilding_count\":{},\"index_failed_count\":{},\"raft_group_count\":{},\"raft_leader_count\":{},\"raft_term_max\":{},\"raft_snapshot_index_max\":{},\"raft_joint_consensus_count\":{},\"web_user_token_count\":{},\"web_audit_event_count\":{}}}",
            self.metrics.http_requests.load(Ordering::Relaxed),
            self.metrics.http_errors.load(Ordering::Relaxed),
            self.metrics.queries.load(Ordering::Relaxed),
            self.metrics.query_errors.load(Ordering::Relaxed),
            self.metrics.slow_queries.load(Ordering::Relaxed),
            self.slow_query_threshold.as_millis(),
            statistics
                .as_ref()
                .map(|statistics| statistics.node_count)
                .unwrap_or_default(),
            statistics
                .as_ref()
                .map(|statistics| statistics.relationship_count)
                .unwrap_or_default(),
            statistics
                .as_ref()
                .map(|statistics| statistics.index_count)
                .unwrap_or_default(),
            statistics
                .as_ref()
                .map(|statistics| statistics.vector_index_count)
                .unwrap_or_default(),
            db.shard_count().unwrap_or_default(),
            db.local_partition_count().unwrap_or_default(),
            committed_indexes
                .iter()
                .map(u64::to_string)
                .collect::<Vec<_>>()
                .join(","),
            applied_indexes
                .iter()
                .map(u64::to_string)
                .collect::<Vec<_>>()
                .join(","),
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
            web_audit_event_count
        )
    }

    fn record_slow_query(&self, query: &str, elapsed: Duration) {
        self.metrics.slow_queries.fetch_add(1, Ordering::Relaxed);
        let unix_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_millis())
            .unwrap_or(0);
        let mut entries = self.slow_queries.entries.lock().unwrap();
        entries.push(SlowQueryEntry {
            unix_ms,
            elapsed_ms: elapsed.as_millis(),
            query: query.to_string(),
        });
        if entries.len() > 128 {
            entries.remove(0);
        }
    }

    fn slow_queries_json(&self) -> String {
        let entries = self.slow_queries.entries.lock().unwrap();
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

    fn backup_to_path(&self, db: &Neo4rDatabaseHandle, path: &str) -> Result<String, String> {
        let source = db.data_dir().map_err(|err| err.to_string())?;
        let target = PathBuf::from(path);
        copy_dir_all(&source, &target).map_err(|err| err.to_string())?;
        let stats = collect_backup_manifest_stats(&target).map_err(|err| err.to_string())?;
        let manifest = format!(
            "neo4r_backup_manifest_version=1\nsource={}\ntarget={}\nfile_count={}\ntotal_bytes={}\nchecksum={}\n",
            source.display(),
            target.display(),
            stats.file_count,
            stats.total_bytes,
            stats.checksum
        );
        fs::write(target.join(BACKUP_MANIFEST_FILE), manifest).map_err(|err| err.to_string())?;
        Ok(format!(
            "{{\"source\":\"{}\",\"target\":\"{}\",\"manifest\":\"{}\",\"file_count\":{},\"total_bytes\":{},\"checksum\":{}}}",
            json_escape(&source.display().to_string()),
            json_escape(&target.display().to_string()),
            json_escape(BACKUP_MANIFEST_FILE),
            stats.file_count,
            stats.total_bytes,
            stats.checksum
        ))
    }

    fn restore_from_path(
        &self,
        db: &Neo4rDatabaseHandle,
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
        verify_backup_manifest(&source, &stats).map_err(|err| err.to_string())?;
        let target = db.data_dir().map_err(|err| err.to_string())?;
        if !dry_run {
            copy_dir_all(&source, &target).map_err(|err| err.to_string())?;
        }
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

}
