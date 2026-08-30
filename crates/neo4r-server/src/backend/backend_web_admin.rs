use super::*;
impl TcpBackend {
    pub(crate) fn execute_http_request(&self, request: &HttpRequest) -> HttpResponse {
        self.metrics.http_requests.fetch_add(1, Ordering::Relaxed);
        if request.method == "GET" && request.path == "/healthz" {
            return HttpResponse::json("{\"status\":\"ok\"}".to_string());
        }
        let database_name = match self.request_database_name(request) {
            Ok(database_name) => database_name,
            Err(err) => return HttpResponse::json_status(400, json_error(&err)),
        };
        if request.method == "GET" && request.path == "/readyz" {
            return match self.database_for_name(&database_name) {
                Ok(db) => match db.statistics_catalog() {
                    Ok(_) => HttpResponse::json(format!(
                        "{{\"status\":\"ready\",\"database\":\"{}\"}}",
                        json_escape(&database_name)
                    )),
                    Err(err) => HttpResponse::json_status(503, json_error(&err.to_string())),
                },
                Err(err) => HttpResponse::json_status(503, json_error(&err)),
            };
        }
        if request.method == "POST" && request.path == "/api/session" {
            return match self.create_web_session(request, &database_name) {
                Ok(body) => HttpResponse::json(body),
                Err(err) => HttpResponse::json_status(401, json_error(&err)),
            };
        }
        if request.method == "POST" && request.path == "/api/session/logout" {
            return match self.delete_web_session(request) {
                Ok(body) => HttpResponse::json(body),
                Err(err) => HttpResponse::json_status(400, json_error(&err)),
            };
        }
        let Some(role) = self.authorized_role(request, &database_name) else {
            self.metrics.http_errors.fetch_add(1, Ordering::Relaxed);
            self.metrics.auth_failures.fetch_add(1, Ordering::Relaxed);
            if self
                .auth_limiter
                .record_and_should_limit("web", unix_millis_now())
            {
                self.metrics
                    .auth_rate_limited
                    .fetch_add(1, Ordering::Relaxed);
                return HttpResponse::json_status(429, json_error("auth rate limited"));
            }
            return HttpResponse::json_status(401, json_error("unauthorized"));
        };
        if !request.method.eq_ignore_ascii_case("GET") && request_uses_session_cookie(request) {
            if !self.valid_session_csrf(request) {
                self.metrics.http_errors.fetch_add(1, Ordering::Relaxed);
                return HttpResponse::json_status(403, json_error("missing csrf token"));
            }
        }
        let selected_db = || self.database_for_name(&database_name);
        if request_is_drained_during_restore(request) {
            match selected_db().and_then(|db| self.restore_maintenance_mode_enabled(&db)) {
                Ok(true) => {
                    self.metrics.http_errors.fetch_add(1, Ordering::Relaxed);
                    return HttpResponse::json_status(
                        503,
                        json_error("restore maintenance mode is draining mutating requests"),
                    );
                }
                Ok(false) => {}
                Err(err) => return HttpResponse::json_status(500, json_error(&err)),
            }
        }
        if matches!(
            request.path.as_str(),
            "/api/query" | "/api/query-plan" | "/api/profile" | "/api/graph"
        ) {
            if let Some(client_epoch) = request
                .header("x-neo4r-ownership-epoch")
                .or_else(|| request.header("x-neo4r-routing-epoch"))
                .and_then(|value| value.parse::<u64>().ok())
            {
                match selected_db().and_then(|db| db.routing_table().map_err(|err| err.to_string()))
                {
                    Ok(routing_table) if client_epoch < routing_table.version => {
                        self.metrics
                            .stale_epoch_rejections
                            .fetch_add(1, Ordering::Relaxed);
                        return HttpResponse::json_status(
                            409,
                            format!(
                                "{{\"error\":\"stale ownership epoch\",\"routing_version\":{},\"ownership_epoch\":{},\"retryable\":true}}",
                                routing_table.version, routing_table.version
                            ),
                        );
                    }
                    Ok(_) => {}
                    Err(err) => return HttpResponse::json_status(500, json_error(&err)),
                }
            }
        }
        match (request.method.as_str(), request.path.as_str()) {
            ("GET", "/") | ("GET", "/index.html") => HttpResponse::html(WEB_INDEX_HTML),
            ("GET", "/api/capabilities") => HttpResponse::json(self.capabilities_json()),
            ("GET", "/api/admin/system-policy") => HttpResponse::json(self.system_policy_json()),
            ("GET", "/api/admin/distributed-query") => {
                match selected_db().and_then(|db| distributed_query_scatter_gather_summary(&db)) {
                    Ok(body) => HttpResponse::json(body),
                    Err(err) => HttpResponse::json_status(500, json_error(&err)),
                }
            }
            ("GET", "/api/graph") => {
                match selected_db()
                    .and_then(|db| self.graph_json(&db, request.query_value("limit")))
                {
                    Ok(body) => HttpResponse::json(body),
                    Err(err) => HttpResponse::json_status(500, json_error(&err)),
                }
            }
            ("GET", "/api/examples") => HttpResponse::json(query_examples_json()),
            ("GET", "/api/metrics") => match selected_db() {
                Ok(db) => HttpResponse::json(self.metrics_json(&db)),
                Err(err) => HttpResponse::json_status(500, json_error(&err)),
            },
            ("GET", "/metrics") => match selected_db() {
                Ok(db) => HttpResponse::text(self.metrics_prometheus(&db, &database_name)),
                Err(err) => HttpResponse::json_status(500, json_error(&err)),
            },
            ("GET", "/api/slow-queries") => HttpResponse::json(self.slow_queries_json()),
            ("GET", "/api/statistics") => match selected_db() {
                Ok(db) => {
                    let response = execute_request(&db, BackendRequest::Statistics);
                    HttpResponse::json(management_response_json(&response))
                }
                Err(err) => HttpResponse::json_status(500, json_error(&err)),
            },
            ("GET", "/api/storage") => match selected_db() {
                Ok(db) => {
                    let response = execute_request(&db, BackendRequest::StorageStatus);
                    HttpResponse::json(management_response_json(&response))
                }
                Err(err) => HttpResponse::json_status(500, json_error(&err)),
            },
            ("GET", "/api/metadata-log") => match selected_db() {
                Ok(db) => {
                    let response = execute_request(&db, BackendRequest::MetadataLog);
                    HttpResponse::json(management_response_json(&response))
                }
                Err(err) => HttpResponse::json_status(500, json_error(&err)),
            },
            ("GET", "/api/cluster") => {
                let response =
                    self.execute_backend_request(BackendRequest::ClusterManagementStatus);
                HttpResponse::json(management_response_json(&response))
            }
            ("GET", "/api/cluster/routing-table") => match selected_db() {
                Ok(db) => HttpResponse::json(Self::routing_table_json(&db)),
                Err(err) => HttpResponse::json_status(500, json_error(&err)),
            },
            ("GET", "/api/cluster/registry") => match selected_db() {
                Ok(db) => {
                    self.metrics
                        .registry_requests
                        .fetch_add(1, Ordering::Relaxed);
                    HttpResponse::json(self.cluster_registry_json(&db, &database_name))
                }
                Err(err) => HttpResponse::json_status(500, json_error(&err)),
            },
            ("GET", "/api/admin/users") if !role.allows(WebRole::Admin) => {
                HttpResponse::json_status(403, json_error("forbidden"))
            }
            ("GET", "/api/admin/users") => match self.web_users_json() {
                Ok(body) => HttpResponse::json(body),
                Err(err) => HttpResponse::json_status(500, json_error(&err)),
            },
            ("GET", "/api/admin/databases") if !role.allows(WebRole::Admin) => {
                HttpResponse::json_status(403, json_error("forbidden"))
            }
            ("GET", "/api/admin/databases") => match self.databases_json() {
                Ok(body) => HttpResponse::json(body),
                Err(err) => HttpResponse::json_status(500, json_error(&err)),
            },
            ("GET", "/api/admin/audit-log") if !role.allows(WebRole::Admin) => {
                HttpResponse::json_status(403, json_error("forbidden"))
            }
            ("GET", "/api/admin/audit-log") => match self.web_audit_json_filtered(
                request.query_value("action").as_deref(),
                request.query_value("target").as_deref(),
                request
                    .query_value("limit")
                    .and_then(|value| value.parse::<usize>().ok()),
            ) {
                Ok(body) => HttpResponse::json(body),
                Err(err) => HttpResponse::json_status(500, json_error(&err)),
            },
            ("POST", "/api/admin/prune-audit-log") if !role.allows(WebRole::Admin) => {
                HttpResponse::json_status(403, json_error("forbidden"))
            }
            ("POST", "/api/admin/prune-audit-log") => {
                match extract_optional_json_u64_field(&request.body, "retention_days").and_then(
                    |days| {
                        self.prune_web_audit_log(
                            days.ok_or_else(|| "retention_days is required".to_string())?,
                        )
                    },
                ) {
                    Ok(body) => HttpResponse::json(body),
                    Err(err) => HttpResponse::json_status(400, json_error(&err)),
                }
            }
            ("GET", "/api/admin/raft-status") if !role.allows(WebRole::Admin) => {
                HttpResponse::json_status(403, json_error("forbidden"))
            }
            ("GET", "/api/admin/raft-status") => match selected_db() {
                Ok(db) => HttpResponse::json(self.raft_status_json(&db)),
                Err(err) => HttpResponse::json_status(500, json_error(&err)),
            },
            ("POST", "/api/admin/snapshot-now" | "/api/admin/cluster/snapshot")
                if !role.allows(WebRole::Admin) =>
            {
                HttpResponse::json_status(403, json_error("forbidden"))
            }
            ("POST", "/api/admin/snapshot-now" | "/api/admin/cluster/snapshot") => {
                match selected_db() {
                    Ok(db) => match db.snapshot_now() {
                        Ok(result) => HttpResponse::json(storage_maintenance_json(&result)),
                        Err(err) => HttpResponse::json_status(500, json_error(&err.to_string())),
                    },
                    Err(err) => HttpResponse::json_status(500, json_error(&err)),
                }
            }
            ("POST", "/api/admin/verify-invariants") if !role.allows(WebRole::Admin) => {
                HttpResponse::json_status(403, json_error("forbidden"))
            }
            ("POST", "/api/admin/verify-invariants") => match selected_db() {
                Ok(db) => match db.verify_storage_invariants() {
                    Ok(result) => HttpResponse::json(storage_maintenance_json(&result)),
                    Err(err) => HttpResponse::json_status(500, json_error(&err.to_string())),
                },
                Err(err) => HttpResponse::json_status(500, json_error(&err)),
            },
            ("POST", "/api/admin/repair-invariants") if !role.allows(WebRole::Admin) => {
                HttpResponse::json_status(403, json_error("forbidden"))
            }
            ("POST", "/api/admin/repair-invariants") => match selected_db() {
                Ok(db) => match db.repair_storage_invariants() {
                    Ok(result) => HttpResponse::json(storage_maintenance_json(&result)),
                    Err(err) => HttpResponse::json_status(500, json_error(&err.to_string())),
                },
                Err(err) => HttpResponse::json_status(500, json_error(&err)),
            },
            ("POST", "/api/admin/maintenance-mode") if !role.allows(WebRole::Admin) => {
                HttpResponse::json_status(403, json_error("forbidden"))
            }
            ("POST", "/api/admin/maintenance-mode") => {
                match extract_optional_json_bool_field(&request.body, "enabled").and_then(
                    |enabled| selected_db().and_then(|db| self.maintenance_mode_json(&db, enabled)),
                ) {
                    Ok(body) => HttpResponse::json(body),
                    Err(err) => HttpResponse::json_status(500, json_error(&err)),
                }
            }
            ("POST", "/api/admin/databases") if !role.allows(WebRole::Admin) => {
                HttpResponse::json_status(403, json_error("forbidden"))
            }
            ("POST", "/api/admin/databases") => match self.create_database_json(&request.body) {
                Ok(body) => HttpResponse::json(body),
                Err(err) => HttpResponse::json_status(400, json_error(&err)),
            },
            ("POST", "/api/admin/delete-database") if !role.allows(WebRole::Admin) => {
                HttpResponse::json_status(403, json_error("forbidden"))
            }
            ("POST", "/api/admin/delete-database") => {
                match self.delete_database_json(&request.body) {
                    Ok(body) => HttpResponse::json(body),
                    Err(err) => HttpResponse::json_status(400, json_error(&err)),
                }
            }
            ("POST", "/api/admin/disable-database") if !role.allows(WebRole::Admin) => {
                HttpResponse::json_status(403, json_error("forbidden"))
            }
            ("POST", "/api/admin/disable-database") => {
                match self.set_database_disabled_json(&request.body, true) {
                    Ok(body) => HttpResponse::json(body),
                    Err(err) => HttpResponse::json_status(400, json_error(&err)),
                }
            }
            ("POST", "/api/admin/enable-database") if !role.allows(WebRole::Admin) => {
                HttpResponse::json_status(403, json_error("forbidden"))
            }
            ("POST", "/api/admin/enable-database") => {
                match self.set_database_disabled_json(&request.body, false) {
                    Ok(body) => HttpResponse::json(body),
                    Err(err) => HttpResponse::json_status(400, json_error(&err)),
                }
            }
            ("GET", "/api/database") => match selected_db() {
                Ok(_) => HttpResponse::json(format!(
                    "{{\"database\":\"{}\"}}",
                    json_escape(&database_name)
                )),
                Err(err) => HttpResponse::json_status(400, json_error(&err)),
            },
            ("POST", "/api/use-database") => match selected_db() {
                Ok(_) => HttpResponse::json(format!(
                    "{{\"database\":\"{}\"}}",
                    json_escape(&database_name)
                )),
                Err(err) => HttpResponse::json_status(400, json_error(&err)),
            },
            ("POST", "/api/admin/users") if !role.allows(WebRole::Admin) => {
                HttpResponse::json_status(403, json_error("forbidden"))
            }
            ("POST", "/api/admin/users") => match self.add_web_user(&request.body) {
                Ok(body) => HttpResponse::json(body),
                Err(err) => HttpResponse::json_status(400, json_error(&err)),
            },
            ("POST", "/api/admin/invoke-token") if !role.allows(WebRole::Admin) => {
                HttpResponse::json_status(403, json_error("forbidden"))
            }
            ("POST", "/api/admin/invoke-token") => match self.add_web_user(&request.body) {
                Ok(body) => HttpResponse::json(body),
                Err(err) => HttpResponse::json_status(400, json_error(&err)),
            },
            ("POST", "/api/admin/revoke-token") if !role.allows(WebRole::Admin) => {
                HttpResponse::json_status(403, json_error("forbidden"))
            }
            ("POST", "/api/admin/revoke-token") => match self.revoke_web_token(&request.body) {
                Ok(body) => HttpResponse::json(body),
                Err(err) => HttpResponse::json_status(400, json_error(&err)),
            },
            ("POST", "/api/admin/cleanup-expired-tokens") if !role.allows(WebRole::Admin) => {
                HttpResponse::json_status(403, json_error("forbidden"))
            }
            ("POST", "/api/admin/cleanup-expired-tokens") => {
                match self.cleanup_expired_web_tokens() {
                    Ok(body) => HttpResponse::json(body),
                    Err(err) => HttpResponse::json_status(400, json_error(&err)),
                }
            }
            ("POST", "/api/admin/cleanup-expired-sessions") if !role.allows(WebRole::Admin) => {
                HttpResponse::json_status(403, json_error("forbidden"))
            }
            ("POST", "/api/admin/cleanup-expired-sessions") => {
                match self.cleanup_expired_web_sessions() {
                    Ok(body) => HttpResponse::json(body),
                    Err(err) => HttpResponse::json_status(400, json_error(&err)),
                }
            }
            ("POST", "/api/admin/delete-user") if !role.allows(WebRole::Admin) => {
                HttpResponse::json_status(403, json_error("forbidden"))
            }
            ("POST", "/api/admin/delete-user") => match self.delete_web_user(&request.body) {
                Ok(body) => HttpResponse::json(body),
                Err(err) => HttpResponse::json_status(400, json_error(&err)),
            },
            ("POST", "/api/cluster/advance-rebalance" | "/api/admin/cluster/migration/advance") => {
                if !role.allows(WebRole::Admin) {
                    return HttpResponse::json_status(403, json_error("forbidden"));
                }
                let response = self.execute_backend_request(BackendRequest::AdvanceRebalance);
                HttpResponse::json(management_response_json(&response))
            }
            ("POST", "/api/cluster/plan-rebalance" | "/api/admin/cluster/migration/plan") => {
                if !role.allows(WebRole::Admin) {
                    return HttpResponse::json_status(403, json_error("forbidden"));
                }
                let response = self.execute_backend_request(BackendRequest::PlanRebalance);
                HttpResponse::json(management_response_json(&response))
            }
            ("POST", "/api/admin/cluster/migration/start") => {
                if !role.allows(WebRole::Admin) {
                    return HttpResponse::json_status(403, json_error("forbidden"));
                }
                let response = self.execute_backend_request(BackendRequest::StartRebalance);
                HttpResponse::json(management_response_json(&response))
            }
            ("POST", "/api/admin/cluster/migration/cancel") => {
                if !role.allows(WebRole::Admin) {
                    return HttpResponse::json_status(403, json_error("forbidden"));
                }
                let response = self.execute_backend_request(BackendRequest::CancelRebalance);
                HttpResponse::json(management_response_json(&response))
            }
            ("POST", "/api/admin/raft-leader-transfer") => {
                if !role.allows(WebRole::Admin) {
                    return HttpResponse::json_status(403, json_error("forbidden"));
                }
                let required_u64 = |name| {
                    extract_optional_json_u64_field(&request.body, name)?
                        .ok_or_else(|| format!("missing {name}"))
                };
                let result = required_u64("shard_id")
                    .and_then(|shard_id| required_u64("transferee_id").map(|id| (shard_id, id)));
                match result {
                    Ok((shard_id, transferee_id)) => match selected_db() {
                        Ok(db) => {
                            let response = execute_request(
                                &db,
                                BackendRequest::RaftLeaderTransfer {
                                    shard_id,
                                    transferee_id,
                                },
                            );
                            HttpResponse::json(management_response_json(&response))
                        }
                        Err(err) => HttpResponse::json_status(500, json_error(&err)),
                    },
                    Err(err) => HttpResponse::json_status(400, json_error(&err)),
                }
            }
            ("POST", "/api/backup") if !role.allows(WebRole::Admin) => {
                HttpResponse::json_status(403, json_error("forbidden"))
            }
            ("POST", "/api/backup") => {
                match extract_json_string_field(&request.body, "path").and_then(|path| {
                    selected_db().and_then(|db| self.backup_to_path(&db, &database_name, &path))
                }) {
                    Ok(body) => HttpResponse::json(body),
                    Err(err) => HttpResponse::json_status(500, json_error(&err)),
                }
            }
            ("POST", "/api/restore") if !role.allows(WebRole::Admin) => {
                HttpResponse::json_status(403, json_error("forbidden"))
            }
            ("POST", "/api/restore") => {
                match extract_json_string_field(&request.body, "path").and_then(|path| {
                    let dry_run = extract_optional_json_bool_field(&request.body, "dry_run")?
                        || extract_optional_json_bool_field(&request.body, "verify_only")?;
                    if !dry_run
                        && extract_optional_json_string_field(&request.body, "confirm")?.as_deref()
                            != Some("RESTORE")
                    {
                        return Err("destructive restore requires confirm=\"RESTORE\"".to_string());
                    }
                    selected_db()
                        .and_then(|db| self.restore_from_path(&db, &database_name, &path, dry_run))
                }) {
                    Ok(body) => HttpResponse::json(body),
                    Err(err) => HttpResponse::json_status(500, json_error(&err)),
                }
            }
            ("POST", "/api/query") if !role.allows(WebRole::Writer) => {
                HttpResponse::json_status(403, json_error("forbidden"))
            }
            ("POST", "/api/query") => match extract_json_string_field(&request.body, "query") {
                Ok(query) => match strip_database_use_clause(&query).and_then(|query| {
                    parse_json_params_field(&request.body).and_then(|params| {
                        parse_query_options(request).and_then(|options| {
                            selected_db().and_then(|db| {
                                self.query_json(&db, &database_name, &query, params, options)
                            })
                        })
                    })
                }) {
                    Ok(body) => HttpResponse::json(body),
                    Err(err) => {
                        self.metrics.http_errors.fetch_add(1, Ordering::Relaxed);
                        HttpResponse::json_status(500, json_error(&err))
                    }
                },
                Err(err) => HttpResponse::json_status(400, json_error(&err)),
            },
            ("POST", "/api/query-plan") => {
                match extract_json_string_field(&request.body, "query") {
                    Ok(query) => match strip_database_use_clause(&query).and_then(|query| {
                        parse_json_params_field(&request.body).and_then(|params| {
                            selected_db().and_then(|db| self.query_plan_json(&db, &query, params))
                        })
                    }) {
                        Ok(body) => HttpResponse::json(body),
                        Err(err) => HttpResponse::json_status(500, json_error(&err)),
                    },
                    Err(err) => HttpResponse::json_status(400, json_error(&err)),
                }
            }
            ("POST", "/api/profile") => match extract_json_string_field(&request.body, "query") {
                Ok(query) => match strip_database_use_clause(&query).and_then(|query| {
                    parse_json_params_field(&request.body).and_then(|params| {
                        selected_db().and_then(|db| self.profile_json(&db, &query, params))
                    })
                }) {
                    Ok(body) => HttpResponse::json(body),
                    Err(err) => HttpResponse::json_status(500, json_error(&err)),
                },
                Err(err) => HttpResponse::json_status(400, json_error(&err)),
            },
            _ => HttpResponse::json_status(404, json_error("not found")),
        }
    }

    pub(crate) fn request_database_name(&self, request: &HttpRequest) -> Result<String, String> {
        if let Some(query) = extract_optional_json_string_field(&request.body, "query")? {
            if let Some(database) = database_from_use_clause(&query)? {
                return Ok(database);
            }
        }
        let database = request
            .header("x-neo4r-database")
            .map(str::to_string)
            .or_else(|| request.query_value("db"))
            .or_else(|| {
                extract_optional_json_string_field(&request.body, "database")
                    .ok()
                    .flatten()
            })
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| DEFAULT_DATABASE.to_string());
        validate_database_name(&database)?;
        Ok(database)
    }

    pub(crate) fn database_for_name(&self, database: &str) -> Result<Neo4rDatabaseHandle, String> {
        validate_database_name(database)?;
        if database == DEFAULT_DATABASE {
            return Ok(self.db.clone());
        }
        self.tenant_databases
            .as_ref()
            .ok_or_else(|| "multi-tenant database manager is unavailable".to_string())?
            .database(database)
            .map_err(|err| err.to_string())
    }

    pub(crate) fn system_policy_json(&self) -> String {
        format!(
            "{{\"system_database\":\"system\",\"tenant_database_root\":\"databases\",\"system_metadata\":[\"web_auth\",\"web_audit\",\"web_sessions\"],\"tenant_backup_includes_system_metadata\":false,\"selected_database_header\":\"x-neo4r-database\"}}"
        )
    }

    pub(crate) fn authorized_role(&self, request: &HttpRequest, database: &str) -> Option<WebRole> {
        if let Some(session_id) =
            request_session_token(request).filter(|token| token.starts_with("sid:"))
        {
            if let Some(role) = self
                .web_sessions
                .as_ref()
                .and_then(|store| store.role_for_session(&session_id, database, unix_seconds_now()))
            {
                return Some(role);
            }
        }
        let Some(expected) = self.web_auth_token.as_ref() else {
            if self
                .web_user_tokens
                .as_ref()
                .and_then(|store| store.list().ok())
                .is_none_or(|users| users.is_empty())
            {
                return Some(WebRole::Admin);
            }
            let supplied = request
                .header("authorization")
                .and_then(|value| value.strip_prefix("Bearer "))
                .map(str::to_string)
                .or_else(|| request.query_value("token"))?;
            return self.web_user_tokens.as_ref().and_then(|store| {
                store.find_role_by_token(&supplied, database, unix_seconds_now())
            });
        };
        let supplied = request
            .header("authorization")
            .and_then(|value| value.strip_prefix("Bearer "))
            .map(str::to_string)
            .or_else(|| request.query_value("token"))?;
        if constant_time_token_eq(&supplied, expected) {
            return Some(web_role_from_token(expected));
        }
        self.web_user_tokens
            .as_ref()
            .and_then(|store| store.find_role_by_token(&supplied, database, unix_seconds_now()))
    }

    fn create_web_session(&self, request: &HttpRequest, database: &str) -> Result<String, String> {
        let token = extract_json_string_field(&request.body, "token").or_else(|_| {
            request
                .header("authorization")
                .and_then(|value| value.strip_prefix("Bearer "))
                .map(str::to_string)
                .ok_or_else(|| "missing token".to_string())
        })?;
        let role = self
            .authorize_token_value(&token, database)
            .ok_or_else(|| "unauthorized".to_string())?;
        let session_id = self
            .web_sessions
            .as_ref()
            .ok_or_else(|| "web session store is unavailable".to_string())?
            .create(&token, database, role, unix_seconds_now(), 3600)?;
        Ok(format!(
            "{{\"session_id\":\"{}\",\"csrf_token\":\"{}\",\"expires_in\":3600,\"database\":\"{}\"}}",
            json_escape(&session_id.session_id),
            json_escape(&session_id.csrf_token),
            json_escape(database)
        ))
    }

    fn delete_web_session(&self, request: &HttpRequest) -> Result<String, String> {
        let Some(session_id) =
            request_session_token(request).filter(|token| token.starts_with("sid:"))
        else {
            return Err("missing web session".to_string());
        };
        self.web_sessions
            .as_ref()
            .ok_or_else(|| "web session store is unavailable".to_string())?
            .delete(&session_id)?;
        Ok("{\"logged_out\":true}".to_string())
    }

    fn valid_session_csrf(&self, request: &HttpRequest) -> bool {
        let Some(session_id) =
            request_session_token(request).filter(|token| token.starts_with("sid:"))
        else {
            return false;
        };
        let Some(expected) = self
            .web_sessions
            .as_ref()
            .and_then(|store| store.csrf_for_session(&session_id, unix_seconds_now()))
        else {
            return false;
        };
        request
            .header("x-neo4r-csrf")
            .is_some_and(|supplied| constant_time_token_eq(supplied, &expected))
    }

    fn authorize_token_value(&self, token: &str, database: &str) -> Option<WebRole> {
        if let Some(expected) = self.web_auth_token.as_ref() {
            if constant_time_token_eq(token, expected) {
                return Some(web_role_from_token(expected));
            }
        }
        self.web_user_tokens
            .as_ref()
            .and_then(|store| store.find_role_by_token(token, database, unix_seconds_now()))
    }

    pub(crate) fn web_users_json(&self) -> Result<String, String> {
        let users = self
            .web_user_tokens
            .as_ref()
            .ok_or_else(|| "web user token store is unavailable".to_string())?
            .list()?;
        let users = users
            .iter()
            .map(|user| {
                format!(
                    "{{\"name\":\"{}\",\"token_id\":\"{}\",\"role\":\"{}\",\"database_roles\":\"{}\",\"expired_at\":{},\"revoked\":{},\"active\":{},\"created_at\":{},\"last_used_at\":{}}}",
                    json_escape(&user.name),
                    json_escape(&user.token_id),
                    user.role.as_str(),
                    json_escape(&format_database_roles(&user.database_roles)),
                    user.expired_at,
                    user.revoked,
                    user.is_active(unix_seconds_now()),
                    user.created_at,
                    user.last_used_at
                )
            })
            .collect::<Vec<_>>()
            .join(",");
        Ok(format!("{{\"users\":[{users}]}}"))
    }

    pub(crate) fn add_web_user(&self, body: &str) -> Result<String, String> {
        let name = extract_json_string_field(body, "name")?;
        let role = extract_json_string_field(body, "role")?;
        let token = extract_json_string_field(body, "token")?;
        let database_roles = if let Some(value) =
            extract_optional_json_string_field(body, "database_roles")?
                .filter(|value| !value.trim().is_empty())
        {
            parse_database_roles(&value)?
        } else if let Some(database) = extract_optional_json_string_field(body, "database")?
            .filter(|value| !value.trim().is_empty())
        {
            let database_role = extract_optional_json_string_field(body, "database_role")?
                .filter(|value| !value.trim().is_empty())
                .unwrap_or_else(|| role.clone());
            parse_database_roles(&format!("{database}={database_role}"))?
        } else {
            BTreeMap::new()
        };
        let token_id = extract_optional_json_string_field(body, "token_id")?
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| format!("token-{}", unix_millis_now()));
        let expired_at = extract_optional_json_string_field(body, "expired_at")?
            .or_else(|| {
                extract_optional_json_string_field(body, "expires_at")
                    .ok()
                    .flatten()
            })
            .filter(|value| !value.trim().is_empty())
            .map(|value| {
                value
                    .parse::<u128>()
                    .map_err(|_| format!("expired_at must be unix seconds, got {value:?}"))
            })
            .transpose()?
            .unwrap_or(0);
        validate_web_user_name(&name)?;
        validate_web_token_id(&token_id)?;
        validate_web_user_token(&token)?;
        let audit_target = format!("{name}/{token_id}");
        let role = parse_web_role(&role)?;
        self.web_user_tokens
            .as_ref()
            .ok_or_else(|| "web user token store is unavailable".to_string())?
            .put(WebUserToken {
                name,
                token_id,
                role,
                token,
                expired_at,
                revoked: false,
                database_roles,
                created_at: unix_seconds_now(),
                last_used_at: 0,
            })?;
        self.audit_admin("token.invoke", &audit_target, "active=true");
        self.web_users_json()
    }

    pub(crate) fn web_audit_json_filtered(
        &self,
        action: Option<&str>,
        target: Option<&str>,
        limit: Option<usize>,
    ) -> Result<String, String> {
        let events = self
            .web_audit
            .as_ref()
            .ok_or_else(|| "web audit store is unavailable".to_string())?
            .list()?;
        let events = events
            .iter()
            .rev()
            .filter(|event| {
                action
                    .map(|action| event.action.contains(action))
                    .unwrap_or(true)
            })
            .filter(|event| {
                target
                    .map(|target| event.target.contains(target))
                    .unwrap_or(true)
            })
            .take(limit.unwrap_or(usize::MAX))
            .map(|event| {
                format!(
                    "{{\"unix_ms\":{},\"action\":\"{}\",\"target\":\"{}\",\"detail\":\"{}\"}}",
                    event.unix_ms,
                    json_escape(&event.action),
                    json_escape(&event.target),
                    json_escape(&event.detail)
                )
            })
            .collect::<Vec<_>>()
            .join(",");
        Ok(format!("{{\"events\":[{events}]}}"))
    }

    pub(crate) fn prune_web_audit_log(&self, retention_days: u64) -> Result<String, String> {
        if retention_days == 0 {
            return Err("retention_days must be greater than zero".to_string());
        }
        let retention_ms = u128::from(retention_days) * 24 * 60 * 60 * 1000;
        let now = unix_millis_now();
        let cutoff = now.saturating_sub(retention_ms);
        let removed = self
            .web_audit
            .as_ref()
            .ok_or_else(|| "web audit store is unavailable".to_string())?
            .prune_older_than(cutoff)?;
        self.audit_admin(
            "audit.prune",
            "web-audit",
            &format!("retention_days={retention_days} removed={removed}"),
        );
        Ok(format!(
            "{{\"removed\":{},\"retention_days\":{},\"cutoff_unix_ms\":{}}}",
            removed, retention_days, cutoff
        ))
    }

    pub(crate) fn audit_admin(&self, action: &str, target: &str, detail: &str) {
        if let Some(audit) = &self.web_audit {
            let _ = audit.append(action, target, detail);
        }
    }

    pub(crate) fn databases_json(&self) -> Result<String, String> {
        let databases = self
            .tenant_databases
            .as_ref()
            .map(|manager| {
                manager
                    .list_database_records()
                    .map_err(|err| err.to_string())
            })
            .unwrap_or_else(|| {
                Ok(vec![crate::tenant::TenantDatabaseRecord {
                    name: DEFAULT_DATABASE.to_string(),
                    disabled: false,
                }])
            })?
            .iter()
            .map(|record| {
                format!(
                    "{{\"name\":\"{}\",\"disabled\":{}}}",
                    json_escape(&record.name),
                    record.disabled
                )
            })
            .collect::<Vec<_>>()
            .join(",");
        Ok(format!("{{\"databases\":[{databases}]}}"))
    }

    pub(crate) fn create_database_json(&self, body: &str) -> Result<String, String> {
        let name = extract_json_string_field(body, "name")?;
        validate_database_name(&name)?;
        self.tenant_databases
            .as_ref()
            .ok_or_else(|| "multi-tenant database manager is unavailable".to_string())?
            .create_database(&name)
            .map_err(|err| err.to_string())?;
        self.audit_admin("database.create", &name, "created");
        self.databases_json()
    }

    pub(crate) fn delete_database_json(&self, body: &str) -> Result<String, String> {
        let name = extract_json_string_field(body, "name")?;
        self.tenant_databases
            .as_ref()
            .ok_or_else(|| "multi-tenant database manager is unavailable".to_string())?
            .delete_database(&name)
            .map_err(|err| err.to_string())?;
        let revoked_tokens = self
            .web_user_tokens
            .as_ref()
            .map(|tokens| tokens.revoke_database_tokens(&name))
            .transpose()?
            .unwrap_or_default();
        self.audit_admin(
            "database.delete",
            &name,
            &format!("revoked_tokens={revoked_tokens}"),
        );
        let databases = self.databases_json()?;
        Ok(format!(
            "{{\"revoked_tokens\":{},\"catalog\":{}}}",
            revoked_tokens, databases
        ))
    }

    pub(crate) fn set_database_disabled_json(
        &self,
        body: &str,
        disabled: bool,
    ) -> Result<String, String> {
        let name = extract_json_string_field(body, "name")?;
        let manager = self
            .tenant_databases
            .as_ref()
            .ok_or_else(|| "multi-tenant database manager is unavailable".to_string())?;
        if disabled {
            manager.disable_database(&name)
        } else {
            manager.enable_database(&name)
        }
        .map_err(|err| err.to_string())?;
        self.audit_admin(
            if disabled {
                "database.disable"
            } else {
                "database.enable"
            },
            &name,
            "updated",
        );
        self.databases_json()
    }

    pub(crate) fn delete_web_user(&self, body: &str) -> Result<String, String> {
        let name = extract_json_string_field(body, "name")?;
        self.web_user_tokens
            .as_ref()
            .ok_or_else(|| "web user token store is unavailable".to_string())?
            .delete_user(&name)?;
        self.audit_admin("user.delete", &name, "deleted");
        self.web_users_json()
    }

    pub(crate) fn revoke_web_token(&self, body: &str) -> Result<String, String> {
        let name = extract_json_string_field(body, "name")?;
        let token_id = extract_json_string_field(body, "token_id")?;
        self.web_user_tokens
            .as_ref()
            .ok_or_else(|| "web user token store is unavailable".to_string())?
            .revoke(&name, &token_id)?;
        self.audit_admin("token.revoke", &format!("{name}/{token_id}"), "revoked");
        self.web_users_json()
    }

    pub(crate) fn cleanup_expired_web_tokens(&self) -> Result<String, String> {
        let removed = self
            .web_user_tokens
            .as_ref()
            .ok_or_else(|| "web user token store is unavailable".to_string())?
            .cleanup_expired(unix_seconds_now())?;
        self.audit_admin(
            "token.cleanup_expired",
            "web-auth",
            &format!("removed={removed}"),
        );
        Ok(format!("{{\"removed\":{removed}}}"))
    }

    pub(crate) fn cleanup_expired_web_sessions(&self) -> Result<String, String> {
        let removed = self
            .web_sessions
            .as_ref()
            .ok_or_else(|| "web session store is unavailable".to_string())?
            .cleanup_expired(unix_seconds_now())?;
        self.audit_admin(
            "session.cleanup_expired",
            "web-session",
            &format!("removed={removed}"),
        );
        Ok(format!("{{\"removed\":{removed}}}"))
    }
}

fn request_session_token(request: &HttpRequest) -> Option<String> {
    request.header("cookie").and_then(|cookie| {
        cookie.split(';').find_map(|part| {
            let (key, value) = part.trim().split_once('=')?;
            if key == "neo4r.session" && !value.trim().is_empty() {
                Some(value.trim().replace("%3A", ":").replace("%20", " "))
            } else {
                None
            }
        })
    })
}

fn request_uses_session_cookie(request: &HttpRequest) -> bool {
    request.header("cookie").is_some_and(|cookie| {
        cookie
            .split(';')
            .any(|part| part.trim().starts_with("neo4r.session="))
    })
}

fn request_is_drained_during_restore(request: &HttpRequest) -> bool {
    request.method == "POST" && matches!(request.path.as_str(), "/api/query")
}

fn parse_query_options(request: &HttpRequest) -> Result<QueryOptions, String> {
    let consistency = request
        .header("x-neo4r-read-consistency")
        .map(str::to_string)
        .or_else(|| {
            extract_optional_json_string_field(&request.body, "read_consistency")
                .ok()
                .flatten()
        })
        .unwrap_or_else(|| "strong".to_string());
    let normalized = consistency.trim().to_ascii_lowercase().replace('-', "_");
    let read_consistency = match normalized.as_str() {
        "strong" => ReadConsistency::Strong,
        "follower_stale" | "stale" => ReadConsistency::FollowerStale,
        "bounded_staleness" | "bounded" => {
            let max_staleness_ms = request
                .header("x-neo4r-max-staleness-ms")
                .and_then(|value| value.parse::<u64>().ok())
                .or_else(|| {
                    extract_optional_json_u64_field(&request.body, "max_staleness_ms")
                        .ok()
                        .flatten()
                })
                .unwrap_or(1_000);
            ReadConsistency::BoundedStaleness { max_staleness_ms }
        }
        other => {
            return Err(format!(
                "unsupported read_consistency {other}; expected strong, follower_stale, or bounded_staleness"
            ));
        }
    };
    Ok(QueryOptions::default().with_consistency(read_consistency))
}
