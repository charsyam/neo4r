use super::*;

impl NativeExecutionContext {
    pub(crate) fn execute_frame(&self, session_id: u64, frame: NativeFrame) -> NativeFrame {
        let request_id = frame.request_id;
        match self.execute_frame_inner(session_id, frame) {
            Ok(response) => NativeFrame::new(
                NativeMessageType::Response,
                request_id,
                response.into_bytes(),
            ),
            Err(err) => NativeFrame::new(
                NativeMessageType::Error,
                request_id,
                if err.starts_with("ERR\t") {
                    err.into_bytes()
                } else {
                    format!("ERR\t{}", escape_payload(&err)).into_bytes()
                },
            ),
        }
    }

    pub(crate) fn execute_frame_inner(
        &self,
        session_id: u64,
        frame: NativeFrame,
    ) -> Result<String, String> {
        match frame.message_type {
            NativeMessageType::Ping => Ok(format_response(&BackendResponse::OkPong)),
            NativeMessageType::Quit => Ok(format_response(&BackendResponse::OkBye)),
            NativeMessageType::Query => {
                self.reject_if_restore_draining_native_query()?;
                let payload = frame.payload_text().map_err(|err| err.to_string())?;
                let (query, params) = parse_query_payload(payload)?;
                self.execute_query(session_id, &query, params)
            }
            NativeMessageType::Command => {
                let payload = frame.payload_text().map_err(|err| err.to_string())?;
                if let Some(command) = parse_prepared_query_command(payload)? {
                    self.execute_prepared_query_command(session_id, command)
                } else if let Some(command) = parse_transaction_command(payload)? {
                    self.execute_transaction_command(session_id, command)
                } else {
                    let request = parse_request(payload)?;
                    self.execute_backend_command(session_id, request)
                }
            }
            NativeMessageType::Fetch => self.fetch_cursor(
                session_id,
                frame
                    .payload_text()
                    .map_err(|err| err.to_string())
                    .and_then(parse_fetch_payload)?,
            ),
            NativeMessageType::CloseCursor => self.close_cursor(
                session_id,
                parse_cursor_id(frame.payload_text().map_err(|err| err.to_string())?)?,
            ),
            NativeMessageType::Cancel => {
                let target_request_id =
                    parse_cancel_payload(frame.payload_text().map_err(|err| err.to_string())?)?;
                let cancelled = self
                    .pending_requests
                    .cancel(session_id, target_request_id)?;
                if cancelled {
                    Ok(format!("OK\tCANCELLED\t{target_request_id}"))
                } else {
                    Ok(format!("OK\tCANCEL_MISSED\t{target_request_id}"))
                }
            }
            NativeMessageType::Response | NativeMessageType::Error => {
                Err("client cannot send response frames".to_string())
            }
        }
    }

    pub(crate) fn execute_backend_command(
        &self,
        session_id: u64,
        request: BackendRequest,
    ) -> Result<String, String> {
        if backend_request_mutates_data(&request) {
            self.reject_if_restore_draining_native_query()?;
        }
        if let Some(response) = self.forward_shard_write_if_needed(&request)? {
            return Ok(response);
        }
        match request {
            BackendRequest::QueryDistributed { query, params } => {
                self.execute_distributed_query_cursor(session_id, &query, params)
            }
            BackendRequest::QueryShard {
                shard_id,
                query,
                params,
            } => self.execute_query_shard_cursor(session_id, shard_id, &query, params),
            BackendRequest::QueryStagedShard {
                shard_id,
                query,
                params,
                staged_writes,
            } => self.execute_staged_query_shard_cursor(
                session_id,
                shard_id,
                &query,
                params,
                &staged_writes,
            ),
            BackendRequest::QueryWriteShard {
                shard_id,
                query,
                params,
            } => self.execute_write_query_on_shard_cursor(session_id, shard_id, &query, params),
            BackendRequest::QueryWriteBatchShard { shard_id, writes } => {
                self.execute_write_query_batch_on_shard(session_id, shard_id, writes)
            }
            BackendRequest::RegisterQueryPeer { server_id, address } => {
                self.query_peers
                    .register(server_id, address)
                    .map_err(|err| err.to_string())?;
                Ok(format_response(&BackendResponse::OkUnit))
            }
            BackendRequest::UnregisterQueryPeer(server_id) => {
                self.query_peers
                    .unregister(server_id)
                    .map_err(|err| err.to_string())?;
                Ok(format_response(&BackendResponse::OkUnit))
            }
            BackendRequest::ListQueryPeers => {
                let peers = self.query_peers.list().map_err(|err| err.to_string())?;
                Ok(format_response(&BackendResponse::OkQueryPeers(
                    format_query_peers(&peers),
                )))
            }
            BackendRequest::RegisterReplicationPeer {
                server_id,
                address,
                node_id,
                transport,
            } => {
                validate_replication_peer_identity(&self.db, server_id, node_id)?;
                if self
                    .replication_peer_identities
                    .would_create_cycle(server_id, node_id)
                    .map_err(|err| err.to_string())?
                {
                    return Err(format!(
                        "replication peer identity cycle detected for server {server_id}"
                    ));
                }
                self.db
                    .register_replication_peer_endpoint(
                        server_id,
                        replication_endpoint(address.clone(), transport)?,
                    )
                    .map_err(|err| err.to_string())?;
                self.replication_peers
                    .register(server_id, address.clone())
                    .map_err(|err| err.to_string())?;
                self.replication_peer_identities
                    .register(local_peer_identity(
                        &self.db, server_id, address, node_id, transport,
                    ))
                    .map_err(|err| err.to_string())?;
                Ok(format_response(&BackendResponse::OkUnit))
            }
            BackendRequest::NegotiateReplicationPeer {
                server_id,
                address,
                node_id,
            } => {
                validate_replication_peer_membership(&self.db, server_id)?;
                let remote = match self.replication_tls_channel_config.get() {
                    Some(config) => request_tls_replication_hello(
                        &address,
                        self.catch_up_connect_timeout,
                        &config,
                    ),
                    None => request_tcp_replication_hello(&address, self.catch_up_connect_timeout),
                }
                .map_err(|err| err.to_string())?;
                validate_remote_replication_identity(&self.db, server_id, node_id, &remote)?;
                validate_replication_peer_identity(&self.db, server_id, Some(remote.node_id))?;
                if self
                    .replication_peer_identities
                    .would_create_cycle(server_id, Some(remote.node_id))
                    .map_err(|err| err.to_string())?
                {
                    return Err(format!(
                        "replication peer identity cycle detected for server {server_id}"
                    ));
                }
                self.db
                    .register_replication_peer_endpoint(
                        server_id,
                        replication_endpoint(address.clone(), Some(ReplicationChannelKind::Tcp))?,
                    )
                    .map_err(|err| err.to_string())?;
                self.replication_peers
                    .register(server_id, address.clone())
                    .map_err(|err| err.to_string())?;
                self.replication_peer_identities
                    .register(ReplicationPeerIdentity::tcp(
                        server_id,
                        address,
                        Some(remote.node_id),
                        remote.cluster_id,
                        remote.database_id,
                    ))
                    .map_err(|err| err.to_string())?;
                Ok(format_response(&BackendResponse::OkUnit))
            }
            BackendRequest::UnregisterReplicationPeer(server_id) => {
                self.db
                    .unregister_replication_peer(server_id)
                    .map_err(|err| err.to_string())?;
                self.replication_peers
                    .unregister(server_id)
                    .map_err(|err| err.to_string())?;
                self.replication_peer_identities
                    .unregister(server_id)
                    .map_err(|err| err.to_string())?;
                Ok(format_response(&BackendResponse::OkUnit))
            }
            BackendRequest::ListReplicationPeers => {
                let peers = self
                    .replication_peers
                    .list()
                    .map_err(|err| err.to_string())?;
                Ok(format_response(&BackendResponse::OkReplicationPeers(
                    format_query_peers(&peers),
                )))
            }
            BackendRequest::ReplicationPeerStatus { server_id } => {
                let status = replication_peer_status(&self.db, &self.replication_peers, server_id)?;
                Ok(format_response(&BackendResponse::OkReplicationPeerStatus(
                    format_replication_peer_status(&status),
                )))
            }
            BackendRequest::ReplicationStatus => Ok(format_response(
                &BackendResponse::OkReplicationStatus(replication_status(
                    &self.db,
                    &self.replication_peers,
                    &self.replication_peer_identities,
                )?),
            )),
            BackendRequest::RoutingTable => Ok(format_response(&execute_request(
                &self.db,
                BackendRequest::RoutingTable,
            ))),
            BackendRequest::Capabilities => Ok(format_response(&BackendResponse::OkCapabilities(
                format_protocol_capabilities(),
            ))),
            BackendRequest::ClusterRegistry => {
                Ok(format_response(&BackendResponse::OkClusterRegistry(
                    cluster_registry(&self.db, &self.query_peers, DEFAULT_DATABASE)?,
                )))
            }
            BackendRequest::SyncIndexCatalogFromPeer(server_id) => {
                self.sync_index_catalog_from_peer(server_id)?;
                Ok(format_response(&BackendResponse::OkUnit))
            }
            BackendRequest::CatchUpFromPrimaries {
                max_entries_per_request,
            } => {
                let results = catch_up_from_primaries(
                    &self.db,
                    &self.replication_peers,
                    self.catch_up_connect_timeout,
                    max_entries_per_request,
                    &self.replication_tls_channel_config,
                )?;
                Ok(format_response(&BackendResponse::OkCatchUp(
                    format_catch_up_results(&results),
                )))
            }
            BackendRequest::CatchUpFromPrimary {
                server_id,
                max_entries_per_request,
            } => {
                let results = catch_up_from_primary(
                    &self.db,
                    &self.replication_peers,
                    self.catch_up_connect_timeout,
                    server_id,
                    max_entries_per_request,
                    &self.replication_tls_channel_config,
                )?;
                Ok(format_response(&BackendResponse::OkCatchUp(
                    format_catch_up_results(&results),
                )))
            }
            BackendRequest::CatchUpPlan { server_id } => {
                let plan = catch_up_plan(&self.db, &self.replication_peers, server_id)?;
                Ok(format_response(&BackendResponse::OkCatchUpPlan(
                    format_catch_up_plan(&plan),
                )))
            }
            BackendRequest::TopologyReconcile {
                max_entries_per_request,
            } => self.execute_topology_reconcile(max_entries_per_request),
            BackendRequest::RecoverTransactionDecisions => {
                Ok(format_response(&BackendResponse::OkTransactionRecovery(
                    recover_transaction_decisions(&self.db, &self.prepared_transactions)?,
                )))
            }
            BackendRequest::ListTransactionDecisions => {
                Ok(format_response(&BackendResponse::OkTransactionDecisions(
                    format_transaction_decisions(&list_transaction_decisions(&self.db)?),
                )))
            }
            BackendRequest::GossipNode {
                server_id,
                query_address,
                replication_address,
                incarnation,
                ttl_ms,
                token,
            } => self.apply_gossip_node(
                server_id,
                query_address,
                replication_address,
                incarnation,
                ttl_ms,
                token,
            ),
            BackendRequest::ListGossipNodes => self.list_gossip_nodes(),
            BackendRequest::GossipRefreshFromMembership => self.refresh_gossip_from_membership(),
            request => Ok(format_response(&execute_request(&self.db, request))),
        }
    }

    pub(crate) fn execute_topology_reconcile(
        &self,
        max_entries_per_request: Option<usize>,
    ) -> Result<String, String> {
        Ok(format_response(&BackendResponse::OkTopologyObservation(
            TcpBackend::with_peer_stores(
                self.db.clone(),
                TcpBackendConfig {
                    default_page_size: self.default_page_size,
                    read_preference: self.read_preference,
                    catch_up_connect_timeout: self.catch_up_connect_timeout,
                    worker_count: 1,
                    queue_capacity: 1,
                },
                self.query_peers.clone(),
                self.replication_peers.clone(),
                self.replication_peer_identities.clone(),
                self.gossip_nodes.clone(),
            )
            .with_replication_tls_channel_config(self.replication_tls_channel_config.get())
            .topology_reconcile_once(max_entries_per_request)?,
        )))
    }

    pub(crate) fn sync_index_catalog_from_peer(&self, server_id: u64) -> Result<(), String> {
        sync_index_catalog_from_peer(&self.db, &self.query_peers, server_id)
    }

    pub(crate) fn forward_shard_write_if_needed(
        &self,
        request: &BackendRequest,
    ) -> Result<Option<String>, String> {
        let status = self.db.cluster_status().map_err(|err| err.to_string())?;
        let Some(shard_id) = write_request_shard(&self.db, request, status.shard_count)? else {
            return Ok(None);
        };
        let shard = status
            .shards
            .iter()
            .find(|shard| shard.shard_id == shard_id)
            .ok_or_else(|| format!("missing shard {shard_id} in cluster status"))?;
        if shard.is_local_primary {
            return Ok(None);
        }
        let primary = shard
            .primary_server_id
            .ok_or_else(|| format!("missing primary for write shard {shard_id}"))?;
        let Some(address) = self.query_peers.address(primary)? else {
            return Ok(Some(format_response(&BackendResponse::Redirect(
                BackendRedirect {
                    kind: RedirectKind::Moved,
                    shard_id,
                    target_server_id: Some(primary),
                    address: None,
                    routing_version: status.routing_version,
                    database: DEFAULT_DATABASE.to_string(),
                    retryable: true,
                },
            ))));
        };
        let payload = format_command_request_payload(request)?;
        Ok(Some(request_remote_command(&address, &payload)?))
    }

    pub(crate) fn execute_query(
        &self,
        session_id: u64,
        query: &str,
        params: neo4r_query::QueryParams,
    ) -> Result<String, String> {
        let quota_permit = self.tenant_quota.acquire_query(DEFAULT_DATABASE)?;
        let cursor: Box<dyn QueryCursor> = if is_write_cypher(query) {
            let rows = self.execute_write_query_with_routing(query, params)?;
            Box::new(VecQueryCursor::new(rows))
        } else if params.is_empty() {
            self.db.query_cursor(query).map_err(|err| err.to_string())?
        } else {
            self.db
                .query_cursor_with_params(query, params)
                .map_err(|err| err.to_string())?
        };
        let total_rows = cursor.total_rows();
        self.validate_native_result_rows(total_rows)?;
        let cursor_id = self
            .cursors
            .insert_with_permit(session_id, cursor, Some(quota_permit));
        let page = self
            .cursors
            .fetch(session_id, cursor_id, self.default_page_size)?;
        Ok(format_result_start(cursor_id, total_rows, page))
    }

    pub(crate) fn execute_prepared_query_command(
        &self,
        session_id: u64,
        command: PreparedQueryCommand,
    ) -> Result<String, String> {
        match command {
            PreparedQueryCommand::Prepare { query } => {
                let prepared_id = self.prepared_queries.prepare(session_id, query);
                Ok(format!("OK\tPREPARED_QUERY\t{prepared_id}"))
            }
            PreparedQueryCommand::Execute {
                prepared_id,
                params,
            } => {
                let query = self.prepared_queries.get(session_id, prepared_id)?;
                validate_prepared_query_params(prepared_id, &query, &params)?;
                self.execute_query(session_id, &query, params)
            }
            PreparedQueryCommand::QueryPlan {
                prepared_id,
                params,
            } => {
                let query = self.prepared_queries.get(session_id, prepared_id)?;
                validate_prepared_query_params(prepared_id, &query, &params)?;
                let plan = format_query_plan(
                    &self
                        .db
                        .query_plan_with_params(&query, params)
                        .map_err(|err| err.to_string())?,
                );
                Ok(format_response(&BackendResponse::OkQueryPlan(plan)))
            }
            PreparedQueryCommand::Route {
                prepared_id,
                params,
            } => {
                let query = self.prepared_queries.get(session_id, prepared_id)?;
                validate_prepared_query_params(prepared_id, &query, &params)?;
                Ok(format_prepared_query_route(
                    prepared_id,
                    self.prepared_query_routing_hint_with_params(&query, &params)?,
                ))
            }
            PreparedQueryCommand::Describe { prepared_id } => {
                let query = self.prepared_queries.get(session_id, prepared_id)?;
                Ok(format_prepared_query_describe(
                    prepared_id,
                    &query,
                    self.prepared_query_routing_hint(&query)?,
                    describe_query_parameters(&query),
                ))
            }
            PreparedQueryCommand::Close { prepared_id } => {
                self.prepared_queries.close(session_id, prepared_id)?;
                Ok(format!("OK\tPREPARED_QUERY_CLOSED\t{prepared_id}"))
            }
            PreparedQueryCommand::List => Ok(format_prepared_query_list(
                self.prepared_queries.list(session_id)?,
            )),
        }
    }

    pub(crate) fn execute_write_query_with_routing(
        &self,
        query: &str,
        params: neo4r_query::QueryParams,
    ) -> Result<Vec<QueryRow>, String> {
        let status = self.db.cluster_status().map_err(|err| err.to_string())?;
        let shards = if is_create_node_cypher(query) {
            vec![select_create_node_write_shard(&status, query, &params)?.shard_id]
        } else if is_merge_node_cypher(query) {
            vec![select_merge_node_write_shard(&status, query, &params)?.shard_id]
        } else {
            self.db
                .write_cypher_target_shards(query, params.clone())
                .map_err(|err| err.to_string())?
        };
        let mut rows = Vec::new();
        for shard_id in shards {
            let shard = status
                .shards
                .iter()
                .find(|shard| shard.shard_id == shard_id)
                .ok_or_else(|| format!("missing shard {shard_id} in cluster status"))?;
            if shard.is_local_primary {
                rows.extend(
                    self.db
                        .execute_cypher_on_shard(shard.shard_id, query, params.clone())
                        .map_err(|err| err.to_string())?,
                );
            } else {
                let primary = shard
                    .primary_server_id
                    .ok_or_else(|| format!("missing primary for write shard {}", shard.shard_id))?;
                let address = self.query_peers.address(primary)?.ok_or_else(|| {
                    format!(
                        "missing query peer address for primary server {primary} on write shard {}",
                        shard.shard_id
                    )
                })?;
                let response = request_remote_command(
                    &address,
                    &format_query_write_shard_payload(shard.shard_id, query, &params)?,
                )?;
                rows.extend(parse_ok_rows_response(&response)?);
            }
        }
        Ok(rows)
    }

    pub(crate) fn execute_distributed_query_cursor(
        &self,
        session_id: u64,
        query: &str,
        params: neo4r_query::QueryParams,
    ) -> Result<String, String> {
        if is_write_cypher(query) {
            return Err("QUERY_DISTRIBUTED only supports read queries".to_string());
        }
        let quota_permit = self.tenant_quota.acquire_query(DEFAULT_DATABASE)?;
        let cursor = build_distributed_query_cursor(
            &self.db,
            &self.query_peers,
            self.read_preference,
            query,
            &params,
        )?;
        let total_rows = cursor.total_rows();
        self.validate_native_result_rows(total_rows)?;
        let cursor_id = self
            .cursors
            .insert_with_permit(session_id, cursor, Some(quota_permit));
        let page = self
            .cursors
            .fetch(session_id, cursor_id, self.default_page_size)?;
        Ok(format_result_start(cursor_id, total_rows, page))
    }

    pub(crate) fn execute_query_shard_cursor(
        &self,
        session_id: u64,
        shard_id: u64,
        query: &str,
        params: neo4r_query::QueryParams,
    ) -> Result<String, String> {
        if is_write_cypher(query) {
            return Err("QUERY_SHARD only supports read queries in native cursor mode".to_string());
        }
        let quota_permit = self.tenant_quota.acquire_query(DEFAULT_DATABASE)?;
        let rows = self
            .db
            .query_shard_with_params(shard_id, query, params)
            .map_err(|err| err.to_string())?;
        let total_rows = Some(rows.len());
        self.validate_native_result_rows(total_rows)?;
        let cursor_id = self.cursors.insert_with_permit(
            session_id,
            Box::new(VecQueryCursor::new(rows)),
            Some(quota_permit),
        );
        let page = self
            .cursors
            .fetch(session_id, cursor_id, self.default_page_size)?;
        Ok(format_result_start(cursor_id, total_rows, page))
    }

    pub(crate) fn execute_staged_query_shard_cursor(
        &self,
        session_id: u64,
        shard_id: u64,
        query: &str,
        params: neo4r_query::QueryParams,
        staged_writes: &[(String, neo4r_query::QueryParams)],
    ) -> Result<String, String> {
        let quota_permit = self.tenant_quota.acquire_query(DEFAULT_DATABASE)?;
        let rows = self
            .db
            .query_shard_with_staged_writes(
                shard_id,
                query,
                params,
                QueryOptions::default(),
                staged_writes,
            )
            .map_err(|err| err.to_string())?;
        let cursor: Box<dyn QueryCursor> = Box::new(VecQueryCursor::new(rows));
        let total_rows = cursor.total_rows();
        self.validate_native_result_rows(total_rows)?;
        let cursor_id = self
            .cursors
            .insert_with_permit(session_id, cursor, Some(quota_permit));
        let page = self
            .cursors
            .fetch(session_id, cursor_id, self.default_page_size)?;
        Ok(format_result_start(cursor_id, total_rows, page))
    }

    pub(crate) fn execute_write_query_on_shard_cursor(
        &self,
        session_id: u64,
        shard_id: u64,
        query: &str,
        params: neo4r_query::QueryParams,
    ) -> Result<String, String> {
        let quota_permit = self.tenant_quota.acquire_query(DEFAULT_DATABASE)?;
        let rows = self
            .db
            .execute_cypher_on_shard(shard_id, query, params)
            .map_err(|err| err.to_string())?;
        let total_rows = Some(rows.len());
        self.validate_native_result_rows(total_rows)?;
        let cursor_id = self.cursors.insert_with_permit(
            session_id,
            Box::new(VecQueryCursor::new(rows)),
            Some(quota_permit),
        );
        let page = self
            .cursors
            .fetch(session_id, cursor_id, self.default_page_size)?;
        Ok(format_result_start(cursor_id, total_rows, page))
    }

    pub(crate) fn execute_write_query_batch_on_shard(
        &self,
        session_id: u64,
        shard_id: u64,
        writes: Vec<(String, neo4r_query::QueryParams)>,
    ) -> Result<String, String> {
        let quota_permit = self.tenant_quota.acquire_query(DEFAULT_DATABASE)?;
        self.db
            .execute_cypher_mutation_batch_on_shard(shard_id, writes)
            .map_err(|err| err.to_string())?;
        let cursor_id = self.cursors.insert_with_permit(
            session_id,
            Box::new(VecQueryCursor::new(Vec::new())),
            Some(quota_permit),
        );
        let page = self
            .cursors
            .fetch(session_id, cursor_id, self.default_page_size)?;
        Ok(format_result_start(cursor_id, Some(0), page))
    }

    pub(crate) fn validate_native_result_rows(
        &self,
        total_rows: Option<usize>,
    ) -> Result<(), String> {
        if let Some(rows) = total_rows {
            self.tenant_quota
                .validate_result_rows(DEFAULT_DATABASE, rows)?;
        }
        Ok(())
    }

    pub(crate) fn fetch_cursor(
        &self,
        session_id: u64,
        request: FetchRequest,
    ) -> Result<String, String> {
        let page = self
            .cursors
            .fetch(session_id, request.cursor_id, request.page_size)?;
        Ok(format_result_page(request.cursor_id, page))
    }

    pub(crate) fn close_cursor(&self, session_id: u64, cursor_id: u64) -> Result<String, String> {
        self.cursors.close(session_id, cursor_id)?;
        Ok(format!("OK\tCURSOR_CLOSED\t{cursor_id}"))
    }

    fn reject_if_restore_draining_native_query(&self) -> Result<(), String> {
        if restore_maintenance_mode_path(&self.db)?.is_file() {
            return Err("restore maintenance mode is draining native requests".to_string());
        }
        Ok(())
    }
}
