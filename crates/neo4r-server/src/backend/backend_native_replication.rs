use super::*;
use crate::protocol::format_rebalance_execution;
impl TcpBackend {
    pub fn serve_replication_listener_once(&self, listener: TcpListener) -> io::Result<()> {
        let (stream, _) = listener.accept()?;
        self.handle_replication_tcp_stream(stream)
    }

    pub fn serve_replication_addr(&self, addr: impl ToSocketAddrs) -> io::Result<SocketAddr> {
        let listener = TcpListener::bind(addr)?;
        let local_addr = listener.local_addr()?;
        self.serve_replication_listener(listener)?;
        Ok(local_addr)
    }

    pub fn serve_replication_listener(&self, listener: TcpListener) -> io::Result<()> {
        let backend = Arc::new(self.clone());
        for stream in listener.incoming() {
            let stream = stream?;
            let backend = backend.clone();
            thread::spawn(move || {
                let _ = backend.handle_replication_tcp_stream(stream);
            });
        }
        Ok(())
    }

    #[cfg(feature = "rdma")]
    pub fn serve_rdma_replication_listener(
        &self,
        listener: RdmaReplicationListener,
    ) -> io::Result<()> {
        let backend = Arc::new(self.clone());
        loop {
            let stream = listener.accept().map_err(io::Error::other)?;
            let backend = backend.clone();
            thread::spawn(move || {
                let _ = backend.handle_replication_stream(stream);
            });
        }
    }

    pub fn serve_replication_listener_until(
        &self,
        listener: TcpListener,
        shutdown: Receiver<()>,
    ) -> io::Result<()> {
        listener.set_nonblocking(true)?;
        let backend = Arc::new(self.clone());
        loop {
            if shutdown.try_recv().is_ok() {
                break;
            }
            match listener.accept() {
                Ok((stream, _)) => {
                    let backend = backend.clone();
                    thread::spawn(move || {
                        let _ = backend.handle_replication_tcp_stream(stream);
                    });
                }
                Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(10));
                }
                Err(err) => return Err(err),
            }
        }
        Ok(())
    }

    pub fn handle_native_stream(&self, stream: TcpStream) -> io::Result<()> {
        self.handle_native_transport(PlainNativeStream::new(stream))
    }

    pub(crate) fn handle_native_transport(
        &self,
        stream: impl IntoNativeStreamParts,
    ) -> io::Result<()> {
        let parts = stream.into_native_stream_parts()?;
        self.handle_native_stream_parts(parts)
    }

    pub(crate) fn handle_native_stream_parts(&self, parts: NativeStreamParts) -> io::Result<()> {
        match parts {
            NativeStreamParts::Split { reader, writer } => {
                self.handle_split_native_stream(reader, writer)
            }
            NativeStreamParts::Unified(stream) => self.handle_unified_native_stream(stream),
        }
    }

    fn handle_split_native_stream(
        &self,
        reader: Box<dyn Read + Send>,
        writer_stream: Box<dyn Write + Send>,
    ) -> io::Result<()> {
        let session_id = self.transactions.next_session_id();
        let mut reader = BufReader::new(reader);
        let (response_tx, response_rx) = mpsc::channel::<NativeFrame>();
        let writer = thread::spawn(move || write_native_responses(writer_stream, response_rx));

        while let Some(frame) = read_frame(&mut reader)? {
            if matches!(frame.message_type, NativeMessageType::Quit) {
                send_native_response(
                    &response_tx,
                    native_response_frame(
                        frame.request_id,
                        execute_request(&self.db, BackendRequest::Quit),
                    ),
                )?;
                break;
            }
            if matches!(frame.message_type, NativeMessageType::Cancel) {
                let response = match frame
                    .payload_text()
                    .map_err(|err| err.to_string())
                    .and_then(parse_cancel_payload)
                    .and_then(|target_request_id| {
                        self.cancel_pending_request(session_id, target_request_id)
                            .map(|cancelled| (target_request_id, cancelled))
                    }) {
                    Ok((target_request_id, true)) => NativeFrame::new(
                        NativeMessageType::Response,
                        frame.request_id,
                        format!("OK\tCANCELLED\t{target_request_id}").into_bytes(),
                    ),
                    Ok((target_request_id, false)) => NativeFrame::new(
                        NativeMessageType::Response,
                        frame.request_id,
                        format!("OK\tCANCEL_MISSED\t{target_request_id}").into_bytes(),
                    ),
                    Err(err) => NativeFrame::new(
                        NativeMessageType::Error,
                        frame.request_id,
                        format!("ERR\t{}", escape_payload(&err)).into_bytes(),
                    ),
                };
                send_native_response(&response_tx, response)?;
                continue;
            }

            self.workers
                .submit(session_id, frame, response_tx.clone())?;
        }
        drop(response_tx);
        writer
            .join()
            .map_err(|_| io::Error::other("native response writer thread panicked"))??;
        self.close_native_session(session_id);
        Ok(())
    }

    fn handle_unified_native_stream(&self, stream: Box<dyn NativeTransport>) -> io::Result<()> {
        let session_id = self.transactions.next_session_id();
        let mut stream = BufReader::new(stream);
        while let Some(frame) = read_frame(&mut stream)? {
            let request_id = frame.request_id;
            let response = if matches!(frame.message_type, NativeMessageType::Quit) {
                native_response_frame(request_id, execute_request(&self.db, BackendRequest::Quit))
            } else if matches!(frame.message_type, NativeMessageType::Cancel) {
                match frame
                    .payload_text()
                    .map_err(|err| err.to_string())
                    .and_then(parse_cancel_payload)
                    .and_then(|target_request_id| {
                        self.cancel_pending_request(session_id, target_request_id)
                            .map(|cancelled| (target_request_id, cancelled))
                    }) {
                    Ok((target_request_id, true)) => NativeFrame::new(
                        NativeMessageType::Response,
                        request_id,
                        format!("OK\tCANCELLED\t{target_request_id}").into_bytes(),
                    ),
                    Ok((target_request_id, false)) => NativeFrame::new(
                        NativeMessageType::Response,
                        request_id,
                        format!("OK\tCANCEL_MISSED\t{target_request_id}").into_bytes(),
                    ),
                    Err(err) => NativeFrame::new(
                        NativeMessageType::Error,
                        request_id,
                        format!("ERR\t{}", escape_payload(&err)).into_bytes(),
                    ),
                }
            } else {
                let (response_tx, response_rx) = mpsc::channel::<NativeFrame>();
                self.workers.submit(session_id, frame, response_tx)?;
                response_rx.recv().map_err(|_| {
                    io::Error::new(io::ErrorKind::BrokenPipe, "native worker stopped")
                })?
            };
            write_frame(stream.get_mut(), &response)?;
            if matches!(response.message_type, NativeMessageType::Response)
                && response.payload_text().unwrap_or("") == "OK\tBYE"
            {
                break;
            }
        }
        self.close_native_session(session_id);
        Ok(())
    }

    fn close_native_session(&self, session_id: u64) {
        let _ = self.cursors.close_session(session_id);
        let _ = self.transactions.close_session(session_id);
        let _ = self.prepared_queries.close_session(session_id);
        let _ = self.pending_requests.close_session(session_id);
    }

    pub(crate) fn cancel_pending_request(
        &self,
        session_id: u64,
        request_id: u64,
    ) -> Result<bool, String> {
        self.pending_requests.cancel(session_id, request_id)
    }

    pub(crate) fn execute_backend_request(&self, request: BackendRequest) -> BackendResponse {
        if backend_request_mutates_data(&request) {
            match self.restore_maintenance_mode_enabled(&self.db) {
                Ok(true) => {
                    return BackendResponse::Err(
                        "restore maintenance mode is draining mutating requests".to_string(),
                    );
                }
                Ok(false) => {}
                Err(err) => return BackendResponse::Err(err),
            }
        }
        match request {
            BackendRequest::QueryDistributed { query, params } => {
                self.execute_distributed_query(&query, params)
            }
            BackendRequest::RegisterQueryPeer { server_id, address } => {
                match self.register_query_peer(server_id, address) {
                    Ok(()) => BackendResponse::OkUnit,
                    Err(err) => BackendResponse::Err(err.to_string()),
                }
            }
            BackendRequest::UnregisterQueryPeer(server_id) => {
                match self.unregister_query_peer(server_id) {
                    Ok(()) => BackendResponse::OkUnit,
                    Err(err) => BackendResponse::Err(err.to_string()),
                }
            }
            BackendRequest::ListQueryPeers => match self.list_query_peers() {
                Ok(peers) => BackendResponse::OkQueryPeers(format_query_peers(&peers)),
                Err(err) => BackendResponse::Err(err.to_string()),
            },
            BackendRequest::RegisterReplicationPeer {
                server_id,
                address,
                node_id,
                transport,
            } => {
                match self
                    .register_replication_peer_endpoint(server_id, address, node_id, transport)
                {
                    Ok(()) => BackendResponse::OkUnit,
                    Err(err) => BackendResponse::Err(err.to_string()),
                }
            }
            BackendRequest::NegotiateReplicationPeer {
                server_id,
                address,
                node_id,
            } => match self.negotiate_replication_peer(server_id, address, node_id) {
                Ok(()) => BackendResponse::OkUnit,
                Err(err) => BackendResponse::Err(err.to_string()),
            },
            BackendRequest::UnregisterReplicationPeer(server_id) => {
                match self.unregister_replication_peer(server_id) {
                    Ok(()) => BackendResponse::OkUnit,
                    Err(err) => BackendResponse::Err(err.to_string()),
                }
            }
            BackendRequest::ListReplicationPeers => match self.list_replication_peers() {
                Ok(peers) => BackendResponse::OkReplicationPeers(format_query_peers(&peers)),
                Err(err) => BackendResponse::Err(err.to_string()),
            },
            BackendRequest::ReplicationPeerStatus { server_id } => {
                match replication_peer_status(&self.db, &self.replication_peers, server_id) {
                    Ok(status) => BackendResponse::OkReplicationPeerStatus(
                        format_replication_peer_status(&status),
                    ),
                    Err(err) => BackendResponse::Err(err),
                }
            }
            BackendRequest::ReplicationStatus => match self.replication_status() {
                Ok(status) => BackendResponse::OkReplicationStatus(status),
                Err(err) => BackendResponse::Err(err),
            },
            BackendRequest::RoutingTable => match self.db.routing_table() {
                Ok(routing_table) => {
                    BackendResponse::OkRoutingTable(format_routing_table(&routing_table))
                }
                Err(err) => BackendResponse::Err(err.to_string()),
            },
            BackendRequest::Capabilities => {
                BackendResponse::OkCapabilities(format_protocol_capabilities())
            }
            BackendRequest::ClusterRegistry => {
                match cluster_registry(&self.db, &self.query_peers, DEFAULT_DATABASE) {
                    Ok(registry) => BackendResponse::OkClusterRegistry(registry),
                    Err(err) => BackendResponse::Err(err),
                }
            }
            BackendRequest::SyncIndexCatalogFromPeer(server_id) => {
                match self.sync_index_catalog_from_peer(server_id) {
                    Ok(()) => BackendResponse::OkUnit,
                    Err(err) => BackendResponse::Err(err),
                }
            }
            BackendRequest::CatchUpFromPrimaries {
                max_entries_per_request,
            } => match self.catch_up_from_primaries_with_limit(max_entries_per_request) {
                Ok(results) => BackendResponse::OkCatchUp(format_catch_up_results(&results)),
                Err(err) => BackendResponse::Err(err),
            },
            BackendRequest::CatchUpFromPrimary {
                server_id,
                max_entries_per_request,
            } => match self.catch_up_from_primary_with_limit(server_id, max_entries_per_request) {
                Ok(results) => BackendResponse::OkCatchUp(format_catch_up_results(&results)),
                Err(err) => BackendResponse::Err(err),
            },
            BackendRequest::CatchUpPlan { server_id } => {
                match catch_up_plan(&self.db, &self.replication_peers, server_id) {
                    Ok(plan) => BackendResponse::OkCatchUpPlan(format_catch_up_plan(&plan)),
                    Err(err) => BackendResponse::Err(err),
                }
            }
            BackendRequest::ListTransactionDecisions => {
                match list_transaction_decisions(&self.db) {
                    Ok(decisions) => BackendResponse::OkTransactionDecisions(
                        format_transaction_decisions(&decisions),
                    ),
                    Err(err) => BackendResponse::Err(err),
                }
            }
            BackendRequest::RecoverTransactionDecisions => {
                match self.recover_transaction_decisions() {
                    Ok(count) => BackendResponse::OkTransactionRecovery(count),
                    Err(err) => BackendResponse::Err(err),
                }
            }
            BackendRequest::AdvanceRebalance => match self.advance_rebalance_with_auto_pump() {
                Ok(response) => BackendResponse::OkRebalanceExecution(response),
                Err(err) => BackendResponse::Err(err),
            },
            request => execute_request(&self.db, request),
        }
    }

    pub(crate) fn advance_rebalance_with_auto_pump(&self) -> Result<String, String> {
        let mut result = self.db.advance_rebalance().map_err(|err| err.to_string())?;
        let mut auto_pump_sent = 0_usize;
        if result.action.starts_with("snapshot_bootstrap_required")
            || result.action.starts_with("waiting_for_catch_up")
        {
            auto_pump_sent = self
                .db
                .run_replication_pump()
                .map_err(|err| err.to_string())?;
            if auto_pump_sent > 0 {
                result = self.db.advance_rebalance().map_err(|err| err.to_string())?;
            }
        }
        Ok(format!(
            "action={} auto_pump_sent={} {}",
            result.action,
            auto_pump_sent,
            format_rebalance_execution(&result.execution)
        ))
    }

    pub(crate) fn execute_distributed_query(
        &self,
        query: &str,
        params: neo4r_query::QueryParams,
    ) -> BackendResponse {
        if is_write_cypher(query) {
            return BackendResponse::Err(
                "QUERY_DISTRIBUTED only supports read queries".to_string(),
            );
        }
        match execute_distributed_query(
            &self.db,
            &self.query_peers,
            self.read_preference,
            query,
            &params,
        ) {
            Ok(rows) => BackendResponse::OkRows {
                count: rows.len(),
                debug_rows: encode_query_rows(&rows),
            },
            Err(err) => BackendResponse::Err(err),
        }
    }
}
