use super::*;
impl TcpBackend {
    pub fn new(db: Neo4rDatabaseHandle) -> Self {
        Self::with_config(db, TcpBackendConfig::default())
    }

    pub fn with_config(db: Neo4rDatabaseHandle, config: TcpBackendConfig) -> Self {
        Self::with_peer_stores(
            db,
            config,
            QueryPeerStore::default(),
            QueryPeerStore::default(),
        )
    }

    pub fn with_persistent_config(
        db: Neo4rDatabaseHandle,
        config: TcpBackendConfig,
    ) -> io::Result<Self> {
        let data_dir = db.data_dir().map_err(io::Error::other)?;
        let query_peers = QueryPeerStore::open(data_dir.join("cluster").join(QUERY_PEERS_FILE))?;
        let replication_peers =
            QueryPeerStore::open(data_dir.join("cluster").join(REPLICATION_PEERS_FILE))?;
        let prepared_transactions = PreparedTransactionStore::open(
            data_dir
                .join("transactions")
                .join(PREPARED_TRANSACTIONS_FILE),
        )?;
        for (server_id, address) in replication_peers.list()? {
            db.register_replication_peer_endpoint(server_id, ReplicationEndpoint::tcp(address))
                .map_err(io::Error::other)?;
        }
        let backend = Self::with_stores(
            db,
            config,
            query_peers,
            replication_peers,
            prepared_transactions,
        );
        backend
            .recover_transaction_decisions()
            .map_err(io::Error::other)?;
        Ok(backend)
    }

    pub(crate) fn with_peer_stores(
        db: Neo4rDatabaseHandle,
        config: TcpBackendConfig,
        query_peers: QueryPeerStore,
        replication_peers: QueryPeerStore,
    ) -> Self {
        Self::with_stores(
            db,
            config,
            query_peers,
            replication_peers,
            PreparedTransactionStore::default(),
        )
    }

    pub(crate) fn with_stores(
        db: Neo4rDatabaseHandle,
        config: TcpBackendConfig,
        query_peers: QueryPeerStore,
        replication_peers: QueryPeerStore,
        prepared_transactions: PreparedTransactionStore,
    ) -> Self {
        let cursors = CursorStore::default();
        let transactions = TransactionStore::default();
        let pending_requests = PendingRequestStore::default();
        let prepared_queries = PreparedQueryStore::default();
        let web_user_tokens = db.data_dir().ok().and_then(|data_dir| {
            WebUserTokenStore::open(data_dir.join(WEB_AUTH_ROCKS_DIR))
                .or_else(|_| WebUserTokenStore::open(data_dir.join("web-auth-rocksdb")))
                .ok()
        });
        let web_audit = db
            .data_dir()
            .ok()
            .and_then(|data_dir| WebAuditStore::open(data_dir.join(WEB_AUDIT_ROCKS_DIR)).ok());
        Self {
            workers: NativeWorkerPool::new(
                NativeExecutionContext {
                    db: db.clone(),
                    cursors: cursors.clone(),
                    transactions: transactions.clone(),
                    prepared_transactions: prepared_transactions.clone(),
                    prepared_queries: prepared_queries.clone(),
                    query_peers: query_peers.clone(),
                    replication_peers: replication_peers.clone(),
                    default_page_size: config.default_page_size.max(1),
                    read_preference: config.read_preference,
                    catch_up_connect_timeout: config.catch_up_connect_timeout,
                    pending_requests: pending_requests.clone(),
                },
                config.worker_count,
                config.queue_capacity,
            ),
            db,
            cursors,
            transactions,
            prepared_transactions,
            prepared_queries,
            query_peers,
            replication_peers,
            read_preference: config.read_preference,
            catch_up_connect_timeout: config.catch_up_connect_timeout,
            pending_requests,
            web_auth_token: None,
            slow_query_threshold: Duration::from_millis(250),
            metrics: WebMetrics::default(),
            slow_queries: SlowQueryLog::default(),
            web_user_tokens,
            web_audit,
            tenant_databases: None,
        }
    }

    pub fn with_web_options(
        mut self,
        web_auth_token: Option<String>,
        slow_query_threshold: Duration,
    ) -> Self {
        self.web_auth_token = web_auth_token;
        self.slow_query_threshold = slow_query_threshold;
        self
    }

    pub fn with_multi_tenant_config(mut self, config: DatabaseConfig) -> io::Result<Self> {
        self.tenant_databases = Some(TenantDatabaseManager::open(self.db.clone(), config)?);
        Ok(self)
    }

    pub fn open(config: DatabaseConfig) -> Result<Self, neo4r_db::DatabaseError> {
        Ok(Self::new(Neo4rDatabaseHandle::open(config)?))
    }

    pub fn register_query_peer(
        &self,
        server_id: u64,
        address: impl Into<String>,
    ) -> io::Result<()> {
        self.query_peers.register(server_id, address.into())
    }

    pub fn unregister_query_peer(&self, server_id: u64) -> io::Result<()> {
        self.query_peers.unregister(server_id)
    }

    pub fn list_query_peers(&self) -> io::Result<Vec<(u64, String)>> {
        self.query_peers.list()
    }

    pub fn register_replication_peer(
        &self,
        server_id: u64,
        address: impl Into<String>,
    ) -> io::Result<()> {
        self.register_replication_peer_endpoint(server_id, address, None, None)
    }

    pub fn register_replication_peer_endpoint(
        &self,
        server_id: u64,
        address: impl Into<String>,
        node_id: Option<u64>,
        transport: Option<ReplicationChannelKind>,
    ) -> io::Result<()> {
        let address = address.into();
        validate_replication_peer_identity(&self.db, server_id, node_id)
            .map_err(io::Error::other)?;
        let endpoint =
            replication_endpoint(address.clone(), transport).map_err(io::Error::other)?;
        self.db
            .register_replication_peer_endpoint(server_id, endpoint)
            .map_err(io::Error::other)?;
        self.replication_peers.register(server_id, address)
    }

    pub fn unregister_replication_peer(&self, server_id: u64) -> io::Result<()> {
        self.db
            .unregister_replication_peer(server_id)
            .map_err(io::Error::other)?;
        self.replication_peers.unregister(server_id)
    }

    pub fn list_replication_peers(&self) -> io::Result<Vec<(u64, String)>> {
        self.replication_peers.list()
    }

    pub fn replication_status(&self) -> Result<String, String> {
        replication_status(&self.db, &self.replication_peers)
    }

    pub fn catch_up_from_primaries(&self) -> Result<Vec<neo4r_db::TcpCatchUpResult>, String> {
        self.catch_up_from_primaries_with_limit(None)
    }

    pub fn catch_up_from_primaries_with_limit(
        &self,
        max_entries_per_request: Option<usize>,
    ) -> Result<Vec<neo4r_db::TcpCatchUpResult>, String> {
        catch_up_from_primaries(
            &self.db,
            &self.replication_peers,
            self.catch_up_connect_timeout,
            max_entries_per_request,
        )
    }

    pub fn catch_up_from_primary_with_limit(
        &self,
        server_id: u64,
        max_entries_per_request: Option<usize>,
    ) -> Result<Vec<neo4r_db::TcpCatchUpResult>, String> {
        catch_up_from_primary(
            &self.db,
            &self.replication_peers,
            self.catch_up_connect_timeout,
            server_id,
            max_entries_per_request,
        )
    }

    pub fn sync_index_catalog_from_peer(&self, server_id: u64) -> Result<(), String> {
        sync_index_catalog_from_peer(&self.db, &self.query_peers, server_id)
    }

    pub fn recover_transaction_decisions(&self) -> Result<usize, String> {
        recover_transaction_decisions(&self.db, &self.prepared_transactions)
    }

    pub fn serve_addr(self, addr: impl ToSocketAddrs) -> io::Result<SocketAddr> {
        let listener = TcpListener::bind(addr)?;
        let local_addr = listener.local_addr()?;
        self.serve_listener(listener)?;
        Ok(local_addr)
    }

    pub fn serve_listener(self, listener: TcpListener) -> io::Result<()> {
        let backend = Arc::new(self);
        for stream in listener.incoming() {
            let stream = stream?;
            let backend = backend.clone();
            thread::spawn(move || {
                let _ = backend.handle_stream(stream);
            });
        }
        Ok(())
    }

    pub fn serve_listener_once(self, listener: TcpListener) -> io::Result<()> {
        let (stream, _) = listener.accept()?;
        self.handle_stream(stream)
    }

    pub fn serve_web_addr(self, addr: impl ToSocketAddrs) -> io::Result<SocketAddr> {
        let listener = TcpListener::bind(addr)?;
        let local_addr = listener.local_addr()?;
        self.serve_web_listener(listener)?;
        Ok(local_addr)
    }

    pub fn serve_web_listener(self, listener: TcpListener) -> io::Result<()> {
        let backend = Arc::new(self);
        for stream in listener.incoming() {
            let stream = stream?;
            let backend = backend.clone();
            thread::spawn(move || {
                let _ = backend.handle_web_stream(stream);
            });
        }
        Ok(())
    }

    pub fn serve_web_listener_once(self, listener: TcpListener) -> io::Result<()> {
        let (stream, _) = listener.accept()?;
        self.handle_web_stream(stream)
    }

    pub fn handle_web_stream(&self, stream: TcpStream) -> io::Result<()> {
        let request = read_http_request(stream.try_clone()?)?;
        let response = self.execute_http_request(&request);
        write_http_response(stream, response)
    }

    pub fn handle_stream(&self, stream: TcpStream) -> io::Result<()> {
        self.handle_native_stream(stream)
    }

    pub fn handle_line_stream(&self, stream: TcpStream) -> io::Result<()> {
        let reader_stream = stream.try_clone()?;
        let mut reader = BufReader::new(reader_stream);
        let mut writer = BufWriter::new(stream);
        let mut line = String::new();

        loop {
            line.clear();
            let bytes = reader.read_line(&mut line)?;
            if bytes == 0 {
                break;
            }
            let response = match parse_request(line.trim_end_matches(['\r', '\n'])) {
                Ok(request) => self.execute_backend_request(request),
                Err(err) => BackendResponse::Err(err),
            };
            write_response(&mut writer, &response)?;
            writer.flush()?;
            if matches!(response, BackendResponse::OkBye) {
                break;
            }
        }
        Ok(())
    }

    pub fn handle_replication_stream(&self, mut stream: TcpStream) -> io::Result<()> {
        handle_tcp_replication_stream(&self.db, &mut stream).map_err(io::Error::other)
    }
}

pub(crate) fn replication_endpoint(
    address: String,
    transport: Option<ReplicationChannelKind>,
) -> Result<ReplicationEndpoint, String> {
    match transport.unwrap_or(ReplicationChannelKind::Tcp) {
        ReplicationChannelKind::Tcp => Ok(ReplicationEndpoint::tcp(address)),
        ReplicationChannelKind::Udp => Ok(ReplicationEndpoint::udp(address, 1200)),
        ReplicationChannelKind::Rdma => {
            Err("rdma replication endpoints require an rdma-enabled provider boundary".to_string())
        }
        ReplicationChannelKind::Custom => {
            Err("custom replication endpoints require an explicit provider boundary".to_string())
        }
    }
}

pub(crate) fn validate_replication_peer_identity(
    db: &Neo4rDatabaseHandle,
    server_id: u64,
    node_id: Option<u64>,
) -> Result<(), String> {
    let local_server_id = db
        .cluster_status()
        .map_err(|err| err.to_string())?
        .server_id;
    if server_id == local_server_id {
        return Err(format!(
            "replication peer {server_id} cannot point to local server"
        ));
    }
    if node_id == Some(local_server_id) {
        return Err(format!(
            "replication peer node_id {local_server_id} cannot point to local server"
        ));
    }
    Ok(())
}
