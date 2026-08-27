//! TCP backend for neo4r.

mod peer_store;
mod protocol;

use neo4r_db::{
    catch_up_from_tcp_primary, catch_up_from_tcp_primary_batched, create_node_routing_key,
    handle_tcp_replication_stream, merge_node_routing_key, CreateNodeRoutingKey, DatabaseConfig,
    Neo4rDatabaseHandle, Neo4rReadTransaction, QueryOptions, ReadIsolation,
};
use neo4r_query::{QueryCursor, QueryRow, VecQueryCursor};
use neo4r_storage::{
    TransactionDecision, TransactionDecisionRecord, TransactionDecisionStore,
    TransactionParticipantRecord,
};
use peer_store::{format_query_peers, QueryPeerStore, QUERY_PEERS_FILE, REPLICATION_PEERS_FILE};
use protocol::{
    decode_index_catalog, decode_query_batch_payload, decode_query_rows,
    encode_query_batch_payload, encode_query_rows, execute_request, format_query_plan,
    format_response, parse_query_payload, parse_request, write_response, BackendResponse,
};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::net::{SocketAddr, TcpListener, TcpStream, ToSocketAddrs};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender, TrySendError};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

const PREPARED_TRANSACTIONS_FILE: &str = "prepared.log";
const PREPARED_TRANSACTIONS_MAGIC: &str = "N4RPTX1";

pub use neo4r_protocol::{read_frame, write_frame, NativeFrame, NativeMessageType};
pub use protocol::BackendRequest;

#[derive(Clone)]
pub struct TcpBackend {
    db: Neo4rDatabaseHandle,
    workers: NativeWorkerPool,
    cursors: CursorStore,
    transactions: TransactionStore,
    prepared_transactions: PreparedTransactionStore,
    prepared_queries: PreparedQueryStore,
    query_peers: QueryPeerStore,
    replication_peers: QueryPeerStore,
    read_preference: QueryReadPreference,
    catch_up_connect_timeout: Duration,
    pending_requests: PendingRequestStore,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TcpBackendConfig {
    pub worker_count: usize,
    pub queue_capacity: usize,
    pub default_page_size: usize,
    pub read_preference: QueryReadPreference,
    pub catch_up_connect_timeout: Duration,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueryReadPreference {
    Primary,
    PreferReplica,
}

impl Default for TcpBackendConfig {
    fn default() -> Self {
        Self {
            worker_count: default_worker_count(),
            queue_capacity: 1024,
            default_page_size: 128,
            read_preference: QueryReadPreference::Primary,
            catch_up_connect_timeout: Duration::from_secs(1),
        }
    }
}

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
            db.register_replication_peer(server_id, address)
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

    fn with_peer_stores(
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

    fn with_stores(
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
        }
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
        let address = address.into();
        self.db
            .register_replication_peer(server_id, address.clone())
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

    pub fn serve_replication_listener_once(&self, listener: TcpListener) -> io::Result<()> {
        let (stream, _) = listener.accept()?;
        self.handle_replication_stream(stream)
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
                let _ = backend.handle_replication_stream(stream);
            });
        }
        Ok(())
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
                        let _ = backend.handle_replication_stream(stream);
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
        let session_id = self.transactions.next_session_id();
        let reader_stream = stream.try_clone()?;
        let mut reader = BufReader::new(reader_stream);
        let (response_tx, response_rx) = mpsc::channel::<NativeFrame>();
        let writer = thread::spawn(move || write_native_responses(stream, response_rx));

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
        let _ = self.cursors.close_session(session_id);
        let _ = self.transactions.close_session(session_id);
        let _ = self.prepared_queries.close_session(session_id);
        let _ = self.pending_requests.close_session(session_id);
        Ok(())
    }

    fn cancel_pending_request(&self, session_id: u64, request_id: u64) -> Result<bool, String> {
        self.pending_requests.cancel(session_id, request_id)
    }

    fn execute_backend_request(&self, request: BackendRequest) -> BackendResponse {
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
            BackendRequest::RegisterReplicationPeer { server_id, address } => {
                match self.register_replication_peer(server_id, address) {
                    Ok(()) => BackendResponse::OkUnit,
                    Err(err) => BackendResponse::Err(err.to_string()),
                }
            }
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
            request => execute_request(&self.db, request),
        }
    }

    fn execute_distributed_query(
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

#[derive(Clone)]
struct NativeExecutionContext {
    db: Neo4rDatabaseHandle,
    cursors: CursorStore,
    transactions: TransactionStore,
    prepared_transactions: PreparedTransactionStore,
    prepared_queries: PreparedQueryStore,
    query_peers: QueryPeerStore,
    replication_peers: QueryPeerStore,
    default_page_size: usize,
    read_preference: QueryReadPreference,
    catch_up_connect_timeout: Duration,
    pending_requests: PendingRequestStore,
}

impl NativeExecutionContext {
    fn execute_frame(&self, session_id: u64, frame: NativeFrame) -> NativeFrame {
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
                format!("ERR\t{}", escape_payload(&err)).into_bytes(),
            ),
        }
    }

    fn execute_frame_inner(&self, session_id: u64, frame: NativeFrame) -> Result<String, String> {
        match frame.message_type {
            NativeMessageType::Ping => Ok(format_response(&BackendResponse::OkPong)),
            NativeMessageType::Quit => Ok(format_response(&BackendResponse::OkBye)),
            NativeMessageType::Query => {
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

    fn execute_backend_command(
        &self,
        session_id: u64,
        request: BackendRequest,
    ) -> Result<String, String> {
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
            BackendRequest::RegisterReplicationPeer { server_id, address } => {
                self.db
                    .register_replication_peer(server_id, address.clone())
                    .map_err(|err| err.to_string())?;
                self.replication_peers
                    .register(server_id, address)
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
            BackendRequest::ReplicationStatus => {
                Ok(format_response(&BackendResponse::OkReplicationStatus(
                    replication_status(&self.db, &self.replication_peers)?,
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
            request => Ok(format_response(&execute_request(&self.db, request))),
        }
    }

    fn sync_index_catalog_from_peer(&self, server_id: u64) -> Result<(), String> {
        sync_index_catalog_from_peer(&self.db, &self.query_peers, server_id)
    }

    fn forward_shard_write_if_needed(
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
        let address = self.query_peers.address(primary)?.ok_or_else(|| {
            format!(
                "missing query peer address for primary server {primary} on write shard {shard_id}"
            )
        })?;
        let payload = format_command_request_payload(request)?;
        Ok(Some(request_remote_command(&address, &payload)?))
    }

    fn execute_query(
        &self,
        session_id: u64,
        query: &str,
        params: neo4r_query::QueryParams,
    ) -> Result<String, String> {
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
        let cursor_id = self.cursors.insert(session_id, cursor);
        let page = self
            .cursors
            .fetch(session_id, cursor_id, self.default_page_size)?;
        Ok(format_result_start(cursor_id, total_rows, page))
    }

    fn execute_prepared_query_command(
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

    fn execute_write_query_with_routing(
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

    fn execute_distributed_query_cursor(
        &self,
        session_id: u64,
        query: &str,
        params: neo4r_query::QueryParams,
    ) -> Result<String, String> {
        if is_write_cypher(query) {
            return Err("QUERY_DISTRIBUTED only supports read queries".to_string());
        }
        let cursor = build_distributed_query_cursor(
            &self.db,
            &self.query_peers,
            self.read_preference,
            query,
            &params,
        )?;
        let total_rows = cursor.total_rows();
        let cursor_id = self.cursors.insert(session_id, cursor);
        let page = self
            .cursors
            .fetch(session_id, cursor_id, self.default_page_size)?;
        Ok(format_result_start(cursor_id, total_rows, page))
    }

    fn execute_query_shard_cursor(
        &self,
        session_id: u64,
        shard_id: u64,
        query: &str,
        params: neo4r_query::QueryParams,
    ) -> Result<String, String> {
        if is_write_cypher(query) {
            return Err("QUERY_SHARD only supports read queries in native cursor mode".to_string());
        }
        let rows = self
            .db
            .query_shard_with_params(shard_id, query, params)
            .map_err(|err| err.to_string())?;
        let total_rows = Some(rows.len());
        let cursor_id = self
            .cursors
            .insert(session_id, Box::new(VecQueryCursor::new(rows)));
        let page = self
            .cursors
            .fetch(session_id, cursor_id, self.default_page_size)?;
        Ok(format_result_start(cursor_id, total_rows, page))
    }

    fn execute_staged_query_shard_cursor(
        &self,
        session_id: u64,
        shard_id: u64,
        query: &str,
        params: neo4r_query::QueryParams,
        staged_writes: &[(String, neo4r_query::QueryParams)],
    ) -> Result<String, String> {
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
        let cursor_id = self.cursors.insert(session_id, cursor);
        let page = self
            .cursors
            .fetch(session_id, cursor_id, self.default_page_size)?;
        Ok(format_result_start(cursor_id, total_rows, page))
    }

    fn execute_write_query_on_shard_cursor(
        &self,
        session_id: u64,
        shard_id: u64,
        query: &str,
        params: neo4r_query::QueryParams,
    ) -> Result<String, String> {
        let rows = self
            .db
            .execute_cypher_on_shard(shard_id, query, params)
            .map_err(|err| err.to_string())?;
        let total_rows = Some(rows.len());
        let cursor_id = self
            .cursors
            .insert(session_id, Box::new(VecQueryCursor::new(rows)));
        let page = self
            .cursors
            .fetch(session_id, cursor_id, self.default_page_size)?;
        Ok(format_result_start(cursor_id, total_rows, page))
    }

    fn execute_write_query_batch_on_shard(
        &self,
        session_id: u64,
        shard_id: u64,
        writes: Vec<(String, neo4r_query::QueryParams)>,
    ) -> Result<String, String> {
        self.db
            .execute_cypher_mutation_batch_on_shard(shard_id, writes)
            .map_err(|err| err.to_string())?;
        let cursor_id = self
            .cursors
            .insert(session_id, Box::new(VecQueryCursor::new(Vec::new())));
        let page = self
            .cursors
            .fetch(session_id, cursor_id, self.default_page_size)?;
        Ok(format_result_start(cursor_id, Some(0), page))
    }

    fn fetch_cursor(&self, session_id: u64, request: FetchRequest) -> Result<String, String> {
        let page = self
            .cursors
            .fetch(session_id, request.cursor_id, request.page_size)?;
        Ok(format_result_page(request.cursor_id, page))
    }

    fn close_cursor(&self, session_id: u64, cursor_id: u64) -> Result<String, String> {
        self.cursors.close(session_id, cursor_id)?;
        Ok(format!("OK\tCURSOR_CLOSED\t{cursor_id}"))
    }

    fn execute_transaction_command(
        &self,
        session_id: u64,
        command: TransactionCommand,
    ) -> Result<String, String> {
        match command {
            TransactionCommand::Begin { mode, isolation } => {
                let tx = match mode {
                    TransactionMode::ReadOnly => {
                        let tx = self
                            .db
                            .begin_read_transaction_with_options(
                                QueryOptions::default().with_isolation(isolation),
                            )
                            .map_err(|err| err.to_string())?;
                        NativeTransaction::ReadOnly(tx)
                    }
                    TransactionMode::ReadWrite => NativeTransaction::ReadWrite {
                        isolation,
                        staged_writes: Vec::new(),
                    },
                };
                let tx_id = self.transactions.insert(session_id, tx);
                Ok(format!(
                    "OK\tTX_BEGIN\t{tx_id}\t{}\t{}",
                    format_transaction_mode(mode),
                    format_read_isolation(isolation)
                ))
            }
            TransactionCommand::Query {
                tx_id,
                query,
                params,
            } => {
                if is_write_cypher(&query) {
                    let staged_count = self
                        .transactions
                        .stage_write(session_id, tx_id, query, params)?;
                    Ok(format!("OK\tTX_STAGED\t{tx_id}\t{staged_count}"))
                } else {
                    let cursor = self
                        .transactions
                        .query_cursor(&self.db, session_id, tx_id, &query, &params)
                        .map_err(|err| err.to_string())?;
                    let total_rows = cursor.total_rows();
                    let cursor_id = self.cursors.insert(session_id, cursor);
                    let page = self
                        .cursors
                        .fetch(session_id, cursor_id, self.default_page_size)?;
                    Ok(format_result_start(cursor_id, total_rows, page))
                }
            }
            TransactionCommand::ExecutePrepared {
                tx_id,
                prepared_id,
                params,
            } => {
                let query = self.prepared_queries.get(session_id, prepared_id)?;
                validate_prepared_query_params(prepared_id, &query, &params)?;
                if is_write_cypher(&query) {
                    let staged_count = self
                        .transactions
                        .stage_write(session_id, tx_id, query, params)?;
                    Ok(format!("OK\tTX_STAGED\t{tx_id}\t{staged_count}"))
                } else {
                    let cursor = self
                        .transactions
                        .query_cursor(&self.db, session_id, tx_id, &query, &params)
                        .map_err(|err| err.to_string())?;
                    let total_rows = cursor.total_rows();
                    let cursor_id = self.cursors.insert(session_id, cursor);
                    let page = self
                        .cursors
                        .fetch(session_id, cursor_id, self.default_page_size)?;
                    Ok(format_result_start(cursor_id, total_rows, page))
                }
            }
            TransactionCommand::PreparedQueryPlan {
                tx_id,
                prepared_id,
                params,
            } => {
                let query = self.prepared_queries.get(session_id, prepared_id)?;
                validate_prepared_query_params(prepared_id, &query, &params)?;
                let context = self.transactions.plan_context(session_id, tx_id)?;
                let mut plan = format_query_plan(
                    &self
                        .db
                        .query_plan_with_params(&query, params)
                        .map_err(|err| err.to_string())?,
                );
                plan.push(' ');
                plan.push_str(&format_transaction_plan_context(&context));
                Ok(format_response(&BackendResponse::OkQueryPlan(plan)))
            }
            TransactionCommand::PreparedQueryRoute {
                tx_id,
                prepared_id,
                params,
            } => {
                let query = self.prepared_queries.get(session_id, prepared_id)?;
                validate_prepared_query_params(prepared_id, &query, &params)?;
                let context = self.transactions.plan_context(session_id, tx_id)?;
                Ok(format_tx_prepared_query_route(
                    tx_id,
                    prepared_id,
                    self.prepared_query_routing_hint_with_params(&query, &params)?,
                    &context,
                ))
            }
            TransactionCommand::DistributedQuery {
                tx_id,
                query,
                params,
            } => {
                if is_write_cypher(&query) {
                    return Err("TX_QUERY_DISTRIBUTED only supports read queries".to_string());
                }
                let cursor = self
                    .transactions
                    .distributed_query_cursor(
                        &self.db,
                        &self.query_peers,
                        self.read_preference,
                        session_id,
                        tx_id,
                        &query,
                        &params,
                    )
                    .map_err(|err| err.to_string())?;
                let total_rows = cursor.total_rows();
                let cursor_id = self.cursors.insert(session_id, cursor);
                let page = self
                    .cursors
                    .fetch(session_id, cursor_id, self.default_page_size)?;
                Ok(format_result_start(cursor_id, total_rows, page))
            }
            TransactionCommand::QueryPlan {
                tx_id,
                query,
                params,
            } => {
                let context = self.transactions.plan_context(session_id, tx_id)?;
                let mut plan = format_query_plan(
                    &self
                        .db
                        .query_plan_with_params(&query, params)
                        .map_err(|err| err.to_string())?,
                );
                plan.push(' ');
                plan.push_str(&format_transaction_plan_context(&context));
                Ok(format_response(&BackendResponse::OkQueryPlan(plan)))
            }
            TransactionCommand::Commit { tx_id } => {
                let staged_writes = self.transactions.staged_writes(session_id, tx_id)?;
                let transaction_writes = self.writes_by_target_shard(&staged_writes)?;
                if transaction_writes.len() > 1
                    && !staged_writes_are_prepare_batchable(&staged_writes)
                {
                    return Err(
                        "multi-shard read-write transaction commit currently requires prepare-batchable CREATE, MERGE, SET, REMOVE, or DELETE writes"
                            .to_string(),
                    );
                }
                let tx = self.transactions.close(session_id, tx_id)?;
                let write_count = staged_writes.len();
                if matches!(tx, NativeTransaction::ReadWrite { .. }) {
                    if !self.try_execute_staged_writes_as_local_batch(
                        tx_id,
                        &transaction_writes,
                        &staged_writes,
                    )? {
                        for staged in staged_writes {
                            self.execute_write_query_with_routing(&staged.query, staged.params)?;
                        }
                    }
                }
                if write_count == 0 {
                    Ok(format!("OK\tTX_COMMIT\t{tx_id}"))
                } else {
                    Ok(format!("OK\tTX_COMMIT\t{tx_id}\t{write_count}"))
                }
            }
            TransactionCommand::Rollback { tx_id } => {
                self.transactions.close(session_id, tx_id)?;
                Ok(format!("OK\tTX_ROLLBACK\t{tx_id}"))
            }
            TransactionCommand::Kill { tx_id } => {
                let info = self.transactions.close_any(tx_id)?;
                Ok(format!("OK\tTX_KILL\t{tx_id}\t{}", info.session_id))
            }
            TransactionCommand::Status { tx_id } => {
                let info = self.transactions.status(session_id, tx_id)?;
                Ok(format_tx_status(info))
            }
            TransactionCommand::PrepareWriteBatchShard { shard_id, writes } => {
                self.ensure_local_primary_shard(shard_id)?;
                if !writes
                    .iter()
                    .all(|(query, _)| is_batchable_cypher_mutation(query))
                {
                    return Err(
                        "prepared write batch currently supports CREATE, MERGE, SET, REMOVE, or DELETE writes only"
                            .to_string(),
                    );
                }
                let write_count = writes.len();
                let prepared_id = self.prepared_transactions.prepare(shard_id, writes)?;
                Ok(format!(
                    "OK\tTX_PREPARED\t{prepared_id}\t{shard_id}\t{write_count}"
                ))
            }
            TransactionCommand::CommitPrepared { prepared_id } => {
                let prepared = self.prepared_transactions.take(prepared_id)?;
                self.db
                    .execute_staged_cypher_transaction_on_shard(prepared.shard_id, prepared.writes)
                    .map_err(|err| err.to_string())?;
                Ok(format!("OK\tTX_PREPARED_COMMIT\t{prepared_id}"))
            }
            TransactionCommand::AbortPrepared { prepared_id } => {
                self.prepared_transactions.take(prepared_id)?;
                Ok(format!("OK\tTX_PREPARED_ABORT\t{prepared_id}"))
            }
            TransactionCommand::PreparedStatus { prepared_id } => {
                let info = self.prepared_transactions.status(prepared_id)?;
                Ok(format_prepared_tx_status(info))
            }
            TransactionCommand::ListPrepared => {
                Ok(format_prepared_tx_list(self.prepared_transactions.list()?))
            }
            TransactionCommand::List => Ok(format_tx_list(self.transactions.list(session_id)?)),
            TransactionCommand::ListAll => Ok(format_tx_list_all(self.transactions.list_all()?)),
        }
    }

    fn ensure_local_primary_shard(&self, shard_id: u64) -> Result<(), String> {
        let status = self.db.cluster_status().map_err(|err| err.to_string())?;
        let shard = status
            .shards
            .iter()
            .find(|shard| shard.shard_id == shard_id)
            .ok_or_else(|| format!("missing shard {shard_id} in cluster status"))?;
        if shard.is_local_primary {
            Ok(())
        } else {
            Err(format!("shard {shard_id} is not a local primary"))
        }
    }

    fn prepared_query_routing_hint(&self, query: &str) -> Result<String, String> {
        prepared_query_routing_hint(&self.db, query)
    }

    fn prepared_query_routing_hint_with_params(
        &self,
        query: &str,
        params: &neo4r_query::QueryParams,
    ) -> Result<String, String> {
        prepared_query_routing_hint_with_params(&self.db, query, params)
    }

    fn writes_by_target_shard(
        &self,
        staged_writes: &[StagedWrite],
    ) -> Result<BTreeMap<u64, Vec<(String, neo4r_query::QueryParams)>>, String> {
        let status = self.db.cluster_status().map_err(|err| err.to_string())?;
        let mut transaction_writes: BTreeMap<u64, Vec<(String, neo4r_query::QueryParams)>> =
            BTreeMap::new();
        for staged in staged_writes {
            let shards = if is_create_node_cypher(&staged.query) {
                vec![
                    select_create_node_write_shard(&status, &staged.query, &staged.params)?
                        .shard_id,
                ]
            } else if is_merge_node_cypher(&staged.query) {
                vec![
                    select_merge_node_write_shard(&status, &staged.query, &staged.params)?.shard_id,
                ]
            } else {
                self.db
                    .write_cypher_target_shards(&staged.query, staged.params.clone())
                    .map_err(|err| err.to_string())?
            };
            if shards.is_empty() && is_staged_transaction_overlay_cypher(&staged.query) {
                continue;
            }
            if shards.len() != 1 && !is_batchable_multi_target_transaction_cypher(&staged.query) {
                return Err(format!(
                    "read-write transaction commit requires each staged write to target exactly one shard; query {:?} targets {:?}",
                    staged.query, shards
                ));
            }
            for shard_id in shards {
                transaction_writes
                    .entry(shard_id)
                    .or_default()
                    .push((staged.query.clone(), staged.params.clone()));
            }
        }
        Ok(transaction_writes)
    }

    fn try_execute_staged_writes_as_local_batch(
        &self,
        tx_id: u64,
        transaction_writes: &BTreeMap<u64, Vec<(String, neo4r_query::QueryParams)>>,
        staged_writes: &[StagedWrite],
    ) -> Result<bool, String> {
        if transaction_writes.is_empty() {
            return Ok(false);
        }
        if staged_writes.is_empty() {
            return Ok(false);
        }
        let status = self.db.cluster_status().map_err(|err| err.to_string())?;
        if transaction_writes.len() == 1 {
            let (shard_id, writes) = transaction_writes.iter().next().unwrap();
            let shard_id = *shard_id;
            let Some(shard) = status
                .shards
                .iter()
                .find(|shard| shard.shard_id == shard_id)
            else {
                return Err(format!("missing shard {shard_id} in cluster status"));
            };
            if shard.is_local_primary {
                let all_writes = staged_writes
                    .iter()
                    .map(|staged| (staged.query.clone(), staged.params.clone()))
                    .collect::<Vec<_>>();
                if staged_writes
                    .iter()
                    .all(|staged| is_staged_transaction_overlay_cypher(&staged.query))
                {
                    self.db
                        .execute_staged_cypher_transaction_on_shard(shard_id, all_writes)
                        .map_err(|err| err.to_string())?;
                    return Ok(true);
                }
                if staged_writes
                    .iter()
                    .all(|staged| is_batchable_transaction_set_cypher(&staged.query))
                {
                    self.db
                        .execute_cypher_mutation_batch_on_shard(shard_id, writes.clone())
                        .map_err(|err| err.to_string())?;
                    return Ok(true);
                }
                return Ok(false);
            }
            if !staged_writes_are_prepare_batchable(staged_writes) {
                return Ok(false);
            }
            let primary = shard
                .primary_server_id
                .ok_or_else(|| format!("missing primary for write shard {shard_id}"))?;
            let address = self.query_peers.address(primary)?.ok_or_else(|| {
                format!(
                    "missing query peer address for primary server {primary} on write shard {shard_id}"
                )
            })?;
            request_remote_prepare_commit_batch(&self.db, tx_id, &address, shard_id, writes)?;
            return Ok(true);
        }

        if !staged_writes_are_prepare_batchable(staged_writes) {
            return Ok(false);
        }

        let mut shard_statuses = Vec::new();
        for shard_id in transaction_writes.keys() {
            let Some(shard) = status
                .shards
                .iter()
                .find(|shard| shard.shard_id == *shard_id)
            else {
                return Err(format!("missing shard {shard_id} in cluster status"));
            };
            shard_statuses.push(shard.clone());
        }
        if shard_statuses.iter().all(|shard| shard.is_local_primary) {
            if staged_writes
                .iter()
                .all(|staged| is_batchable_transaction_set_cypher(&staged.query))
            {
                let writes = staged_writes
                    .iter()
                    .map(|staged| (staged.query.clone(), staged.params.clone()))
                    .collect();
                self.db
                    .execute_cypher_mutation_batch(writes)
                    .map_err(|err| err.to_string())?;
            } else {
                let local_participants = transaction_writes
                    .iter()
                    .map(|(shard_id, writes)| (*shard_id, writes.clone()))
                    .collect();
                self.prepare_commit_mixed_batches(tx_id, local_participants, Vec::new())?;
            }
            return Ok(true);
        }
        let mut remote_participants = Vec::new();
        let mut local_participants = Vec::new();
        for shard in shard_statuses {
            let shard_id = shard.shard_id;
            let writes = transaction_writes
                .get(&shard_id)
                .cloned()
                .unwrap_or_default();
            if shard.is_local_primary {
                local_participants.push((shard_id, writes));
                continue;
            }
            let primary = shard
                .primary_server_id
                .ok_or_else(|| format!("missing primary for write shard {shard_id}"))?;
            let address = self.query_peers.address(primary)?.ok_or_else(|| {
                format!(
                    "missing query peer address for primary server {primary} on write shard {shard_id}"
                )
            })?;
            remote_participants.push((address, shard_id, writes));
        }
        if local_participants.is_empty() {
            request_remote_prepare_commit_batches(&self.db, tx_id, remote_participants)?;
        } else {
            self.prepare_commit_mixed_batches(tx_id, local_participants, remote_participants)?;
        }
        Ok(true)
    }

    fn prepare_commit_mixed_batches(
        &self,
        tx_id: u64,
        local_participants: Vec<(u64, Vec<(String, neo4r_query::QueryParams)>)>,
        remote_participants: Vec<(String, u64, Vec<(String, neo4r_query::QueryParams)>)>,
    ) -> Result<(), String> {
        let mut prepared_locals = Vec::new();
        let mut prepared_remotes = Vec::new();

        for (shard_id, writes) in local_participants {
            if let Err(err) = self.ensure_local_primary_shard(shard_id) {
                record_abort_decision(
                    &self.db,
                    tx_id,
                    decision_participant_records(&prepared_locals, &prepared_remotes),
                )?;
                self.abort_local_prepared_batches(prepared_locals);
                abort_prepared_participants(prepared_remotes);
                return Err(err);
            }
            match self.prepared_transactions.prepare(shard_id, writes) {
                Ok(prepared_id) => prepared_locals.push((shard_id, prepared_id)),
                Err(err) => {
                    record_abort_decision(
                        &self.db,
                        tx_id,
                        decision_participant_records(&prepared_locals, &prepared_remotes),
                    )?;
                    self.abort_local_prepared_batches(prepared_locals);
                    abort_prepared_participants(prepared_remotes);
                    return Err(err);
                }
            }
        }

        for (address, shard_id, writes) in remote_participants {
            let mut stream = TcpStream::connect(&address)
                .map_err(|err| format!("connect write peer {address}: {err}"))?;
            let response = match request_command_on_stream(
                &mut stream,
                1,
                &format_tx_prepare_write_batch_shard_payload(shard_id, &writes),
            ) {
                Ok(response) => response,
                Err(err) => {
                    record_abort_decision(
                        &self.db,
                        tx_id,
                        decision_participant_records(&prepared_locals, &prepared_remotes),
                    )?;
                    self.abort_local_prepared_batches(prepared_locals);
                    abort_prepared_participants(prepared_remotes);
                    return Err(err);
                }
            };
            let prepared_id = match parse_tx_prepared_response(&response) {
                Ok(prepared_id) => prepared_id,
                Err(err) => {
                    record_abort_decision(
                        &self.db,
                        tx_id,
                        decision_participant_records(&prepared_locals, &prepared_remotes),
                    )?;
                    self.abort_local_prepared_batches(prepared_locals);
                    abort_prepared_participants(prepared_remotes);
                    return Err(err);
                }
            };
            prepared_remotes.push(RemotePreparedParticipant {
                stream,
                address,
                shard_id,
                prepared_id,
            });
        }

        record_commit_decision(
            &self.db,
            tx_id,
            prepared_locals
                .iter()
                .map(|(shard_id, prepared_id)| TransactionParticipantRecord {
                    location: "local".to_string(),
                    shard_id: *shard_id,
                    prepared_id: *prepared_id,
                })
                .chain(
                    prepared_remotes
                        .iter()
                        .map(|participant| TransactionParticipantRecord {
                            location: format!("remote:{}", participant.address),
                            shard_id: participant.shard_id,
                            prepared_id: participant.prepared_id,
                        }),
                )
                .collect(),
        )?;

        while !prepared_locals.is_empty() {
            let (_, prepared_id) = prepared_locals.remove(0);
            if let Err(err) = self.commit_local_prepared_batch(prepared_id) {
                return Err(err);
            }
        }

        while !prepared_remotes.is_empty() {
            let mut participant = prepared_remotes.remove(0);
            if let Err(err) = request_command_on_stream(
                &mut participant.stream,
                2,
                &format!("TX_COMMIT_PREPARED\t{}", participant.prepared_id),
            ) {
                return Err(format!(
                    "commit prepared transaction {} on {} failed: {err}",
                    participant.prepared_id, participant.address
                ));
            }
        }
        let _ = clear_transaction_decision(&self.db, tx_id);
        Ok(())
    }

    fn commit_local_prepared_batch(&self, prepared_id: u64) -> Result<(), String> {
        let prepared = self.prepared_transactions.take(prepared_id)?;
        self.db
            .execute_staged_cypher_transaction_on_shard(prepared.shard_id, prepared.writes)
            .map(|_| ())
            .map_err(|err| err.to_string())
    }

    fn abort_local_prepared_batches(&self, prepared_ids: Vec<(u64, u64)>) {
        for (_, prepared_id) in prepared_ids {
            let _ = self.prepared_transactions.take(prepared_id);
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
struct ReplicationPeerStatusEntry {
    server_id: u64,
    address: Option<String>,
    primary_shards: Vec<u64>,
    replica_shards: Vec<u64>,
}

fn replication_peer_status(
    db: &Neo4rDatabaseHandle,
    replication_peers: &QueryPeerStore,
    server_id: Option<u64>,
) -> Result<Vec<ReplicationPeerStatusEntry>, String> {
    let routing_table = db.routing_table().map_err(|err| err.to_string())?;
    let local_server_id = db
        .cluster_status()
        .map_err(|err| err.to_string())?
        .server_id;
    let peer_addresses = replication_peers
        .list()
        .map_err(|err| err.to_string())?
        .into_iter()
        .collect::<BTreeMap<_, _>>();
    let mut server_ids = BTreeSet::new();
    if let Some(server_id) = server_id {
        server_ids.insert(server_id);
    } else {
        server_ids.extend(peer_addresses.keys().copied());
        for placement in &routing_table.placements {
            for replica in &placement.replicas {
                if replica.server_id != local_server_id {
                    server_ids.insert(replica.server_id);
                }
            }
        }
    }

    let mut statuses = Vec::new();
    for server_id in server_ids {
        let mut primary_shards = Vec::new();
        let mut replica_shards = Vec::new();
        for placement in &routing_table.placements {
            if placement.primary_server_id() == Some(server_id) {
                primary_shards.push(placement.shard_id);
            } else if placement.has_server(server_id) {
                replica_shards.push(placement.shard_id);
            }
        }
        statuses.push(ReplicationPeerStatusEntry {
            server_id,
            address: peer_addresses.get(&server_id).cloned(),
            primary_shards,
            replica_shards,
        });
    }
    statuses.sort_by_key(|entry| entry.server_id);
    Ok(statuses)
}

fn format_replication_peer_status(entries: &[ReplicationPeerStatusEntry]) -> String {
    entries
        .iter()
        .map(|entry| {
            let address = entry.address.as_deref().unwrap_or("missing");
            format!(
                "server={} address={} primary_shards={} replica_shards={}",
                entry.server_id,
                address,
                format_shard_id_list(&entry.primary_shards),
                format_shard_id_list(&entry.replica_shards)
            )
        })
        .collect::<Vec<_>>()
        .join(",")
}

fn format_shard_id_list(shards: &[u64]) -> String {
    if shards.is_empty() {
        "-".to_string()
    } else {
        shards
            .iter()
            .map(u64::to_string)
            .collect::<Vec<_>>()
            .join("|")
    }
}

fn replication_status(
    db: &Neo4rDatabaseHandle,
    replication_peers: &QueryPeerStore,
) -> Result<String, String> {
    let status = db.cluster_status().map_err(|err| err.to_string())?;
    let peers = replication_peers.list().map_err(|err| err.to_string())?;
    let peers = if peers.is_empty() {
        "none".to_string()
    } else {
        format_query_peers(&peers)
    };
    let shards = status
        .shards
        .iter()
        .map(format_replication_shard_status)
        .collect::<Vec<_>>()
        .join(",");
    Ok(format!(
        "server={} routing_version={} peers={} shards={}",
        status.server_id, status.routing_version, peers, shards
    ))
}

fn format_replication_shard_status(status: &neo4r_db::ShardStatus) -> String {
    let primary = status
        .primary_server_id
        .map(|server_id| server_id.to_string())
        .unwrap_or_else(|| "none".to_string());
    let replicas = if status.replica_server_ids.is_empty() {
        "none".to_string()
    } else {
        status
            .replica_server_ids
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("|")
    };
    let matches = if status.match_indexes.is_empty() {
        "none".to_string()
    } else {
        status
            .match_indexes
            .iter()
            .map(|(server_id, index)| format!("{server_id}:{index}"))
            .collect::<Vec<_>>()
            .join("|")
    };
    let lag = if status.replica_server_ids.is_empty() {
        "none".to_string()
    } else {
        status
            .replica_server_ids
            .iter()
            .map(|server_id| {
                let match_index = status
                    .match_indexes
                    .iter()
                    .find(|(matched_server_id, _)| matched_server_id == server_id)
                    .map(|(_, index)| *index);
                match match_index {
                    Some(index) => {
                        format!(
                            "{server_id}:{}",
                            status.committed_index.saturating_sub(index)
                        )
                    }
                    None => format!("{server_id}:unknown"),
                }
            })
            .collect::<Vec<_>>()
            .join("|")
    };
    format!(
        "shard:{}:primary={}:replicas={}:local={}:local_primary={}:applied={}:committed={}:match={}:lag={}",
        status.shard_id,
        primary,
        replicas,
        status.has_local_copy,
        status.is_local_primary,
        status.applied_index,
        status.committed_index,
        matches,
        lag
    )
}

fn catch_up_from_primaries(
    db: &Neo4rDatabaseHandle,
    replication_peers: &QueryPeerStore,
    connect_timeout: Duration,
    max_entries_per_request: Option<usize>,
) -> Result<Vec<neo4r_db::TcpCatchUpResult>, String> {
    let routing_table = db.routing_table().map_err(|err| err.to_string())?;
    let status = db.cluster_status().map_err(|err| err.to_string())?;
    let peer_addresses = replication_peers
        .list()
        .map_err(|err| err.to_string())?
        .into_iter()
        .collect::<BTreeMap<_, _>>();
    match max_entries_per_request {
        Some(max_entries_per_request) => neo4r_db::catch_up_from_tcp_primaries_batched(
            db,
            &routing_table,
            &peer_addresses,
            status.server_id,
            connect_timeout,
            max_entries_per_request,
        ),
        None => neo4r_db::catch_up_from_tcp_primaries(
            db,
            &routing_table,
            &peer_addresses,
            status.server_id,
            connect_timeout,
        ),
    }
    .map_err(|err| err.to_string())
}

fn catch_up_from_primary(
    db: &Neo4rDatabaseHandle,
    replication_peers: &QueryPeerStore,
    connect_timeout: Duration,
    server_id: u64,
    max_entries_per_request: Option<usize>,
) -> Result<Vec<neo4r_db::TcpCatchUpResult>, String> {
    let routing_table = db.routing_table().map_err(|err| err.to_string())?;
    let status = db.cluster_status().map_err(|err| err.to_string())?;
    let peer_addresses = replication_peers
        .list()
        .map_err(|err| err.to_string())?
        .into_iter()
        .collect::<BTreeMap<_, _>>();
    let address = peer_addresses
        .get(&server_id)
        .ok_or_else(|| format!("missing peer address for primary server {server_id}"))?;
    let committed_indexes = db.committed_indexes().map_err(|err| err.to_string())?;
    let mut results = Vec::new();
    for placement in &routing_table.placements {
        if !placement.has_server(status.server_id) {
            continue;
        }
        if placement.primary_server_id() != Some(server_id) {
            continue;
        }
        if server_id == status.server_id {
            continue;
        }
        let start_index = committed_indexes
            .get(placement.shard_id as usize)
            .copied()
            .ok_or_else(|| format!("missing committed index for shard {}", placement.shard_id))?
            + 1;
        let fetched_entries = match max_entries_per_request {
            Some(max_entries_per_request) => catch_up_from_tcp_primary_batched(
                db,
                address,
                connect_timeout,
                placement.shard_id,
                start_index,
                max_entries_per_request,
            ),
            None => catch_up_from_tcp_primary(
                db,
                address,
                connect_timeout,
                placement.shard_id,
                start_index,
            ),
        }
        .map_err(|err| err.to_string())?;
        results.push(neo4r_db::TcpCatchUpResult {
            shard_id: placement.shard_id,
            start_index,
            end_index: catch_up_end_index(start_index, fetched_entries),
            fetched_entries,
            primary_server_id: server_id,
        });
    }
    Ok(results)
}

#[derive(Debug, Eq, PartialEq)]
struct CatchUpPlanEntry {
    shard_id: u64,
    primary_server_id: u64,
    start_index: u64,
    peer_registered: bool,
}

fn catch_up_plan(
    db: &Neo4rDatabaseHandle,
    replication_peers: &QueryPeerStore,
    server_id: Option<u64>,
) -> Result<Vec<CatchUpPlanEntry>, String> {
    let routing_table = db.routing_table().map_err(|err| err.to_string())?;
    let status = db.cluster_status().map_err(|err| err.to_string())?;
    let peer_addresses = replication_peers
        .list()
        .map_err(|err| err.to_string())?
        .into_iter()
        .collect::<BTreeMap<_, _>>();
    let committed_indexes = db.committed_indexes().map_err(|err| err.to_string())?;
    let mut entries = Vec::new();
    for placement in &routing_table.placements {
        if !placement.has_server(status.server_id) {
            continue;
        }
        let Some(primary_server_id) = placement.primary_server_id() else {
            return Err(format!("missing primary for shard {}", placement.shard_id));
        };
        if Some(primary_server_id) != server_id && server_id.is_some() {
            continue;
        }
        if primary_server_id == status.server_id {
            continue;
        }
        let start_index = committed_indexes
            .get(placement.shard_id as usize)
            .copied()
            .ok_or_else(|| format!("missing committed index for shard {}", placement.shard_id))?
            + 1;
        entries.push(CatchUpPlanEntry {
            shard_id: placement.shard_id,
            primary_server_id,
            start_index,
            peer_registered: peer_addresses.contains_key(&primary_server_id),
        });
    }
    entries.sort_by_key(|entry| (entry.primary_server_id, entry.shard_id));
    Ok(entries)
}

fn catch_up_end_index(start_index: u64, fetched_entries: usize) -> u64 {
    start_index
        .saturating_add(fetched_entries as u64)
        .saturating_sub(1)
}

fn format_catch_up_plan(entries: &[CatchUpPlanEntry]) -> String {
    entries
        .iter()
        .map(|entry| {
            let peer = if entry.peer_registered {
                "registered"
            } else {
                "missing"
            };
            format!(
                "shard={} primary={} start={} peer={peer}",
                entry.shard_id, entry.primary_server_id, entry.start_index
            )
        })
        .collect::<Vec<_>>()
        .join(",")
}

fn format_catch_up_results(results: &[neo4r_db::TcpCatchUpResult]) -> String {
    results
        .iter()
        .map(|result| {
            format!(
                "shard={} primary={} start={} end={} fetched={}",
                result.shard_id,
                result.primary_server_id,
                result.start_index,
                result.end_index,
                result.fetched_entries
            )
        })
        .collect::<Vec<_>>()
        .join(",")
}

fn sync_index_catalog_from_peer(
    db: &Neo4rDatabaseHandle,
    query_peers: &QueryPeerStore,
    server_id: u64,
) -> Result<(), String> {
    let address = query_peers
        .address(server_id)?
        .ok_or_else(|| format!("missing query peer address for server {server_id}"))?;
    let response = request_remote_command(&address, "DUMP_INDEX_CATALOG")?;
    let catalog = parse_ok_index_catalog_response(&response)?;
    db.install_index_catalog(catalog)
        .map_err(|err| err.to_string())
}

fn execute_distributed_query(
    db: &Neo4rDatabaseHandle,
    query_peers: &QueryPeerStore,
    read_preference: QueryReadPreference,
    query: &str,
    params: &neo4r_query::QueryParams,
) -> Result<Vec<QueryRow>, String> {
    let status = db.cluster_status().map_err(|err| err.to_string())?;
    let mut rows = Vec::new();
    for shard in status.shards {
        if shard.has_local_copy {
            rows.extend(
                db.query_shard_with_params(shard.shard_id, query, params.clone())
                    .map_err(|err| err.to_string())?,
            );
        } else {
            let (_target, address) =
                select_remote_query_target(query_peers, &shard, read_preference)?;
            rows.extend(request_remote_query_shard(
                &address,
                shard.shard_id,
                query,
                params,
            )?);
        }
    }
    Ok(rows)
}

fn build_distributed_query_cursor(
    db: &Neo4rDatabaseHandle,
    query_peers: &QueryPeerStore,
    read_preference: QueryReadPreference,
    query: &str,
    params: &neo4r_query::QueryParams,
) -> Result<Box<dyn QueryCursor>, String> {
    build_distributed_query_cursor_with_options(
        db,
        query_peers,
        read_preference,
        query,
        params,
        QueryOptions::default(),
    )
}

fn build_distributed_query_cursor_with_options(
    db: &Neo4rDatabaseHandle,
    query_peers: &QueryPeerStore,
    read_preference: QueryReadPreference,
    query: &str,
    params: &neo4r_query::QueryParams,
    options: QueryOptions,
) -> Result<Box<dyn QueryCursor>, String> {
    let status = db.cluster_status().map_err(|err| err.to_string())?;
    let mut cursors = Vec::<Box<dyn QueryCursor>>::new();
    for shard in status.shards {
        if shard.has_local_copy {
            let rows = db
                .query_shard_with_params_and_options(shard.shard_id, query, params.clone(), options)
                .map_err(|err| err.to_string())?;
            cursors.push(Box::new(VecQueryCursor::new(rows)));
        } else {
            let (_target, address) =
                select_remote_query_target(query_peers, &shard, read_preference)?;
            cursors.push(Box::new(RemoteShardQueryCursor::open(
                &address,
                shard.shard_id,
                query,
                params,
            )?));
        }
    }
    Ok(Box::new(DistributedQueryCursor::new(cursors)))
}

fn build_distributed_query_cursor_with_local_staged_writes(
    db: &Neo4rDatabaseHandle,
    query_peers: &QueryPeerStore,
    read_preference: QueryReadPreference,
    query: &str,
    params: &neo4r_query::QueryParams,
    options: QueryOptions,
    staged_writes: &[(String, neo4r_query::QueryParams)],
) -> Result<Box<dyn QueryCursor>, String> {
    let status = db.cluster_status().map_err(|err| err.to_string())?;
    let mut cursors = Vec::<Box<dyn QueryCursor>>::new();
    for shard in status.shards {
        if shard.has_local_copy {
            let rows = db
                .query_shard_with_staged_writes(
                    shard.shard_id,
                    query,
                    params.clone(),
                    options,
                    staged_writes,
                )
                .map_err(|err| err.to_string())?;
            cursors.push(Box::new(VecQueryCursor::new(rows)));
        } else {
            let (_target, address) =
                select_remote_query_target(query_peers, &shard, read_preference)?;
            cursors.push(Box::new(RemoteShardQueryCursor::open_with_staged_writes(
                &address,
                shard.shard_id,
                query,
                params,
                staged_writes,
            )?));
        }
    }
    Ok(Box::new(DistributedQueryCursor::new(cursors)))
}

fn build_distributed_read_tx_cursor(
    db: &Neo4rDatabaseHandle,
    query_peers: &QueryPeerStore,
    read_preference: QueryReadPreference,
    tx: &Neo4rReadTransaction,
    query: &str,
    params: &neo4r_query::QueryParams,
) -> Result<Box<dyn QueryCursor>, String> {
    let status = db.cluster_status().map_err(|err| err.to_string())?;
    let mut cursors = Vec::<Box<dyn QueryCursor>>::new();
    for shard in status.shards {
        if shard.has_local_copy {
            let rows = tx
                .query_shard_with_params(shard.shard_id, query, params)
                .map_err(|err| err.to_string())?;
            cursors.push(Box::new(VecQueryCursor::new(rows)));
        } else {
            let (_target, address) =
                select_remote_query_target(query_peers, &shard, read_preference)?;
            cursors.push(Box::new(RemoteShardQueryCursor::open(
                &address,
                shard.shard_id,
                query,
                params,
            )?));
        }
    }
    Ok(Box::new(DistributedQueryCursor::new(cursors)))
}

fn select_remote_query_target(
    query_peers: &QueryPeerStore,
    shard: &neo4r_db::ShardStatus,
    read_preference: QueryReadPreference,
) -> Result<(u64, String), String> {
    if read_preference == QueryReadPreference::PreferReplica {
        for replica in &shard.replica_server_ids {
            if let Some(address) = query_peers.address(*replica)? {
                return Ok((*replica, address));
            }
        }
    }

    let primary = shard
        .primary_server_id
        .ok_or_else(|| format!("missing primary for remote query shard {}", shard.shard_id))?;
    if let Some(address) = query_peers.address(primary)? {
        return Ok((primary, address));
    }

    if read_preference == QueryReadPreference::Primary {
        return Err(format!(
            "missing query peer address for primary server {primary} on shard {}",
            shard.shard_id
        ));
    }

    Err(format!(
        "missing query peer address for preferred replica or primary on shard {}",
        shard.shard_id
    ))
}

struct DistributedQueryCursor {
    cursors: Vec<Box<dyn QueryCursor>>,
    current: usize,
}

impl DistributedQueryCursor {
    fn new(cursors: Vec<Box<dyn QueryCursor>>) -> Self {
        Self {
            cursors,
            current: 0,
        }
    }
}

impl QueryCursor for DistributedQueryCursor {
    fn fetch(&mut self, page_size: usize) -> neo4r_query::QueryPage {
        let page_size = page_size.max(1);
        let mut rows = Vec::new();
        while rows.len() < page_size && self.current < self.cursors.len() {
            let remaining = page_size - rows.len();
            let page = self.cursors[self.current].fetch(remaining);
            rows.extend(page.rows);
            if !page.has_more {
                self.current += 1;
            }
        }
        neo4r_query::QueryPage {
            rows,
            has_more: self.current < self.cursors.len(),
        }
    }

    fn total_rows(&self) -> Option<usize> {
        self.cursors
            .iter()
            .map(|cursor| cursor.total_rows())
            .try_fold(0_usize, |sum, total| total.map(|total| sum + total))
    }
}

struct RemoteShardQueryCursor {
    stream: TcpStream,
    cursor_id: u64,
    buffered_rows: Vec<QueryRow>,
    remote_has_more: bool,
    total_rows: Option<usize>,
    next_request_id: u64,
}

impl RemoteShardQueryCursor {
    fn open(
        address: &str,
        shard_id: u64,
        query: &str,
        params: &neo4r_query::QueryParams,
    ) -> Result<Self, String> {
        Self::open_with_payload(
            address,
            format_query_shard_payload(shard_id, query, params)?,
            "query shard",
        )
    }

    fn open_with_staged_writes(
        address: &str,
        shard_id: u64,
        query: &str,
        params: &neo4r_query::QueryParams,
        staged_writes: &[(String, neo4r_query::QueryParams)],
    ) -> Result<Self, String> {
        Self::open_with_payload(
            address,
            format_query_staged_shard_payload(shard_id, query, params, staged_writes),
            "staged query shard",
        )
    }

    fn open_with_payload(address: &str, payload: String, operation: &str) -> Result<Self, String> {
        let mut stream = TcpStream::connect(address)
            .map_err(|err| format!("connect query peer {address}: {err}"))?;
        write_frame(
            &mut stream,
            &NativeFrame::new(NativeMessageType::Command, 1, payload.into_bytes()),
        )
        .map_err(|err| format!("write remote {operation} cursor request: {err}"))?;
        let frame = read_frame(&mut stream)
            .map_err(|err| format!("read remote {operation} cursor response: {err}"))?
            .ok_or_else(|| format!("remote query peer closed without {operation} response"))?;
        if frame.message_type != NativeMessageType::Response {
            return Err(format!(
                "remote {operation} cursor failed: {}",
                frame.payload_text().map_err(|err| err.to_string())?
            ));
        }
        let start =
            parse_result_start_response(frame.payload_text().map_err(|err| err.to_string())?)?;
        Ok(Self {
            stream,
            cursor_id: start.cursor_id,
            buffered_rows: start.rows,
            remote_has_more: start.has_more,
            total_rows: start.total_rows,
            next_request_id: 2,
        })
    }

    fn fetch_remote(&mut self, page_size: usize) -> Result<(), String> {
        if !self.remote_has_more {
            return Ok(());
        }
        let request_id = self.next_request_id;
        self.next_request_id += 1;
        write_frame(
            &mut self.stream,
            &NativeFrame::new(
                NativeMessageType::Fetch,
                request_id,
                format!("{}\t{page_size}", self.cursor_id).into_bytes(),
            ),
        )
        .map_err(|err| format!("write remote fetch request: {err}"))?;
        let frame = read_frame(&mut self.stream)
            .map_err(|err| format!("read remote fetch response: {err}"))?
            .ok_or_else(|| "remote query peer closed without fetch response".to_string())?;
        if frame.message_type != NativeMessageType::Response {
            self.remote_has_more = false;
            return Err(format!(
                "remote fetch failed: {}",
                frame.payload_text().map_err(|err| err.to_string())?
            ));
        }
        let page = parse_result_page_response(frame.payload_text().unwrap_or(""))?;
        self.buffered_rows.extend(page.rows);
        self.remote_has_more = page.has_more;
        Ok(())
    }
}

impl QueryCursor for RemoteShardQueryCursor {
    fn fetch(&mut self, page_size: usize) -> neo4r_query::QueryPage {
        let page_size = page_size.max(1);
        while self.buffered_rows.len() < page_size && self.remote_has_more {
            if self
                .fetch_remote(page_size.saturating_sub(self.buffered_rows.len()))
                .is_err()
            {
                self.remote_has_more = false;
                break;
            }
        }
        let take = page_size.min(self.buffered_rows.len());
        let rows = self.buffered_rows.drain(..take).collect::<Vec<_>>();
        neo4r_query::QueryPage {
            rows,
            has_more: !self.buffered_rows.is_empty() || self.remote_has_more,
        }
    }

    fn total_rows(&self) -> Option<usize> {
        self.total_rows
    }
}

struct ResultStart {
    cursor_id: u64,
    total_rows: Option<usize>,
    rows: Vec<QueryRow>,
    has_more: bool,
}

struct RemoteResultPage {
    rows: Vec<QueryRow>,
    has_more: bool,
}

fn parse_result_start_response(payload: &str) -> Result<ResultStart, String> {
    let parts = payload.splitn(7, '\t').collect::<Vec<_>>();
    if parts.len() != 7 || parts[0] != "OK" || parts[1] != "RESULT_START" {
        return Err(format!(
            "remote query returned non-cursor response: {payload}"
        ));
    }
    let row_count = parts[4]
        .parse::<usize>()
        .map_err(|_| "RESULT_START row count must be an unsigned integer".to_string())?;
    let rows = decode_query_rows(parts[6])?;
    if rows.len() != row_count {
        return Err(format!(
            "RESULT_START row count mismatch: header {row_count}, decoded {}",
            rows.len()
        ));
    }
    Ok(ResultStart {
        cursor_id: parse_cursor_id(parts[2])?,
        total_rows: if parts[3] == "UNKNOWN" {
            None
        } else {
            Some(
                parts[3].parse::<usize>().map_err(|_| {
                    "RESULT_START total rows must be an unsigned integer".to_string()
                })?,
            )
        },
        rows,
        has_more: parse_bool_token(parts[5], "RESULT_START has_more")?,
    })
}

fn parse_result_page_response(payload: &str) -> Result<RemoteResultPage, String> {
    let parts = payload.splitn(6, '\t').collect::<Vec<_>>();
    if parts.len() != 6 || parts[0] != "OK" || parts[1] != "RESULT_PAGE" {
        return Err(format!(
            "remote query returned non-page response: {payload}"
        ));
    }
    let row_count = parts[3]
        .parse::<usize>()
        .map_err(|_| "RESULT_PAGE row count must be an unsigned integer".to_string())?;
    let rows = decode_query_rows(parts[5])?;
    if rows.len() != row_count {
        return Err(format!(
            "RESULT_PAGE row count mismatch: header {row_count}, decoded {}",
            rows.len()
        ));
    }
    Ok(RemoteResultPage {
        rows,
        has_more: parse_bool_token(parts[4], "RESULT_PAGE has_more")?,
    })
}

fn parse_bool_token(input: &str, name: &str) -> Result<bool, String> {
    match input {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => Err(format!("{name} must be true or false")),
    }
}

fn request_remote_query_shard(
    address: &str,
    shard_id: u64,
    query: &str,
    params: &neo4r_query::QueryParams,
) -> Result<Vec<QueryRow>, String> {
    let mut cursor = RemoteShardQueryCursor::open(address, shard_id, query, params)?;
    let mut rows = Vec::new();
    loop {
        let page = cursor.fetch(1024);
        rows.extend(page.rows);
        if !page.has_more {
            break;
        }
    }
    Ok(rows)
}

fn request_remote_command(address: &str, payload: &str) -> Result<String, String> {
    let mut stream = TcpStream::connect(address)
        .map_err(|err| format!("connect write peer {address}: {err}"))?;
    request_command_on_stream(&mut stream, 1, payload)
}

fn request_command_on_stream(
    stream: &mut TcpStream,
    request_id: u64,
    payload: &str,
) -> Result<String, String> {
    write_frame(
        stream,
        &NativeFrame::new(
            NativeMessageType::Command,
            request_id,
            payload.as_bytes().to_vec(),
        ),
    )
    .map_err(|err| format!("write remote command request: {err}"))?;
    let frame = read_frame(stream)
        .map_err(|err| format!("read remote command response: {err}"))?
        .ok_or_else(|| "remote write peer closed without command response".to_string())?;
    let response = frame.payload_text().map_err(|err| err.to_string())?;
    match frame.message_type {
        NativeMessageType::Response => Ok(response.to_string()),
        NativeMessageType::Error => Err(response
            .strip_prefix("ERR\t")
            .unwrap_or(response)
            .to_string()),
        other => Err(format!(
            "remote command returned unexpected frame {other:?}"
        )),
    }
}

fn request_remote_prepare_commit_batch(
    db: &Neo4rDatabaseHandle,
    tx_id: u64,
    address: &str,
    shard_id: u64,
    writes: &[(String, neo4r_query::QueryParams)],
) -> Result<(), String> {
    let mut stream = TcpStream::connect(address)
        .map_err(|err| format!("connect write peer {address}: {err}"))?;
    let prepared = request_command_on_stream(
        &mut stream,
        1,
        &format_tx_prepare_write_batch_shard_payload(shard_id, writes),
    )?;
    let prepared_id = parse_tx_prepared_response(&prepared)?;
    record_commit_decision(
        db,
        tx_id,
        vec![TransactionParticipantRecord {
            location: format!("remote:{address}"),
            shard_id,
            prepared_id,
        }],
    )?;
    if let Err(err) = request_command_on_stream(
        &mut stream,
        2,
        &format!("TX_COMMIT_PREPARED\t{prepared_id}"),
    ) {
        return Err(err);
    }
    let _ = clear_transaction_decision(db, tx_id);
    Ok(())
}

fn request_remote_commit_prepared(address: &str, prepared_id: u64) -> Result<(), String> {
    match request_remote_command(address, &format!("TX_COMMIT_PREPARED\t{prepared_id}")) {
        Ok(_) => Ok(()),
        Err(err) if err.contains("unknown prepared transaction") => Ok(()),
        Err(err) => Err(err),
    }
}

fn request_remote_abort_prepared(address: &str, prepared_id: u64) -> Result<(), String> {
    match request_remote_command(address, &format!("TX_ABORT_PREPARED\t{prepared_id}")) {
        Ok(_) => Ok(()),
        Err(err) if err.contains("unknown prepared transaction") => Ok(()),
        Err(err) => Err(err),
    }
}

fn recover_transaction_decisions(
    db: &Neo4rDatabaseHandle,
    prepared_transactions: &PreparedTransactionStore,
) -> Result<usize, String> {
    let data_dir = db.data_dir().map_err(|err| err.to_string())?;
    let store = TransactionDecisionStore::open(data_dir).map_err(|err| err.to_string())?;
    let decisions = store.load().map_err(|err| err.to_string())?;
    let mut completed_tx_ids = BTreeSet::new();
    for decision in &decisions {
        if let Err(err) = apply_transaction_decision(db, prepared_transactions, decision) {
            store
                .remove_tx_ids(&completed_tx_ids)
                .map_err(|err| err.to_string())?;
            return Err(err);
        }
        completed_tx_ids.insert(decision.tx_id);
    }
    store
        .remove_tx_ids(&completed_tx_ids)
        .map_err(|err| err.to_string())?;
    Ok(decisions.len())
}

fn list_transaction_decisions(
    db: &Neo4rDatabaseHandle,
) -> Result<Vec<TransactionDecisionRecord>, String> {
    let data_dir = db.data_dir().map_err(|err| err.to_string())?;
    TransactionDecisionStore::open(data_dir)
        .and_then(|store| store.load())
        .map_err(|err| err.to_string())
}

fn apply_transaction_decision(
    db: &Neo4rDatabaseHandle,
    prepared_transactions: &PreparedTransactionStore,
    decision: &TransactionDecisionRecord,
) -> Result<(), String> {
    match decision.decision {
        TransactionDecision::Commit => {
            for participant in &decision.participants {
                commit_decision_participant(db, prepared_transactions, participant)?;
            }
        }
        TransactionDecision::Abort => {
            for participant in &decision.participants {
                abort_decision_participant(prepared_transactions, participant)?;
            }
        }
    }
    Ok(())
}

fn commit_decision_participant(
    db: &Neo4rDatabaseHandle,
    prepared_transactions: &PreparedTransactionStore,
    participant: &TransactionParticipantRecord,
) -> Result<(), String> {
    if participant.location == "local" {
        let prepared = match prepared_transactions.take(participant.prepared_id) {
            Ok(prepared) => prepared,
            Err(err) if err.contains("unknown prepared transaction") => return Ok(()),
            Err(err) => return Err(err),
        };
        db.execute_staged_cypher_transaction_on_shard(prepared.shard_id, prepared.writes)
            .map(|_| ())
            .map_err(|err| err.to_string())
    } else if let Some(address) = participant.location.strip_prefix("remote:") {
        request_remote_commit_prepared(address, participant.prepared_id)
    } else {
        Err(format!(
            "unknown transaction participant location: {}",
            participant.location
        ))
    }
}

fn abort_decision_participant(
    prepared_transactions: &PreparedTransactionStore,
    participant: &TransactionParticipantRecord,
) -> Result<(), String> {
    if participant.location == "local" {
        let _ = prepared_transactions.take(participant.prepared_id);
        Ok(())
    } else if let Some(address) = participant.location.strip_prefix("remote:") {
        request_remote_abort_prepared(address, participant.prepared_id)
    } else {
        Err(format!(
            "unknown transaction participant location: {}",
            participant.location
        ))
    }
}

fn request_remote_prepare_commit_batches(
    db: &Neo4rDatabaseHandle,
    tx_id: u64,
    participants: Vec<(String, u64, Vec<(String, neo4r_query::QueryParams)>)>,
) -> Result<(), String> {
    let mut prepared = Vec::new();
    for (address, shard_id, writes) in participants {
        let mut stream = TcpStream::connect(&address)
            .map_err(|err| format!("connect write peer {address}: {err}"))?;
        let response = match request_command_on_stream(
            &mut stream,
            1,
            &format_tx_prepare_write_batch_shard_payload(shard_id, &writes),
        ) {
            Ok(response) => response,
            Err(err) => {
                record_abort_decision(db, tx_id, remote_decision_participant_records(&prepared))?;
                abort_prepared_participants(prepared);
                return Err(err);
            }
        };
        let prepared_id = match parse_tx_prepared_response(&response) {
            Ok(prepared_id) => prepared_id,
            Err(err) => {
                record_abort_decision(db, tx_id, remote_decision_participant_records(&prepared))?;
                abort_prepared_participants(prepared);
                return Err(err);
            }
        };
        prepared.push(RemotePreparedParticipant {
            stream,
            address,
            shard_id,
            prepared_id,
        });
    }

    record_commit_decision(
        db,
        tx_id,
        prepared
            .iter()
            .map(|participant| TransactionParticipantRecord {
                location: format!("remote:{}", participant.address),
                shard_id: participant.shard_id,
                prepared_id: participant.prepared_id,
            })
            .collect(),
    )?;

    while !prepared.is_empty() {
        let mut participant = prepared.remove(0);
        if let Err(err) = request_command_on_stream(
            &mut participant.stream,
            2,
            &format!("TX_COMMIT_PREPARED\t{}", participant.prepared_id),
        ) {
            return Err(format!(
                "commit prepared transaction {} on {} failed: {err}",
                participant.prepared_id, participant.address
            ));
        }
    }
    let _ = clear_transaction_decision(db, tx_id);
    Ok(())
}

fn abort_prepared_participants(participants: Vec<RemotePreparedParticipant>) {
    for mut participant in participants {
        let _ = request_command_on_stream(
            &mut participant.stream,
            3,
            &format!("TX_ABORT_PREPARED\t{}", participant.prepared_id),
        );
    }
}

struct RemotePreparedParticipant {
    stream: TcpStream,
    address: String,
    shard_id: u64,
    prepared_id: u64,
}

fn record_commit_decision(
    db: &Neo4rDatabaseHandle,
    tx_id: u64,
    participants: Vec<TransactionParticipantRecord>,
) -> Result<(), String> {
    record_transaction_decision(db, tx_id, TransactionDecision::Commit, participants)
}

fn record_abort_decision(
    db: &Neo4rDatabaseHandle,
    tx_id: u64,
    participants: Vec<TransactionParticipantRecord>,
) -> Result<(), String> {
    record_transaction_decision(db, tx_id, TransactionDecision::Abort, participants)
}

fn record_transaction_decision(
    db: &Neo4rDatabaseHandle,
    tx_id: u64,
    decision: TransactionDecision,
    participants: Vec<TransactionParticipantRecord>,
) -> Result<(), String> {
    if participants.is_empty() {
        return Ok(());
    }
    let data_dir = db.data_dir().map_err(|err| err.to_string())?;
    TransactionDecisionStore::open(data_dir)
        .and_then(|store| {
            store.append(&TransactionDecisionRecord {
                tx_id,
                decision,
                participants,
            })
        })
        .map_err(|err| err.to_string())
}

fn clear_transaction_decision(db: &Neo4rDatabaseHandle, tx_id: u64) -> Result<(), String> {
    let data_dir = db.data_dir().map_err(|err| err.to_string())?;
    TransactionDecisionStore::open(data_dir)
        .and_then(|store| store.remove_tx_ids(&BTreeSet::from([tx_id])).map(|_| ()))
        .map_err(|err| err.to_string())
}

fn decision_participant_records(
    prepared_locals: &[(u64, u64)],
    prepared_remotes: &[RemotePreparedParticipant],
) -> Vec<TransactionParticipantRecord> {
    prepared_locals
        .iter()
        .map(|(shard_id, prepared_id)| TransactionParticipantRecord {
            location: "local".to_string(),
            shard_id: *shard_id,
            prepared_id: *prepared_id,
        })
        .chain(remote_decision_participant_records(prepared_remotes))
        .collect()
}

fn remote_decision_participant_records(
    prepared_remotes: &[RemotePreparedParticipant],
) -> Vec<TransactionParticipantRecord> {
    prepared_remotes
        .iter()
        .map(|participant| TransactionParticipantRecord {
            location: format!("remote:{}", participant.address),
            shard_id: participant.shard_id,
            prepared_id: participant.prepared_id,
        })
        .collect()
}

fn parse_ok_rows_response(payload: &str) -> Result<Vec<QueryRow>, String> {
    if payload.starts_with("OK\tROWS\t") {
        let parts = payload.splitn(4, '\t').collect::<Vec<_>>();
        if parts.len() != 4 {
            return Err(format!(
                "remote write returned malformed rows response: {payload}"
            ));
        }
        return decode_query_rows(parts[3]);
    }
    let start = parse_result_start_response(payload)?;
    if start.has_more {
        return Err("remote write returned more rows than a single response page".to_string());
    }
    Ok(start.rows)
}

fn parse_tx_prepared_response(payload: &str) -> Result<u64, String> {
    let parts = payload.split('\t').collect::<Vec<_>>();
    if parts.len() >= 3 && parts[0] == "OK" && parts[1] == "TX_PREPARED" {
        parse_cursor_id(parts[2])
    } else {
        Err(format!(
            "remote prepare returned malformed response: {payload}"
        ))
    }
}

fn parse_ok_index_catalog_response(payload: &str) -> Result<neo4r_db::IndexCatalog, String> {
    let parts = payload.splitn(3, '\t').collect::<Vec<_>>();
    if parts.len() != 3 || parts[0] != "OK" || parts[1] != "INDEX_CATALOG" {
        return Err(format!(
            "remote catalog returned malformed response: {payload}"
        ));
    }
    decode_index_catalog(parts[2])
}

fn select_create_node_write_shard<'a>(
    status: &'a neo4r_db::ClusterStatus,
    query: &str,
    params: &neo4r_query::QueryParams,
) -> Result<&'a neo4r_db::ShardStatus, String> {
    if status.shards.is_empty() {
        return Err("cluster status has no shards".to_string());
    }
    let hash = stable_create_node_hash(query, params);
    let index = (hash % status.shards.len() as u64) as usize;
    status
        .shards
        .get(index)
        .ok_or_else(|| "cluster status has no shards".to_string())
}

fn select_merge_node_write_shard<'a>(
    status: &'a neo4r_db::ClusterStatus,
    query: &str,
    params: &neo4r_query::QueryParams,
) -> Result<&'a neo4r_db::ShardStatus, String> {
    if status.shards.is_empty() {
        return Err("cluster status has no shards".to_string());
    }
    let hash = stable_merge_node_hash(query, params);
    let index = (hash % status.shards.len() as u64) as usize;
    status
        .shards
        .get(index)
        .ok_or_else(|| "cluster status has no shards".to_string())
}

fn stable_create_node_hash(query: &str, params: &neo4r_query::QueryParams) -> u64 {
    let routing_key = match create_node_routing_key(query, params) {
        Ok(Some(key)) => key,
        Ok(None) | Err(_) => {
            return stable_create_node_fallback_hash(query, params);
        }
    };
    stable_create_node_routing_key_hash(&routing_key)
}

fn stable_merge_node_hash(query: &str, params: &neo4r_query::QueryParams) -> u64 {
    let routing_key = match merge_node_routing_key(query, params) {
        Ok(Some(key)) => key,
        Ok(None) | Err(_) => {
            return stable_create_node_fallback_hash(query, params);
        }
    };
    stable_create_node_routing_key_hash(&routing_key)
}

fn stable_create_node_routing_key_hash(routing_key: &CreateNodeRoutingKey) -> u64 {
    const FNV_OFFSET: u64 = 0xcbf29ce484222325;
    const FNV_PRIME: u64 = 0x100000001b3;

    fn update(mut hash: u64, bytes: &[u8]) -> u64 {
        for byte in bytes {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(FNV_PRIME);
        }
        hash
    }

    let mut hash = FNV_OFFSET;
    let mut labels = routing_key.labels.iter().collect::<Vec<_>>();
    labels.sort();
    for label in labels {
        hash = update(hash, b"\0");
        hash = update(hash, b"label:");
        hash = update(hash, label.as_bytes());
    }
    let mut property_keys = routing_key.properties.keys().collect::<Vec<_>>();
    property_keys.sort();
    for key in property_keys {
        hash = update(hash, b"\0");
        hash = update(hash, b"prop:");
        hash = update(hash, key.as_bytes());
        hash = update(hash, b"=");
        hash = update(
            hash,
            format_value_for_request(&routing_key.properties[key]).as_bytes(),
        );
    }
    hash
}

fn stable_create_node_fallback_hash(query: &str, params: &neo4r_query::QueryParams) -> u64 {
    const FNV_OFFSET: u64 = 0xcbf29ce484222325;
    const FNV_PRIME: u64 = 0x100000001b3;

    fn update(mut hash: u64, bytes: &[u8]) -> u64 {
        for byte in bytes {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(FNV_PRIME);
        }
        hash
    }

    let mut hash = update(FNV_OFFSET, query.trim().as_bytes());
    let mut keys = params.keys().collect::<Vec<_>>();
    keys.sort();
    for key in keys {
        hash = update(hash, b"\0");
        hash = update(hash, key.as_bytes());
        hash = update(hash, b"=");
        hash = update(hash, format_value_for_request(&params[key]).as_bytes());
    }
    hash
}

fn write_request_shard(
    db: &Neo4rDatabaseHandle,
    request: &BackendRequest,
    shard_count: u64,
) -> Result<Option<u64>, String> {
    if shard_count == 0 {
        return Ok(None);
    }
    let shard_id = match request {
        BackendRequest::CreateNodeOnShard { shard_id, .. } => Some(*shard_id),
        BackendRequest::CreateRelationship { from, .. } => Some(from % shard_count),
        BackendRequest::SetNodeProperty { id, .. }
        | BackendRequest::RemoveNodeProperty { id, .. }
        | BackendRequest::AddNodeLabel { id, .. }
        | BackendRequest::RemoveNodeLabel { id, .. }
        | BackendRequest::DeleteNode(id) => Some(id % shard_count),
        BackendRequest::SetRelationshipProperty { id, .. }
        | BackendRequest::RemoveRelationshipProperty { id, .. }
        | BackendRequest::DeleteRelationship(id) => Some(
            db.relationship_owner_shard(*id)
                .map_err(|err| err.to_string())?,
        ),
        BackendRequest::CreateIndex { .. }
        | BackendRequest::CreateUniqueConstraint { .. }
        | BackendRequest::CreateVectorIndex { .. }
        | BackendRequest::RebuildVectorIndex { .. }
        | BackendRequest::RebuildVectorIndexes
        | BackendRequest::DropIndex { .. }
        | BackendRequest::DropConstraint { .. } => Some(0),
        _ => None,
    };
    Ok(shard_id)
}

fn format_command_request_payload(request: &BackendRequest) -> Result<String, String> {
    match request {
        BackendRequest::CreateNodeOnShard {
            shard_id,
            labels,
            properties,
        } => Ok(format!(
            "CREATE_NODE_SHARD\t{shard_id}\t{}{}",
            labels.join(","),
            format_properties_suffix(properties)
        )),
        BackendRequest::CreateRelationship {
            from,
            to,
            rel_type,
            properties,
        } => Ok(format!(
            "CREATE_RELATIONSHIP\t{from}\t{to}\t{rel_type}{}",
            format_properties_suffix(properties)
        )),
        BackendRequest::SetNodeProperty { id, key, value } => Ok(format!(
            "SET_NODE_PROPERTY\t{id}\t{key}\t{}",
            format_value_for_request(value)
        )),
        BackendRequest::RemoveNodeProperty { id, key } => {
            Ok(format!("REMOVE_NODE_PROPERTY\t{id}\t{key}"))
        }
        BackendRequest::AddNodeLabel { id, label } => Ok(format!("ADD_NODE_LABEL\t{id}\t{label}")),
        BackendRequest::RemoveNodeLabel { id, label } => {
            Ok(format!("REMOVE_NODE_LABEL\t{id}\t{label}"))
        }
        BackendRequest::SetRelationshipProperty { id, key, value } => Ok(format!(
            "SET_RELATIONSHIP_PROPERTY\t{id}\t{key}\t{}",
            format_value_for_request(value)
        )),
        BackendRequest::RemoveRelationshipProperty { id, key } => {
            Ok(format!("REMOVE_RELATIONSHIP_PROPERTY\t{id}\t{key}"))
        }
        BackendRequest::DeleteNode(id) => Ok(format!("DELETE_NODE\t{id}")),
        BackendRequest::DeleteRelationship(id) => Ok(format!("DELETE_RELATIONSHIP\t{id}")),
        BackendRequest::CreateIndex {
            name,
            label,
            property,
            if_not_exists,
        } => Ok(format!(
            "CREATE_INDEX\t{name}\t{label}\t{property}{}",
            format_if_not_exists_suffix(*if_not_exists)
        )),
        BackendRequest::CreateUniqueConstraint {
            name,
            label,
            property,
            if_not_exists,
        } => Ok(format!(
            "CREATE_CONSTRAINT\t{name}\t{label}\t{property}{}",
            format_if_not_exists_suffix(*if_not_exists)
        )),
        BackendRequest::CreateVectorIndex {
            name,
            label,
            property,
            dimensions,
            metric,
            if_not_exists,
        } => Ok(format!(
            "CREATE_VECTOR_INDEX\t{name}\t{label}\t{property}\t{dimensions}\t{metric}{}",
            format_if_not_exists_suffix(*if_not_exists)
        )),
        BackendRequest::RebuildVectorIndex { name } => Ok(format!("REBUILD_VECTOR_INDEX\t{name}")),
        BackendRequest::RebuildVectorIndexes => Ok("REBUILD_VECTOR_INDEXES".to_string()),
        BackendRequest::DropIndex { name, if_exists } => Ok(format!(
            "DROP_INDEX\t{name}{}",
            format_if_exists_suffix(*if_exists)
        )),
        BackendRequest::DropConstraint { name, if_exists } => Ok(format!(
            "DROP_CONSTRAINT\t{name}{}",
            format_if_exists_suffix(*if_exists)
        )),
        _ => Err("request is not a forwardable write command".to_string()),
    }
}

fn format_if_not_exists_suffix(if_not_exists: bool) -> &'static str {
    if if_not_exists {
        "\tIF_NOT_EXISTS"
    } else {
        ""
    }
}

fn format_if_exists_suffix(if_exists: bool) -> &'static str {
    if if_exists {
        "\tIF_EXISTS"
    } else {
        ""
    }
}

fn format_properties_suffix(properties: &neo4r_core::Properties) -> String {
    let mut keys = properties.keys().collect::<Vec<_>>();
    keys.sort();
    let mut suffix = String::new();
    for key in keys {
        if let Some(value) = properties.get(key) {
            suffix.push('\t');
            suffix.push_str(key);
            suffix.push('=');
            suffix.push_str(&format_value_for_request(value));
        }
    }
    suffix
}

fn format_query_write_shard_payload(
    shard_id: u64,
    query: &str,
    params: &neo4r_query::QueryParams,
) -> Result<String, String> {
    format_query_payload_with_command("QUERY_WRITE_SHARD", shard_id, query, params)
}

fn format_tx_prepare_write_batch_shard_payload(
    shard_id: u64,
    writes: &[(String, neo4r_query::QueryParams)],
) -> String {
    format!(
        "TX_PREPARE_WRITE_BATCH_SHARD\t{shard_id}\t{}",
        encode_query_batch_payload(writes)
    )
}

fn format_query_payload_with_command(
    command: &str,
    shard_id: u64,
    query: &str,
    params: &neo4r_query::QueryParams,
) -> Result<String, String> {
    let mut payload = format!("{command}\t{shard_id}\t{query}");
    let mut keys = params.keys().collect::<Vec<_>>();
    keys.sort();
    for key in keys {
        payload.push('\t');
        payload.push_str(key);
        payload.push('=');
        payload.push_str(&format_value_for_request(
            params
                .get(key)
                .ok_or_else(|| format!("missing query parameter: {key}"))?,
        ));
    }
    Ok(payload)
}

fn format_query_shard_payload(
    shard_id: u64,
    query: &str,
    params: &neo4r_query::QueryParams,
) -> Result<String, String> {
    format_query_payload_with_command("QUERY_SHARD", shard_id, query, params)
}

fn format_query_staged_shard_payload(
    shard_id: u64,
    query: &str,
    params: &neo4r_query::QueryParams,
    staged_writes: &[(String, neo4r_query::QueryParams)],
) -> String {
    let mut batch = Vec::with_capacity(staged_writes.len() + 1);
    batch.push((query.to_string(), params.clone()));
    batch.extend(staged_writes.iter().cloned());
    format!(
        "QUERY_STAGED_SHARD\t{shard_id}\t{}",
        encode_query_batch_payload(&batch)
    )
}

fn format_value_for_request(value: &neo4r_core::Value) -> String {
    match value {
        neo4r_core::Value::Null => "n:".to_string(),
        neo4r_core::Value::Bool(value) => format!("b:{value}"),
        neo4r_core::Value::Int(value) => format!("i:{value}"),
        neo4r_core::Value::Float(value) => format!("f:{value}"),
        neo4r_core::Value::String(value) => format!("s:{value}"),
        neo4r_core::Value::Vector(values) => {
            let values = values
                .iter()
                .map(|value| value.to_string())
                .collect::<Vec<_>>()
                .join(",");
            format!("v:{values}")
        }
        neo4r_core::Value::Map(values) => {
            let encoded = encode_map_for_request(values);
            format!("m:{}", hex_encode_for_request(encoded.as_bytes()))
        }
    }
}

fn encode_map_for_request(values: &neo4r_core::Properties) -> String {
    let mut entries = values.iter().collect::<Vec<_>>();
    entries.sort_by(|(left, _), (right, _)| left.cmp(right));
    entries
        .into_iter()
        .map(|(key, value)| {
            format!(
                "{}~{}",
                hex_encode_for_request(key.as_bytes()),
                encode_value_for_map_request(value)
            )
        })
        .collect::<Vec<_>>()
        .join(",")
}

fn encode_value_for_map_request(value: &neo4r_core::Value) -> String {
    match value {
        neo4r_core::Value::Null => "n".to_string(),
        neo4r_core::Value::Bool(value) => format!("b:{}", u8::from(*value)),
        neo4r_core::Value::Int(value) => format!("i:{value}"),
        neo4r_core::Value::Float(value) => format!("f:{}", value.to_bits()),
        neo4r_core::Value::String(value) => {
            format!("s:{}", hex_encode_for_request(value.as_bytes()))
        }
        neo4r_core::Value::Vector(values) => format!(
            "v:{}",
            values
                .iter()
                .map(|value| value.to_bits().to_string())
                .collect::<Vec<_>>()
                .join(",")
        ),
        neo4r_core::Value::Map(values) => {
            let encoded = encode_map_for_request(values);
            format!("m:{}", hex_encode_for_request(encoded.as_bytes()))
        }
    }
}

fn hex_encode_for_request(input: impl AsRef<[u8]>) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let input = input.as_ref();
    let mut output = String::with_capacity(input.len() * 2);
    for byte in input {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

fn is_write_cypher(query: &str) -> bool {
    let upper = query.trim().to_ascii_uppercase();
    upper.starts_with("CREATE INDEX ")
        || upper.starts_with("CREATE VECTOR INDEX ")
        || upper.starts_with("CREATE CONSTRAINT ")
        || upper.starts_with("DROP INDEX ")
        || upper.starts_with("DROP CONSTRAINT ")
        || upper.starts_with("CREATE ")
        || upper.starts_with("MERGE ")
        || (upper.starts_with("MATCH ")
            && (upper.contains(" CREATE ")
                || upper.contains(" MERGE ")
                || upper.contains(" SET ")
                || upper.contains(" REMOVE ")
                || upper.contains(" DELETE ")))
}

fn is_schema_cypher(query: &str) -> bool {
    let upper = query.trim().to_ascii_uppercase();
    upper.starts_with("CREATE INDEX ")
        || upper.starts_with("CREATE VECTOR INDEX ")
        || upper.starts_with("CREATE CONSTRAINT ")
        || upper.starts_with("DROP INDEX ")
        || upper.starts_with("DROP CONSTRAINT ")
}

fn is_create_node_cypher(query: &str) -> bool {
    let upper = query.trim().to_ascii_uppercase();
    upper.starts_with("CREATE ")
        && !upper.starts_with("CREATE INDEX ")
        && !upper.starts_with("CREATE VECTOR INDEX ")
        && !upper.starts_with("CREATE CONSTRAINT ")
}

fn is_merge_node_cypher(query: &str) -> bool {
    let upper = query.trim().to_ascii_uppercase();
    upper.starts_with("MERGE ")
}

fn is_delete_cypher(query: &str) -> bool {
    let upper = query.trim().to_ascii_uppercase();
    upper.starts_with("MATCH ") && upper.contains(" DELETE ")
}

fn is_batchable_transaction_set_cypher(query: &str) -> bool {
    let input = query.trim();
    let upper = input.to_ascii_uppercase();
    if !upper.starts_with("MATCH ")
        || (!upper.contains(" SET ") && !upper.contains(" REMOVE "))
        || upper.contains(" CREATE ")
        || upper.contains(" DELETE ")
    {
        return false;
    }
    true
}

fn is_batchable_multi_target_transaction_cypher(query: &str) -> bool {
    is_batchable_transaction_set_cypher(query) || is_delete_cypher(query)
}

fn staged_writes_are_prepare_batchable(staged_writes: &[StagedWrite]) -> bool {
    staged_writes
        .iter()
        .all(|staged| is_batchable_cypher_mutation(&staged.query))
}

fn is_batchable_cypher_mutation(query: &str) -> bool {
    let upper = query.trim().to_ascii_uppercase();
    if upper.starts_with("CREATE INDEX ")
        || upper.starts_with("CREATE VECTOR INDEX ")
        || upper.starts_with("CREATE CONSTRAINT ")
        || upper.starts_with("DROP INDEX ")
        || upper.starts_with("DROP CONSTRAINT ")
    {
        return false;
    }
    upper.starts_with("CREATE ")
        || is_merge_node_cypher(query)
        || (upper.starts_with("MATCH ")
            && (upper.contains(" CREATE ")
                || upper.contains(" MERGE ")
                || upper.contains(" SET ")
                || upper.contains(" REMOVE ")
                || upper.contains(" DELETE ")))
}

fn is_staged_transaction_overlay_cypher(query: &str) -> bool {
    let upper = query.trim().to_ascii_uppercase();
    if upper.starts_with("CREATE INDEX ")
        || upper.starts_with("CREATE VECTOR INDEX ")
        || upper.starts_with("CREATE CONSTRAINT ")
        || upper.starts_with("DROP INDEX ")
        || upper.starts_with("DROP CONSTRAINT ")
    {
        return false;
    }
    upper.starts_with("CREATE ")
        || upper.starts_with("MERGE ")
        || (upper.starts_with("MATCH ")
            && (upper.contains(" CREATE ")
                || upper.contains(" MERGE ")
                || upper.contains(" SET ")
                || upper.contains(" REMOVE ")
                || upper.contains(" DELETE ")))
}

enum TransactionCommand {
    Begin {
        mode: TransactionMode,
        isolation: ReadIsolation,
    },
    Query {
        tx_id: u64,
        query: String,
        params: neo4r_query::QueryParams,
    },
    ExecutePrepared {
        tx_id: u64,
        prepared_id: u64,
        params: neo4r_query::QueryParams,
    },
    PreparedQueryPlan {
        tx_id: u64,
        prepared_id: u64,
        params: neo4r_query::QueryParams,
    },
    PreparedQueryRoute {
        tx_id: u64,
        prepared_id: u64,
        params: neo4r_query::QueryParams,
    },
    DistributedQuery {
        tx_id: u64,
        query: String,
        params: neo4r_query::QueryParams,
    },
    QueryPlan {
        tx_id: u64,
        query: String,
        params: neo4r_query::QueryParams,
    },
    Commit {
        tx_id: u64,
    },
    Rollback {
        tx_id: u64,
    },
    Kill {
        tx_id: u64,
    },
    Status {
        tx_id: u64,
    },
    PrepareWriteBatchShard {
        shard_id: u64,
        writes: Vec<(String, neo4r_query::QueryParams)>,
    },
    PreparedStatus {
        prepared_id: u64,
    },
    ListPrepared,
    CommitPrepared {
        prepared_id: u64,
    },
    AbortPrepared {
        prepared_id: u64,
    },
    List,
    ListAll,
}

enum PreparedQueryCommand {
    Prepare {
        query: String,
    },
    Execute {
        prepared_id: u64,
        params: neo4r_query::QueryParams,
    },
    QueryPlan {
        prepared_id: u64,
        params: neo4r_query::QueryParams,
    },
    Route {
        prepared_id: u64,
        params: neo4r_query::QueryParams,
    },
    Describe {
        prepared_id: u64,
    },
    Close {
        prepared_id: u64,
    },
    List,
}

fn parse_prepared_query_command(payload: &str) -> Result<Option<PreparedQueryCommand>, String> {
    let Some((command, rest)) = payload.split_once('\t') else {
        return match payload {
            "LIST_PREPARED" => Ok(Some(PreparedQueryCommand::List)),
            _ => Ok(None),
        };
    };
    match command {
        "PREPARE_QUERY" => {
            if rest.trim().is_empty() {
                Err("PREPARE_QUERY requires a cypher string".to_string())
            } else {
                Ok(Some(PreparedQueryCommand::Prepare {
                    query: rest.to_string(),
                }))
            }
        }
        "EXECUTE_PREPARED" => {
            let (prepared_id, params) = parse_prepared_query_execute_payload(rest)?;
            Ok(Some(PreparedQueryCommand::Execute {
                prepared_id,
                params,
            }))
        }
        "PREPARED_QUERY_PLAN" => {
            let (prepared_id, params) = parse_prepared_query_execute_payload(rest)?;
            Ok(Some(PreparedQueryCommand::QueryPlan {
                prepared_id,
                params,
            }))
        }
        "PREPARED_QUERY_ROUTE" => {
            let (prepared_id, params) = parse_prepared_query_execute_payload(rest)?;
            Ok(Some(PreparedQueryCommand::Route {
                prepared_id,
                params,
            }))
        }
        "DESCRIBE_PREPARED" => Ok(Some(PreparedQueryCommand::Describe {
            prepared_id: parse_cursor_id(rest)?,
        })),
        "CLOSE_PREPARED" => Ok(Some(PreparedQueryCommand::Close {
            prepared_id: parse_cursor_id(rest)?,
        })),
        "LIST_PREPARED" => {
            if rest.trim().is_empty() {
                Ok(Some(PreparedQueryCommand::List))
            } else {
                Err("LIST_PREPARED does not take arguments".to_string())
            }
        }
        _ => Ok(None),
    }
}

fn parse_prepared_query_execute_payload(
    payload: &str,
) -> Result<(u64, neo4r_query::QueryParams), String> {
    let Some((prepared_id, params_payload)) = payload.split_once('\t') else {
        return Ok((parse_cursor_id(payload)?, neo4r_query::QueryParams::new()));
    };
    let prepared_id = parse_cursor_id(prepared_id)?;
    let (_, params) = parse_query_payload(&format!("_\t{params_payload}"))?;
    Ok((prepared_id, params))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TransactionMode {
    ReadOnly,
    ReadWrite,
}

fn parse_transaction_command(payload: &str) -> Result<Option<TransactionCommand>, String> {
    let Some((command, rest)) = payload.split_once('\t') else {
        return match payload {
            "BEGIN_TX" => Ok(Some(TransactionCommand::Begin {
                mode: TransactionMode::ReadOnly,
                isolation: ReadIsolation::Snapshot,
            })),
            "LIST_TX" => Ok(Some(TransactionCommand::List)),
            "LIST_ALL_TX" => Ok(Some(TransactionCommand::ListAll)),
            "LIST_PREPARED_TX" => Ok(Some(TransactionCommand::ListPrepared)),
            _ => Ok(None),
        };
    };
    match command {
        "BEGIN_TX" => {
            let (mode, isolation) = parse_tx_begin_options(rest)?;
            Ok(Some(TransactionCommand::Begin { mode, isolation }))
        }
        "TX_QUERY" => {
            let (tx_id, query_payload) = rest
                .split_once('\t')
                .ok_or_else(|| "TX_QUERY requires transaction id and query".to_string())?;
            let tx_id = parse_cursor_id(tx_id)?;
            let (query, params) = parse_query_payload(query_payload)?;
            Ok(Some(TransactionCommand::Query {
                tx_id,
                query,
                params,
            }))
        }
        "TX_EXECUTE_PREPARED" => {
            let (tx_id, execute_payload) = rest.split_once('\t').ok_or_else(|| {
                "TX_EXECUTE_PREPARED requires transaction id and prepared query id".to_string()
            })?;
            let tx_id = parse_cursor_id(tx_id)?;
            let (prepared_id, params) = parse_prepared_query_execute_payload(execute_payload)?;
            Ok(Some(TransactionCommand::ExecutePrepared {
                tx_id,
                prepared_id,
                params,
            }))
        }
        "TX_PREPARED_QUERY_PLAN" => {
            let (tx_id, execute_payload) = rest.split_once('\t').ok_or_else(|| {
                "TX_PREPARED_QUERY_PLAN requires transaction id and prepared query id".to_string()
            })?;
            let tx_id = parse_cursor_id(tx_id)?;
            let (prepared_id, params) = parse_prepared_query_execute_payload(execute_payload)?;
            Ok(Some(TransactionCommand::PreparedQueryPlan {
                tx_id,
                prepared_id,
                params,
            }))
        }
        "TX_PREPARED_QUERY_ROUTE" => {
            let (tx_id, execute_payload) = rest.split_once('\t').ok_or_else(|| {
                "TX_PREPARED_QUERY_ROUTE requires transaction id and prepared query id".to_string()
            })?;
            let tx_id = parse_cursor_id(tx_id)?;
            let (prepared_id, params) = parse_prepared_query_execute_payload(execute_payload)?;
            Ok(Some(TransactionCommand::PreparedQueryRoute {
                tx_id,
                prepared_id,
                params,
            }))
        }
        "TX_QUERY_DISTRIBUTED" => {
            let (tx_id, query_payload) = rest.split_once('\t').ok_or_else(|| {
                "TX_QUERY_DISTRIBUTED requires transaction id and query".to_string()
            })?;
            let tx_id = parse_cursor_id(tx_id)?;
            let (query, params) = parse_query_payload(query_payload)?;
            Ok(Some(TransactionCommand::DistributedQuery {
                tx_id,
                query,
                params,
            }))
        }
        "TX_QUERY_PLAN" => {
            let (tx_id, query_payload) = rest
                .split_once('\t')
                .ok_or_else(|| "TX_QUERY_PLAN requires transaction id and query".to_string())?;
            let tx_id = parse_cursor_id(tx_id)?;
            let (query, params) = parse_query_payload(query_payload)?;
            Ok(Some(TransactionCommand::QueryPlan {
                tx_id,
                query,
                params,
            }))
        }
        "COMMIT_TX" => Ok(Some(TransactionCommand::Commit {
            tx_id: parse_cursor_id(rest)?,
        })),
        "ROLLBACK_TX" => Ok(Some(TransactionCommand::Rollback {
            tx_id: parse_cursor_id(rest)?,
        })),
        "KILL_TX" => Ok(Some(TransactionCommand::Kill {
            tx_id: parse_cursor_id(rest)?,
        })),
        "TX_STATUS" => Ok(Some(TransactionCommand::Status {
            tx_id: parse_cursor_id(rest)?,
        })),
        "TX_PREPARE_WRITE_BATCH_SHARD" => {
            let (shard_id, payload) = rest.split_once('\t').ok_or_else(|| {
                "TX_PREPARE_WRITE_BATCH_SHARD requires shard id and encoded write batch".to_string()
            })?;
            Ok(Some(TransactionCommand::PrepareWriteBatchShard {
                shard_id: parse_cursor_id(shard_id)?,
                writes: decode_query_batch_payload(payload)?,
            }))
        }
        "TX_COMMIT_PREPARED" => Ok(Some(TransactionCommand::CommitPrepared {
            prepared_id: parse_cursor_id(rest)?,
        })),
        "TX_ABORT_PREPARED" => Ok(Some(TransactionCommand::AbortPrepared {
            prepared_id: parse_cursor_id(rest)?,
        })),
        "TX_PREPARED_STATUS" => Ok(Some(TransactionCommand::PreparedStatus {
            prepared_id: parse_cursor_id(rest)?,
        })),
        "LIST_PREPARED_TX" => {
            if rest.trim().is_empty() {
                Ok(Some(TransactionCommand::ListPrepared))
            } else {
                Err("LIST_PREPARED_TX does not take arguments".to_string())
            }
        }
        "LIST_TX" => {
            if rest.trim().is_empty() {
                Ok(Some(TransactionCommand::List))
            } else {
                Err("LIST_TX does not take arguments".to_string())
            }
        }
        "LIST_ALL_TX" => {
            if rest.trim().is_empty() {
                Ok(Some(TransactionCommand::ListAll))
            } else {
                Err("LIST_ALL_TX does not take arguments".to_string())
            }
        }
        _ => Ok(None),
    }
}

fn parse_tx_begin_options(value: &str) -> Result<(TransactionMode, ReadIsolation), String> {
    let mut mode = TransactionMode::ReadOnly;
    let mut isolation = ReadIsolation::Snapshot;
    for option in value.split_whitespace() {
        match option {
            "READ_ONLY" => mode = TransactionMode::ReadOnly,
            "READ_WRITE" => mode = TransactionMode::ReadWrite,
            "SNAPSHOT" => isolation = ReadIsolation::Snapshot,
            "READ_COMMITTED" => isolation = ReadIsolation::ReadCommitted,
            value => return Err(format!("unsupported transaction option: {value}")),
        }
    }
    Ok((mode, isolation))
}

#[derive(Clone, Default)]
struct TransactionStore {
    next_id: Arc<AtomicU64>,
    next_session_id: Arc<AtomicU64>,
    transactions: Arc<Mutex<HashMap<u64, TransactionState>>>,
}

impl TransactionStore {
    fn next_session_id(&self) -> u64 {
        self.next_session_id.fetch_add(1, Ordering::Relaxed) + 1
    }

    fn insert(&self, session_id: u64, tx: NativeTransaction) -> u64 {
        let tx_id = self.next_id.fetch_add(1, Ordering::Relaxed) + 1;
        self.transactions.lock().unwrap().insert(
            tx_id,
            TransactionState {
                session_id,
                transaction: tx,
            },
        );
        tx_id
    }

    fn query_cursor(
        &self,
        db: &Neo4rDatabaseHandle,
        session_id: u64,
        tx_id: u64,
        query: &str,
        params: &neo4r_query::QueryParams,
    ) -> Result<Box<dyn QueryCursor>, String> {
        let transactions = self
            .transactions
            .lock()
            .map_err(|_| "transaction store lock poisoned".to_string())?;
        let tx = transactions
            .get(&tx_id)
            .ok_or_else(|| format!("unknown transaction: {tx_id}"))?;
        tx.ensure_session(session_id, tx_id)?;
        match &tx.transaction {
            NativeTransaction::ReadOnly(tx) => {
                if tx.options().isolation == ReadIsolation::ReadCommitted {
                    db.query_cursor_with_params_and_options(
                        query,
                        params.clone(),
                        QueryOptions::default().with_isolation(ReadIsolation::ReadCommitted),
                    )
                    .map_err(|err| err.to_string())
                } else {
                    tx.query_cursor_with_params(query, params)
                        .map_err(|err| err.to_string())
                }
            }
            NativeTransaction::ReadWrite {
                isolation,
                staged_writes,
            } => {
                let staged_writes = staged_writes
                    .iter()
                    .map(|staged| (staged.query.clone(), staged.params.clone()))
                    .collect::<Vec<_>>();
                db.query_cursor_with_staged_writes(
                    query,
                    params.clone(),
                    QueryOptions::default().with_isolation(*isolation),
                    &staged_writes,
                )
                .map_err(|err| err.to_string())
            }
        }
    }

    fn distributed_query_cursor(
        &self,
        db: &Neo4rDatabaseHandle,
        query_peers: &QueryPeerStore,
        read_preference: QueryReadPreference,
        session_id: u64,
        tx_id: u64,
        query: &str,
        params: &neo4r_query::QueryParams,
    ) -> Result<Box<dyn QueryCursor>, String> {
        let transactions = self
            .transactions
            .lock()
            .map_err(|_| "transaction store lock poisoned".to_string())?;
        let tx = transactions
            .get(&tx_id)
            .ok_or_else(|| format!("unknown transaction: {tx_id}"))?;
        tx.ensure_session(session_id, tx_id)?;
        match &tx.transaction {
            NativeTransaction::ReadOnly(read_tx) => {
                if read_tx.options().isolation == ReadIsolation::ReadCommitted {
                    build_distributed_query_cursor_with_options(
                        db,
                        query_peers,
                        read_preference,
                        query,
                        params,
                        QueryOptions::default().with_isolation(ReadIsolation::ReadCommitted),
                    )
                } else {
                    build_distributed_read_tx_cursor(
                        db,
                        query_peers,
                        read_preference,
                        read_tx,
                        query,
                        params,
                    )
                }
            }
            NativeTransaction::ReadWrite {
                isolation,
                staged_writes,
            } => {
                if staged_writes.is_empty() {
                    return build_distributed_query_cursor_with_options(
                        db,
                        query_peers,
                        read_preference,
                        query,
                        params,
                        QueryOptions::default().with_isolation(*isolation),
                    );
                }
                let staged_writes = staged_writes
                    .iter()
                    .map(|staged| (staged.query.clone(), staged.params.clone()))
                    .collect::<Vec<_>>();
                build_distributed_query_cursor_with_local_staged_writes(
                    db,
                    query_peers,
                    read_preference,
                    query,
                    params,
                    QueryOptions::default().with_isolation(*isolation),
                    &staged_writes,
                )
            }
        }
    }

    fn plan_context(&self, session_id: u64, tx_id: u64) -> Result<TransactionPlanContext, String> {
        let transactions = self
            .transactions
            .lock()
            .map_err(|_| "transaction store lock poisoned".to_string())?;
        let tx = transactions
            .get(&tx_id)
            .ok_or_else(|| format!("unknown transaction: {tx_id}"))?;
        tx.ensure_session(session_id, tx_id)?;
        Ok(TransactionPlanContext {
            mode: tx.transaction.mode(),
            isolation: tx.transaction.isolation(),
            staged_writes: tx.transaction.staged_write_count(),
        })
    }

    fn stage_write(
        &self,
        session_id: u64,
        tx_id: u64,
        query: String,
        params: neo4r_query::QueryParams,
    ) -> Result<usize, String> {
        let mut transactions = self
            .transactions
            .lock()
            .map_err(|_| "transaction store lock poisoned".to_string())?;
        let tx = transactions
            .get_mut(&tx_id)
            .ok_or_else(|| format!("unknown transaction: {tx_id}"))?;
        tx.ensure_session(session_id, tx_id)?;
        match &mut tx.transaction {
            NativeTransaction::ReadOnly(_) => Err(format!(
                "transaction {tx_id} is read-only; begin with READ_WRITE for write queries"
            )),
            NativeTransaction::ReadWrite { staged_writes, .. } => {
                if is_schema_cypher(&query) {
                    return Err(
                        "schema DDL is not supported inside native read-write transactions"
                            .to_string(),
                    );
                }
                staged_writes.push(StagedWrite { query, params });
                Ok(staged_writes.len())
            }
        }
    }

    fn close(&self, session_id: u64, tx_id: u64) -> Result<NativeTransaction, String> {
        let mut transactions = self
            .transactions
            .lock()
            .map_err(|_| "transaction store lock poisoned".to_string())?;
        let tx = transactions
            .get(&tx_id)
            .ok_or_else(|| format!("unknown transaction: {tx_id}"))?;
        tx.ensure_session(session_id, tx_id)?;
        Ok(transactions.remove(&tx_id).unwrap().transaction)
    }

    fn close_any(&self, tx_id: u64) -> Result<TransactionInfo, String> {
        let mut transactions = self
            .transactions
            .lock()
            .map_err(|_| "transaction store lock poisoned".to_string())?;
        let tx = transactions
            .remove(&tx_id)
            .ok_or_else(|| format!("unknown transaction: {tx_id}"))?;
        Ok(TransactionInfo {
            session_id: tx.session_id,
            tx_id,
            mode: tx.transaction.mode(),
            isolation: tx.transaction.isolation(),
            staged_writes: tx.transaction.staged_write_count(),
        })
    }

    fn staged_writes(&self, session_id: u64, tx_id: u64) -> Result<Vec<StagedWrite>, String> {
        let transactions = self
            .transactions
            .lock()
            .map_err(|_| "transaction store lock poisoned".to_string())?;
        let tx = transactions
            .get(&tx_id)
            .ok_or_else(|| format!("unknown transaction: {tx_id}"))?;
        tx.ensure_session(session_id, tx_id)?;
        match &tx.transaction {
            NativeTransaction::ReadOnly(_) => Ok(Vec::new()),
            NativeTransaction::ReadWrite { staged_writes, .. } => Ok(staged_writes.clone()),
        }
    }

    fn close_session(&self, session_id: u64) -> Result<usize, String> {
        let mut transactions = self
            .transactions
            .lock()
            .map_err(|_| "transaction store lock poisoned".to_string())?;
        let before = transactions.len();
        transactions.retain(|_, tx| tx.session_id != session_id);
        Ok(before - transactions.len())
    }

    fn list(&self, session_id: u64) -> Result<Vec<TransactionInfo>, String> {
        let transactions = self
            .transactions
            .lock()
            .map_err(|_| "transaction store lock poisoned".to_string())?;
        let mut infos = transactions
            .iter()
            .filter(|(_, tx)| tx.session_id == session_id)
            .map(|(tx_id, tx)| TransactionInfo {
                session_id: tx.session_id,
                tx_id: *tx_id,
                mode: tx.transaction.mode(),
                isolation: tx.transaction.isolation(),
                staged_writes: tx.transaction.staged_write_count(),
            })
            .collect::<Vec<_>>();
        infos.sort_by_key(|info| info.tx_id);
        Ok(infos)
    }

    fn list_all(&self) -> Result<Vec<TransactionInfo>, String> {
        let transactions = self
            .transactions
            .lock()
            .map_err(|_| "transaction store lock poisoned".to_string())?;
        let mut infos = transactions
            .iter()
            .map(|(tx_id, tx)| TransactionInfo {
                session_id: tx.session_id,
                tx_id: *tx_id,
                mode: tx.transaction.mode(),
                isolation: tx.transaction.isolation(),
                staged_writes: tx.transaction.staged_write_count(),
            })
            .collect::<Vec<_>>();
        infos.sort_by_key(|info| (info.session_id, info.tx_id));
        Ok(infos)
    }

    fn status(&self, session_id: u64, tx_id: u64) -> Result<TransactionInfo, String> {
        let transactions = self
            .transactions
            .lock()
            .map_err(|_| "transaction store lock poisoned".to_string())?;
        let tx = transactions
            .get(&tx_id)
            .ok_or_else(|| format!("unknown transaction: {tx_id}"))?;
        tx.ensure_session(session_id, tx_id)?;
        Ok(TransactionInfo {
            session_id: tx.session_id,
            tx_id,
            mode: tx.transaction.mode(),
            isolation: tx.transaction.isolation(),
            staged_writes: tx.transaction.staged_write_count(),
        })
    }
}

struct TransactionState {
    session_id: u64,
    transaction: NativeTransaction,
}

#[derive(Clone, Default)]
struct PreparedTransactionStore {
    next_id: Arc<AtomicU64>,
    prepared: Arc<Mutex<HashMap<u64, PreparedWriteBatch>>>,
    path: Option<Arc<PathBuf>>,
}

impl PreparedTransactionStore {
    fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let prepared = load_prepared_transactions(&path)?;
        let next_id = prepared.keys().copied().max().unwrap_or(0);
        Ok(Self {
            next_id: Arc::new(AtomicU64::new(next_id)),
            prepared: Arc::new(Mutex::new(prepared)),
            path: Some(Arc::new(path)),
        })
    }

    fn prepare(
        &self,
        shard_id: u64,
        writes: Vec<(String, neo4r_query::QueryParams)>,
    ) -> Result<u64, String> {
        let prepared_id = self.next_id.fetch_add(1, Ordering::Relaxed) + 1;
        let mut prepared = self
            .prepared
            .lock()
            .map_err(|_| "prepared transaction store lock poisoned".to_string())?;
        prepared.insert(prepared_id, PreparedWriteBatch { shard_id, writes });
        if let Err(err) = self.save(&prepared) {
            prepared.remove(&prepared_id);
            return Err(err);
        }
        Ok(prepared_id)
    }

    fn take(&self, prepared_id: u64) -> Result<PreparedWriteBatch, String> {
        let mut prepared = self
            .prepared
            .lock()
            .map_err(|_| "prepared transaction store lock poisoned".to_string())?;
        let prepared_batch = prepared
            .remove(&prepared_id)
            .ok_or_else(|| format!("unknown prepared transaction: {prepared_id}"))?;
        if let Err(err) = self.save(&prepared) {
            prepared.insert(prepared_id, prepared_batch.clone());
            return Err(err);
        }
        Ok(prepared_batch)
    }

    fn status(&self, prepared_id: u64) -> Result<PreparedTransactionInfo, String> {
        let prepared = self
            .prepared
            .lock()
            .map_err(|_| "prepared transaction store lock poisoned".to_string())?;
        let batch = prepared
            .get(&prepared_id)
            .ok_or_else(|| format!("unknown prepared transaction: {prepared_id}"))?;
        Ok(PreparedTransactionInfo {
            prepared_id,
            shard_id: batch.shard_id,
            write_count: batch.writes.len(),
        })
    }

    fn list(&self) -> Result<Vec<PreparedTransactionInfo>, String> {
        let prepared = self
            .prepared
            .lock()
            .map_err(|_| "prepared transaction store lock poisoned".to_string())?;
        let mut infos = prepared
            .iter()
            .map(|(prepared_id, batch)| PreparedTransactionInfo {
                prepared_id: *prepared_id,
                shard_id: batch.shard_id,
                write_count: batch.writes.len(),
            })
            .collect::<Vec<_>>();
        infos.sort_by_key(|info| info.prepared_id);
        Ok(infos)
    }

    fn save(&self, prepared: &HashMap<u64, PreparedWriteBatch>) -> Result<(), String> {
        let Some(path) = &self.path else {
            return Ok(());
        };
        save_prepared_transactions(path, prepared)
    }
}

#[derive(Clone, Debug)]
struct PreparedWriteBatch {
    shard_id: u64,
    writes: Vec<(String, neo4r_query::QueryParams)>,
}

fn load_prepared_transactions(path: &Path) -> io::Result<HashMap<u64, PreparedWriteBatch>> {
    let file = match File::open(path) {
        Ok(file) => file,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(HashMap::new()),
        Err(err) => return Err(err),
    };
    let mut lines = BufReader::new(file).lines();
    let header = lines
        .next()
        .transpose()?
        .ok_or_else(|| io::Error::other("missing prepared transaction header"))?;
    if header != PREPARED_TRANSACTIONS_MAGIC {
        return Err(io::Error::other("invalid prepared transaction header"));
    }
    let mut prepared = HashMap::new();
    for line in lines {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let (prepared_id, batch) = decode_prepared_transaction_record(&line)?;
        prepared.insert(prepared_id, batch);
    }
    Ok(prepared)
}

fn save_prepared_transactions(
    path: &Path,
    prepared: &HashMap<u64, PreparedWriteBatch>,
) -> Result<(), String> {
    let tmp_path = path.with_extension("log.tmp");
    {
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&tmp_path)
            .map_err(|err| format!("open prepared transaction store: {err}"))?;
        writeln!(file, "{PREPARED_TRANSACTIONS_MAGIC}")
            .map_err(|err| format!("write prepared transaction header: {err}"))?;
        let mut ids = prepared.keys().copied().collect::<Vec<_>>();
        ids.sort_unstable();
        for prepared_id in ids {
            let batch = prepared
                .get(&prepared_id)
                .ok_or_else(|| format!("missing prepared transaction {prepared_id}"))?;
            writeln!(
                file,
                "{}",
                encode_prepared_transaction_record(prepared_id, batch)
            )
            .map_err(|err| format!("write prepared transaction record: {err}"))?;
        }
        file.sync_all()
            .map_err(|err| format!("sync prepared transaction store: {err}"))?;
    }
    fs::rename(&tmp_path, path)
        .map_err(|err| format!("rename prepared transaction store: {err}"))?;
    if let Some(parent) = path.parent() {
        File::open(parent)
            .and_then(|file| file.sync_all())
            .map_err(|err| format!("sync prepared transaction store directory: {err}"))?;
    }
    Ok(())
}

fn encode_prepared_transaction_record(prepared_id: u64, batch: &PreparedWriteBatch) -> String {
    format!(
        "{prepared_id}\t{}\t{}",
        batch.shard_id,
        encode_query_batch_payload(&batch.writes)
    )
}

fn decode_prepared_transaction_record(line: &str) -> io::Result<(u64, PreparedWriteBatch)> {
    let mut parts = line.splitn(3, '\t');
    let prepared_id = parts
        .next()
        .ok_or_else(|| io::Error::other("missing prepared transaction id"))?
        .parse::<u64>()
        .map_err(|_| io::Error::other("invalid prepared transaction id"))?;
    let shard_id = parts
        .next()
        .ok_or_else(|| io::Error::other("missing prepared transaction shard id"))?
        .parse::<u64>()
        .map_err(|_| io::Error::other("invalid prepared transaction shard id"))?;
    let writes = parts
        .next()
        .ok_or_else(|| io::Error::other("missing prepared transaction writes"))
        .and_then(|payload| decode_query_batch_payload(payload).map_err(io::Error::other))?;
    Ok((prepared_id, PreparedWriteBatch { shard_id, writes }))
}

impl TransactionState {
    fn ensure_session(&self, session_id: u64, tx_id: u64) -> Result<(), String> {
        if self.session_id == session_id {
            Ok(())
        } else {
            Err(format!("unknown transaction: {tx_id}"))
        }
    }
}

enum NativeTransaction {
    ReadOnly(Neo4rReadTransaction),
    ReadWrite {
        isolation: ReadIsolation,
        staged_writes: Vec<StagedWrite>,
    },
}

impl NativeTransaction {
    fn mode(&self) -> TransactionMode {
        match self {
            Self::ReadOnly(_) => TransactionMode::ReadOnly,
            Self::ReadWrite { .. } => TransactionMode::ReadWrite,
        }
    }

    fn staged_write_count(&self) -> usize {
        match self {
            Self::ReadOnly(_) => 0,
            Self::ReadWrite { staged_writes, .. } => staged_writes.len(),
        }
    }

    fn isolation(&self) -> ReadIsolation {
        match self {
            Self::ReadOnly(tx) => tx.options().isolation,
            Self::ReadWrite { isolation, .. } => *isolation,
        }
    }
}

struct TransactionInfo {
    session_id: u64,
    tx_id: u64,
    mode: TransactionMode,
    isolation: ReadIsolation,
    staged_writes: usize,
}

#[derive(Debug)]
struct PreparedTransactionInfo {
    prepared_id: u64,
    shard_id: u64,
    write_count: usize,
}

struct TransactionPlanContext {
    mode: TransactionMode,
    isolation: ReadIsolation,
    staged_writes: usize,
}

fn format_transaction_plan_context(context: &TransactionPlanContext) -> String {
    let staged_overlay = if context.staged_writes == 0 {
        "none"
    } else {
        "pending"
    };
    format!(
        "tx_mode={} tx_isolation={} staged_writes={} staged_overlay={}",
        format_transaction_mode(context.mode),
        format_read_isolation(context.isolation),
        context.staged_writes,
        staged_overlay
    )
}

fn format_tx_list(infos: Vec<TransactionInfo>) -> String {
    let entries = infos
        .iter()
        .map(|info| {
            format!(
                "{}:{}:{}:{}",
                info.tx_id,
                format_transaction_mode(info.mode),
                format_read_isolation(info.isolation),
                info.staged_writes
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    format!("OK\tTX_LIST\t{}\t{entries}", infos.len())
}

fn format_tx_status(info: TransactionInfo) -> String {
    format!(
        "OK\tTX_STATUS\t{}\t{}\t{}\t{}",
        info.tx_id,
        format_transaction_mode(info.mode),
        format_read_isolation(info.isolation),
        info.staged_writes
    )
}

fn format_tx_list_all(infos: Vec<TransactionInfo>) -> String {
    let entries = infos
        .iter()
        .map(|info| {
            format!(
                "{}:{}:{}:{}:{}",
                info.session_id,
                info.tx_id,
                format_transaction_mode(info.mode),
                format_read_isolation(info.isolation),
                info.staged_writes
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    format!("OK\tTX_LIST_ALL\t{}\t{entries}", infos.len())
}

fn format_prepared_tx_status(info: PreparedTransactionInfo) -> String {
    format!(
        "OK\tTX_PREPARED_STATUS\t{}\t{}\t{}",
        info.prepared_id, info.shard_id, info.write_count
    )
}

fn format_prepared_tx_list(infos: Vec<PreparedTransactionInfo>) -> String {
    let entries = infos
        .iter()
        .map(|info| {
            format!(
                "{}:{}:{}",
                info.prepared_id, info.shard_id, info.write_count
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    format!("OK\tTX_PREPARED_LIST\t{}\t{entries}", infos.len())
}

fn format_transaction_decisions(decisions: &[TransactionDecisionRecord]) -> String {
    let entries = decisions
        .iter()
        .map(|decision| {
            let participants = decision
                .participants
                .iter()
                .map(|participant| {
                    format!(
                        "{}@{}#{}",
                        participant.location, participant.shard_id, participant.prepared_id
                    )
                })
                .collect::<Vec<_>>()
                .join("|");
            format!(
                "tx={} decision={} participants={participants}",
                decision.tx_id,
                format_transaction_decision(&decision.decision)
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    format!("count={} entries={entries}", decisions.len())
}

fn format_transaction_decision(decision: &TransactionDecision) -> &'static str {
    match decision {
        TransactionDecision::Commit => "commit",
        TransactionDecision::Abort => "abort",
    }
}

fn format_transaction_mode(mode: TransactionMode) -> &'static str {
    match mode {
        TransactionMode::ReadOnly => "READ_ONLY",
        TransactionMode::ReadWrite => "READ_WRITE",
    }
}

fn format_read_isolation(isolation: ReadIsolation) -> &'static str {
    match isolation {
        ReadIsolation::ReadCommitted => "READ_COMMITTED",
        ReadIsolation::Snapshot => "SNAPSHOT",
    }
}

#[derive(Clone)]
struct StagedWrite {
    query: String,
    params: neo4r_query::QueryParams,
}

#[derive(Clone, Default)]
struct PreparedQueryStore {
    next_id: Arc<AtomicU64>,
    queries: Arc<Mutex<HashMap<u64, PreparedQueryState>>>,
}

impl PreparedQueryStore {
    fn prepare(&self, session_id: u64, query: String) -> u64 {
        let prepared_id = self.next_id.fetch_add(1, Ordering::Relaxed) + 1;
        self.queries
            .lock()
            .unwrap()
            .insert(prepared_id, PreparedQueryState { session_id, query });
        prepared_id
    }

    fn get(&self, session_id: u64, prepared_id: u64) -> Result<String, String> {
        let queries = self
            .queries
            .lock()
            .map_err(|_| "prepared query store lock poisoned".to_string())?;
        let query = queries
            .get(&prepared_id)
            .ok_or_else(|| format!("unknown prepared query: {prepared_id}"))?;
        ensure_prepared_query_owner(query, session_id, prepared_id)?;
        Ok(query.query.clone())
    }

    fn close(&self, session_id: u64, prepared_id: u64) -> Result<(), String> {
        let mut queries = self
            .queries
            .lock()
            .map_err(|_| "prepared query store lock poisoned".to_string())?;
        let query = queries
            .get(&prepared_id)
            .ok_or_else(|| format!("unknown prepared query: {prepared_id}"))?;
        ensure_prepared_query_owner(query, session_id, prepared_id)?;
        queries.remove(&prepared_id);
        Ok(())
    }

    fn list(&self, session_id: u64) -> Result<Vec<PreparedQueryInfo>, String> {
        let queries = self
            .queries
            .lock()
            .map_err(|_| "prepared query store lock poisoned".to_string())?;
        let mut infos = queries
            .iter()
            .filter(|(_, query)| query.session_id == session_id)
            .map(|(prepared_id, query)| PreparedQueryInfo {
                prepared_id: *prepared_id,
                query: query.query.clone(),
            })
            .collect::<Vec<_>>();
        infos.sort_by_key(|info| info.prepared_id);
        Ok(infos)
    }

    fn close_session(&self, session_id: u64) -> Result<usize, String> {
        let mut queries = self
            .queries
            .lock()
            .map_err(|_| "prepared query store lock poisoned".to_string())?;
        let before = queries.len();
        queries.retain(|_, query| query.session_id != session_id);
        Ok(before - queries.len())
    }
}

struct PreparedQueryState {
    session_id: u64,
    query: String,
}

struct PreparedQueryInfo {
    prepared_id: u64,
    query: String,
}

fn ensure_prepared_query_owner(
    query: &PreparedQueryState,
    session_id: u64,
    prepared_id: u64,
) -> Result<(), String> {
    if query.session_id == session_id {
        Ok(())
    } else {
        Err(format!(
            "prepared query {prepared_id} belongs to another session"
        ))
    }
}

fn format_prepared_query_list(infos: Vec<PreparedQueryInfo>) -> String {
    let count = infos.len();
    let entries = infos
        .into_iter()
        .map(|info| format!("{}:{}", info.prepared_id, escape_payload(&info.query)))
        .collect::<Vec<_>>()
        .join(",");
    format!("OK\tPREPARED_QUERY_LIST\t{count}\t{entries}")
}

fn format_prepared_query_route(prepared_id: u64, routing: String) -> String {
    format!("OK\tPREPARED_QUERY_ROUTE\t{prepared_id}\t{routing}")
}

fn format_tx_prepared_query_route(
    tx_id: u64,
    prepared_id: u64,
    routing: String,
    context: &TransactionPlanContext,
) -> String {
    format!(
        "OK\tTX_PREPARED_QUERY_ROUTE\t{tx_id}\t{prepared_id}\t{routing}\t{}",
        format_transaction_plan_context(context)
    )
}

fn format_prepared_query_describe(
    prepared_id: u64,
    query: &str,
    routing: String,
    params: Vec<String>,
) -> String {
    format!(
        "OK\tPREPARED_QUERY_DESC\t{prepared_id}\t{}\t{routing}\t{}\t{}",
        format_prepared_query_kind(query),
        params.len(),
        params.join(",")
    )
}

fn format_prepared_query_kind(query: &str) -> &'static str {
    if is_schema_cypher(query) {
        "SCHEMA"
    } else if is_write_cypher(query) {
        "WRITE"
    } else {
        "READ"
    }
}

fn prepared_query_routing_hint(db: &Neo4rDatabaseHandle, query: &str) -> Result<String, String> {
    if is_schema_cypher(query) {
        return Ok("SCHEMA".to_string());
    }
    if is_write_cypher(query) {
        return prepared_write_routing_hint(db, query);
    }
    let route = db.query_route().map_err(|err| err.to_string())?;
    Ok(format_read_routing_hint(route))
}

fn prepared_query_routing_hint_with_params(
    db: &Neo4rDatabaseHandle,
    query: &str,
    params: &neo4r_query::QueryParams,
) -> Result<String, String> {
    if is_schema_cypher(query) {
        return Ok("SCHEMA".to_string());
    }
    if !is_write_cypher(query) {
        let route = db.query_route().map_err(|err| err.to_string())?;
        return Ok(format_read_routing_hint(route));
    }
    if is_create_node_cypher(query) || is_merge_node_cypher(query) {
        let status = db.cluster_status().map_err(|err| err.to_string())?;
        let shard = if is_create_node_cypher(query) {
            select_create_node_write_shard(&status, query, params)?
        } else {
            select_merge_node_write_shard(&status, query, params)?
        };
        return Ok(format!("WRITE_SHARD:{}", shard.shard_id));
    }
    Ok("WRITE_TARGET_DYNAMIC".to_string())
}

fn prepared_write_routing_hint(db: &Neo4rDatabaseHandle, query: &str) -> Result<String, String> {
    let params = describe_query_parameters(query);
    if is_create_node_cypher(query) || is_merge_node_cypher(query) {
        if !params.is_empty() {
            return Ok("WRITE_SHARD_BY_PARAM".to_string());
        }
        let status = db.cluster_status().map_err(|err| err.to_string())?;
        let empty_params = neo4r_query::QueryParams::new();
        let shard = if is_create_node_cypher(query) {
            select_create_node_write_shard(&status, query, &empty_params)?
        } else {
            select_merge_node_write_shard(&status, query, &empty_params)?
        };
        return Ok(format!("WRITE_SHARD:{}", shard.shard_id));
    }
    Ok("WRITE_TARGET_DYNAMIC".to_string())
}

fn format_read_routing_hint(route: neo4r_db::QueryRoute) -> String {
    match route {
        neo4r_db::QueryRoute::LocalOnly => "READ_LOCAL".to_string(),
        neo4r_db::QueryRoute::RequiresRemoteShards(shards) => {
            let shards = shards
                .into_iter()
                .map(|shard| shard.to_string())
                .collect::<Vec<_>>()
                .join(",");
            format!("READ_REMOTE:{shards}")
        }
    }
}

fn describe_query_parameters(query: &str) -> Vec<String> {
    let mut params = BTreeSet::new();
    let mut chars = query.char_indices().peekable();
    let mut quote: Option<char> = None;

    while let Some((_, ch)) = chars.next() {
        if let Some(quote_char) = quote {
            if ch == '\\' {
                let _ = chars.next();
            } else if ch == quote_char {
                quote = None;
            }
            continue;
        }

        if ch == '\'' || ch == '"' {
            quote = Some(ch);
            continue;
        }
        if ch != '$' {
            continue;
        }

        let mut name = String::new();
        match chars.peek().copied() {
            Some((_, next)) if is_query_parameter_start(next) => {
                name.push(next);
                let _ = chars.next();
            }
            _ => continue,
        }
        while let Some((_, next)) = chars.peek().copied() {
            if !is_query_parameter_continue(next) {
                break;
            }
            name.push(next);
            let _ = chars.next();
        }
        params.insert(name);
    }

    params.into_iter().collect()
}

fn validate_prepared_query_params(
    prepared_id: u64,
    query: &str,
    params: &neo4r_query::QueryParams,
) -> Result<(), String> {
    let missing = describe_query_parameters(query)
        .into_iter()
        .filter(|name| !params.contains_key(name))
        .collect::<Vec<_>>();
    if missing.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "prepared query {prepared_id} missing parameter(s): {}",
            missing.join(",")
        ))
    }
}

fn is_query_parameter_start(ch: char) -> bool {
    ch == '_' || ch.is_ascii_alphabetic()
}

fn is_query_parameter_continue(ch: char) -> bool {
    ch == '_' || ch.is_ascii_alphanumeric()
}

#[derive(Clone, Default)]
struct CursorStore {
    next_id: Arc<AtomicU64>,
    cursors: Arc<Mutex<HashMap<u64, CursorState>>>,
}

impl CursorStore {
    fn insert(&self, session_id: u64, cursor: Box<dyn QueryCursor>) -> u64 {
        let cursor_id = self.next_id.fetch_add(1, Ordering::Relaxed) + 1;
        self.cursors
            .lock()
            .unwrap()
            .insert(cursor_id, CursorState { session_id, cursor });
        cursor_id
    }

    fn fetch(
        &self,
        session_id: u64,
        cursor_id: u64,
        page_size: usize,
    ) -> Result<ResultPage, String> {
        let mut cursors = self
            .cursors
            .lock()
            .map_err(|_| "cursor store lock poisoned".to_string())?;
        let cursor = cursors
            .get_mut(&cursor_id)
            .ok_or_else(|| format!("unknown cursor: {cursor_id}"))?;
        ensure_cursor_owner(cursor, session_id, cursor_id)?;
        let page = cursor.cursor.fetch(page_size);
        let rows = page.rows;
        let has_more = page.has_more;
        if !has_more {
            cursors.remove(&cursor_id);
        }
        Ok(ResultPage { rows, has_more })
    }

    fn close(&self, session_id: u64, cursor_id: u64) -> Result<(), String> {
        let mut cursors = self
            .cursors
            .lock()
            .map_err(|_| "cursor store lock poisoned".to_string())?;
        let Some(cursor) = cursors.get(&cursor_id) else {
            return Ok(());
        };
        ensure_cursor_owner(cursor, session_id, cursor_id)?;
        cursors.remove(&cursor_id);
        Ok(())
    }

    fn close_session(&self, session_id: u64) -> Result<usize, String> {
        let mut cursors = self
            .cursors
            .lock()
            .map_err(|_| "cursor store lock poisoned".to_string())?;
        let before = cursors.len();
        cursors.retain(|_, cursor| cursor.session_id != session_id);
        Ok(before - cursors.len())
    }
}

struct CursorState {
    session_id: u64,
    cursor: Box<dyn QueryCursor>,
}

fn ensure_cursor_owner(
    cursor: &CursorState,
    session_id: u64,
    cursor_id: u64,
) -> Result<(), String> {
    if cursor.session_id == session_id {
        Ok(())
    } else {
        Err(format!("cursor {cursor_id} belongs to another session"))
    }
}

#[derive(Clone, Default)]
struct PendingRequestStore {
    state: Arc<Mutex<PendingRequestState>>,
}

#[derive(Default)]
struct PendingRequestState {
    pending: BTreeSet<(u64, u64)>,
    cancelled: BTreeSet<(u64, u64)>,
}

impl PendingRequestStore {
    fn register(&self, session_id: u64, request_id: u64) -> Result<(), String> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| "pending request store lock poisoned".to_string())?;
        state.pending.insert((session_id, request_id));
        state.cancelled.remove(&(session_id, request_id));
        Ok(())
    }

    fn cancel(&self, session_id: u64, request_id: u64) -> Result<bool, String> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| "pending request store lock poisoned".to_string())?;
        if state.pending.contains(&(session_id, request_id)) {
            state.cancelled.insert((session_id, request_id));
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn take_cancelled(&self, session_id: u64, request_id: u64) -> Result<bool, String> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| "pending request store lock poisoned".to_string())?;
        state.pending.remove(&(session_id, request_id));
        Ok(state.cancelled.remove(&(session_id, request_id)))
    }

    fn start(&self, session_id: u64, request_id: u64) -> Result<(), String> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| "pending request store lock poisoned".to_string())?;
        state.pending.remove(&(session_id, request_id));
        state.cancelled.remove(&(session_id, request_id));
        Ok(())
    }

    fn close_session(&self, session_id: u64) -> Result<(), String> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| "pending request store lock poisoned".to_string())?;
        state
            .pending
            .retain(|(pending_session_id, _)| *pending_session_id != session_id);
        state
            .cancelled
            .retain(|(pending_session_id, _)| *pending_session_id != session_id);
        Ok(())
    }
}

#[derive(Debug)]
struct ResultPage {
    rows: Vec<QueryRow>,
    has_more: bool,
}

struct FetchRequest {
    cursor_id: u64,
    page_size: usize,
}

fn parse_fetch_payload(payload: &str) -> Result<FetchRequest, String> {
    let mut parts = payload.split('\t');
    let cursor_id = parse_cursor_id(
        parts
            .next()
            .ok_or_else(|| "FETCH requires cursor id".to_string())?,
    )?;
    let page_size = parts
        .next()
        .map(|value| {
            value
                .parse::<usize>()
                .map_err(|_| "FETCH page size must be a positive integer".to_string())
        })
        .transpose()?
        .unwrap_or(128);
    if page_size == 0 {
        return Err("FETCH page size must be greater than zero".to_string());
    }
    if parts.next().is_some() {
        return Err("FETCH got extra fields".to_string());
    }
    Ok(FetchRequest {
        cursor_id,
        page_size,
    })
}

fn parse_cursor_id(payload: &str) -> Result<u64, String> {
    payload
        .trim()
        .parse::<u64>()
        .map_err(|_| "cursor id must be an unsigned integer".to_string())
}

fn parse_cancel_payload(payload: &str) -> Result<u64, String> {
    let mut parts = payload.trim().split('\t');
    let request_id = parts
        .next()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| "CANCEL requires target request id".to_string())?
        .parse::<u64>()
        .map_err(|_| "CANCEL target request id must be an unsigned integer".to_string())?;
    if parts.next().is_some() {
        return Err("CANCEL got extra fields".to_string());
    }
    Ok(request_id)
}

fn format_result_start(cursor_id: u64, total_rows: Option<usize>, page: ResultPage) -> String {
    let total_rows = total_rows
        .map(|total_rows| total_rows.to_string())
        .unwrap_or_else(|| "UNKNOWN".to_string());
    format!(
        "OK\tRESULT_START\t{cursor_id}\t{total_rows}\t{}\t{}\t{}",
        page.rows.len(),
        page.has_more,
        format_rows(&page.rows)
    )
}

fn format_result_page(cursor_id: u64, page: ResultPage) -> String {
    format!(
        "OK\tRESULT_PAGE\t{cursor_id}\t{}\t{}\t{}",
        page.rows.len(),
        page.has_more,
        format_rows(&page.rows)
    )
}

fn format_rows(rows: &[QueryRow]) -> String {
    encode_query_rows(rows)
}

fn native_response_frame(request_id: u64, response: BackendResponse) -> NativeFrame {
    let message_type = if matches!(response, BackendResponse::Err(_)) {
        NativeMessageType::Error
    } else {
        NativeMessageType::Response
    };
    NativeFrame::new(
        message_type,
        request_id,
        format_response(&response).into_bytes(),
    )
}

fn escape_payload(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('\t', "\\t")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
        .replace('\x1e', "\\x1e")
}

#[derive(Clone)]
struct NativeWorkerPool {
    jobs: Arc<Mutex<Option<SyncSender<NativeJob>>>>,
    joins: Arc<Mutex<Vec<thread::JoinHandle<()>>>>,
    pending_requests: PendingRequestStore,
}

impl NativeWorkerPool {
    fn new(context: NativeExecutionContext, worker_count: usize, queue_capacity: usize) -> Self {
        let worker_count = worker_count.max(1);
        let queue_capacity = queue_capacity.max(1);
        let pending_requests = context.pending_requests.clone();
        let (jobs, job_rx) = mpsc::sync_channel::<NativeJob>(queue_capacity);
        let job_rx = Arc::new(Mutex::new(job_rx));
        let mut joins = Vec::with_capacity(worker_count);

        for _ in 0..worker_count {
            let context = context.clone();
            let job_rx = job_rx.clone();
            joins.push(thread::spawn(move || native_worker_loop(context, job_rx)));
        }

        Self {
            jobs: Arc::new(Mutex::new(Some(jobs))),
            joins: Arc::new(Mutex::new(joins)),
            pending_requests,
        }
    }

    fn submit(
        &self,
        session_id: u64,
        frame: NativeFrame,
        response: mpsc::Sender<NativeFrame>,
    ) -> io::Result<()> {
        let request_id = frame.request_id;
        let jobs = self
            .jobs
            .lock()
            .map_err(|_| io::Error::other("native worker pool lock poisoned"))?
            .as_ref()
            .cloned()
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::BrokenPipe, "native worker pool stopped")
            })?;
        self.pending_requests
            .register(session_id, request_id)
            .map_err(io::Error::other)?;
        let job = NativeJob {
            session_id,
            frame,
            response,
        };
        match jobs.try_send(job) {
            Ok(()) => {}
            Err(TrySendError::Full(job)) => {
                let _ = self.pending_requests.start(session_id, request_id);
                send_native_response(
                    &job.response,
                    NativeFrame::new(
                        NativeMessageType::Error,
                        request_id,
                        b"ERR\tnative worker queue full".to_vec(),
                    ),
                )?;
            }
            Err(TrySendError::Disconnected(_)) => {
                let _ = self.pending_requests.start(session_id, request_id);
                return Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "native worker pool stopped",
                ));
            }
        }
        Ok(())
    }
}

impl Drop for NativeWorkerPool {
    fn drop(&mut self) {
        if Arc::strong_count(&self.jobs) != 1 {
            return;
        }
        if let Ok(mut jobs) = self.jobs.lock() {
            jobs.take();
        }
        if let Ok(mut joins) = self.joins.lock() {
            while let Some(join) = joins.pop() {
                let _ = join.join();
            }
        }
    }
}

struct NativeJob {
    session_id: u64,
    frame: NativeFrame,
    response: mpsc::Sender<NativeFrame>,
}

fn native_worker_loop(context: NativeExecutionContext, jobs: Arc<Mutex<Receiver<NativeJob>>>) {
    loop {
        let job = {
            let jobs = match jobs.lock() {
                Ok(jobs) => jobs,
                Err(_) => break,
            };
            match jobs.recv() {
                Ok(job) => job,
                Err(_) => break,
            }
        };
        if context
            .pending_requests
            .take_cancelled(job.session_id, job.frame.request_id)
            .unwrap_or(false)
        {
            let response = NativeFrame::new(
                NativeMessageType::Error,
                job.frame.request_id,
                b"ERR\trequest cancelled".to_vec(),
            );
            let _ = job.response.send(response);
            continue;
        }
        let _ = context
            .pending_requests
            .start(job.session_id, job.frame.request_id);
        let response = context.execute_frame(job.session_id, job.frame);
        let _ = job.response.send(response);
    }
}

fn write_native_responses(stream: TcpStream, responses: Receiver<NativeFrame>) -> io::Result<()> {
    let mut writer = BufWriter::new(stream);
    for frame in responses {
        write_frame(&mut writer, &frame)?;
    }
    Ok(())
}

fn send_native_response(
    response_tx: &mpsc::Sender<NativeFrame>,
    frame: NativeFrame,
) -> io::Result<()> {
    response_tx
        .send(frame)
        .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "native response writer stopped"))
}

fn default_worker_count() -> usize {
    thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(4)
}

#[cfg(test)]
mod tests;
