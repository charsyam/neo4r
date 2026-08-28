//! TCP backend for neo4r.

mod peer_store;
mod protocol;
mod tenant;
mod web_auth;

use neo4r_core::{Properties, Value};
use neo4r_db::{
    catch_up_from_tcp_primary, catch_up_from_tcp_primary_batched, create_node_routing_key,
    handle_tcp_replication_stream, merge_node_routing_key, CreateNodeRoutingKey, DatabaseConfig,
    Neo4rDatabaseHandle, Neo4rReadTransaction, QueryOptions, ReadIsolation,
};
use neo4r_query::{QueryCursor, QueryParams, QueryRow, QueryValue, VecQueryCursor};
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
use std::io::{self, BufRead, BufReader, BufWriter, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream, ToSocketAddrs};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender, TrySendError};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tenant::{validate_database_name, TenantDatabaseManager, DEFAULT_DATABASE};
use web_auth::{
    constant_time_token_eq, format_database_roles, parse_database_roles, parse_web_role,
    unix_millis_now, unix_seconds_now, validate_web_token_id, validate_web_user_name,
    validate_web_user_token, web_role_from_token, WebAuditStore, WebRole, WebUserToken,
    WebUserTokenStore, WEB_AUDIT_ROCKS_DIR, WEB_AUTH_ROCKS_DIR,
};

const PREPARED_TRANSACTIONS_FILE: &str = "prepared.log";
const PREPARED_TRANSACTIONS_MAGIC: &str = "N4RPTX1";
const BACKUP_MANIFEST_FILE: &str = "neo4r-backup-manifest.txt";

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
    web_auth_token: Option<String>,
    slow_query_threshold: Duration,
    metrics: WebMetrics,
    slow_queries: SlowQueryLog,
    web_user_tokens: Option<WebUserTokenStore>,
    web_audit: Option<WebAuditStore>,
    tenant_databases: Option<TenantDatabaseManager>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
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

#[derive(Clone, Default)]
struct WebMetrics {
    http_requests: Arc<AtomicU64>,
    http_errors: Arc<AtomicU64>,
    queries: Arc<AtomicU64>,
    query_errors: Arc<AtomicU64>,
    slow_queries: Arc<AtomicU64>,
}

#[derive(Clone, Default)]
struct SlowQueryLog {
    entries: Arc<Mutex<Vec<SlowQueryEntry>>>,
}

#[derive(Clone)]
struct SlowQueryEntry {
    unix_ms: u128,
    elapsed_ms: u128,
    query: String,
}

include!("backend/backend_core.rs");
include!("backend/backend_web_admin.rs");
include!("backend/backend_web_query_backup.rs");
include!("backend/backend_native_replication.rs");

include!("backend/native_execution.rs");
include!("backend/replication_admin.rs");
include!("backend/distributed_query.rs");
include!("backend/remote_transactions.rs");
include!("backend/transaction_protocol.rs");
include!("backend/transaction_store.rs");
include!("backend/prepared_query.rs");
include!("backend/native_worker.rs");
include!("backend/http_json_backup.rs");
include!("backend/web_index.rs");
fn default_worker_count() -> usize {
    thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(4)
}

#[cfg(test)]
mod tests;
