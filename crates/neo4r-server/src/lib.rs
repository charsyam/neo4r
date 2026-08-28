//! TCP backend for neo4r.

mod peer_store;
mod protocol;
#[path = "backend/restore_guard.rs"]
mod restore_guard;
mod tenant;
mod web_auth;

use neo4r_core::{Properties, ShardRole, Value};
use neo4r_db::{
    catch_up_from_tcp_primary, catch_up_from_tcp_primary_batched, create_node_routing_key,
    handle_tcp_replication_stream, merge_node_routing_key, request_tcp_replication_hello,
    CreateNodeRoutingKey, DatabaseConfig, Neo4rDatabaseHandle, Neo4rReadTransaction,
    NodeMembershipState, QueryOptions, ReadConsistency, ReadIsolation, ReplicationChannelKind,
    ReplicationEndpoint, ReplicationNodeIdentity,
};
use neo4r_query::{QueryCursor, QueryParams, QueryRow, QueryValue, VecQueryCursor};
use neo4r_storage::{
    TransactionDecision, TransactionDecisionRecord, TransactionDecisionStore,
    TransactionParticipantRecord,
};
use peer_store::{
    format_query_peers, QueryPeerStore, ReplicationPeerIdentity, ReplicationPeerIdentityStore,
    QUERY_PEERS_FILE, REPLICATION_PEERS_FILE, REPLICATION_PEER_IDENTITIES_FILE,
};
use protocol::{
    decode_index_catalog, decode_query_batch_payload, decode_query_rows,
    encode_query_batch_payload, encode_query_rows, execute_request, format_protocol_capabilities,
    format_query_plan, format_response, format_routing_table, parse_query_payload, parse_request,
    write_response, BackendRedirect, BackendResponse, RedirectKind,
};
use restore_guard::{restore_maintenance_mode_path, RestoreLock};
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
    validate_web_user_token, web_role_from_token, WebAuditStore, WebRole, WebSessionStore,
    WebUserToken, WebUserTokenStore, WEB_AUDIT_ROCKS_DIR, WEB_AUTH_ROCKS_DIR,
    WEB_SESSION_ROCKS_DIR,
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
    replication_peer_identities: ReplicationPeerIdentityStore,
    read_preference: QueryReadPreference,
    catch_up_connect_timeout: Duration,
    pending_requests: PendingRequestStore,
    web_auth_token: Option<String>,
    slow_query_threshold: Duration,
    metrics: WebMetrics,
    auth_limiter: AuthFailureLimiter,
    slow_queries: SlowQueryLog,
    web_user_tokens: Option<WebUserTokenStore>,
    web_audit: Option<WebAuditStore>,
    web_sessions: Option<WebSessionStore>,
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
    auth_failures: Arc<AtomicU64>,
    auth_rate_limited: Arc<AtomicU64>,
    queries: Arc<AtomicU64>,
    query_errors: Arc<AtomicU64>,
    slow_queries: Arc<AtomicU64>,
    registry_requests: Arc<AtomicU64>,
    stale_epoch_rejections: Arc<AtomicU64>,
    redirects: Arc<AtomicU64>,
}

#[derive(Clone, Default)]
struct AuthFailureLimiter {
    entries: Arc<Mutex<HashMap<String, AuthFailureEntry>>>,
}

#[derive(Clone, Copy, Debug, Default)]
struct AuthFailureEntry {
    window_start_ms: u128,
    failures: u64,
}

impl AuthFailureLimiter {
    fn record_and_should_limit(&self, key: &str, now_ms: u128) -> bool {
        const WINDOW_MS: u128 = 60_000;
        const MAX_FAILURES: u64 = 5;
        let mut entries = self.entries.lock().unwrap();
        let entry = entries.entry(key.to_string()).or_default();
        if now_ms.saturating_sub(entry.window_start_ms) > WINDOW_MS {
            *entry = AuthFailureEntry {
                window_start_ms: now_ms,
                failures: 0,
            };
        }
        entry.failures = entry.failures.saturating_add(1);
        entry.failures > MAX_FAILURES
    }
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

#[path = "backend/backend_core.rs"]
mod backend_core;
#[path = "backend/backend_native_replication.rs"]
mod backend_native_replication;
#[path = "backend/backend_web_admin.rs"]
mod backend_web_admin;
#[path = "backend/backend_web_query_backup.rs"]
mod backend_web_query_backup;
#[path = "backend/distributed_query.rs"]
mod distributed_query;
#[path = "backend/http_json_backup.rs"]
mod http_json_backup;
#[path = "backend/native_execution.rs"]
mod native_execution;
#[path = "backend/native_worker.rs"]
mod native_worker;
#[path = "backend/prepared_query.rs"]
mod prepared_query;
#[path = "backend/remote_transactions.rs"]
mod remote_transactions;
#[path = "backend/replication_admin.rs"]
mod replication_admin;
#[path = "backend/transaction_protocol.rs"]
mod transaction_protocol;
#[path = "backend/transaction_store.rs"]
mod transaction_store;
#[path = "backend/web_index.rs"]
mod web_index;

#[allow(unused_imports)]
use backend_core::*;
#[allow(unused_imports)]
use backend_native_replication::*;
#[allow(unused_imports)]
use backend_web_admin::*;
#[allow(unused_imports)]
use backend_web_query_backup::*;
#[allow(unused_imports)]
use distributed_query::*;
#[allow(unused_imports)]
use http_json_backup::*;
#[allow(unused_imports)]
use native_execution::*;
#[allow(unused_imports)]
use native_worker::*;
#[allow(unused_imports)]
use prepared_query::*;
#[allow(unused_imports)]
use remote_transactions::*;
#[allow(unused_imports)]
use replication_admin::*;
#[allow(unused_imports)]
use transaction_protocol::*;
#[allow(unused_imports)]
use transaction_store::*;
#[allow(unused_imports)]
use web_index::*;
fn default_worker_count() -> usize {
    thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(4)
}

#[cfg(test)]
mod tests;
