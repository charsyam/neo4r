//! TCP backend for neo4r.

mod peer_store;
mod production_primitives;
mod protocol;
mod tenant;
mod web_auth;

use neo4r_core::{Properties, ShardRole, Value};
#[cfg(feature = "rdma")]
use neo4r_db::RdmaReplicationListener;
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
    format_gossip_nodes, format_query_peers, GossipNodeRecord, GossipNodeStore, QueryPeerStore,
    ReplicationPeerIdentity, ReplicationPeerIdentityStore, GOSSIP_NODES_FILE, QUERY_PEERS_FILE,
    REPLICATION_PEERS_FILE, REPLICATION_PEER_IDENTITIES_FILE,
};
use protocol::{
    backend_request_mutates_data, decode_index_catalog, decode_query_batch_payload,
    decode_query_rows, encode_query_batch_payload, encode_query_rows, format_protocol_capabilities,
    format_query_plan, format_response, format_routing_table, parse_query_payload, parse_request,
    write_response, BackendRedirect, BackendResponse, RedirectKind,
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
    constant_time_token_eq, format_database_roles, latest_audit_unix_seconds, parse_database_roles,
    parse_web_role, unix_millis_now, unix_seconds_now, validate_web_token_id,
    validate_web_user_name, validate_web_user_token, web_role_from_token, WebAuditStore, WebRole,
    WebSessionStore, WebUserToken, WebUserTokenStore, WEB_AUDIT_ROCKS_DIR, WEB_AUTH_ROCKS_DIR,
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
    gossip_nodes: GossipNodeStore,
    gossip_auth_token: GossipAuthTokenStore,
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
    tenant_quota: TenantQuota,
    native_tls_acceptor: Option<NativeTlsAcceptor>,
    web_tls_acceptor: Option<NativeTlsAcceptor>,
    replication_tls_acceptor: Option<NativeTlsAcceptor>,
    replication_tls_channel_config: ReplicationTlsChannelConfigStore,
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

mod backend;

#[allow(unused_imports)]
use backend::*;
pub use backend::{NativeTlsConfig, ReplicationTlsConfig, TlsReplicationChannel};
fn default_worker_count() -> usize {
    thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(4)
}

#[cfg(test)]
mod tests;
