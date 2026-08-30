use super::*;
#[derive(Clone)]
pub(crate) struct NativeExecutionContext {
    pub(crate) db: Neo4rDatabaseHandle,
    pub(crate) cursors: CursorStore,
    pub(crate) transactions: TransactionStore,
    pub(crate) prepared_transactions: PreparedTransactionStore,
    pub(crate) prepared_queries: PreparedQueryStore,
    pub(crate) query_peers: QueryPeerStore,
    pub(crate) replication_peers: QueryPeerStore,
    pub(crate) replication_peer_identities: ReplicationPeerIdentityStore,
    pub(crate) gossip_nodes: GossipNodeStore,
    pub(crate) gossip_auth_token: GossipAuthTokenStore,
    pub(crate) metrics: WebMetrics,
    pub(crate) default_page_size: usize,
    pub(crate) read_preference: QueryReadPreference,
    pub(crate) catch_up_connect_timeout: Duration,
    pub(crate) pending_requests: PendingRequestStore,
    pub(crate) tenant_quota: TenantQuota,
    pub(crate) replication_tls_channel_config: ReplicationTlsChannelConfigStore,
}

mod frames_queries;
mod transactions;
