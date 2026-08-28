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


include!("native_execution/frames_queries.rs");
include!("native_execution/transactions.rs");
