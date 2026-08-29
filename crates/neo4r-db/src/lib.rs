//! Durable database facade for neo4r.

mod consensus;
mod database;
mod error;
mod raft;
mod replication;

pub use consensus::{
    InProcessShardConsensus, ProposedCommand, ShardCommitState, ShardConsensus,
    StaticPrimaryShardReplication,
};
pub use database::{
    create_node_routing_key, merge_node_routing_key, ClusterManagementStatus, ClusterMetadataState,
    ClusterStatus, CreateNodeRoutingKey, DatabaseConfig, DistributedQueryPlan, FailureInjection,
    IndexLifecycleStatus, MetadataOperationRecord, Neo4rDatabase, Neo4rDatabaseHandle,
    Neo4rReadSnapshot, Neo4rReadTransaction, QueryAccessPlan, QueryMetrics, QueryOperatorProfile,
    QueryOptions, QueryProfile, QueryRoute, RaftShardStatus, ReadConsistency, ReadIsolation,
    RebalanceAdvanceResult, RebalanceAutomationSummary, RebalanceExecution, RebalancePlan,
    RebalancePlanState, RebalancePolicy, RebalanceStep, RebalanceStepExecution, RebalanceStepState,
    RemoteTraversalPolicy, ShardStatus, StatisticsCatalog, StorageMaintenanceResult, StorageStatus,
    VectorIndexStatus,
};
pub use error::{DatabaseError, DatabaseResult};
pub use neo4r_storage::{
    ClusterMembership, ClusterNode, ClusterShardAssignment, NodeMembershipState,
    ShardAssignmentState,
};
pub use neo4r_storage::{IndexCatalog, IndexDefinition, IndexKind};
pub use raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    PreVoteRequest, PreVoteResponse, RaftCore, RaftMembership, RaftMembershipChange,
    RaftPersistentState, RaftPersistentStateStore, RaftRole, RaftSnapshotMetadata,
    RequestVoteRequest, RequestVoteResponse, SnapshotChunkAssembler,
};
pub use replication::{
    catch_up_from_tcp_primaries, catch_up_from_tcp_primaries_batched, catch_up_from_tcp_primary,
    catch_up_from_tcp_primary_batched, handle_tcp_replication_stream,
    negotiate_replication_channel, request_tcp_install_snapshot,
    request_tcp_raft_append_or_install_snapshot, request_tcp_raft_leader_transfer,
    request_tcp_raft_pre_vote, request_tcp_raft_vote, request_tcp_replication_hello,
    InProcessShardReplicator, MockRdmaReplicationProvider, NoopShardReplicator,
    RaftAppendChannelResponse, RdmaProbeReport, RdmaReplicationProvider, ReliableDatagramSocket,
    ReplicationAckPolicy, ReplicationChannel, ReplicationChannelAgreement,
    ReplicationChannelCapabilities, ReplicationChannelConfig, ReplicationChannelKind,
    ReplicationChannelMetricsSnapshot, ReplicationChannelOffer, ReplicationEndpoint,
    ReplicationNodeIdentity, ReplicationOutcome, ShardReplicator, TcpCatchUpResult,
    TcpRaftAppendResponse, TcpReplicationChannel, TcpShardReplicator, UdpReplicationChannel,
    UnsupportedReplicationChannel,
};
#[cfg(feature = "rdma")]
pub use replication::{
    RdmaProbeOptions, RdmaReplicationChannel, RdmaReplicationListener, RsocketStream,
    SystemRdmaReplicationProvider,
};
