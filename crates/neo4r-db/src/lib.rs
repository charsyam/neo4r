//! Durable database facade for neo4r.

mod consensus;
mod database;
mod error;
mod replication;

pub use consensus::{InProcessShardConsensus, ProposedCommand, ShardCommitState, ShardConsensus};
pub use database::{
    create_node_routing_key, merge_node_routing_key, ClusterManagementStatus, ClusterMetadataState,
    ClusterStatus, CreateNodeRoutingKey, DatabaseConfig, DistributedQueryPlan,
    MetadataOperationRecord, Neo4rDatabase, Neo4rDatabaseHandle, Neo4rReadSnapshot,
    Neo4rReadTransaction, QueryAccessPlan, QueryMetrics, QueryOperatorProfile, QueryOptions,
    QueryProfile, QueryRoute, ReadConsistency, ReadIsolation, RebalanceAdvanceResult,
    RebalanceExecution, RebalancePlan, RebalancePlanState, RebalancePolicy, RebalanceStep,
    RebalanceStepExecution, RebalanceStepState, RemoteTraversalPolicy, ShardStatus,
    StatisticsCatalog, StorageMaintenanceResult, StorageStatus, VectorIndexStatus,
};
pub use error::{DatabaseError, DatabaseResult};
pub use neo4r_storage::{
    ClusterMembership, ClusterNode, ClusterShardAssignment, NodeMembershipState,
    ShardAssignmentState,
};
pub use neo4r_storage::{IndexCatalog, IndexDefinition, IndexKind};
pub use replication::{
    catch_up_from_tcp_primaries, catch_up_from_tcp_primaries_batched, catch_up_from_tcp_primary,
    catch_up_from_tcp_primary_batched, handle_tcp_replication_stream, InProcessShardReplicator,
    NoopShardReplicator, ReplicationAckPolicy, ReplicationOutcome, ShardReplicator,
    TcpCatchUpResult, TcpShardReplicator,
};
