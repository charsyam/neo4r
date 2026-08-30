use neo4r_core::{
    Properties, ShardId, ShardPlacement, ShardReplica, ShardRole, ShardRoutingTable, Value,
};
use neo4r_db::{
    ClusterManagementStatus, ClusterMembership, ClusterMetadataState, ClusterStatus,
    DistributedQueryPlan, IndexCatalog, IndexDefinition, IndexKind, MetadataOperationRecord,
    NodeMembershipState, QueryAccessPlan, QueryOperatorProfile, QueryProfile, QueryRoute,
    RebalanceExecution, RebalancePlan, RebalancePlanState, RebalanceStep, RebalanceStepState,
    RemoteTraversalPolicy, ReplicationChannelKind, ShardAssignmentState, ShardStatus,
    StatisticsCatalog, StorageMaintenanceResult, StorageStatus,
};
use neo4r_query::{QueryParams, QueryRow};
use std::io::{self, Write};

#[derive(Clone, Debug, PartialEq)]
pub enum BackendRequest {
    Ping,
    Quit,
    Query {
        query: String,
        params: QueryParams,
    },
    QueryPlan {
        query: String,
        params: QueryParams,
    },
    Profile {
        query: String,
        params: QueryParams,
    },
    QueryShard {
        shard_id: ShardId,
        query: String,
        params: QueryParams,
    },
    QueryStagedShard {
        shard_id: ShardId,
        query: String,
        params: QueryParams,
        staged_writes: Vec<(String, QueryParams)>,
    },
    QueryWriteShard {
        shard_id: ShardId,
        query: String,
        params: QueryParams,
    },
    QueryWriteBatchShard {
        shard_id: ShardId,
        writes: Vec<(String, QueryParams)>,
    },
    QueryDistributed {
        query: String,
        params: QueryParams,
    },
    RegisterQueryPeer {
        server_id: u64,
        address: String,
    },
    UnregisterQueryPeer(u64),
    ListQueryPeers,
    RegisterReplicationPeer {
        server_id: u64,
        address: String,
        node_id: Option<u64>,
        transport: Option<ReplicationChannelKind>,
    },
    NegotiateReplicationPeer {
        server_id: u64,
        address: String,
        node_id: Option<u64>,
    },
    UnregisterReplicationPeer(u64),
    ListReplicationPeers,
    GossipNode {
        server_id: u64,
        query_address: String,
        replication_address: String,
        incarnation: u64,
        ttl_ms: u64,
    },
    ListGossipNodes,
    GossipRefreshFromMembership,
    ReplicationPeerStatus {
        server_id: Option<u64>,
    },
    ReplicationStatus,
    RoutingTable,
    ClusterRegistry,
    Capabilities,
    CatchUpFromPrimaries {
        max_entries_per_request: Option<usize>,
    },
    CatchUpFromPrimary {
        server_id: u64,
        max_entries_per_request: Option<usize>,
    },
    CatchUpPlan {
        server_id: Option<u64>,
    },
    ListTransactionDecisions,
    RecoverTransactionDecisions,
    CreateNode {
        labels: Vec<String>,
        properties: Properties,
    },
    CreateNodeOnShard {
        shard_id: ShardId,
        labels: Vec<String>,
        properties: Properties,
    },
    CreateRelationship {
        from: u64,
        to: u64,
        rel_type: String,
        properties: Properties,
    },
    SetNodeProperty {
        id: u64,
        key: String,
        value: Value,
    },
    RemoveNodeProperty {
        id: u64,
        key: String,
    },
    AddNodeLabel {
        id: u64,
        label: String,
    },
    RemoveNodeLabel {
        id: u64,
        label: String,
    },
    SetRelationshipProperty {
        id: u64,
        key: String,
        value: Value,
    },
    RemoveRelationshipProperty {
        id: u64,
        key: String,
    },
    DeleteNode(u64),
    DeleteRelationship(u64),
    CreateIndex {
        name: String,
        label: String,
        property: String,
        if_not_exists: bool,
    },
    CreateUniqueConstraint {
        name: String,
        label: String,
        property: String,
        if_not_exists: bool,
    },
    CreateVectorIndex {
        name: String,
        label: String,
        property: String,
        dimensions: usize,
        metric: String,
        if_not_exists: bool,
    },
    RebuildVectorIndex {
        name: String,
    },
    RebuildVectorIndexes,
    VectorIndexStatus {
        name: Option<String>,
    },
    DropIndex {
        name: String,
        if_exists: bool,
    },
    DropConstraint {
        name: String,
        if_exists: bool,
    },
    ListIndexes,
    DumpIndexCatalog,
    InstallIndexCatalog(IndexCatalog),
    SyncIndexCatalogFromPeer(u64),
    InstallRoutingTable(ShardRoutingTable),
    ClusterStatus,
    StorageStatus,
    Statistics,
    CheckpointNow,
    CompactStorage,
    SnapshotNow,
    RestoreSnapshot {
        shard_id: ShardId,
    },
    VerifyInvariants,
    RepairInvariants,
    BackupNow,
    RaftStatus,
    RaftLeaderTransfer {
        shard_id: ShardId,
        transferee_id: u64,
    },
    MetadataLog,
    RegisterNode {
        server_id: u64,
        address: String,
    },
    JoinRequest {
        server_id: u64,
        address: String,
        protocol_version: u64,
        storage_version: u64,
        shard_count: u64,
    },
    JoinAccept(u64),
    JoinReject {
        server_id: u64,
        reason: String,
    },
    DecommissionNode(u64),
    ListNodes,
    MetadataAuthority,
    SetMetadataAuthority(u64),
    SetRebalancePolicy {
        replication_factor: usize,
        max_steps_per_plan: usize,
    },
    PlanRebalance,
    StartRebalance,
    CancelRebalance,
    RebalanceStatus,
    AdvanceRebalance,
    ClusterManagementStatus,
    PrepareRebalanceStep(RebalanceStep),
    MarkShardCaughtUp {
        shard_id: u64,
        server_id: u64,
        match_index: u64,
    },
    PromoteCaughtUpNode(u64),
    WriteBootstrapManifest {
        mode: String,
        cluster_id: String,
        database_id: String,
    },
    BootstrapSafety {
        expected_cluster_id: String,
        force_new_cluster: bool,
    },
    TopologyObserve,
    TopologyReconcile {
        max_entries_per_request: Option<usize>,
    },
    OperationalSafety {
        operation: String,
        confirmation: Option<String>,
    },
    ChaosChecks,
    ApplyRebalanceStep(RebalanceStep),
}

pub fn backend_request_mutates_data(request: &BackendRequest) -> bool {
    matches!(
        request,
        BackendRequest::CreateNode { .. }
            | BackendRequest::CreateNodeOnShard { .. }
            | BackendRequest::CreateRelationship { .. }
            | BackendRequest::SetNodeProperty { .. }
            | BackendRequest::RemoveNodeProperty { .. }
            | BackendRequest::AddNodeLabel { .. }
            | BackendRequest::RemoveNodeLabel { .. }
            | BackendRequest::SetRelationshipProperty { .. }
            | BackendRequest::RemoveRelationshipProperty { .. }
            | BackendRequest::DeleteNode(_)
            | BackendRequest::DeleteRelationship(_)
            | BackendRequest::CreateIndex { .. }
            | BackendRequest::CreateUniqueConstraint { .. }
            | BackendRequest::CreateVectorIndex { .. }
            | BackendRequest::RebuildVectorIndex { .. }
            | BackendRequest::RebuildVectorIndexes
            | BackendRequest::DropIndex { .. }
            | BackendRequest::DropConstraint { .. }
            | BackendRequest::InstallIndexCatalog(_)
            | BackendRequest::InstallRoutingTable(_)
            | BackendRequest::CheckpointNow
            | BackendRequest::CompactStorage
            | BackendRequest::SnapshotNow
            | BackendRequest::RestoreSnapshot { .. }
            | BackendRequest::RepairInvariants
            | BackendRequest::PromoteCaughtUpNode(_)
            | BackendRequest::WriteBootstrapManifest { .. }
            | BackendRequest::TopologyReconcile { .. }
            | BackendRequest::QueryWriteShard { .. }
            | BackendRequest::QueryWriteBatchShard { .. }
    )
}

#[derive(Clone, Debug, PartialEq)]
pub enum BackendResponse {
    OkPong,
    OkBye,
    OkNode(u64),
    OkRelationship(u64),
    OkUnit,
    OkRows { count: usize, debug_rows: String },
    OkQueryPeers(String),
    OkReplicationPeers(String),
    OkGossip(String),
    OkReplicationPeerStatus(String),
    OkReplicationStatus(String),
    OkRoutingTable(String),
    OkClusterRegistry(String),
    OkCapabilities(String),
    OkCatchUp(String),
    OkCatchUpPlan(String),
    OkTransactionDecisions(String),
    OkTransactionRecovery(usize),
    OkClusterStatus(String),
    OkIndexCatalog(String),
    OkVectorIndexStatus(String),
    OkQueryPlan(String),
    OkQueryProfile(String),
    OkStorageStatus(String),
    OkStatistics(String),
    OkStorageMaintenance(String),
    OkMetadataLog(String),
    OkClusterNodes(String),
    OkRebalancePlan(String),
    OkRebalanceExecution(String),
    OkClusterManagementStatus(String),
    OkBootstrapManifest(String),
    OkTopologyObservation(String),
    OkOperationalSafety(String),
    OkChaosChecks(String),
    Redirect(BackendRedirect),
    Err(String),
}

#[derive(Clone, Debug, PartialEq)]
pub struct BackendRedirect {
    pub kind: RedirectKind,
    pub shard_id: ShardId,
    pub target_server_id: Option<u64>,
    pub address: Option<String>,
    pub routing_version: u64,
    pub database: String,
    pub retryable: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[allow(dead_code)]
pub enum RedirectKind {
    Moved,
    NotLeader,
    StaleRouting,
}

pub fn parse_request(line: &str) -> Result<BackendRequest, String> {
    let Some((command, rest)) = split_once_tab(line) else {
        return parse_zero_arg_request(line);
    };
    match command {
        "QUERY" => {
            if rest.is_empty() {
                Err("QUERY requires a cypher string".to_string())
            } else {
                let (query, params) = parse_query_payload(rest)?;
                Ok(BackendRequest::Query { query, params })
            }
        }
        "QUERY_PLAN" => {
            if rest.is_empty() {
                Err("QUERY_PLAN requires a cypher string".to_string())
            } else {
                let (query, params) = parse_query_payload(rest)?;
                Ok(BackendRequest::QueryPlan { query, params })
            }
        }
        "PROFILE" => {
            if rest.is_empty() {
                Err("PROFILE requires a cypher string".to_string())
            } else {
                let (query, params) = parse_query_payload(rest)?;
                Ok(BackendRequest::Profile { query, params })
            }
        }
        "QUERY_SHARD" => {
            let (shard_id, payload) = split_once_tab(rest)
                .ok_or_else(|| "QUERY_SHARD requires shard id and cypher string".to_string())?;
            let shard_id = shard_id
                .parse::<ShardId>()
                .map_err(|_| "QUERY_SHARD shard id must be an unsigned integer".to_string())?;
            if payload.is_empty() {
                return Err("QUERY_SHARD requires a cypher string".to_string());
            }
            let (query, params) = parse_query_payload(payload)?;
            Ok(BackendRequest::QueryShard {
                shard_id,
                query,
                params,
            })
        }
        "QUERY_STAGED_SHARD" => {
            let (shard_id, payload) = split_once_tab(rest).ok_or_else(|| {
                "QUERY_STAGED_SHARD requires shard id and encoded query batch".to_string()
            })?;
            let shard_id = shard_id.parse::<ShardId>().map_err(|_| {
                "QUERY_STAGED_SHARD shard id must be an unsigned integer".to_string()
            })?;
            let mut batch = decode_query_batch_payload(payload)?;
            if batch.is_empty() {
                return Err("QUERY_STAGED_SHARD requires read query entry".to_string());
            }
            let (query, params) = batch.remove(0);
            Ok(BackendRequest::QueryStagedShard {
                shard_id,
                query,
                params,
                staged_writes: batch,
            })
        }
        "QUERY_WRITE_SHARD" => {
            let (shard_id, payload) = split_once_tab(rest).ok_or_else(|| {
                "QUERY_WRITE_SHARD requires shard id and cypher string".to_string()
            })?;
            let shard_id = shard_id.parse::<ShardId>().map_err(|_| {
                "QUERY_WRITE_SHARD shard id must be an unsigned integer".to_string()
            })?;
            if payload.is_empty() {
                return Err("QUERY_WRITE_SHARD requires a cypher string".to_string());
            }
            let (query, params) = parse_query_payload(payload)?;
            Ok(BackendRequest::QueryWriteShard {
                shard_id,
                query,
                params,
            })
        }
        "QUERY_WRITE_BATCH_SHARD" => {
            let (shard_id, payload) = split_once_tab(rest).ok_or_else(|| {
                "QUERY_WRITE_BATCH_SHARD requires shard id and encoded write batch".to_string()
            })?;
            let shard_id = shard_id.parse::<ShardId>().map_err(|_| {
                "QUERY_WRITE_BATCH_SHARD shard id must be an unsigned integer".to_string()
            })?;
            Ok(BackendRequest::QueryWriteBatchShard {
                shard_id,
                writes: decode_query_batch_payload(payload)?,
            })
        }
        "QUERY_DISTRIBUTED" => {
            if rest.is_empty() {
                Err("QUERY_DISTRIBUTED requires a cypher string".to_string())
            } else {
                let (query, params) = parse_query_payload(rest)?;
                Ok(BackendRequest::QueryDistributed { query, params })
            }
        }
        "REGISTER_QUERY_PEER" => {
            let mut parts = rest.split('\t');
            let request = BackendRequest::RegisterQueryPeer {
                server_id: parse_u64(parts.next(), "REGISTER_QUERY_PEER requires server id")?,
                address: parse_address(parts.next(), "REGISTER_QUERY_PEER requires address")?,
            };
            if parts.next().is_some() {
                return Err("REGISTER_QUERY_PEER got extra fields".to_string());
            }
            Ok(request)
        }
        "UNREGISTER_QUERY_PEER" => Ok(BackendRequest::UnregisterQueryPeer(parse_single_id(
            rest,
            "UNREGISTER_QUERY_PEER requires server id",
        )?)),
        "REGISTER_REPLICATION_PEER" => {
            let mut parts = rest.split('\t');
            let server_id =
                parse_u64(parts.next(), "REGISTER_REPLICATION_PEER requires server id")?;
            let address =
                parse_address(parts.next(), "REGISTER_REPLICATION_PEER requires address")?;
            let node_id = match parts.next() {
                Some(value) if !value.trim().is_empty() => Some(parse_u64(
                    Some(value),
                    "REGISTER_REPLICATION_PEER node id must be numeric",
                )?),
                _ => None,
            };
            let transport = match parts.next() {
                Some(value) if !value.trim().is_empty() => {
                    Some(parse_replication_channel_kind(value)?)
                }
                _ => None,
            };
            let request = BackendRequest::RegisterReplicationPeer {
                server_id,
                address,
                node_id,
                transport,
            };
            if parts.next().is_some() {
                return Err("REGISTER_REPLICATION_PEER got extra fields".to_string());
            }
            Ok(request)
        }
        "NEGOTIATE_REPLICATION_PEER" => {
            let mut parts = rest.split('\t');
            let server_id = parse_u64(
                parts.next(),
                "NEGOTIATE_REPLICATION_PEER requires server id",
            )?;
            let address =
                parse_address(parts.next(), "NEGOTIATE_REPLICATION_PEER requires address")?;
            let node_id = match parts.next() {
                Some(value) if !value.trim().is_empty() => Some(parse_u64(
                    Some(value),
                    "NEGOTIATE_REPLICATION_PEER node id must be numeric",
                )?),
                _ => None,
            };
            if parts.next().is_some() {
                return Err("NEGOTIATE_REPLICATION_PEER got extra fields".to_string());
            }
            Ok(BackendRequest::NegotiateReplicationPeer {
                server_id,
                address,
                node_id,
            })
        }
        "UNREGISTER_REPLICATION_PEER" => Ok(BackendRequest::UnregisterReplicationPeer(
            parse_single_id(rest, "UNREGISTER_REPLICATION_PEER requires server id")?,
        )),
        "GOSSIP_NODE" => parse_gossip_node_request(rest),
        "REPLICATION_PEER_STATUS" => Ok(BackendRequest::ReplicationPeerStatus {
            server_id: Some(parse_single_id(
                rest,
                "REPLICATION_PEER_STATUS requires server id",
            )?),
        }),
        "CATCH_UP_FROM_PRIMARIES" => {
            let max_entries_per_request = rest.trim().parse::<usize>().map_err(|_| {
                "CATCH_UP_FROM_PRIMARIES max entries must be an unsigned integer".to_string()
            })?;
            if max_entries_per_request == 0 {
                return Err(
                    "CATCH_UP_FROM_PRIMARIES max entries must be greater than zero".to_string(),
                );
            }
            Ok(BackendRequest::CatchUpFromPrimaries {
                max_entries_per_request: Some(max_entries_per_request),
            })
        }
        "CATCH_UP_FROM_PRIMARY" => {
            let mut parts = rest.split('\t');
            let server_id = parse_u64(parts.next(), "CATCH_UP_FROM_PRIMARY requires server id")?;
            let max_entries_per_request = match parts.next() {
                Some(value) => {
                    let value = value.parse::<usize>().map_err(|_| {
                        "CATCH_UP_FROM_PRIMARY max entries must be an unsigned integer".to_string()
                    })?;
                    if value == 0 {
                        return Err(
                            "CATCH_UP_FROM_PRIMARY max entries must be greater than zero"
                                .to_string(),
                        );
                    }
                    Some(value)
                }
                None => None,
            };
            if parts.next().is_some() {
                return Err("CATCH_UP_FROM_PRIMARY got extra fields".to_string());
            }
            Ok(BackendRequest::CatchUpFromPrimary {
                server_id,
                max_entries_per_request,
            })
        }
        "CATCH_UP_PLAN_PRIMARY" => Ok(BackendRequest::CatchUpPlan {
            server_id: Some(parse_single_id(
                rest,
                "CATCH_UP_PLAN_PRIMARY requires server id",
            )?),
        }),
        "CREATE_NODE" => {
            let mut parts = rest.split('\t');
            let labels = parse_labels(
                parts
                    .next()
                    .ok_or_else(|| "CREATE_NODE requires labels".to_string())?,
            )?;
            if labels.is_empty() {
                return Err("CREATE_NODE requires labels".to_string());
            }
            Ok(BackendRequest::CreateNode {
                labels,
                properties: parse_properties(parts)?,
            })
        }
        "CREATE_NODE_SHARD" => {
            let mut parts = rest.split('\t');
            let shard_id = parse_u64(parts.next(), "CREATE_NODE_SHARD requires shard id")?;
            let labels = parse_labels(
                parts
                    .next()
                    .ok_or_else(|| "CREATE_NODE_SHARD requires labels".to_string())?,
            )?;
            if labels.is_empty() {
                return Err("CREATE_NODE_SHARD requires labels".to_string());
            }
            Ok(BackendRequest::CreateNodeOnShard {
                shard_id,
                labels,
                properties: parse_properties(parts)?,
            })
        }
        "CREATE_RELATIONSHIP" => {
            let mut parts = rest.split('\t');
            let from = parse_u64(parts.next(), "CREATE_RELATIONSHIP requires from node id")?;
            let to = parse_u64(parts.next(), "CREATE_RELATIONSHIP requires to node id")?;
            let rel_type = parts
                .next()
                .filter(|value| !value.is_empty())
                .ok_or_else(|| "CREATE_RELATIONSHIP requires relationship type".to_string())?
                .to_string();
            Ok(BackendRequest::CreateRelationship {
                from,
                to,
                rel_type,
                properties: parse_properties(parts)?,
            })
        }
        "SET_NODE_PROPERTY" => {
            let mut parts = rest.split('\t');
            let id = parse_u64(parts.next(), "SET_NODE_PROPERTY requires node id")?;
            let key = parse_key(parts.next(), "SET_NODE_PROPERTY requires property key")?;
            let value = parse_value(
                parts
                    .next()
                    .ok_or_else(|| "SET_NODE_PROPERTY requires property value".to_string())?,
            )?;
            Ok(BackendRequest::SetNodeProperty { id, key, value })
        }
        "REMOVE_NODE_PROPERTY" => {
            let mut parts = rest.split('\t');
            Ok(BackendRequest::RemoveNodeProperty {
                id: parse_u64(parts.next(), "REMOVE_NODE_PROPERTY requires node id")?,
                key: parse_key(parts.next(), "REMOVE_NODE_PROPERTY requires property key")?,
            })
        }
        "ADD_NODE_LABEL" => {
            let mut parts = rest.split('\t');
            Ok(BackendRequest::AddNodeLabel {
                id: parse_u64(parts.next(), "ADD_NODE_LABEL requires node id")?,
                label: parse_key(parts.next(), "ADD_NODE_LABEL requires label")?,
            })
        }
        "REMOVE_NODE_LABEL" => {
            let mut parts = rest.split('\t');
            Ok(BackendRequest::RemoveNodeLabel {
                id: parse_u64(parts.next(), "REMOVE_NODE_LABEL requires node id")?,
                label: parse_key(parts.next(), "REMOVE_NODE_LABEL requires label")?,
            })
        }
        "SET_RELATIONSHIP_PROPERTY" => {
            let mut parts = rest.split('\t');
            let id = parse_u64(
                parts.next(),
                "SET_RELATIONSHIP_PROPERTY requires relationship id",
            )?;
            let key = parse_key(
                parts.next(),
                "SET_RELATIONSHIP_PROPERTY requires property key",
            )?;
            let value =
                parse_value(parts.next().ok_or_else(|| {
                    "SET_RELATIONSHIP_PROPERTY requires property value".to_string()
                })?)?;
            Ok(BackendRequest::SetRelationshipProperty { id, key, value })
        }
        "REMOVE_RELATIONSHIP_PROPERTY" => {
            let mut parts = rest.split('\t');
            Ok(BackendRequest::RemoveRelationshipProperty {
                id: parse_u64(
                    parts.next(),
                    "REMOVE_RELATIONSHIP_PROPERTY requires relationship id",
                )?,
                key: parse_key(
                    parts.next(),
                    "REMOVE_RELATIONSHIP_PROPERTY requires property key",
                )?,
            })
        }
        "DELETE_NODE" => Ok(BackendRequest::DeleteNode(parse_single_id(
            rest,
            "DELETE_NODE requires node id",
        )?)),
        "DELETE_RELATIONSHIP" => Ok(BackendRequest::DeleteRelationship(parse_single_id(
            rest,
            "DELETE_RELATIONSHIP requires relationship id",
        )?)),
        "CREATE_INDEX" => {
            let mut parts = rest.split('\t');
            Ok(BackendRequest::CreateIndex {
                name: parse_key(parts.next(), "CREATE_INDEX requires index name")?,
                label: parse_key(parts.next(), "CREATE_INDEX requires label")?,
                property: parse_key(parts.next(), "CREATE_INDEX requires property")?,
                if_not_exists: parse_optional_if_not_exists(parts.next(), parts, "CREATE_INDEX")?,
            })
        }
        "CREATE_CONSTRAINT" => {
            let mut parts = rest.split('\t');
            Ok(BackendRequest::CreateUniqueConstraint {
                name: parse_key(parts.next(), "CREATE_CONSTRAINT requires constraint name")?,
                label: parse_key(parts.next(), "CREATE_CONSTRAINT requires label")?,
                property: parse_key(parts.next(), "CREATE_CONSTRAINT requires property")?,
                if_not_exists: parse_optional_if_not_exists(
                    parts.next(),
                    parts,
                    "CREATE_CONSTRAINT",
                )?,
            })
        }
        "CREATE_VECTOR_INDEX" => {
            let mut parts = rest.split('\t');
            Ok(BackendRequest::CreateVectorIndex {
                name: parse_key(parts.next(), "CREATE_VECTOR_INDEX requires index name")?,
                label: parse_key(parts.next(), "CREATE_VECTOR_INDEX requires label")?,
                property: parse_key(parts.next(), "CREATE_VECTOR_INDEX requires property")?,
                dimensions: parse_usize(parts.next(), "CREATE_VECTOR_INDEX requires dimensions")?,
                metric: parse_key(parts.next(), "CREATE_VECTOR_INDEX requires metric")?,
                if_not_exists: parse_optional_if_not_exists(
                    parts.next(),
                    parts,
                    "CREATE_VECTOR_INDEX",
                )?,
            })
        }
        "REBUILD_VECTOR_INDEX" => Ok(BackendRequest::RebuildVectorIndex {
            name: parse_single_key(rest, "REBUILD_VECTOR_INDEX requires index name")?,
        }),
        "REBUILD_VECTOR_INDEXES" => {
            Err("REBUILD_VECTOR_INDEXES does not take arguments".to_string())
        }
        "VECTOR_INDEX_STATUS" => Ok(BackendRequest::VectorIndexStatus {
            name: Some(parse_single_key(
                rest,
                "VECTOR_INDEX_STATUS requires index name",
            )?),
        }),
        "DROP_INDEX" => {
            let mut parts = rest.split('\t');
            Ok(BackendRequest::DropIndex {
                name: parse_key(parts.next(), "DROP_INDEX requires index name")?,
                if_exists: parse_optional_if_exists(parts.next(), parts, "DROP_INDEX")?,
            })
        }
        "DROP_CONSTRAINT" => {
            let mut parts = rest.split('\t');
            Ok(BackendRequest::DropConstraint {
                name: parse_key(parts.next(), "DROP_CONSTRAINT requires constraint name")?,
                if_exists: parse_optional_if_exists(parts.next(), parts, "DROP_CONSTRAINT")?,
            })
        }
        "DUMP_INDEX_CATALOG" => Err("DUMP_INDEX_CATALOG does not take arguments".to_string()),
        "INSTALL_INDEX_CATALOG" => Ok(BackendRequest::InstallIndexCatalog(decode_index_catalog(
            rest,
        )?)),
        "SYNC_INDEX_CATALOG_FROM_PEER" => Ok(BackendRequest::SyncIndexCatalogFromPeer(
            parse_single_id(rest, "SYNC_INDEX_CATALOG_FROM_PEER requires server id")?,
        )),
        "INSTALL_ROUTING_TABLE" => Ok(BackendRequest::InstallRoutingTable(
            parse_install_routing_table(rest)?,
        )),
        "RESTORE_SNAPSHOT" => Ok(BackendRequest::RestoreSnapshot {
            shard_id: parse_single_id(rest, "RESTORE_SNAPSHOT requires shard id")?,
        }),
        "RAFT_LEADER_TRANSFER" => {
            let mut parts = rest.split('\t');
            let shard_id = parse_u64(parts.next(), "RAFT_LEADER_TRANSFER requires shard id")?;
            let transferee_id =
                parse_u64(parts.next(), "RAFT_LEADER_TRANSFER requires transferee id")?;
            if parts.next().is_some() {
                return Err("RAFT_LEADER_TRANSFER got extra fields".to_string());
            }
            Ok(BackendRequest::RaftLeaderTransfer {
                shard_id,
                transferee_id,
            })
        }
        "VERIFY_INVARIANTS" => Err("VERIFY_INVARIANTS does not take arguments".to_string()),
        "REPAIR_INVARIANTS" => Err("REPAIR_INVARIANTS does not take arguments".to_string()),
        "REGISTER_NODE" => {
            let mut parts = rest.split('\t');
            let server_id = parse_u64(parts.next(), "REGISTER_NODE requires server id")?;
            let address = parse_address(parts.next(), "REGISTER_NODE requires address")?;
            if parts.next().is_some() {
                return Err("REGISTER_NODE got extra fields".to_string());
            }
            Ok(BackendRequest::RegisterNode { server_id, address })
        }
        "JOIN_REQUEST" => {
            let mut parts = rest.split('\t');
            let server_id = parse_u64(parts.next(), "JOIN_REQUEST requires server id")?;
            let address = parse_address(parts.next(), "JOIN_REQUEST requires address")?;
            let protocol_version =
                parse_u64(parts.next(), "JOIN_REQUEST requires protocol version")?;
            let storage_version = parse_u64(parts.next(), "JOIN_REQUEST requires storage version")?;
            let shard_count = parse_u64(parts.next(), "JOIN_REQUEST requires shard count")?;
            if parts.next().is_some() {
                return Err("JOIN_REQUEST got extra fields".to_string());
            }
            Ok(BackendRequest::JoinRequest {
                server_id,
                address,
                protocol_version,
                storage_version,
                shard_count,
            })
        }
        "JOIN_ACCEPT" => Ok(BackendRequest::JoinAccept(parse_single_id(
            rest,
            "JOIN_ACCEPT requires server id",
        )?)),
        "JOIN_REJECT" => {
            let (server_id, reason) = split_once_tab(rest)
                .ok_or_else(|| "JOIN_REJECT requires server id and reason".to_string())?;
            Ok(BackendRequest::JoinReject {
                server_id: server_id
                    .parse::<u64>()
                    .map_err(|_| "JOIN_REJECT server id must be an unsigned integer".to_string())?,
                reason: reason.to_string(),
            })
        }
        "DECOMMISSION_NODE" => Ok(BackendRequest::DecommissionNode(parse_single_id(
            rest,
            "DECOMMISSION_NODE requires server id",
        )?)),
        "SET_METADATA_AUTHORITY" => Ok(BackendRequest::SetMetadataAuthority(parse_single_id(
            rest,
            "SET_METADATA_AUTHORITY requires server id",
        )?)),
        "SET_REBALANCE_POLICY" => {
            let mut parts = rest.split('\t');
            let replication_factor = parse_usize(
                parts.next(),
                "SET_REBALANCE_POLICY requires replication factor",
            )?;
            let max_steps_per_plan = parse_usize(
                parts.next(),
                "SET_REBALANCE_POLICY requires max steps per plan",
            )?;
            if parts.next().is_some() {
                return Err("SET_REBALANCE_POLICY got extra fields".to_string());
            }
            Ok(BackendRequest::SetRebalancePolicy {
                replication_factor,
                max_steps_per_plan,
            })
        }
        "PREPARE_REBALANCE_STEP" => Ok(BackendRequest::PrepareRebalanceStep(parse_rebalance_step(
            rest,
        )?)),
        "MARK_SHARD_CAUGHT_UP" => {
            let mut parts = rest.split('\t');
            let shard_id = parse_u64(parts.next(), "MARK_SHARD_CAUGHT_UP requires shard id")?;
            let server_id = parse_u64(parts.next(), "MARK_SHARD_CAUGHT_UP requires server id")?;
            let match_index = parse_u64(parts.next(), "MARK_SHARD_CAUGHT_UP requires match index")?;
            if parts.next().is_some() {
                return Err("MARK_SHARD_CAUGHT_UP got extra fields".to_string());
            }
            Ok(BackendRequest::MarkShardCaughtUp {
                shard_id,
                server_id,
                match_index,
            })
        }
        "TOPOLOGY_RECONCILE" => Ok(BackendRequest::TopologyReconcile {
            max_entries_per_request: parse_optional_usize(rest, "TOPOLOGY_RECONCILE limit")?,
        }),
        "PROMOTE_CAUGHT_UP_NODE" => Ok(BackendRequest::PromoteCaughtUpNode(parse_single_id(
            rest,
            "PROMOTE_CAUGHT_UP_NODE requires server id",
        )?)),
        "WRITE_BOOTSTRAP_MANIFEST" => {
            let mut parts = rest.split('\t');
            let mode = parts
                .next()
                .ok_or_else(|| "WRITE_BOOTSTRAP_MANIFEST requires mode".to_string())?
                .to_string();
            let cluster_id = parts
                .next()
                .ok_or_else(|| "WRITE_BOOTSTRAP_MANIFEST requires cluster id".to_string())?
                .to_string();
            let database_id = parts
                .next()
                .ok_or_else(|| "WRITE_BOOTSTRAP_MANIFEST requires database id".to_string())?
                .to_string();
            if parts.next().is_some() {
                return Err("WRITE_BOOTSTRAP_MANIFEST got extra fields".to_string());
            }
            Ok(BackendRequest::WriteBootstrapManifest {
                mode,
                cluster_id,
                database_id,
            })
        }
        "BOOTSTRAP_SAFETY" => {
            let mut parts = rest.split('\t');
            let expected_cluster_id = parts
                .next()
                .ok_or_else(|| "BOOTSTRAP_SAFETY requires expected cluster id".to_string())?
                .to_string();
            let force_new_cluster = parse_bool(
                parts.next(),
                "BOOTSTRAP_SAFETY requires force-new-cluster flag",
            )?;
            if parts.next().is_some() {
                return Err("BOOTSTRAP_SAFETY got extra fields".to_string());
            }
            Ok(BackendRequest::BootstrapSafety {
                expected_cluster_id,
                force_new_cluster,
            })
        }
        "OPERATIONAL_SAFETY" => {
            let mut parts = rest.split('\t');
            let operation = parts
                .next()
                .ok_or_else(|| "OPERATIONAL_SAFETY requires operation".to_string())?
                .to_string();
            let confirmation = parts.next().map(str::to_string);
            if parts.next().is_some() {
                return Err("OPERATIONAL_SAFETY got extra fields".to_string());
            }
            Ok(BackendRequest::OperationalSafety {
                operation,
                confirmation,
            })
        }
        "APPLY_REBALANCE_STEP" => Ok(BackendRequest::ApplyRebalanceStep(parse_rebalance_step(
            rest,
        )?)),
        _ => Err(format!("unknown command: {command}")),
    }
}

fn parse_bool(value: Option<&str>, message: &str) -> Result<bool, String> {
    match value.ok_or_else(|| message.to_string())? {
        "1" | "true" | "TRUE" | "yes" | "YES" => Ok(true),
        "0" | "false" | "FALSE" | "no" | "NO" => Ok(false),
        _ => Err(format!("{message}: expected true or false")),
    }
}

fn parse_optional_usize(rest: &str, message: &str) -> Result<Option<usize>, String> {
    let value = rest.trim();
    if value.is_empty() {
        return Ok(None);
    }
    let parsed = value
        .parse::<usize>()
        .map_err(|_| format!("{message} must be a positive integer"))?;
    if parsed == 0 {
        return Err(format!("{message} must be greater than zero"));
    }
    Ok(Some(parsed))
}

fn parse_replication_channel_kind(input: &str) -> Result<ReplicationChannelKind, String> {
    match input.to_ascii_lowercase().as_str() {
        "tcp" => Ok(ReplicationChannelKind::Tcp),
        "udp" => Ok(ReplicationChannelKind::Udp),
        "rdma" => Ok(ReplicationChannelKind::Rdma),
        "custom" => Ok(ReplicationChannelKind::Custom),
        other => Err(format!("unsupported replication transport {other:?}")),
    }
}

mod execute;
mod format;
mod parse_helpers;
mod row_codec;

pub use execute::{format_response, write_response};
pub(crate) use format::*;
use parse_helpers::*;
pub use parse_helpers::{decode_index_catalog, encode_index_catalog};
use row_codec::*;
pub use row_codec::{
    decode_query_batch_payload, decode_query_rows, encode_query_batch_payload, encode_query_rows,
    parse_query_payload,
};

#[cfg(test)]
mod tests;
