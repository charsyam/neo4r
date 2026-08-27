use neo4r_core::{Properties, ShardId, ShardPlacement, ShardReplica, ShardRoutingTable, Value};
use neo4r_db::{
    ClusterManagementStatus, ClusterMembership, ClusterMetadataState, ClusterStatus,
    DatabaseResult, DistributedQueryPlan, IndexCatalog, IndexDefinition, IndexKind,
    MetadataOperationRecord, Neo4rDatabaseHandle, NodeMembershipState, QueryAccessPlan,
    QueryOperatorProfile, QueryProfile, QueryRoute, RebalanceExecution, RebalancePlan,
    RebalancePlanState, RebalancePolicy, RebalanceStep, RebalanceStepState, RemoteTraversalPolicy,
    ShardAssignmentState, ShardStatus, StatisticsCatalog, StorageMaintenanceResult, StorageStatus,
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
    },
    UnregisterReplicationPeer(u64),
    ListReplicationPeers,
    ReplicationPeerStatus {
        server_id: Option<u64>,
    },
    ReplicationStatus,
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
    ApplyRebalanceStep(RebalanceStep),
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
    OkReplicationPeerStatus(String),
    OkReplicationStatus(String),
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
    Err(String),
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
            let request = BackendRequest::RegisterReplicationPeer {
                server_id: parse_u64(parts.next(), "REGISTER_REPLICATION_PEER requires server id")?,
                address: parse_address(parts.next(), "REGISTER_REPLICATION_PEER requires address")?,
            };
            if parts.next().is_some() {
                return Err("REGISTER_REPLICATION_PEER got extra fields".to_string());
            }
            Ok(request)
        }
        "UNREGISTER_REPLICATION_PEER" => Ok(BackendRequest::UnregisterReplicationPeer(
            parse_single_id(rest, "UNREGISTER_REPLICATION_PEER requires server id")?,
        )),
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
        "APPLY_REBALANCE_STEP" => Ok(BackendRequest::ApplyRebalanceStep(parse_rebalance_step(
            rest,
        )?)),
        _ => Err(format!("unknown command: {command}")),
    }
}

pub fn execute_request(db: &Neo4rDatabaseHandle, request: BackendRequest) -> BackendResponse {
    match execute_request_inner(db, request) {
        Ok(response) => response,
        Err(err) => BackendResponse::Err(err.to_string()),
    }
}

pub fn write_response(writer: &mut impl Write, response: &BackendResponse) -> io::Result<()> {
    writeln!(writer, "{}", format_response(response))
}

pub fn format_response(response: &BackendResponse) -> String {
    match response {
        BackendResponse::OkPong => "OK\tPONG".to_string(),
        BackendResponse::OkBye => "OK\tBYE".to_string(),
        BackendResponse::OkNode(id) => format!("OK\tNODE\t{id}"),
        BackendResponse::OkRelationship(id) => format!("OK\tRELATIONSHIP\t{id}"),
        BackendResponse::OkUnit => "OK".to_string(),
        BackendResponse::OkRows { count, debug_rows } => {
            format!("OK\tROWS\t{count}\t{}", escape_response(debug_rows))
        }
        BackendResponse::OkQueryPeers(peers) => {
            format!("OK\tQUERY_PEERS\t{}", escape_response(peers))
        }
        BackendResponse::OkReplicationPeers(peers) => {
            format!("OK\tREPLICATION_PEERS\t{}", escape_response(peers))
        }
        BackendResponse::OkReplicationPeerStatus(status) => {
            format!("OK\tREPLICATION_PEER_STATUS\t{}", escape_response(status))
        }
        BackendResponse::OkReplicationStatus(status) => {
            format!("OK\tREPLICATION_STATUS\t{}", escape_response(status))
        }
        BackendResponse::OkCatchUp(results) => {
            format!("OK\tCATCH_UP\t{}", escape_response(results))
        }
        BackendResponse::OkCatchUpPlan(plan) => {
            format!("OK\tCATCH_UP_PLAN\t{}", escape_response(plan))
        }
        BackendResponse::OkTransactionDecisions(decisions) => {
            format!("OK\tTX_DECISIONS\t{}", escape_response(decisions))
        }
        BackendResponse::OkTransactionRecovery(count) => {
            format!("OK\tTX_RECOVERY\t{count}")
        }
        BackendResponse::OkClusterStatus(status) => {
            format!("OK\tCLUSTER_STATUS\t{}", escape_response(status))
        }
        BackendResponse::OkIndexCatalog(catalog) => {
            format!("OK\tINDEX_CATALOG\t{}", escape_response(catalog))
        }
        BackendResponse::OkVectorIndexStatus(status) => {
            format!("OK\tVECTOR_INDEX_STATUS\t{}", escape_response(status))
        }
        BackendResponse::OkQueryPlan(plan) => {
            format!("OK\tQUERY_PLAN\t{}", escape_response(plan))
        }
        BackendResponse::OkQueryProfile(profile) => {
            format!("OK\tPROFILE\t{}", escape_response(profile))
        }
        BackendResponse::OkStorageStatus(status) => {
            format!("OK\tSTORAGE_STATUS\t{}", escape_response(status))
        }
        BackendResponse::OkStatistics(statistics) => {
            format!("OK\tSTATISTICS\t{}", escape_response(statistics))
        }
        BackendResponse::OkStorageMaintenance(result) => {
            format!("OK\tSTORAGE_MAINTENANCE\t{}", escape_response(result))
        }
        BackendResponse::OkMetadataLog(log) => {
            format!("OK\tMETADATA_LOG\t{}", escape_response(log))
        }
        BackendResponse::OkClusterNodes(nodes) => {
            format!("OK\tCLUSTER_NODES\t{}", escape_response(nodes))
        }
        BackendResponse::OkRebalancePlan(plan) => {
            format!("OK\tREBALANCE_PLAN\t{}", escape_response(plan))
        }
        BackendResponse::OkRebalanceExecution(execution) => {
            format!("OK\tREBALANCE_EXECUTION\t{}", escape_response(execution))
        }
        BackendResponse::OkClusterManagementStatus(status) => {
            format!("OK\tCLUSTER_MANAGEMENT\t{}", escape_response(status))
        }
        BackendResponse::Err(message) => format!("ERR\t{}", escape_response(message)),
    }
}

fn execute_request_inner(
    db: &Neo4rDatabaseHandle,
    request: BackendRequest,
) -> DatabaseResult<BackendResponse> {
    match request {
        BackendRequest::Ping => Ok(BackendResponse::OkPong),
        BackendRequest::Quit => Ok(BackendResponse::OkBye),
        BackendRequest::Query { query, params } => {
            let rows = if params.is_empty() {
                db.execute_cypher(&query)?
            } else {
                db.execute_cypher_with_params(&query, params)?
            };
            Ok(BackendResponse::OkRows {
                count: rows.len(),
                debug_rows: encode_query_rows(&rows),
            })
        }
        BackendRequest::QueryPlan { query, params } => Ok(BackendResponse::OkQueryPlan(
            format_query_plan(&db.query_plan_with_params(&query, params)?),
        )),
        BackendRequest::Profile { query, params } => Ok(BackendResponse::OkQueryProfile(
            format_query_profile(&db.profile_query(&query, params)?),
        )),
        BackendRequest::QueryShard {
            shard_id,
            query,
            params,
        } => {
            let rows = db.query_shard_with_params(shard_id, &query, params)?;
            Ok(BackendResponse::OkRows {
                count: rows.len(),
                debug_rows: encode_query_rows(&rows),
            })
        }
        BackendRequest::QueryStagedShard {
            shard_id,
            query,
            params,
            staged_writes,
        } => {
            let rows = db.query_shard_with_staged_writes(
                shard_id,
                &query,
                params,
                Default::default(),
                &staged_writes,
            )?;
            Ok(BackendResponse::OkRows {
                count: rows.len(),
                debug_rows: encode_query_rows(&rows),
            })
        }
        BackendRequest::QueryWriteShard {
            shard_id,
            query,
            params,
        } => {
            let rows = db.execute_cypher_on_shard(shard_id, &query, params)?;
            Ok(BackendResponse::OkRows {
                count: rows.len(),
                debug_rows: encode_query_rows(&rows),
            })
        }
        BackendRequest::QueryWriteBatchShard { shard_id, writes } => {
            db.execute_cypher_mutation_batch_on_shard(shard_id, writes)?;
            Ok(BackendResponse::OkRows {
                count: 0,
                debug_rows: String::new(),
            })
        }
        BackendRequest::QueryDistributed { .. } => Ok(BackendResponse::Err(
            "QUERY_DISTRIBUTED requires a backend coordinator".to_string(),
        )),
        BackendRequest::RegisterQueryPeer { .. }
        | BackendRequest::UnregisterQueryPeer(_)
        | BackendRequest::ListQueryPeers
        | BackendRequest::RegisterReplicationPeer { .. }
        | BackendRequest::UnregisterReplicationPeer(_)
        | BackendRequest::ListReplicationPeers
        | BackendRequest::ReplicationPeerStatus { .. }
        | BackendRequest::ReplicationStatus
        | BackendRequest::CatchUpFromPrimaries { .. }
        | BackendRequest::CatchUpFromPrimary { .. }
        | BackendRequest::CatchUpPlan { .. } => Ok(BackendResponse::Err(
            "peer management and catch-up require a backend coordinator".to_string(),
        )),
        BackendRequest::CreateNode { labels, properties } => {
            Ok(BackendResponse::OkNode(db.create_node(labels, properties)?))
        }
        BackendRequest::CreateNodeOnShard {
            shard_id,
            labels,
            properties,
        } => Ok(BackendResponse::OkNode(
            db.create_node_on_shard(shard_id, labels, properties)?,
        )),
        BackendRequest::CreateRelationship {
            from,
            to,
            rel_type,
            properties,
        } => Ok(BackendResponse::OkRelationship(
            db.create_relationship(from, to, rel_type, properties)?,
        )),
        BackendRequest::SetNodeProperty { id, key, value } => {
            db.set_node_property(id, key, value)?;
            Ok(BackendResponse::OkUnit)
        }
        BackendRequest::RemoveNodeProperty { id, key } => {
            db.remove_node_property(id, key)?;
            Ok(BackendResponse::OkUnit)
        }
        BackendRequest::AddNodeLabel { id, label } => {
            db.add_node_label(id, label)?;
            Ok(BackendResponse::OkUnit)
        }
        BackendRequest::RemoveNodeLabel { id, label } => {
            db.remove_node_label(id, label)?;
            Ok(BackendResponse::OkUnit)
        }
        BackendRequest::SetRelationshipProperty { id, key, value } => {
            db.set_relationship_property(id, key, value)?;
            Ok(BackendResponse::OkUnit)
        }
        BackendRequest::RemoveRelationshipProperty { id, key } => {
            db.remove_relationship_property(id, key)?;
            Ok(BackendResponse::OkUnit)
        }
        BackendRequest::DeleteNode(id) => {
            db.delete_node(id)?;
            Ok(BackendResponse::OkUnit)
        }
        BackendRequest::DeleteRelationship(id) => {
            db.delete_relationship(id)?;
            Ok(BackendResponse::OkUnit)
        }
        BackendRequest::CreateIndex {
            name,
            label,
            property,
            if_not_exists,
        } => {
            if if_not_exists {
                db.create_node_property_index_if_not_exists(name, label, property)?;
            } else {
                db.create_node_property_index(name, label, property)?;
            }
            Ok(BackendResponse::OkUnit)
        }
        BackendRequest::CreateUniqueConstraint {
            name,
            label,
            property,
            if_not_exists,
        } => {
            if if_not_exists {
                db.create_unique_node_property_constraint_if_not_exists(name, label, property)?;
            } else {
                db.create_unique_node_property_constraint(name, label, property)?;
            }
            Ok(BackendResponse::OkUnit)
        }
        BackendRequest::CreateVectorIndex {
            name,
            label,
            property,
            dimensions,
            metric,
            if_not_exists,
        } => {
            if if_not_exists {
                db.create_vector_index_if_not_exists(name, label, property, dimensions, metric)?;
            } else {
                db.create_vector_index(name, label, property, dimensions, metric)?;
            }
            Ok(BackendResponse::OkUnit)
        }
        BackendRequest::RebuildVectorIndexes => {
            db.rebuild_vector_indexes()?;
            Ok(BackendResponse::OkUnit)
        }
        BackendRequest::RebuildVectorIndex { name } => {
            db.rebuild_vector_index(&name)?;
            Ok(BackendResponse::OkUnit)
        }
        BackendRequest::VectorIndexStatus { name } => {
            let statuses = match name {
                Some(name) => vec![db.vector_index_status_by_name(&name)?],
                None => db.vector_index_status()?,
            };
            Ok(BackendResponse::OkVectorIndexStatus(
                format_vector_index_status(&statuses),
            ))
        }
        BackendRequest::DropIndex { name, if_exists } => {
            if if_exists {
                db.drop_index_if_exists(&name)?;
            } else {
                db.drop_index(&name)?;
            }
            Ok(BackendResponse::OkUnit)
        }
        BackendRequest::DropConstraint { name, if_exists } => {
            if if_exists {
                db.drop_constraint_if_exists(&name)?;
            } else {
                db.drop_constraint(&name)?;
            }
            Ok(BackendResponse::OkUnit)
        }
        BackendRequest::ListIndexes => {
            let indexes = db.list_indexes()?;
            Ok(BackendResponse::OkRows {
                count: indexes.len(),
                debug_rows: format!("{indexes:?}"),
            })
        }
        BackendRequest::DumpIndexCatalog => Ok(BackendResponse::OkIndexCatalog(
            encode_index_catalog(&db.index_catalog()?),
        )),
        BackendRequest::InstallIndexCatalog(catalog) => {
            db.install_index_catalog(catalog)?;
            Ok(BackendResponse::OkUnit)
        }
        BackendRequest::SyncIndexCatalogFromPeer(_) => Ok(BackendResponse::Err(
            "index catalog peer sync requires a backend coordinator".to_string(),
        )),
        BackendRequest::ListTransactionDecisions => Ok(BackendResponse::Err(
            "transaction decision listing requires a backend coordinator".to_string(),
        )),
        BackendRequest::RecoverTransactionDecisions => Ok(BackendResponse::Err(
            "transaction decision recovery requires a backend coordinator".to_string(),
        )),
        BackendRequest::InstallRoutingTable(routing_table) => {
            db.install_routing_table(routing_table)?;
            Ok(BackendResponse::OkUnit)
        }
        BackendRequest::ClusterStatus => Ok(BackendResponse::OkClusterStatus(
            format_cluster_status(&db.cluster_status()?),
        )),
        BackendRequest::StorageStatus => Ok(BackendResponse::OkStorageStatus(
            format_storage_status(&db.storage_status()?),
        )),
        BackendRequest::Statistics => Ok(BackendResponse::OkStatistics(format_statistics_catalog(
            &db.statistics_catalog()?,
        ))),
        BackendRequest::CheckpointNow => Ok(BackendResponse::OkStorageMaintenance(
            format_storage_maintenance(&db.checkpoint_now()?),
        )),
        BackendRequest::CompactStorage => Ok(BackendResponse::OkStorageMaintenance(
            format_storage_maintenance(&db.compact_storage()?),
        )),
        BackendRequest::MetadataLog => Ok(BackendResponse::OkMetadataLog(format_metadata_log(
            &db.metadata_operations()?,
        ))),
        BackendRequest::RegisterNode { server_id, address } => Ok(BackendResponse::OkClusterNodes(
            format_cluster_membership(&db.register_cluster_node(server_id, address)?),
        )),
        BackendRequest::JoinRequest {
            server_id,
            address,
            protocol_version,
            storage_version,
            shard_count,
        } => Ok(BackendResponse::OkClusterNodes(format_cluster_membership(
            &db.request_cluster_join(
                server_id,
                address,
                protocol_version,
                storage_version,
                shard_count,
            )?,
        ))),
        BackendRequest::JoinAccept(server_id) => Ok(BackendResponse::OkClusterNodes(
            format_cluster_membership(&db.accept_cluster_join(server_id)?),
        )),
        BackendRequest::JoinReject { server_id, reason } => Ok(BackendResponse::OkClusterNodes(
            format_cluster_membership(&db.reject_cluster_join(server_id, reason)?),
        )),
        BackendRequest::DecommissionNode(server_id) => Ok(BackendResponse::OkClusterNodes(
            format_cluster_membership(&db.decommission_cluster_node(server_id)?),
        )),
        BackendRequest::ListNodes => Ok(BackendResponse::OkClusterNodes(
            format_cluster_membership(&db.cluster_membership()?),
        )),
        BackendRequest::MetadataAuthority => Ok(BackendResponse::OkClusterManagementStatus(
            format_cluster_metadata(&db.cluster_metadata()?),
        )),
        BackendRequest::SetMetadataAuthority(server_id) => {
            Ok(BackendResponse::OkClusterManagementStatus(
                format_cluster_metadata(&db.set_metadata_authority(server_id)?),
            ))
        }
        BackendRequest::SetRebalancePolicy {
            replication_factor,
            max_steps_per_plan,
        } => Ok(BackendResponse::OkClusterManagementStatus(
            format_cluster_metadata(&db.set_rebalance_policy(RebalancePolicy {
                replication_factor,
                max_steps_per_plan,
            })?),
        )),
        BackendRequest::PlanRebalance => Ok(BackendResponse::OkRebalancePlan(
            format_rebalance_plan(&db.plan_rebalance()?),
        )),
        BackendRequest::StartRebalance => Ok(BackendResponse::OkRebalanceExecution(
            format_rebalance_execution(&db.start_rebalance_plan()?),
        )),
        BackendRequest::CancelRebalance => Ok(BackendResponse::OkRebalanceExecution(
            format_rebalance_execution(&db.cancel_rebalance_plan()?),
        )),
        BackendRequest::RebalanceStatus => Ok(BackendResponse::OkRebalanceExecution(
            db.rebalance_status()?
                .map(|execution| format_rebalance_execution(&execution))
                .unwrap_or_else(|| "none".to_string()),
        )),
        BackendRequest::AdvanceRebalance => {
            let result = db.advance_rebalance()?;
            Ok(BackendResponse::OkRebalanceExecution(format!(
                "action={} {}",
                result.action,
                format_rebalance_execution(&result.execution)
            )))
        }
        BackendRequest::ClusterManagementStatus => Ok(BackendResponse::OkClusterManagementStatus(
            format_cluster_management_status(&db.cluster_management_status()?),
        )),
        BackendRequest::PrepareRebalanceStep(step) => Ok(BackendResponse::OkClusterNodes(
            format_cluster_membership(&db.prepare_rebalance_step(step)?),
        )),
        BackendRequest::MarkShardCaughtUp {
            shard_id,
            server_id,
            match_index,
        } => Ok(BackendResponse::OkClusterNodes(format_cluster_membership(
            &db.mark_shard_caught_up(shard_id, server_id, match_index)?,
        ))),
        BackendRequest::ApplyRebalanceStep(step) => {
            let routing_table = db.apply_rebalance_step(step)?;
            Ok(BackendResponse::OkClusterStatus(format!(
                "routing_version={}",
                routing_table.version
            )))
        }
    }
}

fn parse_zero_arg_request(line: &str) -> Result<BackendRequest, String> {
    match line {
        "PING" => Ok(BackendRequest::Ping),
        "QUIT" => Ok(BackendRequest::Quit),
        "QUERY" => Err("QUERY requires a cypher string".to_string()),
        "QUERY_PLAN" => Err("QUERY_PLAN requires a cypher string".to_string()),
        "PROFILE" => Err("PROFILE requires a cypher string".to_string()),
        "QUERY_SHARD" => Err("QUERY_SHARD requires shard id and cypher string".to_string()),
        "QUERY_WRITE_SHARD" => {
            Err("QUERY_WRITE_SHARD requires shard id and cypher string".to_string())
        }
        "QUERY_DISTRIBUTED" => Err("QUERY_DISTRIBUTED requires a cypher string".to_string()),
        "REGISTER_QUERY_PEER" => Err("REGISTER_QUERY_PEER requires server id".to_string()),
        "UNREGISTER_QUERY_PEER" => Err("UNREGISTER_QUERY_PEER requires server id".to_string()),
        "LIST_QUERY_PEERS" => Ok(BackendRequest::ListQueryPeers),
        "REGISTER_REPLICATION_PEER" => {
            Err("REGISTER_REPLICATION_PEER requires server id".to_string())
        }
        "UNREGISTER_REPLICATION_PEER" => {
            Err("UNREGISTER_REPLICATION_PEER requires server id".to_string())
        }
        "LIST_REPLICATION_PEERS" => Ok(BackendRequest::ListReplicationPeers),
        "REPLICATION_PEER_STATUS" => Ok(BackendRequest::ReplicationPeerStatus { server_id: None }),
        "REPLICATION_STATUS" => Ok(BackendRequest::ReplicationStatus),
        "CATCH_UP_FROM_PRIMARIES" => Ok(BackendRequest::CatchUpFromPrimaries {
            max_entries_per_request: None,
        }),
        "CATCH_UP_FROM_PRIMARY" => Err("CATCH_UP_FROM_PRIMARY requires server id".to_string()),
        "CATCH_UP_PLAN" => Ok(BackendRequest::CatchUpPlan { server_id: None }),
        "CATCH_UP_PLAN_PRIMARY" => Err("CATCH_UP_PLAN_PRIMARY requires server id".to_string()),
        "LIST_TX_DECISIONS" => Ok(BackendRequest::ListTransactionDecisions),
        "RECOVER_TX_DECISIONS" => Ok(BackendRequest::RecoverTransactionDecisions),
        "CREATE_NODE" => Err("CREATE_NODE requires labels".to_string()),
        "CREATE_NODE_SHARD" => Err("CREATE_NODE_SHARD requires shard id".to_string()),
        "CREATE_RELATIONSHIP" => Err("CREATE_RELATIONSHIP requires from node id".to_string()),
        "SET_NODE_PROPERTY" => Err("SET_NODE_PROPERTY requires node id".to_string()),
        "REMOVE_NODE_PROPERTY" => Err("REMOVE_NODE_PROPERTY requires node id".to_string()),
        "ADD_NODE_LABEL" => Err("ADD_NODE_LABEL requires node id".to_string()),
        "REMOVE_NODE_LABEL" => Err("REMOVE_NODE_LABEL requires node id".to_string()),
        "SET_RELATIONSHIP_PROPERTY" => {
            Err("SET_RELATIONSHIP_PROPERTY requires relationship id".to_string())
        }
        "REMOVE_RELATIONSHIP_PROPERTY" => {
            Err("REMOVE_RELATIONSHIP_PROPERTY requires relationship id".to_string())
        }
        "DELETE_NODE" => Err("DELETE_NODE requires node id".to_string()),
        "DELETE_RELATIONSHIP" => Err("DELETE_RELATIONSHIP requires relationship id".to_string()),
        "CREATE_INDEX" => Err("CREATE_INDEX requires index name".to_string()),
        "CREATE_CONSTRAINT" => Err("CREATE_CONSTRAINT requires constraint name".to_string()),
        "CREATE_VECTOR_INDEX" => Err("CREATE_VECTOR_INDEX requires index name".to_string()),
        "REBUILD_VECTOR_INDEX" => Err("REBUILD_VECTOR_INDEX requires index name".to_string()),
        "REBUILD_VECTOR_INDEXES" => Ok(BackendRequest::RebuildVectorIndexes),
        "VECTOR_INDEX_STATUS" => Ok(BackendRequest::VectorIndexStatus { name: None }),
        "DROP_INDEX" => Err("DROP_INDEX requires index name".to_string()),
        "DROP_CONSTRAINT" => Err("DROP_CONSTRAINT requires constraint name".to_string()),
        "LIST_INDEXES" => Ok(BackendRequest::ListIndexes),
        "DUMP_INDEX_CATALOG" => Ok(BackendRequest::DumpIndexCatalog),
        "INSTALL_INDEX_CATALOG" => Err("INSTALL_INDEX_CATALOG requires version".to_string()),
        "SYNC_INDEX_CATALOG_FROM_PEER" => {
            Err("SYNC_INDEX_CATALOG_FROM_PEER requires server id".to_string())
        }
        "INSTALL_ROUTING_TABLE" => Err("INSTALL_ROUTING_TABLE requires version".to_string()),
        "CLUSTER_STATUS" => Ok(BackendRequest::ClusterStatus),
        "STORAGE_STATUS" => Ok(BackendRequest::StorageStatus),
        "STATISTICS" => Ok(BackendRequest::Statistics),
        "CHECKPOINT_NOW" => Ok(BackendRequest::CheckpointNow),
        "COMPACT_STORAGE" => Ok(BackendRequest::CompactStorage),
        "METADATA_LOG" => Ok(BackendRequest::MetadataLog),
        "REGISTER_NODE" => Err("REGISTER_NODE requires server id".to_string()),
        "JOIN_REQUEST" => Err("JOIN_REQUEST requires server id".to_string()),
        "JOIN_ACCEPT" => Err("JOIN_ACCEPT requires server id".to_string()),
        "JOIN_REJECT" => Err("JOIN_REJECT requires server id".to_string()),
        "DECOMMISSION_NODE" => Err("DECOMMISSION_NODE requires server id".to_string()),
        "LIST_NODES" => Ok(BackendRequest::ListNodes),
        "METADATA_AUTHORITY" => Ok(BackendRequest::MetadataAuthority),
        "SET_METADATA_AUTHORITY" => Err("SET_METADATA_AUTHORITY requires server id".to_string()),
        "SET_REBALANCE_POLICY" => {
            Err("SET_REBALANCE_POLICY requires replication factor".to_string())
        }
        "PLAN_REBALANCE" => Ok(BackendRequest::PlanRebalance),
        "START_REBALANCE" => Ok(BackendRequest::StartRebalance),
        "CANCEL_REBALANCE" => Ok(BackendRequest::CancelRebalance),
        "REBALANCE_STATUS" => Ok(BackendRequest::RebalanceStatus),
        "ADVANCE_REBALANCE" => Ok(BackendRequest::AdvanceRebalance),
        "CLUSTER_MANAGEMENT_STATUS" => Ok(BackendRequest::ClusterManagementStatus),
        "PREPARE_REBALANCE_STEP" => Err("PREPARE_REBALANCE_STEP requires step".to_string()),
        "MARK_SHARD_CAUGHT_UP" => Err("MARK_SHARD_CAUGHT_UP requires shard id".to_string()),
        "APPLY_REBALANCE_STEP" => Err("APPLY_REBALANCE_STEP requires step".to_string()),
        "" => Err("empty request".to_string()),
        command => Err(format!("unknown command: {command}")),
    }
}

pub fn encode_index_catalog(catalog: &IndexCatalog) -> String {
    let mut fields = vec![catalog.version.to_string()];
    fields.extend(catalog.indexes.iter().map(encode_index_definition));
    fields.join(";")
}

pub fn decode_index_catalog(input: &str) -> Result<IndexCatalog, String> {
    let mut parts = input.split(';');
    let version = parse_u64(parts.next(), "index catalog requires version")?;
    let indexes = parts
        .map(decode_index_definition)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(IndexCatalog { version, indexes })
}

fn encode_index_definition(index: &IndexDefinition) -> String {
    let mut fields = vec![
        hex_encode(index.name.as_bytes()),
        hex_encode(index.label.as_bytes()),
        hex_encode(index.property.as_bytes()),
    ];
    match &index.kind {
        IndexKind::NodeProperty => fields.push("node".to_string()),
        IndexKind::UniqueNodeProperty => fields.push("unique_node".to_string()),
        IndexKind::Vector { dimensions, metric } => {
            fields.push("vector".to_string());
            fields.push(dimensions.to_string());
            fields.push(hex_encode(metric.as_bytes()));
        }
    }
    fields.join(":")
}

fn decode_index_definition(input: &str) -> Result<IndexDefinition, String> {
    let parts = input.split(':').collect::<Vec<_>>();
    if parts.len() != 4 && parts.len() != 6 {
        return Err(
            "index definition must be name:label:property:kind[:dimensions:metric]".to_string(),
        );
    }
    let name = decode_hex_string(parts[0], "index name")?;
    let label = decode_hex_string(parts[1], "index label")?;
    let property = decode_hex_string(parts[2], "index property")?;
    match parts[3] {
        "node" if parts.len() == 4 => Ok(IndexDefinition::node_property(name, label, property)),
        "unique_node" if parts.len() == 4 => {
            Ok(IndexDefinition::unique_node_property(name, label, property))
        }
        "vector" if parts.len() == 6 => Ok(IndexDefinition::vector(
            name,
            label,
            property,
            parse_u64_token(parts[4], "index vector dimensions")? as usize,
            decode_hex_string(parts[5], "index vector metric")?,
        )),
        "node" => Err("node index definition must not include vector fields".to_string()),
        "unique_node" => {
            Err("unique node index definition must not include vector fields".to_string())
        }
        "vector" => Err("vector index definition requires dimensions and metric".to_string()),
        kind => Err(format!("unknown index kind: {kind}")),
    }
}

fn decode_hex_string(input: &str, name: &str) -> Result<String, String> {
    String::from_utf8(hex_decode(input)?).map_err(|_| format!("{name} is not valid UTF-8"))
}

fn parse_install_routing_table(input: &str) -> Result<ShardRoutingTable, String> {
    let mut parts = input.split('\t');
    let version = parse_u64(parts.next(), "INSTALL_ROUTING_TABLE requires version")?;
    let placements = parts
        .map(parse_routing_placement)
        .collect::<Result<Vec<_>, _>>()?;
    if placements.is_empty() {
        return Err("INSTALL_ROUTING_TABLE requires at least one shard placement".to_string());
    }
    Ok(ShardRoutingTable {
        version,
        placements,
    })
}

fn parse_routing_placement(input: &str) -> Result<ShardPlacement, String> {
    let mut parts = input.split(':');
    let shard_id = parts
        .next()
        .ok_or_else(|| "routing placement requires shard id".to_string())?
        .parse::<u64>()
        .map_err(|_| "routing placement shard id must be an unsigned integer".to_string())?;
    let primary = parts
        .next()
        .ok_or_else(|| "routing placement requires primary server id".to_string())?
        .parse::<u64>()
        .map_err(|_| "routing placement primary must be an unsigned integer".to_string())?;
    if primary == 0 {
        return Err("routing placement primary must be greater than zero".to_string());
    }
    let replica_part = parts.next().unwrap_or("");
    if parts.next().is_some() {
        return Err("routing placement must be shard:primary:replicas".to_string());
    }
    let mut replicas = vec![ShardReplica::primary(primary)];
    if !replica_part.is_empty() {
        for replica in replica_part.split(',').filter(|value| !value.is_empty()) {
            let server_id = replica
                .parse::<u64>()
                .map_err(|_| "routing placement replica must be an unsigned integer".to_string())?;
            if server_id == 0 {
                return Err("routing placement replica must be greater than zero".to_string());
            }
            if server_id != primary
                && !replicas
                    .iter()
                    .any(|existing| existing.server_id == server_id)
            {
                replicas.push(ShardReplica::replica(server_id));
            }
        }
    }
    Ok(ShardPlacement::new(shard_id, replicas))
}

fn parse_rebalance_step(input: &str) -> Result<RebalanceStep, String> {
    let mut parts = input.split('\t');
    match parts.next().unwrap_or("") {
        "ADD_REPLICA" => {
            let shard_id = parse_u64(parts.next(), "ADD_REPLICA requires shard id")?;
            let server_id = parse_u64(parts.next(), "ADD_REPLICA requires server id")?;
            if parts.next().is_some() {
                return Err("ADD_REPLICA got extra fields".to_string());
            }
            Ok(RebalanceStep::AddReplica {
                shard_id,
                server_id,
            })
        }
        "TRANSFER_PRIMARY" => {
            let shard_id = parse_u64(parts.next(), "TRANSFER_PRIMARY requires shard id")?;
            let from = parse_u64(parts.next(), "TRANSFER_PRIMARY requires from server id")?;
            let to = parse_u64(parts.next(), "TRANSFER_PRIMARY requires to server id")?;
            if parts.next().is_some() {
                return Err("TRANSFER_PRIMARY got extra fields".to_string());
            }
            Ok(RebalanceStep::TransferPrimary { shard_id, from, to })
        }
        "REMOVE_REPLICA" => {
            let shard_id = parse_u64(parts.next(), "REMOVE_REPLICA requires shard id")?;
            let server_id = parse_u64(parts.next(), "REMOVE_REPLICA requires server id")?;
            if parts.next().is_some() {
                return Err("REMOVE_REPLICA got extra fields".to_string());
            }
            Ok(RebalanceStep::RemoveReplica {
                shard_id,
                server_id,
            })
        }
        "" => Err("APPLY_REBALANCE_STEP requires step".to_string()),
        step => Err(format!("unknown rebalance step: {step}")),
    }
}

fn format_cluster_status(status: &ClusterStatus) -> String {
    let shards = status
        .shards
        .iter()
        .map(format_shard_status)
        .collect::<Vec<_>>()
        .join(",");
    format!(
        "server={} routing_version={} shards={} partitions={} [{}]",
        status.server_id,
        status.routing_version,
        status.shard_count,
        status.local_partition_count,
        shards
    )
}

fn format_cluster_membership(membership: &ClusterMembership) -> String {
    let nodes = membership
        .nodes
        .iter()
        .map(|node| {
            format!(
                "{}:{}:{}:protocol={}:storage={}:shards={}:reason={}",
                node.server_id,
                format_node_state(node.state),
                node.address,
                node.protocol_version,
                node.storage_version,
                node.shard_count,
                node.rejection_reason
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    let assignments = membership
        .shard_assignments
        .iter()
        .map(|assignment| {
            format!(
                "shard={}:server={}:state={}:match={}",
                assignment.shard_id,
                assignment.server_id,
                format_assignment_state(assignment.state),
                assignment.match_index
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    format!(
        "version={} nodes=[{}] assignments=[{}]",
        membership.version, nodes, assignments
    )
}

fn format_node_state(state: NodeMembershipState) -> &'static str {
    match state {
        NodeMembershipState::Negotiating => "negotiating",
        NodeMembershipState::Joining => "joining",
        NodeMembershipState::Active => "active",
        NodeMembershipState::Draining => "draining",
        NodeMembershipState::Leaving => "leaving",
        NodeMembershipState::Removed => "removed",
        NodeMembershipState::Dead => "dead",
        NodeMembershipState::Rejected => "rejected",
    }
}

fn format_assignment_state(state: ShardAssignmentState) -> &'static str {
    match state {
        ShardAssignmentState::Planned => "planned",
        ShardAssignmentState::CatchingUp => "catching_up",
        ShardAssignmentState::CaughtUp => "caught_up",
        ShardAssignmentState::ServingReplica => "serving_replica",
        ShardAssignmentState::Promoting => "promoting",
        ShardAssignmentState::ServingPrimary => "serving_primary",
        ShardAssignmentState::Removing => "removing",
        ShardAssignmentState::Removed => "removed",
    }
}

fn format_cluster_metadata(metadata: &ClusterMetadataState) -> String {
    format!(
        "authority={} term={} config_epoch={} policy=replication_factor:{}:max_steps:{}",
        metadata.authority_server_id,
        metadata.term,
        metadata.config_epoch,
        metadata.policy.replication_factor,
        metadata.policy.max_steps_per_plan
    )
}

fn format_cluster_management_status(status: &ClusterManagementStatus) -> String {
    format!(
        "{{\"routing_version\":{},\"metadata\":\"{}\",\"membership\":\"{}\",\"rebalance_plan\":\"{}\",\"rebalance_execution\":\"{}\"}}",
        status.routing_version,
        escape_json_fragment(&format_cluster_metadata(&status.metadata)),
        escape_json_fragment(&format_cluster_membership(&status.membership)),
        escape_json_fragment(
            &status
                .rebalance_plan
                .as_ref()
                .map(format_rebalance_plan)
                .unwrap_or_else(|| "none".to_string())
        ),
        escape_json_fragment(
            &status
                .rebalance_execution
                .as_ref()
                .map(format_rebalance_execution)
                .unwrap_or_else(|| "none".to_string())
        )
    )
}

fn format_rebalance_plan(plan: &RebalancePlan) -> String {
    let steps = plan
        .steps
        .iter()
        .map(format_rebalance_step)
        .collect::<Vec<_>>()
        .join(",");
    format!(
        "plan_id={} state={} from_routing_version={} target_routing_version={} steps={} [{}]",
        plan.plan_id,
        format_rebalance_plan_state(plan.state),
        plan.from_routing_version,
        plan.target_routing_version,
        plan.steps.len(),
        steps
    )
}

fn format_rebalance_execution(execution: &RebalanceExecution) -> String {
    let steps = execution
        .steps
        .iter()
        .map(|step| {
            format!(
                "{}:{}:attempts={}:retryable={}:error={}:{}",
                step.step_index,
                format_rebalance_step_state(step.state),
                step.attempts,
                step.retryable,
                step.last_error,
                format_rebalance_step(&step.step)
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    format!(
        "plan_id={} state={} current_step={} last_error={} steps=[{}]",
        execution.plan.plan_id,
        format_rebalance_plan_state(execution.state),
        execution.current_step,
        execution.last_error,
        steps
    )
}

fn format_rebalance_plan_state(state: RebalancePlanState) -> &'static str {
    match state {
        RebalancePlanState::Proposed => "proposed",
        RebalancePlanState::Running => "running",
        RebalancePlanState::Completed => "completed",
        RebalancePlanState::Failed => "failed",
        RebalancePlanState::Cancelled => "cancelled",
    }
}

fn format_rebalance_step_state(state: RebalanceStepState) -> &'static str {
    match state {
        RebalanceStepState::Pending => "pending",
        RebalanceStepState::Preparing => "preparing",
        RebalanceStepState::CatchingUp => "catching_up",
        RebalanceStepState::Ready => "ready",
        RebalanceStepState::Applying => "applying",
        RebalanceStepState::Applied => "applied",
        RebalanceStepState::Failed => "failed",
        RebalanceStepState::Cancelled => "cancelled",
    }
}

fn format_rebalance_step(step: &RebalanceStep) -> String {
    match step {
        RebalanceStep::AddReplica {
            shard_id,
            server_id,
        } => format!("ADD_REPLICA\t{shard_id}\t{server_id}"),
        RebalanceStep::TransferPrimary { shard_id, from, to } => {
            format!("TRANSFER_PRIMARY\t{shard_id}\t{from}\t{to}")
        }
        RebalanceStep::RemoveReplica {
            shard_id,
            server_id,
        } => format!("REMOVE_REPLICA\t{shard_id}\t{server_id}"),
    }
}

fn escape_json_fragment(input: &str) -> String {
    input
        .chars()
        .flat_map(|ch| match ch {
            '\\' => "\\\\".chars().collect::<Vec<_>>(),
            '"' => "\\\"".chars().collect::<Vec<_>>(),
            '\n' => "\\n".chars().collect::<Vec<_>>(),
            '\r' => "\\r".chars().collect::<Vec<_>>(),
            '\t' => "\\t".chars().collect::<Vec<_>>(),
            _ => vec![ch],
        })
        .collect()
}

pub(crate) fn format_query_plan(plan: &DistributedQueryPlan) -> String {
    format!(
        "route={} traversal={} boundary_cache={} access={} cost={} rows={} remote_shards={}",
        format_query_route(&plan.route),
        format_traversal_policy(&plan.traversal_policy),
        plan.uses_boundary_cache,
        format_access_plan(&plan.access_plan),
        plan.estimated_cost,
        plan.estimated_rows,
        plan.remote_shard_count
    )
}

fn format_query_profile(profile: &QueryProfile) -> String {
    format!(
        "plan=[{}] operators=[{}] metrics=planning_us:{}:execution_us:{}:rows:{}:scanned_nodes:{}:scanned_relationships:{}:indexes:{}:cache_hits:{}:cache_misses:{}:index_cache_hits:{}:index_cache_misses:{}",
        format_query_plan(&profile.plan),
        profile
            .operators
            .iter()
            .map(format_query_operator_profile)
            .collect::<Vec<_>>()
            .join(","),
        profile.metrics.planning_elapsed_micros,
        profile.metrics.execution_elapsed_micros,
        profile.metrics.rows_returned,
        profile.metrics.scanned_nodes,
        profile.metrics.scanned_relationships,
        profile.metrics.index_count,
        profile.metrics.read_cache_hits,
        profile.metrics.read_cache_misses,
        profile.metrics.index_cache_hits,
        profile.metrics.index_cache_misses
    )
}

fn format_query_operator_profile(profile: &QueryOperatorProfile) -> String {
    let children = profile
        .children
        .iter()
        .map(format_query_operator_profile)
        .collect::<Vec<_>>()
        .join("|");
    format!(
        "{}:estimated={}:actual={}:elapsed_us={}:children=[{}]",
        profile.name, profile.estimated_rows, profile.actual_rows, profile.elapsed_micros, children
    )
}

fn format_storage_status(status: &StorageStatus) -> String {
    format!(
        "data_dir={} total_bytes={} files={} wal_segments={} checkpoints={} metadata_files={} committed=[{}] cache_hits={} cache_misses={} index_cache_hits={} index_cache_misses={} wal_pruned_until=[{}]",
        status.data_dir.display(),
        status.total_bytes,
        status.file_count,
        status.wal_segment_count,
        status.checkpoint_file_count,
        status.metadata_file_count,
        status
            .committed_indexes
            .iter()
            .map(|index| index.to_string())
            .collect::<Vec<_>>()
            .join(","),
        status.read_cache_hits,
        status.read_cache_misses,
        status.index_cache_hits,
        status.index_cache_misses,
        status
            .wal_pruned_until
            .iter()
            .map(|index| index.to_string())
            .collect::<Vec<_>>()
            .join(",")
    )
}

fn format_statistics_catalog(statistics: &StatisticsCatalog) -> String {
    format!(
        "nodes={} relationships={} labels=[{}] relationship_types=[{}] indexes={} vector_indexes={}",
        statistics.node_count,
        statistics.relationship_count,
        statistics
            .label_counts
            .iter()
            .map(|(label, count)| format!("{label}:{count}"))
            .collect::<Vec<_>>()
            .join(","),
        statistics
            .relationship_type_counts
            .iter()
            .map(|(rel_type, count)| format!("{rel_type}:{count}"))
            .collect::<Vec<_>>()
            .join(","),
        statistics.index_count,
        statistics.vector_index_count
    )
}

fn format_storage_maintenance(result: &StorageMaintenanceResult) -> String {
    format!(
        "action={} files_touched={} bytes_observed={} pruned_until=[{}]",
        result.action,
        result.files_touched,
        result.bytes_observed,
        result
            .pruned_until
            .iter()
            .map(|index| index.to_string())
            .collect::<Vec<_>>()
            .join(",")
    )
}

fn format_metadata_log(records: &[MetadataOperationRecord]) -> String {
    let entries = records
        .iter()
        .map(|record| {
            format!(
                "{}:term={}:epoch={}:op={}",
                record.index, record.term, record.config_epoch, record.operation
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    format!("count={} [{}]", records.len(), entries)
}

fn format_query_route(route: &QueryRoute) -> String {
    match route {
        QueryRoute::LocalOnly => "local".to_string(),
        QueryRoute::RequiresRemoteShards(shards) => {
            format!("remote({})", format_shard_list(shards))
        }
    }
}

fn format_traversal_policy(policy: &RemoteTraversalPolicy) -> String {
    match policy {
        RemoteTraversalPolicy::BoundaryCacheOnly => "boundary_cache".to_string(),
        RemoteTraversalPolicy::RemoteShardHop(shards) => {
            format!("remote_hop({})", format_shard_list(shards))
        }
    }
}

fn format_access_plan(plan: &QueryAccessPlan) -> String {
    match plan {
        QueryAccessPlan::NodeUniqueIndexSeek { label, property } => {
            format!("node_unique_index_seek({label}.{property})")
        }
        QueryAccessPlan::NodeIndexSeek { label, property } => {
            format!("node_index_seek({label}.{property})")
        }
        QueryAccessPlan::NodeLabelScan { label } => format!("node_label_scan({label})"),
        QueryAccessPlan::NodeFullScan => "node_full_scan".to_string(),
        QueryAccessPlan::VectorIndexSeek {
            label,
            property,
            metric,
        } => {
            let label = label.as_deref().unwrap_or("*");
            format!("vector_index_seek({label}.{property},metric={metric})")
        }
        QueryAccessPlan::RelationshipTypeScan { rel_type } => {
            format!("relationship_type_scan({rel_type})")
        }
        QueryAccessPlan::RelationshipScan => "relationship_scan".to_string(),
        QueryAccessPlan::Unsupported { reason } => format!("unsupported({reason})"),
    }
}

fn format_shard_list(shards: &[ShardId]) -> String {
    shards
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(",")
}

fn format_vector_index_status(statuses: &[neo4r_db::VectorIndexStatus]) -> String {
    if statuses.is_empty() {
        return "none".to_string();
    }
    statuses
        .iter()
        .map(|status| {
            format!(
                "{}:{}:{}:dimensions={}:metric={}:entries={}",
                status.name,
                status.label,
                status.property,
                status.dimensions,
                status.metric,
                status.entries
            )
        })
        .collect::<Vec<_>>()
        .join(",")
}

fn format_shard_status(status: &ShardStatus) -> String {
    let primary = status
        .primary_server_id
        .map(|server_id| server_id.to_string())
        .unwrap_or_else(|| "none".to_string());
    let replicas = status
        .replica_server_ids
        .iter()
        .map(|server_id| server_id.to_string())
        .collect::<Vec<_>>()
        .join("|");
    let matches = status
        .match_indexes
        .iter()
        .map(|(server_id, index)| format!("{server_id}:{index}"))
        .collect::<Vec<_>>()
        .join("|");
    format!(
        "shard={} primary={} replicas={} local={} local_primary={} applied={} committed={} match={}",
        status.shard_id,
        primary,
        replicas,
        status.has_local_copy,
        status.is_local_primary,
        status.applied_index,
        status.committed_index,
        matches
    )
}

pub fn encode_query_rows(rows: &[QueryRow]) -> String {
    neo4r_protocol::encode_query_rows(rows)
}

pub fn decode_query_rows(input: &str) -> Result<Vec<QueryRow>, String> {
    neo4r_protocol::decode_query_rows(input)
}

fn decode_value(input: &str) -> Result<Value, String> {
    if input == "n" {
        return Ok(Value::Null);
    }
    let (kind, payload) = input
        .split_once(':')
        .ok_or_else(|| format!("typed value missing kind: {input}"))?;
    match kind {
        "b" => match payload {
            "0" => Ok(Value::Bool(false)),
            "1" => Ok(Value::Bool(true)),
            _ => Err(format!("invalid bool payload: {payload}")),
        },
        "i" => payload
            .parse::<i64>()
            .map(Value::Int)
            .map_err(|_| format!("invalid int payload: {payload}")),
        "f" => payload
            .parse::<u64>()
            .map(f64::from_bits)
            .map(Value::Float)
            .map_err(|_| format!("invalid float payload: {payload}")),
        "s" => String::from_utf8(hex_decode(payload)?)
            .map(Value::String)
            .map_err(|_| "string payload is not valid UTF-8".to_string()),
        "v" => {
            if payload.is_empty() {
                return Ok(Value::Vector(Vec::new()));
            }
            payload
                .split(',')
                .map(|item| {
                    item.parse::<u32>()
                        .map(f32::from_bits)
                        .map_err(|_| format!("invalid vector payload: {item}"))
                })
                .collect::<Result<Vec<_>, _>>()
                .map(Value::Vector)
        }
        "m" => String::from_utf8(hex_decode(payload)?)
            .map_err(|_| "map payload is not valid UTF-8".to_string())
            .and_then(|payload| decode_properties(&payload))
            .map(Value::Map),
        _ => Err(format!("unknown value kind: {kind}")),
    }
}

fn decode_properties(input: &str) -> Result<Properties, String> {
    let mut properties = Properties::new();
    if input.is_empty() {
        return Ok(properties);
    }
    for entry in input.split(',') {
        let (key, value) = entry
            .split_once('~')
            .ok_or_else(|| format!("property entry missing '~': {entry}"))?;
        let key = String::from_utf8(hex_decode(key)?)
            .map_err(|_| "key is not valid UTF-8".to_string())?;
        properties.insert(key, decode_value(value)?);
    }
    Ok(properties)
}

fn parse_u64_token(input: &str, name: &str) -> Result<u64, String> {
    input
        .parse::<u64>()
        .map_err(|_| format!("{name} must be an unsigned integer"))
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

fn hex_decode(input: &str) -> Result<Vec<u8>, String> {
    if !input.len().is_multiple_of(2) {
        return Err(format!("hex string has odd length: {input}"));
    }
    let mut bytes = Vec::with_capacity(input.len() / 2);
    for chunk in input.as_bytes().chunks(2) {
        let high = hex_nibble(chunk[0])?;
        let low = hex_nibble(chunk[1])?;
        bytes.push((high << 4) | low);
    }
    Ok(bytes)
}

fn hex_nibble(byte: u8) -> Result<u8, String> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err(format!("invalid hex byte: {}", byte as char)),
    }
}

fn split_once_tab(line: &str) -> Option<(&str, &str)> {
    line.split_once('\t')
}

pub fn parse_query_payload(payload: &str) -> Result<(String, QueryParams), String> {
    neo4r_protocol::parse_query_payload(payload)
}

pub fn encode_query_batch_payload(writes: &[(String, QueryParams)]) -> String {
    neo4r_protocol::encode_query_batch_payload(writes)
}

pub fn decode_query_batch_payload(payload: &str) -> Result<Vec<(String, QueryParams)>, String> {
    neo4r_protocol::decode_query_batch_payload(payload)
}

fn parse_labels(value: &str) -> Result<Vec<String>, String> {
    let labels = value
        .split(',')
        .filter(|label| !label.is_empty())
        .map(validate_token)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(labels)
}

fn parse_properties<'a>(parts: impl Iterator<Item = &'a str>) -> Result<Properties, String> {
    let mut properties = Properties::new();
    for part in parts {
        let (key, value) = part
            .split_once('=')
            .ok_or_else(|| format!("property must be key=value: {part}"))?;
        properties.insert(validate_token(key)?, parse_value(value)?);
    }
    Ok(properties)
}

fn parse_value(value: &str) -> Result<Value, String> {
    let (kind, raw) = value
        .split_once(':')
        .ok_or_else(|| format!("value must use a typed prefix like s:value or i:1: {value}"))?;
    match kind {
        "n" => {
            if raw.is_empty() {
                Ok(Value::Null)
            } else {
                Err("null values must be encoded as n:".to_string())
            }
        }
        "b" => raw
            .parse::<bool>()
            .map(Value::Bool)
            .map_err(|_| format!("invalid bool value: {raw}")),
        "i" => raw
            .parse::<i64>()
            .map(Value::Int)
            .map_err(|_| format!("invalid int value: {raw}")),
        "f" => raw
            .parse::<f64>()
            .map(Value::Float)
            .map_err(|_| format!("invalid float value: {raw}")),
        "s" => Ok(Value::String(raw.to_string())),
        "v" => parse_vector_value(raw),
        "m" => String::from_utf8(hex_decode(raw)?)
            .map_err(|_| "map payload is not valid UTF-8".to_string())
            .and_then(|payload| decode_properties(&payload))
            .map(Value::Map),
        _ => Err(format!("unknown value type prefix: {kind}")),
    }
}

fn parse_vector_value(raw: &str) -> Result<Value, String> {
    if raw.is_empty() {
        return Err("vector value must contain at least one float".to_string());
    }
    raw.split(',')
        .map(|item| {
            item.parse::<f32>()
                .map_err(|_| format!("invalid vector element: {item}"))
        })
        .collect::<Result<Vec<_>, _>>()
        .map(Value::Vector)
}

fn parse_u64(value: Option<&str>, missing: &str) -> Result<u64, String> {
    value
        .filter(|value| !value.is_empty())
        .ok_or_else(|| missing.to_string())?
        .parse()
        .map_err(|_| missing.to_string())
}

fn parse_usize(value: Option<&str>, missing: &str) -> Result<usize, String> {
    value
        .filter(|value| !value.is_empty())
        .ok_or_else(|| missing.to_string())?
        .parse()
        .map_err(|_| missing.to_string())
}

fn parse_optional_if_not_exists<'a>(
    value: Option<&'a str>,
    mut remaining: impl Iterator<Item = &'a str>,
    command: &str,
) -> Result<bool, String> {
    let Some(value) = value else {
        return Ok(false);
    };
    if value != "IF_NOT_EXISTS" {
        return Err(format!(
            "{command} optional final argument must be IF_NOT_EXISTS"
        ));
    }
    if remaining.next().is_some() {
        return Err(format!(
            "{command} IF_NOT_EXISTS must be the final argument"
        ));
    }
    Ok(true)
}

fn parse_optional_if_exists<'a>(
    value: Option<&'a str>,
    mut remaining: impl Iterator<Item = &'a str>,
    command: &str,
) -> Result<bool, String> {
    let Some(value) = value else {
        return Ok(false);
    };
    if value != "IF_EXISTS" {
        return Err(format!(
            "{command} optional final argument must be IF_EXISTS"
        ));
    }
    if remaining.next().is_some() {
        return Err(format!("{command} IF_EXISTS must be the final argument"));
    }
    Ok(true)
}

fn parse_single_id(value: &str, missing: &str) -> Result<u64, String> {
    if value.contains('\t') {
        return Err(format!("{missing}; got extra fields"));
    }
    parse_u64(Some(value), missing)
}

fn parse_single_key(value: &str, missing: &str) -> Result<String, String> {
    if value.contains('\t') {
        return Err(format!("{missing}; got extra fields"));
    }
    parse_key(Some(value), missing)
}

fn parse_key(value: Option<&str>, missing: &str) -> Result<String, String> {
    value
        .filter(|value| !value.is_empty())
        .ok_or_else(|| missing.to_string())
        .and_then(validate_token)
}

fn parse_address(value: Option<&str>, missing: &str) -> Result<String, String> {
    value
        .filter(|value| !value.is_empty())
        .ok_or_else(|| missing.to_string())
        .and_then(validate_token)
}

fn validate_token(value: &str) -> Result<String, String> {
    if value.is_empty() {
        return Err("empty token".to_string());
    }
    if value.contains(['\t', '\n', '\r']) {
        return Err(format!("token contains a control separator: {value:?}"));
    }
    Ok(value.to_string())
}

fn escape_response(value: &str) -> String {
    value.replace('\\', "\\\\").replace('\t', "\\t")
}

#[cfg(test)]
mod tests {
    use super::*;
    use neo4r_core::{BoundaryNode, Node, Relationship};
    use neo4r_db::DatabaseConfig;
    use neo4r_protocol::encode_properties;
    use neo4r_query::QueryValue;
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn parses_create_node_with_typed_properties() {
        let request = parse_request("CREATE_NODE\tPerson,User\tname=s:alice\tage=i:42").unwrap();

        let BackendRequest::CreateNode { labels, properties } = request else {
            panic!("unexpected request");
        };
        assert_eq!(labels, vec!["Person".to_string(), "User".to_string()]);
        assert_eq!(
            properties.get("name"),
            Some(&Value::String("alice".to_string()))
        );
        assert_eq!(properties.get("age"), Some(&Value::Int(42)));
    }

    #[test]
    fn parses_create_node_on_shard_with_typed_properties() {
        let request =
            parse_request("CREATE_NODE_SHARD\t1\tPerson\tname=s:alice\tage=i:42").unwrap();

        let BackendRequest::CreateNodeOnShard {
            shard_id,
            labels,
            properties,
        } = request
        else {
            panic!("unexpected request");
        };
        assert_eq!(shard_id, 1);
        assert_eq!(labels, vec!["Person".to_string()]);
        assert_eq!(
            properties.get("name"),
            Some(&Value::String("alice".to_string()))
        );
        assert_eq!(properties.get("age"), Some(&Value::Int(42)));
    }

    #[test]
    fn parses_remove_property_commands() {
        assert_eq!(
            parse_request("REMOVE_NODE_PROPERTY\t7\tstatus").unwrap(),
            BackendRequest::RemoveNodeProperty {
                id: 7,
                key: "status".to_string(),
            }
        );
        assert_eq!(
            parse_request("REMOVE_RELATIONSHIP_PROPERTY\t9\tweight").unwrap(),
            BackendRequest::RemoveRelationshipProperty {
                id: 9,
                key: "weight".to_string(),
            }
        );
    }

    #[test]
    fn parses_node_label_commands() {
        assert_eq!(
            parse_request("ADD_NODE_LABEL\t7\tEmployee").unwrap(),
            BackendRequest::AddNodeLabel {
                id: 7,
                label: "Employee".to_string(),
            }
        );
        assert_eq!(
            parse_request("REMOVE_NODE_LABEL\t7\tPerson").unwrap(),
            BackendRequest::RemoveNodeLabel {
                id: 7,
                label: "Person".to_string(),
            }
        );
    }

    #[test]
    fn rejects_unknown_value_prefix() {
        let err = parse_request("CREATE_NODE\tPerson\tname=x:alice").unwrap_err();

        assert_eq!(err, "unknown value type prefix: x");
    }

    #[test]
    fn parses_query_with_typed_params() {
        let props = [
            ("score".to_string(), Value::Int(7)),
            ("status".to_string(), Value::String("active".to_string())),
        ]
        .into_iter()
        .collect();
        let encoded_props = hex_encode(encode_properties(&props).as_bytes());
        let request = parse_request(
            &format!(
                "QUERY\tMATCH (n:Document) WHERE vector.knn(n.embedding, $embedding, $k, $metric) RETURN n.title\tembedding=v:1.0,0.0\tk=i:4\tmetric=s:l2\tprops=m:{encoded_props}"
            ),
        )
        .unwrap();

        let BackendRequest::Query { query, params } = request else {
            panic!("unexpected request");
        };
        assert!(query.contains("vector.knn"));
        assert_eq!(
            params.get("embedding"),
            Some(&Value::Vector(vec![1.0, 0.0]))
        );
        assert_eq!(params.get("k"), Some(&Value::Int(4)));
        assert_eq!(params.get("metric"), Some(&Value::String("l2".to_string())));
        assert_eq!(params.get("props"), Some(&Value::Map(props)));
    }

    #[test]
    fn parses_query_plan_with_typed_params() {
        let request = parse_request(
            "QUERY_PLAN\tMATCH (n:Person) WHERE n.name = $name RETURN n\tname=s:Alice",
        )
        .unwrap();

        let BackendRequest::QueryPlan { query, params } = request else {
            panic!("unexpected request");
        };
        assert!(query.starts_with("MATCH"));
        assert_eq!(
            params.get("name"),
            Some(&Value::String("Alice".to_string()))
        );
    }

    #[test]
    fn parses_query_write_shard_with_typed_params() {
        let request = parse_request(
            "QUERY_WRITE_SHARD\t1\tCREATE (n:Person {name: $name}) RETURN n.name\tname=s:alice",
        )
        .unwrap();

        let BackendRequest::QueryWriteShard {
            shard_id,
            query,
            params,
        } = request
        else {
            panic!("unexpected request");
        };
        assert_eq!(shard_id, 1);
        assert!(query.starts_with("CREATE"));
        assert_eq!(
            params.get("name"),
            Some(&Value::String("alice".to_string()))
        );
    }

    #[test]
    fn query_write_batch_shard_codec_round_trips_params() {
        let writes = vec![
            (
                "MATCH (n:Person) SET n.status = $status".to_string(),
                [(
                    "status".to_string(),
                    Value::String("active\tready".to_string()),
                )]
                .into_iter()
                .collect(),
            ),
            (
                "MATCH (n:Person) SET n.reviewed = $reviewed".to_string(),
                [("reviewed".to_string(), Value::Bool(true))]
                    .into_iter()
                    .collect(),
            ),
            (
                "MATCH (n:Person) SET n += $props".to_string(),
                [(
                    "props".to_string(),
                    Value::Map(
                        [("status".to_string(), Value::String("ready".to_string()))]
                            .into_iter()
                            .collect(),
                    ),
                )]
                .into_iter()
                .collect(),
            ),
        ];
        let request = parse_request(&format!(
            "QUERY_WRITE_BATCH_SHARD\t1\t{}",
            encode_query_batch_payload(&writes)
        ))
        .unwrap();

        assert_eq!(
            request,
            BackendRequest::QueryWriteBatchShard {
                shard_id: 1,
                writes
            }
        );
    }

    #[test]
    fn query_staged_shard_codec_uses_first_batch_entry_as_read() {
        let read_params = [("name".to_string(), Value::String("Alice".to_string()))]
            .into_iter()
            .collect();
        let staged_params = [("status".to_string(), Value::String("staged".to_string()))]
            .into_iter()
            .collect();
        let batch = vec![
            ("MATCH (n:Person) RETURN n.status".to_string(), read_params),
            (
                "MATCH (n:Person) WHERE n.name = $name SET n.status = $status".to_string(),
                staged_params,
            ),
        ];
        let request = parse_request(&format!(
            "QUERY_STAGED_SHARD\t1\t{}",
            encode_query_batch_payload(&batch)
        ))
        .unwrap();

        let BackendRequest::QueryStagedShard {
            shard_id,
            query,
            params,
            staged_writes,
        } = request
        else {
            panic!("unexpected request");
        };
        assert_eq!(shard_id, 1);
        assert_eq!(query, batch[0].0);
        assert_eq!(params, batch[0].1);
        assert_eq!(staged_writes, vec![batch[1].clone()]);
    }

    #[test]
    fn index_catalog_codec_round_trips_definitions() {
        let catalog = IndexCatalog {
            version: 7,
            indexes: vec![
                IndexDefinition::node_property("person_name", "Person", "name"),
                IndexDefinition::unique_node_property("person_email_unique", "Person", "email"),
                IndexDefinition::vector("doc_embedding", "Document", "embedding", 3, "cosine"),
            ],
        };

        let encoded = encode_index_catalog(&catalog);
        assert_eq!(decode_index_catalog(&encoded).unwrap(), catalog);
        assert_eq!(
            parse_request(&format!("INSTALL_INDEX_CATALOG\t{encoded}")).unwrap(),
            BackendRequest::InstallIndexCatalog(catalog)
        );
    }

    #[test]
    fn query_row_codec_round_trips_scalars_nodes_and_relationships() {
        let mut row = QueryRow::new();
        row.insert(
            "name",
            QueryValue::Scalar(Value::String("Alice\tA".to_string())),
        );
        row.insert(
            "n",
            QueryValue::Node(Node::new(
                7,
                vec!["Person".to_string()],
                [("age".to_string(), Value::Int(42))].into_iter().collect(),
            )),
        );
        row.insert(
            "r",
            QueryValue::Relationship(Relationship::new(
                9,
                7,
                8,
                "KNOWS".to_string(),
                [("since".to_string(), Value::Int(2026))]
                    .into_iter()
                    .collect(),
            )),
        );
        row.insert(
            "b",
            QueryValue::BoundaryNode(BoundaryNode::new(
                8,
                1,
                vec!["Person".to_string()],
                [("name".to_string(), Value::String("Bob".to_string()))]
                    .into_iter()
                    .collect(),
                3,
            )),
        );

        let encoded = encode_query_rows(&[row.clone()]);

        assert_eq!(decode_query_rows(&encoded).unwrap(), vec![row]);
    }

    #[test]
    fn query_shard_parses_and_executes_against_one_shard() {
        let dir = temp_dir("server-query-shard");
        let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 2, 2)).unwrap();
        db.create_node(
            vec!["Person".to_string()],
            [("name".to_string(), Value::String("Alice".to_string()))]
                .into_iter()
                .collect(),
        )
        .unwrap();
        db.create_node(
            vec!["Person".to_string()],
            [("name".to_string(), Value::String("Bob".to_string()))]
                .into_iter()
                .collect(),
        )
        .unwrap();

        let request = parse_request(
            "QUERY_SHARD\t1\tMATCH (n:Person) WHERE n.name = $name RETURN n.name\tname=s:Bob",
        )
        .unwrap();
        let BackendRequest::QueryShard {
            shard_id,
            query,
            params,
        } = &request
        else {
            panic!("unexpected request");
        };
        assert_eq!(*shard_id, 1);
        assert!(query.contains("MATCH"));
        assert_eq!(params.get("name"), Some(&Value::String("Bob".to_string())));

        assert!(matches!(
            execute_request(&db, request),
            BackendResponse::OkRows { count: 1, .. }
        ));
        assert!(matches!(
            execute_request(
                &db,
                parse_request(
                    "QUERY_SHARD\t0\tMATCH (n:Person) WHERE n.name = $name RETURN n.name\tname=s:Bob"
                )
                .unwrap()
            ),
            BackendResponse::OkRows { count: 0, .. }
        ));

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn query_distributed_parses_but_requires_backend_coordinator() {
        let dir = temp_dir("server-query-distributed");
        let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 2, 2)).unwrap();

        let request = parse_request(
            "QUERY_DISTRIBUTED\tMATCH (n:Person) WHERE n.name = $name RETURN n.name\tname=s:Alice",
        )
        .unwrap();
        let BackendRequest::QueryDistributed { query, params } = &request else {
            panic!("unexpected request");
        };
        assert!(query.contains("MATCH"));
        assert_eq!(
            params.get("name"),
            Some(&Value::String("Alice".to_string()))
        );
        assert!(matches!(
            execute_request(&db, request),
            BackendResponse::Err(message) if message.contains("requires a backend coordinator")
        ));

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn peer_management_and_catch_up_parse_but_require_backend_coordinator() {
        let dir = temp_dir("server-query-peer-management");
        let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1)).unwrap();

        assert_eq!(
            parse_request("REGISTER_QUERY_PEER\t2\t127.0.0.1:7688").unwrap(),
            BackendRequest::RegisterQueryPeer {
                server_id: 2,
                address: "127.0.0.1:7688".to_string(),
            }
        );
        assert_eq!(
            parse_request("UNREGISTER_QUERY_PEER\t2").unwrap(),
            BackendRequest::UnregisterQueryPeer(2)
        );
        assert_eq!(
            parse_request("LIST_QUERY_PEERS").unwrap(),
            BackendRequest::ListQueryPeers
        );
        assert_eq!(
            parse_request("REGISTER_REPLICATION_PEER\t3\t127.0.0.1:7689").unwrap(),
            BackendRequest::RegisterReplicationPeer {
                server_id: 3,
                address: "127.0.0.1:7689".to_string(),
            }
        );
        assert_eq!(
            parse_request("UNREGISTER_REPLICATION_PEER\t3").unwrap(),
            BackendRequest::UnregisterReplicationPeer(3)
        );
        assert_eq!(
            parse_request("LIST_REPLICATION_PEERS").unwrap(),
            BackendRequest::ListReplicationPeers
        );
        assert_eq!(
            parse_request("REPLICATION_PEER_STATUS").unwrap(),
            BackendRequest::ReplicationPeerStatus { server_id: None }
        );
        assert_eq!(
            parse_request("REPLICATION_PEER_STATUS\t3").unwrap(),
            BackendRequest::ReplicationPeerStatus { server_id: Some(3) }
        );
        assert_eq!(
            parse_request("REPLICATION_STATUS").unwrap(),
            BackendRequest::ReplicationStatus
        );
        assert_eq!(
            parse_request("CATCH_UP_FROM_PRIMARIES").unwrap(),
            BackendRequest::CatchUpFromPrimaries {
                max_entries_per_request: None,
            }
        );
        assert_eq!(
            parse_request("CATCH_UP_FROM_PRIMARIES\t2").unwrap(),
            BackendRequest::CatchUpFromPrimaries {
                max_entries_per_request: Some(2),
            }
        );
        assert_eq!(
            parse_request("CATCH_UP_FROM_PRIMARY\t3").unwrap(),
            BackendRequest::CatchUpFromPrimary {
                server_id: 3,
                max_entries_per_request: None,
            }
        );
        assert_eq!(
            parse_request("CATCH_UP_FROM_PRIMARY\t3\t2").unwrap(),
            BackendRequest::CatchUpFromPrimary {
                server_id: 3,
                max_entries_per_request: Some(2),
            }
        );
        assert_eq!(
            parse_request("CATCH_UP_PLAN").unwrap(),
            BackendRequest::CatchUpPlan { server_id: None }
        );
        assert_eq!(
            parse_request("CATCH_UP_PLAN_PRIMARY\t3").unwrap(),
            BackendRequest::CatchUpPlan { server_id: Some(3) }
        );
        assert!(matches!(
            execute_request(
                &db,
                parse_request("REGISTER_QUERY_PEER\t2\t127.0.0.1:7688").unwrap()
            ),
            BackendResponse::Err(message) if message.contains("backend coordinator")
        ));
        assert!(matches!(
            execute_request(&db, parse_request("CATCH_UP_FROM_PRIMARIES").unwrap()),
            BackendResponse::Err(message) if message.contains("backend coordinator")
        ));

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn query_request_can_execute_cypher_write() {
        let dir = temp_dir("server-cypher-write");
        let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 2, 2)).unwrap();

        let response = execute_request(
            &db,
            parse_request("QUERY\tCREATE (n:Person {name: $name}) RETURN n\tname=s:Alice").unwrap(),
        );

        assert!(matches!(response, BackendResponse::OkRows { count: 1, .. }));
        assert_eq!(
            db.query(r#"MATCH (n:Person) WHERE n.name = "Alice" RETURN n"#)
                .unwrap()
                .len(),
            1
        );

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn query_plan_request_reports_access_path() {
        let dir = temp_dir("server-query-plan");
        let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1)).unwrap();
        db.execute_cypher("CREATE INDEX person_name FOR (n:Person) ON (n.name)")
            .unwrap();

        let response = execute_request(
            &db,
            parse_request("QUERY_PLAN\tMATCH (n:Person {name: $name}) RETURN n\tname=s:Alice")
                .unwrap(),
        );

        let BackendResponse::OkQueryPlan(plan) = response else {
            panic!("expected query plan response");
        };
        assert!(plan.contains("route=local"));
        assert!(plan.contains("access=node_index_seek(Person.name)"));
        assert_eq!(
            format_response(&BackendResponse::OkQueryPlan(plan.clone())),
            format!("OK\tQUERY_PLAN\t{plan}")
        );

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn performance_commands_report_profile_storage_and_statistics() {
        let dir = temp_dir("server-performance-protocol");
        let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1)).unwrap();
        execute_request(
            &db,
            parse_request("QUERY\tCREATE (n:Person {name: $name}) RETURN n\tname=s:Alice").unwrap(),
        );

        let profile = execute_request(
            &db,
            parse_request("PROFILE\tMATCH (n:Person) RETURN n").unwrap(),
        );
        let BackendResponse::OkQueryProfile(profile) = profile else {
            panic!("expected profile response");
        };
        assert!(profile.contains("metrics="));
        assert!(profile.contains("cost="));

        let storage = execute_request(&db, parse_request("STORAGE_STATUS").unwrap());
        let BackendResponse::OkStorageStatus(storage) = storage else {
            panic!("expected storage status");
        };
        assert!(storage.contains("total_bytes="));

        let statistics = execute_request(&db, parse_request("STATISTICS").unwrap());
        let BackendResponse::OkStatistics(statistics) = statistics else {
            panic!("expected statistics");
        };
        assert!(statistics.contains("nodes=1"));

        let checkpoint = execute_request(&db, parse_request("CHECKPOINT_NOW").unwrap());
        assert!(matches!(
            checkpoint,
            BackendResponse::OkStorageMaintenance(result) if result.contains("action=checkpoint")
        ));

        let compact = execute_request(&db, parse_request("COMPACT_STORAGE").unwrap());
        assert!(matches!(
            compact,
            BackendResponse::OkStorageMaintenance(result) if result.contains("action=compact_observe")
        ));

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn remove_property_commands_execute_against_database() {
        let dir = temp_dir("server-remove-property");
        let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 2, 2)).unwrap();
        let alice = db
            .create_node(
                vec!["Person".to_string()],
                [("status".to_string(), Value::String("active".to_string()))]
                    .into_iter()
                    .collect(),
            )
            .unwrap();
        let bob = db
            .create_node(vec!["Person".to_string()], Properties::new())
            .unwrap();
        let rel = db
            .create_relationship(
                alice,
                bob,
                "KNOWS".to_string(),
                [("weight".to_string(), Value::Int(3))]
                    .into_iter()
                    .collect(),
            )
            .unwrap();

        assert_eq!(
            execute_request(
                &db,
                BackendRequest::RemoveNodeProperty {
                    id: alice,
                    key: "status".to_string(),
                },
            ),
            BackendResponse::OkUnit
        );
        assert_eq!(
            execute_request(
                &db,
                BackendRequest::RemoveRelationshipProperty {
                    id: rel,
                    key: "weight".to_string(),
                },
            ),
            BackendResponse::OkUnit
        );

        assert!(db
            .query(r#"MATCH (n:Person) WHERE n.status = "active" RETURN n"#)
            .unwrap()
            .is_empty());
        assert!(db
            .query(r#"MATCH (a:Person)-[r:KNOWS]->(b:Person) WHERE r.weight = 3 RETURN r"#)
            .unwrap()
            .is_empty());

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn command_property_map_values_return_error_before_wal_append() {
        let dir = temp_dir("server-command-map-property-validation");
        let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1)).unwrap();
        let map_value = Value::Map(
            [("nested".to_string(), Value::Bool(true))]
                .into_iter()
                .collect(),
        );
        let encoded_map = hex_encode(
            encode_properties(match &map_value {
                Value::Map(values) => values,
                _ => unreachable!(),
            })
            .as_bytes(),
        );

        let create = execute_request(
            &db,
            parse_request(&format!("CREATE_NODE\tPerson\tprofile=m:{encoded_map}")).unwrap(),
        );

        assert!(matches!(create, BackendResponse::Err(message) if message.contains("nested map")));
        assert!(db.query("MATCH (n:Person) RETURN n").unwrap().is_empty());
        assert_eq!(db.committed_indexes().unwrap(), vec![0]);

        let alice = db
            .create_node(
                vec!["Person".to_string()],
                [("name".to_string(), Value::String("Alice".to_string()))]
                    .into_iter()
                    .collect(),
            )
            .unwrap();
        let set = execute_request(
            &db,
            parse_request(&format!(
                "SET_NODE_PROPERTY\t{alice}\tprofile\tm:{encoded_map}"
            ))
            .unwrap(),
        );

        assert!(matches!(set, BackendResponse::Err(message) if message.contains("nested map")));
        assert_eq!(db.committed_indexes().unwrap(), vec![1]);

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn cluster_status_command_reports_database_positions() {
        let dir = temp_dir("server-cluster-status");
        let db =
            Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1).with_server_id(1)).unwrap();
        db.create_node(
            vec!["Person".to_string()],
            [("name".to_string(), Value::String("Alice".to_string()))]
                .into_iter()
                .collect(),
        )
        .unwrap();

        let response = execute_request(&db, parse_request("CLUSTER_STATUS").unwrap());
        let text = format_response(&response);

        assert!(text.starts_with("OK\tCLUSTER_STATUS\t"));
        assert!(text.contains("server=1"));
        assert!(text.contains("routing_version=1"));
        assert!(text.contains("shards=1"));
        assert!(text.contains("partitions=1"));
        assert!(text.contains("shard=0"));
        assert!(text.contains("primary=1"));
        assert!(text.contains("local=true"));
        assert!(text.contains("local_primary=true"));
        assert!(text.contains("applied=1"));
        assert!(text.contains("committed=1"));

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn cluster_membership_commands_execute_against_database() {
        let dir = temp_dir("server-membership-protocol");
        let db =
            Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 2, 1).with_server_id(1)).unwrap();

        assert_eq!(
            execute_request(
                &db,
                parse_request("JOIN_REQUEST\t2\t127.0.0.1:17688\t1\t1\t2").unwrap()
            ),
            BackendResponse::OkClusterNodes("version=2 nodes=[1:active::protocol=0:storage=0:shards=2:reason=,2:negotiating:127.0.0.1:17688:protocol=1:storage=1:shards=2:reason=] assignments=[]".to_string())
        );
        assert!(matches!(
            parse_request("JOIN_ACCEPT\t2").unwrap(),
            BackendRequest::JoinAccept(2)
        ));
        execute_request(&db, parse_request("JOIN_ACCEPT\t2").unwrap());
        let response = execute_request(&db, parse_request("LIST_NODES").unwrap());
        let BackendResponse::OkClusterNodes(nodes) = response else {
            panic!("expected cluster nodes");
        };
        assert!(nodes.contains("2:joining:127.0.0.1:17688"));
        assert_eq!(
            execute_request(
                &db,
                parse_request("JOIN_REQUEST\t3\t127.0.0.1:17689\t1\t1\t3").unwrap()
            ),
            BackendResponse::OkClusterNodes("version=4 nodes=[1:active::protocol=0:storage=0:shards=2:reason=,2:joining:127.0.0.1:17688:protocol=1:storage=1:shards=2:reason=,3:rejected:127.0.0.1:17689:protocol=1:storage=1:shards=3:reason=shard count mismatch: requested 3, cluster 2] assignments=[]".to_string())
        );
        let response = execute_request(&db, parse_request("PLAN_REBALANCE").unwrap());
        let BackendResponse::OkRebalancePlan(plan) = response else {
            panic!("expected rebalance plan");
        };
        assert!(plan.contains("ADD_REPLICA\t0\t2"));

        assert!(matches!(
            execute_request(
                &db,
                parse_request("APPLY_REBALANCE_STEP\tADD_REPLICA\t0\t2").unwrap()
            ),
            BackendResponse::Err(message) if message.contains("must be prepared and caught up")
        ));
        let response = execute_request(
            &db,
            parse_request("PREPARE_REBALANCE_STEP\tADD_REPLICA\t0\t2").unwrap(),
        );
        let BackendResponse::OkClusterNodes(nodes) = response else {
            panic!("expected cluster nodes");
        };
        assert!(nodes.contains("state=catching_up"));
        execute_request(&db, parse_request("MARK_SHARD_CAUGHT_UP\t0\t2\t0").unwrap());
        assert_eq!(
            execute_request(
                &db,
                parse_request("APPLY_REBALANCE_STEP\tADD_REPLICA\t0\t2").unwrap()
            ),
            BackendResponse::OkClusterStatus("routing_version=2".to_string())
        );
        let response = execute_request(&db, parse_request("LIST_NODES").unwrap());
        let BackendResponse::OkClusterNodes(nodes) = response else {
            panic!("expected cluster nodes");
        };
        assert!(nodes.contains("2:active:127.0.0.1:17688"));

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn cluster_management_commands_report_structured_status() {
        let dir = temp_dir("server-cluster-management-protocol");
        let db =
            Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1).with_server_id(1)).unwrap();

        let response = execute_request(&db, parse_request("METADATA_AUTHORITY").unwrap());
        let BackendResponse::OkClusterManagementStatus(metadata) = response else {
            panic!("expected metadata status");
        };
        assert!(metadata.contains("authority=1"));
        assert_eq!(
            execute_request(&db, parse_request("SET_REBALANCE_POLICY\t2\t4").unwrap()),
            BackendResponse::OkClusterManagementStatus(
                "authority=1 term=1 config_epoch=1 policy=replication_factor:2:max_steps:4"
                    .to_string()
            )
        );

        execute_request(
            &db,
            parse_request("JOIN_REQUEST\t2\t127.0.0.1:17688\t1\t1\t1").unwrap(),
        );
        execute_request(&db, parse_request("JOIN_ACCEPT\t2").unwrap());
        let started = execute_request(&db, parse_request("START_REBALANCE").unwrap());
        let BackendResponse::OkRebalanceExecution(started) = started else {
            panic!("expected rebalance execution");
        };
        assert!(started.contains("state=running"));
        assert!(started.contains("ADD_REPLICA"));

        let advanced = execute_request(&db, parse_request("ADVANCE_REBALANCE").unwrap());
        let BackendResponse::OkRebalanceExecution(advanced) = advanced else {
            panic!("expected rebalance advance");
        };
        assert!(advanced.contains("action=prepared"));

        let status = execute_request(&db, parse_request("CLUSTER_MANAGEMENT_STATUS").unwrap());
        let BackendResponse::OkClusterManagementStatus(status) = status else {
            panic!("expected cluster management status");
        };
        assert!(status.contains("\"metadata\""));
        assert!(status.contains("\"rebalance_execution\""));

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn recover_tx_decisions_requires_backend_coordinator_in_protocol_executor() {
        let dir = temp_dir("server-recover-tx-protocol");
        let db =
            Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1).with_server_id(1)).unwrap();

        assert_eq!(
            parse_request("LIST_TX_DECISIONS").unwrap(),
            BackendRequest::ListTransactionDecisions
        );
        assert_eq!(
            format_response(&BackendResponse::OkTransactionDecisions(
                "count=1 entries=tx=7 decision=commit participants=local@0#3".to_string()
            )),
            "OK\tTX_DECISIONS\tcount=1 entries=tx=7 decision=commit participants=local@0#3"
        );
        assert!(matches!(
            execute_request(&db, parse_request("LIST_TX_DECISIONS").unwrap()),
            BackendResponse::Err(message) if message.contains("requires a backend coordinator")
        ));
        assert_eq!(
            parse_request("RECOVER_TX_DECISIONS").unwrap(),
            BackendRequest::RecoverTransactionDecisions
        );
        assert_eq!(
            format_response(&BackendResponse::OkTransactionRecovery(3)),
            "OK\tTX_RECOVERY\t3"
        );
        assert!(matches!(
            execute_request(&db, parse_request("RECOVER_TX_DECISIONS").unwrap()),
            BackendResponse::Err(message) if message.contains("requires a backend coordinator")
        ));

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn install_routing_table_command_updates_cluster_status() {
        let dir = temp_dir("server-install-routing-table");
        let db =
            Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 2, 2).with_server_id(10)).unwrap();

        let response = execute_request(
            &db,
            parse_request("INSTALL_ROUTING_TABLE\t2\t0:10:11\t1:11:10").unwrap(),
        );

        assert_eq!(response, BackendResponse::OkUnit);
        let text = format_response(&execute_request(
            &db,
            parse_request("CLUSTER_STATUS").unwrap(),
        ));
        assert!(text.contains("routing_version=2"));
        assert!(text.contains("shard=0 primary=10 replicas=11 local=true local_primary=true"));
        assert!(text.contains("shard=1 primary=11 replicas=10 local=true local_primary=false"));

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn install_routing_table_rejects_non_increasing_version() {
        let dir = temp_dir("server-install-routing-version");
        let db =
            Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1).with_server_id(1)).unwrap();

        let response = execute_request(
            &db,
            parse_request("INSTALL_ROUTING_TABLE\t1\t0:1:").unwrap(),
        );

        assert!(
            matches!(response, BackendResponse::Err(message) if message.contains("version must increase"))
        );
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn index_catalog_commands_execute_against_database() {
        let dir = temp_dir("server-index-catalog");
        let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 2, 2)).unwrap();

        assert_eq!(
            execute_request(
                &db,
                parse_request("CREATE_INDEX\tperson_name\tPerson\tname").unwrap(),
            ),
            BackendResponse::OkUnit
        );
        let version = db.index_catalog().unwrap().version;
        assert_eq!(
            execute_request(
                &db,
                parse_request("CREATE_INDEX\tperson_name\tPerson\tname\tIF_NOT_EXISTS").unwrap(),
            ),
            BackendResponse::OkUnit
        );
        assert_eq!(db.index_catalog().unwrap().version, version);
        assert!(matches!(
            parse_request("CREATE_INDEX\tperson_name\tPerson\tname\tUNKNOWN"),
            Err(message) if message.contains("IF_NOT_EXISTS")
        ));
        assert_eq!(
            execute_request(
                &db,
                parse_request("CREATE_VECTOR_INDEX\tdoc_embedding\tDocument\tembedding\t2\tcosine")
                    .unwrap(),
            ),
            BackendResponse::OkUnit
        );
        assert_eq!(
            execute_request(
                &db,
                parse_request(
                    "CREATE_VECTOR_INDEX\tdoc_embedding\tDocument\tembedding\t2\tcosine\tIF_NOT_EXISTS"
                )
                .unwrap(),
            ),
            BackendResponse::OkUnit
        );
        assert_eq!(
            parse_request("REBUILD_VECTOR_INDEX\tdoc_embedding").unwrap(),
            BackendRequest::RebuildVectorIndex {
                name: "doc_embedding".to_string()
            }
        );
        assert!(matches!(
            parse_request("REBUILD_VECTOR_INDEX\tdoc_embedding\textra"),
            Err(message) if message.contains("extra fields")
        ));
        assert_eq!(
            execute_request(
                &db,
                parse_request("REBUILD_VECTOR_INDEX\tdoc_embedding").unwrap(),
            ),
            BackendResponse::OkUnit
        );
        assert!(matches!(
            execute_request(
                &db,
                parse_request("REBUILD_VECTOR_INDEX\tperson_name").unwrap(),
            ),
            BackendResponse::Err(message) if message.contains("does not exist") || message.contains("not a vector index")
        ));
        assert_eq!(
            execute_request(
                &db,
                parse_request("CREATE_CONSTRAINT\tperson_email_unique\tPerson\temail").unwrap(),
            ),
            BackendResponse::OkUnit
        );
        assert_eq!(
            execute_request(
                &db,
                parse_request(
                    "CREATE_CONSTRAINT\tperson_email_unique\tPerson\temail\tIF_NOT_EXISTS"
                )
                .unwrap(),
            ),
            BackendResponse::OkUnit
        );
        assert!(matches!(
            execute_request(
                &db,
                parse_request("CREATE_CONSTRAINT\tperson_email_unique\tPerson\tname\tIF_NOT_EXISTS")
                    .unwrap(),
            ),
            BackendResponse::Err(message) if message.contains("different definition")
        ));
        assert_eq!(
            execute_request(&db, parse_request("REBUILD_VECTOR_INDEXES").unwrap()),
            BackendResponse::OkUnit
        );
        assert!(matches!(
            parse_request("VECTOR_INDEX_STATUS").unwrap(),
            BackendRequest::VectorIndexStatus { name: None }
        ));
        assert_eq!(
            parse_request("VECTOR_INDEX_STATUS\tdoc_embedding").unwrap(),
            BackendRequest::VectorIndexStatus {
                name: Some("doc_embedding".to_string())
            }
        );
        assert!(matches!(
            parse_request("VECTOR_INDEX_STATUS\tdoc_embedding\textra"),
            Err(message) if message.contains("extra fields")
        ));
        let BackendResponse::OkVectorIndexStatus(vector_status) =
            execute_request(&db, parse_request("VECTOR_INDEX_STATUS").unwrap())
        else {
            panic!("expected vector index status");
        };
        assert!(vector_status.contains("doc_embedding:Document:embedding"));
        assert!(vector_status.contains("dimensions=2"));
        assert!(vector_status.contains("metric=cosine"));
        let BackendResponse::OkVectorIndexStatus(vector_status) = execute_request(
            &db,
            parse_request("VECTOR_INDEX_STATUS\tdoc_embedding").unwrap(),
        ) else {
            panic!("expected vector index status");
        };
        assert_eq!(
            vector_status,
            "doc_embedding:Document:embedding:dimensions=2:metric=cosine:entries=0"
        );
        assert!(matches!(
            execute_request(&db, parse_request("VECTOR_INDEX_STATUS\tmissing").unwrap()),
            BackendResponse::Err(message) if message.contains("does not exist")
        ));
        let response = execute_request(&db, parse_request("LIST_INDEXES").unwrap());
        let BackendResponse::OkRows { count, debug_rows } = response else {
            panic!("expected index rows");
        };
        assert_eq!(count, 3);
        assert!(debug_rows.contains("person_name"));
        assert!(debug_rows.contains("doc_embedding"));
        assert!(debug_rows.contains("person_email_unique"));
        let version = db.index_catalog().unwrap().version;
        assert_eq!(
            execute_request(
                &db,
                parse_request("DROP_INDEX\tmissing_index\tIF_EXISTS").unwrap(),
            ),
            BackendResponse::OkUnit
        );
        assert_eq!(
            execute_request(
                &db,
                parse_request("DROP_CONSTRAINT\tmissing_constraint\tIF_EXISTS").unwrap(),
            ),
            BackendResponse::OkUnit
        );
        assert_eq!(db.index_catalog().unwrap().version, version);
        assert!(matches!(
            parse_request("DROP_INDEX\tmissing_index\tUNKNOWN"),
            Err(message) if message.contains("IF_EXISTS")
        ));
        assert!(matches!(
            execute_request(
                &db,
                parse_request("DROP_CONSTRAINT\tdoc_embedding\tIF_EXISTS").unwrap(),
            ),
            BackendResponse::Err(message) if message.contains("is not a constraint")
        ));
        assert_eq!(
            execute_request(
                &db,
                parse_request("DROP_CONSTRAINT\tperson_email_unique").unwrap(),
            ),
            BackendResponse::OkUnit
        );
        assert_eq!(
            execute_request(&db, parse_request("DROP_INDEX\tperson_name").unwrap()),
            BackendResponse::OkUnit
        );
        assert_eq!(
            execute_request(
                &db,
                parse_request("DROP_INDEX\tperson_name\tIF_EXISTS").unwrap(),
            ),
            BackendResponse::OkUnit
        );
        assert_eq!(
            execute_request(
                &db,
                parse_request("DROP_CONSTRAINT\tperson_email_unique\tIF_EXISTS").unwrap(),
            ),
            BackendResponse::OkUnit
        );
        assert!(matches!(
            execute_request(&db, parse_request("LIST_INDEXES").unwrap()),
            BackendResponse::OkRows { count: 1, .. }
        ));

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn node_label_commands_execute_against_database() {
        let dir = temp_dir("server-node-label-command");
        let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1)).unwrap();
        let response = execute_request(
            &db,
            parse_request("CREATE_NODE\tPerson\tname=s:Alice").unwrap(),
        );
        assert_eq!(response, BackendResponse::OkNode(0));

        assert_eq!(
            execute_request(&db, parse_request("ADD_NODE_LABEL\t0\tEmployee").unwrap()),
            BackendResponse::OkUnit
        );
        assert_eq!(
            db.query(r#"MATCH (n:Employee) WHERE n.name = "Alice" RETURN n.name"#)
                .unwrap()
                .len(),
            1
        );

        assert_eq!(
            execute_request(&db, parse_request("REMOVE_NODE_LABEL\t0\tPerson").unwrap()),
            BackendResponse::OkUnit
        );
        assert!(db
            .query(r#"MATCH (n:Person) WHERE n.name = "Alice" RETURN n.name"#)
            .unwrap()
            .is_empty());

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn writes_single_line_error_response() {
        let mut output = Vec::new();
        write_response(
            &mut output,
            &BackendResponse::Err("bad\trequest".to_string()),
        )
        .unwrap();

        assert_eq!(String::from_utf8(output).unwrap(), "ERR\tbad\\trequest\n");
    }

    fn temp_dir(prefix: &str) -> PathBuf {
        let mut path = std::env::temp_dir();
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        path.push(format!("neo4r-{prefix}-{}-{nanos}", std::process::id()));
        let _ = fs::remove_dir_all(&path);
        path
    }
}
