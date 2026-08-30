use super::row_codec::*;
use super::*;

pub(super) fn parse_zero_arg_request(line: &str) -> Result<BackendRequest, String> {
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
        "NEGOTIATE_REPLICATION_PEER" => {
            Err("NEGOTIATE_REPLICATION_PEER requires server id".to_string())
        }
        "UNREGISTER_REPLICATION_PEER" => {
            Err("UNREGISTER_REPLICATION_PEER requires server id".to_string())
        }
        "LIST_REPLICATION_PEERS" => Ok(BackendRequest::ListReplicationPeers),
        "REPLICATION_PEER_STATUS" => Ok(BackendRequest::ReplicationPeerStatus { server_id: None }),
        "REPLICATION_STATUS" => Ok(BackendRequest::ReplicationStatus),
        "ROUTING_TABLE" => Ok(BackendRequest::RoutingTable),
        "CLUSTER_REGISTRY" => Ok(BackendRequest::ClusterRegistry),
        "CAPABILITIES" => Ok(BackendRequest::Capabilities),
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
        "SNAPSHOT_NOW" => Ok(BackendRequest::SnapshotNow),
        "RESTORE_SNAPSHOT" => Err("RESTORE_SNAPSHOT requires shard id".to_string()),
        "VERIFY_INVARIANTS" => Ok(BackendRequest::VerifyInvariants),
        "REPAIR_INVARIANTS" => Ok(BackendRequest::RepairInvariants),
        "BACKUP_NOW" => Ok(BackendRequest::BackupNow),
        "RAFT_STATUS" => Ok(BackendRequest::RaftStatus),
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
        "PROMOTE_CAUGHT_UP_NODE" => Err("PROMOTE_CAUGHT_UP_NODE requires server id".to_string()),
        "WRITE_BOOTSTRAP_MANIFEST" => {
            Err("WRITE_BOOTSTRAP_MANIFEST requires mode, cluster id, and database id".to_string())
        }
        "BOOTSTRAP_SAFETY" => {
            Err("BOOTSTRAP_SAFETY requires expected cluster id and force flag".to_string())
        }
        "TOPOLOGY_OBSERVE" => Ok(BackendRequest::TopologyObserve),
        "OPERATIONAL_SAFETY" => Err("OPERATIONAL_SAFETY requires operation".to_string()),
        "CHAOS_CHECKS" => Ok(BackendRequest::ChaosChecks),
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

pub(super) fn encode_index_definition(index: &IndexDefinition) -> String {
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

pub(super) fn decode_index_definition(input: &str) -> Result<IndexDefinition, String> {
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

pub(super) fn decode_hex_string(input: &str, name: &str) -> Result<String, String> {
    String::from_utf8(hex_decode(input)?).map_err(|_| format!("{name} is not valid UTF-8"))
}

pub(super) fn parse_install_routing_table(input: &str) -> Result<ShardRoutingTable, String> {
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

pub(super) fn parse_routing_placement(input: &str) -> Result<ShardPlacement, String> {
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

pub(super) fn parse_rebalance_step(input: &str) -> Result<RebalanceStep, String> {
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
