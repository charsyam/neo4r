use crate::protocol::*;
use neo4r_db::{DatabaseResult, Neo4rDatabaseHandle, RebalancePolicy};

pub fn execute_request(db: &Neo4rDatabaseHandle, request: BackendRequest) -> BackendResponse {
    match execute_request_inner(db, request) {
        Ok(response) => response,
        Err(err) => BackendResponse::Err(err.to_string()),
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
        | BackendRequest::NegotiateReplicationPeer { .. }
        | BackendRequest::UnregisterReplicationPeer(_)
        | BackendRequest::ListReplicationPeers
        | BackendRequest::ReplicationPeerStatus { .. }
        | BackendRequest::ReplicationStatus
        | BackendRequest::ClusterRegistry
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
        BackendRequest::RoutingTable => Ok(BackendResponse::OkRoutingTable(format_routing_table(
            &db.routing_table()?,
        ))),
        BackendRequest::Capabilities => Ok(BackendResponse::OkCapabilities(
            format_protocol_capabilities(),
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
        BackendRequest::SnapshotNow => Ok(BackendResponse::OkStorageMaintenance(
            format_storage_maintenance(&db.snapshot_now()?),
        )),
        BackendRequest::RestoreSnapshot { shard_id } => Ok(BackendResponse::OkStorageMaintenance(
            format_storage_maintenance(&db.restore_snapshot(shard_id)?),
        )),
        BackendRequest::VerifyInvariants => Ok(BackendResponse::OkStorageMaintenance(
            format_storage_maintenance(&db.verify_storage_invariants()?),
        )),
        BackendRequest::RepairInvariants => Ok(BackendResponse::OkStorageMaintenance(
            format_storage_maintenance(&db.repair_storage_invariants()?),
        )),
        BackendRequest::BackupNow => Ok(BackendResponse::OkStorageMaintenance(
            format_storage_maintenance(&db.snapshot_now()?),
        )),
        BackendRequest::RaftStatus => Ok(BackendResponse::OkClusterStatus(format_raft_status(
            &db.raft_status()?,
        ))),
        BackendRequest::RaftLeaderTransfer {
            shard_id,
            transferee_id,
        } => {
            let request = db.request_raft_leader_transfer(shard_id, transferee_id)?;
            Ok(BackendResponse::OkStorageMaintenance(format!(
                "raft_leader_transfer shard_id={} transferee_id={} term={} last_log_index={} last_log_term={}",
                shard_id,
                transferee_id,
                request.term,
                request.last_log_index,
                request.last_log_term
            )))
        }
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
