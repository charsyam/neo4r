use super::metadata_types::*;
use super::staged_overlay::*;
use super::write_cypher_helpers::*;
use super::write_cypher_model::*;
use super::*;

impl Neo4rDatabaseHandle {
    pub fn storage_status(&self) -> DatabaseResult<StorageStatus> {
        self.lock()?.storage_status()
    }

    pub fn statistics_catalog(&self) -> DatabaseResult<StatisticsCatalog> {
        self.lock()?.statistics_catalog()
    }

    pub fn checkpoint_now(&self) -> DatabaseResult<StorageMaintenanceResult> {
        self.lock()?.checkpoint_now()
    }

    pub fn compact_storage(&self) -> DatabaseResult<StorageMaintenanceResult> {
        self.lock()?.compact_storage()
    }

    pub fn snapshot_now(&self) -> DatabaseResult<StorageMaintenanceResult> {
        self.lock()?.snapshot_now()
    }

    pub fn restore_snapshot(&self, shard_id: ShardId) -> DatabaseResult<StorageMaintenanceResult> {
        self.lock()?.restore_snapshot(shard_id)
    }

    pub fn restore_to_timestamp(
        &self,
        target: HybridTimestamp,
    ) -> DatabaseResult<StorageMaintenanceResult> {
        self.lock()?.restore_to_timestamp(target)
    }

    pub fn verify_storage_invariants(&self) -> DatabaseResult<StorageMaintenanceResult> {
        self.lock()?.verify_storage_invariants()
    }

    pub fn repair_storage_invariants(&self) -> DatabaseResult<StorageMaintenanceResult> {
        self.lock()?.repair_storage_invariants()
    }

    pub fn metadata_operations(&self) -> DatabaseResult<Vec<MetadataOperationRecord>> {
        self.lock()?.metadata_operations()
    }

    pub fn cluster_status(&self) -> DatabaseResult<ClusterStatus> {
        Ok(self.lock()?.cluster_status())
    }

    pub fn replication_node_identity(&self) -> DatabaseResult<ReplicationNodeIdentity> {
        let database = self.lock()?;
        let data_dir = database.data_dir();
        let cluster_id = data_dir
            .parent()
            .map(|parent| parent.display().to_string())
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| "default-cluster".to_string());
        Ok(ReplicationNodeIdentity {
            server_id: database.cluster_status().server_id,
            node_id: database.cluster_status().server_id,
            cluster_id,
            database_id: "default".to_string(),
            transports: vec![crate::ReplicationChannelKind::Tcp],
        })
    }

    pub fn replication_channel_metrics(
        &self,
    ) -> DatabaseResult<Option<ReplicationChannelMetricsSnapshot>> {
        Ok(self.lock()?.replication_channel_metrics())
    }

    pub fn run_replication_pump(&self) -> DatabaseResult<usize> {
        let replicator = self.lock()?.replicator.clone();
        replicator.run_replication_pump(self)
    }

    pub fn install_routing_table(&self, routing_table: ShardRoutingTable) -> DatabaseResult<()> {
        self.lock()?.install_routing_table(routing_table)
    }

    pub fn register_replication_peer(
        &self,
        server_id: ServerId,
        address: impl Into<String>,
    ) -> DatabaseResult<()> {
        self.lock()?
            .register_replication_peer(server_id, address.into())
    }

    pub fn register_replication_peer_endpoint(
        &self,
        server_id: ServerId,
        endpoint: ReplicationEndpoint,
    ) -> DatabaseResult<()> {
        self.lock()?
            .register_replication_peer_endpoint(server_id, endpoint)
    }

    pub fn unregister_replication_peer(&self, server_id: ServerId) -> DatabaseResult<()> {
        self.lock()?.unregister_replication_peer(server_id)
    }

    pub fn routing_table(&self) -> DatabaseResult<ShardRoutingTable> {
        Ok(self.lock()?.routing_table().clone())
    }

    pub fn register_cluster_node(
        &self,
        server_id: ServerId,
        address: impl Into<String>,
    ) -> DatabaseResult<ClusterMembership> {
        self.lock()?
            .register_cluster_node(server_id, address.into())
    }

    pub fn request_cluster_join(
        &self,
        server_id: ServerId,
        address: impl Into<String>,
        protocol_version: u64,
        storage_version: u64,
        shard_count: u64,
    ) -> DatabaseResult<ClusterMembership> {
        self.lock()?.request_cluster_join(
            server_id,
            address.into(),
            protocol_version,
            storage_version,
            shard_count,
        )
    }

    pub fn accept_cluster_join(&self, server_id: ServerId) -> DatabaseResult<ClusterMembership> {
        self.lock()?.accept_cluster_join(server_id)
    }

    pub fn reject_cluster_join(
        &self,
        server_id: ServerId,
        reason: impl Into<String>,
    ) -> DatabaseResult<ClusterMembership> {
        self.lock()?.reject_cluster_join(server_id, reason.into())
    }

    pub fn decommission_cluster_node(
        &self,
        server_id: ServerId,
    ) -> DatabaseResult<ClusterMembership> {
        self.lock()?.decommission_cluster_node(server_id)
    }

    pub fn cluster_membership(&self) -> DatabaseResult<ClusterMembership> {
        Ok(self.lock()?.cluster_membership().clone())
    }

    pub fn cluster_metadata(&self) -> DatabaseResult<ClusterMetadataState> {
        Ok(self.lock()?.cluster_metadata().clone())
    }

    pub fn set_metadata_authority(
        &self,
        server_id: ServerId,
    ) -> DatabaseResult<ClusterMetadataState> {
        self.lock()?.set_metadata_authority(server_id)
    }

    pub fn set_rebalance_policy(
        &self,
        policy: RebalancePolicy,
    ) -> DatabaseResult<ClusterMetadataState> {
        self.lock()?.set_rebalance_policy(policy)
    }

    pub fn plan_rebalance(&self) -> DatabaseResult<RebalancePlan> {
        self.lock()?.plan_rebalance()
    }

    pub fn start_rebalance_plan(&self) -> DatabaseResult<RebalanceExecution> {
        self.lock()?.start_rebalance_plan()
    }

    pub fn cancel_rebalance_plan(&self) -> DatabaseResult<RebalanceExecution> {
        self.lock()?.cancel_rebalance_plan()
    }

    pub fn rebalance_status(&self) -> DatabaseResult<Option<RebalanceExecution>> {
        Ok(self.lock()?.rebalance_status().cloned())
    }

    pub fn advance_rebalance(&self) -> DatabaseResult<RebalanceAdvanceResult> {
        self.lock()?.advance_rebalance()
    }

    pub fn cluster_management_status(&self) -> DatabaseResult<ClusterManagementStatus> {
        Ok(self.lock()?.cluster_management_status())
    }

    pub fn apply_rebalance_step(&self, step: RebalanceStep) -> DatabaseResult<ShardRoutingTable> {
        self.lock()?.apply_rebalance_step(step)
    }

    pub fn prepare_rebalance_step(&self, step: RebalanceStep) -> DatabaseResult<ClusterMembership> {
        self.lock()?.prepare_rebalance_step(step)
    }

    pub fn mark_shard_caught_up(
        &self,
        shard_id: ShardId,
        server_id: ServerId,
        match_index: LogIndex,
    ) -> DatabaseResult<ClusterMembership> {
        self.lock()?
            .mark_shard_caught_up(shard_id, server_id, match_index)
    }

    pub fn committed_indexes(&self) -> DatabaseResult<Vec<LogIndex>> {
        Ok(self.lock()?.committed_indexes())
    }

    pub fn log_entries_from(
        &self,
        shard_id: ShardId,
        start_index: LogIndex,
    ) -> DatabaseResult<Vec<LogEntry>> {
        self.lock()?.log_entries_from(shard_id, start_index)
    }

    pub fn create_node_property_index(
        &self,
        name: impl Into<String>,
        label: impl Into<String>,
        property: impl Into<String>,
    ) -> DatabaseResult<()> {
        self.lock()?
            .create_node_property_index(name, label, property)
    }

    pub fn create_node_property_index_if_not_exists(
        &self,
        name: impl Into<String>,
        label: impl Into<String>,
        property: impl Into<String>,
    ) -> DatabaseResult<()> {
        self.lock()?
            .create_node_property_index_if_not_exists(name, label, property)
    }

    pub fn create_unique_node_property_constraint(
        &self,
        name: impl Into<String>,
        label: impl Into<String>,
        property: impl Into<String>,
    ) -> DatabaseResult<()> {
        self.lock()?
            .create_unique_node_property_constraint(name, label, property)
    }

    pub fn create_unique_node_property_constraint_if_not_exists(
        &self,
        name: impl Into<String>,
        label: impl Into<String>,
        property: impl Into<String>,
    ) -> DatabaseResult<()> {
        self.lock()?
            .create_unique_node_property_constraint_if_not_exists(name, label, property)
    }

    pub fn create_vector_index(
        &self,
        name: impl Into<String>,
        label: impl Into<String>,
        property: impl Into<String>,
        dimensions: usize,
        metric: impl Into<String>,
    ) -> DatabaseResult<()> {
        self.lock()?
            .create_vector_index(name, label, property, dimensions, metric)
    }

    pub fn create_vector_index_if_not_exists(
        &self,
        name: impl Into<String>,
        label: impl Into<String>,
        property: impl Into<String>,
        dimensions: usize,
        metric: impl Into<String>,
    ) -> DatabaseResult<()> {
        self.lock()?
            .create_vector_index_if_not_exists(name, label, property, dimensions, metric)
    }

    pub fn drop_index(&self, name: &str) -> DatabaseResult<()> {
        self.lock()?.drop_index(name)
    }

    pub fn drop_index_if_exists(&self, name: &str) -> DatabaseResult<()> {
        self.lock()?.drop_index_if_exists(name)
    }

    pub fn drop_constraint(&self, name: &str) -> DatabaseResult<()> {
        self.lock()?.drop_constraint(name)
    }

    pub fn drop_constraint_if_exists(&self, name: &str) -> DatabaseResult<()> {
        self.lock()?.drop_constraint_if_exists(name)
    }

    pub fn list_indexes(&self) -> DatabaseResult<Vec<IndexDefinition>> {
        Ok(self.lock()?.list_indexes())
    }

    pub fn index_lifecycle_status(&self) -> DatabaseResult<Vec<IndexLifecycleStatus>> {
        self.lock()?.index_lifecycle_status()
    }

    pub fn show_indexes(&self) -> DatabaseResult<Vec<QueryRow>> {
        Ok(format_index_rows(&self.list_indexes()?))
    }

    pub fn show_index(&self, name: &str) -> DatabaseResult<Vec<QueryRow>> {
        Ok(vec![format_index_row_by_name(&self.list_indexes()?, name)?])
    }

    pub fn show_vector_indexes(&self) -> DatabaseResult<Vec<QueryRow>> {
        Ok(format_vector_index_rows(&self.list_indexes()?))
    }

    pub fn show_vector_index(&self, name: &str) -> DatabaseResult<Vec<QueryRow>> {
        Ok(vec![format_vector_index_row_by_name(
            &self.list_indexes()?,
            name,
        )?])
    }

    pub fn show_vector_index_status(&self) -> DatabaseResult<Vec<QueryRow>> {
        Ok(format_vector_index_status_rows(
            &self.vector_index_status()?,
        ))
    }

    pub fn show_vector_index_status_by_name(&self, name: &str) -> DatabaseResult<Vec<QueryRow>> {
        Ok(format_vector_index_status_rows(&[
            self.vector_index_status_by_name(name)?
        ]))
    }

    pub(super) fn show_index_rows_for_query(
        &self,
        query: &str,
    ) -> DatabaseResult<Option<Vec<QueryRow>>> {
        if let Some(name) = show_vector_index_status_name(query)? {
            Ok(Some(self.show_vector_index_status_by_name(&name)?))
        } else if is_show_vector_index_status_cypher(query) {
            Ok(Some(self.show_vector_index_status()?))
        } else if let Some(name) = show_vector_index_name(query)? {
            Ok(Some(self.show_vector_index(&name)?))
        } else if let Some(name) = show_index_name(query)? {
            Ok(Some(self.show_index(&name)?))
        } else if is_show_vector_indexes_cypher(query) {
            Ok(Some(self.show_vector_indexes()?))
        } else if is_show_indexes_cypher(query) {
            Ok(Some(self.show_indexes()?))
        } else {
            Ok(None)
        }
    }

    pub fn show_constraints(&self) -> DatabaseResult<Vec<QueryRow>> {
        Ok(format_constraint_rows(&self.list_indexes()?))
    }

    pub fn show_constraint(&self, name: &str) -> DatabaseResult<Vec<QueryRow>> {
        Ok(vec![format_constraint_row_by_name(
            &self.list_indexes()?,
            name,
        )?])
    }

    pub(super) fn show_constraint_rows_for_query(
        &self,
        query: &str,
    ) -> DatabaseResult<Option<Vec<QueryRow>>> {
        if let Some(name) = show_constraint_name(query)? {
            Ok(Some(self.show_constraint(&name)?))
        } else if is_show_constraints_cypher(query) {
            Ok(Some(self.show_constraints()?))
        } else {
            Ok(None)
        }
    }

    pub fn index_catalog(&self) -> DatabaseResult<IndexCatalog> {
        Ok(self.lock()?.index_catalog())
    }

    pub fn install_index_catalog(&self, catalog: IndexCatalog) -> DatabaseResult<()> {
        self.lock()?.install_index_catalog(catalog)
    }

    pub fn rebuild_vector_indexes(&self) -> DatabaseResult<()> {
        self.lock()?.rebuild_vector_indexes()
    }

    pub fn rebuild_vector_index(&self, name: &str) -> DatabaseResult<()> {
        self.lock()?.rebuild_vector_index(name)
    }

    pub fn vector_index_status(&self) -> DatabaseResult<Vec<VectorIndexStatus>> {
        self.lock()?.vector_index_status()
    }

    pub fn vector_index_status_by_name(&self, name: &str) -> DatabaseResult<VectorIndexStatus> {
        self.lock()?.vector_index_status_by_name(name)
    }

    pub(super) fn lock(&self) -> DatabaseResult<MutexGuard<'_, Neo4rDatabase>> {
        self.inner.lock().map_err(|_| DatabaseError::LockPoisoned)
    }

    pub(super) fn send_write(&self, operation: WriteOperation) -> DatabaseResult<WriteResponse> {
        let (response_tx, response_rx) = mpsc::channel();
        self.writer.send(WriteRequest {
            operation,
            response: response_tx,
        })?;
        response_rx
            .recv()
            .map_err(|_| DatabaseError::WriterUnavailable)?
    }

    pub(super) fn match_node_ids(
        &self,
        matcher: &NodeMatcher,
        params: &QueryParams,
    ) -> DatabaseResult<Vec<NodeId>> {
        query_match_node_ids(
            |query| self.query_with_params(query, params.clone()),
            matcher,
        )
    }

    pub(super) fn match_relationship_ids(
        &self,
        matcher: &RelationshipMatcher,
        params: &QueryParams,
    ) -> DatabaseResult<Vec<RelationshipId>> {
        query_match_relationship_ids(
            |query| self.query_with_params(query, params.clone()),
            matcher,
        )
    }
}
