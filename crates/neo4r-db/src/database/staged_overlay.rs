use super::*;

pub(super) struct StagedOverlay {
    pub(super) nodes: HashMap<NodeId, Option<Node>>,
    pub(super) relationships: HashMap<RelationshipId, Option<Relationship>>,
    pub(super) temp_node_ids: BTreeSet<NodeId>,
    pub(super) temp_relationship_ids: BTreeSet<RelationshipId>,
}

pub(super) const STAGED_TEMP_NODE_ID_START: NodeId = NodeId::MAX;
pub(super) const STAGED_TEMP_RELATIONSHIP_ID_START: RelationshipId = RelationshipId::MAX;

pub(super) fn allocate_staged_id(next_id: &mut u64) -> DatabaseResult<u64> {
    let id = *next_id;
    *next_id = next_id.checked_sub(1).ok_or_else(|| {
        DatabaseError::InvalidConfig("staged transaction id space exhausted".to_string())
    })?;
    Ok(id)
}

pub(super) struct StagedOverlayGraph<'a> {
    pub(super) base: &'a PartitionedGraphStore<RocksKvSnapshot>,
    pub(super) node_overlay: &'a HashMap<NodeId, Option<Node>>,
    pub(super) relationship_overlay: &'a HashMap<RelationshipId, Option<Relationship>>,
}

impl GraphRead for StagedOverlayGraph<'_> {
    fn node(&self, id: NodeId) -> GraphReadResult<Option<Node>> {
        if let Some(node) = self.node_overlay.get(&id) {
            return Ok(node.clone());
        }
        self.base.node(id)
    }

    fn boundary_node(&self, id: NodeId) -> GraphReadResult<Option<BoundaryNode>> {
        self.base.boundary_node(id)
    }

    fn nodes(&self) -> GraphReadResult<Vec<Node>> {
        let mut nodes = self
            .base
            .nodes()?
            .into_iter()
            .filter(|node| !self.node_overlay.contains_key(&node.id))
            .collect::<Vec<_>>();
        nodes.extend(
            self.node_overlay
                .values()
                .filter_map(|node| node.as_ref().cloned()),
        );
        nodes.sort_by_key(|node| node.id);
        Ok(nodes)
    }

    fn node_ids(&self) -> GraphReadResult<Vec<NodeId>> {
        Ok(self.nodes()?.into_iter().map(|node| node.id).collect())
    }

    fn relationship(&self, id: RelationshipId) -> GraphReadResult<Option<Relationship>> {
        if let Some(relationship) = self.relationship_overlay.get(&id) {
            let Some(relationship) = relationship.clone() else {
                return Ok(None);
            };
            if self.relationship_has_hidden_endpoint(&relationship) {
                return Ok(None);
            }
            return Ok(Some(relationship));
        }
        let Some(relationship) = self.base.relationship(id)? else {
            return Ok(None);
        };
        if self.relationship_has_hidden_endpoint(&relationship) {
            return Ok(None);
        }
        Ok(Some(relationship))
    }

    fn node_ids_by_label(&self, label: &str) -> GraphReadResult<Vec<NodeId>> {
        let mut ids = self
            .base
            .node_ids_by_label(label)?
            .into_iter()
            .filter(|id| !self.node_overlay.contains_key(id))
            .collect::<BTreeSet<_>>();
        for node in self.node_overlay.values().filter_map(|node| node.as_ref()) {
            if node.labels.iter().any(|candidate| candidate == label) {
                ids.insert(node.id);
            }
        }
        Ok(ids.into_iter().collect())
    }

    fn node_ids_by_label_property(
        &self,
        label: &str,
        property_key: &str,
        property_value: &Value,
    ) -> GraphReadResult<Vec<NodeId>> {
        let mut ids = self
            .base
            .node_ids_by_label_property(label, property_key, property_value)?
            .into_iter()
            .filter(|id| !self.node_overlay.contains_key(id))
            .collect::<BTreeSet<_>>();
        for node in self.node_overlay.values().filter_map(|node| node.as_ref()) {
            let has_label = node.labels.iter().any(|candidate| candidate == label);
            if has_label && node.properties.get(property_key) == Some(property_value) {
                ids.insert(node.id);
            }
        }
        Ok(ids.into_iter().collect())
    }

    fn boundary_node_ids_by_label_property(
        &self,
        label: &str,
        property_key: &str,
        property_value: &Value,
    ) -> GraphReadResult<Vec<NodeId>> {
        self.base
            .boundary_node_ids_by_label_property(label, property_key, property_value)
    }

    fn outgoing(&self, node_id: NodeId) -> GraphReadResult<Vec<Relationship>> {
        self.ensure_overlay_node_visible(node_id)?;
        self.overlay_relationships(self.base.outgoing(node_id)?, |relationship| {
            relationship.from == node_id
        })
    }

    fn outgoing_by_type(
        &self,
        node_id: NodeId,
        rel_type: &str,
    ) -> GraphReadResult<Vec<Relationship>> {
        self.ensure_overlay_node_visible(node_id)?;
        self.overlay_relationships(
            self.base.outgoing_by_type(node_id, rel_type)?,
            |relationship| relationship.from == node_id && relationship.rel_type == rel_type,
        )
    }

    fn incoming(&self, node_id: NodeId) -> GraphReadResult<Vec<Relationship>> {
        self.ensure_overlay_node_visible(node_id)?;
        self.overlay_relationships(self.base.incoming(node_id)?, |relationship| {
            relationship.to == node_id
        })
    }

    fn incoming_by_type(
        &self,
        node_id: NodeId,
        rel_type: &str,
    ) -> GraphReadResult<Vec<Relationship>> {
        self.ensure_overlay_node_visible(node_id)?;
        self.overlay_relationships(
            self.base.incoming_by_type(node_id, rel_type)?,
            |relationship| relationship.to == node_id && relationship.rel_type == rel_type,
        )
    }
}

impl StagedOverlayGraph<'_> {
    pub(super) fn ensure_overlay_node_visible(&self, node_id: NodeId) -> GraphReadResult<()> {
        if matches!(self.node_overlay.get(&node_id), Some(None)) {
            Err(GraphReadError::Store(format!(
                "node {node_id} is hidden by staged transaction overlay"
            )))
        } else {
            Ok(())
        }
    }

    pub(super) fn overlay_relationships(
        &self,
        relationships: Vec<Relationship>,
        include_overlay: impl Fn(&Relationship) -> bool,
    ) -> GraphReadResult<Vec<Relationship>> {
        let mut merged = relationships
            .into_iter()
            .filter_map(
                |relationship| match self.relationship_overlay.get(&relationship.id) {
                    Some(Some(relationship)) => Some(relationship.clone()),
                    Some(None) => None,
                    None if self.relationship_has_hidden_endpoint(&relationship) => None,
                    None => Some(relationship),
                },
            )
            .collect::<Vec<_>>();
        let existing_ids = merged
            .iter()
            .map(|relationship| relationship.id)
            .collect::<BTreeSet<_>>();
        merged.extend(
            self.relationship_overlay
                .iter()
                .filter(|(id, _)| !existing_ids.contains(id))
                .filter_map(|(_, relationship)| relationship.as_ref())
                .filter(|relationship| {
                    include_overlay(relationship)
                        && !self.relationship_has_hidden_endpoint(relationship)
                })
                .cloned(),
        );
        merged.sort_by_key(|relationship| relationship.id);
        Ok(merged)
    }

    fn relationship_has_hidden_endpoint(&self, relationship: &Relationship) -> bool {
        matches!(self.node_overlay.get(&relationship.from), Some(None))
            || matches!(self.node_overlay.get(&relationship.to), Some(None))
    }
}

pub(super) struct ShardScopedGraphRead<'a, G: ?Sized> {
    graph: &'a G,
    shard_map: ShardMap,
    shard_id: ShardId,
}

impl<'a, G: ?Sized> ShardScopedGraphRead<'a, G> {
    pub(super) fn new(
        graph: &'a G,
        shard_map: ShardMap,
        shard_id: ShardId,
    ) -> DatabaseResult<Self> {
        if shard_id >= shard_map.shard_count() {
            return Err(DatabaseError::MissingShardLog(shard_id));
        }
        Ok(Self {
            graph,
            shard_map,
            shard_id,
        })
    }

    pub(super) fn owns_node(&self, id: NodeId) -> bool {
        self.shard_map.owner_of_node(id) == self.shard_id
    }

    pub(super) fn owns_relationship(&self, relationship: &Relationship) -> bool {
        self.shard_map.owner_of_relationship(
            relationship.from,
            relationship.to,
            &relationship.rel_type,
        ) == self.shard_id
    }
}

impl<G: GraphRead + ?Sized> GraphRead for ShardScopedGraphRead<'_, G> {
    fn node(&self, id: NodeId) -> GraphReadResult<Option<Node>> {
        if self.owns_node(id) {
            self.graph.node(id)
        } else {
            Ok(None)
        }
    }

    fn boundary_node(&self, id: NodeId) -> GraphReadResult<Option<BoundaryNode>> {
        self.graph.boundary_node(id)
    }

    fn nodes(&self) -> GraphReadResult<Vec<Node>> {
        Ok(self
            .graph
            .nodes()?
            .into_iter()
            .filter(|node| self.owns_node(node.id))
            .collect())
    }

    fn node_ids(&self) -> GraphReadResult<Vec<NodeId>> {
        Ok(self
            .graph
            .node_ids()?
            .into_iter()
            .filter(|id| self.owns_node(*id))
            .collect())
    }

    fn relationship(&self, id: RelationshipId) -> GraphReadResult<Option<Relationship>> {
        Ok(self
            .graph
            .relationship(id)?
            .filter(|relationship| self.owns_relationship(relationship)))
    }

    fn node_ids_by_label(&self, label: &str) -> GraphReadResult<Vec<NodeId>> {
        Ok(self
            .graph
            .node_ids_by_label(label)?
            .into_iter()
            .filter(|id| self.owns_node(*id))
            .collect())
    }

    fn node_ids_by_label_property(
        &self,
        label: &str,
        property_key: &str,
        property_value: &Value,
    ) -> GraphReadResult<Vec<NodeId>> {
        Ok(self
            .graph
            .node_ids_by_label_property(label, property_key, property_value)?
            .into_iter()
            .filter(|id| self.owns_node(*id))
            .collect())
    }

    fn boundary_node_ids_by_label_property(
        &self,
        label: &str,
        property_key: &str,
        property_value: &Value,
    ) -> GraphReadResult<Vec<NodeId>> {
        self.graph
            .boundary_node_ids_by_label_property(label, property_key, property_value)
    }

    fn outgoing(&self, node_id: NodeId) -> GraphReadResult<Vec<Relationship>> {
        if !self.owns_node(node_id) {
            return Ok(Vec::new());
        }
        Ok(self
            .graph
            .outgoing(node_id)?
            .into_iter()
            .filter(|relationship| self.owns_relationship(relationship))
            .collect())
    }

    fn outgoing_by_type(
        &self,
        node_id: NodeId,
        rel_type: &str,
    ) -> GraphReadResult<Vec<Relationship>> {
        if !self.owns_node(node_id) {
            return Ok(Vec::new());
        }
        Ok(self
            .graph
            .outgoing_by_type(node_id, rel_type)?
            .into_iter()
            .filter(|relationship| self.owns_relationship(relationship))
            .collect())
    }

    fn incoming(&self, node_id: NodeId) -> GraphReadResult<Vec<Relationship>> {
        Ok(self
            .graph
            .incoming(node_id)?
            .into_iter()
            .filter(|relationship| self.owns_relationship(relationship))
            .collect())
    }

    fn incoming_by_type(
        &self,
        node_id: NodeId,
        rel_type: &str,
    ) -> GraphReadResult<Vec<Relationship>> {
        Ok(self
            .graph
            .incoming_by_type(node_id, rel_type)?
            .into_iter()
            .filter(|relationship| self.owns_relationship(relationship))
            .collect())
    }
}

impl Neo4rReadTransaction {
    pub fn timestamp(&self) -> HybridTimestamp {
        self.snapshot.timestamp()
    }

    pub fn applied_indexes(&self) -> &[LogIndex] {
        self.snapshot.applied_indexes()
    }

    pub fn committed_indexes(&self) -> &[LogIndex] {
        self.snapshot.committed_indexes()
    }

    pub fn options(&self) -> QueryOptions {
        self.options
    }

    pub fn query(&self, query: &str) -> DatabaseResult<Vec<QueryRow>> {
        self.snapshot.query(query)
    }

    pub fn query_with_params(
        &self,
        query: &str,
        params: &QueryParams,
    ) -> DatabaseResult<Vec<QueryRow>> {
        self.snapshot.query_with_params(query, params)
    }

    pub fn query_shard_with_params(
        &self,
        shard_id: ShardId,
        query: &str,
        params: &QueryParams,
    ) -> DatabaseResult<Vec<QueryRow>> {
        self.snapshot
            .query_shard_with_params(shard_id, query, params)
    }

    pub fn query_cursor(&self, query: &str) -> DatabaseResult<Box<dyn QueryCursor>> {
        self.snapshot.query_cursor(query)
    }

    pub fn query_cursor_with_params(
        &self,
        query: &str,
        params: &QueryParams,
    ) -> DatabaseResult<Box<dyn QueryCursor>> {
        self.snapshot.query_cursor_with_params(query, params)
    }
}

pub(super) struct WriteRequest {
    pub(super) operation: WriteOperation,
    pub(super) response: mpsc::Sender<DatabaseResult<WriteResponse>>,
}

pub(super) struct WriterActor {
    sender: Mutex<Option<mpsc::Sender<WriteRequest>>>,
    join: Mutex<Option<thread::JoinHandle<()>>>,
}

impl WriterActor {
    pub(super) fn send(&self, request: WriteRequest) -> DatabaseResult<()> {
        let sender = self
            .sender
            .lock()
            .map_err(|_| DatabaseError::LockPoisoned)?
            .as_ref()
            .cloned()
            .ok_or(DatabaseError::WriterUnavailable)?;
        sender
            .send(request)
            .map_err(|_| DatabaseError::WriterUnavailable)
    }
}

impl Drop for WriterActor {
    fn drop(&mut self) {
        if let Ok(mut sender) = self.sender.lock() {
            sender.take();
        }
        if let Ok(mut join) = self.join.lock() {
            if let Some(join) = join.take() {
                let _ = join.join();
            }
        }
    }
}

pub(super) enum WriteOperation {
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
        from: NodeId,
        to: NodeId,
        rel_type: String,
        properties: Properties,
    },
    SetNodeProperty {
        id: NodeId,
        key: String,
        value: Value,
    },
    RemoveNodeProperty {
        id: NodeId,
        key: String,
    },
    AddNodeLabel {
        id: NodeId,
        label: String,
    },
    RemoveNodeLabel {
        id: NodeId,
        label: String,
    },
    SetRelationshipProperty {
        id: RelationshipId,
        key: String,
        value: Value,
    },
    RemoveRelationshipProperty {
        id: RelationshipId,
        key: String,
    },
    DeleteRelationship {
        id: RelationshipId,
    },
    DeleteNode {
        id: NodeId,
    },
    ApplyReplicatedEntry(LogEntry),
    ApplyReplicatedEntries(Vec<LogEntry>),
    ApplyRaftAppendEntries {
        shard_id: ShardId,
        entries: Vec<LogEntry>,
        leader_commit: LogIndex,
    },
}

#[derive(Debug)]
pub(super) enum WriteResponse {
    NodeId(NodeId),
    RelationshipId(RelationshipId),
    Unit,
}

pub(super) fn spawn_writer_actor(inner: Arc<Mutex<Neo4rDatabase>>) -> Arc<WriterActor> {
    let (tx, rx) = mpsc::channel::<WriteRequest>();
    let join = thread::spawn(move || {
        while let Ok(first) = rx.recv() {
            let (max_entries, max_delay) = match group_commit_config(&inner) {
                Ok(config) => config,
                Err(err) => {
                    let _ = first.response.send(Err(err));
                    continue;
                }
            };
            let mut batch = vec![first];
            while batch.len() < max_entries {
                match rx.recv_timeout(max_delay) {
                    Ok(request) => batch.push(request),
                    Err(mpsc::RecvTimeoutError::Timeout) => break,
                    Err(mpsc::RecvTimeoutError::Disconnected) => break,
                }
            }
            execute_write_batch(&inner, batch);
        }
    });
    Arc::new(WriterActor {
        sender: Mutex::new(Some(tx)),
        join: Mutex::new(Some(join)),
    })
}

pub(super) fn group_commit_config(
    inner: &Arc<Mutex<Neo4rDatabase>>,
) -> DatabaseResult<(usize, Duration)> {
    let database = inner.lock().map_err(|_| DatabaseError::LockPoisoned)?;
    Ok((
        database.config.group_commit_max_entries.max(1),
        database.config.group_commit_max_delay,
    ))
}

pub(super) fn execute_write_batch(inner: &Arc<Mutex<Neo4rDatabase>>, batch: Vec<WriteRequest>) {
    let mut database = match inner.lock().map_err(|_| DatabaseError::LockPoisoned) {
        Ok(database) => database,
        Err(err) => {
            for request in batch {
                let _ = request.response.send(Err(error_for_batch_response(&err)));
            }
            return;
        }
    };
    let mut prepared = Vec::new();
    for request in batch {
        match request.operation {
            WriteOperation::ApplyReplicatedEntry(entry) => {
                let result = database
                    .apply_replicated_entry(entry)
                    .map(|_| WriteResponse::Unit);
                let _ = request.response.send(result);
            }
            WriteOperation::ApplyReplicatedEntries(entries) => {
                let result = database
                    .apply_replicated_entries(entries)
                    .map(|_| WriteResponse::Unit);
                let _ = request.response.send(result);
            }
            WriteOperation::ApplyRaftAppendEntries {
                shard_id,
                entries,
                leader_commit,
            } => {
                let result = database
                    .apply_raft_append_entries(shard_id, entries, leader_commit)
                    .map(|_| WriteResponse::Unit);
                let _ = request.response.send(result);
            }
            operation => match database.prepare_local_write(operation) {
                Ok(prepared_write) => prepared.push((prepared_write, request.response)),
                Err(err) => {
                    let _ = request.response.send(Err(err));
                }
            },
        }
    }
    if prepared.is_empty() {
        return;
    }
    let entries = prepared
        .iter()
        .map(|(prepared_write, _)| prepared_write.entry.clone())
        .collect::<Vec<_>>();
    let flush_result = database.flush_group_commit(&entries);
    for (prepared_write, response) in prepared {
        let result = match &flush_result {
            Ok(()) => Ok(prepared_write.response),
            Err(err) => Err(error_for_batch_response(err)),
        };
        let _ = response.send(result);
    }
}

pub(super) fn expect_unit(response: WriteResponse) -> DatabaseResult<()> {
    match response {
        WriteResponse::Unit => Ok(()),
        response => Err(unexpected_write_response(response)),
    }
}

pub(super) fn unexpected_write_response(response: WriteResponse) -> DatabaseError {
    DatabaseError::UnexpectedWriteResponse(format!("{response:?}"))
}

pub(super) fn error_for_batch_response(err: &DatabaseError) -> DatabaseError {
    match err {
        DatabaseError::InvalidConfig(message) => DatabaseError::InvalidConfig(message.clone()),
        DatabaseError::MissingShardLog(shard_id) => DatabaseError::MissingShardLog(*shard_id),
        DatabaseError::ShardNotLocal {
            shard_id,
            server_id,
        } => DatabaseError::ShardNotLocal {
            shard_id: *shard_id,
            server_id: *server_id,
        },
        DatabaseError::ShardNotPrimary {
            shard_id,
            server_id,
            primary_server_id,
        } => DatabaseError::ShardNotPrimary {
            shard_id: *shard_id,
            server_id: *server_id,
            primary_server_id: *primary_server_id,
        },
        DatabaseError::UnexpectedLogIndex {
            shard_id,
            expected,
            actual,
        } => DatabaseError::UnexpectedLogIndex {
            shard_id: *shard_id,
            expected: *expected,
            actual: *actual,
        },
        DatabaseError::LogConflict {
            shard_id,
            index,
            message,
        } => DatabaseError::LogConflict {
            shard_id: *shard_id,
            index: *index,
            message: message.clone(),
        },
        DatabaseError::Replication(message) => DatabaseError::Replication(message.clone()),
        DatabaseError::LockPoisoned => DatabaseError::LockPoisoned,
        DatabaseError::WriterUnavailable => DatabaseError::WriterUnavailable,
        DatabaseError::UnexpectedWriteResponse(message) => {
            DatabaseError::UnexpectedWriteResponse(message.clone())
        }
        DatabaseError::Graph(_)
        | DatabaseError::GraphRead(_)
        | DatabaseError::Query(_)
        | DatabaseError::Storage(_) => DatabaseError::Replication(err.to_string()),
    }
}

pub(super) struct PreparedWrite {
    pub(super) entry: LogEntry,
    pub(super) response: WriteResponse,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum QueryRoute {
    LocalOnly,
    RequiresRemoteShards(Vec<ShardId>),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RemoteTraversalPolicy {
    BoundaryCacheOnly,
    RemoteShardHop(Vec<ShardId>),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DistributedQueryPlan {
    pub route: QueryRoute,
    pub traversal_policy: RemoteTraversalPolicy,
    pub uses_boundary_cache: bool,
    pub access_plan: QueryAccessPlan,
    pub access_reason: String,
    pub cost_model_version: u32,
    pub estimated_cost: u64,
    pub estimated_rows: u64,
    pub remote_shard_count: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum QueryAccessPlan {
    NodeUniqueIndexSeek {
        label: String,
        property: String,
    },
    NodeIndexSeek {
        label: String,
        property: String,
    },
    NodeLabelScan {
        label: String,
    },
    NodeFullScan,
    VectorIndexSeek {
        label: Option<String>,
        property: String,
        metric: String,
    },
    RelationshipTypeScan {
        rel_type: String,
    },
    RelationshipScan,
    Unsupported {
        reason: String,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryProfile {
    pub plan: DistributedQueryPlan,
    pub metrics: QueryMetrics,
    pub operators: Vec<QueryOperatorProfile>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryMetrics {
    pub planning_elapsed_micros: u128,
    pub execution_elapsed_micros: u128,
    pub rows_returned: usize,
    pub scanned_nodes: usize,
    pub scanned_relationships: usize,
    pub index_count: usize,
    pub read_cache_hits: u64,
    pub read_cache_misses: u64,
    pub index_cache_hits: u64,
    pub index_cache_misses: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryOperatorProfile {
    pub name: String,
    pub estimated_rows: u64,
    pub actual_rows: usize,
    pub elapsed_micros: u128,
    pub children: Vec<QueryOperatorProfile>,
}
