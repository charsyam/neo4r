use super::metadata_types::*;
use super::staged_overlay::*;
use super::write_cypher_helpers::*;
use super::*;

impl Neo4rDatabaseHandle {
    pub fn execute_cypher_on_shard(
        &self,
        shard_id: ShardId,
        query: &str,
        params: QueryParams,
    ) -> DatabaseResult<Vec<QueryRow>> {
        self.lock()?
            .execute_cypher_on_shard(shard_id, query, &params)
    }

    pub fn execute_cypher_mutation_batch_on_shard(
        &self,
        shard_id: ShardId,
        writes: Vec<(String, QueryParams)>,
    ) -> DatabaseResult<usize> {
        self.lock()?
            .execute_cypher_mutation_batch_on_shard(shard_id, writes)
    }

    pub fn execute_cypher_mutation_batch(
        &self,
        writes: Vec<(String, QueryParams)>,
    ) -> DatabaseResult<usize> {
        self.lock()?.execute_cypher_mutation_batch(writes)
    }

    pub fn execute_staged_cypher_transaction_on_shard(
        &self,
        shard_id: ShardId,
        writes: Vec<(String, QueryParams)>,
    ) -> DatabaseResult<usize> {
        self.lock()?
            .execute_staged_cypher_transaction_on_shard(shard_id, writes)
    }

    pub fn execute_create_node_cypher_on_shard(
        &self,
        shard_id: ShardId,
        query: &str,
        params: QueryParams,
    ) -> DatabaseResult<Vec<QueryRow>> {
        self.execute_cypher_on_shard(shard_id, query, params)
    }

    pub fn write_cypher_target_shards(
        &self,
        query: &str,
        params: QueryParams,
    ) -> DatabaseResult<Vec<ShardId>> {
        self.lock()?.write_cypher_target_shards(query, &params)
    }

    pub fn query(&self, query: &str) -> DatabaseResult<Vec<QueryRow>> {
        self.query_with_options(query, QueryOptions::default())
    }

    pub fn query_with_params(
        &self,
        query: &str,
        params: QueryParams,
    ) -> DatabaseResult<Vec<QueryRow>> {
        self.query_with_params_and_options(query, params, QueryOptions::default())
    }

    pub fn query_with_options(
        &self,
        query: &str,
        options: QueryOptions,
    ) -> DatabaseResult<Vec<QueryRow>> {
        self.query_with_params_and_options(query, QueryParams::new(), options)
    }

    pub fn query_local_stale(&self, query: &str) -> DatabaseResult<Vec<QueryRow>> {
        self.query_with_options(
            query,
            QueryOptions::default().with_consistency(ReadConsistency::FollowerStale),
        )
    }

    pub fn query_local_stale_with_params(
        &self,
        query: &str,
        params: QueryParams,
    ) -> DatabaseResult<Vec<QueryRow>> {
        self.query_with_params_and_options(
            query,
            params,
            QueryOptions::default().with_consistency(ReadConsistency::FollowerStale),
        )
    }

    pub fn query_with_params_and_options(
        &self,
        query: &str,
        params: QueryParams,
        options: QueryOptions,
    ) -> DatabaseResult<Vec<QueryRow>> {
        if let Some(rows) = self.show_index_rows_for_query(query)? {
            validate_read_isolation(options.isolation);
            return Ok(rows);
        }
        if let Some(rows) = self.show_constraint_rows_for_query(query)? {
            validate_read_isolation(options.isolation);
            return Ok(rows);
        }
        self.ensure_raft_read_index(options)?;
        let snapshot = self.read_snapshot()?;
        validate_read_options(&snapshot, options)?;
        snapshot.query_with_params(query, &params)
    }

    pub fn query_shard(&self, shard_id: ShardId, query: &str) -> DatabaseResult<Vec<QueryRow>> {
        self.query_shard_with_params_and_options(
            shard_id,
            query,
            QueryParams::new(),
            QueryOptions::default(),
        )
    }

    pub fn query_local_stale_shard(
        &self,
        shard_id: ShardId,
        query: &str,
    ) -> DatabaseResult<Vec<QueryRow>> {
        self.query_shard_with_params_and_options(
            shard_id,
            query,
            QueryParams::new(),
            QueryOptions::default().with_consistency(ReadConsistency::FollowerStale),
        )
    }

    pub fn query_shard_with_params(
        &self,
        shard_id: ShardId,
        query: &str,
        params: QueryParams,
    ) -> DatabaseResult<Vec<QueryRow>> {
        self.query_shard_with_params_and_options(shard_id, query, params, QueryOptions::default())
    }

    pub fn query_shard_with_params_and_options(
        &self,
        shard_id: ShardId,
        query: &str,
        params: QueryParams,
        options: QueryOptions,
    ) -> DatabaseResult<Vec<QueryRow>> {
        {
            let database = self.lock()?;
            if shard_id >= database.shard_count() {
                return Err(DatabaseError::MissingShardLog(shard_id));
            }
            database.ensure_local_copy(shard_id)?;
        }
        self.ensure_raft_read_index(options)?;
        let snapshot = self.read_snapshot()?;
        validate_read_options(&snapshot, options)?;
        snapshot.query_shard_with_params(shard_id, query, &params)
    }

    pub fn query_cursor(&self, query: &str) -> DatabaseResult<Box<dyn QueryCursor>> {
        self.query_cursor_with_options(query, QueryOptions::default())
    }

    pub fn query_cursor_with_params(
        &self,
        query: &str,
        params: QueryParams,
    ) -> DatabaseResult<Box<dyn QueryCursor>> {
        self.query_cursor_with_params_and_options(query, params, QueryOptions::default())
    }

    pub fn query_cursor_with_options(
        &self,
        query: &str,
        options: QueryOptions,
    ) -> DatabaseResult<Box<dyn QueryCursor>> {
        self.query_cursor_with_params_and_options(query, QueryParams::new(), options)
    }

    pub fn query_cursor_with_params_and_options(
        &self,
        query: &str,
        params: QueryParams,
        options: QueryOptions,
    ) -> DatabaseResult<Box<dyn QueryCursor>> {
        if let Some(rows) = self.show_index_rows_for_query(query)? {
            validate_read_isolation(options.isolation);
            return Ok(Box::new(VecQueryCursor::new(rows)));
        }
        if let Some(rows) = self.show_constraint_rows_for_query(query)? {
            validate_read_isolation(options.isolation);
            return Ok(Box::new(VecQueryCursor::new(rows)));
        }
        self.ensure_raft_read_index(options)?;
        let snapshot = self.read_snapshot()?;
        validate_read_options(&snapshot, options)?;
        snapshot.into_query_cursor_with_params(query, params)
    }

    pub fn query_cursor_with_staged_writes(
        &self,
        query: &str,
        params: QueryParams,
        options: QueryOptions,
        staged_writes: &[(String, QueryParams)],
    ) -> DatabaseResult<Box<dyn QueryCursor>> {
        if let Some(rows) = self.show_index_rows_for_query(query)? {
            validate_read_isolation(options.isolation);
            return Ok(Box::new(VecQueryCursor::new(rows)));
        }
        if let Some(rows) = self.show_constraint_rows_for_query(query)? {
            validate_read_isolation(options.isolation);
            return Ok(Box::new(VecQueryCursor::new(rows)));
        }
        self.ensure_raft_read_index(options)?;
        let snapshot = self.read_snapshot()?;
        validate_read_options(&snapshot, options)?;
        let overlay = snapshot.build_staged_overlay(staged_writes)?;
        let graph = StagedOverlayGraph {
            base: &snapshot.store,
            node_overlay: &overlay.nodes,
            relationship_overlay: &overlay.relationships,
        };
        Ok(Box::new(VecQueryCursor::new(
            snapshot
                .query_engine
                .execute_with_params(&graph, query, &params)?,
        )))
    }

    pub fn query_shard_with_staged_writes(
        &self,
        shard_id: ShardId,
        query: &str,
        params: QueryParams,
        options: QueryOptions,
        staged_writes: &[(String, QueryParams)],
    ) -> DatabaseResult<Vec<QueryRow>> {
        {
            let database = self.lock()?;
            if shard_id >= database.shard_count() {
                return Err(DatabaseError::MissingShardLog(shard_id));
            }
            database.ensure_local_copy(shard_id)?;
        }
        if let Some(rows) = self.show_index_rows_for_query(query)? {
            validate_read_isolation(options.isolation);
            return Ok(rows);
        }
        if let Some(rows) = self.show_constraint_rows_for_query(query)? {
            validate_read_isolation(options.isolation);
            return Ok(rows);
        }
        self.ensure_raft_read_index(options)?;
        let snapshot = self.read_snapshot()?;
        validate_read_options(&snapshot, options)?;
        snapshot.query_shard_with_staged_writes(shard_id, query, &params, staged_writes)
    }

    pub fn read_snapshot(&self) -> DatabaseResult<Neo4rReadSnapshot> {
        self.lock()?.read_snapshot()
    }

    pub(super) fn ensure_raft_read_index(&self, options: QueryOptions) -> DatabaseResult<()> {
        if options.consistency != ReadConsistency::Strong {
            return Ok(());
        }
        self.lock()?.ensure_raft_read_index()
    }

    pub fn begin_read_transaction(&self) -> DatabaseResult<Neo4rReadTransaction> {
        self.begin_read_transaction_with_options(
            QueryOptions::default().with_isolation(ReadIsolation::Snapshot),
        )
    }

    pub fn begin_read_transaction_with_options(
        &self,
        options: QueryOptions,
    ) -> DatabaseResult<Neo4rReadTransaction> {
        self.ensure_raft_read_index(options)?;
        let snapshot = self.read_snapshot()?;
        validate_read_options(&snapshot, options)?;
        Ok(Neo4rReadTransaction { snapshot, options })
    }

    pub fn apply_replicated_entry(&self, entry: LogEntry) -> DatabaseResult<()> {
        expect_unit(self.send_write(WriteOperation::ApplyReplicatedEntry(entry))?)
    }

    pub fn apply_replicated_entries(&self, entries: Vec<LogEntry>) -> DatabaseResult<()> {
        expect_unit(self.send_write(WriteOperation::ApplyReplicatedEntries(entries))?)
    }

    pub fn apply_raft_append_entries(
        &self,
        shard_id: ShardId,
        entries: Vec<LogEntry>,
        leader_commit: LogIndex,
    ) -> DatabaseResult<()> {
        expect_unit(self.send_write(WriteOperation::ApplyRaftAppendEntries {
            shard_id,
            entries,
            leader_commit,
        })?)
    }

    pub fn apply_raft_append_entries_with_response(
        &self,
        shard_id: ShardId,
        entries: Vec<LogEntry>,
        leader_commit: LogIndex,
    ) -> DatabaseResult<AppendEntriesResponse> {
        self.lock()?
            .apply_raft_append_entries_with_response(shard_id, entries, leader_commit)
    }

    pub fn request_raft_vote(
        &self,
        shard_id: ShardId,
        request: RequestVoteRequest,
    ) -> DatabaseResult<RequestVoteResponse> {
        self.lock()?.request_raft_vote(shard_id, request)
    }

    pub fn request_raft_pre_vote(
        &self,
        shard_id: ShardId,
        request: PreVoteRequest,
    ) -> DatabaseResult<PreVoteResponse> {
        self.lock()?.request_raft_pre_vote(shard_id, request)
    }

    pub fn raft_pre_vote_request(&self, shard_id: ShardId) -> DatabaseResult<PreVoteRequest> {
        self.lock()?.raft_pre_vote_request(shard_id)
    }

    pub fn request_raft_leader_transfer(
        &self,
        shard_id: ShardId,
        transferee_id: ServerId,
    ) -> DatabaseResult<RequestVoteRequest> {
        self.lock()?
            .request_raft_leader_transfer(shard_id, transferee_id)
    }

    pub fn install_raft_snapshot(
        &self,
        request: InstallSnapshotRequest,
    ) -> DatabaseResult<InstallSnapshotResponse> {
        self.lock()?.install_raft_snapshot(request)
    }

    pub fn install_snapshot_request_for_shard(
        &self,
        shard_id: ShardId,
    ) -> DatabaseResult<Option<InstallSnapshotRequest>> {
        self.lock()?.install_snapshot_request_for_shard(shard_id)
    }

    pub fn start_raft_election(&self, shard_id: ShardId) -> DatabaseResult<RequestVoteRequest> {
        self.lock()?.start_raft_election(shard_id)
    }

    pub fn record_raft_vote_response(
        &self,
        shard_id: ShardId,
        voter_id: ServerId,
        response: RequestVoteResponse,
    ) -> DatabaseResult<bool> {
        self.lock()?
            .record_raft_vote_response(shard_id, voter_id, response)
    }

    pub fn local_raft_shards(&self) -> DatabaseResult<Vec<ShardId>> {
        self.lock()?.local_raft_shards()
    }

    pub fn raft_status(&self) -> DatabaseResult<Vec<RaftShardStatus>> {
        self.lock()?.raft_status()
    }

    pub fn raft_election_candidates(&self, timeout: Duration) -> DatabaseResult<Vec<ShardId>> {
        self.lock()?.raft_election_candidates(timeout)
    }

    pub fn shard_count(&self) -> DatabaseResult<u64> {
        Ok(self.lock()?.shard_count())
    }

    pub fn local_partition_count(&self) -> DatabaseResult<usize> {
        Ok(self.lock()?.local_partition_count())
    }

    pub fn data_dir(&self) -> DatabaseResult<PathBuf> {
        Ok(self.lock()?.data_dir().to_path_buf())
    }

    pub fn query_route(&self) -> DatabaseResult<QueryRoute> {
        Ok(self.lock()?.query_route())
    }

    pub fn query_plan(&self, query: &str) -> DatabaseResult<DistributedQueryPlan> {
        self.query_plan_with_params(query, QueryParams::new())
    }

    pub fn query_plan_with_params(
        &self,
        query: &str,
        params: QueryParams,
    ) -> DatabaseResult<DistributedQueryPlan> {
        Ok(self.lock()?.query_plan(query, &params))
    }

    pub fn profile_query(&self, query: &str, params: QueryParams) -> DatabaseResult<QueryProfile> {
        let planning_start = Instant::now();
        let plan = self.query_plan_with_params(query, params.clone())?;
        let planning_elapsed_micros = planning_start.elapsed().as_micros();

        let before_cache = self.lock()?.read_cache_stats()?;
        let execution_start = Instant::now();
        let rows = self.query_with_params(query, params)?;
        let execution_elapsed_micros = execution_start.elapsed().as_micros();
        let database = self.lock()?;
        let statistics = database.statistics_catalog()?;
        let after_cache = database.read_cache_stats()?;

        Ok(QueryProfile {
            operators: vec![query_operator_profile(
                &plan.access_plan,
                plan.estimated_rows,
                rows.len(),
                execution_elapsed_micros,
            )],
            metrics: QueryMetrics {
                planning_elapsed_micros,
                execution_elapsed_micros,
                rows_returned: rows.len(),
                scanned_nodes: estimated_scanned_nodes(&statistics, &plan.access_plan),
                scanned_relationships: estimated_scanned_relationships(
                    &statistics,
                    &plan.access_plan,
                ),
                index_count: database.index_catalog.indexes.len(),
                read_cache_hits: after_cache.hits.saturating_sub(before_cache.hits),
                read_cache_misses: after_cache.misses.saturating_sub(before_cache.misses),
                index_cache_hits: after_cache
                    .index_hits
                    .saturating_sub(before_cache.index_hits),
                index_cache_misses: after_cache
                    .index_misses
                    .saturating_sub(before_cache.index_misses),
            },
            plan,
        })
    }
}
