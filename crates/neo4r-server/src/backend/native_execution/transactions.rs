use super::*;

impl NativeExecutionContext {
    pub(crate) fn execute_transaction_command(
        &self,
        session_id: u64,
        command: TransactionCommand,
    ) -> Result<String, String> {
        match command {
            TransactionCommand::Begin { mode, isolation } => {
                let ownership_epoch = self
                    .db
                    .cluster_status()
                    .map_err(|err| err.to_string())?
                    .routing_version;
                let tx = match mode {
                    TransactionMode::ReadOnly => {
                        let tx = self
                            .db
                            .begin_read_transaction_with_options(
                                QueryOptions::default().with_isolation(isolation),
                            )
                            .map_err(|err| err.to_string())?;
                        NativeTransaction::ReadOnly(tx)
                    }
                    TransactionMode::ReadWrite => NativeTransaction::ReadWrite {
                        isolation,
                        ownership_epoch,
                        staged_writes: Vec::new(),
                        conflict_keys: BTreeSet::new(),
                    },
                };
                let tx_id = self.transactions.insert(session_id, tx);
                Ok(format!(
                    "OK\tTX_BEGIN\t{tx_id}\t{}\t{}\townership_epoch={ownership_epoch}",
                    format_transaction_mode(mode),
                    format_read_isolation(isolation)
                ))
            }
            TransactionCommand::Query {
                tx_id,
                query,
                params,
            } => {
                if is_write_cypher(&query) {
                    let staged_count = self
                        .transactions
                        .stage_write(session_id, tx_id, query, params)?;
                    Ok(format!("OK\tTX_STAGED\t{tx_id}\t{staged_count}"))
                } else {
                    let cursor = self
                        .transactions
                        .query_cursor(&self.db, session_id, tx_id, &query, &params)
                        .map_err(|err| err.to_string())?;
                    let total_rows = cursor.total_rows();
                    let cursor_id = self.cursors.insert(session_id, cursor);
                    let page = self
                        .cursors
                        .fetch(session_id, cursor_id, self.default_page_size)?;
                    Ok(format_result_start(cursor_id, total_rows, page))
                }
            }
            TransactionCommand::ExecutePrepared {
                tx_id,
                prepared_id,
                params,
            } => {
                let query = self.prepared_queries.get(session_id, prepared_id)?;
                validate_prepared_query_params(prepared_id, &query, &params)?;
                if is_write_cypher(&query) {
                    let staged_count = self
                        .transactions
                        .stage_write(session_id, tx_id, query, params)?;
                    Ok(format!("OK\tTX_STAGED\t{tx_id}\t{staged_count}"))
                } else {
                    let cursor = self
                        .transactions
                        .query_cursor(&self.db, session_id, tx_id, &query, &params)
                        .map_err(|err| err.to_string())?;
                    let total_rows = cursor.total_rows();
                    let cursor_id = self.cursors.insert(session_id, cursor);
                    let page = self
                        .cursors
                        .fetch(session_id, cursor_id, self.default_page_size)?;
                    Ok(format_result_start(cursor_id, total_rows, page))
                }
            }
            TransactionCommand::PreparedQueryPlan {
                tx_id,
                prepared_id,
                params,
            } => {
                let query = self.prepared_queries.get(session_id, prepared_id)?;
                validate_prepared_query_params(prepared_id, &query, &params)?;
                let context = self.transactions.plan_context(session_id, tx_id)?;
                let mut plan = format_query_plan(
                    &self
                        .db
                        .query_plan_with_params(&query, params)
                        .map_err(|err| err.to_string())?,
                );
                plan.push(' ');
                plan.push_str(&format_transaction_plan_context(&context));
                Ok(format_response(&BackendResponse::OkQueryPlan(plan)))
            }
            TransactionCommand::PreparedQueryRoute {
                tx_id,
                prepared_id,
                params,
            } => {
                let query = self.prepared_queries.get(session_id, prepared_id)?;
                validate_prepared_query_params(prepared_id, &query, &params)?;
                let context = self.transactions.plan_context(session_id, tx_id)?;
                Ok(format_tx_prepared_query_route(
                    tx_id,
                    prepared_id,
                    self.prepared_query_routing_hint_with_params(&query, &params)?,
                    &context,
                ))
            }
            TransactionCommand::DistributedQuery {
                tx_id,
                query,
                params,
            } => {
                if is_write_cypher(&query) {
                    return Err("TX_QUERY_DISTRIBUTED only supports read queries".to_string());
                }
                let cursor = self
                    .transactions
                    .distributed_query_cursor(
                        &self.db,
                        &self.query_peers,
                        self.read_preference,
                        session_id,
                        tx_id,
                        &query,
                        &params,
                    )
                    .map_err(|err| err.to_string())?;
                let total_rows = cursor.total_rows();
                let cursor_id = self.cursors.insert(session_id, cursor);
                let page = self
                    .cursors
                    .fetch(session_id, cursor_id, self.default_page_size)?;
                Ok(format_result_start(cursor_id, total_rows, page))
            }
            TransactionCommand::QueryPlan {
                tx_id,
                query,
                params,
            } => {
                let context = self.transactions.plan_context(session_id, tx_id)?;
                let mut plan = format_query_plan(
                    &self
                        .db
                        .query_plan_with_params(&query, params)
                        .map_err(|err| err.to_string())?,
                );
                plan.push(' ');
                plan.push_str(&format_transaction_plan_context(&context));
                Ok(format_response(&BackendResponse::OkQueryPlan(plan)))
            }
            TransactionCommand::Commit { tx_id } => {
                let staged_writes = self.transactions.staged_writes(session_id, tx_id)?;
                let transaction_writes = self.writes_by_target_shard(&staged_writes)?;
                if transaction_writes.len() > 1
                    && !staged_writes_are_prepare_batchable(&staged_writes)
                {
                    return Err(
                        "multi-shard read-write transaction commit currently requires prepare-batchable CREATE, MERGE, SET, REMOVE, or DELETE writes"
                            .to_string(),
                    );
                }
                let tx = self.transactions.close(session_id, tx_id)?;
                let write_count = staged_writes.len();
                if let NativeTransaction::ReadWrite {
                    ownership_epoch, ..
                } = tx
                {
                    let current_epoch = self
                        .db
                        .cluster_status()
                        .map_err(|err| err.to_string())?
                        .routing_version;
                    if ownership_epoch != current_epoch {
                        return Err(format!(
                            "ERR\tSTALE_EPOCH\ttx_epoch={ownership_epoch}\tcurrent_epoch={current_epoch}\trouting_version={current_epoch}\townership_epoch={current_epoch}\tretryable=true"
                        ));
                    }
                    if !self.try_execute_staged_writes_as_local_batch(
                        tx_id,
                        &transaction_writes,
                        &staged_writes,
                    )? {
                        for staged in staged_writes {
                            self.execute_write_query_with_routing(&staged.query, staged.params)?;
                        }
                    }
                }
                if write_count == 0 {
                    Ok(format!("OK\tTX_COMMIT\t{tx_id}"))
                } else {
                    Ok(format!("OK\tTX_COMMIT\t{tx_id}\t{write_count}"))
                }
            }
            TransactionCommand::Rollback { tx_id } => {
                self.transactions.close(session_id, tx_id)?;
                Ok(format!("OK\tTX_ROLLBACK\t{tx_id}"))
            }
            TransactionCommand::Kill { tx_id } => {
                let info = self.transactions.close_any(tx_id)?;
                Ok(format!("OK\tTX_KILL\t{tx_id}\t{}", info.session_id))
            }
            TransactionCommand::Status { tx_id } => {
                let info = self.transactions.status(session_id, tx_id)?;
                Ok(format_tx_status(info))
            }
            TransactionCommand::PrepareWriteBatchShard { shard_id, writes } => {
                self.ensure_local_primary_shard(shard_id)?;
                if !writes
                    .iter()
                    .all(|(query, _)| is_batchable_cypher_mutation(query))
                {
                    return Err(
                        "prepared write batch currently supports CREATE, MERGE, SET, REMOVE, or DELETE writes only"
                            .to_string(),
                    );
                }
                let write_count = writes.len();
                let prepared_id = self.prepared_transactions.prepare(shard_id, writes)?;
                Ok(format!(
                    "OK\tTX_PREPARED\t{prepared_id}\t{shard_id}\t{write_count}"
                ))
            }
            TransactionCommand::CommitPrepared { prepared_id } => {
                let prepared = self.prepared_transactions.take(prepared_id)?;
                self.db
                    .execute_staged_cypher_transaction_on_shard(prepared.shard_id, prepared.writes)
                    .map_err(|err| err.to_string())?;
                Ok(format!("OK\tTX_PREPARED_COMMIT\t{prepared_id}"))
            }
            TransactionCommand::AbortPrepared { prepared_id } => {
                self.prepared_transactions.take(prepared_id)?;
                Ok(format!("OK\tTX_PREPARED_ABORT\t{prepared_id}"))
            }
            TransactionCommand::PreparedStatus { prepared_id } => {
                let info = self.prepared_transactions.status(prepared_id)?;
                Ok(format_prepared_tx_status(info))
            }
            TransactionCommand::ListPrepared => {
                Ok(format_prepared_tx_list(self.prepared_transactions.list()?))
            }
            TransactionCommand::List => Ok(format_tx_list(self.transactions.list(session_id)?)),
            TransactionCommand::ListAll => Ok(format_tx_list_all(self.transactions.list_all()?)),
        }
    }

    pub(crate) fn ensure_local_primary_shard(&self, shard_id: u64) -> Result<(), String> {
        let status = self.db.cluster_status().map_err(|err| err.to_string())?;
        let shard = status
            .shards
            .iter()
            .find(|shard| shard.shard_id == shard_id)
            .ok_or_else(|| format!("missing shard {shard_id} in cluster status"))?;
        if shard.is_local_primary {
            Ok(())
        } else {
            Err(format!("shard {shard_id} is not a local primary"))
        }
    }

    pub(crate) fn prepared_query_routing_hint(&self, query: &str) -> Result<String, String> {
        prepared_query_routing_hint(&self.db, query)
    }

    pub(crate) fn prepared_query_routing_hint_with_params(
        &self,
        query: &str,
        params: &neo4r_query::QueryParams,
    ) -> Result<String, String> {
        prepared_query_routing_hint_with_params(&self.db, query, params)
    }

    pub(crate) fn writes_by_target_shard(
        &self,
        staged_writes: &[StagedWrite],
    ) -> Result<BTreeMap<u64, Vec<(String, neo4r_query::QueryParams)>>, String> {
        let status = self.db.cluster_status().map_err(|err| err.to_string())?;
        let mut transaction_writes: BTreeMap<u64, Vec<(String, neo4r_query::QueryParams)>> =
            BTreeMap::new();
        for staged in staged_writes {
            let shards = if is_create_node_cypher(&staged.query) {
                vec![
                    select_create_node_write_shard(&status, &staged.query, &staged.params)?
                        .shard_id,
                ]
            } else if is_merge_node_cypher(&staged.query) {
                vec![
                    select_merge_node_write_shard(&status, &staged.query, &staged.params)?.shard_id,
                ]
            } else {
                self.db
                    .write_cypher_target_shards(&staged.query, staged.params.clone())
                    .map_err(|err| err.to_string())?
            };
            if shards.is_empty() && is_staged_transaction_overlay_cypher(&staged.query) {
                continue;
            }
            if shards.len() != 1 && !is_batchable_multi_target_transaction_cypher(&staged.query) {
                return Err(format!(
                    "read-write transaction commit requires each staged write to target exactly one shard; query {:?} targets {:?}",
                    staged.query, shards
                ));
            }
            for shard_id in shards {
                transaction_writes
                    .entry(shard_id)
                    .or_default()
                    .push((staged.query.clone(), staged.params.clone()));
            }
        }
        Ok(transaction_writes)
    }

    pub(crate) fn try_execute_staged_writes_as_local_batch(
        &self,
        tx_id: u64,
        transaction_writes: &BTreeMap<u64, Vec<(String, neo4r_query::QueryParams)>>,
        staged_writes: &[StagedWrite],
    ) -> Result<bool, String> {
        if transaction_writes.is_empty() {
            return Ok(false);
        }
        if staged_writes.is_empty() {
            return Ok(false);
        }
        let status = self.db.cluster_status().map_err(|err| err.to_string())?;
        if transaction_writes.len() == 1 {
            let (shard_id, writes) = transaction_writes.iter().next().unwrap();
            let shard_id = *shard_id;
            let Some(shard) = status
                .shards
                .iter()
                .find(|shard| shard.shard_id == shard_id)
            else {
                return Err(format!("missing shard {shard_id} in cluster status"));
            };
            if shard.is_local_primary {
                let all_writes = staged_writes
                    .iter()
                    .map(|staged| (staged.query.clone(), staged.params.clone()))
                    .collect::<Vec<_>>();
                if staged_writes
                    .iter()
                    .all(|staged| is_staged_transaction_overlay_cypher(&staged.query))
                {
                    self.db
                        .execute_staged_cypher_transaction_on_shard(shard_id, all_writes)
                        .map_err(|err| err.to_string())?;
                    return Ok(true);
                }
                if staged_writes
                    .iter()
                    .all(|staged| is_batchable_transaction_set_cypher(&staged.query))
                {
                    self.db
                        .execute_cypher_mutation_batch_on_shard(shard_id, writes.clone())
                        .map_err(|err| err.to_string())?;
                    return Ok(true);
                }
                return Ok(false);
            }
            if !staged_writes_are_prepare_batchable(staged_writes) {
                return Ok(false);
            }
            let primary = shard
                .primary_server_id
                .ok_or_else(|| format!("missing primary for write shard {shard_id}"))?;
            let address = self.query_peers.address(primary)?.ok_or_else(|| {
                format!(
                    "missing query peer address for primary server {primary} on write shard {shard_id}"
                )
            })?;
            request_remote_prepare_commit_batch(&self.db, tx_id, &address, shard_id, writes)?;
            return Ok(true);
        }

        if !staged_writes_are_prepare_batchable(staged_writes) {
            return Ok(false);
        }

        let mut shard_statuses = Vec::new();
        for shard_id in transaction_writes.keys() {
            let Some(shard) = status
                .shards
                .iter()
                .find(|shard| shard.shard_id == *shard_id)
            else {
                return Err(format!("missing shard {shard_id} in cluster status"));
            };
            shard_statuses.push(shard.clone());
        }
        if shard_statuses.iter().all(|shard| shard.is_local_primary) {
            if staged_writes
                .iter()
                .all(|staged| is_batchable_transaction_set_cypher(&staged.query))
            {
                let writes = staged_writes
                    .iter()
                    .map(|staged| (staged.query.clone(), staged.params.clone()))
                    .collect();
                self.db
                    .execute_cypher_mutation_batch(writes)
                    .map_err(|err| err.to_string())?;
            } else {
                let local_participants = transaction_writes
                    .iter()
                    .map(|(shard_id, writes)| (*shard_id, writes.clone()))
                    .collect();
                self.prepare_commit_mixed_batches(tx_id, local_participants, Vec::new())?;
            }
            return Ok(true);
        }
        let mut remote_participants = Vec::new();
        let mut local_participants = Vec::new();
        for shard in shard_statuses {
            let shard_id = shard.shard_id;
            let writes = transaction_writes
                .get(&shard_id)
                .cloned()
                .unwrap_or_default();
            if shard.is_local_primary {
                local_participants.push((shard_id, writes));
                continue;
            }
            let primary = shard
                .primary_server_id
                .ok_or_else(|| format!("missing primary for write shard {shard_id}"))?;
            let address = self.query_peers.address(primary)?.ok_or_else(|| {
                format!(
                    "missing query peer address for primary server {primary} on write shard {shard_id}"
                )
            })?;
            remote_participants.push((address, shard_id, writes));
        }
        if local_participants.is_empty() {
            request_remote_prepare_commit_batches(&self.db, tx_id, remote_participants)?;
        } else {
            self.prepare_commit_mixed_batches(tx_id, local_participants, remote_participants)?;
        }
        Ok(true)
    }

    pub(crate) fn prepare_commit_mixed_batches(
        &self,
        tx_id: u64,
        local_participants: Vec<(u64, Vec<(String, neo4r_query::QueryParams)>)>,
        remote_participants: Vec<(String, u64, Vec<(String, neo4r_query::QueryParams)>)>,
    ) -> Result<(), String> {
        let mut prepared_locals = Vec::new();
        let mut prepared_remotes = Vec::new();

        for (shard_id, writes) in local_participants {
            if let Err(err) = self.ensure_local_primary_shard(shard_id) {
                record_abort_decision(
                    &self.db,
                    tx_id,
                    decision_participant_records(&prepared_locals, &prepared_remotes),
                )?;
                self.abort_local_prepared_batches(prepared_locals);
                abort_prepared_participants(prepared_remotes);
                return Err(err);
            }
            match self.prepared_transactions.prepare(shard_id, writes) {
                Ok(prepared_id) => prepared_locals.push((shard_id, prepared_id)),
                Err(err) => {
                    record_abort_decision(
                        &self.db,
                        tx_id,
                        decision_participant_records(&prepared_locals, &prepared_remotes),
                    )?;
                    self.abort_local_prepared_batches(prepared_locals);
                    abort_prepared_participants(prepared_remotes);
                    return Err(err);
                }
            }
        }

        for (address, shard_id, writes) in remote_participants {
            let mut stream = TcpStream::connect(&address)
                .map_err(|err| format!("connect write peer {address}: {err}"))?;
            let response = match request_command_on_stream(
                &mut stream,
                1,
                &format_tx_prepare_write_batch_shard_payload(shard_id, &writes),
            ) {
                Ok(response) => response,
                Err(err) => {
                    record_abort_decision(
                        &self.db,
                        tx_id,
                        decision_participant_records(&prepared_locals, &prepared_remotes),
                    )?;
                    self.abort_local_prepared_batches(prepared_locals);
                    abort_prepared_participants(prepared_remotes);
                    return Err(err);
                }
            };
            let prepared_id = match parse_tx_prepared_response(&response) {
                Ok(prepared_id) => prepared_id,
                Err(err) => {
                    record_abort_decision(
                        &self.db,
                        tx_id,
                        decision_participant_records(&prepared_locals, &prepared_remotes),
                    )?;
                    self.abort_local_prepared_batches(prepared_locals);
                    abort_prepared_participants(prepared_remotes);
                    return Err(err);
                }
            };
            prepared_remotes.push(RemotePreparedParticipant {
                stream,
                address,
                shard_id,
                prepared_id,
            });
        }

        record_commit_decision(
            &self.db,
            tx_id,
            prepared_locals
                .iter()
                .map(|(shard_id, prepared_id)| TransactionParticipantRecord {
                    location: "local".to_string(),
                    shard_id: *shard_id,
                    prepared_id: *prepared_id,
                })
                .chain(
                    prepared_remotes
                        .iter()
                        .map(|participant| TransactionParticipantRecord {
                            location: format!("remote:{}", participant.address),
                            shard_id: participant.shard_id,
                            prepared_id: participant.prepared_id,
                        }),
                )
                .collect(),
        )?;

        while !prepared_locals.is_empty() {
            let (_, prepared_id) = prepared_locals.remove(0);
            if let Err(err) = self.commit_local_prepared_batch(prepared_id) {
                return Err(err);
            }
        }

        while !prepared_remotes.is_empty() {
            let mut participant = prepared_remotes.remove(0);
            if let Err(err) = request_command_on_stream(
                &mut participant.stream,
                2,
                &format!("TX_COMMIT_PREPARED\t{}", participant.prepared_id),
            ) {
                return Err(format!(
                    "commit prepared transaction {} on {} failed: {err}",
                    participant.prepared_id, participant.address
                ));
            }
        }
        let _ = clear_transaction_decision(&self.db, tx_id);
        Ok(())
    }

    pub(crate) fn commit_local_prepared_batch(&self, prepared_id: u64) -> Result<(), String> {
        let prepared = self.prepared_transactions.take(prepared_id)?;
        self.db
            .execute_staged_cypher_transaction_on_shard(prepared.shard_id, prepared.writes)
            .map(|_| ())
            .map_err(|err| err.to_string())
    }

    pub(crate) fn abort_local_prepared_batches(&self, prepared_ids: Vec<(u64, u64)>) {
        for (_, prepared_id) in prepared_ids {
            let _ = self.prepared_transactions.take(prepared_id);
        }
    }
}
