use super::*;

impl Neo4rDatabase {
    pub fn query(&self, query: &str) -> DatabaseResult<Vec<QueryRow>> {
        Ok(self.query_engine.execute(&self.store, query)?)
    }

    pub fn node(&self, id: NodeId) -> DatabaseResult<Option<Node>> {
        if let Some(node) = self
            .read_cache
            .lock()
            .map_err(|_| DatabaseError::LockPoisoned)?
            .nodes
            .get(&id)
            .cloned()
        {
            self.observe_read_cache_hit()?;
            return Ok(node);
        }
        self.observe_read_cache_miss()?;
        let node = self.store.node(id).map_err(DatabaseError::from)?;
        self.read_cache
            .lock()
            .map_err(|_| DatabaseError::LockPoisoned)?
            .nodes
            .insert(id, node.clone());
        Ok(node)
    }

    pub fn relationship(&self, id: RelationshipId) -> DatabaseResult<Option<Relationship>> {
        if let Some(relationship) = self
            .read_cache
            .lock()
            .map_err(|_| DatabaseError::LockPoisoned)?
            .relationships
            .get(&id)
            .cloned()
        {
            self.observe_read_cache_hit()?;
            return Ok(relationship);
        }
        self.observe_read_cache_miss()?;
        let relationship = self.store.relationship(id).map_err(DatabaseError::from)?;
        self.read_cache
            .lock()
            .map_err(|_| DatabaseError::LockPoisoned)?
            .relationships
            .insert(id, relationship.clone());
        Ok(relationship)
    }

    pub fn query_with_params(
        &self,
        query: &str,
        params: &QueryParams,
    ) -> DatabaseResult<Vec<QueryRow>> {
        if let Some(rows) = self.show_index_rows_for_query(query)? {
            return Ok(rows);
        }
        if let Some(rows) = self.show_constraint_rows_for_query(query)? {
            return Ok(rows);
        }
        Ok(self
            .query_engine
            .execute_with_params(&self.store, query, params)?)
    }

    pub fn query_cursor(&self, query: &str) -> DatabaseResult<Box<dyn QueryCursor>> {
        if let Some(rows) = self.show_index_rows_for_query(query)? {
            return Ok(Box::new(VecQueryCursor::new(rows)));
        }
        if let Some(rows) = self.show_constraint_rows_for_query(query)? {
            return Ok(Box::new(VecQueryCursor::new(rows)));
        }
        Ok(self.query_engine.execute_cursor(&self.store, query)?)
    }

    pub fn query_cursor_with_params(
        &self,
        query: &str,
        params: &QueryParams,
    ) -> DatabaseResult<Box<dyn QueryCursor>> {
        if let Some(rows) = self.show_index_rows_for_query(query)? {
            return Ok(Box::new(VecQueryCursor::new(rows)));
        }
        if let Some(rows) = self.show_constraint_rows_for_query(query)? {
            return Ok(Box::new(VecQueryCursor::new(rows)));
        }
        Ok(self
            .query_engine
            .execute_cursor_with_params(&self.store, query, params)?)
    }

    pub fn shard_count(&self) -> u64 {
        self.shard_map.shard_count()
    }

    pub fn local_partition_count(&self) -> usize {
        self.store.partition_count()
    }

    pub fn data_dir(&self) -> &Path {
        &self.config.data_dir
    }

    pub fn routing_table(&self) -> &ShardRoutingTable {
        &self.routing_table
    }

    pub fn log_entries_from(
        &self,
        shard_id: ShardId,
        start_index: LogIndex,
    ) -> DatabaseResult<Vec<LogEntry>> {
        Ok(self.log(shard_id)?.replay_from(start_index)?)
    }
}
