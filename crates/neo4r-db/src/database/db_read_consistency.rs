use super::*;

impl Neo4rDatabase {
    pub(super) fn snapshot_store(&self) -> DatabaseResult<PartitionedGraphStore<RocksKvSnapshot>> {
        Ok(self.store.snapshot()?)
    }

    pub(super) fn read_snapshot(&self) -> DatabaseResult<Neo4rReadSnapshot> {
        Ok(Neo4rReadSnapshot {
            store: self.snapshot_store()?,
            shard_map: self.shard_map,
            timestamp: self.clock.now(),
            applied_indexes: self.applied_indexes(),
            committed_indexes: self.committed_indexes(),
            query_engine: CypherEngine::with_vector_indexes(Arc::new(
                SharedVectorIndexProvider::new(self.vector_indexes.clone()),
            )),
        })
    }

    pub(super) fn ensure_raft_read_index(&self) -> DatabaseResult<()> {
        let Some(raft_groups) = self.raft_groups.as_ref() else {
            return Ok(());
        };
        for placement in &self.routing_table.placements {
            if !placement.has_server(self.config.server_id) {
                continue;
            }
            let group = raft_groups
                .groups
                .get(placement.shard_id as usize)
                .ok_or(DatabaseError::MissingShardLog(placement.shard_id))?;
            let read_index = group.leader_lease_read_index()?;
            let committed = self
                .commit_indexes
                .get(placement.shard_id as usize)
                .copied()
                .ok_or(DatabaseError::MissingShardLog(placement.shard_id))?;
            if committed < read_index {
                return Err(DatabaseError::Replication(format!(
                    "strong read requires local commit index {committed} to reach raft read-index {read_index} for shard {}",
                    placement.shard_id
                )));
            }
        }
        Ok(())
    }
}
