use super::*;
use crate::database::write_cypher_helpers::validate_read_options;

impl Neo4rDatabase {
    pub fn execute_batch_write(
        &mut self,
        operations: Vec<BatchWriteOperation>,
    ) -> DatabaseResult<Vec<BatchWriteOutput>> {
        let mut prepared = Vec::with_capacity(operations.len());
        for operation in operations {
            prepared.push(self.prepare_local_write(operation.into_write_operation()?)?);
        }
        if prepared.is_empty() {
            return Ok(Vec::new());
        }

        let entries = prepared
            .iter()
            .map(|prepared_write| prepared_write.entry.clone())
            .collect::<Vec<_>>();
        self.flush_group_commit(&entries)?;
        prepared
            .into_iter()
            .map(|prepared_write| prepared_write.response.into_batch_output())
            .collect()
    }

    pub fn execute_batch_read(
        &self,
        queries: Vec<BatchReadQuery>,
    ) -> DatabaseResult<Vec<Vec<QueryRow>>> {
        let snapshot = self.read_snapshot()?;
        queries
            .into_iter()
            .map(|query| snapshot.query_with_params(&query.query, &query.params))
            .collect()
    }
}

impl Neo4rDatabaseHandle {
    pub fn execute_batch_write(
        &self,
        operations: Vec<BatchWriteOperation>,
    ) -> DatabaseResult<Vec<BatchWriteOutput>> {
        self.lock()?.execute_batch_write(operations)
    }

    pub fn execute_batch_read(
        &self,
        queries: Vec<BatchReadQuery>,
    ) -> DatabaseResult<Vec<Vec<QueryRow>>> {
        self.execute_batch_read_with_options(queries, QueryOptions::default())
    }

    pub fn execute_batch_read_with_options(
        &self,
        queries: Vec<BatchReadQuery>,
        options: QueryOptions,
    ) -> DatabaseResult<Vec<Vec<QueryRow>>> {
        self.ensure_raft_read_index(options)?;
        let snapshot = self.read_snapshot()?;
        validate_read_options(&snapshot, options)?;
        queries
            .into_iter()
            .map(|query| snapshot.query_with_params(&query.query, &query.params))
            .collect()
    }
}

impl BatchWriteOperation {
    fn into_write_operation(self) -> DatabaseResult<WriteOperation> {
        Ok(match self {
            Self::CreateNode { labels, properties } => {
                WriteOperation::CreateNode { labels, properties }
            }
            Self::CreateNodeOnShard {
                shard_id,
                labels,
                properties,
            } => WriteOperation::CreateNodeOnShard {
                shard_id,
                labels,
                properties,
            },
            Self::CreateRelationship {
                from,
                to,
                rel_type,
                properties,
            } => WriteOperation::CreateRelationship {
                from,
                to,
                rel_type,
                properties,
            },
            Self::SetNodeProperty { id, key, value } => {
                WriteOperation::SetNodeProperty { id, key, value }
            }
            Self::RemoveNodeProperty { id, key } => WriteOperation::RemoveNodeProperty { id, key },
            Self::AddNodeLabel { id, label } => WriteOperation::AddNodeLabel { id, label },
            Self::RemoveNodeLabel { id, label } => WriteOperation::RemoveNodeLabel { id, label },
            Self::SetRelationshipProperty { id, key, value } => {
                WriteOperation::SetRelationshipProperty { id, key, value }
            }
            Self::RemoveRelationshipProperty { id, key } => {
                WriteOperation::RemoveRelationshipProperty { id, key }
            }
            Self::DeleteRelationship { id } => WriteOperation::DeleteRelationship { id },
            Self::DeleteNode { id } => WriteOperation::DeleteNode { id },
        })
    }
}

impl WriteResponse {
    fn into_batch_output(self) -> DatabaseResult<BatchWriteOutput> {
        match self {
            Self::NodeId(id) => Ok(BatchWriteOutput::NodeId(id)),
            Self::RelationshipId(id) => Ok(BatchWriteOutput::RelationshipId(id)),
            Self::Unit => Ok(BatchWriteOutput::Unit),
        }
    }
}
