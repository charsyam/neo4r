use neo4r_core::{GraphError, GraphReadError, LogIndex, ServerId, ShardId};
use neo4r_query::QueryError;
use neo4r_storage::StorageError;
use std::fmt;

pub type DatabaseResult<T> = std::result::Result<T, DatabaseError>;

#[derive(Debug)]
pub enum DatabaseError {
    InvalidConfig(String),
    Graph(GraphError),
    GraphRead(GraphReadError),
    Query(QueryError),
    Storage(StorageError),
    MissingShardLog(ShardId),
    ShardNotLocal {
        shard_id: ShardId,
        server_id: ServerId,
    },
    ShardNotPrimary {
        shard_id: ShardId,
        server_id: ServerId,
        primary_server_id: Option<ServerId>,
    },
    ShardReplaying {
        shard_id: ShardId,
        server_id: ServerId,
        applied: LogIndex,
        committed: LogIndex,
    },
    UnexpectedLogIndex {
        shard_id: ShardId,
        expected: LogIndex,
        actual: LogIndex,
    },
    LogConflict {
        shard_id: ShardId,
        index: LogIndex,
        message: String,
    },
    Replication(String),
    LockPoisoned,
    WriterUnavailable,
    UnexpectedWriteResponse(String),
}

impl fmt::Display for DatabaseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfig(message) => write!(f, "invalid database config: {message}"),
            Self::Graph(err) => write!(f, "{err}"),
            Self::GraphRead(err) => write!(f, "{err}"),
            Self::Query(err) => write!(f, "{err}"),
            Self::Storage(err) => write!(f, "{err}"),
            Self::MissingShardLog(shard_id) => write!(f, "missing shard log: {shard_id}"),
            Self::ShardNotLocal {
                shard_id,
                server_id,
            } => write!(f, "shard {shard_id} is not local to server {server_id}"),
            Self::ShardNotPrimary {
                shard_id,
                server_id,
                primary_server_id,
            } => write!(
                f,
                "server {server_id} is not primary for shard {shard_id}; primary is {primary_server_id:?}"
            ),
            Self::ShardReplaying {
                shard_id,
                server_id,
                applied,
                committed,
            } => write!(
                f,
                "server {server_id} cannot serve shard {shard_id}: replaying applied {applied} committed {committed}"
            ),
            Self::UnexpectedLogIndex {
                shard_id,
                expected,
                actual,
            } => write!(
                f,
                "unexpected log index for shard {shard_id}: expected {expected}, got {actual}"
            ),
            Self::LogConflict {
                shard_id,
                index,
                message,
            } => write!(f, "log conflict at shard {shard_id} index {index}: {message}"),
            Self::Replication(message) => write!(f, "replication error: {message}"),
            Self::LockPoisoned => write!(f, "database lock poisoned"),
            Self::WriterUnavailable => write!(f, "database writer actor unavailable"),
            Self::UnexpectedWriteResponse(message) => {
                write!(f, "unexpected write response: {message}")
            }
        }
    }
}

impl std::error::Error for DatabaseError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::InvalidConfig(_) => None,
            Self::Graph(err) => Some(err),
            Self::GraphRead(err) => Some(err),
            Self::Query(err) => Some(err),
            Self::Storage(err) => Some(err),
            Self::MissingShardLog(_) => None,
            Self::ShardNotLocal { .. } => None,
            Self::ShardNotPrimary { .. } => None,
            Self::ShardReplaying { .. } => None,
            Self::UnexpectedLogIndex { .. } => None,
            Self::LogConflict { .. } => None,
            Self::Replication(_) => None,
            Self::LockPoisoned => None,
            Self::WriterUnavailable => None,
            Self::UnexpectedWriteResponse(_) => None,
        }
    }
}

impl From<GraphError> for DatabaseError {
    fn from(err: GraphError) -> Self {
        Self::Graph(err)
    }
}

impl From<GraphReadError> for DatabaseError {
    fn from(err: GraphReadError) -> Self {
        Self::GraphRead(err)
    }
}

impl From<QueryError> for DatabaseError {
    fn from(err: QueryError) -> Self {
        Self::Query(err)
    }
}

impl From<StorageError> for DatabaseError {
    fn from(err: StorageError) -> Self {
        Self::Storage(err)
    }
}
