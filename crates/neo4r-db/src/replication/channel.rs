use super::tcp_responses::request_tcp_catch_up_limited;
use super::*;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReplicationChannelKind {
    Tcp,
    Udp,
    Rdma,
    Custom,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicationChannelConfig {
    pub connect_timeout: Duration,
    pub max_attempts: usize,
    pub retry_backoff: Duration,
}

impl Default for ReplicationChannelConfig {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(1),
            max_attempts: 1,
            retry_backoff: Duration::from_millis(10),
        }
    }
}

pub trait ReplicationChannel: Send + Sync {
    fn kind(&self) -> ReplicationChannelKind;

    fn send_replication_batch(
        &self,
        address: &str,
        config: &ReplicationChannelConfig,
        entries: &[LogEntry],
    ) -> DatabaseResult<Vec<(ShardId, LogIndex)>>;

    fn send_raft_append_batch(
        &self,
        address: &str,
        config: &ReplicationChannelConfig,
        shard_id: ShardId,
        leader_commit: LogIndex,
        entries: &[LogEntry],
    ) -> DatabaseResult<Vec<(ShardId, LogIndex)>>;

    fn send_raft_append_batches_by_shard(
        &self,
        address: &str,
        config: &ReplicationChannelConfig,
        entries: &[LogEntry],
    ) -> DatabaseResult<Vec<(ShardId, LogIndex)>>;

    fn send_raft_append_batch_once(
        &self,
        address: &str,
        config: &ReplicationChannelConfig,
        shard_id: ShardId,
        leader_commit: LogIndex,
        entries: &[LogEntry],
    ) -> DatabaseResult<RaftAppendChannelResponse>;

    fn request_vote(
        &self,
        address: &str,
        config: &ReplicationChannelConfig,
        shard_id: ShardId,
        request: RequestVoteRequest,
    ) -> DatabaseResult<RequestVoteResponse>;

    fn install_snapshot(
        &self,
        address: &str,
        config: &ReplicationChannelConfig,
        request: InstallSnapshotRequest,
    ) -> DatabaseResult<InstallSnapshotResponse>;

    fn catch_up(
        &self,
        address: &str,
        config: &ReplicationChannelConfig,
        shard_id: ShardId,
        start_index: LogIndex,
        max_entries: Option<usize>,
    ) -> DatabaseResult<Vec<LogEntry>>;
}

#[derive(Debug, Default)]
pub struct TcpReplicationChannel;

impl ReplicationChannel for TcpReplicationChannel {
    fn kind(&self) -> ReplicationChannelKind {
        ReplicationChannelKind::Tcp
    }

    fn send_replication_batch(
        &self,
        address: &str,
        config: &ReplicationChannelConfig,
        entries: &[LogEntry],
    ) -> DatabaseResult<Vec<(ShardId, LogIndex)>> {
        send_tcp_replication_batch(
            address,
            config.connect_timeout,
            config.max_attempts,
            config.retry_backoff,
            entries,
        )
    }

    fn send_raft_append_batch(
        &self,
        address: &str,
        config: &ReplicationChannelConfig,
        shard_id: ShardId,
        leader_commit: LogIndex,
        entries: &[LogEntry],
    ) -> DatabaseResult<Vec<(ShardId, LogIndex)>> {
        send_tcp_raft_append_batch(
            address,
            config.connect_timeout,
            config.max_attempts,
            config.retry_backoff,
            shard_id,
            leader_commit,
            entries,
        )
    }

    fn send_raft_append_batches_by_shard(
        &self,
        address: &str,
        config: &ReplicationChannelConfig,
        entries: &[LogEntry],
    ) -> DatabaseResult<Vec<(ShardId, LogIndex)>> {
        send_tcp_raft_append_batches_by_shard(
            address,
            config.connect_timeout,
            config.max_attempts,
            config.retry_backoff,
            entries,
        )
    }

    fn send_raft_append_batch_once(
        &self,
        address: &str,
        config: &ReplicationChannelConfig,
        shard_id: ShardId,
        leader_commit: LogIndex,
        entries: &[LogEntry],
    ) -> DatabaseResult<RaftAppendChannelResponse> {
        send_tcp_raft_append_batch_once(
            address,
            config.connect_timeout,
            shard_id,
            leader_commit,
            entries,
        )
    }

    fn request_vote(
        &self,
        address: &str,
        config: &ReplicationChannelConfig,
        shard_id: ShardId,
        request: RequestVoteRequest,
    ) -> DatabaseResult<RequestVoteResponse> {
        request_tcp_raft_vote(address, config.connect_timeout, shard_id, request)
    }

    fn install_snapshot(
        &self,
        address: &str,
        config: &ReplicationChannelConfig,
        request: InstallSnapshotRequest,
    ) -> DatabaseResult<InstallSnapshotResponse> {
        request_tcp_install_snapshot(address, config.connect_timeout, request)
    }

    fn catch_up(
        &self,
        address: &str,
        config: &ReplicationChannelConfig,
        shard_id: ShardId,
        start_index: LogIndex,
        max_entries: Option<usize>,
    ) -> DatabaseResult<Vec<LogEntry>> {
        request_tcp_catch_up_limited(
            address,
            config.connect_timeout,
            shard_id,
            start_index,
            max_entries,
        )
    }
}

#[derive(Clone, Debug)]
pub struct UnsupportedReplicationChannel {
    kind: ReplicationChannelKind,
}

impl UnsupportedReplicationChannel {
    pub fn udp() -> Self {
        Self {
            kind: ReplicationChannelKind::Udp,
        }
    }

    pub fn rdma() -> Self {
        Self {
            kind: ReplicationChannelKind::Rdma,
        }
    }

    fn unsupported(&self) -> DatabaseError {
        DatabaseError::Replication(format!(
            "{:?} replication channel is not implemented",
            self.kind
        ))
    }
}

impl ReplicationChannel for UnsupportedReplicationChannel {
    fn kind(&self) -> ReplicationChannelKind {
        self.kind
    }

    fn send_replication_batch(
        &self,
        _address: &str,
        _config: &ReplicationChannelConfig,
        _entries: &[LogEntry],
    ) -> DatabaseResult<Vec<(ShardId, LogIndex)>> {
        Err(self.unsupported())
    }

    fn send_raft_append_batch(
        &self,
        _address: &str,
        _config: &ReplicationChannelConfig,
        _shard_id: ShardId,
        _leader_commit: LogIndex,
        _entries: &[LogEntry],
    ) -> DatabaseResult<Vec<(ShardId, LogIndex)>> {
        Err(self.unsupported())
    }

    fn send_raft_append_batches_by_shard(
        &self,
        _address: &str,
        _config: &ReplicationChannelConfig,
        _entries: &[LogEntry],
    ) -> DatabaseResult<Vec<(ShardId, LogIndex)>> {
        Err(self.unsupported())
    }

    fn send_raft_append_batch_once(
        &self,
        _address: &str,
        _config: &ReplicationChannelConfig,
        _shard_id: ShardId,
        _leader_commit: LogIndex,
        _entries: &[LogEntry],
    ) -> DatabaseResult<RaftAppendChannelResponse> {
        Err(self.unsupported())
    }

    fn request_vote(
        &self,
        _address: &str,
        _config: &ReplicationChannelConfig,
        _shard_id: ShardId,
        _request: RequestVoteRequest,
    ) -> DatabaseResult<RequestVoteResponse> {
        Err(self.unsupported())
    }

    fn install_snapshot(
        &self,
        _address: &str,
        _config: &ReplicationChannelConfig,
        _request: InstallSnapshotRequest,
    ) -> DatabaseResult<InstallSnapshotResponse> {
        Err(self.unsupported())
    }

    fn catch_up(
        &self,
        _address: &str,
        _config: &ReplicationChannelConfig,
        _shard_id: ShardId,
        _start_index: LogIndex,
        _max_entries: Option<usize>,
    ) -> DatabaseResult<Vec<LogEntry>> {
        Err(self.unsupported())
    }
}
