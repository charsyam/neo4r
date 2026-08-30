use neo4r_core::{LogIndex, ServerId, ShardRoutingTable};
use std::path::PathBuf;
use std::time::Duration;

#[derive(Clone, Debug)]
pub struct DatabaseConfig {
    pub data_dir: PathBuf,
    pub shard_count: u64,
    pub local_partition_count: usize,
    pub log_entries_per_segment: LogIndex,
    pub checkpoint_interval: LogIndex,
    pub wal_sync_interval: LogIndex,
    pub group_commit_max_entries: usize,
    pub group_commit_max_delay: Duration,
    pub server_id: ServerId,
    pub routing_table: Option<ShardRoutingTable>,
    pub raft_enabled: bool,
    pub failure_injection: FailureInjection,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct FailureInjection {
    pub fail_after_commit_before_apply: bool,
    pub fail_before_snapshot_payload_save: bool,
    pub fail_after_snapshot_payload_save_before_metadata: bool,
    pub fail_after_snapshot_prune_before_apply: bool,
}

impl DatabaseConfig {
    pub fn new(
        data_dir: impl Into<PathBuf>,
        shard_count: u64,
        local_partition_count: usize,
    ) -> Self {
        Self {
            data_dir: data_dir.into(),
            shard_count,
            local_partition_count,
            log_entries_per_segment: 1024,
            checkpoint_interval: 128,
            wal_sync_interval: 128,
            group_commit_max_entries: 32,
            group_commit_max_delay: Duration::from_millis(1),
            server_id: 0,
            routing_table: None,
            raft_enabled: false,
            failure_injection: FailureInjection::default(),
        }
    }

    pub fn with_log_entries_per_segment(mut self, entries_per_segment: LogIndex) -> Self {
        self.log_entries_per_segment = entries_per_segment;
        self
    }

    pub fn with_checkpoint_interval(mut self, checkpoint_interval: LogIndex) -> Self {
        self.checkpoint_interval = checkpoint_interval;
        self
    }

    pub fn with_wal_sync_interval(mut self, wal_sync_interval: LogIndex) -> Self {
        self.wal_sync_interval = wal_sync_interval;
        self
    }

    pub fn with_group_commit_max_entries(mut self, max_entries: usize) -> Self {
        self.group_commit_max_entries = max_entries;
        self
    }

    pub fn with_group_commit_max_delay(mut self, max_delay: Duration) -> Self {
        self.group_commit_max_delay = max_delay;
        self
    }

    pub fn with_server_id(mut self, server_id: ServerId) -> Self {
        self.server_id = server_id;
        self
    }

    pub fn with_routing_table(mut self, routing_table: ShardRoutingTable) -> Self {
        self.routing_table = Some(routing_table);
        self
    }

    pub fn with_raft_enabled(mut self, enabled: bool) -> Self {
        self.raft_enabled = enabled;
        self
    }

    pub fn with_failure_injection(mut self, failure_injection: FailureInjection) -> Self {
        self.failure_injection = failure_injection;
        self
    }
}
