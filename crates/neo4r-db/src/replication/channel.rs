use super::tcp_responses::request_tcp_catch_up_limited;
use super::*;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReplicationChannelKind {
    Tcp,
    Udp,
    Rdma,
    Custom,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicationChannelCapabilities {
    pub raft_append: bool,
    pub vote: bool,
    pub snapshot: bool,
    pub catch_up: bool,
    pub max_frame_bytes: Option<usize>,
    pub fault_profile: ReplicationTransportFaultProfile,
}

impl ReplicationChannelCapabilities {
    pub fn reliable_stream() -> Self {
        Self {
            raft_append: true,
            vote: true,
            snapshot: true,
            catch_up: true,
            max_frame_bytes: None,
            fault_profile: ReplicationTransportFaultProfile::reliable_stream(),
        }
    }

    pub fn unreliable_datagram(max_frame_bytes: usize) -> Self {
        Self {
            raft_append: false,
            vote: false,
            snapshot: false,
            catch_up: false,
            max_frame_bytes: Some(max_frame_bytes),
            fault_profile: ReplicationTransportFaultProfile::unreliable_datagram(),
        }
    }

    pub fn supports(&self, required: &Self) -> bool {
        (!required.raft_append || self.raft_append)
            && (!required.vote || self.vote)
            && (!required.snapshot || self.snapshot)
            && (!required.catch_up || self.catch_up)
            && required.max_frame_bytes.is_none_or(|required_max| {
                self.max_frame_bytes.is_none_or(|max| max >= required_max)
            })
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ReplicationTransportFaultProfile {
    pub may_drop: bool,
    pub may_duplicate: bool,
    pub may_reorder: bool,
    pub may_fragment: bool,
    pub bounded_delivery: bool,
}

impl ReplicationTransportFaultProfile {
    pub fn reliable_stream() -> Self {
        Self {
            may_drop: false,
            may_duplicate: false,
            may_reorder: false,
            may_fragment: false,
            bounded_delivery: true,
        }
    }

    pub fn unreliable_datagram() -> Self {
        Self {
            may_drop: true,
            may_duplicate: true,
            may_reorder: true,
            may_fragment: true,
            bounded_delivery: false,
        }
    }

    pub fn requires_reliable_delivery(&self) -> bool {
        self.may_drop || self.may_duplicate || self.may_reorder || !self.bounded_delivery
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicationEndpoint {
    pub kind: ReplicationChannelKind,
    pub address: String,
    pub capabilities: ReplicationChannelCapabilities,
}

impl ReplicationEndpoint {
    pub fn tcp(address: impl Into<String>) -> Self {
        Self {
            kind: ReplicationChannelKind::Tcp,
            address: address.into(),
            capabilities: ReplicationChannelCapabilities::reliable_stream(),
        }
    }

    pub fn udp(address: impl Into<String>, max_frame_bytes: usize) -> Self {
        Self {
            kind: ReplicationChannelKind::Udp,
            address: address.into(),
            capabilities: ReplicationChannelCapabilities::unreliable_datagram(max_frame_bytes),
        }
    }

    #[cfg(feature = "rdma")]
    pub fn rdma(address: impl Into<String>) -> Self {
        Self {
            kind: ReplicationChannelKind::Rdma,
            address: address.into(),
            capabilities: ReplicationChannelCapabilities::reliable_stream(),
        }
    }

    pub fn ensure_kind(&self, expected: ReplicationChannelKind) -> DatabaseResult<()> {
        if self.kind == expected {
            Ok(())
        } else {
            Err(DatabaseError::Replication(format!(
                "replication endpoint kind mismatch: channel {:?}, endpoint {:?}",
                expected, self.kind
            )))
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicationChannelOffer {
    pub server_id: ServerId,
    pub endpoints: Vec<ReplicationEndpoint>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicationChannelAgreement {
    pub server_id: ServerId,
    pub endpoint: ReplicationEndpoint,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicationNodeIdentity {
    pub server_id: ServerId,
    pub node_id: ServerId,
    pub cluster_id: String,
    pub database_id: String,
    pub transports: Vec<ReplicationChannelKind>,
}

pub fn negotiate_replication_channel(
    preferred: &[ReplicationChannelKind],
    offer: ReplicationChannelOffer,
) -> DatabaseResult<ReplicationChannelAgreement> {
    negotiate_replication_channel_with_capabilities(
        preferred,
        offer,
        &ReplicationChannelCapabilities {
            raft_append: false,
            vote: false,
            snapshot: false,
            catch_up: false,
            max_frame_bytes: None,
            fault_profile: ReplicationTransportFaultProfile::reliable_stream(),
        },
    )
}

pub fn negotiate_replication_channel_with_capabilities(
    preferred: &[ReplicationChannelKind],
    offer: ReplicationChannelOffer,
    required: &ReplicationChannelCapabilities,
) -> DatabaseResult<ReplicationChannelAgreement> {
    for kind in preferred {
        if let Some(endpoint) = offer
            .endpoints
            .iter()
            .find(|endpoint| endpoint.kind == *kind && endpoint.capabilities.supports(required))
            .cloned()
        {
            return Ok(ReplicationChannelAgreement {
                server_id: offer.server_id,
                endpoint,
            });
        }
    }
    Err(DatabaseError::Replication(format!(
        "no compatible replication channel for server {} with required capabilities",
        offer.server_id
    )))
}

#[derive(Debug, Default)]
pub struct ReplicationChannelMetrics {
    sent_batches: AtomicUsize,
    acked_batches: AtomicUsize,
    failed_batches: AtomicUsize,
    sent_entries: AtomicUsize,
    sent_bytes: AtomicU64,
    election_rounds: AtomicUsize,
    append_conflicts: AtomicUsize,
    snapshot_installs: AtomicUsize,
    snapshot_install_millis: AtomicU64,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ReplicationChannelMetricsSnapshot {
    pub sent_batches: usize,
    pub acked_batches: usize,
    pub failed_batches: usize,
    pub sent_entries: usize,
    pub sent_bytes: u64,
    pub election_rounds: usize,
    pub append_conflicts: usize,
    pub snapshot_installs: usize,
    pub snapshot_install_millis: u64,
}

impl ReplicationChannelMetrics {
    pub fn record_send(&self, entries: usize, bytes: u64) {
        self.sent_batches.fetch_add(1, Ordering::Relaxed);
        self.sent_entries.fetch_add(entries, Ordering::Relaxed);
        self.sent_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn record_ack(&self) {
        self.acked_batches.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_failure(&self) {
        self.failed_batches.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_election_round(&self) {
        self.election_rounds.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_append_conflict(&self) {
        self.append_conflicts.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_snapshot_install(&self, duration: Duration) {
        self.snapshot_installs.fetch_add(1, Ordering::Relaxed);
        self.snapshot_install_millis
            .fetch_add(duration.as_millis() as u64, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> ReplicationChannelMetricsSnapshot {
        ReplicationChannelMetricsSnapshot {
            sent_batches: self.sent_batches.load(Ordering::Relaxed),
            acked_batches: self.acked_batches.load(Ordering::Relaxed),
            failed_batches: self.failed_batches.load(Ordering::Relaxed),
            sent_entries: self.sent_entries.load(Ordering::Relaxed),
            sent_bytes: self.sent_bytes.load(Ordering::Relaxed),
            election_rounds: self.election_rounds.load(Ordering::Relaxed),
            append_conflicts: self.append_conflicts.load(Ordering::Relaxed),
            snapshot_installs: self.snapshot_installs.load(Ordering::Relaxed),
            snapshot_install_millis: self.snapshot_install_millis.load(Ordering::Relaxed),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicationChannelConfig {
    pub connect_timeout: Duration,
    pub max_attempts: usize,
    pub retry_backoff: Duration,
    pub retransmit_timeout: Duration,
}

impl Default for ReplicationChannelConfig {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(1),
            max_attempts: 1,
            retry_backoff: Duration::from_millis(10),
            retransmit_timeout: Duration::from_millis(50),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[allow(dead_code)]
pub struct ReliableDatagramFrame {
    pub stream_id: u64,
    pub sequence: u64,
    pub ack: Option<u64>,
    pub fragment_index: u16,
    pub fragment_count: u16,
    pub payload: Vec<u8>,
}

#[allow(dead_code)]
impl ReliableDatagramFrame {
    pub fn single(stream_id: u64, sequence: u64, payload: Vec<u8>) -> Self {
        Self {
            stream_id,
            sequence,
            ack: None,
            fragment_index: 0,
            fragment_count: 1,
            payload,
        }
    }

    pub fn ack(stream_id: u64, sequence: u64, ack: u64) -> Self {
        Self {
            stream_id,
            sequence,
            ack: Some(ack),
            fragment_index: 0,
            fragment_count: 1,
            payload: Vec::new(),
        }
    }

    pub fn fragment_payload(
        stream_id: u64,
        sequence: u64,
        payload: &[u8],
        max_payload: usize,
    ) -> Vec<Self> {
        let max_payload = max_payload.max(1);
        if payload.is_empty() {
            return vec![Self::single(stream_id, sequence, Vec::new())];
        }
        let fragment_count = payload.len().div_ceil(max_payload).max(1);
        payload
            .chunks(max_payload)
            .enumerate()
            .map(|(index, chunk)| Self {
                stream_id,
                sequence,
                ack: None,
                fragment_index: index as u16,
                fragment_count: fragment_count as u16,
                payload: chunk.to_vec(),
            })
            .collect()
    }
}

#[derive(Debug, Default)]
#[allow(dead_code)]
pub struct ReliableDatagramReceiver {
    seen_sequences: BTreeSet<(u64, u64)>,
    fragments: BTreeMap<(u64, u64), BTreeMap<u16, Vec<u8>>>,
}

#[allow(dead_code)]
impl ReliableDatagramReceiver {
    pub fn accept(&mut self, frame: ReliableDatagramFrame) -> Option<Vec<u8>> {
        let key = (frame.stream_id, frame.sequence);
        if self.seen_sequences.contains(&key) {
            return None;
        }
        if frame.fragment_count <= 1 {
            self.seen_sequences.insert(key);
            return Some(frame.payload);
        }
        let fragments = self.fragments.entry(key).or_default();
        fragments
            .entry(frame.fragment_index)
            .or_insert(frame.payload);
        if fragments.len() != frame.fragment_count as usize {
            return None;
        }
        let mut payload = Vec::new();
        for index in 0..frame.fragment_count {
            payload.extend_from_slice(fragments.get(&index)?);
        }
        self.fragments.remove(&key);
        self.seen_sequences.insert(key);
        Some(payload)
    }
}

#[allow(dead_code)]
pub trait RdmaReplicationProvider: Send + Sync {
    fn provider_name(&self) -> &str;

    fn endpoint(&self, address: String) -> ReplicationEndpoint {
        ReplicationEndpoint {
            kind: ReplicationChannelKind::Rdma,
            address,
            capabilities: ReplicationChannelCapabilities::reliable_stream(),
        }
    }

    fn validate(&self) -> DatabaseResult<()>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[allow(dead_code)]
pub struct MockRdmaReplicationProvider {
    name: String,
    available: bool,
}

#[allow(dead_code)]
impl MockRdmaReplicationProvider {
    pub fn available(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            available: true,
        }
    }

    pub fn unavailable(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            available: false,
        }
    }
}

impl RdmaReplicationProvider for MockRdmaReplicationProvider {
    fn provider_name(&self) -> &str {
        &self.name
    }

    fn validate(&self) -> DatabaseResult<()> {
        if self.available {
            Ok(())
        } else {
            Err(DatabaseError::Replication(format!(
                "rdma provider {} is unavailable",
                self.name
            )))
        }
    }
}

pub trait ReplicationChannel: Send + Sync {
    fn kind(&self) -> ReplicationChannelKind;

    fn send_replication_batch(
        &self,
        endpoint: &ReplicationEndpoint,
        config: &ReplicationChannelConfig,
        entries: &[LogEntry],
    ) -> DatabaseResult<Vec<(ShardId, LogIndex)>>;

    fn send_raft_append_batch(
        &self,
        endpoint: &ReplicationEndpoint,
        config: &ReplicationChannelConfig,
        shard_id: ShardId,
        leader_commit: LogIndex,
        entries: &[LogEntry],
    ) -> DatabaseResult<Vec<(ShardId, LogIndex)>>;

    fn send_raft_append_batches_by_shard(
        &self,
        endpoint: &ReplicationEndpoint,
        config: &ReplicationChannelConfig,
        entries: &[LogEntry],
    ) -> DatabaseResult<Vec<(ShardId, LogIndex)>>;

    fn send_raft_append_batch_once(
        &self,
        endpoint: &ReplicationEndpoint,
        config: &ReplicationChannelConfig,
        shard_id: ShardId,
        leader_commit: LogIndex,
        entries: &[LogEntry],
    ) -> DatabaseResult<RaftAppendChannelResponse>;

    fn request_vote(
        &self,
        endpoint: &ReplicationEndpoint,
        config: &ReplicationChannelConfig,
        shard_id: ShardId,
        request: RequestVoteRequest,
    ) -> DatabaseResult<RequestVoteResponse>;

    fn install_snapshot(
        &self,
        endpoint: &ReplicationEndpoint,
        config: &ReplicationChannelConfig,
        request: InstallSnapshotRequest,
    ) -> DatabaseResult<InstallSnapshotResponse>;

    fn catch_up(
        &self,
        endpoint: &ReplicationEndpoint,
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
        endpoint: &ReplicationEndpoint,
        config: &ReplicationChannelConfig,
        entries: &[LogEntry],
    ) -> DatabaseResult<Vec<(ShardId, LogIndex)>> {
        endpoint.ensure_kind(ReplicationChannelKind::Tcp)?;
        send_tcp_replication_batch(
            &endpoint.address,
            config.connect_timeout,
            config.max_attempts,
            config.retry_backoff,
            entries,
        )
    }

    fn send_raft_append_batch(
        &self,
        endpoint: &ReplicationEndpoint,
        config: &ReplicationChannelConfig,
        shard_id: ShardId,
        leader_commit: LogIndex,
        entries: &[LogEntry],
    ) -> DatabaseResult<Vec<(ShardId, LogIndex)>> {
        endpoint.ensure_kind(ReplicationChannelKind::Tcp)?;
        send_tcp_raft_append_batch(
            &endpoint.address,
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
        endpoint: &ReplicationEndpoint,
        config: &ReplicationChannelConfig,
        entries: &[LogEntry],
    ) -> DatabaseResult<Vec<(ShardId, LogIndex)>> {
        endpoint.ensure_kind(ReplicationChannelKind::Tcp)?;
        send_tcp_raft_append_batches_by_shard(
            &endpoint.address,
            config.connect_timeout,
            config.max_attempts,
            config.retry_backoff,
            entries,
        )
    }

    fn send_raft_append_batch_once(
        &self,
        endpoint: &ReplicationEndpoint,
        config: &ReplicationChannelConfig,
        shard_id: ShardId,
        leader_commit: LogIndex,
        entries: &[LogEntry],
    ) -> DatabaseResult<RaftAppendChannelResponse> {
        endpoint.ensure_kind(ReplicationChannelKind::Tcp)?;
        send_tcp_raft_append_batch_once(
            &endpoint.address,
            config.connect_timeout,
            shard_id,
            leader_commit,
            entries,
        )
    }

    fn request_vote(
        &self,
        endpoint: &ReplicationEndpoint,
        config: &ReplicationChannelConfig,
        shard_id: ShardId,
        request: RequestVoteRequest,
    ) -> DatabaseResult<RequestVoteResponse> {
        endpoint.ensure_kind(ReplicationChannelKind::Tcp)?;
        request_tcp_raft_vote(&endpoint.address, config.connect_timeout, shard_id, request)
    }

    fn install_snapshot(
        &self,
        endpoint: &ReplicationEndpoint,
        config: &ReplicationChannelConfig,
        request: InstallSnapshotRequest,
    ) -> DatabaseResult<InstallSnapshotResponse> {
        endpoint.ensure_kind(ReplicationChannelKind::Tcp)?;
        request_tcp_install_snapshot(&endpoint.address, config.connect_timeout, request)
    }

    fn catch_up(
        &self,
        endpoint: &ReplicationEndpoint,
        config: &ReplicationChannelConfig,
        shard_id: ShardId,
        start_index: LogIndex,
        max_entries: Option<usize>,
    ) -> DatabaseResult<Vec<LogEntry>> {
        endpoint.ensure_kind(ReplicationChannelKind::Tcp)?;
        request_tcp_catch_up_limited(
            &endpoint.address,
            config.connect_timeout,
            shard_id,
            start_index,
            max_entries,
        )
    }
}

#[derive(Debug, Default)]
pub struct UdpReplicationChannel {
    pub max_frame_bytes: usize,
}

impl UdpReplicationChannel {
    pub fn prototype(max_frame_bytes: usize) -> Self {
        Self { max_frame_bytes }
    }

    fn unsupported(&self) -> DatabaseError {
        DatabaseError::Replication(
            "udp replication channel negotiation is available, but reliable raft delivery is not implemented".to_string(),
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
        _endpoint: &ReplicationEndpoint,
        _config: &ReplicationChannelConfig,
        _entries: &[LogEntry],
    ) -> DatabaseResult<Vec<(ShardId, LogIndex)>> {
        Err(self.unsupported())
    }

    fn send_raft_append_batch(
        &self,
        _endpoint: &ReplicationEndpoint,
        _config: &ReplicationChannelConfig,
        _shard_id: ShardId,
        _leader_commit: LogIndex,
        _entries: &[LogEntry],
    ) -> DatabaseResult<Vec<(ShardId, LogIndex)>> {
        Err(self.unsupported())
    }

    fn send_raft_append_batches_by_shard(
        &self,
        _endpoint: &ReplicationEndpoint,
        _config: &ReplicationChannelConfig,
        _entries: &[LogEntry],
    ) -> DatabaseResult<Vec<(ShardId, LogIndex)>> {
        Err(self.unsupported())
    }

    fn send_raft_append_batch_once(
        &self,
        _endpoint: &ReplicationEndpoint,
        _config: &ReplicationChannelConfig,
        _shard_id: ShardId,
        _leader_commit: LogIndex,
        _entries: &[LogEntry],
    ) -> DatabaseResult<RaftAppendChannelResponse> {
        Err(self.unsupported())
    }

    fn request_vote(
        &self,
        _endpoint: &ReplicationEndpoint,
        _config: &ReplicationChannelConfig,
        _shard_id: ShardId,
        _request: RequestVoteRequest,
    ) -> DatabaseResult<RequestVoteResponse> {
        Err(self.unsupported())
    }

    fn install_snapshot(
        &self,
        _endpoint: &ReplicationEndpoint,
        _config: &ReplicationChannelConfig,
        _request: InstallSnapshotRequest,
    ) -> DatabaseResult<InstallSnapshotResponse> {
        Err(self.unsupported())
    }

    fn catch_up(
        &self,
        _endpoint: &ReplicationEndpoint,
        _config: &ReplicationChannelConfig,
        _shard_id: ShardId,
        _start_index: LogIndex,
        _max_entries: Option<usize>,
    ) -> DatabaseResult<Vec<LogEntry>> {
        Err(self.unsupported())
    }
}

impl ReplicationChannel for UdpReplicationChannel {
    fn kind(&self) -> ReplicationChannelKind {
        ReplicationChannelKind::Udp
    }

    fn send_replication_batch(
        &self,
        endpoint: &ReplicationEndpoint,
        _config: &ReplicationChannelConfig,
        _entries: &[LogEntry],
    ) -> DatabaseResult<Vec<(ShardId, LogIndex)>> {
        endpoint.ensure_kind(ReplicationChannelKind::Udp)?;
        Err(self.unsupported())
    }

    fn send_raft_append_batch(
        &self,
        endpoint: &ReplicationEndpoint,
        _config: &ReplicationChannelConfig,
        _shard_id: ShardId,
        _leader_commit: LogIndex,
        _entries: &[LogEntry],
    ) -> DatabaseResult<Vec<(ShardId, LogIndex)>> {
        endpoint.ensure_kind(ReplicationChannelKind::Udp)?;
        Err(self.unsupported())
    }

    fn send_raft_append_batches_by_shard(
        &self,
        endpoint: &ReplicationEndpoint,
        _config: &ReplicationChannelConfig,
        _entries: &[LogEntry],
    ) -> DatabaseResult<Vec<(ShardId, LogIndex)>> {
        endpoint.ensure_kind(ReplicationChannelKind::Udp)?;
        Err(self.unsupported())
    }

    fn send_raft_append_batch_once(
        &self,
        endpoint: &ReplicationEndpoint,
        _config: &ReplicationChannelConfig,
        _shard_id: ShardId,
        _leader_commit: LogIndex,
        _entries: &[LogEntry],
    ) -> DatabaseResult<RaftAppendChannelResponse> {
        endpoint.ensure_kind(ReplicationChannelKind::Udp)?;
        Err(self.unsupported())
    }

    fn request_vote(
        &self,
        endpoint: &ReplicationEndpoint,
        _config: &ReplicationChannelConfig,
        _shard_id: ShardId,
        _request: RequestVoteRequest,
    ) -> DatabaseResult<RequestVoteResponse> {
        endpoint.ensure_kind(ReplicationChannelKind::Udp)?;
        Err(self.unsupported())
    }

    fn install_snapshot(
        &self,
        endpoint: &ReplicationEndpoint,
        _config: &ReplicationChannelConfig,
        _request: InstallSnapshotRequest,
    ) -> DatabaseResult<InstallSnapshotResponse> {
        endpoint.ensure_kind(ReplicationChannelKind::Udp)?;
        Err(self.unsupported())
    }

    fn catch_up(
        &self,
        endpoint: &ReplicationEndpoint,
        _config: &ReplicationChannelConfig,
        _shard_id: ShardId,
        _start_index: LogIndex,
        _max_entries: Option<usize>,
    ) -> DatabaseResult<Vec<LogEntry>> {
        endpoint.ensure_kind(ReplicationChannelKind::Udp)?;
        Err(self.unsupported())
    }
}

#[cfg(feature = "rdma")]
#[derive(Debug, Default)]
pub struct RdmaReplicationChannel;

#[cfg(feature = "rdma")]
impl ReplicationChannel for RdmaReplicationChannel {
    fn kind(&self) -> ReplicationChannelKind {
        ReplicationChannelKind::Rdma
    }

    fn send_replication_batch(
        &self,
        endpoint: &ReplicationEndpoint,
        _config: &ReplicationChannelConfig,
        _entries: &[LogEntry],
    ) -> DatabaseResult<Vec<(ShardId, LogIndex)>> {
        endpoint.ensure_kind(ReplicationChannelKind::Rdma)?;
        Err(DatabaseError::Replication(
            "rdma replication channel boundary is feature-gated; provider implementation is not linked".to_string(),
        ))
    }

    fn send_raft_append_batch(
        &self,
        endpoint: &ReplicationEndpoint,
        _config: &ReplicationChannelConfig,
        _shard_id: ShardId,
        _leader_commit: LogIndex,
        _entries: &[LogEntry],
    ) -> DatabaseResult<Vec<(ShardId, LogIndex)>> {
        endpoint.ensure_kind(ReplicationChannelKind::Rdma)?;
        Err(DatabaseError::Replication(
            "rdma replication channel boundary is feature-gated; provider implementation is not linked".to_string(),
        ))
    }

    fn send_raft_append_batches_by_shard(
        &self,
        endpoint: &ReplicationEndpoint,
        _config: &ReplicationChannelConfig,
        _entries: &[LogEntry],
    ) -> DatabaseResult<Vec<(ShardId, LogIndex)>> {
        endpoint.ensure_kind(ReplicationChannelKind::Rdma)?;
        Err(DatabaseError::Replication(
            "rdma replication channel boundary is feature-gated; provider implementation is not linked".to_string(),
        ))
    }

    fn send_raft_append_batch_once(
        &self,
        endpoint: &ReplicationEndpoint,
        _config: &ReplicationChannelConfig,
        _shard_id: ShardId,
        _leader_commit: LogIndex,
        _entries: &[LogEntry],
    ) -> DatabaseResult<RaftAppendChannelResponse> {
        endpoint.ensure_kind(ReplicationChannelKind::Rdma)?;
        Err(DatabaseError::Replication(
            "rdma replication channel boundary is feature-gated; provider implementation is not linked".to_string(),
        ))
    }

    fn request_vote(
        &self,
        endpoint: &ReplicationEndpoint,
        _config: &ReplicationChannelConfig,
        _shard_id: ShardId,
        _request: RequestVoteRequest,
    ) -> DatabaseResult<RequestVoteResponse> {
        endpoint.ensure_kind(ReplicationChannelKind::Rdma)?;
        Err(DatabaseError::Replication(
            "rdma replication channel boundary is feature-gated; provider implementation is not linked".to_string(),
        ))
    }

    fn install_snapshot(
        &self,
        endpoint: &ReplicationEndpoint,
        _config: &ReplicationChannelConfig,
        _request: InstallSnapshotRequest,
    ) -> DatabaseResult<InstallSnapshotResponse> {
        endpoint.ensure_kind(ReplicationChannelKind::Rdma)?;
        Err(DatabaseError::Replication(
            "rdma replication channel boundary is feature-gated; provider implementation is not linked".to_string(),
        ))
    }

    fn catch_up(
        &self,
        endpoint: &ReplicationEndpoint,
        _config: &ReplicationChannelConfig,
        _shard_id: ShardId,
        _start_index: LogIndex,
        _max_entries: Option<usize>,
    ) -> DatabaseResult<Vec<LogEntry>> {
        endpoint.ensure_kind(ReplicationChannelKind::Rdma)?;
        Err(DatabaseError::Replication(
            "rdma replication channel boundary is feature-gated; provider implementation is not linked".to_string(),
        ))
    }
}
