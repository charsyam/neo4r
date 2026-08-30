use super::*;
use std::net::{SocketAddr, ToSocketAddrs};

const UDP_DEFAULT_MAX_FRAME_BYTES: usize = 1200;
const UDP_FRAME_HEADER_BYTES: usize = 39;
const UDP_STREAM_REPLICATION: u64 = 1;
const UDP_STREAM_RAFT_APPEND: u64 = 2;
const UDP_STREAM_RAFT_VOTE: u64 = 3;
const UDP_STREAM_RAFT_PRE_VOTE: u64 = 4;
const UDP_STREAM_RAFT_SNAPSHOT: u64 = 5;
const UDP_OP_REPLICATION: &[u8] = b"REPL";
const UDP_OP_RAFT_APPEND: &[u8] = b"RAPP";
const UDP_OP_RAFT_VOTE: &[u8] = b"RVOT";
const UDP_OP_RAFT_PRE_VOTE: &[u8] = b"RPVT";
const UDP_OP_RAFT_SNAPSHOT: &[u8] = b"RSNP";

#[derive(Debug)]
pub struct UdpReplicationChannel {
    pub max_frame_bytes: usize,
}

impl Default for UdpReplicationChannel {
    fn default() -> Self {
        Self::prototype(UDP_DEFAULT_MAX_FRAME_BYTES)
    }
}

impl UdpReplicationChannel {
    pub fn prototype(max_frame_bytes: usize) -> Self {
        Self {
            max_frame_bytes: max_frame_bytes.max(UDP_FRAME_HEADER_BYTES + 1),
        }
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
        entries: &[LogEntry],
    ) -> DatabaseResult<Vec<(ShardId, LogIndex)>> {
        endpoint.ensure_kind(ReplicationChannelKind::Udp)?;
        self.send_payload(
            endpoint,
            UDP_STREAM_REPLICATION,
            encode_udp_entries(UDP_OP_REPLICATION, entries)?,
        )?;
        Ok(entries
            .iter()
            .map(|entry| (entry.shard_id, entry.index))
            .collect())
    }

    fn send_raft_append_batch(
        &self,
        endpoint: &ReplicationEndpoint,
        _config: &ReplicationChannelConfig,
        shard_id: ShardId,
        leader_commit: LogIndex,
        entries: &[LogEntry],
    ) -> DatabaseResult<Vec<(ShardId, LogIndex)>> {
        endpoint.ensure_kind(ReplicationChannelKind::Udp)?;
        self.send_payload(
            endpoint,
            UDP_STREAM_RAFT_APPEND,
            encode_udp_raft_append(shard_id, leader_commit, entries)?,
        )?;
        Ok(entries
            .iter()
            .map(|entry| (entry.shard_id, entry.index))
            .collect())
    }

    fn send_raft_append_batches_by_shard(
        &self,
        endpoint: &ReplicationEndpoint,
        config: &ReplicationChannelConfig,
        entries: &[LogEntry],
    ) -> DatabaseResult<Vec<(ShardId, LogIndex)>> {
        let mut acked = Vec::new();
        let mut by_shard = BTreeMap::<ShardId, Vec<LogEntry>>::new();
        for entry in entries {
            by_shard
                .entry(entry.shard_id)
                .or_default()
                .push(entry.clone());
        }
        for (shard_id, shard_entries) in by_shard {
            acked.extend(
                self.send_raft_append_batch(
                    endpoint,
                    config,
                    shard_id,
                    shard_entries
                        .iter()
                        .map(|entry| entry.index)
                        .max()
                        .unwrap_or(0),
                    &shard_entries,
                )?,
            );
        }
        Ok(acked)
    }

    fn send_raft_append_batch_once(
        &self,
        endpoint: &ReplicationEndpoint,
        config: &ReplicationChannelConfig,
        shard_id: ShardId,
        leader_commit: LogIndex,
        entries: &[LogEntry],
    ) -> DatabaseResult<RaftAppendChannelResponse> {
        let ack_positions =
            self.send_raft_append_batch(endpoint, config, shard_id, leader_commit, entries)?;
        Ok(RaftAppendChannelResponse {
            append: AppendEntriesResponse {
                term: entries.last().map(|entry| entry.term).unwrap_or_default(),
                success: true,
                durable: false,
                match_index: entries
                    .last()
                    .map(|entry| entry.index)
                    .unwrap_or(leader_commit),
                conflict_index: None,
                conflict_term: None,
            },
            ack_positions,
        })
    }

    fn request_vote(
        &self,
        endpoint: &ReplicationEndpoint,
        _config: &ReplicationChannelConfig,
        shard_id: ShardId,
        request: RequestVoteRequest,
    ) -> DatabaseResult<RequestVoteResponse> {
        endpoint.ensure_kind(ReplicationChannelKind::Udp)?;
        self.send_payload(
            endpoint,
            UDP_STREAM_RAFT_VOTE,
            encode_udp_vote(
                UDP_OP_RAFT_VOTE,
                shard_id,
                request.term,
                request.candidate_id,
                request.last_log_index,
                request.last_log_term,
            ),
        )?;
        Err(DatabaseError::Replication(
            "udp raft vote response path is not implemented".to_string(),
        ))
    }

    fn pre_vote(
        &self,
        endpoint: &ReplicationEndpoint,
        _config: &ReplicationChannelConfig,
        shard_id: ShardId,
        request: PreVoteRequest,
    ) -> DatabaseResult<PreVoteResponse> {
        endpoint.ensure_kind(ReplicationChannelKind::Udp)?;
        self.send_payload(
            endpoint,
            UDP_STREAM_RAFT_PRE_VOTE,
            encode_udp_vote(
                UDP_OP_RAFT_PRE_VOTE,
                shard_id,
                request.next_term,
                request.candidate_id,
                request.last_log_index,
                request.last_log_term,
            ),
        )?;
        Err(DatabaseError::Replication(
            "udp raft pre-vote response path is not implemented".to_string(),
        ))
    }

    fn install_snapshot(
        &self,
        endpoint: &ReplicationEndpoint,
        _config: &ReplicationChannelConfig,
        request: InstallSnapshotRequest,
    ) -> DatabaseResult<InstallSnapshotResponse> {
        endpoint.ensure_kind(ReplicationChannelKind::Udp)?;
        let max_payload = self
            .endpoint_frame_bytes(endpoint)
            .saturating_sub(UDP_FRAME_HEADER_BYTES)
            .saturating_sub(64)
            .max(1);
        for chunk in request.chunks(max_payload) {
            self.send_payload(
                endpoint,
                UDP_STREAM_RAFT_SNAPSHOT,
                encode_udp_snapshot_chunk(&chunk)?,
            )?;
        }
        Ok(InstallSnapshotResponse {
            term: request.term,
            success: true,
            last_included_index: request.metadata.last_included_index,
        })
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
        Err(DatabaseError::Replication(
            "udp catch-up response path is not implemented".to_string(),
        ))
    }
}

impl UdpReplicationChannel {
    fn send_payload(
        &self,
        endpoint: &ReplicationEndpoint,
        stream_id: u64,
        payload: Vec<u8>,
    ) -> DatabaseResult<()> {
        let max_frame_bytes = self.endpoint_frame_bytes(endpoint);
        let max_payload = max_frame_bytes
            .saturating_sub(UDP_FRAME_HEADER_BYTES)
            .max(1);
        let target = resolve_udp_address(&endpoint.address)?;
        let socket = ReliableDatagramSocket::bind("0.0.0.0:0", max_frame_bytes)?;
        for frame in ReliableDatagramFrame::fragment_payload(stream_id, 1, &payload, max_payload) {
            socket.send_frame_to(&frame, target)?;
        }
        Ok(())
    }

    fn endpoint_frame_bytes(&self, endpoint: &ReplicationEndpoint) -> usize {
        endpoint
            .capabilities
            .max_frame_bytes
            .unwrap_or(self.max_frame_bytes)
            .min(self.max_frame_bytes)
            .max(UDP_FRAME_HEADER_BYTES + 1)
    }
}

pub struct ReliableDatagramSocket {
    socket: UdpSocket,
    max_frame_bytes: usize,
}

impl ReliableDatagramSocket {
    pub fn bind(address: &str, max_frame_bytes: usize) -> DatabaseResult<Self> {
        let socket = UdpSocket::bind(address)
            .map_err(|err| DatabaseError::Replication(format!("bind udp {address}: {err}")))?;
        Ok(Self {
            socket,
            max_frame_bytes: max_frame_bytes.max(1),
        })
    }

    pub fn local_addr(&self) -> DatabaseResult<SocketAddr> {
        self.socket
            .local_addr()
            .map_err(|err| DatabaseError::Replication(format!("read udp local addr: {err}")))
    }

    pub fn set_read_timeout(&self, timeout: Option<Duration>) -> DatabaseResult<()> {
        self.socket
            .set_read_timeout(timeout)
            .map_err(|err| DatabaseError::Replication(format!("set udp read timeout: {err}")))
    }

    pub fn send_frame_to(
        &self,
        frame: &ReliableDatagramFrame,
        target: SocketAddr,
    ) -> DatabaseResult<usize> {
        let payload = frame.encode();
        if payload.len() > self.max_frame_bytes {
            return Err(DatabaseError::Replication(format!(
                "udp frame {} exceeds max frame bytes {}",
                payload.len(),
                self.max_frame_bytes
            )));
        }
        self.socket
            .send_to(&payload, target)
            .map_err(|err| DatabaseError::Replication(format!("send udp frame: {err}")))
    }

    pub fn recv_frame_from(&self) -> DatabaseResult<(ReliableDatagramFrame, SocketAddr)> {
        let mut buf = vec![0; self.max_frame_bytes];
        let (len, source) = self
            .socket
            .recv_from(&mut buf)
            .map_err(|err| DatabaseError::Replication(format!("recv udp frame: {err}")))?;
        Ok((ReliableDatagramFrame::decode(&buf[..len])?, source))
    }
}

fn resolve_udp_address(address: &str) -> DatabaseResult<SocketAddr> {
    address
        .to_socket_addrs()
        .map_err(|err| DatabaseError::Replication(format!("resolve udp {address}: {err}")))?
        .next()
        .ok_or_else(|| DatabaseError::Replication(format!("no udp socket address for {address}")))
}

fn encode_udp_entries(op: &[u8], entries: &[LogEntry]) -> DatabaseResult<Vec<u8>> {
    let mut out = Vec::new();
    out.extend_from_slice(op);
    out.extend_from_slice(&(entries.len() as u32).to_be_bytes());
    for entry in entries {
        let payload = encode_log_entry(entry);
        out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        out.extend_from_slice(&payload);
    }
    Ok(out)
}

fn encode_udp_raft_append(
    shard_id: ShardId,
    leader_commit: LogIndex,
    entries: &[LogEntry],
) -> DatabaseResult<Vec<u8>> {
    let mut out = Vec::new();
    out.extend_from_slice(UDP_OP_RAFT_APPEND);
    out.extend_from_slice(&shard_id.to_be_bytes());
    out.extend_from_slice(&leader_commit.to_be_bytes());
    out.extend_from_slice(&encode_udp_entries(UDP_OP_REPLICATION, entries)?);
    Ok(out)
}

fn encode_udp_vote(
    op: &[u8],
    shard_id: ShardId,
    term: u64,
    candidate_id: ServerId,
    last_log_index: LogIndex,
    last_log_term: u64,
) -> Vec<u8> {
    let mut out = Vec::with_capacity(op.len() + 40);
    out.extend_from_slice(op);
    out.extend_from_slice(&shard_id.to_be_bytes());
    out.extend_from_slice(&term.to_be_bytes());
    out.extend_from_slice(&candidate_id.to_be_bytes());
    out.extend_from_slice(&last_log_index.to_be_bytes());
    out.extend_from_slice(&last_log_term.to_be_bytes());
    out
}

fn encode_udp_snapshot_chunk(chunk: &InstallSnapshotChunk) -> DatabaseResult<Vec<u8>> {
    let mut out = Vec::new();
    out.extend_from_slice(UDP_OP_RAFT_SNAPSHOT);
    out.extend_from_slice(&chunk.request.term.to_be_bytes());
    out.extend_from_slice(&chunk.request.leader_id.to_be_bytes());
    out.extend_from_slice(&chunk.request.metadata.shard_id.to_be_bytes());
    out.extend_from_slice(&chunk.request.metadata.last_included_term.to_be_bytes());
    out.extend_from_slice(&chunk.request.metadata.last_included_index.to_be_bytes());
    out.extend_from_slice(&chunk.offset.to_be_bytes());
    out.push(u8::from(chunk.done));
    out.extend_from_slice(&(chunk.request.payload.len() as u32).to_be_bytes());
    out.extend_from_slice(&chunk.request.payload);
    Ok(out)
}
