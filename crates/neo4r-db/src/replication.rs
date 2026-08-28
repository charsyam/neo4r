use crate::{
    AppendEntriesResponse, DatabaseError, DatabaseResult, InstallSnapshotRequest,
    InstallSnapshotResponse, Neo4rDatabaseHandle, RaftSnapshotMetadata, RequestVoteRequest,
    RequestVoteResponse,
};
use neo4r_core::{LogEntry, LogIndex, ServerId, ShardId, ShardRole, ShardRoutingTable};
use neo4r_storage::{decode_log_entry, encode_log_entry};
use std::collections::{BTreeMap, BTreeSet};
use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::sync::{Arc, Mutex, RwLock};
use std::thread;
use std::time::Duration;

const TCP_REPLICATION_REQUEST_MAGIC: &[u8] = b"N4RRP1\n";
const TCP_RAFT_APPEND_REQUEST_MAGIC: &[u8] = b"N4RRAE\n";
const TCP_RAFT_APPEND_RESPONSE_MAGIC: &[u8] = b"N4RRA2\n";
const TCP_RAFT_VOTE_REQUEST_MAGIC: &[u8] = b"N4RRV1\n";
const TCP_RAFT_SNAPSHOT_REQUEST_MAGIC: &[u8] = b"N4RRS1\n";
const TCP_REPLICATION_RESPONSE_MAGIC: &[u8] = b"N4RRA1\n";
const TCP_CATCH_UP_REQUEST_MAGIC: &[u8] = b"N4RCU1\n";
const TCP_CATCH_UP_REQUEST_MAGIC_V2: &[u8] = b"N4RCU3\n";
const TCP_CATCH_UP_RESPONSE_MAGIC: &[u8] = b"N4RCU2\n";
const TCP_REPLICATION_OK: u8 = 1;
const TCP_REPLICATION_ERR: u8 = 2;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RaftAppendChannelResponse {
    pub append: AppendEntriesResponse,
    pub ack_positions: Vec<(ShardId, LogIndex)>,
}

pub type TcpRaftAppendResponse = RaftAppendChannelResponse;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct RaftPeerProgress {
    next_index: LogIndex,
    match_index: LogIndex,
}

impl RaftPeerProgress {
    fn clamp_to_commit(self, leader_commit: LogIndex) -> Self {
        Self {
            next_index: self.next_index.clamp(1, leader_commit.saturating_add(1)),
            match_index: self.match_index.min(leader_commit),
        }
    }
}

pub trait ShardReplicator: Send + Sync {
    fn publish(&self, entry: &LogEntry) -> DatabaseResult<ReplicationOutcome>;

    fn install_routing_table(&self, _routing_table: ShardRoutingTable) -> DatabaseResult<()> {
        Ok(())
    }

    fn register_peer_address(&self, _server_id: ServerId, _address: String) -> DatabaseResult<()> {
        Ok(())
    }

    fn unregister_peer_address(&self, _server_id: ServerId) -> DatabaseResult<()> {
        Ok(())
    }

    fn publish_batch(&self, entries: &[LogEntry]) -> DatabaseResult<Vec<ReplicationOutcome>> {
        entries.iter().map(|entry| self.publish(entry)).collect()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicationOutcome {
    pub acked_server_ids: Vec<ServerId>,
    pub acked_match_indexes: Vec<(ServerId, ShardId, LogIndex)>,
}

impl ReplicationOutcome {
    pub fn local(origin_server_id: ServerId) -> Self {
        Self {
            acked_server_ids: vec![origin_server_id],
            acked_match_indexes: Vec::new(),
        }
    }

    fn ack(&mut self, server_id: ServerId, shard_id: ShardId, index: LogIndex) {
        if !self.acked_server_ids.contains(&server_id) {
            self.acked_server_ids.push(server_id);
        }
        self.acked_match_indexes.push((server_id, shard_id, index));
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReplicationAckPolicy {
    All,
    Quorum,
    Async,
}

impl ReplicationAckPolicy {
    fn required_acks(self, voter_count: usize) -> usize {
        match self {
            Self::All => voter_count,
            Self::Quorum => voter_count / 2 + 1,
            Self::Async => 1,
        }
    }
}

#[derive(Debug, Default)]
pub struct NoopShardReplicator;

impl ShardReplicator for NoopShardReplicator {
    fn publish(&self, entry: &LogEntry) -> DatabaseResult<ReplicationOutcome> {
        Ok(ReplicationOutcome::local(entry.origin_server_id))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TcpCatchUpResult {
    pub shard_id: ShardId,
    pub start_index: LogIndex,
    pub end_index: LogIndex,
    pub fetched_entries: usize,
    pub primary_server_id: ServerId,
}

pub struct InProcessShardReplicator {
    routing_table: RwLock<ShardRoutingTable>,
    ack_policy: ReplicationAckPolicy,
    peers: Mutex<BTreeMap<ServerId, Neo4rDatabaseHandle>>,
}

pub struct TcpShardReplicator {
    routing_table: RwLock<ShardRoutingTable>,
    ack_policy: ReplicationAckPolicy,
    channel_config: ReplicationChannelConfig,
    channel: Arc<dyn ReplicationChannel>,
    raft_transport: bool,
    peers: Mutex<BTreeMap<ServerId, String>>,
    raft_peer_progress: Mutex<BTreeMap<(ServerId, ShardId), RaftPeerProgress>>,
}

impl TcpShardReplicator {
    pub fn new(routing_table: ShardRoutingTable) -> Self {
        Self {
            routing_table: RwLock::new(routing_table),
            ack_policy: ReplicationAckPolicy::All,
            channel_config: ReplicationChannelConfig::default(),
            channel: Arc::new(TcpReplicationChannel),
            raft_transport: false,
            peers: Mutex::new(BTreeMap::new()),
            raft_peer_progress: Mutex::new(BTreeMap::new()),
        }
    }

    pub fn with_ack_policy(mut self, ack_policy: ReplicationAckPolicy) -> Self {
        self.ack_policy = ack_policy;
        self
    }

    pub fn with_connect_timeout(mut self, connect_timeout: Duration) -> Self {
        self.channel_config.connect_timeout = connect_timeout;
        self
    }

    pub fn with_retry(mut self, max_attempts: usize, retry_backoff: Duration) -> Self {
        self.channel_config.max_attempts = max_attempts.max(1);
        self.channel_config.retry_backoff = retry_backoff;
        self
    }

    pub fn with_channel(mut self, channel: Arc<dyn ReplicationChannel>) -> Self {
        self.channel = channel;
        self
    }

    pub fn with_raft_transport(mut self, enabled: bool) -> Self {
        self.raft_transport = enabled;
        self
    }

    pub fn register_peer(
        &self,
        server_id: ServerId,
        address: impl Into<String>,
    ) -> DatabaseResult<()> {
        self.peers
            .lock()
            .map_err(|_| DatabaseError::LockPoisoned)?
            .insert(server_id, address.into());
        Ok(())
    }

    pub fn unregister_peer(&self, server_id: ServerId) -> DatabaseResult<()> {
        self.peers
            .lock()
            .map_err(|_| DatabaseError::LockPoisoned)?
            .remove(&server_id);
        Ok(())
    }

    pub fn send_raft_heartbeats(&self, committed_indexes: &[LogIndex]) -> DatabaseResult<()> {
        if !self.raft_transport {
            return Ok(());
        }
        let peers = self
            .peers
            .lock()
            .map_err(|_| DatabaseError::LockPoisoned)?
            .clone();
        for (_, address) in peers {
            for (shard_id, leader_commit) in committed_indexes.iter().copied().enumerate() {
                let _ = self.channel.send_raft_append_batch(
                    &address,
                    &self.channel_config,
                    shard_id as ShardId,
                    leader_commit,
                    &[],
                );
            }
        }
        Ok(())
    }

    pub fn run_raft_replication_pump(&self, db: &Neo4rDatabaseHandle) -> DatabaseResult<usize> {
        if !self.raft_transport {
            return Ok(0);
        }
        let peers = self
            .peers
            .lock()
            .map_err(|_| DatabaseError::LockPoisoned)?
            .clone();
        let committed_indexes = db.committed_indexes()?;
        let mut sent = 0_usize;
        for (server_id, address) in peers {
            for (shard_id, leader_commit) in committed_indexes.iter().copied().enumerate() {
                let shard_id = shard_id as ShardId;
                for _ in 0..3 {
                    let progress = self.raft_progress(server_id, shard_id, leader_commit)?;
                    let next_index = progress.next_index;
                    let entries = db
                        .log_entries_from(shard_id, next_index)?
                        .into_iter()
                        .filter(|entry| entry.index <= leader_commit)
                        .collect::<Vec<_>>();
                    let response = self.channel.send_raft_append_batch_once(
                        &address,
                        &self.channel_config,
                        shard_id,
                        leader_commit,
                        &entries,
                    );
                    match response {
                        Ok(response) if response.append.success => {
                            let acked = response
                                .ack_positions
                                .into_iter()
                                .filter(|(acked_shard, _)| *acked_shard == shard_id)
                                .map(|(_, index)| index)
                                .max()
                                .unwrap_or(response.append.match_index);
                            self.set_raft_progress(
                                server_id,
                                shard_id,
                                RaftPeerProgress {
                                    next_index: acked.saturating_add(1),
                                    match_index: acked,
                                },
                            )?;
                            sent += 1;
                            break;
                        }
                        Ok(response) => {
                            if let Some(snapshot) =
                                db.install_snapshot_request_for_shard(shard_id)?
                            {
                                if response.append.conflict_index.is_some_and(|index| {
                                    index <= snapshot.metadata.last_included_index
                                }) {
                                    let installed = self.channel.install_snapshot(
                                        &address,
                                        &self.channel_config,
                                        snapshot,
                                    )?;
                                    if installed.success {
                                        self.set_raft_progress(
                                            server_id,
                                            shard_id,
                                            RaftPeerProgress {
                                                next_index: installed
                                                    .last_included_index
                                                    .saturating_add(1),
                                                match_index: installed.last_included_index,
                                            },
                                        )?;
                                        sent += 1;
                                        break;
                                    }
                                }
                            }
                            self.rewind_raft_progress(server_id, shard_id, &response.append)?;
                        }
                        Err(_) if next_index > 1 => {
                            self.set_raft_progress(
                                server_id,
                                shard_id,
                                RaftPeerProgress {
                                    next_index: next_index.saturating_sub(1),
                                    match_index: progress
                                        .match_index
                                        .min(next_index.saturating_sub(2)),
                                },
                            )?;
                        }
                        Err(_) => break,
                    }
                }
            }
        }
        self.send_raft_heartbeats(&committed_indexes)?;
        Ok(sent)
    }

    fn raft_progress(
        &self,
        server_id: ServerId,
        shard_id: ShardId,
        leader_commit: LogIndex,
    ) -> DatabaseResult<RaftPeerProgress> {
        Ok(self
            .raft_peer_progress
            .lock()
            .map_err(|_| DatabaseError::LockPoisoned)?
            .get(&(server_id, shard_id))
            .copied()
            .unwrap_or(RaftPeerProgress {
                next_index: 1,
                match_index: 0,
            })
            .clamp_to_commit(leader_commit))
    }

    fn set_raft_progress(
        &self,
        server_id: ServerId,
        shard_id: ShardId,
        progress: RaftPeerProgress,
    ) -> DatabaseResult<()> {
        self.raft_peer_progress
            .lock()
            .map_err(|_| DatabaseError::LockPoisoned)?
            .insert((server_id, shard_id), progress);
        Ok(())
    }

    fn rewind_raft_progress(
        &self,
        server_id: ServerId,
        shard_id: ShardId,
        response: &AppendEntriesResponse,
    ) -> DatabaseResult<()> {
        let next_index = response
            .conflict_index
            .unwrap_or_else(|| response.match_index.saturating_add(1))
            .max(1);
        self.set_raft_progress(
            server_id,
            shard_id,
            RaftPeerProgress {
                next_index,
                match_index: response.match_index.min(next_index.saturating_sub(1)),
            },
        )
    }

    pub fn run_raft_election_round(&self, db: &Neo4rDatabaseHandle) -> DatabaseResult<usize> {
        self.run_raft_election_round_with_timeout(db, Duration::from_millis(1500))
    }

    pub fn run_raft_election_round_with_timeout(
        &self,
        db: &Neo4rDatabaseHandle,
        election_timeout: Duration,
    ) -> DatabaseResult<usize> {
        if !self.raft_transport {
            return Ok(0);
        }
        let peers = self
            .peers
            .lock()
            .map_err(|_| DatabaseError::LockPoisoned)?
            .clone();
        let mut elected = 0_usize;
        for shard_id in db.raft_election_candidates(election_timeout)? {
            let request = match db.start_raft_election(shard_id) {
                Ok(request) => request,
                Err(DatabaseError::Replication(message))
                    if message.contains("already raft leader") =>
                {
                    continue;
                }
                Err(err) => return Err(err),
            };
            for (server_id, address) in &peers {
                let response = self.channel.request_vote(
                    &address,
                    &self.channel_config,
                    shard_id,
                    request.clone(),
                );
                match response {
                    Ok(response) => {
                        if db.record_raft_vote_response(shard_id, *server_id, response)? {
                            elected += 1;
                            break;
                        }
                    }
                    Err(_) => continue,
                }
            }
        }
        Ok(elected)
    }

    fn replica_targets(&self, entry: &LogEntry) -> DatabaseResult<Vec<ServerId>> {
        let routing_table = self
            .routing_table
            .read()
            .map_err(|_| DatabaseError::LockPoisoned)?;
        replica_targets(&routing_table, entry)
    }

    fn voter_count(&self, entry: &LogEntry) -> DatabaseResult<usize> {
        let routing_table = self
            .routing_table
            .read()
            .map_err(|_| DatabaseError::LockPoisoned)?;
        voter_count(&routing_table, entry)
    }
}

impl InProcessShardReplicator {
    pub fn new(routing_table: ShardRoutingTable) -> Self {
        Self {
            routing_table: RwLock::new(routing_table),
            ack_policy: ReplicationAckPolicy::All,
            peers: Mutex::new(BTreeMap::new()),
        }
    }

    pub fn with_ack_policy(mut self, ack_policy: ReplicationAckPolicy) -> Self {
        self.ack_policy = ack_policy;
        self
    }

    pub fn register_peer(
        &self,
        server_id: ServerId,
        handle: Neo4rDatabaseHandle,
    ) -> DatabaseResult<()> {
        self.peers
            .lock()
            .map_err(|_| DatabaseError::LockPoisoned)?
            .insert(server_id, handle);
        Ok(())
    }

    pub fn unregister_peer(&self, server_id: ServerId) -> DatabaseResult<()> {
        self.peers
            .lock()
            .map_err(|_| DatabaseError::LockPoisoned)?
            .remove(&server_id);
        Ok(())
    }

    fn replica_targets(&self, entry: &LogEntry) -> DatabaseResult<Vec<ServerId>> {
        let routing_table = self
            .routing_table
            .read()
            .map_err(|_| DatabaseError::LockPoisoned)?;
        replica_targets(&routing_table, entry)
    }

    fn voter_count(&self, entry: &LogEntry) -> DatabaseResult<usize> {
        let routing_table = self
            .routing_table
            .read()
            .map_err(|_| DatabaseError::LockPoisoned)?;
        voter_count(&routing_table, entry)
    }
}

impl ShardReplicator for InProcessShardReplicator {
    fn install_routing_table(&self, routing_table: ShardRoutingTable) -> DatabaseResult<()> {
        *self
            .routing_table
            .write()
            .map_err(|_| DatabaseError::LockPoisoned)? = routing_table;
        Ok(())
    }

    fn publish(&self, entry: &LogEntry) -> DatabaseResult<ReplicationOutcome> {
        self.publish_batch(std::slice::from_ref(entry))
            .and_then(|mut outcomes| {
                outcomes.pop().ok_or_else(|| {
                    DatabaseError::Replication("empty replication batch".to_string())
                })
            })
    }

    fn publish_batch(&self, entries: &[LogEntry]) -> DatabaseResult<Vec<ReplicationOutcome>> {
        let mut outcomes = entries
            .iter()
            .map(|entry| ReplicationOutcome::local(entry.origin_server_id))
            .collect::<Vec<_>>();
        let mut batches = BTreeMap::<ServerId, Vec<(usize, LogEntry)>>::new();
        let mut errors_by_entry = vec![Vec::<String>::new(); entries.len()];
        let mut required_acks = Vec::with_capacity(entries.len());

        for (position, entry) in entries.iter().enumerate() {
            required_acks.push(self.ack_policy.required_acks(self.voter_count(entry)?));
            for target in self.replica_targets(entry)? {
                batches
                    .entry(target)
                    .or_default()
                    .push((position, entry.clone()));
            }
        }
        for (target, indexed_entries) in batches {
            let handle = self
                .peers
                .lock()
                .map_err(|_| DatabaseError::LockPoisoned)?
                .get(&target)
                .cloned();
            let Some(handle) = handle else {
                for (position, entry) in indexed_entries {
                    errors_by_entry[position].push(format!(
                        "missing in-process peer {target} for shard {}",
                        entry.shard_id
                    ));
                }
                continue;
            };

            let replicated_entries = indexed_entries
                .iter()
                .map(|(_, entry)| entry.clone())
                .collect::<Vec<_>>();
            match handle.apply_replicated_entries(replicated_entries) {
                Ok(()) => {
                    for (position, _) in indexed_entries {
                        let entry = &entries[position];
                        outcomes[position].ack(target, entry.shard_id, entry.index);
                    }
                }
                Err(err) => {
                    let message = err.to_string();
                    for (position, _) in indexed_entries {
                        errors_by_entry[position].push(message.clone());
                    }
                }
            }
        }

        for (position, entry) in entries.iter().enumerate() {
            let acked = outcomes[position].acked_server_ids.len();
            if acked < required_acks[position] {
                return Err(DatabaseError::Replication(format!(
                    "replication ack policy {:?} not satisfied for shard {}: got {acked}, required {}; errors: {}",
                    self.ack_policy,
                    entry.shard_id,
                    required_acks[position],
                    errors_by_entry[position].join("; ")
                )));
            }
        }

        Ok(outcomes)
    }
}

impl ShardReplicator for TcpShardReplicator {
    fn install_routing_table(&self, routing_table: ShardRoutingTable) -> DatabaseResult<()> {
        *self
            .routing_table
            .write()
            .map_err(|_| DatabaseError::LockPoisoned)? = routing_table;
        Ok(())
    }

    fn register_peer_address(&self, server_id: ServerId, address: String) -> DatabaseResult<()> {
        self.register_peer(server_id, address)
    }

    fn unregister_peer_address(&self, server_id: ServerId) -> DatabaseResult<()> {
        self.unregister_peer(server_id)
    }

    fn publish(&self, entry: &LogEntry) -> DatabaseResult<ReplicationOutcome> {
        self.publish_batch(std::slice::from_ref(entry))
            .and_then(|mut outcomes| {
                outcomes.pop().ok_or_else(|| {
                    DatabaseError::Replication("empty replication batch".to_string())
                })
            })
    }

    fn publish_batch(&self, entries: &[LogEntry]) -> DatabaseResult<Vec<ReplicationOutcome>> {
        let mut outcomes = entries
            .iter()
            .map(|entry| ReplicationOutcome::local(entry.origin_server_id))
            .collect::<Vec<_>>();
        let mut batches = BTreeMap::<ServerId, Vec<(usize, LogEntry)>>::new();
        let mut errors_by_entry = vec![Vec::<String>::new(); entries.len()];
        let mut required_acks = Vec::with_capacity(entries.len());

        for (position, entry) in entries.iter().enumerate() {
            required_acks.push(self.ack_policy.required_acks(self.voter_count(entry)?));
            for target in self.replica_targets(entry)? {
                batches
                    .entry(target)
                    .or_default()
                    .push((position, entry.clone()));
            }
        }
        let peers = self.peers.lock().map_err(|_| DatabaseError::LockPoisoned)?;
        preflight_tcp_ack_capacity(&peers, &batches, &required_acks)?;
        drop(peers);

        for (target, indexed_entries) in batches {
            let address = self
                .peers
                .lock()
                .map_err(|_| DatabaseError::LockPoisoned)?
                .get(&target)
                .cloned();
            let Some(address) = address else {
                for (position, entry) in indexed_entries {
                    errors_by_entry[position].push(format!(
                        "missing tcp peer {target} for shard {}",
                        entry.shard_id
                    ));
                }
                continue;
            };
            let replicated_entries = indexed_entries
                .iter()
                .map(|(_, entry)| entry.clone())
                .collect::<Vec<_>>();
            let append_result = if self.raft_transport {
                self.channel.send_raft_append_batches_by_shard(
                    &address,
                    &self.channel_config,
                    &replicated_entries,
                )
            } else {
                self.channel.send_replication_batch(
                    &address,
                    &self.channel_config,
                    &replicated_entries,
                )
            };
            match append_result {
                Ok(ack_positions) => {
                    let acked_entries = ack_positions.into_iter().collect::<BTreeSet<_>>();
                    for (position, entry) in indexed_entries {
                        if acked_entries.contains(&(entry.shard_id, entry.index)) {
                            outcomes[position].ack(target, entry.shard_id, entry.index);
                        } else {
                            errors_by_entry[position].push(format!(
                                "tcp peer {target} ack did not include shard {} index {}",
                                entry.shard_id, entry.index
                            ));
                        }
                    }
                }
                Err(err) => {
                    let message = err.to_string();
                    for (position, _) in indexed_entries {
                        errors_by_entry[position].push(message.clone());
                    }
                }
            }
        }

        for (position, entry) in entries.iter().enumerate() {
            let acked = outcomes[position].acked_server_ids.len();
            if acked < required_acks[position] {
                return Err(DatabaseError::Replication(format!(
                    "replication ack policy {:?} not satisfied for shard {}: got {acked}, required {}; errors: {}",
                    self.ack_policy,
                    entry.shard_id,
                    required_acks[position],
                    errors_by_entry[position].join("; ")
                )));
            }
        }

        if self.raft_transport {
            self.publish_raft_commit_heartbeats(entries, &outcomes)?;
        }

        Ok(outcomes)
    }
}

impl TcpShardReplicator {
    fn publish_raft_commit_heartbeats(
        &self,
        entries: &[LogEntry],
        outcomes: &[ReplicationOutcome],
    ) -> DatabaseResult<()> {
        let mut commits_by_peer = BTreeMap::<(ServerId, ShardId), LogIndex>::new();
        for (entry, outcome) in entries.iter().zip(outcomes.iter()) {
            for server_id in &outcome.acked_server_ids {
                if *server_id == entry.origin_server_id {
                    continue;
                }
                let slot = commits_by_peer
                    .entry((*server_id, entry.shard_id))
                    .or_default();
                *slot = (*slot).max(entry.index);
            }
        }
        for ((server_id, shard_id), leader_commit) in commits_by_peer {
            let Some(address) = self
                .peers
                .lock()
                .map_err(|_| DatabaseError::LockPoisoned)?
                .get(&server_id)
                .cloned()
            else {
                continue;
            };
            let _ = self.channel.send_raft_append_batch(
                &address,
                &self.channel_config,
                shard_id,
                leader_commit,
                &[],
            );
        }
        Ok(())
    }
}

mod channel;
mod tcp_requests;
mod tcp_responses;

pub use channel::*;
pub use tcp_requests::*;

#[cfg(test)]
mod tests;
