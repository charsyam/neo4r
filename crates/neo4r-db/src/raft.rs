use crate::{DatabaseError, DatabaseResult};
use neo4r_core::{Command, LogEntry, LogIndex, ServerId, ShardId, Term};
use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

const RAFT_STATE_MAGIC: &str = "N4RRAFT1";

#[path = "raft/persistent.rs"]
mod persistent;
#[path = "raft/snapshot_chunks.rs"]
mod snapshot_chunks;
pub use persistent::{RaftPersistentState, RaftPersistentStateStore};
pub use snapshot_chunks::SnapshotChunkAssembler;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RequestVoteRequest {
    pub term: Term,
    pub candidate_id: ServerId,
    pub last_log_index: LogIndex,
    pub last_log_term: Term,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RequestVoteResponse {
    pub term: Term,
    pub vote_granted: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreVoteRequest {
    pub next_term: Term,
    pub candidate_id: ServerId,
    pub last_log_index: LogIndex,
    pub last_log_term: Term,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreVoteResponse {
    pub term: Term,
    pub vote_granted: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct AppendEntriesRequest {
    pub term: Term,
    pub leader_id: ServerId,
    pub prev_log_index: LogIndex,
    pub prev_log_term: Term,
    pub entries: Vec<LogEntry>,
    pub leader_commit: LogIndex,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AppendEntriesResponse {
    pub term: Term,
    pub success: bool,
    pub match_index: LogIndex,
    pub conflict_index: Option<LogIndex>,
    pub conflict_term: Option<Term>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RaftSnapshotMetadata {
    pub shard_id: ShardId,
    pub last_included_term: Term,
    pub last_included_index: LogIndex,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InstallSnapshotRequest {
    pub term: Term,
    pub leader_id: ServerId,
    pub metadata: RaftSnapshotMetadata,
    pub payload: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InstallSnapshotChunk {
    pub request: InstallSnapshotRequest,
    pub offset: u64,
    pub done: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InstallSnapshotResponse {
    pub term: Term,
    pub success: bool,
    pub last_included_index: LogIndex,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RaftRole {
    Follower,
    Candidate,
    Leader,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RaftMembership {
    voters: BTreeSet<ServerId>,
    outgoing_voters: Option<BTreeSet<ServerId>>,
}

impl RaftMembership {
    pub fn new(voters: impl IntoIterator<Item = ServerId>) -> DatabaseResult<Self> {
        let voters = voters.into_iter().collect::<BTreeSet<_>>();
        if voters.is_empty() {
            return Err(DatabaseError::InvalidConfig(
                "raft membership must contain at least one voter".to_string(),
            ));
        }
        Ok(Self {
            voters,
            outgoing_voters: None,
        })
    }

    pub fn voters(&self) -> &BTreeSet<ServerId> {
        &self.voters
    }

    pub fn outgoing_voters(&self) -> Option<&BTreeSet<ServerId>> {
        self.outgoing_voters.as_ref()
    }

    pub fn is_joint(&self) -> bool {
        self.outgoing_voters.is_some()
    }

    pub fn quorum_size(&self) -> usize {
        (self.voters.len() / 2) + 1
    }

    pub fn contains(&self, server_id: ServerId) -> bool {
        self.voters.contains(&server_id)
            || self
                .outgoing_voters
                .as_ref()
                .is_some_and(|voters| voters.contains(&server_id))
    }

    fn has_quorum(&self, matched: &BTreeSet<ServerId>) -> bool {
        has_majority(&self.voters, matched)
            && self
                .outgoing_voters
                .as_ref()
                .is_none_or(|voters| has_majority(voters, matched))
    }

    fn all_voters(&self) -> BTreeSet<ServerId> {
        let mut voters = self.voters.clone();
        if let Some(outgoing) = &self.outgoing_voters {
            voters.extend(outgoing.iter().copied());
        }
        voters
    }

    fn enter_joint(&mut self, next_voters: BTreeSet<ServerId>) -> DatabaseResult<()> {
        if next_voters.is_empty() {
            return Err(DatabaseError::InvalidConfig(
                "raft joint membership cannot be empty".to_string(),
            ));
        }
        self.outgoing_voters = Some(self.voters.clone());
        self.voters = next_voters;
        Ok(())
    }

    fn finalize_joint(&mut self) {
        self.outgoing_voters = None;
    }

    fn add_voter(&mut self, server_id: ServerId) {
        self.voters.insert(server_id);
    }

    fn remove_voter(&mut self, server_id: ServerId) -> DatabaseResult<()> {
        if self.voters.len() == 1 && self.voters.contains(&server_id) {
            return Err(DatabaseError::InvalidConfig(
                "raft membership cannot remove the last voter".to_string(),
            ));
        }
        self.voters.remove(&server_id);
        Ok(())
    }
}

fn has_majority(voters: &BTreeSet<ServerId>, matched: &BTreeSet<ServerId>) -> bool {
    matched.intersection(voters).count() >= voters.len() / 2 + 1
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RaftMembershipChange {
    AddVoter(ServerId),
    RemoveVoter(ServerId),
}

pub struct RaftCore {
    server_id: ServerId,
    shard_id: ShardId,
    state_store: RaftPersistentStateStore,
    persistent: RaftPersistentState,
    log: Vec<LogEntry>,
    snapshot: Option<RaftSnapshotMetadata>,
    commit_index: LogIndex,
    role: RaftRole,
    leader_id: Option<ServerId>,
    membership: RaftMembership,
    match_indexes: BTreeMap<ServerId, LogIndex>,
    votes_received: BTreeSet<ServerId>,
    lease_deadline: Option<Instant>,
    lease_duration: Duration,
    lease_clock_drift_bound: Duration,
    lease_message_delay_bound: Duration,
}

impl RaftCore {
    pub fn open(
        server_id: ServerId,
        shard_id: ShardId,
        state_store: RaftPersistentStateStore,
    ) -> DatabaseResult<Self> {
        Self::open_with_membership(
            server_id,
            shard_id,
            state_store,
            RaftMembership::new([server_id])?,
        )
    }

    pub fn open_with_membership(
        server_id: ServerId,
        shard_id: ShardId,
        state_store: RaftPersistentStateStore,
        membership: RaftMembership,
    ) -> DatabaseResult<Self> {
        if !membership.contains(server_id) {
            return Err(DatabaseError::InvalidConfig(format!(
                "local server {server_id} is not a raft voter"
            )));
        }
        let persistent = state_store.load()?;
        Ok(Self {
            server_id,
            shard_id,
            state_store,
            persistent,
            log: Vec::new(),
            snapshot: None,
            commit_index: 0,
            role: RaftRole::Follower,
            leader_id: None,
            membership,
            match_indexes: BTreeMap::new(),
            votes_received: BTreeSet::new(),
            lease_deadline: None,
            lease_duration: Duration::from_millis(750),
            lease_clock_drift_bound: Duration::ZERO,
            lease_message_delay_bound: Duration::ZERO,
        })
    }

    pub fn open_with_log(
        server_id: ServerId,
        shard_id: ShardId,
        state_store: RaftPersistentStateStore,
        membership: RaftMembership,
        log: Vec<LogEntry>,
        commit_index: LogIndex,
    ) -> DatabaseResult<Self> {
        let mut raft = Self::open_with_membership(server_id, shard_id, state_store, membership)?;
        for entry in log {
            if entry.shard_id != shard_id {
                return Err(DatabaseError::InvalidConfig(format!(
                    "raft log entry has shard {}, expected {shard_id}",
                    entry.shard_id
                )));
            }
            raft.log.push(entry);
        }
        raft.log.sort_by_key(|entry| entry.index);
        raft.commit_index = commit_index.min(raft.last_log_index());
        Ok(raft)
    }

    pub fn open_with_log_and_snapshot(
        server_id: ServerId,
        shard_id: ShardId,
        state_store: RaftPersistentStateStore,
        membership: RaftMembership,
        log: Vec<LogEntry>,
        snapshot: Option<RaftSnapshotMetadata>,
        commit_index: LogIndex,
    ) -> DatabaseResult<Self> {
        let mut raft = Self::open_with_log(
            server_id,
            shard_id,
            state_store,
            membership,
            log,
            commit_index,
        )?;
        if let Some(snapshot) = snapshot {
            if snapshot.shard_id != shard_id {
                return Err(DatabaseError::InvalidConfig(format!(
                    "raft snapshot has shard {}, expected {shard_id}",
                    snapshot.shard_id
                )));
            }
            raft.snapshot = Some(snapshot);
            raft.compact_log_through_snapshot();
            raft.commit_index = raft.commit_index.max(raft.snapshot_index());
        }
        Ok(raft)
    }

    pub fn current_term(&self) -> Term {
        self.persistent.current_term
    }

    pub fn shard_id(&self) -> ShardId {
        self.shard_id
    }

    pub fn voted_for(&self) -> Option<ServerId> {
        self.persistent.voted_for
    }

    pub fn role(&self) -> &RaftRole {
        &self.role
    }

    pub fn leader_id(&self) -> Option<ServerId> {
        self.leader_id
    }

    pub fn commit_index(&self) -> LogIndex {
        self.commit_index
    }

    pub fn log(&self) -> &[LogEntry] {
        &self.log
    }

    pub fn snapshot(&self) -> Option<&RaftSnapshotMetadata> {
        self.snapshot.as_ref()
    }

    pub fn membership(&self) -> &RaftMembership {
        &self.membership
    }

    pub fn last_log_index(&self) -> LogIndex {
        self.log
            .last()
            .map(|entry| entry.index)
            .unwrap_or_else(|| self.snapshot_index())
    }

    pub fn last_log_term(&self) -> Term {
        self.log
            .last()
            .map(|entry| entry.term)
            .unwrap_or_else(|| self.snapshot_term())
    }

    pub fn start_election(&mut self) -> DatabaseResult<RequestVoteRequest> {
        self.role = RaftRole::Candidate;
        self.leader_id = None;
        self.persistent.current_term = self.persistent.current_term.saturating_add(1);
        self.persistent.voted_for = Some(self.server_id);
        self.votes_received.clear();
        self.votes_received.insert(self.server_id);
        self.state_store.save(&self.persistent)?;
        Ok(RequestVoteRequest {
            term: self.persistent.current_term,
            candidate_id: self.server_id,
            last_log_index: self.last_log_index(),
            last_log_term: self.last_log_term(),
        })
    }

    pub fn pre_vote_request(&self) -> PreVoteRequest {
        PreVoteRequest {
            next_term: self.persistent.current_term.saturating_add(1),
            candidate_id: self.server_id,
            last_log_index: self.last_log_index(),
            last_log_term: self.last_log_term(),
        }
    }

    pub fn become_leader(&mut self) {
        self.role = RaftRole::Leader;
        self.leader_id = Some(self.server_id);
        self.votes_received.clear();
        self.match_indexes.clear();
        self.match_indexes
            .insert(self.server_id, self.last_log_index());
        self.refresh_leader_lease_if_quorum();
    }

    pub fn with_lease_duration(mut self, lease_duration: Duration) -> Self {
        self.lease_duration = lease_duration;
        self
    }

    pub fn with_lease_clock_bounds(
        mut self,
        clock_drift_bound: Duration,
        message_delay_bound: Duration,
    ) -> Self {
        self.lease_clock_drift_bound = clock_drift_bound;
        self.lease_message_delay_bound = message_delay_bound;
        self
    }

    pub fn record_vote_response(
        &mut self,
        voter_id: ServerId,
        response: RequestVoteResponse,
    ) -> DatabaseResult<bool> {
        if response.term > self.persistent.current_term {
            self.step_down(response.term)?;
            return Ok(false);
        }
        if self.role != RaftRole::Candidate || response.term != self.persistent.current_term {
            return Ok(false);
        }
        if response.vote_granted {
            self.votes_received.insert(voter_id);
        }
        if self.membership.has_quorum(&self.votes_received) {
            self.become_leader();
            return Ok(true);
        }
        Ok(false)
    }

    pub fn pre_vote(&self, request: PreVoteRequest) -> PreVoteResponse {
        let vote_granted = request.next_term >= self.persistent.current_term
            && self.membership.contains(request.candidate_id)
            && self.is_log_up_to_date(request.last_log_index, request.last_log_term);
        PreVoteResponse {
            term: self.persistent.current_term,
            vote_granted,
        }
    }

    pub fn request_vote(
        &mut self,
        request: RequestVoteRequest,
    ) -> DatabaseResult<RequestVoteResponse> {
        if request.term < self.persistent.current_term {
            return Ok(RequestVoteResponse {
                term: self.persistent.current_term,
                vote_granted: false,
            });
        }

        if request.term > self.persistent.current_term {
            self.step_down(request.term)?;
        }

        let can_vote = self.persistent.voted_for.is_none()
            || self.persistent.voted_for == Some(request.candidate_id);
        let vote_granted =
            can_vote && self.is_log_up_to_date(request.last_log_index, request.last_log_term);

        if vote_granted {
            self.persistent.voted_for = Some(request.candidate_id);
            self.state_store.save(&self.persistent)?;
        }

        Ok(RequestVoteResponse {
            term: self.persistent.current_term,
            vote_granted,
        })
    }

    pub fn leader_transfer_request(
        &self,
        transferee_id: ServerId,
    ) -> DatabaseResult<RequestVoteRequest> {
        if self.role != RaftRole::Leader {
            return Err(DatabaseError::Replication(
                "leader transfer requires raft leader".to_string(),
            ));
        }
        if !self.membership.contains(transferee_id) {
            return Err(DatabaseError::Replication(format!(
                "leader transfer target {transferee_id} is not a voter"
            )));
        }
        if self
            .match_indexes
            .get(&transferee_id)
            .copied()
            .unwrap_or_default()
            < self.commit_index
        {
            return Err(DatabaseError::Replication(format!(
                "leader transfer target {transferee_id} is behind commit index {}",
                self.commit_index
            )));
        }
        Ok(RequestVoteRequest {
            term: self.persistent.current_term.saturating_add(1),
            candidate_id: transferee_id,
            last_log_index: self.last_log_index(),
            last_log_term: self.last_log_term(),
        })
    }

    pub fn append_entries(
        &mut self,
        request: AppendEntriesRequest,
    ) -> DatabaseResult<AppendEntriesResponse> {
        if request.term < self.persistent.current_term {
            return Ok(AppendEntriesResponse {
                term: self.persistent.current_term,
                success: false,
                match_index: self.last_log_index(),
                conflict_index: Some(self.last_log_index().saturating_add(1)),
                conflict_term: Some(self.persistent.current_term),
            });
        }

        if request.term > self.persistent.current_term || self.role != RaftRole::Follower {
            self.step_down(request.term)?;
        }
        self.leader_id = Some(request.leader_id);

        if request.prev_log_index > 0 {
            let Some(previous) = self.entry_at(request.prev_log_index) else {
                if request.prev_log_index == self.snapshot_index()
                    && request.prev_log_term == self.snapshot_term()
                {
                    // The leader is continuing from the installed snapshot boundary.
                } else {
                    return Ok(AppendEntriesResponse {
                        term: self.persistent.current_term,
                        success: false,
                        match_index: self.last_log_index(),
                        conflict_index: Some(self.last_log_index().saturating_add(1)),
                        conflict_term: None,
                    });
                }
                for entry in request.entries {
                    if entry.shard_id != self.shard_id {
                        return Err(DatabaseError::InvalidConfig(format!(
                            "raft append entry has shard {}, expected {}",
                            entry.shard_id, self.shard_id
                        )));
                    }
                    if entry.index <= self.snapshot_index() {
                        continue;
                    }
                    match self.entry_at(entry.index) {
                        Some(existing) if existing.term == entry.term => {}
                        Some(_) => {
                            self.truncate_from(entry.index);
                            self.log.push(entry);
                        }
                        None => self.log.push(entry),
                    }
                }
                self.log.sort_by_key(|entry| entry.index);
                if request.leader_commit > self.commit_index {
                    self.commit_index = request.leader_commit.min(self.last_log_index());
                }
                return Ok(AppendEntriesResponse {
                    term: self.persistent.current_term,
                    success: true,
                    match_index: self.last_log_index(),
                    conflict_index: None,
                    conflict_term: None,
                });
            };
            if previous.term != request.prev_log_term {
                let conflict_term = previous.term;
                let conflict_index = self
                    .log
                    .iter()
                    .find(|entry| entry.term == conflict_term)
                    .map(|entry| entry.index)
                    .unwrap_or(request.prev_log_index);
                self.truncate_from(request.prev_log_index);
                return Ok(AppendEntriesResponse {
                    term: self.persistent.current_term,
                    success: false,
                    match_index: self.last_log_index(),
                    conflict_index: Some(conflict_index),
                    conflict_term: Some(conflict_term),
                });
            }
        }

        for entry in request.entries {
            if entry.shard_id != self.shard_id {
                return Err(DatabaseError::InvalidConfig(format!(
                    "raft append entry has shard {}, expected {}",
                    entry.shard_id, self.shard_id
                )));
            }
            if entry.index <= self.snapshot_index() {
                continue;
            }
            match self.entry_at(entry.index) {
                Some(existing) if existing.term == entry.term => {}
                Some(_) => {
                    self.truncate_from(entry.index);
                    self.log.push(entry);
                }
                None => self.log.push(entry),
            }
        }

        self.log.sort_by_key(|entry| entry.index);
        if request.leader_commit > self.commit_index {
            self.commit_index = request.leader_commit.min(self.last_log_index());
        }

        Ok(AppendEntriesResponse {
            term: self.persistent.current_term,
            success: true,
            match_index: self.last_log_index(),
            conflict_index: None,
            conflict_term: None,
        })
    }

    pub fn install_snapshot(
        &mut self,
        request: InstallSnapshotRequest,
    ) -> DatabaseResult<InstallSnapshotResponse> {
        if request.term < self.persistent.current_term {
            return Ok(InstallSnapshotResponse {
                term: self.persistent.current_term,
                success: false,
                last_included_index: self.snapshot_index(),
            });
        }
        if request.metadata.shard_id != self.shard_id {
            return Err(DatabaseError::InvalidConfig(format!(
                "raft snapshot has shard {}, expected {}",
                request.metadata.shard_id, self.shard_id
            )));
        }
        if request.term > self.persistent.current_term || self.role != RaftRole::Follower {
            self.step_down(request.term)?;
        }
        self.leader_id = Some(request.leader_id);
        if request.metadata.last_included_index >= self.snapshot_index() {
            self.snapshot = Some(request.metadata);
            self.compact_log_through_snapshot();
            self.commit_index = self.commit_index.max(self.snapshot_index());
        }
        Ok(InstallSnapshotResponse {
            term: self.persistent.current_term,
            success: true,
            last_included_index: self.snapshot_index(),
        })
    }

    pub fn compact_log_to_snapshot(
        &mut self,
        metadata: RaftSnapshotMetadata,
    ) -> DatabaseResult<()> {
        if metadata.shard_id != self.shard_id {
            return Err(DatabaseError::InvalidConfig(format!(
                "raft snapshot has shard {}, expected {}",
                metadata.shard_id, self.shard_id
            )));
        }
        if metadata.last_included_index > self.commit_index {
            return Err(DatabaseError::Replication(format!(
                "cannot compact raft shard {} to uncommitted snapshot index {} while commit index is {}",
                self.shard_id, metadata.last_included_index, self.commit_index
            )));
        }
        self.snapshot = Some(metadata);
        self.compact_log_through_snapshot();
        Ok(())
    }

    pub fn append_local_entry(&mut self, command: Command) -> DatabaseResult<LogEntry> {
        if self.role != RaftRole::Leader {
            return Err(DatabaseError::Replication(
                "cannot append local raft entry when not leader".to_string(),
            ));
        }
        let entry = LogEntry::new(
            self.shard_id,
            self.persistent.current_term,
            self.last_log_index().saturating_add(1),
            command,
        );
        self.log.push(entry.clone());
        self.match_indexes.insert(self.server_id, entry.index);
        self.advance_leader_commit_index();
        Ok(entry)
    }

    pub fn append_existing_local_entry(&mut self, entry: LogEntry) -> DatabaseResult<LogEntry> {
        if self.role != RaftRole::Leader {
            return Err(DatabaseError::Replication(
                "cannot append local raft entry when not leader".to_string(),
            ));
        }
        if entry.shard_id != self.shard_id {
            return Err(DatabaseError::InvalidConfig(format!(
                "raft local entry has shard {}, expected {}",
                entry.shard_id, self.shard_id
            )));
        }
        let expected = self.last_log_index().saturating_add(1);
        if entry.index != expected {
            return Err(DatabaseError::UnexpectedLogIndex {
                shard_id: entry.shard_id,
                expected,
                actual: entry.index,
            });
        }
        if entry.term != self.persistent.current_term {
            return Err(DatabaseError::LogConflict {
                shard_id: entry.shard_id,
                index: entry.index,
                message: format!(
                    "local raft entry term {} does not match current term {}",
                    entry.term, self.persistent.current_term
                ),
            });
        }
        self.log.push(entry.clone());
        self.match_indexes.insert(self.server_id, entry.index);
        self.advance_leader_commit_index();
        Ok(entry)
    }

    pub fn record_replication_match(
        &mut self,
        server_id: ServerId,
        match_index: LogIndex,
    ) -> DatabaseResult<LogIndex> {
        if self.role != RaftRole::Leader {
            return Err(DatabaseError::Replication(
                "cannot record raft replication match when not leader".to_string(),
            ));
        }
        if !self.membership.contains(server_id) {
            return Err(DatabaseError::InvalidConfig(format!(
                "server {server_id} is not a raft voter"
            )));
        }
        self.match_indexes.insert(server_id, match_index);
        self.advance_leader_commit_index();
        self.refresh_leader_lease_if_quorum();
        Ok(self.commit_index)
    }

    pub fn read_index(&self) -> DatabaseResult<LogIndex> {
        if self.role != RaftRole::Leader {
            return Err(DatabaseError::Replication(
                "read-index requires raft leader".to_string(),
            ));
        }
        let replicated = self
            .membership
            .all_voters()
            .into_iter()
            .filter(|server_id| {
                self.match_indexes
                    .get(server_id)
                    .copied()
                    .unwrap_or_default()
                    >= self.commit_index
            })
            .collect::<BTreeSet<_>>();
        if !self.membership.has_quorum(&replicated) {
            return Err(DatabaseError::Replication(
                "read-index requires a quorum-confirmed leader commit index".to_string(),
            ));
        }
        Ok(self.commit_index)
    }

    pub fn leader_lease_read_index(&self) -> DatabaseResult<LogIndex> {
        if self.role != RaftRole::Leader {
            return Err(DatabaseError::Replication(
                "leader lease requires raft leader".to_string(),
            ));
        }
        let required = self
            .lease_clock_drift_bound
            .saturating_add(self.lease_message_delay_bound);
        if !required.is_zero() && self.lease_duration <= required {
            return Err(DatabaseError::Replication(format!(
                "leader lease duration {:?} must exceed configured clock/message bound {required:?}",
                self.lease_duration
            )));
        }
        if self
            .lease_deadline
            .is_some_and(|deadline| Instant::now() <= deadline)
        {
            return Ok(self.commit_index);
        }
        self.read_index()
    }

    pub fn leader_lease_remaining_millis(&self) -> u64 {
        self.lease_deadline
            .and_then(|deadline| deadline.checked_duration_since(Instant::now()))
            .map(|duration| duration.as_millis() as u64)
            .unwrap_or_default()
    }

    pub fn begin_joint_consensus(
        &mut self,
        voters: impl IntoIterator<Item = ServerId>,
    ) -> DatabaseResult<()> {
        if self.membership.is_joint() {
            return Err(DatabaseError::InvalidConfig(
                "raft membership is already in joint consensus".to_string(),
            ));
        }
        self.membership
            .enter_joint(voters.into_iter().collect::<BTreeSet<_>>())
    }

    pub fn finalize_joint_consensus(&mut self) {
        self.membership.finalize_joint();
    }

    pub fn apply_committed_membership_change(
        &mut self,
        change: RaftMembershipChange,
    ) -> DatabaseResult<()> {
        match change {
            RaftMembershipChange::AddVoter(server_id) => self.membership.add_voter(server_id),
            RaftMembershipChange::RemoveVoter(server_id) => {
                self.membership.remove_voter(server_id)?;
                self.match_indexes.remove(&server_id);
                if server_id == self.server_id {
                    self.role = RaftRole::Follower;
                    self.leader_id = None;
                }
            }
        }
        Ok(())
    }

    fn step_down(&mut self, term: Term) -> DatabaseResult<()> {
        self.role = RaftRole::Follower;
        self.leader_id = None;
        self.votes_received.clear();
        self.lease_deadline = None;
        if term > self.persistent.current_term {
            self.persistent.current_term = term;
            self.persistent.voted_for = None;
            self.state_store.save(&self.persistent)?;
        }
        Ok(())
    }

    fn entry_at(&self, index: LogIndex) -> Option<&LogEntry> {
        self.log.iter().find(|entry| entry.index == index)
    }

    fn snapshot_index(&self) -> LogIndex {
        self.snapshot
            .as_ref()
            .map(|snapshot| snapshot.last_included_index)
            .unwrap_or_default()
    }

    fn snapshot_term(&self) -> Term {
        self.snapshot
            .as_ref()
            .map(|snapshot| snapshot.last_included_term)
            .unwrap_or_default()
    }

    fn truncate_from(&mut self, index: LogIndex) {
        self.log.retain(|entry| entry.index < index);
        self.commit_index = self
            .commit_index
            .min(index.saturating_sub(1))
            .max(self.snapshot_index());
    }

    fn compact_log_through_snapshot(&mut self) {
        let snapshot_index = self.snapshot_index();
        self.log.retain(|entry| entry.index > snapshot_index);
    }

    fn advance_leader_commit_index(&mut self) {
        if self.role != RaftRole::Leader {
            return;
        }
        let mut matched = self
            .membership
            .all_voters()
            .into_iter()
            .map(|server_id| {
                self.match_indexes
                    .get(&server_id)
                    .copied()
                    .unwrap_or_default()
            })
            .collect::<Vec<_>>();
        matched.sort_unstable_by(|left, right| right.cmp(left));
        let mut quorum_index = 0;
        for candidate in matched {
            let matched_voters = self
                .membership
                .all_voters()
                .into_iter()
                .filter(|server_id| {
                    self.match_indexes
                        .get(server_id)
                        .copied()
                        .unwrap_or_default()
                        >= candidate
                })
                .collect::<BTreeSet<_>>();
            if self.membership.has_quorum(&matched_voters) {
                quorum_index = candidate;
                break;
            }
        }
        if quorum_index <= self.commit_index {
            return;
        }
        if self
            .entry_at(quorum_index)
            .is_some_and(|entry| entry.term == self.persistent.current_term)
        {
            self.commit_index = quorum_index;
        }
    }

    fn refresh_leader_lease_if_quorum(&mut self) {
        if self.role != RaftRole::Leader {
            self.lease_deadline = None;
            return;
        }
        let matched = self
            .membership
            .all_voters()
            .into_iter()
            .filter(|server_id| {
                self.match_indexes
                    .get(server_id)
                    .copied()
                    .unwrap_or_default()
                    >= self.commit_index
            })
            .collect::<BTreeSet<_>>();
        if self.membership.has_quorum(&matched) {
            self.lease_deadline = Some(Instant::now() + self.lease_duration);
        }
    }

    fn is_log_up_to_date(&self, last_log_index: LogIndex, last_log_term: Term) -> bool {
        last_log_term > self.last_log_term()
            || (last_log_term == self.last_log_term() && last_log_index >= self.last_log_index())
    }
}

impl InstallSnapshotRequest {
    pub fn chunks(&self, max_payload_bytes: usize) -> Vec<InstallSnapshotChunk> {
        let max_payload_bytes = max_payload_bytes.max(1);
        if self.payload.is_empty() {
            return vec![InstallSnapshotChunk {
                request: self.clone(),
                offset: 0,
                done: true,
            }];
        }
        self.payload
            .chunks(max_payload_bytes)
            .enumerate()
            .map(|(index, payload)| {
                let offset = (index * max_payload_bytes) as u64;
                let mut request = self.clone();
                request.payload = payload.to_vec();
                InstallSnapshotChunk {
                    request,
                    offset,
                    done: offset as usize + payload.len() >= self.payload.len(),
                }
            })
            .collect()
    }
}

#[cfg(test)]
mod tests;
