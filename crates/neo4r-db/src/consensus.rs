use crate::{DatabaseError, DatabaseResult, ReplicationOutcome, ShardReplicator};
use neo4r_core::{
    Command, HybridClock, LogEntry, LogIndex, ServerId, ShardId, ShardRoutingTable, Term,
};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ShardCommitState {
    pub shard_id: ShardId,
    pub term: Term,
    pub commit_index: LogIndex,
    pub last_log_index: LogIndex,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ProposedCommand {
    pub shard_id: ShardId,
    pub command: Command,
}

pub trait ShardConsensus: Send + Sync {
    fn propose(&self, command: ProposedCommand) -> DatabaseResult<LogEntry>;
    fn append_entries(&self, entries: Vec<LogEntry>) -> DatabaseResult<LogIndex>;
    fn commit_state(&self, shard_id: ShardId) -> DatabaseResult<ShardCommitState>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct InProcessShardState {
    current_term: Term,
    next_index: LogIndex,
    commit_index: LogIndex,
}

impl Default for InProcessShardState {
    fn default() -> Self {
        Self {
            current_term: 1,
            next_index: 1,
            commit_index: 0,
        }
    }
}

pub struct StaticPrimaryShardReplication {
    server_id: ServerId,
    routing_table: ShardRoutingTable,
    replicator: Arc<dyn ShardReplicator>,
    clock: Mutex<HybridClock>,
    states: Mutex<BTreeMap<ShardId, InProcessShardState>>,
}

pub type InProcessShardConsensus = StaticPrimaryShardReplication;

impl StaticPrimaryShardReplication {
    pub fn new(
        server_id: ServerId,
        routing_table: ShardRoutingTable,
        replicator: Arc<dyn ShardReplicator>,
    ) -> Self {
        let states = routing_table
            .placements
            .iter()
            .map(|placement| (placement.shard_id, InProcessShardState::default()))
            .collect();
        Self {
            server_id,
            routing_table,
            replicator,
            clock: Mutex::new(HybridClock::new()),
            states: Mutex::new(states),
        }
    }

    fn primary_server_id(&self, shard_id: ShardId) -> Option<ServerId> {
        self.routing_table.primary_server_id(shard_id)
    }

    fn ensure_local_primary(&self, shard_id: ShardId) -> DatabaseResult<()> {
        let primary_server_id = self.primary_server_id(shard_id);
        if primary_server_id != Some(self.server_id) {
            return Err(DatabaseError::ShardNotPrimary {
                shard_id,
                server_id: self.server_id,
                primary_server_id,
            });
        }
        Ok(())
    }

    fn allocate_entry(&self, proposed: ProposedCommand) -> DatabaseResult<LogEntry> {
        self.ensure_local_primary(proposed.shard_id)?;
        let (term, index) = {
            let mut states = self
                .states
                .lock()
                .map_err(|_| DatabaseError::LockPoisoned)?;
            let state = states.entry(proposed.shard_id).or_default();
            let term = state.current_term;
            let index = state.next_index;
            state.next_index = state.next_index.saturating_add(1);
            (term, index)
        };
        let timestamp = self
            .clock
            .lock()
            .map_err(|_| DatabaseError::LockPoisoned)?
            .tick();
        Ok(LogEntry::new_with_metadata(
            proposed.shard_id,
            term,
            index,
            self.server_id,
            self.routing_table.version,
            timestamp,
            proposed.command,
        ))
    }

    fn rollback_proposal(&self, entry: &LogEntry) -> DatabaseResult<()> {
        let mut states = self
            .states
            .lock()
            .map_err(|_| DatabaseError::LockPoisoned)?;
        let Some(state) = states.get_mut(&entry.shard_id) else {
            return Ok(());
        };
        if state.next_index == entry.index.saturating_add(1)
            && state.commit_index < entry.index
            && state.current_term == entry.term
        {
            state.next_index = entry.index;
        }
        Ok(())
    }

    fn mark_committed(&self, entry: &LogEntry, _outcome: ReplicationOutcome) -> DatabaseResult<()> {
        let mut states = self
            .states
            .lock()
            .map_err(|_| DatabaseError::LockPoisoned)?;
        let state = states.entry(entry.shard_id).or_default();
        state.current_term = state.current_term.max(entry.term);
        state.next_index = state.next_index.max(entry.index.saturating_add(1));
        state.commit_index = state.commit_index.max(entry.index);
        Ok(())
    }

    fn append_entry(&self, entry: &LogEntry) -> DatabaseResult<()> {
        let mut states = self
            .states
            .lock()
            .map_err(|_| DatabaseError::LockPoisoned)?;
        let state = states.entry(entry.shard_id).or_default();

        if entry.term < state.current_term {
            return Err(DatabaseError::LogConflict {
                shard_id: entry.shard_id,
                index: entry.index,
                message: format!(
                    "entry term {} is older than current term {}",
                    entry.term, state.current_term
                ),
            });
        }
        if entry.index < state.next_index {
            state.current_term = state.current_term.max(entry.term);
            state.commit_index = state.commit_index.max(entry.index);
            return Ok(());
        }
        if entry.index > state.next_index {
            return Err(DatabaseError::UnexpectedLogIndex {
                shard_id: entry.shard_id,
                expected: state.next_index,
                actual: entry.index,
            });
        }

        state.current_term = state.current_term.max(entry.term);
        state.next_index = entry.index.saturating_add(1);
        state.commit_index = state.commit_index.max(entry.index);
        Ok(())
    }
}

impl ShardConsensus for StaticPrimaryShardReplication {
    fn propose(&self, command: ProposedCommand) -> DatabaseResult<LogEntry> {
        let entry = self.allocate_entry(command)?;
        match self.replicator.publish(&entry) {
            Ok(outcome) => {
                self.mark_committed(&entry, outcome)?;
                Ok(entry)
            }
            Err(err) => {
                self.rollback_proposal(&entry)?;
                Err(err)
            }
        }
    }

    fn append_entries(&self, entries: Vec<LogEntry>) -> DatabaseResult<LogIndex> {
        let mut last_index = 0;
        for entry in entries {
            self.append_entry(&entry)?;
            last_index = last_index.max(entry.index);
            self.clock
                .lock()
                .map_err(|_| DatabaseError::LockPoisoned)?
                .observe(entry.timestamp);
        }
        Ok(last_index)
    }

    fn commit_state(&self, shard_id: ShardId) -> DatabaseResult<ShardCommitState> {
        let states = self
            .states
            .lock()
            .map_err(|_| DatabaseError::LockPoisoned)?;
        let state = states.get(&shard_id).cloned().unwrap_or_default();
        Ok(ShardCommitState {
            shard_id,
            term: state.current_term,
            commit_index: state.commit_index,
            last_log_index: state.next_index.saturating_sub(1),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use neo4r_core::{Properties, ShardPlacement, ShardReplica};

    #[derive(Default)]
    struct RecordingReplicator {
        entries: Mutex<Vec<LogEntry>>,
    }

    impl RecordingReplicator {
        fn entries(&self) -> Vec<LogEntry> {
            self.entries.lock().unwrap().clone()
        }
    }

    impl ShardReplicator for RecordingReplicator {
        fn publish(&self, entry: &LogEntry) -> DatabaseResult<ReplicationOutcome> {
            self.entries.lock().unwrap().push(entry.clone());
            Ok(ReplicationOutcome::local(entry.origin_server_id))
        }
    }

    fn create_node(id: u64) -> Command {
        Command::CreateNode {
            id,
            labels: vec!["Person".to_string()],
            properties: Properties::new(),
        }
    }

    fn routing_table() -> ShardRoutingTable {
        ShardRoutingTable {
            version: 7,
            placements: vec![ShardPlacement::new(
                0,
                vec![ShardReplica::primary(10), ShardReplica::replica(11)],
            )],
        }
    }

    #[test]
    fn propose_requires_local_primary() {
        let replicator = Arc::new(RecordingReplicator::default());
        let consensus = StaticPrimaryShardReplication::new(11, routing_table(), replicator);

        let err = consensus
            .propose(ProposedCommand {
                shard_id: 0,
                command: create_node(1),
            })
            .unwrap_err();

        assert!(matches!(
            err,
            DatabaseError::ShardNotPrimary {
                shard_id: 0,
                server_id: 11,
                primary_server_id: Some(10),
            }
        ));
    }

    #[test]
    fn propose_allocates_index_and_replicates() {
        let replicator = Arc::new(RecordingReplicator::default());
        let consensus = StaticPrimaryShardReplication::new(10, routing_table(), replicator.clone());

        let entry = consensus
            .propose(ProposedCommand {
                shard_id: 0,
                command: create_node(1),
            })
            .unwrap();

        assert_eq!(entry.shard_id, 0);
        assert_eq!(entry.term, 1);
        assert_eq!(entry.index, 1);
        assert_eq!(entry.origin_server_id, 10);
        assert_eq!(entry.config_version, 7);
        assert_eq!(replicator.entries(), vec![entry.clone()]);
        assert_eq!(
            consensus.commit_state(0).unwrap(),
            ShardCommitState {
                shard_id: 0,
                term: 1,
                commit_index: 1,
                last_log_index: 1,
            }
        );
    }

    #[test]
    fn append_entries_rejects_gaps() {
        let replicator = Arc::new(RecordingReplicator::default());
        let consensus = StaticPrimaryShardReplication::new(11, routing_table(), replicator);
        let entry =
            LogEntry::new_with_metadata(0, 1, 2, 10, 7, HybridClock::new().tick(), create_node(2));

        let err = consensus.append_entries(vec![entry]).unwrap_err();

        assert!(matches!(
            err,
            DatabaseError::UnexpectedLogIndex {
                shard_id: 0,
                expected: 1,
                actual: 2,
            }
        ));
    }

    #[test]
    fn append_entries_advances_term_and_commit_state() {
        let replicator = Arc::new(RecordingReplicator::default());
        let consensus = StaticPrimaryShardReplication::new(11, routing_table(), replicator);
        let entry =
            LogEntry::new_with_metadata(0, 3, 1, 10, 7, HybridClock::new().tick(), create_node(1));

        assert_eq!(consensus.append_entries(vec![entry]).unwrap(), 1);
        assert_eq!(
            consensus.commit_state(0).unwrap(),
            ShardCommitState {
                shard_id: 0,
                term: 3,
                commit_index: 1,
                last_log_index: 1,
            }
        );
    }
}
