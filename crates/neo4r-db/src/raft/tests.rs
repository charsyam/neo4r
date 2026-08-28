use super::*;
use neo4r_core::Properties;
use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[test]
fn request_vote_persists_term_and_vote() {
    let dir = temp_dir("neo4r-raft-vote");
    let store = RaftPersistentStateStore::open(dir.join("state.txt"));
    let mut raft = RaftCore::open(1, 0, store.clone()).unwrap();

    let response = raft
        .request_vote(RequestVoteRequest {
            term: 2,
            candidate_id: 7,
            last_log_index: 0,
            last_log_term: 0,
        })
        .unwrap();
    assert!(response.vote_granted);

    let reloaded = RaftCore::open(1, 0, store).unwrap();
    assert_eq!(reloaded.current_term(), 2);
    assert_eq!(reloaded.voted_for(), Some(7));
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn restart_preserves_candidate_vote_and_rejects_second_candidate_same_term() {
    let dir = temp_dir("neo4r-raft-restart-vote-guard");
    let store = RaftPersistentStateStore::open(dir.join("state.txt"));
    {
        let mut raft = RaftCore::open(1, 0, store.clone()).unwrap();
        let request = raft.start_election().unwrap();
        assert_eq!(request.term, 1);
        assert_eq!(raft.voted_for(), Some(1));
    }

    let mut reloaded = RaftCore::open(1, 0, store).unwrap();
    assert_eq!(reloaded.current_term(), 1);
    assert_eq!(reloaded.voted_for(), Some(1));
    let response = reloaded
        .request_vote(RequestVoteRequest {
            term: 1,
            candidate_id: 2,
            last_log_index: 0,
            last_log_term: 0,
        })
        .unwrap();

    assert!(!response.vote_granted);
    assert_eq!(reloaded.voted_for(), Some(1));
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn request_vote_rejects_stale_candidate_log() {
    let dir = temp_dir("neo4r-raft-stale-vote");
    let store = RaftPersistentStateStore::open(dir.join("state.txt"));
    let mut raft = RaftCore::open(1, 0, store).unwrap();
    raft.append_entries(AppendEntriesRequest {
        term: 3,
        leader_id: 9,
        prev_log_index: 0,
        prev_log_term: 0,
        entries: vec![entry(0, 3, 1)],
        leader_commit: 0,
    })
    .unwrap();

    let response = raft
        .request_vote(RequestVoteRequest {
            term: 4,
            candidate_id: 7,
            last_log_index: 0,
            last_log_term: 0,
        })
        .unwrap();
    assert!(!response.vote_granted);
    assert_eq!(raft.voted_for(), None);
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn append_entries_repairs_divergent_suffix() {
    let dir = temp_dir("neo4r-raft-repair");
    let store = RaftPersistentStateStore::open(dir.join("state.txt"));
    let mut raft = RaftCore::open(1, 0, store).unwrap();
    raft.append_entries(AppendEntriesRequest {
        term: 2,
        leader_id: 9,
        prev_log_index: 0,
        prev_log_term: 0,
        entries: vec![entry(0, 2, 1), entry(0, 2, 2)],
        leader_commit: 1,
    })
    .unwrap();

    let response = raft
        .append_entries(AppendEntriesRequest {
            term: 3,
            leader_id: 8,
            prev_log_index: 1,
            prev_log_term: 2,
            entries: vec![entry(0, 3, 2), entry(0, 3, 3)],
            leader_commit: 3,
        })
        .unwrap();

    assert!(response.success);
    assert_eq!(
        raft.log()
            .iter()
            .map(|entry| entry.term)
            .collect::<Vec<_>>(),
        vec![2, 3, 3]
    );
    assert_eq!(raft.commit_index(), 3);
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn append_entries_reports_conflict_term_and_first_index() {
    let dir = temp_dir("neo4r-raft-conflict-hint");
    let store = RaftPersistentStateStore::open(dir.join("state.txt"));
    let mut raft = RaftCore::open(1, 0, store).unwrap();
    raft.append_entries(AppendEntriesRequest {
        term: 2,
        leader_id: 9,
        prev_log_index: 0,
        prev_log_term: 0,
        entries: vec![entry(0, 2, 1), entry(0, 2, 2), entry(0, 3, 3)],
        leader_commit: 1,
    })
    .unwrap();

    let response = raft
        .append_entries(AppendEntriesRequest {
            term: 4,
            leader_id: 8,
            prev_log_index: 2,
            prev_log_term: 9,
            entries: Vec::new(),
            leader_commit: 1,
        })
        .unwrap();

    assert!(!response.success);
    assert_eq!(response.conflict_term, Some(2));
    assert_eq!(response.conflict_index, Some(1));
    assert_eq!(raft.last_log_index(), 1);
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn leader_advances_commit_after_majority_match_and_serves_read_index() {
    let dir = temp_dir("neo4r-raft-leader-commit");
    let store = RaftPersistentStateStore::open(dir.join("state.txt"));
    let membership = RaftMembership::new([1, 2, 3]).unwrap();
    let mut raft = RaftCore::open_with_membership(1, 0, store, membership).unwrap();
    raft.start_election().unwrap();
    raft.become_leader();

    let first = raft.append_local_entry(command(1)).unwrap();
    assert_eq!(first.index, 1);
    assert_eq!(raft.commit_index(), 0);
    assert!(raft.read_index().is_ok());

    assert_eq!(raft.record_replication_match(2, 1).unwrap(), 1);
    assert_eq!(raft.read_index().unwrap(), 1);
    assert_eq!(raft.leader_lease_read_index().unwrap(), 1);
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn expired_leader_lease_falls_back_to_quorum_read_index() {
    let dir = temp_dir("neo4r-raft-expired-lease-read-index");
    let store = RaftPersistentStateStore::open(dir.join("state.txt"));
    let membership = RaftMembership::new([1, 2, 3]).unwrap();
    let mut raft = RaftCore::open_with_membership(1, 0, store, membership)
        .unwrap()
        .with_lease_duration(Duration::from_millis(1));
    raft.start_election().unwrap();
    raft.become_leader();
    let first = raft.append_local_entry(command(1)).unwrap();
    assert_eq!(raft.record_replication_match(2, first.index).unwrap(), 1);

    std::thread::sleep(Duration::from_millis(5));

    assert_eq!(raft.leader_lease_read_index().unwrap(), 1);
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn leader_lease_rejects_duration_inside_clock_and_message_bound() {
    let dir = temp_dir("neo4r-raft-lease-clock-bound");
    let store = RaftPersistentStateStore::open(dir.join("state.txt"));
    let membership = RaftMembership::new([1, 2, 3]).unwrap();
    let mut raft = RaftCore::open_with_membership(1, 0, store, membership)
        .unwrap()
        .with_lease_duration(Duration::from_millis(100))
        .with_lease_clock_bounds(Duration::from_millis(70), Duration::from_millis(40));
    raft.start_election().unwrap();
    raft.become_leader();
    let first = raft.append_local_entry(command(1)).unwrap();
    assert_eq!(raft.record_replication_match(2, first.index).unwrap(), 1);

    let err = raft.leader_lease_read_index().unwrap_err();
    assert!(err.to_string().contains("clock/message bound"));

    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn candidate_becomes_leader_after_vote_quorum() {
    let dir = temp_dir("neo4r-raft-vote-quorum");
    let store = RaftPersistentStateStore::open(dir.join("state.txt"));
    let membership = RaftMembership::new([1, 2, 3]).unwrap();
    let mut raft = RaftCore::open_with_membership(1, 0, store, membership).unwrap();
    let request = raft.start_election().unwrap();

    assert_eq!(request.term, 1);
    assert_eq!(raft.role(), &RaftRole::Candidate);
    assert!(raft
        .record_vote_response(
            2,
            RequestVoteResponse {
                term: 1,
                vote_granted: true,
            },
        )
        .unwrap());
    assert_eq!(raft.role(), &RaftRole::Leader);
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn pre_vote_checks_log_without_persisting_vote_or_term() {
    let dir = temp_dir("neo4r-raft-prevote");
    let store = RaftPersistentStateStore::open(dir.join("state.txt"));
    let membership = RaftMembership::new([1, 2, 3]).unwrap();
    let mut raft = RaftCore::open_with_membership(1, 0, store.clone(), membership).unwrap();
    raft.append_entries(AppendEntriesRequest {
        term: 2,
        leader_id: 2,
        prev_log_index: 0,
        prev_log_term: 0,
        entries: vec![entry(0, 2, 1)],
        leader_commit: 1,
    })
    .unwrap();

    let stale = raft.pre_vote(PreVoteRequest {
        next_term: 3,
        candidate_id: 3,
        last_log_index: 0,
        last_log_term: 0,
    });
    assert!(!stale.vote_granted);
    let fresh = raft.pre_vote(PreVoteRequest {
        next_term: 3,
        candidate_id: 3,
        last_log_index: 1,
        last_log_term: 2,
    });
    assert!(fresh.vote_granted);
    assert_eq!(store.load().unwrap().voted_for, None);
    assert_eq!(store.load().unwrap().current_term, 2);
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn leader_transfer_requires_voter_to_be_caught_up() {
    let dir = temp_dir("neo4r-raft-leader-transfer");
    let store = RaftPersistentStateStore::open(dir.join("state.txt"));
    let membership = RaftMembership::new([1, 2, 3]).unwrap();
    let mut raft = RaftCore::open_with_membership(1, 0, store, membership).unwrap();
    raft.start_election().unwrap();
    raft.become_leader();
    let first = raft.append_local_entry(command(1)).unwrap();
    assert_eq!(raft.record_replication_match(2, first.index).unwrap(), 1);

    let request = raft.leader_transfer_request(2).unwrap();
    assert_eq!(request.candidate_id, 2);
    assert_eq!(request.term, 2);
    let err = raft.leader_transfer_request(3).unwrap_err();
    assert!(err.to_string().contains("behind commit index"));
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn install_snapshot_request_chunks_payload_with_offsets() {
    let request = InstallSnapshotRequest {
        term: 4,
        leader_id: 1,
        metadata: RaftSnapshotMetadata {
            shard_id: 0,
            last_included_term: 4,
            last_included_index: 9,
        },
        payload: b"abcdef".to_vec(),
    };

    let chunks = request.chunks(2);

    assert_eq!(chunks.len(), 3);
    assert_eq!(chunks[0].offset, 0);
    assert_eq!(chunks[1].offset, 2);
    assert_eq!(chunks[2].offset, 4);
    assert!(!chunks[0].done);
    assert!(chunks[2].done);
    assert_eq!(chunks[2].request.payload, b"ef".to_vec());
}

#[test]
fn snapshot_chunk_assembler_rebuilds_payload_and_rejects_gaps() {
    let request = InstallSnapshotRequest {
        term: 4,
        leader_id: 1,
        metadata: RaftSnapshotMetadata {
            shard_id: 0,
            last_included_term: 4,
            last_included_index: 9,
        },
        payload: b"abcdef".to_vec(),
    };
    let chunks = request.chunks(2);
    let mut assembler = SnapshotChunkAssembler::new(chunks[0].clone()).unwrap();

    let gap = assembler.push(chunks[2].clone()).unwrap_err();
    assert!(gap.to_string().contains("offset gap"));
    assert_eq!(assembler.push(chunks[1].clone()).unwrap(), None);
    assert_eq!(assembler.push(chunks[2].clone()).unwrap().unwrap(), request);
}

#[test]
fn membership_change_updates_voter_set() {
    let dir = temp_dir("neo4r-raft-membership");
    let store = RaftPersistentStateStore::open(dir.join("state.txt"));
    let membership = RaftMembership::new([1, 2]).unwrap();
    let mut raft = RaftCore::open_with_membership(1, 0, store, membership).unwrap();

    raft.apply_committed_membership_change(RaftMembershipChange::AddVoter(3))
        .unwrap();
    assert!(raft.membership().contains(3));
    assert_eq!(raft.membership().quorum_size(), 2);

    raft.apply_committed_membership_change(RaftMembershipChange::RemoveVoter(2))
        .unwrap();
    assert!(!raft.membership().contains(2));
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn membership_guards_duplicate_joint_and_last_voter_removal() {
    let dir = temp_dir("neo4r-raft-membership-guards");
    let store = RaftPersistentStateStore::open(dir.join("state.txt"));
    let membership = RaftMembership::new([1]).unwrap();
    let mut raft = RaftCore::open_with_membership(1, 0, store, membership).unwrap();

    let err = raft
        .apply_committed_membership_change(RaftMembershipChange::RemoveVoter(1))
        .unwrap_err();
    assert!(err.to_string().contains("cannot remove the last voter"));

    raft.begin_joint_consensus([1, 2]).unwrap();
    let err = raft.begin_joint_consensus([1, 3]).unwrap_err();
    assert!(err.to_string().contains("already in joint consensus"));

    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn joint_consensus_requires_old_and_new_quorums() {
    let dir = temp_dir("neo4r-raft-joint-consensus");
    let store = RaftPersistentStateStore::open(dir.join("state.txt"));
    let membership = RaftMembership::new([1, 2, 3]).unwrap();
    let mut raft = RaftCore::open_with_membership(1, 0, store, membership).unwrap();
    raft.start_election().unwrap();
    raft.become_leader();
    raft.begin_joint_consensus([1, 4, 5]).unwrap();
    assert!(raft.membership().is_joint());

    raft.append_local_entry(command(1)).unwrap();
    assert_eq!(raft.record_replication_match(2, 1).unwrap(), 0);
    assert_eq!(raft.record_replication_match(4, 1).unwrap(), 1);
    raft.finalize_joint_consensus();
    assert!(!raft.membership().is_joint());

    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn joint_consensus_election_requires_old_and_new_vote_quorums() {
    let dir = temp_dir("neo4r-raft-joint-election-quorum");
    let store = RaftPersistentStateStore::open(dir.join("state.txt"));
    let membership = RaftMembership::new([1, 2, 3]).unwrap();
    let mut raft = RaftCore::open_with_membership(1, 0, store, membership).unwrap();
    raft.begin_joint_consensus([1, 4, 5]).unwrap();
    let request = raft.start_election().unwrap();

    assert_eq!(request.term, 1);
    assert!(!raft
        .record_vote_response(
            4,
            RequestVoteResponse {
                term: 1,
                vote_granted: true,
            },
        )
        .unwrap());
    assert_eq!(raft.role(), &RaftRole::Candidate);
    assert!(raft
        .record_vote_response(
            2,
            RequestVoteResponse {
                term: 1,
                vote_granted: true,
            },
        )
        .unwrap());
    assert_eq!(raft.role(), &RaftRole::Leader);

    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn removing_local_voter_steps_down_and_blocks_local_appends() {
    let dir = temp_dir("neo4r-raft-remove-local-voter");
    let store = RaftPersistentStateStore::open(dir.join("state.txt"));
    let membership = RaftMembership::new([1, 2]).unwrap();
    let mut raft = RaftCore::open_with_membership(1, 0, store, membership).unwrap();
    raft.start_election().unwrap();
    raft.become_leader();

    raft.apply_committed_membership_change(RaftMembershipChange::RemoveVoter(1))
        .unwrap();

    assert_eq!(raft.role(), &RaftRole::Follower);
    assert_eq!(raft.leader_id(), None);
    assert!(matches!(
        raft.append_local_entry(command(1)),
        Err(DatabaseError::Replication(message)) if message.contains("not leader")
    ));

    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn install_snapshot_compacts_log_and_accepts_append_from_snapshot_boundary() {
    let dir = temp_dir("neo4r-raft-install-snapshot");
    let store = RaftPersistentStateStore::open(dir.join("state.txt"));
    let membership = RaftMembership::new([1, 2]).unwrap();
    let mut raft = RaftCore::open_with_membership(2, 0, store, membership).unwrap();
    raft.append_entries(AppendEntriesRequest {
        term: 3,
        leader_id: 1,
        prev_log_index: 0,
        prev_log_term: 0,
        entries: vec![entry(0, 3, 1), entry(0, 3, 2), entry(0, 3, 3)],
        leader_commit: 3,
    })
    .unwrap();

    let response = raft
        .install_snapshot(InstallSnapshotRequest {
            term: 4,
            leader_id: 1,
            metadata: RaftSnapshotMetadata {
                shard_id: 0,
                last_included_term: 3,
                last_included_index: 3,
            },
            payload: Vec::new(),
        })
        .unwrap();

    assert!(response.success);
    assert_eq!(raft.commit_index(), 3);
    assert_eq!(raft.last_log_index(), 3);
    assert!(raft.log().is_empty());

    let response = raft
        .append_entries(AppendEntriesRequest {
            term: 4,
            leader_id: 1,
            prev_log_index: 3,
            prev_log_term: 3,
            entries: vec![entry(0, 4, 4)],
            leader_commit: 4,
        })
        .unwrap();

    assert!(response.success);
    assert_eq!(raft.last_log_index(), 4);
    assert_eq!(raft.commit_index(), 4);
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn three_node_harness_elects_replicates_and_installs_snapshot() {
    let dir = temp_dir("neo4r-raft-three-node-harness");
    let membership = RaftMembership::new([1, 2, 3]).unwrap();
    let mut node1 = RaftCore::open_with_membership(
        1,
        0,
        RaftPersistentStateStore::open(dir.join("node1.txt")),
        membership.clone(),
    )
    .unwrap();
    let mut node2 = RaftCore::open_with_membership(
        2,
        0,
        RaftPersistentStateStore::open(dir.join("node2.txt")),
        membership.clone(),
    )
    .unwrap();
    let mut node3 = RaftCore::open_with_membership(
        3,
        0,
        RaftPersistentStateStore::open(dir.join("node3.txt")),
        membership,
    )
    .unwrap();

    let vote_request = node1.start_election().unwrap();
    let vote2 = node2.request_vote(vote_request.clone()).unwrap();
    let vote3 = node3.request_vote(vote_request).unwrap();
    assert!(node1.record_vote_response(2, vote2).unwrap());
    assert_eq!(node1.role(), &RaftRole::Leader);
    assert!(!node1.record_vote_response(3, vote3).unwrap());

    let entry = node1.append_local_entry(command(7)).unwrap();
    let append = AppendEntriesRequest {
        term: node1.current_term(),
        leader_id: 1,
        prev_log_index: 0,
        prev_log_term: 0,
        entries: vec![entry.clone()],
        leader_commit: 0,
    };
    assert!(node2.append_entries(append.clone()).unwrap().success);
    assert!(node3.append_entries(append).unwrap().success);
    assert_eq!(node1.record_replication_match(2, entry.index).unwrap(), 1);
    assert_eq!(node1.commit_index(), 1);

    let snapshot = InstallSnapshotRequest {
        term: node1.current_term(),
        leader_id: 1,
        metadata: RaftSnapshotMetadata {
            shard_id: 0,
            last_included_term: entry.term,
            last_included_index: entry.index,
        },
        payload: Vec::new(),
    };
    let response = node3.install_snapshot(snapshot).unwrap();
    assert!(response.success);
    assert_eq!(node3.commit_index(), 1);
    assert!(node3.log().is_empty());

    let _ = std::fs::remove_dir_all(dir);
}

fn entry(shard_id: ShardId, term: Term, index: LogIndex) -> LogEntry {
    LogEntry::new(shard_id, term, index, command(index))
}

fn command(id: u64) -> Command {
    Command::CreateNode {
        id,
        labels: Vec::new(),
        properties: Properties::new(),
    }
}

fn temp_dir(prefix: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "{}-{}",
        prefix,
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}
