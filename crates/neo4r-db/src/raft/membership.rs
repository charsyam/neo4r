use crate::{DatabaseError, DatabaseResult};
use neo4r_core::ServerId;
use std::collections::BTreeSet;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RaftMembership {
    voters: BTreeSet<ServerId>,
    outgoing_voters: Option<BTreeSet<ServerId>>,
    learners: BTreeSet<ServerId>,
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
            learners: BTreeSet::new(),
        })
    }

    pub fn voters(&self) -> &BTreeSet<ServerId> {
        &self.voters
    }

    pub fn outgoing_voters(&self) -> Option<&BTreeSet<ServerId>> {
        self.outgoing_voters.as_ref()
    }

    pub fn learners(&self) -> &BTreeSet<ServerId> {
        &self.learners
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

    pub fn is_member(&self, server_id: ServerId) -> bool {
        self.contains(server_id) || self.learners.contains(&server_id)
    }

    pub(super) fn has_quorum(&self, matched: &BTreeSet<ServerId>) -> bool {
        has_majority(&self.voters, matched)
            && self
                .outgoing_voters
                .as_ref()
                .is_none_or(|voters| has_majority(voters, matched))
    }

    pub(super) fn all_voters(&self) -> BTreeSet<ServerId> {
        let mut voters = self.voters.clone();
        if let Some(outgoing) = &self.outgoing_voters {
            voters.extend(outgoing.iter().copied());
        }
        voters
    }

    pub(super) fn enter_joint(&mut self, next_voters: BTreeSet<ServerId>) -> DatabaseResult<()> {
        if next_voters.is_empty() {
            return Err(DatabaseError::InvalidConfig(
                "raft joint membership cannot be empty".to_string(),
            ));
        }
        self.outgoing_voters = Some(self.voters.clone());
        self.voters = next_voters;
        Ok(())
    }

    pub(super) fn finalize_joint(&mut self) {
        self.outgoing_voters = None;
    }

    pub(super) fn add_voter(&mut self, server_id: ServerId) {
        self.learners.remove(&server_id);
        self.voters.insert(server_id);
    }

    pub(super) fn add_learner(&mut self, server_id: ServerId) -> DatabaseResult<()> {
        if self.contains(server_id) {
            return Err(DatabaseError::InvalidConfig(format!(
                "server {server_id} is already a raft voter"
            )));
        }
        self.learners.insert(server_id);
        Ok(())
    }

    pub(super) fn promote_learner(&mut self, server_id: ServerId) -> DatabaseResult<()> {
        if !self.learners.remove(&server_id) {
            return Err(DatabaseError::InvalidConfig(format!(
                "server {server_id} is not a raft learner"
            )));
        }
        self.voters.insert(server_id);
        Ok(())
    }

    pub(super) fn remove_voter(&mut self, server_id: ServerId) -> DatabaseResult<()> {
        if self.voters.len() == 1 && self.voters.contains(&server_id) {
            return Err(DatabaseError::InvalidConfig(
                "raft membership cannot remove the last voter".to_string(),
            ));
        }
        self.voters.remove(&server_id);
        self.learners.remove(&server_id);
        Ok(())
    }
}

fn has_majority(voters: &BTreeSet<ServerId>, matched: &BTreeSet<ServerId>) -> bool {
    matched.intersection(voters).count() >= voters.len() / 2 + 1
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RaftMembershipChange {
    AddLearner(ServerId),
    PromoteLearner(ServerId),
    AddVoter(ServerId),
    RemoveVoter(ServerId),
}
