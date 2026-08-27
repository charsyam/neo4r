use crate::{StorageError, StorageResult};
use neo4r_core::{LogIndex, ServerId, ShardId};
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, ErrorKind, Write};
use std::path::{Path, PathBuf};

const MAGIC: &str = "N4RMEM1";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NodeMembershipState {
    Negotiating,
    Joining,
    Active,
    Draining,
    Leaving,
    Removed,
    Dead,
    Rejected,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterNode {
    pub server_id: ServerId,
    pub address: String,
    pub state: NodeMembershipState,
    pub protocol_version: u64,
    pub storage_version: u64,
    pub shard_count: u64,
    pub rejection_reason: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ShardAssignmentState {
    Planned,
    CatchingUp,
    CaughtUp,
    ServingReplica,
    Promoting,
    ServingPrimary,
    Removing,
    Removed,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterShardAssignment {
    pub shard_id: ShardId,
    pub server_id: ServerId,
    pub state: ShardAssignmentState,
    pub match_index: LogIndex,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ClusterMembership {
    pub version: u64,
    pub nodes: Vec<ClusterNode>,
    pub shard_assignments: Vec<ClusterShardAssignment>,
}

#[derive(Clone, Debug)]
pub struct ClusterMembershipStore {
    path: PathBuf,
}

impl ClusterMembershipStore {
    pub fn open(data_dir: impl AsRef<Path>) -> StorageResult<Self> {
        let cluster_dir = data_dir.as_ref().join("cluster");
        fs::create_dir_all(&cluster_dir)?;
        Ok(Self {
            path: cluster_dir.join("membership.txt"),
        })
    }

    pub fn save(&self, membership: &ClusterMembership) -> StorageResult<()> {
        let tmp_path = self.path.with_extension("txt.tmp");
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp_path)?;
        writeln!(file, "{MAGIC}\t{}", membership.version)?;
        for node in &membership.nodes {
            if node.address.contains(['\t', '\n', '\r'])
                || node.rejection_reason.contains(['\t', '\n', '\r'])
            {
                return Err(StorageError::CorruptStore(
                    "cluster node contains invalid characters".to_string(),
                ));
            }
            writeln!(
                file,
                "node\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
                node.server_id,
                encode_state(node.state),
                node.protocol_version,
                node.storage_version,
                node.shard_count,
                node.address,
                node.rejection_reason
            )?;
        }
        for assignment in &membership.shard_assignments {
            writeln!(
                file,
                "assignment\t{}\t{}\t{}\t{}",
                assignment.shard_id,
                assignment.server_id,
                encode_assignment_state(assignment.state),
                assignment.match_index
            )?;
        }
        file.sync_all()?;
        drop(file);
        fs::rename(&tmp_path, &self.path)?;
        if let Some(parent) = self.path.parent() {
            File::open(parent)?.sync_all()?;
        }
        Ok(())
    }

    pub fn load(&self) -> StorageResult<Option<ClusterMembership>> {
        let file = match File::open(&self.path) {
            Ok(file) => file,
            Err(err) if err.kind() == ErrorKind::NotFound => return Ok(None),
            Err(err) => return Err(StorageError::Io(err)),
        };
        let mut lines = BufReader::new(file).lines();
        let header = lines.next().transpose()?.ok_or_else(|| {
            StorageError::CorruptStore("missing cluster membership header".to_string())
        })?;
        let (magic, version) = header.split_once('\t').ok_or_else(|| {
            StorageError::CorruptStore("invalid cluster membership header".to_string())
        })?;
        if magic != MAGIC {
            return Err(StorageError::CorruptStore(
                "invalid cluster membership magic".to_string(),
            ));
        }
        let version = version.parse::<u64>().map_err(|_| {
            StorageError::CorruptStore("invalid cluster membership version".to_string())
        })?;
        let mut nodes = Vec::new();
        let mut shard_assignments = Vec::new();
        for line in lines {
            let line = line?;
            if line.is_empty() {
                continue;
            }
            let parts = line.split('\t').collect::<Vec<_>>();
            match parts.first().copied() {
                Some("node") => {
                    if parts.len() != 8 {
                        return Err(StorageError::CorruptStore(
                            "invalid cluster node record".to_string(),
                        ));
                    }
                    nodes.push(ClusterNode {
                        server_id: parse_u64(parts[1], "cluster node server id")?,
                        state: decode_state(parts[2])?,
                        protocol_version: parse_u64(parts[3], "cluster node protocol version")?,
                        storage_version: parse_u64(parts[4], "cluster node storage version")?,
                        shard_count: parse_u64(parts[5], "cluster node shard count")?,
                        address: parts[6].to_string(),
                        rejection_reason: parts[7].to_string(),
                    });
                }
                Some("assignment") => {
                    if parts.len() != 5 {
                        return Err(StorageError::CorruptStore(
                            "invalid cluster shard assignment record".to_string(),
                        ));
                    }
                    shard_assignments.push(ClusterShardAssignment {
                        shard_id: parse_u64(parts[1], "cluster assignment shard id")?,
                        server_id: parse_u64(parts[2], "cluster assignment server id")?,
                        state: decode_assignment_state(parts[3])?,
                        match_index: parse_u64(parts[4], "cluster assignment match index")?,
                    });
                }
                Some(_) if parts.len() == 3 => {
                    nodes.push(ClusterNode {
                        server_id: parse_u64(parts[0], "cluster node server id")?,
                        state: decode_state(parts[1])?,
                        address: parts[2].to_string(),
                        protocol_version: 0,
                        storage_version: 0,
                        shard_count: 0,
                        rejection_reason: String::new(),
                    });
                }
                _ => {
                    return Err(StorageError::CorruptStore(
                        "invalid cluster membership record".to_string(),
                    ));
                }
            }
        }
        Ok(Some(ClusterMembership {
            version,
            nodes,
            shard_assignments,
        }))
    }
}

fn encode_state(state: NodeMembershipState) -> &'static str {
    match state {
        NodeMembershipState::Negotiating => "negotiating",
        NodeMembershipState::Joining => "joining",
        NodeMembershipState::Active => "active",
        NodeMembershipState::Draining => "draining",
        NodeMembershipState::Leaving => "leaving",
        NodeMembershipState::Removed => "removed",
        NodeMembershipState::Dead => "dead",
        NodeMembershipState::Rejected => "rejected",
    }
}

fn decode_state(value: &str) -> StorageResult<NodeMembershipState> {
    match value {
        "negotiating" => Ok(NodeMembershipState::Negotiating),
        "joining" => Ok(NodeMembershipState::Joining),
        "active" => Ok(NodeMembershipState::Active),
        "draining" => Ok(NodeMembershipState::Draining),
        "leaving" => Ok(NodeMembershipState::Leaving),
        "removed" => Ok(NodeMembershipState::Removed),
        "dead" => Ok(NodeMembershipState::Dead),
        "rejected" => Ok(NodeMembershipState::Rejected),
        _ => Err(StorageError::CorruptStore(format!(
            "unknown cluster node state {value:?}"
        ))),
    }
}

fn encode_assignment_state(state: ShardAssignmentState) -> &'static str {
    match state {
        ShardAssignmentState::Planned => "planned",
        ShardAssignmentState::CatchingUp => "catching_up",
        ShardAssignmentState::CaughtUp => "caught_up",
        ShardAssignmentState::ServingReplica => "serving_replica",
        ShardAssignmentState::Promoting => "promoting",
        ShardAssignmentState::ServingPrimary => "serving_primary",
        ShardAssignmentState::Removing => "removing",
        ShardAssignmentState::Removed => "removed",
    }
}

fn decode_assignment_state(value: &str) -> StorageResult<ShardAssignmentState> {
    match value {
        "planned" => Ok(ShardAssignmentState::Planned),
        "catching_up" => Ok(ShardAssignmentState::CatchingUp),
        "caught_up" => Ok(ShardAssignmentState::CaughtUp),
        "serving_replica" => Ok(ShardAssignmentState::ServingReplica),
        "promoting" => Ok(ShardAssignmentState::Promoting),
        "serving_primary" => Ok(ShardAssignmentState::ServingPrimary),
        "removing" => Ok(ShardAssignmentState::Removing),
        "removed" => Ok(ShardAssignmentState::Removed),
        _ => Err(StorageError::CorruptStore(format!(
            "unknown cluster shard assignment state {value:?}"
        ))),
    }
}

fn parse_u64(input: &str, name: &str) -> StorageResult<u64> {
    input
        .parse::<u64>()
        .map_err(|_| StorageError::CorruptStore(format!("invalid {name}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn saves_and_loads_cluster_membership() {
        let dir = temp_dir("neo4r-membership");
        let store = ClusterMembershipStore::open(&dir).unwrap();
        let membership = ClusterMembership {
            version: 3,
            nodes: vec![
                ClusterNode {
                    server_id: 1,
                    address: "127.0.0.1:7687".to_string(),
                    state: NodeMembershipState::Active,
                    protocol_version: 1,
                    storage_version: 1,
                    shard_count: 4,
                    rejection_reason: String::new(),
                },
                ClusterNode {
                    server_id: 2,
                    address: "127.0.0.1:7688".to_string(),
                    state: NodeMembershipState::Joining,
                    protocol_version: 1,
                    storage_version: 1,
                    shard_count: 4,
                    rejection_reason: String::new(),
                },
            ],
            shard_assignments: vec![ClusterShardAssignment {
                shard_id: 1,
                server_id: 2,
                state: ShardAssignmentState::CaughtUp,
                match_index: 42,
            }],
        };

        store.save(&membership).unwrap();

        assert_eq!(store.load().unwrap(), Some(membership));
        let _ = fs::remove_dir_all(dir);
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
}
