use crate::clock::HybridTimestamp;
use crate::command::Command;
use crate::model::{NodeId, Properties, RelationshipId};

pub type ShardId = u64;
pub type LogIndex = u64;
pub type Term = u64;
pub type ServerId = u64;
pub type ConfigVersion = u64;

#[derive(Clone, Debug, PartialEq)]
pub struct LogEntry {
    pub shard_id: ShardId,
    pub term: Term,
    pub index: LogIndex,
    pub origin_server_id: ServerId,
    pub config_version: ConfigVersion,
    pub timestamp: HybridTimestamp,
    pub command: Command,
}

impl LogEntry {
    pub fn new(shard_id: ShardId, term: Term, index: LogIndex, command: Command) -> Self {
        Self::new_with_timestamp(shard_id, term, index, HybridTimestamp::zero(), command)
    }

    pub fn new_with_timestamp(
        shard_id: ShardId,
        term: Term,
        index: LogIndex,
        timestamp: HybridTimestamp,
        command: Command,
    ) -> Self {
        Self::new_with_metadata(shard_id, term, index, 0, 0, timestamp, command)
    }

    pub fn new_with_metadata(
        shard_id: ShardId,
        term: Term,
        index: LogIndex,
        origin_server_id: ServerId,
        config_version: ConfigVersion,
        timestamp: HybridTimestamp,
        command: Command,
    ) -> Self {
        Self {
            shard_id,
            term,
            index,
            origin_server_id,
            config_version,
            timestamp,
            command,
        }
    }
}

pub trait ShardPolicy {
    fn owner_of_node(&self, node_id: NodeId) -> ShardId;
    fn owner_of_relationship(&self, from: NodeId, to: NodeId, rel_type: &str) -> ShardId;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ShardMap {
    shard_count: u64,
}

impl ShardMap {
    pub fn new(shard_count: u64) -> Option<Self> {
        if shard_count == 0 {
            None
        } else {
            Some(Self { shard_count })
        }
    }

    pub fn shard_count(&self) -> u64 {
        self.shard_count
    }
}

impl ShardPolicy for ShardMap {
    fn owner_of_node(&self, node_id: NodeId) -> ShardId {
        node_id % self.shard_count
    }

    fn owner_of_relationship(&self, from: NodeId, _to: NodeId, _rel_type: &str) -> ShardId {
        self.owner_of_node(from)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ShardRole {
    Primary,
    Replica,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ShardReplica {
    pub server_id: ServerId,
    pub role: ShardRole,
}

impl ShardReplica {
    pub fn primary(server_id: ServerId) -> Self {
        Self {
            server_id,
            role: ShardRole::Primary,
        }
    }

    pub fn replica(server_id: ServerId) -> Self {
        Self {
            server_id,
            role: ShardRole::Replica,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ShardPlacement {
    pub shard_id: ShardId,
    pub replicas: Vec<ShardReplica>,
}

impl ShardPlacement {
    pub fn new(shard_id: ShardId, replicas: Vec<ShardReplica>) -> Self {
        Self { shard_id, replicas }
    }

    pub fn primary_server_id(&self) -> Option<ServerId> {
        self.replicas
            .iter()
            .find(|replica| replica.role == ShardRole::Primary)
            .map(|replica| replica.server_id)
    }

    pub fn has_server(&self, server_id: ServerId) -> bool {
        self.replicas
            .iter()
            .any(|replica| replica.server_id == server_id)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ShardRoutingTable {
    pub version: ConfigVersion,
    pub placements: Vec<ShardPlacement>,
}

impl ShardRoutingTable {
    pub fn single_server(shard_count: u64, server_id: ServerId) -> Self {
        Self {
            version: 1,
            placements: (0..shard_count)
                .map(|shard_id| {
                    ShardPlacement::new(shard_id, vec![ShardReplica::primary(server_id)])
                })
                .collect(),
        }
    }

    pub fn placement(&self, shard_id: ShardId) -> Option<&ShardPlacement> {
        self.placements
            .iter()
            .find(|placement| placement.shard_id == shard_id)
    }

    pub fn primary_server_id(&self, shard_id: ShardId) -> Option<ServerId> {
        self.placement(shard_id)
            .and_then(ShardPlacement::primary_server_id)
    }

    pub fn has_local_copy(&self, shard_id: ShardId, server_id: ServerId) -> bool {
        self.placement(shard_id)
            .map(|placement| placement.has_server(server_id))
            .unwrap_or(false)
    }

    pub fn diff(&self, next: &ShardRoutingTable) -> ShardRoutingDiff {
        let mut primary_moves = Vec::new();
        let mut added_replicas = Vec::new();
        let mut removed_replicas = Vec::new();
        for next_placement in &next.placements {
            let Some(current) = self.placement(next_placement.shard_id) else {
                for replica in &next_placement.replicas {
                    added_replicas.push((next_placement.shard_id, replica.server_id));
                }
                continue;
            };
            let current_primary = current.primary_server_id();
            let next_primary = next_placement.primary_server_id();
            if current_primary != next_primary {
                primary_moves.push(ShardPrimaryMove {
                    shard_id: next_placement.shard_id,
                    from: current_primary,
                    to: next_primary,
                });
            }
            for replica in &next_placement.replicas {
                if !current.has_server(replica.server_id) {
                    added_replicas.push((next_placement.shard_id, replica.server_id));
                }
            }
            for replica in &current.replicas {
                if !next_placement.has_server(replica.server_id) {
                    removed_replicas.push((current.shard_id, replica.server_id));
                }
            }
        }
        ShardRoutingDiff {
            from_version: self.version,
            to_version: next.version,
            primary_moves,
            added_replicas,
            removed_replicas,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ShardPrimaryMove {
    pub shard_id: ShardId,
    pub from: Option<ServerId>,
    pub to: Option<ServerId>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ShardRoutingDiff {
    pub from_version: ConfigVersion,
    pub to_version: ConfigVersion,
    pub primary_moves: Vec<ShardPrimaryMove>,
    pub added_replicas: Vec<(ShardId, ServerId)>,
    pub removed_replicas: Vec<(ShardId, ServerId)>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct BoundaryNode {
    pub id: NodeId,
    pub owner_shard: ShardId,
    pub labels: Vec<String>,
    pub properties: Properties,
    pub version: u64,
}

impl BoundaryNode {
    pub fn new(
        id: NodeId,
        owner_shard: ShardId,
        labels: Vec<String>,
        properties: Properties,
        version: u64,
    ) -> Self {
        Self {
            id,
            owner_shard,
            labels,
            properties,
            version,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct BoundaryIncomingRef {
    pub relationship_id: RelationshipId,
    pub relationship_owner_shard: ShardId,
    pub from: NodeId,
    pub to: NodeId,
    pub rel_type: String,
    pub version: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shard_map_rejects_zero_shards() {
        assert_eq!(ShardMap::new(0), None);
    }

    #[test]
    fn node_owner_uses_fixed_modulo_mapping() {
        let map = ShardMap::new(4).unwrap();

        assert_eq!(map.owner_of_node(0), 0);
        assert_eq!(map.owner_of_node(1), 1);
        assert_eq!(map.owner_of_node(4), 0);
        assert_eq!(map.owner_of_node(9), 1);
    }

    #[test]
    fn relationship_owner_is_source_node_owner() {
        let map = ShardMap::new(4).unwrap();

        assert_eq!(map.owner_of_relationship(9, 2, "KNOWS"), 1);
    }

    #[test]
    fn routing_table_tracks_primary_and_replica_placement() {
        let table = ShardRoutingTable {
            version: 7,
            placements: vec![ShardPlacement::new(
                3,
                vec![ShardReplica::primary(10), ShardReplica::replica(11)],
            )],
        };

        assert_eq!(table.primary_server_id(3), Some(10));
        assert!(table.has_local_copy(3, 11));
        assert!(!table.has_local_copy(3, 12));
    }

    #[test]
    fn routing_table_diff_tracks_rebalancing_changes() {
        let current = ShardRoutingTable {
            version: 1,
            placements: vec![ShardPlacement::new(
                0,
                vec![ShardReplica::primary(1), ShardReplica::replica(2)],
            )],
        };
        let next = ShardRoutingTable {
            version: 2,
            placements: vec![ShardPlacement::new(
                0,
                vec![ShardReplica::replica(1), ShardReplica::primary(3)],
            )],
        };

        let diff = current.diff(&next);

        assert_eq!(diff.primary_moves.len(), 1);
        assert_eq!(diff.added_replicas, vec![(0, 3)]);
        assert_eq!(diff.removed_replicas, vec![(0, 2)]);
    }
}
