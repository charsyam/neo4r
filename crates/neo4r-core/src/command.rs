use crate::model::{NodeId, Properties, RelationshipId, Value};
use crate::shard::{ServerId, ShardId, ShardRoutingTable};

#[derive(Clone, Debug, PartialEq)]
pub enum Command {
    CreateNode {
        id: NodeId,
        labels: Vec<String>,
        properties: Properties,
    },
    CreateRelationship {
        id: RelationshipId,
        from: NodeId,
        to: NodeId,
        rel_type: String,
        properties: Properties,
    },
    UpsertBoundaryNode {
        id: NodeId,
        owner_shard: ShardId,
        labels: Vec<String>,
        properties: Properties,
        version: u64,
    },
    SetNodeProperty {
        id: NodeId,
        key: String,
        value: Value,
    },
    RemoveNodeProperty {
        id: NodeId,
        key: String,
    },
    AddNodeLabel {
        id: NodeId,
        label: String,
    },
    RemoveNodeLabel {
        id: NodeId,
        label: String,
    },
    SetRelationshipProperty {
        id: RelationshipId,
        key: String,
        value: Value,
    },
    RemoveRelationshipProperty {
        id: RelationshipId,
        key: String,
    },
    DeleteRelationship {
        id: RelationshipId,
    },
    DeleteNode {
        id: NodeId,
    },
    ClusterConfigChange {
        phase: String,
        description: String,
        voters: Vec<ServerId>,
        routing_table: ShardRoutingTable,
    },
}
