use crate::model::{NodeId, RelationshipId};
use std::fmt;

pub type Result<T> = std::result::Result<T, GraphError>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GraphError {
    NodeAlreadyExists(NodeId),
    NodeNotFound(NodeId),
    RelationshipAlreadyExists(RelationshipId),
    RelationshipNotFound(RelationshipId),
    RelationshipEndpointNotFound {
        relationship_id: RelationshipId,
        node_id: NodeId,
    },
}

impl fmt::Display for GraphError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NodeAlreadyExists(id) => write!(f, "node already exists: {id}"),
            Self::NodeNotFound(id) => write!(f, "node not found: {id}"),
            Self::RelationshipAlreadyExists(id) => {
                write!(f, "relationship already exists: {id}")
            }
            Self::RelationshipNotFound(id) => write!(f, "relationship not found: {id}"),
            Self::RelationshipEndpointNotFound {
                relationship_id,
                node_id,
            } => write!(
                f,
                "relationship {relationship_id} refers to missing node {node_id}"
            ),
        }
    }
}

impl std::error::Error for GraphError {}
