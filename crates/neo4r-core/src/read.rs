use crate::error::GraphError;
use crate::graph::GraphState;
use crate::model::{Node, NodeId, Relationship, RelationshipId, Value};
use crate::shard::BoundaryNode;
use std::fmt;

pub type GraphReadResult<T> = std::result::Result<T, GraphReadError>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GraphReadError {
    Store(String),
}

impl fmt::Display for GraphReadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Store(message) => write!(f, "{message}"),
        }
    }
}

impl std::error::Error for GraphReadError {}

impl From<GraphError> for GraphReadError {
    fn from(err: GraphError) -> Self {
        Self::Store(err.to_string())
    }
}

pub trait GraphRead {
    fn node(&self, id: NodeId) -> GraphReadResult<Option<Node>>;

    fn boundary_node(&self, id: NodeId) -> GraphReadResult<Option<BoundaryNode>>;

    fn nodes(&self) -> GraphReadResult<Vec<Node>>;

    fn node_ids(&self) -> GraphReadResult<Vec<NodeId>>;

    fn relationship(&self, id: RelationshipId) -> GraphReadResult<Option<Relationship>>;

    fn node_ids_by_label(&self, label: &str) -> GraphReadResult<Vec<NodeId>>;

    fn node_ids_by_label_property(
        &self,
        label: &str,
        property_key: &str,
        property_value: &Value,
    ) -> GraphReadResult<Vec<NodeId>>;

    fn boundary_node_ids_by_label_property(
        &self,
        label: &str,
        property_key: &str,
        property_value: &Value,
    ) -> GraphReadResult<Vec<NodeId>>;

    fn outgoing(&self, node_id: NodeId) -> GraphReadResult<Vec<Relationship>>;

    fn outgoing_by_type(
        &self,
        node_id: NodeId,
        rel_type: &str,
    ) -> GraphReadResult<Vec<Relationship>>;

    fn incoming(&self, node_id: NodeId) -> GraphReadResult<Vec<Relationship>>;

    fn incoming_by_type(
        &self,
        node_id: NodeId,
        rel_type: &str,
    ) -> GraphReadResult<Vec<Relationship>>;
}

impl GraphRead for GraphState {
    fn node(&self, id: NodeId) -> GraphReadResult<Option<Node>> {
        Ok(GraphState::node(self, id).cloned())
    }

    fn boundary_node(&self, id: NodeId) -> GraphReadResult<Option<BoundaryNode>> {
        Ok(GraphState::boundary_node(self, id).cloned())
    }

    fn nodes(&self) -> GraphReadResult<Vec<Node>> {
        Ok(GraphState::nodes(self).cloned().collect())
    }

    fn node_ids(&self) -> GraphReadResult<Vec<NodeId>> {
        Ok(GraphState::node_ids(self))
    }

    fn relationship(&self, id: RelationshipId) -> GraphReadResult<Option<Relationship>> {
        Ok(GraphState::relationship(self, id).cloned())
    }

    fn node_ids_by_label(&self, label: &str) -> GraphReadResult<Vec<NodeId>> {
        Ok(GraphState::node_ids_by_label(self, label))
    }

    fn node_ids_by_label_property(
        &self,
        label: &str,
        property_key: &str,
        property_value: &Value,
    ) -> GraphReadResult<Vec<NodeId>> {
        Ok(GraphState::node_ids_by_label_property(
            self,
            label,
            property_key,
            property_value,
        ))
    }

    fn boundary_node_ids_by_label_property(
        &self,
        label: &str,
        property_key: &str,
        property_value: &Value,
    ) -> GraphReadResult<Vec<NodeId>> {
        Ok(GraphState::boundary_node_ids_by_label_property(
            self,
            label,
            property_key,
            property_value,
        ))
    }

    fn outgoing(&self, node_id: NodeId) -> GraphReadResult<Vec<Relationship>> {
        Ok(GraphState::outgoing(self, node_id)?
            .into_iter()
            .cloned()
            .collect())
    }

    fn outgoing_by_type(
        &self,
        node_id: NodeId,
        rel_type: &str,
    ) -> GraphReadResult<Vec<Relationship>> {
        Ok(GraphState::outgoing_by_type(self, node_id, rel_type)?
            .into_iter()
            .cloned()
            .collect())
    }

    fn incoming(&self, node_id: NodeId) -> GraphReadResult<Vec<Relationship>> {
        Ok(GraphState::incoming(self, node_id)?
            .into_iter()
            .cloned()
            .collect())
    }

    fn incoming_by_type(
        &self,
        node_id: NodeId,
        rel_type: &str,
    ) -> GraphReadResult<Vec<Relationship>> {
        Ok(GraphState::incoming_by_type(self, node_id, rel_type)?
            .into_iter()
            .cloned()
            .collect())
    }
}
