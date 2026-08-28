enum Binding {
    Node(BoundNode),
    Relationship(Relationship),
}

enum BoundNode {
    Local(Node),
    Boundary(BoundaryNode),
}

impl BoundNode {
    fn labels(&self) -> &[String] {
        match self {
            Self::Local(node) => &node.labels,
            Self::Boundary(node) => &node.labels,
        }
    }

    fn properties(&self) -> &neo4r_core::Properties {
        match self {
            Self::Local(node) => &node.properties,
            Self::Boundary(node) => &node.properties,
        }
    }

    fn to_query_value(&self) -> QueryValue {
        match self {
            Self::Local(node) => QueryValue::Node(node.clone()),
            Self::Boundary(node) => QueryValue::BoundaryNode(node.clone()),
        }
    }
}

fn lookup_bound_node<G: GraphRead + ?Sized>(
    graph: &G,
    node_id: u64,
) -> QueryResult<Option<BoundNode>> {
    if let Some(node) = graph.node(node_id).map_err(read_error)? {
        return Ok(Some(BoundNode::Local(node)));
    }
    Ok(graph
        .boundary_node(node_id)
        .map_err(read_error)?
        .map(BoundNode::Boundary))
}

fn bound_node_matches(node: &BoundNode, pattern: &NodePattern) -> bool {
    pattern
        .label
        .as_ref()
        .is_none_or(|label| node.labels().iter().any(|node_label| node_label == label))
}

fn read_error(err: GraphReadError) -> QueryError {
    QueryError::Unsupported(format!("graph read failed: {err}"))
}
