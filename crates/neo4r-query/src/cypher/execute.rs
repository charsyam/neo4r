use super::binding::{bound_node_matches, lookup_bound_node, read_error, Binding, BoundNode};
use super::*;

mod rows;

use rows::*;

pub(super) fn execute_query<G: GraphRead + ?Sized>(
    graph: &G,
    query: &Query,
    vector_indexes: Option<&dyn VectorIndexProvider>,
) -> QueryResult<Vec<QueryRow>> {
    let rows = match &query.pattern {
        Pattern::Node(pattern) => {
            let nodes = candidate_nodes(graph, pattern, query.predicate.as_ref())?;
            let nodes = order_nodes_by_vector_knn(
                nodes,
                pattern,
                query.predicate.as_ref(),
                vector_indexes,
            )?;
            nodes
                .into_iter()
                .filter_map(|node| {
                    let mut bindings = HashMap::new();
                    bindings.insert(
                        pattern.variable.as_str(),
                        Binding::Node(BoundNode::Local(node)),
                    );
                    projected_row_if_matches(&bindings, query)
                })
                .collect()
        }
        Pattern::Outgoing {
            from,
            rel_variable,
            rel_type,
            to,
        } => {
            let mut rows = Vec::new();
            for source in candidate_nodes(graph, from, query.predicate.as_ref())? {
                let relationships = match rel_type {
                    Some(rel_type) => graph.outgoing_by_type(source.id, rel_type),
                    None => graph.outgoing(source.id),
                }
                .map_err(read_error)?;

                for relationship in relationships {
                    let Some(target) = lookup_bound_node(graph, relationship.to)? else {
                        continue;
                    };
                    if !bound_node_matches(&target, to) {
                        continue;
                    }

                    let mut bindings = HashMap::new();
                    bindings.insert(
                        from.variable.as_str(),
                        Binding::Node(BoundNode::Local(source.clone())),
                    );
                    bindings.insert(to.variable.as_str(), Binding::Node(target));
                    if let Some(variable) = rel_variable {
                        bindings.insert(variable.as_str(), Binding::Relationship(relationship));
                    }
                    if let Some(row) = projected_row_if_matches(&bindings, query) {
                        rows.push(row);
                    }
                }
            }
            rows
        }
    };
    finish_projected_rows(rows, query)
}

fn finish_projected_rows(rows: Vec<ProjectedRow>, query: &Query) -> QueryResult<Vec<QueryRow>> {
    let rows = if query.has_count_aggregate() {
        aggregate_count_rows(&rows, query)
    } else if query.distinct {
        distinct_rows(rows, &query.returns)
    } else {
        rows
    };
    apply_result_modifiers(rows, &query.modifiers)
}

pub(super) fn execute_physical_query<G: GraphRead + ?Sized>(
    graph: &G,
    query: &Query,
    physical: &PhysicalPlan,
    vector_indexes: Option<&dyn VectorIndexProvider>,
) -> QueryResult<Vec<QueryRow>> {
    validate_physical_plan_for_query(&physical.root, query)?;
    execute_physical_operator(graph, query, &physical.root, vector_indexes)
}

fn execute_physical_operator<G: GraphRead + ?Sized>(
    graph: &G,
    query: &Query,
    operator: &PhysicalOperator,
    vector_indexes: Option<&dyn VectorIndexProvider>,
) -> QueryResult<Vec<QueryRow>> {
    match operator {
        PhysicalOperator::Materialize { input } => {
            execute_physical_operator(graph, query, input, vector_indexes)
        }
        PhysicalOperator::NodeByLabelScan { variable, label } => {
            execute_physical_node_scan(graph, query, variable, Some(label), vector_indexes)
        }
        PhysicalOperator::AllNodesScan { variable } => {
            execute_physical_node_scan(graph, query, variable, None, vector_indexes)
        }
        PhysicalOperator::Project { .. }
        | PhysicalOperator::HashAggregate { .. }
        | PhysicalOperator::HashDistinct { .. }
        | PhysicalOperator::Sort { .. }
        | PhysicalOperator::Offset { .. }
        | PhysicalOperator::Top { .. }
        | PhysicalOperator::PredicateFilter { .. }
        | PhysicalOperator::ExpandOutgoing { .. } => execute_query(graph, query, vector_indexes),
    }
}

fn execute_physical_node_scan<G: GraphRead + ?Sized>(
    graph: &G,
    query: &Query,
    variable: &str,
    label: Option<&String>,
    vector_indexes: Option<&dyn VectorIndexProvider>,
) -> QueryResult<Vec<QueryRow>> {
    let Pattern::Node(pattern) = &query.pattern else {
        return execute_query(graph, query, vector_indexes);
    };
    validate_node_scan_matches_query(query, variable, label)?;
    let nodes = candidate_nodes(graph, pattern, query.predicate.as_ref())?;
    let nodes =
        order_nodes_by_vector_knn(nodes, pattern, query.predicate.as_ref(), vector_indexes)?;
    let rows = nodes
        .into_iter()
        .filter_map(|node| {
            let mut bindings = HashMap::new();
            bindings.insert(
                pattern.variable.as_str(),
                Binding::Node(BoundNode::Local(node)),
            );
            projected_row_if_matches(&bindings, query)
        })
        .collect::<Vec<_>>();
    finish_projected_rows(rows, query)
}

fn validate_physical_plan_for_query(operator: &PhysicalOperator, query: &Query) -> QueryResult<()> {
    match operator {
        PhysicalOperator::Materialize { input }
        | PhysicalOperator::Project { input, .. }
        | PhysicalOperator::HashAggregate { input, .. }
        | PhysicalOperator::HashDistinct { input }
        | PhysicalOperator::Sort { input, .. }
        | PhysicalOperator::Offset { input, .. }
        | PhysicalOperator::Top { input, .. }
        | PhysicalOperator::PredicateFilter { input, .. }
        | PhysicalOperator::ExpandOutgoing { input, .. } => {
            validate_physical_plan_for_query(input, query)
        }
        PhysicalOperator::NodeByLabelScan { variable, label } => {
            validate_node_scan_matches_query(query, variable, Some(label))
        }
        PhysicalOperator::AllNodesScan { variable } => {
            validate_node_scan_matches_query(query, variable, None)
        }
    }
}

fn validate_node_scan_matches_query(
    query: &Query,
    variable: &str,
    label: Option<&String>,
) -> QueryResult<()> {
    let pattern = match &query.pattern {
        Pattern::Node(pattern) => pattern,
        Pattern::Outgoing { from, .. } => from,
    };
    if pattern.variable != variable {
        return Err(QueryError::Unsupported(format!(
            "physical plan scans variable {variable:?}, expected {:?}",
            pattern.variable
        )));
    }
    if let Some(label) = label {
        if pattern.label.as_ref() != Some(label) {
            return Err(QueryError::Unsupported(format!(
                "physical plan scans label {label:?}, expected {:?}",
                pattern.label
            )));
        }
    }
    Ok(())
}

fn candidate_nodes<G: GraphRead + ?Sized>(
    graph: &G,
    pattern: &NodePattern,
    predicate: Option<&Predicate>,
) -> QueryResult<Vec<Node>> {
    if let (Some(label), Some(predicate)) = (&pattern.label, predicate) {
        if let Some(predicate) = indexed_property_predicate(predicate, &pattern.variable) {
            let ids = graph
                .node_ids_by_label_property(label, &predicate.key, &predicate.value)
                .map_err(read_error)?;
            return nodes_by_id(graph, ids);
        }
    }

    if let Some(label) = &pattern.label {
        let ids = graph.node_ids_by_label(label).map_err(read_error)?;
        return nodes_by_id(graph, ids);
    }

    graph.nodes().map_err(read_error)
}

pub(super) fn candidate_node_refs<G: GraphRead + ?Sized>(
    graph: &G,
    pattern: &NodePattern,
    predicate: Option<&Predicate>,
) -> QueryResult<Vec<NodeRef>> {
    if let (Some(label), Some(predicate)) = (&pattern.label, predicate) {
        if let Some(predicate) = indexed_property_predicate(predicate, &pattern.variable) {
            let ids = graph
                .node_ids_by_label_property(label, &predicate.key, &predicate.value)
                .map_err(read_error)?;
            return Ok(nodes_by_id(graph, ids)?
                .into_iter()
                .map(NodeRef::Materialized)
                .collect());
        }
    }

    if let Some(label) = &pattern.label {
        let ids = graph.node_ids_by_label(label).map_err(read_error)?;
        return Ok(nodes_by_id(graph, ids)?
            .into_iter()
            .map(NodeRef::Materialized)
            .collect());
    }

    Ok(graph
        .nodes()
        .map_err(read_error)?
        .into_iter()
        .map(NodeRef::Materialized)
        .collect())
}

#[derive(Clone, Debug, PartialEq)]
pub(super) enum NodeRef {
    Id(u64),
    Materialized(Node),
}

pub(super) struct CypherNodeCursor {
    candidates: Vec<NodeRef>,
    position: usize,
    pattern: NodePattern,
    query: Query,
}

impl CypherNodeCursor {
    pub(super) fn new(candidates: Vec<NodeRef>, pattern: NodePattern, query: Query) -> Self {
        Self {
            candidates,
            position: 0,
            pattern,
            query,
        }
    }
}

impl QueryCursor for CypherNodeCursor {
    fn fetch(&mut self, page_size: usize) -> QueryPage {
        let page_size = page_size.max(1);
        let mut rows = Vec::new();
        while rows.len() < page_size && self.position < self.candidates.len() {
            let node = match self.candidates[self.position].clone() {
                NodeRef::Materialized(node) => node,
                NodeRef::Id(_) => {
                    self.position += 1;
                    continue;
                }
            };
            self.position += 1;

            let mut bindings = HashMap::new();
            bindings.insert(
                self.pattern.variable.as_str(),
                Binding::Node(BoundNode::Local(node)),
            );
            if let Some(row) = row_if_matches(&bindings, &self.query) {
                rows.push(row);
            }
        }

        QueryPage {
            rows,
            has_more: self.position < self.candidates.len(),
        }
    }

    fn total_rows(&self) -> Option<usize> {
        Some(self.candidates.len())
    }
}

pub(super) fn candidate_node_refs_lazy<G: GraphRead + ?Sized>(
    graph: &G,
    pattern: &NodePattern,
    predicate: Option<&Predicate>,
) -> QueryResult<Vec<NodeRef>> {
    if let (Some(label), Some(predicate)) = (&pattern.label, predicate) {
        if let Some(predicate) = indexed_property_predicate(predicate, &pattern.variable) {
            return Ok(graph
                .node_ids_by_label_property(label, &predicate.key, &predicate.value)
                .map_err(read_error)?
                .into_iter()
                .map(NodeRef::Id)
                .collect());
        }
    }

    if let Some(label) = &pattern.label {
        return Ok(graph
            .node_ids_by_label(label)
            .map_err(read_error)?
            .into_iter()
            .map(NodeRef::Id)
            .collect());
    }

    Ok(graph
        .node_ids()
        .map_err(read_error)?
        .into_iter()
        .map(NodeRef::Id)
        .collect())
}

pub(super) struct CypherGraphNodeCursor<G> {
    graph: Arc<G>,
    candidates: Vec<NodeRef>,
    position: usize,
    pattern: NodePattern,
    query: Query,
}

impl<G> CypherGraphNodeCursor<G> {
    pub(super) fn new(
        graph: Arc<G>,
        candidates: Vec<NodeRef>,
        pattern: NodePattern,
        query: Query,
    ) -> Self {
        Self {
            graph,
            candidates,
            position: 0,
            pattern,
            query,
        }
    }
}

impl<G> QueryCursor for CypherGraphNodeCursor<G>
where
    G: GraphRead + Send + Sync + 'static,
{
    fn fetch(&mut self, page_size: usize) -> QueryPage {
        let page_size = page_size.max(1);
        let mut rows = Vec::new();
        while rows.len() < page_size && self.position < self.candidates.len() {
            let node = match self.candidates[self.position].clone() {
                NodeRef::Id(id) => self.graph.node(id).ok().flatten(),
                NodeRef::Materialized(node) => Some(node),
            };
            self.position += 1;

            let Some(node) = node else {
                continue;
            };
            let mut bindings = HashMap::new();
            bindings.insert(
                self.pattern.variable.as_str(),
                Binding::Node(BoundNode::Local(node)),
            );
            if let Some(row) = row_if_matches(&bindings, &self.query) {
                rows.push(row);
            }
        }

        QueryPage {
            rows,
            has_more: self.position < self.candidates.len(),
        }
    }

    fn total_rows(&self) -> Option<usize> {
        Some(self.candidates.len())
    }
}

pub(super) struct CypherGraphOutgoingCursor<G> {
    graph: Arc<G>,
    sources: Vec<NodeRef>,
    source_position: usize,
    current_source: Option<Node>,
    current_relationships: Vec<Relationship>,
    relationship_position: usize,
    from: NodePattern,
    rel_variable: Option<String>,
    rel_type: Option<String>,
    to: NodePattern,
    query: Query,
}

impl<G> CypherGraphOutgoingCursor<G> {
    pub(super) fn new(
        graph: Arc<G>,
        sources: Vec<NodeRef>,
        from: NodePattern,
        rel_variable: Option<String>,
        rel_type: Option<String>,
        to: NodePattern,
        query: Query,
    ) -> Self {
        Self {
            graph,
            sources,
            source_position: 0,
            current_source: None,
            current_relationships: Vec::new(),
            relationship_position: 0,
            from,
            rel_variable,
            rel_type,
            to,
            query,
        }
    }
}

impl<G> QueryCursor for CypherGraphOutgoingCursor<G>
where
    G: GraphRead + Send + Sync + 'static,
{
    fn fetch(&mut self, page_size: usize) -> QueryPage {
        let page_size = page_size.max(1);
        let mut rows = Vec::new();
        while rows.len() < page_size {
            if self.current_source.is_none()
                || self.relationship_position >= self.current_relationships.len()
            {
                if !self.load_next_source() {
                    break;
                }
            }

            while rows.len() < page_size
                && self.relationship_position < self.current_relationships.len()
            {
                let relationship = self.current_relationships[self.relationship_position].clone();
                self.relationship_position += 1;
                let Some(target) = lookup_bound_node(self.graph.as_ref(), relationship.to)
                    .ok()
                    .flatten()
                else {
                    continue;
                };
                if !bound_node_matches(&target, &self.to) {
                    continue;
                }
                let Some(source) = self.current_source.clone() else {
                    continue;
                };

                let mut bindings = HashMap::new();
                bindings.insert(
                    self.from.variable.as_str(),
                    Binding::Node(BoundNode::Local(source)),
                );
                bindings.insert(self.to.variable.as_str(), Binding::Node(target));
                if let Some(variable) = &self.rel_variable {
                    bindings.insert(variable.as_str(), Binding::Relationship(relationship));
                }
                if let Some(row) = row_if_matches(&bindings, &self.query) {
                    rows.push(row);
                }
            }
        }

        QueryPage {
            rows,
            has_more: self.current_source.is_some()
                && self.relationship_position < self.current_relationships.len()
                || self.source_position < self.sources.len(),
        }
    }

    fn total_rows(&self) -> Option<usize> {
        None
    }
}

impl<G> CypherGraphOutgoingCursor<G>
where
    G: GraphRead + Send + Sync + 'static,
{
    fn load_next_source(&mut self) -> bool {
        while self.source_position < self.sources.len() {
            let source = match self.sources[self.source_position].clone() {
                NodeRef::Id(id) => self.graph.node(id).ok().flatten(),
                NodeRef::Materialized(node) => Some(node),
            };
            self.source_position += 1;
            let Some(source) = source else {
                continue;
            };
            let relationships = match &self.rel_type {
                Some(rel_type) => self.graph.outgoing_by_type(source.id, rel_type),
                None => self.graph.outgoing(source.id),
            }
            .unwrap_or_default();

            self.current_source = Some(source);
            self.current_relationships = relationships;
            self.relationship_position = 0;
            if !self.current_relationships.is_empty() {
                return true;
            }
        }

        self.current_source = None;
        self.current_relationships.clear();
        self.relationship_position = 0;
        false
    }
}

fn nodes_by_id<G: GraphRead + ?Sized>(graph: &G, ids: Vec<u64>) -> QueryResult<Vec<Node>> {
    ids.into_iter()
        .map(|id| graph.node(id).map_err(read_error))
        .filter_map(|node| node.transpose())
        .collect()
}

fn order_nodes_by_vector_knn(
    nodes: Vec<Node>,
    pattern: &NodePattern,
    predicate: Option<&Predicate>,
    vector_indexes: Option<&dyn VectorIndexProvider>,
) -> QueryResult<Vec<Node>> {
    let Some(predicate) = predicate.and_then(vector_knn_predicate) else {
        return Ok(nodes);
    };
    if predicate.variable != pattern.variable {
        return Ok(nodes);
    }

    if let Some(vector_indexes) = vector_indexes {
        if let Some(hits) = vector_indexes.search(
            pattern.label.as_deref(),
            &predicate.key,
            &VectorSearch {
                query: predicate.query.clone(),
                k: predicate.k,
                metric: predicate.metric,
            },
        ) {
            let mut nodes_by_id = nodes
                .into_iter()
                .map(|node| (node.id, node))
                .collect::<HashMap<_, _>>();
            return Ok(hits
                .into_iter()
                .filter_map(|hit| nodes_by_id.remove(&hit.node_id))
                .collect());
        }
    }

    let index = HnswVectorIndex::from_nodes(
        &nodes,
        &predicate.key,
        HnswVectorIndexConfig {
            metric: predicate.metric,
            expected_elements: nodes.len().max(1),
            ..HnswVectorIndexConfig::default()
        },
    );
    let hits = index.search(&VectorSearch {
        query: predicate.query.clone(),
        k: predicate.k,
        metric: predicate.metric,
    });
    let mut nodes_by_id = nodes
        .into_iter()
        .map(|node| (node.id, node))
        .collect::<HashMap<_, _>>();
    Ok(hits
        .into_iter()
        .filter_map(|hit| nodes_by_id.remove(&hit.node_id))
        .collect())
}

pub(super) fn is_vector_knn_for_pattern(query: &Query, pattern: &NodePattern) -> bool {
    query
        .predicate
        .as_ref()
        .and_then(vector_knn_predicate)
        .is_some_and(|predicate| predicate.variable == pattern.variable)
}
