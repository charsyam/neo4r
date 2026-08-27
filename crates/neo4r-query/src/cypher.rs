use crate::engine::{QueryCursor, QueryEngine, QueryPage, VecQueryCursor};
use crate::error::{QueryError, QueryResult};
use crate::result::{QueryParams, QueryRow, QueryValue};
use crate::vector::{
    HnswVectorIndex, HnswVectorIndexConfig, VectorIndex, VectorIndexProvider, VectorMetric,
    VectorSearch,
};
use neo4r_core::{BoundaryNode, GraphRead, GraphReadError, Node, Relationship, Value, ValueKey};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Clone, Default)]
pub struct CypherEngine {
    vector_indexes: Option<Arc<dyn VectorIndexProvider>>,
}

impl CypherEngine {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_vector_indexes(vector_indexes: Arc<dyn VectorIndexProvider>) -> Self {
        Self {
            vector_indexes: Some(vector_indexes),
        }
    }
}

impl QueryEngine for CypherEngine {
    fn execute<G: GraphRead + ?Sized>(&self, graph: &G, query: &str) -> QueryResult<Vec<QueryRow>> {
        self.execute_with_params(graph, query, &QueryParams::new())
    }

    fn execute_with_params<G: GraphRead + ?Sized>(
        &self,
        graph: &G,
        query: &str,
        params: &QueryParams,
    ) -> QueryResult<Vec<QueryRow>> {
        let query = parse(query, params)?;
        execute_query(graph, &query, self.vector_indexes.as_deref())
    }

    fn execute_cursor<G: GraphRead + ?Sized>(
        &self,
        graph: &G,
        query: &str,
    ) -> QueryResult<Box<dyn QueryCursor>> {
        self.execute_cursor_with_params(graph, query, &QueryParams::new())
    }

    fn execute_cursor_with_params<G: GraphRead + ?Sized>(
        &self,
        graph: &G,
        query: &str,
        params: &QueryParams,
    ) -> QueryResult<Box<dyn QueryCursor>> {
        let query = parse(query, params)?;
        match &query.pattern {
            _ if query.has_result_modifiers() || query.has_count_aggregate() => {
                Ok(Box::new(VecQueryCursor::new(execute_query(
                    graph,
                    &query,
                    self.vector_indexes.as_deref(),
                )?)))
            }
            Pattern::Node(pattern) if is_vector_knn_for_pattern(&query, pattern) => {
                Ok(Box::new(VecQueryCursor::new(execute_query(
                    graph,
                    &query,
                    self.vector_indexes.as_deref(),
                )?)))
            }
            Pattern::Node(pattern) => {
                let candidates = candidate_node_refs(graph, pattern, query.predicate.as_ref())?;
                Ok(Box::new(CypherNodeCursor::new(
                    candidates,
                    pattern.clone(),
                    query,
                )))
            }
            Pattern::Outgoing { .. } => Ok(Box::new(VecQueryCursor::new(execute_query(
                graph,
                &query,
                self.vector_indexes.as_deref(),
            )?))),
        }
    }

    fn execute_owned_cursor<G>(
        &self,
        graph: Arc<G>,
        query: &str,
    ) -> QueryResult<Box<dyn QueryCursor>>
    where
        G: GraphRead + Send + Sync + 'static,
    {
        self.execute_owned_cursor_with_params(graph, query, QueryParams::new())
    }

    fn execute_owned_cursor_with_params<G>(
        &self,
        graph: Arc<G>,
        query: &str,
        params: QueryParams,
    ) -> QueryResult<Box<dyn QueryCursor>>
    where
        G: GraphRead + Send + Sync + 'static,
    {
        let query = parse(query, &params)?;
        match &query.pattern {
            _ if query.has_result_modifiers() || query.has_count_aggregate() => {
                Ok(Box::new(VecQueryCursor::new(execute_query(
                    graph.as_ref(),
                    &query,
                    self.vector_indexes.as_deref(),
                )?)))
            }
            Pattern::Node(pattern) if is_vector_knn_for_pattern(&query, pattern) => {
                Ok(Box::new(VecQueryCursor::new(execute_query(
                    graph.as_ref(),
                    &query,
                    self.vector_indexes.as_deref(),
                )?)))
            }
            Pattern::Node(pattern) => {
                let candidates =
                    candidate_node_refs_lazy(graph.as_ref(), pattern, query.predicate.as_ref())?;
                Ok(Box::new(CypherGraphNodeCursor::new(
                    graph.clone(),
                    candidates,
                    pattern.clone(),
                    query,
                )))
            }
            Pattern::Outgoing {
                from,
                rel_variable,
                rel_type,
                to,
            } => Ok(Box::new(CypherGraphOutgoingCursor::new(
                graph.clone(),
                candidate_node_refs_lazy(graph.as_ref(), from, query.predicate.as_ref())?,
                from.clone(),
                rel_variable.clone(),
                rel_type.clone(),
                to.clone(),
                query,
            ))),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
struct Query {
    pattern: Pattern,
    predicate: Option<Predicate>,
    returns: Vec<ReturnItem>,
    distinct: bool,
    modifiers: ResultModifiers,
}

impl Query {
    fn has_result_modifiers(&self) -> bool {
        self.distinct
            || !self.modifiers.order_by.is_empty()
            || self.modifiers.skip.unwrap_or(0) > 0
            || self.modifiers.limit.is_some()
    }

    fn has_count_aggregate(&self) -> bool {
        self.returns
            .iter()
            .any(|item| matches!(item, ReturnItem::Count(_)))
    }
}

#[derive(Clone, Debug, PartialEq)]
enum Pattern {
    Node(NodePattern),
    Outgoing {
        from: NodePattern,
        rel_variable: Option<String>,
        rel_type: Option<String>,
        to: NodePattern,
    },
}

#[derive(Clone, Debug, PartialEq)]
struct NodePattern {
    variable: String,
    label: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
enum Predicate {
    Property(PropertyPredicate),
    PropertyNull(PropertyNullPredicate),
    VectorKnn(VectorKnnPredicate),
    And(Vec<Predicate>),
    Or(Vec<Predicate>),
}

#[derive(Clone, Debug, PartialEq)]
struct PropertyPredicate {
    variable: String,
    key: String,
    operator: ComparisonOperator,
    value: Value,
}

#[derive(Clone, Debug, PartialEq)]
struct PropertyNullPredicate {
    variable: String,
    key: String,
    negated: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ComparisonOperator {
    Equal,
    NotEqual,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
}

#[derive(Clone, Debug, PartialEq)]
struct VectorKnnPredicate {
    variable: String,
    key: String,
    query: Vec<f32>,
    k: usize,
    metric: VectorMetric,
}

#[derive(Clone, Debug, PartialEq)]
enum ReturnItem {
    Variable(String),
    Property { variable: String, key: String },
    Count(CountTarget),
}

impl ReturnItem {
    fn name(&self) -> String {
        match self {
            Self::Variable(variable) => variable.clone(),
            Self::Property { variable, key } => format!("{variable}.{key}"),
            Self::Count(CountTarget::All) => "count(*)".to_string(),
            Self::Count(CountTarget::Variable(variable)) => format!("count({variable})"),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
enum CountTarget {
    All,
    Variable(String),
}

#[derive(Clone, Debug, Default, PartialEq)]
struct ResultModifiers {
    order_by: Vec<OrderItem>,
    skip: Option<usize>,
    limit: Option<usize>,
}

#[derive(Clone, Debug, PartialEq)]
struct OrderItem {
    item: ReturnItem,
    direction: SortDirection,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SortDirection {
    Asc,
    Desc,
}

fn execute_query<G: GraphRead + ?Sized>(
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
    let rows = if query.has_count_aggregate() {
        aggregate_count_rows(&rows, query)
    } else if query.distinct {
        distinct_rows(rows, &query.returns)
    } else {
        rows
    };
    apply_result_modifiers(rows, &query.modifiers)
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

fn candidate_node_refs<G: GraphRead + ?Sized>(
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
enum NodeRef {
    Id(u64),
    Materialized(Node),
}

struct CypherNodeCursor {
    candidates: Vec<NodeRef>,
    position: usize,
    pattern: NodePattern,
    query: Query,
}

impl CypherNodeCursor {
    fn new(candidates: Vec<NodeRef>, pattern: NodePattern, query: Query) -> Self {
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

fn candidate_node_refs_lazy<G: GraphRead + ?Sized>(
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

struct CypherGraphNodeCursor<G> {
    graph: Arc<G>,
    candidates: Vec<NodeRef>,
    position: usize,
    pattern: NodePattern,
    query: Query,
}

impl<G> CypherGraphNodeCursor<G> {
    fn new(graph: Arc<G>, candidates: Vec<NodeRef>, pattern: NodePattern, query: Query) -> Self {
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

struct CypherGraphOutgoingCursor<G> {
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
    fn new(
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

fn is_vector_knn_for_pattern(query: &Query, pattern: &NodePattern) -> bool {
    query
        .predicate
        .as_ref()
        .and_then(vector_knn_predicate)
        .is_some_and(|predicate| predicate.variable == pattern.variable)
}

fn row_if_matches(bindings: &HashMap<&str, Binding>, query: &Query) -> Option<QueryRow> {
    projected_row_if_matches(bindings, query).map(|row| row.row)
}

#[derive(Clone, Debug)]
struct ProjectedRow {
    row: QueryRow,
    order_values: Vec<QueryValue>,
}

fn projected_row_if_matches(
    bindings: &HashMap<&str, Binding>,
    query: &Query,
) -> Option<ProjectedRow> {
    if query
        .predicate
        .as_ref()
        .is_some_and(|predicate| !predicate_matches(bindings, predicate))
    {
        return None;
    }

    let order_values = query
        .modifiers
        .order_by
        .iter()
        .map(|item| match item.item {
            ReturnItem::Count(_) => Some(QueryValue::Scalar(Value::Null)),
            _ => query_value_for_return_item(bindings, &item.item),
        })
        .collect::<Option<Vec<_>>>()?;
    let mut row = QueryRow::new();
    for item in &query.returns {
        if matches!(item, ReturnItem::Count(_)) {
            continue;
        }
        let value = query_value_for_return_item(bindings, item)?;
        row.insert(item.name(), value);
    }
    Some(ProjectedRow { row, order_values })
}

fn query_value_for_return_item(
    bindings: &HashMap<&str, Binding>,
    item: &ReturnItem,
) -> Option<QueryValue> {
    match item {
        ReturnItem::Variable(name) => match bindings.get(name.as_str())? {
            Binding::Node(node) => Some(node.to_query_value()),
            Binding::Relationship(relationship) => {
                Some(QueryValue::Relationship(relationship.clone()))
            }
        },
        ReturnItem::Property { variable, key } => match bindings.get(variable.as_str())? {
            Binding::Node(node) => Some(QueryValue::Scalar(
                node.properties().get(key).cloned().unwrap_or(Value::Null),
            )),
            Binding::Relationship(relationship) => Some(QueryValue::Scalar(
                relationship
                    .properties
                    .get(key)
                    .cloned()
                    .unwrap_or(Value::Null),
            )),
        },
        ReturnItem::Count(_) => None,
    }
}

fn aggregate_count_rows(rows: &[ProjectedRow], query: &Query) -> Vec<ProjectedRow> {
    let group_items = query
        .returns
        .iter()
        .filter(|item| !matches!(item, ReturnItem::Count(_)))
        .collect::<Vec<_>>();
    if !group_items.is_empty() {
        return aggregate_count_rows_by_group(rows, query, &group_items);
    }

    let count = rows.len() as i64;
    let mut row = QueryRow::new();
    for item in &query.returns {
        if matches!(item, ReturnItem::Count(_)) {
            row.insert(item.name(), QueryValue::Scalar(Value::Int(count)));
        }
    }
    let order_values = query
        .modifiers
        .order_by
        .iter()
        .map(|item| match item.item {
            ReturnItem::Count(_) => QueryValue::Scalar(Value::Int(count)),
            _ => QueryValue::Scalar(Value::Null),
        })
        .collect();
    vec![ProjectedRow { row, order_values }]
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
enum GroupValueKey {
    Scalar(ValueKey),
    Node(u64),
    BoundaryNode(u64),
    Relationship(u64),
}

#[derive(Clone, Debug)]
struct CountGroup {
    row: QueryRow,
    count: i64,
}

fn aggregate_count_rows_by_group(
    rows: &[ProjectedRow],
    query: &Query,
    group_items: &[&ReturnItem],
) -> Vec<ProjectedRow> {
    let mut groups = HashMap::<Vec<GroupValueKey>, CountGroup>::new();
    for projected in rows {
        let Some(group_key) = group_key_for_row(&projected.row, group_items) else {
            continue;
        };
        groups
            .entry(group_key)
            .and_modify(|group| group.count += 1)
            .or_insert_with(|| CountGroup {
                row: group_row_from_projected_row(&projected.row, group_items),
                count: 1,
            });
    }

    groups
        .into_values()
        .map(|mut group| {
            for item in &query.returns {
                if matches!(item, ReturnItem::Count(_)) {
                    group
                        .row
                        .insert(item.name(), QueryValue::Scalar(Value::Int(group.count)));
                }
            }
            let order_values = query
                .modifiers
                .order_by
                .iter()
                .map(|item| match item.item {
                    ReturnItem::Count(_) => QueryValue::Scalar(Value::Int(group.count)),
                    _ => group
                        .row
                        .get(&item.item.name())
                        .cloned()
                        .unwrap_or(QueryValue::Scalar(Value::Null)),
                })
                .collect();
            ProjectedRow {
                row: group.row,
                order_values,
            }
        })
        .collect()
}

fn group_key_for_row(row: &QueryRow, group_items: &[&ReturnItem]) -> Option<Vec<GroupValueKey>> {
    group_items
        .iter()
        .map(|item| row.get(&item.name()).map(group_value_key))
        .collect()
}

fn group_row_from_projected_row(row: &QueryRow, group_items: &[&ReturnItem]) -> QueryRow {
    let mut group_row = QueryRow::new();
    for item in group_items {
        if let Some(value) = row.get(&item.name()) {
            group_row.insert(item.name(), value.clone());
        }
    }
    group_row
}

fn group_value_key(value: &QueryValue) -> GroupValueKey {
    match value {
        QueryValue::Scalar(value) => GroupValueKey::Scalar(ValueKey::from(value)),
        QueryValue::Node(node) => GroupValueKey::Node(node.id),
        QueryValue::BoundaryNode(node) => GroupValueKey::BoundaryNode(node.id),
        QueryValue::Relationship(relationship) => GroupValueKey::Relationship(relationship.id),
    }
}

fn distinct_rows(rows: Vec<ProjectedRow>, returns: &[ReturnItem]) -> Vec<ProjectedRow> {
    let mut seen = HashMap::<Vec<GroupValueKey>, ()>::new();
    let mut distinct = Vec::new();
    for row in rows {
        let Some(key) = distinct_key_for_row(&row.row, returns) else {
            continue;
        };
        if seen.insert(key, ()).is_none() {
            distinct.push(row);
        }
    }
    distinct
}

fn distinct_key_for_row(row: &QueryRow, returns: &[ReturnItem]) -> Option<Vec<GroupValueKey>> {
    returns
        .iter()
        .map(|item| row.get(&item.name()).map(group_value_key))
        .collect()
}

fn apply_result_modifiers(
    mut rows: Vec<ProjectedRow>,
    modifiers: &ResultModifiers,
) -> QueryResult<Vec<QueryRow>> {
    if !modifiers.order_by.is_empty() {
        rows.sort_by(|left, right| compare_projected_rows(left, right, &modifiers.order_by));
    }
    Ok(rows
        .into_iter()
        .skip(modifiers.skip.unwrap_or(0))
        .take(modifiers.limit.unwrap_or(usize::MAX))
        .map(|row| row.row)
        .collect())
}

fn compare_projected_rows(
    left: &ProjectedRow,
    right: &ProjectedRow,
    order_by: &[OrderItem],
) -> Ordering {
    for (index, order_item) in order_by.iter().enumerate() {
        let ordering = compare_query_values(&left.order_values[index], &right.order_values[index]);
        let ordering = match order_item.direction {
            SortDirection::Asc => ordering,
            SortDirection::Desc => ordering.reverse(),
        };
        if ordering != Ordering::Equal {
            return ordering;
        }
    }
    Ordering::Equal
}

fn compare_query_values(left: &QueryValue, right: &QueryValue) -> Ordering {
    match (left, right) {
        (QueryValue::Scalar(left), QueryValue::Scalar(right)) => compare_values(left, right),
        (QueryValue::Node(left), QueryValue::Node(right)) => left.id.cmp(&right.id),
        (QueryValue::BoundaryNode(left), QueryValue::BoundaryNode(right)) => left.id.cmp(&right.id),
        (QueryValue::Relationship(left), QueryValue::Relationship(right)) => left.id.cmp(&right.id),
        _ => query_value_rank(left).cmp(&query_value_rank(right)),
    }
}

fn query_value_rank(value: &QueryValue) -> u8 {
    match value {
        QueryValue::Scalar(value) => value_rank(value),
        QueryValue::Node(_) => 10,
        QueryValue::BoundaryNode(_) => 11,
        QueryValue::Relationship(_) => 12,
    }
}

fn compare_values(left: &Value, right: &Value) -> Ordering {
    match (left, right) {
        (Value::Bool(left), Value::Bool(right)) => left.cmp(right),
        (Value::Int(left), Value::Int(right)) => left.cmp(right),
        (Value::Float(left), Value::Float(right)) => left.total_cmp(right),
        (Value::Int(left), Value::Float(right)) => (*left as f64).total_cmp(right),
        (Value::Float(left), Value::Int(right)) => left.total_cmp(&(*right as f64)),
        (Value::String(left), Value::String(right)) => left.cmp(right),
        (Value::Vector(left), Value::Vector(right)) => compare_vectors(left, right),
        _ => value_rank(left).cmp(&value_rank(right)),
    }
}

fn compare_vectors(left: &[f32], right: &[f32]) -> Ordering {
    for (left, right) in left.iter().zip(right) {
        let ordering = left.total_cmp(right);
        if ordering != Ordering::Equal {
            return ordering;
        }
    }
    left.len().cmp(&right.len())
}

fn value_rank(value: &Value) -> u8 {
    match value {
        Value::Null => 0,
        Value::Bool(_) => 1,
        Value::Int(_) | Value::Float(_) => 2,
        Value::String(_) => 3,
        Value::Vector(_) => 4,
        Value::Map(_) => 5,
    }
}

fn predicate_matches(bindings: &HashMap<&str, Binding>, predicate: &Predicate) -> bool {
    match predicate {
        Predicate::Property(predicate) => match bindings.get(predicate.variable.as_str()) {
            Some(Binding::Node(node)) => node
                .properties()
                .get(&predicate.key)
                .is_some_and(|value| compare_property_predicate(value, predicate)),
            Some(Binding::Relationship(relationship)) => relationship
                .properties
                .get(&predicate.key)
                .is_some_and(|value| compare_property_predicate(value, predicate)),
            None => false,
        },
        Predicate::PropertyNull(predicate) => match bindings.get(predicate.variable.as_str()) {
            Some(Binding::Node(node)) => {
                property_null_predicate_matches(node.properties().get(&predicate.key), predicate)
            }
            Some(Binding::Relationship(relationship)) => property_null_predicate_matches(
                relationship.properties.get(&predicate.key),
                predicate,
            ),
            None => false,
        },
        Predicate::VectorKnn(_) => true,
        Predicate::And(predicates) => predicates
            .iter()
            .all(|predicate| predicate_matches(bindings, predicate)),
        Predicate::Or(predicates) => predicates
            .iter()
            .any(|predicate| predicate_matches(bindings, predicate)),
    }
}

fn indexed_property_predicate<'a>(
    predicate: &'a Predicate,
    variable: &str,
) -> Option<&'a PropertyPredicate> {
    match predicate {
        Predicate::Property(predicate)
            if predicate.variable == variable
                && predicate.operator == ComparisonOperator::Equal =>
        {
            Some(predicate)
        }
        Predicate::And(predicates) => predicates
            .iter()
            .find_map(|predicate| indexed_property_predicate(predicate, variable)),
        Predicate::Or(_)
        | Predicate::PropertyNull(_)
        | Predicate::Property(_)
        | Predicate::VectorKnn(_) => None,
    }
}

fn compare_property_predicate(value: &Value, predicate: &PropertyPredicate) -> bool {
    let ordering = compare_values(value, &predicate.value);
    match predicate.operator {
        ComparisonOperator::Equal => ordering == Ordering::Equal,
        ComparisonOperator::NotEqual => ordering != Ordering::Equal,
        ComparisonOperator::LessThan => ordering == Ordering::Less,
        ComparisonOperator::LessThanOrEqual => matches!(ordering, Ordering::Less | Ordering::Equal),
        ComparisonOperator::GreaterThan => ordering == Ordering::Greater,
        ComparisonOperator::GreaterThanOrEqual => {
            matches!(ordering, Ordering::Greater | Ordering::Equal)
        }
    }
}

fn property_null_predicate_matches(
    value: Option<&Value>,
    predicate: &PropertyNullPredicate,
) -> bool {
    let is_null = value.is_none_or(|value| matches!(value, Value::Null));
    if predicate.negated {
        !is_null
    } else {
        is_null
    }
}

fn vector_knn_predicate(predicate: &Predicate) -> Option<&VectorKnnPredicate> {
    match predicate {
        Predicate::VectorKnn(predicate) => Some(predicate),
        Predicate::And(predicates) => predicates.iter().find_map(vector_knn_predicate),
        Predicate::Or(predicates) => predicates.iter().find_map(vector_knn_predicate),
        Predicate::PropertyNull(_) | Predicate::Property(_) => None,
    }
}

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

fn parse(input: &str, params: &QueryParams) -> QueryResult<Query> {
    let input = input.trim();
    let input = strip_keyword(input, "MATCH")?;
    let (before_return, return_part) = split_keyword(input, "RETURN")?;
    let (returns, distinct, modifiers) = parse_return_clause(return_part)?;
    let (pattern_part, predicate_part) = match split_keyword(before_return, "WHERE") {
        Ok((pattern, predicate)) => (pattern, Some(predicate)),
        Err(QueryError::Parse(_)) => (before_return, None),
        Err(err) => return Err(err),
    };

    Ok(Query {
        pattern: parse_pattern(pattern_part.trim())?,
        predicate: predicate_part
            .map(|predicate| parse_predicate(predicate, params))
            .transpose()?,
        returns,
        distinct,
        modifiers,
    })
}

fn parse_pattern(input: &str) -> QueryResult<Pattern> {
    let compact = input
        .chars()
        .filter(|ch| !ch.is_whitespace())
        .collect::<String>();
    if let Some((left, right)) = compact.split_once("->") {
        let (from_part, rel_part) = left
            .split_once("-")
            .ok_or_else(|| QueryError::Parse("expected relationship pattern".to_string()))?;
        let relationship = parse_relationship_pattern(rel_part)?;
        return Ok(Pattern::Outgoing {
            from: parse_node_pattern(from_part)?,
            rel_variable: relationship.variable,
            rel_type: relationship.rel_type,
            to: parse_node_pattern(right)?,
        });
    }

    Ok(Pattern::Node(parse_node_pattern(&compact)?))
}

fn parse_node_pattern(input: &str) -> QueryResult<NodePattern> {
    let inner = strip_wrapping(input, '(', ')')?;
    let (variable, label) = match inner.split_once(':') {
        Some((variable, label)) => (variable, Some(label)),
        None => (inner, None),
    };
    validate_identifier(variable)?;
    if let Some(label) = label {
        validate_identifier(label)?;
    }
    Ok(NodePattern {
        variable: variable.to_string(),
        label: label.map(ToString::to_string),
    })
}

#[derive(Clone, Debug, PartialEq)]
struct RelationshipPattern {
    variable: Option<String>,
    rel_type: Option<String>,
}

fn parse_relationship_pattern(input: &str) -> QueryResult<RelationshipPattern> {
    let inner = strip_wrapping(input, '[', ']')?;
    if inner.is_empty() {
        return Ok(RelationshipPattern {
            variable: None,
            rel_type: None,
        });
    }
    let (variable, rel_type) = match inner.split_once(':') {
        Some((variable, rel_type)) => {
            let variable = if variable.is_empty() {
                None
            } else {
                validate_identifier(variable)?;
                Some(variable.to_string())
            };
            validate_identifier(rel_type)?;
            (variable, Some(rel_type.to_string()))
        }
        None => {
            validate_identifier(inner)?;
            (Some(inner.to_string()), None)
        }
    };
    Ok(RelationshipPattern { variable, rel_type })
}

fn parse_predicate(input: &str, params: &QueryParams) -> QueryResult<Predicate> {
    let input = input.trim();
    if let Some(inner) = strip_outer_parentheses(input) {
        return parse_predicate(inner, params);
    }
    let or_parts = split_top_level_keyword(input, "OR");
    if or_parts.len() > 1 {
        return Ok(Predicate::Or(
            or_parts
                .into_iter()
                .map(|part| parse_predicate(part, params))
                .collect::<QueryResult<Vec<_>>>()?,
        ));
    }
    let parts = split_top_level_keyword(input, "AND");
    if parts.len() > 1 {
        return Ok(Predicate::And(
            parts
                .into_iter()
                .map(|part| parse_predicate(part, params))
                .collect::<QueryResult<Vec<_>>>()?,
        ));
    }
    if let Some(inner) = input
        .strip_prefix("vector.knn(")
        .and_then(|value| value.strip_suffix(')'))
    {
        let args = split_top_level_commas(inner);
        if !(args.len() == 3 || args.len() == 4) {
            return Err(QueryError::Parse(
                "vector.knn requires property, vector, k, and optional metric arguments"
                    .to_string(),
            ));
        }
        let (variable, key) = parse_property_ref(args[0])?;
        let k = parse_knn_k(args[2].trim(), params)?;
        if k == 0 {
            return Err(QueryError::Parse(
                "vector.knn k must be greater than zero".to_string(),
            ));
        }
        return Ok(Predicate::VectorKnn(VectorKnnPredicate {
            variable,
            key,
            query: parse_vector_argument(args[1].trim(), params)?,
            k,
            metric: if args.len() == 4 {
                parse_vector_metric(args[3].trim(), params)?
            } else {
                VectorMetric::Cosine
            },
        }));
    }

    if let Some(predicate) = parse_property_null_predicate(input)? {
        return Ok(Predicate::PropertyNull(predicate));
    }

    let (left, operator, right) = split_comparison_predicate(input)?;
    let (variable, key) = parse_property_ref(left.trim())?;
    Ok(Predicate::Property(PropertyPredicate {
        variable,
        key,
        operator,
        value: parse_literal(right.trim(), params)?,
    }))
}

fn parse_property_null_predicate(input: &str) -> QueryResult<Option<PropertyNullPredicate>> {
    let Some((property_ref, negated)) = strip_null_predicate_suffix(input) else {
        return Ok(None);
    };
    let (variable, key) = parse_property_ref(property_ref.trim())?;
    Ok(Some(PropertyNullPredicate {
        variable,
        key,
        negated,
    }))
}

fn strip_null_predicate_suffix(input: &str) -> Option<(&str, bool)> {
    let input = input.trim();
    let upper = input.to_ascii_uppercase();
    if upper.ends_with(" IS NOT NULL") {
        let end = input.len() - " IS NOT NULL".len();
        return Some((&input[..end], true));
    }
    if upper.ends_with(" IS NULL") {
        let end = input.len() - " IS NULL".len();
        return Some((&input[..end], false));
    }
    None
}

fn split_comparison_predicate(input: &str) -> QueryResult<(&str, ComparisonOperator, &str)> {
    for (symbol, operator) in [
        (">=", ComparisonOperator::GreaterThanOrEqual),
        ("<=", ComparisonOperator::LessThanOrEqual),
        ("<>", ComparisonOperator::NotEqual),
        ("!=", ComparisonOperator::NotEqual),
        ("=", ComparisonOperator::Equal),
        ("<", ComparisonOperator::LessThan),
        (">", ComparisonOperator::GreaterThan),
    ] {
        if let Some((left, right)) = input.split_once(symbol) {
            if left.trim().is_empty() || right.trim().is_empty() {
                return Err(QueryError::Parse(
                    "WHERE comparison requires both left and right operands".to_string(),
                ));
            }
            return Ok((left, operator, right));
        }
    }
    Err(QueryError::Parse(
        "WHERE only supports comparison predicates".to_string(),
    ))
}

fn parse_property_ref(input: &str) -> QueryResult<(String, String)> {
    let (variable, key) = input
        .trim()
        .split_once('.')
        .ok_or_else(|| QueryError::Parse("WHERE must use variable.property".to_string()))?;
    validate_identifier(variable.trim())?;
    validate_identifier(key.trim())?;
    Ok((variable.trim().to_string(), key.trim().to_string()))
}

fn parse_literal(input: &str, params: &QueryParams) -> QueryResult<Value> {
    if let Some(name) = input.strip_prefix('$') {
        validate_identifier(name)?;
        return params
            .get(name)
            .cloned()
            .ok_or_else(|| QueryError::Parse(format!("missing query parameter ${name}")));
    }
    if input.starts_with('[') {
        return Ok(Value::Vector(parse_vector_literal(input)?));
    }
    if let Some(value) = input.strip_prefix('"').and_then(|v| v.strip_suffix('"')) {
        return Ok(Value::String(value.to_string()));
    }
    if input.eq_ignore_ascii_case("true") {
        return Ok(Value::Bool(true));
    }
    if input.eq_ignore_ascii_case("false") {
        return Ok(Value::Bool(false));
    }
    if let Ok(value) = input.parse::<i64>() {
        return Ok(Value::Int(value));
    }
    if let Ok(value) = input.parse::<f64>() {
        return Ok(Value::Float(value));
    }
    Err(QueryError::Parse(format!("unsupported literal {input:?}")))
}

fn parse_vector_literal(input: &str) -> QueryResult<Vec<f32>> {
    let inner = strip_wrapping(input.trim(), '[', ']')?;
    if inner.trim().is_empty() {
        return Err(QueryError::Parse(
            "vector literal must contain at least one value".to_string(),
        ));
    }
    inner
        .split(',')
        .map(|item| {
            item.trim().parse::<f32>().map_err(|_| {
                QueryError::Parse(format!("invalid vector literal element {:?}", item.trim()))
            })
        })
        .collect()
}

fn parse_vector_argument(input: &str, params: &QueryParams) -> QueryResult<Vec<f32>> {
    match parse_literal(input, params)? {
        Value::Vector(vector) => Ok(vector),
        value => Err(QueryError::Parse(format!(
            "vector argument must resolve to a vector, got {value:?}"
        ))),
    }
}

fn parse_knn_k(input: &str, params: &QueryParams) -> QueryResult<usize> {
    if let Some(name) = input.strip_prefix('$') {
        validate_identifier(name)?;
        return match params.get(name) {
            Some(Value::Int(value)) if *value >= 0 => Ok(*value as usize),
            Some(value) => Err(QueryError::Parse(format!(
                "vector.knn k parameter ${name} must be a non-negative integer, got {value:?}"
            ))),
            None => Err(QueryError::Parse(format!(
                "missing query parameter ${name}"
            ))),
        };
    }
    input
        .parse::<usize>()
        .map_err(|_| QueryError::Parse("vector.knn k must be a positive integer".to_string()))
}

fn parse_vector_metric(input: &str, params: &QueryParams) -> QueryResult<VectorMetric> {
    let metric = if let Some(name) = input.strip_prefix('$') {
        validate_identifier(name)?;
        match params.get(name) {
            Some(Value::String(value)) => value.as_str(),
            Some(value) => {
                return Err(QueryError::Parse(format!(
                    "vector metric parameter ${name} must be a string, got {value:?}"
                )))
            }
            None => {
                return Err(QueryError::Parse(format!(
                    "missing query parameter ${name}"
                )))
            }
        }
    } else {
        input
            .strip_prefix('"')
            .and_then(|value| value.strip_suffix('"'))
            .unwrap_or(input)
            .trim()
    };
    if metric.eq_ignore_ascii_case("cosine") {
        Ok(VectorMetric::Cosine)
    } else if metric.eq_ignore_ascii_case("l2") {
        Ok(VectorMetric::L2)
    } else {
        Err(QueryError::Parse(format!(
            "unsupported vector metric {input:?}"
        )))
    }
}

fn split_top_level_commas(input: &str) -> Vec<&str> {
    let mut values = Vec::new();
    let mut start = 0;
    let mut bracket_depth = 0_i32;
    for (index, ch) in input.char_indices() {
        match ch {
            '[' => bracket_depth += 1,
            ']' => bracket_depth -= 1,
            ',' if bracket_depth == 0 => {
                values.push(input[start..index].trim());
                start = index + ch.len_utf8();
            }
            _ => {}
        }
    }
    values.push(input[start..].trim());
    values
}

fn split_top_level_keyword<'a>(input: &'a str, keyword: &str) -> Vec<&'a str> {
    let mut values = Vec::new();
    let mut start = 0;
    let mut paren_depth = 0_i32;
    let mut bracket_depth = 0_i32;
    let mut in_string = false;
    let upper = input.to_ascii_uppercase();
    let keyword_upper = keyword.to_ascii_uppercase();
    let keyword_len = keyword.len();
    let bytes = input.as_bytes();
    let mut index = 0;

    while index < input.len() {
        let ch = input[index..].chars().next().expect("valid char boundary");
        match ch {
            '"' => {
                in_string = !in_string;
                index += ch.len_utf8();
                continue;
            }
            '(' if !in_string => paren_depth += 1,
            ')' if !in_string => paren_depth -= 1,
            '[' if !in_string => bracket_depth += 1,
            ']' if !in_string => bracket_depth -= 1,
            _ => {}
        }

        if !in_string
            && paren_depth == 0
            && bracket_depth == 0
            && upper[index..].starts_with(&keyword_upper)
            && is_keyword_boundary(bytes, index, keyword_len)
        {
            values.push(input[start..index].trim());
            index += keyword_len;
            start = index;
            continue;
        }
        index += ch.len_utf8();
    }

    values.push(input[start..].trim());
    values
}

fn strip_outer_parentheses(input: &str) -> Option<&str> {
    let input = input.trim();
    if !input.starts_with('(') || !input.ends_with(')') {
        return None;
    }
    let mut depth = 0_i32;
    let mut in_string = false;
    for (index, ch) in input.char_indices() {
        match ch {
            '"' => in_string = !in_string,
            '(' if !in_string => depth += 1,
            ')' if !in_string => {
                depth -= 1;
                if depth == 0 && index + ch.len_utf8() != input.len() {
                    return None;
                }
            }
            _ => {}
        }
        if depth < 0 {
            return None;
        }
    }
    (depth == 0).then(|| input[1..input.len() - 1].trim())
}

fn is_keyword_boundary(input: &[u8], index: usize, keyword_len: usize) -> bool {
    let before = index
        .checked_sub(1)
        .and_then(|offset| input.get(offset))
        .is_none_or(|byte| byte.is_ascii_whitespace());
    let after = input
        .get(index + keyword_len)
        .is_none_or(|byte| byte.is_ascii_whitespace());
    before && after
}

fn parse_return_clause(input: &str) -> QueryResult<(Vec<ReturnItem>, bool, ResultModifiers)> {
    let input = input.trim();
    let modifier_start = find_earliest_result_modifier(input);
    let (return_part, modifier_part) = match modifier_start {
        Some(index) => (&input[..index], input[index..].trim()),
        None => (input, ""),
    };
    let (return_part, distinct) = parse_distinct_return_prefix(return_part);
    let returns = parse_returns(return_part)?;
    ensure_supported_aggregate_returns(&returns, distinct)?;
    Ok((returns, distinct, parse_result_modifiers(modifier_part)?))
}

fn parse_distinct_return_prefix(input: &str) -> (&str, bool) {
    strip_keyword_prefix(input.trim(), "DISTINCT")
        .map(|rest| (rest, true))
        .unwrap_or((input.trim(), false))
}

fn ensure_supported_aggregate_returns(returns: &[ReturnItem], distinct: bool) -> QueryResult<()> {
    if distinct
        && returns
            .iter()
            .any(|item| matches!(item, ReturnItem::Count(_)))
    {
        return Err(QueryError::Parse(
            "RETURN DISTINCT count(...) is not supported yet".to_string(),
        ));
    }
    Ok(())
}

fn find_earliest_result_modifier(input: &str) -> Option<usize> {
    ["ORDER BY", "SKIP", "LIMIT"]
        .into_iter()
        .filter_map(|keyword| find_result_modifier_keyword(input, keyword))
        .min()
}

fn parse_result_modifiers(input: &str) -> QueryResult<ResultModifiers> {
    let mut modifiers = ResultModifiers::default();
    let mut remaining = input.trim();
    while !remaining.is_empty() {
        if let Some(rest) = strip_keyword_prefix(remaining, "ORDER BY") {
            let (part, rest) = take_until_result_modifier(rest, &["SKIP", "LIMIT"]);
            modifiers.order_by = parse_order_items(part)?;
            remaining = rest.trim();
        } else if let Some(rest) = strip_keyword_prefix(remaining, "SKIP") {
            let (part, rest) = take_until_result_modifier(rest, &["LIMIT"]);
            modifiers.skip = Some(parse_non_negative_usize(part, "SKIP")?);
            remaining = rest.trim();
        } else if let Some(rest) = strip_keyword_prefix(remaining, "LIMIT") {
            modifiers.limit = Some(parse_non_negative_usize(rest, "LIMIT")?);
            remaining = "";
        } else {
            return Err(QueryError::Parse(format!(
                "unsupported RETURN modifier {remaining:?}"
            )));
        }
    }
    Ok(modifiers)
}

fn take_until_result_modifier<'a>(input: &'a str, keywords: &[&str]) -> (&'a str, &'a str) {
    let Some(index) = keywords
        .iter()
        .filter_map(|keyword| find_result_modifier_keyword(input, keyword))
        .min()
    else {
        return (input.trim(), "");
    };
    (&input[..index], &input[index..])
}

fn parse_order_items(input: &str) -> QueryResult<Vec<OrderItem>> {
    let items = input
        .split(',')
        .map(str::trim)
        .filter(|item| !item.is_empty())
        .map(parse_order_item)
        .collect::<QueryResult<Vec<_>>>()?;
    if items.is_empty() {
        Err(QueryError::Parse(
            "ORDER BY requires at least one item".to_string(),
        ))
    } else {
        Ok(items)
    }
}

fn parse_order_item(input: &str) -> QueryResult<OrderItem> {
    let mut parts = input.rsplitn(2, char::is_whitespace);
    let tail = parts.next().unwrap_or("").trim();
    let (item, direction) = if tail.eq_ignore_ascii_case("ASC") {
        (parts.next().unwrap_or("").trim(), SortDirection::Asc)
    } else if tail.eq_ignore_ascii_case("DESC") {
        (parts.next().unwrap_or("").trim(), SortDirection::Desc)
    } else {
        (input.trim(), SortDirection::Asc)
    };
    Ok(OrderItem {
        item: parse_return_item(item)?,
        direction,
    })
}

fn parse_non_negative_usize(input: &str, name: &str) -> QueryResult<usize> {
    let input = input.trim();
    if input.is_empty() {
        return Err(QueryError::Parse(format!("{name} requires a value")));
    }
    input
        .parse::<usize>()
        .map_err(|_| QueryError::Parse(format!("{name} requires a non-negative integer")))
}

fn parse_returns(input: &str) -> QueryResult<Vec<ReturnItem>> {
    let returns = input
        .split(',')
        .map(str::trim)
        .filter(|item| !item.is_empty())
        .map(parse_return_item)
        .collect::<QueryResult<Vec<_>>>()?;

    if returns.is_empty() {
        Err(QueryError::Parse(
            "RETURN requires at least one item".to_string(),
        ))
    } else {
        Ok(returns)
    }
}

fn parse_return_item(item: &str) -> QueryResult<ReturnItem> {
    let item = item.trim();
    let item_lower = item.to_ascii_lowercase();
    if item_lower.starts_with("count(") && item.ends_with(')') {
        let inner = item["count(".len()..item.len() - 1].trim();
        if inner == "*" {
            return Ok(ReturnItem::Count(CountTarget::All));
        }
        validate_identifier(inner)?;
        return Ok(ReturnItem::Count(CountTarget::Variable(inner.to_string())));
    }
    if let Some((variable, key)) = item.split_once('.') {
        validate_identifier(variable.trim())?;
        validate_identifier(key.trim())?;
        Ok(ReturnItem::Property {
            variable: variable.trim().to_string(),
            key: key.trim().to_string(),
        })
    } else {
        validate_identifier(item)?;
        Ok(ReturnItem::Variable(item.to_string()))
    }
}

fn strip_keyword<'a>(input: &'a str, keyword: &str) -> QueryResult<&'a str> {
    input
        .strip_prefix(keyword)
        .or_else(|| input.strip_prefix(&keyword.to_ascii_lowercase()))
        .map(str::trim)
        .ok_or_else(|| QueryError::Parse(format!("expected {keyword}")))
}

fn split_keyword<'a>(input: &'a str, keyword: &str) -> QueryResult<(&'a str, &'a str)> {
    let Some(index) = find_keyword(input, keyword) else {
        return Err(QueryError::Parse(format!("expected {keyword}")));
    };
    Ok((&input[..index], &input[index + keyword.len()..]))
}

fn find_keyword(input: &str, keyword: &str) -> Option<usize> {
    input
        .to_ascii_uppercase()
        .find(&keyword.to_ascii_uppercase())
}

fn find_result_modifier_keyword(input: &str, keyword: &str) -> Option<usize> {
    let upper = input.to_ascii_uppercase();
    let keyword = keyword.to_ascii_uppercase();
    let bytes = input.as_bytes();
    upper
        .match_indices(&keyword)
        .find_map(|(index, _)| is_keyword_boundary(bytes, index, keyword.len()).then_some(index))
}

fn strip_keyword_prefix<'a>(input: &'a str, keyword: &str) -> Option<&'a str> {
    input
        .strip_prefix(keyword)
        .or_else(|| input.strip_prefix(&keyword.to_ascii_lowercase()))
        .map(str::trim)
}

fn strip_wrapping(input: &str, open: char, close: char) -> QueryResult<&str> {
    input
        .strip_prefix(open)
        .and_then(|value| value.strip_suffix(close))
        .ok_or_else(|| QueryError::Parse(format!("expected {open}...{close} pattern")))
}

fn validate_identifier(input: &str) -> QueryResult<()> {
    let mut chars = input.chars();
    let Some(first) = chars.next() else {
        return Err(QueryError::Parse("empty identifier".to_string()));
    };
    if !(first == '_' || first.is_ascii_alphabetic()) {
        return Err(QueryError::Parse(format!("invalid identifier {input:?}")));
    }
    if chars.any(|ch| !(ch == '_' || ch.is_ascii_alphanumeric())) {
        return Err(QueryError::Parse(format!("invalid identifier {input:?}")));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vector::VectorHit;
    use neo4r_core::{Command, GraphState, Properties};
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn properties(entries: &[(&str, Value)]) -> Properties {
        entries
            .iter()
            .map(|(key, value)| ((*key).to_string(), value.clone()))
            .collect()
    }

    fn graph() -> GraphState {
        let mut graph = GraphState::new();
        graph
            .apply(Command::CreateNode {
                id: 1,
                labels: vec!["Person".to_string()],
                properties: properties(&[
                    ("name", Value::String("Alice".to_string())),
                    ("status", Value::String("active".to_string())),
                    ("age", Value::Int(30)),
                ]),
            })
            .unwrap();
        graph
            .apply(Command::CreateNode {
                id: 2,
                labels: vec!["Person".to_string()],
                properties: properties(&[("name", Value::String("Bob".to_string()))]),
            })
            .unwrap();
        graph
            .apply(Command::CreateRelationship {
                id: 1,
                from: 1,
                to: 2,
                rel_type: "KNOWS".to_string(),
                properties: Properties::new(),
            })
            .unwrap();
        graph
    }

    #[test]
    fn matches_nodes_by_label() {
        let rows = CypherEngine::new()
            .execute(&graph(), "MATCH (n:Person) RETURN n")
            .unwrap();

        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn filters_nodes_by_property() {
        let rows = CypherEngine::new()
            .execute(
                &graph(),
                r#"MATCH (n:Person) WHERE n.name = "Alice" RETURN n"#,
            )
            .unwrap();

        assert_eq!(rows.len(), 1);
        assert!(matches!(rows[0].get("n"), Some(QueryValue::Node(node)) if node.id == 1));
    }

    #[test]
    fn returns_null_for_missing_property() {
        let rows = CypherEngine::new()
            .execute(
                &graph(),
                r#"MATCH (n:Person) WHERE n.name = "Alice" RETURN n.missing"#,
            )
            .unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].get("n.missing"),
            Some(&QueryValue::Scalar(Value::Null))
        );
    }

    #[test]
    fn filters_nodes_by_and_predicate() {
        let rows = CypherEngine::new()
            .execute(
                &graph(),
                r#"MATCH (n:Person) WHERE n.name = "Alice" AND n.age = 30 RETURN n"#,
            )
            .unwrap();

        assert_eq!(rows.len(), 1);
        assert!(matches!(rows[0].get("n"), Some(QueryValue::Node(node)) if node.id == 1));
    }

    #[test]
    fn filters_nodes_by_comparison_predicates() {
        let mut graph = graph();
        graph
            .apply(Command::CreateNode {
                id: 3,
                labels: vec!["Person".to_string()],
                properties: properties(&[
                    ("name", Value::String("Carol".to_string())),
                    ("age", Value::Int(25)),
                ]),
            })
            .unwrap();

        let rows = CypherEngine::new()
            .execute(
                &graph,
                r#"MATCH (n:Person) WHERE n.age >= 25 AND n.age < 30 RETURN n.name ORDER BY n.name ASC"#,
            )
            .unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].get("n.name"),
            Some(&QueryValue::Scalar(Value::String("Carol".to_string())))
        );

        let rows = CypherEngine::new()
            .execute(
                &graph,
                r#"MATCH (n:Person) WHERE n.name <> "Bob" RETURN n.name ORDER BY n.name ASC"#,
            )
            .unwrap();

        assert_eq!(rows.len(), 2);
        assert_eq!(
            rows[0].get("n.name"),
            Some(&QueryValue::Scalar(Value::String("Alice".to_string())))
        );
        assert_eq!(
            rows[1].get("n.name"),
            Some(&QueryValue::Scalar(Value::String("Carol".to_string())))
        );
    }

    #[test]
    fn filters_nodes_by_or_predicate_with_and_precedence() {
        let mut graph = graph();
        graph
            .apply(Command::CreateNode {
                id: 3,
                labels: vec!["Person".to_string()],
                properties: properties(&[
                    ("name", Value::String("Carol".to_string())),
                    ("status", Value::String("active".to_string())),
                    ("age", Value::Int(25)),
                ]),
            })
            .unwrap();

        let rows = CypherEngine::new()
            .execute(
                &graph,
                r#"MATCH (n:Person) WHERE n.name = "Bob" OR n.status = "active" AND n.age < 30 RETURN n.name ORDER BY n.name ASC"#,
            )
            .unwrap();

        assert_eq!(rows.len(), 2);
        assert_eq!(
            rows[0].get("n.name"),
            Some(&QueryValue::Scalar(Value::String("Bob".to_string())))
        );
        assert_eq!(
            rows[1].get("n.name"),
            Some(&QueryValue::Scalar(Value::String("Carol".to_string())))
        );
    }

    #[test]
    fn filters_nodes_by_parenthesized_predicates() {
        let mut graph = graph();
        graph
            .apply(Command::CreateNode {
                id: 3,
                labels: vec!["Person".to_string()],
                properties: properties(&[
                    ("name", Value::String("Carol".to_string())),
                    ("status", Value::String("active".to_string())),
                    ("age", Value::Int(25)),
                ]),
            })
            .unwrap();

        let rows = CypherEngine::new()
            .execute(
                &graph,
                r#"MATCH (n:Person) WHERE (n.name = "Bob" OR n.status = "active") AND n.age < 30 RETURN n.name"#,
            )
            .unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].get("n.name"),
            Some(&QueryValue::Scalar(Value::String("Carol".to_string())))
        );
    }

    #[test]
    fn filters_nodes_by_null_predicates() {
        let rows = CypherEngine::new()
            .execute(
                &graph(),
                "MATCH (n:Person) WHERE n.status IS NULL RETURN n.name",
            )
            .unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].get("n.name"),
            Some(&QueryValue::Scalar(Value::String("Bob".to_string())))
        );

        let rows = CypherEngine::new()
            .execute(
                &graph(),
                "MATCH (n:Person) WHERE n.status IS NOT NULL RETURN n.name",
            )
            .unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].get("n.name"),
            Some(&QueryValue::Scalar(Value::String("Alice".to_string())))
        );
    }

    #[test]
    fn matches_outgoing_relationship_pattern() {
        let rows = CypherEngine::new()
            .execute(
                &graph(),
                r#"MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE a.name = "Alice" RETURN b"#,
            )
            .unwrap();

        assert_eq!(rows.len(), 1);
        assert!(matches!(rows[0].get("b"), Some(QueryValue::Node(node)) if node.id == 2));
    }

    #[test]
    fn returns_and_filters_relationship_variables() {
        let mut graph = graph();
        graph
            .apply(Command::SetRelationshipProperty {
                id: 1,
                key: "since".to_string(),
                value: Value::Int(2026),
            })
            .unwrap();

        let rows = CypherEngine::new()
            .execute(
                &graph,
                r#"MATCH (a:Person)-[r:KNOWS]->(b:Person) WHERE r.since = 2026 RETURN r.since"#,
            )
            .unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].get("r.since"),
            Some(&QueryValue::Scalar(Value::Int(2026)))
        );
    }

    #[test]
    fn filters_relationships_by_comparison_predicates() {
        let mut graph = graph();
        graph
            .apply(Command::SetRelationshipProperty {
                id: 1,
                key: "since".to_string(),
                value: Value::Int(2026),
            })
            .unwrap();

        let rows = CypherEngine::new()
            .execute(
                &graph,
                "MATCH (a:Person)-[r:KNOWS]->(b:Person) WHERE r.since > 2020 RETURN r.since",
            )
            .unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].get("r.since"),
            Some(&QueryValue::Scalar(Value::Int(2026)))
        );
    }

    #[test]
    fn filters_relationships_by_or_predicate() {
        let mut graph = graph();
        graph
            .apply(Command::SetRelationshipProperty {
                id: 1,
                key: "since".to_string(),
                value: Value::Int(2026),
            })
            .unwrap();

        let rows = CypherEngine::new()
            .execute(
                &graph,
                r#"MATCH (a:Person)-[r:KNOWS]->(b:Person) WHERE r.since < 2020 OR b.name = "Bob" RETURN r.since"#,
            )
            .unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].get("r.since"),
            Some(&QueryValue::Scalar(Value::Int(2026)))
        );
    }

    #[test]
    fn filters_relationships_by_parenthesized_predicates() {
        let mut graph = graph();
        graph
            .apply(Command::SetRelationshipProperty {
                id: 1,
                key: "since".to_string(),
                value: Value::Int(2026),
            })
            .unwrap();

        let rows = CypherEngine::new()
            .execute(
                &graph,
                r#"MATCH (a:Person)-[r:KNOWS]->(b:Person) WHERE (r.since < 2020 OR b.name = "Bob") AND a.name = "Alice" RETURN r.since"#,
            )
            .unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].get("r.since"),
            Some(&QueryValue::Scalar(Value::Int(2026)))
        );
    }

    #[test]
    fn filters_relationships_by_null_predicates() {
        let mut graph = graph();
        graph
            .apply(Command::SetRelationshipProperty {
                id: 1,
                key: "since".to_string(),
                value: Value::Int(2026),
            })
            .unwrap();

        let rows = CypherEngine::new()
            .execute(
                &graph,
                "MATCH (a:Person)-[r:KNOWS]->(b:Person) WHERE r.since IS NOT NULL RETURN r.since",
            )
            .unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].get("r.since"),
            Some(&QueryValue::Scalar(Value::Int(2026)))
        );

        let rows = CypherEngine::new()
            .execute(
                &graph,
                "MATCH (a:Person)-[r:KNOWS]->(b:Person) WHERE r.missing IS NULL RETURN r.since",
            )
            .unwrap();

        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn filters_relationships_by_and_predicate() {
        let mut graph = graph();
        graph
            .apply(Command::SetRelationshipProperty {
                id: 1,
                key: "since".to_string(),
                value: Value::Int(2026),
            })
            .unwrap();

        let rows = CypherEngine::new()
            .execute(
                &graph,
                r#"MATCH (a:Person)-[r:KNOWS]->(b:Person) WHERE a.name = "Alice" AND r.since = 2026 AND b.name = "Bob" RETURN r"#,
            )
            .unwrap();

        assert_eq!(rows.len(), 1);
        assert!(
            matches!(rows[0].get("r"), Some(QueryValue::Relationship(relationship)) if relationship.id == 1)
        );
    }

    #[test]
    fn orders_skips_and_limits_node_results() {
        let mut graph = graph();
        graph
            .apply(Command::CreateNode {
                id: 3,
                labels: vec!["Person".to_string()],
                properties: properties(&[
                    ("name", Value::String("Carol".to_string())),
                    ("age", Value::Int(25)),
                ]),
            })
            .unwrap();

        let rows = CypherEngine::new()
            .execute(
                &graph,
                "MATCH (n:Person) RETURN n.name ORDER BY n.age DESC, n.name ASC SKIP 1 LIMIT 1",
            )
            .unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].get("n.name"),
            Some(&QueryValue::Scalar(Value::String("Carol".to_string())))
        );
    }

    #[test]
    fn orders_relationship_results_by_property_not_in_return() {
        let mut graph = graph();
        graph
            .apply(Command::SetRelationshipProperty {
                id: 1,
                key: "since".to_string(),
                value: Value::Int(2026),
            })
            .unwrap();
        graph
            .apply(Command::CreateRelationship {
                id: 2,
                from: 1,
                to: 2,
                rel_type: "KNOWS".to_string(),
                properties: properties(&[("since", Value::Int(2024))]),
            })
            .unwrap();

        let rows = CypherEngine::new()
            .execute(
                &graph,
                "MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN r.since ORDER BY r.since ASC LIMIT 1",
            )
            .unwrap();

        assert_eq!(
            rows[0].get("r.since"),
            Some(&QueryValue::Scalar(Value::Int(2024)))
        );
    }

    #[test]
    fn cursor_with_result_modifiers_fetches_materialized_rows() {
        let mut graph = graph();
        graph
            .apply(Command::CreateNode {
                id: 3,
                labels: vec!["Person".to_string()],
                properties: properties(&[
                    ("name", Value::String("Carol".to_string())),
                    ("age", Value::Int(25)),
                ]),
            })
            .unwrap();
        let mut cursor = CypherEngine::new()
            .execute_cursor(
                &graph,
                "MATCH (n:Person) RETURN n.name ORDER BY n.name ASC LIMIT 2",
            )
            .unwrap();

        assert_eq!(cursor.total_rows(), Some(2));
        let page = cursor.fetch(10);
        assert_eq!(page.rows.len(), 2);
        assert!(!page.has_more);
        assert_eq!(
            page.rows[0].get("n.name"),
            Some(&QueryValue::Scalar(Value::String("Alice".to_string())))
        );
        assert_eq!(
            page.rows[1].get("n.name"),
            Some(&QueryValue::Scalar(Value::String("Bob".to_string())))
        );
    }

    #[test]
    fn result_modifier_parser_does_not_split_property_names() {
        let mut graph = graph();
        graph
            .apply(Command::SetNodeProperty {
                id: 1,
                key: "limit".to_string(),
                value: Value::Int(7),
            })
            .unwrap();

        let rows = CypherEngine::new()
            .execute(
                &graph,
                "MATCH (n:Person) RETURN n.limit ORDER BY n.limit DESC",
            )
            .unwrap();

        assert_eq!(
            rows[0].get("n.limit"),
            Some(&QueryValue::Scalar(Value::Int(7)))
        );
    }

    #[test]
    fn returns_distinct_projected_values() {
        let mut graph = graph();
        graph
            .apply(Command::CreateNode {
                id: 3,
                labels: vec!["Person".to_string()],
                properties: properties(&[
                    ("name", Value::String("Carol".to_string())),
                    ("status", Value::String("active".to_string())),
                ]),
            })
            .unwrap();

        let rows = CypherEngine::new()
            .execute(
                &graph,
                "MATCH (n:Person) RETURN DISTINCT n.status ORDER BY n.status ASC",
            )
            .unwrap();

        assert_eq!(rows.len(), 2);
        assert_eq!(
            rows[0].get("n.status"),
            Some(&QueryValue::Scalar(Value::Null))
        );
        assert_eq!(
            rows[1].get("n.status"),
            Some(&QueryValue::Scalar(Value::String("active".to_string())))
        );
    }

    #[test]
    fn distinct_cursor_materializes_unique_rows() {
        let mut graph = graph();
        graph
            .apply(Command::CreateNode {
                id: 3,
                labels: vec!["Person".to_string()],
                properties: properties(&[
                    ("name", Value::String("Carol".to_string())),
                    ("status", Value::String("active".to_string())),
                ]),
            })
            .unwrap();
        let mut cursor = CypherEngine::new()
            .execute_cursor(&graph, "MATCH (n:Person) RETURN DISTINCT n.status")
            .unwrap();

        assert_eq!(cursor.total_rows(), Some(2));
        let page = cursor.fetch(10);
        assert_eq!(page.rows.len(), 2);
        assert!(!page.has_more);
    }

    #[test]
    fn returns_distinct_relationship_traversal_values() {
        let mut graph = graph();
        graph
            .apply(Command::CreateRelationship {
                id: 2,
                from: 1,
                to: 2,
                rel_type: "KNOWS".to_string(),
                properties: Properties::new(),
            })
            .unwrap();

        let rows = CypherEngine::new()
            .execute(
                &graph,
                "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN DISTINCT b.name",
            )
            .unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].get("b.name"),
            Some(&QueryValue::Scalar(Value::String("Bob".to_string())))
        );
    }

    #[test]
    fn counts_matching_nodes_and_relationships() {
        let rows = CypherEngine::new()
            .execute(&graph(), "MATCH (n:Person) RETURN count(n)")
            .unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].get("count(n)"),
            Some(&QueryValue::Scalar(Value::Int(2)))
        );

        let rows = CypherEngine::new()
            .execute(
                &graph(),
                r#"MATCH (a:Person)-[r:KNOWS]->(b:Person) WHERE a.name = "Alice" RETURN COUNT(r)"#,
            )
            .unwrap();

        assert_eq!(
            rows[0].get("count(r)"),
            Some(&QueryValue::Scalar(Value::Int(1)))
        );
    }

    #[test]
    fn count_cursor_materializes_single_aggregate_row() {
        let mut cursor = CypherEngine::new()
            .execute_cursor(&graph(), "MATCH (n:Person) RETURN count(*)")
            .unwrap();

        assert_eq!(cursor.total_rows(), Some(1));
        let page = cursor.fetch(10);
        assert_eq!(page.rows.len(), 1);
        assert!(!page.has_more);
        assert_eq!(
            page.rows[0].get("count(*)"),
            Some(&QueryValue::Scalar(Value::Int(2)))
        );
    }

    #[test]
    fn groups_count_by_returned_properties() {
        let mut graph = graph();
        graph
            .apply(Command::CreateNode {
                id: 3,
                labels: vec!["Person".to_string()],
                properties: properties(&[
                    ("name", Value::String("Carol".to_string())),
                    ("status", Value::String("active".to_string())),
                ]),
            })
            .unwrap();

        let rows = CypherEngine::new()
            .execute(
                &graph,
                "MATCH (n:Person) RETURN n.status, count(n) ORDER BY count(n) DESC, n.status ASC",
            )
            .unwrap();

        assert_eq!(rows.len(), 2);
        assert_eq!(
            rows[0].get("n.status"),
            Some(&QueryValue::Scalar(Value::String("active".to_string())))
        );
        assert_eq!(
            rows[0].get("count(n)"),
            Some(&QueryValue::Scalar(Value::Int(2)))
        );
        assert_eq!(
            rows[1].get("n.status"),
            Some(&QueryValue::Scalar(Value::Null))
        );
        assert_eq!(
            rows[1].get("count(n)"),
            Some(&QueryValue::Scalar(Value::Int(1)))
        );
    }

    #[test]
    fn rejects_distinct_count_until_aggregate_distinct_is_defined() {
        let err = CypherEngine::new()
            .execute(&graph(), "MATCH (n:Person) RETURN DISTINCT count(n)")
            .unwrap_err();

        assert!(matches!(err, QueryError::Parse(_)));
    }

    #[test]
    fn rejects_unsupported_literals() {
        let err = CypherEngine::new()
            .execute(&graph(), "MATCH (n) WHERE n.name = Alice RETURN n")
            .unwrap_err();

        assert!(matches!(err, QueryError::Parse(_)));
    }

    #[test]
    fn node_cursor_fetches_rows_by_page() {
        let mut cursor = CypherEngine::new()
            .execute_cursor(&graph(), "MATCH (n:Person) RETURN n")
            .unwrap();

        assert_eq!(cursor.total_rows(), Some(2));
        let first = cursor.fetch(1);
        assert_eq!(first.rows.len(), 1);
        assert!(first.has_more);

        let second = cursor.fetch(1);
        assert_eq!(second.rows.len(), 1);
        assert!(!second.has_more);
    }

    #[test]
    fn owned_outgoing_cursor_fetches_rows_by_page() {
        let graph = Arc::new(graph());
        let mut cursor = CypherEngine::new()
            .execute_owned_cursor(
                graph,
                r#"MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE a.name = "Alice" RETURN b"#,
            )
            .unwrap();

        assert_eq!(cursor.total_rows(), None);
        let first = cursor.fetch(1);
        assert_eq!(first.rows.len(), 1);
        assert!(!first.has_more);
        assert!(matches!(first.rows[0].get("b"), Some(QueryValue::Node(node)) if node.id == 2));

        let second = cursor.fetch(1);
        assert!(second.rows.is_empty());
        assert!(!second.has_more);
    }

    #[test]
    fn vector_knn_uses_provider_when_available() {
        struct CountingProvider {
            calls: AtomicUsize,
        }

        impl VectorIndexProvider for CountingProvider {
            fn search(
                &self,
                label: Option<&str>,
                property_key: &str,
                search: &VectorSearch,
            ) -> Option<Vec<VectorHit>> {
                self.calls.fetch_add(1, Ordering::Relaxed);
                assert_eq!(label, Some("Document"));
                assert_eq!(property_key, "embedding");
                assert_eq!(search.metric, VectorMetric::Cosine);
                Some(vec![VectorHit {
                    node_id: 2,
                    score: 1.0,
                }])
            }
        }

        let provider = Arc::new(CountingProvider {
            calls: AtomicUsize::new(0),
        });
        let mut graph = GraphState::new();
        graph
            .apply(Command::CreateNode {
                id: 1,
                labels: vec!["Document".to_string()],
                properties: properties(&[
                    ("name", Value::String("fallback-near".to_string())),
                    ("embedding", Value::Vector(vec![1.0, 0.0])),
                ]),
            })
            .unwrap();
        graph
            .apply(Command::CreateNode {
                id: 2,
                labels: vec!["Document".to_string()],
                properties: properties(&[
                    ("name", Value::String("provider-hit".to_string())),
                    ("embedding", Value::Vector(vec![0.0, 1.0])),
                ]),
            })
            .unwrap();
        let engine = CypherEngine::with_vector_indexes(provider.clone());

        let rows = engine
            .execute(
                &graph,
                "MATCH (n:Document) WHERE vector.knn(n.embedding, [1.0, 0.0], 1) RETURN n.name",
            )
            .unwrap();

        assert_eq!(provider.calls.load(Ordering::Relaxed), 1);
        assert_eq!(
            rows[0].get("n.name"),
            Some(&QueryValue::Scalar(Value::String(
                "provider-hit".to_string()
            )))
        );
    }
}
