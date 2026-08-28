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

fn execute_physical_query<G: GraphRead + ?Sized>(
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
