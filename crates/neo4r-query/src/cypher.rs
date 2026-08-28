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

    pub fn parse(&self, query: &str, params: &QueryParams) -> QueryResult<ParsedCypherQuery> {
        parse(query, params)
    }

    pub fn analyze(&self, query: &str, params: &QueryParams) -> QueryResult<SemanticCypherQuery> {
        let parsed = self.parse(query, params)?;
        analyze_query(parsed)
    }

    pub fn logical_plan(&self, query: &str, params: &QueryParams) -> QueryResult<LogicalPlan> {
        let semantic = self.analyze(query, params)?;
        Ok(LogicalPlan {
            root: build_logical_plan(&semantic.query),
        })
    }

    pub fn physical_plan(&self, query: &str, params: &QueryParams) -> QueryResult<PhysicalPlan> {
        let semantic = self.analyze(query, params)?;
        Ok(PhysicalPlan {
            root: build_physical_plan(&semantic.query),
        })
    }

    pub fn explain(&self, query: &str, params: &QueryParams) -> QueryResult<CypherPlan> {
        let semantic = self.analyze(query, params)?;
        Ok(CypherPlan {
            logical: LogicalPlan {
                root: build_logical_plan(&semantic.query),
            },
            physical: PhysicalPlan {
                root: build_physical_plan(&semantic.query),
            },
        })
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
        let query = prepare_query(query, params)?;
        let physical = PhysicalPlan {
            root: build_physical_plan(&query),
        };
        execute_physical_query(graph, &query, &physical, self.vector_indexes.as_deref())
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
        let query = prepare_query(query, params)?;
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
        let query = prepare_query(query, &params)?;
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

pub type ParsedCypherQuery = Query;

#[derive(Clone, Debug, PartialEq)]
pub struct Query {
    pub pattern: Pattern,
    pub predicate: Option<Predicate>,
    pub returns: Vec<ReturnItem>,
    pub distinct: bool,
    pub modifiers: ResultModifiers,
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
pub enum Pattern {
    Node(NodePattern),
    Outgoing {
        from: NodePattern,
        rel_variable: Option<String>,
        rel_type: Option<String>,
        to: NodePattern,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub struct NodePattern {
    pub variable: String,
    pub label: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Predicate {
    Property(PropertyPredicate),
    PropertyNull(PropertyNullPredicate),
    VectorKnn(VectorKnnPredicate),
    And(Vec<Predicate>),
    Or(Vec<Predicate>),
}

#[derive(Clone, Debug, PartialEq)]
pub struct PropertyPredicate {
    pub variable: String,
    pub key: String,
    pub operator: ComparisonOperator,
    pub value: Value,
}

#[derive(Clone, Debug, PartialEq)]
pub struct PropertyNullPredicate {
    pub variable: String,
    pub key: String,
    pub negated: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ComparisonOperator {
    Equal,
    NotEqual,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
}

#[derive(Clone, Debug, PartialEq)]
pub struct VectorKnnPredicate {
    pub variable: String,
    pub key: String,
    pub query: Vec<f32>,
    pub k: usize,
    pub metric: VectorMetric,
}

#[derive(Clone, Debug, PartialEq)]
pub enum ReturnItem {
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
pub enum CountTarget {
    All,
    Variable(String),
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct ResultModifiers {
    pub order_by: Vec<OrderItem>,
    pub skip: Option<usize>,
    pub limit: Option<usize>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct OrderItem {
    pub item: ReturnItem,
    pub direction: SortDirection,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SortDirection {
    Asc,
    Desc,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SemanticCypherQuery {
    pub query: ParsedCypherQuery,
    pub bound_variables: Vec<VariableBinding>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VariableBinding {
    pub name: String,
    pub kind: VariableKind,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum VariableKind {
    Node,
    Relationship,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CypherPlan {
    pub logical: LogicalPlan,
    pub physical: PhysicalPlan,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogicalPlan {
    pub root: LogicalOperator,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LogicalOperator {
    Project {
        items: Vec<String>,
        input: Box<LogicalOperator>,
    },
    Aggregate {
        items: Vec<String>,
        input: Box<LogicalOperator>,
    },
    Distinct {
        input: Box<LogicalOperator>,
    },
    Sort {
        items: Vec<String>,
        input: Box<LogicalOperator>,
    },
    Skip {
        rows: usize,
        input: Box<LogicalOperator>,
    },
    Limit {
        rows: usize,
        input: Box<LogicalOperator>,
    },
    Filter {
        predicate: String,
        input: Box<LogicalOperator>,
    },
    Expand {
        from: String,
        relationship: Option<String>,
        relationship_type: Option<String>,
        to: String,
        input: Box<LogicalOperator>,
    },
    NodeScan {
        variable: String,
        label: Option<String>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PhysicalPlan {
    pub root: PhysicalOperator,
}

impl PhysicalPlan {
    pub fn operator_count(&self) -> usize {
        physical_operator_count(&self.root)
    }

    pub fn operator_names(&self) -> Vec<&'static str> {
        let mut names = Vec::new();
        collect_physical_operator_names(&self.root, &mut names);
        names
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PhysicalOperator {
    Materialize {
        input: Box<PhysicalOperator>,
    },
    Project {
        items: Vec<String>,
        input: Box<PhysicalOperator>,
    },
    HashAggregate {
        items: Vec<String>,
        input: Box<PhysicalOperator>,
    },
    HashDistinct {
        input: Box<PhysicalOperator>,
    },
    Sort {
        items: Vec<String>,
        input: Box<PhysicalOperator>,
    },
    Offset {
        rows: usize,
        input: Box<PhysicalOperator>,
    },
    Top {
        rows: usize,
        input: Box<PhysicalOperator>,
    },
    PredicateFilter {
        predicate: String,
        input: Box<PhysicalOperator>,
    },
    ExpandOutgoing {
        from: String,
        relationship: Option<String>,
        relationship_type: Option<String>,
        to: String,
        input: Box<PhysicalOperator>,
    },
    NodeByLabelScan {
        variable: String,
        label: String,
    },
    AllNodesScan {
        variable: String,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CypherStatementKind {
    Read,
    Write(WriteStatementKind),
    Ddl(WriteStatementKind),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WriteStatementKind {
    Create,
    CreateIndex,
    CreateConstraint,
    CreateVectorIndex,
    DropIndex,
    DropConstraint,
    RebuildVectorIndex,
    Merge,
    MatchCreate,
    MatchMerge,
    MatchSet,
    MatchRemove,
    MatchDelete,
}

pub fn classify_statement(input: &str) -> QueryResult<Option<CypherStatementKind>> {
    let input = input.trim();
    if input.is_empty() {
        return Ok(None);
    }
    if starts_with_keyword(input, "CREATE VECTOR INDEX") {
        return Ok(Some(CypherStatementKind::Ddl(
            WriteStatementKind::CreateVectorIndex,
        )));
    }
    if starts_with_keyword(input, "CREATE CONSTRAINT") {
        return Ok(Some(CypherStatementKind::Ddl(
            WriteStatementKind::CreateConstraint,
        )));
    }
    if starts_with_keyword(input, "CREATE INDEX") {
        return Ok(Some(CypherStatementKind::Ddl(
            WriteStatementKind::CreateIndex,
        )));
    }
    if starts_with_keyword(input, "DROP INDEX") {
        return Ok(Some(CypherStatementKind::Ddl(
            WriteStatementKind::DropIndex,
        )));
    }
    if starts_with_keyword(input, "DROP CONSTRAINT") {
        return Ok(Some(CypherStatementKind::Ddl(
            WriteStatementKind::DropConstraint,
        )));
    }
    if starts_with_keyword(input, "REBUILD VECTOR INDEX") {
        return Ok(Some(CypherStatementKind::Ddl(
            WriteStatementKind::RebuildVectorIndex,
        )));
    }
    if starts_with_keyword(input, "CREATE") {
        return Ok(Some(CypherStatementKind::Write(WriteStatementKind::Create)));
    }
    if starts_with_keyword(input, "MERGE") {
        return Ok(Some(CypherStatementKind::Write(WriteStatementKind::Merge)));
    }
    if starts_with_keyword(input, "MATCH") {
        let kind = if find_keyword(input, "MERGE").is_some() {
            Some(WriteStatementKind::MatchMerge)
        } else if find_keyword(input, "CREATE").is_some() {
            Some(WriteStatementKind::MatchCreate)
        } else if find_keyword(input, "SET").is_some() {
            Some(WriteStatementKind::MatchSet)
        } else if find_keyword(input, "REMOVE").is_some() {
            Some(WriteStatementKind::MatchRemove)
        } else if find_keyword(input, "DETACH DELETE").is_some()
            || find_keyword(input, "DELETE").is_some()
        {
            Some(WriteStatementKind::MatchDelete)
        } else if find_keyword(input, "RETURN").is_some() {
            return Ok(Some(CypherStatementKind::Read));
        } else {
            None
        };
        return Ok(kind.map(|kind| CypherStatementKind::Write(kind)));
    }
    Ok(None)
}

pub fn classify_write_statement(input: &str) -> QueryResult<Option<WriteStatementKind>> {
    match classify_statement(input)? {
        Some(CypherStatementKind::Write(kind) | CypherStatementKind::Ddl(kind)) => Ok(Some(kind)),
        Some(CypherStatementKind::Read) | None => Ok(None),
    }
}

fn prepare_query(query: &str, params: &QueryParams) -> QueryResult<Query> {
    analyze_query(parse(query, params)?).map(|semantic| semantic.query)
}

fn analyze_query(query: ParsedCypherQuery) -> QueryResult<SemanticCypherQuery> {
    let bindings = bound_variables_for_pattern(&query.pattern)?;
    validate_predicate_variables(query.predicate.as_ref(), &bindings)?;
    validate_return_variables(&query.returns, &bindings)?;
    let order_returns = query
        .modifiers
        .order_by
        .iter()
        .map(|item| item.item.clone())
        .collect::<Vec<_>>();
    validate_return_variables(&order_returns, &bindings)?;
    Ok(SemanticCypherQuery {
        query,
        bound_variables: bindings,
    })
}

fn bound_variables_for_pattern(pattern: &Pattern) -> QueryResult<Vec<VariableBinding>> {
    let mut bindings = Vec::new();
    match pattern {
        Pattern::Node(pattern) => {
            push_binding(&mut bindings, &pattern.variable, VariableKind::Node)?
        }
        Pattern::Outgoing {
            from,
            rel_variable,
            to,
            ..
        } => {
            push_binding(&mut bindings, &from.variable, VariableKind::Node)?;
            if let Some(variable) = rel_variable {
                push_binding(&mut bindings, variable, VariableKind::Relationship)?;
            }
            push_binding(&mut bindings, &to.variable, VariableKind::Node)?;
        }
    }
    Ok(bindings)
}

fn push_binding(
    bindings: &mut Vec<VariableBinding>,
    name: &str,
    kind: VariableKind,
) -> QueryResult<()> {
    if let Some(existing) = bindings.iter().find(|binding| binding.name == name) {
        if existing.kind == kind {
            return Err(QueryError::Parse(format!(
                "variable {name:?} is bound more than once"
            )));
        }
        return Err(QueryError::Parse(format!(
            "variable {name:?} is bound as both {:?} and {:?}",
            existing.kind, kind
        )));
    }
    bindings.push(VariableBinding {
        name: name.to_string(),
        kind,
    });
    Ok(())
}

fn validate_predicate_variables(
    predicate: Option<&Predicate>,
    bindings: &[VariableBinding],
) -> QueryResult<()> {
    let Some(predicate) = predicate else {
        return Ok(());
    };
    for (variable, expected) in predicate_variables(predicate) {
        validate_bound_variable(&variable, expected, bindings)?;
    }
    Ok(())
}

fn predicate_variables(predicate: &Predicate) -> Vec<(String, Option<VariableKind>)> {
    match predicate {
        Predicate::Property(predicate) => vec![(predicate.variable.clone(), None)],
        Predicate::PropertyNull(predicate) => vec![(predicate.variable.clone(), None)],
        Predicate::VectorKnn(predicate) => {
            vec![(predicate.variable.clone(), Some(VariableKind::Node))]
        }
        Predicate::And(predicates) | Predicate::Or(predicates) => {
            predicates.iter().flat_map(predicate_variables).collect()
        }
    }
}

fn validate_return_variables(
    returns: &[ReturnItem],
    bindings: &[VariableBinding],
) -> QueryResult<()> {
    for item in returns {
        match item {
            ReturnItem::Variable(variable) | ReturnItem::Count(CountTarget::Variable(variable)) => {
                validate_bound_variable(variable, None, bindings)?;
            }
            ReturnItem::Property { variable, .. } => {
                validate_bound_variable(variable, None, bindings)?;
            }
            ReturnItem::Count(CountTarget::All) => {}
        }
    }
    Ok(())
}

fn validate_bound_variable(
    variable: &str,
    expected: Option<VariableKind>,
    bindings: &[VariableBinding],
) -> QueryResult<()> {
    let Some(binding) = bindings.iter().find(|binding| binding.name == variable) else {
        return Err(QueryError::Parse(format!(
            "variable {variable:?} is not bound by MATCH"
        )));
    };
    if let Some(expected) = expected {
        if binding.kind != expected {
            return Err(QueryError::Parse(format!(
                "variable {variable:?} must be a {:?}",
                expected
            )));
        }
    }
    Ok(())
}

fn build_logical_plan(query: &Query) -> LogicalOperator {
    let mut root = match &query.pattern {
        Pattern::Node(pattern) => LogicalOperator::NodeScan {
            variable: pattern.variable.clone(),
            label: pattern.label.clone(),
        },
        Pattern::Outgoing {
            from,
            rel_variable,
            rel_type,
            to,
        } => LogicalOperator::Expand {
            from: from.variable.clone(),
            relationship: rel_variable.clone(),
            relationship_type: rel_type.clone(),
            to: to.variable.clone(),
            input: Box::new(LogicalOperator::NodeScan {
                variable: from.variable.clone(),
                label: from.label.clone(),
            }),
        },
    };
    if let Some(predicate) = &query.predicate {
        root = LogicalOperator::Filter {
            predicate: format_predicate(predicate),
            input: Box::new(root),
        };
    }
    if query.has_count_aggregate() {
        root = LogicalOperator::Aggregate {
            items: query.returns.iter().map(ReturnItem::name).collect(),
            input: Box::new(root),
        };
    } else {
        root = LogicalOperator::Project {
            items: query.returns.iter().map(ReturnItem::name).collect(),
            input: Box::new(root),
        };
    }
    if query.distinct {
        root = LogicalOperator::Distinct {
            input: Box::new(root),
        };
    }
    if !query.modifiers.order_by.is_empty() {
        root = LogicalOperator::Sort {
            items: query
                .modifiers
                .order_by
                .iter()
                .map(format_order_item)
                .collect(),
            input: Box::new(root),
        };
    }
    if let Some(skip) = query.modifiers.skip {
        root = LogicalOperator::Skip {
            rows: skip,
            input: Box::new(root),
        };
    }
    if let Some(limit) = query.modifiers.limit {
        root = LogicalOperator::Limit {
            rows: limit,
            input: Box::new(root),
        };
    }
    root
}

fn build_physical_plan(query: &Query) -> PhysicalOperator {
    let mut root = match &query.pattern {
        Pattern::Node(pattern) => match &pattern.label {
            Some(label) => PhysicalOperator::NodeByLabelScan {
                variable: pattern.variable.clone(),
                label: label.clone(),
            },
            None => PhysicalOperator::AllNodesScan {
                variable: pattern.variable.clone(),
            },
        },
        Pattern::Outgoing {
            from,
            rel_variable,
            rel_type,
            to,
        } => {
            let input = match &from.label {
                Some(label) => PhysicalOperator::NodeByLabelScan {
                    variable: from.variable.clone(),
                    label: label.clone(),
                },
                None => PhysicalOperator::AllNodesScan {
                    variable: from.variable.clone(),
                },
            };
            PhysicalOperator::ExpandOutgoing {
                from: from.variable.clone(),
                relationship: rel_variable.clone(),
                relationship_type: rel_type.clone(),
                to: to.variable.clone(),
                input: Box::new(input),
            }
        }
    };
    if let Some(predicate) = &query.predicate {
        root = PhysicalOperator::PredicateFilter {
            predicate: format_predicate(predicate),
            input: Box::new(root),
        };
    }
    if query.has_count_aggregate() {
        root = PhysicalOperator::HashAggregate {
            items: query.returns.iter().map(ReturnItem::name).collect(),
            input: Box::new(root),
        };
    } else {
        root = PhysicalOperator::Project {
            items: query.returns.iter().map(ReturnItem::name).collect(),
            input: Box::new(root),
        };
    }
    if query.distinct {
        root = PhysicalOperator::HashDistinct {
            input: Box::new(root),
        };
    }
    if !query.modifiers.order_by.is_empty() {
        root = PhysicalOperator::Sort {
            items: query
                .modifiers
                .order_by
                .iter()
                .map(format_order_item)
                .collect(),
            input: Box::new(root),
        };
    }
    if let Some(skip) = query.modifiers.skip {
        root = PhysicalOperator::Offset {
            rows: skip,
            input: Box::new(root),
        };
    }
    if let Some(limit) = query.modifiers.limit {
        root = PhysicalOperator::Top {
            rows: limit,
            input: Box::new(root),
        };
    }
    PhysicalOperator::Materialize {
        input: Box::new(root),
    }
}

fn physical_operator_count(operator: &PhysicalOperator) -> usize {
    1 + match physical_operator_input(operator) {
        Some(input) => physical_operator_count(input),
        None => 0,
    }
}

fn collect_physical_operator_names(operator: &PhysicalOperator, names: &mut Vec<&'static str>) {
    names.push(physical_operator_name(operator));
    if let Some(input) = physical_operator_input(operator) {
        collect_physical_operator_names(input, names);
    }
}

fn physical_operator_name(operator: &PhysicalOperator) -> &'static str {
    match operator {
        PhysicalOperator::Materialize { .. } => "Materialize",
        PhysicalOperator::Project { .. } => "Project",
        PhysicalOperator::HashAggregate { .. } => "HashAggregate",
        PhysicalOperator::HashDistinct { .. } => "HashDistinct",
        PhysicalOperator::Sort { .. } => "Sort",
        PhysicalOperator::Offset { .. } => "Offset",
        PhysicalOperator::Top { .. } => "Top",
        PhysicalOperator::PredicateFilter { .. } => "PredicateFilter",
        PhysicalOperator::ExpandOutgoing { .. } => "ExpandOutgoing",
        PhysicalOperator::NodeByLabelScan { .. } => "NodeByLabelScan",
        PhysicalOperator::AllNodesScan { .. } => "AllNodesScan",
    }
}

fn physical_operator_input(operator: &PhysicalOperator) -> Option<&PhysicalOperator> {
    match operator {
        PhysicalOperator::Materialize { input }
        | PhysicalOperator::Project { input, .. }
        | PhysicalOperator::HashAggregate { input, .. }
        | PhysicalOperator::HashDistinct { input }
        | PhysicalOperator::Sort { input, .. }
        | PhysicalOperator::Offset { input, .. }
        | PhysicalOperator::Top { input, .. }
        | PhysicalOperator::PredicateFilter { input, .. }
        | PhysicalOperator::ExpandOutgoing { input, .. } => Some(input),
        PhysicalOperator::NodeByLabelScan { .. } | PhysicalOperator::AllNodesScan { .. } => None,
    }
}

fn format_order_item(item: &OrderItem) -> String {
    let direction = match item.direction {
        SortDirection::Asc => "ASC",
        SortDirection::Desc => "DESC",
    };
    format!("{} {direction}", item.item.name())
}

fn format_predicate(predicate: &Predicate) -> String {
    match predicate {
        Predicate::Property(predicate) => {
            format!(
                "{}.{} {} {}",
                predicate.variable,
                predicate.key,
                format_comparison_operator(predicate.operator),
                format_value(&predicate.value)
            )
        }
        Predicate::PropertyNull(predicate) if predicate.negated => {
            format!("{}.{} IS NOT NULL", predicate.variable, predicate.key)
        }
        Predicate::PropertyNull(predicate) => {
            format!("{}.{} IS NULL", predicate.variable, predicate.key)
        }
        Predicate::VectorKnn(predicate) => {
            format!(
                "vector.knn({}.{}, [{}], {}, {})",
                predicate.variable,
                predicate.key,
                predicate
                    .query
                    .iter()
                    .map(|value| value.to_string())
                    .collect::<Vec<_>>()
                    .join(", "),
                predicate.k,
                format_vector_metric(predicate.metric)
            )
        }
        Predicate::And(predicates) => predicates
            .iter()
            .map(format_predicate)
            .collect::<Vec<_>>()
            .join(" AND "),
        Predicate::Or(predicates) => predicates
            .iter()
            .map(format_predicate)
            .collect::<Vec<_>>()
            .join(" OR "),
    }
}

fn format_comparison_operator(operator: ComparisonOperator) -> &'static str {
    match operator {
        ComparisonOperator::Equal => "=",
        ComparisonOperator::NotEqual => "<>",
        ComparisonOperator::LessThan => "<",
        ComparisonOperator::LessThanOrEqual => "<=",
        ComparisonOperator::GreaterThan => ">",
        ComparisonOperator::GreaterThanOrEqual => ">=",
    }
}

fn format_vector_metric(metric: VectorMetric) -> &'static str {
    match metric {
        VectorMetric::Cosine => "cosine",
        VectorMetric::L2 => "l2",
    }
}

fn format_value(value: &Value) -> String {
    match value {
        Value::Null => "null".to_string(),
        Value::Bool(value) => value.to_string(),
        Value::Int(value) => value.to_string(),
        Value::Float(value) => value.to_string(),
        Value::String(value) => format!("{value:?}"),
        Value::Vector(values) => format!(
            "[{}]",
            values
                .iter()
                .map(|value| value.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        ),
        Value::Map(_) => "<map>".to_string(),
    }
}

include!("cypher/execute.rs");
include!("cypher/binding.rs");
include!("cypher/parse.rs");

#[cfg(test)]
mod tests;
