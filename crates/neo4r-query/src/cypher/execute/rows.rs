use super::*;

pub(super) fn row_if_matches(bindings: &HashMap<&str, Binding>, query: &Query) -> Option<QueryRow> {
    projected_row_if_matches(bindings, query).map(|row| row.row)
}

#[derive(Clone, Debug)]
pub(super) struct ProjectedRow {
    row: QueryRow,
    order_values: Vec<QueryValue>,
}

pub(super) fn projected_row_if_matches(
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

pub(super) fn aggregate_count_rows(rows: &[ProjectedRow], query: &Query) -> Vec<ProjectedRow> {
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

pub(super) fn distinct_rows(rows: Vec<ProjectedRow>, returns: &[ReturnItem]) -> Vec<ProjectedRow> {
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

pub(super) fn apply_result_modifiers(
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

pub(super) fn indexed_property_predicate<'a>(
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

pub(super) fn vector_knn_predicate(predicate: &Predicate) -> Option<&VectorKnnPredicate> {
    match predicate {
        Predicate::VectorKnn(predicate) => Some(predicate),
        Predicate::And(predicates) => predicates.iter().find_map(vector_knn_predicate),
        Predicate::Or(predicates) => predicates.iter().find_map(vector_knn_predicate),
        Predicate::PropertyNull(_) | Predicate::Property(_) => None,
    }
}
