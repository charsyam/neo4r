use super::write_cypher_helpers::*;
use super::write_cypher_parse::*;
use super::*;

pub(super) enum WriteCypher {
    CreateNode {
        variable: String,
        labels: Vec<String>,
        properties: Properties,
        assignments: Vec<PropertyAssignment>,
        replacement: Option<Properties>,
        returns: Option<WriteReturnItems>,
    },
    CreateRelationship {
        variable: String,
        from_matcher: NodeMatcher,
        to_matcher: NodeMatcher,
        rel_type: String,
        properties: Properties,
        assignments: Vec<PropertyAssignment>,
        replacement: Option<Properties>,
        returns: Option<WriteReturnItems>,
    },
    CreateNodeThenRelationship {
        node_variable: String,
        labels: Vec<String>,
        node_properties: Properties,
        node_assignments: Vec<PropertyAssignment>,
        node_replacement: Option<Properties>,
        matched_matcher: NodeMatcher,
        relationship_variable: String,
        created_node_is_from: bool,
        rel_type: String,
        relationship_properties: Properties,
        relationship_assignments: Vec<PropertyAssignment>,
        relationship_replacement: Option<Properties>,
        returns: Option<WriteReturnItems>,
    },
    MergeNode {
        labels: Vec<String>,
        properties: Properties,
        on_create: Vec<PropertyAssignment>,
        on_create_replacement: Option<Properties>,
        on_match: Vec<PropertyAssignment>,
        on_match_replacement: Option<Properties>,
        returns: Option<WriteReturnItems>,
    },
    MergeRelationship {
        from_matcher: NodeMatcher,
        to_matcher: NodeMatcher,
        rel_type: String,
        properties: Properties,
        on_create: Vec<PropertyAssignment>,
        on_create_replacement: Option<Properties>,
        on_match: Vec<PropertyAssignment>,
        on_match_replacement: Option<Properties>,
        returns: Option<WriteReturnItems>,
    },
    SetNodeProperty {
        matcher: NodeMatcher,
        assignments: Vec<PropertyAssignment>,
        returns: Option<WriteReturnItems>,
    },
    ReplaceNodeProperties {
        matcher: NodeMatcher,
        properties: Properties,
        returns: Option<WriteReturnItems>,
    },
    RemoveNodeProperty {
        matcher: NodeMatcher,
        keys: Vec<String>,
        returns: Option<WriteReturnItems>,
    },
    AddNodeLabel {
        matcher: NodeMatcher,
        labels: Vec<String>,
        returns: Option<WriteReturnItems>,
    },
    RemoveNodeLabel {
        matcher: NodeMatcher,
        labels: Vec<String>,
        returns: Option<WriteReturnItems>,
    },
    SetRelationshipProperty {
        matcher: RelationshipMatcher,
        assignments: Vec<PropertyAssignment>,
        returns: Option<WriteReturnItems>,
    },
    ReplaceRelationshipProperties {
        matcher: RelationshipMatcher,
        properties: Properties,
        returns: Option<WriteReturnItems>,
    },
    RemoveRelationshipProperty {
        matcher: RelationshipMatcher,
        keys: Vec<String>,
        returns: Option<WriteReturnItems>,
    },
    DeleteNode {
        matcher: NodeMatcher,
        returns: Option<WriteReturnItems>,
    },
    DeleteRelationship {
        matcher: RelationshipMatcher,
        returns: Option<WriteReturnItems>,
    },
    CreateNodePropertyIndex {
        name: String,
        label: String,
        property: String,
        if_not_exists: bool,
    },
    CreateUniqueNodePropertyConstraint {
        name: String,
        label: String,
        property: String,
        if_not_exists: bool,
    },
    CreateVectorIndex {
        name: String,
        label: String,
        property: String,
        dimensions: usize,
        metric: String,
        if_not_exists: bool,
    },
    RebuildVectorIndex {
        name: String,
    },
    DropIndex {
        name: String,
        if_exists: bool,
    },
    DropConstraint {
        name: String,
        if_exists: bool,
    },
}

pub(super) struct NodeMatcher {
    pub(super) variable: String,
    pub(super) match_query: String,
}

pub(super) struct RelationshipMatcher {
    pub(super) variable: String,
    pub(super) match_query: String,
}

#[derive(Clone, Debug, PartialEq)]
pub(super) struct PropertyAssignment {
    pub(super) key: String,
    pub(super) value: Value,
}

#[derive(Default)]
pub(super) struct MergeSetClauses {
    pub(super) on_create: Vec<PropertyAssignment>,
    pub(super) on_create_replacement: Option<Properties>,
    pub(super) on_match: Vec<PropertyAssignment>,
    pub(super) on_match_replacement: Option<Properties>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum WriteReturnItem {
    Variable(String),
    Property { variable: String, key: String },
}

pub(super) type WriteReturnItems = Vec<WriteReturnItem>;

#[derive(Clone, Debug, PartialEq)]
pub struct CreateNodeRoutingKey {
    pub labels: Vec<String>,
    pub properties: Properties,
}

pub fn create_node_routing_key(
    query: &str,
    params: &QueryParams,
) -> DatabaseResult<Option<CreateNodeRoutingKey>> {
    match parse_write_cypher(query, params)? {
        Some(WriteCypher::CreateNode {
            labels,
            properties,
            assignments,
            replacement,
            ..
        }) => {
            let properties = create_properties_after_set(properties, assignments, replacement);
            Ok(Some(CreateNodeRoutingKey { labels, properties }))
        }
        _ => Ok(None),
    }
}

pub fn merge_node_routing_key(
    query: &str,
    params: &QueryParams,
) -> DatabaseResult<Option<CreateNodeRoutingKey>> {
    match parse_write_cypher(query, params)? {
        Some(WriteCypher::MergeNode {
            labels, properties, ..
        }) => Ok(Some(CreateNodeRoutingKey { labels, properties })),
        _ => Ok(None),
    }
}

pub(super) fn parse_write_cypher(
    query: &str,
    params: &QueryParams,
) -> DatabaseResult<Option<WriteCypher>> {
    let input = query.trim();
    if input.is_empty() {
        return Ok(None);
    }
    let Some(statement_kind) = neo4r_query::classify_write_statement(input)? else {
        return Ok(None);
    };
    if starts_with_keyword(input, "CREATE VECTOR INDEX") {
        return parse_create_vector_index_ddl(input).map(Some);
    }
    if starts_with_keyword(input, "CREATE CONSTRAINT") {
        return parse_create_unique_node_property_constraint_ddl(input).map(Some);
    }
    if starts_with_keyword(input, "CREATE INDEX") {
        return parse_create_index_ddl(input).map(Some);
    }
    if starts_with_keyword(input, "DROP INDEX") {
        return parse_drop_index_ddl(input).map(Some);
    }
    if starts_with_keyword(input, "DROP CONSTRAINT") {
        return parse_drop_constraint_ddl(input).map(Some);
    }
    if starts_with_keyword(input, "REBUILD VECTOR INDEX") {
        return parse_rebuild_vector_index_ddl(input).map(Some);
    }
    if starts_with_keyword(input, "CREATE") {
        if split_keyword(input, "WITH").is_some() {
            return parse_create_node_then_relationship_write(input, params).map(Some);
        }
        return parse_create_node_write(input, params).map(Some);
    }
    if starts_with_keyword(input, "MERGE") {
        return parse_merge_node_write(input, params).map(Some);
    }
    if !matches!(
        statement_kind,
        neo4r_query::WriteStatementKind::MatchCreate
            | neo4r_query::WriteStatementKind::MatchMerge
            | neo4r_query::WriteStatementKind::MatchSet
            | neo4r_query::WriteStatementKind::MatchRemove
            | neo4r_query::WriteStatementKind::MatchDelete
    ) {
        return Ok(None);
    }
    if let Some((match_part, merge_part)) = split_keyword(input, "MERGE") {
        return parse_merge_relationship_write(match_part, merge_part, params).map(Some);
    }
    if let Some((match_part, create_part)) = split_keyword(input, "CREATE") {
        return parse_create_relationship_write(match_part, create_part, params).map(Some);
    }
    if let Some((match_part, set_part)) = split_keyword(input, "SET") {
        return parse_set_property(match_part, set_part, params).map(Some);
    }
    if let Some((match_part, remove_part)) = split_keyword(input, "REMOVE") {
        return parse_remove_property(match_part, remove_part, params).map(Some);
    }
    if let Some((match_part, delete_part)) = split_keyword(input, "DETACH DELETE") {
        return parse_delete(match_part, delete_part, params).map(Some);
    }
    if let Some((match_part, delete_part)) = split_keyword(input, "DELETE") {
        return parse_delete(match_part, delete_part, params).map(Some);
    }
    Ok(None)
}

pub(super) fn is_show_indexes_cypher(query: &str) -> bool {
    query.trim().eq_ignore_ascii_case("SHOW INDEXES")
}

pub(super) fn show_index_name(query: &str) -> DatabaseResult<Option<String>> {
    let input = query.trim();
    if !starts_with_keyword(input, "SHOW INDEX") || is_show_indexes_cypher(input) {
        return Ok(None);
    }
    let name = strip_keyword(input, "SHOW INDEX")?.trim();
    ensure_write_parse(
        !name.contains(char::is_whitespace),
        "SHOW INDEX requires a single index name",
    )?;
    validate_identifier_write(name)?;
    Ok(Some(name.to_string()))
}

pub(super) fn is_show_vector_indexes_cypher(query: &str) -> bool {
    query.trim().eq_ignore_ascii_case("SHOW VECTOR INDEXES")
}

pub(super) fn is_show_vector_index_status_cypher(query: &str) -> bool {
    query
        .trim()
        .eq_ignore_ascii_case("SHOW VECTOR INDEX STATUS")
}

pub(super) fn show_vector_index_status_name(query: &str) -> DatabaseResult<Option<String>> {
    let input = query.trim();
    if !starts_with_keyword(input, "SHOW VECTOR INDEX STATUS")
        || is_show_vector_index_status_cypher(input)
    {
        return Ok(None);
    }
    let name = strip_keyword(input, "SHOW VECTOR INDEX STATUS")?.trim();
    ensure_write_parse(
        !name.contains(char::is_whitespace),
        "SHOW VECTOR INDEX STATUS requires a single index name",
    )?;
    validate_identifier_write(name)?;
    Ok(Some(name.to_string()))
}

pub(super) fn show_vector_index_name(query: &str) -> DatabaseResult<Option<String>> {
    let input = query.trim();
    if !starts_with_keyword(input, "SHOW VECTOR INDEX")
        || is_show_vector_indexes_cypher(input)
        || starts_with_keyword(input, "SHOW VECTOR INDEX STATUS")
    {
        return Ok(None);
    }
    let name = strip_keyword(input, "SHOW VECTOR INDEX")?.trim();
    ensure_write_parse(
        !name.contains(char::is_whitespace),
        "SHOW VECTOR INDEX requires a single index name",
    )?;
    validate_identifier_write(name)?;
    Ok(Some(name.to_string()))
}

pub(super) fn is_show_constraints_cypher(query: &str) -> bool {
    query.trim().eq_ignore_ascii_case("SHOW CONSTRAINTS")
}

pub(super) fn show_constraint_name(query: &str) -> DatabaseResult<Option<String>> {
    let input = query.trim();
    if !starts_with_keyword(input, "SHOW CONSTRAINT") || is_show_constraints_cypher(input) {
        return Ok(None);
    }
    let name = strip_keyword(input, "SHOW CONSTRAINT")?.trim();
    ensure_write_parse(
        !name.contains(char::is_whitespace),
        "SHOW CONSTRAINT requires a single constraint name",
    )?;
    validate_identifier_write(name)?;
    Ok(Some(name.to_string()))
}

pub(super) fn format_index_rows(indexes: &[IndexDefinition]) -> Vec<QueryRow> {
    indexes
        .iter()
        .map(|index| {
            let mut row = QueryRow::new();
            row.insert(
                "name",
                QueryValue::Scalar(Value::String(index.name.clone())),
            );
            row.insert(
                "label",
                QueryValue::Scalar(Value::String(index.label.clone())),
            );
            row.insert(
                "property",
                QueryValue::Scalar(Value::String(index.property.clone())),
            );
            match &index.kind {
                IndexKind::NodeProperty => {
                    row.insert(
                        "type",
                        QueryValue::Scalar(Value::String("node".to_string())),
                    );
                    row.insert("dimensions", QueryValue::Scalar(Value::Null));
                    row.insert("metric", QueryValue::Scalar(Value::Null));
                }
                IndexKind::UniqueNodeProperty => {
                    row.insert(
                        "type",
                        QueryValue::Scalar(Value::String("unique_node".to_string())),
                    );
                    row.insert("dimensions", QueryValue::Scalar(Value::Null));
                    row.insert("metric", QueryValue::Scalar(Value::Null));
                }
                IndexKind::Vector { dimensions, metric } => {
                    row.insert(
                        "type",
                        QueryValue::Scalar(Value::String("vector".to_string())),
                    );
                    row.insert(
                        "dimensions",
                        QueryValue::Scalar(Value::Int(*dimensions as i64)),
                    );
                    row.insert("metric", QueryValue::Scalar(Value::String(metric.clone())));
                }
            }
            row.insert(
                "state",
                QueryValue::Scalar(Value::String("ready".to_string())),
            );
            row.insert("failure", QueryValue::Scalar(Value::String(String::new())));
            row
        })
        .collect()
}

pub(super) fn format_vector_index_rows(indexes: &[IndexDefinition]) -> Vec<QueryRow> {
    format_index_rows(
        &indexes
            .iter()
            .filter(|index| matches!(index.kind, IndexKind::Vector { .. }))
            .cloned()
            .collect::<Vec<_>>(),
    )
}

pub(super) fn format_vector_index_status_rows(statuses: &[VectorIndexStatus]) -> Vec<QueryRow> {
    statuses
        .iter()
        .map(|status| {
            let mut row = QueryRow::new();
            row.insert(
                "name",
                QueryValue::Scalar(Value::String(status.name.clone())),
            );
            row.insert(
                "label",
                QueryValue::Scalar(Value::String(status.label.clone())),
            );
            row.insert(
                "property",
                QueryValue::Scalar(Value::String(status.property.clone())),
            );
            row.insert(
                "dimensions",
                QueryValue::Scalar(Value::Int(status.dimensions as i64)),
            );
            row.insert(
                "metric",
                QueryValue::Scalar(Value::String(status.metric.clone())),
            );
            row.insert(
                "entries",
                QueryValue::Scalar(Value::Int(status.entries as i64)),
            );
            row
        })
        .collect()
}

pub(super) fn format_index_row_by_name(
    indexes: &[IndexDefinition],
    name: &str,
) -> DatabaseResult<QueryRow> {
    let Some(index) = indexes.iter().find(|index| index.name == name) else {
        return Err(DatabaseError::InvalidConfig(format!(
            "index {name:?} does not exist"
        )));
    };
    Ok(format_index_rows(std::slice::from_ref(index))
        .into_iter()
        .next()
        .expect("one index row"))
}

pub(super) fn format_vector_index_row_by_name(
    indexes: &[IndexDefinition],
    name: &str,
) -> DatabaseResult<QueryRow> {
    let Some(index) = indexes.iter().find(|index| index.name == name) else {
        return Err(DatabaseError::InvalidConfig(format!(
            "vector index {name:?} does not exist"
        )));
    };
    if !matches!(index.kind, IndexKind::Vector { .. }) {
        return Err(DatabaseError::InvalidConfig(format!(
            "index {name:?} is not a vector index"
        )));
    }
    Ok(format_index_rows(std::slice::from_ref(index))
        .into_iter()
        .next()
        .expect("one index row"))
}

pub(super) fn format_constraint_rows(indexes: &[IndexDefinition]) -> Vec<QueryRow> {
    indexes
        .iter()
        .filter(|index| matches!(index.kind, IndexKind::UniqueNodeProperty))
        .map(|index| {
            let mut row = QueryRow::new();
            row.insert(
                "name",
                QueryValue::Scalar(Value::String(index.name.clone())),
            );
            row.insert(
                "type",
                QueryValue::Scalar(Value::String("unique_node_property".to_string())),
            );
            row.insert(
                "label",
                QueryValue::Scalar(Value::String(index.label.clone())),
            );
            row.insert(
                "property",
                QueryValue::Scalar(Value::String(index.property.clone())),
            );
            row
        })
        .collect()
}

pub(super) fn format_constraint_row_by_name(
    indexes: &[IndexDefinition],
    name: &str,
) -> DatabaseResult<QueryRow> {
    let Some(index) = indexes.iter().find(|index| index.name == name) else {
        return Err(DatabaseError::InvalidConfig(format!(
            "constraint {name:?} does not exist"
        )));
    };
    if !matches!(index.kind, IndexKind::UniqueNodeProperty) {
        return Err(DatabaseError::InvalidConfig(format!(
            "index {name:?} is not a constraint"
        )));
    }
    Ok(format_constraint_rows(std::slice::from_ref(index))
        .into_iter()
        .next()
        .expect("one constraint row"))
}

pub(super) fn parse_create_index_ddl(input: &str) -> DatabaseResult<WriteCypher> {
    let body = strip_keyword(input, "CREATE INDEX")?;
    let (name, target) = split_first_token(body, "CREATE INDEX requires index name")?;
    validate_identifier_write(name)?;
    let (target, if_not_exists) = strip_if_not_exists_prefix(target);
    let (label, property) = parse_index_target(target)?;
    Ok(WriteCypher::CreateNodePropertyIndex {
        name: name.to_string(),
        label,
        property,
        if_not_exists,
    })
}

pub(super) fn parse_create_unique_node_property_constraint_ddl(
    input: &str,
) -> DatabaseResult<WriteCypher> {
    let body = strip_keyword(input, "CREATE CONSTRAINT")?;
    let (name, target) = split_first_token(body, "CREATE CONSTRAINT requires constraint name")?;
    validate_identifier_write(name)?;
    let (target, if_not_exists) = strip_if_not_exists_prefix(target);
    let (for_part, require_part) = split_keyword(target, "REQUIRE").ok_or_else(|| {
        write_parse_error("CREATE CONSTRAINT requires REQUIRE variable.property IS UNIQUE")
    })?;
    let node =
        parse_node_pattern_write(strip_keyword(for_part.trim(), "FOR")?, &QueryParams::new())?;
    ensure_write_parse(
        node.labels.len() == 1 && node.properties.is_empty(),
        "constraint target node pattern must contain one label and no properties",
    )?;
    let (property_ref, unique_part) = split_keyword(require_part, "IS")
        .ok_or_else(|| write_parse_error("CREATE CONSTRAINT requires IS UNIQUE"))?;
    ensure_write_parse(
        unique_part.trim().eq_ignore_ascii_case("UNIQUE"),
        "CREATE CONSTRAINT only supports IS UNIQUE",
    )?;
    let (variable, property) = parse_property_ref_write(property_ref)?;
    ensure_write_parse(
        variable == node.variable,
        "constraint property variable must match the target node variable",
    )?;
    Ok(WriteCypher::CreateUniqueNodePropertyConstraint {
        name: name.to_string(),
        label: node.labels[0].clone(),
        property,
        if_not_exists,
    })
}

pub(super) fn parse_create_vector_index_ddl(input: &str) -> DatabaseResult<WriteCypher> {
    let body = strip_keyword(input, "CREATE VECTOR INDEX")?;
    let (name, rest) = split_first_token(body, "CREATE VECTOR INDEX requires index name")?;
    validate_identifier_write(name)?;
    let (rest, if_not_exists) = strip_if_not_exists_prefix(rest);
    let (target, dimensions_part) = split_keyword(rest, "DIMENSIONS")
        .ok_or_else(|| write_parse_error("CREATE VECTOR INDEX requires DIMENSIONS"))?;
    let (dimensions, metric_part) = split_first_token(
        dimensions_part,
        "CREATE VECTOR INDEX requires vector dimensions",
    )?;
    let dimensions = dimensions
        .parse::<usize>()
        .map_err(|_| write_parse_error("vector dimensions must be an unsigned integer"))?;
    let metric = strip_keyword(metric_part, "METRIC")?.trim();
    validate_identifier_write(metric)?;
    let (label, property) = parse_index_target(target)?;
    Ok(WriteCypher::CreateVectorIndex {
        name: name.to_string(),
        label,
        property,
        dimensions,
        metric: metric.to_string(),
        if_not_exists,
    })
}

pub(super) fn parse_drop_index_ddl(input: &str) -> DatabaseResult<WriteCypher> {
    let body = strip_keyword(input, "DROP INDEX")?;
    let (name, if_exists) = strip_if_exists_suffix(body);
    validate_identifier_write(name)?;
    Ok(WriteCypher::DropIndex {
        name: name.to_string(),
        if_exists,
    })
}

pub(super) fn parse_drop_constraint_ddl(input: &str) -> DatabaseResult<WriteCypher> {
    let body = strip_keyword(input, "DROP CONSTRAINT")?;
    let (name, if_exists) = strip_if_exists_suffix(body);
    validate_identifier_write(name)?;
    Ok(WriteCypher::DropConstraint {
        name: name.to_string(),
        if_exists,
    })
}

pub(super) fn parse_rebuild_vector_index_ddl(input: &str) -> DatabaseResult<WriteCypher> {
    let body = strip_keyword(input, "REBUILD VECTOR INDEX")?;
    ensure_write_parse(
        !body.contains(char::is_whitespace),
        "REBUILD VECTOR INDEX requires a single index name",
    )?;
    validate_identifier_write(body)?;
    Ok(WriteCypher::RebuildVectorIndex {
        name: body.to_string(),
    })
}

pub(super) fn strip_if_exists_suffix(input: &str) -> (&str, bool) {
    let input = input.trim();
    match strip_keyword_suffix(input, "IF EXISTS") {
        Some(name) => (name.trim(), true),
        None => (input, false),
    }
}

pub(super) fn strip_if_not_exists_prefix(input: &str) -> (&str, bool) {
    match strip_keyword(input.trim(), "IF NOT EXISTS") {
        Ok(rest) => (rest.trim(), true),
        Err(_) => (input.trim(), false),
    }
}

pub(super) fn split_first_token<'a>(
    input: &'a str,
    missing: &str,
) -> DatabaseResult<(&'a str, &'a str)> {
    let input = input.trim();
    let Some(index) = input.find(char::is_whitespace) else {
        return Err(write_parse_error(missing));
    };
    let head = input[..index].trim();
    let rest = input[index..].trim();
    ensure_write_parse(!head.is_empty() && !rest.is_empty(), missing)?;
    Ok((head, rest))
}

pub(super) fn parse_index_target(input: &str) -> DatabaseResult<(String, String)> {
    let input = input.trim();
    if starts_with_keyword(input, "FOR") {
        return parse_for_on_index_target(input);
    }
    let target = strip_keyword(input, "ON")?;
    parse_legacy_index_target(target)
}

pub(super) fn parse_for_on_index_target(input: &str) -> DatabaseResult<(String, String)> {
    let body = strip_keyword(input, "FOR")?;
    let (pattern, on_part) =
        split_keyword(body, "ON").ok_or_else(|| write_parse_error("index target requires ON"))?;
    let node = parse_node_pattern_write(pattern.trim(), &QueryParams::new())?;
    ensure_write_parse(
        node.labels.len() == 1 && node.properties.is_empty(),
        "index target node pattern must contain one label and no properties",
    )?;
    let property_ref = strip_wrapping_write(on_part.trim(), '(', ')')?;
    let (variable, property) = parse_property_ref_write(property_ref)?;
    ensure_write_parse(
        variable == node.variable,
        "index property variable must match the indexed node variable",
    )?;
    Ok((node.labels[0].clone(), property))
}

pub(super) fn parse_legacy_index_target(input: &str) -> DatabaseResult<(String, String)> {
    let input = input.trim();
    let label_start = input
        .strip_prefix(':')
        .ok_or_else(|| write_parse_error("index target must start with :Label(property)"))?;
    let open = label_start
        .find('(')
        .ok_or_else(|| write_parse_error("index target requires property parentheses"))?;
    let close = label_start
        .rfind(')')
        .ok_or_else(|| write_parse_error("index target requires property parentheses"))?;
    ensure_write_parse(
        close == label_start.len() - 1 && open < close,
        "index target must end after property parentheses",
    )?;
    let label = label_start[..open].trim();
    let property = label_start[open + 1..close].trim();
    validate_identifier_write(label)?;
    validate_identifier_write(property)?;
    Ok((label.to_string(), property.to_string()))
}
