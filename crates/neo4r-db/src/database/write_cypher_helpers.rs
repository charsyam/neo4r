use super::write_cypher_model::*;
use super::*;

mod mutation;
pub(super) use mutation::*;

pub(super) struct NodePatternWrite {
    pub(super) variable: String,
    pub(super) labels: Vec<String>,
    pub(super) properties: Properties,
}

pub(super) struct RelationshipPatternWrite {
    pub(super) variable: String,
    pub(super) from_variable: String,
    pub(super) to_variable: String,
    pub(super) rel_type: String,
    pub(super) properties: Properties,
}

pub(super) fn parse_node_pattern_write(
    input: &str,
    params: &QueryParams,
) -> DatabaseResult<NodePatternWrite> {
    let inner = strip_wrapping_write(input.trim(), '(', ')')?;
    let (head, properties) = match top_level_brace_start(inner) {
        Some(index) => {
            let head = inner[..index].trim();
            let properties = parse_property_map(&inner[index..], params)?;
            (head, properties)
        }
        None => (inner.trim(), Properties::new()),
    };
    let mut parts = head.split(':').map(str::trim);
    let variable = parts
        .next()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| write_parse_error("node pattern requires a variable"))?;
    validate_identifier_write(variable)?;
    let labels = parts
        .map(|label| {
            validate_identifier_write(label)?;
            Ok(label.to_string())
        })
        .collect::<DatabaseResult<Vec<_>>>()?;
    Ok(NodePatternWrite {
        variable: variable.to_string(),
        labels,
        properties,
    })
}

pub(super) fn parse_relationship_pattern_write(
    input: &str,
    params: &QueryParams,
) -> DatabaseResult<RelationshipPatternWrite> {
    let compact = input
        .chars()
        .filter(|ch| !ch.is_whitespace())
        .collect::<String>();
    let (left, to_part) = compact
        .split_once("->")
        .ok_or_else(|| write_parse_error("relationship pattern must use ->"))?;
    let (from_part, rel_part) = left
        .split_once("-")
        .ok_or_else(|| write_parse_error("relationship pattern must contain -[r:TYPE]->"))?;
    let from_variable = parse_node_pattern_write(from_part, &QueryParams::new())?.variable;
    let to_variable = parse_node_pattern_write(to_part, &QueryParams::new())?.variable;
    let inner = strip_wrapping_write(rel_part, '[', ']')?;
    let (head, properties) = match top_level_brace_start(inner) {
        Some(index) => {
            let head = inner[..index].trim();
            let properties = parse_property_map(&inner[index..], params)?;
            (head, properties)
        }
        None => (inner.trim(), Properties::new()),
    };
    let (variable, rel_type) = head
        .split_once(':')
        .ok_or_else(|| write_parse_error("relationship pattern requires variable:type or :type"))?;
    if !variable.is_empty() {
        validate_identifier_write(variable)?;
    }
    validate_identifier_write(rel_type)?;
    Ok(RelationshipPatternWrite {
        variable: variable.to_string(),
        from_variable,
        to_variable,
        rel_type: rel_type.to_string(),
        properties,
    })
}

pub(super) fn parse_property_map(input: &str, params: &QueryParams) -> DatabaseResult<Properties> {
    if let Some(name) = input.trim().strip_prefix('$') {
        validate_identifier_write(name)?;
        return match params.get(name) {
            Some(Value::Map(properties)) => {
                validate_property_map_values(properties)?;
                Ok(properties.clone())
            }
            Some(value) => Err(write_parse_error(&format!(
                "query parameter ${name} must be a property map, got {value:?}"
            ))),
            None => Err(write_parse_error(&format!(
                "missing query parameter ${name}"
            ))),
        };
    }
    let inner = strip_wrapping_write(input.trim(), '{', '}')?;
    if inner.trim().is_empty() {
        return Ok(Properties::new());
    }
    let mut properties = Properties::new();
    for entry in split_top_level_commas(inner)? {
        let (key, value) = entry
            .split_once(':')
            .ok_or_else(|| write_parse_error("property map entries must use key: value"))?;
        let key = key.trim();
        validate_identifier_write(key)?;
        properties.insert(
            key.to_string(),
            parse_write_property_value(value.trim(), params)?,
        );
    }
    Ok(properties)
}

pub(super) fn parse_write_property_value(
    input: &str,
    params: &QueryParams,
) -> DatabaseResult<Value> {
    let value = parse_write_value(input, params)?;
    ensure_storable_property_value(&value)?;
    Ok(value)
}

pub(super) fn validate_property_map_values(properties: &Properties) -> DatabaseResult<()> {
    for value in properties.values() {
        ensure_storable_property_value(value)?;
    }
    Ok(())
}

pub(super) fn ensure_storable_property_value(value: &Value) -> DatabaseResult<()> {
    ensure_write_parse(
        !matches!(value, Value::Map(_)),
        "graph properties do not support nested map values",
    )
}

pub(super) fn validate_storable_properties(properties: &Properties) -> DatabaseResult<()> {
    for value in properties.values() {
        validate_storable_property_value(value)?;
    }
    Ok(())
}

pub(super) fn validate_storable_property_value(value: &Value) -> DatabaseResult<()> {
    if matches!(value, Value::Map(_)) {
        return Err(DatabaseError::InvalidConfig(
            "graph properties do not support nested map values".to_string(),
        ));
    }
    Ok(())
}

pub(super) fn parse_write_value(input: &str, params: &QueryParams) -> DatabaseResult<Value> {
    if let Some(name) = input.strip_prefix('$') {
        validate_identifier_write(name)?;
        return params
            .get(name)
            .cloned()
            .ok_or_else(|| write_parse_error(&format!("missing query parameter ${name}")));
    }
    if input.starts_with('[') {
        return parse_vector_value(input);
    }
    if let Some(value) = input
        .strip_prefix('"')
        .and_then(|value| value.strip_suffix('"'))
    {
        return Ok(Value::String(value.to_string()));
    }
    if input.eq_ignore_ascii_case("null") {
        return Ok(Value::Null);
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
    Err(write_parse_error(&format!("unsupported literal {input:?}")))
}

pub(super) fn parse_vector_value(input: &str) -> DatabaseResult<Value> {
    let inner = strip_wrapping_write(input.trim(), '[', ']')?;
    if inner.trim().is_empty() {
        return Err(write_parse_error(
            "vector literal must contain at least one value",
        ));
    }
    let vector = inner
        .split(',')
        .map(|item| {
            item.trim().parse::<f32>().map_err(|_| {
                write_parse_error(&format!("invalid vector element {:?}", item.trim()))
            })
        })
        .collect::<DatabaseResult<Vec<_>>>()?;
    Ok(Value::Vector(vector))
}

pub(super) fn parse_property_ref_write(input: &str) -> DatabaseResult<(String, String)> {
    let (variable, key) = input
        .split_once('.')
        .ok_or_else(|| write_parse_error("property reference must use variable.property"))?;
    validate_identifier_write(variable.trim())?;
    validate_identifier_write(key.trim())?;
    Ok((variable.trim().to_string(), key.trim().to_string()))
}

pub(super) fn parse_return_variable(input: &str) -> DatabaseResult<String> {
    let variable = input.trim();
    validate_identifier_write(variable)?;
    Ok(variable.to_string())
}

pub(super) fn parse_optional_write_return<'a>(
    input: &'a str,
) -> DatabaseResult<(&'a str, Option<WriteReturnItems>)> {
    match split_keyword(input, "RETURN") {
        Some((body, returns)) => Ok((body.trim(), Some(parse_write_return_items(returns)?))),
        None => Ok((input.trim(), None)),
    }
}

pub(super) fn parse_write_return_items(input: &str) -> DatabaseResult<WriteReturnItems> {
    let mut items = Vec::new();
    for item in split_top_level_commas(input.trim())? {
        items.push(parse_write_return_item(item)?);
    }
    ensure_write_parse(!items.is_empty(), "write RETURN requires at least one item")?;
    Ok(items)
}

pub(super) fn parse_write_return_item(input: &str) -> DatabaseResult<WriteReturnItem> {
    let input = input.trim();
    if let Some((variable, key)) = input.split_once('.') {
        validate_identifier_write(variable.trim())?;
        validate_identifier_write(key.trim())?;
        Ok(WriteReturnItem::Property {
            variable: variable.trim().to_string(),
            key: key.trim().to_string(),
        })
    } else {
        validate_identifier_write(input)?;
        Ok(WriteReturnItem::Variable(input.to_string()))
    }
}

pub(super) fn ensure_write_return_matches(
    returns: Option<&WriteReturnItems>,
    expected_variable: &str,
    context: &str,
) -> DatabaseResult<()> {
    let Some(returns) = returns else {
        return Ok(());
    };
    for item in returns {
        let variable = match item {
            WriteReturnItem::Variable(variable) => variable,
            WriteReturnItem::Property { variable, .. } => variable,
        };
        ensure_write_parse(
            variable == expected_variable,
            &format!("{context} variable must match the MATCH variable"),
        )?;
    }
    Ok(())
}

pub(super) fn ensure_write_return_variables(
    returns: Option<&WriteReturnItems>,
    allowed_variables: &[&str],
    context: &str,
) -> DatabaseResult<()> {
    let Some(returns) = returns else {
        return Ok(());
    };
    for item in returns {
        let variable = match item {
            WriteReturnItem::Variable(variable) => variable,
            WriteReturnItem::Property { variable, .. } => variable,
        };
        ensure_write_parse(
            allowed_variables.iter().any(|allowed| allowed == variable),
            &format!("{context} variable must be produced by the query"),
        )?;
    }
    Ok(())
}

pub(super) fn query_match_node_ids(
    run_query: impl FnOnce(&str) -> DatabaseResult<Vec<QueryRow>>,
    matcher: &NodeMatcher,
) -> DatabaseResult<Vec<NodeId>> {
    let rows = run_query(&matcher.match_query)?;
    rows.into_iter()
        .map(|row| match row.get(&matcher.variable) {
            Some(QueryValue::Node(node)) => Ok(node.id),
            Some(QueryValue::BoundaryNode(node)) => Err(DatabaseError::Replication(format!(
                "write target node {} is a boundary cache node",
                node.id
            ))),
            Some(value) => Err(write_parse_error(&format!(
                "MATCH returned non-node value {value:?}"
            ))),
            None => Err(write_parse_error(
                "MATCH did not return the target variable",
            )),
        })
        .collect()
}

pub(super) fn query_match_relationship_ids(
    run_query: impl FnOnce(&str) -> DatabaseResult<Vec<QueryRow>>,
    matcher: &RelationshipMatcher,
) -> DatabaseResult<Vec<RelationshipId>> {
    let rows = run_query(&matcher.match_query)?;
    rows.into_iter()
        .map(|row| match row.get(&matcher.variable) {
            Some(QueryValue::Relationship(relationship)) => Ok(relationship.id),
            Some(value) => Err(write_parse_error(&format!(
                "MATCH returned non-relationship value {value:?}"
            ))),
            None => Err(write_parse_error(
                "MATCH did not return the target relationship",
            )),
        })
        .collect()
}

pub(super) fn find_merge_node_in_graph(
    graph: &impl GraphRead,
    labels: &[String],
    properties: &Properties,
) -> DatabaseResult<Option<Node>> {
    let mut nodes = graph.nodes()?;
    nodes.sort_by_key(|node| node.id);
    Ok(nodes
        .into_iter()
        .find(|node| node_matches_merge_pattern(node, labels, properties)))
}

pub(super) fn find_merge_relationship_in_graph(
    graph: &impl GraphRead,
    from: NodeId,
    to: NodeId,
    rel_type: &str,
    properties: &Properties,
) -> DatabaseResult<Option<Relationship>> {
    let mut relationships = graph.outgoing_by_type(from, rel_type)?;
    relationships.sort_by_key(|relationship| relationship.id);
    Ok(relationships.into_iter().find(|relationship| {
        relationship.to == to
            && properties
                .iter()
                .all(|(key, value)| relationship.properties.get(key) == Some(value))
    }))
}

pub(super) fn matches_target_shard(target_shard: Option<ShardId>, shard_id: ShardId) -> bool {
    target_shard
        .map(|target| target == shard_id)
        .unwrap_or(true)
}

pub(super) fn ensure_metadata_target_shard(target_shard: Option<ShardId>) -> DatabaseResult<()> {
    if matches_target_shard(target_shard, 0) {
        Ok(())
    } else {
        Err(DatabaseError::InvalidConfig(
            "index metadata Cypher must target shard 0".to_string(),
        ))
    }
}

pub(super) fn return_created_node(
    variable: String,
    returns: Option<WriteReturnItems>,
    id: NodeId,
    labels: Vec<String>,
    properties: Properties,
) -> Vec<QueryRow> {
    let _ = variable;
    return_node_for_write(Node::new(id, labels, properties), returns.as_ref())
}

pub(super) fn return_created_relationship(
    variable: String,
    returns: Option<WriteReturnItems>,
    relationship: Relationship,
) -> Vec<QueryRow> {
    let _ = variable;
    return_relationship_for_write(relationship, returns.as_ref())
}

pub(super) fn return_node_for_write(
    node: Node,
    returns: Option<&WriteReturnItems>,
) -> Vec<QueryRow> {
    let Some(returns) = returns else {
        return Vec::new();
    };
    vec![write_node_return_row(&node, returns)]
}

pub(super) fn return_relationship_for_write(
    relationship: Relationship,
    returns: Option<&WriteReturnItems>,
) -> Vec<QueryRow> {
    let Some(returns) = returns else {
        return Vec::new();
    };
    vec![write_relationship_return_row(&relationship, returns)]
}
