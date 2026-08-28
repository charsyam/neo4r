struct NodePatternWrite {
    variable: String,
    labels: Vec<String>,
    properties: Properties,
}

struct RelationshipPatternWrite {
    variable: String,
    from_variable: String,
    to_variable: String,
    rel_type: String,
    properties: Properties,
}

fn parse_node_pattern_write(input: &str, params: &QueryParams) -> DatabaseResult<NodePatternWrite> {
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

fn parse_relationship_pattern_write(
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

fn parse_property_map(input: &str, params: &QueryParams) -> DatabaseResult<Properties> {
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

fn parse_write_property_value(input: &str, params: &QueryParams) -> DatabaseResult<Value> {
    let value = parse_write_value(input, params)?;
    ensure_storable_property_value(&value)?;
    Ok(value)
}

fn validate_property_map_values(properties: &Properties) -> DatabaseResult<()> {
    for value in properties.values() {
        ensure_storable_property_value(value)?;
    }
    Ok(())
}

fn ensure_storable_property_value(value: &Value) -> DatabaseResult<()> {
    ensure_write_parse(
        !matches!(value, Value::Map(_)),
        "graph properties do not support nested map values",
    )
}

fn validate_storable_properties(properties: &Properties) -> DatabaseResult<()> {
    for value in properties.values() {
        validate_storable_property_value(value)?;
    }
    Ok(())
}

fn validate_storable_property_value(value: &Value) -> DatabaseResult<()> {
    if matches!(value, Value::Map(_)) {
        return Err(DatabaseError::InvalidConfig(
            "graph properties do not support nested map values".to_string(),
        ));
    }
    Ok(())
}

fn parse_write_value(input: &str, params: &QueryParams) -> DatabaseResult<Value> {
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

fn parse_vector_value(input: &str) -> DatabaseResult<Value> {
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

fn parse_property_ref_write(input: &str) -> DatabaseResult<(String, String)> {
    let (variable, key) = input
        .split_once('.')
        .ok_or_else(|| write_parse_error("property reference must use variable.property"))?;
    validate_identifier_write(variable.trim())?;
    validate_identifier_write(key.trim())?;
    Ok((variable.trim().to_string(), key.trim().to_string()))
}

fn parse_return_variable(input: &str) -> DatabaseResult<String> {
    let variable = input.trim();
    validate_identifier_write(variable)?;
    Ok(variable.to_string())
}

fn parse_optional_write_return<'a>(
    input: &'a str,
) -> DatabaseResult<(&'a str, Option<WriteReturnItems>)> {
    match split_keyword(input, "RETURN") {
        Some((body, returns)) => Ok((body.trim(), Some(parse_write_return_items(returns)?))),
        None => Ok((input.trim(), None)),
    }
}

fn parse_write_return_items(input: &str) -> DatabaseResult<WriteReturnItems> {
    let mut items = Vec::new();
    for item in split_top_level_commas(input.trim())? {
        items.push(parse_write_return_item(item)?);
    }
    ensure_write_parse(!items.is_empty(), "write RETURN requires at least one item")?;
    Ok(items)
}

fn parse_write_return_item(input: &str) -> DatabaseResult<WriteReturnItem> {
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

fn ensure_write_return_matches(
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

fn ensure_write_return_variables(
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

fn query_match_node_ids(
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

fn query_match_relationship_ids(
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

fn find_merge_node_in_graph(
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

fn find_merge_relationship_in_graph(
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

fn matches_target_shard(target_shard: Option<ShardId>, shard_id: ShardId) -> bool {
    target_shard
        .map(|target| target == shard_id)
        .unwrap_or(true)
}

fn ensure_metadata_target_shard(target_shard: Option<ShardId>) -> DatabaseResult<()> {
    if matches_target_shard(target_shard, 0) {
        Ok(())
    } else {
        Err(DatabaseError::InvalidConfig(
            "index metadata Cypher must target shard 0".to_string(),
        ))
    }
}

fn return_created_node(
    variable: String,
    returns: Option<WriteReturnItems>,
    id: NodeId,
    labels: Vec<String>,
    properties: Properties,
) -> Vec<QueryRow> {
    let _ = variable;
    return_node_for_write(Node::new(id, labels, properties), returns.as_ref())
}

fn return_created_relationship(
    variable: String,
    returns: Option<WriteReturnItems>,
    relationship: Relationship,
) -> Vec<QueryRow> {
    let _ = variable;
    return_relationship_for_write(relationship, returns.as_ref())
}

fn return_node_for_write(node: Node, returns: Option<&WriteReturnItems>) -> Vec<QueryRow> {
    let Some(returns) = returns else {
        return Vec::new();
    };
    vec![write_node_return_row(&node, returns)]
}

fn return_relationship_for_write(
    relationship: Relationship,
    returns: Option<&WriteReturnItems>,
) -> Vec<QueryRow> {
    let Some(returns) = returns else {
        return Vec::new();
    };
    vec![write_relationship_return_row(&relationship, returns)]
}

fn apply_assignments_to_properties(
    properties: &mut Properties,
    assignments: &[PropertyAssignment],
) {
    for assignment in assignments {
        if matches!(assignment.value, Value::Null) {
            properties.remove(&assignment.key);
        } else {
            properties.insert(assignment.key.clone(), assignment.value.clone());
        }
    }
}

fn create_properties_after_set(
    mut properties: Properties,
    assignments: Vec<PropertyAssignment>,
    replacement: Option<Properties>,
) -> Properties {
    if let Some(replacement) = replacement {
        properties_without_null_values(replacement)
    } else {
        apply_assignments_to_properties(&mut properties, &assignments);
        properties
    }
}

fn properties_after_set(
    mut properties: Properties,
    assignments: &[PropertyAssignment],
    replacement: Option<&Properties>,
) -> Properties {
    if let Some(replacement) = replacement {
        properties_without_null_values(replacement.clone())
    } else {
        apply_assignments_to_properties(&mut properties, assignments);
        properties
    }
}

fn replace_node_properties(
    db: &mut Neo4rDatabase,
    id: NodeId,
    before: &Properties,
    after: &Properties,
) -> DatabaseResult<()> {
    for key in property_removes(before, after) {
        db.remove_node_property(id, key)?;
    }
    for (key, value) in property_sets(before, after) {
        db.set_node_property(id, key, value)?;
    }
    Ok(())
}

fn replace_relationship_properties(
    db: &mut Neo4rDatabase,
    id: RelationshipId,
    before: &Properties,
    after: &Properties,
) -> DatabaseResult<()> {
    for key in property_removes(before, after) {
        db.remove_relationship_property(id, key)?;
    }
    for (key, value) in property_sets(before, after) {
        db.set_relationship_property(id, key, value)?;
    }
    Ok(())
}

fn apply_node_property_assignment(
    db: &mut Neo4rDatabase,
    id: NodeId,
    assignment: &PropertyAssignment,
) -> DatabaseResult<()> {
    if matches!(assignment.value, Value::Null) {
        db.remove_node_property(id, assignment.key.clone())
    } else {
        db.set_node_property(id, assignment.key.clone(), assignment.value.clone())
    }
}

fn apply_relationship_property_assignment(
    db: &mut Neo4rDatabase,
    id: RelationshipId,
    assignment: &PropertyAssignment,
) -> DatabaseResult<()> {
    if matches!(assignment.value, Value::Null) {
        db.remove_relationship_property(id, assignment.key.clone())
    } else {
        db.set_relationship_property(id, assignment.key.clone(), assignment.value.clone())
    }
}

fn replace_node_properties_with_handle(
    db: &Neo4rDatabaseHandle,
    id: NodeId,
    before: &Properties,
    after: &Properties,
) -> DatabaseResult<()> {
    for key in property_removes(before, after) {
        db.remove_node_property(id, key)?;
    }
    for (key, value) in property_sets(before, after) {
        db.set_node_property(id, key, value)?;
    }
    Ok(())
}

fn replace_relationship_properties_with_handle(
    db: &Neo4rDatabaseHandle,
    id: RelationshipId,
    before: &Properties,
    after: &Properties,
) -> DatabaseResult<()> {
    for key in property_removes(before, after) {
        db.remove_relationship_property(id, key)?;
    }
    for (key, value) in property_sets(before, after) {
        db.set_relationship_property(id, key, value)?;
    }
    Ok(())
}

fn apply_node_property_assignment_with_handle(
    db: &Neo4rDatabaseHandle,
    id: NodeId,
    assignment: &PropertyAssignment,
) -> DatabaseResult<()> {
    if matches!(assignment.value, Value::Null) {
        db.remove_node_property(id, assignment.key.clone())
    } else {
        db.set_node_property(id, assignment.key.clone(), assignment.value.clone())
    }
}

fn apply_relationship_property_assignment_with_handle(
    db: &Neo4rDatabaseHandle,
    id: RelationshipId,
    assignment: &PropertyAssignment,
) -> DatabaseResult<()> {
    if matches!(assignment.value, Value::Null) {
        db.remove_relationship_property(id, assignment.key.clone())
    } else {
        db.set_relationship_property(id, assignment.key.clone(), assignment.value.clone())
    }
}

fn property_sets(before: &Properties, after: &Properties) -> Vec<(String, Value)> {
    let mut keys = after.keys().cloned().collect::<Vec<_>>();
    keys.sort();
    keys.into_iter()
        .filter_map(|key| {
            let value = after.get(&key)?;
            if matches!(value, Value::Null) {
                return None;
            }
            if before.get(&key) == Some(value) {
                None
            } else {
                Some((key, value.clone()))
            }
        })
        .collect()
}

fn property_removes(before: &Properties, after: &Properties) -> Vec<String> {
    let mut keys = before.keys().cloned().collect::<Vec<_>>();
    keys.sort();
    keys.into_iter()
        .filter(|key| !after.contains_key(key) || matches!(after.get(key), Some(Value::Null)))
        .collect()
}

fn properties_without_null_values(properties: Properties) -> Properties {
    properties
        .into_iter()
        .filter(|(_, value)| !matches!(value, Value::Null))
        .collect()
}

fn append_property_delta_commands(
    commands: &mut Vec<Command>,
    before: &Properties,
    after: &Properties,
    mut set_command: impl FnMut(String, Value) -> Command,
    mut remove_command: impl FnMut(String) -> Command,
) {
    let keys = before
        .keys()
        .chain(after.keys())
        .cloned()
        .collect::<BTreeSet<_>>();
    let mut removes = Vec::new();
    let mut sets = Vec::new();
    for key in keys {
        match (before.get(&key), after.get(&key)) {
            (Some(before), Some(after)) if before == after => {}
            (_, Some(after)) => sets.push(set_command(key, after.clone())),
            (Some(_), None) => removes.push(remove_command(key)),
            (None, None) => {}
        }
    }
    commands.extend(removes);
    commands.extend(sets);
}

fn append_label_delta_commands(
    commands: &mut Vec<Command>,
    before: &[String],
    after: &[String],
    mut add_command: impl FnMut(String) -> Command,
    mut remove_command: impl FnMut(String) -> Command,
) {
    let before = before.iter().cloned().collect::<BTreeSet<_>>();
    let after = after.iter().cloned().collect::<BTreeSet<_>>();
    for label in after.difference(&before) {
        commands.push(add_command(label.clone()));
    }
    for label in before.difference(&after) {
        commands.push(remove_command(label.clone()));
    }
}

fn return_nodes_after_write(
    ids: &[NodeId],
    returns: Option<&WriteReturnItems>,
    mut load: impl FnMut(NodeId) -> DatabaseResult<Option<Node>>,
) -> DatabaseResult<Vec<QueryRow>> {
    let Some(returns) = returns else {
        return Ok(Vec::new());
    };
    let mut rows = Vec::new();
    for id in ids {
        if let Some(node) = load(*id)? {
            rows.push(write_node_return_row(&node, returns));
        }
    }
    Ok(rows)
}

fn return_relationships_after_write(
    ids: &[RelationshipId],
    returns: Option<&WriteReturnItems>,
    mut load: impl FnMut(RelationshipId) -> DatabaseResult<Option<Relationship>>,
) -> DatabaseResult<Vec<QueryRow>> {
    let Some(returns) = returns else {
        return Ok(Vec::new());
    };
    let mut rows = Vec::new();
    for id in ids {
        if let Some(relationship) = load(*id)? {
            rows.push(write_relationship_return_row(&relationship, returns));
        }
    }
    Ok(rows)
}

fn write_node_return_row(node: &Node, returns: &[WriteReturnItem]) -> QueryRow {
    let mut row = QueryRow::new();
    for item in returns {
        match item {
            WriteReturnItem::Variable(variable) => {
                row.insert(variable.clone(), QueryValue::Node(node.clone()));
            }
            WriteReturnItem::Property { variable, key } => {
                row.insert(
                    format!("{variable}.{key}"),
                    QueryValue::Scalar(node.properties.get(key).cloned().unwrap_or(Value::Null)),
                );
            }
        }
    }
    row
}

fn write_relationship_return_row(
    relationship: &Relationship,
    returns: &[WriteReturnItem],
) -> QueryRow {
    let mut row = QueryRow::new();
    for item in returns {
        match item {
            WriteReturnItem::Variable(variable) => {
                row.insert(
                    variable.clone(),
                    QueryValue::Relationship(relationship.clone()),
                );
            }
            WriteReturnItem::Property { variable, key } => {
                row.insert(
                    format!("{variable}.{key}"),
                    QueryValue::Scalar(
                        relationship
                            .properties
                            .get(key)
                            .cloned()
                            .unwrap_or(Value::Null),
                    ),
                );
            }
        }
    }
    row
}

fn write_node_relationship_return_row(
    node_variable: &str,
    node: &Node,
    relationship_variable: &str,
    relationship: &Relationship,
    returns: &[WriteReturnItem],
) -> QueryRow {
    let mut row = QueryRow::new();
    for item in returns {
        match item {
            WriteReturnItem::Variable(variable) if variable == node_variable => {
                row.insert(variable.clone(), QueryValue::Node(node.clone()));
            }
            WriteReturnItem::Variable(variable) if variable == relationship_variable => {
                row.insert(
                    variable.clone(),
                    QueryValue::Relationship(relationship.clone()),
                );
            }
            WriteReturnItem::Property { variable, key } if variable == node_variable => {
                row.insert(
                    format!("{variable}.{key}"),
                    QueryValue::Scalar(node.properties.get(key).cloned().unwrap_or(Value::Null)),
                );
            }
            WriteReturnItem::Property { variable, key } if variable == relationship_variable => {
                row.insert(
                    format!("{variable}.{key}"),
                    QueryValue::Scalar(
                        relationship
                            .properties
                            .get(key)
                            .cloned()
                            .unwrap_or(Value::Null),
                    ),
                );
            }
            _ => {}
        }
    }
    row
}

fn strip_node_pattern_properties(input: &str) -> DatabaseResult<String> {
    let input = input.trim();
    let Some(index) = top_level_brace_start(input) else {
        return Ok(input.to_string());
    };
    ensure_write_parse(input.ends_with(')'), "node pattern must end with )")?;
    Ok(format!("{}{}", input[..index].trim_end(), ")"))
}

fn strip_relationship_properties(input: &str) -> DatabaseResult<String> {
    let mut output = String::with_capacity(input.len());
    let mut depth = 0_i32;
    let mut in_string = false;
    for ch in input.chars() {
        match ch {
            '"' if depth == 0 => {
                in_string = !in_string;
                output.push(ch);
            }
            '{' if !in_string => depth += 1,
            '}' if !in_string => {
                depth -= 1;
                ensure_write_parse(depth >= 0, "unbalanced property literal")?;
            }
            _ if depth == 0 => output.push(ch),
            _ => {}
        }
    }
    ensure_write_parse(depth == 0, "unbalanced property literal")?;
    Ok(output)
}

fn write_value_literal(value: &Value) -> DatabaseResult<String> {
    match value {
        Value::Null => Ok("null".to_string()),
        Value::Bool(value) => Ok(value.to_string()),
        Value::Int(value) => Ok(value.to_string()),
        Value::Float(value) => Ok(value.to_string()),
        Value::String(value) => Ok(format!("\"{}\"", value.replace('"', "\\\""))),
        Value::Vector(_) => Err(write_parse_error(
            "MATCH pattern property lookup does not support vector values",
        )),
        Value::Map(_) => Err(write_parse_error(
            "MATCH pattern property lookup does not support map values",
        )),
    }
}

fn starts_with_keyword(input: &str, keyword: &str) -> bool {
    let Some(head) = input.get(..keyword.len()) else {
        return false;
    };
    head.eq_ignore_ascii_case(keyword)
        && input[keyword.len()..]
            .chars()
            .next()
            .is_none_or(|ch| ch.is_whitespace())
}

fn strip_keyword<'a>(input: &'a str, keyword: &str) -> DatabaseResult<&'a str> {
    ensure_write_parse(starts_with_keyword(input, keyword), "expected keyword")?;
    Ok(input[keyword.len()..].trim())
}

fn strip_keyword_suffix<'a>(input: &'a str, keyword: &str) -> Option<&'a str> {
    let input = input.trim();
    let suffix_start = input.len().checked_sub(keyword.len())?;
    let suffix = input.get(suffix_start..)?;
    if !suffix.eq_ignore_ascii_case(keyword) {
        return None;
    }
    if suffix_start > 0
        && !input[..suffix_start]
            .chars()
            .next_back()
            .is_some_and(|ch| ch.is_whitespace())
    {
        return None;
    }
    Some(input[..suffix_start].trim_end())
}

fn split_keyword<'a>(input: &'a str, keyword: &str) -> Option<(&'a str, &'a str)> {
    let haystack = input.to_ascii_uppercase();
    let keyword = keyword.to_ascii_uppercase();
    for (index, _) in haystack.match_indices(&keyword) {
        let before_is_boundary = input[..index]
            .chars()
            .next_back()
            .is_some_and(|ch| ch.is_whitespace());
        let after_index = index + keyword.len();
        let after_is_boundary = input[after_index..]
            .chars()
            .next()
            .is_some_and(|ch| ch.is_whitespace());
        if before_is_boundary && after_is_boundary {
            return Some((&input[..index], input[after_index..].trim_start()));
        }
    }
    None
}

fn find_keyword(input: &str, keyword: &str) -> Option<usize> {
    split_keyword(input, keyword).map(|(before, _)| before.len())
}

fn strip_wrapping_write(input: &str, open: char, close: char) -> DatabaseResult<&str> {
    ensure_write_parse(
        input.starts_with(open) && input.ends_with(close),
        "invalid wrapping",
    )?;
    Ok(&input[open.len_utf8()..input.len() - close.len_utf8()])
}

fn top_level_brace_start(input: &str) -> Option<usize> {
    let mut in_string = false;
    for (index, ch) in input.char_indices() {
        match ch {
            '"' => in_string = !in_string,
            '{' if !in_string => return Some(index),
            _ => {}
        }
    }
    None
}

fn split_top_level_commas(input: &str) -> DatabaseResult<Vec<&str>> {
    let mut entries = Vec::new();
    let mut start = 0;
    let mut depth = 0_i32;
    let mut in_string = false;
    for (index, ch) in input.char_indices() {
        match ch {
            '"' => in_string = !in_string,
            '[' | '{' if !in_string => depth += 1,
            ']' | '}' if !in_string => depth -= 1,
            ',' if !in_string && depth == 0 => {
                entries.push(input[start..index].trim());
                start = index + ch.len_utf8();
            }
            _ => {}
        }
        ensure_write_parse(depth >= 0, "unbalanced property literal")?;
    }
    entries.push(input[start..].trim());
    ensure_write_parse(
        !entries.iter().any(|entry| entry.is_empty()),
        "empty property entry",
    )?;
    Ok(entries)
}

fn validate_identifier_write(input: &str) -> DatabaseResult<()> {
    let mut chars = input.chars();
    let Some(first) = chars.next() else {
        return Err(write_parse_error("identifier cannot be empty"));
    };
    ensure_write_parse(
        first.is_ascii_alphabetic() || first == '_',
        "identifier must start with a letter or underscore",
    )?;
    ensure_write_parse(
        chars.all(|ch| ch.is_ascii_alphanumeric() || ch == '_'),
        "identifier may only contain letters, digits, or underscores",
    )
}

fn ensure_write_parse(condition: bool, message: &str) -> DatabaseResult<()> {
    if condition {
        Ok(())
    } else {
        Err(write_parse_error(message))
    }
}

fn write_parse_error(message: &str) -> DatabaseError {
    DatabaseError::Query(QueryError::Parse(message.to_string()))
}

fn validate_read_options(
    snapshot: &Neo4rReadSnapshot,
    options: QueryOptions,
) -> DatabaseResult<()> {
    validate_read_isolation(options.isolation);
    validate_read_consistency(snapshot, options.consistency)
}

fn validate_read_isolation(isolation: ReadIsolation) {
    match isolation {
        ReadIsolation::ReadCommitted | ReadIsolation::Snapshot => {}
    }
}

fn validate_read_consistency(
    snapshot: &Neo4rReadSnapshot,
    consistency: ReadConsistency,
) -> DatabaseResult<()> {
    match consistency {
        ReadConsistency::Strong => {
            if snapshot.applied_indexes() == snapshot.committed_indexes() {
                Ok(())
            } else {
                Err(DatabaseError::Replication(
                    "strong read requires applied indexes to match committed indexes".to_string(),
                ))
            }
        }
        ReadConsistency::FollowerStale => Ok(()),
        ReadConsistency::BoundedStaleness { max_staleness_ms } => {
            let now_ms = HybridClock::new().tick().physical_ms;
            let age = now_ms.saturating_sub(snapshot.timestamp().physical_ms);
            if age <= max_staleness_ms {
                Ok(())
            } else {
                Err(DatabaseError::Replication(format!(
                    "snapshot staleness {age}ms exceeds bound {max_staleness_ms}ms"
                )))
            }
        }
    }
}
