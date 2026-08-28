use super::*;

pub(in crate::database) fn apply_assignments_to_properties(
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

pub(in crate::database) fn create_properties_after_set(
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

pub(in crate::database) fn properties_after_set(
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

pub(in crate::database) fn replace_node_properties(
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

pub(in crate::database) fn replace_relationship_properties(
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

pub(in crate::database) fn apply_node_property_assignment(
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

pub(in crate::database) fn apply_relationship_property_assignment(
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

pub(in crate::database) fn replace_node_properties_with_handle(
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

pub(in crate::database) fn replace_relationship_properties_with_handle(
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

pub(in crate::database) fn apply_node_property_assignment_with_handle(
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

pub(in crate::database) fn apply_relationship_property_assignment_with_handle(
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

pub(in crate::database) fn property_sets(
    before: &Properties,
    after: &Properties,
) -> Vec<(String, Value)> {
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

pub(in crate::database) fn property_removes(
    before: &Properties,
    after: &Properties,
) -> Vec<String> {
    let mut keys = before.keys().cloned().collect::<Vec<_>>();
    keys.sort();
    keys.into_iter()
        .filter(|key| !after.contains_key(key) || matches!(after.get(key), Some(Value::Null)))
        .collect()
}

pub(in crate::database) fn properties_without_null_values(properties: Properties) -> Properties {
    properties
        .into_iter()
        .filter(|(_, value)| !matches!(value, Value::Null))
        .collect()
}

pub(in crate::database) fn append_property_delta_commands(
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

pub(in crate::database) fn append_label_delta_commands(
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

pub(in crate::database) fn return_nodes_after_write(
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

pub(in crate::database) fn return_relationships_after_write(
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

pub(in crate::database) fn write_node_return_row(
    node: &Node,
    returns: &[WriteReturnItem],
) -> QueryRow {
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

pub(in crate::database) fn write_relationship_return_row(
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

pub(in crate::database) fn write_node_relationship_return_row(
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

pub(in crate::database) fn strip_node_pattern_properties(input: &str) -> DatabaseResult<String> {
    let input = input.trim();
    let Some(index) = top_level_brace_start(input) else {
        return Ok(input.to_string());
    };
    ensure_write_parse(input.ends_with(')'), "node pattern must end with )")?;
    Ok(format!("{}{}", input[..index].trim_end(), ")"))
}

pub(in crate::database) fn strip_relationship_properties(input: &str) -> DatabaseResult<String> {
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

pub(in crate::database) fn write_value_literal(value: &Value) -> DatabaseResult<String> {
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

pub(in crate::database) fn starts_with_keyword(input: &str, keyword: &str) -> bool {
    let Some(head) = input.get(..keyword.len()) else {
        return false;
    };
    head.eq_ignore_ascii_case(keyword)
        && input[keyword.len()..]
            .chars()
            .next()
            .is_none_or(|ch| ch.is_whitespace())
}

pub(in crate::database) fn strip_keyword<'a>(
    input: &'a str,
    keyword: &str,
) -> DatabaseResult<&'a str> {
    ensure_write_parse(starts_with_keyword(input, keyword), "expected keyword")?;
    Ok(input[keyword.len()..].trim())
}

pub(in crate::database) fn strip_keyword_suffix<'a>(
    input: &'a str,
    keyword: &str,
) -> Option<&'a str> {
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

pub(in crate::database) fn split_keyword<'a>(
    input: &'a str,
    keyword: &str,
) -> Option<(&'a str, &'a str)> {
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

pub(in crate::database) fn find_keyword(input: &str, keyword: &str) -> Option<usize> {
    split_keyword(input, keyword).map(|(before, _)| before.len())
}

pub(in crate::database) fn strip_wrapping_write(
    input: &str,
    open: char,
    close: char,
) -> DatabaseResult<&str> {
    ensure_write_parse(
        input.starts_with(open) && input.ends_with(close),
        "invalid wrapping",
    )?;
    Ok(&input[open.len_utf8()..input.len() - close.len_utf8()])
}

pub(in crate::database) fn top_level_brace_start(input: &str) -> Option<usize> {
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

pub(in crate::database) fn split_top_level_commas(input: &str) -> DatabaseResult<Vec<&str>> {
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

pub(in crate::database) fn validate_identifier_write(input: &str) -> DatabaseResult<()> {
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

pub(in crate::database) fn ensure_write_parse(
    condition: bool,
    message: &str,
) -> DatabaseResult<()> {
    if condition {
        Ok(())
    } else {
        Err(write_parse_error(message))
    }
}

pub(in crate::database) fn write_parse_error(message: &str) -> DatabaseError {
    DatabaseError::Query(QueryError::Parse(message.to_string()))
}

pub(in crate::database) fn validate_read_options(
    snapshot: &Neo4rReadSnapshot,
    options: QueryOptions,
) -> DatabaseResult<()> {
    validate_read_isolation(options.isolation);
    validate_read_consistency(snapshot, options.consistency)
}

pub(in crate::database) fn validate_read_isolation(isolation: ReadIsolation) {
    match isolation {
        ReadIsolation::ReadCommitted | ReadIsolation::Snapshot => {}
    }
}

pub(in crate::database) fn validate_read_consistency(
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
