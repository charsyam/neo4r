use super::write_cypher_helpers::*;
use super::write_cypher_model::*;
use super::*;

pub(super) fn parse_create_node_write(
    input: &str,
    params: &QueryParams,
) -> DatabaseResult<WriteCypher> {
    let body = strip_keyword(input, "CREATE")?;
    let (body, returns) = parse_optional_write_return(body)?;
    let (pattern, set_part) = match split_keyword(body, "SET") {
        Some((pattern, set_part)) => (pattern.trim(), Some(set_part.trim())),
        None => (body.trim(), None),
    };
    let NodePatternWrite {
        variable,
        labels,
        properties,
    } = parse_create_node_pattern_write(pattern, params)?;
    let replacement = match set_part {
        Some(set_part) => parse_property_replacement(
            set_part,
            &variable,
            params,
            "CREATE SET replacement variable must match the created variable",
        )?,
        None => None,
    };
    let assignments = match (set_part, replacement.as_ref()) {
        (Some(set_part), None) => parse_set_assignments(
            set_part,
            &variable,
            params,
            "CREATE SET variable must match the created variable",
        )?,
        _ => Vec::new(),
    };
    ensure_write_return_matches(returns.as_ref(), &variable, "CREATE RETURN")?;
    Ok(WriteCypher::CreateNode {
        variable,
        labels,
        properties,
        assignments,
        replacement,
        returns,
    })
}

pub(super) fn parse_create_node_then_relationship_write(
    input: &str,
    params: &QueryParams,
) -> DatabaseResult<WriteCypher> {
    let (create_part, with_part) =
        split_keyword(input, "WITH").ok_or_else(|| write_parse_error("expected WITH"))?;
    let WriteCypher::CreateNode {
        variable: node_variable,
        labels,
        properties: node_properties,
        assignments: node_assignments,
        replacement: node_replacement,
        returns: node_returns,
    } = parse_create_node_write(create_part.trim(), params)?
    else {
        return Err(write_parse_error("WITH create must start with CREATE node"));
    };
    ensure_write_parse(
        node_returns.is_none(),
        "CREATE WITH does not allow RETURN before WITH",
    )?;

    let (with_variables, after_match) = split_keyword(with_part, "MATCH")
        .ok_or_else(|| write_parse_error("expected MATCH after WITH"))?;
    let with_variables = split_top_level_commas(with_variables)?;
    ensure_write_parse(
        with_variables.len() == 1 && with_variables[0] == node_variable,
        "WITH must pass the created node variable",
    )?;

    let (match_part, create_relationship_part) = split_keyword(after_match, "CREATE")
        .ok_or_else(|| write_parse_error("expected relationship CREATE after WITH MATCH"))?;
    let matched_matcher = parse_node_matcher_body(match_part.trim(), params)?;
    let (relationship_pattern, returns) = parse_optional_write_return(create_relationship_part)?;
    let (relationship_pattern, set_part) = match split_keyword(relationship_pattern, "SET") {
        Some((pattern, set_part)) => (pattern.trim(), Some(set_part.trim())),
        None => (relationship_pattern.trim(), None),
    };
    let RelationshipPatternWrite {
        variable: relationship_variable,
        from_variable,
        to_variable,
        rel_type,
        properties: relationship_properties,
    } = parse_relationship_pattern_write(relationship_pattern, params)?;
    let created_node_is_from =
        if from_variable == node_variable && to_variable == matched_matcher.variable {
            true
        } else if to_variable == node_variable && from_variable == matched_matcher.variable {
            false
        } else {
            return Err(write_parse_error(
                "relationship CREATE must connect the WITH variable and MATCH variable",
            ));
        };
    let relationship_replacement = match set_part {
        Some(set_part) => parse_property_replacement(
            set_part,
            &relationship_variable,
            params,
            "relationship CREATE SET replacement variable must match the created relationship variable",
        )?,
        None => None,
    };
    let relationship_assignments = match (set_part, relationship_replacement.as_ref()) {
        (Some(set_part), None) => parse_set_assignments(
            set_part,
            &relationship_variable,
            params,
            "relationship CREATE SET variable must match the created relationship variable",
        )?,
        _ => Vec::new(),
    };
    ensure_write_return_variables(
        returns.as_ref(),
        &[node_variable.as_str(), relationship_variable.as_str()],
        "CREATE WITH RETURN",
    )?;
    Ok(WriteCypher::CreateNodeThenRelationship {
        node_variable,
        labels,
        node_properties,
        node_assignments,
        node_replacement,
        matched_matcher,
        relationship_variable,
        created_node_is_from,
        rel_type,
        relationship_properties,
        relationship_assignments,
        relationship_replacement,
        returns,
    })
}

pub(super) fn parse_create_node_pattern_write(
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
    if !head.starts_with(':') && !head.is_empty() {
        return parse_node_pattern_write(input, params);
    }
    let labels = head
        .trim_start_matches(':')
        .split(':')
        .map(str::trim)
        .filter(|label| !label.is_empty())
        .map(|label| {
            validate_identifier_write(label)?;
            Ok(label.to_string())
        })
        .collect::<DatabaseResult<Vec<_>>>()?;
    Ok(NodePatternWrite {
        variable: "__neo4r_anonymous_node".to_string(),
        labels,
        properties,
    })
}

pub(super) fn parse_merge_node_write(
    input: &str,
    params: &QueryParams,
) -> DatabaseResult<WriteCypher> {
    let body = strip_keyword(input, "MERGE")?;
    let (body, returns) = parse_optional_write_return(body)?;
    let NodePatternWrite {
        variable,
        labels,
        properties,
    } = parse_create_node_pattern_write(parse_merge_pattern_part(body)?, params)?;
    let clauses = parse_merge_set_clauses(body, &variable, params)?;
    ensure_write_return_matches(returns.as_ref(), &variable, "MERGE RETURN")?;
    Ok(WriteCypher::MergeNode {
        labels,
        properties,
        on_create: clauses.on_create,
        on_create_replacement: clauses.on_create_replacement,
        on_match: clauses.on_match,
        on_match_replacement: clauses.on_match_replacement,
        returns,
    })
}

pub(super) fn parse_set_node_property(
    match_part: &str,
    set_part: &str,
    params: &QueryParams,
) -> DatabaseResult<WriteCypher> {
    let matcher = parse_node_matcher_body(strip_keyword(match_part.trim(), "MATCH")?, params)?;
    let (set_part, returns) = parse_optional_write_return(set_part)?;
    if let Some(properties) = parse_property_replacement(
        set_part,
        &matcher.variable,
        params,
        "SET replacement variable must match the MATCH variable",
    )? {
        ensure_write_return_matches(returns.as_ref(), &matcher.variable, "SET RETURN")?;
        return Ok(WriteCypher::ReplaceNodeProperties {
            matcher,
            properties,
            returns,
        });
    }
    let assignments = parse_set_assignments(
        set_part,
        &matcher.variable,
        params,
        "SET variable must match the MATCH variable",
    )?;
    ensure_write_return_matches(returns.as_ref(), &matcher.variable, "SET RETURN")?;
    Ok(WriteCypher::SetNodeProperty {
        matcher,
        assignments,
        returns,
    })
}

pub(super) fn parse_create_relationship_write(
    match_part: &str,
    create_part: &str,
    params: &QueryParams,
) -> DatabaseResult<WriteCypher> {
    let (from_matcher, to_matcher) = parse_relationship_endpoint_matchers(
        match_part,
        params,
        "relationship CREATE requires two MATCH node patterns",
    )?;
    let (pattern, returns) = parse_optional_write_return(create_part)?;
    let (pattern, set_part) = match split_keyword(pattern, "SET") {
        Some((pattern, set_part)) => (pattern.trim(), Some(set_part.trim())),
        None => (pattern.trim(), None),
    };
    let RelationshipPatternWrite {
        variable,
        from_variable,
        to_variable,
        rel_type,
        properties,
    } = parse_relationship_pattern_write(pattern, params)?;
    let replacement = match set_part {
        Some(set_part) => parse_property_replacement(
            set_part,
            &variable,
            params,
            "relationship CREATE SET replacement variable must match the created relationship variable",
        )?,
        None => None,
    };
    let assignments = match (set_part, replacement.as_ref()) {
        (Some(set_part), None) => parse_set_assignments(
            set_part,
            &variable,
            params,
            "relationship CREATE SET variable must match the created relationship variable",
        )?,
        _ => Vec::new(),
    };
    ensure_write_parse(
        from_variable == from_matcher.variable,
        "relationship CREATE source variable must match the first MATCH variable",
    )?;
    ensure_write_parse(
        to_variable == to_matcher.variable,
        "relationship CREATE target variable must match the second MATCH variable",
    )?;
    ensure_write_return_matches(returns.as_ref(), &variable, "relationship CREATE RETURN")?;
    Ok(WriteCypher::CreateRelationship {
        variable,
        from_matcher,
        to_matcher,
        rel_type,
        properties,
        assignments,
        replacement,
        returns,
    })
}

pub(super) fn parse_merge_relationship_write(
    match_part: &str,
    merge_part: &str,
    params: &QueryParams,
) -> DatabaseResult<WriteCypher> {
    let (from_matcher, to_matcher) = parse_relationship_endpoint_matchers(
        match_part,
        params,
        "relationship MERGE requires two MATCH node patterns",
    )?;
    let (merge_part, returns) = parse_optional_write_return(merge_part)?;
    let RelationshipPatternWrite {
        variable,
        from_variable,
        to_variable,
        rel_type,
        properties,
    } = parse_relationship_pattern_write(parse_merge_pattern_part(merge_part)?, params)?;
    let clauses = parse_merge_set_clauses(merge_part, &variable, params)?;
    ensure_write_parse(
        from_variable == from_matcher.variable,
        "relationship MERGE source variable must match the first MATCH variable",
    )?;
    ensure_write_parse(
        to_variable == to_matcher.variable,
        "relationship MERGE target variable must match the second MATCH variable",
    )?;
    ensure_write_return_matches(returns.as_ref(), &variable, "relationship MERGE RETURN")?;
    Ok(WriteCypher::MergeRelationship {
        from_matcher,
        to_matcher,
        rel_type,
        properties,
        on_create: clauses.on_create,
        on_create_replacement: clauses.on_create_replacement,
        on_match: clauses.on_match,
        on_match_replacement: clauses.on_match_replacement,
        returns,
    })
}

pub(super) fn parse_relationship_endpoint_matchers(
    match_part: &str,
    params: &QueryParams,
    missing_message: &str,
) -> DatabaseResult<(NodeMatcher, NodeMatcher)> {
    let body = strip_keyword(match_part.trim(), "MATCH")?;
    if let Some((left_match, right_match)) = split_keyword(body, "MATCH") {
        return Ok((
            parse_node_matcher_body(left_match.trim(), params)?,
            parse_node_matcher_body(right_match.trim(), params)?,
        ));
    }

    let patterns = split_top_level_commas(body)?;
    ensure_write_parse(patterns.len() == 2, missing_message)?;
    Ok((
        parse_node_matcher_body(patterns[0], params)?,
        parse_node_matcher_body(patterns[1], params)?,
    ))
}

pub(super) fn parse_set_property(
    match_part: &str,
    set_part: &str,
    params: &QueryParams,
) -> DatabaseResult<WriteCypher> {
    if match_part.contains("->") {
        return parse_set_relationship_property(match_part, set_part, params);
    }
    if !set_part.contains('=') {
        return parse_add_node_label(match_part, set_part, params);
    }
    parse_set_node_property(match_part, set_part, params)
}

pub(super) fn parse_remove_property(
    match_part: &str,
    remove_part: &str,
    params: &QueryParams,
) -> DatabaseResult<WriteCypher> {
    if match_part.contains("->") {
        return parse_remove_relationship_property(match_part, remove_part, params);
    }
    if remove_part.contains(':') {
        return parse_remove_node_label(match_part, remove_part, params);
    }
    parse_remove_node_property(match_part, remove_part, params)
}

pub(super) fn parse_remove_node_property(
    match_part: &str,
    remove_part: &str,
    params: &QueryParams,
) -> DatabaseResult<WriteCypher> {
    let matcher = parse_node_matcher_body(strip_keyword(match_part.trim(), "MATCH")?, params)?;
    let (remove_part, returns) = parse_optional_write_return(remove_part)?;
    let keys = parse_remove_keys(
        remove_part,
        &matcher.variable,
        "REMOVE variable must match the MATCH variable",
    )?;
    ensure_write_return_matches(returns.as_ref(), &matcher.variable, "REMOVE RETURN")?;
    Ok(WriteCypher::RemoveNodeProperty {
        matcher,
        keys,
        returns,
    })
}

pub(super) fn parse_add_node_label(
    match_part: &str,
    set_part: &str,
    params: &QueryParams,
) -> DatabaseResult<WriteCypher> {
    let matcher = parse_node_matcher_body(strip_keyword(match_part.trim(), "MATCH")?, params)?;
    let (set_part, returns) = parse_optional_write_return(set_part)?;
    let labels = parse_label_refs(
        set_part,
        &matcher.variable,
        "SET label variable must match the MATCH variable",
    )?;
    ensure_write_return_matches(returns.as_ref(), &matcher.variable, "SET RETURN")?;
    Ok(WriteCypher::AddNodeLabel {
        matcher,
        labels,
        returns,
    })
}

pub(super) fn parse_remove_node_label(
    match_part: &str,
    remove_part: &str,
    params: &QueryParams,
) -> DatabaseResult<WriteCypher> {
    let matcher = parse_node_matcher_body(strip_keyword(match_part.trim(), "MATCH")?, params)?;
    let (remove_part, returns) = parse_optional_write_return(remove_part)?;
    let labels = parse_label_refs(
        remove_part,
        &matcher.variable,
        "REMOVE label variable must match the MATCH variable",
    )?;
    ensure_write_return_matches(returns.as_ref(), &matcher.variable, "REMOVE RETURN")?;
    Ok(WriteCypher::RemoveNodeLabel {
        matcher,
        labels,
        returns,
    })
}

pub(super) fn parse_remove_relationship_property(
    match_part: &str,
    remove_part: &str,
    params: &QueryParams,
) -> DatabaseResult<WriteCypher> {
    let matcher = parse_relationship_matcher(match_part, params)?;
    let (remove_part, returns) = parse_optional_write_return(remove_part)?;
    let keys = parse_remove_keys(
        remove_part,
        &matcher.variable,
        "REMOVE variable must match the MATCH relationship variable",
    )?;
    ensure_write_return_matches(returns.as_ref(), &matcher.variable, "REMOVE RETURN")?;
    Ok(WriteCypher::RemoveRelationshipProperty {
        matcher,
        keys,
        returns,
    })
}

pub(super) fn parse_set_relationship_property(
    match_part: &str,
    set_part: &str,
    params: &QueryParams,
) -> DatabaseResult<WriteCypher> {
    let matcher = parse_relationship_matcher(match_part, params)?;
    let (set_part, returns) = parse_optional_write_return(set_part)?;
    if let Some(properties) = parse_property_replacement(
        set_part,
        &matcher.variable,
        params,
        "SET replacement variable must match the MATCH relationship variable",
    )? {
        ensure_write_return_matches(returns.as_ref(), &matcher.variable, "SET RETURN")?;
        return Ok(WriteCypher::ReplaceRelationshipProperties {
            matcher,
            properties,
            returns,
        });
    }
    let assignments = parse_set_assignments(
        set_part,
        &matcher.variable,
        params,
        "SET variable must match the MATCH relationship variable",
    )?;
    ensure_write_return_matches(returns.as_ref(), &matcher.variable, "SET RETURN")?;
    Ok(WriteCypher::SetRelationshipProperty {
        matcher,
        assignments,
        returns,
    })
}

pub(super) fn parse_property_replacement(
    set_part: &str,
    expected_variable: &str,
    params: &QueryParams,
    variable_mismatch_message: &str,
) -> DatabaseResult<Option<Properties>> {
    let entries = split_top_level_commas(set_part.trim())?;
    if entries.len() != 1 {
        return Ok(None);
    }
    let entry = entries[0];
    if entry.contains("+=") {
        return Ok(None);
    }
    let Some((left, right)) = entry.split_once('=') else {
        return Ok(None);
    };
    let variable = left.trim();
    if variable.contains('.') {
        return Ok(None);
    }
    validate_identifier_write(variable)?;
    ensure_write_parse(variable == expected_variable, variable_mismatch_message)?;
    Ok(Some(parse_property_map(right.trim(), params)?))
}

pub(super) fn parse_set_assignments(
    set_part: &str,
    expected_variable: &str,
    params: &QueryParams,
    variable_mismatch_message: &str,
) -> DatabaseResult<Vec<PropertyAssignment>> {
    let mut assignments = Vec::new();
    for assignment in split_top_level_commas(set_part.trim())? {
        if let Some((left, right)) = assignment.split_once("+=") {
            let variable = left.trim();
            validate_identifier_write(variable)?;
            ensure_write_parse(variable == expected_variable, variable_mismatch_message)?;
            let properties = parse_property_map(right.trim(), params)?;
            ensure_write_parse(
                !properties.is_empty(),
                "SET += requires at least one property",
            )?;
            assignments.extend(
                properties
                    .into_iter()
                    .map(|(key, value)| PropertyAssignment { key, value }),
            );
            continue;
        }
        let (left, right) = assignment
            .split_once('=')
            .ok_or_else(|| write_parse_error("SET must use variable.property = value"))?;
        let (variable, key) = parse_property_ref_write(left.trim())?;
        ensure_write_parse(variable == expected_variable, variable_mismatch_message)?;
        assignments.push(PropertyAssignment {
            key,
            value: parse_write_property_value(right.trim(), params)?,
        });
    }
    ensure_write_parse(
        !assignments.is_empty(),
        "SET requires at least one assignment",
    )?;
    Ok(assignments)
}

pub(super) fn parse_merge_pattern_part(input: &str) -> DatabaseResult<&str> {
    let create_index = find_keyword(input, "ON CREATE SET");
    let match_index = find_keyword(input, "ON MATCH SET");
    let pattern_end = [create_index, match_index]
        .into_iter()
        .flatten()
        .min()
        .unwrap_or(input.len());
    let pattern = input[..pattern_end].trim();
    ensure_write_parse(!pattern.is_empty(), "MERGE requires a pattern")?;
    Ok(pattern)
}

pub(super) fn parse_merge_set_clauses(
    input: &str,
    expected_variable: &str,
    params: &QueryParams,
) -> DatabaseResult<MergeSetClauses> {
    let mut clauses = Vec::new();
    if let Some(index) = find_keyword(input, "ON CREATE SET") {
        clauses.push((index, "ON CREATE SET"));
    }
    if let Some(index) = find_keyword(input, "ON MATCH SET") {
        clauses.push((index, "ON MATCH SET"));
    }
    clauses.sort_by_key(|(index, _)| *index);

    let mut parsed = MergeSetClauses::default();
    for (position, (index, keyword)) in clauses.iter().enumerate() {
        let start = index + keyword.len();
        let end = clauses
            .get(position + 1)
            .map(|(next_index, _)| *next_index)
            .unwrap_or(input.len());
        let set_part = &input[start..end];
        let replacement = parse_property_replacement(
            set_part,
            expected_variable,
            params,
            "MERGE SET replacement variable must match the MERGE variable",
        )?;
        let assignments = match replacement.as_ref() {
            Some(_) => Vec::new(),
            None => parse_set_assignments(
                set_part,
                expected_variable,
                params,
                "MERGE SET variable must match the MERGE variable",
            )?,
        };
        match *keyword {
            "ON CREATE SET" => {
                ensure_write_parse(
                    parsed.on_create.is_empty() && parsed.on_create_replacement.is_none(),
                    "duplicate ON CREATE SET clause",
                )?;
                parsed.on_create = assignments;
                parsed.on_create_replacement = replacement;
            }
            "ON MATCH SET" => {
                ensure_write_parse(
                    parsed.on_match.is_empty() && parsed.on_match_replacement.is_none(),
                    "duplicate ON MATCH SET clause",
                )?;
                parsed.on_match = assignments;
                parsed.on_match_replacement = replacement;
            }
            _ => unreachable!(),
        }
    }
    Ok(parsed)
}

pub(super) fn parse_remove_keys(
    remove_part: &str,
    expected_variable: &str,
    variable_mismatch_message: &str,
) -> DatabaseResult<Vec<String>> {
    let mut keys = Vec::new();
    for property_ref in split_top_level_commas(remove_part.trim())? {
        let (variable, key) = parse_property_ref_write(property_ref)?;
        ensure_write_parse(variable == expected_variable, variable_mismatch_message)?;
        keys.push(key);
    }
    ensure_write_parse(!keys.is_empty(), "REMOVE requires at least one property")?;
    Ok(keys)
}

pub(super) fn parse_label_refs(
    input: &str,
    expected_variable: &str,
    variable_mismatch_message: &str,
) -> DatabaseResult<Vec<String>> {
    let mut labels = Vec::new();
    for label_ref in split_top_level_commas(input.trim())? {
        let (variable, label_part) = label_ref
            .split_once(':')
            .ok_or_else(|| write_parse_error("label update must use variable:Label"))?;
        ensure_write_parse(
            variable.trim() == expected_variable,
            variable_mismatch_message,
        )?;
        for label in label_part.split(':') {
            let label = label.trim();
            validate_identifier_write(label)?;
            labels.push(label.to_string());
        }
    }
    ensure_write_parse(
        !labels.is_empty(),
        "label update requires at least one label",
    )?;
    labels.sort();
    labels.dedup();
    Ok(labels)
}

pub(super) fn parse_delete(
    match_part: &str,
    delete_part: &str,
    params: &QueryParams,
) -> DatabaseResult<WriteCypher> {
    if match_part.contains("->") {
        return parse_delete_relationship(match_part, delete_part, params);
    }
    parse_delete_node(match_part, delete_part, params)
}

pub(super) fn parse_delete_node(
    match_part: &str,
    delete_part: &str,
    params: &QueryParams,
) -> DatabaseResult<WriteCypher> {
    let matcher = parse_node_matcher(match_part, params)?;
    let (delete_part, returns) = parse_optional_write_return(delete_part)?;
    let variable = parse_return_variable(delete_part)?;
    ensure_write_parse(
        variable == matcher.variable,
        "DELETE variable must match the MATCH variable",
    )?;
    ensure_write_return_matches(returns.as_ref(), &matcher.variable, "DELETE RETURN")?;
    Ok(WriteCypher::DeleteNode { matcher, returns })
}

pub(super) fn parse_node_matcher(input: &str, params: &QueryParams) -> DatabaseResult<NodeMatcher> {
    parse_node_matcher_body(strip_keyword(input.trim(), "MATCH")?.trim(), params)
}

pub(super) fn parse_node_matcher_body(
    input: &str,
    params: &QueryParams,
) -> DatabaseResult<NodeMatcher> {
    let match_body = input.trim();
    let (pattern, predicate) = match split_keyword(match_body, "WHERE") {
        Some((pattern, predicate)) => (pattern.trim(), Some(predicate.trim())),
        None => (match_body, None),
    };
    let NodePatternWrite {
        variable,
        properties,
        ..
    } = parse_node_pattern_write(pattern, params)?;
    let match_pattern = strip_node_pattern_properties(pattern)?;
    let match_query = if let Some(predicate) = predicate {
        ensure_write_parse(
            properties.is_empty(),
            "MATCH pattern properties cannot be combined with WHERE",
        )?;
        format!("MATCH {match_pattern} WHERE {predicate} RETURN {variable}")
    } else if properties.is_empty() {
        format!("MATCH {match_pattern} RETURN {variable}")
    } else {
        ensure_write_parse(
            properties.len() == 1,
            "MATCH pattern property lookup supports one property",
        )?;
        let (key, value) = properties.iter().next().unwrap();
        format!(
            "MATCH {match_pattern} WHERE {variable}.{key} = {} RETURN {variable}",
            write_value_literal(value)?
        )
    };
    Ok(NodeMatcher {
        variable,
        match_query,
    })
}

pub(super) fn parse_delete_relationship(
    match_part: &str,
    delete_part: &str,
    params: &QueryParams,
) -> DatabaseResult<WriteCypher> {
    let matcher = parse_relationship_matcher(match_part, params)?;
    let (delete_part, returns) = parse_optional_write_return(delete_part)?;
    let variable = parse_return_variable(delete_part)?;
    ensure_write_parse(
        variable == matcher.variable,
        "DELETE variable must match the MATCH relationship variable",
    )?;
    ensure_write_return_matches(returns.as_ref(), &matcher.variable, "DELETE RETURN")?;
    Ok(WriteCypher::DeleteRelationship { matcher, returns })
}

pub(super) fn parse_relationship_matcher(
    input: &str,
    params: &QueryParams,
) -> DatabaseResult<RelationshipMatcher> {
    let match_body = strip_keyword(input.trim(), "MATCH")?.trim();
    let (pattern, predicate) = match split_keyword(match_body, "WHERE") {
        Some((pattern, predicate)) => (pattern.trim(), Some(predicate.trim())),
        None => (match_body, None),
    };
    let relationship = parse_relationship_pattern_write(pattern, params)?;
    let predicate = relationship_matcher_predicate(&relationship, predicate)?;
    Ok(RelationshipMatcher {
        variable: relationship.variable.clone(),
        match_query: format!(
            "MATCH {}{} RETURN {}",
            strip_relationship_properties(pattern)?,
            predicate,
            relationship.variable
        ),
    })
}

pub(super) fn relationship_matcher_predicate(
    relationship: &RelationshipPatternWrite,
    explicit_predicate: Option<&str>,
) -> DatabaseResult<String> {
    let mut predicates = relationship
        .properties
        .iter()
        .map(|(key, value)| {
            Ok(format!(
                "{}.{key} = {}",
                relationship.variable,
                write_value_literal(value)?
            ))
        })
        .collect::<DatabaseResult<Vec<_>>>()?;
    predicates.sort();
    if let Some(predicate) = explicit_predicate {
        predicates.push(predicate.trim().to_string());
    }
    if predicates.is_empty() {
        Ok(String::new())
    } else {
        Ok(format!(" WHERE {}", predicates.join(" AND ")))
    }
}
