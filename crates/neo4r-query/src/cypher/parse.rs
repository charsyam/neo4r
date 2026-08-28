use super::*;

pub(super) fn parse(input: &str, params: &QueryParams) -> QueryResult<Query> {
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

pub(super) fn find_keyword(input: &str, keyword: &str) -> Option<usize> {
    input
        .to_ascii_uppercase()
        .find(&keyword.to_ascii_uppercase())
}

pub(super) fn starts_with_keyword(input: &str, keyword: &str) -> bool {
    let input = input.trim_start();
    let upper = input.to_ascii_uppercase();
    let keyword = keyword.to_ascii_uppercase();
    upper.starts_with(&keyword)
        && input
            .as_bytes()
            .get(keyword.len())
            .is_none_or(|byte| byte.is_ascii_whitespace())
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
