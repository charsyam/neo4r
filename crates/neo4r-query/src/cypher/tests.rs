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
