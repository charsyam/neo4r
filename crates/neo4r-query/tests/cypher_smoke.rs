use neo4r_core::{Command, GraphState, Properties, Value};
use neo4r_query::{
    classify_statement, classify_write_statement, CypherEngine, CypherStatementKind,
    LogicalOperator, Pattern, PhysicalOperator, QueryEngine, QueryParams, QueryValue, VariableKind,
    WriteStatementKind,
};
use neo4r_storage::{KvGraphStore, MemoryKvStore, RocksKvStore};
use std::fs;
use std::path::PathBuf;

#[test]
fn returns_all_nodes_without_label_filter() {
    let rows = execute("MATCH (n) RETURN n");

    assert_eq!(rows.len(), 4);
}

#[test]
fn filters_nodes_by_label_and_string_property() {
    let rows = execute(r#"MATCH (n:Person) WHERE n.name = "Alice" RETURN n.name"#);

    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get("n.name"),
        Some(&QueryValue::Scalar(Value::String("Alice".to_string())))
    );
}

#[test]
fn filters_nodes_by_bool_property() {
    let rows = execute("MATCH (n:Person) WHERE n.active = true RETURN n.name");

    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get("n.name"),
        Some(&QueryValue::Scalar(Value::String("Alice".to_string())))
    );
}

#[test]
fn filters_nodes_by_int_property() {
    let rows = execute("MATCH (n:Person) WHERE n.age = 30 RETURN n.name");

    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get("n.name"),
        Some(&QueryValue::Scalar(Value::String("Alice".to_string())))
    );
}

#[test]
fn filters_nodes_by_parameterized_property() {
    let mut params = QueryParams::new();
    params.insert("name".to_string(), Value::String("Alice".to_string()));
    let rows = CypherEngine::new()
        .execute_with_params(
            &graph(),
            "MATCH (n:Person) WHERE n.name = $name RETURN n.name",
            &params,
        )
        .unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get("n.name"),
        Some(&QueryValue::Scalar(Value::String("Alice".to_string())))
    );
}

#[test]
fn exposes_parsed_semantic_logical_and_physical_plans() {
    let engine = CypherEngine::new();
    let params = QueryParams::new();
    let parsed = engine
        .parse(
            "MATCH (a:Person)-[r:KNOWS]->(b:Person) WHERE a.name = \"Alice\" RETURN b.name ORDER BY b.name LIMIT 1",
            &params,
        )
        .unwrap();
    assert!(matches!(parsed.pattern, Pattern::Outgoing { .. }));

    let semantic = engine
        .analyze(
            "MATCH (a:Person)-[r:KNOWS]->(b:Person) WHERE a.name = \"Alice\" RETURN b.name",
            &params,
        )
        .unwrap();
    assert_eq!(semantic.bound_variables[0].name, "a");
    assert_eq!(semantic.bound_variables[0].kind, VariableKind::Node);
    assert_eq!(semantic.bound_variables[1].name, "r");
    assert_eq!(semantic.bound_variables[1].kind, VariableKind::Relationship);

    let logical = engine
        .logical_plan(
            "MATCH (n:Person) WHERE n.name = \"Alice\" RETURN n.name",
            &params,
        )
        .unwrap();
    assert!(matches!(logical.root, LogicalOperator::Project { .. }));

    let physical = engine
        .physical_plan(
            "MATCH (n:Person) WHERE n.name = \"Alice\" RETURN n.name",
            &params,
        )
        .unwrap();
    assert!(matches!(
        physical.root,
        PhysicalOperator::Materialize { .. }
    ));

    let physical = engine
        .physical_plan(
            "MATCH (n:Person) WHERE n.age >= 18 RETURN n.status, count(*) ORDER BY count(*) DESC LIMIT 5",
            &params,
        )
        .unwrap();
    let names = physical.operator_names();
    assert!(names.contains(&"Materialize"));
    assert!(names.contains(&"Top"));
    assert!(names.contains(&"Sort"));
    assert!(names.contains(&"HashAggregate"));
    assert!(names.contains(&"PredicateFilter"));
    assert!(physical.operator_count() >= 5);
}

#[test]
fn classifies_read_write_and_ddl_statements() {
    assert_eq!(
        classify_statement("MATCH (n) RETURN n").unwrap(),
        Some(CypherStatementKind::Read)
    );
    assert_eq!(
        classify_write_statement("MATCH (n:Person) SET n.name = \"Alice\"").unwrap(),
        Some(WriteStatementKind::MatchSet)
    );
    assert_eq!(
        classify_write_statement("CREATE INDEX person_name FOR (n:Person) ON (n.name)").unwrap(),
        Some(WriteStatementKind::CreateIndex)
    );
    assert_eq!(
        classify_write_statement("MATCH (n) RETURN n").unwrap(),
        None
    );
}

#[test]
fn rejects_unbound_variables_before_execution() {
    let err = CypherEngine::new()
        .execute(&graph(), "MATCH (n:Person) RETURN m.name")
        .unwrap_err();

    assert!(err.to_string().contains("variable \"m\" is not bound"));
}

#[test]
fn filters_nodes_by_and_predicate_with_params() {
    let mut params = QueryParams::new();
    params.insert("name".to_string(), Value::String("Alice".to_string()));
    params.insert("age".to_string(), Value::Int(30));
    let rows = CypherEngine::new()
        .execute_with_params(
            &graph(),
            "MATCH (n:Person) WHERE n.name = $name AND n.age = $age RETURN n.name",
            &params,
        )
        .unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get("n.name"),
        Some(&QueryValue::Scalar(Value::String("Alice".to_string())))
    );
}

#[test]
fn finds_nodes_by_vector_knn() {
    let rows =
        execute("MATCH (n:Person) WHERE vector.knn(n.embedding, [1.0, 0.0], 2) RETURN n.name");

    assert_eq!(rows.len(), 2);
    assert_eq!(
        rows[0].get("n.name"),
        Some(&QueryValue::Scalar(Value::String("Alice".to_string())))
    );
    assert_eq!(
        rows[1].get("n.name"),
        Some(&QueryValue::Scalar(Value::String("Bob".to_string())))
    );
}

#[test]
fn finds_nodes_by_parameterized_vector_knn() {
    let mut params = QueryParams::new();
    params.insert("embedding".to_string(), Value::Vector(vec![0.7, 0.2]));
    params.insert("k".to_string(), Value::Int(1));
    params.insert("metric".to_string(), Value::String("l2".to_string()));
    let rows = CypherEngine::new()
        .execute_with_params(
            &graph(),
            "MATCH (n:Person) WHERE vector.knn(n.embedding, $embedding, $k, $metric) RETURN n.name",
            &params,
        )
        .unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get("n.name"),
        Some(&QueryValue::Scalar(Value::String("Bob".to_string())))
    );
}

#[test]
fn finds_nodes_by_vector_knn_with_l2_metric() {
    let rows = execute(
        r#"MATCH (n:Person) WHERE vector.knn(n.embedding, [0.7, 0.2], 1, "l2") RETURN n.name"#,
    );

    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get("n.name"),
        Some(&QueryValue::Scalar(Value::String("Bob".to_string())))
    );
}

#[test]
fn matches_outgoing_relationship_and_returns_target_property() {
    let rows =
        execute(r#"MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE a.name = "Alice" RETURN b.name"#);

    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get("b.name"),
        Some(&QueryValue::Scalar(Value::String("Bob".to_string())))
    );
}

#[test]
fn uses_boundary_node_for_remote_target_property() {
    let mut graph = GraphState::new();
    graph
        .apply(Command::CreateNode {
            id: 1,
            labels: vec!["Person".to_string()],
            properties: properties(&[("name", Value::String("Alice".to_string()))]),
        })
        .unwrap();
    graph
        .apply(Command::UpsertBoundaryNode {
            id: 20,
            owner_shard: 2,
            labels: vec!["Person".to_string()],
            properties: properties(&[
                ("name", Value::String("RemoteBob".to_string())),
                ("status", Value::String("active".to_string())),
            ]),
            version: 7,
        })
        .unwrap();
    graph
        .apply(Command::CreateRelationship {
            id: 99,
            from: 1,
            to: 20,
            rel_type: "KNOWS".to_string(),
            properties: Properties::new(),
        })
        .unwrap();

    let rows = CypherEngine::new()
        .execute(
            &graph,
            r#"MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE b.status = "active" RETURN b.name"#,
        )
        .unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get("b.name"),
        Some(&QueryValue::Scalar(Value::String("RemoteBob".to_string())))
    );
}

#[test]
fn supports_predicate_on_target_node() {
    let rows = execute(
        r#"MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE b.name = "Bob" RETURN a.name, b.name"#,
    );

    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get("a.name"),
        Some(&QueryValue::Scalar(Value::String("Alice".to_string())))
    );
    assert_eq!(
        rows[0].get("b.name"),
        Some(&QueryValue::Scalar(Value::String("Bob".to_string())))
    );
}

#[test]
fn filters_relationship_pattern_by_and_predicate() {
    let rows = execute(
        r#"MATCH (a:Person)-[r:KNOWS]->(b:Person) WHERE a.name = "Alice" AND r.since = 2020 AND b.name = "Bob" RETURN a.name, b.name"#,
    );

    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get("a.name"),
        Some(&QueryValue::Scalar(Value::String("Alice".to_string())))
    );
    assert_eq!(
        rows[0].get("b.name"),
        Some(&QueryValue::Scalar(Value::String("Bob".to_string())))
    );
}

#[test]
fn returns_empty_rows_for_missing_relationship_type() {
    let rows = execute(r#"MATCH (a:Person)-[:LIKES]->(b:Person) RETURN b.name"#);

    assert!(rows.is_empty());
}

#[test]
fn same_queries_run_against_kv_graph_store() {
    let store = kv_graph();

    let rows = CypherEngine::new()
        .execute(
            &store,
            r#"MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE a.name = "Alice" RETURN b.name"#,
        )
        .unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get("b.name"),
        Some(&QueryValue::Scalar(Value::String("Bob".to_string())))
    );
}

#[test]
fn same_queries_run_against_rocksdb_graph_store() {
    let path = temp_rocks_path("cypher-smoke");
    let rows = {
        let mut store = KvGraphStore::new(RocksKvStore::open(&path).unwrap());
        apply_commands(&mut store);

        CypherEngine::new()
            .execute(
                &store,
                r#"MATCH (n:Person) WHERE n.name = "Alice" RETURN n.name"#,
            )
            .unwrap()
    };
    let _ = fs::remove_dir_all(&path);

    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get("n.name"),
        Some(&QueryValue::Scalar(Value::String("Alice".to_string())))
    );
}

fn execute(query: &str) -> Vec<neo4r_query::QueryRow> {
    CypherEngine::new().execute(&graph(), query).unwrap()
}

fn graph() -> GraphState {
    let mut graph = GraphState::new();
    for command in commands() {
        graph.apply(command).unwrap();
    }
    graph
}

fn kv_graph() -> KvGraphStore<MemoryKvStore> {
    let mut store = KvGraphStore::new(MemoryKvStore::new());
    apply_commands(&mut store);
    store
}

fn apply_commands<KV: neo4r_storage::KeyValueStore>(store: &mut KvGraphStore<KV>) {
    for command in commands() {
        store.apply(&command).unwrap();
    }
}

fn commands() -> Vec<Command> {
    vec![
        Command::CreateNode {
            id: 1,
            labels: vec!["Person".to_string()],
            properties: properties(&[
                ("name", Value::String("Alice".to_string())),
                ("active", Value::Bool(true)),
                ("age", Value::Int(30)),
                ("embedding", Value::Vector(vec![1.0, 0.0])),
            ]),
        },
        Command::CreateNode {
            id: 2,
            labels: vec!["Person".to_string()],
            properties: properties(&[
                ("name", Value::String("Bob".to_string())),
                ("active", Value::Bool(false)),
                ("age", Value::Int(31)),
                ("embedding", Value::Vector(vec![0.8, 0.2])),
            ]),
        },
        Command::CreateNode {
            id: 3,
            labels: vec!["Company".to_string()],
            properties: properties(&[
                ("name", Value::String("Acme".to_string())),
                ("embedding", Value::Vector(vec![0.0, 1.0])),
            ]),
        },
        Command::CreateNode {
            id: 4,
            labels: vec!["City".to_string()],
            properties: properties(&[("name", Value::String("Seoul".to_string()))]),
        },
        Command::CreateRelationship {
            id: 1,
            from: 1,
            to: 2,
            rel_type: "KNOWS".to_string(),
            properties: properties(&[("since", Value::Int(2020))]),
        },
    ]
}

fn temp_rocks_path(name: &str) -> PathBuf {
    let mut path = std::env::temp_dir();
    path.push(format!(
        "neo4r-{name}-{}-{}",
        std::process::id(),
        std::thread::current().name().unwrap_or("test")
    ));
    let _ = fs::remove_dir_all(&path);
    path
}

fn properties(entries: &[(&str, Value)]) -> Properties {
    entries
        .iter()
        .map(|(key, value)| ((*key).to_string(), value.clone()))
        .collect()
}
