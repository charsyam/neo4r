use neo4r_core::{Command, GraphState, Properties, Value};
use neo4r_query::{CypherEngine, QueryEngine, QueryRow, QueryValue};

fn main() {
    let graph = graph();
    let engine = CypherEngine::new();
    let queries = [
        "MATCH (n:Person) RETURN n.name",
        r#"MATCH (n:Person) WHERE n.name = "Alice" RETURN n.name"#,
        "MATCH (n:Person) WHERE n.active = true RETURN n.name",
        r#"MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE a.name = "Alice" RETURN b.name"#,
        r#"MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE b.name = "Bob" RETURN a.name, b.name"#,
        r#"MATCH (a:Person)-[:WORKS_WITH]->(b:Person) WHERE b.status = "active" RETURN b.name"#,
        r#"MATCH (a:Person)-[:LIKES]->(b:Person) RETURN b.name"#,
    ];

    for query in queries {
        println!("query: {query}");
        let rows = engine.execute(&graph, query).expect("query should execute");
        print_rows(&rows);
        println!();
    }
}

fn print_rows(rows: &[QueryRow]) {
    println!("rows: {}", rows.len());
    let mut rendered = rows.iter().map(render_row).collect::<Vec<_>>();
    rendered.sort();
    for (index, values) in rendered.iter().enumerate() {
        println!("  {}: {values}", index + 1);
    }
}

fn render_row(row: &QueryRow) -> String {
    let mut values = row.values().iter().collect::<Vec<_>>();
    values.sort_by(|(left, _), (right, _)| left.cmp(right));
    values
        .into_iter()
        .map(|(name, value)| format!("{name} = {}", display_value(value)))
        .collect::<Vec<_>>()
        .join(", ")
}

fn display_value(value: &QueryValue) -> String {
    match value {
        QueryValue::Node(node) => format!("Node({})", node.id),
        QueryValue::BoundaryNode(node) => format!(
            "BoundaryNode({}, owner_shard={}, version={})",
            node.id, node.owner_shard, node.version
        ),
        QueryValue::Relationship(relationship) => format!("Relationship({})", relationship.id),
        QueryValue::Scalar(value) => format!("{value:?}"),
    }
}

fn graph() -> GraphState {
    let mut graph = GraphState::new();
    graph
        .apply(Command::CreateNode {
            id: 1,
            labels: vec!["Person".to_string()],
            properties: properties(&[
                ("name", Value::String("Alice".to_string())),
                ("active", Value::Bool(true)),
                ("age", Value::Int(30)),
            ]),
        })
        .unwrap();
    graph
        .apply(Command::CreateNode {
            id: 2,
            labels: vec!["Person".to_string()],
            properties: properties(&[
                ("name", Value::String("Bob".to_string())),
                ("active", Value::Bool(false)),
                ("age", Value::Int(31)),
            ]),
        })
        .unwrap();
    graph
        .apply(Command::CreateNode {
            id: 3,
            labels: vec!["Company".to_string()],
            properties: properties(&[("name", Value::String("Acme".to_string()))]),
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
        .apply(Command::UpsertBoundaryNode {
            id: 20,
            owner_shard: 2,
            labels: vec!["Person".to_string()],
            properties: properties(&[
                ("name", Value::String("RemoteCarol".to_string())),
                ("status", Value::String("active".to_string())),
            ]),
            version: 7,
        })
        .unwrap();
    graph
        .apply(Command::CreateRelationship {
            id: 2,
            from: 1,
            to: 20,
            rel_type: "WORKS_WITH".to_string(),
            properties: Properties::new(),
        })
        .unwrap();
    graph
}

fn properties(entries: &[(&str, Value)]) -> Properties {
    entries
        .iter()
        .map(|(key, value)| ((*key).to_string(), value.clone()))
        .collect()
}
