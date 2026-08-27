use neo4r_client::{Client, ClientError, QueryParams, QueryRow, QueryValue, Value};

fn main() -> Result<(), ClientError> {
    let address = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:17687".to_string());
    let mut client = match Client::connect(&address) {
        Ok(client) => client,
        Err(err) => {
            eprintln!("failed to connect to neo4r at {address}: {err}");
            eprintln!(
                "start a local server first:\n  cargo run -p neo4r-server -- --bind {address} --data-dir /tmp/neo4r-rust-sdk-example --shards 1 --partitions 1"
            );
            return Err(err);
        }
    };

    client.ping()?;

    let mut params = QueryParams::new();
    params.insert("name".to_string(), Value::String("Alice".to_string()));
    params.insert("age".to_string(), Value::Int(42));
    let rows = client.execute_with_params(
        "CREATE (n:Person {name: $name, age: $age}) RETURN n.name, n.age",
        &params,
    )?;
    println!(
        "created: {} {}",
        scalar_string(&rows[0], "n.name")?,
        scalar_int(&rows[0], "n.age")?
    );

    let mut params = QueryParams::new();
    params.insert("name".to_string(), Value::String("Alice".to_string()));
    let rows = client.query_with_params(
        "MATCH (n:Person) WHERE n.name = $name RETURN n.name, n.age",
        &params,
    )?;
    println!(
        "matched: {} {}",
        scalar_string(&rows[0], "n.name")?,
        scalar_int(&rows[0], "n.age")?
    );

    let profile = client.profile("MATCH (n:Person) RETURN n", &QueryParams::new())?;
    println!("profile: {profile}");

    let plan = client.query_plan("MATCH (n:Person) RETURN n", &QueryParams::new())?;
    println!("query_plan: {plan}");

    let status = client.storage_status()?;
    println!("storage_status: {status}");

    let cluster = client.cluster_status()?;
    println!("cluster_status: {cluster}");

    client.close()
}

fn scalar_string(row: &QueryRow, key: &str) -> Result<String, ClientError> {
    match row.get(key) {
        Some(QueryValue::Scalar(Value::String(value))) => Ok(value.clone()),
        value => Err(ClientError::Protocol(format!(
            "{key} is not a string scalar: {value:?}"
        ))),
    }
}

fn scalar_int(row: &QueryRow, key: &str) -> Result<i64, ClientError> {
    match row.get(key) {
        Some(QueryValue::Scalar(Value::Int(value))) => Ok(*value),
        value => Err(ClientError::Protocol(format!(
            "{key} is not an int scalar: {value:?}"
        ))),
    }
}
