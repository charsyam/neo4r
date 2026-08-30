use super::json_escape;

pub(crate) fn prometheus_metric(name: &str, value: u64) -> String {
    format!("# TYPE {name} gauge\n{name} {value}\n")
}

pub(crate) fn prometheus_database_metric(name: &str, database_name: &str, value: u64) -> String {
    format!(
        "# TYPE {name} gauge\n{name}{{database=\"{}\"}} {value}\n",
        json_escape(database_name)
    )
}

pub(crate) fn prometheus_shard_metric(
    name: &str,
    database_name: &str,
    shard_id: u64,
    server_id: u64,
    role: &str,
    value: u64,
) -> String {
    format!(
        "# TYPE {name} gauge\n{name}{{database=\"{}\",shard=\"{}\",server=\"{}\",role=\"{}\"}} {value}\n",
        json_escape(database_name),
        shard_id,
        server_id,
        json_escape(role)
    )
}
