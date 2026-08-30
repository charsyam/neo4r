use neo4r_client::{
    Client, ClientError, HttpAdminClient, NativeTlsClientConfig, QueryParams, QueryRow, QueryValue,
    Value,
};
use std::collections::BTreeSet;
use std::fs::{self, OpenOptions};
use std::io::{self, BufRead, Write};
use std::path::PathBuf;

#[path = "../neo4r_cli_cluster.rs"]
mod neo4r_cli_cluster;

const DEFAULT_ADDRESS: &str = "127.0.0.1:7687";
const MAX_HISTORY: usize = 200;

fn main() {
    if let Err(err) = run() {
        eprintln!("{err}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), CliError> {
    let args = CliArgs::parse(std::env::args().skip(1))?;
    if args.show_help {
        println!("{}", usage());
        return Ok(());
    }
    if args.show_history {
        print_history(&args.history_path()?)?;
        return Ok(());
    }
    if args.has_admin_action() {
        execute_admin_action(&args)?;
        return Ok(());
    }

    let mut client = connect_native_client(&args)?;
    if let Some(command) = &args.command {
        let response = client.command(command)?;
        println!("{response}");
        remember_query(
            args.history_enabled,
            &args.history_path()?,
            &format!(":{command}"),
        )?;
        return Ok(());
    }
    if let Some(query) = &args.query {
        execute_query(&mut client, query)?;
        remember_query(args.history_enabled, &args.history_path()?, &query)?;
        return Ok(());
    }
    if let Some(query) = &args.plan {
        println!("{}", client.query_plan(query, &QueryParams::new())?);
        remember_query(
            args.history_enabled,
            &args.history_path()?,
            &format!(":plan {query}"),
        )?;
        return Ok(());
    }
    if let Some(query) = &args.profile {
        println!("{}", client.profile(query, &QueryParams::new())?);
        remember_query(
            args.history_enabled,
            &args.history_path()?,
            &format!(":profile {query}"),
        )?;
        return Ok(());
    }
    repl(client, args)
}

fn connect_native_client(args: &CliArgs) -> Result<Client, CliError> {
    let Some(ca_cert_path) = args.tls_ca.clone() else {
        return Ok(Client::connect(args.address.as_str())?);
    };
    Ok(Client::connect_tls(
        args.address.as_str(),
        NativeTlsClientConfig {
            server_name: args
                .tls_server_name
                .clone()
                .unwrap_or_else(|| default_tls_server_name(&args.address)),
            ca_cert_path,
            client_cert_path: args.tls_client_cert.clone(),
            client_key_path: args.tls_client_key.clone(),
        },
    )?)
}

fn default_tls_server_name(address: &str) -> String {
    address
        .rsplit_once(':')
        .map(|(host, _)| host)
        .filter(|host| !host.is_empty())
        .unwrap_or(address)
        .trim_matches(['[', ']'])
        .to_string()
}

fn repl(mut client: Client, args: CliArgs) -> Result<(), CliError> {
    let history_path = args.history_path()?;
    let stdin = io::stdin();
    let mut input = stdin.lock();
    let mut buffer = String::new();
    let mut active_tx = None;

    println!("neo4r cli connected to {}", args.address);
    println!("type :help for commands, end queries with ;");
    loop {
        print!(
            "{}",
            if buffer.is_empty() {
                "neo4r> "
            } else {
                "   ... "
            }
        );
        io::stdout().flush()?;

        let mut line = String::new();
        let bytes = input.read_line(&mut line)?;
        if bytes == 0 {
            break;
        }
        let line = line.trim_end_matches(['\r', '\n']);
        if buffer.is_empty() && line.trim_start().starts_with(':') {
            if handle_meta_command(&mut client, line.trim(), &history_path, &mut active_tx)? {
                break;
            }
            continue;
        }

        buffer.push_str(line);
        buffer.push('\n');
        if line.trim_end().ends_with(';') {
            let query = trim_query_terminator(&buffer);
            if !query.trim().is_empty() {
                match execute_repl_query(&mut client, active_tx, &query) {
                    Ok(()) => remember_query(args.history_enabled, &history_path, &query)?,
                    Err(err) => eprintln!("{err}"),
                }
            }
            buffer.clear();
        }
    }
    Ok(())
}

fn handle_meta_command(
    client: &mut Client,
    command: &str,
    history_path: &PathBuf,
    active_tx: &mut Option<u64>,
) -> Result<bool, CliError> {
    match command {
        ":q" | ":quit" | ":exit" => Ok(true),
        ":help" => {
            println!("{}", repl_help());
            Ok(false)
        }
        ":history" => {
            print_history(history_path)?;
            Ok(false)
        }
        ":ping" => {
            client.ping()?;
            println!("OK");
            Ok(false)
        }
        ":begin" => {
            if active_tx.is_some() {
                return Err(CliError::Usage("transaction already active".to_string()));
            }
            let response = client.command("BEGIN_TX")?;
            let tx_id = parse_tx_id(&response)?;
            *active_tx = Some(tx_id);
            println!("{response}");
            Ok(false)
        }
        ":commit" => {
            let tx_id = active_tx
                .take()
                .ok_or_else(|| CliError::Usage("no active transaction".to_string()))?;
            println!("{}", client.command(&format!("COMMIT_TX\t{tx_id}"))?);
            Ok(false)
        }
        ":rollback" => {
            let tx_id = active_tx
                .take()
                .ok_or_else(|| CliError::Usage("no active transaction".to_string()))?;
            println!("{}", client.command(&format!("ROLLBACK_TX\t{tx_id}"))?);
            Ok(false)
        }
        ":cluster" => {
            println!("{}", client.cluster_status()?);
            Ok(false)
        }
        ":storage" => {
            println!("{}", client.storage_status()?);
            Ok(false)
        }
        ":routing" => {
            println!("{}", client.routing_table()?);
            Ok(false)
        }
        ":replication" => {
            println!("{}", client.command("REPLICATION_STATUS")?);
            Ok(false)
        }
        ":capabilities" => {
            println!("{}", client.capabilities()?);
            Ok(false)
        }
        value if value.starts_with(":plan ") => {
            let query = value.trim_start_matches(":plan ").trim();
            if query.is_empty() {
                return Err(CliError::Usage(":plan requires a query".to_string()));
            }
            if let Some(tx_id) = active_tx {
                let response = client.command(&format!("TX_QUERY_PLAN\t{tx_id}\t{query}"))?;
                println!("{response}");
            } else {
                println!("{}", client.query_plan(query, &QueryParams::new())?);
            }
            Ok(false)
        }
        value if value.starts_with(":profile ") => {
            let query = value.trim_start_matches(":profile ").trim();
            if query.is_empty() {
                return Err(CliError::Usage(":profile requires a query".to_string()));
            }
            println!("{}", client.profile(query, &QueryParams::new())?);
            Ok(false)
        }
        value if value.starts_with(":command ") => {
            let raw = value.trim_start_matches(":command ").trim();
            if raw.is_empty() {
                return Err(CliError::Usage(":command requires a payload".to_string()));
            }
            let response = client.command(raw)?;
            println!("{response}");
            Ok(false)
        }
        value => Err(CliError::Usage(format!("unknown cli command: {value}"))),
    }
}

fn execute_query(client: &mut Client, query: &str) -> Result<(), CliError> {
    let rows = client.query(query)?;
    print_rows(&rows);
    Ok(())
}

fn execute_repl_query(
    client: &mut Client,
    active_tx: Option<u64>,
    query: &str,
) -> Result<(), CliError> {
    if let Some(tx_id) = active_tx {
        let rows = client.raw_rows_command(&format!("TX_QUERY\t{tx_id}\t{query}"))?;
        print_rows(&rows);
        return Ok(());
    }
    execute_query(client, query)
}

fn parse_tx_id(response: &str) -> Result<u64, CliError> {
    let parts = response.split('\t').collect::<Vec<_>>();
    if parts.len() >= 3 && parts[0] == "OK" && parts[1] == "TX_BEGIN" {
        return parts[2]
            .parse::<u64>()
            .map_err(|_| CliError::Usage(format!("invalid TX_BEGIN response: {response}")));
    }
    Err(CliError::Usage(format!(
        "expected TX_BEGIN response, got {response}"
    )))
}

fn execute_admin_action(args: &CliArgs) -> Result<(), CliError> {
    let admin = HttpAdminClient::connect(&args.http_host, args.http_port, &args.admin_token);
    let database = args.database.as_deref();
    if args.list_databases {
        println!("{}", admin.list_databases()?);
    }
    if let Some(name) = &args.create_database {
        println!("{}", admin.create_database(name)?);
    }
    if let Some(name) = &args.disable_database {
        println!("{}", admin.disable_database(name)?);
    }
    if let Some(name) = &args.enable_database {
        println!("{}", admin.enable_database(name)?);
    }
    if let Some(name) = &args.delete_database {
        println!("{}", admin.delete_database(name)?);
    }
    if args.list_users {
        println!("{}", admin.list_users()?);
    }
    if let Some(name) = &args.delete_user {
        println!("{}", admin.delete_user(name)?);
    }
    if args.cleanup_expired_tokens {
        println!("{}", admin.cleanup_expired_tokens()?);
    }
    if let Some(user) = &args.invoke_user {
        let token_id = args.invoke_token_id.as_deref().ok_or_else(|| {
            CliError::Usage("--invoke-user requires --invoke-token-id".to_string())
        })?;
        let token = args
            .invoke_token
            .as_deref()
            .ok_or_else(|| CliError::Usage("--invoke-user requires --invoke-token".to_string()))?;
        let expired_at = args.invoke_expired_at.as_deref().unwrap_or("4102444800");
        println!(
            "{}",
            admin.invoke_token(
                user,
                token_id,
                token,
                &args.invoke_role,
                expired_at,
                database,
                &args.invoke_database_role,
            )?
        );
    }
    if let Some(user) = &args.revoke_user {
        let token_id = args.revoke_token_id.as_deref().ok_or_else(|| {
            CliError::Usage("--revoke-user requires --revoke-token-id".to_string())
        })?;
        println!("{}", admin.revoke_token(user, token_id)?);
    }
    if let Some(user) = &args.grant_role_user {
        let token_id = args.grant_role_token_id.as_deref().ok_or_else(|| {
            CliError::Usage("--grant-role-user requires --grant-role-token-id".to_string())
        })?;
        let database = args.grant_role_database.as_deref().ok_or_else(|| {
            CliError::Usage("--grant-role-user requires --grant-role-database".to_string())
        })?;
        println!(
            "{}",
            admin.grant_role(
                user,
                token_id,
                database,
                &args.grant_role,
                args.grant_role_reason.as_deref(),
            )?
        );
    }
    if let Some(user) = &args.revoke_role_user {
        let token_id = args.revoke_role_token_id.as_deref().ok_or_else(|| {
            CliError::Usage("--revoke-role-user requires --revoke-role-token-id".to_string())
        })?;
        let database = args.revoke_role_database.as_deref().ok_or_else(|| {
            CliError::Usage("--revoke-role-user requires --revoke-role-database".to_string())
        })?;
        println!(
            "{}",
            admin.revoke_role(user, token_id, database, args.revoke_role_reason.as_deref(),)?
        );
    }
    if let Some(path) = &args.backup_path {
        println!("{}", admin.backup(path, database)?);
    }
    if let Some(path) = &args.restore_path {
        println!(
            "{}",
            admin.restore(
                path,
                args.restore_dry_run,
                args.restore_confirm.as_deref(),
                database,
            )?
        );
    }
    if let Some(target) = args.restore_pitr_target_ms {
        if let Some(confirm) = args.restore_pitr_apply_confirm.as_deref() {
            println!(
                "{}",
                admin.restore_pitr_apply(
                    target,
                    args.restore_pitr_target_logical,
                    database,
                    confirm
                )?
            );
        } else {
            println!(
                "{}",
                admin.restore_pitr_plan(target, args.restore_pitr_target_logical, database)?
            );
        }
    }
    if args.admin_raft_status {
        println!("{}", admin.raft_status(database)?);
    }
    if args.admin_audit_log {
        println!("{}", admin.audit_log()?);
    }
    if let Some(days) = args.prune_audit_retention_days {
        println!("{}", admin.prune_audit_log(days)?);
    }
    Ok(())
}

fn print_rows(rows: &[QueryRow]) {
    if rows.is_empty() {
        println!("OK 0 rows");
        return;
    }
    let columns = ordered_columns(rows);
    println!("{}", columns.join("\t"));
    for row in rows {
        let values = columns
            .iter()
            .map(|column| {
                row.get(column)
                    .map(format_query_value)
                    .unwrap_or_else(|| "null".to_string())
            })
            .collect::<Vec<_>>();
        println!("{}", values.join("\t"));
    }
    println!("OK {} rows", rows.len());
}

fn ordered_columns(rows: &[QueryRow]) -> Vec<String> {
    let mut columns = BTreeSet::new();
    for row in rows {
        for key in row.values().keys() {
            columns.insert(key.clone());
        }
    }
    columns.into_iter().collect()
}

fn format_query_value(value: &QueryValue) -> String {
    match value {
        QueryValue::Scalar(value) => format_value(value),
        QueryValue::Node(node) => format!("{node:?}"),
        QueryValue::BoundaryNode(node) => format!("{node:?}"),
        QueryValue::Relationship(relationship) => format!("{relationship:?}"),
    }
}

fn format_value(value: &Value) -> String {
    match value {
        Value::Null => "null".to_string(),
        Value::Bool(value) => value.to_string(),
        Value::Int(value) => value.to_string(),
        Value::Float(value) => value.to_string(),
        Value::String(value) => value.clone(),
        Value::Vector(value) => format!("{value:?}"),
        Value::Map(properties) => {
            let mut entries = properties.iter().collect::<Vec<_>>();
            entries.sort_by(|left, right| left.0.cmp(right.0));
            let rendered = entries
                .into_iter()
                .map(|(key, value)| format!("{key}: {}", format_value(value)))
                .collect::<Vec<_>>();
            format!("{{{}}}", rendered.join(", "))
        }
    }
}

fn print_history(path: &PathBuf) -> Result<(), CliError> {
    for (index, line) in read_history(path).into_iter().enumerate() {
        println!("{:>4}  {}", index + 1, line);
    }
    Ok(())
}

fn remember_query(enabled: bool, path: &PathBuf, query: &str) -> Result<(), CliError> {
    if !enabled {
        return Ok(());
    }
    let query = query.trim();
    if query.is_empty() {
        return Ok(());
    }
    let mut history = read_history(path);
    history.retain(|entry| entry != query);
    history.push(query.to_string());
    if history.len() > MAX_HISTORY {
        history.drain(0..history.len() - MAX_HISTORY);
    }
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(path)?;
    for entry in history {
        writeln!(file, "{entry}")?;
    }
    Ok(())
}

fn read_history(path: &PathBuf) -> Vec<String> {
    fs::read_to_string(path)
        .map(|contents| {
            contents
                .lines()
                .map(str::trim)
                .filter(|line| !line.is_empty())
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default()
}

fn trim_query_terminator(input: &str) -> String {
    input
        .trim()
        .strip_suffix(';')
        .unwrap_or(input.trim())
        .trim()
        .to_string()
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct CliArgs {
    address: String,
    tls_ca: Option<PathBuf>,
    tls_server_name: Option<String>,
    tls_client_cert: Option<PathBuf>,
    tls_client_key: Option<PathBuf>,
    query: Option<String>,
    plan: Option<String>,
    profile: Option<String>,
    command: Option<String>,
    history_file: Option<PathBuf>,
    history_enabled: bool,
    show_history: bool,
    show_help: bool,
    http_host: String,
    http_port: u16,
    admin_token: String,
    database: Option<String>,
    backup_path: Option<String>,
    restore_path: Option<String>,
    restore_dry_run: bool,
    restore_confirm: Option<String>,
    restore_pitr_target_ms: Option<u64>,
    restore_pitr_target_logical: u32,
    restore_pitr_apply_confirm: Option<String>,
    list_users: bool,
    delete_user: Option<String>,
    cleanup_expired_tokens: bool,
    list_databases: bool,
    create_database: Option<String>,
    delete_database: Option<String>,
    disable_database: Option<String>,
    enable_database: Option<String>,
    invoke_user: Option<String>,
    invoke_token_id: Option<String>,
    invoke_token: Option<String>,
    invoke_role: String,
    invoke_expired_at: Option<String>,
    invoke_database_role: String,
    revoke_user: Option<String>,
    revoke_token_id: Option<String>,
    grant_role_user: Option<String>,
    grant_role_token_id: Option<String>,
    grant_role_database: Option<String>,
    grant_role: String,
    grant_role_reason: Option<String>,
    revoke_role_user: Option<String>,
    revoke_role_token_id: Option<String>,
    revoke_role_database: Option<String>,
    revoke_role_reason: Option<String>,
    admin_raft_status: bool,
    admin_audit_log: bool,
    prune_audit_retention_days: Option<u64>,
}

impl CliArgs {
    fn parse(args: impl IntoIterator<Item = String>) -> Result<Self, CliError> {
        let normalized_args = normalize_subcommand_args(args.into_iter().collect::<Vec<_>>())?;
        let mut parsed = Self {
            address: DEFAULT_ADDRESS.to_string(),
            tls_ca: None,
            tls_server_name: None,
            tls_client_cert: None,
            tls_client_key: None,
            query: None,
            plan: None,
            profile: None,
            command: None,
            history_file: None,
            history_enabled: true,
            show_history: false,
            show_help: false,
            http_host: "127.0.0.1".to_string(),
            http_port: 17687,
            admin_token: std::env::var("NEO4R_ADMIN_TOKEN").unwrap_or_default(),
            database: None,
            backup_path: None,
            restore_path: None,
            restore_dry_run: false,
            restore_confirm: None,
            restore_pitr_target_ms: None,
            restore_pitr_target_logical: 0,
            restore_pitr_apply_confirm: None,
            list_users: false,
            delete_user: None,
            cleanup_expired_tokens: false,
            list_databases: false,
            create_database: None,
            delete_database: None,
            disable_database: None,
            enable_database: None,
            invoke_user: None,
            invoke_token_id: None,
            invoke_token: None,
            invoke_role: "reader".to_string(),
            invoke_expired_at: None,
            invoke_database_role: "reader".to_string(),
            revoke_user: None,
            revoke_token_id: None,
            grant_role_user: None,
            grant_role_token_id: None,
            grant_role_database: None,
            grant_role: "reader".to_string(),
            grant_role_reason: None,
            revoke_role_user: None,
            revoke_role_token_id: None,
            revoke_role_database: None,
            revoke_role_reason: None,
            admin_raft_status: false,
            admin_audit_log: false,
            prune_audit_retention_days: None,
        };
        let mut args = normalized_args.into_iter();
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--addr" | "--address" => parsed.address = next_arg(&mut args, &arg)?,
                "--tls-ca" => parsed.tls_ca = Some(PathBuf::from(next_arg(&mut args, &arg)?)),
                "--tls-server-name" => parsed.tls_server_name = Some(next_arg(&mut args, &arg)?),
                "--tls-client-cert" => {
                    parsed.tls_client_cert = Some(PathBuf::from(next_arg(&mut args, &arg)?))
                }
                "--tls-client-key" => {
                    parsed.tls_client_key = Some(PathBuf::from(next_arg(&mut args, &arg)?))
                }
                "--query" | "-q" => parsed.query = Some(next_arg(&mut args, &arg)?),
                "--plan" => parsed.plan = Some(next_arg(&mut args, &arg)?),
                "--profile" => parsed.profile = Some(next_arg(&mut args, &arg)?),
                "--command" | "-c" => parsed.command = Some(next_arg(&mut args, &arg)?),
                "--history-file" => {
                    parsed.history_file = Some(PathBuf::from(next_arg(&mut args, &arg)?))
                }
                "--history" => parsed.show_history = true,
                "--no-history" => parsed.history_enabled = false,
                "--http-host" => parsed.http_host = next_arg(&mut args, &arg)?,
                "--http-port" => {
                    parsed.http_port = parse_u16_arg(next_arg(&mut args, &arg)?, &arg)?
                }
                "--admin-token" => parsed.admin_token = next_arg(&mut args, &arg)?,
                "--database" | "--db" => parsed.database = Some(next_arg(&mut args, &arg)?),
                "--backup" => parsed.backup_path = Some(next_arg(&mut args, &arg)?),
                "--restore" => parsed.restore_path = Some(next_arg(&mut args, &arg)?),
                "--restore-dry-run" => parsed.restore_dry_run = true,
                "--restore-confirm" => parsed.restore_confirm = Some(next_arg(&mut args, &arg)?),
                "--restore-pitr-target-ms" => {
                    parsed.restore_pitr_target_ms =
                        Some(parse_u64_arg(next_arg(&mut args, &arg)?, &arg)?)
                }
                "--restore-pitr-target-logical" => {
                    parsed.restore_pitr_target_logical =
                        parse_u64_arg(next_arg(&mut args, &arg)?, &arg)? as u32
                }
                "--restore-pitr-apply-confirm" => {
                    parsed.restore_pitr_apply_confirm = Some(next_arg(&mut args, &arg)?)
                }
                "--list-users" => parsed.list_users = true,
                "--delete-user" => parsed.delete_user = Some(next_arg(&mut args, &arg)?),
                "--cleanup-expired-tokens" => parsed.cleanup_expired_tokens = true,
                "--list-databases" => parsed.list_databases = true,
                "--create-database" => parsed.create_database = Some(next_arg(&mut args, &arg)?),
                "--delete-database" => parsed.delete_database = Some(next_arg(&mut args, &arg)?),
                "--disable-database" => parsed.disable_database = Some(next_arg(&mut args, &arg)?),
                "--enable-database" => parsed.enable_database = Some(next_arg(&mut args, &arg)?),
                "--invoke-user" => parsed.invoke_user = Some(next_arg(&mut args, &arg)?),
                "--invoke-token-id" => parsed.invoke_token_id = Some(next_arg(&mut args, &arg)?),
                "--invoke-token" => parsed.invoke_token = Some(next_arg(&mut args, &arg)?),
                "--invoke-role" => parsed.invoke_role = next_arg(&mut args, &arg)?,
                "--invoke-expired-at" => {
                    parsed.invoke_expired_at = Some(next_arg(&mut args, &arg)?)
                }
                "--invoke-database-role" => {
                    parsed.invoke_database_role = next_arg(&mut args, &arg)?
                }
                "--revoke-user" => parsed.revoke_user = Some(next_arg(&mut args, &arg)?),
                "--revoke-token-id" => parsed.revoke_token_id = Some(next_arg(&mut args, &arg)?),
                "--grant-role-user" => parsed.grant_role_user = Some(next_arg(&mut args, &arg)?),
                "--grant-role-token-id" => {
                    parsed.grant_role_token_id = Some(next_arg(&mut args, &arg)?)
                }
                "--grant-role-database" => {
                    parsed.grant_role_database = Some(next_arg(&mut args, &arg)?)
                }
                "--grant-role" => parsed.grant_role = next_arg(&mut args, &arg)?,
                "--grant-role-reason" => {
                    parsed.grant_role_reason = Some(next_arg(&mut args, &arg)?)
                }
                "--revoke-role-user" => parsed.revoke_role_user = Some(next_arg(&mut args, &arg)?),
                "--revoke-role-token-id" => {
                    parsed.revoke_role_token_id = Some(next_arg(&mut args, &arg)?)
                }
                "--revoke-role-database" => {
                    parsed.revoke_role_database = Some(next_arg(&mut args, &arg)?)
                }
                "--revoke-role-reason" => {
                    parsed.revoke_role_reason = Some(next_arg(&mut args, &arg)?)
                }
                "--admin-raft-status" => parsed.admin_raft_status = true,
                "--admin-audit-log" => parsed.admin_audit_log = true,
                "--prune-audit-retention-days" => {
                    parsed.prune_audit_retention_days =
                        Some(parse_u64_arg(next_arg(&mut args, &arg)?, &arg)?)
                }
                "--help" | "-h" => parsed.show_help = true,
                value => return Err(CliError::Usage(format!("unknown argument: {value}"))),
            }
        }
        let native_actions = usize::from(parsed.query.is_some())
            + usize::from(parsed.plan.is_some())
            + usize::from(parsed.profile.is_some())
            + usize::from(parsed.command.is_some());
        if native_actions > 1 {
            return Err(CliError::Usage(
                "--query, --plan, --profile, and --command cannot be combined".to_string(),
            ));
        }
        if parsed.has_admin_action() && native_actions > 0 {
            return Err(CliError::Usage(
                "admin HTTP actions cannot be combined with native query actions".to_string(),
            ));
        }
        if parsed.tls_client_cert.is_some() != parsed.tls_client_key.is_some() {
            return Err(CliError::Usage(
                "--tls-client-cert and --tls-client-key must be provided together".to_string(),
            ));
        }
        if parsed.restore_path.is_some()
            && !parsed.restore_dry_run
            && parsed.restore_confirm.as_deref() != Some("RESTORE")
        {
            return Err(CliError::Usage(
                "--restore requires --restore-confirm RESTORE unless --restore-dry-run is set"
                    .to_string(),
            ));
        }
        Ok(parsed)
    }

    fn has_admin_action(&self) -> bool {
        self.backup_path.is_some()
            || self.restore_path.is_some()
            || self.restore_pitr_target_ms.is_some()
            || self.list_users
            || self.delete_user.is_some()
            || self.cleanup_expired_tokens
            || self.list_databases
            || self.create_database.is_some()
            || self.delete_database.is_some()
            || self.disable_database.is_some()
            || self.enable_database.is_some()
            || self.invoke_user.is_some()
            || self.revoke_user.is_some()
            || self.grant_role_user.is_some()
            || self.revoke_role_user.is_some()
            || self.admin_raft_status
            || self.admin_audit_log
            || self.prune_audit_retention_days.is_some()
    }

    fn history_path(&self) -> Result<PathBuf, CliError> {
        if let Some(path) = &self.history_file {
            return Ok(path.clone());
        }
        if let Ok(path) = std::env::var("NEO4R_HISTORY") {
            return Ok(PathBuf::from(path));
        }
        let home = std::env::var("HOME")
            .map_err(|_| CliError::Usage("HOME is not set; pass --history-file".to_string()))?;
        Ok(PathBuf::from(home).join(".neo4r_history"))
    }
}

fn normalize_subcommand_args(args: Vec<String>) -> Result<Vec<String>, CliError> {
    let Some(first) = args.first().map(String::as_str) else {
        return Ok(args);
    };
    if first.starts_with('-') {
        return Ok(args);
    }
    match first {
        "query" => subcommand_with_payload("--query", &args),
        "plan" => subcommand_with_payload("--plan", &args),
        "profile" => subcommand_with_payload("--profile", &args),
        "command" => subcommand_with_payload("--command", &args),
        "cluster" => neo4r_cli_cluster::normalize_cluster_subcommand(&args),
        "backup" => normalize_backup_subcommand(&args),
        "admin" => normalize_admin_subcommand(&args),
        _ => Ok(args),
    }
}

fn subcommand_with_payload(flag: &str, args: &[String]) -> Result<Vec<String>, CliError> {
    if args.len() < 2 {
        return Err(CliError::Usage(format!("{} requires a payload", args[0])));
    }
    let mut normalized = vec![flag.to_string(), args[1].clone()];
    normalized.extend_from_slice(&args[2..]);
    Ok(normalized)
}

fn normalize_backup_subcommand(args: &[String]) -> Result<Vec<String>, CliError> {
    match args.get(1).map(String::as_str) {
        Some("create") if args.len() >= 3 => {
            let mut normalized = vec!["--backup".to_string(), args[2].clone()];
            normalized.extend_from_slice(&args[3..]);
            Ok(normalized)
        }
        Some("restore") if args.len() >= 3 => {
            let mut normalized = vec!["--restore".to_string(), args[2].clone()];
            normalized.extend_from_slice(&args[3..]);
            Ok(normalized)
        }
        _ => Err(CliError::Usage(
            "backup subcommand supports: create PATH, restore PATH".to_string(),
        )),
    }
}

fn normalize_admin_subcommand(args: &[String]) -> Result<Vec<String>, CliError> {
    match args.get(1).map(String::as_str) {
        Some("users") => Ok(with_tail(vec!["--list-users".to_string()], args, 2)),
        Some("databases") => Ok(with_tail(vec!["--list-databases".to_string()], args, 2)),
        Some("audit") => Ok(with_tail(vec!["--admin-audit-log".to_string()], args, 2)),
        Some("raft") => Ok(with_tail(vec!["--admin-raft-status".to_string()], args, 2)),
        Some("restore-pitr") if args.len() >= 3 => {
            let mut normalized = vec!["--restore-pitr-target-ms".to_string(), args[2].clone()];
            normalized.extend_from_slice(&args[3..]);
            Ok(normalized)
        }
        Some("restore-pitr-apply") if args.len() >= 3 => {
            let mut normalized = vec![
                "--restore-pitr-target-ms".to_string(),
                args[2].clone(),
                "--restore-pitr-apply-confirm".to_string(),
                "RESTORE_PITR".to_string(),
            ];
            normalized.extend_from_slice(&args[3..]);
            Ok(normalized)
        }
        Some("grant-role") if args.len() >= 6 => {
            let mut normalized = vec![
                "--grant-role-user".to_string(),
                args[2].clone(),
                "--grant-role-token-id".to_string(),
                args[3].clone(),
                "--grant-role-database".to_string(),
                args[4].clone(),
                "--grant-role".to_string(),
                args[5].clone(),
            ];
            normalized.extend_from_slice(&args[6..]);
            Ok(normalized)
        }
        Some("revoke-role") if args.len() >= 5 => {
            let mut normalized = vec![
                "--revoke-role-user".to_string(),
                args[2].clone(),
                "--revoke-role-token-id".to_string(),
                args[3].clone(),
                "--revoke-role-database".to_string(),
                args[4].clone(),
            ];
            normalized.extend_from_slice(&args[5..]);
            Ok(normalized)
        }
        Some("cleanup-tokens") => Ok(with_tail(
            vec!["--cleanup-expired-tokens".to_string()],
            args,
            2,
        )),
        Some("prune-audit") if args.len() >= 3 => {
            let mut normalized = vec![
                "--prune-audit-retention-days".to_string(),
                args[2].clone(),
            ];
            normalized.extend_from_slice(&args[3..]);
            Ok(normalized)
        }
        _ => Err(CliError::Usage(
            "admin subcommand supports: users, databases, audit, raft, restore-pitr TARGET_MS, restore-pitr-apply TARGET_MS, grant-role USER TOKEN_ID DB ROLE, revoke-role USER TOKEN_ID DB, cleanup-tokens, prune-audit DAYS".to_string(),
        )),
    }
}

fn with_tail(mut normalized: Vec<String>, args: &[String], start: usize) -> Vec<String> {
    normalized.extend_from_slice(&args[start..]);
    normalized
}

fn next_arg(args: &mut impl Iterator<Item = String>, name: &str) -> Result<String, CliError> {
    args.next()
        .ok_or_else(|| CliError::Usage(format!("{name} requires a value")))
}

fn parse_u16_arg(value: String, name: &str) -> Result<u16, CliError> {
    value
        .parse::<u16>()
        .map_err(|_| CliError::Usage(format!("{name} requires a valid u16 value")))
}

fn parse_u64_arg(value: String, name: &str) -> Result<u64, CliError> {
    value
        .parse::<u64>()
        .map_err(|_| CliError::Usage(format!("{name} requires a valid u64 value")))
}

fn usage() -> &'static str {
    "usage: neo4r-cli [--addr ADDR] [--tls-ca CA.pem] [--tls-server-name NAME] [--tls-client-cert CERT.pem --tls-client-key KEY.pem] [--query CYPHER|--plan CYPHER|--profile CYPHER|--command COMMAND] [--history] [--history-file PATH] [--no-history]
       neo4r-cli [--http-host HOST] [--http-port PORT] [--admin-token TOKEN] [--database DB] [--backup PATH|--restore PATH|--list-users|--invoke-user USER|--list-databases]
       neo4r-cli query|plan|profile|command PAYLOAD
       neo4r-cli cluster status|topology|reconcile [LIMIT]|chaos|gossip SERVER_ID QUERY_ADDR REPL_ADDR INCARNATION TTL_MS [TOKEN]|gossip-list|gossip-refresh|promote SERVER_ID|bootstrap-manifest MODE CLUSTER_ID DB_ID|bootstrap-safety EXPECTED_CLUSTER_ID FORCE|safety OPERATION [TOKEN]
       neo4r-cli admin users|databases|audit|raft|restore-pitr TARGET_MS|restore-pitr-apply TARGET_MS|grant-role USER TOKEN_ID DB ROLE|revoke-role USER TOKEN_ID DB|cleanup-tokens|prune-audit DAYS
       neo4r-cli backup create|restore PATH"
}

fn repl_help() -> &'static str {
    "commands:
  :help                 show this help
  :history              show query history
  :ping                 ping server
  :begin                begin a snapshot transaction
  :commit               commit the active transaction
  :rollback             rollback the active transaction
  :plan QUERY           show the query plan
  :profile QUERY        profile a query
  :cluster              show cluster status
  :storage              show storage status
  :routing              show routing table
  :replication          show replication status
  :capabilities         show server capabilities
  :command COMMAND      send a raw server command
  :quit                 exit

queries:
  End a Cypher query with ; to execute it. Inside :begin/:commit, queries run in the active transaction."
}

#[derive(Debug)]
enum CliError {
    Usage(String),
    Io(io::Error),
    Client(ClientError),
}

impl std::fmt::Display for CliError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Usage(message) => write!(formatter, "{message}\n{}", usage()),
            Self::Io(err) => write!(formatter, "{err}"),
            Self::Client(err) => write!(formatter, "{err}"),
        }
    }
}

impl std::error::Error for CliError {}

impl From<io::Error> for CliError {
    fn from(err: io::Error) -> Self {
        Self::Io(err)
    }
}

impl From<ClientError> for CliError {
    fn from(err: ClientError) -> Self {
        Self::Client(err)
    }
}

#[cfg(test)]
#[path = "../neo4r_cli_tests.rs"]
mod neo4r_cli_tests;
