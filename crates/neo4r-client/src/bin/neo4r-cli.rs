use neo4r_client::{Client, ClientError, QueryRow, QueryValue, Value};
use std::collections::BTreeSet;
use std::fs::{self, OpenOptions};
use std::io::{self, BufRead, Write};
use std::path::PathBuf;

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

    let mut client = Client::connect(args.address.as_str())?;
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
    repl(client, args)
}

fn repl(mut client: Client, args: CliArgs) -> Result<(), CliError> {
    let history_path = args.history_path()?;
    let stdin = io::stdin();
    let mut input = stdin.lock();
    let mut buffer = String::new();

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
            if handle_meta_command(&mut client, line.trim(), &history_path)? {
                break;
            }
            continue;
        }

        buffer.push_str(line);
        buffer.push('\n');
        if line.trim_end().ends_with(';') {
            let query = trim_query_terminator(&buffer);
            if !query.trim().is_empty() {
                match execute_query(&mut client, &query) {
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
    query: Option<String>,
    command: Option<String>,
    history_file: Option<PathBuf>,
    history_enabled: bool,
    show_history: bool,
    show_help: bool,
}

impl CliArgs {
    fn parse(args: impl IntoIterator<Item = String>) -> Result<Self, CliError> {
        let mut parsed = Self {
            address: DEFAULT_ADDRESS.to_string(),
            query: None,
            command: None,
            history_file: None,
            history_enabled: true,
            show_history: false,
            show_help: false,
        };
        let mut args = args.into_iter();
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--addr" | "--address" => parsed.address = next_arg(&mut args, &arg)?,
                "--query" | "-q" => parsed.query = Some(next_arg(&mut args, &arg)?),
                "--command" | "-c" => parsed.command = Some(next_arg(&mut args, &arg)?),
                "--history-file" => {
                    parsed.history_file = Some(PathBuf::from(next_arg(&mut args, &arg)?))
                }
                "--history" => parsed.show_history = true,
                "--no-history" => parsed.history_enabled = false,
                "--help" | "-h" => parsed.show_help = true,
                value => return Err(CliError::Usage(format!("unknown argument: {value}"))),
            }
        }
        if parsed.query.is_some() && parsed.command.is_some() {
            return Err(CliError::Usage(
                "--query and --command cannot be used together".to_string(),
            ));
        }
        Ok(parsed)
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

fn next_arg(args: &mut impl Iterator<Item = String>, name: &str) -> Result<String, CliError> {
    args.next()
        .ok_or_else(|| CliError::Usage(format!("{name} requires a value")))
}

fn usage() -> &'static str {
    "usage: neo4r-cli [--addr ADDR] [--query CYPHER] [--command COMMAND] [--history] [--history-file PATH] [--no-history]"
}

fn repl_help() -> &'static str {
    "commands:
  :help                 show this help
  :history              show query history
  :ping                 ping server
  :command COMMAND      send a raw server command
  :quit                 exit

queries:
  End a Cypher query with ; to execute it."
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
mod tests {
    use super::*;

    #[test]
    fn parses_cli_args() {
        let args = CliArgs::parse([
            "--addr".to_string(),
            "127.0.0.1:9000".to_string(),
            "--query".to_string(),
            "MATCH (n) RETURN n".to_string(),
            "--history-file".to_string(),
            "/tmp/neo4r-history".to_string(),
        ])
        .unwrap();

        assert_eq!(args.address, "127.0.0.1:9000");
        assert_eq!(args.query, Some("MATCH (n) RETURN n".to_string()));
        assert_eq!(args.history_file, Some(PathBuf::from("/tmp/neo4r-history")));
    }

    #[test]
    fn trims_query_terminator() {
        assert_eq!(
            trim_query_terminator("MATCH (n) RETURN n;\n"),
            "MATCH (n) RETURN n"
        );
    }

    #[test]
    fn keeps_history_deduplicated_in_execution_order() {
        let path = std::env::temp_dir().join(format!("neo4r-cli-history-{}", std::process::id()));
        let _ = fs::remove_file(&path);

        remember_query(true, &path, "MATCH (n) RETURN n").unwrap();
        remember_query(true, &path, "MATCH (m) RETURN m").unwrap();
        remember_query(true, &path, "MATCH (n) RETURN n").unwrap();

        assert_eq!(
            read_history(&path),
            vec![
                "MATCH (m) RETURN m".to_string(),
                "MATCH (n) RETURN n".to_string()
            ]
        );
        let _ = fs::remove_file(path);
    }
}
