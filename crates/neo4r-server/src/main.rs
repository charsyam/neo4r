use neo4r_core::{ShardPlacement, ShardReplica, ShardRole, ShardRoutingTable};
use neo4r_db::{DatabaseConfig, ReplicationAckPolicy, TcpShardReplicator};
use neo4r_server::{QueryReadPreference, TcpBackend, TcpBackendConfig};
use std::fs::{self, OpenOptions};
use std::io;
use std::net::TcpListener;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

fn main() {
    if let Err(err) = run() {
        eprintln!("{err}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let args = ServerArgs::parse(std::env::args().skip(1))?;
    if args.daemonize {
        spawn_daemon(&args)?;
        return Ok(());
    }

    let routing_table = args.routing_table()?;
    if let Some(routing_table) = routing_table.as_ref() {
        if routing_table.placements.len() != args.shard_count as usize {
            return Err(format!(
                "routing table contains {} shard placements but --shards is {}",
                routing_table.placements.len(),
                args.shard_count
            )
            .into());
        }
    }
    let mut config = DatabaseConfig::new(&args.data_dir, args.shard_count, args.partition_count)
        .with_server_id(args.server_id);
    if let Some(routing_table) = routing_table.clone() {
        config = config.with_routing_table(routing_table);
    }
    let db = if let Some(routing_table) = routing_table.clone() {
        let replicator = Arc::new(
            TcpShardReplicator::new(routing_table)
                .with_ack_policy(args.replication_ack_policy)
                .with_connect_timeout(Duration::from_millis(args.replication_connect_timeout_ms))
                .with_retry(
                    args.replication_retry_attempts,
                    Duration::from_millis(args.replication_retry_backoff_ms),
                ),
        );
        for peer in &args.replica_peers {
            replicator.register_peer(peer.server_id, peer.address.clone())?;
        }
        for peer in &args.peers {
            replicator.register_peer(peer.server_id, peer.address.clone())?;
        }
        neo4r_db::Neo4rDatabaseHandle::open_with_replicator(config, replicator)?
    } else {
        neo4r_db::Neo4rDatabaseHandle::open(config)?
    };
    let backend = TcpBackend::with_persistent_config(
        db,
        TcpBackendConfig {
            worker_count: args.worker_count,
            queue_capacity: args.queue_capacity,
            default_page_size: args.default_page_size,
            read_preference: args.read_preference,
            catch_up_connect_timeout: Duration::from_millis(args.replication_connect_timeout_ms),
        },
    )?;
    for peer in &args.replica_peers {
        backend.register_replication_peer(peer.server_id, peer.address.clone())?;
    }
    for peer in &args.peers {
        backend.register_replication_peer(peer.server_id, peer.address.clone())?;
    }
    for peer in &args.query_peers {
        backend.register_query_peer(peer.server_id, peer.address.clone())?;
    }
    if args.catch_up_on_startup {
        routing_table
            .as_ref()
            .ok_or("--catch-up-on-startup requires cluster routing")?;
        let results = backend
            .catch_up_from_primaries_with_limit(args.catch_up_batch_size)
            .map_err(|err| {
                format!("failed to catch up from persisted/explicit replication peers: {err}")
            })?;
        for result in results {
            eprintln!(
                "neo4r-server catch-up shard={} from primary={} start_index={} end_index={} fetched={}",
                result.shard_id,
                result.primary_server_id,
                result.start_index,
                result.end_index,
                result.fetched_entries
            );
        }
    }
    if let Some(interval_ms) = args.catch_up_interval_ms {
        routing_table
            .as_ref()
            .ok_or("--catch-up-interval-ms requires cluster routing")?;
        let catch_up_backend = backend.clone();
        let catch_up_batch_size = args.catch_up_batch_size;
        std::thread::spawn(move || {
            let interval = Duration::from_millis(interval_ms);
            loop {
                std::thread::sleep(interval);
                match catch_up_backend.catch_up_from_primaries_with_limit(catch_up_batch_size) {
                    Ok(results) => {
                        for result in results {
                            if result.fetched_entries > 0 {
                                eprintln!(
                                    "neo4r-server periodic catch-up shard={} from primary={} start_index={} end_index={} fetched={}",
                                    result.shard_id,
                                    result.primary_server_id,
                                    result.start_index,
                                    result.end_index,
                                    result.fetched_entries
                                );
                            }
                        }
                    }
                    Err(err) => eprintln!("neo4r-server periodic catch-up failed: {err}"),
                }
            }
        });
    }
    if args.sync_index_catalog_on_startup {
        let routing_table = routing_table
            .as_ref()
            .ok_or("--sync-index-catalog-on-startup requires cluster routing")?;
        let metadata_primary = metadata_primary_server_id(routing_table)?;
        if metadata_primary != args.server_id {
            backend
                .sync_index_catalog_from_peer(metadata_primary)
                .map_err(|err| {
                    format!(
                        "failed to sync index catalog from metadata primary {metadata_primary}: {err}"
                    )
                })?;
            eprintln!("neo4r-server synced index catalog from metadata primary {metadata_primary}");
        }
    }
    if let Some(interval_ms) = args.sync_index_catalog_interval_ms {
        let routing_table = routing_table
            .as_ref()
            .ok_or("--sync-index-catalog-interval-ms requires cluster routing")?;
        let metadata_primary = metadata_primary_server_id(routing_table)?;
        if metadata_primary != args.server_id {
            let sync_backend = backend.clone();
            std::thread::spawn(move || {
                let interval = Duration::from_millis(interval_ms);
                loop {
                    std::thread::sleep(interval);
                    match sync_backend.sync_index_catalog_from_peer(metadata_primary) {
                        Ok(()) => eprintln!(
                            "neo4r-server periodic index catalog sync from metadata primary {metadata_primary}"
                        ),
                        Err(err) => eprintln!(
                            "neo4r-server periodic index catalog sync from metadata primary {metadata_primary} failed: {err}"
                        ),
                    }
                }
            });
        }
    }
    if args.recover_transactions_on_startup {
        let recovered = backend
            .recover_transaction_decisions()
            .map_err(|err| format!("failed to recover transaction decisions: {err}"))?;
        eprintln!("neo4r-server recovered {recovered} transaction decision(s)");
    }
    if let Some(interval_ms) = args.recover_transactions_interval_ms {
        let recovery_backend = backend.clone();
        std::thread::spawn(move || {
            let interval = Duration::from_millis(interval_ms);
            loop {
                std::thread::sleep(interval);
                match recovery_backend.recover_transaction_decisions() {
                    Ok(recovered) => {
                        if recovered > 0 {
                            eprintln!(
                                "neo4r-server periodic transaction recovery recovered {recovered} decision(s)"
                            );
                        }
                    }
                    Err(err) => {
                        eprintln!("neo4r-server periodic transaction recovery failed: {err}")
                    }
                }
            }
        });
    }
    eprintln!("neo4r-server listening on {}", args.bind_addr);
    if let Some(replication_bind_addr) = args.replication_bind_addr.clone() {
        let replication_backend = backend.clone();
        let replication_listener = TcpListener::bind(&replication_bind_addr)?;
        let replication_local_addr = replication_listener.local_addr()?;
        std::thread::spawn(move || {
            eprintln!("neo4r-server replication listening on {replication_local_addr}");
            let _ = replication_backend.serve_replication_listener(replication_listener);
        });
    }
    backend.serve_addr(&args.bind_addr)?;
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ServerArgs {
    bind_addr: String,
    data_dir: PathBuf,
    shard_count: u64,
    partition_count: usize,
    server_id: u64,
    worker_count: usize,
    queue_capacity: usize,
    default_page_size: usize,
    read_preference: QueryReadPreference,
    primary_server_id: u64,
    routing_table_path: Option<PathBuf>,
    replica_peers: Vec<ReplicaPeer>,
    peers: Vec<ReplicaPeer>,
    query_peers: Vec<ReplicaPeer>,
    replication_bind_addr: Option<String>,
    replication_ack_policy: ReplicationAckPolicy,
    replication_connect_timeout_ms: u64,
    replication_retry_attempts: usize,
    replication_retry_backoff_ms: u64,
    catch_up_on_startup: bool,
    catch_up_interval_ms: Option<u64>,
    catch_up_batch_size: Option<usize>,
    sync_index_catalog_on_startup: bool,
    sync_index_catalog_interval_ms: Option<u64>,
    recover_transactions_on_startup: bool,
    recover_transactions_interval_ms: Option<u64>,
    daemonize: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ReplicaPeer {
    server_id: u64,
    address: String,
}

impl ServerArgs {
    fn parse(args: impl IntoIterator<Item = String>) -> Result<Self, String> {
        let mut parsed = Self {
            bind_addr: "127.0.0.1:7687".to_string(),
            data_dir: PathBuf::from("data"),
            shard_count: 1,
            partition_count: 1,
            server_id: 1,
            worker_count: default_worker_count(),
            queue_capacity: 1024,
            default_page_size: 128,
            read_preference: QueryReadPreference::Primary,
            primary_server_id: 1,
            routing_table_path: None,
            replica_peers: Vec::new(),
            peers: Vec::new(),
            query_peers: Vec::new(),
            replication_bind_addr: None,
            replication_ack_policy: ReplicationAckPolicy::All,
            replication_connect_timeout_ms: 1000,
            replication_retry_attempts: 1,
            replication_retry_backoff_ms: 10,
            catch_up_on_startup: false,
            catch_up_interval_ms: None,
            catch_up_batch_size: None,
            sync_index_catalog_on_startup: false,
            sync_index_catalog_interval_ms: None,
            recover_transactions_on_startup: false,
            recover_transactions_interval_ms: None,
            daemonize: false,
        };
        let mut args = args.into_iter();
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--bind" => parsed.bind_addr = next_arg(&mut args, "--bind")?,
                "--data-dir" => parsed.data_dir = PathBuf::from(next_arg(&mut args, "--data-dir")?),
                "--shards" => parsed.shard_count = parse_next(&mut args, "--shards")?,
                "--partitions" => parsed.partition_count = parse_next(&mut args, "--partitions")?,
                "--server-id" => parsed.server_id = parse_next(&mut args, "--server-id")?,
                "--workers" => parsed.worker_count = parse_next(&mut args, "--workers")?,
                "--queue-capacity" => {
                    parsed.queue_capacity = parse_next(&mut args, "--queue-capacity")?
                }
                "--page-size" => parsed.default_page_size = parse_next(&mut args, "--page-size")?,
                "--read-preference" => {
                    parsed.read_preference =
                        parse_read_preference(&next_arg(&mut args, "--read-preference")?)?
                }
                "--primary-server-id" => {
                    parsed.primary_server_id = parse_next(&mut args, "--primary-server-id")?
                }
                "--routing-table" => {
                    parsed.routing_table_path =
                        Some(PathBuf::from(next_arg(&mut args, "--routing-table")?))
                }
                "--replica-peer" => parsed
                    .replica_peers
                    .push(parse_replica_peer(&next_arg(&mut args, "--replica-peer")?)?),
                "--peer" => parsed
                    .peers
                    .push(parse_peer(&next_arg(&mut args, "--peer")?, "--peer")?),
                "--query-peer" => parsed.query_peers.push(parse_peer(
                    &next_arg(&mut args, "--query-peer")?,
                    "--query-peer",
                )?),
                "--replication-bind" => {
                    parsed.replication_bind_addr = Some(next_arg(&mut args, "--replication-bind")?)
                }
                "--replication-ack" => {
                    parsed.replication_ack_policy =
                        parse_ack_policy(&next_arg(&mut args, "--replication-ack")?)?
                }
                "--replication-connect-timeout-ms" => {
                    parsed.replication_connect_timeout_ms =
                        parse_next(&mut args, "--replication-connect-timeout-ms")?
                }
                "--replication-retry-attempts" => {
                    parsed.replication_retry_attempts =
                        parse_next(&mut args, "--replication-retry-attempts")?
                }
                "--replication-retry-backoff-ms" => {
                    parsed.replication_retry_backoff_ms =
                        parse_next(&mut args, "--replication-retry-backoff-ms")?
                }
                "--catch-up-on-startup" => parsed.catch_up_on_startup = true,
                "--catch-up-interval-ms" => {
                    parsed.catch_up_interval_ms =
                        Some(parse_next(&mut args, "--catch-up-interval-ms")?)
                }
                "--catch-up-batch-size" => {
                    parsed.catch_up_batch_size =
                        Some(parse_next(&mut args, "--catch-up-batch-size")?)
                }
                "--sync-index-catalog-on-startup" => parsed.sync_index_catalog_on_startup = true,
                "--sync-index-catalog-interval-ms" => {
                    parsed.sync_index_catalog_interval_ms =
                        Some(parse_next(&mut args, "--sync-index-catalog-interval-ms")?)
                }
                "--recover-transactions-on-startup" => {
                    parsed.recover_transactions_on_startup = true
                }
                "--recover-transactions-interval-ms" => {
                    parsed.recover_transactions_interval_ms =
                        Some(parse_next(&mut args, "--recover-transactions-interval-ms")?)
                }
                "--daemonize" => parsed.daemonize = true,
                "--help" | "-h" => return Err(usage()),
                value => return Err(format!("unknown argument: {value}\n{}", usage())),
            }
        }
        if parsed.shard_count == 0 {
            return Err("--shards must be greater than zero".to_string());
        }
        if parsed.partition_count == 0 {
            return Err("--partitions must be greater than zero".to_string());
        }
        if parsed.worker_count == 0 {
            return Err("--workers must be greater than zero".to_string());
        }
        if parsed.queue_capacity == 0 {
            return Err("--queue-capacity must be greater than zero".to_string());
        }
        if parsed.default_page_size == 0 {
            return Err("--page-size must be greater than zero".to_string());
        }
        if parsed.primary_server_id == 0 {
            return Err("--primary-server-id must be greater than zero".to_string());
        }
        if parsed.replication_retry_attempts == 0 {
            return Err("--replication-retry-attempts must be greater than zero".to_string());
        }
        if parsed.replication_connect_timeout_ms == 0 {
            return Err("--replication-connect-timeout-ms must be greater than zero".to_string());
        }
        if parsed.catch_up_interval_ms == Some(0) {
            return Err("--catch-up-interval-ms must be greater than zero".to_string());
        }
        if parsed.catch_up_batch_size == Some(0) {
            return Err("--catch-up-batch-size must be greater than zero".to_string());
        }
        if parsed.sync_index_catalog_interval_ms == Some(0) {
            return Err("--sync-index-catalog-interval-ms must be greater than zero".to_string());
        }
        if parsed.recover_transactions_interval_ms == Some(0) {
            return Err("--recover-transactions-interval-ms must be greater than zero".to_string());
        }
        for peer in &parsed.replica_peers {
            if peer.server_id == parsed.primary_server_id {
                return Err("--replica-peer cannot reference the primary server id".to_string());
            }
        }
        Ok(parsed)
    }

    fn routing_table(&self) -> Result<Option<ShardRoutingTable>, String> {
        if let Some(path) = &self.routing_table_path {
            return load_routing_table_config(path).map(Some);
        }
        let cluster_requested = self.primary_server_id != self.server_id
            || !self.replica_peers.is_empty()
            || self.replication_bind_addr.is_some();
        if !cluster_requested {
            return Ok(None);
        }
        let mut replicas = vec![ShardReplica::primary(self.primary_server_id)];
        for peer in &self.replica_peers {
            replicas.push(ShardReplica::replica(peer.server_id));
        }
        if self.server_id != self.primary_server_id
            && !replicas
                .iter()
                .any(|replica| replica.server_id == self.server_id)
        {
            replicas.push(ShardReplica::replica(self.server_id));
        }
        Ok(Some(ShardRoutingTable {
            version: 1,
            placements: (0..self.shard_count)
                .map(|shard_id| ShardPlacement::new(shard_id, replicas.clone()))
                .collect(),
        }))
    }
}

fn spawn_daemon(args: &ServerArgs) -> io::Result<()> {
    let mut child_args = daemon_child_args(args);
    let dev_null = OpenOptions::new()
        .read(true)
        .write(true)
        .open("/dev/null")?;
    let child = Command::new(std::env::current_exe()?)
        .args(child_args.drain(..))
        .stdin(Stdio::from(dev_null.try_clone()?))
        .stdout(Stdio::from(dev_null.try_clone()?))
        .stderr(Stdio::from(dev_null))
        .spawn()?;
    println!("{}", child.id());
    Ok(())
}

fn daemon_child_args(args: &ServerArgs) -> Vec<String> {
    let mut child_args = vec![
        "--bind".to_string(),
        args.bind_addr.clone(),
        "--data-dir".to_string(),
        args.data_dir.display().to_string(),
        "--shards".to_string(),
        args.shard_count.to_string(),
        "--partitions".to_string(),
        args.partition_count.to_string(),
        "--server-id".to_string(),
        args.server_id.to_string(),
        "--workers".to_string(),
        args.worker_count.to_string(),
        "--queue-capacity".to_string(),
        args.queue_capacity.to_string(),
        "--page-size".to_string(),
        args.default_page_size.to_string(),
        "--read-preference".to_string(),
        format_read_preference(args.read_preference).to_string(),
        "--primary-server-id".to_string(),
        args.primary_server_id.to_string(),
        "--replication-ack".to_string(),
        format_ack_policy(args.replication_ack_policy).to_string(),
        "--replication-retry-attempts".to_string(),
        args.replication_retry_attempts.to_string(),
        "--replication-retry-backoff-ms".to_string(),
        args.replication_retry_backoff_ms.to_string(),
        "--replication-connect-timeout-ms".to_string(),
        args.replication_connect_timeout_ms.to_string(),
    ];
    if let Some(addr) = &args.replication_bind_addr {
        child_args.push("--replication-bind".to_string());
        child_args.push(addr.clone());
    }
    if let Some(path) = &args.routing_table_path {
        child_args.push("--routing-table".to_string());
        child_args.push(path.display().to_string());
    }
    for peer in &args.replica_peers {
        child_args.push("--replica-peer".to_string());
        child_args.push(format!("{}={}", peer.server_id, peer.address));
    }
    for peer in &args.peers {
        child_args.push("--peer".to_string());
        child_args.push(format!("{}={}", peer.server_id, peer.address));
    }
    for peer in &args.query_peers {
        child_args.push("--query-peer".to_string());
        child_args.push(format!("{}={}", peer.server_id, peer.address));
    }
    if args.catch_up_on_startup {
        child_args.push("--catch-up-on-startup".to_string());
    }
    if let Some(interval_ms) = args.catch_up_interval_ms {
        child_args.push("--catch-up-interval-ms".to_string());
        child_args.push(interval_ms.to_string());
    }
    if let Some(batch_size) = args.catch_up_batch_size {
        child_args.push("--catch-up-batch-size".to_string());
        child_args.push(batch_size.to_string());
    }
    if args.sync_index_catalog_on_startup {
        child_args.push("--sync-index-catalog-on-startup".to_string());
    }
    if let Some(interval_ms) = args.sync_index_catalog_interval_ms {
        child_args.push("--sync-index-catalog-interval-ms".to_string());
        child_args.push(interval_ms.to_string());
    }
    if args.recover_transactions_on_startup {
        child_args.push("--recover-transactions-on-startup".to_string());
    }
    if let Some(interval_ms) = args.recover_transactions_interval_ms {
        child_args.push("--recover-transactions-interval-ms".to_string());
        child_args.push(interval_ms.to_string());
    }
    child_args
}

fn load_routing_table_config(path: &PathBuf) -> Result<ShardRoutingTable, String> {
    parse_routing_table_config(
        &fs::read_to_string(path)
            .map_err(|err| format!("failed to read routing table {}: {err}", path.display()))?,
    )
}

fn parse_routing_table_config(input: &str) -> Result<ShardRoutingTable, String> {
    let mut version = None;
    let mut placements = Vec::new();
    for (line_no, raw_line) in input.lines().enumerate() {
        let line = raw_line.split('#').next().unwrap_or("").trim();
        if line.is_empty() {
            continue;
        }
        if let Some(raw_version) = line.strip_prefix("version=") {
            version = Some(parse_config_u64(raw_version, "version", line_no)?);
            continue;
        }
        let mut shard_id = None;
        let mut primary = None;
        let mut replicas = Vec::new();
        for token in line.split_whitespace() {
            let (key, value) = token
                .split_once('=')
                .ok_or_else(|| format!("routing line {} token must be key=value", line_no + 1))?;
            match key {
                "shard" => shard_id = Some(parse_config_u64(value, "shard", line_no)?),
                "primary" => primary = Some(parse_config_u64(value, "primary", line_no)?),
                "replicas" => {
                    if !value.is_empty() {
                        replicas = value
                            .split(',')
                            .filter(|value| !value.is_empty())
                            .map(|value| parse_config_u64(value, "replica", line_no))
                            .collect::<Result<Vec<_>, _>>()?;
                    }
                }
                _ => {
                    return Err(format!(
                        "routing line {} has unknown key {key:?}",
                        line_no + 1
                    ))
                }
            }
        }
        let shard_id =
            shard_id.ok_or_else(|| format!("routing line {} missing shard", line_no + 1))?;
        let primary =
            primary.ok_or_else(|| format!("routing line {} missing primary", line_no + 1))?;
        if primary == 0 {
            return Err(format!(
                "routing line {} primary must be greater than zero",
                line_no + 1
            ));
        }
        let mut shard_replicas = vec![ShardReplica::primary(primary)];
        for replica in replicas {
            if replica == 0 {
                return Err(format!(
                    "routing line {} replica must be greater than zero",
                    line_no + 1
                ));
            }
            if replica != primary && !shard_replicas.iter().any(|item| item.server_id == replica) {
                shard_replicas.push(ShardReplica::replica(replica));
            }
        }
        placements.push(ShardPlacement::new(shard_id, shard_replicas));
    }
    let version = version.ok_or_else(|| "routing table config missing version".to_string())?;
    if version == 0 {
        return Err("routing table version must be greater than zero".to_string());
    }
    placements.sort_by_key(|placement| placement.shard_id);
    if placements.is_empty() {
        return Err("routing table config must contain at least one shard".to_string());
    }
    for (expected, placement) in placements.iter().enumerate() {
        if placement.shard_id != expected as u64 {
            return Err(format!(
                "routing table shards must be contiguous from 0; expected {expected}, got {}",
                placement.shard_id
            ));
        }
        let primary_count = placement
            .replicas
            .iter()
            .filter(|replica| replica.role == ShardRole::Primary)
            .count();
        if primary_count != 1 {
            return Err(format!(
                "routing shard {} must have exactly one primary",
                placement.shard_id
            ));
        }
    }
    Ok(ShardRoutingTable {
        version,
        placements,
    })
}

fn metadata_primary_server_id(routing_table: &ShardRoutingTable) -> Result<u64, String> {
    routing_table
        .placements
        .iter()
        .find(|placement| placement.shard_id == 0)
        .and_then(|placement| {
            placement
                .replicas
                .iter()
                .find(|replica| replica.role == ShardRole::Primary)
                .map(|replica| replica.server_id)
        })
        .ok_or_else(|| "routing table missing shard 0 primary".to_string())
}

fn parse_config_u64(value: &str, field: &str, line_no: usize) -> Result<u64, String> {
    value
        .parse::<u64>()
        .map_err(|_| format!("routing line {} has invalid {field}", line_no + 1))
}

fn parse_replica_peer(value: &str) -> Result<ReplicaPeer, String> {
    parse_peer(value, "--replica-peer")
}

fn parse_peer(value: &str, option_name: &str) -> Result<ReplicaPeer, String> {
    let (server_id, address) = value
        .split_once('=')
        .ok_or_else(|| format!("{option_name} must be SERVER_ID=ADDR"))?;
    let server_id = server_id
        .parse::<u64>()
        .map_err(|_| format!("{option_name} server id has an invalid value"))?;
    if server_id == 0 {
        return Err(format!("{option_name} server id must be greater than zero"));
    }
    if address.is_empty() {
        return Err(format!("{option_name} address cannot be empty"));
    }
    Ok(ReplicaPeer {
        server_id,
        address: address.to_string(),
    })
}

fn parse_ack_policy(value: &str) -> Result<ReplicationAckPolicy, String> {
    match value {
        "all" | "ALL" => Ok(ReplicationAckPolicy::All),
        "quorum" | "QUORUM" => Ok(ReplicationAckPolicy::Quorum),
        "async" | "ASYNC" => Ok(ReplicationAckPolicy::Async),
        _ => Err("--replication-ack must be all, quorum, or async".to_string()),
    }
}

fn parse_read_preference(value: &str) -> Result<QueryReadPreference, String> {
    match value {
        "primary" | "PRIMARY" => Ok(QueryReadPreference::Primary),
        "prefer-replica" | "PREFER_REPLICA" | "prefer_replica" => {
            Ok(QueryReadPreference::PreferReplica)
        }
        _ => Err("--read-preference must be primary or prefer-replica".to_string()),
    }
}

fn format_ack_policy(policy: ReplicationAckPolicy) -> &'static str {
    match policy {
        ReplicationAckPolicy::All => "all",
        ReplicationAckPolicy::Quorum => "quorum",
        ReplicationAckPolicy::Async => "async",
    }
}

fn format_read_preference(preference: QueryReadPreference) -> &'static str {
    match preference {
        QueryReadPreference::Primary => "primary",
        QueryReadPreference::PreferReplica => "prefer-replica",
    }
}

fn next_arg(args: &mut impl Iterator<Item = String>, name: &str) -> Result<String, String> {
    args.next()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| format!("{name} requires a value"))
}

fn parse_next<T: std::str::FromStr>(
    args: &mut impl Iterator<Item = String>,
    name: &str,
) -> Result<T, String> {
    next_arg(args, name)?
        .parse()
        .map_err(|_| format!("{name} has an invalid value"))
}

fn usage() -> String {
    "usage: neo4r-server [--bind ADDR] [--data-dir DIR] [--shards N] [--partitions N] [--server-id ID] [--primary-server-id ID] [--replica-peer SERVER_ID=ADDR] [--peer SERVER_ID=ADDR] [--query-peer SERVER_ID=ADDR] [--read-preference primary|prefer-replica] [--replication-bind ADDR] [--replication-ack all|quorum|async] [--replication-connect-timeout-ms MS] [--replication-retry-attempts N] [--replication-retry-backoff-ms MS] [--catch-up-on-startup] [--catch-up-interval-ms MS] [--catch-up-batch-size N] [--sync-index-catalog-on-startup] [--sync-index-catalog-interval-ms MS] [--recover-transactions-on-startup] [--recover-transactions-interval-ms MS] [--workers N] [--queue-capacity N] [--page-size N] [--daemonize]".to_string()
}

fn default_worker_count() -> usize {
    std::thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(4)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn parses_server_args() {
        let args = ServerArgs::parse([
            "--bind".to_string(),
            "127.0.0.1:9000".to_string(),
            "--data-dir".to_string(),
            "/tmp/neo4r".to_string(),
            "--shards".to_string(),
            "8".to_string(),
            "--partitions".to_string(),
            "2".to_string(),
            "--server-id".to_string(),
            "10".to_string(),
            "--workers".to_string(),
            "3".to_string(),
            "--queue-capacity".to_string(),
            "32".to_string(),
            "--page-size".to_string(),
            "16".to_string(),
            "--read-preference".to_string(),
            "prefer-replica".to_string(),
            "--primary-server-id".to_string(),
            "10".to_string(),
            "--routing-table".to_string(),
            "/tmp/neo4r-routing.txt".to_string(),
            "--replica-peer".to_string(),
            "11=127.0.0.1:9701".to_string(),
            "--peer".to_string(),
            "10=127.0.0.1:9700".to_string(),
            "--query-peer".to_string(),
            "12=127.0.0.1:7688".to_string(),
            "--replication-bind".to_string(),
            "127.0.0.1:9700".to_string(),
            "--replication-ack".to_string(),
            "quorum".to_string(),
            "--replication-connect-timeout-ms".to_string(),
            "750".to_string(),
            "--replication-retry-attempts".to_string(),
            "3".to_string(),
            "--replication-retry-backoff-ms".to_string(),
            "25".to_string(),
            "--catch-up-on-startup".to_string(),
            "--catch-up-interval-ms".to_string(),
            "1000".to_string(),
            "--catch-up-batch-size".to_string(),
            "128".to_string(),
            "--sync-index-catalog-on-startup".to_string(),
            "--sync-index-catalog-interval-ms".to_string(),
            "2000".to_string(),
            "--recover-transactions-on-startup".to_string(),
            "--recover-transactions-interval-ms".to_string(),
            "3000".to_string(),
            "--daemonize".to_string(),
        ])
        .unwrap();

        assert_eq!(args.bind_addr, "127.0.0.1:9000");
        assert_eq!(args.data_dir, PathBuf::from("/tmp/neo4r"));
        assert_eq!(args.shard_count, 8);
        assert_eq!(args.partition_count, 2);
        assert_eq!(args.server_id, 10);
        assert_eq!(args.worker_count, 3);
        assert_eq!(args.queue_capacity, 32);
        assert_eq!(args.default_page_size, 16);
        assert_eq!(args.read_preference, QueryReadPreference::PreferReplica);
        assert_eq!(args.primary_server_id, 10);
        assert_eq!(
            args.routing_table_path,
            Some(PathBuf::from("/tmp/neo4r-routing.txt"))
        );
        assert_eq!(
            args.replica_peers,
            vec![ReplicaPeer {
                server_id: 11,
                address: "127.0.0.1:9701".to_string(),
            }]
        );
        assert_eq!(
            args.peers,
            vec![ReplicaPeer {
                server_id: 10,
                address: "127.0.0.1:9700".to_string(),
            }]
        );
        assert_eq!(
            args.query_peers,
            vec![ReplicaPeer {
                server_id: 12,
                address: "127.0.0.1:7688".to_string(),
            }]
        );
        assert_eq!(
            args.replication_bind_addr,
            Some("127.0.0.1:9700".to_string())
        );
        assert_eq!(args.replication_ack_policy, ReplicationAckPolicy::Quorum);
        assert_eq!(args.replication_connect_timeout_ms, 750);
        assert_eq!(args.replication_retry_attempts, 3);
        assert_eq!(args.replication_retry_backoff_ms, 25);
        assert!(args.catch_up_on_startup);
        assert_eq!(args.catch_up_interval_ms, Some(1000));
        assert_eq!(args.catch_up_batch_size, Some(128));
        assert!(args.sync_index_catalog_on_startup);
        assert_eq!(args.sync_index_catalog_interval_ms, Some(2000));
        assert!(args.recover_transactions_on_startup);
        assert_eq!(args.recover_transactions_interval_ms, Some(3000));
        assert!(args.daemonize);
    }

    #[test]
    fn rejects_zero_catch_up_interval() {
        assert_eq!(
            ServerArgs::parse([
                "--server-id".to_string(),
                "2".to_string(),
                "--primary-server-id".to_string(),
                "1".to_string(),
                "--catch-up-interval-ms".to_string(),
                "0".to_string(),
            ])
            .unwrap_err(),
            "--catch-up-interval-ms must be greater than zero"
        );
    }

    #[test]
    fn rejects_zero_replication_connect_timeout() {
        assert_eq!(
            ServerArgs::parse([
                "--replication-connect-timeout-ms".to_string(),
                "0".to_string(),
            ])
            .unwrap_err(),
            "--replication-connect-timeout-ms must be greater than zero"
        );
    }

    #[test]
    fn rejects_zero_catch_up_batch_size() {
        assert_eq!(
            ServerArgs::parse([
                "--server-id".to_string(),
                "2".to_string(),
                "--primary-server-id".to_string(),
                "1".to_string(),
                "--catch-up-batch-size".to_string(),
                "0".to_string(),
            ])
            .unwrap_err(),
            "--catch-up-batch-size must be greater than zero"
        );
    }

    #[test]
    fn rejects_zero_sync_index_catalog_interval() {
        assert_eq!(
            ServerArgs::parse([
                "--sync-index-catalog-interval-ms".to_string(),
                "0".to_string(),
            ])
            .unwrap_err(),
            "--sync-index-catalog-interval-ms must be greater than zero"
        );
    }

    #[test]
    fn rejects_zero_recover_transactions_interval() {
        assert_eq!(
            ServerArgs::parse([
                "--recover-transactions-interval-ms".to_string(),
                "0".to_string(),
            ])
            .unwrap_err(),
            "--recover-transactions-interval-ms must be greater than zero"
        );
    }

    #[test]
    fn daemon_child_args_preserve_replication_and_recovery_options() {
        let args = ServerArgs::parse([
            "--bind".to_string(),
            "127.0.0.1:9000".to_string(),
            "--data-dir".to_string(),
            "/tmp/neo4r".to_string(),
            "--server-id".to_string(),
            "2".to_string(),
            "--primary-server-id".to_string(),
            "1".to_string(),
            "--replica-peer".to_string(),
            "2=127.0.0.1:9702".to_string(),
            "--peer".to_string(),
            "1=127.0.0.1:9701".to_string(),
            "--query-peer".to_string(),
            "1=127.0.0.1:7687".to_string(),
            "--replication-bind".to_string(),
            "127.0.0.1:9702".to_string(),
            "--replication-ack".to_string(),
            "quorum".to_string(),
            "--replication-connect-timeout-ms".to_string(),
            "750".to_string(),
            "--replication-retry-attempts".to_string(),
            "5".to_string(),
            "--replication-retry-backoff-ms".to_string(),
            "50".to_string(),
            "--catch-up-on-startup".to_string(),
            "--catch-up-interval-ms".to_string(),
            "250".to_string(),
            "--catch-up-batch-size".to_string(),
            "64".to_string(),
            "--sync-index-catalog-on-startup".to_string(),
            "--sync-index-catalog-interval-ms".to_string(),
            "500".to_string(),
            "--recover-transactions-on-startup".to_string(),
            "--recover-transactions-interval-ms".to_string(),
            "750".to_string(),
        ])
        .unwrap();

        let child_args = daemon_child_args(&args);

        assert!(child_args.contains(&"--replication-bind".to_string()));
        assert!(child_args.contains(&"127.0.0.1:9702".to_string()));
        assert!(child_args.contains(&"--replication-ack".to_string()));
        assert!(child_args.contains(&"quorum".to_string()));
        assert!(child_args.contains(&"--replication-connect-timeout-ms".to_string()));
        assert!(child_args.contains(&"750".to_string()));
        assert!(child_args.contains(&"--replication-retry-attempts".to_string()));
        assert!(child_args.contains(&"5".to_string()));
        assert!(child_args.contains(&"--replication-retry-backoff-ms".to_string()));
        assert!(child_args.contains(&"50".to_string()));
        assert!(child_args.contains(&"--replica-peer".to_string()));
        assert!(child_args.contains(&"2=127.0.0.1:9702".to_string()));
        assert!(child_args.contains(&"--peer".to_string()));
        assert!(child_args.contains(&"1=127.0.0.1:9701".to_string()));
        assert!(child_args.contains(&"--query-peer".to_string()));
        assert!(child_args.contains(&"1=127.0.0.1:7687".to_string()));
        assert!(child_args.contains(&"--catch-up-on-startup".to_string()));
        assert!(child_args.contains(&"--catch-up-interval-ms".to_string()));
        assert!(child_args.contains(&"250".to_string()));
        assert!(child_args.contains(&"--catch-up-batch-size".to_string()));
        assert!(child_args.contains(&"64".to_string()));
        assert!(child_args.contains(&"--sync-index-catalog-on-startup".to_string()));
        assert!(child_args.contains(&"--sync-index-catalog-interval-ms".to_string()));
        assert!(child_args.contains(&"500".to_string()));
        assert!(child_args.contains(&"--recover-transactions-on-startup".to_string()));
        assert!(child_args.contains(&"--recover-transactions-interval-ms".to_string()));
        assert!(child_args.contains(&"750".to_string()));
        assert!(!child_args.contains(&"--daemonize".to_string()));
    }

    #[test]
    fn builds_cluster_routing_table_from_replication_args() {
        let args = ServerArgs::parse([
            "--server-id".to_string(),
            "1".to_string(),
            "--primary-server-id".to_string(),
            "1".to_string(),
            "--shards".to_string(),
            "2".to_string(),
            "--replica-peer".to_string(),
            "2=127.0.0.1:9702".to_string(),
            "--peer".to_string(),
            "3=127.0.0.1:9703".to_string(),
        ])
        .unwrap();

        let table = args.routing_table().unwrap().unwrap();

        assert_eq!(table.version, 1);
        assert_eq!(table.placements.len(), 2);
        for placement in &table.placements {
            assert_eq!(
                placement.replicas,
                vec![ShardReplica::primary(1), ShardReplica::replica(2)]
            );
        }
        assert_eq!(metadata_primary_server_id(&table).unwrap(), 1);
    }

    #[test]
    fn replica_node_routing_table_includes_local_replica() {
        let args = ServerArgs::parse([
            "--server-id".to_string(),
            "2".to_string(),
            "--primary-server-id".to_string(),
            "1".to_string(),
            "--shards".to_string(),
            "1".to_string(),
            "--replication-bind".to_string(),
            "127.0.0.1:9702".to_string(),
        ])
        .unwrap();

        let table = args.routing_table().unwrap().unwrap();

        assert_eq!(
            table.placements[0].replicas,
            vec![ShardReplica::primary(1), ShardReplica::replica(2)]
        );
    }

    #[test]
    fn parses_explicit_routing_table_config() {
        let table = parse_routing_table_config(
            r#"
            # shard routing table
            version=7
            shard=0 primary=1 replicas=2,3
            shard=1 primary=2 replicas=1,3
            "#,
        )
        .unwrap();

        assert_eq!(table.version, 7);
        assert_eq!(
            table.placements,
            vec![
                ShardPlacement::new(
                    0,
                    vec![
                        ShardReplica::primary(1),
                        ShardReplica::replica(2),
                        ShardReplica::replica(3),
                    ],
                ),
                ShardPlacement::new(
                    1,
                    vec![
                        ShardReplica::primary(2),
                        ShardReplica::replica(1),
                        ShardReplica::replica(3),
                    ],
                ),
            ]
        );
    }

    #[test]
    fn rejects_non_contiguous_routing_table_config() {
        let err = parse_routing_table_config(
            r#"
            version=7
            shard=1 primary=1 replicas=2
            "#,
        )
        .unwrap_err();

        assert!(err.contains("contiguous"));
    }

    #[test]
    fn loads_routing_table_from_config_file() {
        let path = temp_file("neo4r-routing-config");
        fs::write(
            &path,
            r#"
            version=9
            shard=0 primary=2 replicas=1
            "#,
        )
        .unwrap();
        let args = ServerArgs::parse([
            "--server-id".to_string(),
            "1".to_string(),
            "--routing-table".to_string(),
            path.display().to_string(),
        ])
        .unwrap();

        let table = args.routing_table().unwrap().unwrap();

        assert_eq!(table.version, 9);
        assert_eq!(
            table.placements[0].replicas,
            vec![ShardReplica::primary(2), ShardReplica::replica(1)]
        );
        let _ = fs::remove_file(path);
    }

    fn temp_file(prefix: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "{}-{}-{}",
            prefix,
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ))
    }
}
