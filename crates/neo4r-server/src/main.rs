use neo4r_core::{ShardPlacement, ShardReplica, ShardRole, ShardRoutingTable};
use neo4r_db::{
    DatabaseConfig, ReplicationAckPolicy, ReplicationChannel, ReplicationChannelKind,
    ReplicationEndpoint, TcpReplicationChannel, TcpShardReplicator,
};
#[cfg(feature = "rdma")]
use neo4r_db::{RdmaReplicationChannel, RdmaReplicationListener};
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
        .with_server_id(args.server_id)
        .with_raft_enabled(routing_table.is_some());
    if let Some(routing_table) = routing_table.clone() {
        config = config.with_routing_table(routing_table);
    }
    let backend_config_template = config.clone();
    let mut raft_replicator = None;
    let db = if let Some(routing_table) = routing_table.clone() {
        let replicator = Arc::new(
            TcpShardReplicator::new(routing_table)
                .with_ack_policy(args.replication_ack_policy)
                .with_connect_timeout(Duration::from_millis(args.replication_connect_timeout_ms))
                .with_retry(
                    args.replication_retry_attempts,
                    Duration::from_millis(args.replication_retry_backoff_ms),
                )
                .with_channel(replication_channel(args.replication_transport)?)
                .with_raft_transport(true),
        );
        for peer in &args.replica_peers {
            replicator.register_peer_endpoint(
                peer.server_id,
                replication_endpoint(peer.address.clone(), args.replication_transport)?,
            )?;
        }
        for peer in &args.peers {
            replicator.register_peer_endpoint(
                peer.server_id,
                replication_endpoint(peer.address.clone(), args.replication_transport)?,
            )?;
        }
        raft_replicator = Some(replicator.clone());
        neo4r_db::Neo4rDatabaseHandle::open_with_replicator(config, replicator)?
    } else {
        neo4r_db::Neo4rDatabaseHandle::open(config)?
    };
    if let Some(replicator) = raft_replicator.clone() {
        let heartbeat_db = db.clone();
        let election_timeout = Duration::from_millis(1500 + (args.server_id % 7) * 137);
        std::thread::spawn(move || {
            let interval = Duration::from_millis(250);
            let mut ticks = 0_u64;
            loop {
                std::thread::sleep(interval);
                ticks = ticks.saturating_add(1);
                if let Ok(committed_indexes) = heartbeat_db.committed_indexes() {
                    let _ = replicator.send_raft_heartbeats(&committed_indexes);
                }
                if ticks % 8 == 0 {
                    let _ = replicator
                        .run_raft_election_round_with_timeout(&heartbeat_db, election_timeout);
                }
            }
        });
    }
    let backend = TcpBackend::with_persistent_config(
        db,
        TcpBackendConfig {
            worker_count: args.worker_count,
            queue_capacity: args.queue_capacity,
            default_page_size: args.default_page_size,
            read_preference: args.read_preference,
            catch_up_connect_timeout: Duration::from_millis(args.replication_connect_timeout_ms),
        },
    )?
    .with_web_options(
        args.web_auth_token.clone(),
        Duration::from_millis(args.slow_query_threshold_ms),
    )
    .with_multi_tenant_config(backend_config_template)?;
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
    if let Some(web_bind_addr) = args.web_bind_addr.clone() {
        let web_backend = backend.clone();
        std::thread::spawn(move || {
            eprintln!("neo4r-server web console listening on {web_bind_addr}");
            let _ = web_backend.serve_web_addr(&web_bind_addr);
        });
    }
    if let Some(replication_bind_addr) = args.replication_bind_addr.clone() {
        let replication_backend = backend.clone();
        match args.replication_transport {
            ReplicationChannelKind::Tcp => {
                let replication_listener = TcpListener::bind(&replication_bind_addr)?;
                let replication_local_addr = replication_listener.local_addr()?;
                std::thread::spawn(move || {
                    eprintln!("neo4r-server replication tcp listening on {replication_local_addr}");
                    let _ = replication_backend.serve_replication_listener(replication_listener);
                });
            }
            ReplicationChannelKind::Rdma => {
                #[cfg(feature = "rdma")]
                {
                    let replication_listener =
                        RdmaReplicationListener::bind(&replication_bind_addr)?;
                    let replication_local_addr = replication_listener.local_addr()?;
                    std::thread::spawn(move || {
                        eprintln!(
                            "neo4r-server replication rdma listening on {replication_local_addr}"
                        );
                        let _ = replication_backend
                            .serve_rdma_replication_listener(replication_listener);
                    });
                }
                #[cfg(not(feature = "rdma"))]
                return Err("--replication-transport rdma requires --features rdma".into());
            }
            other => {
                return Err(format!("replication listener does not support {other:?}").into());
            }
        }
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
    replication_transport: ReplicationChannelKind,
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
    web_bind_addr: Option<String>,
    web_auth_token: Option<String>,
    slow_query_threshold_ms: u64,
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
            replication_transport: ReplicationChannelKind::Tcp,
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
            web_bind_addr: None,
            web_auth_token: None,
            slow_query_threshold_ms: 250,
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
                "--replication-transport" => {
                    parsed.replication_transport = parse_replication_transport(&next_arg(
                        &mut args,
                        "--replication-transport",
                    )?)?
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
                "--web-bind" => parsed.web_bind_addr = Some(next_arg(&mut args, "--web-bind")?),
                "--web-auth-token" => {
                    parsed.web_auth_token = Some(next_arg(&mut args, "--web-auth-token")?)
                }
                "--slow-query-threshold-ms" => {
                    parsed.slow_query_threshold_ms =
                        parse_next(&mut args, "--slow-query-threshold-ms")?
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
        if parsed.slow_query_threshold_ms == 0 {
            return Err("--slow-query-threshold-ms must be greater than zero".to_string());
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
        "--replication-transport".to_string(),
        format_replication_transport(args.replication_transport).to_string(),
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
    if let Some(addr) = &args.web_bind_addr {
        child_args.push("--web-bind".to_string());
        child_args.push(addr.clone());
    }
    if let Some(token) = &args.web_auth_token {
        child_args.push("--web-auth-token".to_string());
        child_args.push(token.clone());
    }
    child_args.push("--slow-query-threshold-ms".to_string());
    child_args.push(args.slow_query_threshold_ms.to_string());
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

fn parse_replication_transport(value: &str) -> Result<ReplicationChannelKind, String> {
    match value.to_ascii_lowercase().as_str() {
        "tcp" => Ok(ReplicationChannelKind::Tcp),
        "rdma" => Ok(ReplicationChannelKind::Rdma),
        "udp" => Err("udp is not supported for raft replication transport".to_string()),
        "custom" => {
            Err("custom replication transport requires explicit provider wiring".to_string())
        }
        _ => Err("--replication-transport must be tcp or rdma".to_string()),
    }
}

fn format_replication_transport(transport: ReplicationChannelKind) -> &'static str {
    match transport {
        ReplicationChannelKind::Tcp => "tcp",
        ReplicationChannelKind::Rdma => "rdma",
        ReplicationChannelKind::Udp => "udp",
        ReplicationChannelKind::Custom => "custom",
    }
}

fn replication_endpoint(
    address: String,
    transport: ReplicationChannelKind,
) -> Result<ReplicationEndpoint, String> {
    match transport {
        ReplicationChannelKind::Tcp => Ok(ReplicationEndpoint::tcp(address)),
        #[cfg(feature = "rdma")]
        ReplicationChannelKind::Rdma => Ok(ReplicationEndpoint::rdma(address)),
        #[cfg(not(feature = "rdma"))]
        ReplicationChannelKind::Rdma => {
            Err("--replication-transport rdma requires --features rdma".to_string())
        }
        ReplicationChannelKind::Udp => {
            Err("udp is not supported for raft replication transport".to_string())
        }
        ReplicationChannelKind::Custom => {
            Err("custom replication transport requires explicit provider wiring".to_string())
        }
    }
}

fn replication_channel(
    transport: ReplicationChannelKind,
) -> Result<Arc<dyn ReplicationChannel>, String> {
    match transport {
        ReplicationChannelKind::Tcp => Ok(Arc::new(TcpReplicationChannel)),
        #[cfg(feature = "rdma")]
        ReplicationChannelKind::Rdma => Ok(Arc::new(RdmaReplicationChannel::default())),
        #[cfg(not(feature = "rdma"))]
        ReplicationChannelKind::Rdma => {
            Err("--replication-transport rdma requires --features rdma".to_string())
        }
        ReplicationChannelKind::Udp => {
            Err("udp is not supported for raft replication transport".to_string())
        }
        ReplicationChannelKind::Custom => {
            Err("custom replication transport requires explicit provider wiring".to_string())
        }
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
    "usage: neo4r-server [--bind ADDR] [--web-bind ADDR] [--web-auth-token TOKEN] [--slow-query-threshold-ms MS] [--data-dir DIR] [--shards N] [--partitions N] [--server-id ID] [--primary-server-id ID] [--replica-peer SERVER_ID=ADDR] [--peer SERVER_ID=ADDR] [--query-peer SERVER_ID=ADDR] [--read-preference primary|prefer-replica] [--replication-bind ADDR] [--replication-transport tcp|rdma] [--replication-ack all|quorum|async] [--replication-connect-timeout-ms MS] [--replication-retry-attempts N] [--replication-retry-backoff-ms MS] [--catch-up-on-startup] [--catch-up-interval-ms MS] [--catch-up-batch-size N] [--sync-index-catalog-on-startup] [--sync-index-catalog-interval-ms MS] [--recover-transactions-on-startup] [--recover-transactions-interval-ms MS] [--workers N] [--queue-capacity N] [--page-size N] [--daemonize]".to_string()
}

fn default_worker_count() -> usize {
    std::thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(4)
}

#[cfg(test)]
#[path = "main/tests.rs"]
mod tests;
