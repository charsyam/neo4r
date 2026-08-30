use neo4r_core::{ShardPlacement, ShardReplica, ShardRole, ShardRoutingTable};
use neo4r_db::{
    DatabaseConfig, ReplicationAckPolicy, ReplicationChannel, ReplicationChannelKind,
    ReplicationEndpoint, TcpReplicationChannel, TcpShardReplicator,
};
#[cfg(feature = "rdma")]
use neo4r_db::{RdmaReplicationChannel, RdmaReplicationListener};
use neo4r_server::{
    NativeTlsConfig, QueryReadPreference, ReplicationTlsConfig, TcpBackend, TcpBackendConfig,
    TlsReplicationChannel,
};
use std::fs::{self, OpenOptions};
use std::io;
use std::net::TcpListener;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

#[path = "config.rs"]
mod config;

pub(crate) fn run() -> Result<(), Box<dyn std::error::Error>> {
    let args = ServerArgs::parse(std::env::args().skip(1))?;
    if args.check_config {
        args.validate_runtime()?;
        eprintln!("neo4r-server config ok");
        return Ok(());
    }
    if args.production_check {
        args.validate_production()?;
        eprintln!("neo4r-server production check ok");
        return Ok(());
    }
    if args.dump_config {
        args.validate_runtime()?;
        print!("{}", args.to_yaml_summary());
        return Ok(());
    }
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
                .with_channel(replication_channel(
                    args.replication_transport,
                    args.replication_tls_channel_config()?,
                )?)
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
    let mut backend = TcpBackend::with_persistent_config(
        db,
        TcpBackendConfig {
            worker_count: args.worker_count,
            queue_capacity: args.queue_capacity,
            default_page_size: args.default_page_size,
            read_preference: args.read_preference,
            catch_up_connect_timeout: Duration::from_millis(args.replication_connect_timeout_ms),
        },
    )?
    .with_tenant_quota(
        args.tenant_max_concurrent_queries,
        args.tenant_max_result_rows,
    )
    .with_replication_tls_channel_config(args.replication_tls_channel_config()?)
    .with_web_options(
        args.web_auth_token.clone(),
        Duration::from_millis(args.slow_query_threshold_ms),
    )
    .with_multi_tenant_config(backend_config_template)?;
    if let Some(tls_config) = args.native_tls_config()? {
        backend = backend.with_native_tls_config(tls_config)?;
    }
    if let Some(tls_config) = args.replication_tls_acceptor_config()? {
        backend = backend.with_replication_tls_config(tls_config)?;
    }
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

mod args;
use args::*;

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
