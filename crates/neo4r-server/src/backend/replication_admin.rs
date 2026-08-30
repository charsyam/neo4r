use super::*;
use neo4r_db::ReplicationChannel;
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct ReplicationPeerStatusEntry {
    server_id: u64,
    address: Option<String>,
    primary_shards: Vec<u64>,
    replica_shards: Vec<u64>,
}

pub(crate) fn replication_peer_status(
    db: &Neo4rDatabaseHandle,
    replication_peers: &QueryPeerStore,
    server_id: Option<u64>,
) -> Result<Vec<ReplicationPeerStatusEntry>, String> {
    let routing_table = db.routing_table().map_err(|err| err.to_string())?;
    let local_server_id = db
        .cluster_status()
        .map_err(|err| err.to_string())?
        .server_id;
    let peer_addresses = replication_peers
        .list()
        .map_err(|err| err.to_string())?
        .into_iter()
        .collect::<BTreeMap<_, _>>();
    let mut server_ids = BTreeSet::new();
    if let Some(server_id) = server_id {
        server_ids.insert(server_id);
    } else {
        server_ids.extend(peer_addresses.keys().copied());
        for placement in &routing_table.placements {
            for replica in &placement.replicas {
                if replica.server_id != local_server_id {
                    server_ids.insert(replica.server_id);
                }
            }
        }
    }

    let mut statuses = Vec::new();
    for server_id in server_ids {
        let mut primary_shards = Vec::new();
        let mut replica_shards = Vec::new();
        for placement in &routing_table.placements {
            if placement.primary_server_id() == Some(server_id) {
                primary_shards.push(placement.shard_id);
            } else if placement.has_server(server_id) {
                replica_shards.push(placement.shard_id);
            }
        }
        statuses.push(ReplicationPeerStatusEntry {
            server_id,
            address: peer_addresses.get(&server_id).cloned(),
            primary_shards,
            replica_shards,
        });
    }
    statuses.sort_by_key(|entry| entry.server_id);
    Ok(statuses)
}

pub(crate) fn format_replication_peer_status(entries: &[ReplicationPeerStatusEntry]) -> String {
    entries
        .iter()
        .map(|entry| {
            let address = entry.address.as_deref().unwrap_or("missing");
            format!(
                "server={} address={} primary_shards={} replica_shards={}",
                entry.server_id,
                address,
                format_shard_id_list(&entry.primary_shards),
                format_shard_id_list(&entry.replica_shards)
            )
        })
        .collect::<Vec<_>>()
        .join(",")
}

pub(crate) fn format_shard_id_list(shards: &[u64]) -> String {
    if shards.is_empty() {
        "-".to_string()
    } else {
        shards
            .iter()
            .map(u64::to_string)
            .collect::<Vec<_>>()
            .join("|")
    }
}

pub(crate) fn replication_status(
    db: &Neo4rDatabaseHandle,
    replication_peers: &QueryPeerStore,
    replication_peer_identities: &ReplicationPeerIdentityStore,
) -> Result<String, String> {
    let status = db.cluster_status().map_err(|err| err.to_string())?;
    let peers = replication_peers.list().map_err(|err| err.to_string())?;
    let peers = if peers.is_empty() {
        "none".to_string()
    } else {
        format_query_peers(&peers)
    };
    let shards = status
        .shards
        .iter()
        .map(format_replication_shard_status)
        .collect::<Vec<_>>()
        .join(",");
    let metrics = db
        .replication_channel_metrics()
        .map_err(|err| err.to_string())?
        .map(|metrics| {
            format!(
                "sent_batches:{}|acked_batches:{}|failed_batches:{}|sent_entries:{}|sent_bytes:{}",
                metrics.sent_batches,
                metrics.acked_batches,
                metrics.failed_batches,
                metrics.sent_entries,
                metrics.sent_bytes
            )
        })
        .unwrap_or_else(|| "unavailable".to_string());
    let identities = replication_peer_identities
        .list()
        .map_err(|err| err.to_string())?;
    let identities = if identities.is_empty() {
        "none".to_string()
    } else {
        identities
            .iter()
            .map(|identity| {
                format!(
                    "{}:{}:{}:{}",
                    identity.server_id,
                    identity.node_id.unwrap_or(0),
                    identity.cluster_id,
                    identity.database_id
                )
            })
            .collect::<Vec<_>>()
            .join("|")
    };
    Ok(format!(
        "server={} routing_version={} peers={} identities={} shards={} metrics={}",
        status.server_id, status.routing_version, peers, identities, shards, metrics
    ))
}

pub(crate) fn cluster_registry(
    db: &Neo4rDatabaseHandle,
    query_peers: &QueryPeerStore,
    database: &str,
) -> Result<String, String> {
    let status = db.cluster_status().map_err(|err| err.to_string())?;
    let routing_table = db.routing_table().map_err(|err| err.to_string())?;
    let generated_at = unix_millis_now();
    let ttl_ms = 5_000_u64;
    let peers = query_peers
        .list()
        .map_err(|err| err.to_string())?
        .into_iter()
        .collect::<BTreeMap<_, _>>();
    let management = db.cluster_management_status().ok();
    let membership_index = management
        .as_ref()
        .map(|management| management.membership.version)
        .unwrap_or(routing_table.version);
    let metadata_index = db
        .metadata_operations()
        .map(|records| {
            records
                .last()
                .map(|record| record.index)
                .unwrap_or_default()
        })
        .unwrap_or_default();
    let migration_state = management
        .as_ref()
        .and_then(|management| management.rebalance_execution.as_ref())
        .map(|execution| format!("{:?}:step={}", execution.state, execution.current_step))
        .unwrap_or_else(|| "idle".to_string());
    let raft = db
        .raft_status()
        .map(|shards| {
            shards
                .iter()
                .map(|shard| {
                    format!(
                        "{}:term={}:role={:?}:leader={}",
                        shard.shard_id,
                        shard.term,
                        shard.role,
                        shard
                            .leader_id
                            .map(|leader| leader.to_string())
                            .unwrap_or_else(|| "none".to_string())
                    )
                })
                .collect::<Vec<_>>()
                .join("|")
        })
        .unwrap_or_else(|_| "unknown".to_string());
    let nodes = management
        .map(|management| management.membership.nodes)
        .unwrap_or_default();
    let nodes = nodes
        .iter()
        .map(|node| {
            let address = if node.address.is_empty() {
                peers.get(&node.server_id).cloned().unwrap_or_default()
            } else {
                node.address.clone()
            };
            format!(
                "{}:{}:{}",
                node.server_id,
                registry_node_state(node.state),
                address
            )
        })
        .collect::<Vec<_>>()
        .join("|");
    let query_peers = if peers.is_empty() {
        "none".to_string()
    } else {
        peers
            .iter()
            .map(|(server_id, address)| format!("{server_id}:{address}"))
            .collect::<Vec<_>>()
            .join("|")
    };
    Ok(format!(
        "database={} local_server={} routing_version={} ownership_epoch={} membership_index={} metadata_index={} generated_at={} ttl_ms={} migration={} raft={} write_authority={} query_peers={} nodes={} routing={}",
        database,
        status.server_id,
        routing_table.version,
        routing_table.version,
        membership_index,
        metadata_index,
        generated_at,
        ttl_ms,
        migration_state,
        raft,
        "shard_primary_and_raft_leader",
        query_peers,
        nodes,
        format_routing_table(&routing_table)
    ))
}

fn registry_node_state(state: NodeMembershipState) -> &'static str {
    match state {
        NodeMembershipState::Negotiating => "negotiating",
        NodeMembershipState::Joining => "joining",
        NodeMembershipState::Active => "active",
        NodeMembershipState::Draining => "draining",
        NodeMembershipState::Leaving => "leaving",
        NodeMembershipState::Removed => "removed",
        NodeMembershipState::Dead => "dead",
        NodeMembershipState::Rejected => "rejected",
    }
}

pub(crate) fn format_replication_shard_status(status: &neo4r_db::ShardStatus) -> String {
    let primary = status
        .primary_server_id
        .map(|server_id| server_id.to_string())
        .unwrap_or_else(|| "none".to_string());
    let replicas = if status.replica_server_ids.is_empty() {
        "none".to_string()
    } else {
        status
            .replica_server_ids
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("|")
    };
    let matches = if status.match_indexes.is_empty() {
        "none".to_string()
    } else {
        status
            .match_indexes
            .iter()
            .map(|(server_id, index)| format!("{server_id}:{index}"))
            .collect::<Vec<_>>()
            .join("|")
    };
    let lag = if status.replica_server_ids.is_empty() {
        "none".to_string()
    } else {
        status
            .replica_server_ids
            .iter()
            .map(|server_id| {
                let match_index = status
                    .match_indexes
                    .iter()
                    .find(|(matched_server_id, _)| matched_server_id == server_id)
                    .map(|(_, index)| *index);
                match match_index {
                    Some(index) => {
                        format!(
                            "{server_id}:{}",
                            status.committed_index.saturating_sub(index)
                        )
                    }
                    None => format!("{server_id}:unknown"),
                }
            })
            .collect::<Vec<_>>()
            .join("|")
    };
    format!(
        "shard:{}:primary={}:replicas={}:local={}:local_primary={}:applied={}:committed={}:match={}:lag={}",
        status.shard_id,
        primary,
        replicas,
        status.has_local_copy,
        status.is_local_primary,
        status.applied_index,
        status.committed_index,
        matches,
        lag
    )
}

pub(crate) fn catch_up_from_primaries(
    db: &Neo4rDatabaseHandle,
    replication_peers: &QueryPeerStore,
    connect_timeout: Duration,
    max_entries_per_request: Option<usize>,
    tls_config: &ReplicationTlsChannelConfigStore,
) -> Result<Vec<neo4r_db::TcpCatchUpResult>, String> {
    if let Some(tls_config) = tls_config.get() {
        return catch_up_from_primaries_with_tls(
            db,
            replication_peers,
            connect_timeout,
            max_entries_per_request,
            tls_config,
        );
    }
    let routing_table = db.routing_table().map_err(|err| err.to_string())?;
    let status = db.cluster_status().map_err(|err| err.to_string())?;
    let peer_addresses = replication_peers
        .list()
        .map_err(|err| err.to_string())?
        .into_iter()
        .collect::<BTreeMap<_, _>>();
    match max_entries_per_request {
        Some(max_entries_per_request) => neo4r_db::catch_up_from_tcp_primaries_batched(
            db,
            &routing_table,
            &peer_addresses,
            status.server_id,
            connect_timeout,
            max_entries_per_request,
        ),
        None => neo4r_db::catch_up_from_tcp_primaries(
            db,
            &routing_table,
            &peer_addresses,
            status.server_id,
            connect_timeout,
        ),
    }
    .map_err(|err| err.to_string())
}

pub(crate) fn catch_up_from_primary(
    db: &Neo4rDatabaseHandle,
    replication_peers: &QueryPeerStore,
    connect_timeout: Duration,
    server_id: u64,
    max_entries_per_request: Option<usize>,
    tls_config: &ReplicationTlsChannelConfigStore,
) -> Result<Vec<neo4r_db::TcpCatchUpResult>, String> {
    if let Some(tls_config) = tls_config.get() {
        return catch_up_from_primary_with_tls(
            db,
            replication_peers,
            connect_timeout,
            server_id,
            max_entries_per_request,
            tls_config,
        );
    }
    let routing_table = db.routing_table().map_err(|err| err.to_string())?;
    let status = db.cluster_status().map_err(|err| err.to_string())?;
    let peer_addresses = replication_peers
        .list()
        .map_err(|err| err.to_string())?
        .into_iter()
        .collect::<BTreeMap<_, _>>();
    let address = peer_addresses
        .get(&server_id)
        .ok_or_else(|| format!("missing peer address for primary server {server_id}"))?;
    let committed_indexes = db.committed_indexes().map_err(|err| err.to_string())?;
    let mut results = Vec::new();
    for placement in &routing_table.placements {
        if !placement.has_server(status.server_id) {
            continue;
        }
        if placement.primary_server_id() != Some(server_id) {
            continue;
        }
        if server_id == status.server_id {
            continue;
        }
        let start_index = committed_indexes
            .get(placement.shard_id as usize)
            .copied()
            .ok_or_else(|| format!("missing committed index for shard {}", placement.shard_id))?
            + 1;
        let fetched_entries = match max_entries_per_request {
            Some(max_entries_per_request) => catch_up_from_tcp_primary_batched(
                db,
                address,
                connect_timeout,
                placement.shard_id,
                start_index,
                max_entries_per_request,
            ),
            None => catch_up_from_tcp_primary(
                db,
                address,
                connect_timeout,
                placement.shard_id,
                start_index,
            ),
        }
        .map_err(|err| err.to_string())?;
        results.push(neo4r_db::TcpCatchUpResult {
            shard_id: placement.shard_id,
            start_index,
            end_index: catch_up_end_index(start_index, fetched_entries),
            fetched_entries,
            primary_server_id: server_id,
        });
    }
    Ok(results)
}

fn catch_up_from_primaries_with_tls(
    db: &Neo4rDatabaseHandle,
    replication_peers: &QueryPeerStore,
    connect_timeout: Duration,
    max_entries_per_request: Option<usize>,
    tls_config: ReplicationTlsConfig,
) -> Result<Vec<neo4r_db::TcpCatchUpResult>, String> {
    let routing_table = db.routing_table().map_err(|err| err.to_string())?;
    let status = db.cluster_status().map_err(|err| err.to_string())?;
    let peer_addresses = replication_peers
        .list()
        .map_err(|err| err.to_string())?
        .into_iter()
        .collect::<BTreeMap<_, _>>();
    let mut results = Vec::new();
    for placement in &routing_table.placements {
        let Some(primary_server_id) = placement.primary_server_id() else {
            return Err(format!("missing primary for shard {}", placement.shard_id));
        };
        if !placement.has_server(status.server_id) || primary_server_id == status.server_id {
            continue;
        }
        let Some(address) = peer_addresses.get(&primary_server_id) else {
            return Err(format!(
                "missing peer address for primary server {primary_server_id} on shard {}",
                placement.shard_id
            ));
        };
        let committed_indexes = db.committed_indexes().map_err(|err| err.to_string())?;
        let start_index = committed_indexes
            .get(placement.shard_id as usize)
            .copied()
            .ok_or_else(|| format!("missing committed index for shard {}", placement.shard_id))?
            + 1;
        let fetched_entries = catch_up_tls_shard(
            db,
            address,
            connect_timeout,
            placement.shard_id,
            start_index,
            max_entries_per_request,
            &tls_config,
        )?;
        results.push(neo4r_db::TcpCatchUpResult {
            shard_id: placement.shard_id,
            start_index,
            end_index: catch_up_end_index(start_index, fetched_entries),
            fetched_entries,
            primary_server_id,
        });
    }
    Ok(results)
}

fn catch_up_from_primary_with_tls(
    db: &Neo4rDatabaseHandle,
    replication_peers: &QueryPeerStore,
    connect_timeout: Duration,
    server_id: u64,
    max_entries_per_request: Option<usize>,
    tls_config: ReplicationTlsConfig,
) -> Result<Vec<neo4r_db::TcpCatchUpResult>, String> {
    let routing_table = db.routing_table().map_err(|err| err.to_string())?;
    let status = db.cluster_status().map_err(|err| err.to_string())?;
    let peer_addresses = replication_peers
        .list()
        .map_err(|err| err.to_string())?
        .into_iter()
        .collect::<BTreeMap<_, _>>();
    let address = peer_addresses
        .get(&server_id)
        .ok_or_else(|| format!("missing peer address for primary server {server_id}"))?;
    let committed_indexes = db.committed_indexes().map_err(|err| err.to_string())?;
    let mut results = Vec::new();
    for placement in &routing_table.placements {
        if !placement.has_server(status.server_id)
            || placement.primary_server_id() != Some(server_id)
            || server_id == status.server_id
        {
            continue;
        }
        let start_index = committed_indexes
            .get(placement.shard_id as usize)
            .copied()
            .ok_or_else(|| format!("missing committed index for shard {}", placement.shard_id))?
            + 1;
        let fetched_entries = catch_up_tls_shard(
            db,
            address,
            connect_timeout,
            placement.shard_id,
            start_index,
            max_entries_per_request,
            &tls_config,
        )?;
        results.push(neo4r_db::TcpCatchUpResult {
            shard_id: placement.shard_id,
            start_index,
            end_index: catch_up_end_index(start_index, fetched_entries),
            fetched_entries,
            primary_server_id: server_id,
        });
    }
    Ok(results)
}

fn catch_up_tls_shard(
    db: &Neo4rDatabaseHandle,
    address: &str,
    connect_timeout: Duration,
    shard_id: u64,
    start_index: u64,
    max_entries_per_request: Option<usize>,
    tls_config: &ReplicationTlsConfig,
) -> Result<usize, String> {
    let endpoint = ReplicationEndpoint::tcp(address.to_string());
    let channel = TlsReplicationChannel::new(tls_config.clone());
    let config = neo4r_db::ReplicationChannelConfig {
        connect_timeout,
        ..neo4r_db::ReplicationChannelConfig::default()
    };
    let entries = channel
        .catch_up(
            &endpoint,
            &config,
            shard_id,
            start_index,
            max_entries_per_request,
        )
        .map_err(|err| err.to_string())?;
    let count = entries.len();
    db.apply_replicated_entries(entries)
        .map_err(|err| err.to_string())?;
    Ok(count)
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct CatchUpPlanEntry {
    shard_id: u64,
    primary_server_id: u64,
    start_index: u64,
    peer_registered: bool,
}

pub(crate) fn catch_up_plan(
    db: &Neo4rDatabaseHandle,
    replication_peers: &QueryPeerStore,
    server_id: Option<u64>,
) -> Result<Vec<CatchUpPlanEntry>, String> {
    let routing_table = db.routing_table().map_err(|err| err.to_string())?;
    let status = db.cluster_status().map_err(|err| err.to_string())?;
    let peer_addresses = replication_peers
        .list()
        .map_err(|err| err.to_string())?
        .into_iter()
        .collect::<BTreeMap<_, _>>();
    let committed_indexes = db.committed_indexes().map_err(|err| err.to_string())?;
    let mut entries = Vec::new();
    for placement in &routing_table.placements {
        if !placement.has_server(status.server_id) {
            continue;
        }
        let Some(primary_server_id) = placement.primary_server_id() else {
            return Err(format!("missing primary for shard {}", placement.shard_id));
        };
        if Some(primary_server_id) != server_id && server_id.is_some() {
            continue;
        }
        if primary_server_id == status.server_id {
            continue;
        }
        let start_index = committed_indexes
            .get(placement.shard_id as usize)
            .copied()
            .ok_or_else(|| format!("missing committed index for shard {}", placement.shard_id))?
            + 1;
        entries.push(CatchUpPlanEntry {
            shard_id: placement.shard_id,
            primary_server_id,
            start_index,
            peer_registered: peer_addresses.contains_key(&primary_server_id),
        });
    }
    entries.sort_by_key(|entry| (entry.primary_server_id, entry.shard_id));
    Ok(entries)
}

pub(crate) fn catch_up_end_index(start_index: u64, fetched_entries: usize) -> u64 {
    start_index
        .saturating_add(fetched_entries as u64)
        .saturating_sub(1)
}

pub(crate) fn format_catch_up_plan(entries: &[CatchUpPlanEntry]) -> String {
    entries
        .iter()
        .map(|entry| {
            let peer = if entry.peer_registered {
                "registered"
            } else {
                "missing"
            };
            format!(
                "shard={} primary={} start={} peer={peer}",
                entry.shard_id, entry.primary_server_id, entry.start_index
            )
        })
        .collect::<Vec<_>>()
        .join(",")
}

pub(crate) fn format_catch_up_results(results: &[neo4r_db::TcpCatchUpResult]) -> String {
    results
        .iter()
        .map(|result| {
            format!(
                "shard={} primary={} start={} end={} fetched={}",
                result.shard_id,
                result.primary_server_id,
                result.start_index,
                result.end_index,
                result.fetched_entries
            )
        })
        .collect::<Vec<_>>()
        .join(",")
}

pub(crate) fn sync_index_catalog_from_peer(
    db: &Neo4rDatabaseHandle,
    query_peers: &QueryPeerStore,
    server_id: u64,
) -> Result<(), String> {
    let address = query_peers
        .address(server_id)?
        .ok_or_else(|| format!("missing query peer address for server {server_id}"))?;
    let response = request_remote_command(&address, "DUMP_INDEX_CATALOG")?;
    let catalog = parse_ok_index_catalog_response(&response)?;
    db.install_index_catalog(catalog)
        .map_err(|err| err.to_string())
}
