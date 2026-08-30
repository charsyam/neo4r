use super::*;

const GOSSIP_MEMBERSHIP_TTL_MS: u64 = 30_000;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct GossipFanoutResult {
    pub attempted: usize,
    pub succeeded: usize,
    pub failed: usize,
    pub errors: Vec<String>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct GossipSummary {
    pub(crate) live: usize,
    pub(crate) expired: usize,
    pub(crate) replication_negotiation_pending: usize,
}

pub(crate) fn apply_gossip_node_to_stores(
    gossip_nodes: &GossipNodeStore,
    query_peers: &QueryPeerStore,
    record: GossipNodeRecord,
) -> Result<bool, String> {
    let accepted = gossip_nodes
        .upsert(record.clone())
        .map_err(|err| err.to_string())?;
    if accepted {
        query_peers
            .register(record.server_id, record.query_address)
            .map_err(|err| err.to_string())?;
    }
    Ok(accepted)
}

pub(crate) fn format_gossip_node_command(
    server_id: u64,
    query_address: &str,
    replication_address: &str,
    incarnation: u64,
    ttl_ms: u64,
    token: Option<&str>,
) -> String {
    let mut command = format!(
        "GOSSIP_NODE\t{server_id}\t{query_address}\t{replication_address}\t{incarnation}\t{ttl_ms}"
    );
    if let Some(token) = token {
        command.push('\t');
        command.push_str(token);
    }
    command
}

pub(crate) fn list_gossip_nodes_from_store(
    gossip_nodes: &GossipNodeStore,
) -> Result<String, String> {
    let now_ms = gossip_millis_now();
    let nodes = gossip_nodes.list().map_err(|err| err.to_string())?;
    Ok(format_gossip_nodes(&nodes, now_ms))
}

pub(crate) fn refresh_gossip_from_membership(
    db: &Neo4rDatabaseHandle,
    gossip_nodes: &GossipNodeStore,
    query_peers: &QueryPeerStore,
) -> Result<usize, String> {
    let membership = db.cluster_membership().map_err(|err| err.to_string())?;
    let now_ms = gossip_millis_now();
    let mut accepted = 0;
    for node in membership.nodes {
        if !membership_node_can_be_gossiped(node.state) || node.address.trim().is_empty() {
            continue;
        }
        let record = GossipNodeRecord {
            server_id: node.server_id,
            query_address: node.address.clone(),
            replication_address: node.address,
            incarnation: membership.version,
            ttl_ms: GOSSIP_MEMBERSHIP_TTL_MS,
            seen_at_ms: now_ms,
        };
        if apply_gossip_node_to_stores(gossip_nodes, query_peers, record)? {
            accepted += 1;
        }
    }
    Ok(accepted)
}

pub(crate) fn gossip_summary(
    gossip_nodes: &GossipNodeStore,
    replication_peers: &QueryPeerStore,
) -> Result<GossipSummary, String> {
    let now_ms = gossip_millis_now();
    let replication_peers = replication_peers
        .list()
        .map_err(|err| err.to_string())?
        .into_iter()
        .map(|(server_id, _)| server_id)
        .collect::<BTreeSet<_>>();
    let mut summary = GossipSummary::default();
    for node in gossip_nodes.list().map_err(|err| err.to_string())? {
        if node.is_alive_at(now_ms) {
            summary.live += 1;
            if !replication_peers.contains(&node.server_id) {
                summary.replication_negotiation_pending += 1;
            }
        } else {
            summary.expired += 1;
        }
    }
    Ok(summary)
}

pub(crate) fn gossip_summary_json(summary: &GossipSummary) -> String {
    format!(
        "{{\"live\":{},\"expired\":{},\"replication_negotiation_pending\":{}}}",
        summary.live, summary.expired, summary.replication_negotiation_pending
    )
}

fn membership_node_can_be_gossiped(state: NodeMembershipState) -> bool {
    matches!(
        state,
        NodeMembershipState::Negotiating
            | NodeMembershipState::Joining
            | NodeMembershipState::Active
            | NodeMembershipState::Draining
            | NodeMembershipState::Leaving
    )
}

impl TcpBackend {
    pub(crate) fn apply_gossip_node(
        &self,
        server_id: u64,
        query_address: String,
        replication_address: String,
        incarnation: u64,
        ttl_ms: u64,
        token: Option<String>,
    ) -> Result<bool, String> {
        self.validate_gossip_token(token.as_deref())?;
        apply_gossip_node_to_stores(
            &self.gossip_nodes,
            &self.query_peers,
            GossipNodeRecord {
                server_id,
                query_address,
                replication_address,
                incarnation,
                ttl_ms,
                seen_at_ms: gossip_millis_now(),
            },
        )
    }

    pub(crate) fn list_gossip_nodes(&self) -> Result<String, String> {
        list_gossip_nodes_from_store(&self.gossip_nodes)
    }

    pub(crate) fn refresh_gossip_from_membership(&self) -> Result<usize, String> {
        refresh_gossip_from_membership(&self.db, &self.gossip_nodes, &self.query_peers)
    }

    pub(crate) fn gossip_summary(&self) -> Result<GossipSummary, String> {
        gossip_summary(&self.gossip_nodes, &self.replication_peers)
    }

    pub(crate) fn gossip_summary_json(&self) -> Result<String, String> {
        Ok(gossip_summary_json(&self.gossip_summary()?))
    }

    pub fn negotiate_gossip_replication_peers(&self) -> GossipFanoutResult {
        let now_ms = gossip_millis_now();
        let local_server_id = self
            .db
            .cluster_status()
            .map(|status| status.server_id)
            .unwrap_or_default();
        let existing = self
            .replication_peers
            .list()
            .unwrap_or_default()
            .into_iter()
            .map(|(server_id, _)| server_id)
            .collect::<BTreeSet<_>>();
        let candidates = self
            .gossip_nodes
            .list()
            .unwrap_or_default()
            .into_iter()
            .filter(|node| node.server_id != local_server_id)
            .filter(|node| node.is_alive_at(now_ms))
            .filter(|node| !existing.contains(&node.server_id))
            .collect::<Vec<_>>();
        let mut result = GossipFanoutResult {
            attempted: candidates.len(),
            ..GossipFanoutResult::default()
        };
        for node in candidates {
            match self.negotiate_replication_peer(node.server_id, node.replication_address, None) {
                Ok(()) => {
                    result.succeeded += 1;
                    self.metrics
                        .gossip_negotiation_success
                        .fetch_add(1, Ordering::Relaxed);
                }
                Err(err) => {
                    result.failed += 1;
                    result.errors.push(format!("{}: {err}", node.server_id));
                    self.metrics
                        .gossip_negotiation_failure
                        .fetch_add(1, Ordering::Relaxed);
                }
            }
        }
        result
    }

    pub fn fanout_gossip_once(
        &self,
        local_server_id: u64,
        query_address: &str,
        replication_address: &str,
        ttl_ms: u64,
        seed_peers: &[String],
    ) -> GossipFanoutResult {
        let incarnation = self
            .db
            .cluster_membership()
            .map(|membership| membership.version)
            .unwrap_or_default()
            .max(gossip_millis_now());
        let command = format_gossip_node_command(
            local_server_id,
            query_address,
            replication_address,
            incarnation,
            ttl_ms,
            self.gossip_auth_token.get().as_deref(),
        );
        let _ = self.apply_gossip_node(
            local_server_id,
            query_address.to_string(),
            replication_address.to_string(),
            incarnation,
            ttl_ms,
            self.gossip_auth_token.get(),
        );
        let mut result = GossipFanoutResult {
            attempted: seed_peers.len(),
            ..GossipFanoutResult::default()
        };
        for peer in seed_peers {
            match request_remote_command(peer, &command) {
                Ok(response) if response.starts_with("OK\tGOSSIP") || response == "OK" => {
                    result.succeeded += 1;
                    self.metrics
                        .gossip_fanout_success
                        .fetch_add(1, Ordering::Relaxed);
                }
                Ok(response) => {
                    result.failed += 1;
                    result.errors.push(format!("{peer}: {response}"));
                    self.metrics
                        .gossip_fanout_failure
                        .fetch_add(1, Ordering::Relaxed);
                }
                Err(err) => {
                    result.failed += 1;
                    result.errors.push(format!("{peer}: {err}"));
                    self.metrics
                        .gossip_fanout_failure
                        .fetch_add(1, Ordering::Relaxed);
                }
            }
        }
        result
    }

    fn validate_gossip_token(&self, token: Option<&str>) -> Result<(), String> {
        let Some(expected) = self.gossip_auth_token.get() else {
            return Ok(());
        };
        match token {
            Some(token) if constant_time_token_eq(&expected, token) => Ok(()),
            _ => {
                self.metrics
                    .gossip_auth_failures
                    .fetch_add(1, Ordering::Relaxed);
                Err("gossip authentication failed".to_string())
            }
        }
    }
}

impl NativeExecutionContext {
    pub(crate) fn apply_gossip_node(
        &self,
        server_id: u64,
        query_address: String,
        replication_address: String,
        incarnation: u64,
        ttl_ms: u64,
        token: Option<String>,
    ) -> Result<String, String> {
        self.validate_gossip_token(token.as_deref())?;
        let accepted = apply_gossip_node_to_stores(
            &self.gossip_nodes,
            &self.query_peers,
            GossipNodeRecord {
                server_id,
                query_address,
                replication_address,
                incarnation,
                ttl_ms,
                seen_at_ms: gossip_millis_now(),
            },
        )?;
        Ok(format_response(&BackendResponse::OkGossip(format!(
            "accepted={accepted}"
        ))))
    }

    pub(crate) fn list_gossip_nodes(&self) -> Result<String, String> {
        Ok(format_response(&BackendResponse::OkGossip(
            list_gossip_nodes_from_store(&self.gossip_nodes)?,
        )))
    }

    pub(crate) fn refresh_gossip_from_membership(&self) -> Result<String, String> {
        let accepted =
            refresh_gossip_from_membership(&self.db, &self.gossip_nodes, &self.query_peers)?;
        Ok(format_response(&BackendResponse::OkGossip(format!(
            "accepted={accepted}"
        ))))
    }

    fn validate_gossip_token(&self, token: Option<&str>) -> Result<(), String> {
        let Some(expected) = self.gossip_auth_token.get() else {
            return Ok(());
        };
        match token {
            Some(token) if constant_time_token_eq(&expected, token) => Ok(()),
            _ => {
                self.metrics
                    .gossip_auth_failures
                    .fetch_add(1, Ordering::Relaxed);
                Err("gossip authentication failed".to_string())
            }
        }
    }
}

fn gossip_millis_now() -> u64 {
    unix_millis_now().min(u64::MAX as u128) as u64
}
