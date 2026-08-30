use super::*;

const GOSSIP_MEMBERSHIP_TTL_MS: u64 = 30_000;

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
    ) -> Result<bool, String> {
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
}

impl NativeExecutionContext {
    pub(crate) fn apply_gossip_node(
        &self,
        server_id: u64,
        query_address: String,
        replication_address: String,
        incarnation: u64,
        ttl_ms: u64,
    ) -> Result<String, String> {
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
}

fn gossip_millis_now() -> u64 {
    unix_millis_now().min(u64::MAX as u128) as u64
}
