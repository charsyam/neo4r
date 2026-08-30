use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

pub(super) const QUERY_PEERS_FILE: &str = "query-peers.txt";
pub(super) const REPLICATION_PEERS_FILE: &str = "replication-peers.txt";
pub(super) const REPLICATION_PEER_IDENTITIES_FILE: &str = "replication-peer-identities.txt";
pub(super) const GOSSIP_NODES_FILE: &str = "gossip-nodes.txt";

const PEER_STORE_MAGIC: &str = "N4RPEERS1";
const REPLICATION_PEER_IDENTITY_MAGIC: &str = "N4RREPLPEERS2";
const GOSSIP_NODE_MAGIC: &str = "N4RGOSSIP1";

#[derive(Clone, Default)]
pub(super) struct QueryPeerStore {
    peers: Arc<Mutex<BTreeMap<u64, String>>>,
    path: Option<Arc<PathBuf>>,
}

impl QueryPeerStore {
    pub(super) fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        Ok(Self {
            peers: Arc::new(Mutex::new(load_peer_store(&path)?)),
            path: Some(Arc::new(path)),
        })
    }

    pub(super) fn register(&self, server_id: u64, address: String) -> io::Result<()> {
        let snapshot = {
            let mut peers = self
                .peers
                .lock()
                .map_err(|_| io::Error::other("query peer store lock poisoned"))?;
            peers.insert(server_id, address);
            peers.clone()
        };
        self.save(&snapshot)
    }

    pub(super) fn unregister(&self, server_id: u64) -> io::Result<()> {
        let snapshot = {
            let mut peers = self
                .peers
                .lock()
                .map_err(|_| io::Error::other("query peer store lock poisoned"))?;
            peers.remove(&server_id);
            peers.clone()
        };
        self.save(&snapshot)
    }

    pub(super) fn list(&self) -> io::Result<Vec<(u64, String)>> {
        Ok(self
            .peers
            .lock()
            .map_err(|_| io::Error::other("query peer store lock poisoned"))?
            .iter()
            .map(|(server_id, address)| (*server_id, address.clone()))
            .collect())
    }

    pub(super) fn address(&self, server_id: u64) -> Result<Option<String>, String> {
        Ok(self
            .peers
            .lock()
            .map_err(|_| "query peer store lock poisoned".to_string())?
            .get(&server_id)
            .cloned())
    }

    fn save(&self, peers: &BTreeMap<u64, String>) -> io::Result<()> {
        let Some(path) = self.path.as_ref() else {
            return Ok(());
        };
        save_peer_store(path, peers)
    }
}

fn load_peer_store(path: &Path) -> io::Result<BTreeMap<u64, String>> {
    let file = match File::open(path) {
        Ok(file) => file,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(BTreeMap::new()),
        Err(err) => return Err(err),
    };
    let mut lines = BufReader::new(file).lines();
    let header = lines
        .next()
        .transpose()?
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing peer store header"))?;
    if header != PEER_STORE_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid peer store header",
        ));
    }

    let mut peers = BTreeMap::new();
    for line in lines {
        let line = line?;
        if line.is_empty() {
            continue;
        }
        let (server_id, address) = line
            .split_once('\t')
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "invalid peer record"))?;
        if address.contains(['\t', '\n', '\r']) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid peer address",
            ));
        }
        let server_id = server_id
            .parse::<u64>()
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid peer server id"))?;
        peers.insert(server_id, address.to_string());
    }
    Ok(peers)
}

fn save_peer_store(path: &Path, peers: &BTreeMap<u64, String>) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let tmp_path = path.with_extension("tmp");
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&tmp_path)?;
    writeln!(file, "{PEER_STORE_MAGIC}")?;
    for (server_id, address) in peers {
        writeln!(file, "{server_id}\t{address}")?;
    }
    file.sync_all()?;
    drop(file);
    fs::rename(&tmp_path, path)?;
    if let Some(parent) = path.parent() {
        File::open(parent)?.sync_all()?;
    }
    Ok(())
}

pub(super) fn format_query_peers(peers: &[(u64, String)]) -> String {
    peers
        .iter()
        .map(|(server_id, address)| format!("{server_id}={address}"))
        .collect::<Vec<_>>()
        .join(",")
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct GossipNodeRecord {
    pub(super) server_id: u64,
    pub(super) query_address: String,
    pub(super) replication_address: String,
    pub(super) incarnation: u64,
    pub(super) ttl_ms: u64,
    pub(super) seen_at_ms: u64,
}

impl GossipNodeRecord {
    pub(super) fn is_alive_at(&self, now_ms: u64) -> bool {
        self.ttl_ms == 0 || now_ms.saturating_sub(self.seen_at_ms) <= self.ttl_ms
    }
}

#[derive(Clone, Default)]
pub(super) struct GossipNodeStore {
    nodes: Arc<Mutex<BTreeMap<u64, GossipNodeRecord>>>,
    path: Option<Arc<PathBuf>>,
}

impl GossipNodeStore {
    pub(super) fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        Ok(Self {
            nodes: Arc::new(Mutex::new(load_gossip_nodes(&path)?)),
            path: Some(Arc::new(path)),
        })
    }

    pub(super) fn upsert(&self, record: GossipNodeRecord) -> io::Result<bool> {
        validate_gossip_node(&record)?;
        let (accepted, snapshot) = {
            let mut nodes = self
                .nodes
                .lock()
                .map_err(|_| io::Error::other("gossip node store lock poisoned"))?;
            let accepted = nodes
                .get(&record.server_id)
                .is_none_or(|old| record.incarnation >= old.incarnation);
            if accepted {
                nodes.insert(record.server_id, record);
            }
            (accepted, nodes.clone())
        };
        if accepted {
            self.save(&snapshot)?;
        }
        Ok(accepted)
    }

    pub(super) fn list(&self) -> io::Result<Vec<GossipNodeRecord>> {
        Ok(self
            .nodes
            .lock()
            .map_err(|_| io::Error::other("gossip node store lock poisoned"))?
            .values()
            .cloned()
            .collect())
    }

    #[cfg(test)]
    pub(super) fn live_query_address(
        &self,
        server_id: u64,
        now_ms: u64,
    ) -> io::Result<Option<String>> {
        Ok(self
            .nodes
            .lock()
            .map_err(|_| io::Error::other("gossip node store lock poisoned"))?
            .get(&server_id)
            .filter(|record| record.is_alive_at(now_ms))
            .map(|record| record.query_address.clone()))
    }

    fn save(&self, nodes: &BTreeMap<u64, GossipNodeRecord>) -> io::Result<()> {
        let Some(path) = self.path.as_ref() else {
            return Ok(());
        };
        save_gossip_nodes(path, nodes)
    }
}

pub(super) fn format_gossip_nodes(nodes: &[GossipNodeRecord], now_ms: u64) -> String {
    nodes
        .iter()
        .map(|node| {
            format!(
                "{}:query={}:replication={}:incarnation={}:ttl_ms={}:seen_at_ms={}:state={}",
                node.server_id,
                node.query_address,
                node.replication_address,
                node.incarnation,
                node.ttl_ms,
                node.seen_at_ms,
                if node.is_alive_at(now_ms) {
                    "alive"
                } else {
                    "expired"
                }
            )
        })
        .collect::<Vec<_>>()
        .join(",")
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ReplicationPeerIdentity {
    pub(super) server_id: u64,
    pub(super) address: String,
    pub(super) node_id: Option<u64>,
    pub(super) transport: String,
    pub(super) cluster_id: String,
    pub(super) database_id: String,
}

impl ReplicationPeerIdentity {
    pub(super) fn tcp(
        server_id: u64,
        address: impl Into<String>,
        node_id: Option<u64>,
        cluster_id: impl Into<String>,
        database_id: impl Into<String>,
    ) -> Self {
        Self {
            server_id,
            address: address.into(),
            node_id,
            transport: "tcp".to_string(),
            cluster_id: cluster_id.into(),
            database_id: database_id.into(),
        }
    }
}

#[derive(Clone, Default)]
pub(super) struct ReplicationPeerIdentityStore {
    identities: Arc<Mutex<BTreeMap<u64, ReplicationPeerIdentity>>>,
    path: Option<Arc<PathBuf>>,
}

impl ReplicationPeerIdentityStore {
    pub(super) fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        Ok(Self {
            identities: Arc::new(Mutex::new(load_replication_peer_identities(&path)?)),
            path: Some(Arc::new(path)),
        })
    }

    pub(super) fn register(&self, identity: ReplicationPeerIdentity) -> io::Result<()> {
        validate_replication_peer_identity_record(&identity)?;
        let snapshot = {
            let mut identities = self
                .identities
                .lock()
                .map_err(|_| io::Error::other("replication peer identity lock poisoned"))?;
            identities.insert(identity.server_id, identity);
            identities.clone()
        };
        self.save(&snapshot)
    }

    pub(super) fn unregister(&self, server_id: u64) -> io::Result<()> {
        let snapshot = {
            let mut identities = self
                .identities
                .lock()
                .map_err(|_| io::Error::other("replication peer identity lock poisoned"))?;
            identities.remove(&server_id);
            identities.clone()
        };
        self.save(&snapshot)
    }

    pub(super) fn list(&self) -> io::Result<Vec<ReplicationPeerIdentity>> {
        Ok(self
            .identities
            .lock()
            .map_err(|_| io::Error::other("replication peer identity lock poisoned"))?
            .values()
            .cloned()
            .collect())
    }

    pub(super) fn get(&self, server_id: u64) -> io::Result<Option<ReplicationPeerIdentity>> {
        Ok(self
            .identities
            .lock()
            .map_err(|_| io::Error::other("replication peer identity lock poisoned"))?
            .get(&server_id)
            .cloned())
    }

    pub(super) fn would_create_cycle(
        &self,
        server_id: u64,
        node_id: Option<u64>,
    ) -> io::Result<bool> {
        let Some(mut current) = node_id else {
            return Ok(false);
        };
        if current == server_id {
            return Ok(false);
        }
        let identities = self
            .identities
            .lock()
            .map_err(|_| io::Error::other("replication peer identity lock poisoned"))?;
        let mut seen = BTreeMap::<u64, ()>::new();
        seen.insert(server_id, ());
        loop {
            if seen.contains_key(&current) {
                return Ok(true);
            }
            seen.insert(current, ());
            let Some(identity) = identities.get(&current) else {
                return Ok(false);
            };
            let Some(next) = identity.node_id else {
                return Ok(false);
            };
            current = next;
        }
    }

    fn save(&self, identities: &BTreeMap<u64, ReplicationPeerIdentity>) -> io::Result<()> {
        let Some(path) = self.path.as_ref() else {
            return Ok(());
        };
        save_replication_peer_identities(path, identities)
    }
}

fn load_replication_peer_identities(
    path: &Path,
) -> io::Result<BTreeMap<u64, ReplicationPeerIdentity>> {
    let file = match File::open(path) {
        Ok(file) => file,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(BTreeMap::new()),
        Err(err) => return Err(err),
    };
    let mut lines = BufReader::new(file).lines();
    let header = lines.next().transpose()?.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "missing replication peer identity header",
        )
    })?;
    if header != REPLICATION_PEER_IDENTITY_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid replication peer identity header",
        ));
    }
    let mut identities = BTreeMap::new();
    for line in lines {
        let line = line?;
        if line.is_empty() {
            continue;
        }
        let identity = decode_replication_peer_identity(&line)?;
        identities.insert(identity.server_id, identity);
    }
    Ok(identities)
}

fn save_replication_peer_identities(
    path: &Path,
    identities: &BTreeMap<u64, ReplicationPeerIdentity>,
) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let tmp_path = path.with_extension("tmp");
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&tmp_path)?;
    writeln!(file, "{REPLICATION_PEER_IDENTITY_MAGIC}")?;
    for identity in identities.values() {
        writeln!(file, "{}", encode_replication_peer_identity(identity))?;
    }
    file.sync_all()?;
    drop(file);
    fs::rename(&tmp_path, path)?;
    if let Some(parent) = path.parent() {
        File::open(parent)?.sync_all()?;
    }
    Ok(())
}

fn encode_replication_peer_identity(identity: &ReplicationPeerIdentity) -> String {
    format!(
        "{}\t{}\t{}\t{}\t{}\t{}",
        identity.server_id,
        identity.address,
        identity
            .node_id
            .map(|node_id| node_id.to_string())
            .unwrap_or_else(|| "-".to_string()),
        identity.transport,
        identity.cluster_id,
        identity.database_id
    )
}

fn decode_replication_peer_identity(line: &str) -> io::Result<ReplicationPeerIdentity> {
    let parts = line.split('\t').collect::<Vec<_>>();
    if parts.len() != 6 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid replication peer identity record",
        ));
    }
    let server_id = parts[0].parse::<u64>().map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid replication peer identity server id",
        )
    })?;
    let node_id = if parts[2] == "-" {
        None
    } else {
        Some(parts[2].parse::<u64>().map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid replication peer identity node id",
            )
        })?)
    };
    let identity = ReplicationPeerIdentity {
        server_id,
        address: parts[1].to_string(),
        node_id,
        transport: parts[3].to_string(),
        cluster_id: parts[4].to_string(),
        database_id: parts[5].to_string(),
    };
    validate_replication_peer_identity_record(&identity)?;
    Ok(identity)
}

fn validate_replication_peer_identity_record(identity: &ReplicationPeerIdentity) -> io::Result<()> {
    for value in [
        identity.address.as_str(),
        identity.transport.as_str(),
        identity.cluster_id.as_str(),
        identity.database_id.as_str(),
    ] {
        if value.is_empty() || value.contains(['\t', '\n', '\r']) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid replication peer identity field",
            ));
        }
    }
    Ok(())
}

fn load_gossip_nodes(path: &Path) -> io::Result<BTreeMap<u64, GossipNodeRecord>> {
    let file = match File::open(path) {
        Ok(file) => file,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(BTreeMap::new()),
        Err(err) => return Err(err),
    };
    let mut lines = BufReader::new(file).lines();
    let header = lines
        .next()
        .transpose()?
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing gossip node header"))?;
    if header != GOSSIP_NODE_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid gossip node header",
        ));
    }
    let mut nodes = BTreeMap::new();
    for line in lines {
        let line = line?;
        if line.is_empty() {
            continue;
        }
        let record = decode_gossip_node(&line)?;
        nodes.insert(record.server_id, record);
    }
    Ok(nodes)
}

fn save_gossip_nodes(path: &Path, nodes: &BTreeMap<u64, GossipNodeRecord>) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let tmp_path = path.with_extension("tmp");
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&tmp_path)?;
    writeln!(file, "{GOSSIP_NODE_MAGIC}")?;
    for node in nodes.values() {
        writeln!(file, "{}", encode_gossip_node(node))?;
    }
    file.sync_all()?;
    drop(file);
    fs::rename(&tmp_path, path)?;
    if let Some(parent) = path.parent() {
        File::open(parent)?.sync_all()?;
    }
    Ok(())
}

fn encode_gossip_node(record: &GossipNodeRecord) -> String {
    format!(
        "{}\t{}\t{}\t{}\t{}\t{}",
        record.server_id,
        record.query_address,
        record.replication_address,
        record.incarnation,
        record.ttl_ms,
        record.seen_at_ms
    )
}

fn decode_gossip_node(line: &str) -> io::Result<GossipNodeRecord> {
    let parts = line.split('\t').collect::<Vec<_>>();
    if parts.len() != 6 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid gossip node record",
        ));
    }
    let record = GossipNodeRecord {
        server_id: parse_gossip_u64(parts[0], "server id")?,
        query_address: parts[1].to_string(),
        replication_address: parts[2].to_string(),
        incarnation: parse_gossip_u64(parts[3], "incarnation")?,
        ttl_ms: parse_gossip_u64(parts[4], "ttl ms")?,
        seen_at_ms: parse_gossip_u64(parts[5], "seen at ms")?,
    };
    validate_gossip_node(&record)?;
    Ok(record)
}

fn parse_gossip_u64(value: &str, field: &str) -> io::Result<u64> {
    value.parse::<u64>().map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid gossip node {field}"),
        )
    })
}

fn validate_gossip_node(record: &GossipNodeRecord) -> io::Result<()> {
    if record.server_id == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "gossip node server id must be greater than zero",
        ));
    }
    for value in [&record.query_address, &record.replication_address] {
        if value.is_empty() || value.contains(['\t', '\n', '\r']) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid gossip node address",
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_path(name: &str) -> PathBuf {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("neo4r-{name}-{suffix}.txt"))
    }

    #[test]
    fn replication_peer_identity_store_persists_records() {
        let path = temp_path("replication-peer-identity");
        let store = ReplicationPeerIdentityStore::open(&path).unwrap();
        store
            .register(ReplicationPeerIdentity::tcp(
                2,
                "127.0.0.1:17688",
                Some(2),
                "cluster-a",
                "default",
            ))
            .unwrap();

        let reopened = ReplicationPeerIdentityStore::open(&path).unwrap();
        assert_eq!(
            reopened.get(2).unwrap(),
            Some(ReplicationPeerIdentity::tcp(
                2,
                "127.0.0.1:17688",
                Some(2),
                "cluster-a",
                "default",
            ))
        );

        let _ = fs::remove_file(path);
    }

    #[test]
    fn replication_peer_identity_store_detects_indirect_cycles() {
        let store = ReplicationPeerIdentityStore::default();
        store
            .register(ReplicationPeerIdentity::tcp(
                2,
                "127.0.0.1:17688",
                Some(3),
                "cluster-a",
                "default",
            ))
            .unwrap();
        store
            .register(ReplicationPeerIdentity::tcp(
                3,
                "127.0.0.1:17689",
                Some(4),
                "cluster-a",
                "default",
            ))
            .unwrap();

        assert!(store.would_create_cycle(4, Some(2)).unwrap());
        assert!(!store.would_create_cycle(5, Some(2)).unwrap());
    }

    #[test]
    fn gossip_node_store_persists_and_rejects_stale_incarnation() {
        let path = temp_path("gossip-node");
        let store = GossipNodeStore::open(&path).unwrap();
        assert!(store
            .upsert(GossipNodeRecord {
                server_id: 2,
                query_address: "127.0.0.1:17688".to_string(),
                replication_address: "127.0.0.1:18688".to_string(),
                incarnation: 7,
                ttl_ms: 1000,
                seen_at_ms: 100,
            })
            .unwrap());
        assert!(!store
            .upsert(GossipNodeRecord {
                server_id: 2,
                query_address: "127.0.0.1:17699".to_string(),
                replication_address: "127.0.0.1:18699".to_string(),
                incarnation: 6,
                ttl_ms: 1000,
                seen_at_ms: 200,
            })
            .unwrap());

        let reopened = GossipNodeStore::open(&path).unwrap();
        assert_eq!(
            reopened.live_query_address(2, 500).unwrap().as_deref(),
            Some("127.0.0.1:17688")
        );
        assert_eq!(reopened.live_query_address(2, 1200).unwrap(), None);
        let _ = fs::remove_file(path);
    }
}
