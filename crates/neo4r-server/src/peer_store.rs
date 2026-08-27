use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

pub(super) const QUERY_PEERS_FILE: &str = "query-peers.txt";
pub(super) const REPLICATION_PEERS_FILE: &str = "replication-peers.txt";

const PEER_STORE_MAGIC: &str = "N4RPEERS1";

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
