use super::*;
use neo4r_core::{LogEntry, LogIndex, ShardId};
use neo4r_db::{
    request_tcp_catch_up_on_stream, request_tcp_install_snapshot_on_stream,
    request_tcp_raft_pre_vote_on_stream, request_tcp_raft_vote_on_stream,
    request_tcp_replication_hello_on_stream, send_tcp_raft_append_batch_on_stream,
    send_tcp_replication_batch_on_stream, DatabaseError, DatabaseResult, InstallSnapshotRequest,
    InstallSnapshotResponse, PreVoteRequest, PreVoteResponse, RaftAppendChannelResponse,
    ReplicationChannel, ReplicationChannelConfig, ReplicationChannelKind, ReplicationEndpoint,
    ReplicationNodeIdentity, RequestVoteRequest, RequestVoteResponse,
};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use rustls::{ClientConfig as RustlsClientConfig, ClientConnection, RootCertStore, StreamOwned};
use std::fs::File;
use std::sync::Once;

static RUSTLS_PROVIDER_INIT: Once = Once::new();

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ReplicationTlsConfig {
    pub server_name: String,
    pub ca_cert_path: PathBuf,
    pub client_cert_path: Option<PathBuf>,
    pub client_key_path: Option<PathBuf>,
}

pub struct TlsReplicationChannel {
    config: ReplicationTlsConfig,
}

impl TlsReplicationChannel {
    pub fn new(config: ReplicationTlsConfig) -> Self {
        Self { config }
    }

    fn connect(
        &self,
        endpoint: &ReplicationEndpoint,
        config: &ReplicationChannelConfig,
    ) -> DatabaseResult<StreamOwned<ClientConnection, TcpStream>> {
        endpoint.ensure_kind(ReplicationChannelKind::Tcp)?;
        install_rustls_provider();
        let mut addrs = endpoint.address.to_socket_addrs().map_err(|err| {
            DatabaseError::Replication(format!("resolve {}: {err}", endpoint.address))
        })?;
        let addr = addrs.next().ok_or_else(|| {
            DatabaseError::Replication(format!("no socket address for {}", endpoint.address))
        })?;
        let tcp = TcpStream::connect_timeout(&addr, config.connect_timeout).map_err(|err| {
            DatabaseError::Replication(format!("connect {}: {err}", endpoint.address))
        })?;
        let tls_config = Arc::new(build_client_config(&self.config)?);
        let server_name = ServerName::try_from(self.config.server_name.clone())
            .map_err(|err| DatabaseError::Replication(format!("invalid TLS server name: {err}")))?;
        let connection = ClientConnection::new(tls_config, server_name).map_err(|err| {
            DatabaseError::Replication(format!("create TLS replication client: {err}"))
        })?;
        Ok(StreamOwned::new(connection, tcp))
    }
}

impl ReplicationChannel for TlsReplicationChannel {
    fn kind(&self) -> ReplicationChannelKind {
        ReplicationChannelKind::Tcp
    }

    fn send_replication_batch(
        &self,
        endpoint: &ReplicationEndpoint,
        config: &ReplicationChannelConfig,
        entries: &[LogEntry],
    ) -> DatabaseResult<Vec<(ShardId, LogIndex)>> {
        retry(config, || {
            let mut stream = self.connect(endpoint, config)?;
            send_tcp_replication_batch_on_stream(&mut stream, entries)
        })
    }

    fn send_raft_append_batch(
        &self,
        endpoint: &ReplicationEndpoint,
        config: &ReplicationChannelConfig,
        shard_id: ShardId,
        leader_commit: LogIndex,
        entries: &[LogEntry],
    ) -> DatabaseResult<Vec<(ShardId, LogIndex)>> {
        retry(config, || {
            let mut stream = self.connect(endpoint, config)?;
            let response = send_tcp_raft_append_batch_on_stream(
                &mut stream,
                shard_id,
                leader_commit,
                entries,
            )?;
            if response.append.success {
                Ok(response.ack_positions)
            } else {
                Err(DatabaseError::LogConflict {
                    shard_id,
                    index: response.append.match_index,
                    message: format!(
                        "raft append rejected term={} conflict_index={:?} conflict_term={:?}",
                        response.append.term,
                        response.append.conflict_index,
                        response.append.conflict_term
                    ),
                })
            }
        })
    }

    fn send_raft_append_batches_by_shard(
        &self,
        endpoint: &ReplicationEndpoint,
        config: &ReplicationChannelConfig,
        entries: &[LogEntry],
    ) -> DatabaseResult<Vec<(ShardId, LogIndex)>> {
        let mut entries_by_shard = BTreeMap::<ShardId, Vec<LogEntry>>::new();
        for entry in entries {
            entries_by_shard
                .entry(entry.shard_id)
                .or_default()
                .push(entry.clone());
        }
        let mut positions = Vec::new();
        for (shard_id, shard_entries) in entries_by_shard {
            positions.extend(self.send_raft_append_batch(
                endpoint,
                config,
                shard_id,
                0,
                &shard_entries,
            )?);
        }
        Ok(positions)
    }

    fn send_raft_append_batch_once(
        &self,
        endpoint: &ReplicationEndpoint,
        config: &ReplicationChannelConfig,
        shard_id: ShardId,
        leader_commit: LogIndex,
        entries: &[LogEntry],
    ) -> DatabaseResult<RaftAppendChannelResponse> {
        let mut stream = self.connect(endpoint, config)?;
        send_tcp_raft_append_batch_on_stream(&mut stream, shard_id, leader_commit, entries)
    }

    fn request_vote(
        &self,
        endpoint: &ReplicationEndpoint,
        config: &ReplicationChannelConfig,
        shard_id: ShardId,
        request: RequestVoteRequest,
    ) -> DatabaseResult<RequestVoteResponse> {
        let mut stream = self.connect(endpoint, config)?;
        request_tcp_raft_vote_on_stream(&mut stream, shard_id, request)
    }

    fn pre_vote(
        &self,
        endpoint: &ReplicationEndpoint,
        config: &ReplicationChannelConfig,
        shard_id: ShardId,
        request: PreVoteRequest,
    ) -> DatabaseResult<PreVoteResponse> {
        let mut stream = self.connect(endpoint, config)?;
        request_tcp_raft_pre_vote_on_stream(&mut stream, shard_id, request)
    }

    fn install_snapshot(
        &self,
        endpoint: &ReplicationEndpoint,
        config: &ReplicationChannelConfig,
        request: InstallSnapshotRequest,
    ) -> DatabaseResult<InstallSnapshotResponse> {
        let mut stream = self.connect(endpoint, config)?;
        request_tcp_install_snapshot_on_stream(&mut stream, request)
    }

    fn catch_up(
        &self,
        endpoint: &ReplicationEndpoint,
        config: &ReplicationChannelConfig,
        shard_id: ShardId,
        start_index: LogIndex,
        max_entries: Option<usize>,
    ) -> DatabaseResult<Vec<LogEntry>> {
        let mut stream = self.connect(endpoint, config)?;
        request_tcp_catch_up_on_stream(&mut stream, shard_id, start_index, max_entries)
    }
}

pub(crate) fn request_tls_replication_hello(
    address: &str,
    connect_timeout: Duration,
    config: &ReplicationTlsConfig,
) -> DatabaseResult<ReplicationNodeIdentity> {
    let endpoint = ReplicationEndpoint::tcp(address.to_string());
    let channel = TlsReplicationChannel::new(config.clone());
    let mut stream = channel.connect(
        &endpoint,
        &ReplicationChannelConfig {
            connect_timeout,
            ..ReplicationChannelConfig::default()
        },
    )?;
    request_tcp_replication_hello_on_stream(&mut stream)
}

fn retry<T>(
    config: &ReplicationChannelConfig,
    mut operation: impl FnMut() -> DatabaseResult<T>,
) -> DatabaseResult<T> {
    let mut last_error = None;
    for attempt in 1..=config.max_attempts.max(1) {
        match operation() {
            Ok(value) => return Ok(value),
            Err(err) => last_error = Some(err),
        }
        if attempt < config.max_attempts {
            thread::sleep(config.retry_backoff);
        }
    }
    Err(last_error.unwrap_or_else(|| {
        DatabaseError::Replication("TLS replication failed without an error".to_string())
    }))
}

fn build_client_config(config: &ReplicationTlsConfig) -> DatabaseResult<RustlsClientConfig> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let builder = RustlsClientConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13, &rustls::version::TLS12])
        .map_err(|err| DatabaseError::Replication(format!("invalid TLS versions: {err}")))?;
    let roots = load_roots(&config.ca_cert_path)?;
    match (&config.client_cert_path, &config.client_key_path) {
        (Some(cert_path), Some(key_path)) => {
            let certs = load_certs(cert_path)?;
            let key = load_private_key(key_path)?;
            builder
                .with_root_certificates(roots)
                .with_client_auth_cert(certs, key)
                .map_err(|err| {
                    DatabaseError::Replication(format!("invalid TLS client cert: {err}"))
                })
        }
        (None, None) => Ok(builder.with_root_certificates(roots).with_no_client_auth()),
        _ => Err(DatabaseError::Replication(
            "replication TLS client cert and key must be provided together".to_string(),
        )),
    }
}

fn install_rustls_provider() {
    RUSTLS_PROVIDER_INIT.call_once(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

fn load_certs(path: &Path) -> DatabaseResult<Vec<CertificateDer<'static>>> {
    let mut reader = BufReader::new(File::open(path).map_err(|err| {
        DatabaseError::Replication(format!("open TLS certificate {}: {err}", path.display()))
    })?);
    rustls_pemfile::certs(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| DatabaseError::Replication(format!("read TLS certificate: {err}")))
}

fn load_private_key(path: &Path) -> DatabaseResult<PrivateKeyDer<'static>> {
    let mut reader = BufReader::new(File::open(path).map_err(|err| {
        DatabaseError::Replication(format!("open TLS private key {}: {err}", path.display()))
    })?);
    rustls_pemfile::private_key(&mut reader)
        .map_err(|err| DatabaseError::Replication(format!("read TLS private key: {err}")))?
        .ok_or_else(|| {
            DatabaseError::Replication(format!("no private key found in {}", path.display()))
        })
}

fn load_roots(path: &Path) -> DatabaseResult<RootCertStore> {
    let mut roots = RootCertStore::empty();
    for cert in load_certs(path)? {
        roots
            .add(cert)
            .map_err(|err| DatabaseError::Replication(format!("invalid CA certificate: {err}")))?;
    }
    Ok(roots)
}
