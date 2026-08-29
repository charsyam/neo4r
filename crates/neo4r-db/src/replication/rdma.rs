#[cfg(feature = "rdma")]
pub use super::rdma_rsocket::{RdmaReplicationListener, RsocketStream};
use super::*;
#[cfg(feature = "rdma")]
use std::process::{Command, Stdio};
#[cfg(feature = "rdma")]
use std::sync::Arc;
#[cfg(feature = "rdma")]
use std::thread;
#[cfg(feature = "rdma")]
use std::time::{Duration, Instant};

const DEFAULT_RPING_PORT: u16 = 18515;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RdmaProbeReport {
    pub provider: String,
    pub target_addr: String,
    pub source_addr: Option<String>,
    pub port: u16,
    pub ping_count: u32,
    pub payload_size: usize,
    pub elapsed_millis: u128,
    pub stdout: String,
    pub stderr: String,
}

#[allow(dead_code)]
pub trait RdmaReplicationProvider: Send + Sync {
    fn provider_name(&self) -> &str;

    fn endpoint(&self, address: String) -> ReplicationEndpoint {
        ReplicationEndpoint {
            kind: ReplicationChannelKind::Rdma,
            address,
            capabilities: ReplicationChannelCapabilities::reliable_stream(),
        }
    }

    fn validate(&self) -> DatabaseResult<()>;

    fn probe(&self, endpoint: &ReplicationEndpoint) -> DatabaseResult<RdmaProbeReport>;

    #[cfg(feature = "rdma")]
    fn connect(
        &self,
        endpoint: &ReplicationEndpoint,
        connect_timeout: Duration,
    ) -> DatabaseResult<Box<dyn RdmaReplicationStream>>;
}

#[cfg(feature = "rdma")]
pub trait RdmaReplicationStream: Read + Write + Send {}

#[cfg(feature = "rdma")]
impl<T: Read + Write + Send> RdmaReplicationStream for T {}

#[derive(Clone, Debug, Eq, PartialEq)]
#[allow(dead_code)]
pub struct MockRdmaReplicationProvider {
    name: String,
    available: bool,
}

#[allow(dead_code)]
impl MockRdmaReplicationProvider {
    pub fn available(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            available: true,
        }
    }

    pub fn unavailable(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            available: false,
        }
    }
}

impl RdmaReplicationProvider for MockRdmaReplicationProvider {
    fn provider_name(&self) -> &str {
        &self.name
    }

    fn validate(&self) -> DatabaseResult<()> {
        if self.available {
            Ok(())
        } else {
            Err(DatabaseError::Replication(format!(
                "rdma provider {} is unavailable",
                self.name
            )))
        }
    }

    fn probe(&self, endpoint: &ReplicationEndpoint) -> DatabaseResult<RdmaProbeReport> {
        endpoint.ensure_kind(ReplicationChannelKind::Rdma)?;
        self.validate()?;
        let target = RdmaProbeTarget::parse(&endpoint.address, None)?;
        Ok(RdmaProbeReport {
            provider: self.name.clone(),
            target_addr: target.host,
            source_addr: None,
            port: target.port,
            ping_count: 0,
            payload_size: 0,
            elapsed_millis: 0,
            stdout: String::new(),
            stderr: String::new(),
        })
    }

    #[cfg(feature = "rdma")]
    fn connect(
        &self,
        endpoint: &ReplicationEndpoint,
        _connect_timeout: Duration,
    ) -> DatabaseResult<Box<dyn RdmaReplicationStream>> {
        endpoint.ensure_kind(ReplicationChannelKind::Rdma)?;
        Err(DatabaseError::Replication(format!(
            "mock rdma provider {} cannot open a system RDMA stream",
            self.name
        )))
    }
}

#[cfg(feature = "rdma")]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RdmaProbeOptions {
    pub source_addr: Option<String>,
    pub ping_count: u32,
    pub payload_size: usize,
    pub port: Option<u16>,
    pub timeout: Duration,
}

#[cfg(feature = "rdma")]
impl Default for RdmaProbeOptions {
    fn default() -> Self {
        Self {
            source_addr: None,
            ping_count: 3,
            payload_size: 64,
            port: None,
            timeout: Duration::from_secs(5),
        }
    }
}

#[cfg(feature = "rdma")]
#[derive(Clone, Debug)]
pub struct SystemRdmaReplicationProvider {
    name: String,
    options: RdmaProbeOptions,
}

#[cfg(feature = "rdma")]
impl Default for SystemRdmaReplicationProvider {
    fn default() -> Self {
        Self {
            name: "system-rdma-cm".to_string(),
            options: RdmaProbeOptions::default(),
        }
    }
}

#[cfg(feature = "rdma")]
#[allow(dead_code)]
impl SystemRdmaReplicationProvider {
    pub fn new(options: RdmaProbeOptions) -> Self {
        Self {
            name: "system-rdma-cm".to_string(),
            options,
        }
    }

    pub fn with_name(name: impl Into<String>, options: RdmaProbeOptions) -> Self {
        Self {
            name: name.into(),
            options,
        }
    }

    fn rdma_device_count(&self) -> DatabaseResult<usize> {
        let entries = std::fs::read_dir("/sys/class/infiniband").map_err(|err| {
            DatabaseError::Replication(format!("failed to inspect RDMA devices: {err}"))
        })?;
        Ok(entries.filter_map(Result::ok).count())
    }

    fn run_rping(&self, target: RdmaProbeTarget) -> DatabaseResult<RdmaProbeReport> {
        let started = Instant::now();
        let mut command = Command::new("rping");
        command
            .arg("-c")
            .arg("-a")
            .arg(&target.host)
            .arg("-p")
            .arg(target.port.to_string())
            .arg("-C")
            .arg(self.options.ping_count.to_string())
            .arg("-S")
            .arg(self.options.payload_size.to_string())
            .arg("-V")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        if let Some(source_addr) = &self.options.source_addr {
            command.arg("-I").arg(source_addr);
        }

        let output = run_with_timeout(command, self.options.timeout)?;
        let elapsed_millis = started.elapsed().as_millis();
        let stdout = String::from_utf8_lossy(&output.stdout).to_string();
        let stderr = String::from_utf8_lossy(&output.stderr).to_string();
        if !output.status.success() {
            return Err(DatabaseError::Replication(format!(
                "rdma provider {} failed rping to {}:{}: status {:?}; stderr: {}",
                self.name,
                target.host,
                target.port,
                output.status.code(),
                stderr.trim()
            )));
        }
        Ok(RdmaProbeReport {
            provider: self.name.clone(),
            target_addr: target.host,
            source_addr: self.options.source_addr.clone(),
            port: target.port,
            ping_count: self.options.ping_count,
            payload_size: self.options.payload_size,
            elapsed_millis,
            stdout,
            stderr,
        })
    }
}

#[cfg(feature = "rdma")]
impl RdmaReplicationProvider for SystemRdmaReplicationProvider {
    fn provider_name(&self) -> &str {
        &self.name
    }

    fn validate(&self) -> DatabaseResult<()> {
        let count = self.rdma_device_count()?;
        if count == 0 {
            return Err(DatabaseError::Replication(
                "no RDMA devices found under /sys/class/infiniband".to_string(),
            ));
        }
        command_exists("rping")?;
        Ok(())
    }

    fn probe(&self, endpoint: &ReplicationEndpoint) -> DatabaseResult<RdmaProbeReport> {
        endpoint.ensure_kind(ReplicationChannelKind::Rdma)?;
        self.validate()?;
        let target = RdmaProbeTarget::parse(&endpoint.address, self.options.port)?;
        self.run_rping(target)
    }

    fn connect(
        &self,
        endpoint: &ReplicationEndpoint,
        connect_timeout: Duration,
    ) -> DatabaseResult<Box<dyn RdmaReplicationStream>> {
        endpoint.ensure_kind(ReplicationChannelKind::Rdma)?;
        self.validate()?;
        let target = RdmaProbeTarget::parse(&endpoint.address, self.options.port)?;
        Ok(Box::new(RsocketStream::connect_target(
            &target,
            connect_timeout,
        )?))
    }
}

#[cfg(feature = "rdma")]
#[derive(Clone)]
pub struct RdmaReplicationChannel {
    provider: Arc<dyn RdmaReplicationProvider>,
}

#[cfg(feature = "rdma")]
impl Default for RdmaReplicationChannel {
    fn default() -> Self {
        Self::new(Arc::new(SystemRdmaReplicationProvider::default()))
    }
}

#[cfg(feature = "rdma")]
impl std::fmt::Debug for RdmaReplicationChannel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RdmaReplicationChannel")
            .field("provider", &self.provider.provider_name())
            .finish()
    }
}

#[cfg(feature = "rdma")]
impl RdmaReplicationChannel {
    pub fn new(provider: Arc<dyn RdmaReplicationProvider>) -> Self {
        Self { provider }
    }

    pub fn probe_endpoint(
        &self,
        endpoint: &ReplicationEndpoint,
    ) -> DatabaseResult<RdmaProbeReport> {
        self.provider.probe(endpoint)
    }

    fn connect(
        &self,
        endpoint: &ReplicationEndpoint,
        config: &ReplicationChannelConfig,
    ) -> DatabaseResult<Box<dyn RdmaReplicationStream>> {
        self.provider.connect(endpoint, config.connect_timeout)
    }
}

#[cfg(feature = "rdma")]
impl ReplicationChannel for RdmaReplicationChannel {
    fn kind(&self) -> ReplicationChannelKind {
        ReplicationChannelKind::Rdma
    }

    fn send_replication_batch(
        &self,
        endpoint: &ReplicationEndpoint,
        config: &ReplicationChannelConfig,
        entries: &[LogEntry],
    ) -> DatabaseResult<Vec<(ShardId, LogIndex)>> {
        endpoint.ensure_kind(ReplicationChannelKind::Rdma)?;
        retry_rdma(config, || {
            let mut stream = self.connect(endpoint, config)?;
            super::tcp_requests::write_tcp_replication_request(&mut stream, entries)?;
            stream.flush().map_err(|err| {
                DatabaseError::Replication(format!("flush RDMA replication request: {err}"))
            })?;
            super::tcp_responses::read_tcp_replication_response(&mut stream)
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
        endpoint.ensure_kind(ReplicationChannelKind::Rdma)?;
        let response = retry_rdma(config, || {
            send_rdma_raft_append_once(self, endpoint, config, shard_id, leader_commit, entries)
        })?;
        if response.append.success {
            Ok(response.ack_positions)
        } else {
            Err(DatabaseError::LogConflict {
                shard_id,
                index: response.append.match_index,
                message: "rdma raft append entries rejected by follower log".to_string(),
            })
        }
    }

    fn send_raft_append_batches_by_shard(
        &self,
        endpoint: &ReplicationEndpoint,
        config: &ReplicationChannelConfig,
        entries: &[LogEntry],
    ) -> DatabaseResult<Vec<(ShardId, LogIndex)>> {
        endpoint.ensure_kind(ReplicationChannelKind::Rdma)?;
        let mut entries_by_shard = BTreeMap::<ShardId, Vec<LogEntry>>::new();
        for entry in entries {
            entries_by_shard
                .entry(entry.shard_id)
                .or_default()
                .push(entry.clone());
        }
        let mut positions = Vec::new();
        for (shard_id, shard_entries) in entries_by_shard {
            let mut acked =
                self.send_raft_append_batch(endpoint, config, shard_id, 0, &shard_entries)?;
            positions.append(&mut acked);
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
        endpoint.ensure_kind(ReplicationChannelKind::Rdma)?;
        send_rdma_raft_append_once(self, endpoint, config, shard_id, leader_commit, entries)
    }

    fn request_vote(
        &self,
        endpoint: &ReplicationEndpoint,
        config: &ReplicationChannelConfig,
        shard_id: ShardId,
        request: RequestVoteRequest,
    ) -> DatabaseResult<RequestVoteResponse> {
        endpoint.ensure_kind(ReplicationChannelKind::Rdma)?;
        let mut stream = self.connect(endpoint, config)?;
        super::tcp_requests::write_tcp_raft_vote_request(&mut stream, shard_id, &request)?;
        stream.flush().map_err(|err| {
            DatabaseError::Replication(format!("flush RDMA raft vote request: {err}"))
        })?;
        super::tcp_responses::read_tcp_raft_vote_response(&mut stream)
    }

    fn pre_vote(
        &self,
        endpoint: &ReplicationEndpoint,
        config: &ReplicationChannelConfig,
        shard_id: ShardId,
        request: PreVoteRequest,
    ) -> DatabaseResult<PreVoteResponse> {
        endpoint.ensure_kind(ReplicationChannelKind::Rdma)?;
        let mut stream = self.connect(endpoint, config)?;
        super::tcp_requests::write_tcp_raft_pre_vote_request(&mut stream, shard_id, &request)?;
        stream.flush().map_err(|err| {
            DatabaseError::Replication(format!("flush RDMA raft pre-vote request: {err}"))
        })?;
        super::tcp_responses::read_tcp_raft_pre_vote_response(&mut stream)
    }

    fn install_snapshot(
        &self,
        endpoint: &ReplicationEndpoint,
        config: &ReplicationChannelConfig,
        request: InstallSnapshotRequest,
    ) -> DatabaseResult<InstallSnapshotResponse> {
        endpoint.ensure_kind(ReplicationChannelKind::Rdma)?;
        let mut stream = self.connect(endpoint, config)?;
        super::tcp_requests::write_tcp_install_snapshot_request(&mut stream, &request)?;
        stream.flush().map_err(|err| {
            DatabaseError::Replication(format!("flush RDMA install snapshot request: {err}"))
        })?;
        super::tcp_responses::read_tcp_install_snapshot_response(&mut stream)
    }

    fn catch_up(
        &self,
        endpoint: &ReplicationEndpoint,
        config: &ReplicationChannelConfig,
        shard_id: ShardId,
        start_index: LogIndex,
        max_entries: Option<usize>,
    ) -> DatabaseResult<Vec<LogEntry>> {
        endpoint.ensure_kind(ReplicationChannelKind::Rdma)?;
        let mut stream = self.connect(endpoint, config)?;
        super::tcp_responses::write_tcp_catch_up_request(
            &mut stream,
            shard_id,
            start_index,
            max_entries,
        )?;
        stream.flush().map_err(|err| {
            DatabaseError::Replication(format!("flush RDMA catch-up request: {err}"))
        })?;
        let entries = super::tcp_responses::read_tcp_catch_up_response(&mut stream)?;
        super::tcp_responses::validate_tcp_catch_up_entries(
            shard_id,
            start_index,
            max_entries,
            &entries,
        )?;
        Ok(entries)
    }
}

#[cfg(feature = "rdma")]
fn retry_rdma<T>(
    config: &ReplicationChannelConfig,
    mut operation: impl FnMut() -> DatabaseResult<T>,
) -> DatabaseResult<T> {
    let mut last_error = None;
    for attempt in 1..=config.max_attempts {
        match operation() {
            Ok(value) => return Ok(value),
            Err(err) => last_error = Some(err),
        }
        if attempt < config.max_attempts {
            thread::sleep(config.retry_backoff);
        }
    }
    Err(last_error.unwrap_or_else(|| {
        DatabaseError::Replication("RDMA replication failed without an error".to_string())
    }))
}

#[cfg(feature = "rdma")]
fn send_rdma_raft_append_once(
    channel: &RdmaReplicationChannel,
    endpoint: &ReplicationEndpoint,
    config: &ReplicationChannelConfig,
    shard_id: ShardId,
    leader_commit: LogIndex,
    entries: &[LogEntry],
) -> DatabaseResult<RaftAppendChannelResponse> {
    let mut stream = channel.connect(endpoint, config)?;
    super::tcp_requests::write_tcp_raft_append_request(
        &mut stream,
        shard_id,
        leader_commit,
        entries,
    )?;
    stream.flush().map_err(|err| {
        DatabaseError::Replication(format!("flush RDMA raft append request: {err}"))
    })?;
    super::tcp_responses::read_tcp_raft_append_response(&mut stream)
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct RdmaProbeTarget {
    host: String,
    port: u16,
}

impl RdmaProbeTarget {
    fn parse(address: &str, default_port: Option<u16>) -> DatabaseResult<Self> {
        let address = address
            .strip_prefix("rdma://")
            .or_else(|| address.strip_prefix("rdma+cm://"))
            .unwrap_or(address);
        let address = address.split('?').next().unwrap_or(address);
        let port = default_port.unwrap_or(DEFAULT_RPING_PORT);
        if address.starts_with('[') {
            let Some((host, rest)) = address.split_once(']') else {
                return Err(DatabaseError::Replication(format!(
                    "invalid RDMA endpoint address: {address}"
                )));
            };
            let host = host.trim_start_matches('[').to_string();
            let port = rest
                .strip_prefix(':')
                .map(parse_port)
                .transpose()?
                .unwrap_or(port);
            return Ok(Self { host, port });
        }
        let colon_count = address.chars().filter(|ch| *ch == ':').count();
        if colon_count == 1 {
            let (host, port_text) = address.rsplit_once(':').ok_or_else(|| {
                DatabaseError::Replication(format!("invalid RDMA endpoint address: {address}"))
            })?;
            if host.is_empty() {
                return Err(DatabaseError::Replication(format!(
                    "invalid RDMA endpoint address: {address}"
                )));
            }
            return Ok(Self {
                host: host.to_string(),
                port: parse_port(port_text)?,
            });
        }
        if address.is_empty() {
            return Err(DatabaseError::Replication(
                "RDMA endpoint address is empty".to_string(),
            ));
        }
        Ok(Self {
            host: address.to_string(),
            port,
        })
    }

    #[cfg(feature = "rdma")]
    pub(super) fn socket_address(&self) -> String {
        if self.host.contains(':') && !self.host.starts_with('[') {
            format!("[{}]:{}", self.host, self.port)
        } else {
            format!("{}:{}", self.host, self.port)
        }
    }
}

fn parse_port(port: &str) -> DatabaseResult<u16> {
    port.parse::<u16>().map_err(|err| {
        DatabaseError::Replication(format!("invalid RDMA endpoint port {port}: {err}"))
    })
}

#[cfg(feature = "rdma")]
fn command_exists(command: &str) -> DatabaseResult<()> {
    let output = Command::new(command)
        .arg("--help")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map_err(|err| DatabaseError::Replication(format!("failed to execute {command}: {err}")))?;
    if output.success() || output.code().is_some() {
        Ok(())
    } else {
        Err(DatabaseError::Replication(format!(
            "failed to execute {command}"
        )))
    }
}

#[cfg(feature = "rdma")]
fn run_with_timeout(
    mut command: Command,
    timeout: Duration,
) -> DatabaseResult<std::process::Output> {
    let mut child = command
        .spawn()
        .map_err(|err| DatabaseError::Replication(format!("failed to start rping: {err}")))?;
    let started = Instant::now();
    loop {
        if child
            .try_wait()
            .map_err(|err| DatabaseError::Replication(format!("failed to poll rping: {err}")))?
            .is_some()
        {
            return child
                .wait_with_output()
                .map_err(|err| DatabaseError::Replication(format!("failed to read rping: {err}")));
        }
        if started.elapsed() >= timeout {
            let _ = child.kill();
            let _ = child.wait();
            return Err(DatabaseError::Replication(format!(
                "rping timed out after {} ms",
                timeout.as_millis()
            )));
        }
        thread::sleep(Duration::from_millis(20));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rdma_probe_target_parses_plain_and_uri_addresses() {
        assert_eq!(
            RdmaProbeTarget::parse("10.0.0.1:18517", None).unwrap(),
            RdmaProbeTarget {
                host: "10.0.0.1".to_string(),
                port: 18517
            }
        );
        assert_eq!(
            RdmaProbeTarget::parse("rdma://10.0.0.1", Some(19999)).unwrap(),
            RdmaProbeTarget {
                host: "10.0.0.1".to_string(),
                port: 19999
            }
        );
        assert_eq!(
            RdmaProbeTarget::parse("rdma://[::1]:18518?device=rocep1s0f0", None).unwrap(),
            RdmaProbeTarget {
                host: "::1".to_string(),
                port: 18518
            }
        );
    }

    #[cfg(feature = "rdma")]
    #[test]
    fn rdma_probe_target_formats_socket_addresses() {
        assert_eq!(
            RdmaProbeTarget::parse("rdma://10.0.0.1:18517", None)
                .unwrap()
                .socket_address(),
            "10.0.0.1:18517"
        );
        assert_eq!(
            RdmaProbeTarget::parse("rdma://[::1]:18518", None)
                .unwrap()
                .socket_address(),
            "[::1]:18518"
        );
    }

    #[test]
    fn mock_provider_returns_probe_report_without_system_rdma() {
        let provider = MockRdmaReplicationProvider::available("mock-rdma");
        let endpoint = provider.endpoint("rdma://node-a:18517".to_string());
        let report = provider.probe(&endpoint).unwrap();

        assert_eq!(report.provider, "mock-rdma");
        assert_eq!(report.target_addr, "node-a");
        assert_eq!(report.port, 18517);
    }
}
