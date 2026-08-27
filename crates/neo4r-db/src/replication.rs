use crate::{DatabaseError, DatabaseResult, Neo4rDatabaseHandle};
use neo4r_core::{LogEntry, LogIndex, ServerId, ShardId, ShardRole, ShardRoutingTable};
use neo4r_storage::{decode_log_entry, encode_log_entry};
use std::collections::{BTreeMap, BTreeSet};
use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::sync::{Mutex, RwLock};
use std::thread;
use std::time::Duration;

const TCP_REPLICATION_REQUEST_MAGIC: &[u8] = b"N4RRP1\n";
const TCP_REPLICATION_RESPONSE_MAGIC: &[u8] = b"N4RRA1\n";
const TCP_CATCH_UP_REQUEST_MAGIC: &[u8] = b"N4RCU1\n";
const TCP_CATCH_UP_REQUEST_MAGIC_V2: &[u8] = b"N4RCU3\n";
const TCP_CATCH_UP_RESPONSE_MAGIC: &[u8] = b"N4RCU2\n";
const TCP_REPLICATION_OK: u8 = 1;
const TCP_REPLICATION_ERR: u8 = 2;

pub trait ShardReplicator: Send + Sync {
    fn publish(&self, entry: &LogEntry) -> DatabaseResult<ReplicationOutcome>;

    fn install_routing_table(&self, _routing_table: ShardRoutingTable) -> DatabaseResult<()> {
        Ok(())
    }

    fn register_peer_address(&self, _server_id: ServerId, _address: String) -> DatabaseResult<()> {
        Ok(())
    }

    fn unregister_peer_address(&self, _server_id: ServerId) -> DatabaseResult<()> {
        Ok(())
    }

    fn publish_batch(&self, entries: &[LogEntry]) -> DatabaseResult<Vec<ReplicationOutcome>> {
        entries.iter().map(|entry| self.publish(entry)).collect()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicationOutcome {
    pub acked_server_ids: Vec<ServerId>,
    pub acked_match_indexes: Vec<(ServerId, ShardId, LogIndex)>,
}

impl ReplicationOutcome {
    pub fn local(origin_server_id: ServerId) -> Self {
        Self {
            acked_server_ids: vec![origin_server_id],
            acked_match_indexes: Vec::new(),
        }
    }

    fn ack(&mut self, server_id: ServerId, shard_id: ShardId, index: LogIndex) {
        if !self.acked_server_ids.contains(&server_id) {
            self.acked_server_ids.push(server_id);
        }
        self.acked_match_indexes.push((server_id, shard_id, index));
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReplicationAckPolicy {
    All,
    Quorum,
    Async,
}

impl ReplicationAckPolicy {
    fn required_acks(self, voter_count: usize) -> usize {
        match self {
            Self::All => voter_count,
            Self::Quorum => voter_count / 2 + 1,
            Self::Async => 1,
        }
    }
}

#[derive(Debug, Default)]
pub struct NoopShardReplicator;

impl ShardReplicator for NoopShardReplicator {
    fn publish(&self, entry: &LogEntry) -> DatabaseResult<ReplicationOutcome> {
        Ok(ReplicationOutcome::local(entry.origin_server_id))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TcpCatchUpResult {
    pub shard_id: ShardId,
    pub start_index: LogIndex,
    pub end_index: LogIndex,
    pub fetched_entries: usize,
    pub primary_server_id: ServerId,
}

pub struct InProcessShardReplicator {
    routing_table: RwLock<ShardRoutingTable>,
    ack_policy: ReplicationAckPolicy,
    peers: Mutex<BTreeMap<ServerId, Neo4rDatabaseHandle>>,
}

pub struct TcpShardReplicator {
    routing_table: RwLock<ShardRoutingTable>,
    ack_policy: ReplicationAckPolicy,
    connect_timeout: Duration,
    max_attempts: usize,
    retry_backoff: Duration,
    peers: Mutex<BTreeMap<ServerId, String>>,
}

impl TcpShardReplicator {
    pub fn new(routing_table: ShardRoutingTable) -> Self {
        Self {
            routing_table: RwLock::new(routing_table),
            ack_policy: ReplicationAckPolicy::All,
            connect_timeout: Duration::from_secs(1),
            max_attempts: 1,
            retry_backoff: Duration::from_millis(10),
            peers: Mutex::new(BTreeMap::new()),
        }
    }

    pub fn with_ack_policy(mut self, ack_policy: ReplicationAckPolicy) -> Self {
        self.ack_policy = ack_policy;
        self
    }

    pub fn with_connect_timeout(mut self, connect_timeout: Duration) -> Self {
        self.connect_timeout = connect_timeout;
        self
    }

    pub fn with_retry(mut self, max_attempts: usize, retry_backoff: Duration) -> Self {
        self.max_attempts = max_attempts.max(1);
        self.retry_backoff = retry_backoff;
        self
    }

    pub fn register_peer(
        &self,
        server_id: ServerId,
        address: impl Into<String>,
    ) -> DatabaseResult<()> {
        self.peers
            .lock()
            .map_err(|_| DatabaseError::LockPoisoned)?
            .insert(server_id, address.into());
        Ok(())
    }

    pub fn unregister_peer(&self, server_id: ServerId) -> DatabaseResult<()> {
        self.peers
            .lock()
            .map_err(|_| DatabaseError::LockPoisoned)?
            .remove(&server_id);
        Ok(())
    }

    fn replica_targets(&self, entry: &LogEntry) -> DatabaseResult<Vec<ServerId>> {
        let routing_table = self
            .routing_table
            .read()
            .map_err(|_| DatabaseError::LockPoisoned)?;
        replica_targets(&routing_table, entry)
    }

    fn voter_count(&self, entry: &LogEntry) -> DatabaseResult<usize> {
        let routing_table = self
            .routing_table
            .read()
            .map_err(|_| DatabaseError::LockPoisoned)?;
        voter_count(&routing_table, entry)
    }
}

impl InProcessShardReplicator {
    pub fn new(routing_table: ShardRoutingTable) -> Self {
        Self {
            routing_table: RwLock::new(routing_table),
            ack_policy: ReplicationAckPolicy::All,
            peers: Mutex::new(BTreeMap::new()),
        }
    }

    pub fn with_ack_policy(mut self, ack_policy: ReplicationAckPolicy) -> Self {
        self.ack_policy = ack_policy;
        self
    }

    pub fn register_peer(
        &self,
        server_id: ServerId,
        handle: Neo4rDatabaseHandle,
    ) -> DatabaseResult<()> {
        self.peers
            .lock()
            .map_err(|_| DatabaseError::LockPoisoned)?
            .insert(server_id, handle);
        Ok(())
    }

    pub fn unregister_peer(&self, server_id: ServerId) -> DatabaseResult<()> {
        self.peers
            .lock()
            .map_err(|_| DatabaseError::LockPoisoned)?
            .remove(&server_id);
        Ok(())
    }

    fn replica_targets(&self, entry: &LogEntry) -> DatabaseResult<Vec<ServerId>> {
        let routing_table = self
            .routing_table
            .read()
            .map_err(|_| DatabaseError::LockPoisoned)?;
        replica_targets(&routing_table, entry)
    }

    fn voter_count(&self, entry: &LogEntry) -> DatabaseResult<usize> {
        let routing_table = self
            .routing_table
            .read()
            .map_err(|_| DatabaseError::LockPoisoned)?;
        voter_count(&routing_table, entry)
    }
}

impl ShardReplicator for InProcessShardReplicator {
    fn install_routing_table(&self, routing_table: ShardRoutingTable) -> DatabaseResult<()> {
        *self
            .routing_table
            .write()
            .map_err(|_| DatabaseError::LockPoisoned)? = routing_table;
        Ok(())
    }

    fn publish(&self, entry: &LogEntry) -> DatabaseResult<ReplicationOutcome> {
        self.publish_batch(std::slice::from_ref(entry))
            .and_then(|mut outcomes| {
                outcomes.pop().ok_or_else(|| {
                    DatabaseError::Replication("empty replication batch".to_string())
                })
            })
    }

    fn publish_batch(&self, entries: &[LogEntry]) -> DatabaseResult<Vec<ReplicationOutcome>> {
        let mut outcomes = entries
            .iter()
            .map(|entry| ReplicationOutcome::local(entry.origin_server_id))
            .collect::<Vec<_>>();
        let mut batches = BTreeMap::<ServerId, Vec<(usize, LogEntry)>>::new();
        let mut errors_by_entry = vec![Vec::<String>::new(); entries.len()];
        let mut required_acks = Vec::with_capacity(entries.len());

        for (position, entry) in entries.iter().enumerate() {
            required_acks.push(self.ack_policy.required_acks(self.voter_count(entry)?));
            for target in self.replica_targets(entry)? {
                batches
                    .entry(target)
                    .or_default()
                    .push((position, entry.clone()));
            }
        }
        for (target, indexed_entries) in batches {
            let handle = self
                .peers
                .lock()
                .map_err(|_| DatabaseError::LockPoisoned)?
                .get(&target)
                .cloned();
            let Some(handle) = handle else {
                for (position, entry) in indexed_entries {
                    errors_by_entry[position].push(format!(
                        "missing in-process peer {target} for shard {}",
                        entry.shard_id
                    ));
                }
                continue;
            };

            let replicated_entries = indexed_entries
                .iter()
                .map(|(_, entry)| entry.clone())
                .collect::<Vec<_>>();
            match handle.apply_replicated_entries(replicated_entries) {
                Ok(()) => {
                    for (position, _) in indexed_entries {
                        let entry = &entries[position];
                        outcomes[position].ack(target, entry.shard_id, entry.index);
                    }
                }
                Err(err) => {
                    let message = err.to_string();
                    for (position, _) in indexed_entries {
                        errors_by_entry[position].push(message.clone());
                    }
                }
            }
        }

        for (position, entry) in entries.iter().enumerate() {
            let acked = outcomes[position].acked_server_ids.len();
            if acked < required_acks[position] {
                return Err(DatabaseError::Replication(format!(
                    "replication ack policy {:?} not satisfied for shard {}: got {acked}, required {}; errors: {}",
                    self.ack_policy,
                    entry.shard_id,
                    required_acks[position],
                    errors_by_entry[position].join("; ")
                )));
            }
        }

        Ok(outcomes)
    }
}

impl ShardReplicator for TcpShardReplicator {
    fn install_routing_table(&self, routing_table: ShardRoutingTable) -> DatabaseResult<()> {
        *self
            .routing_table
            .write()
            .map_err(|_| DatabaseError::LockPoisoned)? = routing_table;
        Ok(())
    }

    fn register_peer_address(&self, server_id: ServerId, address: String) -> DatabaseResult<()> {
        self.register_peer(server_id, address)
    }

    fn unregister_peer_address(&self, server_id: ServerId) -> DatabaseResult<()> {
        self.unregister_peer(server_id)
    }

    fn publish(&self, entry: &LogEntry) -> DatabaseResult<ReplicationOutcome> {
        self.publish_batch(std::slice::from_ref(entry))
            .and_then(|mut outcomes| {
                outcomes.pop().ok_or_else(|| {
                    DatabaseError::Replication("empty replication batch".to_string())
                })
            })
    }

    fn publish_batch(&self, entries: &[LogEntry]) -> DatabaseResult<Vec<ReplicationOutcome>> {
        let mut outcomes = entries
            .iter()
            .map(|entry| ReplicationOutcome::local(entry.origin_server_id))
            .collect::<Vec<_>>();
        let mut batches = BTreeMap::<ServerId, Vec<(usize, LogEntry)>>::new();
        let mut errors_by_entry = vec![Vec::<String>::new(); entries.len()];
        let mut required_acks = Vec::with_capacity(entries.len());

        for (position, entry) in entries.iter().enumerate() {
            required_acks.push(self.ack_policy.required_acks(self.voter_count(entry)?));
            for target in self.replica_targets(entry)? {
                batches
                    .entry(target)
                    .or_default()
                    .push((position, entry.clone()));
            }
        }
        let peers = self.peers.lock().map_err(|_| DatabaseError::LockPoisoned)?;
        preflight_tcp_ack_capacity(&peers, &batches, &required_acks)?;
        drop(peers);

        for (target, indexed_entries) in batches {
            let address = self
                .peers
                .lock()
                .map_err(|_| DatabaseError::LockPoisoned)?
                .get(&target)
                .cloned();
            let Some(address) = address else {
                for (position, entry) in indexed_entries {
                    errors_by_entry[position].push(format!(
                        "missing tcp peer {target} for shard {}",
                        entry.shard_id
                    ));
                }
                continue;
            };
            let replicated_entries = indexed_entries
                .iter()
                .map(|(_, entry)| entry.clone())
                .collect::<Vec<_>>();
            match send_tcp_replication_batch(
                &address,
                self.connect_timeout,
                self.max_attempts,
                self.retry_backoff,
                &replicated_entries,
            ) {
                Ok(ack_positions) => {
                    let acked_entries = ack_positions.into_iter().collect::<BTreeSet<_>>();
                    for (position, entry) in indexed_entries {
                        if acked_entries.contains(&(entry.shard_id, entry.index)) {
                            outcomes[position].ack(target, entry.shard_id, entry.index);
                        } else {
                            errors_by_entry[position].push(format!(
                                "tcp peer {target} ack did not include shard {} index {}",
                                entry.shard_id, entry.index
                            ));
                        }
                    }
                }
                Err(err) => {
                    let message = err.to_string();
                    for (position, _) in indexed_entries {
                        errors_by_entry[position].push(message.clone());
                    }
                }
            }
        }

        for (position, entry) in entries.iter().enumerate() {
            let acked = outcomes[position].acked_server_ids.len();
            if acked < required_acks[position] {
                return Err(DatabaseError::Replication(format!(
                    "replication ack policy {:?} not satisfied for shard {}: got {acked}, required {}; errors: {}",
                    self.ack_policy,
                    entry.shard_id,
                    required_acks[position],
                    errors_by_entry[position].join("; ")
                )));
            }
        }

        Ok(outcomes)
    }
}

fn preflight_tcp_ack_capacity(
    peers: &BTreeMap<ServerId, String>,
    batches: &BTreeMap<ServerId, Vec<(usize, LogEntry)>>,
    required_acks: &[usize],
) -> DatabaseResult<()> {
    let mut possible_acks = vec![1; required_acks.len()];
    let mut missing = Vec::new();
    for (target, indexed_entries) in batches {
        if peers.contains_key(target) {
            for (position, _) in indexed_entries {
                possible_acks[*position] += 1;
            }
        } else {
            missing.push(*target);
        }
    }
    for (position, required) in required_acks.iter().enumerate() {
        if possible_acks[position] < *required {
            return Err(DatabaseError::Replication(format!(
                "replication ack policy cannot be satisfied for entry {position}: possible {}, required {}; missing tcp peers: {:?}",
                possible_acks[position], required, missing
            )));
        }
    }
    Ok(())
}

pub fn handle_tcp_replication_stream(
    db: &Neo4rDatabaseHandle,
    stream: &mut TcpStream,
) -> DatabaseResult<()> {
    let magic = read_magic_bytes(stream)?;
    match magic.as_slice() {
        TCP_REPLICATION_REQUEST_MAGIC => {
            let entries = read_tcp_replication_request_after_magic(stream)?;
            let ack_positions = replication_ack_positions(&entries);
            let result = db.apply_replicated_entries(entries).map(|_| ack_positions);
            write_tcp_replication_response(stream, &result)?;
            result.map(|_| ())
        }
        TCP_CATCH_UP_REQUEST_MAGIC => {
            let request = read_tcp_catch_up_request_after_magic(stream, None)?;
            let result = read_catch_up_entries(db, &request);
            write_tcp_catch_up_response(stream, &result)?;
            result.map(|_| ())
        }
        TCP_CATCH_UP_REQUEST_MAGIC_V2 => {
            let request = read_tcp_catch_up_request_after_magic(stream, Some(()))?;
            let result = read_catch_up_entries(db, &request);
            write_tcp_catch_up_response(stream, &result)?;
            result.map(|_| ())
        }
        _ => Err(DatabaseError::Replication(
            "invalid replication stream magic".to_string(),
        )),
    }
}

pub fn catch_up_from_tcp_primary(
    db: &Neo4rDatabaseHandle,
    address: &str,
    connect_timeout: Duration,
    shard_id: ShardId,
    start_index: LogIndex,
) -> DatabaseResult<usize> {
    let entries = request_tcp_catch_up(address, connect_timeout, shard_id, start_index)?;
    let count = entries.len();
    db.apply_replicated_entries(entries)?;
    Ok(count)
}

pub fn catch_up_from_tcp_primary_batched(
    db: &Neo4rDatabaseHandle,
    address: &str,
    connect_timeout: Duration,
    shard_id: ShardId,
    start_index: LogIndex,
    max_entries_per_request: usize,
) -> DatabaseResult<usize> {
    if max_entries_per_request == 0 {
        return Err(DatabaseError::Replication(
            "catch-up max entries must be greater than zero".to_string(),
        ));
    }
    let mut next_index = start_index;
    let mut total = 0;
    loop {
        let entries = request_tcp_catch_up_limited(
            address,
            connect_timeout,
            shard_id,
            next_index,
            Some(max_entries_per_request),
        )?;
        let count = entries.len();
        if count == 0 {
            break;
        }
        db.apply_replicated_entries(entries)?;
        total += count;
        next_index += count as u64;
        if count < max_entries_per_request {
            break;
        }
    }
    Ok(total)
}

pub fn catch_up_from_tcp_primaries(
    db: &Neo4rDatabaseHandle,
    routing_table: &ShardRoutingTable,
    peer_addresses: &BTreeMap<ServerId, String>,
    local_server_id: ServerId,
    connect_timeout: Duration,
) -> DatabaseResult<Vec<TcpCatchUpResult>> {
    let committed_indexes = db.committed_indexes()?;
    let mut results = Vec::new();
    for placement in &routing_table.placements {
        if !placement.has_server(local_server_id) {
            continue;
        }
        let Some(primary_server_id) = placement.primary_server_id() else {
            return Err(DatabaseError::Replication(format!(
                "missing primary for shard {}",
                placement.shard_id
            )));
        };
        if primary_server_id == local_server_id {
            continue;
        }
        let address = peer_addresses.get(&primary_server_id).ok_or_else(|| {
            DatabaseError::Replication(format!(
                "missing peer address for primary server {primary_server_id} on shard {}",
                placement.shard_id
            ))
        })?;
        let start_index = committed_indexes
            .get(placement.shard_id as usize)
            .copied()
            .ok_or_else(|| {
                DatabaseError::Replication(format!(
                    "missing committed index for shard {}",
                    placement.shard_id
                ))
            })?
            + 1;
        let fetched_entries = catch_up_from_tcp_primary(
            db,
            address,
            connect_timeout,
            placement.shard_id,
            start_index,
        )?;
        results.push(TcpCatchUpResult {
            shard_id: placement.shard_id,
            start_index,
            end_index: catch_up_end_index(start_index, fetched_entries),
            fetched_entries,
            primary_server_id,
        });
    }
    Ok(results)
}

pub fn catch_up_from_tcp_primaries_batched(
    db: &Neo4rDatabaseHandle,
    routing_table: &ShardRoutingTable,
    peer_addresses: &BTreeMap<ServerId, String>,
    local_server_id: ServerId,
    connect_timeout: Duration,
    max_entries_per_request: usize,
) -> DatabaseResult<Vec<TcpCatchUpResult>> {
    if max_entries_per_request == 0 {
        return Err(DatabaseError::Replication(
            "catch-up max entries must be greater than zero".to_string(),
        ));
    }
    let committed_indexes = db.committed_indexes()?;
    let mut results = Vec::new();
    for placement in &routing_table.placements {
        if !placement.has_server(local_server_id) {
            continue;
        }
        let Some(primary_server_id) = placement.primary_server_id() else {
            return Err(DatabaseError::Replication(format!(
                "missing primary for shard {}",
                placement.shard_id
            )));
        };
        if primary_server_id == local_server_id {
            continue;
        }
        let address = peer_addresses.get(&primary_server_id).ok_or_else(|| {
            DatabaseError::Replication(format!(
                "missing peer address for primary server {primary_server_id} on shard {}",
                placement.shard_id
            ))
        })?;
        let start_index = committed_indexes
            .get(placement.shard_id as usize)
            .copied()
            .ok_or_else(|| {
                DatabaseError::Replication(format!(
                    "missing committed index for shard {}",
                    placement.shard_id
                ))
            })?
            + 1;
        let fetched_entries = catch_up_from_tcp_primary_batched(
            db,
            address,
            connect_timeout,
            placement.shard_id,
            start_index,
            max_entries_per_request,
        )?;
        results.push(TcpCatchUpResult {
            shard_id: placement.shard_id,
            start_index,
            end_index: catch_up_end_index(start_index, fetched_entries),
            fetched_entries,
            primary_server_id,
        });
    }
    Ok(results)
}

fn catch_up_end_index(start_index: LogIndex, fetched_entries: usize) -> LogIndex {
    start_index
        .saturating_add(fetched_entries as u64)
        .saturating_sub(1)
}

fn replica_targets(
    routing_table: &ShardRoutingTable,
    entry: &LogEntry,
) -> DatabaseResult<Vec<ServerId>> {
    let placement = routing_table.placement(entry.shard_id).ok_or_else(|| {
        DatabaseError::Replication(format!(
            "missing routing placement for shard {}",
            entry.shard_id
        ))
    })?;
    Ok(placement
        .replicas
        .iter()
        .filter(|replica| {
            replica.role == ShardRole::Replica && replica.server_id != entry.origin_server_id
        })
        .map(|replica| replica.server_id)
        .collect())
}

fn voter_count(routing_table: &ShardRoutingTable, entry: &LogEntry) -> DatabaseResult<usize> {
    let placement = routing_table.placement(entry.shard_id).ok_or_else(|| {
        DatabaseError::Replication(format!(
            "missing routing placement for shard {}",
            entry.shard_id
        ))
    })?;
    Ok(placement.replicas.len())
}

fn send_tcp_replication_batch(
    address: &str,
    connect_timeout: Duration,
    max_attempts: usize,
    retry_backoff: Duration,
    entries: &[LogEntry],
) -> DatabaseResult<Vec<(ShardId, LogIndex)>> {
    let mut last_error = None;
    for attempt in 1..=max_attempts.max(1) {
        match send_tcp_replication_batch_once(address, connect_timeout, entries) {
            Ok(positions) => return Ok(positions),
            Err(err) => last_error = Some(err),
        }
        if attempt < max_attempts {
            thread::sleep(retry_backoff);
        }
    }
    Err(last_error.unwrap_or_else(|| {
        DatabaseError::Replication(format!("replication to {address} failed without an error"))
    }))
}

fn send_tcp_replication_batch_once(
    address: &str,
    connect_timeout: Duration,
    entries: &[LogEntry],
) -> DatabaseResult<Vec<(ShardId, LogIndex)>> {
    let mut addrs = address
        .to_socket_addrs()
        .map_err(|err| DatabaseError::Replication(format!("resolve {address}: {err}")))?;
    let addr = addrs
        .next()
        .ok_or_else(|| DatabaseError::Replication(format!("no socket address for {address}")))?;
    let mut stream = TcpStream::connect_timeout(&addr, connect_timeout)
        .map_err(|err| DatabaseError::Replication(format!("connect {address}: {err}")))?;
    write_tcp_replication_request(&mut stream, entries)?;
    stream
        .flush()
        .map_err(|err| DatabaseError::Replication(format!("flush replication request: {err}")))?;
    read_tcp_replication_response(&mut stream)
}

fn write_tcp_replication_request(
    writer: &mut impl Write,
    entries: &[LogEntry],
) -> DatabaseResult<()> {
    writer
        .write_all(TCP_REPLICATION_REQUEST_MAGIC)
        .map_err(|err| DatabaseError::Replication(format!("write replication magic: {err}")))?;
    write_u32(writer, entries.len() as u32)?;
    for entry in entries {
        let payload = encode_log_entry(entry);
        write_u32(writer, payload.len() as u32)?;
        writer.write_all(&payload).map_err(|err| {
            DatabaseError::Replication(format!("write replication entry payload: {err}"))
        })?;
    }
    Ok(())
}

fn read_tcp_replication_request_after_magic(
    reader: &mut impl Read,
) -> DatabaseResult<Vec<LogEntry>> {
    let count = read_u32(reader)? as usize;
    let mut entries = Vec::with_capacity(count);
    for _ in 0..count {
        let len = read_u32(reader)? as usize;
        let mut payload = vec![0; len];
        reader.read_exact(&mut payload).map_err(|err| {
            DatabaseError::Replication(format!("read replication entry payload: {err}"))
        })?;
        entries.push(decode_log_entry(&payload)?);
    }
    Ok(entries)
}

struct TcpCatchUpRequest {
    shard_id: ShardId,
    start_index: LogIndex,
    max_entries: Option<usize>,
}

fn request_tcp_catch_up(
    address: &str,
    connect_timeout: Duration,
    shard_id: ShardId,
    start_index: LogIndex,
) -> DatabaseResult<Vec<LogEntry>> {
    request_tcp_catch_up_limited(address, connect_timeout, shard_id, start_index, None)
}

fn request_tcp_catch_up_limited(
    address: &str,
    connect_timeout: Duration,
    shard_id: ShardId,
    start_index: LogIndex,
    max_entries: Option<usize>,
) -> DatabaseResult<Vec<LogEntry>> {
    let mut addrs = address
        .to_socket_addrs()
        .map_err(|err| DatabaseError::Replication(format!("resolve {address}: {err}")))?;
    let addr = addrs
        .next()
        .ok_or_else(|| DatabaseError::Replication(format!("no socket address for {address}")))?;
    let mut stream = TcpStream::connect_timeout(&addr, connect_timeout)
        .map_err(|err| DatabaseError::Replication(format!("connect {address}: {err}")))?;
    write_tcp_catch_up_request(&mut stream, shard_id, start_index, max_entries)?;
    stream
        .flush()
        .map_err(|err| DatabaseError::Replication(format!("flush catch-up request: {err}")))?;
    let entries = read_tcp_catch_up_response(&mut stream)?;
    validate_tcp_catch_up_entries(shard_id, start_index, max_entries, &entries)?;
    Ok(entries)
}

fn write_tcp_catch_up_request(
    writer: &mut impl Write,
    shard_id: ShardId,
    start_index: LogIndex,
    max_entries: Option<usize>,
) -> DatabaseResult<()> {
    writer
        .write_all(if max_entries.is_some() {
            TCP_CATCH_UP_REQUEST_MAGIC_V2
        } else {
            TCP_CATCH_UP_REQUEST_MAGIC
        })
        .map_err(|err| DatabaseError::Replication(format!("write catch-up magic: {err}")))?;
    write_u64(writer, shard_id)?;
    write_u64(writer, start_index)?;
    if let Some(max_entries) = max_entries {
        write_u64(writer, max_entries as u64)?;
    }
    Ok(())
}

fn read_tcp_catch_up_request_after_magic(
    reader: &mut impl Read,
    has_limit: Option<()>,
) -> DatabaseResult<TcpCatchUpRequest> {
    Ok(TcpCatchUpRequest {
        shard_id: read_u64(reader)?,
        start_index: read_u64(reader)?,
        max_entries: if has_limit.is_some() {
            let max_entries = read_u64(reader)?;
            if max_entries == 0 {
                return Err(DatabaseError::Replication(
                    "catch-up max entries must be greater than zero".to_string(),
                ));
            }
            Some(max_entries as usize)
        } else {
            None
        },
    })
}

fn read_catch_up_entries(
    db: &Neo4rDatabaseHandle,
    request: &TcpCatchUpRequest,
) -> DatabaseResult<Vec<LogEntry>> {
    let mut entries = db.log_entries_from(request.shard_id, request.start_index)?;
    if let Some(max_entries) = request.max_entries {
        entries.truncate(max_entries);
    }
    Ok(entries)
}

fn validate_tcp_catch_up_entries(
    shard_id: ShardId,
    start_index: LogIndex,
    max_entries: Option<usize>,
    entries: &[LogEntry],
) -> DatabaseResult<()> {
    if let Some(max_entries) = max_entries {
        if entries.len() > max_entries {
            return Err(DatabaseError::Replication(format!(
                "catch-up response returned {} entries, exceeding requested limit {max_entries}",
                entries.len()
            )));
        }
    }
    for (offset, entry) in entries.iter().enumerate() {
        if entry.shard_id != shard_id {
            return Err(DatabaseError::Replication(format!(
                "catch-up response returned shard {} for requested shard {shard_id}",
                entry.shard_id
            )));
        }
        let expected = start_index + offset as u64;
        if entry.index != expected {
            return Err(DatabaseError::Replication(format!(
                "catch-up response returned shard {shard_id} index {}, expected {expected}",
                entry.index
            )));
        }
    }
    Ok(())
}

fn write_tcp_catch_up_response(
    writer: &mut impl Write,
    result: &DatabaseResult<Vec<LogEntry>>,
) -> DatabaseResult<()> {
    writer
        .write_all(TCP_CATCH_UP_RESPONSE_MAGIC)
        .map_err(|err| DatabaseError::Replication(format!("write catch-up response: {err}")))?;
    match result {
        Ok(entries) => {
            writer
                .write_all(&[TCP_REPLICATION_OK])
                .map_err(|err| DatabaseError::Replication(format!("write catch-up ok: {err}")))?;
            write_u32(writer, 0)?;
            write_u32(writer, entries.len() as u32)?;
            for entry in entries {
                let payload = encode_log_entry(entry);
                write_u32(writer, payload.len() as u32)?;
                writer.write_all(&payload).map_err(|err| {
                    DatabaseError::Replication(format!("write catch-up entry payload: {err}"))
                })?;
            }
            Ok(())
        }
        Err(err) => {
            writer
                .write_all(&[TCP_REPLICATION_ERR])
                .map_err(|err| DatabaseError::Replication(format!("write catch-up err: {err}")))?;
            let message = err.to_string();
            write_u32(writer, message.len() as u32)?;
            writer
                .write_all(message.as_bytes())
                .map_err(|err| DatabaseError::Replication(format!("write catch-up err: {err}")))
        }
    }
}

fn read_tcp_catch_up_response(reader: &mut impl Read) -> DatabaseResult<Vec<LogEntry>> {
    read_magic(reader, TCP_CATCH_UP_RESPONSE_MAGIC, "catch-up response")?;
    let mut status = [0; 1];
    reader
        .read_exact(&mut status)
        .map_err(|err| DatabaseError::Replication(format!("read catch-up status: {err}")))?;
    let message_len = read_u32(reader)? as usize;
    let mut message = vec![0; message_len];
    reader
        .read_exact(&mut message)
        .map_err(|err| DatabaseError::Replication(format!("read catch-up message: {err}")))?;
    match status[0] {
        TCP_REPLICATION_OK => {
            let count = read_u32(reader)? as usize;
            let mut entries = Vec::with_capacity(count);
            for _ in 0..count {
                let len = read_u32(reader)? as usize;
                let mut payload = vec![0; len];
                reader.read_exact(&mut payload).map_err(|err| {
                    DatabaseError::Replication(format!("read catch-up entry payload: {err}"))
                })?;
                entries.push(decode_log_entry(&payload)?);
            }
            Ok(entries)
        }
        TCP_REPLICATION_ERR => Err(DatabaseError::Replication(
            String::from_utf8_lossy(&message).into_owned(),
        )),
        value => Err(DatabaseError::Replication(format!(
            "unknown catch-up response status {value}"
        ))),
    }
}

fn write_tcp_replication_response(
    writer: &mut impl Write,
    result: &DatabaseResult<Vec<(ShardId, LogIndex)>>,
) -> DatabaseResult<()> {
    writer
        .write_all(TCP_REPLICATION_RESPONSE_MAGIC)
        .map_err(|err| DatabaseError::Replication(format!("write replication response: {err}")))?;
    match result {
        Ok(positions) => {
            writer.write_all(&[TCP_REPLICATION_OK]).map_err(|err| {
                DatabaseError::Replication(format!("write replication ok: {err}"))
            })?;
            let payload = encode_replication_ack_positions(positions);
            write_u32(writer, payload.len() as u32)?;
            writer.write_all(&payload).map_err(|err| {
                DatabaseError::Replication(format!("write replication ok payload: {err}"))
            })
        }
        Err(err) => {
            writer.write_all(&[TCP_REPLICATION_ERR]).map_err(|err| {
                DatabaseError::Replication(format!("write replication err: {err}"))
            })?;
            let message = err.to_string();
            write_u32(writer, message.len() as u32)?;
            writer
                .write_all(message.as_bytes())
                .map_err(|err| DatabaseError::Replication(format!("write replication err: {err}")))
        }
    }
}

fn read_tcp_replication_response(
    reader: &mut impl Read,
) -> DatabaseResult<Vec<(ShardId, LogIndex)>> {
    read_magic(
        reader,
        TCP_REPLICATION_RESPONSE_MAGIC,
        "replication response",
    )?;
    let mut status = [0; 1];
    reader
        .read_exact(&mut status)
        .map_err(|err| DatabaseError::Replication(format!("read replication status: {err}")))?;
    let message_len = read_u32(reader)? as usize;
    let mut message = vec![0; message_len];
    reader
        .read_exact(&mut message)
        .map_err(|err| DatabaseError::Replication(format!("read replication response: {err}")))?;
    match status[0] {
        TCP_REPLICATION_OK => decode_replication_ack_positions(&message),
        TCP_REPLICATION_ERR => Err(DatabaseError::Replication(
            String::from_utf8_lossy(&message).into_owned(),
        )),
        value => Err(DatabaseError::Replication(format!(
            "unknown replication response status {value}"
        ))),
    }
}

fn replication_ack_positions(entries: &[LogEntry]) -> Vec<(ShardId, LogIndex)> {
    entries
        .iter()
        .map(|entry| (entry.shard_id, entry.index))
        .collect()
}

fn encode_replication_ack_positions(positions: &[(ShardId, LogIndex)]) -> Vec<u8> {
    let mut payload = Vec::with_capacity(4 + positions.len() * 16);
    payload.extend_from_slice(&(positions.len() as u32).to_be_bytes());
    for (shard_id, index) in positions {
        payload.extend_from_slice(&shard_id.to_be_bytes());
        payload.extend_from_slice(&index.to_be_bytes());
    }
    payload
}

fn decode_replication_ack_positions(payload: &[u8]) -> DatabaseResult<Vec<(ShardId, LogIndex)>> {
    if payload.is_empty() {
        return Ok(Vec::new());
    }
    if payload.len() < 4 {
        return Err(DatabaseError::Replication(
            "truncated replication ack payload".to_string(),
        ));
    }
    let count = u32::from_be_bytes(payload[0..4].try_into().unwrap()) as usize;
    let expected_len = 4 + count * 16;
    if payload.len() != expected_len {
        return Err(DatabaseError::Replication(format!(
            "invalid replication ack payload length {}, expected {expected_len}",
            payload.len()
        )));
    }
    let mut positions = Vec::with_capacity(count);
    for offset in (4..payload.len()).step_by(16) {
        let shard_id = u64::from_be_bytes(payload[offset..offset + 8].try_into().unwrap());
        let index = u64::from_be_bytes(payload[offset + 8..offset + 16].try_into().unwrap());
        positions.push((shard_id, index));
    }
    Ok(positions)
}

fn read_magic(reader: &mut impl Read, expected: &[u8], context: &str) -> DatabaseResult<()> {
    let magic = read_magic_bytes(reader)?;
    if magic == expected {
        Ok(())
    } else {
        Err(DatabaseError::Replication(format!(
            "invalid {context} magic"
        )))
    }
}

fn read_magic_bytes(reader: &mut impl Read) -> DatabaseResult<Vec<u8>> {
    let mut magic = vec![0; TCP_REPLICATION_REQUEST_MAGIC.len()];
    reader
        .read_exact(&mut magic)
        .map_err(|err| DatabaseError::Replication(format!("read replication magic: {err}")))?;
    Ok(magic)
}

fn write_u32(writer: &mut impl Write, value: u32) -> DatabaseResult<()> {
    writer
        .write_all(&value.to_be_bytes())
        .map_err(|err| DatabaseError::Replication(format!("write u32: {err}")))
}

fn write_u64(writer: &mut impl Write, value: u64) -> DatabaseResult<()> {
    writer
        .write_all(&value.to_be_bytes())
        .map_err(|err| DatabaseError::Replication(format!("write u64: {err}")))
}

fn read_u64(reader: &mut impl Read) -> DatabaseResult<u64> {
    let mut bytes = [0; 8];
    reader
        .read_exact(&mut bytes)
        .map_err(|err| DatabaseError::Replication(format!("read u64: {err}")))?;
    Ok(u64::from_be_bytes(bytes))
}

fn read_u32(reader: &mut impl Read) -> DatabaseResult<u32> {
    let mut bytes = [0; 4];
    reader
        .read_exact(&mut bytes)
        .map_err(|err| DatabaseError::Replication(format!("read u32: {err}")))?;
    Ok(u32::from_be_bytes(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;
    use neo4r_core::{Command, ShardPlacement, ShardReplica};
    use std::fs;
    use std::net::TcpListener;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_dir(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("neo4r-replication-{name}-{nanos}"))
    }

    #[test]
    fn replication_ack_positions_codec_round_trips() {
        let positions = vec![(0, 3), (2, 9)];

        let encoded = encode_replication_ack_positions(&positions);
        let decoded = decode_replication_ack_positions(&encoded).unwrap();

        assert_eq!(decoded, positions);
    }

    #[test]
    fn replication_ack_positions_preserve_each_entry_index() {
        let entries = vec![
            LogEntry::new(0, 1, 7, neo4r_core::Command::DeleteNode { id: 7 }),
            LogEntry::new(0, 1, 8, neo4r_core::Command::DeleteNode { id: 8 }),
            LogEntry::new(1, 1, 2, neo4r_core::Command::DeleteNode { id: 2 }),
        ];

        assert_eq!(
            replication_ack_positions(&entries),
            vec![(0, 7), (0, 8), (1, 2)]
        );
    }

    #[test]
    fn replication_ack_positions_accepts_legacy_empty_payload() {
        assert_eq!(decode_replication_ack_positions(&[]).unwrap(), Vec::new());
    }

    #[test]
    fn replication_ack_positions_rejects_truncated_payloads() {
        let err = decode_replication_ack_positions(&[0, 0, 0]).unwrap_err();

        assert!(err
            .to_string()
            .contains("truncated replication ack payload"));
    }

    #[test]
    fn tcp_catch_up_validation_rejects_wrong_shard_response() {
        let entries = vec![LogEntry::new(
            1,
            1,
            7,
            neo4r_core::Command::DeleteNode { id: 7 },
        )];

        let err = validate_tcp_catch_up_entries(0, 7, None, &entries).unwrap_err();

        assert!(err
            .to_string()
            .contains("catch-up response returned shard 1 for requested shard 0"));
    }

    #[test]
    fn tcp_catch_up_validation_rejects_non_contiguous_indexes() {
        let entries = vec![
            LogEntry::new(0, 1, 7, neo4r_core::Command::DeleteNode { id: 7 }),
            LogEntry::new(0, 1, 9, neo4r_core::Command::DeleteNode { id: 9 }),
        ];

        let err = validate_tcp_catch_up_entries(0, 7, None, &entries).unwrap_err();

        assert!(err
            .to_string()
            .contains("catch-up response returned shard 0 index 9, expected 8"));
    }

    #[test]
    fn tcp_catch_up_validation_rejects_responses_over_requested_limit() {
        let entries = vec![
            LogEntry::new(0, 1, 7, neo4r_core::Command::DeleteNode { id: 7 }),
            LogEntry::new(0, 1, 8, neo4r_core::Command::DeleteNode { id: 8 }),
        ];

        let err = validate_tcp_catch_up_entries(0, 7, Some(1), &entries).unwrap_err();

        assert!(err
            .to_string()
            .contains("catch-up response returned 2 entries, exceeding requested limit 1"));
    }

    #[test]
    fn tcp_catch_up_rejects_malformed_response_before_applying_entries() {
        let dir = temp_dir("malformed-response");
        let routing_table = ShardRoutingTable {
            version: 3,
            placements: vec![ShardPlacement::new(
                0,
                vec![ShardReplica::primary(1), ShardReplica::replica(2)],
            )],
        };
        let replica = Neo4rDatabaseHandle::open(
            crate::DatabaseConfig::new(&dir, 1, 1)
                .with_server_id(2)
                .with_routing_table(routing_table),
        )
        .unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let magic = read_magic_bytes(&mut stream).unwrap();
            assert_eq!(magic, TCP_CATCH_UP_REQUEST_MAGIC);
            let request = read_tcp_catch_up_request_after_magic(&mut stream, None).unwrap();
            assert_eq!(request.shard_id, 0);
            assert_eq!(request.start_index, 1);
            write_tcp_catch_up_response(
                &mut stream,
                &Ok(vec![LogEntry::new(
                    0,
                    1,
                    2,
                    Command::CreateNode {
                        id: 42,
                        labels: vec!["Person".to_string()],
                        properties: Default::default(),
                    },
                )]),
            )
            .unwrap();
        });

        let err = catch_up_from_tcp_primary(&replica, &address, Duration::from_secs(1), 0, 1)
            .unwrap_err();

        assert!(err
            .to_string()
            .contains("catch-up response returned shard 0 index 2, expected 1"));
        assert!(replica.log_entries_from(0, 1).unwrap().is_empty());
        assert!(replica
            .query("MATCH (n:Person) RETURN n")
            .unwrap()
            .is_empty());
        assert_eq!(replica.committed_indexes().unwrap(), vec![0]);
        server.join().unwrap();

        drop(replica);
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn tcp_replicator_rejects_peer_response_without_exact_entry_ack() {
        let routing_table = ShardRoutingTable {
            version: 1,
            placements: vec![ShardPlacement::new(
                0,
                vec![ShardReplica::primary(1), ShardReplica::replica(2)],
            )],
        };
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let magic = read_magic_bytes(&mut stream).unwrap();
            assert_eq!(magic, TCP_REPLICATION_REQUEST_MAGIC);
            let entries = read_tcp_replication_request_after_magic(&mut stream).unwrap();
            assert_eq!(entries.len(), 1);
            write_tcp_replication_response(&mut stream, &Ok(Vec::new())).unwrap();
        });

        let replicator = TcpShardReplicator::new(routing_table);
        replicator.register_peer(2, address).unwrap();
        let entry = LogEntry::new_with_metadata(
            0,
            1,
            7,
            1,
            1,
            neo4r_core::HybridTimestamp::zero(),
            neo4r_core::Command::DeleteNode { id: 7 },
        );

        let err = replicator.publish(&entry).unwrap_err();
        assert!(err
            .to_string()
            .contains("tcp peer 2 ack did not include shard 0 index 7"));
        server.join().unwrap();
    }
}
