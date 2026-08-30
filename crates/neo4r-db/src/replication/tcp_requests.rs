use super::tcp_responses::*;
use super::tcp_snapshot_fetch::*;
use super::*;

pub(super) fn preflight_tcp_ack_capacity(
    peers: &BTreeMap<ServerId, ReplicationEndpoint>,
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
                "replication ack policy cannot be satisfied for entry {position}: possible {}, required {}; missing replication peers: {:?}",
                possible_acks[position], required, missing
            )));
        }
    }
    Ok(())
}

pub fn handle_tcp_replication_stream(
    db: &Neo4rDatabaseHandle,
    stream: &mut (impl Read + Write),
) -> DatabaseResult<()> {
    let magic = read_magic_bytes(stream)?;
    match magic.as_slice() {
        TCP_REPLICATION_HELLO_MAGIC => {
            write_tcp_replication_hello_response(stream, &db.replication_node_identity())?;
            Ok(())
        }
        TCP_REPLICATION_REQUEST_MAGIC => {
            let entries = read_tcp_replication_request_after_magic(stream)?;
            let ack_positions = replication_ack_positions(&entries);
            let result = db.apply_replicated_entries(entries).map(|_| ack_positions);
            write_tcp_replication_response(stream, &result)?;
            result.map(|_| ())
        }
        TCP_RAFT_APPEND_REQUEST_MAGIC => {
            let shard_id = read_u64(stream)?;
            let leader_commit = read_u64(stream)?;
            let entries = read_tcp_replication_request_after_magic(stream)?;
            let ack_positions = replication_ack_positions(&entries);
            let result = db
                .apply_raft_append_entries_with_response(shard_id, entries, leader_commit)
                .map(|append| TcpRaftAppendResponse {
                    ack_positions: if append.success {
                        ack_positions
                    } else {
                        Vec::new()
                    },
                    append,
                });
            write_tcp_raft_append_response(stream, &result)?;
            result.and_then(|response| {
                if response.append.success {
                    Ok(())
                } else {
                    Err(DatabaseError::LogConflict {
                        shard_id,
                        index: response.append.match_index,
                        message: "raft append entries rejected by follower log".to_string(),
                    })
                }
            })
        }
        TCP_RAFT_VOTE_REQUEST_MAGIC => {
            let shard_id = read_u64(stream)?;
            let request = read_tcp_raft_vote_request_after_magic(stream)?;
            let result = db.request_raft_vote(shard_id, request);
            write_tcp_raft_vote_response(stream, &result)?;
            result.map(|_| ())
        }
        TCP_RAFT_PRE_VOTE_REQUEST_MAGIC => {
            let shard_id = read_u64(stream)?;
            let request = read_tcp_raft_pre_vote_request_after_magic(stream)?;
            let result = db.request_raft_pre_vote(shard_id, request);
            write_tcp_raft_pre_vote_response(stream, &result)?;
            result.map(|_| ())
        }
        TCP_RAFT_LEADER_TRANSFER_REQUEST_MAGIC => {
            let shard_id = read_u64(stream)?;
            let transferee_id = read_u64(stream)?;
            let result = db.request_raft_leader_transfer(shard_id, transferee_id);
            write_tcp_raft_leader_transfer_response(stream, &result)?;
            result.map(|_| ())
        }
        TCP_RAFT_SNAPSHOT_REQUEST_MAGIC => {
            let request = read_tcp_install_snapshot_request_after_magic(stream)?;
            let result = db.install_raft_snapshot(request);
            write_tcp_install_snapshot_response(stream, &result)?;
            result.map(|_| ())
        }
        TCP_RAFT_SNAPSHOT_FETCH_REQUEST_MAGIC => {
            let shard_id = read_u64(stream)?;
            let result = db.install_snapshot_request_for_shard(shard_id);
            write_tcp_snapshot_fetch_response(stream, &result)?;
            result.map(|_| ())
        }
        TCP_RAFT_SNAPSHOT_FETCH_REQUEST_MAGIC_V2 => {
            let shard_id = read_u64(stream)?;
            let offset = read_u64(stream)?;
            let max_bytes = read_u32(stream)? as usize;
            let result = db
                .install_snapshot_request_for_shard(shard_id)
                .and_then(|snapshot| slice_snapshot_fetch_chunk(snapshot, offset, max_bytes));
            write_tcp_snapshot_fetch_chunk_response(stream, &result)?;
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

pub struct TcpNodeCatchUpDataSource {
    connect_timeout: Duration,
}

impl TcpNodeCatchUpDataSource {
    pub fn new(connect_timeout: Duration) -> Self {
        Self { connect_timeout }
    }
}

impl NodeCatchUpDataSource for TcpNodeCatchUpDataSource {
    fn install_snapshot_request(
        &mut self,
        source: &NodeCatchUpSource,
    ) -> DatabaseResult<Option<InstallSnapshotRequest>> {
        request_tcp_snapshot_fetch(
            &source.primary_address,
            self.connect_timeout,
            source.shard_id,
        )
    }

    fn log_entries(
        &mut self,
        source: &NodeCatchUpSource,
        start_index: LogIndex,
        max_entries: Option<usize>,
    ) -> DatabaseResult<Vec<LogEntry>> {
        request_tcp_catch_up_limited(
            &source.primary_address,
            self.connect_timeout,
            source.shard_id,
            start_index,
            max_entries,
        )
    }
}

pub fn request_tcp_raft_vote(
    address: &str,
    connect_timeout: Duration,
    shard_id: ShardId,
    request: RequestVoteRequest,
) -> DatabaseResult<RequestVoteResponse> {
    let mut addrs = address
        .to_socket_addrs()
        .map_err(|err| DatabaseError::Replication(format!("resolve {address}: {err}")))?;
    let addr = addrs
        .next()
        .ok_or_else(|| DatabaseError::Replication(format!("no socket address for {address}")))?;
    let mut stream = TcpStream::connect_timeout(&addr, connect_timeout)
        .map_err(|err| DatabaseError::Replication(format!("connect {address}: {err}")))?;
    write_tcp_raft_vote_request(&mut stream, shard_id, &request)?;
    stream
        .flush()
        .map_err(|err| DatabaseError::Replication(format!("flush raft vote request: {err}")))?;
    read_tcp_raft_vote_response(&mut stream)
}

pub fn request_tcp_raft_pre_vote(
    address: &str,
    connect_timeout: Duration,
    shard_id: ShardId,
    request: PreVoteRequest,
) -> DatabaseResult<PreVoteResponse> {
    let mut stream = connect_tcp_replication(address, connect_timeout)?;
    write_tcp_raft_pre_vote_request(&mut stream, shard_id, &request)?;
    stream
        .flush()
        .map_err(|err| DatabaseError::Replication(format!("flush raft pre-vote request: {err}")))?;
    read_tcp_raft_pre_vote_response(&mut stream)
}

pub fn request_tcp_raft_leader_transfer(
    address: &str,
    connect_timeout: Duration,
    shard_id: ShardId,
    transferee_id: ServerId,
) -> DatabaseResult<RequestVoteRequest> {
    let mut stream = connect_tcp_replication(address, connect_timeout)?;
    write_tcp_raft_leader_transfer_request(&mut stream, shard_id, transferee_id)?;
    stream.flush().map_err(|err| {
        DatabaseError::Replication(format!("flush raft leader-transfer request: {err}"))
    })?;
    read_tcp_raft_leader_transfer_response(&mut stream)
}

pub fn request_tcp_snapshot_fetch(
    address: &str,
    connect_timeout: Duration,
    shard_id: ShardId,
) -> DatabaseResult<Option<InstallSnapshotRequest>> {
    let first =
        request_tcp_snapshot_fetch_chunk(address, connect_timeout, shard_id, 0, 1024 * 1024)?;
    let Some(first_chunk) = first.snapshot else {
        return Ok(None);
    };
    if first_chunk.done {
        return Ok(Some(first_chunk.request));
    }
    let mut assembler = SnapshotChunkAssembler::new(first_chunk)?;
    while !assembler.resume_token().completed {
        let offset = assembler.next_offset();
        let chunk = request_tcp_snapshot_fetch_chunk(
            address,
            connect_timeout,
            shard_id,
            offset,
            1024 * 1024,
        )?;
        if chunk.checksum != first.checksum || chunk.total_len != first.total_len {
            return Err(DatabaseError::Replication(
                "snapshot fetch changed while resuming".to_string(),
            ));
        }
        let Some(snapshot_chunk) = chunk.snapshot else {
            return Err(DatabaseError::Replication(
                "snapshot fetch disappeared while resuming".to_string(),
            ));
        };
        if let Some(snapshot) = assembler.push(snapshot_chunk)? {
            return Ok(Some(snapshot));
        }
    }
    Err(DatabaseError::Replication(
        "snapshot fetch completed without assembled snapshot".to_string(),
    ))
}

pub fn request_tcp_snapshot_fetch_chunk(
    address: &str,
    connect_timeout: Duration,
    shard_id: ShardId,
    offset: u64,
    max_bytes: usize,
) -> DatabaseResult<TcpSnapshotFetchChunk> {
    let mut stream = connect_tcp_replication(address, connect_timeout)?;
    write_tcp_snapshot_fetch_chunk_request(&mut stream, shard_id, offset, max_bytes)?;
    stream.flush().map_err(|err| {
        DatabaseError::Replication(format!("flush snapshot fetch chunk request: {err}"))
    })?;
    read_tcp_snapshot_fetch_chunk_response(&mut stream)
}

pub fn request_tcp_replication_hello(
    address: &str,
    connect_timeout: Duration,
) -> DatabaseResult<ReplicationNodeIdentity> {
    let mut addrs = address
        .to_socket_addrs()
        .map_err(|err| DatabaseError::Replication(format!("resolve {address}: {err}")))?;
    let addr = addrs
        .next()
        .ok_or_else(|| DatabaseError::Replication(format!("no socket address for {address}")))?;
    let mut stream = TcpStream::connect_timeout(&addr, connect_timeout)
        .map_err(|err| DatabaseError::Replication(format!("connect {address}: {err}")))?;
    stream
        .write_all(TCP_REPLICATION_HELLO_MAGIC)
        .map_err(|err| DatabaseError::Replication(format!("write replication hello: {err}")))?;
    stream
        .flush()
        .map_err(|err| DatabaseError::Replication(format!("flush replication hello: {err}")))?;
    read_tcp_replication_hello_response(&mut stream)
}

pub fn request_tcp_replication_hello_on_stream(
    stream: &mut (impl Read + Write),
) -> DatabaseResult<ReplicationNodeIdentity> {
    stream
        .write_all(TCP_REPLICATION_HELLO_MAGIC)
        .map_err(|err| DatabaseError::Replication(format!("write replication hello: {err}")))?;
    stream
        .flush()
        .map_err(|err| DatabaseError::Replication(format!("flush replication hello: {err}")))?;
    read_tcp_replication_hello_response(stream)
}

pub fn request_tcp_install_snapshot(
    address: &str,
    connect_timeout: Duration,
    request: InstallSnapshotRequest,
) -> DatabaseResult<InstallSnapshotResponse> {
    let request = validate_snapshot_chunks_for_tcp(request)?;
    let mut addrs = address
        .to_socket_addrs()
        .map_err(|err| DatabaseError::Replication(format!("resolve {address}: {err}")))?;
    let addr = addrs
        .next()
        .ok_or_else(|| DatabaseError::Replication(format!("no socket address for {address}")))?;
    let mut stream = TcpStream::connect_timeout(&addr, connect_timeout)
        .map_err(|err| DatabaseError::Replication(format!("connect {address}: {err}")))?;
    write_tcp_install_snapshot_request(&mut stream, &request)?;
    stream.flush().map_err(|err| {
        DatabaseError::Replication(format!("flush install snapshot request: {err}"))
    })?;
    read_tcp_install_snapshot_response(&mut stream)
}

fn validate_snapshot_chunks_for_tcp(
    request: InstallSnapshotRequest,
) -> DatabaseResult<InstallSnapshotRequest> {
    let mut chunks = request.chunks(64 * 1024).into_iter();
    let Some(first) = chunks.next() else {
        return Ok(request);
    };
    let mut assembler = crate::raft::SnapshotChunkAssembler::new(first)?;
    for chunk in chunks {
        if let Some(assembled) = assembler.push(chunk)? {
            return Ok(assembled);
        }
    }
    Ok(request)
}

pub fn request_tcp_raft_append_or_install_snapshot(
    address: &str,
    connect_timeout: Duration,
    shard_id: ShardId,
    leader_commit: LogIndex,
    entries: &[LogEntry],
    snapshot: InstallSnapshotRequest,
) -> DatabaseResult<Vec<(ShardId, LogIndex)>> {
    match send_tcp_raft_append_batch_once(
        address,
        connect_timeout,
        shard_id,
        leader_commit,
        entries,
    ) {
        Ok(response) if response.append.success && response.append.durable => {
            Ok(response.ack_positions)
        }
        Ok(response) if response.append.success => Err(DatabaseError::Replication(format!(
            "raft append to {address} was accepted but not durable for shard {shard_id}"
        ))),
        Ok(_) | Err(_) => {
            let response = request_tcp_install_snapshot(address, connect_timeout, snapshot)?;
            if response.success {
                Ok(vec![(shard_id, response.last_included_index)])
            } else {
                Err(DatabaseError::Replication(format!(
                    "install snapshot rejected for shard {shard_id} at index {}",
                    response.last_included_index
                )))
            }
        }
    }
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

pub(super) fn catch_up_end_index(start_index: LogIndex, fetched_entries: usize) -> LogIndex {
    start_index
        .saturating_add(fetched_entries as u64)
        .saturating_sub(1)
}

pub(super) fn replica_targets(
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

pub(super) fn voter_count(
    routing_table: &ShardRoutingTable,
    entry: &LogEntry,
) -> DatabaseResult<usize> {
    let placement = routing_table.placement(entry.shard_id).ok_or_else(|| {
        DatabaseError::Replication(format!(
            "missing routing placement for shard {}",
            entry.shard_id
        ))
    })?;
    Ok(placement.replicas.len())
}

pub(super) fn send_tcp_replication_batch(
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

pub(super) fn send_tcp_raft_append_batch(
    address: &str,
    connect_timeout: Duration,
    max_attempts: usize,
    retry_backoff: Duration,
    shard_id: ShardId,
    leader_commit: LogIndex,
    entries: &[LogEntry],
) -> DatabaseResult<Vec<(ShardId, LogIndex)>> {
    let mut last_error = None;
    for attempt in 1..=max_attempts.max(1) {
        match send_tcp_raft_append_batch_once(
            address,
            connect_timeout,
            shard_id,
            leader_commit,
            entries,
        ) {
            Ok(response) if response.append.success && response.append.durable => {
                return Ok(response.ack_positions);
            }
            Ok(response) if response.append.success => {
                last_error = Some(DatabaseError::Replication(format!(
                    "raft append to {address} was accepted but not durable for shard {shard_id}"
                )));
            }
            Ok(response) => {
                last_error = Some(DatabaseError::LogConflict {
                    shard_id,
                    index: response.append.match_index,
                    message: format!(
                        "raft append rejected term={} conflict_index={:?} conflict_term={:?}",
                        response.append.term,
                        response.append.conflict_index,
                        response.append.conflict_term
                    ),
                });
            }
            Err(err) => last_error = Some(err),
        }
        if attempt < max_attempts {
            thread::sleep(retry_backoff);
        }
    }
    Err(last_error.unwrap_or_else(|| {
        DatabaseError::Replication(format!("raft append to {address} failed without an error"))
    }))
}

pub(super) fn send_tcp_raft_append_batches_by_shard(
    address: &str,
    connect_timeout: Duration,
    max_attempts: usize,
    retry_backoff: Duration,
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
        let mut acked = send_tcp_raft_append_batch(
            address,
            connect_timeout,
            max_attempts,
            retry_backoff,
            shard_id,
            0,
            &shard_entries,
        )?;
        positions.append(&mut acked);
    }
    Ok(positions)
}

pub(super) fn send_tcp_raft_append_batch_once(
    address: &str,
    connect_timeout: Duration,
    shard_id: ShardId,
    leader_commit: LogIndex,
    entries: &[LogEntry],
) -> DatabaseResult<TcpRaftAppendResponse> {
    let mut addrs = address
        .to_socket_addrs()
        .map_err(|err| DatabaseError::Replication(format!("resolve {address}: {err}")))?;
    let addr = addrs
        .next()
        .ok_or_else(|| DatabaseError::Replication(format!("no socket address for {address}")))?;
    let mut stream = TcpStream::connect_timeout(&addr, connect_timeout)
        .map_err(|err| DatabaseError::Replication(format!("connect {address}: {err}")))?;
    write_tcp_raft_append_request(&mut stream, shard_id, leader_commit, entries)?;
    stream
        .flush()
        .map_err(|err| DatabaseError::Replication(format!("flush raft append request: {err}")))?;
    read_tcp_raft_append_response(&mut stream)
}

pub fn send_tcp_raft_append_batch_on_stream(
    stream: &mut (impl Read + Write),
    shard_id: ShardId,
    leader_commit: LogIndex,
    entries: &[LogEntry],
) -> DatabaseResult<TcpRaftAppendResponse> {
    write_tcp_raft_append_request(stream, shard_id, leader_commit, entries)?;
    stream
        .flush()
        .map_err(|err| DatabaseError::Replication(format!("flush raft append request: {err}")))?;
    read_tcp_raft_append_response(stream)
}

pub fn request_tcp_raft_vote_on_stream(
    stream: &mut (impl Read + Write),
    shard_id: ShardId,
    request: RequestVoteRequest,
) -> DatabaseResult<RequestVoteResponse> {
    write_tcp_raft_vote_request(stream, shard_id, &request)?;
    stream
        .flush()
        .map_err(|err| DatabaseError::Replication(format!("flush raft vote request: {err}")))?;
    read_tcp_raft_vote_response(stream)
}

pub fn request_tcp_raft_pre_vote_on_stream(
    stream: &mut (impl Read + Write),
    shard_id: ShardId,
    request: PreVoteRequest,
) -> DatabaseResult<PreVoteResponse> {
    write_tcp_raft_pre_vote_request(stream, shard_id, &request)?;
    stream
        .flush()
        .map_err(|err| DatabaseError::Replication(format!("flush raft pre-vote request: {err}")))?;
    read_tcp_raft_pre_vote_response(stream)
}

pub fn request_tcp_install_snapshot_on_stream(
    stream: &mut (impl Read + Write),
    request: InstallSnapshotRequest,
) -> DatabaseResult<InstallSnapshotResponse> {
    let request = validate_snapshot_chunks_for_tcp(request)?;
    write_tcp_install_snapshot_request(stream, &request)?;
    stream.flush().map_err(|err| {
        DatabaseError::Replication(format!("flush install snapshot request: {err}"))
    })?;
    read_tcp_install_snapshot_response(stream)
}

pub(super) fn write_tcp_raft_append_request(
    writer: &mut impl Write,
    shard_id: ShardId,
    leader_commit: LogIndex,
    entries: &[LogEntry],
) -> DatabaseResult<()> {
    writer
        .write_all(TCP_RAFT_APPEND_REQUEST_MAGIC)
        .map_err(|err| DatabaseError::Replication(format!("write raft append magic: {err}")))?;
    write_u64(writer, shard_id)?;
    write_u64(writer, leader_commit)?;
    write_u32(writer, entries.len() as u32)?;
    for entry in entries {
        let payload = encode_log_entry(entry);
        write_u32(writer, payload.len() as u32)?;
        writer.write_all(&payload).map_err(|err| {
            DatabaseError::Replication(format!("write raft append entry payload: {err}"))
        })?;
    }
    Ok(())
}

pub(super) fn write_tcp_raft_vote_request(
    writer: &mut impl Write,
    shard_id: ShardId,
    request: &RequestVoteRequest,
) -> DatabaseResult<()> {
    writer
        .write_all(TCP_RAFT_VOTE_REQUEST_MAGIC)
        .map_err(|err| DatabaseError::Replication(format!("write raft vote magic: {err}")))?;
    write_u64(writer, shard_id)?;
    write_u64(writer, request.term)?;
    write_u64(writer, request.candidate_id)?;
    write_u64(writer, request.last_log_index)?;
    write_u64(writer, request.last_log_term)
}

pub(super) fn write_tcp_raft_pre_vote_request(
    writer: &mut impl Write,
    shard_id: ShardId,
    request: &PreVoteRequest,
) -> DatabaseResult<()> {
    writer
        .write_all(TCP_RAFT_PRE_VOTE_REQUEST_MAGIC)
        .map_err(|err| DatabaseError::Replication(format!("write raft pre-vote magic: {err}")))?;
    write_u64(writer, shard_id)?;
    write_u64(writer, request.next_term)?;
    write_u64(writer, request.candidate_id)?;
    write_u64(writer, request.last_log_index)?;
    write_u64(writer, request.last_log_term)
}

pub(super) fn write_tcp_raft_leader_transfer_request(
    writer: &mut impl Write,
    shard_id: ShardId,
    transferee_id: ServerId,
) -> DatabaseResult<()> {
    writer
        .write_all(TCP_RAFT_LEADER_TRANSFER_REQUEST_MAGIC)
        .map_err(|err| {
            DatabaseError::Replication(format!("write raft leader-transfer magic: {err}"))
        })?;
    write_u64(writer, shard_id)?;
    write_u64(writer, transferee_id)
}

pub(super) fn write_tcp_install_snapshot_request(
    writer: &mut impl Write,
    request: &InstallSnapshotRequest,
) -> DatabaseResult<()> {
    writer
        .write_all(TCP_RAFT_SNAPSHOT_REQUEST_MAGIC)
        .map_err(|err| {
            DatabaseError::Replication(format!("write install snapshot magic: {err}"))
        })?;
    write_u64(writer, request.term)?;
    write_u64(writer, request.leader_id)?;
    write_u64(writer, request.metadata.shard_id)?;
    write_u64(writer, request.metadata.last_included_term)?;
    write_u64(writer, request.metadata.last_included_index)?;
    write_u32(writer, request.payload.len() as u32)?;
    writer
        .write_all(&request.payload)
        .map_err(|err| DatabaseError::Replication(format!("write snapshot payload: {err}")))
}

#[allow(dead_code)]
pub(super) fn write_tcp_snapshot_fetch_request(
    writer: &mut impl Write,
    shard_id: ShardId,
) -> DatabaseResult<()> {
    writer
        .write_all(TCP_RAFT_SNAPSHOT_FETCH_REQUEST_MAGIC)
        .map_err(|err| DatabaseError::Replication(format!("write snapshot fetch magic: {err}")))?;
    write_u64(writer, shard_id)
}

pub(super) fn write_tcp_snapshot_fetch_chunk_request(
    writer: &mut impl Write,
    shard_id: ShardId,
    offset: u64,
    max_bytes: usize,
) -> DatabaseResult<()> {
    writer
        .write_all(TCP_RAFT_SNAPSHOT_FETCH_REQUEST_MAGIC_V2)
        .map_err(|err| {
            DatabaseError::Replication(format!("write snapshot fetch v2 magic: {err}"))
        })?;
    write_u64(writer, shard_id)?;
    write_u64(writer, offset)?;
    write_u32(writer, max_bytes.min(u32::MAX as usize) as u32)
}

pub(super) fn read_tcp_install_snapshot_request_after_magic(
    reader: &mut impl Read,
) -> DatabaseResult<InstallSnapshotRequest> {
    let term = read_u64(reader)?;
    let leader_id = read_u64(reader)?;
    let shard_id = read_u64(reader)?;
    let last_included_term = read_u64(reader)?;
    let last_included_index = read_u64(reader)?;
    let payload_len = read_u32(reader)? as usize;
    let mut payload = vec![0; payload_len];
    reader
        .read_exact(&mut payload)
        .map_err(|err| DatabaseError::Replication(format!("read snapshot payload: {err}")))?;
    Ok(InstallSnapshotRequest {
        term,
        leader_id,
        metadata: RaftSnapshotMetadata {
            shard_id,
            last_included_term,
            last_included_index,
        },
        payload,
    })
}

pub(super) fn read_tcp_raft_vote_request_after_magic(
    reader: &mut impl Read,
) -> DatabaseResult<RequestVoteRequest> {
    Ok(RequestVoteRequest {
        term: read_u64(reader)?,
        candidate_id: read_u64(reader)?,
        last_log_index: read_u64(reader)?,
        last_log_term: read_u64(reader)?,
    })
}

pub(super) fn read_tcp_raft_pre_vote_request_after_magic(
    reader: &mut impl Read,
) -> DatabaseResult<PreVoteRequest> {
    Ok(PreVoteRequest {
        next_term: read_u64(reader)?,
        candidate_id: read_u64(reader)?,
        last_log_index: read_u64(reader)?,
        last_log_term: read_u64(reader)?,
    })
}

fn connect_tcp_replication(address: &str, connect_timeout: Duration) -> DatabaseResult<TcpStream> {
    let mut addrs = address
        .to_socket_addrs()
        .map_err(|err| DatabaseError::Replication(format!("resolve {address}: {err}")))?;
    let addr = addrs
        .next()
        .ok_or_else(|| DatabaseError::Replication(format!("no socket address for {address}")))?;
    TcpStream::connect_timeout(&addr, connect_timeout)
        .map_err(|err| DatabaseError::Replication(format!("connect {address}: {err}")))
}

pub(super) fn send_tcp_replication_batch_once(
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

pub fn send_tcp_replication_batch_on_stream(
    stream: &mut (impl Read + Write),
    entries: &[LogEntry],
) -> DatabaseResult<Vec<(ShardId, LogIndex)>> {
    write_tcp_replication_request(stream, entries)?;
    stream
        .flush()
        .map_err(|err| DatabaseError::Replication(format!("flush replication request: {err}")))?;
    read_tcp_replication_response(stream)
}

pub(super) fn write_tcp_replication_request(
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

pub(super) fn read_tcp_replication_request_after_magic(
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
