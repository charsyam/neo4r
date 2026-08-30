use super::*;

pub(super) struct TcpCatchUpRequest {
    pub(super) shard_id: ShardId,
    pub(super) start_index: LogIndex,
    pub(super) max_entries: Option<usize>,
}

pub(super) fn request_tcp_catch_up(
    address: &str,
    connect_timeout: Duration,
    shard_id: ShardId,
    start_index: LogIndex,
) -> DatabaseResult<Vec<LogEntry>> {
    request_tcp_catch_up_limited(address, connect_timeout, shard_id, start_index, None)
}

pub(in crate::replication) fn request_tcp_catch_up_limited(
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

pub fn request_tcp_catch_up_on_stream(
    stream: &mut (impl Read + Write),
    shard_id: ShardId,
    start_index: LogIndex,
    max_entries: Option<usize>,
) -> DatabaseResult<Vec<LogEntry>> {
    write_tcp_catch_up_request(stream, shard_id, start_index, max_entries)?;
    stream
        .flush()
        .map_err(|err| DatabaseError::Replication(format!("flush catch-up request: {err}")))?;
    let entries = read_tcp_catch_up_response(stream)?;
    validate_tcp_catch_up_entries(shard_id, start_index, max_entries, &entries)?;
    Ok(entries)
}

pub(super) fn write_tcp_catch_up_request(
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

pub(super) fn read_tcp_catch_up_request_after_magic(
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

pub(super) fn read_catch_up_entries(
    db: &Neo4rDatabaseHandle,
    request: &TcpCatchUpRequest,
) -> DatabaseResult<Vec<LogEntry>> {
    let mut entries = db.log_entries_from(request.shard_id, request.start_index)?;
    if let Some(max_entries) = request.max_entries {
        entries.truncate(max_entries);
    }
    Ok(entries)
}

pub(super) fn validate_tcp_catch_up_entries(
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

pub(super) fn write_tcp_catch_up_response(
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

pub(super) fn read_tcp_catch_up_response(reader: &mut impl Read) -> DatabaseResult<Vec<LogEntry>> {
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

pub(super) fn write_tcp_replication_response(
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

pub(super) fn read_tcp_replication_response(
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

pub(super) fn write_tcp_raft_append_response(
    writer: &mut impl Write,
    result: &DatabaseResult<TcpRaftAppendResponse>,
) -> DatabaseResult<()> {
    writer
        .write_all(TCP_RAFT_APPEND_RESPONSE_MAGIC)
        .map_err(|err| DatabaseError::Replication(format!("write raft append response: {err}")))?;
    match result {
        Ok(response) => {
            writer.write_all(&[TCP_REPLICATION_OK]).map_err(|err| {
                DatabaseError::Replication(format!("write raft append ok: {err}"))
            })?;
            let payload = encode_tcp_raft_append_response(response);
            write_u32(writer, payload.len() as u32)?;
            writer.write_all(&payload).map_err(|err| {
                DatabaseError::Replication(format!("write raft append ok payload: {err}"))
            })
        }
        Err(err) => {
            writer.write_all(&[TCP_REPLICATION_ERR]).map_err(|err| {
                DatabaseError::Replication(format!("write raft append err: {err}"))
            })?;
            let message = err.to_string();
            write_u32(writer, message.len() as u32)?;
            writer.write_all(message.as_bytes()).map_err(|err| {
                DatabaseError::Replication(format!("write raft append err payload: {err}"))
            })
        }
    }
}

pub(super) fn read_tcp_raft_append_response(
    reader: &mut impl Read,
) -> DatabaseResult<TcpRaftAppendResponse> {
    read_magic(
        reader,
        TCP_RAFT_APPEND_RESPONSE_MAGIC,
        "raft append response",
    )?;
    let mut status = [0; 1];
    reader
        .read_exact(&mut status)
        .map_err(|err| DatabaseError::Replication(format!("read raft append status: {err}")))?;
    let message_len = read_u32(reader)? as usize;
    let mut message = vec![0; message_len];
    reader
        .read_exact(&mut message)
        .map_err(|err| DatabaseError::Replication(format!("read raft append response: {err}")))?;
    match status[0] {
        TCP_REPLICATION_OK => decode_tcp_raft_append_response(&message),
        TCP_REPLICATION_ERR => Err(DatabaseError::Replication(
            String::from_utf8_lossy(&message).into_owned(),
        )),
        value => Err(DatabaseError::Replication(format!(
            "unknown raft append response status {value}"
        ))),
    }
}

pub(super) fn encode_tcp_raft_append_response(response: &TcpRaftAppendResponse) -> Vec<u8> {
    let mut payload = Vec::new();
    payload.extend_from_slice(&response.append.term.to_be_bytes());
    payload.push(u8::from(response.append.success));
    payload.extend_from_slice(&response.append.match_index.to_be_bytes());
    encode_optional_u64(&mut payload, response.append.conflict_index);
    encode_optional_u64(&mut payload, response.append.conflict_term);
    let ack_payload = encode_replication_ack_positions(&response.ack_positions);
    payload.extend_from_slice(&(ack_payload.len() as u32).to_be_bytes());
    payload.extend_from_slice(&ack_payload);
    payload
}

pub(super) fn decode_tcp_raft_append_response(
    payload: &[u8],
) -> DatabaseResult<TcpRaftAppendResponse> {
    if payload.len() < 31 {
        return Err(DatabaseError::Replication(
            "invalid raft append response payload".to_string(),
        ));
    }
    let term = u64::from_be_bytes(payload[0..8].try_into().unwrap());
    let success = payload[8] != 0;
    let match_index = u64::from_be_bytes(payload[9..17].try_into().unwrap());
    let (conflict_index, offset) = decode_optional_u64(payload, 17)?;
    let (conflict_term, offset) = decode_optional_u64(payload, offset)?;
    if payload.len() < offset + 4 {
        return Err(DatabaseError::Replication(
            "truncated raft append ack payload length".to_string(),
        ));
    }
    let ack_len = u32::from_be_bytes(payload[offset..offset + 4].try_into().unwrap()) as usize;
    let ack_start = offset + 4;
    if payload.len() != ack_start + ack_len {
        return Err(DatabaseError::Replication(
            "invalid raft append ack payload length".to_string(),
        ));
    }
    Ok(TcpRaftAppendResponse {
        append: AppendEntriesResponse {
            term,
            success,
            match_index,
            conflict_index,
            conflict_term,
        },
        ack_positions: decode_replication_ack_positions(&payload[ack_start..])?,
    })
}

pub(super) fn encode_optional_u64(payload: &mut Vec<u8>, value: Option<u64>) {
    match value {
        Some(value) => {
            payload.push(1);
            payload.extend_from_slice(&value.to_be_bytes());
        }
        None => {
            payload.push(0);
            payload.extend_from_slice(&0_u64.to_be_bytes());
        }
    }
}

pub(super) fn decode_optional_u64(
    payload: &[u8],
    offset: usize,
) -> DatabaseResult<(Option<u64>, usize)> {
    if payload.len() < offset + 9 {
        return Err(DatabaseError::Replication(
            "truncated optional u64 in raft append response".to_string(),
        ));
    }
    let present = payload[offset] != 0;
    let value = u64::from_be_bytes(payload[offset + 1..offset + 9].try_into().unwrap());
    Ok((present.then_some(value), offset + 9))
}

pub(super) fn write_tcp_raft_vote_response(
    writer: &mut impl Write,
    result: &DatabaseResult<RequestVoteResponse>,
) -> DatabaseResult<()> {
    writer
        .write_all(TCP_REPLICATION_RESPONSE_MAGIC)
        .map_err(|err| DatabaseError::Replication(format!("write raft vote response: {err}")))?;
    match result {
        Ok(response) => {
            writer
                .write_all(&[TCP_REPLICATION_OK])
                .map_err(|err| DatabaseError::Replication(format!("write raft vote ok: {err}")))?;
            let mut payload = Vec::with_capacity(9);
            payload.extend_from_slice(&response.term.to_be_bytes());
            payload.push(u8::from(response.vote_granted));
            write_u32(writer, payload.len() as u32)?;
            writer.write_all(&payload).map_err(|err| {
                DatabaseError::Replication(format!("write raft vote ok payload: {err}"))
            })
        }
        Err(err) => {
            writer
                .write_all(&[TCP_REPLICATION_ERR])
                .map_err(|err| DatabaseError::Replication(format!("write raft vote err: {err}")))?;
            let message = err.to_string();
            write_u32(writer, message.len() as u32)?;
            writer
                .write_all(message.as_bytes())
                .map_err(|err| DatabaseError::Replication(format!("write raft vote err: {err}")))
        }
    }
}

pub(super) fn read_tcp_raft_vote_response(
    reader: &mut impl Read,
) -> DatabaseResult<RequestVoteResponse> {
    read_magic(reader, TCP_REPLICATION_RESPONSE_MAGIC, "raft vote response")?;
    let mut status = [0; 1];
    reader
        .read_exact(&mut status)
        .map_err(|err| DatabaseError::Replication(format!("read raft vote status: {err}")))?;
    let message_len = read_u32(reader)? as usize;
    let mut message = vec![0; message_len];
    reader
        .read_exact(&mut message)
        .map_err(|err| DatabaseError::Replication(format!("read raft vote response: {err}")))?;
    match status[0] {
        TCP_REPLICATION_OK => {
            if message.len() != 9 {
                return Err(DatabaseError::Replication(
                    "invalid raft vote response payload".to_string(),
                ));
            }
            Ok(RequestVoteResponse {
                term: u64::from_be_bytes(message[..8].try_into().unwrap()),
                vote_granted: message[8] != 0,
            })
        }
        TCP_REPLICATION_ERR => Err(DatabaseError::Replication(
            String::from_utf8_lossy(&message).into_owned(),
        )),
        value => Err(DatabaseError::Replication(format!(
            "unknown raft vote response status {value}"
        ))),
    }
}

pub(super) fn write_tcp_raft_pre_vote_response(
    writer: &mut impl Write,
    result: &DatabaseResult<PreVoteResponse>,
) -> DatabaseResult<()> {
    writer
        .write_all(TCP_REPLICATION_RESPONSE_MAGIC)
        .map_err(|err| {
            DatabaseError::Replication(format!("write raft pre-vote response: {err}"))
        })?;
    match result {
        Ok(response) => {
            writer.write_all(&[TCP_REPLICATION_OK]).map_err(|err| {
                DatabaseError::Replication(format!("write raft pre-vote ok: {err}"))
            })?;
            let mut payload = Vec::with_capacity(9);
            payload.extend_from_slice(&response.term.to_be_bytes());
            payload.push(u8::from(response.vote_granted));
            write_u32(writer, payload.len() as u32)?;
            writer.write_all(&payload).map_err(|err| {
                DatabaseError::Replication(format!("write raft pre-vote ok payload: {err}"))
            })
        }
        Err(err) => {
            writer.write_all(&[TCP_REPLICATION_ERR]).map_err(|err| {
                DatabaseError::Replication(format!("write raft pre-vote err: {err}"))
            })?;
            let message = err.to_string();
            write_u32(writer, message.len() as u32)?;
            writer.write_all(message.as_bytes()).map_err(|err| {
                DatabaseError::Replication(format!("write raft pre-vote err payload: {err}"))
            })
        }
    }
}

pub(super) fn read_tcp_raft_pre_vote_response(
    reader: &mut impl Read,
) -> DatabaseResult<PreVoteResponse> {
    read_tcp_raft_vote_response(reader).map(|response| PreVoteResponse {
        term: response.term,
        vote_granted: response.vote_granted,
    })
}

pub(super) fn write_tcp_raft_leader_transfer_response(
    writer: &mut impl Write,
    result: &DatabaseResult<RequestVoteRequest>,
) -> DatabaseResult<()> {
    writer
        .write_all(TCP_REPLICATION_RESPONSE_MAGIC)
        .map_err(|err| {
            DatabaseError::Replication(format!("write raft leader-transfer response: {err}"))
        })?;
    match result {
        Ok(request) => {
            writer.write_all(&[TCP_REPLICATION_OK]).map_err(|err| {
                DatabaseError::Replication(format!("write leader-transfer ok: {err}"))
            })?;
            let mut payload = Vec::with_capacity(32);
            payload.extend_from_slice(&request.term.to_be_bytes());
            payload.extend_from_slice(&request.candidate_id.to_be_bytes());
            payload.extend_from_slice(&request.last_log_index.to_be_bytes());
            payload.extend_from_slice(&request.last_log_term.to_be_bytes());
            write_u32(writer, payload.len() as u32)?;
            writer.write_all(&payload).map_err(|err| {
                DatabaseError::Replication(format!("write leader-transfer payload: {err}"))
            })
        }
        Err(err) => {
            writer.write_all(&[TCP_REPLICATION_ERR]).map_err(|err| {
                DatabaseError::Replication(format!("write leader-transfer err: {err}"))
            })?;
            let message = err.to_string();
            write_u32(writer, message.len() as u32)?;
            writer.write_all(message.as_bytes()).map_err(|err| {
                DatabaseError::Replication(format!("write leader-transfer err payload: {err}"))
            })
        }
    }
}

pub(super) fn read_tcp_raft_leader_transfer_response(
    reader: &mut impl Read,
) -> DatabaseResult<RequestVoteRequest> {
    read_magic(
        reader,
        TCP_REPLICATION_RESPONSE_MAGIC,
        "raft leader-transfer response",
    )?;
    let mut status = [0; 1];
    reader
        .read_exact(&mut status)
        .map_err(|err| DatabaseError::Replication(format!("read leader-transfer status: {err}")))?;
    let message_len = read_u32(reader)? as usize;
    let mut message = vec![0; message_len];
    reader.read_exact(&mut message).map_err(|err| {
        DatabaseError::Replication(format!("read leader-transfer response: {err}"))
    })?;
    match status[0] {
        TCP_REPLICATION_OK => {
            if message.len() != 32 {
                return Err(DatabaseError::Replication(
                    "invalid leader-transfer response payload".to_string(),
                ));
            }
            Ok(RequestVoteRequest {
                term: u64::from_be_bytes(message[0..8].try_into().unwrap()),
                candidate_id: u64::from_be_bytes(message[8..16].try_into().unwrap()),
                last_log_index: u64::from_be_bytes(message[16..24].try_into().unwrap()),
                last_log_term: u64::from_be_bytes(message[24..32].try_into().unwrap()),
            })
        }
        TCP_REPLICATION_ERR => Err(DatabaseError::Replication(
            String::from_utf8_lossy(&message).into_owned(),
        )),
        value => Err(DatabaseError::Replication(format!(
            "unknown leader-transfer response status {value}"
        ))),
    }
}

pub(super) fn write_tcp_install_snapshot_response(
    writer: &mut impl Write,
    result: &DatabaseResult<InstallSnapshotResponse>,
) -> DatabaseResult<()> {
    writer
        .write_all(TCP_REPLICATION_RESPONSE_MAGIC)
        .map_err(|err| {
            DatabaseError::Replication(format!("write install snapshot response: {err}"))
        })?;
    match result {
        Ok(response) => {
            writer.write_all(&[TCP_REPLICATION_OK]).map_err(|err| {
                DatabaseError::Replication(format!("write install snapshot ok: {err}"))
            })?;
            let mut payload = Vec::with_capacity(17);
            payload.extend_from_slice(&response.term.to_be_bytes());
            payload.push(u8::from(response.success));
            payload.extend_from_slice(&response.last_included_index.to_be_bytes());
            write_u32(writer, payload.len() as u32)?;
            writer.write_all(&payload).map_err(|err| {
                DatabaseError::Replication(format!("write install snapshot ok payload: {err}"))
            })
        }
        Err(err) => {
            writer.write_all(&[TCP_REPLICATION_ERR]).map_err(|err| {
                DatabaseError::Replication(format!("write install snapshot err: {err}"))
            })?;
            let message = err.to_string();
            write_u32(writer, message.len() as u32)?;
            writer.write_all(message.as_bytes()).map_err(|err| {
                DatabaseError::Replication(format!("write install snapshot err payload: {err}"))
            })
        }
    }
}

pub(super) fn read_tcp_install_snapshot_response(
    reader: &mut impl Read,
) -> DatabaseResult<InstallSnapshotResponse> {
    read_magic(
        reader,
        TCP_REPLICATION_RESPONSE_MAGIC,
        "install snapshot response",
    )?;
    let mut status = [0; 1];
    reader.read_exact(&mut status).map_err(|err| {
        DatabaseError::Replication(format!("read install snapshot status: {err}"))
    })?;
    let message_len = read_u32(reader)? as usize;
    let mut message = vec![0; message_len];
    reader.read_exact(&mut message).map_err(|err| {
        DatabaseError::Replication(format!("read install snapshot response: {err}"))
    })?;
    match status[0] {
        TCP_REPLICATION_OK => {
            if message.len() != 17 {
                return Err(DatabaseError::Replication(
                    "invalid install snapshot response payload".to_string(),
                ));
            }
            Ok(InstallSnapshotResponse {
                term: u64::from_be_bytes(message[..8].try_into().unwrap()),
                success: message[8] != 0,
                last_included_index: u64::from_be_bytes(message[9..17].try_into().unwrap()),
            })
        }
        TCP_REPLICATION_ERR => Err(DatabaseError::Replication(
            String::from_utf8_lossy(&message).into_owned(),
        )),
        value => Err(DatabaseError::Replication(format!(
            "unknown install snapshot response status {value}"
        ))),
    }
}

pub(super) fn replication_ack_positions(entries: &[LogEntry]) -> Vec<(ShardId, LogIndex)> {
    entries
        .iter()
        .map(|entry| (entry.shard_id, entry.index))
        .collect()
}

pub(super) fn encode_replication_ack_positions(positions: &[(ShardId, LogIndex)]) -> Vec<u8> {
    let mut payload = Vec::with_capacity(4 + positions.len() * 16);
    payload.extend_from_slice(&(positions.len() as u32).to_be_bytes());
    for (shard_id, index) in positions {
        payload.extend_from_slice(&shard_id.to_be_bytes());
        payload.extend_from_slice(&index.to_be_bytes());
    }
    payload
}

pub(super) fn decode_replication_ack_positions(
    payload: &[u8],
) -> DatabaseResult<Vec<(ShardId, LogIndex)>> {
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

pub(super) fn read_magic(
    reader: &mut impl Read,
    expected: &[u8],
    context: &str,
) -> DatabaseResult<()> {
    let magic = read_magic_bytes(reader)?;
    if magic == expected {
        Ok(())
    } else {
        Err(DatabaseError::Replication(format!(
            "invalid {context} magic"
        )))
    }
}

pub(super) fn read_magic_bytes(reader: &mut impl Read) -> DatabaseResult<Vec<u8>> {
    let mut magic = vec![0; TCP_REPLICATION_REQUEST_MAGIC.len()];
    reader
        .read_exact(&mut magic)
        .map_err(|err| DatabaseError::Replication(format!("read replication magic: {err}")))?;
    Ok(magic)
}

pub(super) fn write_u32(writer: &mut impl Write, value: u32) -> DatabaseResult<()> {
    writer
        .write_all(&value.to_be_bytes())
        .map_err(|err| DatabaseError::Replication(format!("write u32: {err}")))
}

pub(super) fn write_u64(writer: &mut impl Write, value: u64) -> DatabaseResult<()> {
    writer
        .write_all(&value.to_be_bytes())
        .map_err(|err| DatabaseError::Replication(format!("write u64: {err}")))
}

pub(super) fn read_u64(reader: &mut impl Read) -> DatabaseResult<u64> {
    let mut bytes = [0; 8];
    reader
        .read_exact(&mut bytes)
        .map_err(|err| DatabaseError::Replication(format!("read u64: {err}")))?;
    Ok(u64::from_be_bytes(bytes))
}

pub(super) fn read_u32(reader: &mut impl Read) -> DatabaseResult<u32> {
    let mut bytes = [0; 4];
    reader
        .read_exact(&mut bytes)
        .map_err(|err| DatabaseError::Replication(format!("read u32: {err}")))?;
    Ok(u32::from_be_bytes(bytes))
}

pub(super) fn write_tcp_replication_hello_response(
    writer: &mut impl Write,
    result: &DatabaseResult<ReplicationNodeIdentity>,
) -> DatabaseResult<()> {
    writer
        .write_all(TCP_REPLICATION_RESPONSE_MAGIC)
        .map_err(|err| {
            DatabaseError::Replication(format!("write replication hello magic: {err}"))
        })?;
    match result {
        Ok(identity) => {
            writer.write_all(&[TCP_REPLICATION_OK]).map_err(|err| {
                DatabaseError::Replication(format!("write replication hello ok: {err}"))
            })?;
            let payload = encode_replication_node_identity(identity);
            write_u32(writer, payload.len() as u32)?;
            writer.write_all(payload.as_bytes()).map_err(|err| {
                DatabaseError::Replication(format!("write replication hello payload: {err}"))
            })
        }
        Err(err) => {
            writer
                .write_all(&[TCP_REPLICATION_ERR])
                .map_err(|write_err| {
                    DatabaseError::Replication(format!("write replication hello err: {write_err}"))
                })?;
            let message = err.to_string();
            write_u32(writer, message.len() as u32)?;
            writer.write_all(message.as_bytes()).map_err(|write_err| {
                DatabaseError::Replication(format!(
                    "write replication hello err payload: {write_err}"
                ))
            })
        }
    }
}

pub(super) fn read_tcp_replication_hello_response(
    reader: &mut impl Read,
) -> DatabaseResult<ReplicationNodeIdentity> {
    read_magic(
        reader,
        TCP_REPLICATION_RESPONSE_MAGIC,
        "replication hello response",
    )?;
    let mut status = [0; 1];
    reader.read_exact(&mut status).map_err(|err| {
        DatabaseError::Replication(format!("read replication hello status: {err}"))
    })?;
    let len = read_u32(reader)? as usize;
    let mut payload = vec![0; len];
    reader.read_exact(&mut payload).map_err(|err| {
        DatabaseError::Replication(format!("read replication hello payload: {err}"))
    })?;
    if status[0] == TCP_REPLICATION_ERR {
        return Err(DatabaseError::Replication(
            String::from_utf8_lossy(&payload).to_string(),
        ));
    }
    decode_replication_node_identity(&String::from_utf8_lossy(&payload))
}

fn encode_replication_node_identity(identity: &ReplicationNodeIdentity) -> String {
    let transports = identity
        .transports
        .iter()
        .map(|kind| match kind {
            ReplicationChannelKind::Tcp => "tcp",
            ReplicationChannelKind::Udp => "udp",
            ReplicationChannelKind::Rdma => "rdma",
            ReplicationChannelKind::Custom => "custom",
        })
        .collect::<Vec<_>>()
        .join(",");
    format!(
        "{}\t{}\t{}\t{}\t{}",
        identity.server_id, identity.node_id, identity.cluster_id, identity.database_id, transports
    )
}

fn decode_replication_node_identity(input: &str) -> DatabaseResult<ReplicationNodeIdentity> {
    let parts = input.split('\t').collect::<Vec<_>>();
    if parts.len() != 5 {
        return Err(DatabaseError::Replication(
            "invalid replication hello identity payload".to_string(),
        ));
    }
    let server_id = parts[0]
        .parse::<u64>()
        .map_err(|_| DatabaseError::Replication("invalid hello server id".to_string()))?;
    let node_id = parts[1]
        .parse::<u64>()
        .map_err(|_| DatabaseError::Replication("invalid hello node id".to_string()))?;
    let transports = parts[4]
        .split(',')
        .filter(|part| !part.is_empty())
        .map(|part| match part {
            "tcp" => Ok(ReplicationChannelKind::Tcp),
            "udp" => Ok(ReplicationChannelKind::Udp),
            "rdma" => Ok(ReplicationChannelKind::Rdma),
            "custom" => Ok(ReplicationChannelKind::Custom),
            other => Err(DatabaseError::Replication(format!(
                "invalid hello transport {other:?}"
            ))),
        })
        .collect::<DatabaseResult<Vec<_>>>()?;
    Ok(ReplicationNodeIdentity {
        server_id,
        node_id,
        cluster_id: parts[2].to_string(),
        database_id: parts[3].to_string(),
        transports,
    })
}
