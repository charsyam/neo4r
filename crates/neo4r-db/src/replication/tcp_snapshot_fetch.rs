use super::*;

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

fn read_u32(reader: &mut impl Read) -> DatabaseResult<u32> {
    let mut bytes = [0; 4];
    reader
        .read_exact(&mut bytes)
        .map_err(|err| DatabaseError::Replication(format!("read u32: {err}")))?;
    Ok(u32::from_be_bytes(bytes))
}

fn read_u64(reader: &mut impl Read) -> DatabaseResult<u64> {
    let mut bytes = [0; 8];
    reader
        .read_exact(&mut bytes)
        .map_err(|err| DatabaseError::Replication(format!("read u64: {err}")))?;
    Ok(u64::from_be_bytes(bytes))
}

fn read_magic(reader: &mut impl Read, expected: &[u8], context: &str) -> DatabaseResult<()> {
    let mut magic = vec![0; expected.len()];
    reader
        .read_exact(&mut magic)
        .map_err(|err| DatabaseError::Replication(format!("read {context} magic: {err}")))?;
    if magic == expected {
        Ok(())
    } else {
        Err(DatabaseError::Replication(format!(
            "invalid {context} magic"
        )))
    }
}

pub(super) fn write_tcp_snapshot_fetch_response(
    writer: &mut impl Write,
    result: &DatabaseResult<Option<InstallSnapshotRequest>>,
) -> DatabaseResult<()> {
    writer
        .write_all(TCP_REPLICATION_RESPONSE_MAGIC)
        .map_err(|err| {
            DatabaseError::Replication(format!("write snapshot fetch response: {err}"))
        })?;
    match result {
        Ok(snapshot) => {
            writer.write_all(&[TCP_REPLICATION_OK]).map_err(|err| {
                DatabaseError::Replication(format!("write snapshot fetch ok: {err}"))
            })?;
            let payload = encode_snapshot_fetch_payload(snapshot)?;
            write_u32(writer, payload.len() as u32)?;
            writer.write_all(&payload).map_err(|err| {
                DatabaseError::Replication(format!("write snapshot fetch ok payload: {err}"))
            })
        }
        Err(err) => {
            writer.write_all(&[TCP_REPLICATION_ERR]).map_err(|err| {
                DatabaseError::Replication(format!("write snapshot fetch err: {err}"))
            })?;
            let message = err.to_string();
            write_u32(writer, message.len() as u32)?;
            writer.write_all(message.as_bytes()).map_err(|err| {
                DatabaseError::Replication(format!("write snapshot fetch err payload: {err}"))
            })
        }
    }
}

pub(super) fn write_tcp_snapshot_fetch_chunk_response(
    writer: &mut impl Write,
    result: &DatabaseResult<TcpSnapshotFetchChunk>,
) -> DatabaseResult<()> {
    writer
        .write_all(TCP_REPLICATION_RESPONSE_MAGIC)
        .map_err(|err| {
            DatabaseError::Replication(format!("write snapshot fetch chunk response: {err}"))
        })?;
    match result {
        Ok(chunk) => {
            writer.write_all(&[TCP_REPLICATION_OK]).map_err(|err| {
                DatabaseError::Replication(format!("write snapshot fetch chunk ok: {err}"))
            })?;
            let payload = encode_snapshot_fetch_chunk_payload(chunk)?;
            write_u32(writer, payload.len() as u32)?;
            writer.write_all(&payload).map_err(|err| {
                DatabaseError::Replication(format!("write snapshot fetch chunk payload: {err}"))
            })
        }
        Err(err) => {
            writer.write_all(&[TCP_REPLICATION_ERR]).map_err(|err| {
                DatabaseError::Replication(format!("write snapshot fetch chunk err: {err}"))
            })?;
            let message = err.to_string();
            write_u32(writer, message.len() as u32)?;
            writer.write_all(message.as_bytes()).map_err(|err| {
                DatabaseError::Replication(format!("write snapshot fetch chunk err payload: {err}"))
            })
        }
    }
}

#[allow(dead_code)]
pub(super) fn read_tcp_snapshot_fetch_response(
    reader: &mut impl Read,
) -> DatabaseResult<Option<InstallSnapshotRequest>> {
    read_magic(
        reader,
        TCP_REPLICATION_RESPONSE_MAGIC,
        "snapshot fetch response",
    )?;
    let mut status = [0; 1];
    reader
        .read_exact(&mut status)
        .map_err(|err| DatabaseError::Replication(format!("read snapshot fetch status: {err}")))?;
    let message_len = read_u32(reader)? as usize;
    let mut message = vec![0; message_len];
    reader.read_exact(&mut message).map_err(|err| {
        DatabaseError::Replication(format!("read snapshot fetch response: {err}"))
    })?;
    match status[0] {
        TCP_REPLICATION_OK => decode_snapshot_fetch_payload(&message),
        TCP_REPLICATION_ERR => Err(DatabaseError::Replication(
            String::from_utf8_lossy(&message).into_owned(),
        )),
        value => Err(DatabaseError::Replication(format!(
            "unknown snapshot fetch response status {value}"
        ))),
    }
}

pub(super) fn read_tcp_snapshot_fetch_chunk_response(
    reader: &mut impl Read,
) -> DatabaseResult<TcpSnapshotFetchChunk> {
    read_magic(
        reader,
        TCP_REPLICATION_RESPONSE_MAGIC,
        "snapshot fetch chunk response",
    )?;
    let mut status = [0; 1];
    reader.read_exact(&mut status).map_err(|err| {
        DatabaseError::Replication(format!("read snapshot fetch chunk status: {err}"))
    })?;
    let message_len = read_u32(reader)? as usize;
    let mut message = vec![0; message_len];
    reader.read_exact(&mut message).map_err(|err| {
        DatabaseError::Replication(format!("read snapshot fetch chunk response: {err}"))
    })?;
    match status[0] {
        TCP_REPLICATION_OK => decode_snapshot_fetch_chunk_payload(&message),
        TCP_REPLICATION_ERR => Err(DatabaseError::Replication(
            String::from_utf8_lossy(&message).into_owned(),
        )),
        value => Err(DatabaseError::Replication(format!(
            "unknown snapshot fetch chunk response status {value}"
        ))),
    }
}

pub(super) fn slice_snapshot_fetch_chunk(
    snapshot: Option<InstallSnapshotRequest>,
    offset: u64,
    max_bytes: usize,
) -> DatabaseResult<TcpSnapshotFetchChunk> {
    let Some(snapshot) = snapshot else {
        return Ok(TcpSnapshotFetchChunk {
            snapshot: None,
            total_len: 0,
            checksum: 0,
            resume_offset: 0,
        });
    };
    let total_len = snapshot.payload.len() as u64;
    if offset > total_len {
        return Err(DatabaseError::Replication(format!(
            "snapshot fetch offset {offset} beyond snapshot length {total_len}"
        )));
    }
    let start = offset as usize;
    let end = snapshot
        .payload
        .len()
        .min(start.saturating_add(max_bytes.max(1)));
    let payload = snapshot.payload[start..end].to_vec();
    Ok(TcpSnapshotFetchChunk {
        total_len,
        checksum: snapshot_checksum(&snapshot.payload),
        resume_offset: end as u64,
        snapshot: Some(InstallSnapshotChunk {
            request: InstallSnapshotRequest {
                term: snapshot.term,
                leader_id: snapshot.leader_id,
                metadata: snapshot.metadata,
                payload,
            },
            offset,
            done: end == total_len as usize,
        }),
    })
}

fn encode_snapshot_fetch_payload(
    snapshot: &Option<InstallSnapshotRequest>,
) -> DatabaseResult<Vec<u8>> {
    let mut payload = Vec::new();
    match snapshot {
        Some(snapshot) => {
            payload.push(1);
            write_u64(&mut payload, snapshot.term)?;
            write_u64(&mut payload, snapshot.leader_id)?;
            write_u64(&mut payload, snapshot.metadata.shard_id)?;
            write_u64(&mut payload, snapshot.metadata.last_included_term)?;
            write_u64(&mut payload, snapshot.metadata.last_included_index)?;
            write_u32(&mut payload, snapshot.payload.len() as u32)?;
            payload.extend_from_slice(&snapshot.payload);
        }
        None => payload.push(0),
    }
    Ok(payload)
}

fn encode_snapshot_fetch_chunk_payload(chunk: &TcpSnapshotFetchChunk) -> DatabaseResult<Vec<u8>> {
    let mut payload = Vec::new();
    write_u64(&mut payload, chunk.total_len)?;
    write_u64(&mut payload, chunk.checksum)?;
    write_u64(&mut payload, chunk.resume_offset)?;
    match &chunk.snapshot {
        Some(chunk) => {
            payload.push(1);
            write_u64(&mut payload, chunk.offset)?;
            payload.push(u8::from(chunk.done));
            write_u64(&mut payload, chunk.request.term)?;
            write_u64(&mut payload, chunk.request.leader_id)?;
            write_u64(&mut payload, chunk.request.metadata.shard_id)?;
            write_u64(&mut payload, chunk.request.metadata.last_included_term)?;
            write_u64(&mut payload, chunk.request.metadata.last_included_index)?;
            write_u32(&mut payload, chunk.request.payload.len() as u32)?;
            payload.extend_from_slice(&chunk.request.payload);
        }
        None => payload.push(0),
    }
    Ok(payload)
}

fn decode_snapshot_fetch_chunk_payload(payload: &[u8]) -> DatabaseResult<TcpSnapshotFetchChunk> {
    let mut reader = std::io::Cursor::new(payload);
    let total_len = read_u64(&mut reader)?;
    let checksum = read_u64(&mut reader)?;
    let resume_offset = read_u64(&mut reader)?;
    let mut flag = [0; 1];
    reader.read_exact(&mut flag).map_err(|err| {
        DatabaseError::Replication(format!("read snapshot fetch chunk flag: {err}"))
    })?;
    if flag[0] == 0 {
        return Ok(TcpSnapshotFetchChunk {
            snapshot: None,
            total_len,
            checksum,
            resume_offset,
        });
    }
    if flag[0] != 1 {
        return Err(DatabaseError::Replication(format!(
            "invalid snapshot fetch chunk flag {}",
            flag[0]
        )));
    }
    let offset = read_u64(&mut reader)?;
    let mut done = [0; 1];
    reader.read_exact(&mut done).map_err(|err| {
        DatabaseError::Replication(format!("read snapshot fetch chunk done: {err}"))
    })?;
    let term = read_u64(&mut reader)?;
    let leader_id = read_u64(&mut reader)?;
    let shard_id = read_u64(&mut reader)?;
    let last_included_term = read_u64(&mut reader)?;
    let last_included_index = read_u64(&mut reader)?;
    let payload_len = read_u32(&mut reader)? as usize;
    let mut chunk_payload = vec![0; payload_len];
    reader.read_exact(&mut chunk_payload).map_err(|err| {
        DatabaseError::Replication(format!("read snapshot fetch chunk body: {err}"))
    })?;
    if reader.position() as usize != payload.len() {
        return Err(DatabaseError::Replication(
            "snapshot fetch chunk has trailing bytes".to_string(),
        ));
    }
    Ok(TcpSnapshotFetchChunk {
        total_len,
        checksum,
        resume_offset,
        snapshot: Some(InstallSnapshotChunk {
            request: InstallSnapshotRequest {
                term,
                leader_id,
                metadata: RaftSnapshotMetadata {
                    shard_id,
                    last_included_term,
                    last_included_index,
                },
                payload: chunk_payload,
            },
            offset,
            done: done[0] != 0,
        }),
    })
}

fn snapshot_checksum(payload: &[u8]) -> u64 {
    payload.iter().fold(0xcbf29ce484222325, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(0x100000001b3)
    })
}

#[allow(dead_code)]
fn decode_snapshot_fetch_payload(payload: &[u8]) -> DatabaseResult<Option<InstallSnapshotRequest>> {
    let Some((&flag, rest)) = payload.split_first() else {
        return Err(DatabaseError::Replication(
            "invalid empty snapshot fetch payload".to_string(),
        ));
    };
    if flag == 0 {
        if rest.is_empty() {
            return Ok(None);
        }
        return Err(DatabaseError::Replication(
            "invalid snapshot fetch none payload".to_string(),
        ));
    }
    if flag != 1 {
        return Err(DatabaseError::Replication(format!(
            "invalid snapshot fetch payload flag {flag}"
        )));
    }
    let mut reader = std::io::Cursor::new(rest);
    let term = read_u64(&mut reader)?;
    let leader_id = read_u64(&mut reader)?;
    let shard_id = read_u64(&mut reader)?;
    let last_included_term = read_u64(&mut reader)?;
    let last_included_index = read_u64(&mut reader)?;
    let payload_len = read_u32(&mut reader)? as usize;
    let mut snapshot_payload = vec![0; payload_len];
    reader.read_exact(&mut snapshot_payload).map_err(|err| {
        DatabaseError::Replication(format!("read snapshot fetch payload body: {err}"))
    })?;
    if reader.position() as usize != rest.len() {
        return Err(DatabaseError::Replication(
            "snapshot fetch payload has trailing bytes".to_string(),
        ));
    }
    Ok(Some(InstallSnapshotRequest {
        term,
        leader_id,
        metadata: RaftSnapshotMetadata {
            shard_id,
            last_included_term,
            last_included_index,
        },
        payload: snapshot_payload,
    }))
}
