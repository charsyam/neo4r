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
