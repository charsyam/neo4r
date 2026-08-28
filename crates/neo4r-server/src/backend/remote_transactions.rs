use super::*;
pub(crate) fn request_remote_prepare_commit_batch(
    db: &Neo4rDatabaseHandle,
    tx_id: u64,
    address: &str,
    shard_id: u64,
    writes: &[(String, neo4r_query::QueryParams)],
) -> Result<(), String> {
    let mut stream = TcpStream::connect(address)
        .map_err(|err| format!("connect write peer {address}: {err}"))?;
    let prepared = request_command_on_stream(
        &mut stream,
        1,
        &format_tx_prepare_write_batch_shard_payload(shard_id, writes),
    )?;
    let prepared_id = parse_tx_prepared_response(&prepared)?;
    record_commit_decision(
        db,
        tx_id,
        vec![TransactionParticipantRecord {
            location: format!("remote:{address}"),
            shard_id,
            prepared_id,
        }],
    )?;
    if let Err(err) = request_command_on_stream(
        &mut stream,
        2,
        &format!("TX_COMMIT_PREPARED\t{prepared_id}"),
    ) {
        return Err(err);
    }
    let _ = clear_transaction_decision(db, tx_id);
    Ok(())
}

pub(crate) fn request_remote_commit_prepared(
    address: &str,
    prepared_id: u64,
) -> Result<(), String> {
    match request_remote_command(address, &format!("TX_COMMIT_PREPARED\t{prepared_id}")) {
        Ok(_) => Ok(()),
        Err(err) if err.contains("unknown prepared transaction") => Ok(()),
        Err(err) => Err(err),
    }
}

pub(crate) fn request_remote_abort_prepared(address: &str, prepared_id: u64) -> Result<(), String> {
    match request_remote_command(address, &format!("TX_ABORT_PREPARED\t{prepared_id}")) {
        Ok(_) => Ok(()),
        Err(err) if err.contains("unknown prepared transaction") => Ok(()),
        Err(err) => Err(err),
    }
}

pub(crate) fn recover_transaction_decisions(
    db: &Neo4rDatabaseHandle,
    prepared_transactions: &PreparedTransactionStore,
) -> Result<usize, String> {
    let data_dir = db.data_dir().map_err(|err| err.to_string())?;
    let store = TransactionDecisionStore::open(data_dir).map_err(|err| err.to_string())?;
    let decisions = store.load().map_err(|err| err.to_string())?;
    let mut completed_tx_ids = BTreeSet::new();
    for decision in &decisions {
        if let Err(err) = apply_transaction_decision(db, prepared_transactions, decision) {
            store
                .remove_tx_ids(&completed_tx_ids)
                .map_err(|err| err.to_string())?;
            return Err(err);
        }
        completed_tx_ids.insert(decision.tx_id);
    }
    store
        .remove_tx_ids(&completed_tx_ids)
        .map_err(|err| err.to_string())?;
    Ok(decisions.len())
}

pub(crate) fn list_transaction_decisions(
    db: &Neo4rDatabaseHandle,
) -> Result<Vec<TransactionDecisionRecord>, String> {
    let data_dir = db.data_dir().map_err(|err| err.to_string())?;
    TransactionDecisionStore::open(data_dir)
        .and_then(|store| store.load())
        .map_err(|err| err.to_string())
}

pub(crate) fn apply_transaction_decision(
    db: &Neo4rDatabaseHandle,
    prepared_transactions: &PreparedTransactionStore,
    decision: &TransactionDecisionRecord,
) -> Result<(), String> {
    match decision.decision {
        TransactionDecision::Commit => {
            for participant in &decision.participants {
                commit_decision_participant(db, prepared_transactions, participant)?;
            }
        }
        TransactionDecision::Abort => {
            for participant in &decision.participants {
                abort_decision_participant(prepared_transactions, participant)?;
            }
        }
    }
    Ok(())
}

pub(crate) fn commit_decision_participant(
    db: &Neo4rDatabaseHandle,
    prepared_transactions: &PreparedTransactionStore,
    participant: &TransactionParticipantRecord,
) -> Result<(), String> {
    if participant.location == "local" {
        let prepared = match prepared_transactions.take(participant.prepared_id) {
            Ok(prepared) => prepared,
            Err(err) if err.contains("unknown prepared transaction") => return Ok(()),
            Err(err) => return Err(err),
        };
        db.execute_staged_cypher_transaction_on_shard(prepared.shard_id, prepared.writes)
            .map(|_| ())
            .map_err(|err| err.to_string())
    } else if let Some(address) = participant.location.strip_prefix("remote:") {
        request_remote_commit_prepared(address, participant.prepared_id)
    } else {
        Err(format!(
            "unknown transaction participant location: {}",
            participant.location
        ))
    }
}

pub(crate) fn abort_decision_participant(
    prepared_transactions: &PreparedTransactionStore,
    participant: &TransactionParticipantRecord,
) -> Result<(), String> {
    if participant.location == "local" {
        let _ = prepared_transactions.take(participant.prepared_id);
        Ok(())
    } else if let Some(address) = participant.location.strip_prefix("remote:") {
        request_remote_abort_prepared(address, participant.prepared_id)
    } else {
        Err(format!(
            "unknown transaction participant location: {}",
            participant.location
        ))
    }
}

pub(crate) fn request_remote_prepare_commit_batches(
    db: &Neo4rDatabaseHandle,
    tx_id: u64,
    participants: Vec<(String, u64, Vec<(String, neo4r_query::QueryParams)>)>,
) -> Result<(), String> {
    let mut prepared = Vec::new();
    for (address, shard_id, writes) in participants {
        let mut stream = TcpStream::connect(&address)
            .map_err(|err| format!("connect write peer {address}: {err}"))?;
        let response = match request_command_on_stream(
            &mut stream,
            1,
            &format_tx_prepare_write_batch_shard_payload(shard_id, &writes),
        ) {
            Ok(response) => response,
            Err(err) => {
                record_abort_decision(db, tx_id, remote_decision_participant_records(&prepared))?;
                abort_prepared_participants(prepared);
                return Err(err);
            }
        };
        let prepared_id = match parse_tx_prepared_response(&response) {
            Ok(prepared_id) => prepared_id,
            Err(err) => {
                record_abort_decision(db, tx_id, remote_decision_participant_records(&prepared))?;
                abort_prepared_participants(prepared);
                return Err(err);
            }
        };
        prepared.push(RemotePreparedParticipant {
            stream,
            address,
            shard_id,
            prepared_id,
        });
    }

    record_commit_decision(
        db,
        tx_id,
        prepared
            .iter()
            .map(|participant| TransactionParticipantRecord {
                location: format!("remote:{}", participant.address),
                shard_id: participant.shard_id,
                prepared_id: participant.prepared_id,
            })
            .collect(),
    )?;

    while !prepared.is_empty() {
        let mut participant = prepared.remove(0);
        if let Err(err) = request_command_on_stream(
            &mut participant.stream,
            2,
            &format!("TX_COMMIT_PREPARED\t{}", participant.prepared_id),
        ) {
            return Err(format!(
                "commit prepared transaction {} on {} failed: {err}",
                participant.prepared_id, participant.address
            ));
        }
    }
    let _ = clear_transaction_decision(db, tx_id);
    Ok(())
}

pub(crate) fn abort_prepared_participants(participants: Vec<RemotePreparedParticipant>) {
    for mut participant in participants {
        let _ = request_command_on_stream(
            &mut participant.stream,
            3,
            &format!("TX_ABORT_PREPARED\t{}", participant.prepared_id),
        );
    }
}

pub(crate) struct RemotePreparedParticipant {
    pub(crate) stream: TcpStream,
    pub(crate) address: String,
    pub(crate) shard_id: u64,
    pub(crate) prepared_id: u64,
}

pub(crate) fn record_commit_decision(
    db: &Neo4rDatabaseHandle,
    tx_id: u64,
    participants: Vec<TransactionParticipantRecord>,
) -> Result<(), String> {
    record_transaction_decision(db, tx_id, TransactionDecision::Commit, participants)
}

pub(crate) fn record_abort_decision(
    db: &Neo4rDatabaseHandle,
    tx_id: u64,
    participants: Vec<TransactionParticipantRecord>,
) -> Result<(), String> {
    record_transaction_decision(db, tx_id, TransactionDecision::Abort, participants)
}

pub(crate) fn record_transaction_decision(
    db: &Neo4rDatabaseHandle,
    tx_id: u64,
    decision: TransactionDecision,
    participants: Vec<TransactionParticipantRecord>,
) -> Result<(), String> {
    if participants.is_empty() {
        return Ok(());
    }
    let data_dir = db.data_dir().map_err(|err| err.to_string())?;
    TransactionDecisionStore::open(data_dir)
        .and_then(|store| {
            store.append(&TransactionDecisionRecord {
                tx_id,
                decision,
                participants,
            })
        })
        .map_err(|err| err.to_string())
}

pub(crate) fn clear_transaction_decision(
    db: &Neo4rDatabaseHandle,
    tx_id: u64,
) -> Result<(), String> {
    let data_dir = db.data_dir().map_err(|err| err.to_string())?;
    TransactionDecisionStore::open(data_dir)
        .and_then(|store| store.remove_tx_ids(&BTreeSet::from([tx_id])).map(|_| ()))
        .map_err(|err| err.to_string())
}

pub(crate) fn decision_participant_records(
    prepared_locals: &[(u64, u64)],
    prepared_remotes: &[RemotePreparedParticipant],
) -> Vec<TransactionParticipantRecord> {
    prepared_locals
        .iter()
        .map(|(shard_id, prepared_id)| TransactionParticipantRecord {
            location: "local".to_string(),
            shard_id: *shard_id,
            prepared_id: *prepared_id,
        })
        .chain(remote_decision_participant_records(prepared_remotes))
        .collect()
}

pub(crate) fn remote_decision_participant_records(
    prepared_remotes: &[RemotePreparedParticipant],
) -> Vec<TransactionParticipantRecord> {
    prepared_remotes
        .iter()
        .map(|participant| TransactionParticipantRecord {
            location: format!("remote:{}", participant.address),
            shard_id: participant.shard_id,
            prepared_id: participant.prepared_id,
        })
        .collect()
}

pub(crate) fn parse_ok_rows_response(payload: &str) -> Result<Vec<QueryRow>, String> {
    if payload.starts_with("OK\tROWS\t") {
        let parts = payload.splitn(4, '\t').collect::<Vec<_>>();
        if parts.len() != 4 {
            return Err(format!(
                "remote write returned malformed rows response: {payload}"
            ));
        }
        return decode_query_rows(parts[3]);
    }
    let start = parse_result_start_response(payload)?;
    if start.has_more {
        return Err("remote write returned more rows than a single response page".to_string());
    }
    Ok(start.rows)
}

pub(crate) fn parse_tx_prepared_response(payload: &str) -> Result<u64, String> {
    let parts = payload.split('\t').collect::<Vec<_>>();
    if parts.len() >= 3 && parts[0] == "OK" && parts[1] == "TX_PREPARED" {
        parse_cursor_id(parts[2])
    } else {
        Err(format!(
            "remote prepare returned malformed response: {payload}"
        ))
    }
}

pub(crate) fn parse_ok_index_catalog_response(
    payload: &str,
) -> Result<neo4r_db::IndexCatalog, String> {
    let parts = payload.splitn(3, '\t').collect::<Vec<_>>();
    if parts.len() != 3 || parts[0] != "OK" || parts[1] != "INDEX_CATALOG" {
        return Err(format!(
            "remote catalog returned malformed response: {payload}"
        ));
    }
    decode_index_catalog(parts[2])
}

pub(crate) fn select_create_node_write_shard<'a>(
    status: &'a neo4r_db::ClusterStatus,
    query: &str,
    params: &neo4r_query::QueryParams,
) -> Result<&'a neo4r_db::ShardStatus, String> {
    if status.shards.is_empty() {
        return Err("cluster status has no shards".to_string());
    }
    let hash = stable_create_node_hash(query, params);
    let index = (hash % status.shards.len() as u64) as usize;
    status
        .shards
        .get(index)
        .ok_or_else(|| "cluster status has no shards".to_string())
}

pub(crate) fn select_merge_node_write_shard<'a>(
    status: &'a neo4r_db::ClusterStatus,
    query: &str,
    params: &neo4r_query::QueryParams,
) -> Result<&'a neo4r_db::ShardStatus, String> {
    if status.shards.is_empty() {
        return Err("cluster status has no shards".to_string());
    }
    let hash = stable_merge_node_hash(query, params);
    let index = (hash % status.shards.len() as u64) as usize;
    status
        .shards
        .get(index)
        .ok_or_else(|| "cluster status has no shards".to_string())
}

pub(crate) fn stable_create_node_hash(query: &str, params: &neo4r_query::QueryParams) -> u64 {
    let routing_key = match create_node_routing_key(query, params) {
        Ok(Some(key)) => key,
        Ok(None) | Err(_) => {
            return stable_create_node_fallback_hash(query, params);
        }
    };
    stable_create_node_routing_key_hash(&routing_key)
}

pub(crate) fn stable_merge_node_hash(query: &str, params: &neo4r_query::QueryParams) -> u64 {
    let routing_key = match merge_node_routing_key(query, params) {
        Ok(Some(key)) => key,
        Ok(None) | Err(_) => {
            return stable_create_node_fallback_hash(query, params);
        }
    };
    stable_create_node_routing_key_hash(&routing_key)
}

pub(crate) fn stable_create_node_routing_key_hash(routing_key: &CreateNodeRoutingKey) -> u64 {
    const FNV_OFFSET: u64 = 0xcbf29ce484222325;
    const FNV_PRIME: u64 = 0x100000001b3;

    pub(crate) fn update(mut hash: u64, bytes: &[u8]) -> u64 {
        for byte in bytes {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(FNV_PRIME);
        }
        hash
    }

    let mut hash = FNV_OFFSET;
    let mut labels = routing_key.labels.iter().collect::<Vec<_>>();
    labels.sort();
    for label in labels {
        hash = update(hash, b"\0");
        hash = update(hash, b"label:");
        hash = update(hash, label.as_bytes());
    }
    let mut property_keys = routing_key.properties.keys().collect::<Vec<_>>();
    property_keys.sort();
    for key in property_keys {
        hash = update(hash, b"\0");
        hash = update(hash, b"prop:");
        hash = update(hash, key.as_bytes());
        hash = update(hash, b"=");
        hash = update(
            hash,
            format_value_for_request(&routing_key.properties[key]).as_bytes(),
        );
    }
    hash
}

pub(crate) fn stable_create_node_fallback_hash(
    query: &str,
    params: &neo4r_query::QueryParams,
) -> u64 {
    const FNV_OFFSET: u64 = 0xcbf29ce484222325;
    const FNV_PRIME: u64 = 0x100000001b3;

    pub(crate) fn update(mut hash: u64, bytes: &[u8]) -> u64 {
        for byte in bytes {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(FNV_PRIME);
        }
        hash
    }

    let mut hash = update(FNV_OFFSET, query.trim().as_bytes());
    let mut keys = params.keys().collect::<Vec<_>>();
    keys.sort();
    for key in keys {
        hash = update(hash, b"\0");
        hash = update(hash, key.as_bytes());
        hash = update(hash, b"=");
        hash = update(hash, format_value_for_request(&params[key]).as_bytes());
    }
    hash
}

pub(crate) fn write_request_shard(
    db: &Neo4rDatabaseHandle,
    request: &BackendRequest,
    shard_count: u64,
) -> Result<Option<u64>, String> {
    if shard_count == 0 {
        return Ok(None);
    }
    let shard_id = match request {
        BackendRequest::CreateNodeOnShard { shard_id, .. } => Some(*shard_id),
        BackendRequest::CreateRelationship { from, .. } => Some(from % shard_count),
        BackendRequest::SetNodeProperty { id, .. }
        | BackendRequest::RemoveNodeProperty { id, .. }
        | BackendRequest::AddNodeLabel { id, .. }
        | BackendRequest::RemoveNodeLabel { id, .. }
        | BackendRequest::DeleteNode(id) => Some(id % shard_count),
        BackendRequest::SetRelationshipProperty { id, .. }
        | BackendRequest::RemoveRelationshipProperty { id, .. }
        | BackendRequest::DeleteRelationship(id) => Some(
            db.relationship_owner_shard(*id)
                .map_err(|err| err.to_string())?,
        ),
        BackendRequest::CreateIndex { .. }
        | BackendRequest::CreateUniqueConstraint { .. }
        | BackendRequest::CreateVectorIndex { .. }
        | BackendRequest::RebuildVectorIndex { .. }
        | BackendRequest::RebuildVectorIndexes
        | BackendRequest::DropIndex { .. }
        | BackendRequest::DropConstraint { .. } => Some(0),
        _ => None,
    };
    Ok(shard_id)
}

pub(crate) fn format_command_request_payload(request: &BackendRequest) -> Result<String, String> {
    match request {
        BackendRequest::CreateNodeOnShard {
            shard_id,
            labels,
            properties,
        } => Ok(format!(
            "CREATE_NODE_SHARD\t{shard_id}\t{}{}",
            labels.join(","),
            format_properties_suffix(properties)
        )),
        BackendRequest::CreateRelationship {
            from,
            to,
            rel_type,
            properties,
        } => Ok(format!(
            "CREATE_RELATIONSHIP\t{from}\t{to}\t{rel_type}{}",
            format_properties_suffix(properties)
        )),
        BackendRequest::SetNodeProperty { id, key, value } => Ok(format!(
            "SET_NODE_PROPERTY\t{id}\t{key}\t{}",
            format_value_for_request(value)
        )),
        BackendRequest::RemoveNodeProperty { id, key } => {
            Ok(format!("REMOVE_NODE_PROPERTY\t{id}\t{key}"))
        }
        BackendRequest::AddNodeLabel { id, label } => Ok(format!("ADD_NODE_LABEL\t{id}\t{label}")),
        BackendRequest::RemoveNodeLabel { id, label } => {
            Ok(format!("REMOVE_NODE_LABEL\t{id}\t{label}"))
        }
        BackendRequest::SetRelationshipProperty { id, key, value } => Ok(format!(
            "SET_RELATIONSHIP_PROPERTY\t{id}\t{key}\t{}",
            format_value_for_request(value)
        )),
        BackendRequest::RemoveRelationshipProperty { id, key } => {
            Ok(format!("REMOVE_RELATIONSHIP_PROPERTY\t{id}\t{key}"))
        }
        BackendRequest::DeleteNode(id) => Ok(format!("DELETE_NODE\t{id}")),
        BackendRequest::DeleteRelationship(id) => Ok(format!("DELETE_RELATIONSHIP\t{id}")),
        BackendRequest::CreateIndex {
            name,
            label,
            property,
            if_not_exists,
        } => Ok(format!(
            "CREATE_INDEX\t{name}\t{label}\t{property}{}",
            format_if_not_exists_suffix(*if_not_exists)
        )),
        BackendRequest::CreateUniqueConstraint {
            name,
            label,
            property,
            if_not_exists,
        } => Ok(format!(
            "CREATE_CONSTRAINT\t{name}\t{label}\t{property}{}",
            format_if_not_exists_suffix(*if_not_exists)
        )),
        BackendRequest::CreateVectorIndex {
            name,
            label,
            property,
            dimensions,
            metric,
            if_not_exists,
        } => Ok(format!(
            "CREATE_VECTOR_INDEX\t{name}\t{label}\t{property}\t{dimensions}\t{metric}{}",
            format_if_not_exists_suffix(*if_not_exists)
        )),
        BackendRequest::RebuildVectorIndex { name } => Ok(format!("REBUILD_VECTOR_INDEX\t{name}")),
        BackendRequest::RebuildVectorIndexes => Ok("REBUILD_VECTOR_INDEXES".to_string()),
        BackendRequest::DropIndex { name, if_exists } => Ok(format!(
            "DROP_INDEX\t{name}{}",
            format_if_exists_suffix(*if_exists)
        )),
        BackendRequest::DropConstraint { name, if_exists } => Ok(format!(
            "DROP_CONSTRAINT\t{name}{}",
            format_if_exists_suffix(*if_exists)
        )),
        _ => Err("request is not a forwardable write command".to_string()),
    }
}

pub(crate) fn format_if_not_exists_suffix(if_not_exists: bool) -> &'static str {
    if if_not_exists {
        "\tIF_NOT_EXISTS"
    } else {
        ""
    }
}

pub(crate) fn format_if_exists_suffix(if_exists: bool) -> &'static str {
    if if_exists {
        "\tIF_EXISTS"
    } else {
        ""
    }
}

pub(crate) fn format_properties_suffix(properties: &neo4r_core::Properties) -> String {
    let mut keys = properties.keys().collect::<Vec<_>>();
    keys.sort();
    let mut suffix = String::new();
    for key in keys {
        if let Some(value) = properties.get(key) {
            suffix.push('\t');
            suffix.push_str(key);
            suffix.push('=');
            suffix.push_str(&format_value_for_request(value));
        }
    }
    suffix
}

pub(crate) fn format_query_write_shard_payload(
    shard_id: u64,
    query: &str,
    params: &neo4r_query::QueryParams,
) -> Result<String, String> {
    format_query_payload_with_command("QUERY_WRITE_SHARD", shard_id, query, params)
}

pub(crate) fn format_tx_prepare_write_batch_shard_payload(
    shard_id: u64,
    writes: &[(String, neo4r_query::QueryParams)],
) -> String {
    format!(
        "TX_PREPARE_WRITE_BATCH_SHARD\t{shard_id}\t{}",
        encode_query_batch_payload(writes)
    )
}

pub(crate) fn format_query_payload_with_command(
    command: &str,
    shard_id: u64,
    query: &str,
    params: &neo4r_query::QueryParams,
) -> Result<String, String> {
    let mut payload = format!("{command}\t{shard_id}\t{query}");
    let mut keys = params.keys().collect::<Vec<_>>();
    keys.sort();
    for key in keys {
        payload.push('\t');
        payload.push_str(key);
        payload.push('=');
        payload.push_str(&format_value_for_request(
            params
                .get(key)
                .ok_or_else(|| format!("missing query parameter: {key}"))?,
        ));
    }
    Ok(payload)
}

pub(crate) fn format_query_shard_payload(
    shard_id: u64,
    query: &str,
    params: &neo4r_query::QueryParams,
) -> Result<String, String> {
    format_query_payload_with_command("QUERY_SHARD", shard_id, query, params)
}

pub(crate) fn format_query_staged_shard_payload(
    shard_id: u64,
    query: &str,
    params: &neo4r_query::QueryParams,
    staged_writes: &[(String, neo4r_query::QueryParams)],
) -> String {
    let mut batch = Vec::with_capacity(staged_writes.len() + 1);
    batch.push((query.to_string(), params.clone()));
    batch.extend(staged_writes.iter().cloned());
    format!(
        "QUERY_STAGED_SHARD\t{shard_id}\t{}",
        encode_query_batch_payload(&batch)
    )
}

pub(crate) fn format_value_for_request(value: &neo4r_core::Value) -> String {
    match value {
        neo4r_core::Value::Null => "n:".to_string(),
        neo4r_core::Value::Bool(value) => format!("b:{value}"),
        neo4r_core::Value::Int(value) => format!("i:{value}"),
        neo4r_core::Value::Float(value) => format!("f:{value}"),
        neo4r_core::Value::String(value) => format!("s:{value}"),
        neo4r_core::Value::Vector(values) => {
            let values = values
                .iter()
                .map(|value| value.to_string())
                .collect::<Vec<_>>()
                .join(",");
            format!("v:{values}")
        }
        neo4r_core::Value::Map(values) => {
            let encoded = encode_map_for_request(values);
            format!("m:{}", hex_encode_for_request(encoded.as_bytes()))
        }
    }
}

pub(crate) fn encode_map_for_request(values: &neo4r_core::Properties) -> String {
    let mut entries = values.iter().collect::<Vec<_>>();
    entries.sort_by(|(left, _), (right, _)| left.cmp(right));
    entries
        .into_iter()
        .map(|(key, value)| {
            format!(
                "{}~{}",
                hex_encode_for_request(key.as_bytes()),
                encode_value_for_map_request(value)
            )
        })
        .collect::<Vec<_>>()
        .join(",")
}

pub(crate) fn encode_value_for_map_request(value: &neo4r_core::Value) -> String {
    match value {
        neo4r_core::Value::Null => "n".to_string(),
        neo4r_core::Value::Bool(value) => format!("b:{}", u8::from(*value)),
        neo4r_core::Value::Int(value) => format!("i:{value}"),
        neo4r_core::Value::Float(value) => format!("f:{}", value.to_bits()),
        neo4r_core::Value::String(value) => {
            format!("s:{}", hex_encode_for_request(value.as_bytes()))
        }
        neo4r_core::Value::Vector(values) => format!(
            "v:{}",
            values
                .iter()
                .map(|value| value.to_bits().to_string())
                .collect::<Vec<_>>()
                .join(",")
        ),
        neo4r_core::Value::Map(values) => {
            let encoded = encode_map_for_request(values);
            format!("m:{}", hex_encode_for_request(encoded.as_bytes()))
        }
    }
}

pub(crate) fn hex_encode_for_request(input: impl AsRef<[u8]>) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let input = input.as_ref();
    let mut output = String::with_capacity(input.len() * 2);
    for byte in input {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

pub(crate) fn is_write_cypher(query: &str) -> bool {
    let upper = query.trim().to_ascii_uppercase();
    upper.starts_with("CREATE INDEX ")
        || upper.starts_with("CREATE VECTOR INDEX ")
        || upper.starts_with("CREATE CONSTRAINT ")
        || upper.starts_with("DROP INDEX ")
        || upper.starts_with("DROP CONSTRAINT ")
        || upper.starts_with("CREATE ")
        || upper.starts_with("MERGE ")
        || (upper.starts_with("MATCH ")
            && (upper.contains(" CREATE ")
                || upper.contains(" MERGE ")
                || upper.contains(" SET ")
                || upper.contains(" REMOVE ")
                || upper.contains(" DELETE ")))
}

pub(crate) fn is_schema_cypher(query: &str) -> bool {
    let upper = query.trim().to_ascii_uppercase();
    upper.starts_with("CREATE INDEX ")
        || upper.starts_with("CREATE VECTOR INDEX ")
        || upper.starts_with("CREATE CONSTRAINT ")
        || upper.starts_with("DROP INDEX ")
        || upper.starts_with("DROP CONSTRAINT ")
}

pub(crate) fn is_create_node_cypher(query: &str) -> bool {
    let upper = query.trim().to_ascii_uppercase();
    upper.starts_with("CREATE ")
        && !upper.starts_with("CREATE INDEX ")
        && !upper.starts_with("CREATE VECTOR INDEX ")
        && !upper.starts_with("CREATE CONSTRAINT ")
}

pub(crate) fn is_merge_node_cypher(query: &str) -> bool {
    let upper = query.trim().to_ascii_uppercase();
    upper.starts_with("MERGE ")
}

pub(crate) fn is_delete_cypher(query: &str) -> bool {
    let upper = query.trim().to_ascii_uppercase();
    upper.starts_with("MATCH ") && upper.contains(" DELETE ")
}

pub(crate) fn is_batchable_transaction_set_cypher(query: &str) -> bool {
    let input = query.trim();
    let upper = input.to_ascii_uppercase();
    if !upper.starts_with("MATCH ")
        || (!upper.contains(" SET ") && !upper.contains(" REMOVE "))
        || upper.contains(" CREATE ")
        || upper.contains(" DELETE ")
    {
        return false;
    }
    true
}

pub(crate) fn is_batchable_multi_target_transaction_cypher(query: &str) -> bool {
    is_batchable_transaction_set_cypher(query) || is_delete_cypher(query)
}

pub(crate) fn staged_writes_are_prepare_batchable(staged_writes: &[StagedWrite]) -> bool {
    staged_writes
        .iter()
        .all(|staged| is_batchable_cypher_mutation(&staged.query))
}

pub(crate) fn is_batchable_cypher_mutation(query: &str) -> bool {
    let upper = query.trim().to_ascii_uppercase();
    if upper.starts_with("CREATE INDEX ")
        || upper.starts_with("CREATE VECTOR INDEX ")
        || upper.starts_with("CREATE CONSTRAINT ")
        || upper.starts_with("DROP INDEX ")
        || upper.starts_with("DROP CONSTRAINT ")
    {
        return false;
    }
    upper.starts_with("CREATE ")
        || is_merge_node_cypher(query)
        || (upper.starts_with("MATCH ")
            && (upper.contains(" CREATE ")
                || upper.contains(" MERGE ")
                || upper.contains(" SET ")
                || upper.contains(" REMOVE ")
                || upper.contains(" DELETE ")))
}

pub(crate) fn is_staged_transaction_overlay_cypher(query: &str) -> bool {
    let upper = query.trim().to_ascii_uppercase();
    if upper.starts_with("CREATE INDEX ")
        || upper.starts_with("CREATE VECTOR INDEX ")
        || upper.starts_with("CREATE CONSTRAINT ")
        || upper.starts_with("DROP INDEX ")
        || upper.starts_with("DROP CONSTRAINT ")
    {
        return false;
    }
    upper.starts_with("CREATE ")
        || upper.starts_with("MERGE ")
        || (upper.starts_with("MATCH ")
            && (upper.contains(" CREATE ")
                || upper.contains(" MERGE ")
                || upper.contains(" SET ")
                || upper.contains(" REMOVE ")
                || upper.contains(" DELETE ")))
}
