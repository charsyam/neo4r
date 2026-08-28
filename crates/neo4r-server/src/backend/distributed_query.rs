fn execute_distributed_query(
    db: &Neo4rDatabaseHandle,
    query_peers: &QueryPeerStore,
    read_preference: QueryReadPreference,
    query: &str,
    params: &neo4r_query::QueryParams,
) -> Result<Vec<QueryRow>, String> {
    let status = db.cluster_status().map_err(|err| err.to_string())?;
    let mut rows = Vec::new();
    for shard in status.shards {
        if shard.has_local_copy {
            rows.extend(
                db.query_shard_with_params(shard.shard_id, query, params.clone())
                    .map_err(|err| err.to_string())?,
            );
        } else {
            let (_target, address) =
                select_remote_query_target(query_peers, &shard, read_preference)?;
            rows.extend(request_remote_query_shard(
                &address,
                shard.shard_id,
                query,
                params,
            )?);
        }
    }
    Ok(rows)
}

fn build_distributed_query_cursor(
    db: &Neo4rDatabaseHandle,
    query_peers: &QueryPeerStore,
    read_preference: QueryReadPreference,
    query: &str,
    params: &neo4r_query::QueryParams,
) -> Result<Box<dyn QueryCursor>, String> {
    build_distributed_query_cursor_with_options(
        db,
        query_peers,
        read_preference,
        query,
        params,
        QueryOptions::default(),
    )
}

fn build_distributed_query_cursor_with_options(
    db: &Neo4rDatabaseHandle,
    query_peers: &QueryPeerStore,
    read_preference: QueryReadPreference,
    query: &str,
    params: &neo4r_query::QueryParams,
    options: QueryOptions,
) -> Result<Box<dyn QueryCursor>, String> {
    let status = db.cluster_status().map_err(|err| err.to_string())?;
    let mut cursors = Vec::<Box<dyn QueryCursor>>::new();
    for shard in status.shards {
        if shard.has_local_copy {
            let rows = db
                .query_shard_with_params_and_options(shard.shard_id, query, params.clone(), options)
                .map_err(|err| err.to_string())?;
            cursors.push(Box::new(VecQueryCursor::new(rows)));
        } else {
            let (_target, address) =
                select_remote_query_target(query_peers, &shard, read_preference)?;
            cursors.push(Box::new(RemoteShardQueryCursor::open(
                &address,
                shard.shard_id,
                query,
                params,
            )?));
        }
    }
    Ok(Box::new(DistributedQueryCursor::new(cursors)))
}

fn build_distributed_query_cursor_with_local_staged_writes(
    db: &Neo4rDatabaseHandle,
    query_peers: &QueryPeerStore,
    read_preference: QueryReadPreference,
    query: &str,
    params: &neo4r_query::QueryParams,
    options: QueryOptions,
    staged_writes: &[(String, neo4r_query::QueryParams)],
) -> Result<Box<dyn QueryCursor>, String> {
    let status = db.cluster_status().map_err(|err| err.to_string())?;
    let mut cursors = Vec::<Box<dyn QueryCursor>>::new();
    for shard in status.shards {
        if shard.has_local_copy {
            let rows = db
                .query_shard_with_staged_writes(
                    shard.shard_id,
                    query,
                    params.clone(),
                    options,
                    staged_writes,
                )
                .map_err(|err| err.to_string())?;
            cursors.push(Box::new(VecQueryCursor::new(rows)));
        } else {
            let (_target, address) =
                select_remote_query_target(query_peers, &shard, read_preference)?;
            cursors.push(Box::new(RemoteShardQueryCursor::open_with_staged_writes(
                &address,
                shard.shard_id,
                query,
                params,
                staged_writes,
            )?));
        }
    }
    Ok(Box::new(DistributedQueryCursor::new(cursors)))
}

fn build_distributed_read_tx_cursor(
    db: &Neo4rDatabaseHandle,
    query_peers: &QueryPeerStore,
    read_preference: QueryReadPreference,
    tx: &Neo4rReadTransaction,
    query: &str,
    params: &neo4r_query::QueryParams,
) -> Result<Box<dyn QueryCursor>, String> {
    let status = db.cluster_status().map_err(|err| err.to_string())?;
    let mut cursors = Vec::<Box<dyn QueryCursor>>::new();
    for shard in status.shards {
        if shard.has_local_copy {
            let rows = tx
                .query_shard_with_params(shard.shard_id, query, params)
                .map_err(|err| err.to_string())?;
            cursors.push(Box::new(VecQueryCursor::new(rows)));
        } else {
            let (_target, address) =
                select_remote_query_target(query_peers, &shard, read_preference)?;
            cursors.push(Box::new(RemoteShardQueryCursor::open(
                &address,
                shard.shard_id,
                query,
                params,
            )?));
        }
    }
    Ok(Box::new(DistributedQueryCursor::new(cursors)))
}

fn select_remote_query_target(
    query_peers: &QueryPeerStore,
    shard: &neo4r_db::ShardStatus,
    read_preference: QueryReadPreference,
) -> Result<(u64, String), String> {
    if read_preference == QueryReadPreference::PreferReplica {
        for replica in &shard.replica_server_ids {
            if let Some(address) = query_peers.address(*replica)? {
                return Ok((*replica, address));
            }
        }
    }

    let primary = shard
        .primary_server_id
        .ok_or_else(|| format!("missing primary for remote query shard {}", shard.shard_id))?;
    if let Some(address) = query_peers.address(primary)? {
        return Ok((primary, address));
    }

    if read_preference == QueryReadPreference::Primary {
        return Err(format!(
            "missing query peer address for primary server {primary} on shard {}",
            shard.shard_id
        ));
    }

    Err(format!(
        "missing query peer address for preferred replica or primary on shard {}",
        shard.shard_id
    ))
}

struct DistributedQueryCursor {
    cursors: Vec<Box<dyn QueryCursor>>,
    current: usize,
}

impl DistributedQueryCursor {
    fn new(cursors: Vec<Box<dyn QueryCursor>>) -> Self {
        Self {
            cursors,
            current: 0,
        }
    }
}

impl QueryCursor for DistributedQueryCursor {
    fn fetch(&mut self, page_size: usize) -> neo4r_query::QueryPage {
        let page_size = page_size.max(1);
        let mut rows = Vec::new();
        while rows.len() < page_size && self.current < self.cursors.len() {
            let remaining = page_size - rows.len();
            let page = self.cursors[self.current].fetch(remaining);
            rows.extend(page.rows);
            if !page.has_more {
                self.current += 1;
            }
        }
        neo4r_query::QueryPage {
            rows,
            has_more: self.current < self.cursors.len(),
        }
    }

    fn total_rows(&self) -> Option<usize> {
        self.cursors
            .iter()
            .map(|cursor| cursor.total_rows())
            .try_fold(0_usize, |sum, total| total.map(|total| sum + total))
    }
}

struct RemoteShardQueryCursor {
    stream: TcpStream,
    cursor_id: u64,
    buffered_rows: Vec<QueryRow>,
    remote_has_more: bool,
    total_rows: Option<usize>,
    next_request_id: u64,
}

impl RemoteShardQueryCursor {
    fn open(
        address: &str,
        shard_id: u64,
        query: &str,
        params: &neo4r_query::QueryParams,
    ) -> Result<Self, String> {
        Self::open_with_payload(
            address,
            format_query_shard_payload(shard_id, query, params)?,
            "query shard",
        )
    }

    fn open_with_staged_writes(
        address: &str,
        shard_id: u64,
        query: &str,
        params: &neo4r_query::QueryParams,
        staged_writes: &[(String, neo4r_query::QueryParams)],
    ) -> Result<Self, String> {
        Self::open_with_payload(
            address,
            format_query_staged_shard_payload(shard_id, query, params, staged_writes),
            "staged query shard",
        )
    }

    fn open_with_payload(address: &str, payload: String, operation: &str) -> Result<Self, String> {
        let mut stream = TcpStream::connect(address)
            .map_err(|err| format!("connect query peer {address}: {err}"))?;
        write_frame(
            &mut stream,
            &NativeFrame::new(NativeMessageType::Command, 1, payload.into_bytes()),
        )
        .map_err(|err| format!("write remote {operation} cursor request: {err}"))?;
        let frame = read_frame(&mut stream)
            .map_err(|err| format!("read remote {operation} cursor response: {err}"))?
            .ok_or_else(|| format!("remote query peer closed without {operation} response"))?;
        if frame.message_type != NativeMessageType::Response {
            return Err(format!(
                "remote {operation} cursor failed: {}",
                frame.payload_text().map_err(|err| err.to_string())?
            ));
        }
        let start =
            parse_result_start_response(frame.payload_text().map_err(|err| err.to_string())?)?;
        Ok(Self {
            stream,
            cursor_id: start.cursor_id,
            buffered_rows: start.rows,
            remote_has_more: start.has_more,
            total_rows: start.total_rows,
            next_request_id: 2,
        })
    }

    fn fetch_remote(&mut self, page_size: usize) -> Result<(), String> {
        if !self.remote_has_more {
            return Ok(());
        }
        let request_id = self.next_request_id;
        self.next_request_id += 1;
        write_frame(
            &mut self.stream,
            &NativeFrame::new(
                NativeMessageType::Fetch,
                request_id,
                format!("{}\t{page_size}", self.cursor_id).into_bytes(),
            ),
        )
        .map_err(|err| format!("write remote fetch request: {err}"))?;
        let frame = read_frame(&mut self.stream)
            .map_err(|err| format!("read remote fetch response: {err}"))?
            .ok_or_else(|| "remote query peer closed without fetch response".to_string())?;
        if frame.message_type != NativeMessageType::Response {
            self.remote_has_more = false;
            return Err(format!(
                "remote fetch failed: {}",
                frame.payload_text().map_err(|err| err.to_string())?
            ));
        }
        let page = parse_result_page_response(frame.payload_text().unwrap_or(""))?;
        self.buffered_rows.extend(page.rows);
        self.remote_has_more = page.has_more;
        Ok(())
    }
}

impl QueryCursor for RemoteShardQueryCursor {
    fn fetch(&mut self, page_size: usize) -> neo4r_query::QueryPage {
        let page_size = page_size.max(1);
        while self.buffered_rows.len() < page_size && self.remote_has_more {
            if self
                .fetch_remote(page_size.saturating_sub(self.buffered_rows.len()))
                .is_err()
            {
                self.remote_has_more = false;
                break;
            }
        }
        let take = page_size.min(self.buffered_rows.len());
        let rows = self.buffered_rows.drain(..take).collect::<Vec<_>>();
        neo4r_query::QueryPage {
            rows,
            has_more: !self.buffered_rows.is_empty() || self.remote_has_more,
        }
    }

    fn total_rows(&self) -> Option<usize> {
        self.total_rows
    }
}

struct ResultStart {
    cursor_id: u64,
    total_rows: Option<usize>,
    rows: Vec<QueryRow>,
    has_more: bool,
}

struct RemoteResultPage {
    rows: Vec<QueryRow>,
    has_more: bool,
}

fn parse_result_start_response(payload: &str) -> Result<ResultStart, String> {
    let parts = payload.splitn(7, '\t').collect::<Vec<_>>();
    if parts.len() != 7 || parts[0] != "OK" || parts[1] != "RESULT_START" {
        return Err(format!(
            "remote query returned non-cursor response: {payload}"
        ));
    }
    let row_count = parts[4]
        .parse::<usize>()
        .map_err(|_| "RESULT_START row count must be an unsigned integer".to_string())?;
    let rows = decode_query_rows(parts[6])?;
    if rows.len() != row_count {
        return Err(format!(
            "RESULT_START row count mismatch: header {row_count}, decoded {}",
            rows.len()
        ));
    }
    Ok(ResultStart {
        cursor_id: parse_cursor_id(parts[2])?,
        total_rows: if parts[3] == "UNKNOWN" {
            None
        } else {
            Some(
                parts[3].parse::<usize>().map_err(|_| {
                    "RESULT_START total rows must be an unsigned integer".to_string()
                })?,
            )
        },
        rows,
        has_more: parse_bool_token(parts[5], "RESULT_START has_more")?,
    })
}

fn parse_result_page_response(payload: &str) -> Result<RemoteResultPage, String> {
    let parts = payload.splitn(6, '\t').collect::<Vec<_>>();
    if parts.len() != 6 || parts[0] != "OK" || parts[1] != "RESULT_PAGE" {
        return Err(format!(
            "remote query returned non-page response: {payload}"
        ));
    }
    let row_count = parts[3]
        .parse::<usize>()
        .map_err(|_| "RESULT_PAGE row count must be an unsigned integer".to_string())?;
    let rows = decode_query_rows(parts[5])?;
    if rows.len() != row_count {
        return Err(format!(
            "RESULT_PAGE row count mismatch: header {row_count}, decoded {}",
            rows.len()
        ));
    }
    Ok(RemoteResultPage {
        rows,
        has_more: parse_bool_token(parts[4], "RESULT_PAGE has_more")?,
    })
}

fn parse_bool_token(input: &str, name: &str) -> Result<bool, String> {
    match input {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => Err(format!("{name} must be true or false")),
    }
}

fn request_remote_query_shard(
    address: &str,
    shard_id: u64,
    query: &str,
    params: &neo4r_query::QueryParams,
) -> Result<Vec<QueryRow>, String> {
    let mut cursor = RemoteShardQueryCursor::open(address, shard_id, query, params)?;
    let mut rows = Vec::new();
    loop {
        let page = cursor.fetch(1024);
        rows.extend(page.rows);
        if !page.has_more {
            break;
        }
    }
    Ok(rows)
}

fn request_remote_command(address: &str, payload: &str) -> Result<String, String> {
    let mut stream = TcpStream::connect(address)
        .map_err(|err| format!("connect write peer {address}: {err}"))?;
    request_command_on_stream(&mut stream, 1, payload)
}

fn request_command_on_stream(
    stream: &mut TcpStream,
    request_id: u64,
    payload: &str,
) -> Result<String, String> {
    write_frame(
        stream,
        &NativeFrame::new(
            NativeMessageType::Command,
            request_id,
            payload.as_bytes().to_vec(),
        ),
    )
    .map_err(|err| format!("write remote command request: {err}"))?;
    let frame = read_frame(stream)
        .map_err(|err| format!("read remote command response: {err}"))?
        .ok_or_else(|| "remote write peer closed without command response".to_string())?;
    let response = frame.payload_text().map_err(|err| err.to_string())?;
    match frame.message_type {
        NativeMessageType::Response => Ok(response.to_string()),
        NativeMessageType::Error => Err(response
            .strip_prefix("ERR\t")
            .unwrap_or(response)
            .to_string()),
        other => Err(format!(
            "remote command returned unexpected frame {other:?}"
        )),
    }
}
