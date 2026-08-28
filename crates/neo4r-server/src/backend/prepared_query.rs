use super::*;
pub(crate) enum NativeTransaction {
    ReadOnly(Neo4rReadTransaction),
    ReadWrite {
        isolation: ReadIsolation,
        staged_writes: Vec<StagedWrite>,
        conflict_keys: BTreeSet<String>,
    },
}

impl NativeTransaction {
    pub(crate) fn mode(&self) -> TransactionMode {
        match self {
            Self::ReadOnly(_) => TransactionMode::ReadOnly,
            Self::ReadWrite { .. } => TransactionMode::ReadWrite,
        }
    }

    pub(crate) fn staged_write_count(&self) -> usize {
        match self {
            Self::ReadOnly(_) => 0,
            Self::ReadWrite { staged_writes, .. } => staged_writes.len(),
        }
    }

    pub(crate) fn isolation(&self) -> ReadIsolation {
        match self {
            Self::ReadOnly(tx) => tx.options().isolation,
            Self::ReadWrite { isolation, .. } => *isolation,
        }
    }
}

pub(crate) struct TransactionInfo {
    pub(crate) session_id: u64,
    pub(crate) tx_id: u64,
    pub(crate) mode: TransactionMode,
    pub(crate) isolation: ReadIsolation,
    pub(crate) staged_writes: usize,
}

#[derive(Debug)]
pub(crate) struct PreparedTransactionInfo {
    pub(crate) prepared_id: u64,
    pub(crate) shard_id: u64,
    pub(crate) write_count: usize,
}

pub(crate) struct TransactionPlanContext {
    pub(crate) mode: TransactionMode,
    pub(crate) isolation: ReadIsolation,
    pub(crate) staged_writes: usize,
}

pub(crate) fn format_transaction_plan_context(context: &TransactionPlanContext) -> String {
    let staged_overlay = if context.staged_writes == 0 {
        "none"
    } else {
        "pending"
    };
    format!(
        "tx_mode={} tx_isolation={} staged_writes={} staged_overlay={}",
        format_transaction_mode(context.mode),
        format_read_isolation(context.isolation),
        context.staged_writes,
        staged_overlay
    )
}

pub(crate) fn format_tx_list(infos: Vec<TransactionInfo>) -> String {
    let entries = infos
        .iter()
        .map(|info| {
            format!(
                "{}:{}:{}:{}",
                info.tx_id,
                format_transaction_mode(info.mode),
                format_read_isolation(info.isolation),
                info.staged_writes
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    format!("OK\tTX_LIST\t{}\t{entries}", infos.len())
}

pub(crate) fn format_tx_status(info: TransactionInfo) -> String {
    format!(
        "OK\tTX_STATUS\t{}\t{}\t{}\t{}",
        info.tx_id,
        format_transaction_mode(info.mode),
        format_read_isolation(info.isolation),
        info.staged_writes
    )
}

pub(crate) fn format_tx_list_all(infos: Vec<TransactionInfo>) -> String {
    let entries = infos
        .iter()
        .map(|info| {
            format!(
                "{}:{}:{}:{}:{}",
                info.session_id,
                info.tx_id,
                format_transaction_mode(info.mode),
                format_read_isolation(info.isolation),
                info.staged_writes
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    format!("OK\tTX_LIST_ALL\t{}\t{entries}", infos.len())
}

pub(crate) fn format_prepared_tx_status(info: PreparedTransactionInfo) -> String {
    format!(
        "OK\tTX_PREPARED_STATUS\t{}\t{}\t{}",
        info.prepared_id, info.shard_id, info.write_count
    )
}

pub(crate) fn format_prepared_tx_list(infos: Vec<PreparedTransactionInfo>) -> String {
    let entries = infos
        .iter()
        .map(|info| {
            format!(
                "{}:{}:{}",
                info.prepared_id, info.shard_id, info.write_count
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    format!("OK\tTX_PREPARED_LIST\t{}\t{entries}", infos.len())
}

pub(crate) fn format_transaction_decisions(decisions: &[TransactionDecisionRecord]) -> String {
    let entries = decisions
        .iter()
        .map(|decision| {
            let participants = decision
                .participants
                .iter()
                .map(|participant| {
                    format!(
                        "{}@{}#{}",
                        participant.location, participant.shard_id, participant.prepared_id
                    )
                })
                .collect::<Vec<_>>()
                .join("|");
            format!(
                "tx={} decision={} participants={participants}",
                decision.tx_id,
                format_transaction_decision(&decision.decision)
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    format!("count={} entries={entries}", decisions.len())
}

pub(crate) fn format_transaction_decision(decision: &TransactionDecision) -> &'static str {
    match decision {
        TransactionDecision::Commit => "commit",
        TransactionDecision::Abort => "abort",
    }
}

pub(crate) fn format_transaction_mode(mode: TransactionMode) -> &'static str {
    match mode {
        TransactionMode::ReadOnly => "READ_ONLY",
        TransactionMode::ReadWrite => "READ_WRITE",
    }
}

pub(crate) fn format_read_isolation(isolation: ReadIsolation) -> &'static str {
    match isolation {
        ReadIsolation::ReadCommitted => "READ_COMMITTED",
        ReadIsolation::Snapshot => "SNAPSHOT",
    }
}

#[derive(Clone)]
pub(crate) struct StagedWrite {
    pub(crate) query: String,
    pub(crate) params: neo4r_query::QueryParams,
}

pub(crate) fn write_conflict_key(query: &str, params: &neo4r_query::QueryParams) -> Option<String> {
    let normalized = query
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_ascii_uppercase();
    if !(normalized.starts_with("MATCH ")
        && (normalized.contains(" SET ")
            || normalized.contains(" REMOVE ")
            || normalized.contains(" DELETE ")))
    {
        return None;
    }
    if normalized.contains(" MERGE ") || normalized.contains(" CREATE ") {
        return None;
    }
    let query_key = if let Some((prefix, _)) = normalized.split_once(" RETURN ") {
        prefix.to_string()
    } else {
        normalized
    };
    Some(format!(
        "{query_key}|params={}",
        write_conflict_param_signature(params)
    ))
}

pub(crate) fn write_conflict_param_signature(params: &neo4r_query::QueryParams) -> String {
    let mut entries = params
        .iter()
        .map(|(key, value)| format!("{key}={value:?}"))
        .collect::<Vec<_>>();
    entries.sort();
    entries.join(",")
}

#[derive(Clone, Default)]
pub(crate) struct PreparedQueryStore {
    next_id: Arc<AtomicU64>,
    queries: Arc<Mutex<HashMap<u64, PreparedQueryState>>>,
}

impl PreparedQueryStore {
    pub(crate) fn prepare(&self, session_id: u64, query: String) -> u64 {
        let prepared_id = self.next_id.fetch_add(1, Ordering::Relaxed) + 1;
        self.queries
            .lock()
            .unwrap()
            .insert(prepared_id, PreparedQueryState { session_id, query });
        prepared_id
    }

    pub(crate) fn get(&self, session_id: u64, prepared_id: u64) -> Result<String, String> {
        let queries = self
            .queries
            .lock()
            .map_err(|_| "prepared query store lock poisoned".to_string())?;
        let query = queries
            .get(&prepared_id)
            .ok_or_else(|| format!("unknown prepared query: {prepared_id}"))?;
        ensure_prepared_query_owner(query, session_id, prepared_id)?;
        Ok(query.query.clone())
    }

    pub(crate) fn close(&self, session_id: u64, prepared_id: u64) -> Result<(), String> {
        let mut queries = self
            .queries
            .lock()
            .map_err(|_| "prepared query store lock poisoned".to_string())?;
        let query = queries
            .get(&prepared_id)
            .ok_or_else(|| format!("unknown prepared query: {prepared_id}"))?;
        ensure_prepared_query_owner(query, session_id, prepared_id)?;
        queries.remove(&prepared_id);
        Ok(())
    }

    pub(crate) fn list(&self, session_id: u64) -> Result<Vec<PreparedQueryInfo>, String> {
        let queries = self
            .queries
            .lock()
            .map_err(|_| "prepared query store lock poisoned".to_string())?;
        let mut infos = queries
            .iter()
            .filter(|(_, query)| query.session_id == session_id)
            .map(|(prepared_id, query)| PreparedQueryInfo {
                prepared_id: *prepared_id,
                query: query.query.clone(),
            })
            .collect::<Vec<_>>();
        infos.sort_by_key(|info| info.prepared_id);
        Ok(infos)
    }

    pub(crate) fn close_session(&self, session_id: u64) -> Result<usize, String> {
        let mut queries = self
            .queries
            .lock()
            .map_err(|_| "prepared query store lock poisoned".to_string())?;
        let before = queries.len();
        queries.retain(|_, query| query.session_id != session_id);
        Ok(before - queries.len())
    }
}

pub(crate) struct PreparedQueryState {
    session_id: u64,
    query: String,
}

pub(crate) struct PreparedQueryInfo {
    prepared_id: u64,
    query: String,
}

pub(crate) fn ensure_prepared_query_owner(
    query: &PreparedQueryState,
    session_id: u64,
    prepared_id: u64,
) -> Result<(), String> {
    if query.session_id == session_id {
        Ok(())
    } else {
        Err(format!(
            "prepared query {prepared_id} belongs to another session"
        ))
    }
}

pub(crate) fn format_prepared_query_list(infos: Vec<PreparedQueryInfo>) -> String {
    let count = infos.len();
    let entries = infos
        .into_iter()
        .map(|info| format!("{}:{}", info.prepared_id, escape_payload(&info.query)))
        .collect::<Vec<_>>()
        .join(",");
    format!("OK\tPREPARED_QUERY_LIST\t{count}\t{entries}")
}

pub(crate) fn format_prepared_query_route(prepared_id: u64, routing: String) -> String {
    format!("OK\tPREPARED_QUERY_ROUTE\t{prepared_id}\t{routing}")
}

pub(crate) fn format_tx_prepared_query_route(
    tx_id: u64,
    prepared_id: u64,
    routing: String,
    context: &TransactionPlanContext,
) -> String {
    format!(
        "OK\tTX_PREPARED_QUERY_ROUTE\t{tx_id}\t{prepared_id}\t{routing}\t{}",
        format_transaction_plan_context(context)
    )
}

pub(crate) fn format_prepared_query_describe(
    prepared_id: u64,
    query: &str,
    routing: String,
    params: Vec<String>,
) -> String {
    format!(
        "OK\tPREPARED_QUERY_DESC\t{prepared_id}\t{}\t{routing}\t{}\t{}",
        format_prepared_query_kind(query),
        params.len(),
        params.join(",")
    )
}

pub(crate) fn format_prepared_query_kind(query: &str) -> &'static str {
    if is_schema_cypher(query) {
        "SCHEMA"
    } else if is_write_cypher(query) {
        "WRITE"
    } else {
        "READ"
    }
}

pub(crate) fn prepared_query_routing_hint(
    db: &Neo4rDatabaseHandle,
    query: &str,
) -> Result<String, String> {
    if is_schema_cypher(query) {
        return Ok("SCHEMA".to_string());
    }
    if is_write_cypher(query) {
        return prepared_write_routing_hint(db, query);
    }
    let route = db.query_route().map_err(|err| err.to_string())?;
    Ok(format_read_routing_hint(route))
}

pub(crate) fn prepared_query_routing_hint_with_params(
    db: &Neo4rDatabaseHandle,
    query: &str,
    params: &neo4r_query::QueryParams,
) -> Result<String, String> {
    if is_schema_cypher(query) {
        return Ok("SCHEMA".to_string());
    }
    if !is_write_cypher(query) {
        let route = db.query_route().map_err(|err| err.to_string())?;
        return Ok(format_read_routing_hint(route));
    }
    if is_create_node_cypher(query) || is_merge_node_cypher(query) {
        let status = db.cluster_status().map_err(|err| err.to_string())?;
        let shard = if is_create_node_cypher(query) {
            select_create_node_write_shard(&status, query, params)?
        } else {
            select_merge_node_write_shard(&status, query, params)?
        };
        return Ok(format!("WRITE_SHARD:{}", shard.shard_id));
    }
    Ok("WRITE_TARGET_DYNAMIC".to_string())
}

pub(crate) fn prepared_write_routing_hint(
    db: &Neo4rDatabaseHandle,
    query: &str,
) -> Result<String, String> {
    let params = describe_query_parameters(query);
    if is_create_node_cypher(query) || is_merge_node_cypher(query) {
        if !params.is_empty() {
            return Ok("WRITE_SHARD_BY_PARAM".to_string());
        }
        let status = db.cluster_status().map_err(|err| err.to_string())?;
        let empty_params = neo4r_query::QueryParams::new();
        let shard = if is_create_node_cypher(query) {
            select_create_node_write_shard(&status, query, &empty_params)?
        } else {
            select_merge_node_write_shard(&status, query, &empty_params)?
        };
        return Ok(format!("WRITE_SHARD:{}", shard.shard_id));
    }
    Ok("WRITE_TARGET_DYNAMIC".to_string())
}

pub(crate) fn format_read_routing_hint(route: neo4r_db::QueryRoute) -> String {
    match route {
        neo4r_db::QueryRoute::LocalOnly => "READ_LOCAL".to_string(),
        neo4r_db::QueryRoute::RequiresRemoteShards(shards) => {
            let shards = shards
                .into_iter()
                .map(|shard| shard.to_string())
                .collect::<Vec<_>>()
                .join(",");
            format!("READ_REMOTE:{shards}")
        }
    }
}

pub(crate) fn describe_query_parameters(query: &str) -> Vec<String> {
    let mut params = BTreeSet::new();
    let mut chars = query.char_indices().peekable();
    let mut quote: Option<char> = None;

    while let Some((_, ch)) = chars.next() {
        if let Some(quote_char) = quote {
            if ch == '\\' {
                let _ = chars.next();
            } else if ch == quote_char {
                quote = None;
            }
            continue;
        }

        if ch == '\'' || ch == '"' {
            quote = Some(ch);
            continue;
        }
        if ch != '$' {
            continue;
        }

        let mut name = String::new();
        match chars.peek().copied() {
            Some((_, next)) if is_query_parameter_start(next) => {
                name.push(next);
                let _ = chars.next();
            }
            _ => continue,
        }
        while let Some((_, next)) = chars.peek().copied() {
            if !is_query_parameter_continue(next) {
                break;
            }
            name.push(next);
            let _ = chars.next();
        }
        params.insert(name);
    }

    params.into_iter().collect()
}

pub(crate) fn validate_prepared_query_params(
    prepared_id: u64,
    query: &str,
    params: &neo4r_query::QueryParams,
) -> Result<(), String> {
    let missing = describe_query_parameters(query)
        .into_iter()
        .filter(|name| !params.contains_key(name))
        .collect::<Vec<_>>();
    if missing.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "prepared query {prepared_id} missing parameter(s): {}",
            missing.join(",")
        ))
    }
}

pub(crate) fn is_query_parameter_start(ch: char) -> bool {
    ch == '_' || ch.is_ascii_alphabetic()
}

pub(crate) fn is_query_parameter_continue(ch: char) -> bool {
    ch == '_' || ch.is_ascii_alphanumeric()
}
