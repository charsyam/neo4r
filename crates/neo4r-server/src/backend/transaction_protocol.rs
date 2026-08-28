use super::*;
pub(crate) enum TransactionCommand {
    Begin {
        mode: TransactionMode,
        isolation: ReadIsolation,
    },
    Query {
        tx_id: u64,
        query: String,
        params: neo4r_query::QueryParams,
    },
    ExecutePrepared {
        tx_id: u64,
        prepared_id: u64,
        params: neo4r_query::QueryParams,
    },
    PreparedQueryPlan {
        tx_id: u64,
        prepared_id: u64,
        params: neo4r_query::QueryParams,
    },
    PreparedQueryRoute {
        tx_id: u64,
        prepared_id: u64,
        params: neo4r_query::QueryParams,
    },
    DistributedQuery {
        tx_id: u64,
        query: String,
        params: neo4r_query::QueryParams,
    },
    QueryPlan {
        tx_id: u64,
        query: String,
        params: neo4r_query::QueryParams,
    },
    Commit {
        tx_id: u64,
    },
    Rollback {
        tx_id: u64,
    },
    Kill {
        tx_id: u64,
    },
    Status {
        tx_id: u64,
    },
    PrepareWriteBatchShard {
        shard_id: u64,
        writes: Vec<(String, neo4r_query::QueryParams)>,
    },
    PreparedStatus {
        prepared_id: u64,
    },
    ListPrepared,
    CommitPrepared {
        prepared_id: u64,
    },
    AbortPrepared {
        prepared_id: u64,
    },
    List,
    ListAll,
}

pub(crate) enum PreparedQueryCommand {
    Prepare {
        query: String,
    },
    Execute {
        prepared_id: u64,
        params: neo4r_query::QueryParams,
    },
    QueryPlan {
        prepared_id: u64,
        params: neo4r_query::QueryParams,
    },
    Route {
        prepared_id: u64,
        params: neo4r_query::QueryParams,
    },
    Describe {
        prepared_id: u64,
    },
    Close {
        prepared_id: u64,
    },
    List,
}

pub(crate) fn parse_prepared_query_command(
    payload: &str,
) -> Result<Option<PreparedQueryCommand>, String> {
    let Some((command, rest)) = payload.split_once('\t') else {
        return match payload {
            "LIST_PREPARED" => Ok(Some(PreparedQueryCommand::List)),
            _ => Ok(None),
        };
    };
    match command {
        "PREPARE_QUERY" => {
            if rest.trim().is_empty() {
                Err("PREPARE_QUERY requires a cypher string".to_string())
            } else {
                Ok(Some(PreparedQueryCommand::Prepare {
                    query: rest.to_string(),
                }))
            }
        }
        "EXECUTE_PREPARED" => {
            let (prepared_id, params) = parse_prepared_query_execute_payload(rest)?;
            Ok(Some(PreparedQueryCommand::Execute {
                prepared_id,
                params,
            }))
        }
        "PREPARED_QUERY_PLAN" => {
            let (prepared_id, params) = parse_prepared_query_execute_payload(rest)?;
            Ok(Some(PreparedQueryCommand::QueryPlan {
                prepared_id,
                params,
            }))
        }
        "PREPARED_QUERY_ROUTE" => {
            let (prepared_id, params) = parse_prepared_query_execute_payload(rest)?;
            Ok(Some(PreparedQueryCommand::Route {
                prepared_id,
                params,
            }))
        }
        "DESCRIBE_PREPARED" => Ok(Some(PreparedQueryCommand::Describe {
            prepared_id: parse_cursor_id(rest)?,
        })),
        "CLOSE_PREPARED" => Ok(Some(PreparedQueryCommand::Close {
            prepared_id: parse_cursor_id(rest)?,
        })),
        "LIST_PREPARED" => {
            if rest.trim().is_empty() {
                Ok(Some(PreparedQueryCommand::List))
            } else {
                Err("LIST_PREPARED does not take arguments".to_string())
            }
        }
        _ => Ok(None),
    }
}

pub(crate) fn parse_prepared_query_execute_payload(
    payload: &str,
) -> Result<(u64, neo4r_query::QueryParams), String> {
    let Some((prepared_id, params_payload)) = payload.split_once('\t') else {
        return Ok((parse_cursor_id(payload)?, neo4r_query::QueryParams::new()));
    };
    let prepared_id = parse_cursor_id(prepared_id)?;
    let (_, params) = parse_query_payload(&format!("_\t{params_payload}"))?;
    Ok((prepared_id, params))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TransactionMode {
    ReadOnly,
    ReadWrite,
}

pub(crate) fn parse_transaction_command(
    payload: &str,
) -> Result<Option<TransactionCommand>, String> {
    let Some((command, rest)) = payload.split_once('\t') else {
        return match payload {
            "BEGIN_TX" => Ok(Some(TransactionCommand::Begin {
                mode: TransactionMode::ReadOnly,
                isolation: ReadIsolation::Snapshot,
            })),
            "LIST_TX" => Ok(Some(TransactionCommand::List)),
            "LIST_ALL_TX" => Ok(Some(TransactionCommand::ListAll)),
            "LIST_PREPARED_TX" => Ok(Some(TransactionCommand::ListPrepared)),
            _ => Ok(None),
        };
    };
    match command {
        "BEGIN_TX" => {
            let (mode, isolation) = parse_tx_begin_options(rest)?;
            Ok(Some(TransactionCommand::Begin { mode, isolation }))
        }
        "TX_QUERY" => {
            let (tx_id, query_payload) = rest
                .split_once('\t')
                .ok_or_else(|| "TX_QUERY requires transaction id and query".to_string())?;
            let tx_id = parse_cursor_id(tx_id)?;
            let (query, params) = parse_query_payload(query_payload)?;
            Ok(Some(TransactionCommand::Query {
                tx_id,
                query,
                params,
            }))
        }
        "TX_EXECUTE_PREPARED" => {
            let (tx_id, execute_payload) = rest.split_once('\t').ok_or_else(|| {
                "TX_EXECUTE_PREPARED requires transaction id and prepared query id".to_string()
            })?;
            let tx_id = parse_cursor_id(tx_id)?;
            let (prepared_id, params) = parse_prepared_query_execute_payload(execute_payload)?;
            Ok(Some(TransactionCommand::ExecutePrepared {
                tx_id,
                prepared_id,
                params,
            }))
        }
        "TX_PREPARED_QUERY_PLAN" => {
            let (tx_id, execute_payload) = rest.split_once('\t').ok_or_else(|| {
                "TX_PREPARED_QUERY_PLAN requires transaction id and prepared query id".to_string()
            })?;
            let tx_id = parse_cursor_id(tx_id)?;
            let (prepared_id, params) = parse_prepared_query_execute_payload(execute_payload)?;
            Ok(Some(TransactionCommand::PreparedQueryPlan {
                tx_id,
                prepared_id,
                params,
            }))
        }
        "TX_PREPARED_QUERY_ROUTE" => {
            let (tx_id, execute_payload) = rest.split_once('\t').ok_or_else(|| {
                "TX_PREPARED_QUERY_ROUTE requires transaction id and prepared query id".to_string()
            })?;
            let tx_id = parse_cursor_id(tx_id)?;
            let (prepared_id, params) = parse_prepared_query_execute_payload(execute_payload)?;
            Ok(Some(TransactionCommand::PreparedQueryRoute {
                tx_id,
                prepared_id,
                params,
            }))
        }
        "TX_QUERY_DISTRIBUTED" => {
            let (tx_id, query_payload) = rest.split_once('\t').ok_or_else(|| {
                "TX_QUERY_DISTRIBUTED requires transaction id and query".to_string()
            })?;
            let tx_id = parse_cursor_id(tx_id)?;
            let (query, params) = parse_query_payload(query_payload)?;
            Ok(Some(TransactionCommand::DistributedQuery {
                tx_id,
                query,
                params,
            }))
        }
        "TX_QUERY_PLAN" => {
            let (tx_id, query_payload) = rest
                .split_once('\t')
                .ok_or_else(|| "TX_QUERY_PLAN requires transaction id and query".to_string())?;
            let tx_id = parse_cursor_id(tx_id)?;
            let (query, params) = parse_query_payload(query_payload)?;
            Ok(Some(TransactionCommand::QueryPlan {
                tx_id,
                query,
                params,
            }))
        }
        "COMMIT_TX" => Ok(Some(TransactionCommand::Commit {
            tx_id: parse_cursor_id(rest)?,
        })),
        "ROLLBACK_TX" => Ok(Some(TransactionCommand::Rollback {
            tx_id: parse_cursor_id(rest)?,
        })),
        "KILL_TX" => Ok(Some(TransactionCommand::Kill {
            tx_id: parse_cursor_id(rest)?,
        })),
        "TX_STATUS" => Ok(Some(TransactionCommand::Status {
            tx_id: parse_cursor_id(rest)?,
        })),
        "TX_PREPARE_WRITE_BATCH_SHARD" => {
            let (shard_id, payload) = rest.split_once('\t').ok_or_else(|| {
                "TX_PREPARE_WRITE_BATCH_SHARD requires shard id and encoded write batch".to_string()
            })?;
            Ok(Some(TransactionCommand::PrepareWriteBatchShard {
                shard_id: parse_cursor_id(shard_id)?,
                writes: decode_query_batch_payload(payload)?,
            }))
        }
        "TX_COMMIT_PREPARED" => Ok(Some(TransactionCommand::CommitPrepared {
            prepared_id: parse_cursor_id(rest)?,
        })),
        "TX_ABORT_PREPARED" => Ok(Some(TransactionCommand::AbortPrepared {
            prepared_id: parse_cursor_id(rest)?,
        })),
        "TX_PREPARED_STATUS" => Ok(Some(TransactionCommand::PreparedStatus {
            prepared_id: parse_cursor_id(rest)?,
        })),
        "LIST_PREPARED_TX" => {
            if rest.trim().is_empty() {
                Ok(Some(TransactionCommand::ListPrepared))
            } else {
                Err("LIST_PREPARED_TX does not take arguments".to_string())
            }
        }
        "LIST_TX" => {
            if rest.trim().is_empty() {
                Ok(Some(TransactionCommand::List))
            } else {
                Err("LIST_TX does not take arguments".to_string())
            }
        }
        "LIST_ALL_TX" => {
            if rest.trim().is_empty() {
                Ok(Some(TransactionCommand::ListAll))
            } else {
                Err("LIST_ALL_TX does not take arguments".to_string())
            }
        }
        _ => Ok(None),
    }
}

pub(crate) fn parse_tx_begin_options(
    value: &str,
) -> Result<(TransactionMode, ReadIsolation), String> {
    let mut mode = TransactionMode::ReadOnly;
    let mut isolation = ReadIsolation::Snapshot;
    for option in value.split_whitespace() {
        match option {
            "READ_ONLY" => mode = TransactionMode::ReadOnly,
            "READ_WRITE" => mode = TransactionMode::ReadWrite,
            "SNAPSHOT" => isolation = ReadIsolation::Snapshot,
            "READ_COMMITTED" => isolation = ReadIsolation::ReadCommitted,
            value => return Err(format!("unsupported transaction option: {value}")),
        }
    }
    Ok((mode, isolation))
}
