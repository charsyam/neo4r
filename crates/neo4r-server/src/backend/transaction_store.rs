#[derive(Clone, Default)]
struct TransactionStore {
    next_id: Arc<AtomicU64>,
    next_session_id: Arc<AtomicU64>,
    transactions: Arc<Mutex<HashMap<u64, TransactionState>>>,
}

impl TransactionStore {
    fn next_session_id(&self) -> u64 {
        self.next_session_id.fetch_add(1, Ordering::Relaxed) + 1
    }

    fn insert(&self, session_id: u64, tx: NativeTransaction) -> u64 {
        let tx_id = self.next_id.fetch_add(1, Ordering::Relaxed) + 1;
        self.transactions.lock().unwrap().insert(
            tx_id,
            TransactionState {
                session_id,
                transaction: tx,
            },
        );
        tx_id
    }

    fn query_cursor(
        &self,
        db: &Neo4rDatabaseHandle,
        session_id: u64,
        tx_id: u64,
        query: &str,
        params: &neo4r_query::QueryParams,
    ) -> Result<Box<dyn QueryCursor>, String> {
        let transactions = self
            .transactions
            .lock()
            .map_err(|_| "transaction store lock poisoned".to_string())?;
        let tx = transactions
            .get(&tx_id)
            .ok_or_else(|| format!("unknown transaction: {tx_id}"))?;
        tx.ensure_session(session_id, tx_id)?;
        match &tx.transaction {
            NativeTransaction::ReadOnly(tx) => {
                if tx.options().isolation == ReadIsolation::ReadCommitted {
                    db.query_cursor_with_params_and_options(
                        query,
                        params.clone(),
                        QueryOptions::default().with_isolation(ReadIsolation::ReadCommitted),
                    )
                    .map_err(|err| err.to_string())
                } else {
                    tx.query_cursor_with_params(query, params)
                        .map_err(|err| err.to_string())
                }
            }
            NativeTransaction::ReadWrite {
                isolation,
                staged_writes,
                ..
            } => {
                let staged_writes = staged_writes
                    .iter()
                    .map(|staged| (staged.query.clone(), staged.params.clone()))
                    .collect::<Vec<_>>();
                db.query_cursor_with_staged_writes(
                    query,
                    params.clone(),
                    QueryOptions::default().with_isolation(*isolation),
                    &staged_writes,
                )
                .map_err(|err| err.to_string())
            }
        }
    }

    fn distributed_query_cursor(
        &self,
        db: &Neo4rDatabaseHandle,
        query_peers: &QueryPeerStore,
        read_preference: QueryReadPreference,
        session_id: u64,
        tx_id: u64,
        query: &str,
        params: &neo4r_query::QueryParams,
    ) -> Result<Box<dyn QueryCursor>, String> {
        let transactions = self
            .transactions
            .lock()
            .map_err(|_| "transaction store lock poisoned".to_string())?;
        let tx = transactions
            .get(&tx_id)
            .ok_or_else(|| format!("unknown transaction: {tx_id}"))?;
        tx.ensure_session(session_id, tx_id)?;
        match &tx.transaction {
            NativeTransaction::ReadOnly(read_tx) => {
                if read_tx.options().isolation == ReadIsolation::ReadCommitted {
                    build_distributed_query_cursor_with_options(
                        db,
                        query_peers,
                        read_preference,
                        query,
                        params,
                        QueryOptions::default().with_isolation(ReadIsolation::ReadCommitted),
                    )
                } else {
                    build_distributed_read_tx_cursor(
                        db,
                        query_peers,
                        read_preference,
                        read_tx,
                        query,
                        params,
                    )
                }
            }
            NativeTransaction::ReadWrite {
                isolation,
                staged_writes,
                ..
            } => {
                if staged_writes.is_empty() {
                    return build_distributed_query_cursor_with_options(
                        db,
                        query_peers,
                        read_preference,
                        query,
                        params,
                        QueryOptions::default().with_isolation(*isolation),
                    );
                }
                let staged_writes = staged_writes
                    .iter()
                    .map(|staged| (staged.query.clone(), staged.params.clone()))
                    .collect::<Vec<_>>();
                build_distributed_query_cursor_with_local_staged_writes(
                    db,
                    query_peers,
                    read_preference,
                    query,
                    params,
                    QueryOptions::default().with_isolation(*isolation),
                    &staged_writes,
                )
            }
        }
    }

    fn plan_context(&self, session_id: u64, tx_id: u64) -> Result<TransactionPlanContext, String> {
        let transactions = self
            .transactions
            .lock()
            .map_err(|_| "transaction store lock poisoned".to_string())?;
        let tx = transactions
            .get(&tx_id)
            .ok_or_else(|| format!("unknown transaction: {tx_id}"))?;
        tx.ensure_session(session_id, tx_id)?;
        Ok(TransactionPlanContext {
            mode: tx.transaction.mode(),
            isolation: tx.transaction.isolation(),
            staged_writes: tx.transaction.staged_write_count(),
        })
    }

    fn stage_write(
        &self,
        session_id: u64,
        tx_id: u64,
        query: String,
        params: neo4r_query::QueryParams,
    ) -> Result<usize, String> {
        let mut transactions = self
            .transactions
            .lock()
            .map_err(|_| "transaction store lock poisoned".to_string())?;
        let tx = transactions
            .get_mut(&tx_id)
            .ok_or_else(|| format!("unknown transaction: {tx_id}"))?;
        tx.ensure_session(session_id, tx_id)?;
        match &mut tx.transaction {
            NativeTransaction::ReadOnly(_) => Err(format!(
                "transaction {tx_id} is read-only; begin with READ_WRITE for write queries"
            )),
            NativeTransaction::ReadWrite {
                staged_writes,
                conflict_keys,
                ..
            } => {
                if is_schema_cypher(&query) {
                    return Err(
                        "schema DDL is not supported inside native read-write transactions"
                            .to_string(),
                    );
                }
                if let Some(conflict_key) = write_conflict_key(&query, &params) {
                    if !conflict_keys.insert(conflict_key.clone()) {
                        return Err(format!(
                            "transaction {tx_id} write conflict on staged target {conflict_key:?}"
                        ));
                    }
                }
                staged_writes.push(StagedWrite { query, params });
                Ok(staged_writes.len())
            }
        }
    }

    fn close(&self, session_id: u64, tx_id: u64) -> Result<NativeTransaction, String> {
        let mut transactions = self
            .transactions
            .lock()
            .map_err(|_| "transaction store lock poisoned".to_string())?;
        let tx = transactions
            .get(&tx_id)
            .ok_or_else(|| format!("unknown transaction: {tx_id}"))?;
        tx.ensure_session(session_id, tx_id)?;
        Ok(transactions.remove(&tx_id).unwrap().transaction)
    }

    fn close_any(&self, tx_id: u64) -> Result<TransactionInfo, String> {
        let mut transactions = self
            .transactions
            .lock()
            .map_err(|_| "transaction store lock poisoned".to_string())?;
        let tx = transactions
            .remove(&tx_id)
            .ok_or_else(|| format!("unknown transaction: {tx_id}"))?;
        Ok(TransactionInfo {
            session_id: tx.session_id,
            tx_id,
            mode: tx.transaction.mode(),
            isolation: tx.transaction.isolation(),
            staged_writes: tx.transaction.staged_write_count(),
        })
    }

    fn staged_writes(&self, session_id: u64, tx_id: u64) -> Result<Vec<StagedWrite>, String> {
        let transactions = self
            .transactions
            .lock()
            .map_err(|_| "transaction store lock poisoned".to_string())?;
        let tx = transactions
            .get(&tx_id)
            .ok_or_else(|| format!("unknown transaction: {tx_id}"))?;
        tx.ensure_session(session_id, tx_id)?;
        match &tx.transaction {
            NativeTransaction::ReadOnly(_) => Ok(Vec::new()),
            NativeTransaction::ReadWrite { staged_writes, .. } => Ok(staged_writes.clone()),
        }
    }

    fn close_session(&self, session_id: u64) -> Result<usize, String> {
        let mut transactions = self
            .transactions
            .lock()
            .map_err(|_| "transaction store lock poisoned".to_string())?;
        let before = transactions.len();
        transactions.retain(|_, tx| tx.session_id != session_id);
        Ok(before - transactions.len())
    }

    fn list(&self, session_id: u64) -> Result<Vec<TransactionInfo>, String> {
        let transactions = self
            .transactions
            .lock()
            .map_err(|_| "transaction store lock poisoned".to_string())?;
        let mut infos = transactions
            .iter()
            .filter(|(_, tx)| tx.session_id == session_id)
            .map(|(tx_id, tx)| TransactionInfo {
                session_id: tx.session_id,
                tx_id: *tx_id,
                mode: tx.transaction.mode(),
                isolation: tx.transaction.isolation(),
                staged_writes: tx.transaction.staged_write_count(),
            })
            .collect::<Vec<_>>();
        infos.sort_by_key(|info| info.tx_id);
        Ok(infos)
    }

    fn list_all(&self) -> Result<Vec<TransactionInfo>, String> {
        let transactions = self
            .transactions
            .lock()
            .map_err(|_| "transaction store lock poisoned".to_string())?;
        let mut infos = transactions
            .iter()
            .map(|(tx_id, tx)| TransactionInfo {
                session_id: tx.session_id,
                tx_id: *tx_id,
                mode: tx.transaction.mode(),
                isolation: tx.transaction.isolation(),
                staged_writes: tx.transaction.staged_write_count(),
            })
            .collect::<Vec<_>>();
        infos.sort_by_key(|info| (info.session_id, info.tx_id));
        Ok(infos)
    }

    fn status(&self, session_id: u64, tx_id: u64) -> Result<TransactionInfo, String> {
        let transactions = self
            .transactions
            .lock()
            .map_err(|_| "transaction store lock poisoned".to_string())?;
        let tx = transactions
            .get(&tx_id)
            .ok_or_else(|| format!("unknown transaction: {tx_id}"))?;
        tx.ensure_session(session_id, tx_id)?;
        Ok(TransactionInfo {
            session_id: tx.session_id,
            tx_id,
            mode: tx.transaction.mode(),
            isolation: tx.transaction.isolation(),
            staged_writes: tx.transaction.staged_write_count(),
        })
    }
}

struct TransactionState {
    session_id: u64,
    transaction: NativeTransaction,
}

#[derive(Clone, Default)]
struct PreparedTransactionStore {
    next_id: Arc<AtomicU64>,
    prepared: Arc<Mutex<HashMap<u64, PreparedWriteBatch>>>,
    path: Option<Arc<PathBuf>>,
}

impl PreparedTransactionStore {
    fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let prepared = load_prepared_transactions(&path)?;
        let next_id = prepared.keys().copied().max().unwrap_or(0);
        Ok(Self {
            next_id: Arc::new(AtomicU64::new(next_id)),
            prepared: Arc::new(Mutex::new(prepared)),
            path: Some(Arc::new(path)),
        })
    }

    fn prepare(
        &self,
        shard_id: u64,
        writes: Vec<(String, neo4r_query::QueryParams)>,
    ) -> Result<u64, String> {
        let prepared_id = self.next_id.fetch_add(1, Ordering::Relaxed) + 1;
        let mut prepared = self
            .prepared
            .lock()
            .map_err(|_| "prepared transaction store lock poisoned".to_string())?;
        prepared.insert(prepared_id, PreparedWriteBatch { shard_id, writes });
        if let Err(err) = self.save(&prepared) {
            prepared.remove(&prepared_id);
            return Err(err);
        }
        Ok(prepared_id)
    }

    fn take(&self, prepared_id: u64) -> Result<PreparedWriteBatch, String> {
        let mut prepared = self
            .prepared
            .lock()
            .map_err(|_| "prepared transaction store lock poisoned".to_string())?;
        let prepared_batch = prepared
            .remove(&prepared_id)
            .ok_or_else(|| format!("unknown prepared transaction: {prepared_id}"))?;
        if let Err(err) = self.save(&prepared) {
            prepared.insert(prepared_id, prepared_batch.clone());
            return Err(err);
        }
        Ok(prepared_batch)
    }

    fn status(&self, prepared_id: u64) -> Result<PreparedTransactionInfo, String> {
        let prepared = self
            .prepared
            .lock()
            .map_err(|_| "prepared transaction store lock poisoned".to_string())?;
        let batch = prepared
            .get(&prepared_id)
            .ok_or_else(|| format!("unknown prepared transaction: {prepared_id}"))?;
        Ok(PreparedTransactionInfo {
            prepared_id,
            shard_id: batch.shard_id,
            write_count: batch.writes.len(),
        })
    }

    fn list(&self) -> Result<Vec<PreparedTransactionInfo>, String> {
        let prepared = self
            .prepared
            .lock()
            .map_err(|_| "prepared transaction store lock poisoned".to_string())?;
        let mut infos = prepared
            .iter()
            .map(|(prepared_id, batch)| PreparedTransactionInfo {
                prepared_id: *prepared_id,
                shard_id: batch.shard_id,
                write_count: batch.writes.len(),
            })
            .collect::<Vec<_>>();
        infos.sort_by_key(|info| info.prepared_id);
        Ok(infos)
    }

    fn save(&self, prepared: &HashMap<u64, PreparedWriteBatch>) -> Result<(), String> {
        let Some(path) = &self.path else {
            return Ok(());
        };
        save_prepared_transactions(path, prepared)
    }
}

#[derive(Clone, Debug)]
struct PreparedWriteBatch {
    shard_id: u64,
    writes: Vec<(String, neo4r_query::QueryParams)>,
}

fn load_prepared_transactions(path: &Path) -> io::Result<HashMap<u64, PreparedWriteBatch>> {
    let file = match File::open(path) {
        Ok(file) => file,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(HashMap::new()),
        Err(err) => return Err(err),
    };
    let mut lines = BufReader::new(file).lines();
    let header = lines
        .next()
        .transpose()?
        .ok_or_else(|| io::Error::other("missing prepared transaction header"))?;
    if header != PREPARED_TRANSACTIONS_MAGIC {
        return Err(io::Error::other("invalid prepared transaction header"));
    }
    let mut prepared = HashMap::new();
    for line in lines {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let (prepared_id, batch) = decode_prepared_transaction_record(&line)?;
        prepared.insert(prepared_id, batch);
    }
    Ok(prepared)
}

fn save_prepared_transactions(
    path: &Path,
    prepared: &HashMap<u64, PreparedWriteBatch>,
) -> Result<(), String> {
    let tmp_path = path.with_extension("log.tmp");
    {
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&tmp_path)
            .map_err(|err| format!("open prepared transaction store: {err}"))?;
        writeln!(file, "{PREPARED_TRANSACTIONS_MAGIC}")
            .map_err(|err| format!("write prepared transaction header: {err}"))?;
        let mut ids = prepared.keys().copied().collect::<Vec<_>>();
        ids.sort_unstable();
        for prepared_id in ids {
            let batch = prepared
                .get(&prepared_id)
                .ok_or_else(|| format!("missing prepared transaction {prepared_id}"))?;
            writeln!(
                file,
                "{}",
                encode_prepared_transaction_record(prepared_id, batch)
            )
            .map_err(|err| format!("write prepared transaction record: {err}"))?;
        }
        file.sync_all()
            .map_err(|err| format!("sync prepared transaction store: {err}"))?;
    }
    fs::rename(&tmp_path, path)
        .map_err(|err| format!("rename prepared transaction store: {err}"))?;
    if let Some(parent) = path.parent() {
        File::open(parent)
            .and_then(|file| file.sync_all())
            .map_err(|err| format!("sync prepared transaction store directory: {err}"))?;
    }
    Ok(())
}

fn encode_prepared_transaction_record(prepared_id: u64, batch: &PreparedWriteBatch) -> String {
    format!(
        "{prepared_id}\t{}\t{}",
        batch.shard_id,
        encode_query_batch_payload(&batch.writes)
    )
}

fn decode_prepared_transaction_record(line: &str) -> io::Result<(u64, PreparedWriteBatch)> {
    let mut parts = line.splitn(3, '\t');
    let prepared_id = parts
        .next()
        .ok_or_else(|| io::Error::other("missing prepared transaction id"))?
        .parse::<u64>()
        .map_err(|_| io::Error::other("invalid prepared transaction id"))?;
    let shard_id = parts
        .next()
        .ok_or_else(|| io::Error::other("missing prepared transaction shard id"))?
        .parse::<u64>()
        .map_err(|_| io::Error::other("invalid prepared transaction shard id"))?;
    let writes = parts
        .next()
        .ok_or_else(|| io::Error::other("missing prepared transaction writes"))
        .and_then(|payload| decode_query_batch_payload(payload).map_err(io::Error::other))?;
    Ok((prepared_id, PreparedWriteBatch { shard_id, writes }))
}

impl TransactionState {
    fn ensure_session(&self, session_id: u64, tx_id: u64) -> Result<(), String> {
        if self.session_id == session_id {
            Ok(())
        } else {
            Err(format!("unknown transaction: {tx_id}"))
        }
    }
}
