use super::*;
#[derive(Clone, Default)]
pub(crate) struct CursorStore {
    next_id: Arc<AtomicU64>,
    cursors: Arc<Mutex<HashMap<u64, CursorState>>>,
}

impl CursorStore {
    #[cfg(test)]
    pub(crate) fn insert(&self, session_id: u64, cursor: Box<dyn QueryCursor>) -> u64 {
        self.insert_with_permit(session_id, cursor, None)
    }

    pub(crate) fn insert_with_permit(
        &self,
        session_id: u64,
        cursor: Box<dyn QueryCursor>,
        _tenant_permit: Option<TenantQueryPermit>,
    ) -> u64 {
        let cursor_id = self.next_id.fetch_add(1, Ordering::Relaxed) + 1;
        self.cursors.lock().unwrap().insert(
            cursor_id,
            CursorState {
                session_id,
                cursor,
                _tenant_permit,
            },
        );
        cursor_id
    }

    pub(crate) fn fetch(
        &self,
        session_id: u64,
        cursor_id: u64,
        page_size: usize,
    ) -> Result<ResultPage, String> {
        let mut cursors = self
            .cursors
            .lock()
            .map_err(|_| "cursor store lock poisoned".to_string())?;
        let cursor = cursors
            .get_mut(&cursor_id)
            .ok_or_else(|| format!("unknown cursor: {cursor_id}"))?;
        ensure_cursor_owner(cursor, session_id, cursor_id)?;
        let page = cursor.cursor.fetch(page_size);
        let rows = page.rows;
        let has_more = page.has_more;
        if !has_more {
            cursors.remove(&cursor_id);
        }
        Ok(ResultPage { rows, has_more })
    }

    pub(crate) fn close(&self, session_id: u64, cursor_id: u64) -> Result<(), String> {
        let mut cursors = self
            .cursors
            .lock()
            .map_err(|_| "cursor store lock poisoned".to_string())?;
        let Some(cursor) = cursors.get(&cursor_id) else {
            return Ok(());
        };
        ensure_cursor_owner(cursor, session_id, cursor_id)?;
        cursors.remove(&cursor_id);
        Ok(())
    }

    pub(crate) fn close_session(&self, session_id: u64) -> Result<usize, String> {
        let mut cursors = self
            .cursors
            .lock()
            .map_err(|_| "cursor store lock poisoned".to_string())?;
        let before = cursors.len();
        cursors.retain(|_, cursor| cursor.session_id != session_id);
        Ok(before - cursors.len())
    }
}

pub(crate) struct CursorState {
    session_id: u64,
    cursor: Box<dyn QueryCursor>,
    _tenant_permit: Option<TenantQueryPermit>,
}

pub(crate) fn ensure_cursor_owner(
    cursor: &CursorState,
    session_id: u64,
    cursor_id: u64,
) -> Result<(), String> {
    if cursor.session_id == session_id {
        Ok(())
    } else {
        Err(format!("cursor {cursor_id} belongs to another session"))
    }
}

#[derive(Clone, Default)]
pub(crate) struct PendingRequestStore {
    state: Arc<Mutex<PendingRequestState>>,
}

#[derive(Default)]
pub(crate) struct PendingRequestState {
    pending: BTreeSet<(u64, u64)>,
    cancelled: BTreeSet<(u64, u64)>,
}

impl PendingRequestStore {
    pub(crate) fn register(&self, session_id: u64, request_id: u64) -> Result<(), String> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| "pending request store lock poisoned".to_string())?;
        state.pending.insert((session_id, request_id));
        state.cancelled.remove(&(session_id, request_id));
        Ok(())
    }

    pub(crate) fn cancel(&self, session_id: u64, request_id: u64) -> Result<bool, String> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| "pending request store lock poisoned".to_string())?;
        if state.pending.contains(&(session_id, request_id)) {
            state.cancelled.insert((session_id, request_id));
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub(crate) fn take_cancelled(&self, session_id: u64, request_id: u64) -> Result<bool, String> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| "pending request store lock poisoned".to_string())?;
        state.pending.remove(&(session_id, request_id));
        Ok(state.cancelled.remove(&(session_id, request_id)))
    }

    pub(crate) fn start(&self, session_id: u64, request_id: u64) -> Result<(), String> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| "pending request store lock poisoned".to_string())?;
        state.pending.remove(&(session_id, request_id));
        state.cancelled.remove(&(session_id, request_id));
        Ok(())
    }

    pub(crate) fn close_session(&self, session_id: u64) -> Result<(), String> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| "pending request store lock poisoned".to_string())?;
        state
            .pending
            .retain(|(pending_session_id, _)| *pending_session_id != session_id);
        state
            .cancelled
            .retain(|(pending_session_id, _)| *pending_session_id != session_id);
        Ok(())
    }
}

#[derive(Debug)]
pub(crate) struct ResultPage {
    pub(crate) rows: Vec<QueryRow>,
    pub(crate) has_more: bool,
}

pub(crate) struct FetchRequest {
    pub(crate) cursor_id: u64,
    pub(crate) page_size: usize,
}

pub(crate) fn parse_fetch_payload(payload: &str) -> Result<FetchRequest, String> {
    let mut parts = payload.split('\t');
    let cursor_id = parse_cursor_id(
        parts
            .next()
            .ok_or_else(|| "FETCH requires cursor id".to_string())?,
    )?;
    let page_size = parts
        .next()
        .map(|value| {
            value
                .parse::<usize>()
                .map_err(|_| "FETCH page size must be a positive integer".to_string())
        })
        .transpose()?
        .unwrap_or(128);
    if page_size == 0 {
        return Err("FETCH page size must be greater than zero".to_string());
    }
    if parts.next().is_some() {
        return Err("FETCH got extra fields".to_string());
    }
    Ok(FetchRequest {
        cursor_id,
        page_size,
    })
}

pub(crate) fn parse_cursor_id(payload: &str) -> Result<u64, String> {
    payload
        .trim()
        .parse::<u64>()
        .map_err(|_| "cursor id must be an unsigned integer".to_string())
}

pub(crate) fn parse_cancel_payload(payload: &str) -> Result<u64, String> {
    let mut parts = payload.trim().split('\t');
    let request_id = parts
        .next()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| "CANCEL requires target request id".to_string())?
        .parse::<u64>()
        .map_err(|_| "CANCEL target request id must be an unsigned integer".to_string())?;
    if parts.next().is_some() {
        return Err("CANCEL got extra fields".to_string());
    }
    Ok(request_id)
}

pub(crate) fn format_result_start(
    cursor_id: u64,
    total_rows: Option<usize>,
    page: ResultPage,
) -> String {
    let total_rows = total_rows
        .map(|total_rows| total_rows.to_string())
        .unwrap_or_else(|| "UNKNOWN".to_string());
    format!(
        "OK\tRESULT_START\t{cursor_id}\t{total_rows}\t{}\t{}\t{}",
        page.rows.len(),
        page.has_more,
        format_rows(&page.rows)
    )
}

pub(crate) fn format_result_page(cursor_id: u64, page: ResultPage) -> String {
    format!(
        "OK\tRESULT_PAGE\t{cursor_id}\t{}\t{}\t{}",
        page.rows.len(),
        page.has_more,
        format_rows(&page.rows)
    )
}

pub(crate) fn format_rows(rows: &[QueryRow]) -> String {
    encode_query_rows(rows)
}

pub(crate) fn native_response_frame(request_id: u64, response: BackendResponse) -> NativeFrame {
    let message_type = if matches!(
        response,
        BackendResponse::Err(_) | BackendResponse::Redirect(_)
    ) {
        NativeMessageType::Error
    } else {
        NativeMessageType::Response
    };
    NativeFrame::new(
        message_type,
        request_id,
        format_response(&response).into_bytes(),
    )
}

pub(crate) fn escape_payload(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('\t', "\\t")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
        .replace('\x1e', "\\x1e")
}

#[derive(Clone)]
pub(crate) struct NativeWorkerPool {
    pub(crate) jobs: Arc<Mutex<Option<SyncSender<NativeJob>>>>,
    pub(crate) joins: Arc<Mutex<Vec<thread::JoinHandle<()>>>>,
    pub(crate) pending_requests: PendingRequestStore,
}

impl NativeWorkerPool {
    pub(crate) fn new(
        context: NativeExecutionContext,
        worker_count: usize,
        queue_capacity: usize,
    ) -> Self {
        let worker_count = worker_count.max(1);
        let queue_capacity = queue_capacity.max(1);
        let pending_requests = context.pending_requests.clone();
        let (jobs, job_rx) = mpsc::sync_channel::<NativeJob>(queue_capacity);
        let job_rx = Arc::new(Mutex::new(job_rx));
        let mut joins = Vec::with_capacity(worker_count);

        for _ in 0..worker_count {
            let context = context.clone();
            let job_rx = job_rx.clone();
            joins.push(thread::spawn(move || native_worker_loop(context, job_rx)));
        }

        Self {
            jobs: Arc::new(Mutex::new(Some(jobs))),
            joins: Arc::new(Mutex::new(joins)),
            pending_requests,
        }
    }

    pub(crate) fn submit(
        &self,
        session_id: u64,
        frame: NativeFrame,
        response: mpsc::Sender<NativeFrame>,
    ) -> io::Result<()> {
        let request_id = frame.request_id;
        let jobs = self
            .jobs
            .lock()
            .map_err(|_| io::Error::other("native worker pool lock poisoned"))?
            .as_ref()
            .cloned()
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::BrokenPipe, "native worker pool stopped")
            })?;
        self.pending_requests
            .register(session_id, request_id)
            .map_err(io::Error::other)?;
        let job = NativeJob {
            session_id,
            frame,
            response,
        };
        match jobs.try_send(job) {
            Ok(()) => {}
            Err(TrySendError::Full(job)) => {
                let _ = self.pending_requests.start(session_id, request_id);
                send_native_response(
                    &job.response,
                    NativeFrame::new(
                        NativeMessageType::Error,
                        request_id,
                        b"ERR\tnative worker queue full".to_vec(),
                    ),
                )?;
            }
            Err(TrySendError::Disconnected(_)) => {
                let _ = self.pending_requests.start(session_id, request_id);
                return Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "native worker pool stopped",
                ));
            }
        }
        Ok(())
    }
}

impl Drop for NativeWorkerPool {
    fn drop(&mut self) {
        if Arc::strong_count(&self.jobs) != 1 {
            return;
        }
        if let Ok(mut jobs) = self.jobs.lock() {
            jobs.take();
        }
        if let Ok(mut joins) = self.joins.lock() {
            while let Some(join) = joins.pop() {
                let _ = join.join();
            }
        }
    }
}

pub(crate) struct NativeJob {
    session_id: u64,
    frame: NativeFrame,
    response: mpsc::Sender<NativeFrame>,
}

pub(crate) fn native_worker_loop(
    context: NativeExecutionContext,
    jobs: Arc<Mutex<Receiver<NativeJob>>>,
) {
    loop {
        let job = {
            let jobs = match jobs.lock() {
                Ok(jobs) => jobs,
                Err(_) => break,
            };
            match jobs.recv() {
                Ok(job) => job,
                Err(_) => break,
            }
        };
        if context
            .pending_requests
            .take_cancelled(job.session_id, job.frame.request_id)
            .unwrap_or(false)
        {
            let response = NativeFrame::new(
                NativeMessageType::Error,
                job.frame.request_id,
                b"ERR\trequest cancelled".to_vec(),
            );
            let _ = job.response.send(response);
            continue;
        }
        let _ = context
            .pending_requests
            .start(job.session_id, job.frame.request_id);
        let response = context.execute_frame(job.session_id, job.frame);
        let _ = job.response.send(response);
    }
}

pub(crate) fn write_native_responses(
    stream: Box<dyn Write + Send>,
    responses: Receiver<NativeFrame>,
) -> io::Result<()> {
    let mut writer = BufWriter::new(stream);
    for frame in responses {
        write_frame(&mut writer, &frame)?;
    }
    Ok(())
}

pub(crate) fn send_native_response(
    response_tx: &mpsc::Sender<NativeFrame>,
    frame: NativeFrame,
) -> io::Result<()> {
    response_tx
        .send(frame)
        .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "native response writer stopped"))
}
