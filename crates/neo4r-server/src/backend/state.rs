use super::*;

#[derive(Clone, Default)]
pub(crate) struct WebMetrics {
    pub(crate) http_requests: Arc<AtomicU64>,
    pub(crate) http_errors: Arc<AtomicU64>,
    pub(crate) auth_failures: Arc<AtomicU64>,
    pub(crate) auth_rate_limited: Arc<AtomicU64>,
    pub(crate) queries: Arc<AtomicU64>,
    pub(crate) query_errors: Arc<AtomicU64>,
    pub(crate) slow_queries: Arc<AtomicU64>,
    pub(crate) registry_requests: Arc<AtomicU64>,
    pub(crate) stale_epoch_rejections: Arc<AtomicU64>,
    pub(crate) redirects: Arc<AtomicU64>,
}

#[derive(Clone, Default)]
pub(crate) struct AuthFailureLimiter {
    entries: Arc<Mutex<HashMap<String, AuthFailureEntry>>>,
}

#[derive(Clone, Copy, Debug, Default)]
struct AuthFailureEntry {
    window_start_ms: u128,
    failures: u64,
}

impl AuthFailureLimiter {
    pub(crate) fn record_and_should_limit(&self, key: &str, now_ms: u128) -> bool {
        const WINDOW_MS: u128 = 60_000;
        const MAX_FAILURES: u64 = 5;
        let Ok(mut entries) = self.entries.lock() else {
            return true;
        };
        let entry = entries.entry(key.to_string()).or_default();
        if now_ms.saturating_sub(entry.window_start_ms) > WINDOW_MS {
            *entry = AuthFailureEntry {
                window_start_ms: now_ms,
                failures: 0,
            };
        }
        entry.failures = entry.failures.saturating_add(1);
        entry.failures > MAX_FAILURES
    }
}

#[derive(Clone, Default)]
pub(crate) struct SlowQueryLog {
    entries: Arc<Mutex<Vec<SlowQueryEntry>>>,
}

#[derive(Clone)]
pub(crate) struct SlowQueryEntry {
    pub(crate) unix_ms: u128,
    pub(crate) elapsed_ms: u128,
    pub(crate) query: String,
}

impl SlowQueryLog {
    pub(crate) fn push(&self, entry: SlowQueryEntry) {
        if let Ok(mut entries) = self.entries.lock() {
            entries.push(entry);
            const MAX_SLOW_QUERY_ENTRIES: usize = 128;
            if entries.len() > MAX_SLOW_QUERY_ENTRIES {
                let excess = entries.len() - MAX_SLOW_QUERY_ENTRIES;
                entries.drain(0..excess);
            }
        }
    }

    pub(crate) fn entries(&self) -> Vec<SlowQueryEntry> {
        self.entries
            .lock()
            .map(|entries| entries.clone())
            .unwrap_or_default()
    }
}

#[derive(Clone, Default)]
pub(crate) struct TenantQuota {
    limits: Arc<Mutex<TenantQuotaLimits>>,
    active_queries: Arc<Mutex<HashMap<String, usize>>>,
}

#[derive(Clone, Copy, Debug, Default)]
struct TenantQuotaLimits {
    max_concurrent_queries: Option<usize>,
    max_result_rows: Option<usize>,
}

pub(crate) struct TenantQueryPermit {
    active_queries: Arc<Mutex<HashMap<String, usize>>>,
    database: String,
}

impl TenantQuota {
    pub(crate) fn configure(
        &self,
        max_concurrent_queries: Option<usize>,
        max_result_rows: Option<usize>,
    ) {
        if let Ok(mut limits) = self.limits.lock() {
            limits.max_concurrent_queries = max_concurrent_queries;
            limits.max_result_rows = max_result_rows;
        }
    }

    pub(crate) fn acquire_query(&self, database: &str) -> Result<TenantQueryPermit, String> {
        let limit = self
            .limits
            .lock()
            .map_err(|_| "tenant quota limits lock poisoned".to_string())?
            .max_concurrent_queries;
        if let Some(limit) = limit {
            let mut active = self
                .active_queries
                .lock()
                .map_err(|_| "tenant quota lock poisoned".to_string())?;
            let current = active.entry(database.to_string()).or_default();
            if *current >= limit {
                return Err(format!(
                    "tenant quota exceeded for database {database}: active_queries={current} limit={limit}"
                ));
            }
            *current += 1;
        }
        Ok(TenantQueryPermit {
            active_queries: self.active_queries.clone(),
            database: database.to_string(),
        })
    }

    pub(crate) fn validate_result_rows(&self, database: &str, rows: usize) -> Result<(), String> {
        let limit = self
            .limits
            .lock()
            .map_err(|_| "tenant quota limits lock poisoned".to_string())?
            .max_result_rows;
        if let Some(limit) = limit {
            if rows > limit {
                return Err(format!(
                    "tenant result row quota exceeded for database {database}: rows={rows} limit={limit}"
                ));
            }
        }
        Ok(())
    }
}

impl Drop for TenantQueryPermit {
    fn drop(&mut self) {
        let Ok(mut active) = self.active_queries.lock() else {
            return;
        };
        if let Some(current) = active.get_mut(&self.database) {
            *current = current.saturating_sub(1);
            if *current == 0 {
                active.remove(&self.database);
            }
        }
    }
}

#[derive(Clone, Default)]
pub(crate) struct ReplicationTlsChannelConfigStore {
    config: Arc<Mutex<Option<ReplicationTlsConfig>>>,
}

impl ReplicationTlsChannelConfigStore {
    pub(crate) fn set(&self, config: Option<ReplicationTlsConfig>) {
        if let Ok(mut current) = self.config.lock() {
            *current = config;
        }
    }

    pub(crate) fn get(&self) -> Option<ReplicationTlsConfig> {
        self.config.lock().ok().and_then(|config| config.clone())
    }
}
