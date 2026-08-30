use super::*;
use crate::production_primitives::{
    evaluate_resource_admission, ResourceAdmissionPolicy, ResourceAdmissionRequest,
};

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
    pub(crate) gossip_fanout_success: Arc<AtomicU64>,
    pub(crate) gossip_fanout_failure: Arc<AtomicU64>,
    pub(crate) gossip_auth_failures: Arc<AtomicU64>,
    pub(crate) gossip_negotiation_success: Arc<AtomicU64>,
    pub(crate) gossip_negotiation_failure: Arc<AtomicU64>,
}

#[derive(Clone, Default)]
pub(crate) struct GossipAuthTokenStore {
    token: Arc<Mutex<Option<String>>>,
}

impl GossipAuthTokenStore {
    pub(crate) fn set(&self, token: Option<String>) {
        if let Ok(mut guard) = self.token.lock() {
            *guard = token;
        }
    }

    pub(crate) fn get(&self) -> Option<String> {
        self.token.lock().ok().and_then(|guard| guard.clone())
    }
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
        let limits = *self
            .limits
            .lock()
            .map_err(|_| "tenant quota limits lock poisoned".to_string())?;
        if let Some(limit) = limits.max_concurrent_queries {
            let mut active = self
                .active_queries
                .lock()
                .map_err(|_| "tenant quota lock poisoned".to_string())?;
            let current = active.entry(database.to_string()).or_default();
            evaluate_resource_admission(
                &ResourceAdmissionPolicy {
                    max_concurrent_queries: limit as u64,
                    max_result_rows: limits.max_result_rows.unwrap_or(usize::MAX) as u64,
                    max_memory_bytes: u64::MAX,
                },
                &ResourceAdmissionRequest {
                    active_queries: *current as u64,
                    estimated_result_rows: 0,
                    estimated_memory_bytes: 0,
                },
            )
            .map_err(|err| format!("tenant quota exceeded for database {database}: {err}"))?;
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
            evaluate_resource_admission(
                &ResourceAdmissionPolicy {
                    max_concurrent_queries: u64::MAX,
                    max_result_rows: limit as u64,
                    max_memory_bytes: u64::MAX,
                },
                &ResourceAdmissionRequest {
                    active_queries: 0,
                    estimated_result_rows: rows as u64,
                    estimated_memory_bytes: 0,
                },
            )
            .map_err(|err| {
                format!("tenant result row quota exceeded for database {database}: {err}")
            })?;
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
