#![allow(dead_code)]

use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SchemaMigrationProgress {
    pub(crate) migration_id: String,
    pub(crate) state: String,
    pub(crate) processed_rows: u64,
    pub(crate) total_rows: u64,
    pub(crate) updated_at_unix_ms: u128,
}

#[derive(Clone, Debug)]
pub(crate) struct SchemaMigrationProgressStore {
    path: PathBuf,
}

impl SchemaMigrationProgressStore {
    pub(crate) fn open(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    pub(crate) fn save(&self, progress: &SchemaMigrationProgress) -> Result<(), String> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent).map_err(|err| err.to_string())?;
        }
        let tmp = self.path.with_extension("json.tmp");
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp)
            .map_err(|err| err.to_string())?;
        file.write_all(encode_schema_progress(progress).as_bytes())
            .map_err(|err| err.to_string())?;
        file.sync_all().map_err(|err| err.to_string())?;
        drop(file);
        fs::rename(&tmp, &self.path).map_err(|err| err.to_string())
    }

    pub(crate) fn load(&self) -> Result<Option<SchemaMigrationProgress>, String> {
        match fs::read_to_string(&self.path) {
            Ok(text) => Ok(Some(decode_schema_progress(&text)?)),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(err) => Err(err.to_string()),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct GcPlan {
    pub(crate) dry_run: bool,
    pub(crate) safe_watermark: u64,
    pub(crate) candidates: Vec<PathBuf>,
}

pub(crate) fn execute_gc_plan(
    plan: &GcPlan,
    pending_restore_manifest: impl AsRef<Path>,
) -> Result<usize, String> {
    if pending_restore_manifest.as_ref().exists() {
        return Err("refusing GC while a pending restore manifest exists".to_string());
    }
    if plan.dry_run {
        return Ok(0);
    }
    let mut removed = 0;
    for path in &plan.candidates {
        if !path.exists() {
            continue;
        }
        fs::remove_file(path).map_err(|err| err.to_string())?;
        removed += 1;
    }
    Ok(removed)
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct TlsCertInventoryEntry {
    pub(crate) path: PathBuf,
    pub(crate) exists: bool,
    pub(crate) modified_unix_seconds: u64,
}

pub(crate) fn tls_cert_inventory(paths: &[PathBuf]) -> Vec<TlsCertInventoryEntry> {
    paths
        .iter()
        .map(|path| {
            let modified_unix_seconds = path
                .metadata()
                .and_then(|metadata| metadata.modified())
                .ok()
                .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|duration| duration.as_secs())
                .unwrap_or_default();
            TlsCertInventoryEntry {
                path: path.clone(),
                exists: path.exists(),
                modified_unix_seconds,
            }
        })
        .collect()
}

pub(crate) fn compare_previous_release_metadata(
    current: &str,
    previous: &str,
) -> Result<(), String> {
    let current = parse_key_values(current);
    let previous = parse_key_values(previous);
    for key in [
        "native_protocol_min",
        "native_protocol_max",
        "data_format_version",
        "storage_manifest_format",
    ] {
        let Some(current_value) = current.get(key) else {
            return Err(format!("missing current compatibility key {key}"));
        };
        let Some(previous_value) = previous.get(key) else {
            return Err(format!("missing previous compatibility key {key}"));
        };
        if current_value < previous_value {
            return Err(format!(
                "compatibility key {key} moved backwards from {previous_value} to {current_value}"
            ));
        }
    }
    Ok(())
}

pub(crate) fn alert_rules_cover_required_metrics(
    rules: &str,
    required_metrics: &[&str],
) -> Result<(), String> {
    for metric in required_metrics {
        if !rules.contains(metric) {
            return Err(format!("missing alert coverage for metric {metric}"));
        }
    }
    Ok(())
}

pub(crate) fn perf_baseline_within_thresholds(
    observed: &BTreeMap<String, f64>,
    thresholds: &BTreeMap<String, f64>,
) -> Result<(), String> {
    for (metric, observed_value) in observed {
        let Some(threshold) = thresholds.get(metric) else {
            return Err(format!("missing performance threshold for {metric}"));
        };
        if observed_value > threshold {
            return Err(format!(
                "performance regression for {metric}: observed {observed_value} > threshold {threshold}"
            ));
        }
    }
    Ok(())
}

pub(crate) fn validate_restore_drill_manifest(manifest: &str) -> Result<(), String> {
    for required in [
        "policy: snapshot-plus-wal-archive",
        "max_age_hours:",
        "restore_targets:",
        "metadata-reopens",
        "wal-archive-readable",
        "query-regression-corpus-runs",
        "seed-new-cluster-from-restored-data",
    ] {
        if !manifest.contains(required) {
            return Err(format!("restore drill manifest missing {required}"));
        }
    }
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ResourceAdmissionPolicy {
    pub(crate) max_concurrent_queries: u64,
    pub(crate) max_result_rows: u64,
    pub(crate) max_memory_bytes: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ResourceAdmissionRequest {
    pub(crate) active_queries: u64,
    pub(crate) estimated_result_rows: u64,
    pub(crate) estimated_memory_bytes: u64,
}

pub(crate) fn evaluate_resource_admission(
    policy: &ResourceAdmissionPolicy,
    request: &ResourceAdmissionRequest,
) -> Result<(), String> {
    if request.active_queries >= policy.max_concurrent_queries {
        return Err(format!(
            "query concurrency admission rejected: active={} limit={}",
            request.active_queries, policy.max_concurrent_queries
        ));
    }
    if request.estimated_result_rows > policy.max_result_rows {
        return Err(format!(
            "result row admission rejected: rows={} limit={}",
            request.estimated_result_rows, policy.max_result_rows
        ));
    }
    if request.estimated_memory_bytes > policy.max_memory_bytes {
        return Err(format!(
            "memory admission rejected: bytes={} limit={}",
            request.estimated_memory_bytes, policy.max_memory_bytes
        ));
    }
    Ok(())
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct SloBurnRateInput {
    pub(crate) error_rate: f64,
    pub(crate) latency_p99_ms: f64,
    pub(crate) replication_lag_entries: f64,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct SloThresholds {
    pub(crate) max_error_rate: f64,
    pub(crate) max_latency_p99_ms: f64,
    pub(crate) max_replication_lag_entries: f64,
}

pub(crate) fn evaluate_slo_burn_rate(
    input: &SloBurnRateInput,
    thresholds: &SloThresholds,
) -> Result<(), String> {
    if input.error_rate > thresholds.max_error_rate {
        return Err(format!(
            "slo error burn too high: {} > {}",
            input.error_rate, thresholds.max_error_rate
        ));
    }
    if input.latency_p99_ms > thresholds.max_latency_p99_ms {
        return Err(format!(
            "slo latency burn too high: {} > {}",
            input.latency_p99_ms, thresholds.max_latency_p99_ms
        ));
    }
    if input.replication_lag_entries > thresholds.max_replication_lag_entries {
        return Err(format!(
            "slo replication lag burn too high: {} > {}",
            input.replication_lag_entries, thresholds.max_replication_lag_entries
        ));
    }
    Ok(())
}

fn encode_schema_progress(progress: &SchemaMigrationProgress) -> String {
    format!(
        "{{\"migration_id\":\"{}\",\"state\":\"{}\",\"processed_rows\":{},\"total_rows\":{},\"updated_at_unix_ms\":{}}}\n",
        escape_json(&progress.migration_id),
        escape_json(&progress.state),
        progress.processed_rows,
        progress.total_rows,
        progress.updated_at_unix_ms
    )
}

fn decode_schema_progress(input: &str) -> Result<SchemaMigrationProgress, String> {
    Ok(SchemaMigrationProgress {
        migration_id: json_string_field(input, "migration_id")?,
        state: json_string_field(input, "state")?,
        processed_rows: json_u64_field(input, "processed_rows")?,
        total_rows: json_u64_field(input, "total_rows")?,
        updated_at_unix_ms: u128::from(json_u64_field(input, "updated_at_unix_ms")?),
    })
}

fn parse_key_values(input: &str) -> BTreeMap<String, String> {
    input
        .lines()
        .filter_map(|line| line.split_once('='))
        .map(|(key, value)| (key.trim().to_string(), value.trim().to_string()))
        .collect()
}

fn json_string_field(input: &str, name: &str) -> Result<String, String> {
    input
        .split(&format!("\"{name}\":\""))
        .nth(1)
        .and_then(|tail| tail.split('"').next())
        .map(str::to_string)
        .ok_or_else(|| format!("missing json string field {name}"))
}

fn json_u64_field(input: &str, name: &str) -> Result<u64, String> {
    input
        .split(&format!("\"{name}\":"))
        .nth(1)
        .and_then(|tail| {
            tail.split(|ch: char| !ch.is_ascii_digit())
                .find(|value| !value.is_empty())
        })
        .ok_or_else(|| format!("missing json u64 field {name}"))?
        .parse::<u64>()
        .map_err(|err| err.to_string())
}

fn escape_json(input: &str) -> String {
    input.replace('\\', "\\\\").replace('"', "\\\"")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_path(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "neo4r-production-primitive-{name}-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ))
    }

    #[test]
    fn schema_migration_progress_store_survives_reopen() {
        let path = temp_path("schema-progress").join("system/schema-migration-progress.json");
        let store = SchemaMigrationProgressStore::open(&path);
        let progress = SchemaMigrationProgress {
            migration_id: "idx-person-email-v1".to_string(),
            state: "running".to_string(),
            processed_rows: 7,
            total_rows: 10,
            updated_at_unix_ms: 42,
        };

        store.save(&progress).unwrap();
        let reopened = SchemaMigrationProgressStore::open(&path);

        assert_eq!(reopened.load().unwrap(), Some(progress));
        let _ = fs::remove_dir_all(path.parent().unwrap().parent().unwrap());
    }

    #[test]
    fn gc_executor_respects_dry_run_and_pending_restore_guard() {
        let dir = temp_path("gc");
        fs::create_dir_all(&dir).unwrap();
        let candidate = dir.join("00000000000000000001.log");
        fs::write(&candidate, b"old-wal").unwrap();
        let pending = dir.join("pitr-restore.pending");
        let dry_run = GcPlan {
            dry_run: true,
            safe_watermark: 1,
            candidates: vec![candidate.clone()],
        };

        assert_eq!(execute_gc_plan(&dry_run, &pending).unwrap(), 0);
        assert!(candidate.exists());
        fs::write(&pending, b"pending").unwrap();
        let apply = GcPlan {
            dry_run: false,
            safe_watermark: 1,
            candidates: vec![candidate.clone()],
        };
        assert!(execute_gc_plan(&apply, &pending)
            .unwrap_err()
            .contains("pending restore"));
        fs::remove_file(&pending).unwrap();
        assert_eq!(execute_gc_plan(&apply, &pending).unwrap(), 1);
        assert!(!candidate.exists());
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn tls_cert_inventory_reports_existing_cert_files() {
        let dir = temp_path("tls");
        fs::create_dir_all(&dir).unwrap();
        let cert = dir.join("web.crt");
        fs::write(&cert, b"cert").unwrap();

        let inventory = tls_cert_inventory(&[cert.clone(), dir.join("missing.crt")]);

        assert!(inventory[0].exists);
        assert!(inventory[0].modified_unix_seconds > 0);
        assert!(!inventory[1].exists);
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn previous_release_metadata_must_not_move_backwards() {
        let previous = "native_protocol_min=1\nnative_protocol_max=1\ndata_format_version=1\nstorage_manifest_format=1\n";
        let current = "native_protocol_min=1\nnative_protocol_max=2\ndata_format_version=1\nstorage_manifest_format=1\n";

        assert!(compare_previous_release_metadata(current, previous).is_ok());
        assert!(compare_previous_release_metadata(previous, current)
            .unwrap_err()
            .contains("moved backwards"));
    }

    #[test]
    fn alert_rules_must_cover_required_metrics() {
        let rules =
            "expr: neo4r_raft_leaders == 0\nexpr: neo4r_storage_repair_failures_total > 0\n";

        assert!(alert_rules_cover_required_metrics(
            rules,
            &["neo4r_raft_leaders", "neo4r_storage_repair_failures_total"]
        )
        .is_ok());
        assert!(alert_rules_cover_required_metrics(rules, &["neo4r_missing"]).is_err());
    }

    #[test]
    fn perf_baseline_rejects_regressions() {
        let thresholds = BTreeMap::from([
            ("query_p99_ms".to_string(), 10.0),
            ("write_p99_ms".to_string(), 20.0),
        ]);
        let observed = BTreeMap::from([
            ("query_p99_ms".to_string(), 8.0),
            ("write_p99_ms".to_string(), 25.0),
        ]);

        assert!(perf_baseline_within_thresholds(&observed, &thresholds)
            .unwrap_err()
            .contains("performance regression"));
    }

    #[test]
    fn restore_drill_manifest_requires_seed_and_query_checks() {
        let manifest = "policy: snapshot-plus-wal-archive\nmax_age_hours: 24\nrestore_targets:\n  - expected_checks:\n      - metadata-reopens\n      - wal-archive-readable\n      - query-regression-corpus-runs\n      - seed-new-cluster-from-restored-data\n";
        let missing_seed = "policy: snapshot-plus-wal-archive\nmax_age_hours: 24\nrestore_targets:\n  - expected_checks:\n      - metadata-reopens\n      - wal-archive-readable\n      - query-regression-corpus-runs\n";

        assert!(validate_restore_drill_manifest(manifest).is_ok());
        assert!(validate_restore_drill_manifest(missing_seed)
            .unwrap_err()
            .contains("seed-new-cluster"));
    }

    #[test]
    fn resource_admission_rejects_over_budget_requests() {
        let policy = ResourceAdmissionPolicy {
            max_concurrent_queries: 2,
            max_result_rows: 100,
            max_memory_bytes: 1024,
        };

        assert!(evaluate_resource_admission(
            &policy,
            &ResourceAdmissionRequest {
                active_queries: 1,
                estimated_result_rows: 10,
                estimated_memory_bytes: 512,
            }
        )
        .is_ok());
        assert!(evaluate_resource_admission(
            &policy,
            &ResourceAdmissionRequest {
                active_queries: 2,
                estimated_result_rows: 10,
                estimated_memory_bytes: 512,
            }
        )
        .unwrap_err()
        .contains("concurrency"));
    }

    #[test]
    fn slo_burn_rate_rejects_latency_and_lag_regressions() {
        let thresholds = SloThresholds {
            max_error_rate: 0.01,
            max_latency_p99_ms: 100.0,
            max_replication_lag_entries: 10.0,
        };

        assert!(evaluate_slo_burn_rate(
            &SloBurnRateInput {
                error_rate: 0.001,
                latency_p99_ms: 90.0,
                replication_lag_entries: 3.0,
            },
            &thresholds
        )
        .is_ok());
        assert!(evaluate_slo_burn_rate(
            &SloBurnRateInput {
                error_rate: 0.001,
                latency_p99_ms: 150.0,
                replication_lag_entries: 3.0,
            },
            &thresholds
        )
        .unwrap_err()
        .contains("latency"));
    }
}
