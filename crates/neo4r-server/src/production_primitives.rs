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
pub(crate) struct RestoreDrillEvidence {
    pub(crate) snapshot_restored: bool,
    pub(crate) wal_replayed: bool,
    pub(crate) correctness_corpus_ran: bool,
    pub(crate) seeded_cluster_from_restored_data: bool,
}

pub(crate) fn validate_restore_drill_evidence(
    evidence: &RestoreDrillEvidence,
) -> Result<(), String> {
    if !evidence.snapshot_restored {
        return Err("restore drill did not restore a snapshot".to_string());
    }
    if !evidence.wal_replayed {
        return Err("restore drill did not replay WAL archive".to_string());
    }
    if !evidence.correctness_corpus_ran {
        return Err("restore drill did not run query correctness corpus".to_string());
    }
    if !evidence.seeded_cluster_from_restored_data {
        return Err("restore drill did not seed a cluster from restored data".to_string());
    }
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ReadConsistencyContract {
    pub(crate) default_mode: String,
    pub(crate) read_index_required_for_strong_reads: bool,
    pub(crate) follower_stale_is_explicit: bool,
    pub(crate) stale_reads_return_lag_metadata: bool,
}

pub(crate) fn validate_read_consistency_contract(
    contract: &ReadConsistencyContract,
) -> Result<(), String> {
    if contract.default_mode != "read-index" {
        return Err("read consistency default must be read-index".to_string());
    }
    if !contract.read_index_required_for_strong_reads {
        return Err("strong reads must require read-index".to_string());
    }
    if !contract.follower_stale_is_explicit {
        return Err("follower stale reads must be explicit".to_string());
    }
    if !contract.stale_reads_return_lag_metadata {
        return Err("stale reads must expose lag metadata".to_string());
    }
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct MultiNodeFixtureContract {
    pub(crate) server_count: usize,
    pub(crate) uses_real_tcp_ports: bool,
    pub(crate) verifies_join: bool,
    pub(crate) verifies_catch_up: bool,
    pub(crate) verifies_redirect: bool,
    pub(crate) verifies_leader_failover: bool,
}

pub(crate) fn validate_multi_node_fixture_contract(
    contract: &MultiNodeFixtureContract,
) -> Result<(), String> {
    if contract.server_count < 3 {
        return Err("multi-node fixture must run at least three servers".to_string());
    }
    if !contract.uses_real_tcp_ports {
        return Err("multi-node fixture must use real TCP ports".to_string());
    }
    if !contract.verifies_join {
        return Err("multi-node fixture must verify node join".to_string());
    }
    if !contract.verifies_catch_up {
        return Err("multi-node fixture must verify catch-up".to_string());
    }
    if !contract.verifies_redirect {
        return Err("multi-node fixture must verify redirect".to_string());
    }
    if !contract.verifies_leader_failover {
        return Err("multi-node fixture must verify leader failover".to_string());
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ResourcePressureSample {
    pub(crate) memory_used_bytes: u64,
    pub(crate) memory_limit_bytes: u64,
    pub(crate) disk_used_bytes: u64,
    pub(crate) disk_limit_bytes: u64,
    pub(crate) cpu_load_millis: u64,
    pub(crate) cpu_limit_millis: u64,
}

pub(crate) fn evaluate_resource_pressure(sample: &ResourcePressureSample) -> Result<(), String> {
    if sample.memory_used_bytes > sample.memory_limit_bytes {
        return Err(format!(
            "memory pressure admission rejected: used={} limit={}",
            sample.memory_used_bytes, sample.memory_limit_bytes
        ));
    }
    if sample.disk_used_bytes > sample.disk_limit_bytes {
        return Err(format!(
            "disk pressure admission rejected: used={} limit={}",
            sample.disk_used_bytes, sample.disk_limit_bytes
        ));
    }
    if sample.cpu_load_millis > sample.cpu_limit_millis {
        return Err(format!(
            "cpu pressure admission rejected: load={} limit={}",
            sample.cpu_load_millis, sample.cpu_limit_millis
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SecurityHardeningEvidence {
    pub(crate) token_expiration_enforced: bool,
    pub(crate) rbac_denials_are_audited: bool,
    pub(crate) admin_sessions_are_http_only: bool,
    pub(crate) csrf_required_for_admin_writes: bool,
    pub(crate) tls_or_mtls_required_for_remote_admin: bool,
}

pub(crate) fn validate_security_hardening_evidence(
    evidence: &SecurityHardeningEvidence,
) -> Result<(), String> {
    if !evidence.token_expiration_enforced {
        return Err("token expiration must be enforced".to_string());
    }
    if !evidence.rbac_denials_are_audited {
        return Err("RBAC denials must be audited".to_string());
    }
    if !evidence.admin_sessions_are_http_only {
        return Err("admin sessions must be HttpOnly".to_string());
    }
    if !evidence.csrf_required_for_admin_writes {
        return Err("admin writes must require CSRF protection".to_string());
    }
    if !evidence.tls_or_mtls_required_for_remote_admin {
        return Err("remote admin must require TLS or mTLS".to_string());
    }
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct UpgradeCompatibilityPlan {
    pub(crate) mixed_version_protocol_checked: bool,
    pub(crate) snapshot_fetch_compat_checked: bool,
    pub(crate) membership_metadata_compat_checked: bool,
    pub(crate) rollback_before_format_bump_checked: bool,
    pub(crate) previous_release_fixture_checked: bool,
}

pub(crate) fn validate_upgrade_compatibility_plan(
    plan: &UpgradeCompatibilityPlan,
) -> Result<(), String> {
    if !plan.mixed_version_protocol_checked {
        return Err("upgrade plan missing mixed-version protocol check".to_string());
    }
    if !plan.snapshot_fetch_compat_checked {
        return Err("upgrade plan missing snapshot fetch compatibility check".to_string());
    }
    if !plan.membership_metadata_compat_checked {
        return Err("upgrade plan missing membership metadata compatibility check".to_string());
    }
    if !plan.rollback_before_format_bump_checked {
        return Err("upgrade plan missing rollback-before-format-bump check".to_string());
    }
    if !plan.previous_release_fixture_checked {
        return Err("upgrade plan missing previous release fixture check".to_string());
    }
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RaftWriteSafetyContract {
    pub(crate) commits_are_shard_local: bool,
    pub(crate) entries_require_config_authority_stamp: bool,
    pub(crate) entries_require_leader_authority_stamp: bool,
    pub(crate) tcp_append_success_is_durable_ack: bool,
    pub(crate) udp_append_is_never_durable_ack: bool,
    pub(crate) apply_is_fenced_by_committed_index: bool,
    pub(crate) writes_commit_only_after_quorum_ack: bool,
    pub(crate) clients_refresh_topology_on_replay_or_missing_address: bool,
    pub(crate) anti_entropy_repairs_only_uncommitted_suffixes: bool,
    pub(crate) chaos_and_observability_gates_cover_recovery: bool,
}

pub(crate) fn validate_raft_write_safety_contract(
    contract: &RaftWriteSafetyContract,
) -> Result<(), String> {
    let checks = [
        (
            contract.commits_are_shard_local,
            "commits must be advanced per shard, not by a global max index",
        ),
        (
            contract.entries_require_config_authority_stamp,
            "replicated entries must carry a config authority stamp",
        ),
        (
            contract.entries_require_leader_authority_stamp,
            "replicated entries must carry a leader authority stamp",
        ),
        (
            contract.tcp_append_success_is_durable_ack,
            "TCP append success must mean the follower durably accepted the entry",
        ),
        (
            contract.udp_append_is_never_durable_ack,
            "UDP append must not be counted as a durable write quorum ACK",
        ),
        (
            contract.apply_is_fenced_by_committed_index,
            "apply must be fenced by committed index",
        ),
        (
            contract.writes_commit_only_after_quorum_ack,
            "writes must commit only after current-term quorum ACK",
        ),
        (
            contract.clients_refresh_topology_on_replay_or_missing_address,
            "clients must refresh topology when replay blocks writes or address is missing",
        ),
        (
            contract.anti_entropy_repairs_only_uncommitted_suffixes,
            "anti-entropy must repair only uncommitted suffixes",
        ),
        (
            contract.chaos_and_observability_gates_cover_recovery,
            "chaos and observability gates must cover recovery paths",
        ),
    ];
    for (passed, message) in checks {
        if !passed {
            return Err(message.to_string());
        }
    }
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ProductionReadinessContract {
    pub(crate) membership_authority_is_raft_metadata: bool,
    pub(crate) client_seed_bootstraps_topology_registry: bool,
    pub(crate) multi_node_failover_fixture_is_automated: bool,
    pub(crate) replica_replacement_waits_for_replay: bool,
    pub(crate) read_consistency_modes_are_explicit: bool,
    pub(crate) restore_drill_seeds_cluster_from_data: bool,
    pub(crate) storage_crash_points_are_gated: bool,
    pub(crate) observability_exports_shard_health: bool,
    pub(crate) security_hardening_is_gated: bool,
    pub(crate) production_test_matrix_is_executable: bool,
}

pub(crate) fn validate_production_readiness_contract(
    contract: &ProductionReadinessContract,
) -> Result<(), String> {
    let checks = [
        (
            contract.membership_authority_is_raft_metadata,
            "membership authority must be Raft/metadata-log backed",
        ),
        (
            contract.client_seed_bootstraps_topology_registry,
            "client seed bootstrap must fetch topology registry",
        ),
        (
            contract.multi_node_failover_fixture_is_automated,
            "multi-node failover fixture must be automated",
        ),
        (
            contract.replica_replacement_waits_for_replay,
            "replica replacement must reject writes until replay completes",
        ),
        (
            contract.read_consistency_modes_are_explicit,
            "read consistency modes must be explicit",
        ),
        (
            contract.restore_drill_seeds_cluster_from_data,
            "restore drill must seed a cluster from restored data",
        ),
        (
            contract.storage_crash_points_are_gated,
            "storage crash-point atomicity must be gated",
        ),
        (
            contract.observability_exports_shard_health,
            "observability must export shard health and replay lag",
        ),
        (
            contract.security_hardening_is_gated,
            "security hardening must be enforced by a gate",
        ),
        (
            contract.production_test_matrix_is_executable,
            "production test matrix must be executable",
        ),
    ];
    for (passed, message) in checks {
        if !passed {
            return Err(message.to_string());
        }
    }
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ProductionMaturityEvidence {
    pub(crate) automated_three_node_failover_tests: usize,
    pub(crate) authoritative_membership_ownership_tests: usize,
    pub(crate) replica_replacement_tests: usize,
    pub(crate) replay_write_rejection_tests: usize,
    pub(crate) storage_crash_points: usize,
    pub(crate) pitr_restore_drills: usize,
    pub(crate) rolling_upgrade_checks: usize,
    pub(crate) performance_and_chaos_gates: usize,
}

pub(crate) fn validate_production_maturity_evidence(
    evidence: &ProductionMaturityEvidence,
) -> Result<(), String> {
    let minimums = [
        (
            evidence.automated_three_node_failover_tests,
            2,
            "at least two automated three-node failover tests are required",
        ),
        (
            evidence.authoritative_membership_ownership_tests,
            3,
            "at least three authoritative membership/ownership tests are required",
        ),
        (
            evidence.replica_replacement_tests,
            2,
            "at least two replica replacement tests are required",
        ),
        (
            evidence.replay_write_rejection_tests,
            2,
            "at least two replay write rejection tests are required",
        ),
        (
            evidence.storage_crash_points,
            5,
            "at least five storage crash points are required",
        ),
        (
            evidence.pitr_restore_drills,
            2,
            "at least two PITR restore drills are required",
        ),
        (
            evidence.rolling_upgrade_checks,
            5,
            "at least five rolling upgrade checks are required",
        ),
        (
            evidence.performance_and_chaos_gates,
            4,
            "at least four performance/chaos gates are required",
        ),
    ];
    for (observed, required, message) in minimums {
        if observed < required {
            return Err(format!(
                "{message}: observed {observed}, required {required}"
            ));
        }
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
    fn restore_drill_evidence_requires_snapshot_wal_corpus_and_seed() {
        assert!(validate_restore_drill_evidence(&RestoreDrillEvidence {
            snapshot_restored: true,
            wal_replayed: true,
            correctness_corpus_ran: true,
            seeded_cluster_from_restored_data: true,
        })
        .is_ok());
        assert!(validate_restore_drill_evidence(&RestoreDrillEvidence {
            snapshot_restored: true,
            wal_replayed: false,
            correctness_corpus_ran: true,
            seeded_cluster_from_restored_data: true,
        })
        .unwrap_err()
        .contains("WAL"));
    }

    #[test]
    fn read_consistency_contract_requires_read_index_default() {
        assert!(
            validate_read_consistency_contract(&ReadConsistencyContract {
                default_mode: "read-index".to_string(),
                read_index_required_for_strong_reads: true,
                follower_stale_is_explicit: true,
                stale_reads_return_lag_metadata: true,
            })
            .is_ok()
        );
        assert!(
            validate_read_consistency_contract(&ReadConsistencyContract {
                default_mode: "follower-stale".to_string(),
                read_index_required_for_strong_reads: true,
                follower_stale_is_explicit: true,
                stale_reads_return_lag_metadata: true,
            })
            .unwrap_err()
            .contains("read-index")
        );
    }

    #[test]
    fn multi_node_fixture_contract_requires_join_redirect_and_failover() {
        assert!(
            validate_multi_node_fixture_contract(&MultiNodeFixtureContract {
                server_count: 3,
                uses_real_tcp_ports: true,
                verifies_join: true,
                verifies_catch_up: true,
                verifies_redirect: true,
                verifies_leader_failover: true,
            })
            .is_ok()
        );
        assert!(
            validate_multi_node_fixture_contract(&MultiNodeFixtureContract {
                server_count: 2,
                uses_real_tcp_ports: true,
                verifies_join: true,
                verifies_catch_up: true,
                verifies_redirect: true,
                verifies_leader_failover: true,
            })
            .unwrap_err()
            .contains("three servers")
        );
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
    fn resource_pressure_rejects_memory_disk_and_cpu_pressure() {
        assert!(evaluate_resource_pressure(&ResourcePressureSample {
            memory_used_bytes: 512,
            memory_limit_bytes: 1024,
            disk_used_bytes: 1024,
            disk_limit_bytes: 4096,
            cpu_load_millis: 400,
            cpu_limit_millis: 1000,
        })
        .is_ok());
        assert!(evaluate_resource_pressure(&ResourcePressureSample {
            memory_used_bytes: 2048,
            memory_limit_bytes: 1024,
            disk_used_bytes: 1024,
            disk_limit_bytes: 4096,
            cpu_load_millis: 400,
            cpu_limit_millis: 1000,
        })
        .unwrap_err()
        .contains("memory pressure"));
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

    #[test]
    fn security_hardening_evidence_requires_admin_controls() {
        assert!(
            validate_security_hardening_evidence(&SecurityHardeningEvidence {
                token_expiration_enforced: true,
                rbac_denials_are_audited: true,
                admin_sessions_are_http_only: true,
                csrf_required_for_admin_writes: true,
                tls_or_mtls_required_for_remote_admin: true,
            })
            .is_ok()
        );
        assert!(
            validate_security_hardening_evidence(&SecurityHardeningEvidence {
                token_expiration_enforced: true,
                rbac_denials_are_audited: false,
                admin_sessions_are_http_only: true,
                csrf_required_for_admin_writes: true,
                tls_or_mtls_required_for_remote_admin: true,
            })
            .unwrap_err()
            .contains("RBAC")
        );
    }

    #[test]
    fn upgrade_compatibility_plan_requires_rollback_and_previous_fixture() {
        assert!(
            validate_upgrade_compatibility_plan(&UpgradeCompatibilityPlan {
                mixed_version_protocol_checked: true,
                snapshot_fetch_compat_checked: true,
                membership_metadata_compat_checked: true,
                rollback_before_format_bump_checked: true,
                previous_release_fixture_checked: true,
            })
            .is_ok()
        );
        assert!(
            validate_upgrade_compatibility_plan(&UpgradeCompatibilityPlan {
                mixed_version_protocol_checked: true,
                snapshot_fetch_compat_checked: true,
                membership_metadata_compat_checked: true,
                rollback_before_format_bump_checked: false,
                previous_release_fixture_checked: true,
            })
            .unwrap_err()
            .contains("rollback")
        );
    }

    #[test]
    fn production_readiness_contract_requires_all_ten_gates() {
        let ready = ProductionReadinessContract {
            membership_authority_is_raft_metadata: true,
            client_seed_bootstraps_topology_registry: true,
            multi_node_failover_fixture_is_automated: true,
            replica_replacement_waits_for_replay: true,
            read_consistency_modes_are_explicit: true,
            restore_drill_seeds_cluster_from_data: true,
            storage_crash_points_are_gated: true,
            observability_exports_shard_health: true,
            security_hardening_is_gated: true,
            production_test_matrix_is_executable: true,
        };
        assert!(validate_production_readiness_contract(&ready).is_ok());

        let missing_seed_bootstrap = ProductionReadinessContract {
            client_seed_bootstraps_topology_registry: false,
            ..ready.clone()
        };
        assert!(
            validate_production_readiness_contract(&missing_seed_bootstrap)
                .unwrap_err()
                .contains("seed bootstrap")
        );

        let missing_replay_gate = ProductionReadinessContract {
            replica_replacement_waits_for_replay: false,
            ..ready
        };
        assert!(validate_production_readiness_contract(&missing_replay_gate)
            .unwrap_err()
            .contains("replay completes"));
    }

    #[test]
    fn raft_write_safety_contract_requires_all_ten_guards() {
        let contract = RaftWriteSafetyContract {
            commits_are_shard_local: true,
            entries_require_config_authority_stamp: true,
            entries_require_leader_authority_stamp: true,
            tcp_append_success_is_durable_ack: true,
            udp_append_is_never_durable_ack: true,
            apply_is_fenced_by_committed_index: true,
            writes_commit_only_after_quorum_ack: true,
            clients_refresh_topology_on_replay_or_missing_address: true,
            anti_entropy_repairs_only_uncommitted_suffixes: true,
            chaos_and_observability_gates_cover_recovery: true,
        };
        assert!(validate_raft_write_safety_contract(&contract).is_ok());

        let global_commit = RaftWriteSafetyContract {
            commits_are_shard_local: false,
            ..contract.clone()
        };
        assert!(validate_raft_write_safety_contract(&global_commit)
            .unwrap_err()
            .contains("per shard"));

        let udp_counts_for_quorum = RaftWriteSafetyContract {
            udp_append_is_never_durable_ack: false,
            ..contract
        };
        assert!(validate_raft_write_safety_contract(&udp_counts_for_quorum)
            .unwrap_err()
            .contains("UDP append"));
    }

    #[test]
    fn production_maturity_evidence_requires_runtime_depth() {
        let evidence = ProductionMaturityEvidence {
            automated_three_node_failover_tests: 2,
            authoritative_membership_ownership_tests: 3,
            replica_replacement_tests: 2,
            replay_write_rejection_tests: 2,
            storage_crash_points: 5,
            pitr_restore_drills: 2,
            rolling_upgrade_checks: 5,
            performance_and_chaos_gates: 4,
        };
        assert!(validate_production_maturity_evidence(&evidence).is_ok());

        let weak_failover = ProductionMaturityEvidence {
            automated_three_node_failover_tests: 1,
            ..evidence.clone()
        };
        assert!(validate_production_maturity_evidence(&weak_failover)
            .unwrap_err()
            .contains("three-node failover"));

        let weak_crash_coverage = ProductionMaturityEvidence {
            storage_crash_points: 4,
            ..evidence
        };
        assert!(validate_production_maturity_evidence(&weak_crash_coverage)
            .unwrap_err()
            .contains("storage crash points"));
    }
}
