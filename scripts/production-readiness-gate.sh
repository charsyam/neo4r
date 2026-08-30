#!/usr/bin/env bash
set -euo pipefail

contract=docs/production_readiness_contract.yml

for gate in \
  membership-authority \
  client-seed-topology-bootstrap \
  automated-multi-node-failover \
  replica-replacement-replay-gate \
  explicit-read-consistency \
  restore-seeds-cluster-from-data \
  storage-crash-point-gate \
  shard-health-observability \
  security-hardening \
  executable-production-test-matrix
do
  grep -q "id: ${gate}" "${contract}"
done

grep -q "connect_with_seeds" docs/sdk_api_parity.md
grep -q "REPLAYING" docs/query_result_contract.md
grep -q "replay completes" crates/neo4r-server/src/production_primitives.rs
grep -q "seed-new-cluster-from-restored-data" docs/pitr_restore_drill.yml
grep -q "neo4r_slo_replication_lag_entries" docs/prometheus_alerts.yml
grep -q "scripts/production-readiness-gate.sh" scripts/release-gate.sh
grep -q "automated_three_node_failover_tests: 2" "${contract}"
grep -q "authoritative_membership_ownership_tests: 3" "${contract}"
grep -q "replica_replacement_tests: 2" "${contract}"
grep -q "replay_write_rejection_tests: 2" "${contract}"
grep -q "storage_crash_points: 5" "${contract}"
grep -q "pitr_restore_drills: 2" "${contract}"
grep -q "rolling_upgrade_checks: 5" "${contract}"
grep -q "performance_and_chaos_gates: 4" "${contract}"
grep -q "scripts/production-chaos-longevity-gate.sh" "${contract}"

cargo test -p neo4r-server production_readiness_contract_requires_all_ten_gates --quiet
cargo test -p neo4r-server production_maturity_evidence_requires_runtime_depth --quiet
cargo test -p neo4r-db split_brain_old_leader_without_quorum_cannot_commit_new_write --quiet
cargo test -p neo4r-db new_leader_overwrites_only_old_uncommitted_suffix_after_partition_heals --quiet
cargo test -p neo4r-db follower_rejects_attempt_to_overwrite_committed_entry --quiet

echo "neo4r production readiness gate passed"
