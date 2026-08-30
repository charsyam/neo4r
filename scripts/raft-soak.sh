#!/usr/bin/env bash
set -euo pipefail

ROUNDS="${NEO4R_RAFT_SOAK_ROUNDS:-3}"
for _ in $(seq 1 "$ROUNDS"); do
  cargo test -p neo4r-db raft_append_truncates_divergent_segmented_wal_suffix --quiet
  cargo test -p neo4r-db tcp_raft_append_falls_back_to_install_snapshot_on_rejection --quiet
  cargo test -p neo4r-server raft_election_round_promotes_candidate_after_peer_vote --quiet
  cargo test -p neo4r-server persistent_backends_catch_up_then_live_replicate_with_reloaded_peers --quiet
done

if [[ "${NEO4R_RUN_RAFT_SOAK_LIVE:-0}" == "1" ]]; then
  NEO4R_RUN_CLUSTER_RESTART=1 scripts/multi_process_cluster_smoke.sh
fi

echo "neo4r raft soak passed"
