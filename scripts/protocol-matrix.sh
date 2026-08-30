#!/usr/bin/env bash
set -euo pipefail

test -f docs/protocol_matrix.md
grep -q "native frame" docs/protocol_matrix.md
grep -q "TCP Raft vote/pre-vote/leader-transfer" docs/protocol_matrix.md
grep -q "TCP Raft snapshot fetch" docs/protocol_matrix.md
grep -q "HTTP admin/query" docs/protocol_matrix.md
cargo test -p neo4r-db tcp_raft_pre_vote_codec_round_trips --quiet
cargo test -p neo4r-db tcp_raft_leader_transfer_response_codec_round_trips --quiet
cargo test -p neo4r-db tcp_snapshot_fetch_serves_primary_snapshot_for_node_catch_up --quiet
echo "protocol matrix checks passed"
