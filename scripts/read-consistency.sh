#!/usr/bin/env bash
set -euo pipefail

cargo test -p neo4r-db raft_strong_read_requires_leader_lease_but_follower_stale_can_read --quiet
cargo test -p neo4r-db expired_leader_lease_falls_back_to_quorum_read_index --quiet
cargo test -p neo4r-server native_read_write_transaction_reads_staged_node_property_updates --quiet
cargo test -p neo4r-server read_consistency_contract_requires_read_index_default --quiet

echo "read consistency checks passed"
