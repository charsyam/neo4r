#!/usr/bin/env bash
set -euo pipefail

cargo test -p neo4r-db udp_replication_channel --quiet
cargo test -p neo4r-db reliable_datagram_socket_sends_and_receives_frames --quiet
echo "udp transport checks passed"
