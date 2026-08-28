#!/usr/bin/env bash
set -euo pipefail

cargo test -p neo4r-db replication_transport_fault_profiles_make_udp_reliability_requirements_explicit --quiet
cargo test -p neo4r-db udp_replication_channel_has_explicit_reliability_boundary --quiet

echo "neo4r replication transport fault model checks passed"
