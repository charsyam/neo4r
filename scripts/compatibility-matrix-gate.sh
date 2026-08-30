#!/usr/bin/env bash
set -euo pipefail

grep -q "native_protocol_version" docs/release_compatibility_matrix.yml
grep -q "storage_manifest_format" docs/release_compatibility_matrix.yml
grep -q "unknown_keys_rejected" docs/release_compatibility_matrix.yml
grep -q "previous-release-compatibility-fixture" docs/previous_release_fixture.yml

scripts/protocol-compat.sh
scripts/protocol-matrix.sh
scripts/sdk-api-parity.sh
scripts/sdk-compat.sh
cargo test -p neo4r-server loads_server_args_from_yaml_config_file --quiet
cargo test -p neo4r-db reopens_and_replays_segmented_logs --quiet

echo "neo4r compatibility matrix gate passed"
