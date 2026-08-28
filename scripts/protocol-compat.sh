#!/usr/bin/env bash
set -euo pipefail

cargo test -p neo4r-protocol native_protocol
cargo test -p neo4r-server capabilities
PYTHONPATH=sdks/python python3 -m unittest sdks/python/tests/test_protocol.py
printf 'protocol compatibility checks passed\n'
