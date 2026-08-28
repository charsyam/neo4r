#!/usr/bin/env bash
set -euo pipefail

cargo test -p neo4r-protocol query_result_parser_accepts_rows_start_and_page_contracts --quiet
cargo test -p neo4r-server query_row_codec_round_trips_scalars_nodes_and_relationships --quiet
cargo test -p neo4r-server web_console_serves_index_and_graph_api --quiet
PYTHONPATH=sdks/python python3 -m unittest sdks/python/tests/test_protocol.py

echo "neo4r query result contract checks passed"
