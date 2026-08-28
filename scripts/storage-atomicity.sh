#!/usr/bin/env bash
set -euo pipefail

cargo test -p neo4r-storage graph_store::tests::relationship_create_uses_one_atomic_write_batch
cargo test -p neo4r-storage graph_store::tests::failed_relationship_write_batch_leaves_no_partial_indexes
cargo test -p neo4r-storage graph_store::tests::updates_property_index_when_node_property_changes
cargo test -p neo4r-storage graph_store::tests::removes_node_property_and_property_index
cargo test -p neo4r-storage graph_store::tests::updates_indexes_when_node_labels_change
cargo test -p neo4r-storage graph_store::tests::stores_relationship_type_adjacency_index
cargo test -p neo4r-db --test real_crash_harness real_crash_harness_reopens_child_written_data_after_kill
cargo test -p neo4r-db --test real_crash_harness real_crash_harness_reopens_relationship_and_adjacency_after_kill
