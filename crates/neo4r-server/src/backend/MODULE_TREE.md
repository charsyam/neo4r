# Backend Module Tree

This directory contains the TCP/native/web backend implementation. Keep backend
changes in the narrowest matching module below.

- `backend_core.rs`: `TcpBackend` construction, listener loop, shared request
  dispatch helpers, and common backend state wiring.
- `backend_native_replication.rs`: native backend commands that manage
  replication peers, catch-up, replication status, and raft RPC control paths.
- `backend_web_admin.rs`: authenticated web admin endpoints for cluster, tenant,
  and user/token management.
- `backend_web_query_backup.rs`: web query, graph visualization, metrics, and
  backup HTTP endpoints.
- `command_executor.rs`: line/native `BackendRequest` execution against the DB
  facade. Protocol parsing and response formatting stay in `server::protocol`.
- `distributed_query.rs`: remote query planning and distributed read fan-out.
- `gossip.rs`: gossip-discovered node address records and address-book
  materialization for query/replication routing.
- `http_json_backup.rs`: JSON and backup HTTP codecs.
- `native_execution.rs`: native framed protocol execution pipeline.
- `native_worker.rs`: native worker pool and request scheduling.
- `prepared_query.rs`: prepared query lifecycle and cache state.
- `remote_transactions.rs`: remote participant/coordinator transaction flows.
- `replication_admin.rs`: replication peer status and catch-up plan formatting.
- `replication_tls.rs`: TLS-enabled replication channel implementation.
- `restore_guard.rs`: restore maintenance lock and marker file helpers.
- `state.rs`: shared backend state types for metrics, auth throttling, tenant
  quota, slow query log, and TLS channel config.
- `transaction_protocol.rs`: transaction command parsing and response formatting.
- `transaction_store.rs`: durable prepared transaction and decision state.
- `web_index.rs`: static web console assets.
