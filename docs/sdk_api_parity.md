# SDK API Parity

Rust and Python SDKs intentionally expose the same basic blocking API surface for
the native protocol.

| Capability | Rust SDK | Python SDK |
| --- | --- | --- |
| connect | `Client::connect` | `Client.connect` |
| ping/close | `ping`, `close` | `ping`, `close` |
| query/execute | `query`, `query_with_params`, `execute`, `execute_with_params` | `query`, `execute` |
| native command | `command` | `command` |
| profile | `profile` | `profile` |
| query plan | `query_plan` | `query_plan` |
| statistics | `statistics` | `statistics` |
| storage status | `storage_status` | `storage_status` |
| metadata log | `metadata_log` | `metadata_log` |
| cluster status | `cluster_status` | `cluster_status` |
| cluster management | `cluster_management_status` | `cluster_management_status` |

Compatibility checks:

```bash
scripts/sdk-api-parity.sh
scripts/sdk-compat.sh
NEO4R_RUN_SDK_LIVE=1 scripts/sdk-compat.sh
```

The live SDK compatibility path starts one local server and runs both Rust and
Python examples against the same endpoint. Examples must be idempotent so repeat
runs do not depend on an empty database.
