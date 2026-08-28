# Fault Injection Matrix

The current deterministic fault points are:

| Area | Fault | Expected recovery |
| --- | --- | --- |
| WAL/apply | `fail_after_commit_before_apply` | reopen replays committed WAL |
| snapshot | `fail_before_snapshot_payload_save` | no metadata advances |
| snapshot | `fail_after_snapshot_payload_save_before_metadata` | payload remains recoverable |
| snapshot | `fail_after_snapshot_prune_before_apply` | missing apply is observable |
| restore | pending restore manifest written before materialization | reopen replays pending restore |
| process crash | child process killed after committed write | reopen returns committed data |

Every new fault point should define:

- trigger name in `FailureInjection`
- exact boundary being interrupted
- recovery source of truth
- test command in `scripts/ci-crash.sh`

The recovery source of truth must be one of WAL, committed index, snapshot
payload plus metadata, or restore pending manifest. Ambiguous recovery ownership
is not acceptable for database correctness.
