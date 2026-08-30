# QueryResult Contract

This document fixes the external result naming and wire shape.

## Naming

- `DatabaseResult<T>` is the internal Rust error/result alias for database
  operations.
- `QueryResult` is the external query response concept exposed by native TCP,
  HTTP, Rust SDK, and Python SDK query APIs.

Do not expose new user-facing query APIs as `DatabaseResult`. The name
`DatabaseResult` is reserved for in-process storage/database errors.

## Native TCP

Native query responses keep the existing framed payload:

```text
OK\tRESULT_START\tcolumns=<n>\trows=<n>\thas_more=<bool>\tcursor=<id>\t<rows>
```

Rows are encoded through the shared protocol codec. Clients must not parse
graph values with ad hoc string splitting outside the codec layer.

Redirects and topology changes are typed error payloads:

```text
ERR\tMOVED\tshard=<id>\tleader=<server>\taddress=<host:port>\trouting_version=<n>\tdatabase=<name>\tretryable=true
ERR\tSTALE_EPOCH\ttx_epoch=<n>\tcurrent_epoch=<n>\trouting_version=<n>\townership_epoch=<n>\tretryable=true
ERR\tREPLAYING\tshard=<id>\tserver=<server>\tleader=<server|none>\taddress=<host:port|missing>\trouting_version=<n>\townership_epoch=<n>\tapplied=<n>\tcommitted=<n>\tretryable=true\trefresh=CLUSTER_REGISTRY
```

SDKs should automatically follow retryable redirects with a bounded retry
budget when `address` is present. If `address=missing`, the error must include
`refresh=CLUSTER_REGISTRY`; clients refresh the gossip-backed registry and pick
the shard leader or another query peer from the updated topology cache.

## HTTP

HTTP `/api/query` returns a JSON object with stable top-level fields:

```json
{
  "rows": [],
  "columns": [],
  "plan": null,
  "database": "default"
}
```

New fields may be added, but existing fields must remain backward compatible.
Breaking changes require a protocol compatibility test and a README note.

## Compatibility Gate

Run these before changing the result format:

```bash
scripts/protocol-compat.sh
scripts/query-result-contract.sh
scripts/sdk-api-parity.sh
scripts/sdk-failover.sh
```
