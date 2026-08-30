# Gossip Discovery

Gossip discovery gives cluster nodes a durable address book before Raft
membership changes are committed.

## Contract

- `GOSSIP_NODE <server_id> <query_address> <replication_address> <incarnation>
  <ttl_ms> [token]` records the latest address advertisement for a server.
- Higher incarnation records replace older records. Older incarnations are
  ignored so delayed messages cannot roll an address backward.
- `ttl_ms=0` means the record does not expire. Non-zero TTLs mark records as
  `expired` once `seen_at_ms + ttl_ms` is exceeded.
- Accepted records populate the query address book for address-bearing owner
  errors. The replication address remains in the gossip record until the normal
  replication negotiation flow registers a typed endpoint.
- `GOSSIP_REFRESH_MEMBERSHIP` seeds gossip from current cluster membership for
  bootstrap and recovery flows where a durable membership file already exists.
- Configured servers periodically fan out their local advertisement to
  `gossip.seed_peers` over the native command transport. The fanout layer is
  isolated from Raft authority so it can later be backed by UDP or RDMA
  discovery without changing membership semantics.
- If `gossip.auth_token` is configured, incoming advertisements must include the
  token. This prevents unauthenticated address poisoning in shared networks.

## YAML

```yaml
gossip:
  advertise_query: 10.0.0.2:7687
  advertise_replication: 10.0.0.2:9702
  interval_ms: 500
  ttl_ms: 2000
  fanout: 2
  auth_token: replace-with-a-long-secret
  auto_negotiate_replication: true
  seed_peers:
    - server_id: 1
      address: 10.0.0.1:7687
```

## Ownership Errors

When a read or write targets the wrong shard owner, the server should return an
error with `address=<target query address>`. A missing address means topology
discovery has not converged and the client must refresh topology before retrying.

## Authority Boundary

Gossip answers "where can I reach server N?". Raft metadata answers "is server N
allowed to own shard S?". A node discovered by gossip still has to pass join,
catch-up, promotion, and committed membership changes before it becomes part of
the replicated state machine.

Reconciliation runs in one direction: gossip may seed address books and
replication negotiation candidates, but it must not delete voters or promote a
learner without committed metadata.

## Rolling Upgrade

Nodes advertise support through `CAPABILITIES`:

- `gossip_discovery=true`
- `gossip_transport=native-command-fanout`
- `gossip_auth=optional-shared-token`

During mixed-version rollout, keep static `--query-peer` or `--peer` settings
until every node reports `gossip_discovery=true`.
