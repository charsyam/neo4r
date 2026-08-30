# Gossip Discovery

Gossip discovery gives cluster nodes a durable address book before Raft
membership changes are committed.

## Contract

- `GOSSIP_NODE <server_id> <query_address> <replication_address> <incarnation>
  <ttl_ms>` records the latest address advertisement for a server.
- Higher incarnation records replace older records. Older incarnations are
  ignored so delayed messages cannot roll an address backward.
- `ttl_ms=0` means the record does not expire. Non-zero TTLs mark records as
  `expired` once `seen_at_ms + ttl_ms` is exceeded.
- Accepted records populate the query address book for address-bearing owner
  errors. The replication address remains in the gossip record until the normal
  replication negotiation flow registers a typed endpoint.
- `GOSSIP_REFRESH_MEMBERSHIP` seeds gossip from current cluster membership for
  bootstrap and recovery flows where a durable membership file already exists.

## Ownership Errors

When a read or write targets the wrong shard owner, the server should return an
error with `address=<target query address>`. A missing address means topology
discovery has not converged and the client must refresh topology before retrying.

## Authority Boundary

Gossip answers "where can I reach server N?". Raft metadata answers "is server N
allowed to own shard S?". A node discovered by gossip still has to pass join,
catch-up, promotion, and committed membership changes before it becomes part of
the replicated state machine.
