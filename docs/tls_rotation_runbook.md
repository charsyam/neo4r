# TLS Rotation Runbook

Neo4r production deployments should rotate native, web, and replication TLS
materials without changing the configured listener addresses.

Required sequence:

1. Install new CA bundle with old and new roots.
2. Roll new leaf certificates to followers.
3. Transfer shard leadership away from each node before restarting it.
4. Restart one node at a time and verify `/readyz`, replication catch-up, and
   `neo4r_raft_leaders`.
5. Roll leaders last.
6. Remove old roots only after every peer and SDK trusts the new CA.

Required alerts:

- certificate expires in less than 30 days.
- replication TLS handshake failure rate increases.
- web mTLS client-auth failure rate increases.
