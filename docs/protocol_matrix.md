# Protocol Compatibility Matrix

Neo4r currently exposes protocol version `1` for every public boundary.

| Boundary | Version | Compatibility Fixture |
| --- | ---: | --- |
| native frame | 1 | `scripts/protocol-compat.sh` |
| HTTP admin/query | 1 | `GET /api/capabilities` |
| TCP replication append/catch-up | 1 | `crates/neo4r-db::replication::tests` |
| TCP Raft vote/pre-vote/leader-transfer | 1 | `tcp_raft_pre_vote_codec_round_trips` |
| TCP Raft snapshot install | 1 | `install_snapshot_request_chunks_payload_with_offsets` |

Compatibility rules:

- Adding fields must preserve existing field order and default behavior.
- Removing fields requires a protocol version bump and SDK capability check.
- New Raft control RPCs must have fixed magic bytes and round-trip tests before
  they are used by the replication pump.
- HTTP result envelopes must keep stable top-level fields for SDK parsers.
