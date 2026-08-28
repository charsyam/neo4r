# Replication Fault Model

Replication transport implementations must declare their fault profile before
they can carry Raft traffic.

## TCP

TCP is modeled as a reliable ordered stream at the channel boundary. Higher
layers still handle connection failure, timeout, duplicate request retries, and
remote rejection.

## UDP

UDP endpoints are currently negotiation-only for Raft. The declared fault model
includes drop, duplicate, reorder, fragmentation, and unbounded delivery.
Reliable Raft delivery over UDP needs sequence numbers, acknowledgements,
retransmit windows, fragmentation/reassembly, and duplicate suppression before
`raft_append`, `vote`, `snapshot`, or `catch_up` capabilities can be enabled.

## RDMA

RDMA is feature-gated as a provider boundary. A provider must document ordering,
completion, retry, and memory-registration failure semantics before enabling
Raft capabilities.
