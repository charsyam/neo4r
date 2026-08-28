use super::*;
use neo4r_core::{
    Command, HybridTimestamp, LogEntry, ShardPlacement, ShardReplica, ShardRoutingTable, Value,
};
use neo4r_db::{
    request_tcp_raft_vote, QueryAccessPlan, QueryOptions, ReadConsistency, RequestVoteRequest,
    TcpShardReplicator,
};
use neo4r_storage::IndexKind;
use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::protocol::{decode_query_rows, parse_request, BackendResponse};

include!("tests/tcp_web_console.rs");
include!("tests/native_prepared_query.rs");
include!("tests/native_catalog_commands.rs");
include!("tests/distributed_query.rs");
include!("tests/worker_cancellation.rs");
include!("tests/transaction_protocol.rs");
include!("tests/native_transaction_create.rs");
include!("tests/native_transaction_properties.rs");
include!("tests/native_transaction_group_commit.rs");
include!("tests/native_transaction_remote.rs");
include!("tests/prepared_transaction_store.rs");
include!("tests/multi_shard_prepare.rs");
include!("tests/multi_shard_commit.rs");
include!("tests/raft_replication_rpc.rs");
include!("tests/replication_quorum.rs");
