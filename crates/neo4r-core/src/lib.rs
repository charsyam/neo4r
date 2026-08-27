//! Core property graph state machine for neo4r.
//!
//! This crate deliberately has no cluster dependency. A cluster implementation
//! should replicate [`Command`] values, then call [`GraphState::apply`] in
//! commit order.

pub mod clock;
pub mod command;
pub mod error;
pub mod graph;
pub mod model;
pub mod read;
pub mod shard;

pub use clock::{HybridClock, HybridTimestamp};
pub use command::Command;
pub use error::{GraphError, Result};
pub use graph::GraphState;
pub use model::{Node, NodeId, Properties, Relationship, RelationshipId, Value, ValueKey};
pub use read::{GraphRead, GraphReadError, GraphReadResult};
pub use shard::{
    BoundaryIncomingRef, BoundaryNode, ConfigVersion, LogEntry, LogIndex, ServerId, ShardId,
    ShardMap, ShardPlacement, ShardPolicy, ShardPrimaryMove, ShardReplica, ShardRole,
    ShardRoutingDiff, ShardRoutingTable, Term,
};
