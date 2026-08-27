//! Query engine abstraction for neo4r.

mod cypher;
mod engine;
mod error;
mod result;
mod vector;

pub use cypher::CypherEngine;
pub use engine::{QueryCursor, QueryEngine, QueryPage, VecQueryCursor};
pub use error::{QueryError, QueryResult};
pub use result::{QueryParams, QueryRow, QueryValue};
pub use vector::{
    cosine_similarity, l2_distance, BruteForceVectorIndex, HnswVectorIndex, HnswVectorIndexConfig,
    VectorHit, VectorIndex, VectorIndexProvider, VectorMetric, VectorSearch,
};
