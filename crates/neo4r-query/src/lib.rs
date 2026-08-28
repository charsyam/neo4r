//! Query engine abstraction for neo4r.

mod cypher;
mod engine;
mod error;
mod executor;
mod result;
mod vector;

pub use cypher::{
    classify_statement, classify_write_statement, ComparisonOperator, CountTarget, CypherEngine,
    CypherPlan, CypherStatementKind, LogicalOperator, LogicalPlan, ParsedCypherQuery, Pattern,
    PhysicalOperator, PhysicalPlan, Predicate, PropertyNullPredicate, PropertyPredicate,
    ResultModifiers, ReturnItem, SemanticCypherQuery, SortDirection, VariableBinding, VariableKind,
    VectorKnnPredicate, WriteStatementKind,
};
pub use engine::{QueryCursor, QueryEngine, QueryPage, VecQueryCursor};
pub use error::{QueryError, QueryResult};
pub use executor::{row_scalar, ResultColumnProjector};
pub use result::{QueryParams, QueryRow, QueryValue};
pub use vector::{
    cosine_similarity, l2_distance, BruteForceVectorIndex, HnswVectorIndex, HnswVectorIndexConfig,
    VectorHit, VectorIndex, VectorIndexProvider, VectorMetric, VectorSearch,
};
