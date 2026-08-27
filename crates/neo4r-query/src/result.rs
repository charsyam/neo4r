use neo4r_core::{BoundaryNode, Node, Relationship, Value};
use std::collections::HashMap;

pub type QueryParams = HashMap<String, Value>;

#[derive(Clone, Debug, PartialEq)]
pub enum QueryValue {
    Node(Node),
    BoundaryNode(BoundaryNode),
    Relationship(Relationship),
    Scalar(Value),
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct QueryRow {
    values: HashMap<String, QueryValue>,
}

impl QueryRow {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, name: impl Into<String>, value: QueryValue) {
        self.values.insert(name.into(), value);
    }

    pub fn get(&self, name: &str) -> Option<&QueryValue> {
        self.values.get(name)
    }

    pub fn values(&self) -> &HashMap<String, QueryValue> {
        &self.values
    }
}
