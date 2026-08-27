use std::collections::HashMap;
use std::hash::{Hash, Hasher};

pub type NodeId = u64;
pub type RelationshipId = u64;
pub type Properties = HashMap<String, Value>;

#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
    Vector(Vec<f32>),
    Map(Properties),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ValueKey {
    Null,
    Bool(bool),
    Int(i64),
    Float(u64),
    String(String),
    Vector(Vec<u32>),
    Map(Vec<(String, ValueKey)>),
}

impl Hash for ValueKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            Self::Null => 0_u8.hash(state),
            Self::Bool(value) => {
                1_u8.hash(state);
                value.hash(state);
            }
            Self::Int(value) => {
                2_u8.hash(state);
                value.hash(state);
            }
            Self::Float(value) => {
                3_u8.hash(state);
                value.hash(state);
            }
            Self::String(value) => {
                4_u8.hash(state);
                value.hash(state);
            }
            Self::Vector(value) => {
                5_u8.hash(state);
                value.hash(state);
            }
            Self::Map(value) => {
                6_u8.hash(state);
                value.hash(state);
            }
        }
    }
}

impl From<&Value> for ValueKey {
    fn from(value: &Value) -> Self {
        match value {
            Value::Null => Self::Null,
            Value::Bool(value) => Self::Bool(*value),
            Value::Int(value) => Self::Int(*value),
            Value::Float(value) => Self::Float(value.to_bits()),
            Value::String(value) => Self::String(value.clone()),
            Value::Vector(value) => Self::Vector(value.iter().map(|item| item.to_bits()).collect()),
            Value::Map(value) => {
                let mut entries = value
                    .iter()
                    .map(|(key, value)| (key.clone(), ValueKey::from(value)))
                    .collect::<Vec<_>>();
                entries.sort_by(|left, right| left.0.cmp(&right.0));
                Self::Map(entries)
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct Node {
    pub id: NodeId,
    pub labels: Vec<String>,
    pub properties: Properties,
}

impl Node {
    pub fn new(id: NodeId, labels: Vec<String>, properties: Properties) -> Self {
        Self {
            id,
            labels,
            properties,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct Relationship {
    pub id: RelationshipId,
    pub from: NodeId,
    pub to: NodeId,
    pub rel_type: String,
    pub properties: Properties,
}

impl Relationship {
    pub fn new(
        id: RelationshipId,
        from: NodeId,
        to: NodeId,
        rel_type: String,
        properties: Properties,
    ) -> Self {
        Self {
            id,
            from,
            to,
            rel_type,
            properties,
        }
    }
}
