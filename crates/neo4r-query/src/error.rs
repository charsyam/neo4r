pub type QueryResult<T> = std::result::Result<T, QueryError>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum QueryError {
    Parse(String),
    Unsupported(String),
}

impl std::fmt::Display for QueryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Parse(message) => write!(f, "query parse error: {message}"),
            Self::Unsupported(message) => write!(f, "unsupported query: {message}"),
        }
    }
}

impl std::error::Error for QueryError {}
