use crate::{QueryRow, QueryValue};

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ResultColumnProjector {
    columns: Vec<String>,
}

impl ResultColumnProjector {
    pub fn new(columns: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            columns: columns.into_iter().map(Into::into).collect(),
        }
    }

    pub fn columns(&self) -> &[String] {
        &self.columns
    }

    pub fn project(&self, row: &QueryRow) -> QueryRow {
        let mut projected = QueryRow::new();
        for column in &self.columns {
            if let Some(value) = row.get(column) {
                projected.insert(column.clone(), value.clone());
            }
        }
        projected
    }

    pub fn project_rows(&self, rows: &[QueryRow]) -> Vec<QueryRow> {
        rows.iter().map(|row| self.project(row)).collect()
    }
}

pub fn row_scalar<'a>(row: &'a QueryRow, column: &str) -> Option<&'a QueryValue> {
    row.get(column)
}

#[cfg(test)]
mod tests {
    use super::*;
    use neo4r_core::Value;

    #[test]
    fn result_column_projector_keeps_requested_columns_in_order() {
        let mut row = QueryRow::new();
        row.insert(
            "n.name",
            QueryValue::Scalar(Value::String("Alice".to_string())),
        );
        row.insert("n.age", QueryValue::Scalar(Value::Int(42)));

        let projector = ResultColumnProjector::new(["n.age", "n.missing", "n.name"]);
        let projected = projector.project(&row);

        assert_eq!(
            projector.columns(),
            &[
                "n.age".to_string(),
                "n.missing".to_string(),
                "n.name".to_string()
            ]
        );
        assert_eq!(
            row_scalar(&projected, "n.age"),
            Some(&QueryValue::Scalar(Value::Int(42)))
        );
        assert_eq!(
            row_scalar(&projected, "n.name"),
            Some(&QueryValue::Scalar(Value::String("Alice".to_string())))
        );
        assert!(row_scalar(&projected, "n.missing").is_none());
    }
}
