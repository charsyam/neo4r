use crate::error::QueryResult;
use crate::result::{QueryParams, QueryRow};
use neo4r_core::GraphRead;
use std::sync::Arc;

pub trait QueryEngine {
    fn execute<G: GraphRead + ?Sized>(&self, graph: &G, query: &str) -> QueryResult<Vec<QueryRow>>;

    fn execute_with_params<G: GraphRead + ?Sized>(
        &self,
        graph: &G,
        query: &str,
        params: &QueryParams,
    ) -> QueryResult<Vec<QueryRow>> {
        let _ = params;
        self.execute(graph, query)
    }

    fn execute_cursor<G: GraphRead + ?Sized>(
        &self,
        graph: &G,
        query: &str,
    ) -> QueryResult<Box<dyn QueryCursor>> {
        self.execute_cursor_with_params(graph, query, &QueryParams::new())
    }

    fn execute_cursor_with_params<G: GraphRead + ?Sized>(
        &self,
        graph: &G,
        query: &str,
        params: &QueryParams,
    ) -> QueryResult<Box<dyn QueryCursor>> {
        Ok(Box::new(VecQueryCursor::new(
            self.execute_with_params(graph, query, params)?,
        )))
    }

    fn execute_owned_cursor<G>(
        &self,
        graph: Arc<G>,
        query: &str,
    ) -> QueryResult<Box<dyn QueryCursor>>
    where
        G: GraphRead + Send + Sync + 'static,
    {
        self.execute_owned_cursor_with_params(graph, query, QueryParams::new())
    }

    fn execute_owned_cursor_with_params<G>(
        &self,
        graph: Arc<G>,
        query: &str,
        params: QueryParams,
    ) -> QueryResult<Box<dyn QueryCursor>>
    where
        G: GraphRead + Send + Sync + 'static,
    {
        Ok(Box::new(VecQueryCursor::new(self.execute_with_params(
            graph.as_ref(),
            query,
            &params,
        )?)))
    }
}

pub trait QueryCursor: Send {
    fn fetch(&mut self, page_size: usize) -> QueryPage;
    fn total_rows(&self) -> Option<usize>;
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct QueryPage {
    pub rows: Vec<QueryRow>,
    pub has_more: bool,
}

pub struct VecQueryCursor {
    rows: Vec<QueryRow>,
    position: usize,
}

impl VecQueryCursor {
    pub fn new(rows: Vec<QueryRow>) -> Self {
        Self { rows, position: 0 }
    }
}

impl QueryCursor for VecQueryCursor {
    fn fetch(&mut self, page_size: usize) -> QueryPage {
        let page_size = page_size.max(1);
        let start = self.position;
        let end = start.saturating_add(page_size).min(self.rows.len());
        let rows = self.rows[start..end].to_vec();
        self.position = end;
        QueryPage {
            rows,
            has_more: self.position < self.rows.len(),
        }
    }

    fn total_rows(&self) -> Option<usize> {
        Some(self.rows.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn vec_query_cursor_fetches_pages() {
        let mut cursor =
            VecQueryCursor::new(vec![QueryRow::new(), QueryRow::new(), QueryRow::new()]);

        let first = cursor.fetch(2);
        assert_eq!(first.rows.len(), 2);
        assert!(first.has_more);

        let second = cursor.fetch(2);
        assert_eq!(second.rows.len(), 1);
        assert!(!second.has_more);
        assert_eq!(cursor.total_rows(), Some(3));
    }
}
