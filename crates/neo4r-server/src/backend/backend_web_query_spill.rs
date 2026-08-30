use super::*;

impl TcpBackend {
    pub(crate) fn audit_query_spill_if_needed(&self, database_name: &str, rows: &[QueryRow]) {
        let Some(policy) = neo4r_query::QuerySpillPolicy::from_env() else {
            return;
        };
        let Ok(report) =
            neo4r_query::maybe_spill_operator_rows("query-result", rows, Some(&policy))
        else {
            return;
        };
        if !report.spilled {
            return;
        }
        self.audit_admin(
            "query.spill",
            database_name,
            &format!(
                "estimated_bytes={} manifest={}",
                report.estimated_bytes,
                report
                    .manifest_path
                    .as_ref()
                    .map(|path| path.display().to_string())
                    .unwrap_or_default()
            ),
        );
    }
}
